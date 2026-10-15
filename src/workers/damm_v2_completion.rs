//! Meteora DAMM v2: Registry candidates -> validated RPC accounts -> V10 manifests.
//! Owns all DAMM v2 layout, mint/vault validation, scheduling and evidence encoding.
//! No direct database access, no Registry writes, no provider-specific RPC code.
use std::{collections::{HashMap, HashSet}, env, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::{
    sync::{broadcast, watch},
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};

use crate::{
    core::{
        AccountMeta, CompletionEvidence, CompletionResult, CompletionResultBatch,
        CompletionState, CompletionStatus, ExecutionManifest, MintProof, PoolState,
        PoolUpdateBatch, Venue,
    },
    workers::rpc_transport::{AccountData, AccountView, RpcAnswer, RpcHandle, RpcOperation},
};

const SOURCE: &str = "meteora_damm_v2_execution_completion";
const PROGRAM: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";
const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const POOL_DISCRIMINATOR: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];
const MIN_TVL_USD: f64 = 500.0;
const POOLS_PER_BATCH: usize = 16;
const RETRY_COOLDOWN: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
pub struct Settings {
    pub limit: usize,
    account_batch_size: usize,
    parallel_batches: usize,
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        let account_batch_size = env::var("RPC_ACCOUNT_BATCH_SIZE")
            .unwrap_or_else(|_| "100".into()).parse::<usize>()
            .context("invalid RPC_ACCOUNT_BATCH_SIZE")?;
        ensure!((5..=100).contains(&account_batch_size),
            "DAMM v2 requires RPC_ACCOUNT_BATCH_SIZE in 5..=100");
        let parallel_batches = env::var("DAMM_V2_COMPLETION_PARALLEL_BATCHES")
            .unwrap_or_else(|_| "16".into()).parse::<usize>()
            .context("invalid DAMM_V2_COMPLETION_PARALLEL_BATCHES")?;
        ensure!((1..=64).contains(&parallel_batches),
            "DAMM_V2_COMPLETION_PARALLEL_BATCHES must be 1..=64");
        Ok(Self { limit: 0, account_batch_size, parallel_batches })
    }
    fn pools_per_batch(self) -> usize {
        POOLS_PER_BATCH.min(self.account_batch_size / 5)
    }
}

#[derive(Clone, Copy)]
struct PoolLayout {
    address: Pubkey,
    mint_a: Pubkey,
    mint_b: Pubkey,
    vault_a: Pubkey,
    vault_b: Pubkey,
}

fn key(bytes: &[u8], offset: usize) -> Result<Pubkey> {
    let raw: [u8; 32] = bytes.get(offset..offset + 32)
        .context("truncated DAMM v2 pool state")?.try_into()?;
    Ok(Pubkey::new_from_array(raw))
}

fn decode_pool(account: &AccountData, expected: &PoolState, program: Pubkey) -> Result<PoolLayout> {
    ensure!(account.address == expected.address && account.owner == program,
        "DAMM v2 pool address / owner mismatch");
    ensure!(account.view == AccountView::Full &&
        account.original_len == Some(account.bytes.len()),
        "DAMM v2 pool account is not a full RPC read");
    let data = &account.bytes;
    ensure!(data.len() >= 296 && data[..8] == POOL_DISCRIMINATOR,
        "DAMM v2 Pool discriminator or data length invalid");
    // PoolPair::read(DAMMV2) in the public V10 builder:
    // https://github.com/Cetipoo/solana-onchain-arbitrage-bot/blob/master/src/v10.rs
    let (mint_a, mint_b) = (key(data,168)?, key(data,200)?);
    let (vault_a, vault_b) = (key(data,232)?, key(data,264)?);
    ensure!(mint_a != Pubkey::default() && mint_b != Pubkey::default()
        && mint_a != mint_b && vault_a != Pubkey::default()
        && vault_b != Pubkey::default() && vault_a != vault_b,
        "DAMM v2 Pool mint/vault identity invalid");
    ensure!(expected.mint_a == Some(mint_a) && expected.mint_b == Some(mint_b),
        "DAMM v2 Pool mints disagree with Registry");
    Ok(PoolLayout { address: account.address, mint_a, mint_b, vault_a, vault_b })
}

fn decode_mint(account: &AccountData, spl: Pubkey, token_2022: Pubkey) -> Result<MintProof> {
    let is_2022 = if account.owner == spl { false }
        else if account.owner == token_2022 { true }
        else { bail!("DAMM v2 mint has unsupported program owner {}", account.owner); };
    let d = &account.bytes;
    ensure!(d.len() >= 82 && d[45] == 1, "DAMM v2 mint base layout / initialized flag invalid");
    let mut extensions = Vec::new();
    if is_2022 && d.len() > 82 {
        ensure!(d.len() >= 166 && d[82..165].iter().all(|b| *b == 0)
            && d[165] == 1, "DAMM v2 Token-2022 Mint extension framing invalid");
        let mut pos = 166;
        while pos < d.len() {
            let tail = &d[pos..];
            if tail.iter().all(|b| *b == 0) { break; }
            ensure!(tail.len() >= 4, "truncated DAMM v2 Token-2022 extension");
            let kind = u16::from_le_bytes(tail[..2].try_into()?);
            let len = usize::from(u16::from_le_bytes(tail[2..4].try_into()?));
            if kind == 0 && len == 0 {
                ensure!(tail[4..].iter().all(|b| *b == 0),
                    "nonzero Token-2022 data after TLV terminator");
                break;
            }
            ensure!(kind != 0 && len <= tail.len()-4,
                "invalid DAMM v2 Token-2022 extension");
            extensions.push(kind);
            pos += 4 + len;
        }
    } else if !is_2022 {
        ensure!(d.len() == 82, "unexpected DAMM v2 SPL mint account length");
    }
    extensions.sort_unstable();
    ensure!(!extensions.windows(2).any(|w| w[0] == w[1]),
        "duplicate DAMM v2 Token-2022 extensions");
    // V10 compatibility exclusions match the already accepted DLMM completion policy.
    let unsupported = extensions.iter().copied()
        .filter(|k| matches!(*k, 4 | 9 | 14 | 16 | 24)).collect();
    Ok(MintProof {
        address: account.address, token_2022: is_2022, extensions, unsupported,
    })
}

fn validate_vault(account: &AccountData, mint: Pubkey, token_program: Pubkey,
    pool_authority: Pubkey) -> Result<()> {
    ensure!(account.owner == token_program && account.bytes.len() >= 165,
        "DAMM v2 vault Token program / account length mismatch");
    ensure!(key(&account.bytes,0)? == mint
        && key(&account.bytes,32)? == pool_authority,
        "DAMM v2 vault mint or pool authority mismatch");
    ensure!(matches!(account.bytes[108], 1 | 2), "DAMM v2 vault not initialized");
    Ok(())
}

struct ReadBatch { slot: u64, accounts: Vec<Option<AccountData>> }

async fn read(rpc: &RpcHandle, addresses: Vec<Pubkey>, min_slot: Option<u64>)
    -> Result<ReadBatch> {
    ensure!(!addresses.is_empty(), "empty DAMM v2 RPC batch");
    let expected = addresses.clone();
    let result = rpc.call(RpcOperation::Multiple {
        addresses, view: AccountView::Full, min_context_slot: min_slot,
    }).await?;
    let RpcAnswer::Multiple {slot, accounts} = result.answer
        else { bail!("DAMM v2 RPC returned unexpected answer"); };
    ensure!(accounts.len() == expected.len()
        && min_slot.is_none_or(|min| slot >= min),
        "DAMM v2 RPC cardinality or minContextSlot violation");
    for (address, account) in expected.iter().zip(&accounts) {
        if let Some(account) = account {
            ensure!(account.address == *address && account.view == AccountView::Full
                && account.original_len == Some(account.bytes.len()),
                "DAMM v2 RPC account identity or full-length mismatch");
        }
    }
    println!("damm_v2 completion rpc provider={} slot={} accounts={} present={}",
        result.provider, slot, accounts.len(), accounts.iter().filter(|a| a.is_some()).count());
    Ok(ReadBatch {slot, accounts})
}

fn result(pool: Pubkey, status: CompletionStatus, slot: Option<u64>,
    reason: Option<String>, evidence: Option<Arc<CompletionEvidence>>,
    mints: Option<[MintProof; 2]>, manifest: Option<Arc<ExecutionManifest>>)
    -> CompletionResult {
    CompletionResult {
        pool, status, validated_slot: slot, manifest, reason, evidence, mints,
    }
}

fn retry(pool: Pubkey, reason: impl Into<String>) -> CompletionResult {
    result(pool, CompletionStatus::Retryable, None, Some(reason.into()), None, None, None)
}
fn incomplete(pool: Pubkey, slot: u64, reason: impl Into<String>) -> CompletionResult {
    result(pool, CompletionStatus::Incomplete, Some(slot), Some(reason.into()), None, None, None)
}

fn manifest(layout: PoolLayout, slot: u64, program: Pubkey) -> ExecutionManifest {
    let ro = |address| AccountMeta {address, writable:false};
    let rw = |address| AccountMeta {address, writable:true};
    let event = Pubkey::find_program_address(&[b"__event_authority"], &program).0;
    let authority = Pubkey::find_program_address(&[b"pool_authority"], &program).0;
    let direction = |base_mint: Pubkey, reverse: bool| {
        // Public V10 PoolAccounts::from_keys DammV2 block: 7 accounts.
        // base_a means token A is quote (X=B), base_b means token B is quote (X=A).
        let (x_vault, base_vault) = if reverse {
            (layout.vault_b, layout.vault_a)
        } else { (layout.vault_a, layout.vault_b) };
        Arc::<[AccountMeta]>::from(vec![
            ro(program), ro(base_mint), ro(event), ro(authority),
            rw(layout.address), rw(x_vault), rw(base_vault),
        ])
    };
    ExecutionManifest {
        pool: layout.address, mint_a: layout.mint_a, mint_b: layout.mint_b,
        validated_slot:slot,
        base_a:direction(layout.mint_a,true), base_b:direction(layout.mint_b,false),
    }
}

fn decode_final(pool: &PoolState, original: PoolLayout, slot: u64,
    lookup: &HashMap<Pubkey, Option<AccountData>>, program: Pubkey,
    spl: Pubkey, token_2022: Pubkey) -> CompletionResult {
    let fetch = |address: Pubkey| -> Option<&AccountData> {
        lookup.get(&address).and_then(Option::as_ref)
    };
    let Some(latest) = fetch(pool.address) else {
        return retry(pool.address, "DAMM v2 Pool missing from final RPC snapshot");
    };
    let latest = match decode_pool(latest,pool,program) {
        Ok(v) => v,
        Err(e) => return incomplete(pool.address,slot,format!("DAMM v2 final Pool invalid: {e:#}")),
    };
    if original.vault_a != latest.vault_a || original.vault_b != latest.vault_b {
        return retry(pool.address,"DAMM v2 vaults changed while validating RPC accounts");
    }
    let (Some(a),Some(b),Some(va),Some(vb))=(
        fetch(latest.mint_a),fetch(latest.mint_b),
        fetch(latest.vault_a),fetch(latest.vault_b)
    ) else {
        return retry(pool.address, "DAMM v2 mint/vault account unavailable");
    };
    let mint_a = match decode_mint(a,spl,token_2022) {
        Ok(m)=>m, Err(e)=>return incomplete(pool.address,slot,format!("DAMM v2 mint A invalid: {e:#}")),
    };
    let mint_b = match decode_mint(b,spl,token_2022) {
        Ok(m)=>m, Err(e)=>return incomplete(pool.address,slot,format!("DAMM v2 mint B invalid: {e:#}")),
    };
    let authority = Pubkey::find_program_address(&[b"pool_authority"],&program).0;
    if let Err(e)=validate_vault(va,latest.mint_a,a.owner,authority) {
        return incomplete(pool.address,slot,format!("DAMM v2 Vault A invalid: {e:#}"));
    }
    if let Err(e)=validate_vault(vb,latest.mint_b,b.owner,authority) {
        return incomplete(pool.address,slot,format!("DAMM v2 Vault B invalid: {e:#}"));
    }
    let evidence = Arc::new(CompletionEvidence {
        kind:"meteora_damm_v2_pool".to_owned(), version:1,
        payload: Arc::new(json!({
            "program": program.to_string(),
            "vault_a": latest.vault_a.to_string(),
            "vault_b": latest.vault_b.to_string(),
            "pool_authority": authority.to_string(),
            "token_program_a": a.owner.to_string(),
            "token_program_b": b.owner.to_string(),
        })),
    });
    let mints=[mint_a,mint_b];
    let unsupported = mints.iter().any(|m|!m.unsupported.is_empty());
    if unsupported {
        return result(pool.address,CompletionStatus::Incomplete,Some(slot),
            Some(format!("V10 unsupported Token-2022 mint extensions: {:?} {:?}",
                mints[0].unsupported,mints[1].unsupported)),
            Some(evidence),Some(mints),None);
    }
    result(pool.address,CompletionStatus::Prepared,Some(slot),None,
        Some(evidence),Some(mints),Some(Arc::new(manifest(latest,slot,program))))
}

async fn process(rpc: &RpcHandle, pools: Vec<PoolState>, settings: Settings)
    -> Vec<CompletionResult> {
    let program: Pubkey = PROGRAM.parse().expect("fixed DAMM v2 Program ID");
    let spl: Pubkey = SPL_TOKEN.parse().expect("fixed SPL Token Program ID");
    let token_2022: Pubkey = TOKEN_2022.parse().expect("fixed Token-2022 Program ID");
    let mut results: Vec<Option<CompletionResult>> = vec![None; pools.len()];
    let first = match read(rpc,pools.iter().map(|p|p.address).collect(),None).await {
        Ok(data)=>data,
        Err(e)=>return pools.iter().map(|p|retry(p.address,format!("DAMM v2 Pool RPC failed: {e:#}"))).collect(),
    };
    let mut pending=Vec::new();
    let mut addresses=Vec::new();
    let mut unique=HashSet::new();
    for (i,pool) in pools.iter().enumerate() {
        let Some(account)=first.accounts[i].as_ref() else {
            results[i]=Some(retry(pool.address,"DAMM v2 pool account missing"));
            continue;
        };
        match decode_pool(account,pool,program) {
            Ok(layout)=>{
                for key in [layout.address,layout.mint_a,layout.mint_b,layout.vault_a,layout.vault_b] {
                    if unique.insert(key) {addresses.push(key);}
                }
                pending.push((i,layout));
            }
            Err(e)=>results[i]=Some(incomplete(pool.address,first.slot,format!("DAMM v2 Pool invalid: {e:#}"))),
        }
    }
    if !pending.is_empty() {
        // One final coherent slot validates parent state, both vaults and mint programs.
        assert!(addresses.len() <= settings.account_batch_size,
            "DAMM v2 final account batch exceeds configured limit");
        match read(rpc,addresses.clone(),Some(first.slot)).await {
            Ok(final_read)=>{
                let lookup: HashMap<Pubkey, Option<AccountData>> =
                    addresses.into_iter().zip(final_read.accounts).collect();
                for (i,layout) in pending {
                    results[i]=Some(decode_final(&pools[i],layout,final_read.slot,
                        &lookup,program,spl,token_2022));
                }
            }
            Err(e)=>{
                for (i,_) in pending {
                    results[i]=Some(retry(pools[i].address,format!("DAMM v2 final RPC failed: {e:#}")));
                }
            }
        }
    }
    results.into_iter().map(|r|r.expect("each pool has a completion result")).collect()
}

#[derive(Default)]
struct Scheduler {
    candidates: HashMap<Pubkey,PoolState>,
    attempts: HashMap<Pubkey,u8>,
    retry_after: HashMap<Pubkey,Instant>,
}
impl Scheduler {
    fn update(&mut self, pool: &PoolState) {
        if pool.venue != Venue::MeteoraDammV2 {return;}
        if pool.completion_state==CompletionState::Prepared {
            self.candidates.remove(&pool.address);
            self.retry_after.remove(&pool.address);
        } else if pool.mint_a.is_none() || pool.mint_b.is_none()
            || !pool.tvl_usd.is_some_and(|v|v.is_finite() && v>MIN_TVL_USD) {
            self.candidates.remove(&pool.address);
        } else {
            self.candidates.insert(pool.address,pool.clone());
        }
    }
    fn eligible(&self, pool: &PoolState, now: Instant) -> Option<u8> {
        match self.attempts.get(&pool.address).copied() {
            None=>match pool.completion_state {
                CompletionState::Unknown=>Some(1),
                CompletionState::Incomplete if !pool.completion_reason.as_deref()
                    .is_some_and(|s|s.contains("V10 unsupported Token-2022 mint extensions"))=>Some(2),
                _=>None,
            },
            Some(1) if self.retry_after.get(&pool.address).is_some_and(|t|*t<=now)=>Some(3),
            _=>None,
        }
    }
    fn next(&self, max:usize, now:Instant)->(u8,Vec<PoolState>) {
        let mut tier=0;
        let mut selected=Vec::new();
        for p in self.candidates.values() {
            let Some(level)=self.eligible(p,now) else {continue;};
            if tier==0 || level<tier { tier=level; selected.clear(); }
            if level==tier {selected.push(p.clone());}
        }
        selected.sort_unstable_by(|a,b|
            b.tvl_usd.unwrap_or_default().total_cmp(&a.tvl_usd.unwrap_or_default())
                .then_with(||a.address.to_bytes().cmp(&b.address.to_bytes())));
        selected.truncate(max);
        (tier,selected)
    }
    fn pending_retry(&self)->bool {
        self.retry_after.iter().any(|(pool,_)|
            self.candidates.contains_key(pool) && self.attempts.get(pool)==Some(&1))
    }
    fn counts(&self,now:Instant)->(usize,usize,usize) {
        let mut a=0;let mut b=0;let mut c=0;
        for pool in self.candidates.values() {
            match self.eligible(pool,now) {
                Some(1)=>a+=1,Some(2)=>b+=1,Some(3)=>c+=1,_=>(),
            }
        }
        (a,b,c)
    }
}

pub async fn run(mut input:broadcast::Receiver<Arc<PoolUpdateBatch>>,
    source_done:watch::Receiver<bool>, rpc:RpcHandle, settings:Settings,
    output:broadcast::Sender<Arc<CompletionResultBatch>>) -> Result<()> {
    let mut scheduler=Scheduler::default();
    let mut tasks=JoinSet::new();
    let mut source_open=true;
    let mut seen=HashSet::new();
    let mut received_updates=0_usize;
    let mut attempts=0_usize;
    let mut first_attempts=0_usize;
    let mut retries=0_usize;
    let mut prepared=0_usize;
    let mut incomplete=0_usize;
    let mut retryable=0_usize;
    let mut pulse=tokio::time::interval(Duration::from_secs(5));
    pulse.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let started=Instant::now();
    println!("damm_v2 completion started min_tvl_usd={} limit={} parallel={} pools_per_batch={} retry_cooldown_s={}",
        MIN_TVL_USD,settings.limit,settings.parallel_batches,settings.pools_per_batch(),
        RETRY_COOLDOWN.as_secs());
    loop {
        while tasks.len()<settings.parallel_batches
            && (settings.limit==0 || attempts<settings.limit) {
            let remaining=if settings.limit==0 {settings.pools_per_batch()}
                else {settings.pools_per_batch().min(settings.limit-attempts)};
            if remaining==0 {break;}
            let (tier,selected)=scheduler.next(remaining,Instant::now());
            if selected.is_empty() {break;}
            for pool in &selected {
                let next=scheduler.attempts.get(&pool.address).copied().unwrap_or(0)+1;
                scheduler.attempts.insert(pool.address,next);
                scheduler.retry_after.remove(&pool.address);
            }
            attempts+=selected.len();
            if tier==3 {retries+=selected.len();} else {first_attempts+=selected.len();}
            let rpc=rpc.clone();
            tasks.spawn(async move {
                let results=process(&rpc,selected,settings).await;
                (tier,results)
            });
        }
        let stopped=settings.limit>0 && attempts>=settings.limit;
        let done=!source_open || *source_done.borrow();
        if tasks.is_empty() && (stopped || (done && input.len()==0
            && !scheduler.pending_retry() && scheduler.next(1,Instant::now()).1.is_empty())) {
            break;
        }
        tokio::select! {
            event=input.recv(),if source_open=>match event {
                Ok(batch)=>{
                    received_updates+=batch.pools.len();
                    for pool in &batch.pools {
                        if pool.venue==Venue::MeteoraDammV2 {seen.insert(pool.address);}
                        scheduler.update(pool);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n))=>
                    bail!("DAMM v2 completion lost {n} Registry batches"),
                Err(broadcast::error::RecvError::Closed)=>source_open=false,
            },
            done_batch=tasks.join_next(),if !tasks.is_empty()=>{
                let (tier,results)=done_batch.context("missing DAMM v2 partition")?
                    .context("DAMM v2 completion task panicked")?;
                let mut p=0;let mut i=0;let mut r=0;
                for item in &results {
                    match item.status {
                        CompletionStatus::Prepared=>{
                            p+=1;scheduler.candidates.remove(&item.pool);
                            scheduler.retry_after.remove(&item.pool);
                        }
                        CompletionStatus::Incomplete=>i+=1,
                        CompletionStatus::Retryable=>{
                            r+=1;
                            if scheduler.attempts.get(&item.pool)==Some(&1)
                                && scheduler.candidates.contains_key(&item.pool) {
                                scheduler.retry_after.insert(item.pool,Instant::now()+RETRY_COOLDOWN);
                            }
                        }
                    }
                }
                prepared+=p;incomplete+=i;retryable+=r;
                println!("damm_v2 completion batch tier={tier} attempts={} prepared={p} incomplete={i} retryable={r} total_attempts={attempts} elapsed_ms={}",
                    results.len(),started.elapsed().as_millis());
                for result in results.iter().filter(|r|r.status==CompletionStatus::Prepared).take(2) {
                    if let Some(manifest)=&result.manifest {
                        println!("damm_v2 manifest pool={} slot={} a_accounts={} b_accounts={} base_a={:?} base_b={:?}",
                            manifest.pool,manifest.validated_slot,manifest.base_a.len(),manifest.base_b.len(),
                            manifest.base_a.iter().map(|a|format!("{}:{}",a.address,if a.writable {"rw"} else {"ro"}))
                                .collect::<Vec<_>>(),
                            manifest.base_b.iter().map(|a|format!("{}:{}",a.address,if a.writable {"rw"} else {"ro"}))
                                .collect::<Vec<_>>());
                    }
                }
                for result in results.iter().filter(|r|r.status!=CompletionStatus::Prepared).take(3) {
                    println!("damm_v2 completion result pool={} status={:?} reason={}",
                        result.pool,result.status,result.reason.as_deref().unwrap_or("-"));
                }
                output.send(Arc::new(CompletionResultBatch {source:SOURCE,results}))
                    .map_err(|_|anyhow!("DAMM v2 completion has no Registry consumer"))?;
            },
            _=pulse.tick()=>{
                let (a,b,c)=scheduler.counts(Instant::now());
                println!("damm_v2 completion backlog seen={} candidate_unknown={a} candidate_incomplete={b} eligible_retry={c} candidates_in_ram={} active_batches={} attempts={attempts} prepared={prepared} incomplete={incomplete} retryable={retryable} registry_updates={received_updates}",
                    seen.len(),scheduler.candidates.len(),tasks.len());
            }
        }
    }
    let (a,b,c)=scheduler.counts(Instant::now());
    println!("damm_v2 completion complete min_tvl_usd={MIN_TVL_USD} pools_seen={} registry_updates={received_updates} first_attempts={first_attempts} retries={retries} prepared={prepared} incomplete={incomplete} retryable={retryable} eligible_unknown_left={a} eligible_incomplete_left={b} eligible_retry_left={c} elapsed_ms={}",
        seen.len(),started.elapsed().as_millis());
    Ok(())
}
