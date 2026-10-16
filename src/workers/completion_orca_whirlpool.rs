//! Orca Whirlpool completion: registry candidates -> coherent RPC reads -> V10 accounts.
//! No DB, direct RPC client, graph mutation, or REST dependency.
use std::{collections::{HashMap, HashSet}, env, sync::Arc, time::Duration};
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, watch}, task::JoinSet, time::{Instant, MissedTickBehavior}};
use crate::{
    core::{AccountMeta, CompletionEvidence, CompletionResult, CompletionResultBatch,
        CompletionState, CompletionStatus, ExecutionManifest, MintProof, PoolState,
        PoolUpdateBatch, Venue},
    workers::rpc_transport::{AccountData, AccountView, RpcAnswer, RpcHandle, RpcOperation},
};

const SOURCE: &str = "orca_whirlpool_execution_completion";
const PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const SPL: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const MEMO: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const SYSTEM: &str = "11111111111111111111111111111111";
const WHIRLPOOL_DISC: [u8;8] = [63,149,209,12,225,128,99,9];
const FIXED_DISC: [u8;8] = [69,97,189,190,110,7,66,187];
const DYNAMIC_DISC: [u8;8] = [17,216,246,142,225,199,218,56];
const ORACLE_DISC: [u8;8] = [139,194,131,179,140,179,229,244];
const FIXED_LEN: usize = 8 + 4 + 88 * 113 + 32;
const DYNAMIC_MIN_LEN: usize = 8 + 4 + 32 + 16 + 88;
const DYNAMIC_MAX_LEN: usize = 8 + 4 + 32 + 16 + 88 * 113;
const ORACLE_LEN: usize = 8 + 32 + 8 + 34 + 44 + 128;
const POOLS_PER_BATCH: usize = 8; // second pass <= 12 addresses per pool
const PARALLEL_BATCHES: usize = 16;
const MIN_TVL_USD: f64 = 500.0;
const ARRAY_RADIUS: i32 = 2;
const RETRY_AFTER: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
pub struct Settings {
    pub limit: usize,
    account_batch_size: usize,
    parallel_batches: usize,
    min_tvl_usd: f64,
    radius: i32,
}
impl Settings {
    pub fn from_env() -> Result<Self> {
        // Same RPC_ACCOUNT_BATCH_SIZE as DLMM/DAMM v2. The RPC client and its
        // provider configuration remain exclusively owned by rpc_transport.
        let account_batch_size = env::var("RPC_ACCOUNT_BATCH_SIZE")
            .unwrap_or_else(|_| "100".to_owned()).parse::<usize>()
            .context("invalid RPC_ACCOUNT_BATCH_SIZE")?;
        ensure!((12..=100).contains(&account_batch_size),
            "Orca requires RPC_ACCOUNT_BATCH_SIZE in 12..=100 for its coherent account snapshot");
        Ok(Self { limit:0, account_batch_size, parallel_batches:PARALLEL_BATCHES,
            min_tvl_usd:MIN_TVL_USD, radius:ARRAY_RADIUS })
    }
    fn pools_per_batch(self) -> usize {
        POOLS_PER_BATCH.min(self.account_batch_size / 12)
    }
}

#[derive(Clone, Copy)]
struct Layout {
    pool: Pubkey, mint_a: Pubkey, mint_b: Pubkey,
    vault_a: Pubkey, vault_b: Pubkey,
    tick_spacing: i32, tick_current: i32,
}
fn key(bytes: &[u8], offset: usize) -> Result<Pubkey> {
    Ok(Pubkey::new_from_array(bytes.get(offset..offset+32)
        .context("truncated Whirlpool account key")?.try_into()?))
}
fn decode_pool(account: &AccountData, expected: &PoolState, owner: Pubkey) -> Result<Layout> {
    ensure!(account.address == expected.address && account.owner == owner
        && account.view == AccountView::Full
        && account.original_len == Some(account.bytes.len()), "Whirlpool address, owner or RPC view invalid");
    let data = &account.bytes;
    ensure!(data.len() == 653 && data[..8] == WHIRLPOOL_DISC,
        "Whirlpool account size/discriminator invalid");
    let tick_spacing = i32::from(u16::from_le_bytes(data[41..43].try_into()?));
    let tick_current = i32::from_le_bytes(data[81..85].try_into()?);
    ensure!(tick_spacing > 0 && (-443636..=443636).contains(&tick_current),
        "Whirlpool tick index or spacing invalid");
    let mint_a = key(data,101)?;
    let mint_b = key(data,181)?;
    let vault_a = key(data,133)?;
    let vault_b = key(data,213)?;
    ensure!(expected.mint_a == Some(mint_a) && expected.mint_b == Some(mint_b)
        && mint_a != mint_b && vault_a != vault_b
        && [mint_a,mint_b,vault_a,vault_b].iter().all(|k| *k!=Pubkey::default()),
        "Whirlpool mint/vault identity differs from Registry");
    Ok(Layout {pool:account.address,mint_a,mint_b,vault_a,vault_b,tick_spacing,tick_current})
}
fn array_addresses(layout: Layout, owner: Pubkey, radius: i32) -> Vec<(i32,Pubkey)> {
    let width = layout.tick_spacing * 88;
    let center = layout.tick_current.div_euclid(width);
    // B->A starts in the next array if the next initializable tick crosses its boundary.
    // Fetch one extra upper neighbor so both directions can supply up to three arrays.
    let shifted = layout.tick_current + layout.tick_spacing >= (center + 1) * width;
    let upper_radius = if shifted { radius + 1 } else { radius };
    (-radius..=upper_radius).filter_map(|delta| {
        let index = center + delta;
        let start = index * width;
        // Orca uses a possibly out-of-range array beginning at the minimum tick.
        if start > 443636 || start + width <= -443636 { return None; }
        let address = Pubkey::find_program_address(
            &[b"tick_array",layout.pool.as_ref(),start.to_string().as_bytes()],&owner).0;
        Some((start,address))
    }).collect()
}
fn validate_array(account:&AccountData, pool:Pubkey, start:i32, owner:Pubkey) -> Result<&'static str> {
    ensure!(account.owner==owner && account.view==AccountView::Full
        && account.original_len==Some(account.bytes.len()),"Orca TickArray owner/RPC view mismatch");
    let b = &account.bytes;
    ensure!(b.len()>=44,"Orca TickArray truncated");
    ensure!(i32::from_le_bytes(b[8..12].try_into()?)==start,"Orca TickArray start index mismatch");
    if b[..8]==FIXED_DISC {
        ensure!(b.len()==FIXED_LEN && key(b,8+4+88*113)?==pool,
            "Orca fixed TickArray layout/Whirlpool invalid");
        Ok("fixed")
    } else if b[..8]==DYNAMIC_DISC {
        ensure!((DYNAMIC_MIN_LEN..=DYNAMIC_MAX_LEN).contains(&b.len())
            && key(b,12)?==pool,"Orca dynamic TickArray layout/Whirlpool invalid");
        let mut pos=60;
        for _ in 0..88 {
            let tag=*b.get(pos).context("truncated Orca dynamic tick tag")?;
            pos+=1;
            ensure!(tag<=1,"invalid Orca dynamic tick tag");
            if tag==1 {pos+=112;}
            ensure!(pos<=b.len(),"truncated Orca dynamic initialized tick");
        }
        ensure!(b[pos..].iter().all(|v| *v==0),
            "unexpected Orca dynamic TickArray trailing bytes");
        Ok("dynamic")
    } else { bail!("Orca TickArray discriminator invalid") }
}
fn decode_mint(account: &AccountData, spl:Pubkey, token_2022:Pubkey) -> Result<MintProof> {
    let is_2022 = if account.owner==spl {false}
        else if account.owner==token_2022 {true}
        else {bail!("unsupported Orca mint program {}",account.owner)};
    let b=&account.bytes;
    ensure!(b.len()>=82 && b[45]==1,"Orca mint state not initialized");
    let mut extensions=Vec::new();
    if is_2022 && b.len()>82 {
        ensure!(b.len()>=166 && b[82..165].iter().all(|v|*v==0) && b[165]==1,
            "invalid Orca Token-2022 mint extension framing");
        let mut i=166;
        while i<b.len() {
            if b[i..].iter().all(|v|*v==0) {break;}
            ensure!(b.len()-i>=4,"truncated Orca mint TLV");
            let kind=u16::from_le_bytes(b[i..i+2].try_into()?);
            let len=usize::from(u16::from_le_bytes(b[i+2..i+4].try_into()?));
            ensure!(kind!=0 && len<=b.len()-i-4,"invalid Orca mint TLV");
            extensions.push(kind);
            i+=4+len;
        }
    } else if !is_2022 {ensure!(b.len()==82,"invalid SPL mint account length");}
    extensions.sort_unstable();
    ensure!(!extensions.windows(2).any(|w|w[0]==w[1]),"duplicate Orca mint extensions");
    let unsupported=extensions.iter().copied().filter(|v|matches!(*v,4|9|14|16|24)).collect();
    Ok(MintProof {address:account.address,token_2022:is_2022,extensions,unsupported})
}
fn validate_vault(account:&AccountData,mint:Pubkey,token_program:Pubkey,pool:Pubkey)->Result<()> {
    ensure!(account.owner==token_program && account.bytes.len()>=165,
        "Orca vault token program or account length invalid");
    ensure!(key(&account.bytes,0)?==mint && key(&account.bytes,32)?==pool
        && account.bytes[108]==1,"Orca vault mint, authority or state invalid");
    Ok(())
}
fn validate_oracle(oracle:Option<&AccountData>,pool:Pubkey,owner:Pubkey)->Result<bool> {
    let Some(o)=oracle else {return Ok(false)};
    let system:Pubkey=SYSTEM.parse()?;
    if o.owner==system && o.bytes.is_empty() {return Ok(false);}
    ensure!(o.owner==owner && o.bytes.len()==ORACLE_LEN && o.bytes[..8]==ORACLE_DISC
        && key(&o.bytes,8)?==pool, "invalid Orca Oracle account");
    Ok(true)
}
fn read(rpc:&RpcHandle, addresses:Vec<Pubkey>, min_slot:Option<u64>)
    -> impl std::future::Future<Output=Result<(u64,Vec<Option<AccountData>>)>> + '_ {
    async move {
        let count=addresses.len();
        let expected=addresses.clone();
        let result=rpc.call(RpcOperation::Multiple {
            addresses,view:AccountView::Full,min_context_slot:min_slot
        }).await?;
        let RpcAnswer::Multiple{slot,accounts}=result.answer
            else {bail!("unexpected Orca RPC Multiple answer")};
        ensure!(accounts.len()==count && min_slot.is_none_or(|v|slot>=v),
            "Orca RPC cardinality or slot mismatch");
        ensure!(accounts.iter().zip(&expected).all(|(a,expected)|
            a.as_ref().is_none_or(|item| item.address==*expected
                && item.view==AccountView::Full
                && item.original_len==Some(item.bytes.len()))),
            "Orca RPC address or full-account identity mismatch");
        Ok((slot,accounts))
    }
}
fn result(pool:Pubkey,status:CompletionStatus,slot:Option<u64>,
    reason:Option<String>,evidence:Option<Arc<CompletionEvidence>>,
    mints:Option<[MintProof;2]>,manifest:Option<Arc<ExecutionManifest>>) -> CompletionResult {
    CompletionResult {pool,status,validated_slot:slot,reason,evidence,mints,manifest}
}
fn retry(pool:Pubkey,reason:impl Into<String>)->CompletionResult {
    result(pool,CompletionStatus::Retryable,None,Some(reason.into()),None,None,None)
}
fn incomplete(pool:Pubkey,slot:u64,reason:impl Into<String>)->CompletionResult {
    result(pool,CompletionStatus::Incomplete,Some(slot),Some(reason.into()),None,None,None)
}
fn make_manifest(l:Layout,slot:u64,arrays_a:&[Pubkey],arrays_b:&[Pubkey],
    owner:Pubkey)->ExecutionManifest {
    let ro=|address|AccountMeta{address,writable:false};
    let rw=|address|AccountMeta{address,writable:true};
    let memo:Pubkey=MEMO.parse().expect("known Memo program ID");
    let oracle=Pubkey::find_program_address(&[b"oracle",l.pool.as_ref()],&owner).0;
    let direction=|base:Pubkey,reverse:bool,arrays:&[Pubkey]|{
        let (x_vault,base_vault)=if reverse {(l.vault_b,l.vault_a)}
            else {(l.vault_a,l.vault_b)};
        let mut accounts=vec![ro(owner),ro(base),ro(memo),rw(l.pool),
            rw(oracle),rw(x_vault),rw(base_vault)];
        accounts.extend(arrays.iter().copied().map(rw));
        Arc::<[AccountMeta]>::from(accounts)
    };
    ExecutionManifest {
        pool:l.pool,mint_a:l.mint_a,mint_b:l.mint_b,validated_slot:slot,
        // base_a = B->A: upward; base_b = A->B: downward.
        base_a:direction(l.mint_a,true,arrays_a),base_b:direction(l.mint_b,false,arrays_b),
    }
}
fn finish(pool:&PoolState,initial:Layout,slot:u64,
    lookup:&HashMap<Pubkey,Option<AccountData>>,owner:Pubkey,spl:Pubkey,token_2022:Pubkey,
    radius:i32)->CompletionResult {
    let at=|k:Pubkey|lookup.get(&k).and_then(Option::as_ref);
    let Some(account)=at(pool.address) else {return retry(pool.address,"Orca parent pool missing in final RPC snapshot")};
    let layout=match decode_pool(account,pool,owner) {
        Ok(l)=>l,Err(e)=>return incomplete(pool.address,slot,format!("Orca final Whirlpool: {e:#}")),
    };
    if layout.tick_current!=initial.tick_current || layout.tick_spacing!=initial.tick_spacing
        || layout.vault_a!=initial.vault_a || layout.vault_b!=initial.vault_b {
        return retry(pool.address,"Orca Whirlpool state changed during completion");
    }
    let (Some(a),Some(b),Some(va),Some(vb))=(at(layout.mint_a),at(layout.mint_b),
        at(layout.vault_a),at(layout.vault_b)) else {
        return retry(pool.address,"Orca mint/vault RPC account unavailable")
    };
    let mint_a=match decode_mint(a,spl,token_2022) {
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Orca mint A: {e:#}"))
    };
    let mint_b=match decode_mint(b,spl,token_2022) {
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Orca mint B: {e:#}"))
    };
    for (account,mint,program) in [(va,layout.mint_a,a.owner),(vb,layout.mint_b,b.owner)] {
        if let Err(e)=validate_vault(account,mint,program,layout.pool) {
            return incomplete(pool.address,slot,format!("Orca vault: {e:#}"))
        }
    }
    let oracle=Pubkey::find_program_address(&[b"oracle",layout.pool.as_ref()],&owner).0;
    let oracle_present=match validate_oracle(at(oracle),layout.pool,owner) {
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Orca Oracle: {e:#}"))
    };
    let expected=array_addresses(layout,owner,radius);
    let width=layout.tick_spacing*88;
    let center=layout.tick_current.div_euclid(width)*width;
    let mut present=HashMap::<i32,(Pubkey,&'static str)>::new();
    for (start,address) in expected {
        if let Some(account)=at(address) {
            match validate_array(account,layout.pool,start,owner) {
                Ok(kind)=>{present.insert(start,(address,kind));},
                Err(e)=>return incomplete(pool.address,slot,
                    format!("Orca TickArray start={start}: {e:#}")),
            }
        }
    }
    // Match Orca sparse_swap::get_start_tick_indexes for both directions.
    // A->B starts at center; B->A starts one array higher at the upper boundary.
    let shifted = layout.tick_current + layout.tick_spacing >= center + width;
    let up_center = if shifted { center + width } else { center };
    if !present.contains_key(&center) {
        return incomplete(pool.address,slot,"Orca A->B first TickArray not initialized");
    }
    if !present.contains_key(&up_center) {
        return incomplete(pool.address,slot,"Orca B->A first TickArray not initialized");
    }
    // Keep only consecutive initialized arrays starting at each direction's first array.
    let down=(0..=radius).map(|distance|center-distance*width)
        .take_while(|start|present.contains_key(start)).collect::<Vec<_>>();
    let up=(0..=radius).map(|distance|up_center+distance*width)
        .take_while(|start|present.contains_key(start)).collect::<Vec<_>>();
    let mut selected=down.iter().chain(up.iter()).copied().collect::<Vec<_>>();
    selected.sort_unstable();
    selected.dedup();
    // V10 `base_a` uses mint A as the *output/base*, hence swaps B->A
    // and traverses upward; `base_b` swaps A->B and traverses downward.
    let arrays_a=up.iter().map(|start|present[start].0).collect::<Vec<_>>();
    let arrays_b=down.iter().map(|start|present[start].0).collect::<Vec<_>>();
    let mints=[mint_a,mint_b];
    let ev=Arc::new(CompletionEvidence{
        kind:"orca_whirlpool_tick_arrays".into(),version:3,
        payload:Arc::new(json!({
            "program":owner.to_string(),"oracle":oracle.to_string(),
            "oracle_initialized":oracle_present,"tick_spacing":layout.tick_spacing,
            "tick_current":layout.tick_current,"radius":radius,
            "vault_a":layout.vault_a.to_string(),"vault_b":layout.vault_b.to_string(),
            "selected":selected.iter().map(|start|json!({
                "start":start,"address":present[start].0.to_string(),"layout":present[start].1
            })).collect::<Vec<_>>(),
            "base_a_start_ticks":up,
            "base_b_start_ticks":down,
            "token_program_a":a.owner.to_string(),"token_program_b":b.owner.to_string()
        }))
    });
    if mints.iter().any(|m|!m.unsupported.is_empty()) {
        return result(pool.address,CompletionStatus::Incomplete,Some(slot),
            Some(format!("V10 unsupported Token-2022 mint extensions: {:?} {:?}",
                mints[0].unsupported,mints[1].unsupported)),Some(ev),Some(mints),None);
    }
    let manifest=make_manifest(layout,slot,&arrays_a,&arrays_b,owner);
    result(pool.address,CompletionStatus::Prepared,Some(slot),None,
        Some(ev),Some(mints),Some(Arc::new(manifest)))
}
async fn process(rpc:&RpcHandle,pools:Vec<PoolState>,settings:Settings)->Vec<CompletionResult> {
    let owner:Pubkey=PROGRAM.parse().expect("known Whirlpool program ID");
    let spl:Pubkey=SPL.parse().expect("known SPL token program ID");
    let token_2022:Pubkey=TOKEN_2022.parse().expect("known Token-2022 program ID");
    let mut results=vec![None;pools.len()];
    let (first_slot,first)=match read(rpc,pools.iter().map(|p|p.address).collect(),None).await {
        Ok(v)=>v,Err(e)=>return pools.iter().map(|p|retry(p.address,format!("Orca initial RPC: {e:#}"))).collect()
    };
    let mut pending=Vec::new();
    let mut addresses=Vec::new();
    let mut unique=HashSet::new();
    for (i,pool) in pools.iter().enumerate() {
        let Some(account)=first[i].as_ref() else {
            results[i]=Some(retry(pool.address,"Orca Whirlpool account not found"));continue
        };
        match decode_pool(account,pool,owner) {
            Ok(layout)=>{
                let oracle=Pubkey::find_program_address(&[b"oracle",layout.pool.as_ref()],&owner).0;
                let keys=[layout.pool,layout.mint_a,layout.mint_b,layout.vault_a,layout.vault_b,oracle];
                for key in keys.into_iter().chain(array_addresses(layout,owner,settings.radius)
                    .into_iter().map(|(_,address)|address)) {
                    if unique.insert(key) {addresses.push(key);}
                }
                pending.push((i,layout));
            }
            Err(e)=>results[i]=Some(incomplete(pool.address,first_slot,format!("Orca Whirlpool: {e:#}")))
        }
    }
    if !pending.is_empty() {
        if addresses.len()>settings.account_batch_size {
            for (i,_) in pending {
                results[i]=Some(retry(pools[i].address,"Orca batch exceeds configured RPC size"));
            }
        } else {
            match read(rpc,addresses.clone(),Some(first_slot)).await {
                Ok((slot,final_accounts))=>{
                    let lookup=addresses.into_iter().zip(final_accounts).collect::<HashMap<_,_>>();
                    for (i,layout) in pending {
                        results[i]=Some(finish(&pools[i],layout,slot,&lookup,owner,spl,token_2022,settings.radius));
                    }
                }
                Err(e)=>for (i,_) in pending {
                    results[i]=Some(retry(pools[i].address,format!("Orca final RPC: {e:#}")));
                },
            }
        }
    }
    results.into_iter().map(|v|v.expect("each Orca pool has a result")).collect()
}


// Evidence v1 had reversed directions; v2 missed the B->A boundary shift
// and accepted frozen vaults. Revalidate persisted Prepared v1/v2 proofs.
fn needs_evidence_upgrade(pool: &PoolState) -> bool {
    pool.venue == Venue::OrcaWhirlpool
        && pool.completion_state == CompletionState::Prepared
        && pool.completion_evidence.as_ref().is_some_and(|e|
            e.kind == "orca_whirlpool_tick_arrays" && e.version < 3)
}

#[derive(Default)]
struct Scheduler {
    candidates:HashMap<Pubkey,PoolState>,
    attempts:HashMap<Pubkey,u8>,
    retry_after:HashMap<Pubkey,Instant>,
}
impl Scheduler {
    fn update(&mut self,pool:&PoolState,min_tvl:f64) {
        if pool.venue!=Venue::OrcaWhirlpool {return;}
        if needs_evidence_upgrade(pool) {
            // Old Prepared manifests must be rechecked even without fresh REST TVL.
            self.candidates.insert(pool.address,pool.clone());
        } else if pool.completion_state==CompletionState::Prepared {
            self.candidates.remove(&pool.address);
            self.retry_after.remove(&pool.address);
        } else if pool.mint_a.is_none() || pool.mint_b.is_none()
            || !pool.tvl_usd.is_some_and(|v|v.is_finite() && v>=min_tvl) {
            self.candidates.remove(&pool.address);
        } else {
            self.candidates.insert(pool.address,pool.clone());
        }
    }
    fn eligible(&self,pool:&PoolState,now:Instant)->Option<u8> {
        match self.attempts.get(&pool.address).copied() {
            None=>match pool.completion_state {
                CompletionState::Prepared if needs_evidence_upgrade(pool)=>Some(1),
                CompletionState::Unknown=>Some(2),
                CompletionState::Incomplete if !pool.completion_reason.as_deref()
                    .is_some_and(|reason|reason.contains("V10 unsupported Token-2022 mint extensions"))=>Some(3),
                _=>None,
            },
            Some(1) if self.retry_after.get(&pool.address).is_some_and(|t|*t<=now)=>Some(4),
            _=>None,
        }
    }
    fn next(&self,max:usize,now:Instant)->(u8,Vec<PoolState>) {
        let mut tier=0;
        let mut candidates=Vec::new();
        for pool in self.candidates.values() {
            let Some(level)=self.eligible(pool,now) else {continue};
            if tier==0 || level<tier {tier=level;candidates.clear();}
            if tier==level {candidates.push(pool.clone());}
        }
        candidates.sort_unstable_by(|a,b|
            b.tvl_usd.unwrap_or_default().total_cmp(&a.tvl_usd.unwrap_or_default())
                .then_with(||a.address.to_bytes().cmp(&b.address.to_bytes())));
        candidates.truncate(max);
        (tier,candidates)
    }
    fn pending_retry(&self)->bool {
        self.retry_after.iter().any(|(pool,_)|
            self.candidates.contains_key(pool) && self.attempts.get(pool)==Some(&1))
    }
    fn counts(&self,now:Instant)->(usize,usize,usize,usize) {
        let mut upgrade=0;let mut unknown=0;let mut incomplete=0;let mut retry=0;
        for pool in self.candidates.values() {
            match self.eligible(pool,now) {
                Some(1)=>upgrade+=1,Some(2)=>unknown+=1,Some(3)=>incomplete+=1,
                Some(4)=>retry+=1,_=>(),
            }
        }
        (upgrade,unknown,incomplete,retry)
    }
}
pub async fn run(mut input:broadcast::Receiver<Arc<PoolUpdateBatch>>,
    source_done:watch::Receiver<bool>,rpc:RpcHandle,settings:Settings,
    output:broadcast::Sender<Arc<CompletionResultBatch>>)->Result<()> {
    let mut scheduler=Scheduler::default();
    let mut tasks=JoinSet::new();
    let mut source_open=true;
    let mut seen=HashSet::new();
    let mut registry_updates=0_usize;
    let mut attempts=0_usize;
    let mut prepared=0_usize;
    let mut incomplete=0_usize;
    let mut retryable=0_usize;
    let mut retries=0_usize;
    let mut pulse=tokio::time::interval(Duration::from_secs(5));
    pulse.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let started=Instant::now();
    crate::log_event!(Info,"orca completion started min_tvl_usd={} limit={} radius={} parallel={} pools_per_batch={}",
        settings.min_tvl_usd,settings.limit,settings.radius,
        settings.parallel_batches,settings.pools_per_batch());
    loop {
        while tasks.len()<settings.parallel_batches
            && (settings.limit==0 || attempts<settings.limit) {
            let remaining=if settings.limit==0 {settings.pools_per_batch()}
                else {settings.pools_per_batch().min(settings.limit-attempts)};
            if remaining==0 {break;}
            let (tier,selected)=scheduler.next(remaining,Instant::now());
            if selected.is_empty(){break;}
            for pool in &selected {
                let next=scheduler.attempts.get(&pool.address).copied().unwrap_or(0)+1;
                scheduler.attempts.insert(pool.address,next);
                scheduler.retry_after.remove(&pool.address);
            }
            attempts+=selected.len();
            if tier==4 {retries+=selected.len();}
            let rpc=rpc.clone();
            tasks.spawn(async move {(tier,process(&rpc,selected,settings).await)});
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
                    registry_updates+=batch.pools.len();
                    for pool in &batch.pools {
                        if pool.venue==Venue::OrcaWhirlpool {seen.insert(pool.address);}
                        scheduler.update(pool,settings.min_tvl_usd);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n))=>
                    bail!("Orca completion lost {n} Registry batches"),
                Err(broadcast::error::RecvError::Closed)=>source_open=false,
            },
            done_batch=tasks.join_next(),if !tasks.is_empty()=>{
                let (tier,results)=done_batch.context("Orca completion missing task")?
                    .context("Orca completion task panicked")?;
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
                                scheduler.retry_after.insert(item.pool,Instant::now()+RETRY_AFTER);
                            }
                        }
                    }
                }
                prepared+=p;incomplete+=i;retryable+=r;
                crate::log_event!(Debug,"orca completion batch tier={tier} pools={} prepared={p} incomplete={i} retryable={r} total_attempts={attempts} elapsed_ms={}",
                    results.len(),started.elapsed().as_millis());
                for item in results.iter().filter(|v|v.status==CompletionStatus::Prepared).take(2) {
                    if let Some(manifest)=&item.manifest {
                        crate::log_event!(Debug,"orca manifest pool={} slot={} a_accounts={} b_accounts={} base_a={:?} base_b={:?}",
                            manifest.pool,manifest.validated_slot,manifest.base_a.len(),
                            manifest.base_b.len(),
                            manifest.base_a.iter().map(|a|format!("{}:{}",a.address,if a.writable {"rw"} else {"ro"}))
                                .collect::<Vec<_>>(),
                            manifest.base_b.iter().map(|a|format!("{}:{}",a.address,if a.writable {"rw"} else {"ro"}))
                                .collect::<Vec<_>>());
                    }
                }
                for item in results.iter().filter(|v|v.status!=CompletionStatus::Prepared).take(3) {
                    crate::log_event!(Debug,"orca completion result pool={} status={:?} reason={}",
                        item.pool,item.status,item.reason.as_deref().unwrap_or("-"));
                }
                output.send(Arc::new(CompletionResultBatch{source:SOURCE,results}))
                    .map_err(|_|anyhow!("Orca completion has no Registry consumer"))?;
            },
            _=pulse.tick()=>{
                let (upgrade,a,b,c)=scheduler.counts(Instant::now());
                crate::log_event!(Debug,"orca completion backlog seen={} evidence_upgrade={upgrade} unknown={a} incomplete={b} retry_due={c} candidates={} active_batches={} attempts={attempts} prepared={prepared} incomplete_total={incomplete} retryable={retryable} registry_updates={registry_updates}",
                    seen.len(),scheduler.candidates.len(),tasks.len());
            }
        }
    }
    let (upgrade,a,b,c)=scheduler.counts(Instant::now());
    crate::log_event!(Info,"orca completion complete pools_seen={} registry_updates={registry_updates} attempts={attempts} retries={retries} prepared={prepared} incomplete={incomplete} retryable={retryable} left_evidence_upgrade={upgrade} left_unknown={a} left_incomplete={b} left_retry_due={c} elapsed_ms={}",
        seen.len(),started.elapsed().as_millis());
    Ok(())
}
