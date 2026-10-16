//! PumpSwap V10 completion: Registry candidates -> shared multi-provider RPC -> execution manifests.
//! Reads current pool/mint/vault/global state in batches and verifies fee recipients;
//! no perpetual TVL or vault polling in this checkpoint.
use std::{collections::{HashMap, HashSet}, env, sync::Arc, time::Duration};
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast,watch}, task::JoinSet, time::{Instant,MissedTickBehavior}};
use crate::{
    core::{AccountMeta,CompletionEvidence,CompletionResult,CompletionResultBatch,
        CompletionState,CompletionStatus,ExecutionManifest,MintProof,PoolState,PoolUpdateBatch,Venue},
    workers::rpc_transport::{AccountData,AccountView,RpcAnswer,RpcHandle,RpcOperation},
};

const SOURCE:&str="pumpswap_v10_completion";
const PUMP:&str="pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const PUMP_FEE:&str="pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";
const TOKEN:&str="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022:&str="TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const ASSOCIATED:&str="ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const SOL:&str="So11111111111111111111111111111111111111112";
const USDC:&str="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const DISC:[u8;8]=[241,154,109,4,17,177,109,188];
const RETRY_COOLDOWN:Duration=Duration::from_secs(60);

#[derive(Clone,Copy)]
pub struct Settings {
    pub limit:usize,
    wallet:Pubkey,
    account_batch_size:usize,
    parallel_batches:usize,
}
impl Settings {
    pub fn from_env() -> Result<Self> {
        let wallet:Pubkey=env::var("PUMPSWAP_V10_WALLET")
            .context("PUMPSWAP_V10_WALLET must be the bot execution wallet public key")?
            .parse().context("invalid PUMPSWAP_V10_WALLET")?;
        ensure!(wallet!=Pubkey::default(),"PumpSwap V10 wallet cannot be zero");
        let account_batch_size:usize=env::var("RPC_ACCOUNT_BATCH_SIZE")
            .unwrap_or_else(|_|"100".into()).parse()?;
        ensure!((6..=100).contains(&account_batch_size),
            "PumpSwap requires RPC_ACCOUNT_BATCH_SIZE in 6..=100");
        Ok(Self {limit:0,wallet,account_batch_size,parallel_batches:8})
    }
    fn pools_per_batch(self)->usize {
        10_usize.min((self.account_batch_size-1)/5)
    }
}
#[derive(Clone,Copy)]
struct Layout {
    address:Pubkey,
    mints:[Pubkey;2],
    vaults:[Pubkey;2],
    creator:Pubkey,
    mayhem:bool,
    cashback:bool,
}
fn key(d:&[u8],offset:usize)->Result<Pubkey> {
    Ok(Pubkey::new_from_array(d.get(offset..offset+32)
        .context("truncated PumpSwap key")?.try_into()?))
}
fn full(a:&AccountData)->bool {
    a.view==AccountView::Full && a.original_len==Some(a.bytes.len())
}
fn pool_layout(a:&AccountData,p:&PoolState,program:Pubkey)->Result<Layout> {
    ensure!(a.address==p.address && a.owner==program && full(a)
        && a.bytes.len()>=245 && a.bytes[..8]==DISC,
        "PumpSwap pool owner/discriminator/full state mismatch");
    let d=&a.bytes;
    let mints=[key(d,43)?,key(d,75)?];
    let vaults=[key(d,139)?,key(d,171)?];
    ensure!(p.mint_a==Some(mints[0]) && p.mint_b==Some(mints[1])
        && mints[0]!=mints[1] && mints.iter().all(|m|*m!=Pubkey::default())
        && vaults[0]!=vaults[1] && vaults.iter().all(|v|*v!=Pubkey::default()),
        "PumpSwap pool identity mismatch");
    // V10 PumpKeys::from_state uses coin_creator at offset 211, NOT the pool PDA creator at 11.
    Ok(Layout {address:a.address,mints,vaults,creator:key(d,211)?,
        mayhem:d[243]!=0,cashback:d[244]!=0})
}
fn proof(a:&AccountData,spl:Pubkey,token_2022:Pubkey)->Result<MintProof> {
    ensure!(full(a) && a.bytes.len()>=82 && a.bytes[45]==1,
        "PumpSwap mint not initialized or truncated");
    let is_2022=if a.owner==spl {false} else if a.owner==token_2022 {true}
        else {bail!("PumpSwap unsupported mint program")};
    let d=&a.bytes;
    let mut extensions=Vec::new();
    if is_2022 && d.len()>82 {
        ensure!(d.len()>=166 && d[82..165].iter().all(|v|*v==0)
            && d[165]==1,"PumpSwap invalid Token-2022 mint frame");
        let mut pos=166;
        while pos<d.len() {
            let tail=&d[pos..];
            if tail.iter().all(|v|*v==0) {break;}
            ensure!(tail.len()>=4,"PumpSwap truncated mint TLV");
            let kind=u16::from_le_bytes(tail[..2].try_into()?);
            let n=usize::from(u16::from_le_bytes(tail[2..4].try_into()?));
            if kind==0 && n==0 {
                ensure!(tail[4..].iter().all(|v|*v==0),"PumpSwap invalid mint TLV trailer");
                break;
            }
            ensure!(kind!=0 && n<=tail.len()-4,"PumpSwap invalid mint TLV");
            extensions.push(kind);
            pos+=4+n;
        }
    } else if !is_2022 {
        ensure!(d.len()==82,"PumpSwap invalid legacy mint length");
    }
    extensions.sort_unstable();
    ensure!(extensions.windows(2).all(|p|p[0]!=p[1]),"PumpSwap duplicate mint TLV");
    let unsupported=extensions.iter().copied()
        .filter(|kind|matches!(*kind,4|9|14|16|24)).collect();
    Ok(MintProof{address:a.address,token_2022:is_2022,extensions,unsupported})
}
fn vault(a:&AccountData,mint:Pubkey,program:Pubkey,pool:Pubkey)->Result<()> {
    ensure!(full(a) && a.owner==program && a.bytes.len()>=165
        && key(&a.bytes,0)?==mint && key(&a.bytes,32)?==pool
        && a.bytes[108]==1,"PumpSwap vault token mint/authority/state invalid");
    if program.to_string()==TOKEN_2022 && a.bytes.len()>165 {
        ensure!(a.bytes[165]==2,"PumpSwap Token-2022 vault account type invalid");
    }
    Ok(())
}
fn pda(program:Pubkey,seeds:&[&[u8]])->Pubkey {
    Pubkey::find_program_address(seeds,&program).0
}
fn ata(owner:Pubkey,mint:Pubkey,token_program:Pubkey,associated:Pubkey)->Pubkey {
    pda(associated,&[owner.as_ref(),token_program.as_ref(),mint.as_ref()])
}
fn global_key(program:Pubkey)->Pubkey {
    pda(program,&[b"global_config"])
}
fn fee_candidates(global:&[u8],mayhem:bool)->Result<Vec<Pubkey>> {
    let offsets:Vec<usize>=if mayhem {
        std::iter::once(385).chain((0..7).map(|i|418+i*32)).collect()
    } else {
        (0..8).map(|i|57+i*32).collect()
    };
    let mut result=Vec::new();
    for offset in offsets {
        let recipient=key(global,offset)?;
        if recipient!=Pubkey::default() && !result.contains(&recipient) {
            result.push(recipient);
        }
    }
    ensure!(!result.is_empty(),"PumpSwap global config has no fee recipients");
    Ok(result)
}
async fn read(rpc:&RpcHandle,addresses:Vec<Pubkey>,min_slot:Option<u64>)
    ->Result<(u64,HashMap<Pubkey,Option<AccountData>>)> {
    ensure!(!addresses.is_empty(),"empty PumpSwap account batch");
    let ids=addresses.clone();
    let response=rpc.call(RpcOperation::Multiple{
        addresses,view:AccountView::Full,min_context_slot:min_slot}).await?;
    let RpcAnswer::Multiple{slot,accounts}=response.answer else {
        bail!("PumpSwap RPC returned non-Multiple result");
    };
    ensure!(accounts.len()==ids.len() && min_slot.is_none_or(|min|slot>=min)
        && accounts.iter().zip(&ids).all(|(a,id)|
            a.as_ref().is_none_or(|a|a.address==*id && full(a))),
        "PumpSwap RPC result cardinality/address/full snapshot mismatch");
    println!("pumpswap completion rpc provider={} slot={slot} requested={} present={}",
        response.provider,ids.len(),accounts.iter().filter(|a|a.is_some()).count());
    Ok((slot,ids.into_iter().zip(accounts).collect()))
}
fn result(pool:Pubkey,status:CompletionStatus,slot:Option<u64>,
    reason:Option<String>,evidence:Option<Arc<CompletionEvidence>>,
    mints:Option<[MintProof;2]>,manifest:Option<Arc<ExecutionManifest>>)->CompletionResult {
    CompletionResult{pool,status,validated_slot:slot,reason,evidence,mints,manifest}
}
fn retry(pool:Pubkey,reason:impl Into<String>)->CompletionResult {
    result(pool,CompletionStatus::Retryable,None,Some(reason.into()),None,None,None)
}
fn incomplete(pool:Pubkey,slot:u64,reason:impl Into<String>)->CompletionResult {
    result(pool,CompletionStatus::Incomplete,Some(slot),Some(reason.into()),None,None,None)
}
fn block(l:Layout,base:Pubkey,x:Pubkey,quote_program:Pubkey,
    recipient:Pubkey,buyback:Pubkey,wallet:Pubkey)->Result<Arc<[AccountMeta]>> {
    let program:Pubkey=PUMP.parse()?;
    let fee_program:Pubkey=PUMP_FEE.parse()?;
    let associated:Pubkey=ASSOCIATED.parse()?;
    let quote=l.mints[1];
    let base_vault=if base==l.mints[0] {l.vaults[0]} else {l.vaults[1]};
    let x_vault=if x==l.mints[0] {l.vaults[0]} else {l.vaults[1]};
    let volume=pda(program,&[b"user_volume_accumulator",wallet.as_ref()]);
    let creator_vault=pda(program,&[b"creator_vault",l.creator.as_ref()]);
    let fee_config=pda(fee_program,&[b"fee_config",program.as_ref()]);
    let ro=|address|AccountMeta{address,writable:false};
    let rw=|address|AccountMeta{address,writable:true};
    // Public Cetipoo V10 Pump legacy account block. Use v1 in all cases:
    // no v2 buyback-ATA initialization assumption.
    let mut accounts=vec![
        ro(program),ro(base),ro(global_key(program)),
        ro(pda(program,&[b"__event_authority"])),
        ro(recipient),rw(l.address),rw(x_vault),rw(base_vault),
        rw(ata(recipient,quote,quote_program,associated)),
        rw(ata(creator_vault,quote,quote_program,associated)),
        ro(creator_vault),
        ro(pda(program,&[b"global_volume_accumulator"])),
        rw(volume),ro(fee_config),ro(fee_program),
    ];
    if l.cashback {
        accounts.push(rw(ata(volume,quote,quote_program,associated)));
        accounts.push(rw(volume));
    }
    if l.creator!=Pubkey::default() {
        accounts.push(ro(pda(program,&[b"pool-v2",l.mints[0].as_ref()])));
    }
    accounts.push(ro(buyback));
    accounts.push(rw(ata(buyback,quote,quote_program,associated)));
    Ok(Arc::from(accounts))
}
async fn process(rpc:&RpcHandle,pools:Vec<PoolState>,settings:Settings)
    ->Vec<CompletionResult> {
    let program:Pubkey=PUMP.parse().expect("PumpSwap program ID");
    let spl:Pubkey=TOKEN.parse().expect("Token program");
    let token_2022:Pubkey=TOKEN_2022.parse().expect("Token-2022 program");
    let associated:Pubkey=ASSOCIATED.parse().expect("Associated token program");
    let global_addr=global_key(program);
    // Pool is revalidated in the SAME request as mints, vaults and global state:
    // no separate per-pool RPC call and no trust in a stale discovery snapshot.
    let mut addresses=Vec::new();
    let mut used=HashSet::new();
    for address in std::iter::once(global_addr)
        .chain(pools.iter().map(|p|p.address)) {
        if used.insert(address) {addresses.push(address);}
    }
    // Phase 1: batched pool + global state; mint/vault addresses are not in
    // Registry, so a second batched account request is needed to hydrate them.
    let (pool_slot,first)=match read(rpc,addresses,None).await {
        Ok(v)=>v,Err(e)=>return pools.iter().map(|p|retry(p.address,
            format!("PumpSwap pool/global RPC: {e:#}"))).collect()
    };
    let Some(global)=first.get(&global_addr).and_then(Option::as_ref) else {
        return pools.iter().map(|p|retry(p.address,"PumpSwap GlobalConfig unavailable")).collect();
    };
    if global.owner!=program || global.bytes.len()<675 {
        return pools.iter().map(|p|incomplete(p.address,pool_slot,
            "PumpSwap GlobalConfig invalid owner or too short")).collect();
    }
    let global_bytes=global.bytes.clone();
    let buyback=key(&global_bytes,643).unwrap_or_default();
    let mut layouts=Vec::new();
    let mut results=HashMap::new();
    for pool in &pools {
        match first.get(&pool.address).and_then(Option::as_ref) {
            None=>{results.insert(pool.address,retry(pool.address,"PumpSwap pool account missing"));}
            Some(account)=>match pool_layout(account,pool,program) {
                Ok(layout)=>layouts.push((pool.clone(),layout)),
                Err(e)=>{results.insert(pool.address,incomplete(pool.address,pool_slot,
                    format!("PumpSwap pool: {e:#}")));}
            }
        }
    }
    if layouts.is_empty() {
        return pools.iter().map(|p|results.remove(&p.address)
            .expect("all PumpSwap pools have result")).collect();
    }
    let mut ids=Vec::new();
    let mut seen=HashSet::new();
    for (_,l) in &layouts {
        for address in l.mints.into_iter().chain(l.vaults) {
            if seen.insert(address){ids.push(address);}
        }
    }
    // Bound groups by a provider's configured account limit; preserve per-pool
    // identity even when the same mint appears in multiple pools.
    let mut lookup=HashMap::new();
    let mut last_slot=pool_slot;
    for chunk in ids.chunks(settings.account_batch_size) {
        match read(rpc,chunk.to_vec(),Some(pool_slot)).await {
            Ok((slot,accounts))=>{last_slot=last_slot.max(slot);lookup.extend(accounts);}
            Err(e)=>return pools.iter().map(|p|retry(p.address,
                format!("PumpSwap mint/vault RPC: {e:#}"))).collect(),
        }
    }
    // Phase 3: fee recipient ATA availability for each quote mint and mayhem mode.
    let mut fees=Vec::new();
    let mut fee_seen=HashSet::new();
    let mut per_pool=HashMap::<Pubkey,Vec<Pubkey>>::new();
    for (pool,l) in &layouts {
        let Some(quote_acc)=lookup.get(&l.mints[1]).and_then(Option::as_ref) else {
            results.insert(pool.address,retry(pool.address,"PumpSwap quote mint missing"));
            continue;
        };
        let candidates=match fee_candidates(&global_bytes,l.mayhem) {
            Ok(v)=>v,Err(e)=>{
                results.insert(pool.address,incomplete(pool.address,pool_slot,
                    format!("PumpSwap fee recipients: {e:#}")));
                continue;
            }
        };
        for recipient in &candidates {
            let fee_ata=ata(*recipient,l.mints[1],quote_acc.owner,associated);
            if fee_seen.insert(fee_ata){fees.push(fee_ata);}
        }
        per_pool.insert(pool.address,candidates);
    }
    let mut fee_accounts=HashMap::new();
    for chunk in fees.chunks(settings.account_batch_size) {
        match read(rpc,chunk.to_vec(),Some(pool_slot)).await {
            Ok((slot,found))=>{last_slot=last_slot.max(slot);fee_accounts.extend(found);}
            Err(e)=>return pools.iter().map(|p|retry(p.address,
                format!("PumpSwap fee account RPC: {e:#}"))).collect(),
        }
    }
    for (pool,l) in layouts {
        if results.contains_key(&pool.address) {continue;}
        let get=|address|lookup.get(&address).and_then(Option::as_ref);
        let (Some(m0),Some(m1),Some(v0),Some(v1))=(
            get(l.mints[0]),get(l.mints[1]),get(l.vaults[0]),get(l.vaults[1])
        ) else {
            results.insert(pool.address,retry(pool.address,"PumpSwap mint or vault unavailable"));
            continue;
        };
        let checked=(||->Result<[MintProof;2]>{
            let p0=proof(m0,spl,token_2022)?;
            let p1=proof(m1,spl,token_2022)?;
            ensure!(p0.unsupported.is_empty() && p1.unsupported.is_empty(),
                "PumpSwap unsupported Token-2022 mint extensions");
            vault(v0,l.mints[0],m0.owner,l.address)?;
            vault(v1,l.mints[1],m1.owner,l.address)?;
            Ok([p0,p1])
        })();
        let proofs=match checked {
            Ok(v)=>v,Err(e)=>{
                results.insert(pool.address,incomplete(pool.address,last_slot,
                    format!("PumpSwap mint/vault validation: {e:#}")));continue;
            }
        };
        let Some(candidates)=per_pool.get(&pool.address) else {
            results.insert(pool.address,retry(pool.address,"PumpSwap fee recipient candidates missing"));
            continue;
        };
        // Prefer a checked, initialized fee ATA. Legacy V10 may create a missing
        // quote ATA itself, so choose first configured recipient if none exists.
        let recipient=candidates.iter().copied().find(|recipient|{
            let fee_ata=ata(*recipient,l.mints[1],m1.owner,associated);
            fee_accounts.get(&fee_ata).and_then(Option::as_ref).is_some_and(|a|
                full(a) && a.owner==m1.owner && a.bytes.len()>=165
                    && key(&a.bytes,0).ok()==Some(l.mints[1])
                    && key(&a.bytes,32).ok()==Some(*recipient)
                    && a.bytes[108]==1)
        }).unwrap_or(candidates[0]);
        let [a,b]=[l.mints[0],l.mints[1]];
        let built=(||->Result<ExecutionManifest>{
            Ok(ExecutionManifest{pool:l.address,mint_a:a,mint_b:b,
                validated_slot:last_slot,
                base_a:block(l,a,b,m1.owner,recipient,buyback,settings.wallet)?,
                base_b:block(l,b,a,m1.owner,recipient,buyback,settings.wallet)?})
        })();
        match built {
            Err(e)=>{
                results.insert(pool.address,incomplete(pool.address,last_slot,
                    format!("PumpSwap V10 accounts: {e:#}")));
            }
            Ok(manifest)=>{
                let evidence=Arc::new(CompletionEvidence{
                    kind:"pumpswap_v10_legacy".into(),version:1,
                    payload:Arc::new(json!({
                        "program":program.to_string(),"wallet":settings.wallet.to_string(),
                        "global_config":global_addr.to_string(),
                        "pool":l.address.to_string(),"base_mint":a.to_string(),
                        "quote_mint":b.to_string(),"base_vault":l.vaults[0].to_string(),
                        "quote_vault":l.vaults[1].to_string(),
                        "quote_token_program":m1.owner.to_string(),
                        "recipient":recipient.to_string(),"buyback":buyback.to_string(),
                        "creator":l.creator.to_string(),
                        "is_mayhem_mode":l.mayhem,"is_cashback_coin":l.cashback,
                        "pump_quote_is_base_a":false,"pump_quote_is_base_b":true,
                        "v10_pump_layout":"legacy","pool_snapshot_slot":pool_slot,
                        "fee_snapshot_slot":last_slot
                    }))
                });
                results.insert(pool.address,result(pool.address,
                    CompletionStatus::Prepared,Some(last_slot),None,
                    Some(evidence),Some(proofs),Some(Arc::new(manifest))));
            }
        }
    }
    pools.iter().map(|p|results.remove(&p.address)
        .expect("every PumpSwap pool has a result")).collect()
}
#[derive(Default)]
struct Scheduler {
    candidates:HashMap<Pubkey,PoolState>,
    attempts:HashMap<Pubkey,u8>,
    retry_after:HashMap<Pubkey,Instant>,
}
impl Scheduler {
    fn update(&mut self,p:&PoolState) {
        if p.venue!=Venue::PumpSwap {return;}
        if p.completion_state==CompletionState::Prepared {
            self.candidates.remove(&p.address);
            self.retry_after.remove(&p.address);
        } else if p.mint_a.is_some() && p.mint_b.is_some() {
            self.candidates.insert(p.address,p.clone());
        }
    }
    fn next(&self,max:usize,now:Instant)->Vec<PoolState> {
        let mut pools=self.candidates.values().filter(|p|{
            match self.attempts.get(&p.address).copied() {
                None=>true,
                Some(1)=>self.retry_after.get(&p.address).is_some_and(|t|*t<=now),
                _=>false,
            }
        }).cloned().collect::<Vec<_>>();
        pools.sort_unstable_by_key(|p|p.address.to_bytes());
        pools.truncate(max);
        pools
    }
    fn pending_retry(&self)->bool {
        self.retry_after.iter().any(|(key,_)|
            self.candidates.contains_key(key) && self.attempts.get(key)==Some(&1))
    }
}
pub async fn run(mut input:broadcast::Receiver<Arc<PoolUpdateBatch>>,
    source_done:watch::Receiver<bool>,rpc:RpcHandle,settings:Settings,
    output:broadcast::Sender<Arc<CompletionResultBatch>>)->Result<()> {
    let mut scheduler=Scheduler::default();
    let mut tasks=JoinSet::new();
    let mut source_open=true;
    let mut attempts=0_usize;
    let mut prepared=0_usize;
    let mut incomplete=0_usize;
    let mut retryable=0_usize;
    let mut pulse=tokio::time::interval(Duration::from_secs(5));
    pulse.set_missed_tick_behavior(MissedTickBehavior::Delay);
    println!("pumpswap completion started wallet={} limit={} batch={} parallel={}",
        settings.wallet,settings.limit,settings.pools_per_batch(),settings.parallel_batches);
    loop {
        while tasks.len()<settings.parallel_batches &&
            (settings.limit==0 || attempts<settings.limit) {
            let count=if settings.limit==0 {settings.pools_per_batch()}
                else {settings.pools_per_batch().min(settings.limit-attempts)};
            let selected=scheduler.next(count,Instant::now());
            if selected.is_empty() {break;}
            for pool in &selected {
                let n=scheduler.attempts.get(&pool.address).copied().unwrap_or(0)+1;
                scheduler.attempts.insert(pool.address,n);
                scheduler.retry_after.remove(&pool.address);
            }
            attempts+=selected.len();
            let rpc=rpc.clone();
            tasks.spawn(async move {process(&rpc,selected,settings).await});
        }
        let done=!source_open || *source_done.borrow();
        let stopped=settings.limit>0 && attempts>=settings.limit;
        if tasks.is_empty() && (stopped || (done && input.len()==0
            && !scheduler.pending_retry() && scheduler.next(1,Instant::now()).is_empty())) {
            break;
        }
        tokio::select! {
            event=input.recv(),if source_open=>match event {
                Ok(batch)=>for p in &batch.pools {scheduler.update(p);},
                Err(broadcast::error::RecvError::Lagged(n))=>
                    bail!("PumpSwap completion lost {n} Registry batches"),
                Err(broadcast::error::RecvError::Closed)=>source_open=false,
            },
            finished=tasks.join_next(),if !tasks.is_empty()=>{
                let results=finished.context("missing PumpSwap partition")?
                    .context("PumpSwap completion task panicked")?;
                for r in &results {
                    match r.status {
                        CompletionStatus::Prepared=>{
                            prepared+=1;
                            scheduler.candidates.remove(&r.pool);
                            scheduler.retry_after.remove(&r.pool);
                            if prepared<=3 {
                                if let Some(m)=&r.manifest {
                                    println!("pumpswap manifest pool={} slot={} base_a={} base_b={}",
                                        m.pool,m.validated_slot,m.base_a.len(),m.base_b.len());
                                }
                            }
                        }
                        CompletionStatus::Incomplete=>{incomplete+=1;}
                        CompletionStatus::Retryable=>{
                            retryable+=1;
                            if scheduler.attempts.get(&r.pool)==Some(&1) {
                                scheduler.retry_after.insert(r.pool,Instant::now()+RETRY_COOLDOWN);
                            }
                        }
                    }
                    if r.status!=CompletionStatus::Prepared {
                        println!("pumpswap completion result pool={} status={:?} reason={}",
                            r.pool,r.status,r.reason.as_deref().unwrap_or("-"));
                    }
                }
                println!("pumpswap completion batch results={} attempts={attempts} prepared={prepared} incomplete={incomplete} retryable={retryable}",
                    results.len());
                output.send(Arc::new(CompletionResultBatch{source:SOURCE,results}))
                    .map_err(|_|anyhow!("PumpSwap completion has no Registry receiver"))?;
            },
            _=pulse.tick()=>{
                println!("pumpswap completion backlog={} in_flight={} attempts={attempts} prepared={prepared} incomplete={incomplete} retryable={retryable}",
                    scheduler.candidates.len(),tasks.len());
            }
        }
    }
    println!("pumpswap completion complete attempts={attempts} prepared={prepared} incomplete={incomplete} retryable={retryable}");
    Ok(())
}
