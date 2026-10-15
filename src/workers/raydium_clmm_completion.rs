//! Raydium CLMM execution completion: Pool Registry -> RPC -> validated V10 manifests.
//! Protocol-owned decoding, TickArray/bitmap checks, directional accounts and evidence.
//! No direct SQL, REST, graph mutation, second RPC client or protocol logic in Core.

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

const SOURCE: &str = "raydium_clmm_execution_completion";
const PROGRAM: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const MEMO: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const POOL_DISC: [u8;8] = [247,237,227,245,215,195,222,70];
const CONFIG_DISC: [u8;8] = [218,244,33,104,203,203,43,111];
const OBS_DISC: [u8;8] = [122,174,197,53,129,9,165,132];
const BITMAP_DISC: [u8;8] = [60,150,36,219,97,128,139,153];
const ARRAY_DISC: [u8;8] = [192,155,85,205,49,249,129,42];
const POOL_LEN: usize = 1544;
const CONFIG_LEN: usize = 117;
const OBS_LEN: usize = 4483;
const BITMAP_LEN: usize = 1832;
const ARRAY_LEN: usize = 10240;
const ARRAY_RADIUS: i32 = 2; // Matches public V10 array_candidates max radius
const MIN_TVL_USD: f64 = 500.0;
const POOLS_PER_BATCH: usize = 7;
const PARALLEL_BATCHES: usize = 16;
const RETRY_COOLDOWN: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
pub struct Settings {
    pub limit: usize,
    account_batch_size: usize,
    parallel_batches: usize,
}
impl Settings {
    pub fn from_env() -> Result<Self> {
        // Reuse existing RPC transport settings. 13 = pool + 2 mints +
        // 2 vaults + config + observation + bitmap + 5 array candidates.
        let account_batch_size = env::var("RPC_ACCOUNT_BATCH_SIZE")
            .unwrap_or_else(|_| "100".into()).parse::<usize>()
            .context("invalid RPC_ACCOUNT_BATCH_SIZE")?;
        ensure!((13..=100).contains(&account_batch_size),
            "Raydium CLMM requires RPC_ACCOUNT_BATCH_SIZE in 13..=100");
        Ok(Self {limit:0, account_batch_size, parallel_batches:PARALLEL_BATCHES})
    }
    fn pools_per_batch(self) -> usize {
        POOLS_PER_BATCH.min(self.account_batch_size / 13)
    }
}

#[derive(Clone, Copy)]
struct Layout {
    pool: Pubkey,
    config: Pubkey,
    observation: Pubkey,
    bitmap: Pubkey,
    mints: [Pubkey;2],
    vaults: [Pubkey;2],
    decimals: [u8;2],
    tick_spacing: i32,
    tick_current: i32,
    status: u8,
    open_time: u64,
}
fn key(data:&[u8],offset:usize)->Result<Pubkey>{
    Ok(Pubkey::new_from_array(data.get(offset..offset+32)
        .context("truncated CLMM account key")?.try_into()?))
}
fn u64_at(data:&[u8],offset:usize)->Result<u64>{
    Ok(u64::from_le_bytes(data.get(offset..offset+8)
        .context("truncated CLMM u64")?.try_into()?))
}
fn full(account:&AccountData)->bool {
    account.view==AccountView::Full
        && account.original_len==Some(account.bytes.len())
}
fn decode_pool(account:&AccountData,expected:&PoolState,program:Pubkey)->Result<Layout>{
    ensure!(account.address==expected.address && account.owner==program && full(account)
        && account.bytes.len()==POOL_LEN && account.bytes[..8]==POOL_DISC,
        "Raydium CLMM PoolState owner/size/discriminator invalid");
    let d=&account.bytes;
    let config=key(d,9)?;
    let mints=[key(d,73)?,key(d,105)?];
    let vaults=[key(d,137)?,key(d,169)?];
    let observation=key(d,201)?;
    let decimals=[d[233],d[234]];
    let tick_spacing=i32::from(u16::from_le_bytes(d[235..237].try_into()?));
    let tick_current=i32::from_le_bytes(d[269..273].try_into()?);
    let status=d[389];
    let open_time=u64_at(d,1080)?;
    ensure!((1..=1000).contains(&tick_spacing) && (-443636..=443636).contains(&tick_current),
        "Raydium CLMM tick spacing or tick_current out of bounds");
    ensure!(status&16==0,"Raydium CLMM swaps disabled by pool status bit 4");
    ensure!(open_time<crate::core::now_unix_ms()?/1000,
        "Raydium CLMM pool swap open_time not reached");
    let (Some(a),Some(b))=(expected.mint_a,expected.mint_b) else {
        bail!("Raydium CLMM Registry mint identity absent");
    };
    ensure!((mints==[a,b]||mints==[b,a])&&mints[0]!=mints[1]
        && vaults[0]!=vaults[1] && [mints[0],mints[1],vaults[0],vaults[1],config,observation]
            .iter().all(|v|*v!=Pubkey::default()),
        "Raydium CLMM on-chain mints/vaults differ from Registry or invalid");
    // CLMM pool address supports both legacy and permissioned pool seed_index.
    let seed_index=&d[391..393];
    let seeds:Vec<&[u8]>=if seed_index==[0,0] {
        vec![b"pool",config.as_ref(),mints[0].as_ref(),mints[1].as_ref()]
    } else {
        vec![b"pool",config.as_ref(),mints[0].as_ref(),mints[1].as_ref(),seed_index]
    };
    let (pda,bump)=Pubkey::find_program_address(&seeds,&program);
    ensure!(pda==account.address && bump==d[8],
        "Raydium CLMM pool PDA or bump mismatch");
    let bitmap=Pubkey::find_program_address(
        &[b"pool_tick_array_bitmap_extension",account.address.as_ref()],&program).0;
    for i in 0..2 {
        let vault_pda=Pubkey::find_program_address(
            &[b"pool_vault",account.address.as_ref(),mints[i].as_ref()],&program).0;
        ensure!(vaults[i]==vault_pda,"Raydium CLMM vault PDA mismatch");
    }
    let oracle_pda=Pubkey::find_program_address(
        &[b"observation",account.address.as_ref()],&program).0;
    ensure!(observation==oracle_pda,"Raydium CLMM Observation PDA mismatch");
    Ok(Layout{pool:account.address,config,observation,bitmap,mints,vaults,
        decimals,tick_spacing,tick_current,status,open_time})
}
fn arrays(l:Layout,program:Pubkey)->Vec<(i32,Pubkey)>{
    let width=l.tick_spacing*60;
    let center=l.tick_current.div_euclid(width);
    (-ARRAY_RADIUS..=ARRAY_RADIUS).filter_map(|delta|{
        let start=(center+delta)*width;
        if start>443636 || start+width<=-443636 {return None;}
        Some((start,Pubkey::find_program_address(
            &[b"tick_array",l.pool.as_ref(),&start.to_be_bytes()],&program).0))
    }).collect()
}
fn decode_mint(account:&AccountData,spl:Pubkey,token_2022:Pubkey,
    decimals:u8)->Result<MintProof>{
    ensure!(full(account)&&account.bytes.len()>=82
        && account.bytes[44]==decimals && account.bytes[45]==1,
        "Raydium CLMM mint data, initialization or decimals invalid");
    let is_2022=if account.owner==spl {false}
        else if account.owner==token_2022 {true}
        else {bail!("Raydium CLMM unsupported mint program");};
    let data=&account.bytes;
    let mut extensions=Vec::new();
    if is_2022 && data.len()>82 {
        ensure!(data.len()>=166 && data[82..165].iter().all(|v|*v==0)
            && data[165]==1,"Raydium CLMM Token-2022 mint TLV framing invalid");
        let mut i=166;
        while i<data.len(){
            let tail=&data[i..];
            if tail.iter().all(|v|*v==0){break;}
            ensure!(tail.len()>=4,"truncated Raydium CLMM mint extension");
            let kind=u16::from_le_bytes(tail[0..2].try_into()?);
            let len=usize::from(u16::from_le_bytes(tail[2..4].try_into()?));
            if kind==0 && len==0 {
                ensure!(tail[4..].iter().all(|v|*v==0),"invalid CLMM mint TLV trailer");
                break;
            }
            ensure!(kind!=0&&len<=tail.len()-4,"invalid CLMM mint TLV entry");
            extensions.push(kind);
            i+=4+len;
        }
    } else if !is_2022 {
        ensure!(data.len()==82,"Raydium CLMM legacy SPL mint size invalid");
    }
    extensions.sort_unstable();
    ensure!(!extensions.windows(2).any(|w|w[0]==w[1]),
        "Raydium CLMM duplicate Token-2022 extensions");
    let unsupported=extensions.iter().copied()
        .filter(|v|matches!(*v,4|9|14|16|24)).collect();
    Ok(MintProof{address:account.address,token_2022:is_2022,
        extensions,unsupported})
}
fn validate_vault(account:&AccountData,mint:Pubkey,program:Pubkey,pool:Pubkey)->Result<()>{
    ensure!(full(account)&&account.owner==program && account.bytes.len()>=165,
        "Raydium CLMM vault token owner or account length invalid");
    ensure!(key(&account.bytes,0)?==mint && key(&account.bytes,32)?==pool
        && account.bytes[108]==1,
        "Raydium CLMM vault mint, authority or initialized state mismatch");
    Ok(())
}
fn validate_config(account:&AccountData,l:Layout,program:Pubkey)->Result<()>{
    ensure!(full(account)&&account.address==l.config && account.owner==program
        && account.bytes.len()==CONFIG_LEN && account.bytes[..8]==CONFIG_DISC,
        "Raydium CLMM AMM Config owner/length/discriminator invalid");
    let index=u16::from_le_bytes(account.bytes[9..11].try_into()?);
    let spacing=i32::from(u16::from_le_bytes(account.bytes[51..53].try_into()?));
    let (address,bump)=Pubkey::find_program_address(
        &[b"amm_config",&index.to_be_bytes()],&program);
    ensure!(address==account.address && account.bytes[8]==bump
        && spacing==l.tick_spacing,"Raydium CLMM AMM Config PDA/tick spacing invalid");
    Ok(())
}
fn validate_observation(account:&AccountData,l:Layout,program:Pubkey)->Result<()>{
    ensure!(full(account)&&account.owner==program && account.address==l.observation
        && account.bytes.len()==OBS_LEN && account.bytes[..8]==OBS_DISC,
        "Raydium CLMM Observation owner/length/discriminator invalid");
    // An unused ObservationState is allowed to have initialized=false.
    ensure!(matches!(account.bytes[8],0|1) && key(&account.bytes,19)?==l.pool
        && u16::from_le_bytes(account.bytes[17..19].try_into()?)<100,
        "Raydium CLMM Observation initialization/index/pool mismatch");
    Ok(())
}
fn validate_bitmap(account:&AccountData,l:Layout,program:Pubkey)->Result<()>{
    ensure!(full(account)&&account.owner==program && account.address==l.bitmap
        && account.bytes.len()==BITMAP_LEN && account.bytes[..8]==BITMAP_DISC
        && key(&account.bytes,8)?==l.pool,
        "Raydium CLMM bitmap extension owner/length/pool mismatch");
    Ok(())
}
fn validate_array(account:&AccountData,l:Layout,start:i32,program:Pubkey)->Result<bool>{
    ensure!(full(account)&&account.owner==program && account.bytes.len()==ARRAY_LEN
        && account.bytes[..8]==ARRAY_DISC
        && key(&account.bytes,8)?==l.pool
        && i32::from_le_bytes(account.bytes[40..44].try_into()?)==start,
        "Raydium CLMM TickArray owner/size/pool/start index invalid");
    // 60 * TickState::LEN (168) follows the 44-byte header.
    let count=account.bytes[44+60*168];
    ensure!(count<=60,"Raydium CLMM initialized TickArray count exceeds 60");
    Ok(count>0)
}
fn read(rpc:&RpcHandle,addresses:Vec<Pubkey>,min_slot:Option<u64>)
    -> impl std::future::Future<Output=Result<(u64,Vec<Option<AccountData>>)>> + '_ {
    async move {
        ensure!(!addresses.is_empty(),"empty Raydium CLMM RPC request");
        let expected=addresses.clone();
        let resp=rpc.call(RpcOperation::Multiple{
            addresses,view:AccountView::Full,min_context_slot:min_slot
        }).await?;
        let RpcAnswer::Multiple{slot,accounts}=resp.answer
            else{bail!("Raydium CLMM RPC returned unexpected response");};
        ensure!(accounts.len()==expected.len() && min_slot.is_none_or(|v|slot>=v)
            && accounts.iter().zip(&expected).all(|(a,key)|a.as_ref().is_none_or(|v|
                v.address==*key && full(v))),
            "Raydium CLMM RPC account address/cardinality/slot mismatch");
        Ok((slot,accounts))
    }
}
fn result(pool:Pubkey,status:CompletionStatus,slot:Option<u64>,
    reason:Option<String>,evidence:Option<Arc<CompletionEvidence>>,
    mints:Option<[MintProof;2]>,manifest:Option<Arc<ExecutionManifest>>)->CompletionResult{
    CompletionResult{pool,status,validated_slot:slot,reason,evidence,mints,manifest}
}
fn retry(pool:Pubkey,reason:impl Into<String>)->CompletionResult{
    result(pool,CompletionStatus::Retryable,None,Some(reason.into()),None,None,None)
}
fn incomplete(pool:Pubkey,slot:u64,reason:impl Into<String>)->CompletionResult{
    result(pool,CompletionStatus::Incomplete,Some(slot),Some(reason.into()),None,None,None)
}
fn make_manifest(l:Layout,slot:u64,up:&[Pubkey],down:&[Pubkey],
    token_2022:bool,owner:Pubkey)->ExecutionManifest{
    let ro=|address|AccountMeta{address,writable:false};
    let rw=|address|AccountMeta{address,writable:true};
    let memo:Pubkey=MEMO.parse().expect("known SPL Memo program");
    let direction=|base:Pubkey,x_vault:Pubkey,base_vault:Pubkey,arrays:&[Pubkey]|{
        // V10: owner, quote mint, [memo for Token-2022], pool, config,
        // observation, bitmap extension, input vault, output vault, TickArrays.
        let mut block=vec![ro(owner),ro(base)];
        if token_2022 {block.push(ro(memo));}
        block.extend([rw(l.pool),ro(l.config),rw(l.observation),rw(l.bitmap),
            rw(x_vault),rw(base_vault)]);
        block.extend(arrays.iter().copied().map(rw));
        Arc::<[AccountMeta]>::from(block)
    };
    // Base=A means B->A (price upward). Base=B means A->B (down).
    ExecutionManifest{pool:l.pool,mint_a:l.mints[0],mint_b:l.mints[1],validated_slot:slot,
        base_a:direction(l.mints[0],l.vaults[1],l.vaults[0],up),
        base_b:direction(l.mints[1],l.vaults[0],l.vaults[1],down)}
}
fn finish(pool:&PoolState,initial:Layout,slot:u64,
    lookup:&HashMap<Pubkey,Option<AccountData>>,owner:Pubkey,spl:Pubkey,
    token_2022:Pubkey)->CompletionResult{
    let get=|address:Pubkey|lookup.get(&address).and_then(Option::as_ref);
    let Some(parent)=get(pool.address) else {
        return retry(pool.address,"Raydium CLMM PoolState missing in coherent snapshot");
    };
    let l=match decode_pool(parent,pool,owner){
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Raydium CLMM final PoolState: {e:#}"))
    };
    if l.mints!=initial.mints || l.vaults!=initial.vaults || l.config!=initial.config
        || l.observation!=initial.observation || l.tick_spacing!=initial.tick_spacing
        || l.tick_current!=initial.tick_current {
        return retry(pool.address,"Raydium CLMM parent state changed during completion");
    }
    let (Some(ma),Some(mb),Some(va),Some(vb),Some(config),Some(obs),Some(bitmap))=(
        get(l.mints[0]),get(l.mints[1]),get(l.vaults[0]),get(l.vaults[1]),
        get(l.config),get(l.observation),get(l.bitmap)
    ) else{return retry(pool.address,"Raydium CLMM mint/vault/config/observation/bitmap unavailable");};
    if let Err(e)=validate_config(config,l,owner){
        return incomplete(pool.address,slot,format!("Raydium CLMM Config: {e:#}"));
    }
    if let Err(e)=validate_observation(obs,l,owner){
        return incomplete(pool.address,slot,format!("Raydium CLMM Observation: {e:#}"));
    }
    if let Err(e)=validate_bitmap(bitmap,l,owner){
        return incomplete(pool.address,slot,format!("Raydium CLMM Bitmap extension: {e:#}"));
    }
    let mint0=match decode_mint(ma,spl,token_2022,l.decimals[0]){
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Raydium CLMM mint0: {e:#}"))
    };
    let mint1=match decode_mint(mb,spl,token_2022,l.decimals[1]){
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Raydium CLMM mint1: {e:#}"))
    };
    if let Err(e)=validate_vault(va,l.mints[0],ma.owner,l.pool){
        return incomplete(pool.address,slot,format!("Raydium CLMM vault0: {e:#}"));
    }
    if let Err(e)=validate_vault(vb,l.mints[1],mb.owner,l.pool){
        return incomplete(pool.address,slot,format!("Raydium CLMM vault1: {e:#}"));
    }
    let width=l.tick_spacing*60;
    let center=l.tick_current.div_euclid(width)*width;
    let mut present=HashMap::<i32,Pubkey>::new();
    for (start,address) in arrays(l,owner) {
        if let Some(account)=get(address) {
            match validate_array(account,l,start,owner){
                Ok(true)=>{present.insert(start,address);},
                Ok(false)=>{},
                Err(e)=>return incomplete(pool.address,slot,
                    format!("Raydium CLMM TickArray start={start}: {e:#}")),
            }
        }
    }
    // Supply first initialized array and further initialized arrays in traversal
    // order; the Raydium executor skips noninitialized arrays in its bitmap.
    let up=(0..=ARRAY_RADIUS).map(|i|center+i*width)
        .filter_map(|index|present.get(&index).copied()).collect::<Vec<_>>();
    let down=(0..=ARRAY_RADIUS).map(|i|center-i*width)
        .filter_map(|index|present.get(&index).copied()).collect::<Vec<_>>();
    if up.is_empty()||down.is_empty(){
        return incomplete(pool.address,slot,
            "Raydium CLMM requires initialized TickArrays in both V10 directions within radius 2");
    }
    let proofs=if pool.mint_a==Some(l.mints[0]){[mint0,mint1]}else{[mint1,mint0]};
    let ev=Arc::new(CompletionEvidence{
        kind:"raydium_clmm_tick_arrays".into(),version:1,
        payload:Arc::new(json!({
            "program":owner.to_string(),"config":l.config.to_string(),
            "observation":l.observation.to_string(),"bitmap":l.bitmap.to_string(),
            "mint0":l.mints[0].to_string(),"mint1":l.mints[1].to_string(),
            "vault0":l.vaults[0].to_string(),"vault1":l.vaults[1].to_string(),
            "tick_spacing":l.tick_spacing,"tick_current":l.tick_current,
            "status":l.status,"open_time":l.open_time,
            "radius":ARRAY_RADIUS,
            "base_a_array_addresses":up.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "base_b_array_addresses":down.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "token_program_0":ma.owner.to_string(),"token_program_1":mb.owner.to_string()
        }))
    });
    if proofs.iter().any(|m|!m.unsupported.is_empty()){
        return result(pool.address,CompletionStatus::Incomplete,Some(slot),
            Some(format!("V10 unsupported Token-2022 mint extensions: {:?} {:?}",
                proofs[0].unsupported,proofs[1].unsupported)),
            Some(ev),Some(proofs),None);
    }
    let is_v2=ma.owner==token_2022||mb.owner==token_2022;
    let manifest=make_manifest(l,slot,&up,&down,is_v2,owner);
    // Manifest orientation follows Registry mint identity. V10 base_a denotes
    // mint A as quote, which may be opposite to CLMM's native token_mint_0.
    let swapped=pool.mint_a!=Some(l.mints[0]);
    let manifest=if swapped {
        ExecutionManifest{pool:manifest.pool,mint_a:pool.mint_a.expect("verified mint A"),
            mint_b:pool.mint_b.expect("verified mint B"),validated_slot:manifest.validated_slot,
            base_a:manifest.base_b,base_b:manifest.base_a}
    }else{manifest};
    result(pool.address,CompletionStatus::Prepared,Some(slot),None,
        Some(ev),Some(proofs),Some(Arc::new(manifest)))
}
async fn process(rpc:&RpcHandle,pools:Vec<PoolState>,settings:Settings)->Vec<CompletionResult>{
    let owner:Pubkey=PROGRAM.parse().expect("known CLMM program");
    let spl:Pubkey=SPL_TOKEN.parse().expect("known SPL Token program");
    let token_2022:Pubkey=TOKEN_2022.parse().expect("known Token-2022 program");
    let mut results:Vec<Option<CompletionResult>>=vec![None;pools.len()];
    let (first_slot,parents)=match read(rpc,pools.iter().map(|p|p.address).collect(),None).await {
        Ok(v)=>v,Err(e)=>return pools.iter().map(|p|
            retry(p.address,format!("Raydium CLMM initial RPC: {e:#}"))).collect(),
    };
    let mut pending=Vec::new();
    let mut addresses=Vec::new();
    let mut dedup=HashSet::new();
    for (i,pool) in pools.iter().enumerate(){
        let Some(parent)=parents[i].as_ref() else {
            results[i]=Some(retry(pool.address,"Raydium CLMM PoolState missing"));continue;
        };
        match decode_pool(parent,pool,owner){
            Ok(l)=>{
                for addr in [l.pool,l.config,l.observation,l.bitmap,
                    l.mints[0],l.mints[1],l.vaults[0],l.vaults[1]].into_iter()
                    .chain(arrays(l,owner).into_iter().map(|(_,address)|address)){
                    if dedup.insert(addr){addresses.push(addr);}
                }
                pending.push((i,l));
            }
            Err(e)=>results[i]=Some(incomplete(pool.address,first_slot,
                format!("Raydium CLMM PoolState: {e:#}"))),
        }
    }
    if !pending.is_empty(){
        if addresses.len()>settings.account_batch_size {
            for (i,_) in pending{
                results[i]=Some(retry(pools[i].address,
                    "Raydium CLMM final RPC account set exceeds shared batch size"));
            }
        }else{
            match read(rpc,addresses.clone(),Some(first_slot)).await{
                Ok((slot,all))=>{
                    let lookup=addresses.into_iter().zip(all).collect::<HashMap<_,_>>();
                    for (i,l) in pending{
                        results[i]=Some(finish(&pools[i],l,slot,&lookup,owner,spl,token_2022));
                    }
                }
                Err(e)=>for (i,_) in pending{
                    results[i]=Some(retry(pools[i].address,
                        format!("Raydium CLMM final RPC: {e:#}")));
                }
            }
        }
    }
    results.into_iter().map(|r|r.expect("every CLMM pool received a result")).collect()
}

#[derive(Default)]
struct Scheduler {
    candidates: HashMap<Pubkey,PoolState>,
    attempts: HashMap<Pubkey,u8>,
    retry_after: HashMap<Pubkey,Instant>,
}
impl Scheduler {
    fn update(&mut self, pool: &PoolState) {
        if pool.venue != Venue::RaydiumClmm {return;}
        if pool.completion_state==CompletionState::Prepared {
            self.candidates.remove(&pool.address);
            self.retry_after.remove(&pool.address);
        } else if pool.mint_a.is_none() || pool.mint_b.is_none()
            || !pool.tvl_usd.is_some_and(|v|v.is_finite() && v>=MIN_TVL_USD) {
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
                    .is_some_and(|reason|reason.contains("V10 unsupported Token-2022 mint extensions"))=>Some(2),
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
    println!("raydium clmm completion started min_tvl_usd={} limit={} parallel={} pools_per_batch={} retry_cooldown_s={}",
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
                        if pool.venue==Venue::RaydiumClmm {seen.insert(pool.address);}
                        scheduler.update(pool);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n))=>
                    bail!("Raydium CLMM completion lost {n} Registry batches"),
                Err(broadcast::error::RecvError::Closed)=>source_open=false,
            },
            done_batch=tasks.join_next(),if !tasks.is_empty()=>{
                let (tier,results)=done_batch.context("missing Raydium CLMM partition")?
                    .context("Raydium CLMM completion task panicked")?;
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
                println!("raydium clmm completion batch tier={tier} attempts={} prepared={p} incomplete={i} retryable={r} total_attempts={attempts} elapsed_ms={}",
                    results.len(),started.elapsed().as_millis());
                for result in results.iter().filter(|r|r.status==CompletionStatus::Prepared).take(2) {
                    if let Some(manifest)=&result.manifest {
                        println!("raydium clmm manifest pool={} slot={} a_accounts={} b_accounts={} base_a={:?} base_b={:?}",
                            manifest.pool,manifest.validated_slot,manifest.base_a.len(),manifest.base_b.len(),
                            manifest.base_a.iter().map(|a|format!("{}:{}",a.address,if a.writable {"rw"} else {"ro"}))
                                .collect::<Vec<_>>(),
                            manifest.base_b.iter().map(|a|format!("{}:{}",a.address,if a.writable {"rw"} else {"ro"}))
                                .collect::<Vec<_>>());
                    }
                }
                for result in results.iter().filter(|r|r.status!=CompletionStatus::Prepared).take(3) {
                    println!("raydium clmm completion result pool={} status={:?} reason={}",
                        result.pool,result.status,result.reason.as_deref().unwrap_or("-"));
                }
                output.send(Arc::new(CompletionResultBatch {source:SOURCE,results}))
                    .map_err(|_|anyhow!("Raydium CLMM completion has no Registry consumer"))?;
            },
            _=pulse.tick()=>{
                let (a,b,c)=scheduler.counts(Instant::now());
                println!("raydium clmm completion backlog seen={} candidate_unknown={a} candidate_incomplete={b} eligible_retry={c} candidates_in_ram={} active_batches={} attempts={attempts} prepared={prepared} incomplete={incomplete} retryable={retryable} registry_updates={received_updates}",
                    seen.len(),scheduler.candidates.len(),tasks.len());
            }
        }
    }
    let (a,b,c)=scheduler.counts(Instant::now());
    println!("raydium clmm completion complete min_tvl_usd={MIN_TVL_USD} pools_seen={} registry_updates={received_updates} first_attempts={first_attempts} retries={retries} prepared={prepared} incomplete={incomplete} retryable={retryable} eligible_unknown_left={a} eligible_incomplete_left={b} eligible_retry_left={c} elapsed_ms={}",
        seen.len(),started.elapsed().as_millis());
    Ok(())
}
