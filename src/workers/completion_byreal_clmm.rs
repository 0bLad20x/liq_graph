//! Byreal CLMM execution completion: Lifecycle requests -> RPC -> validated V10 manifests.
//! V10 CLMM-format decoding, TickArray/bitmap checks, directional accounts and evidence.
//! No direct SQL, REST, graph mutation, second RPC client or protocol logic in Core.

use std::{collections::{HashMap, HashSet}, env, sync::Arc};
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::{sync::mpsc, task::JoinSet};

use crate::{
    core::{CompletionReason, AccountMeta, CompletionEvidence, CompletionResult, CompletionRequestBatch, CompletionResponse, CompletionResponseBatch,
        CompletionStatus, ExecutionManifest, MintProof, PoolState,
        Venue},
    workers::rpc_transport::{AccountData, AccountView, RpcAnswer, RpcHandle, RpcOperation},
};

const SOURCE: &str = "byreal_clmm_execution_completion";
const PROGRAM: &str = "REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2";
const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const MEMO: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const POOL_DISC: [u8;8] = [247,237,227,245,215,195,222,70];
const CONFIG_DISC: [u8;8] = [218,244,33,104,203,203,43,111];
const OBS_DISC: [u8;8] = [122,174,197,53,129,9,165,132];
const BITMAP_DISC: [u8;8] = [60,150,36,219,97,128,139,153];
const ARRAY_DISC: [u8;8] = [192,155,85,205,49,249,129,42];
const DYN_ARRAY_DISC: [u8;8] = [106,139,152,36,117,153,184,56];
const POOL_LEN: usize = 1544;
const CONFIG_LEN: usize = 117;
const OBS_LEN: usize = 4483;
const BITMAP_LEN: usize = 1832;
const ARRAY_LEN: usize = 10240;
const ARRAY_RADIUS: i32 = 2; // Matches public V10 array_candidates max radius
const POOLS_PER_BATCH: usize = 7;
const PARALLEL_BATCHES: usize = 16;

#[derive(Clone, Copy)]
pub struct Settings {
    account_batch_size: usize,
    pub parallel_batches: usize,
}
impl Settings {
    pub fn from_env() -> Result<Self> {
        // Reuse existing RPC transport settings. 13 = pool + 2 mints +
        // 2 vaults + config + observation + bitmap + 5 array candidates.
        let account_batch_size = env::var("RPC_ACCOUNT_BATCH_SIZE")
            .unwrap_or_else(|_| "100".into()).parse::<usize>()
            .context("invalid RPC_ACCOUNT_BATCH_SIZE")?;
        ensure!((13..=100).contains(&account_batch_size),
            "Byreal CLMM requires RPC_ACCOUNT_BATCH_SIZE in 13..=100");
        Ok(Self {account_batch_size, parallel_batches:PARALLEL_BATCHES})
    }
    pub fn pools_per_batch(self) -> usize {
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
    decay_fee_flag: u8,
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
        "Byreal CLMM PoolState owner/size/discriminator invalid");
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
        "Byreal CLMM tick spacing or tick_current out of bounds");
    ensure!(status&16==0,"Byreal CLMM swaps disabled by pool status bit 4");
    ensure!(open_time<crate::core::now_unix_ms()?/1000,
        "Byreal CLMM pool swap open_time not reached");
    let (Some(a),Some(b))=(expected.mint_a,expected.mint_b) else {
        bail!("Byreal CLMM Registry mint identity absent");
    };
    ensure!((mints==[a,b]||mints==[b,a])&&mints[0]!=mints[1]
        && vaults[0]!=vaults[1] && [mints[0],mints[1],vaults[0],vaults[1],config,observation]
            .iter().all(|v|*v!=Pubkey::default()),
        "Byreal CLMM on-chain mints/vaults differ from Registry or invalid");
    // CLMM pool address supports both legacy and permissioned pool seed_index.
    let seed_index=&d[391..393];
    let seeds:Vec<&[u8]>=if seed_index.iter().all(|v|*v==0) {
        vec![b"pool",config.as_ref(),mints[0].as_ref(),mints[1].as_ref()]
    } else {
        vec![b"pool",config.as_ref(),mints[0].as_ref(),mints[1].as_ref(),seed_index]
    };
    let (pda,bump)=Pubkey::find_program_address(&seeds,&program);
    ensure!(pda==account.address && bump==d[8],
        "Byreal CLMM pool PDA or bump mismatch");
    let bitmap=Pubkey::find_program_address(
        &[b"pool_tick_array_bitmap_extension",account.address.as_ref()],&program).0;
    for i in 0..2 {
        let vault_pda=Pubkey::find_program_address(
            &[b"pool_vault",account.address.as_ref(),mints[i].as_ref()],&program).0;
        ensure!(vaults[i]==vault_pda,"Byreal CLMM vault PDA mismatch");
    }
    let oracle_pda=Pubkey::find_program_address(
        &[b"observation",account.address.as_ref()],&program).0;
    ensure!(observation==oracle_pda,"Byreal CLMM Observation PDA mismatch");
    Ok(Layout{pool:account.address,config,observation,bitmap,mints,vaults,
        decimals,tick_spacing,tick_current,status,open_time,decay_fee_flag:d[1096]})
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
        "Byreal CLMM mint data, initialization or decimals invalid");
    let is_2022=if account.owner==spl {false}
        else if account.owner==token_2022 {true}
        else {bail!("Byreal CLMM unsupported mint program");};
    let data=&account.bytes;
    let mut extensions=Vec::new();
    if is_2022 && data.len()>82 {
        ensure!(data.len()>=166 && data[82..165].iter().all(|v|*v==0)
            && data[165]==1,"Byreal CLMM Token-2022 mint TLV framing invalid");
        let mut i=166;
        while i<data.len(){
            let tail=&data[i..];
            if tail.iter().all(|v|*v==0){break;}
            ensure!(tail.len()>=4,"truncated Byreal CLMM mint extension");
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
        ensure!(data.len()==82,"Byreal CLMM legacy SPL mint size invalid");
    }
    extensions.sort_unstable();
    ensure!(!extensions.windows(2).any(|w|w[0]==w[1]),
        "Byreal CLMM duplicate Token-2022 extensions");
    // V10's CLMM builder accepts both SPL Token and Token-2022 mints.
    // Do not impose an extension blacklist not present in that builder.
    Ok(MintProof{address:account.address,token_2022:is_2022,
        extensions,unsupported:Vec::new()})
}
fn validate_vault(account:&AccountData,mint:Pubkey,program:Pubkey,pool:Pubkey)->Result<()>{
    ensure!(full(account)&&account.owner==program && account.bytes.len()>=165,
        "Byreal CLMM vault token owner or account length invalid");
    ensure!(key(&account.bytes,0)?==mint && key(&account.bytes,32)?==pool
        && account.bytes[108]==1,
        "Byreal CLMM vault mint, authority or initialized state mismatch");
    Ok(())
}
fn validate_config(account:&AccountData,l:Layout,program:Pubkey)->Result<()>{
    ensure!(full(account)&&account.address==l.config && account.owner==program
        && account.bytes.len()==CONFIG_LEN && account.bytes[..8]==CONFIG_DISC,
        "Byreal CLMM AMM Config owner/length/discriminator invalid");
    let index=u16::from_le_bytes(account.bytes[9..11].try_into()?);
    let spacing=i32::from(u16::from_le_bytes(account.bytes[51..53].try_into()?));
    let (address,bump)=Pubkey::find_program_address(
        &[b"amm_config",&index.to_be_bytes()],&program);
    ensure!(address==account.address && account.bytes[8]==bump
        && spacing==l.tick_spacing,"Byreal CLMM AMM Config PDA/tick spacing invalid");
    Ok(())
}
fn validate_observation(account:&AccountData,l:Layout,program:Pubkey)->Result<()>{
    ensure!(full(account)&&account.owner==program && account.address==l.observation
        && account.bytes.len()==OBS_LEN && account.bytes[..8]==OBS_DISC,
        "Byreal CLMM Observation owner/length/discriminator invalid");
    // An unused ObservationState is allowed to have initialized=false.
    ensure!(matches!(account.bytes[8],0|1) && key(&account.bytes,19)?==l.pool
        && u16::from_le_bytes(account.bytes[17..19].try_into()?)<100,
        "Byreal CLMM Observation initialization/index/pool mismatch");
    Ok(())
}
fn validate_bitmap(account:&AccountData,l:Layout,program:Pubkey)->Result<()>{
    ensure!(full(account)&&account.owner==program && account.address==l.bitmap
        && account.bytes.len()==BITMAP_LEN && account.bytes[..8]==BITMAP_DISC
        && key(&account.bytes,8)?==l.pool,
        "Byreal CLMM bitmap extension owner/length/pool mismatch");
    Ok(())
}
fn validate_array(account:&AccountData,l:Layout,start:i32,program:Pubkey)->Result<()>{
    ensure!(full(account)&&account.owner==program && account.bytes.len()>=44
        && key(&account.bytes,8)?==l.pool
        && i32::from_le_bytes(account.bytes[40..44].try_into()?)==start,
        "Byreal CLMM TickArray owner/pool/start index invalid");
    // Byreal CLMM supports fixed 10,240-byte TickArrayState and compact,
    // variable-length DynTickArrayState at the same tick_array PDA.
    if account.bytes[..8]==ARRAY_DISC {
        ensure!(account.bytes.len()==ARRAY_LEN
            && account.bytes[44+60*168]<=60,
            "Byreal CLMM fixed TickArray length/initialized count invalid");
    }else if account.bytes[..8]==DYN_ARRAY_DISC{
        const HEADER:usize=216;
        ensure!(account.bytes.len()>=HEADER && account.bytes[108]<=60
            && account.bytes[109]<=account.bytes[108]
            && account.bytes.len()==HEADER+usize::from(account.bytes[108])*168,
            "Byreal CLMM dynamic TickArray allocation/length invalid");
        ensure!(account.bytes[48..108].iter().all(|i|*i<=account.bytes[108]),
            "Byreal CLMM dynamic TickArray offset index invalid");
    }else{bail!("Byreal CLMM unsupported TickArray discriminator");}
    Ok(())
}

fn read(rpc:&RpcHandle,addresses:Vec<Pubkey>,min_slot:Option<u64>)
    -> impl std::future::Future<Output=Result<(u64,Vec<Option<AccountData>>)>> + '_ {
    async move {
        ensure!(!addresses.is_empty(),"empty Byreal CLMM RPC request");
        let expected=addresses.clone();
        let resp=rpc.call(RpcOperation::Multiple{
            addresses,view:AccountView::Full,min_context_slot:min_slot
        }).await?;
        let RpcAnswer::Multiple{slot,accounts}=resp.answer
            else{bail!("Byreal CLMM RPC returned unexpected response");};
        ensure!(accounts.len()==expected.len() && min_slot.is_none_or(|v|slot>=v)
            && accounts.iter().zip(&expected).all(|(a,key)|a.as_ref().is_none_or(|v|
                v.address==*key && full(v))),
            "Byreal CLMM RPC account address/cardinality/slot mismatch");
        Ok((slot,accounts))
    }
}
fn result(pool:Pubkey,status:CompletionStatus,slot:Option<u64>,
    reason:Option<String>,evidence:Option<Arc<CompletionEvidence>>,
    mints:Option<[MintProof;2]>,manifest:Option<Arc<ExecutionManifest>>)->CompletionResult{
    CompletionResult::new(pool,status,slot,reason,evidence,mints,manifest)
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
        bound_signer:None,
        base_a:direction(l.mints[0],l.vaults[1],l.vaults[0],up),
        base_b:direction(l.mints[1],l.vaults[0],l.vaults[1],down)}
}
fn finish(pool:&PoolState,initial:Layout,slot:u64,
    lookup:&HashMap<Pubkey,Option<AccountData>>,owner:Pubkey,spl:Pubkey,
    token_2022:Pubkey)->CompletionResult{
    let get=|address:Pubkey|lookup.get(&address).and_then(Option::as_ref);
    let Some(parent)=get(pool.address) else {
        return retry(pool.address,"Byreal CLMM PoolState missing in coherent snapshot");
    };
    let l=match decode_pool(parent,pool,owner){
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Byreal CLMM final PoolState: {e:#}"))
    };
    if l.mints!=initial.mints || l.vaults!=initial.vaults || l.config!=initial.config
        || l.observation!=initial.observation || l.tick_spacing!=initial.tick_spacing
        || l.tick_current!=initial.tick_current {
        return retry(pool.address,"Byreal CLMM parent state changed during completion");
    }
    let (Some(ma),Some(mb),Some(va),Some(vb),Some(config),Some(obs),Some(bitmap))=(
        get(l.mints[0]),get(l.mints[1]),get(l.vaults[0]),get(l.vaults[1]),
        get(l.config),get(l.observation),get(l.bitmap)
    ) else{return retry(pool.address,"Byreal CLMM mint/vault/config/observation/bitmap unavailable");};
    if let Err(e)=validate_config(config,l,owner){
        return incomplete(pool.address,slot,format!("Byreal CLMM Config: {e:#}"));
    }
    if let Err(e)=validate_observation(obs,l,owner){
        return incomplete(pool.address,slot,format!("Byreal CLMM Observation: {e:#}"));
    }
    if let Err(e)=validate_bitmap(bitmap,l,owner){
        return incomplete(pool.address,slot,format!("Byreal CLMM Bitmap extension: {e:#}"));
    }
    let mint0=match decode_mint(ma,spl,token_2022,l.decimals[0]){
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Byreal CLMM mint0: {e:#}"))
    };
    let mint1=match decode_mint(mb,spl,token_2022,l.decimals[1]){
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Byreal CLMM mint1: {e:#}"))
    };
    if let Err(e)=validate_vault(va,l.mints[0],ma.owner,l.pool){
        return incomplete(pool.address,slot,format!("Byreal CLMM vault0: {e:#}"));
    }
    if let Err(e)=validate_vault(vb,l.mints[1],mb.owner,l.pool){
        return incomplete(pool.address,slot,format!("Byreal CLMM vault1: {e:#}"));
    }
    // Match the public V10 AccountLoader: list each existing TickArray
    // in ascending tick order, regardless of direction or liquidity bitmap.
    // Account ownership/layout is still verified before publishing it.
    let mut tick_arrays=Vec::new();
    for (start,address) in arrays(l,owner) {
        if let Some(account)=get(address) {
            if let Err(e)=validate_array(account,l,start,owner){
                return incomplete(pool.address,slot,
                    format!("Byreal CLMM TickArray start={start}: {e:#}"));
            }
            tick_arrays.push(address);
        }
    }
    if tick_arrays.is_empty(){
        return incomplete(pool.address,slot,
            "V10 CLMM requires at least one existing TickArray in the candidate window").with_reason_code(CompletionReason::MissingExecutionAccounts);
    }
    let proofs=if pool.mint_a==Some(l.mints[0]){[mint0,mint1]}else{[mint1,mint0]};
    let ev=Arc::new(CompletionEvidence{
        kind:"byreal_clmm_tick_arrays".into(),version:1,
        payload:Arc::new(json!({
            "program":owner.to_string(),"config":l.config.to_string(),
            "observation":l.observation.to_string(),"bitmap":l.bitmap.to_string(),
            "mint0":l.mints[0].to_string(),"mint1":l.mints[1].to_string(),
            "vault0":l.vaults[0].to_string(),"vault1":l.vaults[1].to_string(),
            "tick_spacing":l.tick_spacing,"tick_current":l.tick_current,
            "status":l.status,"open_time":l.open_time,
            "decay_fee_flag":l.decay_fee_flag,
            "radius":ARRAY_RADIUS,
            "base_a_array_addresses":tick_arrays.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "base_b_array_addresses":tick_arrays.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "token_program_0":ma.owner.to_string(),"token_program_1":mb.owner.to_string()
        }))
    });
    // Byreal swap_v2 rejects bit4-enabled swap-dynamic-fee pools.
    // They require swap_v3_dyn plus two Pyth accounts, which are absent
    // from the public V10 CLMM account builder. Decay-fee bits 0..2 are supported.
    if l.decay_fee_flag & 0b10000 != 0 {
        return result(pool.address,CompletionStatus::Incomplete,Some(slot),
            Some("Byreal swap-dynamic-fee requires swap_v3_dyn Pyth accounts not present in V10 CLMM builder".into()),
            Some(ev),Some(proofs),None).with_reason_code(CompletionReason::UnsupportedConfiguration);
    }
    let is_v2=ma.owner==token_2022||mb.owner==token_2022;
    let manifest=make_manifest(l,slot,&tick_arrays,&tick_arrays,is_v2,owner);
    // Manifest orientation follows Registry mint identity. V10 base_a denotes
    // mint A as quote, which may be opposite to CLMM's native token_mint_0.
    let swapped=pool.mint_a!=Some(l.mints[0]);
    let manifest=if swapped {
        ExecutionManifest{pool:manifest.pool,mint_a:pool.mint_a.expect("verified mint A"),
            mint_b:pool.mint_b.expect("verified mint B"),validated_slot:manifest.validated_slot,
            bound_signer:manifest.bound_signer,
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
            retry(p.address,format!("Byreal CLMM initial RPC: {e:#}"))).collect(),
    };
    let mut pending=Vec::new();
    let mut addresses=Vec::new();
    let mut dedup=HashSet::new();
    for (i,pool) in pools.iter().enumerate(){
        let Some(parent)=parents[i].as_ref() else {
            results[i]=Some(retry(pool.address,"Byreal CLMM PoolState missing"));continue;
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
                format!("Byreal CLMM PoolState: {e:#}"))),
        }
    }
    if !pending.is_empty(){
        if addresses.len()>settings.account_batch_size {
            for (i,_) in pending{
                results[i]=Some(retry(pools[i].address,
                    "Byreal CLMM final RPC account set exceeds shared batch size"));
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
                        format!("Byreal CLMM final RPC: {e:#}")));
                }
            }
        }
    }
    results.into_iter().map(|r|r.expect("every CLMM pool received a result")).collect()
}

/// Protocol executor only. The shared lifecycle owns admission scheduling,
/// retry/backoff, reactivation and Prepared refresh.
pub async fn run(mut input:mpsc::Receiver<CompletionRequestBatch>,
    rpc:RpcHandle,settings:Settings,output:mpsc::Sender<CompletionResponseBatch>)->Result<()> {
    let mut tasks=JoinSet::new();
    let mut open=true;
    let mut processed=0_usize;
    crate::log_event!(Info,"byreal_clmm executor started parallel={} pools_per_batch={} scheduler=completion_lifecycle",
        settings.parallel_batches,settings.pools_per_batch());
    while open || !tasks.is_empty() {
        tokio::select! {
            batch=input.recv(),if open && tasks.len()<settings.parallel_batches=>match batch {
                Some(batch)=>{
                    ensure!(!batch.requests.is_empty() && batch.requests.len()<=settings.pools_per_batch(),
                        "invalid Byreal CLMM request batch size");
                    ensure!(batch.requests.iter().all(|r|r.pool.venue==Venue::ByrealClmm
                        && r.pool.completion_admission.is_admitted()),
                        "Byreal CLMM request has wrong venue or missing admission");
                    let rpc=rpc.clone();
                    tasks.spawn(async move {
                        let pools=batch.requests.iter().map(|r|r.pool.clone()).collect::<Vec<_>>();
                        let results=match tokio::time::timeout(batch.timeout,
                            process(&rpc,pools,settings)).await {
                            Ok(results)=>results,
                            Err(_)=>batch.requests.iter().map(|r|retry(r.pool.address,
                                "Byreal CLMM lifecycle request timed out")).collect(),
                        };
                        (batch.requests,results)
                    });
                },
                None=>open=false,
            },
            done=tasks.join_next(),if !tasks.is_empty()=>{
                let (requests,results)=done.context("missing Byreal CLMM request task")?
                    .context("Byreal CLMM request task panicked")?;
                ensure!(requests.len()==results.len(),"Byreal CLMM request/result count mismatch");
                let mut by_pool=results.into_iter().map(|r|(r.pool,r)).collect::<HashMap<_,_>>();
                ensure!(by_pool.len()==requests.len(),"duplicate Byreal CLMM pool result");
                let mut responses=Vec::with_capacity(requests.len());
                for request in requests {
                    let result=by_pool.remove(&request.pool.address)
                        .context("Byreal CLMM request has no matching pool result")?;
                    crate::log_event!(Debug,
                        "byreal_clmm executor result request_id={} pool={} work={:?} status={:?} reason_code={:?} slot={:?} reason={}",
                        request.id,result.pool,request.work,result.status,result.reason_code,
                        result.validated_slot,result.reason.as_deref().unwrap_or("-"));
                    responses.push(CompletionResponse{id:request.id,result});
                }
                processed+=responses.len();
                output.send(CompletionResponseBatch{source:SOURCE,responses}).await
                    .map_err(|_|anyhow!("Byreal CLMM lifecycle response consumer closed"))?;
            },
        }
    }
    crate::log_event!(Info,"byreal_clmm executor complete requests={processed}");
    Ok(())
}
