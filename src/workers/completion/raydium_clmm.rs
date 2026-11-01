//! Raydium CLMM execution completion: Lifecycle requests -> RPC -> validated V10 manifests.
//! Protocol-owned decoding, TickArray/bitmap checks, directional accounts and evidence.
//! No direct SQL, REST, graph mutation, second RPC client or protocol logic in Core.

use std::{collections::{HashMap, HashSet}, env, sync::Arc};
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::{sync::mpsc, task::JoinSet};

use crate::{
    core::{CompletionReason, AccountMeta, CompletionEvidence, CompletionResult, CompletionOutcome, CompletionRequest, CompletionUnchanged, CompletionRequestBatch, CompletionResponse, CompletionResponseBatch,
        CompletionStatus, ExecutionManifest, MintProof, PoolState,
        Venue},
    core::rpc::{AccountData, AccountView, RpcAnswer, RpcHandle, RpcOperation},
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
            "Raydium CLMM requires RPC_ACCOUNT_BATCH_SIZE in 13..=100");
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
    // Upstream Raydium CLMM PoolState explicitly documents open_time as
    // currently disabled. Never gate on the host wall clock: executable
    // time/slot policy belongs to the deployed on-chain swap implementation.
    let (Some(a),Some(b))=(expected.mint_a,expected.mint_b) else {
        bail!("Raydium CLMM Registry mint identity absent");
    };
    ensure!((mints==[a,b]||mints==[b,a])&&mints[0]!=mints[1]
        && vaults[0]!=vaults[1] && [mints[0],mints[1],vaults[0],vaults[1],config,observation]
            .iter().all(|v|*v!=Pubkey::default()),
        "Raydium CLMM on-chain mints/vaults differ from Registry or invalid");
    // CLMM pool address supports both legacy and permissioned pool seed_index.
    let seed_index=&d[391..393];
    let seeds:Vec<&[u8]>=if seed_index.iter().all(|v|*v==0) {
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
// Raydium CLMM's pool bitmap has 1024 bits centered on tick-array index 0.
// Outside that range, bitmap-extension pages hold 512 bits per side/page.
fn array_initialized(pool:&AccountData,extension:&AccountData,start:i32,width:i32)->Result<bool>{
    let index=start.div_euclid(width);
    let (data,offset,bit)=if (-512..512).contains(&index) {
        let bit=usize::try_from(index+512)?;
        (pool.bytes.as_ref(),904+8*(bit/64),bit%64)
    }else{
        let boundary=512*width;
        let absolute=start.abs();
        let mut page=absolute/boundary-1;
        if start<0 && absolute%boundary==0 {page-=1;}
        ensure!((0..14).contains(&page),
            "Raydium CLMM bitmap extension page outside range");
        let within=absolute%boundary;
        let local=if start<0 && within!=0 {512-within/width}else{within/width};
        let local=usize::try_from(local)?;
        let base=8+32+if start<0 {14*64}else{0};
        let offset=base+usize::try_from(page)?*64+8*(local/64);
        (extension.bytes.as_ref(),offset,local%64)
    };
    Ok((u64_at(data,offset)? & (1_u64<<bit))!=0)
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
    CompletionResult::new(pool,status,slot,reason,evidence,mints,manifest)
}
fn retry(pool:Pubkey,reason:impl Into<String>)->CompletionResult{
    result(pool,CompletionStatus::Retryable,None,Some(reason.into()),None,None,None)
}
fn incomplete(pool:Pubkey,slot:u64,reason:impl Into<String>)->CompletionResult{
    result(pool,CompletionStatus::Incomplete,Some(slot),Some(reason.into()),None,None,None)
}
// Native token 0->1 traverses decreasing ticks (down); 1->0 traverses up.
// An empty side is never a valid V10 execution block; the other side remains
// admitted. Every side must begin with the first initialized bitmap array.
fn ordered_arrays(present:&HashMap<i32,Pubkey>,center:i32,width:i32)->(Vec<Pubkey>,Vec<Pubkey>){
    let up=(0..=ARRAY_RADIUS).map(|i|center+i*width)
        .filter_map(|start|present.get(&start).copied()).collect();
    let down=(0..=ARRAY_RADIUS).map(|i|center-i*width)
        .filter_map(|start|present.get(&start).copied()).collect();
    (up,down)
}

#[cfg(test)]
mod cp15_direction_tests{
    use super::*;
    #[test]fn cp15_one_sided_and_negative_tick_start(){
        let pool=Pubkey::new_unique();let mut present=HashMap::new();
        present.insert(-600,pool);
        let (up,down)=ordered_arrays(&present,-600,600);
        assert_eq!(up,vec![pool]);assert_eq!(down,vec![pool]);
        present.remove(&-600);
        let next=Pubkey::new_unique();present.insert(-1200,next);
        let (up,down)=ordered_arrays(&present,-600,600);
        assert!(up.is_empty());assert_eq!(down,vec![next]);
        present.clear();assert_eq!(ordered_arrays(&present,-600,600),(vec![],vec![]));
    }
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
        let initialized=match array_initialized(parent,bitmap,start,width){
            Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,
                format!("Raydium CLMM bitmap start={start}: {e:#}")),
        };
        match get(address) {
            Some(account)=>{
                let contains=match validate_array(account,l,start,owner){
                    Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,
                        format!("Raydium CLMM TickArray start={start}: {e:#}")),
                };
                if contains!=initialized {
                    return incomplete(pool.address,slot,
                        format!("Raydium CLMM TickArray count/bitmap disagreement at {start}"));
                }
                if initialized {present.insert(start,address);}
            }
            None if initialized =>return incomplete(pool.address,slot,
                format!("Raydium CLMM initialized TickArray missing at {start}")).with_reason_code(CompletionReason::MissingExecutionAccounts),
            None=>{}
        }
    }
    // Supply first initialized array and further initialized arrays in traversal
    // order; the Raydium executor skips noninitialized arrays in its bitmap.
    let (up,down)=ordered_arrays(&present,center,width);
    if up.is_empty() && down.is_empty(){
        return incomplete(pool.address,slot,
            "Raydium CLMM has no first initialized TickArray in either direction within validated V10 radius")
            .with_reason_code(CompletionReason::MissingExecutionAccounts);
    }
    let proofs=if pool.mint_a==Some(l.mints[0]){[mint0,mint1]}else{[mint1,mint0]};
    let ev=Arc::new(CompletionEvidence{
        kind:"raydium_clmm_tick_arrays".into(),version:2,
        payload:Arc::new(json!({
            "program":owner.to_string(),"config":l.config.to_string(),
            "observation":l.observation.to_string(),"bitmap":l.bitmap.to_string(),
            "mint0":l.mints[0].to_string(),"mint1":l.mints[1].to_string(),
            "vault0":l.vaults[0].to_string(),"vault1":l.vaults[1].to_string(),
            "tick_spacing":l.tick_spacing,"tick_current":l.tick_current,
            "status":l.status,"open_time":l.open_time,
            "radius":ARRAY_RADIUS,
            "direction_0_to_1_ready":!down.is_empty(),
            "direction_1_to_0_ready":!up.is_empty(),
            "base_a_array_addresses":up.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "base_b_array_addresses":down.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "token_program_0":ma.owner.to_string(),"token_program_1":mb.owner.to_string()
        }))
    });
    if proofs.iter().any(|m|!m.unsupported.is_empty()){
        return result(pool.address,CompletionStatus::Incomplete,Some(slot),
            Some(format!("V10 unsupported Token-2022 mint extensions: {:?} {:?}",
                proofs[0].unsupported,proofs[1].unsupported)),
            Some(ev),Some(proofs),None).with_reason_code(CompletionReason::UnsupportedConfiguration);
    }
    let is_v2=ma.owner==token_2022||mb.owner==token_2022;
    let manifest=make_manifest(l,slot,&up,&down,is_v2,owner);
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

/// Protocol executor only. The shared lifecycle owns admission scheduling,
/// retry/backoff, reactivation and Prepared refresh.
async fn process_optimized(rpc:&RpcHandle,requests:&[CompletionRequest],settings:Settings)->Vec<CompletionOutcome>{
    let owner:Pubkey=PROGRAM.parse().expect("known Raydium CLMM program");
    let eligible=requests.iter().filter(|r|!r.force_full
        && r.work!=crate::core::CompletionWork::Initial
        && r.pool.completion_state==crate::core::CompletionState::Prepared
        && r.pool.execution_manifest.is_some() && r.pool.mint_proofs.is_some()
        && r.pool.completion_evidence.as_ref().is_some_and(|e|
            e.kind=="raydium_clmm_tick_arrays" && e.version==2)
        && r.pool.completion_validated_slot.is_some()).collect::<Vec<_>>();
    let mut outcomes=HashMap::<Pubkey,CompletionOutcome>::new();
    if !eligible.is_empty(){
        let min_slot=eligible.iter().filter_map(|r|r.pool.completion_validated_slot).max();
        match read(rpc,eligible.iter().map(|r|r.pool.address).collect(),min_slot).await {
            Err(error)=>for r in &eligible {
                outcomes.insert(r.pool.address,CompletionOutcome::Proof(retry(r.pool.address,
                    format!("Raydium CLMM light parent: {error:#}"))));
            },
            Ok((parent_slot,parents))=>{
                let layouts=eligible.iter().zip(parents.iter()).map(|(r,a)|
                    a.as_ref().and_then(|a|decode_pool(a,&r.pool,owner).ok())).collect::<Vec<_>>();
                let mut keys=Vec::new();let mut seen=HashSet::new();
                for layout in layouts.iter().flatten(){
                    for addr in std::iter::once(layout.bitmap)
                        .chain(arrays(*layout,owner).into_iter().map(|(_,addr)|addr)) {
                        if seen.insert(addr){keys.push(addr);}
                    }
                }
                if !keys.is_empty() && keys.len()<=settings.account_batch_size {
                    match read(rpc,keys.clone(),Some(parent_slot)).await {
                        Err(error)=>for r in &eligible {
                            outcomes.insert(r.pool.address,CompletionOutcome::Proof(retry(r.pool.address,
                                format!("Raydium CLMM light arrays: {error:#}"))));
                        },
                        Ok((slot,accounts))=>{
                            let lookup=keys.into_iter().zip(accounts).collect::<HashMap<_,_>>();
                            for ((r,layout),parent) in eligible.iter().zip(layouts.iter()).zip(parents.iter()){
                                let Some(l)=layout else{continue};
                                let p=&r.pool;
                                let valid=(||->Result<bool>{
                                    let parent=parent.as_ref().context("missing parent")?;
                                    let bitmap=lookup.get(&l.bitmap).and_then(Option::as_ref)
                                        .context("missing bitmap")?;
                                    validate_bitmap(bitmap,*l,owner)?;
                                    let mut present=HashMap::<i32,Pubkey>::new();
                                    let width=l.tick_spacing*60;
                                    let center=l.tick_current.div_euclid(width)*width;
                                    for (start,addr) in arrays(*l,owner) {
                                        let initialized=array_initialized(parent,bitmap,start,width)?;
                                        match lookup.get(&addr).and_then(Option::as_ref) {
                                            Some(a)=>{
                                                let contains=validate_array(a,*l,start,owner)?;
                                                if contains!=initialized{return Ok(false);}
                                                if initialized{present.insert(start,addr);}
                                            },
                                            None if initialized=>return Ok(false),
                                            None=>{},
                                        }
                                    }
                                    let (up,down)=ordered_arrays(&present,center,width);
                                    if up.is_empty()&&down.is_empty(){return Ok(false);}
                                    let ev=p.completion_evidence.as_ref().context("no evidence")?;
                                    if ev.payload["direction_0_to_1_ready"].as_bool()!=Some(!down.is_empty())
                                        || ev.payload["direction_1_to_0_ready"].as_bool()!=Some(!up.is_empty())
                                        || ev.payload["base_a_array_addresses"]!=serde_json::json!(
                                            up.iter().map(ToString::to_string).collect::<Vec<_>>())
                                        || ev.payload["base_b_array_addresses"]!=serde_json::json!(
                                            down.iter().map(ToString::to_string).collect::<Vec<_>>())
                                        || ev.payload["config"].as_str()!=Some(l.config.to_string().as_str())
                                        || ev.payload["observation"].as_str()!=Some(l.observation.to_string().as_str())
                                        || ev.payload["vault0"].as_str()!=Some(l.vaults[0].to_string().as_str())
                                        || ev.payload["vault1"].as_str()!=Some(l.vaults[1].to_string().as_str()){
                                        return Ok(false);
                                    }
                                    let proofs=p.mint_proofs.as_ref().context("no mints")?;
                                    let m=p.execution_manifest.as_ref().context("no manifest")?;
                                    let mut candidate=make_manifest(*l,m.validated_slot,&up,&down,
                                        proofs[0].token_2022||proofs[1].token_2022,owner);
                                    if p.mint_a!=Some(l.mints[0]) {
                                        candidate=ExecutionManifest{
                                            pool:candidate.pool,mint_a:p.mint_a.context("mint A")?,
                                            mint_b:p.mint_b.context("mint B")?,
                                            validated_slot:candidate.validated_slot,
                                            bound_signer:candidate.bound_signer,
                                            base_a:candidate.base_b,base_b:candidate.base_a,
                                        };
                                    }
                                    Ok(candidate==**m)
                                })().unwrap_or(false);
                                if valid {
                                    if let Some(u)=CompletionUnchanged::from_pool(p,slot){
                                        outcomes.insert(p.address,CompletionOutcome::Unchanged(u));
                                    }
                                }
                            }
                        },
                    }
                }
            },
        }
    }
    let pending=requests.iter().filter(|r|!outcomes.contains_key(&r.pool.address))
        .map(|r|r.pool.clone()).collect::<Vec<_>>();
    if !pending.is_empty(){
        for proof in process(rpc,pending,settings).await {
            outcomes.insert(proof.pool,CompletionOutcome::Proof(proof));
        }
    }
    requests.iter().map(|r|outcomes.remove(&r.pool.address).expect("Raydium CLMM result")).collect()
}

pub async fn run(mut input:mpsc::Receiver<CompletionRequestBatch>,
    rpc:RpcHandle,settings:Settings,output:mpsc::Sender<CompletionResponseBatch>)->Result<()> {
    let mut tasks=JoinSet::new();
    let mut open=true;
    let mut processed=0_usize;
    crate::log_event!(Info,"raydium_clmm executor started parallel={} pools_per_batch={} scheduler=completion_lifecycle",
        settings.parallel_batches,settings.pools_per_batch());
    while open || !tasks.is_empty() {
        tokio::select! {
            batch=input.recv(),if open && tasks.len()<settings.parallel_batches=>match batch {
                Some(batch)=>{
                    ensure!(!batch.requests.is_empty() && batch.requests.len()<=settings.pools_per_batch(),
                        "invalid Raydium CLMM request batch size");
                    ensure!(batch.requests.iter().all(|r|r.pool.venue==Venue::RaydiumClmm
                        && r.pool.completion_admission.is_admitted()),
                        "Raydium CLMM request has wrong venue or missing admission");
                    let rpc=rpc.clone();
                    tasks.spawn(async move {
                        let results=match tokio::time::timeout(batch.timeout,
                            process_optimized(&rpc,&batch.requests,settings)).await {
                            Ok(results)=>results,
                            Err(_)=>batch.requests.iter().map(|r|CompletionOutcome::Proof(retry(r.pool.address,"Raydium CLMM lifecycle request timed out"))).collect(),
                        };
                        (batch.requests,results)
                    });
                },
                None=>open=false,
            },
            done=tasks.join_next(),if !tasks.is_empty()=>{
                let (requests,results)=done.context("missing Raydium CLMM request task")?
                    .context("Raydium CLMM request task panicked")?;
                ensure!(requests.len()==results.len(),"Raydium CLMM request/result count mismatch");
                let mut by_pool=results.into_iter().map(|outcome|(outcome.pool(),outcome)).collect::<HashMap<_,_>>();
                ensure!(by_pool.len()==requests.len(),"duplicate Raydium CLMM pool result");
                let mut responses=Vec::with_capacity(requests.len());
                for request in requests {
                    let outcome=by_pool.remove(&request.pool.address)
                        .context("Completion request has no matching pool result")?;
                    let kind=match &outcome {
                        CompletionOutcome::Proof(result)=>format!("{:?}",result.status),
                        CompletionOutcome::Unchanged(_)=>"Unchanged".to_owned(),
                    };
                    crate::log_event!(Debug,"completion_raydium_clmm completion request_id={} pool={} work={:?} outcome={}",
                        request.id,request.pool.address,request.work,kind);
                    responses.push(CompletionResponse{id:request.id,outcome});
                }
                processed+=responses.len();
                output.send(CompletionResponseBatch{source:SOURCE,responses}).await
                    .map_err(|_|anyhow!("Raydium CLMM lifecycle response consumer closed"))?;
            },
        }
    }
    crate::log_event!(Info,"raydium_clmm executor complete requests={processed}");
    Ok(())
}
