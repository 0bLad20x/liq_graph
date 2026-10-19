//! Raydium CPMM execution completion: Lifecycle requests -> coherent RPC -> V10 manifests.
//! Owns CPMM binary decoding, token-program/mint/vault checks and evidence.
//! No direct DB, REST parsing, graph mutations or provider-specific RPC code.
use std::{collections::{HashMap, HashSet}, env, sync::Arc};

use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::{
    sync::mpsc,
    task::JoinSet,
};

use crate::{
    core::{CompletionReason,
        AccountMeta, CompletionEvidence, CompletionResult, CompletionRequestBatch, CompletionResponse, CompletionResponseBatch,
        CompletionStatus, ExecutionManifest, MintProof, PoolState,
        Venue,
    },
    workers::rpc_transport::{AccountData, AccountView, RpcAnswer, RpcHandle, RpcOperation},
};

const SOURCE: &str = "raydium_cpmm_execution_completion";
const PROGRAM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const POOL_DISC: [u8;8] = [247,237,227,245,215,195,222,70];
const CONFIG_DISC: [u8;8] = [218,244,33,104,203,203,43,111];
const OBS_DISC: [u8;8] = [122,174,197,53,129,9,165,132];
const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const STATE_LEN: usize = 637;
const CONFIG_LEN: usize = 236;
const OBS_LEN: usize = 4075;
const POOLS_PER_BATCH: usize = 12;
const PARALLEL_BATCHES: usize = 16;

#[derive(Clone, Copy)]
pub struct Settings {
    account_batch_size: usize,
    pub parallel_batches: usize,
}
impl Settings {
    pub fn from_env() -> Result<Self> {
        // Shared RPC_ACCOUNT_BATCH_SIZE; no Raydium-specific RPC transport configuration.
        let account_batch_size = env::var("RPC_ACCOUNT_BATCH_SIZE")
            .unwrap_or_else(|_| "100".into()).parse::<usize>()
            .context("invalid RPC_ACCOUNT_BATCH_SIZE")?;
        ensure!((7..=100).contains(&account_batch_size),
            "Raydium CPMM requires RPC_ACCOUNT_BATCH_SIZE in 7..=100");
        Ok(Self {account_batch_size, parallel_batches:PARALLEL_BATCHES})
    }
    pub fn pools_per_batch(self) -> usize {
        POOLS_PER_BATCH.min(self.account_batch_size / 7)
    }
}
#[derive(Clone, Copy)]
struct PoolLayout {
    address: Pubkey,
    config: Pubkey,
    observation: Pubkey,
    mints: [Pubkey;2],
    vaults: [Pubkey;2],
    programs: [Pubkey;2],
    decimals: [u8;2],
    auth_bump: u8,
    status: u8,
    open_time: u64,
}
fn key(bytes:&[u8],offset:usize)->Result<Pubkey>{
    Ok(Pubkey::new_from_array(bytes.get(offset..offset+32)
        .context("truncated Raydium CPMM account")?.try_into()?))
}
fn u64_at(bytes:&[u8],offset:usize)->Result<u64>{
    Ok(u64::from_le_bytes(bytes.get(offset..offset+8)
        .context("truncated Raydium CPMM scalar")?.try_into()?))
}
fn decode_pool(account:&AccountData,expected:&PoolState,program:Pubkey)->Result<PoolLayout>{
    ensure!(account.address==expected.address && account.owner==program,
        "Raydium CPMM pool identity/program owner mismatch");
    ensure!(account.view==AccountView::Full && account.original_len==Some(account.bytes.len())
        && account.bytes.len()==STATE_LEN && account.bytes[..8]==POOL_DISC,
        "Raydium CPMM requires a full 637-byte PoolState with matching discriminator");
    let data=&account.bytes;
    let config=key(data,8)?;
    let vaults=[key(data,72)?,key(data,104)?];
    let mints=[key(data,168)?,key(data,200)?];
    let programs=[key(data,232)?,key(data,264)?];
    let observation=key(data,296)?;
    let auth_bump=data[328];
    let status=data[329];
    let decimals=[data[331],data[332]];
    let open_time=u64_at(data,373)?;
    // Raydium PoolStatusBitIndex::Swap is bit 2; disabled when bit 4 is set.
    ensure!(status&4==0,"Raydium CPMM swaps disabled in PoolState");
    ensure!(open_time<=crate::core::now_unix_ms()?/1000,
        "Raydium CPMM pool swap open_time is in the future");
    ensure!(mints[0]!=Pubkey::default() && mints[1]!=Pubkey::default()
        && mints[0]!=mints[1] && vaults[0]!=Pubkey::default()
        && vaults[1]!=Pubkey::default() && vaults[0]!=vaults[1]
        && config!=Pubkey::default() && observation!=Pubkey::default(),
        "Raydium CPMM pool account identities invalid");
    let (Some(a),Some(b))=(expected.mint_a,expected.mint_b) else {
        bail!("Raydium CPMM Registry mint identity absent");
    };
    ensure!(mints==[a,b]||mints==[b,a],"Raydium CPMM mints disagree with Registry");
    let spl:Pubkey=SPL_TOKEN.parse()?;
    let token_2022:Pubkey=TOKEN_2022.parse()?;
    ensure!(programs.iter().all(|p|*p==spl||*p==token_2022),
        "Raydium CPMM pool has unsupported token program");
    let (_,expected_bump)=Pubkey::find_program_address(
        &[b"vault_and_lp_mint_auth_seed"],&program);
    ensure!(auth_bump==expected_bump,"Raydium CPMM pool authority bump mismatch");
    for i in 0..2 {
        let vault_pda=Pubkey::find_program_address(
            &[b"pool_vault",account.address.as_ref(),mints[i].as_ref()],&program).0;
        ensure!(vaults[i]==vault_pda,"Raydium CPMM vault PDA mismatch");
    }
    let oracle_pda=Pubkey::find_program_address(
        &[b"observation",account.address.as_ref()],&program).0;
    ensure!(observation==oracle_pda,"Raydium CPMM observation PDA mismatch");
    Ok(PoolLayout{address:account.address,config,observation,mints,vaults,programs,
        decimals,auth_bump,status,open_time})
}
fn decode_mint(account:&AccountData,token_program:Pubkey,decimals:u8)->Result<MintProof>{
    ensure!(account.owner==token_program && account.view==AccountView::Full
        && account.original_len==Some(account.bytes.len()),
        "Raydium CPMM mint program/full-read mismatch");
    let spl:Pubkey=SPL_TOKEN.parse()?;
    let token_2022:Pubkey=TOKEN_2022.parse()?;
    let is_2022=if token_program==spl {false}
        else if token_program==token_2022 {true}
        else {bail!("Raydium CPMM unsupported mint token program");};
    let data=&account.bytes;
    ensure!(data.len()>=82 && data[45]==1 && data[44]==decimals,
        "Raydium CPMM mint initialization/decimals mismatch");
    let mut extensions=Vec::new();
    if is_2022 && data.len()>82 {
        ensure!(data.len()>=166 && data[82..165].iter().all(|b|*b==0)
            && data[165]==1,"Raydium CPMM Token-2022 mint extension framing invalid");
        let mut pos=166;
        while pos<data.len(){
            let tail=&data[pos..];
            if tail.iter().all(|b|*b==0){break;}
            ensure!(tail.len()>=4,"Raydium CPMM truncated Token-2022 mint extension");
            let kind=u16::from_le_bytes(tail[..2].try_into()?);
            let len=usize::from(u16::from_le_bytes(tail[2..4].try_into()?));
            if kind==0 && len==0 {
                ensure!(tail[4..].iter().all(|b|*b==0),
                    "Raydium CPMM nonzero Token-2022 TLV terminator trailing data");
                break;
            }
            ensure!(kind!=0 && len<=tail.len()-4,
                "Raydium CPMM invalid Token-2022 mint extension");
            extensions.push(kind);
            pos+=4+len;
        }
    } else if !is_2022 {
        ensure!(data.len()==82,"Raydium CPMM invalid classic SPL mint length");
    }
    extensions.sort_unstable();
    ensure!(!extensions.windows(2).any(|w|w[0]==w[1]),
        "Raydium CPMM duplicate Token-2022 mint extensions");
    // V10's existing Token-2022 policy: transfer-fee is supported, transfer
    // hooks/confidential-transfer related extensions are excluded.
    let unsupported=extensions.iter().copied()
        .filter(|v|matches!(*v,4|9|14|16|24)).collect();
    Ok(MintProof{address:account.address,token_2022:is_2022,
        extensions,unsupported})
}
fn validate_vault(account:&AccountData,mint:Pubkey,token_program:Pubkey,
    authority:Pubkey)->Result<()>{
    ensure!(account.owner==token_program && account.view==AccountView::Full
        && account.original_len==Some(account.bytes.len())
        && account.bytes.len()>=165,
        "Raydium CPMM vault token program/full-read mismatch");
    ensure!(key(&account.bytes,0)?==mint && key(&account.bytes,32)?==authority
        && account.bytes[108]==1,
        "Raydium CPMM vault mint/authority/initialized state mismatch");
    Ok(())
}
fn validate_config(account:&AccountData,layout:PoolLayout,program:Pubkey)->Result<()>{
    ensure!(account.owner==program && account.address==layout.config
        && account.bytes.len()==CONFIG_LEN && account.bytes[..8]==CONFIG_DISC,
        "Raydium CPMM AMM Config program/length/discriminator invalid");
    let index=u16::from_le_bytes(account.bytes[10..12].try_into()?);
    let (pda,bump)=Pubkey::find_program_address(
        &[b"amm_config",&index.to_be_bytes()],&program);
    ensure!(account.address==pda && account.bytes[8]==bump,
        "Raydium CPMM AMM Config PDA or bump invalid");
    Ok(())
}
fn validate_observation(account:&AccountData,layout:PoolLayout,program:Pubkey)->Result<()>{
    ensure!(account.owner==program && account.address==layout.observation
        && account.bytes.len()==OBS_LEN && account.bytes[..8]==OBS_DISC
        && matches!(account.bytes[8],0|1)
        && key(&account.bytes,11)?==layout.address,
        "Raydium CPMM Observation account owner/state/pool identity invalid");
    // New observation accounts stay uninitialized until the first executed swap.
    let index=u16::from_le_bytes(account.bytes[9..11].try_into()?);
    ensure!(index<100,"Raydium CPMM observation index invalid");
    Ok(())
}
struct ReadBatch{slot:u64,accounts:Vec<Option<AccountData>>}
async fn read(rpc:&RpcHandle,addresses:Vec<Pubkey>,min_slot:Option<u64>)->Result<ReadBatch>{
    ensure!(!addresses.is_empty(),"empty Raydium CPMM RPC request");
    let expected=addresses.clone();
    let response=rpc.call(RpcOperation::Multiple{
        addresses,view:AccountView::Full,min_context_slot:min_slot,
    }).await?;
    let RpcAnswer::Multiple{slot,accounts}=response.answer
        else{bail!("Raydium CPMM RPC returned wrong answer type");};
    ensure!(accounts.len()==expected.len()
        && min_slot.is_none_or(|min|slot>=min),
        "Raydium CPMM RPC cardinality/minContextSlot mismatch");
    for (address,account) in expected.iter().zip(&accounts){
        if let Some(account)=account {
            ensure!(account.address==*address && account.view==AccountView::Full
                && account.original_len==Some(account.bytes.len()),
                "Raydium CPMM RPC account address/full-read mismatch");
        }
    }
    crate::log_event!(Debug,"raydium cpmm completion rpc provider={} slot={} accounts={} present={}",
        response.provider,slot,accounts.len(),accounts.iter().filter(|a|a.is_some()).count());
    Ok(ReadBatch{slot,accounts})
}
fn result(pool:Pubkey,status:CompletionStatus,slot:Option<u64>,
    reason:Option<String>,evidence:Option<Arc<CompletionEvidence>>,
    mints:Option<[MintProof;2]>,manifest:Option<Arc<ExecutionManifest>>)->CompletionResult {
    CompletionResult::new(pool,status,slot,reason,evidence,mints,manifest)
}
fn retry(pool:Pubkey,reason:impl Into<String>)->CompletionResult {
    result(pool,CompletionStatus::Retryable,None,Some(reason.into()),None,None,None)
}
fn incomplete(pool:Pubkey,slot:u64,reason:impl Into<String>)->CompletionResult {
    result(pool,CompletionStatus::Incomplete,Some(slot),Some(reason.into()),None,None,None)
}
fn manifest(layout:PoolLayout,pool:&PoolState,slot:u64,program:Pubkey,
    authority:Pubkey)->ExecutionManifest{
    let ro=|address|AccountMeta{address,writable:false};
    let rw=|address|AccountMeta{address,writable:true};
    let block=|base:Pubkey,x:Pubkey|{
        let xi=if x==layout.mints[0]{0}else{1};
        let bi=if base==layout.mints[0]{0}else{1};
        // V10 CPMM: program, quote mint, authority, pool, config,
        // x vault, quote vault, observation. Eight accounts.
        Arc::<[AccountMeta]>::from(vec![
            ro(program),ro(base),ro(authority),rw(layout.address),
            ro(layout.config),rw(layout.vaults[xi]),rw(layout.vaults[bi]),
            rw(layout.observation),
        ])
    };
    let a=pool.mint_a.expect("validated Registry mint A");
    let b=pool.mint_b.expect("validated Registry mint B");
    ExecutionManifest{pool:layout.address,mint_a:a,mint_b:b,validated_slot:slot,
        bound_signer:None,
        base_a:block(a,b),base_b:block(b,a)}
}
fn decode_final(pool:&PoolState,initial:PoolLayout,slot:u64,
    lookup:&HashMap<Pubkey,Option<AccountData>>,program:Pubkey)->CompletionResult{
    let fetch=|address:Pubkey|lookup.get(&address).and_then(Option::as_ref);
    let Some(account)=fetch(pool.address) else {
        return retry(pool.address,"Raydium CPMM pool missing from final RPC snapshot");
    };
    let current=match decode_pool(account,pool,program){
        Ok(v)=>v,
        Err(e)=>return incomplete(pool.address,slot,
            format!("Raydium CPMM final PoolState invalid: {e:#}")),
    };
    if current.mints!=initial.mints || current.vaults!=initial.vaults
        || current.programs!=initial.programs || current.config!=initial.config
        || current.observation!=initial.observation || current.decimals!=initial.decimals
        || current.auth_bump!=initial.auth_bump {
        return retry(pool.address,"Raydium CPMM pool metadata changed during RPC validation");
    }
    let (Some(m0),Some(m1),Some(v0),Some(v1),Some(config),Some(observation))=(
        fetch(current.mints[0]),fetch(current.mints[1]),
        fetch(current.vaults[0]),fetch(current.vaults[1]),
        fetch(current.config),fetch(current.observation)
    ) else {return retry(pool.address,"Raydium CPMM mint/vault/config/observation unavailable");};
    if let Err(e)=validate_config(config,current,program){
        return incomplete(pool.address,slot,format!("Raydium CPMM AMM Config invalid: {e:#}"));
    }
    if let Err(e)=validate_observation(observation,current,program){
        return incomplete(pool.address,slot,format!("Raydium CPMM Observation invalid: {e:#}"));
    }
    let proof0=match decode_mint(m0,current.programs[0],current.decimals[0]){
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,
            format!("Raydium CPMM mint 0 invalid: {e:#}")),
    };
    let proof1=match decode_mint(m1,current.programs[1],current.decimals[1]){
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,
            format!("Raydium CPMM mint 1 invalid: {e:#}")),
    };
    let authority=Pubkey::find_program_address(&[b"vault_and_lp_mint_auth_seed"],&program).0;
    if let Err(e)=validate_vault(v0,current.mints[0],current.programs[0],authority){
        return incomplete(pool.address,slot,format!("Raydium CPMM vault 0 invalid: {e:#}"));
    }
    if let Err(e)=validate_vault(v1,current.mints[1],current.programs[1],authority){
        return incomplete(pool.address,slot,format!("Raydium CPMM vault 1 invalid: {e:#}"));
    }
    let evidence=Arc::new(CompletionEvidence{
        kind:"raydium_cpmm_pool".to_owned(),version:1,
        payload:Arc::new(json!({
            "program":program.to_string(),
            "amm_config":current.config.to_string(),
            "observation":current.observation.to_string(),
            "authority":authority.to_string(),
            "status":current.status,
            "open_time":current.open_time,
            "onchain_mint_0":current.mints[0].to_string(),
            "onchain_mint_1":current.mints[1].to_string(),
            "onchain_vault_0":current.vaults[0].to_string(),
            "onchain_vault_1":current.vaults[1].to_string(),
            "token_program_0":current.programs[0].to_string(),
            "token_program_1":current.programs[1].to_string(),
        }))
    });
    let proofs=if pool.mint_a==Some(current.mints[0]){[proof0,proof1]}
        else{[proof1,proof0]};
    if proofs.iter().any(|p|!p.unsupported.is_empty()){
        return result(pool.address,CompletionStatus::Incomplete,Some(slot),
            Some(format!("V10 unsupported Token-2022 mint extensions: {:?} {:?}",
                proofs[0].unsupported,proofs[1].unsupported)),
            Some(evidence),Some(proofs),None).with_reason_code(CompletionReason::UnsupportedConfiguration);
    }
    result(pool.address,CompletionStatus::Prepared,Some(slot),None,
        Some(evidence),Some(proofs),
        Some(Arc::new(manifest(current,pool,slot,program,authority))))
}
async fn process(rpc:&RpcHandle,pools:Vec<PoolState>,settings:Settings)->Vec<CompletionResult>{
    let program:Pubkey=PROGRAM.parse().expect("fixed Raydium CPMM program ID");
    let mut results:Vec<Option<CompletionResult>>=vec![None;pools.len()];
    let first=match read(rpc,pools.iter().map(|p|p.address).collect(),None).await {
        Ok(v)=>v,
        Err(e)=>return pools.iter().map(|p|
            retry(p.address,format!("Raydium CPMM pool RPC failed: {e:#}"))).collect(),
    };
    let mut pending=Vec::new();
    let mut addresses=Vec::new();
    let mut unique=HashSet::new();
    for (i,pool) in pools.iter().enumerate(){
        let Some(account)=first.accounts[i].as_ref() else {
            results[i]=Some(retry(pool.address,"Raydium CPMM PoolState missing"));
            continue;
        };
        match decode_pool(account,pool,program){
            Ok(layout)=>{
                for addr in [layout.address,layout.config,layout.observation,
                    layout.mints[0],layout.mints[1],
                    layout.vaults[0],layout.vaults[1]]{
                    if unique.insert(addr){addresses.push(addr);}
                }
                pending.push((i,layout));
            }
            Err(e)=>results[i]=Some(incomplete(pool.address,first.slot,
                format!("Raydium CPMM PoolState invalid: {e:#}"))),
        }
    }
    if !pending.is_empty(){
        if addresses.len()>settings.account_batch_size {
            for (i,_) in pending{
                results[i]=Some(retry(pools[i].address,
                    "Raydium CPMM RPC account set exceeds configured batch limit"));
            }
            return results.into_iter().map(|r|r.expect("pool result")).collect();
        }
        match read(rpc,addresses.clone(),Some(first.slot)).await{
            Ok(batch)=>{
                let lookup:HashMap<Pubkey,Option<AccountData>>=
                    addresses.into_iter().zip(batch.accounts).collect();
                for (i,layout) in pending{
                    results[i]=Some(decode_final(&pools[i],layout,batch.slot,&lookup,program));
                }
            }
            Err(e)=>{
                for (i,_) in pending{
                    results[i]=Some(retry(pools[i].address,
                        format!("Raydium CPMM final RPC failed: {e:#}")));
                }
            }
        }
    }
    results.into_iter().map(|r|r.expect("pool result")).collect()
}

/// Protocol executor only. The shared lifecycle owns admission scheduling,
/// retry/backoff, reactivation and Prepared refresh.
pub async fn run(mut input:mpsc::Receiver<CompletionRequestBatch>,
    rpc:RpcHandle,settings:Settings,output:mpsc::Sender<CompletionResponseBatch>)->Result<()> {
    let mut tasks=JoinSet::new();
    let mut open=true;
    let mut processed=0_usize;
    crate::log_event!(Info,"raydium_cpmm executor started parallel={} pools_per_batch={} scheduler=completion_lifecycle",
        settings.parallel_batches,settings.pools_per_batch());
    while open || !tasks.is_empty() {
        tokio::select! {
            batch=input.recv(),if open && tasks.len()<settings.parallel_batches=>match batch {
                Some(batch)=>{
                    ensure!(!batch.requests.is_empty() && batch.requests.len()<=settings.pools_per_batch(),
                        "invalid Raydium CPMM request batch size");
                    ensure!(batch.requests.iter().all(|r|r.pool.venue==Venue::RaydiumCpmm
                        && r.pool.completion_admission.is_admitted()),
                        "Raydium CPMM request has wrong venue or missing admission");
                    let rpc=rpc.clone();
                    tasks.spawn(async move {
                        let pools=batch.requests.iter().map(|r|r.pool.clone()).collect::<Vec<_>>();
                        let results=match tokio::time::timeout(batch.timeout,
                            process(&rpc,pools,settings)).await {
                            Ok(results)=>results,
                            Err(_)=>batch.requests.iter().map(|r|retry(r.pool.address,
                                "Raydium CPMM lifecycle request timed out")).collect(),
                        };
                        (batch.requests,results)
                    });
                },
                None=>open=false,
            },
            done=tasks.join_next(),if !tasks.is_empty()=>{
                let (requests,results)=done.context("missing Raydium CPMM request task")?
                    .context("Raydium CPMM request task panicked")?;
                ensure!(requests.len()==results.len(),"Raydium CPMM request/result count mismatch");
                let mut by_pool=results.into_iter().map(|r|(r.pool,r)).collect::<HashMap<_,_>>();
                ensure!(by_pool.len()==requests.len(),"duplicate Raydium CPMM pool result");
                let mut responses=Vec::with_capacity(requests.len());
                for request in requests {
                    let result=by_pool.remove(&request.pool.address)
                        .context("Raydium CPMM request has no matching pool result")?;
                    crate::log_event!(Debug,
                        "raydium_cpmm executor result request_id={} pool={} work={:?} status={:?} reason_code={:?} slot={:?} reason={}",
                        request.id,result.pool,request.work,result.status,result.reason_code,
                        result.validated_slot,result.reason.as_deref().unwrap_or("-"));
                    responses.push(CompletionResponse{id:request.id,result});
                }
                processed+=responses.len();
                output.send(CompletionResponseBatch{source:SOURCE,responses}).await
                    .map_err(|_|anyhow!("Raydium CPMM lifecycle response consumer closed"))?;
            },
        }
    }
    crate::log_event!(Info,"raydium_cpmm executor complete requests={processed}");
    Ok(())
}
