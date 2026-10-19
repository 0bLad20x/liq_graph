//! Raydium AMM v4 execution completion: Lifecycle requests -> coherent RPC -> V10 manifests.
//! Owns AMM v4 binary decoding, SPL mint/vault validation and evidence.
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

const SOURCE: &str = "raydium_amm_v4_execution_completion";
const PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const STATE_LEN: usize = 752;
const POOLS_PER_BATCH: usize = 16;
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
        ensure!((5..=100).contains(&account_batch_size),
            "Raydium AMM v4 requires RPC_ACCOUNT_BATCH_SIZE in 5..=100");
        Ok(Self {account_batch_size, parallel_batches:PARALLEL_BATCHES})
    }
    pub fn pools_per_batch(self) -> usize {
        POOLS_PER_BATCH.min(self.account_batch_size / 5)
    }
}
#[derive(Clone, Copy)]
struct PoolLayout {
    address: Pubkey,
    mints: [Pubkey;2],
    vaults: [Pubkey;2],
    decimals: [u8;2],
}
fn key(bytes:&[u8],offset:usize)->Result<Pubkey>{
    Ok(Pubkey::new_from_array(bytes.get(offset..offset+32)
        .context("truncated Raydium AMM v4 account")?.try_into()?))
}
fn u64_at(bytes:&[u8],offset:usize)->Result<u64>{
    Ok(u64::from_le_bytes(bytes.get(offset..offset+8)
        .context("truncated Raydium AMM v4 scalar")?.try_into()?))
}
fn decode_pool(account:&AccountData,expected:&PoolState,program:Pubkey)->Result<PoolLayout>{
    ensure!(account.address==expected.address && account.owner==program,
        "Raydium AMM v4 pool address or program owner mismatch");
    ensure!(account.view==AccountView::Full
        && account.original_len==Some(account.bytes.len())
        && account.bytes.len()==STATE_LEN,
        "Raydium AMM v4 requires the full 752-byte AmmInfo");
    // Raydium AmmInfo packed layout: status 0, coin/pc decimals 32/40,
    // coin/pc vaults 336/368 and coin/pc mints 400/432.
    ensure!(u64_at(&account.bytes,0)?==6,
        "V10 requires Raydium AMM v4 swap-only status 6");
    let decimal_values=[u64_at(&account.bytes,32)?,u64_at(&account.bytes,40)?];
    ensure!(decimal_values.iter().all(|n|*n<=u64::from(u8::MAX)),
        "Raydium AMM v4 decimals outside SPL mint range");
    let mints=[key(&account.bytes,400)?,key(&account.bytes,432)?];
    let vaults=[key(&account.bytes,336)?,key(&account.bytes,368)?];
    ensure!(mints[0]!=Pubkey::default() && mints[1]!=Pubkey::default()
        && mints[0]!=mints[1] && vaults[0]!=Pubkey::default()
        && vaults[1]!=Pubkey::default() && vaults[0]!=vaults[1],
        "Raydium AMM v4 mint or vault identity invalid");
    let (Some(a),Some(b))=(expected.mint_a,expected.mint_b) else {
        bail!("Raydium AMM v4 Registry mints absent");
    };
    // Raydium REST can provide the same pair in opposite mint order.
    ensure!(mints==[a,b]||mints==[b,a],
        "Raydium AMM v4 on-chain mints disagree with Registry");
    Ok(PoolLayout{address:account.address,mints,vaults,
        decimals:[decimal_values[0] as u8,decimal_values[1] as u8]})
}
fn decode_mint(account:&AccountData,spl:Pubkey,decimals:u8)->Result<MintProof>{
    ensure!(account.owner==spl && account.view==AccountView::Full
        && account.original_len==Some(account.bytes.len())
        && account.bytes.len()==82 && account.bytes[45]==1,
        "V10 Raydium AMM v4 requires an initialized classic SPL mint");
    ensure!(account.bytes[44]==decimals,"Raydium AMM v4 pool/mint decimals differ");
    Ok(MintProof{address:account.address,token_2022:false,
        extensions:Vec::new(),unsupported:Vec::new()})
}
fn validate_vault(account:&AccountData,mint:Pubkey,spl:Pubkey,authority:Pubkey)->Result<()>{
    ensure!(account.owner==spl && account.view==AccountView::Full
        && account.original_len==Some(account.bytes.len())
        && account.bytes.len()==165,
        "Raydium AMM v4 vault must be a full classic SPL Token account");
    ensure!(key(&account.bytes,0)?==mint && key(&account.bytes,32)?==authority,
        "Raydium AMM v4 vault mint or authority mismatch");
    ensure!(account.bytes[108]==1,"Raydium AMM v4 vault not initialized");
    Ok(())
}
struct ReadBatch{slot:u64,accounts:Vec<Option<AccountData>>}
async fn read(rpc:&RpcHandle,addresses:Vec<Pubkey>,min_slot:Option<u64>)->Result<ReadBatch>{
    ensure!(!addresses.is_empty(),"empty Raydium AMM v4 RPC request");
    let expected=addresses.clone();
    let response=rpc.call(RpcOperation::Multiple{
        addresses,view:AccountView::Full,min_context_slot:min_slot,
    }).await?;
    let RpcAnswer::Multiple{slot,accounts}=response.answer
        else{bail!("Raydium AMM v4 RPC returned wrong answer type");};
    ensure!(accounts.len()==expected.len()
        && min_slot.is_none_or(|min|slot>=min),
        "Raydium AMM v4 RPC cardinality/minContextSlot mismatch");
    for (address,account) in expected.iter().zip(&accounts){
        if let Some(account)=account {
            ensure!(account.address==*address && account.view==AccountView::Full
                && account.original_len==Some(account.bytes.len()),
                "Raydium AMM v4 RPC account address/full-read mismatch");
        }
    }
    crate::log_event!(Debug,"raydium amm v4 completion rpc provider={} slot={} accounts={} present={}",
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
    authority:Pubkey)->ExecutionManifest {
    let ro=|address|AccountMeta{address,writable:false};
    let rw=|address|AccountMeta{address,writable:true};
    let block=|base:Pubkey,x:Pubkey|{
        let xi=if x==layout.mints[0]{0}else{1};
        let bi=if base==layout.mints[0]{0}else{1};
        // Official V10 RaydiumAmm: program, quote mint, authority PDA,
        // pool, x vault, quote vault. Six accounts; no Serum/OpenBook.
        Arc::<[AccountMeta]>::from(vec![
            ro(program),ro(base),ro(authority),rw(layout.address),
            rw(layout.vaults[xi]),rw(layout.vaults[bi]),
        ])
    };
    let a=pool.mint_a.expect("validated Registry mint A");
    let b=pool.mint_b.expect("validated Registry mint B");
    ExecutionManifest{pool:layout.address,mint_a:a,mint_b:b,validated_slot:slot,
        base_a:block(a,b),base_b:block(b,a)}
}
fn decode_final(pool:&PoolState,initial:PoolLayout,slot:u64,
    lookup:&HashMap<Pubkey,Option<AccountData>>,program:Pubkey,spl:Pubkey)->CompletionResult {
    let fetch=|address:Pubkey|lookup.get(&address).and_then(Option::as_ref);
    let Some(account)=fetch(pool.address) else{
        return retry(pool.address,"Raydium AMM v4 pool missing from final snapshot");
    };
    let current=match decode_pool(account,pool,program){
        Ok(v)=>v,
        Err(e)=>return incomplete(pool.address,slot,format!("Raydium AMM v4 final pool invalid: {e:#}")),
    };
    if current.mints!=initial.mints || current.vaults!=initial.vaults
        || current.decimals!=initial.decimals {
        return retry(pool.address,"Raydium AMM v4 pool changed during RPC validation");
    }
    let (Some(m0),Some(m1),Some(v0),Some(v1))=(
        fetch(current.mints[0]),fetch(current.mints[1]),
        fetch(current.vaults[0]),fetch(current.vaults[1])
    ) else{return retry(pool.address,"Raydium AMM v4 mint/vault account missing");};
    let proof0=match decode_mint(m0,spl,current.decimals[0]){
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Raydium AMM v4 mint 0 invalid: {e:#}")),
    };
    let proof1=match decode_mint(m1,spl,current.decimals[1]){
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Raydium AMM v4 mint 1 invalid: {e:#}")),
    };
    let authority=Pubkey::find_program_address(&[b"amm authority"],&program).0;
    if let Err(e)=validate_vault(v0,current.mints[0],spl,authority){
        return incomplete(pool.address,slot,format!("Raydium AMM v4 vault 0 invalid: {e:#}"));
    }
    if let Err(e)=validate_vault(v1,current.mints[1],spl,authority){
        return incomplete(pool.address,slot,format!("Raydium AMM v4 vault 1 invalid: {e:#}"));
    }
    let evidence=Arc::new(CompletionEvidence{
        kind:"raydium_amm_v4_pool".to_owned(),version:1,
        payload:Arc::new(json!({
            "program":program.to_string(),"status":6,
            "authority":authority.to_string(),
            "onchain_mint_0":current.mints[0].to_string(),
            "onchain_mint_1":current.mints[1].to_string(),
            "onchain_vault_0":current.vaults[0].to_string(),
            "onchain_vault_1":current.vaults[1].to_string(),
            "token_program":spl.to_string(),
        })),
    });
    let proofs=if pool.mint_a==Some(current.mints[0]){
        [proof0,proof1]
    }else{[proof1,proof0]};
    result(pool.address,CompletionStatus::Prepared,Some(slot),None,
        Some(evidence),Some(proofs),
        Some(Arc::new(manifest(current,pool,slot,program,authority))))
}
async fn process(rpc:&RpcHandle,pools:Vec<PoolState>,settings:Settings)->Vec<CompletionResult>{
    let program:Pubkey=PROGRAM.parse().expect("fixed Raydium AMM v4 program ID");
    let spl:Pubkey=SPL_TOKEN.parse().expect("fixed SPL Token program ID");
    let mut results:Vec<Option<CompletionResult>>=vec![None;pools.len()];
    let first=match read(rpc,pools.iter().map(|p|p.address).collect(),None).await{
        Ok(v)=>v,
        Err(e)=>return pools.iter().map(|p|
            retry(p.address,format!("Raydium AMM v4 pool RPC failed: {e:#}"))).collect(),
    };
    let mut pending=Vec::new();
    let mut addresses=Vec::new();
    let mut dedup=HashSet::new();
    for (i,pool) in pools.iter().enumerate(){
        let Some(account)=first.accounts[i].as_ref() else {
            results[i]=Some(retry(pool.address,"Raydium AMM v4 pool account missing"));
            continue;
        };
        match decode_pool(account,pool,program){
            Ok(layout)=>{
                for addr in [layout.address,layout.mints[0],layout.mints[1],
                    layout.vaults[0],layout.vaults[1]]{
                    if dedup.insert(addr){addresses.push(addr);}
                }
                pending.push((i,layout));
            }
            Err(e)=>results[i]=Some(incomplete(pool.address,first.slot,
                format!("Raydium AMM v4 pool invalid: {e:#}"))),
        }
    }
    if !pending.is_empty(){
        if addresses.len()>settings.account_batch_size{
            for (i,_) in pending{
                results[i]=Some(retry(pools[i].address,
                    "Raydium AMM v4 RPC set exceeds shared account batch limit"));
            }
            return results.into_iter().map(|r|r.expect("pool result")).collect();
        }
        match read(rpc,addresses.clone(),Some(first.slot)).await{
            Ok(batch)=>{
                let lookup:HashMap<Pubkey,Option<AccountData>>=
                    addresses.into_iter().zip(batch.accounts).collect();
                for (i,layout) in pending{
                    results[i]=Some(decode_final(&pools[i],layout,batch.slot,
                        &lookup,program,spl));
                }
            }
            Err(e)=>{
                for (i,_) in pending{
                    results[i]=Some(retry(pools[i].address,
                        format!("Raydium AMM v4 final RPC failed: {e:#}")));
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
    crate::log_event!(Info,"raydium_amm_v4 executor started parallel={} pools_per_batch={} scheduler=completion_lifecycle",
        settings.parallel_batches,settings.pools_per_batch());
    while open || !tasks.is_empty() {
        tokio::select! {
            batch=input.recv(),if open && tasks.len()<settings.parallel_batches=>match batch {
                Some(batch)=>{
                    ensure!(!batch.requests.is_empty() && batch.requests.len()<=settings.pools_per_batch(),
                        "invalid Raydium AMM v4 request batch size");
                    ensure!(batch.requests.iter().all(|r|r.pool.venue==Venue::RaydiumAmmV4
                        && r.pool.completion_admission.is_admitted()),
                        "Raydium AMM v4 request has wrong venue or missing admission");
                    let rpc=rpc.clone();
                    tasks.spawn(async move {
                        let pools=batch.requests.iter().map(|r|r.pool.clone()).collect::<Vec<_>>();
                        let results=match tokio::time::timeout(batch.timeout,
                            process(&rpc,pools,settings)).await {
                            Ok(results)=>results,
                            Err(_)=>batch.requests.iter().map(|r|retry(r.pool.address,
                                "Raydium AMM v4 lifecycle request timed out")).collect(),
                        };
                        (batch.requests,results)
                    });
                },
                None=>open=false,
            },
            done=tasks.join_next(),if !tasks.is_empty()=>{
                let (requests,results)=done.context("missing Raydium AMM v4 request task")?
                    .context("Raydium AMM v4 request task panicked")?;
                ensure!(requests.len()==results.len(),"Raydium AMM v4 request/result count mismatch");
                let mut by_pool=results.into_iter().map(|r|(r.pool,r)).collect::<HashMap<_,_>>();
                ensure!(by_pool.len()==requests.len(),"duplicate Raydium AMM v4 pool result");
                let mut responses=Vec::with_capacity(requests.len());
                for request in requests {
                    let result=by_pool.remove(&request.pool.address)
                        .context("Raydium AMM v4 request has no matching pool result")?;
                    crate::log_event!(Debug,
                        "raydium_amm_v4 executor result request_id={} pool={} work={:?} status={:?} reason_code={:?} slot={:?} reason={}",
                        request.id,result.pool,request.work,result.status,result.reason_code,
                        result.validated_slot,result.reason.as_deref().unwrap_or("-"));
                    responses.push(CompletionResponse{id:request.id,result});
                }
                processed+=responses.len();
                output.send(CompletionResponseBatch{source:SOURCE,responses}).await
                    .map_err(|_|anyhow!("Raydium AMM v4 lifecycle response consumer closed"))?;
            },
        }
    }
    crate::log_event!(Info,"raydium_amm_v4 executor complete requests={processed}");
    Ok(())
}
