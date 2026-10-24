//! Manifest DEX execution completion: Lifecycle requests -> RPC -> validated V10 manifest.
//! All MarketFixed/free-list and token/vault decoding is private to this worker.
use std::{collections::{HashMap, HashSet}, env, sync::Arc};
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::{sync::mpsc, task::JoinSet};

use crate::{
    core::{CompletionReason, AccountMeta, CompletionEvidence, CompletionResult, CompletionOutcome, CompletionRequest, CompletionUnchanged, CompletionRequestBatch,
        CompletionResponse, CompletionResponseBatch, CompletionStatus, ExecutionManifest, MintProof,
        PoolState, Venue},
    workers::rpc_transport::{AccountData, AccountView, RpcAnswer, RpcHandle, RpcOperation},
};

const SOURCE: &str = "manifest_execution_completion";
const PROGRAM: &str = "MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms";
const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const MARKET_DISC: u64 = 4859840929024028656;
const MARKET_HEADER_LEN: usize = 256;
const NODE_LEN: usize = 80;
const POOLS_PER_BATCH: usize = 10;
const PARALLEL_BATCHES: usize = 16;

#[derive(Clone, Copy)]
pub struct Settings {
    account_batch_size: usize,
    pub parallel_batches: usize,
}
impl Settings {
    pub fn from_env() -> Result<Self> {
        let account_batch_size = env::var("RPC_ACCOUNT_BATCH_SIZE")
            .unwrap_or_else(|_| "100".into()).parse::<usize>()
            .context("invalid RPC_ACCOUNT_BATCH_SIZE")?;
        ensure!((5..=100).contains(&account_batch_size),
            "Manifest requires RPC_ACCOUNT_BATCH_SIZE in 5..=100");
        Ok(Self {account_batch_size,parallel_batches:PARALLEL_BATCHES})
    }
    pub fn pools_per_batch(self) -> usize {
        POOLS_PER_BATCH.min(self.account_batch_size / 5)
    }
}

#[derive(Clone, Copy)]
struct Layout {
    address: Pubkey,
    mints: [Pubkey;2],
    vaults: [Pubkey;2],
}

fn read<const N: usize>(d: &[u8], offset: usize) -> Result<[u8;N]> {
    Ok(d.get(offset..offset + N).context("truncated Manifest account field")?.try_into()?)
}
fn key(d: &[u8], offset: usize) -> Result<Pubkey> {
    Ok(Pubkey::new_from_array(read::<32>(d,offset)?))
}
fn full(account: &AccountData) -> bool {
    account.view == AccountView::Full
        && account.original_len == Some(account.bytes.len())
}
fn decode_market(account: &AccountData, expected: &PoolState, program: Pubkey) -> Result<Layout> {
    let d = &account.bytes;
    ensure!(account.address == expected.address && account.owner == program && full(account)
        && d.len() >= MARKET_HEADER_LEN
        && u64::from_le_bytes(read::<8>(d,0)?) == MARKET_DISC
        && d[8] == 0,
        "Manifest MarketFixed owner/length/discriminator/version invalid");
    let mints = [key(d,16)?, key(d,48)?];
    let vaults = [key(d,80)?, key(d,112)?];
    let (Some(a),Some(b)) = (expected.mint_a,expected.mint_b) else {
        bail!("Manifest Registry mint identity absent");
    };
    ensure!(mints == [a,b] || mints == [b,a],
        "Manifest on-chain mints differ from Registry");
    ensure!(a != b && a != Pubkey::default() && b != Pubkey::default()
        && vaults[0] != vaults[1] && vaults.iter().all(|v|*v != Pubkey::default()),
        "Manifest invalid mints or vaults");
    for i in 0..2 {
        let vault = Pubkey::find_program_address(
            &[b"vault",account.address.as_ref(),mints[i].as_ref()],&program).0;
        ensure!(vault == vaults[i], "Manifest vault PDA mismatch");
    }
    Ok(Layout{address:account.address,mints,vaults})
}

// Exact V10 manifest_has_spare_node semantics: two valid free-list nodes,
// preventing an executor swap from expanding the Market account.
fn spare_node(data: &[u8]) -> Option<(u32,u32)> {
    let node = |index: u32| -> Option<&[u8]> {
        if index == u32::MAX || index % NODE_LEN as u32 != 0 {return None;}
        let start = MARKET_HEADER_LEN.checked_add(index as usize)?;
        data.get(start..start.checked_add(NODE_LEN)?)
    };
    let free = u32::from_le_bytes(read::<4>(data,176).ok()?);
    let next = u32::from_le_bytes(read::<4>(node(free)?,0).ok()?);
    (next != free && node(next).is_some()).then_some((free,next))
}

fn mint_proof(account: &AccountData, spl: Pubkey, token_2022: Pubkey) -> Result<MintProof> {
    ensure!(full(account) && account.bytes.len() >= 82
        && account.bytes[45] == 1, "Manifest mint account uninitialized/truncated");
    let is_2022 = if account.owner == spl {false}
        else if account.owner == token_2022 {true}
        else {bail!("Manifest mint has unsupported token program");};
    let d = &account.bytes;
    let mut extensions = Vec::new();
    if is_2022 && d.len() > 82 {
        ensure!(d.len() >= 166 && d[82..165].iter().all(|v|*v==0)
            && d[165]==1, "Manifest Token-2022 mint TLV frame invalid");
        let mut cursor = 166_usize;
        while cursor < d.len() {
            let tail = &d[cursor..];
            if tail.iter().all(|v|*v==0) {break;}
            ensure!(tail.len() >= 4, "Manifest mint truncated TLV entry");
            let kind = u16::from_le_bytes(tail[0..2].try_into()?);
            let len = usize::from(u16::from_le_bytes(tail[2..4].try_into()?));
            if kind == 0 && len == 0 {
                ensure!(tail[4..].iter().all(|v|*v==0), "Manifest mint nonzero TLV trailer");
                break;
            }
            ensure!(kind != 0 && len <= tail.len()-4, "Manifest mint invalid TLV entry");
            extensions.push(kind);
            cursor += 4+len;
        }
    } else if !is_2022 {
        ensure!(d.len()==82, "Manifest legacy token mint size invalid");
    }
    extensions.sort_unstable();
    ensure!(!extensions.windows(2).any(|w|w[0]==w[1]),
        "Manifest duplicate Token-2022 mint extension");
    Ok(MintProof{address:account.address,token_2022:is_2022,
        extensions,unsupported:Vec::new()})
}

fn validate_vault(account: &AccountData, expected_mint: Pubkey,
    mint_program: Pubkey) -> Result<()> {
    ensure!(full(account) && account.owner==mint_program
        && account.bytes.len() >= 165
        && key(&account.bytes,0)? == expected_mint
        // Manifest initializes its vault account with itself as token authority.
        && key(&account.bytes,32)? == account.address
        && account.bytes[108] == 1,
        "Manifest vault token program/mint/authority/state invalid");
    if account.bytes.len()>165 && mint_program.to_string()==TOKEN_2022 {
        ensure!(account.bytes[165]==2,"Manifest Token-2022 vault account type invalid");
    }
    Ok(())
}

async fn read_rpc(rpc: &RpcHandle, addresses: Vec<Pubkey>, min_slot: Option<u64>)
    -> Result<(u64,Vec<Option<AccountData>>)> {
    ensure!(!addresses.is_empty(),"empty Manifest RPC request");
    let expected=addresses.clone();
    let response=rpc.call(RpcOperation::Multiple {
        addresses,view:AccountView::Full,min_context_slot:min_slot
    }).await?;
    let RpcAnswer::Multiple{slot,accounts}=response.answer else {
        bail!("Manifest RPC returned unexpected response type");
    };
    ensure!(accounts.len()==expected.len()
        && min_slot.is_none_or(|v|slot>=v)
        && accounts.iter().zip(&expected).all(|(a,addr)|
            a.as_ref().is_none_or(|x|x.address==*addr && full(x))),
        "Manifest RPC cardinality/address/full account/slot mismatch");
    Ok((slot,accounts))
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

fn finish(pool:&PoolState,initial:Layout,slot:u64,
    lookup:&HashMap<Pubkey,Option<AccountData>>,program:Pubkey,
    spl:Pubkey,token_2022:Pubkey)->CompletionResult {
    let get=|address:Pubkey|lookup.get(&address).and_then(Option::as_ref);
    let Some(market)=get(pool.address) else {
        return retry(pool.address,"Manifest Market missing from coherent RPC snapshot");
    };
    let l=match decode_market(market,pool,program) {
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Manifest final MarketFixed: {e:#}"))
    };
    if initial.mints!=l.mints || initial.vaults!=l.vaults {
        return retry(pool.address,"Manifest Market mints/vaults changed during completion");
    }
    let (Some(base_mint),Some(quote_mint),Some(base_vault),Some(quote_vault))=(
        get(l.mints[0]),get(l.mints[1]),get(l.vaults[0]),get(l.vaults[1])
    ) else {
        return retry(pool.address,"Manifest mints or vaults missing in RPC snapshot");
    };
    let mint0=match mint_proof(base_mint,spl,token_2022) {
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Manifest base mint: {e:#}"))
    };
    let mint1=match mint_proof(quote_mint,spl,token_2022) {
        Ok(v)=>v,Err(e)=>return incomplete(pool.address,slot,format!("Manifest quote mint: {e:#}"))
    };
    if let Err(e)=validate_vault(base_vault,l.mints[0],base_mint.owner) {
        return incomplete(pool.address,slot,format!("Manifest base vault: {e:#}"));
    }
    if let Err(e)=validate_vault(quote_vault,l.mints[1],quote_mint.owner) {
        return incomplete(pool.address,slot,format!("Manifest quote vault: {e:#}"));
    }
    let Some((free,next))=spare_node(&market.bytes) else {
        return incomplete(pool.address,slot,
            "V10 Manifest requires at least two valid free-list nodes (no spare order node)").with_reason_code(CompletionReason::MissingExecutionAccounts);
    };
    let mint_vault=|mint:Pubkey|->Pubkey {
        if l.mints[0]==mint {l.vaults[0]} else {l.vaults[1]}
    };
    let ro=|address|AccountMeta{address,writable:false};
    let rw=|address|AccountMeta{address,writable:true};
    // V10's DEX-specific block: owner, quote mint, market, input vault, output vault.
    // base_a quotes mint A: B -> A. base_b quotes mint B: A -> B.
    let [a,b]=[pool.mint_a.expect("verified A"),pool.mint_b.expect("verified B")];
    let block=|base:Pubkey,x:Pubkey|->Arc<[AccountMeta]> {
        Arc::from(vec![ro(program),ro(base),rw(l.address),rw(mint_vault(x)),rw(mint_vault(base))])
    };
    let manifest=ExecutionManifest{pool:l.address,mint_a:a,mint_b:b,validated_slot:slot,
        bound_signer:None,
        base_a:block(a,b),base_b:block(b,a)};
    let proofs=if l.mints[0]==a {[mint0,mint1]} else {[mint1,mint0]};
    let evidence=Arc::new(CompletionEvidence{kind:"manifest_market_v10".into(),version:1,
        payload:Arc::new(json!({
            "program":program.to_string(),"market":l.address.to_string(),
            "base_mint":l.mints[0].to_string(),"quote_mint":l.mints[1].to_string(),
            "base_vault":l.vaults[0].to_string(),"quote_vault":l.vaults[1].to_string(),
            "base_token_program":base_mint.owner.to_string(),
            "quote_token_program":quote_mint.owner.to_string(),
            "free_list_head":free,"free_list_next":next,
            "base_a_quote_mint":a.to_string(),"base_b_quote_mint":b.to_string()
        }))
    });
    result(pool.address,CompletionStatus::Prepared,Some(slot),None,
        Some(evidence),Some(proofs),Some(Arc::new(manifest)))
}
async fn process(rpc:&RpcHandle,pools:Vec<PoolState>,settings:Settings)->Vec<CompletionResult> {
    let program:Pubkey=PROGRAM.parse().expect("known Manifest program");
    let spl:Pubkey=SPL_TOKEN.parse().expect("known SPL Token program");
    let token_2022:Pubkey=TOKEN_2022.parse().expect("known Token-2022 program");
    let mut results:Vec<Option<CompletionResult>>=vec![None;pools.len()];
    let (first_slot,parents)=match read_rpc(rpc,pools.iter().map(|p|p.address).collect(),None).await {
        Ok(v)=>v,Err(e)=>return pools.iter().map(|p|
            retry(p.address,format!("Manifest initial RPC: {e:#}"))).collect(),
    };
    let mut pending=Vec::new();
    let mut addresses=Vec::new();
    let mut dedup=HashSet::new();
    for (i,pool) in pools.iter().enumerate() {
        let Some(parent)=parents[i].as_ref() else {
            results[i]=Some(retry(pool.address,"Manifest Market account missing"));continue;
        };
        match decode_market(parent,pool,program) {
            Ok(l)=>{
                for addr in [l.address,l.mints[0],l.mints[1],l.vaults[0],l.vaults[1]] {
                    if dedup.insert(addr) {addresses.push(addr);}
                }
                pending.push((i,l));
            }
            Err(e)=>results[i]=Some(incomplete(pool.address,first_slot,
                format!("Manifest MarketFixed: {e:#}"))),
        }
    }
    if !pending.is_empty() {
        if addresses.len()>settings.account_batch_size {
            for (i,_) in pending {
                results[i]=Some(retry(pools[i].address,
                    "Manifest final RPC account set exceeds configured batch size"));
            }
        } else {
            match read_rpc(rpc,addresses.clone(),Some(first_slot)).await {
                Ok((slot,all))=>{
                    let lookup=addresses.into_iter().zip(all).collect::<HashMap<_,_>>();
                    for (i,l) in pending {
                        results[i]=Some(finish(&pools[i],l,slot,&lookup,program,spl,token_2022));
                    }
                }
                Err(e)=>for (i,_) in pending {
                    results[i]=Some(retry(pools[i].address,
                        format!("Manifest final RPC: {e:#}")));
                }
            }
        }
    }
    results.into_iter().map(|r|r.expect("every Manifest market has a result")).collect()
}

/// Protocol executor only. The shared lifecycle owns admission scheduling,
/// retry/backoff, reactivation and Prepared refresh.
async fn process_optimized(rpc:&RpcHandle, requests:&[CompletionRequest],settings:Settings)->Vec<CompletionOutcome>{
    let pools=requests.iter().map(|r|r.pool.clone()).collect::<Vec<_>>();
    process(rpc,pools,settings).await.into_iter().map(CompletionOutcome::Proof).collect()
}

pub async fn run(mut input:mpsc::Receiver<CompletionRequestBatch>,
    rpc:RpcHandle,settings:Settings,output:mpsc::Sender<CompletionResponseBatch>)->Result<()> {
    let mut tasks=JoinSet::new();
    let mut open=true;
    let mut processed=0_usize;
    crate::log_event!(Info,"manifest executor started parallel={} pools_per_batch={} scheduler=completion_lifecycle",
        settings.parallel_batches,settings.pools_per_batch());
    while open || !tasks.is_empty() {
        tokio::select! {
            batch=input.recv(),if open && tasks.len()<settings.parallel_batches=>match batch {
                Some(batch)=>{
                    ensure!(!batch.requests.is_empty() && batch.requests.len()<=settings.pools_per_batch(),
                        "invalid Manifest request batch size");
                    ensure!(batch.requests.iter().all(|r|r.pool.venue==Venue::Manifest
                        && r.pool.completion_admission.is_admitted()),
                        "Manifest request has wrong venue or missing admission");
                    let rpc=rpc.clone();
                    tasks.spawn(async move {
                        let results=match tokio::time::timeout(batch.timeout,
                            process_optimized(&rpc,&batch.requests,settings)).await {
                            Ok(results)=>results,
                            Err(_)=>batch.requests.iter().map(|r|CompletionOutcome::Proof(retry(r.pool.address,"Manifest lifecycle request timed out"))).collect(),
                        };
                        (batch.requests,results)
                    });
                },
                None=>open=false,
            },
            done=tasks.join_next(),if !tasks.is_empty()=>{
                let (requests,results)=done.context("missing Manifest request task")?
                    .context("Manifest request task panicked")?;
                ensure!(requests.len()==results.len(),"Manifest request/result count mismatch");
                let mut by_pool=results.into_iter().map(|outcome|(outcome.pool(),outcome)).collect::<HashMap<_,_>>();
                ensure!(by_pool.len()==requests.len(),"duplicate Manifest pool result");
                let mut responses=Vec::with_capacity(requests.len());
                for request in requests {
                    let outcome=by_pool.remove(&request.pool.address)
                        .context("Completion request has no matching pool result")?;
                    let kind=match &outcome {
                        CompletionOutcome::Proof(result)=>format!("{:?}",result.status),
                        CompletionOutcome::Unchanged(_)=>"Unchanged".to_owned(),
                    };
                    crate::log_event!(Debug,"completion_manifest completion request_id={} pool={} work={:?} outcome={}",
                        request.id,request.pool.address,request.work,kind);
                    responses.push(CompletionResponse{id:request.id,outcome});
                }
                processed+=responses.len();
                output.send(CompletionResponseBatch{source:SOURCE,responses}).await
                    .map_err(|_|anyhow!("Manifest lifecycle response consumer closed"))?;
            },
        }
    }
    crate::log_event!(Info,"manifest executor complete requests={processed}");
    Ok(())
}
