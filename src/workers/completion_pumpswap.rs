//! PumpSwap V10 completion: lifecycle requests -> verified RPC state -> account blocks.
//! When PUMPSWAP_V10_WALLET is unset, preserve wallet-independent evidence mode.
//! The shared lifecycle owns admission scheduling, retries, rechecks and refresh.
use std::{collections::{HashMap, HashSet}, env, sync::Arc};
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::{sync::mpsc, task::JoinSet};
use crate::{
    core::{CompletionReason, AccountMeta,CompletionEvidence,CompletionResult,CompletionOutcome, CompletionRequest, CompletionUnchanged, CompletionRequestBatch,CompletionResponse,CompletionResponseBatch,
        CompletionState,CompletionStatus,ExecutionManifest,MintProof,PoolState,Venue},
    workers::rpc_transport::{AccountData,AccountView,RpcAnswer,RpcHandle,RpcOperation},
};

const SOURCE:&str="pumpswap_v10_completion";
const PUMP:&str="pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const PUMP_FEE:&str="pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";
const WSOL:&str="So11111111111111111111111111111111111111112";
const USDC:&str="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const TOKEN:&str="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022:&str="TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const ASSOCIATED:&str="ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const DISC:[u8;8]=[241,154,109,4,17,177,109,188];

#[derive(Clone,Copy)]
pub struct Settings {
    account_batch_size:usize,
    pub parallel_batches:usize,
    pub wallet:Option<Pubkey>,
}
impl Settings {
    pub fn from_env() -> Result<Self> {
        let account_batch_size:usize=env::var("RPC_ACCOUNT_BATCH_SIZE")
            .unwrap_or_else(|_|"100".into()).parse()?;
        ensure!((6..=100).contains(&account_batch_size),
            "PumpSwap requires RPC_ACCOUNT_BATCH_SIZE in 6..=100");
        let wallet=env::var("PUMPSWAP_V10_WALLET").ok()
            .filter(|value|!value.trim().is_empty())
            .map(|value|value.parse::<Pubkey>().context("invalid PUMPSWAP_V10_WALLET"))
            .transpose()?;
        ensure!(wallet!=Some(Pubkey::default()),
            "PUMPSWAP_V10_WALLET must be a nonzero public key");
        Ok(Self {account_batch_size,parallel_batches:8,wallet})
    }
    pub fn pools_per_batch(self)->usize {
        10_usize.min((self.account_batch_size-1)/5)
    }
}

/// Wallet-bound pool blocks are persisted. Never boot a cache of PumpSwap
/// Prepared accounts against a different signer (or without one).
pub fn ensure_wallet_cache(pools:&[PoolState],wallet:Option<Pubkey>)->Result<()> {
    for pool in pools.iter().filter(|pool|
        pool.venue==Venue::PumpSwap
        && pool.completion_state==CompletionState::Prepared) {
        let stored=pool.execution_manifest.as_ref().and_then(|m|m.bound_signer);
        ensure!(stored==wallet,
            "PumpSwap Prepared manifest is bound to another signer or no wallet configured pool={}; use the original PUMPSWAP_V10_WALLET or revalidate the persisted PumpSwap manifests",
            pool.address);
    }
    Ok(())
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
fn buyback_candidates(global:&[u8])->Result<Vec<Pubkey>> {
    let mut result=Vec::new();
    for i in 0..8 {
        let recipient=key(global,643+i*32)
            .context("PumpSwap buyback recipient array truncated")?;
        if recipient!=Pubkey::default() && !result.contains(&recipient) {
            result.push(recipient);
        }
    }
    ensure!(!result.is_empty(),"PumpSwap global config has no buyback recipients");
    Ok(result)
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
    crate::log_event!(Debug,"pumpswap completion rpc provider={} slot={slot} requested={} present={}",
        response.provider,ids.len(),accounts.iter().filter(|a|a.is_some()).count());
    Ok((slot,ids.into_iter().zip(accounts).collect()))
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
fn token_account_ok(a:&AccountData,mint:Pubkey,authority:Pubkey,program:Pubkey)->bool {
    full(a) && a.owner==program && a.bytes.len()>=165
        && key(&a.bytes,0).ok()==Some(mint)
        && key(&a.bytes,32).ok()==Some(authority)
        && a.bytes[108]==1
}

/// The V10 PoolAccounts::from_pump_keys account order (src/v10.rs) for
/// both quote orientations. These lists do not include the outer V10
/// signer/settlement/group accounts, which belong to instruction packing.
fn manifest(l:Layout,slot:u64,wallet:Pubkey,recipient:Pubkey,buyback:Pubkey,
    quote_program:Pubkey,v2:bool,program:Pubkey,associated:Pubkey)->ExecutionManifest {
    let ro=|address|AccountMeta{address,writable:false};
    let rw=|address|AccountMeta{address,writable:true};
    let global=pda(program,&[b"global_config"]);
    let event=pda(program,&[b"__event_authority"]);
    let volume=pda(program,&[b"user_volume_accumulator",wallet.as_ref()]);
    let fee_program:Pubkey=PUMP_FEE.parse().expect("Pump fee program ID");
    let fee_config=pda(fee_program,&[b"fee_config",program.as_ref()]);
    let block=|base:Pubkey,x:Pubkey|->Arc<[AccountMeta]> {
        let x_vault=if x==l.mints[0] {l.vaults[0]} else {l.vaults[1]};
        let base_vault=if base==l.mints[0] {l.vaults[0]} else {l.vaults[1]};
        let mut accounts=vec![ro(program),ro(base),ro(global),ro(event)];
        if v2 {
            accounts.extend([
                rw(l.address),rw(x_vault),rw(base_vault),rw(volume),
                ro(fee_config),rw(ata(buyback,l.mints[1],quote_program,associated)),
            ]);
        } else {
            let creator_vault=pda(program,&[b"creator_vault",l.creator.as_ref()]);
            accounts.extend([
                ro(recipient),rw(l.address),rw(x_vault),rw(base_vault),
                rw(ata(recipient,l.mints[1],quote_program,associated)),
                rw(ata(creator_vault,l.mints[1],quote_program,associated)),
                ro(creator_vault),ro(pda(program,&[b"global_volume_accumulator"])),
                rw(volume),ro(fee_config),ro(fee_program),
            ]);
            if l.cashback {
                accounts.extend([
                    rw(ata(volume,l.mints[1],quote_program,associated)),rw(volume),
                ]);
            }
            if l.creator!=Pubkey::default() {
                accounts.push(ro(pda(program,&[b"pool-v2",l.mints[0].as_ref()])));
            }
            accounts.extend([
                ro(buyback),rw(ata(buyback,l.mints[1],quote_program,associated)),
            ]);
        }
        Arc::from(accounts)
    };
    ExecutionManifest{
        pool:l.address,mint_a:l.mints[0],mint_b:l.mints[1],validated_slot:slot,
        bound_signer:Some(wallet),
        base_a:block(l.mints[0],l.mints[1]),
        base_b:block(l.mints[1],l.mints[0]),
    }
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
    let mut per_buyback=HashMap::<Pubkey,Vec<Pubkey>>::new();
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
        // V10's Pump v2 path needs one EXISTING buyback ATA. V1 can create
        // a missing fee ATA, but must not be treated as V2-ready.
        if settings.wallet.is_some() && !l.cashback
            && (l.mints[1].to_string()==WSOL || l.mints[1].to_string()==USDC)
            && quote_acc.owner==spl {
            match buyback_candidates(&global_bytes) {
                Ok(options)=>{
                    for candidate in &options {
                        let account=ata(*candidate,l.mints[1],quote_acc.owner,associated);
                        if fee_seen.insert(account){fees.push(account);}
                    }
                    per_buyback.insert(pool.address,options);
                },
                Err(error)=>{
                    results.insert(pool.address,incomplete(pool.address,pool_slot,
                        format!("PumpSwap V10 buyback recipients: {error:#}")));
                }
            }
        }
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
        let mut failure_code=CompletionReason::ValidationFailed;
        let checked=(||->Result<[MintProof;2]>{
            let p0=proof(m0,spl,token_2022)?;
            let p1=proof(m1,spl,token_2022)?;
            if !p0.unsupported.is_empty() || !p1.unsupported.is_empty() {
                failure_code=CompletionReason::UnsupportedConfiguration;
            }
            ensure!(p0.unsupported.is_empty() && p1.unsupported.is_empty(),
                "PumpSwap unsupported Token-2022 mint extensions");
            vault(v0,l.mints[0],m0.owner,l.address)?;
            vault(v1,l.mints[1],m1.owner,l.address)?;
            Ok([p0,p1])
        })();
        let proofs=match checked {
            Ok(v)=>v,Err(e)=>{
                results.insert(pool.address,incomplete(pool.address,last_slot,
                    format!("PumpSwap mint/vault validation: {e:#}")).with_reason_code(failure_code));continue;
            }
        };
        let Some(candidates)=per_pool.get(&pool.address) else {
            results.insert(pool.address,retry(pool.address,"PumpSwap fee recipient candidates missing"));
            continue;
        };
        let initialized=candidates.iter().copied().filter(|recipient|{
            let fee_ata=ata(*recipient,l.mints[1],m1.owner,associated);
            fee_accounts.get(&fee_ata).and_then(Option::as_ref)
                .is_some_and(|a|token_account_ok(a,l.mints[1],*recipient,m1.owner))
        }).collect::<Vec<_>>();
        let recipient=initialized.first().copied().unwrap_or(candidates[0]);
        let warmed_buyback=per_buyback.get(&pool.address)
            .and_then(|options|options.iter().copied().find(|owner|{
                let address=ata(*owner,l.mints[1],m1.owner,associated);
                fee_accounts.get(&address).and_then(Option::as_ref)
                    .is_some_and(|account|token_account_ok(account,l.mints[1],*owner,m1.owner))
            }));
        let use_v2=warmed_buyback.is_some();
        let selected_buyback=warmed_buyback.unwrap_or(buyback);
        let [a,b]=[l.mints[0],l.mints[1]];
        let evidence=Arc::new(CompletionEvidence{
            kind:"pumpswap_pool_validated_v1".into(),version:1,
            payload:Arc::new(json!({
                "program":program.to_string(),"pool":l.address.to_string(),
                "global_config":global_addr.to_string(),
                "base_mint":a.to_string(),"quote_mint":b.to_string(),
                "base_vault":l.vaults[0].to_string(),
                "quote_vault":l.vaults[1].to_string(),
                "base_token_program":m0.owner.to_string(),
                "quote_token_program":m1.owner.to_string(),
                "base_decimals":m0.bytes[44],"quote_decimals":m1.bytes[44],
                "recipient":recipient.to_string(),
                "recipient_candidates":candidates.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "initialized_recipients":initialized.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "buyback":selected_buyback.to_string(),"v10_use_pump_v2":use_v2,
                "v10_wallet":settings.wallet.map(|wallet|wallet.to_string()),
                "coin_creator":l.creator.to_string(),
                "is_mayhem_mode":l.mayhem,"is_cashback_coin":l.cashback,
                "pump_quote_is_base_a":false,"pump_quote_is_base_b":true,
                "pool_snapshot_slot":pool_slot,"rpc_last_slot":last_slot,
                "execution_accounts_resolved":settings.wallet.is_some(),
                "execution_wallet_binding":settings.wallet.is_some()
            }))
        });
        if let Some(wallet)=settings.wallet {
            let descriptor=manifest(l,pool_slot,wallet,recipient,selected_buyback,
                m1.owner,use_v2,program,associated);
            results.insert(pool.address,result(pool.address,
                CompletionStatus::Prepared,Some(pool_slot),None,
                Some(evidence),Some(proofs),Some(Arc::new(descriptor))));
        } else {
            results.insert(pool.address,result(pool.address,
                CompletionStatus::Incomplete,Some(pool_slot),
                Some("wallet-independent PumpSwap pool validated; set PUMPSWAP_V10_WALLET for V10 account blocks".into()),
                Some(evidence),Some(proofs),None).with_reason_code(CompletionReason::ConfigurationRequired));
        }
    }
    pools.iter().map(|p|results.remove(&p.address)
        .expect("every PumpSwap pool has a result")).collect()
}
/// Protocol executor only. The shared lifecycle owns admission scheduling,
/// retry/backoff, reactivation and Prepared refresh.
async fn process_optimized(rpc:&RpcHandle,requests:&[CompletionRequest],settings:Settings)->Vec<CompletionOutcome>{
    let program:Pubkey=PUMP.parse().expect("known PumpSwap program");
    let spl:Pubkey=TOKEN.parse().expect("known SPL token program");
    let token_2022:Pubkey=TOKEN_2022.parse().expect("known Token-2022 program");
    let associated:Pubkey=ASSOCIATED.parse().expect("known ATA program");
    let global_addr=global_key(program);
    let eligible=requests.iter().filter(|r|!r.force_full
        && r.work!=crate::core::CompletionWork::Initial
        && r.pool.completion_state==crate::core::CompletionState::Prepared
        && r.pool.execution_manifest.as_ref().is_some_and(|m|m.bound_signer==settings.wallet)
        && r.pool.mint_proofs.is_some()
        && r.pool.completion_evidence.as_ref().is_some_and(|e|
            e.kind=="pumpswap_pool_validated_v1"&&e.version==1)
        && r.pool.completion_validated_slot.is_some() && settings.wallet.is_some())
        .collect::<Vec<_>>();
    let mut outcomes=HashMap::<Pubkey,CompletionOutcome>::new();
    if !eligible.is_empty(){
        let min_slot=eligible.iter().filter_map(|r|r.pool.completion_validated_slot).max();
        let addresses=std::iter::once(global_addr).chain(eligible.iter().map(|r|r.pool.address))
            .collect::<Vec<_>>();
        match read(rpc,addresses,min_slot).await {
            Err(error)=>for r in &eligible{
                outcomes.insert(r.pool.address,CompletionOutcome::Proof(retry(r.pool.address,
                    format!("PumpSwap light pool/global: {error:#}"))));
            },
            Ok((parent_slot,accounts))=>{
                if let Some(global)=accounts.get(&global_addr).and_then(Option::as_ref){
                    if global.owner==program && global.bytes.len()>=675 {
                        let global_bytes=&global.bytes;
                        let buyback=key(global_bytes,643).unwrap_or_default();
                        let mut checked=Vec::new();
                        let mut wanted=Vec::new();let mut seen=HashSet::new();
                        for r in &eligible{
                            let p=&r.pool;
                            let Some(layout)=accounts.get(&p.address).and_then(Option::as_ref)
                                .and_then(|a|pool_layout(a,p,program).ok()) else{continue};
                            let Some(proofs)=p.mint_proofs.as_ref() else{continue};
                            let quote_program=if proofs[1].token_2022{token_2022}else{spl};
                            let Ok(recipients)=fee_candidates(global_bytes,layout.mayhem) else{continue};
                            let buybacks=if !layout.cashback
                                && (layout.mints[1].to_string()==WSOL || layout.mints[1].to_string()==USDC)
                                && quote_program==spl {
                                match buyback_candidates(global_bytes){Ok(v)=>v,Err(_)=>continue}
                            }else{Vec::new()};
                            for wallet in recipients.iter().chain(buybacks.iter()){
                                let addr=ata(*wallet,layout.mints[1],quote_program,associated);
                                if seen.insert(addr){wanted.push(addr);}
                            }
                            checked.push((*r,layout,quote_program,recipients,buybacks));
                        }
                        let mut fee_accounts=HashMap::new();
                        let mut last_slot=parent_slot;
                        let mut fee_error=None;
                        for chunk in wanted.chunks(settings.account_batch_size){
                            match read(rpc,chunk.to_vec(),Some(parent_slot)).await {
                                Ok((slot,accounts))=>{
                                    last_slot=last_slot.max(slot);
                                    fee_accounts.extend(accounts);
                                },
                                Err(error)=>{fee_error=Some(format!("{error:#}"));break;},
                            }
                        }
                        if let Some(error)=fee_error {
                            for (r,_,_,_,_) in &checked {
                                outcomes.insert(r.pool.address,CompletionOutcome::Proof(retry(r.pool.address,
                                    format!("PumpSwap light fee accounts: {error}"))));
                            }
                        }else{
                            for (r,layout,quote_program,recipients,buybacks) in checked {
                                let p=&r.pool;
                                let initialized=recipients.iter().copied().filter(|recipient|{
                                    let addr=ata(*recipient,layout.mints[1],quote_program,associated);
                                    fee_accounts.get(&addr).and_then(Option::as_ref)
                                        .is_some_and(|a|token_account_ok(a,layout.mints[1],*recipient,quote_program))
                                }).collect::<Vec<_>>();
                                let recipient=initialized.first().copied().unwrap_or(recipients[0]);
                                let warmed=buybacks.iter().copied().find(|recipient|{
                                    let addr=ata(*recipient,layout.mints[1],quote_program,associated);
                                    fee_accounts.get(&addr).and_then(Option::as_ref)
                                        .is_some_and(|a|token_account_ok(a,layout.mints[1],*recipient,quote_program))
                                });
                                let use_v2=warmed.is_some();
                                let selected_buyback=warmed.unwrap_or(buyback);
                                let ev=p.completion_evidence.as_ref().expect("eligible evidence");
                                let previous_creator=ev.payload["coin_creator"].as_str();
                                let matched=previous_creator==Some(layout.creator.to_string().as_str())
                                    && ev.payload["is_mayhem_mode"].as_bool()==Some(layout.mayhem)
                                    && ev.payload["is_cashback_coin"].as_bool()==Some(layout.cashback)
                                    && ev.payload["base_vault"].as_str()==Some(layout.vaults[0].to_string().as_str())
                                    && ev.payload["quote_vault"].as_str()==Some(layout.vaults[1].to_string().as_str())
                                    && p.execution_manifest.as_ref().is_some_and(|cached|{
                                        let wallet=settings.wallet.expect("eligible wallet");
                                        let fresh=manifest(layout,cached.validated_slot,wallet,recipient,
                                            selected_buyback,quote_program,use_v2,program,associated);
                                        fresh==**cached
                                    });
                                if matched {
                                    if let Some(u)=CompletionUnchanged::from_pool(p,last_slot){
                                        outcomes.insert(p.address,CompletionOutcome::Unchanged(u));
                                    }
                                }
                            }
                        }
                    }
                }
            },
        }
    }
    let pending=requests.iter().filter(|r|!outcomes.contains_key(&r.pool.address))
        .map(|r|r.pool.clone()).collect::<Vec<_>>();
    if !pending.is_empty(){
        for proof in process(rpc,pending,settings).await{
            outcomes.insert(proof.pool,CompletionOutcome::Proof(proof));
        }
    }
    requests.iter().map(|r|outcomes.remove(&r.pool.address).expect("PumpSwap result")).collect()
}

pub async fn run(mut input:mpsc::Receiver<CompletionRequestBatch>,
    rpc:RpcHandle,settings:Settings,output:mpsc::Sender<CompletionResponseBatch>)->Result<()> {
    let mut tasks=JoinSet::new();
    let mut open=true;
    let mut processed=0_usize;
    crate::log_event!(Info,"pumpswap executor started parallel={} pools_per_batch={} wallet_configured={} wallet={} scheduler=completion_lifecycle",
        settings.parallel_batches,settings.pools_per_batch(),settings.wallet.is_some(),
        settings.wallet.map(|wallet|wallet.to_string()).unwrap_or_else(||"-".into()));
    while open || !tasks.is_empty() {
        tokio::select! {
            batch=input.recv(),if open && tasks.len()<settings.parallel_batches=>match batch {
                Some(batch)=>{
                    ensure!(!batch.requests.is_empty() && batch.requests.len()<=settings.pools_per_batch(),
                        "invalid PumpSwap request batch size");
                    ensure!(batch.requests.iter().all(|r|r.pool.venue==Venue::PumpSwap
                        && r.pool.completion_admission.is_admitted()),
                        "PumpSwap request has wrong venue or missing admission");
                    let rpc=rpc.clone();
                    tasks.spawn(async move {
                        let results=match tokio::time::timeout(batch.timeout,
                            process_optimized(&rpc,&batch.requests,settings)).await {
                            Ok(results)=>results,
                            Err(_)=>batch.requests.iter().map(|r|CompletionOutcome::Proof(retry(r.pool.address,"PumpSwap lifecycle request timed out"))).collect(),
                        };
                        (batch.requests,results)
                    });
                },
                None=>open=false,
            },
            done=tasks.join_next(),if !tasks.is_empty()=>{
                let (requests,results)=done.context("missing PumpSwap request task")?
                    .context("PumpSwap request task panicked")?;
                ensure!(requests.len()==results.len(),"PumpSwap request/result count mismatch");
                let mut by_pool=results.into_iter().map(|outcome|(outcome.pool(),outcome)).collect::<HashMap<_,_>>();
                ensure!(by_pool.len()==requests.len(),"duplicate PumpSwap pool result");
                let mut responses=Vec::with_capacity(requests.len());
                for request in requests {
                    let outcome=by_pool.remove(&request.pool.address)
                        .context("Completion request has no matching pool result")?;
                    let kind=match &outcome {
                        CompletionOutcome::Proof(result)=>format!("{:?}",result.status),
                        CompletionOutcome::Unchanged(_)=>"Unchanged".to_owned(),
                    };
                    crate::log_event!(Debug,"completion_pumpswap completion request_id={} pool={} work={:?} outcome={}",
                        request.id,request.pool.address,request.work,kind);
                    responses.push(CompletionResponse{id:request.id,outcome});
                }
                processed+=responses.len();
                output.send(CompletionResponseBatch{source:SOURCE,responses}).await
                    .map_err(|_|anyhow!("PumpSwap lifecycle response consumer closed"))?;
            },
        }
    }
    crate::log_event!(Info,"pumpswap executor complete requests={processed}");
    Ok(())
}
