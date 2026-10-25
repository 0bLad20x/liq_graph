//! Independent on-chain SPL / Token-2022 Mint classification.
//! Owns only RPC scheduling/retry state; the Mint Registry owns all Mint facts.
use std::{collections::{HashMap,HashSet},env,sync::Arc,time::Duration};
use anyhow::{Result,Context,ensure,bail};
use serde_json::{Value,json};
use solana_pubkey::Pubkey;
use tokio::{sync::{mpsc,watch},task::JoinSet,time};
use crate::core::{MintReader,MintOnchainState,MintOnchainBatch,now_unix_ms};
use crate::core::account_stream::{AccountData,AccountView};
use super::rpc_transport::{RpcHandle,RpcOperation,RpcAnswer};

const SPL:&str="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022:&str="TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

fn get(d:&[u8],offset:usize,len:usize)->Result<&[u8]> {
    d.get(offset..offset.saturating_add(len)).context("truncated mint extension")
}
fn le64(d:&[u8],i:usize)->Result<u64>{Ok(u64::from_le_bytes(get(d,i,8)?.try_into()?))}
fn le16(d:&[u8],i:usize)->Result<u16>{Ok(u16::from_le_bytes(get(d,i,2)?.try_into()?))}
fn hex(d:&[u8])->String {let mut s=String::with_capacity(d.len()*2);for v in d {s.push_str(&format!("{v:02x}"));}s}
fn authority(d:&[u8])->Result<Option<String>> {
    ensure!(d.len()==32,"invalid token authority");
    if d.iter().all(|&v|v==0){Ok(None)}
    else {Ok(Some(Pubkey::new_from_array(d.try_into()?).to_string()))}
}
fn fee(d:&[u8],pos:usize)->Result<Value>{
    Ok(json!({"epoch":le64(d,pos)?.to_string(),
        "maximum_fee_raw":le64(d,pos+8)?.to_string(),
        "basis_points":le16(d,pos+16)?}))
}
fn name(id:u16)->&'static str {match id {
    1=>"TransferFeeConfig",3=>"MintCloseAuthority",4=>"ConfidentialTransferMint",
    6=>"DefaultAccountState",9=>"NonTransferable",10=>"InterestBearingConfig",
    12=>"PermanentDelegate",14=>"TransferHook",16=>"ConfidentialTransferFeeConfig",
    18=>"MetadataPointer",19=>"TokenMetadata",20=>"GroupPointer",21=>"TokenGroup",
    22=>"GroupMemberPointer",23=>"TokenGroupMember",24=>"ConfidentialMintBurn",
    25=>"ScaledUiAmount",26=>"Pausable",28=>"PermissionedBurn",_=>"Other"
}}
pub fn decode(a:&AccountData,slot:u64,at:u64)->Result<MintOnchainState>{
    ensure!(a.view==AccountView::Full && a.bytes.len()>=82 && a.bytes[45]==1,
        "mint not a full initialized account");
    let spl:Pubkey=SPL.parse()?;let t22:Pubkey=TOKEN_2022.parse()?;
    let owner=a.owner;
    ensure!(owner==spl||owner==t22,"mint owner is not SPL or Token-2022");
    let decimals=a.bytes[44];
    let mut ids=HashSet::new();let mut extensions=Vec::<Value>::new();
    let mut transfer_fee=None;
    let mut hook=None::<String>;
    let mut fee_unknown=false;
    if owner==spl {ensure!(a.bytes.len()==82,"legacy SPL mint has nonstandard length");}
    if owner==t22 && a.bytes.len()>82 {
        let d=&a.bytes;
        ensure!(d.len()>=166 && d[82..165].iter().all(|v|*v==0) && d[165]==1,
            "Token-2022 mint type/padding invalid");
        let mut offset=166;
        while offset<d.len() {
            let tail=&d[offset..];
            if tail.iter().all(|v|*v==0){break;}
            ensure!(tail.len()>=4,"truncated Token-2022 TLV header");
            let id=le16(tail,0)?;
            let len=usize::from(le16(tail,2)?);
            if id==0 && len==0 {
                ensure!(tail[4..].iter().all(|v|*v==0),"nonzero data after TLV terminator");
                break;
            }
            ensure!(id!=0 && len<=tail.len()-4 && ids.insert(id),
                "duplicate or invalid Token-2022 extension");
            let body=&tail[4..4+len];
            let mut item=json!({"type_id":id,"name":name(id),"data_hex":hex(body)});
            if id==1 {
                ensure!(body.len()==108,"TransferFeeConfig must have 108 bytes");
                transfer_fee=Some(json!({
                    "status":"configured",
                    "config_authority":authority(get(body,0,32)?)?,
                    "withdraw_withheld_authority":authority(get(body,32,32)?)?,
                    "withheld_amount_raw":le64(body,64)?.to_string(),
                    "older":fee(body,72)?,
                    "newer":fee(body,90)?
                }));
            }
            if id==14 {
                ensure!(body.len()==64,"TransferHook must have 64 bytes");
                hook=authority(get(body,32,32)?)?;
                item["authority"]=json!(authority(get(body,0,32)?)?);
                item["program_id"]=json!(hook);
                fee_unknown=true; // Program-defined behavior is not a computable Mint fee.
            }
            if matches!(id,4|16|24|25|26|28) {fee_unknown=true;}
            extensions.push(item);
            offset+=4+len;
        }
    }
    let program=if owner==spl {"legacy_spl"} else {"token_2022"};
    let state=if fee_unknown {"custom_fee_or_transfer_behavior_unresolved"}
        else if transfer_fee.is_some() {"transfer_fee_configured"} else {"verified_no_standard_transfer_fee"};
    let mut transfer_fee_config=transfer_fee.unwrap_or_else(||json!({"status":"none"}));
    transfer_fee_config["verified_slot"]=json!(slot);
    transfer_fee_config["verified_at_ms"]=json!(at);
    Ok(MintOnchainState {
        address:a.address,owner,decimals,verified_slot:slot,verified_at_ms:at,
        token_extensions:json!({
            "schema_version":1,"source":"solana_mint_account",
            "verified_slot":slot,"verified_at_ms":at,
            "owner":owner.to_string(),"program":program,"decimals":decimals,
            "classification":state,"transfer_hook_program":hook,
            "extensions":extensions
        }),
        transfer_fee_config,
    })
}

async fn fetch(rpc:RpcHandle,addresses:Vec<Pubkey>)->Result<(Vec<MintOnchainState>,usize)>{
    let answer=rpc.call(RpcOperation::Multiple{addresses:addresses.clone(),
        view:AccountView::Full,min_context_slot:None}).await?;
    let RpcAnswer::Multiple{slot,accounts}=answer.answer else {bail!("Mint expected Multiple");};
    ensure!(accounts.len()==addresses.len(),"Mint account batch size mismatch");
    let mut results=Vec::new();let mut rejected=0;
    for (address,account) in addresses.into_iter().zip(accounts) {
        match account {
            Some(a) if a.address==address=>match decode(&a,slot,answer.received_at_ms) {
                Ok(mint)=>results.push(mint),
                Err(error)=>{
                    rejected+=1;
                    crate::log_event!(Warn,"mint extensions invalid mint={} slot={} reason={error:#}",address,slot);
                }
            },
            _=>{rejected+=1;}
        }
    }
    Ok((results,rejected))
}
fn number<T:std::str::FromStr>(key:&str,default:T)->Result<T>
where T::Err:std::fmt::Display {
    env::var(key).map_or(Ok(default),|s|s.parse().map_err(|e|anyhow::anyhow!("invalid {key}: {e}")))
}
/// Prioritize selected market Mints, then remaining economically qualified Mints.
/// Verification snapshots belong exclusively to the existing Mint Registry.
pub async fn run(
    mut qualified:watch::Receiver<Arc<[Pubkey]>>,
    mut selected:watch::Receiver<Arc<[Pubkey]>>,
    reader:Arc<dyn MintReader>,rpc:RpcHandle,
    result:mpsc::Sender<Arc<MintOnchainBatch>>,mut stop:watch::Receiver<bool>,
)->Result<()>{
    let parallel:usize=number("MINT_ONCHAIN_PARALLEL_BATCHES",4)?;
    let mutable_refresh_s:u64=number("MINT_ONCHAIN_MUTABLE_REFRESH_SECONDS",3600)?;
    let stable_refresh_s:u64=number("MINT_ONCHAIN_STABLE_REFRESH_SECONDS",86400)?;
    let retry_s:u64=number("MINT_ONCHAIN_RETRY_SECONDS",60)?;
    ensure!((1..=16).contains(&parallel) && mutable_refresh_s>=60
        && stable_refresh_s>=300 && retry_s>=5,"invalid Mint on-chain refresh settings");
    let size=rpc.account_batch_size().min(100);
    ensure!(size>0,"zero RPC mint batch size");
    let mut attempted=HashMap::<Pubkey,u64>::new();
    let mut in_flight=HashSet::<Pubkey>::new();
    let mut jobs=JoinSet::new();let mut ticker=time::interval(Duration::from_secs(10));
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut scan=true;let mut decoded=0_u64;let mut failed=0_u64;
    let mut qualified_open=true;let mut selected_open=true;let mut last_coverage_log=0_u64;
    loop {
        if *stop.borrow(){break;}
        if scan {
            scan=false;
            let now=now_unix_ms()?;
            let priority=selected.borrow().to_vec();let qualified_list=qualified.borrow().to_vec();
            let mut seen=HashSet::<Pubkey>::new();let mut needed=Vec::new();
            let mut verified=0_usize;
            for address in priority.iter().chain(qualified_list.iter()).copied() {
                if !seen.insert(address)||in_flight.contains(&address){continue;}
                let record=reader.mint(address);
                if record.as_ref().and_then(|s|s.onchain.as_ref()).is_some(){verified+=1;}
                let ttl=record.as_ref().and_then(|s|s.onchain.as_ref()).map(|state|{
                    let fee_changeable=state.transfer_fee_config.get("config_authority")
                        .is_some_and(|v|!v.is_null());
                    let ambiguous=state.token_extensions.get("classification")
                        .and_then(Value::as_str)==Some("custom_fee_or_transfer_behavior_unresolved");
                    if fee_changeable||ambiguous {mutable_refresh_s} else {stable_refresh_s}
                }).unwrap_or(0);
                if record.as_ref().and_then(|s|s.onchain.as_ref())
                    .is_some_and(|v|now.saturating_sub(v.verified_at_ms)<ttl.saturating_mul(1000)) {continue;}
                if attempted.get(&address).is_some_and(|at|
                    now.saturating_sub(*at)<retry_s.saturating_mul(1000)) {continue;}
                needed.push(address);
            }
            for chunk in needed.chunks(size).take(parallel.saturating_sub(jobs.len())) {
                let addresses=chunk.to_vec();
                for address in &addresses {in_flight.insert(*address);attempted.insert(*address,now);}
                let rpc=rpc.clone();
                jobs.spawn(async move {let answer=fetch(rpc,addresses.clone()).await;(addresses,answer)});
            }
            if now.saturating_sub(last_coverage_log)>=30_000 {
                last_coverage_log=now;
                crate::log_event!(Info,"mint extensions coverage target={} verified={} queued={} in_flight={} decoded={} failed={} prioritized={} batch_size={}",
                    seen.len(),verified,needed.len(),in_flight.len(),decoded,failed,priority.len(),size);
            }
            if !needed.is_empty() && jobs.len()>0 {
                crate::log_event!(Debug,"mint extensions scan qualified={} prioritized={} needed={} inflight={} batch_size={}",
                    qualified_list.len(),priority.len(),needed.len(),in_flight.len(),size);
            }
        }
        tokio::select! {
            _=ticker.tick()=>{scan=true;},
            changed=qualified.changed(),if qualified_open=>{
                if changed.is_ok(){scan=true;}else{qualified_open=false;}
            },
            changed=selected.changed(),if selected_open=>{
                if changed.is_ok(){scan=true;}else{selected_open=false;}
            },
            changed=stop.changed()=>{if changed.is_err()||*stop.borrow(){break;}},
            joined=jobs.join_next(),if !jobs.is_empty()=>{
                if let Some(job)=joined {
                    let (addresses,answer)=job.context("Mint RPC task panic")?;
                    for address in &addresses {in_flight.remove(address);}
                    match answer {
                        Ok((states,rejected))=>{
                            decoded+=states.len() as u64;failed+=rejected as u64;
                            if !states.is_empty() {
                                result.send(Arc::new(MintOnchainBatch{states})).await
                                    .context("Mint Registry stopped")?;
                            }
                        },
                        Err(error)=>{
                            failed+=addresses.len() as u64;
                            crate::log_event!(Warn,"mint extensions batch failed size={} error={error:#}",addresses.len());
                        }
                    }
                    scan=true;
                }
            }
        }
    }
    jobs.abort_all();
    crate::log_event!(Info,"mint extensions complete=true decoded={decoded} failed={failed} outstanding={}",jobs.len());
    Ok(())
}
