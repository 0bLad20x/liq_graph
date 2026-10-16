//! PumpSwap identity hydration: incomplete Registry identities -> verified mint pairs.
//! Full scans deliberately discover addresses only. This worker reads just the
//! account prefix required to identify base/quote mints, then emits ordinary
//! Discovery batches; Registry and Persistence remain the only state writers.
use std::{collections::{HashSet, VecDeque}, env, sync::Arc, time::Duration};
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::{sync::broadcast, task::JoinSet, time::MissedTickBehavior};
use crate::{
    core::{now_unix_ms, DataBatch, DataRecord, PoolUpdateBatch, RecordIdentity, Venue},
    workers::rpc_transport::{AccountData, AccountView, RpcAnswer, RpcHandle, RpcOperation},
};

const SOURCE: &str = "pumpswap_identity_hydration";
const PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const DISC: [u8;8] = [241,154,109,4,17,177,109,188];
const SLICE_LEN: usize = 107; // discriminator [0..8], base [43..75], quote [75..107]
const PARALLEL: usize = 2;

pub struct Settings { batch_size:usize }
impl Settings {
    pub fn from_env() -> Result<Self> {
        let batch_size = env::var("RPC_ACCOUNT_BATCH_SIZE")
            .unwrap_or_else(|_|"100".into()).parse::<usize>()
            .context("invalid RPC_ACCOUNT_BATCH_SIZE for PumpSwap hydration")?;
        ensure!((1..=100).contains(&batch_size),"PumpSwap hydration batch size must be 1..=100");
        Ok(Self{batch_size})
    }
}

fn decode(pool:Pubkey, account:&AccountData, owner:Pubkey) -> Result<DataRecord> {
    ensure!(account.address==pool && account.owner==owner
        && account.view==AccountView::Slice{offset:0,length:SLICE_LEN}
        && account.bytes.len()==SLICE_LEN
        && account.bytes[..8]==DISC,
        "PumpSwap identity hydration owner/discriminator/slice mismatch");
    let mint_a=Pubkey::new_from_array(account.bytes[43..75].try_into()?);
    let mint_b=Pubkey::new_from_array(account.bytes[75..107].try_into()?);
    ensure!(mint_a!=Pubkey::default() && mint_b!=Pubkey::default() && mint_a!=mint_b,
        "PumpSwap identity hydration invalid mint pair");
    Ok(DataRecord {
        identity:RecordIdentity::Pool{address:pool,venue:Venue::PumpSwap,
            mint_a:Some(mint_a),mint_b:Some(mint_b)},
        tvl_usd:None,
        payload:json!({"kind":"pumpswap_identity_hydration_v1"}),
    })
}

fn enqueue(pool:Pubkey, queue:&mut VecDeque<Pubkey>, pending:&mut HashSet<Pubkey>) {
    if pending.insert(pool) {queue.push_back(pool);}
}

async fn read_batch(rpc:RpcHandle, addresses:Vec<Pubkey>)
    ->(Vec<Pubkey>,Result<(u64,Vec<Option<AccountData>>)>) {
    let result=async {
        let response=rpc.call(RpcOperation::Multiple{addresses:addresses.clone(),
            view:AccountView::Slice{offset:0,length:SLICE_LEN},
            min_context_slot:None}).await?;
        let RpcAnswer::Multiple{accounts,..}=response.answer else {
            bail!("PumpSwap hydration expected getMultipleAccounts response");
        };
        ensure!(accounts.len()==addresses.len(),"PumpSwap hydration response length mismatch");
        Ok((response.received_at_ms,accounts))
    }.await;
    (addresses,result)
}

/// initial: persisted PumpSwap addresses lacking mints. live: accepted Registry
/// changes, including new address-only Full-Scan records. finite=true is for
/// the bounded, real-provider standalone check, not the production runtime.
pub async fn run(
    initial:Vec<Pubkey>,
    mut live:broadcast::Receiver<Arc<PoolUpdateBatch>>,
    rpc:RpcHandle,
    output:broadcast::Sender<Arc<DataBatch>>,
    settings:Settings,
    finite:bool,
)->Result<()> {
    let owner:Pubkey=PROGRAM.parse()?;
    ensure!(rpc.supports(&RpcOperation::Multiple{addresses:vec![owner],
        view:AccountView::Slice{offset:0,length:SLICE_LEN},min_context_slot:None}),
        "PumpSwap hydration requires a getMultipleAccounts provider");
    let mut queue=VecDeque::from(initial);
    let mut pending=HashSet::with_capacity(queue.len());
    queue.retain(|id|pending.insert(*id));
    let restored=queue.len();
    let mut jobs=JoinSet::new();
    let mut received=0_u64;
    let mut attempted=0_u64;
    let mut hydrated=0_u64;
    let mut missing=0_u64;
    let mut invalid=0_u64;
    let mut failed_rpc=0_u64;
    let mut consecutive_errors=0_u32;
    let mut batches=0_u64;
    let mut pulse=tokio::time::interval(Duration::from_secs(30));
    pulse.set_missed_tick_behavior(MissedTickBehavior::Delay);
    crate::log_event!(Info,"pumpswap identity hydration started restored_missing={} batch_size={} parallel={} finite={finite}",
        restored,settings.batch_size,PARALLEL);
    loop {
        while jobs.len()<PARALLEL && !queue.is_empty() {
            let mut ids=Vec::with_capacity(settings.batch_size);
            while ids.len()<settings.batch_size {
                let Some(id)=queue.pop_front() else {break;};
                if pending.contains(&id) {ids.push(id);}
            }
            if ids.is_empty(){break;}
            let rpc=rpc.clone();
            jobs.spawn(read_batch(rpc,ids));
        }
        if finite && queue.is_empty() && jobs.is_empty(){break;}
        tokio::select! {
            update=live.recv(), if !finite => match update {
                Ok(batch)=>{
                    for p in &batch.pools {
                        if p.venue!=Venue::PumpSwap {continue;}
                        if p.mint_a.is_some() && p.mint_b.is_some(){
                            // Another source may have already resolved the identity.
                            pending.remove(&p.address);
                        }else {
                            enqueue(p.address,&mut queue,&mut pending);
                            received+=1;
                        }
                    }
                },
                Err(broadcast::error::RecvError::Lagged(n))=>
                    bail!("PumpSwap identity hydration lost {n} Registry update batches"),
                Err(broadcast::error::RecvError::Closed)=>
                    bail!("PumpSwap identity hydration Registry feed closed unexpectedly"),
            },
            completed=jobs.join_next(), if !jobs.is_empty()=>{
                let (addresses,response)=completed.context("missing PumpSwap hydration RPC job")?
                    .context("PumpSwap hydration RPC task panicked")?;
                batches+=1;
                match response {
                    Ok((_received_at_ms,accounts))=>{
                        consecutive_errors=0;
                        let mut records=Vec::new();
                        for (id,account) in addresses.iter().copied().zip(accounts) {
                            attempted+=1;
                            // A simultaneous Rolling-Scan result may have resolved this
                            // address while the RPC read was already in flight.
                            if !pending.remove(&id){continue;}
                            match account {
                                None=>missing+=1,
                                Some(account)=>match decode(id,&account,owner) {
                                    Ok(record)=>{records.push(record);hydrated+=1;},
                                    Err(error)=>{
                                        invalid+=1;
                                        if invalid<=5 {
                                            crate::log_event!(Warn,"pumpswap identity hydration invalid pool={id} error={error:#}");
                                        }
                                    },
                                },
                            }
                        }
                        if !records.is_empty() {
                            output.send(Arc::new(DataBatch{source:SOURCE,
                                received_at_ms:now_unix_ms()?,records}))
                                .map_err(|_|anyhow!("PumpSwap hydration has no Registry subscriber"))?;
                        }
                    },
                    Err(error)=>{
                        failed_rpc+=1;
                        consecutive_errors+=1;
                        for id in addresses {queue.push_back(id);}
                        crate::log_event!(Warn,"pumpswap identity hydration rpc_failure={failed_rpc} consecutive={consecutive_errors} error={error:#}");
                        ensure!(consecutive_errors<5,
                            "PumpSwap hydration RPC repeatedly unavailable; retry via supervisor");
                    },
                }
                if batches%100==0 {
                    crate::log_event!(Info,"pumpswap identity hydration progress attempted={attempted} published={hydrated} missing_accounts={missing} invalid={invalid} rpc_failures={failed_rpc} pending={} in_flight={} batches={batches}",
                        pending.len(),jobs.len());
                }
            },
            _=pulse.tick()=>{
                crate::log_event!(Info,"pumpswap identity hydration status restored_missing={restored} new_missing_updates={received} attempted={attempted} published={hydrated} missing_accounts={missing} invalid={invalid} rpc_failures={failed_rpc} pending={} in_flight={}",
                    pending.len(),jobs.len());
            }
        }
    }
    if finite {ensure!(hydrated>0, "standalone PumpSwap hydration found no valid unresolved pool");}
    crate::log_event!(Info,"pumpswap identity hydration complete attempted={attempted} published={hydrated} missing_accounts={missing} invalid={invalid} rpc_failures={failed_rpc} pending={}",pending.len());
    Ok(())
}
