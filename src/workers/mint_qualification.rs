//! Central RAM Mint Registry: discovery identities + Jupiter economic updates.
//! No PostgreSQL, REST transport or Graph dependencies.
//! Persistence is the only durable owner; restored MintState is injected on boot.
use std::{collections::{HashMap,HashSet},sync::{Arc,RwLock},time::Duration};
use anyhow::{Result,ensure};
use solana_pubkey::Pubkey;
use serde_json::Value;
use tokio::sync::{broadcast,watch};
use crate::core::{DataBatch,MintState,PoolState,PoolUpdateBatch,RecordIdentity};

const MIN_LIQUIDITY_USD:f64=2000.0;
const MIN_MARKET_CAP_USD:f64=10000.0;
const MISSING_TARGET_BATCH:usize=20000;

#[derive(Clone,Default)]
pub struct MintRegistry {
    entries:Arc<RwLock<HashMap<Pubkey,MintState>>>,
}
impl MintRegistry {
    pub fn restore(rows:Vec<MintState>)->Result<Self> {
        let mut entries=HashMap::with_capacity(rows.len());
        for row in rows {
            ensure!(row.address!=Pubkey::default(),"invalid restored mint identity");
            ensure!(entries.insert(row.address,row).is_none(),"duplicate restored mint");
        }
        Ok(Self{entries:Arc::new(RwLock::new(entries))})
    }
    pub fn get(&self,address:Pubkey)->Option<MintState> {
        self.entries.read().expect("mint registry lock poisoned").get(&address).cloned()
    }
    pub fn snapshot(&self)->Vec<MintState> {
        self.entries.read().expect("mint registry lock poisoned").values().cloned().collect()
    }
    fn apply(&self,batch:&DataBatch)->Vec<Pubkey> {
        fn amount(v:Option<&Value>)->Option<f64> {
            let v=v?;
            let n=v.as_f64().or_else(||v.as_str()?.parse::<f64>().ok())?;
            (n.is_finite()&&n>=0.0).then_some(n)
        }
        fn date(v:Option<&Value>)->Option<String> {
            match v? {
                Value::Number(n)=>Some(n.to_string()),
                Value::String(s) if !s.trim().is_empty()=>Some(s.clone()),
                _=>None,
            }
        }
        let mut entries=self.entries.write().expect("mint registry lock poisoned");
        let mut changed=Vec::new();
        for record in &batch.records {
            let RecordIdentity::Mint{address}=&record.identity else{continue};
            let v=&record.payload;
            let previous=entries.get(address);
            if previous.is_some_and(|v|v.fetched_at_ms>batch.received_at_ms){continue;}
            let state=MintState{
                address:*address,
                token_program:v.get("tokenProgram").and_then(Value::as_str)
                    .and_then(|s|s.parse::<Pubkey>().ok())
                    .or_else(||previous.and_then(|v|v.token_program)),
                decimals:v.get("decimals").and_then(Value::as_u64)
                    .and_then(|n|u8::try_from(n).ok())
                    .or_else(||previous.and_then(|v|v.decimals)),
                created_at:date(v.get("createdAt"))
                    .or_else(||previous.and_then(|v|v.created_at.clone())),
                updated_at:date(v.get("updatedAt"))
                    .or_else(||previous.and_then(|v|v.updated_at.clone())),
                liquidity_usd:amount(v.get("liquidity")),
                market_cap_usd:amount(v.get("mcap")),
                holder_count:v.get("holderCount").and_then(Value::as_u64),
                usd_price:amount(v.get("usdPrice")),
                fetched_at_ms:batch.received_at_ms,
            };
            entries.insert(*address,state);
            changed.push(*address);
        }
        changed
    }
}
fn eligible(v:&MintState)->bool {
    v.liquidity_usd.is_some_and(|n|n>=MIN_LIQUIDITY_USD)
        && v.market_cap_usd.is_some_and(|n|n>=MIN_MARKET_CAP_USD)
}

/// One authoritative continuously updated economic set. Registry signals only
/// add mint identities; their economic state is owned by the RAM Mint Registry.
/// Jupiter is given new/missing and qualified mints; no database scan is needed.
pub async fn run(
    restored_pools:Vec<PoolState>,
    mints:MintRegistry,
    mut pool_updates:broadcast::Receiver<Arc<PoolUpdateBatch>>,
    mut jupiter_updates:broadcast::Receiver<Arc<DataBatch>>,
    qualified_tx:watch::Sender<Arc<[Pubkey]>>,
    jupiter_targets:watch::Sender<Arc<[Pubkey]>>,
)->Result<()> {
    let mut known=HashSet::<Pubkey>::new();
    for pool in restored_pools {
        for mint in [pool.mint_a,pool.mint_b].into_iter().flatten(){known.insert(mint);}
    }
    let mut qualified=HashSet::<Pubkey>::new();
    for state in mints.snapshot(){if known.contains(&state.address)&&eligible(&state){
        qualified.insert(state.address);
    }}
    let mut ticker=tokio::time::interval(Duration::from_secs(10));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut missing_cursor=0_usize;
    let mut dirty=true;
    let mut pool_open=true;
    let mut jupiter_open=true;
    loop {
        tokio::select! {
            event=pool_updates.recv(),if pool_open=>match event {
                Ok(batch)=>{
                    for pool in &batch.pools {
                        for mint in [pool.mint_a,pool.mint_b].into_iter().flatten(){
                            if known.insert(mint) {
                                if mints.get(mint).as_ref().is_some_and(eligible) {
                                    qualified.insert(mint);
                                }
                                dirty=true;
                            }
                        }
                    }
                },
                Err(broadcast::error::RecvError::Lagged(n))=>
                    anyhow::bail!("mint registry lost {n} pool updates; cannot infer undiscovered identities"),
                Err(broadcast::error::RecvError::Closed)=>pool_open=false,
            },
            event=jupiter_updates.recv(),if jupiter_open=>match event {
                Ok(batch)=>{
                    for mint in mints.apply(&batch){
                        if !known.contains(&mint){continue;}
                        if mints.get(mint).as_ref().is_some_and(eligible){
                            qualified.insert(mint);
                        }else{
                            qualified.remove(&mint);
                        }
                        dirty=true;
                    }
                },
                Err(broadcast::error::RecvError::Lagged(n))=>
                    anyhow::bail!("mint registry lost {n} Jupiter economic updates"),
                Err(broadcast::error::RecvError::Closed)=>jupiter_open=false,
            },
            _=ticker.tick()=>{
                if dirty {
                    let mut v=qualified.iter().copied().collect::<Vec<_>>();
                    v.sort_unstable();
                    let next:Arc<[Pubkey]>=Arc::from(v);
                    if next.as_ref()!=qualified_tx.borrow().as_ref(){
                        qualified_tx.send_replace(next);
                    }
                    dirty=false;
                }
                // High priority: qualified mints continuously refreshed.
                let mut targets=qualified.iter().copied().collect::<Vec<_>>();
                targets.sort_unstable();
                let mut missing=known.iter().copied().filter(|a|mints.get(*a).is_none())
                    .collect::<Vec<_>>();
                missing.sort_unstable();
                if !missing.is_empty(){
                    if missing_cursor>=missing.len(){missing_cursor=0;}
                    for i in 0..MISSING_TARGET_BATCH.min(missing.len()){
                        targets.push(missing[(missing_cursor+i)%missing.len()]);
                    }
                    missing_cursor=(missing_cursor+MISSING_TARGET_BATCH)%missing.len();
                }
                targets.sort_unstable();targets.dedup();
                let next:Arc<[Pubkey]>=Arc::from(targets);
                if next.as_ref()!=jupiter_targets.borrow().as_ref(){
                    jupiter_targets.send_replace(next);
                }
                let (catalog_mints, holders_known, usd_prices_known)={
                    let guard=mints.entries.read().expect("mint lock");
                    (guard.len(),
                     guard.values().filter(|v|v.holder_count.is_some()).count(),
                     guard.values().filter(|v|v.usd_price.is_some()).count())
                };
                crate::log_event!(Info,"mint registry pools_mints={} catalog_mints={} qualified={} refresh_targets={} holders_known={} prices_known={}",
                    known.len(),catalog_mints,qualified.len(),jupiter_targets.borrow().len(),
                    holders_known,usd_prices_known);
            }
        }
        if !pool_open&&!jupiter_open{break;}
    }
    Ok(())
}
