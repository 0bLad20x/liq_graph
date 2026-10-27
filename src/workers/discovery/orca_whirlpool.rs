//! Orca Whirlpool REST discovery: five legacy sort/filter cursor lanes.
//! Every lane publishes the same source identity; route labels stay diagnostic-only.
use std::{sync::Arc, time::Duration};
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, Mutex}, task::JoinSet, time::{Instant, sleep, sleep_until}};
use crate::core::{DataBatch, DataRecord, RecordIdentity, Venue, now_unix_ms};

const URL: &str = "https://api.orca.so/v2/solana/pools";
const SOURCE: &str = "orca_whirlpool_api";
const PAGE_SIZE: usize = 1000;
const MIN_POSITIVE: &str = "0.000000000001";
const ROUTES: [(&str,&str,&str,&str);5] = [
    ("volume24h_desc","volume24h","desc","minVolume"),
    ("tvl_asc","tvl","asc","minTvl"),
    ("tvl_desc","tvl","desc","minTvl"),
    ("locked_liquidity_desc","lockedliquiditypercent","desc","minLockedLiquidityPercent"),
    ("yieldovertvl24h_desc","yieldovertvl24h","desc","minTvl"),
];

#[derive(Clone,Copy)]
pub struct Settings {pub once:bool,pub max_pages:Option<u32>,pub rps:u32}
#[derive(Deserialize)]
struct Response {data:Vec<Value>,meta:Meta}
#[derive(Deserialize)]
struct Meta {cursor:Cursor}
#[derive(Deserialize)]
struct Cursor {next:Option<String>}

fn key(pool:&Value,path:&str)->Result<Pubkey>{
    let value=pool.pointer(path).and_then(Value::as_str)
        .with_context(||format!("missing Orca {path}"))?;
    let value:Pubkey=value.parse().with_context(||format!("invalid Orca {path}"))?;
    ensure!(value!=Pubkey::default(),"zero Orca key {path}");
    Ok(value)
}
fn tvl(pool:&Value)->Option<f64>{
    let raw=pool.get("tvlUsdc")?;
    let value=raw.as_f64().or_else(||raw.as_str()?.parse::<f64>().ok())?;
    (value.is_finite()&&value>=0.0).then_some(value)
}
fn configuration(payload: &Value) -> Option<crate::core::PoolConfigUpdate> {
    let mapping: &[(&str, &str)] = &[
            ("/feeRate", "fee.rate_raw"),
            ("/protocolFeeRate", "fee.protocol_rate_raw"),
            ("/tickSpacing", "tick_spacing"),
            ("/feeTierIndex", "fee_tier_index"),
            ("/whirlpoolsConfig", "config_account"),
            ("/tokenVaultA", "vault_a"),
            ("/tokenVaultB", "vault_b"),
            ("/addressLookupTable", "address_lookup_table"),
            ("/adaptiveFeeEnabled", "fee.adaptive_enabled"),
            ("/adaptiveFee/constants/adaptiveFeeControlFactor", "fee.adaptive.control_factor"),
            ("/adaptiveFee/constants/decayPeriod", "fee.adaptive.decay_period"),
            ("/adaptiveFee/constants/filterPeriod", "fee.adaptive.filter_period"),
            ("/adaptiveFee/constants/majorSwapThresholdTicks", "fee.adaptive.major_swap_threshold_ticks"),
            ("/adaptiveFee/constants/maxVolatilityAccumulator", "fee.adaptive.max_volatility_accumulator"),
            ("/adaptiveFee/constants/reductionFactor", "fee.adaptive.reduction_factor"),
            ("/adaptiveFee/constants/tickGroupSize", "fee.adaptive.tick_group_size"),
            ("/adaptiveFee/maxRate", "fee.adaptive.max_rate_raw"),
            ("/adaptiveFee/currentRate", "fee.snapshot_adaptive_rate_raw"),
            ("/adaptiveFee/variables/lastMajorSwapTimestamp", "fee.snapshot_adaptive_last_major_swap_timestamp"),
            ("/adaptiveFee/variables/lastReferenceUpdateTimestamp", "fee.snapshot_adaptive_last_reference_update_timestamp"),
            ("/adaptiveFee/variables/tickGroupIndexReference", "fee.snapshot_adaptive_tick_group_index_reference"),
            ("/adaptiveFee/variables/volatilityAccumulator", "fee.snapshot_adaptive_volatility_accumulator"),
            ("/adaptiveFee/variables/volatilityReference", "fee.snapshot_adaptive_volatility_reference"),
        ];
    let mut values = serde_json::Map::new();
    for &(path, name) in mapping {
        if let Some(value) = payload.pointer(path)
            .filter(|v| v.is_string() || v.is_number() || v.is_boolean()) {
            values.insert(name.to_owned(), value.clone());
        }
    }
    if let Some(adaptive) = payload.get("adaptiveFee") {
        values.insert("fee.adaptive_available".into(), Value::Bool(adaptive.is_object()));
    }
    let clear = values.get("fee.adaptive_available") == Some(&Value::Bool(false));
    if values.is_empty() { return None; }
    let observation = values.keys().any(|k| k.starts_with("fee.snapshot_")).then_some(
        crate::core::ConfigObservation { value_prefix: "fee.snapshot_", timestamp_key: "fee.snapshot_at_ms" });
    Some(crate::core::PoolConfigUpdate {
        values,
        remove_prefixes: if clear { &["fee.adaptive.", "fee.snapshot_adaptive_"] } else { &[] },
        remove_keys: if clear { &["fee.snapshot_at_ms"] } else { &[] },
        observation,
    })
}

fn decode(payload:Value)->Result<DataRecord>{
    let address=key(&payload,"/address")?;
    let mint_a=key(&payload,"/tokenMintA")?;
    let mint_b=key(&payload,"/tokenMintB")?;
    ensure!(mint_a!=mint_b,"Orca pool has identical mints");
    Ok(DataRecord{
        identity:RecordIdentity::Pool{
            address,venue:Venue::OrcaWhirlpool,mint_a:Some(mint_a),mint_b:Some(mint_b),
        },
        tvl_usd:tvl(&payload),
        config: configuration(&payload),
        payload,
    })
}
async fn wait_slot(gate:&Mutex<Instant>,spacing:Duration){
    let mut next=gate.lock().await;
    sleep_until(*next).await;
    *next=Instant::now()+spacing;
}
async fn cooldown(gate:&Mutex<Instant>,duration:Duration){
    let mut next=gate.lock().await;
    *next=(*next).max(Instant::now()+duration);
}
async fn lane(client:reqwest::Client,gate:Arc<Mutex<Instant>>,
    output:broadcast::Sender<Arc<DataBatch>>,settings:Settings,
    name:&'static str,sort:&'static str,direction:&'static str,filter:&'static str)->Result<()>{
    let spacing=Duration::from_secs_f64(1.0/f64::from(settings.rps));
    loop{
        let started=Instant::now();
        let mut cursor:Option<String>=None;
        let mut page_no=0_u32;
        let mut received=0_usize;
        let mut accepted=0_usize;
        let mut rejected=0_usize;
        let mut errors=0_u64;
        loop{
            wait_slot(&gate,spacing).await;
            let mut request=client.get(URL).query(&[
                ("size","1000"),("includeBlocked","true"),
                ("sortBy",sort),("sortDirection",direction),(filter,MIN_POSITIVE),
            ]);
            if sort.ends_with("24h"){request=request.query(&[("stats","24h")]);}
            if let Some(next)=&cursor{request=request.query(&[("next",next)]);}
            let response=match request.send().await{
                Ok(v)=>v,
                Err(err)=>{
                    errors+=1;
                    crate::log_event!(Warn,"orca route={name} request_error={err:#}");
                    sleep(Duration::from_secs(2)).await;continue;
                }
            };
            if response.status()==reqwest::StatusCode::TOO_MANY_REQUESTS{
                errors+=1;
                let secs=response.headers().get(reqwest::header::RETRY_AFTER)
                    .and_then(|v|v.to_str().ok()).and_then(|v|v.parse::<u64>().ok()).unwrap_or(5);
                cooldown(&gate,Duration::from_secs(secs)).await;
                crate::log_event!(Info,"orca route={name} http_429 cooldown_s={secs}");
                continue;
            }
            if response.status().is_client_error(){
                bail!("Orca route {name} HTTP {}",response.status());
            }
            if !response.status().is_success(){
                errors+=1;
                crate::log_event!(Warn,"orca route={name} http_error={}",response.status());
                sleep(Duration::from_secs(2)).await;continue;
            }
            let bytes=response.bytes().await.context("read Orca response")?;
            let page:Response=serde_json::from_slice(&bytes)
                .with_context(||format!("decode Orca route={name} page={}",page_no+1))?;
            ensure!(page.data.len()<=PAGE_SIZE,"Orca returned oversized page");
            ensure!(!page.data.is_empty()||page.meta.cursor.next.is_none(),
                "Orca empty page with cursor");
            if let Some(next)=&page.meta.cursor.next{
                ensure!(!next.is_empty(),"Orca empty cursor");
                ensure!(cursor.as_ref()!=Some(next),"Orca repeated cursor");
            }
            page_no+=1;
            received+=page.data.len();
            let mut records=Vec::with_capacity(page.data.len());
            for payload in page.data{
                match decode(payload){
                    Ok(record)=>records.push(record),
                    Err(err)=>{
                        rejected+=1;
                        if rejected<=5{crate::log_event!(Warn,"orca route={name} rejected error={err:#}");}
                    }
                }
            }
            accepted+=records.len();
            if !records.is_empty(){
                output.send(Arc::new(DataBatch{
                    source:SOURCE,received_at_ms:now_unix_ms()?,records,
                })).map_err(|_|anyhow!("Orca batch has no consumer"))?;
            }
            if page_no==1||page_no%10==0{
                crate::log_event!(Info,"orca route={name} pages={page_no} received={received} accepted={accepted} rejected={rejected} errors={errors} elapsed_ms={}",started.elapsed().as_millis());
            }
            cursor=page.meta.cursor.next;
            if cursor.is_none()||settings.max_pages.is_some_and(|max|page_no>=max){break;}
        }
        crate::log_event!(Info,"orca route complete route={name} pages={page_no} received={received} accepted={accepted} rejected={rejected} errors={errors} elapsed_ms={}",started.elapsed().as_millis());
        if settings.once{return Ok(());}
    }
}
pub async fn run(client:reqwest::Client,output:broadcast::Sender<Arc<DataBatch>>,
    settings:Settings)->Result<()>{
    ensure!(settings.rps>0&&settings.rps<=100,"Orca RPS must be 1..=100");
    let gate=Arc::new(Mutex::new(Instant::now()));
    let mut tasks=JoinSet::new();
    for (name,sort,direction,filter) in ROUTES{
        let http=client.clone();
        let limiter=gate.clone();
        let tx=output.clone();
        tasks.spawn(lane(http,limiter,tx,settings,name,sort,direction,filter));
    }
    // A lost lane is a worker failure. The orchestrator restarts this complete
    // source; remaining lanes must not hide a permanently stopped cursor.
    while let Some(joined)=tasks.join_next().await {
        joined.context("Orca route task panicked")??;
    }
    if !settings.once{bail!("all Orca API routes stopped");}
    Ok(())
}
