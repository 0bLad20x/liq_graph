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
const REQUEST_ATTEMPTS:u32=3;
const LOCAL_RESTART_BACKOFF:Duration=Duration::from_secs(15);

const ROUTES: [(&str,&str,&str,&str);5] = [
    ("volume24h_desc","volume24h","desc","minVolume"),
    ("tvl_asc","tvl","asc","minTvl"),
    ("tvl_desc","tvl","desc","minTvl"),
    ("locked_liquidity_desc","lockedliquiditypercent","desc","minLockedLiquidityPercent"),
    ("yieldovertvl24h_desc","yieldovertvl24h","desc","minTvl"),
];

#[derive(Clone,Copy)]
pub struct Settings {pub once:bool,pub max_pages:Option<u32>,pub rps:u32}
#[derive(Debug,Deserialize)]
struct Response {data:Vec<Value>,meta:Meta}
#[derive(Debug,Deserialize)]
struct Meta {cursor:Cursor}
#[derive(Debug,Deserialize)]
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
/// A temporary error belongs to one HTTP cursor, not to the worker.
fn retryable_status(status:reqwest::StatusCode)->bool {
    status==reqwest::StatusCode::TOO_MANY_REQUESTS
        || status==reqwest::StatusCode::REQUEST_TIMEOUT
        || status.is_server_error()
}
fn retry_delay(attempt:u32)->Duration {
    Duration::from_secs(2_u64.saturating_pow(attempt.min(4)))
}
#[derive(Debug)]
struct PermanentPageError(String);
impl std::fmt::Display for PermanentPageError {
    fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result{f.write_str(&self.0)}
}
impl std::error::Error for PermanentPageError {}

/// All Orca sort lanes use the existing client and shared API-RPS gate.
/// Send, response-body read, decoding and 408/429/5xx errors are retryable.
/// After three failures the owning lane waits while keeping its cursor.
async fn fetch_page(client:&reqwest::Client,gate:&Mutex<Instant>,spacing:Duration,
    name:&str,sort:&str,direction:&str,filter:&str,cursor:Option<&str>)->Result<Response>{
    fetch_page_at(URL,client,gate,spacing,name,sort,direction,filter,cursor).await
}
async fn fetch_page_at(url:&str,client:&reqwest::Client,gate:&Mutex<Instant>,spacing:Duration,
    name:&str,sort:&str,direction:&str,filter:&str,cursor:Option<&str>)->Result<Response>{
    for attempt in 1..=REQUEST_ATTEMPTS {
        wait_slot(gate,spacing).await;
        let mut request=client.get(url).query(&[
            ("size","1000"),("includeBlocked","true"),
            ("sortBy",sort),("sortDirection",direction),(filter,MIN_POSITIVE),
        ]);
        if sort.ends_with("24h"){request=request.query(&[("stats","24h")]);}
        if let Some(next)=cursor{request=request.query(&[("next",next)]);}
        let response=request.send().await;
        let outcome:Result<Response>=match response {
            Err(error)=>Err(error).context("Orca page request transport"),
            Ok(response)=>{
                let status=response.status();
                if retryable_status(status) {
                    if status==reqwest::StatusCode::TOO_MANY_REQUESTS {
                        let seconds=response.headers().get(reqwest::header::RETRY_AFTER)
                            .and_then(|v|v.to_str().ok())
                            .and_then(|v|v.parse::<u64>().ok()).unwrap_or(5).clamp(1,60);
                        cooldown(gate,Duration::from_secs(seconds)).await;
                        crate::log_event!(Warn,"orca http_429 route={name} cooldown_s={seconds}");
                    }
                    Err(anyhow!("Orca route {name} retryable HTTP {status}"))
                }else if !status.is_success() {
                    return Err(PermanentPageError(format!(
                        "Orca route {name} nonretryable HTTP {status}"
                    )).into());
                }else {
                    async {
                        let bytes=response.bytes().await.context("read Orca response")?;
                        let page:Response=serde_json::from_slice(&bytes)
                            .with_context(||format!("decode Orca route={name} response"))?;
                        ensure!(page.data.len()<=PAGE_SIZE,"Orca returned oversized page");
                        ensure!(!page.data.is_empty()||page.meta.cursor.next.is_none(),
                            "Orca empty page with cursor");
                        Ok(page)
                    }.await
                }
            }
        };
        match outcome {
            Ok(page)=>return Ok(page),
            Err(error)=>{
                if attempt==REQUEST_ATTEMPTS {
                    crate::log_event!(Warn,"orca page_exhausted route={name} cursor_present={} attempts={attempt} error={error:#}",cursor.is_some());
                    return Err(error);
                }
                let backoff=retry_delay(attempt);
                crate::log_event!(Warn,"orca page_retry route={name} cursor_present={} attempt={attempt}/{REQUEST_ATTEMPTS} retry_s={} error={error:#}",
                    cursor.is_some(),backoff.as_secs());
                sleep(backoff).await;
            }
        }
    }
    unreachable!("Orca request attempts are positive")
}

/// A route owns its cursor and partial progress. Temporary page failures are
/// handled in place; only a permanent provider/cursor error resets this lane.
async fn lane_pass(client:&reqwest::Client,gate:&Mutex<Instant>,
    output:&broadcast::Sender<Arc<DataBatch>>,settings:Settings,
    name:&'static str,sort:&'static str,direction:&'static str,filter:&'static str)->Result<()>{
    let spacing=Duration::from_secs_f64(1.0/f64::from(settings.rps));
    let started=Instant::now();
    let mut cursor:Option<String>=None;
    let mut page_no=0_u32;
    let mut received=0_usize;
    let mut accepted=0_usize;
    let mut rejected=0_usize;
    let mut errors=0_u64;
    let mut consecutive_errors=0_u32;
    loop{
        let page=match fetch_page(client,gate,spacing,name,sort,direction,filter,cursor.as_deref()).await {
            Ok(page)=>{consecutive_errors=0;page},
            Err(error)=>{
                if settings.once || error.downcast_ref::<PermanentPageError>().is_some(){
                    return Err(error);
                }
                errors+=1;
                consecutive_errors=consecutive_errors.saturating_add(1);
                let backoff=retry_delay(consecutive_errors);
                crate::log_event!(Warn,"orca route_paused route={name} cursor_present={} pages={page_no} retry_s={} errors={errors} error={error:#}",
                    cursor.is_some(),backoff.as_secs());
                sleep(backoff).await;
                continue;
            }
        };
        if let Some(next)=&page.meta.cursor.next {
            ensure!(!next.is_empty(),"Orca empty cursor route={name}");
            ensure!(cursor.as_ref()!=Some(next),"Orca repeated cursor route={name}");
        }
        page_no+=1;
        received+=page.data.len();
        let mut records=Vec::with_capacity(page.data.len());
        for payload in page.data{
            match decode(payload){
                Ok(record)=>records.push(record),
                Err(error)=>{
                    rejected+=1;
                    if rejected<=5{crate::log_event!(Warn,"orca route={name} rejected error={error:#}");}
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
            crate::log_event!(Info,"orca route={name} pages={page_no} received={received} accepted={accepted} rejected={rejected} errors={errors} elapsed_ms={}",
                started.elapsed().as_millis());
        }
        cursor=page.meta.cursor.next;
        if cursor.is_none()||settings.max_pages.is_some_and(|max|page_no>=max){break;}
    }
    crate::log_event!(Info,"orca route complete route={name} pages={page_no} received={received} accepted={accepted} rejected={rejected} errors={errors} elapsed_ms={}",
        started.elapsed().as_millis());
    Ok(())
}
async fn lane(client:reqwest::Client,gate:Arc<Mutex<Instant>>,
    output:broadcast::Sender<Arc<DataBatch>>,settings:Settings,
    name:&'static str,sort:&'static str,direction:&'static str,filter:&'static str)->Result<()>{
    loop{
        match lane_pass(&client,&gate,&output,settings,name,sort,direction,filter).await {
            Ok(())=>{},
            Err(error)=>{
                if settings.once||output.receiver_count()==0{return Err(error);}
                crate::log_event!(Warn,"orca route_local_restart route={name} backoff_s={} error={error:#}",
                    LOCAL_RESTART_BACKOFF.as_secs());
                sleep(LOCAL_RESTART_BACKOFF).await;
            }
        }
        if settings.once{return Ok(());}
    }
}
errors={errors} elapsed_ms={}",started.elapsed().as_millis());
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
    // Only a fatal child exit (e.g. lost Registry consumer/panic) reaches here.
    // HTTP retries and cursor restarts are contained inside each lane.
    while let Some(joined)=tasks.join_next().await {
        joined.context("Orca route task panicked")??;
    }
    if !settings.once{bail!("all Orca API routes stopped");}
    Ok(())
}
