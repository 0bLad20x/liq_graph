//! Raydium REST discovery: ten cursor routes, shared rate limit and early head refresh.
//! ASC/DESC liquidity passes may end early only after strictly crossed TVL frontiers
//! with observed address overlap. Ties or non-monotonic responses disable early exit.
use std::{collections::{HashMap,HashSet}, env, sync::Arc, time::Duration};
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, Mutex}, task::JoinSet, time::{Instant, sleep, sleep_until}};
use crate::core::{DataBatch, DataRecord, RecordIdentity, Venue, now_unix_ms};
use crate::workers::pool_registry::PoolRegistry;

const URL: &str = "https://api-v3.raydium.io/pools/info/list-v2";
const SOURCE: &str = "raydium_api";
const PAGE_SIZE: usize = 1000;
const OVERLAP_CONFIRM_ADDRESSES: usize = 32;
const TARGET_POOL_BATCH: usize = 10;
const TARGET_POOLS_PER_CYCLE: usize = 512;
const TARGET_RETRY_DELAY: Duration = Duration::from_secs(1800);
const TARGET_SCAN_INTERVAL: Duration = Duration::from_secs(30);
const REQUEST_ATTEMPTS: u32 = 3;
const ROUTE_FAILURE_BACKOFF: Duration = Duration::from_secs(15);
const TARGET_FAILURE_RETRY_DELAY: Duration = Duration::from_secs(45);

/// A failed HTTP request must never discard a route's cursor or restart
/// unrelated Raydium lanes. Permanent malformed cursors restart only that lane.
fn retryable_status(status:reqwest::StatusCode)->bool {
    status==reqwest::StatusCode::TOO_MANY_REQUESTS ||
    status==reqwest::StatusCode::REQUEST_TIMEOUT ||
    status.is_server_error()
}
fn retry_delay(attempt:u32)->Duration {
    Duration::from_secs(2_u64.saturating_pow(attempt.min(4)))
}

const ROUTES: [(&str, &str); 10] = [
    ("liquidity","asc"), ("liquidity","desc"),
    ("volume24h","asc"), ("volume24h","desc"),
    ("fee24h","asc"), ("fee24h","desc"),
    ("volume7d","asc"), ("volume7d","desc"),
    ("volume30d","asc"), ("volume30d","desc"),
];

const AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const CLMM: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
const STABLE: &str = "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h";

#[derive(Clone, Copy)]
pub struct Settings { pub once: bool, pub max_pages: Option<u32>, pub rps: u32,
    pub enabled_venues: crate::core::EnabledVenues }

#[derive(Deserialize)]
struct Response { success: bool, data: Option<Page> }
#[derive(Deserialize)]
struct Page {
    data: Vec<Value>,
    #[serde(rename="nextPageId")]
    next: Option<String>,
}
#[derive(Deserialize)]
struct IdsResponse { success:bool, data:Option<Vec<Value>> }

struct Published {
    batch: Option<Arc<DataBatch>>,
    received: usize,
    accepted: usize,
    rejected: usize,
    rejected_unknown: usize,
}

fn key(pool: &Value, path: &str) -> Result<Pubkey> {
    let value = pool.pointer(path).and_then(Value::as_str)
        .with_context(||format!("missing Raydium {path}"))?;
    let parsed: Pubkey = value.parse().with_context(||format!("invalid Raydium {path}"))?;
    ensure!(parsed != Pubkey::default(), "zero Raydium key {path}");
    Ok(parsed)
}
fn tvl(pool: &Value) -> Option<f64> {
    let raw = pool.get("tvl")?;
    let v = raw.as_f64().or_else(||raw.as_str()?.parse::<f64>().ok())?;
    (v.is_finite() && v >= 0.0).then_some(v)
}
fn configuration(payload: &Value, venue: Venue) -> Option<crate::core::PoolConfigUpdate> {
    let mapping: &[(&str, &str)] = match venue {
        Venue::RaydiumAmmV4 => &[
            ("/feeRate", "fee.rate_raw"),
            ("/feeOn", "fee.on"),
            ("/hasDynamicFee", "fee.dynamic_enabled"),
            ("/marketId", "market_id"),
        ],
        Venue::RaydiumClmm => &[
            ("/feeRate", "fee.rate_raw"),
            ("/feeOn", "fee.on"),
            ("/hasDynamicFee", "fee.dynamic_enabled"),
            ("/config/id", "config_account"),
            ("/config/tickSpacing", "tick_spacing"),
            ("/config/tradeFeeRate", "fee.trade_rate_raw"),
            ("/config/protocolFeeRate", "fee.protocol_rate_raw"),
            ("/config/fundFeeRate", "fee.fund_rate_raw"),
        ],
        Venue::RaydiumCpmm => &[
            ("/feeRate", "fee.rate_raw"),
            ("/feeOn", "fee.on"),
            ("/hasDynamicFee", "fee.dynamic_enabled"),
            ("/config/id", "config_account"),
            ("/config/tradeFeeRate", "fee.trade_rate_raw"),
            ("/config/protocolFeeRate", "fee.protocol_rate_raw"),
            ("/config/fundFeeRate", "fee.fund_rate_raw"),
            ("/config/creatorFeeRate", "fee.creator_rate_raw"),
        ],
        Venue::RaydiumStable => &[
            ("/feeRate", "fee.rate_raw"),
            ("/feeOn", "fee.on"),
            ("/hasDynamicFee", "fee.dynamic_enabled"),
            ("/marketId", "market_id"),
        ],
        _ => return None,
    };
    let mut values = serde_json::Map::new();
    for &(path, name) in mapping {
        if let Some(value) = payload.pointer(path)
            .filter(|v| v.is_string() || v.is_number() || v.is_boolean()) {
            values.insert(name.to_owned(), value.clone());
        }
    }
    if values.is_empty() { return None; }
    let observation = values.keys().any(|k| k.starts_with("fee.snapshot_")).then_some(
        crate::core::ConfigObservation { value_prefix: "fee.snapshot_", timestamp_key: "fee.snapshot_at_ms" });
    Some(crate::core::PoolConfigUpdate {
        values,
        remove_prefixes: &[],
        remove_keys: &[],
        observation,
    })
}

fn decode(payload: Value) -> Result<DataRecord> {
    let program = payload.get("programId").and_then(Value::as_str)
        .context("Raydium programId missing")?;
    let venue = match program {
        AMM_V4 => Venue::RaydiumAmmV4,
        CPMM => Venue::RaydiumCpmm,
        CLMM => Venue::RaydiumClmm,
        STABLE => Venue::RaydiumStable,
        other => bail!("unknown Raydium programId {other}"),
    };
    let address = key(&payload,"/id")?;
    let mint_a = key(&payload,"/mintA/address")?;
    let mint_b = key(&payload,"/mintB/address")?;
    ensure!(mint_a != mint_b, "Raydium pool with identical mints");
    Ok(DataRecord {
        identity: RecordIdentity::Pool { address, venue,
            mint_a: Some(mint_a), mint_b: Some(mint_b) },
        tvl_usd: tvl(&payload),
        config: configuration(&payload, venue),
        payload,
    })
}

async fn wait_slot(gate: &Mutex<Instant>, spacing: Duration) {
    let mut next = gate.lock().await;
    sleep_until(*next).await;
    *next = Instant::now() + spacing;
}
async fn cool_down(gate: &Mutex<Instant>, duration: Duration) {
    let mut next = gate.lock().await;
    *next = (*next).max(Instant::now() + duration);
}

/// All full-scan and head-refresh requests share this transport and rate limit.
/// A bounded request retry covers send, body-read, JSON and provider 5xx failures.
/// The route owner keeps its cursor when these attempts are exhausted.
async fn fetch_page(
    client: &reqwest::Client, gate: &Mutex<Instant>, spacing: Duration,
    sort: &str, direction: &str, cursor: Option<&str>,
) -> Result<Page> {
    for attempt in 1..=REQUEST_ATTEMPTS {
        wait_slot(gate, spacing).await;
        let mut request = client.get(URL).query(&[
            ("sortField",sort), ("sortType",direction), ("size","1000"),
        ]);
        if let Some(next) = cursor { request = request.query(&[("nextPageId",next)]); }
        let response = request.send().await;
        let outcome:Result<Page> = match response {
            Err(error) => Err(error).context("Raydium page request transport"),
            Ok(response) => {
                let status=response.status();
                if retryable_status(status) {
                    if status==reqwest::StatusCode::TOO_MANY_REQUESTS {
                        let seconds=response.headers().get(reqwest::header::RETRY_AFTER)
                            .and_then(|v|v.to_str().ok()).and_then(|v|v.parse::<u64>().ok())
                            .unwrap_or(5).clamp(1,60);
                        cool_down(gate,Duration::from_secs(seconds)).await;
                        crate::log_event!(Warn,"raydium page http_429 sort={sort} direction={direction} cooldown_s={seconds}");
                    }
                    Err(anyhow!("Raydium {sort}/{direction} retryable HTTP {status}"))
                }else if !status.is_success() {
                    // Invalid credentials, invalid queries and other permanent 4xx:
                    // restart only this route rather than burning through retries.
                    bail!("Raydium {sort}/{direction} nonretryable HTTP {status}");
                }else{
                    async {
                        let bytes=response.bytes().await.context("read Raydium page bytes")?;
                        let envelope:Response=serde_json::from_slice(&bytes)
                            .context("decode Raydium envelope")?;
                        ensure!(envelope.success,"Raydium success=false");
                        let page=envelope.data.context("Raydium response missing data")?;
                        ensure!(page.data.len()<=PAGE_SIZE,"Raydium returned oversized page");
                        ensure!(!page.data.is_empty() ||
                            page.next.as_deref().is_none_or(str::is_empty),
                            "Raydium empty page with cursor");
                        Ok(page)
                    }.await
                }
            }
        };
        match outcome {
            Ok(page)=>return Ok(page),
            Err(error)=>{
                if attempt==REQUEST_ATTEMPTS {
                    crate::log_event!(Warn,"raydium page_exhausted sort={sort} direction={direction} cursor_present={} attempts={attempt} error={error:#}",
                        cursor.is_some());
                    return Err(error);
                }
                let backoff=retry_delay(attempt);
                crate::log_event!(Warn,"raydium page_retry sort={sort} direction={direction} cursor_present={} attempt={attempt}/{REQUEST_ATTEMPTS} retry_s={} error={error:#}",
                    cursor.is_some(),backoff.as_secs());
                sleep(backoff).await;
            }
        }
    }
    unreachable!("bounded retry loop")
}

/// Decode the full provider objects once and publish the existing DataBatch contract.
fn publish(
    output: &broadcast::Sender<Arc<DataBatch>>, payloads: Vec<Value>,
    sort: &str, direction: &str, enabled: crate::core::EnabledVenues,
) -> Result<Published> {
    let received = payloads.len();
    let mut records = Vec::with_capacity(received);
    let mut rejected = 0;
    let mut rejected_unknown = 0;
    for payload in payloads {
        match decode(payload) {
            Ok(record) => if matches!(&record.identity,RecordIdentity::Pool{venue,..} if enabled.contains(*venue)) {
                records.push(record);
            },
            Err(error) => {
                rejected += 1;
                if error.to_string().contains("unknown Raydium programId") { rejected_unknown += 1; }
                if rejected <= 5 {
                    crate::log_event!(Warn,"raydium sort={sort} direction={direction} decode_rejected error={error:#}");
                }
            }
        }
    }
    let accepted = records.len();
    let batch = if records.is_empty() { None } else {
        let batch = Arc::new(DataBatch { source: SOURCE,
            received_at_ms: now_unix_ms()?, records });
        output.send(batch.clone()).map_err(|_|anyhow!("Raydium batch has no consumer"))?;
        Some(batch)
    };
    Ok(Published { batch, received, accepted, rejected, rejected_unknown })
}

/// A single paired liquidity pass; this state never leaves the Raydium worker.
struct LiquidityOverlap {
    asc_seen: HashSet<Pubkey>,
    desc_seen: HashSet<Pubkey>,
    asc_last: Option<f64>,
    desc_last: Option<f64>,
    ascending_sorted: bool,
    descending_sorted: bool,
    common_addresses: usize,
    crossed: bool,
}
impl LiquidityOverlap {
    fn new() -> Self {
        Self {
            asc_seen: HashSet::new(), desc_seen: HashSet::new(),
            asc_last: None, desc_last: None,
            ascending_sorted: true, descending_sorted: true,
            common_addresses: 0, crossed: false,
        }
    }
    fn observe(&mut self, direction: &str, batch: &DataBatch) {
        if self.crossed { return; }
        let asc = direction == "asc";
        for record in &batch.records {
            let Some(value) = record.tvl_usd else {
                if asc { self.ascending_sorted = false; } else { self.descending_sorted = false; }
                continue;
            };
            let address = match &record.identity {
                RecordIdentity::Pool { address, .. } => *address,
                RecordIdentity::Mint { .. } => continue,
            };
            if asc {
                if self.asc_last.is_some_and(|last| value < last) {
                    self.ascending_sorted = false;
                }
                self.asc_last = Some(value);
                if self.asc_seen.insert(address) && self.desc_seen.contains(&address) {
                    self.common_addresses += 1;
                }
            } else {
                if self.desc_last.is_some_and(|last| value > last) {
                    self.descending_sorted = false;
                }
                self.desc_last = Some(value);
                if self.desc_seen.insert(address) && self.asc_seen.contains(&address) {
                    self.common_addresses += 1;
                }
            }
        }
        // Strictly crossing TVL frontiers avoids premature exit within equal-TVL ties.
        // A shared pool-ID guard excludes overlap inferred solely from drifting values.
        // The API does not promise an immutable cross-cursor snapshot: this is best effort.
        self.crossed = self.ascending_sorted && self.descending_sorted
            && self.asc_last.zip(self.desc_last).is_some_and(|(a,d)| a > d)
            && self.common_addresses >= OVERLAP_CONFIRM_ADDRESSES;
    }
}

async fn route_pass(
    client: &reqwest::Client, gate: &Mutex<Instant>, spacing: Duration,
    output: &broadcast::Sender<Arc<DataBatch>>, settings: Settings,
    sort: &'static str, direction: &'static str,
    overlap: Option<&Arc<Mutex<LiquidityOverlap>>>,
) -> Result<()> {
    let started = Instant::now();
    let mut cursor: Option<String> = None;
    let mut page_no = 0_u32;
    let mut received = 0_usize;
    let mut accepted = 0_usize;
    let mut rejected = 0_usize;
    let mut rejected_unknown = 0_usize;
    let mut stopped_on_overlap = false;
    let mut consecutive_failures=0_u32;
    loop {
        if let Some(overlap) = overlap {
            if overlap.lock().await.crossed {
                stopped_on_overlap = true;
                break;
            }
        }
        let page = match fetch_page(client, gate, spacing, sort, direction, cursor.as_deref()).await {
            Ok(page)=>{consecutive_failures=0;page},
            Err(error)=>{
                if settings.once {return Err(error);}
                consecutive_failures=consecutive_failures.saturating_add(1);
                let backoff=retry_delay(consecutive_failures.min(4));
                crate::log_event!(Warn,
                    "raydium route_paused sort={sort} direction={direction} cursor_present={} pages={page_no} consecutive_failures={consecutive_failures} retry_s={} error={error:#}",
                    cursor.is_some(),backoff.as_secs());
                sleep(backoff).await;
                // Crucially, do not overwrite cursor or partial coverage on timeout.
                continue;
            }
        };
        let next = page.next.filter(|v| !v.is_empty());
        ensure!(!page.data.is_empty() || next.is_none(),
            "Raydium {sort}/{direction} empty page with cursor");
        if let Some(value) = &next {
            ensure!(cursor.as_ref() != Some(value),
                "Raydium {sort}/{direction} repeated cursor");
        }
        page_no += 1;
        let published = publish(output, page.data, sort, direction, settings.enabled_venues)?;
        received += published.received;
        accepted += published.accepted;
        rejected += published.rejected;
        rejected_unknown += published.rejected_unknown;
        if let (Some(overlap), Some(batch)) = (overlap, published.batch.as_ref()) {
            let mut coverage = overlap.lock().await;
            coverage.observe(direction, batch);
            if coverage.crossed {
                stopped_on_overlap = true;
            }
        }
        if page_no == 1 || page_no % 10 == 0 {
            crate::log_event!(Info,"raydium route sort={sort} direction={direction} pages={page_no} received={received} accepted={accepted} rejected={rejected} rejected_unknown_program={rejected_unknown} elapsed_ms={}",started.elapsed().as_millis());
        }
        cursor = next;
        if stopped_on_overlap || cursor.is_none()
            || settings.max_pages.is_some_and(|max| page_no >= max) { break; }
    }
    crate::log_event!(Info,"raydium route complete sort={sort} direction={direction} pages={page_no} received={received} accepted={accepted} rejected={rejected} rejected_unknown_program={rejected_unknown} early_overlap={stopped_on_overlap} elapsed_ms={}",started.elapsed().as_millis());
    Ok(())
}

/// The two liquidity directions may restart together if their shared overlap
/// state is corrupted; all other lanes (including targeted IDs) stay alive.
async fn liquidity_pair(
    client: reqwest::Client, gate: Arc<Mutex<Instant>>, spacing: Duration,
    output: broadcast::Sender<Arc<DataBatch>>, settings: Settings,
) -> Result<()> {
    let mut pass = 0_u64;
    loop {
        pass += 1;
        let overlap = Arc::new(Mutex::new(LiquidityOverlap::new()));
        let result=tokio::try_join!(
            route_pass(&client, &gate, spacing, &output, settings,
                "liquidity", "asc", Some(&overlap)),
            route_pass(&client, &gate, spacing, &output, settings,
                "liquidity", "desc", Some(&overlap)),
        );
        if let Err(error)=result {
            if settings.once{return Err(error);}
            crate::log_event!(Warn,
                "raydium liquidity_pair local_restart pass={pass} backoff_s={} error={error:#}",
                ROUTE_FAILURE_BACKOFF.as_secs());
            sleep(ROUTE_FAILURE_BACKOFF).await;
            continue;
        }
        let state = overlap.lock().await;
        crate::log_event!(Info,"raydium liquidity_pair pass={pass} overlapped={} common_addresses={} asc_last_tvl={:?} desc_last_tvl={:?} asc_sorted={} desc_sorted={}",
            state.crossed, state.common_addresses, state.asc_last, state.desc_last,
            state.ascending_sorted, state.descending_sorted);
        drop(state);
        if settings.once { return Ok(()); }
    }
}
async fn ordinary_lane(
    client: reqwest::Client, gate: Arc<Mutex<Instant>>, spacing: Duration,
    output: broadcast::Sender<Arc<DataBatch>>, settings: Settings,
    sort: &'static str, direction: &'static str,
) -> Result<()> {
    loop {
        match route_pass(&client, &gate, spacing, &output, settings, sort, direction, None).await {
            Ok(())=>{},
            Err(error)=>{
                if settings.once{return Err(error);}
                crate::log_event!(Warn,
                    "raydium route local_restart sort={sort} direction={direction} backoff_s={} error={error:#}",
                    ROUTE_FAILURE_BACKOFF.as_secs());
                sleep(ROUTE_FAILURE_BACKOFF).await;
            }
        }
        if settings.once { return Ok(()); }
    }
}

/// Every route refreshes its own first page independently. One failure never
/// tears down any other route or the targeted missing-TVL loop.
async fn refresh_head(
    client: reqwest::Client, gate: Arc<Mutex<Instant>>, spacing: Duration,
    output: broadcast::Sender<Arc<DataBatch>>, interval: Duration,
    sort: &'static str, direction: &'static str, enabled: crate::core::EnabledVenues,
) -> Result<()> {
    loop {
        sleep(interval).await;
        let started = Instant::now();
        match fetch_page(&client,&gate,spacing,sort,direction,None).await {
            Ok(page)=>{
                let decoded=publish(&output,page.data,sort,direction,enabled)?;
                crate::log_event!(Debug,
                    "raydium head_refresh sort={sort} direction={direction} received={} accepted={} rejected={} elapsed_ms={}",
                    decoded.received,decoded.accepted,decoded.rejected,started.elapsed().as_millis());
            },
            Err(error)=>crate::log_event!(Warn,
                "raydium head_refresh_error sort={sort} direction={direction} elapsed_ms={} error={error:#}",
                started.elapsed().as_millis()),
        }
    }
}

/// Enrich existing mint-qualified Raydium pools directly by known pool IDs.
/// The same request gate is shared with the global discovery and head refresh.
/// Missing/partial API results are retried later, never synthesized as zero TVL.
async fn target_missing_tvl(
    client:reqwest::Client,gate:Arc<Mutex<Instant>>,spacing:Duration,
    output:broadcast::Sender<Arc<DataBatch>>,registry:PoolRegistry,
    enabled:crate::core::EnabledVenues,
)->Result<()> {
    let mut attempted=HashMap::<Pubkey,Instant>::new();
    let mut cycle=0_u64;
    loop {
        cycle+=1;
        let now=Instant::now();
        let mut selected=registry.raydium_missing_tvl_targets(usize::MAX).into_iter()
            .filter(|id|attempted.get(id).is_none_or(|at|now.duration_since(*at)>=TARGET_RETRY_DELAY))
            .take(TARGET_POOLS_PER_CYCLE).collect::<Vec<_>>();
        let candidates=selected.len();
        let mut accepted=0_usize;
        let mut resolved=0_usize;
        let mut failures=0_usize;
        // Small batches avoid request-URL size and Raydium endpoint limits.
        for chunk in selected.chunks(TARGET_POOL_BATCH) {
            let ids=chunk.iter().map(ToString::to_string).collect::<Vec<_>>().join(",");
            let requested=chunk.iter().copied().collect::<HashSet<_>>();
            let now=Instant::now();
            for id in chunk {attempted.insert(*id,now);}
            wait_slot(&gate,spacing).await;
            let response=client.get("https://api-v3.raydium.io/pools/info/ids")
                .query(&[("ids",ids)]).send().await;
            let result=async {
                let response=response.context("Raydium targeted ID request transport")?;
                if response.status()==reqwest::StatusCode::TOO_MANY_REQUESTS {
                    cool_down(&gate,Duration::from_secs(5)).await;
                    bail!("Raydium targeted ID HTTP 429");
                }
                ensure!(response.status().is_success(),
                    "Raydium targeted ID HTTP {}",response.status());
                let bytes=response.bytes().await.context("Raydium targeted ID response body")?;
                let body:IdsResponse=serde_json::from_slice(&bytes)
                    .context("Raydium targeted ID JSON envelope")?;
                ensure!(body.success,"Raydium targeted ID returned success=false");
                let payloads=body.data.context("Raydium targeted ID response missing data")?;
                let mut verified=Vec::new();
                for item in payloads {
                    // A provider response cannot create unrelated discovery identities.
                    if let Some(id)=item.get("id").and_then(Value::as_str)
                        .and_then(|v|v.parse::<Pubkey>().ok()) {
                        if requested.contains(&id) {verified.push(item);}
                    }
                }
                let count=verified.len();
                let published=publish(&output,verified,"targeted","ids",enabled)?;
                Ok::<(usize,usize),anyhow::Error>((count,published.accepted))
            }.await;
            match result {
                Ok((found,pub_count))=>{resolved+=found;accepted+=pub_count;},
                Err(error)=>{
                    failures+=1;
                    if failures<=3 {
                        crate::log_event!(Warn,"raydium targeted_tvl cycle={cycle} error={error:#}");
                    }
                }
            }
        }
        crate::log_event!(Info,
            "raydium targeted_tvl cycle={cycle} attempted={candidates} found={resolved} published={accepted} failed_batches={failures} tracked_cooldowns={} retry_s={}",
            attempted.len(),TARGET_RETRY_DELAY.as_secs());
        attempted.retain(|_,at|now.duration_since(*at)<TARGET_RETRY_DELAY);
        selected.clear();
        sleep(TARGET_SCAN_INTERVAL).await;
    }
}

pub async fn run(
    client: reqwest::Client, output: broadcast::Sender<Arc<DataBatch>>,
    settings: Settings, registry:Option<PoolRegistry>,
) -> Result<()> {
    ensure!(settings.rps > 0 && settings.rps <= 100, "Raydium RPS must be 1..=100");
    let refresh_seconds: u64 = env::var("RAYDIUM_HEAD_REFRESH_SECONDS")
        .unwrap_or_else(|_| "30".to_owned()).parse()
        .context("invalid RAYDIUM_HEAD_REFRESH_SECONDS")?;
    ensure!((5..=3600).contains(&refresh_seconds),
        "RAYDIUM_HEAD_REFRESH_SECONDS must be 5..=3600");
    let spacing = Duration::from_secs_f64(1.0/f64::from(settings.rps));
    let gate = Arc::new(Mutex::new(Instant::now()));
    crate::log_event!(Info,"raydium discovery routes=10 head_refresh_seconds={refresh_seconds} liquidity_overlap_min_common={OVERLAP_CONFIRM_ADDRESSES} rps={}",settings.rps);

    // Owned tasks: cancelling discovery must also stop every head refresh and
    // release its source sender so Registry and persistence can finish draining.
    let mut refresh_tasks = JoinSet::new();
    for (sort, direction) in ROUTES.into_iter().filter(|_|!settings.once) {
        refresh_tasks.spawn(refresh_head(
            client.clone(), gate.clone(), spacing, output.clone(),
            Duration::from_secs(refresh_seconds), sort, direction, settings.enabled_venues,
        ));
    }
    let mut tasks = JoinSet::new();
    if !settings.once {
        if let Some(registry)=registry {
            tasks.spawn(target_missing_tvl(client.clone(),gate.clone(),spacing,
                output.clone(),registry,settings.enabled_venues));
        }
    }
    for (sort, direction) in ROUTES {
        if sort == "liquidity" { continue; }
        let client = client.clone();
        let gate = gate.clone();
        let output = output.clone();
        tasks.spawn(ordinary_lane(client,gate,spacing,output,settings,sort,direction));
    }
    tasks.spawn(liquidity_pair(client,gate,spacing,output,settings));
    while !tasks.is_empty() {
        tokio::select! {
            joined=tasks.join_next()=>{
                joined.context("missing Raydium route task")?
                    .context("Raydium route task panicked")??;
            },
            joined=refresh_tasks.join_next(),if !refresh_tasks.is_empty()=>{
                joined.context("missing Raydium head task")?
                    .context("Raydium head task panicked")??;
                bail!("Raydium head refresh ended unexpectedly");
            },
        }
    }
    refresh_tasks.shutdown().await;
    if !settings.once { bail!("all Raydium API routes stopped"); }
    Ok(())
}
