//! Raydium REST discovery: ten cursor routes, shared rate limit and early head refresh.
//! ASC/DESC liquidity passes may end early only after strictly crossed TVL frontiers
//! with observed address overlap. Ties or non-monotonic responses disable early exit.
use std::{collections::HashSet, env, sync::Arc, time::Duration};
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, Mutex}, task::JoinSet, time::{Instant, sleep, sleep_until}};
use crate::core::{DataBatch, DataRecord, RecordIdentity, Venue, now_unix_ms};

const URL: &str = "https://api-v3.raydium.io/pools/info/list-v2";
const SOURCE: &str = "raydium_api";
const PAGE_SIZE: usize = 1000;
const OVERLAP_CONFIRM_ADDRESSES: usize = 32;
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
pub struct Settings { pub once: bool, pub max_pages: Option<u32>, pub rps: u32 }

#[derive(Deserialize)]
struct Response { success: bool, data: Option<Page> }
#[derive(Deserialize)]
struct Page {
    data: Vec<Value>,
    #[serde(rename="nextPageId")]
    next: Option<String>,
}

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
async fn fetch_page(
    client: &reqwest::Client, gate: &Mutex<Instant>, spacing: Duration,
    sort: &str, direction: &str, cursor: Option<&str>,
) -> Result<Page> {
    loop {
        wait_slot(gate, spacing).await;
        let mut request = client.get(URL).query(&[
            ("sortField",sort), ("sortType",direction), ("size","1000"),
        ]);
        if let Some(next) = cursor { request = request.query(&[("nextPageId",next)]); }
        let response = match request.send().await {
            Ok(value) => value,
            Err(error) => {
                eprintln!("raydium sort={sort} direction={direction} request_error={error:#}");
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let seconds = response.headers().get(reqwest::header::RETRY_AFTER)
                .and_then(|v|v.to_str().ok()).and_then(|v|v.parse::<u64>().ok()).unwrap_or(5);
            cool_down(gate, Duration::from_secs(seconds)).await;
            eprintln!("raydium sort={sort} direction={direction} http_429 cooldown_s={seconds}");
            continue;
        }
        if response.status().is_client_error() {
            bail!("Raydium {sort}/{direction} HTTP {}", response.status());
        }
        if !response.status().is_success() {
            eprintln!("raydium sort={sort} direction={direction} http_error={}",response.status());
            sleep(Duration::from_secs(2)).await;
            continue;
        }
        let bytes = response.bytes().await.context("read Raydium page bytes")?;
        let envelope: Response = serde_json::from_slice(&bytes).context("decode Raydium envelope")?;
        if !envelope.success {
            eprintln!("raydium sort={sort} direction={direction} success=false retrying_cursor");
            sleep(Duration::from_secs(2)).await;
            continue;
        }
        let page = envelope.data.context("Raydium response missing data")?;
        ensure!(page.data.len() <= PAGE_SIZE, "Raydium returned oversized page");
        ensure!(!page.data.is_empty() || page.next.as_deref().is_none_or(str::is_empty),
            "Raydium empty page with cursor");
        return Ok(page);
    }
}

/// Decode the full provider objects once and publish the existing DataBatch contract.
fn publish(
    output: &broadcast::Sender<Arc<DataBatch>>, payloads: Vec<Value>,
    sort: &str, direction: &str,
) -> Result<Published> {
    let received = payloads.len();
    let mut records = Vec::with_capacity(received);
    let mut rejected = 0;
    let mut rejected_unknown = 0;
    for payload in payloads {
        match decode(payload) {
            Ok(record) => records.push(record),
            Err(error) => {
                rejected += 1;
                if error.to_string().contains("unknown Raydium programId") { rejected_unknown += 1; }
                if rejected <= 5 {
                    eprintln!("raydium sort={sort} direction={direction} decode_rejected error={error:#}");
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
    loop {
        if let Some(overlap) = overlap {
            if overlap.lock().await.crossed {
                stopped_on_overlap = true;
                break;
            }
        }
        let page = fetch_page(client, gate, spacing, sort, direction, cursor.as_deref()).await?;
        let next = page.next.filter(|v| !v.is_empty());
        ensure!(!page.data.is_empty() || next.is_none(),
            "Raydium {sort}/{direction} empty page with cursor");
        if let Some(value) = &next {
            ensure!(cursor.as_ref() != Some(value),
                "Raydium {sort}/{direction} repeated cursor");
        }
        page_no += 1;
        let published = publish(output, page.data, sort, direction)?;
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
            println!("raydium route sort={sort} direction={direction} pages={page_no} received={received} accepted={accepted} rejected={rejected} rejected_unknown_program={rejected_unknown} elapsed_ms={}",started.elapsed().as_millis());
        }
        cursor = next;
        if stopped_on_overlap || cursor.is_none()
            || settings.max_pages.is_some_and(|max| page_no >= max) { break; }
    }
    println!("raydium route complete sort={sort} direction={direction} pages={page_no} received={received} accepted={accepted} rejected={rejected} rejected_unknown_program={rejected_unknown} early_overlap={stopped_on_overlap} elapsed_ms={}",started.elapsed().as_millis());
    Ok(())
}

/// Only the two liquidity directions share overlap state; the other eight keep
/// their independent cursors and restart policies.
async fn liquidity_pair(
    client: reqwest::Client, gate: Arc<Mutex<Instant>>, spacing: Duration,
    output: broadcast::Sender<Arc<DataBatch>>, settings: Settings,
) -> Result<()> {
    let mut pass = 0_u64;
    loop {
        pass += 1;
        let overlap = Arc::new(Mutex::new(LiquidityOverlap::new()));
        tokio::try_join!(
            route_pass(&client, &gate, spacing, &output, settings,
                "liquidity", "asc", Some(&overlap)),
            route_pass(&client, &gate, spacing, &output, settings,
                "liquidity", "desc", Some(&overlap)),
        )?;
        let state = overlap.lock().await;
        println!("raydium liquidity_pair pass={pass} overlapped={} common_addresses={} asc_last_tvl={:?} desc_last_tvl={:?} asc_sorted={} desc_sorted={}",
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
        route_pass(&client, &gate, spacing, &output, settings, sort, direction, None).await?;
        if settings.once { return Ok(()); }
    }
}

/// Every route refreshes its own first page independently. A slow route cannot
/// delay refresh cycles on other routes. All requests use the shared RPS gate.
async fn refresh_head(
    client: reqwest::Client, gate: Arc<Mutex<Instant>>, spacing: Duration,
    output: broadcast::Sender<Arc<DataBatch>>, interval: Duration,
    sort: &'static str, direction: &'static str,
) {
    loop {
        sleep(interval).await;
        let started = Instant::now();
        match fetch_page(&client, &gate, spacing, sort, direction, None).await {
            Ok(page) => match publish(&output, page.data, sort, direction) {
                Ok(decoded) => println!(
                    "raydium head_refresh sort={sort} direction={direction} received={} accepted={} rejected={} elapsed_ms={}",
                    decoded.received, decoded.accepted, decoded.rejected,
                    started.elapsed().as_millis(),
                ),
                Err(error) => {
                    eprintln!("raydium head_refresh_stopped sort={sort} direction={direction} error={error:#}");
                    return;
                }
            },
            Err(error) => {
                eprintln!("raydium head_refresh_failed sort={sort} direction={direction} error={error:#}");
            }
        }
    }
}

pub async fn run(
    client: reqwest::Client, output: broadcast::Sender<Arc<DataBatch>>,
    settings: Settings,
) -> Result<()> {
    ensure!(settings.rps > 0 && settings.rps <= 100, "Raydium RPS must be 1..=100");
    let refresh_seconds: u64 = env::var("RAYDIUM_HEAD_REFRESH_SECONDS")
        .unwrap_or_else(|_| "30".to_owned()).parse()
        .context("invalid RAYDIUM_HEAD_REFRESH_SECONDS")?;
    ensure!((5..=3600).contains(&refresh_seconds),
        "RAYDIUM_HEAD_REFRESH_SECONDS must be 5..=3600");
    let spacing = Duration::from_secs_f64(1.0/f64::from(settings.rps));
    let gate = Arc::new(Mutex::new(Instant::now()));
    println!("raydium discovery routes=10 head_refresh_seconds={refresh_seconds} liquidity_overlap_min_common={OVERLAP_CONFIRM_ADDRESSES} rps={}",settings.rps);

    let mut refresh_tasks = Vec::with_capacity(ROUTES.len());
    for (sort, direction) in ROUTES {
        refresh_tasks.push(tokio::spawn(refresh_head(
            client.clone(), gate.clone(), spacing, output.clone(),
            Duration::from_secs(refresh_seconds), sort, direction,
        )));
    }
    let mut tasks = JoinSet::new();
    for (sort, direction) in ROUTES {
        if sort == "liquidity" { continue; }
        let client = client.clone();
        let gate = gate.clone();
        let output = output.clone();
        tasks.spawn(async move {
            let result = ordinary_lane(client, gate, spacing, output, settings, sort, direction).await;
            if !settings.once {
                if let Err(error) = &result {
                    eprintln!("raydium route_stopped sort={sort} direction={direction} error={error:#}");
                }
                Ok(())
            } else { result }
        });
    }
    tasks.spawn(async move {
        let result = liquidity_pair(client, gate, spacing, output, settings).await;
        if !settings.once {
            if let Err(error) = &result {
                eprintln!("raydium liquidity_pair_stopped error={error:#}");
            }
            Ok(())
        } else { result }
    });
    let mut first_error = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(())) => {},
            Ok(Err(error)) => { if first_error.is_none() { first_error = Some(error); } },
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(anyhow!("Raydium route task panicked: {error}"));
                }
            }
        }
    }
    for task in refresh_tasks { task.abort(); }
    if let Some(error) = first_error { return Err(error); }
    if !settings.once { bail!("all Raydium API routes stopped"); }
    Ok(())
}
