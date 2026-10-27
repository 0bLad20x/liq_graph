//! PancakeSwap Solana CLMM REST discovery; preserve every provider field in DataRecord.payload.
//! The source emits the same pool identity and optional source TVL as other REST workers.
use std::{collections::{BTreeMap, HashSet}, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{
    sync::{broadcast, Mutex},
    task::JoinSet,
    time::{sleep, sleep_until, Instant},
};

use crate::core::{now_unix_ms, DataBatch, DataRecord, RecordIdentity, Venue};

const URL: &str = "https://sol-explorer.pancakeswap.com/api/cached/v1/pools/info/list";
const PROGRAM: &str = "HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq";
const SOURCE: &str = "pancakeswap_api";
const PAGE_SIZE: usize = 100;
const MAX_IN_FLIGHT: usize = 4;
const RETRY_DELAY: Duration = Duration::from_secs(2);

#[derive(Clone, Copy)]
pub struct Settings {
    pub once: bool,
    pub max_pages: Option<u32>,
    pub rpm: u32,
}

#[derive(Deserialize)]
struct Response {
    data: Vec<Value>,
}

fn key(pool: &Value, path: &str) -> Result<Pubkey> {
    let raw = pool.pointer(path).and_then(Value::as_str)
        .with_context(|| format!("PancakeSwap missing {path}"))?;
    let key: Pubkey = raw.parse()
        .with_context(|| format!("PancakeSwap invalid {path}"))?;
    ensure!(key != Pubkey::default(), "PancakeSwap zero {path}");
    Ok(key)
}

// PancakeSwap reports 'tvl' in its pool listing. Retain its raw number in
// payload regardless; a source numeric observation is not an on-chain quote.
fn tvl(pool: &Value) -> Option<f64> {
    let raw = pool.get("tvl")?;
    let value = raw.as_f64().or_else(|| raw.as_str()?.parse::<f64>().ok())?;
    (value.is_finite() && value >= 0.0).then_some(value)
}

fn decode(payload: Value) -> Result<DataRecord> {
    ensure!(payload.get("programId").and_then(Value::as_str) == Some(PROGRAM),
        "not a PancakeSwap Solana CLMM pool");
    let address = key(&payload, "/id")?;
    let mint_a = key(&payload, "/mintA/address")?;
    let mint_b = key(&payload, "/mintB/address")?;
    ensure!(mint_a != mint_b, "PancakeSwap identical mints");
    Ok(DataRecord {
        identity: RecordIdentity::Pool {
            address,
            venue: Venue::PancakeSwapClmm,
            mint_a: Some(mint_a),
            mint_b: Some(mint_b),
        },
        tvl_usd: tvl(&payload),
        payload,
    })
}

// Book request slots before starting I/O. Responses may overlap while the
// shared gate bounds the source to at most rpm request starts per minute.
async fn request_slot(gate: &Mutex<Instant>, spacing: Duration) {
    let mut next = gate.lock().await;
    let slot = (*next).max(Instant::now());
    *next = slot + spacing;
    drop(next);
    sleep_until(slot).await;
}

async fn cooldown(gate: &Mutex<Instant>, delay: Duration) {
    let mut next = gate.lock().await;
    *next = (*next).max(Instant::now() + delay);
}

async fn fetch_page(
    client: &reqwest::Client,
    gate: &Mutex<Instant>,
    spacing: Duration,
    page: u32,
) -> Result<Vec<Value>> {
    loop {
        request_slot(gate, spacing).await;
        let page_text = page.to_string();
        let response = match client.get(URL).query(&[
            ("poolType", "concentrated"),
            ("poolSortField", "default"),
            ("order", "desc"),
            ("pageSize", "100"),
            ("page", page_text.as_str()),
        ]).send().await {
            Ok(response) => response,
            Err(error) => {
                crate::log_event!(Warn,"pancakeswap page={page} request_error={error:#}");
                sleep(RETRY_DELAY).await;
                continue;
            }
        };

        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_seconds = response.headers().get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(10);
            let delay = Duration::from_secs(retry_seconds.max(1));
            cooldown(gate, delay).await;
            crate::log_event!(Debug,"pancakeswap page={page} http_429 cooldown_s={retry_seconds}");
            continue;
        }
        if status.is_client_error() {
            bail!("PancakeSwap page={page} HTTP {status}");
        }
        if !status.is_success() {
            crate::log_event!(Warn,"pancakeswap page={page} http_error={status}");
            sleep(RETRY_DELAY).await;
            continue;
        }
        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                crate::log_event!(Warn,"pancakeswap page={page} body_error={error:#}");
                sleep(RETRY_DELAY).await;
                continue;
            }
        };
        let envelope: Response = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(error) => {
                crate::log_event!(Warn,"pancakeswap page={page} json_error={error:#}");
                sleep(RETRY_DELAY).await;
                continue;
            }
        };
        ensure!(envelope.data.len() <= PAGE_SIZE,
            "PancakeSwap page={page} returned more than {PAGE_SIZE} records");
        return Ok(envelope.data);
    }
}

pub async fn run(
    client: reqwest::Client,
    output: broadcast::Sender<Arc<DataBatch>>,
    settings: Settings,
) -> Result<()> {
    ensure!((1..=60).contains(&settings.rpm), "PANCAKESWAP_API_RPM must be 1..=60");
    let spacing = Duration::from_secs_f64(60.0 / f64::from(settings.rpm));
    let gate = Arc::new(Mutex::new(Instant::now()));
    let mut pass = 0_u64;

    loop {
        pass += 1;
        let started = Instant::now();
        let mut next_request = 1_u32;
        let mut next_publish = 1_u32;
        let mut jobs = JoinSet::<Result<(u32, Vec<Value>)>>::new();
        let mut ready = BTreeMap::<u32, Vec<Value>>::new();
        let mut seen = HashSet::<Pubkey>::new();
        let mut pages = 0_u32;
        let mut received = 0_usize;
        let mut accepted = 0_usize;
        let mut duplicates = 0_usize;
        let mut rejected = 0_usize;
        let mut last_empty_page = None;

        loop {
            while jobs.len() < MAX_IN_FLIGHT
                // Do not run arbitrarily far ahead of a slow or missing page.
                && u64::from(next_request) < u64::from(next_publish) + MAX_IN_FLIGHT as u64
                && settings.max_pages.is_none_or(|max| next_request <= max)
            {
                let page = next_request;
                next_request = next_request.checked_add(1)
                    .context("PancakeSwap page index overflow")?;
                let http = client.clone();
                let throttle = gate.clone();
                jobs.spawn(async move {
                    let data = fetch_page(&http, &throttle, spacing, page).await?;
                    Ok((page, data))
                });
            }
            if jobs.is_empty() {
                break;
            }
            let (page, data) = jobs.join_next().await
                .context("PancakeSwap page task missing")?
                .context("PancakeSwap page task failed")??;
            ensure!(ready.insert(page, data).is_none(),
                "PancakeSwap duplicate page response {page}");

            while let Some(pools) = ready.remove(&next_publish) {
                if pools.is_empty() {
                    last_empty_page = Some(next_publish);
                    break;
                }
                pages += 1;
                received += pools.len();
                let mut records = Vec::with_capacity(pools.len());
                for payload in pools {
                    match decode(payload) {
                        Ok(record) => {
                            let RecordIdentity::Pool { address, .. } = &record.identity else {
                                unreachable!("PancakeSwap emits pools only");
                            };
                            if seen.insert(*address) {
                                records.push(record);
                            } else {
                                duplicates += 1;
                            }
                        }
                        Err(error) => {
                            rejected += 1;
                            if rejected <= 5 {
                                crate::log_event!(Warn,"pancakeswap page={next_publish} rejected_pool={error:#}");
                            }
                        }
                    }
                }
                accepted += records.len();
                if !records.is_empty() {
                    output.send(Arc::new(DataBatch {
                        source: SOURCE,
                        received_at_ms: now_unix_ms()?,
                        records,
                    })).map_err(|_| anyhow!("PancakeSwap source has no consumer"))?;
                }
                next_publish = next_publish.checked_add(1)
                    .context("PancakeSwap publish page overflow")?;
            }
            if last_empty_page.is_some() {
                jobs.abort_all();
                while jobs.join_next().await.is_some() {}
                break;
            }
        }
        crate::log_event!(Info,
            "pancakeswap pass={pass} pages={pages} first_empty_page={last_empty_page:?} received={received} unique={accepted} duplicates={duplicates} rejected={rejected} rpm={} elapsed_ms={}",
            settings.rpm, started.elapsed().as_millis()
        );
        if settings.once {
            return Ok(());
        }
    }
}
