//! Meteora DLMM API source: fetch, paginate, decode once and broadcast complete pool records.
//! No market-state deduplication, selection, persistence or consumer-specific processing.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{
    sync::{Mutex, broadcast},
    task::JoinSet,
    time::{Instant, sleep, sleep_until},
};

use crate::core::{DataBatch, DataRecord, RecordIdentity, Venue, now_unix_ms};

const URL: &str = "https://dlmm.datapi.meteora.ag/pools";
const SOURCE: &str = "meteora_dlmm_api";
const PAGE_SIZE: u32 = 1000;
const MAX_IN_FLIGHT: usize = 8;
const MIN_REQUEST_SPACING: Duration = Duration::from_millis(34);
const RETRY_DELAY: Duration = Duration::from_secs(2);

pub struct Settings {
    pub once: bool,
    pub max_pages: Option<u32>,
}

#[derive(Deserialize)]
struct PageEnvelope {
    current_page: u32,
    pages: u32,
    data: Vec<Value>,
}

struct Page {
    pages: u32,
    rejected: usize,
    batch: DataBatch,
}

fn optional_mint(pool: &Value, pointer: &str) -> Option<Pubkey> {
    pool.pointer(pointer)
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
}

fn decode_pool(payload: Value) -> Result<DataRecord> {
    let address: Pubkey = payload.pointer("/address")
        .and_then(Value::as_str)
        .context("DLMM pool missing address")?
        .parse()
        .context("invalid DLMM pool address")?;
    ensure!(address != Pubkey::default(), "zero DLMM pool address");
    let mint_a = optional_mint(&payload, "/token_x/address");
    let mint_b = optional_mint(&payload, "/token_y/address");
    Ok(DataRecord {
        identity: RecordIdentity::Pool {
            address, venue: Venue::MeteoraDlmm, mint_a, mint_b,
        },
        tvl_usd: payload.get("tvl")
            .and_then(|v| v.as_f64().or_else(||v.as_str()?.parse::<f64>().ok()))
            .filter(|v|v.is_finite() && *v>=0.0),
        payload,
    })
}

fn decode_page(body: &[u8], page: u32) -> Result<Page> {
    let envelope: PageEnvelope = serde_json::from_slice(body)
        .with_context(|| format!("invalid Meteora page {page} JSON envelope"))?;
    ensure!(envelope.current_page == page, "Meteora page number mismatch");
    ensure!(
        envelope.pages >= page || (page == 1 && envelope.pages == 0 && envelope.data.is_empty()),
        "Meteora page count inconsistent"
    );
    ensure!(
        !envelope.data.is_empty() || (page == 1 && envelope.pages == 0),
        "Meteora page unexpectedly empty"
    );
    ensure!(envelope.data.len() <= PAGE_SIZE as usize, "Meteora page size exceeded");

    let mut records = Vec::with_capacity(envelope.data.len());
    let mut rejected = 0_usize;
    for pool in envelope.data {
        match decode_pool(pool) {
            Ok(record) => records.push(record),
            Err(error) => {
                rejected += 1;
                eprintln!("meteora page={page} rejected_pool error={error:#}");
            }
        }
    }
    ensure!(!records.is_empty() || envelope.pages == 0,
        "Meteora page contained no identifiable pools");
    Ok(Page {
        pages: envelope.pages.max(1),
        rejected,
        batch: DataBatch {
            source: SOURCE,
            received_at_ms: now_unix_ms()?,
            records,
        },
    })
}

async fn fetch_page(client: &reqwest::Client, page: u32) -> Result<Page> {
    let response = client.get(URL)
        .query(&[
            ("page", page.to_string()),
            ("page_size", PAGE_SIZE.to_string()),
            ("sort_by", "pool_created_at:desc".to_owned()),
        ])
        .send().await?
        .error_for_status()?;
    let body = response.bytes().await?;
    decode_page(&body, page)
}

/// One request-start limiter shared by concurrent pages and retries.
async fn fetch_with_retry(
    client: reqwest::Client,
    gate: Arc<Mutex<Instant>>,
    page: u32,
) -> Page {
    loop {
        let mut next = gate.lock().await;
        sleep_until(*next).await;
        *next = Instant::now() + MIN_REQUEST_SPACING;
        drop(next);

        match fetch_page(&client, page).await {
            Ok(result) => return result,
            Err(error) => {
                eprintln!("meteora page={page} retry error={error:#}");
                sleep(RETRY_DELAY).await;
            }
        }
    }
}

/// One decoded response is one broadcast message.
fn publish(output: &broadcast::Sender<Arc<DataBatch>>, batch: DataBatch) -> bool {
    output.send(Arc::new(batch)).is_ok()
}

pub async fn run(
    client: reqwest::Client,
    output: broadcast::Sender<Arc<DataBatch>>,
    settings: Settings,
) -> Result<()> {
    let gate = Arc::new(Mutex::new(Instant::now()));

    loop {
        let started = Instant::now();
        let first = fetch_with_retry(client.clone(), gate.clone(), 1).await;
        let total_pages = first.pages;
        let pages = settings.max_pages.unwrap_or(total_pages).min(total_pages);
        let mut records = first.batch.records.len();
        let mut rejected = first.rejected;
        let mut unconsumed_batches = usize::from(!publish(&output, first.batch));
        let mut scanned = 1_u32;

        let mut pending = JoinSet::new();
        let mut next_page = 2_u32;

        while next_page <= pages || !pending.is_empty() {
            while next_page <= pages && pending.len() < MAX_IN_FLIGHT {
                let page = next_page;
                let http = client.clone();
                let limiter = gate.clone();
                pending.spawn(async move { fetch_with_retry(http, limiter, page).await });
                next_page += 1;
            }
            if let Some(task) = pending.join_next().await {
                let page = task.context("Meteora page task stopped unexpectedly")?;
                records += page.batch.records.len();
                rejected += page.rejected;
                unconsumed_batches += usize::from(!publish(&output, page.batch));
                scanned += 1;
            }
        }

        eprintln!(
            "meteora source pages={scanned}/{total_pages} decoded_pools={records} rejected_pools={rejected} unconsumed_batches={unconsumed_batches} elapsed_ms={}",
            started.elapsed().as_millis()
        );
        if settings.once {
            return Ok(());
        }
    }
}
