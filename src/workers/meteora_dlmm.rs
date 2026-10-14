//! Complete, self-contained Meteora DLMM API worker.
//! Input: HTTP. Processing: fetch, parse, validate, deduplicate.
//! Output: typed MarketIdentity channel. No SQL, graph or other workers.

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinSet,
    time::{Instant, sleep, sleep_until},
};

use crate::core::MarketIdentity;

const URL: &str = "https://dlmm.datapi.meteora.ag/pools";
const PAGE_SIZE: u32 = 1000;
const MAX_IN_FLIGHT: usize = 8;
const MIN_REQUEST_SPACING: Duration = Duration::from_millis(34);
const RETRY_DELAY: Duration = Duration::from_secs(2);
const MAX_ATTEMPTS: u32 = 5;
const RESTART_DELAY: Duration = Duration::from_secs(30);
const VENUE: &str = "meteora_dlmm";

pub struct Settings {
    pub once: bool,
    pub max_pages: Option<u32>,
}

#[derive(Deserialize)]
struct Page {
    current_page: u32,
    pages: u32,
    data: Vec<Value>,
}

fn key(pool: &Value, pointer: &str) -> Result<Pubkey> {
    pool.pointer(pointer)
        .and_then(Value::as_str)
        .with_context(|| format!("missing pubkey at {pointer}"))?
        .parse()
        .with_context(|| format!("invalid pubkey at {pointer}"))
}

fn decode(pool: &Value) -> Result<MarketIdentity> {
    let result = MarketIdentity {
        address: key(pool, "/address")?,
        venue: VENUE,
        mint_a: key(pool, "/token_x/address")?,
        mint_b: key(pool, "/token_y/address")?,
    };
    ensure!(
        result.address != Pubkey::default()
            && result.mint_a != Pubkey::default()
            && result.mint_b != Pubkey::default()
            && result.mint_a != result.mint_b,
        "invalid Meteora market identity"
    );
    Ok(result)
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
    let result: Page = response.json().await
        .with_context(|| format!("invalid Meteora page {page} JSON envelope"))?;
    ensure!(result.current_page == page, "Meteora page number mismatch");
    ensure!(
        result.pages >= page || (page == 1 && result.pages == 0 && result.data.is_empty()),
        "Meteora page count inconsistent"
    );
    ensure!(
        !result.data.is_empty() || (page == 1 && result.pages == 0),
        "Meteora page unexpectedly empty"
    );
    ensure!(result.data.len() <= PAGE_SIZE as usize, "Meteora page size exceeded");
    Ok(result)
}

/// All concurrent tasks share one request-start limiter.
async fn fetch_with_retry(
    client: reqwest::Client,
    gate: Arc<Mutex<Instant>>,
    page: u32,
) -> Result<Page> {
    for attempt in 1..=MAX_ATTEMPTS {
        let mut next = gate.lock().await;
        sleep_until(*next).await;
        *next = Instant::now() + MIN_REQUEST_SPACING;
        drop(next);

        match fetch_page(&client, page).await {
            Ok(result) => return Ok(result),
            Err(error) if attempt == MAX_ATTEMPTS => {
                return Err(error).with_context(|| format!(
                    "Meteora page {page} failed after {MAX_ATTEMPTS} attempts"
                ));
            }
            Err(error) => {
                eprintln!("meteora page={page} retry={attempt} error={error:#}");
                sleep(RETRY_DELAY).await;
            }
        }
    }
    bail!("Meteora retry loop ended unexpectedly")
}

async fn emit(
    records: Vec<Value>,
    known: &mut HashMap<Pubkey, MarketIdentity>,
    output: &mpsc::Sender<MarketIdentity>,
) -> Result<(usize, usize)> {
    let mut new = 0;
    let mut rejected = 0;

    for pool in records {
        match decode(&pool) {
            Ok(identity) => {
                match known.get(&identity.address) {
                    Some(previous) if previous != &identity => {
                        bail!("conflicting Meteora market identity: {}", identity.address);
                    }
                    Some(_) => continue,
                    None => {}
                }
                output.send(identity).await
                    .map_err(|_| anyhow::anyhow!("MarketIdentity output consumer closed"))?;
                known.insert(identity.address, identity);
                new += 1;
            }
            Err(error) => {
                rejected += 1;
                if rejected <= 3 {
                    eprintln!("meteora rejected identity: {error:#}");
                }
            }
        }
    }
    Ok((new, rejected))
}

/// One runnable worker. Backpressure is bounded and lossless; no raw-page bridge.
pub async fn run(
    client: reqwest::Client,
    output: mpsc::Sender<MarketIdentity>,
    settings: Settings,
) -> Result<()> {
    let gate = Arc::new(Mutex::new(Instant::now()));
    let mut known: HashMap<Pubkey, MarketIdentity> = HashMap::new();

    loop {
        let started = Instant::now();
        let first = fetch_with_retry(client.clone(), gate.clone(), 1).await?;
        let total_pages = first.pages.max(1);
        let pages = settings.max_pages.unwrap_or(total_pages).min(total_pages);
        let (mut fresh, mut rejected) = emit(first.data, &mut known, &output).await?;
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
                let page = task.context("Meteora page task stopped unexpectedly")??;
                let (new, invalid) = emit(page.data, &mut known, &output).await?;
                fresh += new;
                rejected += invalid;
                scanned += 1;
            }
        }

        eprintln!(
            "meteora discovery pages={scanned}/{total_pages} new={fresh} rejected={rejected} known={} elapsed_ms={}",
            known.len(), started.elapsed().as_millis()
        );
        if settings.once {
            return Ok(());
        }
        sleep(RESTART_DELAY).await;
    }
}
