//! Meteora DLMM API source: fetch pages, extract pool address, forward complete raw records.
//! No field mapping, normalization, deduplication, or consumer-specific processing.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::value::RawValue;
use solana_pubkey::Pubkey;
use tokio::{
    sync::{Mutex, broadcast},
    task::JoinSet,
    time::{Instant, sleep, sleep_until},
};

use crate::core::{RecordKind, SourceRecord};

const URL: &str = "https://dlmm.datapi.meteora.ag/pools";
const SOURCE: &str = "meteora_dlmm_api";
const PAGE_SIZE: u32 = 1000;
const MAX_IN_FLIGHT: usize = 8;
const MIN_REQUEST_SPACING: Duration = Duration::from_millis(34);
const RETRY_DELAY: Duration = Duration::from_secs(2);
const MAX_ATTEMPTS: u32 = 5;
const RESTART_DELAY: Duration = Duration::from_secs(30);

pub struct Settings {
    pub once: bool,
    pub max_pages: Option<u32>,
}

#[derive(Deserialize)]
struct Page {
    current_page: u32,
    pages: u32,
    data: Vec<Box<RawValue>>,
}

#[derive(Deserialize)]
struct PoolAddress<'a> {
    address: &'a str,
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

/// Shared request-start limiter for all concurrent pages.
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

/// Broadcast never waits for a consumer. A slow consumer may lag and lose records.
fn publish(records: Vec<Box<RawValue>>, output: &broadcast::Sender<SourceRecord>) -> Result<(usize, usize)> {
    let count = records.len();
    let mut without_subscribers = 0;

    for raw in records {
        let pool: PoolAddress<'_> = serde_json::from_str(raw.get())
            .context("Meteora pool has no readable address")?;
        let address: Pubkey = pool.address.parse()
            .context("Meteora pool address is invalid")?;
        ensure!(address != Pubkey::default(), "Meteora pool address is zero");

        let message = SourceRecord {
            source: SOURCE,
            kind: RecordKind::Pool,
            address,
            payload: Arc::from(raw.get().as_bytes()),
        };
        if output.send(message).is_err() {
            without_subscribers += 1;
        }
    }
    Ok((count, without_subscribers))
}

pub async fn run(
    client: reqwest::Client,
    output: broadcast::Sender<SourceRecord>,
    settings: Settings,
) -> Result<()> {
    let gate = Arc::new(Mutex::new(Instant::now()));

    loop {
        let started = Instant::now();
        let first = fetch_with_retry(client.clone(), gate.clone(), 1).await?;
        let total_pages = first.pages.max(1);
        let pages = settings.max_pages.unwrap_or(total_pages).min(total_pages);
        let (mut records, mut without_subscribers) =
            publish(first.data, &output).context("Meteora page 1")?;
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
                let number = page.current_page;
                let (count, missing) = publish(page.data, &output)
                    .with_context(|| format!("Meteora page {number}"))?;
                records += count;
                without_subscribers += missing;
                scanned += 1;
            }
        }

        eprintln!(
            "meteora source pages={scanned}/{total_pages} records={records} without_subscribers={without_subscribers} elapsed_ms={}",
            started.elapsed().as_millis()
        );
        if settings.once {
            return Ok(());
        }
        sleep(RESTART_DELAY).await;
    }
}
