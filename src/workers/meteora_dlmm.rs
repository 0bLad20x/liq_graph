//! Meteora DLMM API source: fetch, validate pagination, broadcast unchanged HTTP bytes.
//! No pool decoding, individual allocations, normalization, or deduplication.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use serde::{Deserialize, de::IgnoredAny};
use tokio::{
    sync::{Mutex, broadcast},
    task::JoinSet,
    time::{Instant, sleep, sleep_until},
};

use crate::core::{RecordKind, SourceBatch};

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
    data: Vec<IgnoredAny>,
}

struct Page {
    pages: u32,
    records: usize,
    body: Bytes,
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
    let envelope: PageEnvelope = serde_json::from_slice(&body)
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
    Ok(Page {
        pages: envelope.pages.max(1),
        records: envelope.data.len(),
        body,
    })
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

/// A batch is one whole API response; broadcast never waits for a receiver.
fn publish(output: &broadcast::Sender<SourceBatch>, body: Bytes) -> bool {
    output.send(SourceBatch {
        source: SOURCE,
        kind: RecordKind::Pool,
        body,
    }).is_ok()
}

pub async fn run(
    client: reqwest::Client,
    output: broadcast::Sender<SourceBatch>,
    settings: Settings,
) -> Result<()> {
    let gate = Arc::new(Mutex::new(Instant::now()));

    loop {
        let started = Instant::now();
        let first = fetch_with_retry(client.clone(), gate.clone(), 1).await;
        let total_pages = first.pages;
        let pages = settings.max_pages.unwrap_or(total_pages).min(total_pages);
        let mut records = first.records;
        let mut unconsumed_batches = usize::from(!publish(&output, first.body));
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
                records += page.records;
                unconsumed_batches += usize::from(!publish(&output, page.body));
                scanned += 1;
            }
        }

        eprintln!(
            "meteora source pages={scanned}/{total_pages} records={records} unconsumed_batches={unconsumed_batches} elapsed_ms={}",
            started.elapsed().as_millis()
        );
        if settings.once {
            return Ok(());
        }
    }
}
