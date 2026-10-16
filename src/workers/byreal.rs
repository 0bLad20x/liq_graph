//! Byreal Solana CLMM REST discovery; preserve every provider field in DataRecord.payload.
//! The source emits the same pool identity and optional source TVL as other REST workers.
use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{
    sync::{broadcast, Mutex},
    time::{sleep, sleep_until, Instant},
};

use crate::core::{now_unix_ms, DataBatch, DataRecord, RecordIdentity, Venue};

const URL: &str = "https://api2.byreal.io/byreal/api/dex/v2/pools/info/list";
const SOURCE: &str = "byreal_api";
const PAGE_SIZE: usize = 200;
const RETRY_DELAY: Duration = Duration::from_secs(2);

#[derive(Clone, Copy)]
pub struct Settings {
    pub once: bool,
    pub max_pages: Option<u32>,
    pub rpm: u32,
}

#[derive(Deserialize)]
struct Response {
    result: ApiResult,
}
#[derive(Deserialize)]
struct ApiResult {
    success: bool,
    data: ApiPage,
}
#[derive(Deserialize)]
struct ApiPage {
    total: usize,
    records: Vec<Value>,
}

fn key(pool: &Value, path: &str) -> Result<Pubkey> {
    let raw = pool.pointer(path).and_then(Value::as_str)
        .with_context(|| format!("Byreal missing {path}"))?;
    let key: Pubkey = raw.parse()
        .with_context(|| format!("Byreal invalid {path}"))?;
    ensure!(key != Pubkey::default(), "Byreal zero {path}");
    Ok(key)
}

// Byreal reports 'tvl' in its pool listing. Retain its raw number in
// payload regardless; a source numeric observation is not an on-chain quote.
fn tvl(pool: &Value) -> Option<f64> {
    let raw = pool.get("tvl")?;
    let value = raw.as_f64().or_else(|| raw.as_str()?.parse::<f64>().ok())?;
    (value.is_finite() && value >= 0.0).then_some(value)
}

fn decode(payload: Value) -> Result<DataRecord> {
    // The listing has no programId. Completion verifies the pool owner on-chain.
    let address = key(&payload, "/poolAddress")?;
    let mint_a = key(&payload, "/mintA/mintInfo/address")?;
    let mint_b = key(&payload, "/mintB/mintInfo/address")?;
    ensure!(mint_a != mint_b, "Byreal identical mints");
    Ok(DataRecord {
        identity: RecordIdentity::Pool {
            address,
            venue: Venue::ByrealClmm,
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
) -> Result<ApiPage> {
    loop {
        request_slot(gate, spacing).await;
        let page_text = page.to_string();
        let response = match client.get(URL).query(&[
            ("sortField", "tvl"), ("sortType", "desc"),
            ("pageSize", "200"), ("page", page_text.as_str()),
        ]).send().await {
            Ok(response) => response,
            Err(error) => {
                eprintln!("byreal page={page} request_error={error:#}");
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
            eprintln!("byreal page={page} http_429 cooldown_s={retry_seconds}");
            continue;
        }
        if status.is_client_error() {
            bail!("Byreal page={page} HTTP {status}");
        }
        if !status.is_success() {
            eprintln!("byreal page={page} http_error={status}");
            sleep(RETRY_DELAY).await;
            continue;
        }
        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                eprintln!("byreal page={page} body_error={error:#}");
                sleep(RETRY_DELAY).await;
                continue;
            }
        };
        let envelope: Response = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("byreal page={page} json_error={error:#}");
                sleep(RETRY_DELAY).await;
                continue;
            }
        };
        ensure!(envelope.result.success,"Byreal page={page} API result.success=false");
        ensure!(envelope.result.data.records.len() <= PAGE_SIZE,
            "Byreal page={page} returned more than {PAGE_SIZE} records");
        return Ok(envelope.result.data);
    }
}

pub async fn run(
    client: reqwest::Client,
    output: broadcast::Sender<Arc<DataBatch>>,
    settings: Settings,
) -> Result<()> {
    ensure!((1..=60).contains(&settings.rpm), "BYREAL_API_RPM must be 1..=60");
    let spacing=Duration::from_secs_f64(60.0/f64::from(settings.rpm));
    let gate=Mutex::new(Instant::now());
    let mut pass=0_u64;
    loop {
        pass+=1;
        let started=Instant::now();
        let mut page=1_u32;
        let mut pages=0_u32;
        let mut reported_total=None::<usize>;
        let mut seen=HashSet::<Pubkey>::new();
        let mut received=0_usize;
        let mut duplicates=0_usize;
        let mut rejected=0_usize;
        loop {
            if settings.max_pages.is_some_and(|max|page>max) {break;}
            let response=fetch_page(&client,&gate,spacing,page).await?;
            let total=response.total;
            if let Some(previous)=reported_total {
                ensure!(previous==total,
                    "Byreal inventory changed during pass: total={previous} page={page} total={total}");
            } else {reported_total=Some(total);}
            pages+=1;
            let count=response.records.len();
            received+=count;
            let mut records=Vec::with_capacity(count);
            for payload in response.records {
                match decode(payload){
                    Ok(record)=>{
                        let RecordIdentity::Pool{address,..}=&record.identity else {
                            unreachable!("Byreal emits pools only")
                        };
                        if seen.insert(*address){records.push(record);}
                        else {duplicates+=1;}
                    }
                    Err(error)=>{
                        rejected+=1;
                        if rejected<=5 {eprintln!("byreal page={page} rejected_pool={error:#}");}
                    }
                }
            }
            if !records.is_empty(){
                output.send(Arc::new(DataBatch{
                    source:SOURCE,received_at_ms:now_unix_ms()?,records,
                })).map_err(|_|anyhow!("Byreal source has no consumer"))?;
            }
            if seen.len()>=total {break;}
            ensure!(count>0 && page<10_000,
                "Byreal pagination incomplete page={page} unique={} total={total}",seen.len());
            page=page.checked_add(1).context("Byreal pagination overflow")?;
        }
        let total=reported_total.unwrap_or(0);
        let capped=settings.max_pages.is_some_and(|max|page>max);
        println!("byreal pass={pass} reported_total={total} pages={pages} received={received} unique={} duplicates={duplicates} rejected={rejected} complete={} rpm={} elapsed_ms={}",
            seen.len(),duplicates,rejected,!capped&&seen.len()==total&&rejected==0,settings.rpm,started.elapsed().as_millis());
        if settings.once{return Ok(());}
    }
}
