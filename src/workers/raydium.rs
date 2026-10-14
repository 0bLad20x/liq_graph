//! Raydium REST discovery: ten independent cursor routes -> typed pool batches.
//! Route ordering is for API coverage only; programId identifies the actual venue.
use std::{sync::Arc, time::Duration};
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, Mutex}, task::JoinSet, time::{Instant, sleep, sleep_until}};
use crate::core::{DataBatch, DataRecord, RecordIdentity, Venue, now_unix_ms};

const URL: &str = "https://api-v3.raydium.io/pools/info/list-v2";
const SOURCE: &str = "raydium_api";
const PAGE_SIZE: usize = 1000;
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
async fn route(client: reqwest::Client, gate: Arc<Mutex<Instant>>,
    output: broadcast::Sender<Arc<DataBatch>>, settings: Settings,
    sort: &'static str, direction: &'static str) -> Result<()> {
    let spacing = Duration::from_secs_f64(1.0/f64::from(settings.rps));
    loop {
        let started = Instant::now();
        let mut cursor: Option<String> = None;
        let mut page_no = 0_u32;
        let mut received = 0_usize;
        let mut accepted = 0_usize;
        let mut rejected = 0_usize;
        let mut rejected_unknown = 0_usize;
        let mut errors = 0_u64;
        loop {
            wait_slot(&gate, spacing).await;
            let mut request = client.get(URL).query(&[
                ("sortField",sort),("sortType",direction),("size","1000"),
            ]);
            if let Some(next) = &cursor { request = request.query(&[("nextPageId",next)]); }
            let response = match request.send().await {
                Ok(value) => value,
                Err(err) => { errors += 1; eprintln!("raydium sort={sort} direction={direction} request_error={err:#}"); sleep(Duration::from_secs(2)).await; continue; }
            };
            if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                errors += 1;
                let seconds = response.headers().get(reqwest::header::RETRY_AFTER)
                    .and_then(|v|v.to_str().ok()).and_then(|v|v.parse::<u64>().ok()).unwrap_or(5);
                cool_down(&gate,Duration::from_secs(seconds)).await;
                eprintln!("raydium sort={sort} direction={direction} http_429 cooldown_s={seconds}");
                continue;
            }
            if response.status().is_client_error() {
                bail!("Raydium {sort}/{direction} HTTP {}",response.status());
            }
            if !response.status().is_success() {
                errors += 1; eprintln!("raydium sort={sort} direction={direction} http_error={}",response.status());
                sleep(Duration::from_secs(2)).await; continue;
            }
            let body = response.bytes().await.context("read Raydium response")?;
            let envelope: Response = serde_json::from_slice(&body).context("decode Raydium envelope")?;
            if !envelope.success {
                errors += 1;
                eprintln!("raydium sort={sort} direction={direction} success=false retrying_cursor");
                sleep(Duration::from_secs(2)).await;
                continue;
            }
            let page = envelope.data.context("Raydium response missing data")?;
            ensure!(page.data.len() <= PAGE_SIZE,"Raydium returned oversized page");
            let next = page.next.filter(|v|!v.is_empty());
            ensure!(!page.data.is_empty() || next.is_none(),"Raydium empty page with cursor");
            if let Some(value) = &next {
                ensure!(cursor.as_ref() != Some(value),"Raydium repeated cursor");
            }
            page_no += 1;
            received += page.data.len();
            let mut records = Vec::with_capacity(page.data.len());
            for payload in page.data {
                match decode(payload) {
                    Ok(record)=>records.push(record),
                    Err(err)=>{
                        rejected+=1;
                        if err.to_string().contains("unknown Raydium programId"){rejected_unknown+=1;}
                        if rejected <= 5 { eprintln!("raydium decode_rejected error={err:#}"); }
                    }
                }
            }
            accepted += records.len();
            if !records.is_empty() {
                output.send(Arc::new(DataBatch { source: SOURCE,
                    received_at_ms: now_unix_ms()?, records }))
                    .map_err(|_|anyhow!("Raydium batch has no consumer"))?;
            }
            if page_no==1 || page_no%10==0 {
                println!("raydium route sort={sort} direction={direction} pages={page_no} received={received} accepted={accepted} rejected={rejected} rejected_unknown_program={rejected_unknown} errors={errors} elapsed_ms={}",started.elapsed().as_millis());
            }
            cursor = next;
            if cursor.is_none() || settings.max_pages.is_some_and(|max|page_no>=max){break;}
        }
        println!("raydium route complete sort={sort} direction={direction} pages={page_no} received={received} accepted={accepted} rejected={rejected} rejected_unknown_program={rejected_unknown} errors={errors} elapsed_ms={}",started.elapsed().as_millis());
        if settings.once {return Ok(());}
    }
}
pub async fn run(client: reqwest::Client, output: broadcast::Sender<Arc<DataBatch>>,
    settings: Settings) -> Result<()> {
    ensure!(settings.rps>0 && settings.rps<=100,"Raydium RPS must be 1..=100");
    let gate=Arc::new(Mutex::new(Instant::now()));
    let mut tasks=JoinSet::new();
    for (sort,direction) in ROUTES {
        let http=client.clone();
        let limiter=gate.clone();
        let sender=output.clone();
        tasks.spawn(async move {
            let result=route(http,limiter,sender,settings,sort,direction).await;
            if !settings.once {
                if let Err(err)=&result {eprintln!("raydium route_stopped sort={sort} direction={direction} error={err:#}");}
                Ok(())
            } else {result}
        });
    }
    let mut first_error=None;
    while let Some(joined)=tasks.join_next().await {
        match joined.context("Raydium route task panicked")? {
            Ok(())=>{},
            Err(err)=>{if first_error.is_none(){first_error=Some(err);}},
        }
    }
    if let Some(err)=first_error{return Err(err);}
    if !settings.once {bail!("all Raydium API routes stopped");}
    Ok(())
}
