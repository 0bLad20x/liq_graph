//! Manifest DEX discovery: snapshot the stats server's unpaginated market ticker inventory.
//! Prices/volumes remain source payload; neither TVL nor executable quotes are inferred.
use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::sync::broadcast;

use crate::core::{now_unix_ms, DataBatch, DataRecord, RecordIdentity, Venue};

const URL: &str = "https://mfx-stats-mainnet.fly.dev/tickers";
const SOURCE: &str = "manifest_tickers_api";
const REFRESH: Duration = Duration::from_secs(300);

#[derive(Clone, Copy)]
pub struct Settings {
    pub once: bool,
}

fn key(value: &Value, name: &str) -> Result<Pubkey> {
    let raw = value.get(name).and_then(Value::as_str)
        .with_context(|| format!("Manifest ticker missing {name}"))?;
    let address: Pubkey = raw.parse()
        .with_context(|| format!("Manifest ticker invalid {name}"))?;
    ensure!(address != Pubkey::default(), "Manifest ticker zero {name}");
    Ok(address)
}

fn decode(payload: Value) -> Result<DataRecord> {
    let address = key(&payload, "ticker_id")?;
    let pool_id = key(&payload, "pool_id")?;
    ensure!(address == pool_id, "Manifest ticker_id/pool_id mismatch for {address}");
    let mint_a = key(&payload, "base_currency")?;
    let mint_b = key(&payload, "target_currency")?;
    ensure!(mint_a != mint_b, "Manifest market has identical mints {address}");
    Ok(DataRecord {
        identity: RecordIdentity::Pool {
            address,
            venue: Venue::Manifest,
            mint_a: Some(mint_a),
            mint_b: Some(mint_b),
        },
        // The orderbook protocol's reported liquidity_in_usd is always 0,
        // not a meaningful TVL measurement. Preserve it only in payload.
        tvl_usd: None,
        config: None,
        payload,
    })
}

async fn fetch(client: &reqwest::Client) -> Result<Vec<Value>> {
    // Finite error handling: a discovery/live probe must not loop indefinitely.
    for attempt in 1..=4 {
        match client.get(URL).send().await {
            Ok(response) if response.status().is_success() => {
                let bytes = response.bytes().await.context("Manifest ticker response body")?;
                let values: Vec<Value> = serde_json::from_slice(&bytes)
                    .context("Manifest /tickers must contain one JSON array")?;
                return Ok(values);
            }
            Ok(response) if response.status().is_client_error()
                && response.status() != reqwest::StatusCode::TOO_MANY_REQUESTS =>
            {
                bail!("Manifest /tickers HTTP {}", response.status());
            }
            Ok(response) => {
                let status = response.status();
                if attempt == 4 { bail!("Manifest /tickers HTTP {status} after 4 attempts"); }
                let retry = response.headers().get(reqwest::header::RETRY_AFTER)
                    .and_then(|v|v.to_str().ok()).and_then(|v|v.parse::<u64>().ok())
                    .unwrap_or(3).max(1);
                crate::log_event!(Info,"manifest discovery attempt={attempt} http={status} retry_s={retry}");
                tokio::time::sleep(Duration::from_secs(retry)).await;
            }
            Err(error) => {
                if attempt == 4 { return Err(error).context("Manifest /tickers HTTP transport"); }
                crate::log_event!(Warn,"manifest discovery attempt={attempt} transport={error}");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
    unreachable!("bounded attempts exhaust via return or bail")
}

pub async fn run(
    client: reqwest::Client,
    output: broadcast::Sender<Arc<DataBatch>>,
    settings: Settings,
) -> Result<()> {
    let mut pass = 0_u64;
    loop {
        pass += 1;
        let started = std::time::Instant::now();
        let tickers = fetch(&client).await?;
        let reported = tickers.len();
        let mut seen = HashSet::with_capacity(reported);
        let mut records = Vec::with_capacity(reported);
        let mut duplicates = 0_usize;
        let mut rejected = 0_usize;
        for payload in tickers {
            match decode(payload) {
                Ok(record) => {
                    let RecordIdentity::Pool { address, .. } = &record.identity else {
                        unreachable!("Manifest discovery emits only pool records")
                    };
                    if seen.insert(*address) { records.push(record); }
                    else { duplicates += 1; }
                }
                Err(error) => {
                    rejected += 1;
                    if rejected <= 5 {
                        crate::log_event!(Warn,"manifest discovery rejected_market={error:#}");
                    }
                }
            }
        }
        let unique = records.len();
        ensure!(reported > 0, "Manifest /tickers returned an empty inventory");
        ensure!(rejected == 0 && duplicates == 0,
            "Manifest /tickers inconsistent: received={reported} unique={unique} rejected={rejected} duplicates={duplicates}");
        output.send(Arc::new(DataBatch {
            source: SOURCE, received_at_ms: now_unix_ms()?, records,
        })).map_err(|_|anyhow!("Manifest discovery has no DataBatch consumer"))?;
        crate::log_event!(Info,"manifest discovery pass={pass} received={reported} unique={unique} duplicates={duplicates} rejected={rejected} complete=true tvl_usd=none elapsed_ms={}",
            started.elapsed().as_millis());
        if settings.once {return Ok(());}
        tokio::time::sleep(REFRESH).await;
    }
}
