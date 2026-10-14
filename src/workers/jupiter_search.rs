//! Jupiter Search: rotate a typed mint input, decode once, publish complete token records.
//! All scheduling and request state belongs to this worker.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use reqwest::Client;
use solana_pubkey::Pubkey;
use tokio::{
    sync::{broadcast, watch},
    task::JoinSet,
    time::{Instant, sleep_until},
};

use crate::core::{DataBatch, DataRecord, RecordIdentity, now_unix_ms};

const SOURCE: &str = "jupiter_search";

pub struct Settings {
    pub url: String,
    pub api_keys: Vec<String>,
    pub calls_per_minute_per_key: f64,
    pub batch_size: usize,
    pub timeout: Duration,
}

struct Lane {
    key: String,
    next_due: Instant,
    in_flight: bool,
}

enum RequestOutcome {
    Success { batch: DataBatch, rejected: usize },
    DecodeError(anyhow::Error),
    RateLimited(Option<String>),
    HttpError(reqwest::StatusCode),
    TransportError(reqwest::Error),
    BodyError(reqwest::Error),
}

struct Completion {
    lane: usize,
    requested: usize,
    outcome: RequestOutcome,
}

fn decode_batch(body: &[u8]) -> Result<(DataBatch, usize)> {
    let tokens: Vec<Value> = serde_json::from_slice(body)
        .context("invalid Jupiter Search JSON token array")?;
    let mut records = Vec::with_capacity(tokens.len());
    let mut rejected = 0_usize;
    for payload in tokens {
        let address = payload.get("id")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<Pubkey>().ok());
        match address {
            Some(address) if address != Pubkey::default() => records.push(DataRecord {
                identity: RecordIdentity::Mint { address }, payload,
            }),
            _ => rejected += 1,
        }
    }
    Ok((DataBatch {
        source: SOURCE,
        received_at_ms: now_unix_ms()?,
        records,
    }, rejected))
}

pub async fn run(
    client: Client,
    settings: Settings,
    mut input: watch::Receiver<Arc<[Pubkey]>>,
    output: broadcast::Sender<Arc<DataBatch>>,
) -> Result<()> {
    ensure!(!settings.url.trim().is_empty(), "Jupiter Search URL missing");
    ensure!(!settings.api_keys.is_empty(), "Jupiter Search API keys missing");
    ensure!(
        settings.calls_per_minute_per_key.is_finite()
            && settings.calls_per_minute_per_key > 0.0,
        "Jupiter Search calls per minute must be positive"
    );
    ensure!(
        (1..=100).contains(&settings.batch_size),
        "Jupiter Search batch size must be 1..=100"
    );
    ensure!(!settings.timeout.is_zero(), "Jupiter Search timeout must be positive");

    let period = Duration::from_secs_f64(60.0 / settings.calls_per_minute_per_key);
    let phase = period.div_f64(settings.api_keys.len() as f64);
    let start = Instant::now();
    let mut lanes = settings.api_keys.into_iter().enumerate()
        .map(|(index, key)| Lane {
            key,
            next_due: start + phase.mul_f64(index as f64),
            in_flight: false,
        })
        .collect::<Vec<_>>();
    let mut pending = JoinSet::<Completion>::new();
    let mut cursor = 0_usize;
    let mut published = 0_u64;

    eprintln!(
        "jupiter source started keys={} rate_per_key={} batch_size={} targets={}",
        lanes.len(), settings.calls_per_minute_per_key,
        settings.batch_size, input.borrow().len()
    );

    loop {
        let targets = input.borrow_and_update().clone();

        if !targets.is_empty() {
            for (index, lane) in lanes.iter_mut().enumerate() {
                let now = Instant::now();
                if lane.in_flight || now < lane.next_due {
                    continue;
                }
                if cursor >= targets.len() {
                    cursor = 0;
                }

                let count = settings.batch_size.min(targets.len() - cursor);
                let query = targets[cursor..cursor + count].iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                cursor = (cursor + count) % targets.len();

                lane.next_due = now + period;
                lane.in_flight = true;
                let http = client.clone();
                let url = settings.url.clone();
                let key = lane.key.clone();
                let timeout = settings.timeout;
                pending.spawn(async move {
                    let outcome = match http.get(&url)
                        .query(&[("query", query)])
                        .header("x-api-key", key)
                        .timeout(timeout)
                        .send()
                        .await
                    {
                        Err(error) => RequestOutcome::TransportError(error),
                        Ok(response) => {
                            let status = response.status();
                            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                                let retry_after = response.headers()
                                    .get(reqwest::header::RETRY_AFTER)
                                    .and_then(|header| header.to_str().ok())
                                    .map(ToOwned::to_owned);
                                RequestOutcome::RateLimited(retry_after)
                            } else if !status.is_success() {
                                RequestOutcome::HttpError(status)
                            } else {
                                match response.bytes().await {
                                    Ok(body) => match decode_batch(&body) {
                                        Ok((batch, rejected)) => RequestOutcome::Success { batch, rejected },
                                        Err(error) => RequestOutcome::DecodeError(error),
                                    },
                                    Err(error) => RequestOutcome::BodyError(error),
                                }
                            }
                        }
                    };
                    Completion { lane: index, requested: count, outcome }
                });
            }
        }

        let next_due = if targets.is_empty() {
            None
        } else {
            lanes.iter()
                .filter(|lane| !lane.in_flight)
                .map(|lane| lane.next_due)
                .min()
        };
        let has_pending = !pending.is_empty();

        tokio::select! {
            changed = input.changed() => {
                changed?;
                cursor = 0;
                eprintln!("jupiter source targets_updated={}", input.borrow().len());
            }
            result = pending.join_next(), if has_pending => {
                let Some(result) = result else { continue };
                let completion = result?;
                lanes[completion.lane].in_flight = false;

                match completion.outcome {
                    RequestOutcome::TransportError(error) => eprintln!(
                        "jupiter source lane={} transport_error={error}",
                        completion.lane
                    ),
                    RequestOutcome::BodyError(error) => eprintln!(
                        "jupiter source lane={} body_error={error}",
                        completion.lane
                    ),
                    RequestOutcome::DecodeError(error) => eprintln!(
                        "jupiter source lane={} decode_error={error:#}",
                        completion.lane
                    ),
                    RequestOutcome::RateLimited(retry_after) => eprintln!(
                        "jupiter source lane={} rate_limited retry_after={}",
                        completion.lane, retry_after.as_deref().unwrap_or("unspecified")
                    ),
                    RequestOutcome::HttpError(status) => eprintln!(
                        "jupiter source lane={} http_status={status}",
                        completion.lane
                    ),
                    RequestOutcome::Success { batch, rejected } => {
                        let records = batch.records.len();
                        let delivered = output.send(Arc::new(batch)).is_ok();
                        published += 1;
                        eprintln!(
                            "jupiter source lane={} requested={} decoded_mints={} rejected_mints={} published={} subscriber={delivered}",
                            completion.lane, completion.requested, records, rejected, published
                        );
                    }
                }
            }
            _ = sleep_until(next_due.unwrap_or_else(Instant::now)), if next_due.is_some() => {}
        }
    }
}
