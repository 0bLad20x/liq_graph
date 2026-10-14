//! Jupiter Search: rotate a typed mint input and publish unmodified API responses.
//! All scheduling and request state belongs to this worker.

use std::{sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use reqwest::Client;
use solana_pubkey::Pubkey;
use tokio::{
    sync::{broadcast, watch},
    task::JoinSet,
    time::{Instant, sleep_until},
};

use crate::core::{RecordKind, SourceBatch};

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

struct Completion {
    lane: usize,
    requested: usize,
    response: Result<reqwest::Response, reqwest::Error>,
}

pub async fn run(
    client: Client,
    settings: Settings,
    mut input: watch::Receiver<Arc<[Pubkey]>>,
    output: broadcast::Sender<SourceBatch>,
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
                    let response = http.get(&url)
                        .query(&[("query", query)])
                        .header("x-api-key", key)
                        .timeout(timeout)
                        .send()
                        .await;
                    Completion { lane: index, requested: count, response }
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

                match completion.response {
                    Err(error) => eprintln!(
                        "jupiter source lane={} transport_error={error}",
                        completion.lane
                    ),
                    Ok(response) => {
                        let status = response.status();
                        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                            let retry_after = response.headers()
                                .get(reqwest::header::RETRY_AFTER)
                                .and_then(|header| header.to_str().ok())
                                .unwrap_or("unspecified");
                            eprintln!(
                                "jupiter source lane={} rate_limited retry_after={retry_after}",
                                completion.lane
                            );
                        } else if !status.is_success() {
                            eprintln!(
                                "jupiter source lane={} http_status={status}",
                                completion.lane
                            );
                        } else {
                            match response.bytes().await {
                                Err(error) => eprintln!(
                                    "jupiter source lane={} body_error={error}",
                                    completion.lane
                                ),
                                Ok(body) => {
                                    let bytes = body.len();
                                    let delivered = output.send(SourceBatch {
                                        source: SOURCE,
                                        kind: RecordKind::Mint,
                                        body,
                                    }).is_ok();
                                    published += 1;
                                    eprintln!(
                                        "jupiter source lane={} requested={} bytes={} published={} subscriber={delivered}",
                                        completion.lane, completion.requested, bytes, published
                                    );
                                }
                            }
                        }
                    }
                }
            }
            _ = sleep_until(next_due.unwrap_or_else(Instant::now)), if next_due.is_some() => {}
        }
    }
}
