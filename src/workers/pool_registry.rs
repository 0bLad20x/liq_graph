//! Pool registry: one authoritative in-memory state per pool address.
//! Reads decoded source batches; does not query RPC, persist, decode venue formats,
//! select graph routes, or retain arbitrary API payloads.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::Result;
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::sync::broadcast;

use crate::core::{DataBatch, RecordIdentity, Venue};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // Completion results are connected in a later checkpoint.
pub enum CompletionState {
    Unknown,
    Prepared,
    Incomplete,
}

/// One current pool observation; protocol-specific execution manifests are added
/// only when an actual completion contract exists.
#[derive(Clone, Debug)]
pub struct PoolState {
    pub address: Pubkey,
    pub venue: Venue,
    pub mint_a: Option<Pubkey>,
    pub mint_b: Option<Pubkey>,
    pub source: &'static str,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
    pub last_changed_ms: u64,
    pub tvl_usd: Option<f64>,
    pub completion_state: CompletionState,
}

/// Changed records only; last_seen-only refreshes remain accessible in snapshots.
#[derive(Debug)]
pub struct PoolUpdateBatch {
    pub source: &'static str,
    pub received_at_ms: u64,
    pub pools: Vec<PoolState>,
}

/// The write owner is the registry worker. Consumers can only request typed snapshots.
#[derive(Clone, Default)]
pub struct PoolRegistry {
    entries: Arc<RwLock<HashMap<Pubkey, PoolState>>>,
}

impl PoolRegistry {
    pub fn snapshot(&self) -> Vec<PoolState> {
        self.entries.read().expect("pool registry lock poisoned")
            .values().cloned().collect()
    }

    fn apply(&self, batch: &DataBatch) -> BatchStats {
        let mut stats = BatchStats::default();
        let mut entries = self.entries.write().expect("pool registry lock poisoned");
        let mut updates = Vec::new();

        for record in &batch.records {
            let RecordIdentity::Pool { address, venue, mint_a, mint_b } = &record.identity
            else {
                stats.ignored_mints += 1;
                continue;
            };

            if *address == Pubkey::default() {
                stats.conflicts += 1;
                continue;
            }

            let observed_tvl = tvl_usd(&record.payload);
            match entries.get_mut(address) {
                None => {
                    let state = PoolState {
                        address: *address,
                        venue: *venue,
                        mint_a: *mint_a,
                        mint_b: *mint_b,
                        source: batch.source,
                        first_seen_ms: batch.received_at_ms,
                        last_seen_ms: batch.received_at_ms,
                        last_changed_ms: batch.received_at_ms,
                        tvl_usd: observed_tvl,
                        completion_state: CompletionState::Unknown,
                    };
                    updates.push(state.clone());
                    entries.insert(*address, state);
                    stats.inserted += 1;
                }
                Some(current) => {
                    // An older observation must not replace a newer one.
                    if batch.received_at_ms < current.last_seen_ms {
                        stats.stale += 1;
                        continue;
                    }
                    // Pool ownership and known mints are stable facts. Reject
                    // identity conflicts instead of corrupting the registry.
                    if current.venue != *venue ||
                        mint_a.is_some_and(|mint| current.mint_a.is_some_and(|old| old != *mint)) ||
                        mint_b.is_some_and(|mint| current.mint_b.is_some_and(|old| old != *mint))
                    {
                        stats.conflicts += 1;
                        eprintln!("pool registry identity_conflict pool={address} source={}", batch.source);
                        continue;
                    }

                    let mut changed = false;
                    if current.mint_a.is_none() && mint_a.is_some() {
                        current.mint_a = *mint_a;
                        changed = true;
                    }
                    if current.mint_b.is_none() && mint_b.is_some() {
                        current.mint_b = *mint_b;
                        changed = true;
                    }
                    if let Some(tvl) = observed_tvl {
                        if current.tvl_usd != Some(tvl) {
                            current.tvl_usd = Some(tvl);
                            changed = true;
                        }
                    }
                    if current.source != batch.source {
                        current.source = batch.source;
                        changed = true;
                    }

                    current.last_seen_ms = current.last_seen_ms.max(batch.received_at_ms);
                    if changed {
                        current.last_changed_ms = batch.received_at_ms;
                        updates.push(current.clone());
                        stats.changed += 1;
                    } else {
                        stats.unchanged += 1;
                    }
                }
            }
        }

        stats.total = entries.len();
        stats.updates = updates;
        stats
    }
}

#[derive(Default)]
struct BatchStats {
    inserted: usize,
    changed: usize,
    unchanged: usize,
    stale: usize,
    conflicts: usize,
    ignored_mints: usize,
    total: usize,
    updates: Vec<PoolState>,
}

fn tvl_usd(payload: &Value) -> Option<f64> {
    // Consume existing source fields; the original full payload remains on
    // the source stream. Unknown or invalid TVL never replaces a known value.
    let value = payload.get("tvl_usd").or_else(|| payload.get("tvl"))?;
    let parsed = value.as_f64()
        .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))?;
    (parsed.is_finite() && parsed >= 0.0).then_some(parsed)
}

/// Consume every source observation; only semantic pool-state changes are emitted.
/// A lagged broadcast receiver is observable but cannot reconstruct missing pages.
pub async fn run(
    mut input: broadcast::Receiver<Arc<DataBatch>>,
    registry: PoolRegistry,
    output: broadcast::Sender<Arc<PoolUpdateBatch>>,
) -> Result<()> {
    let mut batches = 0_u64;
    let mut input_records = 0_u64;
    let mut inserted = 0_u64;
    let mut changed = 0_u64;
    let mut unchanged = 0_u64;
    let mut stale = 0_u64;
    let mut conflicts = 0_u64;
    let mut lagged = 0_u64;

    loop {
        match input.recv().await {
            Ok(batch) => {
                batches += 1;
                input_records += batch.records.len() as u64;
                let stats = registry.apply(&batch);
                inserted += stats.inserted as u64;
                changed += stats.changed as u64;
                unchanged += stats.unchanged as u64;
                stale += stats.stale as u64;
                conflicts += stats.conflicts as u64;
                let updates = stats.updates.len();
                if updates > 0 {
                    let delivered = output.send(Arc::new(PoolUpdateBatch {
                        source: batch.source,
                        received_at_ms: batch.received_at_ms,
                        pools: stats.updates,
                    })).is_ok();
                    if !delivered {
                        eprintln!("pool registry update_broadcast_no_subscribers");
                    }
                }
                if batches == 1 || batches % 25 == 0 {
                    println!(
                        "pool registry batches={batches} input_records={input_records} new={inserted} changed={changed} unchanged={unchanged} stale={stale} conflicts={conflicts} pools_in_ram={} updates_published={updates} ignored_mints={}",
                        stats.total, stats.ignored_mints,
                    );
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                lagged += skipped;
                eprintln!("pool registry source_lagged missed_batches={skipped}");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }

    // One full snapshot at the end of a bounded --once live run.
    let snapshot = registry.snapshot();
    let with_tvl = snapshot.iter().filter(|p| p.tvl_usd.is_some()).count();
    println!(
        "pool registry complete batches={batches} input_records={input_records} pools_in_ram={} with_tvl={with_tvl} new={inserted} changed={changed} unchanged={unchanged} stale={stale} conflicts={conflicts} lagged_batches={lagged}",
        snapshot.len(),
    );
    Ok(())
}

/// Temporary live-test subscriber: proves changed states reach a real consumer.
pub async fn observe_updates(
    mut input: broadcast::Receiver<Arc<PoolUpdateBatch>>,
) -> Result<()> {
    let mut batches = 0_u64;
    let mut pool_updates = 0_u64;
    let mut lagged = 0_u64;
    loop {
        match input.recv().await {
            Ok(batch) => {
                batches += 1;
                pool_updates += batch.pools.len() as u64;
                if batches == 1 {
                    if let Some(pool) = batch.pools.first() {
                        println!(
                            "pool registry consumer source={} received_at_ms={} batch_pools={} sample_pool={} venue={:?} mint_a={:?} mint_b={:?} tvl_usd={:?} completion={:?} first_seen_ms={} last_seen_ms={} last_changed_ms={}",
                            batch.source, batch.received_at_ms, batch.pools.len(),
                            pool.address, pool.venue, pool.mint_a, pool.mint_b,
                            pool.tvl_usd, pool.completion_state, pool.first_seen_ms,
                            pool.last_seen_ms, pool.last_changed_ms,
                        );
                    }
                } else if batches % 25 == 0 {
                    println!(
                        "pool registry consumer received_batches={batches} pool_updates={pool_updates}"
                    );
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                lagged += skipped;
                eprintln!("pool registry consumer_lagged missed_batches={skipped}");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    println!(
        "pool registry consumer complete received_batches={batches} pool_updates={pool_updates} lagged_batches={lagged}"
    );
    Ok(())
}
