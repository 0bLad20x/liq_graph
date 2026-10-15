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
use tokio::sync::{broadcast,watch};

use crate::core::{
    now_unix_ms, BinArrayInventory, CompletionResultBatch, CompletionStatus, DataBatch,
    ExecutionManifest, MintProof, RecordIdentity, Venue,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionState {
    Unknown,
    Prepared,
    Incomplete,
}

/// One current pool observation, including accepted execution-completion results.
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
    /// Durable, validated protocol configuration, not arbitrary REST JSON.
    pub pool_config: Option<Arc<Value>>,
    /// Historical persisted observation; never treat as a fresh live quote.
    pub last_observation: Option<Arc<Value>>,
    pub completion_state: CompletionState,
    pub completion_validated_slot: Option<u64>,
    pub execution_manifest: Option<Arc<ExecutionManifest>>,
    pub execution_inventory: Option<Arc<BinArrayInventory>>,
    pub mint_proofs: Option<[MintProof; 2]>,
    pub completion_reason: Option<String>,
}

/// Distinguish source observations from completed execution metadata without
/// interpreting worker-specific source names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoolUpdateKind {
    Source,
    Completion,
}

/// Changed records only; last_seen-only refreshes remain accessible in snapshots.
#[derive(Debug)]
pub struct PoolUpdateBatch {
    pub kind: PoolUpdateKind,
    pub source: &'static str,
    pub received_at_ms: u64,
    pub pools: Vec<PoolState>,
    /// Identity/configuration changes only. Excludes TVL-only updates.
    pub persist_identity: Vec<Pubkey>,
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

    /// Bootstrap complete, previously validated state before any producer starts.
    /// REST observations still own current TVL/freshness; persisted proof is historical.
    pub fn restore(&self, pools: Vec<PoolState>) -> Result<usize> {
        use anyhow::ensure;
        let mut entries = self.entries.write().expect("pool registry lock poisoned");
        ensure!(entries.is_empty(), "Registry restore requires an empty RAM state");
        let mut restored = HashMap::with_capacity(pools.len());
        for pool in pools {
            ensure!(pool.address != Pubkey::default(), "invalid restored pool address");
            let a = pool.mint_a;
            let b = pool.mint_b;
            ensure!(a.is_none_or(|a| a != Pubkey::default())
                && b.is_none_or(|b| b != Pubkey::default())
                && !(a.is_some() && a == b),
                "invalid restored pool mint identity");
            if let Some(config) = &pool.pool_config {
                ensure!(config.is_object(), "invalid restored pool configuration");
            }
            if let Some(observation) = &pool.last_observation {
                ensure!(observation.is_object(), "invalid restored pool observation");
            }
            match pool.completion_state {
                CompletionState::Unknown => {
                    ensure!(pool.completion_validated_slot.is_none()
                        && pool.execution_manifest.is_none()
                        && pool.execution_inventory.is_none()
                        && pool.mint_proofs.is_none(),
                        "restored Unknown pool has execution proof");
                }
                CompletionState::Prepared => {
                    let (Some(a), Some(b), Some(slot), Some(m)) =
                        (a, b, pool.completion_validated_slot, pool.execution_manifest.as_ref())
                    else { anyhow::bail!("restored Prepared pool missing identity or proof"); };
                    ensure!(m.pool == pool.address && m.mint_a == a && m.mint_b == b
                        && m.validated_slot == slot && !m.base_a.is_empty() && !m.base_b.is_empty()
                        && pool.execution_inventory.is_some() && pool.mint_proofs.is_some(),
                        "invalid restored manifest identity, slot or RPC evidence");
                }
                CompletionState::Incomplete => {
                    ensure!(a.is_some() && b.is_some()
                        && pool.completion_validated_slot.is_some()
                        && pool.execution_manifest.is_none(),
                        "invalid restored Incomplete pool");
                }
            }
            ensure!(restored.insert(pool.address, pool).is_none(),
                "duplicate persisted pool address");
        }
        *entries = restored;
        Ok(entries.len())
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

            let observed_tvl = record.tvl_usd;
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
                        pool_config: None,
                        last_observation: None,
                        completion_state: CompletionState::Unknown,
                        completion_validated_slot: None,
                        execution_manifest: None,
                        execution_inventory: None,
                        mint_proofs: None,
                        completion_reason: None,
                    };
                    updates.push(state.clone());
                    stats.identity.push(*address);
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
                        mint_a.as_ref().is_some_and(|mint| current.mint_a.is_some_and(|old| old != *mint)) ||
                        mint_b.as_ref().is_some_and(|mint| current.mint_b.is_some_and(|old| old != *mint))
                    {
                        stats.conflicts += 1;
                        eprintln!("pool registry identity_conflict pool={address} source={}", batch.source);
                        continue;
                    }

                    let mut changed = false;
                    let mut identity_changed = false;
                    if current.mint_a.is_none() && mint_a.is_some() {
                        current.mint_a = *mint_a;
                        changed = true;
                        identity_changed = true;
                    }
                    if current.mint_b.is_none() && mint_b.is_some() {
                        current.mint_b = *mint_b;
                        changed = true;
                        identity_changed = true;
                    }
                    if let Some(tvl) = observed_tvl {
                        if current.tvl_usd != Some(tvl) {
                            // Only the first ever usable TVL snapshot merits a
                            // persistence refresh. Subsequent TVL changes stay RAM-only.
                            if current.tvl_usd.is_none() && current.last_observation.as_ref()
                                .and_then(|o| o.get("tvl_usd"))
                                .and_then(Value::as_f64).is_none() {
                                identity_changed = true;
                            }
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
                        if identity_changed { stats.identity.push(*address); }
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

    // Only the Registry worker calls this method. Completion is a typed input, never
    // an external writer into the Registry's private RAM state.
    fn apply_completion(&self, batch: &CompletionResultBatch, changed_at_ms: u64)
        -> CompletionStats {
        let mut stats = CompletionStats::default();
        let mut entries = self.entries.write().expect("pool registry lock poisoned");
        for result in &batch.results {
            stats.received += 1;
            let Some(pool) = entries.get_mut(&result.pool) else {
                stats.missing_pool += 1;
                continue;
            };
            let accepted_slot = pool.completion_validated_slot;
            if result.validated_slot.is_some_and(|slot|
                accepted_slot.is_some_and(|previous| slot <= previous)) {
                stats.stale += 1;
                continue;
            }
            if result.status == CompletionStatus::Retryable {
                // Transient RPC/validation failures must not overwrite previously
                // validated manifests or turn an Unknown pool into a terminal state.
                stats.retryable += 1;
                if pool.completion_state == CompletionState::Unknown {
                    pool.completion_reason = result.reason.clone();
                    pool.last_changed_ms = pool.last_changed_ms.max(changed_at_ms);
                    stats.updates.push(pool.clone());
                    stats.applied += 1;
                }
                continue;
            }

            let Some(slot) = result.validated_slot else {
                stats.invalid += 1;
                continue;
            };
            if result.status == CompletionStatus::Prepared {
                let Some(manifest) = result.manifest.as_ref() else {
                    stats.invalid += 1;
                    continue;
                };
                if manifest.pool != result.pool || manifest.validated_slot != slot
                    || pool.mint_a != Some(manifest.mint_a)
                    || pool.mint_b != Some(manifest.mint_b)
                    || result.inventory.is_none() || result.mints.is_none() {
                    stats.invalid += 1;
                    continue;
                }
            } else if result.manifest.is_some() {
                stats.invalid += 1;
                continue;
            }

            pool.completion_state = match result.status {
                CompletionStatus::Prepared => CompletionState::Prepared,
                CompletionStatus::Incomplete => CompletionState::Incomplete,
                CompletionStatus::Retryable => unreachable!("handled above"),
            };
            pool.completion_validated_slot = Some(slot);
            pool.execution_manifest = result.manifest.clone();
            pool.execution_inventory = result.inventory.clone();
            pool.mint_proofs = result.mints.clone();
            pool.completion_reason = result.reason.clone();
            pool.last_changed_ms = pool.last_changed_ms.max(changed_at_ms);
            stats.applied += 1;
            match result.status {
                CompletionStatus::Prepared => stats.prepared += 1,
                CompletionStatus::Incomplete => stats.incomplete += 1,
                CompletionStatus::Retryable => (),
            }
            stats.updates.push(pool.clone());
        }
        stats
    }
}

#[derive(Default)]
struct CompletionStats {
    received: usize,
    applied: usize,
    prepared: usize,
    incomplete: usize,
    retryable: usize,
    stale: usize,
    invalid: usize,
    missing_pool: usize,
    updates: Vec<PoolState>,
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
    identity: Vec<Pubkey>,
}


/// Consume API observations and completion results through one Registry writer.
/// Keep running until both streams close: completion may finish after the API source.
pub async fn run(
    mut input: broadcast::Receiver<Arc<DataBatch>>,
    mut completion: Option<broadcast::Receiver<Arc<CompletionResultBatch>>>,
    registry: PoolRegistry,
    output: broadcast::Sender<Arc<PoolUpdateBatch>>,
    source_done: watch::Sender<bool>,
    fail_on_lag: bool,
) -> Result<()> {
    let mut batches = 0_u64;
    let mut input_records = 0_u64;
    let mut inserted = 0_u64;
    let mut changed = 0_u64;
    let mut unchanged = 0_u64;
    let mut stale = 0_u64;
    let mut conflicts = 0_u64;
    let mut lagged = 0_u64;
    let mut by_source = HashMap::<&'static str, u64>::new();
    let mut completion_received = 0_usize;
    let mut completion_applied = 0_usize;
    let mut completion_prepared = 0_usize;
    let mut completion_incomplete = 0_usize;
    let mut completion_retryable = 0_usize;
    let mut completion_rejected = 0_usize;
    let mut source_open = true;
    let mut completion_open = completion.is_some();
    let require_completion = completion_open;
    let bootstrap_snapshot = registry.snapshot();
    let bootstrap_prepared = bootstrap_snapshot.iter()
        .filter(|p| p.completion_state == CompletionState::Prepared).count();
    let bootstrap_incomplete = bootstrap_snapshot.iter()
        .filter(|p| p.completion_state == CompletionState::Incomplete).count();
    let bootstrap_manifests = bootstrap_snapshot.iter()
        .filter(|p| p.execution_manifest.is_some()).count();

    while source_open || completion_open {
        tokio::select! {
            event = input.recv(), if source_open => match event {
                Ok(batch) => {
                    batches += 1;
                    input_records += batch.records.len() as u64;
                    *by_source.entry(batch.source).or_default() += batch.records.len() as u64;
                    let stats = registry.apply(&batch);
                    inserted += stats.inserted as u64;
                    changed += stats.changed as u64;
                    unchanged += stats.unchanged as u64;
                    stale += stats.stale as u64;
                    conflicts += stats.conflicts as u64;
                    let updates = stats.updates.len();
                    if updates > 0 {
                        if output.send(Arc::new(PoolUpdateBatch {
                            kind: PoolUpdateKind::Source,
                            source: batch.source,
                            received_at_ms: batch.received_at_ms,
                            pools: stats.updates,
                            persist_identity: stats.identity,
                        })).is_err() {
                            eprintln!("pool registry update_broadcast_no_subscribers");
                        }
                    }
                    if batches == 1 || batches % 25 == 0 {
                        println!(
                            "pool registry batches={batches} source={} input_records={input_records} new={inserted} changed={changed} unchanged={unchanged} stale={stale} conflicts={conflicts} pools_in_ram={} updates_published={updates} ignored_mints={}",
                            batch.source, stats.total, stats.ignored_mints,
                        );
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    lagged += skipped;
                    eprintln!("pool registry source_lagged missed_batches={skipped}");
                    if fail_on_lag { anyhow::bail!("REST broadcast overflow: Registry lost {skipped} source batches"); }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    source_open = false;
                    // All Source updates have been forwarded. Completion may still
                    // produce results; this lifecycle signal breaks the channel cycle.
                    source_done.send_replace(true);
                },
            },
            event = async {
                completion.as_mut().expect("completion receiver exists").recv().await
            }, if completion_open => match event {
                Ok(batch) => {
                    let stats = registry.apply_completion(&batch, now_unix_ms()?);
                    completion_received += stats.received;
                    completion_applied += stats.applied;
                    completion_prepared += stats.prepared;
                    completion_incomplete += stats.incomplete;
                    completion_retryable += stats.retryable;
                    completion_rejected += stats.invalid + stats.stale + stats.missing_pool;
                    let updated = stats.updates.len();
                    println!("pool registry completion source={} received={} applied={} prepared={} incomplete={} retryable={} stale={} invalid={} missing_pool={} updates_published={}",
                        batch.source, stats.received, stats.applied, stats.prepared, stats.incomplete,
                        stats.retryable, stats.stale, stats.invalid, stats.missing_pool, updated);
                    if updated > 0 && output.send(Arc::new(PoolUpdateBatch {
                        kind: PoolUpdateKind::Completion,
                        source: batch.source,
                        received_at_ms: now_unix_ms()?,
                        pools: stats.updates,
                        persist_identity: Vec::new(),
                    })).is_err() {
                        eprintln!("pool registry completion_broadcast_no_subscribers");
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) =>
                    anyhow::bail!("pool registry completion_lagged missed_batches={skipped}"),
                Err(broadcast::error::RecvError::Closed) => completion_open = false,
            },
        }
    }

    let snapshot = registry.snapshot();
    let with_tvl = snapshot.iter().filter(|p| p.tvl_usd.is_some()).count();
    let prepared = snapshot.iter().filter(|p| p.completion_state == CompletionState::Prepared).count();
    let incomplete = snapshot.iter().filter(|p| p.completion_state == CompletionState::Incomplete).count();
    let manifests = snapshot.iter().filter(|p| p.execution_manifest.is_some()).count();
    let inventories = snapshot.iter().filter(|p| p.execution_inventory.is_some()).count();
    println!(
        "pool registry complete batches={batches} input_records={input_records} pools_in_ram={} with_tvl={with_tvl} new={inserted} changed={changed} unchanged={unchanged} stale={stale} conflicts={conflicts} lagged_batches={lagged} completion_received={completion_received} completion_applied={completion_applied} completion_prepared={completion_prepared} completion_incomplete={completion_incomplete} completion_retryable={completion_retryable} completion_rejected={completion_rejected} prepared_in_ram={prepared} incomplete_in_ram={incomplete} manifests_in_ram={manifests} inventories_in_ram={inventories}",
        snapshot.len(),
    );
    if require_completion {
        anyhow::ensure!(completion_received > 0 && completion_applied == completion_received
            && completion_rejected == 0,
            "registry completion live test did not accept all results");
        anyhow::ensure!(manifests == bootstrap_manifests + completion_prepared
            && prepared == bootstrap_prepared + completion_prepared
            && incomplete == bootstrap_incomplete + completion_incomplete,
            "registry RAM completion snapshot does not match restored and accepted results");
    }
    let mut source_counts = by_source.into_iter().collect::<Vec<_>>();
    source_counts.sort_by_key(|(source,_)|*source);
    let mut venue_counts = HashMap::<String,usize>::new();
    for pool in registry.snapshot() { *venue_counts.entry(format!("{:?}",pool.venue)).or_default() += 1; }
    let mut venue_counts = venue_counts.into_iter().collect::<Vec<_>>();
    venue_counts.sort_by(|a,b|a.0.cmp(&b.0));
    println!("pool registry discovery_sources={source_counts:?} unique_by_venue={venue_counts:?} strict_lag={fail_on_lag}");
    Ok(())
}

/// Temporary live-test subscriber: proves changed states reach a real consumer.
pub async fn observe_updates(
    mut input: broadcast::Receiver<Arc<PoolUpdateBatch>>,
    require_completion: bool,
    fail_on_lag: bool,
) -> Result<()> {
    let mut batches = 0_u64;
    let mut pool_updates = 0_u64;
    let mut lagged = 0_u64;
    let mut completion_batches = 0_u64;
    let mut completion_pools = 0_usize;
    let mut completion_prepared = 0_usize;
    let mut completion_incomplete = 0_usize;
    let mut completion_retryable = 0_usize;
    let mut completion_with_inventory = 0_usize;
    loop {
        match input.recv().await {
            Ok(batch) => {
                batches += 1;
                pool_updates += batch.pools.len() as u64;
                if batch.kind == PoolUpdateKind::Completion {
                    completion_batches += 1;
                    completion_pools += batch.pools.len();
                    let prepared = batch.pools.iter().filter(|p|
                        p.completion_state == CompletionState::Prepared
                        && p.execution_manifest.is_some()
                        && p.completion_validated_slot.is_some()).count();
                    let incomplete = batch.pools.iter().filter(|p|
                        p.completion_state == CompletionState::Incomplete
                        && p.execution_manifest.is_none()
                        && p.completion_validated_slot.is_some()).count();
                    let retryable = batch.pools.iter().filter(|p|
                        p.completion_state == CompletionState::Unknown
                        && p.execution_manifest.is_none()).count();
                    let with_inventory = batch.pools.iter()
                        .filter(|p|p.execution_inventory.is_some()).count();
                    completion_prepared += prepared;
                    completion_incomplete += incomplete;
                    completion_retryable += retryable;
                    completion_with_inventory += with_inventory;
                    println!("pool registry completion consumer source={} received={} prepared={} incomplete={} retryable={} inventories={} manifests={} sample_pool={} sample_slot={:?}",
                        batch.source, batch.pools.len(), prepared, incomplete, retryable, with_inventory,
                        batch.pools.iter().filter(|p|p.execution_manifest.is_some()).count(),
                        batch.pools.first().map_or(Pubkey::default(), |p|p.address),
                        batch.pools.first().and_then(|p|p.completion_validated_slot));
                }
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
                if fail_on_lag { anyhow::bail!("Registry broadcast overflow: observer lost {skipped} update batches"); }
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    println!(
        "pool registry consumer complete received_batches={batches} pool_updates={pool_updates} lagged_batches={lagged} completion_batches={completion_batches} completion_pools={completion_pools} completion_prepared={completion_prepared} completion_incomplete={completion_incomplete} completion_retryable={completion_retryable} completion_inventories={completion_with_inventory}"
    );
    if require_completion {
        anyhow::ensure!(completion_batches > 0 && completion_prepared > 0,
            "registry completion live proof missing prepared output");
        anyhow::ensure!(completion_pools == completion_prepared + completion_incomplete
            + completion_retryable, "registry completion consumer received invalid state updates");
        anyhow::ensure!(completion_with_inventory >= completion_prepared + completion_incomplete,
            "registry completion consumer missing execution inventory for terminal results");
    }
    Ok(())
}
