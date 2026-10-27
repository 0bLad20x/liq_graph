//! Pool registry: one authoritative in-memory state per pool address.
//! Reads decoded source batches; does not query RPC, persist, decode venue formats,
//! select graph routes, or retain arbitrary API payloads.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use anyhow::Result;
use serde_json::{json, Value};
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast,watch};

use crate::core::{
    now_unix_ms, CompletionResultBatch, CompletionStatus, DataBatch, RecordIdentity, Venue,
    CompletionState, CompletionAdmission, PoolState, PoolUpdateBatch, PoolUpdateKind,
};

/// Per-pool provenance. REST has priority while fresh; DexScreener fills missing
/// or stale protocol liquidity. Last-known persistence never implies freshness.
#[derive(Clone,Copy)]
struct Quote { value:f64, at:u64, source:&'static str }
#[derive(Default)]
struct PoolQuotes { protocol:Option<Quote>, dex:Option<Quote>, last_persisted:u64 }
const TVL_SOURCE_MAX_AGE_MS: u64 = 3_600_000;
const TVL_PERSIST_INTERVAL_MS: u64 = 900_000;
const DEX_SOURCE: &str = "dexscreener_token_pairs";
const MIN_NATIVE_TVL_USD: f64 = 10.0;

/// Native pool TVL sources; Manifest has no meaningful TVL and PumpSwap uses RPC.
fn native_source(venue: Venue) -> Option<&'static str> {
    match venue {
        Venue::MeteoraDlmm => Some("meteora_dlmm_api"),
        Venue::MeteoraDammV2 => Some("meteora_damm_v2_api"),
        Venue::RaydiumAmmV4 | Venue::RaydiumCpmm |
        Venue::RaydiumClmm | Venue::RaydiumStable => Some("raydium_api"),
        Venue::OrcaWhirlpool => Some("orca_whirlpool_api"),
        Venue::PancakeSwapClmm => Some("pancakeswap_api"),
        Venue::ByrealClmm => Some("byreal_api"),
        Venue::Manifest | Venue::PumpSwap => None,
    }
}
fn reference_mints() -> &'static [Pubkey; 2] {
    static EXCLUDED: std::sync::OnceLock<[Pubkey; 2]> = std::sync::OnceLock::new();
    EXCLUDED.get_or_init(||[
        "So11111111111111111111111111111111111111112".parse().expect("WSOL pubkey"),
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".parse().expect("USDC pubkey"),
    ])
}

/// Private RAM index, native TVL evidence independent of canonical/DexScreener TVL.
#[derive(Default)]
struct AdmissionIndex {
    qualified: HashSet<Pubkey>,
    pools_by_mint: HashMap<Pubkey, HashSet<Pubkey>>,
    native_tvl: HashMap<Pubkey, f64>,
}
impl AdmissionIndex {
    fn index(&mut self, pool: Pubkey, a: Option<Pubkey>, b: Option<Pubkey>) {
        for mint in [a, b].into_iter().flatten() {
            self.pools_by_mint.entry(mint).or_default().insert(pool);
        }
    }
    fn restore_native(&mut self, pool: &PoolState) {
        let Some(source)=native_source(pool.venue) else {return};
        let Some(observation)=pool.last_observation.as_deref() else {return};
        if observation.get("source").and_then(Value::as_str)!=Some(source) {return;}
        if let Some(value)=observation.get("tvl_usd").and_then(Value::as_f64)
            .filter(|v|v.is_finite() && *v>=0.0) {
            self.native_tvl.insert(pool.address,value);
        }
    }
    fn observe_native(&mut self, pool: Pubkey, venue: Venue,
        source: &str, value: Option<f64>) {
        if native_source(venue)!=Some(source) {return;}
        if let Some(value)=value {
            self.native_tvl.insert(pool,value);
        } else {
            self.native_tvl.remove(&pool);
        }
    }
    fn admission(&self, pool: &PoolState) -> CompletionAdmission {
        // A WSOL/USDC conversion pool contains only settlement reference mints.
        // It cannot satisfy the non-reference Jupiter qualification rule, yet it
        // is required by V10 Direct3/Triangle4 routes. Admit ONLY this exact
        // canonical reference pair; all other pools retain the Mint qualification
        // and venue-native minimum-TVL policy.
        let references=reference_mints();
        let conversion_pair=matches!((pool.mint_a,pool.mint_b),
            (Some(a),Some(b)) if a!=b && references.contains(&a) && references.contains(&b));
        let mint_qualified=conversion_pair || [pool.mint_a,pool.mint_b].into_iter().flatten()
            .any(|mint| !references.contains(&mint) && self.qualified.contains(&mint));
        let native_below_minimum=native_source(pool.venue).is_some()
            && self.native_tvl.get(&pool.address).is_some_and(|v|*v<MIN_NATIVE_TVL_USD);
        CompletionAdmission::from_qualification(
            pool.mint_a,pool.mint_b,mint_qualified,native_below_minimum)
    }
}


fn select_tvl(quotes:&PoolQuotes, now:u64)->Option<Quote> {
    let recent=|q:Quote|q.at<=now && now.saturating_sub(q.at)<=TVL_SOURCE_MAX_AGE_MS;
    quotes.protocol.filter(|q|recent(*q)).or_else(||quotes.dex.filter(|q|recent(*q)))
}
fn record_tvl(quotes:&mut HashMap<Pubkey,PoolQuotes>,address:Pubkey,
              source:&'static str,at:u64,value:Option<f64>)->Option<Quote> {
    let Some(value)=value else {
        return quotes.get(&address).and_then(|market|select_tvl(market,at));
    };
    let state=quotes.entry(address).or_default();
    let quote=Quote{value,at,source};
    let slot=if source==DEX_SOURCE {&mut state.dex} else {&mut state.protocol};
    if slot.is_none_or(|old|quote.at>=old.at) {*slot=Some(quote);}
    select_tvl(state,at)
}

/// Merge a normalized source patch. The source owns field names and explicit
/// removals; Registry owns ordering, provenance and persistence coalescing.
fn combine_metadata(previous: Option<&Arc<Value>>, incoming: &crate::core::PoolConfigUpdate,
    source: &str, received_at_ms: u64) -> Arc<Value> {
    let old = previous.and_then(|v|v.as_object()).filter(|m|
        m.get("schema_version").and_then(Value::as_u64) == Some(1)
        && m.get("source").and_then(Value::as_str) == Some(source));
    let same_observation = incoming.observation.is_some_and(|group|old.is_some_and(|old|
        incoming.values.iter().filter(|(k,_)|k.starts_with(group.value_prefix))
            .all(|(k,v)|old.get(k)==Some(v))));
    let recent_observation = incoming.observation.is_some_and(|group|old
        .and_then(|m|m.get(group.timestamp_key)).and_then(Value::as_u64)
        .is_some_and(|at|received_at_ms>=at && received_at_ms-at<TVL_PERSIST_INTERVAL_MS));
    let removed = |key:&str|incoming.remove_keys.contains(&key)
        || incoming.remove_prefixes.iter().any(|prefix|key.starts_with(prefix));
    let removal_needed = old.is_some_and(|old|old.keys().any(|key|removed(key)));
    if let Some(old) = old {
        if !removal_needed && incoming.values.iter().all(|(k,v)|old.get(k)==Some(v))
            && (incoming.observation.is_none() || same_observation && recent_observation) {
            return Arc::clone(previous.expect("existing normalized configuration"));
        }
    }
    let mut merged = old.cloned().unwrap_or_default();
    merged.retain(|key,_|!removed(key));
    merged.extend(incoming.values.iter().map(|(k,v)|(k.clone(),v.clone())));
    if let Some(group) = incoming.observation {
        if !(same_observation && recent_observation) {
            merged.insert(group.timestamp_key.into(),json!(received_at_ms));
        }
    }
    merged.insert("schema_version".into(),json!(1));
    merged.insert("source".into(),json!(source));
    Arc::new(Value::Object(merged))
}

/// The write owner is the registry worker. Consumers can only request typed snapshots.
#[derive(Clone, Default)]
pub struct PoolRegistry {
    entries: Arc<RwLock<HashMap<Pubkey, PoolState>>>,
    quotes: Arc<RwLock<HashMap<Pubkey,PoolQuotes>>>,
    admission: Arc<RwLock<AdmissionIndex>>,
}

impl PoolRegistry {
    pub fn snapshot(&self) -> Vec<PoolState> {
        self.entries.read().expect("pool registry lock poisoned")
            .values().cloned().collect()
    }

    pub fn get(&self, address: Pubkey) -> Option<PoolState> {
        self.entries.read().expect("pool registry lock poisoned")
            .get(&address).cloned()
    }

    pub fn live_tvl_count(&self) -> usize {
        self.entries.read().expect("pool registry lock poisoned")
            .values().filter(|p|p.tvl_usd.is_some()).count()
    }
    /// Live accounting of the reference pair needed for Direct3/Triangle4.
    /// Diagnostic only: the PoolRegistry remains the sole admission authority.
    fn report_conversion_admission(&self) {
        let references=reference_mints();
        let entries=self.entries.read().expect("pool registry lock poisoned");
        let mut known=0_usize;
        let mut admitted=0_usize;
        let mut prepared=0_usize;
        let mut prepared_admitted=0_usize;
        let mut native_veto=0_usize;
        let mut missing_qualification=0_usize;
        for p in entries.values() {
            let pair=matches!((p.mint_a,p.mint_b),
                (Some(a),Some(b)) if a!=b && references.contains(&a) && references.contains(&b));
            if !pair {continue;}
            known+=1;
            if p.completion_admission.is_admitted(){admitted+=1;}
            if p.completion_state==CompletionState::Prepared{prepared+=1;}
            if p.completion_admission.is_admitted() && p.completion_state==CompletionState::Prepared {
                prepared_admitted+=1;
            }
            if p.completion_admission==CompletionAdmission::NativeTvlBelowMinimum {native_veto+=1;}
            if p.completion_admission==CompletionAdmission::MintNotQualified {missing_qualification+=1;}
        }
        crate::log_event!(Info,
            "pool registry reference_conversion_pools={} admitted={} prepared={} prepared_admitted={} native_tvl_veto={} mint_not_qualified={}",
            known,admitted,prepared,prepared_admitted,native_veto,missing_qualification);
    }

    /// Bootstrap complete, previously validated state before any producer starts.
    /// REST observations still own current TVL/freshness; persisted proof is historical.
    pub fn restore(&self, pools: Vec<PoolState>) -> Result<usize> {
        use anyhow::ensure;
        let mut entries = self.entries.write().expect("pool registry lock poisoned");
        let mut admission = self.admission.write().expect("admission index lock poisoned");
        ensure!(entries.is_empty(), "Registry restore requires an empty RAM state");
        let mut restored = HashMap::with_capacity(pools.len());
        for mut pool in pools {
            admission.index(pool.address,pool.mint_a,pool.mint_b);
            admission.restore_native(&pool);
            pool.completion_admission=admission.admission(&pool);
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
                        && pool.completion_evidence.is_none()
                        && pool.mint_proofs.is_none(),
                        "restored Unknown pool has execution proof");
                }
                CompletionState::Prepared => {
                    let (Some(a), Some(b), Some(slot), Some(m)) =
                        (a, b, pool.completion_validated_slot, pool.execution_manifest.as_ref())
                    else { anyhow::bail!("restored Prepared pool missing identity or proof"); };
                    ensure!(m.pool == pool.address && m.mint_a == a && m.mint_b == b
                        && m.validated_slot == slot && !m.base_a.is_empty() && !m.base_b.is_empty()
                        && pool.completion_evidence.as_ref().is_some_and(|e|
                        !e.kind.is_empty() && e.version > 0 && e.payload.is_object())
                    && pool.mint_proofs.is_some(),
                        "invalid restored manifest identity, slot or protocol evidence");
                }
                CompletionState::Incomplete => {
                    ensure!(a.is_some() && b.is_some()
                        && pool.completion_validated_slot.is_some()
                        && pool.execution_manifest.is_none()
                        && pool.completion_evidence.as_ref().is_none_or(|e|
                            !e.kind.is_empty() && e.version > 0 && e.payload.is_object()),
                        "invalid restored Incomplete pool");
                }
            }
            ensure!(restored.insert(pool.address, pool).is_none(),
                "duplicate persisted pool address");
        }
        *entries = restored;
        Ok(entries.len())
    }

    /// Re-evaluate canonical TVL even when no new REST/DexScreener event arrives.
    /// Historical last_observation stays intact; expired quotes never masquerade
    /// as current liquidity. Called only by the Registry's single writer.
    fn refresh_tvl(&self, now:u64) -> (Vec<PoolState>,Vec<Pubkey>) {
        let mut entries=self.entries.write().expect("pool registry lock poisoned");
        let mut quotes=self.quotes.write().expect("pool market state lock poisoned");
        let mut updates=Vec::new();
        let mut persist=Vec::new();
        for (address, market) in quotes.iter_mut() {
            let selected=select_tvl(market,now);
            if let Some(pool)=entries.get_mut(address) {
                let source=pool.last_observation.as_ref().and_then(|o|o.get("source"))
                    .and_then(Value::as_str);
                if pool.tvl_usd!=selected.map(|q|q.value)
                    || selected.is_some_and(|q|source!=Some(q.source)) {
                    // DexScreener/canonical TVL expiry must not change native admission.
                    pool.tvl_usd=selected.map(|q|q.value);
                    if let Some(q)=selected {
                        pool.last_observation=Some(Arc::new(json!({
                            "tvl_usd":q.value,"source":q.source,
                            "observed_at_ms":q.at,"first_seen_ms":pool.first_seen_ms
                        })));
                        if market.last_persisted==0
                            || now.saturating_sub(market.last_persisted)>=TVL_PERSIST_INTERVAL_MS {
                            market.last_persisted=now;
                            persist.push(*address);
                        }
                    }
                    // Expiry only clears the live value; the DB retains history.
                    pool.last_changed_ms=now;
                    updates.push(pool.clone());
                }
            }
        }
        // Prevent indefinite growth from ever-seen, now-expired sources.
        quotes.retain(|_,q|q.protocol.is_some_and(|v|now.saturating_sub(v.at)<=TVL_SOURCE_MAX_AGE_MS)
             || q.dex.is_some_and(|v|now.saturating_sub(v.at)<=TVL_SOURCE_MAX_AGE_MS));
        (updates,persist)
    }

    /// Update only pools touched by changed qualified Mint identities.
    fn apply_qualified(&self, mints: &[Pubkey], now: u64) -> Vec<PoolState> {
        let mut entries=self.entries.write().expect("pool registry lock poisoned");
        let mut admission=self.admission.write().expect("admission index lock poisoned");
        let next:HashSet<Pubkey>=mints.iter().copied().collect();
        let changed=admission.qualified.symmetric_difference(&next)
            .copied().collect::<Vec<_>>();
        let mut affected=HashSet::new();
        for mint in &changed {
            if let Some(pools)=admission.pools_by_mint.get(mint) {
                affected.extend(pools.iter().copied());
            }
        }
        let affected_count=affected.len();
        admission.qualified=next;
        let mut updates=Vec::new();
        for address in affected {
            if let Some(pool)=entries.get_mut(&address) {
                let decision=admission.admission(pool);
                if decision!=pool.completion_admission {
                    let previous=pool.completion_admission;
                    crate::log_event!(Debug,"pool admission_transition event={}",
                        json!({
                            "pool":address.to_string(),"venue":format!("{:?}",pool.venue),
                            "source":"mint_qualification","received_at_ms":now,
                            "before":format!("{previous:?}"),"after":format!("{decision:?}"),
                            "native_tvl_usd":admission.native_tvl.get(&address).copied(),
                            "canonical_tvl_usd":pool.tvl_usd,
                            "completion":format!("{:?}",pool.completion_state),
                            "completion_reason":pool.completion_reason_code.map(|r|format!("{r:?}")),
                        }));
                    if (previous.is_admitted()&&!decision.is_admitted())
                        ||(previous==CompletionAdmission::NativeTvlBelowMinimum&&decision.is_admitted()){
                        crate::log_event!(Info,"pool admission_membership_change event={}",
                            json!({"pool":address.to_string(),
                                "source":"mint_qualification","at_ms":now,
                                "venue":format!("{:?}",pool.venue),
                                "before":format!("{previous:?}"),"after":format!("{decision:?}"),
                                "native_tvl_usd":admission.native_tvl.get(&address).copied(),
                                "canonical_tvl_usd":pool.tvl_usd}));
                    }
                    pool.completion_admission=decision;
                    pool.last_changed_ms=now;
                    updates.push(pool.clone());
                }
            }
        }
        crate::log_event!(Info,
            "pool registry mint_admission qualified={} changed_mints={} affected_pools={} admission_updates={}",
            admission.qualified.len(),changed.len(),affected_count,updates.len());
        updates
    }

    fn apply(&self, batch: &DataBatch) -> BatchStats {
        let mut stats = BatchStats::default();
        let mut entries = self.entries.write().expect("pool registry lock poisoned");
        let mut quotes = self.quotes.write().expect("pool market state lock poisoned");
        let mut admission = self.admission.write().expect("admission index lock poisoned");
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

            let observed_tvl = record.tvl_usd.filter(|v|v.is_finite() && *v>=0.0);
            match entries.get_mut(address) {
                None => {
                    let metadata=record.config.as_ref().filter(|_|native_source(*venue)==Some(batch.source));
                    admission.observe_native(*address,*venue,batch.source,observed_tvl);
                    let selected=record_tvl(&mut quotes,*address,batch.source,
                        batch.received_at_ms,observed_tvl);
                    let mut state = PoolState {
                        address: *address,
                        venue: *venue,
                        mint_a: *mint_a,
                        mint_b: *mint_b,
                        source: batch.source,
                        first_seen_ms: batch.received_at_ms,
                        last_seen_ms: batch.received_at_ms,
                        last_changed_ms: batch.received_at_ms,
                        tvl_usd: selected.map(|v|v.value),
                        completion_admission: CompletionAdmission::from_inputs(
                            *mint_a,*mint_b,None),
                        pool_config: metadata.map(|patch|combine_metadata(None,patch,batch.source,batch.received_at_ms)),
                        last_observation: selected.map(|v|Arc::new(json!({
                            "tvl_usd":v.value,"source":v.source,"observed_at_ms":v.at,
                            "first_seen_ms":batch.received_at_ms
                        }))),
                        completion_state: CompletionState::Unknown,
                        completion_validated_slot: None,
                        execution_manifest: None,
                        completion_evidence: None,
                        mint_proofs: None,
                        completion_reason: None,
                        completion_reason_code: None,
                        completion_checked_at_ms: None,
                    };
                    admission.index(*address,*mint_a,*mint_b);
                    state.completion_admission=admission.admission(&state);
                    updates.push(state.clone());
                    stats.identity.push(*address);
                    entries.insert(*address, state);
                    stats.inserted += 1;
                }
                Some(current) => {
                    // An older observation must not replace a newer one.
                    if batch.received_at_ms < current.last_seen_ms {
                        stats.stale += 1;
                        stats.metadata_skipped += 1;
                        continue;
                    }
                    // Pool ownership and known mints are stable facts. Reject
                    // identity conflicts instead of corrupting the registry.
                    if current.venue != *venue ||
                        mint_a.as_ref().is_some_and(|mint| current.mint_a.is_some_and(|old| old != *mint)) ||
                        mint_b.as_ref().is_some_and(|mint| current.mint_b.is_some_and(|old| old != *mint))
                    {
                        stats.conflicts += 1;
                        stats.metadata_skipped += 1;
                        crate::log_event!(Warn,"pool registry identity_conflict pool={address} source={}", batch.source);
                        continue;
                    }
                    let metadata=record.config.as_ref().filter(|_|native_source(*venue)==Some(batch.source));
                    admission.observe_native(*address,*venue,batch.source,observed_tvl);
                    let selected=record_tvl(&mut quotes,*address,batch.source,
                        batch.received_at_ms,observed_tvl);
                    let mut changed = false;
                    let mut identity_changed = false;
                    let mut mint_identity_changed = false;
                    if let Some(config)=metadata {
                        let config=combine_metadata(current.pool_config.as_ref(),config,batch.source,batch.received_at_ms);
                        if current.pool_config.as_ref().is_some_and(|old|Arc::ptr_eq(old,&config)) {
                            stats.metadata_reused += 1;
                        }
                        if current.pool_config.as_deref()!=Some(config.as_ref()) {
                            current.pool_config=Some(config);
                            changed=true;
                            identity_changed=true;
                        }
                    }
                    if current.mint_a.is_none() && mint_a.is_some() {
                        current.mint_a = *mint_a;
                        changed = true;
                        identity_changed = true;
                        mint_identity_changed = true;
                    }
                    if current.mint_b.is_none() && mint_b.is_some() {
                        current.mint_b = *mint_b;
                        changed = true;
                        identity_changed = true;
                        mint_identity_changed = true;
                    }
                    if let Some(chosen)=selected {
                        let prior=current.last_observation.as_ref();
                        let prior_source=prior.and_then(|o|o.get("source")).and_then(Value::as_str);
                        let value_changed=current.tvl_usd!=Some(chosen.value);
                        let source_changed=prior_source!=Some(chosen.source);
                        let tracking=quotes.get_mut(address).expect("quote was inserted");
                        let should_persist=tracking.last_persisted==0 ||
                            batch.received_at_ms.saturating_sub(tracking.last_persisted)>=TVL_PERSIST_INTERVAL_MS;
                        if value_changed || source_changed {
                            current.tvl_usd=Some(chosen.value);
                            changed=true;
                        }
                        current.last_observation=Some(Arc::new(json!({
                            "tvl_usd":chosen.value,"source":chosen.source,
                            "observed_at_ms":chosen.at,"first_seen_ms":current.first_seen_ms
                        })));
                        if should_persist {
                            identity_changed=true;
                            tracking.last_persisted=batch.received_at_ms;
                            changed=true;
                        }
                    }
                    if current.source != batch.source {
                        current.source = batch.source;
                        changed = true;
                    }

                    current.last_seen_ms = current.last_seen_ms.max(batch.received_at_ms);
                    if mint_identity_changed {
                        admission.index(current.address,current.mint_a,current.mint_b);
                    }
                    let previous_admission=current.completion_admission;
                    current.completion_admission=admission.admission(current);
                    if current.completion_admission!=previous_admission {
                        changed=true;
                        if (previous_admission.is_admitted()&&!current.completion_admission.is_admitted())
                            ||(previous_admission==CompletionAdmission::NativeTvlBelowMinimum
                                &&current.completion_admission.is_admitted()){
                            crate::log_event!(Info,"pool admission_membership_change event={}",
                                json!({"pool":current.address.to_string(),
                                    "source":batch.source,"at_ms":batch.received_at_ms,
                                    "venue":format!("{:?}",current.venue),
                                    "before":format!("{previous_admission:?}"),
                                    "after":format!("{:?}",current.completion_admission),
                                    "native_tvl_usd":admission.native_tvl.get(&current.address).copied(),
                                    "canonical_tvl_usd":current.tvl_usd}));
                        }
                        crate::log_event!(Debug,"pool admission_transition event={}",
                            json!({
                                "pool":current.address.to_string(),
                                "venue":format!("{:?}",current.venue),
                                "source":batch.source,"received_at_ms":batch.received_at_ms,
                                "before":format!("{previous_admission:?}"),
                                "after":format!("{:?}",current.completion_admission),
                                "native_tvl_usd":admission.native_tvl.get(&current.address).copied(),
                                "canonical_tvl_usd":current.tvl_usd,
                                "completion":format!("{:?}",current.completion_state),
                                "completion_reason":current.completion_reason_code.map(|r|format!("{r:?}")),
                                "manifest_retained":current.execution_manifest.is_some(),
                            }));
                    }
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
            // Retryable does not advance a validated slot. Account for the
            // accepted no-op before checking stale terminal completion proofs.
            if result.status == CompletionStatus::Retryable {
                if result.reason_code != Some(crate::core::CompletionReason::TransientFailure)
                    || result.checked_at_ms.is_some() || result.manifest.is_some() {
                    stats.invalid += 1;
                    continue;
                }
                // Transient RPC/validation failures must not overwrite previously
                // validated manifests or turn an Unknown pool into a terminal state.
                stats.retryable += 1;
                if pool.completion_state == CompletionState::Unknown {
                    pool.completion_reason = result.reason.clone();
                    pool.completion_reason_code = result.reason_code;
                    pool.last_changed_ms = pool.last_changed_ms.max(changed_at_ms);
                    stats.updates.push(pool.clone());
                    stats.applied += 1;
                } else {
                    // Transient failure during a repeat check must not erase an
                    // existing Incomplete/Prepared proof. This is an accepted
                    // no-op, not a rejected completion result.
                    stats.preserved_retryable += 1;
                }
                continue;
            }

            let accepted_slot = pool.completion_validated_slot;
            if result.validated_slot.is_some_and(|slot|
                accepted_slot.is_some_and(|previous| slot <= previous)) {
                stats.stale += 1;
                continue;
            }

            let Some(slot) = result.validated_slot else {
                stats.invalid += 1;
                continue;
            };
            if !result.checked_at_ms.is_some_and(|at|at>0 && at<=changed_at_ms)
                || (result.status==CompletionStatus::Prepared
                    && (result.reason_code.is_some() || result.reason.is_some()))
                || (result.status==CompletionStatus::Incomplete
                    && (result.reason_code.is_none()
                        || result.reason_code==Some(crate::core::CompletionReason::TransientFailure))) {
                stats.invalid += 1;
                continue;
            }
            if result.status == CompletionStatus::Prepared {
                let Some(manifest) = result.manifest.as_ref() else {
                    stats.invalid += 1;
                    continue;
                };
                if manifest.pool != result.pool || manifest.validated_slot != slot
                    || pool.mint_a != Some(manifest.mint_a)
                    || pool.mint_b != Some(manifest.mint_b)
                    || manifest.base_a.is_empty() || manifest.base_b.is_empty()
                    || result.evidence.is_none() || result.mints.as_ref().is_none_or(|m|
                        pool.mint_a!=Some(m[0].address) || pool.mint_b!=Some(m[1].address)) {
                    stats.invalid += 1;
                    continue;
                }
            } else if result.manifest.is_some() {
                stats.invalid += 1;
                continue;
            }
            if result.evidence.as_ref().is_some_and(|e|
                e.kind.is_empty() || e.version == 0 || !e.payload.is_object()) {
                stats.invalid += 1;
                continue;
            }

            let next_state = match result.status {
                CompletionStatus::Prepared => CompletionState::Prepared,
                CompletionStatus::Incomplete => CompletionState::Incomplete,
                CompletionStatus::Retryable => unreachable!("handled above"),
            };
            // Net transitions, not result counts: e.g. Incomplete -> Prepared
            // increases Prepared while decreasing Incomplete.
            match pool.completion_state {
                CompletionState::Prepared => stats.prepared_delta -= 1,
                CompletionState::Incomplete => stats.incomplete_delta -= 1,
                CompletionState::Unknown => (),
            }
            match next_state {
                CompletionState::Prepared => stats.prepared_delta += 1,
                CompletionState::Incomplete => stats.incomplete_delta += 1,
                CompletionState::Unknown => (),
            }
            if pool.execution_manifest.is_some() { stats.manifest_delta -= 1; }
            if result.manifest.is_some() { stats.manifest_delta += 1; }
            pool.completion_state = next_state;
            pool.completion_validated_slot = Some(slot);
            pool.execution_manifest = result.manifest.clone();
            pool.completion_evidence = result.evidence.clone();
            pool.mint_proofs = result.mints.clone();
            pool.completion_reason = result.reason.clone();
            pool.completion_reason_code = result.reason_code;
            pool.completion_checked_at_ms = result.checked_at_ms;
            crate::log_event!(Debug,
                "completion_contract pool={} venue={:?} state={:?} reason_code={:?} checked_at_ms={:?} slot={slot} admission={:?}",
                pool.address,pool.venue,pool.completion_state,pool.completion_reason_code,
                pool.completion_checked_at_ms,pool.completion_admission);
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
    preserved_retryable: usize,
    /// Signed net count changes after accepted terminal state transitions.
    prepared_delta: i64,
    incomplete_delta: i64,
    manifest_delta: i64,
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
    metadata_skipped: usize,
    metadata_reused: usize,
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
    mut qualified: Option<watch::Receiver<Arc<[Pubkey]>>>,
) -> Result<()> {
    let mut batches = 0_u64;
    let mut input_records = 0_u64;
    let mut inserted = 0_u64;
    let mut changed = 0_u64;
    let mut unchanged = 0_u64;
    let mut stale = 0_u64;
    let mut conflicts = 0_u64;
    let mut metadata_skipped = 0_u64;
    let mut metadata_reused = 0_u64;
    let mut lagged = 0_u64;
    let mut by_source = HashMap::<&'static str, u64>::new();
    let mut completion_received = 0_usize;
    let mut completion_applied = 0_usize;
    let mut completion_prepared = 0_usize;
    let mut completion_incomplete = 0_usize;
    let mut completion_retryable = 0_usize;
    let mut completion_preserved_retryable = 0_usize;
    let mut completion_rejected = 0_usize;
    let mut prepared_delta = 0_i64;
    let mut incomplete_delta = 0_i64;
    let mut manifest_delta = 0_i64;
    let mut source_open = true;
    let mut freshness=tokio::time::interval(std::time::Duration::from_secs(60));
    freshness.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
            event=async {
                match qualified.as_mut() {
                    Some(receiver)=>receiver.changed().await,
                    None=>std::future::pending().await,
                }
            }=>{
                match event {
                    Ok(())=>{
                        let next=qualified.as_mut().expect("qualified channel")
                            .borrow_and_update().clone();
                        let changed=registry.apply_qualified(&next,now_unix_ms()?);
                        for pools in changed.chunks(2048) {
                            output.send(Arc::new(PoolUpdateBatch{
                                kind:PoolUpdateKind::Source,
                                source:"mint_admission",
                                received_at_ms:now_unix_ms()?,
                                pools:pools.to_vec(),
                                persist_identity:Vec::new(),
                            })).map_err(|_|anyhow::anyhow!("Registry mint admission has no subscriber"))?;
                        }
                    }
                    Err(_)=>{
                        crate::log_event!(Warn,"pool registry mint_qualification channel_closed");
                        qualified=None;
                    }
                }
            },
            _=freshness.tick()=>{
                registry.report_conversion_admission();
                let (invalidated,persist)=registry.refresh_tvl(now_unix_ms()?);
                if !invalidated.is_empty() {
                    let count=invalidated.len();
                    output.send(Arc::new(PoolUpdateBatch {
                        kind:PoolUpdateKind::Source,
                        source:"tvl_freshness",
                        received_at_ms:now_unix_ms()?,
                        pools:invalidated,
                        persist_identity:persist,
                    })).map_err(|_|anyhow::anyhow!("Registry freshness update has no subscribers"))?;
                    crate::log_event!(Info,"pool registry tvl_freshness_changed pools={count}");
                }
            },
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
                    metadata_skipped += stats.metadata_skipped as u64;
                    metadata_reused += stats.metadata_reused as u64;
                    let updates = stats.updates.len();
                    if updates > 0 {
                        if output.send(Arc::new(PoolUpdateBatch {
                            kind: PoolUpdateKind::Source,
                            source: batch.source,
                            received_at_ms: batch.received_at_ms,
                            pools: stats.updates,
                            persist_identity: stats.identity,
                        })).is_err() {
                            crate::log_event!(Warn,"pool registry update_broadcast_no_subscribers");
                        }
                    }
                    if batches == 1 || batches % 25 == 0 {
                        crate::log_event!(Debug,
                            "pool registry batches={batches} source={} input_records={input_records} new={inserted} changed={changed} unchanged={unchanged} stale={stale} conflicts={conflicts} pools_in_ram={} updates_published={updates} ignored_mints={} metadata_skipped={metadata_skipped} metadata_reused={metadata_reused}",
                            batch.source, stats.total, stats.ignored_mints,
                        );
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    lagged += skipped;
                    crate::log_event!(Error,"pool registry source_lagged missed_batches={skipped}");
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
                    completion_preserved_retryable += stats.preserved_retryable;
                    completion_rejected += stats.invalid + stats.stale + stats.missing_pool;
                    prepared_delta += stats.prepared_delta;
                    incomplete_delta += stats.incomplete_delta;
                    manifest_delta += stats.manifest_delta;
                    let updated = stats.updates.len();
                    crate::log_event!(Debug,"pool registry completion source={} received={} applied={} preserved_retryable={} prepared={} incomplete={} retryable={} stale={} invalid={} missing_pool={} updates_published={}",
                        batch.source, stats.received, stats.applied, stats.preserved_retryable,
                        stats.prepared, stats.incomplete, stats.retryable, stats.stale, stats.invalid,
                        stats.missing_pool, updated);
                    if updated > 0 && output.send(Arc::new(PoolUpdateBatch {
                        kind: PoolUpdateKind::Completion,
                        source: batch.source,
                        received_at_ms: now_unix_ms()?,
                        pools: stats.updates,
                        persist_identity: Vec::new(),
                    })).is_err() {
                        crate::log_event!(Warn,"pool registry completion_broadcast_no_subscribers");
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
    let evidences = snapshot.iter().filter(|p| p.completion_evidence.is_some()).count();
    let admitted=snapshot.iter().filter(|p|p.completion_admission.is_admitted()).count();
    let prepared_admitted=snapshot.iter().filter(|p|p.completion_admission.is_admitted()
        && p.completion_state==CompletionState::Prepared).count();
    crate::log_event!(Info,
        "completion_contracts min_native_tvl_usd={} mint_qualification=true admitted_pools={admitted} prepared_admitted={prepared_admitted} prepared_total={prepared} incomplete_total={incomplete} refresh_scheduler=external",
        MIN_NATIVE_TVL_USD);
    crate::log_event!(Info,
        "pool registry complete batches={batches} input_records={input_records} pools_in_ram={} with_tvl={with_tvl} new={inserted} changed={changed} unchanged={unchanged} stale={stale} conflicts={conflicts} lagged_batches={lagged} completion_received={completion_received} completion_applied={completion_applied} completion_prepared={completion_prepared} completion_incomplete={completion_incomplete} completion_retryable={completion_retryable} completion_preserved_retryable={completion_preserved_retryable} completion_rejected={completion_rejected} prepared_in_ram={prepared} incomplete_in_ram={incomplete} manifests_in_ram={manifests} evidences_in_ram={evidences} metadata_skipped={metadata_skipped} metadata_reused={metadata_reused}",
        snapshot.len(),
    );
    if require_completion {
        // A completed discovery pass with no new eligible candidates is valid.
        // An accepted transient no-op preserves a validated older state and
        // counts towards accounted results without pretending it was applied.
        anyhow::ensure!(completion_applied + completion_preserved_retryable == completion_received
            && completion_rejected == 0,
            "registry completion live test did not accept all results");
        let expected_prepared = i64::try_from(bootstrap_prepared)? + prepared_delta;
        let expected_incomplete = i64::try_from(bootstrap_incomplete)? + incomplete_delta;
        let expected_manifests = i64::try_from(bootstrap_manifests)? + manifest_delta;
        crate::log_event!(Debug,"pool registry completion status_net prepared={prepared_delta:+} incomplete={incomplete_delta:+} manifests={manifest_delta:+} expected_prepared={expected_prepared} expected_incomplete={expected_incomplete} expected_manifests={expected_manifests}");
        anyhow::ensure!(i64::try_from(prepared)? == expected_prepared
            && i64::try_from(incomplete)? == expected_incomplete
            && i64::try_from(manifests)? == expected_manifests,
            "registry RAM completion snapshot does not match restored and accepted net transitions");
    }
    let mut source_counts = by_source.into_iter().collect::<Vec<_>>();
    source_counts.sort_by_key(|(source,_)|*source);
    let mut venue_counts = HashMap::<String,usize>::new();
    for pool in registry.snapshot() { *venue_counts.entry(format!("{:?}",pool.venue)).or_default() += 1; }
    let mut venue_counts = venue_counts.into_iter().collect::<Vec<_>>();
    venue_counts.sort_by(|a,b|a.0.cmp(&b.0));
    crate::log_event!(Info,"pool registry discovery_sources={source_counts:?} unique_by_venue={venue_counts:?} strict_lag={fail_on_lag}");
    Ok(())
}

#[cfg(test)]
mod tvl_freshness_tests {
    use super::*;
    #[test]
    fn fresh_protocol_preferred_then_dex_fallback_then_expiry() {
        let mut state=PoolQuotes::default();
        state.protocol=Some(Quote{value:800.0,at:100,source:"rest"});
        state.dex=Some(Quote{value:650.0,at:200,source:DEX_SOURCE});
        assert_eq!(select_tvl(&state,201).map(|q|q.value),Some(800.0));
        assert_eq!(select_tvl(&state,TVL_SOURCE_MAX_AGE_MS+101)
            .map(|q|q.value),Some(650.0));
        assert!(select_tvl(&state,TVL_SOURCE_MAX_AGE_MS+201).is_none());
    }
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
    let mut completion_with_evidence = 0_usize;
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
                    let with_evidence = batch.pools.iter()
                        .filter(|p|p.completion_evidence.is_some()).count();
                    completion_prepared += prepared;
                    completion_incomplete += incomplete;
                    completion_retryable += retryable;
                    completion_with_evidence += with_evidence;
                    crate::log_event!(Debug,"pool registry completion consumer source={} received={} prepared={} incomplete={} retryable={} evidences={} manifests={} sample_pool={} sample_slot={:?}",
                        batch.source, batch.pools.len(), prepared, incomplete, retryable, with_evidence,
                        batch.pools.iter().filter(|p|p.execution_manifest.is_some()).count(),
                        batch.pools.first().map_or(Pubkey::default(), |p|p.address),
                        batch.pools.first().and_then(|p|p.completion_validated_slot));
                }
                if batches == 1 {
                    if let Some(pool) = batch.pools.first() {
                        crate::log_event!(Debug,
                            "pool registry consumer source={} received_at_ms={} batch_pools={} sample_pool={} venue={:?} mint_a={:?} mint_b={:?} tvl_usd={:?} completion={:?} first_seen_ms={} last_seen_ms={} last_changed_ms={}",
                            batch.source, batch.received_at_ms, batch.pools.len(),
                            pool.address, pool.venue, pool.mint_a, pool.mint_b,
                            pool.tvl_usd, pool.completion_state, pool.first_seen_ms,
                            pool.last_seen_ms, pool.last_changed_ms,
                        );
                    }
                } else if batches % 25 == 0 {
                    crate::log_event!(Debug,
                        "pool registry consumer received_batches={batches} pool_updates={pool_updates}"
                    );
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                lagged += skipped;
                crate::log_event!(Warn,"pool registry consumer_lagged missed_batches={skipped}");
                if fail_on_lag { anyhow::bail!("Registry broadcast overflow: observer lost {skipped} update batches"); }
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    crate::log_event!(Info,
        "pool registry consumer complete received_batches={batches} pool_updates={pool_updates} lagged_batches={lagged} completion_batches={completion_batches} completion_pools={completion_pools} completion_prepared={completion_prepared} completion_incomplete={completion_incomplete} completion_retryable={completion_retryable} completion_evidences={completion_with_evidence}"
    );
    if require_completion {
        // No new qualified pools, or only Incomplete results, are valid outcomes.
        anyhow::ensure!(completion_pools == completion_prepared + completion_incomplete
            + completion_retryable, "registry completion consumer received invalid state updates");
        anyhow::ensure!(completion_with_evidence >= completion_prepared,
            "registry completion consumer missing protocol evidence for Prepared results");
    }
    Ok(())
}
