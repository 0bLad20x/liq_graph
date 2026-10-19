//! V10 liquidity graph: typed Registry updates -> immutable group catalog and inverted indices.
//! No RPC, database, protocol-account interpretation, signal producer, or transaction building.

use std::{
    collections::{HashMap, HashSet},
    hint::black_box,
    mem::size_of,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, watch}, task::JoinSet, time::{Instant as TokioInstant, sleep_until}};

use crate::core::{CompletionState, PoolState, PoolUpdateBatch};

const WSOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const V10_MAX_DECLARATIONS: usize = 16;

type Pair = (Pubkey, Pubkey);

#[derive(Clone, Copy, Debug)]
pub struct Settings;

impl Settings {
    pub fn from_env() -> Result<Self> {
        if std::env::var_os("GRAPH_MIN_TVL_USD").is_some() {
            crate::log_event!(Warn,"GRAPH_MIN_TVL_USD is ignored; Registry completion admission owns the common TVL gate");
        }
        Ok(Self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GroupKind { Direct, Bridge }

/// V10-compatible role template, NOT a packed instruction. Pools in each role are
/// alternatives and can exceed the 16-declaration instruction cap. The eventual
/// execution adapter must pack a valid subset, maximum four groups / 16 declarations.
#[derive(Clone, Debug)]
pub struct Group {
    pub settlement: Pubkey,
    pub target: Pubkey,
    pub bridge: Option<Pubkey>,
    pub intermediate: Option<Pubkey>,
    pub conversion: Option<Pubkey>,
    pub direct_s: Arc<[Pubkey]>,
    /// Only applicable to direct-only groups using Q/S conversion.
    pub direct_q: Arc<[Pubkey]>,
    /// B/S without conversion, B/Q when conversion is present.
    pub bridge_pools: Arc<[Pubkey]>,
    pub kind: GroupKind,
    /// Directed paths specific to this template; excludes direct roundtrips
    /// that are already counted by the standalone Direct group.
    pub route_options: u64,
}

impl Group {
    fn declarations(&self) -> usize {
        self.direct_s.len() + self.direct_q.len() + self.bridge_pools.len()
            + usize::from(self.intermediate.is_some())
            + usize::from(self.conversion.is_some())
    }

    /// Pool order is not group identity: only typed roles and their mint context
    /// distinguish V10 group templates. Alternatives are kept in sorted role lists.
    fn identity(&self) -> (Pubkey, Pubkey, GroupKind, Option<Pubkey>, Option<Pubkey>, Option<Pubkey>) {
        (self.settlement, self.target, self.kind,
            self.bridge, self.intermediate, self.conversion)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct PreparedPool {
    address: Pubkey,
    a: Pubkey,
    b: Pubkey,
}

fn eligible(pool: &PoolState) -> Option<PreparedPool> {
    let (Some(a), Some(b), Some(slot), Some(manifest)) =
        (pool.mint_a, pool.mint_b, pool.completion_validated_slot,
         pool.execution_manifest.as_ref()) else { return None };
    if !pool.completion_admission.is_admitted()
        || pool.completion_state != CompletionState::Prepared || a == b
        || a == Pubkey::default() || b == Pubkey::default()
        || manifest.pool != pool.address || manifest.mint_a != a
        || manifest.mint_b != b || manifest.validated_slot != slot
        || manifest.base_a.is_empty() || manifest.base_b.is_empty() {
        return None;
    }
    Some(PreparedPool { address: pool.address, a, b })
}

fn pair(a: Pubkey, b: Pubkey) -> Pair {
    if a < b { (a, b) } else { (b, a) }
}

fn pools_for(pairs: &HashMap<Pair, Arc<[Pubkey]>>, a: Pubkey, b: Pubkey) -> Arc<[Pubkey]> {
    pairs.get(&pair(a, b)).cloned().unwrap_or_else(|| Arc::from([]))
}

fn route_count(direct: usize, bridge: usize) -> u64 {
    2_u64.saturating_mul(direct as u64).saturating_mul(bridge as u64)
}

/// Validate every published template against the mint roles required by the public
/// V10 instruction contract. This validates topology, NOT a packed instruction:
/// a template may retain more than 16 pool alternatives for later selection.
fn validate_v10_group(group: &Group, pairs: &HashMap<Pair, Arc<[Pubkey]>>,
    wsol: Pubkey, usdc: Pubkey) -> Result<()> {
    let s = group.settlement;
    ensure!(s == wsol || s == usdc, "V10 group has unsupported settlement {s}");
    let q = if s == wsol { usdc } else { wsol };
    let t = group.target;
    ensure!(t != s && t != Pubkey::default(), "V10 group target equals settlement or zero");
    ensure!(!group.direct_s.is_empty()
        && group.direct_s.as_ref() == pools_for(pairs, t, s).as_ref(),
        "V10 group missing or incorrect settlement-quoted direct pools");
    for list in [&group.direct_s, &group.direct_q, &group.bridge_pools] {
        ensure!(list.windows(2).all(|p| p[0] < p[1]),
            "V10 group pool alternatives are not canonical and distinct");
    }
    if let Some(conversion) = group.conversion {
        ensure!(conversion != Pubkey::default()
            && pools_for(pairs, q, s).binary_search(&conversion).is_ok(),
            "V10 group conversion does not pair WSOL and USDC");
        ensure!(t != q, "V10 group target equals conversion quote");
    }
    match group.kind {
        GroupKind::Direct => {
            ensure!(group.bridge.is_none() && group.intermediate.is_none()
                && group.bridge_pools.is_empty(),
                "V10 direct template contains a triangle role");
            let direct_q = if group.conversion.is_some() {
                pools_for(pairs, t, q)
            } else { Arc::from([]) };
            ensure!(group.direct_q.as_ref() == direct_q.as_ref()
                && group.direct_s.len() + group.direct_q.len() >= 2,
                "V10 direct template requires at least two correctly quoted pools");
            let expected_routes = if group.conversion.is_some() {
                route_count(group.direct_s.len(), group.direct_q.len())
            } else {
                let n = group.direct_s.len() as u64;
                n.saturating_mul(n - 1)
            };
            ensure!(expected_routes > 0 && group.route_options == expected_routes,
                "V10 direct template has no valid 2/3-swap choices");
        }
        GroupKind::Bridge => {
            let b = group.bridge.context("V10 triangle missing bridge mint")?;
            let middle = group.intermediate.context("V10 triangle missing intermediate pool")?;
            ensure!(b != s && b != t && b != Pubkey::default()
                && group.direct_q.is_empty() && !group.bridge_pools.is_empty(),
                "V10 triangle has invalid bridge mint or pool roles");
            if group.conversion.is_some() {
                ensure!(b != q, "V10 triangle bridge equals conversion quote");
            }
            ensure!(pools_for(pairs, t, b).binary_search(&middle).is_ok(),
                "V10 triangle intermediate does not pair target and bridge");
            let bridge_quote = if group.conversion.is_some() { q } else { s };
            ensure!(group.bridge_pools.as_ref()
                == pools_for(pairs, b, bridge_quote).as_ref(),
                "V10 triangle bridge pools do not pair bridge and quote");
            let expected_routes = route_count(group.direct_s.len(), group.bridge_pools.len());
            ensure!(expected_routes > 0 && group.route_options == expected_routes,
                "V10 triangle requires an intermediate, a direct and a bridge choice");
        }
    }
    Ok(())
}

/// A generation owns every index; IDs must never cross generation boundaries.
#[derive(Debug, Default)]
pub struct GraphSnapshot {
    pub generation: u64,
    pub qualified_pools: usize,
    pub mint_pairs: usize,
    pub groups: Vec<Group>,
    pub token_index: HashMap<Pubkey, Vec<u32>>,
    pub pool_index: HashMap<Pubkey, Vec<u32>>,
    pub route_options: u64,
    pub role_pool_refs: usize,
    pub approx_index_bytes: usize,
}

impl GraphSnapshot {
    pub fn for_token(&self, mint: Pubkey) -> &[u32] {
        self.token_index.get(&mint).map_or(&[], Vec::as_slice)
    }
    pub fn for_pool(&self, pool: Pubkey) -> &[u32] {
        self.pool_index.get(&pool).map_or(&[], Vec::as_slice)
    }
    pub fn group(&self, id: u32) -> Option<&Group> {
        self.groups.get(id as usize)
    }
}

fn build(pools: impl Iterator<Item = PreparedPool>, generation: u64) -> Result<GraphSnapshot> {
    let mut edge_count = 0_usize;
    let mut pairs: HashMap<Pair, Vec<Pubkey>> = HashMap::new();
    let mut known_pools = HashSet::new();
    for p in pools {
        ensure!(known_pools.insert(p.address),
            "V10 graph received duplicate Prepared pool address {}", p.address);
        pairs.entry(pair(p.a, p.b)).or_default().push(p.address);
        edge_count += 1;
    }
    let mut adjacency: HashMap<Pubkey, HashSet<Pubkey>> = HashMap::new();
    let pair_map: HashMap<Pair, Arc<[Pubkey]>> = pairs.into_iter().map(|((a,b),mut list)| {
        list.sort_unstable();
        adjacency.entry(a).or_default().insert(b);
        adjacency.entry(b).or_default().insert(a);
        ((a,b), Arc::from(list))
    }).collect();

    let wsol: Pubkey = WSOL.parse()?;
    let usdc: Pubkey = USDC.parse()?;
    let settlements = [(wsol, usdc), (usdc, wsol)];
    let mut groups: Vec<Group> = Vec::new();

    for (s, q) in settlements {
        let mut targets: Vec<_> = adjacency.get(&s).into_iter()
            .flat_map(|neighbors| neighbors.iter().copied()).collect();
        targets.sort_unstable();
        for t in targets {
            if t == s { continue; }
            let direct_s = pools_for(&pair_map, t, s);
            if direct_s.len() >= 2 {
                let n = direct_s.len() as u64;
                groups.push(Group {
                    settlement:s, target:t, bridge:None, intermediate:None, conversion:None,
                    direct_s:direct_s.clone(), direct_q:Arc::from([]),
                    bridge_pools:Arc::from([]), kind:GroupKind::Direct,
                    route_options:n.saturating_mul(n-1),
                });
            }

            // Direct-only with Q/S conversion: one or more T/S and T/Q candidates.
            // Distinct pool pairs guarantee no same pool reused on any trade path.
            if t != q {
                let direct_q = pools_for(&pair_map, t, q);
                if !direct_s.is_empty() && !direct_q.is_empty() {
                    let conversions = pools_for(&pair_map, q, s);
                    for conversion in conversions.iter().copied() {
                        groups.push(Group {
                            settlement:s, target:t, bridge:None, intermediate:None,
                            conversion:Some(conversion), direct_s:direct_s.clone(),
                            direct_q:direct_q.clone(), bridge_pools:Arc::from([]),
                            kind:GroupKind::Direct,
                            route_options:route_count(direct_s.len(), direct_q.len()),
                        });
                    }
                }
            }

            // Bridge groups have exactly one T/B intermediate pool per group.
            let mut bridges: Vec<_> = adjacency.get(&t).into_iter()
                .flat_map(|neighbors| neighbors.iter().copied())
                .filter(|b| *b != s && *b != t).collect();
            bridges.sort_unstable();
            for b in bridges {
                let intermediate = pools_for(&pair_map, t, b);
                let return_s = pools_for(&pair_map, b, s);
                if !direct_s.is_empty() && !return_s.is_empty() {
                    for middle in intermediate.iter().copied() {
                        groups.push(Group {
                            settlement:s, target:t, bridge:Some(b), intermediate:Some(middle),
                            conversion:None, direct_s:direct_s.clone(), direct_q:Arc::from([]),
                            bridge_pools:return_s.clone(), kind:GroupKind::Bridge,
                            route_options:route_count(direct_s.len(), return_s.len()),
                        });
                    }
                }
                // With Q/S conversion, both T and B must differ from Q.
                if t != q && b != q && !direct_s.is_empty() {
                    let return_q = pools_for(&pair_map, b, q);
                    if return_q.is_empty() { continue; }
                    let conversions = pools_for(&pair_map, q, s);
                    for middle in intermediate.iter().copied() {
                        for conversion in conversions.iter().copied() {
                            groups.push(Group {
                                settlement:s, target:t, bridge:Some(b), intermediate:Some(middle),
                                conversion:Some(conversion), direct_s:direct_s.clone(),
                                direct_q:Arc::from([]), bridge_pools:return_q.clone(),
                                kind:GroupKind::Bridge,
                                route_options:route_count(direct_s.len(), return_q.len()),
                            });
                        }
                    }
                }
            }
        }
    }
    // Canonical identity is independent of alternative-pool input order.
    // The sorted role lists were built from each unique mint-pair set.
    groups.sort_unstable_by_key(Group::identity);
    let before_dedup = groups.len();
    groups.dedup_by(|a, b| {
        a.identity() == b.identity()
            && a.direct_s == b.direct_s
            && a.direct_q == b.direct_q
            && a.bridge_pools == b.bridge_pools
    });
    ensure!(groups.windows(2).all(|w| w[0].identity() != w[1].identity()),
        "V10 group identity collision with different pool alternatives");
    ensure!(groups.len() <= u32::MAX as usize, "V10 group count exceeds u32 index capacity");

    let mut direct_2 = 0_usize;
    let mut direct_3 = 0_usize;
    let mut triangle_3 = 0_usize;
    let mut triangle_4 = 0_usize;
    for g in &groups {
        validate_v10_group(g, &pair_map, wsol, usdc)?;
        match (g.kind, g.conversion.is_some()) {
            (GroupKind::Direct, false) => direct_2 += 1,
            (GroupKind::Direct, true) => direct_3 += 1,
            (GroupKind::Bridge, false) => triangle_3 += 1,
            (GroupKind::Bridge, true) => triangle_4 += 1,
        }
    }
    crate::log_event!(Debug,"validated generation={} groups={} direct_2={} direct_3={} triangle_3={} triangle_4={} canonical_duplicates_removed={} invalid_groups=0",
        generation, groups.len(), direct_2, direct_3, triangle_3, triangle_4,
        before_dedup - groups.len());

    let mut token_index: HashMap<Pubkey, Vec<u32>> = HashMap::new();
    let mut pool_index: HashMap<Pubkey, Vec<u32>> = HashMap::new();
    let mut routes = 0_u64;
    let mut refs = 0_usize;
    for (index, g) in groups.iter().enumerate() {
        let id = index as u32;
        let mut tokens = vec![g.settlement, g.target];
        if let Some(b) = g.bridge { tokens.push(b); }
        if g.conversion.is_some() { tokens.push(if g.settlement == wsol { usdc } else { wsol }); }
        tokens.sort_unstable();
        tokens.dedup();
        for mint in tokens { token_index.entry(mint).or_default().push(id); }
        for address in g.direct_s.iter().chain(g.direct_q.iter())
            .chain(g.bridge_pools.iter()).copied()
            .chain(g.intermediate).chain(g.conversion) {
            pool_index.entry(address).or_default().push(id);
            refs += 1;
        }
        routes = routes.saturating_add(g.route_options);
    }
    // Each role list contains unique pools, and repeated role group references are intentional.
    let index_items: usize = token_index.values().map(Vec::len).sum::<usize>()
        + pool_index.values().map(Vec::len).sum::<usize>();
    let approx = edge_count * size_of::<PreparedPool>()
        + groups.len() * size_of::<Group>()
        + refs * size_of::<Pubkey>()
        + index_items * size_of::<u32>();
    Ok(GraphSnapshot { generation, qualified_pools:edge_count,
        mint_pairs:pair_map.len(), groups, token_index, pool_index,
        route_options:routes, role_pool_refs:refs, approx_index_bytes:approx })
}

fn probe(snapshot: &GraphSnapshot) {
    let mut samples: Vec<(bool,Pubkey)> = snapshot.token_index.keys()
        .copied().take(32).map(|k|(true,k)).collect();
    samples.extend(snapshot.pool_index.keys().copied().take(32).map(|k|(false,k)));
    if samples.is_empty() {
        crate::log_event!(Debug,"liquidity graph lookup skipped=no_qualified_v10_groups");
        return;
    }
    let mut lookup_ns = Vec::with_capacity(4096);
    let mut resolve_ns = Vec::with_capacity(4096);
    let mut matched = 0_usize;
    // Real indexed addresses, including fetching up to four referenced templates.
    // This is NOT the future 16-pool V10 instruction-packing benchmark.
    for i in 0..4096 {
        let (token,key) = samples[i % samples.len()];
        let started = Instant::now();
        let ids = if token { snapshot.for_token(key) } else { snapshot.for_pool(key) };
        let looked_up = started.elapsed();
        let resolved = ids.iter().take(4)
            .filter_map(|id| snapshot.group(*id))
            .map(|group| group.declarations())
            .sum::<usize>();
        matched += black_box(resolved);
        lookup_ns.push(looked_up.as_nanos() as u64);
        resolve_ns.push(started.elapsed().as_nanos() as u64);
    }
    lookup_ns.sort_unstable();
    resolve_ns.sort_unstable();
    let percentile = |samples: &[u64], numer:usize| samples[((samples.len()-1)*numer)/100];
    crate::log_event!(Debug,"liquidity graph lookup generation={} samples={} resolved_pool_refs={} lookup_p50_ns={} lookup_p95_ns={} lookup_p99_ns={} lookup_max_ns={} resolve4_p50_ns={} resolve4_p95_ns={} resolve4_p99_ns={} resolve4_max_ns={}",
        snapshot.generation,lookup_ns.len(),matched,
        percentile(&lookup_ns,50),percentile(&lookup_ns,95),
        percentile(&lookup_ns,99),lookup_ns[lookup_ns.len()-1],
        percentile(&resolve_ns,50),percentile(&resolve_ns,95),
        percentile(&resolve_ns,99),resolve_ns[resolve_ns.len()-1]);
}

/// A single owner consumes Registry changes. Publishing an Arc replaces the
/// complete immutable generation atomically; no signal can read a partial catalog.
const REBUILD_SPACING: Duration = Duration::from_secs(3);

/// A single owner consumes every Registry update without waiting for a rebuild.
/// At most one full rebuild runs at once; changed edges are coalesced for the
/// next generation. An in-flight generation is immutable and can be superseded
/// by a later one after another three-second interval.
pub async fn run(
    mut input: broadcast::Receiver<Arc<PoolUpdateBatch>>,
    output: watch::Sender<Arc<GraphSnapshot>>,
    _settings: Settings,
) -> Result<()> {
    let mut prepared: HashMap<Pubkey, PreparedPool> = HashMap::new();
    let mut changed_batches = 0_u64;
    let mut observed = 0_usize;
    let mut generation = 0_u64;
    let mut dirty = false;
    let mut pending_changes = 0_usize;
    let mut pending_batches = 0_usize;
    let mut last_source = "persistence_restore";
    let mut next_rebuild = TokioInstant::now();
    let mut builds=JoinSet::<Result<(GraphSnapshot, u128, usize, usize, &'static str)>>::new();
    let mut source_open = true;
    loop {
        if !source_open && !dirty && builds.is_empty() { break; }
        tokio::select! {
            event = input.recv(), if source_open => match event {
                Ok(batch) => {
                    observed += batch.pools.len();
                    let mut changes = 0_usize;
                    for pool in &batch.pools {
                        let candidate = eligible(pool);
                        match candidate {
                            Some(p) => {
                                // TVL and slot changes in an existing edge do not
                                // require a new topology generation.
                                let same_edge = prepared.get(&p.address)
                                    .is_some_and(|old| old.a == p.a && old.b == p.b);
                                prepared.insert(p.address, p);
                                if !same_edge { changes += 1; }
                            }
                            None => {
                                if prepared.remove(&pool.address).is_some() { changes += 1; }
                            }
                        }
                    }
                    if changes > 0 {
                        dirty = true;
                        pending_changes += changes;
                        pending_batches += 1;
                        last_source = batch.source;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) =>
                    anyhow::bail!("liquidity graph lost {skipped} Registry update batches"),
                Err(broadcast::error::RecvError::Closed) => source_open = false,
            },
            result = builds.join_next(), if !builds.is_empty() => {
                let (graph, elapsed_us, changes, batches, source) =
                    result.context("missing liquidity graph build")?
                        .context("liquidity graph build task panicked")??;
                let direct = graph.groups.iter().filter(|g|g.kind==GroupKind::Direct).count();
                let bridge = graph.groups.len()-direct;
                let oversized = graph.groups.iter()
                    .filter(|g|g.declarations()>V10_MAX_DECLARATIONS).count();
                crate::log_event!(Info,
                    "rebuild generation={} source={} changed_pools={} coalesced_batches={} prepared={} pairs={} groups={} direct={} bridge={} trade_paths={} role_refs={} over_16_declarations={} approx_index_bytes={} build_us={}",
                    graph.generation, source, changes, batches, graph.qualified_pools, graph.mint_pairs,
                    graph.groups.len(), direct, bridge, graph.route_options, graph.role_pool_refs,
                    oversized, graph.approx_index_bytes, elapsed_us);
                output.send_replace(Arc::new(graph));
                changed_batches += 1;
            },
            _ = sleep_until(next_rebuild), if dirty && builds.is_empty() => {
                generation = generation.checked_add(1).context("graph generation overflow")?;
                let snapshot: Vec<PreparedPool> = prepared.values().copied().collect();
                let changed = std::mem::take(&mut pending_changes);
                let batches = std::mem::take(&mut pending_batches);
                let source = last_source;
                dirty = false;
                next_rebuild = TokioInstant::now() + REBUILD_SPACING;
                builds.spawn_blocking(move || {
                    let started = Instant::now();
                    let graph = build(snapshot.into_iter(), generation)?;
                    Ok((graph, started.elapsed().as_micros(), changed, batches, source))
                });
            },
        }
    }
    let final_graph = output.borrow().clone();
    crate::log_event!(Info,
        "complete observed_pool_updates={} changed_batches={} generation={} prepared={} groups={}",
        observed,changed_batches,final_graph.generation,
        final_graph.qualified_pools,final_graph.groups.len());

    Ok(())
}

/// Live consumer proves that the immutable RAM generation reaches a real reader.
pub async fn observe(mut input: watch::Receiver<Arc<GraphSnapshot>>) -> Result<()> {
    let mut observed = 0_u64;
    while input.changed().await.is_ok() {
        let snapshot = input.borrow_and_update().clone();
        observed += 1;
        probe(&snapshot);
        if let Some(group) = snapshot.groups.first() {
            crate::log_event!(Debug,"liquidity graph consumer generation={} groups={} sample_kind={:?} settlement={} target={} bridge={:?} intermediate={:?} conversion={:?} direct_s={} direct_q={} bridge_pools={} route_options={} direct_pool_samples={:?} bridge_pool_samples={:?}",
                snapshot.generation,snapshot.groups.len(),group.kind,group.settlement,group.target,
                group.bridge,group.intermediate,group.conversion,group.direct_s.len(),
                group.direct_q.len(),group.bridge_pools.len(),group.route_options,
                group.direct_s.iter().take(3).collect::<Vec<_>>(),
                group.bridge_pools.iter().take(3).collect::<Vec<_>>());
        }
    }
    crate::log_event!(Info,"liquidity graph consumer complete generations={observed}");
    Ok(())
}
