//! Compiled role plan: every affected concrete route retains its full ordered sum.
//! Dense directed prices; no best-price pruning, partial sums or event coalescing.
use std::{collections::{BTreeMap, HashMap}, env, sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use anyhow::{Context, Result, bail, ensure};
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast, watch};
use crate::core::{market_state::{MarketStateEvent, PoolPriceObservation, PriceEventStream, PriceOrigin},
    signals::{RouteEvaluation, SignalEvaluation, SignalEvent, SignalLeg, SignalLegs, SignalStream, SignalTrigger}};
use super::{graph_liquidity::GroupKind, graph_watch_catalog::WatchCatalog};

const WSOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

#[derive(Clone, Copy)]
pub struct Settings { pub threshold_bps: f64, pub sample_limit: usize, pub trace: bool, pub profile: bool }
impl Settings {
    pub fn from_env() -> Result<Self> {
        let threshold_bps: f64 = env::var("GRAPH_SIGNALS_THRESHOLD_BPS")
            .unwrap_or_else(|_| "0".into()).parse().context("invalid GRAPH_SIGNALS_THRESHOLD_BPS")?;
        ensure!(threshold_bps.is_finite() && threshold_bps >= 0.0,
            "GRAPH_SIGNALS_THRESHOLD_BPS must be finite and nonnegative");
        let sample_limit = env::var("GRAPH_SIGNALS_SAMPLE_LIMIT")
            .unwrap_or_else(|_| "16".into()).parse().context("invalid GRAPH_SIGNALS_SAMPLE_LIMIT")?;
        let trace = match env::var("GRAPH_SIGNALS_TRACE").as_deref() {
            Ok("1" | "true") => true,
            Ok("0" | "false") | Err(env::VarError::NotPresent) => false,
            _ => bail!("GRAPH_SIGNALS_TRACE must be true or false"),
        };
        let profile = match env::var("GRAPH_SIGNALS_PROFILE").as_deref() {
            Ok("1" | "true") => true,
            Ok("0" | "false") | Err(env::VarError::NotPresent) => false,
            _ => bail!("GRAPH_SIGNALS_PROFILE must be true or false"),
        };
        Ok(Self { threshold_bps, sample_limit, trace, profile })
    }
}

fn now_us() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros()
        .min(u128::from(u64::MAX)) as u64
}
fn nanos(start: Instant) -> u64 { start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64 }
fn kinds() -> BTreeMap<&'static str, u64> {
    ["direct_2", "direct_3", "triangle_3", "triangle_4"].into_iter().map(|k| (k, 0)).collect()
}
fn websocket(trigger: &SignalTrigger) -> bool {
    matches!(trigger, SignalTrigger::Price { origin: PriceOrigin::Websocket, .. })
}
fn account_age(trigger: &SignalTrigger, at: u64) -> Option<u64> {
    match trigger {
        SignalTrigger::Price { received_at_us, .. } => at.checked_sub(*received_at_us),
        SignalTrigger::Catalog { .. } => None,
    }
}

const EXECUTION_LAYOUT: &str = "indexed_role_plan_v1";
const BLOCK_ROUTES: usize = 256;
const YIELD_ROUTES: u64 = 4096;

#[derive(Clone, Copy)]
enum PriceState { Missing, Nonfinite, Available }
struct PoolIdentity { address: Pubkey, a: Pubkey, b: Pubkey }
#[derive(Default)]
struct Prices {
    index: HashMap<Pubkey, usize>, identities: Vec<PoolIdentity>,
    observations: Vec<Option<Arc<PoolPriceObservation>>>,
    // Two directed slots per pool: a -> b, then b -> a. Addresses/proofs stay cold.
    rates: Vec<f64>, weights: Vec<f64>, states: Vec<PriceState>,
}
impl Prices {
    fn bind(&mut self, address: Pubkey, a: Pubkey, b: Pubkey) -> Result<usize> {
        let pair = (a.min(b), a.max(b));
        if let Some(&id) = self.index.get(&address) {
            let known = &self.identities[id];
            ensure!((known.a, known.b) == pair, "catalog/price identity mismatch for {address}");
            return Ok(id);
        }
        ensure!(a != b, "pool {address} repeats mint");
        let id = self.identities.len();
        self.index.insert(address, id);
        self.identities.push(PoolIdentity { address, a: pair.0, b: pair.1 });
        self.observations.push(None);
        self.rates.extend([0.0; 2]); self.weights.extend([0.0; 2]);
        self.states.extend([PriceState::Missing; 2]);
        Ok(id)
    }
    fn observe(&mut self, p: &PoolPriceObservation) -> Result<(Pubkey, usize)> {
        let pool: Pubkey = p.pool.parse().context("signal pool address")?;
        let base: Pubkey = p.base_mint.parse().context("signal base mint")?;
        let quote: Pubkey = p.quote_mint.parse().context("signal quote mint")?;
        let a: Pubkey = p.mint_a.parse().context("signal mint A")?;
        let b: Pubkey = p.mint_b.parse().context("signal mint B")?;
        ensure!(a != b && ((a == base && b == quote) || (a == quote && b == base)),
            "price mint orientation mismatch for {pool}");
        let price = p.price_quote_per_base;
        ensure!(price.is_finite() && price > 0.0, "nonpositive/nonfinite indicative price for {pool}");
        let id = self.bind(pool, a, b)?;
        let forward = 2 * id + usize::from(base != self.identities[id].a);
        let log_price = price.ln();
        self.rates[forward] = price; self.weights[forward] = -log_price;
        self.rates[forward ^ 1] = 1.0 / price; self.weights[forward ^ 1] = log_price;
        for slot in [forward, forward ^ 1] {
            self.states[slot] = if self.rates[slot].is_finite() && self.rates[slot] > 0.0
                && self.weights[slot].is_finite() { PriceState::Available } else { PriceState::Nonfinite };
        }
        self.observations[id] = Some(Arc::new(p.clone()));
        Ok((pool, id))
    }
    fn leg(&self, address: Pubkey, input: Pubkey, output: Pubkey) -> Result<usize> {
        let &id = self.index.get(&address).context("graph pool outside compiled catalog")?;
        let p = &self.identities[id];
        ensure!((p.a, p.b) == (input.min(output), input.max(output)),
            "pool {address} does not connect graph route mints");
        Ok(2 * id + usize::from(input != p.a))
    }
    fn withdraw(&mut self, id: usize) {
        self.observations[id] = None;
        self.states[2 * id] = PriceState::Missing; self.states[2 * id + 1] = PriceState::Missing;
    }
    fn remove(&mut self, address: Pubkey) {
        if let Some(&id) = self.index.get(&address) { self.withdraw(id); }
    }
    fn retain(&mut self, selected: &std::collections::HashSet<Pubkey>) {
        for id in 0..self.identities.len() {
            if !selected.contains(&self.identities[id].address) { self.withdraw(id); }
        }
    }
    fn clear(&mut self) {
        self.observations.fill(None); self.states.fill(PriceState::Missing);
    }
}

#[derive(Clone, Copy)]
enum Affected { All, Both(usize), Left(usize), Right(usize), Fixed }
#[derive(Clone, Copy)]
struct Binding { group: usize, affected: Affected }
struct CompiledGroup {
    kind: usize, left: Arc<[usize]>, right: Arc<[usize]>, fixed: [usize; 2],
    mints: [Pubkey; 5], route_options: u64,
}
impl CompiledGroup {
    fn expected(&self, affected: Affected) -> u64 {
        match affected {
            Affected::All | Affected::Fixed => self.route_options,
            Affected::Both(_) => 2 * (self.left.len() as u64 - 1),
            Affected::Left(_) => 2 * self.right.len() as u64,
            Affected::Right(_) => 2 * self.left.len() as u64,
        }
    }
}
struct ExecutionPlan {
    catalog: Arc<WatchCatalog>, groups: Vec<CompiledGroup>,
    // CSR pool -> group/role/position bindings. No per-input copied group-ID list.
    offsets: Vec<usize>, bindings: Vec<Binding>,
}
type RoleCache = HashMap<(Pubkey, Pubkey), (Arc<[Pubkey]>, Arc<[usize]>)>;
fn compile_role(prices: &Prices, cache: &mut RoleCache, pools: &Arc<[Pubkey]>,
    input: Pubkey, output: Pubkey) -> Result<Arc<[usize]>> {
    if let Some((known, slots)) = cache.get(&(input, output)) {
        ensure!(known.as_ref() == pools.as_ref(), "graph role alternatives differ for identical mint pair");
        return Ok(slots.clone());
    }
    ensure!(!pools.is_empty() && pools.windows(2).all(|p| p[0] < p[1]),
        "graph role alternatives must be nonempty, sorted and unique");
    let slots: Arc<[usize]> = pools.iter().map(|&p| prices.leg(p, input, output))
        .collect::<Result<Vec<_>>>()?.into();
    cache.insert((input, output), (pools.clone(), slots.clone()));
    Ok(slots)
}
impl ExecutionPlan {
    fn compile(catalog: Arc<WatchCatalog>, prices: &mut Prices) -> Result<Self> {
        ensure!(catalog.generation == catalog.graph.generation, "mixed graph/catalog generation");
        ensure!(catalog.graph.groups.len() <= u32::MAX as usize, "too many signal groups");
        for pool in catalog.pools.iter() { prices.bind(pool.address, pool.mint_a, pool.mint_b)?; }
        let wsol: Pubkey = WSOL.parse()?; let usdc: Pubkey = USDC.parse()?;
        let mut roles = RoleCache::new();
        let mut groups = Vec::with_capacity(catalog.graph.groups.len());
        let mut by_pool = vec![Vec::<Binding>::new(); prices.identities.len()];
        for (id, g) in catalog.graph.groups.iter().enumerate() {
            let s = g.settlement; let t = g.target;
            ensure!(s == wsol || s == usdc, "unsupported graph settlement");
            let q = if s == wsol { usdc } else { wsol };
            let left = compile_role(prices, &mut roles, &g.direct_s, s, t)?;
            let mut right: Arc<[usize]> = Arc::from([]);
            let mut fixed = [0; 2];
            let (kind, length, mints) = match (g.kind, g.conversion) {
                (GroupKind::Direct, None) => {
                    ensure!(g.bridge.is_none() && g.intermediate.is_none()
                        && g.direct_q.is_empty() && g.bridge_pools.is_empty(), "invalid Direct2 roles");
                    (0, 2, [s, t, s, s, s])
                },
                (GroupKind::Direct, Some(conversion)) => {
                    ensure!(g.bridge.is_none() && g.intermediate.is_none()
                        && g.bridge_pools.is_empty(), "invalid Direct3 roles");
                    right = compile_role(prices, &mut roles, &g.direct_q, t, q)?;
                    fixed[0] = prices.leg(conversion, q, s)?;
                    (1, 3, [s, t, q, s, s])
                },
                (GroupKind::Bridge, conversion) => {
                    ensure!(g.direct_q.is_empty(), "triangle contains direct quote role");
                    let b = g.bridge.context("bridge group missing bridge mint")?;
                    fixed[0] = prices.leg(g.intermediate.context("bridge group missing intermediate pool")?, t, b)?;
                    if let Some(conversion) = conversion {
                        right = compile_role(prices, &mut roles, &g.bridge_pools, b, q)?;
                        fixed[1] = prices.leg(conversion, q, s)?;
                        (3, 4, [s, t, b, q, s])
                    } else {
                        right = compile_role(prices, &mut roles, &g.bridge_pools, b, s)?;
                        (2, 3, [s, t, b, s, s])
                    }
                },
            };
            // Distinct cycle mints imply disjoint pool roles for 3/4-leg routes.
            for i in 0..length {
                ensure!(!mints[..i].contains(&mints[i]), "graph route repeats a cycle mint");
            }
            let n = left.len() as u64;
            let expected = (if kind == 0 {
                ensure!(n >= 2, "Direct2 needs distinct pools"); n.checked_mul(n - 1)
            } else { n.checked_mul(right.len() as u64).and_then(|v| v.checked_mul(2)) })
                .context("graph route count overflow")?;
            ensure!(expected == g.route_options, "compiled route count differs from graph group {id}");
            for (position, &slot) in left.iter().enumerate() {
                by_pool[slot / 2].push(Binding { group: id,
                    affected: if kind == 0 { Affected::Both(position) } else { Affected::Left(position) } });
            }
            for (position, &slot) in right.iter().enumerate() {
                by_pool[slot / 2].push(Binding { group: id, affected: Affected::Right(position) });
            }
            let fixed_count = match kind { 0 => 0, 3 => 2, _ => 1 };
            for &slot in &fixed[..fixed_count] {
                by_pool[slot / 2].push(Binding { group: id, affected: Affected::Fixed });
            }
            groups.push(CompiledGroup { kind, left, right, fixed, mints, route_options: expected });
        }
        let mut offsets = Vec::with_capacity(by_pool.len() + 1);
        let mut bindings = Vec::new();
        offsets.push(0);
        for (id, entries) in by_pool.into_iter().enumerate() {
            ensure!(entries.iter().map(|b| b.group as u32)
                .eq(catalog.graph.for_pool(prices.identities[id].address).iter().copied()),
                "compiled pool bindings differ from graph index");
            bindings.extend(entries); offsets.push(bindings.len());
        }
        Ok(Self { catalog, groups, offsets, bindings })
    }
    fn for_pool(&self, pool: usize) -> &[Binding] {
        match (self.offsets.get(pool), self.offsets.get(pool + 1)) {
            (Some(&start), Some(&end)) => &self.bindings[start..end], _ => &[],
        }
    }
}

// Enumerate only affected choices, preserving the former filtered nested-loop order.
struct Pairs { row: usize, end: usize, col: usize, start_col: usize, end_col: usize,
    distinct: bool, changed: Option<usize> }
impl Pairs {
    fn new(g: &CompiledGroup, affected: Affected) -> Self {
        let (row, end) = if let Affected::Left(i) = affected { (i, i + 1) } else { (0, g.left.len()) };
        let columns = if g.kind == 0 { g.left.len() } else { g.right.len() };
        let (col, end_col) = if let Affected::Right(i) = affected { (i, i + 1) } else { (0, columns) };
        Self { row, end, col, start_col: col, end_col, distinct: g.kind == 0,
            changed: if let Affected::Both(i) = affected { Some(i) } else { None } }
    }
}
impl Iterator for Pairs {
    type Item = (usize, usize);
    fn next(&mut self) -> Option<Self::Item> {
        while self.row < self.end {
            if let Some(changed) = self.changed {
                if self.row != changed {
                    let row = self.row; self.row += 1; self.col = self.start_col;
                    return Some((row, changed));
                }
            }
            if self.col == self.end_col { self.row += 1; self.col = self.start_col; continue; }
            let pair = (self.row, self.col); self.col += 1;
            if !self.distinct || pair.0 != pair.1 { return Some(pair); }
        }
        None
    }
}
struct Route<const N: usize> { slots: [usize; N], reverse: bool }
struct Scored { sum: f64, bps: f64 }
enum Calculation { Missing, Nonfinite, Scored(Scored) }
struct Pending { slots: [usize; 4], reverse: bool, scored: Scored }
struct Work {
    stats: SignalEvaluation, by_kind: [u64; 4], pending: Vec<Pending>,
    ready: Vec<Arc<SignalEvent>>, last_yield: u64,
}
struct InputTiming { started: Instant, received_us: u64, preparation_ns: u64, logarithms: u64, plan_ns: u64 }
struct Engine {
    prices: Prices, output: broadcast::Sender<Arc<SignalEvent>>, scratch: Vec<Pending>,
    ready: Vec<Arc<SignalEvent>>, production: ProductionReport,
    settings: Settings, sequence: u64, samples: usize, websocket_samples: usize, threshold_log: f64,
}
impl Engine {
    fn publish(&mut self, event: Arc<SignalEvent>) -> Result<()> {
        self.output.send(event).map_err(|_| anyhow::anyhow!("signal output has no consumer"))?;
        self.production.published_events += 1;
        Ok(())
    }
    fn emit(&mut self, event: SignalEvent) -> Result<()> { self.publish(Arc::new(event)) }
    fn calculate<const N: usize>(&self, slots: &[usize; N]) -> Calculation {
        let mut sum = 0.0;
        for &slot in slots {
            match self.prices.states[slot] {
                PriceState::Missing => return Calculation::Missing,
                PriceState::Nonfinite => return Calculation::Nonfinite,
                PriceState::Available => sum += self.prices.weights[slot],
            }
        }
        let bps = (-sum).exp_m1() * 10_000.0;
        if !sum.is_finite() || !bps.is_finite() { return Calculation::Nonfinite; }
        Calculation::Scored(Scored { sum, bps })
    }
    async fn routes<const N: usize>(&mut self, id: usize, plan: &ExecutionPlan, work: &mut Work,
        mut routes: impl Iterator<Item = Route<N>>) -> Result<()> {
        let compiled = &plan.groups[id]; let group = &plan.catalog.graph.groups[id];
        let live = websocket(&work.stats.trigger);
        loop {
            let started = Instant::now();
            let before = work.stats.combinations_visited;
            let samples = if live { self.websocket_samples } else { self.samples };
            let mut pending_samples = 0;
            for route in routes.by_ref().take(BLOCK_ROUTES) {
                work.stats.combinations_visited += 1;
                let scored = match self.calculate(&route.slots) {
                    Calculation::Missing => { work.stats.routes_missing_prices += 1; continue; },
                    Calculation::Nonfinite => { work.stats.routes_nonfinite += 1; continue; },
                    Calculation::Scored(s) => s,
                };
                work.stats.routes_scored += 1; work.by_kind[compiled.kind] += 1;
                let qualifies = scored.sum < self.threshold_log;
                if qualifies { work.stats.candidates += 1; }
                if qualifies || pending_samples < self.settings.sample_limit.saturating_sub(samples) {
                    if !qualifies { pending_samples += 1; }
                    let mut slots = [0; 4]; slots[..N].copy_from_slice(&route.slots);
                    work.pending.push(Pending { slots, reverse: route.reverse, scored });
                }
            }
            work.stats.calculation_ns = work.stats.calculation_ns.saturating_add(nanos(started));
            if before == work.stats.combinations_visited { break; }
            if !work.pending.is_empty() {
                let started = Instant::now();
                for pending in work.pending.drain(..) {
                    let leg = |i: usize| {
                        let slot = pending.slots[i];
                        let input = if pending.reverse { N - i } else { i };
                        let output = if pending.reverse { N - i - 1 } else { i + 1 };
                        SignalLeg { input_mint: compiled.mints[input], output_mint: compiled.mints[output],
                            indicative_rate: self.prices.rates[slot], negative_log_weight: self.prices.weights[slot],
                            observation: Arc::clone(self.prices.observations[slot / 2].as_ref().expect("scored pool observation")) }
                    };
                    let legs = match N {
                        2 => SignalLegs::Two(std::array::from_fn(leg)),
                        3 => SignalLegs::Three(std::array::from_fn(leg)),
                        4 => SignalLegs::Four(std::array::from_fn(leg)),
                        _ => unreachable!("compiled route length"),
                    };
                    self.sequence += 1;
                    let emitted_at_us = now_us();
                    let qualifies = pending.scored.sum < self.threshold_log;
                    let route = RouteEvaluation { sequence: self.sequence, graph_generation: work.stats.graph_generation,
                        group_id: id as u32, trigger: Arc::clone(&work.stats.trigger), settlement: group.settlement,
                        target: group.target, bridge: group.bridge, conversion_pool: group.conversion, legs,
                        negative_log_sum: pending.scored.sum, indicative_return_bps: pending.scored.bps,
                        threshold_bps: self.settings.threshold_bps, qualifies, emitted_at_us,
                        account_to_emitted_us: account_age(&work.stats.trigger, emitted_at_us) };
                    if qualifies { work.ready.push(Arc::new(SignalEvent::Candidate(route))); }
                    else {
                        if live { self.websocket_samples += 1; } else { self.samples += 1; }
                        work.ready.push(Arc::new(SignalEvent::RouteSample(route)));
                    }
                }
                let built = nanos(started);
                let publishing = Instant::now();
                for event in work.ready.drain(..) { self.publish(event)?; }
                self.production.max_output_queue_depth = self.production.max_output_queue_depth.max(self.output.len());
                let published = nanos(publishing);
                work.stats.event_build_ns = work.stats.event_build_ns.saturating_add(built);
                work.stats.publication_ns = work.stats.publication_ns.saturating_add(published);
                work.stats.output_ns = work.stats.output_ns.saturating_add(built.saturating_add(published));
            }
            if work.stats.combinations_visited - work.last_yield >= YIELD_ROUTES {
                let started = Instant::now();
                tokio::task::yield_now().await;
                work.stats.cooperative_yield_ns = work.stats.cooperative_yield_ns.saturating_add(nanos(started));
                work.last_yield = work.stats.combinations_visited;
            }
        }
        Ok(())
    }
    async fn group(&mut self, id: usize, affected: Affected, plan: &ExecutionPlan, work: &mut Work) -> Result<()> {
        let g = &plan.groups[id]; let pairs = Pairs::new(g, affected);
        let before = work.stats.combinations_visited; let expected = g.expected(affected);
        work.stats.groups_visited += 1; work.stats.combinations_available += g.route_options;
        work.stats.routes_expected += expected;
        match g.kind {
            0 => self.routes(id, plan, work, pairs.map(|(i,j)| Route { slots: [g.left[i], g.left[j] ^ 1], reverse: false })).await?,
            1 => self.routes(id, plan, work, pairs.flat_map(|(i,j)| {
                let a = g.left[i]; let b = g.right[j]; let c = g.fixed[0];
                [Route { slots: [a,b,c], reverse: false }, Route { slots: [c^1,b^1,a^1], reverse: true }]
            })).await?,
            2 => self.routes(id, plan, work, pairs.flat_map(|(i,j)| {
                let a = g.left[i]; let m = g.fixed[0]; let b = g.right[j];
                [Route { slots: [a,m,b], reverse: false }, Route { slots: [b^1,m^1,a^1], reverse: true }]
            })).await?,
            3 => self.routes(id, plan, work, pairs.flat_map(|(i,j)| {
                let a = g.left[i]; let m = g.fixed[0]; let b = g.right[j]; let c = g.fixed[1];
                [Route { slots: [a,m,b,c], reverse: false }, Route { slots: [c^1,b^1,m^1,a^1], reverse: true }]
            })).await?,
            _ => unreachable!("compiled route kind"),
        }
        ensure!(work.stats.combinations_visited - before == expected, "compiled affected-route count differs in group {id}");
        Ok(())
    }
    async fn evaluate(&mut self, plan: Arc<ExecutionPlan>, trigger: SignalTrigger,
        changed: Option<usize>, price_queue_depth: usize, timing: InputTiming) -> Result<()> {
        let mut work = Work { stats: SignalEvaluation { sequence: 0, graph_generation: plan.catalog.generation,
            account_to_worker_us: account_age(&trigger, timing.received_us), trigger: Arc::new(trigger),
            groups_visited: 0, combinations_available: 0, routes_expected: 0, combinations_visited: 0,
            routes_affected: 0, routes_scored: 0, routes_missing_prices: 0, routes_nonfinite: 0,
            candidates: 0, scored_by_kind: kinds(), price_queue_depth,
            price_preparation_ns: timing.preparation_ns, logarithms_prepared: timing.logarithms,
            plan_build_ns: timing.plan_ns, event_build_ns: 0, publication_ns: 0,
            cooperative_yield_ns: 0, calculation_ns: 0, output_ns: 0, processing_ns: 0, completed_at_us: 0 },
            by_kind: [0; 4], pending: std::mem::take(&mut self.scratch),
            ready: std::mem::take(&mut self.ready), last_yield: 0 };
        if let Some(pool) = changed {
            for binding in plan.for_pool(pool) {
                self.group(binding.group, binding.affected, &plan, &mut work).await?;
            }
        } else {
            for id in 0..plan.groups.len() { self.group(id, Affected::All, &plan, &mut work).await?; }
        }
        let stats = &mut work.stats;
        stats.routes_affected = stats.combinations_visited;
        ensure!(stats.routes_affected == stats.routes_expected && stats.routes_affected ==
            stats.routes_scored + stats.routes_missing_prices + stats.routes_nonfinite, "incomplete indexed calculation accounting");
        for (name, count) in ["direct_2", "direct_3", "triangle_3", "triangle_4"].into_iter().zip(work.by_kind) {
            stats.scored_by_kind.insert(name, count);
        }
        self.scratch = work.pending; self.ready = work.ready;
        self.sequence += 1; stats.sequence = self.sequence; stats.processing_ns = nanos(timing.started);
        stats.completed_at_us = now_us();
        self.production.record(stats);
        self.emit(SignalEvent::Evaluated(work.stats))
    }
}

pub async fn run(mut input: PriceEventStream, mut catalogs: watch::Receiver<Arc<WatchCatalog>>,
    output: broadcast::Sender<Arc<SignalEvent>>, settings: Settings) -> Result<ProductionReport> {
    let mut engine = Engine { prices: Prices::default(), output, scratch: Vec::with_capacity(BLOCK_ROUTES),
        ready: Vec::with_capacity(BLOCK_ROUTES), production: ProductionReport::default(),
        settings, sequence: 0, samples: 0, websocket_samples: 0,
        threshold_log: -(settings.threshold_bps / 10_000.0).ln_1p() };
    let outcome: Result<()> = async {
    let mut plan = Arc::new(ExecutionPlan::compile(catalogs.borrow_and_update().clone(), &mut engine.prices)?);
    let mut catalog_open = true;
    crate::log_event!(Info, "graph signals started=true execution_layout={} profiling={} output_layout=shared_fixed_legs_v1 threshold_bps={} execution=false", EXECUTION_LAYOUT, settings.profile, settings.threshold_bps);
    loop { tokio::select! {
        change = catalogs.changed(), if catalog_open => {
            if change.is_err() { catalog_open = false; continue; }
            let started = Instant::now(); let received_us = now_us();
            let next = catalogs.borrow_and_update().clone();
            if next.generation == plan.catalog.generation { continue; }
            let compile_started = Instant::now();
            let next = Arc::new(ExecutionPlan::compile(next, &mut engine.prices)?);
            let plan_ns = nanos(compile_started);
            engine.emit(SignalEvent::Invalidated { graph_generation: next.catalog.generation, pool: None,
                reason: "graph generation replaced; group IDs belong to the new catalog".into() })?;
            plan = next;
            engine.evaluate(plan.clone(), SignalTrigger::Catalog { generation: plan.catalog.generation },
                None, input.len(), InputTiming { started, received_us, preparation_ns: 0, logarithms: 0, plan_ns }).await?;
        },
        received = input.recv() => match received {
            Ok(event) => match event.as_ref() {
                MarketStateEvent::Price(p) => {
                    let started = Instant::now(); let received_us = now_us();
                    let preparation = Instant::now();
                    let (pool, id) = engine.prices.observe(p)?;
                    let preparation_ns = nanos(preparation);
                    let trigger = SignalTrigger::Price { pool, sequence: p.sequence,
                        origin: p.origin, received_at_us: p.received_at_us };
                    engine.evaluate(plan.clone(), trigger, Some(id), input.len(),
                        InputTiming { started, received_us, preparation_ns, logarithms: 1, plan_ns: 0 }).await?;
                },
                MarketStateEvent::Unavailable { pool, reason, .. } => {
                    engine.prices.remove(pool.parse::<Pubkey>()?);
                    engine.emit(SignalEvent::Invalidated { graph_generation: plan.catalog.generation,
                        pool: Some(pool.clone()), reason: reason.clone() })?;
                },
                MarketStateEvent::Removed { pool, .. } => {
                    engine.prices.remove(pool.parse::<Pubkey>()?);
                    engine.emit(SignalEvent::Invalidated { graph_generation: plan.catalog.generation,
                        pool: Some(pool.clone()), reason: "pool removed from observation".into() })?;
                },
                MarketStateEvent::Gap { reason, .. } => {
                    engine.prices.clear();
                    engine.emit(SignalEvent::Invalidated { graph_generation: plan.catalog.generation,
                        pool: None, reason: reason.clone() })?;
                },
                MarketStateEvent::Selection { selected, .. } => {
                    let selected: std::collections::HashSet<Pubkey> = selected.iter()
                        .map(|p| p.parse()).collect::<std::result::Result<_, _>>()?;
                    engine.prices.retain(&selected);
                },
                MarketStateEvent::Stats { .. } => {},
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                engine.prices.clear();
                engine.emit(SignalEvent::Invalidated { graph_generation: plan.catalog.generation,
                    pool: None, reason: format!("price input lagged {n}; coverage invalid") })?;
                bail!("graph signals lost {n} price events; run invalid");
            },
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }}
    Ok(())
    }.await;
    engine.production.error = outcome.err().map(|e| format!("{e:#}"));
    crate::log_event!(Info, "graph signals producer complete={} published={} evaluations={} ws_evaluations={} candidates={} max_output_queue={} error={:?}",
        engine.production.error.is_none(), engine.production.published_events, engine.production.evaluations,
        engine.production.websocket_evaluations, engine.production.candidates,
        engine.production.max_output_queue_depth, engine.production.error);
    Ok(engine.production)
}

/// Fixed logarithmic histogram: bounded measurement memory, no route-result cache.
#[derive(Clone)]
struct Histogram { bins: [u64; 64], count: u64, total: u128, min: u64, max: u64 }
impl Default for Histogram {
    fn default() -> Self { Self { bins: [0; 64], count: 0, total: 0, min: u64::MAX, max: 0 } }
}
impl Histogram {
    fn add(&mut self, n: u64) {
        let bin = if n == 0 { 0 } else { (64 - n.leading_zeros()) as usize };
        self.bins[bin.min(63)] += 1; self.count += 1; self.total += u128::from(n);
        self.min = self.min.min(n); self.max = self.max.max(n);
    }
    fn percentile(&self, percent: u64) -> Option<u64> {
        if self.count == 0 { return None; }
        let target = (u128::from(self.count) * u128::from(percent)).div_ceil(100);
        let mut count = 0_u128;
        for (i, n) in self.bins.iter().enumerate() {
            count += u128::from(*n);
            if count >= target {
                let bound = if i == 63 { u64::MAX } else { (1_u64 << i).saturating_sub(1) };
                return Some(bound.min(self.max));
            }
        }
        Some(self.max)
    }
    fn report(&self) -> Timing {
        Timing { count: self.count, min: (self.count > 0).then_some(self.min), max: (self.count > 0).then_some(self.max),
            mean: if self.count > 0 { Some(self.total as f64 / self.count as f64) } else { None },
            p50_upper_bound: self.percentile(50), p95_upper_bound: self.percentile(95), p99_upper_bound: self.percentile(99) }
    }
}
impl serde::Serialize for Histogram {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.report(), serializer)
    }
}
#[derive(serde::Serialize)]
pub struct Timing { count: u64, min: Option<u64>, max: Option<u64>, mean: Option<f64>,
    p50_upper_bound: Option<u64>, p95_upper_bound: Option<u64>, p99_upper_bound: Option<u64> }

// Owned by the producer: output loss cannot erase these completed-input measurements.
#[derive(Default, serde::Serialize)]
struct ProductionTimings {
    preparation_ns: Histogram, calculation_ns: Histogram, event_build_ns: Histogram,
    publication_ns: Histogram, cooperative_yield_ns: Histogram, processing_ns: Histogram, account_to_worker_us: Histogram,
}
impl ProductionTimings {
    fn record(&mut self, e: &SignalEvaluation) {
        self.preparation_ns.add(e.price_preparation_ns); self.calculation_ns.add(e.calculation_ns);
        self.event_build_ns.add(e.event_build_ns); self.publication_ns.add(e.publication_ns);
        self.cooperative_yield_ns.add(e.cooperative_yield_ns);
        self.processing_ns.add(e.processing_ns);
        if let Some(age) = e.account_to_worker_us { self.account_to_worker_us.add(age); }
    }
}
#[derive(Default, serde::Serialize)]
pub struct ProductionReport {
    error: Option<String>, published_events: u64, evaluations: u64, websocket_evaluations: u64,
    candidates: u64, websocket_candidates: u64, routes_scored: u64, websocket_routes_scored: u64,
    max_price_queue_depth: usize, max_output_queue_depth: usize,
    all_inputs: ProductionTimings, websocket_inputs: ProductionTimings, catalog_plan_build_ns: Histogram,
}
impl ProductionReport {
    fn record(&mut self, e: &SignalEvaluation) {
        self.evaluations += 1; self.candidates += e.candidates; self.routes_scored += e.routes_scored;
        self.max_price_queue_depth = self.max_price_queue_depth.max(e.price_queue_depth);
        self.all_inputs.record(e);
        if websocket(&e.trigger) {
            self.websocket_evaluations += 1; self.websocket_candidates += e.candidates;
            self.websocket_routes_scored += e.routes_scored; self.websocket_inputs.record(e);
        }
        if matches!(e.trigger.as_ref(), SignalTrigger::Catalog { .. }) { self.catalog_plan_build_ns.add(e.plan_build_ns); }
    }
    pub fn ensure_complete(&self) -> Result<()> {
        ensure!(self.error.is_none(), "signal producer failed: {}", self.error.as_deref().unwrap_or(""));
        Ok(())
    }
    pub fn ensure_delivery(&self, received: &ObservationReport) -> Result<()> {
        self.ensure_complete()?; received.ensure_integrity()?;
        ensure!(self.published_events == received.received_events
            && self.evaluations == received.evaluation_events && self.candidates == received.candidates
            && self.websocket_evaluations == received.websocket_evaluations
            && self.websocket_candidates == received.websocket_candidates,
            "signal producer/consumer totals differ; inspect both reports");
        Ok(())
    }
}
#[derive(serde::Serialize)]
pub struct ObservationReport {
    pub profile_enabled: bool, pub error: Option<String>, pub received_events: u64,
    pub lost_events: u64, pub lag_incidents: u64, pub first_lag: Option<serde_json::Value>,
    pub consumer_bookkeeping_sample_period: u64, pub consumer_bookkeeping_sample_ns: Timing,
    pub completion_to_consumer_us: Timing, pub websocket_completion_to_consumer_us: Timing,
    pub consumer_periodic_logging_ns: u64,
    pub output_layout: &'static str,
    pub execution_layout: &'static str,
    pub evaluation_events: u64, pub price_evaluations: u64, pub websocket_evaluations: u64,
    pub catalog_evaluations: u64, pub combinations_visited: u64, pub routes_affected: u64,
    pub combinations_available: u64, pub routes_expected: u64, pub logarithms_prepared: u64,
    pub websocket_combinations_available: u64, pub websocket_combinations_visited: u64,
    pub websocket_routes_expected: u64, pub websocket_logarithms_prepared: u64,
    pub groups_visited: u64, pub max_groups_per_input: u64, pub max_routes_per_input: u64,
    pub routes_scored: u64, pub websocket_routes_scored: u64, pub routes_missing_prices: u64,
    pub routes_nonfinite: u64, pub candidates: u64, pub websocket_candidates: u64,
    pub candidate_leg_references: u64,
    pub invalidations: u64, pub max_price_queue_depth: usize, pub max_signal_queue_depth: usize,
    pub scored_by_kind: BTreeMap<&'static str, u64>, pub websocket_scored_by_kind: BTreeMap<&'static str, u64>,
    pub calculation_ns_per_input: Timing, pub output_ns_per_input: Timing,
    pub price_preparation_ns_per_input: Timing, pub websocket_price_preparation_ns_per_input: Timing,
    pub catalog_plan_build_ns: Timing,
    pub websocket_calculation_ns_per_input: Timing, pub websocket_output_ns_per_input: Timing,
    pub websocket_processing_ns_per_input: Timing,
    pub websocket_account_to_worker_us: Timing, pub websocket_account_to_evaluation_consumer_us: Timing,
    pub processing_ns_per_input: Timing, pub account_to_worker_us: Timing,
    pub account_to_candidate_us: Timing, pub account_to_consumer_us: Timing,
    pub consumer_formatting_ns: u64,
}
impl ObservationReport {
    pub fn ensure_integrity(&self) -> Result<()> {
        ensure!(self.error.is_none(), "signal consumer failed: {}", self.error.as_deref().unwrap_or(""));
        ensure!(self.lost_events == 0, "signal consumer lost {} events in {} incidents; live evidence incomplete",
            self.lost_events, self.lag_incidents);
        Ok(())
    }
    pub fn ensure_calculation_evidence(&self) -> Result<()> {
        self.ensure_integrity()?;
        ensure!(self.websocket_evaluations > 0 && self.websocket_routes_scored > 0,
            "no complete route calculated from a real WebSocket price update; inspect price coverage and per-kind counts");
        Ok(())
    }
    pub fn ensure_live_evidence(&self) -> Result<()> {
        self.ensure_calculation_evidence()?;
        ensure!(self.websocket_candidates > 0,
            "route calculation observed, but no real WebSocket-triggered candidate exceeded the threshold; signal output remains unproven");
        Ok(())
    }
}

pub async fn observe(mut input: SignalStream, settings: Settings) -> Result<ObservationReport> {
    const SAMPLE_PERIOD: u64 = 257;
    let mut received_events = 0_u64; let mut lost_events = 0_u64; let mut lag_incidents = 0_u64;
    let mut first_lag = None; let mut last_sequence = None;
    let mut bookkeeping = Histogram::default(); let mut completion_age = Histogram::default();
    let mut ws_completion_age = Histogram::default(); let mut periodic_logging_ns = 0_u64;
    let mut evaluations = 0; let mut price_evaluations = 0; let mut ws_evaluations = 0; let mut catalog_evaluations = 0;
    let mut combinations = 0; let mut affected = 0; let mut scored = 0; let mut ws_scored = 0;
    let mut available = 0; let mut routes_expected = 0; let mut logarithms = 0;
    let mut ws_available = 0; let mut ws_combinations = 0; let mut ws_expected = 0; let mut ws_logarithms = 0;
    let mut groups = 0_u64; let mut max_groups = 0_u64; let mut max_routes = 0_u64;
    let mut missing = 0; let mut nonfinite = 0; let mut candidates = 0; let mut ws_candidates = 0;
    let mut expected_candidates = 0;
    let mut candidate_legs = 0_u64;
    let mut invalidations = 0_u64; let mut input_max = 0_usize; let mut output_max = 0_usize;
    let mut by_kind = kinds(); let mut ws_by_kind = kinds();
    let mut calculate = Histogram::default(); let mut output = Histogram::default(); let mut process = Histogram::default();
    let mut ws_calculate = Histogram::default(); let mut ws_output = Histogram::default(); let mut ws_process = Histogram::default();
    let mut preparation = Histogram::default(); let mut ws_preparation = Histogram::default(); let mut plan_build = Histogram::default();
    let mut ws_worker_age = Histogram::default(); let mut ws_evaluation_consumer_age = Histogram::default();
    let mut worker_age = Histogram::default(); let mut candidate_age = Histogram::default(); let mut consumer_age = Histogram::default();
    let mut format_ns = 0_u64;
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    let outcome: Result<()> = async {
    loop { tokio::select! {
        _ = ticker.tick() => {
            let started = Instant::now();
            crate::log_event!(Info, "graph signals consumer execution_layout={} evaluations={evaluations} ws_evaluations={ws_evaluations} routes_scored={scored} ws_routes_scored={ws_scored} candidates={candidates} ws_candidates={ws_candidates} max_price_queue={input_max} max_signal_queue={output_max}", EXECUTION_LAYOUT);
            crate::log_event!(Info, "graph signals indexed ws_combinations_available={ws_available} ws_combinations_visited={ws_combinations} ws_routes_expected={ws_expected} ws_logarithms_prepared={ws_logarithms} ws_preparation_mean_ns={:?}", ws_preparation.report().mean);
            crate::log_event!(Info, "graph signals timings output_layout=shared_fixed_legs_v1 ws_output_mean_ns={:?} ws_processing_mean_ns={:?} ws_processing_max_ns={:?} ws_completion_age_max_us={:?}",
                ws_output.report().mean, ws_process.report().mean, ws_process.report().max, ws_evaluation_consumer_age.report().max);
            periodic_logging_ns = periodic_logging_ns.saturating_add(nanos(started));
        },
        event = input.recv() => match event {
            Ok(event) => {
                received_events += 1;
                let sampled = (settings.profile && received_events % SAMPLE_PERIOD == 1).then(Instant::now);
                output_max = output_max.max(input.len());
                last_sequence = match event.as_ref() {
                    SignalEvent::Candidate(r) | SignalEvent::RouteSample(r) => Some(r.sequence),
                    SignalEvent::Evaluated(e) => Some(e.sequence), SignalEvent::Invalidated { .. } => last_sequence,
                };
                let print = match event.as_ref() {
                    SignalEvent::Candidate(route) => {
                        candidates += 1; if websocket(&route.trigger) { ws_candidates += 1; }
                        candidate_legs += route.legs.as_slice().len() as u64;
                        if let Some(age) = route.account_to_emitted_us { candidate_age.add(age); }
                        if let Some(age) = account_age(&route.trigger, now_us()) { consumer_age.add(age); }
                        settings.trace || candidates <= settings.sample_limit as u64
                            || (websocket(&route.trigger) && ws_candidates <= settings.sample_limit as u64)
                    },
                    SignalEvent::RouteSample(_) => true,
                    SignalEvent::Invalidated { .. } => { invalidations += 1; settings.trace || invalidations <= settings.sample_limit as u64 },
                    SignalEvent::Evaluated(e) => {
                        evaluations += 1;
                        let at = now_us();
                        if let Some(age) = at.checked_sub(e.completed_at_us) {
                            completion_age.add(age);
                            if websocket(&e.trigger) { ws_completion_age.add(age); }
                        }
                        match e.trigger.as_ref() {
                            SignalTrigger::Price { .. } => {
                                price_evaluations += 1; preparation.add(e.price_preparation_ns);
                                ensure!(e.logarithms_prepared == 1 && e.plan_build_ns == 0,
                                    "price input did not use exactly one logarithm and the existing plan");
                            },
                            SignalTrigger::Catalog { .. } => { catalog_evaluations += 1; plan_build.add(e.plan_build_ns); },
                        }
                        if websocket(&e.trigger) {
                            ws_evaluations += 1; ws_scored += e.routes_scored;
                            ws_available += e.combinations_available; ws_combinations += e.combinations_visited;
                            ws_expected += e.routes_expected; ws_logarithms += e.logarithms_prepared;
                            ws_preparation.add(e.price_preparation_ns);
                            ws_calculate.add(e.calculation_ns); ws_output.add(e.output_ns); ws_process.add(e.processing_ns);
                            if let Some(age) = e.account_to_worker_us { ws_worker_age.add(age); }
                            // This marker follows every candidate for the input. Its receipt
                            // includes producer work and output queue/consumer work, even if no candidate qualified.
                            if let Some(age) = account_age(&e.trigger, at) { ws_evaluation_consumer_age.add(age); }
                        }
                        combinations += e.combinations_visited; affected += e.routes_affected;
                        available += e.combinations_available; routes_expected += e.routes_expected;
                        logarithms += e.logarithms_prepared;
                        ensure!(e.combinations_visited == e.routes_expected && e.routes_affected == e.routes_expected
                            && e.routes_affected == e.routes_scored + e.routes_missing_prices + e.routes_nonfinite
                            && e.combinations_available >= e.combinations_visited,
                            "indexed route accounting mismatch at evaluation {}", e.sequence);
                        groups += e.groups_visited;
                        max_groups = max_groups.max(e.groups_visited); max_routes = max_routes.max(e.routes_affected);
                        scored += e.routes_scored; missing += e.routes_missing_prices; nonfinite += e.routes_nonfinite;
                        expected_candidates += e.candidates;
                        if lost_events == 0 {
                            ensure!(candidates == expected_candidates, "signal candidate accounting mismatch at evaluation {}", e.sequence);
                        }
                        input_max = input_max.max(e.price_queue_depth);
                        for (&k, &n) in &e.scored_by_kind {
                            *by_kind.entry(k).or_default() += n;
                            if websocket(&e.trigger) { *ws_by_kind.entry(k).or_default() += n; }
                        }
                        calculate.add(e.calculation_ns); output.add(e.output_ns); process.add(e.processing_ns);
                        if let Some(age) = e.account_to_worker_us { worker_age.add(age); }
                        settings.trace || evaluations <= settings.sample_limit as u64
                            || (websocket(&e.trigger) && ws_evaluations <= settings.sample_limit as u64)
                    },
                };
                if let Some(started) = sampled { bookkeeping.add(nanos(started)); }
                if print {
                    let started = Instant::now();
                    println!("{}", serde_json::to_string(event.as_ref())?);
                    format_ns = format_ns.saturating_add(nanos(started));
                }
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                lost_events += n; lag_incidents += 1;
                if first_lag.is_none() {
                    first_lag = Some(serde_json::json!({"at_us":now_us(),"lost_events":n,
                        "received_events":received_events,"evaluations":evaluations,"candidates":candidates,
                        "last_sequence":last_sequence,"queue_depth":input.len(),"formatting_ns":format_ns}));
                    crate::log_event!(Warn, "graph signals consumer first_lag={} profiling={} live_evidence_valid=false", n, settings.profile);
                }
                if !settings.profile { bail!("signal consumer lost {n} events; live evidence incomplete"); }
            },
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }}
    if lost_events == 0 { ensure!(candidates == expected_candidates, "signal candidate consumer accounting mismatch"); }
    Ok(())
    }.await;
    let error = outcome.err().map(|e| format!("{e:#}"));
    crate::log_event!(Info, "graph signals consumer complete={} routes_scored={scored} ws_routes_scored={ws_scored} candidates={candidates} ws_candidates={ws_candidates} received={received_events} lost={lost_events}", error.is_none());
    Ok(ObservationReport { output_layout: "shared_fixed_legs_v1", execution_layout: EXECUTION_LAYOUT,
        profile_enabled: settings.profile, error, received_events, lost_events, lag_incidents, first_lag,
        consumer_bookkeeping_sample_period: SAMPLE_PERIOD, consumer_bookkeeping_sample_ns: bookkeeping.report(),
        completion_to_consumer_us: completion_age.report(), websocket_completion_to_consumer_us: ws_completion_age.report(),
        consumer_periodic_logging_ns: periodic_logging_ns,
        evaluation_events: evaluations, price_evaluations, websocket_evaluations: ws_evaluations,
        catalog_evaluations, combinations_visited: combinations, routes_affected: affected, routes_scored: scored,
        combinations_available: available, routes_expected, logarithms_prepared: logarithms,
        websocket_combinations_available: ws_available, websocket_combinations_visited: ws_combinations,
        websocket_routes_expected: ws_expected, websocket_logarithms_prepared: ws_logarithms,
        groups_visited: groups, max_groups_per_input: max_groups, max_routes_per_input: max_routes,
        websocket_routes_scored: ws_scored, routes_missing_prices: missing, routes_nonfinite: nonfinite,
        candidates, websocket_candidates: ws_candidates, candidate_leg_references: candidate_legs,
        invalidations, max_price_queue_depth: input_max,
        max_signal_queue_depth: output_max, scored_by_kind: by_kind, websocket_scored_by_kind: ws_by_kind,
        calculation_ns_per_input: calculate.report(), output_ns_per_input: output.report(), processing_ns_per_input: process.report(),
        price_preparation_ns_per_input: preparation.report(), websocket_price_preparation_ns_per_input: ws_preparation.report(),
        catalog_plan_build_ns: plan_build.report(),
        websocket_calculation_ns_per_input: ws_calculate.report(), websocket_output_ns_per_input: ws_output.report(),
        websocket_processing_ns_per_input: ws_process.report(), websocket_account_to_worker_us: ws_worker_age.report(),
        websocket_account_to_evaluation_consumer_us: ws_evaluation_consumer_age.report(),
        account_to_worker_us: worker_age.report(), account_to_candidate_us: candidate_age.report(),
        account_to_consumer_us: consumer_age.report(), consumer_formatting_ns: format_ns })
}
