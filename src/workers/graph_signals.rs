//! Reference calculation: every affected concrete route is recomputed in full.
//! Graph membership is indexed; no best-price pruning, cached weights or partial sums.
use std::{collections::{BTreeMap, HashMap}, env, sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use anyhow::{Context, Result, bail, ensure};
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast, watch};
use crate::core::{market_state::{MarketStateEvent, PoolPriceObservation, PriceEventStream, PriceOrigin},
    signals::{RouteEvaluation, SignalEvaluation, SignalEvent, SignalLeg, SignalLegs, SignalStream, SignalTrigger}};
use super::{graph_liquidity::{Group, GroupKind}, graph_watch_catalog::WatchCatalog};

const WSOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

#[derive(Clone, Copy)]
pub struct Settings { pub threshold_bps: f64, pub sample_limit: usize, pub trace: bool }
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
        Ok(Self { threshold_bps, sample_limit, trace })
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
fn kind(g: &Group) -> &'static str {
    match (g.kind, g.conversion.is_some()) {
        (GroupKind::Direct, false) => "direct_2", (GroupKind::Direct, true) => "direct_3",
        (GroupKind::Bridge, false) => "triangle_3", (GroupKind::Bridge, true) => "triangle_4",
    }
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

struct CachedPrice { observation: Arc<PoolPriceObservation>, base: Pubkey, quote: Pubkey, a: Pubkey, b: Pubkey }
impl CachedPrice {
    fn observation(&self) -> &PoolPriceObservation { &self.observation }
    fn new(p: &PoolPriceObservation) -> Result<(Pubkey, Self)> {
        let pool: Pubkey = p.pool.parse().context("signal pool address")?;
        let base: Pubkey = p.base_mint.parse().context("signal base mint")?;
        let quote: Pubkey = p.quote_mint.parse().context("signal quote mint")?;
        let a: Pubkey = p.mint_a.parse().context("signal mint A")?;
        let b: Pubkey = p.mint_b.parse().context("signal mint B")?;
        ensure!(a != b && ((a == base && b == quote) || (a == quote && b == base)),
            "price mint orientation mismatch for {pool}");
        ensure!(p.price_quote_per_base.is_finite() && p.price_quote_per_base > 0.0,
            "nonpositive/nonfinite indicative price for {pool}");
        // Copy the incoming payload once per accepted price, never once per route.
        // Replacing this cache entry cannot change observations held by consumers.
        let observation = Arc::new(p.clone());
        Ok((pool, Self { observation, base, quote, a, b }))
    }
    fn check_catalog(&self, pool: Pubkey, catalog: &WatchCatalog) -> Result<()> {
        if let Some(w) = catalog.for_pool(pool) {
            ensure!((w.mint_a, w.mint_b) == (self.a.min(self.b), self.a.max(self.b)),
                "catalog/price identity mismatch for {pool}");
        }
        Ok(())
    }
}

struct Scored { weights: [f64; 4], rates: [f64; 4], sum: f64, bps: f64 }
enum Calculation { Missing, Nonfinite, Scored(Scored) }
struct Engine {
    prices: HashMap<Pubkey, CachedPrice>, output: broadcast::Sender<Arc<SignalEvent>>,
    settings: Settings, sequence: u64, samples: usize, websocket_samples: usize, threshold_log: f64,
}
impl Engine {
    fn emit(&self, event: SignalEvent) -> Result<()> {
        self.output.send(Arc::new(event)).map(|_| ())
            .map_err(|_| anyhow::anyhow!("signal output has no consumer"))
    }
    // All legs, logs and the complete sum are calculated anew for every concrete route.
    fn calculate(&self, pools: &[Pubkey], mints: &[Pubkey]) -> Result<Calculation> {
        ensure!((2..=4).contains(&pools.len()) && mints.len() == pools.len() + 1
            && mints.first() == mints.last(), "invalid graph roundtrip");
        let mut weights = [0.0; 4];
        let mut rates = [0.0; 4];
        let mut sum = 0.0;
        for (i, pool) in pools.iter().enumerate() {
            ensure!(!pools[..i].contains(pool), "graph route repeats pool {pool}");
            let Some(price) = self.prices.get(pool) else { return Ok(Calculation::Missing); };
            let p = price.observation().price_quote_per_base;
            let log_price = p.ln();
            if mints[i] == price.base && mints[i + 1] == price.quote {
                rates[i] = p; weights[i] = -log_price;
            } else if mints[i] == price.quote && mints[i + 1] == price.base {
                rates[i] = 1.0 / p; weights[i] = log_price;
            } else { bail!("pool {pool} does not connect the graph route mints"); }
            if !rates[i].is_finite() || rates[i] <= 0.0 || !weights[i].is_finite() {
                return Ok(Calculation::Nonfinite);
            }
            sum += weights[i];
        }
        let bps = (-sum).exp_m1() * 10_000.0;
        if !sum.is_finite() || !bps.is_finite() { return Ok(Calculation::Nonfinite); }
        Ok(Calculation::Scored(Scored { weights, rates, sum, bps }))
    }
    async fn route(&mut self, id: u32, g: &Group, pools: &[Pubkey], mints: &[Pubkey],
        changed: Option<Pubkey>, stats: &mut SignalEvaluation) -> Result<()> {
        stats.combinations_visited += 1;
        // Scheduling only: no event coalescing, pruning or arithmetic shortcut.
        if stats.combinations_visited % 4096 == 0 { tokio::task::yield_now().await; }
        if changed.is_some_and(|p| !pools.contains(&p)) { return Ok(()); }
        stats.routes_affected += 1;
        let started = Instant::now();
        let calculated = self.calculate(pools, mints)?;
        stats.calculation_ns = stats.calculation_ns.saturating_add(nanos(started));
        let scored = match calculated {
            Calculation::Missing => { stats.routes_missing_prices += 1; return Ok(()); },
            Calculation::Nonfinite => { stats.routes_nonfinite += 1; return Ok(()); },
            Calculation::Scored(s) => s,
        };
        stats.routes_scored += 1;
        *stats.scored_by_kind.entry(kind(g)).or_default() += 1;
        let qualifies = scored.sum < self.threshold_log;
        if qualifies { stats.candidates += 1; }
        let live = websocket(&stats.trigger);
        let sample_count = if live { self.websocket_samples } else { self.samples };
        if qualifies || sample_count < self.settings.sample_limit {
            let started = Instant::now();
            let leg = |i: usize| SignalLeg { input_mint: mints[i], output_mint: mints[i + 1],
                indicative_rate: scored.rates[i], negative_log_weight: scored.weights[i],
                observation: Arc::clone(&self.prices[&pools[i]].observation) };
            let legs = match pools.len() {
                2 => SignalLegs::Two(std::array::from_fn(leg)),
                3 => SignalLegs::Three(std::array::from_fn(leg)),
                4 => SignalLegs::Four(std::array::from_fn(leg)),
                _ => unreachable!("calculate validated the route length"),
            };
            self.sequence += 1;
            let mut route = RouteEvaluation { sequence: self.sequence, graph_generation: stats.graph_generation,
                group_id: id, trigger: Arc::clone(&stats.trigger), settlement: g.settlement,
                target: g.target, bridge: g.bridge, conversion_pool: g.conversion, legs,
                negative_log_sum: scored.sum, indicative_return_bps: scored.bps,
                threshold_bps: self.settings.threshold_bps, qualifies, emitted_at_us: 0,
                account_to_emitted_us: None };
            route.emitted_at_us = now_us();
            route.account_to_emitted_us = account_age(&stats.trigger, route.emitted_at_us);
            if qualifies { self.emit(SignalEvent::Candidate(route))?; }
            else {
                if live { self.websocket_samples += 1; } else { self.samples += 1; }
                self.emit(SignalEvent::RouteSample(route))?;
            }
            stats.output_ns = stats.output_ns.saturating_add(nanos(started));
        }
        Ok(())
    }
    async fn evaluate(&mut self, catalog: Arc<WatchCatalog>, trigger: SignalTrigger,
        changed: Option<Pubkey>, price_queue_depth: usize, started: Instant, received_at_us: u64) -> Result<()> {
        ensure!(catalog.generation == catalog.graph.generation, "mixed graph/catalog generation");
        let ids: Vec<u32> = if let Some(pool) = changed { catalog.graph.for_pool(pool).to_vec() }
            else { (0..catalog.graph.groups.len()).map(|i| i as u32).collect() };
        let mut stats = SignalEvaluation { sequence: 0, graph_generation: catalog.generation,
            account_to_worker_us: account_age(&trigger, received_at_us), trigger: Arc::new(trigger),
            groups_visited: 0, combinations_visited: 0, routes_affected: 0, routes_scored: 0,
            routes_missing_prices: 0, routes_nonfinite: 0, candidates: 0, scored_by_kind: kinds(),
            price_queue_depth, calculation_ns: 0, output_ns: 0, processing_ns: 0 };
        let wsol: Pubkey = WSOL.parse()?;
        let usdc: Pubkey = USDC.parse()?;
        for id in ids {
            let g = catalog.graph.group(id).context("group index outside graph generation")?;
            let combinations_before = stats.combinations_visited;
            stats.groups_visited += 1;
            let s = g.settlement; let t = g.target;
            ensure!(s == wsol || s == usdc, "unsupported graph settlement");
            let q = if s == wsol { usdc } else { wsol };
            match (g.kind, g.conversion) {
                (GroupKind::Direct, None) => {
                    for &buy in g.direct_s.iter() { for &sell in g.direct_s.iter() {
                        if buy != sell { self.route(id, g, &[buy, sell], &[s, t, s], changed, &mut stats).await?; }
                    }}
                },
                (GroupKind::Direct, Some(conversion)) => {
                    for &direct in g.direct_s.iter() { for &quote in g.direct_q.iter() {
                        self.route(id, g, &[direct, quote, conversion], &[s, t, q, s], changed, &mut stats).await?;
                        self.route(id, g, &[conversion, quote, direct], &[s, q, t, s], changed, &mut stats).await?;
                    }}
                },
                (GroupKind::Bridge, conversion) => {
                    let b = g.bridge.context("bridge group missing bridge mint")?;
                    let middle = g.intermediate.context("bridge group missing intermediate pool")?;
                    for &direct in g.direct_s.iter() { for &back in g.bridge_pools.iter() {
                        if let Some(conversion) = conversion {
                            self.route(id, g, &[direct, middle, back, conversion], &[s, t, b, q, s], changed, &mut stats).await?;
                            self.route(id, g, &[conversion, back, middle, direct], &[s, q, b, t, s], changed, &mut stats).await?;
                        } else {
                            self.route(id, g, &[direct, middle, back], &[s, t, b, s], changed, &mut stats).await?;
                            self.route(id, g, &[back, middle, direct], &[s, b, t, s], changed, &mut stats).await?;
                        }
                    }}
                },
            }
            ensure!(stats.combinations_visited - combinations_before == g.route_options,
                "reference enumeration differs from graph route count in group {id}");
            tokio::task::yield_now().await;
        }
        ensure!(stats.routes_affected == stats.routes_scored + stats.routes_missing_prices + stats.routes_nonfinite,
            "incomplete reference calculation accounting");
        self.sequence += 1; stats.sequence = self.sequence; stats.processing_ns = nanos(started);
        self.emit(SignalEvent::Evaluated(stats))
    }
}

pub async fn run(mut input: PriceEventStream, mut catalogs: watch::Receiver<Arc<WatchCatalog>>,
    output: broadcast::Sender<Arc<SignalEvent>>, settings: Settings) -> Result<()> {
    let mut engine = Engine { prices: HashMap::new(), output, settings, sequence: 0, samples: 0, websocket_samples: 0,
        threshold_log: -(settings.threshold_bps / 10_000.0).ln_1p() };
    let mut current = catalogs.borrow_and_update().clone();
    let mut catalog_open = true;
    crate::log_event!(Info, "graph signals started=true reference=true output_layout=shared_fixed_legs_v1 threshold_bps={} execution=false", settings.threshold_bps);
    loop { tokio::select! {
        change = catalogs.changed(), if catalog_open => {
            if change.is_err() { catalog_open = false; continue; }
            let started = Instant::now(); let received_at_us = now_us();
            let next = catalogs.borrow_and_update().clone();
            if next.generation == current.generation { continue; }
            for (&pool, price) in &engine.prices { price.check_catalog(pool, &next)?; }
            engine.emit(SignalEvent::Invalidated { graph_generation: next.generation, pool: None,
                reason: "graph generation replaced; group IDs belong to the new catalog".into() })?;
            current = next;
            engine.evaluate(current.clone(), SignalTrigger::Catalog { generation: current.generation },
                None, input.len(), started, received_at_us).await?;
        },
        received = input.recv() => match received {
            Ok(event) => match event.as_ref() {
                MarketStateEvent::Price(p) => {
                    let started = Instant::now(); let received_at_us = now_us();
                    let (pool, price) = CachedPrice::new(p)?;
                    price.check_catalog(pool, &current)?;
                    let trigger = SignalTrigger::Price { pool, sequence: p.sequence,
                        origin: p.origin, received_at_us: p.received_at_us };
                    engine.prices.insert(pool, price);
                    engine.evaluate(current.clone(), trigger, Some(pool), input.len(), started, received_at_us).await?;
                },
                MarketStateEvent::Unavailable { pool, reason, .. } => {
                    engine.prices.remove(&pool.parse::<Pubkey>()?);
                    engine.emit(SignalEvent::Invalidated { graph_generation: current.generation,
                        pool: Some(pool.clone()), reason: reason.clone() })?;
                },
                MarketStateEvent::Removed { pool, .. } => {
                    engine.prices.remove(&pool.parse::<Pubkey>()?);
                    engine.emit(SignalEvent::Invalidated { graph_generation: current.generation,
                        pool: Some(pool.clone()), reason: "pool removed from observation".into() })?;
                },
                MarketStateEvent::Gap { reason, .. } => {
                    engine.prices.clear();
                    engine.emit(SignalEvent::Invalidated { graph_generation: current.generation,
                        pool: None, reason: reason.clone() })?;
                },
                MarketStateEvent::Selection { selected, .. } => {
                    let selected: std::collections::HashSet<Pubkey> = selected.iter()
                        .map(|p| p.parse()).collect::<std::result::Result<_, _>>()?;
                    engine.prices.retain(|pool, _| selected.contains(pool));
                },
                MarketStateEvent::Stats { .. } => {},
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                engine.prices.clear();
                engine.emit(SignalEvent::Invalidated { graph_generation: current.generation,
                    pool: None, reason: format!("price input lagged {n}; coverage invalid") })?;
                bail!("graph signals lost {n} price events; reference run invalid");
            },
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }}
    crate::log_event!(Info, "graph signals complete=true reference=true");
    Ok(())
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
#[derive(serde::Serialize)]
pub struct Timing { count: u64, min: Option<u64>, max: Option<u64>, mean: Option<f64>,
    p50_upper_bound: Option<u64>, p95_upper_bound: Option<u64>, p99_upper_bound: Option<u64> }
#[derive(serde::Serialize)]
pub struct ObservationReport {
    pub output_layout: &'static str,
    pub evaluation_events: u64, pub price_evaluations: u64, pub websocket_evaluations: u64,
    pub catalog_evaluations: u64, pub combinations_visited: u64, pub routes_affected: u64,
    pub groups_visited: u64, pub max_groups_per_input: u64, pub max_routes_per_input: u64,
    pub routes_scored: u64, pub websocket_routes_scored: u64, pub routes_missing_prices: u64,
    pub routes_nonfinite: u64, pub candidates: u64, pub websocket_candidates: u64,
    pub candidate_leg_references: u64,
    pub invalidations: u64, pub max_price_queue_depth: usize, pub max_signal_queue_depth: usize,
    pub scored_by_kind: BTreeMap<&'static str, u64>, pub websocket_scored_by_kind: BTreeMap<&'static str, u64>,
    pub calculation_ns_per_input: Timing, pub output_ns_per_input: Timing,
    pub websocket_calculation_ns_per_input: Timing, pub websocket_output_ns_per_input: Timing,
    pub websocket_processing_ns_per_input: Timing,
    pub websocket_account_to_worker_us: Timing, pub websocket_account_to_evaluation_consumer_us: Timing,
    pub processing_ns_per_input: Timing, pub account_to_worker_us: Timing,
    pub account_to_candidate_us: Timing, pub account_to_consumer_us: Timing,
    pub consumer_formatting_ns: u64,
}
impl ObservationReport {
    pub fn ensure_calculation_evidence(&self) -> Result<()> {
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
    let mut evaluations = 0; let mut price_evaluations = 0; let mut ws_evaluations = 0; let mut catalog_evaluations = 0;
    let mut combinations = 0; let mut affected = 0; let mut scored = 0; let mut ws_scored = 0;
    let mut groups = 0_u64; let mut max_groups = 0_u64; let mut max_routes = 0_u64;
    let mut missing = 0; let mut nonfinite = 0; let mut candidates = 0; let mut ws_candidates = 0;
    let mut expected_candidates = 0;
    let mut candidate_legs = 0_u64;
    let mut invalidations = 0_u64; let mut input_max = 0_usize; let mut output_max = 0_usize;
    let mut by_kind = kinds(); let mut ws_by_kind = kinds();
    let mut calculate = Histogram::default(); let mut output = Histogram::default(); let mut process = Histogram::default();
    let mut ws_calculate = Histogram::default(); let mut ws_output = Histogram::default(); let mut ws_process = Histogram::default();
    let mut ws_worker_age = Histogram::default(); let mut ws_evaluation_consumer_age = Histogram::default();
    let mut worker_age = Histogram::default(); let mut candidate_age = Histogram::default(); let mut consumer_age = Histogram::default();
    let mut format_ns = 0_u64;
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    loop { tokio::select! {
        _ = ticker.tick() => {
            crate::log_event!(Info, "graph signals consumer reference=true evaluations={evaluations} ws_evaluations={ws_evaluations} routes_scored={scored} ws_routes_scored={ws_scored} candidates={candidates} ws_candidates={ws_candidates} max_price_queue={input_max} max_signal_queue={output_max}");
            crate::log_event!(Info, "graph signals timings output_layout=shared_fixed_legs_v1 ws_output_mean_ns={:?} ws_processing_mean_ns={:?} ws_processing_max_ns={:?} ws_completion_age_max_us={:?}",
                ws_output.report().mean, ws_process.report().mean, ws_process.report().max, ws_evaluation_consumer_age.report().max);
        },
        event = input.recv() => match event {
            Ok(event) => {
                output_max = output_max.max(input.len());
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
                        match e.trigger.as_ref() {
                            SignalTrigger::Price { .. } => price_evaluations += 1,
                            SignalTrigger::Catalog { .. } => catalog_evaluations += 1,
                        }
                        if websocket(&e.trigger) {
                            ws_evaluations += 1; ws_scored += e.routes_scored;
                            ws_calculate.add(e.calculation_ns); ws_output.add(e.output_ns); ws_process.add(e.processing_ns);
                            if let Some(age) = e.account_to_worker_us { ws_worker_age.add(age); }
                            // This marker follows every candidate for the input. Its receipt
                            // includes producer work and output queue/consumer work, even if no candidate qualified.
                            if let Some(age) = account_age(&e.trigger, now_us()) { ws_evaluation_consumer_age.add(age); }
                        }
                        combinations += e.combinations_visited; affected += e.routes_affected;
                        groups += e.groups_visited;
                        max_groups = max_groups.max(e.groups_visited); max_routes = max_routes.max(e.routes_affected);
                        scored += e.routes_scored; missing += e.routes_missing_prices; nonfinite += e.routes_nonfinite;
                        expected_candidates += e.candidates;
                        ensure!(candidates == expected_candidates, "signal candidate accounting mismatch at evaluation {}", e.sequence);
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
                if print {
                    let started = Instant::now();
                    println!("{}", serde_json::to_string(event.as_ref())?);
                    format_ns = format_ns.saturating_add(nanos(started));
                }
            },
            Err(broadcast::error::RecvError::Lagged(n)) => bail!("signal consumer lost {n} events; live evidence incomplete"),
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }}
    ensure!(candidates == expected_candidates, "signal candidate consumer accounting mismatch");
    crate::log_event!(Info, "graph signals consumer complete=true routes_scored={scored} ws_routes_scored={ws_scored} candidates={candidates} ws_candidates={ws_candidates}");
    Ok(ObservationReport { output_layout: "shared_fixed_legs_v1",
        evaluation_events: evaluations, price_evaluations, websocket_evaluations: ws_evaluations,
        catalog_evaluations, combinations_visited: combinations, routes_affected: affected, routes_scored: scored,
        groups_visited: groups, max_groups_per_input: max_groups, max_routes_per_input: max_routes,
        websocket_routes_scored: ws_scored, routes_missing_prices: missing, routes_nonfinite: nonfinite,
        candidates, websocket_candidates: ws_candidates, candidate_leg_references: candidate_legs,
        invalidations, max_price_queue_depth: input_max,
        max_signal_queue_depth: output_max, scored_by_kind: by_kind, websocket_scored_by_kind: ws_by_kind,
        calculation_ns_per_input: calculate.report(), output_ns_per_input: output.report(), processing_ns_per_input: process.report(),
        websocket_calculation_ns_per_input: ws_calculate.report(), websocket_output_ns_per_input: ws_output.report(),
        websocket_processing_ns_per_input: ws_process.report(), websocket_account_to_worker_us: ws_worker_age.report(),
        websocket_account_to_evaluation_consumer_us: ws_evaluation_consumer_age.report(),
        account_to_worker_us: worker_age.report(), account_to_candidate_us: candidate_age.report(),
        account_to_consumer_us: consumer_age.report(), consumer_formatting_ns: format_ns })
}
