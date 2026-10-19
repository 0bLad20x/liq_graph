//! Indicative route evaluation output. No execution requests or admission policy.
use super::market_state::{PoolPriceObservation, PriceOrigin};

#[derive(Clone, Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignalTrigger {
    Price { pool: String, sequence: u64, origin: PriceOrigin, received_at_us: u64 },
    Catalog { generation: u64 },
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalLeg {
    pub input_mint: String,
    pub output_mint: String,
    pub indicative_rate: f64,
    pub negative_log_weight: f64,
    pub observation: PoolPriceObservation,
}

/// One concrete directed roundtrip, not an amount-dependent quote or packed job.
/// group_id belongs exclusively to graph_generation; addresses identify the route.
#[derive(Clone, Debug, serde::Serialize)]
pub struct RouteEvaluation {
    pub sequence: u64,
    pub graph_generation: u64,
    pub group_id: u32,
    pub trigger: SignalTrigger,
    pub settlement: String,
    pub target: String,
    pub bridge: Option<String>,
    pub conversion_pool: Option<String>,
    pub legs: Vec<SignalLeg>,
    pub negative_log_sum: f64,
    pub indicative_return_bps: f64,
    pub threshold_bps: f64,
    pub qualifies: bool,
    pub emitted_at_us: u64,
    /// Local transport receipt to candidate creation; wall-clock, millisecond input resolution.
    pub account_to_emitted_us: Option<u64>,
}

/// One complete input evaluation. No route is skipped because its price is unchanged.
#[derive(Debug, serde::Serialize)]
pub struct SignalEvaluation {
    pub sequence: u64,
    pub graph_generation: u64,
    pub trigger: SignalTrigger,
    pub groups_visited: u64,
    pub combinations_visited: u64,
    pub routes_affected: u64,
    pub routes_scored: u64,
    pub routes_missing_prices: u64,
    pub routes_nonfinite: u64,
    pub candidates: u64,
    pub scored_by_kind: std::collections::BTreeMap<&'static str, u64>,
    pub price_queue_depth: usize,
    pub account_to_worker_us: Option<u64>,
    /// Lookup, direction resolution, logarithms and complete sum, for every affected route.
    pub calculation_ns: u64,
    /// Evidence construction and publishing; excludes downstream formatting/file IO.
    pub output_ns: u64,
    /// Full processing wall time, including enumeration and cooperative scheduling.
    pub processing_ns: u64,
}

#[derive(Debug, serde::Serialize)]
pub enum SignalEvent {
    Candidate(RouteEvaluation),
    RouteSample(RouteEvaluation),
    Evaluated(SignalEvaluation),
    Invalidated { graph_generation: u64, pool: Option<String>, reason: String },
}

pub type SignalStream = tokio::sync::broadcast::Receiver<std::sync::Arc<SignalEvent>>;
