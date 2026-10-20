//! Indicative route evaluation output. No execution requests or admission policy.
use std::sync::Arc;
use solana_pubkey::Pubkey;
use super::market_state::{PoolPriceObservation, PriceOrigin};

#[derive(Clone, Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignalTrigger {
    Price {
        #[serde(with = "super::execution::pubkey_json")]
        pool: Pubkey,
        sequence: u64, origin: PriceOrigin, received_at_us: u64,
    },
    Catalog { generation: u64 },
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalLeg {
    #[serde(with = "super::execution::pubkey_json")]
    pub input_mint: Pubkey,
    #[serde(with = "super::execution::pubkey_json")]
    pub output_mint: Pubkey,
    pub indicative_rate: f64,
    pub negative_log_weight: f64,
    /// Frozen observation shared by every route using this exact pool update.
    pub observation: Arc<PoolPriceObservation>,
}

/// At most four legs, stored inline. JSON remains an array of exactly 2/3/4 legs.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(untagged)]
pub enum SignalLegs {
    Two([SignalLeg; 2]),
    Three([SignalLeg; 3]),
    Four([SignalLeg; 4]),
}
impl SignalLegs {
    pub fn as_slice(&self) -> &[SignalLeg] {
        match self { Self::Two(v) => v, Self::Three(v) => v, Self::Four(v) => v }
    }
}

/// One concrete directed roundtrip, not an amount-dependent quote or packed job.
/// group_id belongs exclusively to graph_generation; addresses identify the route.
#[derive(Clone, Debug, serde::Serialize)]
pub struct RouteEvaluation {
    pub sequence: u64,
    pub graph_generation: u64,
    pub group_id: u32,
    /// One immutable trigger per input evaluation, shared with its route outputs.
    pub trigger: Arc<SignalTrigger>,
    #[serde(with = "super::execution::pubkey_json")]
    pub settlement: Pubkey,
    #[serde(with = "super::execution::pubkey_json")]
    pub target: Pubkey,
    #[serde(with = "super::execution::optional_pubkey_json")]
    pub bridge: Option<Pubkey>,
    #[serde(with = "super::execution::optional_pubkey_json")]
    pub conversion_pool: Option<Pubkey>,
    pub legs: SignalLegs,
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
    pub trigger: Arc<SignalTrigger>,
    pub groups_visited: u64,
    /// Full group combinations the former enumerator would have visited.
    pub combinations_available: u64,
    /// Analytical affected-route count from the compiled pool-role bindings.
    pub routes_expected: u64,
    pub combinations_visited: u64,
    pub routes_affected: u64,
    pub routes_scored: u64,
    pub routes_missing_prices: u64,
    pub routes_nonfinite: u64,
    pub candidates: u64,
    pub scored_by_kind: std::collections::BTreeMap<&'static str, u64>,
    pub price_queue_depth: usize,
    pub account_to_worker_us: Option<u64>,
    /// Parsing, identity validation, directional weights and storage for this input.
    pub price_preparation_ns: u64,
    pub logarithms_prepared: u64,
    /// Generation compilation; zero for ordinary price inputs.
    pub plan_build_ns: u64,
    /// Block enumeration, dense-array calculation and bounded output staging.
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
