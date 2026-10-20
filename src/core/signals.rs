//! Indicative route evaluation output. No execution requests or admission policy.
use std::sync::Arc;
use solana_pubkey::Pubkey;
use super::market_state::{PoolPriceObservation, PriceOrigin, PricePipelineTiming};

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
    /// Local transport receipt to block construction; wall-clock, millisecond input resolution.
    pub account_to_emitted_us: Option<u64>,
}

/// Bounded output block. Each row keeps its result; context and frozen legs are shared.
pub const SIGNAL_BLOCK_ROWS: usize = 64;
#[derive(Debug, serde::Serialize)]
pub struct SignalRouteRow {
    pub sequence:u64, pub leg_indices:[u16;4], pub negative_log_sum:f64,
    pub indicative_return_bps:f64, pub qualifies:bool,
}
#[derive(Debug, serde::Serialize)]
pub struct SignalRouteBlock {
    pub graph_generation:u64, pub group_id:u32, pub trigger:Arc<SignalTrigger>,
    #[serde(with = "super::execution::pubkey_json")]
    pub settlement:Pubkey,
    #[serde(with = "super::execution::pubkey_json")]
    pub target:Pubkey,
    #[serde(with = "super::execution::optional_pubkey_json")]
    pub bridge:Option<Pubkey>,
    #[serde(with = "super::execution::optional_pubkey_json")]
    pub conversion_pool:Option<Pubkey>,
    /// Local start of block construction; shared by its diagnostic row expansions.
    pub emitted_at_us:u64,
    pub threshold_bps:f64, pub leg_count:usize, pub legs:Vec<SignalLeg>, pub rows:Vec<SignalRouteRow>,
}
impl SignalRouteBlock {
    /// Expand a selected row for a consumer; does not consult mutable worker state.
    pub fn route(&self, index:usize)->Option<RouteEvaluation> {
        let row=self.rows.get(index)?;
        let leg=|i:usize|self.legs.get(usize::from(row.leg_indices[i])).cloned();
        let legs=match self.leg_count {
            2=>SignalLegs::Two([leg(0)?,leg(1)?]),
            3=>SignalLegs::Three([leg(0)?,leg(1)?,leg(2)?]),
            4=>SignalLegs::Four([leg(0)?,leg(1)?,leg(2)?,leg(3)?]), _=>return None,
        };
        let account_to_emitted_us=match self.trigger.as_ref() {
            SignalTrigger::Price{received_at_us,..}=>self.emitted_at_us.checked_sub(*received_at_us),
            SignalTrigger::Catalog{..}=>None,
        };
        Some(RouteEvaluation{sequence:row.sequence,graph_generation:self.graph_generation,group_id:self.group_id,
            trigger:self.trigger.clone(),settlement:self.settlement,target:self.target,bridge:self.bridge,
            conversion_pool:self.conversion_pool,legs,negative_log_sum:row.negative_log_sum,
            indicative_return_bps:row.indicative_return_bps,threshold_bps:self.threshold_bps,
            qualifies:row.qualifies,emitted_at_us:self.emitted_at_us,account_to_emitted_us})
    }
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
    pub worker_received_at_us:u64, pub price_pipeline:Option<PricePipelineTiming>,
    /// Parsing, identity validation, directional weights and storage for this input.
    pub price_preparation_ns: u64,
    pub logarithms_prepared: u64,
    /// Generation compilation; zero for ordinary price inputs.
    pub plan_build_ns: u64,
    /// Route-event construction and allocation, measured per existing calculation block.
    pub event_build_ns: u64,
    /// Non-waiting broadcast publication and publication counters, per block.
    pub publication_ns: u64,
    /// Wall time suspended at the existing cooperative yields.
    pub cooperative_yield_ns: u64,
    /// Block enumeration, dense-array calculation and bounded output staging.
    pub calculation_ns: u64,
    /// Evidence construction and publishing; excludes downstream formatting/file IO.
    pub output_ns: u64,
    /// Full processing wall time, including enumeration and cooperative scheduling.
    pub processing_ns: u64,
    /// Local creation time of this completion marker, before channel publication.
    pub completed_at_us: u64,
}

#[derive(Debug, serde::Serialize)]
pub enum SignalEvent {
    Routes(SignalRouteBlock),
    Evaluated(SignalEvaluation),
    Invalidated { graph_generation: u64, pool: Option<String>, reason: String },
}

pub type SignalStream = tokio::sync::broadcast::Receiver<std::sync::Arc<SignalEvent>>;
