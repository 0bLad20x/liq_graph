//! Versioned factorized route state and exact interval outputs. Data contracts only.
use std::sync::Arc;
use solana_pubkey::Pubkey;
use super::market_state::{PoolPriceObservation, PriceOrigin, PricePipelineTiming};

/// Canonical log-weight unit for the wire contract (Q48).
pub const SIGNAL_LOG_SCALE: i64 = 1_i64 << 48;

#[derive(Clone, Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignalTrigger {
    Price {
        #[serde(with = "super::execution::pubkey_json")]
        pool: Pubkey,
        sequence: u64, origin: PriceOrigin, received_at_us: u64,
    },
    Catalog { generation: u64 },
    Coverage { sequence: u64, reason: String },
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SignalPool {
    #[serde(with = "super::execution::pubkey_json")]
    pub address: Pubkey,
    #[serde(with = "super::execution::pubkey_json")]
    pub mint_a: Pubkey,
    #[serde(with = "super::execution::pubkey_json")]
    pub mint_b: Pubkey,
}
/// Canonical slots 2*pool_id and 2*pool_id+1 mean A->B and B->A.
/// None means that direction is not numerically available.
#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalPoolPrice {
    pub log_weight_units: [Option<i64>; 2],
    pub observation: Arc<PoolPriceObservation>,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalRole { pub slots: Arc<[usize]> }
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalRouteKind { Direct2, Direct3, Triangle3, Triangle4 }
#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalGroup {
    pub kind: SignalRouteKind,
    /// Forward and inverse role IDs; left/right keep their forward topology meaning.
    pub left: [usize; 2], pub right: [usize; 2],
    pub fixed: [usize; 2], pub fixed_count: usize,
    #[serde(serialize_with = "mint_array")]
    pub mints: [Pubkey; 5],
    pub route_options: u64,
}
fn mint_array<S: serde::Serializer>(v: &[Pubkey; 5], s: S) -> Result<S::Ok, S::Error> {
    serde::Serialize::serialize(&v.map(|p| p.to_string()), s)
}
fn optional_mint<S:serde::Serializer>(v:&Option<Pubkey>,s:S)->Result<S::Ok,S::Error>{
    serde::Serialize::serialize(&v.map(|x|x.to_string()),s)
}
fn mint_pairs<S:serde::Serializer>(v:&[(Pubkey,Pubkey);4],s:S)->Result<S::Ok,S::Error>{
    serde::Serialize::serialize(&v.map(|(a,b)|[a.to_string(),b.to_string()]),s)
}
/// Immutable, factored V10 topology for signal and execution consumers.
/// The ordered mint pairs describe the forward route; the reverse is implicit.
#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalTopology {
    pub kind: SignalRouteKind,
    #[serde(with = "super::execution::pubkey_json")]
    pub settlement: Pubkey,
    #[serde(with = "super::execution::pubkey_json")]
    pub target: Pubkey,
    #[serde(serialize_with = "optional_mint")]
    pub bridge: Option<Pubkey>,
    #[serde(serialize_with = "mint_pairs")]
    pub roles: [(Pubkey, Pubkey);4],
    pub length: usize,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalCandidate {
    pub topology_id: usize,
    pub reverse: bool,
    pub change: SignalRangeChange,
    /// Only present for entering/updating profitable candidates.
    pub witness: Option<SignalWitness>,
}
#[derive(Debug, serde::Serialize)]
pub struct SignalCandidateBatch {
    pub sequence: u64, pub revision: u64, pub graph_generation: u64,
    pub trigger: Arc<SignalTrigger>,
    pub threshold_units: i64,
    pub price_pipeline: Option<PricePipelineTiming>,
    pub candidates: Vec<SignalCandidate>,
}
#[derive(Debug, serde::Serialize)]
pub struct SignalPlan {
    pub graph_generation: u64,
    pub log_weight_scale: i64,
    pub roles: Arc<[SignalRole]>, pub groups: Arc<[SignalGroup]>,
    pub compact_topologies: Arc<[SignalTopology]>,
    pub route_options: u64,
}
#[derive(Debug, serde::Serialize)]
pub struct SignalSnapshot {
    pub plan: Arc<SignalPlan>, pub pools: Vec<SignalPool>,
    pub prices: Vec<Option<Arc<SignalPoolPrice>>>,
}
#[derive(Debug, serde::Serialize)]
pub struct SignalPriceChange {
    pub pool_id: usize, pub identity: SignalPool,
    pub price: Option<Arc<SignalPoolPrice>>,
}
/// Apply in stream order. A snapshot replaces the generation; clear withdraws all prices.
/// IDs address this published state only, never another worker's private memory.
#[derive(Debug, serde::Serialize)]
pub struct SignalStateUpdate {
    pub sequence: u64, pub revision: u64, pub graph_generation: u64,
    pub trigger: Arc<SignalTrigger>, pub ready_at_us: u64,
    pub price_pipeline: Option<PricePipelineTiming>,
    pub snapshot: Option<SignalSnapshot>, pub clear: bool,
    pub changes: Vec<SignalPriceChange>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalRangeChange { Entered, Updated, Exited }
#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalLeg {
    pub slot: usize,
    #[serde(with = "super::execution::pubkey_json")]
    pub input_mint: Pubkey,
    #[serde(with = "super::execution::pubkey_json")]
    pub output_mint: Pubkey,
    pub log_weight_units: i64,
    pub observation: Arc<PoolPriceObservation>,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalWitness {
    pub legs: Vec<SignalLeg>,
    /// Current score for Entered/Updated; prior score for Exited.
    pub log_sum_units: i64, pub negative_log_sum: f64, pub indicative_return_bps: Option<f64>,
}
/// Bounds/addends use SIGNAL_LOG_SCALE units. All available partner-role members
/// with lower <= weight < upper, excluding
/// the anchor's pool for Direct2. None lower means -infinity. No top-k truncation.
/// Resolve against the State update of the same revision before applying the next.
/// Previous/current addends determine every member's old/new score in O(1).
#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalRouteRange {
    pub group_id: usize, pub reverse: bool, pub anchor_is_left: bool,
    pub anchor_slot: usize, pub partner_role: usize, pub exclude_pool: Option<usize>,
    pub lower_inclusive: Option<i64>, pub upper_exclusive: i64,
    pub previous_addend: Option<i64>, pub current_addend: Option<i64>,
    pub change: SignalRangeChange, pub witness: SignalWitness,
}
pub const SIGNAL_BLOCK_RANGES: usize = 64;
#[derive(Debug, serde::Serialize)]
pub struct SignalRangeBlock {
    pub sequence: u64, pub revision: u64, pub graph_generation: u64,
    pub trigger: Arc<SignalTrigger>, pub threshold_bps: f64, pub threshold_units: i64,
    pub emitted_at_us: u64, pub price_pipeline: Option<PricePipelineTiming>,
    pub ranges: Vec<SignalRouteRange>,
}
#[derive(Debug, serde::Serialize)]
pub struct SignalEvaluation {
    pub sequence: u64, pub revision: u64, pub graph_generation: u64,
    pub trigger: Arc<SignalTrigger>, pub price_pipeline: Option<PricePipelineTiming>,
    pub worker_received_at_us: u64, pub completed_at_us: u64,
    pub price_queue_depth: usize,
    pub groups_visited: u64, pub logical_routes_affected: u64,
    pub anchors_visited: u64, pub ready_anchors: [u64; 4], pub range_queries: u64,
    /// Counts intervals, not individual concrete routes.
    pub ranges: [u64; 3], pub ranges_by_kind: [u64; 4],
    pub logarithms_prepared: u64, pub price_preparation_ns: u64, pub index_update_ns: u64,
    /// Input acceptance to complete factorized state/index readiness, before output.
    pub state_ready_ns: u64,
    pub plan_build_ns: u64, pub query_ns: u64, pub event_build_ns: u64,
    pub publication_ns: u64, pub first_range_ns: Option<u64>, pub last_range_ns: Option<u64>,
    pub processing_ns: u64,
}
#[derive(Debug, serde::Serialize)]
pub enum SignalEvent {
    State(SignalStateUpdate),
    Ranges(SignalRangeBlock),
    Candidates(SignalCandidateBatch),
    Evaluated(SignalEvaluation),
}
pub type SignalStream = tokio::sync::broadcast::Receiver<Arc<SignalEvent>>;
