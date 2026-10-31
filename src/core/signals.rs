//! Versioned compact route state and best-candidate outputs. Data contracts only.
use std::sync::Arc;
use solana_pubkey::Pubkey;
use super::market_state::{PoolPriceObservation, PriceOrigin, PricePipelineTiming};

/// Canonical log-weight unit for the wire contract (Q48).
pub const SIGNAL_LOG_SCALE: i64 = 1_i64 << 48;
pub const SIGNAL_SCORE_BASIS: &str = "marginal_dex_net";

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
/// None means that direction has no usable marginal DEX-net estimate.
#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalPoolPrice {
    /// Independent directed marginal DEX-net weights. No gross fallback.
    pub log_weight_units: [Option<i64>; 2],
    /// CP9A: eligible / dust / unknown, oriented like log_weight_units.
    pub direction_liquidity_status: [&'static str; 2],
    pub observation: Arc<PoolPriceObservation>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalRouteKind { Direct2, Direct3, Triangle3, Triangle4 }
impl SignalRouteKind {
    pub const ALL:[Self;4]=[Self::Direct2,Self::Direct3,Self::Triangle3,Self::Triangle4];
    pub fn name(self)->&'static str {match self {
        Self::Direct2=>"direct2",Self::Direct3=>"direct3",
        Self::Triangle3=>"triangle3",Self::Triangle4=>"triangle4",
    }}
}
/// One operational route scope for graph publication and all consumers.
#[derive(Clone,Copy,Debug)]
pub struct EnabledRoutes {mask:u8}
impl EnabledRoutes {
    pub fn from_env()->anyhow::Result<Self> {
        let configured=match std::env::var("GRAPH_ENABLED_ROUTES") {
            Ok(value)=>value,Err(std::env::VarError::NotPresent)=>"direct2".into(),
            Err(error)=>return Err(error.into()),
        };
        Self::parse(&configured)
    }
    pub fn parse(configured:&str)->anyhow::Result<Self> {
        let mut routes=Self{mask:0};
        for name in configured.split(',').map(str::trim) {
            let kind=SignalRouteKind::ALL.into_iter().find(|k|k.name()==name)
                .ok_or_else(||anyhow::anyhow!("unknown GRAPH_ENABLED_ROUTES entry '{name}'; use direct2,direct3,triangle3,triangle4"))?;
            anyhow::ensure!(!routes.contains(kind),"duplicate GRAPH_ENABLED_ROUTES entry '{name}'");
            routes.mask|=1_u8<<(kind as u8);
        }
        Ok(routes)
    }
    pub fn contains(self,kind:SignalRouteKind)->bool {self.mask&(1_u8<<(kind as u8))!=0}
    pub fn names(self)->Vec<&'static str> {
        SignalRouteKind::ALL.into_iter().filter(|&kind|self.contains(kind)).map(SignalRouteKind::name).collect()
    }
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
    /// Only present for entering/updating net-qualifying candidates.
    pub witness: Option<SignalWitness>,
}
/// Current membership, not an Entered event or an instruction to execute.
#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalActiveCandidate {
    pub topology_id: usize,
    pub reverse: bool,
    pub witness: SignalWitness,
}
/// Work and membership changes between the two actually installed generations.
#[derive(Default, Debug, serde::Serialize)]
pub struct SignalCatalogReconciliation {
    pub previous_generation: u64,
    pub changed_pairs: usize,
    pub added_topologies: usize,
    pub removed_topologies: usize,
    pub reused_topologies: usize,
    pub reevaluated_topologies: usize,
    pub entered_candidates: usize,
    pub updated_candidates: usize,
    pub exited_candidates: usize,
    pub unchanged_candidates: usize,
    pub active_candidates: usize,
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
    pub score_basis: &'static str,
    pub graph_generation: u64,
    pub log_weight_scale: i64,
    pub compact_topologies: Arc<[SignalTopology]>,
    pub route_options: u64,
}
#[derive(Debug, serde::Serialize)]
pub struct SignalSnapshot {
    pub plan: Arc<SignalPlan>, pub pools: Vec<SignalPool>,
    pub prices: Vec<Option<Arc<SignalPoolPrice>>>,
    pub threshold_units: i64,
    /// Authoritative replacement after catalog reconciliation. Missing keys are withdrawn.
    /// Pool slots and topology IDs refer exclusively to this snapshot's state/plan.
    pub candidates: Vec<SignalActiveCandidate>,
}
#[derive(Debug, serde::Serialize)]
pub struct SignalPriceChange {
    pub pool_id: usize, pub identity: SignalPool,
    pub price: Option<Arc<SignalPoolPrice>>,
}
/// Apply in stream order. A snapshot replaces the generation AND candidate membership;
/// clear withdraws all prices and candidates. Neither operation requests execution.
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
    /// Sum of the directed marginal DEX-net Q48 weights.
    pub log_sum_units: i64,
    pub estimated_net_return_bps: Option<f64>,
}
#[derive(Debug, serde::Serialize)]
pub struct SignalEvaluation {
    pub sequence: u64, pub revision: u64, pub graph_generation: u64,
    pub trigger: Arc<SignalTrigger>, pub price_pipeline: Option<PricePipelineTiming>,
    pub worker_received_at_us: u64, pub completed_at_us: u64,
    pub price_queue_depth: usize,
    pub groups_visited: u64, pub logical_routes_affected: u64,
    pub anchors_visited: u64, pub ready_anchors: [u64; 4], pub range_queries: u64,
    /// Counts best-candidate transitions, not individual pool-combination routes.
    pub ranges: [u64; 3], pub ranges_by_kind: [u64; 4],
    /// Catalog transitions are delivered by the authoritative State snapshot,
    /// separately from ordinary Candidates change events counted in ranges.
    pub catalog: Option<SignalCatalogReconciliation>,
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
    Candidates(SignalCandidateBatch),
    Evaluated(SignalEvaluation),
}
pub type SignalStream = tokio::sync::broadcast::Receiver<Arc<SignalEvent>>;
