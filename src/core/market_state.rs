//! Shared indicative pool-price stream. No execution sizing or admission policy.
use std::sync::Arc;
/// Prices use human token units: quote-mint units per one base-mint unit.
/// The canonical base/quote orientation is deterministic across venues.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub enum PriceKind { DlmmActiveBin, ClmmSpot, ReserveSpot, OrderbookMidpoint }
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub enum Consistency {
    SingleAccount,
    SameSlotNonAtomic,
    MixedSlotNonAtomic,
    OrderbookBboIndicative,
}
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub enum PriceOrigin { Baseline, Websocket, Revaluation }
/// Local pipeline boundaries, starting when a text frame is read from the socket.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct PricePipelineTiming {
    pub frame_received_at_us:u64, pub transport_decoded_at_us:u64,
    pub market_received_at_us:u64, pub price_published_at_us:u64,
    /// Monotonic durations: protocol field decode, then normalized price calculation.
    pub market_decode_ns:u64, pub market_compute_ns:u64,
    /// Catch-up after initialization/resync; excluded from steady-state latency.
    pub initialization_replay:bool,
}
/// CP8A estimates in human units, in canonical base->quote / quote->base order.
/// Marginal DEX-net estimates are not amount-specific profit or sizing proofs.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct FeeValuation {
    pub model: &'static str,
    pub estimated_net_rates: [Option<f64>;2],
    pub dex_fee_rates: [Option<f64>;2],
    pub fees_on_input: [bool;2],
    pub base_fee_rate: Option<f64>, pub variable_fee_rate: Option<f64>,
    pub lp_fee_bps: Option<u64>, pub protocol_fee_bps: Option<u64>, pub creator_fee_bps: Option<u64>,
    pub fee_schedule: Option<&'static str>, pub market_cap_raw: Option<String>,
    pub chain_timestamp: i64, pub chain_epoch:u64,
    pub token_fee_status: [&'static str;2],
    pub unknown_reason: Option<String>,
    /// Host rebates, integer rounding and price impact are not estimated here.
    pub scope: &'static str,
}

/// A protocol-independent description of observed directed price levels.
/// Raw amounts belong to the named input/output mints. No token UI scaling is
/// applied to integer fee and fill calculations. Coverage is not a pool veto.
#[derive(Clone,Debug,PartialEq,serde::Serialize)]
pub struct LiquidityLevel {
    pub source_position:i32,
    pub gross_rate:f64,
    pub net_rate:f64,
    pub fee_rate:f64,
    pub fees_on_input:bool,
    /// Exact atom-output / atom-input ratio; integers serialize as decimal text.
    #[serde(serialize_with="decimal_u128")]
    pub rate_numerator:u128,
    #[serde(serialize_with="decimal_u128")]
    pub rate_denominator:u128,
    /// Ordered output components; rounding applies to each component separately.
    pub output_parts_raw:[u64;3],
    pub fee_numerator:u64,pub fee_denominator:u64,
    /// Upper bound on the refundable part of an input fee. Bounds may assume
    /// this entire fraction is returned; estimates omit rebates.
    pub maximum_rebate_bps:u16,
    /// Full-level output/input after integer rounding, unlike marginal net_rate.
    pub full_level_net_rate:Option<f64>,
    /// Cost/output of filling this level, including integer fee rounding.
    /// None means its complete input does not fit the protocol's u64 amount.
    pub input_raw:Option<u64>,
    pub output_raw:u64,
    pub minimum_input_raw:Option<u64>,
    pub fee_raw:Option<u64>,
}
#[derive(Clone,Debug,PartialEq,serde::Serialize)]
pub struct DirectedLiquidity {
    pub input_mint:String,pub output_mint:String,
    pub input_decimals:u8,pub output_decimals:u8,
    /// available / empty / uncovered / unknown. Empty is relative to coverage.
    pub status:&'static str,
    /// exhausted_bitmap / execution_window_end / account_unavailable / invalid /
    /// amount_unrepresentable. Exhaustion refers to the observed bitmap only.
    pub coverage:&'static str,
    pub next_required_account:Option<String>,
    pub first_source_position:Option<i32>,
    pub marginal_net_rate:Option<f64>,
    /// Whether unobserved later levels can only worsen the raw exchange rate.
    pub monotone_rate_tail:bool,
    pub input_capacity_raw:Option<String>,pub output_capacity_raw:String,
    /// Complete live calculation data. Ordinary JSON evidence records count and
    /// first level; explicit raw-account evidence can include the full vector.
    #[serde(serialize_with="level_summary")]
    pub levels:Vec<LiquidityLevel>,
    pub reason:Option<String>,
}
fn level_summary<S:serde::Serializer>(levels:&[LiquidityLevel],serializer:S)->Result<S::Ok,S::Error> {
    #[derive(serde::Serialize)]
    struct Summary<'a>{count:usize,first:Option<&'a LiquidityLevel>}
    serde::Serialize::serialize(&Summary{count:levels.len(),first:levels.iter().find(|l|l.output_raw>0)},serializer)
}
fn decimal_u128<S:serde::Serializer>(value:&u128,serializer:S)->Result<S::Ok,S::Error> {
    serializer.serialize_str(&value.to_string())
}
/// Reserve curve facts, not a protocol decoder or quoting implementation.
#[derive(Clone,Debug,PartialEq,serde::Serialize)]
pub struct ReserveCurve {
    pub base_mint:String,pub base_reserve_raw:u64,
    pub effective_quote_reserve_raw:u64,pub spendable_quote_reserve_raw:u64,
    /// First charge is retained LP fee; other quote charges may be rebated.
    pub fee_bps:[u64;3],
    /// Conservative quoting padding; zero for models without such padding.
    pub buy_input_padding_raw:u64,
}
#[derive(Clone,Debug,PartialEq,serde::Serialize)]
pub struct PoolLiquidity {
    pub model:&'static str,pub revision:u64,
    /// Material identity survives rehydration/revision restarts.
    pub content_digest:[u8;32],
    pub account_set_digest:[u8;32],
    pub execution_accounts:Vec<String>,
    /// None uses the exact directed levels; Some describes a reserve curve.
    pub reserve_curve:Option<ReserveCurve>,
    /// Canonical base->quote / quote->base, like FeeValuation.
    pub directions:[DirectedLiquidity;2],
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct PoolPriceObservation {
    pub origin: PriceOrigin,
    pub pool: String,
    pub venue: &'static str,
    pub mint_a: String,
    pub mint_b: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub price_quote_per_base: f64,
    pub valuation: Option<FeeValuation>,
    pub liquidity:Option<Arc<PoolLiquidity>>,
    pub previous_price: Option<f64>,
    pub delta_bps: Option<f64>,
    pub price_kind: PriceKind,
    pub best_bid_quote_per_base: Option<f64>,
    pub best_ask_quote_per_base: Option<f64>,
    pub consistency: Consistency,
    pub slot: u64,
    pub received_at_us: u64,
    pub sequence: u64,
    pub component_slot_min: u64,
    pub component_slot_max: u64,
    pub component_received_at_us_min: u64,
    pub component_received_at_us_max: u64,
    pub trigger_field: Option<&'static str>,
    pub pipeline: Option<PricePipelineTiming>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum PoolRemovalReason { Selection, Shutdown }
#[derive(Clone, Debug, serde::Serialize)]
pub enum MarketStateEvent {
    Price(PoolPriceObservation),
    Unavailable {pool:String, venue:&'static str, slot:u64,
        received_at_us:u64, sequence:u64, reason:String},
    Gap {reason:String, sequence:u64},
    Removed {pool:String, sequence:u64, reason:PoolRemovalReason},
    Selection {prepared:usize, selected:Vec<String>, accounts:usize, sequence:u64},
    Stats {pools:usize, ready:usize, available:usize, accounts:usize, active:usize,
        ws_messages:u64, ws_text_bytes:u64, sequence:u64},
}
/// An event stream is not a durable event log: lagging consumers receive RecvError::Lagged.
/// Consumers must handle that explicitly and can request a fresh producer baseline on restart.
pub type PriceEventStream = tokio::sync::broadcast::Receiver<std::sync::Arc<MarketStateEvent>>;

