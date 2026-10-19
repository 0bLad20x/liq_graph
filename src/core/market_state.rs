//! Shared indicative pool-price stream. No execution sizing or admission policy.
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
pub enum PriceOrigin { Baseline, Websocket }
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
}
#[derive(Clone, Debug, serde::Serialize)]
pub enum MarketStateEvent {
    Price(PoolPriceObservation),
    Unavailable {pool:String, venue:&'static str, slot:u64,
        received_at_us:u64, sequence:u64, reason:String},
    Gap {reason:String, sequence:u64},
    Removed {pool:String, sequence:u64},
    Selection {prepared:usize, selected:Vec<String>, accounts:usize, sequence:u64},
    Stats {pools:usize, available:usize, accounts:usize, active:usize,
        ws_messages:u64, ws_text_bytes:u64, sequence:u64},
}
/// An event stream is not a durable event log: lagging consumers receive RecvError::Lagged.
/// Consumers must handle that explicitly and can request a fresh producer baseline on restart.
pub type PriceEventStream = tokio::sync::broadcast::Receiver<std::sync::Arc<MarketStateEvent>>;

