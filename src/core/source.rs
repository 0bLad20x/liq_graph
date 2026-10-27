//! Shared source observations; no provider decoding or admission policy.
use serde_json::Value;
use solana_pubkey::Pubkey;
use super::Venue;

/// An immutable decoded response, fan-out via Arc<DataBatch> without payload copies.
#[derive(Debug)]
pub struct DataBatch {
    pub source: &'static str,
    pub received_at_ms: u64,
    pub records: Vec<DataRecord>,
}

#[derive(Debug)]
pub struct DataRecord {
    pub identity: RecordIdentity,
    /// Source-normalized USD TVL. Never infer this from provider JSON in a consumer.
    pub tvl_usd: Option<f64>,
    /// Normalized pool configuration. None carries no configuration update.
    pub config: Option<PoolConfigUpdate>,
    /// Complete source object: no filtering, flattening or consumer-specific projection.
    pub payload: Value,
}

#[derive(Debug)]
pub enum RecordIdentity {
    Pool {
        address: Pubkey,
        venue: Venue,
        mint_a: Option<Pubkey>,
        mint_b: Option<Pubkey>,
    },
    Mint {
        address: Pubkey,
    },
}

/// Source-normalized patch. Field names are independent of provider JSON paths.
/// Missing values retain prior fields; removals must be explicit.
#[derive(Debug)]
pub struct PoolConfigUpdate {
    pub values: serde_json::Map<String, Value>,
    pub remove_prefixes: &'static [&'static str],
    pub remove_keys: &'static [&'static str],
    pub observation: Option<ConfigObservation>,
}

/// A related group of observed values whose timestamp can be coalesced by a
/// persistence consumer when the values have not changed. This is no fee model.
#[derive(Clone, Copy, Debug)]
pub struct ConfigObservation {
    pub value_prefix: &'static str,
    pub timestamp_key: &'static str,
}
