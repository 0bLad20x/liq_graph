//! Shared source contracts and generic HTTP infrastructure.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde_json::Value;
use solana_pubkey::Pubkey;

/// Only venues actually emitted by implemented source workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Venue {
    MeteoraDlmm,
}

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

pub fn now_unix_ms() -> Result<u64> {
    Ok(u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?)
}

/// Generic HTTP client; no DEX or protocol logic.
pub fn http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?)
}
