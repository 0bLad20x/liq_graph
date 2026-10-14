//! Shared source contracts and generic HTTP infrastructure.
use std::{sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};

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

/// Shared typed completion results. Protocol-specific decoding stays inside workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionStatus {
    Prepared,
    Incomplete,
    Retryable,
}

#[derive(Clone, Debug)]
pub struct AccountMeta {
    pub address: Pubkey,
    pub writable: bool,
}

#[derive(Clone, Debug)]
pub struct ExecutionManifest {
    pub pool: Pubkey,
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
    pub validated_slot: u64,
    pub base_a: Arc<[AccountMeta]>,
    pub base_b: Arc<[AccountMeta]>,
}

#[derive(Clone, Debug)]
pub struct BinArrayRef {
    pub index: i32,
    pub address: Pubkey,
}

#[derive(Clone, Debug)]
pub struct BinArrayInventory {
    pub registry_tvl_usd: Option<f64>,
    pub active_id: i32,
    pub active_array_index: i32,
    pub internal_initialized: usize,
    pub extension_initialized: usize,
    pub extension_present: bool,
    /// The complete initialized array-address inventory represented by the fetched bitmaps.
    pub initialized: Arc<[BinArrayRef]>,
    pub selected_indices: Vec<i32>,
    pub validated_indices: Vec<i32>,
}

#[derive(Clone, Debug)]
pub struct MintProof {
    pub address: Pubkey,
    pub token_2022: bool,
    /// Verified Token-2022 Mint TLV extension type identifiers.
    pub extensions: Vec<u16>,
    pub unsupported: Vec<u16>,
}

#[derive(Clone, Debug)]
pub struct CompletionResult {
    pub pool: Pubkey,
    pub status: CompletionStatus,
    pub validated_slot: Option<u64>,
    pub manifest: Option<Arc<ExecutionManifest>>,
    pub reason: Option<String>,
    pub inventory: Option<Arc<BinArrayInventory>>,
    pub mints: Option<[MintProof; 2]>,
}

#[derive(Debug)]
pub struct CompletionResultBatch {
    pub source: &'static str,
    pub results: Vec<CompletionResult>,
}

