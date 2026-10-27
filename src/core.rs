//! Shared worker contracts and generic HTTP infrastructure.
pub mod logging;
pub mod source;
pub mod execution;
pub mod market_state;
pub mod signals;
pub mod account_stream;
pub mod completion;
pub mod rpc;
pub use completion::{CompletionAdmission,CompletionReason,CompletionStatus,AccountMeta,ExecutionManifest,CompletionEvidence,MintProof,CompletionResult,CompletionResultBatch,CompletionWork,CompletionRequest,CompletionRequestBatch,CompletionUnchanged,CompletionOutcome,CompletionResponse,CompletionResponseBatch,CompletionState};
use std::{sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};

use anyhow::Result;
use serde_json::Value;
use solana_pubkey::Pubkey;

/// Only venues actually emitted by implemented source workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Venue {
    MeteoraDlmm,
    MeteoraDammV2,
    RaydiumAmmV4,
    RaydiumCpmm,
    RaydiumClmm,
    RaydiumStable,
    OrcaWhirlpool,
    PancakeSwapClmm,
    ByrealClmm,
    Manifest,
    PumpSwap,
}

pub use source::{DataBatch, DataRecord, RecordIdentity, PoolConfigUpdate, ConfigObservation};

pub fn now_unix_ms() -> Result<u64> {
    Ok(u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?)
}

/// Persisted economic/static mint state shared by Mint Registry and Persistence.
/// Values are optional when the provider cannot attest them; absence is not zero.
#[derive(Clone, Debug)]
pub struct MintState {
    pub address: Pubkey,
    pub token_program: Option<Pubkey>,
    pub decimals: Option<u8>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub liquidity_usd: Option<f64>,
    pub market_cap_usd: Option<f64>,
    pub holder_count: Option<u64>,
    pub usd_price: Option<f64>,
    pub fetched_at_ms: u64,
    /// Authoritative on-chain extension/fee state, independently versioned from Jupiter.
    pub onchain: Option<Arc<MintOnchainState>>,
}

/// Complete attested on-chain mint data, including raw TLV payloads for extensions
/// whose application-specific execution cost cannot be determined from a Mint alone.
#[derive(Clone, Debug)]
pub struct MintOnchainState {
    pub address: Pubkey,
    pub owner: Pubkey,
    pub decimals: u8,
    pub verified_slot: u64,
    pub verified_at_ms: u64,
    pub token_extensions: Value,
    pub transfer_fee_config: Value,
}

/// Source-independent changes; a single Mint Registry writer validates slot order.
#[derive(Clone, Debug)]
pub struct MintOnchainBatch { pub states: Vec<MintOnchainState> }

/// Shared read-only contract. No worker accesses another worker's implementation.
pub trait MintReader: Send + Sync {
    fn mint(&self, address: Pubkey) -> Option<MintState>;
}

/// Generic HTTP client; no DEX or protocol logic.
pub fn http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?)
}

/// One current pool observation, including accepted execution-completion results.
#[derive(Clone, Debug)]
pub struct PoolState {
    pub address: Pubkey,
    pub venue: Venue,
    pub mint_a: Option<Pubkey>,
    pub mint_b: Option<Pubkey>,
    pub source: &'static str,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
    pub last_changed_ms: u64,
    pub tvl_usd: Option<f64>,
    /// Derived once by the Registry. Consumers use this decision, not a local TVL gate.
    pub completion_admission: CompletionAdmission,
    /// Durable, validated protocol configuration, not arbitrary REST JSON.
    pub pool_config: Option<Arc<Value>>,
    /// Historical persisted observation; never treat as a fresh live quote.
    pub last_observation: Option<Arc<Value>>,
    pub completion_state: CompletionState,
    pub completion_validated_slot: Option<u64>,
    pub execution_manifest: Option<Arc<ExecutionManifest>>,
    pub completion_evidence: Option<Arc<CompletionEvidence>>,
    pub mint_proofs: Option<[MintProof; 2]>,
    pub completion_reason: Option<String>,
    pub completion_reason_code: Option<CompletionReason>,
    /// None for historic descriptors without a recorded check time. Refresh
    /// scheduling is external; this timestamp alone does not imply a TTL.
    pub completion_checked_at_ms: Option<u64>,
}

/// Distinguish source observations from completed execution metadata without
/// interpreting worker-specific source names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoolUpdateKind {
    Source,
    Completion,
}

/// Changed records only; last_seen-only refreshes remain accessible in snapshots.
#[derive(Debug)]
pub struct PoolUpdateBatch {
    pub kind: PoolUpdateKind,
    pub source: &'static str,
    pub received_at_ms: u64,
    pub pools: Vec<PoolState>,
    /// Identity/configuration changes only. Excludes TVL-only updates.
    pub persist_identity: Vec<Pubkey>,
}

