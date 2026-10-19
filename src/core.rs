//! Shared worker contracts and generic HTTP infrastructure.
pub mod logging;
pub mod execution;
pub mod market_state;
pub mod signals;
use std::{sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};

use anyhow::Result;
use serde_json::Value;
use solana_pubkey::Pubkey;

/// Shared completion contract. TVL is the Registry's current canonical value;
/// callers must never substitute a historical observation or provider payload.
pub const COMPLETION_MIN_TVL_USD: f64 = 500.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionAdmission {
    Admitted,
    MissingIdentity,
    TvlUnavailable,
    BelowMinimumTvl,
}

impl CompletionAdmission {
    pub fn from_inputs(a: Option<Pubkey>, b: Option<Pubkey>, tvl: Option<f64>) -> Self {
        let (Some(a), Some(b)) = (a, b) else { return Self::MissingIdentity };
        if a == b || a == Pubkey::default() || b == Pubkey::default() {
            return Self::MissingIdentity;
        }
        match tvl {
            Some(v) if v.is_finite() && v >= COMPLETION_MIN_TVL_USD => Self::Admitted,
            Some(v) if v.is_finite() && v >= 0.0 => Self::BelowMinimumTvl,
            _ => Self::TvlUnavailable,
        }
    }

    pub fn is_admitted(self) -> bool { self == Self::Admitted }
}

/// Stable machine-readable reasons; the diagnostic message is never a policy key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionReason {
    TransientFailure,
    ValidationFailed,
    MissingExecutionAccounts,
    UnsupportedConfiguration,
    ConfigurationRequired,
    LegacyUnclassified,
}

impl CompletionReason {
    /// These reasons require a relevant configuration/support change, not an
    /// unchanged account retry. CP2 will own the actual retry schedule.
    pub fn recheckable(self) -> bool {
        !matches!(self, Self::UnsupportedConfiguration | Self::ConfigurationRequired)
    }
}

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

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountMeta {
    #[serde(with = "execution::pubkey_json")]
    pub address: Pubkey,
    pub writable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionManifest {
    #[serde(with = "execution::pubkey_json")]
    pub pool: Pubkey,
    #[serde(with = "execution::pubkey_json")]
    pub mint_a: Pubkey,
    #[serde(with = "execution::pubkey_json")]
    pub mint_b: Pubkey,
    pub validated_slot: u64,
    /// Wallet context of prepared pool accounts, when those accounts depend on a signer.
    #[serde(default, with = "execution::optional_pubkey_json")]
    pub bound_signer: Option<Pubkey>,
    pub base_a: Arc<[AccountMeta]>,
    pub base_b: Arc<[AccountMeta]>,
}

/// Versioned protocol evidence; consumers do not interpret the schema.
#[derive(Clone, Debug)]
pub struct CompletionEvidence {
    pub kind: String,
    pub version: u32,
    pub payload: Arc<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MintProof {
    #[serde(with = "execution::pubkey_json")]
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
    pub reason_code: Option<CompletionReason>,
    /// Completion finished at this wall-clock time. Retryable attempts do not
    /// establish or renew a validated proof timestamp.
    pub checked_at_ms: Option<u64>,
    pub evidence: Option<Arc<CompletionEvidence>>,
    pub mints: Option<[MintProof; 2]>,
}

impl CompletionResult {
    pub fn new(pool: Pubkey, status: CompletionStatus, validated_slot: Option<u64>,
        reason: Option<String>, evidence: Option<Arc<CompletionEvidence>>,
        mints: Option<[MintProof; 2]>, manifest: Option<Arc<ExecutionManifest>>) -> Self {
        let reason_code = match status {
            CompletionStatus::Prepared => None,
            CompletionStatus::Incomplete => Some(CompletionReason::ValidationFailed),
            CompletionStatus::Retryable => Some(CompletionReason::TransientFailure),
        };
        let checked_at_ms = if status == CompletionStatus::Retryable { None }
            else { now_unix_ms().ok() };
        Self { pool, status, validated_slot, manifest, reason, reason_code,
            checked_at_ms, evidence, mints }
    }

    pub fn with_reason_code(mut self, reason: CompletionReason) -> Self {
        self.reason_code = Some(reason);
        self
    }
}

#[derive(Debug)]
pub struct CompletionResultBatch {
    pub source: &'static str,
    pub results: Vec<CompletionResult>,
}

/// Why the lifecycle requested a protocol check. These are attempt intents,
/// separate from the durable Unknown/Prepared/Incomplete state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionWork { Initial, Recheck, Refresh, Retry }

#[derive(Debug)]
pub struct CompletionRequest {
    pub id: u64,
    pub work: CompletionWork,
    pub pool: PoolState,
}

#[derive(Debug)]
pub struct CompletionRequestBatch {
    pub requests: Vec<CompletionRequest>,
    pub timeout: Duration,
}

#[derive(Debug)]
pub struct CompletionResponse {
    pub id: u64,
    pub result: CompletionResult,
}

#[derive(Debug)]
pub struct CompletionResponseBatch {
    pub source: &'static str,
    pub responses: Vec<CompletionResponse>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionState {
    Unknown,
    Prepared,
    Incomplete,
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

