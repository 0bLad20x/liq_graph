//! Shared Completion messages. Admission and scheduling policy belong to their workers.
use std::{sync::Arc,time::Duration};
use serde_json::Value;
use solana_pubkey::Pubkey;
use super::{PoolState,now_unix_ms,execution};

/// Admission is a Registry-owned decision. The Mint Registry supplies qualified
/// identities; the Registry applies the configured floor to fresh canonical pool TVL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionAdmission {
    Admitted,
    MissingIdentity,
    MintNotQualified,
    TvlBelowMinimum,
    TvlUnavailable,
}

impl CompletionAdmission {
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
    /// On-chain feature mode can change while the required V10 instruction variant is unavailable.
    DynamicModeUnsupported,
    ConfigurationRequired,
    LegacyUnclassified,
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

/// Validated vault roles in the pool's mint_a / mint_b orientation. Stored with
/// the same completion proof as the manifest; balances remain live-only data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolVaults {
    #[serde(with = "execution::pubkey_json")]
    pub vault_a: Pubkey,
    #[serde(with = "execution::pubkey_json")]
    pub vault_b: Pubkey,
}
impl PoolVaults {
    pub fn matches_manifest(self, manifest: &ExecutionManifest) -> bool {
        self.vault_a != Pubkey::default() && self.vault_b != Pubkey::default()
            && self.vault_a != self.vault_b
            && [&manifest.base_a, &manifest.base_b].iter().all(|block|
                [self.vault_a, self.vault_b].iter().all(|key|
                    block.iter().any(|account| account.address == *key && account.writable)))
    }
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

impl ExecutionManifest {
    /// Stable account dependency identity. A renewed proof slot alone does not
    /// change subscriptions; either oriented account block or signer does.
    pub fn account_set_digest(&self) -> [u8;32] {
        let mut digest=ring::digest::Context::new(&ring::digest::SHA256);
        digest.update(b"liq_graph_manifest_accounts_v1");
        for key in [self.pool,self.mint_a,self.mint_b] {digest.update(key.as_ref());}
        digest.update(&[u8::from(self.bound_signer.is_some())]);
        if let Some(signer)=self.bound_signer {digest.update(signer.as_ref());}
        for block in [&self.base_a,&self.base_b] {
            digest.update(&(block.len() as u64).to_le_bytes());
            for account in block.iter() {
                digest.update(account.address.as_ref());digest.update(&[u8::from(account.writable)]);
            }
        }
        digest.finish().as_ref().try_into().expect("SHA256 length")
    }
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
    pub vaults: Option<PoolVaults>,
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
        Self { pool, status, validated_slot, manifest, vaults:None, reason, reason_code,
            checked_at_ms, evidence, mints }
    }

    pub fn with_reason_code(mut self, reason: CompletionReason) -> Self {
        self.reason_code = Some(reason);
        self
    }

    pub fn with_vaults(mut self, vaults: PoolVaults) -> Self {
        self.vaults = Some(vaults);
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
    /// Revalidate all protocol prerequisites; never use a cached shortcut.
    pub force_full: bool,
    pub pool: PoolState,
}

#[derive(Debug)]
pub struct CompletionRequestBatch {
    pub requests: Vec<CompletionRequest>,
    pub timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct CompletionUnchanged {
    pub pool: Pubkey,
    pub baseline_validated_slot: u64,
    pub baseline_checked_at_ms: u64,
    pub baseline_state: CompletionState,
    pub observed_slot: u64,
    pub checked_at_ms: u64,
}

impl CompletionUnchanged {
    /// A successful lightweight check confirms only existing structure, not a new full proof.
    pub fn from_pool(pool: &PoolState, observed_slot: u64) -> Option<Self> {
        let validated = pool.completion_validated_slot?;
        let checked = pool.completion_checked_at_ms?;
        if pool.completion_state == CompletionState::Unknown || observed_slot <= validated {
            return None;
        }
        Some(Self {
            pool: pool.address, baseline_validated_slot: validated,
            baseline_checked_at_ms: checked, baseline_state: pool.completion_state,
            observed_slot, checked_at_ms: now_unix_ms().ok()?,
        })
    }
}

#[derive(Debug)]
pub enum CompletionOutcome {
    Proof(CompletionResult),
    Unchanged(CompletionUnchanged),
}

impl CompletionOutcome {
    pub fn pool(&self) -> Pubkey {
        match self {
            Self::Proof(result) => result.pool,
            Self::Unchanged(check) => check.pool,
        }
    }
}

#[derive(Debug)]
pub struct CompletionResponse {
    pub id: u64,
    pub outcome: CompletionOutcome,
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

