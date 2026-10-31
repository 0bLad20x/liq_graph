//! Complete execution jobs. No graph selection, pool qualification or protocol decoding.
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_pubkey::Pubkey;
use super::{ExecutionManifest, MintProof};

/// A producer supplies the exact pool set. The consumer never repairs or replaces it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPool {
    pub manifest: Arc<ExecutionManifest>,
    pub mints: [MintProof; 2],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionGroup {
    Direct {
        #[serde(with = "pubkey_json")]
        target: Pubkey,
        pools: Vec<ExecutionPool>,
    },
    Bridge {
        #[serde(with = "pubkey_json")]
        target: Pubkey,
        #[serde(with = "pubkey_json")]
        bridge: Pubkey,
        intermediate: ExecutionPool,
        bridges: Vec<ExecutionPool>,
        direct: Vec<ExecutionPool>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionParameters {
    /// Settlement base units; V10 requires net profit strictly above this value.
    pub minimum_profit: u64,
    /// Zero delegates amount selection to the on-chain executor.
    pub trade_size: u64,
    /// Simulation trading capital comes from the executor's settlement loan vault.
    pub use_flashloan: bool,
    pub no_failure: bool,
    pub constant_conversion: bool,
    pub executor_compute_unit_limit: u32,
    pub transaction_compute_unit_limit: u32,
    pub loaded_accounts_data_size_limit: u32,
    /// Absolute lamports in the Solana v1 transaction configuration.
    pub priority_fee_lamports: u64,
    pub additional_fee_bp: u16,
    #[serde(with = "optional_pubkey_json")]
    pub additional_fee_collector: Option<Pubkey>,
}
impl Default for ExecutionParameters {
    fn default() -> Self {
        Self { minimum_profit: 0, trade_size: 0, use_flashloan: true,
            no_failure: false, constant_conversion: false,
            executor_compute_unit_limit: 1_000_000,
            transaction_compute_unit_limit: 1_400_000,
            loaded_accounts_data_size_limit: 64 * 1024 * 1024,
            priority_fee_lamports: 0, additional_fee_bp: 0,
            additional_fee_collector: None }
    }
}

/// One request describes one transaction, with a frozen copy of all pool inputs.
/// No DB/Registry handles, graph-local IDs, private keys or raw market signals.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRequest {
    pub id: String,
    #[serde(with = "pubkey_json")]
    pub wallet: Pubkey,
    #[serde(with = "pubkey_json")]
    pub settlement: Pubkey,
    pub groups: Vec<ExecutionGroup>,
    pub conversion: Option<ExecutionPool>,
    pub parameters: ExecutionParameters,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOutcome {
    Rejected,
    RpcFailure,
    SimulationFailed,
    SimulatedNoDexCallObserved,
    SimulatedDexCallsObserved,
}
#[derive(Clone, Debug, Serialize)]
pub struct ExecutionPoolUse {
    #[serde(with = "pubkey_json")]
    pub pool: Pubkey,
    #[serde(with = "pubkey_json")]
    pub quote: Pubkey,
    pub validated_slot: u64,
}
#[derive(Clone, Debug, Serialize)]
pub struct ExecutionMetrics {
    pub groups: usize,
    pub pool_declarations: usize,
    pub unique_accounts: usize,
    pub account_references: usize,
    pub instruction_bytes: usize,
    pub transaction_bytes: usize,
    pub pools: Vec<ExecutionPoolUse>,
}
#[derive(Debug, Serialize)]
pub struct ExecutionReport {
    pub id: String,
    pub outcome: ExecutionOutcome,
    pub phase: String,
    pub error: Option<String>,
    pub metrics: Option<ExecutionMetrics>,
    /// Signature of the simulated transaction; this is never a submission receipt.
    pub simulated_signature: Option<String>,
    pub blockhash: Option<String>,
    pub last_valid_block_height: Option<u64>,
    pub context_slot: Option<u64>,
    pub provider: Option<String>,
    pub blockhash_retried: bool,
    pub executor_invoked: bool,
    pub dex_invocations: Vec<String>,
    /// Actual CPI order and net wallet token flows, including rolled-back attempts.
    pub trace: ExecutionTrace,
    /// Worker timing, accumulated across signed-mode retries; no queue time.
    pub build_us: Option<u64>,
    pub simulation_rpc_ms: Option<u64>,
    pub elapsed_ms: u64,
    pub trace_us:u64,
    /// Complete RPC simulation value, including err/logs/innerInstructions/accounts.
    pub simulation: Option<Value>,
}

/// Observed wallet changes from a successful simulation, in native raw units.
/// Missing RPC balance evidence remains unknown; rolled-back transfers are excluded.
#[derive(Debug,Serialize)]
pub struct ExecutionBalanceChanges {
    pub settlement_gain_raw:Option<String>,
    pub native_wallet_change_lamports:Option<String>,
    pub new_account_lamports:Option<u64>,
    /// Only comparable when settlement is WSOL; excludes new-account funding.
    pub above_network_fee:Option<bool>,
}

#[derive(Clone,Debug,Default,Serialize,Deserialize)]
pub struct ExecutionTrace {
    pub status:String,
    pub calls:Vec<ExecutionSwapCall>,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct ExecutionSwapCall {
    pub program:String,pub pool:Option<String>,pub stack_height:Option<u64>,
    pub status:String,pub issues:Vec<String>,
    pub inputs:Vec<ExecutionAssetAmount>,pub outputs:Vec<ExecutionAssetAmount>,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct ExecutionAssetAmount {pub mint:String,pub amount_raw:u64,pub gross_amount_raw:u64}

pub mod pubkey_json {
    use serde::{Deserialize, Deserializer, Serializer};
    use solana_pubkey::Pubkey;
    pub fn serialize<S: Serializer>(key: &Pubkey, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&key.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Pubkey, D::Error> {
        String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
    }
}
pub mod optional_pubkey_json {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use solana_pubkey::Pubkey;
    pub fn serialize<S: Serializer>(key: &Option<Pubkey>, s: S) -> Result<S::Ok, S::Error> {
        key.map(|v| v.to_string()).serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Pubkey>, D::Error> {
        Option::<String>::deserialize(d)?.map(|v| v.parse().map_err(serde::de::Error::custom)).transpose()
    }
}
