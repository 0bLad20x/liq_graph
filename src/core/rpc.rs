//! Typed RPC mailbox and client. No providers, protocol decoders or HTTP implementation.
use anyhow::{Context,Result};
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast,mpsc,oneshot},time::Instant};
pub use super::account_stream::{AccountData,AccountView};
use super::account_stream::{AccountEvent,AccountEventStream,AccountStreamControl};

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum AccountFilter {
    DataSize(usize),
    Memcmp { offset: usize, bytes: Vec<u8> },
}

#[derive(Clone, Debug)]
pub enum RpcOperation {
    Multiple { addresses: Vec<Pubkey>, view: AccountView, min_context_slot: Option<u64> },
    Account { address: Pubkey, view: AccountView },
    Program { program: Pubkey, filters: Vec<AccountFilter>, view: AccountView },
    ProgramV2 {
        program: Pubkey, filters: Vec<AccountFilter>, view: AccountView,
        cursor: Option<String>, changed_since_slot: Option<u64>, limit: usize,
    },
    Slot { commitment: String },
    GenesisHash,
    LatestBlockhash { min_context_slot: Option<u64> },
    /// Read-only execution. Signature verification is always enabled; never broadcasts.
    Simulate { transaction: Vec<u8>, min_context_slot: Option<u64>, accounts: Vec<Pubkey> },
    /// Simulation-only: RPC replaces the blockhash and skips signature verification.
    SimulateFast { transaction: Vec<u8>, min_context_slot: Option<u64>, accounts: Vec<Pubkey> },
}

impl RpcOperation {
    pub(crate) fn bit(&self) -> u64 {
        match self {
            Self::Multiple { .. } => 1,
            Self::Account { .. } => 2,
            Self::Program { .. } => 4,
            Self::ProgramV2 { .. } => 8,
            Self::Slot { .. } => 16,
            Self::GenesisHash => 32,
            Self::LatestBlockhash { .. } => 64,
            Self::Simulate { .. } | Self::SimulateFast { .. } => 128,
        }
    }
}

#[derive(Debug)]
pub enum RpcAnswer {
    Multiple { slot: u64, accounts: Vec<Option<AccountData>> },
    Account { slot: u64, account: Option<AccountData> },
    Program { accounts: Vec<AccountData>, next_cursor: Option<String> },
    Slot(u64),
    GenesisHash(String),
    LatestBlockhash { slot: u64, blockhash: String, last_valid_block_height: u64 },
    Simulation { slot: u64, value: Value },
}

#[derive(Debug)]
pub struct RpcResponse {
    pub provider: String,
    pub received_at_ms: u64,
    pub answer: RpcAnswer,
}

#[derive(Clone)]
pub struct RpcHandle {
    pub(crate) requests: mpsc::Sender<Call>,
    pub(crate) stream: AccountStreamControl,
    pub(crate) events: broadcast::Sender<AccountEvent>,
    pub(crate) account_batch_size: usize,
    pub(crate) capabilities: u64,
}

impl RpcHandle {
    pub async fn call(&self, operation: RpcOperation) -> Result<RpcResponse> {
        let (reply, receive)=oneshot::channel();
        self.requests.send(Call{operation,reply,queued_at:Instant::now()}).await.context("RPC worker stopped")?;
        receive.await.context("RPC worker dropped response")?
    }

    pub fn account_batch_size(&self)->usize {self.account_batch_size}
    pub fn account_stream(&self)->(AccountStreamControl,AccountEventStream) {
        (self.stream.clone(),self.events.subscribe())
    }
    pub fn supports(&self, operation: &RpcOperation) -> bool {
        self.capabilities & operation.bit() != 0
    }
}

pub(crate) struct Call {pub(crate) operation:RpcOperation,pub(crate) reply:oneshot::Sender<Result<RpcResponse>>,pub(crate) queued_at:Instant}

