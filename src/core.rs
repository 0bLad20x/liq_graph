use std::{sync::Arc, time::Duration};

use anyhow::Result;
use solana_pubkey::Pubkey;

/// Source-neutral output contract. The payload is the complete, unchanged JSON record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordKind {
    Pool,
    #[allow(dead_code)] // Defined for mint-producing sources; CP1 emits pools only.
    Mint,
}

#[derive(Clone, Debug)]
pub struct SourceRecord {
    pub source: &'static str,
    pub kind: RecordKind,
    pub address: Pubkey,
    pub payload: Arc<[u8]>,
}

/// Generic HTTP client; no DEX or protocol logic.
pub fn http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?)
}
