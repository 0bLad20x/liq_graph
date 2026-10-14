use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;

/// Source-neutral transport classification. Payload semantics belong to consumers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordKind {
    Pool,
    #[allow(dead_code)]
    Mint,
}

/// One unmodified source response, shared without copying its bytes.
#[derive(Clone, Debug)]
pub struct SourceBatch {
    pub source: &'static str,
    pub kind: RecordKind,
    pub body: Bytes,
}

/// Generic HTTP client; no DEX or protocol logic.
pub fn http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?)
}
