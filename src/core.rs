use std::time::Duration;

use anyhow::Result;
use solana_pubkey::Pubkey;

/// Shared public event; providers own their private decoding and state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketIdentity {
    pub address: Pubkey,
    pub venue: &'static str,
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
}

/// Generic HTTP client; no DEX or protocol logic.
pub fn http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?)
}
