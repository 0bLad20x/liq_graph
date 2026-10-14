//! Temporary diagnostic consumer: verify decoded source batches with real inputs.
//! No decoding, routing, persistence or source-specific parsing.

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::broadcast;

use crate::core::{DataBatch, RecordIdentity};

pub async fn run(mut input: broadcast::Receiver<Arc<DataBatch>>) -> Result<()> {
    let mut received = 0_u64;
    let mut received_records = 0_u64;
    let mut lagged = 0_u64;
    let mut source = None;

    loop {
        match input.recv().await {
            Ok(batch) => {
                received += 1;
                received_records += batch.records.len() as u64;
                if source.is_none() {
                    source = Some(batch.source);
                    let sample = batch.records.first();
                    let identity = match sample.map(|record| &record.identity) {
                        Some(RecordIdentity::Pool { address, venue, mint_a, mint_b }) =>
                            format!("pool={address} venue={venue:?} mint_a={mint_a:?} mint_b={mint_b:?}"),
                        Some(RecordIdentity::Mint { address }) => format!("mint={address}"),
                        None => "empty_response".to_owned(),
                    };
                    let payload_fields = sample
                        .and_then(|record| record.payload.as_object())
                        .map_or(0, |object| object.len());
                    println!(
                        "DataBatch source={} received_at_ms={} records={} payload_fields={} {}",
                        batch.source, batch.received_at_ms, batch.records.len(), payload_fields, identity,
                    );
                } else if received % 25 == 0 {
                    println!(
                        "DataBatch source={} received_batches={} received_records={}",
                        batch.source, received, received_records,
                    );
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                lagged += skipped;
                eprintln!("DataBatch consumer lagged missed_batches={skipped}");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }

    println!(
        "DataBatch output complete source={} received_batches={received} received_records={received_records} lagged_batches={lagged}",
        source.unwrap_or("unknown"),
    );
    Ok(())
}
