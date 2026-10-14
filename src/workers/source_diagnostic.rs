//! Temporary diagnostic consumer: observe raw source batches during live tests.
//! No decoding, routing, persistence, or source-specific knowledge.

use anyhow::Result;
use tokio::sync::broadcast;

use crate::core::SourceBatch;

pub async fn run(mut input: broadcast::Receiver<SourceBatch>) -> Result<()> {
    let mut received = 0_u64;
    let mut lagged = 0_u64;
    let mut source = None;
    loop {
        match input.recv().await {
            Ok(batch) => {
                received += 1;
                if source.is_none() {
                    source = Some(batch.source);
                    let preview = &batch.body[..batch.body.len().min(240)];
                    println!(
                        "SourceBatch source={} kind={:?} bytes={} preview={}",
                        batch.source, batch.kind, batch.body.len(),
                        String::from_utf8_lossy(preview)
                    );
                } else if received % 25 == 0 {
                    println!(
                        "SourceBatch source={} received_batches={} bytes={}",
                        batch.source, received, batch.body.len()
                    );
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                lagged += skipped;
                eprintln!("SourceBatch consumer lagged missed_batches={skipped}");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    println!(
        "SourceBatch output complete source={} received_batches={received} lagged_batches={lagged}",
        source.unwrap_or("unknown")
    );
    Ok(())
}
