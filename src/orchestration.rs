//! Resource provisioning, channel connections, and worker lifecycle only.

use anyhow::{Context, Result, bail, ensure};
use tokio::sync::broadcast;

use crate::{core, workers::meteora_dlmm};

fn settings() -> Result<meteora_dlmm::Settings> {
    let mut args = std::env::args().skip(1);
    let mut once = false;
    let mut max_pages = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--once" => once = true,
            "--pages" => {
                let count: u32 = args.next()
                    .context("--pages requires a positive number")?
                    .parse().context("invalid --pages value")?;
                ensure!(count > 0, "--pages must be positive");
                max_pages = Some(count);
            }
            _ => bail!("unknown argument {arg}; use --once or --pages N"),
        }
    }
    Ok(meteora_dlmm::Settings { once, max_pages })
}

pub async fn run() -> Result<()> {
    let http = core::http_client()?;
    let config = settings()?;
    let (output, mut input) = broadcast::channel::<core::SourceBatch>(32);

    // CP1 transport diagnostics only; consumers own payload interpretation.
    let consumer = async move {
        let mut received_batches = 0_u64;
        let mut received_bytes = 0_u64;
        let mut lagged_batches = 0_u64;
        loop {
            match input.recv().await {
                Ok(batch) => {
                    received_batches += 1;
                    received_bytes += batch.body.len() as u64;
                    if received_batches == 1 {
                        let preview = &batch.body[..batch.body.len().min(240)];
                        println!(
                            "source={} kind={:?} payload_bytes={} payload_preview={}",
                            batch.source, batch.kind, batch.body.len(),
                            String::from_utf8_lossy(preview)
                        );
                    }
                    if received_batches % 25 == 0 {
                        println!(
                            "SourceBatch received_batches={received_batches} received_bytes={received_bytes}"
                        );
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    lagged_batches += skipped;
                    eprintln!("SourceBatch consumer lagged, missed_batches={skipped}");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
        println!(
            "SourceBatch output complete, received_batches={received_batches} received_bytes={received_bytes} lagged_batches={lagged_batches}"
        );
        Ok::<(), anyhow::Error>(())
    };

    tokio::try_join!(meteora_dlmm::run(http, output, config), consumer)?;
    Ok(())
}
