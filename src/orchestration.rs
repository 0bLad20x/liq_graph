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
    let (output, mut input) = broadcast::channel::<core::SourceRecord>(4096);

    // CP1 live output inspection; later consumers attach to the same stream.
    let consumer = async move {
        let mut received = 0_u64;
        let mut lagged = 0_u64;
        loop {
            match input.recv().await {
                Ok(record) => {
                    received += 1;
                    if received == 1 {
                        println!(
                            "source={} kind={:?} address={} payload_bytes={} payload={}",
                            record.source, record.kind, record.address, record.payload.len(),
                            String::from_utf8_lossy(&record.payload)
                        );
                    } else if received <= 5 {
                        println!(
                            "source={} kind={:?} address={} payload_bytes={}",
                            record.source, record.kind, record.address, record.payload.len()
                        );
                    }
                    if received % 10_000 == 0 {
                        println!("SourceRecord received={received}");
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    lagged += skipped;
                    eprintln!("SourceRecord consumer lagged, missed={skipped}");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
        println!("SourceRecord output complete, received={received} lagged={lagged}");
        Ok::<(), anyhow::Error>(())
    };

    tokio::try_join!(meteora_dlmm::run(http, output, config), consumer)?;
    Ok(())
}
