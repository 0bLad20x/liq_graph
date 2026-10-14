//! Composition only: resources, typed channel and worker lifecycle.

use anyhow::{Context, Result, bail, ensure};
use tokio::sync::mpsc;

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
    let (output, mut input) = mpsc::channel(2048);

    let consumer = async move {
        let mut received = 0_u64;
        while let Some(identity) = input.recv().await {
            received += 1;
            if received <= 5 {
                println!(
                    "market={} venue={} mint_a={} mint_b={}",
                    identity.address, identity.venue, identity.mint_a, identity.mint_b
                );
            }
            if received % 10_000 == 0 {
                println!("MarketIdentity received={received}");
            }
        }
        println!("MarketIdentity output complete, received={received}");
        Ok::<(), anyhow::Error>(())
    };

    tokio::try_join!(meteora_dlmm::run(http, output, config), consumer)?;
    Ok(())
}
