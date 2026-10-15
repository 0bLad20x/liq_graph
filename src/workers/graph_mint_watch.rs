//! Project the immutable graph's participating mints into a sorted, deduplicated watchlist.
//! No API calls, persistence, signal interpretation, or execution logic.

use std::sync::Arc;

use anyhow::Result;
use solana_pubkey::Pubkey;
use tokio::sync::watch;

use super::liquidity_graph::GraphSnapshot;

/// Every mint referenced by at least one V10-valid group (target, bridge,
/// settlement or conversion). An isolated pool cannot enter this set.
pub async fn run(
    mut graph: watch::Receiver<Arc<GraphSnapshot>>,
    output: watch::Sender<Arc<[Pubkey]>>,
) -> Result<()> {
    let mut current: Arc<[Pubkey]> = Arc::from([]);
    while graph.changed().await.is_ok() {
        let snapshot = graph.borrow_and_update().clone();
        let mut mints: Vec<Pubkey> = snapshot.token_index.keys().copied().collect();
        mints.sort_unstable();
        mints.dedup();
        if mints.as_slice() == current.as_ref() {
            continue;
        }
        let added = mints.iter()
            .filter(|mint| current.binary_search(mint).is_err()).count();
        let removed = current.iter()
            .filter(|mint| mints.binary_search(mint).is_err()).count();
        current = Arc::from(mints);
        output.send_replace(current.clone());
        println!(
            "graph mint watch updated generation={} groups={} mints={} added={} removed={}",
            snapshot.generation, snapshot.groups.len(), current.len(), added, removed
        );
    }
    println!("graph mint watch complete mints={}", current.len());
    Ok(())
}
