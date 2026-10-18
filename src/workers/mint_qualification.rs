//! CP1: read-only economic mint selection, independent of Prepared graph state.
//! The database remains the source of truth; no new table or persisted flag.
use std::{collections::HashSet, env, sync::Arc, time::Duration};
use anyhow::{Context, Result};
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, watch}, time::{interval, MissedTickBehavior}};
use tokio_postgres::NoTls;
use crate::core::{DataBatch, PoolUpdateBatch, RecordIdentity};

const SELECT_QUALIFIED: &str = r#"
WITH edges AS (
    SELECT mint_a AS mint FROM public.liq_graph_pool_cache WHERE mint_a IS NOT NULL
    UNION ALL
    SELECT mint_b AS mint FROM public.liq_graph_pool_cache WHERE mint_b IS NOT NULL
), topology AS (
    SELECT mint, COUNT(*) AS pools FROM edges GROUP BY mint
)
SELECT m.mint_address
FROM public.liq_graph_mint_catalog m
JOIN topology t ON t.mint=m.mint_address
WHERE m.updated_at BETWEEN NOW()-INTERVAL '90 days' AND NOW()
  AND (t.pools<>1 OR m.created_at IS NULL
       OR m.created_at < TIMESTAMPTZ '2020-01-01'
       OR m.created_at > m.updated_at
       OR m.created_at >= NOW()-INTERVAL '30 days')
  AND (m.holder_count>=10 OR m.holder_count IS NULL)
  AND m.liquidity_usd>=2000
  AND m.market_cap_usd>=10000
ORDER BY m.mint_address
"#;

fn key(bytes: Vec<u8>) -> Result<Pubkey> {
    let value: [u8;32] = bytes.try_into().map_err(|_|anyhow::anyhow!("invalid mint address length"))?;
    Ok(Pubkey::new_from_array(value))
}

fn env_seconds(name: &str, default: u64) -> Result<u64> {
    let n=env::var(name).unwrap_or_else(|_|default.to_string()).parse::<u64>()
        .with_context(||format!("invalid {name}"))?;
    anyhow::ensure!(n>=10 && n<=86400,"{name} must be 10..86400");
    Ok(n)
}

/// Rebuild the economic set at boot, on time boundaries and after catalog changes.
/// Targeted Jupiter requests for newly discovered missing mints share the EXISTING
/// Jupiter API lanes; the periodic full backfill remains a separate fallback.
pub async fn run(
    mut pool_updates: broadcast::Receiver<Arc<PoolUpdateBatch>>,
    mut jupiter_updates: broadcast::Receiver<Arc<DataBatch>>,
    qualified_tx: watch::Sender<Arc<[Pubkey]>>,
    jupiter_targets: watch::Sender<Arc<[Pubkey]>>,
) -> Result<()> {
    let url=env::var("LIQ_GRAPH_DATABASE_URL").or_else(|_|env::var("DATABASE_URL"))
        .context("CP1 qualification requires LIQ_GRAPH_DATABASE_URL or DATABASE_URL")?;
    let (db,connection)=tokio_postgres::connect(&url,NoTls).await
        .context("qualification Postgres connect")?;
    tokio::spawn(async move {
        if let Err(e)=connection.await {crate::log_event!(Error,"qualification database connection error={e:#}");}
    });
    let refresh_secs=env_seconds("MINT_QUALIFICATION_REFRESH_SECONDS",60)?;
    let mut pulse=interval(Duration::from_secs(10));
    pulse.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut qualified=Arc::<[Pubkey]>::from([]);
    let mut candidate_missing=HashSet::<Pubkey>::new();
    let mut checked_mints=HashSet::<Pubkey>::new();
    let mut dirty=true;
    let mut elapsed=refresh_secs;
    loop {
        tokio::select! {
            event=pool_updates.recv()=>{
                match event {
                    Ok(batch)=>{
                        // Even an existing pool can gain previously unknown mint identities.
                        for p in &batch.pools {
                            for mint in [p.mint_a,p.mint_b].into_iter().flatten() {
                                if candidate_missing.len()<20000 && checked_mints.insert(mint) {
                                    candidate_missing.insert(mint);
                                }
                            }
                        }
                        dirty=true;
                    },
                    Err(broadcast::error::RecvError::Lagged(n))=>
                        anyhow::bail!("qualification lost {n} registry batches"),
                    Err(broadcast::error::RecvError::Closed)=>break,
                }
            },
            event=jupiter_updates.recv()=>{
                match event {
                    Ok(batch)=>{
                        if batch.records.iter().any(|r|matches!(&r.identity,RecordIdentity::Mint{..})) {
                            dirty=true;
                        }
                    },
                    Err(broadcast::error::RecvError::Lagged(_))=>{
                        // Full SQL refresh remains authoritative even if diagnostic mint
                        // events coalesce; never lose a pool identity update.
                        dirty=true;
                    },
                    Err(broadcast::error::RecvError::Closed)=>break,
                }
            },
            _=pulse.tick()=>{
                elapsed=elapsed.saturating_add(10);
                if elapsed>=refresh_secs {
                    let rows=db.query(SELECT_QUALIFIED,&[]).await.context("qualify Jupiter catalog")?;
                    let mut v=Vec::with_capacity(rows.len());
                    for row in rows {v.push(key(row.get::<_,Vec<u8>>(0))?);}
                    let new: Arc<[Pubkey]>=Arc::from(v);
                    if new.as_ref()!=qualified.as_ref() {
                        qualified=new;
                        qualified_tx.send_replace(qualified.clone());
                    }
                    dirty=false;
                    elapsed=0;
                    crate::log_event!(Info,"mint qualification published qualified={} pending_candidates={}",
                        qualified.len(),candidate_missing.len());
                }
                // Check only NEW registry mint addresses against the existing catalog.
                // No full-table Jupiter scan is required for these requests.
                if !candidate_missing.is_empty() {
                    let keys:Vec<Vec<u8>>=candidate_missing.iter().map(|m|m.to_bytes().to_vec()).collect();
                    let known=db.query(
                        "SELECT mint_address FROM public.liq_graph_mint_catalog
                         WHERE mint_address=ANY($1::bytea[])
                           AND updated_at IS NOT NULL
                           AND liquidity_usd IS NOT NULL
                           AND market_cap_usd IS NOT NULL",
                        &[&keys]).await.context("check targeted Jupiter catalog misses")?;
                    for row in known {candidate_missing.remove(&key(row.get::<_,Vec<u8>>(0))?);}
                }
                let mut targets=Vec::with_capacity(candidate_missing.len()+qualified.len());
                let mut missing:Vec<_>=candidate_missing.iter().copied().collect();
                missing.sort_unstable();
                targets.extend(missing);
                targets.extend(qualified.iter().copied().filter(|mint|!candidate_missing.contains(mint)));
                let next:Arc<[Pubkey]>=Arc::from(targets);
                if next.as_ref()!=jupiter_targets.borrow().as_ref() {
                    jupiter_targets.send_replace(next);
                }
            }
        }
    }
    Ok(())
}
