//! CP1: read-only economic mint selection, independent of Prepared graph state.
//! The database remains the source of truth; no new table or persisted flag.
use std::{collections::{HashMap, HashSet}, env, sync::Arc, time::Duration};
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

const MAX_PENDING_MINTS: usize = 20_000;
const SCAN_PAGE_POOLS: i64 = 2_000;
const PENDING_RETRY_AGE: Duration = Duration::from_secs(900);
const SCAN_MINT_IDENTITIES: &str = r#"
SELECT pool_address,mint_a,mint_b
FROM public.liq_graph_pool_cache
WHERE pool_address > $1::bytea
ORDER BY pool_address LIMIT $2
"#;
const KNOWN_MINTS: &str = r#"
SELECT mint_address FROM public.liq_graph_mint_catalog
WHERE mint_address=ANY($1::bytea[])
  AND updated_at IS NOT NULL
  AND liquidity_usd IS NOT NULL
  AND market_cap_usd IS NOT NULL
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
/// Targeted Jupiter requests share the existing lanes. Indexed reconciliation
/// also catches restored / overflowed pool identities without triggering a
/// remote full-catalogue backfill.
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
    // Bounded in-flight catalogue misses, not a permanent "seen" cache.
    // Persisted pool identities are scanned with a PK cursor so even updates
    // missed at capacity (and pools restored after a restart) are revisited.
    let mut candidate_missing=HashMap::<Pubkey,tokio::time::Instant>::new();
    let mut pool_cursor=vec![0_u8;32];
    let mut scan_pools=0_u64;
    let mut dirty=true;
    let mut elapsed=refresh_secs;
    loop {
        tokio::select! {
            event=pool_updates.recv()=>{
                match event {
                    Ok(batch)=>{
                        // TVL-only enrichment cannot introduce new mint identities.
                        // Do not turn price refreshes into Jupiter queue churn.
                        if batch.source=="dexscreener_token_pairs"
                            || batch.source=="tvl_freshness" {continue}
                        // Even an existing pool can gain previously unknown mint identities.
                        for p in &batch.pools {
                            for mint in [p.mint_a,p.mint_b].into_iter().flatten() {
                                if candidate_missing.len()<MAX_PENDING_MINTS {
                                    candidate_missing.entry(mint).or_insert_with(tokio::time::Instant::now);
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
                        // The periodic SQL snapshot remains authoritative.
                        dirty=true;
                    },
                    Err(broadcast::error::RecvError::Closed)=>break,
                }
            },
            _=pulse.tick()=>{
                elapsed=elapsed.saturating_add(10);
                // Absence of an API result must not block reconciliation forever.
                // Expired misses will be rediscovered on subsequent PK scans.
                candidate_missing.retain(|_,enqueued|
                    enqueued.elapsed()<PENDING_RETRY_AGE);
                if candidate_missing.len()<MAX_PENDING_MINTS {
                    let rows=db.query(SCAN_MINT_IDENTITIES,
                        &[&pool_cursor,&SCAN_PAGE_POOLS]).await
                        .context("scan persisted pool identities for missing Jupiter mints")?;
                    if rows.is_empty() {
                        pool_cursor=vec![0_u8;32];
                        crate::log_event!(Info,
                            "mint qualification reconciliation_wrap scanned_pools={scan_pools}");
                        scan_pools=0;
                    } else {
                        let mut found=HashSet::<Pubkey>::new();
                        for row in &rows {
                            pool_cursor=row.get::<_,Vec<u8>>(0);
                            for column in [1_usize,2_usize] {
                                if let Some(bytes)=row.get::<_,Option<Vec<u8>>>(column) {
                                    let mint=key(bytes)?;
                                    if !candidate_missing.contains_key(&mint) {
                                        found.insert(mint);
                                    }
                                }
                            }
                        }
                        scan_pools+=rows.len() as u64;
                        if !found.is_empty() {
                            let keys=found.iter().map(|m|m.to_bytes().to_vec()).collect::<Vec<_>>();
                            let complete=db.query(KNOWN_MINTS,&[&keys]).await
                                .context("check reconciled Jupiter catalog records")?;
                            for row in complete {
                                found.remove(&key(row.get::<_,Vec<u8>>(0))?);
                            }
                            for mint in found {
                                if candidate_missing.len()>=MAX_PENDING_MINTS {break}
                                candidate_missing.entry(mint)
                                    .or_insert_with(tokio::time::Instant::now);
                            }
                        }
                    }
                }
                if elapsed>=refresh_secs || (dirty && elapsed>=30) {
                    let rows=db.query(SELECT_QUALIFIED,&[]).await.context("qualify Jupiter catalog")?;
                    let mut v=Vec::with_capacity(rows.len());
                    for row in rows {v.push(key(row.get::<_,Vec<u8>>(0))?);}
                    let new: Arc<[Pubkey]>=Arc::from(v);
                    if new.as_ref()!=qualified.as_ref() {
                        qualified=new;
                        qualified_tx.send_replace(qualified.clone());
                    }
                    elapsed=0;
                    dirty=false;
                    crate::log_event!(Info,"mint qualification published qualified={} pending_candidates={}",
                        qualified.len(),candidate_missing.len());
                }
                // Check only NEW registry mint addresses against the existing catalog.
                // No full-table Jupiter scan is required for these requests.
                if !candidate_missing.is_empty() {
                    let keys:Vec<Vec<u8>>=candidate_missing.keys().map(|m|m.to_bytes().to_vec()).collect();
                    let known=db.query(KNOWN_MINTS,&[&keys]).await
                        .context("check targeted Jupiter catalog misses")?;
                    for row in known {
                        candidate_missing.remove(&key(row.get::<_,Vec<u8>>(0))?);
                    }
                }
                let mut targets=Vec::with_capacity(candidate_missing.len()+qualified.len());
                let mut missing:Vec<_>=candidate_missing.keys().copied().collect();
                missing.sort_unstable();
                targets.extend(missing);
                targets.extend(qualified.iter().copied().filter(|mint|!candidate_missing.contains_key(mint)));
                let next:Arc<[Pubkey]>=Arc::from(targets);
                if next.as_ref()!=jupiter_targets.borrow().as_ref() {
                    jupiter_targets.send_replace(next);
                }
            }
        }
    }
    Ok(())
}
