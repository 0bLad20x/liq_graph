//! Finite real-input Market-State wiring. Uses persisted Registry seeds and live RPC accounts.
use std::{env,sync::Arc,time::Duration};
use anyhow::{Context,Result,bail,ensure};
use tokio::{sync::{broadcast,watch},task::JoinHandle};
use crate::{core::{CompletionState,PoolUpdateBatch,PoolUpdateKind},workers::{
    graph_liquidity,graph_watch_catalog,market_state,persistence,pool_registry,rpc_transport}};

// Do not leave background RPC or graph tasks alive after an early orchestration error.
struct Task<T>(JoinHandle<T>);
impl<T> Drop for Task<T> {fn drop(&mut self){self.0.abort();}}

pub async fn run()->Result<()> {
    let duration:u64=env::var("MARKET_STATE_DURATION_SECONDS").unwrap_or_else(|_|"600".into()).parse()?;
    ensure!((30..=86400).contains(&duration),"market live duration must be 30..86400 seconds");
    let rewatch=match env::var("MARKET_STATE_LIVE_REWATCH").as_deref() {
        Ok("1"|"true")=>true,Ok("0"|"false")|Err(env::VarError::NotPresent)=>false,
        _=>bail!("MARKET_STATE_LIVE_REWATCH must be true or false"),
    };
    let settings=market_state::Settings::from_env()?;
    let database=persistence::Persistence::connect().await?;
    let restored=database.restore().await?;
    let prepared:Vec<_>=restored.iter().filter(|p|p.completion_state==CompletionState::Prepared).cloned().collect();
    ensure!(!prepared.is_empty(),"no persisted Prepared pools; run the existing completion lifecycle first");
    let registry=pool_registry::PoolRegistry::default();
    let count=registry.restore(restored)?;
    drop(database);
    let (rpc,rpc_task)=rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
    let mut rpc_task=Task(rpc_task);
    let (updates,_)=broadcast::channel::<Arc<PoolUpdateBatch>>(256);
    let (graphs,graph_rx)=watch::channel(Arc::new(graph_liquidity::GraphSnapshot::default()));
    let (catalogs,catalog_rx)=watch::channel(Arc::new(graph_watch_catalog::WatchCatalog::empty()));
    let mut graph=Task(tokio::spawn(graph_liquidity::run(updates.subscribe(),graphs,graph_liquidity::Settings::from_env()?)));
    let mut catalog=Task(tokio::spawn(graph_watch_catalog::run(graph_rx,catalogs,None)));
    let (stop,stop_rx)=watch::channel(false);
    let (control,enabled)=watch::channel(true);
    let (prices,price_rx)=broadcast::channel(16384);
    let mut consumer=Task(tokio::spawn(market_state::observe(price_rx,catalog_rx,settings.trace)));
    let mut worker=Task(tokio::spawn(market_state::run(registry,updates.subscribe(),rpc,
        prices,settings,stop_rx,enabled)));
    updates.send(Arc::new(PoolUpdateBatch{kind:PoolUpdateKind::Source,source:"market_live_restore",
        received_at_ms:0,pools:prepared,persist_identity:Vec::new()}))?;
    crate::log_event!(Info,"market state live started=true restored={count} duration_s={duration} pool_limit={} rewatch={rewatch} execution=false registry_seed=persisted",settings.pool_limit);
    let rewatch_task=Task(tokio::spawn(async move {
        if rewatch {
            tokio::time::sleep(Duration::from_secs(duration/2)).await;
            crate::log_event!(Info,"market state live observation_control=false");
            control.send(false).context("market rewatch control closed")?;
            tokio::time::sleep(Duration::from_secs(6)).await;
            crate::log_event!(Info,"market state live observation_control=true");
            control.send(true).context("market rewatch control closed")?;
        }
        // Keep the control sender alive until orchestration shutdown.
        std::future::pending::<()>().await;
        Ok::<(),anyhow::Error>(())
    }));
    let early=tokio::select! {
        _=tokio::time::sleep(Duration::from_secs(duration))=>None,
        result=&mut worker.0=>Some(result.context("market producer panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("market producer ended before live duration"))),
        result=&mut consumer.0=>Some(result.context("market consumer panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("market consumer ended before live duration"))),
        result=&mut graph.0=>Some(result.context("market graph panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("market graph ended before live duration"))),
        result=&mut catalog.0=>Some(result.context("market catalog panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("market catalog ended before live duration"))),
        result=&mut rpc_task.0=>Some(result.context("market RPC panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("market RPC ended before live duration"))),
    };
    stop.send_replace(true);
    drop(rewatch_task);
    if let Some(error)=early {return Err(error);}
    tokio::time::timeout(Duration::from_secs(10),&mut worker.0).await.context("market producer shutdown timeout")???;
    let report=tokio::time::timeout(Duration::from_secs(10),&mut consumer.0).await.context("market consumer shutdown timeout")???;
    drop(updates);
    tokio::time::timeout(Duration::from_secs(30),&mut graph.0).await.context("market graph shutdown timeout")???;
    tokio::time::timeout(Duration::from_secs(10),&mut catalog.0).await.context("market catalog shutdown timeout")???;
    tokio::time::timeout(Duration::from_secs(10),&mut rpc_task.0).await.context("market RPC shutdown timeout")???;
    let result=report.ensure_live_evidence(rewatch);
    let evidence=serde_json::json!({"duration_seconds":duration,"pool_limit":settings.pool_limit,
        "rewatch":rewatch,"live_evidence_passed":result.is_ok(),"report":report,
        "error":result.as_ref().err().map(|e|format!("{e:#}"))});
    println!("{}",serde_json::to_string(&evidence)?);
    if let Ok(path)=env::var("MARKET_STATE_REPORT_PATH") {
        std::fs::write(path,serde_json::to_vec_pretty(&evidence)?).context("write market live evidence")?;
    }
    result?;
    crate::log_event!(Info,"market state live complete=true real_websocket=true graph_consumer=true");
    Ok(())
}
