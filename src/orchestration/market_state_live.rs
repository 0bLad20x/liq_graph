//! Finite Market-State wiring: persisted proofs, existing live TVL admission, then RPC prices.
use std::{env,sync::Arc,time::Duration};
use anyhow::{Context,Result,bail,ensure};
use tokio::{sync::{broadcast,watch},task::JoinHandle};
use crate::{core::{self,CompletionState,DataBatch,PoolUpdateBatch,PoolUpdateKind},workers::{
    graph_liquidity,graph_watch_catalog,graph_signals,market_state,mint_qualification,persistence,pool_registry,
    rpc_transport,source_dexscreener,source_jupiter_mint}};

// Do not leave background RPC or graph tasks alive after an early orchestration error.
struct Task<T>(JoinHandle<T>);
impl<T> Drop for Task<T> {fn drop(&mut self){self.0.abort();}}

async fn optional_join<T>(task:&mut Option<Task<T>>)->std::result::Result<T,tokio::task::JoinError> {
    match task { Some(task)=>(&mut task.0).await, None=>std::future::pending().await }
}

pub async fn run(signals_enabled:bool)->Result<()> {
    let duration:u64=env::var("MARKET_STATE_DURATION_SECONDS").unwrap_or_else(|_|"600".into()).parse()?;
    ensure!((30..=86400).contains(&duration),"market live duration must be 30..86400 seconds");
    let rewatch=match env::var("MARKET_STATE_LIVE_REWATCH").as_deref() {
        Ok("1"|"true")=>true,Ok("0"|"false")|Err(env::VarError::NotPresent)=>false,
        _=>bail!("MARKET_STATE_LIVE_REWATCH must be true or false"),
    };
    let settings=market_state::Settings::from_env()?;
    let signal_settings=if signals_enabled {Some(graph_signals::Settings::from_env()?)}else{None};
    // Restore intentionally supplies no live TVL. Reuse CP1's existing admission
    // producers instead of promoting historical observations or bypassing the gate.
    let jupiter_settings=super::jupiter_settings()?
        .context("market live admission requires existing JUPITER_SEARCH_API_KEYS configuration")?;
    let http=core::http_client()?;
    let database=persistence::Persistence::connect().await?;
    let restored=database.restore().await?;
    let prepared:Vec<_>=restored.iter().filter(|p|p.completion_state==CompletionState::Prepared).cloned().collect();
    ensure!(!prepared.is_empty(),"no persisted Prepared pools; run the existing completion lifecycle first");
    let persisted_prepared=prepared.len();
    let registry=pool_registry::PoolRegistry::default();
    let count=registry.restore(restored)?;
    drop(database);
    let rpc_settings=rpc_transport::settings_from_env()?;
    let websocket_plan=rpc_settings.websocket_plan()?;
    let (rpc,rpc_task)=rpc_transport::spawn(rpc_settings)?;
    let mut rpc_task=Task(rpc_task);
    let (updates,_)=broadcast::channel::<Arc<PoolUpdateBatch>>(256);
    let (source,source_rx)=broadcast::channel::<Arc<DataBatch>>(256);
    let (done,_)=watch::channel(false);
    let (qualified_tx,qualified_rx)=watch::channel(Arc::<[solana_pubkey::Pubkey]>::from([]));
    let (target_tx,target_rx)=watch::channel(Arc::<[solana_pubkey::Pubkey]>::from([]));
    let (jupiter_out,_)=broadcast::channel::<Arc<DataBatch>>(256);
    // Subscribe before bootstrap; start producers only after the graph seed is queued.
    let qualification_rx=updates.subscribe();
    let jupiter_rx=jupiter_out.subscribe();
    let (graphs,graph_rx)=watch::channel(Arc::new(graph_liquidity::GraphSnapshot::default()));
    let (catalogs,catalog_rx)=watch::channel(Arc::new(graph_watch_catalog::WatchCatalog::empty()));
    let mut graph=Task(tokio::spawn(graph_liquidity::run(updates.subscribe(),graphs,graph_liquidity::Settings::from_env()?)));
    let mut catalog=Task(tokio::spawn(graph_watch_catalog::run(graph_rx,catalogs,None)));
    let (stop,stop_rx)=watch::channel(false);
    let (control,enabled)=watch::channel(true);
    let (prices,price_rx)=broadcast::channel(16384);
    let (signal_output,mut signal_worker,mut signal_consumer)=if let Some(cfg)=signal_settings {
        let (output,receiver)=broadcast::channel(16384);
        let observer=Task(tokio::spawn(graph_signals::observe(receiver,cfg)));
        let worker=Task(tokio::spawn(graph_signals::run(prices.subscribe(),catalog_rx.clone(),output.clone(),cfg)));
        (Some(output),Some(worker),Some(observer))
    }else{(None,None,None)};
    let mut consumer=Task(tokio::spawn(market_state::observe(price_rx,catalog_rx,settings.trace)));
    // Keep resource roots alive until the producer result has been joined. Otherwise
    // an errored producer can close the consumer/RPC first and hide its actual error.
    let mut worker=Task(tokio::spawn(market_state::run(registry.clone(),updates.subscribe(),rpc.clone(),
        prices.clone(),settings,stop_rx,enabled)));
    updates.send(Arc::new(PoolUpdateBatch{kind:PoolUpdateKind::Source,source:"market_live_restore",
        received_at_ms:0,pools:prepared,persist_identity:Vec::new()}))?;
    let mut registry_task=Task(tokio::spawn(pool_registry::run(source_rx,None,registry.clone(),updates.clone(),done,true)));
    let mut qualification=Task(tokio::spawn(mint_qualification::run(
        qualification_rx,jupiter_rx,qualified_tx,target_tx)));
    let mut jupiter=Task(tokio::spawn(source_jupiter_mint::run_targeted(
        http,jupiter_settings,target_rx,jupiter_out.clone())));
    let mut dex=Task(tokio::spawn(source_dexscreener::run_live(qualified_rx,registry.clone(),source.clone())));
    crate::log_event!(Info,"market state live started=true restored={count} persisted_prepared={persisted_prepared} duration_s={duration} pool_limit={} rewatch={rewatch} execution=false registry_seed=persisted admission_source=live_enrichment warmup=true",settings.pool_limit);
    crate::log_event!(Info,"market state live signals_enabled={signals_enabled} reference=true");
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
        result=optional_join(&mut signal_worker)=>Some(result.context("signal worker panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("signal worker ended before live duration"))),
        result=optional_join(&mut signal_consumer)=>Some(result.context("signal consumer panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("signal consumer ended before live duration"))),
        result=&mut graph.0=>Some(result.context("market graph panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("market graph ended before live duration"))),
        result=&mut catalog.0=>Some(result.context("market catalog panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("market catalog ended before live duration"))),
        result=&mut rpc_task.0=>Some(result.context("market RPC panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("market RPC ended before live duration"))),
        result=&mut registry_task.0=>Some(result.context("market Registry panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("market Registry ended before live duration"))),
        result=&mut qualification.0=>Some(result.context("market mint qualification panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("market mint qualification ended before live duration"))),
        result=&mut jupiter.0=>Some(result.context("market Jupiter panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("market Jupiter ended before live duration"))),
        result=&mut dex.0=>Some(result.context("market DexScreener panicked").and_then(|r|r).err()
            .unwrap_or_else(||anyhow::anyhow!("market DexScreener ended before live duration"))),
    };
    stop.send_replace(true);
    drop(rewatch_task);
    if let Some(error)=early {return Err(error);}
    // Cancel external producers, then let the sole Registry writer drain their inputs.
    dex.0.abort();jupiter.0.abort();qualification.0.abort();
    let _=tokio::join!(&mut dex.0,&mut jupiter.0,&mut qualification.0);
    drop(source);drop(jupiter_out);
    tokio::time::timeout(Duration::from_secs(10),&mut worker.0).await.context("market producer shutdown timeout")???;
    drop(prices);drop(rpc);
    if let Some(task)=signal_worker.as_mut() {
        tokio::time::timeout(Duration::from_secs(30),&mut task.0).await.context("signal worker shutdown timeout")???;
    }
    drop(signal_output);
    let signal_report=if let Some(task)=signal_consumer.as_mut() {
        Some(tokio::time::timeout(Duration::from_secs(30),&mut task.0).await.context("signal consumer shutdown timeout")???)
    }else{None};
    let report=tokio::time::timeout(Duration::from_secs(10),&mut consumer.0).await.context("market consumer shutdown timeout")???;
    tokio::time::timeout(Duration::from_secs(30),&mut registry_task.0).await.context("market Registry shutdown timeout")???;
    drop(updates);
    tokio::time::timeout(Duration::from_secs(30),&mut graph.0).await.context("market graph shutdown timeout")???;
    tokio::time::timeout(Duration::from_secs(10),&mut catalog.0).await.context("market catalog shutdown timeout")???;
    tokio::time::timeout(Duration::from_secs(10),&mut rpc_task.0).await.context("market RPC shutdown timeout")???;
    let result=report.ensure_live_evidence(rewatch).and_then(|()| {
        if let Some(r)=&signal_report {r.ensure_live_evidence()}else{Ok(())}
    });
    let evidence=serde_json::json!({"duration_seconds":duration,"pool_limit":settings.pool_limit,
        "rewatch":rewatch,"persisted_prepared":persisted_prepared,"live_tvl_pools":registry.live_tvl_count(),
        "admission_source":"live_enrichment","live_evidence_passed":result.is_ok(),"report":report,
        "signals_enabled":signals_enabled,"signal_threshold_bps":signal_settings.map(|s|s.threshold_bps),
        "signal_calculation_passed":signal_report.as_ref().map(|r|r.ensure_calculation_evidence().is_ok()),
        "signal_candidate_passed":signal_report.as_ref().map(|r|r.ensure_live_evidence().is_ok()),
        "signal_report":signal_report,"execution":false,
        "websocket_plan":websocket_plan,
        "error":result.as_ref().err().map(|e|format!("{e:#}"))});
    println!("{}",serde_json::to_string(&evidence)?);
    if let Ok(path)=env::var("MARKET_STATE_REPORT_PATH") {
        std::fs::write(path,serde_json::to_vec_pretty(&evidence)?).context("write market live evidence")?;
    }
    result?;
    crate::log_event!(Info,"market state live complete=true real_websocket=true graph_consumer=true signals_enabled={signals_enabled}");
    Ok(())
}
