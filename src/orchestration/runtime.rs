//! Production task supervision and wiring. No DEX logic in the orchestrator.
use std::{env, future::Future, io::{self, BufRead}, sync::Arc, time::Duration};
use anyhow::{anyhow, bail, Context, Result};
use tokio::{sync::{broadcast, oneshot, watch}, task::{JoinHandle, JoinSet}, time::Instant};
use crate::{core::{self, CompletionState, DataBatch, CompletionResultBatch, PoolState,
        PoolUpdateBatch, PoolUpdateKind, Venue},
    workers::{completion_lifecycle, completion_meteora_dlmm, completion_meteora_damm_v2, completion_orca_whirlpool,
        completion_raydium_amm_v4, completion_raydium_cpmm, completion_raydium_clmm,
        completion_pancakeswap_clmm, completion_byreal_clmm, completion_manifest,
        completion_pumpswap, discovery_meteora_dlmm, discovery_meteora_damm_v2,
        discovery_orca_whirlpool, discovery_raydium, discovery_pancakeswap_clmm,
        discovery_byreal_clmm, discovery_manifest, discovery_pumpswap, discovery_rpc_full,
        diagnostic_source, graph_liquidity, graph_watch_catalog, mint_qualification, source_dexscreener,
        identity_hydration_venues, market_state, persistence, pool_registry,
        rpc_transport, source_jupiter_mint}};
use super::jupiter_settings;

struct AbortOnDrop<T>(JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {fn drop(&mut self) {self.0.abort();}}

fn number(key:&str,default:u64)->Result<u64>{
    env::var(key).map_or(Ok(default),|value|value.parse().with_context(||format!("invalid {key}")))
}
fn enabled(key:&str)->Result<bool>{
    match env::var(key) {
        Err(env::VarError::NotPresent)=>Ok(false),Err(e)=>Err(e.into()),
        Ok(value)=>match value.as_str(){"true"|"1"=>Ok(true),"false"|"0"=>Ok(false),
            _=>bail!("{key} must be true or false")}
    }
}
fn critical<F>(tasks:&mut JoinSet<Result<()>>,name:&'static str,run:F)
where F:Future<Output=Result<()>>+Send+'static {
    crate::log_event!(Info,"runtime worker={name} policy=fatal started=true");
    tasks.spawn(async move {
        let started=Instant::now();
        // Join the inner task to preserve the worker name even if its future panics.
        // AbortOnDrop prevents an orphaned worker if its supervisor is cancelled.
        let mut worker=AbortOnDrop(tokio::spawn(run));
        let result=match (&mut worker.0).await {
            Ok(Ok(()))=>Ok(()),
            Ok(Err(error))=>Err(error).with_context(||format!("critical worker {name}")),
            Err(error)=>Err(anyhow!("critical worker {name} panicked_or_cancelled={error}")),
        };
        if let Err(ref error)=result {
            crate::log_event!(Error,"runtime fatal worker={name} elapsed_ms={} error={error:#}",
                started.elapsed().as_millis());
        }
        result
    });
}
async fn supervise<F,Fut>(name:&'static str,mut stop:watch::Receiver<bool>,mut factory:F)->Result<()>
where F:FnMut()->Fut+Send+'static,Fut:Future<Output=Result<()>>+Send+'static {
    let mut failures=0_u32;
    crate::log_event!(Warn,"runtime worker={name} policy=restart started=true");
    loop {
        if *stop.borrow(){return Ok(());}
        let started=Instant::now();
        let mut worker=tokio::spawn(factory());
        let finished=tokio::select!{
            result=&mut worker=>Some(result),
            _=stop.changed()=>{worker.abort();let _=worker.await;None}
        };
        let Some(finished)=finished else{return Ok(());};
        if *stop.borrow(){return Ok(());}
        match finished{
            Ok(Ok(()))=>crate::log_event!(Warn,"runtime worker={name} ended unexpectedly"),
            Ok(Err(error))=>crate::log_event!(Warn,"runtime worker={name} error={error:#}"),
            Err(error)=>crate::log_event!(Warn,"runtime worker={name} task_failure={error}"),
        }
        if started.elapsed()>=Duration::from_secs(600){failures=0;}
        failures=failures.saturating_add(1);
        let pause=if failures>=6{300}else{1_u64<<failures.min(5)};
        crate::log_event!(Warn,"runtime worker={name} restart_in_s={pause} failures={failures}");
        tokio::select!{
            _=tokio::time::sleep(Duration::from_secs(pause))=>{},
            _=stop.changed()=>return Ok(()),
        }
    }
}
// Cancel input producers on operator stop; state-owning consumers drain normally.
async fn until_stop<F>(mut stop:watch::Receiver<bool>,run:F)->Result<()>
where F:Future<Output=Result<()>> {
    if *stop.borrow(){return Ok(());}
    tokio::select! {
        result=run=>result,
        _=stop.changed()=>Ok(()),
    }
}
async fn once<F,Fut>(name:&'static str,mut stop:watch::Receiver<bool>,mut factory:F)->Result<()>
where F:FnMut()->Fut+Send+'static,Fut:Future<Output=Result<()>>+Send+'static {
    crate::log_event!(Info,"runtime worker={name} policy=once started=true");
    for attempt in 1..=3 {
        let mut task=tokio::spawn(factory());
        let result=tokio::select!{
            result=&mut task=>result,
            _=stop.changed()=>{task.abort();let _=task.await;return Ok(());}
        };
        match result{
            Ok(Ok(()))=>{
                crate::log_event!(Info,"runtime worker={name} complete=true attempts={attempt}");
                let _=stop.changed().await;return Ok(());
            }
            other=>crate::log_event!(Warn,"runtime worker={name} attempt={attempt} error={other:?}"),
        }
        if attempt<3 {
            tokio::select!{
                _=tokio::time::sleep(Duration::from_secs(10*attempt))=>{},
                _=stop.changed()=>return Ok(()),
            }
        }
    }
    crate::log_event!(Warn,"runtime worker={name} degraded=true");
    let _=stop.changed().await;
    Ok(())
}
/// Wiring shared by production, the finite protocol mode and the CP2 live run.
/// Protocol code and scheduling stay in their respective workers.
#[derive(Clone, Copy)]
pub(super) struct CompletionExecutors {
    pub(super) dlmm:Option<completion_meteora_dlmm::Settings>,
    pub(super) damm_v2:Option<completion_meteora_damm_v2::Settings>,
    pub(super) orca:Option<completion_orca_whirlpool::Settings>,
    pub(super) pancakeswap_clmm:Option<completion_pancakeswap_clmm::Settings>,
    pub(super) byreal_clmm:Option<completion_byreal_clmm::Settings>,
    pub(super) pumpswap:Option<completion_pumpswap::Settings>,
    pub(super) manifest:Option<completion_manifest::Settings>,
    pub(super) raydium_amm_v4:Option<completion_raydium_amm_v4::Settings>,
    pub(super) raydium_cpmm:Option<completion_raydium_cpmm::Settings>,
    pub(super) raydium_clmm:Option<completion_raydium_clmm::Settings>,
}

pub(super) fn spawn_completion_lifecycle(tasks:&mut JoinSet<Result<()>>,
    registry:&pool_registry::PoolRegistry,
    updates:&broadcast::Sender<Arc<PoolUpdateBatch>>,
    results:broadcast::Sender<Arc<CompletionResultBatch>>,
    source_done:watch::Receiver<bool>,
    stop:(watch::Sender<bool>,watch::Receiver<bool>),rpc:rpc_transport::RpcHandle,
    configured:CompletionExecutors,
    finite:bool,attempt_limit:usize,
)->Result<()> {
    anyhow::ensure!(configured.dlmm.is_some()||configured.damm_v2.is_some()||configured.orca.is_some()
        ||configured.raydium_amm_v4.is_some()||configured.raydium_cpmm.is_some()
        ||configured.raydium_clmm.is_some()||configured.pancakeswap_clmm.is_some()
        ||configured.byreal_clmm.is_some()||configured.pumpswap.is_some()||configured.manifest.is_some(),
        "no lifecycle executor configured");
    let input=updates.subscribe();
    let seed=registry.snapshot();
    if let Some(pump)=configured.pumpswap {
        completion_pumpswap::ensure_wallet_cache(&seed,pump.wallet)?;
    }
    let settings=completion_lifecycle::Settings::from_env(finite,attempt_limit)?;
    let (responses,response_rx)=tokio::sync::mpsc::channel(4);
    let mut executors=Vec::new();
    macro_rules! executor {
        ($module:ident,$venue:path,$configured:expr)=>{
            executor!($module,$venue,$configured,|_|false);
        };
        ($module:ident,$venue:path,$configured:expr,$configuration_ready:expr)=>{
            if let Some((config,batch_size))=$configured {
                let (jobs,job_rx)=tokio::sync::mpsc::channel(4);
                executors.push(completion_lifecycle::Executor{venue:$venue,requests:jobs,
                    batch_size,max_in_flight:config.parallel_batches*batch_size,
                    configuration_ready:($configuration_ready)(config)});
                critical(tasks,concat!("completion.",stringify!($module)),
                    $module::run(job_rx,rpc.clone(),config,responses.clone()));
            }
        };
    }
    executor!(completion_meteora_dlmm,Venue::MeteoraDlmm,configured.dlmm.map(|s|(s,16)));
    executor!(completion_meteora_damm_v2,Venue::MeteoraDammV2,configured.damm_v2.map(|s|(s,s.pools_per_batch())));
    executor!(completion_orca_whirlpool,Venue::OrcaWhirlpool,configured.orca.map(|s|(s,s.pools_per_batch())));
    executor!(completion_pancakeswap_clmm,Venue::PancakeSwapClmm,configured.pancakeswap_clmm.map(|s|(s,s.pools_per_batch())));
    executor!(completion_byreal_clmm,Venue::ByrealClmm,configured.byreal_clmm.map(|s|(s,s.pools_per_batch())));
    executor!(completion_manifest,Venue::Manifest,configured.manifest.map(|s|(s,s.pools_per_batch())));
    executor!(completion_pumpswap,Venue::PumpSwap,configured.pumpswap.map(|s|(s,s.pools_per_batch())),
        |settings:completion_pumpswap::Settings|settings.wallet.is_some());
    executor!(completion_raydium_amm_v4,Venue::RaydiumAmmV4,configured.raydium_amm_v4.map(|s|(s,s.pools_per_batch())));
    executor!(completion_raydium_cpmm,Venue::RaydiumCpmm,configured.raydium_cpmm.map(|s|(s,s.pools_per_batch())));
    executor!(completion_raydium_clmm,Venue::RaydiumClmm,configured.raydium_clmm.map(|s|(s,s.pools_per_batch())));
    drop(responses);
    // One lifecycle instance receives every registered executor's responses.
    critical(tasks,"completion_lifecycle",async move{
        let _stop_owner=stop.0;
        completion_lifecycle::run(seed,input,response_rx,executors,results,
            source_done,stop.1,settings).await
    });
    Ok(())
}

/// Bounded real-provider proof for selected migrated executors. Uses the same
/// dispatch, execution, Registry and single-writer path as --run.
pub async fn completion_lifecycle_live()->Result<()> {
    let duration=number("RUN_DURATION_SECONDS",300)?;
    anyhow::ensure!((30..=86400).contains(&duration),"live duration must be 30..86400");
    let selected=env::var("COMPLETION_LIFECYCLE_LIVE_VENUE").unwrap_or_else(|_|"dlmm".into());
    anyhow::ensure!(matches!(selected.as_str(),"dlmm"|"damm_v2"|"both"|"raydium_amm_v4"
        |"raydium_cpmm"|"raydium_clmm"|"raydium"|"orca"|"pancakeswap"|"byreal"|"pumpswap"|"manifest"|"all"),
        "COMPLETION_LIFECYCLE_LIVE_VENUE must be dlmm, damm_v2, both, raydium_amm_v4, raydium_cpmm, raydium_clmm, raydium, orca, pancakeswap, byreal, pumpswap, manifest or all");
    let configured=CompletionExecutors {
        dlmm:matches!(selected.as_str(),"dlmm"|"both"|"all")
            .then(completion_meteora_dlmm::Settings::from_env).transpose()?,
        damm_v2:matches!(selected.as_str(),"damm_v2"|"both"|"all")
            .then(completion_meteora_damm_v2::Settings::from_env).transpose()?,
        orca:matches!(selected.as_str(),"orca"|"all")
            .then(completion_orca_whirlpool::Settings::from_env).transpose()?,
        pancakeswap_clmm:matches!(selected.as_str(),"pancakeswap"|"all")
            .then(completion_pancakeswap_clmm::Settings::from_env).transpose()?,
        byreal_clmm:matches!(selected.as_str(),"byreal"|"all")
            .then(completion_byreal_clmm::Settings::from_env).transpose()?,
        pumpswap:matches!(selected.as_str(),"pumpswap"|"all")
            .then(completion_pumpswap::Settings::from_env).transpose()?,
        manifest:matches!(selected.as_str(),"manifest"|"all")
            .then(completion_manifest::Settings::from_env).transpose()?,
        raydium_amm_v4:matches!(selected.as_str(),"raydium_amm_v4"|"raydium"|"all")
            .then(completion_raydium_amm_v4::Settings::from_env).transpose()?,
        raydium_cpmm:matches!(selected.as_str(),"raydium_cpmm"|"raydium"|"all")
            .then(completion_raydium_cpmm::Settings::from_env).transpose()?,
        raydium_clmm:matches!(selected.as_str(),"raydium_clmm"|"raydium"|"all")
            .then(completion_raydium_clmm::Settings::from_env).transpose()?,
    };
    if let Some(pump)=configured.pumpswap {
        anyhow::ensure!(pump.wallet.is_some(),
            "PumpSwap Prepared-refresh live proof requires PUMPSWAP_V10_WALLET");
    }
    let enrichment=if configured.pumpswap.is_some()||configured.manifest.is_some() {
        Some(jupiter_settings()?.context("PumpSwap/Manifest live TVL enrichment requires JUPITER_SEARCH_API_KEYS")?)
    }else{None};
    let mut venues=Vec::new();
    if configured.dlmm.is_some(){venues.push(Venue::MeteoraDlmm);}
    if configured.damm_v2.is_some(){venues.push(Venue::MeteoraDammV2);}
    if configured.orca.is_some(){venues.push(Venue::OrcaWhirlpool);}
    if configured.pancakeswap_clmm.is_some(){venues.push(Venue::PancakeSwapClmm);}
    if configured.byreal_clmm.is_some(){venues.push(Venue::ByrealClmm);}
    if configured.pumpswap.is_some(){venues.push(Venue::PumpSwap);}
    if configured.manifest.is_some(){venues.push(Venue::Manifest);}
    if configured.raydium_amm_v4.is_some(){venues.push(Venue::RaydiumAmmV4);}
    if configured.raydium_cpmm.is_some(){venues.push(Venue::RaydiumCpmm);}
    if configured.raydium_clmm.is_some(){venues.push(Venue::RaydiumClmm);}
    let database=persistence::Persistence::connect().await?;
    let restored=database.restore().await?.into_iter()
        .filter(|p|venues.contains(&p.venue)).collect();
    let registry=pool_registry::PoolRegistry::default();
    let restored_count=registry.restore(restored)?;
    let (rpc,rpc_task)=rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
    let (sources,source_rx)=broadcast::channel(256);
    let (updates,_)=broadcast::channel(256);
    let (results,result_rx)=broadcast::channel(256);
    let (done,done_rx)=watch::channel(false);
    let (stop,stop_rx)=watch::channel(false);
    let mut tasks=JoinSet::new();
    critical(&mut tasks,"pool_registry",pool_registry::run(source_rx,Some(result_rx),
        registry.clone(),updates.clone(),done,true));
    critical(&mut tasks,"persistence",database.run(updates.subscribe()));
    if selected=="all" {
        let (graph_out,graph_rx)=watch::channel(Arc::new(graph_liquidity::GraphSnapshot::default()));
        critical(&mut tasks,"graph_liquidity",graph_liquidity::run(
            updates.subscribe(),graph_out,graph_liquidity::Settings::from_env()?));
        critical(&mut tasks,"observer.graph_liquidity",graph_liquidity::observe(graph_rx));
    }
    spawn_completion_lifecycle(&mut tasks,&registry,&updates,results,done_rx,
        (stop.clone(),stop_rx.clone()),rpc.clone(),configured,false,0)?;
    let client=core::http_client()?;
    if let Some(enrichment)=enrichment {
        // Reuse the existing CP1 mint rules and TVL provider. Historical DB
        // observations and RPC vault balances never substitute canonical TVL.
        let (qualified_tx,qualified_rx)=watch::channel(Arc::<[solana_pubkey::Pubkey]>::from([]));
        let (target_tx,target_rx)=watch::channel(Arc::<[solana_pubkey::Pubkey]>::from([]));
        let (jupiter_out,jupiter_rx)=broadcast::channel(256);
        critical(&mut tasks,"mint_qualification",until_stop(stop_rx.clone(),
            mint_qualification::run(updates.subscribe(),jupiter_rx,qualified_tx,target_tx)));
        critical(&mut tasks,"source_jupiter_mint",until_stop(stop_rx.clone(),
            source_jupiter_mint::run_targeted(client.clone(),enrichment,target_rx,jupiter_out)));
        critical(&mut tasks,"source_dexscreener.cp1",until_stop(stop_rx.clone(),
            source_dexscreener::run_live(qualified_rx,registry.clone(),sources.clone())));
        crate::log_event!(Info,"lifecycle live enrichment=cp1");
    }
    if configured.pumpswap.is_some() {
        critical(&mut tasks,"discovery.pumpswap_rolling",until_stop(stop_rx.clone(),
            discovery_pumpswap::run(rpc.clone(),sources.clone(),
                discovery_pumpswap::Settings::from_env(false,None)?)));
        crate::log_event!(Info,"lifecycle live enrichment=cp1 rolling_discovery=true wallet_bound=true");
    }
    if configured.manifest.is_some() {
        let client=client.clone();let publisher=sources.clone();
        tasks.spawn(supervise("discovery.manifest",stop_rx.clone(),move||{
            let client=client.clone();let output=publisher.clone();
            async move{discovery_manifest::run(client,output,
                discovery_manifest::Settings{once:false}).await}
        }));
    }
    if configured.dlmm.is_some() {
        let client=client.clone();let publisher=sources.clone();
        tasks.spawn(supervise("discovery.meteora_dlmm",stop_rx.clone(),move||{
            let client=client.clone();let output=publisher.clone();
            async move{discovery_meteora_dlmm::run(client,output,
                discovery_meteora_dlmm::Settings{once:false,max_pages:None}).await}
        }));
    }
    if configured.damm_v2.is_some() {
        let client=client.clone();
        let publisher=sources.clone();
        tasks.spawn(supervise("discovery.meteora_damm_v2",stop_rx.clone(),move||{
            let client=client.clone();let output=publisher.clone();
            async move{discovery_meteora_damm_v2::run(client,output,
                discovery_meteora_damm_v2::Settings{once:false,max_pages:None}).await}
        }));
    }
    if configured.orca.is_some() {
        let client=client.clone();let publisher=sources.clone();
        let rps=u32::try_from(number("ORCA_API_RPS",10)?)?;
        tasks.spawn(supervise("discovery.orca_whirlpool",stop_rx.clone(),move||{
            let client=client.clone();let output=publisher.clone();
            async move{discovery_orca_whirlpool::run(client,output,
                discovery_orca_whirlpool::Settings{once:false,max_pages:None,rps}).await}
        }));
    }
    if configured.pancakeswap_clmm.is_some() {
        let client=client.clone();let publisher=sources.clone();
        let rpm=u32::try_from(number("PANCAKESWAP_API_RPM",57)?)?;
        tasks.spawn(supervise("discovery.pancakeswap_clmm",stop_rx.clone(),move||{
            let client=client.clone();let output=publisher.clone();
            async move{discovery_pancakeswap_clmm::run(client,output,
                discovery_pancakeswap_clmm::Settings{once:false,max_pages:None,rpm}).await}
        }));
    }
    if configured.byreal_clmm.is_some() {
        let client=client.clone();let publisher=sources.clone();
        let rpm=u32::try_from(number("BYREAL_API_RPM",57)?)?;
        tasks.spawn(supervise("discovery.byreal_clmm",stop_rx.clone(),move||{
            let client=client.clone();let output=publisher.clone();
            async move{discovery_byreal_clmm::run(client,output,
                discovery_byreal_clmm::Settings{once:false,max_pages:None,rpm}).await}
        }));
    }
    if configured.raydium_amm_v4.is_some()||configured.raydium_cpmm.is_some()
        ||configured.raydium_clmm.is_some() {
        let publisher=sources.clone();
        let rps=u32::try_from(number("RAYDIUM_API_RPS",10)?)?;
        tasks.spawn(supervise("discovery.raydium",stop_rx,move||{
            let client=client.clone();let output=publisher.clone();
            async move{discovery_raydium::run(client,output,
                discovery_raydium::Settings{once:false,max_pages:None,rps}).await}
        }));
    }
    drop(sources);
    critical(&mut tasks,"rpc_transport",async move{rpc_task.await?});
    drop(updates);drop(rpc);
    crate::log_event!(Info,"lifecycle live started restored={restored_count} duration_s={duration} executors={} venues={venues:?}",venues.len());
    tokio::select! {
        _=tokio::time::sleep(Duration::from_secs(duration))=>{},
        result=tasks.join_next()=>{
            let result=result.context("all lifecycle live tasks ended")?;
            result.context("lifecycle live task panicked")??;
            bail!("lifecycle live worker ended before duration");
        },
    }
    stop.send_replace(true);
    // Drain active real requests, Registry acceptance and database writes.
    // The outer bound permits the configured executor request timeout.
    let drain_ms=number("COMPLETION_LIFECYCLE_ATTEMPT_SECONDS",180)?*1000+150000;
    tokio::time::timeout(Duration::from_millis(drain_ms),async {
        while let Some(result)=tasks.join_next().await{result??;}
        Ok::<(),anyhow::Error>(())
    }).await.context("lifecycle live drain timed out")??;
    crate::log_event!(Info,"lifecycle live complete=true persisted=true");
    Ok(())
}

/// Isolated CP1 live checkpoint: real Jupiter and DexScreener, restored
/// Registry and the single PostgreSQL writer; NO discovery or Completion RPC.
pub async fn cp1_live() -> Result<()> {
    let database=persistence::Persistence::connect().await?;
    let restored=database.restore().await?;
    let registry=pool_registry::PoolRegistry::default();
    let pool_count=registry.restore(restored)?;
    let (source,source_rx)=broadcast::channel::<Arc<DataBatch>>(256);
    let (updates,_) = broadcast::channel::<Arc<PoolUpdateBatch>>(256);
    let (done,_) = watch::channel(false);
    let registry_task=tokio::spawn(pool_registry::run(
        source_rx,None,registry.clone(),updates.clone(),done,true));
    let writer=tokio::spawn(database.run(updates.subscribe()));
    let (qualified_tx,qualified_rx)=watch::channel(Arc::<[solana_pubkey::Pubkey]>::from([]));
    let (target_tx,target_rx)=watch::channel(Arc::<[solana_pubkey::Pubkey]>::from([]));
    let (jupiter_out,_) =broadcast::channel::<Arc<DataBatch>>(256);
    let qualification=tokio::spawn(mint_qualification::run(
        updates.subscribe(),jupiter_out.subscribe(),qualified_tx,target_tx));
    let settings=jupiter_settings()?.context("CP1 requires JUPITER_SEARCH_API_KEYS")?;
    // This isolated checkpoint must not start an unbounded full backfill.
    let mut jupiter_settings=settings;
    jupiter_settings.backfill_on_start=false;
    let jupiter=tokio::spawn(source_jupiter_mint::run_targeted(
        core::http_client()?,jupiter_settings,target_rx,jupiter_out.clone()));
    let qualification_evidence=qualified_rx.clone();
    let dex=tokio::spawn(source_dexscreener::run_live(
        qualified_rx,registry.clone(),source.clone()));
    let duration=number("CP1_PROBE_DURATION_SECONDS",300)?;
    ensure_duration(duration)?;
    crate::log_event!(Info,"cp1 live started=true restored_pools={pool_count} duration_s={duration} completion_workers=0");
    let mut dex=dex;
    let mut jupiter=jupiter;
    let mut qualification=qualification;
    tokio::select! {
        _=tokio::time::sleep(Duration::from_secs(duration))=>{},
        result=&mut dex=>{result.context("CP1 DexScreener task panicked")??;bail!("CP1 DexScreener exited unexpectedly");},
        result=&mut jupiter=>{result.context("CP1 Jupiter task panicked")??;bail!("CP1 Jupiter exited unexpectedly");},
        result=&mut qualification=>{result.context("CP1 qualification task panicked")??;bail!("CP1 qualification exited unexpectedly");},
    }
    dex.abort();jupiter.abort();qualification.abort();
    let _=tokio::join!(dex,jupiter,qualification);
    drop(jupiter_out);
    drop(source);
    registry_task.await.context("CP1 Registry task panicked")??;
    drop(updates);
    writer.await.context("CP1 Persistence task panicked")??;
    let qualified_count=qualification_evidence.borrow().len();
    let live_tvl=registry.live_tvl_count();
    anyhow::ensure!(qualified_count>0 && live_tvl>0,
        "CP1 live produced no qualified mints or no enriched pool TVL");
    crate::log_event!(Info,
        "cp1 live complete=true persisted=true qualified_mints={qualified_count} live_tvl_pools={live_tvl}");
    Ok(())
}
fn ensure_duration(n:u64)->Result<()> {
    anyhow::ensure!((10..=86400).contains(&n),"CP1_PROBE_DURATION_SECONDS must be 10..86400");
    Ok(())
}

pub async fn run()->Result<()>{
    let duration=number("RUN_DURATION_SECONDS",0)?;
    let full_scan=enabled("RUN_FULL_SCAN")?;
    let source_capacity=usize::try_from(number("REST_BROADCAST_CAPACITY",256)?)?;
    let registry_capacity=usize::try_from(number("REGISTRY_BROADCAST_CAPACITY",256)?)?;
    anyhow::ensure!((32..=65536).contains(&source_capacity)
        && (32..=65536).contains(&registry_capacity),"broadcast capacity must be 32..=65536");
    let graph_settings=graph_liquidity::Settings::from_env()?;
    let http=core::http_client()?;
    let (rpc,rpc_task)=rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
    let database=persistence::Persistence::connect().await?;
    let restored=database.restore().await?;
    let hydration_only=enabled("IDENTITY_HYDRATION_ONLY")?;
    let venue_hydration_settings=identity_hydration_venues::Settings::from_env()?;
    // Every unresolved pool, independent of venue, is assigned to one hydration worker.
    let venue_hydration_seed:Vec<(solana_pubkey::Pubkey,Venue)>=restored.iter()
        .filter(|p|p.mint_a.is_none() || p.mint_b.is_none())
        .map(|p|(p.address,p.venue)).collect();
    // Finite real-provider checkpoint: only multi-venue Hydration, Registry and
    // Persistence. No REST discovery, Graph, Completion or legacy PumpSwap worker.
    if hydration_only {
        let registry=pool_registry::PoolRegistry::default();
        let restored_count=registry.restore(restored)?;
        let (source,source_rx)=broadcast::channel::<Arc<DataBatch>>(256);
        let (updates,_) = broadcast::channel::<Arc<PoolUpdateBatch>>(256);
        let db_rx=updates.subscribe();
        let hydrate_rx=updates.subscribe();
        let (done,_) = watch::channel(false);
        let registry_task=tokio::spawn(pool_registry::run(source_rx,None,registry,
            updates.clone(),done,true));
        let writer_task=tokio::spawn(database.run(db_rx));
        crate::log_event!(Info,
            "multi venue hydration standalone restored={} requested={}",
            restored_count,venue_hydration_seed.len());
        let result=identity_hydration_venues::run(venue_hydration_seed,hydrate_rx,
            rpc.clone(),source.clone(),venue_hydration_settings,true).await;
        drop(source);
        drop(updates);
        drop(rpc);
        // Preserve upstream errors and wait for the Registry/Persistence drain.
        let registry_result=registry_task.await.context("hydration Registry task panicked")?;
        let writer_result=writer_task.await.context("hydration Persistence task panicked")?;
        let rpc_result=rpc_task.await.context("hydration RPC task panicked")?;
        result?;
        registry_result?;
        writer_result?;
        rpc_result?;
        crate::log_event!(Info,"multi venue hydration standalone complete=true persisted=true");
        return Ok(());
    }
    // Existing V10 PumpSwap Prepared manifests contain signer-specific PDAs.
    // Check the current signer before seeding the graph with persisted blocks.
    completion_pumpswap::ensure_wallet_cache(
        &restored,completion_pumpswap::Settings::from_env()?.wallet)?;
    let graph_boot:Vec<PoolState>=restored.iter()
        .filter(|p|p.completion_state==CompletionState::Prepared).cloned().collect();
    let registry=pool_registry::PoolRegistry::default();
    let count=registry.restore(restored)?;
    crate::log_event!(Info,"runtime bootstrap restored={count} prepared={} lifecycle_executors=10 backpressure_unchanged=true",graph_boot.len());

    let (stop_tx,stop_rx)=watch::channel(false);
    let (source,source_rx)=broadcast::channel::<Arc<DataBatch>>(source_capacity);
    let diagnostic_rx=source.subscribe();
    let (updates,observe_rx)=broadcast::channel::<Arc<PoolUpdateBatch>>(registry_capacity);
    let (completions,completion_rx)=broadcast::channel::<Arc<CompletionResultBatch>>(256);
    let (source_done,done_rx)=watch::channel(false);
    let (graph_tx,graph_rx)=watch::channel(Arc::new(graph_liquidity::GraphSnapshot::default()));
    let (catalog_tx,catalog_rx)=watch::channel(Arc::new(graph_watch_catalog::WatchCatalog::empty()));
    let mut tasks=JoinSet::<Result<()>>::new();
    critical(&mut tasks,"pool_registry",pool_registry::run(source_rx,Some(completion_rx),
        registry.clone(),updates.clone(),source_done,true));
    critical(&mut tasks,"persistence",database.run(updates.subscribe()));
    critical(&mut tasks,"graph_liquidity",graph_liquidity::run(
        updates.subscribe(),graph_tx,graph_settings));
    // Price observations are optional. The Registry and graph retain their own eligibility rules.
    let market_enabled=enabled("MARKET_STATE_ENABLED")?;
    let signals_enabled=enabled("GRAPH_SIGNALS_ENABLED")?;
    anyhow::ensure!(!signals_enabled || market_enabled,"GRAPH_SIGNALS_ENABLED requires MARKET_STATE_ENABLED");
    let market_control=if market_enabled {
        let settings=market_state::Settings::from_env()?;
        let (control,enabled_rx)=watch::channel(true);
        let (prices,price_rx)=broadcast::channel(16384);
        if signals_enabled {
            let cfg=crate::workers::graph_signals::Settings::from_env()?;
            let (signals,signal_rx)=broadcast::channel(16384);
            critical(&mut tasks,"graph_signals",crate::workers::graph_signals::run(
                prices.subscribe(),catalog_rx.clone(),signals,cfg));
            critical(&mut tasks,"graph_signals.consumer",async move {
                crate::workers::graph_signals::observe(signal_rx,cfg).await.map(|_|())
            });
        }
        critical(&mut tasks,"market_state",market_state::run(registry.clone(),updates.subscribe(),
            rpc.clone(),prices,settings,stop_rx.clone(),enabled_rx));
        let market_catalog=catalog_rx.clone();
        critical(&mut tasks,"market_state.consumer",async move {
            market_state::observe(price_rx,market_catalog,settings.trace).await.map(|_|())
        });
        Some(control)
    } else {None};
    // Keep the observation-control sender alive for the worker lifetime.
    let _market_control=market_control;
    // Observers must not take down the state-owning runtime on diagnostic lag.
    tasks.spawn(supervise("observer.graph_watch_catalog",stop_rx.clone(),move||{
        let input=catalog_rx.clone();
        async move{graph_watch_catalog::observe(input).await}
    }));
    let cp1=enabled("CP1_MARKET_ENRICHMENT_ENABLED")?;
    let (qualified_tx,qualified_rx)=watch::channel(Arc::<[solana_pubkey::Pubkey]>::from([]));
    let mut qualification_jupiter_rx=None;
    let mut qualification_jupiter_tx=None;
    let jupiter_mints=if jupiter_settings()?.is_some(){
        let (mint_tx,mint_rx)=watch::channel(Arc::<[solana_pubkey::Pubkey]>::from([]));
        let (jupiter_out,jupiter_rx)=broadcast::channel::<Arc<DataBatch>>(256);
        let observer_sender=jupiter_out.clone();
        let mut first_jupiter_observer=Some(jupiter_rx);
        tasks.spawn(supervise("observer.source_jupiter_mint",stop_rx.clone(),move||{
            let input=first_jupiter_observer.take()
                .unwrap_or_else(||observer_sender.subscribe());
            async move{diagnostic_source::run(input,true).await}
        }));
        if cp1 {
            qualification_jupiter_rx=Some(jupiter_out.subscribe());
            qualification_jupiter_tx=Some(mint_tx.clone());
        }
        let client=http.clone();
        tasks.spawn(supervise("source_jupiter_mint",stop_rx.clone(),move||{
            let output=jupiter_out.clone();let client=client.clone();
            let mints=mint_rx.clone();
            async move{
                let settings=jupiter_settings()?.context("Jupiter keys disappeared")?;
                if cp1 {
                    source_jupiter_mint::run_targeted(client,settings,mints,output).await
                } else {
                    source_jupiter_mint::run(client,settings,mints,output).await
                }
            }
        }));
        if cp1 {None} else {Some(mint_tx)}
    }else{None};
    critical(&mut tasks,"graph_watch_catalog",graph_watch_catalog::run(
        graph_rx,catalog_tx,jupiter_mints));
    if cp1 {
        let jupiter_rx=qualification_jupiter_rx.context("CP1 requires Jupiter API keys")?;
        let targets=qualification_jupiter_tx.context("CP1 Jupiter targets missing")?;
        critical(&mut tasks,"mint_qualification",mint_qualification::run(
            updates.subscribe(),jupiter_rx,qualified_tx,targets));
        let reg=registry.clone();let publisher=source.clone();
        tasks.spawn(supervise("source_dexscreener.cp1",stop_rx.clone(),move||{
            let selection=qualified_rx.clone();
            let reg=reg.clone();let publisher=publisher.clone();
            async move{source_dexscreener::run_live(selection,reg,publisher).await}
        }));
    }
    let diagnostic_sender=source.clone();
    let mut first_discovery_observer=Some(diagnostic_rx);
    tasks.spawn(supervise("observer.discovery_batches",stop_rx.clone(),move||{
        let input=first_discovery_observer.take()
            .unwrap_or_else(||diagnostic_sender.subscribe());
        async move{diagnostic_source::run(input,true).await}
    }));
    let registry_observer_sender=updates.clone();
    let mut first_registry_observer=Some(observe_rx);
    tasks.spawn(supervise("observer.registry_updates",stop_rx.clone(),move||{
        let input=first_registry_observer.take()
            .unwrap_or_else(||registry_observer_sender.subscribe());
        async move{pool_registry::observe_updates(input,true,true).await}
    }));
    critical(&mut tasks,"rpc_transport",async move{rpc_task.await?});

    // Seed graph before completion can change a Prepared state.
    if !graph_boot.is_empty(){
        updates.send(Arc::new(PoolUpdateBatch{kind:PoolUpdateKind::Source,
            source:"persistence_restore",received_at_ms:0,
            pools:graph_boot,persist_identity:Vec::new()}))
            .map_err(|_|anyhow!("no graph bootstrap subscriber"))?;
    }
    spawn_completion_lifecycle(&mut tasks,&registry,&updates,completions.clone(),done_rx.clone(),
        (stop_tx.clone(),stop_rx.clone()),rpc.clone(),
        CompletionExecutors {
            dlmm:Some(completion_meteora_dlmm::Settings::from_env()?),
            damm_v2:Some(completion_meteora_damm_v2::Settings::from_env()?),
            orca:Some(completion_orca_whirlpool::Settings::from_env()?),
            pancakeswap_clmm:Some(completion_pancakeswap_clmm::Settings::from_env()?),
            byreal_clmm:Some(completion_byreal_clmm::Settings::from_env()?),
            pumpswap:Some(completion_pumpswap::Settings::from_env()?),
            manifest:Some(completion_manifest::Settings::from_env()?),
            raydium_amm_v4:Some(completion_raydium_amm_v4::Settings::from_env()?),
            raydium_cpmm:Some(completion_raydium_cpmm::Settings::from_env()?),
            raydium_clmm:Some(completion_raydium_clmm::Settings::from_env()?),
        },false,0)?;
    drop(completions);
    // Single continuous hydration consumer for every venue, including PumpSwap.
    let venue_hydrate_registry=registry.clone();
    let venue_hydrate_updates=updates.clone();
    let venue_hydrate_source=source.clone();
    let venue_hydrate_rpc=rpc.clone();
    let mut first_venue_seed=Some(venue_hydration_seed);
    let mut first_venue_rx=Some(updates.subscribe());
    tasks.spawn(supervise("identity_hydration.venues",stop_rx.clone(),move||{
        let seed=first_venue_seed.take().unwrap_or_else(||{
            venue_hydrate_registry.snapshot().into_iter()
                .filter(|p|p.mint_a.is_none() || p.mint_b.is_none())
                .map(|p|(p.address,p.venue)).collect()
        });
        let live=first_venue_rx.take()
            .unwrap_or_else(||venue_hydrate_updates.subscribe());
        let output=venue_hydrate_source.clone();
        let rpc=venue_hydrate_rpc.clone();
        async move{
            identity_hydration_venues::run(seed,live,rpc,output,
                venue_hydration_settings,false).await
        }
    }));
    crate::log_event!(Info,"runtime ready=true workers_completion=10");

    // Keep REST source ownership and retry entirely outside the protocol workers.
    macro_rules! rest{
        ($module:ident,$label:literal,$config:expr)=>{
            {
                let client=http.clone();let tx=source.clone();
                tasks.spawn(supervise($label,stop_rx.clone(),move||{
                    let settings=$config;let client=client.clone();let tx=tx.clone();
                    async move{$module::run(client,tx,settings).await}
                }));
            }
        };
    }
    let raydium_rps=u32::try_from(number("RAYDIUM_API_RPS",10)?)?;
    let orca_rps=u32::try_from(number("ORCA_API_RPS",10)?)?;
    let pancake_rpm=u32::try_from(number("PANCAKESWAP_API_RPM",57)?)?;
    let byreal_rpm=u32::try_from(number("BYREAL_API_RPM",57)?)?;
    rest!(discovery_meteora_dlmm,"discovery.meteora_dlmm",
        discovery_meteora_dlmm::Settings{once:false,max_pages:None});
    rest!(discovery_meteora_damm_v2,"discovery.meteora_damm_v2",
        discovery_meteora_damm_v2::Settings{once:false,max_pages:None});
    rest!(discovery_raydium,"discovery.raydium",
        discovery_raydium::Settings{once:false,max_pages:None,rps:raydium_rps});
    rest!(discovery_orca_whirlpool,"discovery.orca_whirlpool",
        discovery_orca_whirlpool::Settings{once:false,max_pages:None,rps:orca_rps});
    rest!(discovery_pancakeswap_clmm,"discovery.pancakeswap_clmm",
        discovery_pancakeswap_clmm::Settings{once:false,max_pages:None,rpm:pancake_rpm});
    rest!(discovery_byreal_clmm,"discovery.byreal_clmm",
        discovery_byreal_clmm::Settings{once:false,max_pages:None,rpm:byreal_rpm});
    rest!(discovery_manifest,"discovery.manifest",discovery_manifest::Settings{once:false});
    let rolling=discovery_pumpswap::Settings::from_env(false,None)?;
    use rpc_transport::{RpcOperation,AccountView};
    // Check generic transport capabilities only. The PumpSwap worker itself
    // owns the program ID, discriminator, slice and pagination contract.
    let supported=rpc.supports(&RpcOperation::ProgramV2{
        program:solana_pubkey::Pubkey::default(),filters:Vec::new(),
        view:AccountView::Full,cursor:None,changed_since_slot:Some(1),limit:1
    }) && rpc.supports(&RpcOperation::Slot{commitment:"finalized".into()});
    if supported{
        let client=rpc.clone();let tx=source.clone();
        tasks.spawn(supervise("discovery.pumpswap_rolling",stop_rx.clone(),move||{
            let client=client.clone();let tx=tx.clone();
            let settings=rolling.clone();
            async move{discovery_pumpswap::run(client,tx,settings).await}
        }));
    }else{
        crate::log_event!(Info,"runtime worker=discovery.pumpswap_rolling enabled=false missing_rpc_v2_or_slot=true");
    }
    if full_scan{
        let settings=discovery_rpc_full::Settings::from_env()?;
        let tx=source.clone();
        tasks.spawn(once("discovery.rpc_full",stop_rx.clone(),move||{
            let tx=tx.clone();let settings=settings.clone();
            async move{discovery_rpc_full::run(tx,settings).await}
        }));
    }
    drop(source);drop(updates);drop(rpc);

    // Operator termination uses q + Enter; finite RUN_DURATION_SECONDS supports
    // an automatic graceful live run without adding dependencies or lockfile changes.
    let (operator_tx,operator_rx)=oneshot::channel::<()>();
    std::thread::spawn(move||{
        let mut stdin=io::stdin().lock();let mut line=String::new();
        loop{
            line.clear();
            match stdin.read_line(&mut line){
                Ok(0)|Err(_)=>break,
                Ok(_) if line.trim().eq_ignore_ascii_case("q")
                    ||line.trim().eq_ignore_ascii_case("quit")=>{
                        let _=operator_tx.send(());break;
                    }
                Ok(_)=>{}
            }
        }
    });
    crate::log_event!(Info,"runtime running=true stop=q+Enter duration_seconds={duration}");
    let mut error=None;
    let reason=tokio::select!{
        _=async{
            if duration==0{std::future::pending::<()>().await}
            else{tokio::time::sleep(Duration::from_secs(duration)).await}
        }=>"duration",
        _=async {
            if operator_rx.await.is_err() {std::future::pending::<()>().await}
        }=>"operator",
        joined=tasks.join_next()=>{
            let cause=match joined{
                Some(Ok(Err(e)))=>e,
                Some(Ok(Ok(())))=>anyhow!("managed task ended unexpectedly"),
                Some(Err(e))=>anyhow!("managed supervisor task panicked: {e}"),
                None=>anyhow!("all runtime tasks exited"),
            };
            // Log before shutdown: drain errors previously hid the original failure.
            crate::log_event!(Error,"runtime fatal error={cause:#}");
            error=Some(cause);
            "fatal"
        }
    };
    crate::log_event!(Info,"runtime shutdown reason={reason}");
    stop_tx.send_replace(true);
    let drained=tokio::time::timeout(Duration::from_secs(30),async{
        while let Some(joined)=tasks.join_next().await{joined??;}
        Ok::<(),anyhow::Error>(())
    }).await;
    match drained{
        Ok(Ok(()))=>{},
        Ok(Err(drain_error))=>{
            crate::log_event!(Error,"runtime shutdown drain_error={drain_error:#}");
            if let Some(original)=error.take(){
                return Err(original.context(format!("shutdown also failed: {drain_error:#}")));
            }
            return Err(drain_error);
        },
        Err(_)=>{
            tasks.abort_all();
            crate::log_event!(Warn,"runtime shutdown drain_timeout_s=30");
            if let Some(original)=error.take(){
                return Err(original.context("shutdown drain exceeded 30 seconds"));
            }
            bail!("runtime graceful shutdown exceeded 30 seconds");
        }
    }
    if let Some(error)=error{return Err(error);}
    crate::log_event!(Info,"runtime shutdown complete=true");
    Ok(())
}
