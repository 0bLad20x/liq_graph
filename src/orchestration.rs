//! Resource provisioning, channel connections, and worker lifecycle only.
use crate::workers::discovery::meteora_dlmm as discovery_meteora_dlmm;

use std::{env, sync::Arc};

mod runtime;
mod completion;
mod discovery;
use discovery::SourceSet;
mod v10_simulation;

use anyhow::{Context, Result, bail, ensure};
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, watch}, task::JoinSet};

use crate::{core, workers::{          graph_watch_catalog, source_jupiter_mint, source_dexscreener, graph_liquidity, persistence, pool_registry, rpc_transport, http_transport, diagnostic_source}};


struct Cli {
    meteora:discovery_meteora_dlmm::Settings,
    jupiter_only:bool,rpc_live:bool,http_transport_live:bool,
    pumpswap_discovery_live:bool,rpc_full_scan_live:bool,
    persistence_restore_live:bool,persistence_v2_live:bool,
    rest_discovery_live:bool,rest_source_only:Option<SourceSet>,
}
fn settings(args:impl Iterator<Item=String>)->Result<Cli> {
    let mut args=args;let mut once=false;let mut max_pages=None;
    let mut cli=Cli{meteora:discovery_meteora_dlmm::Settings{once:false,max_pages:None},
        jupiter_only:false,rpc_live:false,http_transport_live:false,
        pumpswap_discovery_live:false,rpc_full_scan_live:false,
        persistence_restore_live:false,persistence_v2_live:false,
        rest_discovery_live:false,rest_source_only:None};
    while let Some(arg)=args.next() {
        let source=match arg.as_str() {
            "--once"=>{once=true;None},
            "--pages"=>{let n=args.next().context("--pages requires a number")?.parse::<u32>()?;
                ensure!(n>0,"--pages must be positive");max_pages=Some(n);None},
            "--jupiter-only"=>{cli.jupiter_only=true;None},
            "--rpc-live"=>{cli.rpc_live=true;None},
            "--http-transport-live"=>{cli.http_transport_live=true;None},
            "--pumpswap-discovery-live"=>{cli.pumpswap_discovery_live=true;once=true;None},
            "--rpc-full-scan-live"=>{cli.rpc_full_scan_live=true;once=true;None},
            "--persistence-restore-live"=>{cli.persistence_restore_live=true;once=true;None},
            "--persistence-v2-live"=>{cli.persistence_v2_live=true;once=true;None},
            "--rest-discovery-live"=>{cli.rest_discovery_live=true;once=true;None},
            "--raydium-live"=>Some(SourceSet::Raydium),
            "--pancakeswap-live"=>Some(SourceSet::PancakeSwap),
            "--byreal-live"=>Some(SourceSet::Byreal),
            "--manifest-live"=>Some(SourceSet::Manifest),
            "--orca-live"=>Some(SourceSet::Orca),
            "--damm-v2-live"=>Some(SourceSet::DammV2),
            _=>bail!("unknown argument {arg}; use --run, --discovery-live SOURCE or --completion-live PROTOCOL"),
        };
        if let Some(source)=source {
            ensure!(cli.rest_source_only.is_none(),"select only one REST source");
            cli.rest_source_only=Some(source);once=true;
        }
    }
    let modes=usize::from(cli.jupiter_only)+usize::from(cli.rpc_live)+usize::from(cli.http_transport_live)
        +usize::from(cli.pumpswap_discovery_live)+usize::from(cli.rpc_full_scan_live)
        +usize::from(cli.persistence_restore_live)+usize::from(cli.persistence_v2_live)
        +usize::from(cli.rest_discovery_live)+usize::from(cli.rest_source_only.is_some());
    ensure!(modes<=1,"select only one standalone mode");
    ensure!(!(cli.jupiter_only&&(once||max_pages.is_some())),"Jupiter-only is continuous and has no pages");
    ensure!(!(cli.rpc_live||cli.http_transport_live)||(!once&&max_pages.is_none()),"transport checks have no pages/once");
    ensure!(!(cli.rpc_full_scan_live||cli.persistence_restore_live
        ||cli.rest_source_only==Some(SourceSet::Manifest))||max_pages.is_none(),"selected mode has no pages");
    cli.meteora=discovery_meteora_dlmm::Settings{once,max_pages};
    Ok(cli)
}

fn jupiter_settings() -> Result<Option<source_jupiter_mint::Settings>> {
    if env::var("JUPITER_SEARCH_API_KEYS").unwrap_or_default().trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(source_jupiter_mint::Settings::from_env()?))
}

fn spawn_sources(
    tasks: &mut JoinSet<Result<()>>,
    settings: discovery_meteora_dlmm::Settings,
    graph: Option<graph_liquidity::Settings>,
    jupiter: Option<source_jupiter_mint::Settings>,
    initial_registry: Option<pool_registry::PoolRegistry>,
    persist: Option<persistence::Persistence>,
    source_set: SourceSet,
    strict_lag: bool,
) -> Result<()> {
    let source_capacity: usize = env::var("REST_BROADCAST_CAPACITY")
        .unwrap_or_else(|_| "256".into()).parse().context("invalid REST_BROADCAST_CAPACITY")?;
    let registry_capacity: usize = env::var("REGISTRY_BROADCAST_CAPACITY")
        .unwrap_or_else(|_| "256".into()).parse().context("invalid REGISTRY_BROADCAST_CAPACITY")?;
    ensure!((32..=65536).contains(&source_capacity) && (32..=65536).contains(&registry_capacity),
        "REST/REGISTRY broadcast capacities must be 32..=65536");
    crate::log_event!(Info,"orchestration rest_sources={source_set:?} source_broadcast_capacity={} registry_broadcast_capacity={} fail_on_lag={strict_lag}",
        source_capacity, registry_capacity);
    let (output, input) = broadcast::channel::<Arc<core::DataBatch>>(source_capacity);
    let registry_input = output.subscribe();
    let (registry_output, registry_observer) =
        broadcast::channel::<Arc<core::PoolUpdateBatch>>(registry_capacity);
    let registry = initial_registry.unwrap_or_default();
    let (source_done_tx,_) = watch::channel(false);
    let persistence_input = persist.as_ref().map(|_| registry_output.subscribe());
    if let Some(settings) = graph {
        let (tx, rx) = watch::channel(Arc::new(graph_liquidity::GraphSnapshot::default()));
        tasks.spawn(graph_liquidity::observe(rx.clone()));
        spawn_watch_catalog(tasks, rx, jupiter);
        tasks.spawn(graph_liquidity::run(registry_output.subscribe(), tx, settings));
    } else {
        ensure!(jupiter.is_none(), "Jupiter Search requires a liquidity graph");
    }

    tasks.spawn(diagnostic_source::run(input, strict_lag));
    tasks.spawn(pool_registry::observe_updates(registry_observer, false, strict_lag));
    // Seed restored, historically validated pool records before any fresh source.
    // All recipients are already subscribed; the SQL writer ignores Source batches.
    let restored = registry.snapshot();
    if !restored.is_empty() {
        let count = restored.len();
        registry_output.send(Arc::new(core::PoolUpdateBatch {
            kind: core::PoolUpdateKind::Source,
            source: "persistence_restore",
            received_at_ms: 0,
            pools: restored,
            persist_identity: Vec::new(),
        })).map_err(|_|anyhow::anyhow!("no registry bootstrap subscriber"))?;
        crate::log_event!(Info,"orchestration bootstrap seeded_pools={count}");
    }
    if let Some(writer) = persist {
        tasks.spawn(writer.run(persistence_input.expect("persistence receiver subscribed")));
    }
    tasks.spawn(pool_registry::run(registry_input, None, registry,
        registry_output, source_done_tx, strict_lag, None));
    let (_stop,stop_rx)=watch::channel(false);
    let workers=discovery::configured(source_set,settings.once,settings.max_pages)?;
    discovery::spawn_finite(tasks,workers,discovery::Resources {
        http:core::http_client()?,rpc:None,output,stop:stop_rx,
    });
    Ok(())
}

// One catalog per graph generation; Jupiter receives only plain Mint addresses.
fn spawn_watch_catalog(
    tasks: &mut JoinSet<Result<()>>,
    graph: watch::Receiver<Arc<graph_liquidity::GraphSnapshot>>,
    jupiter: Option<source_jupiter_mint::Settings>,
) {
    let (catalog_tx, catalog_rx) =
        watch::channel(Arc::new(graph_watch_catalog::WatchCatalog::empty()));
    tasks.spawn(graph_watch_catalog::observe(catalog_rx));
    let mint_output = jupiter.map(|settings| {
        let (mint_tx, mint_rx) = watch::channel::<Arc<[Pubkey]>>(Arc::from([]));
        // Jupiter is continuous even if the one-shot graph restore completes.
        // Keep one sender alive for the lifetime of the Jupiter task.
        spawn_jupiter(tasks, settings, mint_rx, mint_tx.clone());
        mint_tx
    });
    tasks.spawn(graph_watch_catalog::run(graph, catalog_tx, mint_output));
}

fn spawn_jupiter(
    tasks: &mut JoinSet<Result<()>>,
    settings: source_jupiter_mint::Settings,
    mints: watch::Receiver<Arc<[Pubkey]>>,
    keepalive: watch::Sender<Arc<[Pubkey]>>,
) {
    let (output, input) = broadcast::channel::<Arc<core::DataBatch>>(32);
    let persistence_rx=output.subscribe();
    tasks.spawn(async move {
        let db=persistence::Persistence::connect().await?;
        let mut rx=persistence_rx;
        loop {
            match rx.recv().await {
                Ok(batch)=>{db.save_mint_batch(&batch).await?;},
                Err(broadcast::error::RecvError::Lagged(n))=>
                    bail!("legacy Jupiter persistence lost {n} batches"),
                Err(broadcast::error::RecvError::Closed)=>break,
            }
        }
        Ok(())
    });
    tasks.spawn(diagnostic_source::run(input, false));
    // Jupiter must outlive a finite graph restore. Its own task retains the
    // watch sender without requiring the catalog producer to stay running.
    tasks.spawn(async move {
        let _mint_sender = keepalive;
        source_jupiter_mint::run(reqwest::Client::new(), settings, mints, output).await
    });
}

fn test_command(worker: &str) -> Result<&'static str> {
    let legacy=match worker {
        "discovery_meteora_dlmm"=>"--once",
        "discovery_meteora_damm_v2"=>"--damm-v2-live",
        "discovery_orca_whirlpool"=>"--orca-live",
        "discovery_raydium"=>"--raydium-live",
        "discovery_pancakeswap_clmm"=>"--pancakeswap-live",
        "discovery_byreal_clmm"=>"--byreal-live",
        "discovery_manifest"=>"--manifest-live",
        "discovery_pumpswap"=>"--pumpswap-discovery-live",
        "discovery_rpc_full"=>"--rpc-full-scan-live",
        "source_jupiter_mint"=>"--jupiter-only",
        "persistence_restore"=>"--persistence-restore-live",
        "persistence_discovery"=>"--persistence-v2-live",
        "rpc_transport"=>"--rpc-live",
        "http_transport"=>"--http-transport-live",
        "discovery_rest_all"=>"--rest-discovery-live",
        _=>bail!("unknown --test module {worker}"),
    };
    Ok(legacy)
}

pub async fn run() -> Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(error) if error.not_found() => {}
        Err(error) => return Err(error).context("load .env"),
    }
    core::logging::init()?;

    let mut args: Vec<String> = env::args().skip(1).collect();
    if args.first().map(String::as_str)==Some("--completion-live") {
        ensure!(args.len()>=2,"use --completion-live PROTOCOL|all [--duration-seconds N] [--pool-limit N] [--revalidate] [--persist]");
        return completion::live(&args[1..]).await;
    }
    if args.len()>=2 && args[0]=="--test" && args[1].starts_with("completion_") {
        let mut selected=vec![args[1].trim_start_matches("completion_").to_owned()];
        selected.extend_from_slice(&args[2..]);
        return completion::live(&selected).await;
    }
    if args.first().map(String::as_str)==Some("--discovery-live") {
        return discovery::live(&args[1..]).await;
    }
    if args.len()>=2 && args[0]=="--test" && args[1]=="discovery_identity_hydration" {
        let mut selected=vec!["identity_hydration".to_owned()];
        selected.extend_from_slice(&args[2..]);
        return discovery::live(&selected).await;
    }

    if args.len()==1 && args[0]=="--rpc-ws-plan" {
        let settings=rpc_transport::settings_from_env()?;
        println!("{}",serde_json::to_string(&settings.websocket_plan()?)?);
        return Ok(());
    }
    if args.first().map(String::as_str)==Some("--v10-simulate") {
        ensure!(args.len()==4 && args[2]=="--wallet",
            "use --v10-simulate REQUEST.json --wallet KEYPAIR.json");
        return v10_simulation::run(std::path::Path::new(&args[1]),
            std::path::Path::new(&args[3])).await;
    }
    if args.first().map(String::as_str)==Some("--v10-wallet") {
        ensure!(args.len()==2,"use --v10-wallet KEYPAIR.json");
        println!("{}",crate::workers::execution_v10::Settings::load(std::path::Path::new(&args[1]))?.wallet());
        return Ok(());
    }
    if args.len()==1 && args[0]=="--run-plan" {
        println!("{}",serde_json::to_string(&runtime::plan()?)?);
        return Ok(());
    }
    if args.len() == 1 && args[0] == "--run" {
        return runtime::run().await;
    }
    if args.len()==1 && args[0]=="--cp1-live" {
        return runtime::cp1_live().await;
    }
    // Standalone backfill: the source only calls Jupiter; persistence owns SQL.
    if (args.len()==1 && args[0]=="--jupiter-mint-backfill")
        || (args.len()==2 && args[0]=="--test" && args[1]=="jupiter_mint_backfill") {
        let db=persistence::Persistence::connect().await?;
        let mut mints=std::collections::BTreeSet::<Pubkey>::new();
        for p in db.restore().await? {
            if let Some(m)=p.mint_a{mints.insert(m);}
            if let Some(m)=p.mint_b{mints.insert(m);}
        }
        let settings=source_jupiter_mint::Settings::from_env()?;
        let mut mints=mints.into_iter().collect::<Vec<_>>();
        if settings.backfill_limit>0{mints.truncate(settings.backfill_limit);}
        let (out,mut rx)=broadcast::channel::<Arc<core::DataBatch>>(4096);
        let writer=tokio::spawn(async move {
            let mut written=0_u64;
            loop {
                match rx.recv().await {
                    Ok(batch)=>written+=db.save_mint_batch(&batch).await?,
                    Err(broadcast::error::RecvError::Lagged(n))=>
                        bail!("Jupiter backfill persistence lost {n} batches"),
                    Err(broadcast::error::RecvError::Closed)=>break,
                }
            }
            crate::log_event!(Info,"jupiter backfill persisted_mints={written}");
            Ok::<(),anyhow::Error>(())
        });
        let result=source_jupiter_mint::run_backfill(core::http_client()?,settings,mints,out).await;
        writer.await.context("Jupiter persistence task panicked")??;
        return result;
    }
    // CP2: standalone DexScreener HTTP/proxy source, typed output and local JSONL.
    // No Pool Registry, Completion, Graph or DB writes.
    if (args.len()==1 && args[0]=="--dexscreener-live")
        || (args.len()==2 && args[0]=="--test" && args[1]=="source_dexscreener")
    {
        let db=persistence::Persistence::connect().await?;
        let registry=pool_registry::PoolRegistry::default();
        registry.restore(db.restore().await?)?;
        return source_dexscreener::live_diagnostic(registry).await;
    }
    if args.first().map(String::as_str) == Some("--test") {
        ensure!(args.len()>=2,"--test requires a module name");
        let normalized=test_command(&args[1])?.to_string();
        args.drain(..2);
        args.insert(0,normalized);
    }
    let cli = settings(args.into_iter())?;
    if cli.rpc_full_scan_live {
        return discovery::live(&["rpc_full".into(),"--persist".into()]).await;
    }
    if cli.pumpswap_discovery_live {
        let mut selected=vec!["pumpswap".to_owned()];
        if let Some(pages)=cli.meteora.max_pages {
            selected.extend(["--max-pages".into(),pages.to_string()]);
        }
        return discovery::live(&selected).await;
    }
    if cli.http_transport_live {
        http_transport::live_diagnostic().await?;
        return Ok(());
    }
    if cli.rpc_live {
        let config = rpc_transport::settings_from_env()?;
        let (handle, task) = rpc_transport::spawn(config)?;
        rpc_transport::live_probe(&handle).await?;
        drop(handle);
        task.await??;
        return Ok(());
    }
    if cli.jupiter_only {
        let jupiter = jupiter_settings()?
            .context("--jupiter-only requires JUPITER_SEARCH_API_KEYS")?;
        let database = persistence::Persistence::connect().await?;
        let registry = pool_registry::PoolRegistry::default();
        let restored = registry.restore(database.restore().await?)?;
        ensure!(restored > 0, "--jupiter-only requires persisted pools for graph bootstrap");
        let graph_settings = graph_liquidity::Settings::from_env()?;
        let (updates, graph_input) = broadcast::channel::<Arc<core::PoolUpdateBatch>>(32);
        let (graph_tx, graph_rx) =
            watch::channel(Arc::new(graph_liquidity::GraphSnapshot::default()));
        let mut tasks = JoinSet::new();
        tasks.spawn(graph_liquidity::observe(graph_rx.clone()));
        spawn_watch_catalog(&mut tasks, graph_rx, Some(jupiter));
        tasks.spawn(graph_liquidity::run(graph_input, graph_tx, graph_settings));
        updates.send(Arc::new(core::PoolUpdateBatch {
            kind: core::PoolUpdateKind::Source,
            source: "persistence_restore",
            received_at_ms: 0,
            pools: registry.snapshot(),
            persist_identity: Vec::new(),
        })).map_err(|_| anyhow::anyhow!("no Jupiter graph bootstrap subscriber"))?;
        drop(updates);
        crate::log_event!(Info,"orchestration jupiter-graph-live restored_pools={restored} source=persistence_restore");
        // Jupiter Search runs continuously against the restored graph's mint set.
        // The catalog producer retains the Jupiter mint sender during streaming.
        while let Some(result) = tasks.join_next().await { result??; }
        return Ok(());
    }
    if cli.persistence_v2_live {
        let database=persistence::Persistence::connect().await?;
        let registry=pool_registry::PoolRegistry::default();
        let restored=registry.restore(database.restore().await?)?;
        crate::log_event!(Info,"orchestration persistence-v2-live bootstrap_pools={restored} sources=All rpc_calls=0");
        let mut tasks=JoinSet::new();
        spawn_sources(&mut tasks,cli.meteora,None,None,
            Some(registry),Some(database),SourceSet::All,true)?;
        while let Some(result)=tasks.join_next().await {result??;}
        crate::log_event!(Info,"orchestration persistence-v2-live complete rpc_calls=0");
        return Ok(());
    }
    if cli.persistence_restore_live {
        let database = persistence::Persistence::connect().await?;
        let registry = pool_registry::PoolRegistry::default();
        let restored = registry.restore(database.restore().await?)?;
        ensure!(restored > 0, "persistence restore-live requires stored pools");
        let graph_settings = graph_liquidity::Settings::from_env()?;
        let (updates, receiver) = broadcast::channel::<Arc<core::PoolUpdateBatch>>(32);
        let (graph_tx, graph_rx) = watch::channel(Arc::new(graph_liquidity::GraphSnapshot::default()));
        let mut tasks = JoinSet::new();
        tasks.spawn(graph_liquidity::observe(graph_rx.clone()));
        spawn_watch_catalog(&mut tasks, graph_rx, None);
        tasks.spawn(graph_liquidity::run(receiver,graph_tx,graph_settings));
        updates.send(Arc::new(core::PoolUpdateBatch {
            kind: core::PoolUpdateKind::Source,
            source:"persistence_restore",
            received_at_ms:0,
            pools:registry.snapshot(),
            persist_identity: Vec::new(),
        })).map_err(|_|anyhow::anyhow!("no graph bootstrap receiver"))?;
        drop(updates);
        while let Some(result)=tasks.join_next().await {result??;}
        crate::log_event!(Info,"persistence restore-live complete restored_pools={restored} rpc_calls=0");
        return Ok(());
    }
    let rpc_handle = if !cli.meteora.once && rpc_transport::configured() {
        let (handle, task) = rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
        // Keep the transport available to future worker consumers.
        // A standalone RPC worker is deliberately idle without explicit requests.
        Some((handle, task))
    } else { None };
    let jupiter = if cli.meteora.once { None } else { jupiter_settings()? };
    let (registry, graph) = if jupiter.is_some() {
        // Reuse validated completion state; new REST discoveries still require
        // their own completion before they can enter the graph watchlist.
        let database = persistence::Persistence::connect().await?;
        let registry = pool_registry::PoolRegistry::default();
        let restored = registry.restore(database.restore().await?)?;
        crate::log_event!(Info,"orchestration Jupiter graph bootstrap restored_pools={restored}");
        (Some(registry), Some(graph_liquidity::Settings::from_env()?))
    } else { (None, None) };

    let mut tasks = JoinSet::new();
    let source_set = cli.rest_source_only.unwrap_or(if cli.rest_discovery_live || !cli.meteora.once {
        SourceSet::All
    } else { SourceSet::Dlmm });
    let fail_on_lag = cli.rest_discovery_live || cli.rest_source_only.is_some()
        || env::var("REST_FAIL_ON_LAG").ok().as_deref() == Some("true");
    spawn_sources(&mut tasks, cli.meteora, graph, jupiter, registry, None,
        source_set, fail_on_lag)?;

    let _rpc_handle = if let Some((handle, task)) = rpc_handle {
        tasks.spawn(async move {task.await?});
        Some(handle)
    } else { None };
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}
