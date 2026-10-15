//! Resource provisioning, channel connections, and worker lifecycle only.

use std::{collections::HashSet, env, fs, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, watch}, task::JoinSet};

use crate::{core, workers::{damm_v2_completion, execution_completion, jupiter_search, liquidity_graph, meteora_dlmm, meteora_damm_v2, raydium, orca, persistence, pool_registry, rpc_transport, source_diagnostic}};

#[derive(Clone, Copy, Debug)]
enum SourceSet { Dlmm, DlmmDammV2, Raydium, Orca, DammV2, All }

struct CompletionConfig {
    rpc: rpc_transport::RpcHandle,
    dlmm: Option<execution_completion::Settings>,
    damm_v2: Option<damm_v2_completion::Settings>,
}

struct Cli {
    meteora: meteora_dlmm::Settings,
    jupiter_only: bool,
    rpc_live: bool,
    completion_live: bool,
    damm_v2_completion_live: bool,
    graph_live: bool,
    completion_limit: Option<usize>,
    persistence_write_live: bool,
    persistence_restore_live: bool,
    persistence_v2_live: bool,
    stability_live: bool,
    rest_discovery_live: bool,
    rest_source_only: Option<SourceSet>,
}

fn settings() -> Result<Cli> {
    let mut args = env::args().skip(1);
    let mut once = false;
    let mut max_pages = None;
    let mut jupiter_only = false;
    let mut rpc_live = false;
    let mut completion_live = false;
    let mut damm_v2_completion_live = false;
    let mut graph_live = false;
    let mut completion_limit = None;
    let mut persistence_write_live = false;
    let mut persistence_restore_live = false;
    let mut persistence_v2_live = false;
    let mut stability_live = false;
    let mut rest_discovery_live = false;
    let mut rest_source_only = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--once" => once = true,
            "--jupiter-only" => jupiter_only = true,
            "--rpc-live" => rpc_live = true,
            "--completion-live" => { completion_live = true; once = true; },
            "--damm-v2-completion-live" => { damm_v2_completion_live = true; once = true; },
            "--graph-live" => { graph_live = true; once = true; },
            "--persistence-write-live" => { persistence_write_live = true; once = true; },
            "--persistence-restore-live" => { persistence_restore_live = true; once = true; },
            "--persistence-v2-live" => { persistence_v2_live = true; once = true; },
            "--stability-live" => stability_live = true,
            "--rest-discovery-live" => { rest_discovery_live = true; once = true; },
            "--raydium-live" => { ensure!(rest_source_only.is_none(), "select only one REST source"); rest_source_only = Some(SourceSet::Raydium); once = true; },
            "--orca-live" => { ensure!(rest_source_only.is_none(), "select only one REST source"); rest_source_only = Some(SourceSet::Orca); once = true; },
            "--damm-v2-live" => { ensure!(rest_source_only.is_none(), "select only one REST source"); rest_source_only = Some(SourceSet::DammV2); once = true; },
            "--completion-limit" => {
                let count: usize = args.next()
                    .context("--completion-limit requires a positive number")?
                    .parse().context("invalid --completion-limit value")?;
                ensure!(count <= 5000, "--completion-limit must be 0 (unbounded) or 1..=5000");
                completion_limit = Some(count);
            }
            "--pages" => {
                let count: u32 = args.next()
                    .context("--pages requires a positive number")?
                    .parse().context("invalid --pages value")?;
                ensure!(count > 0, "--pages must be positive");
                max_pages = Some(count);
            }
            _ => bail!("unknown argument {arg}; use --once, --pages N, --jupiter-only, --rpc-live, --completion-live, --damm-v2-completion-live, --persistence-write-live, --persistence-restore-live, --persistence-v2-live, --stability-live, --rest-discovery-live, --raydium-live, --orca-live, --damm-v2-live, --graph-live or --completion-limit N"),
        }
    }
    ensure!(!(rpc_live && (jupiter_only || once || max_pages.is_some() || stability_live || rest_discovery_live)),
        "--rpc-live is an exclusive RPC transport check");
    ensure!(!((rest_discovery_live || rest_source_only.is_some()) && (completion_live || graph_live || persistence_write_live
        || persistence_restore_live || stability_live || rpc_live || jupiter_only || completion_limit.is_some()
        || (rest_discovery_live && rest_source_only.is_some()))),
        "--rest-discovery-live is an API-only checkpoint with optional --pages");
    ensure!(!(persistence_v2_live && (persistence_write_live || persistence_restore_live
        || completion_live || damm_v2_completion_live || graph_live || stability_live || rest_discovery_live
        || rest_source_only.is_some() || rpc_live || jupiter_only
        || completion_limit.is_some())),
        "--persistence-v2-live is an exclusive REST+PostgreSQL checkpoint");
    ensure!(!(completion_live && graph_live), "--completion-live and --graph-live are exclusive");
    ensure!(!(damm_v2_completion_live && (graph_live || persistence_write_live
        || persistence_restore_live || persistence_v2_live || stability_live || rpc_live
        || rest_discovery_live || rest_source_only.is_some() || jupiter_only)),
        "--damm-v2-completion-live is an integrated RPC/Registry/PostgreSQL/Graph mode; only --completion-live may be combined to enable both DEXes");
    ensure!(!(stability_live && (completion_live || graph_live || persistence_write_live
        || persistence_restore_live || rpc_live || jupiter_only || once)),
        "--stability-live is continuous and cannot be combined with other live modes or --once");
    ensure!(!(persistence_write_live && (completion_live || graph_live || persistence_restore_live)),
        "--persistence-write-live is exclusive");
    ensure!(!(persistence_restore_live && (completion_live || graph_live || jupiter_only
        || rpc_live || max_pages.is_some() || completion_limit.is_some())),
        "--persistence-restore-live is exclusive and does not use API/RPC flags");
    ensure!(!(rpc_live && persistence_write_live), "--rpc-live and --persistence-write-live are exclusive");
    ensure!(!(graph_live && jupiter_only), "--graph-live cannot be combined with --jupiter-only");
    ensure!(!(persistence_write_live && jupiter_only), "--persistence-write-live cannot use Jupiter-only");
    ensure!(!(completion_live && jupiter_only),
        "--completion-live cannot be combined with --jupiter-only");
    ensure!(completion_limit.is_none() || completion_live || damm_v2_completion_live
        || graph_live || persistence_write_live || stability_live,
        "--completion-limit requires a Completion/Graph/Persistence/Stability live mode");
    ensure!(
        !(jupiter_only && (once || max_pages.is_some())),
        "--jupiter-only cannot be combined with --once or --pages"
    );
    Ok(Cli {
        meteora: meteora_dlmm::Settings { once, max_pages },
        jupiter_only,
        rpc_live,
        completion_live,
        damm_v2_completion_live,
        graph_live,
        completion_limit,
        persistence_write_live,
        persistence_restore_live,
        persistence_v2_live,
        stability_live,
        rest_discovery_live,
        rest_source_only,
    })
}

// Temporary startup input for local tests; replaced by a real watch-input producer.
fn load_mints(path: &str) -> Result<Arc<[Pubkey]>> {
    let raw_file = fs::read_to_string(path)
        .with_context(|| format!("read Jupiter mint input {path}"))?;
    let mut mints = Vec::new();
    let mut seen = HashSet::new();
    for (line, raw) in raw_file.lines().enumerate() {
        let raw = raw.trim();
        if raw.is_empty() || raw.starts_with('#') {
            continue;
        }
        let mint: Pubkey = raw.parse()
            .with_context(|| format!("{path}: invalid mint on line {}", line + 1))?;
        ensure!(mint != Pubkey::default(), "{path}: zero mint on line {}", line + 1);
        if seen.insert(mint) {
            mints.push(mint);
        }
    }
    ensure!(!mints.is_empty(), "{path}: no mint addresses");
    Ok(Arc::from(mints))
}

fn jupiter_settings() -> Result<Option<(jupiter_search::Settings, Arc<[Pubkey]>)>> {
    let keys = env::var("JUPITER_SEARCH_API_KEYS").unwrap_or_default();
    let mut seen = HashSet::new();
    let api_keys = keys.split(|ch| ch == ',' || ch == '\n')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .filter(|key| seen.insert((*key).to_owned()))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if api_keys.is_empty() {
        return Ok(None);
    }

    let path = env::var("JUPITER_MINTS_FILE")
        .unwrap_or_else(|_| "jupiter_mints.txt".to_owned());
    if !Path::new(&path).is_file() {
        eprintln!("jupiter source not started: mint input file missing: {path}");
        return Ok(None);
    }

    let calls_per_minute_per_key = env::var("JUPITER_SEARCH_CALLS_PER_MINUTE")
        .unwrap_or_else(|_| "57".to_owned())
        .parse::<f64>()
        .context("invalid JUPITER_SEARCH_CALLS_PER_MINUTE")?;
    let batch_size = env::var("JUPITER_SEARCH_BATCH_SIZE")
        .unwrap_or_else(|_| "100".to_owned())
        .parse::<usize>()
        .context("invalid JUPITER_SEARCH_BATCH_SIZE")?;
    let timeout = env::var("JUPITER_SEARCH_REQUEST_TIMEOUT_SECONDS")
        .unwrap_or_else(|_| "20".to_owned())
        .parse::<u64>()
        .context("invalid JUPITER_SEARCH_REQUEST_TIMEOUT_SECONDS")?;
    let url = env::var("JUPITER_SEARCH_URL")
        .unwrap_or_else(|_| "https://api.jup.ag/tokens/v2/search".to_owned());

    Ok(Some((
        jupiter_search::Settings {
            url, api_keys, calls_per_minute_per_key, batch_size,
            timeout: Duration::from_secs(timeout),
        },
        load_mints(&path)?,
    )))
}

fn spawn_sources(
    tasks: &mut JoinSet<Result<()>>,
    settings: meteora_dlmm::Settings,
    completion: Option<CompletionConfig>,
    graph: Option<liquidity_graph::Settings>,
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
    println!("orchestration rest_sources={source_set:?} source_broadcast_capacity={} registry_broadcast_capacity={} fail_on_lag={strict_lag}",
        source_capacity, registry_capacity);
    let (output, input) = broadcast::channel::<Arc<core::DataBatch>>(source_capacity);
    let registry_input = output.subscribe();
    let (registry_output, registry_observer) =
        broadcast::channel::<Arc<core::PoolUpdateBatch>>(registry_capacity);
    let registry = initial_registry.unwrap_or_default();
    let completion_live = completion.is_some();
    let (source_done_tx,source_done_rx) = watch::channel(false);
    let persistence_input = persist.as_ref().map(|_| registry_output.subscribe());
    if let Some(settings) = graph {
        let (tx, rx) = watch::channel(Arc::new(liquidity_graph::GraphSnapshot::default()));
        tasks.spawn(liquidity_graph::observe(rx));
        tasks.spawn(liquidity_graph::run(registry_output.subscribe(), tx, settings));
    }

    let completion_input = if let Some(config) = completion {
        let (completion_output, _) =
            broadcast::channel::<Arc<core::CompletionResultBatch>>(256);
        let receiver = completion_output.subscribe();
        if let Some(dlmm) = config.dlmm {
            // Verbose DLMM proof only in its original bounded standalone mode.
            if config.damm_v2.is_none() && dlmm.limit > 0 {
                tasks.spawn(execution_completion::observe(completion_output.subscribe()));
            }
            tasks.spawn(execution_completion::run(registry_output.subscribe(),
                source_done_rx.clone(), config.rpc.clone(), dlmm, completion_output.clone()));
        }
        if let Some(damm_v2) = config.damm_v2 {
            tasks.spawn(damm_v2_completion::run(registry_output.subscribe(),
                source_done_rx, config.rpc, damm_v2, completion_output.clone()));
        }
        // Registry closes only after every completion producer has finished.
        drop(completion_output);
        Some(receiver)
    } else { None };
    tasks.spawn(source_diagnostic::run(input, strict_lag));
    tasks.spawn(pool_registry::observe_updates(registry_observer, completion_live, strict_lag));
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
        println!("orchestration bootstrap seeded_pools={count}");
    }
    if let Some(writer) = persist {
        tasks.spawn(writer.run(persistence_input.expect("persistence receiver subscribed")));
    }
    tasks.spawn(pool_registry::run(registry_input, completion_input, registry,
        registry_output, source_done_tx, strict_lag));
    if matches!(source_set, SourceSet::Raydium | SourceSet::All) {
        let rps: u32 = env::var("RAYDIUM_API_RPS")
            .unwrap_or_else(|_| "10".into()).parse().context("invalid RAYDIUM_API_RPS")?;
        tasks.spawn(raydium::run(core::http_client()?, output.clone(), raydium::Settings {
            once: settings.once, max_pages: settings.max_pages, rps,
        }));
    }
    if matches!(source_set, SourceSet::Orca | SourceSet::All) {
        let rps: u32 = env::var("ORCA_API_RPS")
            .unwrap_or_else(|_| "10".into()).parse().context("invalid ORCA_API_RPS")?;
        tasks.spawn(orca::run(core::http_client()?, output.clone(), orca::Settings {
            once: settings.once, max_pages: settings.max_pages, rps,
        }));
    }
    if matches!(source_set, SourceSet::DammV2 | SourceSet::DlmmDammV2 | SourceSet::All) {
        tasks.spawn(meteora_damm_v2::run(core::http_client()?, output.clone(),
            meteora_damm_v2::Settings { once: settings.once, max_pages: settings.max_pages }));
    }
    if matches!(source_set, SourceSet::Dlmm | SourceSet::DlmmDammV2 | SourceSet::All) {
        tasks.spawn(meteora_dlmm::run(core::http_client()?, output, settings));
    }
    Ok(())
}

fn spawn_jupiter(
    tasks: &mut JoinSet<Result<()>>,
    settings: jupiter_search::Settings,
    mints: Arc<[Pubkey]>,
) -> watch::Sender<Arc<[Pubkey]>> {
    let (output, input) = broadcast::channel::<Arc<core::DataBatch>>(32);
    tasks.spawn(source_diagnostic::run(input, false));

    let (input_tx, input_rx) = watch::channel(mints);
    // Independent client: the shared HTTP client applies Meteora's 20-second limit.
    tasks.spawn(jupiter_search::run(reqwest::Client::new(), settings, input_rx, output));
    input_tx
}

pub async fn run() -> Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(error) if error.not_found() => {}
        Err(error) => return Err(error).context("load .env"),
    }

    let cli = settings()?;
    if cli.rpc_live {
        let config = rpc_transport::settings_from_env()?;
        let (handle, task) = rpc_transport::spawn(config)?;
        rpc_transport::live_probe(&handle).await?;
        drop(handle);
        task.await??;
        return Ok(());
    }
    if cli.persistence_v2_live {
        let database=persistence::Persistence::connect().await?;
        let registry=pool_registry::PoolRegistry::default();
        let restored=registry.restore(database.restore().await?)?;
        println!("orchestration persistence-v2-live bootstrap_pools={restored} sources=All rpc_calls=0");
        let mut tasks=JoinSet::new();
        spawn_sources(&mut tasks,cli.meteora,None,None,
            Some(registry),Some(database),SourceSet::All,true)?;
        while let Some(result)=tasks.join_next().await {result??;}
        println!("orchestration persistence-v2-live complete rpc_calls=0");
        return Ok(());
    }
    if cli.persistence_restore_live {
        let database = persistence::Persistence::connect().await?;
        let registry = pool_registry::PoolRegistry::default();
        let restored = registry.restore(database.restore().await?)?;
        ensure!(restored > 0, "persistence restore-live requires stored pools");
        let graph_settings = liquidity_graph::Settings::from_env()?;
        ensure!(graph_settings.min_tvl_usd.is_none(),
            "restore-live cannot use GRAPH_MIN_TVL_USD without a fresh TVL source");
        let (updates, receiver) = broadcast::channel::<Arc<core::PoolUpdateBatch>>(32);
        let (graph_tx, graph_rx) = watch::channel(Arc::new(liquidity_graph::GraphSnapshot::default()));
        let mut tasks = JoinSet::new();
        tasks.spawn(liquidity_graph::observe(graph_rx));
        tasks.spawn(liquidity_graph::run(receiver,graph_tx,graph_settings));
        updates.send(Arc::new(core::PoolUpdateBatch {
            kind: core::PoolUpdateKind::Source,
            source:"persistence_restore",
            received_at_ms:0,
            pools:registry.snapshot(),
            persist_identity: Vec::new(),
        })).map_err(|_|anyhow::anyhow!("no graph bootstrap receiver"))?;
        drop(updates);
        while let Some(result)=tasks.join_next().await {result??;}
        println!("persistence restore-live complete restored_pools={restored} rpc_calls=0");
        return Ok(());
    }
    if cli.completion_live || cli.damm_v2_completion_live
        || cli.graph_live || cli.persistence_write_live || cli.stability_live {
        let (rpc, rpc_task) = rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
        let mut tasks = JoinSet::new();
        let mut completion = execution_completion::Settings::from_env()?;
        // Continuous stability mode has no arbitrary pool-count cap.
        // Zero can also be requested explicitly for finite whole-universe scans.
        completion.limit = cli.completion_limit.unwrap_or(
            if cli.stability_live {0}
            else if cli.graph_live || cli.persistence_write_live {5000}
            else {100});

        let graph = if cli.graph_live || cli.persistence_write_live
            || cli.stability_live || cli.damm_v2_completion_live {
            Some(liquidity_graph::Settings::from_env()?)
        } else {None};
        let (registry, writer) = if cli.persistence_write_live
            || cli.stability_live || cli.damm_v2_completion_live {
            let db = persistence::Persistence::connect().await?;
            let registry = pool_registry::PoolRegistry::default();
            let restored = registry.restore(db.restore().await?)?;
            println!("orchestration initial persistence restored_pools={restored}");
            (Some(registry), Some(db))
        } else {(None,None)};
        if cli.stability_live {
            println!("orchestration stability-live continuous_source=true pages={:?} completion_limit={} (0=unbounded) postgres=true graph=true",
                cli.meteora.max_pages, completion.limit);
        }
        let source_set = if cli.damm_v2_completion_live {
            if cli.completion_live { SourceSet::DlmmDammV2 } else { SourceSet::DammV2 }
        } else { SourceSet::Dlmm };
        let damm_v2 = if cli.damm_v2_completion_live {
            let mut settings = damm_v2_completion::Settings::from_env()?;
            settings.limit = completion.limit;
            Some(settings)
        } else { None };
        let dlmm = if !cli.damm_v2_completion_live || cli.completion_live {
            Some(completion)
        } else { None };
        spawn_sources(&mut tasks, cli.meteora,
            Some(CompletionConfig {rpc:rpc.clone(), dlmm, damm_v2}),
            graph, registry, writer, source_set, false)?;
        // Completion owns the last handle. The HTTP transport terminates once it completes.
        drop(rpc);
        tasks.spawn(async move { rpc_task.await? });
        while let Some(result) = tasks.join_next().await { result??; }
        return Ok(());
    }

    let rpc_handle = if !cli.meteora.once && rpc_transport::configured() {
        let (handle, task) = rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
        // Keep the transport available to future worker consumers.
        // A standalone RPC worker is deliberately idle without explicit requests.
        Some((handle, task))
    } else { None };
    let jupiter = if cli.meteora.once && !cli.jupiter_only {
        None
    } else {
        jupiter_settings()?
    };
    ensure!(
        !cli.jupiter_only || jupiter.is_some(),
        "Jupiter Search needs API keys and a mint input file"
    );

    let mut tasks = JoinSet::new();
    if !cli.jupiter_only {
        let source_set = cli.rest_source_only.unwrap_or(if cli.rest_discovery_live || !cli.meteora.once {
            SourceSet::All
        } else { SourceSet::Dlmm });
        let fail_on_lag = cli.rest_discovery_live || cli.rest_source_only.is_some()
            || env::var("REST_FAIL_ON_LAG").ok().as_deref() == Some("true");
        spawn_sources(&mut tasks, cli.meteora, None, None, None, None,
            source_set, fail_on_lag)?;
    }

    // Keep the provisional watch sender alive until a real input worker takes over.
    let _jupiter_input = jupiter
        .map(|(settings, mints)| spawn_jupiter(&mut tasks, settings, mints));

    let _rpc_handle = if let Some((handle, task)) = rpc_handle {
        tasks.spawn(async move {task.await?});
        Some(handle)
    } else { None };
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}
