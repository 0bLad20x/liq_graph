//! Resource provisioning, channel connections, and worker lifecycle only.

use std::{collections::HashSet, env, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, watch}, task::JoinSet};

use crate::{core, workers::{completion_meteora_damm_v2, completion_orca_whirlpool, completion_raydium_amm_v4, completion_raydium_cpmm, completion_raydium_clmm, completion_pancakeswap_clmm, completion_byreal_clmm, completion_manifest, completion_pumpswap, completion_meteora_dlmm, graph_watch_catalog, source_jupiter_mint, graph_liquidity, discovery_meteora_dlmm, discovery_meteora_damm_v2, discovery_raydium, discovery_pancakeswap_clmm, discovery_byreal_clmm, discovery_manifest, discovery_pumpswap, discovery_orca_whirlpool, persistence, pool_registry, rpc_transport, http_transport, diagnostic_source}};

#[derive(Clone, Copy, Debug)]
enum SourceSet { Dlmm, DlmmDammV2, Raydium, PancakeSwap, Byreal, Manifest, PumpSwap, Orca, DammV2, All }

struct CompletionConfig {
    rpc: rpc_transport::RpcHandle,
    dlmm: Option<completion_meteora_dlmm::Settings>,
    damm_v2: Option<completion_meteora_damm_v2::Settings>,
    orca: Option<completion_orca_whirlpool::Settings>,
    raydium_amm_v4: Option<completion_raydium_amm_v4::Settings>,
    raydium_cpmm: Option<completion_raydium_cpmm::Settings>,
    raydium_clmm: Option<completion_raydium_clmm::Settings>,
    pancakeswap_clmm: Option<completion_pancakeswap_clmm::Settings>,
    byreal_clmm: Option<completion_byreal_clmm::Settings>,
    manifest: Option<completion_manifest::Settings>,
    pumpswap: Option<completion_pumpswap::Settings>,
}

struct Cli {
    meteora: discovery_meteora_dlmm::Settings,
    jupiter_only: bool,
    rpc_live: bool,
    http_transport_live: bool,
    completion_live: bool,
    damm_v2_completion_live: bool,
    orca_completion_live: bool,
    raydium_amm_v4_completion_live: bool,
    raydium_cpmm_completion_live: bool,
    raydium_clmm_completion_live: bool,
    pancakeswap_clmm_completion_live: bool,
    byreal_clmm_completion_live: bool,
    manifest_completion_live: bool,
    pumpswap_discovery_live: bool,
    pumpswap_completion_live: bool,
    pumpswap_rolling: bool,
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
    let mut http_transport_live = false;
    let mut completion_live = false;
    let mut damm_v2_completion_live = false;
    let mut orca_completion_live = false;
    let mut raydium_amm_v4_completion_live = false;
    let mut raydium_cpmm_completion_live = false;
    let mut raydium_clmm_completion_live = false;
    let mut pancakeswap_clmm_completion_live = false;
    let mut byreal_clmm_completion_live = false;
    let mut manifest_completion_live = false;
    let mut pumpswap_discovery_live = false;
    let mut pumpswap_completion_live = false;
    let mut pumpswap_rolling = false;
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
            "--http-transport-live" => http_transport_live = true,
            "--completion-live" => { completion_live = true; once = true; },
            "--damm-v2-completion-live" => { damm_v2_completion_live = true; once = true; },
            "--orca-completion-live" => { orca_completion_live = true; once = true; },
            "--raydium-amm-v4-completion-live" => { raydium_amm_v4_completion_live = true; once = true; },
            "--raydium-cpmm-completion-live" => { raydium_cpmm_completion_live = true; once = true; },
            "--raydium-clmm-completion-live" => { raydium_clmm_completion_live = true; once = true; },
            "--pancakeswap-clmm-completion-live" => { pancakeswap_clmm_completion_live = true; once = true; },
            "--byreal-clmm-completion-live" => { byreal_clmm_completion_live = true; once = true; },
            "--manifest-completion-live" => { manifest_completion_live = true; once = true; },
            "--pumpswap-discovery-live" => { pumpswap_discovery_live = true; once = true; },
            "--pumpswap-completion-live" => { pumpswap_completion_live = true; once = true; },
            "--pumpswap-rolling" => { pumpswap_rolling = true; },
            "--graph-live" => { graph_live = true; once = true; },
            "--persistence-write-live" => { persistence_write_live = true; once = true; },
            "--persistence-restore-live" => { persistence_restore_live = true; once = true; },
            "--persistence-v2-live" => { persistence_v2_live = true; once = true; },
            "--stability-live" => stability_live = true,
            "--rest-discovery-live" => { rest_discovery_live = true; once = true; },
            "--raydium-live" => { ensure!(rest_source_only.is_none(), "select only one REST source"); rest_source_only = Some(SourceSet::Raydium); once = true; },
            "--pancakeswap-live" => { ensure!(rest_source_only.is_none(), "select only one REST source"); rest_source_only = Some(SourceSet::PancakeSwap); once = true; },
            "--byreal-live" => { ensure!(rest_source_only.is_none(), "select only one REST source"); rest_source_only = Some(SourceSet::Byreal); once = true; },
            "--manifest-live" => { ensure!(rest_source_only.is_none(), "select only one REST source"); rest_source_only = Some(SourceSet::Manifest); once = true; },
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
            _ => bail!("unknown argument {arg}; use --once, --pages N, --jupiter-only, --rpc-live, --http-transport-live, --completion-live, --damm-v2-completion-live, --orca-completion-live, --raydium-amm-v4-completion-live, --raydium-cpmm-completion-live, --raydium-clmm-completion-live, --pancakeswap-clmm-completion-live, --byreal-clmm-completion-live, --manifest-completion-live, --manifest-live, --persistence-write-live, --persistence-restore-live, --persistence-v2-live, --stability-live, --rest-discovery-live, --raydium-live, --pancakeswap-live, --byreal-live, --orca-live, --damm-v2-live, --graph-live or --completion-limit N"),
        }
    }
    ensure!(!(manifest_completion_live && (rpc_live || http_transport_live || jupiter_only
        || completion_live || damm_v2_completion_live || orca_completion_live
        || raydium_amm_v4_completion_live || raydium_cpmm_completion_live
        || raydium_clmm_completion_live || pancakeswap_clmm_completion_live
        || byreal_clmm_completion_live || graph_live || persistence_write_live
        || persistence_restore_live || persistence_v2_live || stability_live
        || rest_discovery_live || rest_source_only.is_some())),
        "--manifest-completion-live is an exclusive REST/RPC/Registry/PostgreSQL/Graph checkpoint");
    ensure!(!(matches!(rest_source_only, Some(SourceSet::Manifest)) && max_pages.is_some()),
        "Manifest /tickers has no pagination; do not use --pages");
    ensure!(!(byreal_clmm_completion_live && (rpc_live || http_transport_live || jupiter_only
        || completion_live || damm_v2_completion_live || orca_completion_live
        || raydium_amm_v4_completion_live || raydium_cpmm_completion_live
        || raydium_clmm_completion_live || pancakeswap_clmm_completion_live
        || graph_live || persistence_write_live || persistence_restore_live
        || persistence_v2_live || stability_live || rest_discovery_live
        || rest_source_only.is_some())),
        "--byreal-clmm-completion-live is an exclusive REST/RPC/Registry/PostgreSQL/Graph checkpoint");
    ensure!(!(pancakeswap_clmm_completion_live && (rpc_live || http_transport_live || jupiter_only
        || completion_live || damm_v2_completion_live || orca_completion_live
        || raydium_amm_v4_completion_live || raydium_cpmm_completion_live
        || raydium_clmm_completion_live || graph_live || persistence_write_live
        || persistence_restore_live || persistence_v2_live || stability_live
        || rest_discovery_live || rest_source_only.is_some())),
        "--pancakeswap-clmm-completion-live is an exclusive REST/RPC/Registry/PostgreSQL/Graph checkpoint");
    ensure!(!(raydium_clmm_completion_live && (rpc_live || http_transport_live || jupiter_only
        || completion_live || damm_v2_completion_live || orca_completion_live
        || raydium_amm_v4_completion_live || raydium_cpmm_completion_live || graph_live
        || persistence_write_live || persistence_restore_live || persistence_v2_live
        || stability_live || rest_discovery_live || rest_source_only.is_some())),
        "--raydium-clmm-completion-live is an exclusive REST/RPC/Registry/PostgreSQL/Graph checkpoint");
    ensure!(!(raydium_cpmm_completion_live && (rpc_live || http_transport_live || jupiter_only
        || completion_live || damm_v2_completion_live || orca_completion_live
        || raydium_amm_v4_completion_live || graph_live || persistence_write_live
        || persistence_restore_live || persistence_v2_live || stability_live
        || rest_discovery_live || rest_source_only.is_some())),
        "--raydium-cpmm-completion-live is an exclusive REST/RPC/Registry/PostgreSQL/Graph checkpoint");
    ensure!(!(raydium_amm_v4_completion_live && (rpc_live || http_transport_live || jupiter_only
        || completion_live || damm_v2_completion_live || orca_completion_live || graph_live
        || persistence_write_live || persistence_restore_live || persistence_v2_live
        || stability_live || rest_discovery_live || rest_source_only.is_some())),
        "--raydium-amm-v4-completion-live is an exclusive REST/RPC/Registry/PostgreSQL/Graph checkpoint");
    ensure!(!(orca_completion_live && (rpc_live || http_transport_live || jupiter_only
        || completion_live || damm_v2_completion_live || graph_live
        || persistence_write_live || persistence_restore_live || persistence_v2_live
        || stability_live || rest_discovery_live || rest_source_only.is_some())),
        "--orca-completion-live is an exclusive Orca REST/RPC/Registry/PostgreSQL/Graph checkpoint");
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
        || orca_completion_live || raydium_amm_v4_completion_live
        || raydium_cpmm_completion_live || raydium_clmm_completion_live
        || pancakeswap_clmm_completion_live || byreal_clmm_completion_live
        || manifest_completion_live || pumpswap_completion_live || pumpswap_rolling || graph_live || persistence_write_live || stability_live,
        "--completion-limit requires a Completion/Graph/Persistence/Stability live mode");
    ensure!(
        !(jupiter_only && (once || max_pages.is_some())),
        "--jupiter-only cannot be combined with --once or --pages"
    );
    ensure!(!http_transport_live || (!rpc_live && !jupiter_only && !once
        && max_pages.is_none() && completion_limit.is_none() && !completion_live
        && !damm_v2_completion_live && !orca_completion_live && !raydium_amm_v4_completion_live
        && !raydium_cpmm_completion_live && !raydium_clmm_completion_live
        && !graph_live && !persistence_write_live
        && !persistence_restore_live && !persistence_v2_live && !stability_live
        && !rest_discovery_live && rest_source_only.is_none()),
        "--http-transport-live must be used alone");
    let pump_modes = usize::from(pumpswap_discovery_live)
        + usize::from(pumpswap_completion_live) + usize::from(pumpswap_rolling);
    ensure!(pump_modes <= 1, "select only one PumpSwap mode");
    if pump_modes > 0 {
        ensure!(!jupiter_only && !rpc_live && !http_transport_live
            && !completion_live && !damm_v2_completion_live && !orca_completion_live
            && !raydium_amm_v4_completion_live && !raydium_cpmm_completion_live
            && !raydium_clmm_completion_live && !pancakeswap_clmm_completion_live
            && !byreal_clmm_completion_live && !manifest_completion_live
            && !graph_live && !persistence_write_live && !persistence_restore_live
            && !persistence_v2_live && !stability_live && !rest_discovery_live
            && rest_source_only.is_none(),
            "PumpSwap checkpoint mode is exclusive");
        ensure!(!pumpswap_rolling || (!once && max_pages.is_none()),
            "continuous PumpSwap rolling cannot use --once or --pages");
        ensure!(!pumpswap_discovery_live || completion_limit.is_none(),
            "PumpSwap discovery alone does not support --completion-limit");
        ensure!(!pumpswap_completion_live || max_pages.is_none(),
            "PumpSwap completion requires an untruncated discovery scan");
    }
    Ok(Cli {
        meteora: discovery_meteora_dlmm::Settings { once, max_pages },
        jupiter_only,
        rpc_live,
        http_transport_live,
        completion_live,
        damm_v2_completion_live,
        orca_completion_live,
        raydium_amm_v4_completion_live,
        raydium_cpmm_completion_live,
        raydium_clmm_completion_live,
        pancakeswap_clmm_completion_live,
        byreal_clmm_completion_live,
        manifest_completion_live,
        pumpswap_discovery_live,
        pumpswap_completion_live,
        pumpswap_rolling,
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

fn jupiter_settings() -> Result<Option<source_jupiter_mint::Settings>> {
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

    Ok(Some(source_jupiter_mint::Settings {
        url, api_keys, calls_per_minute_per_key, batch_size,
        timeout: Duration::from_secs(timeout),
    }))
}

fn spawn_sources(
    tasks: &mut JoinSet<Result<()>>,
    settings: discovery_meteora_dlmm::Settings,
    completion: Option<CompletionConfig>,
    graph: Option<graph_liquidity::Settings>,
    jupiter: Option<source_jupiter_mint::Settings>,
    initial_registry: Option<pool_registry::PoolRegistry>,
    persist: Option<persistence::Persistence>,
    source_set: SourceSet,
    strict_lag: bool,
) -> Result<()> {
    let pump_rpc = completion.as_ref().map(|cfg|cfg.rpc.clone());
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
        let (tx, rx) = watch::channel(Arc::new(graph_liquidity::GraphSnapshot::default()));
        tasks.spawn(graph_liquidity::observe(rx.clone()));
        spawn_watch_catalog(tasks, rx, jupiter);
        tasks.spawn(graph_liquidity::run(registry_output.subscribe(), tx, settings));
    } else {
        ensure!(jupiter.is_none(), "Jupiter Search requires a liquidity graph");
    }

    let completion_input = if let Some(config) = completion {
        let (completion_output, _) =
            broadcast::channel::<Arc<core::CompletionResultBatch>>(256);
        let receiver = completion_output.subscribe();
        if let Some(dlmm) = config.dlmm {
            // Verbose DLMM proof only in its original bounded standalone mode.
            if config.damm_v2.is_none() && config.orca.is_none() && dlmm.limit > 0 {
                tasks.spawn(completion_meteora_dlmm::observe(completion_output.subscribe()));
            }
            tasks.spawn(completion_meteora_dlmm::run(registry_output.subscribe(),
                source_done_rx.clone(), config.rpc.clone(), dlmm, completion_output.clone()));
        }
        if let Some(damm_v2) = config.damm_v2 {
            tasks.spawn(completion_meteora_damm_v2::run(registry_output.subscribe(),
                source_done_rx.clone(), config.rpc.clone(), damm_v2, completion_output.clone()));
        }
        if let Some(orca) = config.orca {
            tasks.spawn(completion_orca_whirlpool::run(registry_output.subscribe(),
                source_done_rx.clone(), config.rpc.clone(), orca, completion_output.clone()));
        }
        if let Some(raydium) = config.raydium_amm_v4 {
            tasks.spawn(completion_raydium_amm_v4::run(registry_output.subscribe(),
                source_done_rx.clone(), config.rpc.clone(), raydium, completion_output.clone()));
        }
        if let Some(cpmm) = config.raydium_cpmm {
            tasks.spawn(completion_raydium_cpmm::run(registry_output.subscribe(),
                source_done_rx.clone(), config.rpc.clone(), cpmm, completion_output.clone()));
        }
        if let Some(clmm) = config.raydium_clmm {
            tasks.spawn(completion_raydium_clmm::run(registry_output.subscribe(),
                source_done_rx.clone(), config.rpc.clone(), clmm, completion_output.clone()));
        }
        if let Some(clmm) = config.pancakeswap_clmm {
            tasks.spawn(completion_pancakeswap_clmm::run(registry_output.subscribe(),
                source_done_rx.clone(), config.rpc.clone(), clmm, completion_output.clone()));
        }
        if let Some(pump) = config.pumpswap {
            tasks.spawn(completion_pumpswap::run(registry_output.subscribe(),
                source_done_rx.clone(), config.rpc.clone(), pump, completion_output.clone()));
        }
        if let Some(manifest) = config.manifest {
            tasks.spawn(completion_manifest::run(registry_output.subscribe(),
                source_done_rx.clone(), config.rpc.clone(), manifest, completion_output.clone()));
        }
        if let Some(clmm) = config.byreal_clmm {
            tasks.spawn(completion_byreal_clmm::run(registry_output.subscribe(),
                source_done_rx, config.rpc, clmm, completion_output.clone()));
        }
        // Registry closes only after every completion producer has finished.
        drop(completion_output);
        Some(receiver)
    } else { None };
    tasks.spawn(diagnostic_source::run(input, strict_lag));
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
        tasks.spawn(discovery_raydium::run(core::http_client()?, output.clone(), discovery_raydium::Settings {
            once: settings.once, max_pages: settings.max_pages, rps,
        }));
    }
    if matches!(source_set, SourceSet::Byreal | SourceSet::All) {
        let rpm:u32=env::var("BYREAL_API_RPM")
            .unwrap_or_else(|_| "57".into()).parse().context("invalid BYREAL_API_RPM")?;
        tasks.spawn(discovery_byreal_clmm::run(core::http_client()?,output.clone(),discovery_byreal_clmm::Settings {
            once: settings.once,max_pages:settings.max_pages,rpm,
        }));
    }
    if matches!(source_set, SourceSet::PumpSwap) {
        let rpc = pump_rpc.context("PumpSwap rolling requires the configured shared RPC transport")?;
        let settings = discovery_pumpswap::Settings::from_env(settings.once, settings.max_pages)?;
        tasks.spawn(discovery_pumpswap::run(rpc, output.clone(), settings));
    }
    if matches!(source_set, SourceSet::Manifest | SourceSet::All) {
        tasks.spawn(discovery_manifest::run(core::http_client()?,output.clone(),
            discovery_manifest::Settings {once:settings.once}));
    }
    if matches!(source_set, SourceSet::PancakeSwap | SourceSet::All) {
        let rpm: u32 = env::var("PANCAKESWAP_API_RPM")
            .unwrap_or_else(|_| "57".into()).parse().context("invalid PANCAKESWAP_API_RPM")?;
        tasks.spawn(discovery_pancakeswap_clmm::run(core::http_client()?, output.clone(), discovery_pancakeswap_clmm::Settings {
            once: settings.once, max_pages: settings.max_pages, rpm,
        }));
    }
    if matches!(source_set, SourceSet::Orca | SourceSet::All) {
        let rps: u32 = env::var("ORCA_API_RPS")
            .unwrap_or_else(|_| "10".into()).parse().context("invalid ORCA_API_RPS")?;
        tasks.spawn(discovery_orca_whirlpool::run(core::http_client()?, output.clone(), discovery_orca_whirlpool::Settings {
            once: settings.once, max_pages: settings.max_pages, rps,
        }));
    }
    if matches!(source_set, SourceSet::DammV2 | SourceSet::DlmmDammV2 | SourceSet::All) {
        tasks.spawn(discovery_meteora_damm_v2::run(core::http_client()?, output.clone(),
            discovery_meteora_damm_v2::Settings { once: settings.once, max_pages: settings.max_pages }));
    }
    if matches!(source_set, SourceSet::Dlmm | SourceSet::DlmmDammV2 | SourceSet::All) {
        tasks.spawn(discovery_meteora_dlmm::run(core::http_client()?, output, settings));
    }
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
    tasks.spawn(diagnostic_source::run(input, false));
    // Jupiter must outlive a finite graph restore. Its own task retains the
    // watch sender without requiring the catalog producer to stay running.
    tasks.spawn(async move {
        let _mint_sender = keepalive;
        source_jupiter_mint::run(reqwest::Client::new(), settings, mints, output).await
    });
}

pub async fn run() -> Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(error) if error.not_found() => {}
        Err(error) => return Err(error).context("load .env"),
    }

    let cli = settings()?;
    if cli.pumpswap_discovery_live {
        let (rpc, rpc_task) = rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
        let (output, input) = broadcast::channel::<Arc<core::DataBatch>>(256);
        let observer = tokio::spawn(diagnostic_source::run(input, true));
        let settings = discovery_pumpswap::Settings::from_env(true, cli.meteora.max_pages)?;
        discovery_pumpswap::run(rpc, output, settings).await?;
        observer.await??;
        rpc_task.await??;
        return Ok(());
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
        ensure!(graph_settings.min_tvl_usd.is_none(),
            "--jupiter-only restore cannot use GRAPH_MIN_TVL_USD without fresh TVL");
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
        println!("orchestration jupiter-graph-live restored_pools={restored} source=persistence_restore");
        // Jupiter Search runs continuously against the restored graph's mint set.
        // The catalog producer retains the Jupiter mint sender during streaming.
        while let Some(result) = tasks.join_next().await { result??; }
        return Ok(());
    }
    if cli.persistence_v2_live {
        let database=persistence::Persistence::connect().await?;
        let registry=pool_registry::PoolRegistry::default();
        let restored=registry.restore(database.restore().await?)?;
        println!("orchestration persistence-v2-live bootstrap_pools={restored} sources=All rpc_calls=0");
        let mut tasks=JoinSet::new();
        spawn_sources(&mut tasks,cli.meteora,None,None,None,
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
        let graph_settings = graph_liquidity::Settings::from_env()?;
        ensure!(graph_settings.min_tvl_usd.is_none(),
            "restore-live cannot use GRAPH_MIN_TVL_USD without a fresh TVL source");
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
        println!("persistence restore-live complete restored_pools={restored} rpc_calls=0");
        return Ok(());
    }
    if cli.completion_live || cli.damm_v2_completion_live
        || cli.orca_completion_live || cli.raydium_amm_v4_completion_live
        || cli.raydium_cpmm_completion_live || cli.raydium_clmm_completion_live
        || cli.pancakeswap_clmm_completion_live || cli.byreal_clmm_completion_live
        || cli.manifest_completion_live || cli.pumpswap_completion_live || cli.pumpswap_rolling || cli.graph_live || cli.persistence_write_live || cli.stability_live {
        let (rpc, rpc_task) = rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
        let mut tasks = JoinSet::new();
        // Protocol completion workers share the RPC transport configuration;
        // only instantiate a protocol's settings if that worker actually runs.
        // Zero allows a whole-universe scan in finite/continuous modes.
        let completion_limit = cli.completion_limit.unwrap_or(
            if cli.stability_live {0}
            else if cli.graph_live || cli.persistence_write_live {5000}
            else if cli.orca_completion_live {200}
            else if cli.pancakeswap_clmm_completion_live || cli.byreal_clmm_completion_live
                || cli.manifest_completion_live || cli.pumpswap_completion_live || cli.pumpswap_rolling {0}
            else {100});

        let graph = if cli.graph_live || cli.persistence_write_live
            || cli.stability_live || cli.damm_v2_completion_live
            || cli.orca_completion_live || cli.raydium_amm_v4_completion_live
            || cli.raydium_cpmm_completion_live || cli.raydium_clmm_completion_live
            || cli.pancakeswap_clmm_completion_live || cli.byreal_clmm_completion_live
            || cli.manifest_completion_live || cli.pumpswap_completion_live || cli.pumpswap_rolling {
            let graph_settings=graph_liquidity::Settings::from_env()?;
            if cli.manifest_completion_live {
                ensure!(graph_settings.min_tvl_usd.is_none(),
                    "--manifest-completion-live requires GRAPH_MIN_TVL_USD unset (Manifest TVL is None)");
            }
            if cli.pumpswap_completion_live || cli.pumpswap_rolling {
                ensure!(graph_settings.min_tvl_usd.is_none(),
                    "PumpSwap completion requires GRAPH_MIN_TVL_USD unset until reserve/TVL worker exists");
            }
            Some(graph_settings)
        } else {None};
        let (registry, writer) = if cli.persistence_write_live
            || cli.stability_live || cli.damm_v2_completion_live
            || cli.orca_completion_live || cli.raydium_amm_v4_completion_live
            || cli.raydium_cpmm_completion_live || cli.raydium_clmm_completion_live
            || cli.pancakeswap_clmm_completion_live || cli.byreal_clmm_completion_live
            || cli.manifest_completion_live || cli.pumpswap_completion_live || cli.pumpswap_rolling {
            let db = persistence::Persistence::connect().await?;
            let registry = pool_registry::PoolRegistry::default();
            let restored = registry.restore(db.restore().await?)?;
            println!("orchestration initial persistence restored_pools={restored}");
            (Some(registry), Some(db))
        } else {(None,None)};
        if cli.stability_live {
            println!("orchestration stability-live continuous_source=true pages={:?} completion_limit={} (0=unbounded) postgres=true graph=true",
                cli.meteora.max_pages, completion_limit);
        }
        let source_set = if cli.pumpswap_completion_live || cli.pumpswap_rolling {
            SourceSet::PumpSwap
        } else if cli.manifest_completion_live {
            SourceSet::Manifest
        } else if cli.byreal_clmm_completion_live {
            SourceSet::Byreal
        } else if cli.pancakeswap_clmm_completion_live {
            SourceSet::PancakeSwap
        } else if cli.orca_completion_live {
            SourceSet::Orca
        } else if cli.raydium_amm_v4_completion_live || cli.raydium_cpmm_completion_live
            || cli.raydium_clmm_completion_live {
            SourceSet::Raydium
        } else if cli.damm_v2_completion_live {
            if cli.completion_live { SourceSet::DlmmDammV2 } else { SourceSet::DammV2 }
        } else { SourceSet::Dlmm };
        let damm_v2 = if cli.damm_v2_completion_live {
            let mut settings = completion_meteora_damm_v2::Settings::from_env()?;
            settings.limit = completion_limit;
            Some(settings)
        } else { None };
        let dlmm = if !cli.orca_completion_live && !cli.raydium_amm_v4_completion_live
            && !cli.raydium_cpmm_completion_live && !cli.raydium_clmm_completion_live
            && !cli.pancakeswap_clmm_completion_live && !cli.byreal_clmm_completion_live
            && !cli.manifest_completion_live && !cli.pumpswap_completion_live && !cli.pumpswap_rolling
            && (!cli.damm_v2_completion_live || cli.completion_live) {
            let mut settings = completion_meteora_dlmm::Settings::from_env()?;
            settings.limit = completion_limit;
            Some(settings)
        } else {None};
        let orca = if cli.orca_completion_live {
            let mut settings = completion_orca_whirlpool::Settings::from_env()?;
            settings.limit = completion_limit;
            Some(settings)
        } else {None};
        let raydium_amm_v4 = if cli.raydium_amm_v4_completion_live {
            let mut settings = completion_raydium_amm_v4::Settings::from_env()?;
            settings.limit = completion_limit;
            Some(settings)
        } else { None };
        let raydium_cpmm = if cli.raydium_cpmm_completion_live {
            let mut settings = completion_raydium_cpmm::Settings::from_env()?;
            settings.limit = completion_limit;
            Some(settings)
        } else { None };
        let raydium_clmm = if cli.raydium_clmm_completion_live {
            let mut settings = completion_raydium_clmm::Settings::from_env()?;
            settings.limit = completion_limit;
            Some(settings)
        } else { None };
        let pancakeswap_clmm = if cli.pancakeswap_clmm_completion_live {
            let mut settings = completion_pancakeswap_clmm::Settings::from_env()?;
            settings.limit = completion_limit;
            Some(settings)
        } else { None };
        let byreal_clmm = if cli.byreal_clmm_completion_live {
            let mut settings=completion_byreal_clmm::Settings::from_env()?;
            settings.limit=completion_limit;
            Some(settings)
        } else {None};
        let pumpswap = if cli.pumpswap_completion_live || cli.pumpswap_rolling {
            let mut settings = completion_pumpswap::Settings::from_env()?;
            settings.limit = completion_limit;
            Some(settings)
        } else {None};
        let manifest = if cli.manifest_completion_live {
            let mut settings=completion_manifest::Settings::from_env()?;
            settings.limit=completion_limit;
            Some(settings)
        } else {None};
        let jupiter = if cli.stability_live { jupiter_settings()? } else { None };
        spawn_sources(&mut tasks, cli.meteora,
            Some(CompletionConfig {rpc:rpc.clone(), dlmm, damm_v2, orca, raydium_amm_v4, raydium_cpmm, raydium_clmm, pancakeswap_clmm, byreal_clmm, manifest, pumpswap}),
            graph, jupiter, registry, writer, source_set, false)?;
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
    let jupiter = if cli.meteora.once { None } else { jupiter_settings()? };
    let (registry, graph) = if jupiter.is_some() {
        // Reuse validated completion state; new REST discoveries still require
        // their own completion before they can enter the graph watchlist.
        let database = persistence::Persistence::connect().await?;
        let registry = pool_registry::PoolRegistry::default();
        let restored = registry.restore(database.restore().await?)?;
        println!("orchestration Jupiter graph bootstrap restored_pools={restored}");
        (Some(registry), Some(graph_liquidity::Settings::from_env()?))
    } else { (None, None) };

    let mut tasks = JoinSet::new();
    let source_set = cli.rest_source_only.unwrap_or(if cli.rest_discovery_live || !cli.meteora.once {
        SourceSet::All
    } else { SourceSet::Dlmm });
    let fail_on_lag = cli.rest_discovery_live || cli.rest_source_only.is_some()
        || env::var("REST_FAIL_ON_LAG").ok().as_deref() == Some("true");
    spawn_sources(&mut tasks, cli.meteora, None, graph, jupiter, registry, None,
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
