//! Resource provisioning, channel connections, and worker lifecycle only.

use std::{collections::HashSet, env, fs, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, watch}, task::JoinSet};

use crate::{core, workers::{execution_completion, jupiter_search, meteora_dlmm, pool_registry, rpc_transport, source_diagnostic}};

struct Cli {
    meteora: meteora_dlmm::Settings,
    jupiter_only: bool,
    rpc_live: bool,
    completion_live: bool,
}

fn settings() -> Result<Cli> {
    let mut args = env::args().skip(1);
    let mut once = false;
    let mut max_pages = None;
    let mut jupiter_only = false;
    let mut rpc_live = false;
    let mut completion_live = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--once" => once = true,
            "--jupiter-only" => jupiter_only = true,
            "--rpc-live" => rpc_live = true,
            "--completion-live" => { completion_live = true; once = true; },
            "--pages" => {
                let count: u32 = args.next()
                    .context("--pages requires a positive number")?
                    .parse().context("invalid --pages value")?;
                ensure!(count > 0, "--pages must be positive");
                max_pages = Some(count);
            }
            _ => bail!("unknown argument {arg}; use --once, --pages N, --jupiter-only or --rpc-live"),
        }
    }
    ensure!(!(rpc_live && (jupiter_only || once || max_pages.is_some())),
        "--rpc-live is an exclusive RPC transport check");
    ensure!(!(completion_live && jupiter_only),
        "--completion-live cannot be combined with --jupiter-only");
    ensure!(
        !(jupiter_only && (once || max_pages.is_some())),
        "--jupiter-only cannot be combined with --once or --pages"
    );
    Ok(Cli {
        meteora: meteora_dlmm::Settings { once, max_pages },
        jupiter_only,
        rpc_live,
        completion_live,
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

fn spawn_meteora(
    tasks: &mut JoinSet<Result<()>>,
    settings: meteora_dlmm::Settings,
    completion: Option<(rpc_transport::RpcHandle, execution_completion::Settings)>,
) -> Result<()> {
    let (output, input) = broadcast::channel::<Arc<core::DataBatch>>(32);
    let registry_input = output.subscribe();
    let (registry_output, registry_observer) =
        broadcast::channel::<Arc<pool_registry::PoolUpdateBatch>>(32);
    let registry = pool_registry::PoolRegistry::default();

    if let Some((rpc, config)) = completion {
        let candidate_input = registry_output.subscribe();
        let (completion_output, completion_observer) = broadcast::channel(4);
        tasks.spawn(execution_completion::observe(completion_observer));
        tasks.spawn(execution_completion::run(candidate_input, rpc, config, completion_output));
    }
    tasks.spawn(source_diagnostic::run(input));
    tasks.spawn(pool_registry::observe_updates(registry_observer));
    tasks.spawn(pool_registry::run(registry_input, registry, registry_output));
    tasks.spawn(meteora_dlmm::run(core::http_client()?, output, settings));
    Ok(())
}

fn spawn_jupiter(
    tasks: &mut JoinSet<Result<()>>,
    settings: jupiter_search::Settings,
    mints: Arc<[Pubkey]>,
) -> watch::Sender<Arc<[Pubkey]>> {
    let (output, input) = broadcast::channel::<Arc<core::DataBatch>>(32);
    tasks.spawn(source_diagnostic::run(input));

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
    if cli.completion_live {
        let (rpc, rpc_task) = rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
        let mut tasks = JoinSet::new();
        spawn_meteora(&mut tasks, cli.meteora,
            Some((rpc.clone(), execution_completion::Settings::from_env()?)))?;
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
        spawn_meteora(&mut tasks, cli.meteora, None)?;
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
