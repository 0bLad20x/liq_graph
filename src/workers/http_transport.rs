//! Shared asynchronous HTTP transport with an in-memory public-proxy working set.
//! One boundary: source discovery, transport verification, route selection, raw HTTP I/O.
//! No DEX business decoding beyond a standalone health-test response validator.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    io::ErrorKind,
    net::Ipv4Addr,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex as StdMutex, atomic::{AtomicUsize, Ordering}},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream};
use reqwest::{Client, Method, Proxy, StatusCode, Url, header::HeaderMap};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, Semaphore, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior, sleep_until, timeout},
};

const DEX_PROBE: &str = "https://api.dexscreener.com/token-pairs/v1/solana/DEW9dSN6QpWyNthphCpMmAbZP1Q4cEKR9xQXAri98WDP";
const MAX_SOURCE_BYTES: usize = 16 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const CACHE_VERSION: u32 = 1;
const CACHE_MAX_AGE_MS: u64 = 7 * 24 * 60 * 60 * 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutePolicy {
    ProxyOnly,
    // Public route-policy option; no direct-request consumer is wired yet.
    #[allow(dead_code)]
    DirectOnly,
}

#[derive(Clone, Debug)]
pub struct HttpRequest {
    pub method: Method,
    pub url: Url,
    pub headers: HeaderMap,
    pub body: Option<Bytes>,
    pub timeout: Duration,
    pub policy: RoutePolicy,
}

impl HttpRequest {
    pub fn get(url: &str, policy: RoutePolicy) -> Result<Self> {
        Ok(Self {
            method: Method::GET,
            url: Url::parse(url)?,
            headers: HeaderMap::new(),
            body: None,
            timeout: Duration::from_secs(15),
            policy,
        })
    }
}

#[derive(Debug)]
pub struct HttpResponse {
    pub status: StatusCode,
    pub body: Bytes,
    pub elapsed_ms: u64,
    pub route_id: String,
}

#[derive(Clone, Debug)]
pub struct RouteSnapshot {
    pub id: String,
    pub protocol: String,
    pub sources: Vec<String>,
    pub latency_ms: u64,
    pub verified_age_ms: u64,
}

#[derive(Clone)]
pub struct HttpHandle {
    calls: mpsc::Sender<Call>,
    routes: watch::Receiver<Arc<Vec<RouteSnapshot>>>,
    cycles: watch::Receiver<u64>,
}

impl HttpHandle {
    pub async fn call(&self, request: HttpRequest) -> Result<HttpResponse> {
        let (reply, result) = oneshot::channel();
        self.calls.send(Call { request, reply, queued_at: Instant::now() })
            .await.context("HTTP transport stopped")?;
        result.await.context("HTTP transport dropped request")?
    }
    pub fn working_set(&self) -> Arc<Vec<RouteSnapshot>> {
        Arc::clone(&*self.routes.borrow())
    }
    pub fn subscribe_routes(&self) -> watch::Receiver<Arc<Vec<RouteSnapshot>>> {
        self.routes.clone()
    }
    pub fn subscribe_cycles(&self) -> watch::Receiver<u64> {
        self.cycles.clone()
    }
}

struct Call {
    request: HttpRequest,
    reply: oneshot::Sender<Result<HttpResponse>>,
    queued_at: Instant,
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub refresh: Duration,
    pub tcp_timeout: Duration,
    pub handshake_timeout: Duration,
    pub probe_timeout: Duration,
    pub healthy_for: Duration,
    pub retry_failure_after: Duration,
    pub tcp_parallel: usize,
    pub handshake_parallel: usize,
    pub probe_parallel: usize,
    pub request_parallel: usize,
    pub queue_capacity: usize,
    pub probe_rps: u32,
    pub request_rps_per_host: u32,
    pub probe_url: Url,
    pub verify_url: Url,
    pub extra_txt_urls: Vec<Url>,
    pub list_file: Option<String>,
    pub public_discovery: bool,
    pub host_rps_overrides: HashMap<String, u32>,
    /// Budget per configured proxy IP and destination host; not necessarily egress IP.
    pub request_rps_per_route: u32,
    pub retry_attempts: usize,
    pub cache_path: PathBuf,
}

fn from_env<T: FromStr>(name: &str, default: T) -> Result<T>
where T::Err: std::fmt::Display {
    match env::var(name) {
        Ok(v) => v.parse::<T>().map_err(|e| anyhow!("invalid {name}: {e}")),
        Err(_) => Ok(default),
    }
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        let refresh = from_env("HTTP_PROXY_REFRESH_SECONDS", 900_u64)?;
        let tcp = from_env("HTTP_PROXY_TCP_TIMEOUT_MS", 500_u64)?;
        let handshake = from_env("HTTP_PROXY_HANDSHAKE_TIMEOUT_MS", 1500_u64)?;
        let probe = from_env("HTTP_PROXY_PROBE_TIMEOUT_MS", 4000_u64)?;
        let healthy = from_env("HTTP_PROXY_HEALTHY_SECONDS", 1200_u64)?;
        let retry = from_env("HTTP_PROXY_RETRY_FAILED_SECONDS", 3600_u64)?;
        let tcp_parallel = from_env("HTTP_PROXY_TCP_PARALLEL", 500_usize)?;
        let handshake_parallel = from_env("HTTP_PROXY_HANDSHAKE_PARALLEL", 200_usize)?;
        let probe_parallel = from_env("HTTP_PROXY_PROBE_PARALLEL", 100_usize)?;
        let request_parallel = from_env("HTTP_TRANSPORT_MAX_IN_FLIGHT", 128_usize)?;
        let queue_capacity = from_env("HTTP_TRANSPORT_QUEUE_CAPACITY", 512_usize)?;
        let probe_rps = from_env("HTTP_PROXY_PROBE_RPS", 20_u32)?;
        let request_rps_per_host = from_env("HTTP_TRANSPORT_HOST_RPS", 5_u32)?;
        let route_rps_legacy = from_env("HTTP_TRANSPORT_ROUTE_RPS", 5_u32)?;
        // Address-based cap: two different ports at the same proxy address
        // must not each receive their own independent request budget.
        let request_rps_per_route = from_env("HTTP_TRANSPORT_PROXY_IP_RPS", route_rps_legacy)?;
        let retry_attempts = from_env("HTTP_TRANSPORT_RETRY_ATTEMPTS", 3_usize)?;
        for value in [refresh, tcp, handshake, probe, healthy, retry] {
            ensure!(value > 0, "HTTP transport intervals must be positive");
        }
        for value in [tcp_parallel, handshake_parallel, probe_parallel, request_parallel, queue_capacity] {
            ensure!((1..=4096).contains(&value), "HTTP transport concurrency must be 1..=4096");
        }
        ensure!(probe_rps > 0 && request_rps_per_route > 0,
            "HTTP transport probe and route RPS must be positive");
        ensure!((1..=5).contains(&retry_attempts), "HTTP transport retries must be 1..=5");
        let probe_url = Url::parse(&env::var("HTTP_PROXY_PROBE_URL")
            .unwrap_or_else(|_| DEX_PROBE.to_owned()))?;
        // One real target probe, no unrelated Google connectivity prerequisite.
        let verify_url = Url::parse(&env::var("HTTP_PROXY_VERIFY_URL")
            .unwrap_or_else(|_| probe_url.to_string()))?;
        ensure!([&probe_url, &verify_url].iter().all(|u| u.scheme() == "https" && u.host_str().is_some()),
            "probe and verify URLs must be HTTPS");
        let list_file = env::var("HTTP_PROXY_LIST_FILE").ok().filter(|s| !s.trim().is_empty());
        let public_discovery = match env::var("HTTP_PROXY_PUBLIC_DISCOVERY").as_deref() {
            Ok("false" | "0") => false,
            Ok("true" | "1") | Err(_) => true,
            _ => bail!("invalid HTTP_PROXY_PUBLIC_DISCOVERY"),
        };
        let mut host_rps_overrides = HashMap::new();
        for rule in env::var("HTTP_TRANSPORT_HOST_RPS_OVERRIDES").unwrap_or_default()
            .split(',').map(str::trim).filter(|s| !s.is_empty())
        {
            let (host, raw) = rule.rsplit_once(':')
                .context("host RPS override must be host:rps")?;
            let rps = raw.parse::<u32>().context("invalid host RPS override")?;
            ensure!(!host.is_empty() && !host.contains('/'),
                "invalid HTTP transport host limit");
            ensure!(host_rps_overrides.insert(host.to_ascii_lowercase(), rps).is_none(),
                "duplicate HTTP transport host limit");
        }
        let extra_txt_urls = env::var("HTTP_PROXY_EXTRA_TXT_URLS").unwrap_or_default()
            .split(',').map(str::trim).filter(|s| !s.is_empty())
            .map(Url::parse).collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(extra_txt_urls.iter().all(|v| v.scheme() == "https"), "extra source URLs must be HTTPS");
        Ok(Self {
            refresh: Duration::from_secs(refresh),
            tcp_timeout: Duration::from_millis(tcp),
            handshake_timeout: Duration::from_millis(handshake),
            probe_timeout: Duration::from_millis(probe),
            healthy_for: Duration::from_secs(healthy),
            retry_failure_after: Duration::from_secs(retry),
            tcp_parallel, handshake_parallel, probe_parallel, request_parallel, queue_capacity,
            probe_rps, request_rps_per_host, probe_url, verify_url, extra_txt_urls,
            list_file, public_discovery, host_rps_overrides, request_rps_per_route,
            retry_attempts,
            cache_path: PathBuf::from(env::var("HTTP_PROXY_CACHE_FILE")
                .unwrap_or_else(|_| "target/http_proxy_working_set.json".into())),
        })
    }
}

#[derive(Clone)]
struct Candidate {
    id: String,
    protocol: String,
    host: String,
    port: u16,
    sources: HashSet<String>,
}

struct Route {
    candidate: Candidate,
    client: Option<Client>,
    verified: Option<Instant>,
    latency_ms: u64,
    next_check: Instant,
    last_verified_unix_ms: u64,
}


#[derive(Serialize, Deserialize)]
struct CachedRoute {
    id: String,
    sources: Vec<String>,
    latency_ms: u64,
    last_verified_unix_ms: u64,
}

#[derive(Serialize, Deserialize)]
struct CachedWorkingSet {
    version: u32,
    verify_url: String,
    saved_at_unix_ms: u64,
    routes: Vec<CachedRoute>,
}

fn unix_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64).unwrap_or(0)
}

// Cache entries are candidates, not verified active routes after a restart.
fn load_cached_routes(settings: &Settings) -> HashMap<String, Route> {
    let bytes = match std::fs::read(&settings.cache_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return HashMap::new(),
        Err(error) => {
            crate::log_event!(Warn,"http_transport cache_read_error path={} error={error}",
                settings.cache_path.display());
            return HashMap::new();
        }
    };
    let cache: CachedWorkingSet = match serde_json::from_slice(&bytes) {
        Ok(cache) => cache,
        Err(error) => {
            crate::log_event!(Warn,"http_transport cache_decode_error path={} error={error}",
                settings.cache_path.display());
            return HashMap::new();
        }
    };
    if cache.version != CACHE_VERSION {
        crate::log_event!(Info,"http_transport cache_version_mismatch version={}", cache.version);
        return HashMap::new();
    }
    if cache.verify_url != settings.verify_url.as_str() {
        crate::log_event!(Info,"http_transport cache_verify_target_changed revalidating_all=true");
    }
    let now_ms = unix_ms();
    let cache_age_s = now_ms.saturating_sub(cache.saved_at_unix_ms) / 1000;
    let mut routes = HashMap::new();
    for entry in cache.routes {
        if entry.last_verified_unix_ms == 0 ||
            now_ms.saturating_sub(entry.last_verified_unix_ms) > CACHE_MAX_AGE_MS {
            continue;
        }
        let Ok(url) = Url::parse(&entry.id) else { continue };
        if !url.username().is_empty() || url.password().is_some() { continue; }
        let (Some(host), Some(port)) = (url.host_str(), url.port()) else { continue };
        let mut parsed = HashMap::new();
        add_candidate(&mut parsed, url.scheme(), host, port, "cached");
        for (id, mut candidate) in parsed {
            candidate.sources.extend(entry.sources.iter().cloned());
            routes.insert(id, Route {
                candidate, client: None, verified: None,
                latency_ms: entry.latency_ms,
                next_check: Instant::now(),
                last_verified_unix_ms: entry.last_verified_unix_ms,
            });
        }
    }
    crate::log_event!(Info,"http_transport cache_loaded={} age_s={} path={}", routes.len(),
        cache_age_s, settings.cache_path.display());
    routes
}

// Atomically replace the cache; never hold inventory locks across file I/O.
// Preserve last-known-good candidates even if one refresh or source temporarily fails.
async fn persist_cached_routes(shared: &Shared) {
    let _write = shared.cache_write.lock().await;
    let now = unix_ms();
    let mut routes = {
        let state = shared.inventory.lock().await;
        state.values().filter(|r| r.last_verified_unix_ms != 0 &&
            now.saturating_sub(r.last_verified_unix_ms) <= CACHE_MAX_AGE_MS)
            .map(|r| {
                let mut sources = r.candidate.sources.iter().cloned().collect::<Vec<_>>();
                sources.sort();
                CachedRoute {
                    id: r.candidate.id.clone(), sources,
                    latency_ms: r.latency_ms,
                    last_verified_unix_ms: r.last_verified_unix_ms,
                }
            }).collect::<Vec<_>>()
    };
    routes.sort_by(|a,b| a.id.cmp(&b.id));
    let file = CachedWorkingSet {
        version: CACHE_VERSION, verify_url: shared.settings.verify_url.to_string(),
        saved_at_unix_ms: now, routes,
    };
    let result = (|| -> Result<()> {
        let path = &shared.settings.cache_path;
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let temporary = path.with_extension("json.tmp");
        std::fs::write(&temporary, serde_json::to_vec(&file)?)?;
        std::fs::rename(&temporary, path)?;
        Ok(())
    })();
    if let Err(error) = result {
        crate::log_event!(Warn,"http_transport cache_write_error path={} error={error:#}",
            shared.settings.cache_path.display());
    }
}

#[derive(Default)]
struct TransportStats {
    submitted: u64, completed: u64, succeeded: u64,
    attempts: u64, failovers: u64, http_429: u64, http_403: u64,
    http_5xx: u64, transport_errors: u64, other_errors: u64,
    routes_used: HashSet<String>,
    latency_ms: VecDeque<u64>, queue_ms: VecDeque<u64>,
}

fn percentile(values: &VecDeque<u64>, p: usize) -> u64 {
    if values.is_empty() { return 0; }
    let mut sorted = values.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    sorted[((sorted.len()-1)*p)/100]
}

fn sample(values: &mut VecDeque<u64>, ms: u64) {
    if values.len() >= 4096 { values.pop_front(); }
    values.push_back(ms);
}

fn report_stats(shared: &Shared, throughput_rps: f64) {
    let stats = shared.stats.lock().expect("HTTP stats lock poisoned");
    crate::log_event!(Warn,
        "http_transport stats submitted={} completed={} ok={} failed={} attempts={} failovers={} http_429={} http_403={} http_5xx={} transport_errors={} other_errors={} distinct_routes={} working_routes={} throughput_rps={:.2} latency_p50_ms={} latency_p95_ms={} queue_p95_ms={}",
        stats.submitted, stats.completed, stats.succeeded,
        stats.completed.saturating_sub(stats.succeeded), stats.attempts, stats.failovers,
        stats.http_429, stats.http_403, stats.http_5xx, stats.transport_errors,
        stats.other_errors, stats.routes_used.len(),
        shared.snapshots.borrow().len(), throughput_rps,
        percentile(&stats.latency_ms, 50), percentile(&stats.latency_ms, 95),
        percentile(&stats.queue_ms, 95)
    );
}

struct Shared {
    settings: Settings,
    inventory: Mutex<HashMap<String, Route>>,
    snapshots: watch::Sender<Arc<Vec<RouteSnapshot>>>,
    cycles: watch::Sender<u64>,
    selection: AtomicUsize,
    host_gates: Mutex<HashMap<String, Instant>>,
    host_backoffs: Mutex<HashMap<String, Instant>>,
    route_gates: Mutex<HashMap<(String, String), Instant>>,
    host_route_cooldowns: Mutex<HashMap<(String, String), Instant>>,
    stats: StdMutex<TransportStats>,
    direct_client: Client,
    cache_write: Mutex<()>,
}

pub fn spawn(settings: Settings) -> Result<(HttpHandle, JoinHandle<Result<()>>)> {
    crate::log_event!(Info,
        "http_transport config refresh_s={} verify_url={} public_discovery={} configured_file={} probe_rps={} host_default_rps={} route_rps={} host_overrides={} in_flight={} queue={} retries={}",
        settings.refresh.as_secs(), settings.verify_url,
        settings.public_discovery, settings.list_file.is_some(), settings.probe_rps,
        settings.request_rps_per_host, settings.request_rps_per_route,
        settings.host_rps_overrides.len(), settings.request_parallel,
        settings.queue_capacity, settings.retry_attempts
    );
    let (calls, receiver) = mpsc::channel(settings.queue_capacity);
    let (snapshots, route_reader) = watch::channel(Arc::new(Vec::new()));
    let (cycles, cycle_reader) = watch::channel(0_u64);
    let direct_client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()?;
    let cached_routes = load_cached_routes(&settings);
    let shared = Arc::new(Shared {
        settings: settings.clone(),
        inventory: Mutex::new(cached_routes),
        snapshots, cycles,
        selection: AtomicUsize::new(0),
        host_gates: Mutex::new(HashMap::new()),
        host_backoffs: Mutex::new(HashMap::new()),
        route_gates: Mutex::new(HashMap::new()),
        host_route_cooldowns: Mutex::new(HashMap::new()),
        stats: StdMutex::new(TransportStats::default()),
        direct_client,
        cache_write: Mutex::new(()),
    });
    let task = tokio::spawn(run(shared, receiver));
    Ok((HttpHandle { calls, routes: route_reader, cycles: cycle_reader }, task))
}

async fn run(shared: Arc<Shared>, mut receiver: mpsc::Receiver<Call>) -> Result<()> {
    let has_cached = !shared.inventory.lock().await.is_empty();
    let discover = if has_cached || shared.settings.public_discovery
        || shared.settings.list_file.is_some() || !shared.settings.extra_txt_urls.is_empty()
    {
        let shared = shared.clone();
        Some(tokio::spawn(async move { discovery_loop(shared).await }))
    } else { None };
    let capacity = Arc::new(Semaphore::new(shared.settings.request_parallel));
    let reporter = {
        let shared = shared.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            tick.tick().await;
            let mut previous = 0_u64;
            loop {
                tick.tick().await;
                let completed = shared.stats.lock().expect("HTTP stats lock poisoned").completed;
                report_stats(&shared, (completed.saturating_sub(previous)) as f64 / 10.0);
                previous = completed;
            }
        })
    };
    while let Some(call) = receiver.recv().await {
        shared.stats.lock().expect("HTTP stats lock poisoned").submitted += 1;
        let permit = capacity.clone().acquire_owned().await
            .context("HTTP transport semaphore closed")?;
        let shared = shared.clone();
        tokio::spawn(async move {
            let queued_ms = call.queued_at.elapsed().as_millis() as u64;
            let started = Instant::now();
            let result = execute(&shared, call.request).await;
            {
                let mut stats = shared.stats.lock().expect("HTTP stats lock poisoned");
                stats.completed += 1;
                if result.as_ref().is_ok_and(|v| v.status.is_success()) { stats.succeeded += 1; }
                sample(&mut stats.latency_ms, started.elapsed().as_millis() as u64);
                sample(&mut stats.queue_ms, queued_ms);
            }
            let _ = call.reply.send(result);
            drop(permit);
        });
    }
    if let Some(discover) = discover {
        discover.abort();
        let _ = discover.await;
    }
    reporter.abort();
    report_stats(&shared, 0.0);
    Ok(())
}

fn allowed_ip(host: &str) -> bool {
    let Ok(ip) = host.parse::<Ipv4Addr>() else { return false };
    let o = ip.octets();
    !(ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_multicast()
      || ip.is_unspecified() || ip.is_broadcast() || ip.is_documentation()
      || o[0] >= 224 || o[0] == 0 || o[0] == 127
      || (o[0] == 100 && (64..=127).contains(&o[1]))
      || (o[0] == 198 && (18..=19).contains(&o[1]))
      || (o[0] == 192 && o[1] == 0 && o[2] == 0))
}

fn add_candidate(
    map: &mut HashMap<String, Candidate>, protocol: &str, host: &str,
    port: u16, source: &str,
) {
    if port == 0 || !allowed_ip(host) { return; }
    // Public HTTPS-labelled lists typically mean "HTTPS targets via HTTP CONNECT",
    // not TLS between client and proxy. Preserve that proven transport interpretation.
    let transport = match protocol.to_ascii_lowercase().as_str() {
        "http" | "https" => "http",
        "socks4" | "socks4a" => "socks4a",
        "socks5" | "socks5h" => "socks5h",
        _ => return,
    };
    let id = format!("{transport}://{host}:{port}");
    let entry = map.entry(id.clone()).or_insert_with(|| Candidate {
        id, protocol: transport.to_owned(), host: host.to_owned(), port,
        sources: HashSet::new(),
    });
    entry.sources.insert(source.to_owned());
}

fn txt_entries(map: &mut HashMap<String, Candidate>, source: &str, text: &str, fallback: &str) {
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') { continue; }
        let value = if line.contains("://") { line.to_owned() } else { format!("{fallback}://{line}") };
        if let Ok(uri) = Url::parse(&value) {
            if uri.username().is_empty() && uri.password().is_none() {
                if let (Some(host), Some(port)) = (uri.host_str(), uri.port()) {
                    add_candidate(map, uri.scheme(), host, port, source);
                }
            }
        }
    }
}

fn json_entries(map: &mut HashMap<String, Candidate>, source: &str, text: &str) -> Result<()> {
    let rows: Vec<Value> = serde_json::from_str(text).context("invalid monosans JSON")?;
    for row in rows {
        if row.get("username").is_some_and(|v| !v.is_null())
            || row.get("password").is_some_and(|v| !v.is_null()) { continue; }
        if let (Some(protocol), Some(host), Some(port)) = (
            row.get("protocol").and_then(Value::as_str),
            row.get("host").and_then(Value::as_str),
            row.get("port").and_then(Value::as_u64).and_then(|p| u16::try_from(p).ok()),
        ) {
            add_candidate(map, protocol, host, port, source);
        }
    }
    Ok(())
}

fn csv_entries(map: &mut HashMap<String, Candidate>, source: &str, text: &str) -> Result<()> {
    let mut lines = text.lines();
    let header = lines.next().context("empty hproxy CSV")?;
    let columns: Vec<&str> = header.split(',').collect();
    let col = |name| -> Result<usize> {
        columns.iter().position(|s| s.trim() == name).ok_or_else(|| anyhow!("CSV missing column {name}"))
    };
    let ip = col("ip")?;
    let port = col("port")?;
    let protocols = col("protocols")?;
    let alive = columns.iter().position(|s| s.trim() == "alive");
    for row in lines {
        let fields: Vec<&str> = row.split(',').collect();
        if let Some(a) = alive {
            if fields.get(a).is_some_and(|v| v.trim() == "false") { continue; }
        }
        if let (Some(host), Some(p), Some(protos)) = (fields.get(ip), fields.get(port), fields.get(protocols)) {
            if let Ok(number) = p.trim().parse::<u16>() {
                for protocol in protos.split('|') {
                    add_candidate(map, protocol.trim(), host.trim(), number, source);
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum SourceKind { Json, Csv, Txt(&'static str) }

async fn fetch_inventory(shared: &Shared) -> Result<(HashMap<String, Candidate>, bool)> {
    let sources: Vec<(&str, SourceKind, String)> = vec![
        ("monosans", SourceKind::Json, "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies.json".into()),
        ("proxifly", SourceKind::Txt("http"), "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt".into()),
        ("hproxy", SourceKind::Csv, "https://raw.githubusercontent.com/hproxy-com/free-proxy-list/main/live.csv".into()),
        ("speedx", SourceKind::Txt("http"), "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt".into()),
        ("speedx", SourceKind::Txt("socks4"), "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt".into()),
        ("speedx", SourceKind::Txt("socks5"), "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt".into()),
    ];
    let mut jobs: Vec<(String, SourceKind, String)> = if shared.settings.public_discovery {
        sources.into_iter().map(|(name, kind, url)| (name.to_owned(), kind, url)).collect()
    } else { Vec::new() };
    for url in &shared.settings.extra_txt_urls {
        jobs.push(("extra".to_owned(), SourceKind::Txt("http"), url.as_str().to_owned()));
    }
    let calls = stream::iter(jobs).map(|(name, kind, url)| {
        let client = shared.direct_client.clone();
        async move {
            let result = async {
                let response = client.get(&url).timeout(Duration::from_secs(20)).send().await?;
                ensure!(response.status().is_success(), "source HTTP {}", response.status());
                let bytes = read_capped(response, MAX_SOURCE_BYTES).await
                    .context("proxy source response too large or truncated")?;
                Ok::<_, anyhow::Error>(String::from_utf8(bytes.to_vec())?)
            }.await;
            (name, kind, url, result)
        }
    }).buffer_unordered(8);
    tokio::pin!(calls);
    let mut map = HashMap::new();
    if let Some(path) = &shared.settings.list_file {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("read configured HTTP proxy list {path}"))?;
        txt_entries(&mut map, "configured", &content, "http");
    }
    let mut failures = 0;
    while let Some((name, kind, url, result)) = calls.next().await {
        match result {
            Ok(content) => {
                let parsed = match kind {
                    SourceKind::Json => json_entries(&mut map, &name, &content),
                    SourceKind::Csv => csv_entries(&mut map, &name, &content),
                    SourceKind::Txt(scheme) => { txt_entries(&mut map, &name, &content, scheme); Ok(()) },
                };
                if let Err(error) = parsed {
                    failures += 1;
                    crate::log_event!(Warn,"http_transport source={name} url={url} parse_error={error:#}");
                }
            }
            Err(error) => {
                failures += 1;
                crate::log_event!(Warn,"http_transport source={name} url={url} error={error:#}");
            }
        }
    }
    if failures != 0 {
        crate::log_event!(Info,"http_transport discovery partial_source_failures={failures} preserving_previous_routes=true");
    }
    ensure!(!map.is_empty(), "proxy discovery returned empty inventory");
    Ok((map, failures == 0))
}

async fn discovery_loop(shared: Arc<Shared>) {
    let cached = {
        let state = shared.inventory.lock().await;
        state.values().filter(|route| route.last_verified_unix_ms > 0)
            .map(|route| route.candidate.clone()).collect::<Vec<_>>()
    };
    if !cached.is_empty() {
        crate::log_event!(Info,"http_transport cache_revalidate_start routes={}", cached.len());
        verify_candidates(&shared, cached, "cache").await;
    }
    let discovery_enabled = shared.settings.public_discovery ||
        shared.settings.list_file.is_some() || !shared.settings.extra_txt_urls.is_empty();
    if !discovery_enabled { return; }
    // Cache recovery never waits for a 76k candidate sweep. A healthy warm
    // start postpones the first public sweep until the configured refresh time.
    let recovered = !shared.snapshots.borrow().is_empty();
    let first = if recovered { Instant::now() + shared.settings.refresh }
        else { Instant::now() };
    let mut ticker = tokio::time::interval_at(first, shared.settings.refresh);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if let Err(error) = refresh(&shared).await {
            crate::log_event!(Warn,"http_transport discovery_error={error:#}");
        }
    }
}

async fn refresh(shared: &Arc<Shared>) -> Result<()> {
    shared.host_route_cooldowns.lock().await.retain(|_, until| *until > Instant::now());
    let (found, complete) = fetch_inventory(shared).await?;
    let inventory_count = found.len();
    let now = Instant::now();
    let now_ms = unix_ms();
    let due = {
        let mut state = shared.inventory.lock().await;
        // Never evict last-known-good routes because a feed dropped them.
        // Drop unverified orphan candidates on a complete inventory refresh
        // so the RAM map cannot grow forever.
        if complete {
            state.retain(|id, route| found.contains_key(id) ||
                (route.last_verified_unix_ms != 0 &&
                 now_ms.saturating_sub(route.last_verified_unix_ms) <= CACHE_MAX_AGE_MS));
        }
        for (id, candidate) in found {
            if let Some(route) = state.get_mut(&id) {
                route.candidate.sources = candidate.sources;
            } else {
                state.insert(id, Route {
                    candidate, client: None, verified: None,
                    latency_ms: u64::MAX, next_check: now,
                    last_verified_unix_ms: 0,
                });
            }
        }
        state.values().filter(|r| r.next_check <= now)
            .map(|r| r.candidate.clone()).collect::<Vec<_>>()
    };
    publish(shared).await;
    let tcp_total = due.len();
    let tcp = stream::iter(due).map(|candidate| {
        let deadline = shared.settings.tcp_timeout;
        async move {
            let address = format!("{}:{}", candidate.host, candidate.port);
            let online = timeout(deadline, TcpStream::connect(address)).await
                .is_ok_and(|r| r.is_ok());
            (candidate, online)
        }
    }).buffer_unordered(shared.settings.tcp_parallel);
    tokio::pin!(tcp);
    let mut open = Vec::new();
    let mut dead = Vec::new();
    while let Some((candidate, live)) = tcp.next().await {
        if live { open.push(candidate); } else { dead.push(candidate.id); }
    }
    mark_failures(shared, dead).await;
    crate::log_event!(Info,"http_transport inventory={inventory_count} due={tcp_total} tcp_open={}", open.len());

    let host = shared.settings.verify_url.host_str().context("missing verify host")?.to_owned();
    let port = shared.settings.verify_url.port_or_known_default().unwrap_or(443);
    let probes = stream::iter(open).map(|candidate| {
        let host = host.clone();
        let deadline = shared.settings.handshake_timeout;
        async move {
            let okay = timeout(deadline, proxy_handshake(&candidate, &host, port))
                .await.is_ok_and(|r| r.is_ok());
            (candidate, okay)
        }
    }).buffer_unordered(shared.settings.handshake_parallel);
    tokio::pin!(probes);
    let mut candidates = Vec::new();
    let mut invalid = Vec::new();
    while let Some((candidate, fine)) = probes.next().await {
        if fine { candidates.push(candidate); } else { invalid.push(candidate.id); }
    }
    mark_failures(shared, invalid).await;
    crate::log_event!(Info,"http_transport handshake_ok={} verify_queue={}", candidates.len(), candidates.len());

    // Every actual DEX request is globally paced. Handshakes are not charged to this gate.
    verify_candidates(shared, candidates, "discovery").await;
    shared.cycles.send_modify(|cycle| *cycle += 1);
    Ok(())
}


fn signal_first_ready(shared: &Shared) {
    if *shared.cycles.borrow() == 0 && !shared.snapshots.borrow().is_empty() {
        shared.cycles.send_modify(|cycle| *cycle += 1);
    }
}

// Reuse identical validation for warm-start routes and freshly discovered routes.
// Publish partial successes immediately; do not wait for the whole inventory.
async fn verify_candidates(shared: &Arc<Shared>, candidates: Vec<Candidate>, stage: &str) -> usize {
    let verify = stream::iter(candidates).map(|candidate| {
        let shared = shared.clone();
        async move {
            let outcome = async {
                let client = proxy_client(&candidate)?;
                let host = shared.settings.verify_url.host_str().context("missing verify hostname")?;
                // When a provider endpoint doubles as the probe, respect its
                // production host budget instead of the generic health-test rate.
                let provider_probe = shared.settings.probe_url.host_str() == Some(host);
                let host_rps = shared.settings.host_rps_overrides.get(host).copied()
                    .unwrap_or(shared.settings.request_rps_per_host);
                let allowed_rps = if provider_probe && host_rps > 0 {
                    shared.settings.probe_rps.min(host_rps)
                } else { shared.settings.probe_rps };
                host_limit_at(&shared, host, allowed_rps).await;
                let started = Instant::now();
                let response = client.get(shared.settings.verify_url.clone())
                    .timeout(shared.settings.probe_timeout).send().await?;
                ensure!(response.status().is_success(), "verify HTTP {}", response.status());
                let body = read_capped(response, MAX_RESPONSE_BYTES).await
                    .context("proxy verify response too large or truncated")?;
                validate_live_payload(shared.settings.verify_url.as_str(), &body)
                    .context("proxy did not return the expected target data")?;
                Ok::<_, anyhow::Error>((client, started.elapsed().as_millis() as u64))
            }.await;
            (candidate, outcome)
        }
    }).buffer_unordered(shared.settings.probe_parallel);
    tokio::pin!(verify);
    let mut tested = 0usize;
    let mut ok = 0usize;
    while let Some((candidate, outcome)) = verify.next().await {
        tested += 1;
        let mut state = shared.inventory.lock().await;
        if let Some(route) = state.get_mut(&candidate.id) {
            match outcome {
                Ok((client, elapsed)) => {
                    ok += 1;
                    route.client = Some(client);
                    route.verified = Some(Instant::now());
                    route.last_verified_unix_ms = unix_ms();
                    route.latency_ms = elapsed;
                    route.next_check = Instant::now() + shared.settings.refresh;
                }
                Err(_) => {
                    route.client = None;
                    route.verified = None;
                    route.next_check = Instant::now() + shared.settings.retry_failure_after;
                }
            }
        }
        drop(state);
        if tested % 25 == 0 {
            publish(shared).await;
            signal_first_ready(shared);
            crate::log_event!(Info,"http_transport {stage} verify_tested={tested} verify_ok={ok}");
        }
    }
    publish(shared).await;
    signal_first_ready(shared);
    crate::log_event!(Info,"http_transport {stage}_complete verified_tested={tested} verified_ok={ok} working={}", shared.snapshots.borrow().len());
    ok

}

async fn mark_failures(shared: &Shared, ids: Vec<String>) {
    let mut state = shared.inventory.lock().await;
    let next = Instant::now() + shared.settings.retry_failure_after;
    for id in ids {
        if let Some(route) = state.get_mut(&id) {
            route.client = None;
            route.verified = None;
            route.next_check = next;
        }
    }
}

async fn publish(shared: &Shared) {
    let state = shared.inventory.lock().await;
    let mut healthy = state.values().filter_map(|r| {
        let age = r.verified?.elapsed();
        if age > shared.settings.healthy_for || r.client.is_none() { return None; }
        let mut sources: Vec<_> = r.candidate.sources.iter().cloned().collect();
        sources.sort();
        Some(RouteSnapshot {
            id: r.candidate.id.clone(),
            protocol: r.candidate.protocol.clone(),
            sources,
            latency_ms: r.latency_ms,
            verified_age_ms: age.as_millis() as u64,
        })
    }).collect::<Vec<_>>();
    healthy.sort_by_key(|v| v.latency_ms);
    drop(state);
    shared.snapshots.send_replace(Arc::new(healthy));
    persist_cached_routes(shared).await;
}

async fn proxy_handshake(candidate: &Candidate, host: &str, port: u16) -> Result<()> {
    let mut socket = TcpStream::connect(format!("{}:{}", candidate.host, candidate.port)).await?;
    match candidate.protocol.as_str() {
        "http" => {
            let target = format!("{host}:{port}");
            let request = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\nProxy-Connection: close\r\n\r\n");
            socket.write_all(request.as_bytes()).await?;
            let mut buf = Vec::with_capacity(256);
            let mut byte = [0u8; 1];
            while buf.len() < 4096 && !buf.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await?;
                buf.push(byte[0]);
            }
            ensure!(buf.starts_with(b"HTTP/1.1 200 ") || buf.starts_with(b"HTTP/1.0 200 "), "CONNECT not accepted");
        }
        "socks5h" => {
            socket.write_all(&[5, 1, 0]).await?;
            let mut greeting = [0u8; 2];
            socket.read_exact(&mut greeting).await?;
            ensure!(greeting == [5, 0], "SOCKS5 authentication rejected");
            ensure!(host.len() <= 255, "SOCKS5 target host too long");
            let mut packet = vec![5, 1, 0, 3, host.len() as u8];
            packet.extend_from_slice(host.as_bytes());
            packet.extend_from_slice(&port.to_be_bytes());
            socket.write_all(&packet).await?;
            let mut reply = [0u8; 4];
            socket.read_exact(&mut reply).await?;
            ensure!(reply[0] == 5 && reply[1] == 0, "SOCKS5 CONNECT rejected");
            let addr_len = match reply[3] {
                1 => 4, 4 => 16,
                3 => { let mut n = [0u8]; socket.read_exact(&mut n).await?; usize::from(n[0]) },
                _ => bail!("SOCKS5 unknown address type"),
            };
            let mut remainder = vec![0u8; addr_len + 2];
            socket.read_exact(&mut remainder).await?;
        }
        "socks4a" => {
            let mut packet = vec![4, 1];
            packet.extend_from_slice(&port.to_be_bytes());
            packet.extend_from_slice(&[0, 0, 0, 1, 0]);
            packet.extend_from_slice(host.as_bytes());
            packet.push(0);
            socket.write_all(&packet).await?;
            let mut reply = [0u8; 8];
            socket.read_exact(&mut reply).await?;
            ensure!(reply[1] == 0x5a, "SOCKS4 CONNECT rejected");
        }
        _ => bail!("unsupported proxy protocol"),
    }
    Ok(())
}

fn proxy_client(candidate: &Candidate) -> Result<Client> {
    Ok(Client::builder()
        .no_proxy()
        .proxy(Proxy::all(candidate.id.as_str()).with_context(|| format!("invalid proxy {}", candidate.id))?)
        .pool_idle_timeout(Duration::from_secs(60))
        .timeout(Duration::from_secs(30))
        .build()?)
}

async fn host_limit_at(shared: &Shared, host: &str, rps: u32) {
    let mut gates = shared.host_gates.lock().await;
    let next = gates.entry(host.to_owned()).or_insert_with(Instant::now);
    let at = (*next).max(Instant::now());
    *next = at + Duration::from_secs_f64(1.0 / f64::from(rps));
    drop(gates);
    sleep_until(at).await;
}

async fn pace_request(shared: &Shared, host: &str, route_id: &str) {
    let host_rps = shared.settings.host_rps_overrides.get(host)
        .copied().unwrap_or(shared.settings.request_rps_per_host);
    // Destination limits and proxy-address limits are separate. Identical proxy IPs
    // on different ports share one address budget. The configured address may not
    // equal the actual egress IP of a public proxy.
    let mut host_gates = shared.host_gates.lock().await;
    let mut route_gates = shared.route_gates.lock().await;
    let now = Instant::now();
    let host_due = if host_rps == 0 { now }
        else { host_gates.get(host).copied().unwrap_or(now) };
    let route_key = (host.to_owned(), route_id.to_owned());
    let route_due = if route_id == "direct" { now }
        else { route_gates.get(&route_key).copied().unwrap_or(now) };
    let at = now.max(host_due).max(route_due);
    if host_rps > 0 {
        host_gates.insert(host.to_owned(),
            at + Duration::from_secs_f64(1.0 / f64::from(host_rps)));
    }
    if route_id != "direct" {
        route_gates.insert(route_key,
            at + Duration::from_secs_f64(1.0 / f64::from(shared.settings.request_rps_per_route)));
    }
    drop(route_gates);
    drop(host_gates);
    sleep_until(at).await;
}

// Already scheduled requests must also observe a 429 received by another lane.
// After a cooldown, reserve a fresh provider-rate slot to prevent a wake-up burst.
async fn respect_backoff(shared: &Shared, host: &str, route_id: &str) {
    loop {
        let until = shared.host_backoffs.lock().await.get(host).copied();
        let Some(until) = until.filter(|deadline| *deadline > Instant::now()) else { break; };
        sleep_until(until).await;
        pace_request(shared, host, route_id).await;
    }
}

async fn choose_route(shared: &Shared, host: &str, excluded: &HashSet<String>) -> Result<(String, String, Client)> {
    let snapshot = Arc::clone(&*shared.snapshots.borrow());
    ensure!(!snapshot.is_empty(), "no verified proxy routes; direct fallback forbidden");
    let start = shared.selection.fetch_add(1, Ordering::Relaxed) % snapshot.len();
    let blocked = shared.host_route_cooldowns.lock().await;
    let state = shared.inventory.lock().await;
    for offset in 0..snapshot.len() {
        let id = &snapshot[(start + offset) % snapshot.len()].id;
        if excluded.contains(id) || blocked.get(&(host.to_owned(), id.clone()))
            .is_some_and(|until| *until > Instant::now()) { continue; }
        if let Some(route) = state.get(id) {
            if route.verified.is_some_and(|at| at.elapsed() < shared.settings.healthy_for) {
                if let Some(client) = &route.client {
                    return Ok((id.clone(), route.candidate.host.clone(), client.clone()));
                }
            }
        }
    }
    bail!("no verified untried proxy routes; direct fallback forbidden");
}

async fn cooldown_host_route(shared: &Shared, host: &str, route_id: &str) {
    if route_id == "direct" { return; }
    shared.host_route_cooldowns.lock().await.insert(
        (host.to_owned(), route_id.to_owned()),
        Instant::now() + shared.settings.retry_failure_after,
    );
}

async fn quarantine(shared: &Shared, route_id: &str) {
    if route_id == "direct" { return; }
    let mut state = shared.inventory.lock().await;
    if let Some(route) = state.get_mut(route_id) {
        route.client = None;
        route.verified = None;
        route.next_check = Instant::now() + shared.settings.retry_failure_after;
    }
    drop(state);
    publish(shared).await;
}

async fn read_capped(mut response: reqwest::Response, cap: usize) -> Result<Bytes> {
    ensure!(response.content_length().is_none_or(|n| n <= cap as u64),
        "HTTP response exceeds size cap");
    let mut body = BytesMut::new();
    while let Some(chunk) = response.chunk().await.context("HTTP response body")? {
        ensure!(chunk.len() <= cap.saturating_sub(body.len()),
            "HTTP response exceeds size cap");
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

async fn execute(shared: &Shared, request: HttpRequest) -> Result<HttpResponse> {
    ensure!(matches!(request.url.scheme(), "http" | "https"), "HTTP request requires http(s) URL");
    ensure!(request.url.username().is_empty() && request.url.password().is_none(),
        "credentials in request URL forbidden");
    ensure!(!request.timeout.is_zero(), "request timeout must be positive");
    let host = request.url.host_str().context("request URL missing host")?.to_ascii_lowercase();

    // Public proxies are untrusted; never forward authentication material.
    if request.policy == RoutePolicy::ProxyOnly {
        ensure!(!["authorization", "proxy-authorization", "cookie", "x-api-key"]
            .iter().any(|key| request.headers.contains_key(*key)),
            "authentication headers cannot be forwarded through public proxies");
    }
    let idempotent = request.method == Method::GET || request.method == Method::HEAD;
    let max_attempts = if idempotent { shared.settings.retry_attempts } else { 1 };
    let mut excluded = HashSet::new();
    let mut previous_route = None::<String>;
    let mut last_error = None;
    for attempt in 0..max_attempts {
        let (route_id, proxy_ip, client) = match request.policy {
            RoutePolicy::DirectOnly => ("direct".to_owned(), "direct".to_owned(), shared.direct_client.clone()),
            RoutePolicy::ProxyOnly => match choose_route(shared, &host, &excluded).await {
                Ok(value) => value,
                Err(error) if attempt == 0 => return Err(error),
                Err(_) => break,
            },
        };
        if attempt > 0 {
            if previous_route.as_deref() != Some(&route_id) {
                shared.stats.lock().expect("HTTP stats lock poisoned").failovers += 1;
            }
        }
        previous_route = Some(route_id.clone());
        excluded.insert(route_id.clone());
        {
            let mut stats = shared.stats.lock().expect("HTTP stats lock poisoned");
            stats.attempts += 1;
            stats.routes_used.insert(route_id.clone());
        }
        pace_request(shared, &host, &proxy_ip).await;
        respect_backoff(shared, &host, &proxy_ip).await;
        let started = Instant::now();
        let mut builder = client.request(request.method.clone(), request.url.clone())
            .headers(request.headers.clone())
            .timeout(request.timeout);
        if let Some(body) = &request.body { builder = builder.body(body.clone()); }

        let response = match builder.send().await {
            Ok(response) => response,
            Err(error) => {
                shared.stats.lock().expect("HTTP stats lock poisoned").transport_errors += 1;
                let route_failure = error.is_connect() || error.is_timeout();
                last_error = Some(anyhow!("HTTP route={route_id} transport: {}", error.without_url()));
                if route_id != "direct" && route_failure {
                    quarantine(shared, &route_id).await;
                }
                if !idempotent { break; }
                continue;
            }
        };
        let status = response.status();
        if status == StatusCode::TOO_MANY_REQUESTS {
            shared.stats.lock().expect("HTTP stats lock poisoned").http_429 += 1;
            let retry = response.headers().get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(5).clamp(1, 3600);
            let deadline = Instant::now() + Duration::from_secs(retry);
            let mut backoffs = shared.host_backoffs.lock().await;
            let previous = backoffs.entry(host.clone()).or_insert(deadline);
            *previous = (*previous).max(deadline);
            drop(backoffs);
            let mut gates = shared.host_gates.lock().await;
            let at = gates.entry(host.clone()).or_insert(deadline);
            *at = (*at).max(deadline);
            drop(gates);
        } else if status == StatusCode::FORBIDDEN {
            shared.stats.lock().expect("HTTP stats lock poisoned").http_403 += 1;
        } else if status.is_server_error() {
            shared.stats.lock().expect("HTTP stats lock poisoned").http_5xx += 1;
        }

        let body = match read_capped(response, MAX_RESPONSE_BYTES).await {
            Ok(bytes) => bytes,
            Err(error) => {
                shared.stats.lock().expect("HTTP stats lock poisoned").other_errors += 1;
                last_error = Some(error.context(format!("HTTP route={route_id} response body")));
                quarantine(shared, &route_id).await;
                if !idempotent { break; }
                continue;
            }
        };
        if status.is_success() && route_id != "direct" {
            let mut state = shared.inventory.lock().await;
            if let Some(route) = state.get_mut(&route_id) {
                route.verified = Some(Instant::now());
                route.last_verified_unix_ms = unix_ms();
                route.latency_ms = started.elapsed().as_millis() as u64;
                route.next_check = Instant::now() + shared.settings.refresh;
            }
        }
        if matches!(status, StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT | StatusCode::FORBIDDEN)
        {
            cooldown_host_route(shared, &host, &route_id).await;
            if idempotent && attempt + 1 < max_attempts {
                last_error = Some(anyhow!("HTTP route={route_id} status={status}"));
                continue;
            }
        }
        // HTTP statuses, including 429, are returned to the caller for its policy.
        return Ok(HttpResponse {
            status, body, route_id, elapsed_ms: started.elapsed().as_millis() as u64,
        });
    }
    Err(last_error.unwrap_or_else(|| anyhow!("HTTP transport retries exhausted")))
}


/// Standalone live-probe validator; the transport itself never decodes DEX payloads.
/// Custom non-DEX test URLs are accepted based on HTTP status by the caller.
fn validate_live_payload(url: &str, body: &Bytes) -> Result<()> {
    if url.starts_with("https://api.dexscreener.com/token-pairs/v1/solana/") {
        let expected = url.rsplit('/').next().context("DEX live probe mint missing")?;
        let rows: Value = serde_json::from_slice(body).context("invalid live DEX JSON")?;
        let matches = rows.as_array().is_some_and(|pairs| pairs.iter().any(|pair| {
            pair.get("chainId").and_then(Value::as_str) == Some("solana")
            && ["baseToken", "quoteToken"].iter().any(|key| {
                pair.get(*key).and_then(|v| v.get("address"))
                    .and_then(Value::as_str) == Some(expected)
            })
        }));
        ensure!(matches, "live DEX payload lacks expected Solana mint");
    }
    // Diagnostic validation only: reject a Cloudflare HTML challenge even if
    // a proxy returns HTTP 200; DEX decoding remains inside source workers.
    if url.contains("pancakeswap.com/api/") ||
        url.starts_with("https://frontend-api-v3.pump.fun/coins") {
        let value: Value = serde_json::from_slice(body).context("live API response is not JSON")?;
        ensure!(value.is_object() || value.is_array(), "live API JSON must be object or array");
    }
    Ok(())
}

/// Standalone HTTP proxy discovery and real-request diagnostic.
/// Only invoked by --http-transport-live; normal REST sources keep their own clients.
pub async fn live_diagnostic() -> Result<()> {
    let config = Settings::from_env()?;
    ensure!(config.public_discovery || config.list_file.is_some()
        || !config.extra_txt_urls.is_empty() || !load_cached_routes(&config).is_empty(),
        "HTTP proxy live mode requires a configured source or a saved proxy cache");
    let target = env::var("HTTP_TRANSPORT_LIVE_URL")
        .unwrap_or_else(|_| config.probe_url.to_string());
    let calls: usize = env::var("HTTP_TRANSPORT_LIVE_REQUESTS")
        .unwrap_or_else(|_| "100".into()).parse()
        .context("invalid HTTP_TRANSPORT_LIVE_REQUESTS")?;
    let parallel: usize = env::var("HTTP_TRANSPORT_LIVE_PARALLEL")
        .unwrap_or_else(|_| "32".into()).parse()
        .context("invalid HTTP_TRANSPORT_LIVE_PARALLEL")?;
    ensure!((1..=2000).contains(&calls), "HTTP live requests must be 1..=2000");
    ensure!((1..=512).contains(&parallel), "HTTP live parallel must be 1..=512");
    let (handle, task) = spawn(config)?;
    let route_updates = handle.subscribe_routes();
    let mut cycles = handle.subscribe_cycles();
    tokio::time::timeout(Duration::from_secs(900), async {
        loop {
            if *cycles.borrow_and_update() > 0 { break Ok::<_, anyhow::Error>(()); }
            cycles.changed().await.context("HTTP discovery cycle publisher closed")?;
        }
    }).await.context("HTTP proxy discovery did not finish within 900 seconds")??;
    let verified = handle.working_set();
    ensure!(!verified.is_empty(), "HTTP discovery finished with no verified proxies");
    crate::log_event!(Info,
        "http_transport live working_set={} fastest_ms={} protocol={} sources={} oldest_verified_age_ms={} published_routes={}",
        verified.len(), verified[0].latency_ms, verified[0].protocol,
        verified[0].sources.join(","),
        verified.iter().map(|route| route.verified_age_ms).max().unwrap_or(0),
        route_updates.borrow().len()
    );

    let started = std::time::Instant::now();
    let mut ok = 0usize;
    let mut failed = 0usize;
    let mut status_429 = 0usize;
    let mut other_status = 0usize;
    let mut invalid_payload = 0usize;
    let mut routes_used = HashSet::new();
    let mut latencies = Vec::with_capacity(calls);
    let mut bytes = 0usize;
    let mut sample = None;
    {
        let jobs = stream::iter(0..calls).map(|_| {
            let handle = handle.clone();
            let target = target.clone();
            async move {
                let request = HttpRequest::get(
                    &target, RoutePolicy::ProxyOnly)?;
                handle.call(request).await
            }
        }).buffer_unordered(parallel);
        tokio::pin!(jobs);
        while let Some(result) = jobs.next().await {
            match result {
                Ok(response) => {
                    routes_used.insert(response.route_id.clone());
                    latencies.push(response.elapsed_ms);
                    if response.status.is_success() {
                        match validate_live_payload(&target, &response.body) {
                            Ok(()) => {
                                ok += 1;
                                bytes += response.body.len();
                                if sample.is_none() {
                                    sample = Some(format!("route={} status={} bytes={}",
                                        response.route_id, response.status, response.body.len()));
                                }
                            }
                            Err(error) => {
                                invalid_payload += 1;
                                if invalid_payload <= 3 {
                                    crate::log_event!(Warn,"http_transport live invalid_payload={error:#}");
                                }
                            }
                        }
                    } else if response.status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        status_429 += 1;
                    } else {
                        other_status += 1;
                    }
                }
                Err(error) => {
                    failed += 1;
                    if failed <= 3 { crate::log_event!(Warn,"http_transport live request_error={error:#}"); }
                }
            }
        }
    }
    latencies.sort_unstable();
    let p50 = latencies.get(latencies.len().saturating_sub(1) * 50 / 100)
        .copied().unwrap_or(0);
    let p95 = latencies.get(latencies.len().saturating_sub(1) * 95 / 100)
        .copied().unwrap_or(0);
    let elapsed = started.elapsed().as_secs_f64();
    crate::log_event!(Warn,
        "http_transport live benchmark requests={} success={} errors={} http_429={} other_http={} invalid_payload={} distinct_routes={} bytes={} elapsed_s={:.2} throughput_rps={:.2} latency_p50_ms={} latency_p95_ms={}",
        calls, ok, failed, status_429, other_status, invalid_payload, routes_used.len(),
        bytes, elapsed, calls as f64 / elapsed.max(0.001), p50, p95
    );
    ensure!(ok > 0, "HTTP proxy transport did not produce a successful response");
    crate::log_event!(Info,"http_transport live PASS {}", sample.unwrap_or_default());
    drop(cycles);
    drop(handle);
    task.await??;
    Ok(())
}
