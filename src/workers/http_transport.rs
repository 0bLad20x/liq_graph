//! Shared asynchronous HTTP transport with an in-memory public-proxy working set.
//! One boundary: source discovery, transport verification, route selection, raw HTTP I/O.
//! No DEX business decoding beyond a standalone health-test response validator.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    net::Ipv4Addr,
    str::FromStr,
    sync::{Arc, Mutex as StdMutex, atomic::{AtomicUsize, Ordering}},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream};
use reqwest::{Client, Method, Proxy, StatusCode, Url, header::HeaderMap};
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
    pub request_rps_per_route: u32,
    pub retry_attempts: usize,
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
        let request_rps_per_route = from_env("HTTP_TRANSPORT_ROUTE_RPS", 5_u32)?;
        let retry_attempts = from_env("HTTP_TRANSPORT_RETRY_ATTEMPTS", 3_usize)?;
        for value in [refresh, tcp, handshake, probe, healthy, retry] {
            ensure!(value > 0, "HTTP transport intervals must be positive");
        }
        for value in [tcp_parallel, handshake_parallel, probe_parallel, request_parallel, queue_capacity] {
            ensure!((1..=4096).contains(&value), "HTTP transport concurrency must be 1..=4096");
        }
        ensure!(probe_rps > 0 && request_rps_per_host > 0 && request_rps_per_route > 0,
            "HTTP transport RPS must be positive");
        ensure!((1..=5).contains(&retry_attempts), "HTTP transport retries must be 1..=5");
        let probe_url = Url::parse(&env::var("HTTP_PROXY_PROBE_URL")
            .unwrap_or_else(|_| DEX_PROBE.to_owned()))?;
        let verify_url = Url::parse(&env::var("HTTP_PROXY_VERIFY_URL")
            .unwrap_or_else(|_| "https://www.gstatic.com/generate_204".to_owned()))?;
        ensure!([&probe_url, &verify_url].iter().all(|u| u.scheme() == "https" && u.host_str().is_some()),
            "probe and verify URLs must be HTTPS");
        let list_file = env::var("HTTP_PROXY_LIST_FILE").ok().filter(|s| !s.trim().is_empty());
        let public_discovery = match env::var("HTTP_PROXY_PUBLIC_DISCOVERY").as_deref() {
            Ok("false" | "0") => false,
            Ok("true" | "1") | Err(_) => true,
            _ => bail!("invalid HTTP_PROXY_PUBLIC_DISCOVERY"),
        };
        ensure!(public_discovery || list_file.is_some(), "configure at least one proxy source");
        let mut host_rps_overrides = HashMap::new();
        for rule in env::var("HTTP_TRANSPORT_HOST_RPS_OVERRIDES").unwrap_or_default()
            .split(',').map(str::trim).filter(|s| !s.is_empty())
        {
            let (host, raw) = rule.rsplit_once(':')
                .context("host RPS override must be host:rps")?;
            let rps = raw.parse::<u32>().context("invalid host RPS override")?;
            ensure!(rps > 0 && !host.is_empty() && !host.contains('/'),
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

fn report_stats(shared: &Shared) {
    let stats = shared.stats.lock().expect("HTTP stats lock poisoned");
    println!(
        "http_transport stats submitted={} completed={} ok={} failed={} attempts={} failovers={} http_429={} http_403={} http_5xx={} transport_errors={} other_errors={} distinct_routes={} latency_p50_ms={} latency_p95_ms={} queue_p95_ms={}",
        stats.submitted, stats.completed, stats.succeeded,
        stats.completed.saturating_sub(stats.succeeded), stats.attempts, stats.failovers,
        stats.http_429, stats.http_403, stats.http_5xx, stats.transport_errors,
        stats.other_errors, stats.routes_used.len(),
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
    route_gates: Mutex<HashMap<(String, String), Instant>>,
    stats: StdMutex<TransportStats>,
    direct_client: Client,
}

pub fn spawn(settings: Settings) -> Result<(HttpHandle, JoinHandle<Result<()>>)> {
    let (calls, receiver) = mpsc::channel(settings.queue_capacity);
    let (snapshots, route_reader) = watch::channel(Arc::new(Vec::new()));
    let (cycles, cycle_reader) = watch::channel(0_u64);
    let direct_client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()?;
    let shared = Arc::new(Shared {
        settings: settings.clone(),
        inventory: Mutex::new(HashMap::new()),
        snapshots, cycles,
        selection: AtomicUsize::new(0),
        host_gates: Mutex::new(HashMap::new()),
        route_gates: Mutex::new(HashMap::new()),
        stats: StdMutex::new(TransportStats::default()),
        direct_client,
    });
    let task = tokio::spawn(run(shared, receiver));
    Ok((HttpHandle { calls, routes: route_reader, cycles: cycle_reader }, task))
}

async fn run(shared: Arc<Shared>, mut receiver: mpsc::Receiver<Call>) -> Result<()> {
    let discover = {
        let shared = shared.clone();
        tokio::spawn(async move { discovery_loop(shared).await })
    };
    let capacity = Arc::new(Semaphore::new(shared.settings.request_parallel));
    let reporter = {
        let shared = shared.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            tick.tick().await;
            loop { tick.tick().await; report_stats(&shared); }
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
    discover.abort();
    let _ = discover.await;
    reporter.abort();
    report_stats(&shared);
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
                    eprintln!("http_transport source={name} url={url} parse_error={error:#}");
                }
            }
            Err(error) => {
                failures += 1;
                eprintln!("http_transport source={name} url={url} error={error:#}");
            }
        }
    }
    if failures != 0 {
        eprintln!("http_transport discovery partial_source_failures={failures} preserving_previous_routes=true");
    }
    ensure!(!map.is_empty(), "proxy discovery returned empty inventory");
    Ok((map, failures == 0))
}

async fn discovery_loop(shared: Arc<Shared>) {
    let mut ticker = tokio::time::interval(shared.settings.refresh);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if let Err(error) = refresh(&shared).await {
            eprintln!("http_transport discovery_error={error:#}");
        }
    }
}

async fn refresh(shared: &Arc<Shared>) -> Result<()> {
    let (found, complete) = fetch_inventory(shared).await?;
    let inventory_count = found.len();
    let now = Instant::now();
    let due = {
        let mut state = shared.inventory.lock().await;
        // One unavailable source must not evict its previously verified routes.
        if complete { state.retain(|id, _| found.contains_key(id)); }
        for (id, candidate) in found {
            if let Some(route) = state.get_mut(&id) {
                route.candidate.sources = candidate.sources;
            } else {
                state.insert(id, Route {
                    candidate, client: None, verified: None,
                    latency_ms: u64::MAX, next_check: now,
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
    println!("http_transport inventory={inventory_count} due={tcp_total} tcp_open={}", open.len());

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
    println!("http_transport handshake_ok={} verify_queue={}", candidates.len(), candidates.len());

    // Every actual DEX request is globally paced. Handshakes are not charged to this gate.
    let verify = stream::iter(candidates).map(|candidate| {
        let shared = shared.clone();
        async move {
            let outcome = async {
                let client = proxy_client(&candidate)?;
                let host = shared.settings.verify_url.host_str().context("missing verify hostname")?;
                host_limit_at(&shared, host, shared.settings.probe_rps).await;
                let started = Instant::now();
                let response = client.get(shared.settings.verify_url.clone())
                    .timeout(shared.settings.probe_timeout).send().await?;
                ensure!(response.status().is_success(), "verify HTTP {}", response.status());
                let _body = read_capped(response, 1024).await
                    .context("proxy verify response too large or truncated")?;
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
        if tested % 100 == 0 {
            publish(shared).await;
            println!("http_transport verify_tested={tested} verify_ok={ok}");
        }
    }
    publish(shared).await;
    println!("http_transport cycle_complete verified_tested={tested} verified_ok={ok} working={}", shared.snapshots.borrow().len());
    shared.cycles.send_modify(|cycle| *cycle += 1);
    Ok(())
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
    // Both limits apply. Adding proxy routes does not implicitly enlarge the
    // provider's configured quota; route-level capacity is a separate limit.
    let mut host_gates = shared.host_gates.lock().await;
    let mut route_gates = shared.route_gates.lock().await;
    let now = Instant::now();
    let host_due = host_gates.get(host).copied().unwrap_or(now);
    let route_key = (host.to_owned(), route_id.to_owned());
    let route_due = route_gates.get(&route_key).copied().unwrap_or(now);
    let at = now.max(host_due).max(route_due);
    host_gates.insert(host.to_owned(),
        at + Duration::from_secs_f64(1.0 / f64::from(host_rps)));
    route_gates.insert(route_key,
        at + Duration::from_secs_f64(1.0 / f64::from(shared.settings.request_rps_per_route)));
    drop(route_gates);
    drop(host_gates);
    sleep_until(at).await;
}

async fn choose_route(shared: &Shared, excluded: &HashSet<String>) -> Result<(String, Client)> {
    let snapshot = Arc::clone(&*shared.snapshots.borrow());
    ensure!(!snapshot.is_empty(), "no verified proxy routes; direct fallback forbidden");
    let start = shared.selection.fetch_add(1, Ordering::Relaxed) % snapshot.len();
    let state = shared.inventory.lock().await;
    for offset in 0..snapshot.len() {
        let id = &snapshot[(start + offset) % snapshot.len()].id;
        if excluded.contains(id) { continue; }
        if let Some(route) = state.get(id) {
            if route.verified.is_some_and(|at| at.elapsed() < shared.settings.healthy_for) {
                if let Some(client) = &route.client {
                    return Ok((id.clone(), client.clone()));
                }
            }
        }
    }
    bail!("no verified untried proxy routes; direct fallback forbidden");
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
        let (route_id, client) = match request.policy {
            RoutePolicy::DirectOnly => ("direct".to_owned(), shared.direct_client.clone()),
            RoutePolicy::ProxyOnly => match choose_route(shared, &excluded).await {
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
        pace_request(shared, &host, &route_id).await;
        let started = Instant::now();
        let mut builder = client.request(request.method.clone(), request.url.clone())
            .headers(request.headers.clone())
            .timeout(request.timeout);
        if let Some(body) = &request.body { builder = builder.body(body.clone()); }

        let response = match builder.send().await {
            Ok(response) => response,
            Err(error) => {
                shared.stats.lock().expect("HTTP stats lock poisoned").transport_errors += 1;
                last_error = Some(anyhow!("HTTP route={route_id} transport: {}", error.without_url()));
                if route_id != "direct" && (error.is_connect() || error.is_timeout()) {
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
            let mut gates = shared.host_gates.lock().await;
            let deadline = Instant::now() + Duration::from_secs(retry);
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
                route.latency_ms = started.elapsed().as_millis() as u64;
                route.next_check = Instant::now() + shared.settings.refresh;
            }
        }
        if matches!(status, StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT | StatusCode::FORBIDDEN)
        {
            quarantine(shared, &route_id).await;
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
