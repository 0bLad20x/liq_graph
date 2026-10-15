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
}

impl HttpHandle {
    pub async fn call(&self, request: HttpRequest) -> Result<HttpResponse> {
        let (reply, result) = oneshot::channel();
        self.calls.send(Call { request, reply }).await.context("HTTP transport stopped")?;
        result.await.context("HTTP transport dropped request")?
    }
    pub fn working_set(&self) -> Arc<Vec<RouteSnapshot>> {
        Arc::clone(&*self.routes.borrow())
    }
    pub fn subscribe_routes(&self) -> watch::Receiver<Arc<Vec<RouteSnapshot>>> {
        self.routes.clone()
    }
}

struct Call {
    request: HttpRequest,
    reply: oneshot::Sender<Result<HttpResponse>>,
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
        let probe_rps = from_env("HTTP_PROXY_PROBE_RPS", 5_u32)?;
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

struct Shared {
    settings: Settings,
    inventory: Mutex<HashMap<String, Route>>,
    snapshots: watch::Sender<Arc<Vec<RouteSnapshot>>>,
    selection: AtomicUsize,
    // Limit requests by target host, not by public proxy address.
    host_gates: Mutex<HashMap<String, Instant>>,
    direct_client: Client,
}

pub fn spawn(settings: Settings) -> Result<(HttpHandle, JoinHandle<Result<()>>)> {
    let (calls, receiver) = mpsc::channel(settings.queue_capacity);
    let (snapshots, route_reader) = watch::channel(Arc::new(Vec::new()));
    let direct_client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()?;
    let shared = Arc::new(Shared {
        settings: settings.clone(),
        inventory: Mutex::new(HashMap::new()),
        snapshots,
        selection: AtomicUsize::new(0),
        host_gates: Mutex::new(HashMap::new()),
        direct_client,
    });
    let task = tokio::spawn(run(shared, receiver));
    Ok((HttpHandle { calls, routes: route_reader }, task))
}

async fn run(shared: Arc<Shared>, mut receiver: mpsc::Receiver<Call>) -> Result<()> {
    let discover = {
        let shared = shared.clone();
        tokio::spawn(async move { discovery_loop(shared).await })
    };
    let capacity = Arc::new(Semaphore::new(shared.settings.request_parallel));
    while let Some(call) = receiver.recv().await {
        let permit = capacity.clone().acquire_owned().await.context("HTTP transport semaphore closed")?;
        let shared = shared.clone();
        tokio::spawn(async move {
            let result = execute(&shared, call.request).await;
            let _ = call.reply.send(result);
            drop(permit);
        });
    }
    discover.abort();
    let _ = discover.await;
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

async fn fetch_inventory(shared: &Shared) -> Result<HashMap<String, Candidate>> {
    let sources: Vec<(&str, SourceKind, String)> = vec![
        ("monosans", SourceKind::Json, "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies.json".into()),
        ("proxifly", SourceKind::Txt("http"), "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt".into()),
        ("hproxy", SourceKind::Csv, "https://raw.githubusercontent.com/hproxy-com/free-proxy-list/main/live.csv".into()),
        ("speedx", SourceKind::Txt("http"), "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt".into()),
        ("speedx", SourceKind::Txt("socks4"), "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt".into()),
        ("speedx", SourceKind::Txt("socks5"), "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt".into()),
    ];
    let mut jobs: Vec<(String, SourceKind, String)> = sources.into_iter()
        .map(|(name, kind, url)| (name.to_owned(), kind, url)).collect();
    for url in &shared.settings.extra_txt_urls {
        jobs.push(("extra".to_owned(), SourceKind::Txt("http"), url.as_str().to_owned()));
    }
    let calls = stream::iter(jobs).map(|(name, kind, url)| {
        let client = shared.direct_client.clone();
        async move {
            let result = async {
                let response = client.get(&url).timeout(Duration::from_secs(20)).send().await?;
                ensure!(response.status().is_success(), "source HTTP {}", response.status());
                ensure!(response.content_length().is_none_or(|n| n <= MAX_SOURCE_BYTES as u64), "source too large");
                let bytes = response.bytes().await?;
                ensure!(bytes.len() <= MAX_SOURCE_BYTES, "source too large");
                Ok::<_, anyhow::Error>(String::from_utf8(bytes.to_vec())?)
            }.await;
            (name, kind, url, result)
        }
    }).buffer_unordered(8);
    tokio::pin!(calls);
    let mut map = HashMap::new();
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
    ensure!(failures == 0, "{failures} proxy sources failed; existing registry retained");
    ensure!(!map.is_empty(), "proxy discovery returned empty inventory");
    Ok(map)
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
    let found = fetch_inventory(shared).await?;
    let inventory_count = found.len();
    let now = Instant::now();
    let due = {
        let mut state = shared.inventory.lock().await;
        state.retain(|id, _| found.contains_key(id));
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

    let host = shared.settings.probe_url.host_str().context("missing probe host")?.to_owned();
    let port = shared.settings.probe_url.port_or_known_default().unwrap_or(443);
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
    println!("http_transport handshake_ok={} dex_probe_queue={}", candidates.len(), candidates.len());

    // Every actual DEX request is globally paced. Handshakes are not charged to this gate.
    let verify = stream::iter(candidates).map(|candidate| {
        let shared = shared.clone();
        async move {
            let outcome = async {
                let client = proxy_client(&candidate)?;
                let host = shared.settings.probe_url.host_str().context("missing probe hostname")?;
                host_limit_at(&shared, host, shared.settings.probe_rps).await;
                let started = Instant::now();
                let response = client.get(shared.settings.probe_url.clone())
                    .timeout(shared.settings.probe_timeout).send().await?;
                ensure!(response.status() == StatusCode::OK, "probe status {}", response.status());
                ensure!(response.content_length().is_none_or(|n| n <= MAX_RESPONSE_BYTES as u64), "probe response too large");
                let value: Value = response.json().await?;
                let mint = shared.settings.probe_url.path_segments()
                    .and_then(|mut x| x.next_back()).context("probe URL without mint")?.to_owned();
                let valid = value.as_array().is_some_and(|pairs| pairs.iter().any(|p| {
                    p.get("chainId").and_then(Value::as_str) == Some("solana")
                        && ["baseToken", "quoteToken"].iter().any(|key| {
                            p.get(*key).and_then(|v| v.get("address")).and_then(Value::as_str) == Some(mint.as_str())
                        })
                }));
                ensure!(valid, "probe response has no matching Solana pool");
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
            println!("http_transport dex_tested={tested} dex_ok={ok}");
        }
    }
    publish(shared).await;
    println!("http_transport cycle_complete dex_tested={tested} dex_ok={ok} working={}", shared.snapshots.borrow().len());
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

async fn execute(shared: &Shared, request: HttpRequest) -> Result<HttpResponse> {
    ensure!(matches!(request.url.scheme(), "http" | "https"), "HTTP request requires http(s) URL");
    ensure!(request.url.username().is_empty() && request.url.password().is_none(), "credentials in request URL forbidden");
    ensure!(!request.timeout.is_zero(), "request timeout must be positive");
    let host = request.url.host_str().context("request URL missing host")?.to_owned();
    let (route_id, client) = match request.policy {
        RoutePolicy::DirectOnly => ("direct".to_owned(), shared.direct_client.clone()),
        RoutePolicy::ProxyOnly => {
            let snapshot = Arc::clone(&*shared.snapshots.borrow());
            let routes: Vec<_> = snapshot.iter()
                .filter(|r| r.verified_age_ms < shared.settings.healthy_for.as_millis() as u64)
                .take(64).collect();
            ensure!(!routes.is_empty(), "no verified proxy routes; direct fallback forbidden");
            let start = shared.selection.fetch_add(1, Ordering::Relaxed) % routes.len();
            let state = shared.inventory.lock().await;
            let selected = (0..routes.len()).find_map(|offset| {
                let id = &routes[(start + offset) % routes.len()].id;
                let route = state.get(id)?;
                if !route.verified.is_some_and(|at| at.elapsed() < shared.settings.healthy_for) {
                    return None;
                }
                Some((id.to_owned(), route.client.clone()?))
            }).context("no currently verified proxy; direct fallback forbidden")?;
            selected
        }
    };
    host_limit_at(shared, &host, shared.settings.request_rps_per_host).await;
    let started = Instant::now();
    let mut builder = client.request(request.method, request.url)
        .headers(request.headers)
        .timeout(request.timeout);
    if let Some(body) = request.body { builder = builder.body(body); }
    let reply = builder.send().await;
    let response = match reply {
        Ok(response) => response,
        Err(error) => {
            if route_id != "direct" && (error.is_connect() || error.is_timeout()) {
                let mut state = shared.inventory.lock().await;
                if let Some(route) = state.get_mut(&route_id) {
                    route.client = None;
                    route.verified = None;
                    route.next_check = Instant::now() + shared.settings.retry_failure_after;
                }
                drop(state);
                publish(shared).await;
            }
            return Err(anyhow!("HTTP transport route={route_id}: {}", error.without_url()));
        }
    };
    let status = response.status();
    if status == StatusCode::TOO_MANY_REQUESTS {
        let retry = response.headers().get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(5).clamp(1, 3600);
        let mut gates = shared.host_gates.lock().await;
        let t = Instant::now() + Duration::from_secs(retry);
        let entry = gates.entry(host).or_insert(t);
        if *entry < t { *entry = t; }
    }
    ensure!(response.content_length().is_none_or(|n| n <= MAX_RESPONSE_BYTES as u64), "response exceeds size cap");
    let body = response.bytes().await.context("HTTP response body")?;
    ensure!(body.len() <= MAX_RESPONSE_BYTES, "response exceeds size cap");
    if route_id != "direct" && status != StatusCode::TOO_MANY_REQUESTS {
        let mut state = shared.inventory.lock().await;
        if let Some(route) = state.get_mut(&route_id) {
            route.verified = Some(Instant::now());
            route.latency_ms = started.elapsed().as_millis() as u64;
            route.next_check = Instant::now() + shared.settings.refresh;
        }
    }
    Ok(HttpResponse {
        status, body, route_id, elapsed_ms: started.elapsed().as_millis() as u64,
    })
}
