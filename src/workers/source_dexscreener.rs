//! CP2: finite, read-only DexScreener Solana pair enrichment probe.
//! Own typed broadcast output; deliberately NOT connected to Pool Registry or Graph.
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env, fs::File, io::{BufWriter, Write}, path::PathBuf,
    sync::Arc, time::{Duration, Instant as StdInstant},
};
use anyhow::{Context, Result, ensure, bail};
use futures_util::{stream, StreamExt};
use reqwest::StatusCode;
use serde_json::{json, Value};
use solana_pubkey::Pubkey;
use tokio::{sync::broadcast, time::sleep, task::JoinHandle};
use tokio_postgres::{Client as Db, NoTls};

use super::http_transport::{self, HttpHandle, HttpRequest, RoutePolicy};
use crate::core::now_unix_ms;

const SOURCE: &str = "dexscreener_token_pairs";
const HOST: &str = "api.dexscreener.com";
const MAX_MINTS: usize = 10_000;
const MAX_REQUEST_ATTEMPTS: usize = 3;
const WINDOWS: [&str; 4] = ["m5", "h1", "h6", "h24"];

#[derive(Clone, Debug, Default)]
pub struct WindowMetrics {
    pub volume_usd: Option<f64>,
    pub price_change_pct: Option<f64>,
    pub buys: Option<u64>,
    pub sells: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct DexPair {
    pub address: Pubkey,
    pub dex_id: String,
    pub labels: Vec<String>,
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
    pub price_native: Option<String>,
    pub price_usd: Option<String>,
    pub liquidity_usd: Option<f64>,
    pub liquidity_base: Option<f64>,
    pub liquidity_quote: Option<f64>,
    pub market_cap_usd: Option<f64>,
    pub fdv_usd: Option<f64>,
    pub created_at_ms: Option<u64>,
    pub windows: [WindowMetrics; 4],
    /// Preserve provider-specific fields and nested metadata without coercion.
    pub raw: Value,
}

#[derive(Debug)]
pub struct DexPairBatch {
    pub requested_mint: Pubkey,
    pub received_at_ms: u64,
    pub route_id: String,
    pub proxy_ip: String,
    pub pairs: Vec<DexPair>,
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub mint_limit: usize,
    pub parallel: usize,
    pub per_proxy_ip_rpm: u32,
    pub max_proxy_latency_ms: u64,
    pub report_file: PathBuf,
    pub proxy_wait: Duration,
}
impl Settings {
    pub fn from_env() -> Result<Self> {
        let mint_limit = env::var("DEXSCREENER_MINT_LIMIT")
            .unwrap_or_else(|_| "100".into()).parse::<usize>()
            .context("DEXSCREENER_MINT_LIMIT")?;
        let parallel = env::var("DEXSCREENER_PARALLEL")
            .unwrap_or_else(|_| "48".into()).parse::<usize>()
            .context("DEXSCREENER_PARALLEL")?;
        let per_proxy_ip_rpm=env::var("DEXSCREENER_PROXY_IP_RPM")
            .unwrap_or_else(|_| "280".into()).parse::<u32>()
            .context("DEXSCREENER_PROXY_IP_RPM")?;
        let max_proxy_latency_ms=env::var("DEXSCREENER_PROXY_MAX_LATENCY_MS")
            .unwrap_or_else(|_| "5000".into()).parse::<u64>()
            .context("DEXSCREENER_PROXY_MAX_LATENCY_MS")?;
        let proxy_wait = env::var("DEXSCREENER_PROXY_WAIT_SECONDS")
            .unwrap_or_else(|_| "900".into()).parse::<u64>()
            .context("DEXSCREENER_PROXY_WAIT_SECONDS")?;
        let report_file = PathBuf::from(env::var("DEXSCREENER_REPORT_PATH")
            .unwrap_or_else(|_| "target/dexscreener_cp2.jsonl".into()));
        ensure!((1..=MAX_MINTS).contains(&mint_limit),
            "DEXSCREENER_MINT_LIMIT must be 1..=10000 for CP2");
        ensure!((1..=128).contains(&parallel), "DEXSCREENER_PARALLEL must be 1..=128");
        ensure!((1..=3600).contains(&per_proxy_ip_rpm),
            "DEXSCREENER_PROXY_IP_RPM must be 1..=3600");
        ensure!((1..=60000).contains(&max_proxy_latency_ms),
            "DEXSCREENER_PROXY_MAX_LATENCY_MS must be 1..=60000");
        ensure!((1..=1800).contains(&proxy_wait),
            "DEXSCREENER_PROXY_WAIT_SECONDS must be 1..=1800");
        ensure!(!report_file.as_os_str().is_empty(), "empty DEXSCREENER_REPORT_PATH");
        Ok(Self { mint_limit, parallel, per_proxy_ip_rpm,max_proxy_latency_ms,
            report_file, proxy_wait: Duration::from_secs(proxy_wait) })
    }
}

fn key(value: Option<&Value>) -> Result<Pubkey> {
    let text = value.and_then(Value::as_str).context("pair mint/address absent")?;
    let address = text.parse::<Pubkey>().context("invalid Solana base58 address")?;
    ensure!(address != Pubkey::default(), "zero Solana address");
    Ok(address)
}
fn number(v: Option<&Value>) -> Option<f64> {
    let v = v?;
    let f = v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok())?;
    f.is_finite().then_some(f)
}
fn nonnegative(v: Option<&Value>) -> Option<f64> {
    number(v).filter(|v| *v >= 0.0)
}
fn num_text(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}
fn count(v: Option<&Value>) -> Option<u64> {
    let v=v?;
    v.as_u64().or_else(||v.as_str()?.parse::<u64>().ok())
}
fn parse_pair(item: Value, requested: Pubkey) -> Result<DexPair> {
    ensure!(item.get("chainId").and_then(Value::as_str)==Some("solana"),
        "non-Solana DexScreener pair");
    let address=key(item.get("pairAddress"))?;
    let mint_a=key(item.pointer("/baseToken/address"))?;
    let mint_b=key(item.pointer("/quoteToken/address"))?;
    ensure!(mint_a!=mint_b && (mint_a==requested || mint_b==requested),
        "DexScreener pair does not include requested mint");
    let dex_id=item.get("dexId").and_then(Value::as_str)
        .filter(|value|!value.is_empty()).context("DexScreener dexId absent")?.to_owned();
    let labels=item.get("labels").and_then(Value::as_array).map(|arr|
        arr.iter().filter_map(Value::as_str).map(ToOwned::to_owned).collect()
    ).unwrap_or_default();
    let mut windows:[WindowMetrics;4]=std::array::from_fn(|_| WindowMetrics::default());
    for (i,name) in WINDOWS.iter().enumerate() {
        windows[i]=WindowMetrics {
            volume_usd:nonnegative(item.get("volume").and_then(|v|v.get(*name))),
            price_change_pct:number(item.get("priceChange").and_then(|v|v.get(*name))),
            buys:count(item.get("txns").and_then(|v|v.get(*name))
                .and_then(|v|v.get("buys"))),
            sells:count(item.get("txns").and_then(|v|v.get(*name))
                .and_then(|v|v.get("sells"))),
        };
    }
    Ok(DexPair {
        address,dex_id,labels,mint_a,mint_b,
        price_native:num_text(item.get("priceNative")),
        price_usd:num_text(item.get("priceUsd")),
        liquidity_usd:nonnegative(item.pointer("/liquidity/usd")),
        liquidity_base:nonnegative(item.pointer("/liquidity/base")),
        liquidity_quote:nonnegative(item.pointer("/liquidity/quote")),
        market_cap_usd:nonnegative(item.get("marketCap")),
        fdv_usd:nonnegative(item.get("fdv")),
        created_at_ms:count(item.get("pairCreatedAt")),
        windows,raw:item,
    })
}

struct Decoded {pairs:Vec<DexPair>,rejected:usize}
fn decode(body:&[u8],requested:Pubkey)->Result<Decoded> {
    let values:Vec<Value>=serde_json::from_slice(body)
        .context("DexScreener response must be JSON array")?;
    let mut pairs=Vec::with_capacity(values.len());
    let mut rejected=0;
    for value in values {
        match parse_pair(value,requested) {
            Ok(pair)=>pairs.push(pair),
            Err(error)=>{
                rejected+=1;
                if rejected<=3 {
                    crate::log_event!(Warn,"dexscreener invalid_pair mint={requested} error={error:#}");
                }
            }
        }
    }
    Ok(Decoded{pairs,rejected})
}

async fn connect_db()->Result<(Db,JoinHandle<()>)> {
    let url=env::var("LIQ_GRAPH_DATABASE_URL").or_else(|_|env::var("DATABASE_URL"))
        .context("LIQ_GRAPH_DATABASE_URL or DATABASE_URL required")?;
    let (db,conn)=tokio_postgres::connect(&url,NoTls)
        .await.context("DexScreener database connect")?;
    let task=tokio::spawn(async move {
        if let Err(error)=conn.await {
            crate::log_event!(Error,"dexscreener database connection error={error:#}");
        }
    });
    Ok((db,task))
}
fn decode_db_key(bytes:Vec<u8>)->Result<Pubkey> {
    let bytes:[u8;32]=bytes.try_into()
        .map_err(|_|anyhow::anyhow!("invalid 32-byte Solana key in DB"))?;
    Ok(Pubkey::new_from_array(bytes))
}
async fn mint_sample(db:&Db, limit:usize)->Result<Vec<Pubkey>> {
    // Retain the original 100-Mint test's deterministic Prepared-pool sample.
    // If there are fewer Prepared mints than requested, fill from the remaining
    // distinct pool-catalog mints without duplicating any address.
    const PREPARED:&str="SELECT mint FROM (
        SELECT mint_a AS mint FROM public.liq_graph_pool_cache
            WHERE status=1 AND mint_a IS NOT NULL
        UNION
        SELECT mint_b AS mint FROM public.liq_graph_pool_cache
            WHERE status=1 AND mint_b IS NOT NULL
    ) AS m ORDER BY mint LIMIT $1";
    const ALL:&str="SELECT mint FROM (
        SELECT mint_a AS mint FROM public.liq_graph_pool_cache WHERE mint_a IS NOT NULL
        UNION
        SELECT mint_b AS mint FROM public.liq_graph_pool_cache WHERE mint_b IS NOT NULL
    ) AS m ORDER BY mint LIMIT $1";
    let rows=db.query(PREPARED,&[&(limit as i64)]).await
        .context("read distinct Prepared-pool mints")?;
    let mut result=Vec::with_capacity(limit);
    let mut seen=HashSet::with_capacity(limit);
    for row in rows {
        let mint=decode_db_key(row.get::<_,Vec<u8>>(0))?;
        if seen.insert(mint){result.push(mint);}
    }
    let from_prepared=result.len();
    if result.len()<limit {
        let rows=db.query(ALL,&[&(limit as i64)]).await
            .context("fill benchmark from distinct known pool mints")?;
        for row in rows {
            let mint=decode_db_key(row.get::<_,Vec<u8>>(0))?;
            if seen.insert(mint){result.push(mint);}
            if result.len()==limit {break;}
        }
    }
    ensure!(result.len()==limit,
        "only {} unique pool mints available; {limit} requested",result.len());
    crate::log_event!(Info,
        "dexscreener mint sample selected={} prepared_mints={} other_pool_mints={}",
        result.len(),from_prepared,result.len()-from_prepared);
    Ok(result)
}

#[derive(Default)]
struct Counters {
    ok:usize,empty:usize,failed:usize,rejected:usize,
    http_429:usize,http_403:usize,http_5xx:usize,retries:usize,
    bytes:usize,
    routes:HashSet<String>,
    proxy_ip_counts:HashMap<String,usize>,
}
struct RequestResult {
    mint:Pubkey,
    response:Option<(String,String,Vec<DexPair>,usize,usize)>,
    attempts:usize,
    count_429:usize,
    count_403:usize,
    count_5xx:usize,
    error:Option<String>,
}
async fn fetch_one(handle:HttpHandle,mint:Pubkey)->RequestResult {
    let url=format!("https://{HOST}/token-pairs/v1/solana/{mint}");
    let mut result=RequestResult {
        mint,response:None,attempts:0,count_429:0,count_403:0,count_5xx:0,error:None,
    };
    for attempt in 0..MAX_REQUEST_ATTEMPTS {
        result.attempts+=1;
        let request=match HttpRequest::get(&url,RoutePolicy::ProxyOnly) {
            Ok(value)=>value,
            Err(error)=>{result.error=Some(format!("{error:#}"));break;},
        };
        match handle.call(request).await {
            Ok(response) => {
                if response.status==StatusCode::TOO_MANY_REQUESTS {
                    result.count_429+=1;
                    result.error=Some("HTTP 429".into());
                    crate::log_event!(Debug,"dexscreener http_429 mint={mint} attempt={}",attempt+1);
                    if attempt+1<MAX_REQUEST_ATTEMPTS {sleep(Duration::from_secs(5)).await;}
                    continue;
                }
                if response.status==StatusCode::FORBIDDEN {result.count_403+=1;}
                if response.status.is_server_error() {result.count_5xx+=1;}
                if !response.status.is_success() {
                    result.error=Some(format!("HTTP {}",response.status));
                    if response.status.is_server_error() && attempt+1<MAX_REQUEST_ATTEMPTS {
                        sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    break;
                }
                match decode(&response.body,mint) {
                    Ok(decoded)=>{
                        result.response=Some((response.route_id,response.proxy_ip,
                            decoded.pairs,decoded.rejected,response.body.len()));
                        result.error=None;
                        break;
                    }
                    Err(error)=>result.error=Some(format!("decode: {error:#}")),
                }
            }
            Err(error)=>result.error=Some(format!("transport: {error:#}")),
        }
        if attempt+1<MAX_REQUEST_ATTEMPTS {sleep(Duration::from_secs(2)).await;}
    }
    result
}
async fn fetch_all(handle:HttpHandle,mints:Vec<Pubkey>,parallel:usize,
    tx:broadcast::Sender<Arc<DexPairBatch>>)->Result<()> {
    let started=StdInstant::now();
    let total=mints.len();
    let mut stats=Counters::default();
    let mut last_report=StdInstant::now();
    let mut last_report_done=0usize;
    let jobs=stream::iter(mints).map(|mint|{
        let handle=handle.clone();
        async move {fetch_one(handle,mint).await}
    }).buffer_unordered(parallel);
    tokio::pin!(jobs);
    while let Some(result)=jobs.next().await {
        stats.http_429+=result.count_429;
        stats.http_403+=result.count_403;
        stats.http_5xx+=result.count_5xx;
        stats.retries+=result.attempts.saturating_sub(1);
        if result.count_429>0 && stats.http_429<=5 {
            crate::log_event!(Warn,
                "dexscreener http_429 mint={} occurrences={} total_429={}",
                result.mint,result.count_429,stats.http_429);
        }
        if let Some((route,proxy_ip,pairs,rejected,bytes))=result.response {
            stats.ok+=1;
            stats.empty+=usize::from(pairs.is_empty());
            stats.rejected+=rejected;
            stats.bytes+=bytes;
            stats.routes.insert(route.clone());
            *stats.proxy_ip_counts.entry(proxy_ip.clone()).or_default()+=1;
            tx.send(Arc::new(DexPairBatch {
                requested_mint:result.mint,
                received_at_ms:now_unix_ms()?,
                route_id:route,proxy_ip,
                pairs,
            })).map_err(|_|anyhow::anyhow!("DexScreener typed stream has no reader"))?;
        } else {
            stats.failed+=1;
            if stats.failed<=5 {
                crate::log_event!(Warn,
                    "dexscreener mint_failed mint={} attempts={} reason={}",
                    result.mint,result.attempts,result.error.as_deref().unwrap_or("unknown"));
            }
        }
        let done=stats.ok+stats.failed;
        let seconds=last_report.elapsed().as_secs_f64();
        if done<total && (done-last_report_done>=1000 || seconds>=30.0) {
            crate::log_event!(Info,
                "dexscreener progress completed={done}/{total} ok={} failed={} http_429={} http_403={} http_5xx={} retries={} proxy_ips={} window_rps={:.2} mean_rps={:.2}",
                stats.ok,stats.failed,stats.http_429,stats.http_403,
                stats.http_5xx,stats.retries,stats.proxy_ip_counts.len(),
                (done-last_report_done) as f64/seconds.max(0.001),
                done as f64/started.elapsed().as_secs_f64().max(0.001));
            last_report=StdInstant::now();
            last_report_done=done;
        }
    }
    let elapsed=started.elapsed().as_secs_f64();
    let mut per_ip=stats.proxy_ip_counts.values().copied().collect::<Vec<_>>();
    per_ip.sort_unstable();
    let max_per_ip=per_ip.last().copied().unwrap_or(0);
    let p50_per_ip=per_ip.get(per_ip.len().saturating_sub(1)/2).copied().unwrap_or(0);
    let p95_per_ip=per_ip.get(per_ip.len().saturating_sub(1)*95/100).copied().unwrap_or(0);
    crate::log_event!(Info,
        "dexscreener source complete requests={total} success={} empty={} failed={} invalid_pairs={} retries={} http_429={} http_403={} http_5xx={} proxy_routes={} proxy_ips={} success_per_ip_p50={} success_per_ip_p95={} success_per_ip_max={} response_bytes={} elapsed_s={:.2} success_rps={:.2}",
        stats.ok,stats.empty,stats.failed,stats.rejected,stats.retries,
        stats.http_429,stats.http_403,stats.http_5xx,
        stats.routes.len(),stats.proxy_ip_counts.len(),p50_per_ip,p95_per_ip,max_per_ip,
        stats.bytes,elapsed,stats.ok as f64/elapsed.max(0.001));
    ensure!(stats.ok==total,
        "DexScreener full benchmark incomplete: success={} of {total}; failed={}",stats.ok,stats.failed);
    Ok(())
}

struct RegistryPool {
    venue:i16,
    mint_a:Option<Pubkey>,
    mint_b:Option<Pubkey>,
}
async fn known_pools(db:&Db,pairs:&[Pubkey])->Result<HashMap<Pubkey,RegistryPool>> {
    let mut out=HashMap::new();
    for group in pairs.chunks(1000) {
        let ids=group.iter().map(|p|p.to_bytes().to_vec()).collect::<Vec<_>>();
        let rows=db.query(
            "SELECT pool_address,venue,mint_a,mint_b FROM public.liq_graph_pool_cache
             WHERE pool_address=ANY($1::bytea[])",
             &[&ids]).await.context("resolve DexScreener pair against Pool Registry")?;
        for row in rows {
            let address=decode_db_key(row.get("pool_address"))?;
            let mint_a=row.get::<_,Option<Vec<u8>>>("mint_a")
                .map(decode_db_key).transpose()?;
            let mint_b=row.get::<_,Option<Vec<u8>>>("mint_b")
                .map(decode_db_key).transpose()?;
            out.insert(address,RegistryPool {
                venue:row.get("venue"),mint_a,mint_b,
            });
        }
    }
    Ok(out)
}
fn window_json(value:&WindowMetrics)->Value {
    json!({"volumeUsd":value.volume_usd,
        "priceChangePct":value.price_change_pct,
        "buys":value.buys,"sells":value.sells})
}
async fn observe(mut rx:broadcast::Receiver<Arc<DexPairBatch>>,db:Db,
    report_file:PathBuf)->Result<()> {
    let mut pairs=HashMap::<Pubkey,(Pubkey,u64,String,String,DexPair)>::new();
    let mut received=0;
    let mut repeated=0usize;
    loop {
        match rx.recv().await {
            Ok(batch)=>{
                received+=1;
                for pair in &batch.pairs {
                    if pairs.contains_key(&pair.address) {
                        repeated+=1;
                    } else {
                        pairs.insert(pair.address,(batch.requested_mint,
                            batch.received_at_ms,batch.route_id.clone(),
                            batch.proxy_ip.clone(),pair.clone()));
                    }
                }
            }
            Err(broadcast::error::RecvError::Closed)=>break,
            Err(broadcast::error::RecvError::Lagged(n))=>
                bail!("DexScreener typed observer lagged {n} batches"),
        }
    }
    let mut addresses=pairs.keys().copied().collect::<Vec<_>>();
    addresses.sort_unstable();
    let known=known_pools(&db,&addresses).await?;
    if let Some(parent)=report_file.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).context("create DexScreener report directory")?;
        }
    }
    let file=File::create(&report_file).context("create DexScreener JSONL report")?;
    let mut writer=BufWriter::new(file);
    let mut dex_counts=BTreeMap::<String,usize>::new();
    let (mut known_pairs,mut mismatched,mut liquid,mut price,mut price_change,mut volume,
         mut transactions)=(0,0,0,0,0,0,0);
    for address in addresses {
        let (requested,at,route,proxy_ip,pair)=pairs.remove(&address).context("missing decoded pair")?;
        *dex_counts.entry(pair.dex_id.clone()).or_default()+=1;
        let db_pool=known.get(&address);
        if db_pool.is_some(){known_pairs+=1;}
        let identity_match=db_pool.map(|p|{
            let direct=p.mint_a==Some(pair.mint_a)&&p.mint_b==Some(pair.mint_b);
            let reverse=p.mint_a==Some(pair.mint_b)&&p.mint_b==Some(pair.mint_a);
            if p.mint_a.is_none()||p.mint_b.is_none(){None}
            else {Some(direct||reverse)}
        }).flatten();
        if identity_match==Some(false){mismatched+=1;}
        liquid+=usize::from(pair.liquidity_usd.is_some());
        price+=usize::from(pair.price_usd.is_some());
        price_change+=usize::from(pair.windows.iter().any(|w|w.price_change_pct.is_some()));
        volume+=usize::from(pair.windows.iter().any(|w|w.volume_usd.is_some()));
        transactions+=usize::from(pair.windows.iter().any(|w|w.buys.is_some()||w.sells.is_some()));
        let entry=json!({
            "source":SOURCE, "requestedMint":requested.to_string(),
            "receivedAtMs":at,"routeId":route,"proxyIp":proxy_ip,
            "pairAddress":pair.address.to_string(),"dexId":pair.dex_id,
            "labels":pair.labels,
            "baseMint":pair.mint_a.to_string(),"quoteMint":pair.mint_b.to_string(),
            "knownInRegistry":db_pool.is_some(),
            "registryVenue":db_pool.map(|p|p.venue),
            "mintIdentityMatch":identity_match,
            "priceNative":pair.price_native,"priceUsd":pair.price_usd,
            "liquidityUsd":pair.liquidity_usd,
            "liquidityBase":pair.liquidity_base,"liquidityQuote":pair.liquidity_quote,
            "marketCapUsd":pair.market_cap_usd,"fdvUsd":pair.fdv_usd,
            "pairCreatedAtMs":pair.created_at_ms,
            "m5":window_json(&pair.windows[0]),"h1":window_json(&pair.windows[1]),
            "h6":window_json(&pair.windows[2]),"h24":window_json(&pair.windows[3]),
            "raw":pair.raw,
        });
        serde_json::to_writer(&mut writer,&entry).context("write DexScreener pair")?;
        writer.write_all(b"\n").context("write DexScreener JSONL newline")?;
    }
    writer.flush().context("flush DexScreener JSONL")?;
    let total=dex_counts.values().sum::<usize>();
    crate::log_event!(Info,
        "dexscreener decode complete batches={received} unique_pairs={total} repeated_pairs={repeated} known_pools={known_pairs} unknown_pools={} mint_identity_mismatches={mismatched} with_liquidity={liquid} with_price_usd={price} with_price_change={price_change} with_volume={volume} with_txns={transactions} dex_ids={dex_counts:?} report={}",
        total-known_pairs,report_file.display());
    Ok(())
}

/// Standalone finite CP2 checkpoint; owns the only typed stream consumer.
pub async fn live_diagnostic()->Result<()> {
    let settings=Settings::from_env()?;
    let (db,db_task)=connect_db().await?;
    let mints=mint_sample(&db,settings.mint_limit).await?;
    let mut transport=http_transport::Settings::from_env()?;
    // Keep longer benchmarks readable: regular transport snapshots once/minute,
    // with source progress reported every 1000 completions / 30 seconds.
    if settings.mint_limit>=1000 {
        transport.stats_interval=Duration::from_secs(60);
    }
    // CP2 benchmark mode: no shared 4-RPS host gate. Every verified proxy
    // address gets its own independently paced 280-RPM budget (configurable).
    // Multiple proxy ports on the same address share that budget.
    transport.host_rps_overrides.insert(HOST.into(),0);
    transport.proxy_ip_rpm_overrides.insert(HOST.into(),settings.per_proxy_ip_rpm);
    transport.proxy_latency_ceiling_ms.insert(HOST.into(),settings.max_proxy_latency_ms);
    let (handle,task)=http_transport::spawn(transport)?;
    let mut cycles=handle.subscribe_cycles();
    tokio::time::timeout(settings.proxy_wait,async {
        loop {
            if *cycles.borrow_and_update()>0 {break Ok::<(),anyhow::Error>(());}
            cycles.changed().await.context("DexScreener proxy discovery closed")?;
        }
    }).await.context("DexScreener proxy discovery timeout")??;
    ensure!(!handle.working_set().is_empty(),
        "no verified HTTP proxies for DexScreener");
    crate::log_event!(Info,
        "dexscreener CP2 started selected_mints={} proxy_routes={} global_host_cap=disabled proxy_ip_rpm={} parallel={} preferred_proxy_latency_ms={} read_only=true",
        mints.len(),handle.working_set().len(),
        settings.per_proxy_ip_rpm,settings.parallel,settings.max_proxy_latency_ms);
    let (tx,rx)=broadcast::channel::<Arc<DexPairBatch>>(128);
    let observer=tokio::spawn(observe(rx,db,settings.report_file));
    let result=fetch_all(handle.clone(),mints,settings.parallel,tx.clone()).await;
    drop(tx);
    let observed=observer.await.context("DexScreener observer task panicked")?;
    drop(cycles);drop(handle);
    task.await.context("DexScreener HTTP transport task panicked")??;
    db_task.await.context("DexScreener DB connection task panicked")?;
    result?;
    observed?;
    Ok(())
}

/// CP1: long-running, typed DexScreener enrichment for economically selected mints.
/// Unknown pool addresses are not invented: their venue and canonical identities
/// must already exist in PoolRegistry. The finite diagnostic entrypoint is retained.
pub async fn run_live(
    mut qualified: tokio::sync::watch::Receiver<Arc<[Pubkey]>>,
    registry: super::pool_registry::PoolRegistry,
    output: broadcast::Sender<Arc<crate::core::DataBatch>>,
) -> Result<()> {
    use crate::core::{DataBatch, DataRecord, RecordIdentity};
    let settings=Settings::from_env()?;
    let refresh_secs=env::var("DEXSCREENER_REFRESH_SECONDS")
        .unwrap_or_else(|_|"1800".into()).parse::<u64>()
        .context("DEXSCREENER_REFRESH_SECONDS")?;
    ensure!((60..=86400).contains(&refresh_secs),
        "DEXSCREENER_REFRESH_SECONDS must be 60..86400");
    let mut transport=http_transport::Settings::from_env()?;
    transport.stats_interval=Duration::from_secs(60);
    transport.host_rps_overrides.insert(HOST.into(),0);
    transport.proxy_ip_rpm_overrides.insert(HOST.into(),settings.per_proxy_ip_rpm);
    transport.proxy_latency_ceiling_ms.insert(HOST.into(),settings.max_proxy_latency_ms);
    let (handle,transport_task)=http_transport::spawn(transport)?;
    let mut cycles=handle.subscribe_cycles();
    tokio::time::timeout(settings.proxy_wait,async {
        loop {
            if *cycles.borrow_and_update()>0 {break Ok::<(),anyhow::Error>(());}
            cycles.changed().await.context("DexScreener proxy discovery closed")?;
        }
    }).await.context("DexScreener proxy discovery timeout")??;
    ensure!(!handle.working_set().is_empty(),"no verified DexScreener HTTP proxies");
    let (tx,mut rx)=broadcast::channel::<Arc<DexPairBatch>>(256);
    let publish=tokio::spawn(async move {
        let mut recent=HashMap::<Pubkey,u64>::new();
        loop {
            let batch=match rx.recv().await {
                Ok(batch)=>batch,
                Err(broadcast::error::RecvError::Closed)=>break,
                Err(broadcast::error::RecvError::Lagged(n))=>
                    bail!("DexScreener enrichment lost {n} batches"),
            };
            let mut rows=Vec::new();
            for pair in &batch.pairs {
                let Some(tvl)=pair.liquidity_usd else {continue};
                let Some(pool)=registry.get(pair.address) else {continue};
                let same=pool.mint_a==Some(pair.mint_a)&&pool.mint_b==Some(pair.mint_b);
                let reverse=pool.mint_a==Some(pair.mint_b)&&pool.mint_b==Some(pair.mint_a);
                if !same && !reverse {continue;}
                if recent.get(&pair.address).is_some_and(|previous|
                    batch.received_at_ms.saturating_sub(*previous)<60_000) {continue;}
                recent.insert(pair.address,batch.received_at_ms);
                rows.push(DataRecord {
                    identity:RecordIdentity::Pool {
                        address:pool.address,venue:pool.venue,mint_a:pool.mint_a,mint_b:pool.mint_b,
                    },
                    tvl_usd:Some(tvl),
                    payload:json!({"dexId":pair.dex_id,"liquidityUsd":tvl,
                        "requestedMint":batch.requested_mint.to_string()}),
                });
            }
            if !rows.is_empty() {
                output.send(Arc::new(DataBatch {
                    source:SOURCE,received_at_ms:batch.received_at_ms,records:rows,
                })).map_err(|_|anyhow::anyhow!("Registry discovery receiver closed"))?;
            }
            if recent.len()>100_000 {
                recent.retain(|_,t|batch.received_at_ms.saturating_sub(*t)<60_000);
            }
        }
        Ok::<(),anyhow::Error>(())
    });
    // Each mint has its own next-due timestamp. No fixed-size request rounds:
    // one HTTP request per mint, kept continuously in flight as transport allows.
    // Existing deadlines survive membership changes in the qualified watch set.
    let failure_retry_secs=env::var("DEXSCREENER_FAILURE_RETRY_SECONDS")
        .unwrap_or_else(|_|"120".into()).parse::<u64>()
        .context("DEXSCREENER_FAILURE_RETRY_SECONDS")?;
    ensure!((10..=3600).contains(&failure_retry_secs),
        "DEXSCREENER_FAILURE_RETRY_SECONDS must be 10..3600");
    let refresh=Duration::from_secs(refresh_secs);
    let retry=Duration::from_secs(failure_retry_secs);
    let mut active=HashSet::<Pubkey>::new();
    let mut inflight=HashSet::<Pubkey>::new();
    let mut due=std::collections::BTreeSet::<(tokio::time::Instant,Pubkey)>::new();
    let mut tasks=tokio::task::JoinSet::<RequestResult>::new();
    let mut pulse=tokio::time::interval(Duration::from_secs(30));
    pulse.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut completed=0_u64;
    let mut failures=0_u64;
    let mut published_pairs=0_u64;
    let mut changes=0_u64;
    let mut source_open=true;
    // Initial snapshot also covers updates published before proxy discovery finished.
    let initial=qualified.borrow_and_update().clone();
    for &mint in initial.iter() {
        if active.insert(mint) {due.insert((tokio::time::Instant::now(),mint));}
    }
    crate::log_event!(Info,
        "dexscreener cp1 continuous started=true active_mints={} parallel={} refresh_s={} failure_retry_s={}",
        active.len(),settings.parallel,refresh_secs,failure_retry_secs);
    loop {
        // Dispatch every due mint until the concurrency limit is reached.
        // The shared HTTP transport owns proxy/rate limiting and backpressure.
        while tasks.len()<settings.parallel {
            let Some(&(at,mint))=due.first() else {break};
            if at>tokio::time::Instant::now(){break}
            due.pop_first();
            if !active.contains(&mint) || !inflight.insert(mint) {continue}
            let client=handle.clone();
            tasks.spawn(async move {fetch_one(client,mint).await});
        }
        if !source_open && tasks.is_empty() {break}
        let next_due=due.first().map(|&(at,_)|at);
        tokio::select! {
            update=qualified.changed(), if source_open=>{
                if update.is_err(){
                    source_open=false;
                    active.clear();
                    due.clear();
                } else {
                    let latest=qualified.borrow_and_update().clone();
                    let new_active:HashSet<Pubkey>=latest.iter().copied().collect();
                    let now=tokio::time::Instant::now();
                    due.retain(|(_,mint)|new_active.contains(mint));
                    let mut added=0_usize;
                    for &mint in &new_active {
                        if !active.contains(&mint) && !inflight.contains(&mint) {
                            due.insert((now,mint));
                            added+=1;
                        }
                    }
                    let removed=active.len().saturating_sub(active.intersection(&new_active).count());
                    active=new_active;
                    changes+=1;
                    crate::log_event!(Info,
                        "dexscreener cp1 selection_changed active_mints={} added={} removed={} queued={} in_flight={} change={changes}",
                        active.len(),added,removed,due.len(),tasks.len());
                }
            },
            finished=tasks.join_next(), if !tasks.is_empty()=>{
                let result=finished.context("DexScreener CP1 request missing")?
                    .context("DexScreener CP1 request task panicked")?;
                inflight.remove(&result.mint);
                if !active.contains(&result.mint) {continue;}
                let now=tokio::time::Instant::now();
                if let Some((route,proxy_ip,pairs,_,_))=result.response {
                    published_pairs+=pairs.len() as u64;
                    completed+=1;
                    tx.send(Arc::new(DexPairBatch {
                        requested_mint:result.mint,
                        received_at_ms:now_unix_ms()?,
                        route_id:route,proxy_ip,pairs,
                    })).map_err(|_|anyhow::anyhow!("DexScreener enrichment publisher closed"))?;
                    due.insert((now+refresh,result.mint));
                } else {
                    failures+=1;
                    if failures<=5 || failures%100==0 {
                        crate::log_event!(Warn,
                            "dexscreener cp1 request_failed mint={} attempts={} total_failed={} reason={}",
                            result.mint,result.attempts,failures,result.error.as_deref().unwrap_or("unknown"));
                    }
                    due.insert((now+retry,result.mint));
                }
            },
            _=tokio::time::sleep_until(
                next_due.unwrap_or_else(||tokio::time::Instant::now()+Duration::from_secs(3600))
            ), if tasks.len()<settings.parallel && next_due.is_some()=>{},
            _=pulse.tick()=>{
                crate::log_event!(Info,
                    "dexscreener cp1 continuous active_mints={} queued={} in_flight={} completed={} failed={} pairs={} refresh_s={}",
                    active.len(),due.len(),tasks.len(),completed,failures,published_pairs,refresh_secs);
            }
        }
        if publish.is_finished() {
            publish.await.context("DexScreener enrichment publisher panicked")??;
            bail!("DexScreener enrichment publisher exited");
        }
    }
    drop(tx);
    publish.await.context("DexScreener enrichment publisher panicked")??;
    drop(cycles);drop(handle);
    transport_task.await.context("DexScreener transport panicked")??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    const SOL:&str="So11111111111111111111111111111111111111112";
    const USDC:&str="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
    #[test]
    fn missing_optional_metrics_do_not_invalidate_pair() {
        let mint:Pubkey=SOL.parse().unwrap();
        let body=format!(
            r#"[{{"chainId":"solana","dexId":"pumpfun","pairAddress":"{USDC}",
               "baseToken":{{"address":"{SOL}"}},"quoteToken":{{"address":"{USDC}"}},
               "priceNative":"0.00001","txns":{{"m5":{{"buys":0,"sells":0}}}},
               "priceChange":{{}}}}]"#);
        let decoded=decode(body.as_bytes(),mint).unwrap();
        assert_eq!(decoded.pairs.len(),1);
        assert_eq!(decoded.rejected,0);
        assert!(decoded.pairs[0].liquidity_usd.is_none());
        assert!(decoded.pairs[0].price_usd.is_none());
        assert!(decoded.pairs[0].windows[0].price_change_pct.is_none());
        assert_eq!(decoded.pairs[0].windows[0].buys,Some(0));
    }
}
