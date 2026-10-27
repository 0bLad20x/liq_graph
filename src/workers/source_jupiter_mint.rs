//! Single Jupiter Search HTTP owner: produces typed Mint DataBatches.
//! No PostgreSQL connection or implicit backfill; persistence consumes the stream.
use std::{collections::{HashSet, VecDeque}, env, sync::Arc, time::Duration};
use anyhow::{bail, Context, Result, ensure};
use reqwest::{Client, StatusCode};
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, watch}, task::JoinSet, time::{Instant, sleep_until}};

use crate::core::{DataBatch, DataRecord, RecordIdentity, now_unix_ms};

const SOURCE: &str = "jupiter_search";
const MAX_RETRIES: u8 = 3;
#[derive(Clone)]
pub struct Settings {
    pub url: String,
    pub api_keys: Vec<String>,
    pub calls_per_minute_per_key: f64,
    pub batch_size: usize,
    pub timeout: Duration,
    pub backfill_interval: Duration,
    pub backfill_limit: usize,
}
impl Settings {
    pub fn from_env() -> Result<Self> {
        let mut unique = HashSet::new();
        let api_keys = env::var("JUPITER_SEARCH_API_KEYS").unwrap_or_default()
            .split(|ch| ch == ',' || ch == '\n')
            .map(str::trim).filter(|key| !key.is_empty())
            .filter(|key| unique.insert((*key).to_owned()))
            .map(ToOwned::to_owned).collect::<Vec<_>>();
        ensure!(!api_keys.is_empty(), "JUPITER_SEARCH_API_KEYS required");
        let calls_per_minute_per_key = env::var("JUPITER_SEARCH_CALLS_PER_MINUTE")
            .unwrap_or_else(|_| "57".into()).parse::<f64>()?;
        let batch_size = env::var("JUPITER_SEARCH_BATCH_SIZE")
            .unwrap_or_else(|_| "100".into()).parse::<usize>()?;
        let seconds = env::var("JUPITER_SEARCH_REQUEST_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| "20".into()).parse::<u64>()?;
        let backfill_seconds = env::var("JUPITER_BACKFILL_INTERVAL_SECONDS")
            .unwrap_or_else(|_| "86400".into()).parse::<u64>()?;
        let backfill_limit = env::var("JUPITER_MINT_BACKFILL_LIMIT")
            .unwrap_or_else(|_| "0".into()).parse::<usize>()?;
        let url = env::var("JUPITER_SEARCH_URL")
            .unwrap_or_else(|_| "https://api.jup.ag/tokens/v2/search".into());
        ensure!(calls_per_minute_per_key.is_finite()
            && (0.0..=3600.0).contains(&calls_per_minute_per_key)
            && calls_per_minute_per_key > 0.0, "invalid Jupiter key RPM");
        ensure!((1..=100).contains(&batch_size) && seconds > 0
            && backfill_seconds > 0 && !url.trim().is_empty(),
            "invalid Jupiter batch, timeout, interval or URL");
        Ok(Self {url,api_keys,calls_per_minute_per_key,batch_size,
            timeout:Duration::from_secs(seconds),
            backfill_interval:Duration::from_secs(backfill_seconds),
            backfill_limit})
    }
}

struct Decoded {
    batch: DataBatch,
    found: usize,
    rejected: usize,
}
fn decode(body: &[u8], requested: &[Pubkey]) -> Result<Decoded> {
    let values: Vec<Value> = serde_json::from_slice(body)
        .context("invalid Jupiter Search JSON token array")?;
    let allowed=requested.iter().copied().collect::<HashSet<_>>();
    let mut seen=HashSet::new();
    let mut records=Vec::with_capacity(values.len());
    let mut rejected=0;
    for payload in values {
        let address=payload.get("id").and_then(Value::as_str)
            .and_then(|s|s.parse::<Pubkey>().ok());
        let Some(mint)=address.filter(|a|*a!=Pubkey::default()) else {
            rejected+=1;
            continue;
        };
        ensure!(allowed.contains(&mint),"Jupiter returned unrequested mint {mint}");
        if !seen.insert(mint) {continue;}
        records.push(DataRecord {
            identity:RecordIdentity::Mint{address:mint},tvl_usd:None,payload,
        });
    }
    let found=records.len();
    Ok(Decoded{batch:DataBatch{source:SOURCE,
        received_at_ms:now_unix_ms()?,records},found,rejected})
}
struct Lane { key:String, next_due:Instant, in_flight:bool }
struct Work {mints:Vec<Pubkey>, retry:u8}
enum Outcome {
    Success(Decoded),
    Retry {reason:String, delay:Duration},
}
struct Finished {lane:usize,work:Work,outcome:Outcome}
async fn request(http:Client,url:String,key:String,timeout:Duration,work:&Work)->Outcome {
    let query=work.mints.iter().map(ToString::to_string).collect::<Vec<_>>().join(",");
    let response=http.get(url).query(&[("query",query)])
        .header("x-api-key",key).timeout(timeout).send().await;
    let response=match response {
        Ok(value)=>value,
        Err(error)=>return Outcome::Retry{
            reason:format!("Jupiter transport: {}",error.without_url()),
            delay:Duration::from_secs(2)},
    };
    let status=response.status();
    if status==StatusCode::TOO_MANY_REQUESTS {
        let seconds=response.headers().get(reqwest::header::RETRY_AFTER)
            .and_then(|v|v.to_str().ok()).and_then(|v|v.parse::<u64>().ok())
            .unwrap_or(5).clamp(1,3600);
        return Outcome::Retry{reason:"Jupiter HTTP 429".into(),
            delay:Duration::from_secs(seconds)};
    }
    if !status.is_success() {
        return Outcome::Retry{reason:format!("Jupiter HTTP {status}"),
            delay:Duration::from_secs(2)};
    }
    let body=match response.bytes().await {
        Ok(value)=>value,
        Err(error)=>return Outcome::Retry{
            reason:format!("Jupiter body error: {error}"),
            delay:Duration::from_secs(2)},
    };
    match decode(&body,&work.mints) {
        Ok(value)=>Outcome::Success(value),
        Err(error)=>Outcome::Retry{reason:format!("Jupiter decode: {error:#}"),
            delay:Duration::from_secs(2)},
    }
}
struct Engine {
    http:Client, settings:Settings, lanes:Vec<Lane>,
    pending:JoinSet<Finished>,
}
impl Engine {
    async fn new(http:Client,settings:Settings)->Result<Self> {
        let period=Duration::from_secs_f64(60.0/settings.calls_per_minute_per_key);
        let phase=period.div_f64(settings.api_keys.len() as f64);
        let now=Instant::now();
        let lanes=settings.api_keys.iter().enumerate().map(|(i,key)|Lane {
            key:key.clone(),next_due:now+phase.mul_f64(i as f64),in_flight:false,
        }).collect::<Vec<_>>();
        crate::log_event!(Info,"jupiter unified started keys={} rpm_per_key={} batch={} watch_interval_s={}",
            lanes.len(),settings.calls_per_minute_per_key,settings.batch_size,
            settings.backfill_interval.as_secs());
        Ok(Self{http,settings,lanes,pending:JoinSet::new()})
    }
    fn dispatch(&mut self,lane:usize,work:Work) {
        let period=Duration::from_secs_f64(60.0/self.settings.calls_per_minute_per_key);
        self.lanes[lane].in_flight=true;
        self.lanes[lane].next_due=Instant::now()+period;
        let http=self.http.clone();let url=self.settings.url.clone();
        let key=self.lanes[lane].key.clone();let timeout=self.settings.timeout;
        self.pending.spawn(async move {
            let outcome=request(http,url,key,timeout,&work).await;
            Finished{lane,work,outcome}
        });
    }
    fn available_due(&self)->Option<Instant> {
        self.lanes.iter().filter(|lane|!lane.in_flight)
            .map(|lane|lane.next_due).min()
    }
    fn broadcast(output:&broadcast::Sender<Arc<DataBatch>>,batch:DataBatch) {
        let _=output.send(Arc::new(batch));
    }
    /// Finite source-only full scan. The orchestrator supplies known pool mints
    /// and Persistence independently subscribes to the published mint batches.
    async fn backfill(&mut self,mints:Vec<Pubkey>,
        output:&broadcast::Sender<Arc<DataBatch>>)->Result<()> {
        ensure!(self.pending.is_empty(),"Jupiter backfill overlaps active requests");
        let started=Instant::now();
        let mut queued=VecDeque::from(mints);
        let mut retries=VecDeque::<Work>::new();
        let (mut requested,mut found,mut not_found,mut retry_count)=(0_u64,0_u64,0_u64,0_u64);
        crate::log_event!(Info,"jupiter mode=backfill started mints={}",queued.len());
        while !queued.is_empty() || !retries.is_empty() || !self.pending.is_empty() {
            for lane in 0..self.lanes.len() {
                if self.lanes[lane].in_flight || Instant::now()<self.lanes[lane].next_due {continue;}
                let work=if let Some(work)=retries.pop_front(){work} else {
                    let n=queued.len().min(self.settings.batch_size);
                    if n==0{continue;}
                    Work{mints:queued.drain(..n).collect(),retry:0}
                };
                if work.retry==0{requested+=work.mints.len() as u64;}
                self.dispatch(lane,work);
            }
            let due=if !queued.is_empty()||!retries.is_empty(){self.available_due()}else{None};
            tokio::select! {
                result=self.pending.join_next(),if !self.pending.is_empty()=>{
                    let finished=result.context("missing Jupiter backfill response")??;
                    self.lanes[finished.lane].in_flight=false;
                    match finished.outcome {
                        Outcome::Success(decoded)=>{
                            found+=decoded.found as u64;
                            not_found+=(finished.work.mints.len()-decoded.found) as u64;
                            Self::broadcast(output,decoded.batch);
                        },
                        Outcome::Retry{reason,delay} if finished.work.retry<MAX_RETRIES=>{
                            retry_count+=1;
                            self.lanes[finished.lane].next_due=
                                self.lanes[finished.lane].next_due.max(Instant::now()+delay);
                            crate::log_event!(Warn,"Jupiter backfill retry={} lane={} reason={reason}",
                                finished.work.retry+1,finished.lane);
                            retries.push_back(Work{mints:finished.work.mints,
                                retry:finished.work.retry+1});
                        },
                        Outcome::Retry{reason,..}=>bail!(
                            "Jupiter scan incomplete after {MAX_RETRIES} retries: {reason}"),
                    }
                },
                _=sleep_until(due.unwrap_or_else(Instant::now)),if due.is_some()=>{},
            }
        }
        ensure!(requested==found+not_found,"Jupiter backfill accounting mismatch");
        crate::log_event!(Info,"jupiter mode=backfill complete=true requested={requested} found={found} not_found={not_found} published={found} retries={retry_count} elapsed_ms={}",
            started.elapsed().as_millis());
        Ok(())
    }
    async fn watch_until(&mut self,input:&mut watch::Receiver<Arc<[Pubkey]>>,
        output:&broadcast::Sender<Arc<DataBatch>>,deadline:Instant)->Result<()> {
        ensure!(self.pending.is_empty(),"Jupiter watch overlaps backfill");
        let mut cursor=0_usize;
        let mut source_open=true;
        let mut fetched=0_u64;
        let mut in_flight=HashSet::<Pubkey>::new();
        crate::log_event!(Info,"jupiter mode=watch started targets={}",input.borrow().len());
        while Instant::now()<deadline || !self.pending.is_empty() {
            let targets=input.borrow_and_update().clone();
            if Instant::now()<deadline && !targets.is_empty() {
                for lane in 0..self.lanes.len() {
                    if self.lanes[lane].in_flight || Instant::now()<self.lanes[lane].next_due {continue;}
                    let mut mints=Vec::new();
                    let mut scanned=0;
                    while scanned<targets.len() && mints.len()<self.settings.batch_size {
                        if cursor>=targets.len() {cursor=0;}
                        let mint=targets[cursor];
                        cursor=(cursor+1)%targets.len();
                        scanned+=1;
                        if in_flight.insert(mint) {mints.push(mint);}
                    }
                    if !mints.is_empty() {
                        self.dispatch(lane,Work{mints,retry:0});
                    }
                }
            }
            let due=if Instant::now()<deadline
                && targets.iter().any(|mint|!in_flight.contains(mint)) {
                self.available_due().map(|t|t.min(deadline))
            } else {None};
            tokio::select! {
                changed=input.changed(), if source_open && Instant::now()<deadline =>{
                    if changed.is_err() {source_open=false;}
                    // Do not restart at the beginning on every qualification
                    // update; doing so starves later targets in a changing list.
                    if cursor>=input.borrow().len() {cursor=0;}
                    crate::log_event!(Debug,"jupiter watch targets_updated={}",input.borrow().len());
                }
                result=self.pending.join_next(), if !self.pending.is_empty()=>{
                    let finished=result.context("missing Jupiter watch response")??;
                    self.lanes[finished.lane].in_flight=false;
                    for mint in &finished.work.mints {in_flight.remove(mint);}
                    match finished.outcome {
                        Outcome::Success(decoded)=>{
                            let returned=decoded.found;
                            fetched+=returned as u64;
                            Self::broadcast(output,decoded.batch);
                            crate::log_event!(Debug,
                                "jupiter mode=watch lane={} requested={} returned={} rejected={} delivered_total={fetched}",
                                finished.lane,finished.work.mints.len(),
                                returned,decoded.rejected);
                        }
                        Outcome::Retry{reason,delay}=>{
                            self.lanes[finished.lane].next_due=
                                self.lanes[finished.lane].next_due.max(Instant::now()+delay);
                            crate::log_event!(Warn,"jupiter mode=watch lane={} reason={reason}",
                                finished.lane);
                        }
                    }
                }
                _=sleep_until(due.unwrap_or(deadline)), if due.is_some()=>{}
                _=sleep_until(deadline), if Instant::now()<deadline=>{}
            }
        }
        crate::log_event!(Info,"jupiter mode=watch paused requests_drained=true decoded={fetched}");
        Ok(())
    }
}
/// CP1: preserve the existing API lanes and targeted watch scheduler but
/// never start the full-catalogue scan as part of active qualification.
/// The dedicated --jupiter-mint-backfill command remains available separately.
pub async fn run_targeted(http:Client,settings:Settings,
    mut input:watch::Receiver<Arc<[Pubkey]>>,
    output:broadcast::Sender<Arc<DataBatch>>)->Result<()> {
    let mut engine=Engine::new(http,settings).await?;
    loop {
        let deadline=Instant::now()+engine.settings.backfill_interval;
        engine.watch_until(&mut input,&output,deadline).await?;
    }
}

/// Continuous source-only Jupiter stream. Mint inventory comes from the
/// Mint Registry, not a hidden SQL cursor or Graph-owned backfill.
pub async fn run(http:Client,settings:Settings,input:watch::Receiver<Arc<[Pubkey]>>,
    output:broadcast::Sender<Arc<DataBatch>>)->Result<()> {
    run_targeted(http,settings,input,output).await
}
/// Explicit finite source: caller supplies pool inventory and persists the
/// independent output via the single Persistence worker.
pub async fn run_backfill(http:Client,settings:Settings,mints:Vec<Pubkey>,
    output:broadcast::Sender<Arc<DataBatch>>)->Result<()> {
    let mut engine=Engine::new(http,settings).await?;
    engine.backfill(mints,&output).await
}

