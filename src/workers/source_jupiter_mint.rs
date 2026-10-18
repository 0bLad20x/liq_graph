//! Single Jupiter Search owner: the Graph watch and periodic full SQL mint scan
//! share API-key lanes, decoding, Postgres upserts and one process-wide DB lease.
//! Exactly one mode dispatches HTTP requests at a time.
use std::{collections::{HashSet, VecDeque}, env, sync::Arc, time::Duration};
use anyhow::{anyhow, bail, Context, Result, ensure};
use reqwest::{Client, StatusCode};
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, watch}, task::JoinSet, time::{Instant, sleep_until}};
use tokio_postgres::{Client as Db, NoTls};

use crate::core::{DataBatch, DataRecord, RecordIdentity, now_unix_ms};

const SOURCE: &str = "jupiter_search";
const PAGE_SIZE: usize = 4096;
const WRITE_BATCH: usize = 512;
const MAX_RETRIES: u8 = 3;
// Session-scoped advisory lock: a standalone backfill cannot run alongside --run.
const LOCK_ID: i64 = 0x4a55504954455201;
const CREATE: &str = r#"
CREATE TABLE IF NOT EXISTS public.liq_graph_mint_catalog (
 mint_address BYTEA PRIMARY KEY CHECK (octet_length(mint_address)=32),
 created_at TIMESTAMPTZ NULL, updated_at TIMESTAMPTZ NULL,
 liquidity_usd NUMERIC NULL, market_cap_usd NUMERIC NULL,
 holder_count BIGINT NULL CHECK (holder_count IS NULL OR holder_count>=0),
 usd_price NUMERIC NULL, fetched_at TIMESTAMPTZ NULL)"#;
const UPSERT: &str = r#"
INSERT INTO public.liq_graph_mint_catalog AS old
 (mint_address,created_at,updated_at,liquidity_usd,market_cap_usd,holder_count,usd_price,fetched_at)
SELECT d.mint,
 CASE WHEN d.created ~ '^-?[0-9]{1,16}$'
      THEN to_timestamp(d.created::double precision /
           CASE WHEN length(trim(leading '-' from d.created))>=13 THEN 1000.0 ELSE 1.0 END)
      WHEN pg_input_is_valid(d.created,'timestamp with time zone') THEN d.created::timestamptz
      ELSE NULL END,
 CASE WHEN d.updated ~ '^-?[0-9]{1,16}$'
      THEN to_timestamp(d.updated::double precision /
           CASE WHEN length(trim(leading '-' from d.updated))>=13 THEN 1000.0 ELSE 1.0 END)
      WHEN pg_input_is_valid(d.updated,'timestamp with time zone') THEN d.updated::timestamptz
      ELSE NULL END,
 d.liquidity::numeric,d.mcap::numeric,d.holders::bigint,d.price::numeric,clock_timestamp()
FROM UNNEST($1::bytea[],$2::text[],$3::text[],$4::text[],
            $5::text[],$6::text[],$7::text[])
 AS d(mint,created,updated,liquidity,mcap,holders,price)
ON CONFLICT (mint_address) DO UPDATE SET
 created_at=COALESCE(EXCLUDED.created_at,old.created_at),
 updated_at=COALESCE(EXCLUDED.updated_at,old.updated_at),
 liquidity_usd=COALESCE(EXCLUDED.liquidity_usd,old.liquidity_usd),
 market_cap_usd=COALESCE(EXCLUDED.market_cap_usd,old.market_cap_usd),
 holder_count=COALESCE(EXCLUDED.holder_count,old.holder_count),
 usd_price=COALESCE(EXCLUDED.usd_price,old.usd_price),
 fetched_at=EXCLUDED.fetched_at"#;

#[derive(Clone)]
pub struct Settings {
    pub url: String,
    pub api_keys: Vec<String>,
    pub calls_per_minute_per_key: f64,
    pub batch_size: usize,
    pub timeout: Duration,
    pub backfill_interval: Duration,
    pub backfill_on_start: bool,
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
        let backfill_on_start = match env::var("JUPITER_BACKFILL_ON_START")
            .unwrap_or_else(|_| "false".into()).as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => bail!("JUPITER_BACKFILL_ON_START must be true or false"),
            };
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
            backfill_on_start,backfill_limit})
    }
}

#[derive(Clone)]
struct Token {
    mint: Pubkey,
    created: Option<String>, updated: Option<String>,
    liquidity: Option<String>, mcap: Option<String>,
    holders: Option<String>, price: Option<String>,
}
fn date(v: Option<&Value>) -> Option<String> {
    match v {
        Some(Value::String(s)) if !s.trim().is_empty() && s.len()<=80 => Some(s.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}
fn usd(v: Option<&Value>) -> Option<String> {
    let s = match v {
        Some(Value::String(s)) => s.trim().to_owned(),
        Some(Value::Number(n)) => n.to_string(),
        _ => return None,
    };
    let value=s.parse::<f64>().ok()?;
    (value.is_finite() && value>=0.0).then_some(s)
}
fn count(v: Option<&Value>) -> Option<String> {
    let value=match v {
        Some(Value::String(s))=>s.parse::<i64>().ok()?,
        Some(Value::Number(n))=>n.as_i64()?,
        _=>return None,
    };
    (value>=0).then(||value.to_string())
}
struct Decoded {
    batch: DataBatch,
    rows: Vec<Token>,
    rejected: usize,
}
fn decode(body: &[u8], requested: &[Pubkey]) -> Result<Decoded> {
    let values: Vec<Value> = serde_json::from_slice(body)
        .context("invalid Jupiter Search JSON token array")?;
    let allowed=requested.iter().copied().collect::<HashSet<_>>();
    let mut seen=HashSet::new();
    let mut records=Vec::with_capacity(values.len());
    let mut rows=Vec::with_capacity(values.len());
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
        rows.push(Token {
            mint,created:date(payload.get("createdAt")),
            updated:date(payload.get("updatedAt")),
            liquidity:usd(payload.get("liquidity")),
            mcap:usd(payload.get("mcap")),
            holders:count(payload.get("holderCount")),
            price:usd(payload.get("usdPrice")),
        });
        records.push(DataRecord {
            identity:RecordIdentity::Mint{address:mint},tvl_usd:None,payload,
        });
    }
    Ok(Decoded{batch:DataBatch{source:SOURCE,
        received_at_ms:now_unix_ms()?,records},rows,rejected})
}
async fn connect() -> Result<Db> {
    let dsn=env::var("LIQ_GRAPH_DATABASE_URL").or_else(|_|env::var("DATABASE_URL"))
        .context("LIQ_GRAPH_DATABASE_URL or DATABASE_URL required for Jupiter")?;
    let (db,connection)=tokio_postgres::connect(&dsn,NoTls)
        .await.context("connect Jupiter mint catalog")?;
    tokio::spawn(async move{
        if let Err(error)=connection.await {
            crate::log_event!(Error,"Jupiter DB connection failed={error:#}");
        }
    });
    let acquired:bool=db.query_one("SELECT pg_try_advisory_lock($1::bigint)",&[&LOCK_ID])
        .await.context("acquire Jupiter exclusive database lease")?.get(0);
    ensure!(acquired,"another Jupiter worker/backfill holds the catalog lease");
    db.batch_execute(CREATE).await.context("create Jupiter mint catalog")?;
    db.batch_execute("ALTER TABLE public.liq_graph_mint_catalog ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMPTZ NULL")
        .await.context("migrate Jupiter catalog fetched_at")?;
    Ok(db)
}
async fn write(db:&Db,rows:Vec<Token>) -> Result<u64> {
    if rows.is_empty() {return Ok(0);}
    let mut ids=Vec::new();let mut created=Vec::new();let mut updated=Vec::new();
    let mut liquidity=Vec::new();let mut mcap=Vec::new();let mut holders=Vec::new();
    let mut price=Vec::new();
    for row in rows {
        ids.push(row.mint.to_bytes().to_vec());created.push(row.created);
        updated.push(row.updated);liquidity.push(row.liquidity);
        mcap.push(row.mcap);holders.push(row.holders);price.push(row.price);
    }
    db.execute(UPSERT,&[&ids,&created,&updated,&liquidity,&mcap,&holders,&price])
        .await.context("upsert Jupiter mint catalog")
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
    http:Client, settings:Settings, db:Db, lanes:Vec<Lane>,
    pending:JoinSet<Finished>,
}
impl Engine {
    async fn new(http:Client,settings:Settings)->Result<Self> {
        let db=connect().await?;
        let period=Duration::from_secs_f64(60.0/settings.calls_per_minute_per_key);
        let phase=period.div_f64(settings.api_keys.len() as f64);
        let now=Instant::now();
        let lanes=settings.api_keys.iter().enumerate().map(|(i,key)|Lane {
            key:key.clone(),next_due:now+phase.mul_f64(i as f64),in_flight:false,
        }).collect::<Vec<_>>();
        crate::log_event!(Info,"jupiter unified started keys={} rpm_per_key={} batch={} backfill_interval_s={} on_start={}",
            lanes.len(),settings.calls_per_minute_per_key,settings.batch_size,
            settings.backfill_interval.as_secs(),settings.backfill_on_start);
        Ok(Self{http,settings,db,lanes,pending:JoinSet::new()})
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
    async fn persist(&self,rows:Vec<Token>)->Result<u64> {write(&self.db,rows).await}
    fn broadcast(output:Option<&broadcast::Sender<Arc<DataBatch>>>,batch:DataBatch) {
        if let Some(output)=output {let _=output.send(Arc::new(batch));}
    }
    async fn backfill(&mut self,output:Option<&broadcast::Sender<Arc<DataBatch>>>)->Result<()> {
        ensure!(self.pending.is_empty(),"Jupiter backfill overlaps active watch requests");
        let started=Instant::now();
        let cap=if self.settings.backfill_limit==0 {String::new()}
            else {format!(" LIMIT {}",self.settings.backfill_limit)};
        self.db.batch_execute(&format!(
            "BEGIN READ ONLY; DECLARE jupiter_mint_scan NO SCROLL CURSOR WITH HOLD FOR
             SELECT DISTINCT mint FROM (
                 SELECT mint_a AS mint FROM public.liq_graph_pool_cache WHERE mint_a IS NOT NULL
                 UNION ALL
                 SELECT mint_b AS mint FROM public.liq_graph_pool_cache WHERE mint_b IS NOT NULL
             ) AS all_mints ORDER BY mint{cap}; COMMIT;"
        )).await.context("open full Jupiter mint cursor")?;
        let mut queued=VecDeque::<Pubkey>::new();
        let mut retries=VecDeque::<Work>::new();
        let mut buffer=Vec::<Token>::new();
        let mut exhausted=false;
        let (mut requested,mut found,mut not_found,mut saved,mut retry_count)=(0_u64,0_u64,0_u64,0_u64,0_u64);
        crate::log_event!(Info,"jupiter mode=backfill started limit={}",self.settings.backfill_limit);
        loop {
            if !exhausted && queued.len()<PAGE_SIZE {
                let rows=self.db.query(&format!("FETCH FORWARD {PAGE_SIZE} FROM jupiter_mint_scan"),&[])
                    .await.context("read full Jupiter mint cursor")?;
                if rows.is_empty() {exhausted=true;}
                for row in rows {
                    let raw:Vec<u8>=row.get(0);
                    let bytes:[u8;32]=raw.try_into().map_err(|_|anyhow!("invalid mint bytes"))?;
                    let mint=Pubkey::new_from_array(bytes);
                    ensure!(mint!=Pubkey::default(),"zero mint in backfill cursor");
                    queued.push_back(mint);
                }
            }
            for lane in 0..self.lanes.len() {
                if self.lanes[lane].in_flight || Instant::now()<self.lanes[lane].next_due {continue;}
                let work=if let Some(work)=retries.pop_front() {work}
                    else {
                        let n=queued.len().min(self.settings.batch_size);
                        if n==0 {continue;}
                        Work{mints:queued.drain(..n).collect(),retry:0}
                    };
                if work.retry==0 {requested+=work.mints.len() as u64;}
                self.dispatch(lane,work);
            }
            if exhausted && queued.is_empty() && retries.is_empty() && self.pending.is_empty() {break;}
            let due=if !queued.is_empty()||!retries.is_empty(){self.available_due()}else{None};
            tokio::select! {
                result=self.pending.join_next(), if !self.pending.is_empty() => {
                    let finished=result.context("missing Jupiter backfill response")??;
                    self.lanes[finished.lane].in_flight=false;
                    match finished.outcome {
                        Outcome::Success(decoded)=>{
                            found+=decoded.rows.len() as u64;
                            not_found+=(finished.work.mints.len()-decoded.rows.len()) as u64;
                            if decoded.rejected>0 {
                                crate::log_event!(Warn,"Jupiter backfill rejected_mints={}",decoded.rejected);
                            }
                            buffer.extend(decoded.rows);
                            Self::broadcast(output,decoded.batch);
                            while buffer.len()>=WRITE_BATCH {
                                saved+=self.persist(buffer.drain(..WRITE_BATCH).collect()).await?;
                            }
                        }
                        Outcome::Retry{reason,delay} if finished.work.retry<MAX_RETRIES=>{
                            retry_count+=1;
                            self.lanes[finished.lane].next_due=
                                self.lanes[finished.lane].next_due.max(Instant::now()+delay);
                            crate::log_event!(Warn,"Jupiter backfill retry={} lane={} reason={reason}",
                                finished.work.retry+1,finished.lane);
                            retries.push_back(Work{mints:finished.work.mints,
                                retry:finished.work.retry+1});
                        }
                        Outcome::Retry{reason,..}=>bail!(
                            "Jupiter full scan incomplete after {MAX_RETRIES} retries: {reason}"),
                    }
                }
                _=sleep_until(due.unwrap_or_else(Instant::now)), if due.is_some()=>{}
            }
        }
        saved+=self.persist(buffer).await?;
        self.db.batch_execute("CLOSE jupiter_mint_scan").await.context("close full Jupiter cursor")?;
        ensure!(requested==found+not_found,"Jupiter backfill accounting mismatch");
        crate::log_event!(Info,"jupiter mode=backfill complete=true requested={requested} found={found} not_found={not_found} persisted={saved} retries={retry_count} elapsed_ms={}",
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
                    cursor=0;
                    crate::log_event!(Debug,"jupiter watch targets_updated={}",input.borrow().len());
                }
                result=self.pending.join_next(), if !self.pending.is_empty()=>{
                    let finished=result.context("missing Jupiter watch response")??;
                    self.lanes[finished.lane].in_flight=false;
                    for mint in &finished.work.mints {in_flight.remove(mint);}
                    match finished.outcome {
                        Outcome::Success(decoded)=>{
                            let returned=decoded.rows.len();
                            fetched+=returned as u64;
                            self.persist(decoded.rows).await?;
                            Self::broadcast(Some(output),decoded.batch);
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
/// Production: one process, one API scheduler. Backfill and watch never overlap.
pub async fn run(http:Client,settings:Settings,mut input:watch::Receiver<Arc<[Pubkey]>>,
    output:broadcast::Sender<Arc<DataBatch>>)->Result<()> {
    let mut engine=Engine::new(http,settings).await?;
    if engine.settings.backfill_on_start {
        engine.backfill(None).await?;
    }
    loop {
        let deadline=Instant::now()+engine.settings.backfill_interval;
        engine.watch_until(&mut input,&output,deadline).await?;
        engine.backfill(None).await?;
    }
}
/// Finite compatibility CLI: --jupiter-mint-backfill / --test jupiter_mint_backfill.
pub async fn run_backfill(http:Client,settings:Settings)->Result<()> {
    let mut engine=Engine::new(http,settings).await?;
    engine.backfill(None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decode_requested_only() {
        let mint:Pubkey="So11111111111111111111111111111111111111112".parse().unwrap();
        let body=format!(r#"[{{"id":"{mint}","liquidity":4200,"mcap":10000,"holderCount":12}}]"#);
        let parsed=decode(body.as_bytes(),&[mint]).unwrap();
        assert_eq!(parsed.rows.len(),1);
        assert_eq!(parsed.batch.records.len(),1);
        assert_eq!(parsed.rows[0].liquidity.as_deref(),Some("4200"));
    }
}
