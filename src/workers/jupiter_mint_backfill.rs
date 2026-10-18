//! One-shot Jupiter Mint catalog: distinct SQL mint input, keyed HTTP lanes, batched SQL output.
//! Independent from the live Jupiter source, Pool Registry, Completion and Liquidity Graph.
use std::{collections::{HashSet,VecDeque},env,time::Duration};
use anyhow::{anyhow,Context,Result,ensure,bail};
use reqwest::Client as Http;
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{task::JoinSet,time::{Instant,sleep_until}};
use tokio_postgres::{Client as Db,NoTls};

const CREATE:&str=r#"
CREATE TABLE IF NOT EXISTS public.liq_graph_mint_catalog (
 mint_address BYTEA PRIMARY KEY CHECK (octet_length(mint_address)=32),
 created_at TIMESTAMPTZ NULL, updated_at TIMESTAMPTZ NULL,
 liquidity_usd NUMERIC NULL, market_cap_usd NUMERIC NULL,
 holder_count BIGINT NULL CHECK (holder_count IS NULL OR holder_count>=0),
 usd_price NUMERIC NULL)"#;
const UPSERT:&str=r#"
INSERT INTO public.liq_graph_mint_catalog AS old
 (mint_address,created_at,updated_at,liquidity_usd,market_cap_usd,holder_count,usd_price)
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
 d.liquidity::numeric,d.mcap::numeric,d.holders::bigint,d.price::numeric
FROM UNNEST($1::bytea[],$2::text[],$3::text[],$4::text[],
            $5::text[],$6::text[],$7::text[])
 AS d(mint,created,updated,liquidity,mcap,holders,price)
ON CONFLICT (mint_address) DO UPDATE SET
 created_at=COALESCE(EXCLUDED.created_at,old.created_at),
 updated_at=COALESCE(EXCLUDED.updated_at,old.updated_at),
 liquidity_usd=COALESCE(EXCLUDED.liquidity_usd,old.liquidity_usd),
 market_cap_usd=COALESCE(EXCLUDED.market_cap_usd,old.market_cap_usd),
 holder_count=COALESCE(EXCLUDED.holder_count,old.holder_count),
 usd_price=COALESCE(EXCLUDED.usd_price,old.usd_price)"#;
const SQL_PAGE:usize=4096;
const WRITE_BATCH:usize=512;
const MAX_RETRIES:u8=3;

pub struct Settings {
 url:String,keys:Vec<String>,rate:f64,batch:usize,timeout:Duration,limit:usize,
}
impl Settings {
 pub fn from_env()->Result<Self>{
  let mut unique=HashSet::new();
  let keys=env::var("JUPITER_SEARCH_API_KEYS").unwrap_or_default()
   .split(|c|c==','||c=='\n').map(str::trim).filter(|k|!k.is_empty())
   .filter(|k|unique.insert((*k).to_owned())).map(ToOwned::to_owned).collect::<Vec<_>>();
  ensure!(!keys.is_empty(),"JUPITER_SEARCH_API_KEYS required");
  let rate=env::var("JUPITER_SEARCH_CALLS_PER_MINUTE").unwrap_or_else(|_|"57".into())
   .parse::<f64>().context("invalid JUPITER_SEARCH_CALLS_PER_MINUTE")?;
  let batch=env::var("JUPITER_SEARCH_BATCH_SIZE").unwrap_or_else(|_|"100".into())
   .parse::<usize>().context("invalid JUPITER_SEARCH_BATCH_SIZE")?;
  let seconds=env::var("JUPITER_SEARCH_REQUEST_TIMEOUT_SECONDS").unwrap_or_else(|_|"20".into())
   .parse::<u64>().context("invalid JUPITER_SEARCH_REQUEST_TIMEOUT_SECONDS")?;
  let limit=env::var("JUPITER_MINT_BACKFILL_LIMIT").unwrap_or_else(|_|"0".into())
   .parse::<usize>().context("invalid JUPITER_MINT_BACKFILL_LIMIT")?;
  let url=env::var("JUPITER_SEARCH_URL")
   .unwrap_or_else(|_|"https://api.jup.ag/tokens/v2/search".into());
  ensure!(rate.is_finite()&&rate>0.0&&rate<=3600.0,"invalid Jupiter key RPM");
  ensure!((1..=100).contains(&batch)&&seconds>0&&!url.is_empty(),"invalid Jupiter batch/timeout/url");
  Ok(Self{url,keys,rate,batch,timeout:Duration::from_secs(seconds),limit})
 }
}
struct Token {
 mint:Pubkey,created:Option<String>,updated:Option<String>,
 liquidity:Option<String>,mcap:Option<String>,holders:Option<String>,price:Option<String>,
}
fn date(v:Option<&Value>)->Option<String>{
 match v {
  Some(Value::String(s)) if !s.trim().is_empty()&&s.len()<=80=>Some(s.clone()),
  Some(Value::Number(n))=>Some(n.to_string()),
  _=>None,
 }
}
fn usd(v:Option<&Value>)->Option<String>{
 let s=match v {
  Some(Value::String(s))=>s.trim().to_owned(),
  Some(Value::Number(n))=>n.to_string(),
  _=>return None,
 };
 let n=s.parse::<f64>().ok()?;
 (n.is_finite()&&n>=0.0).then_some(s)
}
fn count(v:Option<&Value>)->Option<String>{
 let n=match v {
  Some(Value::String(s))=>s.parse::<i64>().ok()?,
  Some(Value::Number(n))=>n.as_i64()?,
  _=>return None,
 };
 (n>=0).then(||n.to_string())
}
fn decode(values:Vec<Value>,requested:&[Pubkey])->Result<Vec<Token>>{
 let allowed=requested.iter().copied().collect::<HashSet<_>>();
 let mut seen=HashSet::new();
 let mut out=Vec::new();
 for v in values {
  let id=v.get("id").and_then(Value::as_str).context("Jupiter token missing id")?
   .parse::<Pubkey>().context("invalid Jupiter mint id")?;
  ensure!(allowed.contains(&id),"Jupiter returned unrequested mint {id}");
  if !seen.insert(id){continue;}
  out.push(Token{
   mint:id,created:date(v.get("createdAt")),updated:date(v.get("updatedAt")),
   liquidity:usd(v.get("liquidity")),mcap:usd(v.get("mcap")),
   holders:count(v.get("holderCount")),price:usd(v.get("usdPrice")),
  });
 }
 Ok(out)
}
async fn write(db:&Db,rows:Vec<Token>)->Result<u64>{
 if rows.is_empty(){return Ok(0);}
 let mut ids=Vec::new();let mut created=Vec::new();let mut updated=Vec::new();
 let mut liquidity=Vec::new();let mut mcap=Vec::new();let mut holders=Vec::new();
 let mut price=Vec::new();
 for row in rows {
  ids.push(row.mint.to_bytes().to_vec());created.push(row.created);
  updated.push(row.updated);liquidity.push(row.liquidity);
  mcap.push(row.mcap);holders.push(row.holders);price.push(row.price);
 }
 db.execute(UPSERT,&[&ids,&created,&updated,&liquidity,&mcap,&holders,&price])
  .await.context("write Jupiter mint catalog batch")
}
async fn connect(url:&str)->Result<Db>{
 let (db,conn)=tokio_postgres::connect(url,NoTls).await.context("connect mint catalog PostgreSQL")?;
 tokio::spawn(async move{
  if let Err(error)=conn.await {crate::log_event!(Error,"mint catalog DB connection={error:#}");}
 });
 Ok(db)
}
struct Lane{key:String,due:Instant,busy:bool}
struct Batch{mints:Vec<Pubkey>,retry:u8}
struct Done{lane:usize,batch:Batch,result:Result<Vec<Value>>}
async fn fetch(http:Http,url:String,key:String,timeout:Duration,mints:&[Pubkey])->Result<Vec<Value>>{
 let query=mints.iter().map(ToString::to_string).collect::<Vec<_>>().join(",");
 let response=http.get(&url).query(&[("query",query)]).header("x-api-key",key)
  .timeout(timeout).send().await.context("Jupiter search HTTP")?;
 ensure!(response.status().is_success(),"Jupiter search HTTP {}",response.status());
 response.json::<Vec<Value>>().await.context("decode Jupiter mint JSON")
}
pub async fn run(http:Http,settings:Settings)->Result<()>{
 let url=env::var("LIQ_GRAPH_DATABASE_URL").or_else(|_|env::var("DATABASE_URL"))
  .context("LIQ_GRAPH_DATABASE_URL or DATABASE_URL required")?;
 let reader=connect(&url).await?;
 let writer=connect(&url).await?;
 writer.batch_execute(CREATE).await.context("create mint catalog")?;
 // Materialize unique mints server-side, releasing the transaction before API reads.
 let cap=if settings.limit==0{String::new()}else{format!(" LIMIT {}",settings.limit)};
 reader.batch_execute(&format!(
  "BEGIN READ ONLY; DECLARE mint_scan NO SCROLL CURSOR WITH HOLD FOR
   SELECT DISTINCT mint FROM (
    SELECT mint_a AS mint FROM public.liq_graph_pool_cache WHERE mint_a IS NOT NULL
    UNION ALL SELECT mint_b AS mint FROM public.liq_graph_pool_cache WHERE mint_b IS NOT NULL
   ) AS x ORDER BY mint{cap}; COMMIT;"
 )).await.context("initialize deduplicated mint cursor")?;
 let started=Instant::now();
 let period=Duration::from_secs_f64(60.0/settings.rate);
 let phase=period.div_f64(settings.keys.len() as f64);
 let mut lanes=settings.keys.into_iter().enumerate()
  .map(|(i,key)|Lane{key,busy:false,due:started+phase.mul_f64(i as f64)})
  .collect::<Vec<_>>();
 let mut pending=JoinSet::<Done>::new();
 let mut queued=VecDeque::<Pubkey>::new();
 let mut retries=VecDeque::<Batch>::new();
 let mut buffered=Vec::<Token>::new();
 let mut exhausted=false;
 let(mut requested,mut returned,mut not_found,mut saved,mut retried)=(0_u64,0_u64,0_u64,0_u64,0_u64);
 crate::log_event!(Info,"mint backfill started keys={} rpm={} batch={} limit={}",
  lanes.len(),settings.rate,settings.batch,settings.limit);
 loop {
  if !exhausted&&queued.len()<SQL_PAGE {
   let rows=reader.query(&format!("FETCH FORWARD {SQL_PAGE} FROM mint_scan"),&[])
    .await.context("fetch SQL mint cursor")?;
   if rows.is_empty(){exhausted=true;}
   for row in rows {
    let bytes:Vec<u8>=row.get(0);
    let raw:[u8;32]=bytes.try_into().map_err(|_|anyhow!("invalid persisted mint bytes"))?;
    let mint=Pubkey::new_from_array(raw);
    ensure!(mint!=Pubkey::default(),"zero mint in SQL source");
    queued.push_back(mint);
   }
  }
  for (i,lane) in lanes.iter_mut().enumerate() {
   if lane.busy||Instant::now()<lane.due {continue;}
   let batch=if let Some(v)=retries.pop_front(){v}else{
    let n=queued.len().min(settings.batch);
    if n==0{continue;}
    Batch{mints:queued.drain(..n).collect(),retry:0}
   };
   if batch.retry==0{requested+=batch.mints.len() as u64;}
   lane.busy=true;lane.due=Instant::now()+period;
   let client=http.clone();let url=settings.url.clone();let key=lane.key.clone();
   let timeout=settings.timeout;
   pending.spawn(async move{
    let result=fetch(client,url,key,timeout,&batch.mints).await;
    Done{lane:i,batch,result}
   });
  }
  if exhausted&&queued.is_empty()&&retries.is_empty()&&pending.is_empty(){break;}
  let has_work=!queued.is_empty()||!retries.is_empty();
  let due=if has_work{lanes.iter().filter(|l|!l.busy).map(|l|l.due).min()}else{None};
  tokio::select! {
   outcome=pending.join_next(),if !pending.is_empty()=>{
    let done=outcome.context("missing Jupiter request completion")??;
    lanes[done.lane].busy=false;
    match done.result.and_then(|v|decode(v,&done.batch.mints)){
     Ok(tokens)=>{
      returned+=tokens.len() as u64;
      not_found+=(done.batch.mints.len()-tokens.len()) as u64;
      buffered.extend(tokens);
      while buffered.len()>=WRITE_BATCH {
       let rows=buffered.drain(..WRITE_BATCH).collect();
       saved+=write(&writer,rows).await?;
      }
      if requested>0&&requested%10000<settings.batch as u64 {
       crate::log_event!(Info,
        "mint backfill progress requested={requested} returned={returned} not_found={not_found} persisted={saved} retries={retried}");
      }
     }
     Err(error) if done.batch.retry<MAX_RETRIES=>{
      retried+=1;
      crate::log_event!(Warn,"mint backfill retry={} reason={error:#}",done.batch.retry+1);
      retries.push_back(Batch{mints:done.batch.mints,retry:done.batch.retry+1});
     }
     Err(error)=>bail!("Jupiter backfill incomplete after {MAX_RETRIES} retries: {error:#}"),
    }
   }
   _=sleep_until(due.unwrap_or_else(Instant::now)),if due.is_some()=>{}
  }
 }
 saved+=write(&writer,buffered).await?;
 reader.batch_execute("CLOSE mint_scan").await.context("close SQL mint cursor")?;
 ensure!(requested==returned+not_found,"Jupiter mint request accounting mismatch");
 crate::log_event!(Info,
  "mint backfill complete=true requested={requested} returned={returned} not_found={not_found} persisted={saved} retries={retried} elapsed_ms={}",
  started.elapsed().as_millis());
 Ok(())
}
