//! Meteora DAMM v2 REST discovery: unfiltered paginated source -> complete pool JSON.
//! Only address, token mints and USD TVL are interpreted before the Registry.
use std::{sync::Arc,time::Duration};
use anyhow::{anyhow, ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{sync::{Mutex,broadcast},task::JoinSet,time::{Instant,sleep,sleep_until}};
use crate::core::{DataBatch,DataRecord,RecordIdentity,Venue,now_unix_ms};

const URL:&str="https://damm-v2.datapi.meteora.ag/pools";
const SOURCE:&str="meteora_damm_v2_api";
const PAGE_SIZE:usize=1000;
const MAX_IN_FLIGHT:usize=8;
const REQUEST_SPACING:Duration=Duration::from_millis(100);
const RETRY_DELAY:Duration=Duration::from_secs(2);

#[derive(Clone,Copy)]
pub struct Settings{pub once:bool,pub max_pages:Option<u32>}
#[derive(Deserialize)]
struct Response{
    current_page:u32,
    pages:u32,
    data:Vec<Value>,
}
struct Page{pages:u32,rejected:usize,batch:DataBatch}

fn mint(pool:&Value,path:&str)->Option<Pubkey>{
    pool.pointer(path).and_then(Value::as_str)
        .and_then(|s|s.parse::<Pubkey>().ok())
        .filter(|k|*k!=Pubkey::default())
}
fn tvl(pool:&Value)->Option<f64>{
    let raw=pool.get("tvl")?;
    let value=raw.as_f64().or_else(||raw.as_str()?.parse::<f64>().ok())?;
    (value.is_finite()&&value>=0.0).then_some(value)
}
fn decode(payload:Value)->Result<DataRecord>{
    let address:Pubkey=payload.pointer("/address").and_then(Value::as_str)
        .context("DAMM v2 address missing")?.parse().context("invalid DAMM v2 address")?;
    ensure!(address!=Pubkey::default(),"zero DAMM v2 address");
    let mint_a=mint(&payload,"/token_x/address");
    let mint_b=mint(&payload,"/token_y/address");
    ensure!(!mint_a.is_some_and(|a|mint_b==Some(a)),"identical DAMM v2 mints");
    Ok(DataRecord{
        identity:RecordIdentity::Pool{
            address,venue:Venue::MeteoraDammV2,mint_a,mint_b,
        },
        tvl_usd:tvl(&payload),
        payload,
    })
}
fn decode_page(bytes:&[u8],page:u32)->Result<Page>{
    let envelope:Response=serde_json::from_slice(bytes)
        .with_context(||format!("DAMM v2 invalid JSON page={page}"))?;
    ensure!(envelope.current_page==page,"DAMM v2 unexpected page number");
    ensure!(envelope.pages>=page||(page==1&&envelope.pages==0&&envelope.data.is_empty()),
        "DAMM v2 inconsistent page count");
    ensure!(envelope.data.len()<=PAGE_SIZE,"DAMM v2 page too large");
    ensure!(!envelope.data.is_empty()||(page==1&&envelope.pages==0),
        "DAMM v2 unexpected empty page");
    let mut records=Vec::with_capacity(envelope.data.len());
    let mut rejected=0_usize;
    for pool in envelope.data{
        match decode(pool){
            Ok(record)=>records.push(record),
            Err(err)=>{
                rejected+=1;
                if rejected<=5{eprintln!("damm_v2 page={page} rejected_pool error={err:#}");}
            }
        }
    }
    ensure!(!records.is_empty()||envelope.pages==0,
        "DAMM v2 page contains no valid identities");
    Ok(Page{pages:envelope.pages.max(1),rejected,
        batch:DataBatch{source:SOURCE,received_at_ms:now_unix_ms()?,records}})
}
async fn fetch(client:&reqwest::Client,page:u32)->Result<Page>{
    let response=client.get(URL).query(&[
        ("page",page.to_string()),("page_size",PAGE_SIZE.to_string()),
    ]).send().await?.error_for_status()?;
    decode_page(&response.bytes().await?,page)
}
async fn fetch_retry(client:reqwest::Client,gate:Arc<Mutex<Instant>>,page:u32)->Page{
    loop{
        let mut next=gate.lock().await;
        sleep_until(*next).await;
        *next=Instant::now()+REQUEST_SPACING;
        drop(next);
        match fetch(&client,page).await{
            Ok(page)=>return page,
            Err(err)=>{
                eprintln!("damm_v2 page={page} retry_error={err:#}");
                sleep(RETRY_DELAY).await;
            }
        }
    }
}
pub async fn run(client:reqwest::Client,output:broadcast::Sender<Arc<DataBatch>>,
    settings:Settings)->Result<()>{
    loop{
        let started=Instant::now();
        let gate=Arc::new(Mutex::new(Instant::now()));
        let first=fetch_retry(client.clone(),gate.clone(),1).await;
        let total_pages=first.pages;
        let pages=settings.max_pages.unwrap_or(total_pages).min(total_pages);
        let mut records=first.batch.records.len();
        let mut rejected=first.rejected;
        output.send(Arc::new(first.batch)).map_err(|_|anyhow!("DAMM v2 source has no consumer"))?;
        let mut completed=1_u32;
        let mut next_page=2_u32;
        let mut tasks=JoinSet::new();
        while next_page<=pages||!tasks.is_empty(){
            while next_page<=pages&&tasks.len()<MAX_IN_FLIGHT{
                let page=next_page;
                let http=client.clone();
                let gate=gate.clone();
                tasks.spawn(async move{fetch_retry(http,gate,page).await});
                next_page+=1;
            }
            if let Some(joined)=tasks.join_next().await{
                let page=joined.context("DAMM v2 page task failed")?;
                records+=page.batch.records.len();
                rejected+=page.rejected;
                output.send(Arc::new(page.batch))
                    .map_err(|_|anyhow!("DAMM v2 source has no consumer"))?;
                completed+=1;
            }
        }
        println!("damm_v2 source complete pages={completed}/{total_pages} decoded_pools={records} rejected_pools={rejected} elapsed_ms={}",started.elapsed().as_millis());
        if settings.once{return Ok(());}
    }
}
