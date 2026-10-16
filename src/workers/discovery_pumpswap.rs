//! PumpSwap rolling discovery: Helius V2 changed program accounts -> typed source batches.
//! No persistence, completion, reserve polling, or provider-specific HTTP.
use std::{collections::HashSet, time::Duration, sync::Arc};
use anyhow::{bail, ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::{sync::broadcast, time::{sleep_until, Instant}};
use crate::{
    core::{DataBatch, DataRecord, RecordIdentity, Venue},
    workers::rpc_transport::{AccountData, AccountFilter, AccountView, RpcAnswer, RpcHandle, RpcOperation},
};

const PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const POOL_DISC: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];
const MIN_LEN: usize = 245;

#[derive(Clone, Copy)]
pub struct Settings {
    pub once: bool,
    pub max_pages: Option<u32>,
    pub lookback_slots: u64,
    pub interval: Duration,
    pub page_limit: usize,
}
impl Settings {
    pub fn from_env(once: bool, max_pages: Option<u32>) -> Result<Self> {
        fn value<T: std::str::FromStr>(key: &str, default: T) -> Result<T>
        where T::Err: std::error::Error + Send + Sync + 'static {
            match std::env::var(key) {
                Ok(v) => v.parse::<T>().with_context(||format!("invalid {key}")),
                Err(std::env::VarError::NotPresent) => Ok(default),
                Err(e) => Err(e.into()),
            }
        }
        let lookback_slots = value("PUMPSWAP_ROLLING_LOOKBACK_SLOTS", 9000_u64)?;
        let interval_seconds = value("PUMPSWAP_ROLLING_INTERVAL_SECS", 600_u64)?;
        let page_limit = value("PUMPSWAP_ROLLING_PAGE_LIMIT", 1000_usize)?;
        ensure!(lookback_slots>0 && interval_seconds>0 && (1..=5000).contains(&page_limit),
            "invalid PumpSwap rolling window, interval or page size");
        ensure!(once || max_pages.is_none(),
            "pagination budget is diagnostic-only; continuous scans must not truncate");
        Ok(Self {once,max_pages,lookback_slots,
            interval:Duration::from_secs(interval_seconds),page_limit})
    }
}
fn key(d:&[u8], offset:usize) -> Result<Pubkey> {
    Ok(Pubkey::new_from_array(d.get(offset..offset+32)
        .context("truncated PumpSwap public key")?.try_into()?))
}
fn record(a:AccountData, program:Pubkey) -> Result<DataRecord> {
    ensure!(a.owner==program && a.view==AccountView::Full
        && a.original_len==Some(a.bytes.len()) && a.bytes.len()>=MIN_LEN
        && a.bytes[..8]==POOL_DISC, "PumpSwap pool owner/discriminator/full length mismatch");
    let d=&a.bytes;
    let base=key(d,43)?;
    let quote=key(d,75)?;
    ensure!(base!=Pubkey::default() && quote!=Pubkey::default() && base!=quote,
        "invalid PumpSwap mint pair");
    let creator=key(d,11)?;
    let lp=key(d,107)?;
    let base_vault=key(d,139)?;
    let quote_vault=key(d,171)?;
    ensure!(base_vault!=Pubkey::default() && quote_vault!=Pubkey::default()
        && base_vault!=quote_vault,"invalid PumpSwap vault pair");
    let payload=json!({
        "kind":"pumpswap_pool_v1","account_bytes":d.len(),
        "account_data_base64":STANDARD.encode(d),
        "creator":creator.to_string(),"lp_mint":lp.to_string(),
        "base_vault":base_vault.to_string(),"quote_vault":quote_vault.to_string(),
        "coin_creator":key(d,211)?.to_string(),
        "is_mayhem_mode":d[243]!=0,"is_cashback_coin":d[244]!=0,
        "lp_supply_raw":u64::from_le_bytes(d[203..211].try_into()?),
    });
    Ok(DataRecord{identity:RecordIdentity::Pool{
        address:a.address,venue:Venue::PumpSwap,mint_a:Some(base),mint_b:Some(quote)
    },tvl_usd:None,payload})
}

pub async fn run(rpc:RpcHandle, output:broadcast::Sender<Arc<DataBatch>>,
    settings:Settings) -> Result<()> {
    let program:Pubkey=PROGRAM.parse()?;
    let initial=RpcOperation::ProgramV2{program,
        filters:vec![AccountFilter::Memcmp{offset:0,bytes:POOL_DISC.to_vec()}],
        view:AccountView::Full,cursor:None,changed_since_slot:Some(1),
        limit:settings.page_limit};
    ensure!(rpc.supports(&initial), "PumpSwap requires a configured getProgramAccountsV2 provider");
    ensure!(rpc.supports(&RpcOperation::Slot{commitment:"finalized".into()}),
        "PumpSwap requires a getSlot provider");
    let mut pass=0_u64;
    loop {
        let started=Instant::now();
        let slot_answer=rpc.call(RpcOperation::Slot{commitment:"finalized".into()}).await?;
        let RpcAnswer::Slot(current)=slot_answer.answer else {
            bail!("PumpSwap finalized getSlot returned unexpected result");
        };
        let since=current.saturating_sub(settings.lookback_slots);
        let mut cursor:Option<String>=None;
        let mut cursors=HashSet::new();
        let mut seen=HashSet::new();
        let mut pages=0_u32;
        let mut received=0_usize;
        let mut rejected=0_usize;
        let mut complete=false;
        pass+=1;
        loop {
            let next=rpc.call(RpcOperation::ProgramV2{program,
                filters:vec![AccountFilter::Memcmp{offset:0,bytes:POOL_DISC.to_vec()}],
                view:AccountView::Full,cursor:cursor.clone(),
                changed_since_slot:Some(since),limit:settings.page_limit}).await?;
            let provider=next.provider;
            let RpcAnswer::Program{accounts,next_cursor}=next.answer else {
                bail!("PumpSwap V2 returned unexpected result");
            };
            pages+=1;
            received+=accounts.len();
            let mut records=Vec::with_capacity(accounts.len());
            for account in accounts {
                if !seen.insert(account.address) { continue; }
                match record(account,program) {
                    Ok(record)=>records.push(record),
                    Err(e)=>{rejected+=1;eprintln!("pumpswap discovery invalid_account={e:#}");}
                }
            }
            let accepted=records.len();
            if accepted>0 {
                output.send(Arc::new(DataBatch{source:"pumpswap_rpc_v2",
                    received_at_ms:next.received_at_ms,records}))
                    .map_err(|_|anyhow::anyhow!("PumpSwap discovery has no Registry consumer"))?;
            }
            println!("pumpswap rolling pass={pass} page={pages} provider={provider} received_total={received} accepted_page={accepted} unique={} rejected={rejected} next={}",
                seen.len(),next_cursor.is_some());
            match next_cursor {
                None=>{complete=true;break;}
                Some(key)=>{
                    ensure!(!key.is_empty() && cursors.insert(key.clone()),
                        "PumpSwap V2 repeated/empty cursor; scan incomplete");
                    cursor=Some(key);
                }
            }
            if settings.max_pages.is_some_and(|max|pages>=max) {break;}
        }
        println!("pumpswap rolling summary pass={pass} current_slot={current} since_slot={since} pages={pages} received={received} unique={} rejected={rejected} complete={complete} elapsed_ms={}",
            seen.len(),started.elapsed().as_millis());
        if settings.once {return Ok(());}
        ensure!(complete,"continuous PumpSwap scan must not truncate pagination");
        sleep_until(started+settings.interval).await;
    }
}
