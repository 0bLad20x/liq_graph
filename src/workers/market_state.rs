//! Indicative market state: Prepared pool inputs -> account watches -> typed prices.
//! Protocol decoding lives here; connections and provider policy live in rpc_transport.
mod fees;
mod dlmm;
use crate::core::rpc::{RpcAnswer,RpcHandle,RpcOperation};
use anyhow::{Context, Result, bail, ensure};
use std::{collections::{HashMap, HashSet, BTreeMap, VecDeque}, env, sync::Arc, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, watch}, time};
use futures_util::future::join_all;
use crate::core::{CompletionState, PoolState, PoolVaults, PoolUpdateBatch, Venue as CoreVenue, MintReader,
    market_state::{FeeValuation,PoolLiquidity,DirectedLiquidity,ReserveCurve,Consistency, MarketStateEvent, PoolRemovalReason, PoolPriceObservation, PriceEventStream, PriceKind, PriceOrigin, PricePipelineTiming}};
use super::{pool_registry::PoolRegistry, 
    graph_watch_catalog::WatchCatalog};
use crate::core::account_stream::{AccountView,AccountWatch,AccountEvent as WsEvent,AccountEventStream,AccountStreamControl};
const DLMM_PROGRAM:&str="LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const PUMP_PROGRAM:&str="pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const DAMM_V2_PROGRAM:&str="cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";
const RAYDIUM_AMM_V4_PROGRAM:&str="675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const RAYDIUM_CLMM_PROGRAM:&str="CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
const RAYDIUM_CPMM_PROGRAM:&str="CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const ORCA_WHIRLPOOL_PROGRAM:&str="whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const BYREAL_CLMM_PROGRAM:&str="REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2";
const BYREAL_CLMM_SQRT_OFFSET:usize=253;
const BYREAL_CLMM_DISC:[u8;8]=[247,237,227,245,215,195,222,70]; // Anchor account:PoolState
const MANIFEST_PROGRAM:&str="MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms";
const MANIFEST_FIXED_SIZE:usize=256;
const MANIFEST_NODE_SIZE:usize=80;
const MANIFEST_NIL:u32=u32::MAX;
const MANIFEST_MARKET_DISC:u64=4859840929024028656;
const PANCAKESWAP_CLMM_PROGRAM:&str="HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq";
const PANCAKESWAP_CLMM_SQRT_OFFSET:usize=253;
const PANCAKESWAP_CLMM_DISC:[u8;8]=[247,237,227,245,215,195,222,70]; // Anchor account:PoolState
const ORCA_WHIRLPOOL_ACCOUNT_LEN:usize=653;
const ORCA_WHIRLPOOL_SQRT_OFFSET:usize=65;
const ORCA_WHIRLPOOL_DISC:[u8;8]=[63,149,209,12,225,128,99,9]; // Anchor account:Whirlpool
const RAYDIUM_CPMM_ACCOUNT_LEN:usize=637;
const RAYDIUM_CPMM_FEES_OFFSET:usize=341;
const RAYDIUM_CPMM_FEES_LEN:usize=72;
const RAYDIUM_AMM_V4_ACCOUNT_LEN:usize=752;
const TOKEN_PROGRAM:&str="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022:&str="TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const WSOL:&str="So11111111111111111111111111111111111111112";
const USDC:&str="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const DLMM_DISC:[u8;8]=[33,11,49,98,181,101,177,13];
const POOL_DISC:[u8;8]=[241,154,109,4,17,177,109,188];
const DAMM_SQRT_OFFSET:usize=456;
const RAYDIUM_CLMM_SQRT_OFFSET:usize=253;
const RAYDIUM_CLMM_DISC:[u8;8]=[247,237,227,245,215,195,222,70]; // Anchor account:PoolState
const RAYDIUM_CPMM_DISC:[u8;8]=[247,237,227,245,215,195,222,70]; // Anchor account:PoolState

fn bytes(d:&[u8],off:usize,n:usize)->Result<&[u8]> {
    d.get(off..off+n).with_context(||format!("account layout: offset={off} len={n} actual={}",d.len()))
}
fn u32_le(d:&[u8],i:usize)->Result<u32>{Ok(u32::from_le_bytes(bytes(d,i,4)?.try_into()?))}
fn u16_le(d:&[u8],i:usize)->Result<u16>{Ok(u16::from_le_bytes(bytes(d,i,2)?.try_into()?))}
fn u64_le(d:&[u8],i:usize)->Result<u64>{Ok(u64::from_le_bytes(bytes(d,i,8)?.try_into()?))}
fn i32_le(d:&[u8],i:usize)->Result<i32>{Ok(i32::from_le_bytes(bytes(d,i,4)?.try_into()?))}
fn i128_le(d:&[u8],i:usize)->Result<i128>{Ok(i128::from_le_bytes(bytes(d,i,16)?.try_into()?))}
fn u128_le(d:&[u8],i:usize)->Result<u128>{Ok(u128::from_le_bytes(bytes(d,i,16)?.try_into()?))}
fn key(d:&[u8],i:usize)->Result<String>{Ok(bs58::encode(bytes(d,i,32)?).into_string())}
fn nanos(start:Instant)->u64{start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64}
fn timestamp_us()->u64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64}
fn bps(old:f64,new:f64)->f64 {(new/old-1.0)*10_000.0}
fn env_parse<T:std::str::FromStr>(key:&str,default:T)->Result<T>
where T::Err:std::fmt::Display {
    env::var(key).map_or(Ok(default),|s|s.parse().map_err(|e|anyhow::anyhow!("invalid {key}: {e}")))
}
#[derive(Clone)]struct RpcAccount{owner:String,data:Vec<u8>,slot:u64,received_us:u64}
#[derive(Default,serde::Serialize)]
struct RpcReads {batch_requests:u64,requested_accounts:u64,decoded_account_bytes:u64}
impl RpcReads {
    fn add(&mut self,other:&Self) {
        self.batch_requests+=other.batch_requests;self.requested_accounts+=other.requested_accounts;
        self.decoded_account_bytes+=other.decoded_account_bytes;
    }
}
// Bounded parallel RPC chunks; the transport continues to enforce provider RPS.
async fn fetch_batch(rpc:&RpcHandle,addresses:&[String],reads:&mut RpcReads)->Result<Vec<RpcAccount>> {
    let chunks=addresses.chunks(rpc.account_batch_size()).collect::<Vec<_>>();
    let mut records=Vec::with_capacity(addresses.len());
    for wave in chunks.chunks(4) {
        reads.batch_requests+=wave.len() as u64;
        reads.requested_accounts+=wave.iter().map(|chunk|chunk.len() as u64).sum::<u64>();
        let batches=join_all(wave.iter().map(|chunk|async move {
            let expected=chunk.iter().map(|a|a.parse::<Pubkey>()).collect::<std::result::Result<Vec<_>,_>>()?;
            let answer=rpc.call(RpcOperation::Multiple{addresses:expected,view:AccountView::Full,
                min_context_slot:None}).await?;
            let received_us=answer.received_at_ms.saturating_mul(1000);
            let RpcAnswer::Multiple{slot,accounts}=answer.answer else {bail!("market state expected account batch");};
            ensure!(accounts.len()==chunk.len(),"market state account batch length mismatch");
            let mut result=Vec::with_capacity(chunk.len());
            for (address,account) in chunk.iter().zip(accounts) {
                let account=account.with_context(||format!("market account missing {address}"))?;
                ensure!(account.address.to_string()==address.as_str() && account.view==AccountView::Full,
                    "market account identity/view mismatch");
                result.push(RpcAccount{owner:account.owner.to_string(),data:account.bytes,slot,received_us});
            }
            Ok::<Vec<RpcAccount>,anyhow::Error>(result)
        })).await;
        let mut error=None;
        for batch in batches {match batch {
            Ok(batch)=>{reads.decoded_account_bytes+=batch.iter().map(|a|a.data.len() as u64).sum::<u64>();records.extend(batch);},
            Err(e)=>{error=Some(e);},
        }}
        if let Some(error)=error{return Err(error);}
    }
    Ok(records)
}
#[derive(Clone)]struct Watch {address:String,venue:Venue,manifest_digest:[u8;32]}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
enum Venue {Dlmm,Pump,DammV2,RaydiumAmmV4,RaydiumClmm,RaydiumCpmm,OrcaWhirlpool,PancakeSwapClmm,Manifest,ByrealClmm}
impl Venue {
    fn from_core(v:CoreVenue)->Result<Self> {Ok(match v {
        CoreVenue::MeteoraDlmm=>Self::Dlmm,CoreVenue::PumpSwap=>Self::Pump,
        CoreVenue::MeteoraDammV2=>Self::DammV2,CoreVenue::RaydiumAmmV4=>Self::RaydiumAmmV4,
        CoreVenue::RaydiumClmm=>Self::RaydiumClmm,CoreVenue::RaydiumCpmm=>Self::RaydiumCpmm,
        CoreVenue::OrcaWhirlpool=>Self::OrcaWhirlpool,CoreVenue::PancakeSwapClmm=>Self::PancakeSwapClmm,
        CoreVenue::Manifest=>Self::Manifest,CoreVenue::ByrealClmm=>Self::ByrealClmm,
        CoreVenue::RaydiumStable=>bail!("no market price decoder for Raydium Stable"),
    })}
    fn label(self)->&'static str {match self {
        Self::Dlmm=>"meteora_dlmm",Self::Pump=>"pumpswap",Self::DammV2=>"meteora_damm_v2",
        Self::RaydiumAmmV4=>"raydium_amm_v4",Self::RaydiumClmm=>"raydium_clmm",Self::RaydiumCpmm=>"raydium_cpmm",
        Self::OrcaWhirlpool=>"orca_whirlpool",Self::PancakeSwapClmm=>"pancakeswap_clmm",
        Self::Manifest=>"manifest",Self::ByrealClmm=>"byreal_clmm",
    }}
}
#[derive(Clone)]struct Config {watches:Vec<Watch>}
fn vault_amount(a:&RpcAccount, mint:&str)->Result<u64>{
    ensure!(a.owner==TOKEN_PROGRAM||a.owner==TOKEN_2022,"Invalid token vault owner {}",a.owner);
    ensure!(key(&a.data,0)?==mint,"Vault's mint doesn't match pool mint");u64_le(&a.data,64)
}
fn canonical(a:&str,b:&str)->(String,String){
    let quote=if a==USDC||b==USDC{USDC}else if a==WSOL||b==WSOL{WSOL}
        else if a>b{a}else{b};
    if a==quote{(b.into(),a.into())}else{(a.into(),b.into())}
}
fn price_forward(raw:f64,a:&str,base:&str)->Result<f64>{
    let p=if a==base{raw}else{1.0/raw};
    ensure!(p.is_finite()&&p>0.0,"invalid normalized spot price");Ok(p)
}
fn dlmm_spot(active:i32,step:u16,dec_a:u8,dec_b:u8)->Result<f64>{
    ensure!(step>0,"DLMM bin_step zero");
    let p=(1.0+step as f64/10_000.0).powi(active)*10_f64.powi(i32::from(dec_a)-i32::from(dec_b));
    ensure!(p.is_finite()&&p>0.0,"invalid DLMM spot");Ok(p)
}
fn damm_spot(sqrt_price:u128,da:u8,db:u8)->Result<f64>{
    ensure!(sqrt_price>0,"DAMM V2 sqrt_price zero");
    let s=(sqrt_price as f64)/18_446_744_073_709_551_616.0_f64; // Q64.64
    let p=s*s*10_f64.powi(i32::from(da)-i32::from(db));
    ensure!(p.is_finite()&&p>0.0,"invalid DAMM spot");Ok(p)
}
// AMM v4 swap-invariant indicative reserves are vault balances minus accrued PnL.
// No OpenOrders/quote traversal: only a fast pool-level price indicator.
fn raydium_v4_spot(base:u64,quote:u64,base_pnl:u64,quote_pnl:u64,da:u8,db:u8)->Result<f64>{
    let effective_base=base.checked_sub(base_pnl).context("Raydium base PnL exceeds vault")?;
    let effective_quote=quote.checked_sub(quote_pnl).context("Raydium quote PnL exceeds vault")?;
    ensure!(effective_base>0 && effective_quote>0,"Raydium effective reserve is zero");
    let p=(effective_quote as f64/effective_base as f64)*10_f64.powi(i32::from(da)-i32::from(db));
    ensure!(p.is_finite()&&p>0.0,"invalid Raydium AMM v4 spot");Ok(p)
}
// CPMM PoolState holds fees inside its token vaults. Subtract protocol + fund +
// creator accruals from the vault amounts (Raydium cp-swap PoolState::vault_amount_without_fee).
// `offset` is 341 for a full PoolState and 0 for its accountSubscribe fee slice.
fn cpmm_fee_balances(d:&[u8],offset:usize)->Result<(u64,u64)>{
    let sum=|a:usize,b:usize,c:usize|->Result<u64>{
        let protocol=u64_le(d,offset+a)?;
        let fund=u64_le(d,offset+b)?;
        let creator=u64_le(d,offset+c)?;
        protocol.checked_add(fund).and_then(|v|v.checked_add(creator))
            .context("Raydium CPMM accrued fees overflow")
    };
    Ok((sum(0,16,56)?,sum(8,24,64)?))
}
fn cpmm_spot(base:u64,quote:u64,fee0:u64,fee1:u64,da:u8,db:u8)->Result<f64>{
    let effective0=base.checked_sub(fee0).context("CPMM token0 accrued fees exceed vault balance")?;
    let effective1=quote.checked_sub(fee1).context("CPMM token1 accrued fees exceed vault balance")?;
    ensure!(effective0>0 && effective1>0,"CPMM effective reserve is zero");
    let p=(effective1 as f64/effective0 as f64)*10_f64.powi(i32::from(da)-i32::from(db));
    ensure!(p.is_finite() && p>0.0,"invalid CPMM spot");Ok(p)
}
// Manifest core MarketFixed and RestingOrder are verified against
// hft-team/manifest-sdk/programs/manifest/src/state/{market,resting_order,constants}.rs
// and client/ts/src/{market.ts,utils/redBlackTree.ts}.
// BBO pointers store byte indices relative to the 256-byte dynamic region;
// price is u128 LE / 1e18 quote-atoms/base-atom, decimals normalized below.
// Manifest's best-index is a cheap fast path. If the indexed order expires,
// traverse the same descending-price red-black-tree order used by its SDK.
// Dynamic node: left@0, right@4, payload price@16, size@32,
// last_valid_slot@52, is_bid@56. The two root pointers are at 156/164.
fn manifest_best_prices(d:&[u8],slot:u64)->Result<(f64,f64)> {
    ensure!(d.len()>=MANIFEST_FIXED_SIZE,"Manifest market header truncated");
    ensure!(u64_le(d,0)?==MANIFEST_MARKET_DISC,"Invalid Manifest MarketFixed discriminant");
    let allocated=u32_le(d,152)? as usize;
    ensure!(allocated%MANIFEST_NODE_SIZE==0,"Manifest allocated block count not aligned");
    ensure!(d.len()>=MANIFEST_FIXED_SIZE+allocated,"Manifest dynamic data truncated");
    let da=bytes(d,9,1)?[0];let db=bytes(d,10,1)?[0];
    let node_offset=|idx:u32|->Result<usize>{
        let i=idx as usize;
        ensure!(idx!=MANIFEST_NIL && i%MANIFEST_NODE_SIZE==0
            && i.checked_add(MANIFEST_NODE_SIZE).is_some_and(|end|end<=allocated),
            "Manifest orderbook node offset outside allocated region: {idx}");
        Ok(MANIFEST_FIXED_SIZE+i)
    };
    let best=|root_header:usize,best_header:usize,expected_bid:bool|->Result<f64>{
        let root=u32_le(d,root_header)?;
        let first=u32_le(d,best_header)?;
        ensure!(root!=MANIFEST_NIL && first!=MANIFEST_NIL,"Manifest book side has no orders");
        let quote_at=|index:u32|->Result<Option<f64>>{
            let node=node_offset(index)?;
            let is_bid=bytes(d,node+56,1)?[0];
            ensure!(is_bid==u8::from(expected_bid),"Manifest node is on opposite book side");
            let price_atoms=u128_le(d,node+16)?;
            let base_atoms=u64_le(d,node+32)?;
            ensure!(price_atoms>0 && base_atoms>0,"Manifest resting order has no price/size");
            let expiration=u32_le(d,node+52)?;
            if expiration!=0 && u64::from(expiration)<slot{return Ok(None);}
            let p=price_atoms as f64/1e18 * 10_f64.powi(i32::from(da)-i32::from(db));
            ensure!(p.is_finite() && p>0.0,"Manifest invalid order price");
            Ok(Some(p))
        };
        // Common case: one indexed read; no tree allocations in the hotpath.
        if let Some(price)=quote_at(first)?{return Ok(price);}
        // The first order expired. Descending in-order traversal visits the
        // next executable price even if the top order is still in the tree.
        let mut seen=HashSet::<u32>::new();
        let mut path=Vec::<u32>::new();
        let mut cursor=root;
        while cursor!=MANIFEST_NIL || !path.is_empty(){
            while cursor!=MANIFEST_NIL {
                ensure!(seen.insert(cursor),"Manifest book contains a cycle/duplicate node");
                ensure!(seen.len()<=allocated/MANIFEST_NODE_SIZE,"Manifest book node budget exceeded");
                let off=node_offset(cursor)?;
                path.push(cursor);
                cursor=u32_le(d,off+4)?; // right child: higher competitiveness
            }
            let index=path.pop().context("Manifest orderbook traversal underflow")?;
            if let Some(price)=quote_at(index)?{return Ok(price);}
            cursor=u32_le(d,node_offset(index)?)?; // left subtree: next lower price
        }
        bail!("Manifest book side has no unexpired orders")
    };
    let bid=best(156,160,true)?;
    let ask=best(164,168,false)?;
    ensure!(bid<=ask,"Manifest crossed best bid/ask: {bid} > {ask}");
    Ok((bid,ask))
}
fn pump_spot(base:u64,quote:u64,vquote:i128,da:u8,db:u8)->Result<f64>{
    let effective=i128::from(quote).checked_add(vquote).context("Pump effective reserve overflow")?;
    ensure!(effective>0&&base>0,"invalid PumpSwap effective reserves");
    let p=effective as f64/base as f64*10_f64.powi(i32::from(da)-i32::from(db));
    ensure!(p.is_finite()&&p>0.0,"invalid PumpSwap spot");Ok(p)
}

fn liquidity_digest(profile:&PoolLiquidity)->[u8;32] {
    let mut digest=ring::digest::Context::new(&ring::digest::SHA256);
    digest.update(b"liquidity_amount_profile_v2");digest.update(&profile.account_set_digest);
    let mut string=|value:&str|{digest.update(&(value.len() as u64).to_le_bytes());digest.update(value.as_bytes());};
    for direction in &profile.directions {
        let text:[&str;6]=[&direction.input_mint,&direction.output_mint,direction.status,direction.coverage,
            direction.next_required_account.as_deref().unwrap_or(""),direction.reason.as_deref().unwrap_or("")];
        for value in text {string(value);}
    }
    for direction in &profile.directions {
        digest.update(&[direction.input_decimals,direction.output_decimals,u8::from(direction.monotone_rate_tail)]);
        digest.update(&(direction.levels.len() as u64).to_le_bytes());
        for level in &direction.levels {
            digest.update(&level.source_position.to_le_bytes());
            for rate in [level.gross_rate,level.net_rate,level.fee_rate]{digest.update(&rate.to_bits().to_le_bytes());}
            digest.update(&[u8::from(level.fees_on_input)]);digest.update(&level.output_raw.to_le_bytes());
            digest.update(&level.rate_numerator.to_le_bytes());digest.update(&level.rate_denominator.to_le_bytes());
            for n in level.output_parts_raw{digest.update(&n.to_le_bytes());}
            digest.update(&level.fee_numerator.to_le_bytes());digest.update(&level.fee_denominator.to_le_bytes());
            digest.update(&level.maximum_rebate_bps.to_le_bytes());
            for raw in [level.input_raw,level.minimum_input_raw,level.fee_raw] {
                digest.update(&[u8::from(raw.is_some())]);digest.update(&raw.unwrap_or(0).to_le_bytes());
            }
        }
    }
    if let Some(curve)=&profile.reserve_curve {
        digest.update(&[1]);digest.update(curve.base_mint.as_bytes());
        for n in [curve.base_reserve_raw,curve.effective_quote_reserve_raw,curve.spendable_quote_reserve_raw] {
            digest.update(&n.to_le_bytes());
        }
        for n in curve.fee_bps{digest.update(&n.to_le_bytes());}
        digest.update(&curve.buy_input_padding_raw.to_le_bytes());
    }else{digest.update(&[0]);}
    digest.finish().as_ref().try_into().expect("SHA256 length")
}

#[derive(Clone)]enum State{
    Pending{dlmm:Option<dlmm::Dependencies>,vaults:Option<PoolVaults>},
    Dlmm{active:i32,step:u16,fee:fees::Dlmm,book:Box<dlmm::Book>},
    Pump{vquote:i128,base:u64,quote:u64,vaults:PoolVaults,
         pool_slot:u64,base_slot:u64,quote_slot:u64,fee_pool:fees::PumpPool,
         global:Option<Arc<fees::Global>>,config:Option<Arc<fees::Config>>,supply:Option<u64>,liquidity:Option<Arc<PoolLiquidity>>},
    DammV2{sqrt:u128},
    RaydiumClmm{sqrt:u128},
    OrcaWhirlpool{sqrt:u128},
    PancakeSwapClmm{sqrt:u128},
    ByrealClmm{sqrt:u128},
    Manifest{bid:f64,ask:f64,available:bool},
    RaydiumAmmV4{base:u64,quote:u64,base_pnl:u64,quote_pnl:u64,
        base_vault:String,quote_vault:String,
        pool_slot:u64,base_slot:u64,quote_slot:u64},
    RaydiumCpmm{base:u64,quote:u64,fee0:u64,fee1:u64,
        base_vault:String,quote_vault:String,
        pool_slot:u64,base_slot:u64,quote_slot:u64},
}
#[derive(Clone)]struct Pool{
    key:Pubkey,ready:bool,id:String,venue:Venue,a:String,b:String,da:u8,db:u8,
    canon_base:String,canon_quote:String,state:State,
    price:f64,available:bool,slot:u64,received_us:u64,updates:u64,
    clock:i64,epoch:u64,mint_states:[Option<Arc<crate::core::MintOnchainState>>;2],
    last_valuation:Option<FeeValuation>,
    manifest_digest:[u8;32],
    prepared_at:Instant,
}
impl Pool{
    fn reserve_profile(&self)->Option<PoolLiquidity> {
        let State::Pump{base,quote,vquote,fee_pool,..}=&self.state else{return None;};
        let value=self.valuation()?;
        let fee_bps=[value.lp_fee_bps?,value.protocol_fee_bps?,value.creator_fee_bps?];
        let effective=u64::try_from(i128::from(*quote).checked_add(*vquote)?).ok()?;
        let spendable=quote.checked_sub(fee_pool.protocol_fees.checked_add(fee_pool.creator_fees)?)?;
        let mut directions=std::array::from_fn(|i| {
            let forward=i==0;
            let input=if forward{&self.canon_base}else{&self.canon_quote};
            let output=if forward{&self.canon_quote}else{&self.canon_base};
            let input_is_base=input==&self.a;
            DirectedLiquidity{input_mint:input.clone(),output_mint:output.clone(),
                input_decimals:if input_is_base{self.da}else{self.db},
                output_decimals:if input_is_base{self.db}else{self.da},
                status:if value.estimated_net_rates[i].is_some(){"available"}else{"empty"},coverage:"reserve_curve",
                next_required_account:None,first_source_position:None,marginal_net_rate:value.estimated_net_rates[i],monotone_rate_tail:true,
                input_capacity_raw:None,output_capacity_raw:if input_is_base{spendable}else{*base}.to_string(),
                levels:Vec::new(),reason:None}
        });
        for d in &mut directions {if d.status=="empty"{d.reason=Some("trade direction disabled".into());}}
        Some(PoolLiquidity{model:"constant_product_quote_fees_v1",revision:0,content_digest:[0;32],
            account_set_digest:self.manifest_digest,execution_accounts:Vec::new(),
            reserve_curve:Some(ReserveCurve{base_mint:self.a.clone(),base_reserve_raw:*base,
                effective_quote_reserve_raw:effective,spendable_quote_reserve_raw:spendable,fee_bps,buy_input_padding_raw:1}),directions})
    }
    fn refresh_liquidity(&mut self)->bool {
        if self.venue==Venue::Pump {
            let mut next=self.reserve_profile();
            if let Some(p)=next.as_mut(){p.content_digest=liquidity_digest(p);}
            let State::Pump{liquidity,..}=&mut self.state else{unreachable!()};
            if liquidity.as_ref().map(|p|p.content_digest)==next.as_ref().map(|p|p.content_digest){return false;}
            if let Some(p)=next.as_mut(){p.revision=liquidity.as_ref().map_or(1,|old|old.revision.saturating_add(1));}
            *liquidity=next.map(Arc::new);return true;
        }
        match &mut self.state {
            State::Dlmm{active,step,fee,book}=>book.rebuild(*active,*step,fee,self.clock,
                &self.a,&self.b,self.da,self.db,&self.canon_base),
            _=>false,
        }
    }
    fn liquidity(&self)->Option<Arc<crate::core::market_state::PoolLiquidity>> {
        match &self.state{State::Dlmm{book,..}=>book.profile.clone(),State::Pump{liquidity,..}=>liquidity.clone(),_=>None}
    }
    fn valuation(&self)->Option<FeeValuation> {
        if !matches!(self.venue,Venue::Dlmm|Venue::Pump) || matches!(&self.state,State::Pending{..}){return None;}
        let mut token_status=std::array::from_fn(|i|match &self.mint_states[i] {
            None=>"unknown",
            Some(m)=>match m.token_extensions.get("classification").and_then(serde_json::Value::as_str) {
                Some("verified_no_standard_transfer_fee")=>"verified_no_standard_transfer_fee",
                Some("transfer_fee_configured")=>"amount_dependent_transfer_fee",
                _=>"unresolved_transfer_behavior",
            }
        });
        if self.a!=self.canon_base{token_status.swap(0,1);}
        let mut v=FeeValuation{model:if self.venue==Venue::Dlmm{"dlmm"}else{"pumpswap"},
            estimated_net_rates:[None;2],dex_fee_rates:[None;2],fees_on_input:[true;2],
            base_fee_rate:None,variable_fee_rate:None,lp_fee_bps:None,protocol_fee_bps:None,creator_fee_bps:None,
            fee_schedule:None,market_cap_raw:None,chain_timestamp:self.clock,chain_epoch:self.epoch,
            token_fee_status:token_status,unknown_reason:None,scope:"marginal_dex_fees_only_no_host_rebate_no_depth"};
        let calculated=(||->Result<[f64;2]>{
            ensure!(self.clock>0,"chain Clock unavailable");
            let raw=match &self.state {
                State::Dlmm{active,step,fee,book}=>{
                    let (base,total,on_input)=fee.rates(*active,*step,self.clock)?;
                    v.base_fee_rate=Some(base);v.variable_fee_rate=Some(total-base);
                    v.dex_fee_rates=[Some(total);2];v.fees_on_input=on_input;
                    v.scope="reachable_execution_window_dex_net_no_sizing";
                    let profile=book.profile.as_ref().context("DLMM liquidity profile unavailable")?;
                    // Profiles already use the canonical mint direction. Convert
                    // to native here; the shared normalization below runs once.
                    let mut rates=[0.0;2];
                    for native in 0..2 {
                        let direction=native^usize::from(self.a!=self.canon_base);
                        let value=&profile.directions[direction];
                        rates[native]=value.marginal_net_rate.unwrap_or(0.0);
                        v.dex_fee_rates[native]=value.levels.iter().find(|l|l.output_raw>0).map(|l|l.fee_rate);
                        v.fees_on_input[native]=value.levels.iter().find(|l|l.output_raw>0).map_or(on_input[native],|l|l.fees_on_input);
                    }
                    if rates.contains(&0.0){v.unknown_reason=Some("DLMM direction empty or outside observed execution window; inspect liquidity".into());}
                    rates
                },
                State::Pump{fee_pool,global,config,supply,base,quote,vquote,..}=>{
                    let global=global.as_ref().context("Pump global fees unavailable")?;
                    let config=config.as_ref().context("Pump fee schedule unavailable")?;
                    let effective=i128::from(*quote).checked_add(*vquote).context("Pump effective reserve overflow")?;
                    let (rates,schedule,cap)=config.rates(fee_pool,global,&self.b,
                        supply.context("Pump base supply unavailable")?,*base,effective)?;
                    let f=rates.total()?;v.lp_fee_bps=Some(rates.lp);v.protocol_fee_bps=Some(rates.protocol);
                    v.creator_fee_bps=Some(rates.creator);v.dex_fee_rates=[Some(f);2];
                    v.fee_schedule=Some(schedule);v.market_cap_raw=Some(cap.to_string());v.fees_on_input=[false,true];
                    let p=pump_spot(*base,*quote,*vquote,self.da,self.db)?;
                    // Pump charges on quote: sell subtracts fees, buy adds fees to quote cost.
                    [if global.disabled&16==0{p*(1.0-f)}else{0.0},
                     if global.disabled&8==0{(1.0/p)/(1.0+f)}else{0.0}]
                },_=>unreachable!(),
            };
            Ok(if self.a==self.canon_base{raw}else{v.dex_fee_rates.swap(0,1);v.fees_on_input.swap(0,1);[raw[1],raw[0]]})
        })();
        match calculated {
            Ok(rates)=>{v.estimated_net_rates=rates.map(|r|(r.is_finite()&&r>0.0).then_some(r));},
            Err(e)=>v.unknown_reason=Some(e.to_string()),
        }
        // Token transfer fees have caps/rounding and require an amount. Do not claim
        // these DEX-only estimates represent the final wallet net rate.
        if token_status.iter().any(|s|*s!="verified_no_standard_transfer_fee") && v.unknown_reason.is_none(){
            v.unknown_reason=Some("token transfer cost requires amount-specific evaluation".into());
        }
        Some(v)
    }
    fn compute_price(&self)->Result<f64>{
        let raw=match &self.state{
            State::Pending{..}=>bail!("pool awaits its first dynamic baseline"),
            State::Dlmm{active,step,..}=>dlmm_spot(*active,*step,self.da,self.db)?,
            State::Pump{vquote,base,quote,..}=>pump_spot(*base,*quote,*vquote,self.da,self.db)?,
            State::DammV2{sqrt}=>damm_spot(*sqrt,self.da,self.db)?,
            State::RaydiumClmm{sqrt}=>damm_spot(*sqrt,self.da,self.db)?,
            State::OrcaWhirlpool{sqrt}=>damm_spot(*sqrt,self.da,self.db)?,
            State::PancakeSwapClmm{sqrt}=>damm_spot(*sqrt,self.da,self.db)?,
            State::ByrealClmm{sqrt}=>damm_spot(*sqrt,self.da,self.db)?,
            State::Manifest{bid,ask,available}=>{
                ensure!(*available,"Manifest BBO unavailable");
                let (canon_bid,canon_ask)=if self.a==self.canon_base {(*bid,*ask)}
                    else {(1.0/ask,1.0/bid)};
                let midpoint=(canon_bid+canon_ask)/2.0;
                ensure!(midpoint.is_finite()&&midpoint>0.0,"Manifest invalid midpoint");
                return Ok(midpoint);
            },
            State::RaydiumAmmV4{base,quote,base_pnl,quote_pnl,..}=>
                raydium_v4_spot(*base,*quote,*base_pnl,*quote_pnl,self.da,self.db)?,
            State::RaydiumCpmm{base,quote,fee0,fee1,..}=>
                cpmm_spot(*base,*quote,*fee0,*fee1,self.da,self.db)?,
        };
        price_forward(raw,&self.a,&self.canon_base)
    }
    fn manifest_quotes(&self)->Option<(f64,f64)> {
        if let State::Manifest{bid,ask,available:true}=&self.state {
            if self.a==self.canon_base {Some((*bid,*ask))}
            else {Some((1.0/ask,1.0/bid))}
        } else {None}
    }
    fn price_kind(&self)->PriceKind {
        match self.venue {
            Venue::Dlmm=>PriceKind::DlmmActiveBin,
            Venue::Manifest=>PriceKind::OrderbookMidpoint,
            Venue::Pump|Venue::RaydiumAmmV4|Venue::RaydiumCpmm=>PriceKind::ReserveSpot,
            _=>PriceKind::ClmmSpot,
        }
    }
    fn consistency(&self)->Consistency {
        match &self.state {
            State::Pump{pool_slot,base_slot,quote_slot,..}|
            State::RaydiumAmmV4{pool_slot,base_slot,quote_slot,..}|
            State::RaydiumCpmm{pool_slot,base_slot,quote_slot,..}=>{
                if *pool_slot==*base_slot && *base_slot==*quote_slot {
                    Consistency::SameSlotNonAtomic
                } else {Consistency::MixedSlotNonAtomic}
            }
            State::Manifest{..}=>Consistency::OrderbookBboIndicative,
            _=>Consistency::SingleAccount,
        }
    }
}
#[derive(Clone,Copy,Debug,PartialEq,Eq,Hash)]enum Field{Clock,PumpGlobal,PumpFees,PumpSupply,Dlmm,DlmmArray(u8),DlmmBitmap,VirtualQuote,BaseVault,QuoteVault,DammSqrt,RaydiumV4Pnl,RaydiumV4BaseVault,RaydiumV4QuoteVault,RaydiumClmmSqrt,RaydiumCpmmFees,RaydiumCpmmBaseVault,RaydiumCpmmQuoteVault,OrcaWhirlpoolSqrt,PancakeSwapClmmSqrt,ManifestBook,ByrealClmmSqrt}
impl Field{
    fn full(self)->bool{matches!(self,Self::Dlmm|Self::DlmmArray(_)|Self::DlmmBitmap|Self::VirtualQuote|Self::Clock|Self::PumpGlobal|Self::PumpFees|Self::PumpSupply|Self::ManifestBook)}

    fn label(self)->&'static str{match self{Self::Clock=>"chain_clock",Self::PumpGlobal=>"pump_global_fees",Self::PumpFees=>"pump_fee_config",Self::PumpSupply=>"pump_base_supply",Self::Dlmm=>"dlmm_lbpair",Self::DlmmArray(_)=>"dlmm_bin_array",Self::DlmmBitmap=>"dlmm_bitmap",Self::VirtualQuote=>"pump_pool",Self::BaseVault=>"pump_base_vault",Self::QuoteVault=>"pump_quote_vault",Self::DammSqrt=>"damm_v2_pool",Self::RaydiumV4Pnl=>"raydium_v4_pool_pnl",
        Self::RaydiumV4BaseVault=>"raydium_v4_base_vault",Self::RaydiumV4QuoteVault=>"raydium_v4_quote_vault",
        Self::RaydiumClmmSqrt=>"raydium_clmm_pool",
        Self::RaydiumCpmmFees=>"raydium_cpmm_pool_fees",Self::RaydiumCpmmBaseVault=>"raydium_cpmm_base_vault",
        Self::RaydiumCpmmQuoteVault=>"raydium_cpmm_quote_vault",
        Self::OrcaWhirlpoolSqrt=>"orca_whirlpool_pool",
        Self::PancakeSwapClmmSqrt=>"pancakeswap_clmm_pool",Self::ManifestBook=>"manifest_market_book",
        Self::ByrealClmmSqrt=>"byreal_clmm_pool"}}
    fn slice(self)->(usize,usize){match self{Self::Dlmm|Self::DlmmArray(_)|Self::DlmmBitmap|Self::VirtualQuote|Self::Clock|Self::PumpGlobal|Self::PumpFees|Self::PumpSupply=>(0,0),Self::BaseVault|Self::QuoteVault=>(64,8),Self::DammSqrt=>(DAMM_SQRT_OFFSET,16),Self::RaydiumV4Pnl=>(192,16),
        Self::RaydiumV4BaseVault|Self::RaydiumV4QuoteVault=>(64,8),
        Self::RaydiumClmmSqrt=>(RAYDIUM_CLMM_SQRT_OFFSET,16),
        Self::RaydiumCpmmFees=>(RAYDIUM_CPMM_FEES_OFFSET,RAYDIUM_CPMM_FEES_LEN),
        Self::RaydiumCpmmBaseVault|Self::RaydiumCpmmQuoteVault=>(64,8),
        Self::OrcaWhirlpoolSqrt=>(ORCA_WHIRLPOOL_SQRT_OFFSET,16),
        Self::PancakeSwapClmmSqrt=>(PANCAKESWAP_CLMM_SQRT_OFFSET,16),
        Self::ByrealClmmSqrt=>(BYREAL_CLMM_SQRT_OFFSET,16),
        Self::ManifestBook=>(0,0) // full variable-length market account; no dataSlice
    }}
}
#[derive(Clone)]struct Subscription{pool:usize,field:Field,address:String,key:Pubkey,owner:Pubkey,
    slot:u64,received_us:u64,floor:Option<u64>,valid:bool}
// Fixed-size counters/histograms; no per-update logging or sample serialization.
#[derive(Default)]
struct Latency {count:u64,total:u128,max:u64,bins:[u64;32]}
impl Latency {
    fn add(&mut self,ns:u64){self.count+=1;self.total+=u128::from(ns);self.max=self.max.max(ns);self.bins[(64-ns.leading_zeros() as usize).min(31)]+=1;}
    fn report(&self)->LatencyReport {
        let upper=|percent:u64|{let target=(self.count*percent).div_ceil(100);let mut count=0;
            self.bins.iter().enumerate().find_map(|(i,n)|{count+=n;(count>=target && target>0).then(||if i==31{self.max}else{((1_u64<<i)-1).min(self.max)})})};
        LatencyReport{count:self.count,mean_ns:(self.count>0).then(||self.total as f64/self.count as f64),
            max_ns:(self.count>0).then_some(self.max),p95_upper_ns:upper(95),p99_upper_ns:upper(99)}
    }
}
#[derive(serde::Serialize)]
struct LatencyReport {count:u64,mean_ns:Option<f64>,max_ns:Option<u64>,p95_upper_ns:Option<u64>,p99_upper_ns:Option<u64>}
impl serde::Serialize for Latency {
    fn serialize<S:serde::Serializer>(&self,s:S)->std::result::Result<S::Ok,S::Error> {
        serde::Serialize::serialize(&self.report(),s)
    }
}
#[derive(Default,serde::Serialize)]
struct Metrics {
    received:u64,unmapped:u64,inactive:u64,initializing:u64,buffer_replaced:u64,replayed:u64,
    baseline_skipped:u64,stale:u64,bindings:u64,deleted:u64,gaps:u64,lost:u64,rejections:u64,
    baseline_prices:u64,websocket_prices:u64,no_receiver:u64,max_queue_depth:usize,
    selection_changes:u64,hydration_jobs:u64,discarded_results:u64,hydration_failures:u64,
    index_rebuilds:u64,structure_installs:u64,baseline_installs:u64,baseline_pools_installed:u64,
    max_hydration_in_flight:usize,initial_hydration:Latency,baseline_hydration:Latency,
    prepared_structures_by_venue:BTreeMap<&'static str,u64>,prepared_structure_build:Latency,
    prepared_to_first_baseline:Latency,structure_rpc:RpcReads,baseline_rpc:RpcReads,
    processing:Latency,queue:Latency,install:Latency,watch_plan:Latency,decode_errors:u64,
    post_rewatch_websocket_observations:u64,removals:u64,
    fee_valuation:FeeEvidence,liquidity:LiquidityEvidence,
    reserve_curve_observations:u64,reserve_curve_build:Latency,
    preparation_final:Option<PreparationSample>,subscription_gaps:BTreeMap<String,u64>,
    subscription_gap_samples:VecDeque<serde_json::Value>,
    baseline_failure_samples:Vec<serde_json::Value>,
    #[serde(skip)] cleared:bool,#[serde(skip)] rewatching:bool,
}
#[derive(serde::Serialize)]
struct FeeSample {observation:PoolPriceObservation,inputs:serde_json::Value}
#[derive(Default,serde::Serialize)]
struct FeeEvidence {
    observations_by_model:BTreeMap<&'static str,u64>,
    known_dex_fees_by_model:BTreeMap<&'static str,u64>,
    unknown_by_reason:BTreeMap<String,u64>,
    updates_by_field:BTreeMap<&'static str,u64>,
    unchanged_net_rate_updates:u64,revaluations:u64,asymmetric_rates:u64,
    calculation:Latency,samples:Vec<FeeSample>,invalid_reference_samples:Vec<serde_json::Value>,
    clock_reference_lag_observations:u64,clock_reference_lag_max_seconds:u64,
    clock_reference_lag_samples:Vec<serde_json::Value>,
}
#[derive(Default,serde::Serialize)]
struct LiquidityEvidence {
    profile_observations:u64,profile_changes:u64,array_websocket_updates:u64,bitmap_websocket_updates:u64,
    directions_by_status:BTreeMap<&'static str,u64>,directions_by_coverage:BTreeMap<&'static str,u64>,
    first_level_differs_from_active:u64,cross_array_directions:u64,negative_bin_directions:u64,profile_build:Latency,
    samples:Vec<serde_json::Value>,focused:BTreeMap<String,FocusedLiquidity>,
    #[serde(skip)] focus:HashSet<String>,
}
#[derive(serde::Serialize)]
struct FocusedLiquidity {observations:u64,first:serde_json::Value,last:serde_json::Value}
struct Engine {
    pools: Vec<Pool>, subs: Vec<Subscription>, metrics:Metrics,
    events: broadcast::Sender<Arc<MarketStateEvent>>, sequence: u64,trace:bool,
    account_ranges:HashMap<Pubkey,std::ops::Range<usize>>, account_bindings:Vec<usize>,
    pool_subs:Vec<Vec<usize>>,
    pool_index:HashMap<Pubkey,usize>,
    shared_fees:HashMap<Pubkey,(u64,u64,fees::Shared)>,
}
impl Engine{
    fn empty(events:broadcast::Sender<Arc<MarketStateEvent>>,sequence:u64)->Self {
        let mut metrics=Metrics::default();
        metrics.liquidity.focus=env::var("MARKET_STATE_LIQUIDITY_EVIDENCE_POOLS").unwrap_or_default()
            .split(',').map(str::trim).filter(|v|!v.is_empty()).map(str::to_owned).collect();
        Self{pools:Vec::new(),subs:Vec::new(),metrics,events,sequence,trace:false,
            account_ranges:HashMap::new(),account_bindings:Vec::new(),pool_subs:Vec::new(),pool_index:HashMap::new(),shared_fees:HashMap::new()}
    }
    fn index(&mut self) {
        self.metrics.index_rebuilds+=1;
        self.pool_index=self.pools.iter().enumerate().map(|(i,p)|(p.key,i)).collect();
        let mut by_account=HashMap::<Pubkey,Vec<usize>>::new();
        self.pool_subs=vec![Vec::new();self.pools.len()];
        for (i,s) in self.subs.iter().enumerate() {
            by_account.entry(s.key).or_default().push(i);self.pool_subs[s.pool].push(i);
        }
        self.account_ranges.clear();self.account_bindings.clear();
        for (key,indices) in by_account {
            let start=self.account_bindings.len();self.account_bindings.extend(indices);
            self.account_ranges.insert(key,start..self.account_bindings.len());
        }
    }
    fn retain(&mut self,keep:&HashSet<Pubkey>) {
        if self.retain_unindexed(keep) {self.index();}
    }
    // Structural installation will rebuild once, after removals and additions.
    fn retain_unindexed(&mut self,keep:&HashSet<Pubkey>)->bool {
        if self.pools.iter().all(|p|keep.contains(&p.key)) {return false;}
        let mut remap=vec![None;self.pools.len()];let mut next=0;
        for (i,p) in self.pools.iter().enumerate(){if keep.contains(&p.key){remap[i]=Some(next);next+=1;}}
        self.pools.retain(|p|keep.contains(&p.key));
        self.subs.retain_mut(|s|match remap[s.pool]{Some(pi)=>{s.pool=pi;true},None=>false});
        true
    }
    // Copy only selected Prepared pools and their existing static account descriptors.
    // The baseline phase must not re-read mint metadata or re-decode pool identities.
    fn subset(&self,selected:&HashSet<Pubkey>)->Engine {
        let mut part=Engine::empty(self.events.clone(),0);
        let mut indices:Vec<_>=selected.iter().filter_map(|key|self.pool_index.get(key).copied()).collect();
        indices.sort_unstable();
        for i in indices {
            let pi=part.pools.len();part.pools.push(self.pools[i].clone());
            for &binding in &self.pool_subs[i] {
                let mut next=self.subs[binding].clone();next.pool=pi;part.subs.push(next);
            }
        }
        part.index();part
    }
    // Read only dynamic subscribed account values, after every subscription is active.
    // Keep all updates private to the staged batch until one coherent baseline is ready.
    async fn refresh_baseline(&mut self,rpc:&RpcHandle,reads:&mut RpcReads)->Result<()> {
        let mut addresses=Vec::new();let mut seen=HashSet::new();
        for sub in &self.subs {
            if seen.insert(sub.address.clone()){addresses.push(sub.address.clone());}
        }
        let accounts=fetch_batch(rpc,&addresses,reads).await?;
        let dict:HashMap<_,_>=addresses.into_iter().zip(accounts).collect();
        for sub in &mut self.subs {sub.valid=false;}
        for pool in &mut self.pools {pool.available=false;pool.last_valuation=None;}
        for i in 0..self.subs.len() {
            let sub=&self.subs[i];
            let acc=dict.get(&sub.address).context("baseline account missing")?;
            let owner:Pubkey=acc.owner.parse()?;
            if matches!(sub.field,Field::BaseVault|Field::QuoteVault) {
                let pool=&self.pools[sub.pool];
                vault_amount(acc,if sub.field==Field::BaseVault{&pool.a}else{&pool.b})?;
                ensure!(key(&acc.data,32)?==pool.id,"Pump vault authority changed");
            }
            let (offset,length)=sub.field.slice();
            let data=if sub.field.full() {acc.data.as_slice()}
                else if sub.field==Field::VirtualQuote && acc.data.len()<offset+length {&[]}
                else {bytes(&acc.data,offset,length)?};
            self.subs[i].slot=acc.slot;
            self.subs[i].received_us=acc.received_us;
            self.subs[i].floor=Some(acc.slot);
            let timing=PricePipelineTiming{frame_received_at_us:acc.received_us,
                transport_decoded_at_us:acc.received_us,market_received_at_us:acc.received_us,
                price_published_at_us:0,market_decode_ns:0,market_compute_ns:0,initialization_replay:false};
            self.update(i,acc.slot,owner,data,acc.received_us,timing,false)?;
        }
        Ok(())
    }
    // Install only addresses and verified mint metadata. This does no RPC and
    // publishes no price; the ordinary post-ACK baseline initializes live state.
    fn prepared(inputs:&[PoolInput],mints:&dyn MintReader,events:broadcast::Sender<Arc<MarketStateEvent>>)->Result<Self> {
        let mut engine=Self::empty(events,0);
        let dlmm_program:Pubkey=DLMM_PROGRAM.parse()?;let pump_program:Pubkey=PUMP_PROGRAM.parse()?;
        let global:Pubkey=fees::GLOBAL.parse()?;let fee_program:Pubkey=fees::FEE_PROGRAM.parse()?;
        let fee_config=fees::config_address();let clock:Pubkey=fees::CLOCK.parse()?;
        let sysvar_program:Pubkey="Sysvar1111111111111111111111111111111111111".parse()?;
        for p in inputs {
            let venue=Venue::from_core(p.venue)?;
            let a=mints.mint(p.mint_a).and_then(|m|m.onchain).context("prepared mint A proof missing")?;
            let b=mints.mint(p.mint_b).and_then(|m|m.onchain).context("prepared mint B proof missing")?;
            ensure!(a.address==p.mint_a && b.address==p.mint_b,"prepared mint identity mismatch");
            let mut bindings=vec![(if venue==Venue::Dlmm{Field::Dlmm}else{Field::VirtualQuote},p.address,
                if venue==Venue::Dlmm{dlmm_program}else{pump_program})];
            match venue {
                Venue::Dlmm=>{
                    let deps=p.dlmm.context("DLMM Prepared manifest dependencies missing")?;
                    for (i,&key) in deps.addresses().iter().enumerate() {bindings.push((Field::DlmmArray(i as u8),key,dlmm_program));}
                    if let Some(key)=deps.bitmap {bindings.push((Field::DlmmBitmap,key,dlmm_program));}
                },
                Venue::Pump=>{
                    let v=p.vaults.context("PumpSwap Prepared vault roles missing")?;
                    bindings.extend([(Field::BaseVault,v.vault_a,a.owner),(Field::QuoteVault,v.vault_b,b.owner),
                        (Field::PumpGlobal,global,pump_program),
                        (Field::PumpFees,fee_config,fee_program),
                        (Field::PumpSupply,p.mint_a,a.owner)]);
                },
                _=>bail!("venue has no direct prepared structure"),
            }
            bindings.push((Field::Clock,clock,sysvar_program));
            let (mint_a,mint_b)=(p.mint_a.to_string(),p.mint_b.to_string());
            let (canon_base,canon_quote)=canonical(&mint_a,&mint_b);
            let pi=engine.pools.len();
            engine.pools.push(Pool{key:p.address,ready:false,id:p.address.to_string(),venue,a:mint_a,b:mint_b,
                da:a.decimals,db:b.decimals,canon_base,canon_quote,state:State::Pending{dlmm:p.dlmm,vaults:p.vaults},price:0.0,
                available:false,slot:0,received_us:0,updates:0,clock:0,epoch:0,mint_states:[Some(a),Some(b)],
                last_valuation:None,manifest_digest:p.manifest_digest,prepared_at:Instant::now()});
            for (field,key,owner) in bindings {engine.subs.push(Subscription{pool:pi,field,key,address:key.to_string(),owner,
                slot:0,received_us:0,floor:None,valid:false});}
        }
        Ok(engine)
    }
    fn install_structure(&mut self,mut part:Engine)->bool {
        if part.pools.is_empty() {return false;}
        let replaced:HashSet<_>=part.pools.iter().map(|p|p.key).collect();
        let keep=self.pools.iter().filter(|p|!replaced.contains(&p.key)).map(|p|p.key).collect();
        self.retain_unindexed(&keep);
        let offset=self.pools.len();for sub in &mut part.subs{sub.pool+=offset;}
        self.pools.extend(part.pools);self.subs.extend(part.subs);self.index();
        self.metrics.structure_installs+=1;
        true
    }
    // A validated baseline changes dynamic state only. Validate the whole batch
    // before writing, then keep every live pool position and account binding.
    fn install_baseline(&mut self,part:Engine)->Result<Vec<usize>> {
        let mut positions=Vec::with_capacity(part.pools.len());
        for (source,pool) in part.pools.iter().enumerate() {
            let pi=*self.pool_index.get(&pool.key).context("baseline pool no longer installed")?;
            let old=&self.pools[pi];
            ensure!(!old.ready,"baseline would overwrite a ready pool {}",pool.id);
            ensure!(old.venue==pool.venue && old.a==pool.a && old.b==pool.b
                && old.manifest_digest==pool.manifest_digest
                && old.da==pool.da && old.db==pool.db
                && old.canon_base==pool.canon_base && old.canon_quote==pool.canon_quote,
                "baseline pool identity changed {}",pool.id);
            let before=&self.pool_subs[pi];let after=&part.pool_subs[source];
            ensure!(before.len()==after.len(),"baseline binding count changed {}",pool.id);
            for (&target,&incoming) in before.iter().zip(after) {
                let a=&self.subs[target];let b=&part.subs[incoming];
                ensure!(a.key==b.key && a.field==b.field && a.owner==b.owner && a.address==b.address,
                    "baseline account binding changed {}",pool.id);
            }
            positions.push(pi);
        }
        for (source,(pool,&pi)) in part.pools.into_iter().zip(&positions).enumerate() {
            let old=&mut self.pools[pi];
            if matches!(&old.state,State::Pending{..}) {
                self.metrics.prepared_to_first_baseline.add(nanos(old.prepared_at));
            }
            old.ready=pool.ready;old.state=pool.state;old.price=pool.price;
            old.available=pool.available;old.slot=pool.slot;
            old.received_us=pool.received_us;old.updates=pool.updates;old.clock=pool.clock;old.epoch=pool.epoch;old.mint_states=pool.mint_states;old.last_valuation=pool.last_valuation;
            for (&target,&incoming) in self.pool_subs[pi].iter().zip(&part.pool_subs[source]) {
                let a=&mut self.subs[target];let b=&part.subs[incoming];
                a.slot=b.slot;a.received_us=b.received_us;a.floor=b.floor;a.valid=b.valid;
            }
        }
        if !positions.is_empty() {
            self.metrics.baseline_installs+=1;
            self.metrics.baseline_pools_installed+=positions.len() as u64;
        }
        Ok(positions)
    }
    async fn hydrate(cfg:Config,rpc:&RpcHandle,events:broadcast::Sender<Arc<MarketStateEvent>>,sequence:u64,mints:&dyn MintReader,reads:&mut RpcReads)->Result<Self>{
        let pool_addrs:Vec<_>=cfg.watches.iter().map(|w|w.address.clone()).collect();
        let pool_records=fetch_batch(rpc,&pool_addrs,reads).await?;
        let mut raw_pools=Vec::new();let mut details=Vec::<String>::new();
        for (watch,acc) in cfg.watches.iter().zip(&pool_records){
            let pool_slot=acc.slot;
            let (a,b,state)=match watch.venue{
                Venue::Dlmm|Venue::Pump=>bail!("prepared pool must use the direct structure path"),
                Venue::DammV2=>{
                    ensure!(acc.owner==DAMM_V2_PROGRAM&&acc.data.starts_with(&POOL_DISC),"{} not a DAMM V2 Pool",watch.address);
                    ensure!(acc.data.len()>=472,"DAMM V2 Pool layout shorter than expected: {}",acc.data.len());
                    (key(&acc.data,168)?,key(&acc.data,200)?,State::DammV2{sqrt:u128_le(&acc.data,DAMM_SQRT_OFFSET)?})
                },
                Venue::RaydiumClmm=>{
                    ensure!(acc.owner==RAYDIUM_CLMM_PROGRAM && acc.data.starts_with(&RAYDIUM_CLMM_DISC),
                        "{} not a Raydium CLMM PoolState",watch.address);
                    ensure!(acc.data.len()>=RAYDIUM_CLMM_SQRT_OFFSET+16,
                        "Raydium CLMM account too short {}: {} bytes",watch.address,acc.data.len());
                    (key(&acc.data,73)?,key(&acc.data,105)?,
                        State::RaydiumClmm{sqrt:u128_le(&acc.data,RAYDIUM_CLMM_SQRT_OFFSET)?})
                },
                Venue::PancakeSwapClmm=>{
                    ensure!(acc.owner==PANCAKESWAP_CLMM_PROGRAM && acc.data.starts_with(&PANCAKESWAP_CLMM_DISC),
                        "{} not a PancakeSwap CLMM PoolState",watch.address);
                    ensure!(acc.data.len()>=PANCAKESWAP_CLMM_SQRT_OFFSET+16,
                        "PancakeSwap CLMM account too short {}: {} bytes",watch.address,acc.data.len());
                    (key(&acc.data,73)?,key(&acc.data,105)?,
                        State::PancakeSwapClmm{sqrt:u128_le(&acc.data,PANCAKESWAP_CLMM_SQRT_OFFSET)?})
                },
                Venue::ByrealClmm=>{
                    ensure!(acc.owner==BYREAL_CLMM_PROGRAM && acc.data.starts_with(&BYREAL_CLMM_DISC),
                        "{} not a Byreal CLMM PoolState",watch.address);
                    ensure!(acc.data.len()>=BYREAL_CLMM_SQRT_OFFSET+16,
                        "Byreal CLMM account too short {}: {} bytes",watch.address,acc.data.len());
                    (key(&acc.data,73)?,key(&acc.data,105)?,
                        State::ByrealClmm{sqrt:u128_le(&acc.data,BYREAL_CLMM_SQRT_OFFSET)?})
                },
                Venue::OrcaWhirlpool=>{
                    ensure!(acc.owner==ORCA_WHIRLPOOL_PROGRAM && acc.data.starts_with(&ORCA_WHIRLPOOL_DISC),
                        "{} not an Orca Whirlpool",watch.address);
                    ensure!(acc.data.len()==ORCA_WHIRLPOOL_ACCOUNT_LEN,
                        "{} Orca Whirlpool account length {} != {}",watch.address,acc.data.len(),ORCA_WHIRLPOOL_ACCOUNT_LEN);
                    (key(&acc.data,101)?,key(&acc.data,181)?,
                        State::OrcaWhirlpool{sqrt:u128_le(&acc.data,ORCA_WHIRLPOOL_SQRT_OFFSET)?})
                },
                Venue::RaydiumCpmm=>{
                    ensure!(acc.owner==RAYDIUM_CPMM_PROGRAM && acc.data.starts_with(&RAYDIUM_CPMM_DISC),
                        "{} not a Raydium CPMM PoolState",watch.address);
                    ensure!(acc.data.len()==RAYDIUM_CPMM_ACCOUNT_LEN,
                        "{} Raydium CPMM account length {} != {}",watch.address,acc.data.len(),RAYDIUM_CPMM_ACCOUNT_LEN);
                    let a=key(&acc.data,168)?;let b=key(&acc.data,200)?;
                    let base_vault=key(&acc.data,72)?;let quote_vault=key(&acc.data,104)?;
                    details.push(base_vault.clone());details.push(quote_vault.clone());
                    let (fee0,fee1)=cpmm_fee_balances(&acc.data,RAYDIUM_CPMM_FEES_OFFSET)?;
                    (a,b,State::RaydiumCpmm{base:0,quote:0,fee0,fee1,base_vault,quote_vault,
                        pool_slot,base_slot:pool_slot,quote_slot:pool_slot})
                },
                Venue::Manifest=>{
                    ensure!(acc.owner==MANIFEST_PROGRAM,"{} owner is not Manifest",watch.address);
                    ensure!(u64_le(&acc.data,0)?==MANIFEST_MARKET_DISC,"{} not a Manifest MarketFixed",watch.address);
                    let a=key(&acc.data,16)?;let b=key(&acc.data,48)?;
                    let (bid,ask,available)=match manifest_best_prices(&acc.data,pool_slot) {
                        Ok((bid,ask))=>(bid,ask,true),
                        Err(_)=>(0.0,0.0,false),
                    };
                    (a,b,State::Manifest{bid,ask,available})
                },
                Venue::RaydiumAmmV4=>{
                    ensure!(acc.owner==RAYDIUM_AMM_V4_PROGRAM,"{} owner is not Raydium AMM v4",watch.address);
                    ensure!(acc.data.len()==RAYDIUM_AMM_V4_ACCOUNT_LEN,
                        "{} Raydium AMM v4 account length {} != {}",watch.address,acc.data.len(),RAYDIUM_AMM_V4_ACCOUNT_LEN);
                    let a=key(&acc.data,400)?;let b=key(&acc.data,432)?;
                    let base_vault=key(&acc.data,336)?;let quote_vault=key(&acc.data,368)?;
                    details.push(base_vault.clone());details.push(quote_vault.clone());
                    (a,b,State::RaydiumAmmV4{base:0,quote:0,
                        base_pnl:u64_le(&acc.data,192)?,quote_pnl:u64_le(&acc.data,200)?,
                        base_vault,quote_vault,pool_slot,base_slot:pool_slot,quote_slot:pool_slot})
                }
            };
            ensure!(a!=b,"Pool with identical mints {}",watch.address);
            raw_pools.push((a,b,state,acc.slot));
        }
        // Mint metadata comes exclusively from the authoritative Mint Registry.
        let mint_addresses:HashSet<String>=raw_pools.iter().flat_map(|(a,b,_,_)|
            [a.clone(),b.clone()]).collect();
        let mut verified=HashMap::new();
        for address in mint_addresses {
            let key:Pubkey=address.parse()?;
            let state=mints.mint(key).and_then(|state|state.onchain)
                .with_context(||format!("Mint onchain proof missing {address}"))?;
            ensure!(state.address==key,"Mint proof identity mismatch");
            verified.insert(address,state.decimals);
        }
        let mut unique=Vec::new();let mut seen=HashSet::new();
        for address in details {
            if seen.insert(address.clone()) {
                unique.push(address);
            }
        }
        let records=fetch_batch(rpc,&unique,reads).await?;
        let dict:HashMap<String,RpcAccount>=unique.into_iter().zip(records).collect();
        let mut pools=Vec::new();let mut subs=Vec::new();
        for (idx,(watch,(a,b,mut state,pool_slot))) in cfg.watches.iter().zip(raw_pools).enumerate(){
            let da=*verified.get(&a).context("token A mint decimals missing")?;
            let db=*verified.get(&b).context("token B mint decimals missing")?;
            if matches!(&state,State::Manifest{..}) {
                let acc=&pool_records[idx].data;
                ensure!(bytes(acc,9,1)?[0]==da && bytes(acc,10,1)?[0]==db,
                    "Manifest market mint decimals do not match SPL mints");
            }
            // Startup-only CLMM layout sanity check, zero cost per live event.
            if matches!(watch.venue,Venue::RaydiumClmm|Venue::PancakeSwapClmm|Venue::ByrealClmm) {
                let acc=&pool_records[idx].data;
                ensure!(bytes(acc,233,1)?[0]==da && bytes(acc,234,1)?[0]==db,
                    "{} CLMM PoolState decimals disagree with SPL mints",watch.address);
            }
            if let State::RaydiumAmmV4{base,quote,base_vault,quote_vault,base_slot,quote_slot,..}=&mut state {
                *base=vault_amount(dict.get(base_vault).context("Raydium base vault missing")?,&a)?;
                *quote=vault_amount(dict.get(quote_vault).context("Raydium quote vault missing")?,&b)?;
                *base_slot=dict.get(base_vault).context("base vault slot missing")?.slot;
                *quote_slot=dict.get(quote_vault).context("quote vault slot missing")?.slot;
            }
            if let State::RaydiumCpmm{base,quote,base_vault,quote_vault,base_slot,quote_slot,..}=&mut state {
                *base=vault_amount(dict.get(base_vault).context("CPMM base vault missing")?,&a)?;
                *quote=vault_amount(dict.get(quote_vault).context("CPMM quote vault missing")?,&b)?;
                *base_slot=dict.get(base_vault).context("base vault slot missing")?.slot;
                *quote_slot=dict.get(quote_vault).context("quote vault slot missing")?.slot;
            }
            let (canon_base,canon_quote)=canonical(&a,&b);
            // Single-account prices belong to the pool account slot, not to the
            // unrelated later mint-decimal hydration slot. Composite reserve
            // baselines can contain account values from both fetches.
            let observed_slot=match &state {
                State::RaydiumAmmV4{base_slot,quote_slot,..}|
                State::RaydiumCpmm{base_slot,quote_slot,..}=>pool_slot.max(*base_slot).max(*quote_slot),
                _=>pool_slot,
            };
            let mint_states=[mints.mint(a.parse()?).and_then(|m|m.onchain),mints.mint(b.parse()?).and_then(|m|m.onchain)];
            let pool=Pool{key:watch.address.parse()?,ready:false,id:watch.address.clone(),venue:watch.venue,a,b,da,db,
                canon_base:canon_base.clone(),canon_quote:canon_quote.clone(),state,
                clock:0,epoch:0,mint_states,last_valuation:None,manifest_digest:watch.manifest_digest,
                price:0.0,available:false,slot:observed_slot,received_us:pool_records[idx].received_us,updates:0,prepared_at:Instant::now(),
            };
            let pool_field=match watch.venue{Venue::Dlmm=>Field::Dlmm,Venue::Pump=>Field::VirtualQuote,Venue::DammV2=>Field::DammSqrt,Venue::RaydiumAmmV4=>Field::RaydiumV4Pnl,Venue::RaydiumClmm=>Field::RaydiumClmmSqrt,Venue::RaydiumCpmm=>Field::RaydiumCpmmFees,Venue::OrcaWhirlpool=>Field::OrcaWhirlpoolSqrt,Venue::PancakeSwapClmm=>Field::PancakeSwapClmmSqrt,Venue::Manifest=>Field::ManifestBook,Venue::ByrealClmm=>Field::ByrealClmmSqrt};
            subs.push(Subscription{pool:idx,field:pool_field,address:watch.address.clone(),key:watch.address.parse()?,owner:pool_records[idx].owner.parse()?,slot:pool_slot,received_us:pool_records[idx].received_us,floor:Some(pool_slot),valid:true});
            if let State::RaydiumAmmV4{base_vault,quote_vault,..}=&pool.state {
                for (field,address) in [(Field::RaydiumV4BaseVault,base_vault),
                    (Field::RaydiumV4QuoteVault,quote_vault)] {
                    let acc=dict.get(address).context("Raydium vault not found")?;
                    subs.push(Subscription{pool:idx,field,address:address.clone(),key:address.parse()?,owner:acc.owner.parse()?,slot:acc.slot,received_us:acc.received_us,floor:Some(acc.slot),valid:true});
                }
            }
            if let State::RaydiumCpmm{base_vault,quote_vault,..}=&pool.state {
                for (field,address) in [(Field::RaydiumCpmmBaseVault,base_vault),
                    (Field::RaydiumCpmmQuoteVault,quote_vault)] {
                    let acc=dict.get(address).context("Raydium CPMM vault missing")?;
                    subs.push(Subscription{pool:idx,field,address:address.clone(),key:address.parse()?,owner:acc.owner.parse()?,slot:acc.slot,received_us:acc.received_us,floor:Some(acc.slot),valid:true});
                }
            }
            pools.push(pool);
        }

        let mut engine=Self::empty(events,sequence);
        engine.pools=pools;engine.subs=subs;engine.index();
        Ok(engine)
    }
    fn next_sequence(&mut self)->u64 {
        self.sequence=self.sequence.saturating_add(1);
        self.sequence
    }
    fn publish_price(&mut self,pi:usize,origin:PriceOrigin,previous:Option<f64>,field:Option<Field>,mut pipeline:Option<PricePipelineTiming>) {
        if self.pools[pi].venue==Venue::Pump {
            let started=Instant::now();self.pools[pi].refresh_liquidity();
            self.metrics.reserve_curve_build.add(nanos(started));
        }
        let sequence=self.next_sequence();
        let fees_started=Instant::now();
        let valuation=self.pools[pi].valuation();
        if valuation.is_some(){self.metrics.fee_valuation.calculation.add(nanos(fees_started));}
        let unchanged_net_rates=valuation.as_ref().is_some_and(|next|
            self.pools[pi].last_valuation.as_ref().is_some_and(|previous|
                previous.estimated_net_rates==next.estimated_net_rates));
        self.pools[pi].last_valuation=valuation.clone();
        let pool=&self.pools[pi];
        let (best_bid,best_ask)=pool.manifest_quotes().map_or((None,None),|(bid,ask)|(Some(bid),Some(ask)));
        let mut extrema=None::<(u64,u64,u64,u64)>;
        for &i in &self.pool_subs[pi] {
            let s=&self.subs[i];let (slot,at)=(s.slot,s.received_us);
            extrema=Some(match extrema {Some((a,b,c,d))=>(a.min(slot),b.max(slot),c.min(at),d.max(at)),
                None=>(slot,slot,at,at)});
        }
        let (component_slot_min,component_slot_max,component_received_at_us_min,component_received_at_us_max)=
            extrema.unwrap_or((pool.slot,pool.slot,pool.received_us,pool.received_us));
        let event=PoolPriceObservation {
            origin,pool:pool.id.clone(),venue:pool.venue.label(),
            mint_a:pool.a.clone(),mint_b:pool.b.clone(),
            base_mint:pool.canon_base.clone(),quote_mint:pool.canon_quote.clone(),
            price_quote_per_base:pool.price,valuation,liquidity:pool.liquidity(),
            previous_price:previous,
            delta_bps:previous.map(|old|bps(old,pool.price)).filter(|v|v.is_finite()),
            price_kind:pool.price_kind(),
            best_bid_quote_per_base:best_bid,best_ask_quote_per_base:best_ask,
            consistency:if pool.venue==Venue::Dlmm {
                if component_slot_min==component_slot_max{Consistency::SameSlotNonAtomic}else{Consistency::MixedSlotNonAtomic}
            }else{pool.consistency()},slot:pool.slot,received_at_us:component_received_at_us_max,
            sequence,component_slot_min,component_slot_max,component_received_at_us_min,component_received_at_us_max,trigger_field:field.map(Field::label),
            pipeline:None,
        };
        let mut event=event;
        if let Some(t)=pipeline.as_mut(){t.price_published_at_us=timestamp_us();}
        event.pipeline=pipeline;
        self.metrics.reserve_curve_observations+=u64::from(event.liquidity.as_ref().is_some_and(|p|p.reserve_curve.is_some()));
        if let Some(profile)=event.liquidity.as_ref().filter(|_|pool.venue==Venue::Dlmm) {
            let evidence=&mut self.metrics.liquidity;
            evidence.profile_observations+=1;
            for direction in &profile.directions {
                *evidence.directions_by_status.entry(direction.status).or_default()+=1;
                *evidence.directions_by_coverage.entry(direction.coverage).or_default()+=1;
                evidence.cross_array_directions+=u64::from(direction.levels.first().zip(direction.levels.last())
                    .is_some_and(|(a,b)|a.source_position.div_euclid(70)!=b.source_position.div_euclid(70)));
                evidence.negative_bin_directions+=u64::from(direction.levels.iter().any(|l|l.source_position<0));
                if let State::Dlmm{active,..}=&pool.state {
                    evidence.first_level_differs_from_active+=u64::from(direction.first_source_position.is_some_and(|id|id!=*active));
                }
            }
            if evidence.samples.len()<8 {
                evidence.samples.push(serde_json::json!({"observation":event}));
            }
            if evidence.focus.contains(&pool.id) {
                if let State::Dlmm{book,..}=&pool.state {
                    let sample=serde_json::json!({"observation":event,"accounts":book.evidence(),
                        "components":self.pool_subs[pi].iter().map(|&i|{let s=&self.subs[i];serde_json::json!({
                            "address":s.address,"field":s.field.label(),"slot":s.slot,"received_at_us":s.received_us,"valid":s.valid})}).collect::<Vec<_>>()});
                    match evidence.focused.get_mut(&pool.id) {
                        Some(entry)=>{entry.observations+=1;entry.last=sample;},
                        None=>{evidence.focused.insert(pool.id.clone(),FocusedLiquidity{observations:1,first:sample.clone(),last:sample});},
                    }
                }
            }
        }
        if let Some(v)=&event.valuation {
            let evidence=&mut self.metrics.fee_valuation;
            *evidence.observations_by_model.entry(v.model).or_default()+=1;
            if v.dex_fee_rates.iter().all(Option::is_some){*evidence.known_dex_fees_by_model.entry(v.model).or_default()+=1;}
            if let Some(reason)=&v.unknown_reason{*evidence.unknown_by_reason.entry(reason.clone()).or_default()+=1;}
            if let State::Dlmm{active,step,fee,..}=&pool.state {
                let lag=fee.reference_lag(pool.clock);
                if lag>0 {
                    evidence.clock_reference_lag_observations+=1;
                    evidence.clock_reference_lag_max_seconds=evidence.clock_reference_lag_max_seconds.max(lag);
                    if evidence.clock_reference_lag_samples.len()<4 {
                        evidence.clock_reference_lag_samples.push(serde_json::json!({
                            "pool":pool.id,"origin":origin,"trigger_field":event.trigger_field,
                            "observed_at_us":timestamp_us(),"chain_timestamp":pool.clock,"lag_seconds":lag,
                            "active_id":active,"bin_step":step,"fee_parameters":fee,
                            "estimated_net_rates":v.estimated_net_rates,"unknown_reason":v.unknown_reason,
                            "components":self.pool_subs[pi].iter().map(|&i|{let s=&self.subs[i];serde_json::json!({
                                "address":s.address,"field":s.field.label(),"slot":s.slot,
                                "received_at_us":s.received_us,"baseline_floor":s.floor,"valid":s.valid})}).collect::<Vec<_>>()
                        }));
                    }
                }
            }
            if let Some(reason)=v.unknown_reason.as_deref().filter(|r|*r=="DLMM reduction factor invalid") {
                if evidence.invalid_reference_samples.iter().filter(|s|s["reason"].as_str()==Some(reason)).count()<4 {
                    if let State::Dlmm{active,step,fee,..}=&pool.state {
                        evidence.invalid_reference_samples.push(serde_json::json!({
                            "reason":reason,"pool":pool.id,"origin":origin,"trigger_field":event.trigger_field,
                            "observed_at_us":timestamp_us(),"chain_timestamp":pool.clock,
                            "active_id":active,"bin_step":step,"fee_parameters":fee,
                            "components":self.pool_subs[pi].iter().map(|&i|{let s=&self.subs[i];serde_json::json!({
                                "address":s.address,"field":s.field.label(),"slot":s.slot,
                                "received_at_us":s.received_us,"baseline_floor":s.floor,"valid":s.valid})}).collect::<Vec<_>>()
                        }));
                    }
                }
            }
            *evidence.updates_by_field.entry(event.trigger_field.unwrap_or("baseline_or_mint")).or_default()+=1;
            evidence.revaluations+=u64::from(matches!(origin,PriceOrigin::Revaluation));
            evidence.unchanged_net_rate_updates+=u64::from(unchanged_net_rates);
            evidence.asymmetric_rates+=u64::from(v.estimated_net_rates.iter().all(Option::is_some)
                && (v.estimated_net_rates[0].unwrap()*v.estimated_net_rates[1].unwrap()-1.0).abs()>1e-12);
            if evidence.samples.iter().filter(|p|p.observation.venue==event.venue).count()<8 {
                let p=&self.pools[pi];
                let inputs=match &p.state {
                    State::Dlmm{active,step,fee,..}=>serde_json::json!({"native_mint_a":p.a,"native_mint_b":p.b,
                        "decimals":[p.da,p.db],"active_id":active,"bin_step":step,"fee_parameters":fee}),
                    State::Pump{base,quote,vquote,fee_pool,global,config,supply,..}=>serde_json::json!({
                        "native_mint_a":p.a,"native_mint_b":p.b,"decimals":[p.da,p.db],"base_raw":base,
                        "quote_vault_raw":quote,"virtual_quote_raw":vquote.to_string(),"base_supply_raw":supply,
                        "pool_fee_fields":fee_pool,"global":global,"fee_config":config}),
                    _=>serde_json::Value::Null,
                };
                evidence.samples.push(FeeSample{observation:event.clone(),inputs});
            }
        }
        self.emit(MarketStateEvent::Price(event));
        if matches!(origin,PriceOrigin::Websocket){
            self.metrics.websocket_prices+=1;
            if self.metrics.rewatching {self.metrics.post_rewatch_websocket_observations+=1;}
        }else if matches!(origin,PriceOrigin::Baseline){self.metrics.baseline_prices+=1;}
    }
    fn publish_unavailable(&mut self,pi:usize,slot:u64,recv_us:u64,reason:String) {
        let sequence=self.next_sequence();
        let p=&self.pools[pi];
        self.emit(MarketStateEvent::Unavailable{
            pool:p.id.clone(),venue:p.venue.label(),slot,
            received_at_us:recv_us,sequence,reason,
        });
    }
    fn update(&mut self,index:usize,slot:u64,owner:Pubkey,data:&[u8],recv_us:u64,pipeline:PricePipelineTiming,emit:bool)->Result<()> {
        let decode_started=Instant::now();
        let mut pipeline=pipeline;
        let (pi,field,expected_owner,expected_len)={let s=&self.subs[index];
            (s.pool,s.field,s.owner,s.field.slice().1)};
        ensure!(owner==expected_owner,"{} owner mismatch",self.subs[index].address);
        ensure!(field.full() || data.len()==expected_len ||
            (field==Field::VirtualQuote && data.is_empty()),
            "slice length mismatch: {} expected {} got {}",field.label(),expected_len,data.len());
        let prior_clock=self.pools[pi].clock;let prior_epoch=self.pools[pi].epoch;
        let newest=self.pool_subs[pi].iter().map(|&i|self.subs[i].slot).max().unwrap_or(slot);
        let shared=if matches!(field,Field::PumpGlobal|Field::PumpFees) {
            let address=self.subs[index].key;
            let cached=self.shared_fees.get(&address).filter(|(s,t,_)|*s==slot&&*t==recv_us).map(|(_,_,v)|v.clone());
            Some(if let Some(value)=cached{value}else{
                let value=if field==Field::PumpGlobal{fees::Shared::Global(Arc::new(fees::Global::decode(data)?))}
                    else{fees::Shared::Config(Arc::new(fees::Config::decode(data)?))};
                self.shared_fees.insert(address,(slot,recv_us,value.clone()));value
            })
        }else{None};
        let pool=&mut self.pools[pi];
        if let State::Pending{dlmm,vaults}=&pool.state {
            pool.state=match field {
                Field::Dlmm=>State::Dlmm{active:i32_le(data,76)?,step:u16_le(data,80)?,
                    fee:fees::Dlmm::decode(data)?,book:Box::new(dlmm::Book::new(pool.key,
                        dlmm.context("prepared DLMM dependencies missing")?,data)?)},
                Field::VirtualQuote=>{
                    let v=vaults.context("prepared PumpSwap vaults missing")?;
                    State::Pump{vquote:0,base:0,quote:0,vaults:v,
                        pool_slot:slot,base_slot:0,quote_slot:0,fee_pool:fees::PumpPool::decode(data)?,
                        global:None,config:None,supply:None,liquidity:None}
                },
                _=>bail!("baseline must initialize the pool account before its components"),
            };
        }
        let old=pool.price;
        let was_available=pool.available;
        let mut unavailable_reason=None;
        match (&mut pool.state,field) {
            (State::Manifest{bid,ask,available},Field::ManifestBook)=>{
                match manifest_best_prices(data,slot) {
                    Ok((next_bid,next_ask))=>{*bid=next_bid;*ask=next_ask;*available=true;},
                    Err(error)=>{*available=false;unavailable_reason=Some(error.to_string());},
                }
            },
            (State::Dlmm{active,step,fee,book},Field::Dlmm)=>{
                ensure!(key(data,88)?==pool.a && key(data,120)?==pool.b,"DLMM mint identity changed");
                *fee=fees::Dlmm::decode(data)?;
                let next_active=i32_le(data,76)?;
                let next_step=u16_le(data,80)?;
                ensure!(next_step>0,"DLMM step zero");
                *active=next_active;*step=next_step;
                book.pair(data)?;
            },
            (State::Dlmm{book,..},Field::DlmmArray(position))=>{
                book.array(position,self.subs[index].key,data)?;
            },
            (State::Dlmm{book,..},Field::DlmmBitmap)=>{book.bitmap(data)?;},
            (State::Pump{vquote,pool_slot,fee_pool,vaults,..},Field::VirtualQuote)=>{
                ensure!(key(data,43)?==pool.a&&key(data,75)?==pool.b,"Pump mint identity changed");
                ensure!(bytes(data,139,32)?==vaults.vault_a.as_ref() && bytes(data,171,32)?==vaults.vault_b.as_ref(),"Pump vault identity changed; awaiting Completion");
                *fee_pool=fees::PumpPool::decode(data)?;
                *vquote=if data.len()<261{0}else{i128_le(data,245)?};*pool_slot=slot;
            },
            (_,Field::Clock)=>{ensure!(data.len()==40,"invalid chain Clock length");pool.clock=i64::from_le_bytes(bytes(data,32,8)?.try_into()?);pool.epoch=u64_le(data,16)?;},
            (State::Pump{global,..},Field::PumpGlobal)=>{if let Some(fees::Shared::Global(value))=shared{*global=Some(value);}},
            (State::Pump{config,..},Field::PumpFees)=>{if let Some(fees::Shared::Config(value))=shared{*config=Some(value);}},
            (State::Pump{supply,..},Field::PumpSupply)=>{ensure!(bytes(data,45,1)?[0]==1,"Pump mint uninitialized");*supply=Some(u64_le(data,36)?);},
            (State::Pump{base,base_slot,..},Field::BaseVault)=>{
                *base=u64_le(data,0)?;*base_slot=slot;
            },
            (State::Pump{quote,quote_slot,..},Field::QuoteVault)=>{
                *quote=u64_le(data,0)?;*quote_slot=slot;
            },
            (State::DammV2{sqrt},Field::DammSqrt)|
            (State::RaydiumClmm{sqrt},Field::RaydiumClmmSqrt)|
            (State::OrcaWhirlpool{sqrt},Field::OrcaWhirlpoolSqrt)|
            (State::PancakeSwapClmm{sqrt},Field::PancakeSwapClmmSqrt)|
            (State::ByrealClmm{sqrt},Field::ByrealClmmSqrt)=>{
                *sqrt=u128_le(data,0)?;
            },
            (State::RaydiumAmmV4{base_pnl,quote_pnl,pool_slot,..},Field::RaydiumV4Pnl)=>{
                *base_pnl=u64_le(data,0)?;*quote_pnl=u64_le(data,8)?;*pool_slot=slot;
            },
            (State::RaydiumAmmV4{base,base_slot,..},Field::RaydiumV4BaseVault)=>{
                *base=u64_le(data,0)?;*base_slot=slot;
            },
            (State::RaydiumAmmV4{quote,quote_slot,..},Field::RaydiumV4QuoteVault)=>{
                *quote=u64_le(data,0)?;*quote_slot=slot;
            },
            (State::RaydiumCpmm{fee0,fee1,pool_slot,..},Field::RaydiumCpmmFees)=>{
                let (next0,next1)=cpmm_fee_balances(data,0)?;
                *fee0=next0;*fee1=next1;*pool_slot=slot;
            },
            (State::RaydiumCpmm{base,base_slot,..},Field::RaydiumCpmmBaseVault)=>{
                *base=u64_le(data,0)?;*base_slot=slot;
            },
            (State::RaydiumCpmm{quote,quote_slot,..},Field::RaydiumCpmmQuoteVault)=>{
                *quote=u64_le(data,0)?;*quote_slot=slot;
            },
            _=>bail!("wrong field {:?} for pool {}",field,pool.id),
        }
        pipeline.market_decode_ns=nanos(decode_started);
        self.subs[index].valid=true;
        pool.slot=newest;
        pool.received_us=recv_us;pool.updates+=1;
        if self.pool_subs[pi].iter().any(|&i|!self.subs[i].valid) {pool.available=false;return Ok(());}
        if let Some(reason)=unavailable_reason {
            pool.available=false;
            if emit {self.publish_unavailable(pi,slot,recv_us,reason);}
            return Ok(());
        }
        if field==Field::Clock && prior_epoch==pool.epoch && pool.last_valuation.is_some() {
            let due=match &pool.state{State::Dlmm{fee,..}=>pool.clock>=fee.next_change(prior_clock),_=>false};
            if !due{return Ok(());}
        }
        let liquidity_started=Instant::now();
        let liquidity_changed=if pool.venue==Venue::Dlmm {
            let changed=pool.refresh_liquidity();
            self.metrics.liquidity.profile_build.add(nanos(liquidity_started));
            self.metrics.liquidity.profile_changes+=u64::from(changed);
            if emit && matches!(field,Field::DlmmArray(_)){self.metrics.liquidity.array_websocket_updates+=1;}
            if emit && field==Field::DlmmBitmap{self.metrics.liquidity.bitmap_websocket_updates+=1;}
            changed
        }else{false};
        let compute_started=Instant::now();
        let price=match pool.compute_price() {
            Ok(price)=>price,
            Err(error)=>{
                    pool.available=false;
                if emit {self.publish_unavailable(pi,slot,recv_us,error.to_string());}
                return Ok(());
            }
        };
        pipeline.market_compute_ns=nanos(compute_started);
        pool.price=price;
        pool.available=true;
        // After any unavailable state, the next valid observation has no
        // adjacent previous price. This applies to every venue, not just Manifest.
        let previous=if was_available{Some(old)}else{None};
        // Clock account writes are frequent; emit only when the actual fee estimate changes.
        let next=if field==Field::Clock{pool.valuation()}else{None};
        let clock_changed=liquidity_changed || field!=Field::Clock || prior_epoch!=pool.epoch || pool.last_valuation.as_ref().map(|v|(&v.estimated_net_rates,&v.unknown_reason))
            !=next.as_ref().map(|v|(&v.estimated_net_rates,&v.unknown_reason));
        if emit && clock_changed {self.publish_price(pi,if field==Field::Clock{PriceOrigin::Revaluation}else{PriceOrigin::Websocket},previous,Some(field),Some(pipeline));}
        Ok(())
    }

}


#[derive(Clone,Copy)]
pub struct Settings {pub pool_limit:usize,pub trace:bool}
impl Settings {
    pub fn from_env()->Result<Self> {
        let focus=env::var("MARKET_STATE_LIQUIDITY_EVIDENCE_POOLS").unwrap_or_default();
        let addresses=focus.split(',').map(str::trim).filter(|v|!v.is_empty()).collect::<Vec<_>>();
        ensure!(addresses.len()<=32,"at most 32 explicit liquidity evidence pools");
        for address in addresses {let _:Pubkey=address.parse().context("invalid liquidity evidence pool")?;}
        Ok(Self{
        pool_limit:env_parse("MARKET_STATE_POOL_LIMIT",15000)?,
        trace:env_parse::<u8>("MARKET_STATE_TRACE",0)?!=0,
    })}
}

// Only the price worker's identity index is retained; manifests and canonical pool
// lifecycle remain in the Registry. Notifications reconcile against the current owner.
#[derive(Clone,Copy,PartialEq,Eq)]
struct PoolInput {address:Pubkey,venue:CoreVenue,mint_a:Pubkey,mint_b:Pubkey,dlmm:Option<dlmm::Dependencies>,vaults:Option<PoolVaults>,manifest_digest:[u8;32]}
impl PoolInput {
    fn direct(self)->bool {matches!(self.venue,CoreVenue::MeteoraDlmm|CoreVenue::PumpSwap)}
    fn structure_available(self)->bool {
        match self.venue {CoreVenue::MeteoraDlmm=>self.dlmm.is_some(),CoreVenue::PumpSwap=>self.vaults.is_some(),_=>false}
    }
    fn of(p:&PoolState)->Option<Self> {
        if !p.completion_admission.is_admitted() || p.completion_state!=CompletionState::Prepared{return None;}
        let dlmm=if p.venue==CoreVenue::MeteoraDlmm {
            p.execution_manifest.as_ref().and_then(|m|dlmm::Dependencies::from_manifest(m).ok())
        }else{None};
        let manifest_digest=if matches!(p.venue,CoreVenue::MeteoraDlmm|CoreVenue::PumpSwap) {
            p.execution_manifest.as_ref()?.account_set_digest()
        }else{[0;32]};
        Some(Self{address:p.address,venue:p.venue,mint_a:p.mint_a?,mint_b:p.mint_b?,dlmm,vaults:p.vaults.as_deref().copied(),manifest_digest})
    }
}
fn reconcile(registry:&PoolRegistry,candidates:&mut HashMap<Pubkey,PoolInput>,address:Pubkey)->bool {
    let current=registry.get(address).as_ref().and_then(PoolInput::of);
    if candidates.get(&address).copied()==current {return false;}
    if let Some(p)=current {candidates.insert(address,p);}else{candidates.remove(&address);}
    true
}
fn watch_cost(p:&PoolInput)->usize {
    match p.venue {CoreVenue::MeteoraDlmm=>p.dlmm.map_or(8,|d|d.account_cost()),
        CoreVenue::PumpSwap=>4,CoreVenue::RaydiumAmmV4|CoreVenue::RaydiumCpmm=>3,_=>1}
}
fn route_kind(t:&super::graph_liquidity::RouteTopology)->usize{
    use super::graph_liquidity::RouteTopologyKind as K;
    match t.kind{K::Direct2=>0,K::Direct3=>1,K::Triangle3=>2,K::Triangle4=>3}
}
fn route_options_added(t:&super::graph_liquidity::RouteTopology,
    graph:&super::graph_liquidity::GraphSnapshot,
    selected:&HashSet<Pubkey>,additional:&[Pubkey])->u64{
    let included=|p:&Pubkey|selected.contains(p)||additional.contains(p);
    let mut counts=[0_u64;4];
    for (i,&(a,b)) in t.directed_roles().iter().enumerate(){
        counts[i]=graph.pools_for_pair(a,b).iter().filter(|p|included(p)).count() as u64;
    }
    if route_kind(t)==0{return counts[0].saturating_mul(counts[0].saturating_sub(1));}
    (0..t.length).fold(2_u64,|n,i|n.saturating_mul(counts[i]))
}
fn route_options_selected(t:&super::graph_liquidity::RouteTopology,
    graph:&super::graph_liquidity::GraphSnapshot,selected:&HashSet<Pubkey>)->u64{
    route_options_added(t,graph,selected,&[])
}
fn route_coverage(catalog:&WatchCatalog,selected:&HashSet<Pubkey>)->([usize;4],[u64;4]){
    let mut groups=[0_usize;4];let mut options=[0_u64;4];
    for t in catalog.graph.topologies.iter(){
        let routes=route_options_selected(t,&catalog.graph,selected);
        if routes>0{
            let kind=route_kind(t);groups[kind]+=1;
            options[kind]=options[kind].saturating_add(routes);
        }
    }
    (groups,options)
}
struct RoutePackage {kind:usize,additional:Vec<Pubkey>,account_cost:usize,score:f64}
/// Read-only topological pool selection. Executed on a blocking worker thread,
/// never on the market WS event loop. No independent pool or mint registry.
fn select(candidates:&HashMap<Pubkey,PoolInput>,catalog:&WatchCatalog,
    limit:usize,account_budget:usize,previous:&[PoolInput],registry:&PoolRegistry)->Vec<PoolInput>{
    let started=Instant::now();
    let max_pools=if limit==0 {usize::MAX}else{limit};
    // Both paths use the same generation's graph-relevant identities. Registry
    // notifications and catalog publication may advance independently.
    let eligible=catalog.pools.iter().filter_map(|pool|{
        candidates.get(&pool.address).copied().filter(|p|
            (p.mint_a.min(p.mint_b),p.mint_a.max(p.mint_b))==(pool.mint_a,pool.mint_b))
    }).map(|p|(p.address,p)).collect::<HashMap<_,_>>();
    let shared_cost=if eligible.values().any(|p|p.venue==CoreVenue::PumpSwap){3}
        else if eligible.values().any(|p|p.venue==CoreVenue::MeteoraDlmm){1}else{0};
    let pool_budget=account_budget.saturating_sub(shared_cost);
    let required_accounts=eligible.values().map(watch_cost).sum::<usize>()+shared_cost;
    let (picked,used,backbone,mode)=if eligible.len()<=max_pools && required_accounts<=account_budget {
        // All admissible catalog pools fit. Ranking cannot improve this set.
        (eligible.keys().copied().collect::<HashSet<_>>(),required_accounts,0,"all_fit")
    }else{
        let (picked,used,backbone)=select_budgeted(&eligible,catalog,limit,pool_budget,previous,registry);
        (picked,used+shared_cost,backbone,"budgeted")
    };
    // Keep actual route coverage; there is no historical comparison selection.
    let actual=route_coverage(catalog,&picked);
    let mut result=picked.into_iter().map(|p|eligible[&p]).collect::<Vec<_>>();
    result.sort_unstable_by_key(|p|p.address);
    crate::log_event!(Info,
        "market state selection mode={} generation={} catalog_pools={} eligible_pools={} selected={} pool_limit={} account_upper_bound={} required_accounts={} ws_budget={} groups_by_kind={:?} alternatives_by_kind={:?} conversion_backbone={} selection_us={}",
        mode,catalog.generation,catalog.pools.len(),eligible.len(),result.len(),limit,
        used,required_accounts,account_budget,actual.0,actual.1,backbone,started.elapsed().as_micros());
    result
}

// The existing route-package heuristic is needed only when a budget binds.
fn select_budgeted(candidates:&HashMap<Pubkey,PoolInput>,catalog:&WatchCatalog,
    limit:usize,account_budget:usize,previous:&[PoolInput],registry:&PoolRegistry)
    ->(HashSet<Pubkey>,usize,usize){
    let max_pools=if limit==0 {usize::MAX}else{limit};
    let existing:HashSet<Pubkey>=previous.iter().map(|p|p.address).collect();
    let quality:HashMap<Pubkey,f64>=candidates.keys().map(|address|(*address,
        registry.get(*address).and_then(|p|p.tvl_usd)
            .filter(|v|v.is_finite()&&*v>=0.0).unwrap_or(0.0))).collect();
    let mut pairs=HashMap::<(Pubkey,Pubkey),Vec<Pubkey>>::new();
    for p in candidates.values(){
        pairs.entry((p.mint_a.min(p.mint_b),p.mint_a.max(p.mint_b)))
            .or_default().push(p.address);
    }
    for list in pairs.values_mut(){
        list.sort_by(|a,b|existing.contains(b).cmp(&existing.contains(a))
            .then_with(||quality[b].total_cmp(&quality[a])).then_with(||a.cmp(b)));
    }
    let mut picked=HashSet::<Pubkey>::new();
    let mut used=0_usize;
    // A small diversified conversion backbone is shared by Direct3/Triangle4.
    // Its legs are candidates from the SAME WatchCatalog, not independently discovered.
    let wsol:Pubkey="So11111111111111111111111111111111111111112".parse().expect("WSOL");
    let usdc:Pubkey="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".parse().expect("USDC");
    let mut conversions=catalog.graph.pools_for_pair(wsol,usdc).iter().copied()
        .filter(|p|candidates.contains_key(p)).collect::<Vec<_>>();
    conversions.sort_unstable();conversions.dedup();
    conversions.sort_by(|a,b|quality[b].total_cmp(&quality[a]).then_with(||a.cmp(b)));
    let mut venues=Vec::<CoreVenue>::new();
    let mut backbone=Vec::new();
    for &address in &conversions {
        let p=candidates[&address];
        if venues.contains(&p.venue){continue;}
        let cost=watch_cost(&p);
        if picked.len()<max_pools && used+cost<=account_budget {
            picked.insert(address);used+=cost;backbone.push(address);venues.push(p.venue);
        }
        if backbone.len()>=8 {break;}
    }
    // Do not pin eight accounts from one venue merely because they have high TVL.
    // Each (settlement,target) gets independent queues for all four route types.
    let mut groups=BTreeMap::<(Pubkey,Pubkey),[Vec<usize>;4]>::new();
    for (id,t) in catalog.graph.topologies.iter().enumerate(){
        groups.entry((t.settlement,t.target)).or_insert_with(||std::array::from_fn(|_|Vec::new()))[route_kind(t)].push(id);
    }
    for lists in groups.values_mut(){
        for ids in lists {
            ids.sort_by_key(|&id|(std::cmp::Reverse(catalog.graph.topologies[id].route_options),id));
        }
    }
    let mut cursors=BTreeMap::<(Pubkey,Pubkey),[usize;4]>::new();
    let mut coverage=[0_usize;4];
    let mut selected_per_target=HashMap::<(Pubkey,Pubkey),usize>::new();
    // Candidate leg order: already covered, retained, then TVL, then stable pubkey.
    let choose=|options:&[Pubkey],count:usize,picked:&HashSet<Pubkey>,
        local:&mut Vec<Pubkey>|->bool {
        let Some(first)=options.first() else{return false;};
        let Some(w)=catalog.for_pool(*first) else{return false;};
        let Some(ordered)=pairs.get(&(w.mint_a,w.mint_b)) else{return false;};
        for desired in 0..3 {
            for &address in ordered {
                if options.binary_search(&address).is_err() || local.contains(&address){continue;}
                let tier=if picked.contains(&address){0}
                    else if existing.contains(&address){1}else{2};
                if tier==desired {
                    local.push(address);
                    if local.len()>=count{return true;}
                }
            }
        }
        false
    };
    // One useful package per target per sweep. Compare four route kinds on their
    // incremental route benefit, completeness, diversity and actual account cost.
    loop {
        let mut progressed=false;
        for (target,lists) in &groups {
            let cursors_for=cursors.entry(*target).or_insert([0;4]);
            let mut best:Option<RoutePackage>=None;
            for kind in [1_usize,3,0,2] {
                let ids=&lists[kind];
                while cursors_for[kind]<ids.len() {
                    let t=&catalog.graph.topologies[ids[cursors_for[kind]]];
                    let before=route_options_selected(t,&catalog.graph,&picked);
                    let mut local=Vec::<Pubkey>::new();
                    let mut valid=true;
                    // One pool per leg, except Direct2's two distinct alternatives
                    // on the same pair. Intermediate/Conversion remain fully variable.
                    for (i,&(a,b)) in t.directed_roles().iter().enumerate(){
                        let count=local.len()+1;
                        let options=catalog.graph.pools_for_pair(a,b);
                        valid=choose(options,count,&picked,&mut local);
                        if !valid {break;}
                        if kind==0 && i==0 {
                            valid=choose(options,local.len()+1,&picked,&mut local);
                            break;
                        }
                    }
                    if !valid {cursors_for[kind]+=1;continue;}
                    let additional:Vec<_>=local.iter().copied()
                        .filter(|p|!picked.contains(p)).collect();
                    let cost=additional.iter().map(|p|watch_cost(&candidates[p])).sum::<usize>();
                    if cost==0 || picked.len()+additional.len()>max_pools
                        ||used+cost>account_budget {
                        cursors_for[kind]+=1;continue;
                    }
                    let new_routes=route_options_added(t,&catalog.graph,&picked,&additional);
                    let gain=new_routes.saturating_sub(before);
                    if gain==0 {cursors_for[kind]+=1;continue;}
                    let underrepresented=1.0/(1.0+coverage[kind] as f64/100.0);
                    let target_damping=1.0/(1.0+*selected_per_target.get(target).unwrap_or(&0) as f64/12.0);
                    let avg_quality=additional.iter().map(|p|quality[p].ln_1p())
                        .sum::<f64>()/(additional.len() as f64);
                    // Saturating log prevents large parallel-pool arrays from
                    // overwhelming topology breadth. Exact weights are a testable heuristic.
                    let score=(10.0*underrepresented+(gain as f64).ln_1p()
                        +0.12*avg_quality)*target_damping/(cost as f64);
                    if best.as_ref().is_none_or(|b|score>b.score) {
                        best=Some(RoutePackage{kind,additional,account_cost:cost,score});
                    }
                    break;
                }
            }
            let Some(best)=best else{continue;};
            cursors_for[best.kind]+=1;
            for address in best.additional {picked.insert(address);}
            used+=best.account_cost;coverage[best.kind]+=1;
            *selected_per_target.entry(*target).or_default()+=1;
            progressed=true;
            if picked.len()>=max_pools || used>=account_budget {break;}
        }
        if !progressed || picked.len()>=max_pools || used>=account_budget {break;}
    }
    // Remaining capacity: diversify parallel venues/pairs without building full
    // copies of Pools. Prefer already monitored pools for subscription stability.
    let mut occupied=HashSet::new();
    for address in &picked {
        let p=candidates[address];
        occupied.insert((p.mint_a.min(p.mint_b),p.mint_a.max(p.mint_b),
            std::mem::discriminant(&p.venue)));
    }
    let mut remaining=candidates.values().copied().filter(|p|
        !picked.contains(&p.address) && catalog.for_pool(p.address).is_some()).collect::<Vec<_>>();
    remaining.sort_by(|a,b|{
        let rank=|p:&PoolInput|{
            let c=catalog.for_pool(p.address).expect("catalog indexed");
            (usize::from(!occupied.contains(&(p.mint_a.min(p.mint_b),p.mint_a.max(p.mint_b),
                std::mem::discriminant(&p.venue)))),
                c.contexts.len(),c.pair_pool_count.min(16))
        };
        rank(b).cmp(&rank(a)).then_with(||existing.contains(&b.address).cmp(&existing.contains(&a.address)))
            .then_with(||quality[&b.address].total_cmp(&quality[&a.address]))
            .then_with(||a.address.cmp(&b.address))
    });
    for p in remaining {
        if picked.len()>=max_pools {break;}
        let cost=watch_cost(&p);
        if used+cost<=account_budget {picked.insert(p.address);used+=cost;}
    }
    (picked,used,backbone.len())
}

impl Engine {
    fn emit(&mut self,event:MarketStateEvent) {
        if matches!(&event,MarketStateEvent::Removed{..}){self.metrics.removals+=1;}
        if self.trace {crate::log_event!(Info,"market state event={}",serde_json::to_string(&event).expect("typed market event"));}
        if self.events.send(Arc::new(event)).is_err(){self.metrics.no_receiver+=1;}
    }
}
// Preparation never publishes or borrows the running engine. Only the event loop
// installs results, after checking that identities and resync revisions still match.
async fn hydrate(pools:&[PoolInput],rpc:&RpcHandle,out:&broadcast::Sender<Arc<MarketStateEvent>>,
    mints:&dyn MintReader,baseline_source:Option<&Engine>,reads:&mut RpcReads)->Result<(Engine,Vec<(PoolInput,String)>)> {
    let mut engine=Engine::empty(out.clone(),0);
    let mut pending=vec![pools.to_vec()];let mut failed=Vec::new();
    while let Some(batch)=pending.pop() {
        if batch.is_empty(){continue;}
        let config=(||->Result<Config>{Ok(Config{watches:batch.iter().map(|p|
            Ok(Watch{address:p.address.to_string(),venue:Venue::from_core(p.venue)?,manifest_digest:p.manifest_digest}))
            .collect::<Result<Vec<_>>>()?})})();
        let result=match (baseline_source,config) {
            (Some(source),_)=>{
                let selected=batch.iter().map(|p|p.address).collect();
                let mut part=source.subset(&selected);
                part.refresh_baseline(rpc,reads).await.map(|()|part)
            },
            (None,Ok(cfg))=>Engine::hydrate(cfg,rpc,out.clone(),0,mints,reads).await,
            (None,Err(error))=>Err(error),
        };
        let result=result.and_then(|part| {
            ensure!(part.pools.len()==batch.len(),"hydration pool count mismatch");
            for actual in &part.pools {
                let expected=batch.iter().find(|p|p.address==actual.key)
                    .context("hydration returned an unexpected pool")?;
                ensure!(actual.a==expected.mint_a.to_string()
                    && actual.b==expected.mint_b.to_string(),"Registry/pool mint identity mismatch");
            }
            Ok(part)
        });
        match result {
            Ok(mut part)=>{
                let offset=engine.pools.len();
                for sub in &mut part.subs {sub.pool+=offset;}
                engine.pools.extend(part.pools);engine.subs.extend(part.subs);
            },
            Err(error) if batch.len()>1=>{
                let middle=batch.len()/2;pending.push(batch[middle..].to_vec());pending.push(batch[..middle].to_vec());
                crate::log_event!(Debug,"market state split failed batch pools={} error={error:#}",batch.len());
            },
            Err(error)=>failed.push((batch[0],format!("hydrate: {error:#}"))),
        }
    }
    engine.index();
    Ok((engine,failed))
}
fn watches(engine:&Engine,failed:&[Pubkey])->Result<Vec<AccountWatch>> {
    let mut wanted=BTreeMap::<Pubkey,AccountView>::new();
    for s in &engine.subs {
        let address=s.key;let (offset,length)=s.field.slice();
        let view=if s.field.full() {AccountView::Full}else{AccountView::Slice{offset,length}};
        let combined=match (wanted.get(&address).copied(),view) {
            (None,v)=>v,(Some(AccountView::Full),_)|(_,AccountView::Full)=>AccountView::Full,
            (Some(AccountView::Slice{offset:a,length:b}),AccountView::Slice{offset:c,length:d})=>{
                let start=a.min(c);AccountView::Slice{offset:start,length:(a+b).max(c+d)-start}
            }
        };
        wanted.insert(address,combined);
    }
    for &address in failed {wanted.insert(address,AccountView::Full);}
    Ok(wanted.into_iter().map(|(address,view)|AccountWatch{address,view}).collect())
}

fn complete(engine:&Engine,pi:usize,active:&HashMap<Pubkey,String>)->bool {
    engine.pool_subs[pi].iter().all(|&i|active.contains_key(&engine.subs[i].key))
}
// Detailed owner-only projection after the live loop. Progress uses existing Stats.
impl Engine {
    fn preparation_sample(&self,selected:&[PoolInput],mints:&dyn MintReader,
        active:&HashMap<Pubkey,String>,planned:&HashMap<Pubkey,String>,
        in_flight:&HashMap<Pubkey,time::Instant>,retry:&HashMap<Pubkey,time::Instant>)->PreparationSample {
        let started=Instant::now();
        let mut sample=PreparationSample{at_us:timestamp_us(),build_ns:0,selected:selected.len(),net_direction_pools:[0;3],
            stages:PreparationStatus::default(),baseline_in_flight:0,baseline_backoff:0,baseline_queued:0,
            oldest_baseline_in_flight_ms:0,providers:BTreeMap::new(),missing_bindings_by_field:BTreeMap::new(),
            blockers:Vec::new(),shared_accounts:Vec::new()};
        let now=time::Instant::now();
        for p in selected {
            if let Some(&pi)=self.pool_index.get(&p.address) {
                if self.pools[pi].ready&&self.pools[pi].available {
                    if let Some(value)=&self.pools[pi].last_valuation {
                        sample.net_direction_pools[value.estimated_net_rates.iter().flatten().count()]+=1;
                    }
                }
                if !complete(self,pi,active){sample.stages.waiting_subscriptions+=1;}
                else if !self.pools[pi].ready {
                    sample.stages.waiting_baseline+=1;
                    if let Some(at)=in_flight.get(&p.address) {
                        sample.baseline_in_flight+=1;
                        sample.oldest_baseline_in_flight_ms=sample.oldest_baseline_in_flight_ms.max(
                            now.duration_since(*at).as_millis().min(u128::from(u64::MAX)) as u64);
                    }else if retry.get(&p.address).is_some_and(|at|*at>now){sample.baseline_backoff+=1;}
                    else{sample.baseline_queued+=1;}
                }else if !self.pools[pi].available{sample.stages.unpriceable+=1;}
                else{sample.stages.priced+=1;}
            }else if [p.mint_a,p.mint_b].into_iter().any(|mint|mints.mint(mint).and_then(|m|m.onchain).is_none()) {
                sample.stages.waiting_mints+=1;
            }else{sample.stages.waiting_structure+=1;}
        }
        let mut accounts=Vec::with_capacity(self.account_ranges.len());
        let mut blocked=BTreeMap::<&str,HashSet<usize>>::new();
        for (&key,range) in &self.account_ranges {
            let provider=planned.get(&key).map(String::as_str).unwrap_or("assignment_not_observed");
            let row=sample.providers.entry(provider.to_owned()).or_default();row.component_accounts+=1;
            let acknowledged=active.contains_key(&key);
            if acknowledged{row.acknowledged+=1;}else {
                row.missing+=1;
                let pools=blocked.entry(provider).or_default();
                for position in range.clone() {
                    let sub=&self.subs[self.account_bindings[position]];pools.insert(sub.pool);
                    *sample.missing_bindings_by_field.entry(sub.field.label()).or_default()+=1;
                }
            }
            accounts.push((range.len(),key,acknowledged));
        }
        for (provider,pools) in blocked {sample.providers.get_mut(provider).unwrap().blocked_pools=pools.len();}
        accounts.sort_unstable_by(|a,b|b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        for (bindings,key,acknowledged) in accounts {
            let missing_sample=!acknowledged&&sample.blockers.len()<8;
            let shared_sample=bindings>1&&sample.shared_accounts.len()<8;
            if missing_sample||shared_sample {
                let range=&self.account_ranges[&key];let first=&self.subs[self.account_bindings[range.start]];
                let value=serde_json::json!({"address":key.to_string(),"field":first.field.label(),
                    "pool_bindings":bindings,"assigned_provider":planned.get(&key),
                    "acknowledged_provider":active.get(&key)});
                if missing_sample{sample.blockers.push(value.clone());}
                if shared_sample{sample.shared_accounts.push(value);}
            }
        }
        sample.build_ns=nanos(started);sample
    }
}
// The source session owns continuity. Invalidate only pools using affected accounts.
fn invalidate_accounts(engine:&mut Engine,keys:&HashSet<Pubkey>,versions:&mut HashMap<Pubkey,u64>,revision:&mut u64,reason:&str)->(usize,usize) {
    let pools:HashSet<_>=keys.iter().filter_map(|key|engine.account_ranges.get(key))
        .flat_map(|range|range.clone().map(|i|engine.subs[engine.account_bindings[i]].pool)).collect();
    let affected=pools.len();let ready=pools.iter().filter(|&&pi|engine.pools[pi].ready).count();
    for pi in pools {
        let p=&mut engine.pools[pi];p.ready=false;p.available=false;
        *revision+=1;versions.insert(p.key,*revision);
        engine.publish_unavailable(pi,engine.pools[pi].slot,timestamp_us(),reason.into());
    }
    (affected,ready)
}

impl Engine {
    // One address lookup -> contiguous subscription bindings. Never scan the pool catalog.
    // Returns whether an initializing pool still needs the latest account for catch-up.
    fn account(&mut self,event:&WsEvent,active:&HashMap<Pubkey,String>,market_us:u64,replay:bool,only:Option<&HashSet<Pubkey>>)->bool {
        let WsEvent::Account{address,provider,slot,frame_received_at_us,decoded_at_us,value,..}=event else{return false;};
        let started=Instant::now();
        if !replay {
            self.metrics.received+=1;
            if let Some(us)=market_us.checked_sub(*decoded_at_us){self.metrics.queue.add(us.saturating_mul(1000));}
            if value.is_none(){self.metrics.deleted+=1;}
        }else{self.metrics.replayed+=1;}
        if active.get(address)!=Some(provider){if !replay{self.metrics.inactive+=1;}return false;}
        let Some(range)=self.account_ranges.get(address).cloned() else{if !replay{self.metrics.unmapped+=1;}return false;};
        let mut initializing=false;
        for position in range {
            let index=self.account_bindings[position];let pi=self.subs[index].pool;
            if !self.pools[pi].ready {self.metrics.initializing+=1;initializing=true;continue;}
            if only.is_some_and(|keys|!keys.contains(&self.pools[pi].key)){continue;}
            let sub=&mut self.subs[index];
            // HTTP gives no write ordering inside one slot. Accept the first strictly
            // newer slot, then preserve all subsequent same-slot WebSocket writes.
            if sub.floor.is_some_and(|floor|*slot<=floor){self.metrics.baseline_skipped+=1;continue;}
            sub.floor=None;
            if *slot<sub.slot{self.metrics.stale+=1;continue;}
            sub.slot=*slot;sub.received_us=*frame_received_at_us;
            self.metrics.bindings+=1;
            let decoded=(||->Result<(Pubkey,&[u8])>{
                let account=value.as_ref().context("account deleted")?;
                ensure!(account.address==*address,"account envelope identity mismatch");
                let (offset,length)=self.subs[index].field.slice();
                let data=if self.subs[index].field.full() {
                    ensure!(account.view==AccountView::Full,"protocol valuation requires full account");account.bytes.as_slice()
                }else {match account.view {
                    AccountView::Full=>{
                        if self.subs[index].field==Field::VirtualQuote && account.bytes.len()<offset+length{&[]}
                        else{bytes(&account.bytes,offset,length)?}
                    },
                    AccountView::Slice{offset:start,..}=>{
                        let begin=offset.checked_sub(start).context("account slice starts after requested field")?;
                        if self.subs[index].field==Field::VirtualQuote && account.bytes.is_empty(){&[]}
                        else{bytes(&account.bytes,begin,length)?}
                    }
                }};
                Ok((account.owner,data))
            })();
            let pipeline=PricePipelineTiming{frame_received_at_us:*frame_received_at_us,transport_decoded_at_us:*decoded_at_us,
                market_received_at_us:market_us,price_published_at_us:0,market_decode_ns:0,market_compute_ns:0,initialization_replay:replay};
            let updated=match decoded {
                Ok((owner,data))=>self.update(index,*slot,owner,data,*frame_received_at_us,pipeline,true),Err(e)=>Err(e),
            };
            if let Err(error)=updated {
                self.metrics.decode_errors+=1;
                self.subs[index].valid=false;self.pools[pi].available=false;
                self.publish_unavailable(pi,*slot,*frame_received_at_us,format!("account update: {error:#}"));
            }
        }
        if !replay{self.metrics.processing.add(nanos(started));}
        initializing
    }
}

/// Registry identities select observations, never establish market prices.
/// Bounded baseline/legacy hydration jobs run while this task consumes live events.
async fn produce(registry:PoolRegistry,mut input:broadcast::Receiver<Arc<PoolUpdateBatch>>,
    rpc:RpcHandle,control:AccountStreamControl,mut events:AccountEventStream,out:broadcast::Sender<Arc<MarketStateEvent>>,
    mut catalog:watch::Receiver<Arc<WatchCatalog>>,mints:Arc<dyn MintReader>,
    mut mint_revision:watch::Receiver<u64>,selected_mints:watch::Sender<Arc<[Pubkey]>>,
    mut stop:watch::Receiver<bool>,mut enabled:watch::Receiver<bool>,settings:Settings)->Result<ProductionReport> {
    ensure!(control.capacity()>0,"market state requires enabled RPC_N_WS_ACCOUNT_SUBSCRIBE providers");
    let snapshot=|registry:&PoolRegistry|->HashMap<Pubkey,PoolInput>{registry.snapshot().iter()
        .filter_map(PoolInput::of).map(|p|(p.address,p)).collect()};
    let mut candidates=snapshot(&registry);
    let mut selected=Vec::<PoolInput>::new();let mut current=HashMap::<Pubkey,PoolInput>::new();
    let mut engine=Engine::empty(out.clone(),0);engine.trace=settings.trace;let mut wanted=Vec::<AccountWatch>::new();
    let mut active=HashMap::<Pubkey,String>::new();let mut planned=HashMap::<Pubkey,String>::new();
    let mut versions=HashMap::<Pubkey,u64>::new();let mut revision=0;
    let mut retry=HashMap::<Pubkey,time::Instant>::new();
    let mut cached=HashMap::<Pubkey,(WsEvent,u64)>::new();
    let mut jobs=tokio::task::JoinSet::new();
    let mut in_flight=HashMap::<Pubkey,time::Instant>::new();
     let concurrency:usize=env_parse("MARKET_STATE_HYDRATION_CONCURRENCY",8)?;
    let batch_pools:usize=env_parse("MARKET_STATE_HYDRATION_BATCH_POOLS",64)?;
    let selection_interval_ms:u64=env_parse("MARKET_STATE_SELECTION_INTERVAL_MS",5000)?;
    let account_budget:usize=env_parse("MARKET_STATE_WS_ACCOUNT_BUDGET",25000)?;
    ensure!((1000..=300000).contains(&selection_interval_ms)
        && account_budget>0,"invalid selection interval or WS account budget");
    let selection_capacity=control.capacity().min(account_budget);

    ensure!((1..=32).contains(&concurrency) && (1..=256).contains(&batch_pools),
        "invalid market hydration concurrency/batch configuration");
    crate::log_event!(Info,"market state hydration configured concurrency={concurrency} batch_pools={batch_pools} mint_source=central_registry baseline=dynamic_only graph_selection=true");
    let mut rebuild=true;let mut replan=false;let mut prepare=true;let mut schedule=true;let mut input_open=true;let mut ticks=0;
    let mut selector_jobs=tokio::task::JoinSet::<(u64,Vec<PoolInput>)>::new();
    let mut selected_ready:Option<Vec<PoolInput>>=None;
    let mut next_selection_at=time::Instant::now();
    crate::log_event!(Info,"market state selector_async=true interval_ms={selection_interval_ms} pool_limit={} account_budget={} transport_capacity={}",
        settings.pool_limit,selection_capacity,control.capacity());
    let mut tick=time::interval(Duration::from_secs(1));tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    loop {
        if *stop.borrow(){break;}
        if let Some(next)=selected_ready.take() {
            if next!=selected {
                let next_map:HashMap<_,_>=next.iter().map(|p|(p.address,*p)).collect();
                let keep:HashSet<_>=selected.iter().filter(|p|next_map.get(&p.address)==Some(*p)).map(|p|p.address).collect();
                for p in &selected {if !keep.contains(&p.address){engine.sequence+=1;engine.emit(MarketStateEvent::Removed{pool:p.address.to_string(),sequence:engine.sequence,reason:PoolRemovalReason::Selection});}}
                engine.retain(&keep);cached.retain(|key,_|engine.account_ranges.contains_key(key));
                versions.retain(|key,_|keep.contains(key));retry.retain(|key,_|keep.contains(key));
                for p in &next {if !keep.contains(&p.address){revision+=1;versions.insert(p.address,revision);}}
                if next.is_empty(){engine.metrics.cleared=true;}
                else if engine.metrics.cleared {engine.metrics.rewatching=true;}
                selected=next;current=next_map;prepare=true;replan=true;engine.metrics.selection_changes+=1;
                let mut mints_to_check=selected.iter().flat_map(|p|[p.mint_a,p.mint_b])
                    .collect::<Vec<_>>();
                mints_to_check.sort_unstable();mints_to_check.dedup();
                selected_mints.send_replace(Arc::from(mints_to_check));
                crate::log_event!(Info,"market state graph selection selected_pools={} ws_upper_bound={} ws_capacity={} graph_topologies={}",
                    selected.len(),selected.iter().map(watch_cost).sum::<usize>(),
                    selection_capacity,catalog.borrow().graph.topologies.len());
                engine.sequence+=1;engine.emit(MarketStateEvent::Selection{prepared:candidates.len(),
                    selected:selected.iter().map(|p|p.address.to_string()).collect(),accounts:0,sequence:engine.sequence});
                crate::log_event!(Debug,"market state selection prepared={} selected={} retained={} initialization=incremental",candidates.len(),selected.len(),keep.len());
            }
        }
        if prepare {
            prepare=false;
            let next=selected.iter().filter(|p|!engine.pool_index.contains_key(&p.address)
                && p.structure_available() && [p.mint_a,p.mint_b].into_iter().all(|mint|
                    mints.mint(mint).and_then(|record|record.onchain).is_some())).copied().collect::<Vec<_>>();
            if !next.is_empty() {
                let started=Instant::now();
                let part=Engine::prepared(&next,mints.as_ref(),out.clone())?;
                for p in &next {*engine.metrics.prepared_structures_by_venue.entry(p.venue.name()).or_default()+=1;}
                replan|=engine.install_structure(part);schedule=true;
                engine.metrics.prepared_structure_build.add(nanos(started));
            }
        }
        if replan {
            let started=Instant::now();
            let failed:Vec<_>=retry.keys().copied().filter(|key|!engine.pool_index.contains_key(key)).collect();
            let next=watches(&engine,&failed)?;
            ensure!(next.len()<=selection_capacity,"market state selected {} pools require {} accounts; WS budget={} configured WS capacity={}",
                selected.len(),next.len(),selection_capacity,control.capacity());
            if next!=wanted {
                let old:HashMap<_,_>=wanted.iter().map(|w|(w.address,w.view)).collect();
                let changed:HashSet<_>=next.iter().filter(|w|old.get(&w.address)!=Some(&w.view)).map(|w|w.address).collect();
                invalidate_accounts(&mut engine,&changed,&mut versions,&mut revision,"account watch changed; awaiting baseline");
                let desired:HashSet<_>=next.iter().map(|w|w.address).collect();
                active.retain(|key,_|desired.contains(key)&&!changed.contains(key));
                planned.retain(|key,_|desired.contains(key));
                control.replace(next.clone())?;wanted=next;
            }
            engine.metrics.watch_plan.add(nanos(started));replan=false;schedule=true;
        }
        if schedule {
            schedule=false;
            while jobs.len()<concurrency {
                let now=time::Instant::now();
                let present=&engine.pool_index;
                let mut targets:Vec<_>=selected.iter().filter(|p|present.get(&p.address)
                    .is_some_and(|&pi|!engine.pools[pi].ready && complete(&engine,pi,&active))
                    && !in_flight.contains_key(&p.address)
                    && retry.get(&p.address).is_none_or(|at|*at<=now)).copied().collect();
                let baseline=!targets.is_empty();
                if !baseline {
                    targets=selected.iter().filter(|p|!p.direct() && !present.contains_key(&p.address)
                        && !in_flight.contains_key(&p.address)
                        && retry.get(&p.address).is_none_or(|at|*at<=now)
                        && [p.mint_a,p.mint_b].into_iter().all(|mint|mints.mint(mint)
                            .and_then(|record|record.onchain).is_some())).copied().collect();
                }
                targets.truncate(batch_pools);
                if targets.is_empty(){break;}
                let source=baseline.then(||engine.subset(&targets.iter().map(|p|p.address).collect()));
                let tickets:Vec<_>=targets.iter().map(|p|(*p,versions[&p.address])).collect();
                for p in &targets {in_flight.insert(p.address,now);}
                let rpc=rpc.clone();let output=out.clone();let reader=mints.clone();
                engine.metrics.hydration_jobs+=1;
                jobs.spawn(async move {
                    let started=Instant::now();
                    let mut reads=RpcReads::default();
                    let result=hydrate(&targets,&rpc,&output,reader.as_ref(),source.as_ref(),&mut reads).await;
                    (tickets,baseline,result,started.elapsed(),reads)
                });
                engine.metrics.max_hydration_in_flight=engine.metrics.max_hydration_in_flight.max(jobs.len());
            }
        }
        tokio::select! {
            // Selection is read-only and potentially expensive; the WS loop
            // keeps consuming account events while this blocking job runs.
            _=time::sleep_until(next_selection_at),if rebuild && selector_jobs.is_empty()=>{
                let source=candidates.clone();
                let view=catalog.borrow().clone();
                let generation=view.generation;
                let previous=selected.clone();
                let owner=registry.clone();
                let pool_cap=settings.pool_limit;
                let enabled_now=*enabled.borrow();
                selector_jobs.spawn_blocking(move||{
                    let picked=if enabled_now {
                        select(&source,&view,pool_cap,selection_capacity,&previous,&owner)
                    }else{Vec::new()};
                    (generation,picked)
                });
                rebuild=false;
                next_selection_at=time::Instant::now()+Duration::from_millis(selection_interval_ms);
            },
            computed=selector_jobs.join_next(),if !selector_jobs.is_empty()=>{
                let (source_generation,mut next)=computed.context("market selection task disappeared")?
                    .context("market selection task panicked")?;
                next.retain(|p|candidates.get(&p.address)==Some(p));
                if !*enabled.borrow(){next.clear();}
                if settings.pool_limit>0 {next.truncate(settings.pool_limit);}
                if source_generation!=catalog.borrow().generation {
                    rebuild=true;
                    crate::log_event!(Debug,"market state selection_generation_advanced old={} new={}",
                        source_generation,catalog.borrow().generation);
                }
                selected_ready=Some(next);
            },
            changed=stop.changed()=>{if changed.is_err()||*stop.borrow(){break;}},
            changed=enabled.changed()=>{
                if changed.is_err(){break;}
                if !*enabled.borrow(){
                    engine.metrics.cleared=false;engine.metrics.rewatching=false;
                    engine.metrics.post_rewatch_websocket_observations=0;
                    selected_ready=Some(Vec::new());
                }
                rebuild=true;next_selection_at=time::Instant::now();
            },
            changed=catalog.changed()=>{if changed.is_err(){break;}rebuild=true;},
            changed=mint_revision.changed()=>{
                if changed.is_err(){break;}schedule=true;prepare=true;
                for pi in 0..engine.pools.len() {
                    let p=&mut engine.pools[pi];
                    let next=[p.a.parse().ok().and_then(|a|mints.mint(a)).and_then(|m|m.onchain),
                        p.b.parse().ok().and_then(|b|mints.mint(b)).and_then(|m|m.onchain)];
                    let changed=(0..2).any(|i|match (&p.mint_states[i],&next[i]){(Some(a),Some(b))=>!Arc::ptr_eq(a,b),(None,None)=>false,_=>true});
                    if changed {p.mint_states=next;if p.ready&&p.available{engine.publish_price(pi,PriceOrigin::Revaluation,None,None,None);}}
                }
            },
            update=input.recv(),if input_open=>match update {
                Ok(batch)=>{for p in &batch.pools {
                    rebuild|=reconcile(&registry,&mut candidates,p.address);
                }},
                Err(broadcast::error::RecvError::Lagged(_))=>{candidates=snapshot(&registry);rebuild=true;},
                Err(broadcast::error::RecvError::Closed)=>input_open=false,
            },
            joined=jobs.join_next(),if !jobs.is_empty()=>{
                let started=Instant::now();
                let (tickets,baseline,result,elapsed,reads)=joined.context("market preparation task disappeared")?.context("market preparation panicked")?;
                for (pool,_) in &tickets {in_flight.remove(&pool.address);}
                if baseline {engine.metrics.baseline_hydration.add(elapsed.as_nanos() as u64);engine.metrics.baseline_rpc.add(&reads);}
                else {engine.metrics.initial_hydration.add(elapsed.as_nanos() as u64);engine.metrics.structure_rpc.add(&reads);}
                let (mut part,failures)=result?;schedule=true;
                let valid:HashSet<_>=tickets.iter().filter(|(p,version)|current.get(&p.address)==Some(p)&&versions.get(&p.address)==Some(version))
                    .map(|(p,_)|p.address).collect();
                engine.metrics.discarded_results+=(part.pools.len()-part.pools.iter().filter(|p|valid.contains(&p.key)).count()) as u64;
                part.retain(&valid);
                let mut failed_watch_changed=false;
                for (p,reason) in failures {if valid.contains(&p.address) {
                    retry.insert(p.address,time::Instant::now()+Duration::from_secs(30));engine.metrics.hydration_failures+=1;
                    if engine.metrics.baseline_failure_samples.len()<4 {
                        engine.metrics.baseline_failure_samples.push(serde_json::json!({"at_us":timestamp_us(),
                            "pool":p.address.to_string(),"baseline":baseline,"reason":reason}));
                    }
                    // First-hydration failures need a root watch; baseline failures keep their watches.
                    failed_watch_changed|=!baseline;
                    engine.sequence+=1;engine.emit(MarketStateEvent::Unavailable{pool:p.address.to_string(),venue:Venue::from_core(p.venue).map_or("unsupported",Venue::label),slot:0,received_at_us:timestamp_us(),sequence:engine.sequence,reason});
                }}
                let keys:HashSet<_>=part.pools.iter().map(|p|p.key).collect();
                for p in &mut part.pools {p.ready=baseline;retry.remove(&p.key);}
                if baseline {
                    let positions=engine.install_baseline(part)?;
                    for &pi in &positions {
                        if engine.pools[pi].available {engine.publish_price(pi,PriceOrigin::Baseline,None,None,None);}
                        else{engine.publish_unavailable(pi,engine.pools[pi].slot,timestamp_us(),"baseline has no valid indicative price".into());}
                    }
                    let replay_keys:HashSet<_>=positions.iter().flat_map(|&pi|engine.pool_subs[pi].iter()
                        .map(|&i|engine.subs[i].key)).collect();
                    for key in replay_keys {if let Some((event,received))=cached.remove(&key) {
                        if engine.account(&event,&active,received,true,Some(&keys)){cached.insert(key,(event,received));}
                    }}
                }else{
                    replan|=engine.install_structure(part) || failed_watch_changed;
                }
                engine.metrics.install.add(nanos(started));
            },
            event=events.recv()=>{
                engine.metrics.max_queue_depth=engine.metrics.max_queue_depth.max(events.len());
                match event {
                    Ok(event@WsEvent::Account{address,slot,..})=>{
                        let received=timestamp_us();
                        if engine.account(&event,&active,received,false,None) {
                            let newer=cached.get(&address).is_none_or(|(old,_)|matches!(old,WsEvent::Account{slot:old,..} if slot>=*old));
                            if newer && cached.insert(address,(event,received)).is_some(){engine.metrics.buffer_replaced+=1;}
                        }
                    },
                    Ok(WsEvent::Assigned{addresses,provider})=>{
                        for &address in addresses.iter(){planned.insert(address,provider.clone());}
                    },
                    Ok(WsEvent::Subscribed{address,provider})=>{active.insert(address,provider);schedule=true;},
                    Ok(WsEvent::Unsubscribed{address,provider})=>{
                        if active.get(&address)==Some(&provider){active.remove(&address);invalidate_accounts(&mut engine,&HashSet::from([address]),&mut versions,&mut revision,"account unsubscribed");}
                    },
                    Ok(WsEvent::Gap{provider,reason})=>{
                        engine.metrics.gaps+=1;
                        *engine.metrics.subscription_gaps.entry(provider.clone()).or_default()+=1;
                        let keys:HashSet<_>=active.iter().filter(|(_,source)|source.as_str()==provider.as_str()).map(|(key,_)|*key).collect();
                        active.retain(|_,source|source.as_str()!=provider.as_str());
                        let (affected,ready)=invalidate_accounts(&mut engine,&keys,&mut versions,&mut revision,reason);
                        if !keys.is_empty() {
                            let mut shared=keys.iter().filter_map(|key|engine.account_ranges.get(key)
                                .filter(|r|r.len()>1).map(|r|(r.len(),*key))).collect::<Vec<_>>();
                            shared.sort_unstable_by(|a,b|b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
                            if engine.metrics.subscription_gap_samples.len()==8{engine.metrics.subscription_gap_samples.pop_front();}
                            engine.metrics.subscription_gap_samples.push_back(serde_json::json!({
                                "at_us":timestamp_us(),"provider":provider,"reason":reason,"removed_accounts":keys.len(),
                                "invalidated_pools":affected,"previously_ready_pools":ready,
                                "shared_accounts":shared.into_iter().take(8).map(|(count,key)| {
                                    let range=&engine.account_ranges[&key];let sub=&engine.subs[engine.account_bindings[range.start]];
                                    serde_json::json!({"address":key.to_string(),"field":sub.field.label(),"pool_bindings":count})
                                }).collect::<Vec<_>>()
                            }));
                        }
                    },
                    Ok(WsEvent::Rejected{address,provider,code})=>{
                        engine.metrics.rejections+=1;active.remove(&address);
                        invalidate_accounts(&mut engine,&HashSet::from([address]),&mut versions,&mut revision,"subscription rejected");
                        crate::log_event!(Warn,"market state subscription rejected provider={provider} account={address} code={code}");
                    },
                    Err(broadcast::error::RecvError::Lagged(n))=>{
                        schedule=true;
                        engine.metrics.lost+=n;let keys=engine.account_ranges.keys().copied().collect();cached.clear();
                        invalidate_accounts(&mut engine,&keys,&mut versions,&mut revision,"account receiver lagged");
                        engine.sequence+=1;engine.emit(MarketStateEvent::Gap{reason:format!("account receiver lagged {n}"),sequence:engine.sequence});
                    },
                    Err(broadcast::error::RecvError::Closed)=>bail!("market account transport closed"),
                }
            },
            _=tick.tick()=>{
                ticks+=1;
                if !retry.is_empty(){schedule=true;}
                // TVL quality may change without changing PoolInput identity.
                if ticks%60==0 {rebuild=true;}
                if ticks%5==0 {
                    let hydrated=engine.pools.len();
                    let ready=engine.pools.iter().filter(|p|p.ready).count();
                    let available=engine.pools.iter().filter(|p|p.ready && p.available).count();
                    crate::log_event!(Info,
                        "market state hydration progress selected={} hydrated={} ready={} priced={} in_flight={} init_jobs={} baseline_jobs={} initial_mean_ms={} baseline_mean_ms={} mint_verified_pools={} live_index_rebuilds={} structure_installs={} baseline_installs={} baseline_pools_installed={} watch_plans={}",
                        selected.len(),hydrated,ready,available,in_flight.len(),
                        engine.metrics.initial_hydration.count,engine.metrics.baseline_hydration.count,
                        if engine.metrics.initial_hydration.count>0 {engine.metrics.initial_hydration.total/engine.metrics.initial_hydration.count as u128/1_000_000}else{0},
                        if engine.metrics.baseline_hydration.count>0 {engine.metrics.baseline_hydration.total/engine.metrics.baseline_hydration.count as u128/1_000_000}else{0},
                        selected.iter().filter(|p|[p.mint_a,p.mint_b].into_iter().all(|mint|mints.mint(mint)
                            .and_then(|r|r.onchain).is_some())).count(),
                        engine.metrics.index_rebuilds,engine.metrics.structure_installs,engine.metrics.baseline_installs,
                        engine.metrics.baseline_pools_installed,engine.metrics.watch_plan.count);
                    for p in &selected {rebuild|=reconcile(&registry,&mut candidates,p.address);}
                    let (ws_messages,ws_text_bytes)=control.stream_counts();engine.sequence+=1;
                    engine.emit(MarketStateEvent::Stats{pools:selected.len(),ready:engine.pools.iter().filter(|p|p.ready).count(),
                        available:engine.pools.iter().filter(|p|p.available).count(),accounts:wanted.len(),active:active.len(),ws_messages,ws_text_bytes,sequence:engine.sequence});
                }
            }
        }
    }
    // Dropping JoinSet cancels unfinished preparation; no detached HTTP initializer.
    drop(jobs);
    crate::log_event!(Info,"market state hydration final selected={} hydrated={} ready={} priced={} initial_jobs={} baseline_jobs={} max_in_flight={} hydration_failures={} live_index_rebuilds={} structure_installs={} baseline_installs={} baseline_pools_installed={} watch_plans={}",
        selected.len(),engine.pools.len(),engine.pools.iter().filter(|p|p.ready).count(),engine.pools.iter().filter(|p|p.ready&&p.available).count(),
        engine.metrics.initial_hydration.count,engine.metrics.baseline_hydration.count,
        engine.metrics.max_hydration_in_flight,engine.metrics.hydration_failures,
        engine.metrics.index_rebuilds,engine.metrics.structure_installs,engine.metrics.baseline_installs,
        engine.metrics.baseline_pools_installed,engine.metrics.watch_plan.count);
    let selected_pools=selected.len();
    let hydrated_pools=engine.pools.len();
    let ready_pools=engine.pools.iter().filter(|p|p.ready).count();
    let priced_pools=engine.pools.iter().filter(|p|p.ready&&p.available).count();
    let final_sample=engine.preparation_sample(&selected,mints.as_ref(),&active,&planned,&in_flight,&retry);
    let preparation=final_sample.stages.clone();engine.metrics.preparation_final=Some(final_sample);
    for p in &selected {engine.sequence+=1;engine.emit(MarketStateEvent::Removed{pool:p.address.to_string(),sequence:engine.sequence,reason:PoolRemovalReason::Shutdown});}
    crate::log_event!(Info,"market state stopped=true events={} observations={} websocket={} lost={}",
        engine.sequence,engine.metrics.baseline_prices+engine.metrics.websocket_prices+engine.metrics.fee_valuation.revaluations,engine.metrics.websocket_prices,engine.metrics.lost);
    Ok(ProductionReport{events:engine.sequence,
        observations:engine.metrics.baseline_prices+engine.metrics.websocket_prices+engine.metrics.fee_valuation.revaluations,
        websocket_observations:engine.metrics.websocket_prices,
        post_rewatch_websocket_observations:engine.metrics.post_rewatch_websocket_observations,
        selected_pools,hydrated_pools,ready_pools,priced_pools,preparation,metrics:engine.metrics})
}

/// Mutually exclusive stages of the selected set at shutdown; no price mirror.
#[derive(Clone,Default,serde::Serialize)]
struct PreparationStatus {
    waiting_structure:usize,waiting_mints:usize,waiting_subscriptions:usize,
    waiting_baseline:usize,unpriceable:usize,priced:usize,
}
#[derive(Default,serde::Serialize)]
struct ProviderReadiness {component_accounts:usize,acknowledged:usize,missing:usize,blocked_pools:usize}
#[derive(serde::Serialize)]
struct PreparationSample {
    at_us:u64,build_ns:u64,selected:usize,net_direction_pools:[usize;3],stages:PreparationStatus,
    baseline_in_flight:usize,baseline_backoff:usize,baseline_queued:usize,oldest_baseline_in_flight_ms:u64,
    providers:BTreeMap<String,ProviderReadiness>,missing_bindings_by_field:BTreeMap<&'static str,usize>,
    blockers:Vec<serde_json::Value>,shared_accounts:Vec<serde_json::Value>,
}
/// Counters and timings from the state owner; no diagnostic price mirror.
#[derive(serde::Serialize)]
pub struct ProductionReport {
    pub events:u64,pub observations:u64,pub websocket_observations:u64,
    pub post_rewatch_websocket_observations:u64,
    selected_pools:usize,hydrated_pools:usize,ready_pools:usize,priced_pools:usize,
    preparation:PreparationStatus,metrics:Metrics,
}
impl ProductionReport {
    pub fn ensure_delivery_evidence(&self)->Result<()> {
        ensure!(self.observations>0 && self.websocket_observations>0,"no real WebSocket price observations");
        ensure!(self.metrics.lost==0 && self.metrics.no_receiver==0,
            "MarketState lost account inputs or published without a consumer");
        Ok(())
    }
    pub fn ensure_rewatch_evidence(&self)->Result<()> {
        ensure!(self.metrics.selection_changes>=3 && self.metrics.removals>0 && self.post_rewatch_websocket_observations>0,
            "rewatch did not complete removal, reselection and subsequent WebSocket prices");
        Ok(())
    }
}

/// Cancels hydration as well as idle streaming, then releases the sole desired watch set.
pub async fn run(registry:PoolRegistry,input:broadcast::Receiver<Arc<PoolUpdateBatch>>,
    rpc:RpcHandle,control:AccountStreamControl,events:AccountEventStream,out:broadcast::Sender<Arc<MarketStateEvent>>,settings:Settings,
    catalog:watch::Receiver<Arc<WatchCatalog>>,mints:Arc<dyn MintReader>,mint_revision:watch::Receiver<u64>,
    selected_mints:watch::Sender<Arc<[Pubkey]>>,
    stop:watch::Receiver<bool>,enabled:watch::Receiver<bool>)->Result<ProductionReport> {
    let result=produce(registry,input,rpc,control.clone(),events,out,catalog,mints,
        mint_revision,selected_mints,stop,enabled,settings).await;
    let cleanup=control.replace(Vec::new());
    crate::log_event!(Info,"market state released_watches=true");
    let report=result?;cleanup?;Ok(report)
}

/// Standalone MarketState proof only: count the real output without retaining prices.
#[derive(Default,serde::Serialize)]
pub struct ObservationReport {pub events:u64,pub observations:u64,pub websocket_observations:u64}
pub async fn observe(mut input:PriceEventStream)->Result<ObservationReport> {
    let mut report=ObservationReport::default();
    loop {match input.recv().await {
        Ok(event)=>{
            report.events+=1;
            if let MarketStateEvent::Price(p)=event.as_ref() {
                report.observations+=1;
                report.websocket_observations+=u64::from(matches!(p.origin,PriceOrigin::Websocket));
            }
        },
        Err(broadcast::error::RecvError::Lagged(n))=>bail!("MarketState proof consumer lost {n} events"),
        Err(broadcast::error::RecvError::Closed)=>break,
    }}
    Ok(report)
}
