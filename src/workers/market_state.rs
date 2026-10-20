//! Indicative market state: Prepared pool inputs -> account watches -> typed prices.
//! Protocol decoding lives here; connections and provider policy live in rpc_transport.
mod live;
pub use live::decode_live;
use anyhow::{Context, Result, bail, ensure};
use std::{collections::{HashMap, HashSet, BTreeMap}, env, sync::Arc, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, watch}, time};
use crate::core::{CompletionState, PoolState, PoolUpdateBatch, Venue as CoreVenue,
    market_state::{Consistency, MarketStateEvent, PoolPriceObservation, PriceEventStream, PriceKind, PriceOrigin, PricePipelineTiming}};
use super::{pool_registry::PoolRegistry, rpc_transport::{RpcAnswer, RpcHandle, RpcOperation},
    graph_watch_catalog::{WatchCatalog,RouteKind}};
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
async fn fetch_batch(rpc:&RpcHandle,addresses:&[String])->Result<Vec<RpcAccount>> {
    let mut records=Vec::with_capacity(addresses.len());
    for chunk in addresses.chunks(rpc.account_batch_size()) {
        let addresses=chunk.iter().map(|a|a.parse()).collect::<Result<Vec<Pubkey>,_>>()?;
        let answer=rpc.call(RpcOperation::Multiple{addresses,view:AccountView::Full,min_context_slot:None}).await?;
        let received_us=answer.received_at_ms.saturating_mul(1000);
        let RpcAnswer::Multiple{slot,accounts}=answer.answer else {bail!("market state expected account batch");};
        ensure!(accounts.len()==chunk.len(),"market state account batch length mismatch");
        for (address,account) in chunk.iter().zip(accounts) {
            let account=account.with_context(||format!("market account missing {address}"))?;
            ensure!(account.address.to_string()==*address && account.view==AccountView::Full,"market account identity/view mismatch");
            records.push(RpcAccount{owner:account.owner.to_string(),data:account.bytes,slot,received_us});
        }
    }
    Ok(records)
}
#[derive(Clone)]struct Watch {address:String,venue:Venue}
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
fn mint_dec(a:&RpcAccount)->Result<u8>{
    ensure!(a.owner==TOKEN_PROGRAM||a.owner==TOKEN_2022,"Invalid token mint program {}",a.owner);
    Ok(bytes(&a.data,44,1)?[0])
}
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

#[derive(Clone)]enum State{
    Dlmm{active:i32,step:u16},
    Pump{vquote:i128,base:u64,quote:u64,base_vault:String,quote_vault:String,
         pool_slot:u64,base_slot:u64,quote_slot:u64},
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
struct Pool{
    key:Pubkey,ready:bool,id:String,venue:Venue,a:String,b:String,da:u8,db:u8,
    canon_base:String,canon_quote:String,state:State,
    price:f64,available:bool,slot:u64,received_us:u64,updates:u64,
}
impl Pool{
    fn compute_price(&self)->Result<f64>{
        let raw=match &self.state{
            State::Dlmm{active,step}=>dlmm_spot(*active,*step,self.da,self.db)?,
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
#[derive(Clone,Copy,Debug,PartialEq,Eq,Hash)]enum Field{Dlmm,VirtualQuote,BaseVault,QuoteVault,DammSqrt,RaydiumV4Pnl,RaydiumV4BaseVault,RaydiumV4QuoteVault,RaydiumClmmSqrt,RaydiumCpmmFees,RaydiumCpmmBaseVault,RaydiumCpmmQuoteVault,OrcaWhirlpoolSqrt,PancakeSwapClmmSqrt,ManifestBook,ByrealClmmSqrt}
impl Field{
    fn label(self)->&'static str{match self{Self::Dlmm=>"dlmm_lbpair",Self::VirtualQuote=>"pump_pool",Self::BaseVault=>"pump_base_vault",Self::QuoteVault=>"pump_quote_vault",Self::DammSqrt=>"damm_v2_pool",Self::RaydiumV4Pnl=>"raydium_v4_pool_pnl",
        Self::RaydiumV4BaseVault=>"raydium_v4_base_vault",Self::RaydiumV4QuoteVault=>"raydium_v4_quote_vault",
        Self::RaydiumClmmSqrt=>"raydium_clmm_pool",
        Self::RaydiumCpmmFees=>"raydium_cpmm_pool_fees",Self::RaydiumCpmmBaseVault=>"raydium_cpmm_base_vault",
        Self::RaydiumCpmmQuoteVault=>"raydium_cpmm_quote_vault",
        Self::OrcaWhirlpoolSqrt=>"orca_whirlpool_pool",
        Self::PancakeSwapClmmSqrt=>"pancakeswap_clmm_pool",Self::ManifestBook=>"manifest_market_book",
        Self::ByrealClmmSqrt=>"byreal_clmm_pool"}}
    fn slice(self)->(usize,usize){match self{Self::Dlmm=>(76,6),Self::VirtualQuote=>(245,16),Self::BaseVault|Self::QuoteVault=>(64,8),Self::DammSqrt=>(DAMM_SQRT_OFFSET,16),Self::RaydiumV4Pnl=>(192,16),
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
struct Subscription{pool:usize,field:Field,address:String,key:Pubkey,owner:Pubkey,
    slot:u64,received_us:u64,floor:Option<u64>,valid:bool}
// Fixed-size counters/histograms; no per-update logging or sample serialization.
#[derive(Default)]
struct Latency {count:u64,total:u128,max:u64,bins:[u64;32]}
impl Latency {
    fn add(&mut self,ns:u64){self.count+=1;self.total+=u128::from(ns);self.max=self.max.max(ns);self.bins[(64-ns.leading_zeros() as usize).min(31)]+=1;}
    fn report(&self)->serde_json::Value {
        let upper=|percent:u64|{let target=(self.count*percent).div_ceil(100);let mut count=0;
            self.bins.iter().enumerate().find_map(|(i,n)|{count+=n;(count>=target && target>0).then(||if i==31{self.max}else{((1_u64<<i)-1).min(self.max)})})};
        serde_json::json!({"count":self.count,"mean_ns":(self.count>0).then(||self.total as f64/self.count as f64),
            "max_ns":(self.count>0).then_some(self.max),"p95_upper_ns":upper(95),"p99_upper_ns":upper(99)})
    }
}
#[derive(Default)]
struct VenueTiming {decode:Latency,compute:Latency,publish:Latency,prices:u64,unavailable:u64,decode_errors:u64}
#[derive(Default)]
struct Metrics {
    received:u64,unmapped:u64,inactive:u64,initializing:u64,buffer_replaced:u64,replayed:u64,
    baseline_skipped:u64,stale:u64,bindings:u64,deleted:u64,gaps:u64,lost:u64,rejections:u64,
    baseline_prices:u64,websocket_prices:u64,no_receiver:u64,max_queue_depth:usize,
    selection_changes:u64,hydration_jobs:u64,discarded_results:u64,hydration_failures:u64,
    processing:Latency,queue:Latency,install:Latency,watch_plan:Latency,venues:[VenueTiming;10],
}
impl Metrics {
    fn report(&self)->serde_json::Value {
        let names=["meteora_dlmm","pumpswap","meteora_damm_v2","raydium_amm_v4","raydium_clmm","raydium_cpmm","orca_whirlpool","pancakeswap_clmm","manifest","byreal_clmm"];
        let venues:BTreeMap<_,_>=names.into_iter().zip(&self.venues).map(|(name,v)|(name,serde_json::json!({
            "prices":v.prices,"unavailable":v.unavailable,"decode_errors":v.decode_errors,
            "protocol_decode":v.decode.report(),"price_compute":v.compute.report(),"price_publish":v.publish.report()}))).collect();
        serde_json::json!({"account_updates":self.received,"unmapped_updates":self.unmapped,"inactive_updates":self.inactive,
            "initializing_bindings":self.initializing,"initialization_buffer_replacements":self.buffer_replaced,"replayed_updates":self.replayed,
            "baseline_floor_skipped_bindings":self.baseline_skipped,"stale_bindings":self.stale,"processed_bindings":self.bindings,
            "deleted_accounts":self.deleted,"gaps":self.gaps,"lost_events":self.lost,"rejections":self.rejections,
            "baseline_prices":self.baseline_prices,"websocket_prices":self.websocket_prices,"no_receiver":self.no_receiver,
            "max_queue_depth":self.max_queue_depth,"selection_changes":self.selection_changes,"hydration_jobs":self.hydration_jobs,
            "discarded_preparation_pools":self.discarded_results,"hydration_failures":self.hydration_failures,
            "account_processing":self.processing.report(),"transport_to_market":self.queue.report(),
            "preparation_install":self.install.report(),"watch_reconcile":self.watch_plan.report(),"by_venue":venues})
    }
}
struct Engine {
    pools: Vec<Pool>, subs: Vec<Subscription>, metrics:Metrics,
    events: broadcast::Sender<Arc<MarketStateEvent>>, sequence: u64,
    account_ranges:HashMap<Pubkey,std::ops::Range<usize>>, account_bindings:Vec<usize>,
    pool_subs:Vec<Vec<usize>>,
}
impl Engine{
    fn empty(events:broadcast::Sender<Arc<MarketStateEvent>>,sequence:u64)->Self {
        Self{pools:Vec::new(),subs:Vec::new(),metrics:Metrics::default(),events,sequence,
            account_ranges:HashMap::new(),account_bindings:Vec::new(),pool_subs:Vec::new()}
    }
    fn index(&mut self) {
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
        let mut remap=vec![None;self.pools.len()];let mut next=0;
        for (i,p) in self.pools.iter().enumerate(){if keep.contains(&p.key){remap[i]=Some(next);next+=1;}}
        self.pools.retain(|p|keep.contains(&p.key));
        self.subs.retain_mut(|s|match remap[s.pool]{Some(pi)=>{s.pool=pi;true},None=>false});
        self.index();
    }
    fn install(&mut self,mut part:Engine) {
        let replaced:HashSet<_>=part.pools.iter().map(|p|p.key).collect();
        let keep=self.pools.iter().filter(|p|!replaced.contains(&p.key)).map(|p|p.key).collect();
        self.retain(&keep);
        let offset=self.pools.len();for sub in &mut part.subs{sub.pool+=offset;}
        self.pools.extend(part.pools);self.subs.extend(part.subs);self.index();
    }
    async fn hydrate(cfg:Config,rpc:&RpcHandle,events:broadcast::Sender<Arc<MarketStateEvent>>,sequence:u64,prices:bool)->Result<Self>{
        let pool_addrs:Vec<_>=cfg.watches.iter().map(|w|w.address.clone()).collect();
        let pool_records=fetch_batch(rpc,&pool_addrs).await?;
        let mut raw_pools=Vec::new();let mut details=Vec::<String>::new();
        for (watch,acc) in cfg.watches.iter().zip(&pool_records){
            let pool_slot=acc.slot;
            let (a,b,state)=match watch.venue{
                Venue::Dlmm=>{
                    ensure!(acc.owner==DLMM_PROGRAM&&acc.data.starts_with(&DLMM_DISC),"{} not a DLMM LbPair",watch.address);
                    let active=i32_le(&acc.data,76)?;let step=u16_le(&acc.data,80)?;
                    (key(&acc.data,88)?,key(&acc.data,120)?,State::Dlmm{active,step})
                },
                Venue::Pump=>{
                    ensure!(acc.owner==PUMP_PROGRAM&&acc.data.starts_with(&POOL_DISC),"{} not a PumpSwap Pool",watch.address);
                    let a=key(&acc.data,43)?;let b=key(&acc.data,75)?;
                    let base_vault=key(&acc.data,139)?;let quote_vault=key(&acc.data,171)?;
                    details.push(base_vault.clone());details.push(quote_vault.clone());
                    let vquote=if acc.data.len()>=261{i128_le(&acc.data,245)?}else{0};
                    (a,b,State::Pump{vquote,base:0,quote:0,base_vault,quote_vault,
                        pool_slot,base_slot:pool_slot,quote_slot:pool_slot})
                },
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
            details.push(a.clone());details.push(b.clone());
            raw_pools.push((a,b,state,acc.slot));
        }
        let mut unique=Vec::new();let mut seen=HashSet::new();
        for address in details{if seen.insert(address.clone()){unique.push(address);}}
        let records=fetch_batch(rpc,&unique).await?;
        let dict:HashMap<String,RpcAccount>=unique.into_iter().zip(records).collect();
        let mut pools=Vec::new();let mut subs=Vec::new();
        for (idx,(watch,(a,b,mut state,pool_slot))) in cfg.watches.iter().zip(raw_pools).enumerate(){
            let da=mint_dec(dict.get(&a).context("token A mint missing")?)?;
            let db=mint_dec(dict.get(&b).context("token B mint missing")?)?;
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
            if let State::Pump{base,quote,base_vault,quote_vault,base_slot,quote_slot,..}=&mut state {
                *base=vault_amount(dict.get(base_vault).context("base vault missing")?,&a)?;
                *quote=vault_amount(dict.get(quote_vault).context("quote vault missing")?,&b)?;
                *base_slot=dict.get(base_vault).context("base vault slot missing")?.slot;
                *quote_slot=dict.get(quote_vault).context("quote vault slot missing")?.slot;
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
                State::Pump{base_slot,quote_slot,..}|
                State::RaydiumAmmV4{base_slot,quote_slot,..}|
                State::RaydiumCpmm{base_slot,quote_slot,..}=>pool_slot.max(*base_slot).max(*quote_slot),
                _=>pool_slot,
            };
            let mut pool=Pool{key:watch.address.parse()?,ready:false,id:watch.address.clone(),venue:watch.venue,a,b,da,db,
                canon_base:canon_base.clone(),canon_quote:canon_quote.clone(),state,
                price:0.0,available:false,slot:observed_slot,received_us:pool_records[idx].received_us,updates:0,
            };
            if prices {
                if let Ok(price)=pool.compute_price(){pool.price=price;pool.available=true;}
            }
            let pool_field=match watch.venue{Venue::Dlmm=>Field::Dlmm,Venue::Pump=>Field::VirtualQuote,Venue::DammV2=>Field::DammSqrt,Venue::RaydiumAmmV4=>Field::RaydiumV4Pnl,Venue::RaydiumClmm=>Field::RaydiumClmmSqrt,Venue::RaydiumCpmm=>Field::RaydiumCpmmFees,Venue::OrcaWhirlpool=>Field::OrcaWhirlpoolSqrt,Venue::PancakeSwapClmm=>Field::PancakeSwapClmmSqrt,Venue::Manifest=>Field::ManifestBook,Venue::ByrealClmm=>Field::ByrealClmmSqrt};
            subs.push(Subscription{pool:idx,field:pool_field,address:watch.address.clone(),key:watch.address.parse()?,owner:pool_records[idx].owner.parse()?,slot:pool_slot,received_us:pool_records[idx].received_us,floor:Some(pool_slot),valid:true});
            if let State::Pump{base_vault,quote_vault,..}=&pool.state{
                for (field,address) in [(Field::BaseVault,base_vault),(Field::QuoteVault,quote_vault)]{
                    let acc=dict.get(address).context("vault not found")?;
                    subs.push(Subscription{pool:idx,field,address:address.clone(),key:address.parse()?,owner:acc.owner.parse()?,slot:acc.slot,received_us:acc.received_us,floor:Some(acc.slot),valid:true});
                }
            }
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
        let sequence=self.next_sequence();
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
            price_quote_per_base:pool.price,
            previous_price:previous,
            delta_bps:previous.map(|old|bps(old,pool.price)).filter(|v|v.is_finite()),
            price_kind:pool.price_kind(),
            best_bid_quote_per_base:best_bid,best_ask_quote_per_base:best_ask,
            consistency:pool.consistency(),slot:pool.slot,received_at_us:component_received_at_us_max,
            sequence,component_slot_min,component_slot_max,component_received_at_us_min,component_received_at_us_max,trigger_field:field.map(Field::label),
            pipeline:None,
        };
        let mut event=event;
        if let Some(t)=pipeline.as_mut(){t.price_published_at_us=timestamp_us();}
        event.pipeline=pipeline;
        let started=Instant::now();
        if self.events.send(Arc::new(MarketStateEvent::Price(event))).is_err(){self.metrics.no_receiver+=1;}
        let v=&mut self.metrics.venues[self.pools[pi].venue as usize];v.publish.add(nanos(started));
        if matches!(origin,PriceOrigin::Websocket){v.prices+=1;self.metrics.websocket_prices+=1;}else{self.metrics.baseline_prices+=1;}
    }
    fn publish_unavailable(&mut self,pi:usize,slot:u64,recv_us:u64,reason:String) {
        let sequence=self.next_sequence();
        let p=&self.pools[pi];
        let _=self.events.send(Arc::new(MarketStateEvent::Unavailable{
            pool:p.id.clone(),venue:p.venue.label(),slot,
            received_at_us:recv_us,sequence,reason,
        }));
    }
    fn update(&mut self,index:usize,slot:u64,owner:Pubkey,data:&[u8],recv_us:u64,pipeline:PricePipelineTiming)->Result<()> {
        let decode_started=Instant::now();
        let mut pipeline=pipeline;
        let (pi,field,expected_owner,expected_len)={let s=&self.subs[index];
            (s.pool,s.field,s.owner,s.field.slice().1)};
        ensure!(owner==expected_owner,"{} owner mismatch",self.subs[index].address);
        ensure!(field==Field::ManifestBook || data.len()==expected_len ||
            (field==Field::VirtualQuote && data.is_empty()),
            "slice length mismatch: {} expected {} got {}",field.label(),expected_len,data.len());
        let newest=self.pool_subs[pi].iter().map(|&i|self.subs[i].slot).max().unwrap_or(slot);
        let pool=&mut self.pools[pi];
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
            (State::Dlmm{active,step},Field::Dlmm)=>{
                let next_active=i32_le(data,0)?;
                let next_step=u16_le(data,4)?;
                ensure!(next_step>0,"DLMM step zero");
                *active=next_active;*step=next_step;
            },
            (State::Pump{vquote,pool_slot,..},Field::VirtualQuote)=>{
                *vquote=if data.is_empty(){0}else{i128_le(data,0)?};*pool_slot=slot;
            },
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
        self.metrics.venues[pool.venue as usize].decode.add(pipeline.market_decode_ns);
        self.subs[index].valid=true;
        pool.slot=newest;
        pool.received_us=recv_us;pool.updates+=1;
        if self.pool_subs[pi].iter().any(|&i|!self.subs[i].valid) {pool.available=false;return Ok(());}
        if let Some(reason)=unavailable_reason {
            self.metrics.venues[pool.venue as usize].unavailable+=1;
            pool.available=false;
            self.publish_unavailable(pi,slot,recv_us,reason);
            return Ok(());
        }
        let compute_started=Instant::now();
        let price=match pool.compute_price() {
            Ok(price)=>price,
            Err(error)=>{
                self.metrics.venues[pool.venue as usize].compute.add(nanos(compute_started));
                self.metrics.venues[pool.venue as usize].unavailable+=1;
                pool.available=false;
                self.publish_unavailable(pi,slot,recv_us,error.to_string());
                return Ok(());
            }
        };
        pipeline.market_compute_ns=nanos(compute_started);
        self.metrics.venues[pool.venue as usize].compute.add(pipeline.market_compute_ns);
        pool.price=price;
        pool.available=true;
        // After any unavailable state, the next valid observation has no
        // adjacent previous price. This applies to every venue, not just Manifest.
        let previous=if was_available{Some(old)}else{None};
        self.publish_price(pi,PriceOrigin::Websocket,previous,Some(field),Some(pipeline));
        Ok(())
    }

}


#[derive(Clone,Copy)]
pub struct Settings {pub pool_limit:usize,pub trace:bool}
impl Settings {
    pub fn from_env()->Result<Self> {Ok(Self{
        pool_limit:env_parse("MARKET_STATE_POOL_LIMIT",1000)?,
        trace:env_parse::<u8>("MARKET_STATE_TRACE",0)?!=0,
    })}
}

// Only the price worker's identity index is retained; manifests and canonical pool
// lifecycle remain in the Registry. Notifications reconcile against the current owner.
#[derive(Clone,Copy,PartialEq,Eq)]
struct PoolInput {address:Pubkey,venue:CoreVenue,mint_a:Pubkey,mint_b:Pubkey}
impl PoolInput {
    fn of(p:&PoolState)->Option<Self> {
        if !p.completion_admission.is_admitted(){return None;}
        Self::prepared(p)
    }
    fn prepared(p:&PoolState)->Option<Self> {
        if p.completion_state!=CompletionState::Prepared{return None;}
        Some(Self{address:p.address,venue:p.venue,mint_a:p.mint_a?,mint_b:p.mint_b?})
    }
}
fn reconcile(registry:&PoolRegistry,candidates:&mut HashMap<Pubkey,PoolInput>,address:Pubkey)->bool {
    let current=registry.get(address).as_ref().and_then(PoolInput::of);
    if candidates.get(&address).copied()==current {return false;}
    if let Some(p)=current {candidates.insert(address,p);}else{candidates.remove(&address);}
    true
}
fn select(candidates:&HashMap<Pubkey,PoolInput>,limit:usize)->Vec<PoolInput> {
    let mut pools:Vec<_>=candidates.values().copied().collect();
    pools.sort_by_key(|p|(p.mint_a.min(p.mint_b),p.mint_a.max(p.mint_b),p.address));
    if limit>0 {pools.truncate(limit);}
    pools
}
fn send(out:&broadcast::Sender<Arc<MarketStateEvent>>,event:MarketStateEvent) {
    let _=out.send(Arc::new(event));
}
// Preparation never publishes or borrows the running engine. Only the event loop
// installs results, after checking that identities and resync revisions still match.
async fn hydrate(pools:&[PoolInput],rpc:&RpcHandle,out:&broadcast::Sender<Arc<MarketStateEvent>>,
    prices:bool)->Result<(Engine,Vec<(PoolInput,String)>)> {
    let mut engine=Engine::empty(out.clone(),0);
    let mut pending=vec![pools.to_vec()];let mut failed=Vec::new();
    while let Some(batch)=pending.pop() {
        if batch.is_empty(){continue;}
        let config=(||->Result<Config>{Ok(Config{watches:batch.iter().map(|p|
            Ok(Watch{address:p.address.to_string(),venue:Venue::from_core(p.venue)?}))
            .collect::<Result<Vec<_>>>()?})})();
        let result=match config {Ok(cfg)=>Engine::hydrate(cfg,rpc,out.clone(),0,prices).await,Err(e)=>Err(e)};
        let result=result.and_then(|part| {
            for (actual,expected) in part.pools.iter().zip(&batch) {
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
        let view=if s.field==Field::ManifestBook {AccountView::Full}else{AccountView::Slice{offset,length}};
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

/// Real transport input only: persisted Prepared proofs are not live Registry admission.
/// Reuse the production account discovery and slice planner; do not publish prices.
pub async fn account_stream_plan(registry:&PoolRegistry,rpc:&RpcHandle)->Result<Vec<AccountWatch>> {
    let started=time::Instant::now();let limit=Settings::from_env()?.pool_limit;
    let candidates=registry.snapshot().iter().filter_map(PoolInput::prepared).map(|p|(p.address,p)).collect();
    let selected=select(&candidates,limit);
    ensure!(!selected.is_empty(),"no persisted Prepared pools for the account transport plan");
    crate::log_event!(Info,"market account plan preparing=true prepared={} selected={} pool_limit={limit} admission=historical_proof_only prices=false",candidates.len(),selected.len());
    let (output,_)=broadcast::channel(16);
    let (engine,failed)=hydrate(&selected,rpc,&output,false).await?;
    let wanted=watches(&engine,&[])?;
    let failures:Vec<_>=failed.iter().map(|(p,reason)|serde_json::json!({"pool":p.address.to_string(),"reason":reason})).collect();
    let mut venues=BTreeMap::<&str,usize>::new();
    for pool in &engine.pools {*venues.entry(pool.venue.label()).or_default()+=1;}
    let capacity=rpc.account_stream().0.capacity();
    let report=serde_json::json!({"source":"persisted_prepared_proofs","live_admission_established":false,
        "pool_limit":limit,"prepared_candidates":candidates.len(),"selected_pools":selected.len(),
        "planned_pools":engine.pools.len(),"failed_pools":failed.iter().map(|(p,_)|p.address.to_string()).collect::<Vec<_>>(),
        "failures":failures,"pools_by_venue":venues,"account_references":engine.subs.len(),
        "unique_accounts":wanted.len(),"account_capacity":capacity,"capacity_sufficient":wanted.len()<=capacity,
        "preparation_seconds":started.elapsed().as_secs_f64(),
        "pools":selected.iter().map(|p|p.address.to_string()).collect::<Vec<_>>()});
    let path=env::var("MARKET_STATE_ACCOUNT_PLAN_PATH").context("MARKET_STATE_ACCOUNT_PLAN_PATH is required")?;
    std::fs::write(path,serde_json::to_vec_pretty(&report)?).context("write market account plan")?;
    ensure!(failed.is_empty(),"account plan failed for {} of {} pools; inspect preparation.json",failed.len(),selected.len());
    ensure!(wanted.len()<=capacity,"{} pools require {} distinct accounts; ENV WebSocket capacity is {}; no subscriptions were opened",selected.len(),wanted.len(),capacity);
    crate::log_event!(Info,"market account plan complete=true pools={} accounts={} seconds={:.3}",engine.pools.len(),wanted.len(),started.elapsed().as_secs_f64());
    Ok(wanted)
}
fn complete(engine:&Engine,pi:usize,active:&HashMap<Pubkey,String>)->bool {
    engine.pool_subs[pi].iter().all(|&i|active.contains_key(&engine.subs[i].key))
}
// The source session owns continuity. Invalidate only pools using affected accounts.
fn invalidate_accounts(engine:&mut Engine,keys:&HashSet<Pubkey>,versions:&mut HashMap<Pubkey,u64>,revision:&mut u64,reason:&str) {
    let pools:HashSet<_>=keys.iter().filter_map(|key|engine.account_ranges.get(key))
        .flat_map(|range|range.clone().map(|i|engine.subs[engine.account_bindings[i]].pool)).collect();
    for pi in pools {
        let p=&mut engine.pools[pi];p.ready=false;p.available=false;
        *revision+=1;versions.insert(p.key,*revision);
        engine.publish_unavailable(pi,engine.pools[pi].slot,timestamp_us(),reason.into());
    }
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
                let data=if self.subs[index].field==Field::ManifestBook {
                    ensure!(account.view==AccountView::Full,"orderbook requires full account");account.bytes.as_slice()
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
                Ok((owner,data))=>self.update(index,*slot,owner,data,*frame_received_at_us,pipeline),Err(e)=>Err(e),
            };
            if let Err(error)=updated {
                self.metrics.venues[self.pools[pi].venue as usize].decode_errors+=1;
                self.subs[index].valid=false;self.pools[pi].available=false;
                self.publish_unavailable(pi,*slot,*frame_received_at_us,format!("account update: {error:#}"));
            }
        }
        if !replay{self.metrics.processing.add(nanos(started));}
        initializing
    }
}

/// Registry identities select observations, never establish market prices.
/// Exactly one bounded preparation job may run while live events continue on this task.
async fn produce(registry:PoolRegistry,mut input:broadcast::Receiver<Arc<PoolUpdateBatch>>,
    rpc:RpcHandle,control:AccountStreamControl,mut events:AccountEventStream,out:broadcast::Sender<Arc<MarketStateEvent>>,
    mut stop:watch::Receiver<bool>,mut enabled:watch::Receiver<bool>,mut limit:watch::Receiver<usize>,persisted:bool)->Result<serde_json::Value> {
    ensure!(control.capacity()>0,"market state requires enabled RPC_N_WS_ACCOUNT_SUBSCRIBE providers");
    let snapshot=|registry:&PoolRegistry|->HashMap<Pubkey,PoolInput>{registry.snapshot().iter()
        .filter_map(|p|if persisted{PoolInput::prepared(p)}else{PoolInput::of(p)}).map(|p|(p.address,p)).collect()};
    let mut candidates=snapshot(&registry);
    let mut selected=Vec::<PoolInput>::new();let mut current=HashMap::<Pubkey,PoolInput>::new();
    let mut engine=Engine::empty(out.clone(),0);let mut wanted=Vec::<AccountWatch>::new();
    let mut active=HashMap::<Pubkey,String>::new();let mut versions=HashMap::<Pubkey,u64>::new();let mut revision=0;
    let mut retry=HashMap::<Pubkey,time::Instant>::new();
    let mut cached=HashMap::<Pubkey,(WsEvent,u64)>::new();
    let mut jobs=tokio::task::JoinSet::new();
    let mut rebuild=true;let mut replan=false;let mut schedule=true;let mut input_open=true;let mut ticks=0;
    let mut tick=time::interval(Duration::from_secs(1));tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    loop {
        if *stop.borrow(){break;}
        if rebuild {
            let next=if *enabled.borrow(){select(&candidates,*limit.borrow())}else{Vec::new()};
            if next!=selected {
                let next_map:HashMap<_,_>=next.iter().map(|p|(p.address,*p)).collect();
                let keep:HashSet<_>=selected.iter().filter(|p|next_map.get(&p.address)==Some(*p)).map(|p|p.address).collect();
                for p in &selected {if !keep.contains(&p.address){engine.sequence+=1;send(&out,MarketStateEvent::Removed{pool:p.address.to_string(),sequence:engine.sequence});}}
                engine.retain(&keep);cached.retain(|key,_|engine.account_ranges.contains_key(key));
                versions.retain(|key,_|keep.contains(key));retry.retain(|key,_|keep.contains(key));
                for p in &next {if !keep.contains(&p.address){revision+=1;versions.insert(p.address,revision);}}
                selected=next;current=next_map;replan=true;engine.metrics.selection_changes+=1;
                engine.sequence+=1;send(&out,MarketStateEvent::Selection{prepared:candidates.len(),
                    selected:selected.iter().map(|p|p.address.to_string()).collect(),accounts:0,sequence:engine.sequence});
                crate::log_event!(Info,"market state selection prepared={} selected={} retained={} initialization=incremental",candidates.len(),selected.len(),keep.len());
            }
            rebuild=false;
        }
        if replan {
            let started=Instant::now();
            let failed:Vec<_>=retry.keys().copied().filter(|key|!engine.pools.iter().any(|p|p.key==*key)).collect();
            let next=watches(&engine,&failed)?;
            ensure!(next.len()<=control.capacity(),"market state selected {} pools require {} accounts; configured WS capacity={}",selected.len(),next.len(),control.capacity());
            if next!=wanted {
                let old:HashMap<_,_>=wanted.iter().map(|w|(w.address,w.view)).collect();
                let changed:HashSet<_>=next.iter().filter(|w|old.get(&w.address)!=Some(&w.view)).map(|w|w.address).collect();
                invalidate_accounts(&mut engine,&changed,&mut versions,&mut revision,"account watch changed; awaiting baseline");
                let desired:HashSet<_>=next.iter().map(|w|w.address).collect();
                active.retain(|key,_|desired.contains(key)&&!changed.contains(key));
                control.replace(next.clone())?;wanted=next;
            }
            engine.metrics.watch_plan.add(nanos(started));replan=false;schedule=true;
        }
        if schedule && jobs.is_empty() {
            schedule=false;
            let now=time::Instant::now();
            let present:HashMap<_,_>=engine.pools.iter().enumerate().map(|(i,p)|(p.key,i)).collect();
            let mut targets:Vec<_>=selected.iter().filter(|p|present.get(&p.address).is_some_and(|&pi|
                !engine.pools[pi].ready && complete(&engine,pi,&active)) && retry.get(&p.address).is_none_or(|at|*at<=now)).copied().collect();
            let baseline=!targets.is_empty();
            if !baseline {targets=selected.iter().filter(|p|!present.contains_key(&p.address)&&retry.get(&p.address).is_none_or(|at|*at<=now)).copied().collect();}
            targets.truncate(64);
            if !targets.is_empty() {
                let tickets:Vec<_>=targets.iter().map(|p|(*p,versions[&p.address])).collect();
                let rpc=rpc.clone();let output=out.clone();engine.metrics.hydration_jobs+=1;
                jobs.spawn(async move {let result=hydrate(&targets,&rpc,&output,baseline).await;(tickets,baseline,result)});
            }
        }
        tokio::select! {
            changed=stop.changed()=>{if changed.is_err()||*stop.borrow(){break;}},
            changed=enabled.changed()=>{if changed.is_err(){break;}rebuild=true;},
            changed=limit.changed()=>{if changed.is_err(){break;}rebuild=true;},
            update=input.recv(),if input_open=>match update {
                Ok(batch)=>{for p in &batch.pools {
                    if persisted {
                        let next=registry.get(p.address).as_ref().and_then(PoolInput::prepared);
                        if candidates.get(&p.address).copied()!=next {if let Some(p)=next{candidates.insert(p.address,p);}else{candidates.remove(&p.address);}rebuild=true;}
                    }else{rebuild|=reconcile(&registry,&mut candidates,p.address);}
                }},
                Err(broadcast::error::RecvError::Lagged(_))=>{candidates=snapshot(&registry);rebuild=true;},
                Err(broadcast::error::RecvError::Closed)=>input_open=false,
            },
            joined=jobs.join_next(),if !jobs.is_empty()=>{
                let started=Instant::now();
                let (tickets,baseline,result)=joined.context("market preparation task disappeared")?.context("market preparation panicked")?;
                let (mut part,failures)=result?;schedule=true;
                let valid:HashSet<_>=tickets.iter().filter(|(p,version)|current.get(&p.address)==Some(p)&&versions.get(&p.address)==Some(version))
                    .map(|(p,_)|p.address).collect();
                engine.metrics.discarded_results+=(part.pools.len()-part.pools.iter().filter(|p|valid.contains(&p.key)).count()) as u64;
                part.retain(&valid);
                for (p,reason) in failures {if valid.contains(&p.address) {
                    retry.insert(p.address,time::Instant::now()+Duration::from_secs(30));engine.metrics.hydration_failures+=1;
                    engine.sequence+=1;send(&out,MarketStateEvent::Unavailable{pool:p.address.to_string(),venue:Venue::from_core(p.venue).map_or("unsupported",Venue::label),slot:0,received_at_us:timestamp_us(),sequence:engine.sequence,reason});
                }}
                let keys:HashSet<_>=part.pools.iter().map(|p|p.key).collect();
                for p in &mut part.pools {p.ready=baseline;retry.remove(&p.key);}
                engine.install(part);replan=true;
                if baseline {
                    for pi in 0..engine.pools.len() {if keys.contains(&engine.pools[pi].key) {
                        if engine.pools[pi].available {engine.publish_price(pi,PriceOrigin::Baseline,None,None,None);}
                        else{engine.publish_unavailable(pi,engine.pools[pi].slot,timestamp_us(),"baseline has no valid indicative price".into());}
                    }}
                    let replay_keys:HashSet<_>=engine.subs.iter().filter(|sub|keys.contains(&engine.pools[sub.pool].key)).map(|sub|sub.key).collect();
                    for key in replay_keys {if let Some((event,received))=cached.remove(&key) {
                        if engine.account(&event,&active,received,true,Some(&keys)){cached.insert(key,(event,received));}
                    }}
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
                    Ok(WsEvent::Subscribed{address,provider})=>{active.insert(address,provider);schedule=true;},
                    Ok(WsEvent::Unsubscribed{address,provider})=>{
                        if active.get(&address)==Some(&provider){active.remove(&address);invalidate_accounts(&mut engine,&HashSet::from([address]),&mut versions,&mut revision,"account unsubscribed");}
                    },
                    Ok(WsEvent::Gap{provider,reason})=>{
                        engine.metrics.gaps+=1;
                        let keys:HashSet<_>=active.iter().filter(|(_,source)|source.as_str()==provider.as_str()).map(|(key,_)|*key).collect();
                        active.retain(|_,source|source.as_str()!=provider.as_str());
                        invalidate_accounts(&mut engine,&keys,&mut versions,&mut revision,reason);
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
                        engine.sequence+=1;send(&out,MarketStateEvent::Gap{reason:format!("account receiver lagged {n}"),sequence:engine.sequence});
                    },
                    Err(broadcast::error::RecvError::Closed)=>bail!("market account transport closed"),
                }
            },
            _=tick.tick()=>{
                ticks+=1;
                if !retry.is_empty(){schedule=true;}
                if ticks%5==0 {
                    for p in &selected {if !persisted{rebuild|=reconcile(&registry,&mut candidates,p.address);}}
                    let (ws_messages,ws_text_bytes)=control.stream_counts();engine.sequence+=1;
                    send(&out,MarketStateEvent::Stats{pools:selected.len(),ready:engine.pools.iter().filter(|p|p.ready).count(),
                        available:engine.pools.iter().filter(|p|p.available).count(),accounts:wanted.len(),active:active.len(),ws_messages,ws_text_bytes,sequence:engine.sequence});
                }
            }
        }
    }
    // JoinSet drops/cancels the one preparation job. No detached HTTP initializer.
    drop(jobs);
    let report=engine.metrics.report();
    for p in &selected {engine.sequence+=1;send(&out,MarketStateEvent::Removed{pool:p.address.to_string(),sequence:engine.sequence});}
    crate::log_event!(Info,"market state stopped=true");
    Ok(report)
}

/// Real typed consumer. Maintains price coverage against the same graph catalog generation.
#[derive(Debug,serde::Serialize)]
pub struct ObservationReport {
    pub observations:u64,pub websocket_observations:u64,pub websocket_pools:usize,
    pub max_priced_direct_pairs:usize,pub max_live_priced_direct_pairs:usize,pub selections:u64,pub removals:u64,pub gaps:u64,
    pub max_active_accounts:usize,pub unavailable_pools:usize,
    pub post_rewatch_websocket_observations:u64,pub websocket_venues:BTreeMap<&'static str,u64>,
}
impl ObservationReport {
    pub fn ensure_live_evidence(&self,rewatch:bool)->Result<()> {
        ensure!(self.observations>0,"no real pool price observations");
        ensure!(self.websocket_observations>0,"HTTP baseline alone is insufficient: no real WebSocket price updates");
        ensure!(self.max_live_priced_direct_pairs>0,"no graph Direct2 pair covered by two prices including a real WebSocket observation; inspect selection and decoder availability");
        if rewatch {
            ensure!(self.selections>=3 && self.removals>0 && self.post_rewatch_websocket_observations>0,
                "rewatch did not complete removal, reselection and subsequent real WebSocket price delivery");
        }
        Ok(())
    }
}

/// Cancels hydration as well as idle streaming, then releases the sole desired watch set.
pub async fn run(registry:PoolRegistry,input:broadcast::Receiver<Arc<PoolUpdateBatch>>,
    rpc:RpcHandle,control:AccountStreamControl,events:AccountEventStream,out:broadcast::Sender<Arc<MarketStateEvent>>,settings:Settings,
    stop:watch::Receiver<bool>,enabled:watch::Receiver<bool>)->Result<()> {
    let (_limit_tx,limit)=watch::channel(settings.pool_limit);
    let result=produce(registry,input,rpc,control.clone(),events,out,stop,enabled,limit,false).await;
    let cleanup=control.replace(Vec::new());
    crate::log_event!(Info,"market state released_watches=true");
    result.map(|_|()).and(cleanup)
}

pub async fn observe(mut input:PriceEventStream,mut catalog:watch::Receiver<Arc<WatchCatalog>>,trace:bool)->Result<ObservationReport> {
    let mut prices=HashMap::<String,PriceOrigin>::new();
    let mut first_ws=HashSet::new();let mut observed=0_u64;let mut websocket=0_u64;
    let mut pairs_max=0_usize;let mut live_pairs_max=0_usize;let mut catalog_open=true;
    let mut selections=0;let mut removals=0;let mut gaps=0;let mut max_active=0;
    let mut unavailable=HashSet::new();let mut websocket_venues=BTreeMap::new();
    let mut cleared=false;let mut rewatching=false;let mut post_rewatch=0;
    loop {tokio::select! {
        changed=catalog.changed(),if catalog_open=>{if changed.is_err(){catalog_open=false;}},
        event=input.recv()=>match event {
            Ok(event)=>{
                match event.as_ref() {
                    MarketStateEvent::Price(p)=>{
                        observed+=1;
                        if matches!(p.origin,PriceOrigin::Websocket){
                            websocket+=1;*websocket_venues.entry(p.venue).or_insert(0)+=1;
                            if rewatching {post_rewatch+=1;}
                        }
                        let first=prices.insert(p.pool.clone(),p.origin).is_none();
                        let first_live=matches!(p.origin,PriceOrigin::Websocket) && first_ws.insert(p.pool.clone());
                        if trace || (first && observed<=16) || (first_live && first_ws.len()<=16) {println!("{}",serde_json::to_string(event.as_ref())?);}
                    },
                    MarketStateEvent::Unavailable{pool,..}=>{
                        prices.remove(pool);
                        let first=unavailable.insert(pool.clone());
                        if trace || (first && unavailable.len()<=16){println!("{}",serde_json::to_string(event.as_ref())?);}
                    },
                    MarketStateEvent::Removed{pool,..}=>{removals+=1;prices.remove(pool);
                        if trace || removals<=16 {println!("{}",serde_json::to_string(event.as_ref())?);}},
                    MarketStateEvent::Gap{..}=>{gaps+=1;prices.clear();println!("{}",serde_json::to_string(event.as_ref())?);},
                    MarketStateEvent::Selection{selected,..}=>{selections+=1;
                        if selected.is_empty(){cleared=true;}else if cleared{rewatching=true;}
                        let current:HashSet<_>=selected.iter().cloned().collect();prices.retain(|p,_|current.contains(p));
                        println!("{}",serde_json::to_string(event.as_ref())?);
                    },
                    MarketStateEvent::Stats{active,..}=>{
                        max_active=max_active.max(*active);
                        let graph=catalog.borrow().clone();
                        let covered:Vec<_>=graph.pairs.iter().filter(|pair|pair.contexts.iter().any(|c|c.kind==RouteKind::Direct2)
                            && pair.pools.iter().filter(|p|prices.contains_key(&p.to_string())).take(2).count()==2).collect();
                        let direct_pairs=covered.len();
                        let live_pairs=covered.iter().filter(|pair|pair.pools.iter()
                            .any(|p|matches!(prices.get(&p.to_string()),Some(PriceOrigin::Websocket)))).count();
                        pairs_max=pairs_max.max(direct_pairs);live_pairs_max=live_pairs_max.max(live_pairs);
                        crate::log_event!(Info,"market state consumer prices={} observations={observed} websocket={websocket} graph_generation={} priced_pairs={direct_pairs} live_priced_pairs={live_pairs}",prices.len(),graph.generation);
                        println!("{}",serde_json::to_string(event.as_ref())?);
                    }
                }
            },
            Err(broadcast::error::RecvError::Lagged(n))=>{
                prices.clear();bail!("market price consumer lagged {n}; current coverage invalid");
            },
            Err(broadcast::error::RecvError::Closed)=>break,
        }
    }}
    crate::log_event!(Info,"market state consumer complete=true observations={observed} websocket={websocket} max_priced_pairs={pairs_max}");
    Ok(ObservationReport{observations:observed,websocket_observations:websocket,websocket_pools:first_ws.len(),
        max_priced_direct_pairs:pairs_max,max_live_priced_direct_pairs:live_pairs_max,selections,removals,gaps,max_active_accounts:max_active,unavailable_pools:unavailable.len(),
        post_rewatch_websocket_observations:post_rewatch,websocket_venues})
}
