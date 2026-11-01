//! Raydium CLMM protocol-owned decoding and marginal fee valuation.
//! Source: raydium-io/raydium-clmm 6d7a0f82d2f293c553c69aaf943377367c6c4cc6.
//! No RPC, persistent history or candidate/execution policy lives here.
use anyhow::{ensure,Result,Context};
use solana_pubkey::Pubkey;
const Q64:f64=18_446_744_073_709_551_616.0;
const DEN:f64=1_000_000.0;
const POOL_DISC:[u8;8]=[247,237,227,245,215,195,222,70];
const CONFIG_DISC:[u8;8]=[218,244,33,104,203,203,43,111];
const ARRAY_DISC:[u8;8]=[192,155,85,205,49,249,129,42];

fn raw<const N:usize>(d:&[u8],n:usize)->Result<[u8;N]>{
    Ok(d.get(n..n+N).context("CLMM account field truncated")?.try_into()?)
}
fn u16_at(d:&[u8],n:usize)->Result<u16>{Ok(u16::from_le_bytes(raw(d,n)?))}
fn u32_at(d:&[u8],n:usize)->Result<u32>{Ok(u32::from_le_bytes(raw(d,n)?))}
fn u64_at(d:&[u8],n:usize)->Result<u64>{Ok(u64::from_le_bytes(raw(d,n)?))}
fn u128_at(d:&[u8],n:usize)->Result<u128>{Ok(u128::from_le_bytes(raw(d,n)?))}
fn i32_at(d:&[u8],n:usize)->Result<i32>{Ok(i32::from_le_bytes(raw(d,n)?))}
fn i128_at(d:&[u8],n:usize)->Result<i128>{Ok(i128::from_le_bytes(raw(d,n)?))}
fn key(d:&[u8],n:usize)->Result<Pubkey>{Ok(Pubkey::new_from_array(raw(d,n)?))}

#[derive(Clone,Copy,Debug)]
pub(super) struct Dynamic{
    filter:u16,decay:u16,reduction:u16,control:u32,max:u32,
    reference_index:i32,reference:u32,accumulator:u32,last_update:u64,
}
#[derive(Clone,Copy,Debug)]
pub(super) struct Pool{
    pub sqrt:u128,pub liquidity:u128,pub tick:i32,pub spacing:u16,
    pub status:u8,pub fee_on:u8,pub config:Pubkey,pub dynamic:Option<Dynamic>,
}
pub(super) fn pool(d:&[u8])->Result<Pool>{
    ensure!(d.len()==1544&&d.starts_with(&POOL_DISC),"invalid Raydium CLMM PoolState");
    let dynamic=if d[1096..1176].iter().all(|x|*x==0){None}else{
        let v=Dynamic{filter:u16_at(d,1096)?,decay:u16_at(d,1098)?,
            reduction:u16_at(d,1100)?,control:u32_at(d,1102)?,
            max:u32_at(d,1106)?,reference_index:i32_at(d,1110)?,
            reference:u32_at(d,1114)?,accumulator:u32_at(d,1118)?,
            last_update:u64_at(d,1122)?};
        ensure!(v.filter>0&&v.decay>v.filter&&v.reduction>0&&v.reduction<10000
            &&v.control>0&&v.control<100000
            &&u64::from(v.max)*u64::from(u16_at(d,235)?)<=u64::from(u32::MAX),
            "invalid Raydium dynamic fee parameters");
        Some(v)
    };
    let p=Pool{sqrt:u128_at(d,253)?,liquidity:u128_at(d,237)?,
        tick:i32_at(d,269)?,spacing:u16_at(d,235)?,
        status:d[389],fee_on:d[390],config:key(d,9)?,dynamic};
    ensure!(p.spacing>0&&p.spacing<=1000&&(-443636..=443636).contains(&p.tick)
        &&p.sqrt>0&&p.fee_on<=2&&p.status&16==0,
        "invalid Raydium CLMM price/tick/fee-on or swaps disabled");
    Ok(p)
}
#[derive(Clone,Copy,Debug)]
pub(super) struct Config{pub trade:u32,pub protocol:u32,pub fund:u32,pub spacing:u16}
pub(super) fn config(d:&[u8])->Result<Config>{
    ensure!(d.len()==117&&d.starts_with(&CONFIG_DISC),"invalid Raydium CLMM AmmConfig");
    let c=Config{trade:u32_at(d,47)?,protocol:u32_at(d,43)?,
        fund:u32_at(d,53)?,spacing:u16_at(d,51)?};
    ensure!(c.trade<100000&&c.protocol<=1000000&&c.fund<=1000000
        &&u64::from(c.protocol)+u64::from(c.fund)<=1000000&&c.spacing>0,
        "invalid CLMM AmmConfig fee rates or tick spacing");
    Ok(c)
}
/// The pool fee is the **total** trader charge. Protocol/fund fees are a split
/// of this trade fee, not additional charges.
pub(super) fn fee(p:Pool,c:Config,chain_time:i64)->Result<u32>{
    ensure!(p.spacing==c.spacing,"Raydium CLMM PoolState/AmmConfig spacing mismatch");
    let Some(v)=p.dynamic else{return Ok(c.trade);};
    ensure!(chain_time>0,"Raydium dynamic fee awaits chain Clock");
    let t=u64::try_from(chain_time)?;
    ensure!(t>=v.last_update,"Raydium dynamic fee Clock predates pool reference");
    let idx=p.tick.div_euclid(i32::from(p.spacing));
    let elapsed=t-v.last_update;
    let (reference,reference_idx)=if elapsed<u64::from(v.filter){
        (v.reference,v.reference_index)
    }else if elapsed<u64::from(v.decay){
        ((u64::from(v.accumulator)*u64::from(v.reduction)/10000) as u32,idx)
    }else{(0,idx)};
    let delta=u64::from(reference_idx.abs_diff(idx))*10000;
    let accumulator=(u64::from(reference)+delta).min(u64::from(v.max));
    let cross=accumulator*u64::from(p.spacing);
    let numerator=u128::from(v.control)*u128::from(cross)*u128::from(cross);
    let dynamic=(numerator.div_ceil(100000_u128*10000_u128*10000_u128)).min(100000);
    Ok((u128::from(c.trade)+dynamic).min(100000) as u32)
}
pub(super) fn fee_on_input(mode:u8,zero_for_one:bool)->Result<bool>{
    ensure!(mode<=2,"invalid Raydium CLMM fee_on");
    Ok(match mode{0=>true,1=>zero_for_one,2=>!zero_for_one,_=>unreachable!()})
}
pub(super) fn rates(p:Pool,fee:u32,da:u8,db:u8)->Result<[f64;2]>{
    let sqrt=p.sqrt as f64/Q64;
    let forward=sqrt*sqrt*10_f64.powi(i32::from(da)-i32::from(db));
    let factor=1.0-f64::from(fee)/DEN;
    ensure!(forward.is_finite()&&forward>0.0&&factor>0.0,
        "invalid CLMM marginal exchange rate");
    Ok([forward*factor,1.0/forward*factor])
}
#[derive(Clone,Debug)]
pub(super) struct Tick{pub index:i32,pub liquidity_net:i128,pub has_orders:bool}
#[derive(Clone,Debug)]
pub(super) struct TickArray{pub start:i32,pub ticks:Vec<Tick>}
pub(super) fn tick_array(d:&[u8],pool:Pubkey,spacing:u16)->Result<TickArray>{
    ensure!(d.len()==10240&&d.starts_with(&ARRAY_DISC)
        &&key(d,8)?==pool,"invalid Raydium TickArray owner/pool/length");
    let start=i32_at(d,40)?;
    ensure!(start%(i32::from(spacing)*60)==0,"invalid CLMM TickArray start");
    let mut ticks=Vec::new();
    for i in 0..60{
        let off=44+i*168;
        let idx=i32_at(d,off)?;
        let gross=u128_at(d,off+20)?;
        let orders=u64_at(d,off+124)?;
        let partial=u64_at(d,off+132)?;
        if gross>0||orders>0||partial>0{
            ensure!(idx==start+i32::try_from(i)?*i32::from(spacing),
                "invalid CLMM initialized TickState index");
            ticks.push(Tick{index:idx,liquidity_net:i128_at(d,off+4)?,
                has_orders:orders>0||partial>0});
        }
    }
    ensure!(d[44+60*168] as usize==ticks.len(),
        "CLMM TickArray initialized count mismatch");
    Ok(TickArray{start,ticks})
}
/// Conservative within-observed-interval capacity; never an exhaustive proof
/// of dust, never a multi-tick quote. Returns None if the next interval cannot
/// be anchored to an observed initialized tick (or limit order boundary).
pub(super) fn active_window(p:Pool,arrays:&[TickArray],fee_rate:u32,down:bool)
    ->Option<(u64,u64)>{
    if p.liquidity==0||arrays.is_empty(){return None;}
    let nearest=arrays.iter().flat_map(|a|a.ticks.iter())
        .filter(|t|if down{t.index<=p.tick}else{t.index>p.tick})
        .min_by_key(|t|if down{i64::from(p.tick)-i64::from(t.index)}
            else{i64::from(t.index)-i64::from(p.tick)});
    // When there is no initialized tick ahead in the prepared arrays, bound
    // depth to the *observed array's* last accessible tick, never beyond it.
    let covered=arrays.iter().filter_map(|a|{
        let end=a.start+59*i32::from(p.spacing);
        if a.start<=p.tick && p.tick<=end {
            Some(if down{a.start}else{end})
        }else{None}
    }).next()?;
    let mut target_tick=nearest.map_or(covered,|t|if down{t.index.max(covered)}else{t.index.min(covered)});
    if nearest.is_some_and(|t|t.index==target_tick && t.has_orders){return None;}
    if p.dynamic.is_some(){
        let spacing=i32::from(p.spacing);
        let fee_boundary=if down{p.tick.div_euclid(spacing)*spacing}
            else{(p.tick.div_euclid(spacing)+1)*spacing};
        target_tick=if down{target_tick.max(fee_boundary)}
            else{target_tick.min(fee_boundary)};
    }
    let s=p.sqrt as f64/Q64;
    let target=1.0001_f64.powf(f64::from(target_tick)/2.0);
    let l=p.liquidity as f64;
    let (inp,out)=if down{
        (l*(1.0/target-1.0/s),l*(s-target))
    }else{(l*(target-s),l*(1.0/s-1.0/target))};
    let factor=1.0-f64::from(fee_rate)/DEN;
    let gross=inp/factor;
    // A strict haircut keeps a float-based capacity estimate away from the
    // actual tick boundary; failure to cover the notional remains Unknown.
    let (input,output)=(gross*0.98,out*0.98);
    if !input.is_finite()||!output.is_finite()||input<=1.0||output<=1.0
        ||input>u64::MAX as f64||output>u64::MAX as f64{return None;}
    Some((input.floor() as u64,output.floor() as u64))
}
/// Bounded diagnostic prediction at the amount chosen by V10.
/// The canonical reference script uses exact integers. This fast model is
/// deliberately advisory; tick/fee-group/limit-order crossings return Unknown.
pub(crate) fn quote(s:&crate::core::market_state::ClmmQuoteSnapshot,
    input:&str,output:&str,amount:u64)->std::result::Result<u64,&'static str>{
    let native=if input==s.mint_0&&output==s.mint_1{0}
        else if input==s.mint_1&&output==s.mint_0{1}
        else{return Err("clmm_direction_mismatch")};
    if amount==0{return Err("clmm_empty_or_dust");}
    if s.active_input_limit_raw[native]==0||amount>s.active_input_limit_raw[native]{
        return Err("clmm_outside_observed_active_interval");
    }
    if s.fee_rate>=1_000_000||s.active_liquidity==0||s.sqrt_price_x64==0{
        return Err("clmm_invalid_frozen_state");
    }
    let fee_on_input=fee_on_input(s.fee_on,native==0).map_err(|_|"clmm_invalid_fee_on")?;
    let gross=amount as f64;
    let fee=s.fee_rate as f64/DEN;
    let net_input=if fee_on_input{(gross*(1.0-fee)).floor()}else{gross};
    let (liquidity,sqrt)=(s.active_liquidity as f64,s.sqrt_price_x64 as f64/Q64);
    if net_input<=0.0||sqrt<=0.0||liquidity<=0.0{return Err("clmm_empty_or_dust");}
    let next=if native==0{liquidity*sqrt/(liquidity+net_input*sqrt)}
        else{sqrt+net_input/liquidity};
    let gross_out=if native==0{liquidity*(sqrt-next)}
        else{liquidity*(1.0/sqrt-1.0/next)};
    if !gross_out.is_finite()||gross_out<1.0||gross_out>u64::MAX as f64{
        return Err("clmm_amount_unrepresentable");
    }
    let raw=gross_out.floor() as u64;
    let net=if fee_on_input{raw}else{
        let fee_raw=(u128::from(raw)*u128::from(s.fee_rate)).div_ceil(1_000_000);
        raw.checked_sub(u64::try_from(fee_raw).map_err(|_|"clmm_fee_overflow")?)
            .ok_or("clmm_output_fee_exceeds_output")?
    };
    if net==0{Err("clmm_empty_or_dust")}else{Ok(net)}
}
#[cfg(test)]
mod tests{
    use super::*;
    fn p()->Pool{Pool{sqrt:1u128<<64,liquidity:100_000_000,tick:0,spacing:10,
        status:0,fee_on:0,config:Pubkey::default(),dynamic:None}}
    #[test]fn cp15_static_fee_and_input_output_side(){
        let c=Config{trade:3000,protocol:100000,fund:100000,spacing:10};
        assert_eq!(fee(p(),c,0).unwrap(),3000);
        assert!(fee_on_input(0,true).unwrap()&&fee_on_input(0,false).unwrap());
        assert!(fee_on_input(1,true).unwrap()&&!fee_on_input(1,false).unwrap());
        assert!(!fee_on_input(2,true).unwrap()&&fee_on_input(2,false).unwrap());
        assert!((rates(p(),3000,6,6).unwrap()[0]-0.997).abs()<1e-12);
    }
    #[test]fn cp15_dynamic_fee_clock_and_reset(){
        let mut x=p();
        x.dynamic=Some(Dynamic{filter:10,decay:100,reduction:5000,control:1000,max:50000,
            reference_index:0,reference:10000,accumulator:20000,last_update:1000});
        let c=Config{trade:3000,protocol:0,fund:0,spacing:10};
        assert!(fee(x,c,0).is_err());
        assert!(fee(x,c,999).is_err());
        assert!(fee(x,c,1002).unwrap()>3000);
        assert!(fee(x,c,1100).unwrap()==3000);
    }
    #[test]fn cp15_negative_tick_and_limit_order_boundary_unknown(){
        let mut x=p();x.tick=-5;x.sqrt=(Q64*1.0001_f64.powf(-5.0/2.0)) as u128;
        let array=TickArray{start:-600,ticks:vec![Tick{index:-10,liquidity_net:100,has_orders:true}]};
        assert!(active_window(x,&[array.clone()],3000,true).is_none());
        let mut clean=array;clean.ticks[0].has_orders=false;
        assert!(active_window(x,&[clean],3000,true).is_some());
    }
}