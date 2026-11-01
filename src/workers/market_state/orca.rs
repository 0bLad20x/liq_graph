//! Orca Whirlpool protocol-local directed marginal valuation and bounded
//! active-liquidity witness. No routing, RPC, V10 amount search or global policy.
//! Layouts and adaptive fee rules correspond to orca-so/whirlpools
//! programs/whirlpool/src/state/{whirlpool,oracle}.rs and manager/fee_rate_manager.rs.
use anyhow::{ensure,Result,Context};
use crate::core::market_state::OrcaQuoteSnapshot;
const Q64:f64=18_446_744_073_709_551_616.0;
const DEN:f64=1_000_000.0;
const WHIRLPOOL_DISC:[u8;8]=[63,149,209,12,225,128,99,9];
const ORACLE_DISC:[u8;8]=[139,194,131,179,140,179,229,244];

fn chunk<const N:usize>(data:&[u8],off:usize)->Result<[u8;N]>{
    Ok(data.get(off..off+N).context("truncated Orca account")?.try_into()?)
}
fn u16_at(d:&[u8],n:usize)->Result<u16>{Ok(u16::from_le_bytes(chunk(d,n)?))}
fn u32_at(d:&[u8],n:usize)->Result<u32>{Ok(u32::from_le_bytes(chunk(d,n)?))}
fn u64_at(d:&[u8],n:usize)->Result<u64>{Ok(u64::from_le_bytes(chunk(d,n)?))}
fn i32_at(d:&[u8],n:usize)->Result<i32>{Ok(i32::from_le_bytes(chunk(d,n)?))}
fn u128_at(d:&[u8],n:usize)->Result<u128>{Ok(u128::from_le_bytes(chunk(d,n)?))}

#[derive(Clone,Copy,Debug)]
pub(super) struct Whirlpool {
    pub spacing:u16,pub fee:u16,pub protocol_share:u16,
    pub liquidity:u128,pub sqrt:u128,pub tick:i32,
}
pub(super) fn pool(d:&[u8])->Result<Whirlpool>{
    ensure!(d.len()==653&&d.starts_with(&WHIRLPOOL_DISC),"invalid Whirlpool account/discriminator");
    let p=Whirlpool{spacing:u16_at(d,41)?,fee:u16_at(d,45)?,
        protocol_share:u16_at(d,47)?,liquidity:u128_at(d,49)?,
        sqrt:u128_at(d,65)?,tick:i32_at(d,81)?};
    ensure!(p.spacing>0&&p.spacing<=32768&&p.fee<=65535&&p.protocol_share<=10000
        &&(-443636..=443636).contains(&p.tick)&&p.sqrt>0,"invalid Whirlpool state");
    Ok(p)
}
#[derive(Clone,Copy,Debug)]
pub(super) struct Oracle {
    pub trade_enable:u64,pub filter:u16,pub decay:u16,pub reduction:u16,
    pub control:u32,pub max_volatility:u32,pub tick_group:u16,
    pub last_reference:u64,pub last_major:u64,pub volatility_reference:u32,
    pub reference_group:i32,pub accumulator:u32,
}
pub(super) fn oracle(d:&[u8],expected_pool:&[u8;32])->Result<Oracle>{
    ensure!(d.len()==254&&d.starts_with(&ORACLE_DISC)
        &&d.get(8..40)==Some(expected_pool.as_slice()),"invalid Orca Oracle ownership/layout");
    let o=Oracle{trade_enable:u64_at(d,40)?,filter:u16_at(d,48)?,
        decay:u16_at(d,50)?,reduction:u16_at(d,52)?,control:u32_at(d,54)?,
        max_volatility:u32_at(d,58)?,tick_group:u16_at(d,62)?,
        last_reference:u64_at(d,82)?,last_major:u64_at(d,90)?,
        volatility_reference:u32_at(d,98)?,reference_group:i32_at(d,102)?,
        accumulator:u32_at(d,106)?};
    ensure!(o.filter>0&&o.decay>o.filter&&o.reduction<10000&&o.control<100000
        &&o.tick_group>0&&o.max_volatility as u64*o.tick_group as u64<=u32::MAX as u64,
        "invalid adaptive fee constants");
    Ok(o)
}
pub(super) fn fee(pool:Whirlpool,oracle:Option<Oracle>,timestamp:i64)->Result<u32>{
    let Some(o)=oracle else {return Ok(u32::from(pool.fee));};
    ensure!(timestamp>0,"Orca chain timestamp unavailable");
    let t=u64::try_from(timestamp)?;
    ensure!(o.trade_enable<=t,"Orca trading time gate closed");
    let most_recent=o.last_reference.max(o.last_major);
    ensure!(t>=most_recent,"Orca Oracle from future slot/time");
    ensure!(o.tick_group<=pool.spacing && pool.spacing%o.tick_group==0,
        "Orca Oracle tick group invalid for Whirlpool spacing");
    let group=pool.tick.div_euclid(i32::from(o.tick_group));
    let (reference,ref_group)=if t.saturating_sub(o.last_reference)>3600 {
        (0,group)
    }else{
        let elapsed=t-most_recent;
        if elapsed<u64::from(o.filter){(o.volatility_reference,o.reference_group)}
        else if elapsed<u64::from(o.decay){
            ((u64::from(o.accumulator)*u64::from(o.reduction)/10000) as u32,group)
        }else{(0,group)}
    };
    let delta=u64::from(o.max_volatility).min(
        u64::from(reference).saturating_add(
            u64::from(ref_group.abs_diff(group))*10000));
    let crossed=u128::from(delta)*u128::from(o.tick_group);
    let squared=crossed.checked_mul(crossed).context("Orca volatility overflow")?;
    let charge=u128::from(o.control).checked_mul(squared)
        .context("Orca adaptive fee overflow")?.div_ceil(100_000_u128*100_000_000);
    Ok((u128::from(pool.fee)+charge).min(100000) as u32)
}
pub(super) fn rates(p:Whirlpool,applied_fee:u32,da:u8,db:u8)->Result<[f64;2]> {
    let sqrt=p.sqrt as f64/Q64;
    let price=sqrt*sqrt*10_f64.powi(i32::from(da)-i32::from(db));
    let effective=1.0-applied_fee as f64/DEN;
    ensure!(price.is_finite()&&price>0.0&&effective>0.0,"Orca invalid net price");
    Ok([price*effective,effective/price])
}
#[derive(Clone,Debug)]
pub(super) struct TickArray {
    pub start:i32,
    pub initialized:Vec<crate::core::market_state::OrcaQuoteTick>,
}
const FIXED_DISC:[u8;8]=[69,97,189,190,110,7,66,187];
const DYNAMIC_DISC:[u8;8]=[17,216,246,142,225,199,218,56];
pub(super) fn tick_array(d:&[u8],pool:&[u8;32],spacing:u16)->Result<TickArray>{
    ensure!(spacing>0&&d.len()>=60,"Orca TickArray short/spacing zero");
    let start=i32_at(d,8)?;
    ensure!(start%(i32::from(spacing)*88)==0,"Orca tick array start is not initializable");
    let mut ticks=Vec::new();
    if d.starts_with(&FIXED_DISC){
        ensure!(d.len()==8+4+88*113+32 &&
            d.get(8+4+88*113..)==Some(pool.as_slice()),"invalid Orca fixed TickArray");
        for i in 0..88{
            let off=12+i*113;
            let tag=d[off];
            ensure!(tag<=1,"invalid Orca fixed TickArray flag");
            if tag==1 {ticks.push(crate::core::market_state::OrcaQuoteTick{
                index:start+i as i32*i32::from(spacing),
                liquidity_net:i128::from_le_bytes(chunk(d,off+1)?)
            });}
        }
    }else if d.starts_with(&DYNAMIC_DISC){
        ensure!(d.len()>=8+4+32+16+88 &&
            d.get(12..44)==Some(pool.as_slice()),"invalid Orca dynamic TickArray");
        let mut off=60;
        for i in 0..88{
            let tag=*d.get(off).context("Orca dynamic TickArray truncated tag")?;
            ensure!(tag<=1,"invalid Orca dynamic tick flag");
            off+=1;
            if tag==1{
                ensure!(d.get(off..off+112).is_some(),"Orca dynamic tick payload truncated");
                ticks.push(crate::core::market_state::OrcaQuoteTick{
                    index:start+i as i32*i32::from(spacing),
                    liquidity_net:i128::from_le_bytes(chunk(d,off)?)
                });
                off+=112;
            }
        }
        ensure!(off==d.len() || d[off..].iter().all(|&v|v==0),
            "Orca dynamic TickArray trailing data invalid");
    }else{anyhow::bail!("Orca TickArray discriminator invalid")};
    Ok(TickArray{start,initialized:ticks})
}
pub(super) fn nearest_initialized(arrays:&[TickArray],tick:i32,direction:usize)->Option<i32>{
    arrays.iter().flat_map(|a|a.initialized.iter().map(|v|v.index))
        .filter(|&i|if direction==0{i<tick}else{i>tick})
        .min_by_key(|&i|i.abs_diff(tick))
}
/// Conservative active-position witness to the next initializable tick.
/// It never assumes liquidity beyond that boundary, where `liquidity_net` may
/// alter available capacity. A small witness is Unknown, not evidence of Dust.
pub(super) fn active_capacity(p:Whirlpool,applied_fee:u32,arrays:&[TickArray],oracle:Option<Oracle>)->Result<([u64;2],[u64;2])>{
    let now=p.sqrt as f64/Q64;
    ensure!(now>0.0&&now.is_finite()&&p.liquidity>0,"Orca no active liquidity");
    let width=i32::from(p.spacing);
    let mut lower=p.tick.div_euclid(width)*width;
    let mut upper=lower+width;
    if let Some(t)=nearest_initialized(arrays,p.tick,0){lower=lower.min(t);}
    if let Some(t)=nearest_initialized(arrays,p.tick,1){upper=upper.max(t);}
    // Adaptive fee may change on a tick-group boundary even without an
    // initialized liquidity tick. Keep the marginal fee witness inside that group.
    if let Some(o)=oracle{
        let g=i32::from(o.tick_group);
        lower=lower.max(p.tick.div_euclid(g)*g);
        upper=upper.min(p.tick.div_euclid(g)*g+g);
    }
    let low=1.0001_f64.powf(lower as f64/2.0);
    let high=1.0001_f64.powf(upper as f64/2.0);
    let l=p.liquidity as f64;
    // Restrict to one fee group when the adaptive rate can change inside a tick.
    let efficiency=1.0-applied_fee as f64/DEN;
    ensure!(low.is_finite()&&high.is_finite()&&low>0.0&&efficiency>0.0,"Orca invalid price boundary");
    let floored=|v:f64|->u64{
        if v.is_finite()&&v>=1.0{v.floor().min(u64::MAX as f64*0.999) as u64}else{0}
    };
    let down=(now-low).max(0.0);let up=(high-now).max(0.0);
    let input=[floored(l*down/(now*low)/efficiency*0.999),
        floored(l*up/efficiency*0.999)];
    let output=[floored(l*down*0.999),
        floored(l*up/(now*high)*0.999)];
    Ok((input,output))
}
/// Calibration-only **approximate** active-segment quote. No tick crossing, no
/// exact integer-swap promise. Unknown when V10's input crosses the witness.
pub(crate) fn quote(snapshot:&OrcaQuoteSnapshot,input:&str,output:&str,amount:u64)
    ->std::result::Result<u64,&'static str>{
    let direction=if input==snapshot.mint_a&&output==snapshot.mint_b{0}
        else if input==snapshot.mint_b&&output==snapshot.mint_a{1}
        else{return Err("orca_direction_mismatch")};
    if amount==0||amount>snapshot.active_input_limit_raw[direction]{
        return Err("orca_amount_outside_frozen_active_segment");
    }
    let p=snapshot.sqrt_price_x64 as f64/Q64;
    let l=snapshot.active_liquidity as f64;
    let effective=1.0-snapshot.fee_rate as f64/DEN;
    if p<=0.0||l<=0.0||effective<=0.0{return Err("orca_invalid_snapshot");}
    let after=amount as f64*effective;
    let out=if direction==0 {
        let next=l*p/(l+after*p);
        l*(p-next)
    }else{
        let next=p+after/l;
        l*(next-p)/(p*next)
    };
    if !out.is_finite()||out<0.0||out>u64::MAX as f64{return Err("orca_quote_range");}
    Ok(out.floor() as u64)
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]fn cp14_layout_price_fee_static(){
        let mut d=[0_u8;653];d[..8].copy_from_slice(&WHIRLPOOL_DISC);
        d[41..43].copy_from_slice(&64_u16.to_le_bytes());
        d[45..47].copy_from_slice(&3000_u16.to_le_bytes());
        d[49..65].copy_from_slice(&1_000_000_u128.to_le_bytes());
        d[65..81].copy_from_slice(&(1_u128<<64).to_le_bytes());
        let p=pool(&d).unwrap();assert_eq!(p.spacing,64);
        let f=fee(p,None,0).unwrap();assert_eq!(f,3000);
        let r=rates(p,f,6,6).unwrap();assert!((r[0]-0.997).abs()<1e-12);
        assert!((r[1]-0.997).abs()<1e-12);
        let (input,_)=active_capacity(p,f,&[],None).unwrap();assert!(input[1]>0);
    }
    #[test]fn cp14_fixed_and_dynamic_tick_array_decoding(){
        let pool=[42_u8;32];
        let mut fixed=vec![0_u8;8+4+88*113+32];
        fixed[..8].copy_from_slice(&FIXED_DISC);
        fixed[12+4*113]=1;
        let end=12+88*113;fixed[end..end+32].copy_from_slice(&pool);
        let a=tick_array(&fixed,&pool,64).unwrap();
        assert_eq!(a.initialized[0].index,4*64);
        let mut dynamic=vec![0_u8;60+88+112];
        dynamic[..8].copy_from_slice(&DYNAMIC_DISC);
        dynamic[12..44].copy_from_slice(&pool);
        dynamic[60+2]=1;
        let b=tick_array(&dynamic,&pool,64).unwrap();
        assert_eq!(b.initialized[0].index,2*64);
        assert_eq!(nearest_initialized(&[a,b],0,1),Some(128));
        assert!(tick_array(&dynamic,&[0_u8;32],64).is_err());
        let mut corrupted=dynamic;corrupted[60]=2;
        assert!(tick_array(&corrupted,&pool,64).is_err());
    }
    #[test]fn cp14_oracle_layout_and_trade_enable(){
        let pool=[12_u8;32];let mut d=vec![0_u8;254];
        d[..8].copy_from_slice(&ORACLE_DISC);
        d[8..40].copy_from_slice(&pool);
        d[48..50].copy_from_slice(&10_u16.to_le_bytes());
        d[50..52].copy_from_slice(&30_u16.to_le_bytes());
        d[52..54].copy_from_slice(&5000_u16.to_le_bytes());
        d[54..58].copy_from_slice(&1000_u32.to_le_bytes());
        d[58..62].copy_from_slice(&10000_u32.to_le_bytes());
        d[62..64].copy_from_slice(&8_u16.to_le_bytes());
        d[40..48].copy_from_slice(&101_u64.to_le_bytes());
        let o=oracle(&d,&pool).unwrap();
        let p=Whirlpool{spacing:64,fee:3000,protocol_share:0,liquidity:1_000_000,
            sqrt:1_u128<<64,tick:0};
        assert!(fee(p,Some(o),100).is_err());
        assert_eq!(fee(p,Some(o),200).unwrap(),3000);
        assert!(oracle(&d,&[0_u8;32]).is_err());
    }
    #[test]fn cp14_adaptive_time_and_volatility(){
        let p=Whirlpool{spacing:64,fee:3000,protocol_share:200,liquidity:1_000_000,
            sqrt:1_u128<<64,tick:0};
        let o=Oracle{trade_enable:1,filter:10,decay:30,reduction:5000,
            control:1000,max_volatility:100000,tick_group:8,last_reference:100,
            last_major:100,volatility_reference:1000,reference_group:-4,accumulator:4000};
        assert!(fee(p,Some(o),90).is_err());
        assert!(fee(p,Some(o),105).unwrap()>3000);
        assert_eq!(fee(p,Some(o),4000).unwrap(),3000);
    }
}
