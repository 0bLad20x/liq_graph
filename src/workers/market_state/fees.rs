//! Private CP8A protocol math. SDK references and supported layouts: docs/CP8A.md.
//! Rates are marginal estimates, never an exact-in quote or an exclusion proof.
use anyhow::{Context,Result,ensure};
use solana_pubkey::Pubkey;
use std::sync::Arc;
use super::{bytes,u16_le,u32_le,u64_le,i32_le,u128_le,WSOL,USDC};

pub(super) const CLOCK:&str="SysvarC1ock11111111111111111111111111111111";
pub(super) const GLOBAL:&str="ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw";
pub(super) const FEE_PROGRAM:&str="pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";
pub(super) fn config_address()->Pubkey {
    Pubkey::find_program_address(&[b"fee_config",super::PUMP_PROGRAM.parse::<Pubkey>().unwrap().as_ref()],
        &FEE_PROGRAM.parse().unwrap()).0
}
#[derive(Clone,Debug,PartialEq,serde::Serialize)]
pub(super) struct Dlmm {
    factor:u16,filter:u16,decay:u16,reduction:u16,control:u32,max_vol:u32,
    power:u8,mode:u8,vol:u32,reference:u32,index:i32,last:i64,
}
impl Dlmm {
    pub fn decode(d:&[u8])->Result<Self> {
        ensure!(d.len()==904 && d.starts_with(&super::DLMM_DISC),"unsupported DLMM LbPair layout");
        let mode=d[36];ensure!(mode<=1,"unsupported DLMM fee mode");
        Ok(Self{factor:u16_le(d,8)?,filter:u16_le(d,10)?,decay:u16_le(d,12)?,
            reduction:u16_le(d,14)?,control:u32_le(d,16)?,max_vol:u32_le(d,20)?,
            power:d[34],mode,vol:u32_le(d,40)?,reference:u32_le(d,44)?,index:i32_le(d,48)?,
            last:i64::from_le_bytes(bytes(d,56,8)?.try_into()?)})
    }
    pub fn next_change(&self,now:i64)->i64 {
        if now<self.last{return self.last;}
        let filter=self.last.saturating_add(i64::from(self.filter));
        let decay=self.last.saturating_add(i64::from(self.decay));
        if now<filter{filter}else if now<decay{decay}else{i64::MAX}
    }
    pub fn reference_lag(&self,now:i64)->u64 {
        if now<self.last{self.last.abs_diff(now)}else{0}
    }
    pub fn rates(&self,active:i32,step:u16,now:i64)->Result<(f64,f64,[bool;2])> {
        self.rates_at(active,active,step,now)
    }
    /// Reference reset uses the actual current bin; volatility then follows the
    /// reached price level. Resetting at the target would erase crossing fees.
    pub fn rates_at(&self,active:i32,target:i32,step:u16,now:i64)->Result<(f64,f64,[bool;2])> {
        let (base,total,directions)=self.rate_units_at(active,target,step,now)?;
        Ok((base as f64/1e9,total as f64/1e9,directions))
    }
    pub fn rate_units_at(&self,active:i32,target:i32,step:u16,now:i64)->Result<(u64,u64,[bool;2])> {
        ensure!(self.reduction<=10000,"DLMM reduction factor invalid");
        // SDK update_references uses a signed delta. A newer LbPair may arrive
        // before Clock; negative elapsed keeps the existing reference unchanged.
        let elapsed=now.checked_sub(self.last).context("DLMM fee timestamp difference overflow")?;
        let (index,reference)=if elapsed>=i64::from(self.filter) {
            (active,if elapsed<i64::from(self.decay){u64::from(self.vol)*u64::from(self.reduction)/10000}else{0})
        }else{(self.index,u64::from(self.reference))};
        let vol=(reference+u64::from(index.abs_diff(target))*10000).min(u64::from(self.max_vol));
        let power=10_u128.checked_pow(u32::from(self.power)).context("DLMM fee power overflow")?;
        let base=u128::from(self.factor).checked_mul(u128::from(step)).and_then(|n|n.checked_mul(10))
            .and_then(|n|n.checked_mul(power)).context("DLMM base fee overflow")?;
        let square=(u128::from(vol)*u128::from(step)).checked_pow(2).context("DLMM volatility overflow")?;
        let variable=u128::from(self.control).checked_mul(square).context("DLMM variable fee overflow")?.div_ceil(100_000_000_000);
        let total=base.checked_add(variable).context("DLMM total fee overflow")?.min(100_000_000);
        Ok((base.min(100_000_000) as u64,total as u64,
            if self.mode==0{[true,true]}else{[false,true]})) // native X->Y, Y->X
    }
}
#[derive(Clone,Debug,PartialEq,serde::Serialize)]
pub(super) struct PumpPool {
    #[serde(with="crate::core::execution::pubkey_json")]
    pub creator:Pubkey,
    #[serde(with="crate::core::execution::pubkey_json")]
    pub coin_creator:Pubkey,pub mayhem:bool,pub override_bps:u64,pub canonical:bool,
    pub protocol_fees:u64,pub creator_fees:u64}
impl PumpPool {
    pub fn decode(d:&[u8])->Result<Self> {
        ensure!(d.len()>=243 && d.starts_with(&super::POOL_DISC),"unsupported Pump pool layout");
        let creator=Pubkey::new_from_array(bytes(d,11,32)?.try_into()?);
        let mint=Pubkey::new_from_array(bytes(d,43,32)?.try_into()?);
        let canonical=Pubkey::find_program_address(&[b"pool-authority",mint.as_ref()],
            &"6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P".parse().unwrap()).0==creator;
        Ok(Self{creator,canonical,
            coin_creator:Pubkey::new_from_array(bytes(d,211,32)?.try_into()?),
            mayhem:d.get(243).copied().unwrap_or(0)!=0,
            protocol_fees:if d.len()>=279{u64_le(d,271)?}else{0},
            creator_fees:if d.len()>=287{u64_le(d,279)?}else{0},
            override_bps:if d.len()>=269{u64_le(d,261)?}else{0}})
    }
}
#[derive(Clone,Debug,PartialEq,serde::Serialize)]
pub(super) struct Global {pub configurable:bool,pub disabled:u8}
impl Global {
    pub fn decode(d:&[u8])->Result<Self>{
        ensure!(d.len()>=321 && d.starts_with(&[149,8,156,202,160,252,176,217]),"invalid Pump GlobalConfig");
        Ok(Self{disabled:d[56],configurable:d.get(940).copied().unwrap_or(0)!=0})
    }
}
#[derive(Clone,Copy,Debug,PartialEq,serde::Serialize)]
pub(super) struct Rates {pub lp:u64,pub protocol:u64,pub creator:u64}
impl Rates {
    fn decode(d:&[u8],p:usize)->Result<Self>{Ok(Self{lp:u64_le(d,p)?,protocol:u64_le(d,p+8)?,creator:u64_le(d,p+16)?})}
    pub fn total(self)->Result<f64>{let n=self.lp.checked_add(self.protocol).and_then(|n|n.checked_add(self.creator)).context("Pump fee overflow")?;
        ensure!(n<10000,"Pump fee >=100 percent");Ok(n as f64/10000.0)}
}
#[derive(Clone,Debug,PartialEq,serde::Serialize)]
pub(super) struct Config {flat:Rates,tiers:Vec<(u128,Rates)>,stable:Vec<(u128,Rates)>,exotic:Option<Rates>}
impl Config {
    pub fn decode(d:&[u8])->Result<Self> {
        ensure!(d.starts_with(&[143,52,146,187,219,123,76,155]),"invalid Pump FeeConfig discriminator");
        let flat=Rates::decode(d,41)?;let mut pos=65;
        fn tiers(d:&[u8],pos:&mut usize)->Result<Vec<(u128,Rates)>> {
            let n=u32_le(d,*pos)? as usize;*pos+=4;
            ensure!(n<=d.len().saturating_sub(*pos)/40,"truncated Pump fee tiers");
            let mut out=Vec::with_capacity(n);
            for _ in 0..n {out.push((u128_le(d,*pos)?,Rates::decode(d,*pos+16)?));*pos+=40;}
            ensure!(out.windows(2).all(|w|w[0].0<w[1].0),"Pump fee thresholds unordered");Ok(out)
        }
        let first=tiers(d,&mut pos)?;
        // Old accounts have reserved zero padding. Current append-only Borsh fields.
        let stable=if d.len()>=pos+4{tiers(d,&mut pos)?}else{Vec::new()};
        let exotic=if d.len()>=pos+24{Some(Rates::decode(d,pos)?)}else{None};
        Ok(Self{flat,tiers:first,stable,exotic})
    }
    pub fn rates(&self,pool:&PumpPool,global:&Global,quote:&str,supply:u64,base:u64,effective_quote:i128)->Result<(Rates,&'static str,u128)> {
        ensure!(base>0 && effective_quote>0,"Pump reserves unavailable");
        let basis=if pool.mayhem{1_000_000_000_000_000_u64}else{supply};
        let cap=(effective_quote as u128).checked_mul(u128::from(basis)).context("Pump cap overflow")?/u128::from(base);
        let canonical=pool.canonical;
        let (mut rates,schedule)=if !canonical{(self.flat,"flat")}
            else if quote==WSOL || quote=="9pan9bMn5HatX4EJdBwg9VgCa7Uz5HL8N1m5D3NdXejP" {
                (self.tier(&self.tiers,cap)?,"sol_tiers")
            }else if quote==USDC {
                (self.tier(if self.stable.is_empty(){&self.tiers}else{&self.stable},cap)?,"stable_tiers")
            }else{(self.exotic.filter(|f|f.lp!=0||f.protocol!=0||f.creator!=0).unwrap_or(self.flat),"exotic_flat")};
        if global.configurable && pool.override_bps>0 {rates.creator=pool.override_bps;}
        if pool.coin_creator==Pubkey::default(){rates.creator=0;}
        rates.total()?;Ok((rates,schedule,cap))
    }
    fn tier(&self,tiers:&[(u128,Rates)],cap:u128)->Result<Rates>{
        let first=tiers.first().context("Pump fee tiers empty")?;
        Ok(tiers.iter().rev().find(|(threshold,_)|cap>=*threshold).unwrap_or(first).1)
    }
}
#[derive(Clone)]
pub(super) enum Shared {Global(Arc<Global>),Config(Arc<Config>)}
