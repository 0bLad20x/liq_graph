//! CP9A: cheap per-direction minimum-notional gate. No RPC, no route sizing.
//! USD reference units: 1 USDC = 1 USD, 1 WSOL = 100 USD (deliberately fixed).
use anyhow::{ensure,Result,Context,bail};
use crate::core::market_state::{PoolPriceObservation,DirectedLiquidity,ReserveCurve};

const USDC:&str="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const WSOL:&str="So11111111111111111111111111111111111111112";

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(super) enum Verdict { Eligible, Dust, Unknown }
impl Verdict {
    pub(super) fn label(self)->&'static str {
        match self { Self::Eligible=>"eligible",Self::Dust=>"dust",Self::Unknown=>"unknown" }
    }
    fn index(self)->usize {match self{Self::Eligible=>0,Self::Dust=>1,Self::Unknown=>2}}
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(super) enum Mode { Off,Shadow,Filter }
impl Mode {
    pub(super) fn label(self)->&'static str {match self{Self::Off=>"off",Self::Shadow=>"shadow",Self::Filter=>"filter"}}
}
#[derive(Clone,Copy,Debug)]
pub(super) struct Settings {
    pub(super) mode:Mode,
    pub(super) minimum_usd:f64,
    usdc_atoms:u128,
    wsol_lamports:u128,
}
impl Settings {
    pub(super) fn from_env()->Result<Self>{
        let minimum_usd:f64=std::env::var("GRAPH_MIN_DIRECTION_NOTIONAL_USD")
            .unwrap_or_else(|_|"1".into()).parse().context("invalid GRAPH_MIN_DIRECTION_NOTIONAL_USD")?;
        ensure!(minimum_usd.is_finite()&&minimum_usd>=0.0&&minimum_usd<=1_000_000.0,
            "GRAPH_MIN_DIRECTION_NOTIONAL_USD must be 0..=1000000");
        let mode=match std::env::var("GRAPH_DIRECTION_LIQUIDITY_MODE")
            .unwrap_or_else(|_|"filter".into()).to_ascii_lowercase().as_str(){
            "off"=>Mode::Off,"shadow"=>Mode::Shadow,"filter"=>Mode::Filter,
            _=>bail!("GRAPH_DIRECTION_LIQUIDITY_MODE must be off, shadow or filter"),
        };
        Ok(Self{mode,minimum_usd,
            usdc_atoms:(minimum_usd*1_000_000.0).ceil() as u128,
            wsol_lamports:(minimum_usd*10_000_000.0).ceil() as u128})
    }
    #[cfg(test)]
    fn test(minimum_usd:f64)->Self{Self{mode:Mode::Filter,minimum_usd,
        usdc_atoms:(minimum_usd*1_000_000.0).ceil() as u128,
        wsol_lamports:(minimum_usd*10_000_000.0).ceil() as u128}}
    fn required(self,mint:&str)->Option<u128> {
        match mint {USDC=>Some(self.usdc_atoms),WSOL=>Some(self.wsol_lamports),_=>None}
    }
}
fn fee(n:u128,r:u64)->Option<u128>{
    (r<10_000).then(||(n*u128::from(r)).div_ceil(10_000))
}
/// A PumpSwap quote-input test using the already published reserve curve.
/// Only proves that a reference-sized anchor input produces positive integer
/// output. A zero/indeterminate result is unknown, NOT a proof of dust.
fn pump_anchor_input_positive(c:&ReserveCurve,dir:&DirectedLiquidity,amount:u128)->bool{
    if amount==0||c.base_reserve_raw==0||c.effective_quote_reserve_raw==0{return false;}
    if dir.input_mint!=c.base_mint {
        let total=c.fee_bps.iter().map(|&r|u128::from(r)).sum::<u128>();
        if total>=10_000{return false;}
        let mut net=amount.saturating_mul(10_000)/(10_000+total);
        let Some(charges)=c.fee_bps.iter().try_fold(0_u128,|n,&r|n.checked_add(fee(net,r)?)) else{return false;};
        let over=net.saturating_add(charges);
        net=net.saturating_sub(over.saturating_sub(amount))
            .saturating_sub(u128::from(c.buy_input_padding_raw));
        c.base_reserve_raw as u128*net/(u128::from(c.effective_quote_reserve_raw)+net)>0
    }else{
        let gross=u128::from(c.effective_quote_reserve_raw)*amount/
            (u128::from(c.base_reserve_raw)+amount);
        let Some(charges)=c.fee_bps.iter().try_fold(0_u128,|n,&r|n.checked_add(fee(gross,r)?)) else{return false;};
        let Some(lp)=fee(gross,c.fee_bps[0]) else{return false;};
        gross.saturating_sub(lp)<=u128::from(c.spendable_quote_reserve_raw)
            &&gross.saturating_sub(charges)>0
    }
}

/// The gate is conservative about incomplete DLMM windows. A missing tail is
/// UNKNOWN, never silently treated as zero. No market-wide USD price lookup.
pub(super) fn classify(obs:&PoolPriceObservation, direction:usize,cfg:Settings)->Verdict{
    if cfg.mode==Mode::Off||cfg.usdc_atoms==0 {return Verdict::Eligible;}
    let Some(liq)=obs.liquidity.as_ref() else{return Verdict::Unknown;};
    let Some(dir)=liq.directions.get(direction) else{return Verdict::Unknown;};
    if dir.status=="unknown"||dir.status=="uncovered"{return Verdict::Unknown;}
    if dir.status=="empty" {
        return if dir.coverage=="exhausted_bitmap"||dir.coverage=="reserve_curve"{
            Verdict::Dust
        }else{Verdict::Unknown};
    }
    if dir.status!="available"{return Verdict::Unknown;}
    // Output-anchor direction: only real, spendable output inventory counts.
    if let Some(required)=cfg.required(&dir.output_mint) {
        let Ok(cap)=dir.output_capacity_raw.parse::<u128>() else{return Verdict::Unknown;};
        return if cap>=required{Verdict::Eligible}
            else if dir.coverage=="exhausted_bitmap"||dir.coverage=="reserve_curve"{
                Verdict::Dust
            }else{Verdict::Unknown};
    }
    // Input-anchor direction: volume must be processable through observed depth.
    if let Some(required)=cfg.required(&dir.input_mint) {
        if let Some(cap)=dir.input_capacity_raw.as_deref().and_then(|v|v.parse::<u128>().ok()) {
            return if cap>=required{Verdict::Eligible}
                else if dir.coverage=="exhausted_bitmap"{Verdict::Dust}
                else{Verdict::Unknown};
        }
        if let Some(c)=liq.reserve_curve.as_ref(){
            return if pump_anchor_input_positive(c,dir,required){Verdict::Eligible}
                else{Verdict::Unknown};
        }
    }
    Verdict::Unknown
}

#[derive(Default,serde::Serialize)]
pub(super) struct Report {
    mode:&'static str, minimum_usd:f64, reference_sol_usd:u64,
    direction_observations:[u64;3],dust_with_marginal_rate:u64,
    unknown_with_marginal_rate:u64, filtered_direction_observations:u64,
    examples:Vec<serde_json::Value>,
}
impl Report {
    pub(super) fn new(settings:Settings)->Self{
        Self{mode:settings.mode.label(),minimum_usd:settings.minimum_usd,
            reference_sol_usd:100,..Default::default()}
    }
    pub(super) fn record(&mut self,obs:&PoolPriceObservation,results:[Verdict;2],cfg:Settings){
        for (i,v) in results.into_iter().enumerate(){
            self.direction_observations[v.index()]+=1;
            let rate=obs.valuation.as_ref().and_then(|f|f.estimated_net_rates[i]);
            if rate.is_none(){continue;}
            if v==Verdict::Dust {
                self.dust_with_marginal_rate+=1;
                self.filtered_direction_observations+=u64::from(cfg.mode==Mode::Filter);
            }
            if v==Verdict::Unknown{self.unknown_with_marginal_rate+=1;}
            if v!=Verdict::Eligible && self.examples.len()<16 {
                let d=obs.liquidity.as_ref().map(|l|&l.directions[i]);
                self.examples.push(serde_json::json!({"pool":obs.pool,"venue":obs.venue,
                    "direction":i,"input_mint":d.map(|d|&d.input_mint),
                    "output_mint":d.map(|d|&d.output_mint),
                    "classification":v.label(),"coverage":d.map(|d|d.coverage),
                    "input_capacity_raw":d.and_then(|d|d.input_capacity_raw.as_deref()),
                    "output_capacity_raw":d.map(|d|d.output_capacity_raw.as_str()),
                    "slot":obs.slot,"liquidity_revision":obs.liquidity.as_ref().map(|l|l.revision)}));
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::core::market_state::{PoolLiquidity,PriceOrigin,PriceKind,Consistency};
    fn observation(input:&str,output:&str,cap:&str,coverage:&'static str,input_cap:Option<&str>)->PoolPriceObservation{
        let direction=DirectedLiquidity{input_mint:input.to_string(),output_mint:output.to_string(),
            input_decimals:6,output_decimals:6,status:"available",coverage,
            next_required_account:None,first_source_position:Some(0),
            marginal_net_rate:Some(1.0),monotone_rate_tail:true,
            input_capacity_raw:input_cap.map(str::to_string),output_capacity_raw:cap.to_string(),
            levels:Vec::new(),reason:None};
        PoolPriceObservation{origin:PriceOrigin::Baseline,pool:"test".into(),venue:"meteora_dlmm",
            mint_a:input.into(),mint_b:output.into(),base_mint:input.into(),quote_mint:output.into(),
            price_quote_per_base:1.0,valuation:None,liquidity:Some(Arc::new(PoolLiquidity{
                model:"test",revision:1,content_digest:[0;32],account_set_digest:[0;32],
                execution_accounts:Vec::new(),reserve_curve:None,cpmm_quote:None,orca_quote:None,
                directions:[direction.clone(),direction]})),previous_price:None,delta_bps:None,
            price_kind:PriceKind::DlmmActiveBin,best_bid_quote_per_base:None,best_ask_quote_per_base:None,
            consistency:Consistency::SingleAccount,slot:1,received_at_us:1,sequence:1,
            component_slot_min:1,component_slot_max:1,
            component_received_at_us_min:1,component_received_at_us_max:1,
            trigger_field:None,pipeline:None}
    }
    #[test]
    fn one_usdc_atom_dust_and_one_dollar_eligible(){
        let cfg=Settings::test(1.0);
        assert_eq!(classify(&observation("X",USDC,"1","exhausted_bitmap",Some("100")),0,cfg),Verdict::Dust);
        assert_eq!(classify(&observation("X",USDC,"1000000","exhausted_bitmap",Some("100")),0,cfg),Verdict::Eligible);
        assert_eq!(classify(&observation(USDC,"X","1000000","exhausted_bitmap",Some("1000000")),0,cfg),Verdict::Eligible);
    }
    #[test]
    fn wsol_reference_and_incomplete_window_preserved(){
        let cfg=Settings::test(1.0);
        assert_eq!(classify(&observation("X",WSOL,"9999999","exhausted_bitmap",None),0,cfg),Verdict::Dust);
        assert_eq!(classify(&observation("X",WSOL,"10000000","exhausted_bitmap",None),0,cfg),Verdict::Eligible);
        assert_eq!(classify(&observation("X",USDC,"1","execution_window_end",None),0,cfg),Verdict::Unknown);
        assert_eq!(classify(&observation("X","Y","1","exhausted_bitmap",None),0,cfg),Verdict::Unknown);
    }
}
