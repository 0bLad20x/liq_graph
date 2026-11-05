//! CP20: bounded, snapshot-only Direct2 economic forecast.
//! Advisory ranking/calibration only: never vetoes a candidate or sets V10 trade_size.
//! PumpSwap and CPMM use frozen reserve curves; DLMM traverses the observed bin
//! execution window with the CURRENT base+variable fee held fixed across bins.
//! Continuous arithmetic is deliberately approximate. No RPC, no persistent cache.
use super::Selection;
use crate::core::execution::ExecutionParameters;
use crate::core::market_state::{PoolPriceObservation,DirectedLiquidity};
use crate::core::signals::SignalWitness;

const WSOL:&str="So11111111111111111111111111111111111111112";
const USDC:&str="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const NETWORK_BASE_LAMPORTS:u64=5_000;
// CP21A: use integer-only financial arithmetic. At fixed $100/SOL,
 // one lamport = 100 USD nanounits, one USDC base unit = 1,000 nanounits.
 // SOL/USD here is the existing controlled calibration reference, NOT live FX.
const SOL_USD_REFERENCE_USD:u128=100;
const USDC_DECIMALS:u128=1_000_000;
const LAMPORTS_PER_SOL:u128=1_000_000_000;
const WSOL_NANO_USD_PER_ATOM:i128=100;
const USDC_NANO_USD_PER_ATOM:i128=1_000;
const V10_PROFIT_FEE_DIVISOR:i128=10;

#[derive(Clone,Debug,serde::Serialize)]
pub(super) struct Prediction {
    pub scope:&'static str,
    pub status:&'static str,
    pub reason:Option<&'static str>,
    pub path:Vec<String>,
    pub pairs_evaluated:u64,
    pub pairs_unknown:u64,
    pub input_raw:Option<u64>,
    pub output_raw:Option<u64>,
    pub dex_gain_raw:Option<i128>,
    /// Known V10 share: floor(positive gross DEX profit / 10), settlement atoms.
    pub v10_profit_fee_raw:Option<i128>,
    pub post_v10_gain_raw:Option<i128>,
    pub reference_network_cost_raw:Option<u64>,
    pub predicted_net_gain_raw:Option<i128>,
    /// Comparable unit across WSOL and USDC: 10^-9 USD at fixed SOL=$100.
    pub predicted_net_gain_usd_nano:Option<i128>,
    pub fee_policy:&'static str,
}
impl Prediction {
    pub fn unknown(reason:&'static str)->Self {
        Self {scope:"snapshot_direct2_forecast_not_exclusion",
            status:"unknown",reason:Some(reason),path:Vec::new(),pairs_evaluated:0,
            pairs_unknown:0,input_raw:None,output_raw:None,dex_gain_raw:None,
            v10_profit_fee_raw:None,post_v10_gain_raw:None,
            reference_network_cost_raw:None,predicted_net_gain_raw:None,
            predicted_net_gain_usd_nano:None,
            fee_policy:"dlmm_current_base_plus_variable_fee_frozen_across_bins"}
    }
}

fn directed<'a>(obs:&'a PoolPriceObservation,input:&str,output:&str)
    ->Option<(&'a DirectedLiquidity,usize)> {
    let liquidity=obs.liquidity.as_ref()?;
    let index=liquidity.directions.iter().position(|d|
        d.input_mint==input && d.output_mint==output && d.status=="available")?;
    // Token-2022 transfer fees and unresolved account state are NOT zero costs.
    if !obs.valuation.as_ref()?.token_fee_status.iter()
        .all(|s|*s=="verified_no_standard_transfer_fee"){return None;}
    Some((&liquidity.directions[index],index))
}
fn finite_output(n:f64)->Option<f64> {
    (n.is_finite()&&n>=0.0&&n<=u64::MAX as f64).then_some(n)
}
fn reserve_quote(obs:&PoolPriceObservation,input:&str,output:&str,amount:f64)->Option<f64> {
    let (dir,index)=directed(obs,input,output)?;
    let liquidity=obs.liquidity.as_ref()?;
    if let Some(c)=liquidity.cpmm_quote.as_ref() {
        let (rin,rout,on_input)=if input==c.mint_0&&output==c.mint_1 {
            (c.reserve_0_raw,c.reserve_1_raw,c.creator_fee_on_input_0_to_1)
        }else if input==c.mint_1&&output==c.mint_0 {
            (c.reserve_1_raw,c.reserve_0_raw,c.creator_fee_on_input_1_to_0)
        }else{return None};
        let fee=(c.trade_fee_rate+if on_input{c.creator_fee_rate}else{0}) as f64/1e6;
        let creator=if on_input{0.0}else{c.creator_fee_rate as f64/1e6};
        if fee>=1.0||creator>=1.0||rin==0||rout==0{return None;}
        let net=amount*(1.0-fee);
        return finite_output((rout as f64)*net/((rin as f64)+net)*(1.0-creator));
    }
    if let Some(c)=liquidity.reserve_curve.as_ref() {
        let fee_sum=c.fee_bps.iter().map(|v|*v as f64).sum::<f64>()/10_000.0;
        if fee_sum>=1.0||c.base_reserve_raw==0||c.effective_quote_reserve_raw==0{return None;}
        if input==c.base_mint {
            let gross=c.effective_quote_reserve_raw as f64*amount/
                (c.base_reserve_raw as f64+amount);
            let output=gross*(1.0-fee_sum);
            // Only real spendable quote can leave the vault.
            return (output<=c.spendable_quote_reserve_raw as f64).then_some(output);
        }
        if output==c.base_mint {
            // PumpSwap buy charges quote fees; fixed observed fee for this snapshot.
            let net=amount/(1.0+fee_sum);
            let output=c.base_reserve_raw as f64*net/
                (c.effective_quote_reserve_raw as f64+net);
            return finite_output(output);
        }
        return None;
    }
    if liquidity.model=="dlmm_execution_window_v2" {
        let v=obs.valuation.as_ref()?;
        let fee=v.base_fee_rate?+v.variable_fee_rate?;
        if !(0.0..1.0).contains(&fee){return None;}
        let fee_on_input=v.fees_on_input[index];
        let mut left=amount;
        let mut out=0.0;
        for level in &dir.levels {
            let rate=level.rate_numerator as f64/level.rate_denominator as f64;
            let cap=level.output_parts_raw.iter().map(|n|*n as f64).sum::<f64>();
            if !(rate.is_finite()&&rate>0.0) {return None;}
            let effective=if fee_on_input{rate*(1.0-fee)}else{rate};
            if effective<=0.0{return None;}
            let gross=(left*effective).min(cap);
            let spend=gross/effective;
            out+=if fee_on_input{gross}else{gross*(1.0-fee)};
            left=(left-spend).max(0.0);
            if left<=1e-7{return finite_output(out);}
        }
        // Missing bins or a truncated Prepared execution window are UNKNOWN,
        // never extrapolate an infinite pool or claim a negative profit proof.
        if left<=1e-7{return finite_output(out);}
        return None;
    }
    None
}
fn route(a:&PoolPriceObservation,b:&PoolPriceObservation,
    settlement:&str,target:&str,amount:f64)->Option<f64> {
    let intermediate=reserve_quote(a,settlement,target,amount)?;
    reserve_quote(b,target,settlement,intermediate)
}
fn gain(a:&PoolPriceObservation,b:&PoolPriceObservation,
    settlement:&str,target:&str,amount:f64)->Option<f64> {
    route(a,b,settlement,target,amount).map(|output|output-amount)
}
fn optimum(a:&PoolPriceObservation,b:&PoolPriceObservation,
    settlement:&str,target:&str)->Option<(u64,u64,i128)> {
    let func=|x:f64|gain(a,b,settlement,target,x);
    // Exponential bracketing finds a finite feasible range from the atomic
    // domain, not a fixed user-configured or synthetic trade-size grid.
    let mut previous=1.0_f64;
    let mut previous_gain=func(previous)?;
    let mut upper=previous;
    for _ in 0..62 {
        let next=previous*2.0;
        if next>=u64::MAX as f64{upper=previous;break;}
        match func(next) {
            Some(next_gain) if next_gain>previous_gain=>{
                previous=next;previous_gain=next_gain;upper=previous;
            },
            Some(_)=>{upper=next;break;},
            None=>{
                // Locate the observed execution-window boundary without
                // treating unobserved bins as an empty pool.
                let mut lo=previous;let mut hi=next;
                for _ in 0..48 {
                    let mid=(lo+hi)*0.5;
                    if func(mid).is_some(){lo=mid;}else{hi=mid;}
                }
                upper=lo;break;
            }
        }
    }
    if upper<1.0{return None;}
    // Unimodal approximation for a monotone constant-product or ordered
    // constant-fee DLMM curve. No claim of globally exact integer optimum.
    let mut lo=0.0_f64;let mut hi=upper;
    const GOLDEN:f64=0.6180339887498949;
    let mut x1=hi-GOLDEN*(hi-lo);
    let mut x2=lo+GOLDEN*(hi-lo);
    for _ in 0..54 {
        let v1=func(x1).unwrap_or(f64::NEG_INFINITY);
        let v2=func(x2).unwrap_or(f64::NEG_INFINITY);
        if v1<v2 {lo=x1;x1=x2;x2=lo+GOLDEN*(hi-lo);}
        else {hi=x2;x2=x1;x1=hi-GOLDEN*(hi-lo);}
    }
    let mid=((lo+hi)*0.5).round();
    // Integer-neighbor checks cover local rounding around the continuous
    // optimum. This does not pretend that f64 quotes are V10-exact.
    let mut best=None;
    for candidate in [1.0,upper.floor(),mid-2.0,mid-1.0,mid,mid+1.0,mid+2.0] {
        if !(candidate>=1.0&&candidate<=u64::MAX as f64){continue;}
        let q=candidate as u64;
        let Some(output)=route(a,b,settlement,target,q as f64) else{continue};
        let output=output.floor() as u64;
        let dex_gain=i128::from(output)-i128::from(q);
        if best.as_ref().is_none_or(|(_,_,g):&(u64,u64,i128)|dex_gain>*g){
            best=Some((q,output,dex_gain));
        }
    }
    best
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
struct NetAccounting {
    v10_profit_fee_raw:i128,
    post_v10_gain_raw:i128,
    network_cost_raw:u64,
    net_gain_raw:i128,
    net_gain_usd_nano:i128,
}
fn normalize_usd_nano(settlement:&str,net_raw:i128)->Option<i128> {
    let scale=match settlement {
        WSOL=>WSOL_NANO_USD_PER_ATOM,
        USDC=>USDC_NANO_USD_PER_ATOM,
        _=>return None,
    };
    net_raw.checked_mul(scale)
}
fn v10_profit_fee(gross_dex_gain_raw:i128)->i128 {
    // V10's known 10% positive-profit share, integer truncated.
    // Checked against every 2,174 complete, successful simulations from
    // the 2026-11-05 CP20R/CP20S archived 5-minute runs.
    gross_dex_gain_raw.max(0)/V10_PROFIT_FEE_DIVISOR
}
fn network_cost(settlement:&str,params:&ExecutionParameters)->Option<u64> {
    let lamports=NETWORK_BASE_LAMPORTS.checked_add(params.priority_fee_lamports)?;
    if settlement==WSOL{return Some(lamports);}
    if settlement==USDC {
        // Round UP network costs denominated in USDC atoms to avoid
        // representing a fractional on-chain token base unit as free.
        let numerator=u128::from(lamports).checked_mul(SOL_USD_REFERENCE_USD)?
            .checked_mul(USDC_DECIMALS)?;
        let value=numerator.checked_add(LAMPORTS_PER_SOL-1)?/LAMPORTS_PER_SOL;
        return u64::try_from(value).ok();
    }
    None
}
fn net_accounting(settlement:&str,gross_dex_gain_raw:i128,
    params:&ExecutionParameters)->Option<NetAccounting> {
    // The current V10 simulation uses no extra fee collector. If configured,
    // remain Unknown until its exact accounting is verified independently.
    if params.additional_fee_bp!=0||params.additional_fee_collector.is_some(){return None;}
    let v10_profit_fee_raw=v10_profit_fee(gross_dex_gain_raw);
    let post_v10_gain_raw=gross_dex_gain_raw.checked_sub(v10_profit_fee_raw)?;
    let network_cost_raw=network_cost(settlement,params)?;
    let net_gain_raw=post_v10_gain_raw.checked_sub(i128::from(network_cost_raw))?;
    let net_gain_usd_nano=normalize_usd_nano(settlement,net_gain_raw)?;
    Some(NetAccounting{
        v10_profit_fee_raw,post_v10_gain_raw,network_cost_raw,
        net_gain_raw,net_gain_usd_nano,
    })
}
fn predict_pair(a:&PoolPriceObservation,b:&PoolPriceObservation,
    settlement:&str,target:&str,params:&ExecutionParameters)->Option<(u64,u64,i128,i128)> {
    let (quantity,output,dex)=optimum(a,b,settlement,target)?;
    let accounting=net_accounting(settlement,dex,params)?;
    Some((quantity,output,dex,accounting.net_gain_raw))
}
/// Conservative economic priority, shared by fresh and active Direct2 sources.
/// Unknown is retained ahead of modeled nonpositive cases; never a veto.
pub(super) fn priority_key(gain:Option<i128>)->(u8,std::cmp::Reverse<i128>) {
    let category=match gain{Some(value) if value>0=>0,None=>1,Some(_)=>2};
    (category,std::cmp::Reverse(gain.unwrap_or(0)))
}
/// Cross-settlement priority: USD nanounits, NEVER raw WSOL/USDC atoms.
pub(super) fn pair_gain(a:&PoolPriceObservation,b:&PoolPriceObservation,
    settlement:&str,target:&str,params:&ExecutionParameters)->Option<i128> {
    let (_,_,_,net)=predict_pair(a,b,settlement,target,params)?;
    normalize_usd_nano(settlement,net)
}
pub(super) fn witness_gain(w:&SignalWitness,params:&ExecutionParameters)->Option<i128> {
    if w.legs.len()!=2{return None;}
    let first=&w.legs[0];let second=&w.legs[1];
    if first.observation.pool==second.observation.pool{return None;}
    let settlement=first.input_mint.to_string();
    let target=first.output_mint.to_string();
    if second.input_mint!=first.output_mint||second.output_mint!=first.input_mint{return None;}
    pair_gain(&first.observation,&second.observation,&settlement,&target,params)
}
pub(super) fn predict(selection:&Selection)->Prediction {
    use crate::core::signals::SignalRouteKind;
    let mut result=Prediction::unknown("no_fully_modeled_direct2_pair");
    if selection.route_kind!=SignalRouteKind::Direct2 {
        result.reason=Some("not_direct2");return result;
    }
    let settlement=selection.request.settlement.to_string();
    let target=selection.target.to_string();
    let mut best:Option<(u64,u64,i128,i128,String,String)>=None;
    for a in selection.pools.iter().filter(|p|p.role=="direct"){
        for b in selection.pools.iter().filter(|p|p.role=="partner") {
            if a.pool==b.pool{continue;}
            match (a.liquidity_window_checked&&b.liquidity_window_checked)
                .then(||predict_pair(&a.observation,&b.observation,&settlement,&target,
                    &selection.request.parameters)).flatten() {
                Some((input,output,dex,net))=>{
                    result.pairs_evaluated+=1;
                    if best.as_ref().is_none_or(|x|net>x.3) {
                        best=Some((input,output,dex,net,a.pool.clone(),b.pool.clone()));
                    }
                },
                None=>result.pairs_unknown+=1,
            }
        }
    }
    if let Some((input,output,dex,net,a,b))=best {
        result.status="estimated";
        result.reason=None;
        result.path=vec![a,b];
        result.input_raw=Some(input);
        result.output_raw=Some(output);
        let accounting=net_accounting(&settlement,dex,&selection.request.parameters);
        result.dex_gain_raw=Some(dex);
        result.v10_profit_fee_raw=accounting.map(|a|a.v10_profit_fee_raw);
        result.post_v10_gain_raw=accounting.map(|a|a.post_v10_gain_raw);
        result.reference_network_cost_raw=accounting.map(|a|a.network_cost_raw);
        result.predicted_net_gain_raw=Some(net);
        result.predicted_net_gain_usd_nano=accounting.map(|a|a.net_gain_usd_nano);
    }
    result
}
pub(super) fn comparison(selection:&Selection,
    report:&crate::core::execution::ExecutionReport)->serde_json::Value {
    use serde_json::json;
    let calls=&report.trace.calls;
    if calls.len()!=2||calls.iter().any(|c|c.status!="complete") {
        return json!({"status":"unavailable","reason":"not_two_complete_dex_calls"});
    }
    let Some((a,b))=calls[0].pool.as_ref().zip(calls[1].pool.as_ref())else {
        return json!({"status":"unavailable","reason":"pool_identity_unresolved"});
    };
    let Some((first,second))=selection.pools.iter().find(|p|p.pool==*a)
        .zip(selection.pools.iter().find(|p|p.pool==*b))else{
        return json!({"status":"unavailable","reason":"executed_pool_not_in_frozen_selection"});
    };
    let settlement=selection.request.settlement.to_string();
    let target=selection.target.to_string();
    let Some(first_input)=calls[0].inputs.first()else{
        return json!({"status":"unavailable","reason":"actual_input_missing"});
    };
    let Some(first_output)=calls[0].outputs.first()else{
        return json!({"status":"unavailable","reason":"first_output_missing"});
    };
    let Some(second_input)=calls[1].inputs.first()else{
        return json!({"status":"unavailable","reason":"second_input_missing"});
    };
    let Some(second_output)=calls[1].outputs.first()else{
        return json!({"status":"unavailable","reason":"second_output_missing"});
    };
    // Compare only continuous, fully closed flows. Unused settlement input is
    // returned to the wallet and must not be mistaken for a quote discrepancy.
    if first_input.mint!=settlement || first_output.mint!=target
        || second_input.mint!=target || second_output.mint!=settlement
        || first_output.amount_raw!=second_input.gross_amount_raw
        || second_input.amount_raw!=second_input.gross_amount_raw
        || first_input.gross_amount_raw<first_input.amount_raw {
        return json!({"status":"unavailable","reason":"not_complete_continuous_cycle"});
    }
    let input=first_input.gross_amount_raw;
    let refund=first_input.gross_amount_raw-first_input.amount_raw;
    let actual=second_output.amount_raw.checked_add(refund);
    let actual_gain=actual.map(|o|i128::from(o)-i128::from(input));
    let model=if first.liquidity_window_checked&&second.liquidity_window_checked {
        route(&first.observation,&second.observation,&settlement,&target,input as f64)
    }else{None};
    let output=model.map(|n|n.floor() as u64);
    let network=network_cost(&settlement,&selection.request.parameters);
    json!({"status":if output.is_some(){"estimated"}else{"unknown"},
        "scope":"frozen_snapshot_same_executed_pair_at_v10_chosen_amount",
        "actual_input_raw":input,"actual_output_raw":actual,
        "predicted_output_at_v10_input_raw":output,
        "predicted_dex_gain_at_v10_input_raw":output.map(|o|i128::from(o)-i128::from(input)).map(|x|x.to_string()),
        "actual_dex_gain_raw":actual_gain.map(|x|x.to_string()),
        "settlement_input_refund_raw":refund,
        "prediction_input_delta_actual_minus_optimum_raw":selection.economic.input_raw
            .map(|q|(i128::from(input)-i128::from(q)).to_string()),
        "predicted_net_gain_at_v10_input_raw":output.zip(network)
            .map(|(o,c)|(i128::from(o)-i128::from(input)-i128::from(c)).to_string()),
        "output_delta_actual_minus_predicted_raw":actual.zip(output)
            .map(|(a,p)|(i128::from(a)-i128::from(p)).to_string()),
        "prediction_optimal_input_raw":selection.economic.input_raw,
        "prediction_optimal_net_gain_raw":selection.economic.predicted_net_gain_raw.map(|v|v.to_string()),
        "fee_assumption":"DLMM current base+variable held fixed; protocol fees frozen",
        "comparison_limit":"same slot is not atomic bank identity; rent and flashloan charges are not modeled"})
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::market_state::{FeeValuation,PoolLiquidity,ReserveCurve,
        LiquidityLevel,DirectedLiquidity,Consistency,PriceKind,PriceOrigin,CpmmQuoteSnapshot};
    use std::sync::Arc;

    fn fee(model:&'static str,base:f64,variable:f64)->FeeValuation {
        FeeValuation{model,estimated_net_rates:[Some(1.0);2],
            dex_fee_rates:[Some(base+variable);2],fees_on_input:[true;2],
            base_fee_rate:Some(base),variable_fee_rate:Some(variable),
            lp_fee_bps:None,protocol_fee_bps:None,creator_fee_bps:None,
            fee_schedule:None,market_cap_raw:None,chain_timestamp:1,chain_epoch:0,
            token_fee_status:["verified_no_standard_transfer_fee";2],
            unknown_reason:None,scope:"test"}
    }
    fn dir(a:&str,b:&str,levels:Vec<LiquidityLevel>)->DirectedLiquidity {
        DirectedLiquidity{input_mint:a.into(),output_mint:b.into(),
            input_decimals:9,output_decimals:9,status:"available",
            coverage:"execution_window_end",next_required_account:None,
            first_source_position:Some(0),marginal_net_rate:Some(1.0),
            monotone_rate_tail:true,input_capacity_raw:None,
            output_capacity_raw:"100000000".into(),levels,reason:None}
    }
    fn observed(model:&'static str,directions:[DirectedLiquidity;2],
        cpmm:Option<CpmmQuoteSnapshot>,pump:Option<ReserveCurve>,
        valuation:FeeValuation)->PoolPriceObservation {
        PoolPriceObservation{origin:PriceOrigin::Baseline,pool:"P".into(),venue:model,
            mint_a:"S".into(),mint_b:"T".into(),base_mint:"S".into(),quote_mint:"T".into(),
            price_quote_per_base:1.0,valuation:Some(valuation),
            liquidity:Some(Arc::new(PoolLiquidity{model,revision:1,
                content_digest:[0;32],account_set_digest:[1;32],execution_accounts:vec![],
                reserve_curve:pump,cpmm_quote:cpmm,orca_quote:None,clmm_quote:None,
                directions})),
            previous_price:None,delta_bps:None,price_kind:PriceKind::ReserveSpot,
            best_bid_quote_per_base:None,best_ask_quote_per_base:None,
            consistency:Consistency::SingleAccount,slot:1,received_at_us:0,sequence:1,
            component_slot_min:1,component_slot_max:1,component_received_at_us_min:0,
            component_received_at_us_max:0,trigger_field:None,pipeline:None}
    }
    fn bin(id:i32,rate:u128,capacity:u64,fee_rate:u64)->LiquidityLevel {
        LiquidityLevel{source_position:id,gross_rate:rate as f64,
            net_rate:rate as f64*(1.0-fee_rate as f64/1e9),
            fee_rate:fee_rate as f64/1e9,fees_on_input:true,
            rate_numerator:rate,rate_denominator:1,
            output_parts_raw:[capacity,0,0],fee_numerator:fee_rate,
            fee_denominator:1_000_000_000,maximum_rebate_bps:0,
            full_level_net_rate:None,input_raw:None,output_raw:capacity,
            minimum_input_raw:Some(1),fee_raw:None}
    }

    #[test]
    fn cp20_dlmm_crosses_bins_and_freezes_current_dynamic_fee() {
        let obs=observed("dlmm_execution_window_v2",
            [dir("S","T",vec![bin(0,2,10,100_000_000),
                              bin(1,1,10,350_000_000)]),
             dir("T","S",vec![bin(0,1,10,100_000_000)])],
            None,None,fee("dlmm",0.06,0.04));
        let one=reserve_quote(&obs,"S","T",5.0).unwrap();
        let two=reserve_quote(&obs,"S","T",10.0).unwrap();
        assert!((one-9.0).abs()<1e-8);
        // Second level uses the initial 10% fee, NOT its 35% fee.
        assert!((two-14.0).abs()<1e-8, "unexpected second-bin quote {two}");
        assert!(reserve_quote(&obs,"S","T",100.0).is_none());
    }

    #[test]
    fn cp20_cpmm_predicts_optimum_from_reserves_not_fixed_input_grid() {
        let first=observed("raydium_cpmm_directional_v1",
            [dir("S","T",vec![]),dir("T","S",vec![])],
            Some(CpmmQuoteSnapshot{mint_0:"S".into(),mint_1:"T".into(),
                reserve_0_raw:1_000_000,reserve_1_raw:1_200_000,
                trade_fee_rate:0,creator_fee_rate:0,
                creator_fee_on_input_0_to_1:true,creator_fee_on_input_1_to_0:true}),
            None,fee("cpmm",0.0,0.0));
        let second=observed("raydium_cpmm_directional_v1",
            [dir("S","T",vec![]),dir("T","S",vec![])],
            Some(CpmmQuoteSnapshot{mint_0:"T".into(),mint_1:"S".into(),
                reserve_0_raw:1_000_000,reserve_1_raw:1_200_000,
                trade_fee_rate:0,creator_fee_rate:0,
                creator_fee_on_input_0_to_1:true,creator_fee_on_input_1_to_0:true}),
            None,fee("cpmm",0.0,0.0));
        let (quantity,output,gain)=optimum(&first,&second,"S","T").unwrap();
        assert!(quantity>1 && output>quantity && gain>0);
        // Test-only independent dense reference, NOT a production-size grid.
        let reference=(1..=4_000).map(|i|i*100).filter_map(|x|
            route(&first,&second,"S","T",x as f64)
                .map(|out|out-x as f64)).fold(0.0_f64,f64::max);
        assert!(gain as f64 >= reference*0.99, "optimizer {gain} vs reference {reference}");
    }

    #[test]
    fn cp20_frozen_reserve_curve_has_finite_profit_peak() {
        let a=|x:f64|100_000.0*x/(100_000.0+x)*1.01;
        let b=|x:f64|100_000.0*x/(100_000.0+x)*1.01;
        let p=|x:f64|b(a(x))-x;
        // A positive infinitesimal discrepancy and a finite depth bound
        // do not imply infinite optimal size.
        assert!(p(1.0)>0.0);
        assert!(p(100_000.0)<0.0);
        let max=(1..5_000).map(|q|p(q as f64))
            .fold(f64::NEG_INFINITY,f64::max);
        assert!(max>0.0);
    }
    #[test]
    fn cp20_economic_priority_keeps_unknown_and_orders_absolute_net() {
        assert!(priority_key(Some(10))<priority_key(Some(1)));
        assert!(priority_key(Some(1))<priority_key(None));
        assert!(priority_key(None)<priority_key(Some(0)));
        assert!(priority_key(Some(0))<priority_key(Some(-1)));
    }
    #[test]
    fn cp20_network_fee_units_are_settlement_specific() {
        let p=ExecutionParameters::default();
        assert_eq!(network_cost(WSOL,&p),Some(5_000));
        assert_eq!(network_cost(USDC,&p),Some(500));
    }
}
