//! CP12 differential tests: optimized evaluator versus frozen CP10 evaluator.
//! All economic decisions, integer values and reasons must match; only timing
//! may differ. No networking, signing, provider configuration or live data.
use super::{amounts,amounts_cp12_reference};
use std::sync::Arc;
use crate::core::market_state::{
    Consistency,DirectedLiquidity,FeeValuation,LiquidityLevel,
    PoolLiquidity,PoolPriceObservation,PriceKind,PriceOrigin,ReserveCurve,
};

fn level(position:i32,numer:u128,denom:u128,fees_on_input:bool)->LiquidityLevel {
    LiquidityLevel {
        source_position:position,gross_rate:1.0,net_rate:1.0,fee_rate:0.003,
        fees_on_input,rate_numerator:numer,rate_denominator:denom,
        output_parts_raw:[1000,3000,5000],fee_numerator:30,fee_denominator:10000,
        maximum_rebate_bps:5000,full_level_net_rate:Some(1.0),
        input_raw:Some(20_000),output_raw:9000,minimum_input_raw:Some(1),
        fee_raw:Some(10),
    }
}
pub(super) fn pool(id:u8,kind:u8,levels:usize,coverage:&'static str,fee_verified:bool)->Arc<PoolPriceObservation>{
    let (a,b)=("SOL","USD");
    let level_vector=(0..levels).map(|i|{
        let n=match kind {2=>u128::MAX-100,3=>0,_=>if id%2==0{3}else{2}};
        level(i as i32,n,if kind==3{0}else{1},i%2==0)
    }).collect::<Vec<_>>();
    let directed=|direction:usize|{
        let (input_mint,output_mint)=if direction==0 {(a,b)} else {(b,a)};
        DirectedLiquidity{
            input_mint:input_mint.into(),output_mint:output_mint.into(),
            input_decimals:9,output_decimals:6,status:if kind==4{"unknown"}else{"available"},
            coverage,next_required_account:None,first_source_position:Some(0),
            marginal_net_rate:Some(1.001),monotone_rate_tail:true,
            input_capacity_raw:Some("1000000".into()),output_capacity_raw:"9000".into(),
            levels:level_vector.clone(),reason:None,
        }
    };
    let reserve_curve=if kind==1 {Some(ReserveCurve{
        base_mint:a.into(),base_reserve_raw:1_000_000,
        effective_quote_reserve_raw:2_000_000,
        spendable_quote_reserve_raw:1_900_000,
        fee_bps:[25,5,7],buy_input_padding_raw:2,
    })}else{None};
    let liquidity=Arc::new(PoolLiquidity{
        model:"test",revision:1,content_digest:[id;32],account_set_digest:[id;32],
        execution_accounts:Vec::new(),reserve_curve,cpmm_quote:None,
        directions:[directed(0),directed(1)],
    });
    Arc::new(PoolPriceObservation{
        origin:PriceOrigin::Baseline,pool:format!("test-pool-{id}"),venue:"test",
        mint_a:a.into(),mint_b:b.into(),base_mint:a.into(),quote_mint:b.into(),
        price_quote_per_base:1.0,
        valuation:Some(FeeValuation{
            model:"test",estimated_net_rates:[Some(1.0);2],
            dex_fee_rates:[Some(0.001);2],fees_on_input:[false;2],
            base_fee_rate:None,variable_fee_rate:None,lp_fee_bps:Some(25),
            protocol_fee_bps:Some(5),creator_fee_bps:Some(7),
            fee_schedule:None,market_cap_raw:None,
            chain_timestamp:1,chain_epoch:1,
            token_fee_status:if fee_verified{
                ["verified_no_standard_transfer_fee";2]
            }else{["unknown";2]},
            unknown_reason:None,scope:"test",
        }),
        liquidity:Some(liquidity),previous_price:None,delta_bps:None,
        price_kind:PriceKind::DlmmActiveBin,best_bid_quote_per_base:Some(1.0),
        best_ask_quote_per_base:Some(1.0),consistency:Consistency::SingleAccount,
        slot:1,received_at_us:1,sequence:1,component_slot_min:1,
        component_slot_max:1,component_received_at_us_min:1,
        component_received_at_us_max:1,trigger_field:None,pipeline:None,
    })
}
fn compare_results(new:amounts::Assessment,old:amounts_cp12_reference::Assessment){
    let mut a=serde_json::to_value(&new).unwrap();
    let mut b=serde_json::to_value(&old).unwrap();
    a.as_object_mut().unwrap().remove("elapsed_ns");
    b.as_object_mut().unwrap().remove("elapsed_ns");
    assert_eq!(a,b,"CP12 assessment differs from the frozen CP11 reference");
}
pub(super) fn cases()->Vec<(Vec<Arc<PoolPriceObservation>>,Vec<bool>)> {
    let mut cases=Vec::new();
    for kind in [0_u8,1,2,3,4] {
        for levels in [0_usize,1,2,6,24] {
            for fee_verified in [false,true] {
                let first=pool(1,kind,levels,"exhausted_bitmap",fee_verified);
                let second=pool(2,kind,levels,"execution_window_end",fee_verified);
                cases.push((vec![first.clone(),second.clone()],vec![true,true]));
                cases.push((vec![first,second],vec![true,false]));
            }
        }
    }
    cases
}
#[test]
fn cp12_exact_quantity_decisions_against_cp10() {
    for (observations,manifest_matches) in cases() {
        let modern=observations.iter().zip(&manifest_matches).enumerate().map(|(i,(o,&valid))|
            amounts::Input{slot:i,observation:o.clone(),manifest_matches:valid}).collect::<Vec<_>>();
        let previous=observations.iter().zip(&manifest_matches).enumerate().map(|(i,(o,&valid))|
            amounts_cp12_reference::Input{slot:i,observation:o.clone(),manifest_matches:valid}).collect::<Vec<_>>();
        for settlement in ["SOL","USD"] {
            for minimum_profit in [0_u64,1,1000,u64::MAX] {
                for other in [None,Some("SOL"),Some("USD")] {
                    compare_results(
                        amounts::assess(&modern,settlement,minimum_profit,other),
                        amounts_cp12_reference::assess(&previous,settlement,minimum_profit,other),
                    );
                }
            }
        }
        for i in 0..modern.len() {
            for direction in 0..2 {
                for quantity in [0,1,2,10,999,1000,9999,1_000_000,u64::MAX] {
                    assert_eq!(
                        format!("{:?}",amounts::quote(&modern[i],direction,quantity)),
                        format!("{:?}",amounts_cp12_reference::quote(&previous[i],direction,quantity)));
                    assert_eq!(
                        format!("{:?}",amounts::minimum_input(&modern[i],direction,quantity)),
                        format!("{:?}",amounts_cp12_reference::minimum_input(&previous[i],direction,quantity)));
                }
            }
        }
        for route in [vec![("SOL".into(),"USD".into()),("USD".into(),"SOL".into())],
                      vec![("USD".into(),"SOL".into()),("SOL".into(),"USD".into())]] {
            assert_eq!(
                amounts::seed(&modern,&route),
                amounts_cp12_reference::seed(&previous,&route));
        }
    }
}
#[test]
#[ignore="CP12 local release-mode microbenchmark only"]
fn cp12_frozen_amount_benchmark() {
    let observations=vec![pool(1,0,24,"exhausted_bitmap",true),
                          pool(2,0,24,"exhausted_bitmap",true)];
    let modern=observations.iter().enumerate().map(|(i,o)|
        amounts::Input{slot:i,observation:o.clone(),manifest_matches:true}).collect::<Vec<_>>();
    let previous=observations.iter().enumerate().map(|(i,o)|
        amounts_cp12_reference::Input{slot:i,observation:o.clone(),manifest_matches:true}).collect::<Vec<_>>();
    let iterations=5000_u32;
    let now=std::time::Instant::now();
    let mut legacy=String::new();
    for _ in 0..iterations {legacy=amounts_cp12_reference::assess(&previous,"SOL",0,None).reason.into();}
    let legacy_ns=now.elapsed().as_nanos();
    let now=std::time::Instant::now();
    let mut optimized=String::new();
    for _ in 0..iterations {optimized=amounts::assess(&modern,"SOL",0,None).reason.into();}
    let optimized_ns=now.elapsed().as_nanos();
    assert_eq!(legacy,optimized);
    println!("CP12 frozen evaluator: iterations={iterations} legacy_ns={legacy_ns} optimized_ns={optimized_ns} speedup={:.3}",
        legacy_ns as f64/optimized_ns.max(1) as f64);
}


#[test]
fn cp12_cached_quote_trace_matches_uncached_cp11() {
    // Vary input quantities across all directional level/boundary cases;
    // check repeating identical requests and interspersed other quantities.
    let mut quantities=vec![0_u64,1,2,3,5,10,29,30,31,100,999,1000,1001,2999,3000,
        4999,5000,5001,8999,9000,9001,9999,10000,10001,65_535,1_000_000,u64::MAX];
    let mut state=0x42a7_fab1_1234_5678_u64;
    for _ in 0..384 {
        state^=state<<13;state^=state>>7;state^=state<<17;
        quantities.push(state);
    }
    quantities.extend_from_within(0..27);
    for (observations,validities) in cases() {
        let new=observations.iter().zip(&validities).enumerate().map(|(i,(o,&valid))|
            amounts::Input{slot:i,observation:o.clone(),manifest_matches:valid}).collect::<Vec<_>>();
        let old=observations.iter().zip(&validities).enumerate().map(|(i,(o,&valid))|
            amounts_cp12_reference::Input{slot:i,observation:o.clone(),manifest_matches:valid}).collect::<Vec<_>>();
        for pool_idx in 0..new.len(){
            for direction in 0..2 {
                let cached=amounts::cp12_cached_quote_trace(&new,pool_idx,direction,&quantities);
                for (quantity,result) in quantities.iter().zip(cached){
                    let previous=amounts_cp12_reference::quote(&old[pool_idx],direction,*quantity);
                    assert_eq!(format!("{result:?}"),format!("{previous:?}"),
                        "pool={pool_idx} direction={direction} amount={quantity}");
                }
            }
        }
    }
}
#[test]
#[ignore="CP12 same-frozen-input release-mode benchmark; informational, not a performance assertion"]
fn cp12_frozen_quote_benchmark() {
    let observations=vec![pool(1,0,24,"exhausted_bitmap",true),
                          pool(2,0,24,"exhausted_bitmap",true)];
    let fresh=observations.iter().enumerate().map(|(i,o)|
        amounts::Input{slot:i,observation:o.clone(),manifest_matches:true}).collect::<Vec<_>>();
    let prev=observations.iter().enumerate().map(|(i,o)|
        amounts_cp12_reference::Input{slot:i,observation:o.clone(),manifest_matches:true}).collect::<Vec<_>>();
    let loops=3000_usize;
    let start=std::time::Instant::now();
    let mut legacy=0_u64;
    for _ in 0..loops {
        let result=amounts_cp12_reference::assess(&prev,"SOL",0,None);
        legacy=legacy.wrapping_add(result.probes);
    }
    let old_ns=start.elapsed().as_nanos();
    let start=std::time::Instant::now();
    let mut new=0_u64;
    for _ in 0..loops {
        let result=amounts::assess(&fresh,"SOL",0,None);
        new=new.wrapping_add(result.probes);
    }
    let new_ns=start.elapsed().as_nanos();
    assert_eq!(legacy,new);
    println!("CP12 frozen assessment: iterations={loops} cp11_ns={old_ns} cp12_ns={new_ns} ratio={:.3}",old_ns as f64/new_ns.max(1) as f64);
}


#[test]
#[ignore="CP12 repeated frozen-quote benchmark; equal input sequences, informational"]
fn cp12_repeated_quote_benchmark() {
    // Include partial fills and exhausted levels; the same frozen observation
    // and direction receive many different integer quantities per assessment.
    let observations=vec![pool(1,0,24,"exhausted_bitmap",true)];
    let modern=vec![amounts::Input{slot:0,observation:observations[0].clone(),manifest_matches:true}];
    let previous=vec![amounts_cp12_reference::Input{slot:0,observation:observations[0].clone(),manifest_matches:true}];
    let mut quantities=Vec::new();
    for i in 0..256_u64 {quantities.push((i*983+1)%120_000);}
    let iterations=1000_u64;
    let start=std::time::Instant::now();
    let mut old_checksum=0_u64;
    for _ in 0..iterations {
        let values=quantities.iter().map(|&q|amounts_cp12_reference::quote(&previous[0],0,q))
            .collect::<Vec<_>>();
        for result in values {old_checksum=old_checksum.wrapping_add(result.unwrap_or(0));}
    }
    let old_ns=start.elapsed().as_nanos();
    let start=std::time::Instant::now();
    let mut new_checksum=0_u64;
    for _ in 0..iterations {
        let values=amounts::cp12_cached_quote_trace(&modern,0,0,&quantities);
        for result in values {new_checksum=new_checksum.wrapping_add(result.unwrap_or(0));}
    }
    let new_ns=start.elapsed().as_nanos();
    assert_eq!(new_checksum,old_checksum);
    println!("CP12 repeated frozen quotes: calls={} cp11_ns={} cp12_ns={} speedup={:.3}",
        iterations*quantities.len() as u64,old_ns,new_ns,old_ns as f64/new_ns.max(1) as f64);
}
