//! Raydium CPMM directed marginal fee model. No route selection or amount optimization.
//! Layouts: raydium-io/raydium-cp-swap programs/cp-swap/src/states/{config,pool}.rs.
//! Fee denominator is 1_000_000 (not the AMM-v4 denominator).
use anyhow::{Result,ensure};
const DEN:f64=1_000_000.0;
const CONFIG_DISC:[u8;8]=[218,244,33,104,203,203,43,111];

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(super) struct Fees {pub trade:u64,pub creator:u64}

fn at(data:&[u8],offset:usize)->Result<u64>{
    let bytes=data.get(offset..offset+8).ok_or_else(||anyhow::anyhow!("CPMM config truncated"))?;
    Ok(u64::from_le_bytes(bytes.try_into()?))
}
pub(super) fn config(data:&[u8],owner:&str,program:&str)->Result<Fees>{
    ensure!(owner==program && data.len()==236 && data.starts_with(&CONFIG_DISC),
        "Raydium CPMM config owner/discriminator/length mismatch");
    let trade=at(data,12)?;
    let creator=at(data,108)?;
    ensure!(trade<1_000_000 && creator<1_000_000 && trade+creator<1_000_000,
        "Raydium CPMM invalid fee rates");
    Ok(Fees{trade,creator})
}

/// PoolState fragment starts at status byte (absolute byte offset 329).
/// creator_fee_on is at offset 389, enable_creator_fee at 390.
pub(super) fn pool_flags(data:&[u8],offset:usize)->Result<(u8,u8,bool)>{
    let get=|at:usize|->Result<u8>{Ok(*data.get(offset+at).ok_or_else(||anyhow::anyhow!("CPMM flags truncated"))?)};
    let status=get(0)?;
    let mode=get(60)?;
    let enabled=get(61)?;
    ensure!(mode<=2 && enabled<=1,"Raydium CPMM invalid creator-fee mode/flag");
    Ok((status,mode,enabled==1))
}
pub(super) fn marginal(native_price:f64,fees:Fees,mode:u8,creator_enabled:bool,status:u8)->Result<[f64;2]>{
    ensure!(status&4==0,"Raydium CPMM swap disabled");
    ensure!(native_price.is_finite() && native_price>0.0 && mode<=2,"invalid CPMM price/fee mode");
    let trade=fees.trade as f64/DEN;
    let creator=if creator_enabled {fees.creator as f64/DEN}else{0.0};
    let rate=|creator_on_input:bool|{
        if creator_on_input {1.0-trade-creator}
        else {(1.0-trade)*(1.0-creator)}
    };
    let first=rate(mode==0||mode==1);
    let second=rate(mode==0||mode==2);
    ensure!(first>0.0 && second>0.0,"Raydium CPMM nonpositive fee-adjusted rate");
    Ok([native_price*first,second/native_price])
}
/// Frozen, base-input Raydium cp-swap quote for comparison against V10's
/// actual chosen amount. Based on CurveCalculator::swap_base_input and
/// ConstantProductCurve::swap_base_input_without_fees. No amount search or veto.
pub(crate) fn quote_base_input(
    state:&crate::core::market_state::CpmmQuoteSnapshot,
    input_mint:&str,output_mint:&str,amount:u64,
)->std::result::Result<u64,&'static str>{
    if amount==0{return Err("cpmm_zero_input");}
    let (rin,rout,fee_on_input)=if input_mint==state.mint_0&&output_mint==state.mint_1 {
        (state.reserve_0_raw,state.reserve_1_raw,state.creator_fee_on_input_0_to_1)
    }else if input_mint==state.mint_1&&output_mint==state.mint_0 {
        (state.reserve_1_raw,state.reserve_0_raw,state.creator_fee_on_input_1_to_0)
    }else{return Err("cpmm_direction_mismatch")};
    if rin==0||rout==0{return Err("cpmm_empty_reserves");}
    let creator=state.creator_fee_rate;
    let rate=state.trade_fee_rate.checked_add(if fee_on_input{creator}else{0})
        .ok_or("cpmm_fee_overflow")?;
    const DEN:u128=1_000_000;
    if rate as u128>=DEN||creator as u128>=DEN{return Err("cpmm_fee_invalid");}
    // Raydium takes a single ceil-rounded fee on the combined input rate.
    let fee=u128::from(amount).checked_mul(u128::from(rate))
        .ok_or("cpmm_fee_overflow")?.div_ceil(DEN);
    let net=u128::from(amount).checked_sub(fee).ok_or("cpmm_input_exhausted")?;
    if net==0{return Err("cpmm_input_exhausted");}
    let numerator=net.checked_mul(u128::from(rout)).ok_or("cpmm_amount_overflow")?;
    let denominator=u128::from(rin).checked_add(net).ok_or("cpmm_amount_overflow")?;
    let gross=numerator/denominator;
    if gross==0{return Err("cpmm_zero_output");}
    let creator_output_fee=if fee_on_input{0}else{
        gross.checked_mul(u128::from(creator)).ok_or("cpmm_fee_overflow")?.div_ceil(DEN)
    };
    u64::try_from(gross.checked_sub(creator_output_fee)
        .ok_or("cpmm_creator_fee_exceeds_output")?).map_err(|_|"cpmm_amount_overflow")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cp13_directional_creator_fee_and_disabled_fee(){
        let fees=Fees{trade:2500,creator:1000};
        let r=marginal(2.0,fees,1,true,0).unwrap();
        assert!((r[0]-2.0*0.9965).abs()<1e-12);
        assert!((r[1]-0.5*0.9975*0.999).abs()<1e-12);
        let disabled=marginal(2.0,fees,1,false,0).unwrap();
        assert!((disabled[0]-2.0*0.9975).abs()<1e-12);
        assert!((disabled[1]-0.5*0.9975).abs()<1e-12);
        assert!(marginal(2.0,fees,1,true,4).is_err());
    }
    #[test]
    fn cp13_fixed_amount_cpmm_calibration_exact_base_input_rounding(){
        use crate::core::market_state::CpmmQuoteSnapshot;
        let mut state=CpmmQuoteSnapshot{
            mint_0:"A".into(),mint_1:"B".into(),reserve_0_raw:1_000_000,
            reserve_1_raw:2_000_000,trade_fee_rate:2500,creator_fee_rate:1000,
            creator_fee_on_input_0_to_1:true,creator_fee_on_input_1_to_0:false,
        };
        // Input A -> B: ceil(10_000 * 3500 / 1e6)=35, then CPMM floor.
        let expected=(9965_u128*2_000_000/(1_000_000+9965)) as u64;
        assert_eq!(quote_base_input(&state,"A","B",10_000).unwrap(),expected);
        // B -> A: trade fee on input, creator ceil fee on gross output.
        let gross=(9975_u128*1_000_000/(2_000_000+9975)) as u64;
        let creator=(u128::from(gross)*1000).div_ceil(1_000_000) as u64;
        assert_eq!(quote_base_input(&state,"B","A",10_000).unwrap(),gross-creator);
        state.creator_fee_rate=0;
        assert_eq!(quote_base_input(&state,"B","A",10_000).unwrap(),gross);
        assert_eq!(quote_base_input(&state,"A","X",10_000),Err("cpmm_direction_mismatch"));
        assert_eq!(quote_base_input(&state,"A","B",0),Err("cpmm_zero_input"));
    }
    #[test]
    fn cp13_frozen_cpmm_quote_rounding_boundary(){
        use crate::core::market_state::CpmmQuoteSnapshot;
        let state=CpmmQuoteSnapshot{mint_0:"A".into(),mint_1:"B".into(),reserve_0_raw:1_000_000,
            reserve_1_raw:2_000_000,trade_fee_rate:1000,creator_fee_rate:1000,
            creator_fee_on_input_0_to_1:false,creator_fee_on_input_1_to_0:true};
        // Tiny output is rounded away by output-side creator fee.
        assert_eq!(quote_base_input(&state,"A","B",1).unwrap(),0);
        // Nonzero input with fee >= input yields Unknown, never a spurious proof.
        let large_fee=CpmmQuoteSnapshot{trade_fee_rate:999_999,..state};
        assert_eq!(quote_base_input(&large_fee,"B","A",1),Err("cpmm_fee_invalid"));
    }
    #[test]
    fn cp13_pool_mode_layout_and_config(){
        let mut pool=[0_u8;84];pool[60]=2;pool[61]=1;
        assert_eq!(pool_flags(&pool,0).unwrap(),(0,2,true));
        let mut raw=[0_u8;637];raw[389]=1;raw[390]=1;
        assert_eq!(pool_flags(&raw,329).unwrap(),(0,1,true));
        let mut config_raw=[0_u8;236];config_raw[..8].copy_from_slice(&CONFIG_DISC);
        config_raw[12..20].copy_from_slice(&2500_u64.to_le_bytes());
        config_raw[108..116].copy_from_slice(&1000_u64.to_le_bytes());
        let p="CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
        assert_eq!(config(&config_raw,p,p).unwrap(),Fees{trade:2500,creator:1000});
        assert!(config(&config_raw,"unknown",p).is_err());
    }
}
