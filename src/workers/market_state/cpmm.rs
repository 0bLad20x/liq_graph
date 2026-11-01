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
