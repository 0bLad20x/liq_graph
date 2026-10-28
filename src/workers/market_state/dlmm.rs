//! MarketState-owned DLMM depth. Native integer amounts; SDK pin: docs/CP8B.md.
//! No RPC, Registry lookup, sizing optimizer or persistent liquidity mirror.
use std::sync::Arc;
use anyhow::{Context,Result,ensure,bail};
use solana_pubkey::Pubkey;
use base64::Engine as _;
use crate::core::{ExecutionManifest,market_state::{PoolLiquidity,DirectedLiquidity,LiquidityLevel}};
use super::{bytes,key,u64_le,u128_le,i32_le,fees,DLMM_PROGRAM,DLMM_DISC};

const ARRAY_DISC:[u8;8]=[92,142,92,220,5,148,70,181];
const BITMAP_DISC:[u8;8]=[80,111,124,113,55,237,18,5];
const PRECISION:u128=1_000_000_000;
const Q64:f64=18_446_744_073_709_551_616.0;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(super) struct Dependencies {
    pub arrays:[Pubkey;6],pub count:u8,pub bitmap:Option<Pubkey>,
    pub digest:[u8;32],
}
impl Dependencies {
    pub fn from_manifest(m:&ExecutionManifest)->Result<Self> {
        let program:Pubkey=DLMM_PROGRAM.parse()?;
        ensure!(m.base_a.first().is_some_and(|a|a.address==program),"DLMM manifest program mismatch");
        let root=m.base_a.iter().position(|a|a.address==m.pool).context("DLMM manifest root missing")?;
        let mut tail=&m.base_a.get(root+4..).context("DLMM manifest vault/oracle block missing")?[..];
        ensure!(m.base_b.first().is_some_and(|a|a.address==program),"DLMM reverse manifest program mismatch");
        let reverse_root=m.base_b.iter().position(|a|a.address==m.pool).context("DLMM reverse manifest root missing")?;
        let reverse_tail=m.base_b.get(reverse_root+4..).context("DLMM reverse manifest vault/oracle block missing")?;
        ensure!(tail.len()==reverse_tail.len() && tail.iter().zip(reverse_tail).all(|(a,b)|
            a.address==b.address && a.writable==b.writable),"DLMM directional array windows differ");
        let bitmap_address=Pubkey::find_program_address(&[b"bitmap",m.pool.as_ref()],&program).0;
        let bitmap=if tail.first().is_some_and(|a|a.address==bitmap_address) {
            tail=&tail[1..];Some(bitmap_address)
        }else{None};
        ensure!((1..=6).contains(&tail.len()),"DLMM execution window must contain 1..=6 arrays");
        let mut arrays=[Pubkey::default();6];
        for (i,a) in tail.iter().enumerate() {
            ensure!(a.writable && !arrays[..i].contains(&a.address),"invalid DLMM array dependency");
            arrays[i]=a.address;
        }
        Ok(Self{arrays,count:tail.len() as u8,bitmap,digest:m.account_set_digest()})
    }
    pub fn account_cost(self)->usize{1+usize::from(self.count)+usize::from(self.bitmap.is_some())}
    pub fn addresses(&self)->&[Pubkey]{&self.arrays[..usize::from(self.count)]}
}

#[derive(Clone,Debug)]
struct Bin {id:i32,x:u64,y:u64,price:u128,processed:u64,open:u64,ask:bool}
#[derive(Clone,Debug)]
struct Array {index:i32,version:u8,bins:Vec<Bin>,raw:Arc<[u8]>}
#[derive(Clone)]
pub(super) struct Book {
    pool:Pubkey,pub dependencies:Dependencies,raw_pair:Arc<[u8]>,
    arrays:[Option<Arc<Array>>;6],extension:Option<Arc<[u8]>>,
    pub profile:Option<Arc<PoolLiquidity>>,
}
impl Book {
    pub fn new(pool:Pubkey,dependencies:Dependencies,raw:&[u8])->Result<Self> {
        ensure!(raw.len()==904 && raw.starts_with(&DLMM_DISC),"invalid depth LbPair");
        Ok(Self{pool,dependencies,raw_pair:Arc::from(raw),arrays:std::array::from_fn(|_|None),
            extension:None,profile:None})
    }
    pub fn pair(&mut self,raw:&[u8])->Result<()> {
        ensure!(raw.len()==904 && raw.starts_with(&DLMM_DISC),"invalid depth LbPair update");
        self.raw_pair=Arc::from(raw);Ok(())
    }
    pub fn array(&mut self,position:u8,address:Pubkey,raw:&[u8])->Result<()> {
        let position=usize::from(position);
        ensure!(self.dependencies.addresses().get(position)==Some(&address),"unexpected DLMM array address");
        ensure!(raw.len()==56+70*144 && raw.starts_with(&ARRAY_DISC),"invalid DLMM BinArray layout");
        ensure!(key(raw,24)?==self.pool.to_string(),"DLMM BinArray parent changed");
        let index=i64::from_le_bytes(bytes(raw,8,8)?.try_into()?);
        let index=i32::try_from(index).context("DLMM array index overflow")?;
        ensure!((-6656..=6655).contains(&index),"DLMM array outside supported bitmap");
        let program:Pubkey=DLMM_PROGRAM.parse()?;
        ensure!(Pubkey::find_program_address(&[b"bin_array",self.pool.as_ref(),&i64::from(index).to_le_bytes()],&program).0==address,
            "DLMM array PDA/index mismatch");
        let version=raw[16];ensure!(version<=3,"unsupported DLMM BinArray version");
        let mut bins=Vec::with_capacity(70);
        for i in 0..70 {
            let p=56+i*144;let x=u64_le(raw,p)?;let y=u64_le(raw,p+8)?;
            let price=u128_le(raw,p+16)?;
            let processed=u64_le(raw,p+128)?;let open=u64_le(raw,p+112)?;
            ensure!(raw[p+140]<=1,"invalid DLMM order side");
            ensure!(price>0 || (x==0&&y==0&&processed==0&&open==0),"DLMM nonempty bin has zero price");
            bins.push(Bin{id:index*70+i as i32,x,y,price,processed,open,ask:raw[p+140]!=0});
        }
        self.arrays[position]=Some(Arc::new(Array{index,version,bins,raw:Arc::from(raw)}));Ok(())
    }
    pub fn bitmap(&mut self,raw:&[u8])->Result<()> {
        ensure!(raw.len()==1576 && raw.starts_with(&BITMAP_DISC) && key(raw,8)?==self.pool.to_string(),
            "invalid DLMM bitmap extension");
        self.extension=Some(Arc::from(raw));Ok(())
    }
    fn initialized(&self)->Result<Vec<i32>> {
        let mut out=Vec::new();
        fn append(bytes:&[u8],mut convert:impl FnMut(usize)->i32,out:&mut Vec<i32>) {
            for (i,b) in bytes.iter().enumerate(){for bit in 0..8 {if b&(1<<bit)!=0 {out.push(convert(8*i+bit));}}}
        }
        append(bytes(&self.raw_pair,584,128)?,|i|i as i32-512,&mut out);
        if self.dependencies.bitmap.is_some() {
            let extension=self.extension.as_ref().context("DLMM bitmap baseline unavailable")?;
            append(bytes(extension,40,768)?,|i|i as i32+512,&mut out);
            // Negative extension is twelve chunks of 512 bits, reversed within
            // each range by the protocol's index mapping: -513, -514, ... .
            append(bytes(extension,808,768)?,|i|-513-i as i32,&mut out);
        }
        out.sort_unstable();Ok(out)
    }
    fn limit_orders(&self)->Result<bool> {
        Ok(match self.raw_pair[35] {
            0=>bytes(&self.raw_pair,264,32)?.iter().all(|v|*v==0)
                &&bytes(&self.raw_pair,408,32)?.iter().all(|v|*v==0),
            1=>false,2=>true,_=>bail!("unsupported DLMM function type"),
        })
    }
    fn direction(&self,active:i32,step:u16,fee:&fees::Dlmm,clock:i64,
        a:&str,b:&str,da:u8,db:u8,forward:bool)->DirectedLiquidity {
        let (input,output,di,do_) = if forward {(a,b,da,db)}else{(b,a,db,da)};
        let mut out=DirectedLiquidity{input_mint:input.into(),output_mint:output.into(),input_decimals:di,output_decimals:do_,
            status:"unknown",coverage:"invalid",next_required_account:None,first_source_position:None,
            marginal_net_rate:None,monotone_rate_tail:true,input_capacity_raw:None,output_capacity_raw:"0".into(),levels:Vec::new(),reason:None};
        let calculated=(||->Result<()>{
            let mut initialized=self.initialized()?;
            let orders=self.limit_orders()?;
            if forward {initialized.reverse();}
            let current=active.div_euclid(70);
            let mut input_total=Some(0_u128);let mut output_total=0_u128;
            out.coverage="exhausted_bitmap";
            for index in initialized.into_iter().filter(|i|if forward{*i<=current}else{*i>=current}) {
                let array=self.arrays.iter().flatten().find(|a|a.index==index);
                let Some(array)=array else {
                    let program:Pubkey=DLMM_PROGRAM.parse()?;
                    out.next_required_account=Some(Pubkey::find_program_address(
                        &[b"bin_array",self.pool.as_ref(),&i64::from(index).to_le_bytes()],&program).0.to_string());
                    out.coverage=if self.arrays.iter().flatten().count()==usize::from(self.dependencies.count) {
                        "execution_window_end"
                    }else{"account_unavailable"};
                    break;
                };
                let mut bins=array.bins.iter().collect::<Vec<_>>();
                if forward{bins.reverse();}
                for bin in bins.into_iter().filter(|bin|if forward{bin.id<=active}else{bin.id>=active}) {
                    let mm=if forward{bin.y}else{bin.x};
                    let has_order=orders && if forward{!bin.ask}else{bin.ask};
                    let parts=[mm,if has_order{bin.processed}else{0},if has_order{bin.open}else{0}];
                    let total=parts.iter().try_fold(0_u64,|n,v|n.checked_add(*v)).context("DLMM output sum overflow")?;
                    if total==0{continue;}
                    ensure!(bin.price>0,"DLMM price unavailable");
                    let (_,fee_units,on_input)=fee.rate_units_at(active,bin.id,step,clock)?;
                    let rate=fee_units as f64/PRECISION as f64;
                    let fees_on_input=on_input[usize::from(!forward)];
                    let input=parts.iter().try_fold(0_u128,|n,v| {
                        let cost=amount_in(*v,bin.price,forward)?;
                        n.checked_add(cost).context("DLMM input sum overflow")
                    })?;
                    let full_input=u64::try_from(input).ok();
                    let included=if fees_on_input {full_input.and_then(|n|included_fee(n,fee_units))}else{full_input};
                    let output_raw=if fees_on_input{total}else{total-excluded_fee(total,fee_units)};
                    let fee_raw=if fees_on_input{included.zip(full_input).map(|(a,b)|a-b)}else{Some(total-output_raw)};
                    let needed_output=if fees_on_input {1}else {included_fee(1,fee_units).context("DLMM minimum output fee overflow")?};
                    let minimum=if output_raw==0{None}else{u64::try_from(minimum_cost(&parts,needed_output,bin.price,forward)?).ok()};
                    let minimum=if fees_on_input {minimum.and_then(|n|included_fee(n,fee_units))}else{minimum};
                    let native=bin.price as f64/Q64;
                    let gross=if forward{native}else{1.0/native};
                    let gross=gross*10_f64.powi(i32::from(di)-i32::from(do_));
                    let net=gross*(1.0-rate);
                    let full_level_net_rate=included.filter(|n|*n>0).map(|n|
                        output_raw as f64/n as f64*10_f64.powi(i32::from(di)-i32::from(do_)));
                    ensure!(net.is_finite()&&net>0.0,"DLMM reachable net price invalid");
                    if output_raw>0 && minimum.is_none(){out.coverage="amount_unrepresentable";break;}
                    input_total=input_total.zip(included).and_then(|(n,v)|n.checked_add(u128::from(v)));
                    output_total=output_total.checked_add(u128::from(output_raw)).context("DLMM output capacity overflow")?;
                    out.levels.push(LiquidityLevel{source_position:bin.id,gross_rate:gross,net_rate:net,fee_rate:rate,
                        rate_numerator:if forward{bin.price}else{1_u128<<64},
                        rate_denominator:if forward{1_u128<<64}else{bin.price},
                        output_parts_raw:parts,fee_numerator:fee_units,fee_denominator:PRECISION as u64,
                        maximum_rebate_bps:if parts[1]==0&&parts[2]==0{super::u16_le(&self.raw_pair,32)?.min(10000)}else{10000},
                        fees_on_input,full_level_net_rate,input_raw:included,output_raw,minimum_input_raw:minimum,fee_raw});
                }
                if out.coverage=="amount_unrepresentable"{break;}
            }
            out.input_capacity_raw=input_total.map(|n|n.to_string());out.output_capacity_raw=output_total.to_string();
            if let Some(first)=out.levels.iter().find(|l|l.output_raw>0) {
                out.status="available";out.first_source_position=Some(first.source_position);out.marginal_net_rate=Some(first.net_rate);
            }else{
                out.status=if out.coverage=="exhausted_bitmap"{"empty"}else{"uncovered"};
                out.reason=Some(if out.status=="empty"{"no positive integer output in this direction"}
                    else{"no reachable output established before observation boundary"}.into());
            }
            Ok(())
        })();
        if let Err(error)=calculated {
            out.status="unknown";out.coverage="invalid";out.levels.clear();out.marginal_net_rate=None;
            out.first_source_position=None;out.input_capacity_raw=None;out.output_capacity_raw="0".into();
            out.reason=Some(error.to_string());
        }
        out
    }
    pub fn rebuild(&mut self,active:i32,step:u16,fee:&fees::Dlmm,clock:i64,
        a:&str,b:&str,da:u8,db:u8,canonical_base:&str)->bool {
        let mut directions=[self.direction(active,step,fee,clock,a,b,da,db,true),
            self.direction(active,step,fee,clock,a,b,da,db,false)];
        if a!=canonical_base {directions.swap(0,1);}
        let mut next=PoolLiquidity{model:"dlmm_execution_window_v2",revision:0,content_digest:[0;32],
            account_set_digest:self.dependencies.digest,
            execution_accounts:self.dependencies.addresses().iter().map(ToString::to_string).collect(),reserve_curve:None,directions};
        next.content_digest=super::liquidity_digest(&next);
        if let Some(previous)=&self.profile {
            next.revision=previous.revision;
            if previous.content_digest==next.content_digest{return false;}
            next.revision=next.revision.saturating_add(1);
        }else{next.revision=1;}
        self.profile=Some(Arc::new(next));true
    }
    pub fn evidence(&self)->serde_json::Value {
        let encode=|v:&[u8]|base64::engine::general_purpose::STANDARD.encode(v);
        serde_json::json!({"pool":self.pool.to_string(),"lb_pair_base64":encode(&self.raw_pair),
            "protocol_fee_x_raw":u64_le(&self.raw_pair,216).ok(),"protocol_fee_y_raw":u64_le(&self.raw_pair,224).ok(),
            "active_id":i32_le(&self.raw_pair,76).ok(),"bin_step":super::u16_le(&self.raw_pair,80).ok(),
            "arrays":self.arrays.iter().enumerate().filter_map(|(i,a)|a.as_ref().map(|a|serde_json::json!({
                "address":self.dependencies.arrays[i].to_string(),"index":a.index,"version":a.version,"data_base64":encode(&a.raw)}))).collect::<Vec<_>>(),
            "bitmap_extension_base64":self.extension.as_ref().map(|v|encode(v)),"profile":self.profile,
            "complete_levels":self.profile.as_ref().map(|p|p.directions.iter().map(|d|&d.levels).collect::<Vec<_>>())})
    }
}

// Q64 multiply/divide without a new bigint crate. u64 * u128 is decomposed
// before shifting; no floating-point value enters integer fill/fee arithmetic.
fn amount_in(output:u64,price:u128,forward:bool)->Result<u128> {
    ensure!(price>0,"DLMM zero Q64 price");
    if output==0{return Ok(0);}
    if forward {Ok((u128::from(output)<<64).div_ceil(price))}
    else {
        let low=u128::from(output)*u128::from(price as u64);
        let high=u128::from(output)*(price>>64);
        high.checked_add(low>>64).and_then(|n|n.checked_add(u128::from(low as u64!=0)))
            .context("DLMM Q64 input overflow")
    }
}
fn minimum_cost(parts:&[u64;3],mut output:u64,price:u128,forward:bool)->Result<u128> {
    let mut input=0_u128;
    for &capacity in parts {
        let take=capacity.min(output);input=input.checked_add(amount_in(take,price,forward)?)
            .context("DLMM minimum input overflow")?;output-=take;
        if output==0{return Ok(input);}
    }
    bail!("DLMM minimum output exceeds bin capacity")
}
fn included_fee(excluded:u64,fee:u64)->Option<u64> {
    if u128::from(fee)>=PRECISION{return None;}
    u64::try_from((u128::from(excluded)*PRECISION).div_ceil(PRECISION-u128::from(fee))).ok()
}
fn excluded_fee(included:u64,fee:u64)->u64 {
    (u128::from(included)*u128::from(fee)).div_ceil(PRECISION) as u64
}
