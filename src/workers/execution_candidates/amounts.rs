//! Private quantity analysis of typed, frozen observations. No RPC or worker calls.
//! CP8D: primary-route qualification before packing. Bounds overstate profit;
//! estimates use full DEX fees. Failed probes never prove absence. See docs/CP8D.md.
use std::{collections::{BTreeMap,BTreeSet},sync::Arc,time::Instant};
use crate::core::market_state::{PoolPriceObservation,PoolLiquidity,LiquidityLevel};

#[derive(Clone)]
pub(super) struct Input {pub slot:usize,pub observation:Arc<PoolPriceObservation>,pub manifest_matches:bool}
#[derive(Clone,Copy,Debug)]
pub(super) enum Issue {Unknown(&'static str),Insufficient(&'static str)}
type Calc<T>=Result<T,Issue>;
#[derive(Clone,Copy,Debug,PartialEq,Eq,serde::Serialize)]
#[serde(rename_all="snake_case")]
pub(super) enum Decision {Possible,Unknown,Excluded}
#[derive(Clone,serde::Serialize)]
pub(super) struct Leg {pub pool:String,pub slot:usize,pub input_mint:String,pub output_mint:String,pub input_raw:u64,pub output_raw:u64}
#[derive(Clone,serde::Serialize)]
pub(super) struct Estimate {pub input_raw:u64,pub output_raw:u64,pub profit_raw:u64,pub legs:Vec<Leg>}
#[derive(Clone,serde::Serialize)]
pub(super) struct Assessment {
    pub decision:Decision,pub scope:&'static str,pub reason:&'static str,
    pub pool_count:usize,pub probes:u64,pub elapsed_ns:u64,
    pub cost_guard_raw:u64,pub required_profit_raw:u64,
    pub first_probe_raw:Option<u64>,pub last_probe_raw:Option<u64>,
    pub estimate:Option<Estimate>,pub best_below_guard:Option<Estimate>,
    pub gross_output_upper_bound_raw:Option<u64>,
}
impl Assessment {
    pub fn unknown(reason:&'static str)->Self {Self{decision:Decision::Unknown,scope:"frozen_observed_primary_route",
        reason,pool_count:0,probes:0,elapsed_ns:0,cost_guard_raw:0,required_profit_raw:0,
        first_probe_raw:None,last_probe_raw:None,estimate:None,best_below_guard:None,gross_output_upper_bound_raw:None}}
}
#[derive(Default,serde::Serialize)]
pub(super) struct Report {
    pub attempts:u64,pub possible:u64,pub unknown:u64,pub excluded:u64,pub probes:u64,
    pub cache_hits:u64,pub observations_reused:u64,pub unchanged_states:u64,
    pub processing_ns_total:u64,pub processing_ns_max:u64,pub processing_ns_histogram:Vec<u64>,
    pub reasons:BTreeMap<&'static str,u64>,#[serde(skip_serializing)] pub samples:Vec<serde_json::Value>,
}
impl Report {
    pub fn record(&mut self,a:&Assessment) {
        self.attempts+=1;self.probes+=a.probes;self.processing_ns_total+=a.elapsed_ns;
        self.processing_ns_max=self.processing_ns_max.max(a.elapsed_ns);
        if self.processing_ns_histogram.is_empty(){self.processing_ns_histogram.resize(64,0);}
        let bin=if a.elapsed_ns==0{0}else{63-a.elapsed_ns.leading_zeros() as usize};
        self.processing_ns_histogram[bin]+=1;*self.reasons.entry(a.reason).or_default()+=1;
        match a.decision{Decision::Possible=>self.possible+=1,Decision::Unknown=>self.unknown+=1,Decision::Excluded=>self.excluded+=1}
    }
    pub fn sample(&mut self,a:&Assessment,inputs:&[Input],context:serde_json::Value) {
        let label=match a.decision{Decision::Possible=>"qualified",Decision::Unknown=>"unknown",Decision::Excluded=>"excluded"};
        if self.samples.iter().filter(|s|s["kind"]==label).count()<2 {
            self.samples.push(serde_json::json!({"kind":label,"context":context,"assessment":a,
                "models":inputs.iter().map(|i|serde_json::json!({"slot":i.slot,"manifest_matches":i.manifest_matches,
                    "observation":i.observation,"complete_levels":i.observation.liquidity.as_ref()
                        .map(|p|p.directions.iter().map(|d|&d.levels).collect::<Vec<_>>())})).collect::<Vec<_>>()}));
        }
    }
}

#[derive(Clone,Copy)]
struct Edge {pool:usize,direction:usize}
fn profile(input:&Input)->Calc<&PoolLiquidity> {
    if !input.manifest_matches{return Err(Issue::Unknown("manifest_window_changed"));}
    if !input.observation.valuation.as_ref().is_some_and(|v|v.token_fee_status.iter().all(|s|*s=="verified_no_standard_transfer_fee")) {
        return Err(Issue::Unknown("token_transfer_cost_unresolved"));
    }
    input.observation.liquidity.as_deref().ok_or(Issue::Unknown("amount_model_unavailable"))
}
fn ceil_fee(amount:u64,rate:u64,den:u64)->Calc<u64> {
    if den==0||rate>=den{return Err(Issue::Unknown("invalid_fee_fraction"));}
    u64::try_from((u128::from(amount)*u128::from(rate)).div_ceil(u128::from(den))).map_err(|_|Issue::Unknown("fee_overflow"))
}
fn included(amount:u128,level:&LiquidityLevel,optimistic:bool)->Calc<u128> {
    let (rate,den)=(u128::from(level.fee_numerator),u128::from(level.fee_denominator));
    if den==0||rate>=den||level.maximum_rebate_bps>10000{return Err(Issue::Unknown("invalid_fee_fraction"));}
    let full=amount.checked_mul(den).ok_or(Issue::Unknown("amount_overflow"))?.div_ceil(den-rate);
    Ok(if optimistic {full-(full-amount).checked_mul(u128::from(level.maximum_rebate_bps)).ok_or(Issue::Unknown("amount_overflow"))?/10000}else{full})
}

// Exact 64*128 /128 math, with the common u128 product as a fast path.
// The long division fallback has a fixed integer bit width, not a search budget.
pub(super) fn mul_div(a:u64,b:u128,d:u128,up:bool)->Calc<u128> {
    if d==0{return Err(Issue::Unknown("zero_price_denominator"));}
    if let Some(n)=u128::from(a).checked_mul(b){return Ok(if up{n.div_ceil(d)}else{n/d});}
    let low=u128::from(a)*u128::from(b as u64);let high=u128::from(a)*(b>>64);
    let shifted=high<<64;let tail=shifted.wrapping_add(low);
    let head=(high>>64)+u128::from(tail<shifted);
    let mut remainder=0_u128;let mut quotient=0_u128;
    for bit in (0..192).rev() {
        let value=if bit>=128{(head>>(bit-128))&1}else{(tail>>bit)&1};
        let carry=remainder>>127;remainder=(remainder<<1)|value;
        if carry!=0||remainder>=d {
            remainder=remainder.wrapping_sub(d);
            if bit>=128{return Err(Issue::Unknown("amount_overflow"));}
            quotient|=1_u128<<bit;
        }
    }
    quotient.checked_add(u128::from(up&&remainder!=0)).ok_or(Issue::Unknown("amount_overflow"))
}
fn raw_capacity(l:&LiquidityLevel)->Calc<u64> {
    l.output_parts_raw.iter().try_fold(0_u64,|n,&v|n.checked_add(v).ok_or(Issue::Unknown("capacity_overflow")))
}
fn price_cost(l:&LiquidityLevel,mut gross:u64)->Calc<u128> {
    let mut cost=0_u128;
    for part in l.output_parts_raw {
        let take=gross.min(part);gross-=take;
        cost=cost.checked_add(mul_div(take,l.rate_denominator,l.rate_numerator,true)?).ok_or(Issue::Unknown("amount_overflow"))?;
        if gross==0{return Ok(cost);}
    }
    Err(Issue::Insufficient("level_output_exhausted"))
}
fn level_cost(l:&LiquidityLevel,gross:u64,optimistic:bool)->Calc<u128> {
    let cost=price_cost(l,gross)?;
    if l.fees_on_input{included(cost,l,optimistic)}else{Ok(cost)}
}

/// An optimistic lower input cost: input fees may refund their entire declared
/// protocol portion and output fees may all be rebated. Thus rebates cannot make
/// an excluded model path profitable behind our back.
pub(super) fn minimum_input(input:&Input,direction:usize,mut output:u64)->Calc<u64> {
    if output==0{return Ok(0);}
    let p=profile(input)?;let dir=&p.directions[direction];
    if let Some(c)=&p.reserve_curve {
        if dir.status=="empty"{return Err(Issue::Insufficient("direction_disabled"));}
        if dir.status!="available"{return Err(Issue::Unknown("direction_unknown"));}
        let sell=dir.input_mint==c.base_mint;
        let (rin,rout)=if sell{(c.base_reserve_raw,c.effective_quote_reserve_raw)}else{(c.effective_quote_reserve_raw,c.base_reserve_raw)};
        if output>=rout || (sell&&output>c.spendable_quote_reserve_raw){return Err(Issue::Insufficient("reserve_output_exhausted"));}
        let cost=(u128::from(rin)*u128::from(output)).div_ceil(u128::from(rout-output));
        let cost=if sell{cost}else{
            if c.fee_bps[0]>=10000{return Err(Issue::Unknown("invalid_lp_fee"));}
            cost.checked_add(cost.checked_mul(u128::from(c.fee_bps[0])).ok_or(Issue::Unknown("amount_overflow"))?.div_ceil(10000))
                .ok_or(Issue::Unknown("amount_overflow"))?
        };
        return u64::try_from(cost).map_err(|_|Issue::Unknown("amount_overflow"));
    }
    if matches!(dir.status,"unknown"|"uncovered"){return Err(Issue::Unknown("direction_unknown"));}
    let mut cost=0_u128;
    for l in &dir.levels {
        let capacity=raw_capacity(l)?;let take=output.min(capacity);
        cost=cost.checked_add(level_cost(l,take,true)?).ok_or(Issue::Unknown("amount_overflow"))?;
        output-=take;
        if output==0{return u64::try_from(cost).map_err(|_|Issue::Unknown("amount_overflow"));}
    }
    if dir.coverage=="exhausted_bitmap"{Err(Issue::Insufficient("insufficient_integer_output"))}
    else{Err(Issue::Unknown("liquidity_window_incomplete"))}
}

pub(super) fn quote(input:&Input,direction:usize,mut amount:u64)->Calc<u64> {
    let p=profile(input)?;let dir=&p.directions[direction];
    if dir.status!="available"{return Err(Issue::Unknown("direction_unavailable_for_estimate"));}
    if let Some(c)=&p.reserve_curve {
        let fee=|n:u64|->Calc<u64>{c.fee_bps.iter().try_fold(0_u64,|sum,&r|sum.checked_add(ceil_fee(n,r,10000)?).ok_or(Issue::Unknown("fee_overflow")))};
        if dir.input_mint==c.base_mint {
            let gross=(u128::from(c.effective_quote_reserve_raw)*u128::from(amount))/(u128::from(c.base_reserve_raw)+u128::from(amount));
            let gross=u64::try_from(gross).map_err(|_|Issue::Unknown("amount_overflow"))?;
            let lp=ceil_fee(gross,c.fee_bps[0],10000)?;
            if gross.saturating_sub(lp)>c.spendable_quote_reserve_raw{return Err(Issue::Unknown("physical_quote_limit"));}
            return Ok(gross.saturating_sub(fee(gross)?));
        }
        let total=c.fee_bps.iter().try_fold(0_u64,|n,&r|n.checked_add(r)).ok_or(Issue::Unknown("fee_overflow"))?;
        if total>=10000{return Err(Issue::Unknown("invalid_fee_fraction"));}
        let net=(u128::from(amount)*10000/u128::from(10000+total)) as u64;
        let over=u128::from(net)+u128::from(fee(net)?);
        let net=net.saturating_sub(u64::try_from(over.saturating_sub(u128::from(amount))).map_err(|_|Issue::Unknown("amount_overflow"))?);
        let net=net.saturating_sub(c.buy_input_padding_raw);
        return Ok((u128::from(c.base_reserve_raw)*u128::from(net)/(u128::from(c.effective_quote_reserve_raw)+u128::from(net))) as u64);
    }
    let mut output=0_u64;
    for l in &dir.levels {
        if amount==0{break;}
        let excluded=if l.fees_on_input{amount-ceil_fee(amount,l.fee_numerator,l.fee_denominator)?}else{amount};
        let mut left=u128::from(excluded);let mut raw_out=0_u64;
        for capacity in l.output_parts_raw {
            let cost=mul_div(capacity,l.rate_denominator,l.rate_numerator,true)?;
            let (spent,out)=if left>=cost{(cost,capacity)}else{
                let n=u64::try_from(left).map_err(|_|Issue::Unknown("amount_overflow"))?;
                (left,u64::try_from(mul_div(n,l.rate_numerator,l.rate_denominator,false)?).map_err(|_|Issue::Unknown("amount_overflow"))?)
            };
            left-=spent;raw_out=raw_out.checked_add(out).ok_or(Issue::Unknown("amount_overflow"))?;
            if left==0{break;}
        }
        let used=u128::from(excluded)-left;
        let used=if l.fees_on_input {if left==0{u128::from(amount)}else{included(used,l,false)?}}else{used};
        amount=amount.checked_sub(u64::try_from(used).map_err(|_|Issue::Unknown("amount_overflow"))?).ok_or(Issue::Unknown("input_accounting"))?;
        let net=if l.fees_on_input{raw_out}else{raw_out-ceil_fee(raw_out,l.fee_numerator,l.fee_denominator)?};
        output=output.checked_add(net).ok_or(Issue::Unknown("amount_overflow"))?;
    }
    if amount!=0{return Err(Issue::Unknown("partial_fill_or_window_end"));}
    Ok(output)
}

#[derive(Clone,Copy)]
pub(super) struct Rate(pub u128,pub u128);
// Exact rational products, avoiding overflow and floating-point vetoes.
fn product(values:impl Iterator<Item=u128>)->Vec<u32> {
    let mut out=vec![1_u32];
    for n in values {
        let parts=[n as u32,(n>>32) as u32,(n>>64) as u32,(n>>96) as u32];
        let mut next=vec![0_u32;out.len()+4];
        for (i,&a) in out.iter().enumerate(){let mut carry=0_u64;for (j,&b) in parts.iter().enumerate(){
            let v=u64::from(a)*u64::from(b)+u64::from(next[i+j])+carry;next[i+j]=v as u32;carry=v>>32;
        }next[i+4]=carry as u32;}
        while next.len()>1 && next.last()==Some(&0){next.pop();}out=next;
    }out
}
fn compare_products(a:impl Iterator<Item=u128>,b:impl Iterator<Item=u128>)->std::cmp::Ordering {
    let a=product(a);let b=product(b);a.len().cmp(&b.len()).then_with(||a.iter().rev().cmp(b.iter().rev()))
}
fn no_gross_gain(rates:&[Rate])->bool {
    compare_products(rates.iter().map(|r|r.0),rates.iter().map(|r|r.1))!=std::cmp::Ordering::Greater
}
pub(super) fn upper_rate(input:&Input,direction:usize)->Calc<Rate> {
    let p=profile(input)?;let d=&p.directions[direction];
    if let Some(c)=&p.reserve_curve {
        if d.status=="empty"{return Ok(Rate(0,1));}
        if d.status!="available"{return Err(Issue::Unknown("direction_unknown"));}
        let r=if d.input_mint==c.base_mint{Rate(u128::from(c.effective_quote_reserve_raw),u128::from(c.base_reserve_raw))}
            else{Rate(u128::from(c.base_reserve_raw),u128::from(c.effective_quote_reserve_raw))};
        return if r.1==0{Err(Issue::Unknown("zero_reserve"))}else{Ok(r)};
    }
    if !d.monotone_rate_tail && d.coverage!="exhausted_bitmap"{return Err(Issue::Unknown("rate_bound_unknown"));}
    if matches!(d.status,"unknown"|"uncovered"){return Err(Issue::Unknown("direction_unknown"));}
    let mut best=Rate(0,1);
    for l in &d.levels {
        if l.rate_denominator==0{return Err(Issue::Unknown("zero_price_denominator"));}
        let r=Rate(l.rate_numerator,l.rate_denominator);
        if compare_products([r.0,best.1].into_iter(),[best.0,r.1].into_iter())==std::cmp::Ordering::Greater {best=r;}
    }Ok(best)
}

fn domains(inputs:&[Input],pairs:&[(String,String)])->Vec<Vec<Edge>> {
    pairs.iter().map(|(a,b)|inputs.iter().enumerate().filter_map(|(pool,input)|{
        let o=&input.observation;
        if &o.base_mint==a&&&o.quote_mint==b{Some(Edge{pool,direction:0})}
        else if &o.quote_mint==a&&&o.base_mint==b{Some(Edge{pool,direction:1})}else{None}
    }).collect()).collect()
}
fn minimum_path(domains:&[Vec<Edge>],inputs:&[Input],mut need:u64)->Calc<(u64,Vec<Edge>)> {
    let mut chosen=Vec::new();
    for domain in domains.iter().rev() {
        let mut best=None::<(u64,Edge)>;
        for &e in domain {if let Ok(n)=minimum_input(&inputs[e.pool],e.direction,need) {
            if best.is_none_or(|(old,_)|n<old){best=Some((n,e));}
        }}
        let Some((n,e))=best else{return Err(Issue::Unknown("inverse_path_unresolved"));};
        need=n;chosen.push(e);
    }
    chosen.reverse();
    if chosen.iter().enumerate().any(|(i,e)|chosen[..i].iter().any(|p|p.pool==e.pool)) {
        return Err(Issue::Unknown("duplicate_pool_relaxation"));
    }
    Ok((need,chosen))
}
fn estimate(path:&[Edge],inputs:&[Input],initial:u64)->Option<Estimate> {
    let mut amount=initial;let mut legs=Vec::new();
    for &e in path {
        let input=&inputs[e.pool];let p=profile(input).ok()?;let d=&p.directions[e.direction];
        let output=quote(input,e.direction,amount).ok()?;
        if output==0{return None;}
        legs.push(Leg{pool:input.observation.pool.clone(),slot:input.slot^e.direction,input_mint:d.input_mint.clone(),output_mint:d.output_mint.clone(),input_raw:amount,output_raw:output});amount=output;
    }
    if amount<=initial{return None;}
    Some(Estimate{input_raw:initial,output_raw:amount,profit_raw:amount-initial,legs})
}
fn forward_path(domains:&[Vec<Edge>],inputs:&[Input],mut amount:u64)->Option<Vec<Edge>> {
    let mut path=Vec::<Edge>::new();
    for domain in domains {
        let mut best=None::<(u64,Edge)>;
        for &e in domain {
            if path.iter().any(|p|p.pool==e.pool){continue;}
            if let Ok(n)=quote(&inputs[e.pool],e.direction,amount){if n>0&&best.is_none_or(|(old,_)|n>old){best=Some((n,e));}}
        }
        let (n,e)=best?;amount=n;path.push(e);
    }Some(path)
}
// A capacity bound is valid only for complete observations. Unknown tails cannot
// establish absence. These bounds ignore fees/rebates and overstate opportunity.
pub(super) fn output_bound(input:&Input,direction:usize)->Calc<u64> {
    let p=profile(input)?;let d=&p.directions[direction];
    if let Some(c)=&p.reserve_curve {
        return Ok(if d.input_mint==c.base_mint{c.spendable_quote_reserve_raw}else{c.base_reserve_raw});
    }
    if d.coverage!="exhausted_bitmap"{return Err(Issue::Unknown("capacity_tail_unknown"));}
    d.levels.iter().try_fold(0_u64,|n,l|n.checked_add(raw_capacity(l)?).ok_or(Issue::Unknown("capacity_overflow")))
}
fn maxima(domains:&[Vec<Edge>],inputs:&[Input])->Calc<Vec<Rate>> {
    domains.iter().map(|d|{
        let mut best=Rate(0,1);
        for e in d {let r=upper_rate(&inputs[e.pool],e.direction)?;
            if compare_products([r.0,best.1].into_iter(),[best.0,r.1].into_iter())==std::cmp::Ordering::Greater{best=r;}}
        Ok(best)
    }).collect()
}
fn no_profit_above(rates:&[Rate],output:u64,profit:u64)->bool {
    output<=profit||compare_products(std::iter::once(u128::from(output-profit)).chain(rates.iter().map(|r|r.0)),
        std::iter::once(u128::from(output)).chain(rates.iter().map(|r|r.1)))!=std::cmp::Ordering::Greater
}
fn minimum_economic_input(rates:&[Rate],profit:u64)->Option<u64> {
    let sufficient=|q:u64|compare_products(std::iter::once(u128::from(q)).chain(rates.iter().map(|r|r.0)),
        std::iter::once(u128::from(q)+u128::from(profit)).chain(rates.iter().map(|r|r.1)))==std::cmp::Ordering::Greater;
    if !sufficient(u64::MAX){return None;}
    let (mut lo,mut hi)=(1_u64,u64::MAX);
    while lo<hi{let mid=lo+(hi-lo)/2;if sufficient(mid){hi=mid}else{lo=mid+1}}
    Some(lo)
}

/// Find the best *observed probe* of this primary mint route, before packing.
/// Forward envelopes and optimistic inverse paths avoid pool Cartesian products.
/// No positive probe is not a global no-profit proof; alternatives remain unknown.
pub(super) fn assess(inputs:&[Input],pairs:&[(String,String)],cost_guard:u64,minimum_net_profit:u64)->Assessment {
    let started=Instant::now();let mut result=Assessment::unknown("no_economic_witness_found");
    result.pool_count=inputs.len();result.cost_guard_raw=cost_guard;
    let Some(required)=cost_guard.checked_add(minimum_net_profit)else{result.reason="cost_guard_overflow";return result;};
    result.required_profit_raw=required;
    let finish=|mut r:Assessment|{r.elapsed_ns=started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;r};
    let choices=domains(inputs,pairs);
    if choices.iter().any(Vec::is_empty){result.reason="missing_primary_pair";return finish(result);}
    let upper=maxima(&choices,inputs);
    if let Err(Issue::Unknown(reason)|Issue::Insufficient(reason))=&upper{result.reason=reason;}
    let mut start=1_u64;
    if let Ok(rates)=&upper {
        if no_gross_gain(rates){result.decision=Decision::Excluded;result.reason="raw_rate_upper_bound_not_profitable";return finish(result);}
        for (i,domain) in choices.iter().enumerate(){
            let cap=domain.iter().map(|e|output_bound(&inputs[e.pool],e.direction)).collect::<Calc<Vec<_>>>();
            if let Ok(cap)=cap {
                let mut bound=cap.into_iter().max().unwrap_or(0);let mut valid=true;
                for rate in &rates[i+1..] {match mul_div(bound,rate.0,rate.1,true).ok().and_then(|n|u64::try_from(n).ok()){
                    Some(n)=>bound=n,None=>{valid=false;break;}
                }}
                if valid {result.gross_output_upper_bound_raw=Some(result.gross_output_upper_bound_raw.map_or(bound,|n|n.min(bound)));
                    if no_profit_above(rates,bound,required){result.decision=Decision::Excluded;result.reason="capacity_profit_upper_bound_below_cost_guard";return finish(result);}}
            }
        }
        if let Some(n)=minimum_economic_input(rates,required){start=n;}
        else{result.reason="economic_input_not_representable";return finish(result);}
    }
    let mut probes=BTreeSet::from([start]);let mut n=start;
    while let Some(next)=n.checked_mul(2){probes.insert(next);n=next;}
    probes.insert(u64::MAX);
    // Include all known level input boundaries from every alternative, projected
    // through the prefix domains. This is search coverage, not an exclusion rule.
    for (i,domain) in choices.iter().enumerate(){for e in domain {if let Ok(p)=profile(&inputs[e.pool]){
        let mut total=0_u64;
        for l in &p.directions[e.direction].levels {
            let Some(v)=l.input_raw.and_then(|v|total.checked_add(v))else{break;};total=v;
            let q=if i==0{Some(total)}else{minimum_path(&choices[..i],inputs,total).ok().map(|p|p.0)};
            if let Some(q)=q.filter(|q|*q>=start){probes.insert(q);if q>start{probes.insert(q-1);}if let Some(q)=q.checked_add(1){probes.insert(q);}}
        }
    }}}
    result.first_probe_raw=probes.first().copied();result.last_probe_raw=probes.last().copied();
    let mut best=None::<Estimate>;
    for q in probes {
        result.probes+=1;
        let target=q.checked_add(required).and_then(|n|n.checked_add(1));
        let paths=[forward_path(&choices,inputs,q),target.and_then(|n|minimum_path(&choices,inputs,n).ok().map(|p|p.1))];
        for path in paths.into_iter().flatten(){if let Some(e)=estimate(&path,inputs,q){
            if best.as_ref().is_none_or(|old|e.profit_raw>old.profit_raw){best=Some(e);}
        }}
    }
    if let Some(best)=best {if best.profit_raw>required {
        result.decision=Decision::Possible;result.reason="economic_primary_witness";result.estimate=Some(best);
    }else{result.best_below_guard=Some(best);}}
    finish(result)
}

pub(super) fn evaluate_slots(inputs:&[Input],pairs:&[(String,String)],slots:&[usize],amount:u64)->Option<Estimate>{
    if pairs.len()!=slots.len(){return None;}
    let choices=domains(inputs,pairs);let mut path=Vec::new();
    for (domain,slot) in choices.iter().zip(slots){path.push(*domain.iter().find(|e|(inputs[e.pool].slot^e.direction)==*slot)?);}
    if path.iter().enumerate().any(|(i,e)|path[..i].iter().any(|p|p.pool==e.pool)){return None;}
    estimate(&path,inputs,amount)
}
