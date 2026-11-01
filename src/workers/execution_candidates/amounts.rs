//! Private quantity analysis of typed, frozen observations. No RPC or worker calls.
//! Proofs use optimistic costs (maximum possible fee rebates); estimates use full
//! DEX fees. Failed probes are never an exclusion proof. See docs/CP8C.md.
use std::{collections::BTreeMap,sync::{Arc,LazyLock,atomic::{AtomicU64,Ordering}},time::Instant};
use crate::core::market_state::{PoolPriceObservation,PoolLiquidity,LiquidityLevel};

#[derive(Clone)]
pub(super) struct Input {pub slot:usize,pub observation:Arc<PoolPriceObservation>,pub manifest_matches:bool}
#[derive(Clone,Copy,Debug)]
pub(super) enum Issue {Unknown(&'static str),Insufficient(&'static str)}
type Calc<T>=Result<T,Issue>;

// Bounded aggregate phase counters. No per-pool or per-revision history.
static PROFILE_ON:LazyLock<bool>=LazyLock::new(||
    matches!(std::env::var("GRAPH_SIGNALS_PROFILE").as_deref(),
        Ok("1"|"true"|"yes")));
static ASSESS_CALLS:AtomicU64=AtomicU64::new(0);
static ASSESS_NS:AtomicU64=AtomicU64::new(0);
static MINIMUM_CALLS:AtomicU64=AtomicU64::new(0);
static MINIMUM_NS:AtomicU64=AtomicU64::new(0);
static QUOTE_CALLS:AtomicU64=AtomicU64::new(0);
static QUOTE_NS:AtomicU64=AtomicU64::new(0);
static RATIONAL_FAST:AtomicU64=AtomicU64::new(0);
static RATIONAL_FALLBACK:AtomicU64=AtomicU64::new(0);
static RATIONAL_STACK:AtomicU64=AtomicU64::new(0);
static BOUNDS_NS:AtomicU64=AtomicU64::new(0);
static DOMAINS_NS:AtomicU64=AtomicU64::new(0);
static PROBE_BUILD_NS:AtomicU64=AtomicU64::new(0);
static PROBE_VALUES:AtomicU64=AtomicU64::new(0);
// CP12: aggregate-only quote cost evidence, no per-pool or per-revision buffers.
static QUOTE_LEVEL_CALLS:AtomicU64=AtomicU64::new(0);
static QUOTE_RESERVE_CALLS:AtomicU64=AtomicU64::new(0);
static QUOTE_LEVEL_NS:AtomicU64=AtomicU64::new(0);
static QUOTE_RESERVE_NS:AtomicU64=AtomicU64::new(0);
static QUOTE_FULL_COST_HITS:AtomicU64=AtomicU64::new(0);
static QUOTE_FULL_COST_MISSES:AtomicU64=AtomicU64::new(0);
static QUOTE_RESERVE_FEE_HITS:AtomicU64=AtomicU64::new(0);
static QUOTE_RESERVE_FEE_MISSES:AtomicU64=AtomicU64::new(0);
static MUL_DIV_U128:AtomicU64=AtomicU64::new(0);
static MUL_DIV_WIDE:AtomicU64=AtomicU64::new(0);
#[derive(Default,serde::Serialize)]
pub(super) struct PhaseProfile {
    assess_calls:u64,assess_ns:u64,minimum_calls:u64,minimum_ns:u64,
    quote_calls:u64,quote_ns:u64,rational_fast:u64,rational_stack:u64,rational_fallback:u64,
    bounds_ns:u64,domains_ns:u64,probe_build_ns:u64,probe_values:u64,
    cp12_quote_level_calls:u64,cp12_quote_reserve_calls:u64,
    cp12_quote_level_ns:u64,cp12_quote_reserve_ns:u64,
    cp12_full_cost_cache_hits:u64,cp12_full_cost_cache_misses:u64,
    cp12_reserve_fee_cache_hits:u64,cp12_reserve_fee_cache_misses:u64,
    cp12_mul_div_u128:u64,cp12_mul_div_wide:u64,
}
pub(super) fn phase_profile()->PhaseProfile {
    PhaseProfile{assess_calls:ASSESS_CALLS.load(Ordering::Relaxed),
        assess_ns:ASSESS_NS.load(Ordering::Relaxed),
        minimum_calls:MINIMUM_CALLS.load(Ordering::Relaxed),
        minimum_ns:MINIMUM_NS.load(Ordering::Relaxed),
        quote_calls:QUOTE_CALLS.load(Ordering::Relaxed),
        quote_ns:QUOTE_NS.load(Ordering::Relaxed),
        rational_fast:RATIONAL_FAST.load(Ordering::Relaxed),
        rational_stack:RATIONAL_STACK.load(Ordering::Relaxed),
        rational_fallback:RATIONAL_FALLBACK.load(Ordering::Relaxed),
        bounds_ns:BOUNDS_NS.load(Ordering::Relaxed),
        domains_ns:DOMAINS_NS.load(Ordering::Relaxed),
        probe_build_ns:PROBE_BUILD_NS.load(Ordering::Relaxed),
        probe_values:PROBE_VALUES.load(Ordering::Relaxed),
        cp12_quote_level_calls:QUOTE_LEVEL_CALLS.load(Ordering::Relaxed),
        cp12_quote_reserve_calls:QUOTE_RESERVE_CALLS.load(Ordering::Relaxed),
        cp12_quote_level_ns:QUOTE_LEVEL_NS.load(Ordering::Relaxed),
        cp12_quote_reserve_ns:QUOTE_RESERVE_NS.load(Ordering::Relaxed),
        cp12_full_cost_cache_hits:QUOTE_FULL_COST_HITS.load(Ordering::Relaxed),
        cp12_full_cost_cache_misses:QUOTE_FULL_COST_MISSES.load(Ordering::Relaxed),
        cp12_reserve_fee_cache_hits:QUOTE_RESERVE_FEE_HITS.load(Ordering::Relaxed),
        cp12_reserve_fee_cache_misses:QUOTE_RESERVE_FEE_MISSES.load(Ordering::Relaxed),
        cp12_mul_div_u128:MUL_DIV_U128.load(Ordering::Relaxed),
        cp12_mul_div_wide:MUL_DIV_WIDE.load(Ordering::Relaxed)}
}
struct AssessClock(Option<Instant>);
impl AssessClock {
    fn start()->Self {
        if *PROFILE_ON {ASSESS_CALLS.fetch_add(1,Ordering::Relaxed);Self(Some(Instant::now()))}
        else {Self(None)}
    }
}
impl Drop for AssessClock {
    fn drop(&mut self) {
        if let Some(started)=self.0 {
            ASSESS_NS.fetch_add(started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                Ordering::Relaxed);
        }
    }
}
fn ns(started:Instant)->u64{started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64}
#[derive(Clone,Copy,Debug,PartialEq,Eq,serde::Serialize)]
#[serde(rename_all="snake_case")]
pub(super) enum Decision {Possible,Unknown,Excluded}
#[derive(serde::Serialize)]
pub(super) struct Leg {pub pool:String,pub input_mint:String,pub output_mint:String,pub input_raw:u64,pub output_raw:u64}
#[derive(serde::Serialize)]
pub(super) struct Estimate {pub input_raw:u64,pub output_raw:u64,pub profit_raw:u64,pub legs:Vec<Leg>}
#[derive(serde::Serialize)]
pub(super) struct Cycle {
    pub mints:Vec<String>,pub status:&'static str,pub reason:&'static str,
    pub minimum_input_lower_bound_raw:Option<u64>,
}
#[derive(serde::Serialize)]
pub(super) struct Assessment {
    pub decision:Decision,pub scope:&'static str,pub reason:&'static str,
    pub pool_count:usize,pub cycles:Vec<Cycle>,pub probes:u64,pub elapsed_ns:u64,
    pub estimate:Option<Estimate>,
}
impl Assessment {
    pub fn unknown(reason:&'static str)->Self {Self{decision:Decision::Unknown,scope:"current_observed_simple_cycles_dex_only",
        reason,pool_count:0,cycles:Vec::new(),probes:0,elapsed_ns:0,estimate:None}}
}
#[derive(Default,serde::Serialize)]
pub(super) struct Report {
    pub attempts:u64,pub possible:u64,pub unknown:u64,pub excluded:u64,pub seed_repaired:u64,
    pub global_proofs:u64,pub packed_exclusions:u64,pub probes:u64,pub processing_ns_total:u64,pub processing_ns_max:u64,
    pub processing_ns_histogram:Vec<u64>,pub reasons:BTreeMap<&'static str,u64>,
    pub excluded_samples:Vec<serde_json::Value>,
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
    pub fn retain_exclusion(&mut self,a:Assessment,inputs:&[Input],context:serde_json::Value) {
        if self.excluded_samples.len()<4 {self.excluded_samples.push(serde_json::json!({"context":context,"assessment":a,
            "models":inputs.iter().map(|i|serde_json::json!({"slot":i.slot,"manifest_matches":i.manifest_matches,
                "observation":i.observation,"complete_levels":i.observation.liquidity.as_ref()
                    .map(|p|p.directions.iter().map(|d|&d.levels).collect::<Vec<_>>())})).collect::<Vec<_>>()}));}
    }
}

#[derive(Clone,Copy)]
struct Edge {pool:usize,direction:usize}
type Domains=BTreeMap<(String,String),Vec<Edge>>;
fn profile(input:&Input)->Calc<&PoolLiquidity> {
    if !input.manifest_matches{return Err(Issue::Unknown("manifest_window_changed"));}
    if !input.observation.valuation.as_ref().is_some_and(|v|v.token_fee_status.iter().all(|s|*s=="verified_no_standard_transfer_fee")) {
        return Err(Issue::Unknown("token_transfer_cost_unresolved"));
    }
    let p=input.observation.liquidity.as_deref().ok_or(Issue::Unknown("amount_model_unavailable"))?;
    // The signal layer may observe lightweight directional reserve profiles.
    // Only fully modeled and frozen execution windows may establish a quantity
    // exclusion proof; unknown leaves sizing and final choice to V10.
    if p.model!="dlmm_execution_window_v2" && p.model!="constant_product_quote_fees_v1" {
        return Err(Issue::Unknown("amount_model_not_implemented"));
    }
    Ok(p)
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
fn mul_div(a:u64,b:u128,d:u128,up:bool)->Calc<u128> {
    if d==0{return Err(Issue::Unknown("zero_price_denominator"));}
    if let Some(n)=u128::from(a).checked_mul(b){
        if *PROFILE_ON {MUL_DIV_U128.fetch_add(1,Ordering::Relaxed);}
        return Ok(if up{n.div_ceil(d)}else{n/d});
    }
    if *PROFILE_ON {MUL_DIV_WIDE.fetch_add(1,Ordering::Relaxed);}
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
pub(super) fn minimum_input(input:&Input,direction:usize,output:u64)->Calc<u64> {
    if !*PROFILE_ON {return minimum_input_inner(input,direction,output);}
    let started=Instant::now();
    let result=minimum_input_inner(input,direction,output);
    MINIMUM_CALLS.fetch_add(1,Ordering::Relaxed);
    MINIMUM_NS.fetch_add(ns(started),Ordering::Relaxed);
    result
}
fn minimum_input_inner(input:&Input,direction:usize,mut output:u64)->Calc<u64> {
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

// No cache survives a frozen assessment: entries are indexed by the input slice
// and direction, which makes cross-revision or cross-window reuse impossible.
#[derive(Default)]
struct QuoteEntry {
    full_component_costs:Option<Vec<[Option<Calc<u128>>;3]>>,
    reserve_fee_total:Option<Calc<u64>>,
}
struct FrozenQuoteScratch {entries:Vec<[QuoteEntry;2]>}
impl FrozenQuoteScratch {
    fn new(pool_count:usize)->Self {
        Self{entries:(0..pool_count).map(|_|
            std::array::from_fn(|_|QuoteEntry::default())).collect()}
    }
    fn quote(&mut self,inputs:&[Input],edge:Edge,amount:u64)->Calc<u64> {
        let input=&inputs[edge.pool];
        timed_quote(input,||quote_inner(input,edge.direction,amount,
            Some(&mut self.entries[edge.pool][edge.direction])))
    }
}
fn timed_quote(input:&Input, f:impl FnOnce()->Calc<u64>)->Calc<u64> {
    if !*PROFILE_ON {return f();}
    let reserve=input.observation.liquidity.as_ref().is_some_and(|p|p.reserve_curve.is_some());
    let started=Instant::now();
    let result=f();
    let elapsed=ns(started);
    QUOTE_CALLS.fetch_add(1,Ordering::Relaxed);
    QUOTE_NS.fetch_add(elapsed,Ordering::Relaxed);
    if reserve {
        QUOTE_RESERVE_CALLS.fetch_add(1,Ordering::Relaxed);
        QUOTE_RESERVE_NS.fetch_add(elapsed,Ordering::Relaxed);
    }else{
        QUOTE_LEVEL_CALLS.fetch_add(1,Ordering::Relaxed);
        QUOTE_LEVEL_NS.fetch_add(elapsed,Ordering::Relaxed);
    }
    result
}
pub(super) fn quote(input:&Input,direction:usize,amount:u64)->Calc<u64> {
    timed_quote(input,||quote_inner(input,direction,amount,None))
}
fn quote_inner(input:&Input,direction:usize,mut amount:u64,mut cache:Option<&mut QuoteEntry>)->Calc<u64> {
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
        // Only the buy branch evaluates the fee total, preserving the
        // existing evaluation order and error precedence on every quote.
        let calculate_total=||c.fee_bps.iter().try_fold(0_u64,|n,&r|n.checked_add(r))
            .ok_or(Issue::Unknown("fee_overflow"));
        let total=if let Some(entry)=cache.as_mut(){
            if let Some(value)=entry.reserve_fee_total {
                if *PROFILE_ON {QUOTE_RESERVE_FEE_HITS.fetch_add(1,Ordering::Relaxed);}
                value?
            }else{
                let value=calculate_total();
                entry.reserve_fee_total=Some(value);
                if *PROFILE_ON {QUOTE_RESERVE_FEE_MISSES.fetch_add(1,Ordering::Relaxed);}
                value?
            }
        }else{calculate_total()?};
        if total>=10000{return Err(Issue::Unknown("invalid_fee_fraction"));}
        let net=(u128::from(amount)*10000/u128::from(10000+total)) as u64;
        let over=u128::from(net)+u128::from(fee(net)?);
        let net=net.saturating_sub(u64::try_from(over.saturating_sub(u128::from(amount))).map_err(|_|Issue::Unknown("amount_overflow"))?);
        let net=net.saturating_sub(c.buy_input_padding_raw);
        return Ok((u128::from(c.base_reserve_raw)*u128::from(net)/(u128::from(c.effective_quote_reserve_raw)+u128::from(net))) as u64);
    }
    // Allocate an empty per-level scratch only for actually quoted directed
    // level data. Component costs are computed lazily on first use. Cached
    // Result values include errors, preserving the original first error seen.
    let mut full_costs=cache.as_mut().map(|entry|
        entry.full_component_costs.get_or_insert_with(||
            vec![[None;3];dir.levels.len()]));
    let mut output=0_u64;
    for (level_index,l) in dir.levels.iter().enumerate() {
        if amount==0{break;}
        let excluded=if l.fees_on_input{amount-ceil_fee(amount,l.fee_numerator,l.fee_denominator)?}else{amount};
        let mut left=u128::from(excluded);let mut raw_out=0_u64;
        for (part_index,capacity) in l.output_parts_raw.into_iter().enumerate() {
            let cost=if let Some(costs)=full_costs.as_mut() {
                let entry=&mut costs[level_index][part_index];
                if let Some(value)=*entry {
                    if *PROFILE_ON {QUOTE_FULL_COST_HITS.fetch_add(1,Ordering::Relaxed);}
                    value?
                }else{
                    let value=mul_div(capacity,l.rate_denominator,l.rate_numerator,true);
                    *entry=Some(value);
                    if *PROFILE_ON {QUOTE_FULL_COST_MISSES.fetch_add(1,Ordering::Relaxed);}
                    value?
                }
            }else{mul_div(capacity,l.rate_denominator,l.rate_numerator,true)?};
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
struct Rate(u128,u128);
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
/// Exact 32-bit-limb schoolbook product on bounded stack scratch. The old
/// allocation-based implementation remains the fallback for arbitrary lengths.
fn product_stack(values:impl Iterator<Item=u128>)->Option<([u32;32],usize)> {
    let mut out=[0_u32;32];out[0]=1;let mut len=1_usize;
    for n in values {
        // No overflowing writes, and never truncate large cycle products.
        if len+4>out.len(){return None;}
        let parts=[n as u32,(n>>32) as u32,(n>>64) as u32,(n>>96) as u32];
        let mut next=[0_u32;32];
        for (i,&a) in out[..len].iter().enumerate(){
            let mut carry=0_u64;
            for (j,&b) in parts.iter().enumerate(){
                let value=u64::from(a)*u64::from(b)+u64::from(next[i+j])+carry;
                next[i+j]=value as u32;carry=value>>32;
            }
            next[i+4]=carry as u32;
        }
        len+=4;
        while len>1 && next[len-1]==0{len-=1;}
        out=next;
    }
    Some((out,len))
}
fn compare_products(a:impl Iterator<Item=u128>+Clone,b:impl Iterator<Item=u128>+Clone)->std::cmp::Ordering {
    // Exact fast path for small products, no allocations.
    let left=a.clone().try_fold(1_u128,|acc,n|acc.checked_mul(n));
    let right=b.clone().try_fold(1_u128,|acc,n|acc.checked_mul(n));
    if let (Some(left),Some(right))=(left,right) {
        if *PROFILE_ON {RATIONAL_FAST.fetch_add(1,Ordering::Relaxed);}
        return left.cmp(&right);
    }
    // Normal large 2-4-leg price products fit within 1024 bits. Even when
    // the 128-bit shortcut overflows, keep multiplication on the stack.
    if let (Some((left,ln)),Some((right,rn)))=(product_stack(a.clone()),product_stack(b.clone())){
        if *PROFILE_ON {RATIONAL_STACK.fetch_add(1,Ordering::Relaxed);}
        return ln.cmp(&rn).then_with(||left[..ln].iter().rev().cmp(right[..rn].iter().rev()));
    }
    // If there are more factors, the original unlimited exact arithmetic
    // is authoritative. No new size bound on cycles or candidates.
    if *PROFILE_ON {RATIONAL_FALLBACK.fetch_add(1,Ordering::Relaxed);}
    let left=product(a);let right=product(b);
    left.len().cmp(&right.len()).then_with(||left.iter().rev().cmp(right.iter().rev()))
}
fn no_gross_gain(rates:&[Rate])->bool {
    compare_products(rates.iter().map(|r|r.0),rates.iter().map(|r|r.1))!=std::cmp::Ordering::Greater
}
fn upper_rate(input:&Input,direction:usize)->Calc<Rate> {
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

fn cycles(node:&str,settlement:&str,domains:&Domains,path:&mut Vec<String>,out:&mut Vec<Vec<String>>) {
    for (_,b) in domains.keys().filter(|pair|pair.0.as_str()==node) {
        if b==settlement {if path.len()>=2 {let mut p=path.clone();p.push(b.clone());out.push(p);}continue;}
        if path.contains(b){continue;}
        path.push(b.clone());cycles(b,settlement,domains,path,out);path.pop();
    }
}
fn minimum_path(domains:&[Vec<Edge>],inputs:&[Input],mut need:u64,allow_unknown_options:bool)->Calc<(u64,Vec<Edge>)> {
    let mut chosen=Vec::new();let mut uncertain=None;
    for domain in domains.iter().rev() {
        let mut best=None::<(u64,Edge)>;
        for &e in domain {match minimum_input(&inputs[e.pool],e.direction,need) {
            Ok(n)=>if best.is_none_or(|(old,_)|n<old){best=Some((n,e));},
            Err(Issue::Unknown(reason))=>uncertain=Some(reason),Err(Issue::Insufficient(_))=>{},
        }}
        let Some((n,e))=best else{return Err(if let Some(reason)=uncertain{Issue::Unknown(reason)}else{Issue::Insufficient("no_compatible_integer_flow")});};
        need=n;chosen.push(e);
    }
    chosen.reverse();
    if chosen.iter().enumerate().any(|(i,e)|chosen[..i].iter().any(|p|p.pool==e.pool)) {
        return Err(Issue::Unknown("duplicate_pool_relaxation"));
    }
    // A lower bound based on a known option is unsafe if another option is unknown.
    if !allow_unknown_options {if let Some(reason)=uncertain{return Err(Issue::Unknown(reason));}}
    Ok((need,chosen))
}
fn estimate(path:&[Edge],inputs:&[Input],initial:u64,minimum_profit:u64,
    scratch:&mut Option<FrozenQuoteScratch>)->Option<Estimate> {
    let mut amount=initial;let mut legs=Vec::new();
    for &e in path {
        let input=&inputs[e.pool];let p=profile(input).ok()?;let d=&p.directions[e.direction];
        let output=scratch.get_or_insert_with(||FrozenQuoteScratch::new(inputs.len()))
            .quote(inputs,e,amount).ok()?;
        if output==0{return None;}
        legs.push(Leg{pool:input.observation.pool.clone(),input_mint:d.input_mint.clone(),output_mint:d.output_mint.clone(),input_raw:amount,output_raw:output});amount=output;
    }
    if amount<=initial || amount-initial<=minimum_profit{return None;}
    Some(Estimate{input_raw:initial,output_raw:amount,profit_raw:amount-initial,legs})
}

pub(super) fn assess(inputs:&[Input],settlement:&str,minimum_profit:u64,single_conversion_other:Option<&str>)->Assessment {
    let _cp11_clock=AssessClock::start();
    let started=Instant::now();let mut result=Assessment::unknown("no_closed_cycle");result.pool_count=inputs.len();
    // Scratch bounds for this frozen assessment only. A shared pair may occur
    // in several mint cycles; do not rescan all of its levels for each cycle.
    let bound_started=Instant::now();
    let bounds=inputs.iter().map(|i|[upper_rate(i,0),upper_rate(i,1)]).collect::<Vec<_>>();
    if *PROFILE_ON{BOUNDS_NS.fetch_add(ns(bound_started),Ordering::Relaxed);}
    let domains_started=Instant::now();
    let mut domains=Domains::new();
    for (i,input) in inputs.iter().enumerate(){let o=&input.observation;
        for (direction,a,b) in [(0,&o.base_mint,&o.quote_mint),(1,&o.quote_mint,&o.base_mint)] {
            domains.entry((a.clone(),b.clone())).or_default().push(Edge{pool:i,direction});
        }
    }
    let mut paths=Vec::new();cycles(settlement,settlement,&domains,&mut vec![settlement.into()],&mut paths);
    if *PROFILE_ON{DOMAINS_NS.fetch_add(ns(domains_started),Ordering::Relaxed);}
    // Lazily allocate per-assessment scratch only if a quote is actually
    // evaluated. Pure proof/exclusion paths retain CP11's zero-cache cost.
    // It never survives the frozen assessment.
    let mut quote_scratch=None;
    let mut any_unknown=false;
    for mints in paths {
        // The typed V10 request supplies only one conversion pool. Two different
        // S/Q conversion pools cannot form their own two-leg route in this job.
        if mints.len()==3 && single_conversion_other==Some(mints[1].as_str()){continue;}
        let mut choices=mints.windows(2).map(|m|domains[&(m[0].clone(),m[1].clone())].clone()).collect::<Vec<_>>();
        let maxima=choices.iter().map(|d|{
            let mut max=Rate(0,1);
            for e in d {let r=bounds[e.pool][e.direction]?;
                if compare_products([r.0,max.1].into_iter(),[max.0,r.1].into_iter())==std::cmp::Ordering::Greater{max=r;}}
            Ok(max)
        }).collect::<Calc<Vec<_>>>();
        if let Ok(maxima)=maxima {
            if no_gross_gain(&maxima){result.cycles.push(Cycle{mints,status:"excluded",reason:"optimistic_rate_product_not_above_one",minimum_input_lower_bound_raw:None});continue;}
            for (i,domain) in choices.iter_mut().enumerate(){domain.retain(|e|{
                let Ok(rate)=bounds[e.pool][e.direction]else{return true;};
                let mut rates=maxima.clone();rates[i]=rate;!no_gross_gain(&rates)
            });}
        }
        match minimum_path(&choices,inputs,1,false) {
            Err(Issue::Insufficient(reason))=>result.cycles.push(Cycle{mints,status:"excluded",reason,minimum_input_lower_bound_raw:None}),
            Err(Issue::Unknown(reason))=>{any_unknown=true;result.cycles.push(Cycle{mints,status:"unknown",reason,minimum_input_lower_bound_raw:None});},
            Ok((minimum,path))=>{
                let probe_started=Instant::now();
                // Identical probe domain and ascending order as BTreeSet,
                // but only one allocation, one sort and one dedup.
                let mut probes=Vec::with_capacity(72);
                probes.push(minimum);
                let mut n=minimum.max(1);
                while let Some(next)=n.checked_mul(2){probes.push(next);n=next;}
                probes.push(u64::MAX);
                // Include every representable observed level boundary, projected
                // backward through optimistic prefix costs. Sampling cannot veto.
                for (i,e) in path.iter().enumerate(){if let Ok(p)=profile(&inputs[e.pool]){
                    let mut total=0_u64;
                    for l in &p.directions[e.direction].levels {
                        let Some(v)=l.input_raw.and_then(|n|total.checked_add(n))else{break;};total=v;
                        let mut need=total;let mut valid=true;
                        for prior in path[..i].iter().rev(){match minimum_input(&inputs[prior.pool],prior.direction,need){Ok(n)=>need=n,Err(_)=>{valid=false;break;}}}
                        if valid{probes.push(need);}
                    }
                }}
                probes.sort_unstable();
                probes.dedup();
                if *PROFILE_ON {
                    PROBE_BUILD_NS.fetch_add(ns(probe_started),Ordering::Relaxed);
                    PROBE_VALUES.fetch_add(probes.len() as u64,Ordering::Relaxed);
                }
                let mut found=None;
                for q in probes {if q==0{continue;}result.probes+=1;
                    if let Some(e)=estimate(&path,inputs,q,minimum_profit,&mut quote_scratch){found=Some(e);break;}
                }
                if let Some(e)=found {
                    result.estimate=Some(e);result.decision=Decision::Possible;result.reason="positive_amount_estimate";
                    result.cycles.push(Cycle{mints,status:"possible",reason:"positive_amount_estimate",minimum_input_lower_bound_raw:Some(minimum)});
                    result.elapsed_ns=started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;return result;
                }
                any_unknown=true;result.cycles.push(Cycle{mints,status:"unknown",reason:"no_positive_probe_is_not_a_proof",minimum_input_lower_bound_raw:Some(minimum)});
            },
        }
    }
    if !result.cycles.is_empty(){result.decision=if any_unknown{Decision::Unknown}else{Decision::Excluded};
        result.reason=if any_unknown{"quantity_not_proven"}else{"all_model_cycles_excluded"};}
    result.elapsed_ns=started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;result
}

/// Find a quantity-compatible seed without a Cartesian pool product. Each mint
/// role has an independent monotone optimistic inverse; unknowns never prove a veto.
pub(super) fn seed(inputs:&[Input],pairs:&[(String,String)])->Option<Vec<usize>> {
    let domains=pairs.iter().map(|(a,b)|inputs.iter().enumerate().filter_map(|(i,input)|{
        let o=&input.observation;
        if &o.base_mint==a&&&o.quote_mint==b{Some(Edge{pool:i,direction:0})}
        else if &o.quote_mint==a&&&o.base_mint==b{Some(Edge{pool:i,direction:1})}else{None}
    }).collect::<Vec<_>>()).collect::<Vec<_>>();
    let (_,path)=minimum_path(&domains,inputs,1,true).ok()?;
    Some(path.iter().map(|e|inputs[e.pool].slot^e.direction).collect())
}


#[cfg(test)]
mod cp11_product_tests {
    use super::*;
    #[test]
    fn cp11_stack_product_matches_unbounded_reference() {
        for len in 0..=12 {
            for variant in 0..100_u128 {
                let a=(0..len).map(|i|{
                    let i=i as u128;
                    match (variant+i)%7 {
                        0=>0,1=>1,2=>u128::MAX,3=>1_u128<<127,
                        4=>(variant+i+3)*(1_u128<<70),
                        5=>((variant+i+7)<<64)|((i+1)<<32)|100,
                        _=>2_u128.pow((variant%65) as u32),
                    }
                }).collect::<Vec<_>>();
                let b=(0..len).map(|i|{
                    let i=i as u128;
                    match (variant*3+i)%6 {
                        0=>u128::MAX,1=>0,2=>1,
                        3=>(variant+i+17)*(1_u128<<60),
                        4=>1_u128<<127,
                        _=>(i+2)*((variant+1)<<64),
                    }
                }).collect::<Vec<_>>();
                let old_a=product(a.iter().copied());
                let old_b=product(b.iter().copied());
                let expected=old_a.len().cmp(&old_b.len())
                    .then_with(||old_a.iter().rev().cmp(old_b.iter().rev()));
                let actual=compare_products(a.iter().copied(),b.iter().copied());
                assert_eq!(expected,actual,"len={len} variant={variant}");
            }
        }
    }
}


#[cfg(test)]
pub(super) fn cp12_cached_quote_trace(
    inputs:&[Input],pool:usize,direction:usize,amounts:&[u64]
)->Vec<Calc<u64>>{
    let mut scratch=FrozenQuoteScratch::new(inputs.len());
    amounts.iter().map(|&amount|scratch.quote(inputs,Edge{pool,direction},amount)).collect()
}
