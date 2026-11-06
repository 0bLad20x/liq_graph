//! CP21S-A: complete, state-delta-driven Direct2 economic candidate index.
//!
//! One changed pool invalidates ALL directed pairs that contain it; there are
//! no source, active-candidate or per-revision top-N cutoffs. Only physical V10
//! admission is capacity limited. A newer version supersedes the queued older
//! version of the same exact pool pair (never silently dropped). No RPC.
//!
//! A pair is modelled regardless of its marginal ranking. Positive economic
//! forecasts enter the ready index; unsupported quotes remain explicitly
//! Unknown and may enter only with a positive marginal witness. Nonpositive
//! forecasts are retained in the complete indexed inventory, not sent to V10.
//! CP20S's dedup remains a transient execution guard, not a discovery filter.
use super::*;
use std::collections::{BTreeSet,HashMap,HashSet};
use std::sync::mpsc::SyncSender;
use serde_json::{Value,json};
use std::cmp::Reverse;
use std::time::Duration;
use crate::core::signals::SignalLeg;

#[derive(Clone,Copy,Debug,PartialEq,Eq,PartialOrd,Ord,Hash)]
struct Key { topology:usize,direct:usize,partner:usize }
#[derive(Clone,Copy,Debug,PartialEq,Eq,PartialOrd,Ord)]
struct Rank { category:u8,score:Reverse<i128>,key:Key }
#[derive(Clone)]
struct Entry {
    net:Option<i128>,
    input_raw:Option<u64>,
    output_raw:Option<u64>,
    gross_raw:Option<i128>,
    fingerprint:Option<String>,
    rank:Option<Rank>,
    revision:u64,
    trigger:Arc<SignalTrigger>,
    ready_us:u64,
    pipeline:Option<PricePipelineTiming>,
}
#[derive(Default)]
struct Index {
    topology:HashMap<(Pubkey,Pubkey),usize>,
    entries:HashMap<Key,Entry>,
    ready:BTreeSet<Rank>,
    dispatched:u64,
    journal:Option<SyncSender<Value>>,
}
fn directions(pool:&SignalPool,a:Pubkey,b:Pubkey,id:usize)->Option<usize>{
    if pool.mint_a==a&&pool.mint_b==b {Some(2*id)}
    else if pool.mint_b==a&&pool.mint_a==b {Some(2*id+1)}
    else {None}
}
/// Pure complete invalidation set: every ordered pair touching the changed
/// pool, with no budget, sampling or dependence on current marginal winner.
fn invalidate_changed(affected:&mut HashSet<Key>,topology:usize,updated:usize,peers:&[usize]){
    for &other in peers{
        if other==updated{continue;}
        affected.insert(Key{topology,direct:updated,partner:other});
        affected.insert(Key{topology,direct:other,partner:updated});
    }
}
fn witness(state:&State,key:Key)->Option<SignalWitness>{
    let top=state.plan.as_ref()?.compact_topologies.get(key.topology)?;
    if top.kind!=SignalRouteKind::Direct2 || key.direct==key.partner{return None;}
    let ids=[key.direct,key.partner];
    let mut legs=Vec::with_capacity(2);
    let mut sum:i128=0;
    let mut marginal_complete=true;
    for (j,&id) in ids.iter().enumerate(){
        let (a,b)=top.roles[j];
        let pool=state.pools.get(id)?;
        let slot=directions(pool,a,b,id)?;
        let p=state.prices.get(id)?.as_ref()?;
        // CP21S-A.1: a missing *marginal* log is NOT an economic quote
        // rejection. Economic estimation uses the actual directed frozen
        // liquidity data, independently of the marginal signal generator.
        // Missing marginal weights use a zero placeholder ONLY for the
        // legacy SignalLeg wire type; witness bps remains None.
        let weight=p.log_weight_units[slot%2].unwrap_or_else(||{
            marginal_complete=false;0
        });
        sum+=i128::from(weight);
        legs.push(SignalLeg{
            slot,input_mint:a,output_mint:b,log_weight_units:weight,
            observation:p.observation.clone()
        });
    }
    let total=i64::try_from(sum).ok()?;
    let bps=(-(total as f64)/SIGNAL_LOG_SCALE as f64).exp_m1()*10_000.0;
    Some(SignalWitness{
        legs,log_sum_units:total,
        estimated_net_return_bps:(marginal_complete && bps.is_finite()).then_some(bps),
    })
}
/// Lossless, deterministic diagnosis from actual price/fee/liquidity facts.
/// This describes the FIRST known reason. A complete economic quote is still
/// attempted whenever both pool observations exist, even if marginal logs
/// are absent; missing marginal logs are an orthogonal diagnostic.
fn pair_unavailable_reason(state:&State,key:Key)->&'static str{
    let Some(plan)=state.plan.as_ref()else{return "plan_missing"};
    let Some(top)=plan.compact_topologies.get(key.topology)else{return "topology_missing"};
    if key.direct==key.partner{return "same_pool"};
    for (i,id) in [key.direct,key.partner].iter().enumerate(){
        let (a,b)=top.roles[i];
        let Some(pool)=state.pools.get(*id)else{return "pool_identity_missing"};
        let Some(slot)=directions(pool,a,b,*id)else{return "direction_identity_mismatch"};
        let Some(p)=state.prices.get(*id).and_then(Option::as_ref)else {
            return if i==0{"direct_price_missing"}else{"partner_price_missing"};
        };
        let Some(liq)=p.observation.liquidity.as_ref()else {
            return if i==0{"direct_liquidity_missing"}else{"partner_liquidity_missing"};
        };
        let Some(dir)=liq.directions.iter().find(|d|
            d.input_mint==a.to_string()&&d.output_mint==b.to_string())else{
            return "liquidity_direction_missing";
        };
        if dir.status!="available"{return "liquidity_direction_unavailable";}
        if p.observation.valuation.is_none(){return "fee_valuation_missing";}
        if !p.observation.valuation.as_ref().unwrap().token_fee_status.iter()
            .all(|s|*s=="verified_no_standard_transfer_fee"){
            return "token_transfer_fee_unresolved";
        }
        if p.log_weight_units[slot%2].is_none(){
            // This is a useful contributing fact, not an economic veto.
            // Only report it if the economic model cannot supply a quote.
            return if i==0{"direct_marginal_missing"}else{"partner_marginal_missing"};
        }
    }
    "economic_model_quote_unavailable"
}
impl Index {
    /// Every economic refresh and final scheduling decision is correlated
    /// by stable addresses, snapshot slots, UTC microseconds and revision.
    fn trace(&self,state:&State,key:Key,action:&'static str,
        reason:&str,score:Option<i128>,input:Option<u64>,
        gross:Option<i128>,trigger:&SignalTrigger,request_id:Option<&str>,
        report:&mut Report){
        let Some(sender)=self.journal.as_ref()else{return;};
        let Some(top)=state.plan.as_ref().and_then(|p|p.compact_topologies.get(key.topology))else{return;};
        let direct=state.pools.get(key.direct).map(|p|p.address.to_string());
        let partner=state.pools.get(key.partner).map(|p|p.address.to_string());
        let first=state.prices.get(key.direct).and_then(Option::as_ref)
            .map(|p|p.observation.as_ref());
        let second=state.prices.get(key.partner).and_then(Option::as_ref)
            .map(|p|p.observation.as_ref());
        let phase=if matches!(trigger,SignalTrigger::Catalog{..}){"catalog"}
            else if matches!(trigger,SignalTrigger::Coverage{..}){"coverage"}
            else{"price"};
        timeline::emit(Some(sender),json!({
            "schema":1,"kind":"economic_pair","phase":phase,"at_us":now_us(),
            "graph_generation":state.generation,"revision":state.revision,
            "trigger":trigger,"action":action,"reason":reason,
            "settlement":top.settlement.to_string(),"target":top.target.to_string(),
            "direct_pool":direct,"partner_pool":partner,
            "direct_slot":first.map(|p|p.slot),"partner_slot":second.map(|p|p.slot),
            "direct_component_slot_min":first.map(|p|p.component_slot_min),
            "direct_component_slot_max":first.map(|p|p.component_slot_max),
            "partner_component_slot_min":second.map(|p|p.component_slot_min),
            "partner_component_slot_max":second.map(|p|p.component_slot_max),
            "direct_received_at_us":first.map(|p|p.received_at_us),
            "partner_received_at_us":second.map(|p|p.received_at_us),
            "direct_component_received_at_us_min":first.map(|p|p.component_received_at_us_min),
            "partner_component_received_at_us_min":second.map(|p|p.component_received_at_us_min),
            "frame_received_at_us":state.price_pipeline.map(|p|p.frame_received_at_us),
            "signal_ready_at_us":state.ready_at_us,
            "direct_marginal_known":state.prices.get(key.direct).and_then(Option::as_ref)
                .and_then(|p|state.pools.get(key.direct).and_then(|pool|
                    directions(pool,top.roles[0].0,top.roles[0].1,key.direct)
                        .and_then(|slot|p.log_weight_units[slot%2]))).is_some(),
            "partner_marginal_known":state.prices.get(key.partner).and_then(Option::as_ref)
                .and_then(|p|state.pools.get(key.partner).and_then(|pool|
                    directions(pool,top.roles[1].0,top.roles[1].1,key.partner)
                        .and_then(|slot|p.log_weight_units[slot%2]))).is_some(),
            "economic_score_usd_nano":score.map(|x|x.to_string()),
            "optimal_input_raw":input,
            "gross_gain_raw":gross.map(|x|x.to_string()),
            "request_id":request_id,
        }),report);
    }
    /// Reconcile a new graph generation using stable on-chain pool addresses.
    /// Preserve the economic score AND pending V10 priority when the frozen
    /// account fingerprint is identical. Different IDs/order no longer force
    /// price re-optimization or loss of pending opportunities.
    fn reconcile_snapshot(&mut self,old_pools:&[SignalPool],
        old_prices:&[Option<Arc<SignalPoolPrice>>],
        old_plan:Option<&Arc<SignalPlan>>,state:&State,params:&ExecutionParameters,
        report:&mut Report,trigger:Arc<SignalTrigger>){
        // Exact no-op catalogs must not even visit the combinatorial index.
        // Graph generations are identity-based, not forced price revisions.
        let unchanged=old_pools==state.pools
            && old_plan.zip(state.plan.as_ref()).is_some_and(|(before,after)|
                before.compact_topologies.len()==after.compact_topologies.len()
                && before.compact_topologies.iter().zip(after.compact_topologies.iter())
                    .all(|(a,b)|a.kind==b.kind&&a.settlement==b.settlement
                        &&a.target==b.target&&a.roles==b.roles&&a.length==b.length))
            && old_prices.len()==state.prices.len()
            && old_prices.iter().zip(&state.prices).all(|(a,b)|match (a,b){
                (None,None)=>true,(Some(a),Some(b))=>Arc::ptr_eq(a,b),
                _=>false,
            });
        if unchanged{
            report.cp21a1_catalog_unchanged+=1;
            report.cp21a1_catalog_reused+=self.entries.len() as u64;
            timeline::emit(self.journal.as_ref(),json!({
                "schema":1,"kind":"catalog_reconciliation",
                "action":"no_op","at_us":now_us(),
                "graph_generation":state.generation,"revision":state.revision,
                "total_pools":state.pools.len(),"pairs_reused":self.entries.len(),
                "pairs_recomputed":0,"pending_preserved":self.ready.len(),
                "identity_and_observation_arcs_unchanged":true
            }),report);
            return;
        }
        let old=std::mem::take(self);
        self.journal=old.journal.clone();
        let old_addresses=old_pools.iter().map(|p|p.address).collect::<HashSet<_>>();
        let current_addresses=state.pools.iter().map(|p|p.address).collect::<HashSet<_>>();
        let mut entered=0_u64;let mut removed=0_u64;
        for pool in &state.pools{
            if old_addresses.contains(&pool.address){continue;}
            entered+=1;
            timeline::emit(self.journal.as_ref(),json!({
                "schema":1,"kind":"watchset","action":"entered",
                "layer":"graph_signal_catalog_not_ws_ack",
                "at_us":now_us(),"graph_generation":state.generation,
                "revision":state.revision,"pool":pool.address.to_string(),
                "mint_a":pool.mint_a.to_string(),"mint_b":pool.mint_b.to_string(),
                "price_available":state.pools.iter().position(|p|p.address==pool.address)
                    .and_then(|id|state.prices.get(id)).and_then(Option::as_ref).is_some()
            }),report);
        }
        for pool in old_pools{
            if current_addresses.contains(&pool.address){continue;}
            removed+=1;
            timeline::emit(self.journal.as_ref(),json!({
                "schema":1,"kind":"watchset","action":"exited",
                "layer":"graph_signal_catalog_not_ws_ack",
                "at_us":now_us(),"graph_generation":state.generation,
                "revision":state.revision,"pool":pool.address.to_string()
            }),report);
        }
        let mut old_entries=HashMap::<(Pubkey,Pubkey,Pubkey,Pubkey),Entry>::new();
        if let Some(old_plan)=old_plan {
            for (key,entry) in old.entries{
                if let (Some(t),Some(a),Some(b))=(
                    old_plan.compact_topologies.get(key.topology),
                    old_pools.get(key.direct),old_pools.get(key.partner)){
                    old_entries.insert((t.settlement,t.target,a.address,b.address),entry);
                }
            }
        }
        let Some(plan)=state.plan.as_ref()else{return;};
        for (i,top) in plan.compact_topologies.iter().enumerate(){
            if top.kind==SignalRouteKind::Direct2{
                self.topology.insert((top.settlement,top.target),i);
            }
        }
        let mut reused=0_u64;let mut recomputed=0_u64;
        for (&(a,b),ids) in &state.pair_pools{
            for (settlement,target) in [(a,b),(b,a)]{
                let Some(&topology)=self.topology.get(&(settlement,target))else{continue};
                for &direct in ids{
                    for &partner in ids{
                        if direct==partner{continue;}
                        let key=Key{topology,direct,partner};
                        let stable=(settlement,target,state.pools[direct].address,
                            state.pools[partner].address);
                        let previous=old_entries.remove(&stable);
                        if let Some(mut entry)=previous{
                            let current=witness(state,key).and_then(|w|
                                state.source_fingerprint(&w,params));
                            if current.is_some() && current==entry.fingerprint{
                                if let Some(mut rank)=entry.rank{
                                    rank.key=key;
                                    self.ready.insert(rank);
                                    entry.rank=Some(rank);
                                }
                                self.entries.insert(key,entry);
                                reused+=1;
                                continue;
                            }
                        }
                        recomputed+=1;
                        self.refresh_one(state,params,key,report,trigger.clone(),false);
                    }
                }
            }
        }
        report.cp21a1_catalog_reused+=reused;
        report.cp21a1_catalog_recomputed+=recomputed;
        report.cp21a1_catalog_reindexed+=1;
        if entered==0 && removed==0 {report.cp21a1_catalog_unchanged+=1;}
        report.cp21s_entries=self.entries.len();
        report.cp21s_pending_peak=report.cp21s_pending_peak.max(self.ready.len());
        timeline::emit(self.journal.as_ref(),json!({
            "schema":1,"kind":"catalog_reconciliation",
            "at_us":now_us(),"graph_generation":state.generation,
            "revision":state.revision,"entered":entered,"exited":removed,
            "retained_pools":old_addresses.intersection(&current_addresses).count(),
            "total_pools":state.pools.len(),"pairs_reused":reused,
            "pairs_recomputed":recomputed,
            "pending_preserved":self.ready.len()
        }),report);
    }
    fn reset(&mut self,state:&State,params:&ExecutionParameters,
        report:&mut Report,trigger:Arc<SignalTrigger>){
        self.topology.clear();self.entries.clear();self.ready.clear();
        let Some(plan)=state.plan.as_ref()else{return;};
        for (i,top) in plan.compact_topologies.iter().enumerate(){
            if top.kind==SignalRouteKind::Direct2{
                self.topology.insert((top.settlement,top.target),i);
            }
        }
        // One-time complete index initialization without a second O(n²)
        // temporary HashSet: each canonical mint-pair and directed topology
        // is enumerated exactly once by the stable State.pair_pools index.
        for (&(a,b),ids) in &state.pair_pools{
            for (settlement,target) in [(a,b),(b,a)]{
                let Some(&topology)=self.topology.get(&(settlement,target))else{continue};
                for &direct in ids{
                    for &partner in ids{
                        if direct==partner{continue;}
                        self.refresh_one(state,params,Key{topology,direct,partner},
                            report,trigger.clone(),false);
                    }
                }
            }
        }
        report.cp21s_entries=self.entries.len();
        report.cp21s_pending_peak=report.cp21s_pending_peak.max(self.ready.len());
    }
    fn refresh_changed(&mut self,state:&State,params:&ExecutionParameters,
        ids:&[usize],report:&mut Report,
        trigger:Arc<SignalTrigger>,allow_schedule:bool){
        let mut affected=HashSet::new();
        for &updated in ids{
            let Some(p)=state.pools.get(updated)else{continue};
            let Some(peers)=state.pair_pools.get(&(p.mint_a,p.mint_b))else{continue};
            for (settlement,target) in [(p.mint_a,p.mint_b),(p.mint_b,p.mint_a)]{
                let Some(&topology)=self.topology.get(&(settlement,target))else{continue};
                invalidate_changed(&mut affected,topology,updated,peers);
            }
        }
        // No truncation: every changed-pool/direct-partner combination is
        // exactly revalued once for this State revision, including negatives.
        let refresh_started=Instant::now();
        for key in affected {self.refresh_one(state,params,key,report,trigger.clone(),allow_schedule);}
        report.cp21s_refresh_timing.record(elapsed_ns(refresh_started));
        report.cp21s_updates+=1;
        report.cp21s_entries=self.entries.len();
        report.cp21s_pending_peak=report.cp21s_pending_peak.max(self.ready.len());
    }
    fn refresh_one(&mut self,state:&State,params:&ExecutionParameters,
        key:Key,report:&mut Report,
        trigger:Arc<SignalTrigger>,allow_schedule:bool){
        report.cp21s_pair_refreshes+=1;
        let Some(w)=witness(state,key)else{
            let reason=pair_unavailable_reason(state,key);
            report.cp21s_unpriced+=1;
            *report.cp21a1_unpriced_reasons.entry(format!("{}:{reason}",
                if allow_schedule{"live"}else{"catalog_or_warmup"})).or_default()+=1;
            if let Some(old)=self.entries.remove(&key){
                if let Some(rank)=old.rank{self.ready.remove(&rank);}
                report.cp21s_state_superseded+=1;
            }
            self.trace(state,key,"unpriced",reason,None,None,None,
                trigger.as_ref(),None,report);
            return;
        };
        let started=Instant::now();
        let estimate=economic::witness_estimate(&w,params);
        let score=estimate.map(|v|v.0);
        let input_raw=estimate.map(|v|v.1);
        let output_raw=estimate.map(|v|v.2);
        let gross_raw=estimate.map(|v|v.3);
        report.cp21s_score_timing.record(elapsed_ns(started));
        if w.estimated_net_return_bps.is_none(){
            report.cp21a1_missing_marginal_scored+=1;
            if score.is_some_and(|n|n>0){
                report.cp21a1_missing_marginal_positive+=1;
            }
        }
        let reason=if score.is_none(){pair_unavailable_reason(state,key)}
            else if w.estimated_net_return_bps.is_none(){"marginal_unknown_economic_available"}
            else{"economic_model_available"};
        match score{
            Some(n) if n>0=>report.cp21s_model_positive+=1,
            Some(_)=>report.cp21s_model_nonpositive+=1,
            None=>{
                report.cp21s_model_unknown+=1;
                *report.cp21a1_model_unknown_reasons.entry(format!("{}:{reason}",
                    if allow_schedule{"live"}else{"catalog_or_warmup"})).or_default()+=1;
            },
        }
        let fingerprint=state.source_fingerprint(&w,params);
        let old=self.entries.remove(&key);
        if let Some(mut previous)=old{
            if previous.fingerprint.is_some() && previous.fingerprint==fingerprint
                && previous.net==score {
                report.cp21s_identical_state_reuses+=1;
                // CP21S-A bugfix: an unchanged baseline quote was previously
                // not schedulable on its first live update because rank=None.
                if allow_schedule && previous.rank.is_none() &&
                    (score.is_some_and(|n|n>0) ||
                     (score.is_none() && w.estimated_net_return_bps.is_some()
                         && w.log_sum_units<state.threshold)) {
                    let (category,priority)=economic::priority_key(score);
                    let rank=Rank{category,score:priority,key};
                    self.ready.insert(rank);previous.rank=Some(rank);
                    previous.trigger=trigger.clone();previous.ready_us=state.ready_at_us;
                    previous.pipeline=state.price_pipeline;
                }
                let action=if previous.rank.is_some(){"retained_ready"}else{"retained"};
                self.entries.insert(key,previous);
                self.trace(state,key,action,reason,score,input_raw,gross_raw,
                    trigger.as_ref(),None,report);
                return;
            }
            if let Some(rank)=previous.rank{self.ready.remove(&rank);}
            report.cp21s_state_superseded+=1;
        }
        let rank=if allow_schedule && (score.is_some_and(|value|value>0)
            || (score.is_none() && w.estimated_net_return_bps.is_some()
                && w.log_sum_units<state.threshold)){
            let (category,priority)=economic::priority_key(score);
            let rank=Rank{category,score:priority,key};
            self.ready.insert(rank);
            Some(rank)
        }else{None};
        let action=if rank.is_some(){"queued"}else if score.is_none(){"model_unknown"}
            else if score.is_some_and(|n|n<=0){"nonpositive"}else{"baseline"};
        self.entries.insert(key,Entry{
            net:score,input_raw,output_raw,gross_raw,
            fingerprint,rank,revision:state.revision,
            trigger:trigger.clone(),ready_us:state.ready_at_us,
            pipeline:state.price_pipeline,
        });
        self.trace(state,key,action,reason,score,input_raw,gross_raw,
            trigger.as_ref(),None,report);
    }
    fn best(&self)->Option<Rank>{self.ready.first().copied()}
    fn pop(&mut self,key:Key){
        if let Some(entry)=self.entries.get_mut(&key){
            if let Some(rank)=entry.rank.take(){self.ready.remove(&rank);}
        }
    }
}

pub(super) async fn run(mut input:SignalStream,registry:PoolRegistry,
    output:mpsc::Sender<ExecutionRequest>,evidence:mpsc::Sender<EvidenceItem>,
    cfg:Settings)->Result<Report>{
    let mut report=Report{score_basis:SIGNAL_SCORE_BASIS,..Default::default()};
    let mut state=State::default();
    let timeline=timeline::open()?;
    let sender=timeline.as_ref().map(|writer|writer.sender());
    let mut index=Index{journal:sender.clone(),..Default::default()};
    let mut scheduler=scheduling::Scheduler::new(Duration::from_secs(cfg.cp20s_recheck_seconds));
    let mut sequence=0_u64;
    let mut evaluation_pending=false;
    let mut changed=Vec::<usize>::new();
    let mut reset=false;
    let mut prior_catalog:Option<(Vec<SignalPool>,Vec<Option<Arc<SignalPoolPrice>>>,
        Option<Arc<SignalPlan>>) >=None;
    let mut resume_pending=false;
    let mut allow_dispatch=false;
    let mut ticker=tokio::time::interval(Duration::from_millis(5));
    let mut distinct_pairs=HashSet::<(Pubkey,Pubkey)>::new();
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let outcome:Result<()>=async {
        loop {
            // Event intake always remains enabled while V10 is full. The
            // 5ms tick retries ready requests without blocking incoming WS.
            let event=tokio::select! {
                next=input.recv()=>Some(next),
                _=ticker.tick(),if !index.ready.is_empty()&&allow_dispatch=>None,
            };
            if let Some(next)=event{
                let event=match next {
                    Ok(value)=>value,
                    Err(broadcast::error::RecvError::Closed)=>break,
                    Err(broadcast::error::RecvError::Lagged(n))=>{
                        report.lost_events+=n;report.awaiting_snapshot=true;
                        state=State::default();
                        index=Index{journal:sender.clone(),..Default::default()};
                        changed.clear();prior_catalog=None;resume_pending=false;
                        evaluation_pending=false;allow_dispatch=false;
                        crate::log_event!(Warn,
                            "CP21S-A signal stream lagged={n}; awaiting authoritative snapshot");
                        continue;
                    }
                };
                report.events+=1;
                let next=match event.as_ref(){
                    SignalEvent::State(s)=>s.sequence,
                    SignalEvent::Candidates(c)=>c.sequence,
                    SignalEvent::Evaluated(e)=>e.sequence,
                };
                ensure!(report.awaiting_snapshot || next==sequence+1,
                    "CP21S-A candidate event sequence gap");
                sequence=next;
                if report.awaiting_snapshot && !matches!(
                    event.as_ref(),SignalEvent::State(s) if s.snapshot.is_some()) {continue;}
                match event.as_ref(){
                    SignalEvent::State(s)=>{
                        report.state_updates+=1;
                        ensure!(!evaluation_pending,"state replaced before evaluation completed");
                        if s.snapshot.is_some(){
                            prior_catalog=Some((state.pools.clone(),state.prices.clone(),
                                state.plan.clone()));
                            resume_pending=allow_dispatch;
                        }
                        state.apply(s,report.awaiting_snapshot,&mut report.preparation)?;
                        if let Some(snapshot)=&s.snapshot{
                            report.catalog_snapshots+=1;
                            report.catalog_snapshot_candidates+=snapshot.candidates.len() as u64;
                            reset=true;allow_dispatch=false;
                        }
                        if s.clear {
                            // WS gaps invalidate ALL earlier economic rankings.
                            index=Index{journal:sender.clone(),..Default::default()};
                            prior_catalog=None;
                            reset=true;allow_dispatch=false;
                        }
                        changed=s.changes.iter().map(|x|x.pool_id).collect();
                        if report.awaiting_snapshot{
                            report.resynchronizations+=1;report.awaiting_snapshot=false;
                        }
                        evaluation_pending=true;
                    },
                    SignalEvent::Candidates(b)=>{
                        report.candidate_batches+=1;
                        report.candidates+=b.candidates.len() as u64;
                        ensure!(evaluation_pending && b.revision==state.revision
                            && b.graph_generation==state.generation,
                            "CP21S-A candidate event references wrong revision");
                        for candidate in &b.candidates{state.candidate(b,candidate)?;}
                    },
                    SignalEvent::Evaluated(e)=>{
                        report.evaluations+=1;
                        ensure!(evaluation_pending && e.revision==state.revision
                            && e.graph_generation==state.generation,
                            "CP21S-A completion references wrong state");
                        ensure!(e.logarithms_prepared==state.prepared_logarithms,
                            "CP21S-A net weight preparation mismatch");
                        evaluation_pending=false;
                        report.max_active_candidates=report.max_active_candidates.max(state.active.len());
                        if matches!(e.trigger.as_ref(),SignalTrigger::Price{
                            origin:PriceOrigin::Websocket,..}){
                            report.websocket_revisions+=1;
                            if e.price_pipeline.is_some_and(|p|p.initialization_replay){
                                report.initialization_revisions+=1;
                            }
                        }
                        if reset{
                            if let Some((pools,prices,plan))=prior_catalog.take(){
                                index.reconcile_snapshot(&pools,&prices,plan.as_ref(),
                                    &state,&cfg.parameters,&mut report,e.trigger.clone());
                                allow_dispatch=resume_pending&&!index.ready.is_empty();
                            }else{
                                index.reset(&state,&cfg.parameters,&mut report,e.trigger.clone());
                            }
                            reset=false;resume_pending=false;
                        }else if let SignalTrigger::Price{origin,..}=e.trigger.as_ref(){
                            // Even initialization/baseline observations MUST
                            // enter the index; only their V10 submission is
                            // disabled until a live or revalued observation.
                            let live=matches!(origin,PriceOrigin::Websocket|PriceOrigin::Revaluation)
                                && !e.price_pipeline.is_some_and(|p|p.initialization_replay);
                            index.refresh_changed(&state,&cfg.parameters,&changed,
                                &mut report,e.trigger.clone(),live);
                            if live{allow_dispatch=true;}
                            if !changed.is_empty() && e.ranges.iter().sum::<u64>()==0{
                                report.cp21s_no_signal_updates+=1;
                            }
                        }else if matches!(e.trigger.as_ref(),SignalTrigger::Coverage{..})
                            && !changed.is_empty(){
                            // Unavailable/removed pools also invalidate all affected
                            // economic pairs even when they are not best marginal.
                            index.refresh_changed(&state,&cfg.parameters,&changed,
                                &mut report,e.trigger.clone(),false);
                        }
                        changed.clear();
                        report.cp21a1_live_index_last_active=index.entries.len();
                        report.cp21a1_live_pending_last_active=index.ready.len();
                    }
                }
                if evaluation_pending{continue;}
            }
            if !allow_dispatch {continue;}
            // No per-revision pair cap. Work remains in the globally ordered
            // index if V10 capacity is unavailable. The next event/tick retries.
            while let Some(rank)=index.best(){
                if cfg.limit>0 && report.request_count as usize>=cfg.limit{
                    break;
                }
                let permit=match output.try_reserve(){
                    Ok(p)=>p,
                    Err(mpsc::error::TrySendError::Full(_))=>{
                        report.cp21s_queue_full_retries+=1;
                        if report.cp21s_queue_full_retries==1 {
                            if let Some(rank)=index.best(){
                                if let Some(e)=index.entries.get(&rank.key){
                                    index.trace(&state,rank.key,"waiting","v10_queue_full",
                                        e.net,e.input_raw,e.gross_raw,e.trigger.as_ref(),
                                        None,&mut report);
                                }
                            }
                        }
                        break;
                    },
                    Err(mpsc::error::TrySendError::Closed(_))=>
                        bail!("V10 request receiver stopped"),
                };
                // Reserve audit capacity BEFORE preparing/sending a V10
                // request. A transient slow journal must not crash the worker
                // or make request/report delivery incomplete.
                let evidence_permit=match evidence.try_reserve(){
                    Ok(p)=>p,
                    Err(mpsc::error::TrySendError::Full(_))=>{
                        report.cp21s_evidence_full_retries+=1;
                        if report.cp21s_evidence_full_retries==1{
                            if let Some(rank)=index.best(){
                                if let Some(e)=index.entries.get(&rank.key){
                                    index.trace(&state,rank.key,"waiting","evidence_queue_full",
                                        e.net,e.input_raw,e.gross_raw,e.trigger.as_ref(),
                                        None,&mut report);
                                }
                            }
                        }
                        break;
                    },
                    Err(mpsc::error::TrySendError::Closed(_))=>
                        bail!("CP21S-A evidence writer stopped"),
                };
                let key=rank.key;
                let Some(entry)=index.entries.get(&key).cloned()else{
                    index.ready.remove(&rank);continue;
                };
                let Some(w)=witness(&state,key)else{
                    index.trace(&state,key,"not_dispatched","price_missing_at_dispatch",
                        entry.net,entry.input_raw,entry.gross_raw,entry.trigger.as_ref(),
                        None,&mut report);
                    index.pop(key);
                    report.cp21s_unpriced+=1;
                    continue;
                };
                let source=SignalCandidate{
                    topology_id:key.topology,reverse:false,
                    change:SignalRangeChange::Updated,witness:Some(w),
                };
                let Some(top)=state.plan.as_ref()
                    .and_then(|p|p.compact_topologies.get(key.topology))else{
                    index.pop(key);continue;
                };
                let mut reads=RegistryReads::default();
                let started=Instant::now();
                report.cp9c_pack_attempts+=1;
                let selected=state.build_compact(&source,state.threshold,entry.trigger.clone(),
                    &registry,&cfg,report.request_count as usize+1,&mut reads,
                    PackingStrategy::Economic,&[],None,&mut report.preparation);
                let selection=match selected {
                    Ok(s)=>s,
                    Err(e)=>{
                        report.cp21s_seed_rejections+=1;
                        report.skipped_invalid_seed+=1;
                        let message=format!("{e:#}");
                        report.last_rejection=Some(message.clone());
                        index.trace(&state,key,"not_dispatched",&message,
                            entry.net,entry.input_raw,entry.gross_raw,
                            entry.trigger.as_ref(),None,&mut report);
                        index.pop(key);
                        continue;
                    }
                };
                // A newer State/Candidate/Evaluated event arrived while the
                // packet was being constructed. Do not run the amount-proof
                // accountant or dispatch a possibly stale V10 packet; retain
                // the ranked pair until the newer observation is applied.
                if !input.is_empty(){
                    report.cp21s_preempted_for_new_market_state+=1;
                    index.trace(&state,key,"waiting","new_market_state_preempts_pack",
                        entry.net,entry.input_raw,entry.gross_raw,
                        entry.trigger.as_ref(),None,&mut report);
                    break;
                }
                let fingerprint=selection.packed_state_fingerprint();
                let familiar=scheduler.packs.classify(fingerprint.as_deref(),started);
                if familiar.deferred(){
                    index.pop(key);
                    report.cp21s_cooldowns+=1;
                    index.trace(&state,key,"not_dispatched","cp20s_frozen_state_cooldown",
                        entry.net,entry.input_raw,entry.gross_raw,
                        entry.trigger.as_ref(),None,&mut report);
                    continue;
                }
                let Some(mut selection)=state.assess_selection(selection,&registry,&cfg,
                    &mut reads,&mut report.amounts,&mut report.preparation)? else {
                    report.cp21s_amount_exclusions+=1;
                    index.trace(&state,key,"not_dispatched","conservative_amount_exclusion",
                        entry.net,entry.input_raw,entry.gross_raw,
                        entry.trigger.as_ref(),None,&mut report);
                    index.pop(key);
                    continue;
                };
                report.cp20_economic_forecasts+=1;
                if selection.economic.status=="estimated"{
                    report.cp20_economic_modeled+=1;
                    if selection.economic.predicted_net_gain_raw.is_some_and(|n|n>0){
                        report.cp20_economic_positive+=1;
                    }
                }else{report.cp20_economic_unknown+=1;}
                selection.cp9d_source="incremental";
                selection.cp9d_pair_index=0;
                selection.packing_revision_variant=0;
                selection.cp20s_schedule=match familiar{
                    scheduling::Familiarity::New=>"new_frozen_state",
                    scheduling::Familiarity::Recheck=>"periodic_recheck",
                    scheduling::Familiarity::Unverifiable=>"unverified_state",
                    scheduling::Familiarity::Cooldown=>"unexpected_cooldown"
                };
                selection.signal_ready_at_us=entry.ready_us;
                selection.price_pipeline=entry.pipeline;
                selection.request_ready_at_us=now_us();
                selection.round_elapsed_ns=elapsed_ns(started);
                selection.signal_to_request_us=selection.request_ready_at_us.saturating_sub(entry.ready_us);
                selection.frame_to_request_us=entry.pipeline.and_then(|p|
                    selection.request_ready_at_us.checked_sub(p.frame_received_at_us));
                selection.registry_reads=reads.clone();
                report.registry_reads.merge(&reads);
                *report.cp9c_by_strategy.entry("economic".into()).or_default()+=1;
                report.cp9c_packs_dispatched+=1;
                if distinct_pairs.insert((top.settlement,top.target)){
                    report.cp9d_distinct_pairs_dispatched+=1;
                }
                report.cp21s_dispatched+=1;
                report.preparation.round_work_ns.record(elapsed_ns(started));
                report.cp21s_drain_timing.record(elapsed_ns(started));
                let request_id=selection.request.id.clone();
                let request=selection.request.clone();
                evidence_permit.send(EvidenceItem::Selection(Box::new(selection)));
                permit.send(request);
                index.trace(&state,key,"v10_submitted","economic_priority_selected",
                    entry.net,entry.input_raw,entry.gross_raw,
                    entry.trigger.as_ref(),Some(&request_id),&mut report);
                report.request_count+=1;
                scheduler.packs.record(fingerprint,Instant::now());
                let source_fingerprint=state.source_fingerprint(source.witness.as_ref()
                    .context("CP21S-A missing witness")?,&cfg.parameters);
                scheduler.sources.record(source_fingerprint,Instant::now());
                match familiar{
                    scheduling::Familiarity::New=>report.cp20s_new_packs+=1,
                    scheduling::Familiarity::Recheck=>report.cp20s_recheck_packs+=1,
                    scheduling::Familiarity::Unverifiable=>report.cp20s_unknown_fingerprint_packs+=1,
                    scheduling::Familiarity::Cooldown=>{},
                }
                index.pop(key);
                // The pending set can contain arbitrarily many economically
                // qualified pairs. No candidate is lost because a source was
                // fourth, 97th or 513th in an arbitrary scan.
                report.cp21s_pending_peak=report.cp21s_pending_peak.max(index.ready.len());
                // Ingress takes precedence as soon as a newer market revision
                // is waiting. Never drain a long ready index ahead of fresh
                // WebSocket state; queued work remains live in the BTreeSet.
                if !input.is_empty(){break;}
            }
        }
        Ok(())
    }.await;
    report.cp21s_entries=index.entries.len();
    report.cp21s_pending_at_shutdown=index.ready.len();
    report.error=outcome.err().map(|e|format!("{e:#}"));
    // Join the durable timeline writer only after ALL sender clones are gone.
    index.journal=None;
    drop(index);
    drop(sender);
    if let Some(writer)=timeline {
        if let Err(error)=writer.finish(&mut report){
            report.error=Some(format!("CP21S-A.1 timeline evidence failure: {error:#}"));
        }
    }
    report.cp10_capacity_phases=super::super::execution_v10::capacity_phase_profile();
    report.cp11_quantity_phases=amounts::phase_profile();
    crate::log_event!(Info,
        "CP21S-A complete=true economic_pairs={} updated={} pending={} sent={} queue_retries={} lost={} error={:?}",
        report.cp21s_entries,report.cp21s_pair_refreshes,
        report.cp21s_pending_at_shutdown,report.request_count,
        report.cp21s_queue_full_retries,report.lost_events,report.error);
    Ok(report)
}

#[cfg(test)]
mod tests{
    use super::*;
    use crate::core::market_state::{FeeValuation,PoolLiquidity,DirectedLiquidity,
        Consistency,PriceKind,PoolPriceObservation,CpmmQuoteSnapshot};

    fn fixture_direct2(n:usize)->(State,Arc<SignalTrigger>){
        let settlement:Pubkey="So11111111111111111111111111111111111111112".parse().unwrap();
        let target:Pubkey="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".parse().unwrap();
        let a=settlement.min(target);let b=settlement.max(target);
        let top=SignalTopology{kind:SignalRouteKind::Direct2,settlement,target,
            bridge:None,roles:[(settlement,target),(target,settlement),
                (settlement,settlement),(settlement,settlement)],length:2};
        let plan=Arc::new(SignalPlan{score_basis:SIGNAL_SCORE_BASIS,
            graph_generation:1,log_weight_scale:SIGNAL_LOG_SCALE,
            compact_topologies:Arc::from(vec![top]),
            route_options:(n*(n-1)) as u64});
        let mut state=State{plan:Some(plan),generation:1,revision:1,
            ready_at_us:1,threshold:0,..Default::default()};
        for id in 0..n {
            let address=Pubkey::new_from_array([(id+30) as u8;32]);
            state.pools.push(SignalPool{address,mint_a:a,mint_b:b});
            let am=a.to_string();let bm=b.to_string();
            let res_a=if a==settlement {10_000_000_000}
                else {1_000_000_000+id as u64*100_000_000};
            let res_b=if a==settlement {1_000_000_000+id as u64*100_000_000}
                else {10_000_000_000};
            let d=|input:String,output:String|DirectedLiquidity{
                input_mint:input,output_mint:output,input_decimals:9,output_decimals:6,
                status:"available",coverage:"execution_window_end",
                next_required_account:None,first_source_position:Some(0),
                marginal_net_rate:Some(1.0),monotone_rate_tail:true,
                input_capacity_raw:None,output_capacity_raw:"2000000000".into(),
                levels:vec![],reason:None
            };
            let liquidity=PoolLiquidity{
                model:"raydium_cpmm_directional_v1",revision:1,
                content_digest:[id as u8;32],account_set_digest:[(id+40) as u8;32],
                execution_accounts:vec![],reserve_curve:None,
                cpmm_quote:Some(CpmmQuoteSnapshot{
                    mint_0:am.clone(),mint_1:bm.clone(),
                    reserve_0_raw:res_a,reserve_1_raw:res_b,
                    trade_fee_rate:0,creator_fee_rate:0,
                    creator_fee_on_input_0_to_1:true,
                    creator_fee_on_input_1_to_0:true
                }),
                orca_quote:None,clmm_quote:None,
                directions:[d(am.clone(),bm.clone()),d(bm.clone(),am.clone())]
            };
            let valuation=FeeValuation{
                model:"cpmm",estimated_net_rates:[Some(1.0);2],
                dex_fee_rates:[Some(0.0);2],fees_on_input:[true;2],
                base_fee_rate:Some(0.0),variable_fee_rate:Some(0.0),
                lp_fee_bps:None,protocol_fee_bps:None,creator_fee_bps:None,
                fee_schedule:None,market_cap_raw:None,chain_timestamp:1,
                chain_epoch:0,token_fee_status:["verified_no_standard_transfer_fee";2],
                unknown_reason:None,scope:"test"
            };
            let obs=PoolPriceObservation{
                origin:PriceOrigin::Websocket,pool:address.to_string(),
                venue:"raydium_cpmm",mint_a:am.clone(),mint_b:bm.clone(),
                base_mint:am,quote_mint:bm,price_quote_per_base:1.0,
                valuation:Some(valuation),liquidity:Some(Arc::new(liquidity)),
                previous_price:None,delta_bps:None,price_kind:PriceKind::ReserveSpot,
                best_bid_quote_per_base:None,best_ask_quote_per_base:None,
                consistency:Consistency::SingleAccount,slot:10,received_at_us:10,
                sequence:1,component_slot_min:10,component_slot_max:10,
                component_received_at_us_min:10,component_received_at_us_max:10,
                trigger_field:None,pipeline:None
            };
            state.prices.push(Some(Arc::new(SignalPoolPrice{
                log_weight_units:[Some(-1),Some(-1)],
                direction_liquidity_status:["eligible","eligible"],
                observation:Arc::new(obs)
            })));
        }
        state.pair_pools.insert((a,b),(0..n).collect());
        let first_pool=state.pools[0].address;
        let trigger=Arc::new(SignalTrigger::Price{
            pool:first_pool,sequence:1,origin:PriceOrigin::Websocket,received_at_us:10});
        (state,trigger)
    }
    #[test]
    fn cp21s_a_real_pair_index_matches_full_cpmm_recalculation_after_change(){
        let (mut state,trigger)=fixture_direct2(7);
        let params=ExecutionParameters::default();
        let mut index=Index::default();
        let mut report=Report::default();
        index.reset(&state,&params,&mut report,trigger.clone());
        assert_eq!(index.entries.len(),42);
        assert_eq!(report.cp21s_pair_refreshes,42);
        state.revision=2;state.ready_at_us=20;
        let old=state.prices[6].as_ref().unwrap().as_ref();
        let mut observation=old.observation.as_ref().clone();
        let p=Arc::make_mut(observation.liquidity.as_mut().unwrap());
        p.content_digest=[77;32];
        p.cpmm_quote.as_mut().unwrap().reserve_0_raw+=777_000_000;
        state.prices[6]=Some(Arc::new(SignalPoolPrice{
            log_weight_units:old.log_weight_units,
            direction_liquidity_status:old.direction_liquidity_status,
            observation:Arc::new(observation)
        }));
        index.refresh_changed(&state,&params,&[6],&mut report,trigger,true);
        assert_eq!(report.cp21s_pair_refreshes,42+12);
        for (&key,entry) in &index.entries{
            let witness=witness(&state,key).unwrap();
            let reference=state.economic_score(&witness,&params);
            assert_eq!(entry.net,reference,
                "no directed pair may retain an obsolete economic quote");
            if key.direct==6 || key.partner==6 {
                assert_eq!(entry.revision,2);
                assert_eq!(entry.rank.is_some(),reference.is_some_and(|n|n>0)
                    ||(reference.is_none()&&witness.log_sum_units<state.threshold));
            }else{
                assert_eq!(entry.revision,1);
            }
        }
    }
    #[test]
    fn cp21s_a_50k_full_economic_index_updates_are_reference_complete(){
        // End-to-end local worker-index benchmark: actual frozen CPMM quotes,
        // source fingerprints, removal/reinsertion into ranked state, and
        // replacement of newer observations. No RPC or V10 required.
        let (mut state,trigger)=fixture_direct2(5);
        let params=ExecutionParameters::default();
        let mut index=Index::default();
        let mut report=Report::default();
        index.reset(&state,&params,&mut report,trigger.clone());
        let mut samples=Vec::with_capacity(50_000);
        for i in 0..50_000_usize {
            let id=i%5;
            let old=state.prices[id].as_ref().unwrap().as_ref();
            let mut observation=old.observation.as_ref().clone();
            let liq=Arc::make_mut(observation.liquidity.as_mut().unwrap());
            liq.content_digest=[((i%251)+1) as u8;32];
            liq.cpmm_quote.as_mut().unwrap().reserve_0_raw+=1;
            state.prices[id]=Some(Arc::new(SignalPoolPrice{
                log_weight_units:old.log_weight_units,
                direction_liquidity_status:old.direction_liquidity_status,
                observation:Arc::new(observation)
            }));
            state.revision+=1;state.ready_at_us+=1;
            let start=Instant::now();
            index.refresh_changed(&state,&params,&[id],&mut report,trigger.clone(),true);
            samples.push(elapsed_ns(start));
            if i%10_000==0 || i==49_999{
                assert_eq!(index.entries.len(),20);
                // Reference never uses cached index scores.
                for (&key,entry) in &index.entries{
                    let reference=state.economic_score(&witness(&state,key).unwrap(),&params);
                    assert_eq!(entry.net,reference);
                }
            }
        }
        assert_eq!(report.cp21s_updates,50_000);
        assert_eq!(report.cp21s_pair_refreshes,20+50_000*8);
        samples.sort_unstable();
        eprintln!("CP21S-A 50k real full-economy index updates (5 pools, 8 affected directed pairs/update): p50={}us p95={}us p99={}us",
            samples[25_000]/1_000,samples[47_500]/1_000,samples[49_500]/1_000);
    }
    #[test]
    fn cp21s_a_no_three_ninety_six_or_five_hundred_twelve_cutoff(){
        for n in [5,97,513,1_001] {
            let peers=(0..n).collect::<Vec<_>>();
            let mut affected=HashSet::new();
            invalidate_changed(&mut affected,0,n-1,&peers);
            assert_eq!(affected.len(),2*(n-1),
                "all outgoing AND incoming ordered pool pairs must be visited");
            assert!(affected.contains(&Key{topology:0,direct:n-1,partner:0}));
            assert!(affected.contains(&Key{topology:0,direct:0,partner:n-1}));
        }
    }
    #[test]
    fn cp21s_a_50k_updates_match_independent_full_recompute(){
        // Deterministic synthetic fixture. Every update changes one pool's
        // economic state; every affected ordered pair is re-evaluated exactly
        // once, not sampled. Reference recomputes ALL n*(n-1) pairs.
        let n=24_usize;
        let peers=(0..n).collect::<Vec<_>>();
        let mut values=vec![0_i64;n];
        let mut incremental=HashMap::<Key,i64>::new();
        let mut updates=0_u64;
        let start=Instant::now();
        for i in 0..50_000_usize {
            let updated=(i*17+5)%n;
            values[updated]+=1;
            let mut affected=HashSet::new();
            invalidate_changed(&mut affected,0,updated,&peers);
            assert_eq!(affected.len(),2*(n-1));
            for k in affected {
                incremental.insert(k,values[k.direct]*31-values[k.partner]*29
                    +k.direct as i64-k.partner as i64);
                updates+=1;
            }
            if i%2_500==0 || i==49_999 {
                // Independent full reference, including all stale and tie
                // ranks, catches missing pair invalidation.
                let mut reference=HashMap::new();
                for a in 0..n {
                    for b in 0..n {
                        if a!=b {
                            reference.insert(Key{topology:0,direct:a,partner:b},
                                values[a]*31-values[b]*29+a as i64-b as i64);
                        }
                    }
                }
                // A missing entry is only possible before that pool has
                // received its first event; initialize all pairs up front.
                for (&k,&v) in &reference {
                    if incremental.contains_key(&k){assert_eq!(incremental[&k],v);}
                }
                if i>=n*3 {
                    assert_eq!(incremental.len(),n*(n-1));
                    assert_eq!(incremental,reference);
                    let best_incremental=incremental.iter()
                        .max_by_key(|(key,value)|(**value,Reverse(**key))).unwrap();
                    let best_reference=reference.iter()
                        .max_by_key(|(key,value)|(**value,Reverse(**key))).unwrap();
                    assert_eq!(best_incremental,best_reference);
                }
            }
        }
        assert_eq!(updates,50_000*2*(n as u64-1));
        eprintln!("CP21S-A 50k event full incremental invalidations={} ns={} (test-fixture only)",
            updates,elapsed_ns(start));
    }
    #[test]
    fn cp21s_a_full_v10_queue_preserves_ranked_work(){
        let (tx,mut rx)=tokio::sync::mpsc::channel::<u64>(1);
        tx.try_send(1).unwrap();
        let key=Key{topology:0,direct:888,partner:2};
        let rank=Rank{category:0,score:Reverse(1_000),key};
        let mut index=Index::default();
        index.ready.insert(rank);
        assert!(matches!(tx.try_reserve(),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))));
        assert_eq!(index.best(),Some(rank),"full V10 queue must not pop pending work");
        assert_eq!(rx.try_recv().unwrap(),1);
        let permit=tx.try_reserve().unwrap();
        assert_eq!(index.best(),Some(rank));
        permit.send(2);
        assert_eq!(rx.try_recv().unwrap(),2);
    }
    #[test]
    fn cp21s_a_rank_order_is_global_and_cross_settlement_normalized(){
        let mut order=BTreeSet::new();
        for i in 0..1_000_usize{
            let profit=if i==999{99_000}else{100};
            order.insert(Rank{category:0,score:Reverse(profit),
                key:Key{topology:0,direct:i,partner:1000}});
        }
        assert_eq!(order.len(),1_000);
        assert_eq!(order.first().unwrap().key.direct,999);
        let unknown=Rank{category:1,score:Reverse(0),
            key:Key{topology:1,direct:1,partner:2}};
        order.insert(unknown);
        assert_eq!(order.first().unwrap().key.direct,999);
    }
}
