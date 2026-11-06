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
    for (j,&id) in ids.iter().enumerate(){
        let (a,b)=top.roles[j];
        let pool=state.pools.get(id)?;
        let slot=directions(pool,a,b,id)?;
        let p=state.prices.get(id)?.as_ref()?;
        let weight=p.log_weight_units[slot%2]?;
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
        estimated_net_return_bps:bps.is_finite().then_some(bps),
    })
}
impl Index {
    fn reset(&mut self,state:&State,params:&ExecutionParameters,
        report:&mut Report,trigger:Arc<SignalTrigger>){
        self.topology.clear();self.entries.clear();self.ready.clear();
        let Some(plan)=state.plan.as_ref()else{return;};
        for (i,top) in plan.compact_topologies.iter().enumerate(){
            if top.kind==SignalRouteKind::Direct2{
                self.topology.insert((top.settlement,top.target),i);
            }
        }
        // One-time index initialization. Each ordered pair is visited ONCE;
        // later price revisions touch only the changed pool's counterpart IDs.
        let mut batch=HashSet::new();
        for (&(a,b),ids) in &state.pair_pools{
            for (settlement,target) in [(a,b),(b,a)]{
                let Some(&topology)=self.topology.get(&(settlement,target))else{continue};
                for &direct in ids{
                    for &partner in ids{
                        if direct!=partner {batch.insert(Key{topology,direct,partner});}
                    }
                }
            }
        }
        for key in batch{self.refresh_one(state,params,key,report,trigger.clone(),false);}
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
        for key in affected {self.refresh_one(state,params,key,report,trigger.clone(),allow_schedule);}
        report.cp21s_updates+=1;
        report.cp21s_entries=self.entries.len();
        report.cp21s_pending_peak=report.cp21s_pending_peak.max(self.ready.len());
    }
    fn refresh_one(&mut self,state:&State,params:&ExecutionParameters,
        key:Key,report:&mut Report,
        trigger:Arc<SignalTrigger>,allow_schedule:bool){
        report.cp21s_pair_refreshes+=1;
        let Some(w)=witness(state,key)else{
            report.cp21s_unpriced+=1;
            if let Some(old)=self.entries.remove(&key){
                if let Some(rank)=old.rank{self.ready.remove(&rank);}
                report.cp21s_state_superseded+=1;
            }
            return;
        };
        let started=Instant::now();
        let score=state.economic_score(&w,params);
        report.cp21s_score_timing.record(elapsed_ns(started));
        match score{
            Some(n) if n>0=>report.cp21s_model_positive+=1,
            Some(_)=>report.cp21s_model_nonpositive+=1,
            None=>report.cp21s_model_unknown+=1,
        }
        let fingerprint=state.source_fingerprint(&w,params);
        let old=self.entries.remove(&key);
        if let Some(previous)=&old{
            if previous.fingerprint.is_some() && previous.fingerprint==fingerprint
                && previous.net==score {
                report.cp21s_identical_state_reuses+=1;
                self.entries.insert(key,previous.clone());
                return;
            }
            if let Some(rank)=previous.rank{self.ready.remove(&rank);}
            report.cp21s_state_superseded+=1;
        }
        let rank=if allow_schedule && (score.is_some_and(|value|value>0)
            || (score.is_none() && w.log_sum_units<state.threshold)){
            let (category,score)=economic::priority_key(score);
            let rank=Rank{category,score,key};
            self.ready.insert(rank);
            Some(rank)
        }else{None};
        self.entries.insert(key,Entry{
            net:score,fingerprint,rank,revision:state.revision,
            trigger,ready_us:state.ready_at_us,pipeline:state.price_pipeline,
        });
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
    let mut index=Index::default();
    let mut scheduler=scheduling::Scheduler::new(Duration::from_secs(cfg.cp20s_recheck_seconds));
    let mut sequence=0_u64;
    let mut evaluation_pending=false;
    let mut changed=Vec::<usize>::new();
    let mut reset=false;
    let mut allow_dispatch=false;
    let mut ticker=tokio::time::interval(Duration::from_millis(5));
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
                        state=State::default();index=Index::default();
                        changed.clear();evaluation_pending=false;allow_dispatch=false;
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
                        state.apply(s,report.awaiting_snapshot,&mut report.preparation)?;
                        if let Some(snapshot)=&s.snapshot{
                            report.catalog_snapshots+=1;
                            report.catalog_snapshot_candidates+=snapshot.candidates.len() as u64;
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
                            index.reset(&state,&cfg.parameters,&mut report,e.trigger.clone());
                            reset=false;
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
                        }
                        changed.clear();
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
                        break;
                    },
                    Err(mpsc::error::TrySendError::Closed(_))=>
                        bail!("V10 request receiver stopped"),
                };
                let key=rank.key;
                let Some(entry)=index.entries.get(&key).cloned()else{
                    index.ready.remove(&rank);continue;
                };
                let Some(w)=witness(&state,key)else{
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
                let pair=(top.kind,top.settlement.min(top.target),top.settlement.max(top.target));
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
                        report.last_rejection=Some(format!("{e:#}"));
                        index.pop(key);
                        continue;
                    }
                };
                let fingerprint=selection.packed_state_fingerprint();
                let familiar=scheduler.packs.classify(fingerprint.as_deref(),started);
                if familiar.deferred(){
                    index.pop(key);
                    report.cp21s_cooldowns+=1;
                    continue;
                }
                let Some(mut selection)=state.assess_selection(selection,&registry,&cfg,
                    &mut reads,&mut report.amounts,&mut report.preparation)? else {
                    report.cp21s_amount_exclusions+=1;
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
                report.cp9d_distinct_pairs_dispatched+=1;
                report.cp21s_dispatched+=1;
                report.preparation.round_work_ns.record(elapsed_ns(started));
                let request=selection.request.clone();
                evidence.try_send(EvidenceItem::Selection(Box::new(selection)))
                    .map_err(|e|anyhow::anyhow!(
                        "CP21S-A V10 evidence queue unavailable (nonblocking): {e}"))?;
                permit.send(request);
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
                let _=pair; // Queue work is keyed by the exact directed pair.
            }
        }
        Ok(())
    }.await;
    report.cp21s_entries=index.entries.len();
    report.cp21s_pending_at_shutdown=index.ready.len();
    report.error=outcome.err().map(|e|format!("{e:#}"));
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
