//! Versioned indicative ranges + current Prepared proofs -> frozen V10 jobs.
//! Selection is downstream of calculation; no RPC, signer, decoding or sending.
use std::{collections::HashSet, env, sync::Arc, time::{Instant, SystemTime, UNIX_EPOCH}};
use anyhow::{Context, Result, bail, ensure};
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast, mpsc};
use crate::core::{CompletionState, execution::{ExecutionGroup, ExecutionMetrics,
    ExecutionOutcome, ExecutionParameters, ExecutionPool, ExecutionReport, ExecutionRequest}, market_state::{PriceOrigin,PricePipelineTiming},
    signals::{SignalEvent, SignalGroup, SignalPlan, SignalPool, SignalPoolPrice,
        SignalRangeChange, SignalRouteKind, SignalRouteRange, SignalStateUpdate,
        SignalStream, SignalTrigger, SIGNAL_LOG_SCALE}};
use super::pool_registry::PoolRegistry;

pub struct Settings {
    pub wallet: Pubkey,
    pub parameters: ExecutionParameters,
    pub limit: usize,
    pub kind: Option<SignalRouteKind>,
    /// Public typed packing boundary, supplied by orchestration. Never a signer.
    pub capacity: fn(&ExecutionRequest) -> Result<ExecutionMetrics>,
}
impl Settings {
    pub fn from_env(wallet: Pubkey, capacity: fn(&ExecutionRequest)->Result<ExecutionMetrics>) -> Result<Self> {
        let limit=env::var("GRAPH_EXECUTION_MAX_REQUESTS").unwrap_or_else(|_|"10".into()).parse()?;
        ensure!((1..=1000).contains(&limit), "GRAPH_EXECUTION_MAX_REQUESTS must be 1..1000");
        let kind=match env::var("GRAPH_EXECUTION_ROUTE_KIND").unwrap_or_else(|_|"All".into()).to_ascii_lowercase().as_str() {
            "all"=>None, "direct2"=>Some(SignalRouteKind::Direct2), "direct3"=>Some(SignalRouteKind::Direct3),
            "triangle3"=>Some(SignalRouteKind::Triangle3), "triangle4"=>Some(SignalRouteKind::Triangle4),
            _=>bail!("GRAPH_EXECUTION_ROUTE_KIND must be All, Direct2, Direct3, Triangle3 or Triangle4"),
        };
        let parameters=match env::var("GRAPH_EXECUTION_PARAMETERS_PATH") {
            Ok(path) if !path.is_empty()=>serde_json::from_slice(&std::fs::read(path)
                .context("read execution parameters")?).context("decode ExecutionParameters")?,
            _=>ExecutionParameters::default(),
        };
        Ok(Self {wallet,parameters,limit,kind,capacity})
    }
}
fn now_us()->u64 {SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default()
    .as_micros().min(u128::from(u64::MAX)) as u64}
fn elapsed_ns(start:Instant)->u64 {start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64}

#[derive(serde::Serialize)]
struct PoolEvidence {
    pool: String, role: &'static str, slot: usize, log_weight_units: i64,
    observation: Arc<crate::core::market_state::PoolPriceObservation>,
    manifest_validated_slot: u64, manifest_checked_at_ms: Option<u64>,
}
#[derive(serde::Serialize)]
pub struct Selection {
    pub request: ExecutionRequest,
    graph_generation: u64, revision: u64, group_id: usize, route_kind: SignalRouteKind,
    reverse: bool, trigger: Arc<SignalTrigger>, seed: SignalRouteRange,
    threshold_units: i64, indicative_return_bps: Option<f64>,
    pools: Vec<PoolEvidence>, packing: ExecutionMetrics,
    signal_ready_at_us: u64, candidate_received_at_us: u64, request_ready_at_us: u64,
    price_pipeline: Option<PricePipelineTiming>,
    frame_to_request_us: Option<u64>, signal_to_request_us: u64,
    selection_ns: u64, alternatives_considered: u64, packing_rejections: u64,
    unavailable_alternatives: u64, last_alternative_rejection: Option<String>,
    alternative_pools_added: usize,
}
#[derive(Default, serde::Serialize)]
pub struct Report {
    pub error: Option<String>, pub requests: Vec<Selection>,
    events: u64, websocket_revisions: u64, initialization_revisions: u64, qualifying_ranges: u64,
    lost_events: u64, resynchronizations: u64, skipped_busy_revisions: u64,
    skipped_invalid_seed: u64, last_rejection: Option<String>, awaiting_snapshot: bool,
    manifest_refresh_performed: bool,
}
impl Report {
    pub fn ensure_complete(&self)->Result<()> {
        ensure!(self.error.is_none(), "execution candidate worker failed: {}", self.error.as_deref().unwrap_or(""));
        Ok(())
    }
    pub fn ensure_handoff(&self, reports:&[ExecutionReport])->Result<()> {
        self.ensure_complete()?;
        ensure!(!self.requests.is_empty(), "no qualifying live request was produced; inspect coverage, threshold and candidate rejections");
        ensure!(self.requests.len()==reports.len(), "candidate/request report counts differ");
        for selection in &self.requests {
            let matched=reports.iter().filter(|r|r.id==selection.request.id).collect::<Vec<_>>();
            ensure!(matched.len()==1, "missing or duplicate execution report for {}",selection.request.id);
            let report=matched[0];
            ensure!(report.error.is_none() && report.phase=="complete" && report.executor_invoked
                && report.simulation.is_some(), "request {} did not reach an executor simulation",report.id);
            ensure!(serde_json::to_value(&selection.packing)?==serde_json::to_value(&report.metrics)?,
                "selected/simulated packing differs for {}",report.id);
        }
        Ok(())
    }
    pub fn ensure_live_evidence(&self, reports:&[ExecutionReport])->Result<()> {
        self.ensure_handoff(reports)?;
        ensure!(reports.iter().any(|r|matches!(r.outcome,
            ExecutionOutcome::SimulatedNoDexCallObserved|ExecutionOutcome::SimulatedDexCallsObserved)),
            "handoff observed, but no successful simulation; on-chain errors remain unclassified");
        Ok(())
    }
}

/// A consumer-owned view of the published contract, not another route engine.
#[derive(Default)]
struct State {
    plan: Option<Arc<SignalPlan>>, pools: Vec<SignalPool>,
    prices: Vec<Option<Arc<SignalPoolPrice>>>, revision: u64, generation: u64,
    ready_at_us: u64, received_at_us: u64,
    price_pipeline: Option<PricePipelineTiming>,
}
impl State {
    fn apply(&mut self, s:&SignalStateUpdate, recovering:bool)->Result<()> {
        self.received_at_us=now_us();
        ensure!(recovering || s.revision==self.revision+1, "candidate state revision gap");
        if let Some(snapshot)=&s.snapshot {
            ensure!(snapshot.plan.graph_generation==s.graph_generation
                && snapshot.plan.log_weight_scale==SIGNAL_LOG_SCALE
                && snapshot.pools.len()==snapshot.prices.len(), "invalid candidate snapshot");
            self.plan=Some(snapshot.plan.clone()); self.pools=snapshot.pools.clone(); self.prices=snapshot.prices.clone();
            self.generation=s.graph_generation;
        } else {ensure!(self.plan.is_some() && s.graph_generation==self.generation, "candidate delta without snapshot");}
        if s.clear {self.prices.fill(None);}
        for change in &s.changes {
            if change.pool_id==self.pools.len() {self.pools.push(change.identity.clone()); self.prices.push(None);}
            ensure!(self.pools.get(change.pool_id)==Some(&change.identity), "candidate pool identity mismatch");
            self.prices[change.pool_id]=change.price.clone();
        }
        self.revision=s.revision; self.ready_at_us=s.ready_at_us; self.price_pipeline=s.price_pipeline;
        Ok(())
    }
    fn weight(&self, slot:usize)->Option<i64> {
        self.prices.get(slot/2)?.as_ref()?.log_weight_units[slot%2]
    }
    fn pool(&self, slot:usize, role:&'static str, registry:&PoolRegistry, wallet:Pubkey)
        ->Result<(ExecutionPool,PoolEvidence)> {
        let identity=self.pools.get(slot/2).context("candidate slot identity missing")?;
        let price=self.prices.get(slot/2).and_then(|p|p.as_ref()).context("candidate price unavailable")?;
        let state=registry.get(identity.address).context("candidate missing from Registry")?;
        ensure!(state.completion_admission.is_admitted() && state.completion_state==CompletionState::Prepared,
            "candidate {} no longer admitted Prepared",identity.address);
        let manifest=state.execution_manifest.context("Prepared manifest missing")?;
        let mints=state.mint_proofs.context("Prepared mint proofs missing")?;
        ensure!(manifest.pool==identity.address && state.mint_a==Some(manifest.mint_a)
            && state.mint_b==Some(manifest.mint_b)
            && [manifest.mint_a.min(manifest.mint_b),manifest.mint_a.max(manifest.mint_b)]==[identity.mint_a,identity.mint_b],
            "candidate Registry/price/manifest identity mismatch");
        ensure!(manifest.bound_signer.is_none_or(|bound|bound==wallet), "candidate manifest belongs to another wallet");
        let evidence=PoolEvidence {pool:identity.address.to_string(),role,slot,
            log_weight_units:self.weight(slot).context("candidate direction unavailable")?,
            observation:price.observation.clone(),manifest_validated_slot:manifest.validated_slot,
            manifest_checked_at_ms:state.completion_checked_at_ms};
        Ok((ExecutionPool{manifest,mints},evidence))
    }
    fn build(&self, seed:&SignalRouteRange, threshold:i64, trigger:Arc<SignalTrigger>,
        registry:&PoolRegistry, cfg:&Settings, number:usize)->Result<Selection> {
        let started=Instant::now();
        let plan=self.plan.as_ref().context("candidate plan unavailable")?;
        let g=plan.groups.get(seed.group_id).context("candidate group unavailable")?;
        let d=usize::from(seed.reverse);
        let left=&plan.roles[g.left[d]].slots; let right=&plan.roles[g.right[d]].slots;
        let seed_left=seed.witness.legs.iter().find(|l|left.contains(&l.slot)).context("seed left role missing")?.slot;
        let seed_right=seed.witness.legs.iter().find(|l|right.contains(&l.slot)).context("seed right role missing")?.slot;
        ensure!(seed.witness.legs.iter().all(|leg|self.weight(leg.slot)==Some(leg.log_weight_units))
            && seed.witness.legs.iter().map(|leg|leg.log_weight_units).sum::<i64>()==seed.witness.log_sum_units,
            "seed does not match current published prices");
        let mut slots=vec![(seed_left,"direct"),(seed_right,"partner")];
        for (i,&slot) in g.fixed[..g.fixed_count].iter().enumerate() {
            slots.push((slot^d,if i==0 && matches!(g.kind,SignalRouteKind::Triangle3|SignalRouteKind::Triangle4) {"intermediate"}else{"conversion"}));
        }
        let mut request=ExecutionRequest {id:format!("graph-{}-{}-{number}",self.generation,self.revision),
            wallet:cfg.wallet,settlement:g.mints[0],groups:Vec::new(),conversion:None,parameters:cfg.parameters.clone()};
        let mut pools=Vec::new(); let mut used=HashSet::new();
        for &(slot,role) in &slots {
            ensure!(used.insert(slot/2), "seed repeats a pool");
            let (pool,evidence)=self.pool(slot,role,registry,cfg.wallet)?;
            pools.push((pool,evidence));
        }
        install(&mut request,g,&pools);
        let mut packing=(cfg.capacity)(&request).context("seed does not fit V10")?;
        let mut candidates=Vec::new();
        let fixed_sum=slots[2..].iter().map(|&(slot,_)|self.weight(slot).context("fixed price unavailable")).collect::<Result<Vec<_>>>()?.iter().sum::<i64>();
        for (role,partner,slots) in [("direct",seed_right,left),("partner",seed_left,right)] {
            for &slot in slots.iter() {
                if slot/2==partner/2 || used.contains(&(slot/2)) {continue;}
                if let Some(weight)=self.weight(slot) {
                    let score=weight+self.weight(partner).context("seed price unavailable")?+fixed_sum;
                    if score<threshold {candidates.push((score,slot,role));}
                }
            }
        }
        // Greedy indicative ranking inside one topology, no Cartesian-product expansion.
        candidates.sort_unstable();
        let mut considered=0; let mut packing_rejections=0; let mut unavailable_alternatives=0;
        let mut last_alternative_rejection=None;
        for (_,slot,role) in candidates {
            if used.contains(&(slot/2)) {continue;}
            considered+=1;
            let pool=match self.pool(slot,role,registry,cfg.wallet) {
                Ok(pool)=>pool,
                Err(error)=>{unavailable_alternatives+=1;last_alternative_rejection=Some(format!("{error:#}"));continue;},
            };
            pools.push(pool); install(&mut request,g,&pools);
            match (cfg.capacity)(&request) {
                Ok(metrics)=>{packing=metrics;used.insert(slot/2);},
                Err(error)=>{pools.pop();packing_rejections+=1;last_alternative_rejection=Some(format!("{error:#}"));},
            }
        }
        install(&mut request,g,&pools);
        let alternative_pools_added=pools.len()-slots.len();
        let ready=now_us();
        let frame=self.price_pipeline.map(|t|t.frame_received_at_us);
        Ok(Selection {request,graph_generation:self.generation,revision:self.revision,group_id:seed.group_id,
            route_kind:g.kind,reverse:seed.reverse,trigger,seed:seed.clone(),threshold_units:threshold,
            indicative_return_bps:seed.witness.indicative_return_bps,
            pools:pools.into_iter().map(|(_,e)|e).collect(),packing,signal_ready_at_us:self.ready_at_us,
            candidate_received_at_us:self.received_at_us,request_ready_at_us:ready,
            price_pipeline:self.price_pipeline,
            frame_to_request_us:frame.and_then(|at|ready.checked_sub(at)),
            signal_to_request_us:ready.saturating_sub(self.ready_at_us),selection_ns:elapsed_ns(started),
            alternatives_considered:considered,packing_rejections,unavailable_alternatives,
            last_alternative_rejection,alternative_pools_added})
    }
}

fn install(request:&mut ExecutionRequest, g:&SignalGroup, pools:&[(ExecutionPool,PoolEvidence)]) {
    let direct=pools.iter().filter(|(_,e)|e.role=="direct").map(|(p,_)|p.clone()).collect::<Vec<_>>();
    let partners=pools.iter().filter(|(_,e)|e.role=="partner").map(|(p,_)|p.clone()).collect::<Vec<_>>();
    request.conversion=pools.iter().find(|(_,e)|e.role=="conversion").map(|(p,_)|p.clone());
    request.groups=vec![match g.kind {
        SignalRouteKind::Direct2|SignalRouteKind::Direct3=>ExecutionGroup::Direct {
            target:g.mints[1],pools:direct.into_iter().chain(partners).collect(),
        },
        SignalRouteKind::Triangle3|SignalRouteKind::Triangle4=>ExecutionGroup::Bridge {
            target:g.mints[1],bridge:g.mints[2],
            intermediate:pools.iter().find(|(_,e)|e.role=="intermediate").expect("seed intermediate").0.clone(),
            bridges:partners,direct,
        },
    }];
}

pub async fn run(mut input:SignalStream, registry:PoolRegistry, output:mpsc::Sender<ExecutionRequest>, cfg:Settings)->Result<Report> {
    let mut report=Report::default(); let mut state=State::default(); let mut sequence=0;
    // Retain range references only until Evaluated; observations stay Arc-shared.
    let mut ranked:Vec<(i64,Arc<SignalEvent>,usize)>=Vec::new();
    let mut pending=false;
    let outcome:Result<()>=async {
        loop {
            let event=match input.recv().await {
                Ok(event)=>event,
                Err(broadcast::error::RecvError::Closed)=>break,
                Err(broadcast::error::RecvError::Lagged(n))=>{
                    report.lost_events+=n;report.awaiting_snapshot=true;state=State::default();ranked.clear();pending=false;
                    crate::log_event!(Warn,"execution candidates lost={n} awaiting_snapshot=true requests_paused=true");
                    continue;
                },
            };
            report.events+=1;
            let next=match event.as_ref() {SignalEvent::State(s)=>s.sequence,SignalEvent::Ranges(b)=>b.sequence,SignalEvent::Evaluated(e)=>e.sequence};
            ensure!(report.awaiting_snapshot || next==sequence+1,"candidate event sequence gap"); sequence=next;
            if report.awaiting_snapshot && !matches!(event.as_ref(),SignalEvent::State(s) if s.snapshot.is_some()) {continue;}
            match event.as_ref() {
                SignalEvent::State(s)=>{
                    ensure!(!pending,"candidate state replaced before evaluation completed");
                    state.apply(s,report.awaiting_snapshot)?;
                    if report.awaiting_snapshot {report.resynchronizations+=1;report.awaiting_snapshot=false;}
                    pending=true;ranked.clear();
                },
                SignalEvent::Ranges(b)=>{
                    ensure!(pending && b.revision==state.revision && b.graph_generation==state.generation,"candidate ranges reference wrong state");
                    if report.requests.len()>=cfg.limit || !matches!(b.trigger.as_ref(),SignalTrigger::Price{origin:PriceOrigin::Websocket,..})
                        || !b.price_pipeline.is_some_and(|t|!t.initialization_replay) {continue;}
                    for (index,range) in b.ranges.iter().enumerate() {
                        if range.change==SignalRangeChange::Exited {continue;}
                        let group=state.plan.as_ref().and_then(|p|p.groups.get(range.group_id)).context("candidate range group missing")?;
                        if cfg.kind.is_some_and(|kind|kind!=group.kind) {continue;}
                        report.qualifying_ranges+=1;
                        ensure!(range.witness.log_sum_units<b.threshold_units,"candidate seed exceeds threshold");
                        ranked.push((range.witness.log_sum_units,event.clone(),index));
                    }
                },
                SignalEvent::Evaluated(e)=>{
                    ensure!(pending && e.revision==state.revision && e.graph_generation==state.generation,"candidate completion references wrong state");
                    pending=false;
                    if matches!(e.trigger.as_ref(),SignalTrigger::Price{origin:PriceOrigin::Websocket,..}) {
                        report.websocket_revisions+=1;
                        if e.price_pipeline.is_some_and(|t|t.initialization_replay) {report.initialization_revisions+=1;}
                    }
                    if ranked.is_empty() {continue;}
                    // Never await simulation capacity or propagate backpressure to calculation.
                    let permit=match output.try_reserve() {
                        Ok(permit)=>permit,
                        Err(mpsc::error::TrySendError::Full(_))=>{report.skipped_busy_revisions+=1;continue;},
                        Err(mpsc::error::TrySendError::Closed(_))=>bail!("V10 request receiver stopped"),
                    };
                    let started=Instant::now();
                    ranked.sort_unstable_by_key(|entry|entry.0);
                    let mut chosen=None;
                    for (_,event,index) in &ranked {
                        let SignalEvent::Ranges(block)=event.as_ref() else {unreachable!("stored range event")};
                        match state.build(&block.ranges[*index],block.threshold_units,block.trigger.clone(),&registry,&cfg,report.requests.len()+1) {
                            Ok(mut selection)=>{selection.selection_ns=elapsed_ns(started);chosen=Some(selection);break;},
                            Err(error)=>{report.skipped_invalid_seed+=1;report.last_rejection=Some(format!("{error:#}"));},
                        }
                    }
                    ranked.clear();
                    if let Some(selection)=chosen {
                        crate::log_event!(Info,"execution candidates request_id={} generation={} revision={} kind={:?} reverse={} pools={} accounts={} selection_ns={} frame_to_request_us={:?} broadcasts=false",
                            selection.request.id,state.generation,state.revision,selection.route_kind,selection.reverse,
                            selection.packing.pool_declarations,selection.packing.unique_accounts,selection.selection_ns,selection.frame_to_request_us);
                        permit.send(selection.request.clone());report.requests.push(selection);
                    } else {
                        crate::log_event!(Warn,"execution candidates no_packable_seed=true generation={} revision={} last_rejection={:?}",
                            state.generation,state.revision,report.last_rejection);
                    }
                },
            }
        }
        ensure!(!pending,"candidate stream closed during evaluation");
        Ok(())
    }.await;
    report.error=outcome.err().map(|e|format!("{e:#}"));
    crate::log_event!(Info,"execution candidates complete=true requests={} lost={} error={:?}",report.requests.len(),report.lost_events,report.error);
    Ok(report)
}
