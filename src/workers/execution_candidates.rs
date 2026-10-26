//! Versioned compact best-candidates + current Prepared proofs -> frozen V10 jobs.
//! Selection is downstream of calculation; no RPC, signer, decoding or sending.
use std::{collections::HashSet, env, sync::Arc, time::{Instant, SystemTime, UNIX_EPOCH}};
use anyhow::{Context, Result, bail, ensure};
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast, mpsc};
use crate::core::{CompletionState, execution::{ExecutionGroup, ExecutionMetrics,
    ExecutionOutcome, ExecutionParameters, ExecutionPool, ExecutionReport, ExecutionRequest}, market_state::{PriceOrigin,PricePipelineTiming},
    signals::{SignalCandidate, SignalEvent, SignalPlan, SignalPool, SignalPoolPrice, SignalTopology,
        SignalRangeChange, SignalRouteKind, SignalStateUpdate,
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
        ensure!((1..=5000).contains(&limit), "GRAPH_EXECUTION_MAX_REQUESTS must be 1..5000");
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
    graph_generation: u64, revision: u64, topology_id: Option<usize>, route_kind: SignalRouteKind,
    reverse: bool, trigger: Arc<SignalTrigger>, candidate: Option<SignalCandidate>,
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
    /// Pack an exact CP6A best-witness without rebuilding any route product.
    /// Alternatives are drawn from currently priced slots with the same mint roles.
    fn build_compact(&self,c:&SignalCandidate,threshold:i64,trigger:Arc<SignalTrigger>,
        registry:&PoolRegistry,cfg:&Settings,number:usize)->Result<Selection>{
        let started=Instant::now();
        let plan=self.plan.as_ref().context("compact candidate plan missing")?;
        let top=plan.compact_topologies.get(c.topology_id)
            .context("compact topology missing")?;
        ensure!(c.change!=SignalRangeChange::Exited,"cannot pack exited candidate");
        let witness=c.witness.as_ref().context("compact candidate witness missing")?;
        ensure!(witness.legs.len()==top.length
            &&witness.log_sum_units<threshold,"invalid compact candidate length/score");
        let role=|index:usize|->&'static str{
            match top.kind {
                SignalRouteKind::Direct2=>if index==0{"direct"}else{"partner"},
                SignalRouteKind::Direct3=>match index{0=>"direct",1=>"partner",_=>"conversion"},
                SignalRouteKind::Triangle3=>match index{0=>"direct",1=>"intermediate",_=>"partner"},
                SignalRouteKind::Triangle4=>match index{
                    0=>"direct",1=>"intermediate",2=>"partner",_=>"conversion"},
            }
        };
        let mut slots=Vec::<(usize,&'static str)>::new();
        let mut used=HashSet::new();
        for (i,leg) in witness.legs.iter().enumerate(){
            let j=if c.reverse{top.length-1-i}else{i};
            let (a,b)=top.roles[j];
            let (input,output)=if c.reverse{(b,a)}else{(a,b)};
            ensure!((leg.input_mint,leg.output_mint)==(input,output)
                && used.insert(leg.slot/2),"compact witness has wrong or duplicate pools");
            ensure!(self.weight(leg.slot)==Some(leg.log_weight_units),
                "compact candidate is stale relative to price revision");
            slots.push((leg.slot,role(j)));
        }
        ensure!(witness.legs.last().is_some_and(|l|l.output_mint==top.settlement)
            &&witness.legs[0].input_mint==top.settlement,
            "compact route does not settle");
        let mut request=ExecutionRequest{id:format!("graph-{}-{}-{number}",self.generation,self.revision),
            wallet:cfg.wallet,settlement:top.settlement,groups:Vec::new(),conversion:None,
            parameters:cfg.parameters.clone()};
        let mut pools=Vec::new();
        for &(slot,r) in &slots{
            pools.push(self.pool(slot,r,registry,cfg.wallet)?);
        }
        install_compact(&mut request,top,&pools);
        let mut packing=(cfg.capacity)(&request).context("compact seed does not fit V10")?;
        let mut options=Vec::new();
        // Only direct/partner roles accept parallel alternatives in the current
        // V10 group contract. Intermediate/conversion remain the best chosen pool.
        for (i,&(selected,role_name)) in slots.iter().enumerate(){
            if role_name!="direct"&&role_name!="partner"{continue;}
            let target=self.pools.get(selected/2).context("candidate identity missing")?;
            let (a,b)=(target.mint_a,target.mint_b);
            let original=self.weight(selected).context("seed weight missing")?;
            for (id,p) in self.pools.iter().enumerate(){
                if p.mint_a!=a||p.mint_b!=b||used.contains(&id){continue;}
                let slot=2*id+selected%2;
                if let Some(w)=self.weight(slot){
                    let new_sum=i128::from(witness.log_sum_units)-i128::from(original)+i128::from(w);
                    if new_sum<i128::from(threshold){
                        options.push((new_sum,slot,role_name,i));
                    }
                }
            }
        }
        options.sort_unstable_by_key(|(score,slot,_,_)|(*score,*slot));
        let mut considered=0;let mut rejected=0;let mut unavailable=0;
        let mut last_rejection=None;
        for (_,slot,role_name,_) in options{
            if used.contains(&(slot/2)){continue;}
            considered+=1;
            let pool=match self.pool(slot,role_name,registry,cfg.wallet){
                Ok(pool)=>pool,Err(error)=>{
                    unavailable+=1;last_rejection=Some(format!("{error:#}"));continue;
                }
            };
            pools.push(pool);
            install_compact(&mut request,top,&pools);
            match (cfg.capacity)(&request){
                Ok(metrics)=>{packing=metrics;used.insert(slot/2);},
                Err(error)=>{pools.pop();rejected+=1;last_rejection=Some(format!("{error:#}"));}
            }
        }
        install_compact(&mut request,top,&pools);
        let ready=now_us();
        let frame=self.price_pipeline.map(|t|t.frame_received_at_us);
        let alternative_pools_added=pools.len()-slots.len();
        Ok(Selection{request,graph_generation:self.generation,revision:self.revision,
            topology_id:Some(c.topology_id),route_kind:top.kind,reverse:c.reverse,
            trigger,candidate:Some(c.clone()),threshold_units:threshold,
            indicative_return_bps:witness.indicative_return_bps,
            pools:pools.into_iter().map(|(_,e)|e).collect(),packing,
            signal_ready_at_us:self.ready_at_us,candidate_received_at_us:self.received_at_us,
            request_ready_at_us:ready,price_pipeline:self.price_pipeline,
            frame_to_request_us:frame.and_then(|at|ready.checked_sub(at)),
            signal_to_request_us:ready.saturating_sub(self.ready_at_us),selection_ns:elapsed_ns(started),
            alternatives_considered:considered,packing_rejections:rejected,
            unavailable_alternatives:unavailable,last_alternative_rejection:last_rejection,
            alternative_pools_added})
    }

}


fn install_compact(request:&mut ExecutionRequest,top:&SignalTopology,
    pools:&[(ExecutionPool,PoolEvidence)]){
    let direct=pools.iter().filter(|(_,e)|e.role=="direct")
        .map(|(p,_)|p.clone()).collect::<Vec<_>>();
    let partners=pools.iter().filter(|(_,e)|e.role=="partner")
        .map(|(p,_)|p.clone()).collect::<Vec<_>>();
    request.conversion=pools.iter().find(|(_,e)|e.role=="conversion")
        .map(|(p,_)|p.clone());
    request.groups=vec![match top.kind{
        SignalRouteKind::Direct2|SignalRouteKind::Direct3=>ExecutionGroup::Direct{
            target:top.target,pools:direct.into_iter().chain(partners).collect(),
        },
        SignalRouteKind::Triangle3|SignalRouteKind::Triangle4=>ExecutionGroup::Bridge{
            target:top.target,bridge:top.bridge.expect("triangle has bridge mint"),
            intermediate:pools.iter().find(|(_,e)|e.role=="intermediate")
                .expect("triangle has intermediate").0.clone(),
            bridges:partners,direct,
        },
    }];
}

pub async fn run(mut input:SignalStream, registry:PoolRegistry, output:mpsc::Sender<ExecutionRequest>, cfg:Settings)->Result<Report> {
    let mut report=Report::default(); let mut state=State::default(); let mut sequence=0;
    // Retain compact candidate references only until Evaluated; observations stay Arc-shared.
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
            let next=match event.as_ref() {SignalEvent::State(s)=>s.sequence,
                SignalEvent::Candidates(b)=>b.sequence,SignalEvent::Evaluated(e)=>e.sequence};
            ensure!(report.awaiting_snapshot || next==sequence+1,"candidate event sequence gap"); sequence=next;
            if report.awaiting_snapshot && !matches!(event.as_ref(),SignalEvent::State(s) if s.snapshot.is_some()) {continue;}
            match event.as_ref() {
                SignalEvent::State(s)=>{
                    ensure!(!pending,"candidate state replaced before evaluation completed");
                    state.apply(s,report.awaiting_snapshot)?;
                    if report.awaiting_snapshot {report.resynchronizations+=1;report.awaiting_snapshot=false;}
                    pending=true;ranked.clear();
                },
                SignalEvent::Candidates(b)=>{
                    ensure!(pending&&b.revision==state.revision&&b.graph_generation==state.generation,
                        "candidate batch references wrong state");
                    if report.requests.len()>=cfg.limit
                        ||!matches!(b.trigger.as_ref(),SignalTrigger::Price{origin:PriceOrigin::Websocket,..})
                        ||!b.price_pipeline.is_some_and(|t|!t.initialization_replay){continue;}
                    for (index,candidate) in b.candidates.iter().enumerate(){
                        if candidate.change==SignalRangeChange::Exited{continue;}
                        let top=state.plan.as_ref()
                            .and_then(|p|p.compact_topologies.get(candidate.topology_id))
                            .context("candidate references unknown topology")?;
                        if cfg.kind.is_some_and(|kind|kind!=top.kind){continue;}
                        if let Some(w)=candidate.witness.as_ref(){
                            ensure!(w.log_sum_units<b.threshold_units,
                                "best candidate does not satisfy threshold");
                            report.qualifying_ranges+=1;
                            ranked.push((w.log_sum_units,event.clone(),index));
                        }
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
                        let selected=match event.as_ref(){
                            SignalEvent::Candidates(block)=>state.build_compact(&block.candidates[*index],
                                block.threshold_units,block.trigger.clone(),&registry,&cfg,
                                report.requests.len()+1),
                            _=>bail!("ranked event is not a compact candidate"),
                        };
                        match selected {
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
