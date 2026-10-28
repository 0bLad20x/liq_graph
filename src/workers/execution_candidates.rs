//! Versioned compact best-candidates + current Prepared proofs -> frozen V10 jobs.
//! Selection is downstream of calculation; no RPC, signer, decoding or sending.
mod amounts;
use std::{collections::{HashMap,HashSet}, env, sync::Arc, time::{Instant, SystemTime, UNIX_EPOCH}};
use anyhow::{Context, Result, bail, ensure};
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast, mpsc};
use crate::core::{CompletionState, execution::{ExecutionGroup, ExecutionMetrics,
    ExecutionOutcome, ExecutionParameters, ExecutionPool, ExecutionReport, ExecutionRequest}, market_state::{PriceOrigin,PricePipelineTiming},
    signals::{SignalCandidate, SignalCandidateBatch, SignalEvent, SignalPlan, SignalPool, SignalPoolPrice, SignalTopology,SignalWitness,
        SignalRangeChange, SignalRouteKind, SignalStateUpdate,
        SignalStream, SignalTrigger, SIGNAL_LOG_SCALE,SIGNAL_SCORE_BASIS}};
use super::pool_registry::PoolRegistry;

pub struct Settings {
    pub wallet: Pubkey,
    pub parameters: ExecutionParameters,
    /// Zero means no lifetime request cap; the output queue remains bounded.
    pub limit: usize,
    pub kind: Option<SignalRouteKind>,
    /// Public typed packing boundary, supplied by orchestration. Never a signer.
    pub capacity: fn(&ExecutionRequest) -> Result<ExecutionMetrics>,
}
impl Settings {
    pub fn from_env(wallet: Pubkey, capacity: fn(&ExecutionRequest)->Result<ExecutionMetrics>) -> Result<Self> {
        let limit=env::var("GRAPH_EXECUTION_MAX_REQUESTS").unwrap_or_else(|_|"0".into()).parse()?;
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

#[derive(Clone,Default,serde::Serialize)]
struct RegistryReads {
    calls:u64,wait_ns:u64,hold_ns:u64,max_wait_ns:u64,max_hold_ns:u64,
}
impl RegistryReads {
    fn record(&mut self,wait:u64,hold:u64) {
        self.calls+=1;self.wait_ns=self.wait_ns.saturating_add(wait);
        self.hold_ns=self.hold_ns.saturating_add(hold);
        self.max_wait_ns=self.max_wait_ns.max(wait);self.max_hold_ns=self.max_hold_ns.max(hold);
    }
    fn merge(&mut self,other:&Self) {
        self.calls+=other.calls;self.wait_ns=self.wait_ns.saturating_add(other.wait_ns);
        self.hold_ns=self.hold_ns.saturating_add(other.hold_ns);
        self.max_wait_ns=self.max_wait_ns.max(other.max_wait_ns);
        self.max_hold_ns=self.max_hold_ns.max(other.max_hold_ns);
    }
}

#[derive(serde::Serialize)]
struct PoolEvidence {
    pool: String, role: &'static str, slot: usize, log_weight_units: i64,
    observation: Arc<crate::core::market_state::PoolPriceObservation>,
    manifest_validated_slot: u64, manifest_checked_at_ms: Option<u64>,
    liquidity_window_checked:bool,
}
#[derive(serde::Serialize)]
pub struct Selection {
    pub request: ExecutionRequest,
    graph_generation: u64, revision: u64, topology_id: Option<usize>, route_kind: SignalRouteKind,
    reverse: bool, trigger: Arc<SignalTrigger>, candidate: Option<SignalCandidate>,
    threshold_units: i64, estimated_net_return_bps:Option<f64>,
    pools: Vec<PoolEvidence>, packing: ExecutionMetrics,
    signal_ready_at_us: u64, candidate_received_at_us: u64, request_ready_at_us: u64,
    price_pipeline: Option<PricePipelineTiming>,
    frame_to_request_us: Option<u64>, signal_to_request_us: u64,
    selection_ns: u64, alternatives_considered: u64, packing_rejections: u64,
    registry_reads:RegistryReads,
    unavailable_alternatives: u64, last_alternative_rejection: Option<String>,
    alternative_pools_added: usize,
    /// May differ from the source SignalCandidate after quantity seed repair.
    packing_seed_log_sum_units:i64,
    amount_assessment:amounts::Assessment,amount_seed_changed:bool,
}
#[derive(Default, serde::Serialize)]
pub struct Report {
    pub error: Option<String>, pub requests: Vec<Selection>,
    score_basis:&'static str,
    pub events:u64,pub state_updates:u64,pub candidate_batches:u64,pub candidates:u64,pub evaluations:u64,
    websocket_revisions: u64, initialization_revisions: u64, qualifying_ranges: u64,
    lost_events: u64, resynchronizations: u64, skipped_busy_revisions: u64,
    skipped_invalid_seed: u64, last_rejection: Option<String>, awaiting_snapshot: bool,
    manifest_refresh_performed: bool,
    catalog_snapshots:u64, catalog_snapshot_candidates:u64, max_active_candidates:usize,
    registry_reads:RegistryReads,
    amounts:amounts::Report,
}
impl Report {
    pub fn ensure_complete(&self)->Result<()> {
        ensure!(self.error.is_none(), "execution candidate worker failed: {}", self.error.as_deref().unwrap_or(""));
        ensure!(self.score_basis==SIGNAL_SCORE_BASIS,"execution candidate score basis differs from net contract");
        ensure!(self.lost_events==0 && !self.awaiting_snapshot,
            "execution candidate input delivery incomplete: lost={} awaiting_snapshot={}",self.lost_events,self.awaiting_snapshot);
        ensure!(self.amounts.attempts==self.amounts.possible+self.amounts.unknown+self.amounts.excluded
            &&self.amounts.possible+self.amounts.unknown==self.requests.len() as u64,
            "quantity decision accounting mismatch");
        ensure!(self.requests.iter().all(|r|r.amount_assessment.decision!=amounts::Decision::Excluded),"excluded quantity request dispatched");
        Ok(())
    }
    pub fn ensure_handoff(&self, reports:&[ExecutionReport])->Result<()> {
        self.ensure_complete()?;
        ensure!(!self.requests.is_empty(), "no qualifying live request was produced; inspect coverage, threshold and candidate rejections");
        ensure!(self.requests.len()==reports.len(), "candidate/request report counts differ");
        let mut by_id=HashMap::with_capacity(reports.len());
        for report in reports {
            ensure!(by_id.insert(report.id.as_str(),report).is_none(),
                "duplicate execution report for {}",report.id);
        }
        let mut requested=HashSet::with_capacity(self.requests.len());
        for selection in &self.requests {
            ensure!(requested.insert(selection.request.id.as_str()),
                "duplicate execution request {}",selection.request.id);
            let report=by_id.get(selection.request.id.as_str())
                .with_context(||format!("missing execution report for {}",selection.request.id))?;
            ensure!(report.error.is_none() && report.phase=="complete" && report.executor_invoked
                && report.simulation.is_some(), "request {} did not reach an executor simulation",report.id);
            ensure!(serde_json::to_value(&selection.packing)?==serde_json::to_value(&report.metrics)?,
                "selected/simulated packing differs for {}",report.id);
        }
        Ok(())
    }
    /// Additional execution criterion, evaluated after ensure_handoff succeeds.
    pub fn ensure_successful_simulation(&self, reports:&[ExecutionReport])->Result<()> {
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
    threshold:i64, active:HashSet<(usize,bool)>,prepared_logarithms:u64,
}
impl State {
    fn amount_input(&self,id:usize,manifest_matches:bool)->Option<amounts::Input> {
        let price=self.prices.get(id)?.as_ref()?;let identity=self.pools.get(id)?;
        Some(amounts::Input{slot:2*id+usize::from(identity.mint_a.to_string()!=price.observation.base_mint),
            observation:price.observation.clone(),manifest_matches})
    }
    fn amount_universe(&self,top:&SignalTopology,registry:&PoolRegistry,wallet:Pubkey,reads:&mut RegistryReads)->Vec<amounts::Input> {
        let mints=top.roles[..top.length].iter().flat_map(|&(a,b)|[a,b]).collect::<HashSet<_>>();
        self.pools.iter().enumerate().filter_map(|(id,p)|{
            if !mints.contains(&p.mint_a)||!mints.contains(&p.mint_b){return None;}
            let mut input=self.amount_input(id,false)?;
            let (state,wait,hold)=registry.measured_get(p.address);reads.record(wait,hold);
            let state=state?;
            if !state.completion_admission.is_admitted()||state.completion_state!=CompletionState::Prepared{return None;}
            input.manifest_matches=state.execution_manifest.as_ref().is_some_and(|m|
                m.bound_signer.is_none_or(|w|w==wallet)&&input.observation.liquidity.as_ref()
                    .is_some_and(|q|q.account_set_digest==m.account_set_digest()));
            Some(input)
        }).collect()
    }
    fn packed_amount_inputs(&self,s:&Selection)->Vec<amounts::Input> {
        s.pools.iter().filter_map(|p|self.amount_input(p.slot/2,p.liquidity_window_checked)).collect()
    }
    fn assess_selection(&self,mut selection:Selection,registry:&PoolRegistry,cfg:&Settings,
        reads:&mut RegistryReads,report:&mut amounts::Report)->Result<Option<Selection>> {
        let started=Instant::now();
        let conversion_other=selection.request.conversion.as_ref().map(|p|
            if p.manifest.mint_a==selection.request.settlement{p.manifest.mint_b}else{p.manifest.mint_a}.to_string());
        let mut assessment=if cfg.parameters.trade_size!=0||cfg.parameters.constant_conversion||cfg.parameters.no_failure {
            amounts::Assessment::unknown("nondefault_execution_mode")
        }else{amounts::assess(&self.packed_amount_inputs(&selection),&selection.request.settlement.to_string(),cfg.parameters.minimum_profit,conversion_other.as_deref())};
        if assessment.decision==amounts::Decision::Excluded {
            let candidate=selection.candidate.as_ref().context("quantity candidate missing")?;
            let top=&self.plan.as_ref().context("quantity plan missing")?.compact_topologies[candidate.topology_id];
            let universe=self.amount_universe(top,registry,cfg.wallet,reads);
            let mut global=amounts::assess(&universe,&selection.request.settlement.to_string(),cfg.parameters.minimum_profit,conversion_other.as_deref());
            global.scope="current_prepared_candidate_mint_universe_dex_only";report.global_proofs+=1;
            if global.decision==amounts::Decision::Excluded {
                global.elapsed_ns=elapsed_ns(started);report.record(&global);
                report.retain_exclusion(global,&universe,serde_json::json!({
                    "graph_generation":self.generation,"signal_revision":self.revision,
                    "topology_id":candidate.topology_id,"reverse":candidate.reverse,
                    "trigger":selection.trigger,"source_witness":candidate.witness
                }));return Ok(None);
            }
            let witness=candidate.witness.as_ref().context("quantity witness missing")?;
            let pairs=witness.legs.iter().map(|l|(l.input_mint.to_string(),l.output_mint.to_string())).collect::<Vec<_>>();
            if let Some(preferred)=amounts::seed(&universe,&pairs) {
                if let Ok(mut repaired)=self.build_compact(candidate,selection.threshold_units,selection.trigger.clone(),
                    registry,cfg,selection.request.id.rsplit('-').next().context("request number absent")?.parse()?,reads,Some(&preferred)) {
                    if repaired.amount_seed_changed {
                        let next=amounts::assess(&self.packed_amount_inputs(&repaired),&repaired.request.settlement.to_string(),cfg.parameters.minimum_profit,conversion_other.as_deref());
                        if next.decision!=amounts::Decision::Excluded {
                            assessment=next;repaired.amount_seed_changed=true;report.seed_repaired+=1;selection=repaired;
                        }else{assessment=amounts::Assessment::unknown("unpacked_or_untested_alternative");}
                    }else{assessment=amounts::Assessment::unknown("unpacked_or_untested_alternative");}
                }else{assessment=amounts::Assessment::unknown("quantity_seed_does_not_fit");}
            }else{assessment=amounts::Assessment::unknown("alternative_quantity_unknown");}
        }
        assessment.elapsed_ns=elapsed_ns(started);report.record(&assessment);
        selection.amount_assessment=assessment;Ok(Some(selection))
    }
    fn apply(&mut self, s:&SignalStateUpdate, recovering:bool)->Result<()> {
        self.received_at_us=now_us();
        ensure!(recovering || s.revision==self.revision+1, "candidate state revision gap");
        if let Some(snapshot)=&s.snapshot {
            ensure!(snapshot.plan.graph_generation==s.graph_generation
                && snapshot.plan.log_weight_scale==SIGNAL_LOG_SCALE
                && snapshot.plan.score_basis==SIGNAL_SCORE_BASIS
                && snapshot.pools.len()==snapshot.prices.len(), "invalid candidate snapshot");
            self.plan=Some(snapshot.plan.clone()); self.pools=snapshot.pools.clone(); self.prices=snapshot.prices.clone();
            self.generation=s.graph_generation;
            self.threshold=snapshot.threshold_units;
            self.active.clear();
            for candidate in &snapshot.candidates {
                self.witness(candidate.topology_id,candidate.reverse,&candidate.witness)?;
                ensure!(self.active.insert((candidate.topology_id,candidate.reverse)),
                    "duplicate execution candidate snapshot key");
            }
        } else {ensure!(self.plan.is_some() && s.graph_generation==self.generation, "candidate delta without snapshot");}
        if s.clear {self.prices.fill(None);self.active.clear();}
        for change in &s.changes {
            if change.pool_id==self.pools.len() {self.pools.push(change.identity.clone()); self.prices.push(None);}
            ensure!(self.pools.get(change.pool_id)==Some(&change.identity), "candidate pool identity mismatch");
            self.prices[change.pool_id]=change.price.clone();
        }
        self.revision=s.revision; self.ready_at_us=s.ready_at_us; self.price_pipeline=s.price_pipeline;
        self.prepared_logarithms=if matches!(s.trigger.as_ref(),SignalTrigger::Price{..}) {
            s.changes.iter().filter_map(|c|c.price.as_ref())
                .map(|p|p.log_weight_units.iter().flatten().count() as u64).sum()
        }else{0};
        Ok(())
    }
    fn weight(&self, slot:usize)->Option<i64> {
        self.prices.get(slot/2)?.as_ref()?.log_weight_units[slot%2]
    }
    fn witness(&self,id:usize,reverse:bool,witness:&SignalWitness)->Result<()> {
        let top=self.plan.as_ref().and_then(|p|p.compact_topologies.get(id))
            .context("candidate state topology absent")?;
        ensure!(witness.legs.len()==top.length && (2..=4).contains(&top.length)
            && !(top.kind==SignalRouteKind::Direct2&&reverse),"candidate state length/direction invalid");
        let mut unique=HashSet::new();let mut sum=0_i128;
        for (i,leg) in witness.legs.iter().enumerate(){
            let j=if reverse{top.length-1-i}else{i};
            let (a,b)=top.roles[j];let (input,output)=if reverse{(b,a)}else{(a,b)};
            let p=self.pools.get(leg.slot/2).context("candidate state pool absent")?;
            ensure!((leg.input_mint,leg.output_mint)==(input,output)
                && (p.mint_a,p.mint_b)==(input.min(output),input.max(output))
                && leg.slot%2==usize::from(input!=p.mint_a)
                && unique.insert(leg.slot/2),"candidate state pool/direction invalid");
            ensure!(self.weight(leg.slot)==Some(leg.log_weight_units),"candidate state weight mismatch");
            sum+=i128::from(leg.log_weight_units);
        }
        ensure!(witness.legs[0].input_mint==top.settlement
            && witness.legs.last().is_some_and(|l|l.output_mint==top.settlement)
            && witness.legs.windows(2).all(|l|l[0].output_mint==l[1].input_mint)
            && sum==i128::from(witness.log_sum_units) && witness.log_sum_units<self.threshold,
            "candidate state score/closed route invalid");
        Ok(())
    }
    fn candidate(&mut self,b:&SignalCandidateBatch,c:&SignalCandidate)->Result<()> {
        ensure!(b.threshold_units==self.threshold,"candidate threshold changed without snapshot");
        let key=(c.topology_id,c.reverse);
        match c.change {
            SignalRangeChange::Exited=>ensure!(c.witness.is_none()&&self.active.remove(&key),
                "execution candidate exit missing or carries witness"),
            SignalRangeChange::Entered|SignalRangeChange::Updated=>{
                self.witness(c.topology_id,c.reverse,c.witness.as_ref().context("candidate witness absent")?)?;
                if c.change==SignalRangeChange::Entered {
                    ensure!(self.active.insert(key),"execution candidate entered twice");
                } else {ensure!(self.active.contains(&key),"execution candidate updated before entering");}
            },
        }
        Ok(())
    }
    fn pool(&self, slot:usize, role:&'static str, registry:&PoolRegistry, wallet:Pubkey,reads:&mut RegistryReads)
        ->Result<(ExecutionPool,PoolEvidence)> {
        let identity=self.pools.get(slot/2).context("candidate slot identity missing")?;
        let price=self.prices.get(slot/2).and_then(|p|p.as_ref()).context("candidate price unavailable")?;
        let (state,wait,hold)=registry.measured_get(identity.address);
        reads.record(wait,hold);
        let state=state.context("candidate missing from Registry")?;
        ensure!(state.completion_admission.is_admitted() && state.completion_state==CompletionState::Prepared,
            "candidate {} no longer admitted Prepared",identity.address);
        let manifest=state.execution_manifest.context("Prepared manifest missing")?;
        let mints=state.mint_proofs.context("Prepared mint proofs missing")?;
        ensure!(manifest.pool==identity.address && state.mint_a==Some(manifest.mint_a)
            && state.mint_b==Some(manifest.mint_b)
            && [manifest.mint_a.min(manifest.mint_b),manifest.mint_a.max(manifest.mint_b)]==[identity.mint_a,identity.mint_b],
            "candidate Registry/price/manifest identity mismatch");
        ensure!(manifest.bound_signer.is_none_or(|bound|bound==wallet), "candidate manifest belongs to another wallet");
        ensure!(state.venue!=crate::core::Venue::MeteoraDlmm || price.observation.liquidity.is_some(),
            "DLMM candidate has no observed liquidity profile");
        let liquidity_window_checked=if let Some(profile)=&price.observation.liquidity {
            ensure!(profile.account_set_digest==manifest.account_set_digest(),
                "candidate liquidity window differs from current Prepared account dependencies");
            let direction=usize::from(if slot%2==0{identity.mint_a.to_string()}else{identity.mint_b.to_string()}
                !=price.observation.base_mint);
            ensure!(profile.directions[direction].status=="available"
                &&profile.directions[direction].marginal_net_rate.is_some(),"candidate liquidity direction unavailable");
            true
        }else{false};
        let evidence=PoolEvidence {pool:identity.address.to_string(),role,slot,
            log_weight_units:self.weight(slot).context("candidate direction unavailable")?,
            observation:price.observation.clone(),manifest_validated_slot:manifest.validated_slot,
            manifest_checked_at_ms:state.completion_checked_at_ms,liquidity_window_checked};
        Ok((ExecutionPool{manifest,mints},evidence))
    }
    /// Pack an exact CP6A best-witness without rebuilding any route product.
    /// Alternatives are drawn from currently priced slots with the same mint roles.
    fn build_compact(&self,c:&SignalCandidate,threshold:i64,trigger:Arc<SignalTrigger>,
        registry:&PoolRegistry,cfg:&Settings,number:usize,reads:&mut RegistryReads,preferred:Option<&[usize]>)->Result<Selection>{
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
        let mut amount_seed_changed=false;
        if let Some(preferred)=preferred.filter(|s|s.len()==slots.len()) {
            let mut seen=HashSet::new();let mut total=0_i128;let mut valid=true;
            for (i,&slot) in preferred.iter().enumerate() {
                let Some(identity)=self.pools.get(slot/2)else{valid=false;break;};
                let leg=&witness.legs[i];
                let pair=if slot%2==0{(identity.mint_a,identity.mint_b)}else{(identity.mint_b,identity.mint_a)};
                let Some(w)=self.weight(slot)else{valid=false;break;};
                valid&=pair==(leg.input_mint,leg.output_mint)&&seen.insert(slot/2);total+=i128::from(w);
            }
            if valid&&total<i128::from(threshold) {
                for (entry,&slot) in slots.iter_mut().zip(preferred){amount_seed_changed|=entry.0!=slot;entry.0=slot;}
                used=seen;
            }
        }
        let seed_sum=slots.iter().try_fold(0_i128,|sum,(slot,_)|Ok::<_,anyhow::Error>(sum+i128::from(self.weight(*slot).context("seed net weight unavailable")?)))?;
        let seed_sum=i64::try_from(seed_sum).context("seed net sum overflow")?;
        ensure!(seed_sum<threshold,"quantity seed is not net qualified");
        let mut request=ExecutionRequest{id:format!("graph-{}-{}-{number}",self.generation,self.revision),
            wallet:cfg.wallet,settlement:top.settlement,groups:Vec::new(),conversion:None,
            parameters:cfg.parameters.clone()};
        let mut pools=Vec::new();
        for &(slot,r) in &slots{
            pools.push(self.pool(slot,r,registry,cfg.wallet,reads)?);
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
                    let new_sum=i128::from(seed_sum)-i128::from(original)+i128::from(w);
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
            let pool=match self.pool(slot,role_name,registry,cfg.wallet,reads){
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
        let estimated_net_return_bps=(-(seed_sum as f64)/SIGNAL_LOG_SCALE as f64).exp_m1()*10000.0;
        Ok(Selection{request,graph_generation:self.generation,revision:self.revision,
            topology_id:Some(c.topology_id),route_kind:top.kind,reverse:c.reverse,
            trigger,candidate:Some(c.clone()),threshold_units:threshold,
            estimated_net_return_bps:estimated_net_return_bps.is_finite().then_some(estimated_net_return_bps),
            pools:pools.into_iter().map(|(_,e)|e).collect(),packing,
            signal_ready_at_us:self.ready_at_us,candidate_received_at_us:self.received_at_us,
            request_ready_at_us:ready,price_pipeline:self.price_pipeline,
            frame_to_request_us:frame.and_then(|at|ready.checked_sub(at)),
            signal_to_request_us:ready.saturating_sub(self.ready_at_us),selection_ns:elapsed_ns(started),
            registry_reads:reads.clone(),
            alternatives_considered:considered,packing_rejections:rejected,
            unavailable_alternatives:unavailable,last_alternative_rejection:last_rejection,
            alternative_pools_added,packing_seed_log_sum_units:seed_sum,
            amount_assessment:amounts::Assessment::unknown("not_evaluated"),amount_seed_changed})
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
    let mut report=Report{score_basis:SIGNAL_SCORE_BASIS,..Default::default()}; let mut state=State::default(); let mut sequence=0;
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
                    report.state_updates+=1;
                    ensure!(!pending,"candidate state replaced before evaluation completed");
                    state.apply(s,report.awaiting_snapshot)?;
                    if matches!(s.trigger.as_ref(),SignalTrigger::Catalog{..}) {
                        if let Some(snapshot)=&s.snapshot {
                            report.catalog_snapshots+=1;
                            report.catalog_snapshot_candidates+=snapshot.candidates.len() as u64;
                        }
                    }
                    if report.awaiting_snapshot {report.resynchronizations+=1;report.awaiting_snapshot=false;}
                    pending=true;ranked.clear();
                },
                SignalEvent::Candidates(b)=>{
                    report.candidate_batches+=1;report.candidates+=b.candidates.len() as u64;
                    ensure!(pending&&b.revision==state.revision&&b.graph_generation==state.generation,
                        "candidate batch references wrong state");
                    for candidate in &b.candidates {state.candidate(b,candidate)?;}
                    if (cfg.limit>0 && report.requests.len()>=cfg.limit)
                        ||!matches!(b.trigger.as_ref(),SignalTrigger::Price{origin:PriceOrigin::Websocket|PriceOrigin::Revaluation,..})
                        ||b.price_pipeline.is_some_and(|t|t.initialization_replay){continue;}
                    for (index,candidate) in b.candidates.iter().enumerate(){
                        if candidate.change==SignalRangeChange::Exited{continue;}
                        let top=state.plan.as_ref()
                            .and_then(|p|p.compact_topologies.get(candidate.topology_id))
                            .context("candidate references unknown topology")?;
                        if cfg.kind.is_some_and(|kind|kind!=top.kind){continue;}
                        if let Some(w)=candidate.witness.as_ref(){
                            ensure!(w.log_sum_units<b.threshold_units,
                                "best candidate does not satisfy net threshold");
                            report.qualifying_ranges+=1;
                            ranked.push((w.log_sum_units,event.clone(),index));
                        }
                    }
                },
                SignalEvent::Evaluated(e)=>{
                    report.evaluations+=1;
                    ensure!(pending && e.revision==state.revision && e.graph_generation==state.generation,"candidate completion references wrong state");
                    ensure!(e.logarithms_prepared==state.prepared_logarithms,"candidate directed net logarithm accounting mismatch");
                    pending=false;
                    if let Some(c)=&e.catalog {
                        ensure!(matches!(e.trigger.as_ref(),SignalTrigger::Catalog{..})
                            && ranked.is_empty() && state.active.len()==c.active_candidates,
                            "execution catalog snapshot mismatch or attempted catalog execution");
                    }
                    report.max_active_candidates=report.max_active_candidates.max(state.active.len());
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
                    let mut reads=RegistryReads::default();
                    ranked.sort_unstable_by_key(|entry|entry.0);
                    let mut chosen=None;
                    let mut invalid_seed=false;
                    for (_,event,index) in &ranked {
                        let selected=match event.as_ref(){
                            SignalEvent::Candidates(block)=>state.build_compact(&block.candidates[*index],
                                block.threshold_units,block.trigger.clone(),&registry,&cfg,
                                report.requests.len()+1,&mut reads,None),
                            _=>bail!("ranked event is not a compact candidate"),
                        };
                        match selected {
                            Ok(selection)=>{
                                if let Some(mut selection)=state.assess_selection(selection,&registry,&cfg,&mut reads,&mut report.amounts)? {
                                    selection.selection_ns=elapsed_ns(started);selection.registry_reads=reads.clone();
                                    selection.request_ready_at_us=now_us();
                                    selection.frame_to_request_us=selection.price_pipeline.and_then(|t|selection.request_ready_at_us.checked_sub(t.frame_received_at_us));
                                    selection.signal_to_request_us=selection.request_ready_at_us.saturating_sub(selection.signal_ready_at_us);
                                    chosen=Some(selection);break;
                                }
                            },
                            Err(error)=>{invalid_seed=true;report.skipped_invalid_seed+=1;report.last_rejection=Some(format!("{error:#}"));},
                        }
                    }
                    ranked.clear();
                    report.registry_reads.merge(&reads);
                    if let Some(selection)=chosen {
                        crate::log_event!(Info,"execution candidates request_id={} generation={} revision={} kind={:?} reverse={} pools={} accounts={} selection_ns={} frame_to_request_us={:?} registry_reads={} registry_wait_ns={} registry_hold_ns={} broadcasts=false",
                            selection.request.id,state.generation,state.revision,selection.route_kind,selection.reverse,
                            selection.packing.pool_declarations,selection.packing.unique_accounts,selection.selection_ns,selection.frame_to_request_us,
                            selection.registry_reads.calls,selection.registry_reads.wait_ns,selection.registry_reads.hold_ns);
                        permit.send(selection.request.clone());report.requests.push(selection);
                    } else if invalid_seed {
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
