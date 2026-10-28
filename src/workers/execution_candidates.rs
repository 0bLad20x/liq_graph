//! Versioned compact best-candidates + current Prepared proofs -> frozen V10 jobs.
//! Selection is downstream of calculation; no RPC, signer, decoding or sending.
mod amounts;
mod comparison;
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
    economics:EconomicPolicy,
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
        let economics=EconomicPolicy::from_env()?;
        Ok(Self {wallet,parameters,economics,limit,kind,capacity})
    }
}
/// Internal assumptions, never copied into ExecutionParameters or CPI data.
#[derive(Clone,serde::Serialize)]
struct EconomicPolicy {network_fee_lamports:u64,additional_cost_reserve_lamports:u64,minimum_net_profit_lamports:u64}
impl EconomicPolicy {
    fn from_env()->Result<Self>{
        let number=|name:&str,default:&str|->Result<u64>{env::var(name).unwrap_or_else(|_|default.into()).parse().with_context(||format!("invalid {name}"))};
        Ok(Self{network_fee_lamports:number("GRAPH_ECONOMIC_NETWORK_FEE_LAMPORTS","5000")?,
            additional_cost_reserve_lamports:number("GRAPH_ECONOMIC_COST_RESERVE_LAMPORTS","0")?,
            minimum_net_profit_lamports:number("GRAPH_ECONOMIC_MIN_NET_PROFIT_LAMPORTS","0")?})
    }
}
#[derive(Clone,serde::Serialize)]
struct CostContext {
    policy:EconomicPolicy,priority_fee_lamports:u64,cost_guard_raw:u64,minimum_net_profit_raw:u64,
    settlement:String,reference_pool:Option<String>,reference_observed_at_us:Option<u64>,
    scope:&'static str,
}
struct CachedAssessment {fingerprint:[u8;32],assessment:amounts::Assessment,sent:bool}
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
    amount_assessment:amounts::Assessment,economic_primary_changed:bool,
    cost_assumptions:CostContext,alternative_estimates:Vec<amounts::Estimate>,
}
#[derive(Default, serde::Serialize)]
pub struct Report {
    pub error: Option<String>, #[serde(skip_serializing)] pub requests: Vec<Selection>,
    request_count:usize,pub economic_qualified_requests:u64,
    pub evaluated_revisions:u64,
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
        ensure!(self.amounts.attempts==self.amounts.possible+self.amounts.unknown+self.amounts.excluded,
            "economic decision accounting mismatch");
        ensure!(self.request_count==self.requests.len() && self.economic_qualified_requests==self.requests.len() as u64,
            "economic request accounting mismatch");
        for r in &self.requests {
            let a=&r.amount_assessment;
            ensure!(a.decision==amounts::Decision::Possible && a.estimate.as_ref().is_some_and(|e|e.profit_raw>a.required_profit_raw),
                "unqualified economic request dispatched");
            ensure!(a.estimate.as_ref().is_some_and(|e|e.legs.iter().all(|l|r.pools.iter().any(|p|p.pool==l.pool))),
                "economic primary was not packed");
        }
        Ok(())
    }
    pub fn ensure_handoff(&self, reports:&[ExecutionReport])->Result<()> {
        self.ensure_complete()?;
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
    pub fn compare_simulations(&self,reports:&[ExecutionReport])->Vec<serde_json::Value>{
        let by_id=reports.iter().map(|r|(r.id.as_str(),r)).collect::<HashMap<_,_>>();
        self.requests.iter().filter_map(|s|by_id.get(s.request.id.as_str()).map(|r|comparison::compare(s,r))).collect()
    }
    pub fn request_count(&self)->usize{self.requests.len()}
    pub fn economic_samples(&self)->&[serde_json::Value]{&self.amounts.samples}
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
    pair_index:HashMap<(Pubkey,Pubkey),Vec<usize>>,economic_cache:HashMap<Vec<(String,String)>,CachedAssessment>,
    cost_contexts:HashMap<Pubkey,(u64,CostContext)>,
}
impl State {
    fn amount_input(&self,id:usize,manifest_matches:bool)->Option<amounts::Input> {
        let price=self.prices.get(id)?.as_ref()?;let identity=self.pools.get(id)?;
        Some(amounts::Input{slot:2*id+usize::from(identity.mint_a.to_string()!=price.observation.base_mint),
            observation:price.observation.clone(),manifest_matches})
    }
    fn economic_inputs(&self,top:&SignalTopology)->Vec<amounts::Input> {
        let mut ids=top.roles[..top.length].iter().flat_map(|&(a,b)|self.pair_index.get(&(a.min(b),a.max(b))).into_iter().flatten().copied()).collect::<Vec<_>>();
        ids.sort_unstable();ids.dedup();ids.into_iter().filter_map(|id|self.amount_input(id,true)).collect()
    }
    fn cost_context(&mut self,settlement:Pubkey,cfg:&Settings)->Result<CostContext>{
        if let Some((revision,cost))=self.cost_contexts.get(&settlement).filter(|(r,_)|*r==self.revision){let _=revision;return Ok(cost.clone());}
        let total=cfg.economics.network_fee_lamports.checked_add(cfg.parameters.priority_fee_lamports)
            .and_then(|n|n.checked_add(cfg.economics.additional_cost_reserve_lamports)).context("economic costs overflow")?;
        ensure!(total>0,"internal economic cost guard must be positive");
        let mut out=CostContext{policy:cfg.economics.clone(),priority_fee_lamports:cfg.parameters.priority_fee_lamports,
            cost_guard_raw:total,minimum_net_profit_raw:cfg.economics.minimum_net_profit_lamports,
            settlement:settlement.to_string(),reference_pool:None,reference_observed_at_us:None,
            scope:"DEX_net_minus_configured_cost_guard_not_verified_contract_costs"};
        if settlement.to_string()=="So11111111111111111111111111111111111111112"{self.cost_contexts.insert(settlement,(self.revision,out.clone()));return Ok(out);}
        // Value lamport costs through the deepest complete observed SOL/settlement
        // reference, never an arbitrary first pool or a hard-coded dollar peg.
        let mut reference=None;
        for id in 0..self.pools.len(){if let Some(i)=self.amount_input(id,true){
            if let Some(p)=&i.observation.liquidity {
                for direction in 0..2 {let d=&p.directions[direction];
                    if d.input_mint!="So11111111111111111111111111111111111111112"||d.output_mint!=out.settlement{continue;}
                    let capacity=amounts::output_bound(&i,1-direction).ok();
                    if let Some(capacity)=capacity.filter(|n|*n>0){if let Ok(rate)=amounts::upper_rate(&i,direction){
                        if reference.as_ref().is_none_or(|(old,_,_):&(u64,amounts::Rate,amounts::Input)|capacity>*old){reference=Some((capacity,rate,i.clone()));}
                    }}
                }
            }
        }}
        let (_,rate,input)=reference.context("settlement cost reference unavailable")?;
        out.cost_guard_raw=u64::try_from(amounts::mul_div(total,rate.0,rate.1,true).map_err(|_|anyhow::anyhow!("reference cost overflow"))?)?;
        out.minimum_net_profit_raw=u64::try_from(amounts::mul_div(cfg.economics.minimum_net_profit_lamports,rate.0,rate.1,true).map_err(|_|anyhow::anyhow!("reference profit overflow"))?)?;
        out.reference_pool=Some(input.observation.pool.clone());out.reference_observed_at_us=Some(input.observation.received_at_us);
        ensure!(out.cost_guard_raw>0,"zero settlement cost reference");
        self.cost_contexts.insert(settlement,(self.revision,out.clone()));Ok(out)
    }
    fn qualify(&mut self,c:&SignalCandidate,cfg:&Settings,report:&mut amounts::Report)
        ->Result<Option<(amounts::Assessment,CostContext,Vec<amounts::Input>)>>{
        let top=&self.plan.as_ref().context("economic plan absent")?.compact_topologies[c.topology_id];
        let inputs=self.economic_inputs(top);let settlement=top.settlement;
        let cost=match self.cost_context(settlement,cfg){Ok(v)=>v,Err(_)=>{
            let a=amounts::Assessment::unknown("settlement_cost_reference_unavailable");report.record(&a);return Ok(None);
        }};
        let pairs=c.witness.as_ref().context("economic source witness absent")?.legs.iter()
            .map(|l|(l.input_mint.to_string(),l.output_mint.to_string())).collect::<Vec<_>>();
        let mut hash=ring::digest::Context::new(&ring::digest::SHA256);
        hash.update(&cost.cost_guard_raw.to_le_bytes());hash.update(&cost.minimum_net_profit_raw.to_le_bytes());
        for i in &inputs {
            hash.update(&(i.slot as u64).to_le_bytes());
            if let Some(p)=&i.observation.liquidity{hash.update(&p.content_digest);hash.update(&p.account_set_digest);}
            if let Some(v)=&i.observation.valuation{for t in &v.token_fee_status{hash.update(t.as_bytes());}}
        }
        let fingerprint:[u8;32]=hash.finish().as_ref().try_into().expect("SHA256 length");
        let key=pairs.clone();
        if let Some(cached)=self.economic_cache.get(&key).filter(|e|e.fingerprint==fingerprint){
            report.cache_hits+=1;report.observations_reused+=inputs.len() as u64;
            if cached.sent{report.unchanged_states+=1;return Ok(None);}
            return Ok((cached.assessment.decision==amounts::Decision::Possible).then(||(cached.assessment.clone(),cost,inputs)));
        }
        let assessment=if cfg.parameters.trade_size!=0||cfg.parameters.constant_conversion||cfg.parameters.no_failure||cfg.parameters.additional_fee_bp!=0 {
            amounts::Assessment::unknown("nondefault_execution_mode_unmodelled")
        }else{amounts::assess(&inputs,&pairs,cost.cost_guard_raw,cost.minimum_net_profit_raw)};
        report.record(&assessment);
        if report.wants_sample(assessment.decision){report.sample(&assessment,&inputs,serde_json::json!({
            "generation":self.generation,"revision":self.revision,"topology_id":c.topology_id,"reverse":c.reverse,
            "source_witness":c.witness,"cost_assumptions":cost}));}
        self.economic_cache.insert(key,CachedAssessment{fingerprint,assessment:assessment.clone(),sent:false});
        Ok((assessment.decision==amounts::Decision::Possible).then_some((assessment,cost,inputs)))
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
            self.pair_index.clear();
            let alive=snapshot.plan.compact_topologies.iter().flat_map(|t|[false,true].into_iter().map(move|reverse|{
                (0..t.length).map(|i|{let (a,b)=t.roles[if reverse{t.length-1-i}else{i}];
                    if reverse{(b.to_string(),a.to_string())}else{(a.to_string(),b.to_string())}}).collect::<Vec<_>>()
            })).collect::<HashSet<_>>();
            self.economic_cache.retain(|key,_|alive.contains(key));
            for (id,p) in self.pools.iter().enumerate(){self.pair_index.entry((p.mint_a,p.mint_b)).or_default().push(id);}
            self.generation=s.graph_generation;
            self.threshold=snapshot.threshold_units;
            self.active.clear();
            for candidate in &snapshot.candidates {
                self.witness(candidate.topology_id,candidate.reverse,&candidate.witness)?;
                ensure!(self.active.insert((candidate.topology_id,candidate.reverse)),
                    "duplicate execution candidate snapshot key");
            }
        } else {ensure!(self.plan.is_some() && s.graph_generation==self.generation, "candidate delta without snapshot");}
        if s.clear {self.prices.fill(None);self.active.clear();self.economic_cache.clear();}
        for change in &s.changes {
            if change.pool_id==self.pools.len() {self.pair_index.entry((change.identity.mint_a,change.identity.mint_b)).or_default().push(change.pool_id);self.pools.push(change.identity.clone()); self.prices.push(None);}
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
            SignalRangeChange::Exited=>{ensure!(c.witness.is_none()&&self.active.remove(&key),
                "execution candidate exit missing or carries witness");},
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
        registry:&PoolRegistry,cfg:&Settings,number:usize,reads:&mut RegistryReads,assessment:amounts::Assessment,cost:CostContext,inputs:&[amounts::Input])->Result<Selection>{
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
        let preferred=assessment.estimate.as_ref().context("economic primary absent")?.legs.iter().map(|l|l.slot).collect::<Vec<_>>();
        let mut economic_primary_changed=false;
        {
            let mut seen=HashSet::new();let mut total=0_i128;let mut valid=true;
            for (i,&slot) in preferred.iter().enumerate() {
                let Some(identity)=self.pools.get(slot/2)else{valid=false;break;};
                let leg=&witness.legs[i];
                let pair=if slot%2==0{(identity.mint_a,identity.mint_b)}else{(identity.mint_b,identity.mint_a)};
                let Some(w)=self.weight(slot)else{valid=false;break;};
                valid&=pair==(leg.input_mint,leg.output_mint)&&seen.insert(slot/2);total+=i128::from(w);
            }
            ensure!(valid && total<i128::from(threshold),"economic primary does not satisfy directed net contract");
            for (entry,&slot) in slots.iter_mut().zip(&preferred){economic_primary_changed|=entry.0!=slot;entry.0=slot;}
            used=seen;
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
        let primary=assessment.estimate.as_ref().context("primary estimate absent")?;
        let pairs=primary.legs.iter().map(|l|(l.input_mint.clone(),l.output_mint.clone())).collect::<Vec<_>>();
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
                        let mut alternative=preferred.clone();alternative[i]=slot;
                        if let Some(e)=amounts::evaluate_slots(inputs,&pairs,&alternative,primary.input_raw) {
                            if e.profit_raw>assessment.required_profit_raw {options.push((std::cmp::Reverse(e.profit_raw),slot,role_name,e));}
                        }
                    }
                }
            }
        }
        options.sort_unstable_by_key(|(score,slot,_,_)|(*score,*slot));
        let mut alternative_estimates=Vec::new();
        let mut considered=0;let mut rejected=0;let mut unavailable=0;
        let mut last_rejection=None;
        for (_,slot,role_name,estimate) in options{
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
                Ok(metrics)=>{packing=metrics;used.insert(slot/2);alternative_estimates.push(estimate);},
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
            amount_assessment:assessment,economic_primary_changed,cost_assumptions:cost,alternative_estimates})
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
                    report.evaluated_revisions+=1;
                    let mut qualified=Vec::new();
                    for (_,event,index) in &ranked {
                        let SignalEvent::Candidates(block)=event.as_ref() else{bail!("ranked event is not a candidate")};
                        if let Some((assessment,cost,inputs))=state.qualify(&block.candidates[*index],&cfg,&mut report.amounts)? {
                            qualified.push((event.clone(),*index,assessment,cost,inputs));
                        }
                    }
                    // Compare absolute gains on a common cost-normalized currency
                    // basis, not marginal BPS. The same configured lamport guard
                    // is converted for USDC settlements using the recorded reference.
                    qualified.sort_unstable_by(|a,b|{
                        let ag=a.2.estimate.as_ref().unwrap().profit_raw-a.2.cost_guard_raw;
                        let bg=b.2.estimate.as_ref().unwrap().profit_raw-b.2.cost_guard_raw;
                        (u128::from(bg)*u128::from(a.2.cost_guard_raw)).cmp(&(u128::from(ag)*u128::from(b.2.cost_guard_raw)))
                    });
                    let mut chosen=None;let mut invalid_seed=false;
                    for (event,index,assessment,cost,inputs) in qualified {
                        let SignalEvent::Candidates(block)=event.as_ref() else{bail!("qualified event is not a candidate")};
                        match state.build_compact(&block.candidates[index],block.threshold_units,block.trigger.clone(),
                            &registry,&cfg,report.requests.len()+1,&mut reads,assessment,cost,&inputs) {
                            Ok(mut selection)=>{
                                selection.selection_ns=elapsed_ns(started);selection.registry_reads=reads.clone();
                                selection.request_ready_at_us=now_us();
                                selection.frame_to_request_us=selection.price_pipeline.and_then(|t|selection.request_ready_at_us.checked_sub(t.frame_received_at_us));
                                selection.signal_to_request_us=selection.request_ready_at_us.saturating_sub(selection.signal_ready_at_us);
                                chosen=Some(selection);break;
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
                        let key=selection.amount_assessment.estimate.as_ref().unwrap().legs.iter()
                            .map(|l|(l.input_mint.clone(),l.output_mint.clone())).collect::<Vec<_>>();
                        if let Some(entry)=state.economic_cache.get_mut(&key){entry.sent=true;}
                        permit.send(selection.request.clone());report.requests.push(selection);
                        report.request_count=report.requests.len();report.economic_qualified_requests+=1;
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
