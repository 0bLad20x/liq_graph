//! Versioned compact best-candidates + current Prepared proofs -> frozen V10 jobs.
//! Selection is downstream of calculation; no RPC, signer, decoding or sending.
mod amounts;
mod diversity;
use diversity::{Strategy as PackingStrategy,Alternative};
use std::{collections::{BTreeMap,HashMap,HashSet}, env, sync::Arc, time::{Instant, SystemTime, UNIX_EPOCH}};
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
    pub routes:crate::core::EnabledRoutes,
    /// CP9C: independent alternative-packing strategies for the same signal revision.
    strategies:Vec<PackingStrategy>,
    pub cp9d_max_pairs:usize,
    /// Public typed packing boundary, supplied by orchestration. Never a signer.
    pub capacity: fn(&ExecutionRequest) -> Result<ExecutionMetrics>,
}
impl Settings {
    pub fn from_env(wallet: Pubkey, capacity: fn(&ExecutionRequest)->Result<ExecutionMetrics>) -> Result<Self> {
        let limit=env::var("GRAPH_EXECUTION_MAX_REQUESTS").unwrap_or_else(|_|"0".into()).parse()?;
        let routes=crate::core::EnabledRoutes::from_env()?;
        let parameters:ExecutionParameters=match env::var("GRAPH_EXECUTION_PARAMETERS_PATH") {
            Ok(path) if !path.is_empty()=>serde_json::from_slice(&std::fs::read(path)
                .context("read execution parameters")?).context("decode ExecutionParameters")?,
            _=>ExecutionParameters::default(),
        };
        ensure!(parameters.use_flashloan,"V10 simulation requires use_flashloan=true; update the execution parameters file");
        let mode=env::var("GRAPH_CP9C_PACKING_MODE").unwrap_or_else(|_|"baseline".into());
        let strategies=match mode.to_ascii_lowercase().as_str() {
            "baseline"=>vec![PackingStrategy::Baseline],
            "contrarian"=>vec![PackingStrategy::Contrarian],
            "mixed"=>vec![PackingStrategy::Mixed],
            "all"=>vec![PackingStrategy::Baseline,PackingStrategy::Contrarian,PackingStrategy::Mixed],
            "all_coverage"=>vec![PackingStrategy::Baseline,PackingStrategy::Contrarian,PackingStrategy::Mixed,PackingStrategy::Coverage],
            "coverage"=>vec![PackingStrategy::Coverage],
            _=>bail!("GRAPH_CP9C_PACKING_MODE must be baseline, contrarian, mixed, all, coverage or all_coverage"),
        };
        let cp9d_max_pairs:usize=env::var("GRAPH_CP9D_MAX_DISTINCT_PAIRS")
            .unwrap_or_else(|_|"1".into()).parse().context("invalid GRAPH_CP9D_MAX_DISTINCT_PAIRS")?;
        ensure!((1..=8).contains(&cp9d_max_pairs),"GRAPH_CP9D_MAX_DISTINCT_PAIRS must be 1..=8");
        ensure!((strategies==[PackingStrategy::Baseline]&&cp9d_max_pairs==1)
            ||(!routes.contains(SignalRouteKind::Triangle3)&&!routes.contains(SignalRouteKind::Triangle4)),
            "alternative packing comparisons require enabled routes direct2 and/or direct3");
        Ok(Self {wallet,parameters,limit,routes,strategies,cp9d_max_pairs,capacity})
    }
    fn accepts(&self,kind:SignalRouteKind)->bool {
        self.routes.contains(kind)
    }
}
fn now_us()->u64 {SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default()
    .as_micros().min(u128::from(u64::MAX)) as u64}
fn elapsed_ns(start:Instant)->u64 {start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64}

// Fixed-memory phase timings, serialized only in the final worker report.
#[derive(Default)]
struct Timing {count:u64,total_ns:u64,max_ns:u64,bins:Vec<u64>}
impl Timing {
    fn record(&mut self,ns:u64) {
        if self.bins.is_empty(){self.bins.resize(64,0);}
        self.count+=1;self.total_ns=self.total_ns.saturating_add(ns);self.max_ns=self.max_ns.max(ns);
        self.bins[if ns==0{0}else{63-ns.leading_zeros() as usize}]+=1;
    }
    fn percentile(&self,p:u64)->Option<u64> {
        if self.count==0{return None;}
        let rank=(self.count*p).div_ceil(100);let mut seen=0;
        for (i,&n) in self.bins.iter().enumerate() {
            seen+=n;
            if seen>=rank {
                let upper=if i==63{u64::MAX}else{(1_u64<<(i+1))-1};
                return Some(upper.min(self.max_ns));
            }
        }
        None
    }
}
impl serde::Serialize for Timing {
    fn serialize<S:serde::Serializer>(&self,s:S)->std::result::Result<S::Ok,S::Error> {
        serde::Serialize::serialize(&serde_json::json!({"count":self.count,"total_ns":self.total_ns,
            "mean_ns":(self.count>0).then(||self.total_ns as f64/self.count as f64),
            "max_ns":(self.count>0).then_some(self.max_ns),"p50_upper_ns":self.percentile(50),
            "p95_upper_ns":self.percentile(95),"p99_upper_ns":self.percentile(99)}),s)
    }
}
#[derive(Default,serde::Serialize)]
struct Preparation {
    pair_index_rebuilds:u64,pair_index_appends:u64,indexed_pools:usize,indexed_pairs:usize,
    alternative_scans:u64,indexed_pool_visits:u64,full_scan_pool_visits_equivalent:u64,alternative_options:u64,
    capacity_checks:u64,capacity_rejections:u64,coverage_rankings:u64,coverage_key_evaluations:u64,
    pair_index_ns:Timing,alternative_lookup_ns:Timing,ranking_ns:Timing,pool_resolution_ns:Timing,
    capacity_validation_ns:Timing,pack_build_ns:Timing,signal_to_round_start_ns:Timing,round_work_ns:Timing,
}

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
    /// CP9A: eligible/dust/unknown at the original directed signal slot.
    direction_liquidity_status:&'static str,
}
#[derive(serde::Serialize)]
pub struct Selection {
    pub request: ExecutionRequest,
    packing_strategy:&'static str,
    packing_revision_variant:u8,
    cp9d_source:&'static str,
    cp9d_pair_index:u8,
    graph_generation: u64, revision: u64, topology_id: Option<usize>, route_kind: SignalRouteKind,
    #[serde(with="crate::core::execution::pubkey_json")]
    target:Pubkey,
    reverse: bool, trigger: Arc<SignalTrigger>, candidate: Option<SignalCandidate>,
    threshold_units: i64, estimated_net_return_bps:Option<f64>,
    pools: Vec<PoolEvidence>, packing: ExecutionMetrics,
    signal_ready_at_us: u64, candidate_received_at_us: u64, request_ready_at_us: u64,
    price_pipeline: Option<PricePipelineTiming>,
    frame_to_request_us: Option<u64>, signal_to_request_us: u64,
    round_elapsed_ns: u64, alternatives_considered: u64, packing_rejections: u64,
    registry_reads:RegistryReads,
    unavailable_alternatives: u64, last_alternative_rejection: Option<String>,
    alternative_pools_added: usize,
    /// May differ from the source SignalCandidate after quantity seed repair.
    packing_seed_log_sum_units:i64,
    amount_assessment:amounts::Assessment,amount_seed_changed:bool,
}
#[derive(Default, serde::Serialize)]
pub struct Report {
    pub error: Option<String>,
    #[serde(skip_serializing)]
    pub request_count: u64,
    score_basis:&'static str,
    pub events:u64,pub state_updates:u64,pub candidate_batches:u64,pub candidates:u64,pub evaluations:u64,
    websocket_revisions: u64, initialization_revisions: u64, qualifying_ranges: u64,
    lost_events: u64, resynchronizations: u64, skipped_busy_revisions: u64,
    pub cp9c_pack_attempts:u64,pub cp9c_packs_dispatched:u64,pub cp9c_duplicate_packs:u64,
    pub cp9c_queue_full_packs:u64,pub cp9c_by_strategy:std::collections::BTreeMap<String,u64>,
    pub cp9d_fresh_pairs:u64,pub cp9d_active_pairs:u64,pub cp9d_active_candidates_examined:u64,
    pub cp9d_distinct_pairs_dispatched:u64,
    pub cp9d_queue_full_by_strategy:BTreeMap<String,u64>,
    pub cp9e_direct3_reserved_from_fresh:u64,pub cp9e_direct3_reserved_from_active:u64,
    skipped_invalid_seed: u64, last_rejection: Option<String>, awaiting_snapshot: bool,
    manifest_refresh_performed: bool,
    catalog_snapshots:u64, catalog_snapshot_candidates:u64, max_active_candidates:usize,
    registry_reads:RegistryReads,
    amounts:amounts::Report,
    preparation:Preparation,
}
pub enum EvidenceItem { Selection(Box<Selection>), Report(Box<ExecutionReport>) }
impl Selection {
    pub fn packing_metrics(&self)->&ExecutionMetrics {&self.packing}
    /// One frozen-model quote at a supplied amount; no search, live state or RPC.
    fn quote_frozen_leg(&self,pool:&str,input_mint:&str,output_mint:&str,amount:u64)->Result<u64,&'static str> {
        let p=self.pools.iter().find(|p|p.pool==pool).ok_or("pool_not_in_frozen_selection")?;
        let profile=p.observation.liquidity.as_ref().ok_or("amount_model_unavailable")?;
        let direction=profile.directions.iter().position(|d|d.input_mint==input_mint
            &&d.output_mint==output_mint).ok_or("direction_not_in_model")?;
        let input=amounts::Input{slot:p.slot,observation:p.observation.clone(),manifest_matches:p.liquidity_window_checked};
        amounts::quote(&input,direction,amount).map_err(|e|match e{amounts::Issue::Unknown(r)|amounts::Issue::Insufficient(r)=>r})
    }
    fn quote_observed_leg(&self,call:&crate::core::execution::ExecutionSwapCall,amount:u64)->Result<u64,&'static str> {
        if call.status!="complete"{return Err("observed_flow_incomplete");}
        self.quote_frozen_leg(call.pool.as_deref().ok_or("pool_identity_unresolved")?,
            &call.inputs[0].mint,&call.outputs[0].mint,amount)
    }
    /// Evidence-thread comparison only. Attempted flows on failure are never realized gains.
    pub fn calibration(&self,report:&ExecutionReport)->serde_json::Value {
        use serde_json::json;
        let started=Instant::now();
        let witness=self.candidate.as_ref().and_then(|c|c.witness.as_ref());
        let original=witness.map(|w|w.legs.iter().map(|l|l.observation.pool.as_str()).collect::<Vec<_>>()).unwrap_or_default();
        let seed=self.pools.iter().take(original.len()).map(|p|p.pool.as_str()).collect::<Vec<_>>();
        let directions=witness.map(|w|w.legs.iter().map(|l|(l.input_mint.to_string(),l.output_mint.to_string())).collect::<Vec<_>>()).unwrap_or_default();
        let calls=&report.trace.calls;
        let observed=calls.iter().map(|c|c.pool.as_deref()).collect::<Option<Vec<_>>>();
        let relation=|path:&[&str]|match &observed {
            _ if calls.is_empty()=>"no_observed_dex",
            None=>"pool_identity_unresolved",
            Some(actual) if actual==path || (actual.len()<path.len()&&path.starts_with(actual))=>{
                if calls.iter().enumerate().any(|(i,c)|c.status=="complete"&&directions.get(i)
                    .is_none_or(|d|d.0!=c.inputs[0].mint||d.1!=c.outputs[0].mint)){"different_direction"}
                else if actual==path && calls.iter().all(|c|c.status=="complete"){"same_path"}
                else if actual==path{"same_pools_direction_unresolved"}else{"attempted_prefix"}
            },
            _=>"different_path",
        };
        let committed=matches!(report.outcome,ExecutionOutcome::SimulatedNoDexCallObserved|ExecutionOutcome::SimulatedDexCallsObserved);
        let mut legs=Vec::new();let mut marginal=Some(1.0_f64);
        for (call_index,call) in calls.iter().enumerate() {
            let complete=call.status=="complete";
            let reused=calls[..call_index].iter().any(|c|c.pool.is_some()&&c.pool==call.pool);
            let quoted=if reused{Err("pool_reused_after_prior_swap")}
                else if complete{self.quote_observed_leg(call,call.inputs[0].gross_amount_raw)}else{Err("observed_flow_incomplete")};
            let p=self.pools.iter().find(|p|Some(p.pool.as_str())==call.pool.as_deref());
            let rate=p.and_then(|p|p.observation.liquidity.as_ref()).and_then(|q|q.directions.iter()
                .find(|d|complete&&d.input_mint==call.inputs[0].mint&&d.output_mint==call.outputs[0].mint))
                .and_then(|d|d.marginal_net_rate);
            marginal=marginal.zip(rate).map(|(a,b)|a*b).filter(|r|r.is_finite());
            let actual=complete.then(||call.outputs[0].amount_raw);
            let error=quoted.as_ref().ok().zip(actual).map(|(&model,actual)|i128::from(actual)-i128::from(model));
            legs.push(json!({"call_index":call_index,"pool":call.pool,"program":call.program,"venue":p.map(|p|p.observation.venue),
                "flow_status":call.status,"observed_output_raw":actual,
                "model_input_raw":complete.then(||call.inputs[0].gross_amount_raw),
                "model_output_raw":quoted.as_ref().ok(),"model_unavailable_reason":quoted.err(),
                "observed_minus_model_raw":error.map(|e|e.to_string()),
                "output_error_bps":quoted.as_ref().ok().filter(|&&v|v>0).zip(actual)
                    .map(|(&model,actual)|(actual as f64/model as f64-1.0)*10000.0),
                "observation_slot":p.map(|p|p.observation.slot),
                "component_slot_min":p.map(|p|p.observation.component_slot_min),
                "component_slot_max":p.map(|p|p.observation.component_slot_max),
                // Same-slot does not mean an identical processed bank or a
                // simultaneously sampled multi-account observation.
                "same_slot_proven_same_bank":false,
                "observation_consistency":p.map(|p|p.observation.consistency),
                "component_received_at_us_min":p.map(|p|p.observation.component_received_at_us_min),
                "component_received_at_us_max":p.map(|p|p.observation.component_received_at_us_max),
                "snapshot_content_digest":p.and_then(|p|p.observation.liquidity.as_ref().map(|l|l.content_digest)),
                "snapshot_account_set_digest":p.and_then(|p|p.observation.liquidity.as_ref().map(|l|l.account_set_digest)),
                "snapshot_reserve_curve":p.and_then(|p|p.observation.liquidity.as_ref().and_then(|l|l.reserve_curve.as_ref())),
                "snapshot_direction":p.and_then(|p|p.observation.liquidity.as_ref().and_then(|l|
                    l.directions.iter().find(|d|complete&&d.input_mint==call.inputs[0].mint&&d.output_mint==call.outputs[0].mint)))
                    .map(|d|json!({"status":d.status,"coverage":d.coverage,
                        "input_capacity_raw":d.input_capacity_raw,"output_capacity_raw":d.output_capacity_raw,
                        "first_source_position":d.first_source_position,"marginal_net_rate":d.marginal_net_rate,
                        "first_level":d.levels.first()})),
                "slots_to_simulation":p.and_then(|p|report.context_slot.and_then(|s|s.checked_sub(p.observation.slot))),
                "observation_age_at_request_us":p.and_then(|p|self.request_ready_at_us.checked_sub(p.observation.received_at_us))}));
        }
        let full=!calls.is_empty()&&calls.iter().all(|c|c.status=="complete");
        let continuous=full&&calls.windows(2).all(|w|w[0].outputs[0].mint==w[1].inputs[0].mint
            &&w[0].outputs[0].amount_raw==w[1].inputs[0].gross_amount_raw
            &&w[1].inputs[0].amount_raw==w[1].inputs[0].gross_amount_raw);
        let unique=observed.as_ref().is_some_and(|p|p.iter().copied().collect::<HashSet<_>>().len()==p.len());
        let settlement=self.request.settlement.to_string();
        let observed_cycle=(2..=4).contains(&calls.len())&&full&&continuous&&unique
            &&calls[0].inputs[0].mint==settlement&&calls.last().expect("cycle").outputs[0].mint==settlement;
        let refund=observed_cycle.then(||calls[0].inputs[0].gross_amount_raw-calls[0].inputs[0].amount_raw);
        let actual_output=refund.and_then(|refund|calls.last().expect("cycle").outputs[0].amount_raw.checked_add(refund));
        let cycle=observed_cycle&&actual_output.is_some();
        let initial=cycle.then(||calls[0].inputs[0].gross_amount_raw).filter(|&v|v>0);
        let model=initial.ok_or("no_complete_single_cycle").and_then(|mut amount|{
            for call in calls{amount=self.quote_observed_leg(call,amount)?;}Ok(amount)
        });
        let bps=|output:u64,input:u64|(output as f64/input as f64-1.0)*10000.0;
        let actual_bps=initial.zip(actual_output).map(|(input,output)|bps(output,input));
        let model_bps=initial.zip(model.as_ref().ok()).map(|(input,&output)|bps(output,input));
        let quote_path=|path:&[&str]|initial.ok_or("no_observed_input_amount").and_then(|mut amount|{
            if path.is_empty()||path.len()!=directions.len(){return Err("source_direction_unavailable");}
            for (pool,(input,output)) in path.iter().zip(&directions){amount=self.quote_frozen_leg(pool,input,output,amount)?;}
            Ok(amount)
        });
        let seed_model=if relation(&seed)=="same_path"{model}else{quote_path(&seed)};
        let source_model=if original==seed{seed_model}
            else if relation(&original)=="same_path"{model}else{quote_path(&original)};
        let probe=self.amount_assessment.estimate.as_ref();
        let mut result=json!({"schema_version":1,"scope":"frozen_dex_model_vs_later_simulation",
            "marginal_signal_bps":witness.and_then(|w|{
                let bps=(-(w.log_sum_units as f64)/SIGNAL_LOG_SCALE as f64).exp_m1()*10000.0;bps.is_finite().then_some(bps)}),
            "packing_marginal_bps":self.estimated_net_return_bps,
            "source_witness_pools":original,"packing_seed_pools":seed,"observed_pools":observed,
            "source_path_relation":relation(&original),"seed_path_relation":relation(&seed),
            "source_model_output_at_trade_input_raw":source_model.as_ref().ok(),
            "source_model_unavailable_reason":source_model.err(),
            "source_model_return_bps":initial.zip(source_model.as_ref().ok()).map(|(i,&o)|bps(o,i)),
            "seed_model_output_at_trade_input_raw":seed_model.as_ref().ok(),
            "seed_model_unavailable_reason":seed_model.err(),
            "seed_model_return_bps":initial.zip(seed_model.as_ref().ok()).map(|(i,&o)|bps(o,i)),
            "trace_status":report.trace.status,"simulation_committed":committed,
            "legs":legs,"flow_continuous":continuous,"single_cycle":cycle,
            "actual_path_marginal_bps":if cycle{marginal.map(|r|(r-1.0)*10000.0)}else{None},
            "trade_input_raw":initial,"trade_output_raw":actual_output,
            "settlement_input_refund_raw":refund,
            "attempted_dex_gain_raw":initial.zip(actual_output).map(|(i,o)|(i128::from(o)-i128::from(i)).to_string()),
            "attempted_dex_return_bps":actual_bps,"simulated_dex_return_bps":if committed{actual_bps}else{None},
            "model_route_status":if model.is_ok(){"quoted"}else{"unavailable"},
            "model_route_unavailable_reason":model.err(),"model_route_output_raw":model.as_ref().ok(),
            "model_route_return_bps":model_bps,"actual_minus_model_bps":actual_bps.zip(model_bps).map(|(a,m)|a-m),
            "probe_input_raw":probe.map(|p|p.input_raw),"probe_profit_raw":probe.map(|p|p.profit_raw),
            "probe_same_path":probe.filter(|_|full).map(|p|p.legs.len()==calls.len()&&p.legs.iter().zip(calls)
                .all(|(l,c)|Some(l.pool.as_str())==c.pool.as_deref()&&l.input_mint==c.inputs[0].mint&&l.output_mint==c.outputs[0].mint)),
            "probe_same_amount":probe.zip(initial).map(|(p,i)|p.input_raw==i),"processing_ns":elapsed_ns(started)});
        // Keep the legacy calibration macro small to avoid json! recursion depth.
        result["cp9c_audit_schema"]=json!(1);
        result["packing_strategy"]=json!(self.packing_strategy);
        result["packing_revision_variant"]=json!(self.packing_revision_variant);
        result["simulation_provider"]=json!(report.provider);
        result["simulation_context_slot"]=json!(report.context_slot);
        result["state_comparability"]=json!("slot_is_not_bank_identity");
        result["request_ready_at_us"]=json!(self.request_ready_at_us);
        result["cp9d_source"]=json!(self.cp9d_source);
        result["cp9d_pair_index"]=json!(self.cp9d_pair_index);
        result
    }
    /// Small result facts; full frozen inputs are retained only in bounded examples.
    pub fn compact_evidence(&self)->Result<serde_json::Value> {
        let witness=self.candidate.as_ref().and_then(|c|c.witness.as_ref())
            .context("execution evidence lacks source witness")?;
        let source=witness.legs.iter().map(|l|l.observation.pool.as_str()).collect::<Vec<_>>();
        let seed=self.pools.iter().take(source.len()).map(|p|p.pool.as_str()).collect::<Vec<_>>();
        let mut value=serde_json::json!({
            "id":self.request.id,"settlement":self.request.settlement.to_string(),
            "target":self.target.to_string(),"route_kind":self.route_kind,"reverse":self.reverse,
            "graph_generation":self.graph_generation,"revision":self.revision,
            "packing_strategy":self.packing_strategy,"packing_revision_variant":self.packing_revision_variant,
            "cp9d_source":self.cp9d_source,"cp9d_pair_index":self.cp9d_pair_index,
            "pools":self.pools.iter().map(|p|p.pool.as_str()).collect::<Vec<_>>(),
            "source_pools":source,"seed_pools":(seed!=source).then_some(seed),
            "source_log_sum_units":witness.log_sum_units,
            "packing_seed_log_sum_units":self.packing_seed_log_sum_units,"threshold_units":self.threshold_units,
            "marginal_signal_bps":witness.estimated_net_return_bps,
            "estimated_net_return_bps":self.estimated_net_return_bps,
            "amount_decision":self.amount_assessment.decision,
            "amount_estimate":self.amount_assessment.estimate.as_ref().map(|e|[e.input_raw,e.output_raw,e.profit_raw]),
            "liquidity_checked":self.pools.iter().all(|p|p.observation.venue!="meteora_dlmm"
                ||(p.liquidity_window_checked&&p.observation.liquidity.is_some())),
            "packing":[self.packing.groups,self.packing.pool_declarations,self.packing.unique_accounts,
                self.packing.account_references,self.packing.instruction_bytes,self.packing.transaction_bytes],
            "request_ready_at_us":self.request_ready_at_us
        });
        value["round_elapsed_ns"]=serde_json::json!(self.round_elapsed_ns);
        value["frame_to_request_us"]=serde_json::json!(self.frame_to_request_us);
        value["signal_to_request_us"]=serde_json::json!(self.signal_to_request_us);
        Ok(value)
    }

}
impl Report {
    pub fn take_exclusion_samples(&mut self)->Vec<serde_json::Value> {
        std::mem::take(&mut self.amounts.excluded_samples)
    }
    pub fn ensure_complete(&self)->Result<()> {
        ensure!(self.error.is_none(), "execution candidate worker failed: {}",self.error.as_deref().unwrap_or(""));
        ensure!(self.score_basis==SIGNAL_SCORE_BASIS,"execution candidate score basis differs from net contract");
        ensure!(self.lost_events==0 && !self.awaiting_snapshot,
            "execution candidate input delivery incomplete: lost={} awaiting_snapshot={}",self.lost_events,self.awaiting_snapshot);
        ensure!(self.amounts.attempts==self.amounts.possible+self.amounts.unknown+self.amounts.excluded
            && self.amounts.possible+self.amounts.unknown==self.request_count,
            "quantity decision accounting mismatch");
        Ok(())
    }
}

/// A consumer-owned view of the published contract, not another route engine.
#[derive(Default)]
struct State {
    plan: Option<Arc<SignalPlan>>, pools: Vec<SignalPool>,
    // Identities only. Price revisions never rebuild this consumer-owned index.
    pair_pools:HashMap<(Pubkey,Pubkey),Vec<usize>>,
    prices: Vec<Option<Arc<SignalPoolPrice>>>, revision: u64, generation: u64,
    ready_at_us: u64, received_at_us: u64,
    price_pipeline: Option<PricePipelineTiming>,
    threshold:i64, active:HashSet<(usize,bool)>,
    active_witnesses:BTreeMap<(usize,bool),SignalCandidate>,
    prepared_logarithms:u64,
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
        reads:&mut RegistryReads,report:&mut amounts::Report,preparation:&mut Preparation)->Result<Option<Selection>> {
        let started=Instant::now();
        let conversion_other=selection.request.conversion.as_ref().map(|p|
            if p.manifest.mint_a==selection.request.settlement{p.manifest.mint_b}else{p.manifest.mint_a}.to_string());
        let mut assessment=if cfg.parameters.trade_size!=0||cfg.parameters.constant_conversion||cfg.parameters.no_failure {
            amounts::Assessment::unknown("nondefault_execution_mode")
        }else{amounts::assess(&self.packed_amount_inputs(&selection),&selection.request.settlement.to_string(),cfg.parameters.minimum_profit,conversion_other.as_deref())};
        if assessment.decision==amounts::Decision::Excluded {
            let candidate=selection.candidate.clone().context("quantity candidate missing")?;
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
                if let Ok(mut repaired)=self.build_compact(&candidate,selection.threshold_units,selection.trigger.clone(),
                    registry,cfg,selection.request.id.rsplit('-').next().context("request number absent")?.parse()?,reads,
                    match selection.packing_strategy {
                        "contrarian"=>PackingStrategy::Contrarian,"mixed"=>PackingStrategy::Mixed,
                        "coverage"=>PackingStrategy::Coverage,_=>PackingStrategy::Baseline
                    },&[],Some(&preferred),preparation) {
                    if repaired.amount_seed_changed {
                        let next=amounts::assess(&self.packed_amount_inputs(&repaired),&repaired.request.settlement.to_string(),cfg.parameters.minimum_profit,conversion_other.as_deref());
                        if next.decision!=amounts::Decision::Excluded {
                            assessment=next;repaired.amount_seed_changed=true;report.seed_repaired+=1;selection=repaired;
                        }
                    }
                }
            }
            // Unknown alternatives do not change the proof for this packed set.
            // Only an actually rebuilt, re-assessed request can replace it.
            if assessment.decision==amounts::Decision::Excluded {
                assessment.elapsed_ns=elapsed_ns(started);report.record(&assessment);
                report.packed_exclusions+=1;
                report.retain_exclusion(assessment,&self.packed_amount_inputs(&selection),serde_json::json!({
                    "scope":"actual_packed_request","request_id":selection.request.id,
                    "graph_generation":self.generation,"signal_revision":self.revision,
                    "topology_id":candidate.topology_id,"reverse":candidate.reverse,
                    "trigger":selection.trigger,"source_witness":candidate.witness,
                    "universe_decision":global.decision
                }));return Ok(None);
            }
        }
        assessment.elapsed_ns=elapsed_ns(started);report.record(&assessment);
        selection.amount_assessment=assessment;Ok(Some(selection))
    }
    fn apply(&mut self, s:&SignalStateUpdate, recovering:bool,preparation:&mut Preparation)->Result<()> {
        self.received_at_us=now_us();
        ensure!(recovering || s.revision==self.revision+1, "candidate state revision gap");
        if let Some(snapshot)=&s.snapshot {
            ensure!(snapshot.plan.graph_generation==s.graph_generation
                && snapshot.plan.log_weight_scale==SIGNAL_LOG_SCALE
                && snapshot.plan.score_basis==SIGNAL_SCORE_BASIS
                && snapshot.pools.len()==snapshot.prices.len(), "invalid candidate snapshot");
            self.plan=Some(snapshot.plan.clone()); self.pools=snapshot.pools.clone(); self.prices=snapshot.prices.clone();
            let started=Instant::now();self.pair_pools.clear();
            for (id,pool) in self.pools.iter().enumerate() {
                self.pair_pools.entry((pool.mint_a,pool.mint_b)).or_default().push(id);
            }
            preparation.pair_index_rebuilds+=1;
            preparation.pair_index_ns.record(elapsed_ns(started));
            self.generation=s.graph_generation;
            self.threshold=snapshot.threshold_units;
            self.active.clear();self.active_witnesses.clear();
            for candidate in &snapshot.candidates {
                self.witness(candidate.topology_id,candidate.reverse,&candidate.witness)?;
                let key=(candidate.topology_id,candidate.reverse);
                ensure!(self.active.insert(key),"duplicate execution candidate snapshot key");
                self.active_witnesses.insert(key,SignalCandidate{
                    topology_id:candidate.topology_id,reverse:candidate.reverse,
                    change:SignalRangeChange::Updated,witness:Some(candidate.witness.clone())});
            }
        } else {ensure!(self.plan.is_some() && s.graph_generation==self.generation, "candidate delta without snapshot");}
        if s.clear {self.prices.fill(None);self.active.clear();self.active_witnesses.clear();}
        for change in &s.changes {
            if change.pool_id==self.pools.len() {
                let started=Instant::now();
                self.pair_pools.entry((change.identity.mint_a,change.identity.mint_b))
                    .or_default().push(change.pool_id);
                self.pools.push(change.identity.clone()); self.prices.push(None);
                preparation.pair_index_appends+=1;
                preparation.pair_index_ns.record(elapsed_ns(started));
            }
            ensure!(self.pools.get(change.pool_id)==Some(&change.identity), "candidate pool identity mismatch");
            self.prices[change.pool_id]=change.price.clone();
        }
        preparation.indexed_pools=self.pools.len();preparation.indexed_pairs=self.pair_pools.len();
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
            SignalRangeChange::Exited=>{
                ensure!(c.witness.is_none()&&self.active.remove(&key),
                    "execution candidate exit missing or carries witness");
                self.active_witnesses.remove(&key);
            },
            SignalRangeChange::Entered|SignalRangeChange::Updated=>{
                self.witness(c.topology_id,c.reverse,c.witness.as_ref().context("candidate witness absent")?)?;
                if c.change==SignalRangeChange::Entered {
                    ensure!(self.active.insert(key),"execution candidate entered twice");
                } else {ensure!(self.active.contains(&key),"execution candidate updated before entering");}
                self.active_witnesses.insert(key,c.clone());
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
            manifest_checked_at_ms:state.completion_checked_at_ms,liquidity_window_checked,
            direction_liquidity_status:price.direction_liquidity_status[slot%2]};
        Ok((ExecutionPool{manifest,mints},evidence))
    }
    /// Pack an exact CP6A best-witness without rebuilding any route product.
    /// Alternatives are drawn from currently priced slots with the same mint roles.
    fn build_compact(&self,c:&SignalCandidate,threshold:i64,trigger:Arc<SignalTrigger>,
        registry:&PoolRegistry,cfg:&Settings,number:usize,reads:&mut RegistryReads,strategy:PackingStrategy,
        reference_packs:&[Vec<usize>],preferred:Option<&[usize]>,preparation:&mut Preparation)->Result<Selection>{
        let started=Instant::now();
        let result=(||->Result<Selection>{
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
            let resolving=Instant::now();let next=self.pool(slot,r,registry,cfg.wallet,reads);
            preparation.pool_resolution_ns.record(elapsed_ns(resolving));
            pools.push(next?);
        }
        install_compact(&mut request,top,&pools);
        let checking=Instant::now();let checked=(cfg.capacity)(&request);
        preparation.capacity_checks+=1;preparation.capacity_rejections+=u64::from(checked.is_err());
        preparation.capacity_validation_ns.record(elapsed_ns(checking));
        let mut packing=checked.context("compact seed does not fit V10")?;
        let lookup=Instant::now();
        let mut options=Vec::new();
        let mut alternative_profiles=Vec::<Alternative>::new();
        // Only direct/partner roles accept parallel alternatives in the current
        // V10 group contract. Intermediate/conversion remain the best chosen pool.
        for (i,&(selected,role_name)) in slots.iter().enumerate(){
            if role_name!="direct"&&role_name!="partner"{continue;}
            let target=self.pools.get(selected/2).context("candidate identity missing")?;
            let (a,b)=(target.mint_a,target.mint_b);
            let original=self.weight(selected).context("seed weight missing")?;
            let matching=self.pair_pools.get(&(a,b)).context("candidate mint pair absent from identity index")?;
            preparation.alternative_scans+=1;
            preparation.indexed_pool_visits+=matching.len() as u64;
            preparation.full_scan_pool_visits_equivalent+=self.pools.len() as u64;
            for &id in matching {
                if used.contains(&id){continue;}
                let slot=2*id+selected%2;
                if let Some(w)=self.weight(slot){
                    let new_sum=i128::from(seed_sum)-i128::from(original)+i128::from(w);
                    if strategy!=PackingStrategy::Baseline || new_sum<i128::from(threshold){
                        options.push((new_sum,slot,role_name,i));
                        let obs=self.prices.get(id).and_then(Option::as_ref).map(|p|p.observation.as_ref());
                        let depth=obs.and_then(|o|o.liquidity.as_ref()).and_then(|p|p.directions.iter()
                            .find(|d|d.input_mint==if slot%2==0{target.mint_a.to_string()}else{target.mint_b.to_string()}))
                            .and_then(|d|d.input_capacity_raw.as_deref()).and_then(|s|s.parse::<u128>().ok());
                        alternative_profiles.push(Alternative{slot,role:i,marginal_score:new_sum,
                            depth_input_raw:depth,venue:obs.map_or("unknown",|o|o.venue)});
                    }
                }
            }
        }
        preparation.alternative_options+=alternative_profiles.len() as u64;
        preparation.alternative_lookup_ns.record(elapsed_ns(lookup));
        let ranking_started=Instant::now();
        let ranking=if strategy==PackingStrategy::Coverage {
            preparation.coverage_rankings+=1;
            preparation.coverage_key_evaluations+=alternative_profiles.len() as u64;
            diversity::coverage_order(&alternative_profiles,&slots.iter().map(|(s,_)|s/2).collect::<Vec<_>>(),
                reference_packs)
        } else {diversity::order(strategy,&alternative_profiles)};
        preparation.ranking_ns.record(elapsed_ns(ranking_started));
        let mut considered=0;let mut rejected=0;let mut unavailable=0;
        let mut last_rejection=None;
        for index in ranking{
            let (_,slot,role_name,_)=options[index];
            if used.contains(&(slot/2)){continue;}
            considered+=1;
            let resolving=Instant::now();let next=self.pool(slot,role_name,registry,cfg.wallet,reads);
            preparation.pool_resolution_ns.record(elapsed_ns(resolving));
            let pool=match next{
                Ok(pool)=>pool,Err(error)=>{
                    unavailable+=1;last_rejection=Some(format!("{error:#}"));continue;
                }
            };
            pools.push(pool);
            install_compact(&mut request,top,&pools);
            let checking=Instant::now();let checked=(cfg.capacity)(&request);
            preparation.capacity_checks+=1;preparation.capacity_rejections+=u64::from(checked.is_err());
            preparation.capacity_validation_ns.record(elapsed_ns(checking));
            match checked{
                Ok(metrics)=>{packing=metrics;used.insert(slot/2);},
                Err(error)=>{pools.pop();rejected+=1;last_rejection=Some(format!("{error:#}"));}
            }
        }
        install_compact(&mut request,top,&pools);
        let ready=now_us();
        let frame=self.price_pipeline.map(|t|t.frame_received_at_us);
        let alternative_pools_added=pools.len()-slots.len();
        let estimated_net_return_bps=(-(seed_sum as f64)/SIGNAL_LOG_SCALE as f64).exp_m1()*10000.0;
        Ok(Selection{request,packing_strategy:strategy.label(),packing_revision_variant:0,
            cp9d_source:"fresh",cp9d_pair_index:0,graph_generation:self.generation,revision:self.revision,
            topology_id:Some(c.topology_id),route_kind:top.kind,target:top.target,reverse:c.reverse,
            trigger,candidate:Some(c.clone()),threshold_units:threshold,
            estimated_net_return_bps:estimated_net_return_bps.is_finite().then_some(estimated_net_return_bps),
            pools:pools.into_iter().map(|(_,e)|e).collect(),packing,
            signal_ready_at_us:self.ready_at_us,candidate_received_at_us:self.received_at_us,
            request_ready_at_us:ready,price_pipeline:self.price_pipeline,
            frame_to_request_us:frame.and_then(|at|ready.checked_sub(at)),
            signal_to_request_us:ready.saturating_sub(self.ready_at_us),round_elapsed_ns:0,
            registry_reads:reads.clone(),
            alternatives_considered:considered,packing_rejections:rejected,
            unavailable_alternatives:unavailable,last_alternative_rejection:last_rejection,
            alternative_pools_added,packing_seed_log_sum_units:seed_sum,
            amount_assessment:amounts::Assessment::unknown("not_evaluated"),amount_seed_changed})
        })();
        preparation.pack_build_ns.record(elapsed_ns(started));
        result
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

pub async fn run(mut input:SignalStream, registry:PoolRegistry, output:mpsc::Sender<ExecutionRequest>,
    evidence:mpsc::Sender<EvidenceItem>, cfg:Settings)->Result<Report> {
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
                    state.apply(s,report.awaiting_snapshot,&mut report.preparation)?;
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
                    if (cfg.limit>0 && report.request_count as usize>=cfg.limit)
                        ||!matches!(b.trigger.as_ref(),SignalTrigger::Price{origin:PriceOrigin::Websocket|PriceOrigin::Revaluation,..})
                        ||b.price_pipeline.is_some_and(|t|t.initialization_replay){continue;}
                    for (index,candidate) in b.candidates.iter().enumerate(){
                        if candidate.change==SignalRangeChange::Exited{continue;}
                        let top=state.plan.as_ref()
                            .and_then(|p|p.compact_topologies.get(candidate.topology_id))
                            .context("candidate references unknown topology")?;
                        if !cfg.accepts(top.kind){continue;}
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
                    let started=Instant::now();
                    report.preparation.signal_to_round_start_ns.record(
                        now_us().saturating_sub(state.ready_at_us).saturating_mul(1000));
                    let mut reads=RegistryReads::default();
                    ranked.sort_unstable_by_key(|entry|entry.0);
                    // CP9D: one source candidate per distinct physical token pair.
                    // First give changed, currently priced witnesses priority. Then
                    // rotate through authoritative active witnesses from this
                    // generation. Never invent a profitable source witness.
                    type RoutePair=(SignalRouteKind,Pubkey,Pubkey);
                    let mut sources=Vec::<(SignalCandidate,Arc<SignalTrigger>,i64,&'static str,RoutePair)>::new();
                    let mut pairs=HashSet::new();
                    for (_,event,index) in &ranked {
                        let SignalEvent::Candidates(block)=event.as_ref() else {continue};
                        let candidate=&block.candidates[*index];
                        let Some(top)=state.plan.as_ref().and_then(|p|p.compact_topologies.get(candidate.topology_id)) else{continue};
                        let pair=(top.kind,top.settlement.min(top.target),top.settlement.max(top.target));
                        if !pairs.insert(pair){continue;}
                        sources.push((candidate.clone(),block.trigger.clone(),block.threshold_units,"fresh",pair));
                        if sources.len()>=cfg.cp9d_max_pairs{break;}
                    }
                    if sources.len()<cfg.cp9d_max_pairs && cfg.cp9d_max_pairs>1 {
                        let count=state.active_witnesses.len();
                        if count>0 {
                            let offset=(state.revision as usize)%count;
                            for (_,candidate) in state.active_witnesses.iter().skip(offset)
                                .chain(state.active_witnesses.iter().take(offset)).take(512) {
                                report.cp9d_active_candidates_examined+=1;
                                if candidate.change==SignalRangeChange::Exited {continue;}
                                let Some(top)=state.plan.as_ref().and_then(|p|p.compact_topologies.get(candidate.topology_id)) else{continue};
                                if !cfg.accepts(top.kind){continue;}
                                let pair=(top.kind,top.settlement.min(top.target),top.settlement.max(top.target));
                                if pairs.contains(&pair){continue;}
                                let Some(w)=candidate.witness.as_ref() else{continue};
                                if w.log_sum_units>=state.threshold ||
                                    state.witness(candidate.topology_id,candidate.reverse,w).is_err(){continue;}
                                pairs.insert(pair);
                                sources.push((candidate.clone(),e.trigger.clone(),state.threshold,"active",pair));
                                if sources.len()>=cfg.cp9d_max_pairs{break;}
                            }
                        }
                    }
                    // Preserve a comparable Direct3 arm when current positive
                    // signals exist, even if many Direct2 entries rank ahead.
                    // Otherwise a 3-source limit could silently starve Direct3.
                    if cfg.routes.contains(SignalRouteKind::Direct3) && cfg.cp9d_max_pairs>=2
                        && !sources.iter().any(|s|s.4.0==SignalRouteKind::Direct3) {
                        let mut reserved=None;
                        for (_,event,index) in &ranked {
                            let SignalEvent::Candidates(batch)=event.as_ref() else{continue};
                            let c=&batch.candidates[*index];
                            let Some(top)=state.plan.as_ref().and_then(|p|
                                p.compact_topologies.get(c.topology_id))else{continue};
                            if top.kind!=SignalRouteKind::Direct3{continue;}
                            let key=(top.kind,top.settlement.min(top.target),top.settlement.max(top.target));
                            if pairs.contains(&key){continue;}
                            reserved=Some((c.clone(),batch.trigger.clone(),
                                batch.threshold_units,"fresh",key));
                            break;
                        }
                        if reserved.is_none(){
                            for (_,c) in state.active_witnesses.iter().take(512){
                                report.cp9d_active_candidates_examined+=1;
                                let Some(top)=state.plan.as_ref().and_then(|p|
                                    p.compact_topologies.get(c.topology_id))else{continue};
                                if top.kind!=SignalRouteKind::Direct3{continue;}
                                let key=(top.kind,top.settlement.min(top.target),top.settlement.max(top.target));
                                if pairs.contains(&key){continue;}
                                let Some(w)=c.witness.as_ref()else{continue};
                                if w.log_sum_units>=state.threshold
                                    ||state.witness(c.topology_id,c.reverse,w).is_err(){continue;}
                                reserved=Some((c.clone(),e.trigger.clone(),
                                    state.threshold,"active",key));
                                break;
                            }
                        }
                        if let Some(source)=reserved{
                            if sources.len()>=cfg.cp9d_max_pairs{
                                if let Some(evicted)=sources.pop(){pairs.remove(&evicted.4);}
                            }
                            pairs.insert(source.4);
                            if source.3=="fresh"{report.cp9e_direct3_reserved_from_fresh+=1;}
                            else{report.cp9e_direct3_reserved_from_active+=1;}
                            sources.push(source);
                        }
                    }
                    let mut previous=HashMap::<RoutePair,Vec<Vec<usize>>>::new();
                    let mut sent_sets=HashMap::<RoutePair,HashSet<Vec<String>>>::new();
                    let mut sent_pair=HashSet::new();
                    let mut invalid_seed=false;
                    let mut busy_revision=false;
                    // The first round tests each token pair before allowing any
                    // pair to consume more queue slots with its second strategy.
                    // Coverage always runs after comparison packages.
                    let ordered=diversity::strategy_schedule(&cfg.strategies,state.revision);
                    'packages: for (variant,strategy) in ordered.into_iter().enumerate(){
                        for (pair_index,(candidate,trigger,threshold,origin,pair)) in sources.iter().enumerate(){
                            if cfg.limit>0 && report.request_count as usize>=cfg.limit{break 'packages;}
                            report.cp9c_pack_attempts+=1;
                            // No awaiting capacity: prices and signal decisions must
                            // never be blocked by slower RPC simulation responses.
                            let permit=match output.try_reserve() {
                                Ok(permit)=>permit,
                                Err(mpsc::error::TrySendError::Full(_))=>{
                                    report.cp9c_queue_full_packs+=1;
                                    *report.cp9d_queue_full_by_strategy.entry(strategy.label().into()).or_default()+=1;
                                    busy_revision=true;break 'packages;
                                },
                                Err(mpsc::error::TrySendError::Closed(_))=>bail!("V10 request receiver stopped"),
                            };
                            let references=previous.get(pair).map(Vec::as_slice).unwrap_or(&[]);
                            let selected=state.build_compact(candidate,*threshold,trigger.clone(),
                                &registry,&cfg,report.request_count as usize+1,&mut reads,strategy,references,None,
                                &mut report.preparation);
                            let selection=match selected{
                                Ok(selection)=>selection,
                                Err(error)=>{
                                    invalid_seed=true;report.skipped_invalid_seed+=1;
                                    report.last_rejection=Some(format!("{error:#}"));continue;
                                }
                            };
                            let mut pool_set=selection.pools.iter().map(|p|p.pool.clone()).collect::<Vec<_>>();
                            pool_set.sort_unstable();
                            if sent_sets.get(pair).is_some_and(|sent|sent.contains(&pool_set)){
                                report.cp9c_duplicate_packs+=1;continue;
                            }
                            let Some(mut selection)=state.assess_selection(selection,&registry,&cfg,&mut reads,
                                &mut report.amounts,&mut report.preparation)? else{
                                continue;
                            };
                            let mut actual=selection.pools.iter().map(|p|p.pool.clone()).collect::<Vec<_>>();
                            actual.sort_unstable();
                            // Once the amount assessment has been accounted for,
                            // dispatch the request; dropping it would violate
                            // successful-quantity vs request delivery accounting.
                            sent_sets.entry(*pair).or_default().insert(actual);
                            previous.entry(*pair).or_default().push(
                                selection.pools.iter().map(|p|p.slot/2).collect());
                            selection.cp9d_source=*origin;
                            selection.cp9d_pair_index=pair_index as u8;
                            selection.packing_revision_variant=variant as u8;
                            selection.round_elapsed_ns=elapsed_ns(started);
                            selection.registry_reads=reads.clone();
                            selection.request_ready_at_us=now_us();
                            selection.frame_to_request_us=selection.price_pipeline.and_then(|t|
                                selection.request_ready_at_us.checked_sub(t.frame_received_at_us));
                            selection.signal_to_request_us=selection.request_ready_at_us.saturating_sub(selection.signal_ready_at_us);
                            *report.cp9c_by_strategy.entry(strategy.label().into()).or_default()+=1;
                            report.cp9c_packs_dispatched+=1;
                            if sent_pair.insert(*pair) {
                                report.cp9d_distinct_pairs_dispatched+=1;
                                if *origin=="fresh"{report.cp9d_fresh_pairs+=1;}
                                else{report.cp9d_active_pairs+=1;}
                            }
                            crate::log_event!(Debug,"execution candidates request_id={} strategy={} source={} pair_rank={} generation={} revision={} pools={} accounts={} round_elapsed_ns={} broadcasts=false",
                                selection.request.id,selection.packing_strategy,selection.cp9d_source,selection.cp9d_pair_index,
                                state.generation,state.revision,selection.packing.pool_declarations,
                                selection.packing.unique_accounts,selection.round_elapsed_ns);
                            // Evidence is queued without awaiting filesystem I/O.
                            // On overload, fail explicitly rather than claiming a complete audit.
                            let request=selection.request.clone();
                            evidence.try_send(EvidenceItem::Selection(Box::new(selection)))
                                .map_err(|err|anyhow::anyhow!("V10 evidence queue unavailable (nonblocking): {err}"))?;
                            permit.send(request);report.request_count+=1;
                        }
                    }
                    if busy_revision{report.skipped_busy_revisions+=1;}
                    report.preparation.round_work_ns.record(elapsed_ns(started));
                    ranked.clear();
                    report.registry_reads.merge(&reads);
                    if invalid_seed {
                        crate::log_event!(Debug,"execution candidates rejected_seed=true generation={} revision={} last_rejection={:?}",
                            state.generation,state.revision,report.last_rejection);
                    }
                },
            }
        }
        ensure!(!pending,"candidate stream closed during evaluation");
        Ok(())
    }.await;
    report.error=outcome.err().map(|e|format!("{e:#}"));
    crate::log_event!(Info,"execution candidates complete=true requests={} lost={} error={:?}",report.request_count as usize,report.lost_events,report.error);
    Ok(report)
}
