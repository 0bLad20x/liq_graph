//! Bounded streaming result journal. Full RPC/manifest data is sampled, never accumulated.
use std::{collections::{HashMap,HashSet},fs::File,io::{BufWriter,Write},path::PathBuf,time::Instant};
use anyhow::{Result,Context,ensure};
use serde_json::{json,Value};
use tokio::sync::mpsc;
use crate::{core::execution::{ExecutionOutcome,ExecutionReport},
    workers::{execution_candidates::{EvidenceItem,Selection},execution_v10}};

const SELECTION:&[&str]=&["id","settlement","target","route_kind","reverse","graph_generation","revision",
    "packing_strategy","packing_revision_variant","cp9d_source","cp9d_pair_index","pools","source_pools",
    "seed_pools","source_log_sum_units","packing_seed_log_sum_units","threshold_units",
    "marginal_signal_bps","estimated_net_return_bps","amount_decision","amount_estimate",
    "liquidity_checked","packing","request_ready_at_us"];
const REPORT:&[&str]=&["context_slot","provider","err","error_name","fee","unitsConsumed",
    "balance_changes","trace_status","dex_call_count"];
const CALIBRATION:&[&str]=&["source_path_relation","seed_path_relation","trade_input_raw","trade_output_raw",
    "settlement_input_refund_raw","model_route_output_raw","model_route_unavailable_reason",
    "source_model_output_at_trade_input_raw","source_model_unavailable_reason",
    "seed_model_output_at_trade_input_raw","seed_model_unavailable_reason","actual_path_marginal_bps","legs"];
const LEG:&[&str]=&["pool","program","venue","flow_status","input_mint","output_mint","input_gross_raw",
    "input_net_raw","observed_output_raw","model_output_raw","model_unavailable_reason",
    "snapshot_content_digest","observation_slot","component_slot_min","component_slot_max",
    "observation_age_at_request_us","observation_consistency","issues"];
const TIMING:&[&str]=&["selection_ns","frame_to_request_us","signal_to_request_us","build_us",
    "simulation_rpc_ms","elapsed_ms","trace_us","calibration_ns"];

#[derive(Default,serde::Serialize)]
pub(super) struct Summary {
    pub matched:u64,pub report_successes:u64,pub report_failures:u64,pub dex_call_reports:u64,
    pub detail_samples:u64,pub event_bytes:u64,pub detail_bytes:u64,
    pub max_unmatched:usize,pub max_backlog:usize,pub flushes:u64,pub durable_syncs:u64,
}
fn columns(names:&[&str],value:&Value)->Value {
    Value::Array(names.iter().map(|name|value.get(*name).cloned().unwrap_or(Value::Null)).collect())
}
fn jsonline<W:Write,T:serde::Serialize>(writer:&mut W,value:&T)->Result<u64> {
    let bytes=serde_json::to_vec(value)?;
    writer.write_all(&bytes)?;writer.write_all(b"\n")?;Ok(bytes.len() as u64+1)
}
struct Writer {
    events:BufWriter<File>,details:BufWriter<File>,configuration:Option<Value>,
    sample_classes:HashSet<String>,detail_limit:usize,stats:Summary,last_flush:Instant,
}
impl Writer {
    fn new(folder:PathBuf,detail_limit:usize)->Result<Self>{
        std::fs::create_dir_all(&folder)?;
        Ok(Self{events:BufWriter::new(File::create(folder.join("execution_events.jsonl"))?),
            details:BufWriter::new(File::create(folder.join("execution_details.jsonl"))?),
            configuration:None,sample_classes:HashSet::new(),detail_limit,stats:Summary::default(),
            last_flush:Instant::now()})
    }
    fn flush(&mut self)->Result<()> {
        self.events.flush()?;self.details.flush()?;
        self.stats.flushes+=1;self.last_flush=Instant::now();Ok(())
    }
    fn record(&mut self,s:Selection,r:ExecutionReport)->Result<()> {
        ensure!(r.id==s.request.id,"evidence handoff id mismatch");
        ensure!(r.error.is_none()&&r.phase=="complete"&&r.executor_invoked&&r.simulation.is_some(),
            "request {} did not reach a complete V10 simulation",r.id);
        ensure!(serde_json::to_value(&r.metrics)?==serde_json::to_value(s.packing_metrics())?,
            "packing differs for {}",r.id);
        let compact=s.compact_evidence()?;
        let configuration=json!({"wallet":s.request.wallet.to_string(),"parameters":s.request.parameters});
        if let Some(previous)=&self.configuration {
            ensure!(previous==&configuration,"execution evidence configuration changed within one run");
        }else{
            self.stats.event_bytes+=jsonline(&mut self.events,&json!({
                "schema_version":4,"row":["payload_sha256","selection","report","calibration","timing"],
                "selection_columns":SELECTION,"report_columns":REPORT,"calibration_columns":CALIBRATION,
                "leg_columns":LEG,"timing_columns":TIMING,"configuration":configuration,
                "scope":"complete_simulations_with_bounded_full_examples"
            }))?;
            self.configuration=Some(configuration);
        }
        let mut payload=serde_json::to_value(&s.request)?;
        payload.as_object_mut().context("request must be an object")?.remove("id");
        let hash=ring::digest::digest(&ring::digest::SHA256,&serde_json::to_vec(&payload)?)
            .as_ref().iter().map(|b|format!("{b:02x}")).collect::<String>();
        let sim=r.simulation.as_ref().expect("validated simulation");
        let success=matches!(r.outcome,ExecutionOutcome::SimulatedNoDexCallObserved|ExecutionOutcome::SimulatedDexCallsObserved);
        ensure!(success==sim["err"].is_null(),"simulation outcome differs from RPC error");
        let balances=execution_v10::balance_changes(&s.request,&r)?;
        let error_name=sim["logs"].as_array().and_then(|logs|logs.iter().filter_map(Value::as_str)
            .find_map(|line|line.split_once("Error Code: ").map(|(_,tail)|tail.split('.').next().unwrap_or(tail))));
        let report=json!({"context_slot":r.context_slot,"provider":r.provider,"err":sim["err"],
            "error_name":error_name,"fee":sim["fee"],"unitsConsumed":sim["unitsConsumed"],
            "balance_changes":balances,"trace_status":r.trace.status,"dex_call_count":r.dex_invocations.len()});
        // Without a DEX call there is no observed trade quantity to quote.
        let mut calibration=(!r.trace.calls.is_empty()).then(||s.calibration(&r));
        if let Some(calibration)=&mut calibration {
            let mut rows=Vec::new();
            let legs=calibration["legs"].as_array_mut().context("calibration legs missing")?;
            for (mut facts,call) in std::mem::take(legs).into_iter().zip(&r.trace.calls) {
                facts["input_mint"]=json!(call.inputs.first().map(|x|&x.mint));
                facts["output_mint"]=json!(call.outputs.first().map(|x|&x.mint));
                facts["input_gross_raw"]=json!(call.inputs.first().map(|x|x.gross_amount_raw));
                facts["input_net_raw"]=json!(call.inputs.first().map(|x|x.amount_raw));
                facts["issues"]=json!(call.issues);
                if let Some(digest)=facts["snapshot_content_digest"].as_array() {
                    facts["snapshot_content_digest"]=json!(digest.iter().filter_map(Value::as_u64)
                        .map(|b|format!("{b:02x}")).collect::<String>());
                }
                rows.push(columns(LEG,&facts));
            }
            *legs=rows;
        }
        let timing=json!({"selection_ns":compact["selection_ns"],"frame_to_request_us":compact["frame_to_request_us"],
            "signal_to_request_us":compact["signal_to_request_us"],"build_us":r.build_us,
            "simulation_rpc_ms":r.simulation_rpc_ms,"elapsed_ms":r.elapsed_ms,"trace_us":r.trace_us,
            "calibration_ns":calibration.as_ref().map_or(json!(0),|c|c["processing_ns"].clone())});
        self.stats.event_bytes+=jsonline(&mut self.events,&json!([hash,columns(SELECTION,&compact),
            columns(REPORT,&report),calibration.as_ref().map(|c|columns(CALIBRATION,c)),
            columns(TIMING,&timing)]))?;
        self.stats.report_successes+=u64::from(success);
        self.stats.report_failures+=u64::from(!success);
        self.stats.dex_call_reports+=u64::from(!r.dex_invocations.is_empty());
        let ordinary_limit=if self.detail_limit>=4{self.detail_limit-self.detail_limit/4}else{self.detail_limit};
        let notable=balances.as_ref().and_then(|b|b.above_network_fee)==Some(true);
        if self.stats.detail_samples<ordinary_limit as u64
            ||(notable&&self.stats.detail_samples<self.detail_limit as u64) {
            let class=format!("{:?}:{:?}:{}:{:?}:{}:{:?}",r.outcome,r.dex_invocations,sim["err"],
                s.request.settlement,compact["packing_strategy"],balances.as_ref().and_then(|b|b.above_network_fee));
            if self.sample_classes.insert(class) {
                self.stats.detail_bytes+=jsonline(&mut self.details,&json!({"payload_sha256":hash,"selection":s,"report":r}))?;
                self.stats.detail_samples+=1;
            }
        }
        self.stats.matched+=1;
        if self.stats.matched%64==0||self.last_flush.elapsed().as_secs()>=5{self.flush()?;}
        if self.stats.matched%256==0 {
            self.events.get_ref().sync_data()?;self.details.get_ref().sync_data()?;
            self.stats.durable_syncs+=1;
        }
        Ok(())
    }
    fn finish(mut self)->Result<Summary>{
        self.flush()?;self.events.get_ref().sync_data()?;self.details.get_ref().sync_data()?;Ok(self.stats)
    }
}
/// Only unmatched in-flight jobs are retained, bounded to 1024 entries.
pub(super) fn run(mut input:mpsc::Receiver<EvidenceItem>,folder:PathBuf,detail_limit:usize)->Result<Summary>{
    ensure!(detail_limit<=256,"RUN_EXECUTION_DETAIL_LIMIT must be 0..256");
    let mut writer=Writer::new(folder,detail_limit)?;
    let mut selections=HashMap::<String,Box<Selection>>::new();
    let mut reports=HashMap::<String,Box<ExecutionReport>>::new();
    while let Some(item)=input.blocking_recv(){
        match item {
            EvidenceItem::Selection(s)=>{
                let id=s.request.id.clone();
                if let Some(r)=reports.remove(&id){writer.record(*s,*r)?;}
                else{ensure!(selections.insert(id.clone(),s).is_none(),"duplicate evidence selection {id}");}
            },
            EvidenceItem::Report(r)=>{
                let id=r.id.clone();
                if let Some(s)=selections.remove(&id){writer.record(*s,*r)?;}
                else{ensure!(reports.insert(id.clone(),r).is_none(),"duplicate V10 evidence report {id}");}
            },
        }
        let pending=selections.len()+reports.len();
        writer.stats.max_unmatched=writer.stats.max_unmatched.max(pending);
        writer.stats.max_backlog=writer.stats.max_backlog.max(input.len());
        ensure!(pending<=1024,"V10 evidence unmatched buffer exceeded 1024 entries");
    }
    ensure!(selections.is_empty()&&reports.is_empty(),
        "V10 evidence missing pairs: selections={} reports={}",selections.len(),reports.len());
    writer.finish()
}
