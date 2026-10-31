//! CP9G: bounded, blocking-thread evidence spool. Never called on WS/MarketState task.
//! Join at most in-flight selections/reports; persist immediately, then release RAM.
use std::{collections::{HashMap,HashSet,VecDeque},fs::{File,OpenOptions},
    io::{BufWriter,Write},path::PathBuf,time::Instant};
use anyhow::{Result,Context,bail,ensure};
use serde_json::{json,Value};
use tokio::sync::mpsc;
use crate::{core::execution::{ExecutionOutcome,ExecutionReport},
    workers::execution_candidates::{EvidenceItem,Selection}};

#[derive(Default,serde::Serialize)]
pub(super) struct Summary {
    pub matched:u64,pub unique_payloads:u64,pub snapshots_written:u64,
    pub report_successes:u64,pub report_failures:u64,pub dex_call_reports:u64,
    pub detail_samples:u64,pub max_unmatched:usize,pub max_backlog:usize,
    pub flushes:u64,pub evidence_queue_failures:u64,
}

// A deliberately bounded accelerator, not a permanent in-memory payload database.
// Re-emitting a digest after eviction is valid: the reference remains content-addressed.
struct Recent {
    keys:HashSet<String>,order:VecDeque<String>,cap:usize,
}
impl Recent {
    fn new(cap:usize)->Self{Self{keys:HashSet::new(),order:VecDeque::new(),cap}}
    fn first_recent(&mut self,digest:String)->bool {
        if self.keys.contains(&digest){return false;}
        if self.order.len()==self.cap{
            if let Some(old)=self.order.pop_front(){self.keys.remove(&old);}
        }
        self.keys.insert(digest.clone());self.order.push_back(digest);true
    }
}
fn sha(data:&[u8])->String {
    let digest=ring::digest::digest(&ring::digest::SHA256,data);
    digest.as_ref().iter().map(|b|format!("{b:02x}")).collect()
}
fn jsonline<W:Write,T:serde::Serialize>(w:&mut W,value:&T)->Result<()> {
    serde_json::to_writer(&mut *w,value)?;w.write_all(b"\n")?;Ok(())
}
fn compact_simulation(report:&ExecutionReport)->Result<Value>{
    let mut compact=serde_json::to_value(report)?;
    if let Some(sim)=compact.get_mut("simulation").and_then(Value::as_object_mut){
        sim.retain(|k,_|matches!(k.as_str(),"err"|"fee"|"unitsConsumed"|"preBalances"|
            "postBalances"|"preTokenBalances"|"postTokenBalances"|"logs"));
        if let Some(logs)=sim.get_mut("logs").and_then(Value::as_array_mut){
            logs.retain(|line|line.as_str().is_some_and(|v|v.contains("Error")||v.contains("failed:")));
        }
    }
    Ok(compact)
}
fn store_snapshots(
    selection:&mut Value,cache:&mut Recent,snapshots:&mut BufWriter<File>,stats:&mut Summary
)->Result<()> {
    let Some(pools)=selection.get_mut("pools").and_then(Value::as_array_mut) else{return Ok(());};
    for pool in pools {
        let Some(observation)=pool.get_mut("observation") else{continue;};
        if observation.is_null(){continue;}
        let bytes=serde_json::to_vec(observation)?;
        let digest=sha(&bytes);
        if cache.first_recent(digest.clone()){
            jsonline(snapshots,&json!({"observation_ref":digest,"observation":observation}))?;
            stats.snapshots_written+=1;
        }
        // The analyzer only needs these two properties for its invariants;
        // detailed amount-specific calibration has already been computed
        // against the full frozen original and is saved with this event.
        let venue=observation.get("venue").cloned().unwrap_or(Value::Null);
        let liquidity=observation.get("liquidity").filter(|v|!v.is_null()).map(|v|{
            json!({"content_digest":v.get("content_digest")})
        }).unwrap_or(Value::Null);
        *observation=json!({"venue":venue,"liquidity":liquidity,"snapshot_ref":digest});
        pool["observation_ref"]=Value::String(digest);
    }
    Ok(())
}

struct Writer {
    events:BufWriter<File>,payloads:BufWriter<File>,details:BufWriter<File>,
    snapshots:BufWriter<File>,seen_payloads:Recent,seen_snapshots:Recent,
    sample_classes:HashSet<String>,detail_limit:usize,stats:Summary,
    last_flush:Instant,
}
impl Writer {
    fn new(folder:PathBuf,detail_limit:usize)->Result<Self>{
        std::fs::create_dir_all(&folder)?;
        let create=|name:&str|File::create(folder.join(name));
        Ok(Self{
            events:BufWriter::new(create("execution_events.jsonl")?),
            payloads:BufWriter::new(create("execution_payloads.jsonl")?),
            details:BufWriter::new(create("execution_details.jsonl")?),
            snapshots:BufWriter::new(create("execution_snapshots.jsonl")?),
            seen_payloads:Recent::new(32_768),seen_snapshots:Recent::new(32_768),
            sample_classes:HashSet::new(),detail_limit,stats:Summary::default(),
            last_flush:Instant::now()
        })
    }
    fn flush(&mut self)->Result<()>{
        self.events.flush()?;self.payloads.flush()?;self.details.flush()?;self.snapshots.flush()?;
        self.stats.flushes+=1;self.last_flush=Instant::now();Ok(())
    }
    fn record(&mut self,s:Selection,r:ExecutionReport)->Result<()>{
        ensure!(r.id==s.request.id,"evidence handoff id mismatch");
        ensure!(r.error.is_none()&&r.phase=="complete"&&r.executor_invoked&&r.simulation.is_some(),
            "request {} did not reach a complete V10 simulation",r.id);
        ensure!(serde_json::to_value(&r.metrics)?==serde_json::to_value(
            &s.compact_evidence()?["packing"])?, "packing differs for {}",r.id);
        let mut payload=serde_json::to_value(&s.request)?;
        payload.as_object_mut().context("request must be an object")?.remove("id");
        let hash=sha(&serde_json::to_vec(&payload)?);
        if self.seen_payloads.first_recent(hash.clone()){
            jsonline(&mut self.payloads,&json!({"payload_sha256":hash,"request":&s.request}))?;
            self.stats.unique_payloads+=1;
        }
        let calibration=s.calibration(&r);
        let mut compact=s.compact_evidence()?;
        store_snapshots(&mut compact,&mut self.seen_snapshots,&mut self.snapshots,&mut self.stats)?;
        let compact_report=compact_simulation(&r)?;
        jsonline(&mut self.events,&json!({
            "schema_version":3,"payload_sha256":hash,"selection":compact,
            "report":compact_report,"calibration":calibration
        }))?;
        let success=matches!(r.outcome,ExecutionOutcome::SimulatedNoDexCallObserved|
            ExecutionOutcome::SimulatedDexCallsObserved);
        self.stats.report_successes+=u64::from(success);
        self.stats.report_failures+=u64::from(!success);
        self.stats.dex_call_reports+=u64::from(!r.dex_invocations.is_empty());
        let class=format!("{:?}:{:?}:{}:{:?}",r.outcome,r.dex_invocations,
            r.simulation.as_ref().map_or(Value::Null,|v|v["err"].clone()),s.request.settlement);
        if self.stats.detail_samples < self.detail_limit as u64
            && self.sample_classes.insert(class) {
            jsonline(&mut self.details,&json!({
                "payload_sha256":hash,"selection":s,"report":r
            }))?;
            self.stats.detail_samples+=1;
        }
        self.stats.matched+=1;
        if self.stats.matched%64==0 || self.last_flush.elapsed().as_secs()>=5{self.flush()?;}
        Ok(())
    }
    fn finish(mut self)->Result<Summary>{
        self.flush()?;
        // Periodic flush already bounds userspace buffers. Explicit sync_data
        // makes successful shutdown durable at the filesystem interface.
        self.events.get_ref().sync_data()?;
        self.payloads.get_ref().sync_data()?;
        self.details.get_ref().sync_data()?;
        self.snapshots.get_ref().sync_data()?;
        Ok(self.stats)
    }
}

/// Called exclusively from a dedicated spawn_blocking thread.
/// The Rust process keeps no unbounded request/report vectors.
pub(super) fn run(mut input:mpsc::Receiver<EvidenceItem>,folder:PathBuf,
    detail_limit:usize)->Result<Summary>{
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
        ensure!(pending<=1024,"V10 evidence unmatched buffer exceeded 1024 entries: writer stalled");
    }
    ensure!(selections.is_empty()&&reports.is_empty(),
        "V10 evidence missing pairs: selections={} reports={}",selections.len(),reports.len());
    writer.finish()
}
