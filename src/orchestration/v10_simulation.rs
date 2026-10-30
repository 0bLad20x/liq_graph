//! File source -> typed channel -> execution worker -> JSON report. No route decisions.
use std::{collections::{HashMap,HashSet},env,path::Path,time::Duration,io::{BufWriter,Write}};
use anyhow::{Context, Result, bail, ensure};
use tokio::{sync::{mpsc,watch},task::JoinHandle};
use crate::{core::{execution::{ExecutionOutcome,ExecutionReport,ExecutionRequest},signals::SignalStream},
    workers::{execution_candidates,execution_v10,pool_registry,rpc_transport}};

pub async fn run(request_path: &Path, wallet_path: &Path) -> Result<()> {
    let bytes=std::fs::read(request_path).context("read V10 execution request")?;
    let request:ExecutionRequest=serde_json::from_slice(&bytes).context("decode complete V10 execution request")?;
    let settings=execution_v10::Settings::load(wallet_path)?;
    let (rpc,rpc_task)=rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
    let (jobs,input)=mpsc::channel(1);
    let (output,mut reports)=mpsc::channel(1);
    let worker=tokio::spawn(execution_v10::run(settings,rpc,input,output));
    let sent=jobs.send(request).await;
    drop(jobs);
    let report=reports.recv().await;
    let worker_result=worker.await.context("join V10 worker");
    let rpc_result=rpc_task.await.context("join RPC worker");
    if let Some(report)=&report { println!("{}",serde_json::to_string_pretty(report)?); }
    worker_result??;
    rpc_result??;
    sent.context("V10 request consumer stopped")?;
    let report=report.context("V10 worker returned no report")?;
    ensure!(matches!(report.outcome,ExecutionOutcome::SimulatedNoDexCallObserved
        |ExecutionOutcome::SimulatedDexCallsObserved),
        "V10 request {:?} failed; inspect the JSON report",report.id);
    Ok(())
}

// Keep accepted execution jobs owned through shutdown; cancellation aborts their tasks.
struct Task<T>(JoinHandle<T>);
impl<T> Drop for Task<T> {fn drop(&mut self){self.0.abort();}}

pub(super) struct ExecutionTasks {
    candidates:Task<Result<execution_candidates::Report>>,
    executor:Task<Result<execution_v10::ParallelReport>>,
    reports:Task<Vec<ExecutionReport>>,
}
impl ExecutionTasks {
    pub(super) fn start(settings:execution_v10::Settings,input:SignalStream,
        registry:pool_registry::PoolRegistry,rpc:crate::core::rpc::RpcHandle)->Result<Self> {
        let cfg=execution_candidates::Settings::from_env(settings.wallet(),execution_v10::validate)?;
        let parallelism:usize=env::var("GRAPH_V10_SIMULATION_PARALLELISM")
            .unwrap_or_else(|_|"1".into()).parse().context("invalid GRAPH_V10_SIMULATION_PARALLELISM")?;
        ensure!((1..=64).contains(&parallelism),"GRAPH_V10_SIMULATION_PARALLELISM must be 1..64");
        let (jobs,requests)=mpsc::channel(parallelism);
        let (output,mut responses)=mpsc::channel(1);
        let reports=Task(tokio::spawn(async move {
            let mut received=Vec::new();
            while let Some(report)=responses.recv().await {received.push(report);}
            received
        }));
        Ok(Self {
            candidates:Task(tokio::spawn(execution_candidates::run(input,registry,jobs,cfg))),
            executor:Task(tokio::spawn(execution_v10::run_parallel(settings,rpc,requests,output,parallelism))),reports,
        })
    }
    /// Production candidates are long-lived consumers. Only drain them after the
    /// orchestration has stopped its upstream producers.
    pub(super) async fn until_stop(&mut self,mut stop:watch::Receiver<bool>)->Result<()> {
        if *stop.borrow() {return Ok(());}
        tokio::select! {
            changed=stop.changed()=>{
                changed.context("runtime stop channel closed before shutdown")?;
                ensure!(*stop.borrow(),"runtime stop channel changed without stopping");
                Ok(())
            },
            result=&mut self.candidates.0=>{
                match result {
                    Ok(Ok(report))=>bail!("V10 candidate worker exited before runtime shutdown: requests={} error={:?}",report.requests.len(),report.error),
                    Ok(Err(error))=>Err(error).context("V10 candidate worker exited before runtime shutdown"),
                    Err(error)=>Err(error).context("V10 candidate worker task failed before shutdown"),
                }
            },
            result=&mut self.executor.0=>{
                match result {
                    Ok(Ok(()))=>bail!("V10 executor exited before runtime shutdown"),
                    Ok(Err(error))=>Err(error).context("V10 executor exited before runtime shutdown"),
                    Err(error)=>Err(error).context("V10 executor task failed before shutdown"),
                }
            },
            result=&mut self.reports.0=>{
                match result {
                    Ok(reports)=>bail!("V10 report collector exited before runtime shutdown after {} reports",reports.len()),
                    Err(error)=>Err(error).context("V10 report collector task failed before shutdown"),
                }
            },
        }
    }
    pub(super) async fn finish(&mut self,evidence:&mut serde_json::Value)->Result<execution_candidates::Report> {
        let mut candidates=tokio::time::timeout(Duration::from_secs(30),&mut self.candidates.0).await
            .context("candidate shutdown timeout")???;
        let exclusions=candidates.take_exclusion_samples();
        // The candidate stream has closed. Let accepted jobs and reports drain
        // while the shared RPC transport remains alive.
        evidence["execution_candidates"]=serde_json::to_value(&candidates)?;
        let executed=tokio::time::timeout(Duration::from_secs(300),&mut self.executor.0).await
            .context("V10 simulation shutdown timeout").and_then(|r|r.context("V10 worker panicked")).and_then(|r|r);
        let reports=tokio::time::timeout(Duration::from_secs(10),&mut self.reports.0).await
            .context("V10 report shutdown timeout")??;
        let parallel_report=executed?;
        ensure!(parallel_report.completed_requests==reports.len() as u64,
            "parallel V10 executor completion/report count mismatch");
        evidence["v10_parallelism"]=serde_json::to_value(&parallel_report)?;
        evidence["schema_version"]=serde_json::json!(3);
        evidence["request_count"]=serde_json::json!(candidates.requests.len());
        evidence["report_count"]=serde_json::json!(reports.len());
        evidence["simulation_successes"]=serde_json::json!(reports.iter().filter(|r|matches!(r.outcome,
            ExecutionOutcome::SimulatedNoDexCallObserved|ExecutionOutcome::SimulatedDexCallsObserved)).count());
        evidence["simulation_failures"]=serde_json::json!(reports.iter().filter(|r|matches!(r.outcome,ExecutionOutcome::SimulationFailed)).count());
        evidence["simulation_dex_calls_observed"]=serde_json::json!(reports.iter().filter(|r|!r.dex_invocations.is_empty()).count());
        // All artifacts share the existing run directory; no additional code hierarchy.
        if let Ok(path)=env::var("GRAPH_EXECUTION_REPORT_PATH") {
            let folder=std::path::Path::new(&path).parent().context("report directory missing")?;
            let by_id:HashMap<_,_>=reports.iter().map(|r|(r.id.as_str(),r)).collect();
            let detail_limit:usize=env::var("RUN_EXECUTION_DETAIL_LIMIT").unwrap_or_else(|_|"16".into()).parse()?;
            ensure!(detail_limit<=256,"RUN_EXECUTION_DETAIL_LIMIT must be 0..256");
            let mut events=BufWriter::new(std::fs::File::create(folder.join("execution_events.jsonl"))?);
            let mut payloads=BufWriter::new(std::fs::File::create(folder.join("execution_payloads.jsonl"))?);
            let mut details=BufWriter::new(std::fs::File::create(folder.join("execution_details.jsonl"))?);
            let mut seen=HashSet::new();let mut sampled=HashSet::new();let mut detail_count=0;
            for exclusion in exclusions.iter().take(detail_limit) {
                serde_json::to_writer(&mut details,&serde_json::json!({"quantity_exclusion":exclusion}))?;
                writeln!(details)?;detail_count+=1;
            }
            for selection in &candidates.requests {
                let mut payload=serde_json::to_value(&selection.request)?;
                payload.as_object_mut().context("request is not an object")?.remove("id");
                let digest=ring::digest::digest(&ring::digest::SHA256,&serde_json::to_vec(&payload)?);
                let hash=digest.as_ref().iter().map(|b|format!("{b:02x}")).collect::<String>();
                if seen.insert(hash.clone()) {
                    serde_json::to_writer(&mut payloads,&serde_json::json!({"payload_sha256":hash,"request":selection.request}))?;
                    writeln!(payloads)?;
                }
                let report=by_id.get(selection.request.id.as_str()).copied();
                let mut compact=serde_json::to_value(report)?;
                if let Some(simulation)=compact.get_mut("simulation").and_then(serde_json::Value::as_object_mut) {
                    simulation.retain(|key,_|matches!(key.as_str(),"err"|"fee"|"unitsConsumed"|"preBalances"|"postBalances"|"preTokenBalances"|"postTokenBalances"|"logs"));
                    if let Some(logs)=simulation.get_mut("logs").and_then(serde_json::Value::as_array_mut) {
                        logs.retain(|line|line.as_str().is_some_and(|s|s.contains("Error")||s.contains("failed:")));
                    }
                    let wallet=selection.request.wallet.to_string();
                    for key in ["preTokenBalances","postTokenBalances"] {
                        if let Some(balances)=simulation.get_mut(key).and_then(serde_json::Value::as_array_mut) {
                            balances.retain(|balance|balance["owner"].as_str()==Some(wallet.as_str()));
                        }
                    }
                }
                serde_json::to_writer(&mut events,&serde_json::json!({"schema_version":3,"payload_sha256":hash,
                    "selection":selection.compact_evidence()?,"report":compact,
                    "calibration":report.map(|r|selection.calibration(r))}))?;
                writeln!(events)?;
                let class=format!("{:?}:{:?}:{}:{:?}",report.map(|r|r.outcome),report.map(|r|&r.dex_invocations),
                    report.and_then(|r|r.simulation.as_ref()).map_or(serde_json::Value::Null,|s|s["err"].clone()),
                    selection.request.settlement);
                if detail_count<detail_limit && sampled.insert(class) {
                    serde_json::to_writer(&mut details,&serde_json::json!({"payload_sha256":hash,"selection":selection,"report":report}))?;
                    writeln!(details)?;detail_count+=1;
                }
            }
            events.flush()?;payloads.flush()?;details.flush()?;
            evidence["detail_samples"]=serde_json::json!(detail_count);
            evidence["unique_payloads"]=serde_json::json!(seen.len());
        }
        let handoff=candidates.ensure_handoff(&reports);
        evidence["execution_handoff_observed"]=serde_json::json!(handoff.is_ok());
        let acceptance=handoff.and_then(|_|candidates.ensure_successful_simulation(&reports));
        evidence["execution_integration_passed"]=serde_json::json!(acceptance.is_ok());
        evidence["execution_integration_error"]=serde_json::json!(acceptance.err().map(|e|format!("{e:#}")));
        candidates.ensure_complete()?;
        Ok(candidates)
    }
}
