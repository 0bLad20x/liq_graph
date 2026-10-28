//! File source -> typed channel -> execution worker -> JSON report. No route decisions.
use std::{collections::HashMap,env,path::Path,time::Duration};
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
    executor:Task<Result<()>>,
    reports:Task<Vec<ExecutionReport>>,
}
impl ExecutionTasks {
    pub(super) fn start(settings:execution_v10::Settings,input:SignalStream,
        registry:pool_registry::PoolRegistry,rpc:crate::core::rpc::RpcHandle)->Result<Self> {
        let cfg=execution_candidates::Settings::from_env(settings.wallet(),execution_v10::validate)?;
        let (jobs,requests)=mpsc::channel(1);
        let (output,mut responses)=mpsc::channel(1);
        let reports=Task(tokio::spawn(async move {
            let mut received=Vec::new();
            while let Some(report)=responses.recv().await {received.push(report);}
            received
        }));
        Ok(Self {
            candidates:Task(tokio::spawn(execution_candidates::run(input,registry,jobs,cfg))),
            executor:Task(tokio::spawn(execution_v10::run(settings,rpc,requests,output))),reports,
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
        let candidates=tokio::time::timeout(Duration::from_secs(30),&mut self.candidates.0).await
            .context("candidate shutdown timeout")???;
        // The candidate stream has closed. Let accepted jobs and reports drain
        // while the shared RPC transport remains alive.
        evidence["execution_candidates"]=serde_json::to_value(&candidates)?;
        let executed=tokio::time::timeout(Duration::from_secs(300),&mut self.executor.0).await
            .context("V10 simulation shutdown timeout").and_then(|r|r.context("V10 worker panicked")).and_then(|r|r);
        let reports=tokio::time::timeout(Duration::from_secs(10),&mut self.reports.0).await
            .context("V10 report shutdown timeout")??;
        // Full reports already have individual artifacts; keep the owner summary bounded.
        evidence["execution_reports"]=serde_json::json!(reports.iter().map(|r|serde_json::json!({
            "id":r.id,"outcome":r.outcome,"phase":r.phase,"error":r.error,"executor_invoked":r.executor_invoked})).collect::<Vec<_>>());
        evidence["simulation_successes"]=serde_json::json!(reports.iter().filter(|r|matches!(r.outcome,
            ExecutionOutcome::SimulatedNoDexCallObserved|ExecutionOutcome::SimulatedDexCallsObserved)).count());
        evidence["simulation_failures"]=serde_json::json!(reports.iter().filter(|r|matches!(r.outcome,ExecutionOutcome::SimulationFailed)).count());
        evidence["simulation_dex_calls_observed"]=serde_json::json!(reports.iter().filter(|r|!r.dex_invocations.is_empty()).count());
        // All artifacts share the existing run directory; no additional code hierarchy.
        if let Ok(path)=env::var("GRAPH_EXECUTION_REPORT_PATH") {
            let folder=std::path::Path::new(&path).parent().context("report directory missing")?;
            let by_id:HashMap<_,_>=reports.iter().map(|r|(r.id.as_str(),r)).collect();
            for (i,selection) in candidates.requests.iter().enumerate() {
                std::fs::write(folder.join(format!("request-{:02}.json",i+1)),serde_json::to_vec_pretty(&selection.request)?)?;
                std::fs::write(folder.join(format!("selection-{:02}.json",i+1)),serde_json::to_vec_pretty(selection)?)?;
                if let Some(report)=by_id.get(selection.request.id.as_str()) {
                    std::fs::write(folder.join(format!("simulation-{:02}.json",i+1)),serde_json::to_vec_pretty(report)?)?;
                }
            }
        }
        executed?;
        let comparisons=candidates.compare_simulations(&reports);
        if let Ok(path)=env::var("GRAPH_EXECUTION_REPORT_PATH") {
            let folder=std::path::Path::new(&path).parent().context("report directory missing")?;
            std::fs::write(folder.join("economic_comparison.json"),serde_json::to_vec_pretty(&comparisons)?)?;
            std::fs::write(folder.join("economic_models.json"),serde_json::to_vec_pretty(candidates.economic_samples())?)?;
        }
        evidence["economic_positive_after_network"]=serde_json::json!(comparisons.iter()
            .filter(|c|c["positive_after_network_without_other_token_inventory"]==true).count());
        evidence["report_count"]=serde_json::json!(reports.len());
        let handoff=candidates.ensure_handoff(&reports);
        evidence["execution_delivery_passed"]=serde_json::json!(handoff.is_ok());
        evidence["request_count"]=serde_json::json!(candidates.request_count());
        evidence["execution_handoff_observed"]=serde_json::json!(handoff.is_ok()&&candidates.request_count()>0);
        let acceptance=handoff.and_then(|_|candidates.ensure_successful_simulation(&reports));
        evidence["execution_integration_passed"]=serde_json::json!(acceptance.is_ok());
        evidence["execution_integration_error"]=serde_json::json!(acceptance.err().map(|e|format!("{e:#}")));
        candidates.ensure_complete()?;
        Ok(candidates)
    }
}
