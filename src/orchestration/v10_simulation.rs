//! File source -> typed channel -> execution worker -> JSON report. No route decisions.
use std::{env,path::Path,time::Duration,io::{BufWriter,Write},fs::OpenOptions};
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

mod evidence;

// Keep accepted execution jobs owned through shutdown; cancellation aborts their tasks.
struct Task<T>(JoinHandle<T>);
impl<T> Drop for Task<T> {fn drop(&mut self){self.0.abort();}}

#[derive(Default)]
struct ReportCounts {count:u64,successes:u64,failures:u64,dex_calls:u64}

pub(super) struct ExecutionTasks {
    candidates:Task<Result<execution_candidates::Report>>,
    executor:Task<Result<execution_v10::ParallelReport>>,
    reports:Task<Result<ReportCounts>>,
    writer:Task<Result<evidence::Summary>>,
}
impl ExecutionTasks {
    pub(super) fn start(settings:execution_v10::Settings,input:SignalStream,
        registry:pool_registry::PoolRegistry,rpc:crate::core::rpc::RpcHandle)->Result<Self> {
        let cfg=execution_candidates::Settings::from_env(settings.wallet(),execution_v10::validate)?;
        let parallelism:usize=env::var("GRAPH_V10_SIMULATION_PARALLELISM")
            .unwrap_or_else(|_|"1".into()).parse().context("invalid GRAPH_V10_SIMULATION_PARALLELISM")?;
        ensure!((1..=64).contains(&parallelism),"GRAPH_V10_SIMULATION_PARALLELISM must be 1..64");
        let folder=std::path::PathBuf::from(env::var("GRAPH_EXECUTION_REPORT_PATH")
            .context("streaming V10 requires GRAPH_EXECUTION_REPORT_PATH")?);
        let folder=folder.parent().context("V10 report directory missing")?.to_path_buf();
        let detail_limit:usize=env::var("RUN_EXECUTION_DETAIL_LIMIT")
            .unwrap_or_else(|_|"16".into()).parse().context("invalid RUN_EXECUTION_DETAIL_LIMIT")?;
        let (jobs,requests)=mpsc::channel(parallelism);
        let (output,mut responses)=mpsc::channel(parallelism*2);
        // The entire evidence pipeline has bounded memory, with one writer
        // running on a blocking thread. No file or JSON work in the candidate
        // or WS/MarketState tasks; never await evidence channel capacity.
        let (evidence_tx,evidence_rx)=mpsc::channel(1024);
        let writer=Task(tokio::task::spawn_blocking(move||
            evidence::run(evidence_rx,folder,detail_limit)));
        let response_evidence=evidence_tx.clone();
        let reports=Task(tokio::spawn(async move {
            let mut counts=ReportCounts::default();
            while let Some(report)=responses.recv().await {
                counts.count+=1;
                counts.successes+=u64::from(matches!(report.outcome,
                    ExecutionOutcome::SimulatedNoDexCallObserved|ExecutionOutcome::SimulatedDexCallsObserved));
                counts.failures+=u64::from(matches!(report.outcome,ExecutionOutcome::SimulationFailed));
                counts.dex_calls+=u64::from(!report.dex_invocations.is_empty());
                response_evidence.try_send(execution_candidates::EvidenceItem::Report(Box::new(report)))
                    .map_err(|err|anyhow::anyhow!("V10 report evidence queue unavailable (nonblocking): {err}"))?;
            }
            Ok(counts)
        }));
        Ok(Self {
            candidates:Task(tokio::spawn(execution_candidates::run(input,registry,jobs,evidence_tx,cfg))),
            executor:Task(tokio::spawn(execution_v10::run_parallel(settings,rpc,requests,output,parallelism))),
            reports,writer,
        })
    }
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
                    Ok(Ok(report))=>bail!("V10 candidate worker exited before runtime shutdown: requests={} error={:?}",report.request_count,report.error),
                    Ok(Err(error))=>Err(error).context("V10 candidate worker exited before runtime shutdown"),
                    Err(error)=>Err(error).context("V10 candidate worker task failed before shutdown"),
                }
            },
            result=&mut self.executor.0=>{
                match result {
                    Ok(Ok(_stats))=>bail!("V10 executor exited before runtime shutdown"),
                    Ok(Err(error))=>Err(error).context("V10 executor exited before runtime shutdown"),
                    Err(error)=>Err(error).context("V10 executor task failed before shutdown"),
                }
            },
            result=&mut self.reports.0=>{
                match result {
                    Ok(Ok(counts))=>bail!("V10 report collector exited before runtime shutdown after {} reports",counts.count),
                    Ok(Err(error))=>Err(error).context("V10 report evidence path failed before shutdown"),
                    Err(error)=>Err(error).context("V10 report collector task failed before shutdown"),
                }
            },
            result=&mut self.writer.0=>{
                match result {
                    Ok(Ok(summary))=>bail!("V10 file writer exited before runtime shutdown after {} records",summary.matched),
                    Ok(Err(error))=>Err(error).context("V10 file writer failed before shutdown"),
                    Err(error)=>Err(error).context("V10 file writer task failed before shutdown"),
                }
            }
        }
    }
    pub(super) async fn finish(&mut self,evidence:&mut serde_json::Value)->Result<execution_candidates::Report> {
        let mut candidates=tokio::time::timeout(Duration::from_secs(30),&mut self.candidates.0).await
            .context("candidate shutdown timeout")???;
        let exclusions=candidates.take_exclusion_samples();
        evidence["execution_candidates"]=serde_json::to_value(&candidates)?;
        let executed=tokio::time::timeout(Duration::from_secs(300),&mut self.executor.0).await
            .context("V10 simulation shutdown timeout").and_then(|r|r.context("V10 worker panicked")).and_then(|r|r)?;
        let received=tokio::time::timeout(Duration::from_secs(30),&mut self.reports.0).await
            .context("V10 report collector shutdown timeout")???;
        let streamed=tokio::time::timeout(Duration::from_secs(60),&mut self.writer.0).await
            .context("V10 evidence writer shutdown timeout")???;
        ensure!(executed.completed_requests==received.count,
            "parallel V10 executor completion/report count mismatch");
        ensure!(candidates.request_count==received.count&&streamed.matched==received.count,
            "V10 evidence count mismatch: selected={} simulated={} streamed={}",
            candidates.request_count,received.count,streamed.matched);
        candidates.ensure_complete()?;
        if let Ok(path)=env::var("GRAPH_EXECUTION_REPORT_PATH") {
            if !exclusions.is_empty() {
                let path=std::path::Path::new(&path).parent()
                    .context("V10 report directory missing")?.join("execution_details.jsonl");
                let mut details=BufWriter::new(OpenOptions::new().append(true).open(&path)?);
                for exclusion in exclusions.iter().take(detail_limit_from_env()?) {
                    serde_json::to_writer(&mut details,&serde_json::json!({"quantity_exclusion":exclusion}))?;
                    writeln!(details)?;
                }
                details.flush()?;
            }
        }
        evidence["v10_parallelism"]=serde_json::to_value(&executed)?;
        evidence["schema_version"]=serde_json::json!(4);
        evidence["request_count"]=serde_json::json!(candidates.request_count);
        evidence["report_count"]=serde_json::json!(received.count);
        evidence["simulation_successes"]=serde_json::json!(received.successes);
        evidence["simulation_failures"]=serde_json::json!(received.failures);
        evidence["simulation_dex_calls_observed"]=serde_json::json!(received.dex_calls);
        evidence["streaming_evidence"]=serde_json::to_value(&streamed)?;
        evidence["detail_samples"]=serde_json::json!(streamed.detail_samples);
        evidence["unique_payloads"]=serde_json::json!(streamed.unique_payloads);
        evidence["execution_handoff_observed"]=serde_json::json!(candidates.request_count>0);
        evidence["execution_integration_passed"]=serde_json::json!(received.successes>0);
        evidence["execution_integration_error"]=serde_json::json!(
            if received.successes==0{Some("no successful V10 simulation observed")}else{None});
        Ok(candidates)
    }
}
fn detail_limit_from_env()->Result<usize>{
    Ok(env::var("RUN_EXECUTION_DETAIL_LIMIT").unwrap_or_else(|_|"16".into()).parse()?)
}
