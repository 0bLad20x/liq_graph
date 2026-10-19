//! File source -> typed channel -> execution worker -> JSON report. No route decisions.
use std::path::Path;
use anyhow::{Context, Result, ensure};
use tokio::sync::mpsc;
use crate::{core::execution::{ExecutionOutcome, ExecutionRequest},
    workers::{execution_v10, rpc_transport}};

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
