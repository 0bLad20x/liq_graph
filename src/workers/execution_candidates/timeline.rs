//! CP21S-A.1: durable, bounded-memory Direct2 state and decision journal.
//! The economic hot path never performs filesystem IO. The dedicated writer
//! has a finite channel; overflow is explicitly counted and fails run evidence,
//! never silently discarded or used to throttle economic search.
use anyhow::{Context,Result,ensure};
use serde_json::Value;
use std::{fs::{File,create_dir_all},io::{BufWriter,Write},path::PathBuf,
    sync::mpsc::{self,SyncSender,TrySendError},thread::{self,JoinHandle}};
use super::Report;

pub(super) struct Timeline{
    sender:SyncSender<Value>,
    worker:JoinHandle<Result<Summary>>,
}
#[derive(Clone,Copy,Debug,Default)]
pub(super) struct Summary{pub written:u64,pub bytes:u64,pub durable_syncs:u64}
pub(super) fn open()->Result<Option<Timeline>>{
    let path=match std::env::var("GRAPH_CP21S_TIMELINE_PATH"){
        Ok(path) if !path.trim().is_empty()=>PathBuf::from(path),
        Ok(_)|Err(std::env::VarError::NotPresent)=>return Ok(None),
        Err(e)=>return Err(e.into()),
    };
    // This bounds only the audit IO backlog, NEVER the economic candidate set.
    // Overflow fails evidence completeness, and is not a discovery exclusion.
    let (sender,rx)=mpsc::sync_channel::<Value>(32_768);
    let worker=thread::Builder::new().name("cp21s-timeline-writer".into())
        .spawn(move||->Result<Summary>{
            if let Some(folder)=path.parent(){create_dir_all(folder)?;}
            let file=File::create(&path)
                .with_context(||format!("create timeline journal {}",path.display()))?;
            let mut output=BufWriter::with_capacity(1024*1024,file);
            let mut summary=Summary::default();
            for record in rx{
                let data=serde_json::to_vec(&record)?;
                output.write_all(&data)?;
                output.write_all(b"\n")?;
                summary.written+=1;
                summary.bytes+=data.len() as u64+1;
                if summary.written%1_024==0{output.flush()?;}
                if summary.written%16_384==0{
                    output.get_ref().sync_data()?;
                    summary.durable_syncs+=1;
                }
            }
            output.flush()?;
            output.get_ref().sync_data()?;
            summary.durable_syncs+=1;
            Ok(summary)
        }).context("spawn CP21S-A.1 timeline writer")?;
    Ok(Some(Timeline{sender,worker}))
}
impl Timeline{
    pub(super) fn sender(&self)->SyncSender<Value>{self.sender.clone()}
    pub(super) fn finish(self,report:&mut Report)->Result<()>{
        // Caller MUST drop all indexed clones of sender first.
        drop(self.sender);
        let summary=self.worker.join()
            .map_err(|_|anyhow::anyhow!("timeline journal writer panicked"))??;
        report.cp21a1_timeline_written=summary.written;
        report.cp21a1_timeline_bytes=summary.bytes;
        report.cp21a1_timeline_syncs=summary.durable_syncs;
        ensure!(report.cp21a1_timeline_lost==0
            && report.cp21a1_timeline_emitted==summary.written,
            "timeline evidence incomplete: sent={} durable={} lost={}",
            report.cp21a1_timeline_emitted,summary.written,report.cp21a1_timeline_lost);
        Ok(())
    }
}
pub(super) fn emit(sender:Option<&SyncSender<Value>>,record:Value,report:&mut Report){
    let Some(sender)=sender else{return;};
    match sender.try_send(record){
        Ok(())=>report.cp21a1_timeline_emitted+=1,
        Err(TrySendError::Full(_)|TrySendError::Disconnected(_))=>
            report.cp21a1_timeline_lost+=1,
    }
}
