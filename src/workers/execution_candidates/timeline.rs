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
    Ok(Some(Timeline::at_path(path)?))
}
impl Timeline{
    fn at_path(path:PathBuf)->Result<Self>{
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
    Ok(Timeline{sender,worker})
    }
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
/// Compact journal is the default. Full replay is an explicit opt-in; focus
/// pools retain per-pair detail only when one leg matches a selected pool.
/// This changes audit storage, never the economic index or V10 decisions.
#[derive(Default)]
struct Policy{
    full:bool,
    focus:std::collections::HashSet<String>,
}
fn policy()->&'static Policy{
    static POLICY:std::sync::OnceLock<Policy>=std::sync::OnceLock::new();
    POLICY.get_or_init(||Policy{
        full:std::env::var("GRAPH_CP21S_TIMELINE_MODE")
            .is_ok_and(|v|v.eq_ignore_ascii_case("full")),
        focus:std::env::var("GRAPH_CP21S_TIMELINE_FOCUS_POOLS")
            .unwrap_or_default().split(',')
            .map(str::trim).filter(|s|!s.is_empty())
            .map(str::to_owned).collect(),
    })
}
fn policy_check<T:ToString>(
    p:&Policy,action:&str,direct:Option<&T>,partner:Option<&T>)->bool{
    // Never suppress economically actionable transitions, admissions,
    // dispatch failures, queue wait or other exceptional classifications.
    if !matches!(action,"unpriced"|"nonpositive"|"retained"){return true;}
    if p.full{return true;}
    if p.focus.is_empty(){return false;}
    direct.is_some_and(|x|p.focus.contains(&x.to_string()))||
        partner.is_some_and(|x|p.focus.contains(&x.to_string()))
}
pub(super) fn economic_detail_for<T:ToString>(
    action:&str,direct:Option<&T>,partner:Option<&T>)->bool{
    policy_check(policy(),action,direct,partner)
}

pub(super) fn emit(sender:Option<&SyncSender<Value>>,record:Value,report:&mut Report){
    let Some(sender)=sender else{return;};
    match sender.try_send(record){
        Ok(())=>report.cp21a1_timeline_emitted+=1,
        Err(TrySendError::Full(_)|TrySendError::Disconnected(_))=>
            report.cp21a1_timeline_lost+=1,
    }
}

#[cfg(test)]
mod tests{
    use super::*;
    use serde_json::json;
    #[test]
    fn cp21a2_compact_keeps_all_dispatch_decisions_and_focused_pool_details(){
        let compact=Policy::default();
        for action in ["unpriced","nonpositive","retained"]{
            assert!(!policy_check(&compact,action,Some(&"POOL_A"),Some(&"POOL_B")));
        }
        for action in ["queued","v10_submitted","model_unknown","waiting",
                       "retained_ready","not_dispatched","baseline"]{
            assert!(policy_check(&compact,action,Some(&"POOL_A"),Some(&"POOL_B")));
        }
        let focused=Policy{focus:["POOL_B".to_string()].into(),..Default::default()};
        assert!(policy_check(&focused,"unpriced",Some(&"POOL_A"),Some(&"POOL_B")));
        assert!(!policy_check(&focused,"unpriced",Some(&"POOL_A"),Some(&"POOL_C")));
        let full=Policy{full:true,..Default::default()};
        assert!(policy_check(&full,"unpriced",Some(&"POOL_A"),Some(&"POOL_C")));
    }
    #[test]
    fn cp21a1_timeline_is_durable_complete_and_order_preserving(){
        let path=std::env::temp_dir().join(format!(
            "cp21a1-timeline-{}-{:?}.jsonl",std::process::id(),std::thread::current().id()));
        let writer=Timeline::at_path(path.clone()).unwrap();
        let sender=writer.sender();
        let mut report=Report::default();
        for i in 0..10_000_u64 {
            emit(Some(&sender),json!({"schema":1,"kind":"economic_pair","at_us":i,
                "action":"evaluated","direct_pool":"D","partner_pool":"P",
                "direct_slot":5,"partner_slot":5}),&mut report);
        }
        drop(sender);
        writer.finish(&mut report).unwrap();
        assert_eq!(report.cp21a1_timeline_emitted,10_000);
        assert_eq!(report.cp21a1_timeline_written,10_000);
        assert_eq!(report.cp21a1_timeline_lost,0);
        let content=std::fs::read_to_string(&path).unwrap();
        assert_eq!(content.lines().count(),10_000);
        let first:Value=serde_json::from_str(content.lines().next().unwrap()).unwrap();
        let last:Value=serde_json::from_str(content.lines().last().unwrap()).unwrap();
        assert_eq!(first["at_us"],0);
        assert_eq!(last["at_us"],9_999);
        std::fs::remove_file(&path).unwrap();
    }
}
