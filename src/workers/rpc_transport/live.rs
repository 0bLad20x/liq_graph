//! Finite real-account audit. Input planning finishes before subscriptions and measured reception.
use std::{collections::{BTreeMap,HashMap,HashSet},env,sync::Arc,time::Duration};
use anyhow::{Result,Context,ensure};
use base64::{engine::general_purpose::STANDARD,Engine as _};
use serde::Deserialize;
use serde_json::{json,Value};
use solana_pubkey::Pubkey;
use tokio::{sync::broadcast,time::{self,Instant}};
use crate::core::account_stream::{AccountEvent,AccountView,AccountWatch};
use super::{RpcHandle,ws::{self,Latency}};

#[derive(Deserialize,serde::Serialize)]
#[serde(deny_unknown_fields)]
struct WatchInput {address:String,offset:Option<usize>,length:Option<usize>}
#[derive(serde::Serialize)]
struct Phase {
    name:&'static str,started_at_us:u64,ended_at_us:u64,requested_accounts:usize,active_at_end:usize,
    all_acked:bool,all_acked_at_us:Option<u64>,account_updates:u64,continuing_account_updates:u64,
    updates_before_transition_complete:u64,continuing_updates_during_transition:u64,transition_seconds:f64,steady_seconds:f64,
    #[serde(skip)] transition:Traffic,
    #[serde(skip)] steady:Traffic,
}
impl Phase {
    fn report(&self)->Value {
        let mut result=serde_json::to_value(self).expect("phase serialization");
        result["transition"]=self.transition.report(self.transition_seconds);
        result["steady"]=self.steady.report(self.steady_seconds);
        result
    }
}
#[derive(Default)]
struct Window {bucket:u128,updates:u64,bytes:u64,peak_updates:u64,peak_bytes:u64}
impl Window {
    fn add(&mut self,bucket:u128,bytes:u64) {
        if bucket!=self.bucket {self.bucket=bucket;self.updates=0;self.bytes=0;}
        self.updates+=1;self.bytes+=bytes;
        self.peak_updates=self.peak_updates.max(self.updates);self.peak_bytes=self.peak_bytes.max(self.bytes);
    }
}
struct Traffic {
    started:Instant,updates:u64,bytes:u64,ten_ms:Window,second:Window,
    frame:Latency,delivery:Latency,accounts:HashSet<Pubkey>,
}
impl Traffic {
    fn new()->Self {Self{started:Instant::now(),updates:0,bytes:0,ten_ms:Window::default(),second:Window::default(),frame:Latency::default(),delivery:Latency::default(),accounts:HashSet::new()}}
    fn add(&mut self,now:Instant,address:Pubkey,bytes:u64,frame:Option<u64>,delivery:Option<u64>) {
        let elapsed=now.duration_since(self.started).as_micros();
        self.updates+=1;self.bytes+=bytes;self.accounts.insert(address);self.ten_ms.add(elapsed/10_000,bytes);self.second.add(elapsed/1_000_000,bytes);
        if let Some(n)=frame {self.frame.add(n);}
        if let Some(n)=delivery {self.delivery.add(n);}
    }
    fn report(&self,seconds:f64)->Value {
        json!({"account_updates":self.updates,"updated_accounts":self.accounts.len(),"decoded_account_bytes":self.bytes,"seconds":seconds,
            "updates_per_second":(seconds>0.0).then(||self.updates as f64/seconds),
            "decoded_bytes_per_second":(seconds>0.0).then(||self.bytes as f64/seconds),
            "peak_updates_in_10ms":self.ten_ms.peak_updates,"peak_bytes_in_10ms":self.ten_ms.peak_bytes,
            "peak_updates_in_1s":self.second.peak_updates,"peak_bytes_in_1s":self.second.peak_bytes,
            "frame_to_consumer":self.frame.report(),"decode_to_consumer":self.delivery.report()})
    }
}
struct Audit {
    active:HashMap<Pubkey,String>,sources:BTreeMap<String,(u64,u64,u64)>,
    accounts:u64,subscribe_acks:u64,unsubscribe_acks:u64,gaps:u64,rejections:u64,lost:u64,
    sequence_errors:u64,clock_errors:u64,deleted:u64,bytes:u64,max_queue_depth:usize,
    transition:Latency,steady:Latency,
    decode_to_consumer:Latency,frame_to_consumer:Latency,samples:Vec<Value>,
}
impl Audit {
    fn new()->Self {Self{active:HashMap::new(),sources:BTreeMap::new(),accounts:0,
        subscribe_acks:0,unsubscribe_acks:0,gaps:0,rejections:0,lost:0,sequence_errors:0,clock_errors:0,
        deleted:0,bytes:0,max_queue_depth:0,transition:Latency::default(),steady:Latency::default(),decode_to_consumer:Latency::default(),frame_to_consumer:Latency::default(),samples:Vec::new()}}
    fn receive(&mut self,event:std::result::Result<AccountEvent,broadcast::error::RecvError>,phase:&mut Phase,continuing:&HashSet<Pubkey>)->Result<()> {
        match event {
            Ok(AccountEvent::Subscribed{provider,address})=>{self.active.insert(address,provider);self.subscribe_acks+=1;},
            Ok(AccountEvent::Unsubscribed{provider,address})=>{
                if self.active.get(&address)==Some(&provider){self.active.remove(&address);}
                self.unsubscribe_acks+=1;
            },
            Ok(AccountEvent::Gap{provider,reason})=>{
                self.gaps+=1;self.active.retain(|_,p|p!=&provider);
                crate::log_event!(Warn,"account stream gap source={provider} reason={reason}");
            },
            Ok(AccountEvent::Rejected{provider,address,code})=>{
                self.rejections+=1;crate::log_event!(Warn,"account stream rejected source={provider} address={address} code={code}");
            },
            Ok(AccountEvent::Account{provider,address,slot,session,sequence,frame_received_at_us,decoded_at_us,value,..})=>{
                let received=ws::now_us();let now=Instant::now();
                let delivery=received.checked_sub(decoded_at_us).map(|n|n.saturating_mul(1000));
                let frame=received.checked_sub(frame_received_at_us).map(|n|n.saturating_mul(1000));
                if let Some(ns)=delivery {self.decode_to_consumer.add(ns);}else{self.clock_errors+=1;}
                if let Some(ns)=frame {
                    self.frame_to_consumer.add(ns);
                    if phase.all_acked {self.steady.add(ns);}else{self.transition.add(ns);}
                }else{self.clock_errors+=1;}
                let bytes=value.as_ref().map_or(0,|v|v.bytes.len() as u64);
                if phase.all_acked {phase.steady.add(now,address,bytes,frame,delivery);}else{phase.transition.add(now,address,bytes,frame,delivery);}
                if let Some((count,old,last))=self.sources.get_mut(&provider) {
                    let expected=if *old==session{*last+1}else{1};
                    if sequence!=expected {self.sequence_errors+=1;}
                    *count+=1;*old=session;*last=sequence;
                }else {
                    if sequence!=1 {self.sequence_errors+=1;}
                    self.sources.insert(provider.clone(),(1,session,sequence));
                }
                self.accounts+=1;phase.account_updates+=1;
                if continuing.contains(&address){
                    phase.continuing_account_updates+=1;
                    if !phase.all_acked {phase.continuing_updates_during_transition+=1;}
                }
                if !phase.all_acked {phase.updates_before_transition_complete+=1;}
                if let Some(data)=value.as_ref(){ensure!(data.address==address,"account envelope/payload identity mismatch");self.bytes+=data.bytes.len() as u64;}else{self.deleted+=1;}
                if self.samples.len()<16 {
                    self.samples.push(json!({"source":provider,"address":address.to_string(),"slot":slot,"session":session,"sequence":sequence,
                        "owner":value.as_ref().map(|v|v.owner.to_string()),"view":value.as_ref().map(|v|format!("{:?}",v.view)),
                        "data_len":value.as_ref().map(|v|v.bytes.len()),"original_len":value.as_ref().and_then(|v|v.original_len),"executable":value.as_ref().map(|v|v.executable),"data_prefix_base64":value.as_ref().map(|v|STANDARD.encode(&v.bytes[..v.bytes.len().min(32)])),
                        "shared_payload_references":value.as_ref().map(Arc::strong_count),
                        "frame_received_at_us":frame_received_at_us,"decoded_at_us":decoded_at_us,"consumer_received_at_us":received}));
                }
            },
            Err(broadcast::error::RecvError::Lagged(n))=>self.lost+=n,
            Err(broadcast::error::RecvError::Closed)=>anyhow::bail!("account event stream closed"),
        }
        Ok(())
    }
}

pub async fn account_stream_live(handle:&RpcHandle,planned:Option<Vec<AccountWatch>>)->Result<()> {
    let mut report=json!({"run_complete":false,"live_evidence_passed":false,"execution":false,"error":null});
    let result=run(handle,planned,&mut report).await;
    if let Err(error)=&result {report["error"]=json!(format!("{error:#}"));}
    if let Ok(path)=env::var("RPC_ACCOUNT_STREAM_REPORT_PATH") {
        std::fs::write(path,serde_json::to_vec_pretty(&report)?).context("write account stream evidence")?;
    }
    println!("{}",serde_json::to_string(&report)?);
    result
}

async fn run(handle:&RpcHandle,planned:Option<Vec<AccountWatch>>,report:&mut Value)->Result<()> {
    let path=env::var("RPC_ACCOUNT_STREAM_ACCOUNTS_PATH").context("RPC_ACCOUNT_STREAM_ACCOUNTS_PATH is required")?;
    let input_mode=if planned.is_some(){"prepared_plan"}else{"account_file"};
    let watches=if let Some(watches)=planned {
        let rows:Vec<_>=watches.iter().map(|w|{
            let (offset,length)=match w.view {AccountView::Full=>(None,None),AccountView::Slice{offset,length}=>(Some(offset),Some(length))};
            WatchInput{address:w.address.to_string(),offset,length}
        }).collect();
        std::fs::write(&path,serde_json::to_vec_pretty(&rows)?).context("write planned account watches")?;
        watches
    }else {
        let inputs:Vec<WatchInput>=serde_json::from_slice(&std::fs::read(&path).context("read account watch file")?).context("parse account watch file")?;
        inputs.into_iter().map(|w|->Result<_>{
            let view=match (w.offset,w.length){(None,None)=>AccountView::Full,(Some(offset),Some(length))=>AccountView::Slice{offset,length},_=>anyhow::bail!("offset and length must be provided together")};
            Ok(AccountWatch{address:w.address.parse().context("invalid watched account address")?,view})
        }).collect::<Result<Vec<_>>>()?
    };
    let duration:u64=env::var("RPC_ACCOUNT_STREAM_DURATION_SECONDS").unwrap_or_else(|_|"300".into()).parse()?;
    ensure!((60..=86400).contains(&duration),"account stream duration must be 60..86400 seconds");
    ensure!(watches.len()>=4,"at least four real accounts are required for add/remove/restore evidence");
    let (control,mut input)=handle.account_stream();ensure!(watches.len()<=control.capacity(),"watch file exceeds selected account capacity");
    ensure!(watches.iter().map(|w|w.address).collect::<HashSet<_>>().len()==watches.len(),"duplicate watched account");
    let transition_timeout:u64=env::var("RPC_ACCOUNT_STREAM_TRANSITION_TIMEOUT_SECONDS").unwrap_or_else(|_|"600".into()).parse()?;
    ensure!((1..=86400).contains(&transition_timeout),"transition timeout must be 1..86400 seconds");
    let half=watches.len()/2;let started=Instant::now();let mut audit=Audit::new();let mut phases=Vec::new();
    let mut previous=HashSet::new();let mut failure=None;
    let mut ticker=time::interval(Duration::from_secs(5));ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let work:Result<()>=async {
        for (i,name) in ["initial","add","remove","restore","drain"].into_iter().enumerate() {
            let desired=match i {0|2=>&watches[..half],1|3=>watches.as_slice(),_=>&[]};
            let desired_set:HashSet<_>=desired.iter().map(|w|w.address).collect();
            let continuing=previous.intersection(&desired_set).copied().collect();
            control.replace(desired.to_vec())?;
            let mut phase=Phase{name,started_at_us:ws::now_us(),ended_at_us:0,requested_accounts:desired.len(),
                active_at_end:0,all_acked:false,all_acked_at_us:None,account_updates:0,
                continuing_account_updates:0,updates_before_transition_complete:0,
                continuing_updates_during_transition:0,transition_seconds:0.0,steady_seconds:0.0,transition:Traffic::new(),steady:Traffic::new()};
            crate::log_event!(Info,"account stream phase={name} desired={} accounts_received={} lost={}",desired.len(),audit.accounts,audit.lost);
            let transition_end=Instant::now()+Duration::from_secs(transition_timeout);
            let steady_duration=if i<4 {Duration::from_secs(duration*(i as u64+1)/4-duration*i as u64/4)}else{Duration::from_millis(1500)};
            let deadline=time::sleep_until(transition_end);tokio::pin!(deadline);
            let phase_result:Result<()>=async {
                loop {
                    if !phase.all_acked && audit.active.len()==desired_set.len() && audit.active.keys().all(|a|desired_set.contains(a)) {
                        phase.all_acked=true;phase.all_acked_at_us=Some(ws::now_us());
                        phase.transition_seconds=phase.transition.started.elapsed().as_secs_f64();
                        phase.steady.started=Instant::now();deadline.as_mut().reset(phase.steady.started+steady_duration);
                        crate::log_event!(Info,"account stream acknowledged phase={name} accounts={} transition_seconds={:.3} measurement_seconds={:.3}",desired.len(),phase.transition_seconds,steady_duration.as_secs_f64());
                    }
                    tokio::select! {
                        _=&mut deadline=>{ensure!(phase.all_acked,"subscription transition timed out in phase {name}: active={} expected={}",audit.active.len(),desired.len());break;},
                        event=input.recv()=>{audit.max_queue_depth=audit.max_queue_depth.max(input.len());audit.receive(event,&mut phase,&continuing)?;},
                        _=ticker.tick()=>crate::log_event!(Info,"account stream progress phase={name} active={} received={} lost={} gaps={}",audit.active.len(),audit.accounts,audit.lost,audit.gaps),
                    }
                }
                Ok(())
            }.await;
            phase.ended_at_us=ws::now_us();phase.active_at_end=audit.active.len();
            if phase.all_acked {phase.steady_seconds=phase.steady.started.elapsed().as_secs_f64();}
            else {phase.transition_seconds=phase.transition.started.elapsed().as_secs_f64();}
            phases.push(phase);previous=desired_set;
            phase_result?;
        }
        Ok(())
    }.await;
    if let Err(error)=work {failure=Some(error);}
    let cleanup=control.replace(Vec::new());
    if failure.is_none(){failure=cleanup.err();}
    let producer=handle.stream_report();
    let sources=producer["sources"].as_array().context("missing account producer reports")?;
    let published:u64=sources.iter().filter_map(|s|s["account_published"].as_u64()).sum();
    let (text_messages,text_bytes)=control.stream_counts();
    let source_counts:BTreeMap<_,_>=audit.sources.iter().map(|(source,(count,_,_))|(source,*count)).collect();
    *report=json!({"run_complete":failure.is_none(),"live_evidence_passed":false,"execution":false,"duration_seconds":duration,
        "elapsed_seconds":started.elapsed().as_secs_f64(),"input_mode":input_mode,"transition_timeout_seconds":transition_timeout,"requested_accounts":watches.len(),"commitment":env::var("RPC_COMMITMENT").unwrap_or_else(|_|"confirmed".into()),"producer":producer,
        "transport_text_messages":text_messages,"transport_text_bytes":text_bytes,
        "consumer":{"account_updates":audit.accounts,"by_source":source_counts,"subscribe_acks":audit.subscribe_acks,
            "unsubscribe_acks":audit.unsubscribe_acks,"gaps":audit.gaps,"rejections":audit.rejections,"lost_events":audit.lost,
            "max_queue_depth":audit.max_queue_depth,"sequence_errors":audit.sequence_errors,"clock_errors":audit.clock_errors,"deleted_accounts":audit.deleted,"data_bytes":audit.bytes,
            "decode_to_consumer":audit.decode_to_consumer.report(),"frame_to_consumer":audit.frame_to_consumer.report(),"transition_frame_to_consumer":audit.transition.report(),"steady_frame_to_consumer":audit.steady.report()},"phases":phases.iter().map(Phase::report).collect::<Vec<_>>(),"samples":audit.samples});
    // Control-change activity is additional evidence; a quiet transition is not proof of a stall.
    report["transition_activity_observed"]=json!(phases.len()==5 && phases[1..4].iter().all(|p|p.continuing_updates_during_transition>0));
    if let Some(error)=failure{return Err(error);}
    ensure!(audit.gaps==0 && audit.rejections==0 && audit.lost==0 && audit.sequence_errors==0 && audit.clock_errors==0,"account stream continuity failed; inspect report counters");
    ensure!(audit.accounts>0 && published==audit.accounts,"producer/consumer account totals differ or no real account updates: published={published} received={}",audit.accounts);
    ensure!(report["producer"]["sources"].as_array().is_some_and(|v|!v.is_empty() && v.iter().all(|s|s["account_published"].as_u64().unwrap_or(0)>0 && s["decode_errors"]==0 && s["no_receiver"]==0 && s["gaps"]==0 && s["rejections"]==0
            && s["account_notifications"].as_u64()==Some(s["account_published"].as_u64().unwrap_or(0)+s["retired_notifications"].as_u64().unwrap_or(0)))),"every selected socket must publish real account updates");
    ensure!(phases.len()==5 && phases.iter().all(|p|p.all_acked && p.active_at_end==p.requested_accounts)
        && phases[..4].iter().all(|p|p.steady.updates>0)
        && phases[1..4].iter().all(|p|p.continuing_account_updates>0),
        "add/remove/restore did not establish subscriptions and ongoing real updates in every phase");
    report["live_evidence_passed"]=json!(true);
    Ok(())
}
