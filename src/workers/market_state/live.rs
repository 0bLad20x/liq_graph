//! Finite audit of the production decoder. No graph, signals, execution or live admission.
use super::*;
use serde_json::{json,Value};

#[derive(Clone)]
struct Phase {index:usize,retained:Arc<HashSet<String>>}
#[derive(Clone,Copy,Default)]
struct Progress {selected:usize,ready:usize,accounts:usize,active:usize}
#[derive(Default)]
struct Measurements {
    prices:u64,retained:u64,replayed:u64,clock_errors:u64,
    decode:Latency,compute:Latency,market:Latency,delivery:Latency,frame:Latency,queue:Latency,
}
impl Measurements {
    fn add(&mut self,p:&PoolPriceObservation,received:u64,retained:bool) {
        self.prices+=1;if retained{self.retained+=1;}
        let Some(t)=p.pipeline else{return;};
        if t.initialization_replay{self.replayed+=1;return;}
        self.decode.add(t.market_decode_ns);self.compute.add(t.market_compute_ns);
        for (start,end,meter) in [
            (t.market_received_at_us,t.price_published_at_us,&mut self.market),
            (t.price_published_at_us,received,&mut self.delivery),
            (t.frame_received_at_us,received,&mut self.frame),
            (t.transport_decoded_at_us,t.market_received_at_us,&mut self.queue),
        ] {if let Some(us)=end.checked_sub(start){meter.add(us.saturating_mul(1000));}else{self.clock_errors+=1;}}
    }
    fn report(&self)->Value {json!({"websocket_prices":self.prices,"retained_pool_prices":self.retained,
        "initialization_replays":self.replayed,"clock_errors":self.clock_errors,
        "protocol_decode":self.decode.report(),"price_compute":self.compute.report(),
        "market_to_publish":self.market.report(),"publish_to_consumer":self.delivery.report(),
        "frame_to_consumer":self.frame.report(),"transport_to_market":self.queue.report()})}
}

async fn observe(mut input:PriceEventStream,phase:watch::Receiver<Phase>,progress:watch::Sender<Progress>)->Result<Value> {
    let mut measurements:[Measurements;8]=std::array::from_fn(|_|Measurements::default());
    let mut by_venue=BTreeMap::<&'static str,Measurements>::new();
    let mut samples=Vec::<Value>::new();let mut sampled=HashSet::new();
    let mut last_sequence=0;let mut sequence_errors=0;let mut lost=0;
    let mut baseline=0_u64;let mut websocket=0_u64;let mut unavailable=0_u64;
    let mut state=Progress::default();
    loop {match input.recv().await {
        Ok(event)=>{
            let received=timestamp_us();
            let sequence=match event.as_ref(){
                MarketStateEvent::Price(p)=>p.sequence,
                MarketStateEvent::Selection{sequence,..}|MarketStateEvent::Stats{sequence,..}|
                MarketStateEvent::Unavailable{sequence,..}|MarketStateEvent::Removed{sequence,..}|
                MarketStateEvent::Gap{sequence,..}=>*sequence,
            };
            if sequence!=last_sequence+1{sequence_errors+=1;}last_sequence=sequence;
            match event.as_ref() {
                MarketStateEvent::Price(p)=>{
                    if matches!(p.origin,PriceOrigin::Baseline){baseline+=1;continue;}
                    websocket+=1;
                    let phase=phase.borrow();let retained=phase.retained.contains(&p.pool);
                    measurements[phase.index].add(p,received,retained);
                    by_venue.entry(p.venue).or_default().add(p,received,retained);
                    // One real price example per venue, serialized only after streaming ends.
                    if sampled.insert(p.venue){samples.push(json!({"price":p,"consumer_received_at_us":received}));}
                },
                MarketStateEvent::Selection{selected,..}=>{
                    state=Progress{selected:selected.len(),..Progress::default()};progress.send_replace(state);
                },
                MarketStateEvent::Stats{pools,ready,accounts,active,..}=>{
                    state=Progress{selected:*pools,ready:*ready,accounts:*accounts,active:*active};progress.send_replace(state);
                    let index=phase.borrow().index;let m=&measurements[index];
                    crate::log_event!(Info,"market decode consumer phase={index} selected={pools} ready={ready} accounts={accounts} active={active} baseline={baseline} websocket={websocket} market_max_ns={} frame_to_consumer_max_ns={}",m.market.max,m.frame.max);
                },
                MarketStateEvent::Unavailable{..}=>unavailable+=1,
                _=>{},
            }
        },
        Err(broadcast::error::RecvError::Lagged(n))=>lost+=n,
        Err(broadcast::error::RecvError::Closed)=>break,
    }}
    let names=["initial_transition","initial_steady","add_transition","add_steady","remove_transition","remove_steady","restore_transition","restore_steady"];
    Ok(json!({"baseline_prices":baseline,"websocket_prices":websocket,"unavailable_events":unavailable,
        "lost_events":lost,"sequence_errors":sequence_errors,"last_sequence":last_sequence,
        "phases":names.into_iter().zip(&measurements).map(|(name,m)|(name,m.report())).collect::<BTreeMap<_,_>>(),
        "by_venue":by_venue.iter().map(|(name,m)|(*name,m.report())).collect::<BTreeMap<_,_>>(),"samples":samples,
        "last_coverage":{"selected":state.selected,"ready":state.ready,"accounts":state.accounts,"active":state.active}}))
}

struct Task<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for Task<T>{fn drop(&mut self){self.0.abort();}}

pub async fn decode_live(registry:PoolRegistry,rpc:RpcHandle)->Result<()> {
    let mut report=json!({"run_complete":false,"live_evidence_passed":false,"decoding_evidence_passed":false,
        "execution":false,"signals":false,"source":"persisted_prepared_proofs","live_admission_established":false});
    let result=run(registry,rpc,&mut report).await;
    if let Err(error)=&result{report["error"]=json!(format!("{error:#}"));}
    if let Ok(path)=env::var("MARKET_STATE_REPORT_PATH"){
        std::fs::write(path,serde_json::to_vec_pretty(&report)?).context("write market decode report")?;
    }
    println!("{}",serde_json::to_string(&report)?);
    result
}

async fn run(registry:PoolRegistry,rpc:RpcHandle,report:&mut Value)->Result<()> {
    let settings=Settings::from_env()?;
    let duration:u64=env_parse("MARKET_STATE_DURATION_SECONDS",600)?;
    let timeout:u64=env_parse("MARKET_STATE_TRANSITION_TIMEOUT_SECONDS",600)?;
    ensure!((60..=86400).contains(&duration)&&timeout>0,"invalid decode duration/transition timeout");
    let candidates=registry.snapshot().iter().filter_map(PoolInput::prepared).map(|p|(p.address,p)).collect();
    let all=select(&candidates,settings.pool_limit);
    ensure!(all.len()>=4,"at least four persisted Prepared pools are required");
    // Resolve/validate account requirements before measured streaming; same planner as CP1.2.
    let watches=account_stream_plan(&registry,&rpc).await?;
    report["selected_pools"]=json!(all.len());report["planned_accounts"]=json!(watches.len());
    if let Ok(path)=env::var("RPC_ACCOUNT_STREAM_ACCOUNTS_PATH") {
        let rows:Vec<_>=watches.iter().map(|w|match w.view{
            AccountView::Full=>json!({"address":w.address.to_string(),"offset":null,"length":null}),
            AccountView::Slice{offset,length}=>json!({"address":w.address.to_string(),"offset":offset,"length":length}),
        }).collect();
        std::fs::write(path,serde_json::to_vec_pretty(&rows)?).context("write decode watch plan")?;
    }
    let half=all.len()/2;
    let (account_control,account_events)=rpc.account_stream();
    let (prices,price_rx)=broadcast::channel(16384);
    let (updates,update_rx)=broadcast::channel(16);
    let (stop,stop_rx)=watch::channel(false);let (_enabled_tx,enabled)=watch::channel(true);
    let (limit_tx,limit)=watch::channel(half);
    let (phase_tx,phase)=watch::channel(Phase{index:0,retained:Arc::new(HashSet::new())});
    let (progress_tx,mut progress)=watch::channel(Progress::default());
    let mut consumer=Task(tokio::spawn(observe(price_rx,phase,progress_tx)));
    let mut producer=Task(tokio::spawn(produce(registry,update_rx,rpc.clone(),account_control.clone(),account_events,
        prices,stop_rx,enabled,limit,true)));
    let mut phases=Vec::new();let mut previous=HashSet::<String>::new();
    let phases_result:Result<()>=async {
        for (i,name) in ["initial","add","remove","restore"].into_iter().enumerate(){
            let target=if i%2==0{half}else{all.len()};
            let current:HashSet<_>=all[..target].iter().map(|p|p.address.to_string()).collect();
            let retained=Arc::new(current.intersection(&previous).cloned().collect());
            phase_tx.send_replace(Phase{index:i*2,retained:Arc::clone(&retained)});
            limit_tx.send_replace(target);
            let started=time::Instant::now();
            // Wait for this selection, every pool's post-ACK baseline and all account ACKs.
            let warmup=async {loop {
                let state=*progress.borrow_and_update();
                if state.selected==target && state.ready==target && state.accounts>0 && state.active==state.accounts{break;}
                progress.changed().await.context("decode consumer stopped during initialization")?;
            } Ok::<(),anyhow::Error>(())};
            time::timeout(Duration::from_secs(timeout),warmup).await.context("market decode initialization timed out")??;
            let transition=started.elapsed().as_secs_f64();
            phase_tx.send_replace(Phase{index:i*2+1,retained});
            let steady=duration*(i as u64+1)/4-duration*i as u64/4;
            crate::log_event!(Info,"market decode phase={name} pools={target} ready=true transition_seconds={transition:.3} measurement_seconds={steady}");
            time::sleep(Duration::from_secs(steady)).await;
            phases.push(json!({"name":name,"pools":target,"transition_seconds":transition,"measurement_seconds":steady}));
            previous=current;
        }
        Ok(())
    }.await;
    stop.send_replace(true);drop(updates);
    let produced=time::timeout(Duration::from_secs(10),&mut producer.0).await.context("market decode shutdown timed out")??;
    account_control.replace(Vec::new())?;
    let consumed=time::timeout(Duration::from_secs(10),&mut consumer.0).await.context("decode consumer shutdown timed out")???;
    report["phases"]=json!(phases);report["consumer"]=consumed;
    report["transport"]=rpc.stream_report();
    report["producer"]=produced?;
    report["run_complete"]=json!(phases_result.is_ok());
    phases_result?;
    let p=&report["producer"];let c=&report["consumer"];
    let decoding=c["websocket_prices"].as_u64().unwrap_or(0)>0 && p["websocket_prices"]==c["websocket_prices"]
        && p["baseline_prices"]==c["baseline_prices"] && p["no_receiver"]==0 && p["lost_events"]==0
        && c["lost_events"]==0 && c["sequence_errors"]==0
        && c["by_venue"].as_object().is_some_and(|v|v.values().all(|v|v["clock_errors"]==0));
    let decoding=decoding && p["by_venue"].as_object().is_some_and(|v|v.values().all(|v|v["decode_errors"]==0))
        && ["initial_steady","add_steady","remove_steady","restore_steady"].iter()
            .all(|name|c["phases"][*name]["websocket_prices"].as_u64().unwrap_or(0)>0);
    let continuity=p["gaps"]==0&&p["rejections"]==0;
    report["decoding_evidence_passed"]=json!(decoding);
    report["continuity_passed"]=json!(continuity);
    report["transition_activity_observed"]=json!(["add_transition","remove_transition","restore_transition"].iter()
        .all(|name|report["consumer"]["phases"][*name]["retained_pool_prices"].as_u64().unwrap_or(0)>0));
    report["live_evidence_passed"]=json!(decoding);
    ensure!(decoding,"decoder/consumer evidence incomplete; inspect producer, consumer and per-venue counters");
    Ok(())
}
