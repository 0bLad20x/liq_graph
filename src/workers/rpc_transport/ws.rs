//! Transport framing and socket lifecycle. The reader never awaits the writer or a consumer.
use std::{borrow::Cow,collections::{BTreeMap,HashMap,HashSet,VecDeque},sync::{Arc,Mutex,atomic::Ordering},time::{Duration,SystemTime,UNIX_EPOCH}};
use anyhow::{Result,Context,bail,ensure};
use base64::{engine::general_purpose::STANDARD,Engine as _};
use futures_util::{SinkExt,StreamExt};
use serde::Deserialize;
use serde_json::{json,Value};
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast,mpsc,watch},task::JoinSet,time::{self,Instant,timeout,sleep,sleep_until}};
use tokio_tungstenite::{connect_async,tungstenite::Message};
use crate::core::account_stream::{AccountData,AccountEvent,AccountStreamControl,AccountView,AccountWatch};
use super::WsSettings;

pub(super) fn now_us()->u64 {SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64}
fn nanos(at:Instant)->u64 {at.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64}

#[derive(Clone,Default)]
pub(super) struct Latency {count:u64,total:u128,max:u64,bins:[u64;32]}
impl Latency {
    pub(super) fn add(&mut self,n:u64) {
        self.count+=1;self.total+=u128::from(n);self.max=self.max.max(n);
        let i=if n==0 {0}else{(64-n.leading_zeros() as usize).min(31)};self.bins[i]+=1;
    }
    pub(super) fn report(&self)->Value {
        let percentile=|p:u64|->Option<u64>{
            if self.count==0{return None;}
            let rank=(self.count*p).div_ceil(100);let mut count=0;
            for (i,&n) in self.bins.iter().enumerate() {count+=n;if count>=rank {
                return Some(if i==31 {self.max}else{((1_u64<<i)-1).min(self.max)});
            }}None
        };
        json!({"count":self.count,"mean_ns":(self.count>0).then(||self.total as f64/self.count as f64),
            "max_ns":(self.count>0).then_some(self.max),"p50_upper_ns":percentile(50),
            "p95_upper_ns":percentile(95),"p99_upper_ns":percentile(99)})
    }
}
#[derive(Clone,Default)]
pub(super) struct SourceStats {
    sessions:u64,frames:u64,notifications:u64,published:u64,retired:u64,subscribe_acks:u64,
    unsubscribe_acks:u64,rejections:u64,gaps:u64,decode_errors:u64,no_receiver:u64,
    decode:Latency,forward:Latency,write_wait:Latency,write:Latency,reconcile:Latency,enqueue:Latency,
}
pub(super) type Metrics=Arc<Mutex<BTreeMap<String,SourceStats>>>;
pub(super) fn report(metrics:&Metrics)->Value {
    let stats=metrics.lock().expect("stream metrics poisoned");
    let sources:Vec<_>=stats.iter().map(|(name,s)|json!({"source":name,"sessions":s.sessions,
        "text_frames":s.frames,"account_notifications":s.notifications,"account_published":s.published,
        "retired_notifications":s.retired,"subscribe_acks":s.subscribe_acks,"unsubscribe_acks":s.unsubscribe_acks,
        "rejections":s.rejections,"gaps":s.gaps,"decode_errors":s.decode_errors,"no_receiver":s.no_receiver,
        "decode":s.decode.report(),"publication":s.forward.report(),"control_pacing":s.write_wait.report(),"control_write":s.write.report(),"watch_reconcile":s.reconcile.report(),"control_enqueue":s.enqueue.report()})).collect();
    json!({"layout":"independent_socket_reader_v1","sources":sources})
}

#[derive(Deserialize)]
struct Data<'a>(#[serde(borrow)] Cow<'a,str>,#[serde(borrow)] Cow<'a,str>);
#[derive(Deserialize)]
struct Account<'a> {#[serde(borrow)] owner:Cow<'a,str>,#[serde(borrow)] data:Data<'a>,executable:bool,space:Option<usize>}
#[derive(Deserialize)]
#[serde(untagged)]
enum AccountValue<'a> {Present(#[serde(borrow)] Account<'a>),Deleted(())}
#[derive(Deserialize)]
struct ContextSlot {slot:u64}
#[derive(Deserialize)]
struct Update<'a> {context:ContextSlot,#[serde(borrow)] value:AccountValue<'a>}
#[derive(Deserialize)]
struct Params<'a> {subscription:u64,#[serde(borrow)] result:Update<'a>}
#[derive(Deserialize)]
#[serde(untagged)]
enum Reply {Subscription(u64),Unsubscribed(bool)}
#[derive(Deserialize)]
struct RpcError {code:i64}
#[derive(Deserialize)]
struct Frame<'a> {id:Option<u64>,result:Option<Reply>,error:Option<RpcError>,
    #[serde(borrow)] method:Option<Cow<'a,str>>,#[serde(borrow)] params:Option<Params<'a>>}

enum Write {Message(Message,bool),Flush}
struct Written {wait:u64,write:u64}
enum Pending {Subscribe(AccountWatch),Unsubscribe{subscription:u64,address:Pubkey}}

async fn session(cfg:WsSettings,name:String,capacity:usize,commitment:String,
    mut targets:watch::Receiver<Vec<AccountWatch>>,events:broadcast::Sender<AccountEvent>,
    control:AccountStreamControl,metrics:Metrics)->Result<()> {
    let mut stats=SourceStats::default();let mut slices=true;let mut failures=0_u32;
    let save=|s:&SourceStats| {metrics.lock().expect("stream metrics poisoned").insert(name.clone(),s.clone());};
    let emit=|e|{let _=events.send(e);};
    let gap=|reason,s:&mut SourceStats| {s.gaps+=1;emit(AccountEvent::Gap{provider:name.clone(),reason});};
    loop {
        let empty=targets.borrow().is_empty();
        if empty {if targets.changed().await.is_err(){save(&stats);return Ok(());}continue;}
        let connection=timeout(Duration::from_millis(cfg.timeout_ms),connect_async(cfg.url.as_str())).await;
        let socket=match connection {
            Ok(Ok((socket,_)))=>socket,
            failure=>{
                let reason=match failure {
                    Err(_)=>"connect_timeout",
                    Ok(Err(tokio_tungstenite::tungstenite::Error::Http(response))) if response.status().as_u16()==429=>"handshake_http_429",
                    _=>"connect_error",
                };
                gap(reason,&mut stats);save(&stats);failures=failures.saturating_add(1);
                let delay=cfg.reconnect_ms.saturating_mul(1_u64<<failures.min(6)).min(60000);
                tokio::select! {_=sleep(Duration::from_millis(delay))=>{},r=targets.changed()=>if r.is_err(){return Ok(());}}
                continue;
            }
        };
        if stats.sessions>0 {gap("reconnect_requires_resync",&mut stats);}
        stats.sessions+=1;failures=0;let session=stats.sessions;let mut sequence=0;
        let (mut sink,mut reader)=socket.split();
        // <=32 pending RPC requests; a separate bounded writer also carries heartbeat/flush work.
        let (commands,mut outgoing)=mpsc::channel::<Write>(64);
        let (receipts,mut written)=mpsc::channel::<Written>(64);
        let writer=async {
            let mut next=Instant::now();
            while let Some(command)=outgoing.recv().await {
                let started=Instant::now();
                let paced=matches!(&command,Write::Message(_,true));
                if paced {sleep_until(next).await;}
                let wait=nanos(started);let started=Instant::now();
                timeout(Duration::from_millis(cfg.timeout_ms),async {
                    match command {Write::Message(m,_)=>sink.send(m).await,Write::Flush=>sink.flush().await}
                }).await.context("WebSocket write timed out")??;
                if paced {next=Instant::now()+Duration::from_millis(cfg.spacing_ms);}
                receipts.try_send(Written{wait,write:nanos(started)}).context("WebSocket write receipts full/closed")?;
            }
            Ok::<(),anyhow::Error>(())
        };
        tokio::pin!(writer);
        let mut desired:BTreeMap<_,_>=targets.borrow_and_update().iter().map(|w|(w.address,w.view)).collect();
        let mut active=HashMap::<u64,AccountWatch>::new();let mut retired=HashSet::new();
        let mut pending=HashMap::<u64,Pending>::new();let mut deadlines=VecDeque::<(u64,Instant)>::new();
        let mut busy=HashSet::new();let mut subscribe=VecDeque::new();let mut unsubscribe=VecDeque::new();
        let mut dirty=true;let mut id=0_u64;let mut pending_subscriptions=0_usize;
        let mut heartbeat=time::interval(Duration::from_secs(20));heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Skip);heartbeat.tick().await;
        let mut publish_stats=time::interval(Duration::from_secs(1));publish_stats.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        'connected:loop {
            if dirty {
                let started=Instant::now();
                unsubscribe=active.iter().filter(|(_,w)|!desired.contains_key(&w.address)).map(|(&sub,w)|(sub,w.address)).collect();
                subscribe=desired.iter().filter(|(a,_)|!busy.contains(*a)).map(|(&address,&view)|AccountWatch{address,view}).collect();
                dirty=false;stats.reconcile.add(nanos(started));
            }
            let enqueue_started=Instant::now();let queued_before=id;
            while pending.len()<32 {
                let (request,command)=if let Some((subscription,address))=unsubscribe.pop_front() {
                    if !active.contains_key(&subscription) || desired.contains_key(&address){continue;}
                    id+=1;
                    (Pending::Unsubscribe{subscription,address},json!({"jsonrpc":"2.0","id":id,"method":"accountUnsubscribe","params":[subscription]}))
                }else {
                    if active.len()+pending_subscriptions>=capacity {break;}
                    let Some(mut w)=subscribe.pop_front() else {break;};
                    if busy.contains(&w.address) || desired.get(&w.address)!=Some(&w.view){continue;}
                    if !slices {w.view=AccountView::Full;}
                    id+=1;let mut config=json!({"encoding":"base64","commitment":commitment});
                    if let AccountView::Slice{offset,length}=w.view {config["dataSlice"]=json!({"offset":offset,"length":length});}
                    (Pending::Subscribe(w),json!({"jsonrpc":"2.0","id":id,"method":"accountSubscribe","params":[w.address.to_string(),config]}))
                };
                if commands.try_send(Write::Message(Message::Text(command.to_string().into()),matches!(&request,Pending::Subscribe(_)))).is_err() {
                    gap("control_queue_full",&mut stats);break 'connected;
                }
                match &request {
                    Pending::Subscribe(w)=>{busy.insert(w.address);pending_subscriptions+=1;},
                    Pending::Unsubscribe{subscription,..}=>{active.remove(subscription);retired.insert(*subscription);},
                }
                pending.insert(id,request);deadlines.push_back((id,Instant::now()+Duration::from_millis(cfg.timeout_ms)));
            }
            if id!=queued_before {stats.enqueue.add(nanos(enqueue_started));}
            while deadlines.front().is_some_and(|(id,_)|!pending.contains_key(id)){deadlines.pop_front();}
            let deadline=deadlines.front().map(|(_,at)|*at).unwrap_or_else(||Instant::now()+Duration::from_secs(86400));
            tokio::select! {
                result=&mut writer=>{
                    if let Err(error)=&result {crate::log_event!(Warn,"rpc websocket writer source={name} error={error:#}");}
                    gap(if result.is_err(){"socket_write_failed"}else{"socket_writer_closed"},&mut stats);break;
                },
                receipt=written.recv()=>if let Some(r)=receipt {stats.write_wait.add(r.wait);stats.write.add(r.write);},
                _=publish_stats.tick()=>save(&stats),
                _=heartbeat.tick()=>if commands.try_send(Write::Message(Message::Ping(Vec::new().into()),false)).is_err(){gap("control_queue_full",&mut stats);break;},
                _=sleep_until(deadline),if !deadlines.is_empty()=>{gap("subscription_reply_timeout",&mut stats);break;},
                change=targets.changed()=>{
                    if change.is_err(){save(&stats);return Ok(());}
                    desired=targets.borrow_and_update().iter().map(|w|(w.address,w.view)).collect();
                    if slices && active.values().any(|w|desired.get(&w.address).is_some_and(|v|*v!=w.view)) {
                        gap("view_changed_requires_resync",&mut stats);break;
                    }
                    dirty=true;
                },
                incoming=reader.next()=>{
                    let text=match incoming {
                        Some(Ok(Message::Text(text)))=>text,
                        Some(Ok(Message::Ping(_)))=>{
                            if commands.try_send(Write::Flush).is_err(){gap("control_queue_full",&mut stats);break;}
                            continue;
                        },
                        Some(Ok(Message::Pong(_)))=>continue,
                        _=>break,
                    };
                    let frame_received_at_us=now_us();let started=Instant::now();
                    stats.frames+=1;control.messages.fetch_add(1,Ordering::Relaxed);control.text_bytes.fetch_add(text.len() as u64,Ordering::Relaxed);
                    let frame:Frame<'_>=match serde_json::from_str(&text) {
                        Ok(frame)=>frame,Err(error)=>{stats.decode_errors+=1;crate::log_event!(Warn,"rpc websocket decode source={name} error={error}");gap("malformed_rpc_frame",&mut stats);break;}
                    };
                    if let Some(id)=frame.id {
                        match pending.remove(&id) {
                            Some(Pending::Subscribe(w))=>{
                                pending_subscriptions-=1;
                                if let Some(error)=frame.error {
                                    if slices && matches!(w.view,AccountView::Slice{..}) && matches!(error.code,-32600|-32602) {
                                        slices=false;gap("slice_rejected_retry_full",&mut stats);
                                    }else {
                                        stats.rejections+=1;emit(AccountEvent::Rejected{provider:name.clone(),address:w.address,code:error.code});
                                        gap("subscribe_rejected",&mut stats);
                                    }
                                    break;
                                }
                                let Some(Reply::Subscription(subscription))=frame.result else {gap("invalid_subscribe_reply",&mut stats);break;};
                                active.insert(subscription,w);stats.subscribe_acks+=1;
                                emit(AccountEvent::Subscribed{provider:name.clone(),address:w.address});
                                if slices && desired.get(&w.address).is_some_and(|v|*v!=w.view){gap("view_changed_requires_resync",&mut stats);break;}
                                if !desired.contains_key(&w.address){dirty=true;}
                            },
                            Some(Pending::Unsubscribe{address,..})=>{
                                if !matches!(frame.result,Some(Reply::Unsubscribed(true))) || frame.error.is_some(){gap("unsubscribe_rejected",&mut stats);break;}
                                busy.remove(&address);stats.unsubscribe_acks+=1;
                                emit(AccountEvent::Unsubscribed{provider:name.clone(),address});
                                if let Some(&view)=desired.get(&address){subscribe.push_back(AccountWatch{address,view});}
                            },
                            None=>{gap("unexpected_rpc_reply",&mut stats);break;},
                        }
                        continue;
                    }
                    if frame.method.as_deref()!=Some("accountNotification"){gap("unexpected_rpc_notification",&mut stats);break;}
                    let Some(params)=frame.params else {gap("malformed_account_notification",&mut stats);break;};
                    stats.notifications+=1;
                    let Some(w)=active.get(&params.subscription).copied() else {
                        if retired.contains(&params.subscription){stats.retired+=1;continue;}
                        gap("unknown_subscription",&mut stats);break;
                    };
                    let value=(||->Result<Option<Arc<AccountData>>>{
                        let AccountValue::Present(account)=params.result.value else {return Ok(None);};
                        ensure!(account.data.1=="base64","unsupported account encoding");
                        let bytes=STANDARD.decode(account.data.0.as_bytes())?;
                        let owner=account.owner.parse()?;let space=account.space;
                        let view=match w.view {AccountView::Slice{length,..} if bytes.len()!=length && space==Some(bytes.len()) && !bytes.is_empty()=>AccountView::Full,v=>v};
                        if view==AccountView::Full {ensure!(space.is_none_or(|s|s==bytes.len()),"full account length mismatch");}
                        let original_len=if view==AccountView::Full{Some(bytes.len())}else{space};
                        Ok(Some(Arc::new(AccountData{address:w.address,owner,executable:account.executable,bytes,view,original_len})))
                    })();
                    let value=match value {Ok(value)=>value,Err(error)=>{stats.decode_errors+=1;crate::log_event!(Warn,"rpc websocket account source={name} error={error:#}");gap("malformed_account_notification",&mut stats);break;}};
                    let decoded_at_us=now_us();stats.decode.add(nanos(started));sequence+=1;
                    let started=Instant::now();
                    let sent=events.send(AccountEvent::Account{provider:name.clone(),address:w.address,slot:params.result.context.slot,session,sequence,
                        received_at_ms:decoded_at_us/1000,frame_received_at_us,decoded_at_us,value});
                    if sent.is_ok(){stats.published+=1;}else{stats.no_receiver+=1;}stats.forward.add(nanos(started));
                },
            }
        }
        gap("socket_disconnected",&mut stats);save(&stats);
        tokio::select! {_=sleep(Duration::from_millis(cfg.reconnect_ms))=>{},r=targets.changed()=>if r.is_err(){return Ok(());}}
    }
}

pub(super) async fn run(settings:Vec<WsSettings>,commitment:String,mut targets:watch::Receiver<Vec<AccountWatch>>,
    events:broadcast::Sender<AccountEvent>,control:AccountStreamControl,metrics:Metrics)->Result<()> {
    let mut tasks=JoinSet::new();let mut sockets=Vec::new();
    for cfg in settings {
        let mut remaining=cfg.max_subscriptions;
        for i in 0..cfg.max_connections {
            if remaining==0{break;}
            let capacity=remaining.min(cfg.per_connection);remaining-=capacity;
            let (tx,rx)=watch::channel(Vec::new());let name=format!("{}/socket-{}",cfg.name,i+1);
            tasks.spawn(session(cfg.clone(),name,capacity,commitment.clone(),rx,events.clone(),control.clone(),metrics.clone()));
            sockets.push((capacity,tx));
        }
        ensure!(remaining==0,"WS socket budget insufficient");
    }
    let mut assignments=HashMap::<Pubkey,usize>::new();
    loop {
        let desired:BTreeMap<_,_>=targets.borrow_and_update().iter().map(|w|(w.address,*w)).collect();
        ensure!(desired.len()<=sockets.iter().map(|(n,_)|*n).sum::<usize>(),"WS account capacity exceeded");
        let mut groups=vec![Vec::new();sockets.len()];let mut next=HashMap::with_capacity(desired.len());
        for (&address,w) in &desired {
            if let Some(i)=assignments.get(&address).copied() {
                if groups[i].len()<sockets[i].0 {groups[i].push(*w);next.insert(address,i);}
            }
        }
        for (address,w) in desired {
            if next.contains_key(&address){continue;}
            let i=groups.iter().enumerate().filter(|(i,g)|g.len()<sockets[*i].0).min_by_key(|(_,g)|g.len()).map(|(i,_)|i).context("WS socket distribution exhausted")?;
            groups[i].push(w);next.insert(address,i);
        }
        assignments=next;
        for ((_,tx),group) in sockets.iter().zip(groups){tx.send_if_modified(|old|if *old==group{false}else{*old=group;true});}
        tokio::select! {
            r=targets.changed()=>if r.is_err(){return Ok(());},
            r=tasks.join_next(),if !tasks.is_empty()=>{r.context("no socket task")?.context("socket task panicked")??;bail!("socket task ended unexpectedly");},
        }
    }
}
