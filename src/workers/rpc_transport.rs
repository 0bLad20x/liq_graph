//! Unified, protocol-neutral Solana RPC boundary: multi-provider HTTP and account WebSockets.
//! All rate limits, provider capabilities, retries and subscription state live here.
//! No venue/account decoding beyond JSON-RPC and base64 transport framing.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    env,
    sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}},
    time::Duration,
};
use anyhow::{anyhow, bail, ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use solana_pubkey::Pubkey;
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch, Mutex, Semaphore},
    task::JoinHandle,
    time::{sleep, sleep_until, timeout, Instant},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::core::now_unix_ms;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountView {
    Full,
    Slice { offset: usize, length: usize },
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum AccountFilter {
    DataSize(usize),
    Memcmp { offset: usize, bytes: Vec<u8> },
}

#[derive(Clone, Debug)]
pub enum RpcOperation {
    Multiple { addresses: Vec<Pubkey>, view: AccountView, min_context_slot: Option<u64> },
    Account { address: Pubkey, view: AccountView },
    Program { program: Pubkey, filters: Vec<AccountFilter>, view: AccountView },
    ProgramV2 {
        program: Pubkey, filters: Vec<AccountFilter>, view: AccountView,
        cursor: Option<String>, changed_since_slot: Option<u64>, limit: usize,
    },
    Slot { commitment: String },
    GenesisHash,
}

impl RpcOperation {
    fn method(&self) -> &'static str {
        match self {
            Self::Multiple { .. } => "getMultipleAccounts",
            Self::Account { .. } => "getAccountInfo",
            Self::Program { .. } => "getProgramAccounts",
            Self::ProgramV2 { .. } => "getProgramAccountsV2",
            Self::Slot { .. } => "getSlot",
            Self::GenesisHash => "getGenesisHash",
        }
    }
    fn bit(&self) -> u64 {
        match self {
            Self::Multiple { .. } => 1,
            Self::Account { .. } => 2,
            Self::Program { .. } => 4,
            Self::ProgramV2 { .. } => 8,
            Self::Slot { .. } => 16,
            Self::GenesisHash => 32,
        }
    }
    fn params(&self, commitment: &str, account_limit: usize, v2_limit: usize) -> Result<Value> {
        fn view_json(view: AccountView, config: &mut Value) {
            if let AccountView::Slice {offset,length} = view {
                config["dataSlice"] = json!({"offset":offset,"length":length});
            }
        }
        fn filter_json(filters: &[AccountFilter]) -> Vec<Value> {
            filters.iter().map(|f| match f {
                AccountFilter::DataSize(size) => json!({"dataSize":size}),
                AccountFilter::Memcmp {offset,bytes} =>
                    json!({"memcmp":{"offset":offset,"bytes":bs58::encode(bytes).into_string()}}),
            }).collect()
        }
        Ok(match self {
            Self::Multiple {addresses,view,min_context_slot} => {
                ensure!(!addresses.is_empty() && addresses.len() <= account_limit && addresses.len() <= 100,
                    "account read exceeds configured batch limit");
                let mut cfg=json!({"encoding":"base64","commitment":commitment});
                view_json(*view,&mut cfg);
                if let Some(slot)=min_context_slot {cfg["minContextSlot"]=json!(slot);}
                json!([addresses.iter().map(ToString::to_string).collect::<Vec<_>>(),cfg])
            }
            Self::Account {address,view} => {
                let mut cfg=json!({"encoding":"base64","commitment":commitment});
                view_json(*view,&mut cfg);
                json!([address.to_string(),cfg])
            }
            Self::Program {program,filters,view} => {
                let mut cfg=json!({"encoding":"base64","commitment":commitment,"filters":filter_json(filters)});
                view_json(*view,&mut cfg);
                json!([program.to_string(),cfg])
            }
            Self::ProgramV2 {program,filters,view,cursor,changed_since_slot,limit} => {
                ensure!(*limit>0 && *limit<=v2_limit && *limit<=5000, "V2 page limit exceeded");
                ensure!(cursor.as_ref().is_none_or(|s| !s.is_empty()),"V2 cursor must not be empty");
                let mut cfg=json!({"encoding":"base64","commitment":commitment,"filters":filter_json(filters),"limit":limit});
                view_json(*view,&mut cfg);
                if let Some(s)=cursor {cfg["paginationKey"]=json!(s);}
                if let Some(s)=changed_since_slot {cfg["changedSinceSlot"]=json!(s);}
                json!([program.to_string(),cfg])
            }
            Self::Slot {commitment:level} => {
                ensure!(valid_commitment(level),"invalid slot commitment");
                json!([{"commitment":level}])
            }
            Self::GenesisHash => json!([]),
        })
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct AccountData {
    pub address: Pubkey,
    pub owner: Pubkey,
    pub bytes: Vec<u8>,
    pub view: AccountView,
    pub original_len: Option<usize>,
}

#[derive(Debug)]
pub enum RpcAnswer {
    Multiple { slot: u64, accounts: Vec<Option<AccountData>> },
    Account { slot: u64, account: Option<AccountData> },
    Program { accounts: Vec<AccountData>, next_cursor: Option<String> },
    Slot(u64),
    GenesisHash(String),
}

#[derive(Debug)]
pub struct RpcResponse {
    pub provider: String,
    pub received_at_ms: u64,
    pub answer: RpcAnswer,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum WsEvent {
    Subscribed {provider:String, address:Pubkey},
    Unsubscribed {provider:String, address:Pubkey},
    Account {provider:String, address:Pubkey, slot:u64, received_at_ms:u64,
        value:Option<AccountData>},
    Gap {provider:String, reason:&'static str},
    Rejected {provider:String, address:Pubkey, code:i64},
}

#[derive(Clone)]
pub struct RpcHandle {
    requests: mpsc::Sender<Call>,
    targets: watch::Sender<Vec<Pubkey>>,
    events: broadcast::Sender<WsEvent>,
    ws_capacity: usize,
    capabilities: u64,
}

impl RpcHandle {
    pub async fn call(&self, operation: RpcOperation) -> Result<RpcResponse> {
        let (reply, receive)=oneshot::channel();
        self.requests.send(Call{operation,reply}).await.context("RPC worker stopped")?;
        receive.await.context("RPC worker dropped response")?
    }

    /// One explicit desired account-watch set. Subscriptions are not started at construction.
    pub fn replace_watches(&self, addresses: Vec<Pubkey>) -> Result<()> {
        let unique:HashSet<Pubkey>=addresses.iter().copied().collect();
        ensure!(unique.len()==addresses.len(), "duplicate watched account");
        ensure!(addresses.len()<=self.ws_capacity, "WebSocket subscription capacity exceeded");
        self.targets.send(addresses).map_err(|_|anyhow!("RPC watch worker stopped"))
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<WsEvent> { self.events.subscribe() }
    pub fn supports(&self, operation: &RpcOperation) -> bool {
        self.capabilities & operation.bit() != 0
    }
    pub fn can_watch(&self) -> bool { self.ws_capacity > 0 }
}

struct Call {operation:RpcOperation,reply:oneshot::Sender<Result<RpcResponse>>}

#[derive(Clone, Debug)]
struct HttpSettings {
    name:String, url:String, rps:u32, max_in_flight:usize, timeout_ms:u64,
    retries:usize, capabilities:u64,
}
#[derive(Clone, Debug)]
struct WsSettings {
    name:String, url:String, max_connections:usize, max_subscriptions:usize,
    per_connection:usize, spacing_ms:u64, timeout_ms:u64, reconnect_ms:u64,
}
#[derive(Clone, Debug)]
pub struct Settings {
    http:Vec<HttpSettings>, ws:Vec<WsSettings>, commitment:String,
    account_batch_size:usize, v2_page_limit:usize,
}

fn valid_commitment(s:&str)->bool {
    matches!(s,"processed"|"confirmed"|"finalized")
}
fn var<T:std::str::FromStr>(name:&str,default:T)->Result<T>
where T::Err:std::fmt::Display {
    match env::var(name) {
        Ok(raw)=>raw.parse().map_err(|e|anyhow!("invalid {name}: {e}")),
        Err(_)=>Ok(default),
    }
}
fn flag(name:&str,default:bool)->Result<bool> {
    match env::var(name) {
        Ok(s)=> match s.as_str() {"true"|"1"=>Ok(true),"false"|"0"=>Ok(false),_=>bail!("invalid boolean {name}")},
        Err(_)=>Ok(default),
    }
}
fn url(name:&str, schemes:&[&str])->Result<Option<String>> {
    let s=env::var(name).unwrap_or_default();
    if s.trim().is_empty(){return Ok(None);}
    let parsed=reqwest::Url::parse(&s).map_err(|_|anyhow!("invalid URL in {name}"))?;
    ensure!(schemes.contains(&parsed.scheme()),"invalid URL scheme in {name}");
    Ok(Some(s))
}
pub fn configured()->bool {
    env::vars().any(|(key,val)|{
        key.starts_with("RPC_") && (key.ends_with("_HTTP_URL")||key.ends_with("_WS_URL"))
            && !val.trim().is_empty()
    })
}

pub fn settings_from_env()->Result<Settings> {
    let mut indices=BTreeSet::new();
    for (key,_) in env::vars() {
        if let Some(rest)=key.strip_prefix("RPC_") {
            if let Some((index, _))=rest.split_once('_') {
                if let Ok(n)=index.parse::<usize>() {ensure!(n>0,"RPC provider index must be positive");indices.insert(n);}
            }
        }
    }
    let mut http=Vec::new();let mut ws=Vec::new();
    let mut names=HashSet::new();let mut http_urls=HashSet::new();let mut ws_urls=HashSet::new();
    for index in indices {
        let prefix=format!("RPC_{index}_");
        let name=env::var(format!("{prefix}NAME")).unwrap_or_else(|_|format!("provider-{index}"));
        ensure!(!name.trim().is_empty(),"RPC provider name is blank");
        let http_url=url(&format!("{prefix}HTTP_URL"),&["http","https"])?;
        let ws_url=url(&format!("{prefix}WS_URL"),&["ws","wss"])?;
        if http_url.is_none() && ws_url.is_none() {continue;}
        ensure!(names.insert(name.clone()),"duplicate RPC provider name {name}");
        if let Some(endpoint)=http_url {
            ensure!(http_urls.insert(endpoint.clone()),"duplicate RPC HTTP URL");
            let rps:u32=var(&format!("{prefix}HTTP_RPS"),1)?;
            let max:usize=var(&format!("{prefix}MAX_IN_FLIGHT"),1)?;
            let timeout_ms:u64=var(&format!("{prefix}TIMEOUT_MS"),30000)?;
            let retries:usize=var(&format!("{prefix}RETRY_ATTEMPTS"),2)?;
            ensure!(rps>0 && max>0 && max<=1024 && timeout_ms>0 && (1..=10).contains(&retries),
                "invalid RPC HTTP limits for provider {name}");
            let capabilities=[
                ("GET_MULTIPLE_ACCOUNTS",1,true),
                ("GET_ACCOUNT_INFO",2,true),
                ("GET_PROGRAM_ACCOUNTS",4,false),
                ("GET_PROGRAM_ACCOUNTS_V2",8,false),
                ("GET_SLOT",16,true),
                ("GET_GENESIS_HASH",32,true),
            ];
            let mut mask=0_u64;
            for (suffix,bit,default) in capabilities {
                if flag(&format!("{prefix}{suffix}"),default)? {mask|=bit;}
            }
            http.push(HttpSettings{
                name:name.clone(),url:endpoint,rps,max_in_flight:max,timeout_ms,retries,capabilities:mask,
            });
        }
        if let Some(endpoint)=ws_url {
            ensure!(ws_urls.insert(endpoint.clone()),"duplicate RPC WS URL");
            if flag(&format!("{prefix}WS_ACCOUNT_SUBSCRIBE"),false)? {
                let connections:usize=var(&format!("{prefix}WS_MAX_CONNECTIONS"),1)?;
                let maximum:usize=var(&format!("{prefix}WS_MAX_SUBSCRIPTIONS"),1)?;
                let per_connection:usize=var(&format!("{prefix}WS_SUBSCRIPTIONS_PER_CONNECTION"),1)?;
                let spacing_ms:u64=var(&format!("{prefix}WS_SUBSCRIBE_SPACING_MS"),10)?;
                let timeout_ms:u64=var(&format!("{prefix}WS_TIMEOUT_MS"),45000)?;
                let reconnect_ms:u64=var(&format!("{prefix}WS_RECONNECT_MS"),1000)?;
                ensure!(connections>0 && connections<=64 && maximum>0 && per_connection>0
                    && connections.checked_mul(per_connection).is_some_and(|n|n>=maximum)
                    && timeout_ms>0 && reconnect_ms>0,
                    "invalid WebSocket limits for provider {name}");
                ws.push(WsSettings{name,url:endpoint,max_connections:connections,max_subscriptions:maximum,
                    per_connection,spacing_ms,timeout_ms,reconnect_ms});
            }
        }
    }
    ensure!(!http.is_empty() || !ws.is_empty(),"no usable RPC HTTP or WS provider configured");
    let commitment=env::var("RPC_COMMITMENT").unwrap_or_else(|_|"confirmed".to_owned());
    ensure!(valid_commitment(&commitment),"invalid RPC_COMMITMENT");
    let account_batch_size:usize=var("RPC_ACCOUNT_BATCH_SIZE",100)?;
    let v2_page_limit:usize=var("RPC_V2_PAGE_LIMIT",5000)?;
    ensure!((1..=100).contains(&account_batch_size),"invalid RPC_ACCOUNT_BATCH_SIZE");
    ensure!((1..=5000).contains(&v2_page_limit),"invalid RPC_V2_PAGE_LIMIT");
    Ok(Settings{http,ws,commitment,account_batch_size,v2_page_limit})
}

struct HttpProvider {
    cfg:HttpSettings, client:reqwest::Client,
    gate:Mutex<Instant>, concurrent:Semaphore,
    cooldown:Mutex<Option<Instant>>, request_id:AtomicU64,
    disabled:AtomicU64,
}
impl HttpProvider {
    fn new(cfg:HttpSettings)->Result<Self> {
        let client=reqwest::Client::builder()
            .timeout(Duration::from_millis(cfg.timeout_ms)).build()?;
        Ok(Self{concurrent:Semaphore::new(cfg.max_in_flight),
            gate:Mutex::new(Instant::now()),cooldown:Mutex::new(None),
            request_id:AtomicU64::new(1),disabled:AtomicU64::new(0),client,cfg})
    }
    fn eligible(&self,bit:u64)->bool {
        self.cfg.capabilities&bit!=0 && self.disabled.load(Ordering::Relaxed)&bit==0
    }
    async fn request(&self,operation:&RpcOperation,params:&Value)->Result<Value> {
        let method=operation.method();
        let mut previous=None;
        for attempt in 0..self.cfg.retries {
            if self.cooldown.lock().await.is_some_and(|until|until>Instant::now()) {
                bail!("provider {} in HTTP 429 cooldown",self.cfg.name);
            }
            if attempt>0 {sleep(Duration::from_millis(200_u64.saturating_mul(1 << attempt.min(7)))).await;}
            let _permit=self.concurrent.acquire().await?;
            let mut next=self.gate.lock().await;
            sleep_until(*next).await;
            *next=Instant::now()+Duration::from_secs_f64(1.0/f64::from(self.cfg.rps));
            drop(next);

            let id=self.request_id.fetch_add(1,Ordering::Relaxed);
            let outcome=self.client.post(&self.cfg.url)
                .json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
                .send().await;
            let response=match outcome {
                Ok(value)=>value,
                Err(error)=>{
                    previous=Some(anyhow!("transport error: {}",error.without_url()));
                    continue;
                }
            };
            if response.status()==reqwest::StatusCode::TOO_MANY_REQUESTS {
                let retry_after=response.headers().get(reqwest::header::RETRY_AFTER)
                    .and_then(|s|s.to_str().ok()).and_then(|s|s.parse::<u64>().ok())
                    .unwrap_or(2).max(1);
                *self.cooldown.lock().await=Instant::now().checked_add(Duration::from_secs(retry_after));
                bail!("HTTP 429 provider={} cooldown_s={retry_after}",self.cfg.name);
            }
            if !response.status().is_success() {
                let status=response.status();
                if status.is_server_error() {
                    previous=Some(anyhow!("HTTP {status} at provider {}",self.cfg.name));
                    continue;
                }
                bail!("HTTP {status} at provider {}",self.cfg.name);
            }
            let body=response.bytes().await.context("RPC HTTP response body")?;
            let value:Value=serde_json::from_slice(&body).context("invalid RPC response JSON")?;
            ensure!(value.get("jsonrpc").and_then(Value::as_str)==Some("2.0")
                && value.get("id")==Some(&json!(id)), "JSON-RPC envelope id/version mismatch");
            if let Some(error)=value.get("error") {
                let code=error.get("code").and_then(Value::as_i64).unwrap_or(0);
                if code == -32601 {
                    self.disabled.fetch_or(operation.bit(),Ordering::Relaxed);
                    bail!("method {method} unsupported by provider {}",self.cfg.name);
                }
                if matches!(code,-32004|-32005|-32016) {
                    previous=Some(anyhow!("RPC transient code={code} provider={}",self.cfg.name));
                    continue;
                }
                bail!("RPC rejected method={method} code={code} provider={}",self.cfg.name);
            }
            return value.get("result").cloned().context("RPC response missing result");
        }
        Err(previous.unwrap_or_else(||anyhow!("RPC attempts exhausted provider={}",self.cfg.name)))
    }
}

fn decode_account(address:Pubkey,value:&Value,view:AccountView)->Result<Option<AccountData>> {
    if value.is_null(){return Ok(None);}
    let owner:Pubkey=value.get("owner").and_then(Value::as_str)
        .context("RPC account owner missing")?.parse().context("invalid RPC owner")?;
    let tuple=value.get("data").and_then(Value::as_array).context("account data missing")?;
    ensure!(tuple.len()==2 && tuple[1].as_str()==Some("base64"),"unsupported RPC data encoding");
    let bytes=STANDARD.decode(tuple[0].as_str().context("invalid RPC base64 string")?)?;
    let space=value.get("space").and_then(Value::as_u64).map(|n|n as usize);
    let original_len=match view {
        AccountView::Full=>{
            ensure!(space.is_none_or(|n|n==bytes.len()),"RPC full account length mismatch");
            Some(bytes.len())
        }
        AccountView::Slice{..}=>space,
    };
    Ok(Some(AccountData{address,owner,bytes,view,original_len}))
}

fn interpret(operation:RpcOperation,result:Value)->Result<RpcAnswer> {
    let v2_request = match &operation {
        RpcOperation::ProgramV2{cursor,limit,..}=>Some((cursor.clone(),*limit)),
        _=>None,
    };
    fn response_slot(value:&Value)->Result<u64>{
        value.pointer("/context/slot").and_then(Value::as_u64).context("RPC context slot missing")
    }
    match operation {
        RpcOperation::Multiple{addresses,view,min_context_slot}=>{
            let slot=response_slot(&result)?;
            ensure!(min_context_slot.is_none_or(|min|slot>=min),"RPC slot is older than requested");
            let values=result.get("value").and_then(Value::as_array).context("RPC accounts missing")?;
            ensure!(values.len()==addresses.len(),"RPC accounts cardinality mismatch");
            let accounts=addresses.into_iter().zip(values).map(|(address,val)|decode_account(address,val,view))
                .collect::<Result<Vec<_>>>()?;
            Ok(RpcAnswer::Multiple{slot,accounts})
        }
        RpcOperation::Account{address,view}=>{
            let slot=response_slot(&result)?;
            let account=decode_account(address,result.get("value").context("RPC account value missing")?,view)?;
            Ok(RpcAnswer::Account{slot,account})
        }
        RpcOperation::Program{view,..}|RpcOperation::ProgramV2{view,..}=>{
            let (items,cursor)=if let Some(items)=result.as_array(){
                (items,None)
            } else {
                let items=result.get("accounts").and_then(Value::as_array).context("V2 accounts missing")?;
                let cursor=result.get("paginationKey").and_then(Value::as_str).map(str::to_owned);
                ensure!(cursor.as_deref().is_none_or(|s|!s.is_empty()),"empty V2 cursor");
                (items,cursor)
            };
            if let Some((previous,limit))=&v2_request {
                ensure!(items.len()<=*limit,"V2 provider exceeded page limit");
                ensure!(cursor.as_ref().is_none_or(|new|Some(new)!=previous.as_ref()),
                    "V2 provider returned repeated cursor");
            }
            let mut accounts=Vec::with_capacity(items.len());
            for item in items {
                let address:Pubkey=item.get("pubkey").and_then(Value::as_str)
                    .context("program account pubkey missing")?.parse()?;
                let data=decode_account(address,item.get("account").context("program account body missing")?,view)?
                    .context("null program account")?;
                accounts.push(data);
            }
            Ok(RpcAnswer::Program{accounts,next_cursor:cursor})
        }
        RpcOperation::Slot{..}=>Ok(RpcAnswer::Slot(result.as_u64().context("invalid getSlot value")?)),
        RpcOperation::GenesisHash=>Ok(RpcAnswer::GenesisHash(result.as_str()
            .context("invalid genesis hash")?.to_owned())),
    }
}

async fn http_run(settings:Settings, mut rx:mpsc::Receiver<Call>)->Result<()> {
    let providers=settings.http.into_iter().map(HttpProvider::new)
        .map(|r|r.map(Arc::new)).collect::<Result<Vec<_>>>()?;
    let mut tasks=tokio::task::JoinSet::new();
    let next=Arc::new(AtomicUsize::new(0));
    while let Some(Call{operation,reply})=rx.recv().await {
        // A semaphore inside each provider limits concurrency independently.
        let eligible:Vec<_>=providers.iter().filter(|provider|provider.eligible(operation.bit()))
            .cloned().collect();
        let params=operation.params(&settings.commitment,settings.account_batch_size,settings.v2_page_limit);
        let cursor=next.fetch_add(1,Ordering::Relaxed);
        tasks.spawn(async move {
            let answer=async {
                let params=params?;
                ensure!(!eligible.is_empty(),"no configured RPC provider supports {}",operation.method());
                let mut error=None;
                for offset in 0..eligible.len() {
                    let provider=&eligible[(cursor+offset)%eligible.len()];
                    if !provider.eligible(operation.bit()) {continue;}
                    match provider.request(&operation,&params).await {
                        Ok(value)=>match interpret(operation.clone(),value) {
                            Ok(answer)=>return Ok(RpcResponse {
                                provider:provider.cfg.name.clone(),
                                received_at_ms:now_unix_ms()?,
                                answer,
                            }),
                            Err(invalid)=>{
                                eprintln!("rpc provider={} method={} response_validation_error={invalid:#}",
                                    provider.cfg.name,operation.method());
                                error=Some(invalid);
                            }
                        },
                        Err(err)=>{
                            eprintln!("rpc provider={} method={} error={err:#}",provider.cfg.name,operation.method());
                            error=Some(err);
                        }
                    }
                }
                Err(error.unwrap_or_else(||anyhow!("no healthy eligible RPC provider")))
            }.await;
            let _=reply.send(answer);
        });
        while tasks.len()>1024 {
            let _=tasks.join_next().await;
        }
    }
    while let Some(result)=tasks.join_next().await {result?;}
    Ok(())
}

fn emit(tx:&broadcast::Sender<WsEvent>,event:WsEvent) {
    let _=tx.send(event);
}

async fn ws_session(
    name:String,endpoint:String,capacity:usize,spacing_ms:u64,timeout_ms:u64,reconnect_ms:u64,
    commitment:String,mut targets:watch::Receiver<Vec<Pubkey>>,events:broadcast::Sender<WsEvent>,
)->Result<()> {
    let mut connected_before=false;
    let mut failures=0_u32;
    loop {
        if targets.borrow().is_empty() {
            if targets.changed().await.is_err(){return Ok(());}
            continue;
        }
        let connection=timeout(Duration::from_millis(timeout_ms),connect_async(endpoint.as_str())).await;
        let mut ws=match connection {
            Ok(Ok((socket,_)))=>socket,
            failure=>{
                let reason=match &failure {
                    Err(_)=>"connect_timeout",
                    Ok(Err(tokio_tungstenite::tungstenite::Error::Http(response)))
                        if response.status().as_u16()==429 => {
                            emit(&events,WsEvent::Gap{provider:name.clone(),reason:"handshake_http_429"});
                            return Ok(());
                        }
                    Ok(Err(_))=>"connect_error",
                    Ok(Ok(_))=>unreachable!(),
                };
                emit(&events,WsEvent::Gap{provider:name.clone(),reason});
                failures=failures.saturating_add(1);
                let delay=reconnect_ms.saturating_mul(1_u64<<failures.min(6)).min(60000);
                tokio::select! {
                    _=sleep(Duration::from_millis(delay))=>{},
                    change=targets.changed()=>if change.is_err(){return Ok(())},
                }
                continue;
            }
        };
        if connected_before {
            emit(&events,WsEvent::Gap{provider:name.clone(),reason:"reconnect_requires_resync"});
        }
        connected_before=true;failures=0;
        let mut heartbeat=tokio::time::interval(Duration::from_secs(20));
        heartbeat.tick().await;
        let mut active:HashMap<u64,Pubkey>=HashMap::new();
        let mut retired:HashSet<u64>=HashSet::new();
        let mut pending:HashMap<u64,Pubkey>=HashMap::new();
        let mut pending_unsub:HashMap<u64,Pubkey>=HashMap::new();
        let mut request_id=1_u64;
        let mut desired:BTreeSet<Pubkey>=targets.borrow().iter().copied().collect();
        'connected:loop {
            // Remove obsolete subscriptions; keep valid active subscriptions stable.
            let obsolete:Vec<_>=active.iter().filter(|(_,address)|!desired.contains(address))
                .map(|(id,address)|(*id,*address)).collect();
            for (subscription_id,address) in obsolete {
                let id=request_id;request_id+=1;
                if ws.send(Message::Text(json!({"jsonrpc":"2.0","id":id,
                        "method":"accountUnsubscribe","params":[subscription_id]}).to_string().into()))
                    .await.is_err(){break 'connected;}
                active.remove(&subscription_id);
                retired.insert(subscription_id);
                pending_unsub.insert(id,address);
            }
            let busy:BTreeSet<Pubkey>=active.values().chain(pending.values()).copied().collect();
            for address in desired.iter().filter(|address|!busy.contains(address))
                .take(32_usize.saturating_sub(pending.len())).copied().collect::<Vec<_>>() {
                if active.len()+pending.len()>=capacity {break;}
                let id=request_id;request_id+=1;
                if ws.send(Message::Text(json!({"jsonrpc":"2.0","id":id,
                    "method":"accountSubscribe","params":[address.to_string(),
                    {"encoding":"base64","commitment":commitment}]]}).to_string().into()))
                    .await.is_err(){break 'connected;}
                pending.insert(id,address);
                if spacing_ms>0 {sleep(Duration::from_millis(spacing_ms)).await;}
            }
            tokio::select! {
                _=heartbeat.tick()=>{
                    if ws.send(Message::Ping(Vec::<u8>::new().into())).await.is_err() {
                        break 'connected;
                    }
                }
                update=targets.changed()=>{
                    if update.is_err(){return Ok(());}
                    desired=targets.borrow().iter().copied().collect();
                }
                _=sleep(Duration::from_millis(timeout_ms)), if !pending.is_empty()=>{
                    emit(&events,WsEvent::Gap{provider:name.clone(),reason:"subscribe_ack_timeout"});
                    break 'connected;
                }
                incoming=ws.next()=>{
                    let message=match incoming {
                        Some(Ok(Message::Text(text)))=>text,
                        Some(Ok(Message::Ping(_)))|Some(Ok(Message::Pong(_)))=>continue,
                        Some(Ok(_))=>continue,
                        _=>break 'connected,
                    };
                    let value:Value=match serde_json::from_str(&message) {
                        Ok(v)=>v,Err(_)=>break 'connected,
                    };
                    if let Some(id)=value.get("id").and_then(Value::as_u64) {
                        if let Some(address)=pending.remove(&id) {
                            if let Some(code)=value.pointer("/error/code").and_then(Value::as_i64) {
                                emit(&events,WsEvent::Rejected{provider:name.clone(),address,code});
                                emit(&events,WsEvent::Gap{provider:name.clone(),reason:"subscribe_rejected"});
                            }else if let Some(subscription)=value.get("result").and_then(Value::as_u64) {
                                active.insert(subscription,address);
                                emit(&events,WsEvent::Subscribed{provider:name.clone(),address});
                            }else {break 'connected;}
                        } else if let Some(address)=pending_unsub.remove(&id) {
                            if value.get("result").and_then(Value::as_bool)==Some(true) {
                                emit(&events,WsEvent::Unsubscribed{provider:name.clone(),address});
                            } else {
                                emit(&events,WsEvent::Gap{provider:name.clone(),reason:"unsubscribe_rejected"});
                            }
                        }
                        continue;
                    }
                    if value.get("method").and_then(Value::as_str)!=Some("accountNotification") {
                        break 'connected;
                    }
                    let subscription=value.pointer("/params/subscription").and_then(Value::as_u64);
                    let Some(address)=subscription.and_then(|id|active.get(&id).copied()) else {
                        if subscription.is_some_and(|id|retired.contains(&id)) {continue;}
                        emit(&events,WsEvent::Gap{provider:name.clone(),reason:"unknown_subscription"});
                        continue;
                    };
                    let decoded=(||->Result<(u64,Option<AccountData>)> {
                        let slot=value.pointer("/params/result/context/slot").and_then(Value::as_u64)
                            .context("WS notification missing slot")?;
                        let item=value.pointer("/params/result/value")
                            .context("WS notification missing value")?;
                        Ok((slot,decode_account(address,item,AccountView::Full)?))
                    })();
                    match decoded {
                        Ok((slot,payload))=>emit(&events,WsEvent::Account{
                            provider:name.clone(),address,slot,
                            received_at_ms:now_unix_ms()?,value:payload,
                        }),
                        Err(error)=>{
                            eprintln!("rpc websocket provider={name} decode_error={error:#}");
                            emit(&events,WsEvent::Gap{provider:name.clone(),reason:"malformed_account_notification"});
                            break 'connected;
                        }
                    }
                }
            }
        }
        emit(&events,WsEvent::Gap{provider:name.clone(),reason:"socket_disconnected"});
        tokio::select! {
            _=sleep(Duration::from_millis(reconnect_ms))=>{},
            change=targets.changed()=>if change.is_err(){return Ok(())},
        }
    }
}

async fn ws_run(settings:Vec<WsSettings>,commitment:String,mut targets:watch::Receiver<Vec<Pubkey>>,
    events:broadcast::Sender<WsEvent>)->Result<()> {
    let mut sockets=Vec::new();
    for provider in settings {
        let mut remaining=provider.max_subscriptions;
        for index in 0..provider.max_connections {
            if remaining==0{break;}
            let capacity=remaining.min(provider.per_connection);
            remaining-=capacity;
            let (tx,rx)=watch::channel(Vec::new());
            let name=format!("{}/socket-{}",provider.name,index+1);
            let events=events.clone();let commitment=commitment.clone();let endpoint=provider.url.clone();
            let spacing=provider.spacing_ms;let timeout=provider.timeout_ms;let reconnect=provider.reconnect_ms;
            let handle=tokio::spawn(async move{
                ws_session(name,endpoint,capacity,spacing,timeout,reconnect,commitment,rx,events).await
            });
            sockets.push((capacity,tx,handle));
        }
        ensure!(remaining==0,"WS socket budget insufficient");
    }
    let mut assignments:HashMap<Pubkey,usize>=HashMap::new();
    loop {
        let desired:BTreeSet<Pubkey>=targets.borrow_and_update().iter().copied().collect();
        let capacity:usize=sockets.iter().map(|(limit,_,_)|*limit).sum();
        ensure!(desired.len()<=capacity,"WS account capacity exceeded");
        let mut groups=vec![Vec::new();sockets.len()];
        let mut next=HashMap::with_capacity(desired.len());
        // Keep previous technical assignments for existing accounts.
        for address in &desired {
            if let Some(index)=assignments.get(address).copied() {
                if groups[index].len()<sockets[index].0 {
                    groups[index].push(*address);
                    next.insert(*address,index);
                }
            }
        }
        for address in desired {
            if next.contains_key(&address) {continue;}
            let index=groups.iter().enumerate().filter(|(i,group)|group.len()<sockets[*i].0)
                .min_by_key(|(_,group)|group.len()).map(|(i,_)|i)
                .context("WS socket distribution exhausted")?;
            groups[index].push(address);
            next.insert(address,index);
        }
        assignments=next;
        for ((_,tx,_),group) in sockets.iter().zip(groups) {
            if *tx.borrow()!=group {tx.send(group).ok();}
        }
        if targets.changed().await.is_err(){break;}
    }
    drop(sockets);
    Ok(())
}

pub fn spawn(settings:Settings)->Result<(RpcHandle,JoinHandle<Result<()>>)> {
    let (tx,rx)=mpsc::channel(256);
    let (watch_tx,watch_rx)=watch::channel(Vec::new());
    let (events,_)=broadcast::channel(4096);
    let capacity=settings.ws.iter().map(|cfg|cfg.max_subscriptions).sum();
    let capabilities=settings.http.iter().fold(0_u64,|mask,cfg|mask|cfg.capabilities);
    let handle=RpcHandle{requests:tx,targets:watch_tx,events:events.clone(),ws_capacity:capacity,capabilities};
    let task=tokio::spawn(async move {
        let commitment=settings.commitment.clone();
        let ws=settings.ws.clone();
        let ws_task=tokio::spawn(ws_run(ws,commitment,watch_rx,events));
        let result=http_run(settings,rx).await;
        ws_task.abort();
        let _=ws_task.await;
        result
    });
    Ok((handle,task))
}

/// Explicit real-provider smoke run; no synthetic data or hidden provider calls.
pub async fn live_probe(handle:&RpcHandle)->Result<()> {
    let address:Pubkey="So11111111111111111111111111111111111111112".parse()?;
    let mut passed=0_u64;
    for operation in [
        RpcOperation::GenesisHash,
        RpcOperation::Slot{commitment:"confirmed".to_owned()},
        RpcOperation::Account{address,view:AccountView::Full},
        RpcOperation::Multiple{addresses:vec![address],view:AccountView::Full,min_context_slot:None},
    ] {
        if !handle.supports(&operation) { continue; }
        let method=operation.method();
        let response=handle.call(operation).await.with_context(||format!("live RPC {method} failed"))?;
        println!("rpc live provider={} received_at_ms={}",response.provider,response.received_at_ms);
        match response.answer {
            RpcAnswer::GenesisHash(hash)=>println!("rpc live method={method} genesis_hash={hash}"),
            RpcAnswer::Slot(slot)=>println!("rpc live method={method} slot={slot}"),
            RpcAnswer::Account{slot,account}=>println!(
                "rpc live method={method} slot={slot} found={} bytes={}",
                account.is_some(),account.as_ref().map_or(0,|a|a.bytes.len())),
            RpcAnswer::Multiple{slot,accounts}=>println!(
                "rpc live method={method} slot={slot} returned={} found={}",
                accounts.len(),accounts.iter().filter(|a|a.is_some()).count()),
            _=>bail!("unexpected RPC live response for {method}"),
        }
        passed+=1;
    }
    let program:Pubkey="ComputeBudget111111111111111111111111111111".parse()?;
    for operation in [
        RpcOperation::Program{program,filters:vec![AccountFilter::DataSize(0)],view:AccountView::Slice{offset:0,length:0}},
        RpcOperation::ProgramV2{program,filters:vec![AccountFilter::DataSize(0)],
            view:AccountView::Slice{offset:0,length:0},cursor:None,changed_since_slot:None,limit:10},
    ] {
        if !handle.supports(&operation) { continue; }
        let method=operation.method();
        let response=handle.call(operation).await.with_context(||format!("live RPC {method} failed"))?;
        println!("rpc live provider={} received_at_ms={}",response.provider,response.received_at_ms);
        match response.answer {
            RpcAnswer::Program{accounts,next_cursor}=>println!(
                "rpc live method={method} accounts={} next_cursor_present={}",
                accounts.len(),next_cursor.is_some()),
            _=>bail!("unexpected RPC live response for {method}"),
        }
        passed+=1;
    }
    println!("rpc live http_methods_passed={passed}");
    ensure!(passed>0 || handle.can_watch(),"no RPC HTTP method or WebSocket watch enabled");

    if handle.can_watch() {
        let mut events=handle.subscribe_events();
        handle.replace_watches(vec![address])?;
        let observed=timeout(Duration::from_secs(12),async {
            loop {
                match events.recv().await {
                    Ok(WsEvent::Subscribed{provider,address})=>{
                        println!("rpc live websocket subscribed provider={provider} account={address}");
                        return Ok(());
                    }
                    Ok(WsEvent::Account{provider,address,slot,value,received_at_ms})=>{
                        println!("rpc live websocket update provider={provider} address={address} slot={slot} received_at_ms={received_at_ms} found={}",value.is_some());
                    }
                    Ok(WsEvent::Gap{provider,reason})=>
                        eprintln!("rpc live websocket gap provider={provider} reason={reason}"),
                    Ok(WsEvent::Rejected{provider,address,code})=>
                        bail!("rpc live websocket subscription rejected provider={provider} address={address} code={code}"),
                    Ok(_)=>{},
                    Err(error)=>bail!("rpc live WebSocket event stream failed: {error}"),
                }
            }
        }).await;
        handle.replace_watches(Vec::new())?;
        observed.context("WebSocket subscription ACK timeout")??;
    } else {
        println!("rpc live websocket not configured or not enabled");
    }
    Ok(())
}
