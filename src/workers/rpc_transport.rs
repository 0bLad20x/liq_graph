//! Unified, protocol-neutral Solana RPC boundary: multi-provider HTTP and account WebSockets.
//! All rate limits, provider capabilities, retries and subscription state live here.
//! No venue/account decoding beyond JSON-RPC and base64 transport framing.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    env,
    sync::{Arc, Mutex as StdMutex, atomic::{AtomicU64, AtomicUsize, Ordering}},
    time::Duration,
};
use anyhow::{anyhow, bail, ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};
use solana_pubkey::Pubkey;
use tokio::{
    sync::{broadcast, mpsc, watch, Mutex, Semaphore},
    task::JoinHandle,
    time::{sleep, sleep_until, Instant},
};

use crate::core::now_unix_ms;

use crate::core::rpc::{AccountData,AccountView,AccountFilter,RpcOperation,RpcAnswer,RpcResponse,RpcHandle,Call};
use crate::core::account_stream::AccountStreamControl;

impl RpcOperation {
    fn method(&self) -> &'static str {
        match self {
            Self::Multiple { .. } => "getMultipleAccounts",
            Self::Account { .. } => "getAccountInfo",
            Self::Program { .. } => "getProgramAccounts",
            Self::ProgramV2 { .. } => "getProgramAccountsV2",
            Self::Slot { .. } => "getSlot",
            Self::GenesisHash => "getGenesisHash",
            Self::LatestBlockhash { .. } => "getLatestBlockhash",
            Self::Simulate { .. } | Self::SimulateFast { .. } => "simulateTransaction",
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
            Self::LatestBlockhash { min_context_slot } => {
                let mut cfg=json!({"commitment":commitment});
                if let Some(slot)=min_context_slot {cfg["minContextSlot"]=json!(slot);}
                json!([cfg])
            }
            Self::Simulate { transaction, min_context_slot, accounts }
            | Self::SimulateFast { transaction, min_context_slot, accounts } => {
                ensure!(!transaction.is_empty() && transaction.len()<=4096,
                    "simulation transaction exceeds wire limit");
                ensure!(accounts.len()<=64,"too many simulation account projections");
                let fast=matches!(self,Self::SimulateFast{..});
                let mut cfg=json!({"encoding":"base64","commitment":commitment,
                    "sigVerify":!fast,"replaceRecentBlockhash":fast,"innerInstructions":true,
                    "accounts":{"encoding":"base64",
                        "addresses":accounts.iter().map(ToString::to_string).collect::<Vec<_>>()}});
                if let Some(slot)=min_context_slot {cfg["minContextSlot"]=json!(slot);}
                json!([STANDARD.encode(transaction),cfg])
            }
        })
    }
}

#[derive(Clone, Debug)]
struct HttpSettings {
    name:String, url:String, rps:u32, max_in_flight:usize, timeout_ms:u64,
    retries:usize, capabilities:u64,
}
#[derive(Clone, Debug)]
struct WsSettings {
    index:usize, name:String, url:String, max_connections:usize, max_subscriptions:usize,
    per_connection:usize, spacing_ms:u64, timeout_ms:u64, reconnect_ms:u64,
}
#[derive(Clone, Debug)]
pub struct Settings {
    http:Vec<HttpSettings>, ws:Vec<WsSettings>, commitment:String,
    account_batch_size:usize, v2_page_limit:usize,
    ws_selection:String, ws_account_budget:Option<usize>,
}

/// Read-only effective transport configuration. Never includes endpoint URLs or credentials.
#[derive(Clone, Debug, serde::Serialize)]
pub struct WsProviderPlan {
    pub index:usize, pub name:String, pub max_connections:usize, pub max_subscriptions:usize,
    pub subscriptions_per_connection:usize, pub subscribe_spacing_ms:u64,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct WsPlan {
    pub selection:String, pub account_capacity:usize, pub socket_count:usize,
    pub test_account_budget:Option<usize>, pub providers:Vec<WsProviderPlan>,
}
impl Settings {
    pub fn websocket_plan(&self)->Result<WsPlan> {
        let account_capacity=self.ws.iter().try_fold(0_usize,|sum,p|
            sum.checked_add(p.max_subscriptions).context("WebSocket account capacity overflow"))?;
        let socket_count=self.ws.iter().try_fold(0_usize,|sum,p|
            sum.checked_add(p.max_subscriptions.div_ceil(p.per_connection))
                .context("WebSocket socket count overflow"))?;
        Ok(WsPlan{selection:self.ws_selection.clone(),account_capacity,socket_count,
            test_account_budget:self.ws_account_budget,providers:self.ws.iter().map(|p|WsProviderPlan{
                index:p.index,name:p.name.clone(),max_connections:p.max_connections,max_subscriptions:p.max_subscriptions,
                subscriptions_per_connection:p.per_connection,subscribe_spacing_ms:p.spacing_ms}).collect()})
    }
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
    let requested=env::var("RPC_WS_PROVIDER_SELECTION").unwrap_or_else(|_|"configured".into());
    let requested=requested.trim().to_ascii_lowercase();
    let mut selected=BTreeSet::new();
    if requested!="configured" && requested!="all" {
        ensure!(!requested.is_empty(),"RPC_WS_PROVIDER_SELECTION must be Configured, All or comma-separated provider indices");
        for item in requested.split(',') {
            let index:usize=item.trim().parse().context("invalid RPC_WS_PROVIDER_SELECTION index")?;
            ensure!(index>0 && selected.insert(index),"RPC_WS_PROVIDER_SELECTION requires distinct positive indices");
        }
    }
    let ws_selection=if selected.is_empty(){requested.clone()}else{
        selected.iter().map(ToString::to_string).collect::<Vec<_>>().join(",")};
    let ws_budget:usize=var("RPC_WS_ACCOUNT_BUDGET",0)?;
    let connections_override:usize=var("RPC_WS_CONNECTIONS_PER_PROVIDER",0)?;
    ensure!(connections_override<=64 && (connections_override==0 || ws_budget>0),
        "RPC_WS_CONNECTIONS_PER_PROVIDER must be 0..64; a nonzero value requires RPC_WS_ACCOUNT_BUDGET");
    let mut selected_found=BTreeSet::new();
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
                ("GET_LATEST_BLOCKHASH",64,true),
                ("SIMULATE_TRANSACTION",128,true),
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
            let enabled=match requested.as_str(){"configured"=>flag(&format!("{prefix}WS_ACCOUNT_SUBSCRIBE"),false)?,
                "all"=>true,_=>selected.contains(&index)};
            if enabled {
                selected_found.insert(index);
                let connections:usize=if connections_override>0 {connections_override}
                    else{var(&format!("{prefix}WS_MAX_CONNECTIONS"),1)?};
                // An explicit aggregate test budget is distributed after all selected providers are known.
                let maximum:usize=if ws_budget>0 {1}else{var(&format!("{prefix}WS_MAX_SUBSCRIPTIONS"),1)?};
                let per_connection:usize=if ws_budget>0 {1}else{var(&format!("{prefix}WS_SUBSCRIPTIONS_PER_CONNECTION"),1)?};
                let spacing_ms:u64=var(&format!("{prefix}WS_SUBSCRIBE_SPACING_MS"),10)?;
                let timeout_ms:u64=var(&format!("{prefix}WS_TIMEOUT_MS"),45000)?;
                let reconnect_ms:u64=var(&format!("{prefix}WS_RECONNECT_MS"),1000)?;
                ensure!(connections>0 && connections<=64 && maximum>0 && per_connection>0
                    && connections.checked_mul(per_connection).is_some_and(|n|n>=maximum)
                    && timeout_ms>0 && reconnect_ms>0,
                    "invalid WebSocket limits for provider {name}");
                ws.push(WsSettings{index,name,url:endpoint,max_connections:connections,max_subscriptions:maximum,
                    per_connection,spacing_ms,timeout_ms,reconnect_ms});
            }
        }
    }
    if !selected.is_empty() {
        let missing:Vec<_>=selected.difference(&selected_found).copied().collect();
        ensure!(missing.is_empty(),"selected WebSocket providers have no configured WS_URL: {missing:?}");
    }
    if ws_budget>0 {
        ensure!(!ws.is_empty(),"RPC_WS_ACCOUNT_BUDGET requires at least one selected WebSocket provider");
        ensure!(ws_budget>=ws.len(),"RPC_WS_ACCOUNT_BUDGET must cover at least one account per selected provider");
        let count=ws.len();
        for (i,p) in ws.iter_mut().enumerate() {
            p.max_subscriptions=ws_budget/count+usize::from(i<ws_budget%count);
            p.per_connection=p.max_subscriptions.div_ceil(p.max_connections);
        }
    }
    ensure!(!http.is_empty() || !ws.is_empty(),"no usable RPC HTTP or WS provider configured");
    let commitment=env::var("RPC_COMMITMENT").unwrap_or_else(|_|"confirmed".to_owned());
    ensure!(valid_commitment(&commitment),"invalid RPC_COMMITMENT");
    let account_batch_size:usize=var("RPC_ACCOUNT_BATCH_SIZE",100)?;
    let v2_page_limit:usize=var("RPC_V2_PAGE_LIMIT",5000)?;
    ensure!((1..=100).contains(&account_batch_size),"invalid RPC_ACCOUNT_BATCH_SIZE");
    ensure!((1..=5000).contains(&v2_page_limit),"invalid RPC_V2_PAGE_LIMIT");
    Ok(Settings{http,ws,commitment,account_batch_size,v2_page_limit,ws_selection,
        ws_account_budget:(ws_budget>0).then_some(ws_budget)})
}

struct HttpProvider {
    cfg:HttpSettings, client:reqwest::Client,
    gate:Mutex<Instant>, concurrent:Semaphore,
    cooldown:Mutex<Option<Instant>>, request_id:AtomicU64,
    disabled:AtomicU64,
    // Assigned requests include requests still waiting for HTTP RPS/concurrency.
    pending:AtomicUsize,
    // Actual HTTP attempt duration, excluding transport scheduling and retries.
    multiple_latency_ms:AtomicU64,
    other_latency_ms:AtomicU64,
    multiple_wait_ms:AtomicU64,
    other_wait_ms:AtomicU64,
}
impl HttpProvider {
    fn new(cfg:HttpSettings)->Result<Self> {
        let client=reqwest::Client::builder()
            .timeout(Duration::from_millis(cfg.timeout_ms)).build()?;
        Ok(Self{concurrent:Semaphore::new(cfg.max_in_flight),
            gate:Mutex::new(Instant::now()),cooldown:Mutex::new(None),
            request_id:AtomicU64::new(1),disabled:AtomicU64::new(0),
            pending:AtomicUsize::new(0),
            multiple_latency_ms:AtomicU64::new(0),other_latency_ms:AtomicU64::new(0),
            multiple_wait_ms:AtomicU64::new(0),other_wait_ms:AtomicU64::new(0),
            client,cfg})
    }
    fn eligible(&self,bit:u64)->bool {
        self.cfg.capabilities&bit!=0 && self.disabled.load(Ordering::Relaxed)&bit==0
    }
    fn schedulable(&self,bit:u64)->bool {
        self.eligible(bit) && self.cooldown.try_lock().map_or(true,|guard|
            guard.as_ref().is_none_or(|until|*until<=Instant::now()))
    }
    fn latency_cell(&self,bit:u64)->&AtomicU64 {
        if bit==1 {&self.multiple_latency_ms} else {&self.other_latency_ms}
    }
    fn observe_gate(&self,bit:u64,duration:Duration) {
        let ms=duration.as_millis().min(120_000) as u64;
        let cell=if bit==1 {&self.multiple_wait_ms}else{&self.other_wait_ms};
        let _=cell.fetch_update(Ordering::Relaxed,Ordering::Relaxed,|old|
            Some(if old==0 {ms}else{old.saturating_mul(7).saturating_add(ms)/8}));
    }
    fn observe_http(&self,bit:u64,duration:Duration) {
        let ms=duration.as_millis().clamp(1,120_000) as u64;
        let cell=self.latency_cell(bit);
        let _=cell.fetch_update(Ordering::Relaxed,Ordering::Relaxed,|old|
            Some(if old==0 {ms} else {old.saturating_mul(7).saturating_add(ms)/8}));
    }
    /// Predicted queue completion cost: account for both per-provider RPS and
    /// actual concurrency-limited HTTP response time, not a round-robin slot.
    fn estimated_queue_ms(&self,bit:u64)->u128 {
        let assigned=(self.pending.load(Ordering::Relaxed) as u128).saturating_add(1);
        let observed=self.latency_cell(bit).load(Ordering::Relaxed);
        let latency=if observed==0 {1_000_u128} else {u128::from(observed)};
        let service=(latency*assigned).div_ceil(self.cfg.max_in_flight as u128);
        let rate=(1_000_u128*assigned).div_ceil(self.cfg.rps as u128);
        service.max(rate)
    }
    async fn request(&self,operation:&RpcOperation,params:&Value)->Result<Value> {
        let method=operation.method();
        let mut previous=None;
        for attempt in 0..self.cfg.retries {
            if self.cooldown.lock().await.is_some_and(|until|until>Instant::now()) {
                bail!("provider {} in HTTP 429 cooldown",self.cfg.name);
            }
            if attempt>0 {sleep(Duration::from_millis(200_u64.saturating_mul(1 << attempt.min(7)))).await;}
            let wait_started=Instant::now();
            let _permit=self.concurrent.acquire().await?;
            // Reserve a distinct request start slot, releasing the gate mutex
            // before sleeping. The semaphore remains held so delayed slots
            // cannot turn into a burst above the configured RPS.
            let due={
                let mut next=self.gate.lock().await;
                let due=(*next).max(Instant::now());
                *next=due+Duration::from_secs_f64(1.0/f64::from(self.cfg.rps));
                due
            };
            sleep_until(due).await;
            self.observe_gate(operation.bit(),wait_started.elapsed());
            let id=self.request_id.fetch_add(1,Ordering::Relaxed);
            let sent=Instant::now();
            let outcome=self.client.post(&self.cfg.url)
                .json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
                .send().await;
            let response=match outcome {
                Ok(value)=>value,
                Err(error)=>{
                    self.observe_http(operation.bit(),sent.elapsed());
                    previous=Some(anyhow!("transport error: {}",error.without_url()));
                    continue;
                }
            };
            if response.status()==reqwest::StatusCode::TOO_MANY_REQUESTS {
                self.observe_http(operation.bit(),sent.elapsed());
                let retry_after=response.headers().get(reqwest::header::RETRY_AFTER)
                    .and_then(|s|s.to_str().ok()).and_then(|s|s.parse::<u64>().ok())
                    .unwrap_or(2).max(1);
                *self.cooldown.lock().await=Instant::now().checked_add(Duration::from_secs(retry_after));
                bail!("HTTP 429 provider={} cooldown_s={retry_after}",self.cfg.name);
            }
            if !response.status().is_success() {
                self.observe_http(operation.bit(),sent.elapsed());
                let status=response.status();
                if status.is_server_error() {
                    previous=Some(anyhow!("HTTP {status} at provider {}",self.cfg.name));
                    continue;
                }
                bail!("HTTP {status} at provider {}",self.cfg.name);
            }
            let body=match response.bytes().await {
                Ok(body)=>body,
                Err(error)=>{
                    self.observe_http(operation.bit(),sent.elapsed());
                    // Body timeout usually means an overloaded provider; let
                    // the transport fail over instead of repeating slow reads.
                    return Err(anyhow!("RPC HTTP response body: {error}"));
                }
            };
            self.observe_http(operation.bit(),sent.elapsed());
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
    let view=if let AccountView::Slice{length,..}=view {
        if bytes.len()!=length && space==Some(bytes.len()) && !bytes.is_empty() {AccountView::Full}else{view}
    }else{view};
    let original_len=match view {
        AccountView::Full=>{
            ensure!(space.is_none_or(|n|n==bytes.len()),"RPC full account length mismatch");
            Some(bytes.len())
        }
        AccountView::Slice{..}=>space,
    };
    let executable=value.get("executable").and_then(Value::as_bool)
        .context("RPC executable flag missing")?;
    Ok(Some(AccountData{address,owner,executable,bytes,view,original_len}))
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
        RpcOperation::LatestBlockhash{min_context_slot}=>{
            let slot=response_slot(&result)?;
            ensure!(min_context_slot.is_none_or(|min|slot>=min),"blockhash context is older than requested");
            let value=result.get("value").context("blockhash value missing")?;
            Ok(RpcAnswer::LatestBlockhash{slot,
                blockhash:value.get("blockhash").and_then(Value::as_str)
                    .context("blockhash missing")?.to_owned(),
                last_valid_block_height:value.get("lastValidBlockHeight").and_then(Value::as_u64)
                    .context("blockhash expiry missing")?})
        }
        RpcOperation::Simulate{min_context_slot,..}|RpcOperation::SimulateFast{min_context_slot,..}=>{
            let slot=response_slot(&result)?;
            ensure!(min_context_slot.is_none_or(|min|slot>=min),"simulation context is older than requested");
            let value=result.get("value").filter(|v|v.is_object() && v.get("err").is_some())
                .context("simulation value/err missing")?.clone();
            Ok(RpcAnswer::Simulation{slot,value})
        }
    }
}

#[derive(Clone, Default)]
struct RpcCallStats {
    calls:u64, succeeded:u64, total_ms:u128, max_ms:u128,
    http_401:u64, http_403:u64, http_429:u64,
    rpc_32602:u64, rpc_32016:u64, transport:u64, invalid_response:u64, other:u64,
}

type RpcStats = Arc<StdMutex<HashMap<(String, &'static str), RpcCallStats>>>;
#[derive(Default)]
struct RpcDispatchWait { calls:u64,total_us:u128,max_us:u128 }
type RpcDispatchStats=Arc<StdMutex<HashMap<&'static str,RpcDispatchWait>>>;
fn record_dispatch(stats:&RpcDispatchStats,method:&'static str,queued_at:Instant) {
    let elapsed=queued_at.elapsed().as_micros();
    let mut locked=stats.lock().expect("RPC dispatch metrics poisoned");
    let entry=locked.entry(method).or_default();
    entry.calls+=1;entry.total_us+=elapsed;entry.max_us=entry.max_us.max(elapsed);
}
fn report_dispatch(stats:&RpcDispatchStats) {
    let guard=stats.lock().expect("RPC dispatch metrics poisoned");
    let mut rows=guard.iter().collect::<Vec<_>>();
    rows.sort_unstable_by_key(|(method,_)|*method);
    for (method,v) in rows {
        crate::log_event!(Info,"rpc dispatch method={} calls={} mean_queue_us={} max_queue_us={}",
            method,v.calls,v.total_us/u128::from(v.calls),v.max_us);
    }
}


fn record_rpc(stats:&RpcStats, provider:&str, method:&'static str,
    elapsed:Duration, failure:Option<&str>) {
    let mut guard=stats.lock().expect("RPC statistics lock poisoned");
    let item=guard.entry((provider.to_owned(),method)).or_default();
    item.calls+=1;
    let millis=elapsed.as_millis();
    item.total_ms+=millis;
    item.max_ms=item.max_ms.max(millis);
    match failure {
        None=>item.succeeded+=1,
        Some(reason) if reason.contains("HTTP 401")=>item.http_401+=1,
        Some(reason) if reason.contains("HTTP 403")=>item.http_403+=1,
        Some(reason) if reason.contains("HTTP 429")=>item.http_429+=1,
        Some(reason) if reason.contains("code=-32602")=>item.rpc_32602+=1,
        Some(reason) if reason.contains("code=-32016")=>item.rpc_32016+=1,
        Some(reason) if reason.contains("transport error")=>item.transport+=1,
        Some("response_validation")=>item.invalid_response+=1,
        Some(_)=>item.other+=1,
    }
}

fn report_rpc(stats:&RpcStats) {
    let mut rows=stats.lock().expect("RPC statistics lock poisoned").iter()
        .map(|((provider,method),values)|(provider.clone(),*method,values.clone()))
        .collect::<Vec<_>>();
    rows.sort_unstable_by(|a,b|(&a.0,a.1).cmp(&(&b.0,b.1)));
    for (provider,method,v) in rows {
        let failed=v.calls-v.succeeded;
        let success_pct=100.0*(v.succeeded as f64)/(v.calls as f64);
        let avg_ms=v.total_ms/u128::from(v.calls);
        crate::log_event!(Info,"rpc stats provider={provider} method={method} calls={} ok={} failed={failed} ok_pct={success_pct:.1} avg_ms={avg_ms} max_ms={} http_401={} http_403={} http_429={} rpc_32602={} rpc_32016={} transport={} invalid_response={} other={}",
            v.calls,v.succeeded,v.max_ms,v.http_401,v.http_403,v.http_429,
            v.rpc_32602,v.rpc_32016,v.transport,v.invalid_response,v.other);
    }
}

/// A per-request assignment is tracked from dispatch through response or task
/// cancellation. This prevents a slow provider from accumulating an invisible
/// FIFO queue while faster configured providers have spare capacity.
struct ProviderLease { provider:Arc<HttpProvider> }
impl ProviderLease {
    fn new(provider:Arc<HttpProvider>)->Self {
        provider.pending.fetch_add(1,Ordering::Relaxed);
        Self{provider}
    }
}
impl Drop for ProviderLease {
    fn drop(&mut self) {
        self.provider.pending.fetch_sub(1,Ordering::Relaxed);
    }
}
fn reserve_provider(
    eligible:&[Arc<HttpProvider>], attempted:&[bool], cursor:usize, bit:u64,
)->Option<(usize,ProviderLease)> {
    let count=eligible.len();
    let (idx,provider)=eligible.iter().enumerate()
        .filter(|(i,p)| !attempted[*i] && p.schedulable(bit))
        .min_by_key(|(i,p)| (
            p.estimated_queue_ms(bit),
            (*i+count-(cursor%count))%count,
        ))?;
    Some((idx,ProviderLease::new(provider.clone())))
}

async fn http_run(settings:Settings, mut rx:mpsc::Receiver<Call>)->Result<()> {
    let providers=settings.http.into_iter().map(HttpProvider::new)
        .map(|r|r.map(Arc::new)).collect::<Result<Vec<_>>>()?;
    let enabled_multiple=providers.iter().filter(|p|p.eligible(1)).collect::<Vec<_>>();
    let total_rps: u64=enabled_multiple.iter().map(|p|u64::from(p.cfg.rps)).sum();
    let total_parallel:usize=enabled_multiple.iter().map(|p|p.cfg.max_in_flight).sum();
    crate::log_event!(Info,
        "rpc http effective_plan providers={} get_multiple_accounts={} configured_multiple_rps={} configured_multiple_parallel={} weighted_queue=true",
        providers.len(),enabled_multiple.len(),total_rps,total_parallel);
    for p in &providers {
        crate::log_event!(Info,
            "rpc http provider={} multiple={} rps={} max_in_flight={} timeout_ms={} retries={}",
            p.cfg.name,p.eligible(1),p.cfg.rps,p.cfg.max_in_flight,
            p.cfg.timeout_ms,p.cfg.retries);
    }
    let mut tasks=tokio::task::JoinSet::new();
    let next=Arc::new(AtomicUsize::new(0));
    let stats:RpcStats=Arc::new(StdMutex::new(HashMap::new()));
    let dispatch:RpcDispatchStats=Arc::new(StdMutex::new(HashMap::new()));
    let stats_interval:u64=var("RPC_STATS_INTERVAL_SECONDS",30)?;
    ensure!((1..=3600).contains(&stats_interval), "RPC_STATS_INTERVAL_SECONDS must be 1..=3600");
    let report_stats=stats.clone();
    let report_dispatch_stats=dispatch.clone();
    let report_providers=providers.clone();
    let reporter=tokio::spawn(async move {
        let mut ticker=tokio::time::interval(Duration::from_secs(stats_interval));
        ticker.tick().await;
        loop {
            ticker.tick().await;
            report_rpc(&report_stats);
            report_dispatch(&report_dispatch_stats);
            for p in &report_providers {
                crate::log_event!(Info,
                    "rpc provider_load provider={} assigned={} multiple_http_ema_ms={} other_http_ema_ms={} rps={} max_in_flight={} multiple_enabled={} multiple_wait_ema_ms={} other_wait_ema_ms={}",
                    p.cfg.name,p.pending.load(Ordering::Relaxed),
                    p.multiple_latency_ms.load(Ordering::Relaxed),
                    p.other_latency_ms.load(Ordering::Relaxed),
                    p.cfg.rps,p.cfg.max_in_flight,p.eligible(1),
                    p.multiple_wait_ms.load(Ordering::Relaxed),
                    p.other_wait_ms.load(Ordering::Relaxed));
            }
        }
    });
    while let Some(Call{operation,reply,queued_at})=rx.recv().await {
        record_dispatch(&dispatch,operation.method(),queued_at);
        let bit=operation.bit();
        let eligible:Vec<_>=providers.iter().filter(|provider|provider.eligible(bit))
            .cloned().collect();
        let params=operation.params(&settings.commitment,settings.account_batch_size,settings.v2_page_limit);
        let cursor=next.fetch_add(1,Ordering::Relaxed);
        // Reserve the initial provider before spawning: otherwise a burst of
        // new calls would all see the same zero-load selection.
        let mut attempted=vec![false;eligible.len()];
        let first=reserve_provider(&eligible,&attempted,cursor,bit);
        let stats=stats.clone();
        tasks.spawn(async move {
            let answer=async {
                let params=params?;
                ensure!(!eligible.is_empty(),"no configured RPC provider supports {}",operation.method());
                let mut error=None;
                let mut ticket=first;
                while let Some((index,lease))=ticket {
                    attempted[index]=true;
                    let provider=lease.provider.clone();
                    let started=Instant::now();
                    let result=provider.request(&operation,&params).await;
                    // Drop the assignment before selecting a failover provider.
                    drop(lease);
                    match result {
                        Ok(value)=>match interpret(operation.clone(),value) {
                            Ok(answer)=>{
                                record_rpc(&stats,&provider.cfg.name,operation.method(),started.elapsed(),None);
                                return Ok(RpcResponse {
                                    provider:provider.cfg.name.clone(),
                                    received_at_ms:now_unix_ms()?,
                                    answer,
                                });
                            }
                            Err(invalid)=>{
                                record_rpc(&stats,&provider.cfg.name,operation.method(),started.elapsed(),Some("response_validation"));
                                crate::log_event!(Warn,"rpc provider={} method={} response_validation_error={invalid:#}",
                                    provider.cfg.name,operation.method());
                                error=Some(invalid);
                            }
                        },
                        Err(err)=>{
                            let reason=format!("{err:#}");
                            record_rpc(&stats,&provider.cfg.name,operation.method(),started.elapsed(),Some(&reason));
                            crate::log_event!(Warn,"rpc provider={} method={} error={err:#}",provider.cfg.name,operation.method());
                            error=Some(err);
                        }
                    }
                    ticket=reserve_provider(&eligible,&attempted,cursor,bit);
                }
                Err(error.unwrap_or_else(||anyhow!("no healthy eligible RPC provider")))
            }.await;
            let _=reply.send(answer);
        });
        while tasks.len()>1024 {
            let _=tasks.join_next().await;
        }
    }
    // The request channel closes only after every RpcHandle has been dropped.
    // At that point no caller can consume an outstanding response. Cancel the
    // remaining in-flight/queued provider calls rather than waiting for their
    // rate-limit gates and timeouts during an otherwise graceful shutdown.
    let abandoned = tasks.len();
    tasks.abort_all();
    while let Some(result)=tasks.join_next().await {
        if let Err(error)=result {
            if !error.is_cancelled() { return Err(error.into()); }
        }
    }
    reporter.abort();
    report_rpc(&stats);
    report_dispatch(&dispatch);
    crate::log_event!(Info,"rpc transport shutdown complete=true abandoned_calls={abandoned}");
    Ok(())
}

pub fn spawn(settings:Settings)->Result<(RpcHandle,JoinHandle<Result<()>>)> {
    let ws_plan=settings.websocket_plan()?;
    crate::log_event!(Info,"rpc websocket effective_plan={}",serde_json::to_string(&ws_plan)?);
    let (tx,rx)=mpsc::channel(256);
    let (targets,target_rx)=watch::channel(Vec::new());
    let (events,_)=broadcast::channel(4096);
    let capacity=ws_plan.account_capacity;
    let capabilities=settings.http.iter().fold(0_u64,|mask,cfg|mask|cfg.capabilities);
    let stream=AccountStreamControl{targets,capacity,messages:Arc::new(AtomicU64::new(0)),text_bytes:Arc::new(AtomicU64::new(0))};
    let handle=RpcHandle{requests:tx,stream:stream.clone(),events:events.clone(),
        account_batch_size:settings.account_batch_size,capabilities};
    let task=tokio::spawn(async move {
        let websocket=ws::run(settings.ws.clone(),settings.commitment.clone(),target_rx,events,stream);
        tokio::pin!(websocket);
        tokio::select! {
            result=http_run(settings,rx)=>result,
            result=&mut websocket=>{
                result?;
                bail!("account stream ended before RPC shutdown")
            },
        }
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
        crate::log_event!(Info,"rpc live provider={} received_at_ms={}",response.provider,response.received_at_ms);
        match response.answer {
            RpcAnswer::GenesisHash(hash)=>crate::log_event!(Debug,"rpc live method={method} genesis_hash={hash}"),
            RpcAnswer::Slot(slot)=>crate::log_event!(Debug,"rpc live method={method} slot={slot}"),
            RpcAnswer::Account{slot,account}=>crate::log_event!(Debug,
                "rpc live method={method} slot={slot} found={} bytes={}",
                account.is_some(),account.as_ref().map_or(0,|a|a.bytes.len())),
            RpcAnswer::Multiple{slot,accounts}=>crate::log_event!(Debug,
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
        crate::log_event!(Info,"rpc live provider={} received_at_ms={}",response.provider,response.received_at_ms);
        match response.answer {
            RpcAnswer::Program{accounts,next_cursor}=>crate::log_event!(Debug,
                "rpc live method={method} accounts={} next_cursor_present={}",
                accounts.len(),next_cursor.is_some()),
            _=>bail!("unexpected RPC live response for {method}"),
        }
        passed+=1;
    }
    crate::log_event!(Info,"rpc live http_methods_passed={passed}");
    ensure!(passed>0,"no enabled RPC HTTP method available for live probe");
    Ok(())
}

// Private transport internals: part of the same RPC worker, not a separate worker.
mod ws {
//! Transport framing and socket lifecycle. The reader never awaits the writer or a consumer.
use std::{borrow::Cow,collections::{BTreeMap,HashMap,HashSet,VecDeque},sync::{Arc,atomic::Ordering},time::{Duration,SystemTime,UNIX_EPOCH}};
use anyhow::{Result,Context,bail,ensure};
use base64::{engine::general_purpose::STANDARD,Engine as _};
use futures_util::{SinkExt,StreamExt};
use serde::Deserialize;
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast,mpsc,watch},task::JoinSet,time::{self,Instant,timeout,sleep,sleep_until}};
use tokio_tungstenite::{connect_async,tungstenite::Message};
use crate::core::account_stream::{AccountData,AccountEvent,AccountStreamControl,AccountView,AccountWatch};
use super::WsSettings;

pub(super) fn now_us()->u64 {SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64}
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
enum Pending {Subscribe(AccountWatch),Unsubscribe{subscription:u64,address:Pubkey}}

async fn session(cfg:WsSettings,name:String,capacity:usize,commitment:String,
    mut targets:watch::Receiver<Vec<AccountWatch>>,events:broadcast::Sender<AccountEvent>,
    control:AccountStreamControl)->Result<()> {
    let mut sessions=0_u64;let mut slices=true;let mut failures=0_u32;
    let emit=|e|{let _=events.send(e);};
    let gap=|reason| {crate::log_event!(Warn,"rpc websocket gap source={name} reason={reason}");emit(AccountEvent::Gap{provider:name.clone(),reason});};
    loop {
        let empty=targets.borrow().is_empty();
        if empty {if targets.changed().await.is_err(){return Ok(());}continue;}
        let connection=timeout(Duration::from_millis(cfg.timeout_ms),connect_async(cfg.url.as_str())).await;
        let socket=match connection {
            Ok(Ok((socket,_)))=>socket,
            failure=>{
                let reason=match failure {
                    Err(_)=>"connect_timeout",
                    Ok(Err(tokio_tungstenite::tungstenite::Error::Http(response))) if response.status().as_u16()==429=>"handshake_http_429",
                    _=>"connect_error",
                };
                gap(reason);failures=failures.saturating_add(1);
                let delay=cfg.reconnect_ms.saturating_mul(1_u64<<failures.min(6)).min(60000);
                tokio::select! {_=sleep(Duration::from_millis(delay))=>{},r=targets.changed()=>if r.is_err(){return Ok(());}}
                continue;
            }
        };
        if sessions>0 {gap("reconnect_requires_resync");}
        sessions+=1;failures=0;let session=sessions;
        let (mut sink,mut reader)=socket.split();
        // <=32 pending RPC requests; a separate bounded writer also carries heartbeat/flush work.
        let (commands,mut outgoing)=mpsc::channel::<Write>(64);
        let writer=async {
            let mut next=Instant::now();
            while let Some(command)=outgoing.recv().await {
                let paced=matches!(&command,Write::Message(_,true));
                if paced {sleep_until(next).await;}
                timeout(Duration::from_millis(cfg.timeout_ms),async {
                    match command {Write::Message(m,_)=>sink.send(m).await,Write::Flush=>sink.flush().await}
                }).await.context("WebSocket write timed out")??;
                if paced {next=Instant::now()+Duration::from_millis(cfg.spacing_ms);}
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
        'connected:loop {
            if dirty {
                unsubscribe=active.iter().filter(|(_,w)|!desired.contains_key(&w.address)).map(|(&sub,w)|(sub,w.address)).collect();
                subscribe=desired.iter().filter(|(a,_)|!busy.contains(*a)).map(|(&address,&view)|AccountWatch{address,view}).collect();
                dirty=false;
            }
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
                    gap("control_queue_full");break 'connected;
                }
                match &request {
                    Pending::Subscribe(w)=>{busy.insert(w.address);pending_subscriptions+=1;},
                    Pending::Unsubscribe{subscription,..}=>{active.remove(subscription);retired.insert(*subscription);},
                }
                pending.insert(id,request);deadlines.push_back((id,Instant::now()+Duration::from_millis(cfg.timeout_ms)));
            }
            while deadlines.front().is_some_and(|(id,_)|!pending.contains_key(id)){deadlines.pop_front();}
            let deadline=deadlines.front().map(|(_,at)|*at).unwrap_or_else(||Instant::now()+Duration::from_secs(86400));
            tokio::select! {
                result=&mut writer=>{
                    if let Err(error)=&result {crate::log_event!(Warn,"rpc websocket writer source={name} error={error:#}");}
                    gap(if result.is_err(){"socket_write_failed"}else{"socket_writer_closed"});break;
                },
                _=heartbeat.tick()=>if commands.try_send(Write::Message(Message::Ping(Vec::new().into()),false)).is_err(){gap("control_queue_full");break;},
                _=sleep_until(deadline),if !deadlines.is_empty()=>{gap("subscription_reply_timeout");break;},
                change=targets.changed()=>{
                    if change.is_err(){return Ok(());}
                    desired=targets.borrow_and_update().iter().map(|w|(w.address,w.view)).collect();
                    if slices && active.values().any(|w|desired.get(&w.address).is_some_and(|v|*v!=w.view)) {
                        gap("view_changed_requires_resync");break;
                    }
                    dirty=true;
                },
                incoming=reader.next()=>{
                    let text=match incoming {
                        Some(Ok(Message::Text(text)))=>text,
                        Some(Ok(Message::Ping(_)))=>{
                            if commands.try_send(Write::Flush).is_err(){gap("control_queue_full");break;}
                            continue;
                        },
                        Some(Ok(Message::Pong(_)))=>continue,
                        other=>{
                            let reason=match other {
                                Some(Ok(Message::Close(Some(frame))))=>format!("close code={} reason={:?}",frame.code,frame.reason),
                                Some(Ok(Message::Close(None)))=>"close without status".into(),
                                Some(Err(error))=>format!("read error: {error}"),
                                None=>"read stream ended without close frame".into(),
                                _=>"unexpected non-text WebSocket frame".into(),
                            };
                            crate::log_event!(Warn,"rpc websocket read ended source={name} session={session} reason={reason}");
                            break;
                        },
                    };
                    let frame_received_at_us=now_us();
                    control.messages.fetch_add(1,Ordering::Relaxed);control.text_bytes.fetch_add(text.len() as u64,Ordering::Relaxed);
                    let frame:Frame<'_>=match serde_json::from_str(&text) {
                        Ok(frame)=>frame,Err(error)=>{crate::log_event!(Warn,"rpc websocket decode source={name} error={error}");gap("malformed_rpc_frame");break;}
                    };
                    if let Some(id)=frame.id {
                        match pending.remove(&id) {
                            Some(Pending::Subscribe(w))=>{
                                pending_subscriptions-=1;
                                if let Some(error)=frame.error {
                                    if slices && matches!(w.view,AccountView::Slice{..}) && matches!(error.code,-32600|-32602) {
                                        slices=false;gap("slice_rejected_retry_full");
                                    }else {
                                        emit(AccountEvent::Rejected{provider:name.clone(),address:w.address,code:error.code});
                                        gap("subscribe_rejected");
                                    }
                                    break;
                                }
                                let Some(Reply::Subscription(subscription))=frame.result else {gap("invalid_subscribe_reply");break;};
                                active.insert(subscription,w);
                                emit(AccountEvent::Subscribed{provider:name.clone(),address:w.address});
                                if slices && desired.get(&w.address).is_some_and(|v|*v!=w.view){gap("view_changed_requires_resync");break;}
                                if !desired.contains_key(&w.address){dirty=true;}
                            },
                            Some(Pending::Unsubscribe{address,..})=>{
                                if !matches!(frame.result,Some(Reply::Unsubscribed(true))) || frame.error.is_some(){gap("unsubscribe_rejected");break;}
                                busy.remove(&address);
                                emit(AccountEvent::Unsubscribed{provider:name.clone(),address});
                                if let Some(&view)=desired.get(&address){subscribe.push_back(AccountWatch{address,view});}
                            },
                            None=>{gap("unexpected_rpc_reply");break;},
                        }
                        continue;
                    }
                    if frame.method.as_deref()!=Some("accountNotification"){gap("unexpected_rpc_notification");break;}
                    let Some(params)=frame.params else {gap("malformed_account_notification");break;};
                    
                    let Some(w)=active.get(&params.subscription).copied() else {
                        if retired.contains(&params.subscription){continue;}
                        gap("unknown_subscription");break;
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
                    let value=match value {Ok(value)=>value,Err(error)=>{crate::log_event!(Warn,"rpc websocket account source={name} error={error:#}");gap("malformed_account_notification");break;}};
                    let decoded_at_us=now_us();
                    let _=events.send(AccountEvent::Account{provider:name.clone(),address:w.address,slot:params.result.context.slot,
                        frame_received_at_us,decoded_at_us,value});
                },
            }
        }
        gap("socket_disconnected");
        tokio::select! {_=sleep(Duration::from_millis(cfg.reconnect_ms))=>{},r=targets.changed()=>if r.is_err(){return Ok(());}}
    }
}

pub(super) async fn run(settings:Vec<WsSettings>,commitment:String,mut targets:watch::Receiver<Vec<AccountWatch>>,
    events:broadcast::Sender<AccountEvent>,control:AccountStreamControl)->Result<()> {
    let mut tasks=JoinSet::new();let mut sockets=Vec::new();
    for cfg in settings {
        let mut remaining=cfg.max_subscriptions;
        for i in 0..cfg.max_connections {
            if remaining==0{break;}
            let capacity=remaining.min(cfg.per_connection);remaining-=capacity;
            let (tx,rx)=watch::channel(Vec::new());let name=format!("{}/socket-{}",cfg.name,i+1);
            tasks.spawn(session(cfg.clone(),name,capacity,commitment.clone(),rx,events.clone(),control.clone()));
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

}
