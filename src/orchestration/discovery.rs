//! One registration and resource boundary for runtime and standalone discovery.
//! Source decoding, cursors and protocol layouts remain inside their workers.
use std::{env, sync::Arc, time::Duration};
use anyhow::{bail, ensure, Context, Result};
use tokio::{sync::{broadcast, watch}, task::JoinSet};
use crate::{core::{self, DataBatch, PoolUpdateBatch, Venue},
    workers::{discovery::{meteora_dlmm, meteora_damm_v2, raydium, orca_whirlpool,
        pancakeswap_clmm, byreal_clmm, manifest, pumpswap, rpc_full, identity_hydration},
        diagnostic_source, persistence, pool_registry, rpc_transport}};

#[derive(Clone, Copy, Debug)]
pub(super) enum SourceSet { Dlmm, DlmmDammV2, Raydium, PancakeSwap, Byreal, Manifest, PumpSwap, Orca, DammV2, All }

#[derive(Clone)]
pub(super) enum Worker {
    Dlmm(meteora_dlmm::Settings), Damm(meteora_damm_v2::Settings),
    Raydium(raydium::Settings), Orca(orca_whirlpool::Settings),
    Pancake(pancakeswap_clmm::Settings), Byreal(byreal_clmm::Settings),
    Manifest(manifest::Settings), Pump(pumpswap::Settings), Full(rpc_full::Settings),
}
#[derive(Clone)]
pub(super) struct Resources {
    pub http:reqwest::Client,
    pub rpc:Option<rpc_transport::RpcHandle>,
    pub output:broadcast::Sender<Arc<DataBatch>>,
    pub stop:watch::Receiver<bool>,
}
impl Worker {
    pub(super) fn label(&self)->&'static str { match self {
        Self::Dlmm(_)=>"discovery.meteora_dlmm", Self::Damm(_)=>"discovery.meteora_damm_v2",
        Self::Raydium(_)=>"discovery.raydium", Self::Orca(_)=>"discovery.orca_whirlpool",
        Self::Pancake(_)=>"discovery.pancakeswap_clmm", Self::Byreal(_)=>"discovery.byreal_clmm",
        Self::Manifest(_)=>"discovery.manifest", Self::Pump(_)=>"discovery.pumpswap_rolling",
        Self::Full(_)=>"discovery.rpc_full",
    }}
    pub(super) async fn run(self,r:Resources)->Result<()> { match self {
        Self::Dlmm(s)=>meteora_dlmm::run(r.http,r.output,s).await,
        Self::Damm(s)=>meteora_damm_v2::run(r.http,r.output,s).await,
        Self::Raydium(s)=>raydium::run(r.http,r.output,s).await,
        Self::Orca(s)=>orca_whirlpool::run(r.http,r.output,s).await,
        Self::Pancake(s)=>pancakeswap_clmm::run(r.http,r.output,s).await,
        Self::Byreal(s)=>byreal_clmm::run(r.http,r.output,s).await,
        Self::Manifest(s)=>manifest::run(r.http,r.output,s).await,
        Self::Pump(s)=>pumpswap::run(r.rpc.context("PumpSwap discovery requires shared RPC")?,r.output,s).await,
        Self::Full(s)=>rpc_full::run(r.output,s,r.stop).await,
    }}
}
fn rate(name:&str,default:u32)->Result<u32> {
    env::var(name).map_or(Ok(default),|v|v.parse().with_context(||format!("invalid {name}")))
}
/// All means the seven REST sources; rolling V2 and full bootstrap are explicit.
pub(super) fn configured(set:SourceSet,once:bool,max_pages:Option<u32>)->Result<Vec<Worker>> {
    let mut out=Vec::new();
    if matches!(set,SourceSet::Dlmm|SourceSet::DlmmDammV2|SourceSet::All) {
        out.push(Worker::Dlmm(meteora_dlmm::Settings{once,max_pages}));
    }
    if matches!(set,SourceSet::DammV2|SourceSet::DlmmDammV2|SourceSet::All) {
        out.push(Worker::Damm(meteora_damm_v2::Settings{once,max_pages}));
    }
    if matches!(set,SourceSet::Raydium|SourceSet::All) {
        out.push(Worker::Raydium(raydium::Settings{once,max_pages,rps:rate("RAYDIUM_API_RPS",10)?}));
    }
    if matches!(set,SourceSet::Orca|SourceSet::All) {
        out.push(Worker::Orca(orca_whirlpool::Settings{once,max_pages,rps:rate("ORCA_API_RPS",10)?}));
    }
    if matches!(set,SourceSet::PancakeSwap|SourceSet::All) {
        out.push(Worker::Pancake(pancakeswap_clmm::Settings{once,max_pages,rpm:rate("PANCAKESWAP_API_RPM",57)?}));
    }
    if matches!(set,SourceSet::Byreal|SourceSet::All) {
        out.push(Worker::Byreal(byreal_clmm::Settings{once,max_pages,rpm:rate("BYREAL_API_RPM",57)?}));
    }
    if matches!(set,SourceSet::Manifest|SourceSet::All) {
        out.push(Worker::Manifest(manifest::Settings{once}));
    }
    if matches!(set,SourceSet::PumpSwap) {
        out.push(Worker::Pump(pumpswap::Settings::from_env(once,max_pages)?));
    }
    Ok(out)
}
pub(super) fn pump_supported(rpc:&rpc_transport::RpcHandle)->bool {
    use rpc_transport::RpcOperation;
    rpc.supports(&RpcOperation::ProgramV2{
        program:solana_pubkey::Pubkey::default(),filters:Vec::new(),
        view:core::account_stream::AccountView::Full,cursor:None,changed_since_slot:Some(1),limit:1,
    }) && rpc.supports(&RpcOperation::Slot{commitment:"finalized".into()})
}
pub(super) fn spawn_continuous(tasks:&mut JoinSet<Result<()>>,workers:Vec<Worker>,r:Resources) {
    for worker in workers {
        let resources=r.clone();
        tasks.spawn(super::runtime::supervise(worker.label(),r.stop.clone(),move||{
            worker.clone().run(resources.clone())
        }));
    }
}
pub(super) fn spawn_finite(tasks:&mut JoinSet<Result<()>>,workers:Vec<Worker>,r:Resources) {
    for worker in workers { tasks.spawn(worker.run(r.clone())); }
}
/// Bootstrap is a single process, not a continuously restarted source. Stop is
/// cooperative: the worker kills/reaps curl and joins its blocking reader first.
pub(super) fn spawn_bootstrap(tasks:&mut JoinSet<Result<()>>,r:Resources)->Result<()> {
    let worker=Worker::Full(rpc_full::Settings::from_env()?);
    tasks.spawn(async move {
        let mut stop=r.stop.clone();
        crate::log_event!(Info,"runtime worker=discovery.rpc_full policy=bootstrap_once started=true");
        worker.run(r).await?;
        if !*stop.borrow() {
            crate::log_event!(Info,"runtime worker=discovery.rpc_full complete=true");
            let _=stop.changed().await;
        }
        Ok(())
    });
    Ok(())
}

pub(super) async fn hydrate(initial:Vec<(solana_pubkey::Pubkey,Venue)>,
    live:broadcast::Receiver<Arc<PoolUpdateBatch>>,rpc:rpc_transport::RpcHandle,
    output:broadcast::Sender<Arc<DataBatch>>,settings:identity_hydration::Settings,finite:bool)->Result<()> {
    identity_hydration::run(initial,live,rpc,output,settings,finite).await
}
pub(super) fn spawn_hydration(tasks:&mut JoinSet<Result<()>>,registry:pool_registry::PoolRegistry,
    updates:broadcast::Sender<Arc<PoolUpdateBatch>>,r:Resources,
    settings:identity_hydration::Settings,initial:Vec<(solana_pubkey::Pubkey,Venue)>)->Result<()> {
    let rpc=r.rpc.clone().context("identity hydration requires shared RPC")?;
    let mut first_seed=Some(initial);
    let mut first_live=Some(updates.subscribe());
    tasks.spawn(super::runtime::supervise("identity_hydration.venues",r.stop.clone(),move||{
        let seed=first_seed.take().unwrap_or_else(||registry.snapshot().into_iter()
            .filter(|p|p.mint_a.is_none()||p.mint_b.is_none()).map(|p|(p.address,p.venue)).collect());
        let live=first_live.take().unwrap_or_else(||updates.subscribe());
        hydrate(seed,live,rpc.clone(),r.output.clone(),settings,false)
    }));
    Ok(())
}

/// Explicit real-source proof: only selected discovery, Registry and its output
/// consumer. Persistence is opt-in; no Completion, Graph, MarketState or V10.
pub(super) async fn live(args:&[String])->Result<()> {
    let name=args.first().context("use --discovery-live SOURCE [--max-pages N] [--duration-seconds N] [--persist]")?;
    let mut pages=None;
    let mut duration=None;
    let mut persist=false;
    let mut index=1;
    while index<args.len() {
        match args[index].as_str() {
            "--max-pages"=>{index+=1;pages=Some(args.get(index).context("missing max-pages")?.parse::<u32>()?);},
            "--duration-seconds"=>{index+=1;duration=Some(args.get(index).context("missing duration")?.parse::<u64>()?);},
            "--persist"=>persist=true,
            other=>bail!("unknown discovery argument {other}"),
        }
        index+=1;
    }
    ensure!(pages.is_none_or(|n|n>0),"max-pages must be positive");
    ensure!(duration.is_none_or(|n|(1..=86400).contains(&n)),"duration must be 1..=86400 seconds");
    ensure!(name!="rpc_full"||pages.is_none(),"full RPC scan has no pages; use duration for cancellation proof");
    ensure!(name!="manifest"||pages.is_none(),"Manifest inventory has no pages");
    let set=match name.as_str() {
        "meteora_dlmm"=>Some(SourceSet::Dlmm),"meteora_damm_v2"=>Some(SourceSet::DammV2),
        "raydium"=>Some(SourceSet::Raydium),"orca_whirlpool"=>Some(SourceSet::Orca),
        "pancakeswap_clmm"=>Some(SourceSet::PancakeSwap),"byreal_clmm"=>Some(SourceSet::Byreal),
        "manifest"=>Some(SourceSet::Manifest),"pumpswap"=>Some(SourceSet::PumpSwap),
        "rest"=>Some(SourceSet::All),"rpc_full"|"identity_hydration"=>None,
        _=>bail!("unknown discovery source {name}"),
    };
    let needs_rpc=matches!(name.as_str(),"pumpswap"|"identity_hydration");
    let (rpc,rpc_task)=if needs_rpc {
        let (rpc,task)=rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
        (Some(rpc),Some(super::runtime::AbortOnDrop(task)))
    }else{(None,None)};
    let database=if persist||name=="identity_hydration" {Some(persistence::Persistence::connect().await?)}else{None};
    let registry=pool_registry::PoolRegistry::default();
    if let Some(db)=&database {registry.restore(db.restore().await?)?;}
    let workers=if let Some(set)=set {configured(set,true,pages)?}
        else if name=="rpc_full" {vec![Worker::Full(rpc_full::Settings::from_env()?)]}else{Vec::new()};
    let (stop,stop_rx)=watch::channel(false);
    let (source,input)=broadcast::channel(256);
    let registry_input=source.subscribe();
    let (updates,output)=broadcast::channel(256);
    let writer_input=persist.then(||updates.subscribe());
    let live=updates.subscribe();
    let (done,_)=watch::channel(false);
    let mut consumers=JoinSet::new();
    consumers.spawn(diagnostic_source::run(input,true));
    consumers.spawn(pool_registry::observe_updates(output,false,true));
    consumers.spawn(pool_registry::run(registry_input,None,registry.clone(),updates.clone(),done,true,None));
    if persist {consumers.spawn(database.context("missing persistence")?.run(writer_input.context("missing writer input")?));}
    let resources=Resources{http:core::http_client()?,rpc,output:source.clone(),stop:stop_rx};
    let mut producers=JoinSet::new();
    if name=="identity_hydration" {
        let initial=registry.snapshot().into_iter().filter(|p|p.mint_a.is_none()||p.mint_b.is_none())
            .map(|p|(p.address,p.venue)).collect();
        producers.spawn(hydrate(initial,live,resources.rpc.clone().context("missing hydration RPC")?,
            source.clone(),identity_hydration::Settings::from_env()?,true));
    }else{spawn_finite(&mut producers,workers,resources.clone());}
    drop(resources);drop(source);drop(updates);
    let mut error=None;
    let deadline=async {match duration {
        Some(seconds)=>tokio::time::sleep(Duration::from_secs(seconds)).await,
        None=>std::future::pending::<()>().await,
    }};
    tokio::pin!(deadline);
    let mut cancelled=false;
    while !producers.is_empty() {
        tokio::select! {
            joined=producers.join_next()=>if let Some(result)=joined {
                if let Err(e)=result.context("discovery producer panicked").and_then(|r|r) {error=Some(e);break;}
            },
            joined=consumers.join_next(),if !consumers.is_empty()=>match joined {
                Some(Ok(Ok(())))=>{},
                Some(Ok(Err(e)))=>{error=Some(e);break;},
                other=>{error=Some(anyhow::anyhow!("discovery consumer failed: {other:?}"));break;},
            },
            _=&mut deadline=>{cancelled=true;break;},
        }
    }
    stop.send_replace(true);
    if cancelled||error.is_some() {
        // Full scan responds cooperatively. Other producer futures are cancelled
        // by their owned JoinSet; their internal page tasks then abort on drop.
        if name!="rpc_full" {producers.abort_all();}
    }
    while let Some(joined)=producers.join_next().await {
        match joined {Ok(Ok(()))=>{},Err(e) if e.is_cancelled()=>{},
            other=>{if error.is_none(){error=Some(anyhow::anyhow!("discovery drain: {other:?}"));}}}
    }
    while let Some(joined)=consumers.join_next().await {
        if let Err(e)=joined.context("discovery consumer panicked").and_then(|r|r) {if error.is_none(){error=Some(e);}}
    }
    if let Some(mut task)=rpc_task {(&mut task.0).await.context("discovery RPC task panicked")??;}
    if let Some(error)=error {return Err(error);}
    crate::log_event!(Info,"discovery run source={} run_complete=true cancelled={} source_exhausted={} page_cap={pages:?} persisted={} pools_in_ram={}",
        name,cancelled,!cancelled&&pages.is_none(),persist,registry.snapshot().len());
    Ok(())
}
