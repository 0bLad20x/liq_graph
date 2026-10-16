//! Production task supervision and wiring. No DEX logic in the orchestrator.
use std::{env, future::Future, io::{self, BufRead}, pin::Pin, sync::Arc, time::Duration};
use anyhow::{anyhow, bail, Context, Result};
use tokio::{sync::{broadcast, oneshot, watch}, task::{JoinHandle, JoinSet}, time::Instant};
use crate::{core::{self, CompletionState, DataBatch, CompletionResultBatch, PoolState,
        PoolUpdateBatch, PoolUpdateKind, Venue},
    workers::{completion_meteora_dlmm, completion_meteora_damm_v2, completion_orca_whirlpool,
        completion_raydium_amm_v4, completion_raydium_cpmm, completion_raydium_clmm,
        completion_pancakeswap_clmm, completion_byreal_clmm, completion_manifest,
        completion_pumpswap, discovery_meteora_dlmm, discovery_meteora_damm_v2,
        discovery_orca_whirlpool, discovery_raydium, discovery_pancakeswap_clmm,
        discovery_byreal_clmm, discovery_manifest, discovery_pumpswap, discovery_rpc_full,
        diagnostic_source, graph_liquidity, graph_watch_catalog, persistence, pool_registry,
        rpc_transport, source_jupiter_mint}};
use super::jupiter_settings;

struct AbortOnDrop<T>(JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {fn drop(&mut self) {self.0.abort();}}

fn number(key:&str,default:u64)->Result<u64>{
    env::var(key).map_or(Ok(default),|value|value.parse().with_context(||format!("invalid {key}")))
}
fn enabled(key:&str)->Result<bool>{
    match env::var(key) {
        Err(env::VarError::NotPresent)=>Ok(false),Err(e)=>Err(e.into()),
        Ok(value)=>match value.as_str(){"true"|"1"=>Ok(true),"false"|"0"=>Ok(false),
            _=>bail!("{key} must be true or false")}
    }
}
fn critical<F>(tasks:&mut JoinSet<Result<()>>,name:&'static str,run:F)
where F:Future<Output=Result<()>>+Send+'static {
    println!("runtime worker={name} policy=fatal started=true");
    tasks.spawn(async move {run.await.with_context(||format!("critical worker {name}"))});
}
async fn supervise<F,Fut>(name:&'static str,mut stop:watch::Receiver<bool>,mut factory:F)->Result<()>
where F:FnMut()->Fut+Send+'static,Fut:Future<Output=Result<()>>+Send+'static {
    let mut failures=0_u32;
    loop {
        if *stop.borrow(){return Ok(());}
        let started=Instant::now();
        let mut worker=tokio::spawn(factory());
        let finished=tokio::select!{
            result=&mut worker=>Some(result),
            _=stop.changed()=>{worker.abort();let _=worker.await;None}
        };
        let Some(finished)=finished else{return Ok(());};
        if *stop.borrow(){return Ok(());}
        match finished{
            Ok(Ok(()))=>eprintln!("runtime worker={name} ended unexpectedly"),
            Ok(Err(error))=>eprintln!("runtime worker={name} error={error:#}"),
            Err(error)=>eprintln!("runtime worker={name} task_failure={error}"),
        }
        if started.elapsed()>=Duration::from_secs(600){failures=0;}
        failures=failures.saturating_add(1);
        let pause=if failures>=6{300}else{1_u64<<failures.min(5)};
        eprintln!("runtime worker={name} restart_in_s={pause} failures={failures}");
        tokio::select!{
            _=tokio::time::sleep(Duration::from_secs(pause))=>{},
            _=stop.changed()=>return Ok(()),
        }
    }
}
async fn once<F,Fut>(name:&'static str,mut stop:watch::Receiver<bool>,mut factory:F)->Result<()>
where F:FnMut()->Fut+Send+'static,Fut:Future<Output=Result<()>>+Send+'static {
    for attempt in 1..=3 {
        let mut task=tokio::spawn(factory());
        let result=tokio::select!{
            result=&mut task=>result,
            _=stop.changed()=>{task.abort();let _=task.await;return Ok(());}
        };
        match result{
            Ok(Ok(()))=>{
                println!("runtime worker={name} complete=true attempts={attempt}");
                let _=stop.changed().await;return Ok(());
            }
            other=>eprintln!("runtime worker={name} attempt={attempt} error={other:?}"),
        }
        if attempt<3 {
            tokio::select!{
                _=tokio::time::sleep(Duration::from_secs(10*attempt))=>{},
                _=stop.changed()=>return Ok(()),
            }
        }
    }
    eprintln!("runtime worker={name} degraded=true");
    let _=stop.changed().await;
    Ok(())
}
fn candidates(registry:&pool_registry::PoolRegistry,venue:Venue)->Vec<PoolState>{
    registry.snapshot().into_iter().filter(|p|p.venue==venue
        && p.mint_a.is_some()&&p.mint_b.is_some()
        && p.completion_state!=CompletionState::Prepared).collect()
}
type CompletionRun=Pin<Box<dyn Future<Output=Result<()>>+Send>>;
// A restarted completion gets a fresh Registry snapshot plus subsequent live updates;
// no global replay to unrelated consumers. The feed fails loudly on broadcast lag.
async fn completion_feed(
    venue:Venue,seed:Vec<PoolState>,mut live:broadcast::Receiver<Arc<PoolUpdateBatch>>,
    ready:Option<oneshot::Sender<()>>,
    run:impl FnOnce(broadcast::Receiver<Arc<PoolUpdateBatch>>)->CompletionRun,
)->Result<()>{
    let (tx,rx)=broadcast::channel::<Arc<PoolUpdateBatch>>(256);
    if !seed.is_empty(){
        tx.send(Arc::new(PoolUpdateBatch{kind:PoolUpdateKind::Source,
            source:"persistence_restore",received_at_ms:0,
            pools:seed,persist_identity:Vec::new()}))
            .map_err(|_|anyhow!("completion startup has no receiver"))?;
    }
    let mut relay=AbortOnDrop(tokio::spawn(async move{
        loop {
            match live.recv().await{
                Ok(batch)=>{
                    if batch.pools.iter().any(|p|p.venue==venue){
                        tx.send(batch).map_err(|_|anyhow!("completion feed receiver closed"))?;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n))=>
                    bail!("completion feed venue={venue:?} lost {n} batches"),
                Err(broadcast::error::RecvError::Closed)=>return Ok(()),
            }
        }
    }));
    if let Some(ready)=ready{let _=ready.send(());}
    tokio::select!{
        result=run(rx)=>result,
        result=&mut relay.0=>{
            result.context("completion relay panicked")??;
            bail!("completion feed venue={venue:?} closed");
        }
    }
}

pub async fn run()->Result<()>{
    let duration=number("RUN_DURATION_SECONDS",0)?;
    let full_scan=enabled("RUN_FULL_SCAN")?;
    let source_capacity=usize::try_from(number("REST_BROADCAST_CAPACITY",256)?)?;
    let registry_capacity=usize::try_from(number("REGISTRY_BROADCAST_CAPACITY",256)?)?;
    anyhow::ensure!((32..=65536).contains(&source_capacity)
        && (32..=65536).contains(&registry_capacity),"broadcast capacity must be 32..=65536");
    let graph_settings=graph_liquidity::Settings::from_env()?;
    let http=core::http_client()?;
    let (rpc,rpc_task)=rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
    let database=persistence::Persistence::connect().await?;
    let restored=database.restore().await?;
    let graph_boot:Vec<PoolState>=restored.iter()
        .filter(|p|p.completion_state==CompletionState::Prepared).cloned().collect();
    let mut boot:Vec<(Venue,Vec<PoolState>)>=Vec::new();
    for p in &restored {
        if p.mint_a.is_none()||p.mint_b.is_none()
            ||p.completion_state==CompletionState::Prepared{continue;}
        if let Some((_,list))=boot.iter_mut().find(|(venue,_)|*venue==p.venue){
            list.push(p.clone());
        }else{boot.push((p.venue,vec![p.clone()]));}
    }
    let registry=pool_registry::PoolRegistry::default();
    let count=registry.restore(restored)?;
    println!("runtime bootstrap restored={count} prepared={} venues={} backpressure_unchanged=true",graph_boot.len(),boot.len());

    let (stop_tx,stop_rx)=watch::channel(false);
    let (source,source_rx)=broadcast::channel::<Arc<DataBatch>>(source_capacity);
    let diagnostic_rx=source.subscribe();
    let (updates,observe_rx)=broadcast::channel::<Arc<PoolUpdateBatch>>(registry_capacity);
    let (completions,completion_rx)=broadcast::channel::<Arc<CompletionResultBatch>>(256);
    let (source_done,done_rx)=watch::channel(false);
    let (graph_tx,graph_rx)=watch::channel(Arc::new(graph_liquidity::GraphSnapshot::default()));
    let (catalog_tx,catalog_rx)=watch::channel(Arc::new(graph_watch_catalog::WatchCatalog::empty()));
    let mut tasks=JoinSet::<Result<()>>::new();
    critical(&mut tasks,"pool_registry",pool_registry::run(source_rx,Some(completion_rx),
        registry.clone(),updates.clone(),source_done,true));
    critical(&mut tasks,"persistence",database.run(updates.subscribe()));
    critical(&mut tasks,"graph_liquidity",graph_liquidity::run(
        updates.subscribe(),graph_tx,graph_settings));
    critical(&mut tasks,"graph_watch_catalog.observe",graph_watch_catalog::observe(catalog_rx));
    let jupiter_mints=if jupiter_settings()?.is_some(){
        let (mint_tx,mint_rx)=watch::channel(Arc::<[solana_pubkey::Pubkey]>::from([]));
        let (jupiter_out,jupiter_rx)=broadcast::channel::<Arc<DataBatch>>(32);
        critical(&mut tasks,"source_jupiter_mint.observe",diagnostic_source::run(jupiter_rx,true));
        let client=http.clone();
        tasks.spawn(supervise("source_jupiter_mint",stop_rx.clone(),move||{
            let output=jupiter_out.clone();let client=client.clone();
            let mints=mint_rx.clone();
            async move{
                let settings=jupiter_settings()?.context("Jupiter keys disappeared")?;
                source_jupiter_mint::run(client,settings,mints,output).await
            }
        }));
        Some(mint_tx)
    }else{None};
    critical(&mut tasks,"graph_watch_catalog",graph_watch_catalog::run(
        graph_rx,catalog_tx,jupiter_mints));
    critical(&mut tasks,"diagnostic_source",diagnostic_source::run(diagnostic_rx,true));
    critical(&mut tasks,"pool_registry.observe",
        pool_registry::observe_updates(observe_rx,true,true));
    critical(&mut tasks,"rpc_transport",async move{rpc_task.await?});

    // Seed graph before completion can change a Prepared state.
    if !graph_boot.is_empty(){
        updates.send(Arc::new(PoolUpdateBatch{kind:PoolUpdateKind::Source,
            source:"persistence_restore",received_at_ms:0,
            pools:graph_boot,persist_identity:Vec::new()}))
            .map_err(|_|anyhow!("no graph bootstrap subscriber"))?;
    }
    let mut ready=Vec::new();
    macro_rules! completion{
        ($module:ident,$venue:path)=>{
            {
                let mut config=$module::Settings::from_env()?;
                config.limit=0;
                let reg=registry.clone();let updates=updates.clone();
                let sink=completions.clone();let client=rpc.clone();
                let source_done=done_rx.clone();
                let (ready_tx,ready_rx)=oneshot::channel();
                ready.push(ready_rx);
                let mut initial=Some(boot.iter_mut()
                    .find(|(v,_)|*v==$venue).map(|(_,p)|std::mem::take(p))
                    .unwrap_or_default());
                let mut ack=Some(ready_tx);
                tasks.spawn(supervise(concat!("completion.",stringify!($module)),
                    stop_rx.clone(),move||{
                        let live=updates.subscribe();
                        let seed=initial.take().unwrap_or_else(||candidates(&reg,$venue));let client=client.clone();
                        let sink=sink.clone();let done=source_done.clone();
                        let first_ack=ack.take();
                        async move{
                            completion_feed($venue,seed,live,first_ack,move|input|
                                Box::pin($module::run(input,done,client,config,sink))).await
                        }
                    }));
            }
        };
    }
    completion!(completion_meteora_dlmm,Venue::MeteoraDlmm);
    completion!(completion_meteora_damm_v2,Venue::MeteoraDammV2);
    completion!(completion_orca_whirlpool,Venue::OrcaWhirlpool);
    completion!(completion_raydium_amm_v4,Venue::RaydiumAmmV4);
    completion!(completion_raydium_cpmm,Venue::RaydiumCpmm);
    completion!(completion_raydium_clmm,Venue::RaydiumClmm);
    completion!(completion_pancakeswap_clmm,Venue::PancakeSwapClmm);
    completion!(completion_byreal_clmm,Venue::ByrealClmm);
    completion!(completion_manifest,Venue::Manifest);
    completion!(completion_pumpswap,Venue::PumpSwap);
    for confirmed in ready{
        confirmed.await.context("completion failed before startup readiness")?;
    }
    drop(completions);
    println!("runtime ready=true workers_completion=10");

    // Keep REST source ownership and retry entirely outside the protocol workers.
    macro_rules! rest{
        ($module:ident,$label:literal,$config:expr)=>{
            {
                let client=http.clone();let tx=source.clone();
                tasks.spawn(supervise($label,stop_rx.clone(),move||{
                    let settings=$config;let client=client.clone();let tx=tx.clone();
                    async move{$module::run(client,tx,settings).await}
                }));
            }
        };
    }
    let raydium_rps=u32::try_from(number("RAYDIUM_API_RPS",10)?)?;
    let orca_rps=u32::try_from(number("ORCA_API_RPS",10)?)?;
    let pancake_rpm=u32::try_from(number("PANCAKESWAP_API_RPM",57)?)?;
    let byreal_rpm=u32::try_from(number("BYREAL_API_RPM",57)?)?;
    rest!(discovery_meteora_dlmm,"discovery.meteora_dlmm",
        discovery_meteora_dlmm::Settings{once:false,max_pages:None});
    rest!(discovery_meteora_damm_v2,"discovery.meteora_damm_v2",
        discovery_meteora_damm_v2::Settings{once:false,max_pages:None});
    rest!(discovery_raydium,"discovery.raydium",
        discovery_raydium::Settings{once:false,max_pages:None,rps:raydium_rps});
    rest!(discovery_orca_whirlpool,"discovery.orca_whirlpool",
        discovery_orca_whirlpool::Settings{once:false,max_pages:None,rps:orca_rps});
    rest!(discovery_pancakeswap_clmm,"discovery.pancakeswap_clmm",
        discovery_pancakeswap_clmm::Settings{once:false,max_pages:None,rpm:pancake_rpm});
    rest!(discovery_byreal_clmm,"discovery.byreal_clmm",
        discovery_byreal_clmm::Settings{once:false,max_pages:None,rpm:byreal_rpm});
    rest!(discovery_manifest,"discovery.manifest",discovery_manifest::Settings{once:false});
    let rolling=discovery_pumpswap::Settings::from_env(false,None)?;
    use rpc_transport::{RpcOperation,AccountView};
    // Check generic transport capabilities only. The PumpSwap worker itself
    // owns the program ID, discriminator, slice and pagination contract.
    let supported=rpc.supports(&RpcOperation::ProgramV2{
        program:solana_pubkey::Pubkey::default(),filters:Vec::new(),
        view:AccountView::Full,cursor:None,changed_since_slot:Some(1),limit:1
    }) && rpc.supports(&RpcOperation::Slot{commitment:"finalized".into()});
    if supported{
        let client=rpc.clone();let tx=source.clone();
        tasks.spawn(supervise("discovery.pumpswap_rolling",stop_rx.clone(),move||{
            let client=client.clone();let tx=tx.clone();
            let settings=rolling.clone();
            async move{discovery_pumpswap::run(client,tx,settings).await}
        }));
    }else{
        println!("runtime worker=discovery.pumpswap_rolling enabled=false missing_rpc_v2_or_slot=true");
    }
    if full_scan{
        let settings=discovery_rpc_full::Settings::from_env()?;
        let tx=source.clone();
        tasks.spawn(once("discovery.rpc_full",stop_rx.clone(),move||{
            let tx=tx.clone();let settings=settings.clone();
            async move{discovery_rpc_full::run(tx,settings).await}
        }));
    }
    drop(source);drop(updates);drop(rpc);

    // Operator termination uses q + Enter; finite RUN_DURATION_SECONDS supports
    // an automatic graceful live run without adding dependencies or lockfile changes.
    let (operator_tx,operator_rx)=oneshot::channel::<()>();
    std::thread::spawn(move||{
        let mut stdin=io::stdin().lock();let mut line=String::new();
        loop{
            line.clear();
            match stdin.read_line(&mut line){
                Ok(0)|Err(_)=>break,
                Ok(_) if line.trim().eq_ignore_ascii_case("q")
                    ||line.trim().eq_ignore_ascii_case("quit")=>{
                        let _=operator_tx.send(());break;
                    }
                Ok(_)=>{}
            }
        }
    });
    println!("runtime running=true stop=q+Enter duration_seconds={duration}");
    let mut error=None;
    let reason=tokio::select!{
        _=async{
            if duration==0{std::future::pending::<()>().await}
            else{tokio::time::sleep(Duration::from_secs(duration)).await}
        }=>"duration",
        _=async {
            if operator_rx.await.is_err() {std::future::pending::<()>().await}
        }=>"operator",
        joined=tasks.join_next()=>{
            error=Some(match joined{
                Some(Ok(Err(e)))=>e,
                Some(Ok(Ok(())))=>anyhow!("managed worker exited unexpectedly"),
                Some(Err(e))=>anyhow!("managed worker panicked: {e}"),
                None=>anyhow!("all runtime tasks exited"),
            });
            "fatal"
        }
    };
    eprintln!("runtime shutdown reason={reason}");
    stop_tx.send_replace(true);
    let drained=tokio::time::timeout(Duration::from_secs(30),async{
        while let Some(joined)=tasks.join_next().await{joined??;}
        Ok::<(),anyhow::Error>(())
    }).await;
    match drained{
        Ok(result)=>result?,
        Err(_)=>{tasks.abort_all();bail!("runtime graceful shutdown exceeded 30 seconds");}
    }
    if let Some(error)=error{return Err(error);}
    println!("runtime shutdown complete=true");
    Ok(())
}
