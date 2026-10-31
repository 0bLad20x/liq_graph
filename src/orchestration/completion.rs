//! Common Completion registration and resource wiring for runtime and bounded runs.
//! Protocol account semantics stay in the workers; lifecycle owns scheduling.
use std::{env,sync::Arc,time::Duration};
use anyhow::{anyhow,bail,ensure,Context,Result};
use tokio::{sync::{broadcast,oneshot,watch},task::JoinSet};
use crate::{core::{self,CompletionResultBatch,DataBatch,PoolState,PoolUpdateBatch,Venue},
    workers::{completion::{lifecycle,meteora_dlmm,meteora_damm_v2,raydium_amm_v4,raydium_cpmm,raydium_clmm,orca_whirlpool,pancakeswap_clmm,byreal_clmm,manifest,pumpswap},
        mint_qualification,persistence,pool_registry,rpc_transport}};
use super::{discovery,runtime::{critical,until_stop,AbortOnDrop},jupiter_settings};

#[derive(Clone,Copy)]
pub(super) enum Worker {
    Dlmm(meteora_dlmm::Settings),
    Damm(meteora_damm_v2::Settings),
    Amm(raydium_amm_v4::Settings),
    Cpmm(raydium_cpmm::Settings),
    Clmm(raydium_clmm::Settings),
    Orca(orca_whirlpool::Settings),
    Pancake(pancakeswap_clmm::Settings),
    Byreal(byreal_clmm::Settings),
    Manifest(manifest::Settings),
    Pump(pumpswap::Settings),
}
impl Worker {
    fn venue(self)->Venue {match self {
        Self::Dlmm(_)=>Venue::MeteoraDlmm,
        Self::Damm(_)=>Venue::MeteoraDammV2,
        Self::Amm(_)=>Venue::RaydiumAmmV4,
        Self::Cpmm(_)=>Venue::RaydiumCpmm,
        Self::Clmm(_)=>Venue::RaydiumClmm,
        Self::Orca(_)=>Venue::OrcaWhirlpool,
        Self::Pancake(_)=>Venue::PancakeSwapClmm,
        Self::Byreal(_)=>Venue::ByrealClmm,
        Self::Manifest(_)=>Venue::Manifest,
        Self::Pump(_)=>Venue::PumpSwap,
    }}
    fn launch(self,tasks:&mut JoinSet<Result<()>>,rpc:core::rpc::RpcHandle,
        responses:tokio::sync::mpsc::Sender<core::CompletionResponseBatch>)->lifecycle::Executor {
        let venue=self.venue();
        let (requests,input)=tokio::sync::mpsc::channel(4);
        let (batch_size,max_in_flight,configuration_ready)=match self {
            Self::Dlmm(settings)=>{
                let size=settings.pools_per_batch();
                critical(tasks,"completion.meteora_dlmm",meteora_dlmm::run(input,rpc,settings,responses));
                (size,size*settings.parallel_batches,false)
            },
            Self::Damm(settings)=>{
                let size=settings.pools_per_batch();
                critical(tasks,"completion.meteora_damm_v2",meteora_damm_v2::run(input,rpc,settings,responses));
                (size,size*settings.parallel_batches,false)
            },
            Self::Amm(settings)=>{
                let size=settings.pools_per_batch();
                critical(tasks,"completion.raydium_amm_v4",raydium_amm_v4::run(input,rpc,settings,responses));
                (size,size*settings.parallel_batches,false)
            },
            Self::Cpmm(settings)=>{
                let size=settings.pools_per_batch();
                critical(tasks,"completion.raydium_cpmm",raydium_cpmm::run(input,rpc,settings,responses));
                (size,size*settings.parallel_batches,false)
            },
            Self::Clmm(settings)=>{
                let size=settings.pools_per_batch();
                critical(tasks,"completion.raydium_clmm",raydium_clmm::run(input,rpc,settings,responses));
                (size,size*settings.parallel_batches,false)
            },
            Self::Orca(settings)=>{
                let size=settings.pools_per_batch();
                critical(tasks,"completion.orca_whirlpool",orca_whirlpool::run(input,rpc,settings,responses));
                (size,size*settings.parallel_batches,false)
            },
            Self::Pancake(settings)=>{
                let size=settings.pools_per_batch();
                critical(tasks,"completion.pancakeswap_clmm",pancakeswap_clmm::run(input,rpc,settings,responses));
                (size,size*settings.parallel_batches,false)
            },
            Self::Byreal(settings)=>{
                let size=settings.pools_per_batch();
                critical(tasks,"completion.byreal_clmm",byreal_clmm::run(input,rpc,settings,responses));
                (size,size*settings.parallel_batches,false)
            },
            Self::Manifest(settings)=>{
                let size=settings.pools_per_batch();
                critical(tasks,"completion.manifest",manifest::run(input,rpc,settings,responses));
                (size,size*settings.parallel_batches,false)
            },
            Self::Pump(settings)=>{
                let size=settings.pools_per_batch();
                critical(tasks,"completion.pumpswap",pumpswap::run(input,rpc,settings,responses));
                (size,size*settings.parallel_batches,settings.wallet.is_some())
            },
        };
        lifecycle::Executor{venue,requests,batch_size,max_in_flight,configuration_ready}
    }
}
pub(super) fn protocols(enabled:core::EnabledVenues)->impl Iterator<Item=Venue> {
    enabled.iter().filter(|v|*v!=Venue::RaydiumStable)
}
pub(super) fn configured(name:&str)->Result<Vec<Worker>> {
    let enabled=core::EnabledVenues::from_env()?;
    if name!="all" {
        let venue=Venue::ALL.into_iter().find(|v|v.name()==name)
            .with_context(||format!("unknown Completion protocol {name}"))?;
        ensure!(enabled.contains(venue),"Completion protocol {name} is disabled by GRAPH_ENABLED_VENUES");
        ensure!(venue!=Venue::RaydiumStable,"Raydium Stable has no Completion executor");
    }
    let mut workers=Vec::new();
    for venue in protocols(enabled).filter(|v|name=="all"||v.name()==name) {
        let worker=match venue {
            Venue::MeteoraDlmm=>Worker::Dlmm(meteora_dlmm::Settings::from_env()?),
            Venue::MeteoraDammV2=>Worker::Damm(meteora_damm_v2::Settings::from_env()?),
            Venue::RaydiumAmmV4=>Worker::Amm(raydium_amm_v4::Settings::from_env()?),
            Venue::RaydiumCpmm=>Worker::Cpmm(raydium_cpmm::Settings::from_env()?),
            Venue::RaydiumClmm=>Worker::Clmm(raydium_clmm::Settings::from_env()?),
            Venue::OrcaWhirlpool=>Worker::Orca(orca_whirlpool::Settings::from_env()?),
            Venue::PancakeSwapClmm=>Worker::Pancake(pancakeswap_clmm::Settings::from_env()?),
            Venue::ByrealClmm=>Worker::Byreal(byreal_clmm::Settings::from_env()?),
            Venue::Manifest=>Worker::Manifest(manifest::Settings::from_env()?),
            Venue::PumpSwap=>Worker::Pump(pumpswap::Settings::from_env()?),
            Venue::RaydiumStable=>continue,
        };
        workers.push(worker);
    }
    ensure!(!workers.is_empty(),"GRAPH_ENABLED_VENUES selects no supported Completion executor");
    Ok(workers)
}
/// Called before publishing restored manifests to any graph consumer.
pub(super) fn validate_bootstrap(workers:&[Worker],pools:&[PoolState])->Result<()> {
    for worker in workers {
        if let Worker::Pump(settings)=worker {pumpswap::ensure_wallet_cache(pools,settings.wallet)?;}
    }
    Ok(())
}
pub(super) fn start(tasks:&mut JoinSet<Result<()>>,registry:&pool_registry::PoolRegistry,
    updates:&broadcast::Sender<Arc<PoolUpdateBatch>>,results:broadcast::Sender<Arc<CompletionResultBatch>>,
    stop:(watch::Sender<bool>,watch::Receiver<bool>),
    rpc:core::rpc::RpcHandle,workers:Vec<Worker>,settings:lifecycle::Settings,
    report:Option<oneshot::Sender<lifecycle::Report>>)->Result<()> {
    ensure!(!workers.is_empty(),"no Completion protocol configured");
    // Subscribe first so a concurrent accepted update cannot fall between seed and stream.
    let input=updates.subscribe();let seed=registry.snapshot();
    let (responses,response_rx)=tokio::sync::mpsc::channel(4);
    let executors=workers.into_iter().map(|worker|
        worker.launch(tasks,rpc.clone(),responses.clone())).collect();
    drop(responses);
    critical(tasks,"completion.lifecycle",async move {
        let _stop_owner=stop.0;
        let completed=lifecycle::run(seed,input,response_rx,executors,results,stop.1,settings).await?;
        if let Some(output)=report {let _=output.send(completed);}
        Ok(())
    });
    Ok(())
}

/// A bounded profile of the same Registry/admission/lifecycle/protocol wiring.
/// Restore is the input basis; discovery and Jupiter are explicit opt-ins.
/// Only an explicit --persist starts the single database writer.
pub(super) async fn live(args:&[String])->Result<()> {
    let name=args.first().map(String::as_str).unwrap_or("all");
    let mut duration=60_u64;let mut pool_limit=8_usize;
    let mut revalidate=false;let mut persist=false;let mut discover=false;let mut enrich=false;let mut index=1;
    while index<args.len() {
        match args[index].as_str() {
            "--duration-seconds"=>{index+=1;duration=args.get(index).context("missing duration")?.parse()?;},
            "--pool-limit"=>{index+=1;pool_limit=args.get(index).context("missing pool limit")?.parse()?;},
            "--revalidate"=>revalidate=true,
            "--persist"=>persist=true,
            "--discover"=>discover=true,
            "--enrich"=>enrich=true,
            other=>bail!("unknown Completion argument {other}"),
        }
        index+=1;
    }
    ensure!((10..=86400).contains(&duration),"Completion duration must be 10..86400 seconds");
    ensure!((1..=100000).contains(&pool_limit),"Completion live pool limit must be 1..100000 per protocol");
    let enrichment=if enrich {Some(jupiter_settings()?.context("--enrich requires JUPITER_SEARCH_API_KEYS")?)}else{None};
    let workers=configured(name)?;
    let mut settings=lifecycle::Settings::from_env()?;
    settings.pool_limit=pool_limit;settings.revalidate_once=revalidate;
    let venues=workers.iter().map(|worker|worker.venue()).collect::<Vec<_>>();
    let database=persistence::Persistence::connect().await?;
    let restored=database.restore().await?.into_iter().filter(|p|venues.contains(&p.venue)).collect::<Vec<_>>();
    let mints=mint_qualification::MintRegistry::restore(database.restore_mints().await?)?;
    validate_bootstrap(&workers,&restored)?;
    let registry=pool_registry::PoolRegistry::from_env()?;
    let restored_count=registry.restore(restored)?;
    let (rpc,rpc_task)=rpc_transport::spawn(rpc_transport::settings_from_env()?)?;
    let mut rpc_task=AbortOnDrop(rpc_task);
    let (stop,stop_rx)=watch::channel(false);
    let (source,source_rx)=broadcast::channel::<Arc<DataBatch>>(256);
    let (updates,_)=broadcast::channel::<Arc<PoolUpdateBatch>>(256);
    let (results,result_rx)=broadcast::channel(256);
    let (done,_)=watch::channel(false);
    let (qualified_tx,qualified_rx)=watch::channel(Arc::<[solana_pubkey::Pubkey]>::from([]));
    let (jupiter_out,jupiter_rx)=broadcast::channel::<Arc<DataBatch>>(256);
    let (jupiter_persist_tx,jupiter_persist_rx)=tokio::sync::mpsc::channel::<Arc<DataBatch>>(256);
    let (target_tx,target_rx)=watch::channel(Arc::<[solana_pubkey::Pubkey]>::from([]));
    let mut tasks=JoinSet::new();
    critical(&mut tasks,"pool_registry",pool_registry::run(source_rx,Some(result_rx),
        registry.clone(),updates.clone(),done,true,Some(qualified_rx)));
    // One explicit stateless output proof; no extra consumer in production.
    critical(&mut tasks,"proof.completion_registry",pool_registry::observe_updates(updates.subscribe(),true,true));
    if persist {
        critical(&mut tasks,"persistence",database.run_with_mints(
            updates.subscribe(),Some(jupiter_persist_rx),None));
    }else{
        drop(jupiter_persist_rx);
    }
    critical(&mut tasks,"mint_qualification",mint_qualification::run(registry.snapshot(),mints,
        updates.subscribe(),jupiter_rx,qualified_tx,target_tx,None,None,None,Some(stop_rx.clone())));
    if let Some(settings)=enrichment {
        critical(&mut tasks,"source_jupiter_mint",until_stop(stop_rx.clone(),
            crate::workers::source_jupiter_mint::run_targeted(
                core::http_client()?,settings,target_rx,jupiter_out.clone(),
                if persist{Some(jupiter_persist_tx.clone())}else{None})));
    }
    drop(jupiter_persist_tx);
    let (report_tx,report_rx)=oneshot::channel();
    start(&mut tasks,&registry,&updates,results,(stop.clone(),stop_rx.clone()),
        rpc.clone(),workers,settings,Some(report_tx))?;
    if discover {
    let mut sources=Vec::new();
    for venue in &venues {
        let set=match venue {
            Venue::MeteoraDlmm=>discovery::SourceSet::Dlmm,
            Venue::MeteoraDammV2=>discovery::SourceSet::DammV2,
            Venue::OrcaWhirlpool=>discovery::SourceSet::Orca,
            Venue::PancakeSwapClmm=>discovery::SourceSet::PancakeSwap,
            Venue::ByrealClmm=>discovery::SourceSet::Byreal,
            Venue::Manifest=>discovery::SourceSet::Manifest,
            Venue::PumpSwap=>discovery::SourceSet::PumpSwap,
            Venue::RaydiumAmmV4|Venue::RaydiumCpmm|Venue::RaydiumClmm=>discovery::SourceSet::Raydium,
            Venue::RaydiumStable=>bail!("Raydium Stable has no Completion worker"),
        };
        if !sources.contains(&set) {sources.push(set);}
    }
    let mut discovery_workers=Vec::new();
    for set in sources {discovery_workers.extend(discovery::configured(set,false,None)?);}
    discovery::spawn_continuous(&mut tasks,discovery_workers,discovery::Resources{
        http:core::http_client()?,rpc:Some(rpc.clone()),output:source.clone(),stop:stop_rx});
    }
    critical(&mut tasks,"rpc_transport",async move{(&mut rpc_task.0).await?});
    drop(rpc);drop(source);drop(updates);
    crate::log_event!(Info,"completion live started protocols={venues:?} restored={restored_count} duration_s={duration} pool_limit={pool_limit} revalidate={revalidate} persist={persist} discovery={discover} enrichment={enrich}");
    let failure=tokio::select! {
        _=tokio::time::sleep(Duration::from_secs(duration))=>None,
        joined=tasks.join_next()=>Some(match joined {
            Some(Ok(Err(error)))=>error,
            Some(Err(error))=>anyhow!("Completion task panicked: {error}"),
            _=>anyhow!("Completion task ended before the requested duration"),
        }),
    };
    stop.send_replace(true);drop(jupiter_out);
    let drain_ms=env::var("COMPLETION_LIFECYCLE_ATTEMPT_SECONDS").unwrap_or_else(|_|"180".into()).parse::<u64>()?*1000+150000;
    let drained=tokio::time::timeout(Duration::from_millis(drain_ms),async {
        while let Some(joined)=tasks.join_next().await {joined??;}
        Ok::<(),anyhow::Error>(())
    }).await.context("Completion drain timed out")?;
    if let Some(error)=failure {return Err(error);}
    drained?;
    let report=report_rx.await.context("missing Completion owner report")?;
    let protocols=report.protocols.iter().map(|p|serde_json::json!({
        "venue":format!("{:?}",p.venue),"dispatched":p.dispatched,"accepted":p.accepted,
        "proofs":p.proofs,"unchanged":p.unchanged,
        "prepared":p.prepared,"incomplete":p.incomplete,"retryable":p.retryable})).collect::<Vec<_>>();
    let passed=report.protocols.iter().all(|p|p.accepted>0 && (!revalidate || p.proofs>0));
    let evidence=serde_json::json!({"run_complete":true,"completion_evidence_passed":passed,
        "revalidate":revalidate,"persisted":persist,"discovery_enabled":discover,"enrichment_enabled":enrich,"protocols":protocols,"broadcasts":false});
    if let Ok(path)=env::var("COMPLETION_REPORT_PATH") {
        std::fs::write(path,serde_json::to_vec_pretty(&evidence)?).context("write Completion evidence")?;
    }
    crate::log_event!(Info,"completion live evidence={evidence}");
    ensure!(passed,"Completion evidence missing accepted work for at least one requested protocol; inspect protocol_summary");
    Ok(())
}
