//! CP6A: factorized canonical topologies, directed-price ranking and one
//! best executable witness per changed topology. Never enumerate route products.
use std::{collections::{BTreeMap,BTreeSet,HashMap,HashSet},env,sync::Arc,
    time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
use anyhow::{Context,Result,bail,ensure};
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast,watch};
use crate::core::{
    market_state::{MarketStateEvent,PoolPriceObservation,PriceEventStream,PriceOrigin,PricePipelineTiming},
    signals::{SignalActiveCandidate,SignalCatalogReconciliation,SignalCandidate,SignalCandidateBatch,SignalEvaluation,SignalEvent,SignalLeg,
        SignalPlan,SignalPool,SignalPoolPrice,SignalPriceChange,
        SignalRangeChange,SignalRouteKind,SignalSnapshot,SignalStateUpdate,SignalStream,
        SignalTopology,SignalTrigger,SignalWitness,SIGNAL_LOG_SCALE}};
use super::graph_liquidity::{GraphSnapshot,RouteTopologyKind};

const EXECUTION_LAYOUT:&str="compact_topologies_cp6a";
const OUTPUT_LAYOUT:&str="versioned_candidate_state_cp7";
pub const OUTPUT_CAPACITY:usize=16384;

#[derive(Clone,Copy)]
pub struct Settings {pub threshold_bps:f64,pub sample_limit:usize,pub trace:bool,pub profile:bool}
impl Settings {
    pub fn from_env()->Result<Self>{
        let threshold_bps:f64=env::var("GRAPH_SIGNALS_THRESHOLD_BPS")
            .unwrap_or_else(|_|"0".into()).parse().context("invalid GRAPH_SIGNALS_THRESHOLD_BPS")?;
        ensure!(threshold_bps.is_finite()&&threshold_bps>=0.0,"invalid signal threshold");
        let sample_limit=env::var("GRAPH_SIGNALS_SAMPLE_LIMIT").unwrap_or_else(|_|"16".into())
            .parse().context("invalid GRAPH_SIGNALS_SAMPLE_LIMIT")?;
        let flag=|name:&str|->Result<bool>{match env::var(name).as_deref(){
            Ok("1"|"true")=>Ok(true),Ok("0"|"false")|Err(env::VarError::NotPresent)=>Ok(false),
            _=>bail!("{name} must be true or false"),
        }};
        Ok(Self{threshold_bps,sample_limit,trace:flag("GRAPH_SIGNALS_TRACE")?,
            profile:flag("GRAPH_SIGNALS_PROFILE")?})
    }
}
fn now_us()->u64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default()
    .as_micros().min(u128::from(u64::MAX)) as u64}
fn nanos(start:Instant)->u64{start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64}
fn live(t:&SignalTrigger)->bool{matches!(t,SignalTrigger::Price{origin:PriceOrigin::Websocket,..})}
fn kind_id(k:SignalRouteKind)->usize{match k{
    SignalRouteKind::Direct2=>0,SignalRouteKind::Direct3=>1,
    SignalRouteKind::Triangle3=>2,SignalRouteKind::Triangle4=>3,
}}
fn change_id(c:SignalRangeChange)->usize{match c{
    SignalRangeChange::Entered=>0,SignalRangeChange::Updated=>1,SignalRangeChange::Exited=>2,
}}
fn names(v:[u64;4])->BTreeMap<&'static str,u64>{
    ["direct_2","direct_3","triangle_3","triangle_4"].into_iter().zip(v).collect()
}
struct Previous { pool: usize, price: Option<Arc<SignalPoolPrice>> }
#[derive(Clone, Default)]
struct Prices {
    index: HashMap<Pubkey, usize>, pools: Vec<SignalPool>,
    mint_text: Vec<[String; 2]>, values: Vec<Option<Arc<SignalPoolPrice>>>,
}
impl Prices {
    fn bind(&mut self, address: Pubkey, a: Pubkey, b: Pubkey) -> Result<usize> {
        let pair = (a.min(b), a.max(b));
        if let Some(&id) = self.index.get(&address) {
            ensure!((self.pools[id].mint_a, self.pools[id].mint_b) == pair, "pool mint identity changed: {address}");
            return Ok(id);
        }
        ensure!(a != b, "pool repeats mint");
        let id = self.pools.len();
        self.index.insert(address, id);
        self.pools.push(SignalPool { address, mint_a: pair.0, mint_b: pair.1 });
        self.mint_text.push([pair.0.to_string(), pair.1.to_string()]);
        self.values.push(None);
        Ok(id)
    }
    fn observe(&mut self, p: &PoolPriceObservation) -> Result<(Pubkey, Previous)> {
        let address: Pubkey = p.pool.parse().context("signal pool address")?;
        let id = match self.index.get(&address).copied() {
            Some(id) => id, None => self.bind(address, p.mint_a.parse()?, p.mint_b.parse()?)?,
        };
        let [a,b] = &self.mint_text[id];
        ensure!((&p.mint_a == a && &p.mint_b == b) || (&p.mint_a == b && &p.mint_b == a), "price/catalog mint mismatch");
        let forward = if &p.base_mint == a && &p.quote_mint == b { 0 }
            else if &p.base_mint == b && &p.quote_mint == a { 1 }
            else { bail!("price orientation mismatch"); };
        let price = p.price_quote_per_base;
        ensure!(price.is_finite() && price > 0.0, "invalid indicative price");
        let logarithm = price.ln();
        let mut weights = [None; 2];
        let units = (-logarithm * SIGNAL_LOG_SCALE as f64).round() as i64;
        weights[forward] = logarithm.is_finite().then_some(units);
        weights[forward ^ 1] = (logarithm.is_finite() && (1.0 / price).is_finite()).then_some(-units);
        let old = self.values[id].replace(Arc::new(SignalPoolPrice {
            log_weight_units: weights, observation: Arc::new(p.clone()),
        }));
        Ok((address, Previous { pool: id, price: old }))
    }
    fn price<'a>(&'a self, pool: usize, previous: Option<&'a Previous>) -> Option<&'a Arc<SignalPoolPrice>> {
        if let Some(p) = previous { if p.pool == pool { return p.price.as_ref(); } }
        self.values.get(pool)?.as_ref()
    }
    fn weight(&self, slot: usize, previous: Option<&Previous>) -> Option<i64> {
        self.price(slot / 2, previous)?.log_weight_units[slot % 2]
    }
    fn change(&self, id: usize) -> SignalPriceChange { SignalPriceChange {
        pool_id: id, identity: self.pools[id].clone(), price: self.values[id].clone(),
    }}
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Key { weight: i64, slot: usize }
fn key(weight: i64, slot: usize) -> Key { Key { weight, slot } }

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
struct Winner{slots:[usize;4],length:usize,total:i64}
fn first_two(set:Option<&BTreeSet<Key>>)->[Option<Key>;2]{
    let mut values=set.into_iter().flat_map(|v|v.iter());
    [values.next().copied(),values.next().copied()]
}
fn pair(a:Pubkey,b:Pubkey)->(Pubkey,Pubkey){(a.min(b),a.max(b))}
#[derive(Default)]
struct CompactIndex {
    graph:Arc<GraphSnapshot>,
    ranked:HashMap<(Pubkey,Pubkey),BTreeSet<Key>>,
    qualifying:HashMap<(u32,bool),Winner>,
}
struct CatalogInstall {
    affected:Vec<u32>,
    previous_candidates:usize,
    summary:SignalCatalogReconciliation,
}
impl CompactIndex {
    /// Re-index only changed pair-alternative lists. Compare complete generations
    /// because watch channels can skip intermediate publications.
    fn install(&mut self,graph:Arc<GraphSnapshot>,prices:&Prices)->CatalogInstall {
        let mut dirty=HashSet::new();
        for (pair,pools) in &graph.pair_pools {
            let unchanged=self.graph.pair_pools.get(pair)
                .is_some_and(|old|Arc::ptr_eq(old,pools)||old==pools);
            if !unchanged {dirty.insert(*pair);}
        }
        for pair in self.graph.pair_pools.keys(){
            if !graph.pair_pools.contains_key(pair){dirty.insert(*pair);}
        }
        for &(a,b) in &dirty {
            for (input,output) in [(a,b),(b,a)] {
                let mut ordered=BTreeSet::new();
                for &address in graph.pools_for_pair(a,b){
                    if let Some(&id)=prices.index.get(&address){
                        let slot=2*id+usize::from(input!=a);
                        if let Some(weight)=prices.weight(slot,None){
                            ordered.insert(key(weight,slot));
                        }
                    }
                }
                if ordered.is_empty(){self.ranked.remove(&(input,output));}
                else{self.ranked.insert((input,output),ordered);}
            }
        }
        // Remap by mathematical identity, never by generation-local ID. Keep
        // affected prior winners until reconciliation so Updated/Exited remain
        // distinct from Entered. Removed shapes are withdrawn by the snapshot.
        let mut old_ids=HashMap::new();
        for (id,t) in self.graph.topologies.iter().enumerate(){
            old_ids.insert((t.kind,t.settlement,t.target,t.bridge),id as u32);
        }
        let previous=std::mem::take(&mut self.qualifying);
        let mut affected=Vec::new();
        let mut summary=SignalCatalogReconciliation{
            previous_generation:self.graph.generation,changed_pairs:dirty.len(),
            ..Default::default()};
        let mut matched=0;
        for (id,t) in graph.topologies.iter().enumerate(){
            let prior=old_ids.get(&(t.kind,t.settlement,t.target,t.bridge)).copied()
                .filter(|&old|self.graph.topologies[old as usize].directed_roles()==t.directed_roles());
            if let Some(old)=prior {
                matched+=1;
                for reverse in [false,true]{
                    if let Some(w)=previous.get(&(old,reverse)){
                        self.qualifying.insert((id as u32,reverse),*w);
                    }
                }
            }
            if prior.is_none() || t.directed_roles().iter().any(|&(a,b)|dirty.contains(&pair(a,b))){
                affected.push(id as u32);
            }
        }
        summary.added_topologies=graph.topologies.len()-matched;
        summary.removed_topologies=self.graph.topologies.len()-matched;
        summary.reused_topologies=graph.topologies.len()-affected.len();
        summary.reevaluated_topologies=affected.len();
        summary.exited_candidates=previous.len()-self.qualifying.len();
        self.graph=graph;
        CatalogInstall{affected,previous_candidates:previous.len(),summary}
    }
    fn clear_prices(&mut self){
        self.ranked.clear();
        self.qualifying.clear();
    }
    /// True only when a directed pair's best or runner-up has changed.
    /// The second-best is required to preserve direct-2 same-pool exclusion.
    fn refresh(&mut self,address:Pubkey,old:&Previous,prices:&Prices)->Result<bool>{
        let Some((a,b))=self.graph.pair_for_pool(address) else{return Ok(false)};
        let mut changed=false;
        for (direction,role) in [(0,(a,b)),(1,(b,a))] {
            let slot=2*old.pool+direction;
            let before=old.price.as_ref().and_then(|p|p.log_weight_units[direction]);
            let after=prices.weight(slot,None);
            if before==after{continue;}
            let set=self.ranked.entry(role).or_default();
            let old_top=first_two(Some(set));
            if let Some(w)=before {
                ensure!(set.remove(&key(w,slot)),"CP6A ordered price missing prior weight");
            }
            if let Some(w)=after {
                ensure!(set.insert(key(w,slot)),"CP6A ordered price duplicated");
            }
            changed|=old_top!=first_two(Some(set));
        }
        Ok(changed)
    }
    fn first(&self,a:Pubkey,b:Pubkey,exclude:Option<usize>)->Option<Key>{
        self.ranked.get(&(a,b))?.iter().find(|p|Some(p.slot/2)!=exclude).copied()
    }
    fn best(&self,top:&SignalTopology,reverse:bool)->Result<Option<Winner>>{
        let mut slots=[0_usize;4];
        if top.kind==SignalRouteKind::Direct2 {
            if reverse{return Ok(None);}
            let (s,t)=top.roles[0];
            let left=self.first(s,t,None);
            let right=self.first(t,s,None);
            let first=left.and_then(|l|self.first(t,s,Some(l.slot/2))
                .map(|r|(l,r)));
            let second=right.and_then(|r|self.first(s,t,Some(r.slot/2))
                .map(|l|(l,r)));
            let result=[first,second].into_iter().flatten().filter_map(|(l,r)|{
                let total=i64::try_from(i128::from(l.weight)+i128::from(r.weight)).ok()?;
                Some(Winner{slots:[l.slot,r.slot,0,0],length:2,total})
            }).min_by_key(|p|(p.total,p.slots));
            return Ok(result);
        }
        let mut total=0_i128;
        for i in 0..top.length {
            let j=if reverse{top.length-1-i}else{i};
            let (input,output)=top.roles[j];
            let chosen=if reverse{self.first(output,input,None)}else{self.first(input,output,None)};
            let Some(chosen)=chosen else{return Ok(None)};
            slots[i]=chosen.slot;
            total+=i128::from(chosen.weight);
        }
        let total=i64::try_from(total).context("CP6A sum exceeds fixed log-weight representation")?;
        Ok(Some(Winner{slots,length:top.length,total}))
    }
    fn witness(&self,top:&SignalTopology,winner:Winner,reverse:bool,prices:&Prices)->Result<SignalWitness>{
        let mut legs=Vec::with_capacity(winner.length);
        for i in 0..winner.length {
            let j=if reverse{winner.length-1-i}else{i};
            let (a,b)=top.roles[j];
            let (input,output)=if reverse{(b,a)}else{(a,b)};
            let slot=winner.slots[i];
            let p=prices.pools.get(slot/2).context("candidate pool index invalid")?;
            ensure!(pair(input,output)==(p.mint_a,p.mint_b),
                "candidate mint role differs from priced pool");
            let value=prices.values.get(slot/2).and_then(|p|p.as_ref())
                .context("candidate price unavailable")?;
            legs.push(SignalLeg{slot,input_mint:input,output_mint:output,
                log_weight_units:value.log_weight_units[slot%2]
                    .context("candidate directed price unavailable")?,
                observation:value.observation.clone()});
        }
        ensure!(legs.windows(2).all(|w|w[0].output_mint==w[1].input_mint)
            && legs.last().is_some_and(|last|last.output_mint==legs[0].input_mint),
            "CP6A candidate route is not closed");
        ensure!(legs.iter().map(|l|i128::from(l.log_weight_units)).sum::<i128>()
            ==i128::from(winner.total),"CP6A candidate score mismatch");
        let sum=winner.total as f64/SIGNAL_LOG_SCALE as f64;
        let bps=(-sum).exp_m1()*10_000.0;
        Ok(SignalWitness{legs,log_sum_units:winner.total,negative_log_sum:sum,
            indicative_return_bps:bps.is_finite().then_some(bps)})
    }
    fn reconcile(&mut self,ids:&[u32],prices:&Prices,threshold:i64,
        stats:&mut SignalEvaluation,publish_changes:bool)->Result<(Vec<SignalCandidate>,[usize;3])>{
        let mut out=Vec::new();
        let mut transitions=[0;3];
        for &id in ids {
            let top=&self.graph.topologies[id as usize];
            let kind=match top.kind {
                RouteTopologyKind::Direct2=>SignalRouteKind::Direct2,
                RouteTopologyKind::Direct3=>SignalRouteKind::Direct3,
                RouteTopologyKind::Triangle3=>SignalRouteKind::Triangle3,
                RouteTopologyKind::Triangle4=>SignalRouteKind::Triangle4,
            };
            let definition=SignalTopology{kind,settlement:top.settlement,target:top.target,
                bridge:top.bridge,roles:top.roles,length:top.length};
            stats.groups_visited+=1;
            stats.logical_routes_affected=stats.logical_routes_affected
                .saturating_add(top.route_options);
            for reverse in [false,true] {
                if kind==SignalRouteKind::Direct2 && reverse{continue;}
                stats.range_queries+=1;
                let best=self.best(&definition,reverse)?;
                if best.is_some(){stats.ready_anchors[kind_id(kind)]+=1;}
                let selected=best.filter(|b|b.total<threshold);
                let key=(id,reverse);
                let previous=self.qualifying.get(&key).copied();
                if previous==selected{continue;}
                let change=match(previous,selected){
                    (None,Some(_))=>SignalRangeChange::Entered,
                    (Some(_),None)=>SignalRangeChange::Exited,
                    (Some(_),Some(_))=>SignalRangeChange::Updated,
                    (None,None)=>continue,
                };
                if let Some(w)=selected{self.qualifying.insert(key,w);}
                else{self.qualifying.remove(&key);}
                transitions[change_id(change)]+=1;
                let witness=if publish_changes {
                    stats.ranges[change_id(change)]+=1;
                    stats.ranges_by_kind[kind_id(kind)]+=1;
                    stats.anchors_visited+=1;
                    selected.map(|w|self.witness(&definition,w,reverse,prices)).transpose()?
                }else{None};
                out.push(SignalCandidate{topology_id:id as usize,reverse,change,witness});
            }
        }
        Ok((out,transitions))
    }
    fn candidates(&mut self,address:Pubkey,prices:&Prices,threshold:i64,
        stats:&mut SignalEvaluation)->Result<Vec<SignalCandidate>>{
        let graph=self.graph.clone();
        self.reconcile(graph.topologies_for_pool(address),prices,threshold,stats,true)
            .map(|(changes,_)|changes)
    }
    fn snapshot_candidates(&self,plan:&SignalPlan,prices:&Prices)->Result<Vec<SignalActiveCandidate>>{
        let mut keys=self.qualifying.keys().copied().collect::<Vec<_>>();
        keys.sort_unstable();
        keys.into_iter().map(|(id,reverse)|{
            let top=plan.compact_topologies.get(id as usize).context("snapshot topology absent")?;
            let winner=self.qualifying[&(id,reverse)];
            Ok(SignalActiveCandidate{topology_id:id as usize,reverse,
                witness:self.witness(top,winner,reverse,prices)?})
        }).collect()
    }
}
struct Work {
    started:Instant,stats:SignalEvaluation,
}
struct Engine {
    prices:Prices,plan:Arc<SignalPlan>,index:CompactIndex,
    output:broadcast::Sender<Arc<SignalEvent>>,
    sequence:u64,revision:u64,threshold:i64,production:ProductionReport,
}
impl Engine {
    fn next_sequence(&mut self)->u64{self.sequence+=1;self.sequence}
    fn emit(&mut self,event:SignalEvent)->Result<()>{
        self.output.send(Arc::new(event)).map_err(|_|anyhow::anyhow!("signal output has no consumer"))?;
        self.production.max_output_queue_depth=self.production.max_output_queue_depth.max(self.output.len());
        Ok(())
    }
    fn begin(&mut self,trigger:SignalTrigger,started:Instant,received:u64,
        pipeline:Option<PricePipelineTiming>,queue:usize)->Work{
        self.revision+=1;
        Work{started,stats:SignalEvaluation{sequence:0,revision:self.revision,
            graph_generation:self.plan.graph_generation,trigger:Arc::new(trigger),
            price_pipeline:pipeline,worker_received_at_us:received,completed_at_us:0,
            price_queue_depth:queue,groups_visited:0,logical_routes_affected:0,
            anchors_visited:0,ready_anchors:[0;4],range_queries:0,ranges:[0;3],
            ranges_by_kind:[0;4],catalog:None,logarithms_prepared:0,price_preparation_ns:0,
            index_update_ns:0,state_ready_ns:0,plan_build_ns:0,query_ns:0,
            event_build_ns:0,publication_ns:0,first_range_ns:None,
            last_range_ns:None,processing_ns:0}}
    }
    fn state(&mut self,work:&mut Work,snapshot:bool,clear:bool,
        changes:Vec<SignalPriceChange>)->Result<()>{
        work.stats.state_ready_ns=nanos(work.started);
        let ready_at_us=now_us();
        let started=Instant::now();
        let snapshot=if snapshot{Some(SignalSnapshot{plan:self.plan.clone(),
            pools:self.prices.pools.clone(),prices:self.prices.values.clone(),
            threshold_units:self.threshold,
            candidates:self.index.snapshot_candidates(&self.plan,&self.prices)?})}else{None};
        let update=SignalStateUpdate{sequence:self.next_sequence(),revision:self.revision,
            graph_generation:self.plan.graph_generation,trigger:work.stats.trigger.clone(),
            ready_at_us,price_pipeline:work.stats.price_pipeline,snapshot,clear,changes};
        work.stats.event_build_ns+=nanos(started);
        self.production.counts.state(&update);
        let started=Instant::now();
        self.emit(SignalEvent::State(update))?;
        work.stats.publication_ns+=nanos(started);
        Ok(())
    }
    fn candidates(&mut self,work:&mut Work,address:Pubkey)->Result<()>{
        let started=Instant::now();
        let candidates=self.index.candidates(address,&self.prices,self.threshold,&mut work.stats)?;
        work.stats.query_ns=nanos(started);
        if candidates.is_empty(){return Ok(());}
        let t=Instant::now();
        let block=SignalCandidateBatch{sequence:self.next_sequence(),revision:self.revision,
            graph_generation:self.plan.graph_generation,trigger:work.stats.trigger.clone(),
            price_pipeline:work.stats.price_pipeline,threshold_units:self.threshold,candidates};
        self.production.counts.candidate(&block);
        work.stats.event_build_ns+=nanos(t);
        let started=Instant::now();
        self.emit(SignalEvent::Candidates(block))?;
        work.stats.publication_ns+=nanos(started);
        let end=nanos(work.started);
        work.stats.first_range_ns=Some(end);work.stats.last_range_ns=Some(end);
        Ok(())
    }
    fn evaluate(&mut self,mut work:Work)->Result<()>{
        work.stats.sequence=self.next_sequence();
        work.stats.processing_ns=nanos(work.started);
        work.stats.completed_at_us=now_us();
        self.production.record(&work.stats);
        self.emit(SignalEvent::Evaluated(work.stats))
    }
    fn install(&mut self,graph:Arc<GraphSnapshot>)->CatalogInstall{
        let compact=graph.topologies.iter().map(|t|SignalTopology{
            kind:match t.kind{RouteTopologyKind::Direct2=>SignalRouteKind::Direct2,
                RouteTopologyKind::Direct3=>SignalRouteKind::Direct3,
                RouteTopologyKind::Triangle3=>SignalRouteKind::Triangle3,
                RouteTopologyKind::Triangle4=>SignalRouteKind::Triangle4},
            settlement:t.settlement,target:t.target,bridge:t.bridge,
            roles:t.roles,length:t.length}).collect::<Vec<_>>();
        let installed=self.index.install(graph.clone(),&self.prices);
        crate::log_event!(Info,"graph signals cp6b rankings generation={} changed_pairs={} directed_roles={} old_groups_compiled=0",
            graph.generation,installed.summary.changed_pairs,self.index.ranked.len());
        let plan=Arc::new(SignalPlan{graph_generation:graph.generation,
            log_weight_scale:SIGNAL_LOG_SCALE,
            compact_topologies:compact.into(),route_options:graph.route_options});
        let old=std::mem::replace(&mut self.plan,plan);
        tokio::task::spawn_blocking(move||drop(old));
        installed
    }
    fn reconcile_catalog(&mut self,work:&mut Work,mut installed:CatalogInstall,sample_limit:usize)->Result<()>{
        let started=Instant::now();
        let (changes,transitions)=self.index.reconcile(&installed.affected,&self.prices,
            self.threshold,&mut work.stats,false)?;
        work.stats.query_ns=nanos(started);
        let summary=&mut installed.summary;
        summary.entered_candidates=transitions[0];
        summary.updated_candidates=transitions[1];
        summary.exited_candidates+=transitions[2];
        summary.active_candidates=self.index.qualifying.len();
        summary.unchanged_candidates=summary.active_candidates
            .checked_sub(summary.entered_candidates+summary.updated_candidates)
            .context("catalog candidate transition accounting underflow")?;
        ensure!(installed.previous_candidates+summary.entered_candidates
            ==summary.active_candidates+summary.exited_candidates,
            "catalog candidate membership accounting mismatch");
        for mut candidate in changes.into_iter().filter(|c|c.change!=SignalRangeChange::Exited)
            .take(sample_limit.saturating_sub(self.production.catalog_examples.len())) {
            let top=&self.plan.compact_topologies[candidate.topology_id];
            if let Some(&winner)=self.index.qualifying.get(&(candidate.topology_id as u32,candidate.reverse)) {
                candidate.witness=Some(self.index.witness(top,winner,candidate.reverse,&self.prices)?);
            }
            self.production.catalog_examples.push(serde_json::json!({
                "graph_generation":self.plan.graph_generation,"previous_generation":summary.previous_generation,
                "revision":work.stats.revision,"trigger":work.stats.trigger,"topology":top,
                "candidate":candidate}));
        }
        crate::log_event!(Info,"graph signals cp7_catalog generation={} previous={} changed_pairs={} added_topologies={} removed_topologies={} reused_topologies={} reevaluated_topologies={} entered={} updated={} exited={} unchanged={} active={} catalog_execution=false",
            self.plan.graph_generation,summary.previous_generation,summary.changed_pairs,
            summary.added_topologies,summary.removed_topologies,summary.reused_topologies,
            summary.reevaluated_topologies,summary.entered_candidates,summary.updated_candidates,
            summary.exited_candidates,summary.unchanged_candidates,summary.active_candidates);
        work.stats.catalog=Some(installed.summary);
        Ok(())
    }
}
pub async fn run(mut input:PriceEventStream,mut graphs:watch::Receiver<Arc<GraphSnapshot>>,
    output:broadcast::Sender<Arc<SignalEvent>>,settings:Settings)->Result<ProductionReport>{
    let initial=Arc::new(SignalPlan{graph_generation:0,log_weight_scale:SIGNAL_LOG_SCALE,
        compact_topologies:Arc::from([]),route_options:0});
    let mut e=Engine{prices:Prices::default(),plan:initial,index:CompactIndex::default(),
        output,sequence:0,revision:0,
        threshold:(-(settings.threshold_bps/10_000.0).ln_1p()*SIGNAL_LOG_SCALE as f64).round() as i64,
        production:ProductionReport::default()};
    let outcome:Result<()>=async{
        let started=Instant::now();let received=now_us();
        let mut work=e.begin(SignalTrigger::Catalog{generation:0},started,received,None,input.len());
        e.state(&mut work,true,false,Vec::new())?;e.evaluate(work)?;
        let mut graph_open=true;
        crate::log_event!(Info,"graph signals cp6a started=true output_layout={} compact=true",OUTPUT_LAYOUT);
        loop{tokio::select!{
            change=graphs.changed(),if graph_open=>{
                if change.is_err(){graph_open=false;continue;}
                let graph=graphs.borrow_and_update().clone();
                if graph.generation==e.plan.graph_generation{continue;}
                let started=Instant::now();let received=now_us();
                // Only pools on a valid route need signal identities. Never
                // build or wait for a separate WatchCatalog projection.
                for &(a,b) in graph.pair_topologies.keys(){
                    for &pool in graph.pools_for_pair(a,b){
                        e.prices.bind(pool,a,b)?;
                    }
                }
                let installed=e.install(graph.clone());
                let mut work=e.begin(SignalTrigger::Catalog{generation:graph.generation},
                    started,received,None,input.len());
                work.stats.plan_build_ns=nanos(started);
                e.reconcile_catalog(&mut work,installed,settings.sample_limit)?;
                e.state(&mut work,true,false,Vec::new())?;
                e.production.plan(&e.plan,&e.index,&e.prices,work.stats.plan_build_ns);
                e.evaluate(work)?;
            },
            event=input.recv()=>match event{
                Ok(event)=>match event.as_ref(){
                    MarketStateEvent::Price(p)=>{
                        let started=Instant::now();let received=now_us();let preparing=Instant::now();
                        let (address,old)=e.prices.observe(p)?;
                        let preparation_ns=nanos(preparing);
                        let index_start=Instant::now();
                        let change=e.index.refresh(address,&old,&e.prices)?;
                        let index_ns=nanos(index_start);
                        let mut work=e.begin(SignalTrigger::Price{pool:address,
                            sequence:p.sequence,origin:p.origin,received_at_us:p.received_at_us},
                            started,received,p.pipeline,input.len());
                        work.stats.price_preparation_ns=preparation_ns;
                        work.stats.index_update_ns=index_ns;work.stats.logarithms_prepared=1;
                        e.state(&mut work,false,false,vec![e.prices.change(old.pool)])?;
                        if change{e.candidates(&mut work,address)?;}
                        e.evaluate(work)?;
                    },
                    MarketStateEvent::Unavailable{pool,sequence,..}|MarketStateEvent::Removed{pool,sequence}=>{
                        let started=Instant::now();let received=now_us();
                        let address:Pubkey=pool.parse().context("unavailable pool key")?;
                        let id=e.prices.index.get(&address).copied();
                        let mut changed=false;
                        if let Some(id)=id{
                            let old=Previous{pool:id,price:e.prices.values[id].take()};
                            changed=e.index.refresh(address,&old,&e.prices)?;
                        }
                        let mut work=e.begin(SignalTrigger::Coverage{sequence:*sequence,
                            reason:"pool unavailable or removed".into()},started,received,None,input.len());
                        let changes=id.map(|i|e.prices.change(i)).into_iter().collect();
                        e.state(&mut work,false,false,changes)?;
                        if changed{e.candidates(&mut work,address)?;}
                        e.evaluate(work)?;
                    },
                    MarketStateEvent::Gap{sequence,reason}=>{
                        let started=Instant::now();let received=now_us();
                        e.prices.values.fill(None);
                        e.index.clear_prices();
                        let mut work=e.begin(SignalTrigger::Coverage{sequence:*sequence,reason:reason.clone()},
                            started,received,None,input.len());
                        e.state(&mut work,false,true,Vec::new())?;e.evaluate(work)?;
                    },
                    MarketStateEvent::Selection{sequence,selected,..}=>{
                        let started=Instant::now();let received=now_us();
                        let selected=selected.iter().map(|p|p.parse::<Pubkey>())
                            .collect::<Result<HashSet<_>,_>>()?;
                        let mut changes=Vec::new();
                        let mut affected=HashSet::new();
                        for id in 0..e.prices.pools.len(){
                            let address=e.prices.pools[id].address;
                            if e.prices.values[id].is_some()&&!selected.contains(&address){
                                let old=Previous{pool:id,price:e.prices.values[id].take()};
                                if e.index.refresh(address,&old,&e.prices)?{affected.insert(address);}
                                changes.push(e.prices.change(id));
                            }
                        }
                        let mut work=e.begin(SignalTrigger::Coverage{sequence:*sequence,
                            reason:"observation selection reconciled".into()},started,received,None,input.len());
                        e.state(&mut work,false,false,changes)?;
                        for address in affected{e.candidates(&mut work,address)?;}
                        e.evaluate(work)?;
                    },
                    MarketStateEvent::Stats{..}=>{},
                },
                Err(broadcast::error::RecvError::Lagged(n))=>{
                    e.prices.values.fill(None);
                    e.index.clear_prices();
                    let started=Instant::now();let received=now_us();
                    let mut work=e.begin(SignalTrigger::Coverage{sequence:0,
                        reason:format!("price stream lost {n} inputs; state invalid")},
                        started,received,None,input.len());
                    e.state(&mut work,false,true,Vec::new())?;
                    e.evaluate(work)?;
                    bail!("signal price stream lost {n} inputs; state invalid");
                },
                Err(broadcast::error::RecvError::Closed)=>break,
            },
        }}
        Ok(())
    }.await;
    e.production.error=outcome.err().map(|e|format!("{e:#}"));
    e.production.final_price_pools=e.prices.values.iter().filter(|p|p.is_some()).count();
    e.production.final_available_directions=e.prices.values.iter().flatten()
        .map(|p|p.log_weight_units.iter().filter(|w|w.is_some()).count()).sum();
    crate::log_event!(Info,"graph signals cp6a complete={} websocket={} candidates={} error={:?}",
        e.production.error.is_none(),e.production.counts.websocket_evaluations,
        e.production.counts.candidates,e.production.error);
    Ok(e.production)
}
#[derive(Clone)]
struct Histogram { bins: [u64; 64], count: u64, total: u128, min: u64, max: u64 }
impl Default for Histogram {
    fn default() -> Self { Self { bins: [0; 64], count: 0, total: 0, min: u64::MAX, max: 0 } }
}
impl Histogram {
    fn add(&mut self, n: u64) {
        self.add_many(n,1);
    }
    fn add_many(&mut self,n:u64,count:u64) {
        if count==0 {return;}
        let bin = if n == 0 { 0 } else { (64 - n.leading_zeros()) as usize };
        self.bins[bin.min(63)] += count; self.count += count; self.total += u128::from(n)*u128::from(count);
        self.min = self.min.min(n); self.max = self.max.max(n);
    }
    fn percentile(&self, percent: u64) -> Option<u64> {
        if self.count == 0 { return None; }
        let target = (u128::from(self.count) * u128::from(percent)).div_ceil(100);
        let mut count = 0_u128;
        for (i, n) in self.bins.iter().enumerate() {
            count += u128::from(*n);
            if count >= target {
                let bound = if i == 63 { u64::MAX } else { (1_u64 << i).saturating_sub(1) };
                return Some(bound.min(self.max));
            }
        }
        Some(self.max)
    }
    fn report(&self) -> Timing {
        Timing { count: self.count, min: (self.count > 0).then_some(self.min), max: (self.count > 0).then_some(self.max),
            mean: if self.count > 0 { Some(self.total as f64 / self.count as f64) } else { None },
            p50_upper_bound: self.percentile(50), p95_upper_bound: self.percentile(95), p99_upper_bound: self.percentile(99) }
    }
}
impl serde::Serialize for Histogram {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.report(), serializer)
    }
}
#[derive(serde::Serialize)]
pub struct Timing { count: u64, min: Option<u64>, max: Option<u64>, mean: Option<f64>,
    p50_upper_bound: Option<u64>, p95_upper_bound: Option<u64>, p99_upper_bound: Option<u64> }


#[derive(Default, PartialEq, Eq, serde::Serialize)]
struct Counts {
    events: u64, state_updates: u64, plan_snapshots: u64, changed_pools: u64, resets: u64,
    evaluations: u64, price_evaluations: u64, websocket_evaluations: u64,
    catalog_evaluations: u64, coverage_evaluations: u64, logarithms_prepared: u64,
    range_blocks: u64, ranges: u64, entered_ranges: u64, updated_ranges: u64, exited_ranges: u64,
    candidate_batches:u64, candidates:u64,
    snapshot_candidates:u64, catalog_reconciliations:u64,
    catalog_added_topologies:u64, catalog_removed_topologies:u64, catalog_reused_topologies:u64,
    catalog_reevaluated_topologies:u64, catalog_entered_candidates:u64,
    catalog_updated_candidates:u64, catalog_exited_candidates:u64,
    catalog_unchanged_candidates:u64,
    websocket_ranges: u64, websocket_candidate_ranges: u64,
    groups_visited: u64, logical_routes_affected: u64, anchors_visited: u64, range_queries: u64,
    ready_anchors: [u64;4], websocket_ready_anchors: [u64;4], ranges_by_kind: [u64;4],
}
impl Counts {
    fn state(&mut self, s: &SignalStateUpdate) {
        self.events+=1; self.state_updates+=1; self.changed_pools+=s.changes.len() as u64;
        self.plan_snapshots+=u64::from(s.snapshot.is_some()); self.resets+=u64::from(s.clear);
        self.snapshot_candidates+=s.snapshot.as_ref().map_or(0,|s|s.candidates.len() as u64);
    }
    fn candidate(&mut self,b:&SignalCandidateBatch) {
        self.events+=1;self.candidate_batches+=1;self.candidates+=b.candidates.len() as u64;
        for c in &b.candidates {
            self.ranges+=1;
            match c.change {
                SignalRangeChange::Entered=>self.entered_ranges+=1,
                SignalRangeChange::Updated=>self.updated_ranges+=1,
                SignalRangeChange::Exited=>self.exited_ranges+=1,
            }
            if live(&b.trigger) {
                self.websocket_ranges+=1;
                self.websocket_candidate_ranges+=u64::from(c.change!=SignalRangeChange::Exited);
            }
        }
    }
    fn evaluation(&mut self, e: &SignalEvaluation) {
        self.events+=1; self.evaluations+=1;
        if let Some(c)=&e.catalog {
            self.catalog_reconciliations+=1;
            self.catalog_added_topologies+=c.added_topologies as u64;
            self.catalog_removed_topologies+=c.removed_topologies as u64;
            self.catalog_reused_topologies+=c.reused_topologies as u64;
            self.catalog_reevaluated_topologies+=c.reevaluated_topologies as u64;
            self.catalog_entered_candidates+=c.entered_candidates as u64;
            self.catalog_updated_candidates+=c.updated_candidates as u64;
            self.catalog_exited_candidates+=c.exited_candidates as u64;
            self.catalog_unchanged_candidates+=c.unchanged_candidates as u64;
        }
        match e.trigger.as_ref() {
            SignalTrigger::Price { .. } => self.price_evaluations+=1,
            SignalTrigger::Catalog { .. } => self.catalog_evaluations+=1,
            SignalTrigger::Coverage { .. } => self.coverage_evaluations+=1,
        }
        self.websocket_evaluations+=u64::from(live(&e.trigger));
        self.logarithms_prepared+=e.logarithms_prepared;
        self.groups_visited+=e.groups_visited; self.logical_routes_affected+=e.logical_routes_affected;
        self.anchors_visited+=e.anchors_visited; self.range_queries+=e.range_queries;
        for i in 0..4 {
            self.ready_anchors[i]+=e.ready_anchors[i]; self.ranges_by_kind[i]+=e.ranges_by_kind[i];
            if live(&e.trigger) { self.websocket_ready_anchors[i]+=e.ready_anchors[i]; }
        }
    }
}
#[derive(Default, serde::Serialize)]
struct ProductionTimings {
    preparation_ns: Histogram, index_update_ns: Histogram, state_ready_ns: Histogram,
    query_ns: Histogram, event_build_ns: Histogram, publication_ns: Histogram, processing_ns: Histogram,
    first_range_ns: Histogram, last_range_ns: Histogram,
    transport_decode_us: Histogram, transport_to_market_us: Histogram, market_processing_us: Histogram,
    price_queue_us: Histogram, frame_to_worker_us: Histogram, frame_to_state_ready_us: Histogram,
}
impl ProductionTimings {
    fn record(&mut self, e: &SignalEvaluation) {
        self.preparation_ns.add(e.price_preparation_ns); self.index_update_ns.add(e.index_update_ns);
        self.state_ready_ns.add(e.state_ready_ns); self.query_ns.add(e.query_ns);
        self.event_build_ns.add(e.event_build_ns); self.publication_ns.add(e.publication_ns);
        self.processing_ns.add(e.processing_ns);
        if let Some(t)=e.first_range_ns { self.first_range_ns.add(t); }
        if let Some(t)=e.last_range_ns { self.last_range_ns.add(t); }
        if let Some(t)=e.price_pipeline {
            for (end,start,hist) in [
                (t.transport_decoded_at_us,t.frame_received_at_us,&mut self.transport_decode_us),
                (t.market_received_at_us,t.transport_decoded_at_us,&mut self.transport_to_market_us),
                (t.price_published_at_us,t.market_received_at_us,&mut self.market_processing_us),
                (e.worker_received_at_us,t.price_published_at_us,&mut self.price_queue_us),
                (e.worker_received_at_us,t.frame_received_at_us,&mut self.frame_to_worker_us),
            ] { if let Some(us)=end.checked_sub(start) { hist.add(us); } }
            if let Some(us)=e.worker_received_at_us.checked_sub(t.frame_received_at_us) {
                self.frame_to_state_ready_us.add(us.saturating_add(e.state_ready_ns.div_ceil(1000)));
            }
        }
    }
}
#[derive(Default, serde::Serialize)]
pub struct ProductionReport {
    error: Option<String>, counts: Counts,
    all_inputs: ProductionTimings, websocket_inputs: ProductionTimings,
    steady_state_inputs: ProductionTimings, initialization_inputs: ProductionTimings,
    catalog_plan_build_ns: Histogram, last_plan: Option<serde_json::Value>,
    catalog_examples:Vec<serde_json::Value>,
    final_price_pools: usize, final_available_directions: usize,
    max_price_queue_depth: usize, max_output_queue_depth: usize,
}
impl ProductionReport {
    fn plan(&mut self,p:&Arc<SignalPlan>,index:&CompactIndex,prices:&Prices,ns:u64){
        self.catalog_plan_build_ns.add(ns);
        self.last_plan=Some(serde_json::json!({
            "generation":p.graph_generation,"pools":prices.pools.len(),
            "compact_topologies":p.compact_topologies.len(),
            "legacy_groups_compiled":0,"logical_routes":p.route_options,
            "directed_price_roles":index.ranked.len(),
            "ordered_entries_at_install":index.ranked.values().map(BTreeSet::len).sum::<usize>(),
            "dependency_index_shared":true,"build_ns":ns,
        }));
        crate::log_event!(Info,"graph signals cp6a plan={}",self.last_plan.as_ref().unwrap());
    }
    fn record(&mut self, e: &SignalEvaluation) {
        self.counts.evaluation(e); self.all_inputs.record(e);
        self.max_price_queue_depth=self.max_price_queue_depth.max(e.price_queue_depth);
        if live(&e.trigger) {
            self.websocket_inputs.record(e);
            if e.price_pipeline.is_some_and(|t|!t.initialization_replay) { self.steady_state_inputs.record(e); }
            else { self.initialization_inputs.record(e); }
        } else if matches!(e.trigger.as_ref(),SignalTrigger::Price { .. }) {
            self.initialization_inputs.record(e);
        }
    }
    pub fn ensure_complete(&self) -> Result<()> {
        ensure!(self.error.is_none(),"signal producer failed: {}",self.error.as_deref().unwrap_or(""));
        Ok(())
    }
    pub fn ensure_delivery(&self, r: &ObservationReport) -> Result<()> {
        self.ensure_complete()?; r.ensure_integrity()?;
        ensure!(self.counts==r.counts,"signal producer/consumer counts differ");
        Ok(())
    }
    pub fn ensure_input_delivery(&self, prices: u64, websocket: u64) -> Result<()> {
        self.ensure_complete()?;
        ensure!(self.counts.price_evaluations==prices && self.counts.websocket_evaluations==websocket,
            "Market-State/signal input totals differ");
        let t=&self.websocket_inputs;
        ensure!([&t.transport_decode_us,&t.transport_to_market_us,&t.market_processing_us,
            &t.price_queue_us,&t.frame_to_worker_us].iter().all(|h|h.count==self.counts.websocket_evaluations),
            "incomplete WebSocket pipeline timing");
        Ok(())
    }
}
#[derive(Default, serde::Serialize)]
pub struct ObservationReport {
    pub error: Option<String>, counts: Counts,
    execution_layout: &'static str, output_layout: &'static str,
    output_capacity: usize, reference_comparison_performed: bool,
    lost_events: u64, max_queue_depth: usize, last_sequence: u64, last_revision: u64,
    lag_incidents: u64, first_loss_at_us: Option<u64>, last_loss_at_us: Option<u64>,
    resynchronizations: u64, awaiting_snapshot: bool, unverified_events: u64, unverified_ranges: u64,
    state_ready_to_consumer_us: Histogram, websocket_state_ready_to_consumer_us: Histogram,
    websocket_frame_to_state_consumer_us: Histogram, websocket_frame_to_range_consumer_us: Histogram,
    completion_to_consumer_us: Histogram, websocket_frame_to_completion_consumer_us: Histogram,
    ranges_by_kind: BTreeMap<&'static str,u64>, ready_anchors_by_kind: BTreeMap<&'static str,u64>,
    websocket_ready_anchors_by_kind: BTreeMap<&'static str,u64>,
    candidate_examples:Vec<SignalCandidate>, consumer_processing_ns: Histogram,
    max_active_candidates:usize, final_active_candidates:usize,
    consumer_logging_ns: u64,
}
impl ObservationReport {
    pub fn ensure_complete(&self) -> Result<()> {
        ensure!(self.error.is_none(),"signal consumer failed: {}",self.error.as_deref().unwrap_or(""));
        Ok(())
    }
    pub fn has_delivery_loss(&self) -> bool { self.lost_events>0 }
    pub fn ensure_integrity(&self) -> Result<()> {
        self.ensure_complete()?;
        ensure!(self.lost_events==0,"signal consumer lost {} events",self.lost_events);
        Ok(())
    }
    pub fn ensure_calculation_evidence(&self) -> Result<()> {
        self.ensure_integrity()?;
        ensure!(self.counts.websocket_evaluations>0
            && self.counts.websocket_ready_anchors.iter().any(|&n|n>0),
            "no fully priceable topology queried from real WebSocket inputs");
        Ok(())
    }
    pub fn ensure_live_evidence(&self) -> Result<()> {
        self.ensure_calculation_evidence()?;
        ensure!(self.counts.websocket_candidate_ranges>0,"no real WebSocket candidate range observed");
        Ok(())
    }
}
#[derive(Default)]
struct Mirror {
    plan: Option<Arc<SignalPlan>>, pools: Vec<SignalPool>,
    prices: Vec<Option<Arc<SignalPoolPrice>>>,
    revision: u64, generation: u64,
    threshold:i64, active:HashSet<(usize,bool)>,
}
impl Mirror {
    fn state(&mut self, s: &SignalStateUpdate) -> Result<()> {
        ensure!(s.revision==self.revision+1,"state revision gap");
        if let Some(snapshot)=&s.snapshot {
            ensure!(snapshot.plan.graph_generation==s.graph_generation
                && snapshot.plan.log_weight_scale==SIGNAL_LOG_SCALE && snapshot.pools.len()==snapshot.prices.len(),"invalid state snapshot");
            self.plan=Some(snapshot.plan.clone()); self.pools=snapshot.pools.clone(); self.prices=snapshot.prices.clone();
            self.generation=s.graph_generation;
            self.threshold=snapshot.threshold_units;
            self.active.clear();
            for candidate in &snapshot.candidates {
                self.witness(candidate.topology_id,candidate.reverse,&candidate.witness)?;
                ensure!(self.active.insert((candidate.topology_id,candidate.reverse)),
                    "duplicate candidate snapshot key");
            }
        } else { ensure!(self.plan.is_some() && s.graph_generation==self.generation,"state update without matching snapshot"); }
        if s.clear { self.prices.fill(None);self.active.clear(); }
        for c in &s.changes {
            if c.pool_id==self.pools.len() {
                self.pools.push(c.identity.clone()); self.prices.push(None);
            }
            ensure!(self.pools.get(c.pool_id)==Some(&c.identity),"price delta identity mismatch");
            self.prices[c.pool_id]=c.price.clone();
        }
        self.revision=s.revision;
        Ok(())
    }
    fn candidate(&mut self,b:&SignalCandidateBatch,c:&SignalCandidate)->Result<()>{
        ensure!(b.revision==self.revision&&b.graph_generation==self.generation,
            "candidate references another state revision");
        ensure!(b.threshold_units==self.threshold,"candidate threshold differs from snapshot");
        let key=(c.topology_id,c.reverse);
        if c.change==SignalRangeChange::Exited{
            ensure!(c.witness.is_none()&&self.active.remove(&key),
                "exited candidate missing from current membership or carries witness");
            return Ok(());
        }
        self.witness(c.topology_id,c.reverse,c.witness.as_ref().context("live candidate missing witness")?)?;
        match c.change {
            SignalRangeChange::Entered=>ensure!(self.active.insert(key),"candidate entered twice"),
            SignalRangeChange::Updated=>ensure!(self.active.contains(&key),"candidate updated before entering"),
            SignalRangeChange::Exited=>unreachable!(),
        }
        Ok(())
    }
    fn witness(&self,id:usize,reverse:bool,witness:&SignalWitness)->Result<()>{
        let plan=self.plan.as_ref().context("candidate before plan snapshot")?;
        let top=plan.compact_topologies.get(id).context("candidate topology absent")?;
        ensure!(witness.legs.len()==top.length
            && (2..=4).contains(&top.length)
            && !(top.kind==SignalRouteKind::Direct2&&reverse),
            "candidate topology length or direction mismatch");
        let mut unique=HashSet::new();
        let mut sum=0_i128;
        for (i,leg) in witness.legs.iter().enumerate(){
            let j=if reverse{top.length-1-i}else{i};
            let (a,b)=top.roles[j];
            let (input,output)=if reverse{(b,a)}else{(a,b)};
            ensure!(leg.input_mint==input&&leg.output_mint==output,
                "candidate mint direction mismatch");
            let pool=self.pools.get(leg.slot/2).context("candidate pool missing")?;
            ensure!((pool.mint_a,pool.mint_b)==pair(input,output)
                &&leg.slot%2==usize::from(input!=pool.mint_a)
                &&unique.insert(leg.slot/2),"candidate reused pool or wrong pair");
            let weight=self.prices.get(leg.slot/2).and_then(|p|p.as_ref())
                .and_then(|p|p.log_weight_units[leg.slot%2])
                .context("candidate has no current directed price")?;
            ensure!(weight==leg.log_weight_units,"candidate price revision mismatch");
            sum+=i128::from(weight);
        }
        ensure!(witness.legs.last().is_some_and(|l|l.output_mint==witness.legs[0].input_mint)
            &&sum==i128::from(witness.log_sum_units)
            &&witness.log_sum_units<self.threshold,
            "candidate is not a profitable closed route");
        Ok(())
    }
}
pub async fn observe(mut input: SignalStream, settings: Settings) -> Result<ObservationReport> {
    let mut report=ObservationReport { execution_layout:EXECUTION_LAYOUT,output_layout:OUTPUT_LAYOUT,
        output_capacity:OUTPUT_CAPACITY,..Default::default() };
    let mut mirror=Mirror::default(); let mut pending=false; let mut ranges=[0_u64;3];
    let mut sample_baseline=[0_usize;4]; let mut sample_ws=[0_usize;4];
    let mut ticker=tokio::time::interval(Duration::from_secs(5));
    let outcome: Result<()> = async {
        loop { tokio::select! {
            _=ticker.tick() => {
                let started=Instant::now();
                crate::log_event!(Info,"graph signals factorized evaluations={} ws={} ready_anchors={} ranges={} entered={} updated={} exited={} max_queue={} reference_comparison=false",
                    report.counts.evaluations,report.counts.websocket_evaluations,
                    report.counts.websocket_ready_anchors.iter().sum::<u64>(),report.counts.ranges,
                    report.counts.entered_ranges,report.counts.updated_ranges,report.counts.exited_ranges,report.max_queue_depth);
                if settings.profile {
                    crate::log_event!(Info,"graph signals receiver state_age_mean_us={:?} state_age_max_us={:?} completion_age_max_us={:?}",
                        report.websocket_frame_to_state_consumer_us.report().mean,
                        report.websocket_frame_to_state_consumer_us.report().max,
                        report.websocket_frame_to_completion_consumer_us.report().max);
                }
                report.consumer_logging_ns+=nanos(started);
            },
            event=input.recv() => match event {
                Ok(event) => {
                    let started=Instant::now(); let at=now_us();
                    report.max_queue_depth=report.max_queue_depth.max(input.len());
                    let sequence=match event.as_ref() {
                        SignalEvent::State(s)=>s.sequence,
                        SignalEvent::Candidates(b)=>b.sequence,SignalEvent::Evaluated(e)=>e.sequence,
                    };
                    ensure!(report.awaiting_snapshot || sequence==report.last_sequence+1,"signal event sequence gap");
                    report.last_sequence=sequence;
                    // Deltas cannot repair a missing revision. Keep measuring arrivals,
                    // but resume state-dependent checks only at an existing full snapshot.
                    let recovering=report.awaiting_snapshot
                        && matches!(event.as_ref(),SignalEvent::State(s) if s.snapshot.is_some());
                    if report.awaiting_snapshot && !recovering {report.unverified_events+=1;}
                    match event.as_ref() {
                        SignalEvent::State(s) => {
                            if !report.awaiting_snapshot || recovering {
                                ensure!(!pending,"state replaced before previous evaluation completed");
                                if recovering {mirror.revision=s.revision.checked_sub(1).context("invalid snapshot revision")?;}
                                mirror.state(s)?; pending=true; ranges=[0;3]; report.last_revision=s.revision;
                                if recovering {
                                    report.awaiting_snapshot=false; report.resynchronizations+=1;
                                    crate::log_event!(Warn,"graph signals receiver snapshot_applied=true sequence={sequence} revision={} generation={} resynchronizations={} lost_total={} delivery_valid=false",
                                        s.revision,s.graph_generation,report.resynchronizations,report.lost_events);
                                }
                            }
                            report.counts.state(s);
                            if let Some(us)=at.checked_sub(s.ready_at_us) {
                                report.state_ready_to_consumer_us.add(us);
                                if live(&s.trigger) {report.websocket_state_ready_to_consumer_us.add(us);}
                            }
                            if live(&s.trigger) {
                                if let Some(t)=s.price_pipeline {
                                    if let Some(us)=now_us().checked_sub(t.frame_received_at_us) {
                                        report.websocket_frame_to_state_consumer_us.add(us);
                                    }
                                }
                            }
                        },
                        SignalEvent::Candidates(b)=>{
                            ensure!(!b.candidates.is_empty(),"empty CP6A candidate batch");
                            if report.awaiting_snapshot{report.unverified_ranges+=b.candidates.len() as u64;}
                            else{
                                ensure!(pending,"candidate batch without state");
                                for c in &b.candidates{
                                    mirror.candidate(b,c)?;
                                    ranges[change_id(c.change)]+=1;
                                    let kind=kind_id(mirror.plan.as_ref().unwrap()
                                        .compact_topologies[c.topology_id].kind);
                                    let samples=if live(&b.trigger){&mut sample_ws}else{&mut sample_baseline};
                                    if samples[kind]<settings.sample_limit{
                                        samples[kind]+=1;report.candidate_examples.push(c.clone());
                                    }
                                    if settings.trace{
                                        crate::log_event!(Info,"graph signals cp6a candidate={}",serde_json::to_string(c)?);
                                    }
                                }
                            }
                            report.counts.candidate(b);
                            if live(&b.trigger){
                                if let Some(t)=b.price_pipeline{
                                    if let Some(us)=at.checked_sub(t.frame_received_at_us){
                                        report.websocket_frame_to_range_consumer_us.add(us);
                                    }
                                }
                            }
                        },
                        SignalEvent::Evaluated(e) => {
                            if !report.awaiting_snapshot {
                                ensure!(pending && e.revision==mirror.revision && e.graph_generation==mirror.generation,
                                    "completion references wrong state");
                                ensure!(ranges==e.ranges,"range/completion counts differ");
                                if let Some(c)=&e.catalog {
                                    ensure!(matches!(e.trigger.as_ref(),SignalTrigger::Catalog{..})
                                        && ranges==[0;3] && mirror.active.len()==c.active_candidates,
                                        "catalog candidate snapshot/completion mismatch");
                                }
                                report.max_active_candidates=report.max_active_candidates.max(mirror.active.len());
                            }
                            ensure!(e.ranges.iter().sum::<u64>()==e.ranges_by_kind.iter().sum::<u64>(),
                                "range/completion counts differ");
                            ensure!(e.logarithms_prepared==u64::from(matches!(e.trigger.as_ref(),SignalTrigger::Price {..})),
                                "price logarithm accounting mismatch");
                            report.counts.evaluation(e); pending=false;
                            if let Some(us)=at.checked_sub(e.completed_at_us) {report.completion_to_consumer_us.add(us);}
                            if live(&e.trigger) {
                                if let Some(t)=e.price_pipeline {
                                    if let Some(us)=at.checked_sub(t.frame_received_at_us) {report.websocket_frame_to_completion_consumer_us.add(us);}
                                }
                            }
                        },
                    }
                    report.consumer_processing_ns.add(nanos(started));
                },
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    let at=now_us();
                    report.lost_events+=n; report.lag_incidents+=1;
                    report.first_loss_at_us.get_or_insert(at); report.last_loss_at_us=Some(at);
                    report.max_queue_depth=report.max_queue_depth.max(input.len());
                    report.awaiting_snapshot=true; mirror=Mirror::default(); pending=false; ranges=[0;3];
                    crate::log_event!(Warn,"graph signals channel=signal_output lost={n} lost_total={} lag_incidents={} last_sequence={} last_valid_revision={} queue_depth={} awaiting_snapshot=true continuing=true delivery_valid=false",
                        report.lost_events,report.lag_incidents,report.last_sequence,report.last_revision,input.len());
                },
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }}
        ensure!(!pending,"signal stream closed during an evaluation");
        Ok(())
    }.await;
    report.error=outcome.err().map(|e|format!("{e:#}"));
    report.final_active_candidates=mirror.active.len();
    report.ranges_by_kind=names(report.counts.ranges_by_kind);
    report.ready_anchors_by_kind=names(report.counts.ready_anchors);
    report.websocket_ready_anchors_by_kind=names(report.counts.websocket_ready_anchors);
    crate::log_event!(Info,"graph signals consumer complete={} events={} ranges={} lost={} awaiting_snapshot={} delivery_valid={} error={:?}",
        report.error.is_none(),report.counts.events,report.counts.ranges,report.lost_events,
        report.awaiting_snapshot,report.ensure_integrity().is_ok(),report.error);
    Ok(report)
}
