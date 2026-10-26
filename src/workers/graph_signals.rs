//! CP6A: factorized canonical topologies, directed-price ranking and one
//! best executable witness per changed topology. Never enumerate route products.
use std::{collections::{BTreeMap,BTreeSet,HashMap,HashSet},env,sync::Arc,
    time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
use anyhow::{Context,Result,bail,ensure};
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast,watch};
use crate::core::{
    market_state::{MarketStateEvent,PoolPriceObservation,PriceEventStream,PriceOrigin,PricePipelineTiming},
    signals::{SignalCandidate,SignalCandidateBatch,SignalEvaluation,SignalEvent,SignalLeg,
        SignalPlan,SignalPool,SignalPoolPrice,SignalPriceChange,SignalRangeBlock,
        SignalRangeChange,SignalRouteKind,SignalSnapshot,SignalStateUpdate,SignalStream,
        SignalTopology,SignalTrigger,SignalWitness,SIGNAL_BLOCK_RANGES,SIGNAL_LOG_SCALE}};
use super::{graph_liquidity::{GraphSnapshot,RouteTopologyKind},graph_watch_catalog::WatchCatalog};

const EXECUTION_LAYOUT:&str="compact_topologies_cp6a";
const OUTPUT_LAYOUT:&str="versioned_best_candidates_cp6a";
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
    fn slot(&self, pool: Pubkey, input: Pubkey, output: Pubkey) -> Result<usize> {
        let id = *self.index.get(&pool).context("pool absent from signal identities")?;
        let p = &self.pools[id];
        ensure!((p.mint_a,p.mint_b) == (input.min(output),input.max(output)), "invalid graph pool role");
        Ok(2 * id + usize::from(input != p.mint_a))
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
impl CompactIndex {
    fn install(&mut self,graph:Arc<GraphSnapshot>,prices:&Prices){
        let mut ranked=HashMap::<(Pubkey,Pubkey),BTreeSet<Key>>::new();
        for (id,identity) in prices.pools.iter().enumerate(){
            if graph.pair_for_pool(identity.address)!=Some((identity.mint_a,identity.mint_b)){continue;}
            for direction in 0..2 {
                let slot=2*id+direction;
                if let Some(weight)=prices.weight(slot,None) {
                    let role=if direction==0 {(identity.mint_a,identity.mint_b)}
                        else {(identity.mint_b,identity.mint_a)};
                    ranked.entry(role).or_default().insert(key(weight,slot));
                }
            }
        }
        self.graph=graph;
        self.ranked=ranked;
        // No candidate calculated against an older topology remains valid.
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
                .map(|r|[l.slot,r.slot]));
            let second=right.and_then(|r|self.first(s,t,Some(r.slot/2))
                .map(|l|[l.slot,r.slot]));
            let result=[first,second].into_iter().flatten().filter_map(|choice|{
                let w0=self.ranked.get(&(s,t))?.iter().find(|k|k.slot==choice[0])?;
                let w1=self.ranked.get(&(t,s))?.iter().find(|k|k.slot==choice[1])?;
                let total=i64::try_from(i128::from(w0.weight)+i128::from(w1.weight)).ok()?;
                Some(Winner{slots:[choice[0],choice[1],0,0],length:2,total})
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
    fn candidates(&mut self,address:Pubkey,prices:&Prices,threshold:i64,
        stats:&mut SignalEvaluation)->Result<Vec<SignalCandidate>>{
        let mut out=Vec::new();
        for &id in self.graph.topologies_for_pool(address) {
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
                let witness=selected.map(|w|self.witness(&definition,w,reverse,prices))
                    .transpose()?;
                if let Some(w)=selected{self.qualifying.insert(key,w);}
                else{self.qualifying.remove(&key);}
                stats.ranges[change_id(change)]+=1;
                stats.ranges_by_kind[kind_id(kind)]+=1;
                stats.anchors_visited+=1;
                out.push(SignalCandidate{topology_id:id as usize,reverse,change,witness});
            }
        }
        Ok(out)
    }
}
struct Work {
    started:Instant,stats:SignalEvaluation,
}
struct Engine {
    prices:Prices,plan:Arc<SignalPlan>,index:CompactIndex,
    output:broadcast::Sender<Arc<SignalEvent>>,settings:Settings,
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
            ranges_by_kind:[0;4],logarithms_prepared:0,price_preparation_ns:0,
            index_update_ns:0,state_ready_ns:0,plan_build_ns:0,query_ns:0,
            event_build_ns:0,publication_ns:0,first_range_ns:None,
            last_range_ns:None,processing_ns:0}}
    }
    fn state(&mut self,work:&mut Work,snapshot:bool,clear:bool,
        changes:Vec<SignalPriceChange>)->Result<()>{
        work.stats.state_ready_ns=nanos(work.started);
        let ready_at_us=now_us();
        let started=Instant::now();
        let snapshot=snapshot.then(||SignalSnapshot{plan:self.plan.clone(),
            pools:self.prices.pools.clone(),prices:self.prices.values.clone()});
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
    fn install(&mut self,graph:Arc<GraphSnapshot>)->Result<()>{
        if graph.generation==self.plan.graph_generation{return Ok(());}
        let compact=graph.topologies.iter().map(|t|SignalTopology{
            kind:match t.kind{RouteTopologyKind::Direct2=>SignalRouteKind::Direct2,
                RouteTopologyKind::Direct3=>SignalRouteKind::Direct3,
                RouteTopologyKind::Triangle3=>SignalRouteKind::Triangle3,
                RouteTopologyKind::Triangle4=>SignalRouteKind::Triangle4},
            settlement:t.settlement,target:t.target,bridge:t.bridge,
            roles:t.roles,length:t.length}).collect::<Vec<_>>();
        self.index.install(graph.clone(),&self.prices);
        let plan=Arc::new(SignalPlan{graph_generation:graph.generation,
            log_weight_scale:SIGNAL_LOG_SCALE,roles:Arc::from([]),groups:Arc::from([]),
            compact_topologies:compact.into(),route_options:graph.route_options});
        let old=std::mem::replace(&mut self.plan,plan);
        tokio::task::spawn_blocking(move||drop(old));
        Ok(())
    }
}
pub async fn run(mut input:PriceEventStream,mut catalogs:watch::Receiver<Arc<WatchCatalog>>,
    output:broadcast::Sender<Arc<SignalEvent>>,settings:Settings)->Result<ProductionReport>{
    let initial=Arc::new(SignalPlan{graph_generation:0,log_weight_scale:SIGNAL_LOG_SCALE,
        roles:Arc::from([]),groups:Arc::from([]),compact_topologies:Arc::from([]),route_options:0});
    let mut e=Engine{prices:Prices::default(),plan:initial,index:CompactIndex::default(),
        output,settings,sequence:0,revision:0,
        threshold:(-(settings.threshold_bps/10_000.0).ln_1p()*SIGNAL_LOG_SCALE as f64).round() as i64,
        production:ProductionReport::default()};
    let outcome:Result<()>=async{
        let started=Instant::now();let received=now_us();
        let mut work=e.begin(SignalTrigger::Catalog{generation:0},started,received,None,input.len());
        e.state(&mut work,true,false,Vec::new())?;e.evaluate(work)?;
        let mut catalog_open=true;
        crate::log_event!(Info,"graph signals cp6a started=true output_layout={} compact=true",OUTPUT_LAYOUT);
        loop{tokio::select!{
            change=catalogs.changed(),if catalog_open=>{
                if change.is_err(){catalog_open=false;continue;}
                let c=catalogs.borrow_and_update().clone();
                if c.generation==e.plan.graph_generation{continue;}
                let started=Instant::now();let received=now_us();
                for p in c.pools.iter(){e.prices.bind(p.address,p.mint_a,p.mint_b)?;}
                e.install(c.graph.clone())?;
                let mut work=e.begin(SignalTrigger::Catalog{generation:c.generation},
                    started,received,None,input.len());
                work.stats.plan_build_ns=nanos(started);
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
                        e.index.install(e.index.graph.clone(),&e.prices);
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
                    e.index.install(e.index.graph.clone(),&e.prices);
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
    websocket_ranges: u64, websocket_candidate_ranges: u64,
    groups_visited: u64, logical_routes_affected: u64, anchors_visited: u64, range_queries: u64,
    ready_anchors: [u64;4], websocket_ready_anchors: [u64;4], ranges_by_kind: [u64;4],
}
impl Counts {
    fn state(&mut self, s: &SignalStateUpdate) {
        self.events+=1; self.state_updates+=1; self.changed_pools+=s.changes.len() as u64;
        self.plan_snapshots+=u64::from(s.snapshot.is_some()); self.resets+=u64::from(s.clear);
    }
    fn block(&mut self, b: &SignalRangeBlock) {
        self.events+=1; self.range_blocks+=1; self.ranges+=b.ranges.len() as u64;
        for r in &b.ranges {
            match r.change {
                SignalRangeChange::Entered => self.entered_ranges+=1,
                SignalRangeChange::Updated => self.updated_ranges+=1,
                SignalRangeChange::Exited => self.exited_ranges+=1,
            }
            if live(&b.trigger) {
                self.websocket_ranges+=1;
                self.websocket_candidate_ranges+=u64::from(r.change != SignalRangeChange::Exited);
            }
        }
    }
    fn evaluation(&mut self, e: &SignalEvaluation) {
        self.events+=1; self.evaluations+=1;
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
    final_price_pools: usize, final_available_directions: usize,
    max_price_queue_depth: usize, max_output_queue_depth: usize,
}
impl ProductionReport {
    fn plan(&mut self, p: &ExecutionPlan, prices: &Prices, ns: u64) {
        self.catalog_plan_build_ns.add(ns);
        self.last_plan=Some(serde_json::json!({
            "generation":p.data.graph_generation,"pools":prices.pools.len(),"roles":p.roles.len(),
            "groups":p.data.groups.len(),"logical_routes":p.data.route_options,
            "role_slot_references":p.roles.iter().map(|r|r.definition.slots.len()).sum::<usize>(),
            "ordered_entries_at_install":p.roles.iter().map(|r|r.ordered.len()).sum::<usize>(),
            "dependency_bindings":p.graph.pool_index.values().map(Vec::len).sum::<usize>(),
            "dependency_index_shared":true,"build_ns":ns,
        }));
        crate::log_event!(Info,"graph signals plan={}",self.last_plan.as_ref().unwrap());
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
#[derive(Clone, serde::Serialize)]
struct Example {
    graph_generation: u64, revision: u64, trigger: Arc<SignalTrigger>, range: SignalRouteRange,
}
#[derive(Default, serde::Serialize)]
pub struct ObservationReport {
    pub error: Option<String>, counts: Counts,
    execution_layout: &'static str, output_layout: &'static str,
    output_capacity: usize, max_ranges_per_block: usize, reference_comparison_performed: bool,
    lost_events: u64, max_queue_depth: usize, last_sequence: u64, last_revision: u64,
    lag_incidents: u64, first_loss_at_us: Option<u64>, last_loss_at_us: Option<u64>,
    resynchronizations: u64, awaiting_snapshot: bool, unverified_events: u64, unverified_ranges: u64,
    state_ready_to_consumer_us: Histogram, websocket_state_ready_to_consumer_us: Histogram,
    websocket_frame_to_state_consumer_us: Histogram, websocket_frame_to_range_consumer_us: Histogram,
    completion_to_consumer_us: Histogram, websocket_frame_to_completion_consumer_us: Histogram,
    ranges_by_kind: BTreeMap<&'static str,u64>, ready_anchors_by_kind: BTreeMap<&'static str,u64>,
    websocket_ready_anchors_by_kind: BTreeMap<&'static str,u64>,
    examples: Vec<Example>, consumer_processing_ns: Histogram,
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
    prices: Vec<Option<Arc<SignalPoolPrice>>>, slot_roles: Vec<Option<usize>>,
    revision: u64, generation: u64,
}
impl Mirror {
    fn state(&mut self, s: &SignalStateUpdate) -> Result<()> {
        ensure!(s.revision==self.revision+1,"state revision gap");
        if let Some(snapshot)=&s.snapshot {
            ensure!(snapshot.plan.graph_generation==s.graph_generation
                && snapshot.plan.log_weight_scale==SIGNAL_LOG_SCALE && snapshot.pools.len()==snapshot.prices.len(),"invalid state snapshot");
            self.plan=Some(snapshot.plan.clone()); self.pools=snapshot.pools.clone(); self.prices=snapshot.prices.clone();
            self.slot_roles=vec![None;2*self.pools.len()];
            for (id,role) in snapshot.plan.roles.iter().enumerate() {
                for &slot in role.slots.iter() {
                    let entry=self.slot_roles.get_mut(slot).context("snapshot slot outside pool identities")?;
                    ensure!(entry.replace(id).is_none(),"duplicate snapshot role slot");
                }
            }
            self.generation=s.graph_generation;
        } else { ensure!(self.plan.is_some() && s.graph_generation==self.generation,"state update without matching snapshot"); }
        if s.clear { self.prices.fill(None); }
        for c in &s.changes {
            if c.pool_id==self.pools.len() {
                self.pools.push(c.identity.clone()); self.prices.push(None); self.slot_roles.extend([None;2]);
            }
            ensure!(self.pools.get(c.pool_id)==Some(&c.identity),"price delta identity mismatch");
            self.prices[c.pool_id]=c.price.clone();
        }
        self.revision=s.revision;
        Ok(())
    }
    fn range(&self, b: &SignalRangeBlock, r: &SignalRouteRange) -> Result<()> {
        ensure!(b.revision==self.revision && b.graph_generation==self.generation,"range references another state revision");
        let plan=self.plan.as_ref().context("range before state snapshot")?;
        let g=plan.groups.get(r.group_id).context("range group missing")?;
        let direction=usize::from(r.reverse);
        let anchor_role=if r.anchor_is_left {g.left[direction]}else{g.right[direction]};
        let partner_role=if r.anchor_is_left {g.right[direction]}else{g.left[direction]};
        ensure!(partner_role==r.partner_role && self.slot_roles.get(r.anchor_slot)==Some(&Some(anchor_role)),
            "range role mismatch");
        ensure!(r.exclude_pool==(g.kind==SignalRouteKind::Direct2).then_some(r.anchor_slot/2),
            "range same-pool exclusion mismatch");
        let count=match g.kind { SignalRouteKind::Direct2=>2,SignalRouteKind::Triangle4=>4,_=>3 };
        ensure!(r.witness.legs.len()==count 
            && r.lower_inclusive.is_none_or(|l|l<r.upper_exclusive),"invalid range or witness");
        let partner=r.witness.legs.iter().find(|leg|self.slot_roles.get(leg.slot)==Some(&Some(partner_role))
            && leg.slot!=r.anchor_slot).context("witness partner missing")?;
        let w=self.prices.get(partner.slot/2).and_then(|p|p.as_ref())
            .and_then(|p|p.log_weight_units[partner.slot%2]).context("witness partner unavailable")?;
        ensure!(Some(partner.slot/2)!=r.exclude_pool && r.lower_inclusive.is_none_or(|l|w>=l)
            && w<r.upper_exclusive,"witness outside reported interval");
        let threshold=b.threshold_units;
        let before=r.previous_addend.is_some_and(|a|w<threshold-a);
        let after=r.current_addend.is_some_and(|a|w<threshold-a);
        ensure!(match r.change {
            SignalRangeChange::Entered=>!before && after,
            SignalRangeChange::Updated=>before && after,
            SignalRangeChange::Exited=>before && !after,
        },"witness status mismatch");
        let addend=if r.change==SignalRangeChange::Exited {r.previous_addend}else{r.current_addend}.context("witness addend missing")?;
        ensure!(r.witness.log_sum_units==addend+w && r.witness.negative_log_sum.is_finite(),"invalid witness score");
        ensure!(r.witness.legs.iter().map(|l|l.log_weight_units).sum::<i64>()==r.witness.log_sum_units,
            "witness legs do not match the reported score");
        for i in 0..count {
            let next=(i+1)%count;
            ensure!(r.witness.legs[i].output_mint==r.witness.legs[next].input_mint,"witness is not a closed route");
        }
        Ok(())
    }
}
pub async fn observe(mut input: SignalStream, settings: Settings) -> Result<ObservationReport> {
    let mut report=ObservationReport { execution_layout:EXECUTION_LAYOUT,output_layout:OUTPUT_LAYOUT,
        output_capacity:OUTPUT_CAPACITY,max_ranges_per_block:SIGNAL_BLOCK_RANGES,..Default::default() };
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
                        SignalEvent::State(s)=>s.sequence, SignalEvent::Ranges(b)=>b.sequence,
                        SignalEvent::Evaluated(e)=>e.sequence,
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
                        SignalEvent::Ranges(b) => {
                            ensure!(!b.ranges.is_empty() && b.ranges.len()<=SIGNAL_BLOCK_RANGES,"invalid signal range block");
                            if report.awaiting_snapshot {report.unverified_ranges+=b.ranges.len() as u64;}
                            else {
                                ensure!(pending,"range block without state");
                                for r in &b.ranges {
                                    mirror.range(b,r)?; ranges[change_id(r.change)]+=1;
                                    let kind=kind_id(mirror.plan.as_ref().unwrap().groups[r.group_id].kind);
                                    let samples=if live(&b.trigger) {&mut sample_ws}else{&mut sample_baseline};
                                    if samples[kind]<settings.sample_limit {
                                        samples[kind]+=1;
                                        report.examples.push(Example { graph_generation:b.graph_generation,revision:b.revision,
                                            trigger:b.trigger.clone(),range:r.clone() });
                                    }
                                    if settings.trace {
                                        crate::log_event!(Info,"graph signals range={}",serde_json::to_string(r)?);
                                    }
                                }
                            }
                            report.counts.block(b);
                            if live(&b.trigger) {
                                if let Some(t)=b.price_pipeline {
                                    if let Some(us)=at.checked_sub(t.frame_received_at_us) {report.websocket_frame_to_range_consumer_us.add(us);}
                                }
                            }
                        },
                        SignalEvent::Evaluated(e) => {
                            if !report.awaiting_snapshot {
                                ensure!(pending && e.revision==mirror.revision && e.graph_generation==mirror.generation,
                                    "completion references wrong state");
                                ensure!(ranges==e.ranges,"range/completion counts differ");
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
    report.ranges_by_kind=names(report.counts.ranges_by_kind);
    report.ready_anchors_by_kind=names(report.counts.ready_anchors);
    report.websocket_ready_anchors_by_kind=names(report.counts.websocket_ready_anchors);
    crate::log_event!(Info,"graph signals consumer complete={} events={} ranges={} lost={} awaiting_snapshot={} delivery_valid={} error={:?}",
        report.error.is_none(),report.counts.events,report.counts.ranges,report.lost_events,
        report.awaiting_snapshot,report.ensure_integrity().is_ok(),report.error);
    Ok(report)
}
