//! V10 liquidity graph: typed Registry updates -> immutable group catalog and inverted indices.
//! No RPC, database, protocol-account interpretation, signal producer, or transaction building.

use std::{
    collections::{HashMap, HashSet},
    mem::size_of,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast, watch}, task::JoinSet, time::{Instant as TokioInstant, sleep_until}};

use crate::core::{CompletionState, PoolState, PoolUpdateBatch};

const WSOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

pub type Pair = (Pubkey, Pubkey);

#[derive(Clone, Copy, Debug)]
pub struct Settings;

impl Settings {
    pub fn from_env() -> Result<Self> {
        if std::env::var_os("GRAPH_MIN_TVL_USD").is_some() {
            crate::log_event!(Warn,"GRAPH_MIN_TVL_USD is ignored; Registry completion admission owns the common TVL gate");
        }
        Ok(Self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct PreparedPool {
    address: Pubkey,
    a: Pubkey,
    b: Pubkey,
}

fn eligible(pool: &PoolState) -> Option<PreparedPool> {
    let (Some(a), Some(b), Some(slot), Some(manifest)) =
        (pool.mint_a, pool.mint_b, pool.completion_validated_slot,
         pool.execution_manifest.as_ref()) else { return None };
    if !pool.completion_admission.is_admitted()
        || pool.completion_state != CompletionState::Prepared || a == b
        || a == Pubkey::default() || b == Pubkey::default()
        || manifest.pool != pool.address || manifest.mint_a != a
        || manifest.mint_b != b || manifest.validated_slot != slot
        || manifest.base_a.is_empty() || manifest.base_b.is_empty() {
        return None;
    }
    Some(PreparedPool { address: pool.address, a, b })
}

/// CP6B observational evidence; never participates in admission or routing.
#[derive(Clone)]
struct TransitionEvidence {
    venue:String,
    admission:String,
    completion:String,
    reason_code:Option<String>,
    rejection:&'static str,
    canonical_tvl_usd:Option<f64>,
    validated_slot:Option<u64>,
    checked_at_ms:Option<u64>,
    last_changed_ms:u64,
}
fn rejection(pool:&PoolState)->&'static str{
    let (Some(a),Some(b))=(pool.mint_a,pool.mint_b) else{return "missing_mint_identity"};
    if a==b||a==Pubkey::default()||b==Pubkey::default(){return "invalid_mint_identity";}
    if !pool.completion_admission.is_admitted(){return "admission_withdrawn";}
    if pool.completion_state!=CompletionState::Prepared{return "not_prepared";}
    let Some(slot)=pool.completion_validated_slot else{return "missing_validated_slot"};
    let Some(manifest)=pool.execution_manifest.as_ref() else{return "missing_manifest"};
    if manifest.pool!=pool.address||manifest.mint_a!=a||manifest.mint_b!=b {
        return "manifest_identity_mismatch";
    }
    if manifest.validated_slot!=slot{return "manifest_slot_mismatch";}
    if manifest.base_a.is_empty()||manifest.base_b.is_empty(){return "manifest_basis_missing";}
    "eligible"
}
fn evidence(pool:&PoolState)->TransitionEvidence{
    TransitionEvidence {
        venue:format!("{:?}",pool.venue),
        admission:format!("{:?}",pool.completion_admission),
        completion:format!("{:?}",pool.completion_state),
        reason_code:pool.completion_reason_code.map(|v|format!("{v:?}")),
        rejection:rejection(pool),
        // This is the canonical observation, NOT necessarily venue-native TVL.
        canonical_tvl_usd:pool.tvl_usd,
        validated_slot:pool.completion_validated_slot,
        checked_at_ms:pool.completion_checked_at_ms,
        last_changed_ms:pool.last_changed_ms,
    }
}
fn evidence_json(value:Option<&TransitionEvidence>)->serde_json::Value{
    match value {
        None=>serde_json::Value::Null,
        Some(e)=>serde_json::json!({
            "venue":e.venue,"admission":e.admission,"completion":e.completion,
            "completion_reason":e.reason_code,"eligibility":e.rejection,
            "canonical_tvl_usd":e.canonical_tvl_usd,"validated_slot":e.validated_slot,
            "completion_checked_at_ms":e.checked_at_ms,"registry_last_changed_ms":e.last_changed_ms,
        })
    }
}
fn now_ms()->u64{
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default()
        .as_millis().min(u128::from(u64::MAX)) as u64
}
fn pair_json(value:Option<Pair>)->serde_json::Value{
    match value{
        None=>serde_json::Value::Null,
        Some((a,b))=>serde_json::json!([a.to_string(),b.to_string()])
    }
}
fn transition_kind(before:Option<PreparedPool>,after:Option<PreparedPool>)->&'static str{
    match (before,after){
        (None,Some(_))=>"ADDED",
        (Some(_),None)=>"REMOVED",
        (Some(a),Some(b)) if pair(a.a,a.b)!=pair(b.a,b.b)=>"PAIR_CHANGED",
        (Some(_),Some(_))=>"PAIR_ORDER_ONLY",
        (None,None)=>"UNCHANGED",
    }
}
#[derive(Clone)]
struct ObservedTransition {
    address:Pubkey,source:&'static str,batch_at_ms:u64,observed_at_ms:u64,
    previous_duration_ms:Option<u64>,
    before:Option<PreparedPool>,after:Option<PreparedPool>,
    before_evidence:Option<TransitionEvidence>,after_evidence:TransitionEvidence,
}
fn transition_json(t:&ObservedTransition,generation:u64,event:&str,
    old_graph:Option<Pair>,new_graph:Option<Pair>,published_effective:bool)->serde_json::Value{
    serde_json::json!({
        "event":event,"generation":generation,"observed_at_ms":t.observed_at_ms,
        "source_received_at_ms":t.batch_at_ms,"pool":t.address.to_string(),
        "batch_source":t.source,"transition":transition_kind(t.before,t.after),
        "before_pair":pair_json(t.before.map(|p|pair(p.a,p.b))),
        "after_pair":pair_json(t.after.map(|p|pair(p.a,p.b))),
        "published_before_pair":pair_json(old_graph),
        "published_after_pair":pair_json(new_graph),
        "published_effective":published_effective,
        "previous_state_duration_ms":t.previous_duration_ms,
        "before":evidence_json(t.before_evidence.as_ref()),
        "after":evidence_json(Some(&t.after_evidence)),
    })
}

fn pair(a: Pubkey, b: Pubkey) -> Pair {
    if a < b { (a, b) } else { (b, a) }
}

#[derive(Clone,Copy,Debug,PartialEq,Eq,Hash,PartialOrd,Ord)]
pub enum RouteTopologyKind { Direct2, Direct3, Triangle3, Triangle4 }

/// One mathematical route shape, never one particular choice of pools.
/// Reverse direction is implicit; each role resolves through pair_pools.
#[derive(Clone,Debug)]
pub struct RouteTopology {
    pub kind: RouteTopologyKind,
    pub settlement: Pubkey,
    pub target: Pubkey,
    pub bridge: Option<Pubkey>,
    pub roles: [(Pubkey, Pubkey); 4],
    pub length: usize,
    pub route_options: u64,
}
impl RouteTopology {
    pub fn directed_roles(&self) -> &[(Pubkey,Pubkey)] { &self.roles[..self.length] }
}

/// Only distinct mint paths are enumerated. Pool alternatives stay factored by
/// pair. The legacy Group projection is retained temporarily for CP5 consumers.
fn build_topologies(
    pairs:&HashMap<Pair,Arc<[Pubkey]>>,
    adjacency:&HashMap<Pubkey,HashSet<Pubkey>>,
    wsol:Pubkey, usdc:Pubkey, target_filter:Option<&HashSet<(Pubkey,Pubkey)>>,
) -> Result<(Arc<[RouteTopology]>,HashMap<Pair,Vec<u32>>)> {
    let mut result=Vec::<RouteTopology>::new();
    let mut index=HashMap::<Pair,Vec<u32>>::new();
    let mut push=|kind,settlement,target,bridge,roles:&[(Pubkey,Pubkey)],options:u64| -> Result<()> {
        ensure!(options>0 && roles.len()>=2 && roles.len()<=4,"empty or invalid compact topology");
        ensure!(result.len()<u32::MAX as usize,"too many compact topologies");
        let id=result.len() as u32;
        let mut storage=[(Pubkey::default(),Pubkey::default());4];
        storage[..roles.len()].copy_from_slice(roles);
        result.push(RouteTopology { kind,settlement,target,bridge,roles:storage,
            length:roles.len(),route_options:options });
        let mut seen=HashSet::new();
        for &(a,b) in roles {
            let p=pair(a,b);
            if seen.insert(p) { index.entry(p).or_default().push(id); }
        }
        Ok(())
    };
    let count=|a,b| -> u64 { pairs.get(&pair(a,b)).map_or(0,|v|v.len() as u64) };
    for (s,q) in [(wsol,usdc),(usdc,wsol)] {
        for &t in adjacency.get(&s).into_iter().flatten() {
            if t==s || target_filter.is_some_and(|f|!f.contains(&(s,t))) { continue; }
            let direct=count(s,t);
            if direct>=2 {
                push(RouteTopologyKind::Direct2,s,t,None,&[(s,t),(t,s)],
                    direct.saturating_mul(direct-1))?;
            }
            if t!=q && direct>0 {
                let quoted=count(t,q);let conversions=count(q,s);
                if quoted>0 && conversions>0 {
                    let options=2_u64.saturating_mul(direct)
                        .saturating_mul(quoted).saturating_mul(conversions);
                    push(RouteTopologyKind::Direct3,s,t,None,&[(s,t),(t,q),(q,s)],options)?;
                }
            }
            for &b in adjacency.get(&t).into_iter().flatten() {
                if b==s || b==t { continue; }
                let middle=count(t,b);
                let back=count(b,s);
                if direct>0 && middle>0 && back>0 {
                    let options=2_u64.saturating_mul(direct)
                        .saturating_mul(middle).saturating_mul(back);
                    push(RouteTopologyKind::Triangle3,s,t,Some(b),&[(s,t),(t,b),(b,s)],options)?;
                }
                if t!=q && b!=q && direct>0 && middle>0 {
                    let back_q=count(b,q);let conversions=count(q,s);
                    if back_q>0 && conversions>0 {
                        let options=2_u64.saturating_mul(direct)
                            .saturating_mul(middle).saturating_mul(back_q).saturating_mul(conversions);
                        push(RouteTopologyKind::Triangle4,s,t,Some(b),
                            &[(s,t),(t,b),(b,q),(q,s)],options)?;
                    }
                }
            }
        }
    }
    // Stable IDs within one snapshot regardless of HashMap adjacency iteration order.
    result.sort_unstable_by_key(|topology| {
        (topology.settlement,topology.target,topology.bridge,match topology.kind {
            RouteTopologyKind::Direct2=>0,
            RouteTopologyKind::Direct3=>1,
            RouteTopologyKind::Triangle3=>2,
            RouteTopologyKind::Triangle4=>3,
        })
    });
    index.clear();
    for (id,topology) in result.iter().enumerate() {
        let mut seen=HashSet::new();
        for &(a,b) in topology.directed_roles() {
            let key=pair(a,b);
            if seen.insert(key) { index.entry(key).or_default().push(id as u32); }
        }
    }
    Ok((result.into(),index))
}

/// An immutable generation shares unchanged pair-alternative arcs with its parent.
/// Route topology identities are independent of alternative-pool combinations.
#[derive(Debug,Default)]
pub struct GraphSnapshot {
    pub generation:u64,
    pub qualified_pools:usize,
    pub mint_pairs:usize,
    pub pair_pools:HashMap<Pair,Arc<[Pubkey]>>,
    pub pool_pairs:HashMap<Pubkey,Pair>,
    pub topologies:Arc<[RouteTopology]>,
    pub pair_topologies:HashMap<Pair,Vec<u32>>,
    pub token_index:HashMap<Pubkey,Vec<u32>>,
    pub route_options:u64,
    pub role_pool_refs:usize,
    pub approx_index_bytes:usize,
    pub changed_pairs:Arc<[Pair]>,
    pub recomputed_targets:usize,
    pub regenerated_topologies:usize,
    pub reused_topologies:usize,
}
impl GraphSnapshot {
    pub fn topologies_for_pair(&self,a:Pubkey,b:Pubkey)->&[u32]{
        self.pair_topologies.get(&pair(a,b)).map_or(&[],Vec::as_slice)
    }
    pub fn topologies_for_pool(&self,pool:Pubkey)->&[u32]{
        self.pool_pairs.get(&pool).map_or(&[],|(a,b)|self.topologies_for_pair(*a,*b))
    }
    pub fn pools_for_pair(&self,a:Pubkey,b:Pubkey)->&[Pubkey]{
        self.pair_pools.get(&pair(a,b)).map_or(&[],|x|x.as_ref())
    }
    pub fn pair_for_pool(&self,pool:Pubkey)->Option<Pair>{
        self.pool_pairs.get(&pool).copied()
    }
    pub fn for_token(&self,mint:Pubkey)->&[u32]{
        self.token_index.get(&mint).map_or(&[],Vec::as_slice)
    }
}
fn create(
    old:Arc<GraphSnapshot>, updates:HashMap<Pubkey,Option<PreparedPool>>,generation:u64,
)->Result<GraphSnapshot>{
    let mut pool_pairs=old.pool_pairs.clone();
    let mut changed_pairs=HashSet::<Pair>::new();
    let mut changes=HashMap::<Pair,(HashSet<Pubkey>,Vec<Pubkey>)>::new();
    for (address,new) in updates {
        let prev=pool_pairs.get(&address).copied();
        let next=new.map(|p|pair(p.a,p.b));
        if prev==next {continue;}
        if let Some(prev)=prev{
            changes.entry(prev).or_default().0.insert(address);
            changed_pairs.insert(prev);
        }
        if let Some(next)=next{
            changes.entry(next).or_default().1.push(address);
            pool_pairs.insert(address,next);
            changed_pairs.insert(next);
        } else {pool_pairs.remove(&address);}
    }
    let mut pairs=old.pair_pools.clone();
    for (key,(removed,added)) in changes {
        let mut list=pairs.get(&key).map_or_else(Vec::new,|v|v.to_vec());
        list.retain(|p|!removed.contains(p));
        list.extend(added);
        list.sort_unstable();list.dedup();
        if list.is_empty(){pairs.remove(&key);}
        else{pairs.insert(key,Arc::from(list));}
    }
    // The compact mint adjacency is at most tens of thousands of entries.
    // Recreating it is cheap and never multiplies the pool-alternative products.
    let mut adjacency=HashMap::<Pubkey,HashSet<Pubkey>>::new();
    for &(a,b) in pairs.keys(){
        adjacency.entry(a).or_default().insert(b);
        adjacency.entry(b).or_default().insert(a);
    }
    let wsol:Pubkey=WSOL.parse()?;
    let usdc:Pubkey=USDC.parse()?;
    // Rebuild only (settlement,target) shapes touching a changed pair. A changed
    // conversion pair legitimately touches every topology using conversion.
    let previous_targets=old.topologies.iter().map(|t|(t.settlement,t.target))
        .collect::<HashSet<_>>();
    let initial=old.generation==0;
    let mut affected=HashSet::<(Pubkey,Pubkey)>::new();
    for &(a,b) in &changed_pairs {
        if initial {break;}
        for &s in &[wsol,usdc]{
            let mut targets=HashSet::from([a,b]);
            targets.extend(adjacency.get(&a).into_iter().flatten().copied());
            targets.extend(adjacency.get(&b).into_iter().flatten().copied());
            if pair(a,b)==pair(wsol,usdc){
                targets.extend(adjacency.get(&s).into_iter().flatten().copied());
            }
            for t in targets {
                if t!=s && (adjacency.get(&s).is_some_and(|n|n.contains(&t))
                    ||previous_targets.contains(&(s,t))){
                    affected.insert((s,t));
                }
            }
        }
        // Removed pairs may be absent in the new adjacency: discover their old paths.
        for &id in old.topologies_for_pair(a,b){
            let top=&old.topologies[id as usize];
            affected.insert((top.settlement,top.target));
        }
    }
    let retained_count=if initial {0}else{old.topologies.iter()
        .filter(|t|!affected.contains(&(t.settlement,t.target))).count()};
    let mut topologies=if initial {
        build_topologies(&pairs,&adjacency,wsol,usdc,None)?.0.to_vec()
    } else {
        let mut retained=old.topologies.iter()
            .filter(|t|!affected.contains(&(t.settlement,t.target)))
            .cloned().collect::<Vec<_>>();
        if !affected.is_empty() {
            let (regenerated,_)=build_topologies(&pairs,&adjacency,wsol,usdc,Some(&affected))?;
            retained.extend(regenerated.iter().cloned());
        }
        retained
    };
    let generated_count=topologies.len().saturating_sub(retained_count);
    topologies.sort_unstable_by_key(|t| (t.settlement,t.target,t.bridge,t.kind));
    ensure!(topologies.windows(2).all(|w|
        (w[0].settlement,w[0].target,w[0].bridge,w[0].kind)
        !=(w[1].settlement,w[1].target,w[1].bridge,w[1].kind)),
        "duplicate compact route identity");
    ensure!(topologies.len()<u32::MAX as usize,"compact graph topology ID overflow");
    let mut pair_topologies=HashMap::<Pair,Vec<u32>>::new();
    let mut token_index=HashMap::<Pubkey,Vec<u32>>::new();
    let mut options=0_u64;
    for (i,t) in topologies.iter().enumerate(){
        let id=i as u32;
        let roles=t.directed_roles();
        ensure!(roles.len()>=2 && roles.len()<=4,"invalid compact topology length");
        ensure!(roles[0].0==t.settlement
            &&roles[roles.len()-1].1==t.settlement
            &&roles.windows(2).all(|r|r[0].1==r[1].0),
            "V10 compact topology route is not a closed mint cycle");
        let mut encountered=HashSet::new();
        ensure!(roles.iter().all(|(a,b)|a!=b && encountered.insert(*a)),
            "V10 compact topology repeats a mint before settlement");
        let mut expected=if t.kind==RouteTopologyKind::Direct2 {1_u64}else{2_u64};
        for (leg,&(a,b)) in roles.iter().enumerate(){
            let count=pairs.get(&pair(a,b)).map_or(0,|p|p.len()) as u64;
            if t.kind==RouteTopologyKind::Direct2 && leg==1 {continue;}
            expected=expected.checked_mul(count).context("compact route total overflow")?;
        }
        if t.kind==RouteTopologyKind::Direct2 {
            let count=pairs.get(&pair(roles[0].0,roles[0].1)).map_or(0,|p|p.len()) as u64;
            expected=count.checked_mul(count.saturating_sub(1))
                .context("direct2 total overflow")?;
        }
        ensure!(expected>0 && expected==t.route_options,
            "stale compact topology options after incremental update");
        options=options.checked_add(t.route_options).context("route options overflow")?;
        let mut seen_pairs=HashSet::new();
        let mut seen_tokens=HashSet::new();
        for &(a,b) in t.directed_roles(){
            let p=pair(a,b);
            ensure!(pairs.contains_key(&p),"topology role references unavailable pair");
            if seen_pairs.insert(p){pair_topologies.entry(p).or_default().push(id);}
            if seen_tokens.insert(a){token_index.entry(a).or_default().push(id);}
            if seen_tokens.insert(b){token_index.entry(b).or_default().push(id);}
        }
    }
    ensure!(pool_pairs.len()==pairs.values().map(|v|v.len()).sum::<usize>(),
        "graph pool/pair identity accounting mismatch");
    let refs=pair_topologies.values().map(Vec::len).sum::<usize>();
    let approx=pool_pairs.len()*size_of::<PreparedPool>()
        +pairs.values().map(|v|v.len()*size_of::<Pubkey>()).sum::<usize>()
        +topologies.len()*size_of::<RouteTopology>()
        +(refs+token_index.values().map(Vec::len).sum::<usize>())*size_of::<u32>();
    let mut changed=changed_pairs.into_iter().collect::<Vec<_>>();
    changed.sort_unstable();
    Ok(GraphSnapshot{generation,qualified_pools:pool_pairs.len(),
        mint_pairs:pairs.len(),pair_pools:pairs,pool_pairs,
        topologies:topologies.into(),pair_topologies,token_index,
        route_options:options,role_pool_refs:refs,approx_index_bytes:approx,
        changed_pairs:changed.into(),
        recomputed_targets:affected.len(),regenerated_topologies:generated_count,
        reused_topologies:retained_count})
}
const REBUILD_SPACING:Duration=Duration::from_secs(3);
/// Registry is the sole writer; every immutable generation publishes atomically.
/// Incoming deltas are coalesced even while the previous graph is being updated.
pub async fn run(
    mut input:broadcast::Receiver<Arc<PoolUpdateBatch>>,
    output:watch::Sender<Arc<GraphSnapshot>>,
    _settings:Settings,
)->Result<()>{
    let mut prepared=HashMap::<Pubkey,PreparedPool>::new();
    let mut dirty=HashMap::<Pubkey,Option<PreparedPool>>::new();
    let mut pending_traces=Vec::<ObservedTransition>::new();
    let mut previous_evidence=HashMap::<Pubkey,TransitionEvidence>::new();
    let mut previous_transition_at=HashMap::<Pubkey,u64>::new();
    let mut generation=0_u64;
    let mut source_open=true;
    let mut pending_batches=0_usize;
    let mut last_source="persistence_restore";
    let mut next_rebuild=TokioInstant::now();
    let mut builds=JoinSet::<Result<(GraphSnapshot,u128,usize,usize,&'static str,Vec<ObservedTransition>,HashMap<Pubkey,Option<Pair>>,bool)>>::new();
    loop {
        if !source_open&&dirty.is_empty()&&builds.is_empty(){break;}
        tokio::select!{
            event=input.recv(),if source_open=>match event {
                Ok(batch)=>{
                    let mut changes=0;
                    for pool in &batch.pools {
                        let next=eligible(pool);
                        let before=prepared.get(&pool.address).copied();
                        if before==next {continue;}
                        let at=now_ms();
                        let duration=previous_transition_at.insert(pool.address,at)
                            .map(|last|at.saturating_sub(last));
                        let before_evidence=previous_evidence.insert(pool.address,evidence(pool));
                        // Initial Registry restoration can add >20k pools. Its
                        // generation is summarized separately, rather than emitting
                        // tens of thousands of uninteresting bootstrap records.
                        if generation>0 {
                            pending_traces.push(ObservedTransition {
                                address:pool.address,source:batch.source,
                                batch_at_ms:batch.received_at_ms,observed_at_ms:at,
                                previous_duration_ms:duration,before,after:next,
                                before_evidence,
                                after_evidence:evidence(pool),
                            });
                        }
                        if let Some(p)=next {prepared.insert(pool.address,p);}
                        else{prepared.remove(&pool.address);}
                        dirty.insert(pool.address,next);
                        changes+=1;
                    }
                    if changes>0 {pending_batches+=1;last_source=batch.source;}
                },
                Err(broadcast::error::RecvError::Lagged(n))=>
                    anyhow::bail!("liquidity graph lost {n} Registry update batches"),
                Err(broadcast::error::RecvError::Closed)=>source_open=false,
            },
            result=builds.join_next(),if !builds.is_empty()=>{
                let (graph,micros,changes,batches,source,traces,before_pairs,bootstrap)=result
                    .context("missing graph delta build")?
                    .context("graph delta task panicked")??;
                let effective=before_pairs.iter().filter(|(pool,prior)|
                    **prior!=graph.pair_for_pool(**pool)).count();
                let no_effect=before_pairs.len().saturating_sub(effective);
                let mut source_counts=HashMap::<&str,usize>::new();
                for t in &traces {*source_counts.entry(t.source).or_default()+=1;}
                crate::log_event!(Info,
                    "graph transition_summary generation={} bootstrap={} observed={} distinct={} effective={} no_effect={} batches={} source_counts={}",
                    graph.generation,bootstrap,traces.len(),before_pairs.len(),
                    effective,no_effect,batches,serde_json::json!(source_counts));
                if !bootstrap {
                    for t in &traces {
                        let prior=before_pairs.get(&t.address).copied().flatten();
                        let after=graph.pair_for_pool(t.address);
                        let published_effective=prior!=after;
                        crate::log_event!(Info,"graph pool_transition event={}",
                            transition_json(t,graph.generation,"observed",
                                prior,after,published_effective));
                    }
                    // One published transition per actual changed identity. The
                    // observed stream above can include multiple flips for one pool.
                    for (&pool,&before) in &before_pairs {
                        let after=graph.pair_for_pool(pool);
                        if before==after {continue;}
                        if let Some(t)=traces.iter().rev().find(|t|t.address==pool) {
                            crate::log_event!(Info,"graph pool_transition event={}",
                                transition_json(t,graph.generation,"published",
                                    before,after,true));
                        }
                    }
                }
                let total=graph.topologies.len();
                crate::log_event!(Info,
                    "graph incremental generation={} source={} changed_pools={} changed_pairs={} affected_targets={} regenerated_topologies={} reused_topologies={} coalesced_batches={} prepared={} pairs={} compact_topologies={} compact_pair_index_refs={} legacy_groups=0 trade_paths={} role_refs={} approx_index_bytes={} build_us={}",
                    graph.generation,source,changes,graph.changed_pairs.len(),
                    graph.recomputed_targets,graph.regenerated_topologies,graph.reused_topologies,
                    batches,graph.qualified_pools,graph.mint_pairs,total,graph.role_pool_refs,
                    graph.route_options,graph.role_pool_refs,graph.approx_index_bytes,micros);
                let old=output.send_replace(Arc::new(graph));
                tokio::task::spawn_blocking(move||drop(old));
            },
            _=sleep_until(next_rebuild),if !dirty.is_empty()&&builds.is_empty()=>{
                generation=generation.checked_add(1).context("graph generation overflow")?;
                let diff=std::mem::take(&mut dirty);
                let traces=std::mem::take(&mut pending_traces);
                let count=diff.len();
                let batches=std::mem::take(&mut pending_batches);
                let source=last_source;
                let old=output.borrow().clone();
                next_rebuild=TokioInstant::now()+REBUILD_SPACING;
                builds.spawn_blocking(move||{
                    let started=Instant::now();
                    let bootstrap=old.generation==0;
                    let before_pairs=diff.keys().map(|pool|(*pool,old.pair_for_pool(*pool)))
                        .collect::<HashMap<_,_>>();
                    let next=create(old,diff,generation)?;
                    Ok((next,started.elapsed().as_micros(),count,batches,source,
                        traces,before_pairs,bootstrap))
                });
            },
        }
    }
    let graph=output.borrow().clone();
    crate::log_event!(Info,"graph complete generation={} prepared={} compact_topologies={} legacy_groups=0",
        graph.generation,graph.qualified_pools,graph.topologies.len());
    Ok(())
}
/// Live observer reads the same immutable generation as every consumer.
pub async fn observe(mut input:watch::Receiver<Arc<GraphSnapshot>>)->Result<()>{
    let mut generations=0;
    while input.changed().await.is_ok(){
        let graph=input.borrow_and_update().clone();
        generations+=1;
        if let Some(t)=graph.topologies.first(){
            crate::log_event!(Info,"graph observer generation={} topologies={} changed_pairs={} first={:?} settlement={} target={} paths={}",
                graph.generation,graph.topologies.len(),graph.changed_pairs.len(),
                t.kind,t.settlement,t.target,t.route_options);
        }
    }
    crate::log_event!(Info,"graph observer complete generations={generations}");
    Ok(())
}
