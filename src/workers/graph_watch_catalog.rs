//! Read-only projection of the V10 graph into a generational, typed watch catalog.
//! No source calls, prices, market-quality scoring, execution or strategy filters.
//! All classifications are facts derivable from validated compact topology roles.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use solana_pubkey::Pubkey;
use tokio::sync::watch;

use super::graph_liquidity::{GraphSnapshot, RouteTopology, RouteTopologyKind};

const WSOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

type MintPair = (Pubkey, Pubkey);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RouteKind {
    Direct2,
    Direct3,
    Triangle3,
    Triangle4,
}

impl RouteKind {
    fn of(topology: &RouteTopology) -> Self {
        match topology.kind {
            RouteTopologyKind::Direct2 => Self::Direct2,
            RouteTopologyKind::Direct3 => Self::Direct3,
            RouteTopologyKind::Triangle3 => Self::Triangle3,
            RouteTopologyKind::Triangle4 => Self::Triangle4,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PoolRole {
    DirectSettlement,
    DirectQuote,
    Intermediate,
    BridgeReturn,
    Conversion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PoolContext {
    pub kind: RouteKind,
    pub role: PoolRole,
}

#[derive(Clone, Debug)]
pub struct MintWatch {
    pub address: Pubkey,
}

#[derive(Clone, Debug)]
pub struct PoolWatch {
    pub address: Pubkey,
    /// Canonical, unordered mint pair; not the on-chain base/quote orientation.
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
    pub contexts: Arc<[PoolContext]>,
    pub group_ids: Arc<[u32]>,
    /// Other catalog pools for the same mint pair, including this pool.
    pub pair_pool_count: usize,
}

#[derive(Clone, Debug)]
pub struct PairWatch {
    pub pools: Arc<[Pubkey]>,
    pub contexts: Arc<[PoolContext]>,
}

/// One atomic graph generation plus all its observable mint/pool classifications.
/// No token is classified as "exotic": a consumer decides exclusion sets and priorities.
#[derive(Debug)]
pub struct WatchCatalog {
    pub generation: u64,
    pub graph: Arc<GraphSnapshot>,
    pub mints: Arc<[MintWatch]>,
    pub pools: Arc<[PoolWatch]>,
    pub pairs: Arc<[PairWatch]>,
    pool_index: HashMap<Pubkey, usize>,
}

impl WatchCatalog {
    pub fn empty() -> Self {
        Self {
            generation: 0,
            graph: Arc::new(GraphSnapshot::default()),
            mints: Arc::from([]),
            pools: Arc::from([]),
            pairs: Arc::from([]),
            pool_index: HashMap::new(),
        }
    }

    pub fn for_pool(&self, address: Pubkey) -> Option<&PoolWatch> {
        self.pool_index.get(&address).map(|&i| &self.pools[i])
    }

}

#[derive(Default)]
struct PoolAccumulator {
    pair: Option<MintPair>,
    contexts: BTreeSet<PoolContext>,
}

#[derive(Default)]
struct PairAccumulator {
    pools: BTreeSet<Pubkey>,
    contexts: BTreeSet<PoolContext>,
}

fn pair(a: Pubkey, b: Pubkey) -> MintPair {
    if a < b { (a, b) } else { (b, a) }
}

fn pool_role(
    pools: &mut BTreeMap<Pubkey, PoolAccumulator>,
    address: Pubkey,
    a: Pubkey,
    b: Pubkey,
    kind: RouteKind,
    role: PoolRole,
) -> Result<()> {
    let expected = pair(a, b);
    let entry = pools.entry(address).or_default();
    match entry.pair {
        Some(previous) => ensure!(previous == expected,
            "catalog pool {} has inconsistent mint pairs", address),
        None => entry.pair = Some(expected),
    }
    entry.contexts.insert(PoolContext { kind, role });
    Ok(())
}

fn build(graph: Arc<GraphSnapshot>) -> Result<WatchCatalog> {
    let wsol: Pubkey = WSOL.parse()?;
    let usdc: Pubkey = USDC.parse()?;
    let mut mint_state = BTreeSet::<Pubkey>::new();
    let mut pool_state = BTreeMap::<Pubkey, PoolAccumulator>::new();

    for topology in graph.topologies.iter() {
        let kind=RouteKind::of(topology);
        let settlement=topology.settlement;
        ensure!(settlement==wsol||settlement==usdc,"catalog invalid settlement");
        let quote=if settlement==wsol{usdc}else{wsol};
        let target=topology.target;
        mint_state.insert(settlement);
        mint_state.insert(target);
        if kind==RouteKind::Direct3||kind==RouteKind::Triangle4 {
            mint_state.insert(quote);
        }
        if let Some(bridge)=topology.bridge {
            mint_state.insert(bridge);
        }
        for (i,&(a,b)) in topology.directed_roles().iter().enumerate() {
            let role=match (kind,i) {
                (RouteKind::Direct2,_) | (_,0)=>PoolRole::DirectSettlement,
                (RouteKind::Direct3,1)=>PoolRole::DirectQuote,
                (RouteKind::Direct3,2)|(RouteKind::Triangle4,3)=>PoolRole::Conversion,
                (RouteKind::Triangle3,1)|(RouteKind::Triangle4,1)=>PoolRole::Intermediate,
                (RouteKind::Triangle3,2)|(RouteKind::Triangle4,2)=>PoolRole::BridgeReturn,
                _=>anyhow::bail!("unexpected compact topology role"),
            };
            for &address in graph.pools_for_pair(a,b) {
                pool_role(&mut pool_state,address,a,b,kind,role)?;
            }
        }
    }
    ensure!(pool_state.len()==graph.pair_topologies.keys().map(|p|
        graph.pair_pools.get(p).map_or(0,|v|v.len())).sum::<usize>(),
        "catalog pool set differs from compact topology coverage");

    let mut pair_state = BTreeMap::<MintPair, PairAccumulator>::new();
    for (&address, entry) in &pool_state {
        let key = entry.pair.context("catalog pool has no mint pair")?;
        let pair_entry = pair_state.entry(key).or_default();
        pair_entry.pools.insert(address);
        pair_entry.contexts.extend(entry.contexts.iter().copied());
    }

    let mints: Vec<MintWatch> = mint_state.into_iter().map(|address| MintWatch { address }).collect();
    let pools: Vec<PoolWatch> = pool_state.into_iter().map(|(address, state)| {
        let (mint_a, mint_b) = state.pair.expect("catalog pool pair established");
        let pair_pool_count = pair_state[&(mint_a, mint_b)].pools.len();
        PoolWatch {
            address, mint_a, mint_b,
            contexts: Arc::from(state.contexts.into_iter().collect::<Vec<_>>()),
            group_ids: Arc::from(graph.topologies_for_pool(address)),
            pair_pool_count,
        }
    }).collect();
    let pairs: Vec<PairWatch> = pair_state.into_values().map(|state| PairWatch {
        pools: Arc::from(state.pools.into_iter().collect::<Vec<_>>()),
        contexts: Arc::from(state.contexts.into_iter().collect::<Vec<_>>()),
    }).collect();

    let pool_index = pools.iter().enumerate().map(|(i, pool)| (pool.address, i)).collect();

    Ok(WatchCatalog {
        generation: graph.generation,
        graph, mints: Arc::from(mints), pools: Arc::from(pools),
        pairs: Arc::from(pairs), pool_index,
    })
}

/// Graph in; one full catalog out; optional legacy Mint-address projection for
/// Jupiter Search. Both outputs are derived from the identical graph generation.
pub async fn run(
    mut input: watch::Receiver<Arc<GraphSnapshot>>,
    output: watch::Sender<Arc<WatchCatalog>>,
    jupiter_mints: Option<watch::Sender<Arc<[Pubkey]>>>,
) -> Result<()> {
    let mut last_generation = None;
    while input.changed().await.is_ok() {
        let graph = input.borrow_and_update().clone();
        if last_generation == Some(graph.generation) { continue; }
        // The projection scans the complete graph. Never block Tokio's price/RPC tasks.
        let catalog = Arc::new(tokio::task::spawn_blocking(move || build(graph)).await
            .context("watch catalog build task panicked")??);
        let mint_addresses: Arc<[Pubkey]> =
            Arc::from(catalog.mints.iter().map(|m| m.address).collect::<Vec<_>>());
        last_generation = Some(catalog.generation);
        let old=output.send_replace(catalog.clone());
        // Old catalog projections can own millions of references.
        tokio::task::spawn_blocking(move || drop(old));
        if let Some(tx) = &jupiter_mints {
            // A topology generation change need not change the mint universe.
            if tx.borrow().as_ref() != mint_addresses.as_ref() {
                tx.send_replace(mint_addresses);
            }
        }
        crate::log_event!(Info,"graph watch catalog published generation={} groups={} mints={} pools={} mint_pairs={}",
            catalog.generation, catalog.graph.topologies.len(),
            catalog.mints.len(), catalog.pools.len(), catalog.pairs.len());
    }
    Ok(())
}

/// Live checks use the same compact topology ID generation as Signal Engine.
pub async fn observe(mut input:watch::Receiver<Arc<WatchCatalog>>)->Result<()>{
    let mut n=0_u64;
    while input.changed().await.is_ok(){
        let c=input.borrow_and_update().clone();
        n+=1;
        ensure!(c.generation==c.graph.generation,"watch catalog generation mismatch");
        if let Some(p)=c.pools.first(){
            ensure!(c.graph.pair_for_pool(p.address)==Some((p.mint_a,p.mint_b)),
                "catalog pair differs from graph");
            for &id in p.group_ids.iter().take(8){
                ensure!(c.graph.topologies.get(id as usize).is_some(),"invalid compact topology ID");
            }
        }
        crate::log_event!(Info,"watch catalog consumer generation={} compact_topologies={} watched_pools={}",
            c.generation,c.graph.topologies.len(),c.pools.len());
    }
    crate::log_event!(Info,"watch catalog consumer complete generations={n}");
    Ok(())
}
