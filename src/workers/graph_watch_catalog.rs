//! Read-only projection of the V10 graph into a generational, typed watch catalog.
//! No source calls, prices, market-quality scoring, execution or strategy filters.
//! All classifications are facts derivable from validated V10 group roles.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use solana_pubkey::Pubkey;
use tokio::sync::watch;

use super::liquidity_graph::{GraphSnapshot, Group, GroupKind};

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
    fn of(group: &Group) -> Self {
        match (group.kind, group.conversion.is_some()) {
            (GroupKind::Direct, false) => Self::Direct2,
            (GroupKind::Direct, true) => Self::Direct3,
            (GroupKind::Bridge, false) => Self::Triangle3,
            (GroupKind::Bridge, true) => Self::Triangle4,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MintRole {
    Settlement,
    Target,
    Bridge,
    ConversionQuote,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PoolRole {
    DirectSettlement,
    DirectQuote,
    Intermediate,
    BridgeReturn,
    Conversion,
}

/// A role is only meaningful in the stated route category, not globally.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MintContext {
    pub kind: RouteKind,
    pub role: MintRole,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PoolContext {
    pub kind: RouteKind,
    pub role: PoolRole,
}

#[derive(Clone, Debug)]
pub struct MintWatch {
    pub address: Pubkey,
    pub contexts: Arc<[MintContext]>,
    /// GraphSnapshot group IDs; only valid for this catalog generation.
    pub group_ids: Arc<[u32]>,
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
    /// Canonical, unordered mint addresses.
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
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
    mint_index: HashMap<Pubkey, usize>,
    pool_index: HashMap<Pubkey, usize>,
    pair_index: HashMap<MintPair, usize>,
}

impl WatchCatalog {
    pub fn empty() -> Self {
        Self {
            generation: 0,
            graph: Arc::new(GraphSnapshot::default()),
            mints: Arc::from([]),
            pools: Arc::from([]),
            pairs: Arc::from([]),
            mint_index: HashMap::new(),
            pool_index: HashMap::new(),
            pair_index: HashMap::new(),
        }
    }

    pub fn for_mint(&self, address: Pubkey) -> Option<&MintWatch> {
        self.mint_index.get(&address).map(|&i| &self.mints[i])
    }

    pub fn for_pool(&self, address: Pubkey) -> Option<&PoolWatch> {
        self.pool_index.get(&address).map(|&i| &self.pools[i])
    }

    pub fn for_pair(&self, a: Pubkey, b: Pubkey) -> Option<&PairWatch> {
        self.pair_index.get(&pair(a, b)).map(|&i| &self.pairs[i])
    }
}

#[derive(Default)]
struct MintAccumulator {
    contexts: BTreeSet<MintContext>,
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

fn mint_role(
    mints: &mut BTreeMap<Pubkey, MintAccumulator>,
    address: Pubkey,
    kind: RouteKind,
    role: MintRole,
) {
    let entry = mints.entry(address).or_default();
    entry.contexts.insert(MintContext { kind, role });
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
    let mut mint_state = BTreeMap::<Pubkey, MintAccumulator>::new();
    let mut pool_state = BTreeMap::<Pubkey, PoolAccumulator>::new();

    for group in &graph.groups {
        let kind = RouteKind::of(group);
        let settlement = group.settlement;
        ensure!(settlement == wsol || settlement == usdc,
            "catalog has non-WSOL/USDC settlement");
        let quote = if settlement == wsol { usdc } else { wsol };
        let target = group.target;
        mint_role(&mut mint_state, settlement, kind, MintRole::Settlement);
        mint_role(&mut mint_state, target, kind, MintRole::Target);
        if group.conversion.is_some() {
            mint_role(&mut mint_state, quote, kind, MintRole::ConversionQuote);
        }

        for &address in group.direct_s.iter() {
            pool_role(&mut pool_state, address, target, settlement,
                kind, PoolRole::DirectSettlement)?;
        }
        for &address in group.direct_q.iter() {
            pool_role(&mut pool_state, address, target, quote,
                kind, PoolRole::DirectQuote)?;
        }
        if let Some(bridge) = group.bridge {
            mint_role(&mut mint_state, bridge, kind, MintRole::Bridge);
            let middle = group.intermediate.context("bridge group without intermediate pool")?;
            pool_role(&mut pool_state, middle, target, bridge,
                kind, PoolRole::Intermediate)?;
            let return_quote = if group.conversion.is_some() { quote } else { settlement };
            for &address in group.bridge_pools.iter() {
                pool_role(&mut pool_state, address, bridge, return_quote,
                    kind, PoolRole::BridgeReturn)?;
            }
        }
        if let Some(conversion) = group.conversion {
            pool_role(&mut pool_state, conversion, settlement, quote,
                kind, PoolRole::Conversion)?;
        }
    }

    ensure!(mint_state.len() == graph.token_index.len(),
        "catalog mint set differs from graph token index");
    ensure!(pool_state.len() == graph.pool_index.len(),
        "catalog pool set differs from graph pool index");

    let mut pair_state = BTreeMap::<MintPair, PairAccumulator>::new();
    for (&address, entry) in &pool_state {
        let key = entry.pair.context("catalog pool has no mint pair")?;
        let pair_entry = pair_state.entry(key).or_default();
        pair_entry.pools.insert(address);
        pair_entry.contexts.extend(entry.contexts.iter().copied());
    }

    let mints: Vec<MintWatch> = mint_state.into_iter().map(|(address, state)| MintWatch {
        address,
        contexts: Arc::from(state.contexts.into_iter().collect::<Vec<_>>()),
        group_ids: Arc::from(graph.for_token(address)),
    }).collect();
    let pools: Vec<PoolWatch> = pool_state.into_iter().map(|(address, state)| {
        let (mint_a, mint_b) = state.pair.expect("catalog pool pair established");
        let pair_pool_count = pair_state[&(mint_a, mint_b)].pools.len();
        PoolWatch {
            address, mint_a, mint_b,
            contexts: Arc::from(state.contexts.into_iter().collect::<Vec<_>>()),
            group_ids: Arc::from(graph.for_pool(address)),
            pair_pool_count,
        }
    }).collect();
    let pairs: Vec<PairWatch> = pair_state.into_iter().map(|((mint_a, mint_b), state)| PairWatch {
        mint_a, mint_b,
        pools: Arc::from(state.pools.into_iter().collect::<Vec<_>>()),
        contexts: Arc::from(state.contexts.into_iter().collect::<Vec<_>>()),
    }).collect();

    let mint_index = mints.iter().enumerate().map(|(i, mint)| (mint.address, i)).collect();
    let pool_index = pools.iter().enumerate().map(|(i, pool)| (pool.address, i)).collect();
    let pair_index = pairs.iter().enumerate()
        .map(|(i, pair)| ((pair.mint_a, pair.mint_b), i)).collect();

    Ok(WatchCatalog {
        generation: graph.generation,
        graph, mints: Arc::from(mints), pools: Arc::from(pools),
        pairs: Arc::from(pairs), mint_index, pool_index, pair_index,
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
        let catalog = Arc::new(build(graph)?);
        let mint_addresses: Arc<[Pubkey]> =
            Arc::from(catalog.mints.iter().map(|m| m.address).collect::<Vec<_>>());
        last_generation = Some(catalog.generation);
        output.send_replace(catalog.clone());
        if let Some(tx) = &jupiter_mints {
            // A topology generation change need not change the mint universe.
            if tx.borrow().as_ref() != mint_addresses.as_ref() {
                tx.send_replace(mint_addresses);
            }
        }
        println!("graph watch catalog published generation={} groups={} mints={} pools={} mint_pairs={}",
            catalog.generation, catalog.graph.groups.len(),
            catalog.mints.len(), catalog.pools.len(), catalog.pairs.len());
    }
    Ok(())
}

/// Actual watch-channel consumer for the standalone Postgres restore/live mode.
/// Reports each route category and the deduplicated triangle-4 intermediate set.
pub async fn observe(mut input: watch::Receiver<Arc<WatchCatalog>>) -> Result<()> {
    while input.changed().await.is_ok() {
        let catalog = input.borrow_and_update().clone();
        let mut categories = [0_usize; 4];
        for group in &catalog.graph.groups {
            let index = match RouteKind::of(group) {
                RouteKind::Direct2 => 0,
                RouteKind::Direct3 => 1,
                RouteKind::Triangle3 => 2,
                RouteKind::Triangle4 => 3,
            };
            categories[index] += 1;
        }
        let bridge4_pools: Vec<_> = catalog.pools.iter().filter(|p|
            p.contexts.contains(&PoolContext {
                kind: RouteKind::Triangle4, role: PoolRole::Intermediate,
            })
        ).collect();
        let bridge4_pairs: BTreeSet<_> = bridge4_pools.iter()
            .map(|p| (p.mint_a, p.mint_b)).collect();
        let bridge4_mints: BTreeSet<_> = bridge4_pairs.iter()
            .flat_map(|&(a,b)| [a,b]).collect();
        let parallel_pairs = catalog.pairs.iter().filter(|p| p.pools.len() >= 2).count();
        println!("graph watch catalog consumer generation={} direct_2={} direct_3={} triangle_3={} triangle_4={} mints={} pools={} mint_pairs={} parallel_pairs={} triangle4_intermediate_pools={} triangle4_intermediate_pairs={} triangle4_intermediate_mints={}",
            catalog.generation, categories[0], categories[1],
            categories[2], categories[3], catalog.mints.len(),
            catalog.pools.len(), catalog.pairs.len(), parallel_pairs,
            bridge4_pools.len(), bridge4_pairs.len(), bridge4_mints.len());
        // Prove an actual indexed lookup and a group-reference dereference.
        if let Some(pool) = bridge4_pools.first() {
            let selected = catalog.for_pool(pool.address)
                .context("catalog consumer lookup missing published pool")?;
            let group = selected.group_ids.first()
                .and_then(|&id| catalog.graph.group(id))
                .context("catalog consumer group reference invalid")?;
            println!("graph watch catalog sample generation={} pool={} mint_a={} mint_b={} pair_pool_count={} group_refs={} group_settlement={} group_target={}",
                catalog.generation, selected.address, selected.mint_a, selected.mint_b,
                selected.pair_pool_count, selected.group_ids.len(),
                group.settlement, group.target);
        }
    }
    Ok(())
}
