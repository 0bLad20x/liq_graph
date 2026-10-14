//! DLMM execution completion: registry candidate -> RPC account observations -> V10 manifest.
//! No persistence, no writes to Pool Registry, and no hidden RPC clients.

use std::{collections::HashSet, env, sync::Arc};
use anyhow::{anyhow, bail, ensure, Context, Result};
use solana_pubkey::Pubkey;
use tokio::sync::broadcast;

use crate::{
    core::Venue,
    workers::{
        pool_registry::{CompletionState, PoolState, PoolUpdateBatch},
        rpc_transport::{AccountData, AccountView, RpcAnswer, RpcHandle, RpcOperation},
    },
};

const DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const MEMO: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const POOL_DISC: [u8; 8] = [33, 11, 49, 98, 181, 101, 177, 13];
const ARRAY_DISC: [u8; 8] = [92, 142, 92, 220, 5, 148, 70, 181];
const BITMAP_DISC: [u8; 8] = [80, 111, 124, 113, 55, 237, 18, 5];
const ARRAY_BYTES: usize = 56 + 70 * 144;
const BITMAP_BYTES: usize = 40 + 2 * 12 * 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionStatus {
    Prepared,
    Incomplete,
    Retryable,
}

#[derive(Clone, Debug)]
pub struct AccountMeta {
    pub address: Pubkey,
    pub writable: bool,
}

#[derive(Clone, Debug)]
pub struct ExecutionManifest {
    pub pool: Pubkey,
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
    pub validated_slot: u64,
    pub base_a: Arc<[AccountMeta]>,
    pub base_b: Arc<[AccountMeta]>,
}

#[derive(Clone, Debug)]
pub struct CompletionResult {
    pub pool: Pubkey,
    pub status: CompletionStatus,
    pub validated_slot: Option<u64>,
    pub manifest: Option<Arc<ExecutionManifest>>,
    pub reason: Option<String>,
}

#[derive(Debug)]
pub struct CompletionResultBatch {
    pub source: &'static str,
    pub results: Vec<CompletionResult>,
}

#[derive(Clone, Copy, Debug)]
pub struct Settings {
    pub limit: usize,
    pub account_batch_size: usize,
    pub below: usize,
    pub above: usize,
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        let account_batch_size = env::var("RPC_ACCOUNT_BATCH_SIZE")
            .unwrap_or_else(|_| "100".to_owned()).parse::<usize>()
            .context("invalid RPC_ACCOUNT_BATCH_SIZE")?;
        ensure!((1..=100).contains(&account_batch_size), "invalid account batch size");
        let below = env::var("DLMM_EXECUTION_BELOW")
            .unwrap_or_else(|_| "2".to_owned()).parse::<usize>()
            .context("invalid DLMM_EXECUTION_BELOW")?;
        let above = env::var("DLMM_EXECUTION_ABOVE")
            .unwrap_or_else(|_| "2".to_owned()).parse::<usize>()
            .context("invalid DLMM_EXECUTION_ABOVE")?;
        ensure!(1 + below + above <= 6, "DLMM V10 supports at most six BinArrays");
        Ok(Self { limit: 100, account_batch_size, below, above })
    }
}

#[derive(Clone)]
struct PoolLayout {
    address: Pubkey,
    mint_a: Pubkey,
    mint_b: Pubkey,
    vault_a: Pubkey,
    vault_b: Pubkey,
    oracle: Pubkey,
    active_id: i32,
    bitmap: [u8; 128],
    token_2022_a: bool,
    token_2022_b: bool,
}

fn key(bytes: &[u8], offset: usize) -> Pubkey {
    let mut b = [0_u8; 32];
    b.copy_from_slice(&bytes[offset..offset + 32]);
    Pubkey::new_from_array(b)
}

fn i32_le(bytes: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("checked data layout"))
}

fn i64_le(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("checked data layout"))
}

fn u64_le(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("checked data layout"))
}

fn program() -> Result<Pubkey> { Ok(DLMM_PROGRAM.parse()?) }

fn pda(pool: Pubkey, seed: &[u8], index: Option<i32>, owner: &Pubkey) -> Pubkey {
    let pool_bytes = pool.to_bytes();
    if let Some(index) = index {
        Pubkey::find_program_address(&[seed, &pool_bytes, &i64::from(index).to_le_bytes()], owner).0
    } else {
        Pubkey::find_program_address(&[seed, &pool_bytes], owner).0
    }
}

fn decode_pool(account: &AccountData, expected: &PoolState, owner: &Pubkey) -> Result<PoolLayout> {
    ensure!(account.address == expected.address && account.owner == *owner,
        "LbPair address / program owner mismatch");
    let data = &account.bytes;
    ensure!(data.len() == 904 && data[..8] == POOL_DISC,
        "LbPair discriminator or length mismatch");
    let mint_a = expected.mint_a.context("missing registry mint A")?;
    let mint_b = expected.mint_b.context("missing registry mint B")?;
    ensure!(key(data, 88) == mint_a && key(data, 120) == mint_b,
        "LbPair mint identity differs from Registry");
    ensure!(u16::from_le_bytes(data[80..82].try_into()?) > 0
        && data[36] <= 1 && data[880] <= 1 && data[881] <= 1,
        "LbPair bin step, fee mode or token flags invalid");
    let mut bitmap = [0_u8;128];
    bitmap.copy_from_slice(&data[584..712]);
    Ok(PoolLayout {
        address: account.address, mint_a, mint_b,
        vault_a: key(data, 152), vault_b: key(data, 184),
        oracle: key(data, 552), active_id: i32_le(data, 76),
        bitmap, token_2022_a: data[880] == 1, token_2022_b: data[881] == 1,
    })
}

fn decode_bitmap(account: &AccountData, pool: Pubkey, owner: &Pubkey) -> Result<()> {
    ensure!(account.address == pda(pool, b"bitmap", None, owner)
        && account.owner == *owner, "DLMM BitmapExtension identity/owner mismatch");
    ensure!(account.bytes.len() == BITMAP_BYTES
        && account.bytes[..8] == BITMAP_DISC
        && key(&account.bytes, 8) == pool,
        "DLMM BitmapExtension discriminator, length or pool mismatch");
    Ok(())
}

fn bitmap_indices(pool: &PoolLayout, bitmap: Option<&AccountData>) -> Result<Vec<i32>> {
    let center = pool.active_id.div_euclid(70);
    ensure!((-6656..=6655).contains(&center), "active BinArray outside bitmap coverage");
    if bitmap.is_none() {
        ensure!((-512..=511).contains(&center), "outer BinArray requires bitmap extension");
    }
    fn append(bytes: &[u8], mut convert: impl FnMut(usize) -> i32, dest: &mut Vec<i32>) {
        for (byte_index, &byte) in bytes.iter().enumerate() {
            let mut value = byte;
            while value != 0 {
                let bit = value.trailing_zeros() as usize;
                dest.push(convert(byte_index * 8 + bit));
                value &= value - 1;
            }
        }
    }
    let mut indices = Vec::new();
    append(&pool.bitmap, |bit| bit as i32 - 512, &mut indices);
    if let Some(bitmap) = bitmap {
        append(&bitmap.bytes[40..808], |bit| bit as i32 + 512, &mut indices);
        append(&bitmap.bytes[808..1576], |bit| -513 - bit as i32, &mut indices);
    }
    indices.sort_unstable();
    Ok(indices)
}

fn select_indices(indices: &[i32], center: i32, below: usize, above: usize) -> Vec<i32> {
    let left = indices.partition_point(|index| *index < center);
    let mut selected: Vec<_> = indices[..left].iter().rev().take(below).copied().collect();
    selected.reverse();
    if indices.get(left) == Some(&center) { selected.push(center); }
    selected.extend(indices[left..].iter().copied()
        .filter(|index| *index > center).take(above));
    selected
}

fn validate_array(account: &AccountData, pool: Pubkey, index: i32, owner: &Pubkey) -> Result<()> {
    ensure!(account.address == pda(pool, b"bin_array", Some(index), owner)
        && account.owner == *owner, "BinArray PDA / program owner mismatch");
    let data = &account.bytes;
    ensure!(data.len() == ARRAY_BYTES && data[..8] == ARRAY_DISC,
        "BinArray discriminator or length mismatch");
    ensure!(i64_le(data, 8) == i64::from(index) && key(data, 24) == pool,
        "BinArray index or parent pool mismatch");
    for i in 0..70 {
        let pos = 56 + i * 144;
        let x = u64_le(data, pos);
        let y = u64_le(data, pos + 8);
        if x != 0 || y != 0 {
            let price = u128::from_le_bytes(data[pos + 16..pos + 32].try_into()?);
            ensure!(price != 0, "BinArray has nonzero liquidity with zero price");
        }
    }
    Ok(())
}

fn is_initialized(pool: &PoolLayout, bitmap: Option<&AccountData>, index: i32) -> Result<bool> {
    if (-512..=511).contains(&index) {
        let bit = usize::try_from(index + 512)?;
        return Ok(pool.bitmap[bit / 8] & (1 << (bit % 8)) != 0);
    }
    let bitmap = bitmap.context("bitmap extension unavailable")?;
    let (offset, bit) = if index >= 512 {
        (40_usize, usize::try_from(index - 512)?)
    } else {
        (808_usize, usize::try_from(-i64::from(index) - 513)?)
    };
    ensure!(bit < 12 * 64 * 8, "extension bitmap index out of range");
    Ok(bitmap.bytes[offset + bit / 8] & (1 << (bit % 8)) != 0)
}

struct ReadBatch { slot: u64, accounts: Vec<Option<AccountData>> }

async fn read(rpc: &RpcHandle, addresses: Vec<Pubkey>, min_slot: Option<u64>) -> Result<ReadBatch> {
    ensure!(!addresses.is_empty(), "empty completion RPC read");
    let expected = addresses.clone();
    let response = rpc.call(RpcOperation::Multiple {
        addresses, view: AccountView::Full, min_context_slot: min_slot,
    }).await?;
    let RpcAnswer::Multiple { slot, accounts } = response.answer
        else { bail!("RPC returned unexpected answer to getMultipleAccounts"); };
    ensure!(accounts.len() == expected.len(), "RPC result cardinality mismatch");
    ensure!(min_slot.is_none_or(|minimum| slot >= minimum), "RPC result below minContextSlot");
    for (expected, account) in expected.iter().zip(&accounts) {
        if let Some(account) = account {
            ensure!(account.address == *expected && account.view == AccountView::Full
                && account.original_len == Some(account.bytes.len()),
                "RPC returned wrong account address or length");
        }
    }
    Ok(ReadBatch { slot, accounts })
}

fn retryable(pool: Pubkey, reason: impl Into<String>) -> CompletionResult {
    CompletionResult { pool, status: CompletionStatus::Retryable, validated_slot: None,
        manifest: None, reason: Some(reason.into()) }
}

fn prepared(pool: Pubkey, slot: u64, manifest: Option<ExecutionManifest>) -> CompletionResult {
    let status = if manifest.is_some() { CompletionStatus::Prepared }
        else { CompletionStatus::Incomplete };
    CompletionResult { pool, status, validated_slot: Some(slot),
        manifest: manifest.map(Arc::new),
        reason: if status == CompletionStatus::Incomplete {
            Some("no bitmap-initialized BinArrays in execution window".to_owned())
        } else { None },
    }
}

struct Plan {
    candidate: PoolState,
    first: PoolLayout,
    first_slot: u64,
    planning_bitmap: Option<Vec<u8>>,
    indices: Vec<i32>,
    addresses: Vec<Pubkey>,
}

fn manifest(layout: &PoolLayout, slot: u64, arrays: &[Pubkey],
    bitmap: Option<Pubkey>, owner: Pubkey) -> Result<ExecutionManifest> {
    ensure!((1..=6).contains(&arrays.len()), "V10 requires 1..=6 BinArrays");
    let event_authority = Pubkey::find_program_address(&[b"__event_authority"], &owner).0;
    let memo = if layout.token_2022_a || layout.token_2022_b {
        Some(MEMO.parse::<Pubkey>()?)
    } else { None };
    let ro = |address| AccountMeta { address, writable: false };
    let rw = |address| AccountMeta { address, writable: true };
    let directional = |base_mint: Pubkey, reverse: bool| {
        let mut metas = vec![ro(owner), ro(base_mint), ro(event_authority)];
        if let Some(memo) = memo { metas.push(ro(memo)); }
        metas.extend([
            rw(layout.address),
            rw(if reverse { layout.vault_b } else { layout.vault_a }),
            rw(if reverse { layout.vault_a } else { layout.vault_b }),
            rw(layout.oracle),
        ]);
        if let Some(bitmap) = bitmap { metas.push(rw(bitmap)); }
        metas.extend(arrays.iter().copied().map(rw));
        Arc::<[AccountMeta]>::from(metas)
    };
    Ok(ExecutionManifest {
        pool: layout.address, mint_a: layout.mint_a, mint_b: layout.mint_b,
        validated_slot: slot,
        base_a: directional(layout.mint_a, true),
        base_b: directional(layout.mint_b, false),
    })
}

async fn process(rpc: &RpcHandle, candidates: &[PoolState], settings: Settings)
    -> Vec<CompletionResult> {
    let mut results = Vec::with_capacity(candidates.len());
    let owner = match program() {
        Ok(owner) => owner,
        Err(err) => return candidates.iter()
            .map(|pool| retryable(pool.address, err.to_string())).collect(),
    };
    // Initial LbPairs share one RPC context per bounded batch.
    let mut plans = Vec::new();
    for group in candidates.chunks(settings.account_batch_size) {
        let keys: Vec<_> = group.iter().map(|pool| pool.address).collect();
        let first = match read(rpc, keys, None).await {
            Ok(data) => data,
            Err(err) => {
                for pool in group { results.push(retryable(pool.address, format!("LbPair RPC: {err:#}"))); }
                continue;
            }
        };
        let mut initial = Vec::new();
        let mut wanted_extensions = Vec::new();
        for (pool, account) in group.iter().zip(first.accounts.into_iter()) {
            let Some(account) = account else {
                results.push(retryable(pool.address, "LbPair account missing"));
                continue;
            };
            match decode_pool(&account, pool, &owner) {
                Ok(layout) => {
                    let center = layout.active_id.div_euclid(70);
                    let initial_indices = bitmap_indices(&layout, None);
                    let needs_extension = match initial_indices {
                        Ok(indices) => select_indices(&indices, center, settings.below, settings.above).len()
                            < 1 + settings.below + settings.above,
                        Err(_) => true,
                    };
                    if needs_extension {
                        wanted_extensions.push((initial.len(),
                            pda(pool.address, b"bitmap", None, &owner)));
                    }
                    initial.push((pool.clone(), layout, None::<AccountData>));
                }
                Err(err) => results.push(retryable(pool.address, format!("LbPair decode: {err:#}"))),
            }
        }
        // Retrieve optional extensions as a single batch; None is a valid absent account.
        for group in wanted_extensions.chunks(settings.account_batch_size) {
            let addresses = group.iter().map(|(_, address)| *address).collect();
            match read(rpc, addresses, Some(first.slot)).await {
                Ok(batch) => for ((index, _), extension) in
                    group.iter().zip(batch.accounts.into_iter()) {
                    initial[*index].2 = extension;
                },
                Err(err) => {
                    for (index, _) in group {
                        results.push(retryable(initial[*index].0.address,
                            format!("bitmap RPC: {err:#}")));
                    }
                    let failed: HashSet<usize> = group.iter().map(|(i, _)| *i).collect();
                    for index in failed.into_iter() {
                        initial[index].0.completion_state = CompletionState::Incomplete;
                    }
                }
            }
        }
        for (pool, layout, bitmap) in initial {
            // Temporary skip marker for a failed planning RPC; not written into Registry.
            if pool.completion_state != CompletionState::Unknown { continue; }
            let constructed = (|| -> Result<Plan> {
                if let Some(ref bitmap) = bitmap {
                    decode_bitmap(bitmap, pool.address, &owner)?;
                }
                let indices = bitmap_indices(&layout, bitmap.as_ref())?;
                let indices = select_indices(&indices, layout.active_id.div_euclid(70),
                    settings.below, settings.above);
                let mut addresses = vec![pool.address, pda(pool.address, b"bitmap", None, &owner)];
                addresses.extend(indices.iter().map(|index|
                    pda(pool.address, b"bin_array", Some(*index), &owner)));
                ensure!(addresses.len() <= settings.account_batch_size,
                    "per-pool execution account set exceeds RPC_ACCOUNT_BATCH_SIZE");
                Ok(Plan { candidate: pool.clone(), first: layout,
                    first_slot: first.slot, planning_bitmap: bitmap.map(|b|b.bytes),
                    indices, addresses })
            })();
            match constructed {
                Ok(plan) => plans.push(plan),
                Err(err) => results.push(retryable(pool.address, format!("DLMM planning: {err:#}"))),
            }
        }
    }
    // Keep every complete pool plan together in one getMultipleAccounts response context.
    let mut cursor = 0;
    while cursor < plans.len() {
        let start = cursor;
        let mut keys = Vec::new();
        let mut min_slot = 0_u64;
        while cursor < plans.len() {
            let plan = &plans[cursor];
            if !keys.is_empty() && keys.len() + plan.addresses.len() > settings.account_batch_size {
                break;
            }
            keys.extend_from_slice(&plan.addresses);
            min_slot = min_slot.max(plan.first_slot);
            cursor += 1;
        }
        let batch = match read(rpc, keys, Some(min_slot)).await {
            Ok(batch) => batch,
            Err(err) => {
                for plan in &plans[start..cursor] {
                    results.push(retryable(plan.candidate.address, format!("validation RPC: {err:#}")));
                }
                continue;
            }
        };
        let mut offset = 0;
        for plan in &plans[start..cursor] {
            let count = plan.addresses.len();
            let accounts = &batch.accounts[offset..offset + count];
            offset += count;
            let checked = validate(plan, accounts, batch.slot, &owner);
            match checked {
                Ok(manifest) => results.push(prepared(plan.candidate.address, batch.slot, manifest)),
                Err(err) => results.push(retryable(plan.candidate.address,
                    format!("DLMM validation: {err:#}"))),
            }
        }
    }
    results
}

fn validate(plan: &Plan, accounts: &[Option<AccountData>], slot: u64,
    owner: &Pubkey) -> Result<Option<ExecutionManifest>> {
    ensure!(accounts.len() == plan.addresses.len(), "DLMM validation cardinality mismatch");
    ensure!(slot >= plan.first_slot, "DLMM final context slot is stale");
    let current = accounts[0].as_ref().context("LbPair missing on final read")?;
    let latest = decode_pool(current, &plan.candidate, owner)?;
    ensure!(latest.mint_a == plan.first.mint_a && latest.mint_b == plan.first.mint_b
        && latest.vault_a == plan.first.vault_a
        && latest.vault_b == plan.first.vault_b && latest.oracle == plan.first.oracle
        && latest.active_id == plan.first.active_id && latest.bitmap == plan.first.bitmap,
        "LbPair structure / active bin / bitmap changed while preparing manifest");

    let bitmap = accounts[1].as_ref();
    if let Some(bitmap) = bitmap { decode_bitmap(bitmap, latest.address, owner)?; }
    if let Some(previous) = &plan.planning_bitmap {
        ensure!(bitmap.is_some_and(|b|&b.bytes == previous),
            "planning bitmap changed or disappeared before validation");
    }

    let mut arrays = Vec::new();
    for (i, index) in plan.indices.iter().copied().enumerate() {
        let initialized = is_initialized(&latest, bitmap, index)?;
        match (initialized, accounts.get(i + 2).and_then(Option::as_ref)) {
            (true, Some(account)) => {
                validate_array(account, latest.address, index, owner)?;
                arrays.push(account.address);
            }
            (true, None) => bail!("initialized BinArray index {index} missing"),
            (false, Some(account)) => validate_array(account, latest.address, index, owner)?,
            (false, None) => (),
        }
    }
    if arrays.is_empty() {return Ok(None);}
    let bitmap_key = bitmap.map(|b| b.address);
    Ok(Some(manifest(&latest, slot, &arrays, bitmap_key, *owner)?))
}

/// Standalone consumer of Registry's typed change stream. Never reads private Registry state.
pub async fn run(mut input: broadcast::Receiver<Arc<PoolUpdateBatch>>,
    rpc: RpcHandle, settings: Settings,
    output: broadcast::Sender<Arc<CompletionResultBatch>>) -> Result<()> {
    ensure!(settings.limit > 0 && settings.account_batch_size > 0,
        "invalid Completion settings");
    let mut selected = Vec::with_capacity(settings.limit);
    let mut seen = HashSet::new();
    let mut observed = 0_u64;
    loop {
        match input.recv().await {
            Ok(batch) => {
                observed += batch.pools.len() as u64;
                for pool in &batch.pools {
                    if pool.venue == Venue::MeteoraDlmm
                        && pool.completion_state == CompletionState::Unknown
                        && pool.mint_a.is_some() && pool.mint_b.is_some()
                        && pool.tvl_usd.is_some_and(|tvl| tvl > 0.0)
                        && seen.insert(pool.address) {
                        selected.push(pool.clone());
                    }
                    if selected.len() == settings.limit { break; }
                }
                if selected.len() == settings.limit { break; }
            }
            Err(broadcast::error::RecvError::Lagged(count)) =>
                bail!("execution completion missed {count} Registry batches"),
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    println!("execution completion selection observed_pools={observed} selected_unknown={} max={}",
        selected.len(), settings.limit);
    ensure!(!selected.is_empty(), "no unknown DLMM pools with positive TVL observed");

    let mut results = process(&rpc, &selected, settings).await;
    results.sort_by_key(|result| result.pool.to_bytes());
    ensure!(results.len() == selected.len(), "completion output count != selected pool count");
    let prepared = results.iter().filter(|r| r.status == CompletionStatus::Prepared).count();
    let incomplete = results.iter().filter(|r| r.status == CompletionStatus::Incomplete).count();
    let retryable = results.iter().filter(|r| r.status == CompletionStatus::Retryable).count();
    println!("execution completion complete selected={} prepared={} incomplete={} retryable={}",
        results.len(), prepared, incomplete, retryable);
    let batch = Arc::new(CompletionResultBatch {source: "dlmm_execution_completion", results});
    output.send(batch).map_err(|_|anyhow!("completion results have no consumer"))?;
    Ok(())
}

pub async fn observe(mut input: broadcast::Receiver<Arc<CompletionResultBatch>>) -> Result<()> {
    let mut batches = 0_u64;
    let mut results = 0_usize;
    loop {
        match input.recv().await {
            Ok(batch) => {
                batches += 1;
                for result in &batch.results {
                    results += 1;
                    if let Some(manifest) = &result.manifest {
                        println!(
                            "execution manifest pool={} status={:?} slot={} mint_a={} mint_b={} \
                             base_a_accounts={:?} base_b_accounts={:?}",
                            result.pool, result.status, manifest.validated_slot,
                            manifest.mint_a, manifest.mint_b,
                            manifest.base_a.iter().map(|account|
                                format!("{}:{}", account.address, if account.writable {"rw"} else {"ro"}))
                                .collect::<Vec<_>>(),
                            manifest.base_b.iter().map(|account|
                                format!("{}:{}", account.address, if account.writable {"rw"} else {"ro"}))
                                .collect::<Vec<_>>(),
                        );
                    } else {
                        println!("execution result pool={} status={:?} slot={:?} reason={}",
                            result.pool, result.status, result.validated_slot,
                            result.reason.as_deref().unwrap_or("-"));
                    }
                }
                println!("execution completion consumer source={} batches={} results={}",
                    batch.source, batches, results);
            }
            Err(broadcast::error::RecvError::Lagged(count)) =>
                bail!("execution completion output consumer lagged {count} batches"),
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    ensure!(batches > 0, "completion consumer received no batch");
    Ok(())
}
