//! DLMM execution completion: registry candidate -> RPC account observations -> V10 manifest.
//! No persistence, no writes to Pool Registry, and no hidden RPC clients.

use std::{collections::{HashMap, HashSet}, env, sync::Arc, time::Duration};
use tokio::time::Instant;
use anyhow::{anyhow, bail, ensure, Context, Result};
use solana_pubkey::Pubkey;
use tokio::{sync::{broadcast,watch}, task::JoinSet};

use crate::{
    core::{Venue, AccountMeta, ExecutionManifest, BinArrayRef, BinArrayInventory,
        MintProof, CompletionStatus, CompletionResult, CompletionResultBatch},
    workers::{
        pool_registry::{CompletionState, PoolState, PoolUpdateBatch},
        rpc_transport::{AccountData, AccountView, RpcAnswer, RpcHandle, RpcOperation},
    },
};

const DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const MEMO: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const POOL_DISC: [u8; 8] = [33, 11, 49, 98, 181, 101, 177, 13];
const ARRAY_DISC: [u8; 8] = [92, 142, 92, 220, 5, 148, 70, 181];
const BITMAP_DISC: [u8; 8] = [80, 111, 124, 113, 55, 237, 18, 5];
const ARRAY_BYTES: usize = 56 + 70 * 144;
const BITMAP_BYTES: usize = 40 + 2 * 12 * 64;

#[derive(Clone, Copy, Debug)]
pub struct Settings {
    /// Zero means continuous/unbounded; positive limits cap finite live runs.
    pub limit: usize,
    pub account_batch_size: usize,
    pub below: usize,
    pub above: usize,
    pub parallel_batches: usize,
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
        let parallel_batches = env::var("DLMM_COMPLETION_PARALLEL_BATCHES")
            .unwrap_or_else(|_| "32".to_owned()).parse::<usize>()
            .context("invalid DLMM_COMPLETION_PARALLEL_BATCHES")?;
        ensure!((1..=64).contains(&parallel_batches),
            "DLMM_COMPLETION_PARALLEL_BATCHES must be 1..=64");
        Ok(Self { limit: 100, account_batch_size, below, above, parallel_batches })
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
    let provider = response.provider.clone();
    let RpcAnswer::Multiple { slot, accounts } = response.answer
        else { bail!("RPC returned unexpected answer to getMultipleAccounts"); };
    println!("execution completion rpc method=getMultipleAccounts provider={provider} slot={slot} accounts={} present={}",
        accounts.len(), accounts.iter().filter(|item|item.is_some()).count());
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


#[derive(Clone)]
enum MintObservation {
    Verified(MintProof),
    Invalid(String),
    Unavailable(String),
}

/// Mint account ownership proves SPL vs Token-2022 independently of LbPair flags.
fn decode_mint(account: &AccountData) -> Result<MintProof> {
    let spl: Pubkey = SPL_TOKEN.parse()?;
    let token_2022: Pubkey = TOKEN_2022.parse()?;
    ensure!(account.view == AccountView::Full, "mint account must be a full RPC read");
    let is_token_2022 = if account.owner == spl { false }
        else if account.owner == token_2022 { true }
        else { bail!("Mint has unexpected token program owner {}", account.owner); };
    let data = &account.bytes;
    ensure!(data.len() >= 82 && data[45] == 1, "Mint base layout or initialized flag invalid");
    let mut extensions = Vec::new();
    if is_token_2022 && data.len() > 82 {
        // Extended Token-2022 Mints use 82 bytes of base state, 83 bytes of padding,
        // AccountType::Mint at offset 165, then TLV extensions starting at 166.
        ensure!(data.len() >= 166, "extended Token-2022 Mint too short");
        ensure!(data[82..165].iter().all(|byte| *byte == 0),
            "Token-2022 Mint padding is not zero");
        ensure!(data[165] == 1, "Token-2022 account type is not Mint");
        let mut offset = 166;
        while offset < data.len() {
            let trailing = &data[offset..];
            if trailing.iter().all(|byte| *byte == 0) { break; }
            ensure!(trailing.len() >= 4, "truncated Token-2022 Mint extension header");
            let kind = u16::from_le_bytes(trailing[0..2].try_into()?);
            let size = usize::from(u16::from_le_bytes(trailing[2..4].try_into()?));
            if kind == 0 && size == 0 {
                ensure!(trailing[4..].iter().all(|byte| *byte == 0),
                    "nonzero data after Token-2022 Mint TLV terminator");
                break;
            }
            ensure!(kind != 0 && size <= trailing.len() - 4,
                "invalid Token-2022 Mint extension type or size");
            extensions.push(kind);
            offset += 4 + size;
        }
    } else if !is_token_2022 {
        ensure!(data.len() == 82, "SPL Mint account has unexpected length");
    }
    extensions.sort_unstable();
    ensure!(!extensions.windows(2).any(|v|v[0] == v[1]),
        "duplicate Token-2022 Mint extension");
    // Public V10 documentation rejects transfer hooks and confidential transfers.
    // NonTransferable mints cannot participate in a swap.
    let unsupported = extensions.iter().copied()
        .filter(|kind| matches!(*kind, 4 | 9 | 14 | 16 | 24))
        .collect();
    Ok(MintProof { address: account.address, token_2022: is_token_2022,
        extensions, unsupported })
}

fn mint_failure(pending: PendingMintValidation, status: CompletionStatus,
    reason: impl Into<String>) -> CompletionResult {
    CompletionResult { pool: pending.pool, status, validated_slot: Some(pending.slot),
        manifest: None, reason: Some(reason.into()),
        inventory: Some(Arc::new(pending.validated.inventory)), mints: None }
}

async fn complete_mint_verification(rpc: &RpcHandle, account_limit: usize,
    pending: Vec<PendingMintValidation>, results: &mut Vec<CompletionResult>) {
    if pending.is_empty() { return; }
    let mut unique = HashSet::new();
    let mut addresses = Vec::new();
    let mut minimum_slot = 0_u64;
    for item in &pending {
        minimum_slot = minimum_slot.max(item.slot);
        for mint in [item.validated.layout.mint_a, item.validated.layout.mint_b] {
            if unique.insert(mint) { addresses.push(mint); }
        }
    }
    let mut observed: HashMap<Pubkey, MintObservation> = HashMap::new();
    for page in addresses.chunks(account_limit) {
        match read(rpc, page.to_vec(), Some(minimum_slot)).await {
            Ok(batch) => {
                for (key, account) in page.iter().zip(batch.accounts) {
                    let observation = match account {
                        Some(a) => match decode_mint(&a) {
                            Ok(proof) => MintObservation::Verified(proof),
                            Err(error) => MintObservation::Invalid(error.to_string()),
                        },
                        None => MintObservation::Unavailable("Mint account missing".to_owned()),
                    };
                    observed.insert(*key, observation);
                }
            }
            Err(error) => {
                for mint in page {
                    observed.insert(*mint, MintObservation::Unavailable(format!(
                        "Mint RPC read failed: {error:#}")));
                }
            }
        }
    }
    for item in pending {
        let layout = &item.validated.layout;
        let check = (|| -> std::result::Result<[MintProof; 2], (CompletionStatus, String)> {
            let mut proofs = Vec::new();
            for (key, expected_2022) in [(layout.mint_a, layout.token_2022_a),
                (layout.mint_b, layout.token_2022_b)] {
                match observed.get(&key) {
                    Some(MintObservation::Verified(proof)) => {
                        if proof.token_2022 != expected_2022 {
                            return Err((CompletionStatus::Incomplete, format!(
                                "mint {key} Token-2022 owner differs from LbPair flag expected={expected_2022} actual={}",
                                proof.token_2022)));
                        }
                        proofs.push(proof.clone());
                    }
                    Some(MintObservation::Invalid(reason)) =>
                        return Err((CompletionStatus::Incomplete, format!("mint {key}: {reason}"))),
                    Some(MintObservation::Unavailable(reason)) =>
                        return Err((CompletionStatus::Retryable, format!("mint {key}: {reason}"))),
                    None => return Err((CompletionStatus::Retryable,
                        format!("mint {key} RPC response missing"))),
                }
            }
            Ok([proofs.remove(0), proofs.remove(0)])
        })();
        match check {
            Ok(mints) => results.push(prepared(item.pool, item.slot, item.validated, mints)),
            Err((status, reason)) => results.push(mint_failure(item, status, reason)),
        }
    }
}

fn retryable(pool: Pubkey, reason: impl Into<String>) -> CompletionResult {
    CompletionResult { pool, status: CompletionStatus::Retryable, validated_slot: None,
        manifest: None, reason: Some(reason.into()), inventory: None, mints: None }
}

fn prepared(pool: Pubkey, slot: u64, validated: Validated, mints: [MintProof; 2]) -> CompletionResult {
    let unsupported: Vec<String> = mints.iter().filter(|mint| !mint.unsupported.is_empty())
        .map(|mint| format!("{}:{:?}", mint.address, mint.unsupported)).collect();
    let reason = if !unsupported.is_empty() {
        Some(format!("V10 unsupported Token-2022 mint extensions: {}", unsupported.join(", ")))
    } else if validated.manifest.is_none() {
        Some(format!("no bitmap-initialized BinArrays in execution window; internal={} extension={} selected={} validated={}",
            validated.inventory.internal_initialized, validated.inventory.extension_initialized,
            validated.inventory.selected_indices.len(), validated.inventory.validated_indices.len()))
    } else { None };
    let status = if reason.is_some() { CompletionStatus::Incomplete } else { CompletionStatus::Prepared };
    CompletionResult {
        pool, status, validated_slot: Some(slot),
        manifest: if status == CompletionStatus::Prepared { validated.manifest.map(Arc::new) } else { None },
        reason, inventory: Some(Arc::new(validated.inventory)), mints: Some(mints),
    }
}

struct Validated {
    layout: PoolLayout,
    inventory: BinArrayInventory,
    manifest: Option<ExecutionManifest>,
}

struct PendingMintValidation {
    pool: Pubkey,
    slot: u64,
    validated: Validated,
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
    let mut validated_for_mints = Vec::new();
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
        let mut failed_extensions = HashSet::new();
        for (pool, account) in group.iter().zip(first.accounts.into_iter()) {
            let Some(account) = account else {
                results.push(retryable(pool.address, "LbPair account missing"));
                continue;
            };
            match decode_pool(&account, pool, &owner) {
                Ok(layout) => {
                    // Read the extension for every pool: the inventory must cover all initialized
                    // arrays, even when the requested execution window is already satisfied.
                    wanted_extensions.push((initial.len(),
                        pda(pool.address, b"bitmap", None, &owner)));
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
                    for (index, _) in group {
                        failed_extensions.insert(initial[*index].0.address);
                    }
                }
            }
        }
        for (pool, layout, bitmap) in initial {
            if failed_extensions.contains(&pool.address) { continue; }
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
                Ok(validated) => validated_for_mints.push(PendingMintValidation {
                    pool: plan.candidate.address, slot: batch.slot, validated,
                }),
                Err(err) => results.push(retryable(plan.candidate.address,
                    format!("DLMM validation: {err:#}"))),
            }
        }
    }
    complete_mint_verification(rpc, settings.account_batch_size,
        validated_for_mints, &mut results).await;
    results
}

fn validate(plan: &Plan, accounts: &[Option<AccountData>], slot: u64,
    owner: &Pubkey) -> Result<Validated> {
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

    let all_indices = bitmap_indices(&latest, bitmap)?;
    let mut arrays = Vec::new();
    let mut validated_indices = Vec::new();
    for (i, index) in plan.indices.iter().copied().enumerate() {
        let initialized = is_initialized(&latest, bitmap, index)?;
        match (initialized, accounts.get(i + 2).and_then(Option::as_ref)) {
            (true, Some(account)) => {
                validate_array(account, latest.address, index, owner)?;
                arrays.push(account.address);
                validated_indices.push(index);
            }
            (true, None) => bail!("initialized BinArray index {index} missing"),
            (false, Some(account)) => validate_array(account, latest.address, index, owner)?,
            (false, None) => (),
        }
    }
    let inventory = BinArrayInventory {
        registry_tvl_usd: plan.candidate.tvl_usd,
        active_id: latest.active_id,
        active_array_index: latest.active_id.div_euclid(70),
        internal_initialized: latest.bitmap.iter().map(|byte| byte.count_ones() as usize).sum(),
        extension_initialized: bitmap.map_or(0, |b|
            b.bytes[40..].iter().map(|byte| byte.count_ones() as usize).sum()),
        extension_present: bitmap.is_some(),
        initialized: Arc::from(all_indices.iter().copied().map(|index| BinArrayRef {
            index, address: pda(latest.address, b"bin_array", Some(index), owner),
        }).collect::<Vec<_>>()),
        selected_indices: plan.indices.clone(),
        validated_indices,
    };
    let manifest = if arrays.is_empty() { None } else {
        Some(manifest(&latest, slot, &arrays, bitmap.map(|b| b.address), *owner)?)
    };
    Ok(Validated {layout: latest, inventory, manifest})
}

/// This is a worker-local attempt scheduler, not an economic signal engine.
/// It uses only fresh Registry TVL; historic SQL observations never qualify a pool.
const MIN_COMPLETION_TVL_USD: f64 = 1_000.0;
const POOLS_PER_WORK_BATCH: usize = 16;
const RETRY_COOLDOWN: Duration = Duration::from_secs(60);

#[derive(Default)]
struct CandidateScheduler {
    /// Latest qualifying Registry state. Keyed for deduplication and TVL reprioritization.
    candidates: HashMap<Pubkey, PoolState>,
    /// 1 = first completion attempt finished/started; 2 = one delayed technical retry.
    attempts: HashMap<Pubkey, u8>,
    /// Only transient validation/RPC failures may enter the third priority tier.
    retry_after: HashMap<Pubkey, Instant>,
}

impl CandidateScheduler {
    fn update(&mut self, pool: &PoolState) {
        if pool.venue != Venue::MeteoraDlmm { return; }
        if pool.completion_state == CompletionState::Prepared {
            self.candidates.remove(&pool.address);
            self.retry_after.remove(&pool.address);
        } else if pool.mint_a.is_none() || pool.mint_b.is_none()
            || !pool.tvl_usd.is_some_and(|v| v.is_finite() && v > MIN_COMPLETION_TVL_USD)
        {
            // TVL is only a temporary eligibility filter. Keep the attempt and
            // transient retry history if the pool later re-enters the threshold.
            self.candidates.remove(&pool.address);
        } else {
            self.candidates.insert(pool.address, pool.clone());
        }
    }

    fn repeatable_incomplete(pool: &PoolState) -> bool {
        // Re-reading cannot cure a verified V10-incompatible mint.
        !pool.completion_reason.as_deref().is_some_and(|reason|
            reason.contains("V10 unsupported Token-2022 mint extensions")
            || reason.contains("Token-2022 owner differs from LbPair flag"))
    }

    fn tier(&self, pool: &PoolState, now: Instant) -> Option<u8> {
        match self.attempts.get(&pool.address).copied() {
            None => match pool.completion_state {
                CompletionState::Unknown => Some(1),
                CompletionState::Incomplete if Self::repeatable_incomplete(pool) => Some(2),
                _ => None,
            },
            Some(1) if self.retry_after.get(&pool.address).is_some_and(|due| *due <= now) =>
                Some(3),
            _ => None,
        }
    }

    /// Always consume currently known tier-1 before tier-2, and tier-2 before tier-3.
    /// Within a tier, highest current TVL wins (never an inferred profit metric).
    fn next(&self, capacity: usize, now: Instant) -> (u8, Vec<PoolState>) {
        let mut tier=0;
        let mut sorted=Vec::new();
        for pool in self.candidates.values() {
            let Some(available)=self.tier(pool,now) else {continue;};
            if tier==0 || available < tier {
                tier=available;
                sorted.clear();
            }
            if available==tier {sorted.push(pool.clone());}
        }
        sorted.sort_unstable_by(|a,b|{
            b.tvl_usd.unwrap_or_default().total_cmp(&a.tvl_usd.unwrap_or_default())
                .then_with(||a.address.to_bytes().cmp(&b.address.to_bytes()))
        });
        sorted.truncate(capacity);
        (tier,sorted)
    }

    fn pending_retry(&self) -> bool {
        self.retry_after.iter().any(|(address,_)|
            self.candidates.contains_key(address)
            && self.attempts.get(address)==Some(&1))
    }

    fn classify(&self) -> (usize,usize,usize,usize) {
        let mut unknown=0; let mut incomplete=0; let mut retryable=0; let mut blocked=0;
        for pool in self.candidates.values() {
            if self.attempts.get(&pool.address)==Some(&1)
                && self.retry_after.contains_key(&pool.address) {
                retryable+=1;
            } else if !self.attempts.contains_key(&pool.address) {
                match pool.completion_state {
                    CompletionState::Unknown => unknown+=1,
                    CompletionState::Incomplete if Self::repeatable_incomplete(pool) => incomplete+=1,
                    CompletionState::Incomplete => blocked+=1,
                    CompletionState::Prepared => (),
                }
            }
        }
        (unknown,incomplete,retryable,blocked)
    }
}

/// Standalone DLMM consumer. First attempts are never interrupted by retry loops.
/// Each finished partition is published immediately; no 5,000-pool accumulation.
pub async fn run(mut input: broadcast::Receiver<Arc<PoolUpdateBatch>>,
    source_done: watch::Receiver<bool>,
    rpc: RpcHandle, settings: Settings,
    output: broadcast::Sender<Arc<CompletionResultBatch>>) -> Result<()> {
    ensure!(settings.account_batch_size > 0 && settings.parallel_batches > 0,
        "invalid Completion settings");
    let mut scheduler=CandidateScheduler::default();
    let mut tasks=JoinSet::new();
    let mut source_open=true;
    let mut observed=0_u64;
    let mut unique_dlmm_seen=HashSet::new();
    let mut total_attempts=0_usize;
    let mut initial_attempts=0_usize;
    let mut repeated_attempts=0_usize;
    let mut prepared_count=0_usize;
    let mut incomplete_count=0_usize;
    let mut retryable_count=0_usize;
    let started=Instant::now();
    let mut pulse=tokio::time::interval(Duration::from_secs(5));
    pulse.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    println!("dlmm completion started min_tvl_usd={} limit={} (0=unbounded) parallel={} pools_per_batch={} retry_cooldown_s={}",
        MIN_COMPLETION_TVL_USD,settings.limit,settings.parallel_batches,
        POOLS_PER_WORK_BATCH,RETRY_COOLDOWN.as_secs());

    loop {
        // Limit launch concurrency and provide enough headroom for the single Registry writer.
        while tasks.len()<settings.parallel_batches
            && (settings.limit==0 || total_attempts<settings.limit) {
            let capacity=if settings.limit==0 {POOLS_PER_WORK_BATCH}
                else {POOLS_PER_WORK_BATCH.min(settings.limit-total_attempts)};
            if capacity==0 {break;}
            let (tier,selected)=scheduler.next(capacity,Instant::now());
            if selected.is_empty(){break;}
            for pool in &selected {
                let prior=scheduler.attempts.get(&pool.address).copied().unwrap_or(0);
                scheduler.attempts.insert(pool.address,prior+1);
                scheduler.retry_after.remove(&pool.address);
            }
            total_attempts+=selected.len();
            if tier==3 {repeated_attempts+=selected.len();}
            else {initial_attempts+=selected.len();}
            let rpc=rpc.clone();
            tasks.spawn(async move {
                let result=process(&rpc,&selected,settings).await;
                (tier,selected,result)
            });
        }

        let limit_reached=settings.limit>0 && total_attempts>=settings.limit;
        // The Registry cannot close its broadcast while the Completion producer
        // is still alive. Use its independent source-drained signal instead.
        let discovery_finished = !source_open || *source_done.borrow();
        if tasks.is_empty() && (limit_reached || (discovery_finished
            && input.len()==0 && !scheduler.pending_retry()
            && scheduler.next(1,Instant::now()).1.is_empty())) {
            break;
        }

        tokio::select! {
            event=input.recv(),if source_open => match event {
                Ok(batch)=>{
                    observed+=batch.pools.len() as u64;
                    for pool in &batch.pools {
                        if pool.venue==Venue::MeteoraDlmm {
                            unique_dlmm_seen.insert(pool.address);
                        }
                        scheduler.update(pool);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) =>
                    bail!("DLMM completion lost {n} Registry update batches"),
                Err(broadcast::error::RecvError::Closed)=>source_open=false,
            },
            result=tasks.join_next(),if !tasks.is_empty()=>{
                let (tier,selected,results)=result.context("missing completion partition")?
                    .context("DLMM completion partition panicked")?;
                ensure!(results.len()==selected.len(),
                    "DLMM completion partition lost pool results");
                let mut prep=0_usize;let mut inc=0_usize;let mut retry=0_usize;
                let mut missing_arrays=0_usize;let mut unsupported_mints=0_usize;
                for result in &results {
                    if let Some(reason)=result.reason.as_deref() {
                        if reason.contains("no bitmap-initialized BinArrays") {missing_arrays+=1;}
                        if reason.contains("V10 unsupported Token-2022 mint extensions") {unsupported_mints+=1;}
                    }
                    match result.status {
                        CompletionStatus::Prepared=>{
                            prep+=1; scheduler.candidates.remove(&result.pool);
                            scheduler.retry_after.remove(&result.pool);
                        }
                        CompletionStatus::Incomplete=>{inc+=1;}
                        CompletionStatus::Retryable=>{
                            retry+=1;
                            if scheduler.attempts.get(&result.pool)==Some(&1)
                                && scheduler.candidates.contains_key(&result.pool) {
                                scheduler.retry_after.insert(result.pool,Instant::now()+RETRY_COOLDOWN);
                            }
                        }
                    }
                }
                prepared_count+=prep;incomplete_count+=inc;retryable_count+=retry;
                println!("dlmm completion batch tier={} attempts={} prepared={} incomplete={} retryable={} missing_arrays={} unsupported_mints={} total_attempts={} first_attempts={} retries={} elapsed_ms={}",
                    tier,results.len(),prep,inc,retry,missing_arrays,unsupported_mints,
                    total_attempts,initial_attempts,repeated_attempts,started.elapsed().as_millis());
                // Small, fully evaluated batches flow to Registry/Persistence/Graph at once.
                output.send(Arc::new(CompletionResultBatch {
                    source:"dlmm_execution_completion",results
                })).map_err(|_|anyhow!("DLMM completion results have no consumer"))?;
            },
            _=pulse.tick()=>{
                let (unknown,incomplete,retryable,blocked)=scheduler.classify();
                println!("dlmm completion backlog distinct_dlmm_seen={} eligible_unknown={} eligible_incomplete={} cooldown_retryable={} blocked_incompatible={} candidates_in_ram={} active_batches={} first_attempts={} retries={} prepared={} incomplete={} retryable={} observed_updates={}",
                    unique_dlmm_seen.len(),unknown,incomplete,retryable,blocked,scheduler.candidates.len(),tasks.len(),
                    initial_attempts,repeated_attempts,prepared_count,incomplete_count,retryable_count,observed);
            }
        }
    }

    let (unknown,incomplete,retryable,blocked)=scheduler.classify();
    println!("dlmm completion complete min_tvl_usd={} distinct_dlmm_seen={} observed_updates={} first_attempts={} retries={} prepared={} incomplete={} retryable={} eligible_unknown_left={} eligible_incomplete_left={} eligible_retryable_left={} blocked_incompatible={} elapsed_ms={}",
        MIN_COMPLETION_TVL_USD,unique_dlmm_seen.len(),observed,initial_attempts,repeated_attempts,
        prepared_count,incomplete_count,retryable_count,unknown,incomplete,retryable,
        blocked,started.elapsed().as_millis());
    Ok(())
}

pub async fn observe(mut input: broadcast::Receiver<Arc<CompletionResultBatch>>) -> Result<()> {
    let mut batches = 0_u64;
    let mut results = 0_usize;
    let mut manifests = 0_usize;
    loop {
        match input.recv().await {
            Ok(batch) => {
                batches += 1;
                for result in &batch.results {
                    results += 1;
                    if let Some(inventory) = &result.inventory {
                        println!("execution inventory pool={} registry_tvl_usd={:?} active_bin={} active_array={} internal_bitmap_bits={} extension_bitmap_bits={} bitmap_extension_present={} initialized={} selected={:?} validated={:?} initialized_addresses={:?}",
                            result.pool, inventory.registry_tvl_usd, inventory.active_id, inventory.active_array_index,
                            inventory.internal_initialized, inventory.extension_initialized,
                            inventory.extension_present, inventory.initialized.len(),
                            inventory.selected_indices, inventory.validated_indices,
                            inventory.initialized.iter().map(|a| format!("{}:{}", a.index, a.address))
                                .collect::<Vec<_>>());
                    }
                    if let Some(mints) = &result.mints {
                        for mint in mints {
                            println!("execution mint pool={} mint={} token_2022={} extensions={:?} unsupported={:?}",
                                result.pool, mint.address, mint.token_2022, mint.extensions, mint.unsupported);
                        }
                    }
                    if let Some(manifest) = &result.manifest {
                        manifests += 1;
                        println!(
                            "execution manifest pool={} status={:?} slot={} mint_a={} mint_b={} \
                             base_a_accounts={:?} base_b_accounts={:?}",
                            manifest.pool, result.status, manifest.validated_slot,
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
    println!("execution completion consumer complete batches={batches} results={results} validated_manifests={manifests}");
    Ok(())
}
