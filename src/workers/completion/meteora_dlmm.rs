//! DLMM execution completion: lifecycle request -> RPC observations -> V10 manifest.
//! No persistence, no writes to Pool Registry, and no hidden RPC clients.

use std::{collections::{HashMap, HashSet}, env, sync::Arc};
use anyhow::{anyhow, bail, ensure, Context, Result};
use solana_pubkey::Pubkey;
use serde_json::json;
use tokio::{sync::mpsc, task::JoinSet};

use crate::{
    core::{CompletionReason, Venue, AccountMeta, ExecutionManifest, CompletionEvidence, MintProof,
        CompletionStatus, CompletionResult,
        CompletionOutcome, CompletionRequest, CompletionUnchanged, CompletionRequestBatch, CompletionResponse, CompletionResponseBatch, PoolState},
    core::rpc::{AccountData, AccountView, RpcAnswer, RpcHandle, RpcOperation},
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

// This DLMM-only account inventory never crosses the shared contract boundary.
#[derive(Clone, Debug)]
struct BinArrayRef {
    pub index: i32,
    pub address: Pubkey,
}

#[derive(Clone, Debug)]
struct BinArrayInventory {
    pub registry_tvl_usd: Option<f64>,
    pub active_id: i32,
    pub active_array_index: i32,
    pub internal_initialized: usize,
    pub extension_initialized: usize,
    pub extension_present: bool,
    /// The complete initialized array-address inventory represented by the fetched bitmaps.
    pub initialized: Arc<[BinArrayRef]>,
    pub selected_indices: Vec<i32>,
    pub validated_indices: Vec<i32>,
}

fn evidence(inventory: BinArrayInventory) -> Arc<CompletionEvidence> {
    Arc::new(CompletionEvidence {
        kind: "meteora_dlmm_bin_arrays".to_owned(),
        version: 1,
        payload: Arc::new(json!({
            "registry_tvl_usd": inventory.registry_tvl_usd,
            "active_id": inventory.active_id,
            "active_array_index": inventory.active_array_index,
            "internal_initialized": inventory.internal_initialized,
            "extension_initialized": inventory.extension_initialized,
            "extension_present": inventory.extension_present,
            "initialized": inventory.initialized.iter().map(|a| json!({
                "index": a.index, "address": a.address.to_string()
            })).collect::<Vec<_>>(),
            "selected_indices": inventory.selected_indices,
            "validated_indices": inventory.validated_indices,
        })),
    })
}

#[derive(Clone, Copy, Debug)]
pub struct Settings {
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
        ensure!(account_batch_size >= 3 + below + above,
            "DLMM account batch must fit pool, bitmap and the configured BinArray window");
        let parallel_batches = env::var("DLMM_COMPLETION_PARALLEL_BATCHES")
            .unwrap_or_else(|_| "32".to_owned()).parse::<usize>()
            .context("invalid DLMM_COMPLETION_PARALLEL_BATCHES")?;
        ensure!((1..=64).contains(&parallel_batches),
            "DLMM_COMPLETION_PARALLEL_BATCHES must be 1..=64");
        Ok(Self { account_batch_size, below, above, parallel_batches })
    }
    /// Worst case per pool: LbPair + bitmap + active array + configured neighbors.
    /// Batch capacity follows the same account budget as protocol validation.
    pub fn pools_per_batch(self) -> usize {
        self.account_batch_size / (3 + self.below + self.above)
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
    crate::log_event!(Debug,"execution completion rpc method=getMultipleAccounts provider={provider} slot={slot} accounts={} present={}",
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
    code: CompletionReason, reason: impl Into<String>) -> CompletionResult {
    CompletionResult::new(pending.pool,status,Some(pending.slot),Some(reason.into()),
        Some(evidence(pending.validated.inventory)),None,None).with_reason_code(code)
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
        let check = (|| -> std::result::Result<[MintProof; 2], (CompletionStatus, CompletionReason, String)> {
            let mut proofs = Vec::new();
            for (key, expected_2022) in [(layout.mint_a, layout.token_2022_a),
                (layout.mint_b, layout.token_2022_b)] {
                match observed.get(&key) {
                    Some(MintObservation::Verified(proof)) => {
                        if proof.token_2022 != expected_2022 {
                            return Err((CompletionStatus::Incomplete, CompletionReason::UnsupportedConfiguration, format!(
                                "mint {key} Token-2022 owner differs from LbPair flag expected={expected_2022} actual={}",
                                proof.token_2022)));
                        }
                        proofs.push(proof.clone());
                    }
                    Some(MintObservation::Invalid(reason)) =>
                        return Err((CompletionStatus::Incomplete, CompletionReason::ValidationFailed, format!("mint {key}: {reason}"))),
                    Some(MintObservation::Unavailable(reason)) =>
                        return Err((CompletionStatus::Retryable, CompletionReason::TransientFailure, format!("mint {key}: {reason}"))),
                    None => return Err((CompletionStatus::Retryable, CompletionReason::TransientFailure,
                        format!("mint {key} RPC response missing"))),
                }
            }
            Ok([proofs.remove(0), proofs.remove(0)])
        })();
        match check {
            Ok(mints) => results.push(prepared(item.pool, item.slot, item.validated, mints)),
            Err((status, code, reason)) => results.push(mint_failure(item, status, code, reason)),
        }
    }
}

fn retryable(pool: Pubkey, reason: impl Into<String>) -> CompletionResult {
    CompletionResult::new(pool,CompletionStatus::Retryable,None,
        Some(reason.into()),None,None,None)
}

fn prepared(pool: Pubkey, slot: u64, validated: Validated, mints: [MintProof; 2]) -> CompletionResult {
    let unsupported: Vec<String> = mints.iter().filter(|mint| !mint.unsupported.is_empty())
        .map(|mint| format!("{}:{:?}", mint.address, mint.unsupported)).collect();
    let reason_code = if !unsupported.is_empty() {
        Some(CompletionReason::UnsupportedConfiguration)
    } else if validated.manifest.is_none() {
        Some(CompletionReason::MissingExecutionAccounts)
    } else {None};
    let reason = if !unsupported.is_empty() {
        Some(format!("V10 unsupported Token-2022 mint extensions: {}", unsupported.join(", ")))
    } else if validated.manifest.is_none() {
        Some(format!("no bitmap-initialized BinArrays in execution window; internal={} extension={} selected={} validated={}",
            validated.inventory.internal_initialized, validated.inventory.extension_initialized,
            validated.inventory.selected_indices.len(), validated.inventory.validated_indices.len()))
    } else { None };
    let status = if reason.is_some() { CompletionStatus::Incomplete } else { CompletionStatus::Prepared };
    let manifest=if status==CompletionStatus::Prepared {validated.manifest.map(Arc::new)} else {None};
    let mut result=CompletionResult::new(pool,status,Some(slot),reason,
        Some(evidence(validated.inventory)),Some(mints),manifest);
    result.reason_code=reason_code;
    result
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
        bound_signer: None,
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

/// Execute explicit lifecycle requests. Protocol decoding/validation remains local;
/// scheduling, retries and Prepared refresh decisions belong to the lifecycle.
async fn process_optimized(rpc:&RpcHandle, requests:&[CompletionRequest],settings:Settings)->Vec<CompletionOutcome>{
    let owner=match program(){
        Ok(owner)=>owner,
        Err(_)=>return requests.iter().map(|r|CompletionOutcome::Proof(retryable(r.pool.address,
            "DLMM program ID unavailable"))).collect(),
    };
    let eligible=requests.iter().filter(|r|!r.force_full
        && r.work!=crate::core::CompletionWork::Initial
        && (r.pool.completion_state==crate::core::CompletionState::Prepared
            && r.pool.execution_manifest.is_some()
            || r.pool.completion_state==crate::core::CompletionState::Incomplete
                && r.pool.completion_reason_code==Some(CompletionReason::MissingExecutionAccounts))
        && r.pool.mint_proofs.is_some()
        && r.pool.completion_evidence.as_ref().is_some_and(|e|
            e.kind=="meteora_dlmm_bin_arrays" && e.version==1)
        && r.pool.completion_validated_slot.is_some()).collect::<Vec<_>>();
    let mut outcome=HashMap::<Pubkey,CompletionOutcome>::new();
    if !eligible.is_empty() {
        let min_slot=eligible.iter().filter_map(|r|r.pool.completion_validated_slot).max();
        let first=read(rpc,eligible.iter().map(|r|r.pool.address).collect(),min_slot).await;
        match first {
            Err(error)=>for r in &eligible {
                outcome.insert(r.pool.address,CompletionOutcome::Proof(retryable(r.pool.address,
                    format!("DLMM light LbPair: {error:#}"))));
            },
            Ok(first)=>{
                let extensions=eligible.iter().map(|r|
                    pda(r.pool.address,b"bitmap",None,&owner)).collect::<Vec<_>>();
                match read(rpc,extensions,Some(first.slot)).await {
                    Err(error)=>for r in &eligible {
                        outcome.insert(r.pool.address,CompletionOutcome::Proof(retryable(r.pool.address,
                            format!("DLMM light bitmap: {error:#}"))));
                    },
                    Ok(bitmap_batch)=>{
                        for ((r,account),bitmap) in eligible.iter()
                            .zip(first.accounts.iter()).zip(bitmap_batch.accounts.iter()) {
                            let p=&r.pool;
                            let valid=(||->Result<bool>{
                                let layout=decode_pool(account.as_ref().context("LbPair missing")?,p,&owner)?;
                                let extension=bitmap.as_ref();
                                if let Some(x)=extension {decode_bitmap(x,p.address,&owner)?;}
                                let initialized=bitmap_indices(&layout,extension)?;
                                let selected=select_indices(&initialized,layout.active_id.div_euclid(70),
                                    settings.below,settings.above);
                                let arrays=selected.iter().copied().filter(|idx|
                                    is_initialized(&layout,extension,*idx).unwrap_or(false))
                                    .map(|idx|pda(p.address,b"bin_array",Some(idx),&owner))
                                    .collect::<Vec<_>>();
                                if arrays.is_empty(){
                                    return Ok(p.completion_state==crate::core::CompletionState::Incomplete
                                        && p.completion_reason_code==Some(CompletionReason::MissingExecutionAccounts));
                                }
                                let proof=p.mint_proofs.as_ref().context("missing MintProof")?;
                                if layout.token_2022_a!=proof[0].token_2022
                                    || layout.token_2022_b!=proof[1].token_2022 {return Ok(false);}
                                let current=manifest(&layout,
                                    p.completion_validated_slot.context("no validated slot")?,
                                    &arrays,extension.map(|x|x.address),owner)?;
                                Ok(p.execution_manifest.as_ref()
                                    .is_some_and(|cached| current==**cached))
                            })().unwrap_or(false);
                            if valid {
                                if let Some(unchanged)=CompletionUnchanged::from_pool(p,bitmap_batch.slot) {
                                    outcome.insert(p.address,CompletionOutcome::Unchanged(unchanged));
                                }
                            }
                        }
                    },
                }
            },
        }
    }
    let pending=requests.iter().filter(|r|!outcome.contains_key(&r.pool.address))
        .map(|r|r.pool.clone()).collect::<Vec<_>>();
    if !pending.is_empty(){
        for proof in process(rpc,&pending,settings).await{
            outcome.insert(proof.pool,CompletionOutcome::Proof(proof));
        }
    }
    requests.iter().map(|r|outcome.remove(&r.pool.address).expect("DLMM completion result")).collect()
}

pub async fn run(mut input: mpsc::Receiver<CompletionRequestBatch>,
    rpc: RpcHandle, settings: Settings,
    output: mpsc::Sender<CompletionResponseBatch>) -> Result<()> {
    ensure!(settings.account_batch_size > 0 && settings.parallel_batches > 0,
        "invalid DLMM execution settings");
    let mut tasks=JoinSet::new();
    let mut open=true;
    let mut processed=0_usize;
    crate::log_event!(Info,"dlmm executor started parallel={} scheduler=completion_lifecycle",
        settings.parallel_batches);
    while open || !tasks.is_empty() {
        tokio::select! {
            batch=input.recv(), if open && tasks.len()<settings.parallel_batches => {
                match batch {
                    Some(batch)=>{
                        ensure!(!batch.requests.is_empty() && batch.requests.len()<=settings.pools_per_batch(),
                            "invalid DLMM request batch size");
                        ensure!(batch.requests.iter().all(|r|
                            r.pool.venue==Venue::MeteoraDlmm
                            && r.pool.completion_admission.is_admitted()),
                            "DLMM request has wrong venue or missing admission");
                        let rpc=rpc.clone();
                        tasks.spawn(async move {
                            let pools=batch.requests.iter().map(|r|r.pool.clone()).collect::<Vec<_>>();
                            let results=match tokio::time::timeout(batch.timeout,
                                process_optimized(&rpc,&batch.requests,settings)).await {
                                Ok(results)=>results,
                                Err(_)=>pools.iter().map(|p|CompletionOutcome::Proof(retryable(p.address,
                                    "DLMM lifecycle request timed out"))).collect(),
                            };
                            (batch.requests,results)
                        });
                    },
                    None=>open=false,
                }
            },
            done=tasks.join_next(), if !tasks.is_empty()=>{
                let (requests,results)=done.context("missing DLMM request task")?
                    .context("DLMM request task panicked")?;
                ensure!(requests.len()==results.len(),"DLMM request/result count mismatch");
                // process() partitions its output internally; correlate by address,
                // never by the order in which RPC partitions completed.
                let mut by_pool=results.into_iter().map(|outcome|(outcome.pool(),outcome)).collect::<HashMap<_,_>>();
                ensure!(by_pool.len()==requests.len(),"duplicate DLMM pool result");
                let mut responses=Vec::with_capacity(requests.len());
                for request in requests {
                    let outcome=by_pool.remove(&request.pool.address)
                        .context("Completion request has no matching pool result")?;
                    let kind=match &outcome {
                        CompletionOutcome::Proof(result)=>format!("{:?}",result.status),
                        CompletionOutcome::Unchanged(_)=>"Unchanged".to_owned(),
                    };
                    crate::log_event!(Debug,"completion_meteora_dlmm completion request_id={} pool={} work={:?} outcome={}",
                        request.id,request.pool.address,request.work,kind);
                    responses.push(CompletionResponse{id:request.id,outcome});
                }
                processed+=responses.len();
                output.send(CompletionResponseBatch{source:"dlmm_execution_completion",responses})
                    .await.map_err(|_|anyhow!("DLMM lifecycle response consumer closed"))?;
            },
        }
    }
    crate::log_event!(Info,"dlmm executor complete requests={processed}");
    Ok(())
}

