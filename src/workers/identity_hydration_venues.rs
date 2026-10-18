//! Mint identity hydration for known Solana pool venues.
//! Reads only a verified pool-account prefix; publishes ordinary Discovery records.
//! Protocol-specific completion, Registry state and PostgreSQL remain untouched.

use std::{collections::{HashSet, VecDeque}, env, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::{sync::broadcast, task::JoinSet, time::MissedTickBehavior};

use crate::{
    core::{now_unix_ms, DataBatch, DataRecord, PoolUpdateBatch, RecordIdentity, Venue},
    workers::rpc_transport::{AccountData, AccountView, RpcAnswer, RpcHandle, RpcOperation},
};

const SOURCE: &str = "multi_venue_identity_hydration";
const VENUES: [Venue; 11] = [
    Venue::MeteoraDlmm, Venue::MeteoraDammV2, Venue::RaydiumAmmV4,
    Venue::RaydiumCpmm, Venue::RaydiumClmm, Venue::RaydiumStable,
    Venue::OrcaWhirlpool, Venue::PancakeSwapClmm, Venue::ByrealClmm,
    Venue::Manifest, Venue::PumpSwap,
];

#[derive(Clone, Copy)]
struct Layout {
    program: &'static str,
    mint_a: usize,
    mint_b: usize,
    account_len: usize,
    exact_len: bool,
    discriminator: Option<[u8; 8]>,
    version_at_8: Option<u8>,
}

impl Layout {
    fn slice_len(self) -> usize { (self.mint_a.max(self.mint_b) + 32).max(8) }
}

fn layout(venue: Venue) -> Layout {
    // Offsets and pool-account framing follow the existing Completion decoders.
    // Raydium Stable is the distinct V5/1232-byte SDK layout, NOT AMM v4.
    const CLMM: [u8; 8] = [247, 237, 227, 245, 215, 195, 222, 70];
    const DAMM: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];
    match venue {
        Venue::MeteoraDlmm => Layout {
            program: "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
            mint_a: 88, mint_b: 120, account_len: 904, exact_len: true,
            discriminator: Some([33, 11, 49, 98, 181, 101, 177, 13]), version_at_8: None,
        },
        Venue::MeteoraDammV2 => Layout {
            program: "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",
            mint_a: 168, mint_b: 200, account_len: 296, exact_len: false,
            discriminator: Some(DAMM), version_at_8: None,
        },
        Venue::RaydiumAmmV4 => Layout {
            program: "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
            mint_a: 400, mint_b: 432, account_len: 752, exact_len: true,
            discriminator: None, version_at_8: None,
        },
        Venue::RaydiumCpmm => Layout {
            program: "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
            mint_a: 168, mint_b: 200, account_len: 637, exact_len: true,
            discriminator: Some(CLMM), version_at_8: None,
        },
        Venue::RaydiumClmm => Layout {
            program: "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
            mint_a: 73, mint_b: 105, account_len: 1544, exact_len: true,
            discriminator: Some(CLMM), version_at_8: None,
        },
        Venue::RaydiumStable => Layout {
            // Raydium SDK V2 liquidityStateV5Layout: baseMint at 432, quoteMint at 464.
            // Layout has 20+8+8+2 u64 fields, 4 u128, 11 Pubkeys, 64 u64 padding.
            program: "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h",
            mint_a: 432, mint_b: 464, account_len: 1232, exact_len: true,
            discriminator: None, version_at_8: None,
        },
        Venue::OrcaWhirlpool => Layout {
            program: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            mint_a: 101, mint_b: 181, account_len: 653, exact_len: true,
            discriminator: Some([63, 149, 209, 12, 225, 128, 99, 9]), version_at_8: None,
        },
        Venue::PancakeSwapClmm => Layout {
            program: "HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq",
            mint_a: 73, mint_b: 105, account_len: 1544, exact_len: true,
            discriminator: Some(CLMM), version_at_8: None,
        },
        Venue::ByrealClmm => Layout {
            program: "REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2",
            mint_a: 73, mint_b: 105, account_len: 1544, exact_len: true,
            discriminator: Some(CLMM), version_at_8: None,
        },
        Venue::Manifest => Layout {
            program: "MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms",
            mint_a: 16, mint_b: 48, account_len: 256, exact_len: false,
            discriminator: Some(4859840929024028656_u64.to_le_bytes()),
            version_at_8: Some(0),
        },
        Venue::PumpSwap => Layout {
            program: "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
            // Identity needs only the first 107 bytes; older pool variants can
            // legitimately be shorter than Completion's 245-byte full-state frame.
            mint_a: 43, mint_b: 75, account_len: 107, exact_len: false,
            discriminator: Some(DAMM), version_at_8: None,
        },
    }
}

#[derive(Clone, Copy)]
pub struct Settings {
    pub batch_size: usize,
    pub parallel_batches: usize,
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        let batch_size = env::var("RPC_ACCOUNT_BATCH_SIZE")
            .unwrap_or_else(|_| "100".into()).parse::<usize>()
            .context("invalid RPC_ACCOUNT_BATCH_SIZE for multi-venue hydration")?;
        let parallel_batches = env::var("IDENTITY_HYDRATION_PARALLEL_BATCHES")
            .unwrap_or_else(|_| "2".into()).parse::<usize>()
            .context("invalid IDENTITY_HYDRATION_PARALLEL_BATCHES")?;
        ensure!((1..=100).contains(&batch_size), "invalid hydration RPC batch size");
        ensure!((1..=16).contains(&parallel_batches), "hydration parallel batches must be 1..=16");
        Ok(Self { batch_size, parallel_batches })
    }
}

fn mint(bytes: &[u8], offset: usize) -> Result<Pubkey> {
    let raw: [u8; 32] = bytes.get(offset..offset + 32)
        .context("truncated pool mint field")?.try_into()?;
    Ok(Pubkey::new_from_array(raw))
}

fn decode(pool: Pubkey, venue: Venue, account: &AccountData, slot: u64) -> Result<DataRecord> {
    let spec = layout(venue);
    let owner: Pubkey = spec.program.parse().context("invalid hydration program ID")?;
    let space = account.original_len.context("RPC slice response omitted account space")?;
    ensure!(account.address == pool && account.owner == owner
        && account.view == AccountView::Slice { offset: 0, length: spec.slice_len() }
        && account.bytes.len() == spec.slice_len(),
        "pool address/owner/RPC slice mismatch");
    ensure!((!spec.exact_len || space == spec.account_len)
        && (spec.exact_len || space >= spec.account_len),
        "pool account length mismatch");
    if let Some(disc) = spec.discriminator {
        ensure!(account.bytes[..8] == disc, "pool discriminator mismatch");
    }
    if let Some(version) = spec.version_at_8 {
        ensure!(account.bytes.get(8) == Some(&version), "pool version mismatch");
    }
    let a = mint(&account.bytes, spec.mint_a)?;
    let b = mint(&account.bytes, spec.mint_b)?;
    ensure!(a != Pubkey::default() && b != Pubkey::default() && a != b,
        "pool has invalid or identical token mints");
    Ok(DataRecord {
        identity: RecordIdentity::Pool {
            address: pool, venue, mint_a: Some(a), mint_b: Some(b),
        },
        tvl_usd: None,
        payload: json!({"kind":"multi_venue_identity_hydration_v1",
            "validated_slot":slot}),
    })
}

async fn read_batch(rpc: RpcHandle, venue: Venue, keys: Vec<Pubkey>)
    -> (Venue, Vec<Pubkey>, Result<(u64, Vec<Option<AccountData>>)>)
{
    let spec = layout(venue);
    let result = async {
        let response = rpc.call(RpcOperation::Multiple {
            addresses: keys.clone(),
            view: AccountView::Slice { offset: 0, length: spec.slice_len() },
            min_context_slot: None,
        }).await?;
        let RpcAnswer::Multiple { slot, accounts } = response.answer else {
            bail!("hydration expected getMultipleAccounts response");
        };
        ensure!(accounts.len() == keys.len(), "hydration RPC account cardinality mismatch");
        Ok((slot, accounts))
    }.await;
    (venue, keys, result)
}

fn index(venue: Venue) -> usize {
    VENUES.iter().position(|item| *item == venue)
        .expect("all known venues in hydration table")
}

fn enqueue(address: Pubkey, venue: Venue,
    queues: &mut [VecDeque<Pubkey>], pending: &mut HashSet<Pubkey>)
{
    if pending.insert(address) {
        queues[index(venue)].push_back(address);
    }
}

/// Standalone Input -> RPC prefix read -> DataBatch output.
/// Initial addresses are restored incomplete identities. Live Registry events
/// also cover new address-only scans. The Registry is the sole identity writer.
pub async fn run(
    initial: Vec<(Pubkey, Venue)>,
    mut live: broadcast::Receiver<Arc<PoolUpdateBatch>>,
    rpc: RpcHandle,
    output: broadcast::Sender<Arc<DataBatch>>,
    settings: Settings,
    finite: bool,
) -> Result<()> {
    ensure!(rpc.supports(&RpcOperation::Multiple {
        addresses: vec![Pubkey::default()],
        view: AccountView::Slice { offset: 0, length: 496 },
        min_context_slot: None,
    }), "multi-venue hydration requires a getMultipleAccounts provider");

    let mut queues = vec![VecDeque::<Pubkey>::new(); VENUES.len()];
    let mut pending = HashSet::new();
    for (address, venue) in initial {
        enqueue(address, venue, &mut queues, &mut pending);
    }
    let initial_pending = pending.len();
    let mut jobs = JoinSet::new();
    let mut cursor = 0_usize;
    let mut attempted = 0_u64;
    let mut published = 0_u64;
    let mut invalid = 0_u64;
    let mut missing = 0_u64;
    let mut rpc_failures = 0_u64;
    let mut by_venue = [0_u64; 11];
    let mut consecutive_errors = [0_u8; 11];
    let mut pulse = tokio::time::interval(Duration::from_secs(30));
    pulse.set_missed_tick_behavior(MissedTickBehavior::Delay);

    crate::log_event!(Info,
        "multi venue hydration started initial_missing={} batch_size={} parallel={} finite={finite}",
        initial_pending, settings.batch_size, settings.parallel_batches);

    loop {
        while jobs.len() < settings.parallel_batches {
            let mut selected = None;
            for offset in 0..VENUES.len() {
                let idx = (cursor + offset) % VENUES.len();
                let mut keys = Vec::with_capacity(settings.batch_size);
                while keys.len() < settings.batch_size {
                    let Some(address) = queues[idx].pop_front() else { break; };
                    if pending.contains(&address) { keys.push(address); }
                }
                if !keys.is_empty() {
                    selected = Some((VENUES[idx], keys));
                    cursor = (idx + 1) % VENUES.len();
                    break;
                }
            }
            let Some((venue, keys)) = selected else { break; };
            let client = rpc.clone();
            jobs.spawn(read_batch(client, venue, keys));
        }

        if finite && jobs.is_empty() && queues.iter().all(VecDeque::is_empty) { break; }

        tokio::select! {
            event = live.recv(), if !finite => match event {
                Ok(batch) => for pool in &batch.pools {
                    if pool.mint_a.is_some() && pool.mint_b.is_some() {
                        pending.remove(&pool.address);
                    } else {
                        enqueue(pool.address, pool.venue, &mut queues, &mut pending);
                    }
                },
                Err(broadcast::error::RecvError::Lagged(n)) =>
                    bail!("multi-venue hydration lost {n} Registry updates"),
                Err(broadcast::error::RecvError::Closed) =>
                    bail!("multi-venue hydration Registry feed closed unexpectedly"),
            },
            finished = jobs.join_next(), if !jobs.is_empty() => {
                let (venue, keys, result) = finished
                    .context("multi-venue hydration job missing")?
                    .context("multi-venue hydration job panicked")?;
                let idx = index(venue);
                match result {
                    Ok((slot, accounts)) => {
                        consecutive_errors[idx] = 0;
                        let mut records = Vec::new();
                        for (address, account) in keys.into_iter().zip(accounts) {
                            attempted += 1;
                            if !pending.remove(&address) { continue; }
                            match account {
                                None => missing += 1,
                                Some(account) => match decode(address, venue, &account, slot) {
                                    Ok(record) => {
                                        published += 1;
                                        by_venue[idx] += 1;
                                        records.push(record);
                                    },
                                    Err(error) => {
                                        invalid += 1;
                                        if invalid <= 25 {
                                            crate::log_event!(Warn,
                                                "multi venue hydration invalid venue={venue:?} pool={address} reason={error:#}");
                                        }
                                    }
                                }
                            }
                        }
                        if !records.is_empty() {
                            output.send(Arc::new(DataBatch {
                                source: SOURCE, received_at_ms: now_unix_ms()?, records
                            })).map_err(|_| anyhow!("multi-venue hydration has no Registry consumer"))?;
                        }
                    },
                    Err(error) => {
                        rpc_failures += 1;
                        consecutive_errors[idx] = consecutive_errors[idx].saturating_add(1);
                        for address in keys {
                            if pending.contains(&address) {
                                queues[idx].push_back(address);
                            }
                        }
                        crate::log_event!(Warn,
                            "multi venue hydration rpc_failure venue={venue:?} count={} consecutive={} reason={error:#}",
                            rpc_failures, consecutive_errors[idx]);
                        ensure!(consecutive_errors[idx] < 5,
                            "multi-venue hydration RPC repeatedly unavailable venue={venue:?}");
                    },
                }
            },
            _ = pulse.tick() => {
                crate::log_event!(Info,
                    "multi venue hydration status initial_missing={initial_pending} attempted={attempted} published={published} missing_accounts={missing} invalid={invalid} rpc_failures={rpc_failures} pending={} in_flight={} published_by_venue={by_venue:?}",
                    pending.len(), jobs.len());
            }
        }
    }
    crate::log_event!(Info,
        "multi venue hydration complete attempted={attempted} published={published} missing_accounts={missing} invalid={invalid} rpc_failures={rpc_failures} pending={} published_by_venue={by_venue:?}",
        pending.len());
    if finite {
        ensure!(attempted == published + invalid + missing && invalid == 0 && missing == 0,
            "finite hydration incomplete: attempted={attempted} published={published} missing_accounts={missing} invalid={invalid}");
    }
    Ok(())
}
