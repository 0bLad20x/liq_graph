//! Single PostgreSQL persistence boundary for accepted execution completion results.
//! One new table in public; no REST metrics, no graph state, no SQL in Registry or other workers.
use std::{collections::HashMap, env, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure, bail};
use serde_json::{Value, json};
use solana_pubkey::Pubkey;
use tokio::{sync::broadcast, time::{interval, MissedTickBehavior}};
use tokio_postgres::{Client, NoTls};

use crate::{
    core::{AccountMeta, BinArrayInventory, BinArrayRef, ExecutionManifest, MintProof, Venue},
    workers::pool_registry::{CompletionState, PoolState, PoolUpdateBatch, PoolUpdateKind},
};

const TABLE: &str = "public.liq_graph_pool_cache";
const DESCRIPTOR_VERSION: i32 = 1;
const PLANNER_VERSION: i32 = 1;
const BATCH_SIZE: usize = 512;
const WRITE_INTERVAL: Duration = Duration::from_secs(1);

const CREATE_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS public.liq_graph_pool_cache (
    pool_address BYTEA PRIMARY KEY CHECK (octet_length(pool_address) = 32),
    venue SMALLINT NOT NULL,
    mint_a BYTEA NOT NULL CHECK (octet_length(mint_a) = 32),
    mint_b BYTEA NOT NULL CHECK (octet_length(mint_b) = 32),
    status SMALLINT NOT NULL CHECK (status IN (1, 2)),
    validated_slot BIGINT NOT NULL CHECK (validated_slot >= 0),
    descriptor_version INTEGER NOT NULL CHECK (descriptor_version >= 1),
    planner_version INTEGER NOT NULL CHECK (planner_version >= 1),
    descriptor JSONB NOT NULL,
    CHECK (mint_a <> mint_b)
)"#;

const UPSERT: &str = r#"
INSERT INTO public.liq_graph_pool_cache
(pool_address, venue, mint_a, mint_b, status, validated_slot, descriptor_version, planner_version, descriptor)
SELECT v.pool_address, v.venue, v.mint_a, v.mint_b, v.status, v.validated_slot,
       1, 1, v.descriptor::jsonb
FROM UNNEST($1::bytea[], $2::smallint[], $3::bytea[], $4::bytea[],
            $5::smallint[], $6::bigint[], $7::text[])
    AS v(pool_address, venue, mint_a, mint_b, status, validated_slot, descriptor)
ON CONFLICT (pool_address) DO UPDATE SET
    venue = EXCLUDED.venue,
    mint_a = EXCLUDED.mint_a,
    mint_b = EXCLUDED.mint_b,
    status = EXCLUDED.status,
    validated_slot = EXCLUDED.validated_slot,
    descriptor_version = EXCLUDED.descriptor_version,
    planner_version = EXCLUDED.planner_version,
    descriptor = EXCLUDED.descriptor
WHERE public.liq_graph_pool_cache.venue = EXCLUDED.venue
  AND public.liq_graph_pool_cache.mint_a = EXCLUDED.mint_a
  AND public.liq_graph_pool_cache.mint_b = EXCLUDED.mint_b
  AND (public.liq_graph_pool_cache.validated_slot < EXCLUDED.validated_slot
       OR (public.liq_graph_pool_cache.validated_slot = EXCLUDED.validated_slot
           AND public.liq_graph_pool_cache.planner_version <= EXCLUDED.planner_version))"#;

fn venue_code(venue: Venue) -> i16 {
    match venue { Venue::MeteoraDlmm => 1, Venue::MeteoraDammV2 => 2,
        Venue::RaydiumAmmV4 => 3, Venue::RaydiumCpmm => 4, Venue::RaydiumClmm => 5,
        Venue::RaydiumStable => 6, Venue::OrcaWhirlpool => 7 }
}

fn venue_from(code: i16) -> Result<Venue> {
    match code { 1=>Ok(Venue::MeteoraDlmm), 2=>Ok(Venue::MeteoraDammV2),
        3=>Ok(Venue::RaydiumAmmV4), 4=>Ok(Venue::RaydiumCpmm),
        5=>Ok(Venue::RaydiumClmm), 6=>Ok(Venue::RaydiumStable),
        7=>Ok(Venue::OrcaWhirlpool), _=>bail!("unsupported persisted venue {code}") }
}

fn status_code(status: CompletionState) -> Result<i16> {
    match status {
        CompletionState::Prepared => Ok(1),
        CompletionState::Incomplete => Ok(2),
        CompletionState::Unknown => bail!("Unknown is not a persisted completion result"),
    }
}

fn status_from(code: i16) -> Result<CompletionState> {
    match code {
        1 => Ok(CompletionState::Prepared),
        2 => Ok(CompletionState::Incomplete),
        _ => bail!("invalid persisted completion status {code}"),
    }
}

fn pubkey(bytes: &[u8]) -> Result<Pubkey> {
    let value: [u8; 32] = bytes.try_into().context("persisted Pubkey is not exactly 32 bytes")?;
    Ok(Pubkey::new_from_array(value))
}

fn json_key(value: &Value) -> Result<Pubkey> {
    value.as_str().context("expected base58 Pubkey string")?.parse()
        .context("invalid persisted base58 Pubkey")
}

fn array<'a>(value: &'a Value, name: &str) -> Result<&'a [Value]> {
    Ok(value.get(name).and_then(Value::as_array)
        .with_context(||format!("missing array {name}"))?)
}

fn int(value: &Value, name: &str) -> Result<i32> {
    let number = value.get(name).and_then(Value::as_i64)
        .with_context(||format!("missing integer {name}"))?;
    Ok(i32::try_from(number).context("stored signed integer overflow")?)
}

fn usize_value(value: &Value, name: &str) -> Result<usize> {
    let number = value.get(name).and_then(Value::as_u64)
        .with_context(||format!("missing unsigned integer {name}"))?;
    Ok(usize::try_from(number).context("stored unsigned integer overflow")?)
}

fn index_array(value: &Value, name: &str) -> Result<Vec<i32>> {
    array(value, name)?.iter().map(|v| {
        i32::try_from(v.as_i64().context("invalid signed BinArray index")?)
            .context("BinArray index overflow")
    }).collect()
}

fn encode_accounts(accounts: &[AccountMeta]) -> Value {
    Value::Array(accounts.iter().map(|a|
        json!({"address":a.address.to_string(),"writable":a.writable})).collect())
}

fn decode_accounts(value: &Value, name: &str) -> Result<Arc<[AccountMeta]>> {
    let entries = array(value,name)?;
    ensure!(!entries.is_empty(), "empty persisted manifest account direction");
    Ok(Arc::from(entries.iter().map(|entry| Ok(AccountMeta {
        address: json_key(entry.get("address").context("account address missing")?)?,
        writable: entry.get("writable").and_then(Value::as_bool)
            .context("account writable flag missing")?,
    })).collect::<Result<Vec<_>>>()?))
}

fn encode_descriptor(pool: &PoolState) -> Result<String> {
    let slot = pool.completion_validated_slot.context("completion slot missing")?;
    let manifest = pool.execution_manifest.as_ref().map(|m| json!({
        "base_a":encode_accounts(&m.base_a), "base_b":encode_accounts(&m.base_b)
    }));
    ensure!(pool.completion_state != CompletionState::Prepared || manifest.is_some(),
        "Prepared pool lacks manifest");
    let inventory = pool.execution_inventory.as_ref().map(|i| json!({
        "active_id":i.active_id,
        "active_array_index":i.active_array_index,
        "internal_initialized":i.internal_initialized,
        "extension_initialized":i.extension_initialized,
        "extension_present":i.extension_present,
        "initialized":i.initialized.iter().map(|v| json!({
            "index":v.index, "address":v.address.to_string()
        })).collect::<Vec<_>>(),
        "selected_indices":i.selected_indices,
        "validated_indices":i.validated_indices
    }));
    let mints = pool.mint_proofs.as_ref().map(|proofs| proofs.iter().map(|m| json!({
        "address":m.address.to_string(), "token_2022":m.token_2022,
        "extensions":m.extensions, "unsupported":m.unsupported
    })).collect::<Vec<_>>());
    let payload = json!({
        "validated_slot":slot,
        "manifest":manifest,
        "inventory":inventory,
        "mints":mints,
        "reason":pool.completion_reason,
    });
    Ok(serde_json::to_string(&payload)?)
}

fn decode_descriptor(
    address: Pubkey, a: Pubkey, b: Pubkey, status: CompletionState, slot: u64, payload: &Value
) -> Result<(Option<Arc<ExecutionManifest>>, Option<Arc<BinArrayInventory>>,
             Option<[MintProof;2]>, Option<String>)> {
    ensure!(payload.get("validated_slot").and_then(Value::as_u64)==Some(slot),
        "persisted validated_slot / descriptor mismatch");
    let manifest = if payload.get("manifest").is_some_and(|v|!v.is_null()) {
        let value = &payload["manifest"];
        Some(Arc::new(ExecutionManifest {
            pool:address, mint_a:a, mint_b:b, validated_slot:slot,
            base_a:decode_accounts(value,"base_a")?,
            base_b:decode_accounts(value,"base_b")?,
        }))
    } else { None };
    let inventory = if let Some(v) = payload.get("inventory").filter(|v|!v.is_null()) {
        let initialized = array(v,"initialized")?.iter().map(|entry| Ok(BinArrayRef {
            index:int(entry,"index")?,
            address:json_key(entry.get("address").context("BinArray address missing")?)?,
        })).collect::<Result<Vec<_>>>()?;
        Some(Arc::new(BinArrayInventory {
            registry_tvl_usd:None, // Live TVL must never be restored as fresh.
            active_id:int(v,"active_id")?,
            active_array_index:int(v,"active_array_index")?,
            internal_initialized:usize_value(v,"internal_initialized")?,
            extension_initialized:usize_value(v,"extension_initialized")?,
            extension_present:v.get("extension_present").and_then(Value::as_bool)
                .context("extension_present missing")?,
            initialized:Arc::from(initialized),
            selected_indices:index_array(v,"selected_indices")?,
            validated_indices:index_array(v,"validated_indices")?,
        }))
    } else {None};
    let mints = if let Some(v) = payload.get("mints").filter(|v|!v.is_null()) {
        let raw = v.as_array().context("invalid stored mint proofs")?;
        ensure!(raw.len()==2, "expected two persisted mint proofs");
        let proofs = raw.iter().map(|m| {
            let extensions = array(m,"extensions")?.iter().map(|e|
                u16::try_from(e.as_u64().context("invalid mint extension")?)
                    .context("mint extension overflow")).collect::<Result<Vec<_>>>()?;
            let unsupported = array(m,"unsupported")?.iter().map(|e|
                u16::try_from(e.as_u64().context("invalid unsupported extension")?)
                    .context("unsupported mint extension overflow")).collect::<Result<Vec<_>>>()?;
            Ok(MintProof {
                address:json_key(m.get("address").context("mint proof address missing")?)?,
                token_2022:m.get("token_2022").and_then(Value::as_bool)
                    .context("token_2022 missing")?,
                extensions,unsupported,
            })
        }).collect::<Result<Vec<_>>>()?;
        let pair:[MintProof;2]=proofs.try_into()
            .map_err(|_|anyhow::anyhow!("invalid two-mint proof conversion"))?;
        ensure!(pair[0].address == a && pair[1].address == b,
            "persisted mint proofs mismatch Pool identity");
        Some(pair)
    } else {None};
    ensure!((status==CompletionState::Prepared)==manifest.is_some(),
        "stored Prepared status and manifest disagree");
    if status==CompletionState::Prepared {
        ensure!(inventory.is_some() && mints.is_some(), "Prepared missing RPC proof");
    }
    let reason = payload.get("reason").filter(|v|!v.is_null())
        .map(|v|v.as_str().context("invalid persisted reason").map(str::to_owned))
        .transpose()?;
    Ok((manifest, inventory, mints, reason))
}

fn stage(staged:&mut HashMap<Pubkey,PoolState>, pool:&PoolState) -> Result<()> {
    if pool.completion_state==CompletionState::Unknown { return Ok(()); }
    let slot=pool.completion_validated_slot.context("completion slot missing")?;
    ensure!(pool.mint_a.is_some() && pool.mint_b.is_some(), "completed pool missing mints");
    match staged.get(&pool.address) {
        Some(prior) if prior.completion_validated_slot.is_some_and(|old|old>=slot) => {},
        _ => { staged.insert(pool.address,pool.clone()); }
    }
    Ok(())
}

pub struct Persistence {
    client:Client,
}

impl Persistence {
    pub async fn connect() -> Result<Self> {
        let url=env::var("LIQ_GRAPH_DATABASE_URL")
            .or_else(|_|env::var("DATABASE_URL"))
            .context("LIQ_GRAPH_DATABASE_URL or DATABASE_URL required")?;
        let (client,connection)=tokio_postgres::connect(&url,NoTls).await
            .context("connect PostgreSQL for liq_graph persistence")?;
        tokio::spawn(async move {
            if let Err(err)=connection.await {
                eprintln!("persistence connection failed: {err}");
            }
        });
        client.batch_execute(CREATE_TABLE).await.context("initialize liq_graph_pool_cache")?;
        Ok(Self {client})
    }

    pub async fn restore(&self) -> Result<Vec<PoolState>> {
        let rows=self.client.query(&format!(
            "SELECT pool_address,venue,mint_a,mint_b,status,validated_slot,descriptor_version,planner_version,descriptor::text AS payload FROM {TABLE} ORDER BY pool_address"
        ),&[]).await.context("read persistence bootstrap")?;
        let mut pools=Vec::with_capacity(rows.len());
        let mut prepared=0_usize;
        let mut incomplete=0_usize;
        for row in rows {
            let address=pubkey(&row.get::<_,Vec<u8>>("pool_address"))?;
            let a=pubkey(&row.get::<_,Vec<u8>>("mint_a"))?;
            let b=pubkey(&row.get::<_,Vec<u8>>("mint_b"))?;
            ensure!(a!=b && address!=Pubkey::default(), "invalid persisted pool identity");
            let venue=venue_from(row.get("venue"))?;
            let status=status_from(row.get("status"))?;
            let slot_i64:i64=row.get("validated_slot");
            let slot=u64::try_from(slot_i64).context("negative persisted slot")?;
            let version:i32=row.get("descriptor_version");
            let planner:i32=row.get("planner_version");
            ensure!(version==DESCRIPTOR_VERSION && planner==PLANNER_VERSION,
                "unsupported persisted descriptor/planner version pool={address} descriptor={version} planner={planner}");
            let payload_str:String=row.get("payload");
            let payload:Value=serde_json::from_str(&payload_str)
                .with_context(||format!("decode descriptor pool={address}"))?;
            let (manifest,inventory,mints,reason)=decode_descriptor(address,a,b,status,slot,&payload)
                .with_context(||format!("restore pool={address}"))?;
            if status==CompletionState::Prepared { prepared+=1; }else{ incomplete+=1; }
            pools.push(PoolState {
                address,venue,mint_a:Some(a),mint_b:Some(b),source:"persistence_restore",
                first_seen_ms:0,last_seen_ms:0,last_changed_ms:0,tvl_usd:None,
                completion_state:status,completion_validated_slot:Some(slot),
                execution_manifest:manifest,execution_inventory:inventory,
                mint_proofs:mints,completion_reason:reason,
            });
        }
        if let Some(sample)=pools.first() {
            println!("persistence restore sample_pool={} mint_a={} mint_b={} slot={}",
                sample.address,
                sample.mint_a.context("restored mint A missing")?,
                sample.mint_b.context("restored mint B missing")?,
                sample.completion_validated_slot.context("restored slot missing")?);
        }
        println!("persistence restored pools={} prepared={} incomplete={} table={TABLE}",
            pools.len(),prepared,incomplete);
        Ok(pools)
    }

    async fn flush(&self, staged:&mut HashMap<Pubkey,PoolState>, written:&mut usize)->Result<()> {
        if staged.is_empty(){return Ok(());}
        let batch:Vec<_>=staged.drain().map(|(_,v)|v).collect();
        for chunk in batch.chunks(BATCH_SIZE) {
            let mut addresses=Vec::with_capacity(chunk.len());
            let mut venues=Vec::with_capacity(chunk.len());
            let mut mint_a=Vec::with_capacity(chunk.len());
            let mut mint_b=Vec::with_capacity(chunk.len());
            let mut status=Vec::with_capacity(chunk.len());
            let mut slots=Vec::with_capacity(chunk.len());
            let mut descriptors=Vec::with_capacity(chunk.len());
            for pool in chunk {
                addresses.push(pool.address.to_bytes().to_vec());
                venues.push(venue_code(pool.venue));
                mint_a.push(pool.mint_a.context("persist mint A missing")?.to_bytes().to_vec());
                mint_b.push(pool.mint_b.context("persist mint B missing")?.to_bytes().to_vec());
                status.push(status_code(pool.completion_state)?);
                slots.push(i64::try_from(pool.completion_validated_slot
                    .context("persist validated slot missing")?).context("Solana slot exceeds PG BIGINT")?);
                descriptors.push(encode_descriptor(pool)?);
            }
            let changed=self.client.execute(UPSERT,&[
                &addresses,&venues,&mint_a,&mint_b,&status,&slots,&descriptors
            ]).await.context("batch UPSERT completed pools")?;
            *written+=usize::try_from(changed)?;
            println!("persistence batch input={} upserted={} total_upserted={} sample_pool={} sample_mint_a={} sample_mint_b={} sample_slot={}",
                chunk.len(),changed,*written,chunk[0].address,
                chunk[0].mint_a.context("sample mint A missing")?,
                chunk[0].mint_b.context("sample mint B missing")?,
                chunk[0].completion_validated_slot.context("sample slot missing")?);
        }
        Ok(())
    }

    pub async fn run(self,mut rx:broadcast::Receiver<Arc<PoolUpdateBatch>>) -> Result<()> {
        let mut pending=HashMap::with_capacity(BATCH_SIZE);
        let mut written=0_usize;
        let mut completion_events=0_usize;
        let mut ticker=interval(WRITE_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                event=rx.recv()=>match event {
                    Ok(batch) => {
                        if batch.kind!=PoolUpdateKind::Completion {continue;}
                        completion_events+=batch.pools.len();
                        for pool in &batch.pools {stage(&mut pending,pool)?;}
                        if pending.len()>=BATCH_SIZE {
                            self.flush(&mut pending,&mut written).await?;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) =>
                        bail!("persistence lost {n} accepted Registry batches"),
                    Err(broadcast::error::RecvError::Closed)=>break,
                },
                _=ticker.tick(),if !pending.is_empty() =>self.flush(&mut pending,&mut written).await?,
            }
        }
        self.flush(&mut pending,&mut written).await?;
        println!("persistence complete completion_events={} upserted={} table={TABLE}",
            completion_events,written);
        Ok(())
    }
}
