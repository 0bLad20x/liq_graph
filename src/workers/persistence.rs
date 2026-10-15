//! Single durable PostgreSQL boundary for pool identities and execution completion.
//! REST/RPC observations never turn historical data into a live quote.
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
    mint_a BYTEA NULL CHECK (mint_a IS NULL OR octet_length(mint_a) = 32),
    mint_b BYTEA NULL CHECK (mint_b IS NULL OR octet_length(mint_b) = 32),
    status SMALLINT NOT NULL DEFAULT 0 CHECK (status IN (0, 1, 2)),
    validated_slot BIGINT NULL CHECK (validated_slot IS NULL OR validated_slot >= 0),
    descriptor_version INTEGER NOT NULL DEFAULT 1 CHECK (descriptor_version >= 1),
    planner_version INTEGER NOT NULL DEFAULT 1 CHECK (planner_version >= 1),
    descriptor JSONB NULL,
    pool_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_observation JSONB NULL,
    CHECK (mint_a IS NULL OR mint_b IS NULL OR mint_a <> mint_b),
    CHECK (status <> 1 OR (
        mint_a IS NOT NULL AND mint_b IS NOT NULL
        AND validated_slot IS NOT NULL AND descriptor IS NOT NULL)),
    CHECK (status <> 0 OR (validated_slot IS NULL AND descriptor IS NULL))
)"#;

/// Idempotent expansion of the V1 completion-only table. No data is deleted.
const MIGRATE_TABLE: &str = r#"
BEGIN;
ALTER TABLE public.liq_graph_pool_cache
    ADD COLUMN IF NOT EXISTS pool_config JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE public.liq_graph_pool_cache
    ADD COLUMN IF NOT EXISTS last_observation JSONB NULL;
ALTER TABLE public.liq_graph_pool_cache
    ALTER COLUMN mint_a DROP NOT NULL,
    ALTER COLUMN mint_b DROP NOT NULL,
    ALTER COLUMN validated_slot DROP NOT NULL,
    ALTER COLUMN descriptor DROP NOT NULL,
    ALTER COLUMN status SET DEFAULT 0,
    ALTER COLUMN descriptor_version SET DEFAULT 1,
    ALTER COLUMN planner_version SET DEFAULT 1;
ALTER TABLE public.liq_graph_pool_cache
    DROP CONSTRAINT IF EXISTS liq_graph_pool_cache_status_check;
DO $migration$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
        WHERE conrelid='public.liq_graph_pool_cache'::regclass
          AND conname='liq_graph_pool_cache_status_v2_check') THEN
        ALTER TABLE public.liq_graph_pool_cache
            ADD CONSTRAINT liq_graph_pool_cache_status_v2_check
            CHECK (status IN (0,1,2));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
        WHERE conrelid='public.liq_graph_pool_cache'::regclass
          AND conname='liq_graph_pool_cache_prepared_v2_check') THEN
        ALTER TABLE public.liq_graph_pool_cache
            ADD CONSTRAINT liq_graph_pool_cache_prepared_v2_check
            CHECK (status <> 1 OR (
                mint_a IS NOT NULL AND mint_b IS NOT NULL
                AND validated_slot IS NOT NULL AND descriptor IS NOT NULL));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
        WHERE conrelid='public.liq_graph_pool_cache'::regclass
          AND conname='liq_graph_pool_cache_unknown_v2_check') THEN
        ALTER TABLE public.liq_graph_pool_cache
            ADD CONSTRAINT liq_graph_pool_cache_unknown_v2_check
            CHECK (status <> 0 OR (validated_slot IS NULL AND descriptor IS NULL));
    END IF;
END
$migration$;
COMMIT;
"#;

/// Source events insert identities and fill previously unknown mints/configuration.
/// No changes to already validated execution state; TVL-only updates never arrive here.
const UPSERT_IDENTITY: &str = r#"
INSERT INTO public.liq_graph_pool_cache AS existing
(pool_address,venue,mint_a,mint_b,status,pool_config,last_observation)
SELECT d.pool, d.venue, d.a, d.b, 0, d.config::jsonb, d.observation::jsonb
FROM UNNEST($1::bytea[],$2::smallint[],$3::bytea[],$4::bytea[],
            $5::text[],$6::text[])
    AS d(pool,venue,a,b,config,observation)
WHERE TRUE
ON CONFLICT (pool_address) DO UPDATE SET
    mint_a=COALESCE(existing.mint_a,EXCLUDED.mint_a),
    mint_b=COALESCE(existing.mint_b,EXCLUDED.mint_b),
    pool_config=CASE
      WHEN existing.pool_config='{}'::jsonb THEN EXCLUDED.pool_config
      ELSE existing.pool_config END,
    last_observation=CASE
      WHEN existing.last_observation IS NULL
        OR (existing.last_observation->>'tvl_usd' IS NULL
            AND EXCLUDED.last_observation->>'tvl_usd' IS NOT NULL)
        THEN EXCLUDED.last_observation
      ELSE existing.last_observation END
WHERE existing.venue=EXCLUDED.venue
  AND (existing.mint_a IS NULL OR EXCLUDED.mint_a IS NULL OR existing.mint_a=EXCLUDED.mint_a)
  AND (existing.mint_b IS NULL OR EXCLUDED.mint_b IS NULL OR existing.mint_b=EXCLUDED.mint_b)
  AND (
      (existing.mint_a IS NULL AND EXCLUDED.mint_a IS NOT NULL)
   OR (existing.mint_b IS NULL AND EXCLUDED.mint_b IS NOT NULL)
   OR (existing.pool_config='{}'::jsonb AND EXCLUDED.pool_config<>'{}'::jsonb)
   OR (existing.last_observation IS NULL AND EXCLUDED.last_observation IS NOT NULL)
   OR (existing.last_observation->>'tvl_usd' IS NULL
       AND EXCLUDED.last_observation->>'tvl_usd' IS NOT NULL))
"#;

/// Only an accepted completion may update an execution descriptor, at a non-stale slot.
/// Preserve independently acquired structure/observation fields and fill missing mints.
const UPSERT_COMPLETION: &str = r#"
INSERT INTO public.liq_graph_pool_cache AS existing
(pool_address,venue,mint_a,mint_b,status,validated_slot,
 descriptor_version,planner_version,descriptor,pool_config,last_observation)
SELECT d.pool,d.venue,d.a,d.b,d.status,d.slot,1,1,d.descriptor::jsonb,
       d.config::jsonb,d.observation::jsonb
FROM UNNEST($1::bytea[],$2::smallint[],$3::bytea[],$4::bytea[],
            $5::smallint[],$6::bigint[],$7::text[],$8::text[],$9::text[])
    AS d(pool,venue,a,b,status,slot,descriptor,config,observation)
WHERE TRUE
ON CONFLICT (pool_address) DO UPDATE SET
    mint_a=COALESCE(existing.mint_a,EXCLUDED.mint_a),
    mint_b=COALESCE(existing.mint_b,EXCLUDED.mint_b),
    status=EXCLUDED.status,
    validated_slot=EXCLUDED.validated_slot,
    descriptor_version=EXCLUDED.descriptor_version,
    planner_version=EXCLUDED.planner_version,
    descriptor=EXCLUDED.descriptor,
    pool_config=CASE
      WHEN existing.pool_config='{}'::jsonb THEN EXCLUDED.pool_config
      ELSE existing.pool_config END,
    last_observation=CASE
      WHEN EXCLUDED.last_observation->>'tvl_usd' IS NOT NULL
        THEN EXCLUDED.last_observation
      ELSE COALESCE(existing.last_observation,EXCLUDED.last_observation) END
WHERE existing.venue=EXCLUDED.venue
  AND (existing.mint_a IS NULL OR existing.mint_a=EXCLUDED.mint_a)
  AND (existing.mint_b IS NULL OR existing.mint_b=EXCLUDED.mint_b)
  AND (existing.validated_slot IS NULL
       OR existing.validated_slot < EXCLUDED.validated_slot
       OR (existing.validated_slot=EXCLUDED.validated_slot
           AND existing.planner_version<=EXCLUDED.planner_version))
"#;

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
        0 => Ok(CompletionState::Unknown),
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

#[derive(Clone, Copy)]
enum WriteKind { Identity, Completion }

fn stage(staged:&mut HashMap<Pubkey,PoolState>, pool:&PoolState, kind:WriteKind) -> Result<()> {
    if matches!(kind,WriteKind::Completion) {
        if pool.completion_state == CompletionState::Unknown { return Ok(()); }
        ensure!(pool.completion_validated_slot.is_some()
            && pool.mint_a.is_some() && pool.mint_b.is_some(),
            "completed pool missing slot/mint identity");
        if staged.get(&pool.address).is_some_and(|prior|
            prior.completion_validated_slot >= pool.completion_validated_slot) {return Ok(());}
    }
    staged.insert(pool.address,pool.clone());
    Ok(())
}

fn encoded_config(pool:&PoolState) -> Result<String> {
    let config=pool.pool_config.as_deref();
    ensure!(config.is_none_or(Value::is_object), "pool_config must be JSON object");
    Ok(config.map(ToString::to_string).unwrap_or_else(||"{}".into()))
}

fn encoded_observation(pool:&PoolState) -> Option<String> {
    // Fresh observation is a persisted historical snapshot, never a live quote.
    if pool.last_seen_ms>0 && pool.source!="persistence_restore" {
        Some(json!({"tvl_usd":pool.tvl_usd,
            "first_seen_ms":pool.first_seen_ms,
            "observed_at_ms":pool.last_seen_ms,"source":pool.source}).to_string())
    } else {
        pool.last_observation.as_ref().map(ToString::to_string)
    }
}

fn bytes_key(key:Option<Pubkey>) -> Option<Vec<u8>> {
    key.map(|v|v.to_bytes().to_vec())
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
        let v2:bool=client.query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='liq_graph_pool_cache'
               AND column_name='pool_config')",&[]).await
            .context("inspect persistence schema version")?.get(0);
        if !v2 {
            client.batch_execute(MIGRATE_TABLE).await
                .context("migrate liq_graph_pool_cache V2")?;
            println!("persistence migrated_schema=V2");
        }
        Ok(Self {client})
    }

    /// Incremental database pages bound SQL-response memory during large restores.
    /// Historical observations are kept separate; live TVL stays unknown on restart.
    pub async fn restore(&self) -> Result<Vec<PoolState>> {
        let mut pools=Vec::new();
        let mut after:Option<Vec<u8>>=None;
        let mut prepared=0_usize;
        let mut incomplete=0_usize;
        let mut unknown=0_usize;
        let query=format!(
            "SELECT pool_address,venue,mint_a,mint_b,status,validated_slot,descriptor_version,planner_version,descriptor::text AS payload, pool_config::text AS config, last_observation::text AS observation
            FROM {TABLE}
            WHERE ($1::bytea IS NULL OR pool_address > $1)
            ORDER BY pool_address LIMIT 8192");
        loop {
            let rows=self.client.query(&query,&[&after]).await
                .context("read persistence bootstrap page")?;
            if rows.is_empty(){break;}
            for row in rows {
                let raw_address:Vec<u8>=row.get("pool_address");
                let address=pubkey(&raw_address)?;
                let a=row.get::<_,Option<Vec<u8>>>("mint_a")
                    .map(|b|pubkey(&b)).transpose()?;
                let b=row.get::<_,Option<Vec<u8>>>("mint_b")
                    .map(|b|pubkey(&b)).transpose()?;
                ensure!(address!=Pubkey::default() &&
                    a.is_none_or(|v|v!=Pubkey::default()) &&
                    b.is_none_or(|v|v!=Pubkey::default()) &&
                    !(a.is_some() && a==b), "invalid stored pool identity");
                let venue=venue_from(row.get("venue"))?;
                let status=status_from(row.get("status"))?;
                let slot=row.get::<_,Option<i64>>("validated_slot")
                    .map(|v|u64::try_from(v).context("negative persisted slot")).transpose()?;
                let version:i32=row.get("descriptor_version");
                let planner:i32=row.get("planner_version");
                ensure!(version==DESCRIPTOR_VERSION && planner==PLANNER_VERSION,
                    "unsupported persisted descriptor/planner version pool={address}");
                let raw_descriptor:Option<String>=row.get("payload");
                let (manifest,inventory,mints,reason)=match (status,slot,raw_descriptor) {
                    (CompletionState::Unknown,None,None) => {
                        unknown+=1; (None,None,None,None)
                    }
                    (CompletionState::Prepared|CompletionState::Incomplete,Some(slot),Some(raw))=>{
                        let a=a.context("completed pool mint A missing")?;
                        let b=b.context("completed pool mint B missing")?;
                        let payload:Value=serde_json::from_str(&raw)
                            .with_context(||format!("decode descriptor pool={address}"))?;
                        let decoded=decode_descriptor(address,a,b,status,slot,&payload)
                            .with_context(||format!("restore pool={address}"))?;
                        if status==CompletionState::Prepared{prepared+=1;}else{incomplete+=1;}
                        decoded
                    }
                    _ => bail!("inconsistent persisted pool status/slot/descriptor: {address}"),
                };
                let raw_config:String=row.get("config");
                let config:Value=serde_json::from_str(&raw_config)?;
                ensure!(config.is_object(), "stored pool_config must be an object");
                let pool_config=if config.as_object().is_some_and(|v|v.is_empty()){
                    None
                }else{Some(Arc::new(config))};
                let last_observation=row.get::<_,Option<String>>("observation")
                    .map(|raw|serde_json::from_str::<Value>(&raw))
                    .transpose()?
                    .map(Arc::new);
                ensure!(last_observation.as_ref().is_none_or(|v|v.is_object()),
                    "stored observation must be an object");
                let first_seen_ms=last_observation.as_ref()
                    .and_then(|v|v.get("first_seen_ms"))
                    .and_then(Value::as_u64).unwrap_or(0);
                pools.push(PoolState{
                    address,venue,mint_a:a,mint_b:b,source:"persistence_restore",
                    first_seen_ms,last_seen_ms:0,last_changed_ms:0,tvl_usd:None,
                    pool_config,last_observation,
                    completion_state:status,completion_validated_slot:slot,
                    execution_manifest:manifest,execution_inventory:inventory,
                    mint_proofs:mints,completion_reason:reason,
                });
                after=Some(raw_address);
            }
        }
        println!("persistence restored pools={} unknown={} prepared={} incomplete={} table={TABLE}",
            pools.len(),unknown,prepared,incomplete);
        Ok(pools)
    }

    /// Flush only after SQL succeeds. On an error nothing is removed from staging.
    async fn flush(&self, staged:&mut HashMap<Pubkey,PoolState>,
        kind:WriteKind, written:&mut usize) -> Result<()> {
        while !staged.is_empty() {
            let batch:Vec<PoolState>=staged.values().take(BATCH_SIZE).cloned().collect();
            let mut addresses=Vec::with_capacity(batch.len());
            let mut venues=Vec::with_capacity(batch.len());
            let mut mint_a=Vec::with_capacity(batch.len());
            let mut mint_b=Vec::with_capacity(batch.len());
            let mut configs=Vec::with_capacity(batch.len());
            let mut observations=Vec::with_capacity(batch.len());
            for pool in &batch {
                addresses.push(pool.address.to_bytes().to_vec());
                venues.push(venue_code(pool.venue));
                mint_a.push(bytes_key(pool.mint_a));
                mint_b.push(bytes_key(pool.mint_b));
                configs.push(encoded_config(pool)?);
                observations.push(encoded_observation(pool));
            }
            let changed=match kind {
                WriteKind::Identity=>{
                    self.client.execute(UPSERT_IDENTITY,&[
                        &addresses,&venues,&mint_a,&mint_b,&configs,&observations
                    ]).await.context("batch upsert pool identities")?
                }
                WriteKind::Completion=>{
                    let mut states=Vec::with_capacity(batch.len());
                    let mut slots=Vec::with_capacity(batch.len());
                    let mut descriptors=Vec::with_capacity(batch.len());
                    for pool in &batch {
                        states.push(status_code(pool.completion_state)?);
                        slots.push(i64::try_from(pool.completion_validated_slot
                            .context("completed pool missing validation slot")?)
                            .context("Solana slot exceeds PostgreSQL BIGINT")?);
                        descriptors.push(encode_descriptor(pool)?);
                    }
                    self.client.execute(UPSERT_COMPLETION,&[
                        &addresses,&venues,&mint_a,&mint_b,&states,&slots,&descriptors,
                        &configs,&observations
                    ]).await.context("batch upsert execution completions")?
                }
            };
            *written+=usize::try_from(changed)?;
            for pool in &batch { staged.remove(&pool.address); }
            println!("persistence batch kind={} input={} upserted={} total_upserted={} sample_pool={}",
                match kind{WriteKind::Identity=>"identity",WriteKind::Completion=>"completion"},
                batch.len(),changed,*written,batch[0].address);
        }
        Ok(())
    }

    pub async fn run(self,mut rx:broadcast::Receiver<Arc<PoolUpdateBatch>>) -> Result<()> {
        use std::collections::HashSet;
        let mut pending_identity=HashMap::with_capacity(BATCH_SIZE);
        let mut pending_completion=HashMap::with_capacity(BATCH_SIZE);
        let mut identities_seen=0_usize;
        let mut completions_seen=0_usize;
        let mut written_identity=0_usize;
        let mut written_completion=0_usize;
        let mut ticker=interval(WRITE_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                event=rx.recv()=>match event {
                    Ok(batch)=>{
                        match batch.kind {
                            PoolUpdateKind::Source=>{
                                // Bootstrap records already exist in SQL. Never rewrite them.
                                if batch.source=="persistence_restore" {continue;}
                                let keys:HashSet<Pubkey>=batch.persist_identity.iter().copied().collect();
                                for pool in &batch.pools {
                                    if keys.contains(&pool.address) {
                                        stage(&mut pending_identity,pool,WriteKind::Identity)?;
                                        identities_seen+=1;
                                    }
                                }
                                if pending_identity.len()>=BATCH_SIZE {
                                    self.flush(&mut pending_identity,WriteKind::Identity,
                                        &mut written_identity).await?;
                                }
                            }
                            PoolUpdateKind::Completion=>{
                                for pool in &batch.pools {
                                    if pool.completion_state!=CompletionState::Unknown {
                                        stage(&mut pending_completion,pool,WriteKind::Completion)?;
                                        completions_seen+=1;
                                    }
                                }
                                if pending_completion.len()>=BATCH_SIZE {
                                    self.flush(&mut pending_identity,WriteKind::Identity,
                                        &mut written_identity).await?;
                                    self.flush(&mut pending_completion,WriteKind::Completion,
                                        &mut written_completion).await?;
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n))=>
                        bail!("persistence lost {n} accepted Registry batches"),
                    Err(broadcast::error::RecvError::Closed)=>break,
                },
                _=ticker.tick(), if !pending_identity.is_empty() ||
                    !pending_completion.is_empty()=>{
                    self.flush(&mut pending_identity,WriteKind::Identity,
                        &mut written_identity).await?;
                    self.flush(&mut pending_completion,WriteKind::Completion,
                        &mut written_completion).await?;
                }
            }
        }
        self.flush(&mut pending_identity,WriteKind::Identity,&mut written_identity).await?;
        self.flush(&mut pending_completion,WriteKind::Completion,&mut written_completion).await?;
        println!("persistence complete identities_seen={identities_seen} completions_seen={completions_seen} identities_upserted={written_identity} completions_upserted={written_completion} table={TABLE}");
        Ok(())
    }
}
