//! Single durable PostgreSQL boundary for pool identities and execution completion.
//! REST/RPC observations never turn historical data into a live quote.
use std::{collections::HashMap, env, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure, bail};
use serde_json::{Value, json};
use solana_pubkey::Pubkey;
use tokio::{sync::broadcast, time::{interval, MissedTickBehavior}};
use tokio_postgres::{Client, NoTls};

use crate::{
    core::{AccountMeta, CompletionAdmission, CompletionEvidence, CompletionReason, ExecutionManifest, MintProof, Venue, MintState, DataBatch, RecordIdentity,
        CompletionState, PoolState, PoolUpdateBatch, PoolUpdateKind},
};


const TABLE: &str = "public.pools";
const BATCH_SIZE: usize = 512;
const WRITE_INTERVAL: Duration = Duration::from_secs(1);

/// Only the project tables; never drop unrelated schemas or objects.
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS public.mints (
    mint_address TEXT PRIMARY KEY,
    token_program TEXT NULL,
    decimals SMALLINT NULL CHECK (decimals BETWEEN 0 AND 255),
    created_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NULL,
    liquidity_usd NUMERIC NULL,
    market_cap_usd NUMERIC NULL,
    holder_count BIGINT NULL CHECK (holder_count IS NULL OR holder_count >= 0),
    usd_price NUMERIC NULL,
    fetched_at TIMESTAMPTZ NULL,
    token_extensions JSONB NULL,
    transfer_fee_config JSONB NULL
);
CREATE TABLE IF NOT EXISTS public.pools (
    pool_address TEXT PRIMARY KEY,
    venue SMALLINT NOT NULL,
    mint_a TEXT NULL,
    mint_b TEXT NULL,
    tvl_usd NUMERIC NULL,
    tvl_source TEXT NULL,
    tvl_observed_at TIMESTAMPTZ NULL,
    pool_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    completion JSONB NULL,
    CHECK (mint_a IS NULL OR mint_b IS NULL OR mint_a <> mint_b),
    CHECK (completion IS NULL OR jsonb_typeof(completion)='object'),
    CHECK (completion IS NULL OR
        (jsonb_typeof(completion->'validated_slot')='number'
         AND (completion->>'validated_slot')::bigint>=0
         AND jsonb_typeof(completion->'checked_at_ms')='number'
         AND (completion->>'checked_at_ms')::bigint>0))
);
"#;

const UPSERT_MINT: &str = r#"
INSERT INTO public.mints AS old
 (mint_address,token_program,decimals,created_at,updated_at,
  liquidity_usd,market_cap_usd,holder_count,usd_price,fetched_at)
SELECT d.mint,d.program,d.decimals,
 CASE WHEN d.created ~ '^-?[0-9]{1,16}$' THEN to_timestamp(d.created::double precision /
    CASE WHEN length(trim(leading '-' from d.created))>=13 THEN 1000.0 ELSE 1.0 END)
      WHEN pg_input_is_valid(d.created,'timestamp with time zone') THEN d.created::timestamptz
      ELSE NULL END,
 CASE WHEN d.updated ~ '^-?[0-9]{1,16}$' THEN to_timestamp(d.updated::double precision /
    CASE WHEN length(trim(leading '-' from d.updated))>=13 THEN 1000.0 ELSE 1.0 END)
      WHEN pg_input_is_valid(d.updated,'timestamp with time zone') THEN d.updated::timestamptz
      ELSE NULL END,
 d.liq::numeric,d.mcap::numeric,d.holders,d.price::numeric,
 to_timestamp(d.fetched_ms / 1000.0)
FROM UNNEST($1::text[],$2::text[],$3::smallint[],$4::text[],$5::text[],
            $6::text[],$7::text[],$8::bigint[],$9::text[],$10::bigint[])
  AS d(mint,program,decimals,created,updated,liq,mcap,holders,price,fetched_ms)
ON CONFLICT (mint_address) DO UPDATE SET
 token_program=COALESCE(EXCLUDED.token_program,old.token_program),
 decimals=COALESCE(EXCLUDED.decimals,old.decimals),
 created_at=COALESCE(EXCLUDED.created_at,old.created_at),
 updated_at=COALESCE(EXCLUDED.updated_at,old.updated_at),
 liquidity_usd=EXCLUDED.liquidity_usd,
 market_cap_usd=EXCLUDED.market_cap_usd,
 holder_count=EXCLUDED.holder_count,
 usd_price=EXCLUDED.usd_price,
 fetched_at=EXCLUDED.fetched_at
WHERE old.fetched_at IS NULL OR EXCLUDED.fetched_at >= old.fetched_at
"#;

const UPSERT_IDENTITY: &str = r#"
INSERT INTO public.pools AS existing
 (pool_address,venue,mint_a,mint_b,tvl_usd,tvl_source,tvl_observed_at,pool_config)
SELECT d.pool,d.venue,d.a,d.b,d.tvl::numeric,d.tvl_source,
       CASE WHEN d.tvl_ms IS NULL THEN NULL ELSE to_timestamp(d.tvl_ms / 1000.0) END,
       d.config::jsonb
FROM UNNEST($1::text[],$2::smallint[],$3::text[],$4::text[],
            $5::text[],$6::text[],$7::text[],$8::bigint[])
  AS d(pool,venue,a,b,config,tvl,tvl_source,tvl_ms)
ON CONFLICT (pool_address) DO UPDATE SET
    mint_a=COALESCE(existing.mint_a,EXCLUDED.mint_a),
    mint_b=COALESCE(existing.mint_b,EXCLUDED.mint_b),
    pool_config=CASE WHEN EXCLUDED.pool_config<>'{}'::jsonb
                     THEN EXCLUDED.pool_config ELSE existing.pool_config END,
    tvl_usd=CASE WHEN EXCLUDED.tvl_observed_at IS NOT NULL
               AND (existing.tvl_observed_at IS NULL OR EXCLUDED.tvl_observed_at >= existing.tvl_observed_at)
               THEN EXCLUDED.tvl_usd ELSE existing.tvl_usd END,
    tvl_source=CASE WHEN EXCLUDED.tvl_observed_at IS NOT NULL
               AND (existing.tvl_observed_at IS NULL OR EXCLUDED.tvl_observed_at >= existing.tvl_observed_at)
               THEN EXCLUDED.tvl_source ELSE existing.tvl_source END,
    tvl_observed_at=GREATEST(existing.tvl_observed_at,EXCLUDED.tvl_observed_at)
WHERE existing.venue=EXCLUDED.venue
  AND (existing.mint_a IS NULL OR EXCLUDED.mint_a IS NULL OR existing.mint_a=EXCLUDED.mint_a)
  AND (existing.mint_b IS NULL OR EXCLUDED.mint_b IS NULL OR existing.mint_b=EXCLUDED.mint_b)
"#;

const UPSERT_COMPLETION: &str = r#"
INSERT INTO public.pools AS existing
 (pool_address,venue,mint_a,mint_b,completion,pool_config,tvl_usd,tvl_source,tvl_observed_at)
SELECT d.pool,d.venue,d.a,d.b,d.completion::jsonb,d.config::jsonb,
       d.tvl::numeric,d.tvl_source,
       CASE WHEN d.tvl_ms IS NULL THEN NULL ELSE to_timestamp(d.tvl_ms / 1000.0) END
FROM UNNEST($1::text[],$2::smallint[],$3::text[],$4::text[],
            $5::text[],$6::text[],$7::text[],$8::bigint[],$9::text[])
  AS d(pool,venue,a,b,config,tvl,tvl_source,tvl_ms,completion)
ON CONFLICT (pool_address) DO UPDATE SET
    mint_a=COALESCE(existing.mint_a,EXCLUDED.mint_a),
    mint_b=COALESCE(existing.mint_b,EXCLUDED.mint_b),
    completion=EXCLUDED.completion,
    pool_config=CASE WHEN EXCLUDED.pool_config<>'{}'::jsonb
                  THEN EXCLUDED.pool_config ELSE existing.pool_config END,
    tvl_usd=CASE WHEN EXCLUDED.tvl_observed_at IS NOT NULL
               AND (existing.tvl_observed_at IS NULL OR EXCLUDED.tvl_observed_at >= existing.tvl_observed_at)
               THEN EXCLUDED.tvl_usd ELSE existing.tvl_usd END,
    tvl_source=CASE WHEN EXCLUDED.tvl_observed_at IS NOT NULL
               AND (existing.tvl_observed_at IS NULL OR EXCLUDED.tvl_observed_at >= existing.tvl_observed_at)
               THEN EXCLUDED.tvl_source ELSE existing.tvl_source END,
    tvl_observed_at=GREATEST(existing.tvl_observed_at,EXCLUDED.tvl_observed_at)
WHERE existing.venue=EXCLUDED.venue
  AND (existing.mint_a IS NULL OR existing.mint_a=EXCLUDED.mint_a)
  AND (existing.mint_b IS NULL OR existing.mint_b=EXCLUDED.mint_b)
  AND (existing.completion IS NULL
       OR (EXCLUDED.completion->>'validated_slot')::bigint >
          (existing.completion->>'validated_slot')::bigint)
"#;

fn venue_code(venue: Venue) -> i16 {
    match venue { Venue::MeteoraDlmm => 1, Venue::MeteoraDammV2 => 2,
        Venue::RaydiumAmmV4 => 3, Venue::RaydiumCpmm => 4, Venue::RaydiumClmm => 5,
        Venue::RaydiumStable => 6, Venue::OrcaWhirlpool => 7,
        Venue::PancakeSwapClmm => 8, Venue::ByrealClmm => 9,
        Venue::Manifest => 10, Venue::PumpSwap => 11 }
}

fn venue_from(code: i16) -> Result<Venue> {
    match code { 1=>Ok(Venue::MeteoraDlmm), 2=>Ok(Venue::MeteoraDammV2),
        3=>Ok(Venue::RaydiumAmmV4), 4=>Ok(Venue::RaydiumCpmm),
        5=>Ok(Venue::RaydiumClmm), 6=>Ok(Venue::RaydiumStable),
        7=>Ok(Venue::OrcaWhirlpool), 8=>Ok(Venue::PancakeSwapClmm), 9=>Ok(Venue::ByrealClmm), 10=>Ok(Venue::Manifest), 11=>Ok(Venue::PumpSwap), _=>bail!("unsupported persisted venue {code}") }
}

fn pubkey(bytes: &str) -> Result<Pubkey> {
    bytes.parse::<Pubkey>().context("invalid persisted Base58 Pubkey")
}

fn json_key(value: &Value) -> Result<Pubkey> {
    value.as_str().context("expected base58 Pubkey string")?.parse()
        .context("invalid persisted base58 Pubkey")
}

fn array<'a>(value: &'a Value, name: &str) -> Result<&'a [Value]> {
    Ok(value.get(name).and_then(Value::as_array)
        .with_context(||format!("missing array {name}"))?)
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
    let slot=pool.completion_validated_slot.context("completion slot missing")?;
    let checked=pool.completion_checked_at_ms.context("completion check time missing")?;
    ensure!(checked>0, "completion check time zero");
    let manifest = pool.execution_manifest.as_ref().map(|m| json!({
        "base_a":encode_accounts(&m.base_a), "base_b":encode_accounts(&m.base_b)
    }));
    ensure!(pool.completion_state != CompletionState::Prepared || manifest.is_some(),
        "Prepared pool lacks manifest");
    let evidence = pool.completion_evidence.as_ref().map(|e| json!({
        "kind": e.kind, "version": e.version, "payload": e.payload.as_ref()
    }));
    let mints = pool.mint_proofs.as_ref().map(|proofs| proofs.iter().map(|m| json!({
        "address":m.address.to_string(), "token_2022":m.token_2022,
        "extensions":m.extensions, "unsupported":m.unsupported
    })).collect::<Vec<_>>());
    let payload = json!({
        "manifest":manifest,
        "bound_signer":pool.execution_manifest.as_ref().and_then(|m|m.bound_signer).map(|wallet|wallet.to_string()),
        "evidence":evidence,
        "mints":mints,
        "reason":pool.completion_reason,
        "reason_code":pool.completion_reason_code,
        "validated_slot":slot,
        "checked_at_ms":checked,
    });
    Ok(serde_json::to_string(&payload)?)
}

fn decode_descriptor(
    address: Pubkey, a: Pubkey, b: Pubkey, slot: u64, payload: &Value
) -> Result<(Option<Arc<ExecutionManifest>>, Option<Arc<CompletionEvidence>>,
             Option<[MintProof;2]>, Option<String>, Option<CompletionReason>, Option<u64>)> {
    let manifest = if payload.get("manifest").is_some_and(|v|!v.is_null()) {
        let value = &payload["manifest"];
        // Upgrade the old persisted wallet context once at the storage boundary.
        let bound_signer=payload.get("bound_signer")
            .or_else(||payload.pointer("/evidence/payload/v10_wallet"))
            .filter(|v|!v.is_null()).map(json_key).transpose()?;
        Some(Arc::new(ExecutionManifest {
            pool:address, mint_a:a, mint_b:b, validated_slot:slot,
            bound_signer,
            base_a:decode_accounts(value,"base_a")?,
            base_b:decode_accounts(value,"base_b")?,
        }))
    } else { None };
    let evidence = payload.get("evidence").filter(|v| !v.is_null()).map(|v| {
        let kind = v.get("kind").and_then(Value::as_str)
            .filter(|v| !v.is_empty()).context("completion evidence kind missing")?;
        let version = u32::try_from(v.get("version").and_then(Value::as_u64)
            .context("completion evidence version missing")?)?;
        ensure!(version > 0, "invalid completion evidence version");
        let data = v.get("payload").context("completion evidence payload missing")?;
        ensure!(data.is_object(), "completion evidence payload must be an object");
        Ok(Arc::new(CompletionEvidence {
            kind: kind.to_owned(), version, payload: Arc::new(data.clone()),
        }))
    }).transpose()?;
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
    if manifest.is_some() {
        ensure!(evidence.is_some() && mints.is_some(), "Prepared missing protocol evidence and mint proof");
    }
    let reason = payload.get("reason").filter(|v|!v.is_null())
        .map(|v|v.as_str().context("invalid persisted reason").map(str::to_owned))
        .transpose()?;
    let reason_code=payload.get("reason_code").filter(|v|!v.is_null())
        .map(|v|serde_json::from_value::<CompletionReason>(v.clone())).transpose()?;
    let reason_code=if manifest.is_none() {
        Some(reason_code.context("Incomplete completion requires reason_code")?)
    } else {
        ensure!(reason_code.is_none(), "Prepared completion has reason_code");
        None
    };
    let checked_at_ms=payload.get("checked_at_ms").and_then(Value::as_u64)
        .filter(|v|*v>0).context("completion requires positive checked_at_ms")?;
    ensure!(reason_code!=Some(CompletionReason::TransientFailure),
        "transient attempt is not a durable completion proof");
    Ok((manifest, evidence, mints, reason, reason_code, Some(checked_at_ms)))
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

fn timestamp_ms(ms:u64)->Result<Option<i64>> {
    if ms==0 {Ok(None)} else {Ok(Some(i64::try_from(ms).context("timestamp overflow")?))}
}
fn observation_values(pool:&PoolState)->Result<(Option<String>,Option<String>,Option<i64>)> {
    let observation=pool.last_observation.as_deref();
    let tvl=pool.tvl_usd.or_else(||observation.and_then(|v|v.get("tvl_usd")).and_then(Value::as_f64));
    let source=observation.and_then(|v|v.get("source")).and_then(Value::as_str).map(str::to_owned);
    let at=observation.and_then(|v|v.get("observed_at_ms")).and_then(Value::as_u64);
    Ok((tvl.map(|v|v.to_string()),source,at.map(i64::try_from).transpose()?))
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
                crate::log_event!(Warn,"persistence connection failed: {err}");
            }
        });
        client.batch_execute(SCHEMA).await.context("initialize mints and pools")?;
        Ok(Self {client})
    }

    /// Restore the durable Mint Registry state before producers are started.
    pub async fn restore_mints(&self)->Result<Vec<MintState>> {
        let mut after:Option<String>=None;
        let mut result=Vec::new();
        loop {
            let rows=self.client.query(
                "SELECT mint_address,token_program,decimals,created_at::text AS created,
                        updated_at::text AS updated,
                        liquidity_usd::double precision AS liquidity,
                        market_cap_usd::double precision AS mcap,
                        holder_count,usd_price::double precision AS price,
                        (extract(epoch FROM fetched_at)*1000)::bigint AS fetched
                 FROM public.mints WHERE ($1::text IS NULL OR mint_address>$1)
                 ORDER BY mint_address LIMIT 8192",&[&after]).await
                .context("restore mint catalog")?;
            if rows.is_empty(){break;}
            for row in rows {
                let addr:String=row.get("mint_address");
                let program:Option<String>=row.get("token_program");
                let decimal:Option<i16>=row.get("decimals");
                let holders:Option<i64>=row.get("holder_count");
                let fetched:Option<i64>=row.get("fetched");
                result.push(MintState{
                    address:pubkey(&addr)?,
                    token_program:program.as_deref().map(pubkey).transpose()?,
                    decimals:decimal.map(u8::try_from).transpose()?,
                    created_at:row.get("created"),
                    updated_at:row.get("updated"),
                    liquidity_usd:row.get("liquidity"),
                    market_cap_usd:row.get("mcap"),
                    holder_count:holders.map(u64::try_from).transpose()?,
                    usd_price:row.get("price"),
                    fetched_at_ms:fetched.map(u64::try_from).transpose()?.unwrap_or(0),
                });
                after=Some(addr);
            }
        }
        crate::log_event!(Info,"persistence restored_mints={} table=public.mints",result.len());
        Ok(result)
    }

    /// Sole PostgreSQL writer for Jupiter batches; sources only publish typed DataBatch.
    pub async fn save_mint_batch(&self,batch:&DataBatch)->Result<u64> {
        let mut keys=Vec::<String>::new();
        let mut programs=Vec::<Option<String>>::new();
        let mut decimals=Vec::<Option<i16>>::new();
        let mut created=Vec::<Option<String>>::new();
        let mut updated=Vec::<Option<String>>::new();
        let mut liquidity=Vec::<Option<String>>::new();
        let mut mcap=Vec::<Option<String>>::new();
        let mut holders=Vec::<Option<i64>>::new();
        let mut prices=Vec::<Option<String>>::new();
        let mut fetched=Vec::<i64>::new();
        let amount=|v:Option<&Value>|->Option<String>{
            let raw=match v? {Value::Number(n)=>n.to_string(),Value::String(s)=>s.clone(),_=>return None};
            let parsed=raw.parse::<f64>().ok()?;
            (parsed.is_finite() && parsed>=0.0).then_some(raw)
        };
        let date=|v:Option<&Value>|->Option<String>{
            match v? {Value::Number(n)=>Some(n.to_string()),
                Value::String(s) if !s.trim().is_empty() && s.len()<=80=>Some(s.clone()),_=>None}
        };
        for record in &batch.records {
            let RecordIdentity::Mint{address}=&record.identity else {continue};
            let payload=&record.payload;
            keys.push(address.to_string());
            programs.push(payload.get("tokenProgram").and_then(Value::as_str)
                .and_then(|v|v.parse::<Pubkey>().ok()).map(|v|v.to_string()));
            decimals.push(payload.get("decimals").and_then(Value::as_u64)
                .and_then(|v|u8::try_from(v).ok()).map(i16::from));
            created.push(date(payload.get("createdAt")));
            updated.push(date(payload.get("updatedAt")));
            liquidity.push(amount(payload.get("liquidity")));
            mcap.push(amount(payload.get("mcap")));
            holders.push(payload.get("holderCount").and_then(|v|
                v.as_u64().or_else(||v.as_str().and_then(|s|s.parse::<u64>().ok())))
                .and_then(|v|i64::try_from(v).ok()));
            prices.push(amount(payload.get("usdPrice")));
            fetched.push(i64::try_from(batch.received_at_ms)?);
        }
        if keys.is_empty(){return Ok(0);}
        self.client.execute(UPSERT_MINT,&[&keys,&programs,&decimals,
            &created,&updated,&liquidity,&mcap,&holders,&prices,&fetched]).await
            .context("persist Jupiter mint catalog")
    }

    /// Incremental database pages bound SQL-response memory during large restores.
    /// Historical observations are kept separate; live TVL stays unknown on restart.
    pub async fn restore(&self) -> Result<Vec<PoolState>> {
        let mut pools=Vec::new();
        let mut after:Option<String>=None;
        let mut prepared=0_usize;
        let mut incomplete=0_usize;
        let mut unknown=0_usize;
        let query=format!(
            "SELECT pool_address,venue,mint_a,mint_b,status,validated_slot,descriptor_version,planner_version,
                descriptor::text AS payload, pool_config::text AS config,
                tvl_usd::double precision AS historical_tvl,tvl_source,
                (extract(epoch FROM tvl_observed_at)*1000)::bigint AS observed_ms,
                (extract(epoch FROM first_seen_at)*1000)::bigint AS first_seen_ms,
                completion_reason_code,
                (extract(epoch FROM completion_checked_at)*1000)::bigint AS completion_checked_ms
            FROM {TABLE}
            WHERE ($1::text IS NULL OR pool_address > $1)
            ORDER BY pool_address LIMIT 8192");
        loop {
            let rows=self.client.query(&query,&[&after]).await
                .context("read persistence bootstrap page")?;
            if rows.is_empty(){break;}
            for row in rows {
                let raw_address:String=row.get("pool_address");
                let address=pubkey(&raw_address)?;
                let a=row.get::<_,Option<String>>("mint_a").as_deref()
                    .map(pubkey).transpose()?;
                let b=row.get::<_,Option<String>>("mint_b").as_deref()
                    .map(pubkey).transpose()?;
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
                ensure!((1..=DESCRIPTOR_VERSION).contains(&version) && planner==PLANNER_VERSION,
                    "unsupported persisted descriptor/planner version pool={address}");
                let raw_descriptor:Option<String>=row.get("payload");
                let (manifest,evidence,mints,reason,reason_code,checked_at_ms)=match (status,slot,raw_descriptor) {
                    (CompletionState::Unknown,None,None) => {
                        unknown+=1; (None,None,None,None,None,None)
                    }
                    (CompletionState::Prepared|CompletionState::Incomplete,Some(slot),Some(raw))=>{
                        let a=a.context("completed pool mint A missing")?;
                        let b=b.context("completed pool mint B missing")?;
                        let payload:Value=serde_json::from_str(&raw)
                            .with_context(||format!("decode descriptor pool={address}"))?;
                        let typed_reason:Option<String>=row.get("completion_reason_code");
                        let typed_checked=row.get::<_,Option<i64>>("completion_checked_ms")
                            .map(u64::try_from).transpose()?;
                        let decoded=decode_descriptor(address,a,b,status,slot,version,&payload,
                            typed_reason,typed_checked)
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
                let historical_tvl:Option<f64>=row.get("historical_tvl");
                let tvl_source:Option<String>=row.get("tvl_source");
                let observed_ms:Option<i64>=row.get("observed_ms");
                let first_seen_ms:u64=row.get::<_,Option<i64>>("first_seen_ms")
                    .map_or(Ok(0),|v|u64::try_from(v).context("negative first_seen_ms"))?;
                let last_observation=if let (Some(value),Some(source),Some(at))=
                    (historical_tvl,tvl_source,observed_ms) {
                    Some(Arc::new(json!({"tvl_usd":value,"source":source,
                        "observed_at_ms":at,"first_seen_ms":first_seen_ms})))
                } else {None};
                pools.push(PoolState{
                    address,venue,mint_a:a,mint_b:b,source:"persistence_restore",
                    first_seen_ms,last_seen_ms:0,last_changed_ms:0,tvl_usd:None,
                    completion_admission:CompletionAdmission::from_inputs(a,b,None),
                    pool_config,last_observation,
                    completion_state:status,completion_validated_slot:slot,
                    execution_manifest:manifest,completion_evidence:evidence,
                    mint_proofs:mints,completion_reason:reason,
                    completion_reason_code:reason_code,completion_checked_at_ms:checked_at_ms,
                });
                after=Some(raw_address);
            }
        }
        crate::log_event!(Info,"persistence restored pools={} unknown={} prepared={} incomplete={} table={TABLE}",
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
            let mut tvls=Vec::with_capacity(batch.len());
            let mut sources=Vec::with_capacity(batch.len());
            let mut tvl_times=Vec::with_capacity(batch.len());
            let mut first_seen=Vec::with_capacity(batch.len());
            let mut last_seen=Vec::with_capacity(batch.len());
            for pool in &batch {
                addresses.push(pool.address.to_string());
                venues.push(venue_code(pool.venue));
                mint_a.push(pool.mint_a.map(|m|m.to_string()));
                mint_b.push(pool.mint_b.map(|m|m.to_string()));
                configs.push(encoded_config(pool)?);
                let (tvl,source,at)=observation_values(pool)?;
                tvls.push(tvl);sources.push(source);tvl_times.push(at);
                first_seen.push(timestamp_ms(pool.first_seen_ms)?);
                last_seen.push(timestamp_ms(pool.last_seen_ms)?);
            }
            let changed=match kind {
                WriteKind::Identity=>{
                    self.client.execute(UPSERT_IDENTITY,&[
                        &addresses,&venues,&mint_a,&mint_b,&configs,&tvls,&sources,
                        &tvl_times,&first_seen,&last_seen,
                    ]).await.context("batch upsert pool identities")?
                }
                WriteKind::Completion=>{
                    let mut states=Vec::with_capacity(batch.len());
                    let mut slots=Vec::with_capacity(batch.len());
                    let mut descriptors=Vec::with_capacity(batch.len());
                    let mut checked=Vec::with_capacity(batch.len());
                    let mut reasons=Vec::with_capacity(batch.len());
                    for pool in &batch {
                        states.push(status_code(pool.completion_state)?);
                        slots.push(i64::try_from(pool.completion_validated_slot
                            .context("completed pool missing validation slot")?)
                            .context("Solana slot exceeds PostgreSQL BIGINT")?);
                        descriptors.push(encode_descriptor(pool)?);
                        checked.push(pool.completion_checked_at_ms.map(i64::try_from).transpose()?);
                        reasons.push(pool.completion_reason_code.map(|r|serde_json::to_value(r))
                            .transpose()?.and_then(|v|v.as_str().map(str::to_owned)));
                    }
                    self.client.execute(UPSERT_COMPLETION,&[
                        &addresses,&venues,&mint_a,&mint_b,&states,&slots,&descriptors,
                        &configs,&tvls,&sources,&tvl_times,&first_seen,&last_seen,
                        &checked,&reasons,
                    ]).await.context("batch upsert execution completions")?
                }
            };
            *written+=usize::try_from(changed)?;
            for pool in &batch { staged.remove(&pool.address); }
            crate::log_event!(Debug,"persistence batch kind={} input={} upserted={} total_upserted={} sample_pool={}",
                match kind{WriteKind::Identity=>"identity",WriteKind::Completion=>"completion"},
                batch.len(),changed,*written,batch[0].address);
        }
        Ok(())
    }

    pub async fn run(self,rx:broadcast::Receiver<Arc<PoolUpdateBatch>>) -> Result<()> {
        self.run_with_mints(rx,None).await
    }
    pub async fn run_with_mints(self,mut rx:broadcast::Receiver<Arc<PoolUpdateBatch>>,
        mut mint_rx:Option<broadcast::Receiver<Arc<DataBatch>>>) -> Result<()> {
        use std::collections::HashSet;
        let mut pending_identity=HashMap::with_capacity(BATCH_SIZE);
        let mut pending_completion=HashMap::with_capacity(BATCH_SIZE);
        let mut identities_seen=0_usize;
        let mut completions_seen=0_usize;
        let mut written_identity=0_usize;
        let mut written_completion=0_usize;
        let mut ticker=interval(WRITE_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut pool_open=true;
        while pool_open || mint_rx.is_some() {
            let mint_open=mint_rx.is_some();
            tokio::select! {
                event=async {mint_rx.as_mut().expect("mint channel").recv().await},if mint_open=>match event {
                    Ok(batch)=>{self.save_mint_batch(&batch).await?;},
                    Err(broadcast::error::RecvError::Lagged(n))=>
                        bail!("mint persistence lost {n} Jupiter batches"),
                    Err(broadcast::error::RecvError::Closed)=>{mint_rx=None;},
                },
                event=rx.recv(),if pool_open=>match event {
                    Ok(batch)=>{
                        match batch.kind {
                            PoolUpdateKind::Source=>{
                                // Source batches without explicit identity changes (including
                                // restored bootstrap snapshots) never require SQL writes.
                                if batch.persist_identity.is_empty() {continue;}
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
                    Err(broadcast::error::RecvError::Closed)=>pool_open=false,
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
        crate::log_event!(Info,"persistence complete identities_seen={identities_seen} completions_seen={completions_seen} identities_upserted={written_identity} completions_upserted={written_completion} table={TABLE}");
        Ok(())
    }
}
