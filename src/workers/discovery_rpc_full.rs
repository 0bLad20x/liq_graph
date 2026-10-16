//! Generic full HTTP getProgramAccounts bootstrap: addresses only, streamed into typed Discovery batches.
//! Large responses are processed incrementally. No protocol decoding, mint hydration or DB writes.
//! curl's --compressed handles providers that serve gzip even for Accept-Encoding: identity.
use std::{
    collections::HashSet, env, io::{BufReader, Write}, process::{Command, Stdio},
    sync::Arc, thread, time::{Duration, Instant},
};
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor}};
use serde_json::{json, Value};
use solana_pubkey::Pubkey;
use tokio::sync::broadcast;
use crate::core::{DataBatch, DataRecord, RecordIdentity, Venue, now_unix_ms};

const SOURCE: &str = "rpc_full_scan";
const PUMP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const PUMP_DISC: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];
const BATCH: usize = 512;

#[derive(Clone)]
pub struct Settings {
    program: Pubkey,
    venue: Venue,
    filter_offset: usize,
    filter_bytes: Vec<u8>,
    providers: Vec<(usize, String)>,
    timeout_seconds: u64,
    batch_spacing_ms: u64,
}
impl Settings {
    pub fn from_env() -> Result<Self> {
        let venue_name = env::var("FULL_SCAN_VENUE").unwrap_or_else(|_| "PumpSwap".into());
        let venue = match venue_name.as_str() {
            "PumpSwap" => Venue::PumpSwap,
            "MeteoraDlmm" => Venue::MeteoraDlmm,
            "MeteoraDammV2" => Venue::MeteoraDammV2,
            "RaydiumAmmV4" => Venue::RaydiumAmmV4,
            "RaydiumCpmm" => Venue::RaydiumCpmm,
            "RaydiumClmm" => Venue::RaydiumClmm,
            "RaydiumStable" => Venue::RaydiumStable,
            "OrcaWhirlpool" => Venue::OrcaWhirlpool,
            "PancakeSwapClmm" => Venue::PancakeSwapClmm,
            "ByrealClmm" => Venue::ByrealClmm,
            "Manifest" => Venue::Manifest,
            _ => bail!("unsupported FULL_SCAN_VENUE={venue_name}"),
        };
        let program_name = match env::var("FULL_SCAN_PROGRAM_ID") {
            Ok(value) => value,
            Err(env::VarError::NotPresent) if venue == Venue::PumpSwap => PUMP.into(),
            Err(e) => bail!("FULL_SCAN_PROGRAM_ID required for custom venue: {e}"),
        };
        let program: Pubkey = program_name.parse().context("FULL_SCAN_PROGRAM_ID")?;
        ensure!(program != Pubkey::default(), "zero FULL_SCAN_PROGRAM_ID");
        let filter_offset: usize = env::var("FULL_SCAN_FILTER_OFFSET")
            .unwrap_or_else(|_| "0".into()).parse()?;
        let filter_bytes = match env::var("FULL_SCAN_FILTER_BASE58") {
            Ok(encoded) => bs58::decode(encoded).into_vec()
                .context("invalid FULL_SCAN_FILTER_BASE58")?,
            Err(env::VarError::NotPresent) if venue == Venue::PumpSwap && program.to_string() == PUMP =>
                PUMP_DISC.to_vec(),
            Err(e) => bail!("FULL_SCAN_FILTER_BASE58 required for custom program: {e}"),
        };
        ensure!(!filter_bytes.is_empty() && filter_bytes.len() <= 128,
            "FULL_SCAN_FILTER_BASE58 must identify the pool account type");
        let indices = env::var("FULL_SCAN_RPC_INDICES").unwrap_or_else(|_| "9,10".into());
        let mut providers = Vec::new();
        for raw in indices.split(',') {
            let index: usize = raw.trim().parse().context("invalid FULL_SCAN_RPC_INDICES")?;
            ensure!(index > 0 && !providers.iter().any(|(n,_)| *n == index),
                "duplicate or zero full scan provider index");
            let endpoint = env::var(format!("RPC_{index}_HTTP_URL"))
                .with_context(|| format!("RPC_{index}_HTTP_URL not set"))?;
            ensure!(endpoint.starts_with("https://") || endpoint.starts_with("http://"),
                "invalid HTTP scheme for RPC_{index}");
            ensure!(!endpoint.chars().any(|ch| matches!(ch, '\n'|'\r'|'"'|'\\')),
                "invalid RPC URL for curl config");
            providers.push((index, endpoint));
        }
        ensure!(!providers.is_empty(), "FULL_SCAN_RPC_INDICES is empty");
        let timeout_seconds: u64 = env::var("FULL_SCAN_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| "1800".into()).parse()?;
        let batch_spacing_ms: u64 = env::var("FULL_SCAN_BATCH_SPACING_MS")
            .unwrap_or_else(|_| "250".into()).parse()?;
        ensure!(timeout_seconds >= 30 && timeout_seconds <= 7200
            && (10..=10000).contains(&batch_spacing_ms),
            "invalid full scan timeout / batch spacing");
        Ok(Self { program, venue, filter_offset, filter_bytes, providers,
            timeout_seconds, batch_spacing_ms })
    }
}

#[derive(Deserialize)]
struct AccountHeader { owner: String, data: (String, String) }
#[derive(Deserialize)]
struct Entry { pubkey: String, account: AccountHeader }

struct Collector {
    expected_owner: String,
    venue: Venue,
    output: broadcast::Sender<Arc<DataBatch>>,
    batch_spacing_ms: u64,
    seen: HashSet<Pubkey>,
    pending: Vec<DataRecord>,
    accepted: u64,
    duplicates: u64,
}
impl Collector {
    fn accept(&mut self, entry: Entry) -> Result<()> {
        ensure!(entry.account.owner == self.expected_owner,
            "getProgramAccounts returned account with wrong program owner");
        ensure!(entry.account.data.0.is_empty() && entry.account.data.1 == "base64",
            "full scan must receive zero-length base64 dataSlice");
        let address: Pubkey = entry.pubkey.parse().context("invalid program account pubkey")?;
        ensure!(address != Pubkey::default(), "zero program account pubkey");
        if !self.seen.insert(address) {
            self.duplicates += 1;
            return Ok(());
        }
        self.pending.push(DataRecord {
            identity: RecordIdentity::Pool { address, venue: self.venue, mint_a: None, mint_b: None },
            tvl_usd: None,
            payload: json!({"kind": "rpc_full_scan_address_v1"}),
        });
        self.accepted += 1;
        if self.pending.len() == BATCH { self.flush()?; }
        if self.accepted % 50000 == 0 {
            println!("rpc full scan progress unique={} duplicates={}",
                self.accepted, self.duplicates);
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() { return Ok(()); }
        let records = std::mem::replace(&mut self.pending, Vec::with_capacity(BATCH));
        self.output.send(Arc::new(DataBatch {
            source: SOURCE, received_at_ms: now_unix_ms()?, records,
        })).map_err(|_| anyhow::anyhow!("full scan has no Registry receiver"))?;
        // The existing Registry/Persistence pipeline is broadcast-based, not an ACK queue.
        // Rate-gate batches rather than flooding its bounded channels.
        thread::sleep(Duration::from_millis(self.batch_spacing_ms));
        Ok(())
    }
}
struct ListSeed<'a>(&'a mut Collector);
struct ListVisitor<'a>(&'a mut Collector);
impl<'de> DeserializeSeed<'de> for ListSeed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D)
        -> std::result::Result<(), D::Error> {
        deserializer.deserialize_seq(ListVisitor(self.0))
    }
}
impl<'de> Visitor<'de> for ListVisitor<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("array of Solana program accounts")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<(), A::Error> {
        while let Some(entry) = seq.next_element::<Entry>()? {
            self.0.accept(entry).map_err(serde::de::Error::custom)?;
        }
        Ok(())
    }
}
struct EnvelopeSeed<'a>(&'a mut Collector);
struct EnvelopeVisitor<'a>(&'a mut Collector);
impl<'de> DeserializeSeed<'de> for EnvelopeSeed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D)
        -> std::result::Result<(), D::Error> {
        deserializer.deserialize_map(EnvelopeVisitor(self.0))
    }
}
impl<'de> Visitor<'de> for EnvelopeVisitor<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("JSON-RPC getProgramAccounts envelope")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
        let mut has_result = false;
        let mut id_is_one = false;
        let mut error = None::<Value>;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "result" => {
                    if has_result { return Err(serde::de::Error::custom("duplicate result")); }
                    map.next_value_seed(ListSeed(&mut *self.0))?;
                    has_result = true;
                },
                "id" => {
                    let id: Value = map.next_value()?;
                    id_is_one = id == json!(1);
                },
                "error" => { error = Some(map.next_value()?); },
                _ => { map.next_value::<IgnoredAny>()?; }
            }
        }
        if let Some(error) = error.filter(|v|!v.is_null()) {
            return Err(serde::de::Error::custom(format!("RPC error: {error}")));
        }
        if !has_result || !id_is_one {
            return Err(serde::de::Error::custom("missing RPC result or mismatched id"));
        }
        Ok(())
    }
}
fn scan_one(index: usize, url: &str, settings: &Settings, collector: &mut Collector) -> Result<()> {
    let body = json!({
        "jsonrpc": "2.0", "id": 1, "method": "getProgramAccounts",
        "params": [settings.program.to_string(), {
            "encoding": "base64", "commitment": "confirmed",
            "dataSlice": {"offset": 0, "length": 0},
            "filters": [{"memcmp": {"offset": settings.filter_offset,
                "bytes": bs58::encode(&settings.filter_bytes).into_string()}}]
        }]
    }).to_string();
    let binary = if cfg!(windows) { "curl.exe" } else { "curl" };
    let mut child = Command::new(binary)
        .args(["--config", "-", "--compressed", "--silent", "--show-error", "--fail",
            "--http1.1", "--max-time", &settings.timeout_seconds.to_string(),
            "-H", "Content-Type: application/json", "--data-raw", &body])
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null())
        .spawn().context("spawn curl full scan HTTP transport (curl required on PATH)")?;
    // URL (potentially containing a key) stays out of the command-line argument list.
    {
        let mut stdin = child.stdin.take().context("curl stdin missing")?;
        writeln!(stdin, "url = \"{url}\"").context("write curl URL config")?;
    }
    let started = Instant::now();
    let parse = {
        let stdout = child.stdout.take().context("curl stdout missing")?;
        let mut deserializer = serde_json::Deserializer::from_reader(
            BufReader::with_capacity(256 * 1024, stdout));
        let parsed = EnvelopeSeed(collector).deserialize(&mut deserializer)
            .context("streaming getProgramAccounts JSON response");
        parsed.and_then(|_| deserializer.end().context("trailing JSON after RPC response"))
    };
    if parse.is_err() { let _ = child.kill(); }
    let exit = child.wait().context("wait for curl RPC request")?;
    parse?;
    ensure!(exit.success(), "full scan RPC_{index} HTTP transfer failed: {exit}");
    collector.flush()?;
    println!("rpc full scan provider={} complete=true unique={} duplicates={} elapsed_ms={}",
        index, collector.accepted, collector.duplicates, started.elapsed().as_millis());
    Ok(())
}
fn blocking_run(settings: Settings, output: broadcast::Sender<Arc<DataBatch>>) -> Result<()> {
    let mut collector = Collector {
        expected_owner: settings.program.to_string(), venue: settings.venue, output,
        batch_spacing_ms: settings.batch_spacing_ms,
        seen: HashSet::new(), pending: Vec::with_capacity(BATCH),
        accepted: 0, duplicates: 0,
    };
    let mut errors = Vec::new();
    for (index,url) in &settings.providers {
        println!("rpc full scan start provider={} venue={:?} program={} slice_bytes=0",
            index, settings.venue, settings.program);
        match scan_one(*index, url, &settings, &mut collector) {
            Ok(()) => {
                println!("rpc full scan summary complete=true program={} venue={:?} unique={} duplicates={}",
                    settings.program, settings.venue, collector.accepted, collector.duplicates);
                return Ok(());
            },
            Err(e) => {
                eprintln!("rpc full scan provider={} failed={} retrying_next_provider=true",index,e);
                errors.push(format!("RPC_{index}: {e}"));
            }
        }
    }
    bail!("all full scan HTTP providers failed: {}", errors.join("; "))
}
pub async fn run(output: broadcast::Sender<Arc<DataBatch>>, settings: Settings) -> Result<()> {
    tokio::task::spawn_blocking(move || blocking_run(settings, output))
        .await.context("full scan blocking transport task panicked")?
}
