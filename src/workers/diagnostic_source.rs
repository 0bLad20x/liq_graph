//! Diagnostic consumer of unmodified Discovery DataBatch payloads.
//! Optionally inventories every observed JSON path and writes full representative
//! payload examples. No protocol decoding, Registry changes or database writes.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::broadcast;

use crate::core::{DataBatch, DataRecord, RecordIdentity};

const SAMPLES_PER_SOURCE_VENUE: usize = 8;

#[derive(Default, Serialize)]
struct FieldStats {
    /// Number of received records containing the path, not array element count.
    present_records: u64,
    null_records: u64,
    /// Each type contributes at most once per record, including for JSON arrays.
    types: BTreeMap<&'static str, u64>,
}

#[derive(Default)]
struct AuditGroup {
    records: u64,
    fields: BTreeMap<String, FieldStats>,
    signatures: BTreeSet<String>,
    samples: Vec<Value>,
}

#[derive(Default)]
struct PayloadAudit {
    root: PathBuf,
    groups: BTreeMap<(String, String), AuditGroup>,
}

fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// JSON Pointer-compatible object keys; "[]" denotes any array element.
/// Record-level dedup avoids counting multiple elements as multiple pools.
fn collect_paths(
    value: &Value,
    path: &str,
    fields: &mut BTreeMap<String, BTreeSet<&'static str>>,
) {
    if !path.is_empty() {
        fields.entry(path.to_owned()).or_default().insert(value_kind(value));
    }
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let escaped = key.replace('~', "~0").replace('/', "~1");
                collect_paths(child, &format!("{path}/{escaped}"), fields);
            }
        }
        Value::Array(items) => {
            let element_path = format!("{path}/[]");
            for child in items {
                collect_paths(child, &element_path, fields);
            }
        }
        _ => {}
    }
}

fn safe_part(name: &str) -> String {
    name.chars().map(|c| {
        if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' }
    }).collect()
}

impl PayloadAudit {
    fn new(root: PathBuf) -> Self {
        Self { root, groups: BTreeMap::new() }
    }

    fn observe(&mut self, batch: &DataBatch) {
        for record in &batch.records {
            let venue = match &record.identity {
                RecordIdentity::Pool { venue, .. } => format!("{venue:?}"),
                RecordIdentity::Mint { .. } => "Mint".to_owned(),
            };
            let group = self.groups.entry((batch.source.to_owned(), venue.clone())).or_default();
            group.records += 1;

            let mut paths = BTreeMap::<String, BTreeSet<&'static str>>::new();
            collect_paths(&record.payload, "", &mut paths);
            // Same field names with different nesting or JSON types are distinct.
            let signature = paths.iter().flat_map(|(path, types)| {
                types.iter().map(move |kind| format!("{path}:{kind}\n"))
            }).collect::<String>();
            let new_structure = group.signatures.insert(signature);

            for (path, kinds) in paths {
                let stat = group.fields.entry(path).or_default();
                stat.present_records += 1;
                if kinds.contains("null") { stat.null_records += 1; }
                for kind in kinds {
                    *stat.types.entry(kind).or_default() += 1;
                }
            }

            // Full original JSON, not a projection. Keep sample memory bounded.
            if new_structure && group.samples.len() < SAMPLES_PER_SOURCE_VENUE {
                let identity = match &record.identity {
                    RecordIdentity::Pool { address, mint_a, mint_b, .. } => json!({
                        "pool_address": address.to_string(),
                        "mint_a": mint_a.as_ref().map(ToString::to_string),
                        "mint_b": mint_b.as_ref().map(ToString::to_string),
                    }),
                    RecordIdentity::Mint { address } => json!({
                        "mint_address": address.to_string(),
                    }),
                };
                group.samples.push(json!({
                    "source": batch.source,
                    "venue": venue,
                    "received_at_ms": batch.received_at_ms,
                    "identity": identity,
                    "tvl_usd": record.tvl_usd,
                    "payload": &record.payload,
                }));
            }
        }
    }

    fn write(&self, batches: u64, records: u64, lagged: u64) -> Result<()> {
        fs::create_dir_all(&self.root)
            .with_context(|| format!("create payload audit directory {}", self.root.display()))?;
        let mut groups = Vec::with_capacity(self.groups.len());
        for ((source, venue), data) in &self.groups {
            let directory = self.root.join(safe_part(source)).join(safe_part(venue));
            fs::create_dir_all(&directory)
                .with_context(|| format!("create audit group {}", directory.display()))?;

            let field_file = File::create(directory.join("fields.json"))?;
            serde_json::to_writer_pretty(BufWriter::new(field_file), &data.fields)
                .with_context(|| format!("write fields for {source}/{venue}"))?;

            let mut examples = BufWriter::new(File::create(directory.join("samples.jsonl"))?);
            for sample in &data.samples {
                serde_json::to_writer(&mut examples, sample)?;
                writeln!(examples)?;
            }
            examples.flush()?;

            groups.push(json!({
                "source": source,
                "venue": venue,
                "records": data.records,
                "field_paths": data.fields.len(),
                "distinct_path_type_structures": data.signatures.len(),
                "full_payload_samples": data.samples.len(),
                "sample_limit": SAMPLES_PER_SOURCE_VENUE,
                "all_observed_records_included_in_field_counts": true,
                "all_distinct_structures_sampled": data.signatures.len() <= data.samples.len(),
                "directory": format!("{}/{}", safe_part(source), safe_part(venue)),
            }));
        }
        let summary = json!({
            "format": "cp4-discovery-payload-audit-v1",
            "received_batches": batches,
            "received_records": records,
            "lagged_batches": lagged,
            "field_count_semantics": "records containing path; arrays use /[] and count once per record",
            "sample_semantics": "full unmodified payload for first 8 distinct path/type structures per source and venue",
            "groups": groups,
        });
        let output = self.root.join("summary.json");
        let file = File::create(&output)?;
        serde_json::to_writer_pretty(BufWriter::new(file), &summary)?;
        crate::log_event!(Info,
            "payload audit complete groups={} records={} batches={} lagged={} output={}",
            self.groups.len(), records, batches, lagged, output.display());
        Ok(())
    }
}

pub async fn run(mut input: broadcast::Receiver<Arc<DataBatch>>, fail_on_lag: bool) -> Result<()> {
    let mut received = 0_u64;
    let mut received_records = 0_u64;
    let mut lagged = 0_u64;
    let mut source = None;
    let mut sources = HashMap::<&'static str, (u64, u64)>::new();
    let mut audit = env::var_os("DISCOVERY_PAYLOAD_AUDIT_DIR")
        .filter(|path| !path.is_empty())
        .map(|path| PayloadAudit::new(PathBuf::from(path)));

    loop {
        match input.recv().await {
            Ok(batch) => {
                received += 1;
                received_records += batch.records.len() as u64;
                let counts = sources.entry(batch.source).or_default();
                counts.0 += 1;
                counts.1 += batch.records.len() as u64;
                if let Some(audit) = audit.as_mut() {
                    audit.observe(&batch);
                }
                if source.is_none() {
                    source = Some(batch.source);
                    let sample = batch.records.first();
                    let identity = match sample.map(|record| &record.identity) {
                        Some(RecordIdentity::Pool { address, venue, mint_a, mint_b }) =>
                            format!("pool={address} venue={venue:?} mint_a={mint_a:?} mint_b={mint_b:?}"),
                        Some(RecordIdentity::Mint { address }) => format!("mint={address}"),
                        None => "empty_response".to_owned(),
                    };
                    let payload_fields = sample
                        .and_then(|record| record.payload.as_object())
                        .map_or(0, |object| object.len());
                    crate::log_event!(Debug,
                        "DataBatch source={} received_at_ms={} records={} payload_fields={} {}",
                        batch.source, batch.received_at_ms, batch.records.len(), payload_fields, identity,
                    );
                } else if received % 25 == 0 {
                    crate::log_event!(Debug,
                        "DataBatch source={} received_batches={} received_records={}",
                        batch.source, received, received_records,
                    );
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                lagged += skipped;
                crate::log_event!(Warn, "DataBatch consumer lagged missed_batches={skipped}");
                if fail_on_lag {
                    anyhow::bail!("REST broadcast overflow: diagnostic lost {skipped} source batches");
                }
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }

    if let Some(audit) = audit.as_ref() {
        audit.write(received, received_records, lagged)?;
    }
    crate::log_event!(Warn,
        "DataBatch output complete source={} received_batches={received} received_records={received_records} lagged_batches={lagged}",
        source.unwrap_or("unknown"),
    );
    let mut per_source = sources.into_iter().collect::<Vec<_>>();
    per_source.sort_by_key(|(name, _)| *name);
    crate::log_event!(Debug, "DataBatch per_source_batches_records={per_source:?} strict_lag={fail_on_lag}");
    Ok(())
}
