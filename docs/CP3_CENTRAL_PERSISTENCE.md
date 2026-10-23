# CP3 — 9-column Pool Cache (Draft)

## Storage contract

PostgreSQL is a bootstrap/persistence cache, NEVER a hotpath read dependency.

`public.pools` contains exactly:
`pool_address TEXT PRIMARY KEY`, `venue SMALLINT`, `mint_a TEXT`, `mint_b TEXT`,
`tvl_usd NUMERIC`, `tvl_source TEXT`, `tvl_observed_at TIMESTAMPTZ`,
`pool_config JSONB`, `completion JSONB NULL`.

`public.mints` is unchanged in this increment.

`completion IS NULL` means Unknown. Presence of a structurally validated
`completion.manifest` (plus evidence/mints) means Prepared; otherwise a completion
with validated_slot, checked_at_ms and reason_code means Incomplete.
The persistence worker decodes this object to the existing typed `PoolState` on
startup. Existing RAM status, proof, slot, scheduling and V10 contracts remain.
Historical stored TVL is never promoted to fresh live TVL on restore.

**Scope:** This increment changes only the pool SQL schema, completion serialization,
restore and queries using removed SQL fields. It does **not** yet change the
Discovery output contracts or the incorrect Registry `pool_metadata` mapping.
Those are the next, separate task.

## Local update

```powershell
cd C:\dev_projects\liq_graph
git status --short
git pull --ff-only
git branch --show-current
git rev-parse --short HEAD
git status --short
git diff -- Cargo.lock
cargo build --locked
```

A clean build does NOT prove functional correctness. The feature branch must be
`cp3-central-persistence-dual-registry`. Do not reset/clean untracked user files.

## One-time non-destructive SQL migration

Stop all Rust processes (no old binary writers), back up the PostgreSQL database
if the existing pool/mint data is valuable, then inspect the configured target:

```powershell
.\scripts\cp3_db.ps1 -Action Inspect
```

Only after reviewing the target:

```powershell
.\scripts\cp3_db.ps1 -Action SqlFile -SqlFile scripts/cp3_pools_9_columns_migrate.sql
```

The migration uses a single PostgreSQL transaction, rejects invalid or legacy
completion proofs, retains existing pool rows and mint rows, copies any current
V3 completion descriptors into `completion` including their slot, check time
and reason code, and drops only the removed pool columns and old indexes.
It never deletes the tables or uses CASCADE. Do not execute twice.

## Immediate read-only proof

```powershell
$sql = @'
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='public' AND table_name='pools'
ORDER BY ordinal_position;
SELECT COUNT(*) AS pools,
 COUNT(tvl_usd) AS tvl_known,
 COUNT(*) FILTER (WHERE completion IS NOT NULL) AS completed,
 COUNT(*) FILTER (WHERE jsonb_typeof(completion->'manifest')='object') AS prepared
FROM public.pools;
SELECT COUNT(*) AS mints FROM public.mints;
'@
.\scripts\cp3_db.ps1 -Action Query -Query $sql
```

Must show nine columns, no lost rows and no direct `status`,
`validated_slot`, `planner_version`, `descriptor_version` or separate
`descriptor` column.

## Real live acceptance (after migration)

```powershell
.\scripts\cp3_db.ps1 -Action Verify
cargo run --locked --release -- --cp1-live
```

For Completion, use the existing `--completion-lifecycle-live` venue settings,
then inspect `scripts/completion_lifecycle_inspect.sql` through `cp3_db.ps1`
and restart the runtime to prove Prepared and Incomplete proof restore.
The Completion-Lifecycle and V10 export SQL verification scripts now read
`completion`. Any timeout, lag, incomplete finite run or missing real
evidence is NOT acceptance. No database reset or new table recreation is needed.
