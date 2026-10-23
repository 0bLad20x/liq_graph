# CP3 — Central Persistence & Dual Registry

**Draft.** No acceptance without `cargo build --locked` on the feature branch and real-provider logs. This checkpoint does not change the V10 fee/route logic.

## Architecture

```
Discovery REST/RPC ------------> Pool Registry (RAM) -----> Persistence
Jupiter API -------------------> Mint Registry (RAM) -----> Persistence
                                      |                          |
                              qualified mint watch               v
                                      |                    public.mints
                                 DexScreener               public.pools
                                      |                          |
                                      +----> Pool Registry <--- startup restore
```

- Two project tables: `public.mints` and `public.pools`.
- `src/workers/persistence.rs` owns all PostgreSQL SQL, DDL, batch writes, and restore.
- Existing Discovery `DataBatch` contract remains in place.
- `mint_qualification.rs` owns a RAM `MintRegistry`, receives restored mints/known pool identities and economic Jupiter events. The economic watch is independent of Graph Prepared selection.
- Jupiter source and DexScreener source never open PostgreSQL connections.
- Pool addresses and mint addresses are Base58 `TEXT`; USD fields are `NUMERIC`; times are `TIMESTAMPTZ`; variable protocol configuration and completion descriptors remain `JSONB`.
- Historical saved TVL is *not* a current live TVL on boot. The Pool Registry reevaluates live admission as sources refresh.
- `token_extensions` and `transfer_fee_config` are nullable and **not** asserted from unverified provider JSON. Validated Token-2022 metadata can be populated by a later on-chain enrichment.

## Local preparation (PowerShell)

```powershell
cd C:\dev_projects\liq_graph
git status --short
git fetch origin
git switch cp3-central-persistence-dual-registry
git branch --show-current
git rev-parse --short HEAD
git status --short
cargo build --locked
```

Do **not** proceed to destructive database actions on a compilation error. If checkout is blocked by local files, first identify and preserve those files. Never run `git reset --hard` or `git clean` indiscriminately.

## Destructive project-table reset

**Stop all liq_graph/Jupiter processes first. Verify the database target and take a backup if any data is valuable.** This destroys historical pool completion manifests and all Jupiter mint data. It does not affect unrelated tables, schemas, or databases.

```powershell
# Verify the target BEFORE executing this.
psql "$env:LIQ_GRAPH_DATABASE_URL" -c "SELECT current_database(), current_schema();"
# Explicit destructive action; no implicit runtime drop.
psql "$env:LIQ_GRAPH_DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/cp3_reset_project_tables.sql
```

`Persistence::connect` creates the two new tables on the next start; do not maintain separate duplicated CREATE DDL.

## Live acceptance sequence

1. **Pool Discovery + PostgreSQL** (real DEX REST responses, no RPC needed for this phase):

```powershell
cargo run --locked --release -- --persistence-v2-live
```

The `--persistence-v2-live` mode is a finite REST+Registry+Persistence pass. Inspect pool counts and check logs for `pool registry` and `persistence complete`.

2. **Mint Registry, Jupiter and DexScreener** (requires Jupiter keys and the configured proxy transport):

```powershell
$env:CP1_PROBE_DURATION_SECONDS="300"
cargo run --locked --release -- --cp1-live
```

Find `persistence restored_mints`, `mint registry pools_mints`, `jupiter mode=watch`, `dexscreener cp1 continuous`, and `cp1 live complete=true`.

3. **Completion manifest on real RPC** (start with DLMM to avoid unrelated venue settings):

```powershell
$env:COMPLETION_LIFECYCLE_LIVE_VENUE="dlmm"
$env:RUN_DURATION_SECONDS="300"
cargo run --locked --release -- --completion-lifecycle-live
```

Check the accepted Prepared/Incomplete proofs and persisted pool status; no mocks or simulation-only success counts qualify.

4. **Restart/restore**:

```powershell
$env:CP1_MARKET_ENRICHMENT_ENABLED="true"
$env:RUN_DURATION_SECONDS="120"
cargo run --locked --release -- --run
```

Verify `persistence restored pools`, `persistence restored_mints`, qualified watch publishing, and continued Registry/DB updates after restore.

## Read-only PostgreSQL checks

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema='public'
  AND table_name IN ('mints','pools','liq_graph_mint_catalog','liq_graph_pool_cache')
ORDER BY table_name;

SELECT COUNT(*) AS pools, COUNT(*) FILTER (WHERE status=1) AS prepared,
       COUNT(*) FILTER (WHERE tvl_usd IS NOT NULL) AS historical_tvl_known
FROM public.pools;

SELECT COUNT(*) AS mints,
       COUNT(*) FILTER (WHERE decimals IS NOT NULL) AS decimals_known,
       COUNT(*) FILTER (WHERE token_program IS NOT NULL) AS program_known,
       COUNT(*) FILTER (WHERE liquidity_usd>=2000 AND market_cap_usd>=10000) AS economically_qualified
FROM public.mints;

SELECT venue,status,COUNT(*) FROM public.pools GROUP BY venue,status ORDER BY venue,status;

SELECT pool_address,venue,mint_a,mint_b,tvl_usd,tvl_source,tvl_observed_at,
       completion_checked_at,completion_reason_code,
       jsonb_typeof(pool_config) AS config_type,
       jsonb_typeof(descriptor) AS descriptor_type
FROM public.pools ORDER BY last_seen_at DESC NULLS LAST LIMIT 10;

SELECT mint_address,token_program,decimals,liquidity_usd,market_cap_usd,fetched_at
FROM public.mints ORDER BY fetched_at DESC NULLS LAST LIMIT 10;
```

## Acceptance gates

- Two project tables only; no legacy `liq_graph_*` table.
- Only `persistence.rs` contains PostgreSQL SQL calls.
- Real token and pool records reach RAM registries and DB.
- A restart restores both registries and any saved Prepared manifest without treating historical TVL as fresh.
- `cargo build --locked` on the actual feature HEAD; real logs for Discovery → Registry → Persistence → Restore.
- Do not mark this draft complete without the above. A CI job that never obtains a runner does **not** prove compilation.
