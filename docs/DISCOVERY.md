# Discovery: sources, identity and Registry ingress

Discovery workers live under `src/workers/discovery/`. They have no Completion,
Graph, MarketState or execution dependencies. Each worker owns its provider schema,
program IDs, account layouts, pagination, local deduplication and request pacing.

| Worker | Source | Purpose |
|---|---|---|
| meteora_dlmm | REST pages | DLMM identity, TVL and configuration |
| meteora_damm_v2 | REST pages | DAMM v2 identity, TVL and configuration |
| raydium | Ten REST cursor lanes and continuous head refresh | AMM v4, CPMM, CLMM and Stable inventory |
| orca_whirlpool | Five REST cursor lanes | Whirlpool inventory and configuration |
| pancakeswap_clmm | Ordered parallel REST pages | Pancake inventory and configuration |
| byreal_clmm | REST pages with reported-total checks | Byreal inventory and configuration |
| manifest | One ticker inventory | Market identities; no inferred TVL |
| pumpswap | Rolling getProgramAccountsV2 | Changed accounts and their 64-byte mint slice |
| rpc_full | Streaming getProgramAccounts | Address-only initial inventory for one configured program |
| identity_hydration | getMultipleAccounts | Missing mint identities for eleven venues |

Raydium Stable discovery does not imply a corresponding Completion/V10 executor.
Jupiter mint enrichment and DexScreener enrichment remain separate workers.

## Boundaries

`orchestration/discovery.rs` is the common registration and resource wiring for
`--run`, standalone discovery and discovery inside the Completion live mode.
Workers do not call each other. Identity hydration consumes Registry updates and
publishes ordinary source observations back to the Registry, the sole pool writer.

The existing `DataBatch`, `DataRecord` and `RecordIdentity` definitions now live in
`core/source.rs`; `core` re-exports those same definitions. There is no second batch
contract. Pool mints and USD TVL can be unknown. Missing inventory entries are not
removals; a capped or rolling scan cannot establish global pool completeness.

REST sources produce `PoolConfigUpdate` alongside their original payload. The
Registry no longer extracts provider-specific JSON paths. A patch carries fields,
explicit key/prefix removals and an optional related observation timestamp group.
Missing fields preserve prior optional values. Sources own the semantics of these
fields; Registry owns provenance, ordering, merging and timestamp coalescing. The
persisted schema/version/field names remain compatible; no DB migration is needed.
REST fee metadata remains historical configuration, not a fresh swap-fee quote.

Orca and Raydium fail the whole worker if a required lane permanently exits. The
normal runtime supervisor restarts that source. Finite runs return the actual
error. Raydium head refresh runs only in continuous mode.

Full scan remains a one-shot bootstrap. `FULL_SCAN_RPC_INDICES` defaults to `9,10`:
the providers are sequential fallbacks, not parallel inventories. A successful
provider ends the scan. There is no runtime restart loop for this bootstrap.
`FULL_SCAN_VENUE` defaults to PumpSwap; other programs require their explicit ID
and account-type filter. The response is streamed through curl and serde rather
than materialized as one JSON document. Deduplication still stores seen addresses.
Stop kills/reaps the owned curl process and joins the blocking reader. Partial
published batches remain observations; cancellation is logged as `complete=false`,
with published/unpublished counts, and is not a completed inventory.

The dedicated streaming HTTP implementation remains in `rpc_full`; migrating
that capability into the generic RPC transport is outside this refactor. PumpSwap
and identity hydration continue to use the existing shared RPC transport.

## Standalone real-input runs

Build the checked-out feature branch once:

```powershell
git fetch origin
if ($LASTEXITCODE -ne 0) { throw 'Fetch fehlgeschlagen.' }
git switch --track origin/refactor-discovery-boundaries
if ($LASTEXITCODE -ne 0) { throw 'Branchwechsel fehlgeschlagen.' }
cargo build --locked --release
if ($LASTEXITCODE -ne 0) { throw 'Build fehlgeschlagen.' }
```

If the branch already exists, use `git switch refactor-discovery-boundaries` and
`git pull --ff-only origin refactor-discovery-boundaries` instead of creating it.

All seven REST sources, one page per paginated lane and a 60-second upper bound:

```powershell
.\target\release\liq_graph.exe --discovery-live rest --max-pages 1 --duration-seconds 60
if ($LASTEXITCODE -ne 0) { throw 'Discovery-Lauf fehlgeschlagen.' }
```

Individual source names are listed in the worker table. For example:

```powershell
.\target\release\liq_graph.exe --discovery-live pumpswap --max-pages 1 --duration-seconds 30
.\target\release\liq_graph.exe --discovery-live rpc_full --duration-seconds 10
.\target\release\liq_graph.exe --discovery-live identity_hydration --duration-seconds 30
```

The full-scan command checks controlled cancellation if the scan is still active;
its zero exit code then means clean cancellation/drain, not complete inventory.
The hydration command reads unresolved identities from the configured database.
No standalone command writes the DB unless `--persist` is explicitly present.
For an actual initial inventory installation use `--discovery-live rpc_full --persist`
without a deadline. Legacy `--rpc-full-scan-live` retains its persistence behavior.

These explicit standalone runs start source and Registry output diagnostics only.
Normal simulation/runtime operation gains no diagnostic consumers. Source summary
records and Registry input totals must agree, with zero lag/conflicts. A timeout
with no records proves neither source decoding nor successful handoff. The summary
separates clean process completion, cancellation and uncapped source exhaustion;
none of these is a V10/price/freshness/profitability acceptance flag.

For optional bounded payload/config examples, set `DISCOVERY_PAYLOAD_AUDIT_DIR`
only for the standalone invocation, restoring its prior process value afterwards.
The existing source audit includes raw payload, normalized config and removals.

A short downstream integration run reuses the existing starter:

```powershell
.\scripts\runtime_live.ps1 -ExpectedBranch refactor-discovery-boundaries `
    -DurationSeconds 60 -PoolLimit 1000 -AccountBudget 25000 -Rewatch
```

There is no new starter or historical acceptance suite. Build success is a
prerequisite; the checkpoint stays open until actual source/Registry and runtime
logs demonstrate the requested real data flow on the delivered commit.
