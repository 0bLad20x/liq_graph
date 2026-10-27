# Completion ownership and live proof

All ten protocol workers and the single lifecycle live under `src/workers/completion/`.
The protocol module names are: `meteora_dlmm`, `meteora_damm_v2`, `raydium_amm_v4`,
`raydium_cpmm`, `raydium_clmm`, `orca_whirlpool`, `pancakeswap_clmm`, `byreal_clmm`,
`manifest`, and `pumpswap`. Raydium Stable has discovery but no Completion executor.

`orchestration/completion.rs` owns registration and the common start function used
by `--run` and `--completion-live`. Each protocol supplies its batching capacity and
owns decoding, PDAs, account order, mint compatibility, full checks and light checks.
Lifecycle owns scheduling, retries, request IDs, late-response protection and waiting
for Registry acceptance. Registry is the sole pool writer and admission authority;
Persistence is the sole durable writer. Core contains the shared messages and the
channel-based RPC client. Provider selection, encoding, rate limits and retries stay
in the RPC transport worker. There is no additional crate or shared protocol engine.

DLMM no longer has a 16-pool constant in orchestration. Its batch size is
`RPC_ACCOUNT_BATCH_SIZE / (3 + DLMM_EXECUTION_BELOW + DLMM_EXECUTION_ABOVE)`:
LbPair, bitmap, active BinArray and neighboring BinArrays. With the defaults
100/2/2 this is 14 pools per batch. This is a bounded work batch, not a universe
limit. The six-BinArray protocol limit remains. Invalid account/window combinations
fail at configuration instead of continuously producing account-budget retries.

Normal runtime scheduling defaults remain unchanged: Prepared refresh 900 s,
Incomplete recheck 300 s, full refresh 3600 s, retry 60..900 s plus pool jitter.
A light Unchanged check confirms existing structure without renewing the persisted
full-proof timestamp. Retryable failures retain the last accepted proof. This
checkpoint introduces no manifest age gate, fee model or V10 simulation acceptance.
Signer binding is validated before restored manifests can reach the graph. Existing
JSON fields, evidence versions, mint policies and legacy persisted signer decoding
remain compatible; there is no schema migration or blanket revalidation on startup.

The old protocol-specific Completion flags, `--completion-lifecycle-live`,
`--completion-limit`, `--pumpswap-rolling`, and the historical Completion-backed
`--graph-live`, `--persistence-write-live` and `--stability-live` setups are removed.
Use `--completion-live` for a bounded Completion profile, `runtime_live.ps1 -Stage Graph`
for graph evidence, `--run` for continuous operation and `--persistence-v2-live` for
Discovery persistence. The old Byreal-only recheck environment switch is removed;
`--revalidate` performs the same bounded operator intent for any selected protocol.

## Bounded real-input proof

Use branch `refactor-completion-boundaries`, then build once with
`cargo build --locked --release`. Verify the branch, HEAD and clean tracked state
before running. No additional starter or automated test suite is introduced.

```powershell
.\target\release\liq_graph.exe --completion-live all `
    --duration-seconds 60 --pool-limit 8 --revalidate --persist `
    2>&1 | Out-File -FilePath completion_live.log -Encoding utf8
if ($LASTEXITCODE -ne 0) { throw 'Completion-Nachweis fehlgeschlagen; Log pruefen.' }
```

`--pool-limit` bounds a fixed cohort per protocol. `--revalidate` forces an initial
full check even for a paused durable reason; a successful Registry acceptance
consumes that one-shot override. Normal retry/backoff then applies. This mode uses
restored real pools/mints and the same MintQualification policy as production.
Discovery and Jupiter enrichment are disabled by default for this bounded profile.
`--discover` enables the selected venues' continuous sources; `--enrich` enables
Jupiter and requires configured API keys. These flags do not change the normal
`--run` pipeline: its Discovery and enrichment remain enabled as configured.
It cannot grant admission using invented TVL or a diagnostic shortcut.

A single protocol is selectable, for example `--completion-live byreal_clmm`.
`--test completion_byreal_clmm` resolves to that same profile. Parameters match the
all-protocol command. Only `--persist` starts the database writer. The default mode
reads existing database state and keeps accepted updates in RAM.

PumpSwap Prepared manifests require the same configured `PUMPSWAP_V10_WALLET` as
the stored signer binding. Use the existing `--v10-wallet KEYPAIR.json` utility to
resolve a public key if needed. The proof never broadcasts or simulates a transaction.

The command prints per-protocol `dispatched`, `accepted`, `proofs`, `unchanged`,
`prepared`, `incomplete` and `retryable` counts. Zero accepted work in any requested
protocol fails evidence. With
`--revalidate`, at least one accepted full proof per requested protocol is required.
An Incomplete result is a valid checked result; these counters do not imply that
every pool was Prepared or that a trade is executable. Shutdown drains protocol
responses, Registry acknowledgements and, when enabled, persistence writes.
`COMPLETION_REPORT_PATH` optionally stores the same small owner-counter summary.
Only this explicit profile has a stateless Registry output proof consumer.

For a targeted light-refresh proof, lower `COMPLETION_LIFECYCLE_REFRESH_SECONDS`
and `COMPLETION_LIFECYCLE_RECHECK_SECONDS` temporarily for this bounded profile and
restore both process values afterwards. An observed `unchanged` result proves the
light path; absence of that result is not silently accepted as light-path evidence.

A short downstream integration run uses the existing starter:

```powershell
.\scripts\runtime_live.ps1 -ExpectedBranch refactor-completion-boundaries `
    -DurationSeconds 60 -PoolLimit 1000 -AccountBudget 25000
```

No Rewatch or V10 proof is requested for this structural change. Existing market
and signal delivery requirements remain active. Lifecycle refresh evidence uses final owner counters instead of per-pool logs. No additional production diagnostic consumers are started.

## Closing fix and complete integration run

The supplied all-protocol log showed 75 accepted proofs (66 Prepared, 9 Incomplete)
and 5 Retryable responses across 80 requests. All ten venues had accepted work;
there were no rejected Registry writes or missing acknowledgements. This was an
individual Completion proof, not a full-system or light-refresh proof.

Its excessive output was concrete: 130,940 Debug lines from Registry, including
130,794 admission-transition records. Registry no longer formats or emits per-pool
admission or completion logs while holding its locks. Mint updates publish the
unchanged typed pool updates, then log one aggregate with `install_us`. Ordinary
Discovery admission changes contribute to a final aggregate. Membership-crossing
records needed by the graph analyzer remain opt-in via
`POOL_REGISTRY_ADMISSION_AUDIT=1`, set automatically by `-GraphAudit`, and are emitted
after publication and lock release. This does not alter admission policy.

Incomplete/Retryable outcomes retain per-venue/reason counts and one bounded sample
pool plus diagnostic message per reason in the Registry's final `completion_issue`
records. Lifecycle's final `protocol_summary` distinguishes full Prepared/Incomplete,
light Unchanged and Retryable outcomes. These are small owner summaries, not extra
consumers. Debug output now always requires an explicit `LIQ_GRAPH_LOG` setting,
including the `--test` aliases. The runtime starter overrides inherited Debug filters,
writes worker logs to a file and prints the final summary.

## Registry bootstrap correction and 30-minute integration

The previous 900-second run on `983248a` completed with all ten Completion venues,
117,676 accepted light Unchanged results, exact market/signal delivery and 100
simulations covering Direct2, Direct3, Triangle3 and Triangle4. It exposed an
independent start-state bug: the Runtime copied graph seed pools from Persistence
before Registry restore computed their admission. Light Unchanged checks correctly
do not rewrite Registry state and therefore cannot repair a missing graph seed.
The last counts differed by 23 admitted Prepared pools; the WSOL/USDC counts were
186 in Registry and 163 reconstructed from graph transitions. A reference comparison
using only the graph's own input set did not detect the missing Registry membership.

Registry now publishes its restored Prepared snapshot through the existing typed
update contract, with its already computed admission decisions. All consumers are
connected before publication, and the Registry writer starts only afterwards.
There is no second seed built directly from database objects and no admission
policy in orchestration. Qualification's watch receiver is connected before its
producer starts. Light Unchanged semantics remain unchanged.

After every normal Runtime shutdown, all tasks drain first. The Graph worker's
one-shot comparison then checks concrete pool addresses and canonical mint pairs
against the final Registry snapshot. `graph_membership_passed` is required for finite
runtime acceptance. `graph_membership` reports both universe and WSOL/USDC counts,
plus missing, unexpected and pair-mismatched addresses. This is a final comparison,
not another live consumer, and it never repairs the graph to hide missing deltas.
The existing independent topology/index reference audit remains available.

For the requested half-hour run, use:

```powershell
.\scripts\runtime_live.ps1 `
    -ExpectedBranch refactor-completion-boundaries `
    -DurationSeconds 1800 -Stage Signals `
    -PoolLimit 15000 -AccountBudget 25000 `
    -CompletionRefreshSeconds 900 -CompletionRecheckSeconds 300 `
    -GraphAudit -SimulateV10 -SimulationLimit 0 -RouteKind All `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

`SimulationLimit=0` and `GRAPH_EXECUTION_MAX_REQUESTS=0` mean no total request cap;
this is now the default. A positive value still permits a bounded operator run.
There is still one executing simulation and at most one queued request. Busy
revisions are counted and skipped rather than blocking signal calculation. Handoff
and artifact matching index reports by request ID instead of repeatedly scanning
the entire report list. Every accepted request must still have exactly one complete
simulation report with matching packing; duplicate and missing IDs fail evidence.
Original request, selection and simulation artifacts remain available for all jobs.
Their total size grows with the number of accepted requests.

This run uses the regular 900/300-second Completion intervals, with full refresh
still at 3600 seconds, and no Completion inspection cohort. It leaves observations
continuously enabled; Rewatch was already proven in the previous run. `-Rewatch`
remains available for an explicit removal/reselection proof. Discovery, enrichment,
persistence, all ten Completion workers, Graph, MarketState, signals and V10 all
use the same normal runtime. A one-shot full inventory RPC bootstrap remains a
separate operator mode. Pump rolling Discovery requires a configured RPC V2 provider;
disabled sources are explicitly logged. The starter resolves only the simulation
wallet's public key for PumpSwap and retains the persisted signer compatibility guard.
No transaction is broadcast.

`summary.json` includes Completion counters, bounded per-reason diagnostic samples,
actual Registry input counts by Discovery source, log size/level counts and the new
exact membership result. MarketState also reports `ready_pools` separately from
structurally installed `hydrated_pools` and priceable `priced_pools`; pricing and
hydration behavior are unchanged. `completion_protocol_evidence_passed` requires accepted work
for every Completion venue; positive `unchanged` counts establish a light check.
`RouteKind All` permits every route form but does not guarantee each has an opportunity.
Successful handoff and successful on-chain simulation remain distinct outcomes.

The graph audit adds diagnostic work, so the run measures this integration profile.
The implementation environment has no local Rust/PowerShell or configured database,
RPC or wallet. CI checks the exact feature-head locked build and existing utility
syntax. The new 1800-second real-input run remains pending until its artifacts are
available. Build success does not close the checkpoint.

## Remaining protocol verification

Mint compatibility rules and Retryable/Incomplete classification intentionally
retain their current protocol differences. They require a separate semantic review
before changing route eligibility. PumpSwap reads pool/global state and dependent
mint/vault/fee accounts in successive contexts. `minContextSlot` is a lower bound,
not an atomic shared snapshot; the comment now states the actual behavior. This
checkpoint does not claim that snapshot coherence or V10 execution is proven.

Build and source review are prerequisites. The checkpoint remains pending until
real logs on the delivered commit show each requested protocol's accepted work,
persistence where requested, any claimed light refresh and the downstream handoff.
