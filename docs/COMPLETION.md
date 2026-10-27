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

The command prints per-protocol `dispatched`, `accepted`, `proofs` and `unchanged`
`prepared`, `incomplete` and `retryable` counts. Zero accepted work in any requested protocol fails evidence. With
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

For the requested 15-minute full-system run, use the existing starter:

```powershell
.\scripts\runtime_live.ps1 `
    -ExpectedBranch refactor-completion-boundaries `
    -DurationSeconds 900 -Stage Signals `
    -PoolLimit 15000 -AccountBudget 25000 `
    -CompletionRefreshSeconds 120 -CompletionRecheckSeconds 120 `
    -GraphAudit -Rewatch -SimulateV10 -SimulationLimit 100 -RouteKind All `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

This uses the normal Discovery, enrichment, persistence and all ten Completion
workers, then Graph, MarketState, signals, ExecutionCandidates and the V10 executor.
Completion has no inspection cohort limit. The two 120-second overrides exercise
repeat/light checks within 900 seconds; they are run configuration, not new defaults,
and are restored with the other process variables. The independent one-shot full
inventory RPC bootstrap remains excluded, as in the normal runtime: it is not a
missing continuous Discovery source. Pump rolling Discovery requires a configured
RPC V2 provider; disabled sources are explicitly logged. The starter resolves the
simulation wallet's public key for PumpSwap, without printing or moving its secret
key; the existing persisted signer compatibility guard still applies.

`summary.json` adds all ten Completion protocol counters, aggregated diagnostic
samples, actual Registry input counts by Discovery source, log size and level counts.
`completion_protocol_evidence_passed` requires accepted work for every protocol;
light refresh is proven only by positive `unchanged` counts. This is reported
separately from market/signal delivery and V10 results. `RouteKind All` permits all
four route forms; it does not guarantee that the bounded request sample contains
each form. At most 100 real simulations are requested and no transaction is sent.
A correct simulation handoff with on-chain errors does not prove successful swaps.
The graph reference audit adds diagnostic work, so its timings represent this
integration configuration rather than a clean production speed benchmark.

The live run must execute on the operator's configured checkout. The implementation
environment has no Rust/PowerShell, database, RPC or wallet configuration. CI checks
the locked build and script syntax on the exact feature head; it cannot substitute
for this live evidence. Keep the checkpoint open until the resulting artifact folder
has been evaluated.

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
