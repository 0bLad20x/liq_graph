> Current entrypoint: one `--run` pipeline and `scripts/runtime_live.ps1`.
> See [CP7A_RUNTIME.md](CP7A_RUNTIME.md). Earlier checkpoint measurements and output names
> below describe their historical implementations; output loss now fails a finite proof.

# Graph signals: compact candidate state

## CP7C: graph index cleanup and focused graph evidence

Basis: `main@12d7a465ffcfec902afd75a69e0e88bcba24c284`, including CP7, CP7A and CP7B.
Feature branch: `cp7c-graph-index-cleanup`.

CP7C was accepted from the real user artifact `20261027T080736887Z` on feature
commit `25b880462c8a29b861a5085b28749102ac2898a9`: 600.003 seconds, 111/111 nonempty
exact route/index references and 24,562 observed/published transitions. PR #67 merged
it into main at `42a941f84327eadf6d08f56192b394e64e98377c`. MarketState, signals and
execution were disabled in that proof. CP7D's MarketState/catalog cleanup and its
separate live acceptance are documented in [MARKET_STATE.md](MARKET_STATE.md#cp7d-selection-and-catalog-cleanup).

`build_topologies()` now returns only a sorted `Vec<RouteTopology>`. Its two
temporary inverse-index constructions are removed. The initial builder takes
the vector directly; incremental discovery consumes its new topologies without
an intermediate Arc/copy. Enumeration, stable sort order, directed roles and
route multiplicities are unchanged. `create()` constructs the one published
pair-to-topology index after combining and validating the final generation.

The optional full reference still checks topology identity, directed roles,
multiplicities and exact inverse-index equality. Its expected mapping is derived
locally from the full enumeration, using sorted/deduplicated role pairs rather
than the production indexing loop. Direct2's repeated undirected pair produces
one reference per topology. A successful reference log explicitly includes
`pair_index=true`. These temporary proof data are not another published graph.
The normal runtime keeps reference comparison disabled unless requested.

`-Stage Graph -GraphAudit` uses the same runtime with MarketState and signals
disabled. Discovery, Registry, Persistence, mint qualification and Completion
remain on their normal path. There is no price hydration or V10 execution in
this stage. The shared starter enables the existing reference comparison for
every generation; other stages with GraphAudit retain the previous interval.
An account-subscription capacity is not required by the Graph stage.

The requested real observation is ten minutes, after a clean locked release build:

```powershell
git fetch origin
if ($LASTEXITCODE -ne 0) { throw 'Fetch fehlgeschlagen.' }
git switch --track origin/cp7c-graph-index-cleanup
if ($LASTEXITCODE -ne 0) { throw 'Branchwechsel fehlgeschlagen; lokale Aenderungen zuerst sichern.' }

.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp7c-graph-index-cleanup `
    -Stage Graph -GraphAudit -DurationSeconds 600
```

`-Rewatch`, price/signal tracing, catalog candidate acceptance and V10 simulation
belong to stages with their actual consumers and are rejected when incompatible.
No new starter or runtime implementation is introduced.

Acceptance requires a completed error-free runtime, actual observed and published
pool transitions, multiple audited graph generations, a nonempty topology universe,
and exact route/index references for every published generation. At least one
verified reference must be nonempty; comparing empty bootstrap graphs is insufficient.
The analyzer reports `incremental_new_pair_generations`; an unobserved transition
category is not presented as verified. The source retains all generation invariants
and pool-alternative consistency checks.

The analyzer measures from `runtime running=true` to `runtime shutdown reason=duration`.
Build, bootstrap and drain time cannot fill the requested 600 seconds. In this stage,
normal operator termination before the timer is not a completed duration proof.
No priced WS route or profitable candidate is required. If optional signal reports
are present in other stages, their errors, complete counter parity and delivery loss
remain checked. `-RequireCatalogReconciliation` keeps its separate CP7 witness criteria.

Artifacts remain in `runtime_live/<UTC timestamp>/`: checkout/plan, runtime log/report,
graph audit JSONL, `graph_summary.json` and the existing CSVs. Relevant graph summary
fields are `runtime_measured_seconds`, `exact_route_and_index_verified_generations`,
`nonempty_reference_generations` and `all_published_generations_reference_verified`.
Price/signal/Execution artifacts are absent when those workers are disabled.

The cleanup reduces discarded allocation/index work at bootstrap and new-edge
discovery; alternatives-only updates already bypass the full enumerator. Neither a
specific speedup nor live acceptance is inferred from a build. CP7C's accepted real
evidence is recorded above; it does not accept later feature commits.

## CP7: catalog reconciliation (merged)

The current output layout is `versioned_candidate_state_cp7`; numerical price
weights, threshold, graph admission and the four route forms are unchanged.

On a graph change, `graph_signals` compares the **two actually installed**
generations, not just the latest graph delta (watch channels may coalesce updates).
It remaps winners by mathematical topology identity and directed roles. Only new
topologies or those using changed pool-alternative lists are reevaluated, against
the prices already retained by the signal worker. Unchanged winners are reused;
a changed generation-local ID is not a new candidate. Removed topologies are
withdrawn. No price update or ranking change is required to finish this work.

The published `State.snapshot` atomically replaces plan, prices **and the entire
current candidate membership**. `snapshot.candidates` contains active witnesses,
not Entered events or execution commands. Every witness uses this snapshot's pool
slots, topology IDs, threshold and current observations. Snapshot absence means
withdrawal of a prior key; IDs from an old plan must never be applied to the new
one. The regular price/coverage `Candidates` deltas continue after that snapshot.
`clear` withdraws all prices and candidate keys. Both existing consumers validate
the replacement and subsequent membership transitions. An existing full snapshot
also restores candidate membership after output loss; no new replay service,
snapshot timer or guarantee about the time until that snapshot is added here.

Catalog changes are accounted in `Evaluated.catalog` and cumulative `counts.catalog_*`.
They do not inflate ordinary `Candidates`/`ranges` counts. The producer logs one
`cp7_catalog` summary per reconciliation and retains bounded `catalog_examples`
with actual entered/updated witnesses, topology, source observations and generation.
`snapshot_candidates` counts membership entries delivered, including reused ones;
it is not a count of new opportunities. No full reference recomputation is added
to price processing. Snapshot construction/validation cost remains observable in
the existing producer/consumer timings.

`execution_candidates` mirrors catalog membership but does not enqueue it. Only
the existing non-initialization WebSocket Price revisions can enqueue V10 jobs;
baseline, catalog and coverage remain state-only. Queue policy, seed selection,
packing, signer/RPC behavior and economic assumptions are outside CP7.

Use the existing full-runtime graph runner with real providers and local `.env`:

```powershell
git fetch origin
git switch main
git pull --ff-only origin main
git branch --show-current
git rev-parse --short HEAD
git status --short
.\scripts\runtime_live.ps1 -Stage Signals -GraphAudit -ExpectedBranch main -DurationSeconds 900 -PoolLimit 15000 -AccountBudget 25000 -RequireCatalogReconciliation
```

For an already existing local branch, switch to it and update with `git pull
--ff-only` instead of repeating `--track`. Preserve tracked local edits before
switching. The runner requires a committed clean checkout and locked release
build; it does not simulate or broadcast transactions. In addition to existing
graph/reference and signal-delivery checks, the strict option requires actual
catalog candidate visibility, withdrawal, unchanged reuse, a real catalog
witness, matching complete producer/consumer counts and zero loss. A quiet or
insufficient run fails acceptance while preserving its evidence. Examine
`summary.json`, `signal_production.json`, `signal_consumer.json`, the catalog
summaries and the retained graph audit together. Unobserved specific cases
(e.g. Direct2 threshold crossings) remain unproven; a generic successful run must
not be represented as proof of every transition category or V10 execution.

Build and live status must be attached to the actual branch HEAD. CP7 and CP7A
were merged through PR #65 after the recorded real user evidence; CP7B followed
through PR #66. This does not imply acceptance of a later feature commit.

## Historical interval implementation and earlier measurement context

The sections below describe earlier checkpoints. Their Ranges contracts,
capacities and startup/production-wiring statements are historical; use the CP7
contract above and current code for the active compact candidate pipeline.

This branch implements the measurement stage of issue #40. It runs one new signal engine.
There is no reference engine or performance comparison. An opt-in downstream V10 simulation
handoff is described below; the measurement-only mode remains the default.
A successful measurement run is not a numerical-equivalence acceptance against the previous enumerator.

## Ownership and scope

Registry admission and the existing liquidity graph determine the allowed universe.
GraphSnapshot/WatchCatalog are unchanged. Market-State still supplies normalized indicative
pool prices; RPC transport and protocol decoders are unchanged. The signal worker owns the
private compiled plan, ordered weights, validity, interval queries, output and measurements.
Core contains the necessary shared data contracts, not the evaluation algorithm.

The old per-route production path and its contracts are replaced. The optional execution
handoff adds one flat worker file, `src/workers/execution_candidates.rs`. There is no new
crate, dependency, formatter or automated test suite.

## State and compilation

Each observed pool has a stable worker-local integer ID and two canonical directed slots:
2*pool is mint-A -> mint-B; 2*pool+1 is mint-B -> mint-A. These IDs are published with
identities; they are not database IDs. The current indicative model still prepares one
logarithm per Price and reciprocal directions. Bid/ask or amount-dependent quotes are not
introduced here.

Each graph generation compiles:

- the existing Direct2, Direct3, Triangle3 and Triangle4 groups;
- one shared ordered index per directed mint-pair role, with pool identity as a tie breaker;
- pool -> group/role dependencies and slot -> ordered-role bindings;
- all role alternatives, including those whose price is currently missing.

Compilation and initial index seeding are timed as catalog_plan_build_ns. This first
measurement implementation compiles inline on a catalog change; it can delay later price
inputs. It does not claim a background generation swap or a bound on this pause. Ordinary
price updates never rebuild the plan.

A BTreeSet stores each available directed weight once in its shared role index. A price
update changes at most two indexed slots, independently of its number of
referencing groups. Equal normalized weights leave the ordered entries in place; the new
observation/revision is still published and queried. Tree maintenance is logarithmic in the role size. Fixed-role-only
directions need no ordered index. Identity checking and one logarithm are measured separately.

There is no stored floating-point score per concrete route. All route values are represented
by their current weights and role definitions. Group alternatives are not narrowed by TVL,
ranking or best-price selection in this worker.

## Mathematical query and numeric contract

For directed rates q, the normalized weight is round(-ln(q) * 2^48), stored as i64.
The inverse uses the exact negated integer. The scale is published in the plan; the threshold
is round(-ln1p(threshold_bps/10000) * 2^48). Direct2 combines two distinct pools; the other
forms combine two alternatives and one or two fixed weights:

- Direct2: a_i + b_j, excluding the same pool;
- Direct3: a_i + b_j + c;
- Triangle3: a_i + m + b_j;
- Triangle4: a_i + m + b_j + c.

For each affected group/direction, one role member is an anchor and the other role supplies
partners. The addend is the anchor weight followed by the fixed weights in their stored
order, all in normalized integer units. The qualification limit is tau_units - addend.
Membership is the strict scalar comparison partner_weight < limit.

Old and current limits partition the partner weights into at most two disjoint intervals:
the common qualifying prefix is Updated; the difference is Entered or Exited.
An initially unavailable addend has an empty prior qualifying set. Equal limits still
produce Updated, so improvements or fresh observations of already-qualified routes are
not lost. The actual current state is retained even when no interval qualifies.

A range query uses BTreeSet::range and takes the first nonexcluded member as a witness.
It does not enumerate all interval members or count their cardinality. At most one same-pool
member is skipped for Direct2. An ordinary alternative update queries its row/column in both
directions. Direct2 covers the two disjoint affected row/column sets once. A fixed-role
update walks available anchors in the shorter catalog role, then performs ordered range
lookups. Thus work still depends on referencing groups and available anchors, not just the
two indexed weight writes.

All sums and interval comparisons use the published integer units. For the same normalized
weights, classification is independent of anchor choice or update order; summation does not
accumulate floating-point drift. The witness publishes both the authoritative integer sum
and its display conversion to negative-log f64 / return bps.

Q48 has a step of 2^-48 (about 3.55e-15 in logarithmic weight). Rounding a weight contributes
at most half a unit relative to the computed f64 logarithm. Four weights plus a rounded
threshold therefore have a quantization comparison bound of 5/(2*2^48), about 8.89e-15 in
log space. This bound covers normalization only, not ln approximation or market accuracy.
Positive finite f64 rates have absolute logarithms below 745, so four legs and threshold
subtraction fit well within i64. Original observations retain their f64 prices.
External JSON readers must preserve 64-bit integers; the typed Rust stream does so.

This is an explicit normalization change, not bit-equivalence to the old f64 leg sum.
No epsilon widening is applied. Return bps are converted with expm1 only for witnesses,
not for every represented route. Nonfinite converted bps are reported as null.

These are indicative spot differences. Fees, sizing, slippage, depth, transfer fees and
execution costs are not modeled. A positive difference is not executable profit.

## Typed output

execution_layout = factorized_roles_v1
output_layout = versioned_state_ranges_v1

The stream is ordered and versioned:

1. State contains either a full snapshot or price/coverage changes. A snapshot publishes
   the plan, identities and current price values. A normal delta publishes the affected
   pool identity and immutable price observation. Every update has its own revision.
2. Ranges contains complete intervals against that revision. Each row names the group,
   direction, anchor, partner role, bounds, pool exclusion, old/current addends and change
   class. One immutable, closed-route witness includes its observation references.
3. Evaluated completes the input revision with accounting and timings.

Each ordinary price update therefore makes all its affected scores reconstructable before
query/output enumeration. A receiver must apply State, resolve/use Ranges for that revision,
then process the next State. A range block by itself is not a standalone snapshot of all its
members. A receiver retaining a chosen route for later execution must freeze its required
observations before advancing its state. No output refers to another worker's mutable arrays.

All eligible members inside each emitted interval are represented. A witness is a concrete
example, not a top-1 filter. Updated intervals also carry improvements that never cross the
configured threshold. Missing/nonqualifying routes remain represented in State.

The first nonempty interval is published immediately. Remaining rows are emitted in blocks
of up to 64 and the final partial block is flushed before the completion marker. There is
no timer, waiting for another price input, consumer acknowledgement or capacity await.
The existing broadcast capacity is 256 block/control events. Overflow invalidates delivery;
queue enlargement is not used as a performance fix.

Gap clears all prices and ordered indices. Unavailable/Removed withdraw the pool and may
emit Exited intervals. Selection withdraws all deselected pools in one state revision; the
State changes invalidate every route depending on them without individual exit expansion.
A new graph generation replaces the published plan and seeds it from retained prices.
Price-input loss publishes a clear/invalidation before the producer reports failure.
Signal-output broadcast loss is a warning: the observer records the lost count, incident,
timestamps, last sequence/revision and queue depth, discards its invalid local mirror and
continues consuming. Producer timings and independent arrival measurements continue.
State-dependent range validation, samples and trace output resume only after the next
existing full State snapshot. No replay request, extra snapshot, larger queue, waiting or
transport change is introduced. If no snapshot arrives, the observer remains unsynchronized.
Other contract violations and producer/input failures remain errors.

## Measurement report

Production and receiver counters must match for delivery acceptance. Market-State price/WS totals must also equal
signal price/WS evaluation totals. Existing source-gap/admission/coverage measurements
remain in the Market-State section of report.json.

Ranges count intervals, not concrete routes. logical_routes_affected is an analytical
topology count, including currently unpriceable combinations. ready_anchors counts queried
anchors having a valid addend and at least one valid nonexcluded partner. None of these
fields is a count of individually recalculated or successfully executable routes.

signal_production_report contains:

| Field | Meaning |
| --- | --- |
| catalog_plan_build_ns | Compilation/index-seeding times per installed generation, including the initial empty generation. |
| last_plan | Last installed generation: identities, roles, groups, logical route count, role references and index entries at installation. |
| final_price_pools / final_available_directions | Coverage retained at producer shutdown, not coverage over the entire run. |
| all_inputs | Timings for all completed state revisions, including coverage/control. |
| websocket_inputs | Timings for all completed WebSocket price inputs. |
| initialization_inputs | Baseline prices and WS initialization/resynchronization replay. |
| steady_state_inputs | WS prices explicitly marked as outside initialization replay by Market-State. |
| max_price_queue_depth / max_output_queue_depth | Observed queue depths, not substitutes for age measurements. |

Each timing group reports count, mean, minimum, maximum, p50/p95/p99 logarithmic-bucket upper
bounds. Empty populations have null values. The phase classification follows the source's
initialization_replay flag; it does not prove that all subscriptions are ready.

| Timing | Boundary |
| --- | --- |
| preparation_ns | Price identity validation, logarithm and immutable observation storage. |
| index_update_ns | Removal/insertion of shared directed weights; coverage reconciliation for control inputs. |
| state_ready_ns | Input acceptance to fully updated private factorized state and ordered indices, before constructing State output. |
| query_ns | Group/anchor walk, interval searches, witness creation and staging, excluding measured output-block construction/publication. |
| event_build_ns | State snapshot/delta and range-block construction; includes snapshot copying on generation changes. |
| publication_ns | Nonwaiting State/Range broadcast publication and publication counters. |
| first_range_ns / last_range_ns | Input acceptance to first/last range-block publication; absent if there was no qualifying/exit range. |
| processing_ns | Input acceptance through queries and output; ends before aggregate histogram bookkeeping and completion-marker send. |
| transport_decode_us | Text-frame receipt to decoded transport event. |
| transport_to_market_us | Decoded transport event to Market-State acceptance. |
| market_processing_us | Market-State acceptance to price publication boundary. |
| price_queue_us | Price publication boundary to signal acceptance. |
| frame_to_worker_us | Text-frame receipt to signal acceptance. |
| frame_to_state_ready_us | Wall-clock frame-to-worker age plus monotonic state_ready_ns, rounded up to microseconds. |

Frame timing starts when the transport reads a text frame, not at provider generation or
OS-buffer arrival. Clock reversals omit wall-clock durations and invalidate complete-pipeline
delivery checks. Maxima/percentiles of individual stages cannot be added.

signal_report contains receiver-side state, range and completion arrival ages, bounded
consumer-processing histograms, per-kind ready-anchor/range counts and bounded witnesses
from baseline and WS inputs separately for each kind. State arrival age ends before applying
the delta; frame-to-state-consumer ends after applying it. Consumer timing includes contract
checks but does not run the old reference enumeration.

Receiver counters count received events, including unverified events after a loss. They
need not balance across revisions in a run with loss. lag_incidents, first_loss_at_us and
last_loss_at_us locate output overruns; lost_events remains cumulative after recovery.
unverified_events/unverified_ranges count received data skipped by state-dependent checks,
not the contents of missing events. resynchronizations counts accepted recovery snapshots;
awaiting_snapshot describes the final mirror status. last_revision is the last applied
revision. Arrival ages still include unverified arrivals (without delta application while
unsynchronized); witnesses are collected only from synchronized revisions.

The finite live runner completes its requested duration after output loss and exits normally
with a warning, provided the producer, Market-State/input accounting, pipeline timing and
other contract checks pass. run_complete=true then means only that the run finished.
signal_output_loss_warning=true is separate from live_evidence_passed,
signal_delivery_passed and signal_measurement_passed, which remain false for any loss,
even after resynchronization. Producer timings remain available for diagnosis; this is not
a successful delivery/acceptance result. A lost output event alone no longer shuts down
the experiment. Unexpected errors still produce a nonzero exit.

signal_measurement_passed requires real WS inputs, a fully priceable topology queried,
and complete delivery. A quiet market can pass measurement without a qualifying range;
signal_candidate_passed independently records whether one was observed. Neither flag
proves equivalence to the previous implementation. reference_comparison_performed is false.

## Local run

Use the existing DB, live admission and provider settings. Provider selection is unchanged:
Configured honors the ENV account-subscribe flags and capacities; explicit indices override
selection only as before. No wallet is needed.

PowerShell:

    git fetch origin
    git switch --track origin/cp7a-unified-runtime
    git pull --ff-only
    .\scripts\runtime_live.ps1 -DurationSeconds 600 -PoolLimit 750 -ThresholdBps 0 -ProfileSignals

Use the normal error guards if running these commands in a block. The runner requires a clean
tracked checkout and cargo build --locked --release before any live subscription. Keep
TracePrices/TraceSignals off for the measurement. PoolLimit limits observation, not the full
graph catalog's topology universe.

The common runner writes checkout.json, websocket_plan.json, runtime.json and runtime.log
under runtime_live/<timestamp>. See [CP7A_RUNTIME.md](CP7A_RUNTIME.md) for the current artifact names and commands.
Report startup, resynchronization, catalog pauses and steady-state values separately.
One low-load measurement does not establish behavior at arbitrary fan-out or pool counts.

## Status

The first uploaded real run built and processed prices, but ended after about 32 seconds
when the output observer lost 158 events. This identifies a signal-output delivery failure;
it does not establish a WebSocket transport failure or full-load acceptance.
The warning/continuation change needs a new real run. Rust/Cargo, PowerShell and live
DB/RPC credentials are unavailable in the agent environment, so no build or live verification
of that change is claimed here. Issue #40 remains open for complete measurements and the
subsequent separately requested reference comparison.

## Optional live signal -> V10 simulation handoff

`execution_candidates` consumes the existing State/Ranges/Evaluated contract independently
of the measurement observer. It applies the published snapshot/deltas in order; it owns
no graph builder, global profitability engine, protocol decoder, signer or RPC client.
The existing live orchestration supplies the Registry read handle, wallet public key,
ExecutionParameters and the executor's typed `validate` packing boundary.

Only non-replay WebSocket Price revisions may create jobs. Baseline, catalog, coverage
and initialization replay still update the consumer's view but do not consume the
simulation budget. A complete Evaluated marker closes selection for one price revision.
All non-Exited range witnesses of the requested route kind are ranked by their current
Q48 sum, ascending. The first seed whose current Prepared proofs and exact V10 packing
are valid becomes the selected topology. Invalid seeds are counted and the next seed is
tried. This is selection among reported indicative witnesses, not proof of globally optimal
execution or enumeration of every represented route.

One request contains one topology/group with multiple pool alternatives:

| Signal form | V10 request |
| --- | --- |
| Direct2 | Direct group, settlement/target pool alternatives |
| Direct3 | Direct group with settlement/target and other-settlement/target alternatives, plus one conversion pool |
| Triangle3 | Bridge group: one fixed target/bridge intermediate, alternatives for bridge/settlement and target/settlement |
| Triangle4 | Bridge group with those roles quoted as prescribed by the graph, plus one conversion pool |

Forward/reverse selects the indicative weights used for ranking; V10 receives the pool
set and retains its own on-chain route selection. Both alternative roles may grow.
The selected witness's pools and fixed legs remain mandatory. Additional priced pools
from these two roles are ranked by their sum with the seed's opposite leg and fixed
weights; only sums strictly below the same signal threshold are considered. Pools are
added greedily while the exact existing V10 validator accepts the whole request. A failed
addition is removed; later, possibly cheaper additions are still tried. No new local
16-pool/account/byte validator, account truncation, fixed TVL ranking or Cartesian-product
expansion is introduced. This greedy policy does not claim an optimal pool set under
resource constraints. `alternative_pools_added` records whether the selected request
actually contains alternatives beyond its seed route.

Every selected pool must still be admitted and Prepared in the Registry, have a price
for the direction used, and supply matching manifests/mint proofs for the configured
wallet. The resulting ExecutionRequest freezes those exact inputs. No mint-proof lookup,
manifest repair, fee/depth/slippage model, amount optimization or send operation is added.
CP7A uses the normal runtime for finite runs too: restore bootstraps the Registry,
then the regular CompletionLifecycle refreshes manifests. The candidate worker itself
still performs no refresh; `manifest_refresh_performed=false` refers to that worker.
`manifest_checked_at_ms` and `manifest_validated_slot` retain actual proof ages.
Fresh market prices must not be mistaken for a fresh execution manifest.

The signal producer never awaits this consumer or the simulator. The candidate worker
uses `try_reserve` on the existing typed job channel (capacity one); busy revisions are
counted and skipped before packing. There may be one simulation running and one accepted
job waiting. This is a bounded simulation probe, not a guarantee of execution against the
latest market state. On signal output loss the candidate view is invalidated and jobs
pause until the next existing full snapshot. Previously accepted jobs remain frozen.
There is no transport change, replay request, extra snapshot or generation optimization.

Use the existing runner, existing provider configuration and the real wallet path:

```powershell
.\scripts\runtime_live.ps1 `
    -DurationSeconds 600 -PoolLimit 750 -ThresholdBps 0 -ProfileSignals `
    -SimulateV10 `
    -WalletPath (Join-Path $env:USERPROFILE ".solana\meteora-arb-test.json") `
    -SimulationLimit 10 -RouteKind All
```

`-RouteKind Direct2|Direct3|Triangle3|Triangle4` restricts this downstream probe to one
form; it does not change graph calculation. `All` does not promise coverage of all four
forms: only fully priced, qualifying, packable live inputs can produce requests.
`-ExecutionParametersPath` optionally loads an existing ExecutionParameters JSON object;
otherwise the shared Rust defaults apply, including trade_size=0 and no_failure=false.
No parameter is changed just to manufacture a successful simulation. `-SimulateV10` is
explicit opt-in. Without it the runner sets GRAPH_EXECUTION_SIMULATE=false and neither
loads a signer nor starts either execution worker. Both settings use the same `--run`.

Native configuration for the common `--run` entry additionally requires
MARKET_STATE_ENABLED=true, GRAPH_SIGNALS_ENABLED=true and the usual live admission settings:


- GRAPH_EXECUTION_SIMULATE=true
- GRAPH_EXECUTION_WALLET_PATH=<absolute keypair path>
- GRAPH_EXECUTION_MAX_REQUESTS=10 (1..5000; successful queue submissions, not profitability)
- GRAPH_EXECUTION_ROUTE_KIND=All
- GRAPH_EXECUTION_PARAMETERS_PATH=<optional ExecutionParameters JSON path>

The runner builds with --locked --release before opening connections and preserves the
existing environment afterward. No private key bytes are logged or copied into artifacts.
After the live duration it drains accepted simulations with RPC still available (up to
300 seconds), then writes additional flat files beside checkout.json/runtime.json/runtime.log:

- request-NN.json: complete reusable ExecutionRequest;
- selection-NN.json: trigger, generation/revision, direction, original indicative seed,
  selected role/pool prices and observations, proof times, packing counts, rejection
  counts and timings;
- simulation-NN.json: actual existing executor report with the same request ID.

execution.json retains execution_candidates, execution_reports, simulation_successes,
simulation_failures and simulation_dex_calls_observed. Per request, selection_ns includes
ranking and rejected seed attempts through construction; frame_to_request_us starts at
local WebSocket frame read, signal_to_request_us at signal state readiness. The original
price_pipeline distinguishes initialization and the preceding transport/market stages.
These are not network-to-chain execution latency measurements.

execution_handoff_observed requires at least one actual non-replay live request, exactly
one corresponding report per submitted request, matching exact packing metrics, and an
actual V10 program invocation with a completed simulation response. A raw on-chain
simulation failure can establish that handoff; it is counted separately and is never
called a successful simulation or profitable trade. execution_integration_passed additionally
requires at least one successful simulation (with or without observed DEX calls). A run
with only on-chain errors does not pass acceptance; their causes are not inferred. Local
rejection, RPC failure, missing reports or zero requests fail the handoff. Output-loss measurement flags remain independent
and false for a lossy run; any request issued after a loss requires snapshot recovery.
Successful simulation, DEX-call observation and profit remain distinct evidence.

Implementation status: new handoff code is a draft until a locked build and local live
run prove these artifacts. The earlier factorized calculation measurements do not validate
this new worker or any of the four V10 mappings.
