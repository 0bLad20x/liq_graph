# Graph signals: factorized state and versioned interval output

This branch implements the measurement stage of issue #40. It runs one new signal engine.
There is no reference engine, performance comparison, transaction simulation or execution.
A successful measurement run is not a numerical-equivalence acceptance against the previous enumerator.

## Ownership and scope

Registry admission and the existing liquidity graph determine the allowed universe.
GraphSnapshot/WatchCatalog are unchanged. Market-State still supplies normalized indicative
pool prices; RPC transport and protocol decoders are unchanged. The signal worker owns the
private compiled plan, ordered weights, validity, interval queries, output and measurements.
Core contains the necessary shared data contracts, not the evaluation algorithm.

The old per-route production path and its contracts are replaced. No new worker, crate,
dependency, formatter or automated test suite is introduced. The existing V10 executor is
not connected to these signals by this checkpoint.

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
    git switch agent/factorized-route-state
    git pull --ff-only
    .\scripts\graph_signals_live.ps1 -DurationSeconds 600 -PoolLimit 750 -ThresholdBps 0 -ProfileSignals

Use the normal error guards if running these commands in a block. The runner requires a clean
tracked checkout and cargo build --locked --release before any live subscription. Keep
TracePrices/TraceSignals off for the measurement. PoolLimit limits observation, not the full
graph catalog's topology universe.

The existing runner writes checkout.json, websocket_plan.json, report.json and worker.log
under graph_signals_live/<timestamp>. Upload all four, including failed/partial runs.
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
