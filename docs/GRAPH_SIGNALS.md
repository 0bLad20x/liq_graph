[CP8D economic primary qualification](CP8D.md) runs before packing in the existing candidate consumer; marginal signals remain preliminary. External V10 parameters are unchanged.

# Graph and Signal Engine

Launch and acceptance: [RUNTIME.md](RUNTIME.md). Prices: [MARKET_STATE.md](MARKET_STATE.md).
Execution boundary: [EXECUTION_V10.md](EXECUTION_V10.md).

## Ownership and graph completeness

Registry admission and Prepared proofs determine the graph universe. GraphLiquidity
owns mint pairs, alternatives, closed compact topologies and one published inverse
pair-to-topology index. Direct2, Direct3, Triangle3 and Triangle4 remain represented;
pool alternatives are not expanded into a full route product. Changes to alternative
counts reuse existing topology identities; structural mint-pair changes discover or
remove the affected topologies. No discarded builder index is returned.

WatchCatalog projects the graph's actual pool identities, role contexts and pair
alternative counts needed by selection. It shares the GraphSnapshot dependency index.
It retains no copied topology ID arrays, mint array or diagnostic pair summaries.
An explicitly connected standalone Jupiter output may derive mint addresses; the normal
runtime does no such projection. Its Jupiter targets come from MintQualification.

The optional full reference independently compares topology identity, directed roles,
route multiplicities and the complete inverse index. It is outside the price hotpath.
`Stage Graph -GraphAudit` checks every published generation. Other stages with
GraphAudit check generation 1 and every tenth generation. The analyzer distinguishes
sampled references from verification of all generations.

## Price evaluation and catalog reconciliation

Signal Engine owns stable pool identities, current prices, ordered directed weights,
compact topology rankings and qualifying candidates. Weights use integer Q48
negative logarithms. A candidate is the best closed-route witness for a topology and
direction below the configured threshold; it is not a swap quote or profit guarantee.
CP8A.2 uses directed DLMM/PumpSwap marginal DEX-net weights as its sole
selection/admission score. The default zero threshold requires strictly positive
estimated DEX-net return. Missing net rates make that direction unavailable;
there is no gross fallback. The existing factorized index and all topologies stay.
Amount-specific route profit, exact transfer-fee effects, slippage and execution
costs are outside this estimate. [CP8B](CP8B.md) restricts DLMM marginal rates to
observed reachable positive-output levels and carries depth in the same price
contract. Material depth changes refresh affected candidates even at equal net
weights. There is no second index or gross fallback. See also [CP8A](CP8A.md).

On graph replacement, the worker compares the two actually installed generations.
It remaps winners by topology identity and directed roles. New topologies and those
using changed alternative lists are evaluated against already available prices.
Unchanged winners are reused and removed topologies are withdrawn, without waiting
for another price update. Catalog accounting distinguishes entered, updated, exited
and reused memberships. A generation-local ID change is not a new opportunity.

## Versioned output

Workers exchange `core::signals::SignalEvent` through the ordered bounded broadcast:

1. State installs a complete snapshot or changes at one revision. A snapshot replaces
   plan, pool identities, prices, threshold and all active candidate memberships.
2. Candidates publishes Entered, Updated or Exited changes against that state.
3. Evaluated completes the revision with counts and timing.

Catalog replacement uses a snapshot rather than ordinary candidate deltas. Snapshot
memberships include reused candidates and must not be counted as new opportunities.
Clear/coverage changes withdraw invalid prices and affected candidates. A consumer
rejects sequence/revision errors, inconsistent witnesses and incomplete evaluations.
Lag invalidates consumer state; recovery requires a complete later snapshot. A finite
proof with loss remains incomplete even if a subsequent snapshot restores state.

Only non-initialization WebSocket Price or fee/clock Revaluation revisions may enqueue V10 jobs. Baseline,
catalog reconciliation and coverage changes update candidate state without triggering
execution. The actual ExecutionCandidates worker validates the contract at its input.
The independent executor request validator remains necessary for manual requests.

## Measurement

`signal_production.json` records actual input totals, state/candidate/evaluation counts,
catalog reconciliations, bounded examples, queue maxima and latency histograms.
Initialization/replayed inputs and ordinary WebSocket inputs have separate timings.
Nanosecond fields measure local work; microsecond fields measure pipeline stages
including frame receipt, MarketState, queue and signal state readiness. Percentiles
are histogram upper bounds, not exact sample percentiles.

In simulation there is no diagnostic Signal Engine mirror. Producer event, state,
candidate batch, candidate and evaluation counts must match the real ExecutionCandidates
report with zero input loss. Execution validates every revision after its request limit
is reached; the limit bounds simulation work, not signal consumption.

For a signal-only proof, `RUN_SIGNAL_PROOF=true` explicitly starts one validating
consumer. The common starter sets this only when signals run without simulation.
Its report is `signal_consumer.json`; all producer/consumer counters must match.
Simulation rejects that proof setting, so the second mirror cannot be enabled there.

`RequireCatalogReconciliation` is an optional strict event-specific proof. It requires
real candidate visibility, withdrawal, reuse and a catalog witness. Absence of such
market events is incomplete evidence, never an invented success.

CP8A.2 adds local monotonic net-candidate window durations inside the owner state,
with threshold, availability, catalog, gap and right-censor outcomes separated.
Winner changes and catalog ID remapping preserve continuous qualification time.
These are estimate-membership windows, not guaranteed executable profit windows.
Fee revaluations have a separate timing population; frame-to-evaluation completion
complements frame-to-state readiness. See CP8A.md for units and run comparison.
