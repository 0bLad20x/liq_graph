# CP8D: economic primary evidence before packing

Base: `2758473a60933ab19c408c1bed2a7f5261ba49c4`, CP8C merged as an
explicitly incomplete comparison baseline. Branch: `cp8d-economic-signal-qualification`.

The real CP8C run `20261028T192217273Z` produced 2,174 requests, 250 successful
simulations, no observed profit above network fees, and zero final amount vetoes.
The PowerShell postprocessor exhausted memory on a 244 MB combined report.
This checkpoint does not call that baseline economically accepted.

Manual inspection of those real records gives two useful anchors. Recorded primary
637 has one USDC atom at its bottleneck; even its fee-free final Pump conversion
can return at most 10 lamports, below the 5,000-lamport guard before input cost.
This is a bound on that recorded primary, not all omitted conversion alternatives.
In record 2139 the actual 359,976-lamport first input produces about 8,335,703 token
atoms under the saved first-level model; the second pool's first level accepts
8,335,729. Larger first inputs cross that boundary. This helps explain the chosen
size; summarized first-level data cannot establish profit over all later levels.
Neither observation is a live acceptance of the new implementation.

## One decision path

GraphSignals remains the fast marginal DEX-net candidate producer. Its output is
a preliminary price condition, not a profitable-trade assertion. The existing
ExecutionCandidates owner now qualifies a concrete primary route **before** any
packing. There is no new worker, observer, RPC-per-signal path, crate or decoder.

`execution_candidates/amounts.rs` reuses the existing generic level/reserve math.
It evaluates the candidate's directed mint roles over their currently priced
pool alternatives. Forward output envelopes and optimistic inverse paths avoid
enumerating Cartesian pool products. The best positive sampled absolute gain is
retained, rather than the first positive atom-sized result. Every recorded
primary is a closed, positive, full-fill integer chain over named pools.
Search is shortened by proved gross output bounds: a no-rebate estimate cannot
gain more than its output cap minus input. This is not a configurable work budget.

All representable doublings from the cost-derived lower input bound and observed
level input boundaries (also their adjacent integers) are considered. Boundaries
are projected through prefix domains. These are search points, not a proof of a
global optimum. Greedy forward paths and inverse relaxations may miss other
assignments, including those affected by rounding. Failure to find an economic
witness is **unknown**, not a proof of all-amount unprofitability.

Conservative negative proofs use exact rational raw-rate products and complete
observed capacity bounds. A bottleneck's output is propagated through optimistic
remaining rates. If even that output/profit cannot exceed the cost guard, the
frozen primary route is excluded. Missing tails or unresolved alternatives prevent
capacity proofs. Raw ratios here are upper bounds only, not a restored gross
price signal or gross fallback. Future state is never covered by the proof.

Internal settings, independent of `ExecutionParameters`:

* `GRAPH_ECONOMIC_NETWORK_FEE_LAMPORTS=5000`: explicit baseline fee assumption;
  the actual simulation fee is recorded separately.
* `GRAPH_ECONOMIC_COST_RESERVE_LAMPORTS=0`: additional configured allowance.
* `GRAPH_ECONOMIC_MIN_NET_PROFIT_LAMPORTS=0`: extra required net surplus.

Qualification requires DEX gain strictly above network assumption + request
priority fee + cost reserve + extra minimum surplus. There is no hard-coded dollar
notional or cents target; the earlier dollar figure was withdrawn by the user.
Contract-specific profit charges are **not verified by this model**. The explicit
scope is DEX net minus a configured cost guard, not guaranteed all-cost profit.
Host rebates are omitted in positive estimates, allowed optimistically in proofs.
Transfer-cost-unknown inputs cannot support a positive primary.

WSOL costs are in lamports. Other settlements require a SOL/settlement observation
from the deepest complete observed reference. Its raw upper rate converts the
configured guard with ceil rounding. The chosen reference and observation time
are recorded. This is a conditional in-memory valuation, not an oracle or a USD
peg. Missing reference means unknown. Mixed-slot observations remain non-atomic;
neither fresh global/Clock frames nor unchanged digests prove fresh pool accounts.

## Packing and repeated state

Current event candidates are economically assessed before packing. The bounded
CPU task first evaluates each concrete source witness, then explores pair-domain
alternatives for unresolved candidates. A positive source witness is sufficient;
a failed source witness is not a verdict on other pools. Ready economic proposals
can be dispatched before the remaining candidates finish. Marginal scores order
work only; they do not replace the amount/cost gate. No global best-across-candidates
ranking is claimed.
The chosen primary must satisfy the existing net contract, unique-pool rules,
current Prepared proofs/account digest/signer, and V10 capacity. An unfit primary
is not replaced by the old unqualified witness.

Every added direct/partner alternative must support its own economic substitution
of that primary at the same initial input, with a positive closed estimate above
the same guard. These alternatives are ranked by absolute sampled gain. Individual
substitution evidence is not a claim that arbitrary combinations of alternatives
are economic. Intermediate/conversion pools are chosen by the primary evaluation.
V10 may still choose a different route or size; its choices are compared afterward.

The CP8C global-universe rescue and after-packing seed repair are removed. No graph
route is deleted. A private pair index avoids repeated whole-pool scans. Cached
decisions are keyed by directed mint roles, all relevant material profile/account
digests, transfer-fee status and internal guard. Generation-only changes preserve
these identities. An unchanged already-dispatched state is not a new opportunity.
There is no failure blacklist or cooldown. Cache entries are pruned with catalog
membership, and gaps clear them.

One private blocking CPU task and one result slot keep calculation separate from
the existing typed input consumer. That consumer continues applying/validating the
complete stream. Prefix sums preserve exact full-bin fees/component rounding;
queries binary-search complete bins and calculate one partial bin. Compiled curves
are reused by material pool identity throughout a job, including packing. Integer
overflow falls back to the original incremental arithmetic rather than invalidating
otherwise representable small fills. There is no new runtime, worker, RPC or work
budget. The amount search checks cancellation between probes/levels; dropping its
owner also cancels it. Closing the input stops new dispatches and cancels unfinished
search while accepted simulations retain their normal drain.

Each completed proposal is rechecked against live candidate membership, the material
profile/fee fingerprint, cost reference and Prepared manifests before enqueueing.
Unrelated revisions do not by themselves invalidate it; changed relevant inputs do.
Generation changes conservatively discard old proposals. Busy revisions/results and
stale results are counted separately; they are not delivery losses.

Simulation backpressure behavior remains bounded: busy input revisions are counted
and skipped, not queued for complete later service. Economic statistics describe
the assessed live candidate revisions, **not every route or every market state**.
The marginal prefilter is not proved complete for every future protocol/fee curve.
Price coverage, stale state, funding constraints, search completeness and V10 amount
choice can still limit results; no profitability improvement is promised.

## Evidence and acceptance

`execution.json` is a compact owner summary without repeated frozen requests or full
simulation bodies. Existing `selection-NN.json`, `request-NN.json`, and
`simulation-NN.json` retain complete per-request artifacts. The existing starter
reads one selection at a time and keeps a small projection. Full diagnostic models
are bounded to two examples per decision in `economic_models.json`, including exact
level vectors; the starter never parses this large raw-model file.

`execution_candidates_progress.json` is refreshed every five seconds with compact
completed counters and the current job/topology/phase/time. The final version marks
`complete=true`. A candidate drain failure retains this partial report in the
execution summary; missing request counts remain null, never a fabricated zero.
Job started/completed/cancelled counters distinguish shutdown cancellation from a
completed economic assessment. Source-witness and pair-domain decisions carry
different explicit scopes; one candidate can therefore have two assessments.

`economic_evidence.json` counts real decisions, bounds, probes, cache reuse,
unchanged states and processing times. `economic_comparison.json` records expected
primary flow, actual declared pools from inner CPI account lists, generic parsed
token transfers, the frozen model evaluated at actual observed leg inputs when
resolvable, wallet token deltas, actual network fee and native balance changes.
Wallet token outflows outside declared swaps are retained without assuming their
purpose, so contract payouts can be separated from DEX output differences.
Ambiguous legacy transfers remain unresolved. No protocol instruction decoder is
duplicated. Fees/host rebates and different bank states can explain differences;
this comparison is not a same-state mathematical proof.

Known wallet-owned account deposits are reported separately. Native + WSOL liquid
change is retained. Positive-after-network evidence additionally requires success,
no consumption of other pre-existing token inventory and positive liquid change
after excluding recorded wallet deposits. Non-native settlement fee valuation is
not counted as this native economic proof. Recoverability and other costs still
need review; no simulated result is a real broadcast or strategy-profit claim.

Zero qualified requests are valid. Delivery/accounting is checked even with zero
jobs; `execution_handoff_observed` stays false and simulation success stays false.
`-RequireEconomicEvidence` requires real calculations and model consumption, but
does not fabricate a positive signal or require a profitable market to exist.
Its legacy spelling `-RequireAmountEvidence` is only a CLI alias, not another mode.

Locked release build and script parsing must pass on the exact feature head.
No automated test suite or formatting checks are permitted by AGENTS.md. Live
acceptance remains pending in the configured Windows environment. First use one
bounded 600-second run; extend only if the evidence justifies it:

```powershell
.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp8d-economic-signal-qualification `
    -EnabledVenues 'meteora_dlmm,pumpswap' `
    -Stage Signals -DurationSeconds 600 `
    -PoolLimit 0 -AccountBudget 25000 -ProfileSignals `
    -RequireFeeEvidence -RequireLiquidityEvidence -RequireEconomicEvidence `
    -SimulateV10 -SimulationLimit 0 `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

V10 `minimum_profit=0`, `trade_size=0`, other CPI settings, and the external
instruction path are unchanged. Internal guard settings never overwrite them.
Current expected signal sizes are hypotheses; actual V10 size/pool differences
remain evidence. Check model bounds against complete saved observations, verify
qualified primary inclusion, quantify same-size leg differences, fee-assumption
errors, processing latency and actual after-cost results before any merge decision.

## First real CP8D run and correction

Run `20261028T223232895Z` on `027b0f7` stopped its producer window at 600.001
seconds but failed candidate drain at 630.008 seconds. ExecutionCandidates logged
45,014 missed broadcast events; its final report was absent (`execution.json={}`).
MarketState produced 28,604 observations and GraphSignals completed 41,959
evaluations. The old synchronous all-candidate search blocked its own consumer.
No economic acceptance or zero-opportunity conclusion follows from that run.
The changes above address CPU isolation, indexed curve queries, incremental result
delivery, cooperative cancellation and retained partial evidence. Re-run the same
600-second command; compare event loss, job drain, proposal staleness, economic
processing times and actual fee-adjusted simulation results. Live acceptance of
this correction remains pending.
