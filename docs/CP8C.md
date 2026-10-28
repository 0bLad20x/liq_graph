# CP8C: quantity-compatible candidates for DLMM and PumpSwap

Base: main `e00e07f1c44a3b7fd634b8c7015d6bf9e9050cba`, accepted CP8B.
Branch: `cp8c-route-amount-compatibility`. One runtime and directed net index;
no extra observer, RPC-per-signal path, dependency, worker, database table or suite.

## Problem and scope

In the real CP8B run `20261028T172648362Z`, 239/383 requests had at least one
witness leg with one output atom in its observed DLMM direction. An example
offered one USDC atom while the next leg needed two. Local marginal net rates
could multiply to an attractive number without a positive closed integer flow.
A token atom by itself is not an economic exclusion criterion.

CP8C preserves the fast directed net index and all topologies. It adds typed
amount models and quantity analysis in the real ExecutionCandidates owner before
dispatch. Unknown observations remain unknown; no failed probe, fixed amount
grid, top-K, additional route budget or blacklist is an exclusion rule. V10 keeps
automatic sizing (`trade_size=0`). Positive estimates are not executed quantities,
an optimizer result, a profit guarantee or a new wallet/network-cost threshold.

## Model ownership

Core only carries facts. DLMM levels add exact raw output/input rational prices,
ordered output components, integer fee fractions and an upper bound on refundable
input fees. u128 ratios serialize as decimal strings to preserve precision. The
same level vector supports marginal rates, integer traversal and evidence; bins
whose net output rounds to zero are retained for traversal costs. Marginal
availability and ordinary JSON's first level still mean first positive net output.

MarketState privately decodes and constructs these models. PumpSwap publishes a
reserve curve in the same PoolLiquidity contract: base reserves, signed-virtual-
adjusted quote reserves, physically spendable quote and the three fee rates.
`protocol_fees` and `creator_fees` are read from the existing full pool account;
they reduce physical spendable quote, not the effective pricing reserve a second
time. Short append-only layouts read missing fee buckets as zero.

Both models carry the current manifest's stable account digest. Proof-slot renewal
alone keeps watches; an actual dependency change replaces the affected pool.
Pump reserve/fee changes update the material profile digest even at unchanged spot
and net rates, so affected candidates are republished by the existing pair index.
No fee model or unresolved token transfer cost is silently replaced by zero fees.

ExecutionCandidates owns its private generic level/reserve calculations and route
composition. It does not call MarketState implementations or fetch protocol data.
Its new private module is `workers/execution_candidates/amounts.rs`. Registry reads
for alternative eligibility/window matching reuse the existing measured interface.
All normal source/price/manifest continuity checks and independent executor request
validation remain. Nondefault fixed-size, constant-conversion and no-failure modes
are explicitly unknown to this filter rather than assigned unproven semantics.

## Exclusion proof and alternatives

The analysis builds directed pair domains from frozen observations. It enumerates
simple closed **mint cycles**, including shorter cycles, rather than a Cartesian
product of pool assignments. A typed request has at most one conversion pool;
two distinct S/Q conversions cannot form an independent two-hop route in that job.
All represented pool alternatives are considered in their respective domains.

Two necessary conditions provide conservative exclusions:

1. Every edge has an optimistic raw-rate upper bound. Reserve curves use their
   initial raw reserve ratio; ordered price levels use their maximum raw ratio.
   DLMM certifies that further bins only worsen the raw directional rate. Fees
   are omitted from this bound. Integer rational products are compared exactly,
   without floating-point rounding. A product <= 1 cannot produce a positive
   fresh-amount closed return. An edge is removed from a domain only if even
   combining it with all other domains' best bounds cannot beat one.
2. Required positive output is propagated backward through the remaining domains.
   Each step chooses the lowest optimistic input cost. Unknown options prevent a
   negative conclusion. Output fees may all be returned; input fees may refund
   their entire protocol portion (all fees for mixed/order liquidity). These
   assumptions overestimate opportunity and prevent host/cashback omission from
   creating a false veto. If no domain can supply the required integer amount,
   that frozen cycle has no positive closed flow. Missing liquidity beyond an
   execution window is unknown, not zero.

Monotone inverse minimization is factorized across distinct mint roles. A relaxed
choice that reuses a pool becomes unknown. It is not an exclusion proof. An
independent known rate upper bound can still establish no gain without knowing
every later bin's capacity. These are proofs about the recorded models and
prepared windows, not a future bank state or liquidation of pre-existing token
inventory. New price/clock/dependency observations are evaluated again.

First the actual packed request is analyzed. Before any veto, the worker checks
the wider currently priced/admitted Prepared pool universe on the candidate's
mints, including pool choices omitted by packing. Changed account digests turn
that model unknown. A request is suppressed only when every covered cycle in
this enlarged model universe is excluded. No topology is deleted from the graph
or active candidate state. A failed best witness alone cannot remove alternatives.

If alternatives keep a possibility open, the worker attempts a quantity-compatible
seed using the same mint roles and optimistic inverse costs. It must still pass
the directed net threshold, pool uniqueness, current proofs and V10 capacity.
Ordinary alternative packing then runs against that seed. Unresolved/unpacked
combinations remain unknown and retain the normal simulation path.

## Positive estimates and integer arithmetic

A possible result needs a concrete full-fee estimate with positive integer output
through each leg and a closed return above input plus the configured settlement
minimum_profit. Host rebates, exact transfer fees, execution fees, network fees,
account deposits and flashloan costs are not included in that DEX estimate.

Full-fee probes include the optimistic minimum, its representable doublings and
all representable observed level boundaries projected through preceding inverses.
Probing stops after finding a positive estimate. Failure to find one is unknown;
it never proves all trade sizes unprofitable. No amount is forced into the request.

Level fills use ceil input per ordered component, floor output, per-bin fees and
the current reached-bin dynamic fee. Exhausted windows/partial fills cannot make
a fully closed positive estimate. Reserve sells use effective quote reserves and
separate ceil-rounded LP/protocol/creator fees, plus the real-quote outflow limit.
Buys use the exact-quote-in net fee split and the SDK quote builder's conservative
one-atom padding. This deliberately omits rebates and is not a claim of exact
V10 instruction/amount selection. Nonmonotone multi-fee rounding in exact-quote-in
is never used to justify a binary-search or negative all-amount conclusion.

Protocol references: pinned Meteora SDK
`af36dd53ab180783ccecda306926189a4700953c`, `helpers/{bin,fee}.ts` and its IDL;
published `@pump-fun/pump-swap-sdk` 2.1.0 `sdk/{buy,sell,util}.ts`, and official
[PumpSwap documentation](https://github.com/pump-fun/pump-public-docs/blob/main/docs/PUMP_SWAP_README.md),
[negative virtual reserves](https://github.com/pump-fun/pump-public-docs/blob/main/docs/NEGATIVE_VIRTUAL_QUOTE_RESERVES.md)
and [trade V2](https://github.com/pump-fun/pump-public-docs/blob/main/docs/instructions/PUMP_SWAP_TRADE_V2.md).
The SDK source copy inspected is mirrored at
`nirholas/pump-swap-sdk@250861bbac73da3f9f58de8f03f9a86f96d5fe3f`;
direct npm/CDN access is unavailable here, so no npm tarball integrity verification
is claimed. Official program docs/IDL and actual captured fee events remain primary
references. Real quote/rounding comparison is part of live acceptance.

## Evidence and thirty-minute run

Every dispatched Selection contains `amount_assessment` (possible/unknown), a
positive estimate with per-leg raw amounts when found, `amount_seed_changed`, and
`packing_seed_log_sum_units`. The latter may differ from the original triggering
SignalCandidate after repair. Both must remain net qualified. The previous
linearized first-depth boundary and its leg counter are removed.

The owner report counts every assessed candidate, excluded/possible/unknown,
global proofs, seed repairs, probe work, reasons and quantity-stage wall times.
Four bounded exclusion examples preserve complete typed models for independent
inspection. This sample limit affects evidence retention only. No per-exclusion
terminal logging or extra stateful diagnostic consumer is added.
Request-ready/frame-to-request timestamps now include the entire quantity stage
and repair. Registry read metrics include the wider proof universe. The stage's
histogram is wall time, including these reads, not isolated CPU instruction time.
Busy-revision handling is unchanged; no pending queue/deduplicator is introduced.

Exact-head locked release build and starter syntax checks precede handoff. Real
acceptance is pending: this implementation environment has no Rust/PowerShell,
configured project DB/RPC or wallet and cannot run the requested live simulation.
Use the existing configured Windows checkout, without broadcasts:

```powershell
cd C:\dev_projects\liq_graph
git fetch origin
if ($LASTEXITCODE -ne 0) { throw 'Fetch fehlgeschlagen.' }
git switch --track origin/cp8c-route-amount-compatibility
if ($LASTEXITCODE -ne 0) { throw 'Branchwechsel fehlgeschlagen.' }
.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp8c-route-amount-compatibility `
    -EnabledVenues 'meteora_dlmm,pumpswap' `
    -Stage Signals -DurationSeconds 1800 `
    -PoolLimit 0 -AccountBudget 25000 `
    -ProfileSignals -ThresholdBps 0 `
    -RequireFeeEvidence -RequireLiquidityEvidence -RequireAmountEvidence `
    -LiquidityEvidencePools '3w6Bk1xj4wAVG7ZPHRgpT5SW77hB2ofs3iYYvE5gezLN,2D6YWhkKLVUaJMhmyYQUS5z54ZwpqcxE9SmGJTXJmq2G' `
    -SimulateV10 -SimulationLimit 0 `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

If the local feature branch already exists, switch to it and use a guarded
`git pull --ff-only origin cp8c-route-amount-compatibility`. The focus only records
raw evidence; it does not pin pool selection. A missing pool is reported honestly.
No forced Rewatch or graph audit adds load to this quantity/timing measurement.

`-RequireAmountEvidence` additionally requires a real exclusion, a positive amount
estimate, matched amount decision/request accounting, actual DLMM array WS input,
Pump reserve curves consumed by the Signal Engine, and valid closed positive
estimate legs belonging to the dispatched pool set. No live occurrence is
fabricated if the market does not supply one. Existing handoff/net/fee/window
checks remain separate. Success and profit are separate from model acceptance.

Inspect `amount_evidence.json`, `execution.json`, `selection-NN.json`,
`simulation-NN.json`, `liquidity_evidence.json`, `market_state.json`,
`signal_production.json` and `summary.json`. Compare real exclusions against the
complete saved models, especially one-USDC-atom -> two-required cases, full-fee
rounding, shorter paths and alternatives. Verify unknown boundaries remain unknown,
positive estimates never reference unpacked pools, and future state changes restore
eligibility. Compare quantities and chosen pools to actual simulation events.
Account for the 31.3% ready-pool coverage and transport gaps in the CP8B baseline;
different duration/load does not establish a speed or profitability improvement.
Hydration/recovery scheduling remains a separate checkpoint.
