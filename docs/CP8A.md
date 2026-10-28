# CP8A: directed fee assessments for DLMM and PumpSwap

Base: main 1c7559bab88e9a95358f23d242753c4b03608053. One runtime,
one Graph/CompactIndex and one V10 simulation path. No broadcasts.

## Contract and selection

MarketState publishes directed marginal DEX-net rates for DLMM and PumpSwap.
`estimated_net_rates` are canonical **base -> quote / quote -> base**, in human
token units. Raw pool prices remain protocol inputs and evidence, not a second
signal-selection score. Amount, depth, host rebates, exact transfer-fee effects,
network fees and account deposits are outside this estimate. Token-2022 costs
that need an amount remain explicitly labelled; known DEX-net rates may still
be used with that limitation.

CP8A.2 replaces the gross admission/selection path completely. The existing
CompactIndex orders independent directed net weights, finds the best net witness
per topology/direction, and applies `GRAPH_SIGNALS_THRESHOLD_BPS` to that net
return. Default zero means **strictly positive estimated DEX-net return**. This
is an intentional admission change: old gross-positive/net-negative requests
are no longer admitted through that witness. It is not a network-cost or final
wallet-profit threshold. Q48 rounding remains part of the contract.

`SignalPoolPrice.log_weight_units`, leg weights and witness `log_sum_units` now
all mean marginal DEX-net weights. `SignalPlan.score_basis` and both owner reports
identify `marginal_dex_net`; consumers reject a mismatched snapshot. The duplicate
net weight/log-sum fields, gross-return aliases and gross fallback are removed.
Unknown/nonpositive/nonfinite net rates leave that direction unavailable. A
missing fee model does not mean zero fees, and does not remove graph topologies.
Other venues need their own fee model before they can contribute net weights.

ExecutionCandidates ranks those net witnesses and tests/ranks parallel pool
alternatives with the same net weights and threshold. Existing V10/account limits,
all route kinds, pool alternatives and V10 automatic sizing remain. Fee-only
changes update the ordered net index. Relevant fee changes on parallel alternatives
still republish affected candidates even when the best witness stays unchanged.
Catalog reconciliation updates current membership without requesting execution.
Clock/mint revaluation can request execution; baseline and initialization replay
cannot. There is one index and one runtime, no legacy switch or comparison engine.

## Protocol references and rules

DLMM: MeteoraAg/dlmm-sdk `idls/dlmm.json`,
`commons/src/extensions/lb_pair.rs`, `ts-client/src/dlmm/helpers/fee.ts` and
`helpers/lbPair.ts`. LbPair is the verified 904-byte layout. MarketState now
watches the full pool account. Static parameters start at 8, variable parameters
at 40; active_id/bin_step remain 76/80. Fee references are recomputed using the
chain Clock's timestamp, not the host wall clock. Total base + variable rate is
capped at 100,000,000 / 1,000,000,000 (10%). Protocol share is a split, not an
extra fee. InputOnly and OnlyY collection modes are recorded by direction.
The marginal estimate deducts the total trading fee, conservatively omitting
any host rebate. It does not predict the fee evolution across multiple bins.

PumpSwap: pump-fun/pump-public-docs `idl/pump_amm.json`, `idl/pump_fees.json`,
and the official published @pump-fun/pump-swap-sdk 2.1.0
`src/sdk/{fees,buy,sell,util}.ts`:
https://app.unpkg.com/@pump-fun/pump-swap-sdk@2.1.0/files/src/sdk/fees.ts

Canonical classification uses the pool PDA creator, not coin_creator. The fee
config's SOL-like tiers, stable tiers (legacy fallback), noncanonical flat fees,
and exotic flat fees (zero/unset fallback) are decoded from chain. Mayhem cap
uses the SDK's fixed 1,000,000,000,000,000 supply; ordinary cap uses the watched
base Mint supply. Cap uses effective quote reserves, with checked integer math.
A gated nonzero pool creator override replaces only the creator rate; absent
coin_creator zeroes it. Buyback/holder/cashback routing does not add another copy
of the same creator/protocol charge. Buy/sell disable flags are reflected in
rate availability.

For native base/quote gross rate p and fee fraction f:
- Sell base -> quote: p * (1-f).
- Buy quote -> base: (1/p) / (1+f).

Effective quote = signed vault amount + virtual quote reserves. Fee buckets
must not be subtracted a second time. The default/short append-only pool layout
reads absent trailing creator fields as zero. Missing or malformed mandatory
fee accounts fail initialization or withdraw availability; no zero-fee fallback.

Shared GlobalConfig/FeeConfig/Clock watches are deduplicated. Shared fee tables
are parsed once per account observation and Arc-shared across pool bindings.
Pump costs four per-pool accounts conservatively (pool, two vaults, base Mint)
plus shared accounts; DLMM one full pool plus shared Clock. Existing subscription
continuity/baseline guards still apply. Clock writes only emit a valuation when
a fee boundary/epoch or unknown-to-known assessment changes; they do not emit
one price per pool per slot. Mint facts remain owned by the central Mint Registry.

## Evidence and live acceptance

`market_state.json.metrics.fee_valuation` contains per-model observed/known fee
counts, unknown reasons, change fields, revaluation counts, asymmetric-rate
counts and fee calculation timing. At most eight samples per protocol include
the observation and the exact decoded fee inputs for independent comparison
against the pinned official SDK. These are bounded evidence samples, not a cap
on live calculations. Existing request artifacts preserve each selected leg's
assessment, provenance, component slots and indicative price.

`-RequireFeeEvidence` requires observed known DEX fees for **both** models and
non-reciprocal rates. This check is necessary, not sufficient: inspect the real
samples/formulas, unknown states, fee-only changes, delivery and latency tails.
A run that never observes a particular configuration change cannot prove that
change path live. Simulation handoff, successful swaps and profit are separate.

The uploaded successful V9 transaction 2VZZJEZpk6juj8ZxZ7mAb2TKMYaneJTHq9u7q6JsPMF9rLgDCmBtyibWJmgp6NSPESKQYkqEoh4GiFNvdXXSnPci
is reference evidence, not a replay fixture. Its Pump sell charged 20 + 5 + 18
bps; buyback was part of protocol fee. Its positive virtual reserves materially
changed the price. Its DLMM host fee was credited to the wallet. Historical
DLMM pre-state is absent, so no full historical signal reproduction is claimed.

After the locked feature build, run the requested single
1800-second measurement in the configured Windows environment (no transaction broadcast):

```powershell
git switch cp8a-directed-fee-signals
if ($LASTEXITCODE -ne 0) { throw 'Branchwechsel fehlgeschlagen.' }
git pull --ff-only origin cp8a-directed-fee-signals
if ($LASTEXITCODE -ne 0) { throw 'Synchronisierung fehlgeschlagen.' }
.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp8a-directed-fee-signals `
    -EnabledVenues 'meteora_dlmm,pumpswap' `
    -Stage Signals `
    -DurationSeconds 1800 `
    -PoolLimit 0 `
    -AccountBudget 25000 `
    -ProfileSignals `
    -ThresholdBps 0 `
    -RequireFeeEvidence `
    -SimulateV10 `
    -SimulationLimit 0 `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

Live acceptance is pending until real input/output artifacts have been evaluated.
This implementation environment has no Rust/PowerShell, DB/RPC configuration or
wallet; GitHub CI supplies the locked build, not the live proof. No new automated
unit/mock/regression suites or historical checkpoint scripts are introduced.

## CP8A.1: Registry maintenance and request read timing

Continues on `cp8a-directed-fee-signals`, based on 2af5e94. The two real
600-second runs 20261028T080230472Z and 20261028T084147169Z used that exact head.
The second eliminated the first run's native-SOL account funding failures and
recorded 28 successful simulations, 18 involving both DLMM and PumpSwap.
All 32 emitted PumpSwap swap events matched the recorded LP/protocol/creator
fee rates. Successful requests are not necessarily economically profitable.

The second run's three largest request builds took 290.6, 292.8 and 208.5 ms
at approximately 311, 371 and 431 seconds after process start. Two considered
zero alternatives. These are steady-running stalls, not startup/shutdown samples.
They coincided with minute maintenance; the old reports did not measure Registry
lock waits and therefore do not prove the exact contribution of each cause.

Registry remains the only writer. The minute task is awaited on the blocking
pool before the owner accepts another source/completion/qualification update.
The reference-pair diagnostic scan also runs there. TVL preparation uses a read
lock on entries, so execution readers can proceed during the full quote scan and
cleanup. Only changed TVL/provenance values are installed, in write sections of
at most 64 pools, with a scheduler yield between sections. This is installation
granularity, not a pool/route limit. All patches are applied before publication.
No alternate Registry, legacy mode, new worker, crate or dependency is introduced.

TTL, source precedence, persistence coalescing, historical observations and
native-TVL admission rules are preserved. Canonical TVL expiry does not withdraw
native admission. MarketState can still use canonical TVL when budgets bind.
Execution manifests and mint proofs are untouched by TVL installation.

Measurements:
- One `pool registry tvl_refresh={...}` log per maintenance cycle: scan count,
  changed/persisted pools, preparation time, lock wait, total/max write hold and
  actual maximum batch size. `scan_ns` on the existing reference-pair log records
  that separate diagnostic scan. No per-pool maintenance logs are added.
- Each selection stores `registry_reads`: call count, total read-lock wait/hold,
  and maximum individual wait/hold in ns. This includes seed attempts and
  alternatives, including failed lookups. The aggregate is in
  `execution.json.execution_candidates.registry_reads`. Ordinary untimed Registry
  consumers continue using `get()`.
- `selection_ns` remains total wall time, including Registry access. A large
  selection time with small measured Registry wait must not be labelled a lock
  stall without further evidence; scheduling/other work can still contribute.
- `summary.json` includes `registry_maintenance`, `execution_registry_reads`,
  `registry_timing_observed` and `execution_skipped_busy_revisions`. The timing flag
  means at least two maintenance cycles and actual request reads were measured;
  it does not assert absence of contention or replace functional acceptance.

Run the command above with `-DurationSeconds 600`, retaining the same venues,
budgets, wallet, automatic sizing, minimum profit and flashloan configuration.
Inspect several maintenance cycles, actual read waits, request tails, coverage,
delivery and simulation results. `changed=0`/`write_batches=0` correctly means a
scan needed no installation; that run cannot prove mass-expiry installation.
Large expiry batches require real observations of such expiry, not synthetic
inputs or a claim inferred from the configured duration. Other source/completion
write sections are not refactored by this checkpoint; measured waits include them.
The real 600-second run `20261028T095251967Z` on `f560b01` verified concurrent
execution reads during long scans: 11 maintenance cycles, up to 413.157 ms
preparation, 10,040 measured reads and at most 16.918 ms individual read wait.
Three requests overlapped scans with 0.6--2.2 us total read waits. All cycles
had zero changes/write batches, so the changed-patch install remains unproven.
The 802 requests and reports matched, with 23 successful simulations. CP8A.2
changes net selection and requires its own live evidence.

### Deferred candidate/swap analysis

Dispatch is unchanged: a full output channel skips a revision with no retry,
and at most one packable candidate is sent per revision. `skipped_busy_revisions`
counts revisions, not unique pools, executable swaps or missed profitable trades.
No pending queue, new threshold or duplicate-opportunity filter is installed.
Accepted requests and failed simulations remain individually available in the
existing request/selection/simulation artifacts. Busy revision identities are
not retained by the existing counter, so those cannot be reconstructed later.

The later analysis should compare actual pool/direction/route and time, both
negative-estimate successful requests (graph-32-47837-315 and graph-35-51873-384),
three-swap successes, slippage failures and repeated pool sets. Equality of pool
sets alone does not prove equal opportunities: direction, state revision and
available liquidity matter. Complete historical DLMM BinArray pre-states are
absent from the current evidence. Request route labels also do not prove the
executed hop count: successful Direct3/Triangle4 requests in the second run
executed two swaps. Real four-swap execution remains unproven.


## CP8A.2: net-only selection and duration telemetry

Scope is the current DLMM/PumpSwap branch, building on `f560b01`. Registry
maintenance, Discovery, Completion, GraphLiquidity and the executor retain their
existing responsibilities. No new worker, observer, script, crate or dependency.
The existing starter command above runs 1800 seconds with unlimited total request
count, no pool-count cap, the same account budget and no broadcasts.

The implementation must pass the exact-head locked build before handoff. Real
1800-second acceptance remains pending until that run's artifacts are inspected.
No successful simulations or improved speed are promised by the net threshold;
our previous successes sometimes used another supplied pool or a different
number of hops. An estimated DEX-net edge is not a quantity-dependent quote.

The existing owner histograms still separate ordinary WebSocket processing from
initialization. New `revaluation_inputs` separates fee/clock-only processing.
`frame_to_evaluation_complete_us` includes state/index work, candidate queries
and state/candidate publication up to evaluation completion; it starts at local
socket-frame receipt, not validator execution. Signal histograms report percentile
upper bounds, not exact percentiles. `logarithms_prepared` now counts actual
available directed net logarithms, rather than one gross logarithm per price.
The preparation timer includes net-rate availability counting.

`candidate_windows` is bounded telemetry within the actual active-candidate map:
- One window is continuous net qualification for one topology and direction.
  Changing the winning pool/weight while remaining qualified does not restart it.
- Catalog remapping preserves the original monotonic entry time. Removed shapes
  end in `catalog_exit_ns`; source/selection unavailability ends in
  `unavailable_exit_ns`; stream gaps end in `gap_exit_ns`.
- `threshold_exit_ns` records a complete priced evaluation falling below the
  configured net advantage. These are the observed threshold-window durations.
- Windows still open at shutdown go to `right_censored_at_end_ns`, separately
  from economic exits. A typed MarketState removal reason identifies shutdown
  before per-pool withdrawal. Abnormal stream completion has a separate
  `right_censored_at_error_ns`. All durations use monotonic nanoseconds.
- Entry and all exit/censor counts must balance. `peak_active_candidates` is
  recorded before shutdown. The windows describe local estimate validity,
  not guaranteed executable opportunity lifetime or the lifetime of a frozen job.
  Baseline/catalog-created memberships can be included without dispatching them.

`summary.json` exposes the score basis, net threshold, known-direction counts,
net witness violations, all window histograms, steady-state preparation/index/
query/publication/processing timings, fee-revaluation timing, frame-to-state and
frame-to-completion timings, and exact post-run request selection/frame/request
sample statistics. Existing Registry wait and simulation results remain separate.
No per-pool duration log or second price/candidate state is created.
MarketState's old unchanged-gross counter is replaced by
`unchanged_net_rate_updates`; this measures rate equality, not deduplication.

Compare the new run to the `f560b01` 600-second baseline by event type and workload:
62,475 steady-state samples, mean processing 49.189 us, request selection median
0.29865 ms/P95 5.0457 ms/max 45.1736 ms, frame-to-request median 0.5695 ms/max
49.242 ms. Account for changed event mix, queries and candidate counts before
attributing changes to code. Duration telemetry has no comparable historical
window baseline. A longer run alone does not establish statistical significance.

The live review must check net-only pool ordering, both directions, fee-only
updates, unavailable-rate withdrawal/recovery, candidate/window accounting,
complete delivery, actual accepted request/report IDs and timing tails. Inspect
raw fee samples and selected net witnesses; `net_evidence_passed` is a necessary
structural check, not a proof of exact math, liquidity depth, all configuration
changes or profitability. Busy retry/coalescing and directed liquidity remain
separate work; skipped revision identities are still not retained.
