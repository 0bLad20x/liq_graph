# CP14 Orca Whirlpool Direct2 — implementation status

Branch: `cp14-orca-whirlpool-direct2`. Base main `23fba2e` after CP13. Scope: four venues, Direct2, simulation only.

## Implemented code

- `src/workers/market_state/orca.rs`: Whirlpool account parser; original Orca Q64.64 price, active liquidity, static swap fee and optional adaptive Oracle fee. Reference decay, volatility accumulator, group size, hard maximum 10% fee, trade-enable time and owner/identity checks. Fees are per million; protocol share is not double charged.
- `src/workers/market_state.rs`: Full Whirlpool account WS watch, optional adaptive Oracle and chain Clock, Prepared TickArray full-account watch; fixed/dynamic TickArray initialized tick extraction. Typed per-direction `FeeValuation`, `DirectedLiquidity`, `PoolLiquidity` and frozen digest. Static pools do **not** subscribe to chain Clock. Any unsupported trade or lost config yields no fee-net rate.
- Orca tick addresses obtained from the current Prepared V10 manifests and costed in `MarketState` subscription budget. Distinct watched account addresses deduplicated. Tick-array layouts checked against owner, pool and spacing; rewatch on Prepared manifest changes via account-set digest.
- Directional minimum-liquidity support is bounded to observed active liquidity until the next relevant initialized tick and, for adaptive pools, the next fee group. Underfilled or uncovered bounds are `Unknown`, not false `Dust`. No Orca-specific graph or signal rules.
- `src/workers/execution_candidates/amounts.rs`: post-CP13 Orca signal-only profile is `Unknown` to the pre-V10 exact quantity evaluator, preventing a false exclusion.
- `src/workers/execution_candidates.rs`: frozen-amount Orca diagnostic output estimate at the **actual V10-selected gross input** with protected MintProof fee classification, exact account-set manifest digest and observed slot/time metadata.
- `src/core/market_state.rs`: compact, frozen Orca price/liquidity/fee/tick data for calibration. `orca_quote` is optional for all earlier DEXes.
- `scripts/cp14_orca_live.ps1`: reproducible 60/600-second local simulation for four venues; `scripts/cp14_calibration_check.py` writes actual-quote comparisons and reasons for unquoted legs; no transmissions.
- Targeted CP14 unit tests for Whirlpool layout, Oracle, dynamic/fixed TickArrays, adaptive fee/time policy, bounded fixed-input quotes, V10 writable Oracle and direction order. Existing CP10–CP13 tests unchanged.

## Important precise boundary

The Orca post-V10 quote is **diagnostic and floating-point approximate**, not an exact byte-compatible `orca_whirlpools_core` or Solana-program integer quote. Crossed tick liquidity is supported if observed in the frozen window. Adaptive trades crossing a fee-group boundary return `orca_adaptive_fee_group_crossing_unmodeled` and unseen array tails return `orca_frozen_tick_window_uncovered`. These gaps are reported in the calibration, never silently treated as accurate model outputs.

The existing prepared Completion worker requires first TickArrays in **both** directions; it may exclude some one-direction-only executable pools. A future direction-aware admission policy needs a separate quantified economic case before changing the common Prepared manifest contract.

The implementation therefore closes the **fourth-venue signal pipeline** but is NOT a claim of 100%-exact Orca program simulation or true economic profitability.

## Running local CP14 evidence

```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
git switch --track origin/cp14-orca-whirlpool-direct2
git pull --ff-only origin cp14-orca-whirlpool-direct2
.\scripts\cp14_orca_live.ps1 -DurationSeconds 60
.\scripts\cp14_orca_live.ps1 -DurationSeconds 600
```

If already local, use `git switch cp14-orca-whirlpool-direct2`. The runner first performs `cargo test --locked --release cp14_`. The local account providers, registry, Windows test wallet and .env are required. No live run can be performed in GitHub CI.

The generated `cp14_orca_calibration.json` must show `orca_observed_complete_legs>0` and `orca_quoted_legs>0` to prove the quantity comparison. Every `orca_missing_model` entry must be justified (adaptive fee-group boundary, frozen window uncovered, unresolved token fee or manifest change). A 600-second run with no Orca execution is **incomplete evidence**, not success.

Assess additional Orca Prepared/selected/priced pools, new Direct2 pairs/options and actual V10 choice; zero lost WS/signal events; P50/P95/P99 frame→V10; RPC 429/timeout; wallet/settlement cashflow, protocol fee and actual net viability.

Do not merge CP14 until locked CI plus user-provided 600-second live artifacts have been audited. Even after technical closure, floating-point quote calibration is not an exact program clone.

## CP14 closeout: three targeted corrections (November 1, 2026)

### 1. Adaptive Clock/Oracle consistency

Orca PoolState now retains its own update slot alongside its initialized Oracle and chain Clock slots. The net marginal fee and directional profile for adaptive pools are withheld until the chain Clock slot is at least as recent as **both** the pool and Oracle slots, with a positive chain timestamp. No manufactured clock timestamp and no static-fee substitution. Clock WS updates trigger recalculation, and a deterministic test verifies older, equal/newer, missing-clock and static-pool cases. A slot being newer is **not proof of the same Solana bank**; model/trace output still labels this limitation.

### 2. Direction-aware Prepared TickArray admission

`completion/orca_whirlpool.rs` previously required the first TickArray in both A->B and B->A directions. It now requires **at least one valid direction**; the opposite manifest remains a seven-account, **explicitly unusable** block with no invented TickArray. Completion evidence schema version 4 records `direction_a_to_b_ready` and `direction_b_to_a_ready`. Both the full and the light refresh paths share the same consecutive-arrays calculation and verify the direction flags.

`PoolInput` / MarketState carries the manifest readiness in `orca_ready`. An unsupported direction receives no marginal net rate or available directional liquidity and cannot originate an economic signal; the shared Graph and Signal Engine remain unchanged. `execution_v10::block()` additionally **rejects a seven-account Orca block** even if an upstream caller were to bypass the directed-price gate. Regression tests cover A-only, B-only, neither and V10's selected side.

### 3. Independent integer-reference amount differential

`scripts/cp14_orca_reference.py` is a separate pure-Python integer implementation of Orca's *single active interval* base-input fee / Q64.64 sqrt / input-output rounding from `programs/whirlpool/src/math/{swap_math,token_math}.rs`. It reads the bounded detailed live evidence, joins matching compact calibration records by payload hash, and compares the independent output with the existing Rust approximate output and V10's observed output. It reports exact matches, P95 absolute output deviation, per-leg raw output and explicit refusal reasons. CI checks a pinned live fixture (Orca 8EyLm..., 191 raw input => 45104 output).

This is **not** a full `orca_whirlpools_core` multi-tick U256 clone. Crossing the active boundary or adaptive fee-group boundary remains Unknown to this independent reference. The Rust post-selection model still supports bounded multi-tick approximate diagnostics. No economic route exclusion uses either post-selection quote.

### Five-minute acceptance

```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
git switch cp14-orca-whirlpool-direct2
git pull --ff-only origin cp14-orca-whirlpool-direct2
.\scripts\cp14_orca_live.ps1 -DurationSeconds 300
```

The wrapper runs the `cp14_` regression tests and the unchanged four-venue 300-second V10 simulation, then writes `cp14_orca_calibration.json` and `cp14_orca_reference.json` to the new runtime directory. It checks for admitted and actually valued Orca pools and Orca complete V10 swap legs.

Compare against CP14 600-second baseline `20261101T070830054Z`:
- **Adaptive time:** old `Orca Oracle from future slot/time` count=17; new count and `awaits matching Clock slot` occurrences, recovery after clock advance, and no invalid directional fee rates
- **One-direction pools:** old first TickArray missing cases=86 of 704 completion proofs (both sides combined); new Prepared, A-only/B-only admission and orphan execution errors
- **Independent quote:** number of calculable single-interval details, model vs integer and observed vs integer errors; unexplained deviations are not proof of profit
- **Health:** 0 lost market/signal events, ws_gap count and recovery, no unexpected V10 Orca account failure, end-to-end P50/P95/P99 and output trace match

A provider disconnect is not fixed by any of these three protocol-code patches; compare the gap count but do not call a provider gap a regression in Orca arithmetic. Five-minute samples are not time-normalized with the 600-second baseline. Do not merge until the live artifacts have been analyzed and the user approves.
