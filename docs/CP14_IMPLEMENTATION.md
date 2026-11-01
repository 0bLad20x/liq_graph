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
