# CP9C: Direct2 alternative pool diversity + frozen quote/state audit

**Base:** CP9B `cp9b-direct2-parallel-v10`. Simulation only, never broadcast. Keep the SOL/USDC and other token pairs in the same qualified universe; no SOL/USDC-specific selection.

## A. Same-revision counterfactual V10 pool packages

Graph and signal source remain Direct2-only. The price-based best witness still triggers a revision, but its marginal rate does **not** veto individual non-seed alternatives under CP9C alternative modes.

Environment switch `GRAPH_CP9C_PACKING_MODE` (PowerShell `-CP9CPackingMode`):

- `Baseline`: CP9B behavior. Rank individually positive marginal substitutions, leave the original seed/pool group unchanged.
- `Contrarian`: for every enabled direction, walk the entire known marginal-price distribution by alternating its extrema, then median and quartiles. Do not require a single alternative to remain positive relative to the original seed.
- `Mixed`: round-robin across marginal favorites, per-direction input capacity, different venue labels, and extrema/quantiles of marginal rates. Capacity values are compared **only among alternatives with the same directed input token**, not across SOL and USDC atoms.
- `All`: independently build up to three alternative packages for the **same** signal revision (baseline, contrarian, mixed). Rotate the first strategy on successive revisions so a busy V10 queue does not systematically favor the baseline. Do not queue the same sorted pool set twice in a revision.

For every package, preserve the original seed witness (or an existing amount-seed repair), the **exact** V10 validator/capacity boundary (unique accounts, bytes), Prepared Manifest, current observed pool price and liquidity profile, and the existing conservative quantity proof (never discard `Unknown` simply because no positive amount was found).

Only distinct, actually packable pool sets are dispatched. There is no RPC within the selector. Slots are immutable per selection; request IDs still join all simulation results, even when completion is out-of-order. The 16 parallel V10 workers and shared multi-provider RPC dispatcher are unchanged.

The existing selection of one **source candidate** per revision still applies. This checkpoint adds breadth *within* that candidate's token pair, rather than inventing other signal routes.

### Key limitations

- The score-based first witness is still mandatory to trigger the revision. CP9C does not scan a token pair without an initial marginal opportunity.
- A pool with an uncovered direction cannot become a provably executable quote merely by being selected as an alternative.
- Contrarian means a **different directed marginal price**, not necessarily a profitable tradable finite size. Mixed uses directional input-capacity where known, not an USD normalization of unpriced tokens.
- There can be fewer than three distinct valid packages; hard account limits and amount proofs remain in force.
- A `full` queue still skips immediately. `cp9c_queue_full_packs` counts individual skipped packages and `skipped_busy_revisions` counts affected revisions. No price propagation stalls.
- The three packages consume additional RPC calls. Compare per-revision costs and returns, not simply raw success counts.

## B. Quote-vs-simulation discrepancy audit

The per-request `execution_events.jsonl` `calibration` now adds `cp9c_audit_schema=1`, `packing_strategy`, `simulation_provider`, `simulation_context_slot`. Per DEX call it records:

- Its exact observed input/output and frozen quote at the **same observed input**;
- Pool venue and identifier, direction, slot, component min/max slots, component receipt timestamps, and consistency class;
- Frozen `liquidity.content_digest` and `account_set_digest` and reserve-curve parameters, when present;
- The selected directed level summary, directional input/output capacities and model's predicted output error in bps.

Do not infer state equality from identical Solana slots. `minContextSlot` only bounds the minimum RPC context. A different account digest or mixed-slot observation is not proof that another bot removed the profit.

Analysis script `scripts/execution_analyze.py` creates `cp9c_strategy_comparison` in `execution_analysis.json`: requests, RPC success/failure, complete cycles, above-network WSOL outcomes, immediate positive wallet cashflow, distinct executed pool paths and positive path+slot combinations, per-revision packing overlap, and the largest quote discrepancies with frozen reserves.

For economic success, require `err=null`, a complete continuous closed DEX cycle, and the appropriate observable settlement and native-wallet delta. Recoverable rent capital is not automatically a permanent cost. Repeated simulations of the same path and slot are **not** distinct trades.

## Local PowerShell: preflight and 30-minute live comparison

```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
git switch cp9c-direct2-diversity-quote-audit
git pull --ff-only origin cp9c-direct2-diversity-quote-audit
cargo test --locked
if ($LASTEXITCODE -ne 0) { throw 'Rust tests failed' }

$previousCommitment=$env:RPC_COMMITMENT
try {
    $env:RPC_COMMITMENT='confirmed'
    .\scripts\runtime_live.ps1 `
        -ExpectedBranch cp9c-direct2-diversity-quote-audit `
        -EnabledVenues 'meteora_dlmm,pumpswap' `
        -Stage Signals -DurationSeconds 1800 `
        -PoolLimit 0 -AccountBudget 25000 -MinPoolTvlUsd 500 `
        -MinDirectionNotionalUsd 50 -DirectionLiquidityMode Filter `
        -Direct2Only -RouteKind Direct2 -V10Parallelism 16 `
        -CP9CPackingMode All -ProfileSignals `
        -ExecutionDetailLimit 64 -RequireCalibrationEvidence `
        -SimulateV10 -SimulationLimit 0 `
        -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
    if ($LASTEXITCODE -ne 0) {throw 'CP9C live run failed'}
} finally {
    $env:RPC_COMMITMENT=$previousCommitment
}
```

**Acceptance:** `cargo test --locked` and release build; checkout has correct branch/commit; `plan.json`/checkout show Direct2-only and All mode; `execution.json` proves completed 16-worker simulation with no transaction broadcasts; `execution_analysis.json` has `cp9c_strategy_comparison`, `violation_counts` all zero. Review `cp9c_by_strategy`, same-revision overlap, `positive_path_slot_combinations` and quote discrepancies. Count `Custom(1)` as failure, not as successful execution.

**Comparison note:** Shared market windows are much more informative than sequential independent hours; rates of positive *unique* path-slot outcomes matter more than counts of repeated profitable `simulateTransaction` requests.

No automatic merge to CP9B or main without live evidence.
