# CP9D – Direct2 signal breadth and complementary pool coverage

**Parent:** `cp9c-direct2-diversity-quote-audit`. Draft experiment; no real transactions, no merge.

## The isolated hypotheses

A. **Signal breadth**: CP9C selected at most one source candidate per graph price revision. CP9D optionally selects up to `GRAPH_CP9D_MAX_DISTINCT_PAIRS` distinct *physical* (unordered) settlement/target mint pairs. A newly changed, priced positive witness is preferred; additional pairs come from the signal producer's **authoritative active candidate membership**, with a deterministic rotating start. Every active witness is revalidated against current per-direction marginal weights and original threshold. Active snapshots are never treated as new "Entered" signals. This does not create arbitrary mints or invent new profitable witnesses. On live price candidate revisions only; no execution for catalog bootstrap or initialization replay. The active scan is capped at 512 candidates per revision (performance bound).

B. **Complementary pool coverage**: preserves CP9C baseline, contrarian, and mixed. New `Coverage` ranks candidates by how many additional ordered, directed pool pairs would be offered between that candidate and the original two seed pools, relative to pool sets already packed for that same token pair in the current revision. It does **not** claim those directed pairs are amount-profitable or fully executable: exact Prepared and V10-transaction capacity checks are still authoritative. Coverage executes after comparison strategies, not before them.

C. **Fairness and concurrency**: dispatch one pack for each chosen token pair before dispatching the next strategy for any pair. Existing three strategies rotate by revision to avoid an always-preferred reference; Coverage is appended. All attempts use the same nonblocking V10 request queue, which may still drop packages at capacity; record drops by strategy. Configured concurrency may be raised from 16 to **32** (existing legal range 1..64), with existing per-provider RPS/in-flight limits unchanged. Nothing waits on V10 from the signal worker.

## Environment / local runner

- `GRAPH_CP9D_MAX_DISTINCT_PAIRS`: integer 1–8, default 1 for CP9C compatibility; `-CP9DMaxPairs` in `scripts/runtime_live.ps1`.
- `GRAPH_CP9C_PACKING_MODE=all_coverage`: baseline, contrarian, mixed, coverage. Also accepts `coverage` independently. PowerShell `-CP9CPackingMode AllCoverage`.
- `-V10Parallelism 32`, as previous CP9B executor already supports this; actual provider throughput must be measured, not inferred.
- Existing `minimum_profit=0`, `trade_size=0`, `use_flashloan=true` remain unchanged. No extra financial veto or new transaction fees. The post-simulation analyzer reports hypothetical WSOL settlement thresholds of 0, 5,000, 10,000, 25,000 lamports, **without changing the on-chain parameter**.
- All existing MarketState/RPC, 50 USD directed notional, 500 USD pool TVL, prepared manifests, direction gate, account and transaction byte limits remain unchanged.

The source selector *still requires at least one fresh qualifying changed Direct2 candidate* to cause a new round; it does not flood V10 on unrelated inactive pool price frames. Active-source candidates preserve their last positive proof under a new market revision and their directed weights must still exactly agree with current SignalState. They may have older observations for pools that did not change. A matching Solana slot still is not proof of atomic account-state equality.

## One five-minute local comparison (300 seconds)

```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
git switch cp9d-signal-breadth-pool-coverage
if ($LASTEXITCODE -ne 0) {throw "Git switch failed"}
git pull --ff-only origin cp9d-signal-breadth-pool-coverage
if ($LASTEXITCODE -ne 0) {throw "Git pull failed"}
cargo test --locked
if ($LASTEXITCODE -ne 0) {throw "Rust tests failed"}
$previousCommitment=$env:RPC_COMMITMENT
try {
    $env:RPC_COMMITMENT='confirmed'
    .\scripts\runtime_live.ps1 `
        -ExpectedBranch cp9d-signal-breadth-pool-coverage `
        -EnabledVenues 'meteora_dlmm,pumpswap' `
        -Stage Signals `
        -DurationSeconds 300 `
        -PoolLimit 0 `
        -AccountBudget 25000 `
        -MinPoolTvlUsd 500 `
        -MinDirectionNotionalUsd 50 `
        -DirectionLiquidityMode Filter `
        -Direct2Only -RouteKind Direct2 `
        -V10Parallelism 32 `
        -CP9DMaxPairs 3 `
        -CP9CPackingMode AllCoverage `
        -ProfileSignals `
        -ExecutionDetailLimit 64 `
        -SimulateV10 -SimulationLimit 0 `
        -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
    if ($LASTEXITCODE -ne 0) {throw "CP9D runtime failed"}
} finally {
    $env:RPC_COMMITMENT=$previousCommitment
}
```

The script itself handles `cargo build --locked --release`, exact checkout/dirty-tree integrity, full evidence generation, run summary and ZIP packaging. Use its final ZIP for comparison.

## Reanalyze an already completed run after an analyzer-only failure

The compact `selection.request` intentionally omits the full V10 `groups`
manifests; CP9D token pair reconstruction uses the frozen
`selection.candidate.witness.legs`. A failure of the post-run Python
analysis does **not** imply that the 300-second V10 simulation failed.

```powershell
Set-Location C:\\dev_projects\\liq_graph
git pull --ff-only origin cp9d-signal-breadth-pool-coverage
if ($LASTEXITCODE -ne 0) { throw "Git pull failed" }
python -m unittest discover -s scripts/tests -p 'test_*.py'
if ($LASTEXITCODE -ne 0) { throw "Analyzer tests failed" }
$run = Join-Path (Get-Location) 'runtime_live\\20261031T063803524Z'
python .\\scripts\\execution_analyze.py --folder $run --output (Join-Path $run 'execution_analysis.json')
if ($LASTEXITCODE -ne 0) { throw "Post-run analyzer failed; inspect execution_analysis.json" }
```

No RPC queries, on-chain execution, V10 replay or new live run are required.
The CP9D report also distinguishes positive *pool-path + simulation-slot*
combinations from *pool-path + frozen content-digest* states; an unchanged
source state across different simulated slots is not an independent discovered
price opportunity. A matching digest is not proof of equal bank state.

## Evidence and decision rules

From `execution.json` inspect `execution_candidates.cp9d_fresh_pairs`, `cp9d_active_pairs`, `cp9d_active_candidates_examined`, `cp9d_queue_full_by_strategy` and `cp9c_by_strategy`. Confirm V10 parallel report max and RPC provider failures independently.

From `execution_analysis.json`, inspect:

- `cp9d_signal_breadth_and_coverage.source_classes` for *fresh* vs *active* source requested/successful, different token pairs, distinct positive pool-path+slot events;
- `offered_oriented_pool_pairs_by_strategy` for diversity coverage **not** equivalent to executability;
- `hypothetical_wsol_settlement_greater_than_lamports` for threshold sensitivity without changing minimum_profit;
- `cp9c_strategy_comparison` for technical successes, economic positives and directly comparable same-revision **same-token-pair** overlap. Cross-token-pair overlap is not compared;
- `violations` must all be zero, requested/report counts exact, and no live broadcasts.

The five-minute run establishes technical/diagnostic behavior only; statistical superiority of strategies needs longer paired evidence. A follow-up ten-minute test is **not started automatically**. If comparing A and B causally, run individually configured controls later: `-CP9DMaxPairs 1 -CP9CPackingMode All` (CP9C equivalent); `-CP9DMaxPairs 3 -CP9CPackingMode All` (breadth alone); `-CP9DMaxPairs 1 -CP9CPackingMode AllCoverage` (coverage alone).

**Known limits:** Only positive active graph anchors are considered, no global scan of every imaginable token pair. Same-slot simulations are not independent real-world fills. No cost model is newly imposed on signal selection. A positive simulation is not proof of profitable mainnet broadcast.
