# CP9B – Direct2-only graph and concurrent V10 simulations

## Scope and invariants

- **Direct2 only, from graph generation onward**, not just an executor route filter. Set `GRAPH_DIRECT2_ONLY=true`. The Registry, market observations, WS account watching, and prepared pools remain unchanged: only `RouteTopology` objects for Direct3/Triangle3/Triangle4 are not emitted. Incremental graph updates and CP6C reference comparison use the same explicit scope.
- **Parallel read-only V10 `simulateTransaction`**, not sending real transactions. Set `GRAPH_V10_SIMULATION_PARALLELISM=N` with `1 <= N <= 64`. A single shared RPC scheduler distributes eligible calls according to provider capabilities, rate limits, load and retry policy. N workers do **not** imply N independent RPC accounts or N completed transactions per second.
- No new price algorithm, no new `unknown` gate, no changes in V10 choice of amount, same CP9A threshold.
- Candidate source still selects **at most one request per evaluated signal revision**. Concurrent V10 frees slots sooner but does not enumerate every qualified same-revision candidate. The bounded jobs channel holds at most N waiting requests. Full channels continue to skip revisions, never backpressure the MarketState or graph. Retain and compare `skipped_busy_revisions`, `qualifying_ranges`, `requests`, and payload repeats.
- Independent V10 requests share an immutable signer via `Arc`. Each request has its own build data and RPC operation. Reports may finish out of order; the existing report collector joins on `request.id`. The shutdown drains accepted simulation tasks before writing final evidence. All jobs remain simulation-only (zero broadcasts).
- The evidence field `execution.json.v10_parallelism` includes configured concurrency, observed peak in-flight and completed request count; the checked-out run folder logs the exact input settings. HTTP RPC provider/method counts and dispatch queue stats already exist in `runtime.log`.
- Successful **RPC execution**, completed **DEX cycle**, and **profitable net wallet outcome** must be measured separately. `Custom(1)` never counts as success. Do not use age of last account update as a staleness indicator unless a missing WS event or proven state mismatch is established.

## Local PowerShell acceptance – controlled comparison

Use the branch `cp9b-direct2-parallel-v10`. Make sure local mainnet RPC, Postgres and wallet are configured. Run a 60-second test with 4 workers, followed by a 600-second comparison with N=1, N=4, N=8 at the same 50 USD minimum threshold. The script compiles a locked release before starting and never sends real transactions.

```powershell
Set-Location C:\dev_projects\liq_graph
git status --short
git fetch origin
git switch cp9b-direct2-parallel-v10
git pull --ff-only origin cp9b-direct2-parallel-v10
cargo test --locked
if ($LASTEXITCODE -ne 0) { throw "Rust tests failed" }

foreach ($parallel in @(1,4,8)) {
    .\scripts\runtime_live.ps1 `
        -ExpectedBranch cp9b-direct2-parallel-v10 `
        -EnabledVenues 'meteora_dlmm,pumpswap' `
        -Stage Signals -DurationSeconds 600 `
        -PoolLimit 0 -AccountBudget 25000 -MinPoolTvlUsd 500 `
        -MinDirectionNotionalUsd 50 -DirectionLiquidityMode Filter `
        -Direct2Only -RouteKind Direct2 -V10Parallelism $parallel `
        -ProfileSignals -ExecutionDetailLimit 32 `
        -SimulateV10 -SimulationLimit 0 `
        -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
    if ($LASTEXITCODE -ne 0) { throw "Failed parallelism=$parallel" }
}
```

Compare request count per minute, completed full unique DEX cycles, amount/return calibration at V10 chosen sizes, provider/method RPC calls, simulation RPC elapsed milliseconds, queue busy skips, duplicates, `amount_assessment` and wallet net gains. Keep RPC commitment constant (`confirmed`) for all runs; where possible set the provider commitment consistently in the local `.env`.

## Tests and non-goals

- Tests cover true Direct2-only graph birth and incremental update, exclusion of longer topologies, unchanged pool membership and reference parity.
- `GRAPH_V10_SIMULATION_PARALLELISM=1` preserves serial behavior.
- **Not** implemented: sending transactions, simulating all alternatives in one signal revision, de-duplication/cooldowns, new candidate ranking, dynamic USD oracle, or provider-specific dedicated wallets.
- RPC response order is naturally nondeterministic under parallelism; request IDs remain the stable join key for diagnostics.
