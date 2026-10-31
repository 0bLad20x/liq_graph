# CP9E – Jupiter mint persistence reliability + Direct2/Direct3 V10 experiment

**Base**: CP9D `cp9d-signal-breadth-pool-coverage`. **Simulation-only.** No real transaction broadcasts, no removal of established CP9D strategies or liquidation/fee gates.

## A. Root cause / transport correction

CP9D's attempted 60-minute run failed after about 38m34s with:
`critical worker persistence: mint persistence lost 5 Jupiter batches`.
The source was publishing `Arc<DataBatch>` through a bounded Tokio *broadcast* stream to both the mint qualification worker and PostgreSQL persistence. PostgreSQL writes can fall behind the Jupiter producer and a broadcast receiver then receives `Lagged(n)`. It is correct for the writer to fail on missing batches; suppressing the error would silently corrupt persisted mint freshness.

In CP9E, the Orchestrator wires a separate `tokio::sync::mpsc::channel::<Arc<DataBatch>>(256)` from **Jupiter source directly to PostgreSQL writer**. The Jupiter source must await `send(batch)` before broadcasting that batch to mint qualification. A database stall now backpressures Jupiter instead of overrunning the persistence receiver; a closed writer channel returns an error. The existing unmodified qualification broadcast remains a distinct consumer. The channel also retains bounded memory and ordered delivery, rather than using an arbitrarily large buffer. The CP1 live path uses the same writer connection. A Rust test sends 256 consecutive batches through a queue with capacity 1 and verifies ordered, lossless receipt.

**Limits:** This guarantees backpressured in-process delivery to the writer under a slow receiver and normal graceful shutdown. It is not a disk-backed queue, exactly-once guarantee across process crash, nor a claim that an unavailable PostgreSQL database cannot still fail. The existing accepted Registry and Onchain Mint broadcast receivers remain fatal on overrun; they are not modified by this checkpoint. Persisted row counts should be checked on the final evidence.

## B. Signal and graph scopes

`-Direct2Direct3Only` and `-RouteKind Direct2Direct3` must be used together. This selects the two intended families in graph and execution consumer (not merely filtering after constructing all Triangle3/4 shapes). Graph full/reference reconciliation remains available; existing Direct2-only mode is unchanged and mutually exclusive.

- **Direct2**: direct S→T→S; two distinct pools within a single settlement/target pair.
- **Direct3**: direct S→T, quoted pool T→Q, converter Q→S (S=WSOL or USDC; Q=other anchor); V10 receives two direct group pools plus the fixed conversion pool.
- Baseline / Contrarian / Mixed / Coverage remain, built from current prepared V10 manifests and kept within exact 64-key, byte and 16-pool V10 limits.
- `-CP9DMaxPairs 3` now means up to three distinct **(route kind, settlement, target)** source variants per revision, allowing Direct2 and Direct3 for the same target to be compared. They are not mixed together into one package.
- Active signal witnesses are revalidated against current directed marginal rates. No false assumption that an old active positive source has a new independent profitable trade.
- V10 continues to choose trade size with `trade_size=0`; `minimum_profit=0`. No additional economic veto introduced.

The analyzer's `cp9e_direct_route_comparison.by_route` distinguishes requests, RPC successes, positive WSOL-settlement simulations over network fees, different executed pool-path/slot observations and different frozen path/state observations for Direct2 and Direct3. Direct3 pair identity is reconstructed from the original three-leg source witness because archived compact requests deliberately omit V10 manifests/groups.

## C. One 60-minute local experiment

Use the **same existing, working wallet path** as in CP9D. Example below matches the user's previous successful shell command. A shorter smoke test (`DurationSeconds 120`) may be run first if desired to validate RPC/provider support for Direct3, but the user requested a one-hour comparison.

```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
if ($LASTEXITCODE -ne 0) { throw "Git fetch failed" }
git switch cp9e-jupiter-reliable-direct3
if ($LASTEXITCODE -ne 0) { throw "Git switch failed" }
git pull --ff-only origin cp9e-jupiter-reliable-direct3
if ($LASTEXITCODE -ne 0) { throw "Git pull failed" }
cargo test --locked
if ($LASTEXITCODE -ne 0) { throw "Rust tests failed" }
python -m unittest discover -s scripts/tests -p 'test_*.py'
if ($LASTEXITCODE -ne 0) { throw "Python tests failed" }
$previousCommitment=$env:RPC_COMMITMENT
try {
    $env:RPC_COMMITMENT='confirmed'
    .\scripts\runtime_live.ps1 `
        -ExpectedBranch cp9e-jupiter-reliable-direct3 `
        -EnabledVenues 'meteora_dlmm,pumpswap' `
        -Stage Signals `
        -DurationSeconds 3600 `
        -PoolLimit 0 -AccountBudget 25000 `
        -MinPoolTvlUsd 500 `
        -MinDirectionNotionalUsd 50 -DirectionLiquidityMode Filter `
        -Direct2Direct3Only -RouteKind Direct2Direct3 `
        -ThresholdBps 0 -V10Parallelism 32 `
        -CP9DMaxPairs 3 -CP9CPackingMode AllCoverage `
        -ProfileSignals -ExecutionDetailLimit 64 `
        -RequireCalibrationEvidence `
        -SimulateV10 -SimulationLimit 0 `
        -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
    if ($LASTEXITCODE -ne 0) { throw "CP9E live test failed" }
} finally {
    $env:RPC_COMMITMENT=$previousCommitment
}
```

## D. Acceptance

- 3600 seconds completed by runtime; no `mint persistence lost Jupiter batches`; PostgreSQL writes and other consumers complete normally.
- Report request IDs match V10 reports exactly, and every request satisfies manifest, quantity-proof and account invariants.
- `graph_direct2_direct3_only=true`, `route_kind=Direct2Direct3`; published signal and V10 route types only `direct2`, `direct3`. Confirm both produce V10 requests; if one has no positive marginal signals, report its absence rather than inventing successes.
- Compare offered distinct positive frozen pool path/states and eligible active/fresh signal breadth *per route type and strategy*, not total profits of different time windows as if matched.
- Investigate quote errors against observation slot, component slot spread and simulation context; slot equality alone does not prove bank equality.
- No transaction broadcasts. No automatic run longer than the user's requested 60 minutes.

**Note**: External `runtime_live.ps1` after an early fatal error may show misleading `calibration evidence incomplete`. Read `runtime.json` for the actual fatal worker; never treat that summary flag as a numerical quote failure.
