# CP21S-A complete incremental mainnet validation. Requires the user's local .env and wallet.
# Runs V10 simulateTransaction only; never broadcasts real transactions.
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..'))
if ((git branch --show-current).Trim() -ne 'cp21s-a-complete-incremental-direct2') {
    throw 'Checkout CP21S-A branch before running this test'
}
cargo test --locked --release cp20_
if ($LASTEXITCODE -ne 0) { throw 'CP20 tests failed' }
cargo test --locked --release cp20s_
if ($LASTEXITCODE -ne 0) { throw 'CP20S scheduling tests failed' }
python -m unittest discover -s scripts/tests -p 'test_cp20_economic_audit.py'
if ($LASTEXITCODE -ne 0) { throw 'CP20 economic evidence tests failed' }
cargo test --locked --release cp21a_
if ($LASTEXITCODE -ne 0) { throw 'CP21A profit accounting tests failed' }
python -m unittest discover -s scripts/tests -p 'test_cp21a_profit_audit.py'
if ($LASTEXITCODE -ne 0) { throw 'CP21A profit audit tests failed' }
cargo test --locked --release cp21s_a_ -- --nocapture
if ($LASTEXITCODE -ne 0) { throw 'CP21S-A completeness tests failed' }
# Explicit opt-in: this flag chooses the unbounded incremental Direct2 engine.
# Legacy -CP9DMaxPairs is ignored in this new path.
$hadFlag = Test-Path Env:GRAPH_CP21S_INCREMENTAL_SIGNALS
$oldFlag = $env:GRAPH_CP21S_INCREMENTAL_SIGNALS
$env:GRAPH_CP21S_INCREMENTAL_SIGNALS = 'true'
try {
.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp21s-a-complete-incremental-direct2 `
    -Stage Signals `
    -EnabledVenues 'pumpswap,meteora_dlmm,raydium_cpmm' `
    -EnabledRoutes direct2 `
    -PoolLimit 0 `
    -AccountBudget 10000 `
    -WsProviders '1,2' `
    -MinPoolTvlUsd 500 `
    -MinDirectionNotionalUsd 50 `
    -DirectionLiquidityMode Filter `
    -ThresholdBps 0 `
    -DurationSeconds 300 `
    -TvlSourceInfo `
    -ProfileSignals `
    -SimulateV10 `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json') `
    -SimulationLimit 0 `
    -V10Parallelism 32 `
    -CP9CPackingMode Economic `
    -CP9DMaxPairs 1 `
    -ExecutionDetailLimit 128
if ($LASTEXITCODE -ne 0) { throw 'CP21S-A five-minute live run failed' }
} finally {
    if ($hadFlag) { $env:GRAPH_CP21S_INCREMENTAL_SIGNALS = $oldFlag }
    else { Remove-Item Env:GRAPH_CP21S_INCREMENTAL_SIGNALS -ErrorAction SilentlyContinue }
}
