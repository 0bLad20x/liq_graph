# CP20 read-only mainnet validation. Requires the user's local .env and wallet.
# Runs V10 simulateTransaction only; never broadcasts real transactions.
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..'))
if ((git branch --show-current).Trim() -ne 'cp20-direct2-economic-predictor') {
    throw 'Checkout CP20 branch before running this test'
}
cargo test --locked --release cp20_
if ($LASTEXITCODE -ne 0) { throw 'CP20 tests failed' }
.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp20-direct2-economic-predictor `
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
    -CP9CPackingMode AllCoverage `
    -CP9DMaxPairs 3 `
    -ExecutionDetailLimit 128
if ($LASTEXITCODE -ne 0) { throw 'CP20 five-minute live run failed' }
