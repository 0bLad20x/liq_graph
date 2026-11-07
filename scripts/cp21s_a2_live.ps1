# CP21S-A.2 timeline and complete incremental mainnet validation. Requires the user's local .env and wallet.
# Runs V10 simulateTransaction only; never broadcasts real transactions.
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..'))
if ((git branch --show-current).Trim() -ne 'cp21s-a2-state-journal-optimization') {
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
cargo test --locked --release cp21a1_ -- --nocapture
if ($LASTEXITCODE -ne 0) { throw 'CP21S-A.2 timeline and catalog tests failed' }
python -m unittest discover -s scripts/tests -p 'test_cp21a1_timeline.py'
if ($LASTEXITCODE -ne 0) { throw 'CP21S-A.2 timeline analyzer regression tests failed' }
cargo test --locked --release cp21a2_ -- --nocapture
if ($LASTEXITCODE -ne 0) { throw 'CP21S-A.2 incremental direction regressions failed' }
python -m unittest discover -s scripts/tests -p 'test_cp21a2_timeline_archive.py'
if ($LASTEXITCODE -ne 0) { throw 'CP21S-A.2 gzip archiving and reconstruction tests failed' }
# Explicit opt-in: this flag chooses the unbounded incremental Direct2 engine.
# Legacy -CP9DMaxPairs is ignored in this new path.
$hadFlag = Test-Path Env:GRAPH_CP21S_INCREMENTAL_SIGNALS
$oldFlag = $env:GRAPH_CP21S_INCREMENTAL_SIGNALS
$env:GRAPH_CP21S_INCREMENTAL_SIGNALS = 'true'
$hadTimeline = Test-Path Env:GRAPH_CP21S_A1_TIMELINE
$oldTimeline = $env:GRAPH_CP21S_A1_TIMELINE
$env:GRAPH_CP21S_A1_TIMELINE = 'true'
$hadArchive = Test-Path Env:GRAPH_CP21S_A2_ARCHIVE
$oldArchive = $env:GRAPH_CP21S_A2_ARCHIVE
$env:GRAPH_CP21S_A2_ARCHIVE = 'true'
try {
.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp21s-a2-state-journal-optimization `
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
    -ExecutionDetailLimit 128
if ($LASTEXITCODE -ne 0) { throw 'CP21S-A.2 five-minute live run failed' }
} finally {
    if ($hadFlag) { $env:GRAPH_CP21S_INCREMENTAL_SIGNALS = $oldFlag }
    else { Remove-Item Env:GRAPH_CP21S_INCREMENTAL_SIGNALS -ErrorAction SilentlyContinue }
    if ($hadTimeline) { $env:GRAPH_CP21S_A1_TIMELINE = $oldTimeline }
    else { Remove-Item Env:GRAPH_CP21S_A1_TIMELINE -ErrorAction SilentlyContinue }
    if ($hadArchive) { $env:GRAPH_CP21S_A2_ARCHIVE = $oldArchive }
    else { Remove-Item Env:GRAPH_CP21S_A2_ARCHIVE -ErrorAction SilentlyContinue }
}
