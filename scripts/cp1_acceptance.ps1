# Reproducible CP1 local acceptance: compile, targeted regression and real provider run.
# Requires .env/PostgreSQL, Jupiter API keys and the configured HTTP proxy transport.
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

cargo check --locked
if ($LASTEXITCODE -ne 0) { throw "CP1 cargo check failed: $LASTEXITCODE" }

cargo test --locked tvl_freshness_tests
if ($LASTEXITCODE -ne 0) { throw "CP1 TVL regression failed: $LASTEXITCODE" }

$env:CP1_PROBE_DURATION_SECONDS = "300"
$env:MINT_QUALIFICATION_REFRESH_SECONDS = "60"
$env:DEXSCREENER_PARALLEL = "16"
$env:DEXSCREENER_REFRESH_SECONDS = "1800"
$env:DEXSCREENER_FAILURE_RETRY_SECONDS = "120"
$env:JUPITER_BACKFILL_ON_START = "false"

$log = "cp1_live_final.log"
cargo run --locked --release -- --cp1-live 2>&1 | Tee-Object -FilePath $log
if ($LASTEXITCODE -ne 0) { throw "CP1 live run failed: $LASTEXITCODE; inspect $log" }
$finished = Select-String -Path $log -Pattern 'cp1 live complete=true persisted=true qualified_mints=[1-9][0-9]* live_tvl_pools=[1-9][0-9]*' | Select-Object -Last 1
if (-not $finished) { throw "CP1 missing successful live proof; inspect $log" }
$started = Select-String -Path $log -Pattern 'dexscreener cp1 continuous started=true' | Select-Object -First 1
if (-not $started) { throw "CP1 continuous DexScreener scheduler did not start" }
$qualified = Select-String -Path $log -Pattern 'mint qualification published qualified=[1-9]' | Select-Object -First 1
if (-not $qualified) { throw "CP1 mint qualification did not publish any targets" }
$registry = Select-String -Path $log -Pattern 'pool registry complete .*conflicts=0 lagged_batches=0' | Select-Object -Last 1
if (-not $registry) { throw "CP1 Registry had conflicts/losses or no final accounting" }
Write-Host "CP1 ACCEPTANCE PASSED: $($finished.Line)"
