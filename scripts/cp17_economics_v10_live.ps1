# CP17: economic qualification + V10 simulateTransaction only; Windows PowerShell.
param(
    [ValidateRange(60,3600)][int]$DurationSeconds = 600,
    [string]$WalletPath = (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$priorPolicy = [Environment]::GetEnvironmentVariable('MINT_QUALIFICATION_ALLOW_MISSING_METRICS','Process')
$priorOne = [Environment]::GetEnvironmentVariable('MINT_QUALIFICATION_ONE_MISSING_ONLY','Process')
$priorDex = [Environment]::GetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE','Process')
Push-Location -LiteralPath $repo
try {
    if ((& git branch --show-current) -ne 'cp17-mint-economics-v10') {
        throw 'Use cp17-mint-economics-v10 branch for this controlled test'
    }
    if (-not (Test-Path -LiteralPath $WalletPath)) { throw "WalletPath missing: $WalletPath" }
    $env:MINT_QUALIFICATION_ALLOW_MISSING_METRICS = 'true'
    $env:MINT_QUALIFICATION_ONE_MISSING_ONLY = 'true'
    $env:DEXSCREENER_TRANSPORT_MODE = 'direct'

    & cargo test --locked --release mint_qualification
    if ($LASTEXITCODE -ne 0) { throw 'Mint qualification regression tests failed' }

    $root = Join-Path $repo 'runtime_live'
    $preflight = Join-Path $root 'cp17_economics_preflight.json'
    & python (Join-Path $repo 'scripts\cp17_economic_audit.py') --out $preflight
    if ($LASTEXITCODE -ne 0) { throw 'Read-only economic preflight failed; psql/database connection required' }

    $before = @{}
    Get-ChildItem -LiteralPath $root -Directory | ForEach-Object { $before[$_.FullName] = $true }
    Write-Host 'CP17: Jupiter one missing accepted; both missing not accepted'
    Write-Host 'TVL, Direct2, DEXs, WS configuration unchanged. Simulate only, no broadcasts.'
    $options = @{
        ExpectedBranch='cp17-mint-economics-v10'
        EnabledVenues='meteora_dlmm,raydium_cpmm,raydium_clmm,orca_whirlpool,pumpswap'
        EnabledRoutes='direct2'
        Stage='Signals'
        DurationSeconds=$DurationSeconds
        PoolLimit=0
        AccountBudget=25000
        MinPoolTvlUsd=500
        MinDirectionNotionalUsd=50
        DirectionLiquidityMode='Filter'
        ThresholdBps=0
        TvlSourceInfo=$true
        ProfileSignals=$true
        SimulateV10=$true
        WalletPath=$WalletPath
        SimulationLimit=0
        V10Parallelism=32
        CP9CPackingMode='AllCoverage'
        CP9DMaxPairs=3
        ExecutionDetailLimit=128
    }
    & (Join-Path $repo 'scripts\runtime_live.ps1') @options
    if ($LASTEXITCODE -ne 0) { throw 'CP17 V10 simulation failed; check runtime_live artifacts' }

    $runs = @(Get-ChildItem -LiteralPath $root -Directory | Where-Object { -not $before.ContainsKey($_.FullName) })
    if ($runs.Count -ne 1) { throw "Expected one new run folder, found $($runs.Count)" }
    $folder=$runs[0].FullName
    Copy-Item -LiteralPath $preflight -Destination (Join-Path $folder 'cp17_economics_preflight.json')
    $events = Join-Path $folder 'execution_events.jsonl'
    if (-not (Test-Path -LiteralPath $events)) { throw 'No V10 simulation events found' }
    & python (Join-Path $repo 'scripts\cp17_economic_audit.py') --events $events --out (Join-Path $folder 'cp17_economics_simulations.json')
    if ($LASTEXITCODE -ne 0) { throw 'CP17 V10 economic classification failed' }
    Write-Host "CP17 completed; artifacts: $folder"
} finally {
    [Environment]::SetEnvironmentVariable('MINT_QUALIFICATION_ALLOW_MISSING_METRICS',$priorPolicy,'Process')
    [Environment]::SetEnvironmentVariable('MINT_QUALIFICATION_ONE_MISSING_ONLY',$priorOne,'Process')
    [Environment]::SetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE',$priorDex,'Process')
    Pop-Location
}
