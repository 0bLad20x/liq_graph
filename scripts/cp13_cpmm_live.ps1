# CP13 Raydium CPMM directional valuation + unchanged Direct2/V10 600-second live simulation.
param(
    [string]$WalletPath = (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json'),
    [ValidateRange(60,600)][int]$DurationSeconds = 600
)
$ErrorActionPreference='Stop'
$repo=Split-Path -Parent $PSScriptRoot
$branch='cp13-raydium-cpmm-direct2'
Push-Location -LiteralPath $repo
$oldCommitment=[Environment]::GetEnvironmentVariable('RPC_COMMITMENT','Process')
$oldDexMode=[Environment]::GetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE','Process')
try {
    $dirty=@(& git status --porcelain --untracked-files=no)
    if ($LASTEXITCODE -ne 0 -or $dirty.Count -gt 0) { throw 'Tracked changes exist; commit or save first.' }
    git fetch origin
    if ($LASTEXITCODE -ne 0) { throw 'git fetch failed' }
    git switch $branch
    if ($LASTEXITCODE -ne 0) { throw 'CP13 checkout failed' }
    git pull --ff-only origin $branch
    if ($LASTEXITCODE -ne 0) { throw 'CP13 fast-forward failed' }
    & cargo test --locked --release cp13_
    if ($LASTEXITCODE -ne 0) { throw 'CP13 directed fee regression tests failed' }
    if (-not (Test-Path -LiteralPath $WalletPath)) { throw "Wallet not found: $WalletPath" }
    $env:RPC_COMMITMENT='confirmed'
    $env:DEXSCREENER_TRANSPORT_MODE='direct'
    $opts=@{
        ExpectedBranch=$branch
        EnabledVenues='meteora_dlmm,pumpswap,raydium_cpmm'
        EnabledRoutes='direct2'
        Stage='Signals'
        DurationSeconds=$DurationSeconds
        PoolLimit=0
        AccountBudget=25000
        MinPoolTvlUsd=500
        MinDirectionNotionalUsd=50
        DirectionLiquidityMode='Filter'
        ThresholdBps=0
        V10Parallelism=32
        CP9DMaxPairs=3
        CP9CPackingMode='AllCoverage'
        ProfileSignals=$true
        ExecutionDetailLimit=64
        RequireCalibrationEvidence=$true
        SimulateV10=$true
        SimulationLimit=0
        WalletPath=$WalletPath
    }
    $artifactRoot=Join-Path $repo 'runtime_live'
    $existing=@{}
    if (Test-Path -LiteralPath $artifactRoot) {
        Get-ChildItem -LiteralPath $artifactRoot -Directory | ForEach-Object { $existing[$_.FullName]=$true }
    }
    & (Join-Path $repo 'scripts\runtime_live.ps1') @opts
    if ($LASTEXITCODE -ne 0) { throw 'CP13 600-second mainnet simulation did not pass' }
    $runs=@(Get-ChildItem -LiteralPath $artifactRoot -Directory |
        Where-Object { -not $existing.ContainsKey($_.FullName) })
    if ($runs.Count -ne 1) { throw "Expected one new CP13 artifact directory; found $($runs.Count)." }
    $folder=$runs[0].FullName
    $summary=Get-Content -LiteralPath (Join-Path $folder 'summary.json') -Raw | ConvertFrom-Json
    $market=Get-Content -LiteralPath (Join-Path $folder 'market_state.json') -Raw | ConvertFrom-Json
    $cpmmPrepared=[long]$summary.registry_tvl.by_venue.raydium_cpmm.prepared_admitted
    $cpmmObserved=[long]$market.metrics.fee_valuation.observations_by_model.raydium_cpmm
    if ($cpmmPrepared -le 0 -or $cpmmObserved -le 0) {
        throw "CP13 requires observed CPMM valuation: prepared=$cpmmPrepared observations=$cpmmObserved artifacts=$folder"
    }
    if (-not ($summary.runtime_completed -and $summary.execution_integration_passed -and
        $summary.market_delivery_passed -and $summary.signal_delivery_passed)) {
        throw "CP13 runtime/delivery acceptance failed: $folder"
    }
    if ($DurationSeconds -eq 60) {
        $events=Join-Path $folder 'execution_events.jsonl'
        $calibration=Join-Path $folder 'cp13_cpmm_calibration.json'
        & python (Join-Path $repo 'scripts\cp13_calibration_check.py') --events $events --out $calibration
        if ($LASTEXITCODE -ne 0) { throw "CP13 60-second CPMM calibration incomplete or failed (exit=$LASTEXITCODE); see $calibration" }
        Write-Host "CP13 CPMM frozen-vs-V10 calibration: $calibration"
    }
    Write-Host "CP13 verified: CPMM prepared=$cpmmPrepared valuations=$cpmmObserved artifacts=$folder"

} finally {
    [Environment]::SetEnvironmentVariable('RPC_COMMITMENT',$oldCommitment,'Process')
    [Environment]::SetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE',$oldDexMode,'Process')
    Pop-Location
}
