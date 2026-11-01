# CP15 Raydium CLMM directed valuation + unchanged Direct2/V10 60..600s live simulation.
param(
    [string]$WalletPath = (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json'),
    [ValidateRange(60,600)][int]$DurationSeconds = 600
)
$ErrorActionPreference='Stop'
$repo=Split-Path -Parent $PSScriptRoot
$branch='cp15-raydium-clmm-direct2'
Push-Location -LiteralPath $repo
$oldCommitment=[Environment]::GetEnvironmentVariable('RPC_COMMITMENT','Process')
$oldDexMode=[Environment]::GetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE','Process')
try {
    $dirty=@(& git status --porcelain --untracked-files=no)
    if ($LASTEXITCODE -ne 0 -or $dirty.Count -gt 0) { throw 'Tracked changes exist; commit or save first.' }
    git fetch origin
    if ($LASTEXITCODE -ne 0) { throw 'git fetch failed' }
    git switch $branch
    if ($LASTEXITCODE -ne 0) { throw 'CP15 checkout failed' }
    git pull --ff-only origin $branch
    if ($LASTEXITCODE -ne 0) { throw 'CP15 fast-forward failed' }
    & cargo test --locked --release cp15_
    if ($LASTEXITCODE -ne 0) { throw 'CP15 directed fee regression tests failed' }
    & python (Join-Path $repo 'scripts\test_cp15_clmm_reference.py')
    if ($LASTEXITCODE -ne 0) { throw 'CP15 independent integer reference regressions failed' }
    if (-not (Test-Path -LiteralPath $WalletPath)) { throw "Wallet not found: $WalletPath" }
    $env:RPC_COMMITMENT='confirmed'
    $env:DEXSCREENER_TRANSPORT_MODE='direct'
    $opts=@{
        ExpectedBranch=$branch
        EnabledVenues='meteora_dlmm,pumpswap,raydium_cpmm,orca_whirlpool,raydium_clmm'
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
        ExecutionDetailLimit=128
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
    if ($LASTEXITCODE -ne 0) { throw 'CP15 mainnet simulation failed' }
    $runs=@(Get-ChildItem -LiteralPath $artifactRoot -Directory |
        Where-Object { -not $existing.ContainsKey($_.FullName) })
    if ($runs.Count -ne 1) { throw "Expected one new CP15 artifact directory; found $($runs.Count)." }
    $folder=$runs[0].FullName
    $summary=Get-Content -LiteralPath (Join-Path $folder 'summary.json') -Raw | ConvertFrom-Json
    $market=Get-Content -LiteralPath (Join-Path $folder 'market_state.json') -Raw | ConvertFrom-Json
    $clmmPrepared=[long]$summary.registry_tvl.by_venue.raydium_clmm.prepared_admitted
    $clmmObserved=[long]$market.metrics.fee_valuation.observations_by_model.raydium_clmm
    # Registry-owned, end-of-run snapshot: no additional telemetry queue or
    # mutable history in MarketState. Counts include only currently admitted pools.
    $directions=$summary.registry_tvl.clmm_direction_admission
    if ($null -eq $directions) { throw 'CP15 registry direction counters missing from summary.json' }
    $preparedDirections=[long]$directions.prepared_both +
        [long]$directions.prepared_0_to_1_only +
        [long]$directions.prepared_1_to_0_only +
        [long]$directions.prepared_unclassified
    if ($preparedDirections -ne $clmmPrepared) {
        throw "CP15 Raydium CLMM direction counts inconsistent: $preparedDirections != $clmmPrepared"
    }
    $directions | ConvertTo-Json -Depth 5 |
        Set-Content -LiteralPath (Join-Path $folder 'cp15_clmm_directions.json') -Encoding utf8
    Write-Host "CP15 Raydium CLMM directions: both=$($directions.prepared_both) 0to1_only=$($directions.prepared_0_to_1_only) 1to0_only=$($directions.prepared_1_to_0_only) unclassified=$($directions.prepared_unclassified) incomplete_neither=$($directions.incomplete_neither)"
    if ($clmmPrepared -le 0 -or $clmmObserved -le 0) {
        throw "CP15 requires observed CLMM valuation: prepared=$clmmPrepared observations=$clmmObserved artifacts=$folder"
    }
    if (-not ($summary.runtime_completed -and $summary.execution_integration_passed -and
        $summary.market_delivery_passed -and $summary.signal_delivery_passed)) {
        throw "CP15 runtime/delivery acceptance failed: $folder"
    }
    if ($DurationSeconds -ge 60) {
        $events=Join-Path $folder 'execution_events.jsonl'
        $calibration=Join-Path $folder 'cp15_clmm_calibration.json'
        & python (Join-Path $repo 'scripts\cp15_clmm_calibration.py') --events $events --out $calibration
        if ($LASTEXITCODE -ne 0) { throw "CP15 CLMM calibration incomplete or failed (exit=$LASTEXITCODE); see $calibration" }
        Write-Host "CP15 CLMM frozen-vs-V10 calibration: $calibration"
        $reference=Join-Path $folder 'cp15_clmm_reference.json'
        & python (Join-Path $repo 'scripts\cp15_clmm_reference.py') --details (Join-Path $folder 'execution_details.jsonl') --events $events --out $reference
        if ($LASTEXITCODE -ne 0) { throw "CP15 Raydium CLMM independent integer reference check failed; see $reference" }
        Write-Host "CP15 independent integer fee/amount reference: $reference"
    }
    Write-Host "CP15 verified: CLMM prepared=$clmmPrepared valuations=$clmmObserved artifacts=$folder"

} finally {
    [Environment]::SetEnvironmentVariable('RPC_COMMITMENT',$oldCommitment,'Process')
    [Environment]::SetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE',$oldDexMode,'Process')
    Pop-Location
}
