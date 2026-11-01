# CP14 Orca Whirlpool directed valuation + unchanged Direct2/V10 600-second live simulation.
param(
    [string]$WalletPath = (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json'),
    [ValidateRange(60,600)][int]$DurationSeconds = 600
)
$ErrorActionPreference='Stop'
$repo=Split-Path -Parent $PSScriptRoot
$branch='cp14-orca-whirlpool-direct2'
Push-Location -LiteralPath $repo
$oldCommitment=[Environment]::GetEnvironmentVariable('RPC_COMMITMENT','Process')
$oldDexMode=[Environment]::GetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE','Process')
try {
    $dirty=@(& git status --porcelain --untracked-files=no)
    if ($LASTEXITCODE -ne 0 -or $dirty.Count -gt 0) { throw 'Tracked changes exist; commit or save first.' }
    git fetch origin
    if ($LASTEXITCODE -ne 0) { throw 'git fetch failed' }
    git switch $branch
    if ($LASTEXITCODE -ne 0) { throw 'CP14 checkout failed' }
    git pull --ff-only origin $branch
    if ($LASTEXITCODE -ne 0) { throw 'CP14 fast-forward failed' }
    & cargo test --locked --release cp14_
    if ($LASTEXITCODE -ne 0) { throw 'CP14 directed fee regression tests failed' }
    if (-not (Test-Path -LiteralPath $WalletPath)) { throw "Wallet not found: $WalletPath" }
    $env:RPC_COMMITMENT='confirmed'
    $env:DEXSCREENER_TRANSPORT_MODE='direct'
    $opts=@{
        ExpectedBranch=$branch
        EnabledVenues='meteora_dlmm,pumpswap,raydium_cpmm,orca_whirlpool'
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
    if ($LASTEXITCODE -ne 0) { throw 'CP14 mainnet simulation failed' }
    $runs=@(Get-ChildItem -LiteralPath $artifactRoot -Directory |
        Where-Object { -not $existing.ContainsKey($_.FullName) })
    if ($runs.Count -ne 1) { throw "Expected one new CP14 artifact directory; found $($runs.Count)." }
    $folder=$runs[0].FullName
    $summary=Get-Content -LiteralPath (Join-Path $folder 'summary.json') -Raw | ConvertFrom-Json
    $market=Get-Content -LiteralPath (Join-Path $folder 'market_state.json') -Raw | ConvertFrom-Json
    $orcaPrepared=[long]$summary.registry_tvl.by_venue.orca_whirlpool.prepared_admitted
    $orcaObserved=[long]$market.metrics.fee_valuation.observations_by_model.orca_whirlpool
    if ($orcaPrepared -le 0 -or $orcaObserved -le 0) {
        throw "CP14 requires observed ORCA valuation: prepared=$orcaPrepared observations=$orcaObserved artifacts=$folder"
    }
    if (-not ($summary.runtime_completed -and $summary.execution_integration_passed -and
        $summary.market_delivery_passed -and $summary.signal_delivery_passed)) {
        throw "CP14 runtime/delivery acceptance failed: $folder"
    }
    if ($DurationSeconds -ge 60) {
        $events=Join-Path $folder 'execution_events.jsonl'
        $calibration=Join-Path $folder 'cp14_orca_calibration.json'
        & python (Join-Path $repo 'scripts\cp14_calibration_check.py') --events $events --out $calibration
        if ($LASTEXITCODE -ne 0) { throw "CP14 ORCA calibration incomplete or failed (exit=$LASTEXITCODE); see $calibration" }
        Write-Host "CP14 ORCA frozen-vs-V10 calibration: $calibration"
        $reference=Join-Path $folder 'cp14_orca_reference.json'
        & python (Join-Path $repo 'scripts\cp14_orca_reference.py') --details (Join-Path $folder 'execution_details.jsonl') --events $events --out $reference
        if ($LASTEXITCODE -ne 0) { throw "CP14 Orca independent integer reference check failed; see $reference" }
        Write-Host "CP14 independent integer fee/amount reference: $reference"
    }
    Write-Host "CP14 verified: ORCA prepared=$orcaPrepared valuations=$orcaObserved artifacts=$folder"

} finally {
    [Environment]::SetEnvironmentVariable('RPC_COMMITMENT',$oldCommitment,'Process')
    [Environment]::SetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE',$oldDexMode,'Process')
    Pop-Location
}
