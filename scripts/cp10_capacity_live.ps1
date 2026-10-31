# CP10 locked tests, then 600-second read-only Direct2/V10 live run.
param(
    [string]$WalletPath = (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
)
$ErrorActionPreference='Stop'
$repo=Split-Path -Parent $PSScriptRoot
$branch='cp10-v10-capacity-fastpath'
Push-Location -LiteralPath $repo
$oldCommitment=[Environment]::GetEnvironmentVariable('RPC_COMMITMENT','Process')
$oldDexMode=[Environment]::GetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE','Process')
try {
    # Match runtime_live.ps1: only tracked modifications can invalidate the
    # reproducible build. Preserve untracked live reports, snapshots and files.
    $dirty=@(& git status --porcelain --untracked-files=no)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect tracked Git changes.' }
    if ($dirty.Count -gt 0) {
        $dirty | ForEach-Object { Write-Warning "Tracked change: $_" }
        throw 'Tracked changes exist. Commit or save them before CP10.'
    }
    git fetch origin
    if ($LASTEXITCODE -ne 0) { throw 'git fetch failed' }
    git switch $branch
    if ($LASTEXITCODE -ne 0) { throw 'CP10 branch checkout failed' }
    git pull --ff-only origin $branch
    if ($LASTEXITCODE -ne 0) { throw 'CP10 branch fast-forward failed' }
    & cargo test --locked --release cp10_tests
    if ($LASTEXITCODE -ne 0) { throw 'CP10 byte-exact regression tests failed' }
    if (-not (Test-Path -LiteralPath $WalletPath)) { throw "Wallet file not found: $WalletPath" }
    $env:RPC_COMMITMENT='confirmed'
    $env:DEXSCREENER_TRANSPORT_MODE='direct'
    $opts=@{
        ExpectedBranch=$branch
        EnabledVenues='meteora_dlmm,pumpswap'
        EnabledRoutes='direct2'
        Stage='Signals'
        DurationSeconds=600
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
    & (Join-Path $repo 'scripts\runtime_live.ps1') @opts
    if ($LASTEXITCODE -ne 0) { throw 'CP10 600-second live run failed' }
} finally {
    [Environment]::SetEnvironmentVariable('RPC_COMMITMENT',$oldCommitment,'Process')
    [Environment]::SetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE',$oldDexMode,'Process')
    Pop-Location
}
