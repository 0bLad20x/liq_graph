# CP16 10-minute controlled Jupiter-qualification comparison.
# Other components, including WebSocket provider configuration, remain unchanged.
param(
    [ValidateRange(60,3600)][int]$DurationSeconds = 600
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$previousPolicy = [Environment]::GetEnvironmentVariable('MINT_QUALIFICATION_ALLOW_MISSING_METRICS','Process')
$previousDex = [Environment]::GetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE','Process')
Push-Location -LiteralPath $repo
try {
    [Environment]::SetEnvironmentVariable('MINT_QUALIFICATION_ALLOW_MISSING_METRICS','true','Process')
    # Same transport as the preceding five-protocol baseline; no WS overrides.
    [Environment]::SetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE','direct','Process')
    Write-Host 'CP16 Jupiter policy: missing economic values pass, known values below thresholds veto'
    Write-Host 'Protocol scope: DLMM, CPMM, CLMM, Orca Whirlpool, PumpSwap'
    Write-Host 'Unchanged: WS provider assignment/capacities, 500 USD pool TVL (PumpSwap exempt), Direct2'
    & (Join-Path $repo 'scripts\runtime_live.ps1') `
        -ExpectedBranch 'cp16-mint-missing-metrics-10min' `
        -EnabledVenues 'meteora_dlmm,raydium_cpmm,raydium_clmm,orca_whirlpool,pumpswap' `
        -EnabledRoutes direct2 `
        -Stage Signals `
        -DurationSeconds $DurationSeconds `
        -PoolLimit 0 `
        -AccountBudget 25000 `
        -MinPoolTvlUsd 500 `
        -MinDirectionNotionalUsd 50 `
        -DirectionLiquidityMode Filter `
        -ThresholdBps 0 `
        -TvlSourceInfo `
        -ProfileSignals
    if ($LASTEXITCODE -ne 0) { throw "CP16 10-minute live run failed (exit=$LASTEXITCODE)" }
} finally {
    [Environment]::SetEnvironmentVariable('MINT_QUALIFICATION_ALLOW_MISSING_METRICS',$previousPolicy,'Process')
    [Environment]::SetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE',$previousDex,'Process')
    Pop-Location
}
