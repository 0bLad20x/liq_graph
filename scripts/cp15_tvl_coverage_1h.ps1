# CP15: one-hour Raydium TVL coverage audit; no transaction simulation.
# Requires local .env, PostgreSQL and configured REST/RPC providers.
param(
    [string]$EnabledVenues = 'raydium_cpmm,raydium_clmm,orca_whirlpool',
    [int]$DurationSeconds = 3600
)
$ErrorActionPreference='Stop'
if ($DurationSeconds -ne 3600) {throw 'CP15 one-hour audit requires DurationSeconds=3600'}
$repo=Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $repo
try {
    & (Join-Path $repo 'scripts\runtime_live.ps1') `
        -ExpectedBranch cp15-raydium-clmm-direct2 `
        -EnabledVenues $EnabledVenues `
        -EnabledRoutes direct2 `
        -Stage Graph `
        -GraphAudit `
        -DurationSeconds $DurationSeconds `
        -PoolLimit 0 `
        -AccountBudget 25000 `
        -MinPoolTvlUsd 500
    if ($LASTEXITCODE -ne 0) {throw 'CP15 Raydium TVL one-hour audit failed'}
} finally {
    Pop-Location
}
