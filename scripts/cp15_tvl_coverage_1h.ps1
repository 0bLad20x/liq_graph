# CP15: one-hour Raydium TVL coverage audit; no transaction simulation.
# Requires local .env, PostgreSQL and configured REST/RPC providers.
param(
    [string]$EnabledVenues = 'raydium_cpmm,raydium_clmm,orca_whirlpool',
    [ValidateSet(300,3600)][int]$DurationSeconds = 3600
)
$ErrorActionPreference='Stop'
if ($DurationSeconds -eq 300) {Write-Host 'CP15 TVL coverage: 300-second transport smoke test, not one-hour acceptance'}
$repo=Split-Path -Parent $PSScriptRoot
$oldDexMode=[Environment]::GetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE','Process')
Push-Location -LiteralPath $repo
try {
    # Keep the same working direct API transport as the CP15 simulation runner.
    # Proxy-only mode without verified routes rejects every enrichment request.
    $env:DEXSCREENER_TRANSPORT_MODE='direct'
    & (Join-Path $repo 'scripts\runtime_live.ps1') `
        -ExpectedBranch cp15-raydium-clmm-direct2 `
        -EnabledVenues $EnabledVenues `
        -EnabledRoutes direct2 `
        -Stage Graph `
        -GraphAudit `
        -TvlSourceInfo `
        -DurationSeconds $DurationSeconds `
        -PoolLimit 0 `
        -AccountBudget 25000 `
        -MinPoolTvlUsd 500
    if ($LASTEXITCODE -ne 0) {throw 'CP15 Raydium TVL one-hour audit failed'}
} finally {
    [Environment]::SetEnvironmentVariable('DEXSCREENER_TRANSPORT_MODE',$oldDexMode,'Process')
    Pop-Location
}
