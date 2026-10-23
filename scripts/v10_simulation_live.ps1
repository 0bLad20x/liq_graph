param(
    [Parameter(Mandatory = $true)][string]$WalletPath,
    [string]$RequestPath,
    [string]$PoolA,
    [string]$PoolB,
    [ValidateSet("WSOL", "USDC")][string]$Settlement = "WSOL",
    [switch]$UseFlashloan,
    [UInt64]$MinimumProfit = 0,
    [string]$PsqlPath = "C:\Program Files\PostgreSQL\18\bin\psql.exe"
)

# Manual job source only. No route search, ranking, admission decision or DB write.
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$branch = git branch --show-current
$head = git rev-parse HEAD
$tracked = git status --short --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $branch -notin @("agent/v10-simulation-worker", "main", "cp3-central-persistence-dual-registry") -or $tracked) {
    throw "Use a clean, committed agent/v10-simulation-worker checkout, or main after merge."
}
Write-Host "V10 simulation branch=$branch head=$head broadcasts=false"
cargo build --locked --release
if ($LASTEXITCODE -ne 0) { throw "cargo build --locked --release failed; no simulation started." }
git diff --exit-code -- Cargo.lock
if ($LASTEXITCODE -ne 0) { throw "Cargo.lock changed during build." }
$binary = Join-Path (Get-Location) "target\release\liq_graph.exe"
$WalletPath = (Resolve-Path $WalletPath).Path
$wallet = (& $binary --v10-wallet $WalletPath | Out-String).Trim()
if ($LASTEXITCODE -ne 0) { throw "Cannot load the wallet keypair." }
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ")
$folder = Join-Path (Get-Location) "v10_live\$stamp"
New-Item -ItemType Directory -Path $folder | Out-Null
$frozenRequest = Join-Path $folder "request.json"
$utf8 = [System.Text.UTF8Encoding]::new($false)

if ($RequestPath) {
    if ($PoolA -or $PoolB -or $UseFlashloan -or $MinimumProfit -ne 0 -or $Settlement -ne "WSOL") {
        throw "A complete RequestPath supplies its own pool selection and execution parameters."
    }
    Copy-Item -LiteralPath (Resolve-Path $RequestPath).Path -Destination $frozenRequest
} else {
    foreach ($address in @($PoolA, $PoolB, $wallet)) {
        if ($address -cnotmatch '^[1-9A-HJ-NP-Za-km-z]{32,44}$') { throw "Supply two explicit pool addresses and a valid wallet." }
    }
    if ($PoolA -eq $PoolB) { throw "Select two different pools." }
    $sql = @'
SELECT COALESCE(jsonb_agg(jsonb_build_object(
  'manifest', jsonb_build_object('pool', w.address,
    'mint_a', p.descriptor->'mints'->0->>'address',
    'mint_b', p.descriptor->'mints'->1->>'address',
    'validated_slot', p.validated_slot,
    'bound_signer', COALESCE(p.descriptor->'bound_signer', CASE WHEN p.venue=11
      THEN p.descriptor->'evidence'->'payload'->'v10_wallet' ELSE NULL END)
  ) || (p.descriptor->'manifest'),
  'mints', p.descriptor->'mints'
) ORDER BY w.ordinal), '[]'::jsonb)
FROM (VALUES (:'pool_a', 0),
             (:'pool_b', 1)) AS w(address,ordinal)
JOIN public.pools p ON p.pool_address=w.address
WHERE p.status=1 AND jsonb_typeof(p.descriptor->'manifest')='object'
  AND jsonb_array_length(p.descriptor->'mints')=2;
'@
    $raw = & (Join-Path $PSScriptRoot "cp3_db.ps1") -Action Query -Query $sql `
        -Variables @{ pool_a = $PoolA; pool_b = $PoolB } -TuplesOnly -NoAlign -PsqlPath $PsqlPath
    if (-not $raw) { throw "Manual pool export returned no records." }
    $pools = @(($raw | Out-String | ConvertFrom-Json))
    if ($pools.Count -ne 2) { throw "Both explicitly selected pools must have persisted Prepared manifests and mint proofs." }
    $settlementMint = if ($Settlement -eq "WSOL") { "So11111111111111111111111111111111111111112" } else { "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" }
    $target = $null
    foreach ($pool in $pools) {
        $pair = @($pool.manifest.mint_a, $pool.manifest.mint_b)
        if ($settlementMint -notin $pair) { throw "Both selected pools must pair the chosen settlement with the same target." }
        $other = @($pair | Where-Object { $_ -ne $settlementMint })
        if ($other.Count -ne 1 -or ($target -and $target -ne $other[0])) { throw "Selected pools have incompatible mint pairs." }
        $target = $other[0]
        if ($pool.manifest.bound_signer -and $pool.manifest.bound_signer -ne $wallet) { throw "A selected manifest is bound to another wallet." }
    }
    $request = @{
        id = "manual-cache-direct-$stamp"; wallet = $wallet; settlement = $settlementMint
        groups = @(@{ kind = "direct"; target = $target; pools = @($pools) })
        conversion = $null
        parameters = @{ use_flashloan = [bool]$UseFlashloan; minimum_profit = $MinimumProfit }
    }
    [System.IO.File]::WriteAllText($frozenRequest, ($request | ConvertTo-Json -Depth 30), $utf8)
    Write-Host "Manual persisted snapshot exported. This does not establish live Registry admission or refresh a manifest."
}
$reportPath = Join-Path $folder "report.json"
$logPath = Join-Path $folder "worker.log"
& $binary --v10-simulate $frozenRequest --wallet $WalletPath 1> $reportPath 2> $logPath
$simulationExit = $LASTEXITCODE
Write-Host "Request: $frozenRequest"
Write-Host "Report:  $reportPath"
Write-Host "Log:     $logPath"
if (Test-Path $reportPath) { Get-Content -Raw $reportPath | Write-Host }
if ($simulationExit -ne 0) { throw "V10 simulation failed. Inspect report.json and worker.log." }
$report = Get-Content -Raw $reportPath | ConvertFrom-Json
if (-not $report.executor_invoked) { throw "Simulation did not prove executor invocation." }
Write-Host "Simulation completed: $($report.outcome). No transaction was broadcast."
