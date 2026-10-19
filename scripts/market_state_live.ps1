param(
    [ValidateRange(30, 86400)][int]$DurationSeconds = 600,
    [ValidateRange(0, 2147483647)][int]$PoolLimit = 1000,
    [switch]$TracePrices,
    [switch]$Rewatch
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$branch = git branch --show-current
if ($LASTEXITCODE -ne 0) { throw "Cannot determine branch." }
$head = git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw "Cannot determine HEAD." }
$status = git status --short
if ($LASTEXITCODE -ne 0) { throw "Cannot inspect working tree." }
$tracked = git status --short --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $tracked) { throw "Commit or preserve tracked changes before the live run; nothing was changed by this script." }
if ($branch -notin @("agent/market-state-stream", "main")) { throw "Use agent/market-state-stream, or main after merge." }
Write-Host "Market-State branch=$branch head=$head execution=false"
if ($status) { Write-Host ($status | Out-String) }
cargo build --locked --release
if ($LASTEXITCODE -ne 0) { throw "cargo build --locked --release failed; no live run started." }
git diff --exit-code -- Cargo.lock
if ($LASTEXITCODE -ne 0) { throw "Cargo.lock changed during build; stop and inspect it." }
$tracked = git status --short --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $tracked) { throw "Tracked files changed during build; stop and inspect them." }

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ")
$folder = Join-Path (Get-Location) "market_state_live\$stamp"
New-Item -ItemType Directory -Path $folder | Out-Null
$log = Join-Path $folder "worker.log"
$report = Join-Path $folder "report.json"
$metadata = Join-Path $folder "checkout.json"
$utf8 = [System.Text.UTF8Encoding]::new($false)
$checkout = @{ branch = $branch; head = $head; status = @($status); duration_seconds = $DurationSeconds; pool_limit = $PoolLimit; rewatch = [bool]$Rewatch }
[System.IO.File]::WriteAllText($metadata, ($checkout | ConvertTo-Json -Depth 5), $utf8)

$values = @{
    MARKET_STATE_DURATION_SECONDS = "$DurationSeconds"
    MARKET_STATE_POOL_LIMIT = "$PoolLimit"
    MARKET_STATE_TRACE = $(if ($TracePrices) { "1" } else { "0" })
    MARKET_STATE_LIVE_REWATCH = $(if ($Rewatch) { "true" } else { "false" })
    MARKET_STATE_REPORT_PATH = $report
}
# Optional ACK logs demonstrate actual unsubscriptions during a real-pool rewatch.
if ($Rewatch) {
    $level = if ($env:LIQ_GRAPH_LOG) { $env:LIQ_GRAPH_LOG } else { "info" }
    $values["LIQ_GRAPH_LOG"] = "$level,rpc_transport=debug"
}
$previous = @{}
foreach ($name in $values.Keys) {
    $previous[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
    [Environment]::SetEnvironmentVariable($name, $values[$name], "Process")
}
try {
    Write-Host "Report: $report"
    Write-Host "Log:    $log"
    $binary = Join-Path (Get-Location) "target\release\liq_graph.exe"
    # stderr contains ordinary Rust log lines. Do not interpret those as PowerShell errors.
    $oldPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $binary --market-state-live 2>&1 | Tee-Object -FilePath $log
        $workerExit = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $oldPreference
    }
    if ($workerExit -ne 0) { throw "Market-State live run failed (exit $workerExit). Inspect worker.log and report.json if present." }
    if (-not (Test-Path -LiteralPath $report)) { throw "No live evidence report was produced." }
    $evidence = Get-Content -LiteralPath $report -Raw | ConvertFrom-Json
    if ($evidence.live_evidence_passed -ne $true) { throw "Live evidence requirements were not fulfilled." }
    Write-Host "Market-State live evidence passed. Review per-venue coverage in report.json before accepting the checkpoint."
} finally {
    foreach ($name in $previous.Keys) {
        [Environment]::SetEnvironmentVariable($name, $previous[$name], "Process")
    }
}
