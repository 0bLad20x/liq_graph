param(
    [ValidateRange(30, 86400)][int]$DurationSeconds = 600,
    [ValidateRange(0, 2147483647)][int]$PoolLimit = 1000,
    [switch]$Signals,
    [double]$ThresholdBps = 0,
    [switch]$TraceSignals,
    [switch]$TracePrices,
    [string]$WsProviders = "Configured",
    [ValidateRange(0, 2147483647)][int]$WsAccountBudget = 0,
    [ValidateRange(0, 64)][int]$WsConnectionsPerProvider = 0,
    [switch]$PlanOnly,
    [switch]$Rewatch
)

$ErrorActionPreference = "Stop"
if ([double]::IsNaN($ThresholdBps) -or [double]::IsInfinity($ThresholdBps) -or $ThresholdBps -lt 0) { throw "ThresholdBps must be finite and nonnegative." }
$label = if ($Signals) { "Graph-Signals" } else { "Market-State" }
$outputRoot = if ($Signals) { "graph_signals_live" } else { "market_state_live" }
$runFlag = if ($Signals) { "--graph-signals-live" } else { "--market-state-live" }
Set-Location (Split-Path -Parent $PSScriptRoot)
$branch = git branch --show-current
if ($LASTEXITCODE -ne 0) { throw "Cannot determine branch." }
$head = git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw "Cannot determine HEAD." }
$status = git status --short
if ($LASTEXITCODE -ne 0) { throw "Cannot inspect working tree." }
$tracked = git status --short --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $tracked) { throw "Commit or preserve tracked changes before the live run; nothing was changed by this script." }
if ($branch -notin @("agent/market-state-stream", "agent/graph-signals-reference", "main")) { throw "Use a Market-State/Graph-Signals feature branch, or main after merge." }
Write-Host "$label branch=$branch head=$head signals=$([bool]$Signals) execution=false"
if ($status) { Write-Host ($status | Out-String) }
cargo build --locked --release
if ($LASTEXITCODE -ne 0) { throw "cargo build --locked --release failed; no live run started." }
git diff --exit-code -- Cargo.lock
if ($LASTEXITCODE -ne 0) { throw "Cargo.lock changed during build; stop and inspect it." }
$tracked = git status --short --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $tracked) { throw "Tracked files changed during build; stop and inspect them." }

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ")
$folder = Join-Path (Get-Location) "$outputRoot\$stamp"
New-Item -ItemType Directory -Path $folder | Out-Null
$log = Join-Path $folder "worker.log"
$report = Join-Path $folder "report.json"
$metadata = Join-Path $folder "checkout.json"
$utf8 = [System.Text.UTF8Encoding]::new($false)
$checkout = @{ branch = $branch; head = $head; status = @($status); duration_seconds = $DurationSeconds; pool_limit = $PoolLimit; rewatch = [bool]$Rewatch; signals_enabled = [bool]$Signals; threshold_bps = $ThresholdBps; trace_prices = [bool]$TracePrices; trace_signals = [bool]$TraceSignals; ws_providers = $WsProviders; ws_account_budget = $WsAccountBudget; ws_connections_per_provider = $WsConnectionsPerProvider; plan_only = [bool]$PlanOnly }
[System.IO.File]::WriteAllText($metadata, ($checkout | ConvertTo-Json -Depth 5), $utf8)

$values = @{
    MARKET_STATE_DURATION_SECONDS = "$DurationSeconds"
    MARKET_STATE_POOL_LIMIT = "$PoolLimit"
    MARKET_STATE_TRACE = $(if ($TracePrices) { "1" } else { "0" })
    MARKET_STATE_LIVE_REWATCH = $(if ($Rewatch) { "true" } else { "false" })
    MARKET_STATE_REPORT_PATH = $report
    RPC_WS_PROVIDER_SELECTION = $WsProviders
    RPC_WS_ACCOUNT_BUDGET = "$WsAccountBudget"
    RPC_WS_CONNECTIONS_PER_PROVIDER = "$WsConnectionsPerProvider"
}
if ($Signals) {
    $values["GRAPH_SIGNALS_THRESHOLD_BPS"] = $ThresholdBps.ToString([System.Globalization.CultureInfo]::InvariantCulture)
    $values["GRAPH_SIGNALS_SAMPLE_LIMIT"] = "16"
    $values["GRAPH_SIGNALS_TRACE"] = $(if ($TraceSignals) { "true" } else { "false" })
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
    # The binary loads .env and resolves the same transport configuration as the live run.
    # This mode opens no RPC connection and reads no database; URLs/credentials are omitted.
    $planText = & $binary --rpc-ws-plan
    if ($LASTEXITCODE -ne 0) { throw "WebSocket configuration is invalid; no live run started." }
    $plan = ($planText -join [Environment]::NewLine) | ConvertFrom-Json
    $planPath = Join-Path $folder "websocket_plan.json"
    [System.IO.File]::WriteAllText($planPath, ($plan | ConvertTo-Json -Depth 8), $utf8)
    $checkout["websocket_plan"] = $plan
    [System.IO.File]::WriteAllText($metadata, ($checkout | ConvertTo-Json -Depth 10), $utf8)
    Write-Host "WebSocket selection=$($plan.selection) account_capacity=$($plan.account_capacity) sockets=$($plan.socket_count)"
    $plan.providers | Format-Table index, name, max_connections, max_subscriptions, subscriptions_per_connection | Out-Host
    if ($WsAccountBudget -gt 0) { Write-Host "The explicit account budget is a local test setting; provider acceptance is measured by the live run." }
    if ($PlanOnly) { Write-Host "Plan only: $planPath. No RPC subscriptions or database reads were started."; return }
    if ($plan.account_capacity -le 0) { throw "No WebSocket account providers selected. Use Configured flags, All, or explicit indices." }
    # stderr contains ordinary Rust log lines. Do not interpret those as PowerShell errors.
    $oldPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $binary $runFlag 2>&1 | Tee-Object -FilePath $log
        $workerExit = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $oldPreference
    }
    if ($workerExit -ne 0) { throw "$label live run failed (exit $workerExit). Inspect worker.log and report.json if present." }
    if (-not (Test-Path -LiteralPath $report)) { throw "No live evidence report was produced." }
    $evidence = Get-Content -LiteralPath $report -Raw | ConvertFrom-Json
    if ($evidence.live_evidence_passed -ne $true) { throw "Live evidence requirements were not fulfilled." }
    if ($Signals) {
        if ($evidence.signal_calculation_passed -ne $true -or $evidence.signal_candidate_passed -ne $true) { throw "No complete live signal proof; inspect signal_report and worker.log." }
        $signalReport = $evidence.signal_report
        if ($signalReport.execution_layout -ne "indexed_role_plan_v1") { throw "Unexpected signal execution layout; inspect checkout.json and worker.log." }
        if ($signalReport.combinations_visited -ne $signalReport.routes_expected -or
            $signalReport.routes_affected -ne $signalReport.routes_expected -or
            $signalReport.logarithms_prepared -ne $signalReport.price_evaluations -or
            $signalReport.websocket_combinations_visited -ne $signalReport.websocket_routes_expected -or
            $signalReport.websocket_logarithms_prepared -ne $signalReport.websocket_evaluations) { throw "Indexed signal accounting failed; inspect signal_report." }
        Write-Host "Graph-Signals indexed plan live evidence passed. Review per-kind coverage and complete latency in report.json."
    } else {
        Write-Host "Market-State live evidence passed. Review per-venue coverage in report.json before accepting the checkpoint."
    }
} finally {
    foreach ($name in $previous.Keys) {
        [Environment]::SetEnvironmentVariable($name, $previous[$name], "Process")
    }
}
