param(
    [ValidateRange(30, 86400)][int]$DurationSeconds = 600,
    [ValidateRange(0, 2147483647)][int]$PoolLimit = 1000,
    [switch]$Signals,
    [double]$ThresholdBps = 0,
    [switch]$TraceSignals,
    [switch]$ProfileSignals,
    [switch]$TracePrices,
    [string]$WsProviders = "Configured",
    [ValidateRange(0, 2147483647)][int]$WsAccountBudget = 0,
    [ValidateRange(0, 64)][int]$WsConnectionsPerProvider = 0,
    [switch]$PlanOnly,
    [switch]$Rewatch
)

$ErrorActionPreference = "Stop"
function Write-RunLog([string]$Message) {
    $timestamp = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    Write-Host "ts_ms=$timestamp level=INFO module=liq_graph::scripts::market_state_live $Message"
}
if ($ProfileSignals -and -not $Signals) { throw "ProfileSignals requires the Graph-Signals runner." }
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
Write-RunLog "$label branch=$branch head=$head signals=$([bool]$Signals) execution=false"
if ($status) { Write-RunLog "untracked_or_local_entries=$(@($status).Count)" }
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
$checkout = @{ branch = $branch; head = $head; status = @($status); duration_seconds = $DurationSeconds; pool_limit = $PoolLimit; rewatch = [bool]$Rewatch; signals_enabled = [bool]$Signals; threshold_bps = $ThresholdBps; trace_prices = [bool]$TracePrices; trace_signals = [bool]$TraceSignals; profile_signals = [bool]$ProfileSignals; ws_providers = $WsProviders; ws_account_budget = $WsAccountBudget; ws_connections_per_provider = $WsConnectionsPerProvider; plan_only = [bool]$PlanOnly }
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
    $values["GRAPH_SIGNALS_PROFILE"] = $(if ($ProfileSignals) { "true" } else { "false" })
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
    Write-RunLog "report=$report worker_log=$log"
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
    Write-RunLog "websocket_selection=$($plan.selection) account_capacity=$($plan.account_capacity) sockets=$($plan.socket_count)"
    foreach ($provider in $plan.providers) {
        Write-RunLog "ws_provider=$($provider.name) index=$($provider.index) connections=$($provider.max_connections) subscriptions=$($provider.max_subscriptions) per_connection=$($provider.subscriptions_per_connection)"
    }
    if ($WsAccountBudget -gt 0) { Write-RunLog "explicit_ws_budget=$WsAccountBudget provider_acceptance_requires_live_run=true" }
    if ($PlanOnly) { Write-RunLog "plan_only=true path=$planPath rpc_connections=false database_reads=false"; return }
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
    if (-not (Test-Path -LiteralPath $report)) {
        # A crash/forced process termination cannot return worker measurements.
        $partial = @{ run_complete = $false; live_evidence_passed = $false; worker_exit = $workerExit; error = "Worker produced no report; inspect worker.log." }
        [System.IO.File]::WriteAllText($report, ($partial | ConvertTo-Json), $utf8)
    }
    $evidence = Get-Content -LiteralPath $report -Raw | ConvertFrom-Json
    Write-RunLog "run_complete=$($evidence.run_complete) live_evidence_passed=$($evidence.live_evidence_passed) report=$report"
    if ($Signals) {
        $produced = $evidence.signal_production_report
        $received = $evidence.signal_report
        Write-RunLog "signals_published=$($produced.counts.events) signals_received=$($received.counts.events) ranges_published=$($produced.counts.ranges) ranges_received=$($received.counts.ranges) lost=$($received.lost_events) delivery_passed=$($evidence.signal_delivery_passed) reference_comparison=false"
        Write-RunLog "state_ready_mean_ns=$($produced.steady_state_inputs.state_ready_ns.mean) state_ready_max_ns=$($produced.steady_state_inputs.state_ready_ns.max) query_mean_ns=$($produced.steady_state_inputs.query_ns.mean) processing_max_ns=$($produced.steady_state_inputs.processing_ns.max)"
        if ($ProfileSignals) { Write-RunLog "profile=true details=report.json websocket_inputs=true" }
    }
    if ($workerExit -ne 0) { throw "$label live run failed (exit $workerExit). Diagnostics: $report and $log" }
    if ($evidence.run_complete -ne $true) { throw "Live run did not complete; inspect report.json and worker.log." }
    $deliveryWarning = $Signals -and $evidence.signal_output_loss_warning -eq $true -and $received.lost_events -gt 0
    if ($evidence.live_evidence_passed -ne $true -and -not $deliveryWarning) { throw "Live evidence requirements were not fulfilled." }
    if ($Signals) {
        if (-not $deliveryWarning -and ($evidence.signal_delivery_passed -ne $true -or $evidence.signal_calculation_passed -ne $true)) { throw "No complete live state/query measurement; inspect both signal reports and worker.log." }
        $signalReport = $evidence.signal_report
        if ($signalReport.execution_layout -ne "factorized_roles_v1" -or $signalReport.output_layout -ne "versioned_state_ranges_v1") { throw "Unexpected signal execution layout; inspect checkout.json and worker.log." }
        if ($evidence.reference_comparison_performed -ne $false -or $signalReport.reference_comparison_performed -ne $false) { throw "Unexpected reference comparison in measurement run." }
        if ((-not $deliveryWarning -and $produced.counts.ranges -ne $signalReport.counts.ranges) -or
            $signalReport.output_capacity -ne 256 -or $signalReport.max_ranges_per_block -ne 64) { throw "Signal range delivery or output bounds differ; inspect both signal reports." }
        $pipeline = $produced.websocket_inputs
        foreach ($stage in @("transport_decode_us", "transport_to_market_us", "market_processing_us", "price_queue_us", "frame_to_worker_us")) {
            if ($pipeline.$stage.count -ne $produced.counts.websocket_evaluations) { throw "Incomplete WebSocket pipeline timing: $stage" }
        }
        # Producer accounting stays mandatory even if the output observer missed events.
        $counts = $produced.counts
        if ($counts.logarithms_prepared -ne $counts.price_evaluations -or
            $counts.state_updates -ne $counts.evaluations -or
            $counts.price_evaluations -ne $evidence.report.observations -or
            $counts.websocket_evaluations -ne $evidence.report.websocket_observations -or
            $counts.ranges -ne ($counts.entered_ranges + $counts.updated_ranges + $counts.exited_ranges)) { throw "Factorized signal accounting failed; inspect signal_production_report." }
        if ($deliveryWarning) {
            Write-Warning "Measurement run completed with signal output loss: lost=$($received.lost_events) incidents=$($received.lag_incidents) resynchronizations=$($received.resynchronizations) awaiting_snapshot=$($received.awaiting_snapshot) unverified_ranges=$($received.unverified_ranges). Delivery/acceptance failed; producer timings are retained in report.json."
        }
        Write-RunLog "graph_signals_measurement_passed=$($evidence.signal_measurement_passed) candidate_observed=$($evidence.signal_candidate_passed) reference_comparison=false details=report.json"
    } else {
        Write-RunLog "market_state_live_passed=true details=report.json"
    }
} finally {
    foreach ($name in $previous.Keys) {
        [Environment]::SetEnvironmentVariable($name, $previous[$name], "Process")
    }
}
