# One production pipeline for finite graph, MarketState, signal and V10 simulation proofs.
# Uses the local .env. Never broadcasts transactions or modifies provider credentials.
param(
    [ValidateRange(30,86400)][int]$DurationSeconds = 60,
    [ValidateRange(0,2147483647)][int]$PoolLimit = 15000,
    [ValidateRange(0,1000000)][int]$AccountBudget = 25000,
    [ValidateSet('Graph','Signals','MarketState')][string]$Stage = 'Signals',
    [string]$ExpectedBranch = '',
    [switch]$PlanOnly,
    [switch]$Rewatch,
    [switch]$GraphAudit,
    [switch]$RequireCatalogReconciliation,
    [switch]$TracePrices,
    [switch]$TraceSignals,
    [switch]$ProfileSignals,
    [ValidateRange(0,1000000)][double]$ThresholdBps = 0,
    [string]$WsProviders = 'Configured',
    [ValidateRange(0,2147483647)][int]$WsAccountBudget = 0,
    [ValidateRange(0,64)][int]$WsConnectionsPerProvider = 0,
    [switch]$SimulateV10,
    [string]$WalletPath,
    [ValidateRange(1,5000)][int]$SimulationLimit = 10,
    [ValidateSet('All','Direct2','Direct3','Triangle3','Triangle4')][string]$RouteKind = 'All',
    [string]$ExecutionParametersPath
)
$ErrorActionPreference = 'Stop'
$signals = $Stage -eq 'Signals'
$marketEnabled = $Stage -ne 'Graph'
if (($SimulateV10 -or $RequireCatalogReconciliation -or $TraceSignals -or $ProfileSignals) -and -not $signals) {
    throw 'Simulation and signal diagnostics require -Stage Signals.'
}
if (($Rewatch -or $TracePrices) -and -not $marketEnabled) { throw 'Rewatch and TracePrices require MarketState.' }
if ($Stage -eq 'Graph' -and -not $GraphAudit) { throw 'Stage Graph requires GraphAudit for actual graph/reference evidence.' }
if ($RequireCatalogReconciliation -and -not $GraphAudit) { throw 'RequireCatalogReconciliation requires GraphAudit.' }
if ([double]::IsNaN($ThresholdBps) -or [double]::IsInfinity($ThresholdBps)) { throw 'ThresholdBps must be finite.' }
if ($SimulateV10 -and -not $PlanOnly -and -not $WalletPath) { throw 'SimulateV10 requires WalletPath.' }

function Read-OptionalJson([string]$Path) {
    if (Test-Path -LiteralPath $Path) { return (Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json) }
    return $null
}
$repo = Split-Path -Parent $PSScriptRoot
$previous = @{}
Push-Location -LiteralPath $repo
try {
    $branch = (& git branch --show-current)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot read checked-out branch.' }
    $head = (& git rev-parse HEAD)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot read HEAD.' }
    $tracked = @(& git status --short --untracked-files=no)
    if ($LASTEXITCODE -ne 0 -or $tracked.Count -gt 0) { throw 'Tracked changes exist; preserve or commit them before running.' }
    if ($ExpectedBranch -and $branch -ne $ExpectedBranch) { throw "Expected '$ExpectedBranch', found '$branch'." }
    if ($SimulateV10 -and -not $PlanOnly) { $WalletPath = (Resolve-Path -LiteralPath $WalletPath).Path }
    if ($ExecutionParametersPath) { $ExecutionParametersPath = (Resolve-Path -LiteralPath $ExecutionParametersPath).Path }

    Write-Host "Runtime checkout: $branch $head"
    & cargo build --locked --release
    if ($LASTEXITCODE -ne 0) { throw 'cargo build --locked --release failed; no live run started.' }
    & git diff --exit-code -- Cargo.lock
    if ($LASTEXITCODE -ne 0) { throw 'Build changed Cargo.lock; stop and inspect.' }
    $tracked = @(& git status --short --untracked-files=no)
    if ($LASTEXITCODE -ne 0 -or $tracked.Count -gt 0) { throw 'Build changed tracked files; stop and inspect.' }
    $binary = Join-Path $repo 'target\release\liq_graph.exe'
    if (-not (Test-Path -LiteralPath $binary)) { throw "Missing executable: $binary" }

    $stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ')
    $folder = Join-Path $repo "runtime_live\$stamp"
    New-Item -Path $folder -ItemType Directory -Force | Out-Null
    $log = Join-Path $folder 'runtime.log'
    $runtimePath = Join-Path $folder 'runtime.json'
    $marketPath = Join-Path $folder 'market_state.json'
    $producerPath = Join-Path $folder 'signal_production.json'
    $consumerPath = Join-Path $folder 'signal_consumer.json'
    $executionPath = Join-Path $folder 'execution.json'
    $auditPath = Join-Path $folder 'graph_pool_audit.jsonl'
    $values = @{
        RUN_DURATION_SECONDS = "$DurationSeconds"
        RUN_SHUTDOWN_SECONDS = '360'
        RUN_SIGNAL_PROOF = "$($signals -and -not $SimulateV10)".ToLowerInvariant()
        RUN_REWATCH = "$([bool]$Rewatch)".ToLowerInvariant()
        RUN_FULL_SCAN = 'false'
        IDENTITY_HYDRATION_ONLY = 'false'
        RUN_REPORT_PATH = $runtimePath
        RUN_MARKET_REPORT_PATH = $marketPath
        MARKET_STATE_ENABLED = "$marketEnabled".ToLowerInvariant()
        GRAPH_SIGNALS_ENABLED = "$signals".ToLowerInvariant()
        CP1_MARKET_ENRICHMENT_ENABLED = 'true'
        MARKET_STATE_POOL_LIMIT = "$PoolLimit"
        MARKET_STATE_WS_ACCOUNT_BUDGET = "$AccountBudget"
        MARKET_STATE_SELECTION_INTERVAL_MS = '5000'
        MARKET_STATE_HYDRATION_CONCURRENCY = '8'
        MINT_ONCHAIN_PARALLEL_BATCHES = '4'
        MARKET_STATE_TRACE = $(if ($TracePrices) { '1' } else { '0' })
        RPC_WS_PROVIDER_SELECTION = $WsProviders
        RPC_WS_ACCOUNT_BUDGET = "$WsAccountBudget"
        RPC_WS_CONNECTIONS_PER_PROVIDER = "$WsConnectionsPerProvider"
        GRAPH_SIGNALS_THRESHOLD_BPS = $ThresholdBps.ToString([System.Globalization.CultureInfo]::InvariantCulture)
        GRAPH_SIGNALS_TRACE = "$([bool]$TraceSignals)".ToLowerInvariant()
        GRAPH_SIGNALS_PROFILE = "$([bool]$ProfileSignals)".ToLowerInvariant()
        GRAPH_SIGNALS_SAMPLE_LIMIT = '16'
        GRAPH_RUNTIME_PRODUCTION_REPORT_PATH = $producerPath
        GRAPH_RUNTIME_SIGNAL_REPORT_PATH = $consumerPath
        GRAPH_EXECUTION_SIMULATE = "$([bool]$SimulateV10)".ToLowerInvariant()
        GRAPH_EXECUTION_WALLET_PATH = $WalletPath
        GRAPH_EXECUTION_MAX_REQUESTS = "$SimulationLimit"
        GRAPH_EXECUTION_ROUTE_KIND = $RouteKind
        GRAPH_EXECUTION_PARAMETERS_PATH = $ExecutionParametersPath
        GRAPH_EXECUTION_REPORT_PATH = $executionPath
        GRAPH_DIAGNOSTIC_JSONL_PATH = $(if ($GraphAudit) { $auditPath } else { $null })
        CP6C_REFERENCE_EVERY = $(if ($GraphAudit -and $Stage -eq 'Graph') { '1' } elseif ($GraphAudit) { '10' } else { '0' })
        LIQ_GRAPH_LOG = (@(
            'warn','liq_graph::orchestration::runtime=info',
            'liq_graph::workers::persistence=info','liq_graph::workers::pool_registry=info',
            'liq_graph::workers::completion_lifecycle=info','liq_graph::workers::graph_liquidity=info',
            'liq_graph::workers::graph_watch_catalog=info','liq_graph::workers::market_state=info',
            'liq_graph::workers::mint_extensions=info','liq_graph::workers::mint_qualification=info',
            'liq_graph::workers::graph_signals=info','liq_graph::workers::execution_candidates=info',
            'liq_graph::workers::execution_v10=info','liq_graph::workers::rpc_transport=info'
        ) -join ',')
    }
    foreach ($name in $values.Keys) {
        $previous[$name] = [Environment]::GetEnvironmentVariable($name,'Process')
        [Environment]::SetEnvironmentVariable($name,$values[$name],'Process')
    }
    # The binary resolves exactly the same settings as --run, without starting workers.
    $planText = & $binary --run-plan
    if ($LASTEXITCODE -ne 0) { throw 'Invalid runtime plan; no live run started.' }
    $plan = ($planText -join [Environment]::NewLine) | ConvertFrom-Json
    $plan | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $folder 'plan.json') -Encoding UTF8
    $plan.websocket_plan | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $folder 'websocket_plan.json') -Encoding UTF8
    $checkout = [ordered]@{
        branch=$branch; head=$head; start_ms=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        duration_seconds=$DurationSeconds; pool_limit=$PoolLimit; ws_account_budget=$AccountBudget
        stage=$Stage; rewatch=[bool]$Rewatch; simulation_enabled=[bool]$SimulateV10
        simulation_limit=$SimulationLimit; route_kind=$RouteKind; graph_audit=[bool]$GraphAudit
        require_catalog_reconciliation=[bool]$RequireCatalogReconciliation
        plan_only=[bool]$PlanOnly; transactions_sent=0; plan=$plan
    }
    $checkout | ConvertTo-Json -Depth 14 | Set-Content -LiteralPath (Join-Path $folder 'checkout.json') -Encoding UTF8
    Write-Host "Output: $folder"
    Write-Host "Pipeline=runtime completion_refresh=true stage=$Stage simulation=$([bool]$SimulateV10) broadcasts=false"
    Write-Host "WS accounts=$($plan.websocket_plan.account_capacity) sockets=$($plan.websocket_plan.socket_count)"
    if ($PlanOnly) {
        @{ plan_only=$true; runtime_started=$false; orchestration_evidence_passed=$false } |
            ConvertTo-Json | Set-Content -LiteralPath (Join-Path $folder 'summary.json') -Encoding UTF8
        Write-Host 'Plan only: no RPC connections, database reads or live acceptance.'
        return
    }
    if ($marketEnabled -and $plan.websocket_plan.account_capacity -le 0) { throw 'No WebSocket account capacity selected.' }
    Write-Host "Starting $DurationSeconds-second live run; full log: $log"
    $clock = [System.Diagnostics.Stopwatch]::StartNew()
    $priorPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $binary --run 2>&1 | Out-File -FilePath $log -Encoding utf8
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $priorPreference
    }
    $clock.Stop()
    $runtime = Read-OptionalJson $runtimePath
    $market = Read-OptionalJson $marketPath
    $execution = Read-OptionalJson $executionPath
    $production = Read-OptionalJson $producerPath
    $refreshes = @(Select-String -LiteralPath $log -Pattern 'lifecycle (unchanged request_id=.* work=Refresh state=Prepared|accepted request_id=.* work=Refresh status=Prepared)').Count
    $lifecycleStarted = [bool](Select-String -LiteralPath $log -Pattern 'lifecycle started' -Quiet)
    $candidateDelivery = if ($SimulateV10) { $null -ne $execution -and
        $execution.execution_candidates.lost_events -eq 0 -and
        $execution.execution_candidates.awaiting_snapshot -eq $false -and
        $runtime.orchestration_evidence_passed -eq $true } else { $null }
    $orchestrationPassed = $exitCode -eq 0 -and $null -ne $runtime -and $runtime.run_complete -eq $true -and
        $runtime.orchestration_evidence_passed -eq $true -and $lifecycleStarted -and
        (-not $marketEnabled -or $refreshes -gt 0) -and
        (-not $SimulateV10 -or $candidateDelivery) -and $clock.Elapsed.TotalSeconds -ge ($DurationSeconds - 1)
    $graphPassed = $null
    if ($GraphAudit) {
        $analyzerArgs = @('--audit',$auditPath,'--runtime',$log,'--output',$folder,
            '--exit-code',"$exitCode",'--min-duration',"$DurationSeconds",
            '--summary-name','graph_summary.json')
        if ($RequireCatalogReconciliation) { $analyzerArgs += '--require-catalog-reconciliation' }
        & python (Join-Path $PSScriptRoot 'graph_live_analyze.py') @analyzerArgs
        $graphPassed = $LASTEXITCODE -eq 0
    }
    $requests = if ($null -ne $execution.execution_candidates.requests) { @($execution.execution_candidates.requests) } else { @() }
    $reports = if ($null -ne $execution.execution_reports) { @($execution.execution_reports) } else { @() }
    $summary = [ordered]@{
        branch=$branch; head=$head; pipeline='runtime'; stage=$Stage; exit_code=$exitCode
        diagnostic_consumers=$runtime.diagnostic_consumers; signal_proof=$runtime.signal_proof
        runtime_completed=($null -ne $runtime -and $runtime.run_complete -eq $true)
        orchestration_evidence_passed=[bool]$orchestrationPassed; runtime_evidence=$runtime
        elapsed_seconds=[Math]::Round($clock.Elapsed.TotalSeconds,2)
        requested_duration_seconds=$DurationSeconds; completion_lifecycle_started=$lifecycleStarted
        prepared_refreshes_observed=$refreshes; graph_audit_passed=$graphPassed
        market_state_observations=$market.observations; websocket_observations=$market.websocket_observations
        post_rewatch_websocket_observations=$market.post_rewatch_websocket_observations
        market_account_processing_ns=$market.metrics.processing
        market_queue_ns=$market.metrics.queue
        signal_steady_state_processing_ns=$production.steady_state_inputs.processing_ns
        frame_to_signal_state_us=$production.steady_state_inputs.frame_to_state_ready_us
        candidate_delivery_passed=$candidateDelivery
        v10_requests=$requests.Count; v10_reports=$reports.Count
        v10_simulation_successes=$execution.simulation_successes
        v10_simulation_failures=$execution.simulation_failures
        v10_dex_call_reports=$execution.simulation_dex_calls_observed
        execution_integration_passed=$execution.execution_integration_passed
        transactions_sent=0; artifacts=$folder
    }
    $summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $folder 'summary.json') -Encoding UTF8
    $summary | ConvertTo-Json -Depth 10 | Write-Host
    if (-not $orchestrationPassed) { throw "Orchestrierungsnachweis unvollstaendig; Details: $folder" }
    Write-Host "Orchestrierungsnachweis bestanden. Artefakte: $folder"
    if ($SimulateV10) {
        Write-Host "V10: Uebergabe an Simulation nachgewiesen; erfolgreiche Simulationen=$($execution.simulation_successes), fehlgeschlagene=$($execution.simulation_failures), DEX-Aufrufberichte=$($execution.simulation_dex_calls_observed)."
        Write-Host 'Erfolgreiche Handelsausfuehrung und Profitabilitaet bleiben gesondert nachzuweisen (CP8).'
    } else {
        Write-Host 'V10-Simulation war in diesem Lauf deaktiviert.'
    }
    if ($GraphAudit -and -not $graphPassed) { throw "Zusaetzlich angeforderter Graphaudit unvollstaendig; Orchestrierung bestanden. Details: $folder" }
    if ($GraphAudit) { Write-Host 'Graphnachweis bestanden: reale Uebergaenge und exakter Routen-/Indexreferenzvergleich.' }
} finally {
    # Only restore values actually saved, including after a guard/build failure.
    foreach ($name in $previous.Keys) {
        [Environment]::SetEnvironmentVariable($name,$previous[$name],'Process')
    }
    Pop-Location
}
