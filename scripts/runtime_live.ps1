# One production pipeline for finite graph, MarketState, signal and V10 simulation proofs.
# Uses the local .env. Never broadcasts transactions or modifies provider credentials.
param(
    [ValidateRange(30,86400)][int]$DurationSeconds = 60,
    [ValidateRange(0,2147483647)][int]$PoolLimit = 15000,
    [ValidateRange(0,1000000)][int]$AccountBudget = 25000,
    [ValidateRange(0,1000000000000)][double]$MinPoolTvlUsd = 500,
    [ValidateRange(0,256)][int]$ExecutionDetailLimit = 16,
    [ValidateSet('Graph','Signals','MarketState')][string]$Stage = 'Signals',
    [string]$ExpectedBranch = '',
    [switch]$PlanOnly,
    [ValidateRange(0,86400)][int]$CompletionRefreshSeconds = 0,
    [ValidateRange(0,86400)][int]$CompletionRecheckSeconds = 0,
    [switch]$Rewatch,
    [switch]$GraphAudit,
    [switch]$RequireCatalogReconciliation,
    [switch]$TracePrices,
    [switch]$TraceSignals,
    [switch]$ProfileSignals,
    # Required marginal DEX-net advantage; network/rent costs are separate.
    [ValidateRange(0,1000000)][double]$ThresholdBps = 0,
    # CP9A: fixed 100 USD/SOL proxy, per-direction notional prequalification.
    [ValidateRange(0,1000000)][double]$MinDirectionNotionalUsd = 1,
    [ValidateSet('Off','Shadow','Filter')][string]$DirectionLiquidityMode = 'Filter',
    [string]$WsProviders = 'Configured',
    [ValidateRange(0,2147483647)][int]$WsAccountBudget = 0,
    [ValidateRange(0,64)][int]$WsConnectionsPerProvider = 0,
    [switch]$RequireFeeEvidence,
    [switch]$RequireLiquidityEvidence,
    [switch]$RequireAmountEvidence,
    [switch]$RequirePreparedEvidence,
    [switch]$RequireCalibrationEvidence,
    # Diagnostic raw snapshots only. This does not pin or filter pool selection.
    [string]$LiquidityEvidencePools = '',
    # Overrides the single ENV allowlist for the complete pipeline.
    [string]$EnabledVenues = '',
    [switch]$SimulateV10,
    [string]$WalletPath,
    [ValidateRange(0,2147483647)][int]$SimulationLimit = 0,
    [ValidateSet('All','Direct2','Direct3','Direct2Direct3','Triangle3','Triangle4')][string]$RouteKind = 'All',
    # CP9B: exclude non-Direct2 shapes already in the liquidity graph.
    [switch]$Direct2Only,
    # CP9E: graph/selector admit exactly Direct2 + Direct3, not Triangle3/4.
    [switch]$Direct2Direct3Only,
    # Concurrent in-flight simulateTransaction jobs (1 restores serial baseline).
    [ValidateRange(1,64)][int]$V10Parallelism = 4,
    # CP9C: compare same-revision Direct2 V10 payload strategies.
    [ValidateSet('Baseline','Contrarian','Mixed','All','Coverage','AllCoverage')][string]$CP9CPackingMode = 'Baseline',
    # CP9D: one fresh source plus rotating active, still-positive distinct pairs.
    [ValidateRange(1,8)][int]$CP9DMaxPairs = 1,
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
if ($RequireFeeEvidence -and -not $signals) { throw 'Fee evidence requires Stage Signals.' }
if (($RequireLiquidityEvidence -or $LiquidityEvidencePools) -and -not $signals) { throw 'Liquidity evidence requires Stage Signals.' }
if ($RequireAmountEvidence -and (-not $signals -or -not $SimulateV10)) { throw 'Amount evidence requires Stage Signals and SimulateV10.' }
if ($RequirePreparedEvidence -and -not $marketEnabled) { throw 'Prepared evidence requires MarketState.' }
if ($RequireCalibrationEvidence -and -not $SimulateV10) { throw 'Calibration evidence requires SimulateV10.' }
if ($RequireCatalogReconciliation -and -not $GraphAudit) { throw 'RequireCatalogReconciliation requires GraphAudit.' }
if ($Direct2Only -and $Direct2Direct3Only) { throw 'Choose Direct2Only or Direct2Direct3Only, not both.' }
if ($Direct2Only -and $RouteKind -notin @('All','Direct2')) { throw 'Direct2Only requires RouteKind All or Direct2.' }
if ($Direct2Direct3Only -and $RouteKind -ne 'Direct2Direct3') {
    throw 'Direct2Direct3Only requires RouteKind Direct2Direct3.'
}
if ($RouteKind -eq 'Direct2Direct3' -and -not $Direct2Direct3Only) {
    throw 'RouteKind Direct2Direct3 requires -Direct2Direct3Only (graph restriction).'
}
if (($CP9CPackingMode -ne 'Baseline' -or $CP9DMaxPairs -gt 1) -and
    (-not (($Direct2Only -and $RouteKind -eq 'Direct2') -or
       ($Direct2Direct3Only -and $RouteKind -eq 'Direct2Direct3')) -or
       -not $SimulateV10 -or -not $signals)) {
    throw 'CP9D/CP9E comparisons require restricted Direct2 or Direct2Direct3 graph and V10 simulation.'
}
if ([double]::IsNaN($ThresholdBps) -or [double]::IsInfinity($ThresholdBps)) { throw 'ThresholdBps must be finite.' }
if ([double]::IsNaN($MinDirectionNotionalUsd) -or [double]::IsInfinity($MinDirectionNotionalUsd)) { throw 'MinDirectionNotionalUsd must be finite.' }
if ([double]::IsNaN($MinPoolTvlUsd) -or [double]::IsInfinity($MinPoolTvlUsd)) { throw 'MinPoolTvlUsd must be finite.' }
if ($SimulateV10 -and -not $PlanOnly -and -not $WalletPath) { throw 'SimulateV10 requires WalletPath.' }

function Read-OptionalJson([string]$Path) {
    if (Test-Path -LiteralPath $Path) {
        if ((Get-Item -LiteralPath $Path).Length -gt 16MB) { throw "Expected a compact JSON report: $Path. Analyze legacy execution files with execution_analyze.py." }
        return (Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json)
    }
    return $null
}
function Measure-AmountStage($Report) {
    $n = [long]$Report.attempts
    if ($n -eq 0) { return [ordered]@{count=0; mean_ns=$null; p95_upper_ns=$null; p99_upper_ns=$null; max_ns=$null} }
    $p95 = $null; $p99 = $null; $seen = 0L
    for ($i = 0; $i -lt $Report.processing_ns_histogram.Count; $i++) {
        $seen += [long]$Report.processing_ns_histogram[$i]
        $upper = [Math]::Min([Math]::Pow(2,$i+1)-1,[double]$Report.processing_ns_max)
        if ($null -eq $p95 -and $seen -ge [Math]::Ceiling($n*0.95)) { $p95 = [long]$upper }
        if ($null -eq $p99 -and $seen -ge [Math]::Ceiling($n*0.99)) { $p99 = [long]$upper; break }
    }
    return [ordered]@{count=$n; mean_ns=([double]$Report.processing_ns_total/$n); p95_upper_ns=$p95; p99_upper_ns=$p99; max_ns=$Report.processing_ns_max}
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
        COMPLETION_LIFECYCLE_POOL_LIMIT = '0'
        POOL_REGISTRY_ADMISSION_AUDIT = $(if ($GraphAudit) { '1' } else { '0' })
        IDENTITY_HYDRATION_ONLY = 'false'
        RUN_REPORT_PATH = $runtimePath
        RUN_MARKET_REPORT_PATH = $marketPath
        MARKET_STATE_ENABLED = "$marketEnabled".ToLowerInvariant()
        GRAPH_SIGNALS_ENABLED = "$signals".ToLowerInvariant()
        CP1_MARKET_ENRICHMENT_ENABLED = 'true'
        MARKET_STATE_POOL_LIMIT = "$PoolLimit"
        POOL_MIN_TVL_USD = $MinPoolTvlUsd.ToString([System.Globalization.CultureInfo]::InvariantCulture)
        RUN_EXECUTION_DETAIL_LIMIT = "$ExecutionDetailLimit"
        MARKET_STATE_WS_ACCOUNT_BUDGET = "$AccountBudget"
        MARKET_STATE_SELECTION_INTERVAL_MS = '5000'
        MARKET_STATE_HYDRATION_CONCURRENCY = '8'
        MINT_ONCHAIN_PARALLEL_BATCHES = '4'
        MARKET_STATE_TRACE = $(if ($TracePrices) { '1' } else { '0' })
        MARKET_STATE_LIQUIDITY_EVIDENCE_POOLS = $LiquidityEvidencePools
        RPC_WS_PROVIDER_SELECTION = $WsProviders
        RPC_WS_ACCOUNT_BUDGET = "$WsAccountBudget"
        RPC_WS_CONNECTIONS_PER_PROVIDER = "$WsConnectionsPerProvider"
        GRAPH_SIGNALS_THRESHOLD_BPS = $ThresholdBps.ToString([System.Globalization.CultureInfo]::InvariantCulture)
        GRAPH_DIRECT2_ONLY = "$([bool]$Direct2Only)".ToLowerInvariant()
        GRAPH_DIRECT2_DIRECT3_ONLY = "$([bool]$Direct2Direct3Only)".ToLowerInvariant()
        GRAPH_MIN_DIRECTION_NOTIONAL_USD = $MinDirectionNotionalUsd.ToString([System.Globalization.CultureInfo]::InvariantCulture)
        GRAPH_DIRECTION_LIQUIDITY_MODE = $DirectionLiquidityMode.ToLowerInvariant()
        GRAPH_SIGNALS_TRACE = "$([bool]$TraceSignals)".ToLowerInvariant()
        GRAPH_SIGNALS_PROFILE = "$([bool]$ProfileSignals)".ToLowerInvariant()
        GRAPH_SIGNALS_SAMPLE_LIMIT = '16'
        GRAPH_RUNTIME_PRODUCTION_REPORT_PATH = $producerPath
        GRAPH_RUNTIME_SIGNAL_REPORT_PATH = $consumerPath
        GRAPH_EXECUTION_SIMULATE = "$([bool]$SimulateV10)".ToLowerInvariant()
        GRAPH_V10_SIMULATION_PARALLELISM = "$V10Parallelism"
        GRAPH_CP9C_PACKING_MODE = $(if ($CP9CPackingMode -eq 'AllCoverage') { 'all_coverage' }
            else { $CP9CPackingMode.ToLowerInvariant() })
        GRAPH_CP9D_MAX_DISTINCT_PAIRS = "$CP9DMaxPairs"
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
            'liq_graph::workers::completion::lifecycle=info','liq_graph::orchestration::completion=info',
            'liq_graph::workers::graph_liquidity=info',
            'liq_graph::workers::graph_watch_catalog=info','liq_graph::workers::market_state=info',
            'liq_graph::workers::mint_extensions=info','liq_graph::workers::mint_qualification=info',
            'liq_graph::workers::graph_signals=info','liq_graph::workers::execution_candidates=info',
            'liq_graph::workers::execution_v10=info','liq_graph::workers::rpc_transport=info',
            'liq_graph::workers::rpc_transport::ws=info'
        ) -join ',')
    }
    if ($EnabledVenues) { $values.GRAPH_ENABLED_VENUES = $EnabledVenues }
    if ($CompletionRefreshSeconds -gt 0) { $values.COMPLETION_LIFECYCLE_REFRESH_SECONDS = "$CompletionRefreshSeconds" }
    if ($CompletionRecheckSeconds -gt 0) { $values.COMPLETION_LIFECYCLE_RECHECK_SECONDS = "$CompletionRecheckSeconds" }
    if ($SimulateV10 -and -not $PlanOnly) {
        & python -c "import sys; assert sys.version_info >= (3, 10)"
        if ($LASTEXITCODE -ne 0) { throw 'Python 3.10+ is required for streaming execution evidence.' }
        # Resolve only the public signer. The keypair remains local and is never logged.
        $walletPublicKey = & $binary --v10-wallet $WalletPath
        if ($LASTEXITCODE -ne 0 -or @($walletPublicKey).Count -ne 1) { throw 'Cannot resolve execution wallet public key.' }
        $values.PUMPSWAP_V10_WALLET = "$walletPublicKey".Trim()
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
        minimum_pool_tvl_usd=$MinPoolTvlUsd; execution_detail_limit=$ExecutionDetailLimit
        stage=$Stage; rewatch=[bool]$Rewatch; simulation_enabled=[bool]$SimulateV10
        signal_score_basis='marginal_dex_net'; signal_net_threshold_bps=$ThresholdBps
        direction_liquidity_mode=$DirectionLiquidityMode.ToLowerInvariant()
        minimum_direction_notional_usd=$MinDirectionNotionalUsd; fixed_wsol_usd_reference=100
        profile_signals=[bool]$ProfileSignals
        require_fee_evidence=[bool]$RequireFeeEvidence; enabled_venues=@($plan.enabled_venues)
        require_liquidity_evidence=[bool]$RequireLiquidityEvidence
        liquidity_evidence_pools=$LiquidityEvidencePools; liquidity_model='dlmm_execution_window_v2'
        require_amount_evidence=[bool]$RequireAmountEvidence; amount_check='observed_simple_cycles_dex_only'
        require_prepared_evidence=[bool]$RequirePreparedEvidence
        require_calibration_evidence=[bool]$RequireCalibrationEvidence
        simulation_limit=$SimulationLimit; route_kind=$RouteKind; graph_audit=[bool]$GraphAudit
        graph_direct2_only=[bool]$Direct2Only
        graph_direct2_direct3_only=[bool]$Direct2Direct3Only
        v10_simulation_parallelism=$V10Parallelism
        cp9c_packing_mode=$CP9CPackingMode.ToLowerInvariant()
        cp9d_max_distinct_pairs=$CP9DMaxPairs
        require_catalog_reconciliation=[bool]$RequireCatalogReconciliation
        completion_pool_limit=0; completion_refresh_seconds=$CompletionRefreshSeconds
        completion_recheck_seconds=$CompletionRecheckSeconds
        log_filter=$values.LIQ_GRAPH_LOG
        plan_only=[bool]$PlanOnly; transactions_sent=0; plan=$plan
    }
    $checkout | ConvertTo-Json -Depth 14 | Set-Content -LiteralPath (Join-Path $folder 'checkout.json') -Encoding UTF8
    Write-Host "Output: $folder"
    Write-Host "Pipeline=runtime completion_refresh=true stage=$Stage simulation=$([bool]$SimulateV10) broadcasts=false"
    if ($signals) { Write-Host "Signals: marginal_dex_net threshold_bps=$ThresholdBps gross_fallback=false direction_notional=$MinDirectionNotionalUsd USD mode=$DirectionLiquidityMode fixed_SOL_USD=100 direct2_only=$([bool]$Direct2Only) direct2_direct3_only=$([bool]$Direct2Direct3Only) packing=$CP9CPackingMode cp9d_max_pairs=$CP9DMaxPairs" }
    Write-Host "Pool TVL minimum=$MinPoolTvlUsd USD, PumpSwap exempt; detail samples <= $ExecutionDetailLimit"
    Write-Host "Protocols=$($plan.enabled_venues -join ',') completion_workers=$(@($plan.completion_protocols).Count)"
    if ($RequirePreparedEvidence -and @($plan.enabled_venues | Where-Object { $_ -notin @('meteora_dlmm','pumpswap') }).Count -gt 0) {
        throw 'Prepared evidence currently covers only meteora_dlmm and pumpswap.'
    }
    Write-Host "WS accounts=$($plan.websocket_plan.account_capacity) sockets=$($plan.websocket_plan.socket_count)"
    if ($PlanOnly) {
        @{ plan_only=$true; runtime_started=$false; orchestration_evidence_passed=$false } |
            ConvertTo-Json | Set-Content -LiteralPath (Join-Path $folder 'summary.json') -Encoding UTF8
        Write-Host 'Plan only: no RPC connections, database reads or live acceptance.'
        return
    }
    if ($SimulateV10) {
        if ($SimulationLimit -eq 0) { Write-Host "Simulation requests: no total cap; parallel_in_flight=$V10Parallelism queue_capacity=$V10Parallelism broadcasts=false." }
        else { Write-Host "Simulation requests: total cap=$SimulationLimit, broadcasts=false." }
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
    $executionAnalysis = $null
    if ($SimulateV10 -and (Test-Path -LiteralPath (Join-Path $folder 'execution_events.jsonl'))) {
        $analysisPath = Join-Path $folder 'execution_analysis.json'
        & python (Join-Path $PSScriptRoot 'execution_analyze.py') --folder $folder --output $analysisPath
        if ($LASTEXITCODE -ne 0) { throw "Execution evidence contains delivery or valuation violations; inspect $analysisPath." }
        $executionAnalysis = Read-OptionalJson $analysisPath
        if ([long]$executionAnalysis.request_count -ne [long]$execution.request_count -or
            [long]$executionAnalysis.report_count -ne [long]$execution.report_count) {
            throw 'Execution summary/event counts differ.'
        }
    }
    $production = Read-OptionalJson $producerPath
    $refreshRecord = Select-String -LiteralPath $log -Pattern 'lifecycle stats final=true .*? prepared_refreshes=(\d+)' | Select-Object -Last 1
    $refreshes = if ($null -ne $refreshRecord) { [long]$refreshRecord.Matches[0].Groups[1].Value } else { 0 }
    $lifecycleStarted = [bool](Select-String -LiteralPath $log -Pattern 'lifecycle started' -Quiet)
    $protocols = @(Select-String -LiteralPath $log -Pattern 'lifecycle protocol_summary venue=(\w+) dispatched=(\d+) accepted=(\d+) proofs=(\d+) unchanged=(\d+) prepared=(\d+) incomplete=(\d+) retryable=(\d+)' | ForEach-Object {
        $fields = $_.Matches[0].Groups
        [ordered]@{venue=$fields[1].Value; dispatched=[long]$fields[2].Value; accepted=[long]$fields[3].Value
            proofs=[long]$fields[4].Value; unchanged=[long]$fields[5].Value; prepared=[long]$fields[6].Value
            incomplete=[long]$fields[7].Value; retryable=[long]$fields[8].Value}
    })
    $expectedProtocols = @($plan.completion_protocols)
    $observedProtocols = @($protocols | ForEach-Object { $_.venue } | Sort-Object -Unique)
    $unexpectedProtocols = @($observedProtocols | Where-Object { $_ -notin $expectedProtocols })
    $missingProtocols = @($expectedProtocols | Where-Object { $_ -notin $observedProtocols })
    $protocolScopePassed = $expectedProtocols.Count -gt 0 -and $unexpectedProtocols.Count -eq 0 -and $missingProtocols.Count -eq 0
    $protocolEvidence = $protocolScopePassed -and @($protocols | Where-Object { $_.accepted -eq 0 }).Count -eq 0
    $discovery = [ordered]@{}
    $sourceRecord = Select-String -LiteralPath $log -Pattern 'pool registry discovery_sources=' | Select-Object -Last 1
    if ($null -ne $sourceRecord) {
        $sourcePart = ($sourceRecord.Line -split 'discovery_sources=',2)[1] -replace ' unique_by_venue=.*$', ''
        foreach ($match in [regex]::Matches($sourcePart, '\("([^"\r\n]+)", (\d+)\)')) {
            $discovery[$match.Groups[1].Value] = [long]$match.Groups[2].Value
        }
    }
    $logLevels = [ordered]@{ERROR=0; WARN=0; INFO=0; DEBUG=0}
    foreach ($record in Select-String -LiteralPath $log -Pattern 'level=(ERROR|WARN|INFO|DEBUG) ') {
        $logLevels[$record.Matches[0].Groups[1].Value]++
    }
    $issueRecords = @(Select-String -LiteralPath $log -Pattern 'pool registry completion_issue ' | ForEach-Object { $_.Line })
    $candidateDelivery = $runtime.candidate_delivery_passed
    $orchestrationPassed = $protocolScopePassed -and $exitCode -eq 0 -and $null -ne $runtime -and $runtime.run_complete -eq $true -and
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
    $requestCount = [long]$execution.request_count
    $reportCount = [long]$execution.report_count
    $netViolations = if ($null -ne $executionAnalysis) { @($executionAnalysis.violations.net) } else { @() }
    $netPassed = $null
    if ($signals) {
        $netPassed = $null -ne $production -and $production.score_basis -eq 'marginal_dex_net' -and
            $production.last_plan.score_basis -eq 'marginal_dex_net' -and
            ([long]$production.observations_by_known_net_directions[1] + [long]$production.observations_by_known_net_directions[2]) -gt 0 -and
            $netViolations.Count -eq 0 -and
            (-not $SimulateV10 -or $execution.execution_candidates.score_basis -eq 'marginal_dex_net')
    }
    $feeEvidence = $market.metrics.fee_valuation
    $preparedPassed = $null
    if ($RequirePreparedEvidence) {
        $stages = $market.preparation
        $partition = [long]$stages.waiting_structure + [long]$stages.waiting_mints + [long]$stages.waiting_subscriptions +
            [long]$stages.waiting_baseline + [long]$stages.unpriceable + [long]$stages.priced
        $preparedPassed = $null -ne $stages -and $partition -eq [long]$market.selected_pools -and
            [long]$market.metrics.structure_rpc.batch_requests -eq 0 -and [long]$market.metrics.initial_hydration.count -eq 0 -and
            [long]$market.metrics.baseline_rpc.batch_requests -gt 0 -and [long]$market.metrics.baseline_prices -gt 0 -and
            [long]$market.metrics.decode_errors -eq 0 -and [long]$market.metrics.lost -eq 0
        foreach ($venue in $plan.enabled_venues) {
            $model = if ($venue -eq 'meteora_dlmm') { 'dlmm' } else { 'pumpswap' }
            $preparedPassed = $preparedPassed -and [long]$market.metrics.prepared_structures_by_venue.$venue -gt 0 -and
                [long]$feeEvidence.known_dex_fees_by_model.$model -gt 0
        }
    }
    $feePassed = $null
    if ($RequireFeeEvidence) {
        $feePassed = $null -ne $feeEvidence -and
            [long]$feeEvidence.known_dex_fees_by_model.dlmm -gt 0 -and
            [long]$feeEvidence.known_dex_fees_by_model.pumpswap -gt 0 -and
            [long]$feeEvidence.asymmetric_rates -gt 0
    }
    $liquidityEvidence = $market.metrics.liquidity
    $liquidityPassed = $null
    $liquidityViolations = if ($null -ne $executionAnalysis) { @($executionAnalysis.violations.liquidity) } else { @() }
    $focusRequested = @($LiquidityEvidencePools -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    $focusObserved = @($liquidityEvidence.focused.PSObject.Properties | ForEach-Object { $_.Name })
    $focusMissing = @($focusRequested | Where-Object { $_ -notin $focusObserved })
    if ($null -ne $liquidityEvidence -and ($RequireLiquidityEvidence -or $LiquidityEvidencePools)) {
        $liquidityEvidence | ConvertTo-Json -Depth 40 |
            Set-Content -LiteralPath (Join-Path $folder 'liquidity_evidence.json') -Encoding UTF8
    }
    if ($RequireLiquidityEvidence) {
        $liquidityPassed = $null -ne $liquidityEvidence -and
            [long]$liquidityEvidence.profile_observations -gt 0 -and
            [long]$liquidityEvidence.directions_by_status.available -gt 0 -and
            [long]$liquidityEvidence.array_websocket_updates -gt 0 -and
            [long]$production.liquidity_inputs -gt 0 -and
            $liquidityViolations.Count -eq 0 -and $focusMissing.Count -eq 0
    }
    $liquiditySummary = if ($null -eq $liquidityEvidence) { $null } else { [ordered]@{
        profile_observations=$liquidityEvidence.profile_observations
        profile_changes=$liquidityEvidence.profile_changes
        array_websocket_updates=$liquidityEvidence.array_websocket_updates
        bitmap_websocket_updates=$liquidityEvidence.bitmap_websocket_updates
        directions_by_status=$liquidityEvidence.directions_by_status
        directions_by_coverage=$liquidityEvidence.directions_by_coverage
        first_level_differs_from_active=$liquidityEvidence.first_level_differs_from_active
        cross_array_directions=$liquidityEvidence.cross_array_directions
        negative_bin_directions=$liquidityEvidence.negative_bin_directions
        profile_build_ns=$liquidityEvidence.profile_build
        signal_liquidity_inputs=$production.liquidity_inputs
        signal_liquidity_only_updates=$production.liquidity_only_updates
        focused_requested=$focusRequested; focused_observed=$focusObserved; focused_missing=$focusMissing
        request_window_violations=$liquidityViolations
    } }
    $amount = $execution.execution_candidates.amounts
    $amountViolations = if ($null -ne $executionAnalysis) { @($executionAnalysis.violations.amount) } else { @() }
    if ($null -ne $amount -and $RequireAmountEvidence) {
        $amount | ConvertTo-Json -Depth 60 | Set-Content -LiteralPath (Join-Path $folder 'amount_evidence.json') -Encoding UTF8
    }
    $amountPassed = $null
    if ($RequireAmountEvidence) {
        $amountPassed = $null -ne $amount -and [long]$amount.possible -gt 0 -and [long]$amount.excluded -gt 0 -and
            ([long]$amount.possible+[long]$amount.unknown) -eq $requestCount -and
            [long]$amount.attempts -eq ([long]$amount.possible+[long]$amount.unknown+[long]$amount.excluded) -and
            [long]$market.metrics.reserve_curve_observations -gt 0 -and
            [long]$market.metrics.reserve_curve_observations -eq [long]$production.reserve_curve_inputs -and
            [long]$liquidityEvidence.array_websocket_updates -gt 0 -and $amountViolations.Count -eq 0
    }
    $amountSummary = if ($null -eq $amount) { $null } else { [ordered]@{
        attempts=$amount.attempts; possible=$amount.possible; unknown=$amount.unknown; excluded=$amount.excluded
        seed_repaired=$amount.seed_repaired; global_proofs=$amount.global_proofs
        packed_exclusions=$amount.packed_exclusions; probes=$amount.probes
        reasons=$amount.reasons; processing_ns=(Measure-AmountStage $amount)
        violations=$amountViolations; reserve_curve_observations=$market.metrics.reserve_curve_observations
        reserve_curve_inputs=$production.reserve_curve_inputs; reserve_curve_build_ns=$market.metrics.reserve_curve_build
    } }
    # Owner measurements only; no extra observer or per-pool timing logs.
    $registryCycles = @(Select-String -LiteralPath $log -Pattern 'pool registry tvl_refresh=(\{.*\})$' | ForEach-Object {
        $_.Matches[0].Groups[1].Value | ConvertFrom-Json
    })
    $registryMaintenance = [ordered]@{
        cycles=$registryCycles.Count
        scanned=[long](($registryCycles | Measure-Object -Property scanned -Sum).Sum)
        changed=[long](($registryCycles | Measure-Object -Property changed -Sum).Sum)
        persisted=[long](($registryCycles | Measure-Object -Property persisted -Sum).Sum)
        write_batches=[long](($registryCycles | Measure-Object -Property write_batches -Sum).Sum)
        max_write_batch_pools=[long](($registryCycles | Measure-Object -Property max_write_batch_pools -Maximum).Maximum)
        max_prepare_ns=[long](($registryCycles | Measure-Object -Property prepare_ns -Maximum).Maximum)
        write_lock_wait_ns=[long](($registryCycles | Measure-Object -Property write_lock_wait_ns -Sum).Sum)
        write_lock_hold_ns=[long](($registryCycles | Measure-Object -Property write_lock_hold_ns -Sum).Sum)
        max_write_lock_hold_ns=[long](($registryCycles | Measure-Object -Property max_write_lock_hold_ns -Maximum).Maximum)
        max_elapsed_ns=[long](($registryCycles | Measure-Object -Property elapsed_ns -Maximum).Maximum)
    }
    $registryReads = $execution.execution_candidates.registry_reads
    $requestRepetition = $executionAnalysis.payload_repetition
    $tvl = $runtime.registry_tvl
    $tvlPassed = $null -ne $tvl -and [double]$tvl.minimum_pool_tvl_usd -eq $MinPoolTvlUsd -and
        [long]$tvl.admission_violations -eq 0
    $calibration = $executionAnalysis.calibration
    $calibrationPassed = $null
    if ($RequireCalibrationEvidence) {
        $calibrationPassed = [long]$execution.schema_version -ge 3 -and $requestCount -gt 0 -and
            [long]$calibration.counts.records -eq $requestCount -and
            [long]$calibration.model_route_statuses.quoted -gt 0 -and
            [long]$executionAnalysis.violation_counts.calibration -eq 0 -and
            'pumpswap' -in @($tvl.tvl_exempt_venues)
    }
    $feeSummary = $feeEvidence | Select-Object * -ExcludeProperty samples,invalid_reference_samples,clock_reference_lag_samples
    $summary = [ordered]@{
        branch=$branch; head=$head; pipeline='runtime'; stage=$Stage; exit_code=$exitCode
        minimum_pool_tvl_usd=$MinPoolTvlUsd; pool_tvl_evidence_passed=[bool]$tvlPassed; registry_tvl=$tvl
        calibration_evidence_passed=$calibrationPassed; execution_calibration=$calibration
        dlmm_pumpswap_counterparts=$tvl.dlmm_pumpswap_counterparts
        execution_detail_limit=$ExecutionDetailLimit; settlement_results=$executionAnalysis.settlement_results
        diagnostic_consumers=$runtime.diagnostic_consumers; signal_proof=$runtime.signal_proof
        runtime_completed=($null -ne $runtime -and $runtime.run_complete -eq $true)
        orchestration_evidence_passed=[bool]$orchestrationPassed; runtime_evidence=$runtime
        market_delivery_passed=$runtime.market_delivery_passed
        signal_delivery_passed=$runtime.signal_delivery_passed
        rewatch_evidence_passed=$runtime.rewatch_evidence_passed
        graph_membership_passed=$runtime.graph_membership_passed
        graph_membership=$runtime.graph_membership
        protocol_scope_passed=[bool]$protocolScopePassed; enabled_venues=@($plan.enabled_venues)
        registry_pools_by_venue=$runtime.registry_pools_by_venue
        expected_completion_protocols=$expectedProtocols
        unexpected_completion_protocols=$unexpectedProtocols; missing_completion_protocols=$missingProtocols
        evidence_errors=$runtime.evidence_errors
        elapsed_seconds=[Math]::Round($clock.Elapsed.TotalSeconds,2)
        requested_duration_seconds=$DurationSeconds; completion_lifecycle_started=$lifecycleStarted
        prepared_refreshes_observed=$refreshes; graph_audit_passed=$graphPassed
        completion_protocol_evidence_passed=[bool]$protocolEvidence; completion_protocols=$protocols
        completion_issues=$issueRecords; discovery_records_by_source=$discovery
        log_bytes=(Get-Item -LiteralPath $log).Length; log_levels=$logLevels
        selected_pools=$market.selected_pools; hydrated_pools=$market.hydrated_pools
        ready_pools=$market.ready_pools; priced_pools=$market.priced_pools
        market_state_observations=$market.observations; websocket_observations=$market.websocket_observations
        post_rewatch_websocket_observations=$market.post_rewatch_websocket_observations
        fee_evidence_passed=$feePassed; fee_valuation=$feeSummary
        liquidity_evidence_passed=$liquidityPassed; liquidity=$liquiditySummary
        amount_evidence_passed=$amountPassed; amount_evaluation=$amountSummary
        request_payload_repetition=$requestRepetition
        signal_score_basis=$production.score_basis; signal_net_threshold_bps=$ThresholdBps
        net_evidence_passed=$netPassed; net_witness_violations=$netViolations
        observations_by_known_net_directions=$production.observations_by_known_net_directions
        net_candidate_windows=$production.candidate_windows
        market_account_processing_ns=$market.metrics.processing
        prepared_bootstrap_passed=$preparedPassed; market_preparation=$market.preparation
        # Detailed readiness is projected only after the live loop has stopped.
        market_readiness=$market.metrics.preparation_final
        subscription_gaps_by_source=$market.metrics.subscription_gaps
        prepared_structures_by_venue=$market.metrics.prepared_structures_by_venue
        prepared_structure_build_ns=$market.metrics.prepared_structure_build
        prepared_to_first_baseline_ns=$market.metrics.prepared_to_first_baseline
        completed_structure_rpc=$market.metrics.structure_rpc; completed_baseline_rpc=$market.metrics.baseline_rpc
        market_queue_ns=$market.metrics.queue
        signal_steady_state_processing_ns=$production.steady_state_inputs.processing_ns
        signal_preparation_ns=$production.steady_state_inputs.preparation_ns
        signal_index_update_ns=$production.steady_state_inputs.index_update_ns
        signal_state_ready_ns=$production.steady_state_inputs.state_ready_ns
        signal_query_ns=$production.steady_state_inputs.query_ns
        signal_event_build_ns=$production.steady_state_inputs.event_build_ns
        signal_publication_ns=$production.steady_state_inputs.publication_ns
        signal_revaluation_processing_ns=$production.revaluation_inputs.processing_ns
        frame_to_signal_state_us=$production.steady_state_inputs.frame_to_state_ready_us
        frame_to_signal_complete_us=$production.steady_state_inputs.frame_to_evaluation_complete_us
        execution_selection_ns=$executionAnalysis.timings.selection_ns
        signal_to_request_us=$executionAnalysis.timings.signal_to_request_us
        frame_to_request_us=$executionAnalysis.timings.frame_to_request_us
        executor_build_us=$executionAnalysis.timings.build_us
        executor_simulation_rpc_ms=$executionAnalysis.timings.simulation_rpc_ms
        executor_elapsed_ms=$executionAnalysis.timings.elapsed_ms
        registry_maintenance=$registryMaintenance
        execution_registry_reads=$registryReads
        registry_timing_observed=($registryCycles.Count -ge 2 -and [long]$registryReads.calls -gt 0)
        execution_skipped_busy_revisions=$execution.execution_candidates.skipped_busy_revisions
        candidate_delivery_passed=$candidateDelivery
        v10_requests=$requestCount; v10_reports=$reportCount
        v10_simulation_successes=$execution.simulation_successes
        v10_simulation_failures=$execution.simulation_failures
        v10_dex_call_reports=$execution.simulation_dex_calls_observed
        execution_integration_passed=$execution.execution_integration_passed
        transactions_sent=0; artifacts=$folder
    }
    $summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $folder 'summary.json') -Encoding UTF8
    Write-Host "Runtime complete=$($summary.runtime_completed); orchestration=$orchestrationPassed; pool TVL evidence=$tvlPassed"
    Write-Host "Pools selected=$($market.selected_pools), priced=$($market.priced_pools); WS gaps=$($market.metrics.gaps)"
    if ($RequireCalibrationEvidence) {
        Write-Host "Calibration: requests=$($calibration.counts.records), quoted routes=$($calibration.model_route_statuses.quoted), passed=$calibrationPassed"
    }
    foreach ($venue in $tvl.by_venue.PSObject.Properties) {
        $v = $venue.Value
        Write-Host "TVL $($venue.Name): prepared/admitted=$($v.prepared_admitted), blocked below minimum=$($v.below_minimum), blocked missing TVL=$($v.missing_tvl), admitted without TVL=$($v.admitted_without_tvl)"
    }
    Write-Host "Summary: $(Join-Path $folder 'summary.json')"
    if ($RequireCalibrationEvidence -and -not $calibrationPassed) { throw 'CP8H calibration evidence incomplete; inspect execution_analysis.json and execution_events.jsonl.' }
    if (-not $tvlPassed) { throw 'Pool TVL admission evidence incomplete; inspect registry_tvl in summary.json.' }
    if ($RequireFeeEvidence -and -not $feePassed) { throw "CP8A fee evidence incomplete; inspect $folder." }
    if ($RequirePreparedEvidence -and -not $preparedPassed) { throw "CP8F prepared bootstrap evidence incomplete; inspect $folder." }
    if ($RequireLiquidityEvidence -and -not $liquidityPassed) { throw "CP8B liquidity evidence incomplete; inspect liquidity_evidence.json in $folder." }
    if ($RequireAmountEvidence -and -not $amountPassed) { throw "CP8C amount evidence incomplete; inspect amount_evidence.json in $folder. A real exclusion and a positive amount estimate are required." }
    if ($signals -and -not $netPassed) { throw "Net signal evidence incomplete; inspect $folder." }
    if (-not $protocolEvidence) { Write-Warning 'Nicht fuer jedes Completion-Protokoll angenommene Arbeit nachgewiesen; protocol_summary pruefen.' }
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
