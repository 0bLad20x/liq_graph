# One production pipeline for finite graph, MarketState, signal and V10 simulation proofs.
# Uses the local .env. Never broadcasts transactions or modifies provider credentials.
param(
    [ValidateRange(30,86400)][int]$DurationSeconds = 60,
    [ValidateRange(0,2147483647)][int]$PoolLimit = 15000,
    [ValidateRange(0,1000000)][int]$AccountBudget = 25000,
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
    [string]$WsProviders = 'Configured',
    [ValidateRange(0,2147483647)][int]$WsAccountBudget = 0,
    [ValidateRange(0,64)][int]$WsConnectionsPerProvider = 0,
    [switch]$RequireFeeEvidence,
    [switch]$RequireLiquidityEvidence,
    [Alias('RequireAmountEvidence')][switch]$RequireEconomicEvidence,
    [UInt64]$EconomicNetworkFeeLamports = 5000,
    [UInt64]$EconomicCostReserveLamports = 0,
    [UInt64]$EconomicMinNetProfitLamports = 0,
    # Diagnostic raw snapshots only. This does not pin or filter pool selection.
    [string]$LiquidityEvidencePools = '',
    [string]$EnabledVenues = '',
    [switch]$SimulateV10,
    [string]$WalletPath,
    [ValidateRange(0,2147483647)][int]$SimulationLimit = 0,
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
if ($RequireFeeEvidence -and -not $signals) { throw 'Fee evidence requires Stage Signals.' }
if (($RequireLiquidityEvidence -or $LiquidityEvidencePools) -and -not $signals) { throw 'Liquidity evidence requires Stage Signals.' }
if ($RequireEconomicEvidence -and (-not $signals -or -not $SimulateV10)) { throw 'Economic evidence requires Stage Signals and SimulateV10.' }
if ($RequireCatalogReconciliation -and -not $GraphAudit) { throw 'RequireCatalogReconciliation requires GraphAudit.' }
if ([double]::IsNaN($ThresholdBps) -or [double]::IsInfinity($ThresholdBps)) { throw 'ThresholdBps must be finite.' }
if ($SimulateV10 -and -not $PlanOnly -and -not $WalletPath) { throw 'SimulateV10 requires WalletPath.' }

function Read-OptionalJson([string]$Path) {
    if (Test-Path -LiteralPath $Path) { return (Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json) }
    return $null
}
# Parse one full selection at a time, retain only small scalar evidence. The
# combined owner report deliberately contains no duplicated frozen requests.
function Read-SelectionSummary([string]$Path) {
    $s = Read-OptionalJson $Path
    $hasher = [System.Security.Cryptography.SHA256]::Create()
    try {
        $payload = $s.request | Select-Object * -ExcludeProperty id | ConvertTo-Json -Depth 40 -Compress
        $hash = [Convert]::ToBase64String($hasher.ComputeHash([Text.Encoding]::UTF8.GetBytes($payload)))
    } finally { $hasher.Dispose() }
    return [pscustomobject]@{
        request=[pscustomobject]@{id=$s.request.id; settlement=$s.request.settlement; parameters=$s.request.parameters}
        candidate=[pscustomobject]@{witness=[pscustomobject]@{log_sum_units=$s.candidate.witness.log_sum_units}}
        threshold_units=$s.threshold_units; packing_seed_log_sum_units=$s.packing_seed_log_sum_units
        amount_assessment=$s.amount_assessment; economic_primary_changed=$s.economic_primary_changed
        cost_assumptions=$s.cost_assumptions; payload_sha256=$hash
        selection_ns=$s.selection_ns; signal_to_request_us=$s.signal_to_request_us; frame_to_request_us=$s.frame_to_request_us
        pools=@($s.pools | ForEach-Object { [pscustomobject]@{
            pool=$_.pool; liquidity_window_checked=$_.liquidity_window_checked
            observation=[pscustomobject]@{venue=$_.observation.venue; liquidity=$_.observation.liquidity.model}
        } })
    }
}
function Measure-Durations($Values) {
    # Exact post-run request samples; signal-owner percentiles remain histogram bounds.
    $sorted = @($Values | Where-Object { $null -ne $_ } | ForEach-Object { [long]$_ } | Sort-Object)
    $n = $sorted.Count
    if ($n -eq 0) { return [ordered]@{count=0; min=$null; median=$null; mean=$null; p95=$null; p99=$null; max=$null} }
    $middle = [int][Math]::Floor($n / 2)
    $median = if ($n % 2 -eq 0) { ([double]$sorted[$middle - 1] + [double]$sorted[$middle]) / 2 } else { $sorted[$middle] }
    return [ordered]@{
        count=$n; min=$sorted[0]; median=$median
        mean=($sorted | Measure-Object -Average).Average
        p95=$sorted[[int][Math]::Ceiling($n * 0.95) - 1]
        p99=$sorted[[int][Math]::Ceiling($n * 0.99) - 1]; max=$sorted[$n - 1]
    }
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
        GRAPH_ECONOMIC_NETWORK_FEE_LAMPORTS = "$EconomicNetworkFeeLamports"
        GRAPH_ECONOMIC_COST_RESERVE_LAMPORTS = "$EconomicCostReserveLamports"
        GRAPH_ECONOMIC_MIN_NET_PROFIT_LAMPORTS = "$EconomicMinNetProfitLamports"
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
            'liq_graph::workers::execution_v10=info','liq_graph::workers::rpc_transport=info'
        ) -join ',')
    }
    if ($EnabledVenues) { $values.GRAPH_ENABLED_VENUES = $EnabledVenues }
    if ($CompletionRefreshSeconds -gt 0) { $values.COMPLETION_LIFECYCLE_REFRESH_SECONDS = "$CompletionRefreshSeconds" }
    if ($CompletionRecheckSeconds -gt 0) { $values.COMPLETION_LIFECYCLE_RECHECK_SECONDS = "$CompletionRecheckSeconds" }
    if ($SimulateV10 -and -not $PlanOnly) {
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
        stage=$Stage; rewatch=[bool]$Rewatch; simulation_enabled=[bool]$SimulateV10
        signal_score_basis='marginal_dex_net'; signal_net_threshold_bps=$ThresholdBps
        profile_signals=[bool]$ProfileSignals
        require_fee_evidence=[bool]$RequireFeeEvidence; enabled_venues=$EnabledVenues
        require_liquidity_evidence=[bool]$RequireLiquidityEvidence
        liquidity_evidence_pools=$LiquidityEvidencePools; liquidity_model='dlmm_execution_window_v2'
        require_economic_evidence=[bool]$RequireEconomicEvidence; economic_check='primary_route_before_packing'
        economic_network_fee_lamports=$EconomicNetworkFeeLamports; economic_cost_reserve_lamports=$EconomicCostReserveLamports
        economic_min_net_profit_lamports=$EconomicMinNetProfitLamports
        simulation_limit=$SimulationLimit; route_kind=$RouteKind; graph_audit=[bool]$GraphAudit
        require_catalog_reconciliation=[bool]$RequireCatalogReconciliation
        completion_pool_limit=0; completion_refresh_seconds=$CompletionRefreshSeconds
        completion_recheck_seconds=$CompletionRecheckSeconds
        log_filter=$values.LIQ_GRAPH_LOG
        plan_only=[bool]$PlanOnly; transactions_sent=0; plan=$plan
    }
    $checkout | ConvertTo-Json -Depth 14 | Set-Content -LiteralPath (Join-Path $folder 'checkout.json') -Encoding UTF8
    Write-Host "Output: $folder"
    Write-Host "Pipeline=runtime completion_refresh=true stage=$Stage simulation=$([bool]$SimulateV10) broadcasts=false"
    if ($signals) { Write-Host "Signals: marginal_dex_net threshold_bps=$ThresholdBps gross_fallback=false" }
    Write-Host "WS accounts=$($plan.websocket_plan.account_capacity) sockets=$($plan.websocket_plan.socket_count)"
    if ($PlanOnly) {
        @{ plan_only=$true; runtime_started=$false; orchestration_evidence_passed=$false } |
            ConvertTo-Json | Set-Content -LiteralPath (Join-Path $folder 'summary.json') -Encoding UTF8
        Write-Host 'Plan only: no RPC connections, database reads or live acceptance.'
        return
    }
    if ($SimulateV10) {
        if ($SimulationLimit -eq 0) { Write-Host 'Simulation requests: no total cap; one executor and one queued request, broadcasts=false.' }
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
    $protocolEvidence = $protocols.Count -eq 10 -and @($protocols | Where-Object { $_.accepted -eq 0 }).Count -eq 0
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
    $requests = @(Get-ChildItem -LiteralPath $folder -Filter 'selection-*.json' | ForEach-Object { Read-SelectionSummary $_.FullName })
    $reports = if ($null -ne $execution.execution_reports) { @($execution.execution_reports) } else { @() }
    $netViolations = @($requests | Where-Object { $_.candidate.witness.log_sum_units -ge $_.threshold_units -or $_.packing_seed_log_sum_units -ge $_.threshold_units } | ForEach-Object { $_.request.id })
    $netPassed = $null
    if ($signals) {
        $netPassed = $null -ne $production -and $production.score_basis -eq 'marginal_dex_net' -and
            $production.last_plan.score_basis -eq 'marginal_dex_net' -and
            ([long]$production.observations_by_known_net_directions[1] + [long]$production.observations_by_known_net_directions[2]) -gt 0 -and
            $netViolations.Count -eq 0 -and
            (-not $SimulateV10 -or $execution.execution_candidates.score_basis -eq 'marginal_dex_net')
    }
    $feeEvidence = $market.metrics.fee_valuation
    $feePassed = $null
    if ($RequireFeeEvidence) {
        $feePassed = $null -ne $feeEvidence -and
            [long]$feeEvidence.known_dex_fees_by_model.dlmm -gt 0 -and
            [long]$feeEvidence.known_dex_fees_by_model.pumpswap -gt 0 -and
            [long]$feeEvidence.asymmetric_rates -gt 0
    }
    $liquidityEvidence = $market.metrics.liquidity
    $liquidityPassed = $null
    $liquidityViolations = @($requests | ForEach-Object {
        $id = $_.request.id
        foreach ($pool in $_.pools) {
            if ($pool.observation.venue -eq 'meteora_dlmm' -and
                ($null -eq $pool.observation.liquidity -or -not $pool.liquidity_window_checked)) { $id }
        }
    } | Sort-Object -Unique)
    $focusRequested = @($LiquidityEvidencePools -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    $focusObserved = @($liquidityEvidence.focused.PSObject.Properties | ForEach-Object { $_.Name })
    $focusMissing = @($focusRequested | Where-Object { $_ -notin $focusObserved })
    if ($null -ne $liquidityEvidence) {
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
    $amountViolations = @($requests | ForEach-Object {
        $r = $_; $a = $r.amount_assessment
        if ($null -eq $a -or $a.decision -ne 'possible' -or $a.reason -eq 'not_evaluated') { $r.request.id; return }
        $estimate = $a.estimate; $legs = @($estimate.legs)
        $invalid = $null -eq $estimate -or $legs.Count -lt 2 -or
            [decimal]$estimate.input_raw -le 0 -or [decimal]$estimate.output_raw -le [decimal]$estimate.input_raw -or
            [decimal]$estimate.profit_raw -ne ([decimal]$estimate.output_raw-[decimal]$estimate.input_raw) -or
            [decimal]$estimate.profit_raw -le [decimal]$a.required_profit_raw -or
            $r.request.parameters.minimum_profit -ne 0 -or $r.request.parameters.trade_size -ne 0
        if ($legs.Count -ge 2) {
            $invalid = $invalid -or $legs[0].input_mint -ne $r.request.settlement -or
                $legs[-1].output_mint -ne $r.request.settlement -or
                [decimal]$legs[0].input_raw -ne [decimal]$estimate.input_raw -or
                [decimal]$legs[-1].output_raw -ne [decimal]$estimate.output_raw
            for ($i = 0; $i -lt $legs.Count; $i++) {
                $invalid = $invalid -or $legs[$i].pool -notin @($r.pools.pool)
                if ($i -gt 0) { $invalid = $invalid -or $legs[$i].input_mint -ne $legs[$i-1].output_mint -or
                    [decimal]$legs[$i].input_raw -ne [decimal]$legs[$i-1].output_raw }
            }
        }
        if ($invalid) { $r.request.id }
    } | Sort-Object -Unique)
    if ($null -ne $amount) {
        $amount | ConvertTo-Json -Depth 60 | Set-Content -LiteralPath (Join-Path $folder 'economic_evidence.json') -Encoding UTF8
    }
    $amountPassed = $null
    if ($RequireEconomicEvidence) {
        $amountPassed = $null -ne $amount -and [long]$amount.attempts -gt 0 -and
            [long]$execution.request_count -eq $requests.Count -and
            [long]$execution.execution_candidates.economic_qualified_requests -eq $requests.Count -and
            [long]$amount.attempts -eq ([long]$amount.possible+[long]$amount.unknown+[long]$amount.excluded) -and
            [long]$market.metrics.reserve_curve_observations -gt 0 -and
            [long]$market.metrics.reserve_curve_observations -eq [long]$production.reserve_curve_inputs -and
            [long]$liquidityEvidence.array_websocket_updates -gt 0 -and $amountViolations.Count -eq 0
    }
    $amountSummary = if ($null -eq $amount) { $null } else { [ordered]@{
        attempts=$amount.attempts; possible=$amount.possible; unknown=$amount.unknown; excluded=$amount.excluded
        cache_hits=$amount.cache_hits; unchanged_states=$amount.unchanged_states; probes=$amount.probes
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
    # Post-run payload identity only: equal accounts/parameters do not imply
    # equal pool state, equal liquidity, or the same economic opportunity.
    $payloads = @{}
    foreach ($selection in $requests) {
        $key = $selection.payload_sha256
        if (-not $payloads.ContainsKey($key)) { $payloads[$key] = [ordered]@{payload_sha256=$key; request_ids=@()} }
        $payloads[$key].request_ids += $selection.request.id
    }
    $repeated = @($payloads.Values | Where-Object { $_.request_ids.Count -gt 1 } | Sort-Object { $_.request_ids.Count } -Descending)
    ConvertTo-Json -InputObject $repeated -Depth 10 |
        Set-Content -LiteralPath (Join-Path $folder 'repeated_request_sets.json') -Encoding UTF8
    $requestRepetition = [ordered]@{
        unique_payloads=$payloads.Count; repeated_payload_sets=$repeated.Count
        requests_in_repeated_sets=[long](($repeated | ForEach-Object { $_.request_ids.Count } | Measure-Object -Sum).Sum)
        largest_set=[long](($repeated | ForEach-Object { $_.request_ids.Count } | Measure-Object -Maximum).Maximum)
    }
    $summary = [ordered]@{
        branch=$branch; head=$head; pipeline='runtime'; stage=$Stage; exit_code=$exitCode
        diagnostic_consumers=$runtime.diagnostic_consumers; signal_proof=$runtime.signal_proof
        runtime_completed=($null -ne $runtime -and $runtime.run_complete -eq $true)
        orchestration_evidence_passed=[bool]$orchestrationPassed; runtime_evidence=$runtime
        market_delivery_passed=$runtime.market_delivery_passed
        signal_delivery_passed=$runtime.signal_delivery_passed
        rewatch_evidence_passed=$runtime.rewatch_evidence_passed
        graph_membership_passed=$runtime.graph_membership_passed
        graph_membership=$runtime.graph_membership
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
        fee_evidence_passed=$feePassed; fee_valuation=$feeEvidence
        liquidity_evidence_passed=$liquidityPassed; liquidity=$liquiditySummary
        economic_evidence_passed=$amountPassed; economic_evaluation=$amountSummary
        economic_positive_after_network=$execution.economic_positive_after_network
        request_payload_repetition=$requestRepetition
        signal_score_basis=$production.score_basis; signal_net_threshold_bps=$ThresholdBps
        net_evidence_passed=$netPassed; net_witness_violations=$netViolations
        observations_by_known_net_directions=$production.observations_by_known_net_directions
        net_candidate_windows=$production.candidate_windows
        market_account_processing_ns=$market.metrics.processing
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
        execution_selection_ns=(Measure-Durations @($requests | ForEach-Object { $_.selection_ns }))
        signal_to_request_us=(Measure-Durations @($requests | ForEach-Object { $_.signal_to_request_us }))
        frame_to_request_us=(Measure-Durations @($requests | ForEach-Object { $_.frame_to_request_us }))
        registry_maintenance=$registryMaintenance
        execution_registry_reads=$registryReads
        registry_timing_observed=($registryCycles.Count -ge 2 -and [long]$registryReads.calls -gt 0)
        execution_skipped_busy_revisions=$execution.execution_candidates.skipped_busy_revisions
        economic_jobs_started=$execution.execution_candidates.economic_jobs_started
        economic_jobs_completed=$execution.execution_candidates.economic_jobs_completed
        economic_jobs_cancelled=$execution.execution_candidates.economic_jobs_cancelled
        economic_stale_results=$execution.execution_candidates.economic_stale_results
        economic_busy_results=$execution.execution_candidates.economic_busy_results
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
    if ($RequireFeeEvidence -and -not $feePassed) { throw "CP8A fee evidence incomplete; inspect $folder." }
    if ($RequireLiquidityEvidence -and -not $liquidityPassed) { throw "CP8B liquidity evidence incomplete; inspect liquidity_evidence.json in $folder." }
    if ($RequireEconomicEvidence -and -not $amountPassed) { throw "CP8D economic evidence incomplete; inspect economic_evidence.json in $folder. Zero qualified requests are valid; decisions and model delivery must be observed." }
    if ($signals -and -not $netPassed) { throw "Net signal evidence incomplete; inspect $folder." }
    if (-not $protocolEvidence) { Write-Warning 'Nicht fuer jedes Completion-Protokoll angenommene Arbeit nachgewiesen; protocol_summary pruefen.' }
    if (-not $orchestrationPassed) { throw "Orchestrierungsnachweis unvollstaendig; Details: $folder" }
    Write-Host "Orchestrierungsnachweis bestanden. Artefakte: $folder"
    if ($SimulateV10) { Write-Host "Wirtschaftlicher Vergleich: qualifizierte Requests=$($requests.Count), positive Ergebnisse nach Netzwerkgebuehr=$($execution.economic_positive_after_network). Details: economic_comparison.json" }
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
