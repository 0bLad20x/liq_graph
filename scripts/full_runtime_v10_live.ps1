# Single continuous production audit: discovery -> completion -> graph -> WS signals -> V10 simulate.
# No on-chain transactions are sent.  Runtime diagnostics and V10 simulation artifacts are saved.
param(
    [ValidateRange(30,86400)][int]$DurationSeconds = 600,
    [ValidateRange(1,1000)][int]$SimulationLimit = 100,
    [ValidateRange(0,2147483647)][int]$PoolLimit = 750,
    [ValidateRange(0,1000000)][double]$ThresholdBps = 0,
    [string]$WalletPath = (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json'),
    [string]$ExpectedBranch = 'agent/full-runtime-v10-audit'
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $repo
$previous = @{}
$envNames = @(
    'RUN_DURATION_SECONDS','RUN_SHUTDOWN_SECONDS',
    'MARKET_STATE_ENABLED','GRAPH_SIGNALS_ENABLED','CP1_MARKET_ENRICHMENT_ENABLED',
    'MARKET_STATE_POOL_LIMIT','GRAPH_SIGNALS_THRESHOLD_BPS','GRAPH_SIGNALS_PROFILE',
    'GRAPH_SIGNALS_TRACE','GRAPH_SIGNALS_SAMPLE_LIMIT','GRAPH_EXECUTION_SIMULATE',
    'GRAPH_EXECUTION_WALLET_PATH','GRAPH_EXECUTION_MAX_REQUESTS','GRAPH_EXECUTION_ROUTE_KIND',
    'GRAPH_EXECUTION_REPORT_PATH','GRAPH_RUNTIME_PRODUCTION_REPORT_PATH',
    'GRAPH_RUNTIME_SIGNAL_REPORT_PATH','LIQ_GRAPH_LOG'
)
try {
    $branch = (& git branch --show-current)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot read checked-out branch.' }
    $head = (& git rev-parse HEAD)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot read HEAD.' }
    $status = @(& git status --short --untracked-files=no)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot read tracked working-tree status.' }
    if ($status.Count -gt 0) { throw "Tracked changes exist. Preserve them before starting: $($status -join ', ')" }
    if ($ExpectedBranch -and $branch -ne $ExpectedBranch) {
        throw "Expected branch '$ExpectedBranch', found '$branch'. No checkout was attempted."
    }
    $WalletPath = (Resolve-Path -LiteralPath $WalletPath).Path
    $stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ')
    $folder = Join-Path $repo "runtime_v10_live\$stamp"
    New-Item -Path $folder -ItemType Directory -Force | Out-Null
    $log = Join-Path $folder 'runtime.log'
    $execReport = Join-Path $folder 'execution.json'
    $producer = Join-Path $folder 'signal_production.json'
    $consumer = Join-Path $folder 'signal_consumer.json'
    $startMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $checkout = [ordered]@{
        branch = $branch; head = $head; start_ms = $startMs
        duration_seconds = $DurationSeconds; simulation_limit = $SimulationLimit
        pool_limit = $PoolLimit; threshold_bps = $ThresholdBps
        execution_candidates_wired = $true; v10_simulation_wired = $true
        transactions_sent = $false; simulation_only = $true
    }
    $checkout | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $folder 'checkout.json') -Encoding UTF8
    $settings = @{
        RUN_DURATION_SECONDS = "$DurationSeconds"
        RUN_SHUTDOWN_SECONDS = '360'
        MARKET_STATE_ENABLED = 'true'
        GRAPH_SIGNALS_ENABLED = 'true'
        CP1_MARKET_ENRICHMENT_ENABLED = 'true'
        MARKET_STATE_POOL_LIMIT = "$PoolLimit"
        GRAPH_SIGNALS_THRESHOLD_BPS = $ThresholdBps.ToString([System.Globalization.CultureInfo]::InvariantCulture)
        GRAPH_SIGNALS_PROFILE = 'true'
        GRAPH_SIGNALS_TRACE = 'false'
        GRAPH_SIGNALS_SAMPLE_LIMIT = '4'
        GRAPH_EXECUTION_SIMULATE = 'true'
        GRAPH_EXECUTION_WALLET_PATH = $WalletPath
        GRAPH_EXECUTION_MAX_REQUESTS = "$SimulationLimit"
        GRAPH_EXECUTION_ROUTE_KIND = 'All'
        GRAPH_EXECUTION_REPORT_PATH = $execReport
        GRAPH_RUNTIME_PRODUCTION_REPORT_PATH = $producer
        GRAPH_RUNTIME_SIGNAL_REPORT_PATH = $consumer
        # Default WARN; enable only useful aggregate/periodic and execution INFO logs.
        # Deliberately NO 'completion_lifecycle=debug' (tens of thousands of pool plans).
        LIQ_GRAPH_LOG = (
            'warn',
            'liq_graph::orchestration::runtime=info',
            'liq_graph::workers::persistence=info',
            'liq_graph::workers::pool_registry=info',
            'liq_graph::workers::completion_lifecycle=info',
            'liq_graph::workers::graph_liquidity=info',
            'liq_graph::workers::graph_watch_catalog=info',
            'liq_graph::workers::market_state=info',
            'liq_graph::workers::graph_signals=info',
            'liq_graph::workers::execution_candidates=info',
            'liq_graph::workers::execution_v10=info',
            'liq_graph::workers::rpc_transport=info',
            'liq_graph::workers::discovery_manifest=info',
            'liq_graph::workers::discovery_meteora_dlmm=info',
            'liq_graph::workers::discovery_meteora_damm_v2=info',
            'liq_graph::workers::discovery_raydium=info',
            'liq_graph::workers::discovery_orca_whirlpool=info',
            'liq_graph::workers::discovery_pancakeswap_clmm=info',
            'liq_graph::workers::discovery_byreal_clmm=info',
            'liq_graph::workers::discovery_pumpswap=info'
        ) -join ','
    }
    foreach ($name in $envNames) {
        $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        [Environment]::SetEnvironmentVariable($name, $settings[$name], 'Process')
    }
    Write-Host "Branch: $branch / $head"
    Write-Host "Live production + V10 simulation: $DurationSeconds seconds. No sendTransaction."
    Write-Host "Full diagnostic artifacts: $folder"
    & cargo build --locked --release
    if ($LASTEXITCODE -ne 0) { throw 'cargo build --locked --release failed. No live test was started.' }
    & git diff --exit-code -- Cargo.lock
    if ($LASTEXITCODE -ne 0) { throw 'Build changed Cargo.lock. Stop and inspect.' }
    $statusAfter = @(& git status --short --untracked-files=no)
    if ($LASTEXITCODE -ne 0 -or $statusAfter.Count -gt 0) {
        throw 'Build changed tracked files; stop and inspect.'
    }
    $binary = Join-Path $repo 'target\release\liq_graph.exe'
    if (-not (Test-Path -LiteralPath $binary)) { throw "Executable missing: $binary" }
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $lifecycleAccepted = 0
    $preparedRefreshes = 0
    $acceptedByVenue = @{}
    $priorPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        # The per-pool acceptance record is counted, not repeated 10k+ times in the log.
        # All warning/error lines, lifecycle aggregates, timing and V10 result lines remain.
        & $binary --run 2>&1 | ForEach-Object {
            $line = "$_"
            if ($line -match 'lifecycle accepted request_id=.* venue=([A-Za-z0-9]+) work=(\w+) status=(\w+)') {
                $lifecycleAccepted++
                $venue = $matches[1]
                if (-not $acceptedByVenue.ContainsKey($venue)) { $acceptedByVenue[$venue] = 0 }
                $acceptedByVenue[$venue]++
                if ($matches[2] -eq 'Refresh' -and $matches[3] -eq 'Prepared') { $preparedRefreshes++ }
            } else {
                Write-Output $line
            }
        } | Tee-Object -FilePath $log
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $priorPreference
    }
    $sw.Stop()
    $errors = @(Select-String -LiteralPath $log -Pattern 'level=ERROR|runtime fatal worker=|runtime shutdown drain_error=')
    $warnings = @(Select-String -LiteralPath $log -Pattern 'level=WARN')
    $warningFile = Join-Path $folder 'warnings_errors.log'
    @($warnings + $errors | ForEach-Object { $_.Line }) |
        Set-Content -LiteralPath $warningFile -Encoding UTF8
    $execution = $null
    if (Test-Path -LiteralPath $execReport) {
        $execution = Get-Content -LiteralPath $execReport -Raw | ConvertFrom-Json
    }
    $reports = if ($null -ne $execution -and $null -ne $execution.execution_reports) {
        @($execution.execution_reports)
    } else { @() }
    $requests = if ($null -ne $execution -and $null -ne $execution.execution_candidates -and
        $null -ne $execution.execution_candidates.requests) {
        @($execution.execution_candidates.requests)
    } else { @() }
    $summary = [ordered]@{
        branch=$branch; head=$head; elapsed_seconds=[math]::Round($sw.Elapsed.TotalSeconds,2)
        requested_duration_seconds=$DurationSeconds; exit_code=$exitCode
        runtime_ready=[bool](Select-String -LiteralPath $log -Pattern 'runtime ready=true' -Quiet)
        runtime_completed=[bool](Select-String -LiteralPath $log -Pattern 'runtime shutdown complete=true' -Quiet)
        lifecycle_started=[bool](Select-String -LiteralPath $log -Pattern 'lifecycle started' -Quiet)
        lifecycle_accepted_from_filtered_log=$lifecycleAccepted
        lifecycle_prepared_refreshes_from_filtered_log=$preparedRefreshes
        lifecycle_accepted_by_venue=$acceptedByVenue
        v10_candidates_started=[bool](Select-String -LiteralPath $log -Pattern 'execution candidates|v10.simulation' -Quiet)
        v10_requests=$requests.Count; v10_simulation_reports=$reports.Count
        v10_simulation_successes=@($reports | Where-Object { $_.outcome -like 'simulated_*' }).Count
        v10_simulation_failures=@($reports | Where-Object { $_.outcome -eq 'simulation_failed' }).Count
        v10_handoff_observed=if ($null -ne $execution) { $execution.execution_handoff_observed } else { $false }
        signal_output_loss_warning=[bool](Select-String -LiteralPath $log -Pattern 'signal delivery_loss=true' -Quiet)
        warnings=$warnings.Count; errors=$errors.Count
        transactions_sent=0; execution_mode='simulateTransaction_only'
        artifacts=$folder
    }
    $summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $folder 'summary.json') -Encoding UTF8
    $summary | Format-List
    Write-Host "Audit completed. Artifacts: $folder"
    if ($exitCode -ne 0) { throw "Runtime exit_code=$exitCode. Inspect summary.json and warnings_errors.log." }
} finally {
    foreach ($name in $envNames) {
        [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process')
    }
    Pop-Location
}