# Current real exact graph delta observation run: Registry -> Graph -> 15k MarketState -> all four signal kinds.
# Uses existing local .env RPC/database credentials. Does not submit or simulate transactions.
param(
    [ValidateRange(30,86400)][int]$DurationSeconds = 900,
    [ValidateRange(1,50000)][int]$PoolLimit = 15000,
    [ValidateRange(1,100000)][int]$AccountBudget = 25000,
    [string]$ExpectedBranch = 'main'
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $repo
$names=@(
    'RUN_DURATION_SECONDS','RUN_SHUTDOWN_SECONDS','MARKET_STATE_ENABLED',
    'GRAPH_SIGNALS_ENABLED','CP1_MARKET_ENRICHMENT_ENABLED',
    'GRAPH_EXECUTION_SIMULATE','MARKET_STATE_POOL_LIMIT',
    'MARKET_STATE_WS_ACCOUNT_BUDGET','MARKET_STATE_SELECTION_INTERVAL_MS',
    'MARKET_STATE_HYDRATION_CONCURRENCY','MINT_ONCHAIN_PARALLEL_BATCHES',
    'GRAPH_SIGNALS_THRESHOLD_BPS','GRAPH_SIGNALS_PROFILE',
    'GRAPH_RUNTIME_PRODUCTION_REPORT_PATH','GRAPH_RUNTIME_SIGNAL_REPORT_PATH','LIQ_GRAPH_LOG','GRAPH_DIAGNOSTIC_JSONL_PATH','CP6C_REFERENCE_EVERY'
)
$saved=@{}
try {
    $branch=(& git branch --show-current)
    if ($LASTEXITCODE -ne 0 -or $branch -ne $ExpectedBranch) {
        throw "Wrong branch: '$branch', expected '$ExpectedBranch'"
    }
    $head=(& git rev-parse HEAD)
    $tracked=@(& git status --short --untracked-files=no)
    if ($tracked.Count -gt 0) { throw "Tracked edits exist: $($tracked -join ', ')" }
    & git diff --exit-code -- Cargo.lock
    if ($LASTEXITCODE -ne 0) { throw 'Lockfile has modifications.' }
    $stamp=(Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ')
    $folder=Join-Path $repo "runtime_graph_live\$stamp"
    New-Item -Path $folder -ItemType Directory -Force | Out-Null
    $log=Join-Path $folder 'runtime.log'
    $productionPath=Join-Path $folder 'signal_production.json'
    $consumerPath=Join-Path $folder 'signal_consumer.json'
    $auditPath=Join-Path $folder 'graph_pool_audit.jsonl'
    $settings=@{
        RUN_DURATION_SECONDS="$DurationSeconds"
        RUN_SHUTDOWN_SECONDS='300'
        MARKET_STATE_ENABLED='true'
        GRAPH_SIGNALS_ENABLED='true'
        CP1_MARKET_ENRICHMENT_ENABLED='true'
        GRAPH_EXECUTION_SIMULATE='false'
        MARKET_STATE_POOL_LIMIT="$PoolLimit"
        MARKET_STATE_WS_ACCOUNT_BUDGET="$AccountBudget"
        MARKET_STATE_SELECTION_INTERVAL_MS='5000'
        MARKET_STATE_HYDRATION_CONCURRENCY='8'
        MINT_ONCHAIN_PARALLEL_BATCHES='4'
        GRAPH_SIGNALS_THRESHOLD_BPS='0'
        GRAPH_SIGNALS_PROFILE='true'
        GRAPH_RUNTIME_PRODUCTION_REPORT_PATH="$productionPath"
        GRAPH_RUNTIME_SIGNAL_REPORT_PATH="$consumerPath"
        GRAPH_DIAGNOSTIC_JSONL_PATH="$auditPath"
        CP6C_REFERENCE_EVERY='10'
        LIQ_GRAPH_LOG=(
            'warn',
            'liq_graph::orchestration::runtime=info',
            'liq_graph::workers::market_state=info',
            'liq_graph::workers::graph_liquidity=info',
            'liq_graph::workers::graph_watch_catalog=info',
            'liq_graph::workers::graph_signals=info',
            'liq_graph::workers::mint_extensions=info',
            'liq_graph::workers::mint_qualification=info',
            'liq_graph::workers::persistence=info',
            'liq_graph::workers::rpc_transport=info'
        ) -join ','
    }
    foreach($name in $names){
        $saved[$name]=[Environment]::GetEnvironmentVariable($name,'Process')
        [Environment]::SetEnvironmentVariable($name,$settings[$name],'Process')
    }
    $checkout=[ordered]@{
        branch=$branch;head=$head;start_ms=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        duration_seconds=$DurationSeconds;pool_limit=$PoolLimit
        ws_account_budget=$AccountBudget;transactions_sent=0;simulation_enabled=$false
    }
    $checkout|ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $folder 'checkout.json') -Encoding UTF8
    Write-Host "Graph checkout: $branch $head"
    Write-Host "Output: $folder"
    & cargo build --locked --release
    if ($LASTEXITCODE -ne 0){throw 'cargo build --locked --release failed; live run not started.'}
    & git diff --exit-code -- Cargo.lock
    if ($LASTEXITCODE -ne 0){throw 'Build modified Cargo.lock; stopping.'}
    $binary=Join-Path $repo 'target\release\liq_graph.exe'
    if (-not (Test-Path -LiteralPath $binary)){throw "Missing binary $binary"}
    Write-Host "Starting real $DurationSeconds-second graph delta verification, audit file: $auditPath"
    & $binary --run 2>&1 | Out-File -FilePath $log -Encoding utf8
    $exitCode=$LASTEXITCODE
    $analyzer=Join-Path $repo 'scripts\graph_live_analyze.py'
    & python $analyzer --audit $auditPath --runtime $log --output $folder --exit-code $exitCode --min-duration ([Math]::Max(30,$DurationSeconds - 15))
    if ($LASTEXITCODE -ne 0) {
        throw "Graph live audit not accepted; all evidence retained in $folder"
    }
    Write-Host "Graph live evidence accepted. Analyze graph_generations.csv, pool_flapping.csv and provider_websocket_health.csv in $folder."
} finally {
    foreach($name in $names){[Environment]::SetEnvironmentVariable($name,$saved[$name],'Process')}
    Pop-Location
}
