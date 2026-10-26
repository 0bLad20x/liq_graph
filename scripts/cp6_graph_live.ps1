# CP6 real 15-minute observation run: Registry -> Graph -> 15k MarketState -> all four signal kinds.
# Uses existing local .env RPC/database credentials. Does not submit or simulate transactions.
param(
    [ValidateRange(30,86400)][int]$DurationSeconds = 900,
    [ValidateRange(1,50000)][int]$PoolLimit = 15000,
    [ValidateRange(1,100000)][int]$AccountBudget = 25000,
    [string]$ExpectedBranch = 'cp6-shared-topology-signal-fastpath'
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
    'GRAPH_RUNTIME_PRODUCTION_REPORT_PATH','GRAPH_RUNTIME_SIGNAL_REPORT_PATH','LIQ_GRAPH_LOG'
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
    $folder=Join-Path $repo "runtime_cp6_live\$stamp"
    New-Item -Path $folder -ItemType Directory -Force | Out-Null
    $log=Join-Path $folder 'runtime.log'
    $productionPath=Join-Path $folder 'signal_production.json'
    $consumerPath=Join-Path $folder 'signal_consumer.json'
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
    Write-Host "CP6 checkout: $branch $head"
    Write-Host "Output: $folder"
    & cargo build --locked --release
    if ($LASTEXITCODE -ne 0){throw 'cargo build --locked --release failed; live run not started.'}
    & git diff --exit-code -- Cargo.lock
    if ($LASTEXITCODE -ne 0){throw 'Build modified Cargo.lock; stopping.'}
    $binary=Join-Path $repo 'target\release\liq_graph.exe'
    if (-not (Test-Path -LiteralPath $binary)){throw "Missing binary $binary"}
    Write-Host "Starting real $DurationSeconds-second CP6 run, no V10 execution"
    & $binary --run 2>&1 | Tee-Object -FilePath $log
    $exitCode=$LASTEXITCODE
    $selection=@(Select-String -LiteralPath $log -Pattern 'market state cp6 coverage selected='|Select-Object -Last 1)
    $progress=@(Select-String -LiteralPath $log -Pattern 'market state hydration progress selected='|Select-Object -Last 1)
    $production=if(Test-Path -LiteralPath $productionPath){Get-Content $productionPath -Raw | ConvertFrom-Json}else{$null}
    $consumer=if(Test-Path -LiteralPath $consumerPath){Get-Content $consumerPath -Raw | ConvertFrom-Json}else{$null}
    $ready=if($null -ne $consumer){$consumer.websocket_ready_anchors_by_kind}else{$null}
    $ranges=if($null -ne $consumer){$consumer.ranges_by_kind}else{$null}
    $finished=[bool](Select-String -LiteralPath $log -Pattern 'runtime shutdown complete=true' -Quiet)
    $summary=[ordered]@{
        branch=$branch;head=$head;exit_code=$exitCode;runtime_completed=$finished
        target_pools=$PoolLimit;account_budget=$AccountBudget
        selection_record=if($selection.Count){$selection[0].Line}else{$null}
        hydration_record=if($progress.Count){$progress[0].Line}else{$null}
        signal_error=if($null -ne $production){$production.error}else{'missing production report'}
        signal_websocket_evaluations=if($null -ne $production){$production.counts.websocket_evaluations}else{0}
        shared_dependency_index=if($null -ne $production -and $null -ne $production.last_plan){$production.last_plan.dependency_index_shared}else{$false}
        compiled_signal_generation=if($null -ne $production -and $null -ne $production.last_plan){$production.last_plan.generation}else{$null}
        signal_plan_build_ns=if($null -ne $production){$production.catalog_plan_build_ns}else{$null}
        max_price_queue_depth=if($null -ne $production){$production.max_price_queue_depth}else{$null}
        price_queue_us=if($null -ne $production){$production.websocket_inputs.price_queue_us}else{$null}
        websocket_frame_to_range_consumer_us=if($null -ne $consumer){$consumer.websocket_frame_to_range_consumer_us}else{$null}
        consumer_delivery_loss=if($null -ne $consumer){$consumer.lost_events}else{$null}
        websocket_ready_anchors_by_kind=$ready
        ranges_by_kind=$ranges
        direct3_ready=if($null -ne $ready){$ready.direct_3 -gt 0}else{$false}
        triangle4_ready=if($null -ne $ready){$ready.triangle_4 -gt 0}else{$false}
        simulation_enabled=$false;transactions_sent=0
    }
    $summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $folder 'summary.json') -Encoding UTF8
    $summary | Format-List
    if ($exitCode -ne 0 -or -not $finished){throw 'CP6 runtime failed or shutdown incomplete; inspect runtime.log'}
    if ($null -eq $production -or $null -eq $consumer){throw 'Missing signal evidence; inspect logs'}
    if (-not $summary.shared_dependency_index -or $summary.compiled_signal_generation -le 0){
        throw 'CP6 shared index or compiled real graph generation missing.'
    }
    if ($production.counts.websocket_evaluations -le 0 -or $consumer.lost_events -gt 0){
        throw 'CP6 websocket evidence missing or consumer events lost'
    }
    if (-not $summary.direct3_ready -or -not $summary.triangle4_ready){
        throw 'Direct3 or Triangle4 has no websocket-ready anchor: inspect CP6 group coverage and conversion quotes.'
    }
    Write-Host 'CP6 live acceptance evidence observed. Review runtime.log and summary.json.'
} finally {
    foreach($name in $names){[Environment]::SetEnvironmentVariable($name,$saved[$name],'Process')}
    Pop-Location
}
