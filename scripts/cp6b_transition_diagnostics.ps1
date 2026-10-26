# CP6B-DIAG real 15-minute observation run: Registry -> Graph -> 15k MarketState -> all four signal kinds.
# Uses existing local .env RPC/database credentials. Does not submit or simulate transactions.
param(
    [ValidateRange(30,86400)][int]$DurationSeconds = 900,
    [ValidateRange(1,50000)][int]$PoolLimit = 15000,
    [ValidateRange(1,100000)][int]$AccountBudget = 25000,
    [string]$ExpectedBranch = 'cp6b-pool-transition-diagnostics'
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
    $folder=Join-Path $repo "runtime_cp6b_diag_live\$stamp"
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
            'liq_graph::workers::pool_registry=info',
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
    Write-Host "CP6B-DIAG checkout: $branch $head"
    Write-Host "Output: $folder"
    & cargo build --locked --release
    if ($LASTEXITCODE -ne 0){throw 'cargo build --locked --release failed; live run not started.'}
    & git diff --exit-code -- Cargo.lock
    if ($LASTEXITCODE -ne 0){throw 'Build modified Cargo.lock; stopping.'}
    $binary=Join-Path $repo 'target\release\liq_graph.exe'
    if (-not (Test-Path -LiteralPath $binary)){throw "Missing binary $binary"}
    Write-Host "Starting real $DurationSeconds-second CP6B-DIAG run, no V10 execution"
    & $binary --run 2>&1 | Tee-Object -FilePath $log
    $exitCode=$LASTEXITCODE
    $selection=@(Select-String -LiteralPath $log -Pattern 'market state cp5 coverage selected='|Select-Object -Last 1)
    $progress=@(Select-String -LiteralPath $log -Pattern 'market state hydration progress selected='|Select-Object -Last 1)
    $graphUpdates=@(Select-String -LiteralPath $log -Pattern 'module=liq_graph::workers::graph_liquidity graph incremental generation=')
    $legacyRebuild=@(Select-String -LiteralPath $log -Pattern 'module=liq_graph::workers::graph_liquidity rebuild generation=')
    # Audit records are one JSON payload per real transition, not a sampled subset.
    function Read-EventRecords($marker) {
        $matches=@(Select-String -LiteralPath $log -SimpleMatch $marker)
        $records=@(foreach($match in $matches){
            $index=$match.Line.IndexOf($marker,[StringComparison]::Ordinal)
            if($index -lt 0){continue}
            $payload=$match.Line.Substring($index+$marker.Length)
            $payload | ConvertFrom-Json -ErrorAction Stop
        })
        return ,$records
    }
    $transitions=@(Read-EventRecords 'graph pool_transition event=')
    $observed=@($transitions | Where-Object {$_.event -eq 'observed'})
    $published=@($transitions | Where-Object {$_.event -eq 'published'})
    $admissions=@(Read-EventRecords 'pool admission_transition event=')
    $mintTransitions=@(Read-EventRecords 'mint qualification_transition event=')
    $transitionLines=@(Select-String -LiteralPath $log -SimpleMatch 'graph transition_summary generation=')
    $reportedObserved=0;$reportedEffective=0;$reportedNoEffect=0;$bootstrapGenerations=0
    foreach($line in $transitionLines){
        if($line.Line -match '\bbootstrap=true\b'){$bootstrapGenerations++;continue}
        if($line.Line -match '\bobserved=(\d+)'){$reportedObserved += [long]$Matches[1]}
        if($line.Line -match '\beffective=(\d+)'){$reportedEffective += [long]$Matches[1]}
        if($line.Line -match '\bno_effect=(\d+)'){$reportedNoEffect += [long]$Matches[1]}
    }
    if($reportedObserved -ne $observed.Count -or $reportedEffective -ne $published.Count){
        throw "Graph transition evidence mismatch: observed=$($observed.Count)/$reportedObserved published=$($published.Count)/$reportedEffective"
    }
    $transitionRows=@(foreach($t in $observed){
        [pscustomobject][ordered]@{
            observed_at_ms=$t.observed_at_ms;generation=$t.generation
            pool=$t.pool;transition=$t.transition;source=$t.batch_source
            previous_state_duration_ms=$t.previous_state_duration_ms
            published_effective=$t.published_effective
            before_mint_a=if($null -ne $t.before_pair){$t.before_pair[0]}else{$null}
            before_mint_b=if($null -ne $t.before_pair){$t.before_pair[1]}else{$null}
            after_mint_a=if($null -ne $t.after_pair){$t.after_pair[0]}else{$null}
            after_mint_b=if($null -ne $t.after_pair){$t.after_pair[1]}else{$null}
            venue=$t.after.venue
            before_admission=$t.before.admission;after_admission=$t.after.admission
            before_completion=$t.before.completion;after_completion=$t.after.completion
            before_eligibility=$t.before.eligibility;after_eligibility=$t.after.eligibility
            before_reason=$t.before.completion_reason;after_reason=$t.after.completion_reason
            canonical_tvl_usd=$t.after.canonical_tvl_usd
            completion_validated_slot=$t.after.validated_slot
        }
    })
    $transitionRows | Export-Csv -LiteralPath (Join-Path $folder 'pool_transitions.csv') -NoTypeInformation -Encoding UTF8
    @($published | ConvertTo-Json -Depth 9 -Compress) | Set-Content -LiteralPath (Join-Path $folder 'published_transitions.json') -Encoding UTF8
    @($admissions | ConvertTo-Json -Depth 7 -Compress) | Set-Content -LiteralPath (Join-Path $folder 'pool_admission_transitions.json') -Encoding UTF8
    @($mintTransitions | ConvertTo-Json -Depth 7 -Compress) | Set-Content -LiteralPath (Join-Path $folder 'mint_qualification_transitions.json') -Encoding UTF8
    $flap=@(foreach($group in ($observed | Group-Object pool)){
        $timeline=@($group.Group | Sort-Object observed_at_ms)
        $adds=@($timeline | Where-Object {$_.transition -eq 'ADDED'}).Count
        $removes=@($timeline | Where-Object {$_.transition -eq 'REMOVED'}).Count
        $returns=0
        $minimumAbsent=$null
        for($i=1;$i -lt $timeline.Count;$i++){
            if($timeline[$i-1].transition -eq 'REMOVED' -and $timeline[$i].transition -eq 'ADDED'){
                $returns++
                $duration=[long]$timeline[$i].observed_at_ms-[long]$timeline[$i-1].observed_at_ms
                if($null -eq $minimumAbsent -or $duration -lt $minimumAbsent){$minimumAbsent=$duration}
            }
        }
        [pscustomobject][ordered]@{
            pool=$group.Name;transitions=$timeline.Count
            added=$adds;removed=$removes;remove_then_readd=$returns
            minimum_absent_ms=$minimumAbsent
            first_at_ms=$timeline[0].observed_at_ms
            last_at_ms=$timeline[-1].observed_at_ms
            sources=(@($timeline | ForEach-Object {$_.batch_source} | Sort-Object -Unique) -join ';')
            last_admission=$timeline[-1].after.admission
            last_eligibility=$timeline[-1].after.eligibility
        }
    })
    $flap=@($flap | Sort-Object @{Expression='remove_then_readd';Descending=$true},@{Expression='transitions';Descending=$true})
    $flap | Export-Csv -LiteralPath (Join-Path $folder 'pool_flapping.csv') -NoTypeInformation -Encoding UTF8
    $sourceStats=@($observed | Group-Object batch_source | Sort-Object Count -Descending |
        ForEach-Object {[pscustomobject]@{source=$_.Name;observed=$_.Count}})
    $sourceStats | Export-Csv -LiteralPath (Join-Path $folder 'transition_sources.csv') -NoTypeInformation -Encoding UTF8

    $production=if(Test-Path -LiteralPath $productionPath){Get-Content $productionPath -Raw | ConvertFrom-Json}else{$null}
    $consumer=if(Test-Path -LiteralPath $consumerPath){Get-Content $consumerPath -Raw | ConvertFrom-Json}else{$null}
    $ready=if($null -ne $consumer){$consumer.websocket_ready_anchors_by_kind}else{$null}
    $ranges=if($null -ne $consumer){$consumer.ranges_by_kind}else{$null}
    $finished=[bool](Select-String -LiteralPath $log -Pattern 'runtime shutdown complete=true' -Quiet)
    $summary=[ordered]@{
        branch=$branch;head=$head;exit_code=$exitCode;runtime_completed=$finished
        target_pools=$PoolLimit;account_budget=$AccountBudget
        selection_record=if($selection.Count){$selection[0].Line}else{$null}
        compact_graph_updates=$graphUpdates.Count
        graph_transition_observed=$observed.Count
        graph_transition_published=$published.Count
        graph_transition_no_effect=$reportedNoEffect
        graph_transition_bootstrap_generations=$bootstrapGenerations
        pool_transition_distinct_pools=@($observed | Select-Object -ExpandProperty pool -Unique).Count
        pool_flapping_suspects=@($flap | Where-Object {$_.remove_then_readd -gt 0}).Count
        top_flapping_pools=@($flap | Select-Object -First 15)
        transition_sources=$sourceStats
        mint_qualification_crossings=$mintTransitions.Count
        pool_admission_crossings=$admissions.Count
        last_compact_graph_update=if($graphUpdates.Count){$graphUpdates[-1].Line}else{$null}
        legacy_graph_rebuilds=$legacyRebuild.Count
        graph_bootstrap_legacy_groups=0
        hydration_record=if($progress.Count){$progress[0].Line}else{$null}
        signal_error=if($null -ne $production){$production.error}else{'missing production report'}
        signal_websocket_evaluations=if($null -ne $production){$production.counts.websocket_evaluations}else{0}
        shared_dependency_index=if($null -ne $production -and $null -ne $production.last_plan){$production.last_plan.dependency_index_shared}else{$false}
        compact_topologies=if($null -ne $production -and $null -ne $production.last_plan){$production.last_plan.compact_topologies}else{0}
        producer_candidates=if($null -ne $production){$production.counts.candidates}else{0}
        consumer_candidates=if($null -ne $consumer){$consumer.counts.candidates}else{0}
        legacy_groups_compiled=if($null -ne $production -and $null -ne $production.last_plan){$production.last_plan.legacy_groups_compiled}else{$null}
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
    if ($exitCode -ne 0 -or -not $finished){throw 'CP6B-DIAG runtime failed or shutdown incomplete; inspect runtime.log'}
    if ($summary.compact_graph_updates -lt 2 -or $summary.graph_transition_observed -le 0 -or $summary.graph_transition_published -le 0){
        throw 'CP6B-DIAG missing post-bootstrap per-pool transition evidence.'
    }
    if ($null -eq $production -or $null -eq $consumer){throw 'Missing signal evidence; inspect logs'}
    if (-not $summary.shared_dependency_index -or $summary.compiled_signal_generation -le 0){
        throw 'CP6B-DIAG compact graph generation or shared index missing.'
    }
    if ($summary.compact_graph_updates -lt 2 -or $summary.legacy_graph_rebuilds -ne 0){
        throw 'CP6B-DIAG incremental graph update evidence missing or legacy graph rebuild detected.'
    }
    if ($summary.compact_topologies -le 0 -or $summary.legacy_groups_compiled -ne 0){
        throw 'CP6B-DIAG still compiles legacy signal groups.'
    }
    if ($summary.producer_candidates -le 0 -or $summary.consumer_candidates -le 0){
        throw 'No compact candidates observed in real provider signals.'
    }
    if ($summary.producer_candidates -ne $summary.consumer_candidates){
        throw 'Producer/consumer candidate counts do not match.'
    }

    if ($production.counts.websocket_evaluations -le 0 -or $consumer.lost_events -gt 0){
        throw 'CP6B-DIAG websocket evidence missing or consumer events lost'
    }
    if (-not $summary.direct3_ready -or -not $summary.triangle4_ready){
        throw 'Direct3 or Triangle4 has no websocket-ready anchor: inspect CP6B-DIAG group coverage and conversion quotes.'
    }
    Write-Host 'CP6B-DIAG live acceptance evidence observed. Review runtime.log and summary.json.'
} finally {
    foreach($name in $names){[Environment]::SetEnvironmentVariable($name,$saved[$name],'Process')}
    Pop-Location
}
