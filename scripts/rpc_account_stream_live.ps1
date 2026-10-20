param(
    [ValidateRange(60,86400)][int]$DurationSeconds = 600,
    [ValidateRange(0,2147483647)][int]$PoolLimit = 1000,
    [string]$AccountsPath = "",
    [switch]$DecodePrices,
    [ValidateRange(1,86400)][int]$TransitionTimeoutSeconds = 600
)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$branch = git branch --show-current
if ($LASTEXITCODE -ne 0) { throw "Cannot read branch." }
$head = git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw "Cannot read HEAD." }
$status = @(git status --short)
if ($LASTEXITCODE -ne 0) { throw "Cannot inspect checkout." }
$tracked = git status --short --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $tracked) { throw "Tracked changes must be committed before this live run." }
$runFlag = if ($DecodePrices) { "--market-state-decode-live" } else { "--rpc-account-stream-prepared-live" }
if ($DecodePrices -and $AccountsPath) { throw "Price decoding requires Prepared pool identities; omit AccountsPath." }
$accountText = $null
if ($AccountsPath) {
    $AccountsPath = (Resolve-Path -LiteralPath $AccountsPath).Path
    $accountText = Get-Content -LiteralPath $AccountsPath -Raw
    if (@($accountText | ConvertFrom-Json).Count -lt 4) { throw "Provide at least four real accounts." }
    $runFlag = "--rpc-account-stream-live"
}
Write-Host "Account/Price-Stream decode_prices=$([bool]$DecodePrices) branch=$branch head=$head pool_limit=$PoolLimit input=$runFlag execution=false"
cargo build --locked --release
if ($LASTEXITCODE -ne 0) { throw "Locked build failed; no live run started." }
git diff --exit-code -- Cargo.lock
if ($LASTEXITCODE -ne 0) { throw "Cargo.lock changed during build." }
$tracked = git status --short --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $tracked) { throw "Tracked checkout changed during build." }

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ")
$outputRoot = if ($DecodePrices) { "market_state_live" } else { "rpc_account_stream_live" }
$folder = Join-Path (Get-Location) "$outputRoot\$stamp"
New-Item -ItemType Directory -Path $folder | Out-Null
$utf8 = [System.Text.UTF8Encoding]::new($false)
$inputCopy = Join-Path $folder "accounts.json"
if ($null -ne $accountText) { [System.IO.File]::WriteAllText($inputCopy,$accountText,$utf8) }
$preparation = Join-Path $folder "preparation.json"
$report = Join-Path $folder "report.json"
$log = Join-Path $folder "worker.log"
$metadata = Join-Path $folder "checkout.json"
$checkout = @{ branch=$branch; head=$head; status=$status; measured_seconds=$DurationSeconds; pool_limit=$PoolLimit;
    transition_timeout_seconds=$TransitionTimeoutSeconds; input=$runFlag; transport_config="RPC_N_ENV"; decode_prices=[bool]$DecodePrices }
# ENV provider flags and limits are authoritative. Disable the older aggregate test overrides.
# The file supplies provider values; old shell overrides must not shadow its WS keys.
$values = @{
    RPC_ACCOUNT_STREAM_ACCOUNTS_PATH=$inputCopy
    RPC_ACCOUNT_STREAM_REPORT_PATH=$report
    RPC_ACCOUNT_STREAM_DURATION_SECONDS="$DurationSeconds"
    RPC_ACCOUNT_STREAM_TRANSITION_TIMEOUT_SECONDS="$TransitionTimeoutSeconds"
    MARKET_STATE_POOL_LIMIT="$PoolLimit"
    MARKET_STATE_DURATION_SECONDS="$DurationSeconds"
    MARKET_STATE_TRANSITION_TIMEOUT_SECONDS="$TransitionTimeoutSeconds"
    MARKET_STATE_REPORT_PATH=$report
    MARKET_STATE_ACCOUNT_PLAN_PATH=$preparation
    RPC_WS_PROVIDER_SELECTION="configured"
    RPC_WS_ACCOUNT_BUDGET="0"
    RPC_WS_CONNECTIONS_PER_PROVIDER="0"
}
$previous = @{}
foreach ($name in $values.Keys) {
    $previous[$name] = [Environment]::GetEnvironmentVariable($name,"Process")
    [Environment]::SetEnvironmentVariable($name,$values[$name],"Process")
}
try {
    $envFile = Join-Path (Get-Location) ".env"
    $ignoredWsOverrides = @()
    if (Test-Path -LiteralPath $envFile) {
        # Read key names only. dotenvy in each child binary still parses all values.
        foreach ($line in [System.IO.File]::ReadLines($envFile)) {
            if ($line -match '^\s*(?:export\s+)?(RPC_[1-9][0-9]*_WS_[A-Z0-9_]+)\s*=') {
                $name = $Matches[1]
                if (-not $previous.ContainsKey($name)) {
                    $previous[$name] = [Environment]::GetEnvironmentVariable($name,"Process")
                    if ($null -ne $previous[$name]) { $ignoredWsOverrides += $name }
                }
                [Environment]::SetEnvironmentVariable($name,$null,"Process")
            }
        }
    }
    $checkout["repository_env_present"] = Test-Path -LiteralPath $envFile
    $checkout["ignored_ws_process_variables"] = $ignoredWsOverrides
    Write-Host "WS configuration: repository .env first; ignored inherited overrides=$($ignoredWsOverrides.Count)"
    [System.IO.File]::WriteAllText($metadata,($checkout | ConvertTo-Json -Depth 10),$utf8)
    $binary = Join-Path (Get-Location) "target\release\liq_graph.exe"
    $planText = & $binary --rpc-ws-plan
    if ($LASTEXITCODE -ne 0) { throw "Invalid WebSocket configuration." }
    $plan = ($planText -join [Environment]::NewLine) | ConvertFrom-Json
    $checkout["websocket_plan"] = $plan
    [System.IO.File]::WriteAllText($metadata,($checkout | ConvertTo-Json -Depth 10),$utf8)
    [System.IO.File]::WriteAllText((Join-Path $folder "websocket_plan.json"),($plan | ConvertTo-Json -Depth 8),$utf8)
    Write-Host "Evidence: $folder"
    $plan.providers | Format-Table index,name,max_connections,max_subscriptions,subscriptions_per_connection,subscribe_spacing_ms | Out-Host
    if ($plan.account_capacity -le 0) { throw "No WS provider enabled. Set the intended RPC_N_WS_ACCOUNT_SUBSCRIBE=true in .env." }
    Write-Host "Provider ENV capacity=$($plan.account_capacity); measurement=$DurationSeconds seconds after ACKs, plus preparation and transitions."
    $oldPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $binary $runFlag 2>&1 | Tee-Object -FilePath $log
        $workerExit = $LASTEXITCODE
    } finally { $ErrorActionPreference = $oldPreference }
    if (Test-Path -LiteralPath $inputCopy) {
        $checkout["accounts"] = @(Get-Content -LiteralPath $inputCopy -Raw | ConvertFrom-Json).Count
        $checkout["accounts_sha256"] = (Get-FileHash -LiteralPath $inputCopy -Algorithm SHA256).Hash
    }
    [System.IO.File]::WriteAllText($metadata,($checkout | ConvertTo-Json -Depth 10),$utf8)
    if (Test-Path -LiteralPath $preparation) {
        $prepared = Get-Content -LiteralPath $preparation -Raw | ConvertFrom-Json
        Write-Host "Prepared plan: selected=$($prepared.selected_pools) planned=$($prepared.planned_pools) distinct_accounts=$($prepared.unique_accounts) capacity=$($prepared.account_capacity) preparation_seconds=$($prepared.preparation_seconds)"
    }
    if (-not (Test-Path -LiteralPath $report)) {
        $partial = @{ run_complete=$false; live_evidence_passed=$false; worker_exit=$workerExit; error="Worker produced no account/price report; inspect preparation.json and worker.log." }
        [System.IO.File]::WriteAllText($report,($partial | ConvertTo-Json),$utf8)
    }
    $evidence = Get-Content -LiteralPath $report -Raw | ConvertFrom-Json
    if ($DecodePrices) {
        Write-Host "Decoder passed=$($evidence.decoding_evidence_passed) transport continuity=$($evidence.continuity_passed)"
        Write-Host "Prices published=$($evidence.producer.websocket_prices) consumed=$($evidence.consumer.websocket_prices) account updates=$($evidence.producer.account_updates)"
        foreach ($phase in $evidence.consumer.phases.PSObject.Properties) {
            $m = $phase.Value
            Write-Host "$($phase.Name): prices=$($m.websocket_prices) replay=$($m.initialization_replays) market_mean_ms=$($m.market_to_publish.mean_ns / 1000000) market_max_ms=$($m.market_to_publish.max_ns / 1000000) frame_to_consumer_max_ms=$($m.frame_to_consumer.max_ns / 1000000)"
        }
        if ($workerExit -ne 0 -or $evidence.decoding_evidence_passed -ne $true) { throw "Market-State decode run failed. Inspect report.json and worker.log." }
    } else {
        Write-Host "Complete=$($evidence.run_complete) passed=$($evidence.live_evidence_passed) retained_activity_during_transitions=$($evidence.transition_activity_observed)"
        $evidence.phases | Select-Object name,requested_accounts,transition_seconds,steady_seconds,
            @{Name="steady_updates_s";Expression={$_.steady.updates_per_second}},
            @{Name="steady_max_ns";Expression={$_.steady.frame_to_consumer.max_ns}},
            continuing_updates_during_transition | Format-Table | Out-Host
        if ($workerExit -ne 0 -or $evidence.live_evidence_passed -ne $true) { throw "Account-Stream CP1.2 failed. Inspect report.json, preparation.json and worker.log." }
    }
} finally {
    foreach ($name in $previous.Keys) { [Environment]::SetEnvironmentVariable($name,$previous[$name],"Process") }
}
