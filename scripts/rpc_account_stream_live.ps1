param(
    [ValidateRange(60,86400)][int]$DurationSeconds = 300,
    [string]$AccountsPath = "",
    [string]$WsProviders = "5,6",
    [ValidateRange(1,2147483647)][int]$WsAccountBudget = 64,
    [ValidateRange(1,64)][int]$WsConnectionsPerProvider = 1
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
if (-not $AccountsPath) { $AccountsPath = Join-Path $PSScriptRoot "rpc_account_stream_accounts.json" }
$AccountsPath = (Resolve-Path -LiteralPath $AccountsPath).Path
$accountText = Get-Content -LiteralPath $AccountsPath -Raw
$accounts = @($accountText | ConvertFrom-Json)
if ($accounts.Count -lt 4) { throw "Provide at least four real account addresses in the JSON watch file." }
Write-Host "Account-Stream CP1 branch=$branch head=$head accounts=$($accounts.Count) execution=false"
cargo build --locked --release
if ($LASTEXITCODE -ne 0) { throw "Locked build failed; no live run started." }
git diff --exit-code -- Cargo.lock
if ($LASTEXITCODE -ne 0) { throw "Cargo.lock changed during build." }
$tracked = git status --short --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $tracked) { throw "Tracked checkout changed during build." }

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ")
$folder = Join-Path (Get-Location) "rpc_account_stream_live\$stamp"
New-Item -ItemType Directory -Path $folder | Out-Null
$utf8 = [System.Text.UTF8Encoding]::new($false)
$inputCopy = Join-Path $folder "accounts.json"
[System.IO.File]::WriteAllText($inputCopy,$accountText,$utf8)
$report = Join-Path $folder "report.json"
$log = Join-Path $folder "worker.log"
$checkout = @{ branch=$branch; head=$head; status=$status; duration_seconds=$DurationSeconds; accounts=$accounts.Count;
    accounts_sha256=(Get-FileHash -LiteralPath $inputCopy -Algorithm SHA256).Hash;
    ws_providers=$WsProviders; ws_account_budget=$WsAccountBudget; ws_connections_per_provider=$WsConnectionsPerProvider }
$values = @{
    RPC_ACCOUNT_STREAM_ACCOUNTS_PATH=$inputCopy
    RPC_ACCOUNT_STREAM_REPORT_PATH=$report
    RPC_ACCOUNT_STREAM_DURATION_SECONDS="$DurationSeconds"
    RPC_WS_PROVIDER_SELECTION=$WsProviders
    RPC_WS_ACCOUNT_BUDGET="$WsAccountBudget"
    RPC_WS_CONNECTIONS_PER_PROVIDER="$WsConnectionsPerProvider"
}
$previous = @{}
foreach ($name in $values.Keys) {
    $previous[$name] = [Environment]::GetEnvironmentVariable($name,"Process")
    [Environment]::SetEnvironmentVariable($name,$values[$name],"Process")
}
try {
    $binary = Join-Path (Get-Location) "target\release\liq_graph.exe"
    $planText = & $binary --rpc-ws-plan
    if ($LASTEXITCODE -ne 0) { throw "Invalid WebSocket configuration." }
    $plan = ($planText -join [Environment]::NewLine) | ConvertFrom-Json
    if ($plan.account_capacity -lt $accounts.Count) { throw "Watch file exceeds the selected account capacity." }
    $checkout["websocket_plan"] = $plan
    [System.IO.File]::WriteAllText((Join-Path $folder "checkout.json"),($checkout | ConvertTo-Json -Depth 10),$utf8)
    [System.IO.File]::WriteAllText((Join-Path $folder "websocket_plan.json"),($plan | ConvertTo-Json -Depth 8),$utf8)
    Write-Host "Evidence: $folder"
    $plan.providers | Format-Table index,name,max_connections,max_subscriptions,subscriptions_per_connection | Out-Host
    $oldPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $binary --rpc-account-stream-live 2>&1 | Tee-Object -FilePath $log
        $workerExit = $LASTEXITCODE
    } finally { $ErrorActionPreference = $oldPreference }
    if (-not (Test-Path -LiteralPath $report)) {
        $partial = @{ run_complete=$false; live_evidence_passed=$false; worker_exit=$workerExit; error="Worker produced no report; inspect worker.log." }
        [System.IO.File]::WriteAllText($report,($partial | ConvertTo-Json),$utf8)
    }
    $evidence = Get-Content -LiteralPath $report -Raw | ConvertFrom-Json
    Write-Host "Complete=$($evidence.run_complete) passed=$($evidence.live_evidence_passed)"
    if ($workerExit -ne 0 -or $evidence.live_evidence_passed -ne $true) { throw "Account-Stream CP1 failed. Inspect report.json and worker.log." }
} finally {
    foreach ($name in $values.Keys) { [Environment]::SetEnvironmentVariable($name,$previous[$name],"Process") }
}
