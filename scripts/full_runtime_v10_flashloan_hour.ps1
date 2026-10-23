# One-hour, multi-provider, simulation-only V10 load audit.
# Does not modify .env. Does not send transactions.
param(
    [ValidateRange(30,86400)][int]$DurationSeconds = 3600,
    [ValidateRange(1,2147483647)][int]$PoolLimit = 10000,
    [ValidateRange(1,5000)][int]$SimulationLimit = 3000,
    [string]$WalletPath = (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json'),
    [string]$ExpectedBranch = 'agent/v10-flashloan-hour-stress'
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $repo
$names = @('RPC_WS_PROVIDER_SELECTION','RPC_WS_ACCOUNT_BUDGET',
    'RPC_WS_CONNECTIONS_PER_PROVIDER','GRAPH_EXECUTION_PARAMETERS_PATH')
$previous = @{}
foreach ($name in $names) {
    $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
try {
    # All four endpoints were capacity-tested separately, not concurrently.
    # Disable global rebalancing: use each endpoint's individual tested cap.
    [Environment]::SetEnvironmentVariable('RPC_WS_PROVIDER_SELECTION','1,2,5,6','Process')
    [Environment]::SetEnvironmentVariable('RPC_WS_ACCOUNT_BUDGET','0','Process')
    [Environment]::SetEnvironmentVariable('RPC_WS_CONNECTIONS_PER_PROVIDER','0','Process')

    $runRoot = Join-Path $repo 'runtime_v10_live'
    New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
    $stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ')
    $parameterPath = Join-Path $runRoot ("flashloan_parameters_" + $stamp + ".json")
    # Explicit per-run settings; the program chooses the swap size, not this script.
    # UTF-8 without BOM is required for Rust's serde_json::from_slice.
    $parameters = @'
{
  "minimum_profit": 0,
  "trade_size": 0,
  "use_flashloan": true,
  "no_failure": false,
  "constant_conversion": false,
  "executor_compute_unit_limit": 1000000,
  "transaction_compute_unit_limit": 1400000,
  "loaded_accounts_data_size_limit": 67108864,
  "priority_fee_lamports": 0,
  "additional_fee_bp": 0,
  "additional_fee_collector": null
}
'@
    [System.IO.File]::WriteAllText($parameterPath,$parameters,[System.Text.UTF8Encoding]::new($false))
    [Environment]::SetEnvironmentVariable('GRAPH_EXECUTION_PARAMETERS_PATH',$parameterPath,'Process')

    $binary = Join-Path $repo 'target\release\liq_graph.exe'
    if (-not (Test-Path -LiteralPath $binary)) {
        throw "Missing release binary for WS plan: $binary. Build the checkout first with cargo build --locked --release."
    }
    $planJson = (& $binary --rpc-ws-plan | Out-String)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot load WebSocket plan; no live test started.' }
    $plan = $planJson | ConvertFrom-Json

    # Reject unexpected limits; .env is not modified.
    $expected = @{
        '1' = @{ sockets = 5; accounts = 5000 }
        '2' = @{ sockets = 5; accounts = 5000 }
        '5' = @{ sockets = 1; accounts = 10000 }
        '6' = @{ sockets = 1; accounts = 10000 }
    }
    if ($plan.account_capacity -ne 30000 -or $plan.socket_count -ne 12 -or
        @($plan.providers).Count -ne 4) {
        throw "Unexpected WS plan: account_capacity=$($plan.account_capacity), sockets=$($plan.socket_count), providers=$(@($plan.providers).Count). Expected 30000/12/4. No runtime started."
    }
    foreach ($provider in $plan.providers) {
        $index = [string]$provider.index
        if (-not $expected.ContainsKey($index)) {
            throw "Unexpected WS provider index=$index. No runtime started."
        }
        $requirement = $expected[$index]
        if ($provider.max_connections -ne $requirement.sockets -or
            $provider.max_subscriptions -ne $requirement.accounts -or
            $provider.subscriptions_per_connection -ne
                [int]($requirement.accounts / $requirement.sockets)) {
            throw "Provider $index differs from tested WS settings. No runtime started."
        }
    }

    $record = [ordered]@{
        started_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        duration_seconds = $DurationSeconds
        pool_limit = $PoolLimit
        simulation_limit = $SimulationLimit
        simulation_only = $true
        use_flashloan = $true
        trade_size = 0
        no_failure = $false
        ws_selection = '1,2,5,6'
        ws_plan = $plan
        execution_parameters_path = $parameterPath
        independent_provider_limits_only = $true
        combined_provider_stability_not_yet_proven = $true
    }
    $recordPath = Join-Path $runRoot ("flashloan_audit_setup_" + $stamp + ".json")
    [System.IO.File]::WriteAllText($recordPath,($record | ConvertTo-Json -Depth 9),
        [System.Text.UTF8Encoding]::new($false))
    Write-Host "WS plan: $($plan.socket_count) sockets, $($plan.account_capacity) account slots, providers=1,2,5,6"
    Write-Host "V10: use_flashloan=true trade_size=0 (on-chain auto sizing) no_failure=false"
    Write-Host "Duration=$DurationSeconds seconds pool_limit=$PoolLimit max_requests=$SimulationLimit"
    Write-Host "No on-chain transactions will be sent. The four endpoints are being tested together for the first time."
    Write-Host "Parameter file: $parameterPath"

    $runArguments = @{
        ExpectedBranch = $ExpectedBranch
        DurationSeconds = $DurationSeconds
        PoolLimit = $PoolLimit
        SimulationLimit = $SimulationLimit
        ThresholdBps = 0
        WalletPath = $WalletPath
    }
    & (Join-Path $PSScriptRoot 'full_runtime_v10_live.ps1') @runArguments
    if (-not $?) { throw 'Full runtime audit did not complete successfully; inspect runtime_v10_live reports.' }
}
finally {
    foreach ($name in $names) {
        [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process')
    }
    Pop-Location
}
