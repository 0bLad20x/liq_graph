param(
    [ValidateRange(30, 86400)][int]$DurationSeconds = 600,
    [ValidateRange(0, 2147483647)][int]$PoolLimit = 16,
    [double]$ThresholdBps = 0,
    [switch]$TracePrices,
    [switch]$TraceSignals,
    [switch]$ProfileSignals,
    [string]$WsProviders = "Configured",
    [ValidateRange(0, 2147483647)][int]$WsAccountBudget = 0,
    [ValidateRange(0, 64)][int]$WsConnectionsPerProvider = 0,
    [switch]$PlanOnly,
    [switch]$Rewatch,
    [switch]$SimulateV10,
    [string]$WalletPath,
    [ValidateRange(1, 1000)][int]$SimulationLimit = 10,
    [ValidateSet("All", "Direct2", "Direct3", "Triangle3", "Triangle4")][string]$RouteKind = "All",
    [string]$ExecutionParametersPath
)

# Share checkout/build guards, evidence capture and environment restoration.
& (Join-Path $PSScriptRoot "market_state_live.ps1") -Signals `
    -DurationSeconds $DurationSeconds -PoolLimit $PoolLimit -ThresholdBps $ThresholdBps `
    -WsProviders $WsProviders -WsAccountBudget $WsAccountBudget -WsConnectionsPerProvider $WsConnectionsPerProvider `
    -PlanOnly:$PlanOnly `
    -TracePrices:$TracePrices -TraceSignals:$TraceSignals -ProfileSignals:$ProfileSignals -Rewatch:$Rewatch `
    -SimulateV10:$SimulateV10 -WalletPath $WalletPath -SimulationLimit $SimulationLimit `
    -RouteKind $RouteKind -ExecutionParametersPath $ExecutionParametersPath
