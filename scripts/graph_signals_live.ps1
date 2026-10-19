param(
    [ValidateRange(30, 86400)][int]$DurationSeconds = 600,
    [ValidateRange(0, 2147483647)][int]$PoolLimit = 16,
    [double]$ThresholdBps = 0,
    [switch]$TracePrices,
    [switch]$TraceSignals,
    [string]$WsProviders = "Configured",
    [ValidateRange(0, 2147483647)][int]$WsAccountBudget = 0,
    [ValidateRange(0, 64)][int]$WsConnectionsPerProvider = 0,
    [switch]$PlanOnly,
    [switch]$Rewatch
)

# Share checkout/build guards, evidence capture and environment restoration.
& (Join-Path $PSScriptRoot "market_state_live.ps1") -Signals `
    -DurationSeconds $DurationSeconds -PoolLimit $PoolLimit -ThresholdBps $ThresholdBps `
    -WsProviders $WsProviders -WsAccountBudget $WsAccountBudget -WsConnectionsPerProvider $WsConnectionsPerProvider `
    -PlanOnly:$PlanOnly `
    -TracePrices:$TracePrices -TraceSignals:$TraceSignals -Rewatch:$Rewatch
