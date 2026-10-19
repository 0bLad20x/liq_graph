param(
    [ValidateRange(30, 86400)][int]$DurationSeconds = 600,
    [ValidateRange(0, 2147483647)][int]$PoolLimit = 16,
    [double]$ThresholdBps = 0,
    [switch]$TracePrices,
    [switch]$TraceSignals,
    [switch]$Rewatch
)

# Share checkout/build guards, evidence capture and environment restoration.
& (Join-Path $PSScriptRoot "market_state_live.ps1") -Signals `
    -DurationSeconds $DurationSeconds -PoolLimit $PoolLimit -ThresholdBps $ThresholdBps `
    -TracePrices:$TracePrices -TraceSignals:$TraceSignals -Rewatch:$Rewatch
