param(
    [ValidateRange(60, 86400)][int]$DurationSeconds = 600,
    [ValidateRange(1, 128)][int]$PoolLimit = 16,
    [ValidateSet("Dlmm", "DammV2", "Both", "RaydiumAmmV4", "RaydiumCpmm", "RaydiumClmm", "Raydium", "Orca", "PancakeSwap", "Byreal", "PumpSwap", "Manifest", "All")][string]$Venue = "All"
)

# Real protocol REST/RPC -> one lifecycle -> Registry -> existing PostgreSQL writer.
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$branch = git branch --show-current
$head = git rev-parse HEAD
$tracked = git status --short --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $branch -notin @("agent/cp2-lifecycle-final", "main", "cp3-central-persistence-dual-registry") -or $tracked) {
    throw "Use a clean, committed agent/cp2-lifecycle-final checkout, or main after merging this checkpoint."
}
cargo build --locked --release
if ($LASTEXITCODE -ne 0) { throw "cargo build --locked --release failed." }
git diff --exit-code -- Cargo.lock
if ($LASTEXITCODE -ne 0) { throw "Cargo.lock changed during build." }

$selector = switch ($Venue) {
    "Dlmm" { "dlmm" }
    "DammV2" { "damm_v2" }
    "Both" { "both" }
    "RaydiumAmmV4" { "raydium_amm_v4" }
    "RaydiumCpmm" { "raydium_cpmm" }
    "RaydiumClmm" { "raydium_clmm" }
    "Raydium" { "raydium" }
    "Orca" { "orca" }
    "PancakeSwap" { "pancakeswap" }
    "Byreal" { "byreal" }
    "PumpSwap" { "pumpswap" }
    "Manifest" { "manifest" }
    "All" { "all" }
}
$venueCodes = @{ MeteoraDlmm = 1; MeteoraDammV2 = 2; RaydiumAmmV4 = 3; RaydiumCpmm = 4; RaydiumClmm = 5; OrcaWhirlpool = 7; PancakeSwapClmm = 8; ByrealClmm = 9; Manifest = 10; PumpSwap = 11 }
$selectedVenues = switch ($Venue) {
    "Dlmm" { @("MeteoraDlmm") }
    "DammV2" { @("MeteoraDammV2") }
    "Both" { @("MeteoraDlmm", "MeteoraDammV2") }
    "RaydiumAmmV4" { @("RaydiumAmmV4") }
    "RaydiumCpmm" { @("RaydiumCpmm") }
    "RaydiumClmm" { @("RaydiumClmm") }
    "Raydium" { @("RaydiumAmmV4", "RaydiumCpmm", "RaydiumClmm") }
    "Orca" { @("OrcaWhirlpool") }
    "PancakeSwap" { @("PancakeSwapClmm") }
    "Byreal" { @("ByrealClmm") }
    "PumpSwap" { @("PumpSwap") }
    "Manifest" { @("Manifest") }
    "All" { @("MeteoraDlmm", "MeteoraDammV2", "RaydiumAmmV4", "RaydiumCpmm", "RaydiumClmm", "OrcaWhirlpool", "PancakeSwapClmm", "ByrealClmm", "Manifest", "PumpSwap") }
}
# Manifest is structurally migrated; its missing live TVL supply is explicitly
# deferred for the combined checkpoint. Its standalone proof remains strict.
$deferredVenues = if ($Venue -eq "All") { @("Manifest") } else { @() }
$requiredVenues = @($selectedVenues | Where-Object { $_ -notin $deferredVenues })
$settings = @{
    RUN_DURATION_SECONDS = "$DurationSeconds"
    LIQ_GRAPH_LOG = "info,completion_lifecycle=debug,completion_meteora_dlmm=debug,completion_meteora_damm_v2=debug,completion_raydium_amm_v4=debug,completion_raydium_cpmm=debug,completion_raydium_clmm=debug,completion_orca_whirlpool=debug,completion_pancakeswap_clmm=debug,completion_byreal_clmm=debug,completion_manifest=debug,completion_pumpswap=debug"
    COMPLETION_LIFECYCLE_LIVE_VENUE = $selector
    COMPLETION_LIFECYCLE_POOL_LIMIT = "$PoolLimit"
    COMPLETION_LIFECYCLE_REFRESH_SECONDS = "30"
    COMPLETION_LIFECYCLE_RECHECK_SECONDS = "45"
    COMPLETION_LIFECYCLE_RETRY_SECONDS = "5"
    COMPLETION_LIFECYCLE_RETRY_MAX_SECONDS = "30"
    COMPLETION_LIFECYCLE_ATTEMPT_SECONDS = "180"
    COMPLETION_LIFECYCLE_ACK_SECONDS = "120"
    DLMM_COMPLETION_PARALLEL_BATCHES = "2"
    DAMM_V2_COMPLETION_PARALLEL_BATCHES = "2"
}
$previous = @{}
foreach ($key in $settings.Keys) {
    $previous[$key] = [Environment]::GetEnvironmentVariable($key, "Process")
}
$log = "completion_lifecycle_${selector}_live.log"
$verification = "completion_lifecycle_${selector}_verify.sql"
# Remove a previous verification result before starting a new run.
if (Test-Path $verification) { Remove-Item $verification }
try {
    foreach ($key in $settings.Keys) {
        [Environment]::SetEnvironmentVariable($key, $settings[$key], "Process")
    }
    $startMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    "Completion CP2 branch=$branch head=$head start_ms=$startMs venues=$selector pool_limit_per_venue=$PoolLimit duration_s=$DurationSeconds" |
        Tee-Object -FilePath $log
    "Completion CP2 required_refresh=$($requiredVenues -join ',') deferred_refresh=$($deferredVenues -join ',')" |
        Tee-Object -FilePath $log -Append
    # Windows PowerShell 5 treats redirected native stderr as ErrorRecord.
    # Keep the real exit code authoritative while preserving the combined log.
    $ErrorActionPreference = "Continue"
    cargo run --locked --release -- --completion-lifecycle-live 2>&1 |
        Tee-Object -FilePath $log -Append
    $runtimeExit = $LASTEXITCODE
    $ErrorActionPreference = "Stop"
    if ($runtimeExit -ne 0) { throw "Runtime failed: $runtimeExit; inspect $log" }
} finally {
    $ErrorActionPreference = "Stop"
    foreach ($key in $settings.Keys) {
        [Environment]::SetEnvironmentVariable($key, $previous[$key], "Process")
    }
}

$text = Get-Content -Raw $log
if ($text -notmatch 'lifecycle live complete=true persisted=true') {
    throw "Missing completed live drain/persistence; inspect $log"
}
if ($text -notmatch 'lifecycle stats final=true .*in_flight=0 .*repeated_prepared_refreshes=[1-9][0-9]*') {
    throw "No repeated successful Prepared refresh of the same real pool. Inspect $log; this is not a refresh acceptance."
}
if ($text -notmatch 'pool registry complete .*conflicts=0 .*lagged_batches=0 .*completion_rejected=0') {
    throw "Missing clean Registry accounting; inspect $log"
}
if ($text -notmatch 'persistence complete .*completions_seen=[1-9][0-9]*') {
    throw "No completion reached the persistence consumer; inspect $log"
}
if ($text -match 'runtime fatal|level=ERROR|lost [1-9][0-9]* .*batches|completion_lagged') {
    throw "Runtime failure/loss found; inspect $log"
}
$managed = [regex]::Matches($text, 'lifecycle started managed=\[([^\]]+)\]')
if ($managed.Count -ne 1) { throw "Expected exactly one shared lifecycle; inspect $log" }
$registered = @($managed[0].Groups[1].Value -split ',\s*')
if (@(Compare-Object $selectedVenues $registered).Count -ne 0) {
    throw "Lifecycle executor registration differs from the selected protocols; inspect $log"
}
if ($Venue -eq "All" -and ($text -notmatch 'module=liq_graph::workers::graph_liquidity complete observed_pool_updates=[1-9][0-9]* ' -or $text -notmatch 'liquidity graph consumer complete generations=[1-9][0-9]*')) {
    throw "Missing Graph consumption and completed drain in combined run; inspect $log"
}

# Link this run's last accepted proof per pool to a read-only SQL verification.
$pumpWallet = $null
if ($requiredVenues -contains "PumpSwap") {
    $walletRecord = [regex]::Match($text, 'pumpswap executor started .*wallet_configured=true wallet=([1-9A-HJ-NP-Za-km-z]{32,44}) ')
    if (-not $walletRecord.Success) { throw "Missing configured PumpSwap wallet in $log" }
    $pumpWallet = $walletRecord.Groups[1].Value
}
$pattern = 'lifecycle accepted .*pool=([1-9A-HJ-NP-Za-km-z]{32,44}) pool_hex=[0-9a-f]{64} venue=(MeteoraDlmm|MeteoraDammV2|RaydiumAmmV4|RaydiumCpmm|RaydiumClmm|OrcaWhirlpool|PancakeSwapClmm|ByrealClmm|Manifest|PumpSwap) work=(Initial|Recheck|Refresh|Retry) status=(Prepared|Incomplete) .*checked_at_ms=Some\(([0-9]+)\) .*slot=Some\(([0-9]+)\)'
$latest = @{}
$refreshCounts = @{}
$acceptedCounts = @{}
$refreshWithdrawals = 0
$proofMatches = [regex]::Matches($text, $pattern)
$finalAccepted = [regex]::Match($text, 'lifecycle stats final=true [^\r\n]*\baccepted=([0-9]+) ')
if (-not $finalAccepted.Success -or $proofMatches.Count -ne [long]$finalAccepted.Groups[1].Value) {
    throw "Accepted proof records differ from final lifecycle accounting; inspect $log"
}
foreach ($match in $proofMatches) {
    $poolAddress = $match.Groups[1].Value
    $protocol = $match.Groups[2].Value
    $status = if ($match.Groups[4].Value -eq "Prepared") { 1 } else { 2 }
    $code = $venueCodes[$protocol]
    $acceptedCounts[$protocol] = 1 + [int]$acceptedCounts[$protocol]
    $walletSql = if ($protocol -eq "PumpSwap") { "'$pumpWallet'::text" } else { "NULL::text" }
    $latest[$poolAddress] = "(${code}::smallint, '$poolAddress', $($match.Groups[5].Value)::bigint, $($match.Groups[6].Value)::bigint, ${status}::smallint, $walletSql)"
    if ($match.Groups[3].Value -eq "Refresh" -and $status -eq 1) {
        $key = "$protocol|$poolAddress"
        $refreshCounts[$key] = 1 + [int]$refreshCounts[$key]
    } elseif ($match.Groups[3].Value -eq "Refresh" -and $status -eq 2) {
        $refreshWithdrawals++
    }
}
$summary = @()
foreach ($protocol in $selectedVenues) {
    $repeated = @($refreshCounts.Keys | Where-Object {
        $_.StartsWith("$protocol|") -and $refreshCounts[$_] -ge 2
    })
    if ($protocol -in $requiredVenues -and $repeated.Count -eq 0) {
        throw "No repeated successful Prepared refresh for $protocol. Inspect $log; this protocol is not accepted."
    }
    $required = $protocol -in $requiredVenues
    $summary += "CP2 protocol=$protocol connected=true accepted=$([int]$acceptedCounts[$protocol]) repeated_refresh_pools=$($repeated.Count) refresh_required=$required"
}
if ($latest.Count -eq 0) { throw "No accepted proof records found in $log" }
$values = ($latest.Values | Sort-Object) -join ",`n    "
@"
-- CP2 branch=$branch head=$head start_ms=$startMs
-- Read-only. Run against the same database immediately after this live run.
-- Expected: proofs_expected = exact_matches; mismatches = 0.
WITH expected(venue, pool_address, checked_at_ms, validated_slot, status, wallet) AS (
    VALUES $values
), checks AS (
    SELECT e.*, p.pool_address IS NOT NULL
        AND p.venue = e.venue AND p.status = e.status
        AND p.validated_slot = e.validated_slot
        AND (extract(epoch FROM p.completion_checked_at)*1000)::bigint = e.checked_at_ms
        AND (e.venue <> 11 OR e.status <> 1
          OR p.descriptor->'evidence'->'payload'->>'v10_wallet' = e.wallet)
        AND ((e.status = 1 AND p.completion_reason_code IS NULL
              AND jsonb_typeof(p.descriptor->'manifest') = 'object')
          OR (e.status = 2 AND p.completion_reason_code IS NOT NULL
              AND p.completion_reason_code <> 'transient_failure'
              AND p.descriptor->>'manifest' IS NULL)) AS matches
    FROM expected e LEFT JOIN public.pools p
      ON p.pool_address = e.pool_address
)
SELECT COUNT(*) AS proofs_expected,
       COUNT(*) FILTER (WHERE matches IS TRUE) AS exact_matches,
       COUNT(*) FILTER (WHERE matches IS NOT TRUE) AS mismatches
FROM checks;
"@ | Set-Content -Encoding UTF8 $verification
$summary | Tee-Object -FilePath $log -Append
"CP2 refresh_to_incomplete_observed=$refreshWithdrawals proofs_expected=$($latest.Count) deferred_refresh=$($deferredVenues -join ',')" |
    Tee-Object -FilePath $log -Append
Write-Host "CP2 live refresh path passed for head=$head. Run the read-only $verification to verify the exact stored proofs."
if ($deferredVenues.Count -gt 0) {
    Write-Host "Manifest TVL/refresh proof is explicitly deferred; runtime errors remain fatal and any accepted Manifest proofs are included in SQL."
}
Write-Host "Initial, Recheck, Retry and TVL withdrawal/reactivation count only when actually observed in the log."
