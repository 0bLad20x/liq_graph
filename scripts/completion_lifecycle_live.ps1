param(
    [ValidateRange(60, 86400)][int]$DurationSeconds = 300,
    [ValidateRange(1, 128)][int]$PoolLimit = 16,
    [ValidateSet("Dlmm", "DammV2", "Both")][string]$Venue = "DammV2"
)

# Real protocol REST/RPC -> one lifecycle -> Registry -> existing PostgreSQL writer.
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$branch = git branch --show-current
$head = git rev-parse HEAD
$tracked = git status --short --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $branch -ne "agent/cp2-damm-v2-lifecycle" -or $tracked) {
    throw "Use a clean, committed agent/cp2-damm-v2-lifecycle checkout."
}
cargo build --locked --release
if ($LASTEXITCODE -ne 0) { throw "cargo build --locked --release failed." }
git diff --exit-code -- Cargo.lock
if ($LASTEXITCODE -ne 0) { throw "Cargo.lock changed during build." }

$selector = switch ($Venue) {
    "Dlmm" { "dlmm" }
    "DammV2" { "damm_v2" }
    "Both" { "both" }
}
$venueCodes = @{ MeteoraDlmm = 1; MeteoraDammV2 = 2 }
$requiredVenues = switch ($Venue) {
    "Dlmm" { @("MeteoraDlmm") }
    "DammV2" { @("MeteoraDammV2") }
    "Both" { @("MeteoraDlmm", "MeteoraDammV2") }
}
$settings = @{
    RUN_DURATION_SECONDS = "$DurationSeconds"
    LIQ_GRAPH_LOG = "info,completion_lifecycle=debug,completion_meteora_dlmm=debug,completion_meteora_damm_v2=debug"
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

# Link this run's last accepted proof per pool to a read-only SQL verification.
$pattern = 'lifecycle accepted .*pool_hex=([0-9a-f]{64}) venue=(MeteoraDlmm|MeteoraDammV2) work=(Initial|Recheck|Refresh|Retry) status=(Prepared|Incomplete) .*checked_at_ms=Some\(([0-9]+)\) .*slot=Some\(([0-9]+)\)'
$latest = @{}
$refreshCounts = @{}
foreach ($match in [regex]::Matches($text, $pattern)) {
    $hex = $match.Groups[1].Value
    $protocol = $match.Groups[2].Value
    $status = if ($match.Groups[4].Value -eq "Prepared") { 1 } else { 2 }
    $code = $venueCodes[$protocol]
    $latest[$hex] = "(${code}::smallint, '$hex', $($match.Groups[5].Value)::bigint, $($match.Groups[6].Value)::bigint, ${status}::smallint)"
    if ($match.Groups[3].Value -eq "Refresh" -and $status -eq 1) {
        $key = "$protocol|$hex"
        $refreshCounts[$key] = 1 + [int]$refreshCounts[$key]
    }
}
foreach ($protocol in $requiredVenues) {
    $repeated = @($refreshCounts.Keys | Where-Object {
        $_.StartsWith("$protocol|") -and $refreshCounts[$_] -ge 2
    })
    if ($repeated.Count -eq 0) {
        throw "No repeated successful Prepared refresh for $protocol. Inspect $log; this protocol is not accepted."
    }
}
if ($latest.Count -eq 0) { throw "No accepted proof records found in $log" }
$values = ($latest.Values | Sort-Object) -join ",`n    "
@"
-- CP2 branch=$branch head=$head start_ms=$startMs
-- Read-only. Run against the same database immediately after this live run.
-- Expected: proofs_expected = exact_matches; mismatches = 0.
WITH expected(venue, pool_hex, checked_at_ms, validated_slot, status) AS (
    VALUES $values
), checks AS (
    SELECT e.*, p.pool_address IS NOT NULL
        AND p.venue = e.venue AND p.status = e.status
        AND p.validated_slot = e.validated_slot
        AND (p.descriptor->>'checked_at_ms')::bigint = e.checked_at_ms
        AND ((e.status = 1 AND p.descriptor->>'reason_code' IS NULL)
          OR (e.status = 2 AND p.descriptor->>'reason_code' IS NOT NULL
              AND p.descriptor->>'reason_code' <> 'transient_failure')) AS matches
    FROM expected e LEFT JOIN public.liq_graph_pool_cache p
      ON p.pool_address = decode(e.pool_hex, 'hex')
)
SELECT COUNT(*) AS proofs_expected,
       COUNT(*) FILTER (WHERE matches IS TRUE) AS exact_matches,
       COUNT(*) FILTER (WHERE matches IS NOT TRUE) AS mismatches
FROM checks;
"@ | Set-Content -Encoding UTF8 $verification
Write-Host "CP2 live refresh path passed for head=$head. Run the read-only $verification to verify the exact stored proofs."
Write-Host "Initial, Recheck, Retry and TVL withdrawal/reactivation count only when actually observed in the log."
