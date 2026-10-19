param([ValidateRange(30, 86400)][int]$DurationSeconds = 300)

# Real providers and the existing PostgreSQL database. No generated input,
# status resets, transaction submission, unit tests or formatting checks.
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

$branch = git branch --show-current
if ($LASTEXITCODE -ne 0 -or $branch -ne "agent/cp1-completion-contracts") {
    throw "Check out agent/cp1-completion-contracts before running this checkpoint."
}
$head = git rev-parse --short HEAD
if ($LASTEXITCODE -ne 0) { throw "Cannot resolve checkout HEAD." }
$trackedChanges = git status --short --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $trackedChanges) {
    throw "Tracked files differ from the committed checkout. Inspect git status first."
}
Write-Host "Completion CP1 branch=$branch head=$head duration_s=$DurationSeconds"

cargo build --locked
if ($LASTEXITCODE -ne 0) { throw "cargo build --locked failed: $LASTEXITCODE" }
git diff --exit-code -- Cargo.lock
if ($LASTEXITCODE -ne 0) { throw "Cargo.lock changed during build." }

$env:CP1_MARKET_ENRICHMENT_ENABLED = "true"
$env:JUPITER_BACKFILL_ON_START = "false"
$env:RUN_DURATION_SECONDS = "$DurationSeconds"
$env:RUST_LOG = "info,liq_graph::workers::pool_registry=debug"
$log = "completion_contracts_live.log"
cargo run --locked --release -- --run 2>&1 | Tee-Object -FilePath $log
if ($LASTEXITCODE -ne 0) { throw "Runtime failed: $LASTEXITCODE; inspect $log" }

$text = Get-Content -Raw $log
if ($text -notmatch 'runtime shutdown complete=true') {
    throw "Missing complete runtime shutdown; inspect $log"
}
if ($text -notmatch 'completion_contracts min_tvl_usd=500 admitted_pools=[1-9][0-9]*') {
    throw "No currently admitted real pools were observed; inspect TVL enrichment in $log"
}
if ($text -notmatch 'completion_contract pool=.*checked_at_ms=Some\([1-9][0-9]*\)') {
    throw "No new terminal completion proof was observed. This run is not a completion acceptance."
}
if ($text -notmatch 'pool registry complete .*conflicts=0 .*lagged_batches=0 .*completion_rejected=0') {
    throw "Registry final accounting is missing or reports conflicts/rejected results/loss; inspect $log"
}
if ($text -notmatch 'persistence complete .*completions_seen=[1-9][0-9]*') {
    throw "No terminal completion reached the persistence consumer; inspect $log"
}
if ($text -match 'lost [1-9][0-9]* .*batches|lagged.*missed_batches=[1-9]|runtime fatal') {
    throw "Runtime loss/fatal diagnostic found; inspect $log"
}
Write-Host "Completion CP1 live path passed for branch=$branch head=$head. Inspect per-venue SQL with scripts/completion_contracts_inspect.sql."
Write-Host "This checkpoint establishes contracts/admission. Periodic Prepared refresh belongs to CP2."
