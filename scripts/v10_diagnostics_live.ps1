#requires -Version 7.2
param(
    [Parameter(Mandatory = $true)][string]$RequestPath,
    [Parameter(Mandatory = $true)][string]$WalletPath,
    [ValidateSet("All", "mint_proof_mismatch", "pool_is_settlement_mint", "target_is_wallet",
        "foreign_target_vault", "foreign_bin_array", "low_transaction_cu", "high_minimum_profit")]
    [string[]]$Scenario = @("All")
)

# Live diagnosis only: frozen input copies -> existing simulation command -> observations.
# Expected native failures are data. No DB access, send operation or validation bypass.
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false
$sourcePath = (Resolve-Path -LiteralPath $RequestPath).Path
$WalletPath = (Resolve-Path -LiteralPath $WalletPath).Path
$root = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $root
$branch = (& git branch --show-current | Out-String).Trim()
if ($LASTEXITCODE -ne 0) { throw "Cannot determine branch." }
$head = (& git rev-parse HEAD | Out-String).Trim()
if ($LASTEXITCODE -ne 0) { throw "Cannot determine HEAD." }
$tree = (& git rev-parse "HEAD^{tree}" | Out-String).Trim()
if ($LASTEXITCODE -ne 0) { throw "Cannot determine tree." }
$tracked = git status --short --untracked-files=no
if ($LASTEXITCODE -ne 0 -or $tracked -or $branch -notin @("agent/v10-simulation-worker", "main")) {
    throw "Use a clean, committed agent/v10-simulation-worker checkout, or main after merge."
}
$sourceJson = Get-Content -LiteralPath $sourcePath -Raw
$source = $sourceJson | ConvertFrom-Json
$dlmm = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
if (@($source.groups).Count -ne 1 -or $source.groups[0].kind -ne "direct" -or
    @($source.groups[0].pools).Count -ne 2 -or $null -ne $source.conversion -or
    $null -eq $source.parameters) {
    throw "Supply one existing direct request with exactly two DLMM pools, parameters and no conversion."
}

function Get-Layout($Request, $Pool) {
    $m = $Pool.manifest
    $name = if ($m.mint_a -ceq $Request.settlement) { "base_a" }
        elseif ($m.mint_b -ceq $Request.settlement) { "base_b" }
        else { throw "Pool does not contain settlement." }
    $other = if ($name -eq "base_a") { $m.mint_b } else { $m.mint_a }
    if ($other -cne $Request.groups[0].target) { throw "Pool does not contain the declared target." }
    $accounts = @($m.$name)
    $positions = @(for ($i = 0; $i -lt $accounts.Count; $i++) {
        if ($accounts[$i].address -ceq $m.pool) { $i }
    })
    if ($accounts.Count -lt 8 -or $accounts[0].address -cne $dlmm -or
        $accounts[1].address -cne $Request.settlement -or $positions.Count -ne 1 -or
        $positions[0] -notin @(3, 4) -or $positions[0] + 4 -ge $accounts.Count) {
        throw "Expected a DLMM block: program, quote, event authority, optional memo, pool, vaults, oracle, optional bitmap, bin arrays."
    }
    # In our DLMM manifests the final account is a BinArray, never the optional bitmap.
    [pscustomobject]@{ name = $name; accounts = $accounts; pool_index = $positions[0] }
}

function Set-Field($Object, [string]$Name, $Value, [string]$Path, $Changes) {
    $property = $Object.PSObject.Properties[$Name]
    $before = if ($null -ne $property) { $property.Value } else { $null }
    $present = $null -ne $property
    $Object | Add-Member -NotePropertyName $Name -NotePropertyValue $Value -Force
    $Changes.Add([pscustomobject]@{ path = $Path; present_before = $present; before = $before; after = $Value })
}

function Replace-BlockAddress($Manifest, [string]$Old, [string]$New, $Changes) {
    foreach ($name in @("base_a", "base_b")) {
        $accounts = @($Manifest.$name)
        $matched = 0
        for ($i = 0; $i -lt $accounts.Count; $i++) {
            if ($accounts[$i].address -ceq $Old) {
                Set-Field $accounts[$i] "address" $New "/groups/0/pools/0/manifest/$name/$i/address" $Changes
                $matched++
            }
        }
        if ($matched -ne 1) { throw "Expected exactly one $Old entry in $name; source unchanged." }
    }
}

$first = Get-Layout $source ($source.groups[0].pools[0])
$second = Get-Layout $source ($source.groups[0].pools[1])
$foreignVault = $second.accounts[$second.pool_index + 1].address
$foreignArray = $second.accounts[-1].address
if ($source.groups[0].pools[0].manifest.pool -ceq $source.groups[0].pools[1].manifest.pool -or
    $foreignVault -ceq $first.accounts[$first.pool_index + 1].address -or
    $foreignArray -ceq $first.accounts[-1].address) {
    throw "The two source pools must be different and supply different target vaults and BinArrays."
}

Write-Host "V10 live diagnostics branch=$branch head=$head tree=$tree broadcasts=false"
cargo build --locked --release
if ($LASTEXITCODE -ne 0) { throw "Build failed; no diagnostic simulation started." }
git diff --exit-code -- Cargo.lock
if ($LASTEXITCODE -ne 0) { throw "Cargo.lock changed during build." }
$binary = Join-Path $root "target\release\liq_graph.exe"
$wallet = (& $binary --v10-wallet $WalletPath | Out-String).Trim()
if ($LASTEXITCODE -ne 0 -or $wallet -cne $source.wallet) { throw "Cannot load the request's configured signer." }
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ")
$folder = Join-Path $root "v10_live\diagnostics-$stamp"
New-Item -ItemType Directory -Path $folder | Out-Null
$utf8 = [System.Text.UTF8Encoding]::new($false)
function Save-Json([string]$Path, $Value) {
    [System.IO.File]::WriteAllText($Path, (ConvertTo-Json -InputObject $Value -Depth 100), $utf8)
}
[System.IO.File]::WriteAllText((Join-Path $folder "source-request.json"), $sourceJson, $utf8)
Save-Json (Join-Path $folder "run.json") ([ordered]@{
    branch = $branch; head = $head; tree = $tree; tracked_status = @($tracked)
    source_path = $sourcePath; source_sha256 = (Get-FileHash -LiteralPath $sourcePath -Algorithm SHA256).Hash
    wallet = $wallet; broadcasts = $false; started_utc = $stamp; selected_scenarios = @($Scenario)
})

$descriptions = [ordered]@{
    baseline_before = "Reference before mutations; only request ID and no_failure are set explicitly."
    mint_proof_mismatch = "First pool target proof says wallet, while manifest and group retain the original mint (local guard)."
    pool_is_settlement_mint = "First pool identity and its two block entries become the existing settlement mint account."
    target_is_wallet = "Target role, both pool mint identities/proofs and quote entries consistently name the wallet account instead of a mint."
    foreign_target_vault = "First pool target vault becomes the target vault declared by the second pool."
    foreign_bin_array = "First pool final BinArray becomes the final BinArray declared by the second pool."
    low_transaction_cu = "Transaction compute limit becomes 1000; executor compute allowance is unchanged."
    high_minimum_profit = "Minimum profit becomes 1000000000000 settlement base units; no particular outcome is assumed."
    baseline_after = "Reference after mutations; compare slots and results because live market state can change."
}
$cases = @($descriptions.Keys | Where-Object {
    $_ -in @("baseline_before", "baseline_after") -or "All" -in $Scenario -or $_ -in $Scenario
})
$rows = [System.Collections.Generic.List[object]]::new()
$ordinal = 0
foreach ($case in $cases) {
    foreach ($noFailure in @($false, $true)) {
        $ordinal++
        $request = $sourceJson | ConvertFrom-Json
        $changes = [System.Collections.Generic.List[object]]::new()
        $flag = [int]$noFailure
        $request.id = "diagnostic-$stamp-$case-nf$flag"
        Set-Field $request.parameters "no_failure" $noFailure "/parameters/no_failure" $changes
        $pool = $request.groups[0].pools[0]
        $layout = Get-Layout $request $pool
        switch ($case) {
            "mint_proof_mismatch" {
                $index = if ($pool.mints[0].address -ceq $request.groups[0].target) { 0 } else { 1 }
                Set-Field ($pool.mints[$index]) "address" $wallet "/groups/0/pools/0/mints/$index/address" $changes
            }
            "pool_is_settlement_mint" {
                Replace-BlockAddress $pool.manifest $pool.manifest.pool $request.settlement $changes
                Set-Field $pool.manifest "pool" $request.settlement "/groups/0/pools/0/manifest/pool" $changes
            }
            "target_is_wallet" {
                $target = $request.groups[0].target
                for ($p = 0; $p -lt 2; $p++) {
                    $item = $request.groups[0].pools[$p]
                    $mintName = if ($item.manifest.mint_a -ceq $target) { "mint_a" } else { "mint_b" }
                    Set-Field $item.manifest $mintName $wallet "/groups/0/pools/$p/manifest/$mintName" $changes
                    $index = if ($item.mints[0].address -ceq $target) { 0 } else { 1 }
                    Set-Field ($item.mints[$index]) "address" $wallet "/groups/0/pools/$p/mints/$index/address" $changes
                    foreach ($name in @("base_a", "base_b")) {
                        if ($item.manifest.$name[1].address -ceq $target) {
                            Set-Field ($item.manifest.$name[1]) "address" $wallet "/groups/0/pools/$p/manifest/$name/1/address" $changes
                        }
                    }
                }
                Set-Field $request.groups[0] "target" $wallet "/groups/0/target" $changes
            }
            "foreign_target_vault" {
                Replace-BlockAddress $pool.manifest ($layout.accounts[$layout.pool_index + 1].address) $foreignVault $changes
            }
            "foreign_bin_array" {
                Replace-BlockAddress $pool.manifest ($layout.accounts[-1].address) $foreignArray $changes
            }
            "low_transaction_cu" {
                Set-Field $request.parameters "transaction_compute_unit_limit" 1000 "/parameters/transaction_compute_unit_limit" $changes
            }
            "high_minimum_profit" {
                Set-Field $request.parameters "minimum_profit" ([UInt64]1000000000000) "/parameters/minimum_profit" $changes
            }
        }
        $caseFolder = Join-Path $folder ("{0:D2}-{1}-nf{2}" -f $ordinal, $case, $flag)
        New-Item -ItemType Directory -Path $caseFolder | Out-Null
        $inputPath = Join-Path $caseFolder "request.json"
        $reportPath = Join-Path $caseFolder "report.json"
        $logPath = Join-Path $caseFolder "worker.log"
        Save-Json $inputPath $request
        Save-Json (Join-Path $caseFolder "mutation.json") ([ordered]@{
            scenario = $case; description = $descriptions[$case]; no_failure = $noFailure
            original_id = $source.id; request_id = $request.id; changes = @($changes.ToArray())
        })
        & $binary --v10-simulate $inputPath --wallet $WalletPath 1> $reportPath 2> $logPath
        $nativeExit = $LASTEXITCODE
        $report = $null
        try { $report = Get-Content -LiteralPath $reportPath -Raw | ConvertFrom-Json }
        catch { Write-Host "Case $case produced no readable report; inspect its worker.log." }
        $failure = @($report.simulation.logs | Where-Object { $_ -match '^Program [1-9A-HJ-NP-Za-km-z]+ failed:' })
        $firstFailedProgram = if ($failure.Count) { ($failure[0] -split ' ')[1] } else { $null }
        $simulationError = if ($null -ne $report.simulation.err) {
            ConvertTo-Json -InputObject $report.simulation.err -Depth 20 -Compress
        } else { $null }
        $outcome = if ($null -ne $report) { $report.outcome } else { "process_error" }
        $dexCalls = if ($null -ne $report) { @($report.dex_invocations).Count } else { $null }
        $rows.Add([pscustomobject][ordered]@{
            scenario = $case; no_failure = $noFailure; outcome = $outcome; phase = $report.phase
            exit_code = $nativeExit; executor_invoked = $report.executor_invoked
            dex_calls = $dexCalls; cu = $report.simulation.unitsConsumed
            context_slot = $report.context_slot; provider = $report.provider
            first_failed_program = $firstFailedProgram; simulation_error = $simulationError
            local_error = $report.error; evidence_folder = $caseFolder
        })
        Save-Json (Join-Path $folder "summary.json") @($rows.ToArray())
        $rows.ToArray() | Export-Csv -LiteralPath (Join-Path $folder "summary.csv") -NoTypeInformation -Delimiter ';' -Encoding utf8
        Write-Host "[$ordinal/$($cases.Count * 2)] $case no_failure=$noFailure -> $outcome"
        if ($case -eq "baseline_before" -and $noFailure -and
            ($outcome -notin @("simulated_no_dex_call_observed", "simulated_dex_calls_observed") -or
             -not $report.executor_invoked -or $nativeExit -ne 0)) {
            throw "Unmodified no_failure=true reference did not succeed. Inspect $caseFolder and summary.json before further mutations."
        }
    }
}
$rows.ToArray() | Format-Table scenario, no_failure, outcome, cu -AutoSize
Write-Host "Evidence: $folder"
Write-Host "Summary:  $(Join-Path $folder 'summary.csv')"
Write-Host "Diagnostic observations saved. No transaction was broadcast; no error meaning was inferred automatically."
