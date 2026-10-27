# Replay one complete frozen request through the existing executor. No DB export.
param(
    [Parameter(Mandatory=$true)][string]$WalletPath,
    [Parameter(Mandatory=$true)][string]$RequestPath,
    [string]$ExpectedBranch = ''
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $repo
try {
    $branch = & git branch --show-current
    if ($LASTEXITCODE -ne 0) { throw 'Cannot read branch.' }
    $head = & git rev-parse HEAD
    if ($LASTEXITCODE -ne 0) { throw 'Cannot read HEAD.' }
    $tracked = @(& git status --short --untracked-files=no)
    if ($LASTEXITCODE -ne 0 -or $tracked.Count -gt 0) { throw 'Use a clean committed checkout.' }
    if ($ExpectedBranch -and $branch -ne $ExpectedBranch) { throw "Expected '$ExpectedBranch', found '$branch'." }
    $WalletPath = (Resolve-Path -LiteralPath $WalletPath).Path
    $RequestPath = (Resolve-Path -LiteralPath $RequestPath).Path
    Write-Host "V10 replay branch=$branch head=$head broadcasts=false"
    & cargo build --locked --release
    if ($LASTEXITCODE -ne 0) { throw 'Build failed; no simulation started.' }
    & git diff --exit-code -- Cargo.lock
    if ($LASTEXITCODE -ne 0) { throw 'Build changed Cargo.lock.' }
    $tracked = @(& git status --short --untracked-files=no)
    if ($LASTEXITCODE -ne 0 -or $tracked.Count -gt 0) { throw 'Build changed tracked files.' }
    $binary = Join-Path $repo 'target\release\liq_graph.exe'
    $stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ')
    $folder = Join-Path $repo "v10_live\$stamp"
    New-Item -ItemType Directory -Path $folder | Out-Null
    $request = Join-Path $folder 'request.json'
    Copy-Item -LiteralPath $RequestPath -Destination $request
    @{ branch=$branch; head=$head; transactions_sent=0 } | ConvertTo-Json |
        Set-Content -LiteralPath (Join-Path $folder 'checkout.json') -Encoding UTF8
    $reportPath = Join-Path $folder 'report.json'
    $logPath = Join-Path $folder 'worker.log'
    $priorPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $binary --v10-simulate $request --wallet $WalletPath 1> $reportPath 2> $logPath
        $exitCode = $LASTEXITCODE
    } finally { $ErrorActionPreference = $priorPreference }
    Write-Host "Artifacts: $folder"
    if (Test-Path -LiteralPath $reportPath) { Get-Content -LiteralPath $reportPath -Raw | Write-Host }
    if ($exitCode -ne 0) { throw 'Replay failed; inspect report.json and worker.log.' }
    $report = Get-Content -LiteralPath $reportPath -Raw | ConvertFrom-Json
    if (-not $report.executor_invoked -or $null -eq $report.simulation) { throw 'No executor simulation observed.' }
    Write-Host "Simulation completed: $($report.outcome). DEX calls and profit require separate evidence."
} finally { Pop-Location }
