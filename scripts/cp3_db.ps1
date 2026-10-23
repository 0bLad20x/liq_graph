param(
    [ValidateSet("Inspect","Reset","Verify","SqlFile")]
    [string]$Action = "Inspect",
    [switch]$ConfirmReset,
    [string]$SqlFile,
    [string]$PsqlPath = "psql"
)
# Uses the same LIQ_GRAPH_DATABASE_URL -> DATABASE_URL precedence as Rust/dotenvy.
# psql does NOT load .env itself. Avoid passing credentials on the command line.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

function Get-ConfiguredDbUrl {
    foreach ($key in @("LIQ_GRAPH_DATABASE_URL", "DATABASE_URL")) {
        $value = [Environment]::GetEnvironmentVariable($key, "Process")
        if (-not [string]::IsNullOrWhiteSpace($value)) { return $value }
    }
    $envFile = Join-Path $root ".env"
    if (-not (Test-Path -LiteralPath $envFile)) {
        throw "No DB URL in process environment and .env is missing."
    }
    $options = @{}
    foreach ($line in (Get-Content -LiteralPath $envFile)) {
        if ($line -match '^\s*(?:export\s+)?(LIQ_GRAPH_DATABASE_URL|DATABASE_URL)\s*=\s*(.*)$') {
            $key = $Matches[1]
            $raw = $Matches[2].Trim()
            if ($raw.Length -ge 2 -and
                (($raw[0] -eq [char]34 -and $raw[$raw.Length-1] -eq [char]34) -or
                 ($raw[0] -eq [char]39 -and $raw[$raw.Length-1] -eq [char]39))) {
                $raw = $raw.Substring(1,$raw.Length-2)
            } else {
                $raw = ($raw -split '\s+#',2)[0].Trim()
            }
            if ($raw) { $options[$key]=$raw }
        }
    }
    foreach ($key in @("LIQ_GRAPH_DATABASE_URL", "DATABASE_URL")) {
        if ($options.ContainsKey($key)) { return $options[$key] }
    }
    throw "No LIQ_GRAPH_DATABASE_URL or DATABASE_URL in .env."
}

$dsn = Get-ConfiguredDbUrl
try {
    $url = [uri]$dsn
    if ($url.Scheme -notin @("postgres","postgresql") -or
        -not $url.Host -or -not $url.UserInfo -or
        -not $url.AbsolutePath.Trim('/')) {
        throw "Invalid PostgreSQL connection URL."
    }
    $credentials = $url.UserInfo.Split(@(':'),2)
    if ($credentials.Count -ne 2) {throw "PostgreSQL URL must contain a user and password."}
    $userName = [uri]::UnescapeDataString($credentials[0])
    $password = [uri]::UnescapeDataString($credentials[1])
    $database = [uri]::UnescapeDataString($url.AbsolutePath.TrimStart('/'))
    $hostName = $url.Host
    $port = if ($url.Port -ge 0) {$url.Port} else {5432}
    $sslMode = $null
    foreach ($kv in $url.Query.TrimStart('?').Split('&')) {
        if ($kv -match '^sslmode=(.*)$') { $sslMode = [uri]::UnescapeDataString($Matches[1]) }
    }
} catch {
    throw "Invalid or unsupported database URL. Check the value in .env; credentials have not been printed."
}
if (-not $userName -or -not $database) {throw "Missing PostgreSQL user or database name."}

$oldPass=[Environment]::GetEnvironmentVariable("PGPASSWORD","Process")
$oldSsl=[Environment]::GetEnvironmentVariable("PGSSLMODE","Process")
try {
    [Environment]::SetEnvironmentVariable("PGPASSWORD",$password,"Process")
    if ($sslMode) { [Environment]::SetEnvironmentVariable("PGSSLMODE",$sslMode,"Process") }
    $common = @("-X","-w","-v","ON_ERROR_STOP=1","-h",$hostName,"-p","$port","-U",$userName,"-d",$database)
    if ($Action -eq "Reset") {
        if (-not $ConfirmReset) {throw "Reset is destructive. Use -Action Reset -ConfirmReset explicitly."}
        Write-Host "DESTRUCTIVE project-only reset: host=$hostName port=$port database=$database user=$userName"
        & $PsqlPath @common -f (Join-Path $PSScriptRoot "cp3_reset_project_tables.sql")
    } elseif ($Action -eq "Inspect") {
        Write-Host "Inspecting host=$hostName port=$port database=$database user=$userName"
        & $PsqlPath @common -c "SELECT current_database(), current_user, current_schema(), inet_server_addr(), inet_server_port();"
    } elseif ($Action -eq "Verify") {
        & $PsqlPath @common -c "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('mints','pools','liq_graph_mint_catalog','liq_graph_pool_cache') ORDER BY table_name;"
        if ($LASTEXITCODE -ne 0) {throw "psql schema inspection failed with exit code $LASTEXITCODE"}
        & $PsqlPath @common -c "SELECT COUNT(*) AS pools, COUNT(*) FILTER (WHERE status=1) AS prepared FROM public.pools;"
        if ($LASTEXITCODE -ne 0) {throw "psql pool count failed with exit code $LASTEXITCODE"}
        & $PsqlPath @common -c "SELECT COUNT(*) AS mints, COUNT(*) FILTER (WHERE decimals IS NOT NULL) AS decimals_known FROM public.mints;"
    } else {
        if (-not $SqlFile) {throw "-SqlFile is required with -Action SqlFile."}
        $file = (Resolve-Path -LiteralPath $SqlFile).Path
        & $PsqlPath @common -f $file
    }
    if ($LASTEXITCODE -ne 0) { throw "psql exited with code $LASTEXITCODE. Check DB host/user and .env configuration." }
} finally {
    [Environment]::SetEnvironmentVariable("PGPASSWORD",$oldPass,"Process")
    [Environment]::SetEnvironmentVariable("PGSSLMODE",$oldSsl,"Process")
    $password=$null
    $dsn=$null
}
