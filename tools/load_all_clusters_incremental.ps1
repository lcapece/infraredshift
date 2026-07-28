#requires -Version 5.1
<#
.SYNOPSIS
  Manual multi-cluster incremental load + promote for DataBa6ix.

.DESCRIPTION
  Run this after producer + consumers are configured (host/user/password and ENABLED).

  Incremental = resume checkpoints + high-water capture on existing DuckDB.
  Does NOT wipe the warehouse.

  Standard client layout: the application folder is %USERPROFILE%\RQP and the
  warehouse is %USERPROFILE%\RQP\data\redshift.duckdb. The script works from
  both a client install (Databas6ix.py beside it) and the source tree
  (tools\ inside the repo).

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File .\load_all_clusters_incremental.ps1

.EXAMPLE
  # Show the exact command without starting a load
  powershell -NoProfile -ExecutionPolicy Bypass -File .\load_all_clusters_incremental.ps1 -DryRun
#>
[CmdletBinding()]
param(
  [string]$DuckDbPath = "$env:USERPROFILE\RQP\data\redshift.duckdb",
  [double]$Days = 7.0,
  [double]$FloorSeconds = 600.0,
  [ValidateSet("execution_time", "elapsed_time")]
  [string]$FloorBasis = "execution_time",
  [switch]$SkipExternal,
  [switch]$Fresh,
  [switch]$NoPromote,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Client installs keep the single-file application beside this script; the
# source tree keeps this script in tools\ under the repo root.
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Launcher = $null
foreach ($candidate in @("Databas6ix.py", "Databas6ix.txt", "redshift_analyzer_text.py")) {
  $path = Join-Path $ScriptDir $candidate
  if (Test-Path -LiteralPath $path) { $Launcher = $path; break }
}
if ($Launcher) {
  Set-Location $ScriptDir
  $entry = @($Launcher, "--loader", "refresh")
  $mode = "client install ($([System.IO.Path]::GetFileName($Launcher)))"
} else {
  Set-Location (Split-Path -Parent $ScriptDir)
  $entry = @("-m", "analyzer.loader", "refresh")
  $mode = "source tree"
}

$DataFolder = Split-Path -Parent $DuckDbPath
if ($DataFolder -and -not (Test-Path -LiteralPath $DataFolder)) {
  New-Item -ItemType Directory -Force -Path $DataFolder | Out-Null
}

# Parallel cluster capture is ON by default in runner (one worker per cluster).
if (-not $env:DATABA6IX_PARALLEL_LOAD) { $env:DATABA6IX_PARALLEL_LOAD = "1" }

Write-Host "DataBa6ix manual multi-cluster PARALLEL incremental load" -ForegroundColor Cyan
Write-Host "  Mode     : $mode"
Write-Host "  Layout   : 1 producer + 3 consumers (all ENABLED)"
Write-Host "  DuckDB   : $DuckDbPath"
Write-Host "  Days     : $Days"
Write-Host "  Floor    : ${FloorSeconds}s ($FloorBasis)"
Write-Host "  Parallel : $env:DATABA6IX_PARALLEL_LOAD  (set 0 to force sequential)"
Write-Host "  Resume   : $(-not $Fresh)   Promote: $(-not $NoPromote)"
Write-Host "  External : excluded in this version"
Write-Host ""

$loaderArgs = $entry + @(
  "--duckdb-path", $DuckDbPath,
  "--days", "$Days",
  "--floor-seconds", "$FloorSeconds",
  "--floor-basis", $FloorBasis,
  "--external-timeout-action", "skip",
  "--json-events"
)
if (-not $NoPromote) { $loaderArgs += "--promote" }
if ($SkipExternal)   { $loaderArgs += "--skip-external" }
if ($Fresh)          { $loaderArgs += "--fresh" }

Write-Host ("python " + ($loaderArgs -join " ")) -ForegroundColor DarkGray
Write-Host ""

if ($DryRun) {
  Write-Host "Dry run: no load was started." -ForegroundColor Yellow
  exit 0
}

& python @loaderArgs
exit $LASTEXITCODE
