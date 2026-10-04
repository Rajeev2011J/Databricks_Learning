<#
.SYNOPSIS
    Central test runner for all Databricks Asset Bundle test suites.

.DESCRIPTION
    Runs the full pytest suite from the Databricks/ root directory using the
    single pyproject.toml configuration.  All six bundles are tested in one
    invocation: AdHocRequests, clusters, GCOB_Consumer, GCOB_Reportingv1,
    PartyCatalog, radarv1.

    The script handles:
      - Virtual environment creation (first run only)
      - Dependency installation from requirements-dev.txt
      - Selective test execution by marker or bundle
      - HTML + terminal coverage reporting
      - Non-zero exit code on test failure (for CI integration)

.PARAMETER Marker
    Run only tests with this pytest marker.
    Examples: unit, spark, dq, radarv1, gcob_consumer, clusters, party_catalog

.PARAMETER Bundle
    Run only tests for a specific bundle folder.
    Examples: radarv1, GCOB_Consumer, GCOB_Reportingv1, clusters, AdHocRequests, PartyCatalog

.PARAMETER NoCoverage
    Skip coverage reporting (faster for local development iterations).

.PARAMETER Parallel
    Run tests in parallel using pytest-xdist (-n auto).
    Note: incompatible with the session-scoped SparkSession — use only for unit tests.

.PARAMETER Install
    Force reinstall of dependencies even if venv already exists.

.PARAMETER Verbose
    Pass -v -s to pytest for full stdout output (useful when debugging).

.EXAMPLE
    # Run the full suite with coverage
    .\run_tests.ps1

.EXAMPLE
    # Run only unit tests (no Spark, fast)
    .\run_tests.ps1 -Marker unit

.EXAMPLE
    # Run only radarv1 bundle tests
    .\run_tests.ps1 -Bundle radarv1

.EXAMPLE
    # Run DQ tests without coverage (fast feedback loop)
    .\run_tests.ps1 -Marker dq -NoCoverage

.EXAMPLE
    # Run unit tests in parallel across all bundles
    .\run_tests.ps1 -Marker unit -Parallel

.EXAMPLE
    # First-time setup: create venv and install all deps
    .\run_tests.ps1 -Install
#>

[CmdletBinding()]
param(
    [string] $Marker    = "",
    [string] $Bundle    = "",
    [switch] $NoCoverage,
    [switch] $Parallel,
    [switch] $Install,
    [switch] $Verbose
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Resolve the Databricks/ root (script directory)
# ---------------------------------------------------------------------------
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Databricks Asset Bundles — Central Test Runner" -ForegroundColor Cyan
Write-Host "  Root: $ScriptDir" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# ---------------------------------------------------------------------------
# Virtual environment setup
# ---------------------------------------------------------------------------
$VenvDir     = Join-Path $ScriptDir ".venv"
$PythonExe   = if ($IsWindows -or $env:OS -eq "Windows_NT") {
                   Join-Path $VenvDir "Scripts\python.exe"
               } else {
                   Join-Path $VenvDir "bin/python"
               }
$PytestExe   = if ($IsWindows -or $env:OS -eq "Windows_NT") {
                   Join-Path $VenvDir "Scripts\pytest.exe"
               } else {
                   Join-Path $VenvDir "bin/pytest"
               }

if ($Install -or -not (Test-Path $VenvDir)) {
    Write-Host "[Setup] Creating virtual environment at $VenvDir ..." -ForegroundColor Yellow
    python -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to create virtual environment. Ensure Python 3.9+ is on PATH."
        exit 1
    }
}

if ($Install -or -not (Test-Path $PytestExe)) {
    Write-Host "[Setup] Installing dependencies from requirements-dev.txt ..." -ForegroundColor Yellow
    & $PythonExe -m pip install --upgrade pip --quiet
    & $PythonExe -m pip install -r requirements-dev.txt --quiet
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Dependency installation failed."
        exit 1
    }
    Write-Host "[Setup] Dependencies installed successfully." -ForegroundColor Green
}

Write-Host "[Setup] Using Python: $PythonExe" -ForegroundColor DarkGray
Write-Host ""

# ---------------------------------------------------------------------------
# Build pytest argument list
# ---------------------------------------------------------------------------
$PytestArgs = @()

# ---- test path / bundle selection ----
if ($Bundle) {
    $BundleTestPath = Join-Path $ScriptDir "$Bundle\tests"
    if (-not (Test-Path $BundleTestPath)) {
        Write-Error "Bundle test path not found: $BundleTestPath"
        exit 1
    }
    Write-Host "[Filter] Bundle: $Bundle  →  $BundleTestPath" -ForegroundColor Magenta
    $PytestArgs += $BundleTestPath
    # Also include shared unit_tests/ infrastructure
    $PytestArgs += Join-Path $ScriptDir "unit_tests"
} else {
    # Default: discover from pyproject.toml testpaths
    Write-Host "[Filter] Running ALL bundles" -ForegroundColor Magenta
}

# ---- marker filter ----
if ($Marker) {
    Write-Host "[Filter] Marker: -m $Marker" -ForegroundColor Magenta
    $PytestArgs += "-m"
    $PytestArgs += $Marker
}

# ---- coverage ----
if (-not $NoCoverage) {
    $PytestArgs += "--cov"
    $PytestArgs += "--cov-report=term-missing"
    $PytestArgs += "--cov-report=html:htmlcov"
    Write-Host "[Coverage] HTML report → $ScriptDir\htmlcov\index.html" -ForegroundColor DarkGray
} else {
    Write-Host "[Coverage] Skipped (-NoCoverage)" -ForegroundColor DarkGray
}

# ---- parallelism ----
if ($Parallel) {
    Write-Host "[Parallel] Running with -n auto (pytest-xdist)" -ForegroundColor DarkGray
    $PytestArgs += "-n"
    $PytestArgs += "auto"
}

# ---- verbosity ----
if ($Verbose) {
    $PytestArgs += "-v"
    $PytestArgs += "-s"
} else {
    $PytestArgs += "-v"
}

# ---- always: short traceback, strict markers ----
$PytestArgs += "--tb=short"
$PytestArgs += "--strict-markers"

Write-Host ""
Write-Host "[Run] pytest $($PytestArgs -join ' ')" -ForegroundColor DarkGray
Write-Host ""

# ---------------------------------------------------------------------------
# Execute pytest
# ---------------------------------------------------------------------------
$StartTime = Get-Date
& $PytestExe @PytestArgs
$ExitCode  = $LASTEXITCODE
$Duration  = (Get-Date) - $StartTime

Write-Host ""
Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray
Write-Host "  Duration : $($Duration.ToString('mm\:ss'))" -ForegroundColor DarkGray

if ($ExitCode -eq 0) {
    Write-Host "  Result   : PASSED" -ForegroundColor Green
    if (-not $NoCoverage) {
        Write-Host "  Coverage : $ScriptDir\htmlcov\index.html" -ForegroundColor DarkGray
    }
} elseif ($ExitCode -eq 5) {
    # Exit code 5 = no tests collected (e.g. marker matched nothing)
    Write-Host "  Result   : NO TESTS COLLECTED (exit 5)" -ForegroundColor Yellow
    Write-Host "  Hint     : Check your -Marker or -Bundle filter." -ForegroundColor Yellow
} else {
    Write-Host "  Result   : FAILED (exit $ExitCode)" -ForegroundColor Red
}
Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray
Write-Host ""

exit $ExitCode
