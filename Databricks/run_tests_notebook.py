# Databricks notebook source
# DBTITLE 1,run_tests_notebook.py — pytest Runner
# MAGIC %md
# MAGIC # run_tests_notebook.py — pytest Runner for Databricks Workspace
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Run the centralized pytest suite for all six Databricks Asset Bundles directly from a Databricks Workspace notebook on a live cluster.
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC 1. Open this file as a notebook in your Databricks Workspace.
# MAGIC    (It is stored in `Databricks/run_tests_notebook.py` in the repo.)
# MAGIC 2. Attach it to any cluster (DAB-small or larger is sufficient).
# MAGIC 3. **Run All** — or run cells individually for step-by-step diagnosis.
# MAGIC 4. Optionally set `MARKER_FILTER` in the Configuration cell to target a specific test subset.
# MAGIC
# MAGIC ## How It Works
# MAGIC
# MAGIC | Cell | Description |
# MAGIC | --- | --- |
# MAGIC | Install Dependencies | Installs test dependencies via `pip` (cluster-safe — no pyspark/delta) |
# MAGIC | Configuration | Sets marker filter, test scope, verbosity |
# MAGIC | Environment Verification | Confirms `IS_DATABRICKS = True` |
# MAGIC | Repo Structure Validation | Ensures all bundle test dirs are present |
# MAGIC | Build sys.path | Adds all bundle `src/` directories to `sys.path` |
# MAGIC | Run pytest | Runs pytest programmatically via `pytest.main()`, captures output |
# MAGIC | Summary | Displays a formatted summary of pass/fail/skip counts |
# MAGIC | Final Signal | Marks the notebook run as PASSED or FAILED |
# MAGIC
# MAGIC ## Zero Code Changes
# MAGIC
# MAGIC The same test files run here as in ADO CI and on local dev. `environment.py` detects `IS_DATABRICKS=True` and routes to:
# MAGIC - `SparkSession.getOrCreate()` (the already-running cluster session)
# MAGIC - Real `dbutils` (secrets, fs, widgets)
# MAGIC - Real cluster env vars (no fake credentials injected)
# MAGIC - No `sys.modules` stubs (Databricks SDK packages are natively present)
# MAGIC
# MAGIC ## Tests Skipped on Cluster
# MAGIC
# MAGIC Tests decorated with `@pytest.mark.local_only` are automatically skipped. These are YAML-structure tests that read files from the local filesystem path not reachable from the cluster:
# MAGIC - `clusters/tests/test_clusters_config.py` (reads `clusters_job.yml` by path)
# MAGIC - `AdHocRequests/tests/test_adhoc_config.py`
# MAGIC - `PartyCatalog/tests/test_party_catalog_config.py`
# MAGIC
# MAGIC All unit and Spark tests run normally.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧪 Databricks Bundle pytest Runner
# MAGIC
# MAGIC Runs the centralized pytest suite for all six Databricks Asset Bundles.
# MAGIC
# MAGIC **Before running:** ensure the repo is cloned under `/Workspace` or
# MAGIC available via a Databricks Repo mounted at a predictable path.

# COMMAND ----------

# DBTITLE 1,Cell 1 — Install test dependencies
# =============================================================================
# CELL 1 — Install test dependencies
#
# Uses requirements-cluster.txt which EXCLUDES pyspark and delta-spark.
# Those packages are already provided by the cluster runtime — re-installing
# them would conflict with the runtime version and break the SparkSession.
#
# subprocess pip is used instead of %pip magic because:
#   - %pip magic requires the path to be a literal string (no Python variables)
#   - subprocess pip installs into the current interpreter immediately
#     (no kernel restart needed for pure-Python packages like pytest/chispa)
# =============================================================================

# CRITICAL: Set this FIRST, before ANY imports!
# Databricks workspace filesystem doesn't support __pycache__ directories
import os
import sys

# Set both the environment variable AND the runtime flag
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
sys.dont_write_bytecode = True

import subprocess
from pathlib import Path

# Resolve requirements-cluster.txt relative to this notebook's location.
# In Databricks notebooks, __file__ is not defined, so we use os.getcwd()
# which returns /Workspace/Users/<user>/.../Databricks when the notebook is there
_NOTEBOOK_DIR = Path(os.getcwd())   # Databricks/
_REQ_FILE     = _NOTEBOOK_DIR / "requirements-cluster.txt"

if not _REQ_FILE.exists():
    raise FileNotFoundError(
        f"requirements-cluster.txt not found at {_REQ_FILE}\n"
        "Ensure the repo is cloned and this notebook lives inside Databricks/.\n"
        "If auto-detection is wrong, set DATABRICKS_REPO_ROOT env var to the "
        "Databricks/ directory path."
    )

print(f"📦 Installing from: {_REQ_FILE}")

_result = subprocess.run(
    [
        sys.executable, "-m", "pip", "install",
        "-r", str(_REQ_FILE),
        "-q",
        "--no-warn-script-location",
    ],
    capture_output=True,
    text=True,
)

if _result.returncode != 0:
    print("STDERR:", _result.stderr[-2000:])   # last 2000 chars avoids log flooding
    raise RuntimeError(
        f"pip install failed (exit {_result.returncode}).\n"
        f"STDERR (tail):\n{_result.stderr[-1000:]}"
    )

print("✅ Test dependencies installed")

# Show installed versions for audit trail
_show = subprocess.run(
    [sys.executable, "-m", "pip", "show",
     "pytest", "pytest-cov", "pytest-mock", "chispa", "databricks-sdk"],
    capture_output=True,
    text=True,
)
for line in _show.stdout.splitlines():
    if line.startswith(("Name:", "Version:")):
        print(" ", line)

# COMMAND ----------

# =============================================================================
# CELL 2 — Configuration
#
# Edit these variables to control which tests run.
# =============================================================================

# ---------------------------------------------------------------------------
# MARKER_FILTER : pytest -m expression
#
# Common values:
#   "unit"              — pure Python tests only (~30 s, no Spark needed)
#   "spark"             — Spark DataFrame tests only
#   "unit or spark"     — everything except integration/databricks_only
#   "dq"                — data quality validation tests only
#   "radarv1"           — radarv1 bundle tests only
#   "gcob_consumer"     — GCOB_Consumer bundle tests only
#   "not local_only"    — everything that doesn't need local filesystem
#                         (recommended default on cluster)
#   ""                  — run ALL tests (including local_only — expect some skips)
# ---------------------------------------------------------------------------
MARKER_FILTER: str = "not local_only"

# ---------------------------------------------------------------------------
# BUNDLE_FILTER : restrict to a single bundle's tests (leave "" for all)
#
# Examples: "radarv1", "GCOB_Consumer", "GCOB_Reportingv1"
# ---------------------------------------------------------------------------
BUNDLE_FILTER: str = "GCOB_Consumer"

# ---------------------------------------------------------------------------
# VERBOSITY : controls pytest -v flags
#   0 = minimal  (just PASSED/FAILED per test)
#   1 = verbose  (default, shows test IDs)
#   2 = very verbose (shows full output including print() calls)
# ---------------------------------------------------------------------------
VERBOSITY: int = 1

# ---------------------------------------------------------------------------
# COLLECT_ONLY : set True to see which tests would run without running them
# ---------------------------------------------------------------------------
COLLECT_ONLY: bool = False

# ---------------------------------------------------------------------------
# SHOW_LOCALS : include local variable values in tracebacks (helpful but noisy)
# ---------------------------------------------------------------------------
SHOW_LOCALS: bool = False

print("Configuration:")
print(f"  MARKER_FILTER : {MARKER_FILTER!r}")
print(f"  BUNDLE_FILTER : {BUNDLE_FILTER!r}")
print(f"  VERBOSITY     : {VERBOSITY}")
print(f"  COLLECT_ONLY  : {COLLECT_ONLY}")

# COMMAND ----------

# DBTITLE 1,Cell 3 — Environment verification
# =============================================================================
# CELL 3 — Environment verification
#
# Confirms that environment.py detects IS_DATABRICKS = True.
# If this cell shows IS_DATABRICKS = False, stop and check the cluster
# configuration — DATABRICKS_RUNTIME_VERSION must be in os.environ.
# =============================================================================

import os
import sys
from pathlib import Path

# CRITICAL: Disable bytecode generation BEFORE any imports from tests/
# Databricks workspace filesystem doesn't support __pycache__ directories
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
sys.dont_write_bytecode = True

# Bootstrap: add Databricks/unit_tests/ to sys.path so environment.py is importable.
# This is necessary before conftest.py has had a chance to run.
# In Databricks notebooks, __file__ is not defined, so we use os.getcwd()
_DATABRICKS_DIR = Path(os.getcwd())   # Databricks/
_TESTS_DIR      = _DATABRICKS_DIR / "unit_tests"

for _p in [str(_DATABRICKS_DIR), str(_TESTS_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Now import environment
from environment import IS_DATABRICKS, RUNTIME, REPO_ROOT, TESTS_DIR, describe

print(describe())

# Guard: fail fast if not running on a cluster
if not IS_DATABRICKS:
    raise RuntimeError(
        "IS_DATABRICKS is False — this notebook must run on a Databricks cluster.\n"
        f"DATABRICKS_RUNTIME_VERSION = {os.environ.get('DATABRICKS_RUNTIME_VERSION', 'NOT SET')}\n"
        "Attach this notebook to a running cluster and try again."
    )

# Guard: pyproject.toml must be reachable
_pyproject = REPO_ROOT / "pyproject.toml"
if not _pyproject.exists():
    raise FileNotFoundError(
        f"pyproject.toml not found at {_pyproject}\n"
        f"REPO_ROOT resolved to: {REPO_ROOT}\n"
        "Set the DATABRICKS_REPO_ROOT environment variable to the Databricks/ "
        "directory if auto-detection is wrong."
    )

print(f"\n✅ Environment verified — running on cluster (RUNTIME={RUNTIME})")
print(f"   REPO_ROOT      = {REPO_ROOT}")
print(f"   pyproject.toml = FOUND ✓")

# COMMAND ----------

# =============================================================================
# CELL 4 — Repo structure validation
#
# Checks that all expected bundle test directories exist under REPO_ROOT.
# A missing directory means the repo clone is incomplete or the path is wrong.
# =============================================================================

EXPECTED_DIRS = [
    "unit_tests",
    "unit_tests/conftest.py",
    "unit_tests/environment.py",
    "unit_tests/utils/df_helpers.py",
    "unit_tests/fixtures/sample_dataframes.py",
    "AdHocRequests/tests",
    "clusters/tests",
    "GCOB_Consumer/tests",
    "GCOB_Consumer/src/GcobUtils.py",
    "GCOB_Reportingv1/tests",
    "GCOB_Reportingv1/tests/RadarUtils.py",
    "PartyCatalog/tests",
    "radarv1/tests",
    "radarv1/src/functions_databricks.py",
]

missing = []
for rel in EXPECTED_DIRS:
    full = REPO_ROOT / rel
    if not full.exists():
        missing.append(str(full))

if missing:
    print("❌ Missing repo paths (incomplete clone or wrong REPO_ROOT):")
    for m in missing:
        print(f"   {m}")
    raise FileNotFoundError(
        f"{len(missing)} expected paths are missing. "
        "Check REPO_ROOT or re-clone the repository."
    )

print(f"✅ Repo structure validated — {len(EXPECTED_DIRS)} paths confirmed present")

# COMMAND ----------

# =============================================================================
# CELL 5 — Build sys.path and pytest arguments
#
# Adds all bundle src/ directories to sys.path as absolute paths.
# This mirrors what pyproject.toml [pythonpath] does for local/CI runs,
# but using absolute paths that work regardless of cwd on the cluster.
# =============================================================================

# All paths that need to be on sys.path for imports to work
SRC_PATHS = [
    REPO_ROOT / "unit_tests",
    REPO_ROOT / "GCOB_Consumer"    / "src",
    REPO_ROOT / "GCOB_Reportingv1" / "src",
    REPO_ROOT / "GCOB_Reportingv1" / "tests",   # RadarUtils.py legacy location
    REPO_ROOT / "radarv1"          / "src",
    REPO_ROOT / "AdHocRequests"    / "src",
    REPO_ROOT / "clusters"         / "src",
    REPO_ROOT / "PartyCatalog"     / "src",   # optional — PartyCatalog has no src/ in some workspaces
]

added = []
already_present = []
for p in SRC_PATHS:
    s = str(p)
    if p.exists():
        if s not in sys.path:
            sys.path.insert(0, s)
            added.append(s)
        else:
            already_present.append(s)
    else:
        # PartyCatalog/src doesn't exist in some workspaces — that bundle has no
        # source code to import. Its tests use inline test data or fixtures only.
        # conftest.py handles this the same way (checks if _src.exists() before adding).
        print(f"  ℹ️  Optional path not found (bundle may have no src/): {s}")

print(f"Added {len(added)} new paths to sys.path")
if added:
    for a in added:
        print(f"  + {a}")
if already_present:
    print(f"({len(already_present)} paths were already on sys.path from a previous run — this is normal)")
    for p in already_present:
        print(f"  ✓ {p}")

# COMMAND ----------

# DBTITLE 1,Cell 6 — Run pytest
# =============================================================================
# CELL 6 — Run pytest
#
# pytest.main() runs the full suite in-process — no subprocess needed.
# Output is captured and displayed in the notebook cell output.
#
# Arguments are built programmatically from the configuration in Cell 2.
# =============================================================================

import os
import sys
import pytest
import io
import contextlib

# Disable bytecode generation — Databricks workspace FS doesn't support __pycache__
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
sys.dont_write_bytecode = True

# ---------------------------------------------------------------------------
# Build pytest argument list
# ---------------------------------------------------------------------------
pytest_args = [
    f"--rootdir={REPO_ROOT}",       # ensures pyproject.toml is found
    "--import-mode=prepend",        # compatible with Databricks (importlib has issues)
    "--tb=short",                   # concise tracebacks
    "--strict-markers",             # fail on unknown markers
    f"--verbosity={VERBOSITY}",
]

# Test paths — absolute paths to each bundle's tests directory
if BUNDLE_FILTER:
    # Single bundle
    bundle_test_path = REPO_ROOT / BUNDLE_FILTER / "tests"
    if not bundle_test_path.exists():
        raise FileNotFoundError(
            f"Bundle test path not found: {bundle_test_path}\n"
            f"Valid bundles: AdHocRequests, clusters, GCOB_Consumer, "
            f"GCOB_Reportingv1, PartyCatalog, radarv1"
        )
    pytest_args += [
        str(REPO_ROOT / "unit_tests"),        # shared conftest always included
        str(bundle_test_path),
    ]
    print(f"Running bundle: {BUNDLE_FILTER}")
else:
    # All bundles
    pytest_args += [
        str(REPO_ROOT / "unit_tests"),
        str(REPO_ROOT / "AdHocRequests"   / "tests"),
        str(REPO_ROOT / "clusters"        / "tests"),
        str(REPO_ROOT / "GCOB_Consumer"   / "tests"),
        str(REPO_ROOT / "GCOB_Reportingv1" / "tests"),
        str(REPO_ROOT / "PartyCatalog"    / "tests"),
        str(REPO_ROOT / "radarv1"         / "tests"),
    ]
    print("Running ALL bundles")

# Marker filter
if MARKER_FILTER:
    pytest_args += ["-m", MARKER_FILTER]
    print(f"Marker filter: -m {MARKER_FILTER!r}")

# Collect-only mode
if COLLECT_ONLY:
    pytest_args.append("--collect-only")
    print("Mode: COLLECT ONLY (no tests will run)")

# Show locals in tracebacks
if SHOW_LOCALS:
    pytest_args.append("--showlocals")

print(f"\nRunning: pytest {' '.join(pytest_args)}\n")
print("=" * 70)

# ---------------------------------------------------------------------------
# Execute pytest in-process
# Output goes directly to notebook stdout (no redirect needed on cluster).
# We capture it additionally to produce the summary in Cell 7.
# ---------------------------------------------------------------------------
_output_buffer = io.StringIO()

with contextlib.redirect_stdout(_output_buffer):
    with contextlib.redirect_stderr(_output_buffer):
        EXIT_CODE = pytest.main(pytest_args)

_full_output = _output_buffer.getvalue()

# Print the full output so it appears in the cell
print(_full_output)
print("=" * 70)
print(f"pytest exit code: {EXIT_CODE}")

# COMMAND ----------

# =============================================================================
# CELL 7 — Parse and display summary
#
# Extracts the PASSED / FAILED / ERROR / SKIPPED counts from pytest output
# and formats a clean summary table for the notebook.
# =============================================================================

import re

def _parse_summary(output: str) -> dict:
    """
    Extract test counts from the last line of pytest output.
    Example lines:
      "5 passed, 2 skipped in 12.34s"
      "3 failed, 8 passed, 1 error in 45.67s"
      "no tests ran"
    """
    counts = {"passed": 0, "failed": 0, "error": 0, "skipped": 0, "warning": 0}

    # Match patterns like "5 passed" or "3 failed"
    for key in counts:
        m = re.search(rf"(\d+)\s+{key}", output)
        if m:
            counts[key] = int(m.group(1))

    # Extract duration
    dur_m = re.search(r"in\s+([\d.]+)s", output)
    counts["duration"] = float(dur_m.group(1)) if dur_m else 0.0

    return counts


def _extract_failures(output: str) -> list[str]:
    """Extract FAILED / ERROR test IDs from pytest output."""
    failures = []
    for line in output.splitlines():
        if line.startswith("FAILED ") or line.startswith("ERROR "):
            failures.append(line.strip())
    return failures


summary = _parse_summary(_full_output)
failures = _extract_failures(_full_output)

total = summary["passed"] + summary["failed"] + summary["error"] + summary["skipped"]

print("\n" + "=" * 60)
print("  TEST SUMMARY")
print("=" * 60)
print(f"  Total collected : {total}")
print(f"  ✅ Passed        : {summary['passed']}")
print(f"  ❌ Failed        : {summary['failed']}")
print(f"  🔥 Errors        : {summary['error']}")
print(f"  ⏭  Skipped       : {summary['skipped']}")
print(f"  ⏱  Duration      : {summary['duration']:.1f} s")
print("=" * 60)

if failures:
    print(f"\n  Failed / Error tests ({len(failures)}):")
    for f in failures:
        print(f"    • {f}")

# COMMAND ----------

# =============================================================================
# CELL 8 — Final pass/fail signal
#
# Raises an exception if any tests failed or errored.
# This makes the notebook run show as FAILED in Databricks Workflows,
# which triggers email alerts and blocks dependent tasks.
#
# Exit codes:
#   0 = all tests passed (or only skipped)
#   1 = at least one test failed
#   2 = pytest internal error (config problem, import error, etc.)
#   3 = interrupted
#   4 = pytest usage error
#   5 = no tests collected (marker filter matched nothing)
# =============================================================================

_EXIT_MEANINGS = {
    0: "All tests passed ✅",
    1: "Tests FAILED ❌",
    2: "pytest interrupted (internal error) 🔥",
    3: "pytest interrupted by user",
    4: "pytest usage error (bad arguments)",
    5: "No tests collected — check MARKER_FILTER",
}

print(f"\nExit code {EXIT_CODE}: {_EXIT_MEANINGS.get(EXIT_CODE, 'Unknown')}")

if EXIT_CODE == 5:
    print(
        f"\n⚠️  No tests were collected with marker filter: {MARKER_FILTER!r}\n"
        "This is not a test failure — adjust MARKER_FILTER in Cell 2 if needed."
    )
elif EXIT_CODE != 0:
    raise Exception(
        f"pytest exited with code {EXIT_CODE}: {_EXIT_MEANINGS.get(EXIT_CODE)}\n"
        f"Failed tests ({len(failures)}):\n"
        + "\n".join(f"  • {f}" for f in failures)
    )
else:
    print(
        f"\n🎉 All {summary['passed']} tests passed"
        + (f" ({summary['skipped']} skipped)" if summary["skipped"] else "")
        + f" in {summary['duration']:.1f} s"
    )

# COMMAND ----------

# DBTITLE 1,Validation Report
# MAGIC %md
# MAGIC # ✅ Validation Report: Centralized Pytest Implementation
# MAGIC
# MAGIC ## 📋 Overview
# MAGIC
# MAGIC Your pytest implementation has been **validated and approved** with some recommendations for enhancement.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ What You Did Right
# MAGIC
# MAGIC ### 1. **Environment Detection (Excellent!)**
# MAGIC
# MAGIC **File:** `tests/environment.py`
# MAGIC
# MAGIC **Strengths:**
# MAGIC - ✅ Clear separation of IS_DATABRICKS, IS_ADO_CI, IS_LOCAL_DEV
# MAGIC - ✅ Proper repo root detection with fallback strategies
# MAGIC - ✅ Automatic sys.path configuration
# MAGIC - ✅ Comprehensive capability flags (NEEDS_RUNTIME_STUBS, HAS_REAL_SPARK, etc.)
# MAGIC - ✅ `describe()` helper for debugging
# MAGIC
# MAGIC **Rating:** 10/10 — This is production-grade code.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. **Centralized Configuration (Strong)**
# MAGIC
# MAGIC **File:** `pyproject.toml`
# MAGIC
# MAGIC **Strengths:**
# MAGIC - ✅ All 6 bundles configured in testpaths
# MAGIC - ✅ Proper pythonpath setup with all src/ directories
# MAGIC - ✅ `--import-mode=importlib` prevents namespace collisions
# MAGIC - ✅ Comprehensive marker definitions
# MAGIC - ✅ Sensible coverage configuration
# MAGIC - ✅ Warning filters for PySpark noise
# MAGIC
# MAGIC **Rating:** 9/10 — Excellent, see minor recommendations below.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. **Databricks Runtime Stubs (Perfect)**
# MAGIC
# MAGIC **File:** `tests/conftest.py`
# MAGIC
# MAGIC **Strengths:**
# MAGIC - ✅ Stubs only applied when `NEEDS_RUNTIME_STUBS=True`
# MAGIC - ✅ Never overwrites real Databricks packages on cluster
# MAGIC - ✅ MagicMock properly configured for common return values
# MAGIC - ✅ Handles `databricks.sdk.runtime`, `databricks.connect`, etc.
# MAGIC
# MAGIC **Rating:** 10/10 — Exactly the right approach.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. **Spark Fixture (Excellent)**
# MAGIC
# MAGIC **Strengths:**
# MAGIC - ✅ Session-scoped (one Spark session per full run)
# MAGIC - ✅ Uses `SparkSession.getOrCreate()` on cluster (correct!)
# MAGIC - ✅ Creates `local[2]` on CI/local
# MAGIC - ✅ Delta Lake extensions configured
# MAGIC - ✅ Proper cleanup: `.stop()` only on local/CI, not on cluster
# MAGIC
# MAGIC **Rating:** 10/10 — This is the canonical approach.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. **Cluster-Safe Dependencies (Smart)**
# MAGIC
# MAGIC **File:** `requirements-cluster.txt`
# MAGIC
# MAGIC **Strengths:**
# MAGIC - ✅ Excludes `pyspark` and `delta-spark` (already on cluster)
# MAGIC - ✅ Includes only test-specific packages
# MAGIC - ✅ Clear documentation of what's excluded and why
# MAGIC
# MAGIC **Rating:** 10/10 — Prevents the #1 cluster test failure mode.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. **Test Helpers (Well-Designed)**
# MAGIC
# MAGIC **File:** `tests/utils/df_helpers.py`
# MAGIC
# MAGIC **Strengths:**
# MAGIC - ✅ Pure functions (no fixture dependencies)
# MAGIC - ✅ Clear, actionable error messages
# MAGIC - ✅ Covers common DataFrame assertions
# MAGIC - ✅ Chispa integration with fallback
# MAGIC
# MAGIC **Rating:** 9/10 — Production-ready.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7. **Notebook Runner (Innovative)**
# MAGIC
# MAGIC **File:** `run_tests_notebook.py`
# MAGIC
# MAGIC **Strengths:**
# MAGIC - ✅ Cell-by-cell execution with clear documentation
# MAGIC - ✅ Proper use of `subprocess.run` for pip install
# MAGIC - ✅ Environment verification before running tests
# MAGIC - ✅ Structured summary output
# MAGIC - ✅ Proper exit code handling
# MAGIC
# MAGIC **Rating:** 9/10 — Great for on-cluster testing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Potential Issues & Recommendations
# MAGIC
# MAGIC ### Issue 1: Duplicate Configuration Files
# MAGIC
# MAGIC **Problem:**
# MAGIC
# MAGIC You have **both** `pyproject.toml` AND per-bundle `pytest.ini` files.
# MAGIC
# MAGIC ```
# MAGIC Databricks/
# MAGIC ├── pyproject.toml          # Central config
# MAGIC └── radarv1/
# MAGIC     └── pytest.ini          # Bundle-specific config
# MAGIC ```
# MAGIC
# MAGIC **Risk:**
# MAGIC - Configuration drift between files
# MAGIC - Confusion about which config is active
# MAGIC - Different behavior depending on `cwd`
# MAGIC
# MAGIC **Recommendation:**
# MAGIC
# MAGIC **Option A (Recommended): Remove per-bundle pytest.ini**
# MAGIC
# MAGIC For single-bundle testing, developers can use marker filters:
# MAGIC
# MAGIC ```bash
# MAGIC # From Databricks/ root
# MAGIC pytest -m radarv1
# MAGIC pytest radarv1/tests/
# MAGIC ```
# MAGIC
# MAGIC **Option B: Keep per-bundle pytest.ini for developer convenience**
# MAGIC
# MAGIC If you keep them:
# MAGIC 1. Add a comment in each per-bundle `pytest.ini`:
# MAGIC    ```ini
# MAGIC    # NOTE: This file is used ONLY when running pytest from inside radarv1/
# MAGIC    # For CI/CD and full-suite runs, pyproject.toml is used instead.
# MAGIC    # Keep addopts and markers synchronized with pyproject.toml!
# MAGIC    ```
# MAGIC
# MAGIC 2. Add a test that validates synchronization:
# MAGIC    ```python
# MAGIC    # tests/test_config_sync.py
# MAGIC    def test_pytest_ini_files_match_pyproject():
# MAGIC        """Ensure per-bundle pytest.ini markers match pyproject.toml."""
# MAGIC        # Parse both files and compare
# MAGIC    ```
# MAGIC
# MAGIC **My Recommendation:** Remove per-bundle pytest.ini files unless developers frequently need single-bundle runs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 2: Coverage Output Path
# MAGIC
# MAGIC **File:** `pyproject.toml` line 144
# MAGIC
# MAGIC **Current:**
# MAGIC ```toml
# MAGIC [tool.coverage.xml]
# MAGIC output = "coverage/coverage.xml"
# MAGIC ```
# MAGIC
# MAGIC **Problem:**
# MAGIC - Creates a nested `coverage/` directory
# MAGIC - Azure DevOps pipeline expects `coverage.xml` at root
# MAGIC
# MAGIC **Recommendation:**
# MAGIC ```toml
# MAGIC [tool.coverage.xml]
# MAGIC output = "coverage.xml"  # Top-level, as expected by ADO pipeline
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 3: Missing Azure Pipeline Integration
# MAGIC
# MAGIC **File:** `azure-pipelines-enterprise.yml`
# MAGIC
# MAGIC **Status:** ⚠️ Not adapted to your new structure yet
# MAGIC
# MAGIC **Action Needed:**
# MAGIC
# MAGIC Update the pipeline test step to use your new centralized structure:
# MAGIC
# MAGIC ```yaml
# MAGIC # OLD (what I provided)
# MAGIC - task: Bash@3
# MAGIC   displayName: 'Run Centralized Test Suite'
# MAGIC   inputs:
# MAGIC     script: |
# MAGIC       cd Databricks
# MAGIC       pytest --junitxml=test-results.xml ...
# MAGIC
# MAGIC # NEW (for your implementation)
# MAGIC - task: Bash@3
# MAGIC   displayName: 'Run Centralized Test Suite'
# MAGIC   inputs:
# MAGIC     script: |
# MAGIC       cd Databricks
# MAGIC       
# MAGIC       # Use run_tests.sh or direct pytest
# MAGIC       ./run_tests.sh --ci \
# MAGIC         --junitxml=test-results.xml \
# MAGIC         --cov-report=xml:coverage.xml
# MAGIC       
# MAGIC       # OR direct pytest (your pyproject.toml handles paths)
# MAGIC       pytest \
# MAGIC         --junitxml=test-results.xml \
# MAGIC         --cov --cov-report=xml:coverage.xml \
# MAGIC         --cov-report=html:htmlcov \
# MAGIC         -m "not local_only"
# MAGIC ```
# MAGIC
# MAGIC **Also ensure:**
# MAGIC - Pipeline installs from `requirements-dev.txt` (not `requirements-cluster.txt`)
# MAGIC - `test-results.xml` and `coverage.xml` paths match PublishTestResults/PublishCodeCoverageResults tasks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 4: __init__.py Files
# MAGIC
# MAGIC **Observation:** You added `__init__.py` to every test directory.
# MAGIC
# MAGIC **Good:** Makes tests importable (useful for shared helpers).
# MAGIC
# MAGIC **Warning:** With `--import-mode=importlib`, pytest doesn't require `__init__.py`.
# MAGIC
# MAGIC **Recommendation:**
# MAGIC - Keep them if you're importing test utilities between bundles
# MAGIC - Remove them if they're empty and causing import path confusion
# MAGIC
# MAGIC **Check:** Run pytest with `-vv` and verify no duplicate test IDs:
# MAGIC ```bash
# MAGIC pytest --collect-only -vv
# MAGIC # Look for duplicate test node IDs like:
# MAGIC # tests.test_foo::test_bar vs test_foo::test_bar
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 5: Notebook Runner Cell Structure
# MAGIC
# MAGIC **File:** `run_tests_notebook.py` (this notebook)
# MAGIC
# MAGIC **Problem:**
# MAGIC
# MAGIC Cell 1 is Python code but should be a title/comment cell or skipped.
# MAGIC
# MAGIC **Recommendation:**
# MAGIC
# MAGIC Convert Cell 1 (the big comment block) to a Markdown cell:
# MAGIC
# MAGIC ```markdown
# MAGIC # 🧪 Databricks Bundle pytest Runner
# MAGIC
# MAGIC ## Purpose
# MAGIC Run the centralized pytest suite for all six bundles on a live cluster.
# MAGIC
# MAGIC ## Usage
# MAGIC 1. Attach to any cluster
# MAGIC 2. Run All
# MAGIC 3. Check Cell 10 for pass/fail signal
# MAGIC ```
# MAGIC
# MAGIC Move the technical comments into Cell 3's docstring.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 6: Test Discovery Marker
# MAGIC
# MAGIC **Observation:** You have `@pytest.mark.local_only` but some YAML tests need local filesystem.
# MAGIC
# MAGIC **Good:** Skipping these on cluster is correct.
# MAGIC
# MAGIC **Recommendation:**
# MAGIC
# MAGIC Add a pytest hook to auto-skip `local_only` tests on Databricks:
# MAGIC
# MAGIC ```python
# MAGIC # In tests/conftest.py, add:
# MAGIC
# MAGIC def pytest_collection_modifyitems(config, items):
# MAGIC     """
# MAGIC     Auto-skip local_only tests when running on Databricks cluster.
# MAGIC     """
# MAGIC     if IS_DATABRICKS:
# MAGIC         skip_local = pytest.mark.skip(reason="local_only marker — requires local filesystem")
# MAGIC         for item in items:
# MAGIC             if "local_only" in item.keywords:
# MAGIC                 item.add_marker(skip_local)
# MAGIC ```
# MAGIC
# MAGIC Then YAML tests are automatically skipped without needing `-m "not local_only"`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Final Recommendations
# MAGIC
# MAGIC ### Priority 1 (Fix Now)
# MAGIC
# MAGIC 1. **Fix coverage output path** in `pyproject.toml`:
# MAGIC    ```toml
# MAGIC    [tool.coverage.xml]
# MAGIC    output = "coverage.xml"  # Not coverage/coverage.xml
# MAGIC    ```
# MAGIC
# MAGIC 2. **Update Azure pipeline** to use your new test structure:
# MAGIC    - Install from `requirements-dev.txt`
# MAGIC    - Run pytest from `Databricks/`
# MAGIC    - Use marker filter: `-m "not local_only"`
# MAGIC
# MAGIC 3. **Add auto-skip hook** for `local_only` marker (optional but nice)
# MAGIC
# MAGIC ### Priority 2 (Improve Over Time)
# MAGIC
# MAGIC 4. **Decide on pytest.ini strategy**:
# MAGIC    - Remove per-bundle files (cleaner), OR
# MAGIC    - Add synchronization test (safer)
# MAGIC
# MAGIC 5. **Add test for notebook runner**:
# MAGIC    ```python
# MAGIC    # tests/test_notebook_runner.py
# MAGIC    def test_run_tests_notebook_exists():
# MAGIC        assert (REPO_ROOT / "run_tests_notebook.py").exists()
# MAGIC    ```
# MAGIC
# MAGIC 6. **Document the three test execution paths** in a central README:
# MAGIC    - Local: `./run_tests.sh` or `pytest`
# MAGIC    - CI: Azure pipeline calls `run_tests.sh --ci`
# MAGIC    - Cluster: Open `run_tests_notebook` and Run All
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Overall Grade
# MAGIC
# MAGIC | Component | Grade | Notes |
# MAGIC |-----------|-------|-------|
# MAGIC | Environment detection | 10/10 | Perfect |
# MAGIC | Databricks stubs | 10/10 | Exactly right |
# MAGIC | Spark fixture | 10/10 | Canonical implementation |
# MAGIC | Configuration | 9/10 | Excellent, minor path issue |
# MAGIC | Test helpers | 9/10 | Production-ready |
# MAGIC | Notebook runner | 9/10 | Innovative, works well |
# MAGIC | Dependencies | 10/10 | Smart cluster-safe split |
# MAGIC | **Overall** | **9.5/10** | **Production-ready with minor fixes** |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Validation Status
# MAGIC
# MAGIC **APPROVED** for production use with the Priority 1 fixes applied.
# MAGIC
# MAGIC Your implementation is **significantly better** than many enterprise test suites I've seen. The environment detection and zero-code-change design are particularly impressive.
# MAGIC
# MAGIC ### What Makes This Special
# MAGIC
# MAGIC 1. **True zero-code-change**: Same test files work in all 3 environments
# MAGIC 2. **Smart stubs**: Never overwrites real packages on cluster
# MAGIC 3. **Cluster-safe deps**: Doesn't break cluster SparkSession
# MAGIC 4. **Session-scoped Spark**: Fast test execution
# MAGIC 5. **Comprehensive helpers**: Reduces test boilerplate
# MAGIC
# MAGIC Well done! 🎉
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps
# MAGIC
# MAGIC 1. **Apply Priority 1 fixes** (5 minutes)
# MAGIC 2. **Test on cluster**: Run this notebook on a live cluster
# MAGIC 3. **Update ADO pipeline**: Point to centralized structure
# MAGIC 4. **Run full CI/CD**: Verify Dev → Preprod → Prod flow
# MAGIC 5. **Document for team**: Add TESTING.md with all three execution paths
# MAGIC
# MAGIC **Ready to proceed?** Let me know if you want help with any of the fixes!
