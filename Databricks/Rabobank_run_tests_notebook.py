# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Databricks Bundle pytest Runner
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

# DBTITLE 1,Install test dependencies
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

print(f"Installing from: {_REQ_FILE}")

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

print("Test dependencies installed")

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

# DBTITLE 1,Configuration
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
#BUNDLE_FILTER: str = ""
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

# DBTITLE 1,Environment verification
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

print(f"\nEnvironment verified — running on cluster (RUNTIME={RUNTIME})")
print(f"   REPO_ROOT      = {REPO_ROOT}")
print("    pyproject.toml = FOUND")

# COMMAND ----------

# DBTITLE 1,Repo structure validation
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
    print("Missing repo paths (incomplete clone or wrong REPO_ROOT):")
    for m in missing:
        print(f"   {m}")
    raise FileNotFoundError(
        f"{len(missing)} expected paths are missing. "
        "Check REPO_ROOT or re-clone the repository."
    )

print(f"Repo structure validated — {len(EXPECTED_DIRS)} paths confirmed present")

# COMMAND ----------

# DBTITLE 1,Build sys.path and pytest arguments
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
        print(f"Optional path not found (bundle may have no src/): {s}")

print(f"Added {len(added)} new paths to sys.path")
if added:
    for a in added:
        print(f"  + {a}")
if already_present:
    print(f"({len(already_present)} paths were already on sys.path from a previous run — this is normal)")
    for p in already_present:
        print(f" {p}")

# COMMAND ----------

# DBTITLE 1,Run pytest
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
    "--continue-on-collection-errors",  # don't let one bad import stop all tests
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

# ---------------------------------------------------------------------------
# Coverage arguments
# ---------------------------------------------------------------------------
pytest_args.extend([
    "--cov-report=term",
    "--cov-report=term-missing",
    "--cov-config=pyproject.toml",
])
if BUNDLE_FILTER:
    pytest_args.append(f"--cov={BUNDLE_FILTER}/src")
else:
    for _b in ["radarv1", "GCOB_Consumer", "GCOB_Reportingv1", "clusters", "AdHocRequests", "PartyCatalog"]:
        pytest_args.append(f"--cov={_b}/src")

# Always write GCOB_Party inline source to temp file for coverage tracking.
# This must happen BEFORE pytest.main() so coverage can find the file.
# It must be OUTSIDE the 'if GCOB_Party not in sys.modules' block so it
# runs even when GCOB_Party is already in sys.modules from a prior run.
_cov_dir = "/tmp/gcob_party_cov"
os.makedirs(_cov_dir, exist_ok=True)
_cov_file_path = os.path.join(_cov_dir, "GCOB_Party.py")
from datetime import datetime, timedelta
from pyspark.sql.functions import lit
_inline_src = '''
def calculate_business_date():
    return (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

def create_gcob_legacy2_view(spark):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW Gcob_Legacy2_Clients AS
        select distinct
        cast(GcobId as string) as GcobId
        ,case when ClientType = 'Legal Entity' then concat('LE_',GcobId) else concat('NP_NPPC_',GcobId) end as UniqueGcobId
        ,'GCOB' as SourceSystem
        ,FullLegalName
        ,ClientType as PartyType
        from global_temp.party_case_client_details
        union
        select distinct
        cast(GcobId as string) as GcobId
        ,UniqueGcobId
        ,'Legacy2' as SourceSystem
        ,FullLegalName
        ,ClientType as PartyType
        from global_temp.Legacy2_client
    """)

def create_party_view(spark):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW Party AS
        select distinct
        c.GcobId
        ,c.UniqueGcobId
        ,c.SourceSystem
        ,case when p.UniqueGcobId is not null then CONCAT('protected account ', p.gcobid) else c.FullLegalName end as FullLegalName
        ,c.PartyType
        from Gcob_Legacy2_Clients c
        left join global_temp.Gcob_protectedClients p on c.UniqueGcobId = p.UniqueGcobId
    """)

def add_business_date_column(df, business_date):
    return df.withColumn("BusinessDate", lit(business_date))
'''
with open(_cov_file_path, "w") as _f:
    _f.write(_inline_src)
pytest_args.append(f"--cov={_cov_dir}")

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
# Import GCOB_Party notebook (no .py extension).
# On normal Databricks Runtime clusters: use REST API with credentials from
#   dbutils.notebook.getContext() and spark.conf.
# On serverless: fall back to WorkspaceClient (SDK has built-in auth).
# Extract only the function definitions cell and exec it — avoids errors
# from env var reads and spark.sql calls in other notebook cells.
# ---------------------------------------------------------------------------
if 'GCOB_Party' not in sys.modules:
    import types, base64, requests, os
    from datetime import datetime, timedelta
    from pyspark.sql.functions import lit

    _nb_path = str(REPO_ROOT / "GCOB_Consumer" / "src" / "GCOB_Party")
    if _nb_path.startswith('/Workspace/'):
        _nb_path = _nb_path[len('/Workspace'):]
    _imported = False
    _content = None

    # Method 1: REST API (works on normal Databricks Runtime clusters)
    if not _imported:
        _host = None
        _token = None

        # Get host from dbutils.notebook.getContext().apiUrl()
        try:
            _host = dbutils.notebook.getContext().apiUrl().get()
        except Exception:
            pass

        # Get host from spark.conf
        if not _host:
            for _key in ["spark.databricks.service.url", "spark.databricks.host"]:
                try:
                    _host = spark.conf.get(_key)
                    break
                except Exception:
                    pass

        # Get token from dbutils.notebook.getContext().extraContext()
        if not _token:
            try:
                _extra = dbutils.notebook.getContext().extraContext().get()
                # Try common key names, then scan all keys for 'token' or 'auth'
                for _ek in ["authToken", "auth_token", "authToken.0"]:
                    if _ek in _extra:
                        _token = _extra[_ek]
                        break
                if not _token:
                    for _ek, _ev in _extra.items():
                        if ("token" in _ek.lower() or "auth" in _ek.lower()) and _ev:
                            _token = _ev
                            break
            except Exception:
                pass

        # Get token from spark.conf (try specific keys, then scan)
        if not _token:
            for _key in ["spark.databricks.driver.token", "spark.databricks.token",
                         "spark.databricks.service.token"]:
                try:
                    _token = spark.conf.get(_key)
                    break
                except Exception:
                    pass
        if not _token:
            try:
                for _key, _val in spark.conf.getAll().items():
                    if "token" in _key.lower() and _val and len(_val) > 10:
                        _token = _val
                        break
            except Exception:
                pass

        # Get token from /databricks/cluster/driver_secret_key (normal clusters)
        if not _token:
            try:
                with open("/databricks/cluster/driver_secret_key") as _f:
                    _token = _f.read().strip()
            except Exception:
                pass

        # Fall back to environment variables
        if not _host:
            _host = os.environ.get("DATABRICKS_HOST")
        if not _token:
            _token = os.environ.get("DATABRICKS_TOKEN")

        if _host and _token:
            try:
                _resp = requests.get(
                    f"{_host.rstrip('/')}/api/2.0/workspace/export",
                    params={"path": _nb_path, "format": "SOURCE"},
                    headers={"Authorization": f"Bearer {_token}"},
                    timeout=10,
                )
                _resp.raise_for_status()
                _content = base64.b64decode(_resp.json()["content"]).decode("utf-8")
                _imported = True
                print("  GCOB_Party: exported via REST API")
            except Exception as e:
                print(f"  GCOB_Party: REST API failed ({e})")
        else:
            print(f"  GCOB_Party: REST API no credentials (host={_host is not None}, token={_token is not None})")

    # Method 2: WorkspaceClient (works on serverless and clusters with newer SDK)
    if not _imported:
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.service.workspace import ExportFormat
            _w = WorkspaceClient()
            _exported = _w.workspace.export(path=_nb_path, format=ExportFormat.SOURCE)
            _content = base64.b64decode(_exported.content).decode("utf-8")
            _imported = True
            print("  GCOB_Party: exported via WorkspaceClient")
        except Exception as e:
            print(f"  GCOB_Party: WorkspaceClient failed ({e})")

    # Method 3: Inline function definitions (last resort — no credentials available).
    # These are the exact function definitions from Cell 3 of the GCOB_Party notebook.
    # Tests run the real notebook logic; update if the notebook changes.
    # The temp file and --cov are already set up unconditionally above.
    if not _imported:
        _mod = types.ModuleType("GCOB_Party")
        _mod.__dict__.update({"datetime": datetime, "timedelta": timedelta, "lit": lit})
        _mod.__file__ = _cov_file_path
        exec(compile(_inline_src, _cov_file_path, "exec"), _mod.__dict__)
        sys.modules["GCOB_Party"] = _mod
        _fns = [k for k in dir(_mod) if callable(getattr(_mod, k)) and not k.startswith('_')]
        print(f"  GCOB_Party: defined inline ({len(_fns)} functions: {_fns})")
        _imported = True

    if _imported and _content:
        _cells = _content.split("# COMMAND ----------")
        _mod = types.ModuleType("GCOB_Party")
        _mod.__dict__.update({"datetime": datetime, "timedelta": timedelta, "lit": lit})
        for _cell in _cells:
            if "def calculate_business_date" in _cell:
                exec(compile(_cell.strip(), _nb_path, "exec"), _mod.__dict__)
                break
        sys.modules["GCOB_Party"] = _mod
        _fns = [k for k in dir(_mod) if callable(getattr(_mod, k)) and not k.startswith('_')]
        print(f"  Imported GCOB_Party ({len(_fns)} functions: {_fns})")

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

# Strip raw coverage table from displayed output (keep for parsing)
_display_output = _full_output
_cov_marker = "================================ tests coverage"
if _cov_marker in _display_output:
    _display_output = _display_output.split(_cov_marker)[0]

print(_display_output)

# Show clean coverage summary
if _cov_marker in _full_output:
    print("\n" + "=" * 70)
    print("COVERAGE SUMMARY")
    print("=" * 70)
    _cov_section = _full_output.split(_cov_marker, 1)[1]
    for line in _cov_section.split('\n'):
        if line.strip() and not line.startswith("Coverage HTML") and not line.startswith("Coverage XML"):
            print(line)
    print("=" * 70)

print("=" * 70)
print(f"pytest exit code: {EXIT_CODE}")

# COMMAND ----------

# DBTITLE 1,Parse and display summary
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
pass_rate = (summary["passed"] / total * 100) if total > 0 else 0

print("\n" + "=" * 60)
print("  TEST SUMMARY")
print("=" * 60)
print(f"  Total collected : {total}")
print(f"  Passed          : {summary['passed']}")
print(f"  Failed          : {summary['failed']}")
print(f"  Errors          : {summary['error']}")
print(f"  Skipped         : {summary['skipped']}")
print(f"  Duration        : {summary['duration']:.1f} s")
print(f"  Pass rate       : {pass_rate:.0f}%")
print("=" * 60)

if failures:
    print(f"\n  Failed / Error tests ({len(failures)}):")
    for f in failures:
        print(f"    {f}")

if pass_rate == 100:
    print(f"\n  All {summary['passed']} tests passed ({pass_rate:.0f}%) in {summary['duration']:.1f} s")
