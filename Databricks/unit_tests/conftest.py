"""
Databricks/tests/conftest.py
============================
Central pytest configuration shared by ALL bundle test suites.

Works identically in all three environments with ZERO code changes:

  LOCAL-DEV  — developer laptop (Windows / Mac / Linux)
               Databricks SDK stubbed -> local[2] SparkSession

  ADO-CI     — Azure DevOps Linux agent
               Databricks SDK stubbed -> local[2] SparkSession
               TF_BUILD / BUILD_BUILDID set by Azure Pipelines

  DATABRICKS — Cluster notebook or Workflow task
               No stubs → SparkSession.getOrCreate() (the live cluster session)
               Real dbutils -> real env vars -> real Unity Catalog

How zero-code-change is achieved
---------------------------------
1. environment.py detects the runtime from os.environ at import time.
2. This file gates every environment-sensitive decision behind those flags:
     NEEDS_RUNTIME_STUBS  → inject MagicMocks (local-dev + ado-ci only)
     IS_DATABRICKS        → use real SparkSession and real dbutils
3. No test file ever needs to know which environment it's in.
   All tests use the same fixtures: spark, mock_dbutils, make_df, mock_env.

Databricks SparkSession note
-----------------------------
On a live cluster the correct call is SparkSession.getOrCreate() — this
returns the already-running cluster session with zero overhead.
DatabricksSession (Databricks Connect) is a REMOTE client for connecting
from *outside* a cluster. Using it inside a cluster creates a broken
second session. We use it here only on local-dev as a fallback if someone
has Databricks Connect configured; the primary path is always
SparkSession.getOrCreate() on the cluster.
"""

from __future__ import annotations

import os
import sys
import types
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Import the environment detector.
# environment.py is in the same directory (tests/) which is on sys.path
# both via pyproject.toml [pythonpath] and via environment.py's own
# sys.path insertion — so this import always resolves.
# ---------------------------------------------------------------------------
from environment import (
    IS_DATABRICKS,
    IS_ADO_CI,
    IS_LOCAL_DEV,
    NEEDS_RUNTIME_STUBS,
    REPO_ROOT,
    RUNTIME,
    TESTS_DIR,
)


# ---------------------------------------------------------------------------
# 1. Databricks runtime stubs
#
# Injected ONLY on LOCAL-DEV and ADO-CI (NEEDS_RUNTIME_STUBS=True).
# NEVER on a real cluster — the real packages are already present in
# sys.modules and must not be replaced with MagicMocks.
# ---------------------------------------------------------------------------

def _stub_databricks_runtime() -> None:
    """
    Pre-register fake Databricks packages in sys.modules.

    After this runs, any source file doing:
        from databricks.sdk.runtime import spark, dbutils
        from pyspark.dbutils import DBUtils
        from databricks.connect import DatabricksSession
    returns a MagicMock instead of raising ImportError.
    """
    runtime_stub = types.ModuleType("databricks.sdk.runtime")
    runtime_stub.spark   = MagicMock(name="stub_spark")
    runtime_stub.dbutils = MagicMock(name="stub_dbutils")

    stubs: dict[str, Any] = {
        "databricks":               MagicMock(name="databricks"),
        "databricks.sdk":           MagicMock(name="databricks.sdk"),
        "databricks.sdk.runtime":   runtime_stub,
        "databricks.connect":       MagicMock(name="databricks.connect"),
        "databricks.automl":        MagicMock(name="databricks.automl"),
        "databricks.feature_store": MagicMock(name="databricks.feature_store"),
        "databricks_dlt":           MagicMock(name="databricks_dlt"),
        "pyspark.dbutils":          MagicMock(name="pyspark.dbutils"),
    }

    for name, stub in stubs.items():
        if name not in sys.modules:
            sys.modules[name] = stub

    sys.modules["databricks.connect"].DatabricksSession = MagicMock(
        name="DatabricksSession"
    )
    sys.modules["pyspark.dbutils"].DBUtils = MagicMock(name="DBUtils")


# Run at module import time — before pytest begins collecting test files.
if NEEDS_RUNTIME_STUBS:
    _stub_databricks_runtime()


# ---------------------------------------------------------------------------
# 2. Ensure bundle src/ directories are on sys.path
#
# pyproject.toml [pythonpath] handles this when pytest is invoked from
# REPO_ROOT (local-dev, ado-ci). On Databricks the working directory is
# /databricks/driver, so pyproject.toml's relative paths don't resolve.
# We add absolute paths here as a safety net for the cluster path.
# ---------------------------------------------------------------------------

_BUNDLE_SRC_PATHS = [
    REPO_ROOT / "unit_tests",                          # utils/, fixtures/, environment.py
    REPO_ROOT / "GCOB_Consumer"   / "src",
    REPO_ROOT / "GCOB_Reportingv1" / "src",
    REPO_ROOT / "GCOB_Reportingv1" / "tests",     # RadarUtils.py (legacy location)
    REPO_ROOT / "radarv1"          / "src",
    REPO_ROOT / "AdHocRequests"    / "src",
    REPO_ROOT / "clusters"         / "src",
    REPO_ROOT / "PartyCatalog"     / "src",
]

for _src in _BUNDLE_SRC_PATHS:
    _str = str(_src)
    if _src.exists() and _str not in sys.path:
        sys.path.insert(0, _str)


# ---------------------------------------------------------------------------
# 3. Session-scoped SparkSession
#
# ONE session per full pytest run, regardless of how many test files use it.
#
# LOCAL-DEV / ADO-CI
#   → SparkSession.builder.master("local[2]").getOrCreate()
#     Creates a lightweight embedded JVM. ~8-10 s cold start, once per run.
#
# DATABRICKS (cluster notebook or workflow task)
#   → SparkSession.getOrCreate()
#     Returns the ALREADY RUNNING cluster session. Zero overhead.
#     IMPORTANT: we do NOT call session.stop() at teardown — the session
#     belongs to the cluster, not to pytest.
#
# Why NOT DatabricksSession on the cluster?
#   DatabricksSession is the Databricks Connect remote client. It is for
#   connecting to Databricks FROM a laptop. Inside a cluster it either
#   fails to import correctly or creates a misconfigured second session.
# ---------------------------------------------------------------------------

def _make_local_session():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .master("local[2]")
        .appName(f"pytest-databricks-{RUNTIME}")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def _make_cluster_session():
    """
    Return the cluster's live SparkSession.
    
    Handles both:
      - Traditional Databricks clusters: SparkSession.getOrCreate()
      - Spark Connect (Serverless): SparkSession.getActiveSession()
    
    pyspark is pre-installed on every Databricks runtime — this always works.
    """
    from pyspark.sql import SparkSession  # noqa: PLC0415
    
    # Try traditional cluster first (most common)
    if hasattr(SparkSession, 'getOrCreate'):
        return SparkSession.getOrCreate()
    
    # Fall back to Spark Connect / Serverless
    if hasattr(SparkSession, 'getActiveSession'):
        session = SparkSession.getActiveSession()
        if session is not None:
            return session
    
    # Last resort: use Builder
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def spark():
    """
    SparkSession — session-scoped, created once per pytest run.

    DATABRICKS  : cluster session via SparkSession.getOrCreate()
    LOCAL / CI  : local[2] session with Delta Lake extensions
    """
    if IS_DATABRICKS:
        yield _make_cluster_session()
        # Do NOT stop — the cluster session must outlive pytest
    else:
        session = _make_local_session()
        session.sparkContext.setLogLevel("ERROR")
        yield session
        session.stop()


# ---------------------------------------------------------------------------
# 4. mock_env — auto-applied to every test
#
# LOCAL-DEV / ADO-CI : patches os.environ with 18 fake Azure credentials
#                      so module-level os.environ reads in src/ files work.
# DATABRICKS         : no-op — real cluster env vars are already present.
# ---------------------------------------------------------------------------

_FAKE_ENV: dict[str, str] = {
    "APP_REG_APP_ID":                 "test-app-reg-00000000-0000-0000-0000-000000000000",
    "TENANT_ID":                      "test-tenant-00000000-0000-0000-0000-000000000000",
    "ENV":                            "dev",
    "GDP_STORAGE_NAME":               "testgdpstorage",
    "GDP_SA_STORAGE_NAME":            "testgdpsastorage",
    "GDP_NA_STORAGE_NAME":            "testgdpnastorage",
    "GNS_STORAGE_ACCOUNT":            "testgnsstorage",
    "GNS_CONTAINER":                  "testgnscontainer",
    "SALZReadStorage":                "testsalzstorage",
    "AU_GDP_Defined_Storage_Account": "testaugdpstorage",
    "CATALOG":                        "wr_fj_parties_and_risk_assessment_dev",
    "GCOB_UC_SCHEMA":                 "gcobreportingv103",
    "DATAVERSE_URL":                  "https://test-org.crm4.dynamics.com",
    "CosmosAccount":                  "test-cosmos-account",
    "subscriptionId":                 "test-subscription-id",
    "resourceGroup_API":              "test-resource-group",
    "NEXUSCLOUD_USERNAME":            "test-nexus-user",
    "NEXUSCLOUD_PASSWORD":            "test-nexus-pass",  # noqa: S105  (fake)
}

# ---------------------------------------------------------------------------
# Set fake env vars at MODULE LEVEL (before test collection) so that source
# files with module-level os.environ reads (GcobUtils.py, RadarUtils.py) can
# be imported without KeyError in ADO-CI and local-dev.
# On Databricks cluster, NEEDS_RUNTIME_STUBS is False — real env vars are used.
# The mock_env fixture below still uses monkeypatch.setenv() for per-test
# isolation; this module-level setting only ensures imports succeed.
# ---------------------------------------------------------------------------
if NEEDS_RUNTIME_STUBS:
    for _key, _val in _FAKE_ENV.items():
        os.environ.setdefault(_key, _val)


@pytest.fixture(autouse=True)
def mock_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Patch os.environ for every test.
    LOCAL/CI  : injects all _FAKE_ENV entries.
    DATABRICKS: no-op (real env vars already set on the cluster).
    """
    if NEEDS_RUNTIME_STUBS:
        for key, value in _FAKE_ENV.items():
            monkeypatch.setenv(key, value)


# ---------------------------------------------------------------------------
# 5. mock_dbutils — real on cluster, MagicMock elsewhere
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_dbutils():
    """
    DATABRICKS  : real cluster dbutils (fs, secrets, widgets, etc.)
    LOCAL / CI  : MagicMock pre-wired with the most common return values
    """
    if IS_DATABRICKS:
        from databricks.sdk.runtime import dbutils as _real  # noqa: PLC0415
        return _real
    else:
        m = MagicMock(name="dbutils")
        m.secrets.get.return_value  = "fake-secret-value"  # noqa: S105
        m.fs.ls.return_value        = []
        m.fs.mkdirs.return_value    = True
        m.fs.rm.return_value        = True
        return m


# ---------------------------------------------------------------------------
# 6. make_df — identical in all environments
# ---------------------------------------------------------------------------

@pytest.fixture
def make_df(spark):
    """
    Build a small PySpark DataFrame from a list of dicts.
    Schema is inferred from the first row.

    Usage:
        def test_something(make_df):
            df = make_df([{"id": "A", "val": 1}, {"id": "B", "val": None}])
            assert df.filter("val IS NULL").count() == 1
    """
    from pyspark.sql import Row

    def _factory(rows: list[dict]):
        if not rows:
            raise ValueError("make_df: rows list must not be empty")
        return spark.createDataFrame([Row(**r) for r in rows])

    return _factory


# ---------------------------------------------------------------------------
# 7. spark_with_delta — semantic alias
# ---------------------------------------------------------------------------

@pytest.fixture
def spark_with_delta(spark):
    """Alias for spark; use this name in tests that write Delta tables."""
    return spark


# ---------------------------------------------------------------------------
# 8. Session-start banner
# ---------------------------------------------------------------------------

def pytest_configure(config: pytest.Config) -> None:
    """Print a one-time environment banner so CI logs are self-describing."""
    # Pad to exactly 44 chars to keep box alignment
    def _pad(s: str, width: int = 44) -> str:
        return s[:width].ljust(width)

    spark_label  = "SparkSession.getOrCreate() (cluster)" if IS_DATABRICKS else "local[2] SparkSession"
    stubs_label  = "disabled — real cluster packages"    if IS_DATABRICKS else "enabled — MagicMock replacements"
    env_label    = "inactive — real cluster env vars"    if IS_DATABRICKS else "active — fake Azure credentials"
    root_label   = str(REPO_ROOT)

    banner = (
        "\n"
        "╔══════════════════════════════════════════════════════════╗\n"
        "║  pytest  ·  Databricks Asset Bundles                    ║\n"
        f"║  Runtime : {_pad(RUNTIME)}║\n"
        f"║  Spark   : {_pad(spark_label)}║\n"
        f"║  Stubs   : {_pad(stubs_label)}║\n"
        f"║  Env mock: {_pad(env_label)}║\n"
        f"║  Root    : {_pad(root_label)}║\n"
        "╚══════════════════════════════════════════════════════════╝"
    )
    try:
        print(banner)
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# 9. Auto-skip local_only tests on Databricks cluster
# ---------------------------------------------------------------------------

def pytest_collection_modifyitems(config: pytest.Config, items: list) -> None:
    """
    Auto-skip tests marked with @pytest.mark.local_only when running on
    Databricks cluster.

    This eliminates the need for manual -m "not local_only" filter in
    the notebook runner and ensures YAML config tests (which need local
    filesystem access) are automatically skipped on cluster.

    Tests marked local_only:
    - clusters/tests/test_clusters_config.py
    - AdHocRequests/tests/test_adhoc_config.py
    - PartyCatalog/tests/test_party_catalog_config.py
    """
    if IS_DATABRICKS:
        skip_local = pytest.mark.skip(
            reason="local_only marker — requires local filesystem (not available on cluster)"
        )
        for item in items:
            if "local_only" in item.keywords:
                item.add_marker(skip_local)
