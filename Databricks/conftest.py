"""
Databricks/conftest.py  (ROOTDIR LEVEL)
=======================================
This is a copy of tests/conftest.py placed at the project rootdir so that
pytest makes shared fixtures (spark, make_df, mock_dbutils, mock_env) available
to ALL test directories — not just tests/ subdirectory.

Why this file is needed
------------------------
Pytest scopes conftest.py fixtures to the directory containing the conftest.py
and its subdirectories. The original tests/conftest.py only covers tests/ and
its children. Test directories like GCOB_Consumer/tests/, radarv1/tests/, etc.
are NOT subdirectories of tests/, so they could not see the spark fixture.

This rootdir-level conftest.py ensures every test in every bundle can access
the shared fixtures. The tests/conftest.py is kept for backward compatibility.

See tests/conftest.py for the full documentation of each fixture and hook.
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
# CRITICAL: Add tests/ to sys.path BEFORE importing environment.py.
# Unlike tests/conftest.py (which is in the same dir as environment.py),
# this file is at the project rootdir, so tests/ is not automatically on
# the import path.
# ---------------------------------------------------------------------------
_ROOTDIR = Path(__file__).resolve().parent
_TESTS_DIR = str(_ROOTDIR / "unit_tests")
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

# ---------------------------------------------------------------------------
# Import the environment detector.
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
# ---------------------------------------------------------------------------

def _stub_databricks_runtime() -> None:
    """
    Pre-register fake Databricks packages in sys.modules.
    """
    # Package name constants (SonarQube python:S1192 - avoid string literal duplication)
    _PKG_DATABRICKS = "databricks"
    _PKG_DATABRICKS_SDK = "databricks.sdk"
    _PKG_DATABRICKS_SDK_RUNTIME = "databricks.sdk.runtime"
    _PKG_DATABRICKS_CONNECT = "databricks.connect"
    _PKG_DATABRICKS_AUTOML = "databricks.automl"
    _PKG_DATABRICKS_FEATURE_STORE = "databricks.feature_store"
    _PKG_DATABRICKS_DLT = "databricks_dlt"
    _PKG_PYSPARK_DBUTILS = "pyspark.dbutils"
    
    runtime_stub = types.ModuleType(_PKG_DATABRICKS_SDK_RUNTIME)
    runtime_stub.spark   = MagicMock(name="stub_spark")
    runtime_stub.dbutils = MagicMock(name="stub_dbutils")

    stubs: dict[str, Any] = {
        _PKG_DATABRICKS:               MagicMock(name=_PKG_DATABRICKS),
        _PKG_DATABRICKS_SDK:           MagicMock(name=_PKG_DATABRICKS_SDK),
        _PKG_DATABRICKS_SDK_RUNTIME:   runtime_stub,
        _PKG_DATABRICKS_CONNECT:       MagicMock(name=_PKG_DATABRICKS_CONNECT),
        _PKG_DATABRICKS_AUTOML:        MagicMock(name=_PKG_DATABRICKS_AUTOML),
        _PKG_DATABRICKS_FEATURE_STORE: MagicMock(name=_PKG_DATABRICKS_FEATURE_STORE),
        _PKG_DATABRICKS_DLT:           MagicMock(name=_PKG_DATABRICKS_DLT),
        _PKG_PYSPARK_DBUTILS:          MagicMock(name=_PKG_PYSPARK_DBUTILS),
    }

    for name, stub in stubs.items():
        if name not in sys.modules:
            sys.modules[name] = stub

    sys.modules[_PKG_DATABRICKS_CONNECT].DatabricksSession = MagicMock(
        name="DatabricksSession"
    )
    sys.modules[_PKG_PYSPARK_DBUTILS].DBUtils = MagicMock(name="DBUtils")


if NEEDS_RUNTIME_STUBS:
    _stub_databricks_runtime()


# ---------------------------------------------------------------------------
# 2. Ensure bundle src/ directories are on sys.path
# ---------------------------------------------------------------------------

_BUNDLE_SRC_PATHS = [
    REPO_ROOT / "unit_tests",
    REPO_ROOT / "GCOB_Consumer"   / "src",
    REPO_ROOT / "GCOB_Reportingv1" / "src",
    REPO_ROOT / "GCOB_Reportingv1" / "tests",
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
    """
    from pyspark.sql import SparkSession  # noqa: PLC0415

    if hasattr(SparkSession, 'getOrCreate'):
        return SparkSession.getOrCreate()

    if hasattr(SparkSession, 'getActiveSession'):
        session = SparkSession.getActiveSession()
        if session is not None:
            return session

    return SparkSession.builder.getOrCreate()  # NOSONAR - On Databricks cluster, master and appName are pre-configured by runtime


@pytest.fixture(scope="session")
def spark():
    """
    SparkSession — session-scoped, created once per pytest run.
    """
    if IS_DATABRICKS:
        yield _make_cluster_session()
    else:
        session = _make_local_session()
        session.sparkContext.setLogLevel("ERROR")
        yield session
        session.stop()


# ---------------------------------------------------------------------------
# 4. mock_env — auto-applied to every test
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
    "NEXUSCLOUD_PASSWORD":            "test-nexus-pass",  # noqa: S105  # NOSONAR - fake test credential
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
    DATABRICKS  : real cluster dbutils
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
    """
    if IS_DATABRICKS:
        skip_local = pytest.mark.skip(
            reason="local_only marker — requires local filesystem (not available on cluster)"
        )
        for item in items:
            if "local_only" in item.keywords:
                item.add_marker(skip_local)