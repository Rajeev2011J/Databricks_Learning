"""
Databricks/tests/conftest.py
============================
Central pytest configuration shared by ALL bundle test suites.

What this file does
-------------------
1. Stubs the entire Databricks runtime (databricks.sdk.runtime, pyspark.dbutils,
   databricks.connect) BEFORE any source module is imported, so top-level
   ``from databricks.sdk.runtime import spark, dbutils`` lines in source files
   resolve to MagicMock objects rather than raising ImportError.

2. Provides a *session-scoped* local SparkSession that all Spark tests share —
   created once, torn down after the full suite, keeping cold-start overhead to
   a single ~8-10 s hit.

3. Provides an *auto-used* ``mock_env`` fixture that patches os.environ with
   fake Azure / Databricks credentials so that modules that read env-vars at
   import time don't crash.

4. Provides reusable fixtures: ``mock_dbutils``, ``make_df``, ``spark_with_delta``.

Pytest discovers this file automatically because it lives in the ``tests/``
directory which is listed under ``testpaths`` in pyproject.toml.  Every bundle
test sub-directory inherits these fixtures without any extra import.
"""

from __future__ import annotations

import os
import sys
import types
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# 1. Databricks runtime stubs
#    Must run before any src module is imported by a test file.
# ---------------------------------------------------------------------------

def _stub_databricks_runtime() -> None:
    """
    Pre-register fake Databricks packages in sys.modules.

    After this runs, any source file doing:
        from databricks.sdk.runtime import spark, dbutils
        from pyspark.dbutils import DBUtils
        from databricks.connect import DatabricksSession
    will get a MagicMock instead of an ImportError or a real Databricks call.
    """
    # Build a realistic-looking runtime stub with named attributes
    runtime_stub = types.ModuleType("databricks.sdk.runtime")
    runtime_stub.spark   = MagicMock(name="stub_spark")
    runtime_stub.dbutils = MagicMock(name="stub_dbutils")

    stubs: dict[str, Any] = {
        "databricks":                  MagicMock(name="databricks"),
        "databricks.sdk":              MagicMock(name="databricks.sdk"),
        "databricks.sdk.runtime":      runtime_stub,
        "databricks.connect":          MagicMock(name="databricks.connect"),
        "databricks.automl":           MagicMock(name="databricks.automl"),
        "databricks.feature_store":    MagicMock(name="databricks.feature_store"),
        "databricks_dlt":              MagicMock(name="databricks_dlt"),
        "pyspark.dbutils":             MagicMock(name="pyspark.dbutils"),
    }

    for name, stub in stubs.items():
        if name not in sys.modules:
            sys.modules[name] = stub

    # Make DatabricksSession importable (used in main.py template files)
    sys.modules["databricks.connect"].DatabricksSession = MagicMock(
        name="DatabricksSession"
    )
    # DBUtils() is instantiated in functions_databricks.py
    sys.modules["pyspark.dbutils"].DBUtils = MagicMock(name="DBUtils")


_stub_databricks_runtime()


# ---------------------------------------------------------------------------
# 2. Session-scoped SparkSession — created once per pytest run
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Local SparkSession for all Spark-based tests.

    Configuration notes
    -------------------
    - ``local[2]``  — two threads; fast on CI runners, matches small cluster
    - shuffle partitions = 2 — avoids the 200-partition default on tiny datasets
    - Delta extensions registered — tests that write Delta tables work correctly
    - UI disabled — removes the /tmp/spark-events noise in CI logs
    """
    from pyspark.sql import SparkSession  # real pyspark, not the stub

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-databricks-central")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        # Delta Lake — required by GCOB_Consumer (write_to_unity_catalog uses delta)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Suppress verbose Spark output during test runs
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


# ---------------------------------------------------------------------------
# 3. Environment variable fixture — auto-applied to every test
# ---------------------------------------------------------------------------

#: Fake Azure / Databricks credentials injected for every test automatically.
#: Tests that need specific overrides can use ``monkeypatch.setenv()``.
_FAKE_ENV: dict[str, str] = {
    # Azure App Registration
    "APP_REG_APP_ID":               "test-app-reg-00000000-0000-0000-0000-000000000000",
    "TENANT_ID":                    "test-tenant-00000000-0000-0000-0000-000000000000",
    # Environment selector used in storage account names (e.g. saradardev)
    "ENV":                          "dev",
    # Azure Storage accounts
    "GDP_STORAGE_NAME":             "testgdpstorage",
    "GDP_SA_STORAGE_NAME":          "testgdpsastorage",
    "GDP_NA_STORAGE_NAME":          "testgdpnastorage",
    "GNS_STORAGE_ACCOUNT":          "testgnsstorage",
    "GNS_CONTAINER":                "testgnscontainer",
    "SALZReadStorage":              "testsalzstorage",
    "AU_GDP_Defined_Storage_Account": "testaugdpstorage",
    # Unity Catalog
    "CATALOG":                      "wr_fj_parties_and_risk_assessment_dev",
    "GCOB_UC_SCHEMA":               "gcobreportingv103",
    # Dataverse / Cosmos
    "DATAVERSE_URL":                "https://test-org.crm4.dynamics.com",
    "CosmosAccount":                "test-cosmos-account",
    "subscriptionId":               "test-subscription-id",
    "resourceGroup_API":            "test-resource-group",
    # Nexus (used in cluster env vars)
    "NEXUSCLOUD_USERNAME":          "test-nexus-user",
    "NEXUSCLOUD_PASSWORD":          "test-nexus-pass",  # noqa: S105  (fake credential)
}


@pytest.fixture(autouse=True)
def mock_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Automatically patch os.environ for every test with fake Azure credentials.

    Because this is ``autouse=True`` it applies to the whole suite — no
    individual test needs to request it.  A test can still override a specific
    key with::

        def test_something(monkeypatch):
            monkeypatch.setenv("ENV", "prod")
    """
    for key, value in _FAKE_ENV.items():
        monkeypatch.setenv(key, value)


# ---------------------------------------------------------------------------
# 4. mock_dbutils fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_dbutils() -> MagicMock:
    """
    A pre-configured MagicMock standing in for the Databricks ``dbutils`` object.

    Pre-wired return values match the most common call patterns in this codebase:

    - ``dbutils.secrets.get(scope=..., key=...)`` → ``"fake-secret-value"``
    - ``dbutils.fs.ls(path)``                     → ``[]``  (empty directory)
    - ``dbutils.fs.mkdirs(path)``                 → ``True``

    Inject into a test that exercises code paths using dbutils::

        def test_read_gdp(mock_dbutils, monkeypatch):
            monkeypatch.setattr("GcobUtils.dbutils", mock_dbutils)
            monkeypatch.setattr("GcobUtils.spark",   mock_spark)
    """
    dbutils = MagicMock(name="dbutils")
    dbutils.secrets.get.return_value = "fake-secret-value"   # noqa: S105
    dbutils.fs.ls.return_value       = []
    dbutils.fs.mkdirs.return_value   = True
    dbutils.fs.rm.return_value       = True
    return dbutils


# ---------------------------------------------------------------------------
# 5. make_df — convenience DataFrame factory
# ---------------------------------------------------------------------------

@pytest.fixture
def make_df(spark):
    """
    Factory fixture for creating small PySpark DataFrames from a list of dicts.

    Usage::

        def test_null_check(make_df):
            df = make_df([
                {"UniqueGcobId": "A1", "FullLegalName": "Acme Corp"},
                {"UniqueGcobId": "A2", "FullLegalName": None},
            ])
            assert df.filter("FullLegalName IS NULL").count() == 1

    The schema is inferred from the first row.  To enforce a specific schema,
    use ``spark.createDataFrame(data, schema=...)`` directly.
    """
    from pyspark.sql import Row

    def _factory(rows: list[dict]) -> "pyspark.sql.DataFrame":
        if not rows:
            raise ValueError("make_df requires at least one row")
        return spark.createDataFrame([Row(**r) for r in rows])

    return _factory


# ---------------------------------------------------------------------------
# 6. spark_with_delta — SparkSession that has Delta enabled (alias for clarity)
# ---------------------------------------------------------------------------

@pytest.fixture
def spark_with_delta(spark):
    """
    Alias for the session-scoped ``spark`` fixture.
    Use this name in tests that explicitly write / read Delta tables to make
    the intent clear in the test body.
    """
    return spark
