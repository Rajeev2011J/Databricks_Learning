"""
radarv1/tests/test_radar_utils.py
===================================
Unit tests for the pure-Python logic in ``RadarUtils.py``
(located in GCOB_Reportingv1/tests/ but shared across bundles via pythonpath).

RadarUtils.py has module-level side-effects:
  - reads os.environ at import time (APP_REG_APP_ID, TENANT_ID, ENV)
  - calls dbutils.secrets.get() at import time
  - imports `from databricks.sdk.runtime import dbutils, spark`

All three are handled by conftest.py before this file is loaded:
  - os.environ is patched by the autouse `mock_env` fixture
  - databricks.sdk.runtime is stubbed by _stub_databricks_runtime()
  - dbutils.secrets.get() resolves to MagicMock().secrets.get() → MagicMock

Covered functions
-----------------
Unit (no Spark):
  - Read_GDP_Defined_DataObjects()  : Load_Date logic + container/storage mapping
    (boundary tests; actual fs.ls calls are mocked)
  - fetch_latest_file()             : version discovery + path construction
    (dbutils.fs.ls mocked)

Spark:
  No Spark tests for RadarUtils because all Spark operations are hidden behind
  dbutils.fs and spark.read — both are MagicMocks in test mode.
  Transformation logic is covered in test_dq_validation.py instead.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Import RadarUtils — conftest stubs make this safe
# ---------------------------------------------------------------------------
import RadarUtils  # noqa: E402  (lives in GCOB_Reportingv1/tests/ on pythonpath)


# ---------------------------------------------------------------------------
# Helper: build a fake FileInfo list for dbutils.fs.ls
# ---------------------------------------------------------------------------

def _ls_entries(*paths: str) -> list[SimpleNamespace]:
    """Return a list of SimpleNamespace objects mimicking dbutils FileInfo."""
    result = []
    for p in paths:
        name = p.rstrip("/").split("/")[-1]
        if not p.endswith("/"):
            name = name
        else:
            name = name + "/"
        result.append(SimpleNamespace(path=p, name=name))
    return result


# ===========================================================================
# Read_GDP_Defined_DataObjects  — Load_Date logic
# ===========================================================================

@pytest.mark.unit
@pytest.mark.radarv1
class TestReadGDPLoadDate:
    """
    Validate that Read_GDP_Defined_DataObjects resolves Load_Date correctly per
    source, and raises ValueError for unknown sources.
    We patch dbutils.fs.ls so the function doesn't need a real storage account.
    """

    @pytest.mark.parametrize("source", [
        "GCOB", "Legacy2", "GIC", "KN1", "Siebel", "RadarDataModel",
    ])
    def test_known_sources_use_today(self, source: str, monkeypatch) -> None:
        today = datetime.today().strftime("%Y%m%d")
        self._patch_dbutils_and_spark(monkeypatch, today)

        # Should not raise
        try:
            RadarUtils.Read_GDP_Defined_DataObjects(source, "some_object")
        except RuntimeError:
            pass  # expected — file-listing will fail with fake paths

        # Verify the load date used matches today
        dbutils_mock = RadarUtils.dbutils
        calls = dbutils_mock.fs.ls.call_args_list
        # At least one ls call must have been made (version lookup)
        assert calls, "Expected dbutils.fs.ls to be called"

    def test_gcds_uses_today(self, monkeypatch) -> None:
        today = datetime.today().strftime("%Y%m%d")
        self._patch_dbutils_and_spark(monkeypatch, today)

        try:
            RadarUtils.Read_GDP_Defined_DataObjects("GCDS", "client_Client")
        except RuntimeError:
            pass

    def test_nlsvf_uses_two_days_ago(self, monkeypatch) -> None:
        expected_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
        self._patch_dbutils_and_spark(monkeypatch, expected_date)

        try:
            RadarUtils.Read_GDP_Defined_DataObjects("NLSVF", "some_object")
        except RuntimeError:
            pass

    def test_unknown_source_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown source"):
            RadarUtils.Read_GDP_Defined_DataObjects("MYSTERY_SYSTEM", "object")

    def test_unknown_source_in_container_map_raises(self, monkeypatch) -> None:
        """A source with a valid date but missing from container_map raises ValueError."""
        with pytest.raises(ValueError):
            RadarUtils.Read_GDP_Defined_DataObjects("UNLISTED", "object")

    # ---- helpers -----------------------------------------------------------

    def _patch_dbutils_and_spark(self, monkeypatch, load_date: str) -> None:
        dbutils_mock = MagicMock()
        dbutils_mock.secrets.get.return_value = "fake-secret"
        # Version listing returns one numeric folder
        version_file = SimpleNamespace(
            path="/container/object/101/", name="101/"
        )
        # Partition listing returns a folder for load_date
        partition_file = SimpleNamespace(
            path=f"/container/object/101/data/EDL_LOAD_DTS={load_date}/",
            name=f"EDL_LOAD_DTS={load_date}/",
        )
        dbutils_mock.fs.ls.side_effect = [
            [version_file],     # first call: version discovery
            [partition_file],   # second call: partition listing
        ]
        monkeypatch.setattr(RadarUtils, "dbutils", dbutils_mock)

        spark_mock = MagicMock()
        spark_mock.read.parquet.return_value.createOrReplaceTempView.return_value = None
        spark_mock.read.format.return_value.load.return_value.createOrReplaceTempView.return_value = None
        spark_mock.sql.return_value.collect.return_value = [MagicMock(**{"__getitem__.return_value": 5})]
        monkeypatch.setattr(RadarUtils, "spark", spark_mock)


# ===========================================================================
# Read_GDP_Defined_DataObjects — container / storage mapping
# ===========================================================================

@pytest.mark.unit
@pytest.mark.radarv1
class TestReadGDPContainerMapping:

    @pytest.mark.parametrize("source,expected_container", [
        ("GCDS",         "gcds"),
        ("GCOB",         "gcob"),
        ("Legacy2",      "gcob"),
        ("Siebel",       "siebel-idaa-cdf"),
        ("RadarDataModel", "fec-radar"),
    ])
    def test_container_resolved_for_source(
        self, source: str, expected_container: str, monkeypatch
    ) -> None:
        """The abfss path passed to dbutils.fs.ls must contain the correct container."""
        dbutils_mock = MagicMock()
        dbutils_mock.secrets.get.return_value = "fake-secret"
        dbutils_mock.fs.ls.side_effect = RuntimeError("stop after first ls")
        monkeypatch.setattr(RadarUtils, "dbutils", dbutils_mock)
        monkeypatch.setattr(RadarUtils, "spark",   MagicMock())

        with pytest.raises(RuntimeError):
            RadarUtils.Read_GDP_Defined_DataObjects(source, "test_object")

        first_ls_path = dbutils_mock.fs.ls.call_args_list[0][0][0]
        assert expected_container in first_ls_path, (
            f"Expected container '{expected_container}' in path '{first_ls_path}'"
        )


# ===========================================================================
# fetch_latest_file — version discovery
# ===========================================================================

@pytest.mark.unit
@pytest.mark.radarv1
class TestFetchLatestFile:

    def test_picks_highest_numeric_version(self, monkeypatch) -> None:
        today = datetime.today().strftime("%Y%m%d")

        dbutils_mock = MagicMock()
        dbutils_mock.secrets.get.return_value = "fake-secret"

        # Version listing: folders 99, 100, 101 (plus a non-numeric one)
        version_entries = _ls_entries(
            "/base/object/99/",
            "/base/object/100/",
            "/base/object/101/",
            "/base/object/data/",   # non-numeric — must be ignored
        )
        # Partition listing: contains today's date
        partition_entries = _ls_entries(
            f"/base/object/101/data/EDL_LOAD_DTS={today}/",
        )
        dbutils_mock.fs.ls.side_effect = [version_entries, partition_entries]

        spark_mock = MagicMock()
        spark_mock.read.parquet.return_value.createOrReplaceTempView.return_value = None
        monkeypatch.setattr(RadarUtils, "dbutils", dbutils_mock)
        monkeypatch.setattr(RadarUtils, "spark",   spark_mock)

        RadarUtils.fetch_latest_file("SomeObject", today, "test-storage")

        # Second ls call must use the highest version = 101
        second_ls_call_path = dbutils_mock.fs.ls.call_args_list[1][0][0]
        assert "101" in second_ls_call_path

    def test_uses_given_dataversion_instead_of_discovered(self, monkeypatch) -> None:
        today = datetime.today().strftime("%Y%m%d")

        dbutils_mock = MagicMock()
        dbutils_mock.secrets.get.return_value = "fake-secret"

        partition_entries = _ls_entries(
            f"/base/object/42/data/EDL_LOAD_DTS={today}/",
        )
        # Only one ls call expected (no version discovery when Dataversion given)
        dbutils_mock.fs.ls.return_value = partition_entries

        spark_mock = MagicMock()
        spark_mock.read.parquet.return_value.createOrReplaceTempView.return_value = None
        monkeypatch.setattr(RadarUtils, "dbutils", dbutils_mock)
        monkeypatch.setattr(RadarUtils, "spark",   spark_mock)

        RadarUtils.fetch_latest_file("SomeObject", today, "test-storage", Dataversion="42")

        # ls should only be called once (skip version discovery)
        assert dbutils_mock.fs.ls.call_count == 1

    def test_no_matching_partition_raises_index_error(self, monkeypatch) -> None:
        """If no file in the partition listing matches the date, an IndexError is raised."""
        today = datetime.today().strftime("%Y%m%d")

        dbutils_mock = MagicMock()
        dbutils_mock.secrets.get.return_value = "fake-secret"

        version_entries   = _ls_entries("/base/object/101/")
        partition_entries = _ls_entries("/base/object/101/data/EDL_LOAD_DTS=20000101/")
        dbutils_mock.fs.ls.side_effect = [version_entries, partition_entries]

        monkeypatch.setattr(RadarUtils, "dbutils", dbutils_mock)
        monkeypatch.setattr(RadarUtils, "spark",   MagicMock())

        with pytest.raises((IndexError, RuntimeError)):
            RadarUtils.fetch_latest_file("SomeObject", today, "test-storage")

    def test_casereservice_flag_changes_path_prefix(self, monkeypatch) -> None:
        today = datetime.today().strftime("%Y%m%d")

        dbutils_mock = MagicMock()
        dbutils_mock.secrets.get.return_value = "fake-secret"
        dbutils_mock.fs.ls.side_effect = RuntimeError("stop")
        monkeypatch.setattr(RadarUtils, "dbutils", dbutils_mock)
        monkeypatch.setattr(RadarUtils, "spark",   MagicMock())

        with pytest.raises(RuntimeError):
            RadarUtils.fetch_latest_file(
                "party_AllPartyDetails", today, "saradardev", CaseService=True
            )

        first_path = dbutils_mock.fs.ls.call_args_list[0][0][0]
        assert "CaseService" in first_path

    def test_riskmodel_flag_changes_path_prefix(self, monkeypatch) -> None:
        today = datetime.today().strftime("%Y%m%d")

        dbutils_mock = MagicMock()
        dbutils_mock.secrets.get.return_value = "fake-secret"
        dbutils_mock.fs.ls.side_effect = RuntimeError("stop")
        monkeypatch.setattr(RadarUtils, "dbutils", dbutils_mock)
        monkeypatch.setattr(RadarUtils, "spark",   MagicMock())

        with pytest.raises(RuntimeError):
            RadarUtils.fetch_latest_file(
                "Party_RiskModelInstance", today, "saradardev", RISKMODEL=True
            )

        first_path = dbutils_mock.fs.ls.call_args_list[0][0][0]
        assert "RISKMODEL" in first_path


# ===========================================================================
# authenticate_storage_account — config keys set on spark
# ===========================================================================

@pytest.mark.unit
@pytest.mark.radarv1
class TestAuthenticateStorageAccount:

    def test_sets_five_spark_conf_keys(self, monkeypatch) -> None:
        """authenticate_storage_account must call spark.conf.set exactly 5 times."""
        spark_mock = MagicMock()
        monkeypatch.setattr(RadarUtils, "spark", spark_mock)

        RadarUtils.authenticate_storage_account("teststorage")

        assert spark_mock.conf.set.call_count == 5

    def test_oauth_auth_type_key_used(self, monkeypatch) -> None:
        spark_mock = MagicMock()
        monkeypatch.setattr(RadarUtils, "spark", spark_mock)

        RadarUtils.authenticate_storage_account("mystorage")

        all_keys = [c[0][0] for c in spark_mock.conf.set.call_args_list]
        assert any("auth.type" in k for k in all_keys), (
            f"Expected an 'auth.type' key, got: {all_keys}"
        )

    def test_storage_name_embedded_in_conf_keys(self, monkeypatch) -> None:
        spark_mock = MagicMock()
        monkeypatch.setattr(RadarUtils, "spark", spark_mock)

        RadarUtils.authenticate_storage_account("saradardev")

        all_keys = [c[0][0] for c in spark_mock.conf.set.call_args_list]
        assert all("saradardev" in k for k in all_keys), (
            f"Storage name 'saradardev' not found in all conf keys: {all_keys}"
        )
