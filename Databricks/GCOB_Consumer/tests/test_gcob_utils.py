"""
GCOB_Consumer/tests/test_gcob_utils.py
========================================
Unit and Spark tests for GcobUtils.py.

Covers
------
Unit (no Spark, no Azure):
  - get_load_date()          : date logic per source system
  - determine_version()      : version resolution from mapping / file list
  - get_matching_partition() : partition folder matching by date

Spark:
  - write_to_unity_catalog() : Delta overwrite path (schema validation)
  - get_group_members_df()   : Graph API response → Spark DataFrame

Mocked boundaries:
  - dbutils.secrets.get  → "fake-secret-value"
  - dbutils.fs.ls        → controlled list of FileInfo-like objects
  - os.environ           → _FAKE_ENV from conftest.py (autouse)
  - spark (module-level) → replaced with the real local SparkSession via monkeypatch
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Import helpers from the central shared layer
# ---------------------------------------------------------------------------
from utils.df_helpers import (
    assert_column_subset,
    assert_no_nulls,
    assert_row_count,
    assert_schema_equal,
    assert_unique,
)
from fixtures.sample_dataframes import gcob_party_df


# ---------------------------------------------------------------------------
# GcobUtils imports
# NOTE: conftest.py stubs `databricks.sdk.runtime` before this file is loaded,
# so the module-level `from databricks.sdk.runtime import dbutils, spark` in
# GcobUtils resolves to MagicMocks without raising ImportError.
# ---------------------------------------------------------------------------
import GcobUtils  # noqa: E402  (must come after stub setup)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_file_info(path: str) -> SimpleNamespace:
    """Simulate a dbutils.fs FileInfo object with .name and .path attributes."""
    name = path.rstrip("/").split("/")[-1] + "/"
    return SimpleNamespace(name=name, path=path)


# ===========================================================================
# get_load_date
# ===========================================================================

class TestGetLoadDate:

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    @pytest.mark.parametrize("source", ["GCOB", "Legacy2", "GIC", "KN1", "Siebel", "RadarDataModel", "GCDS"])
    def test_known_sources_return_today(self, source: str) -> None:
        """All sources except NLSVF should return today's date in yyyymmdd."""
        result = GcobUtils.get_load_date(source)
        assert result == datetime.today().strftime("%Y%m%d"), (
            f"Expected today for source={source!r}, got {result!r}"
        )

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_nlsvf_returns_two_days_ago(self) -> None:
        expected = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
        assert GcobUtils.get_load_date("NLSVF") == expected

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_unknown_source_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown source"):
            GcobUtils.get_load_date("UNKNOWN_SOURCE")

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    @pytest.mark.parametrize("source", ["gcob", "GCOB ", " GCOB", "gcds"])
    def test_wrong_case_or_whitespace_raises(self, source: str) -> None:
        """Source names are case-sensitive; wrong case must raise."""
        with pytest.raises((ValueError, KeyError)):
            GcobUtils.get_load_date(source)


# ===========================================================================
# determine_version
# ===========================================================================

class TestDetermineVersion:

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_radar_datamodel_returns_version_num(self) -> None:
        result = GcobUtils.determine_version("RadarDataModel", "any_object", [], {}, version_num=3)
        assert result == 3

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    @pytest.mark.parametrize("dataobject,expected_version", [
        ("cdf_ggm_rel_x_ar_hist", 3),
        ("cdf_ggm_org_hist",       2),
        ("cdf_ggm_ar_hist",        2),
        ("cdf_ggm_np_hist",        2),
    ])
    def test_siebel_known_objects_resolve_version(
        self, dataobject: str, expected_version: int
    ) -> None:
        mapping = {
            "Siebel": {
                3: ["cdf_ggm_rel_x_ar_hist"],
                2: ["cdf_ggm_org_hist", "cdf_ggm_ar_hist", "cdf_ggm_rel_x_rel_hist", "cdf_ggm_np_hist"],
            }
        }
        result = GcobUtils.determine_version("Siebel", dataobject, [], mapping, version_num=1)
        assert result == expected_version

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_siebel_unknown_object_raises(self) -> None:
        mapping = {"Siebel": {3: ["known_table"]}}
        with pytest.raises(ValueError, match="not found in version mapping"):
            GcobUtils.determine_version("Siebel", "ghost_table", [], mapping, version_num=1)

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_gcds_known_object_resolves(self) -> None:
        mapping = {
            "GCDS": {4602: ["client_Client", "client_KeyStoreKey"]}
        }
        result = GcobUtils.determine_version("GCDS", "client_Client", [], mapping, version_num=1)
        assert result == 4602

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_non_mapped_source_uses_file_list_max(self) -> None:
        """Sources not in the version mapping pick the highest numeric folder from files."""
        files = [
            _make_file_info("/container/obj/100/"),
            _make_file_info("/container/obj/101/"),
            _make_file_info("/container/obj/data/"),   # non-numeric, must be ignored
        ]
        result = GcobUtils.determine_version("GCOB", "some_object", files, {}, version_num=1)
        assert result == 101

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_radar_datamodel_ignores_mapping(self) -> None:
        """RadarDataModel short-circuits before the mapping is consulted."""
        mapping = {"RadarDataModel": {99: ["anything"]}}
        result = GcobUtils.determine_version("RadarDataModel", "anything", [], mapping, version_num=5)
        assert result == 5


# ===========================================================================
# get_matching_partition
# ===========================================================================

class TestGetMatchingPartition:

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_finds_matching_partition(self, spark) -> None:
        load_date = datetime.today().strftime("%Y%m%d")
        folders = [
            _make_file_info(f"/base/EDL_LOAD_DTS={load_date}/"),
            _make_file_info("/base/_delta_log/"),
        ]
        result = GcobUtils.get_matching_partition(
            folders, load_date, "GCOB", "some_object", "/base"
        )
        assert load_date in result

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_missing_partition_raises_runtime_error(self, spark) -> None:
        folders = [_make_file_info("/base/EDL_LOAD_DTS=20200101/")]
        with pytest.raises(RuntimeError, match="Partition file not found"):
            GcobUtils.get_matching_partition(
                folders, "99991231", "GCOB", "some_object", "/base"
            )

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_gcds_falls_back_to_previous_day(self, spark) -> None:
        """GCDS-specific: if today's partition is absent, fall back to yesterday."""
        today     = datetime.today()
        yesterday = (today - timedelta(days=1)).strftime("%Y%m%d")
        folders   = [_make_file_info(f"/base/EDL_LOAD_DTS={yesterday}/")]

        result = GcobUtils.get_matching_partition(
            folders, today.strftime("%Y%m%d"), "GCDS", "client_Client", "/base"
        )
        assert yesterday in result


# ===========================================================================
# write_to_unity_catalog  (Spark test)
# ===========================================================================

class TestWriteToUnityCatalog:

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_raises_when_schema_does_not_exist(self, spark, gcob_party) -> None:
        """
        write_to_unity_catalog must raise ValueError when the target schema
        does not exist in the local test SparkSession's catalog.
        """
        with pytest.raises(ValueError, match="does not exist"):
            GcobUtils.write_to_unity_catalog(
                gcob_party,
                catalog="nonexistent_catalog",
                schema="nonexistent_schema",
                table="gcob_party",
            )

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_writes_rows_to_delta_table(self, spark, tmp_path, gcob_party) -> None:
        """
        Writes a small DataFrame via write_to_unity_catalog and verifies
        the row count matches input.  Uses a real local Delta write.
        """
        # Create the database in the local metastore so the schema exists
        catalog = "spark_catalog"
        schema  = "test_write_schema"
        table   = "gcob_party_out"

        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")

        # Patch spark.catalog.databaseExists so the guard passes without a
        # real Unity Catalog; also patch the module-level spark reference.
        with (
            patch.object(
                spark.catalog, "databaseExists", return_value=True
            ),
            patch.object(GcobUtils, "spark", spark),
        ):
            GcobUtils.write_to_unity_catalog(gcob_party, catalog, schema, table)

        result = spark.table(f"{schema}.{table}")
        assert_row_count(result, gcob_party.count())

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {schema}.{table}")
        spark.sql(f"DROP DATABASE IF EXISTS {schema}")


# ===========================================================================
# get_group_members_df  (Spark test — tests Graph API → DataFrame conversion)
# ===========================================================================

class TestGetGroupMembersDF:

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_returns_dataframe_with_expected_columns(self, spark) -> None:
        groups = {
            "value": [
                {"id": "grp-001", "displayName": "AnalyticsEngineerAdmin"},
                {"id": "grp-002", "displayName": "AnalyticsEngineerWrTribe"},
            ]
        }
        headers = {"Authorization": "Bearer fake-token"}

        fake_members = [
            {
                "id": "usr-001",
                "displayName": "Alice",
                "userPrincipalName": "alice@rabobank.com",
                "mail": "alice@rabobank.com",
                "GroupName": "AnalyticsEngineerAdmin",
            }
        ]

        with patch.object(GcobUtils, "get_group_members", return_value=fake_members):
            df = GcobUtils.get_group_members_df(spark, groups, headers)

        assert_column_subset(df, ["id", "displayName", "userPrincipalName", "mail", "GroupName"])
        assert_row_count(df, 1)

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_empty_groups_returns_empty_dataframe(self, spark) -> None:
        with patch.object(GcobUtils, "get_group_members", return_value=[]):
            df = GcobUtils.get_group_members_df(spark, {"value": []}, {})

        assert df.count() == 0


# ===========================================================================
# get_graph_paginated_data  (unit — no Spark, mocks requests)
# ===========================================================================

class TestGetGraphPaginatedData:

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_single_page_returns_values(self) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": [{"id": "u1"}, {"id": "u2"}]}
        mock_response.raise_for_status.return_value = None

        with patch("GcobUtils.requests.get", return_value=mock_response):
            result = GcobUtils.get_graph_paginated_data(
                "https://graph.microsoft.com/v1.0/groups",
                {"Authorization": "Bearer fake"},
            )

        assert result == [{"id": "u1"}, {"id": "u2"}]

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_pagination_follows_next_link(self) -> None:
        page1 = MagicMock()
        page1.json.return_value = {
            "value": [{"id": "u1"}],
            "@odata.nextLink": "https://next-page",
        }
        page1.raise_for_status.return_value = None

        page2 = MagicMock()
        page2.json.return_value = {"value": [{"id": "u2"}]}
        page2.raise_for_status.return_value = None

        with patch("GcobUtils.requests.get", side_effect=[page1, page2]):
            result = GcobUtils.get_graph_paginated_data("https://page1", {})

        assert len(result) == 2
        assert result[0]["id"] == "u1"
        assert result[1]["id"] == "u2"

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_request_exception_returns_partial_results(self) -> None:
        import requests as req_lib

        good_page = MagicMock()
        good_page.json.return_value = {
            "value": [{"id": "u1"}],
            "@odata.nextLink": "https://bad-next",
        }
        good_page.raise_for_status.return_value = None

        bad_page = MagicMock()
        bad_page.raise_for_status.side_effect = req_lib.exceptions.RequestException("timeout")

        with patch("GcobUtils.requests.get", side_effect=[good_page, bad_page]):
            result = GcobUtils.get_graph_paginated_data("https://page1", {})

        # Should return whatever was collected before the error
        assert result == [{"id": "u1"}]


# ===========================================================================
# Fixtures local to this module
# ===========================================================================

@pytest.fixture
def gcob_party(spark):
    """Small GCOB party DataFrame for write tests."""
    from fixtures.sample_dataframes import gcob_party_df
    return gcob_party_df(spark)
