"""
GCOB_Consumer/tests/test_gcob_utils_unit.py
=============================================
Comprehensive unit tests for ALL functions in GcobUtils.py.

Each test case documents:
  - Input:       The exact arguments passed to the function
  - Expected Output: What the function should return or raise
  - Mocked:      Which external dependencies are mocked

Functions tested (9 total):
  1. get_load_date(source)                            - Pure unit
  2. determine_version(...)                           - Pure unit
  3. get_matching_partition(...)                      - Spark
  4. authenticate_storage_account(write_storage)       - Spark (mocked)
  5. write_to_unity_catalog(df, catalog, schema, table) - Spark
  6. get_graph_paginated_data(url, headers, timeout)   - Pure unit (mocks requests)
  7. get_group_members(groups, headers)               - Pure unit
  8. get_group_members_df(spark, groups, headers)      - Spark
  9. read_gdp_defined_dataobjects(...)                  - Integration (mocks all)
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest

from utils.df_helpers import (
    assert_column_subset, assert_no_nulls, assert_row_count,
    assert_schema_equal, assert_unique,
)
from fixtures.sample_dataframes import gcob_party_df

import GcobUtils  # noqa: E402


def _make_file_info(path: str) -> SimpleNamespace:
    name = path.rstrip("/").split("/")[-1] + "/"
    return SimpleNamespace(name=name, path=path)


# ===========================================================================
# 1. get_load_date
# ===========================================================================

class TestGetLoadDate:
    """Test get_load_date(source) -> str.

    Logic:
        GCOB/Legacy2/GIC/KN1/Siebel/RadarDataModel/GCDS -> today %Y%m%d
        NLSVF -> 2 days ago %Y-%m-%d (different format!)
        else -> ValueError
    """

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    @pytest.mark.parametrize("source", ["GCOB", "Legacy2", "GIC", "KN1", "Siebel", "RadarDataModel", "GCDS"])
    def test_known_sources_return_today_yyyymmdd(self, source: str) -> None:
        """Input: source in known list. Output: today as 'YYYYMMDD'. Mocked: none."""
        result = GcobUtils.get_load_date(source)
        expected = datetime.today().strftime("%Y%m%d")
        assert result == expected
        assert re.match(r"^\d{8}$", result)

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_nlsvf_returns_two_days_ago_dash_format(self) -> None:
        """Input: source='NLSVF'. Output: 2 days ago as 'YYYY-MM-DD'. Mocked: none."""
        result = GcobUtils.get_load_date("NLSVF")
        expected = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
        assert result == expected
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", result)

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_unknown_source_raises_value_error(self) -> None:
        """Input: source='UNKNOWN_SOURCE'. Output: ValueError 'Unknown source'. Mocked: none."""
        with pytest.raises(ValueError, match="Unknown source"):
            GcobUtils.get_load_date("UNKNOWN_SOURCE")

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    @pytest.mark.parametrize("source", ["gcob", "GCOB ", " GCOB", "gcds"])
    def test_wrong_case_or_whitespace_raises(self, source: str) -> None:
        """Input: source with wrong case or whitespace. Output: ValueError. Mocked: none."""
        with pytest.raises((ValueError, KeyError)):
            GcobUtils.get_load_date(source)

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_empty_string_raises(self) -> None:
        """Input: source=''. Output: ValueError. Mocked: none."""
        with pytest.raises(ValueError, match="Unknown source"):
            GcobUtils.get_load_date("")

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_none_raises(self) -> None:
        """Input: source=None. Output: TypeError. Mocked: none."""
        with pytest.raises((TypeError, ValueError)):
            GcobUtils.get_load_date(None)


# ===========================================================================
# 2. determine_version
# ===========================================================================

class TestDetermineVersion:
    """Test determine_version(source, dataobject, files, version_mapping_by_source, version_num) -> int."""

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_radar_datamodel_returns_version_num(self) -> None:
        """Input: source='RadarDataModel', version_num=3. Output: 3. Mocked: none."""
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
    def test_siebel_known_objects_resolve_version(self, dataobject: str, expected_version: int) -> None:
        """Input: source='Siebel', dataobject in mapping. Output: mapped version. Mocked: none."""
        mapping = {"Siebel": {3: ["cdf_ggm_rel_x_ar_hist"], 2: ["cdf_ggm_org_hist", "cdf_ggm_ar_hist", "cdf_ggm_rel_x_rel_hist", "cdf_ggm_np_hist"]}}
        result = GcobUtils.determine_version("Siebel", dataobject, [], mapping, version_num=1)
        assert result == expected_version

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_siebel_unknown_object_raises(self) -> None:
        """Input: source='Siebel', dataobject='ghost_table'. Output: ValueError. Mocked: none."""
        mapping = {"Siebel": {3: ["known_table"]}}
        with pytest.raises(ValueError, match="not found in version mapping"):
            GcobUtils.determine_version("Siebel", "ghost_table", [], mapping, version_num=1)

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_gcds_known_object_resolves(self) -> None:
        """Input: source='GCDS', dataobject='client_Client'. Output: 4602. Mocked: none."""
        mapping = {"GCDS": {4602: ["client_Client", "client_KeyStoreKey"]}}
        result = GcobUtils.determine_version("GCDS", "client_Client", [], mapping, version_num=1)
        assert result == 4602

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_non_mapped_source_uses_file_list_max(self) -> None:
        """Input: source='GCOB', files=[/100/, /101/, /data/]. Output: 101. Mocked: none."""
        files = [_make_file_info("/c/obj/100/"), _make_file_info("/c/obj/101/"), _make_file_info("/c/obj/data/")]
        result = GcobUtils.determine_version("GCOB", "some_object", files, {}, version_num=1)
        assert result == 101

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_radar_datamodel_ignores_mapping(self) -> None:
        """Input: source='RadarDataModel', mapping has conflict, version_num=5. Output: 5. Mocked: none."""
        mapping = {"RadarDataModel": {99: ["anything"]}}
        result = GcobUtils.determine_version("RadarDataModel", "anything", [], mapping, version_num=5)
        assert result == 5

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_file_list_with_single_version(self) -> None:
        """Input: source='GCOB', files=[/50/]. Output: 50. Mocked: none."""
        files = [_make_file_info("/c/obj/50/")]
        result = GcobUtils.determine_version("GCOB", "obj", files, {}, version_num=1)
        assert result == 50

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_empty_file_list_raises(self) -> None:
        """Input: source='GCOB', files=[]. Output: ValueError (max of empty). Mocked: none."""
        with pytest.raises(ValueError):
            GcobUtils.determine_version("GCOB", "obj", [], {}, version_num=1)


# ===========================================================================
# 3. get_matching_partition
# ===========================================================================

class TestGetMatchingPartition:
    """Test get_matching_partition(partition_folders, load_date, source, dataobject, base_path) -> str."""

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_finds_matching_partition(self, spark) -> None:
        """Input: folders=[EDL_LOAD_DTS=<today>/, _delta_log/], load_date=today. Output: folder name with today's date. Mocked: spark fixture."""
        load_date = datetime.today().strftime("%Y%m%d")
        folders = [_make_file_info(f"/base/EDL_LOAD_DTS={load_date}/"), _make_file_info("/base/_delta_log/")]
        result = GcobUtils.get_matching_partition(folders, load_date, "GCOB", "some_object", "/base")
        assert load_date in result

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_missing_partition_raises_runtime_error(self, spark) -> None:
        """Input: folders=[old date], load_date='99991231'. Output: RuntimeError 'Partition file not found'. Mocked: spark fixture."""
        folders = [_make_file_info("/base/EDL_LOAD_DTS=20200101/")]
        with pytest.raises(RuntimeError, match="Partition file not found"):
            GcobUtils.get_matching_partition(folders, "99991231", "GCOB", "some_object", "/base")

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_gcds_falls_back_to_previous_day(self, spark) -> None:
        """Input: folders=[yesterday only], load_date=today, source='GCDS'. Output: yesterday's folder. Mocked: spark fixture."""
        today = datetime.today()
        yesterday = (today - timedelta(days=1)).strftime("%Y%m%d")
        folders = [_make_file_info(f"/base/EDL_LOAD_DTS={yesterday}/")]
        result = GcobUtils.get_matching_partition(folders, today.strftime("%Y%m%d"), "GCDS", "client_Client", "/base")
        assert yesterday in result

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_non_gcds_no_fallback_raises(self, spark) -> None:
        """Input: folders=[yesterday only], source='GCOB'. Output: RuntimeError (no fallback for non-GCDS). Mocked: spark fixture."""
        today = datetime.today()
        yesterday = (today - timedelta(days=1)).strftime("%Y%m%d")
        folders = [_make_file_info(f"/base/EDL_LOAD_DTS={yesterday}/")]
        with pytest.raises(RuntimeError, match="Partition file not found"):
            GcobUtils.get_matching_partition(folders, today.strftime("%Y%m%d"), "GCOB", "obj", "/base")

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_delta_log_folder_is_ignored(self, spark) -> None:
        """Input: folders=[_delta_log/, EDL_LOAD_DTS=<today>/]. Output: EDL_LOAD_DTS folder (not _delta_log). Mocked: spark fixture."""
        load_date = datetime.today().strftime("%Y%m%d")
        folders = [_make_file_info("/base/_delta_log/"), _make_file_info(f"/base/EDL_LOAD_DTS={load_date}/")]
        result = GcobUtils.get_matching_partition(folders, load_date, "GCOB", "obj", "/base")
        assert "_delta_log" not in result
        assert load_date in result


# ===========================================================================
# 4. authenticate_storage_account
# ===========================================================================

class TestAuthenticateStorageAccount:
    """Test authenticate_storage_account(write_storage) -> None. Sets 5 Spark conf keys for OAuth."""

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_sets_five_spark_conf_keys(self) -> None:
        """Input: write_storage='saradard'. Output: spark.conf.set called 5 times. Mocked: GcobUtils.spark."""
        mock_spark = MagicMock()
        with patch.object(GcobUtils, "spark", mock_spark):
            GcobUtils.authenticate_storage_account("saradard")
        assert mock_spark.conf.set.call_count == 5
        set_keys = [c.args[0] for c in mock_spark.conf.set.call_args_list]
        assert any("auth.type" in k for k in set_keys)
        assert any("provider.type" in k for k in set_keys)
        assert any("client.id" in k for k in set_keys)
        assert any("client.secret" in k for k in set_keys)
        assert any("client.endpoint" in k for k in set_keys)

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_oauth_auth_type_used(self) -> None:
        """Input: write_storage='teststorage'. Output: auth.type value='OAuth'. Mocked: GcobUtils.spark."""
        mock_spark = MagicMock()
        with patch.object(GcobUtils, "spark", mock_spark):
            GcobUtils.authenticate_storage_account("teststorage")
        calls = mock_spark.conf.set.call_args_list
        auth_call = next(c for c in calls if "auth.type" in c.args[0])
        assert auth_call.args[1] == "OAuth"

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_storage_name_embedded_in_conf_keys(self) -> None:
        """Input: write_storage='mysaradarstorage'. Output: all 5 keys contain 'mysaradarstorage.dfs.core.windows.net'. Mocked: GcobUtils.spark."""
        mock_spark = MagicMock()
        with patch.object(GcobUtils, "spark", mock_spark):
            GcobUtils.authenticate_storage_account("mysaradarstorage")
        set_keys = [c.args[0] for c in mock_spark.conf.set.call_args_list]
        for key in set_keys:
            assert "mysaradarstorage.dfs.core.windows.net" in key

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_oauth_endpoint_contains_tenant_id(self) -> None:
        """Input: write_storage='teststorage'. Output: endpoint URL contains 'login.microsoftonline.com' and '/oauth2/token'. Mocked: GcobUtils.spark."""
        mock_spark = MagicMock()
        with patch.object(GcobUtils, "spark", mock_spark):
            GcobUtils.authenticate_storage_account("teststorage")
        calls = mock_spark.conf.set.call_args_list
        endpoint_call = next(c for c in calls if "client.endpoint" in c.args[0])
        endpoint_url = endpoint_call.args[1]
        assert "login.microsoftonline.com" in endpoint_url
        assert "oauth2/token" in endpoint_url


# ===========================================================================
# 5. write_to_unity_catalog
# ===========================================================================

class TestWriteToUnityCatalog:
    """Test write_to_unity_catalog(df, catalog, schema, table) -> None. Writes DataFrame to UC Delta table."""

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_raises_when_schema_does_not_exist(self, spark, gcob_party) -> None:
        """Input: df=gcob_party, catalog='nonexistent', schema='nonexistent'. Output: ValueError 'does not exist'. Mocked: none (real spark.catalog)."""
        with pytest.raises(ValueError, match="does not exist"):
            GcobUtils.write_to_unity_catalog(gcob_party, catalog="nonexistent_catalog", schema="nonexistent_schema", table="gcob_party")

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_writes_rows_to_delta_table(self, spark, tmp_path, gcob_party) -> None:
        """Input: df=gcob_party (3 rows), catalog='spark_catalog', schema='test_write_schema'. Output: table with 3 rows. Mocked: databaseExists=True, GcobUtils.spark."""
        schema = "test_write_schema"
        table = "gcob_party_out"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        with (patch.object(spark.catalog, "databaseExists", return_value=True), patch.object(GcobUtils, "spark", spark)):
            GcobUtils.write_to_unity_catalog(gcob_party, "spark_catalog", schema, table)
        result = spark.table(f"{schema}.{table}")
        assert_row_count(result, gcob_party.count())
        spark.sql(f"DROP TABLE IF EXISTS {schema}.{table}")
        spark.sql(f"DROP DATABASE IF EXISTS {schema}")

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_analysis_exception_treated_as_nonexistent(self, spark, gcob_party) -> None:
        """Input: df=gcob_party, catalog='uc_catalog', schema='nested.schema'. Output: ValueError (AnalysisException caught). Mocked: databaseExists raises AnalysisException."""
        from pyspark.sql.utils import AnalysisException
        with (patch.object(spark.catalog, "databaseExists", side_effect=AnalysisException("nested namespace")), patch.object(GcobUtils, "spark", spark)):
            with pytest.raises(ValueError, match="does not exist"):
                GcobUtils.write_to_unity_catalog(gcob_party, catalog="uc_catalog", schema="nested.schema", table="test")

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_write_uses_overwrite_mode_and_delta_format(self, spark, gcob_party) -> None:
        """Input: df=mock, catalog='spark_catalog', schema='test_write_fmt'. Output: write called with format('delta'), mode('overwrite'), option('overwriteSchema','true'). Mocked: databaseExists=True, df.write."""
        schema = "test_write_fmt"
        table = "test_tbl"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        mock_df = MagicMock()
        mock_df.count.return_value = 5
        with (patch.object(spark.catalog, "databaseExists", return_value=True), patch.object(GcobUtils, "spark", spark)):
            GcobUtils.write_to_unity_catalog(mock_df, "spark_catalog", schema, table)
        mock_df.write.format.assert_called_with("delta")
        mock_df.write.format().mode.assert_called_with("overwrite")
        mock_df.write.format().mode().option.assert_called_with("overwriteSchema", "true")
        spark.sql(f"DROP TABLE IF EXISTS {schema}.{table}")
        spark.sql(f"DROP DATABASE IF EXISTS {schema}")


# ===========================================================================
# 6. get_graph_paginated_data
# ===========================================================================

class TestGetGraphPaginatedData:
    """Test get_graph_paginated_data(url, headers, timeout=30) -> list[dict]. Fetches paginated Graph API data."""

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_single_page_returns_values(self) -> None:
        """Input: url, headers. Output: [{'id':'u1'},{'id':'u2'}]. Mocked: requests.get."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": [{"id": "u1"}, {"id": "u2"}]}
        mock_response.raise_for_status.return_value = None
        with patch("GcobUtils.requests.get", return_value=mock_response):
            result = GcobUtils.get_graph_paginated_data("https://graph.microsoft.com/v1.0/groups", {"Authorization": "Bearer fake"})
        assert result == [{"id": "u1"}, {"id": "u2"}]

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_pagination_follows_next_link(self) -> None:
        """Input: url='https://page1'. Output: 2 records from 2 pages. Mocked: requests.get side_effect."""
        page1 = MagicMock()
        page1.json.return_value = {"value": [{"id": "u1"}], "@odata.nextLink": "https://next-page"}
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
        """Input: url. Output: partial results [{'id':'u1'}]. Mocked: requests.get side_effect=[good, bad]."""
        import requests as req_lib
        good_page = MagicMock()
        good_page.json.return_value = {"value": [{"id": "u1"}], "@odata.nextLink": "https://bad-next"}
        good_page.raise_for_status.return_value = None
        bad_page = MagicMock()
        bad_page.raise_for_status.side_effect = req_lib.exceptions.RequestException("timeout")
        with patch("GcobUtils.requests.get", side_effect=[good_page, bad_page]):
            result = GcobUtils.get_graph_paginated_data("https://page1", {})
        assert result == [{"id": "u1"}]

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_empty_value_returns_empty_list(self) -> None:
        """Input: url. Output: [] (empty value). Mocked: requests.get returns {value:[]}."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        mock_response.raise_for_status.return_value = None
        with patch("GcobUtils.requests.get", return_value=mock_response):
            result = GcobUtils.get_graph_paginated_data("https://page1", {})
        assert result == []

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_missing_value_key_returns_empty_list(self) -> None:
        """Input: url. Output: [] (no 'value' key). Mocked: requests.get returns {other:...}."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"@odata.context": "some context"}
        mock_response.raise_for_status.return_value = None
        with patch("GcobUtils.requests.get", return_value=mock_response):
            result = GcobUtils.get_graph_paginated_data("https://page1", {})
        assert result == []

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_custom_timeout_passed_to_requests(self) -> None:
        """Input: url, timeout=60. Output: requests.get called with timeout=60. Mocked: requests.get."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        mock_response.raise_for_status.return_value = None
        with patch("GcobUtils.requests.get", return_value=mock_response) as mock_get:
            GcobUtils.get_graph_paginated_data("https://page1", {}, timeout=60)
        _, kwargs = mock_get.call_args
        assert kwargs.get("timeout") == 60

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_first_request_exception_returns_empty(self) -> None:
        """Input: url='https://bad-page'. Output: [] (first request fails). Mocked: requests.get raises."""
        import requests as req_lib
        with patch("GcobUtils.requests.get", side_effect=req_lib.exceptions.RequestException("connection refused")):
            result = GcobUtils.get_graph_paginated_data("https://bad-page", {})
        assert result == []


# ===========================================================================
# 7. get_group_members
# ===========================================================================

class TestGetGroupMembers:
    """Test get_group_members(groups, headers) -> list[dict]. Retrieves all members for all groups."""

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_returns_members_with_group_name(self) -> None:
        """Input: groups={'value':[{'id':'grp-001','displayName':'Admins'}]}. Output: [{'id':'usr-001','GroupName':'Admins'}]. Mocked: get_graph_paginated_data."""
        groups = {"value": [{"id": "grp-001", "displayName": "Admins"}]}
        with patch.object(GcobUtils, "get_graph_paginated_data", return_value=[{"id": "usr-001", "displayName": "Alice"}]):
            result = GcobUtils.get_group_members(groups, {})
        assert len(result) == 1
        assert result[0]["id"] == "usr-001"
        assert result[0]["GroupName"] == "Admins"

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_multiple_groups_aggregates_members(self) -> None:
        """Input: 2 groups. Output: 2 members with correct GroupName. Mocked: get_graph_paginated_data side_effect."""
        groups = {"value": [{"id": "grp-1", "displayName": "TeamA"}, {"id": "grp-2", "displayName": "TeamB"}]}
        with patch.object(GcobUtils, "get_graph_paginated_data", side_effect=[[{"id": "u1"}], [{"id": "u2"}]]):
            result = GcobUtils.get_group_members(groups, {})
        assert len(result) == 2
        assert result[0]["GroupName"] == "TeamA"
        assert result[1]["GroupName"] == "TeamB"

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_empty_groups_returns_empty_list(self) -> None:
        """Input: groups={'value':[]}. Output: []. Mocked: get_graph_paginated_data (not called)."""
        with patch.object(GcobUtils, "get_graph_paginated_data") as mock_paginated:
            result = GcobUtils.get_group_members({"value": []}, {})
        assert result == []
        mock_paginated.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_group_without_id_is_skipped(self) -> None:
        """Input: groups={'value':[{'displayName':'NoId'}]}. Output: [] (skipped). Mocked: get_graph_paginated_data."""
        with patch.object(GcobUtils, "get_graph_paginated_data") as mock_paginated:
            result = GcobUtils.get_group_members({"value": [{"displayName": "NoId"}]}, {})
        assert result == []
        mock_paginated.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_empty_member_list_returns_empty(self) -> None:
        """Input: 1 group, members=[]. Output: []. Mocked: get_graph_paginated_data returns []."""
        with patch.object(GcobUtils, "get_graph_paginated_data", return_value=[]):
            result = GcobUtils.get_group_members({"value": [{"id": "grp-1", "displayName": "Empty"}]}, {})
        assert result == []

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_correct_endpoint_called_per_group(self) -> None:
        """Input: groups={'value':[{'id':'grp-abc',...}]}. Output: URL contains 'grp-abc' and '$count=true'. Mocked: get_graph_paginated_data."""
        groups = {"value": [{"id": "grp-abc", "displayName": "Test"}]}
        with patch.object(GcobUtils, "get_graph_paginated_data", return_value=[]) as mock_paginated:
            GcobUtils.get_group_members(groups, {"Authorization": "Bearer x"})
        called_url = mock_paginated.call_args.kwargs.get("url") or mock_paginated.call_args.args[0]
        assert "grp-abc" in called_url
        assert "$count=true" in called_url


# ===========================================================================
# 8. get_group_members_df
# ===========================================================================

class TestGetGroupMembersDF:
    """Test get_group_members_df(spark, groups, headers) -> DataFrame. Converts group members to Spark DataFrame."""

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_returns_dataframe_with_expected_columns(self, spark) -> None:
        """Input: spark, groups with 2 groups. Output: DataFrame with columns id, displayName, userPrincipalName, mail, GroupName. Mocked: get_group_members."""
        fake_members = [{"id": "usr-001", "displayName": "Alice", "userPrincipalName": "alice@rabobank.com", "mail": "alice@rabobank.com", "GroupName": "AnalyticsEngineerAdmin"}]
        with patch.object(GcobUtils, "get_group_members", return_value=fake_members):
            df = GcobUtils.get_group_members_df(spark, {"value": []}, {"Authorization": "Bearer fake"})
        assert_column_subset(df, ["id", "displayName", "userPrincipalName", "mail", "GroupName"])
        assert_row_count(df, 1)

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_empty_groups_returns_empty_dataframe(self, spark) -> None:
        """Input: spark, groups={'value':[]}. Output: Empty DataFrame (0 rows) with correct schema. Mocked: get_group_members returns []."""
        with patch.object(GcobUtils, "get_group_members", return_value=[]):
            df = GcobUtils.get_group_members_df(spark, {"value": []}, {})
        assert df.count() == 0
        assert_column_subset(df, ["id", "displayName", "userPrincipalName", "mail", "GroupName"])

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_multiple_members_preserve_group_name(self, spark) -> None:
        """Input: spark, 2 members from different groups. Output: 2 rows with correct GroupName each. Mocked: get_group_members."""
        fake_members = [
            {"id": "u1", "displayName": "A", "userPrincipalName": "a@r.com", "mail": "a@r.com", "GroupName": "TeamA"},
            {"id": "u2", "displayName": "B", "userPrincipalName": "b@r.com", "mail": "b@r.com", "GroupName": "TeamB"},
        ]
        with patch.object(GcobUtils, "get_group_members", return_value=fake_members):
            df = GcobUtils.get_group_members_df(spark, {"value": []}, {})
        assert_row_count(df, 2)
        rows = df.collect()
        group_names = {row["GroupName"] for row in rows}
        assert group_names == {"TeamA", "TeamB"}


# ===========================================================================
# 9. read_gdp_defined_dataobjects (Integration — mocks dbutils, spark)
# ===========================================================================

class TestReadGdpDefinedDataobjects:
    """Test read_gdp_defined_dataobjects(source, dataobject, path_prefix='', version_num=3). Orchestrates full read pipeline."""

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_unknown_source_raises_value_error(self) -> None:
        """Input: source='UNKNOWN_SOURCE', dataobject='obj'. Output: RuntimeError/ValueError. Mocked: os.environ (via conftest)."""
        with pytest.raises((RuntimeError, ValueError)):
            GcobUtils.read_gdp_defined_dataobjects("UNKNOWN_SOURCE", "obj")

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_gcob_source_calls_dbutils_fs_ls_twice(self) -> None:
        """Input: source='GCOB', dataobject='party_AllPartyDetails', version_num=103. Output: dbutils.fs.ls called 2 times. Mocked: dbutils, spark, get_load_date, determine_version, get_matching_partition."""
        mock_dbutils = MagicMock()
        load_date = datetime.today().strftime("%Y%m%d")
        mock_dbutils.fs.ls.side_effect = [
            [_make_file_info("/gcob/party_AllPartyDetails/103/")],
            [_make_file_info(f"/gcob/party_AllPartyDetails/103/data/EDL_LOAD_DTS={load_date}/")],
        ]
        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.return_value.createOrReplaceGlobalTempView.return_value = None
        mock_spark.sql.return_value.collect.return_value = [MagicMock()]
        mock_spark.sql.return_value.collect.return_value[0].__getitem__ = lambda self, key: 0
        with (patch.object(GcobUtils, "dbutils", mock_dbutils), patch.object(GcobUtils, "spark", mock_spark), patch.object(GcobUtils, "get_load_date", return_value=load_date), patch.object(GcobUtils, "determine_version", return_value=103), patch.object(GcobUtils, "get_matching_partition", return_value=f"EDL_LOAD_DTS={load_date}/")):
            GcobUtils.read_gdp_defined_dataobjects("GCOB", "party_AllPartyDetails", version_num=103)
        assert mock_dbutils.fs.ls.call_count == 2

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_siebel_source_reads_delta_format(self) -> None:
        """Input: source='Siebel', dataobject='cdf_ggm_org_hist', version_num=2. Output: spark.read.format('delta') used. Mocked: dbutils, spark, helpers."""
        mock_dbutils = MagicMock()
        load_date = datetime.today().strftime("%Y%m%d")
        mock_dbutils.fs.ls.side_effect = [
            [_make_file_info("/siebel/cdf_ggm_org_hist/2/")],
            [_make_file_info(f"/siebel/cdf_ggm_org_hist/2/data/EDL_LOAD_DTS={load_date}/")],
        ]
        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.return_value.createOrReplaceGlobalTempView.return_value = None
        mock_spark.sql.return_value.collect.return_value = [MagicMock()]
        mock_spark.sql.return_value.collect.return_value[0].__getitem__ = lambda self, key: 42
        with (patch.object(GcobUtils, "dbutils", mock_dbutils), patch.object(GcobUtils, "spark", mock_spark), patch.object(GcobUtils, "get_load_date", return_value=load_date), patch.object(GcobUtils, "determine_version", return_value=2), patch.object(GcobUtils, "get_matching_partition", return_value=f"EDL_LOAD_DTS={load_date}/")):
            GcobUtils.read_gdp_defined_dataobjects("Siebel", "cdf_ggm_org_hist", version_num=2)
        mock_spark.read.format.assert_called_with("delta")

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_non_delta_source_reads_parquet_format(self) -> None:
        """Input: source='GIC', dataobject='some_object', version_num=1. Output: spark.read.parquet() used. Mocked: dbutils, spark, helpers."""
        mock_dbutils = MagicMock()
        load_date = datetime.today().strftime("%Y%m%d")
        mock_dbutils.fs.ls.side_effect = [
            [_make_file_info("/gic/some_object/1/")],
            [_make_file_info(f"/gic/some_object/1/data/EDL_LOAD_DTS={load_date}/")],
        ]
        mock_spark = MagicMock()
        mock_spark.read.parquet.return_value.createOrReplaceGlobalTempView.return_value = None
        mock_spark.sql.return_value.collect.return_value = [MagicMock()]
        mock_spark.sql.return_value.collect.return_value[0].__getitem__ = lambda self, key: 10
        with (patch.object(GcobUtils, "dbutils", mock_dbutils), patch.object(GcobUtils, "spark", mock_spark), patch.object(GcobUtils, "get_load_date", return_value=load_date), patch.object(GcobUtils, "determine_version", return_value=1), patch.object(GcobUtils, "get_matching_partition", return_value=f"EDL_LOAD_DTS={load_date}/")):
            GcobUtils.read_gdp_defined_dataobjects("GIC", "some_object", version_num=1)
        mock_spark.read.parquet.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_dbutils_error_wrapped_as_runtime_error(self) -> None:
        """Input: source='GCOB', dbutils.fs.ls raises Exception. Output: RuntimeError 'Error reading'. Mocked: dbutils.fs.ls raises, spark."""
        mock_dbutils = MagicMock()
        mock_dbutils.fs.ls.side_effect = Exception("connection failed")
        with (patch.object(GcobUtils, "dbutils", mock_dbutils), patch.object(GcobUtils, "spark", MagicMock())):
            with pytest.raises(RuntimeError, match="Error reading"):
                GcobUtils.read_gdp_defined_dataobjects("GCOB", "obj")


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture
def gcob_party(spark):
    """Input: spark session. Output: 3-row GCOB party DataFrame from fixtures.sample_dataframes."""
    return gcob_party_df(spark)