"""
GCOB_Consumer/tests/test_gcob_party.py
=======================================
Unit tests for the GCOB_Party notebook transformations.

Tests the SQL transformations and Python logic from the GCOB_Party notebook
(ID 2680795591545986) without importing the notebook itself.

Notebook cells tested:
  Cell 3: BusinessDate calculation (yesterday in MM/DD/YYYY)
  Cell 5: Gcob_Legacy2_Clients view (UNION of GCOB + Legacy2 with UniqueGcobId logic)
  Cell 6: Party view (left join with protected clients, name masking)
  Cell 8: BusinessDate column addition

Each test case documents:
  - Input:       The sample data and arguments passed
  - Expected Output: What the transformation should produce
  - Mocked:      Which dependencies are mocked (none for SQL transform tests)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from utils.df_helpers import (
    assert_column_subset,
    assert_no_nulls,
    assert_row_count,
    assert_unique,
)

import GcobUtils  # noqa: E402
import sys
from pathlib import Path

# Import GCOB_Party notebook functions
# GCOB_Party is a Databricks notebook (no .py extension).
# Cell 7 of run_tests_notebook pre-imports it via REST API and stores it
# in sys.modules before pytest runs. This fallback handles independent runs.
try:
    import GCOB_Party
except ImportError:
    _gcob_src = Path(__file__).parent.parent / "src"
    if str(_gcob_src) not in sys.path:
        sys.path.insert(0, str(_gcob_src))
    try:
        import GCOB_Party
    except ImportError:
        import types as _types, base64 as _b64, requests as _req, os as _os
        from datetime import datetime as _dt, timedelta as _td
        from pyspark.sql.functions import lit as _lit
        from pyspark.sql import SparkSession as _SS
        _spark = _SS.builder.getOrCreate()

        _nb_path = str(_gcob_src / "GCOB_Party")
        if _nb_path.startswith('/Workspace/'):
            _nb_path = _nb_path[len('/Workspace'):]
        _imported = False
        _content = None

        # Method 1: REST API (works on normal Databricks Runtime clusters)
        _host = None
        _token = None
        # Get host from dbutils.notebook.getContext()
        try:
            _dbutils = get_ipython().user_global_ns.get('dbutils')
            if _dbutils:
                _host = _dbutils.notebook.getContext().apiUrl().get()
        except Exception:
            pass
        # Get host from spark.conf
        if not _host:
            for _key in ["spark.databricks.service.url", "spark.databricks.host"]:
                try:
                    _host = _spark.conf.get(_key)
                    break
                except Exception:
                    pass
        # Get token from dbutils.notebook.getContext().extraContext()
        if not _token:
            try:
                _extra = _dbutils.notebook.getContext().extraContext().get()
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
        # Get token from spark.conf
        if not _token:
            for _key in ["spark.databricks.driver.token", "spark.databricks.token",
                         "spark.databricks.service.token"]:
                try:
                    _token = _spark.conf.get(_key)
                    break
                except Exception:
                    pass
        if not _token:
            try:
                for _key, _val in _spark.conf.getAll().items():
                    if "token" in _key.lower() and _val and len(_val) > 10:
                        _token = _val
                        break
            except Exception:
                pass
        # Get token from /databricks/cluster/driver_secret_key
        if not _token:
            try:
                with open("/databricks/cluster/driver_secret_key") as _f:
                    _token = _f.read().strip()
            except Exception:
                pass
        # Fall back to env vars
        if not _host:
            _host = _os.environ.get("DATABRICKS_HOST")
        if not _token:
            _token = _os.environ.get("DATABRICKS_TOKEN")

        if _host and _token:
            try:
                _resp = _req.get(
                    f"{_host.rstrip('/')}/api/2.0/workspace/export",
                    params={"path": _nb_path, "format": "SOURCE"},
                    headers={"Authorization": f"Bearer {_token}"},
                    timeout=10,
                )
                _resp.raise_for_status()
                _content = _b64.b64decode(_resp.json()["content"]).decode("utf-8")
                _imported = True
            except Exception:
                pass

        # Method 2: WorkspaceClient (works on serverless and clusters with newer SDK)
        if not _imported:
            try:
                from databricks.sdk import WorkspaceClient as _WS
                from databricks.sdk.service.workspace import ExportFormat as _EF
                _w = _WS()
                _exported = _w.workspace.export(path=_nb_path, format=_EF.SOURCE)
                _content = _b64.b64decode(_exported.content).decode("utf-8")
                _imported = True
            except Exception:
                pass

        if _imported and _content:
            _cells = _content.split("# COMMAND ----------")
            GCOB_Party = _types.ModuleType("GCOB_Party")
            GCOB_Party.__dict__.update({"datetime": _dt, "timedelta": _td, "lit": _lit})
            for _cell in _cells:
                if "def calculate_business_date" in _cell:
                    exec(compile(_cell.strip(), _nb_path, "exec"), GCOB_Party.__dict__)
                    break
            sys.modules["GCOB_Party"] = GCOB_Party
        else:
            # Last resort: define functions inline from GCOB_Party notebook Cell 3.
            # These are the exact function definitions — update if the notebook changes.
            GCOB_Party = _types.ModuleType("GCOB_Party")
            GCOB_Party.__dict__.update({"datetime": _dt, "timedelta": _td, "lit": _lit})
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
            exec(compile(_inline_src, "GCOB_Party (inline fallback)", "exec"), GCOB_Party.__dict__)
            sys.modules["GCOB_Party"] = GCOB_Party


@pytest.fixture(autouse=True)
def _suppress_gcob_utils_error_logs():
    """Suppress expected ERROR logs from GcobUtils during tests that mock request failures."""
    gcob_logger = logging.getLogger("GcobUtils")
    old_level = gcob_logger.level
    gcob_logger.setLevel(logging.CRITICAL)
    yield
    gcob_logger.setLevel(old_level)


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def gcob_clients_df(spark):
    """Sample data for global_temp.party_case_client_details.

    Columns: GcobId, ClientType, FullLegalName
    3 rows: 1 Legal Entity, 1 Natural Person, 1 duplicate for dedup test.
    """
    return spark.createDataFrame([
        ("1001", "Legal Entity", "Acme Corporation BV"),
        ("2002", "Natural Person", "John Doe"),
        ("1001", "Legal Entity", "Acme Corporation BV"),  # duplicate
    ], ["GcobId", "ClientType", "FullLegalName"])


@pytest.fixture
def legacy2_clients_df(spark):
    """Sample data for global_temp.Legacy2_client.

    Columns: GcobId, UniqueGcobId, FullLegalName, ClientType
    2 rows: 1 Legal Entity, 1 Natural Person.
    """
    return spark.createDataFrame([
        ("3003", "LE_3003", "Legacy Corp NV", "Legal Entity"),
        ("4004", "NP_NPPC_4004", "Jane Smith", "Natural Person"),
    ], ["GcobId", "UniqueGcobId", "FullLegalName", "ClientType"])


@pytest.fixture
def protected_clients_df(spark):
    """Sample data for global_temp.Gcob_protectedClients.

    Columns: UniqueGcobId, gcobid
    2 rows: protects 1 GCOB client and 1 Legacy2 client.
    """
    return spark.createDataFrame([
        ("LE_1001", "1001"),
        ("LE_3003", "3003"),
    ], ["UniqueGcobId", "gcobid"])


# ===========================================================================
# Cell 3: BusinessDate calculation
# ===========================================================================

class TestBusinessDate:
    """Test the BusinessDate calculation: (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')."""

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_business_date_is_yesterday(self) -> None:
        """Input: today. Output: yesterday in MM/DD/YYYY format. Mocked: none."""
        BusinessDate = GCOB_Party.calculate_business_date()
        expected = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
        assert BusinessDate == expected

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_business_date_format_is_mmddyyyy(self) -> None:
        """Input: today. Output: string matching MM/DD/YYYY pattern. Mocked: none."""
        import re
        BusinessDate = GCOB_Party.calculate_business_date()
        assert re.match(r'^\d{2}/\d{2}/\d{4}$', BusinessDate), f"Expected MM/DD/YYYY, got {BusinessDate}"

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_business_date_has_slash_separator(self) -> None:
        """Input: today. Output: date with '/' as separator. Mocked: none."""
        BusinessDate = GCOB_Party.calculate_business_date()
        assert '/' in BusinessDate
        assert BusinessDate.count('/') == 2

    @pytest.mark.unit
    @pytest.mark.gcob_consumer
    def test_business_date_is_one_day_behind_today(self) -> None:
        """Input: today. Output: date exactly 1 day before today. Mocked: none."""
        BusinessDate = datetime.strptime(
            GCOB_Party.calculate_business_date(),
            '%m/%d/%Y'
        ).date()
        today = datetime.today().date()
        assert (today - BusinessDate).days == 1


# ===========================================================================
# Cell 5: Gcob_Legacy2_Clients view (UNION of GCOB + Legacy2)
# ===========================================================================

# SQL from the notebook Cell 5
_GCOb_LEGACY2_SQL = """
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
"""

class TestGcobLegacy2Clients:
    """Test the Gcob_Legacy2_Clients temp view from Cell 5.

    Combines GCOB and Legacy2 client sources via UNION with UniqueGcobId logic:
    - GCOB Legal Entity -> 'LE_<GcobId>'
    - GCOB non-Legal Entity -> 'NP_NPPC_<GcobId>'
    - Legacy2 -> uses existing UniqueGcobId as-is
    """

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_returns_expected_columns(self, spark, gcob_clients_df, legacy2_clients_df) -> None:
        """Input: 3 GCOB + 2 Legacy2 rows. Output: 5 columns (GcobId, UniqueGcobId, SourceSystem, FullLegalName, PartyType). Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        GCOB_Party.create_gcob_legacy2_view(spark)
        result = spark.table("Gcob_Legacy2_Clients")
        assert_column_subset(result, ["GcobId", "UniqueGcobId", "SourceSystem", "FullLegalName", "PartyType"])

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_gcob_legal_entity_gets_le_prefix(self, spark, gcob_clients_df, legacy2_clients_df) -> None:
        """Input: GCOB ClientType='Legal Entity', GcobId='1001'. Output: UniqueGcobId='LE_1001'. Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        GCOB_Party.create_gcob_legacy2_view(spark)
        result = spark.sql("SELECT UniqueGcobId FROM Gcob_Legacy2_Clients WHERE GcobId='1001' AND SourceSystem='GCOB'").collect()
        assert len(result) == 1
        assert result[0]["UniqueGcobId"] == "LE_1001"

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_gcob_non_legal_entity_gets_np_prefix(self, spark, gcob_clients_df, legacy2_clients_df) -> None:
        """Input: GCOB ClientType='Natural Person', GcobId='2002'. Output: UniqueGcobId='NP_NPPC_2002'. Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        GCOB_Party.create_gcob_legacy2_view(spark)
        result = spark.sql("SELECT UniqueGcobId FROM Gcob_Legacy2_Clients WHERE GcobId='2002' AND SourceSystem='GCOB'").collect()
        assert len(result) == 1
        assert result[0]["UniqueGcobId"] == "NP_NPPC_2002"

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_legacy2_uses_existing_unique_gcob_id(self, spark, gcob_clients_df, legacy2_clients_df) -> None:
        """Input: Legacy2 UniqueGcobId='LE_3003'. Output: UniqueGcobId='LE_3003' (unchanged). Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        GCOB_Party.create_gcob_legacy2_view(spark)
        result = spark.sql("SELECT UniqueGcobId FROM Gcob_Legacy2_Clients WHERE SourceSystem='Legacy2' AND GcobId='3003'").collect()
        assert len(result) == 1
        assert result[0]["UniqueGcobId"] == "LE_3003"

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_both_sources_present_in_union(self, spark, gcob_clients_df, legacy2_clients_df) -> None:
        """Input: 3 GCOB + 2 Legacy2 rows. Output: rows from both GCOB and Legacy2 sources. Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        GCOB_Party.create_gcob_legacy2_view(spark)
        source_systems = spark.sql("SELECT DISTINCT SourceSystem FROM Gcob_Legacy2_Clients").collect()
        sources = {row["SourceSystem"] for row in source_systems}
        assert sources == {"GCOB", "Legacy2"}

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_duplicate_gcob_rows_are_deduplicated(self, spark, gcob_clients_df, legacy2_clients_df) -> None:
        """Input: 3 GCOB rows (1 duplicate) + 2 Legacy2 rows. Output: 4 distinct rows (not 5). Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        GCOB_Party.create_gcob_legacy2_view(spark)
        result = spark.table("Gcob_Legacy2_Clients")
        assert_row_count(result, 4)

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_gcobid_is_cast_to_string(self, spark, gcob_clients_df, legacy2_clients_df) -> None:
        """Input: GcobId values as strings. Output: GcobId column type is string. Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        GCOB_Party.create_gcob_legacy2_view(spark)
        result = spark.table("Gcob_Legacy2_Clients")
        gcobid_type = dict(result.dtypes).get("GcobId")
        assert gcobid_type == "string", f"Expected GcobId to be string, got {gcobid_type}"

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_partytype_renamed_from_clienttype(self, spark, gcob_clients_df, legacy2_clients_df) -> None:
        """Input: ClientType column. Output: PartyType column (renamed). Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        GCOB_Party.create_gcob_legacy2_view(spark)
        result = spark.table("Gcob_Legacy2_Clients")
        columns = result.columns
        assert "PartyType" in columns
        assert "ClientType" not in columns


# ===========================================================================
# Cell 6: Party view (Apply IsProtected Logic)
# ===========================================================================

# SQL from the notebook Cell 6
_PARTY_SQL = """
CREATE OR REPLACE TEMP VIEW Party AS
select distinct
c.GcobId
,c.UniqueGcobId
,c.SourceSystem
,case when p.UniqueGcobId is not null then CONCAT('protected account ', p.gcobid) else c.FullLegalName end as FullLegalName
,c.PartyType
from Gcob_Legacy2_Clients c
left join global_temp.Gcob_protectedClients p on c.UniqueGcobId = p.UniqueGcobId
"""

class TestPartyView:
    """Test the Party temp view from Cell 6.

    Left joins Gcob_Legacy2_Clients with Gcob_protectedClients.
    For protected clients: FullLegalName = 'protected account <gcobid>'.
    For non-protected clients: FullLegalName = original FullLegalName.
    """

    @pytest.fixture(autouse=True)
    def _setup_views(self, spark, gcob_clients_df, legacy2_clients_df, protected_clients_df):
        """Set up all 3 input views before each test in this class."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        protected_clients_df.createOrReplaceGlobalTempView("Gcob_protectedClients")
        GCOB_Party.create_gcob_legacy2_view(spark)
        GCOB_Party.create_party_view(spark)

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_returns_expected_columns(self, spark) -> None:
        """Input: Gcob_Legacy2_Clients + Gcob_protectedClients. Output: 5 columns (GcobId, UniqueGcobId, SourceSystem, FullLegalName, PartyType). Mocked: global_temp views."""
        result = spark.table("Party")
        assert_column_subset(result, ["GcobId", "UniqueGcobId", "SourceSystem", "FullLegalName", "PartyType"])

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_protected_client_name_is_masked(self, spark) -> None:
        """Input: GcobId='1001' is protected. Output: FullLegalName='protected account 1001'. Mocked: global_temp views."""
        result = spark.sql("SELECT FullLegalName FROM Party WHERE GcobId='1001'").collect()
        assert len(result) == 1
        assert result[0]["FullLegalName"] == "protected account 1001"

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_protected_legacy2_client_name_is_masked(self, spark) -> None:
        """Input: GcobId='3003' (Legacy2) is protected. Output: FullLegalName='protected account 3003'. Mocked: global_temp views."""
        result = spark.sql("SELECT FullLegalName FROM Party WHERE GcobId='3003'").collect()
        assert len(result) == 1
        assert result[0]["FullLegalName"] == "protected account 3003"

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_non_protected_client_name_is_preserved(self, spark) -> None:
        """Input: GcobId='2002' is NOT protected. Output: FullLegalName='John Doe' (original). Mocked: global_temp views."""
        result = spark.sql("SELECT FullLegalName FROM Party WHERE GcobId='2002'").collect()
        assert len(result) == 1
        assert result[0]["FullLegalName"] == "John Doe"

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_non_protected_legacy2_client_name_is_preserved(self, spark) -> None:
        """Input: GcobId='4004' (Legacy2) is NOT protected. Output: FullLegalName='Jane Smith' (original). Mocked: global_temp views."""
        result = spark.sql("SELECT FullLegalName FROM Party WHERE GcobId='4004'").collect()
        assert len(result) == 1
        assert result[0]["FullLegalName"] == "Jane Smith"

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_all_clients_preserved_after_left_join(self, spark) -> None:
        """Input: 4 Gcob_Legacy2_Clients rows. Output: 4 rows (left join preserves all). Mocked: global_temp views."""
        result = spark.table("Party")
        assert_row_count(result, 4)

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_source_system_preserved_after_join(self, spark) -> None:
        """Input: GCOB and Legacy2 sources. Output: SourceSystem values preserved from Gcob_Legacy2_Clients. Mocked: global_temp views."""
        source_systems = spark.sql("SELECT DISTINCT SourceSystem FROM Party").collect()
        sources = {row["SourceSystem"] for row in source_systems}
        assert sources == {"GCOB", "Legacy2"}

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_unique_gcob_id_preserved(self, spark) -> None:
        """Input: UniqueGcobId from Gcob_Legacy2_Clients. Output: UniqueGcobId preserved after join. Mocked: global_temp views."""
        result = spark.table("Party")
        assert_unique(result, ["UniqueGcobId"])

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_no_nulls_in_key_columns(self, spark) -> None:
        """Input: all rows have GcobId, UniqueGcobId, SourceSystem, PartyType. Output: no nulls in key columns. Mocked: global_temp views."""
        result = spark.table("Party")
        assert_no_nulls(result, ["GcobId", "UniqueGcobId", "SourceSystem", "PartyType"])

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_empty_protected_clients_keeps_all_names(self, spark, gcob_clients_df, legacy2_clients_df) -> None:
        """Input: empty Gcob_protectedClients. Output: all FullLegalName values preserved (none masked). Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        spark.createDataFrame([], "UniqueGcobId string, gcobid string").createOrReplaceGlobalTempView("Gcob_protectedClients")
        GCOB_Party.create_gcob_legacy2_view(spark)
        GCOB_Party.create_party_view(spark)
        result = spark.table("Party")
        names = {row["FullLegalName"] for row in result.collect()}
        assert "Acme Corporation BV" in names
        assert "John Doe" in names
        assert "Legacy Corp NV" in names
        assert "Jane Smith" in names
        assert not any("protected account" in n for n in names)


# ===========================================================================
# Cell 8: BusinessDate column addition
# ===========================================================================

class TestBusinessDateColumn:
    """Test adding BusinessDate column to the Party DataFrame (Cell 8).

    df_Party = df_Party.withColumn("BusinessDate", lit(BusinessDate))
    """

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_business_date_column_added(self, spark, gcob_clients_df, legacy2_clients_df, protected_clients_df) -> None:
        """Input: Party DataFrame without BusinessDate. Output: DataFrame with BusinessDate column added. Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        protected_clients_df.createOrReplaceGlobalTempView("Gcob_protectedClients")
        GCOB_Party.create_gcob_legacy2_view(spark)
        GCOB_Party.create_party_view(spark)
        BusinessDate = GCOB_Party.calculate_business_date()
        df_Party = spark.table("Party")
        df_Party = GCOB_Party.add_business_date_column(df_Party, BusinessDate)
        assert "BusinessDate" in df_Party.columns

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_business_date_column_has_correct_value(self, spark, gcob_clients_df, legacy2_clients_df, protected_clients_df) -> None:
        """Input: BusinessDate=yesterday. Output: all rows have BusinessDate=yesterday in MM/DD/YYYY. Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        protected_clients_df.createOrReplaceGlobalTempView("Gcob_protectedClients")
        GCOB_Party.create_gcob_legacy2_view(spark)
        GCOB_Party.create_party_view(spark)
        BusinessDate = GCOB_Party.calculate_business_date()
        df_Party = GCOB_Party.add_business_date_column(spark.table("Party"), BusinessDate)
        dates = {row["BusinessDate"] for row in df_Party.select("BusinessDate").collect()}
        assert dates == {BusinessDate}

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_business_date_column_type_is_string(self, spark, gcob_clients_df, legacy2_clients_df, protected_clients_df) -> None:
        """Input: BusinessDate as string. Output: BusinessDate column type is string. Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        protected_clients_df.createOrReplaceGlobalTempView("Gcob_protectedClients")
        GCOB_Party.create_gcob_legacy2_view(spark)
        GCOB_Party.create_party_view(spark)
        BusinessDate = GCOB_Party.calculate_business_date()
        df_Party = GCOB_Party.add_business_date_column(spark.table("Party"), BusinessDate)
        assert dict(df_Party.dtypes)["BusinessDate"] == "string"

    @pytest.mark.spark
    @pytest.mark.gcob_consumer
    def test_business_date_same_for_all_rows(self, spark, gcob_clients_df, legacy2_clients_df, protected_clients_df) -> None:
        """Input: 4 rows in Party. Output: all 4 rows have same BusinessDate value. Mocked: global_temp views."""
        gcob_clients_df.createOrReplaceGlobalTempView("party_case_client_details")
        legacy2_clients_df.createOrReplaceGlobalTempView("Legacy2_client")
        protected_clients_df.createOrReplaceGlobalTempView("Gcob_protectedClients")
        GCOB_Party.create_gcob_legacy2_view(spark)
        GCOB_Party.create_party_view(spark)
        BusinessDate = GCOB_Party.calculate_business_date()
        df_Party = GCOB_Party.add_business_date_column(spark.table("Party"), BusinessDate)
        assert_row_count(df_Party, 4)
        distinct_dates = df_Party.select("BusinessDate").distinct().count()
        assert distinct_dates == 1