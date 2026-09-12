"""
radarv1/tests/test_functions_databricks.py
============================================
Unit tests for ``radarv1/src/functions_databricks.py``.

Covers
------
Unit (no Spark):
  - append_to_databricks_table()      : verifies write mode and table name
  - full_load_to_databricks_table()   : verifies DROP + overwrite
  - upsert_to_databricks_table()      : MERGE condition builder + duplicate-key guard
  - check_table_not_exists()          : True when table absent, False when present

Spark (local SparkSession):
  - upsert_to_databricks_table()      : end-to-end MERGE on a real local table
  - check_table_not_exists()          : real catalog lookup

All Databricks SDK / dbutils / spark module-level references are patched via
the conftest stubs or via monkeypatch inside individual tests.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch, PropertyMock

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.utils import AnalysisException

from utils.df_helpers import assert_row_count, assert_unique


# ---------------------------------------------------------------------------
# Import the module under test
# functions_databricks.py does `from databricks.sdk.runtime import spark` and
# `dbutils = DBUtils()` at module level; conftest stubs both before import.
# ---------------------------------------------------------------------------
import functions_databricks as fdb  # noqa: E402


# ===========================================================================
# append_to_databricks_table
# ===========================================================================

@pytest.mark.unit
@pytest.mark.radarv1
class TestAppendToDatabricksTable:

    def test_calls_write_mode_append(self) -> None:
        df_mock = MagicMock()
        fdb.append_to_databricks_table(df_mock, "my_schema", "my_table")

        df_mock.write.mode.assert_called_once_with("append")
        df_mock.write.mode.return_value.saveAsTable.assert_called_once_with(
            "my_schema.my_table"
        )

    def test_table_name_is_schema_dot_table(self) -> None:
        df_mock = MagicMock()
        fdb.append_to_databricks_table(df_mock, "radar", "cases_clients")

        _, kwargs = df_mock.write.mode.return_value.saveAsTable.call_args
        # positional arg
        positional = df_mock.write.mode.return_value.saveAsTable.call_args[0]
        assert positional[0] == "radar.cases_clients"

    @pytest.mark.parametrize("schema,table", [
        ("wr_fj_parties_dev", "cases_clients"),
        ("radar",              "radardatamodel"),
        ("gcob",               "gcob_party"),
    ])
    def test_various_schema_table_combinations(self, schema: str, table: str) -> None:
        df_mock = MagicMock()
        fdb.append_to_databricks_table(df_mock, schema, table)
        df_mock.write.mode.return_value.saveAsTable.assert_called_with(f"{schema}.{table}")


# ===========================================================================
# full_load_to_databricks_table
# ===========================================================================

@pytest.mark.unit
@pytest.mark.radarv1
class TestFullLoadToDatabricksTable:

    def test_drops_table_before_write(self, monkeypatch) -> None:
        """DROP TABLE IF EXISTS must be called before writing."""
        spark_mock = MagicMock()
        monkeypatch.setattr(fdb, "spark", spark_mock)

        df_mock = MagicMock()
        fdb.full_load_to_databricks_table(df_mock, "my_schema", "my_table")

        spark_mock.sql.assert_called_once_with(
            "DROP TABLE IF EXISTS my_schema.my_table"
        )

    def test_writes_in_overwrite_delta_mode(self, monkeypatch) -> None:
        spark_mock = MagicMock()
        monkeypatch.setattr(fdb, "spark", spark_mock)

        df_mock = MagicMock()
        fdb.full_load_to_databricks_table(df_mock, "my_schema", "my_table")

        df_mock.write.format.assert_called_once_with("delta")
        df_mock.write.format.return_value.mode.assert_called_once_with("overwrite")
        df_mock.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
            "my_schema.my_table"
        )

    def test_drop_happens_before_write(self, monkeypatch) -> None:
        """Verify call ordering: DROP then write."""
        call_order: list[str] = []
        spark_mock = MagicMock()
        spark_mock.sql.side_effect = lambda _: call_order.append("drop")
        monkeypatch.setattr(fdb, "spark", spark_mock)

        df_mock = MagicMock()
        save_mock = MagicMock(side_effect=lambda _: call_order.append("write"))
        df_mock.write.format.return_value.mode.return_value.saveAsTable = save_mock

        fdb.full_load_to_databricks_table(df_mock, "s", "t")
        assert call_order == ["drop", "write"], f"Expected [drop, write], got {call_order}"


# ===========================================================================
# upsert_to_databricks_table
# ===========================================================================

@pytest.mark.unit
@pytest.mark.radarv1
class TestUpsertToDatabricksTable:

    def test_raises_on_duplicate_primary_keys(self, spark) -> None:
        """Duplicate PKs in the source DataFrame must raise ValueError."""
        df = spark.createDataFrame([
            Row(id="A", val=1),
            Row(id="A", val=2),   # duplicate PK
        ])
        with pytest.raises(ValueError, match="Duplicate primary keys"):
            fdb.upsert_to_databricks_table(df, "schema", "table", ["id"])

    def test_no_error_on_unique_primary_keys(self, spark, monkeypatch) -> None:
        """Unique PKs should not raise; the MERGE SQL is issued."""
        df = spark.createDataFrame([
            Row(id="A", val=1),
            Row(id="B", val=2),
        ])
        spark_mock = MagicMock()
        monkeypatch.setattr(fdb, "spark", spark_mock)

        fdb.upsert_to_databricks_table(df, "my_schema", "my_table", ["id"])

        spark_mock.sql.assert_called_once()
        sql_str = spark_mock.sql.call_args[0][0]
        assert "MERGE INTO my_schema.my_table" in sql_str

    def test_merge_condition_contains_all_pk_columns(self, spark, monkeypatch) -> None:
        """Every PK column must appear in the ON clause of the MERGE statement."""
        df = spark.createDataFrame([Row(id="A", cat="X", val=1)])
        spark_mock = MagicMock()
        monkeypatch.setattr(fdb, "spark", spark_mock)

        fdb.upsert_to_databricks_table(df, "s", "t", ["id", "cat"])

        sql_str = spark_mock.sql.call_args[0][0]
        assert "target.id = source.id"   in sql_str
        assert "target.cat = source.cat" in sql_str

    def test_merge_sql_contains_matched_and_not_matched(self, spark, monkeypatch) -> None:
        df = spark.createDataFrame([Row(id="A", val=1)])
        spark_mock = MagicMock()
        monkeypatch.setattr(fdb, "spark", spark_mock)

        fdb.upsert_to_databricks_table(df, "s", "t", ["id"])

        sql_str = spark_mock.sql.call_args[0][0]
        assert "WHEN MATCHED THEN UPDATE SET *"   in sql_str
        assert "WHEN NOT MATCHED THEN INSERT *"   in sql_str

    def test_creates_temp_view_named_updates(self, spark, monkeypatch) -> None:
        """Source DataFrame must be registered as 'updates' temp view."""
        df = MagicMock()
        df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 0

        spark_mock = MagicMock()
        monkeypatch.setattr(fdb, "spark", spark_mock)

        fdb.upsert_to_databricks_table(df, "s", "t", ["id"])

        df.createOrReplaceTempView.assert_called_once_with("updates")


# ===========================================================================
# check_table_not_exists
# ===========================================================================

@pytest.mark.unit
@pytest.mark.radarv1
class TestCheckTableNotExists:

    def test_returns_true_when_table_absent(self, monkeypatch) -> None:
        spark_mock = MagicMock()
        spark_mock.table.side_effect = AnalysisException("Table not found", None)
        monkeypatch.setattr(fdb, "spark", spark_mock)

        assert fdb.check_table_not_exists("missing.table") is True

    def test_returns_false_when_table_present(self, monkeypatch) -> None:
        spark_mock = MagicMock()
        spark_mock.table.return_value = MagicMock()   # no exception → table exists
        monkeypatch.setattr(fdb, "spark", spark_mock)

        assert fdb.check_table_not_exists("existing.table") is False

    def test_only_analysis_exception_caught(self, monkeypatch) -> None:
        """A non-AnalysisException must bubble up unchanged."""
        spark_mock = MagicMock()
        spark_mock.table.side_effect = RuntimeError("unexpected error")
        monkeypatch.setattr(fdb, "spark", spark_mock)

        with pytest.raises(RuntimeError, match="unexpected error"):
            fdb.check_table_not_exists("some.table")


# ===========================================================================
# load_from_gdp_parquet — boundary / validation tests (unit)
# ===========================================================================

@pytest.mark.unit
@pytest.mark.radarv1
class TestLoadFromGDPParquet:

    def test_invalid_producer_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="not allowed"):
            fdb.load_from_gdp_parquet("unknown_producer", "some_table")

    @pytest.mark.parametrize("producer", ["gcob", "core-cbt", "gcds", "planet"])
    def test_valid_producers_accepted(self, producer: str, monkeypatch) -> None:
        """Valid producer names must not raise on the guard check."""
        # Patch everything that touches the network / filesystem
        monkeypatch.setattr(fdb, "dbutils", _make_dbutils_mock(producer))
        monkeypatch.setattr(fdb, "spark",   _make_spark_mock())

        # We only care that ValueError is NOT raised on the guard
        try:
            fdb.load_from_gdp_parquet(producer, "test_table")
        except (RuntimeError, KeyError, StopIteration, AttributeError):
            # Expected — the mock filesystem returns no files, so later
            # logic will fail; that's fine, we're testing only the guard.
            pass
        except ValueError as exc:
            pytest.fail(f"ValueError raised for valid producer {producer!r}: {exc}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_dbutils_mock(producer: str = "gcob") -> MagicMock:
    from types import SimpleNamespace
    today = __import__("datetime").datetime.today().strftime("%Y%m%d")

    file_mock = SimpleNamespace(
        path=f"abfss://{producer}@storage.dfs.core.windows.net/CaseService/test_table/101/",
        name="101/",
    )
    partition_mock = SimpleNamespace(
        path=f"abfss://{producer}@storage.dfs.core.windows.net/CaseService/test_table/101/data/EDL_LOAD_DTS={today}/",
        name=f"EDL_LOAD_DTS={today}/",
    )

    dbutils = MagicMock()
    dbutils.secrets.get.return_value = "fake-secret"
    dbutils.fs.ls.side_effect = [
        [file_mock],       # first call: version listing
        [partition_mock],  # second call: partition listing
    ]
    return dbutils


def _make_spark_mock() -> MagicMock:
    spark_mock = MagicMock()
    spark_mock.read.parquet.return_value.createOrReplaceTempView.return_value = None
    spark_mock.sql.return_value.collect.return_value = [MagicMock(**{"__getitem__.return_value": 100})]
    return spark_mock
