"""
GCOB_Reportingv1/tests/test_dq_validation.py
==============================================
Spark tests for the DQ validation functions defined in
``GCOB_Reportingv1/tests/GCOB_Producer_DQ_Test.py``.

Because that notebook is a Databricks source file (not a Python module), we
cannot import it directly.  Instead, the functions under test
(``test_dataobject_uniqueness``, ``test_dataobject_null_fields``,
``find_missing_clients``) are re-defined here in a thin wrapper module pattern:
the actual logic is copied verbatim from the notebook but ``log_to_catalog``
is replaced with a no-op stub so tests run without a live Hive metastore.

The conftest.py `spark` fixture provides a session-scoped local SparkSession.
All assertions use the shared helpers from tests/utils/df_helpers.py.

Test groups
-----------
TestUniquenessCheck     — test_dataobject_uniqueness()
TestNullCheck           — test_dataobject_null_fields()
TestMissingClientsCheck — find_missing_clients()
TestStructureViolation  — the inline Directorship/Authorised Representative rule
"""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from fixtures.sample_dataframes import (
    party_all_party_details_df,
    party_business_activities_df,
    party_business_activities_with_duplicates_df,
    party_case_client_details_df,
    party_client_structure_gui_df,
    party_workitem_df,
    risk_model_instance_df,
)
from utils.df_helpers import assert_no_nulls, assert_row_count, assert_unique


# ---------------------------------------------------------------------------
# Inline re-implementation of the notebook DQ functions
# (identical logic, log_to_catalog replaced with a test double)
# ---------------------------------------------------------------------------

def _noop_log(dataframe_name, rule, columns, status, message):
    """No-op replacement for log_to_catalog — captures args via mock."""
    pass


def test_dataobject_uniqueness(
    df: DataFrame,
    dataframe_name: str,
    columns: list[str],
    rule_name: str,
    id_column: str | None = None,
    where_condition: str | None = None,
    *,
    log_fn=_noop_log,
) -> dict:
    """
    Extracted from GCOB_Producer_DQ_Test.py — checks column uniqueness.
    Returns a result dict {status, message, unique_count, total_count}.
    """
    try:
        if where_condition:
            df = df.filter(where_condition)

        unique_count = df.select(columns).distinct().count()
        total_count  = df.count()

        if unique_count == total_count:
            status  = "Pass"
            message = f"All values in column(s) {columns} are unique"
        else:
            duplicates_df    = df.groupBy(columns).count().filter("count > 1")
            duplicates_count = duplicates_df.count()

            if id_column and id_column in df.columns:
                duplicate_ids = duplicates_df.select(id_column).limit(10).collect()
                id_str        = "\n".join(str(r[id_column]) for r in duplicate_ids)
                message       = (
                    f"Column(s) {columns} contains {duplicates_count} duplicate values. "
                    f"Few problematic {id_column}: {id_str}"
                )
            else:
                message = f"Column(s) {columns} contains {duplicates_count} duplicate values."
            status = "Fail"

        log_fn(dataframe_name, rule_name, ",".join(columns), status, message)
        return {"status": status, "message": message,
                "unique_count": unique_count, "total_count": total_count}
    except Exception as exc:  # noqa: BLE001
        log_fn(dataframe_name, rule_name, ",".join(columns), "Error", str(exc))
        raise

# Prevent pytest from collecting these as test functions (they are DQ logic, not tests)
test_dataobject_uniqueness.__test__ = False


def test_dataobject_null_fields(
    df: DataFrame,
    dataframe_name: str,
    columns: list[str],
    rule_name: str,
    id_column: str | None = None,
    where_condition: str | None = None,
    *,
    log_fn=_noop_log,
) -> dict:
    """
    Extracted from GCOB_Producer_DQ_Test.py — checks for NULL values.
    Returns a result dict {status, message} for the last checked column.
    """
    results = {}
    try:
        if where_condition:
            df = df.filter(where_condition)

        for column in columns:
            null_count = df.filter(df[column].isNull()).count()

            if null_count == 0:
                status  = "Pass"
                message = f"Column(s) {columns} does not contain any null values"
            else:
                status         = "Fail"
                problematic_id = df.filter(df[column].isNull()).select(id_column).limit(10).collect()
                id_str         = "\n".join(str(r[id_column]) for r in problematic_id)
                message        = (
                    f"Column(s) {columns} contains {null_count} null values. "
                    f"Few Problematic UniqueGcobIds: {id_str}"
                )

            log_fn(dataframe_name, rule_name, ",".join(columns), status, message)
            results[column] = {"status": status, "message": message, "null_count": null_count}

        return results
    except Exception as exc:  # noqa: BLE001
        log_fn(dataframe_name, rule_name, ",".join(columns), "Error", str(exc))
        raise

# Prevent pytest from collecting this as a test function (it is DQ logic, not a test)
test_dataobject_null_fields.__test__ = False


def find_missing_clients(
    df1: DataFrame,
    df2: DataFrame,
    dataframe_name1: str,
    dataframe_name2: str,
    column: str,
    rule_name: str,
    where_condition: str | None = None,
    *,
    log_fn=_noop_log,
) -> dict:
    """
    Extracted from GCOB_Producer_DQ_Test.py — finds values in df1 not present in df2.
    Returns {status, message, missing_count}.
    """
    try:
        if where_condition:
            df1 = df1.filter(where_condition)

        joined            = df1.join(df2, df1[column] == df2[column], "left")
        missing_df        = joined.filter(df2[column].isNull())
        missing_count     = missing_df.count()

        if missing_count == 0:
            status  = "Pass"
            message = (
                f"All {column} values in {dataframe_name1} are present in {dataframe_name2}."
            )
        else:
            sample = missing_df.select(df1[column]).distinct().limit(10).collect()
            sample_str = "\n".join(str(r[column]) for r in sample)
            status  = "Fail"
            message = (
                f"There are {missing_count} {column} values in {dataframe_name1} "
                f"that are not present in {dataframe_name2}. Details:\n{sample_str}"
            )

        log_fn(dataframe_name2, rule_name, column, status, message)
        return {"status": status, "message": message, "missing_count": missing_count}
    except Exception as exc:  # noqa: BLE001
        log_fn(dataframe_name2, rule_name, column, "Error", str(exc))
        raise


# ===========================================================================
# TestUniquenessCheck
# ===========================================================================

@pytest.mark.spark
@pytest.mark.gcob_reportingv1
@pytest.mark.dq
class TestUniquenessCheck:

    def test_unique_columns_pass(self, spark) -> None:
        df = party_case_client_details_df(spark)
        mock_log = MagicMock()

        result = test_dataobject_uniqueness(
            df, "party_case_client_details",
            ["UniqueGcobId", "CaseId"], "GcobId_uniqueness_check",
            "UniqueGcobId", log_fn=mock_log,
        )

        assert result["status"] == "Pass"
        mock_log.assert_called_once()
        _, _, _, status, _ = mock_log.call_args[0]
        assert status == "Pass"

    def test_duplicate_columns_fail(self, spark) -> None:
        df = party_business_activities_with_duplicates_df(spark)
        mock_log = MagicMock()

        result = test_dataobject_uniqueness(
            df, "party_business_activities",
            ["SourceClient", "NaicsCode"], "uniqueness_check",
            "SourceClient", log_fn=mock_log,
        )

        assert result["status"] == "Fail"
        assert "duplicate" in result["message"].lower()
        _, _, _, status, message = mock_log.call_args[0]
        assert status == "Fail"
        assert "1 duplicate" in message

    def test_uniqueness_with_where_condition(self, spark) -> None:
        """Filtering before uniqueness check should reduce scope correctly."""
        df = party_case_client_details_df(spark)
        mock_log = MagicMock()

        # Filter to only Completed rows — UniqueGcobId+CaseId still unique
        result = test_dataobject_uniqueness(
            df, "party_case_client_details",
            ["UniqueGcobId", "CaseId"], "filtered_uniqueness",
            "UniqueGcobId",
            where_condition="CaseStatusName = 'Completed'",
            log_fn=mock_log,
        )

        assert result["status"] == "Pass"

    def test_uniqueness_logs_exactly_once_per_call(self, spark) -> None:
        df = party_workitem_df(spark)
        mock_log = MagicMock()

        test_dataobject_uniqueness(
            df, "party_workitem",
            ["SourceClient", "WorkItemId"], "wi_uniqueness",
            log_fn=mock_log,
        )

        assert mock_log.call_count == 1


# ===========================================================================
# TestNullCheck
# ===========================================================================

@pytest.mark.spark
@pytest.mark.gcob_reportingv1
@pytest.mark.dq
class TestNullCheck:

    def test_no_nulls_passes(self, spark) -> None:
        df = party_case_client_details_df(spark)
        mock_log = MagicMock()

        result = test_dataobject_null_fields(
            df, "party_case_client_details",
            ["UniqueGcobId", "CaseId"], "null_check",
            "UniqueGcobId", log_fn=mock_log,
        )

        # UniqueGcobId and CaseId have no nulls in the sample fixture
        for col, r in result.items():
            assert r["status"] == "Pass", f"Expected Pass for {col}, got {r['status']}"

    def test_null_in_column_fails(self, spark) -> None:
        """ValidatedRiskLevel is NULL for the InProgress row in the fixture."""
        df = party_case_client_details_df(spark)
        mock_log = MagicMock()

        result = test_dataobject_null_fields(
            df, "party_case_client_details",
            ["ValidatedRiskLevel"], "risk_level_null_check",
            "UniqueGcobId", log_fn=mock_log,
        )

        assert result["ValidatedRiskLevel"]["status"] == "Fail"
        assert result["ValidatedRiskLevel"]["null_count"] >= 1

    def test_null_check_with_where_condition_narrows_scope(self, spark) -> None:
        """
        When filtering to CaseStatusName='Completed', ValidatedRiskLevel
        should have no NULLs because only the InProgress row has a NULL.
        """
        df = party_case_client_details_df(spark)
        mock_log = MagicMock()

        result = test_dataobject_null_fields(
            df, "party_case_client_details",
            ["ValidatedRiskLevel"], "risk_level_null_check",
            "UniqueGcobId",
            where_condition="CaseStatusName = 'Completed'",
            log_fn=mock_log,
        )

        assert result["ValidatedRiskLevel"]["status"] == "Pass"

    def test_multiple_columns_checked_independently(self, spark) -> None:
        """Each column in the list is checked separately; results are per-column."""
        df = party_case_client_details_df(spark)
        mock_log = MagicMock()

        result = test_dataobject_null_fields(
            df, "party_case_client_details",
            ["UniqueGcobId", "ValidatedRiskLevel"], "multi_col_null",
            "UniqueGcobId", log_fn=mock_log,
        )

        assert result["UniqueGcobId"]["status"]        == "Pass"
        assert result["ValidatedRiskLevel"]["status"]  == "Fail"
        # log called once per column
        assert mock_log.call_count == 2

    def test_null_message_contains_count(self, spark) -> None:
        df = party_case_client_details_df(spark)
        mock_log = MagicMock()

        test_dataobject_null_fields(
            df, "party_case_client_details",
            ["ValidatedRiskLevel"], "null_msg_check",
            "UniqueGcobId", log_fn=mock_log,
        )

        _, _, _, _, message = mock_log.call_args[0]
        assert "null values" in message.lower()


# ===========================================================================
# TestMissingClientsCheck
# ===========================================================================

@pytest.mark.spark
@pytest.mark.gcob_reportingv1
@pytest.mark.dq
class TestMissingClientsCheck:

    def test_all_clients_present_passes(self, spark) -> None:
        """All SourceClients in risk model table are present in case table."""
        df_cases  = party_case_client_details_df(spark)
        df_risk   = risk_model_instance_df(spark)
        mock_log  = MagicMock()

        result = find_missing_clients(
            df_risk, df_cases,
            "Party_RiskModelInstance", "party_case_client_details",
            "SourceClient", "missing_clients_check",
            log_fn=mock_log,
        )

        assert result["status"] == "Pass"
        assert result["missing_count"] == 0

    def test_missing_client_detected(self, spark) -> None:
        """A SourceClient in the risk model table that is absent from cases → Fail."""
        df_cases = party_case_client_details_df(spark)

        # Add an extra SourceClient that doesn't exist in df_cases
        extra_row = spark.createDataFrame(
            [Row(SourceClient="SC-GHOST", InstanceId="INST-GHOST", RiskModelName="Unknown")],
        )
        from fixtures.sample_dataframes import RISK_MODEL_INSTANCE_SCHEMA
        df_risk = risk_model_instance_df(spark).union(
            spark.createDataFrame(
                [("SC-GHOST", "INST-GHOST", "Unknown Model")],
                schema=RISK_MODEL_INSTANCE_SCHEMA,
            )
        )
        mock_log = MagicMock()

        result = find_missing_clients(
            df_risk, df_cases,
            "Party_RiskModelInstance", "party_case_client_details",
            "SourceClient", "missing_check",
            log_fn=mock_log,
        )

        assert result["status"] == "Fail"
        assert result["missing_count"] >= 1
        assert "SC-GHOST" in result["message"]

    def test_where_condition_applied_before_join(self, spark) -> None:
        """
        Rows filtered out by where_condition should not trigger missing-client
        failures even if they have no match in the reference table.
        """
        df_cases = party_case_client_details_df(spark)
        from fixtures.sample_dataframes import RISK_MODEL_INSTANCE_SCHEMA
        # SC-NULL has a null SourceClient — filtered out by where_condition
        df_risk = spark.createDataFrame(
            [("SC-001", "INST-001", "Model A"), (None, "INST-X", "Model B")],
            schema=RISK_MODEL_INSTANCE_SCHEMA,
        )
        mock_log = MagicMock()

        result = find_missing_clients(
            df_risk, df_cases,
            "Party_RiskModelInstance", "party_case_client_details",
            "SourceClient", "null_filter_check",
            where_condition="SourceClient IS NOT NULL",
            log_fn=mock_log,
        )

        assert result["status"] == "Pass"

    def test_log_called_with_correct_dataframe_name(self, spark) -> None:
        df_cases = party_case_client_details_df(spark)
        df_risk  = risk_model_instance_df(spark)
        mock_log = MagicMock()

        find_missing_clients(
            df_risk, df_cases,
            "Party_RiskModelInstance", "party_case_client_details",
            "SourceClient", "log_name_check",
            log_fn=mock_log,
        )

        logged_df_name = mock_log.call_args[0][0]
        assert logged_df_name == "party_case_client_details"


# ===========================================================================
# TestDirectorshipStructureRule
# ===========================================================================

@pytest.mark.spark
@pytest.mark.gcob_reportingv1
@pytest.mark.dq
class TestDirectorshipStructureRule:
    """
    Tests for the inline violation-detection block in GCOB_Producer_DQ_Test:
    'Do not display anything above Directorship/Authorised Representative
    except other Directorship/Authorised/Other'.
    """

    def _run_rule(self, df: DataFrame) -> tuple[int, str]:
        """Execute the inline rule logic and return (violation_count, status)."""
        allowed_relations = ["Directorship", "Authorised Representative"]
        allowed_extended  = ["Directorship", "Authorised Representative", "Other"]

        parents_with_disallowed = (
            df.filter(~F.col("TypesOfRelation").isin(*allowed_relations))
              .select("ParentIdentity")
              .distinct()
        )

        dir_auth_parents = (
            df.filter(F.col("TypesOfRelation").isin(*allowed_relations))
              .join(parents_with_disallowed, "ParentIdentity", "left_anti")
              .select(
                  F.col("ParentIdentity").alias("DirectAuthParentIdentity"),
                  F.col("ClientStructureSnapshotId").alias("DirectAuthClientStructureSnapshotId"),
                  F.col("ClientGcobId").alias("DirectAuthGcobId"),
                  F.col("ClientCaseId").alias("DirectAuthClientCaseId"),
              )
              .distinct()
        )

        upstream = df.join(
            dir_auth_parents,
            (df.ChildIdentity == dir_auth_parents.DirectAuthParentIdentity) &
            (df.ClientStructureSnapshotId == dir_auth_parents.DirectAuthClientStructureSnapshotId) &
            (df.ClientGcobId == dir_auth_parents.DirectAuthGcobId) &
            (df.ClientCaseId == dir_auth_parents.DirectAuthClientCaseId),
            "inner",
        )

        violations    = upstream.filter(~F.col("TypesOfRelation").isin(*allowed_extended))
        violation_cnt = violations.count()
        status        = "Pass" if violation_cnt == 0 else "Fail"
        return violation_cnt, status

    def test_clean_structure_passes(self, spark) -> None:
        df = party_client_structure_gui_df(spark)
        violation_cnt, status = self._run_rule(df)
        assert status == "Pass", f"Expected Pass but got {violation_cnt} violations"

    def test_illegal_relation_above_directorship_fails(self, spark) -> None:
        """
        Inject a 'Shareholder' relation as the parent of a Directorship node —
        this is a violation because non-allowed types must not appear above Dir/Auth.
        """
        from pyspark.sql.types import (
            BooleanType, StringType, StructField, StructType,
        )

        schema = StructType([
            StructField("SourceClient",                   StringType(), True),
            StructField("UniqueGcobId",                   StringType(), True),
            StructField("ClientGcobId",                   StringType(), True),
            StructField("ClientCaseId",                   StringType(), True),
            StructField("ClientFullLegalName",            StringType(), True),
            StructField("ClientLifeCycleName",            StringType(), True),
            StructField("CaseStatusName",                 StringType(), True),
            StructField("ReviewTypeName",                 StringType(), True),
            StructField("ClientCountryOfRegistration",    StringType(), True),
            StructField("ParentIdentity",                 StringType(), True),
            StructField("ParentIdentityName",             StringType(), True),
            StructField("ParentType",                     StringType(), True),
            StructField("ChildIdentity",                  StringType(), True),
            StructField("ChildIdentityName",              StringType(), True),
            StructField("ChildType",                      StringType(), True),
            StructField("TypesOfRelation",                StringType(), True),
            StructField("UniqueParentPartyId",            StringType(), True),
            StructField("UniqueChildPartyId",             StringType(), True),
            StructField("ClientStructureSnapshotId",      StringType(), True),
            StructField("IsLatestApprovedVersionOfClient", BooleanType(), True),
        ])

        rows = [
            # DIR node: Child=CHILD-DIR is a Directorship of Parent=P1
            ("SC-1","UG-1","GCOB-1","CASE-1","Corp","Client","Completed","PR","NL",
             "P1","Parent","LE","CHILD-DIR","Director","NP",
             "Directorship","UP1","UC1","SNAP-1", True),
            # VIOLATION: P1 is itself a child of GRAND via "UBO" (not allowed above Dir/Auth)
            ("SC-1","UG-1","GCOB-1","CASE-1","Corp","Client","Completed","PR","NL",
             "GRAND","GrandParent","LE","P1","Parent","LE",
             "UBO","UP0","UP1","SNAP-1", True),
        ]

        df = spark.createDataFrame(rows, schema=schema)
        violation_cnt, status = self._run_rule(df)
        assert status == "Fail", f"Expected Fail but got {violation_cnt} violations"
        assert violation_cnt >= 1
