"""
tests/fixtures/sample_dataframes.py
=====================================
Reusable factory functions that return small, realistic PySpark DataFrames
modelled on the actual GDP data objects consumed by these bundles.

Usage in tests
--------------
    from fixtures.sample_dataframes import party_all_party_details_df

    def test_something(spark):
        df = party_all_party_details_df(spark)
        assert df.count() == 3

All factories accept a ``SparkSession`` as their sole argument so that they
always use the same session as the test — never a stray global one.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Schemas — defined once, shared between factory and schema-assertion tests
# ---------------------------------------------------------------------------

PARTY_ALL_PARTY_DETAILS_SCHEMA = StructType([
    StructField("PartyId",                    StringType(),   False),
    StructField("UniquePartyId",              StringType(),   False),
    StructField("GcobId",                     StringType(),   True),
    StructField("GCDSID",                     StringType(),   True),
    StructField("FullLegalName",              StringType(),   True),
    StructField("ClientType",                 StringType(),   True),
    StructField("ClientLifeCycleStatus",      StringType(),   True),
    StructField("CaseStatusName",             StringType(),   True),
    StructField("IsLatestApprovedVersionOfClient", BooleanType(), True),
    StructField("CountryOfRegistration",      StringType(),   True),
])

PARTY_CASE_CLIENT_DETAILS_SCHEMA = StructType([
    StructField("SourceClient",               StringType(),   False),
    StructField("UniqueGcobId",               StringType(),   False),
    StructField("CaseId",                     StringType(),   False),
    StructField("GcobId",                     StringType(),   True),
    StructField("ClientId",                   StringType(),   True),
    StructField("FullLegalName",              StringType(),   True),
    StructField("ClientType",                 StringType(),   True),
    StructField("ClientLifeCycleName",        StringType(),   True),
    StructField("CaseStatusName",             StringType(),   True),
    StructField("ReviewTypeName",             StringType(),   True),
    StructField("FIHubIndicator",             BooleanType(),  True),
    StructField("CaseCompletedDate",          DateType(),     True),
    StructField("ClientOwnerSignOffDate",     DateType(),     True),
    StructField("ValidatedRiskLevel",         StringType(),   True),
    StructField("CountryOfRegistration",      StringType(),   True),
    StructField("RiskModelName",              StringType(),   True),
    StructField("EntityTypeRiskLevel",        StringType(),   True),
    StructField("StructureRiskLevel",         StringType(),   True),
    StructField("GlobalClientOwnerLocation",  StringType(),   True),
])

PARTY_BUSINESS_ACTIVITIES_SCHEMA = StructType([
    StructField("SourceClient",   StringType(), False),
    StructField("ClientId",       StringType(), True),
    StructField("NaicsCode",      StringType(), True),
    StructField("NaicsName",      StringType(), True),
    StructField("SectorGroup",    StringType(), True),
    StructField("CaseStatusName", StringType(), True),
])

PARTY_CLIENT_STRUCTURE_GUI_SCHEMA = StructType([
    StructField("SourceClient",                   StringType(),  False),
    StructField("UniqueGcobId",                   StringType(),  True),
    StructField("ClientGcobId",                   StringType(),  True),
    StructField("ClientCaseId",                   StringType(),  True),
    StructField("ClientFullLegalName",            StringType(),  True),
    StructField("ClientLifeCycleName",            StringType(),  True),
    StructField("CaseStatusName",                 StringType(),  True),
    StructField("ReviewTypeName",                 StringType(),  True),
    StructField("ClientCountryOfRegistration",    StringType(),  True),
    StructField("ParentIdentity",                 StringType(),  True),
    StructField("ParentIdentityName",             StringType(),  True),
    StructField("ParentType",                     StringType(),  True),
    StructField("ChildIdentity",                  StringType(),  True),
    StructField("ChildIdentityName",              StringType(),  True),
    StructField("ChildType",                      StringType(),  True),
    StructField("TypesOfRelation",                StringType(),  True),
    StructField("UniqueParentPartyId",            StringType(),  True),
    StructField("UniqueChildPartyId",             StringType(),  True),
    StructField("ClientStructureSnapshotId",      StringType(),  True),
    StructField("IsLatestApprovedVersionOfClient", BooleanType(), True),
])

PARTY_WORKITEM_SCHEMA = StructType([
    StructField("SourceClient",       StringType(), False),
    StructField("GcobId",             StringType(), True),
    StructField("WorkItemId",         StringType(), True),
    StructField("ClientId",           StringType(), True),
    StructField("CaseStatusName",     StringType(), True),
    StructField("ReviewTypeName",     StringType(), True),
    StructField("ClientLifeCycleName", StringType(), True),
])

RISK_MODEL_INSTANCE_SCHEMA = StructType([
    StructField("SourceClient",    StringType(), True),
    StructField("InstanceId",      StringType(), True),
    StructField("RiskModelName",   StringType(), True),
])

GNS_FILE_SCHEMA = StructType([
    StructField("UniquePartyId",               StringType(), False),
    StructField("ListUid",                     StringType(), True),
    StructField("ClientStructureSnapshotId",   StringType(), True),
])

GCOB_PARTY_SCHEMA = StructType([
    StructField("UniquePartyId",          StringType(), False),
    StructField("GcobId",                 StringType(), True),
    StructField("FullLegalName",          StringType(), True),
    StructField("ClientType",             StringType(), True),
    StructField("CaseStatusName",         StringType(), True),
    StructField("ClientLifeCycleStatus",  StringType(), True),
])


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------

def party_all_party_details_df(spark: SparkSession) -> DataFrame:
    """
    Three-row sample of ``party_AllPartyDetails``.

    Covers:
    - active LEC client (Completed case, Client lifecycle)
    - active NP client
    - inactive / prospect record (should be filtered out in most queries)
    """
    rows = [
        (
            "PARTY-001", "UNIQ-001", "GCOB-001", "GCDS-001",
            "Acme Corp NV", "LegalEntityClient", "Client",
            "Completed", True, "NL",
        ),
        (
            "PARTY-002", "UNIQ-002", "GCOB-002", None,
            "John Doe", "NaturalPersonClient", "Client",
            "Completed", True, "DE",
        ),
        (
            "PARTY-003", "UNIQ-003", "GCOB-003", "GCDS-003",
            "Prospect Ltd", "LegalEntityClient", "Prospect",
            "InProgress", False, "US",
        ),
    ]
    return spark.createDataFrame(rows, schema=PARTY_ALL_PARTY_DETAILS_SCHEMA)


def party_case_client_details_df(spark: SparkSession) -> DataFrame:
    """
    Four-row sample of ``party_case_client_details``.

    Covers:
    - two completed LE clients (different risk models)
    - one completed NP client
    - one in-progress record (edge case for null checks)
    """
    today = date.today()
    rows = [
        (
            "SC-001", "UG-001", "CASE-001", "GCOB-001", "CLI-001",
            "Acme Corp NV", "Legal Entity", "Client", "Completed", "Periodic Review",
            True, today, today, "High", "NL", "Corporate Client Risk Model 2020 (Phase 2) (Review)",
            "Medium", "Low", "Rabobank Netherlands",
        ),
        (
            "SC-002", "UG-002", "CASE-002", "GCOB-002", "CLI-002",
            "Beta Industries BV", "Legal Entity", "Client", "Completed", "Onboarding",
            False, today, today, "Low", "DE", "Rabobank Subsidiaries Risk Model",
            None, None, "Rabobank Frankfurt",
        ),
        (
            "SC-003", "UG-003", "CASE-003", "GCOB-003", "CLI-003",
            "Jane Smith", "Natural Person", "Client", "Completed", "Periodic Review",
            False, today, today, "Low", "NL",
            "NPPC Risk Model 2023 (Review)", "Low", "Low", "Rabobank Netherlands",
        ),
        (
            "SC-004", "UG-004", "CASE-004", "GCOB-004", "CLI-004",
            "Gamma Corp", "Legal Entity", "Client", "InProgress", "Onboarding",
            False, None, None, None, "FR", None, None, None, "Rabobank Paris",
        ),
    ]
    return spark.createDataFrame(rows, schema=PARTY_CASE_CLIENT_DETAILS_SCHEMA)


def party_business_activities_df(spark: SparkSession) -> DataFrame:
    """Two-row sample of ``party_business_activities`` — unique SourceClient+NaicsCode."""
    rows = [
        ("SC-001", "CLI-001", "1111", "Soybean Farming",      "Agriculture", "Completed"),
        ("SC-002", "CLI-002", "5221", "Commercial Banking",   "Finance",     "Completed"),
    ]
    return spark.createDataFrame(rows, schema=PARTY_BUSINESS_ACTIVITIES_SCHEMA)


def party_business_activities_with_duplicates_df(spark: SparkSession) -> DataFrame:
    """Intentional duplicate SourceClient+NaicsCode — used to test uniqueness failures."""
    rows = [
        ("SC-001", "CLI-001", "1111", "Soybean Farming",    "Agriculture", "Completed"),
        ("SC-001", "CLI-001", "1111", "Soybean Farming",    "Agriculture", "Completed"),  # duplicate
        ("SC-002", "CLI-002", "5221", "Commercial Banking", "Finance",     "Completed"),
    ]
    return spark.createDataFrame(rows, schema=PARTY_BUSINESS_ACTIVITIES_SCHEMA)


def party_workitem_df(spark: SparkSession) -> DataFrame:
    """Two-row sample of ``party_workitem``."""
    rows = [
        ("SC-001", "GCOB-001", "WI-001", "CLI-001", "Completed", "Periodic Review", "Client"),
        ("SC-002", "GCOB-002", "WI-002", "CLI-002", "Completed", "Onboarding",      "Client"),
    ]
    return spark.createDataFrame(rows, schema=PARTY_WORKITEM_SCHEMA)


def party_client_structure_gui_df(spark: SparkSession) -> DataFrame:
    """
    Four-row sample of ``party_client_structure_GUI``.
    Covers parent-child hierarchy with Directorship and Ownership relations.
    """
    rows = [
        (
            "SC-001", "UG-001", "GCOB-001", "CASE-001",
            "Acme Corp NV", "Client", "Completed", "Periodic Review", "NL",
            "PARENT-001", "Parent Corp", "LegalEntityClient",
            "CHILD-001",  "Acme Corp NV", "LegalEntityClient",
            "Ownership", "UNIQ-PARENT-001", "UNIQ-001", "SNAP-001", True,
        ),
        (
            "SC-001", "UG-001", "GCOB-001", "CASE-001",
            "Acme Corp NV", "Client", "Completed", "Periodic Review", "NL",
            "PARENT-001", "Parent Corp", "LegalEntityClient",
            "CHILD-002",  "Director Person", "NaturalPersonClient",
            "Directorship", "UNIQ-PARENT-001", "UNIQ-002", "SNAP-001", True,
        ),
        (
            "SC-002", "UG-002", "GCOB-002", "CASE-002",
            "Beta BV", "Client", "Completed", "Onboarding", "DE",
            "PARENT-002", "Beta Holding", "LegalEntityClient",
            "CHILD-003",  "Beta BV", "LegalEntityClient",
            "Shareholder", "UNIQ-PARENT-002", "UNIQ-003", "SNAP-002", True,
        ),
        (
            "SC-003", "UG-003", "GCOB-003", "CASE-003",
            "Gamma Corp", "Client", "Completed", "Periodic Review", "FR",
            "PARENT-003", "Gamma Holding", "LegalEntityClient",
            "CHILD-004",  "Gamma Corp", "LegalEntityClient",
            "Authorised Representative", "UNIQ-PARENT-003", "UNIQ-004", "SNAP-003", True,
        ),
    ]
    return spark.createDataFrame(rows, schema=PARTY_CLIENT_STRUCTURE_GUI_SCHEMA)


def risk_model_instance_df(spark: SparkSession) -> DataFrame:
    """Two-row sample of ``Party_RiskModelInstance``."""
    rows = [
        ("SC-001", "INST-001", "Corporate Client Risk Model 2020 (Phase 2) (Review)"),
        ("SC-002", "INST-002", "Rabobank Subsidiaries Risk Model"),
    ]
    return spark.createDataFrame(rows, schema=RISK_MODEL_INSTANCE_SCHEMA)


def gns_file_df(spark: SparkSession) -> DataFrame:
    """
    Small GNS output file sample — used in GNS validation tests.
    Two entries with unique ListUid + SnapshotId.
    """
    rows = [
        ("UNIQ-001", "GCDS-001", "SNAP-001"),
        ("UNIQ-002", "LE_GCOB-002", "SNAP-002"),
    ]
    return spark.createDataFrame(rows, schema=GNS_FILE_SCHEMA)


def gns_file_with_null_listuid_df(spark: SparkSession) -> DataFrame:
    """GNS file where one row has a NULL ListUid — triggers NULL check failure."""
    rows = [
        ("UNIQ-001", "GCDS-001",  "SNAP-001"),
        ("UNIQ-002", None,         "SNAP-002"),   # NULL ListUid
    ]
    return spark.createDataFrame(rows, schema=GNS_FILE_SCHEMA)


def gns_file_with_duplicate_listuid_df(spark: SparkSession) -> DataFrame:
    """GNS file with duplicate (ListUid, SnapshotId) — triggers duplicate check failure."""
    rows = [
        ("UNIQ-001", "GCDS-001", "SNAP-001"),
        ("UNIQ-001", "GCDS-001", "SNAP-001"),   # exact duplicate
        ("UNIQ-002", "GCDS-002", "SNAP-002"),
    ]
    return spark.createDataFrame(rows, schema=GNS_FILE_SCHEMA)


def gcob_party_df(spark: SparkSession) -> DataFrame:
    """Sample GCOB Consumer Party output — used to test write_to_unity_catalog path."""
    rows = [
        ("UNIQ-001", "GCOB-001", "Acme Corp NV",      "LegalEntityClient",   "Completed", "Client"),
        ("UNIQ-002", "GCOB-002", "John Doe",           "NaturalPersonClient", "Completed", "Client"),
        ("UNIQ-003", "GCOB-003", "Prospect Ltd",       "LegalEntityClient",   "InProgress", "Prospect"),
    ]
    return spark.createDataFrame(rows, schema=GCOB_PARTY_SCHEMA)
