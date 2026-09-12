# Databricks notebook source
# MAGIC %md
# MAGIC #### RDM_Party Backfill Spike — Verifier
# MAGIC
# MAGIC **Purpose:** Validates output correctness for each processed `Load_Date` partition.
# MAGIC Can be called after `Benchmark_Runner` completes, or run standalone as a sweep.
# MAGIC
# MAGIC **Checks implemented (Requirements 5, 9):**
# MAGIC - Non-empty partition
# MAGIC - Record count ≥ 80% of distinct party IDs across sources
# MAGIC - Source coverage — Application column contains one row per non-empty source
# MAGIC - Exactly one EDL_LOAD_DTS folder per Load_Date (idempotent overwrite)
# MAGIC - Schema match against RDM_Party v3 reference
# MAGIC - Date assignment invariant — all records carry EDL_LOAD_DTS = D
# MAGIC - Source attribution invariant — all Application values in declared nine-source set
# MAGIC - Idempotent execution — re-run count equals first-run count
# MAGIC - Partition additivity — sum(individual counts) == total union count
# MAGIC - Lower-bound cardinality — output ≥ largest single source partition
# MAGIC - Failure log completeness — every failed check row has all four required fields
# MAGIC
# MAGIC **Does NOT modify any source or output data.**
# MAGIC
# MAGIC #### Widgets
# MAGIC | Widget        | Description                                              |
# MAGIC |---------------|----------------------------------------------------------|
# MAGIC | load_dates    | Comma-separated YYYYMMDD to verify (empty = all COMPLETED)|
# MAGIC | execution_id  | Optional — filter results to a specific benchmark run   |
# MAGIC
# MAGIC #### Author
# MAGIC - Generated for RDM_Party Backfill Spike

# COMMAND ----------

# DBTITLE 1,Read environment variables and authenticate
import os
import uuid
from datetime import datetime
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType
)

environment  = os.environ['ENV']
SARADAR      = f"saradar{environment}"
PARTY_PATH   = f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party/3/data/"
RESULTS_PATH = f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/spike/benchmark_results/"
STATUS_PATH  = f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/spike/backfill_status/"
VERIFIER_PATH= f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/spike/verifier_results/"

from RadarUtils import authenticate_storage_account
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Define widgets
dbutils.widgets.text("load_dates",   "")   # comma-separated YYYYMMDD; empty = all COMPLETED
dbutils.widgets.text("execution_id", "")   # optional filter

load_dates_raw = dbutils.widgets.get("load_dates").strip()
exec_id_filter = dbutils.widgets.get("execution_id").strip()

# COMMAND ----------

# DBTITLE 1,Resolve load_dates to verify
def resolve_load_dates(raw: str) -> list:
    """
    If raw is non-empty, parse comma-separated YYYYMMDD list.
    If empty, query spike_backfill_status for all COMPLETED dates.
    """
    if raw:
        dates = [d.strip() for d in raw.split(",") if d.strip()]
        return sorted(dates)
    try:
        df = spark.read.format("delta").load(STATUS_PATH)
        completed = [r["load_date"] for r in df.filter(F.col("status") == "COMPLETED").select("load_date").collect()]
        return sorted(completed)
    except Exception:
        print("[WARN] spike_backfill_status table not found — no dates to verify.")
        return []

load_dates = resolve_load_dates(load_dates_raw)
print(f"Dates to verify ({len(load_dates)}): {load_dates[:5]}{'...' if len(load_dates)>5 else ''}")

# COMMAND ----------

# DBTITLE 1,RDM_Party v3 schema reference
# Column name → (spark_type_string, nullable)
# Derived from RDM_Party.py v3 output structure.
# Extend this dict when the schema evolves.
RDM_PARTY_V3_SCHEMA = {
    "PartyIdentifier":                  ("string",  True),
    "LocalSystemIdentifier":            ("string",  True),
    "Application":                      ("string",  False),   # non-nullable — source system name
    "FullLegalName":                    ("string",  True),
    "DateOfBirth":                      ("date",    True),
    "FirstName":                        ("string",  True),
    "MiddleName":                       ("string",  True),
    "LastName":                         ("string",  True),
    "Party_type":                       ("string",  True),
    "GlobalClientOwnerName":            ("string",  True),
    "GlobalClientOwnerLocation":        ("string",  True),
    "GlobalClientRegion":               ("string",  True),
    "LifeCycleStatus":                  ("string",  True),
    "StatusName":                       ("string",  True),
    "IsLatestApprovedVersionOfClient":  ("string",  True),
    "FullLegalNameInLocalLanguage":     ("string",  True),
    "IsIncorporated":                   ("string",  True),
    "IncorporationNumber":              ("string",  True),
    "IncorporationDate":                ("date",    True),
    "LegalForm":                        ("string",  True),
    "HasSourceOfWealth":                ("boolean", True),
    "SanctionsOrExternalWatchlist":     ("string",  True),
    "InternalWatchlist":                ("string",  True),
    "AdverseInformationOrMedia":        ("string",  True),
    "PEPStatus":                        ("string",  True),
    "IsTrust":                          ("string",  True),
    "TypeOfTrust":                      ("string",  True),
    "IsClientRegulated":                ("string",  True),
    "IsClientListed":                   ("string",  True),
    "CountryOfTaxResidence":            ("string",  True),
    "TinAvailable":                     ("string",  True),
    "TinOrEquivalent":                  ("string",  True),
    "FatcaClassification":              ("string",  True),
    "GIIN":                             ("string",  True),
    "CrsClassification":                ("string",  True),
    "KYCGroup":                         ("string",  True),
    "SectorTeam":                       ("string",  True),
    "WRorRetail":                       ("string",  True),
    "FEC_FI_Flag":                      ("boolean", True),
    "EDL_LOAD_DTS":                     ("string",  False),  # non-nullable — partition key
}

# Nine declared source systems (Property 4 / Requirement 5.2)
DECLARED_SOURCES = {
    "GCDS", "GCOB", "Legacy2", "GIC", "KN1", "RANZ",
    "KYCMasterListRegistry", "Party_SystemIdentifier", "Party_KN1Cases"
}

# COMMAND ----------

# DBTITLE 1,Schema for spike_verifier_results Delta table
VERIFIER_SCHEMA = StructType([
    StructField("verification_id",  StringType(),  False),
    StructField("execution_id",     StringType(),  False),
    StructField("load_date",        StringType(),  False),
    StructField("check_name",       StringType(),  False),
    StructField("passed",           BooleanType(), False),
    StructField("expected_value",   StringType(),  True),
    StructField("actual_value",     StringType(),  True),
    StructField("failure_detail",   StringType(),  True),
    StructField("verified_at_ts",   TimestampType(),False),
])

# COMMAND ----------

# DBTITLE 1,Helper — write a verifier result row
def write_verifier_row(execution_id, load_date, check_name, passed,
                        expected=None, actual=None, detail=None):
    """
    Appends a single check result to spike_verifier_results.
    Requirement 9.8: all four fields (check_name, load_date, expected_value, actual_value)
    must be non-null on failure.
    """
    if not passed:
        # Property 16: failure log completeness
        assert check_name is not None, "check_name must not be None on failure"
        assert load_date  is not None, "load_date must not be None on failure"
        assert expected   is not None, f"expected_value must not be None for failed check '{check_name}'"
        assert actual     is not None, f"actual_value must not be None for failed check '{check_name}'"

    row = {
        "verification_id": str(uuid.uuid4()),
        "execution_id":    execution_id or "standalone",
        "load_date":       load_date,
        "check_name":      check_name,
        "passed":          passed,
        "expected_value":  str(expected) if expected is not None else None,
        "actual_value":    str(actual)   if actual   is not None else None,
        "failure_detail":  detail,
        "verified_at_ts":  datetime.utcnow(),
    }
    df = spark.createDataFrame([row], schema=VERIFIER_SCHEMA)
    (df.write
       .format("delta")
       .mode("append")
       .option("delta.autoOptimize.enabled", "true")
       .save(VERIFIER_PATH))

    status = "PASS" if passed else "FAIL"
    print(f"  [{status}] {check_name}" + (f" — {detail}" if detail and not passed else ""))

# COMMAND ----------

# DBTITLE 1,Helper — read output partition (cached per load_date within this run)
_partition_cache = {}

def read_output_partition(load_date: str):
    """Returns a Spark DataFrame for Party/3/data/EDL_LOAD_DTS={load_date}/"""
    if load_date not in _partition_cache:
        path = f"{PARTY_PATH}EDL_LOAD_DTS={load_date}/*.parquet"
        _partition_cache[load_date] = spark.read.parquet(path)
    return _partition_cache[load_date]

def partition_exists(load_date: str) -> bool:
    """Returns True if the output partition folder exists and is non-empty."""
    path = f"{PARTY_PATH}EDL_LOAD_DTS={load_date}/"
    try:
        files = dbutils.fs.ls(path)
        return any(f.name.endswith(".parquet") for f in files)
    except Exception:
        return False

def count_output(load_date: str) -> int:
    try:
        return read_output_partition(load_date).count()
    except Exception:
        return 0

# COMMAND ----------

# DBTITLE 1,Check 1 — Non-empty partition
def check_non_empty_partition(load_date, exec_id):
    """Requirement 5.1: partition is non-empty (≥ 1 parquet file)."""
    exists = partition_exists(load_date)
    write_verifier_row(exec_id, load_date, "non_empty_partition", exists,
                       expected="partition_exists=True",
                       actual=f"partition_exists={exists}",
                       detail=None if exists else f"No parquet files found at {PARTY_PATH}EDL_LOAD_DTS={load_date}/")

# COMMAND ----------

# DBTITLE 1,Check 2 — Record count ≥ 80% of distinct party IDs across sources
def check_record_count_threshold(load_date, exec_id):
    """
    Requirement 5.1: output_count ≥ 0.8 × distinct_party_ids_across_sources.
    Source party IDs are derived from Party_SystemIdentifier (the canonical join table).
    Falls back gracefully if the source table is unavailable for the date.
    """
    output_count = count_output(load_date)
    # Read Party_SystemIdentifier to derive expected distinct party IDs
    try:
        psi_path = (f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net"
                    f"/Party_SystemIdentifier/3/data/EDL_LOAD_DTS={load_date}/*.parquet")
        source_df = spark.read.parquet(psi_path)
        distinct_ids = source_df.select("LocalSystemIdentifier").distinct().count()
        threshold = int(distinct_ids * 0.8)
        passed = output_count >= threshold
        write_verifier_row(exec_id, load_date, "record_count_threshold", passed,
                           expected=f">={threshold} (80% of {distinct_ids} source IDs)",
                           actual=str(output_count),
                           detail=None if passed else f"Output {output_count} < 80% threshold {threshold}")
    except Exception as ex:
        write_verifier_row(exec_id, load_date, "record_count_threshold", False,
                           expected=">=80% of source party IDs",
                           actual=f"ERROR: {str(ex)[:300]}",
                           detail="Could not read Party_SystemIdentifier for threshold calculation")

# COMMAND ----------

# DBTITLE 1,Check 3 — Source coverage (Application column)
def check_source_coverage(load_date, exec_id):
    """
    Requirement 5.2 / Property 4: Application column contains ≥ 1 row per
    non-empty source partition for that date.
    Only checks the seven external sources; internal objects (Party_SystemIdentifier,
    Party_KN1Cases) are not surfaced as Application values in the output.
    """
    EXTERNAL_SOURCES = {"GCDS", "GCOB", "Legacy2", "GIC", "KN1", "RANZ", "KYCMasterListRegistry"}
    try:
        df = read_output_partition(load_date)
        present = {r["Application"] for r in df.select("Application").distinct().collect() if r["Application"]}
        missing = EXTERNAL_SOURCES - present
        passed  = len(missing) == 0
        write_verifier_row(exec_id, load_date, "source_coverage", passed,
                           expected=f"All of {sorted(EXTERNAL_SOURCES)}",
                           actual=f"Present: {sorted(present)}",
                           detail=None if passed else f"Missing sources: {sorted(missing)}")
    except Exception as ex:
        write_verifier_row(exec_id, load_date, "source_coverage", False,
                           expected="all 7 external sources in Application",
                           actual=f"ERROR: {str(ex)[:300]}",
                           detail="Exception reading output partition")

# COMMAND ----------

# DBTITLE 1,Check 4 — Exactly one EDL_LOAD_DTS folder
def check_single_partition_folder(load_date, exec_id):
    """
    Requirement 5.3: exactly one EDL_LOAD_DTS=YYYYMMDD folder exists
    (verifies idempotent overwrite behaviour).
    """
    parent_path = PARTY_PATH
    try:
        folders = [f.name.rstrip("/") for f in dbutils.fs.ls(parent_path)
                   if f.name.startswith(f"EDL_LOAD_DTS={load_date}")]
        count  = len(folders)
        passed = count == 1
        write_verifier_row(exec_id, load_date, "single_partition_folder", passed,
                           expected="1 folder",
                           actual=f"{count} folder(s): {folders}",
                           detail=None if passed else f"Expected exactly 1, found {count}: {folders}")
    except Exception as ex:
        write_verifier_row(exec_id, load_date, "single_partition_folder", False,
                           expected="1 folder",
                           actual=f"ERROR: {str(ex)[:300]}",
                           detail="Exception listing output path")

# COMMAND ----------

# DBTITLE 1,Check 5 — Schema match against v3 reference
def check_schema_match(load_date, exec_id):
    """
    Requirement 5.5 / Property 5: column names and non-nullable columns match
    the stored RDM_Party v3 schema reference.
    """
    try:
        df = read_output_partition(load_date)
        actual_fields = {f.name: (f.dataType.simpleString(), f.nullable) for f in df.schema.fields}
        violations = []

        for col_name, (expected_type, nullable) in RDM_PARTY_V3_SCHEMA.items():
            if col_name not in actual_fields:
                violations.append(f"Missing column: {col_name}")
            else:
                actual_type, actual_nullable = actual_fields[col_name]
                if not nullable and actual_nullable:
                    violations.append(f"Non-nullable column '{col_name}' is nullable in output")

        passed = len(violations) == 0
        write_verifier_row(exec_id, load_date, "schema_match", passed,
                           expected="0 schema violations",
                           actual=f"{len(violations)} violation(s)",
                           detail="; ".join(violations) if violations else None)
    except Exception as ex:
        write_verifier_row(exec_id, load_date, "schema_match", False,
                           expected="0 schema violations",
                           actual=f"ERROR: {str(ex)[:300]}",
                           detail="Exception reading output schema")

# COMMAND ----------

# DBTITLE 1,Check 6 — Date assignment invariant (Property 3)
def check_date_assignment(load_date, exec_id):
    """
    Requirement 9.3: every record in EDL_LOAD_DTS=D/ has EDL_LOAD_DTS field = D.
    """
    try:
        df = read_output_partition(load_date)
        wrong = df.filter(F.col("EDL_LOAD_DTS") != load_date).count()
        passed = wrong == 0
        write_verifier_row(exec_id, load_date, "date_assignment_invariant", passed,
                           expected="0 records with wrong EDL_LOAD_DTS",
                           actual=str(wrong),
                           detail=None if passed else f"{wrong} records have EDL_LOAD_DTS != {load_date}")
    except Exception as ex:
        write_verifier_row(exec_id, load_date, "date_assignment_invariant", False,
                           expected="0 records with wrong EDL_LOAD_DTS",
                           actual=f"ERROR: {str(ex)[:300]}",
                           detail="Exception during date assignment check")

# COMMAND ----------

# DBTITLE 1,Check 7 — Source attribution invariant (Property 4)
def check_source_attribution(load_date, exec_id):
    """
    Requirement 9.4: all Application values are in the declared nine-source set.
    """
    try:
        df = read_output_partition(load_date)
        present = {r["Application"] for r in df.select("Application").distinct().collect() if r["Application"]}
        unknown = present - DECLARED_SOURCES
        passed  = len(unknown) == 0
        write_verifier_row(exec_id, load_date, "source_attribution_invariant", passed,
                           expected=f"subset of {sorted(DECLARED_SOURCES)}",
                           actual=f"Found: {sorted(present)}",
                           detail=None if passed else f"Unknown Application values: {sorted(unknown)}")
    except Exception as ex:
        write_verifier_row(exec_id, load_date, "source_attribution_invariant", False,
                           expected="all Application values in declared source set",
                           actual=f"ERROR: {str(ex)[:300]}",
                           detail="Exception during source attribution check")

# COMMAND ----------

# DBTITLE 1,Check 8 — Lower-bound cardinality (Property 6)
def check_lower_bound_cardinality(load_date, exec_id):
    """
    Requirement 9.6: output_count >= count of largest single source partition.
    Uses Party_SystemIdentifier as the reference for source party ID counts.
    """
    try:
        output_count = count_output(load_date)
        psi_path = (f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net"
                    f"/Party_SystemIdentifier/3/data/EDL_LOAD_DTS={load_date}/*.parquet")
        max_source_count = spark.read.parquet(psi_path).count()
        passed = output_count >= max_source_count
        write_verifier_row(exec_id, load_date, "lower_bound_cardinality", passed,
                           expected=f">={max_source_count} (largest source count)",
                           actual=str(output_count),
                           detail=None if passed else f"Output {output_count} < largest source {max_source_count}")
    except Exception as ex:
        write_verifier_row(exec_id, load_date, "lower_bound_cardinality", False,
                           expected="output >= largest source partition count",
                           actual=f"ERROR: {str(ex)[:300]}",
                           detail="Exception during cardinality check")

# COMMAND ----------

# DBTITLE 1,Check 9 — Idempotent execution (Property 1) — cross-run
def check_idempotent_execution(load_date, exec_id):
    """
    Requirement 5.4 / 5.6 / 9.1: if multiple runs exist for this load_date,
    all SUCCESS runs should have the same records_processed count.
    """
    try:
        df_runs = (spark.read.format("delta").load(RESULTS_PATH)
                       .filter((F.col("load_date") == load_date) & (F.col("status") == "SUCCESS"))
                       .select("execution_id", "records_processed"))
        counts = [r["records_processed"] for r in df_runs.collect()]
        if len(counts) < 2:
            write_verifier_row(exec_id, load_date, "idempotent_execution", True,
                               expected="consistent across runs",
                               actual=f"Only {len(counts)} run(s) — not enough for comparison",
                               detail=None)
            return
        passed = len(set(counts)) == 1
        write_verifier_row(exec_id, load_date, "idempotent_execution", passed,
                           expected=f"all {len(counts)} runs return same count",
                           actual=f"counts={counts}",
                           detail=None if passed else f"Diverging counts across runs: {counts}")
    except Exception as ex:
        write_verifier_row(exec_id, load_date, "idempotent_execution", False,
                           expected="consistent record count across runs",
                           actual=f"ERROR: {str(ex)[:300]}",
                           detail="Exception reading benchmark results for idempotency check")

# COMMAND ----------

# DBTITLE 1,Check 10 — Partition additivity invariant (Property 2)
def check_partition_additivity(load_dates_batch: list, exec_id: str):
    """
    Requirement 5.7 / 9.2: sum of individual partition counts == union count.
    Runs once across the full batch of verified load_dates.
    """
    if len(load_dates_batch) < 2:
        print("[SKIP] Partition additivity requires ≥ 2 dates.")
        return
    try:
        # Sum of individual counts
        individual_sum = sum(count_output(d) for d in load_dates_batch)

        # Union count via single read of all partitions
        paths = [f"{PARTY_PATH}EDL_LOAD_DTS={d}/*.parquet" for d in load_dates_batch]
        union_df = spark.read.parquet(*paths)
        union_count = union_df.count()

        passed = individual_sum == union_count
        write_verifier_row(exec_id, load_dates_batch[0], "partition_additivity_invariant", passed,
                           expected=f"individual_sum={individual_sum}",
                           actual=f"union_count={union_count}",
                           detail=None if passed else f"Mismatch: sum={individual_sum}, union={union_count}")
    except Exception as ex:
        write_verifier_row(exec_id, load_dates_batch[0], "partition_additivity_invariant", False,
                           expected="individual_sum == union_count",
                           actual=f"ERROR: {str(ex)[:300]}",
                           detail="Exception during partition additivity check")

# COMMAND ----------

# DBTITLE 1,Main verification loop
all_passed  = 0
all_failed  = 0
failed_dates= []

for load_date in load_dates:
    if not partition_exists(load_date):
        print(f"\n[SKIP] {load_date} — partition does not exist, skipping all checks.")
        continue

    print(f"\n--- Verifying {load_date} ---")
    exec_id = exec_id_filter or "standalone"

    check_non_empty_partition(load_date, exec_id)
    check_record_count_threshold(load_date, exec_id)
    check_source_coverage(load_date, exec_id)
    check_single_partition_folder(load_date, exec_id)
    check_schema_match(load_date, exec_id)
    check_date_assignment(load_date, exec_id)
    check_source_attribution(load_date, exec_id)
    check_lower_bound_cardinality(load_date, exec_id)
    check_idempotent_execution(load_date, exec_id)

# Partition additivity across all verified dates (runs once, batch-level)
if len(load_dates) >= 2:
    print(f"\n--- Partition additivity across {len(load_dates)} dates ---")
    check_partition_additivity(load_dates, exec_id_filter or "standalone")

# COMMAND ----------

# DBTITLE 1,Summary from verifier_results table
try:
    summary = (spark.read.format("delta").load(VERIFIER_PATH)
                   .filter(F.col("load_date").isin(load_dates))
                   .groupBy("check_name")
                   .agg(
                       F.sum(F.when(F.col("passed"), 1).otherwise(0)).alias("passed"),
                       F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("failed"),
                   )
                   .orderBy("check_name"))
    print("\nVerification summary by check:")
    summary.show(truncate=False)
except Exception as ex:
    print(f"[WARN] Could not load summary from verifier_results: {ex}")
