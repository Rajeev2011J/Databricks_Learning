# Databricks notebook source
# MAGIC %md
# MAGIC #### RDM_Party Backfill Spike — Monitor
# MAGIC
# MAGIC **Purpose:** Maintains and displays the per-`Load_Date` status view for the full
# MAGIC backfill range. Enables the team to identify FAILED or PENDING dates and trigger
# MAGIC targeted re-runs by resubmitting `Benchmark_Runner` with the failed `Load_Date`.
# MAGIC
# MAGIC **Requirements covered:** 6.5 (monitoring approach), design Property 13 (status validity)
# MAGIC
# MAGIC **Status lifecycle:**
# MAGIC ```
# MAGIC PENDING → IN_PROGRESS  (Benchmark_Runner starts)
# MAGIC IN_PROGRESS → COMPLETED (Verifier all-pass)
# MAGIC IN_PROGRESS → FAILED    (execution error or Verifier failure)
# MAGIC FAILED → IN_PROGRESS    (re-submission)
# MAGIC ```
# MAGIC
# MAGIC #### Widgets
# MAGIC | Widget        | Description                                               |
# MAGIC |---------------|-----------------------------------------------------------|
# MAGIC | action        | `init` (seed PENDING rows), `status` (show dashboard), `rerun_list` (print FAILED dates) |
# MAGIC | date_from     | Start of backfill range YYYYMMDD (default: 20250101)      |
# MAGIC | date_to       | End of backfill range YYYYMMDD (default: today)           |
# MAGIC
# MAGIC #### Author
# MAGIC - Generated for RDM_Party Backfill Spike

# COMMAND ----------

# DBTITLE 1,Read environment variables
import os
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

environment  = os.environ['ENV']
SARADAR      = f"saradar{environment}"
STATUS_PATH  = f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/spike/backfill_status/"
RESULTS_PATH = f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/spike/benchmark_results/"

from RadarUtils import authenticate_storage_account
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Define widgets
dbutils.widgets.text("action",    "status")       # init | status | rerun_list
dbutils.widgets.text("date_from", "20250101")
dbutils.widgets.text("date_to",   datetime.today().strftime("%Y%m%d"))

action    = dbutils.widgets.get("action").strip().lower()
date_from = dbutils.widgets.get("date_from").strip()
date_to   = dbutils.widgets.get("date_to").strip()

assert action in ("init", "status", "rerun_list"), f"Unknown action '{action}'"
print(f"Action: {action} | Range: {date_from} → {date_to}")

# COMMAND ----------

# DBTITLE 1,Schema and constants
STATUS_SCHEMA = StructType([
    StructField("load_date",       StringType(),   False),
    StructField("status",          StringType(),   False),
    StructField("last_updated_ts", TimestampType(),False),
    StructField("execution_id",    StringType(),   True),
    StructField("failure_reason",  StringType(),   True),
])

VALID_STATUSES = {"PENDING", "IN_PROGRESS", "COMPLETED", "FAILED"}

def all_dates_in_range(date_from: str, date_to: str) -> list:
    """Returns a sorted list of YYYYMMDD strings from date_from to date_to (inclusive)."""
    start = datetime.strptime(date_from, "%Y%m%d")
    end   = datetime.strptime(date_to,   "%Y%m%d")
    days  = (end - start).days + 1
    return [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range(days)]

# COMMAND ----------

# DBTITLE 1,Property 13 — validate all status values in table are in VALID_STATUSES
def validate_status_table():
    """
    Property 13: every load_date entry must have a status in {PENDING, IN_PROGRESS, COMPLETED, FAILED}.
    Raises AssertionError if any out-of-vocabulary value is found.
    """
    try:
        df = spark.read.format("delta").load(STATUS_PATH)
        invalid = (df.filter(~F.col("status").isin(list(VALID_STATUSES)) | F.col("status").isNull())
                     .select("load_date", "status"))
        invalid_count = invalid.count()
        assert invalid_count == 0, (
            f"Property 13 VIOLATED: {invalid_count} rows have invalid status values:\n"
            + str(invalid.collect())
        )
        print(f"[OK] Property 13 — All {df.count()} status entries are valid.")
    except Exception as ex:
        if "Path does not exist" in str(ex) or "is not a Delta table" in str(ex):
            print("[INFO] Status table does not exist yet — run action=init first.")
        else:
            raise

# COMMAND ----------

# DBTITLE 1,ACTION: init — seed PENDING rows for the full backfill range
if action == "init":
    dates = all_dates_in_range(date_from, date_to)
    print(f"Seeding {len(dates)} PENDING entries ({date_from} → {date_to})...")

    rows = [{
        "load_date":       d,
        "status":          "PENDING",
        "last_updated_ts": datetime.utcnow(),
        "execution_id":    None,
        "failure_reason":  None,
    } for d in dates]

    df_seed = spark.createDataFrame(rows, schema=STATUS_SCHEMA)

    # Only write rows that don't already exist (to avoid resetting IN_PROGRESS / COMPLETED)
    try:
        existing_dates = set(
            r["load_date"] for r in
            spark.read.format("delta").load(STATUS_PATH).select("load_date").collect()
        )
        new_rows = [r for r in rows if r["load_date"] not in existing_dates]
        print(f"  Already tracked: {len(existing_dates)} | New PENDING rows: {len(new_rows)}")
    except Exception:
        new_rows = rows
        print(f"  No existing table found — writing all {len(new_rows)} rows.")

    if new_rows:
        df_new = spark.createDataFrame(new_rows, schema=STATUS_SCHEMA)
        (df_new.write
               .format("delta")
               .mode("append")
               .option("delta.autoOptimize.enabled", "true")
               .save(STATUS_PATH))
        print(f"[OK] Seeded {len(new_rows)} PENDING entries.")

    validate_status_table()

# COMMAND ----------

# DBTITLE 1,ACTION: status — display dashboard
if action == "status":
    try:
        df = spark.read.format("delta").load(STATUS_PATH)

        # Filter to requested date range
        df_range = df.filter(
            (F.col("load_date") >= date_from) & (F.col("load_date") <= date_to)
        )

        # Summary counts
        summary = (df_range
                   .groupBy("status")
                   .agg(F.count("*").alias("count"))
                   .orderBy("status"))

        print(f"\n=== Backfill Status Dashboard ({date_from} → {date_to}) ===")
        summary.show(truncate=False)

        total        = df_range.count()
        completed    = df_range.filter(F.col("status") == "COMPLETED").count()
        failed       = df_range.filter(F.col("status") == "FAILED").count()
        in_progress  = df_range.filter(F.col("status") == "IN_PROGRESS").count()
        pending      = df_range.filter(F.col("status") == "PENDING").count()
        pct_complete = round(completed / total * 100, 1) if total > 0 else 0.0

        print(f"Total dates in range: {total}")
        print(f"Progress: {completed}/{total} ({pct_complete}% COMPLETED)")
        if failed:
            print(f"[!] FAILED dates requiring re-run: {failed}")
        if in_progress:
            print(f"[~] Currently IN_PROGRESS: {in_progress}")

        # Show last 10 FAILED entries
        if failed > 0:
            print(f"\nFailed entries (last 10):")
            (df_range.filter(F.col("status") == "FAILED")
                     .orderBy(F.col("last_updated_ts").desc())
                     .select("load_date", "status", "failure_reason", "last_updated_ts")
                     .limit(10)
                     .show(truncate=80))

        validate_status_table()

        # Cross-reference with benchmark results for consistency
        try:
            df_bm = spark.read.format("delta").load(RESULTS_PATH)
            success_dates = set(
                r["load_date"] for r in
                df_bm.filter(F.col("status") == "SUCCESS").select("load_date").distinct().collect()
            )
            completed_dates = set(
                r["load_date"] for r in
                df_range.filter(F.col("status") == "COMPLETED").select("load_date").collect()
            )
            discrepancy = completed_dates - success_dates
            if discrepancy:
                print(f"[WARN] {len(discrepancy)} dates marked COMPLETED in status table but "
                      f"have no SUCCESS row in benchmark_results: {sorted(discrepancy)[:10]}")
        except Exception:
            pass

    except Exception as ex:
        if "Path does not exist" in str(ex):
            print("[INFO] Status table does not exist yet — run action=init first.")
        else:
            raise

# COMMAND ----------

# DBTITLE 1,ACTION: rerun_list — print FAILED load_dates for re-submission
if action == "rerun_list":
    try:
        df = spark.read.format("delta").load(STATUS_PATH)
        failed_dates = sorted([
            r["load_date"] for r in
            df.filter(
                (F.col("status") == "FAILED") &
                (F.col("load_date") >= date_from) &
                (F.col("load_date") <= date_to)
            ).select("load_date").collect()
        ])

        if not failed_dates:
            print(f"[OK] No FAILED dates in range {date_from}→{date_to}.")
        else:
            print(f"\n=== FAILED dates to re-run ({len(failed_dates)}) ===")
            print("Pass these as the load_dates widget in Benchmark_Runner:\n")
            print(",".join(failed_dates))
            print(f"\nFor day-by-day strategy, submit one Benchmark_Runner call per batch.")
            print(f"For monthly strategy, submit per month group:\n")

            # Group by month for monthly re-run guidance
            from collections import defaultdict
            by_month = defaultdict(list)
            for d in failed_dates:
                by_month[d[:6]].append(d)
            for month, days in sorted(by_month.items()):
                print(f"  Month {month}: {len(days)} day(s) to reprocess → {','.join(days)}")

        validate_status_table()

    except Exception as ex:
        if "Path does not exist" in str(ex):
            print("[INFO] Status table does not exist yet — run action=init first.")
        else:
            raise

# COMMAND ----------

# DBTITLE 1,Recovery guidance — Requirement 6.1 / 6.2 / 6.3 / 6.4
print("""
=== Failure Recovery Reference (Requirement 6) ===

Day-by-Day Strategy:
  Minimum recoverable unit: 1 Load_Date partition (~50 million records)
  Recovery procedure:
    1. Find FAILED load_dates: run this notebook with action=rerun_list
    2. For each failed date D:
       a. Submit Benchmark_Runner with strategy=day_by_day, load_dates=D
       b. RDM_Party.py uses mode("overwrite") scoped to EDL_LOAD_DTS=D/
          → only the failed partition is overwritten; all other dates are unchanged
       c. Run Verifier for D (load_dates=D)
       d. spike_backfill_status will update to COMPLETED if all checks pass

Monthly Strategy:
  Minimum recoverable unit: all partitions after the last successfully verified date
  Recovery procedure:
    1. Identify the last COMPLETED load_date D_last in the failed month
    2. Re-execute the monthly loop starting from D_last+1 through end of month
    3. mode("overwrite") safely overwrites any partially written partitions
    4. Run Verifier for all re-run dates

Write semantics confirmation:
  RadarUtils.py save_to_saradar_storage_account uses:
    df.repartition(1).write.format("parquet").mode("overwrite")
    .save(saradar_path + object_name + "/" + version + "/data/EDL_LOAD_DTS=YYYYMMDD/")
  → Overwrite is scoped to the single EDL_LOAD_DTS=YYYYMMDD/ subfolder.
  → Every daily execution is safe to re-run without risk of duplicate records.

MAX_PARALLEL_EXECUTIONS: 1 (sequential-only recommended)
  Rationale: each execution reads from shared source partitions (GDP storage).
  Parallel reads have not been assessed for source read contention.
  Set to > 1 only after confirming no contention in a controlled test.
""")
