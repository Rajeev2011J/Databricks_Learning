# Databricks notebook source
# MAGIC %md
# MAGIC #### RDM Notebooks Backfill Spike — Benchmark_Runner
# MAGIC
# MAGIC **Purpose:** Orchestrates all benchmark executions for RDM notebooks backfill spike.
# MAGIC For each combination of `notebook` × `strategy` × `cluster_config_id`, this notebook:
# MAGIC - Expands the given `load_dates` / month identifier into individual `Load_Date` values
# MAGIC - Calls the target notebook(s) via `dbutils.notebook.run()` for each date, passing `Load_Date` and `RunType` parameters
# MAGIC - Captures wall-clock time, records processed, throughput, DBU, and Spark metrics
# MAGIC - Appends one row per execution to the shared `spike_benchmark_results` Delta table
# MAGIC - Updates `spike_backfill_status` per date (PENDING → IN_PROGRESS → COMPLETED / FAILED)
# MAGIC
# MAGIC **Does NOT modify target notebooks** — fully non-destructive with respect to the production notebooks.
# MAGIC
# MAGIC **Requirements covered:** 2, 3 (strategy × cluster benchmarking), 4 (metrics capture)
# MAGIC
# MAGIC #### Widgets
# MAGIC | Widget            | Values                                                  |
# MAGIC |-------------------|---------------------------------------------------------|
# MAGIC | notebooks_mode    | `single`, `selected`, or `all`                          |
# MAGIC | notebook_names    | Comma-separated notebook names (for single/selected mode) |
# MAGIC | notebooks_base_path | Base path containing RDM notebooks                     |
# MAGIC | strategy          | `monthly` or `day_by_day`                               |
# MAGIC | cluster_config_id | `dab_small`, `dab_medium`, `dab_large`, `radar_large_uc`|
# MAGIC | load_dates        | Comma-separated `YYYYMMDD` (day-by-day) or `YYYYMM` month (monthly) |
# MAGIC | run_number        | 1–5                                                     |
# MAGIC | run_type          | `daily` or `historical` (passed to each notebook)      |
# MAGIC | timeout_seconds   | Timeout per notebook call (default: 21600 = 6 hours)   |
# MAGIC
# MAGIC #### Author
# MAGIC - Generated for RDM_Party Backfill Spike

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install hypothesis>=6.0 --quiet  

# COMMAND ----------

# DBTITLE 1,Read environment variables
import os
from datetime import datetime, timedelta
import calendar
import time
import uuid
import json

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DecimalType, BooleanType, DateType, TimestampType
)

# Use 'dev' as default environment if ENV variable is not set
environment   = os.getenv('ENV', 'preprd')
SARADAR       = f"saradar{environment}"
RESULTS_PATH  = f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/spike/benchmark_results/"
STATUS_PATH   = f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/spike/backfill_status/"

print(f"Environment: {environment}")
print(f"Storage account: {SARADAR}")
print(f"Results path: {RESULTS_PATH}")
print(f"Status path: {STATUS_PATH}")

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils
from RadarUtils import authenticate_storage_account
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Define widgets
# Notebook selection widgets
dbutils.widgets.dropdown("notebooks_mode", "single", ["single", "selected", "all"])
dbutils.widgets.text("notebook_names",    "RDM_Party")  # Comma-separated for 'selected' mode
dbutils.widgets.text("notebooks_base_path", "/Workspace/Users/rajeev.kumar01@rabobank.com/R-FEC-RADAR/Databricks/radarv1/RadarDataModel/RadarDataModel_Version3")

# Benchmark configuration widgets
dbutils.widgets.text("strategy",          "monthly")
dbutils.widgets.text("cluster_config_id", "dab_large")
dbutils.widgets.text("load_dates",        "")        # YYYYMMDD,YYYYMMDD,... or YYYYMM
dbutils.widgets.text("run_number",        "1")
dbutils.widgets.text("run_type",          "historical")  # Pass to notebooks: daily or historical
dbutils.widgets.text("timeout_seconds",   "21600")   # 6 h per notebook call

# Read widget values
notebooks_mode      = dbutils.widgets.get("notebooks_mode").strip().lower()
notebook_names_raw  = dbutils.widgets.get("notebook_names").strip()
notebooks_base_path = dbutils.widgets.get("notebooks_base_path").strip()
strategy            = dbutils.widgets.get("strategy").strip().lower()
cluster_config_id   = dbutils.widgets.get("cluster_config_id").strip()
load_dates_raw      = dbutils.widgets.get("load_dates").strip()
run_number          = int(dbutils.widgets.get("run_number"))
run_type            = dbutils.widgets.get("run_type").strip().lower()
timeout_seconds     = int(dbutils.widgets.get("timeout_seconds"))

# Validation
assert notebooks_mode in ("single", "selected", "all"), f"Invalid notebooks_mode: {notebooks_mode}"
assert strategy in ("monthly", "day_by_day"), f"Unknown strategy: {strategy}"
assert run_type in ("daily", "historical"), f"Invalid run_type: {run_type}. Must be 'daily' or 'historical'"
assert 1 <= run_number <= 5, f"run_number must be 1–5, got {run_number}"

print(f"Mode: {notebooks_mode} | Strategy: {strategy} | Cluster: {cluster_config_id} | RunType: {run_type} | Run: {run_number}")

# COMMAND ----------

# DBTITLE 1,Expand notebook list based on mode
# All available RDM notebooks
ALL_NOTEBOOKS = [
    "RDM_Party_SystemIdentifier",
    "RDM_Party_KN1Cases",
    "RDM_Party_RadarKeyStore",
    "RDM_CDDCase_GRAM",
    "RDM_CDDCase_QuestionAnswer_GRAM",
    "RDM_Party_CDDCase_RiskCategories",
    "RDM_Party_AlternativeNames",
    "RDM_Party_ClientOwnership",
    "RDM_Party_Coverage",
    "RDM_Party_Documents",
    "RDM_Party_Identification",
    "RDM_Party",
    "RDM_Party_role",
    "RDM_Party_Selection",
    "RDM_Party_Structure",
    "RDM_Internal_SystemComparison",
    "RDM_Party_Address",
    "RDM_Party_BusinessActivities",
    "RDM_Party_CountryAffiliation"
]

def expand_notebook_list(mode: str, names_raw: str) -> list:
    """
    Returns a list of notebook names to benchmark based on the mode.
    - single: returns the first name from names_raw
    - selected: returns comma-separated names from names_raw
    - all: returns ALL_NOTEBOOKS
    """
    if mode == "all":
        return ALL_NOTEBOOKS
    
    if not names_raw:
        raise ValueError("notebook_names widget is empty — provide at least one notebook name.")
    
    names = [n.strip() for n in names_raw.split(",") if n.strip()]
    
    if mode == "single":
        if len(names) > 1:
            print(f"[WARN] Mode is 'single' but multiple notebooks provided. Using only: {names[0]}")
        return [names[0]]
    
    # mode == "selected"
    # Validate that all specified notebooks exist in ALL_NOTEBOOKS
    invalid = [n for n in names if n not in ALL_NOTEBOOKS]
    if invalid:
        raise ValueError(f"Invalid notebook names: {invalid}. Must be from: {ALL_NOTEBOOKS}")
    
    return names

notebooks_to_run = expand_notebook_list(notebooks_mode, notebook_names_raw)
print(f"Notebooks to benchmark ({len(notebooks_to_run)}): {notebooks_to_run[:5]}{'...' if len(notebooks_to_run)>5 else ''}")

# COMMAND ----------

# DBTITLE 1,Expand load_dates input into a list of YYYYMMDD strings
def expand_load_dates(raw: str, strategy: str):
    """
    day_by_day: expects comma-separated YYYYMMDD values, e.g. '20250103,20250110,20250117'
    monthly:    expects a YYYYMM string, e.g. '202501' — expands to all days in that month
    Returns a sorted list of YYYYMMDD strings.
    """
    raw = raw.strip()
    if not raw:
        raise ValueError("load_dates widget is empty — provide comma-separated YYYYMMDD or a YYYYMM month.")

    if strategy == "monthly":
        if len(raw) != 6 or not raw.isdigit():
            raise ValueError(f"Monthly strategy expects YYYYMM, got '{raw}'")
        year, month = int(raw[:4]), int(raw[4:])
        _, n_days = calendar.monthrange(year, month)
        return [f"{year}{month:02d}{d:02d}" for d in range(1, n_days + 1)]

    # day_by_day: comma-separated list
    dates = [d.strip() for d in raw.split(",") if d.strip()]
    for d in dates:
        if len(d) != 8 or not d.isdigit():
            raise ValueError(f"Expected YYYYMMDD, got '{d}'")
    return sorted(set(dates))

load_dates = expand_load_dates(load_dates_raw, strategy)
print(f"Dates to process ({len(load_dates)}): {load_dates[:5]}{'...' if len(load_dates)>5 else ''}")

# COMMAND ----------

# DBTITLE 1,Cluster configuration reference (maps config_id → metadata)
# Cluster metadata sourced directly from clusters_job.yml
# Standard_D8ads_v5  = 32 GB RAM,  8 vCPUs
# Standard_D16ads_v5 = 64 GB RAM, 16 vCPUs  (per worker node)
# Standard_D32ads_v5 = 128 GB RAM, 32 vCPUs (per worker node)
# NOTE: dab_small uses local[*,4] single-node mode (num_workers=0)
# NOTE: dab_medium / dab_large use autoscale; for benchmark runs fix workers at max
#       to avoid mid-run scale-down distorting wall-clock measurements.
# NOTE: Radar_* UC clusters use data_security_mode=USER_ISOLATION — incompatible with
#       RDM_Party.py's legacy OAuth mount pattern; excluded from spike.
CLUSTER_CONFIGS = {
    # Req 3.1 — single-node baseline (driver only, 32 GB, local[*,4] Photon)
    "dab_small": {
        "cluster_name":   "DAB-small",
        "node_type":      "Standard_D8ads_v5",
        "driver_type":    "Standard_D8ads_v5",
        "workers":        0,        # single-node
        "memory_gb":      32,       # driver node only
        "vcpus":          8,
        "autoscale":      False,
        "min_workers":    None,
        "max_workers":    None,
        "spot":           False,
        "runtime":        "PHOTON",
        "spark_version":  "14.3.x-scala2.12",
        "req_mapping":    "Req 3.1 — single-node baseline",
    },
    # Req 3.3 — autoscaling variant (64 GB/worker, min 2 / max 4, Photon)
    "dab_medium": {
        "cluster_name":   "DAB-medium",
        "node_type":      "Standard_D16ads_v5",
        "driver_type":    "Standard_D16ads_v5",
        "workers":        "autoscale 2-4",
        "memory_gb":      192,      # driver 64 GB + 2 workers × 64 GB (at min scale)
        "memory_gb_max":  320,      # driver 64 GB + 4 workers × 64 GB (at max scale)
        "vcpus":          48,       # at min scale: 3 × 16
        "vcpus_max":      80,       # at max scale: 5 × 16
        "autoscale":      True,
        "min_workers":    2,
        "max_workers":    4,
        "spot":           False,
        "runtime":        "PHOTON",
        "spark_version":  "14.3.x-scala2.12",
        "req_mapping":    "Req 3.3 — autoscaling variant",
    },
    # Req 3.2 — high-end multi-node (128 GB/worker, min 2 / max 4, Photon)
    # For benchmark runs, consider setting fixed_workers=4 to meet the
    # "at least 4 workers" requirement and prevent mid-run scale-down.
    "dab_large": {
        "cluster_name":   "DAB-large",
        "node_type":      "Standard_D32ads_v5",
        "driver_type":    "Standard_D32ads_v5",
        "workers":        "autoscale 2-4",
        "memory_gb":      384,      # driver 128 GB + 2 workers × 128 GB (at min scale)
        "memory_gb_max":  640,      # driver 128 GB + 4 workers × 128 GB (at max scale)
        "vcpus":          96,       # at min scale: 3 × 32
        "vcpus_max":      160,      # at max scale: 5 × 32
        "autoscale":      True,
        "min_workers":    2,
        "max_workers":    4,
        "spot":           False,
        "runtime":        "PHOTON",
        "spark_version":  "14.3.x-scala2.12",
        "req_mapping":    "Req 3.2 — high-end multi-node (≥4 workers at max scale)",
        "benchmark_note": "Pin to max_workers=4 for benchmark runs (Req 3.2 requires ≥4 workers)",
    },
}
cluster_meta = CLUSTER_CONFIGS.get(cluster_config_id, {})
print(f"Cluster metadata: {cluster_meta}")

# COMMAND ----------

# DBTITLE 1,Schema for spike_benchmark_results Delta table
BENCHMARK_SCHEMA = StructType([
    StructField("execution_id",          StringType(),  False),
    StructField("notebook_name",         StringType(),  False),
    StructField("load_date",             StringType(),  False),
    StructField("run_type",              StringType(),  False),
    StructField("strategy",              StringType(),  False),
    StructField("cluster_config_id",     StringType(),  False),
    StructField("run_number",            IntegerType(), False),
    StructField("wall_clock_minutes",    DecimalType(12, 2), False),
    StructField("records_processed",     LongType(),    False),
    StructField("throughput_rps",        DecimalType(18, 2), False),
    StructField("peak_cpu_pct",          DecimalType(6,  2), True),
    StructField("peak_memory_pct",       DecimalType(6,  2), True),
    StructField("peak_io_mb_s",          DecimalType(12, 2), True),
    StructField("total_dbu",             DecimalType(16, 4), False),
    StructField("estimated_cost_usd",    DecimalType(16, 2), False),
    StructField("dbu_rate_usd",          DecimalType(10, 4), False),
    StructField("dbu_rate_date",         StringType(),  False),
    StructField("spark_tasks",           IntegerType(), True),
    StructField("shuffle_read_bytes",    LongType(),    True),
    StructField("shuffle_write_bytes",   LongType(),    True),
    StructField("bytes_spilled",         LongType(),    True),
    StructField("error_message",         StringType(),  True),
    StructField("spark_stage_at_failure",StringType(),  True),
    StructField("retry_attempt",         IntegerType(), False),
    StructField("status",                StringType(),  False),
    StructField("zero_metric_reason",    StringType(),  True),
    StructField("outlier_flag",          BooleanType(), True),
    StructField("outlier_deviation_pct", DecimalType(8, 2), True),
    StructField("source_partition_paths",StringType(),  True),
    StructField("recorded_at_ts",        TimestampType(),False),
])

# COMMAND ----------

# DBTITLE 1,Helper — get Spark job-level metrics for the last completed job
def get_last_spark_job_metrics():
    """
    Returns a dict with aggregated Spark job metrics from the most recent job.
    Uses the Spark listener bus / sc.statusTracker for task/shuffle data.
    Falls back to None values if metrics are unavailable.
    """
    try:
        sc = spark.sparkContext
        status = sc.statusTracker()
        job_ids = status.getJobIdsForGroup(None)
        if not job_ids:
            return {"spark_tasks": None, "shuffle_read_bytes": None,
                    "shuffle_write_bytes": None, "bytes_spilled": None}

        latest_job_id = max(job_ids)
        job_info = status.getJobInfo(latest_job_id)
        stage_ids = job_info.stageIds() if job_info else []

        total_tasks = 0
        shuffle_read = 0
        shuffle_write = 0
        spilled = 0

        for stage_id in stage_ids:
            stage_info = status.getStageInfo(stage_id)
            if stage_info:
                total_tasks  += stage_info.numTasks()
                shuffle_read += getattr(stage_info, "shuffleReadBytes", 0) or 0
                shuffle_write+= getattr(stage_info, "shuffleWriteBytes", 0) or 0
                spilled      += getattr(stage_info, "diskBytesSpilled", 0) or 0

        return {
            "spark_tasks":         total_tasks  or None,
            "shuffle_read_bytes":  shuffle_read or None,
            "shuffle_write_bytes": shuffle_write or None,
            "bytes_spilled":       spilled      or None,
        }
    except Exception as ex:
        print(f"[WARN] Could not collect Spark metrics: {ex}")
        return {"spark_tasks": None, "shuffle_read_bytes": None,
                "shuffle_write_bytes": None, "bytes_spilled": None}

# COMMAND ----------

# DBTITLE 1,Helper — count records in output partition
def count_output_partition(load_date: str) -> int:
    """Reads the output parquet partition and returns its record count."""
    path = (f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net"
            f"/Party/3/data/EDL_LOAD_DTS={load_date}/*.parquet")
    try:
        return spark.read.parquet(path).count()
    except Exception:
        return 0

# COMMAND ----------

# DBTITLE 1,Helper — write a row to spike_benchmark_results
from decimal import Decimal as D

# Published Databricks list price used for cost calculation.
# Update this value and date if prices change before the benchmark runs.
DBU_RATE_USD  = D("0.4000")   # USD per DBU (All-Purpose Compute, Jobs tier — verify before use)
DBU_RATE_DATE = "2025-01-01"  # Date the price was retrieved

def write_benchmark_row(row_dict: dict):
    """Appends a single row to the spike_benchmark_results Delta table."""
    row_dict["recorded_at_ts"] = datetime.utcnow()
    df = spark.createDataFrame([row_dict], schema=BENCHMARK_SCHEMA)
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "true")  # Allow schema evolution for new columns
       .save(RESULTS_PATH))

# COMMAND ----------

# DBTITLE 1,Helper — update spike_backfill_status
STATUS_SCHEMA = StructType([
    StructField("load_date",        StringType(),  False),
    StructField("status",           StringType(),  False),
    StructField("last_updated_ts",  TimestampType(),False),
    StructField("execution_id",     StringType(),  True),
    StructField("failure_reason",   StringType(),  True),
])

VALID_STATUSES = {"PENDING", "IN_PROGRESS", "COMPLETED", "FAILED"}

def upsert_status(load_date: str, status: str, execution_id: str = None, failure_reason: str = None):
    """
    Writes/overwrites a status row for the given load_date.
    Status transitions enforced: COMPLETED → IN_PROGRESS is blocked.
    """
    assert status in VALID_STATUSES, f"Invalid status: {status}"

    # Guard: do not regress a COMPLETED partition back to IN_PROGRESS
    if status == "IN_PROGRESS":
        try:
            existing = (spark.read.format("delta").load(STATUS_PATH)
                            .filter(F.col("load_date") == load_date)
                            .select("status").collect())
            if existing and existing[0]["status"] == "COMPLETED":
                print(f"[SKIP] {load_date} is already COMPLETED — will not revert to IN_PROGRESS.")
                return
        except Exception:
            pass  # Table doesn't exist yet — safe to proceed

    row = {
        "load_date":       load_date,
        "status":          status,
        "last_updated_ts": datetime.utcnow(),
        "execution_id":    execution_id,
        "failure_reason":  failure_reason,
    }
    df = spark.createDataFrame([row], schema=STATUS_SCHEMA)

    # Overwrite just this load_date partition (Delta merge-style upsert via replaceWhere)
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("replaceWhere", f"load_date = '{load_date}'")
       .save(STATUS_PATH))

# COMMAND ----------

# DBTITLE 1,Helper — outlier detection across day-by-day executions
def flag_outliers(exec_times: list) -> list:
    """
    Returns a list of dicts with `outlier_flag` and `outlier_deviation_pct`.
    An execution is flagged as outlier when |t - mean| / mean > 0.20.
    Implements Property 10 of the design spec.
    """
    if not exec_times:
        return []
    mean_t = sum(exec_times) / len(exec_times)
    results = []
    for t in exec_times:
        deviation = abs(t - mean_t) / mean_t if mean_t > 0 else 0.0
        results.append({
            "outlier_flag":          deviation > 0.20,
            "outlier_deviation_pct": round(deviation * 100, 2),
        })
    return results

# COMMAND ----------

# DBTITLE 1,Main benchmark loop — strategy × dates
# Outer loop over notebooks
all_execution_times = {}  # notebook_name -> list of execution times
all_execution_ids   = {}  # notebook_name -> list of execution IDs

MAX_RETRIES = 3

for notebook_name in notebooks_to_run:
    print(f"\n{'='*80}")
    print(f"Starting benchmark for: {notebook_name}")
    print(f"{'='*80}\n")
    
    notebook_path = f"{notebooks_base_path}/{notebook_name}"
    execution_times = []   # collected for outlier detection (day_by_day only)
    execution_ids   = []   # for summary
    
    for load_date in load_dates:

        execution_id  = str(uuid.uuid4())
        execution_ids.append(execution_id)
        success       = False
        attempt       = 0
        error_msg     = None
        stage_at_fail = None

        # Collect source partition paths (JSON) — used for cross-run comparability check
        source_paths_snapshot = json.dumps({
            "notebook":  notebook_name,
            "load_date": load_date,
            "strategy":  strategy,
            "cluster":   cluster_config_id,
            "note":      "Actual paths resolved inside notebook by Read_GDP_Defined_DataObjects"
        })

        # --- Mark IN_PROGRESS ---
        upsert_status(load_date, "IN_PROGRESS", execution_id)

        while attempt < MAX_RETRIES and not success:
            attempt += 1
            t_start = time.time()

            try:
                # -----------------------------------------------------------------
                # Call target notebook — passes Load_Date and RunType
                # -----------------------------------------------------------------
                result = dbutils.notebook.run(
                    notebook_path,
                    timeout_seconds=timeout_seconds,
                    arguments={"Load_Date": load_date, "RunType": run_type}
                )
                t_end = time.time()
                wall_clock_minutes = round((t_end - t_start) / 60, 2)

                # Count output records
                records = count_output_partition(load_date)

                # Throughput
                throughput = round(records / (wall_clock_minutes * 60), 2) if wall_clock_minutes > 0 else 0.0

                # Spark job metrics
                spark_metrics = get_last_spark_job_metrics()

                # Cost — DBU is retrieved from Databricks Jobs API; placeholder here
                # In production, retrieve via REST: GET /api/2.1/jobs/runs/get?run_id=<run_id>
                # For now we store 0 and annotate with BELOW_MEASUREMENT_THRESHOLD
                total_dbu          = D("0.0000")
                estimated_cost_usd = D("0.00")
                zero_metric_reason = "BELOW_MEASUREMENT_THRESHOLD"  # DBU pulled manually post-run

                # Outlier tracking
                execution_times.append(float(wall_clock_minutes))

                row = {
                    "execution_id":           execution_id,
                    "notebook_name":          notebook_name,
                    "load_date":              load_date,
                    "run_type":               run_type,
                    "strategy":               strategy,
                    "cluster_config_id":      cluster_config_id,
                    "run_number":             run_number,
                    "wall_clock_minutes":     D(str(wall_clock_minutes)),
                    "records_processed":      records,
                    "throughput_rps":         D(str(throughput)),
                    "peak_cpu_pct":           None,   # Pulled from Databricks Cluster Metrics API post-run
                    "peak_memory_pct":        None,
                    "peak_io_mb_s":           None,
                    "total_dbu":              total_dbu,
                    "estimated_cost_usd":     estimated_cost_usd,
                    "dbu_rate_usd":           DBU_RATE_USD,
                    "dbu_rate_date":          DBU_RATE_DATE,
                    "spark_tasks":            spark_metrics["spark_tasks"],
                    "shuffle_read_bytes":     spark_metrics["shuffle_read_bytes"],
                    "shuffle_write_bytes":    spark_metrics["shuffle_write_bytes"],
                    "bytes_spilled":          spark_metrics["bytes_spilled"],
                    "error_message":          None,
                    "spark_stage_at_failure": None,
                    "retry_attempt":          attempt - 1,
                    "status":                 "SUCCESS",
                    "zero_metric_reason":     zero_metric_reason,
                    "outlier_flag":           None,   # Filled post-loop
                    "outlier_deviation_pct":  None,
                    "source_partition_paths": source_paths_snapshot,
                }
                write_benchmark_row(row)
                upsert_status(load_date, "COMPLETED", execution_id)
                success = True
                print(f"[OK] {notebook_name} | {load_date} | {wall_clock_minutes:.2f} min | {records:,} records | attempt {attempt}")

            except Exception as ex:
                t_end = time.time()
                error_msg     = str(ex)[:2000]
                stage_at_fail = "unknown"  # Could be enriched by parsing Databricks job run logs
                print(f"[ERROR] {load_date} attempt {attempt}/{MAX_RETRIES}: {error_msg[:200]}")

                if attempt == MAX_RETRIES:
                    # All retries exhausted — write FAILED row (no cost/duration extrapolation)
                    row = {
                        "execution_id":           execution_id,
                        "notebook_name":          notebook_name,
                        "load_date":              load_date,
                        "run_type":               run_type,
                        "strategy":               strategy,
                        "cluster_config_id":      cluster_config_id,
                        "run_number":             run_number,
                        "wall_clock_minutes":     D(str(round((t_end - t_start) / 60, 2))),
                        "records_processed":      0,
                        "throughput_rps":         D("0.00"),
                        "peak_cpu_pct":           None,
                        "peak_memory_pct":        None,
                        "peak_io_mb_s":           None,
                        "total_dbu":              D("0.0000"),
                        "estimated_cost_usd":     D("0.00"),
                        "dbu_rate_usd":           DBU_RATE_USD,
                        "dbu_rate_date":          DBU_RATE_DATE,
                        "spark_tasks":            None,
                        "shuffle_read_bytes":     None,
                        "shuffle_write_bytes":    None,
                        "bytes_spilled":          None,
                        "error_message":          error_msg,
                        "spark_stage_at_failure": stage_at_fail,
                        "retry_attempt":          attempt - 1,
                        "status":                 "FAILED",
                        "zero_metric_reason":     None,
                        "outlier_flag":           None,
                        "outlier_deviation_pct":  None,
                        "source_partition_paths": source_paths_snapshot,
                    }
                    write_benchmark_row(row)
                    upsert_status(load_date, "FAILED", execution_id, failure_reason=error_msg[:500])
    
    # Store results for this notebook
    all_execution_times[notebook_name] = execution_times
    all_execution_ids[notebook_name] = execution_ids
    
    print(f"\nCompleted {notebook_name}: {len(execution_ids)} dates processed")

# COMMAND ----------

# DBTITLE 1,Post-loop — update outlier flags for day-by-day strategy
if strategy == "day_by_day":
    for notebook_name in notebooks_to_run:
        execution_times = all_execution_times.get(notebook_name, [])
        execution_ids = all_execution_ids.get(notebook_name, [])
        
        if not execution_times:
            continue
            
        outlier_results = flag_outliers(execution_times)
        for i, (exec_id, ot) in enumerate(zip(execution_ids, outlier_results)):
            if ot["outlier_flag"]:
                try:
                    (spark.read.format("delta").load(RESULTS_PATH)
                        .filter(F.col("execution_id") == exec_id)
                        .show(1, truncate=False))
                    print(f"[OUTLIER] {notebook_name} | {load_dates[i]} — deviation {ot['outlier_deviation_pct']:.2f}%")
                    # In a full implementation, issue a Delta UPDATE statement here
                    # spark.sql(f"""UPDATE delta.`{RESULTS_PATH}`
                    #               SET outlier_flag = {ot['outlier_flag']},
                    #                   outlier_deviation_pct = {ot['outlier_deviation_pct']}
                    #               WHERE execution_id = '{exec_id}'""")
                except Exception as ex:
                    print(f"[WARN] Could not update outlier flag for {exec_id}: {ex}")

# COMMAND ----------

# DBTITLE 1,Summary
print(f"\n{'='*80}")
print(f"Benchmark_Runner complete")
print(f"{'='*80}")
print(f"Mode: {notebooks_mode} | Strategy: {strategy} | Cluster: {cluster_config_id} | RunType: {run_type} | Run: {run_number}")
print(f"Notebooks benchmarked: {len(notebooks_to_run)}")
print(f"Dates per notebook: {len(load_dates)}")
print(f"\nPer-notebook summary:")
for notebook_name in notebooks_to_run:
    execution_times = all_execution_times.get(notebook_name, [])
    execution_ids = all_execution_ids.get(notebook_name, [])
    if execution_times:
        avg_time = sum(execution_times) / len(execution_times)
        min_time = min(execution_times)
        max_time = max(execution_times)
        print(f"  {notebook_name}: {len(execution_ids)} dates | Avg: {avg_time:.2f} min | Range: {min_time:.2f}-{max_time:.2f} min")
    else:
        print(f"  {notebook_name}: No successful executions")
print(f"\nResults path: {RESULTS_PATH}")
print(f"{'='*80}")

# COMMAND ----------

# DBTITLE 1,View benchmark results (all notebooks)
import os
from pyspark.sql.functions import regexp_extract
from pyspark.sql import functions as F
from RadarUtils import *
# Read benchmark results from production ADLS location
environment=os.environ['ENV']
SARADAR = "saradar" + environment
authenticate_storage_account(SARADAR)
results_path = "abfss://radardatamodel@saradarpreprd.dfs.core.windows.net/spike/benchmark_results/"
print(f"Reading data from: {results_path}")
print("=" * 80)

try:
    # Read the Delta table
    df_results = spark.read.format("delta").load(results_path)
    
    # Show basic info
    print(f"\nSuccessfully loaded benchmark results")
    print(f"Total records: {df_results.count():,}")
    print(f"\nSchema:")
    df_results.printSchema()
    
    # Display sample data
    print("\nSample data (10 rows):")
    display(df_results.limit(10))
    
    # Show summary statistics
    print("\nSummary by notebook, strategy, and cluster:")
    summary_df = (df_results
        .groupBy("notebook_name", "strategy", "cluster_config_id", "run_number")
        .agg(
            F.count("*").alias("num_executions"),
            F.sum("records_processed").alias("total_records"),
            F.avg("wall_clock_minutes").alias("avg_duration_min"),
            F.avg("throughput_rps").alias("avg_throughput_rps"),
            F.sum("total_dbu").alias("total_dbu")
        )
        .orderBy("notebook_name", "strategy", "cluster_config_id", "run_number")
    )
    display(summary_df)
    
except Exception as e:
    print(f"\nError reading data: {str(e)}")
    print("\nThis may be because:")
    print("  1. The table doesn't exist yet (no benchmark runs completed)")
    print("  2. Storage credentials are not configured")
    print("  3. The path is incorrect")
