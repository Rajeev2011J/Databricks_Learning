# Databricks notebook source
SARADAR = "saradar" + environment
authenticate_storage_account(SARADAR)

# COMMAND ----------


# ---------------------------------------------------------
# Logging Notebook: Write tab-delimited CSV to ADLS Gen2
# Hardcoded blob path; no compression; single file per run
# ---------------------------------------------------------

dbutils.widgets.text("payload_json", "")

import json, datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ---- Hardcoded ADLS path (update this to your container/folder)
BLOB_PATH = f"abfss://logs@{SARADAR}.dfs.core.windows.net/airflow-logs/"

payload_json = dbutils.widgets.get("payload_json")
if not payload_json:
    raise ValueError("Missing required widget 'payload_json'.")

# Parse payload into list of dicts
rows = json.loads(payload_json)
if not isinstance(rows, list):
    raise ValueError("payload_json must be a JSON array")

# Define schema for stability
schema = StructType([
    StructField("job_name",            StringType(), True),
    StructField("task_name",           StringType(), True),
    StructField("status",              StringType(), True),
    StructField("start_time",          StringType(), True),
    StructField("end_time",            StringType(), True),
    StructField("duration_seconds",    LongType(),   True),
    StructField("trial",               IntegerType(),True),
    StructField("standard_error",      StringType(), True),
    StructField("databricks_run_id",   LongType(),   True),
    StructField("databricks_job_id",   LongType(),   True),
])

df = spark.createDataFrame(rows, schema=schema)

# Add helper columns
df = (
    df
    .withColumn("ingest_ts_utc", F.current_timestamp())
    .withColumn("date_utc", F.to_date(F.col("ingest_ts_utc")))
)

# Build partitioned output path
today = datetime.date.today().isoformat()
out_path = BLOB_PATH.rstrip("/") + f"/date_utc={today}/"

# Write as tab-delimited CSV with header
(df.coalesce(1)
    .write
    .mode("append")
    .option("delimiter", "\t")
    .option("header", "true")
    .csv(out_path))

print(f"Wrote {df.count()} rows to: {out_path}")
dbutils.notebook.exit("OK")
