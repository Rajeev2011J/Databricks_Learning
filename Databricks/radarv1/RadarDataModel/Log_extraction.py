# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To extract the log details from the databricks job
# MAGIC
# MAGIC #### author
# MAGIC - Sowmyashree.parashivamurthy.sudha@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Extract job IDs from environment variables to identify which Databricks jobs to process.
# MAGIC - Fetch task run details for those jobs using the Databricks Jobs API (including metadata and execution info).
# MAGIC - Extract and clean log details, normalize fields (status, timestamps, task names), and prepare structured records.
# MAGIC - Save the processed logs as a single CSV file in the Azure Storage Account and create a catalog table by loading the last 30 days of CSV files for monitoring.
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Sowmyashree 	 |7-jan-2025 |14897926 |First release
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import libraries
import os, re, time, json, requests
from datetime import datetime, timedelta, timezone
from pyspark.sql.functions import to_timestamp, from_utc_timestamp, date_format, col, current_date
from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from RadarUtils import *
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, DoubleType
)
from pyspark.sql import Row
from azure.identity import ClientSecretCredential

# COMMAND ----------

# DBTITLE 1,Import env variables
environment    = os.environ['ENV']
APP_REG_APP_ID = os.environ["APP_REG_APP_ID"]
TENANT_ID      = os.environ["TENANT_ID"]   
JOB_ID_LIST  = os.environ['job_list']
JOB_ID_LIST_FROM_ENV = [int(x) for x in JOB_ID_LIST.split(",")]
print((JOB_ID_LIST_FROM_ENV))

STORAGE_ACCOUNT_NAME='saradar'+environment
CONTAINER_NAME       = "radar-logs"  
authenticate_storage_account(STORAGE_ACCOUNT_NAME)

WORKSPACE_URL = f"""https://{spark.conf.get("spark.databricks.workspaceUrl")}"""                   

HOURS_BACK = 24
USE_UTC_DATE_FOLDER  = True
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_CALLS = 0.05

# COMMAND ----------

SERVICE_CREDENTIAL = dbutils.secrets.get(scope="connectedsecrets", key=f"appreg-{APP_REG_APP_ID}")
DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"

_sp_cred = ClientSecretCredential(tenant_id=TENANT_ID,
                                  client_id=APP_REG_APP_ID,
                                  client_secret=SERVICE_CREDENTIAL)
ACCESS_TOKEN = _sp_cred.get_token(DATABRICKS_SCOPE).token

HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# COMMAND ----------

# DBTITLE 1,helper
def _today_utc_window_ms():
    now_utc = datetime.now(timezone.utc)
    start_utc = datetime(year=now_utc.year, month=now_utc.month, day=now_utc.day, tzinfo=timezone.utc)
    end_utc = start_utc + timedelta(days=1) - timedelta(milliseconds=1)
    start_ms = int(start_utc.timestamp() * 1000)
    end_ms   = int(end_utc.timestamp() * 1000)
    return start_ms, end_ms

def _iso_to_date_key(iso_str):
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).date().isoformat() if iso_str else None

def _map_status(result_state, life_cycle_state):
    rs = (result_state or "").upper()
    if rs == "SUCCESS":
        return "success"
    if rs == "SKIPPED":
        return "deferred"
    if not rs:
        ls = (life_cycle_state or "").upper()
        if ls in ("PENDING", "RUNNING", "QUEUED", "WAITING_FOR_RETRY", "TERMINATING", "BLOCKED"):
            return "running"
    return "fail"

def _start_ms(hours_back):
    ms = int((datetime.now(timezone.utc) - timedelta(hours=hours_back)).timestamp() * 1000)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return min(ms, now_ms)

def _get_json(url, params=None):
    r = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def _get_jobs_map():

    url = f"{WORKSPACE_URL}/api/2.2/jobs/list"
    params = {"limit": 100}
    jobs_map = {}
    while True:
        data = _get_json(url, params=params)
        for j in data.get("jobs", []):
            jobs_map[j.get("job_id")] = j.get("settings", {}).get("name")
        token = data.get("next_page_token")
        if not token:
            break
        params["page_token"] = token
    return jobs_map  # Docs: Jobs list v2.2 supports 1..100 limit

# COMMAND ----------

def _list_recent_job_runs(job_id=None, hours_back=24, limit=50, today_only=False):
    url = f"{WORKSPACE_URL}/api/2.2/jobs/runs/list"

    if today_only:
        start_ms, end_ms = _today_utc_window_ms()
        params = {
            "limit": min(limit, 50),
            "start_time_from": start_ms,
            "start_time_to": end_ms,
        }
    else:
        params = {
            "limit": min(limit, 50),
            "start_time_from": _start_ms(hours_back),
        }

    if job_id:
        params["job_id"] = int(job_id)

    while True:
        resp = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 400:
            raise requests.HTTPError(f"400 from runs/list. Body: {resp.text}. Params: {params}")
        resp.raise_for_status()
        data = resp.json()
        runs = data.get("runs", [])
        for r in runs:
            yield r
        token = data.get("next_page_token")
        if not token or not runs:
            break
        params["page_token"] = token

def _get_job_run(run_id):
    return _get_json(f"{WORKSPACE_URL}/api/2.1/jobs/runs/get", params={"run_id": run_id})

def _get_task_output(task_run_id):
    try:
        return _get_json(f"{WORKSPACE_URL}/api/2.2/jobs/runs/get-output", params={"run_id": task_run_id})
    except requests.HTTPError:
        return _get_json(f"{WORKSPACE_URL}/api/2.1/jobs/runs/get-output", params={"run_id": task_run_id})

def _identity_from_metadata(metadata):
    task_key = None
    notebook_path = None
    task_name = None

    if "tasks" in metadata and metadata["tasks"]:
        t0 = metadata["tasks"][0] or {}
        task_key = t0.get("task_key") or t0.get("run_name")
        task_name = t0.get("run_name") or t0.get("task_key")
        nb = t0.get("notebook_task") or {}
        notebook_path = nb.get("notebook_path")
    elif "task" in metadata:
        t = metadata["task"] or {}
        task_key = t.get("task_key") or t.get("run_name")
        task_name = t.get("run_name") or t.get("task_key")
        nb = t.get("notebook_task") or {}
        notebook_path = nb.get("notebook_path")
    else:
        task_key = metadata.get("run_name")
        task_name = metadata.get("run_name")

    identity_key = task_key or notebook_path or task_name or "UNKNOWN_TASK"
    return identity_key, task_name, notebook_path, metadata.get("job_name")


# COMMAND ----------

_CONSUMER_RE = re.compile(r'(?i)CONSUMER_(.*)')

def extract_after_consumer(task_name: str) -> str:

    if task_name is None:
        return None
    s = str(task_name)
    m = _CONSUMER_RE.search(s)
    result = m.group(1).strip() if m else s.strip()
    result = re.sub(r'^_+', '', result)
    return result

def _collect_all_task_runs_for_job(job_id, hours_back, jobs_map):
    records = []

    for run in _list_recent_job_runs(job_id=job_id, hours_back=hours_back, limit=25, today_only=True):
        job_run_id = run["run_id"]
        run_full = _get_job_run(job_run_id)
        this_job_id = run_full.get("job_id")

        tasks = run_full.get("tasks", [])
        task_run_ids = [t.get("run_id") for t in tasks] if tasks else [job_run_id]

        for task_run_id in task_run_ids:
            out = _get_task_output(task_run_id)
            meta = out.get("metadata", {}) or {}

            identity_key, task_name, nb_path, job_name_meta = _identity_from_metadata(meta)
            raw_job_name = job_name_meta or jobs_map.get(this_job_id) or run_full.get("run_name")
            job_name_final = raw_job_name.split(".")[0] if raw_job_name else None

            start_ms = meta.get("start_time")
            end_ms   = meta.get("end_time")
            start_iso = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat()
            end_iso   = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).isoformat()

            state = meta.get("state", {}) or {}
            result_state     = state.get("result_state")
            life_cycle_state = state.get("life_cycle_state")

            rec = {
                "job_id": this_job_id,
                "job_name": job_name_final,
                "job_run_id": job_run_id,
                "task_run_id": task_run_id,
                "task_identity_key": identity_key,  # grouping key
                "task_name": task_name,
                "notebook_path": nb_path,
                "start_time_ms": start_ms,
                "end_time_ms": end_ms,
                "start_time_utc": start_iso,
                "end_time_utc": end_iso,
                "status": _map_status(result_state, life_cycle_state),
                "error_message": out.get("error"),
                "error_trace": out.get("error_trace"),
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "retry_number": None,
                "duration_minutes": None,
            }
            records.append(rec)
            time.sleep(SLEEP_BETWEEN_CALLS)
    return records

def _assign_retries_per_job_task_date(records):
    groups = defaultdict(list)
    for rec in records:
        date_key = _iso_to_date_key(rec["start_time_utc"])
        grp_key = (rec["job_id"], rec["task_identity_key"], date_key)
        groups[grp_key].append(rec)

    for _, items in groups.items():
        items.sort(key=lambda r: r["start_time_ms"] if r["start_time_ms"] is not None else float("inf"))
        for idx, rec in enumerate(items, start=1):
            rec["retry_number"] = idx
            st_ms, en_ms = rec["start_time_ms"], rec["end_time_ms"]
            if st_ms is not None and en_ms is not None:
                rec["duration_minutes"] = round((en_ms - st_ms) / 60000.0, 2)

    for rec in records:
        rec.pop("task_identity_key", None)
        rec.pop("start_time_ms", None)
        rec.pop("end_time_ms", None)
    return records

def collect_job_task_runs_for_ids(job_ids, hours_back=HOURS_BACK):
    jobs_map = _get_jobs_map()
    all_records = []
    for jid in job_ids:
        all_records.extend(_collect_all_task_runs_for_job(jid, hours_back, jobs_map))
    # de-dupe by task_run_id
    unique = {rec["task_run_id"]: rec for rec in all_records}
    records = list(unique.values())
    return _assign_retries_per_job_task_date(records)


# COMMAND ----------

def write_as_single_csv_to_abfs_spark(records, storage_account, container, use_utc_date=True):
    spark = SparkSession.builder.getOrCreate()

    # Stable column order (matches your code)
    cols = [
        "job_id", "job_name", "job_run_id", "task_run_id",
        "task_name", "notebook_path",
        "execution_start_time", "execution_end_time",
        "retry_number", "execution_duration",
        "status", "error_message"
    ]

    # 1) Define an explicit schema to avoid inference ambiguity
    schema = StructType([
        StructField("job_id", LongType(), True),
        StructField("job_name", StringType(), True),
        StructField("job_run_id", LongType(), True),
        StructField("task_run_id", LongType(), True),
        StructField("task_name", StringType(), True),
        StructField("notebook_path", StringType(), True),
        StructField("start_time_utc", StringType(), True),  # keep as string; easier for CSV
        StructField("end_time_utc", StringType(), True),
        StructField("retry_number", IntegerType(), True),
        StructField("execution_time_min", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True)
    ])
    
    import re
    import html
    _AT_LINE_RE = re.compile(r'^\s*at\b')  # matches a stack-trace line only at start-of-line

    def sanitize_error(v):
    
        if v is None:
            return None

        s = str(v)

        # 1) Unescape HTML entities (&lt;span&gt; -> <span>)
        s = html.unescape(s)

        # 2) Remove <span ...> tags and then any remaining HTML tags
        s = re.sub(r'</?span\b[^>]*>', '', s, flags=re.IGNORECASE)  # strip span tags
        s = re.sub(r'<[^>]+>', '', s)  # strip any other tags (keep inner text)

        # 3) Cut BEFORE the first stack-trace line that *starts* with 'at'
        kept_lines = []
        for line in s.splitlines():
            if _AT_LINE_RE.match(line):
                break  # stop at the first true stack-trace line
            kept_lines.append(line)

        # If there were no stack-trace lines, we keep all lines
        s = ' '.join(kept_lines)

        # 4) Flatten whitespace (including tabs/newlines), then trim
        s = re.sub(r'[\r\n\t]+', ' ', s)
        s = re.sub(r'\s{2,}', ' ', s).strip()

        return s


    # 2) Normalize & cast every record to match the schema
    def _to_long(v):
        try:
            return int(v) if v is not None else None
        except Exception:
            return None

    def _to_int(v):
        try:
            return int(v) if v is not None else None
        except Exception:
            return None

    def _to_double(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    def _to_str(v):
        return None if v is None else str(v)

    normalized_rows = []
    for rec in records:
        raw_job_name = rec.get("job_name")
        normalized_job_name = (raw_job_name.split(".", 1)[0]
                               if isinstance(raw_job_name, str) and raw_job_name
                               else raw_job_name)

        raw_task_name = rec.get("task_name")
        tn_in = raw_task_name if isinstance(raw_task_name, str) else _to_str(raw_task_name)
        tn = extract_after_consumer(tn_in)

        row_dict = {
            "job_id": _to_long(rec.get("job_id")),
            "job_name": _to_str(normalized_job_name),
            "job_run_id": _to_long(rec.get("job_run_id")),
            "task_run_id": _to_long(rec.get("task_run_id")),
            "task_name": _to_str(tn),
            "notebook_path": _to_str(rec.get("notebook_path")),
            "start_time_utc": _to_str(rec.get("start_time_utc")),
            "end_time_utc": _to_str(rec.get("end_time_utc")),
            "retry_number": _to_int(rec.get("retry_number")),
            "execution_time_min": _to_double(rec.get("duration_minutes")),
            "status": _to_str(rec.get("status")),
            "error_message": sanitize_error(rec.get("error_message")),
        }
        normalized_rows.append(Row(**row_dict))

    # 3) Create the Spark DataFrame with the explicit schema
    df = spark.createDataFrame(normalized_rows, schema=schema).withColumn("processing_stage", lit("radar_consumer"))

    # Convert UTC -> CET/CEST (Europe/Amsterdam)
    df_spark = (
        df
        .withColumn("execution_start_time_cet",
                    date_format(from_utc_timestamp(to_timestamp(col("start_time_utc")), "Europe/Amsterdam"),
                                "yyyy-MM-dd HH:mm:ss"))
        .withColumn("execution_end_time_cet",
                    date_format(from_utc_timestamp(to_timestamp(col("end_time_utc")), "Europe/Amsterdam"),
                                "yyyy-MM-dd HH:mm:ss"))
        .withColumn("execution_date", date_format(current_date(), 'dd/MM/yyyy'))
        .drop("start_time_utc", "end_time_utc")
    )
    date_str = (datetime.now(timezone.utc).strftime("%Y%m%d")
                if use_utc_date else datetime.now().strftime("%Y%m%d"))
    workspace_name = 'radar_consumer'
    base_dir = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{date_str}"
    target_csv = f"{base_dir}/{workspace_name}_{date_str}.csv"

    df_log = df_spark.select(
        "processing_stage", "job_id", "job_run_id", "job_name", "task_run_id",
        "task_name", "retry_number", "status", "error_message",
        "execution_start_time_cet", "execution_end_time_cet",
        "execution_time_min", "notebook_path", "execution_date"
    )

    (df_log.coalesce(1)
     .write
     .format("csv")
     .option("header", "true")
     .mode("overwrite")
     .save(target_csv))

    print(f"CSV saved to: {target_csv}")
    return target_csv

# COMMAND ----------

if __name__ == "__main__":
    # Resolve job IDs
    job_ids =JOB_ID_LIST_FROM_ENV

    if not job_ids:
        raise ValueError("No job IDs found. Set JOB_ID_LIST env var")

    print("Processing job IDs:", job_ids)
    records = collect_job_task_runs_for_ids(job_ids, hours_back=HOURS_BACK)
    print(f"Collected {len(records)} task run records.")

    target_path = write_as_single_csv_to_abfs_spark(
        records=records,
        storage_account=STORAGE_ACCOUNT_NAME,
        container=CONTAINER_NAME,
        use_utc_date=USE_UTC_DATE_FOLDER
    )
    print("Output:", target_path)


# COMMAND ----------

# DBTITLE 1,Read 30 days files

from datetime import datetime
import re
from pyspark.sql.types import StructType

ROOT = f"abfss://radar-logs@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/" 

# 1) List date folders and select last 30 (descending)
dirs = [e.name.rstrip("/") for e in dbutils.fs.ls(ROOT) if re.fullmatch(r"\d{8}", e.name.rstrip("/"))]
last30 = sorted(dirs, key=lambda s: datetime.strptime(s, "%Y%m%d"), reverse=True)[:30]
paths = [f"{ROOT}{d}/" for d in last30]

# 2) Read all CSVs inside those folders (recursively)
try:
    df = (spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("recursiveFileLookup", "true")
          .load(paths))
    df = df.withColumn("execution_date", col("execution_date").cast("string"))
except Exception:
    df = spark.createDataFrame([], StructType([]))  # empty DF if nothing matched

df.createOrReplaceTempView("logs")
spark.sql('select * from logs').write.mode('overwrite').option("mergeSchema", "true").saveAsTable('radar.radar_logging_monitoring')

# COMMAND ----------

display(spark.table('radar.radar_logging_monitoring'))
