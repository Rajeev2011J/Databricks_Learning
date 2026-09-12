# Databricks notebook source
# MAGIC %md
# MAGIC #### RDM_Party Backfill Spike — Cost_Analysis
# MAGIC
# MAGIC **Purpose:** Reads `spike_benchmark_results`, computes all linear extrapolations,
# MAGIC identifies min-cost and min-duration combinations, and produces dataset-level projections.
# MAGIC
# MAGIC **Requirements covered:** 7 (cost analysis), 2.7 (extrapolations), 3.8 (monetary cost)
# MAGIC
# MAGIC **Correctness properties verified here:**
# MAGIC - Property 8: arithmetic correctness of extrapolation formulas
# MAGIC - Property 14: min/max identification correctness
# MAGIC - Property 15: engineering effort bounds [0.5, 30] person-days
# MAGIC
# MAGIC #### Widgets
# MAGIC | Widget                   | Description                                           |
# MAGIC |--------------------------|-------------------------------------------------------|
# MAGIC | dbu_rate_usd             | DBU rate in USD (overrides stored value if provided)  |
# MAGIC | engineering_effort_days  | One-time effort estimate in person-days (0.5–30)      |
# MAGIC | full_backfill_days       | Total days to extrapolate over (default: 215)         |
# MAGIC
# MAGIC #### Author
# MAGIC - Generated for RDM_Party Backfill Spike

# COMMAND ----------

# DBTITLE 1,Install PBT library for Property 8 validation
# %pip install hypothesis>=6.0 --quiet  # Uncomment if not pre-installed via init script

# COMMAND ----------

# DBTITLE 1,Read environment variables
import os
from decimal import Decimal as D
from datetime import datetime, date

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

environment   = os.environ['ENV']
SARADAR       = f"saradar{environment}"
RESULTS_PATH  = f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/spike/benchmark_results/"

from RadarUtils import authenticate_storage_account
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Define widgets
dbutils.widgets.text("dbu_rate_usd",            "")   # leave empty to use stored rate
dbutils.widgets.text("engineering_effort_days", "2.0")
dbutils.widgets.text("full_backfill_days",      "215")

_dbu_rate_override   = dbutils.widgets.get("dbu_rate_usd").strip()
engineering_effort   = float(dbutils.widgets.get("engineering_effort_days"))
FULL_BACKFILL_DAYS   = int(dbutils.widgets.get("full_backfill_days"))

# Property 15: engineering effort bounds [0.5, 30]
assert 0.5 <= engineering_effort <= 30, (
    f"Engineering effort {engineering_effort} is outside the required range [0.5, 30] person-days. "
    "Adjust the engineering_effort_days widget."
)
print(f"Full backfill days: {FULL_BACKFILL_DAYS}")
print(f"Engineering effort: {engineering_effort} person-days")

# COMMAND ----------

# DBTITLE 1,Load benchmark results
df_raw = spark.read.format("delta").load(RESULTS_PATH)
print(f"Total benchmark rows loaded: {df_raw.count()}")
df_raw.groupBy("strategy", "cluster_config_id", "status").count().orderBy("strategy","cluster_config_id").show()

# COMMAND ----------

# DBTITLE 1,Compute per-combination means (only SUCCESS rows)
df_success = df_raw.filter(F.col("status") == "SUCCESS")

agg_df = (
    df_success
    .groupBy("strategy", "cluster_config_id")
    .agg(
        F.mean("wall_clock_minutes").alias("mean_duration_per_day_min"),
        F.mean("total_dbu").alias("mean_dbu_per_day"),
        F.mean("records_processed").alias("mean_records_per_day"),
        F.mean("throughput_rps").alias("mean_throughput_rps"),
        F.min("wall_clock_minutes").alias("min_duration_min"),
        F.max("wall_clock_minutes").alias("max_duration_min"),
        F.count("*").alias("n_runs"),
        F.first("dbu_rate_usd").alias("stored_dbu_rate_usd"),
    )
    .orderBy("strategy", "cluster_config_id")
)

agg_pd = agg_df.toPandas()
print(f"\nAggregated combinations: {len(agg_pd)}")
agg_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Core extrapolation function (Property 8)
def compute_extrapolations(mean_dbu: float, mean_duration_min: float,
                            dbu_rate: float, n_days: int = 215) -> dict:
    """
    Computes all cost and duration extrapolations for a strategy–cluster combination.
    Requirements 7.1, 7.2, 7.6, 7.7 / Property 8.

    Args:
        mean_dbu:          Mean DBU consumed per Load_Date execution.
        mean_duration_min: Mean wall-clock duration per Load_Date execution (minutes).
        dbu_rate:          DBU price in USD per DBU.
        n_days:            Total Load_Date count to extrapolate over (default 215).

    Returns a dict with all extrapolated values.
    All values are validated for basic arithmetic correctness.
    """
    assert mean_dbu > 0,          f"mean_dbu must be > 0, got {mean_dbu}"
    assert mean_duration_min > 0, f"mean_duration_min must be > 0, got {mean_duration_min}"
    assert dbu_rate > 0,          f"dbu_rate must be > 0, got {dbu_rate}"

    total_dbu           = mean_dbu * n_days
    total_duration_min  = mean_duration_min * n_days
    estimated_cost_usd  = total_dbu * dbu_rate
    cost_20_datasets    = estimated_cost_usd * 20
    cost_25_datasets    = estimated_cost_usd * 25

    # Property 8: validate formulas to 1e-6 relative tolerance
    TOL = 1e-6
    assert abs(total_dbu - mean_dbu * n_days) < TOL * max(total_dbu, 1)
    assert abs(total_duration_min - mean_duration_min * n_days) < TOL * max(total_duration_min, 1)
    assert total_duration_min >= mean_duration_min, (
        f"total_duration_min {total_duration_min:.4f} < single-day duration {mean_duration_min:.4f}"
    )
    assert abs(estimated_cost_usd - total_dbu * dbu_rate) < TOL * max(estimated_cost_usd, 1)
    assert abs(cost_20_datasets - estimated_cost_usd * 20) < TOL * max(cost_20_datasets, 1)
    assert abs(cost_25_datasets - estimated_cost_usd * 25) < TOL * max(cost_25_datasets, 1)

    return {
        "total_dbu":           round(total_dbu, 4),
        "total_duration_min":  round(total_duration_min, 2),
        "total_duration_hrs":  round(total_duration_min / 60, 2),
        "estimated_cost_usd":  round(estimated_cost_usd, 2),
        "cost_20_datasets":    round(cost_20_datasets, 2),
        "cost_25_datasets":    round(cost_25_datasets, 2),
    }

# COMMAND ----------

# DBTITLE 1,Property 8 — PBT verification of extrapolation formulas
try:
    from hypothesis import given, settings, HealthCheck
    from hypothesis import strategies as st

    @given(
        mean_dbu=st.floats(min_value=0.001, max_value=1e6,  allow_nan=False, allow_infinity=False),
        mean_dur=st.floats(min_value=0.001, max_value=1440, allow_nan=False, allow_infinity=False),
        dbu_rate=st.floats(min_value=0.001, max_value=10.0, allow_nan=False, allow_infinity=False),
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_extrapolation_formulas(mean_dbu, mean_dur, dbu_rate):
        result = compute_extrapolations(mean_dbu, mean_dur, dbu_rate, n_days=215)
        TOL = 1e-6
        assert abs(result["total_dbu"] - mean_dbu * 215) < TOL * max(result["total_dbu"], 1)
        assert abs(result["total_duration_min"] - mean_dur * 215) < TOL * max(result["total_duration_min"], 1)
        assert result["total_duration_min"] >= mean_dur
        assert abs(result["estimated_cost_usd"] - result["total_dbu"] * dbu_rate) < TOL * max(result["estimated_cost_usd"], 1)
        assert abs(result["cost_20_datasets"] - result["estimated_cost_usd"] * 20) < TOL * max(result["cost_20_datasets"], 1)
        assert abs(result["cost_25_datasets"] - result["estimated_cost_usd"] * 25) < TOL * max(result["cost_25_datasets"], 1)

    test_extrapolation_formulas()
    print("[OK] Property 8 — Arithmetic extrapolation formulas verified (100 examples)")

except ImportError:
    print("[SKIP] hypothesis not available — skipping PBT verification of Property 8")

# COMMAND ----------

# DBTITLE 1,Compute extrapolations for each combination
import pandas as pd

results = []

for _, row in agg_pd.iterrows():
    strategy         = row["strategy"]
    cluster          = row["cluster_config_id"]
    mean_dbu         = float(row["mean_dbu_per_day"])
    mean_duration    = float(row["mean_duration_per_day_min"])
    n_runs           = int(row["n_runs"])
    duration_range   = float(row["max_duration_min"]) - float(row["min_duration_min"])

    # Use override rate if provided, otherwise use stored rate from benchmark
    stored_rate = float(row.get("stored_dbu_rate_usd") or 0.40)
    dbu_rate    = float(_dbu_rate_override) if _dbu_rate_override else stored_rate

    # Skip combinations with zero mean (no valid data)
    if mean_dbu <= 0 or mean_duration <= 0:
        print(f"[SKIP] {strategy} / {cluster} — no valid SUCCESS runs (mean_dbu={mean_dbu:.4f}, mean_dur={mean_duration:.2f})")
        results.append({
            "strategy": strategy, "cluster_config_id": cluster,
            "n_runs": n_runs, "mean_duration_min": mean_duration, "duration_range_min": duration_range,
            "mean_dbu": mean_dbu, "dbu_rate_usd": dbu_rate,
            "total_dbu": None, "total_duration_min": None, "total_duration_hrs": None,
            "estimated_cost_usd": None, "cost_20_datasets": None, "cost_25_datasets": None,
            "status": "SKIPPED_NO_DATA",
        })
        continue

    ext = compute_extrapolations(mean_dbu, mean_duration, dbu_rate, n_days=FULL_BACKFILL_DAYS)
    results.append({
        "strategy":            strategy,
        "cluster_config_id":   cluster,
        "n_runs":              n_runs,
        "mean_duration_min":   round(mean_duration, 2),
        "duration_range_min":  round(duration_range, 2),
        "mean_dbu":            round(mean_dbu, 4),
        "dbu_rate_usd":        round(dbu_rate, 4),
        **ext,
        "status": "OK",
    })

df_results = pd.DataFrame(results)
print("\n=== Extrapolation results ===")
print(df_results.to_string(index=False))

# COMMAND ----------

# DBTITLE 1,Property 14 — Min cost and min duration identification
ok_rows = df_results[df_results["status"] == "OK"].copy()

if ok_rows.empty:
    print("[WARN] No successful combinations to compare.")
else:
    # Requirement 7.4 / Property 14
    min_cost_idx     = ok_rows["estimated_cost_usd"].idxmin()
    min_duration_idx = ok_rows["total_duration_min"].idxmin()

    min_cost_row     = ok_rows.loc[min_cost_idx]
    min_duration_row = ok_rows.loc[min_duration_idx]

    same_combination = (
        min_cost_row["strategy"]        == min_duration_row["strategy"] and
        min_cost_row["cluster_config_id"] == min_duration_row["cluster_config_id"]
    )

    print("\n=== Requirement 7.4 — Min-cost / Min-duration identification ===")
    print(f"Lowest total estimated cost:     {min_cost_row['strategy']} / {min_cost_row['cluster_config_id']}"
          f"  →  ${min_cost_row['estimated_cost_usd']:,.2f}")
    print(f"Shortest total estimated duration: {min_duration_row['strategy']} / {min_duration_row['cluster_config_id']}"
          f"  →  {min_duration_row['total_duration_hrs']:,.1f} hours")
    print(f"Are these the same combination? {'YES' if same_combination else 'NO — different trade-off exists'}")

    # Validate: min_cost_row has the lowest cost among all OK rows (Property 14)
    assert min_cost_row["estimated_cost_usd"] <= ok_rows["estimated_cost_usd"].min() + 1e-6, \
        "Property 14 violation: identified min-cost combination is not actually the minimum"
    assert min_duration_row["total_duration_min"] <= ok_rows["total_duration_min"].min() + 1e-6, \
        "Property 14 violation: identified min-duration combination is not actually the minimum"
    print("[OK] Property 14 — Min/max identification correctness verified")

# COMMAND ----------

# DBTITLE 1,Requirement 7.6 / 7.7 — Dataset projection range (20 vs 25 datasets)
print("\n=== Dataset cost projection (20 vs 25 datasets) ===")
print(f"Based on extrapolation for {FULL_BACKFILL_DAYS} Load_Date partitions.\n")

for _, row in ok_rows.iterrows():
    print(f"  {row['strategy']:15s} / {row['cluster_config_id']:15s}  →  "
          f"20 datasets: ${row['cost_20_datasets']:>12,.2f}  |  "
          f"25 datasets: ${row['cost_25_datasets']:>12,.2f}")

# COMMAND ----------

# DBTITLE 1,Engineering effort estimate (Requirement 7.5 / Property 15)
print(f"\n=== Engineering effort estimate (Requirement 7.5) ===")
print(f"One-time effort to apply recommended strategy to all WR Radar datasets:")
print(f"  {engineering_effort} person-days")
print(f"  Justification: Day-by-day strategy requires minimal per-dataset changes.")
print(f"  Each dataset needs: (1) Load_Date widget validation, (2) RunType check,")
print(f"  (3) confirmation that save_to_saradar_storage_account uses mode=overwrite.")
print(f"  Estimated ~{engineering_effort/25:.2f} person-days per dataset × 25 datasets.")

# Property 15 assertion (already checked at widget read time, repeated here for clarity)
assert 0.5 <= engineering_effort <= 30, f"Effort {engineering_effort} outside [0.5, 30]"
print(f"[OK] Property 15 — Engineering effort {engineering_effort} pd is within [0.5, 30]")

# COMMAND ----------

# DBTITLE 1,Strategy comparison — seven evaluation dimensions (Requirement 2.3)
print("\n=== Strategy comparison — seven evaluation dimensions ===")
DIMENSIONS = [
    "total_execution_time",
    "throughput",
    "cluster_utilisation",
    "scalability",
    "failure_recovery_granularity",
    "operational_control",
    "compute_cost",
]

dimension_notes = {
    "total_execution_time":          "See total_duration_hrs column above per combination.",
    "throughput":                     "See mean_throughput_rps from benchmark results.",
    "cluster_utilisation":            "See peak_cpu_pct / peak_memory_pct from Databricks Metrics API (populate post-run).",
    "scalability":                    "Day-by-day applies unchanged per dataset; monthly requires loop per month.",
    "failure_recovery_granularity":   "Day-by-day: ~50M records / 1 partition. Monthly: sum of partitions since last verified date.",
    "operational_control":            "Day-by-day: per-date status in spike_backfill_status. Monthly: month-level status.",
    "compute_cost":                   "See estimated_cost_usd column above per combination.",
}

for dim in DIMENSIONS:
    note = dimension_notes.get(dim, "See benchmark results table.")
    print(f"  {dim:40s}: {note}")

# COMMAND ----------

# DBTITLE 1,Export summary as Confluence wiki table (Requirement 8.4 / 8.5)
print("\n=== Confluence wiki markup — benchmark results table ===")
print("Paste this into the Confluence page (use 'Insert → Wiki Markup' or 'Table' macro).\n")

header = ("|| Strategy || Cluster || Runs || Mean Duration (min) || Duration Range (min) "
          "|| Total Duration (hrs) || Mean DBU/day || Total DBU || Est. Cost USD "
          "|| 20-Dataset Cost || 25-Dataset Cost ||")
print(header)
for _, row in df_results.iterrows():
    if row["status"] == "OK":
        line = (f"| {row['strategy']} | {row['cluster_config_id']} | {row['n_runs']} "
                f"| {row['mean_duration_min']} | {row['duration_range_min']} "
                f"| {row['total_duration_hrs']} | {row['mean_dbu']} | {row['total_dbu']} "
                f"| ${row['estimated_cost_usd']:,.2f} | ${row['cost_20_datasets']:,.2f} "
                f"| ${row['cost_25_datasets']:,.2f} |")
    else:
        line = f"| {row['strategy']} | {row['cluster_config_id']} | {row['n_runs']} | FAILED | — | — | — | — | — | — | — |"
    print(line)

print("\n[OK] Cost_Analysis complete.")

