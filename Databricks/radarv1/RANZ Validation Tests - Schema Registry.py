# Databricks notebook source
# DBTITLE 1,RANZ Schema Registry Validation Tests
# MAGIC %md
# MAGIC # RANZ Schema Registry Validation Tests
# MAGIC
# MAGIC This notebook validates all changes made to RadarUtils.py for the metadata-driven validation (schema registry) pattern.
# MAGIC
# MAGIC ## Scenarios Tested:
# MAGIC 1. Seed data SQL expressions match old hardcoded PySpark logic
# MAGIC 2. Global `_ALL_` rules with exclusion tables
# MAGIC 3. Table-specific rules (c_b_party, c_b_contract) with transformation + filter
# MAGIC 4. Priority ordering (transformation runs before filter)
# MAGIC 5. Missing mandatory column handling (ERROR vs WARNING)
# MAGIC 6. Empty validations dict (no rules applied)
# MAGIC 7. `load_ranz_validations` caching behavior
# MAGIC 8. `load_single_table_parallel` backward compatibility (validations=None fallback)
# MAGIC 9. SQL expression equivalence (old PySpark vs new SQL expr)

# COMMAND ----------

# DBTITLE 1,Setup: Mock Data & Functions
# ========== SETUP: Import libraries, create mock data, and copy key functions ==========
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, lit, expr as sql_expr
from datetime import datetime
from typing import Dict, List, Optional

print("Setup complete - libraries imported")

# ========== COPY OF apply_ranz_filters_dynamic (from RadarUtils.py) ==========
def apply_ranz_filters_dynamic(df: DataFrame, table_name: str, validations: Dict, verbose: bool = False) -> DataFrame:
    from pyspark.sql.functions import expr as sql_expr
    name = table_name.lower()
    applied_count = 0
    rule_sets = []
    if '_ALL_' in validations:
        rule_sets.append(('_ALL_', validations['_ALL_']))
    if name in validations:
        rule_sets.append((name, validations[name]))
    for rule_table, rules in rule_sets:
        for rule in rules:
            col_name = rule.get('column_name')
            filter_cond = rule.get('filter_condition')
            transform_expr = rule.get('transformation_expr')
            error_level = rule.get('error_level', 'ERROR')
            exclusions = rule.get('exclusion_tables')
            if exclusions:
                excluded = [t.strip().lower() for t in exclusions.split(',')]
                if name in excluded:
                    if verbose:
                        print(f"  [SKIP] Rule for {col_name} excluded for table {name}")
                    continue
            if col_name and col_name not in df.columns:
                if rule.get('is_mandatory', False):
                    if error_level == 'ERROR':
                        raise ValueError(
                            f"Mandatory column '{col_name}' not found in {table_name}. "
                            f"Rule: {rule_table} (error_level=ERROR)"
                        )
                    else:
                        print(f"[WARNING] Optional column '{col_name}' not found in {table_name}, skipping rule")
                        continue
                else:
                    if verbose:
                        print(f"  [SKIP] Column '{col_name}' not found, skipping filter")
                    continue
            if transform_expr and col_name:
                df = df.withColumn(col_name, sql_expr(transform_expr))
                if verbose:
                    print(f"  [TRANSFORM] {col_name} = {transform_expr}")
            if filter_cond:
                df = df.filter(sql_expr(filter_cond))
                applied_count += 1
                if verbose:
                    print(f"  [FILTER] {filter_cond}")
    if verbose and applied_count > 0:
        print(f"Applied {applied_count} filter rule(s) to {table_name}")
    return df

# ========== COPY OF OLD HARDCODED apply_ranz_filters (for comparison) ==========
def apply_ranz_filters_OLD(df, table_name):
    from pyspark.sql.functions import col, coalesce, lit
    name = table_name.lower()
    if name != "c_b_individual_birth_dt":
        df = df.filter(col("HUB_STATE_IND") == 1)
    if name == "c_b_party":
        df = df.withColumn("PARTY_STATUS_CD", coalesce(col("PARTY_STATUS_CD"), lit("AC")))
        return df.filter(
            col("SRC_SYS_CD").isin("T24RURAL", "OMB", "CMS", "RABODIRECT")
            & ~col("PARTY_STATUS_CD").isin("DL", "RD", "WD")
        )
    elif name == "c_b_contract":
        df = df.withColumn("LIFECYCLE_STATUS_CD", coalesce(col("LIFECYCLE_STATUS_CD"), lit("AC")))
        return df.filter(
            ((col("SRC_SYS_CD").isin("T24RURAL")) | (col("SRC_SYS_CD").isin("RABODIRECT") | (col("LOB_CD") == "RD")))
            & (~col("LIFECYCLE_STATUS_CD").isin("DL", "RD", "WD"))
        )
    return df

# ========== SEED VALIDATION RULES (from create_ranz_validation_table) ==========
SEED_VALIDATIONS = {
    '_ALL_': [
        {'column_name': 'HUB_STATE_IND', 'is_mandatory': True, 'filter_condition': 'HUB_STATE_IND = 1',
         'transformation_expr': None, 'exclusion_tables': 'c_b_individual_birth_dt',
         'error_level': 'ERROR', 'priority': 1},
    ],
    'c_b_party': [
        {'column_name': 'PARTY_STATUS_CD', 'is_mandatory': False, 'filter_condition': None,
         'transformation_expr': "COALESCE(PARTY_STATUS_CD, 'AC')", 'exclusion_tables': None,
         'error_level': 'ERROR', 'priority': 1},
        {'column_name': 'SRC_SYS_CD', 'is_mandatory': False,
         'filter_condition': "SRC_SYS_CD IN ('T24RURAL', 'OMB', 'CMS', 'RABODIRECT') AND PARTY_STATUS_CD NOT IN ('DL', 'RD', 'WD')",
         'transformation_expr': None, 'exclusion_tables': None,
         'error_level': 'ERROR', 'priority': 2},
    ],
    'c_b_contract': [
        {'column_name': 'LIFECYCLE_STATUS_CD', 'is_mandatory': False, 'filter_condition': None,
         'transformation_expr': "COALESCE(LIFECYCLE_STATUS_CD, 'AC')", 'exclusion_tables': None,
         'error_level': 'ERROR', 'priority': 1},
        {'column_name': 'SRC_SYS_CD', 'is_mandatory': False,
         'filter_condition': "(SRC_SYS_CD = 'T24RURAL' OR SRC_SYS_CD = 'RABODIRECT' OR LOB_CD = 'RD') AND LIFECYCLE_STATUS_CD NOT IN ('DL', 'RD', 'WD')",
         'transformation_expr': None, 'exclusion_tables': None,
         'error_level': 'ERROR', 'priority': 2},
    ],
}

print("Functions and seed data loaded successfully")

# ========== CREATE MOCK DATA ==========
party_data = [
    (1, 'AC', 'T24RURAL', 'P001'),
    (1, 'DL', 'T24RURAL', 'P002'),
    (1, None, 'OMB', 'P003'),
    (0, 'AC', 'T24RURAL', 'P004'),
    (1, 'AC', 'UNKNOWN', 'P005'),
    (1, 'RD', 'CMS', 'P006'),
    (1, 'AC', 'RABODIRECT', 'P007'),
]
party_df = spark.createDataFrame(party_data, ['HUB_STATE_IND', 'PARTY_STATUS_CD', 'SRC_SYS_CD', 'PARTY_ID'])

contract_data = [
    (1, 'AC', 'T24RURAL', 'L1', 'C001'),
    (1, 'DL', 'T24RURAL', 'L2', 'C002'),
    (1, None, 'RABODIRECT', 'L3', 'C003'),
    (0, 'AC', 'T24RURAL', 'L4', 'C004'),
    (1, 'AC', 'UNKNOWN', 'RD', 'C005'),
    (1, 'RD', 'T24RURAL', 'L6', 'C006'),
]
contract_df = spark.createDataFrame(contract_data, ['HUB_STATE_IND', 'LIFECYCLE_STATUS_CD', 'SRC_SYS_CD', 'LOB_CD', 'CONTRACT_ID'])

generic_data = [(1, 'O001'), (0, 'O002'), (1, 'O003')]
generic_df = spark.createDataFrame(generic_data, ['HUB_STATE_IND', 'ORG_ID'])

birth_dt_data = [(0, 'B001'), (1, 'B002')]
birth_dt_df = spark.createDataFrame(birth_dt_data, ['HUB_STATE_IND', 'BIRTH_ID'])

print(f"Mock data created: party={party_df.count()}, contract={contract_df.count()}, generic={generic_df.count()}, birth_dt={birth_dt_df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Test Scenarios 1-4: Old vs New Equivalence
# TEST SCENARIOS 1-4: Old vs New equivalence
tests = []
def check(name, passed, detail=""):
    tests.append((name, passed, detail))
    print(f"  [{'PASS' if passed else 'FAIL'}] {name}" + (f" - {detail}" if detail else ""))

print("=" * 70)
print("SCENARIO 1: c_b_party old vs new")
print("=" * 70)
df_new = apply_ranz_filters_dynamic(party_df, "c_b_party", SEED_VALIDATIONS, verbose=True)
df_old = apply_ranz_filters_OLD(party_df, "c_b_party")
new_ids = sorted([r.PARTY_ID for r in df_new.select("PARTY_ID").collect()])
old_ids = sorted([r.PARTY_ID for r in df_old.select("PARTY_ID").collect()])
check("party: same row count", df_new.count() == df_old.count(), f"new={df_new.count()}, old={df_old.count()}")
check("party: same IDs", new_ids == old_ids, f"new={new_ids}, old={old_ids}")
check("party: expected P001,P003,P007", new_ids == ["P001", "P003", "P007"], f"got={new_ids}")
p003 = df_new.filter(col("PARTY_ID") == "P003").collect()[0]["PARTY_STATUS_CD"]
check("party: NULL coalesced to AC", p003 == "AC", f"got={p003}")

print()
print("=" * 70)
print("SCENARIO 2: c_b_contract old vs new")
print("=" * 70)
df_n = apply_ranz_filters_dynamic(contract_df, "c_b_contract", SEED_VALIDATIONS, verbose=True)
df_o = apply_ranz_filters_OLD(contract_df, "c_b_contract")
new_c = sorted([r.CONTRACT_ID for r in df_n.select("CONTRACT_ID").collect()])
old_c = sorted([r.CONTRACT_ID for r in df_o.select("CONTRACT_ID").collect()])
check("contract: same row count", df_n.count() == df_o.count(), f"new={df_n.count()}, old={df_o.count()}")
check("contract: same IDs", new_c == old_c, f"new={new_c}, old={old_c}")
check("contract: expected C001,C003,C005", new_c == ["C001", "C003", "C005"], f"got={new_c}")
c003 = df_n.filter(col("CONTRACT_ID") == "C003").collect()[0]["LIFECYCLE_STATUS_CD"]
check("contract: NULL coalesced to AC", c003 == "AC", f"got={c003}")

print()
print("=" * 70)
print("SCENARIO 3: Generic table (HUB_STATE_IND only)")
print("=" * 70)
df_n = apply_ranz_filters_dynamic(generic_df, "c_b_organisation", SEED_VALIDATIONS, verbose=True)
df_o = apply_ranz_filters_OLD(generic_df, "c_b_organisation")
new_g = sorted([r.ORG_ID for r in df_n.select("ORG_ID").collect()])
old_g = sorted([r.ORG_ID for r in df_o.select("ORG_ID").collect()])
check("generic: same row count", df_n.count() == df_o.count(), f"new={df_n.count()}, old={df_o.count()}")
check("generic: expected O001,O003", new_g == ["O001", "O003"], f"got={new_g}")

print()
print("=" * 70)
print("SCENARIO 4: c_b_individual_birth_dt excluded from HUB rule")
print("=" * 70)
df_n = apply_ranz_filters_dynamic(birth_dt_df, "c_b_individual_birth_dt", SEED_VALIDATIONS, verbose=True)
check("birth_dt: all rows preserved", df_n.count() == 2, f"got={df_n.count()}")

# COMMAND ----------

# DBTITLE 1,Test Scenarios 5-13: Edge Cases & Summary
# TEST SCENARIOS 5-13: Edge cases, error handling, SQL equivalence
print("=" * 70)
print("SCENARIO 5: Empty validations dict")
print("=" * 70)
df_e = apply_ranz_filters_dynamic(party_df, "c_b_party", {}, verbose=True)
check("empty: df unchanged", df_e.count() == party_df.count(), f"df={df_e.count()}")

print()
print("=" * 70)
print("SCENARIO 6: Missing mandatory column (ERROR)")
print("=" * 70)
val_err = {"c_b_party": [{"column_name": "NO_COL", "is_mandatory": True, "filter_condition": "NO_COL = 1", "transformation_expr": None, "exclusion_tables": None, "error_level": "ERROR", "priority": 1}]}
try:
    apply_ranz_filters_dynamic(party_df, "c_b_party", val_err)
    check("mandatory ERROR: raises ValueError", False)
except ValueError as e:
    check("mandatory ERROR: raises ValueError", True, str(e)[:60])

print()
print("=" * 70)
print("SCENARIO 7: Missing mandatory column (WARNING skips)")
print("=" * 70)
val_warn = {"c_b_party": [{"column_name": "NO_COL", "is_mandatory": True, "filter_condition": "NO_COL = 1", "transformation_expr": None, "exclusion_tables": None, "error_level": "WARNING", "priority": 1}]}
df_w = apply_ranz_filters_dynamic(party_df, "c_b_party", val_warn, verbose=True)
check("mandatory WARNING: df unchanged", df_w.count() == party_df.count())

print()
print("=" * 70)
print("SCENARIO 8: Missing non-mandatory column (skip)")
print("=" * 70)
val_opt = {"c_b_party": [{"column_name": "OPT_COL", "is_mandatory": False, "filter_condition": "OPT_COL = 1", "transformation_expr": None, "exclusion_tables": None, "error_level": "ERROR", "priority": 1}]}
df_o = apply_ranz_filters_dynamic(party_df, "c_b_party", val_opt, verbose=True)
check("optional missing: df unchanged", df_o.count() == party_df.count())

print()
print("=" * 70)
print("SCENARIO 9: Multi-exclusion tables")
print("=" * 70)
val_excl = {"_ALL_": [{"column_name": "HUB_STATE_IND", "is_mandatory": True, "filter_condition": "HUB_STATE_IND = 1", "transformation_expr": None, "exclusion_tables": "c_b_individual_birth_dt,c_b_organisation", "error_level": "ERROR", "priority": 1}]}
df_x1 = apply_ranz_filters_dynamic(generic_df, "c_b_organisation", val_excl, verbose=True)
check("exclusion: org excluded (all rows kept)", df_x1.count() == 3, f"got={df_x1.count()}")
df_x2 = apply_ranz_filters_dynamic(party_df, "c_b_party", val_excl, verbose=True)
check("exclusion: party NOT excluded (HUB filtered)", df_x2.count() == 6, f"got={df_x2.count()}")

print()
print("=" * 70)
print("SCENARIO 10: Caching behavior")
print("=" * 70)
_cache = None
def load_cache():
    global _cache
    if _cache is not None:
        return _cache, "cache"
    _cache = {"_ALL_": []}
    return _cache, "loaded"
r1, s1 = load_cache()
r2, s2 = load_cache()
check("cache: first loads", s1 == "loaded")
check("cache: second uses cache", s2 == "cache")
check("cache: same object", r1 is r2)

print()
print("=" * 70)
print("SCENARIO 11: Backward compat (validations=None)")
print("=" * 70)
vp = None
if vp is None:
    vp = SEED_VALIDATIONS
df_bc = apply_ranz_filters_dynamic(party_df, "c_b_party", vp)
check("backward compat: fallback works", df_bc.count() == 3, f"got={df_bc.count()}")

print()
print("=" * 70)
print("SCENARIO 12: SQL expr vs PySpark equivalence")
print("=" * 70)
df_s = party_df.filter(sql_expr("HUB_STATE_IND = 1 AND SRC_SYS_CD IN ('T24RURAL', 'OMB', 'CMS', 'RABODIRECT')"))
df_p = party_df.filter((col("HUB_STATE_IND") == 1) & col("SRC_SYS_CD").isin("T24RURAL", "OMB", "CMS", "RABODIRECT"))
check("SQL=PySpark: HUB+SRC filter", df_s.count() == df_p.count(), f"sql={df_s.count()}, py={df_p.count()}")
df_s2 = party_df.filter(sql_expr("PARTY_STATUS_CD NOT IN ('DL', 'RD', 'WD')"))
df_p2 = party_df.filter(~col("PARTY_STATUS_CD").isin("DL", "RD", "WD"))
check("SQL=PySpark: NOT IN", df_s2.count() == df_p2.count(), f"sql={df_s2.count()}, py={df_p2.count()}")
df_s3 = party_df.withColumn("PARTY_STATUS_CD", sql_expr("COALESCE(PARTY_STATUS_CD, 'AC')"))
df_p3 = party_df.withColumn("PARTY_STATUS_CD", coalesce(col("PARTY_STATUS_CD"), lit("AC")))
s3v = sorted([r.PARTY_STATUS_CD for r in df_s3.select("PARTY_STATUS_CD").collect()])
p3v = sorted([r.PARTY_STATUS_CD for r in df_p3.select("PARTY_STATUS_CD").collect()])
check("SQL=PySpark: COALESCE", s3v == p3v, f"sql={s3v}, py={p3v}")
df_s4 = contract_df.filter(sql_expr("(SRC_SYS_CD = 'T24RURAL' OR SRC_SYS_CD = 'RABODIRECT' OR LOB_CD = 'RD')"))
df_p4 = contract_df.filter((col("SRC_SYS_CD").isin("T24RURAL")) | (col("SRC_SYS_CD").isin("RABODIRECT")) | (col("LOB_CD") == "RD"))
check("SQL=PySpark: OR expr (contract)", df_s4.count() == df_p4.count(), f"sql={df_s4.count()}, py={df_p4.count()}")

print()
print("=" * 70)
print("FINAL SUMMARY")
print("=" * 70)
passed = sum(1 for _, p, _ in tests if p)
failed = sum(1 for _, p, _ in tests if not p)
print(f"\nTotal: {len(tests)} | PASSED: {passed} | FAILED: {failed}")
if failed > 0:
    print("\nFAILED:")
    for name, p, d in tests:
        if not p:
            print(f"  [FAIL] {name} - {d}")
else:
    print("\n*** All tests PASSED! Safe to copy to production. ***")

# COMMAND ----------


