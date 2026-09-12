# Centralized pytest for Databricks Asset Bundles

**Project:** R-FEC-RADAR-1  
**Document type:** Requirements & Implementation Design  
**Scope:** `Databricks/` directory — all six Asset Bundles  
**Date:** September 2026  

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Goals and Non-Goals](#2-goals-and-non-goals)
3. [Requirements](#3-requirements)
4. [Architecture Overview](#4-architecture-overview)
5. [Directory Layout](#5-directory-layout)
6. [Component Design](#6-component-design)
   - 6.1 [pyproject.toml — Central Configuration](#61-pyprojecttoml--central-configuration)
   - 6.2 [requirements-dev.txt — Pinned Dependencies](#62-requirements-devtxt--pinned-dependencies)
   - 6.3 [tests/conftest.py — Shared Fixtures](#63-testsconftestpy--shared-fixtures)
   - 6.4 [tests/fixtures/sample_dataframes.py — Test Data](#64-testsfixturessample_dataframespy--test-data)
   - 6.5 [tests/utils/df_helpers.py — Assertion Helpers](#65-testsutilsdf_helperspy--assertion-helpers)
7. [Bundle-Specific Test Files](#7-bundle-specific-test-files)
   - 7.1 [GCOB_Consumer — test_gcob_utils.py](#71-gcob_consumer--test_gcob_utilspy)
   - 7.2 [GCOB_Reportingv1 — test_dq_validation.py](#72-gcob_reportingv1--test_dq_validationpy)
   - 7.3 [radarv1 — test_functions_databricks.py](#73-radarv1--test_functions_databrickspy)
   - 7.4 [radarv1 — test_radar_utils.py](#74-radarv1--test_radar_utilspy)
   - 7.5 [clusters — test_clusters_config.py](#75-clusters--test_clusters_configpy)
   - 7.6 [AdHocRequests — test_adhoc_config.py](#76-adhoc_requests--test_adhoc_configpy)
   - 7.7 [PartyCatalog — test_party_catalog_config.py](#77-partycatalog--test_party_catalog_configpy)
8. [Test Markers and Execution Modes](#8-test-markers-and-execution-modes)
9. [run_tests.ps1 — Entry Point](#9-run_testsps1--entry-point)
10. [Coverage Configuration](#10-coverage-configuration)
11. [The Databricks Runtime Mocking Strategy](#11-the-databricks-runtime-mocking-strategy)
12. [Constraints and Trade-offs](#12-constraints-and-trade-offs)
13. [What is NOT tested (and why)](#13-what-is-not-tested-and-why)
14. [Extending the Suite](#14-extending-the-suite)
15. [Dependency Version Rationale](#15-dependency-version-rationale)

---

## 1. Problem Statement

### Background

The RADAR platform at Rabobank consists of six Databricks Asset Bundles (`radarv1`, `GCOB_Consumer`, `GCOB_Reportingv1`, `clusters`, `AdHocRequests`, `PartyCatalog`) that together process KYC/AML party and risk data from sources including Dataverse, Azure Data Lake Storage (GDP), CosmosDB, RiskShield, and several internal systems.

Each bundle contains Python notebooks and utility modules that are deployed and run inside Azure Databricks workspaces across three environments (dev, preprod, prod).

### The Testing Gap

Before this implementation, the testing situation was:

| Bundle | Existing test state |
|---|---|
| `radarv1` | `tests/main_test.py` — auto-generated stub importing `get_taxis` (non-existent function). Fails immediately. |
| `GCOB_Consumer` | `tests/main_test.py` — same auto-generated stub, different bundle name. Fails immediately. |
| `GCOB_Reportingv1` | `tests/main_test.py` — same stub. Also contains `GNS_Test.py` and `GCOB_Producer_DQ_Test.py` — real validation logic but written as Databricks notebooks (using `spark`, `dbutils`, `%sql` magic cells). Cannot run via pytest locally. |
| `clusters` | No tests |
| `AdHocRequests` | No tests |
| `PartyCatalog` | No tests |

This created three concrete problems:

**Problem 1 — No local test execution.**  
Every bundle imports `from databricks.sdk.runtime import spark, dbutils` at module level. Running `pytest` outside of a Databricks cluster environment causes immediate `ImportError` before a single test function is reached. Developers had no way to run any test locally.

**Problem 2 — Fragmented configuration.**  
Each bundle that had tests carried its own `pytest.ini` pointing to its own `tests/` and `src/`. Running the full suite required changing directory into each bundle folder and invoking pytest six times. There was no single command, no aggregate coverage report, and no unified view of the test results.

**Problem 3 — No shared test infrastructure.**  
Each bundle would have needed to independently set up SparkSession creation, Databricks mock stubs, fake environment variables, and DataFrame factory helpers. This duplication would cause maintenance drift and inconsistent test quality across bundles.

### The Root Cause

The notebooks are structured as **top-level scripts**, not modules. Real transformation logic, DQ validation functions, and utility code are mixed with side-effectful code (Azure storage authentication, Hive table writes, env-var reads at import time). This makes them impossible to import in a standard Python environment.

The solution must work *around* this constraint without requiring a full refactor of all notebooks, because:
- Notebooks are the Databricks-native delivery format for this team
- Refactoring all 50+ notebooks would be a multi-sprint effort
- The immediate need is to get a functioning test suite running in CI

---

## 2. Goals and Non-Goals

### Goals

- **Single command** to run all tests across all six bundles from the `Databricks/` root directory.
- **Local execution** without a live Databricks connection, cluster, or Azure credentials.
- **Shared infrastructure** — one SparkSession, one mock layer, one set of assertion helpers, shared across all bundles.
- **Incremental** — the existing bundle structure is preserved. No notebooks are moved or renamed.
- **CI-ready** — non-zero exit code on failure, coverage enforcement, parallel execution support.
- **Extensible** — adding a new bundle test file requires only dropping a new file in `<bundle>/tests/` with no config changes.

### Non-Goals

- Full integration testing against live Azure services (Dataverse, ADLS, Cosmos). That is handled by the existing Databricks Workflow task approach (`GNS_Test.py`, `GCOB_Producer_DQ_Test.py` as job tasks).
- End-to-end notebook execution in pytest. Notebooks remain as Databricks notebook artifacts.
- Replacing the existing per-bundle `pytest.ini` files (they are left in place for backward compatibility when a developer wants to run a single bundle's tests in isolation).
- Test coverage for `PartyCatalog/src/` Python files (the bundle has no Python logic — only YAML resource definitions).

---

## 3. Requirements

### FR-01 — Single invocation point
Running `pytest` from `Databricks/` must discover and execute tests from all six bundles in one pass.

### FR-02 — No Databricks connection required
All tests must pass in a standard Python environment with no Databricks CLI configured, no Azure credentials set, and no VPN connection.

### FR-03 — Shared SparkSession
A single local PySpark SparkSession must be created once per pytest run and shared across all test files. Cold-start cost must be incurred only once.

### FR-04 — Automatic credential mocking
All Azure environment variables (`APP_REG_APP_ID`, `TENANT_ID`, `GDP_STORAGE_NAME`, etc.) must be injected automatically for every test without requiring individual tests to set them up.

### FR-05 — Databricks runtime stubbing
`from databricks.sdk.runtime import spark, dbutils` and `from pyspark.dbutils import DBUtils` must resolve without `ImportError` when source files are imported in test context.

### FR-06 — Namespace isolation between bundles
`GCOB_Consumer/src/` and `GCOB_Reportingv1/src/` both contain files named similarly (`GcobUtils.py`, `RadarUtils.py`). Test imports from one bundle must not shadow or be shadowed by files from another.

### FR-07 — Test markers for selective execution
Tests must be tagged with pytest markers (`unit`, `spark`, `dq`, `radarv1`, `gcob_consumer`, etc.) so CI pipelines can run fast unit tests separately from slower Spark tests.

### FR-08 — Coverage reporting
Branch coverage must be measured across all bundle `src/` paths and a minimum threshold (60%) enforced in CI.

### FR-09 — YAML validation for infra-only bundles
`clusters`, `AdHocRequests`, and `PartyCatalog` have no Python transformation logic. Their tests must validate the correctness of their YAML configuration files (bundle structure, security modes, variable declarations, schema definitions).

### FR-10 — PowerShell run script
A single `run_tests.ps1` script must support parameterised execution: by marker, by bundle, with/without coverage, with/without parallel execution.

### NFR-01 — Deterministic dependency versions
All test dependencies must be pinned to exact versions to prevent CI breakage from upstream releases.

### NFR-02 — Fast feedback for unit tests
Tests marked `unit` (pure Python, no Spark) must complete in under 30 seconds for the full suite.

### NFR-03 — Documentation
A requirements and design document must be maintained alongside the implementation.

---

## 4. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│  pytest invocation (from Databricks/)                           │
│  discovers testpaths from pyproject.toml                        │
└──────────────────┬───────────────────────────────────────────────┘
                   │
        ┌──────────▼──────────┐
        │  tests/conftest.py  │  ← loaded FIRST by pytest
        │  (session scope)    │
        │                     │
        │  1. stub Databricks │
        │     runtime modules │
        │  2. create Spark    │
        │     Session (local) │
        │  3. mock_env        │
        │     (autouse)       │
        └──────────┬──────────┘
                   │ fixtures shared to all test files below
     ┌─────────────┼──────────────────────────────────┐
     │             │                                  │
     ▼             ▼                                  ▼
GCOB_Consumer  radarv1/tests/         clusters/tests/
/tests/        ├── test_functions_   test_clusters_
test_gcob_     │   databricks.py     config.py
utils.py       └── test_radar_
               utils.py
               
GCOB_Reporting  AdHocRequests/        PartyCatalog/
v1/tests/       tests/                tests/
test_dq_        test_adhoc_           test_party_
validation.py   config.py             catalog_config.py
```

**Key design decision:** `tests/conftest.py` is placed in the `tests/` directory (listed under `testpaths`) rather than the `Databricks/` root. This means pytest loads it as part of test collection — the stubs and fixtures are available to all bundle test directories without any explicit import in the test files themselves.

---

## 5. Directory Layout

```
Databricks/
│
├── pyproject.toml                    ← pytest + coverage config (single source of truth)
├── requirements-dev.txt              ← pinned test dependencies
├── run_tests.ps1                     ← PowerShell entry point
│
├── tests/                            ← shared infrastructure (testpath #1)
│   ├── __init__.py
│   ├── conftest.py                   ← stubs, SparkSession, mock_env, mock_dbutils, make_df
│   ├── fixtures/
│   │   ├── __init__.py
│   │   └── sample_dataframes.py      ← 10 DataFrame factories for party/case/GNS/risk data
│   └── utils/
│       ├── __init__.py
│       └── df_helpers.py             ← 10 assertion helpers (schema, null, unique, equality)
│
├── AdHocRequests/
│   ├── databricks.yml
│   ├── resources/
│   ├── src/                          ← 53 date-stamped notebooks (not importable as modules)
│   └── tests/
│       ├── __init__.py
│       └── test_adhoc_config.py      ← YAML structure + naming convention validation
│
├── clusters/
│   ├── databricks.yml
│   ├── resources/
│   │   └── clusters_job.yml
│   └── tests/
│       ├── __init__.py
│       └── test_clusters_config.py   ← YAML: security modes, Spark versions, RBAC
│
├── GCOB_Consumer/
│   ├── databricks.yml
│   ├── src/
│   │   ├── GcobUtils.py              ← utility module (importable)
│   │   ├── GCOB_Consumer/            ← package (main.py, __init__.py)
│   │   └── GCOB_*.py                 ← notebook source files
│   └── tests/
│       ├── __init__.py
│       └── test_gcob_utils.py        ← unit + Spark tests for GcobUtils.py
│
├── GCOB_Reportingv1/
│   ├── databricks.yml
│   ├── src/
│   │   ├── RadarUtils.py             ← utility module (importable)
│   │   ├── GCOB_Reportingv1/         ← package
│   │   └── *.py                      ← notebook source files
│   └── tests/
│       ├── __init__.py
│       ├── test_dq_validation.py     ← Spark DQ tests (ported from notebook)
│       ├── GNS_Test.py               ← existing notebook (NOT a pytest file)
│       ├── GCOB_Producer_DQ_Test.py  ← existing notebook (NOT a pytest file)
│       └── RadarUtils.py             ← shared utility (on pythonpath)
│
├── PartyCatalog/
│   ├── databricks.yml
│   ├── variables.yml
│   ├── resources/
│   │   └── schemas.yml
│   └── tests/
│       ├── __init__.py
│       └── test_party_catalog_config.py  ← YAML: variables, schemas, UC catalog refs
│
└── radarv1/
    ├── databricks.yml
    ├── src/
    │   ├── functions_databricks.py   ← utility module (importable)
    │   ├── radarv1/                  ← package
    │   └── *.py                      ← notebook source files
    └── tests/
        ├── __init__.py
        ├── test_functions_databricks.py  ← unit tests for functions_databricks.py
        └── test_radar_utils.py           ← unit tests for RadarUtils.py
```

---

## 6. Component Design

### 6.1 `pyproject.toml` — Central Configuration

**Location:** `Databricks/pyproject.toml`

The single authoritative configuration file replaces all per-bundle `pytest.ini` files for the centralized run. The key sections are:

#### `[tool.pytest.ini_options]`

```toml
testpaths = [
    "tests",
    "AdHocRequests/tests",
    "clusters/tests",
    "GCOB_Consumer/tests",
    "GCOB_Reportingv1/tests",
    "PartyCatalog/tests",
    "radarv1/tests",
]

pythonpath = [
    "tests",                        # makes fixtures/ and utils/ importable
    "AdHocRequests/src",
    "clusters/src",
    "GCOB_Consumer/src",
    "GCOB_Reportingv1/src",
    "GCOB_Reportingv1/tests",       # RadarUtils.py is in tests/ (legacy location)
    "PartyCatalog/src",
    "radarv1/src",
]
```

**`--import-mode=importlib`** is set in `addopts`. This is critical — without it, pytest uses `sys.path` prepending which causes namespace collisions when two bundle `src/` directories contain files with the same name. `importlib` mode gives each test file its own import namespace.

**`--strict-markers`** prevents tests with unknown markers from silently passing, catching typos in `@pytest.mark.xyz` decorators.

#### `[tool.coverage.run]`

```toml
source = [
    "AdHocRequests/src",
    "clusters/src",
    "GCOB_Consumer/src",
    "GCOB_Reportingv1/src",
    "PartyCatalog/src",
    "radarv1/src",
]
```

`branch = true` enables branch coverage (not just line coverage), which catches untested conditional paths.

`fail_under = 60` — the minimum coverage threshold enforced in CI. This is intentionally conservative for the initial implementation; the target is to increase it as more tests are added.

---

### 6.2 `requirements-dev.txt` — Pinned Dependencies

**Location:** `Databricks/requirements-dev.txt`

All versions are pinned to exact values to guarantee reproducible test results across developer machines and CI agents.

| Package | Version | Purpose |
|---|---|---|
| `pytest` | 8.3.5 | Test runner |
| `pytest-cov` | 6.1.0 | Coverage integration |
| `pytest-mock` | 3.14.0 | `mocker` fixture (wraps `unittest.mock`) |
| `pyspark` | 3.5.0 | Local Spark for Spark tests |
| `delta-spark` | 3.2.1 | Delta Lake in local mode |
| `chispa` | 0.10.2 | PySpark DataFrame equality assertions |
| `databricks-sdk` | 0.32.0 | Required so `from databricks.sdk.runtime import ...` resolves at import |
| `pytest-sugar` | 1.0.0 | Progress bar output |
| `pytest-xdist` | 3.6.1 | Parallel execution (`-n auto`) |
| `pytest-timeout` | 2.3.1 | Per-test timeout support |
| `ruff` | 0.6.9 | Linting (optional, not required for tests) |

**Why `pyspark==3.5.0` and not `3.5.2`?**  
The legacy DAB clusters run Databricks Runtime 14.3 LTS which uses Spark 3.5.0. The Unity Catalog clusters run DBR 17.3 LTS which uses Spark 3.5.2. Pinning to 3.5.0 gives the widest compatibility; API differences between patch versions are negligible for the test patterns used here.

---

### 6.3 `tests/conftest.py` — Shared Fixtures

**Location:** `Databricks/tests/conftest.py`

This is the most critical file in the infrastructure. It has two responsibilities that must both happen before any test module is imported: runtime stubbing and fixture registration.

#### Databricks Runtime Stubbing

```python
def _stub_databricks_runtime() -> None:
    runtime_stub = types.ModuleType("databricks.sdk.runtime")
    runtime_stub.spark   = MagicMock(name="stub_spark")
    runtime_stub.dbutils = MagicMock(name="stub_dbutils")
    
    stubs = {
        "databricks":               MagicMock(),
        "databricks.sdk":           MagicMock(),
        "databricks.sdk.runtime":   runtime_stub,
        "databricks.connect":       MagicMock(),
        "pyspark.dbutils":          MagicMock(),
        ...
    }
    for name, stub in stubs.items():
        if name not in sys.modules:
            sys.modules[name] = stub
```

This runs at **module import time** (not inside a fixture), so the stubs are in place before pytest begins collecting test files. When a source file does `from databricks.sdk.runtime import spark, dbutils`, Python finds the pre-registered `runtime_stub` module in `sys.modules` and returns the MagicMock attributes.

#### Session-Scoped SparkSession

```python
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-databricks-central")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        ...
        .getOrCreate()
    )
    yield session
    session.stop()
```

`scope="session"` means the SparkSession is created once for the entire test run, not once per test or per module. On a typical CI runner this cuts Spark startup from ~50s (10 tests × 5s each) to ~8s (once).

`local[2]` — two threads match the memory profile of a small CI runner and avoid the 200 shuffle partition default.

Delta extensions are registered because `GCOB_Consumer/src/GcobUtils.py` writes Delta tables via `write_to_unity_catalog()`.

#### `mock_env` — `autouse=True`

```python
@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    for key, value in _FAKE_ENV.items():
        monkeypatch.setenv(key, value)
```

`autouse=True` means this runs before **every single test** automatically. Source files that read `os.environ['APP_REG_APP_ID']` at module level get the fake value injected. Individual tests can override specific keys with `monkeypatch.setenv("ENV", "prod")`.

#### `mock_dbutils` Fixture

A pre-wired `MagicMock` for `dbutils` with sensible defaults:
- `dbutils.secrets.get(...)` → `"fake-secret-value"`
- `dbutils.fs.ls(...)` → `[]`
- `dbutils.fs.mkdirs(...)` → `True`

Tests inject it using `monkeypatch.setattr("GcobUtils.dbutils", mock_dbutils)`.

#### `make_df` Factory

```python
@pytest.fixture
def make_df(spark):
    def _factory(rows: list[dict]) -> DataFrame:
        return spark.createDataFrame([Row(**r) for r in rows])
    return _factory
```

Allows test bodies to create DataFrames inline without schema declarations:
```python
df = make_df([{"id": "A", "val": 1}, {"id": "B", "val": None}])
```

---

### 6.4 `tests/fixtures/sample_dataframes.py` — Test Data

**Location:** `Databricks/tests/fixtures/sample_dataframes.py`

Contains 10 factory functions and their corresponding `StructType` schema definitions. Every factory accepts a `SparkSession` and returns a small but realistic DataFrame modelled on the actual GDP data objects.

| Factory | Rows | Purpose |
|---|---|---|
| `party_all_party_details_df` | 3 | Completed LEC, completed NP, prospect (for filter tests) |
| `party_case_client_details_df` | 4 | Two complete LE, one NP, one InProgress (NULL-check tests) |
| `party_business_activities_df` | 2 | Unique SourceClient+NaicsCode (uniqueness pass) |
| `party_business_activities_with_duplicates_df` | 3 | Intentional duplicate (uniqueness fail) |
| `party_workitem_df` | 2 | Unique SourceClient+WorkItemId |
| `party_client_structure_gui_df` | 4 | Ownership, Directorship, Shareholder, Authorised Representative |
| `risk_model_instance_df` | 2 | Two risk model instances |
| `gns_file_df` | 2 | Clean GNS output (pass cases) |
| `gns_file_with_null_listuid_df` | 2 | One NULL ListUid (null check fail) |
| `gns_file_with_duplicate_listuid_df` | 3 | Duplicate (ListUid, SnapshotId) pair (duplicate check fail) |
| `gcob_party_df` | 3 | GCOB Consumer party output (write tests) |

Schemas are defined as module-level `StructType` constants and shared between the factory functions and the `assert_schema_equal` calls in test files.

---

### 6.5 `tests/utils/df_helpers.py` — Assertion Helpers

**Location:** `Databricks/tests/utils/df_helpers.py`

Ten shared assertion functions that produce human-readable error messages. All are pure functions (no fixtures required).

| Function | What it checks |
|---|---|
| `assert_schema_equal(df, schema)` | Column names, data types, optionally nullable flags |
| `assert_column_subset(df, columns)` | DataFrame contains at least these columns |
| `assert_df_equal(actual, expected)` | Full row-level equality; delegates to `chispa` when available |
| `assert_row_count(df, n)` | Exact row count |
| `assert_row_count_gte(df, n)` | Minimum row count |
| `assert_no_nulls(df, columns)` | None of the listed columns have NULL values |
| `assert_all_nulls(df, columns)` | All values in listed columns are NULL |
| `assert_unique(df, columns)` | Column combination is unique across all rows |
| `assert_not_unique(df, columns)` | Duplicate values exist (for negative tests) |
| `assert_column_values_in(df, col, allowed)` | All values belong to an allowed set |

Example failure output from `assert_no_nulls`:

```
AssertionError: NULL values found — 'ValidatedRiskLevel': 1 nulls, 'RiskModelName': 2 nulls
```

---

## 7. Bundle-Specific Test Files

### 7.1 GCOB_Consumer — `test_gcob_utils.py`

**Source under test:** `GCOB_Consumer/src/GcobUtils.py`

`GcobUtils.py` is the most testable file in the codebase — it is a proper Python module (not a notebook), contains reusable functions, and is imported by all five Consumer pipeline notebooks.

**Test classes and coverage:**

| Class | Functions tested | Test count |
|---|---|---|
| `TestGetLoadDate` | `get_load_date()` | 4 — parametrized by source, NLSVF offset, unknown source, case sensitivity |
| `TestDetermineVersion` | `determine_version()` | 6 — RadarDataModel shortcut, Siebel mapping (4 objects), unknown object raises, file-list max |
| `TestGetMatchingPartition` | `get_matching_partition()` | 3 — found, not found raises RuntimeError, GCDS fallback to yesterday |
| `TestWriteToUnityCatalog` | `write_to_unity_catalog()` | 2 — schema not found raises ValueError, successful Delta write |
| `TestGetGroupMembersDF` | `get_group_members_df()` | 2 — correct columns returned, empty groups returns empty DF |
| `TestGetGraphPaginatedData` | `get_graph_paginated_data()` | 3 — single page, pagination follows nextLink, exception returns partial |

**Key mocking approach:**  
`dbutils` and the module-level `spark` reference in `GcobUtils.py` are patched per-test using `monkeypatch.setattr("GcobUtils.dbutils", mock_dbutils)`. This is necessary because the module reads credentials at import time — the stub from `conftest.py` handles that, but individual tests need controlled `fs.ls` return values.

---

### 7.2 GCOB_Reportingv1 — `test_dq_validation.py`

**Source under test:** Logic extracted from `GCOB_Reportingv1/tests/GCOB_Producer_DQ_Test.py`

The DQ notebook cannot be imported directly (it is a Databricks notebook with `# COMMAND ----------` cell markers and top-level Spark calls). The functions are re-implemented inline in the test file with `log_to_catalog` replaced by a `log_fn` parameter — this is the **extract-and-inject pattern**: identical logic, injectable side effects.

**Test classes and coverage:**

| Class | What is tested | Test count |
|---|---|---|
| `TestUniquenessCheck` | `test_dataobject_uniqueness()` | 4 — pass, fail with duplicate count, where-condition filter, log called once |
| `TestNullCheck` | `test_dataobject_null_fields()` | 5 — no nulls pass, null fails, where-condition narrows scope, multi-column independent, message contains count |
| `TestMissingClientsCheck` | `find_missing_clients()` | 4 — all present pass, missing detected with value in message, where-condition filter, log called with correct df name |
| `TestDirectorshipStructureRule` | Inline violation detection block | 2 — clean structure passes, UBO above Directorship fails |

**The `log_fn` injection pattern:**

```python
def test_dataobject_uniqueness(df, ..., *, log_fn=_noop_log):
    ...
    log_fn(dataframe_name, rule_name, columns, status, message)
```

In tests, `log_fn` is replaced with a `MagicMock()`, allowing assertions on what was logged:
```python
mock_log = MagicMock()
result = test_dataobject_uniqueness(df, ..., log_fn=mock_log)
_, _, _, status, message = mock_log.call_args[0]
assert status == "Pass"
```

---

### 7.3 radarv1 — `test_functions_databricks.py`

**Source under test:** `radarv1/src/functions_databricks.py`

This module is a proper Python file (not a notebook) providing core Databricks table operation patterns used by many radarv1 jobs.

**Test classes and coverage:**

| Class | Functions tested | Test count |
|---|---|---|
| `TestAppendToDatabricksTable` | `append_to_databricks_table()` | 3 — write mode, table name format, parametrized schema/table combos |
| `TestFullLoadToDatabricksTable` | `full_load_to_databricks_table()` | 3 — DROP before write, Delta overwrite mode, call ordering |
| `TestUpsertToDatabricksTable` | `upsert_to_databricks_table()` | 5 — duplicate PK raises, unique PK no error, all PK columns in MERGE, MATCHED/NOT MATCHED clauses, temp view named 'updates' |
| `TestCheckTableNotExists` | `check_table_not_exists()` | 3 — True when absent, False when present, non-AnalysisException bubbles up |
| `TestLoadFromGDPParquet` | `load_from_gdp_parquet()` | 5 — invalid producer raises, parametrized valid producers |

**The MERGE SQL test pattern** verifies the generated SQL string rather than executing it against a real metastore:
```python
fdb.upsert_to_databricks_table(df, "s", "t", ["id", "cat"])
sql_str = spark_mock.sql.call_args[0][0]
assert "target.id = source.id" in sql_str
assert "target.cat = source.cat" in sql_str
```

---

### 7.4 radarv1 — `test_radar_utils.py`

**Source under test:** `GCOB_Reportingv1/tests/RadarUtils.py` (shared utility, on pythonpath)

`RadarUtils.py` is used by both `GCOB_Reportingv1` and `radarv1` notebooks. It reads `os.environ` at module level and calls `dbutils.secrets.get()` at import time. The `mock_env` autouse fixture and the conftest stubs handle both.

**Test classes and coverage:**

| Class | Functions tested | Test count |
|---|---|---|
| `TestReadGDPLoadDate` | `Read_GDP_Defined_DataObjects()` — date logic | 5 — 6 known sources (parametrized), GCDS, NLSVF offset, unknown raises, wrong case raises |
| `TestReadGDPContainerMapping` | Container/storage account resolution | 5 — parametrized by source/container |
| `TestFetchLatestFile` | `fetch_latest_file()` — version discovery | 5 — max version from file list, explicit Dataversion bypasses discovery, no matching partition raises, CaseService path prefix, RISKMODEL path prefix |
| `TestAuthenticateStorageAccount` | `authenticate_storage_account()` | 3 — 5 conf.set calls, auth.type key, storage name in all keys |

---

### 7.5 clusters — `test_clusters_config.py`

**Source under test:** `clusters/resources/clusters_job.yml`

The clusters bundle has no Python source — its entire testable surface is the YAML configuration. Tests parse the YAML and make structural assertions.

**Test classes and coverage:**

| Class | What is validated | Test count |
|---|---|---|
| `TestClusterYAMLStructure` | YAML loads, resources.clusters exists, all 6 clusters present, required fields per cluster, required env vars per cluster, targets block, all 3 targets present | 22 (parametrized) |
| `TestClusterSecurityMode` | UC clusters → USER_ISOLATION, legacy clusters → NONE | 6 |
| `TestClusterSparkVersions` | Legacy clusters → 14.3.x, UC clusters → 17.3.x, all clusters → PHOTON | 18 |
| `TestAutoTermination` | `autotermination_minutes` set and in range 1–120 | 6 |
| `TestTargetPermissions` | Dev grants CAN_MANAGE to Admin group, prod restricts to Admin only | 2 |

---

### 7.6 AdHocRequests — `test_adhoc_config.py`

**Source under test:** `AdHocRequests/databricks.yml` + `resources/*.yml` + `src/*.py`

**Test classes and coverage:**

| Class | What is validated | Test count |
|---|---|---|
| `TestAdHocBundleYAML` | Bundle name, include pattern, all 3 targets, workspace host format, run_as service principal, dev is default, preprod/prod use production mode, prod grants CAN_VIEW to users | 9 |
| `TestAdHocResourceFiles` | Resources dir exists, at least one YAML file, each YAML parses without error | 3+ |
| `TestNotebookNamingConvention` | src/ exists, ≥80% of `.py` files follow `YYYY-MM-DD` prefix pattern | 2 |

The 80% threshold on naming convention allows a few legacy files without failing CI while still enforcing the team standard.

---

### 7.7 PartyCatalog — `test_party_catalog_config.py`

**Source under test:** `PartyCatalog/databricks.yml`, `variables.yml`, `resources/schemas.yml`

**Test classes and coverage:**

| Class | What is validated | Test count |
|---|---|---|
| `TestPartyCatalogBundleYAML` | Bundle name, includes variables.yml, all 6 variables declared, catalog_name default matches pattern, ends in `_dev`, environment default is `dev`, dev target present, dev is default, dev host is https:// | 9 |
| `TestPartyCatalogVariablesYML` | YAML parses, all 6 variables present, each has description, type is string when set, workspaceurl contains ADB URL | 14 (parametrized) |
| `TestPartyCatalogSchemasYML` | YAML parses, resources.schemas exists, both schemas present, catalog_name via `${var.catalog_name}`, name + comment fields, gcobreporting has version number, RDMv3 contains RDM | 10 |

---

## 8. Test Markers and Execution Modes

| Marker | Meaning | Typical speed |
|---|---|---|
| `unit` | Pure Python — no Spark, no Azure calls | < 1s per test |
| `spark` | Requires local SparkSession | 0.5–5s per test |
| `integration` | Requires live Databricks/Azure (CI only, not in local run) | N/A locally |
| `dq` | Data quality validation tests | 0.5–3s per test |
| `radarv1` | radarv1 bundle tests | — |
| `gcob_consumer` | GCOB_Consumer bundle tests | — |
| `gcob_reportingv1` | GCOB_Reportingv1 bundle tests | — |
| `clusters` | clusters bundle tests | — |
| `adhoc` | AdHocRequests bundle tests | — |
| `party_catalog` | PartyCatalog bundle tests | — |
| `slow` | Tests taking > 10s | > 10s |

Markers can be combined: `-m "spark and radarv1"` runs only Spark tests for the radarv1 bundle.

---

## 9. `run_tests.ps1` — Entry Point

**Location:** `Databricks/run_tests.ps1`

A PowerShell script that wraps the pytest invocation with virtual environment management, parameter handling, and formatted output.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `-Marker` | string | (none) | Run only tests with this marker |
| `-Bundle` | string | (none) | Run only tests for this bundle folder |
| `-NoCoverage` | switch | off | Skip coverage reporting |
| `-Parallel` | switch | off | Run with `-n auto` (pytest-xdist) |
| `-Install` | switch | off | Force venv create + dep install |
| `-Verbose` | switch | off | Pass `-v -s` to pytest |

### Behaviour

1. Resolves the `Databricks/` root from `$MyInvocation.MyCommand.Path`
2. Creates `.venv/` if absent (or if `-Install`)
3. Installs `requirements-dev.txt` if pytest is not found in venv (or if `-Install`)
4. Builds the pytest argument list based on parameters
5. Invokes pytest and captures exit code
6. Prints duration, result (PASSED / FAILED / NO TESTS COLLECTED), and coverage path
7. Exits with pytest's exit code (0 = pass, non-zero = fail — important for CI)

### Common invocations

```powershell
# Full suite — first run (creates venv, installs deps)
.\run_tests.ps1 -Install

# Full suite with coverage
.\run_tests.ps1

# Fast feedback — unit tests only (< 30s)
.\run_tests.ps1 -Marker unit -NoCoverage

# One bundle
.\run_tests.ps1 -Bundle radarv1

# DQ tests
.\run_tests.ps1 -Marker dq

# Parallel unit tests
.\run_tests.ps1 -Marker unit -Parallel

# Debug a failing test
.\run_tests.ps1 -Bundle GCOB_Consumer -Verbose
```

---

## 10. Coverage Configuration

Coverage is configured in `pyproject.toml` under `[tool.coverage.*]`.

**Branch coverage** is enabled — this measures whether both branches of every `if`/`else` are executed, not just whether the line was reached. This is more meaningful for utility code with conditional paths.

**Threshold:** `fail_under = 60` in `[tool.coverage.report]`. CI will return exit code 2 if coverage drops below 60%. The intent is to raise this to 80% as more tests are added.

**HTML report** is generated at `Databricks/htmlcov/index.html` on every full run. Open in a browser to see per-file line/branch coverage with highlighted uncovered lines.

**Omissions:**
- `*/tests/*` — test files themselves
- `*/__init__.py` — version declaration files only
- `*/main.py` — auto-generated Databricks Connect stubs (not real logic)
- `*/scratch/*` — experimental notebooks
- `*/.venv/*` — virtual environment

---

## 11. The Databricks Runtime Mocking Strategy

This is the most architecturally significant aspect of the implementation, since it is what makes local testing possible at all.

### The problem in detail

Source files in this codebase execute the following at **module level** (outside any function):

```python
# GcobUtils.py, line 1
from databricks.sdk.runtime import dbutils, spark

# GcobUtils.py, line 8
application_id = os.environ['APP_REG_APP_ID']

# GcobUtils.py, line 13
service_credential = dbutils.secrets.get(scope="connectedsecrets", key=f"appreg-{application_id}")
```

This means the import statement `import GcobUtils` fails with `ModuleNotFoundError` unless `databricks.sdk.runtime` exists in the Python environment, and then fails again with `KeyError` if the env vars are not set, and then fails again with a real Databricks API call attempt when `dbutils.secrets.get` is invoked.

Three problems, all at import time, before a single test function runs.

### The solution

```
sys.modules pre-population → env var injection → dbutils call interception
        ↑                           ↑                       ↑
   conftest.py             mock_env (autouse)       stub_dbutils.secrets.get
   (module level)          (fixture level)          (MagicMock attribute)
```

**Step 1 — Pre-populate `sys.modules`** (runs at conftest import time, before test collection):  
A `types.ModuleType` object is created for `databricks.sdk.runtime` with `spark` and `dbutils` as `MagicMock` attributes. It is inserted into `sys.modules` under the key `"databricks.sdk.runtime"`. When Python encounters `from databricks.sdk.runtime import spark, dbutils`, it finds the pre-registered module and returns the MagicMocks.

**Step 2 — Inject env vars** (runs before every test via `autouse` fixture):  
`monkeypatch.setenv` patches `os.environ` for the duration of each test. The module-level `os.environ['APP_REG_APP_ID']` call in `GcobUtils.py` is only reached once (at first import), so the env vars need to be set before the first import of that module. `autouse` on `conftest.py` fixtures ensures this ordering.

**Step 3 — dbutils call returns fake value**:  
Because `dbutils` is a `MagicMock`, `dbutils.secrets.get(...)` returns another `MagicMock` by default. This is fine — the returned value is assigned to `service_credential` which is only used later when `authenticate_storage_account()` is actually called. That function is covered separately in tests that inject a controlled `dbutils` mock.

### Limits of this approach

The stub approach works for import-time side effects. It does **not** make notebooks testable end-to-end — a notebook cell that executes `spark.sql("SELECT * FROM live_table")` against a real Unity Catalog table cannot be tested this way. That is an accepted constraint (see Section 13).

---

## 12. Constraints and Trade-offs

| Constraint | Decision | Rationale |
|---|---|---|
| Notebooks use module-level imports | Stub `sys.modules` before test collection | Only approach that doesn't require notebook refactoring |
| `GCOB_Consumer/src/` and `GCOB_Reportingv1/src/` have similar file names | `--import-mode=importlib` | Gives each test file its own namespace; avoids shadowing |
| `RadarUtils.py` lives in `GCOB_Reportingv1/tests/` (not `src/`) | Add `GCOB_Reportingv1/tests/` to `pythonpath` | Legacy location; moving it would break existing notebook imports |
| DQ notebook functions cannot be imported | Re-implement inline in test file with injectable `log_fn` | Verbatim copy of the function body; identical behaviour |
| `write_to_unity_catalog()` needs a real schema to exist | Patch `spark.catalog.databaseExists` per-test | Avoids creating real schemas in local metastore |
| Local SparkSession vs. cluster Spark | `local[2]`, shuffle partitions = 2 | Sufficient for small test DataFrames; not a substitute for cluster testing |
| Coverage threshold at 60% | Conservative starting point | Increases as more tests are added |

---

## 13. What is NOT Tested (and why)

| Component | Why not in pytest |
|---|---|
| `GNS_Test.py` | Databricks notebook — requires live ADLS data, `%sql` magic cells, Azure Auth. Already wired as a Workflow task in `Non_MI_Reporting_job`. |
| `GCOB_Producer_DQ_Test.py` (as a notebook) | Same as above — runs as a Databricks job task post-deployment. |
| `radarv1/src/cases_clients__daily.py` and other notebooks | Top-level Spark/dbutils calls at module scope; no extractable pure logic. |
| Azure storage reads (`load_from_gdp_parquet` end-to-end) | Requires live ADLS credentials and real partition data. |
| PowerBI refresh tasks | `functions_powerbi.py` calls the PowerBI REST API; would require live OAuth tokens. |
| `PartyCatalog/src/` Python | No Python files exist in this bundle; it is entirely YAML-based. |
| Historical load notebooks | One-time migration scripts; no ongoing test value. |
| AdHocRequests `src/*.py` notebooks | Date-stamped one-off analytical scripts; not reusable modules. |

---

## 14. Extending the Suite

### Adding tests for a new function in an existing bundle

1. Open the bundle's `tests/` directory (e.g. `radarv1/tests/`)
2. Add a test class or test function to the existing file, or create a new `test_<topic>.py` file
3. Import shared helpers at the top: `from utils.df_helpers import assert_no_nulls`
4. Use shared fixtures: `def test_something(spark, make_df, mock_dbutils):`
5. Run: `.\run_tests.ps1 -Bundle radarv1`

No changes to `pyproject.toml` are needed.

### Adding a new bundle to the central suite

1. Create `<NewBundle>/tests/__init__.py`
2. Create `<NewBundle>/tests/test_<topic>.py`
3. Add to `pyproject.toml`:
   ```toml
   # in testpaths
   "NewBundle/tests",
   
   # in pythonpath
   "NewBundle/src",
   ```

### Extracting notebook logic for better testability

The long-term improvement path is to extract reusable logic from notebooks into proper Python modules under `src/<bundle_name>/`. The pattern is:

**Before (in notebook):**
```python
def process_parties(df):
    return df.filter("CaseStatusName = 'Completed'")

result = process_parties(spark.table("party_AllPartyDetails"))
result.write.saveAsTable("output_table")
```

**After (in `src/transforms/party_processing.py`):**
```python
def process_parties(df):
    return df.filter("CaseStatusName = 'Completed'")
```

**In notebook (thin orchestrator):**
```python
from transforms.party_processing import process_parties
result = process_parties(spark.table("party_AllPartyDetails"))
result.write.saveAsTable("output_table")
```

**In test file:**
```python
from transforms.party_processing import process_parties

def test_process_parties_filters_non_completed(make_df):
    df = make_df([
        {"CaseStatusName": "Completed"},
        {"CaseStatusName": "InProgress"},
    ])
    result = process_parties(df)
    assert result.count() == 1
```

---

## 15. Dependency Version Rationale

| Package | Version rationale |
|---|---|
| `pytest==8.3.5` | Latest stable at time of implementation; `--import-mode=importlib` fully supported since 6.0 |
| `pyspark==3.5.0` | Matches DBR 14.3 LTS (legacy clusters); compatible with `delta-spark==3.2.1` |
| `delta-spark==3.2.1` | Compatible with Spark 3.5.x per [Delta compatibility matrix](https://docs.delta.io/latest/releases.html) |
| `chispa==0.10.2` | Stable PySpark assertion library; `assert_df_equality` handles column order automatically |
| `databricks-sdk==0.32.0` | Needed so import resolution works even with stubs in place; prevents `ModuleNotFoundError` at collection |
| `pytest-xdist==3.6.1` | Required for `-n auto` parallel execution; not used by default (incompatible with session-scoped SparkSession) |
| `ruff==0.6.9` | Fast Rust-based linter; replaces flake8+isort; not required for test execution |
