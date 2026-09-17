# ⚡ Quick Reference: Centralized Pytest for All Bundles

## 🎯 One-Minute Overview

**What:** Centralized pytest framework testing all 6 DAB bundles  
**Where:** Works in both Databricks workspace AND Azure DevOps  
**How:** Zero code changes needed between environments  

---

## 📁 Essential Files

| File | Location | Purpose |
|------|----------|---------|
| `tests/conftest.py` | `Databricks/tests/` | Environment detection + fixtures |
| `pyproject.toml` | `Databricks/` | Pytest configuration |
| `requirements-dev.txt` | `Databricks/` | Test dependencies |
| `run_tests.sh` | `Databricks/` | Test runner script |
| `azure-pipelines-enterprise.yml` | `Databricks/` | Azure DevOps pipeline |

---

## 🚀 Common Commands

### In Databricks Workspace

```python
# Quick unit tests (no Spark, ~30 seconds)
%sh
cd /Workspace/Users/YOUR_EMAIL/Databricks_Learning/Databricks
./run_tests.sh --marker unit --no-coverage

# Full test suite with coverage (~5 minutes)
%sh
cd /Workspace/Users/YOUR_EMAIL/Databricks_Learning/Databricks
./run_tests.sh

# Test specific bundle
%sh
cd /Workspace/Users/YOUR_EMAIL/Databricks_Learning/Databricks
./run_tests.sh --bundle radarv1

# Test with verbose output
%sh
cd /Workspace/Users/YOUR_EMAIL/Databricks_Learning/Databricks
./run_tests.sh --verbose
```

### In Azure DevOps

**Automatically runs on:**
- Push to `development`, `preprod`, `main`
- Pull requests to `main` or `develop`

**Manual trigger:**
- Azure DevOps → Pipelines → Run Pipeline

---

## 🧪 Test Markers

Use markers to organize and filter tests:

```python
@pytest.mark.unit           # Pure Python, no Spark (fast)
@pytest.mark.spark          # Requires SparkSession
@pytest.mark.dq             # Data quality tests
@pytest.mark.databricks     # Only runs in Databricks workspace
@pytest.mark.azure_devops   # Only runs in Azure DevOps pipeline
@pytest.mark.slow           # Tests > 10 seconds

# Bundle-specific markers
@pytest.mark.radarv1
@pytest.mark.gcob_consumer
@pytest.mark.gcob_reportingv1
@pytest.mark.clusters
@pytest.mark.adhoc
@pytest.mark.party_catalog
```

**Run specific markers:**
```bash
./run_tests.sh --marker unit           # Only unit tests
./run_tests.sh --marker "spark and not slow"  # Spark tests, skip slow ones
./run_tests.sh --marker radarv1        # Only radarv1 bundle tests
```

---

## 🔧 Available Fixtures

Import automatically from centralized `tests/conftest.py`:

```python
def test_example(spark, make_df, mock_dbutils, mock_env):
    """All fixtures available automatically."""
    
    # spark: SparkSession (auto-adapts to environment)
    df = spark.createDataFrame([...])
    
    # make_df: Easy DataFrame factory
    df = make_df([
        {"id": 1, "value": "test"}
    ])
    
    # mock_dbutils: Mocked dbutils for non-Databricks
    secret = mock_dbutils.secrets.get("scope", "key")
    
    # mock_env: Fake environment variables
    app_id = os.getenv('APP_REG_APP_ID')  # Auto-injected
```

---

## 📊 Coverage Thresholds

```yaml
Current:  60%  # Starting point
Target:   80%  # Goal for all bundles
```

**Adjust in pipeline:**
```yaml
# In azure-pipelines-enterprise.yml
variables:
  - name: COVERAGE_THRESHOLD
    value: 60  # Change this value
```

---

## 🌍 Environment Detection

Happens automatically in `tests/conftest.py`:

| Environment | Indicator | Spark | dbutils | Env Vars |
|-------------|-----------|-------|---------|----------|
| **Databricks Workspace** | `DATABRICKS_RUNTIME_VERSION` | Cluster Spark | Real | Real |
| **Azure DevOps** | `BUILD_BUILDID` / `TF_BUILD` | `local[*]` | Mocked | Fake |
| **Local Dev** | Neither | `local[*]` | Mocked | Fake |

**No manual setup needed** - conftest.py handles it!

---

## 📂 Test Organization

```
Databricks/
├── tests/                      # Centralized fixtures
│   └── conftest.py             # ← Auto-loaded by pytest
│
└── [bundle]/tests/
    └── test_*.py               # ← Your tests here
```

**Naming conventions:**
- `test_*.py` - Test files (auto-discovered)
- `test_*()` - Test functions
- `Test*` - Test classes

---

## 🎯 Writing Tests: Quick Examples

### Unit Test (Pure Python)
```python
@pytest.mark.unit
def test_simple():
    assert 2 + 2 == 4
```

### Spark Test
```python
@pytest.mark.spark
def test_dataframe(spark, make_df):
    df = make_df([{"id": 1}])
    assert df.count() == 1
```

### Databricks-Only Test
```python
@pytest.mark.databricks
def test_unity_catalog(spark):
    df = spark.sql("SELECT * FROM main.default.table LIMIT 1")
    assert df.count() <= 1
```

---

## 🚨 Troubleshooting Quick Fixes

### "conftest.py not found"
```bash
# Verify file exists
ls Databricks/tests/conftest.py

# If missing, check it's committed
git status Databricks/tests/
```

### "Java not found" (PySpark tests fail)
```bash
# In Azure DevOps: JavaToolInstaller task must run
# In Databricks: Already installed on cluster
```

### "Coverage below threshold"
```bash
# Option 1: Add more tests
# Option 2: Lower threshold temporarily
# In pipeline: COVERAGE_THRESHOLD: 45
```

### "Tests pass in Databricks, fail in Azure DevOps"
```bash
# Check environment-specific code
# Use markers to skip environment-specific tests:
@pytest.mark.databricks  # Skipped in Azure DevOps
```

---

## 📈 Test Results

### In Databricks Workspace
- Terminal output with pass/fail
- HTML coverage report: `htmlcov/index.html`

### In Azure DevOps
- **Test Results tab**: Test pass/fail, trends, drill-down
- **Code Coverage tab**: Coverage %, trend charts
- **Artifacts**: Download HTML reports

---

## 🔗 Documentation Links

| Doc | Purpose |
|-----|---------|
| `ENTERPRISE_SETUP_GUIDE.md` | Full integration guide |
| `DUAL_ENVIRONMENT_SETUP.md` | Environment-specific details |
| `MIGRATION_FROM_SINGLE_BUNDLE.md` | Old vs new comparison |
| `QUICK_REFERENCE.md` | This document |

---

## 💡 Pro Tips

1. **Fast feedback loop:**
   ```bash
   # Run unit tests first (30s), then Spark tests (5min)
   ./run_tests.sh --marker unit --no-coverage
   ```

2. **Parallel execution:**
   ```bash
   # Use pytest-xdist for speed (if installed)
   ./run_tests.sh --parallel
   ```

3. **Debug single test:**
   ```bash
   pytest radarv1/tests/test_main.py::test_specific_function -v -s
   ```

4. **Coverage for specific bundle:**
   ```bash
   pytest --cov=radarv1/src radarv1/tests/
   ```

5. **Skip slow tests:**
   ```bash
   ./run_tests.sh --marker "not slow"
   ```

---

## ✅ Pre-Commit Checklist

Before pushing code:

- [ ] Run unit tests: `./run_tests.sh --marker unit`
- [ ] Check coverage: `./run_tests.sh`
- [ ] Verify tests pass: All green ✅
- [ ] Coverage meets threshold: ≥ 60%
- [ ] No new lint issues: `ruff check .` (if using ruff)

---

## 🎓 Learning Path

1. **Start here:** Run existing tests
   ```bash
   ./run_tests.sh --marker unit
   ```

2. **Understand fixtures:** Read `tests/conftest.py`

3. **Write simple test:** Add to your bundle's `tests/`
   ```python
   @pytest.mark.unit
   def test_my_function():
       assert True
   ```

4. **Add Spark test:** Use fixtures
   ```python
   @pytest.mark.spark
   def test_with_spark(spark, make_df):
       df = make_df([{"x": 1}])
       assert df.count() == 1
   ```

5. **Run in CI:** Push and watch Azure DevOps pipeline

---

## 📞 Need Help?

1. Check troubleshooting section above
2. Review detailed guides in `Databricks/` directory
3. Check pytest docs: https://docs.pytest.org/
4. Check PySpark testing: https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html

---

**Remember: Same test code works in both Databricks workspace AND Azure DevOps!** 🚀
