# 🏢 Enterprise Setup Guide: Centralized Pytest for All Bundles

## Overview

This guide shows how to integrate the centralized pytest framework with your existing enterprise Azure DevOps pipeline infrastructure.

**Key Changes from Single-Bundle Testing:**
- ✅ **One centralized `tests/conftest.py`** instead of per-bundle conftest files
- ✅ **Tests all 6 bundles** in a single pipeline run
- ✅ **Shared fixtures and utilities** across bundles
- ✅ **Single pyproject.toml** for unified pytest configuration
- ✅ **Same enterprise features**: Nexus, security scans, coverage gates

---

## 📁 File Structure (Enterprise)

```
Databricks/
├── azure-pipelines-enterprise.yml    # ← New unified pipeline
├── pyproject.toml                    # ← Centralized pytest config
├── requirements-dev.txt              # ← Test dependencies (Nexus-compatible)
├── run_tests.sh                      # ← Cross-platform test runner
│
├── tests/                            # ← CENTRALIZED test infrastructure
│   ├── conftest.py                   # ← Environment detection + fixtures
│   │   # Auto-detects: Databricks workspace vs Azure DevOps
│   │   # Provides: spark, make_df, mock_dbutils, mock_env
│   ├── fixtures/
│   │   └── databricks_fixtures.py    # Shared fixtures
│   └── utils/
│       └── test_helpers.py           # Test utilities
│
└── [6 bundles]/                      # Each bundle has its own tests
    ├── src/                          # Source code
    └── tests/
        └── test_*.py                 # Bundle-specific tests
                                      # Use centralized fixtures from tests/conftest.py
```

---

## 🔧 Integration Steps

### Step 1: Review Your Current Pipeline

Your existing `DAB-GCOB-reporting.yml` has:
- ✅ Security scans (SonarQube, Checkmarx, secret scan)
- ✅ Java 17 + Python 3.11 setup
- ✅ Internal Nexus repository
- ✅ Coverage threshold enforcement (80%)
- ✅ Multi-stage deployment (Dev → Preprod → Prod)
- ❌ **Only tests radarv1 bundle**

### Step 2: Adopt the New Pipeline

Replace your existing pipeline with `azure-pipelines-enterprise.yml`:

**Key Differences:**

| Aspect | Old (DAB-GCOB-reporting.yml) | New (azure-pipelines-enterprise.yml) |
|--------|----------------------------|-------------------------------------|
| **Bundles Tested** | Only radarv1 | All 6 bundles |
| **Test Structure** | `Databricks/radarv1/tests/conftest.py` | `Databricks/tests/conftest.py` (centralized) |
| **Configuration** | Per-bundle `pytest.ini` | Single `pyproject.toml` |
| **Fixtures** | Bundle-specific | Shared across all bundles |
| **Coverage** | `--cov=src/radarv1` | `--cov=*/src` (all bundles) |

### Step 3: Update Variable Groups

Your pipeline uses these variable groups:
- `L-FEC-RADAR` (shared)
- `L-FEC-RADAR-DEV`
- `L-FEC-RADAR-PREPRD`
- `L-FEC-RADAR-PROD`
- `NexusCloud_eu.NpaRadarNexusUser@rabobank.com`

**No changes needed** - the new pipeline uses the same variable groups.

### Step 4: Install Centralized Test Dependencies

The new `requirements-dev.txt` is compatible with your Nexus repository:

```bash
# In Azure DevOps pipeline (automatic):
pip install -r Databricks/requirements-dev.txt \
  --index-url $(NEXUSCLOUD_PYPI_URL)

# In Databricks workspace (manual):
%sh
cd /Workspace/Users/YOUR_EMAIL/Databricks_Learning/Databricks
pip install -r requirements-dev.txt
```

**Packages Required:**
- `pytest==8.3.5`
- `pytest-cov==6.0.0`
- `pytest-html==3.2.0`
- `pytest-metadata==3.0.0`
- `pytest-xdist==3.5.0` (parallel execution)
- `pyspark==3.5.5`
- `coverage[toml]==7.6.0`

All versions are pinned for reproducibility and Nexus compatibility.

---

## 🧪 How It Works

### Environment Detection (Automatic)

The centralized `tests/conftest.py` automatically detects the environment:

```python
def is_databricks_workspace() -> bool:
    return (
        'DATABRICKS_RUNTIME_VERSION' in os.environ or
        '/databricks/' in sys.prefix
    )

def is_azure_devops_pipeline() -> bool:
    return (
        'BUILD_BUILDID' in os.environ or
        'TF_BUILD' in os.environ
    )
```

**In Azure DevOps:**
- Creates `local[*]` SparkSession
- Mocks `dbutils`
- Injects fake environment variables
- Generates JUnit XML + Cobertura coverage

**In Databricks Workspace:**
- Uses existing cluster Spark
- Uses real `dbutils`
- Uses real environment variables
- Generates HTML coverage report

### Writing Tests (Same Code, Both Environments)

```python
# radarv1/tests/test_example.py
import pytest

@pytest.mark.unit
def test_simple():
    """Pure Python test - works everywhere."""
    assert 2 + 2 == 4

@pytest.mark.spark
def test_dataframe(spark, make_df):
    """Spark test - fixtures adapt automatically."""
    df = make_df([{"id": 1, "value": "test"}])
    assert df.count() == 1

@pytest.mark.databricks
def test_workspace_only(spark):
    """Only runs in Databricks workspace."""
    # Access real Unity Catalog
    df = spark.sql("SELECT * FROM main.default.example LIMIT 1")
    assert df.count() <= 1
```

**No code changes needed** - same test file works in both environments!

---

## 📊 Pipeline Stages

### Stage 1: Build & Test (All Bundles)

```yaml
- stage: Build
  jobs:
    - Security scans (SonarQube, Checkmarx, secrets)
    - Install Java 17 + Python 3.11
    - Verify centralized test structure
    - Install dependencies from Nexus
    - Run pytest on ALL 6 bundles
    - Enforce coverage threshold (60%)
    - Publish results to Azure DevOps
```

**Coverage Sources:**
```bash
--cov=radarv1/src \
--cov=GCOB_Consumer/src \
--cov=GCOB_Reportingv1/src \
--cov=clusters/src \
--cov=AdHocRequests/src \
--cov=PartyCatalog/src
```

### Stage 2-4: Deployment (Unchanged)

- **Dev**: Auto-deploy on `development` branch
- **Preprod**: Auto-deploy on `preprod` branch
- **Prod**: Manual approval on `main` branch with change management

---

## 🎯 Coverage Configuration

### pyproject.toml Coverage Settings

```toml
[tool.coverage.run]
source = [
    "radarv1/src",
    "GCOB_Consumer/src",
    "GCOB_Reportingv1/src",
    "clusters/src",
    "AdHocRequests/src",
    "PartyCatalog/src",
]
branch = true  # Branch coverage (more thorough)
omit = ["*/tests/*", "*/__init__.py"]

[tool.coverage.report]
fail_under = 60  # Start at 60%, increase over time
precision = 2
show_missing = true
```

### Coverage Threshold Evolution

```bash
# Current: 60% (reasonable starting point for centralized tests)
COVERAGE_THRESHOLD: 60

# After stabilization:
# - Increase to 70% (Q2 2024)
# - Increase to 80% (Q3 2024) - matching your radarv1 target
```

---

## 🚀 Running Tests

### In Azure DevOps Pipeline

**Automatically runs on:**
- Push to `development`, `preprod`, `main`
- Pull requests
- Manual pipeline trigger

**Test execution:**
```bash
cd Databricks
pytest \
  --junitxml=test-results.xml \
  --html=test-report.html \
  --cov=*/src \
  --cov-report=xml:coverage.xml \
  --cov-report=html:htmlcov \
  -v
```

### In Databricks Workspace

**Quick test (unit tests only):**
```python
%sh
cd /Workspace/Users/YOUR_EMAIL/Databricks_Learning/Databricks
./run_tests.sh --marker unit --no-coverage
```

**Full test suite:**
```python
%sh
cd /Workspace/Users/YOUR_EMAIL/Databricks_Learning/Databricks
./run_tests.sh
```

**Test specific bundle:**
```python
%sh
cd /Workspace/Users/YOUR_EMAIL/Databricks_Learning/Databricks
./run_tests.sh --bundle radarv1
```

---

## 🔍 Verification Checklist

Before deploying the new pipeline:

- [ ] **Centralized structure exists**
  ```bash
  ls Databricks/tests/conftest.py           # ✓ Exists
  ls Databricks/pyproject.toml              # ✓ Exists
  ls Databricks/requirements-dev.txt        # ✓ Exists
  ```

- [ ] **Bundle test directories exist**
  ```bash
  ls Databricks/radarv1/tests/              # ✓ Has test_*.py files
  ls Databricks/GCOB_Consumer/tests/        # ✓ Has test_*.py files
  ls Databricks/GCOB_Reportingv1/tests/     # ✓ Has test_*.py files
  # ... etc for all 6 bundles
  ```

- [ ] **Dependencies available in Nexus**
  ```bash
  # Verify these packages exist in your Nexus:
  # pytest==8.3.5
  # pyspark==3.5.5
  # pytest-cov==6.0.0
  # pytest-html==3.2.0
  # pytest-xdist==3.5.0
  ```

- [ ] **Variable groups configured**
  - `L-FEC-RADAR` with `NEXUSCLOUD_USERNAME`, `NEXUSCLOUD_PASSWORD`
  - Environment-specific groups (DEV, PREPRD, PROD)

- [ ] **Agent pool available**
  - `Shared-EU-Container-Linux-Python-S-Prod` (Build stage)
  - `Shared-EU-Container-Linux-Python-S-Test` (Dev stage)

---

## 🐛 Troubleshooting

### Issue: "conftest.py not found"

**Symptom:**
```
ERROR: Databricks/tests/conftest.py NOT FOUND
```

**Fix:**
Ensure the centralized conftest.py is committed to your repository:
```bash
git add Databricks/tests/conftest.py
git commit -m "Add centralized test fixtures"
git push
```

### Issue: "Package not found in Nexus"

**Symptom:**
```
ERROR: Could not find a version that satisfies the requirement pytest-xdist==3.5.0
```

**Fix:**
Either:
1. Upload the package to your Nexus repository, OR
2. Remove it from `requirements-dev.txt` (parallel execution optional)

### Issue: "Coverage below threshold"

**Symptom:**
```
[FAILED] Coverage 45.2% is below threshold 60%
```

**Fix:**
Either:
1. Add more tests to increase coverage, OR
2. Temporarily lower the threshold in the pipeline:
   ```yaml
   variables:
     - name: COVERAGE_THRESHOLD
       value: 45  # Lower threshold temporarily
   ```

### Issue: "Java not found - PySpark tests fail"

**Symptom:**
```
'NoneType' object has no attribute 'sc'
```

**Fix:**
Verify the JavaToolInstaller task runs successfully in your pipeline.
Check agent has Java 17 available:
```bash
java -version  # Should show Java 17
```

---

## 📈 Migration Path

### Phase 1: POC (Current)
- ✅ One bundle (radarv1) working in enterprise pipeline
- ✅ Centralized structure designed
- ✅ Environment detection implemented

### Phase 2: Integration (Next)
- [ ] Deploy centralized test structure to repository
- [ ] Update pipeline to use `azure-pipelines-enterprise.yml`
- [ ] Verify all 6 bundles have test directories
- [ ] Run full test suite in Dev environment

### Phase 3: Stabilization
- [ ] Ensure all tests pass in Azure DevOps
- [ ] Tune coverage threshold (start 60%, grow to 80%)
- [ ] Add bundle-specific tests as needed
- [ ] Document bundle testing patterns

### Phase 4: Full Rollout
- [ ] Deploy to Preprod and Prod
- [ ] Train team on centralized test patterns
- [ ] Establish test coverage policies
- [ ] Regular test maintenance

---

## 📚 Key Files Reference

| File | Purpose | Location |
|------|---------|----------|
| `azure-pipelines-enterprise.yml` | Main CI/CD pipeline | `Databricks/` |
| `pyproject.toml` | Pytest configuration | `Databricks/` |
| `requirements-dev.txt` | Test dependencies | `Databricks/` |
| `tests/conftest.py` | Environment detection + fixtures | `Databricks/tests/` |
| `run_tests.sh` | Cross-platform test runner | `Databricks/` |
| `DUAL_ENVIRONMENT_SETUP.md` | Detailed setup guide | `Databricks/` |

---

## 🎓 Next Steps

1. **Review the new pipeline**
   ```bash
   # Compare with your existing pipeline
   diff DAB-GCOB-reporting.yml azure-pipelines-enterprise.yml
   ```

2. **Test locally in Databricks workspace**
   ```python
   %sh
   cd /Workspace/Users/YOUR_EMAIL/Databricks_Learning/Databricks
   ./run_tests.sh --marker unit
   ```

3. **Deploy to Azure DevOps**
   ```bash
   git add Databricks/azure-pipelines-enterprise.yml
   git add Databricks/pyproject.toml
   git add Databricks/tests/
   git commit -m "Add centralized pytest framework"
   git push
   ```

4. **Update pipeline in Azure DevOps**
   - Azure DevOps → Pipelines → Edit
   - Point to `Databricks/azure-pipelines-enterprise.yml`
   - Save and run

5. **Monitor first run**
   - Check test results tab
   - Verify code coverage meets threshold
   - Review test execution report artifact

---

## 🤝 Support

For questions or issues:
1. Check `DUAL_ENVIRONMENT_SETUP.md` for detailed environment-specific guidance
2. Review `tests/conftest.py` for available fixtures
3. See `pyproject.toml` for pytest configuration options
4. Check pipeline logs for detailed diagnostics

**The centralized test framework is designed to be zero-code-change between environments - same tests work in both Databricks workspace and Azure DevOps!** 🎉
