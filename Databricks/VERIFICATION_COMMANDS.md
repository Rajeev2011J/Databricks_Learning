# 🧪 Quick Verification Commands

Copy and paste these commands to verify your setup works in both environments.

---

## Databricks Workspace Verification

### Option 1: Quick Test (in notebook cell)
```python
%sh
cd /Workspace/Users/net.rajeev@gmail.com/Databricks_Learning/Databricks

# Test collection (should see ~30-40 tests)
echo "Testing collection..."
pytest --collect-only -q

# Run unit tests only (fast check)
echo ""
echo "Running unit tests..."
pytest -m unit -v --tb=short
```

### Option 2: Full Notebook Runner (recommended)
1. Open this notebook: `run_tests_notebook`
2. Attach to any cluster
3. **Run All** cells
4. Check **Cell 10** for results

Expected output:
```
✅ Environment: IS_DATABRICKS: True
✅ All [X] tests passed in [Y.Y] s
```

---

## Azure DevOps Pipeline Verification

### Commit and Push
```bash
cd /path/to/your/local/repo
git add .
git commit -m "Fix: pytest dual-environment setup (coverage path, auto-skip, CI filter)"
git push origin development
```

### Monitor Pipeline
1. Go to Azure DevOps → Pipelines
2. Find the triggered build for your commit
3. Watch these stages:
   - **Install Test Dependencies** — should succeed
   - **Run Centralized Test Suite** — should pass with ~35-38 tests
   - **Display Test & Coverage Summary** — should show pass rate
   - **Publish Test Results** — should show in Tests tab
   - **Publish Code Coverage** — should show in Code Coverage tab

### Expected Pipeline Output
```
Testing bundles:
  - radarv1
  - GCOB_Consumer
  - GCOB_Reportingv1
  - clusters
  - AdHocRequests
  - PartyCatalog

========== 35 passed, 3 skipped in 45.2s ==========

Coverage: 78%
```

### Check Coverage Path
After pipeline runs, verify coverage file location:
```bash
# In pipeline Bash task
ls -la Databricks/coverage.xml
# Should exist at: Databricks/coverage.xml (not Databricks/coverage/coverage.xml)
```

---

## Local Development Verification

### Linux/Mac
```bash
cd ~/projects/your-repo/Databricks

# Test collection
pytest --collect-only -q

# Run unit tests
pytest -m unit -v

# Run specific bundle
pytest radarv1/tests/ -v

# Full suite with coverage
pytest --cov --cov-report=html:htmlcov -v
open htmlcov/index.html  # View coverage report
```

### Windows (PowerShell)
```powershell
cd C:\projects\your-repo\Databricks

# Test collection
pytest --collect-only -q

# Run unit tests
pytest -m unit -v

# Run specific bundle
pytest radarv1/tests/ -v

# Full suite with coverage
pytest --cov --cov-report=html:htmlcov -v
Start-Process htmlcov/index.html  # View coverage report
```

---

## Key Differences by Environment

### What Auto-Skips in Each Environment?

**Databricks Cluster:**
- ✅ Auto-skips: `@pytest.mark.local_only` tests (via conftest.py hook)
- ✅ Uses: Real cluster SparkSession
- ✅ Uses: Real dbutils
- Example skipped: `clusters/tests/test_clusters_config.py::test_yaml_structure`

**Azure DevOps CI:**
- ✅ Filters: `-m "not local_only"` (via pipeline YAML)
- ✅ Uses: local[2] SparkSession
- ✅ Stubs: databricks.sdk (MagicMock)
- Same tests skipped as cluster

**Local Dev:**
- ℹ️ Runs ALL tests (including local_only)
- ✅ Uses: local[2] SparkSession
- ✅ Stubs: databricks.sdk (MagicMock)
- YAML tests run because local filesystem available

---

## Troubleshooting Quick Checks

### Check 1: Environment Detection
```python
# In Databricks notebook
%sh
cd /Workspace/Users/net.rajeev@gmail.com/Databricks_Learning/Databricks
python -c "
import sys; sys.path.insert(0, 'tests')
from environment import describe
describe()
"
```

Expected on cluster:
```
Environment: Databricks cluster
IS_DATABRICKS: True
NEEDS_RUNTIME_STUBS: False
HAS_REAL_SPARK: True
```

### Check 2: Coverage Path
```bash
# After running pytest
cat Databricks/pyproject.toml | grep -A2 "tool.coverage.xml"
```

Should show:
```toml
[tool.coverage.xml]
output = "coverage.xml"
```

### Check 3: Pipeline Marker
```bash
cat Databricks/azure-pipelines-enterprise.yml | grep -A5 "pytest"
```

Should include:
```yaml
pytest \
  ...
  -m "not local_only" \
```

### Check 4: Auto-Skip Hook
```bash
tail -20 Databricks/tests/conftest.py
```

Should show:
```python
def pytest_collection_modifyitems(config, items):
    if IS_DATABRICKS:
        skip_local = pytest.mark.skip(...)
        for item in items:
            if "local_only" in item.keywords:
                item.add_marker(skip_local)
```

---

## Success Criteria

### ✅ Databricks Workspace
- [ ] Notebook runs without errors
- [ ] Cell 10 shows "All tests passed"
- [ ] Environment shows `IS_DATABRICKS: True`
- [ ] ~35-38 tests pass (local_only skipped)

### ✅ Azure DevOps
- [ ] Pipeline completes successfully
- [ ] Test results published (check Tests tab)
- [ ] Coverage report published (check Code Coverage tab)
- [ ] No Spark import errors
- [ ] ~35-38 tests pass (local_only filtered)

### ✅ Local
- [ ] `pytest --collect-only` works
- [ ] All tests pass (including local_only)
- [ ] Coverage report generated
- [ ] No import errors

---

## Next Actions

1. **Test on Cluster** (5 minutes)
   - Open `run_tests_notebook`
   - Attach to any cluster
   - Run All

2. **Test in Azure DevOps** (15 minutes)
   - Commit changes
   - Push to development branch
   - Monitor pipeline

3. **If Both Pass** → Deploy to preprod → prod!

4. **If Issues** → Check troubleshooting section above

---

**All systems are configured!** 🚀

The three execution paths (cluster, CI, local) are now fully aligned.
