# ⚡ Quick Fixes Checklist

**Time needed:** 10 minutes  
**Priority:** HIGH — Apply before deploying to CI/CD

---

## Fix 1: Coverage Output Path (30 seconds)

**File:** `Databricks/pyproject.toml`

**Line 144, change:**
```toml
# BEFORE
[tool.coverage.xml]
output = "coverage/coverage.xml"

# AFTER
[tool.coverage.xml]
output = "coverage.xml"
```

**Why:** Azure DevOps PublishCodeCoverageResults expects `coverage.xml` at root.

---

## Fix 2: Azure Pipeline Test Step (5 minutes)

**File:** `Databricks/azure-pipelines-enterprise.yml`

**Find the "Run Centralized Test Suite" step and update:**

```yaml
- task: Bash@3
  displayName: 'Run Centralized Test Suite (All Bundles)'
  inputs:
    targetType: 'inline'
    script: |
      set -e
      cd $(system.defaultworkingdirectory)/Databricks
      
      # Run pytest with marker filter to skip filesystem-dependent tests
      pytest \
        --junitxml=test-results.xml \
        --cov --cov-report=xml:coverage.xml \
        --cov-report=html:htmlcov \
        --cov-report=term \
        -m "not local_only" \
        -v
      
      echo "[DONE] Test execution complete"
```

**Key change:** Added `-m "not local_only"` to skip YAML config tests on CI.

---

## Fix 3: Verify Dependencies Installation (2 minutes)

**File:** Same Azure pipeline

**Find the "Install Test Dependencies" step and verify:**

```yaml
- task: Bash@3
  displayName: 'Install Test Dependencies (Internal Nexus)'
  inputs:
    script: |
      cd Databricks
      
      # IMPORTANT: Use requirements-dev.txt (NOT requirements-cluster.txt)
      # requirements-dev.txt includes pyspark for CI
      # requirements-cluster.txt excludes it (cluster-only)
      pip install -r requirements-dev.txt \
        --index-url $(NEXUSCLOUD_PYPI_URL)
```

**Why:** CI needs pyspark in requirements. Cluster uses requirements-cluster.txt.

---

## Verification Steps

After applying fixes:

### 1. Local Test (2 minutes)
```bash
cd Databricks
pytest -m "not local_only" --collect-only
# Should see ~20-30 tests collected, none marked local_only
```

### 2. Coverage Path Test (1 minute)
```bash
cd Databricks
pytest --cov --cov-report=xml:coverage.xml -m unit
ls -la coverage.xml  # Should exist at Databricks/coverage.xml (not coverage/coverage.xml)
```

### 3. Cluster Test (5 minutes)
- Open `run_tests_notebook` in Databricks
- Attach to any cluster
- Run Cell 3 (environment verification)
- Should see: `IS_DATABRICKS: True`

### 4. CI Test (automatic)
- Push to development branch
- Monitor Azure DevOps pipeline
- Check Test Results tab for published tests
- Check Code Coverage tab for coverage report

---

## Optional: Auto-Skip Hook (2 minutes)

**File:** `Databricks/tests/conftest.py`

**Add at the end of the file:**

```python
# ---------------------------------------------------------------------------
# 7. Auto-skip local_only tests on Databricks cluster
# ---------------------------------------------------------------------------

def pytest_collection_modifyitems(config, items):
    """
    Auto-skip tests marked local_only when running on Databricks cluster.
    This eliminates the need for manual -m "not local_only" filter.
    """
    if IS_DATABRICKS:
        skip_local = pytest.mark.skip(
            reason="local_only marker — requires local filesystem"
        )
        for item in items:
            if "local_only" in item.keywords:
                item.add_marker(skip_local)
```

**Benefit:** Notebook runner doesn't need `-m "not local_only"` — tests auto-skip.

---

## Summary

| Fix | File | Time | Priority |
|-----|------|------|----------|
| Coverage path | pyproject.toml | 30s | HIGH |
| Pipeline test step | azure-pipelines-enterprise.yml | 5m | HIGH |
| Verify deps install | azure-pipelines-enterprise.yml | 2m | MEDIUM |
| Auto-skip hook | tests/conftest.py | 2m | OPTIONAL |
| **Total** | | **10 min** | |

---

## Ready?

✅ Apply fixes  
✅ Run local verification  
✅ Test on cluster  
✅ Push to development  
✅ Monitor CI/CD pipeline  

**Your test framework is 99% ready — just these final touches!** 🚀
