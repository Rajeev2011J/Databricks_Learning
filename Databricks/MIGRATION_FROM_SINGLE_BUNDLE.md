# 📋 Migration Summary: Single-Bundle → Multi-Bundle Centralized Testing

## What Changed

This document shows exactly what was adapted from your existing `DAB-GCOB-reporting.yml` to create the new centralized testing framework.

---

## Side-by-Side Comparison

### Pipeline Structure

| Aspect | DAB-GCOB-reporting.yml (Old) | azure-pipelines-enterprise.yml (New) |
|--------|----------------------------|-------------------------------------|
| **Bundles Covered** | radarv1 only | All 6 bundles |
| **Test Location** | `Databricks/radarv1/tests/` | `Databricks/tests/` (centralized) + bundle tests |
| **Configuration** | `radarv1/pytest.ini` | `Databricks/pyproject.toml` |
| **Fixtures** | `radarv1/tests/conftest.py` | `Databricks/tests/conftest.py` (shared) |
| **Test Command** | `cd radarv1 && pytest tests/` | `cd Databricks && pytest` (all bundles) |

### Features Preserved ✅

All your enterprise features are **kept intact**:

1. **Security Compliance**
   - ✅ SonarQube scan
   - ✅ Checkmarx scan
   - ✅ Secret scan
   - ✅ Template references

2. **Environment Setup**
   - ✅ Java 17 installation for PySpark
   - ✅ Python 3.11.x strict version
   - ✅ Version validation scripts
   - ✅ Diagnostic verification steps

3. **Internal Nexus Repository**
   - ✅ `NEXUSCLOUD_PYPI_URL` variable
   - ✅ `--index-url` installation
   - ✅ Package caching
   - ✅ No public PyPI fallback

4. **Coverage & Quality Gates**
   - ✅ Coverage threshold enforcement
   - ✅ Branch coverage enabled
   - ✅ Test result publishing
   - ✅ Code coverage publishing
   - ✅ HTML + XML + JSON reports

5. **Multi-Stage Deployment**
   - ✅ Dev → Preprod → Prod stages
   - ✅ Token replacement
   - ✅ Environment-specific variable groups
   - ✅ Change management (Prod)

6. **Agent Pools**
   - ✅ `Shared-EU-Container-Linux-Python-S-Prod` (Build)
   - ✅ `Shared-EU-Container-Linux-Python-S-Test` (Dev)

---

## Test Execution Changes

### OLD: Single Bundle (radarv1)

```bash
# In DAB-GCOB-reporting.yml
cd $(system.defaultworkingdirectory)/Databricks/radarv1

export APP_REG_APP_ID="test-app-id"
export TENANT_ID="test-tenant-id"
export ENV="dev"
export CI=true

pytest tests/ \
  --junitxml=test-results.xml \
  --cov=src/radarv1 \
  --cov-report=xml:coverage.xml \
  --cov-report=html:htmlcov \
  -v
```

**Characteristics:**
- Tests ONLY radarv1
- Uses bundle-specific conftest.py
- Manual environment variable setup
- Bundle-specific pytest.ini

### NEW: Multi-Bundle (All 6)

```bash
# In azure-pipelines-enterprise.yml
cd $(system.defaultworkingdirectory)/Databricks

# Environment variables auto-detected by centralized conftest.py
# No manual export needed - conftest.py handles it

pytest \
  --junitxml=test-results.xml \
  --cov=radarv1/src \
  --cov=GCOB_Consumer/src \
  --cov=GCOB_Reportingv1/src \
  --cov=clusters/src \
  --cov=AdHocRequests/src \
  --cov=PartyCatalog/src \
  --cov-report=xml:coverage.xml \
  --cov-report=html:htmlcov \
  -v
```

**Characteristics:**
- Tests ALL 6 bundles
- Uses centralized conftest.py
- Auto-detects environment (Azure DevOps vs Databricks)
- Single pyproject.toml configuration

---

## Coverage Configuration

### OLD: Per-Bundle Coverage

**In radarv1 only:**
```yaml
--cov=src/radarv1    # Only this bundle
```

**Coverage threshold:**
```yaml
COVERAGE_THRESHOLD: 80  # High threshold for mature bundle
```

### NEW: Multi-Bundle Coverage

**All bundles:**
```yaml
--cov=radarv1/src
--cov=GCOB_Consumer/src
--cov=GCOB_Reportingv1/src
--cov=clusters/src
--cov=AdHocRequests/src
--cov=PartyCatalog/src
```

**Coverage threshold (adjusted):**
```yaml
COVERAGE_THRESHOLD: 60  # Lower start, will grow to 80%
```

**Rationale:**
- radarv1 already has good coverage (80%)
- Other bundles may have less coverage initially
- Start at 60%, increase gradually as tests are added

---

## File Structure Changes

### OLD Structure

```
Databricks/
└── radarv1/
    ├── src/
    │   └── radarv1/
    │       ├── main.py
    │       └── RadarUtils.py
    ├── tests/
    │   ├── conftest.py         # radarv1-specific fixtures
    │   ├── test_main.py
    │   └── test_radar_utils.py
    └── pytest.ini              # radarv1-specific config
```

### NEW Structure

```
Databricks/
├── tests/                      # ← NEW: Centralized infrastructure
│   ├── conftest.py             # Shared fixtures for all bundles
│   ├── fixtures/
│   └── utils/
│
├── pyproject.toml              # ← NEW: Central pytest config
├── requirements-dev.txt        # ← NEW: Unified dependencies
├── run_tests.sh                # ← NEW: Cross-platform runner
│
├── radarv1/
│   ├── src/
│   └── tests/
│       ├── test_main.py        # Uses centralized conftest.py
│       └── test_radar_utils.py
│
├── GCOB_Consumer/
│   ├── src/
│   └── tests/                  # Tests use centralized conftest.py
│
├── GCOB_Reportingv1/
│   ├── src/
│   └── tests/
│
└── [other bundles]/
    ├── src/
    └── tests/
```

**Key Changes:**
1. **Centralized `tests/conftest.py`** - shared by all bundles
2. **Single `pyproject.toml`** - replaces per-bundle `pytest.ini` files
3. **Bundle tests** still live in their own directories
4. **Fixtures** are shared across all bundles

---

## Environment Detection

### OLD: Manual CI Flag

```bash
# Manually set CI flag
export CI=true

# conftest.py checks this flag
if os.getenv('CI') == 'true':
    # Use local[2] Spark
else:
    # Use existing Spark
```

### NEW: Automatic Detection

```python
# In centralized tests/conftest.py
def is_azure_devops_pipeline() -> bool:
    """Auto-detect Azure DevOps."""
    return (
        'BUILD_BUILDID' in os.environ or
        'TF_BUILD' in os.environ
    )

def is_databricks_workspace() -> bool:
    """Auto-detect Databricks workspace."""
    return (
        'DATABRICKS_RUNTIME_VERSION' in os.environ or
        '/databricks/' in sys.prefix
    )

# No manual export needed - automatic!
```

**Benefits:**
- No manual environment variable exports
- Works in both environments automatically
- Single source of truth for environment logic

---

## Diagnostic Steps

### OLD: Bundle-Specific Verification

```bash
# Verify radarv1 conftest.py
EXPECTED_PATH="Databricks/radarv1/tests/conftest.py"

if [ -f "$EXPECTED_PATH" ]; then
  echo "✅ conftest.py FOUND"
fi
```

### NEW: Centralized Verification

```bash
# Verify centralized structure
if [ -f "Databricks/tests/conftest.py" ]; then
  echo "✅ Centralized conftest.py found"
fi

if [ -f "Databricks/pyproject.toml" ]; then
  echo "✅ pyproject.toml found"
fi

# Check all bundle test directories
for bundle in radarv1 GCOB_Consumer GCOB_Reportingv1 clusters AdHocRequests PartyCatalog; do
  if [ -d "Databricks/$bundle/tests" ]; then
    echo "✅ $bundle/tests/ found"
  fi
done
```

---

## Dependencies

### OLD: Inline pip install

```bash
pip install --cache-dir=$(Pipeline.Workspace)/.pip \
  pytest==8.3.5 \
  pyspark==3.5.5 \
  pytest-cov==6.0.0 \
  pytest-html==3.2.0 \
  pytest-metadata==3.0.0 \
  --index-url $(NEXUSCLOUD_PYPI_URL)
```

### NEW: requirements-dev.txt

```bash
# Install from file (easier to maintain)
pip install --cache-dir=$(Pipeline.Workspace)/.pip \
  -r Databricks/requirements-dev.txt \
  --index-url $(NEXUSCLOUD_PYPI_URL)
```

**Benefits:**
- Easier to update versions (edit one file)
- Can pin additional dependencies
- Same file works locally and in CI

---

## What Stays the Same

### Deployment Stages (Unchanged)

All deployment stages work **identically** to before:

```yaml
# Dev stage
- stage: Dev
  condition: eq(variables['Build.SourceBranchName'], 'development')
  jobs:
    - template: databricks/DAB_template.yml
      parameters:
        DATABRICKS_HOST: 'https://...'
        TARGET_ENV: 'dev'

# Preprod stage
- stage: Preprod
  condition: eq(variables['Build.SourceBranchName'], 'preprod')

# Prod stage
- stage: Prod
  condition: eq(variables['Build.SourceBranchName'], 'main')
  jobs:
    - template: changemanagement/changecreate.yml
    - template: databricks/DAB_template.yml
```

**No changes needed** - your deployment templates work as-is.

### Variable Groups (Unchanged)

All existing variable groups are used:
- `L-FEC-RADAR`
- `L-FEC-RADAR-DEV`
- `L-FEC-RADAR-PREPRD`
- `L-FEC-RADAR-PROD`
- `NexusCloud_eu.NpaRadarNexusUser@rabobank.com`

### Templates (Unchanged)

All existing templates are referenced:
- `compliancyscans/secretscan.yml`
- `compliancyscans/sonarqube.yml@templates`
- `compliancyscans/checkmarx.yml`
- `artifacts/downloadartifacts.yml`
- `databricks/DAB_template.yml`
- `changemanagement/changecreate.yml`
- `changemanagement/changeclose.yml`

---

## Migration Checklist

To migrate from single-bundle to multi-bundle testing:

- [ ] **Commit centralized test structure**
  ```bash
  git add Databricks/tests/
  git add Databricks/pyproject.toml
  git add Databricks/requirements-dev.txt
  git add Databricks/run_tests.sh
  ```

- [ ] **Verify all bundles have test directories**
  ```bash
  ls Databricks/radarv1/tests/
  ls Databricks/GCOB_Consumer/tests/
  ls Databricks/GCOB_Reportingv1/tests/
  ls Databricks/clusters/tests/
  ls Databricks/AdHocRequests/tests/
  ls Databricks/PartyCatalog/tests/
  ```

- [ ] **Test locally in Databricks workspace**
  ```python
  %sh
  cd /Workspace/Users/YOUR_EMAIL/Databricks_Learning/Databricks
  ./run_tests.sh --marker unit
  ```

- [ ] **Update Azure pipeline**
  - Replace `DAB-GCOB-reporting.yml` with `azure-pipelines-enterprise.yml`
  - OR rename `azure-pipelines-enterprise.yml` to match your naming

- [ ] **Run first pipeline**
  - Push to `development` branch
  - Monitor Azure DevOps pipeline
  - Check test results and coverage

- [ ] **Adjust coverage threshold if needed**
  - Start at 60%
  - Gradually increase as tests are added

- [ ] **Document bundle-specific test patterns**
  - Add README in each bundle's tests/ directory
  - Document fixtures and helpers

---

## Rollback Plan

If issues arise, you can **quickly rollback**:

1. **Revert to old pipeline**
   ```bash
   git checkout DAB-GCOB-reporting.yml
   ```

2. **Keep using radarv1 single-bundle tests**
   - No changes to radarv1 test code needed
   - Old conftest.py still works

3. **Debug centralized tests separately**
   - Test in Databricks workspace first
   - Fix issues before re-deploying to pipeline

---

## Benefits Summary

### Single-Bundle (Old)

**Pros:**
- ✅ Focused on one bundle (radarv1)
- ✅ High coverage (80%)
- ✅ Well-tested

**Cons:**
- ❌ Doesn't test other 5 bundles
- ❌ Duplicate fixtures if adding more bundles
- ❌ Separate pytest.ini per bundle

### Multi-Bundle (New)

**Pros:**
- ✅ Tests all 6 bundles in one run
- ✅ Shared fixtures across bundles
- ✅ Single pytest configuration
- ✅ Environment auto-detection
- ✅ Works in Databricks workspace AND Azure DevOps
- ✅ No code changes needed between environments

**Cons:**
- ⚠️ Initial coverage may be lower (60%) until tests are added
- ⚠️ Slightly longer test execution time (testing 6 bundles)

---

## Summary

**What you had:**
- ✅ Enterprise-grade pipeline for radarv1
- ✅ All security, quality, and deployment features

**What you now have:**
- ✅ **All the same enterprise features**
- ✅ **Tests ALL 6 bundles, not just radarv1**
- ✅ **Centralized test infrastructure**
- ✅ **Zero code changes between Databricks workspace and Azure DevOps**

**Migration effort:** Low - mostly configuration changes, test code stays the same!
