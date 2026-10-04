# Quick Start Guide - DAB Testing Framework

## 🎯 What Was Created

A centralized pytest testing framework for all 6 Databricks Asset Bundles:
* AdHocRequests
* PartyCatalog
* radarv1
* clusters
* GCOB_Consumer
* GCOB_Reportingv1

## 📦 Installation

```bash
cd /Workspace/Users/net.rajeev@gmail.com/Databricks_Learning/Databricks/tests_centralized
pip install -r requirements-test.txt
```

## ⚡ Run Your First Test

```bash
# Validate all bundle configurations
python run_tests.py validation

# Test a specific bundle
python run_tests.py bundle radarv1

# Run all tests
python run_tests.py all
```

## 📊 Common Commands

```bash
# Run only fast unit tests
python run_tests.py unit

# Test all bundles have valid YAML
pytest -k "test_yaml_syntax"

# Test service principal configurations
pytest -k "test_service_principal"

# Generate coverage report
pytest --cov=. --cov-report=html
```

## 🎓 Next Steps

1. Review example tests in `unit/adhoc_requests/` and `unit/radarv1/`
2. Add bundle-specific tests for your bundles
3. Run tests before deploying bundles
4. Integrate with CI/CD pipeline

## 📖 Full Documentation

See `README.md` for complete documentation.
