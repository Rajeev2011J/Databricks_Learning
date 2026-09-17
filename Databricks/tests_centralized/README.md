# Centralized Pytest Setup for Databricks Asset Bundles

Comprehensive testing framework for 6 Databricks Asset Bundles (DABs).

## 📁 Directory Structure

```
tests_centralized/
├── README.md                      # Documentation
├── pytest.ini                     # Pytest configuration
├── conftest.py                    # Shared fixtures
├── requirements-test.txt          # Test dependencies
├── run_tests.py                   # Test runner script
├── utils/                         # Shared utilities
│   └── bundle_validator.py
├── unit/                          # Unit tests
│   ├── test_all_bundles_common.py
│   ├── adhoc_requests/
│   ├── radarv1/
│   └── ...
└── integration/                   # Integration tests
```

## 🚀 Quick Start

### Install Dependencies
```bash
pip install -r requirements-test.txt
```

### Run Tests
```bash
# All tests
python run_tests.py all

# Unit tests only
python run_tests.py unit

# Specific bundle
python run_tests.py bundle radarv1

# Direct pytest
pytest -v
pytest -m validation
```

## 🏷️ Test Markers

* `unit` / `integration` - Test type
* `adhoc_requests`, `radarv1`, `clusters`, etc. - Bundle-specific
* `validation`, `deployment`, `permissions` - Test category
* `dev`, `preprod`, `prod` - Environment-specific

## 📊 Coverage
```bash
pytest --cov=. --cov-report=html
```

## 📝 Writing Tests

See examples in `unit/` directories. Use BundleValidator utility for config validation.
