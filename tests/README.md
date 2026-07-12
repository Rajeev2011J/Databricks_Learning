# RDM Party Role Testing Framework

## Overview

This testing framework provides comprehensive Pytest-based testing for the **Radar Data Model (RDM) Party_Role** notebook. It implements structured testing patterns with reusable fixtures, mocked dependencies, and CI/CD integration.

## Project Structure

```
tests/
├── conftest.py                          # Shared fixtures and configuration
├── pytest.ini                           # Pytest configuration
├── requirements-test.txt                # Testing dependencies
├── README.md                           # This file
├── unit/                               # Unit tests
│   ├── test_identifier_generation.py   # LocalSystemIdentifier tests
│   └── test_business_logic.py          # Business logic validation
├── integration/                        # Integration tests
│   └── test_multi_system_scenarios.py  # Multi-system integration
└── data_quality/                       # Data quality tests (to be added)
    └── test_validation_rules.py
```

## Features

* **Comprehensive Test Coverage**: Unit, integration, and data quality tests
* **Reusable Fixtures**: Pre-configured Spark sessions, mock data, and utilities
* **Mocked Dependencies**: Databricks utilities (dbutils), storage accounts, environment variables
* **CI/CD Integration**: GitHub Actions workflow with automated testing
* **Coverage Reporting**: 80%+ coverage target with detailed reports
* **Parallel Execution**: Tests can run in parallel for faster feedback

## Prerequisites

* Python 3.9+
* Java 11+ (required for PySpark)
* pip

## Installation

### 1. Install testing dependencies

```bash
pip install -r requirements-test.txt
```

### 2. Verify installation

```bash
pytest --version
```

## Running Tests

### Run all tests

```bash
cd tests
pytest
```

### Run specific test categories

```bash
# Unit tests only
pytest unit/ -v

# Integration tests only
pytest integration/ -v

# Tests marked as 'smoke'
pytest -m smoke

# Tests marked as 'unit'
pytest -m unit
```

### Run tests with coverage

```bash
pytest --cov=../ --cov-report=html --cov-report=term-missing
```

Coverage report will be generated in `coverage_report/index.html`

### Run tests in parallel

```bash
pytest -n auto
```

### Run specific test file

```bash
pytest unit/test_identifier_generation.py -v
```

### Run specific test function

```bash
pytest unit/test_identifier_generation.py::TestLocalSystemIdentifierGeneration::test_gic_identifier_format -v
```

## Test Scenarios Covered

### Unit Tests

#### Identifier Generation (`test_identifier_generation.py`)
* GIC identifier format: `GIC_<cod_institucional>`
* RANZ identifier format: `RANZ_<pkey_src_object>`
* GCDS identifier format: `GCDS_<gcid>`
* CASE logic prioritization (GIC > RANZ > GCDS)
* Identifier uniqueness validation
* Null handling in identifier generation

#### Business Logic (`test_business_logic.py`)
* COALESCE priority: GCDS > GIC > RANZ
* RANZ lifecycle status code mappings (50+ codes)
* Date logic validation (StartDate < ChangeDate)
* RunType determination (daily vs historical)
* Load_Date default to today

### Integration Tests

#### Multi-System Scenarios (`test_multi_system_scenarios.py`)
* GCDS + GIC integration
  * KeyStore join on `KeyStore_type = 'GIC'`
  * Party exists in GCDS + GIC only
* GCDS + RANZ integration
  * KeyStore join on `KeyStore_type = 'CB RANZ'`
  * Party exists in GCDS + RANZ only
* All systems integration
  * Party exists in all three systems
  * COALESCE priority validation
  * Left anti join excludes matched parties
  * UNION of GCDS and NonGCDS parties
* PartyIdentifier join
  * Party filter for GIC and GCDS
  * Final left join preserves all Party records

## Fixtures Reference

### Session-Scoped Fixtures

* `spark`: Configured Spark session for testing
* `mock_dbutils`: Mocked Databricks utilities

### Function-Scoped Fixtures

* `mock_environment_variables`: Mocked environment variables
* `mock_storage_functions`: Mocked storage authentication
* `sample_gcds_keystore_data`: Sample GCDS KeyStore data
* `sample_gcds_partyrole_data`: Sample GCDS PartyRole data
* `sample_gic_pessoa_data`: Sample GIC Pessoa data
* `sample_gic_tipo_cadastro_data`: Sample GIC Tipo Cadastro data
* `sample_ranz_party_data`: Sample RANZ Party data
* `sample_party_data`: Sample Party data
* `sample_party_system_identifier_data`: Sample Party System Identifier data

See `conftest.py` for complete fixture definitions.

## Writing New Tests

### Test Structure (AAA Pattern)

```python
def test_my_feature(spark):
    # Arrange - Set up test data
    data = [("value1",), ("value2",)]
    df = spark.createDataFrame(data, ["column_name"])
    
    # Act - Execute the logic
    result = df.filter(col("column_name") == "value1")
    
    # Assert - Verify expectations
    assert result.count() == 1
```

### Using Fixtures

```python
def test_with_fixtures(spark, sample_gcds_keystore_data):
    # Fixtures are automatically injected
    df_gcds = sample_gcds_keystore_data
    assert df_gcds.count() > 0
```

### Parametrized Tests

```python
@pytest.mark.parametrize("input_val,expected", [
    ("AC", "Active Client"),
    ("BL", "Blocked Client"),
])
def test_status_mapping(input_val, expected):
    assert mapping[input_val] == expected
```

## CI/CD Integration

### GitHub Actions Workflow

The `.github/workflows/pytest-ci.yml` workflow automatically:

1. **Runs on**:
   * Every push to `main`, `develop`, or `feature/*` branches
   * Every pull request to `main` or `develop`
   * Daily at 2 AM UTC (scheduled)

2. **Test Matrix**: Tests run on Python 3.9, 3.10, 3.11

3. **Pipeline Stages**:
   * **Test**: Run unit and integration tests
   * **Code Quality**: Black, Flake8, Pylint checks
   * **Coverage Threshold**: Enforce 80%+ coverage
   * **Databricks Integration**: Upload tests to Databricks workspace

### Setting Up GitHub Secrets

Configure these secrets in your GitHub repository:

* `DATABRICKS_HOST`: Your Databricks workspace URL
* `DATABRICKS_TOKEN`: Personal access token
* `DATABRICKS_USER`: Your Databricks username

### Coverage Badge

Add to your main README:

```markdown
![Coverage](https://img.shields.io/badge/coverage-80%25-brightgreen)
```

## Best Practices

### Test Naming
* Use descriptive names: `test_<feature>_<scenario>_<expected_result>`
* Example: `test_coalesce_gcds_priority_when_all_sources_present`

### Test Organization
* Group related tests in classes
* Use markers for categorization: `@pytest.mark.unit`, `@pytest.mark.integration`

### Data Isolation
* Each test should be independent
* Use fixtures for test data
* Clean up resources after tests

### Assertions
* Use specific assertions: `assert result == expected`
* Include helpful messages: `assert count > 0, "Expected non-zero count"`

### Performance
* Keep unit tests fast (<1 second each)
* Mark slow tests: `@pytest.mark.slow`
* Use session-scoped fixtures for expensive setup

## Troubleshooting

### Common Issues

**Issue**: `ImportError: No module named 'pyspark'`
```bash
pip install pyspark==3.5.0
```

**Issue**: `Java not found`
```bash
# Install Java 11
sudo apt-get install openjdk-11-jdk
```

**Issue**: `Coverage below threshold`
```bash
# Check uncovered lines
pytest --cov=../ --cov-report=term-missing
```

**Issue**: `Test data mismatch`
```bash
# Verify fixture data in conftest.py
# Update sample data to match real schema
```

## Extending the Framework

### Adding New Test Categories

1. Create directory: `tests/new_category/`
2. Add marker in `pytest.ini`:
   ```ini
   markers =
       new_category: Description of new category
   ```
3. Mark tests: `@pytest.mark.new_category`

### Adding New Fixtures

1. Add to `conftest.py`:
   ```python
   @pytest.fixture
   def my_new_fixture(spark):
       # Setup
       yield data
       # Teardown (optional)
   ```

2. Use in tests:
   ```python
   def test_feature(my_new_fixture):
       assert my_new_fixture is not None
   ```

## Roadmap

* ✅ Unit tests for identifier generation
* ✅ Unit tests for business logic
* ✅ Integration tests for multi-system scenarios
* ✅ CI/CD pipeline with GitHub Actions
* ⬜ Data quality validation tests
* ⬜ Performance benchmarking tests
* ⬜ Regression test suite
* ⬜ Property-based testing with Hypothesis
* ⬜ Integration with Great Expectations

## Contributing

1. Create feature branch: `git checkout -b feature/new-test`
2. Write tests following AAA pattern
3. Run tests locally: `pytest`
4. Ensure coverage: `pytest --cov`
5. Submit pull request

## Resources

* [Pytest Documentation](https://docs.pytest.org/)
* [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html)
* [Databricks Testing Best Practices](https://docs.databricks.com/dev-tools/databricks-utils.html)

## Contact

For questions or issues:
* Create GitHub issue
* Contact: Aayushi.jain@rabobank.nl (RDM Owner)

---

**Version**: 1.0.0  
**Last Updated**: June 18, 2026  
**Maintainer**: Radar Data Model Team