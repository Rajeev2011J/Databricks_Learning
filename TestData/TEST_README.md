# Party Data Generator - Pytest Test Suite

Comprehensive unit tests for the Party Data Generator notebook.

## Overview

This test suite provides comprehensive validation for synthetic party data generation using `dbldatagen`. The tests cover:

* **Configuration validation** - ID prefixes, party types, CDD types, KYC groups
* **Schema validation** - Column names, types, and counts
* **Data quality** - Row counts, null handling, data distribution
* **Format validation** - PartyIdentifier, EIN, postal codes, NAICS codes
* **Business rules** - Value constraints, valid enumerations
* **Integration** - Relationships between parent and child dataframes

## Files

* `party_data_generator.py` - Refactored module with testable functions
* `test_party_data_generator.py` - Pytest test suite (50+ tests)
* `TEST_README.md` - This documentation file

## Prerequisites

```bash
# Install pytest and dbldatagen
%pip install pytest dbldatagen
```

## Running Tests

### Run All Tests

```bash
pytest test_party_data_generator.py -v
```

### Run Specific Test Classes

```bash
# Test only party dataframe
pytest test_party_data_generator.py::TestPartyDataFrame -v

# Test only address dataframe
pytest test_party_data_generator.py::TestPartyAddressDataFrame -v

# Test only configurations
pytest test_party_data_generator.py::TestConfigurations -v
```

### Run Specific Tests

```bash
# Test PartyIdentifier format
pytest test_party_data_generator.py::TestPartyDataFrame::test_party_identifier_format -v

# Test schema validation
pytest test_party_data_generator.py::TestPartyDataFrame::test_party_df_schema_columns -v
```

### Run with Coverage Report

```bash
pytest test_party_data_generator.py --cov=party_data_generator --cov-report=html
```

### Run Tests in Parallel (faster)

```bash
%pip install pytest-xdist
pytest test_party_data_generator.py -v -n auto
```

## Test Coverage

### 1. Configuration Tests (5 tests)

| Test | Description |
|------|-------------|
| `test_id_prefixes_count` | Validates 21 ID prefixes exist |
| `test_id_prefixes_format` | Ensures all prefixes end with underscore |
| `test_party_types_count` | Validates 31 party types including None |
| `test_cdd_types_count` | Validates 39 CDD types including None |
| `test_kyc_groups_count` | Validates 31 KYC group company names |

### 2. Party DataFrame Tests (11 tests)

| Test | Description |
|------|-------------|
| `test_party_df_row_count` | Validates correct number of rows generated |
| `test_party_df_schema_columns` | Validates all 59 columns present |
| `test_party_identifier_format` | Validates PREFIX_NNNNNNNN format |
| `test_party_identifier_uniqueness` | Ensures >95% unique identifiers |
| `test_boolean_columns_types` | Validates boolean column values |
| `test_ein_format` | Validates XX-XXXXXXX EIN format |
| `test_party_type_values` | Ensures valid party type values |
| `test_kyc_group_values` | Ensures valid KYC group values |
| `test_name_fields_not_empty` | FirstName/LastName never null |
| `test_incorporation_number_format` | Validates INC+5digits format |

### 3. Party Address DataFrame Tests (5 tests)

| Test | Description |
|------|-------------|
| `test_party_address_row_count` | Validates row count |
| `test_party_address_schema` | Validates 11 columns present |
| `test_party_address_identifiers_valid` | PartyIdentifier from parent list |
| `test_postal_code_format` | 5-digit postal codes |
| `test_address_system_id_format` | ASID+4digits format |

### 4. Party NAICS DataFrame Tests (5 tests)

| Test | Description |
|------|-------------|
| `test_party_naics_row_count` | Validates row count |
| `test_party_naics_schema` | Validates 7 columns present |
| `test_naics_code_format` | 6-digit NAICS codes |
| `test_primary_naics_flag_values` | Y or N flag values |
| `test_naics_percentage_range` | 1-100 percentage range |

### 5. Party System Identifier DataFrame Tests (4 tests)

| Test | Description |
|------|-------------|
| `test_party_systemidentifier_row_count` | Validates row count |
| `test_party_systemidentifier_schema` | Validates 3 columns present |
| `test_local_system_identifier_format` | LSI+6digits format |
| `test_application_values` | Valid application names |

### 6. Party Alternative Names DataFrame Tests (5 tests)

| Test | Description |
|------|-------------|
| `test_party_altnames_row_count` | Validates row count |
| `test_party_altnames_schema` | Validates 7 columns present |
| `test_party_name_type_code_values` | Valid name type codes |
| `test_first_last_name_not_null` | Names never null |
| `test_full_name_constructed` | FullName properly constructed |

### 7. Integration Tests (2 tests)

| Test | Description |
|------|-------------|
| `test_child_dataframes_use_parent_ids` | Child DFs reference parent IDs |
| `test_complete_data_pipeline` | End-to-end pipeline works |

**Total: 37 tests across 7 test classes**

## Test Output Examples

### Successful Test Run

```
test_party_data_generator.py::TestConfigurations::test_id_prefixes_count PASSED
test_party_data_generator.py::TestPartyDataFrame::test_party_df_row_count PASSED
test_party_data_generator.py::TestPartyDataFrame::test_party_identifier_format PASSED
...
================================ 37 passed in 12.34s ================================
```

### Failed Test Example

```
FAILURE: test_party_identifier_format
AssertionError: Invalid PartyIdentifier: XYZ_12345678
```

## Test Fixtures

* `spark` (session-scoped) - Shared SparkSession for all tests
* `sample_party_ids` (function-scoped) - Sample IDs for child dataframe testing

## Continuous Integration

To integrate with CI/CD:

```yaml
# .github/workflows/test.yml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install pytest dbldatagen pyspark
      - name: Run tests
        run: pytest test_party_data_generator.py -v
```

## Debugging Failed Tests

### Verbose Output

```bash
pytest test_party_data_generator.py -vv
```

### Show Print Statements

```bash
pytest test_party_data_generator.py -v -s
```

### Run Last Failed Tests Only

```bash
pytest test_party_data_generator.py --lf
```

### Stop on First Failure

```bash
pytest test_party_data_generator.py -x
```

## Extending Tests

### Add New Test Class

```python
class TestNewFeature:
    """Test new feature description."""
    
    def test_new_validation(self, spark):
        """Test specific validation."""
        df = generate_party_df(spark, num_records=10)
        # Add assertions
        assert condition, "Error message"
```

### Add Parameterized Tests

```python
import pytest

@pytest.mark.parametrize("num_records", [10, 50, 100, 500])
def test_scalability(spark, num_records):
    """Test data generation at different scales."""
    party_df = generate_party_df(spark, num_records=num_records)
    assert party_df.count() == num_records
```

## Known Limitations

* PartyIdentifier uniqueness tested at 95% threshold (some collisions expected with random generation)
* Tests use small sample sizes for performance (10-100 records)
* Tests assume local Spark session available

## Troubleshooting

### Issue: `ModuleNotFoundError: No module named 'party_data_generator'`

**Solution:** Ensure `party_data_generator.py` is in the same directory as test file.

### Issue: `java.lang.OutOfMemoryError`

**Solution:** Reduce test sample sizes or increase Spark driver memory:

```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

### Issue: Tests slow to run

**Solution:** Run tests in parallel:

```bash
pytest test_party_data_generator.py -n 4
```

## Best Practices

1. **Run tests before committing** - Ensures code quality
2. **Add tests for new features** - Maintain high coverage
3. **Keep tests fast** - Use small sample sizes
4. **Test edge cases** - Null values, empty strings, boundary conditions
5. **Use descriptive test names** - Clearly indicate what's being tested

## Support

For issues or questions:
* Review test output carefully
* Check Spark logs for detailed errors
* Verify dbldatagen version compatibility
* Ensure PySpark environment configured correctly
