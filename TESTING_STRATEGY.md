# Radar Data Model Unit Testing Strategy with Pytest

## Executive Summary

This document outlines the **unit testing strategy** for Radar Data Model (RDM) products using Pytest, specifically focusing on the **RDM_Party_role** notebook. Unit testing forms the foundation of our testing pyramid, providing fast, isolated validation of individual components and business logic.

## Problem Statement

### Current State

Testing within Databricks environments is currently:
* **Manual or ad-hoc**: No structured testing approach
* **Not standardized**: Inconsistent practices across notebooks
* **Lacking automation**: No CI/CD integration for validation

### Risks

* Undetected regressions in production
* Inconsistent testing practices across the team
* Limited confidence in deployments
* Difficulty in maintaining code quality
* Longer development cycles due to manual testing

## What is Unit Testing?

**Unit testing** focuses on testing individual components, functions, or business logic in isolation from external dependencies. For RDM notebooks, this means:

* Testing data transformations on small, controlled datasets
* Validating business rules (identifier generation, status mappings)
* Verifying edge cases (null handling, empty datasets)
* Fast execution (< 1 second per test)
* No dependencies on actual storage accounts, databases, or external systems

### Unit Tests vs Other Tests

```
Unit Tests:
  ✓ Test single function/transformation
  ✓ Use mocked data (fixtures)
  ✓ Fast (< 1 second each)
  ✓ No external dependencies
  ✓ Run in isolation

Integration Tests (NOT covered here):
  ✗ Test multiple systems together
  ✗ Use real data connections
  ✗ Slower execution
  ✗ External dependencies
```

## Solution: Pytest-Based Unit Testing Framework

### Objectives

1. **Standardize Testing**: Implement consistent unit testing patterns
2. **Automate Validation**: Fast feedback on code changes
3. **Improve Coverage**: Achieve 80%+ code coverage for business logic
4. **Enable Confidence**: Catch bugs before integration
5. **Support Collaboration**: Share reusable fixtures and utilities

## Unit Testing Approach

### 1. AAA Pattern (Arrange-Act-Assert)

All unit tests follow the **AAA Pattern**:

```python
def test_gic_identifier_format(spark):
    # Arrange - Set up test data
    data = [("12345",), ("67890",)]
    df = spark.createDataFrame(data, ["COD_INSTITUCIONAL"])
    
    # Act - Execute the transformation
    df_result = df.withColumn(
        "LocalSystemIdentifier",
        concat(lit("GIC_"), col("COD_INSTITUCIONAL"))
    )
    
    # Assert - Verify expected outcome
    results = df_result.select("LocalSystemIdentifier").collect()
    assert results[0][0] == "GIC_12345"
    assert results[1][0] == "GIC_67890"
```

### 2. Test Isolation

Each unit test should:
* Run independently (no shared state)
* Use mocked data (fixtures)
* Not depend on other tests
* Be repeatable (same input → same output)

### 3. Fast Execution

Unit tests must be fast:
* Target: < 1 second per test
* Use small datasets (5-10 rows)
* Avoid external calls
* Mock expensive operations

## Reusable Fixtures

Centralized in `conftest.py` for all unit tests:

### Session-Scoped Fixtures

```python
@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder
        .appName("RDM_Unit_Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def mock_dbutils():
    """Mock Databricks utilities."""
    dbutils_mock = Mock()
    # Mock secrets, widgets, etc.
    return dbutils_mock
```

### Function-Scoped Fixtures

```python
@pytest.fixture
def sample_gic_data(spark):
    """Sample GIC data for testing."""
    schema = StructType([
        StructField("COD_INSTITUCIONAL", StringType(), True)
    ])
    data = [("12345",), ("67890",), ("11111",)]
    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_ranz_data(spark):
    """Sample RANZ data for testing."""
    schema = StructType([
        StructField("PKEY_SRC_OBJECT", StringType(), True)
    ])
    data = [("RANZ_PK001",), ("RANZ_PK002",)]
    return spark.createDataFrame(data, schema)
```

## Unit Test Scenarios for RDM_Party_role

### Category 1: Identifier Generation

Testing the core business logic for creating LocalSystemIdentifier:

#### Test: GIC Identifier Format
```python
def test_gic_identifier_format(spark):
    """Test GIC identifier format: GIC_<cod_institucional>."""
    # Arrange
    data = [("12345",), ("67890",)]
    df = spark.createDataFrame(data, ["COD_INSTITUCIONAL"])
    
    # Act
    df_result = df.withColumn(
        "LocalSystemIdentifier",
        concat(lit("GIC_"), col("COD_INSTITUCIONAL"))
    )
    
    # Assert
    results = df_result.select("LocalSystemIdentifier").collect()
    assert all(row[0].startswith("GIC_") for row in results)
    assert results[0][0] == "GIC_12345"
```

#### Test: RANZ Identifier Format
```python
def test_ranz_identifier_format(spark):
    """Test RANZ identifier format: RANZ_<pkey_src_object>."""
    # Arrange
    data = [("RANZ_PK001",)]
    df = spark.createDataFrame(data, ["PKEY_SRC_OBJECT"])
    
    # Act
    df_result = df.withColumn(
        "LocalSystemIdentifier",
        concat(lit("RANZ_"), col("PKEY_SRC_OBJECT"))
    )
    
    # Assert
    result = df_result.first()[0]
    assert result == "RANZ_RANZ_PK001"
```

#### Test: GCDS Identifier Format
```python
def test_gcds_identifier_format(spark):
    """Test GCDS identifier format: GCDS_<gcid>."""
    # Arrange
    data = [("GCDS001",)]
    df = spark.createDataFrame(data, ["gcid"])
    
    # Act
    df_result = df.withColumn(
        "LocalSystemIdentifier",
        concat(lit("GCDS_"), col("gcid"))
    )
    
    # Assert
    assert df_result.first()[0] == "GCDS_GCDS001"
```

#### Test: CASE Logic Prioritization
```python
def test_case_logic_prioritization(spark):
    """Test CASE logic prioritizes GIC match > RANZ match > GCDS default."""
    # Arrange
    schema = StructType([
        StructField("gcid", StringType(), True),
        StructField("identifier", StringType(), True),
        StructField("KeyStore_type", StringType(), True),
        StructField("gic_cod", StringType(), True),
        StructField("ranz_id", StringType(), True)
    ])
    
    data = [
        ("GCDS001", "12345", "GIC", "12345", None),      # Should be GIC
        ("GCDS002", "RANZ001", "CB RANZ", None, "RANZ001"),  # Should be RANZ
        ("GCDS003", "OTHER", "OTHER", None, None)        # Should be GCDS
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Act
    df_result = df.withColumn(
        "LocalSystemIdentifier",
        when(
            (col("identifier") == col("gic_cod")) & (col("KeyStore_type") == "GIC"),
            concat(lit("GIC_"), col("gic_cod"))
        ).when(
            (col("identifier") == col("ranz_id")) & (col("KeyStore_type") == "CB RANZ"),
            concat(lit("RANZ_"), col("ranz_id"))
        ).otherwise(
            concat(lit("GCDS_"), col("gcid"))
        )
    )
    
    # Assert
    results = df_result.select("LocalSystemIdentifier").collect()
    assert results[0][0] == "GIC_12345"
    assert results[1][0] == "RANZ_RANZ001"
    assert results[2][0] == "GCDS_GCDS003"
```

#### Test: Null Handling
```python
def test_null_handling_in_identifier_generation(spark):
    """Test handling of null values in identifier generation."""
    # Arrange
    schema = StructType([
        StructField("COD_INSTITUCIONAL", StringType(), True)
    ])
    data = [("12345",), (None,), ("67890",)]
    df = spark.createDataFrame(data, schema)
    
    # Act
    df_result = df.withColumn(
        "LocalSystemIdentifier",
        concat(lit("GIC_"), col("COD_INSTITUCIONAL"))
    )
    
    # Assert
    results = df_result.select("LocalSystemIdentifier").collect()
    assert results[0][0] == "GIC_12345"
    assert results[1][0] is None  # Concat with null produces null
    assert results[2][0] == "GIC_67890"
```

### Category 2: Business Logic Validation

#### Test: COALESCE Priority
```python
def test_coalesce_gcds_priority(spark):
    """Test COALESCE selects GCDS value when present."""
    # Arrange
    schema = StructType([
        StructField("gcds_value", StringType(), True),
        StructField("gic_value", StringType(), True),
        StructField("ranz_value", StringType(), True)
    ])
    data = [("GCDS_Data", "GIC_Data", "RANZ_Data")]
    df = spark.createDataFrame(data, schema)
    
    # Act
    df_result = df.withColumn(
        "result",
        coalesce(col("gcds_value"), col("gic_value"), col("ranz_value"))
    )
    
    # Assert
    assert df_result.select("result").first()[0] == "GCDS_Data"

def test_coalesce_gic_fallback(spark):
    """Test COALESCE selects GIC value when GCDS is null."""
    # Arrange
    schema = StructType([
        StructField("gcds_value", StringType(), True),
        StructField("gic_value", StringType(), True),
        StructField("ranz_value", StringType(), True)
    ])
    data = [(None, "GIC_Data", "RANZ_Data")]
    df = spark.createDataFrame(data, schema)
    
    # Act
    df_result = df.withColumn(
        "result",
        coalesce(col("gcds_value"), col("gic_value"), col("ranz_value"))
    )
    
    # Assert
    assert df_result.select("result").first()[0] == "GIC_Data"
```

#### Test: RANZ Status Mapping
```python
@pytest.mark.parametrize("status_code,expected_description", [
    ("AC", "Active Client"),
    ("BL", "Blocked Client"),
    ("CL", "Closed Client"),
    ("DC", "Declined Client"),
    ("AP", "Applicant Client")
])
def test_ranz_status_mappings(spark, status_code, expected_description):
    """Test RANZ status code mappings."""
    # Arrange
    status_mapping = {
        "AC": "Active Client",
        "BL": "Blocked Client",
        "CL": "Closed Client",
        "DC": "Declined Client",
        "AP": "Applicant Client"
    }
    
    data = [(status_code,)]
    df = spark.createDataFrame(data, ["status_code"])
    
    # Act
    mapping_df = spark.createDataFrame(
        [(k, v) for k, v in status_mapping.items()],
        ["status_code", "description"]
    )
    df_result = df.join(mapping_df, "status_code", "left")
    
    # Assert
    result = df_result.select("description").first()[0]
    assert result == expected_description
```

#### Test: Date Logic
```python
def test_start_date_before_change_date(spark):
    """Test PartyRoleLifecycleStartDate < PartyRoleLifecycleChangeDate."""
    from pyspark.sql.functions import to_date
    
    # Arrange
    schema = StructType([
        StructField("StartDate", StringType(), True),
        StructField("ChangeDate", StringType(), True)
    ])
    data = [
        ("2024-01-01", "2024-01-15"),
        ("2024-02-01", "2024-02-10")
    ]
    df = spark.createDataFrame(data, schema)
    
    # Act
    df_result = df.withColumn("StartDate_dt", to_date(col("StartDate"))) \
                  .withColumn("ChangeDate_dt", to_date(col("ChangeDate"))) \
                  .withColumn("is_valid", col("StartDate_dt") < col("ChangeDate_dt"))
    
    # Assert
    results = df_result.select("is_valid").collect()
    assert all(row[0] for row in results)
```

#### Test: RunType Logic
```python
def test_historical_run_when_load_date_provided():
    """Test RunType becomes 'historical' when Load_Date is provided."""
    # Arrange
    Load_Date = "20240115"
    RunType = "daily"
    
    # Act
    if Load_Date:
        RunType = "historical"
    
    # Assert
    assert RunType == "historical"

def test_daily_run_when_no_load_date():
    """Test RunType remains 'daily' when no Load_Date is provided."""
    # Arrange
    Load_Date = ""
    RunType = "daily"
    
    # Act
    if Load_Date:
        RunType = "historical"
    
    # Assert
    assert RunType == "daily"
```

### Category 3: Data Validation

#### Test: Uniqueness
```python
def test_identifier_uniqueness(spark):
    """Test that LocalSystemIdentifier values are unique after distinct()."""
    # Arrange
    data = [
        ("GIC_12345",),
        ("GIC_12345",),  # Duplicate
        ("RANZ_RANZ001",),
        ("GCDS_GCDS001",)
    ]
    df = spark.createDataFrame(data, ["LocalSystemIdentifier"])
    
    # Act
    df_unique = df.distinct()
    
    # Assert
    assert df_unique.count() == 3
    identifiers = [row[0] for row in df_unique.collect()]
    assert len(identifiers) == len(set(identifiers))
```

#### Test: Empty Dataset
```python
def test_empty_dataset_handling(spark):
    """Test handling of empty datasets."""
    # Arrange
    schema = StructType([
        StructField("COD_INSTITUCIONAL", StringType(), True)
    ])
    df = spark.createDataFrame([], schema)
    
    # Act
    df_result = df.withColumn(
        "LocalSystemIdentifier",
        concat(lit("GIC_"), col("COD_INSTITUCIONAL"))
    )
    
    # Assert
    assert df_result.count() == 0
```

## Test Organization

### File Structure

```
tests/
├── conftest.py                          # Shared fixtures
├── pytest.ini                           # Configuration
├── requirements-test.txt                # Dependencies
└── unit/                                # Unit tests only
    ├── test_identifier_generation.py   # Identifier tests
    └── test_business_logic.py          # Business logic tests
```

### Test Class Organization

Group related tests in classes:

```python
class TestLocalSystemIdentifierGeneration:
    """Test LocalSystemIdentifier generation for GIC, RANZ, and GCDS."""
    
    def test_gic_identifier_format(self, spark):
        # Test implementation
        pass
    
    def test_ranz_identifier_format(self, spark):
        # Test implementation
        pass
    
    def test_gcds_identifier_format(self, spark):
        # Test implementation
        pass
```

## Running Unit Tests

### Run All Unit Tests

```bash
cd tests
pytest unit/ -v
```

### Run Specific Test File

```bash
pytest unit/test_identifier_generation.py -v
```

### Run Specific Test

```bash
pytest unit/test_identifier_generation.py::TestLocalSystemIdentifierGeneration::test_gic_identifier_format -v
```

### Run with Coverage

```bash
pytest unit/ --cov=../ --cov-report=term-missing
```

### Run Marked Tests

```bash
# Run only unit tests
pytest -m unit

# Run only fast tests
pytest -m "not slow"
```

## Pytest Configuration

### pytest.ini

```ini
[pytest]
# Test discovery
python_files = test_*.py
python_classes = Test*
python_functions = test_*

# Paths
testpaths = unit

# Options
addopts = 
    -v
    --tb=short
    --strict-markers
    --cov=../
    --cov-report=term-missing
    --cov-fail-under=80

# Markers for unit tests
markers =
    unit: Unit tests for individual components
    fast: Fast-running tests (< 1 second)
    slow: Slower tests that may need optimization
```

## Best Practices for Unit Testing

### 1. Test Naming

Use descriptive names following the pattern:
```
test_<what>_<condition>_<expected_result>
```

Examples:
* `test_gic_identifier_format_with_valid_input_returns_correct_format`
* `test_coalesce_with_null_gcds_returns_gic_value`
* `test_identifier_generation_with_null_input_returns_null`

### 2. Single Responsibility

Each test should verify one specific behavior:

```python
# Good - Tests one thing
def test_gic_identifier_starts_with_prefix(spark):
    result = generate_identifier("12345", "GIC")
    assert result.startswith("GIC_")

def test_gic_identifier_includes_code(spark):
    result = generate_identifier("12345", "GIC")
    assert "12345" in result

# Bad - Tests multiple things
def test_gic_identifier(spark):
    result = generate_identifier("12345", "GIC")
    assert result.startswith("GIC_")
    assert "12345" in result
    assert len(result) == 9
```

### 3. Clear Assertions

Use specific assertions with helpful messages:

```python
# Good
assert result.count() == 5, f"Expected 5 records, got {result.count()}"
assert identifier == "GIC_12345", f"Expected GIC_12345, got {identifier}"

# Bad
assert result
assert identifier
```

### 4. Use Fixtures

Avoid repeating setup code:

```python
# Good - Use fixture
def test_with_fixture(sample_gic_data):
    result = process_data(sample_gic_data)
    assert result.count() > 0

# Bad - Repeat setup
def test_without_fixture(spark):
    data = [("12345",), ("67890",)]
    df = spark.createDataFrame(data, ["COD_INSTITUCIONAL"])
    result = process_data(df)
    assert result.count() > 0
```

### 5. Test Edge Cases

Always test:
* Null values
* Empty datasets
* Boundary conditions
* Invalid inputs
* Special characters

```python
def test_null_handling(spark):
    data = [(None,)]
    df = spark.createDataFrame(data, ["value"])
    result = transform(df)
    assert result.first()[0] is None

def test_empty_dataset(spark):
    df = spark.createDataFrame([], schema)
    result = transform(df)
    assert result.count() == 0
```

### 6. Parametrized Tests

Use `@pytest.mark.parametrize` for multiple similar tests:

```python
@pytest.mark.parametrize("input_code,expected_output", [
    ("12345", "GIC_12345"),
    ("67890", "GIC_67890"),
    ("11111", "GIC_11111")
])
def test_identifier_generation(spark, input_code, expected_output):
    result = generate_identifier(input_code)
    assert result == expected_output
```

### 7. Keep Tests Fast

* Use small datasets (5-10 rows max)
* Avoid external calls
* Mock expensive operations
* Target: < 1 second per test

```python
# Good - Small dataset
def test_transformation(spark):
    data = [("value1",), ("value2",)]  # Only 2 rows
    df = spark.createDataFrame(data, ["col"])
    result = transform(df)
    assert result.count() == 2

# Bad - Large dataset
def test_transformation_slow(spark):
    data = [(f"value{i}",) for i in range(10000)]  # 10K rows
    df = spark.createDataFrame(data, ["col"])
    result = transform(df)
    assert result.count() == 10000
```

## Code Coverage

### Coverage Targets

* **Overall Target**: 80%+ for unit tests
* **Business Logic**: 100% coverage
* **Edge Cases**: Explicit test cases

### Generate Coverage Report

```bash
# Terminal output
pytest unit/ --cov=../ --cov-report=term-missing

# HTML report
pytest unit/ --cov=../ --cov-report=html
# Open coverage_report/index.html
```

### Coverage in CI/CD

```yaml
- name: Run unit tests with coverage
  run: |
    cd tests
    pytest unit/ --cov=../ --cov-report=xml --cov-fail-under=80
```

## Mocking Databricks Components

### Mock dbutils

```python
@pytest.fixture
def mock_dbutils():
    """Mock Databricks utilities."""
    dbutils_mock = Mock()
    
    # Mock secrets
    secrets_mock = Mock()
    secrets_mock.get.return_value = "mock_secret"
    dbutils_mock.secrets = secrets_mock
    
    # Mock widgets
    widgets_mock = Mock()
    widgets_mock.get.return_value = ""
    dbutils_mock.widgets = widgets_mock
    
    return dbutils_mock
```

### Mock Environment Variables

```python
@pytest.fixture
def mock_env_vars():
    """Mock environment variables."""
    env_vars = {
        "APP_REG_APP_ID": "test-app-id",
        "STORAGE_NAME": "test-storage"
    }
    
    for key, value in env_vars.items():
        os.environ[key] = value
    
    yield env_vars
    
    # Cleanup
    for key in env_vars.keys():
        os.environ.pop(key, None)
```

## Common Unit Testing Patterns

### Pattern 1: Test Data Transformation

```python
def test_add_column_transformation(spark):
    # Arrange
    data = [("value1",)]
    df = spark.createDataFrame(data, ["col1"])
    
    # Act
    df_result = df.withColumn("col2", lit("new_value"))
    
    # Assert
    assert "col2" in df_result.columns
    assert df_result.select("col2").first()[0] == "new_value"
```

### Pattern 2: Test Filter Logic

```python
def test_filter_active_records(spark):
    # Arrange
    data = [("Active",), ("Inactive",), ("Active",)]
    df = spark.createDataFrame(data, ["status"])
    
    # Act
    df_filtered = df.filter(col("status") == "Active")
    
    # Assert
    assert df_filtered.count() == 2
```

### Pattern 3: Test Join Operations

```python
def test_join_two_dataframes(spark):
    # Arrange
    df1 = spark.createDataFrame([("1", "A")], ["id", "value1"])
    df2 = spark.createDataFrame([("1", "B")], ["id", "value2"])
    
    # Act
    df_joined = df1.join(df2, "id", "inner")
    
    # Assert
    assert df_joined.count() == 1
    assert df_joined.columns == ["id", "value1", "value2"]
```

### Pattern 4: Test Aggregations

```python
def test_count_by_group(spark):
    # Arrange
    data = [("A",), ("A",), ("B",)]
    df = spark.createDataFrame(data, ["group"])
    
    # Act
    df_agg = df.groupBy("group").count()
    
    # Assert
    assert df_agg.count() == 2
    result_a = df_agg.filter(col("group") == "A").select("count").first()[0]
    assert result_a == 2
```

## Troubleshooting

### Issue: Spark Session Not Starting

```bash
# Solution: Check Java installation
java -version

# Install Java 11 if needed
sudo apt-get install openjdk-11-jdk
```

### Issue: Import Errors

```bash
# Solution: Install dependencies
pip install -r requirements-test.txt
```

### Issue: Tests Running Slowly

```python
# Solution: Reduce dataset size
# Bad
data = [(i,) for i in range(10000)]

# Good
data = [(1,), (2,), (3,)]
```

### Issue: Fixture Not Found

```python
# Solution: Check conftest.py is present
# Ensure tests/conftest.py exists
# Fixtures are auto-discovered by pytest
```

## Summary

Unit testing with Pytest provides:

* **Fast Feedback**: Tests run in seconds
* **Isolation**: Each test is independent
* **Coverage**: Track code coverage metrics
* **Maintainability**: Easy to update and extend
* **Confidence**: Catch bugs early

### Key Takeaways

1. **Use AAA Pattern**: Arrange → Act → Assert
2. **Keep Tests Fast**: < 1 second per test
3. **Test Edge Cases**: Nulls, empty datasets, boundaries
4. **Use Fixtures**: Reusable test data and mocks
5. **Parametrize**: Reduce duplicate test code
6. **Clear Naming**: Tests should be self-documenting
7. **Mock Dependencies**: No external calls in unit tests

---

**Document Version**: 2.0 (Unit Testing Focus)  
**Last Updated**: January 18, 2025  
**Focus**: Unit Testing Only with Pytest  
**Status**: ✅ Ready for Implementation
