# Databricks notebook source
# DBTITLE 1,RDM Party Role Testing Framework - Quick Start
# MAGIC %md
# MAGIC # RDM Party Role Testing Framework - Quick Start
# MAGIC
# MAGIC This notebook demonstrates how to run the Pytest testing framework for the RDM_Party_role notebook from within Databricks.
# MAGIC
# MAGIC ## What's Included
# MAGIC
# MAGIC * **Unit Tests**: Test individual components (identifier generation, business logic)
# MAGIC * **Integration Tests**: Test multi-system scenarios (GCDS + GIC + RANZ)
# MAGIC * **CI/CD Integration**: GitHub Actions workflow
# MAGIC * **Coverage Reporting**: Track test coverage
# MAGIC
# MAGIC ## Test Structure
# MAGIC
# MAGIC ```
# MAGIC tests/
# MAGIC ├── conftest.py                 # Shared fixtures
# MAGIC ├── pytest.ini                  # Configuration
# MAGIC ├── unit/                       # Unit tests
# MAGIC │   ├── test_identifier_generation.py
# MAGIC │   └── test_business_logic.py
# MAGIC └── integration/                # Integration tests
# MAGIC     └── test_multi_system_scenarios.py
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Install Testing Dependencies
# MAGIC %pip install pytest pytest-cov pytest-mock --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Set Test Directory Path
import os

# Set the test directory path
test_dir = "/Workspace/Users/net.rajeev@gmail.com/Databricks_Learning/tests"

print(f"Test directory: {test_dir}")
print(f"Exists: {os.path.exists(test_dir)}")

# COMMAND ----------

# DBTITLE 1,Run All Tests
import subprocess
import sys

# Change to test directory
os.chdir(test_dir)

# Run pytest
result = subprocess.run(
    [sys.executable, "-m", "pytest", "-v", "--tb=short"],
    capture_output=True,
    text=True
)

print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)

print(f"\nReturn code: {result.returncode}")

# COMMAND ----------

# DBTITLE 1,Run Unit Tests Only
# Run only unit tests
result = subprocess.run(
    [sys.executable, "-m", "pytest", "unit/", "-v", "--tb=short"],
    capture_output=True,
    text=True,
    cwd=test_dir
)

print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)

# COMMAND ----------

# DBTITLE 1,Run Integration Tests Only
# Run only integration tests
result = subprocess.run(
    [sys.executable, "-m", "pytest", "integration/", "-v", "--tb=short"],
    capture_output=True,
    text=True,
    cwd=test_dir
)

print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)

# COMMAND ----------

# DBTITLE 1,Run Tests with Coverage Report
# Run tests with coverage
result = subprocess.run(
    [sys.executable, "-m", "pytest", "--cov=../", "--cov-report=term-missing", "--tb=short"],
    capture_output=True,
    text=True,
    cwd=test_dir
)

print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)

print("\n" + "="*60)
print("Coverage report generated!")
print("="*60)

# COMMAND ----------

# DBTITLE 1,Run Specific Test File
# Run a specific test file
test_file = "unit/test_identifier_generation.py"

result = subprocess.run(
    [sys.executable, "-m", "pytest", test_file, "-v"],
    capture_output=True,
    text=True,
    cwd=test_dir
)

print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)

# COMMAND ----------

# DBTITLE 1,Next Steps
# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC ### 1. Add More Test Scenarios
# MAGIC * Data quality validation tests
# MAGIC * Performance benchmarking tests
# MAGIC * Regression test suite
# MAGIC
# MAGIC ### 2. Integrate with CI/CD
# MAGIC * Set up GitHub Actions workflow
# MAGIC * Configure Databricks secrets in GitHub
# MAGIC * Enable automated testing on PR
# MAGIC
# MAGIC ### 3. Monitor Coverage
# MAGIC * Target: 80%+ code coverage
# MAGIC * Review uncovered lines
# MAGIC * Add tests for edge cases
# MAGIC
# MAGIC ### 4. Documentation
# MAGIC * Update README with new tests
# MAGIC * Document fixtures and utilities
# MAGIC * Share best practices with team
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Useful Commands
# MAGIC
# MAGIC ```bash
# MAGIC # Run all tests
# MAGIC pytest
# MAGIC
# MAGIC # Run with verbose output
# MAGIC pytest -v
# MAGIC
# MAGIC # Run specific test
# MAGIC pytest unit/test_identifier_generation.py::TestLocalSystemIdentifierGeneration::test_gic_identifier_format
# MAGIC
# MAGIC # Run with coverage
# MAGIC pytest --cov=../ --cov-report=html
# MAGIC
# MAGIC # Run marked tests
# MAGIC pytest -m unit
# MAGIC pytest -m integration
# MAGIC ```
# MAGIC
# MAGIC ## Resources
# MAGIC
# MAGIC * [tests/README.md](./tests/README.md) - Full documentation
# MAGIC * [Pytest Documentation](https://docs.pytest.org/)
# MAGIC * [PySpark Testing](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html)

# COMMAND ----------


