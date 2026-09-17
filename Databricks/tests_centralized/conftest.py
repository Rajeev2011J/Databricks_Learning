"""Shared pytest fixtures for DAB testing."""
import os
import yaml
import pytest
from pathlib import Path
from databricks.sdk import WorkspaceClient

@pytest.fixture(scope="session")
def workspace_client():
    """Create Databricks workspace client."""
    try:
        return WorkspaceClient()
    except Exception as e:
        pytest.skip(f"Could not create workspace client: {e}")

@pytest.fixture(scope="session")
def bundle_base_path():
    """Base path where all bundles are located."""
    return Path(__file__).parent.parent

@pytest.fixture
def bundle_names():
    """List of all bundle names."""
    return ["AdHocRequests", "PartyCatalog", "radarv1", "clusters", "GCOB_Consumer", "GCOB_Reportingv1"]

@pytest.fixture
def bundle_config_loader():
    """Load bundle configuration from databricks.yml."""
    def load_config(bundle_name: str, base_path: Path = None):
        if base_path is None:
            base_path = Path(__file__).parent.parent
        config_path = base_path / bundle_name / "databricks.yml"
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    return load_config

@pytest.fixture
def validate_yaml_syntax():
    """Validate YAML file syntax."""
    def validate(file_path: Path):
        try:
            with open(file_path, 'r') as f:
                yaml.safe_load(f)
            return True, "Valid YAML"
        except yaml.YAMLError as e:
            return False, str(e)
    return validate
