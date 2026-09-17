"""Common tests that run against all bundles."""
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "utils"))
from bundle_validator import BundleValidator


@pytest.mark.unit
@pytest.mark.validation
@pytest.mark.parametrize("bundle_name", [
    "AdHocRequests",
    "PartyCatalog", 
    "radarv1",
    "clusters",
    "GCOB_Consumer",
    "GCOB_Reportingv1"
])
class TestAllBundlesCommon:
    """Common validation tests for all bundles."""
    
    def test_databricks_yml_exists(self, bundle_base_path, bundle_name):
        """Test databricks.yml file exists."""
        config_path = bundle_base_path / bundle_name / "databricks.yml"
        assert config_path.exists(), f"databricks.yml not found in {bundle_name}"
    
    def test_yaml_syntax_valid(self, bundle_base_path, bundle_name):
        """Test databricks.yml has valid YAML syntax."""
        validator = BundleValidator(bundle_base_path / bundle_name)
        is_valid, message = validator.validate_yaml_syntax()
        assert is_valid, f"Invalid YAML in {bundle_name}: {message}"
    
    def test_bundle_name_configured(self, bundle_base_path, bundle_name):
        """Test bundle.name is properly configured."""
        validator = BundleValidator(bundle_base_path / bundle_name)
        validator.load_config()
        assert "bundle" in validator.config, f"Missing bundle section in {bundle_name}"
        assert "name" in validator.config["bundle"], f"Missing bundle.name in {bundle_name}"
        assert validator.config["bundle"]["name"], f"Empty bundle.name in {bundle_name}"
    
    def test_at_least_one_target(self, bundle_base_path, bundle_name):
        """Test bundle has at least one target configured."""
        validator = BundleValidator(bundle_base_path / bundle_name)
        validator.load_config()
        targets = validator.config.get("targets", {})
        assert len(targets) > 0, f"No targets configured in {bundle_name}"
    
    def test_default_target_set(self, bundle_base_path, bundle_name):
        """Test bundle has a default target."""
        validator = BundleValidator(bundle_base_path / bundle_name)
        validator.load_config()
        targets = validator.config.get("targets", {})
        has_default = any(t.get("default") for t in targets.values())
        assert has_default, f"No default target set in {bundle_name}"
    
    def test_workspace_hosts_valid_urls(self, bundle_base_path, bundle_name):
        """Test all workspace hosts are valid URLs."""
        validator = BundleValidator(bundle_base_path / bundle_name)
        validator.load_config()
        targets = validator.config.get("targets", {})
        
        for target_name, target_config in targets.items():
            if "workspace" in target_config and "host" in target_config["workspace"]:
                host = target_config["workspace"]["host"]
                assert host.startswith("https://"), \
                    f"Invalid host URL in {bundle_name}.{target_name}: {host}"
    
    def test_mode_is_valid(self, bundle_base_path, bundle_name):
        """Test target mode is a valid value."""
        validator = BundleValidator(bundle_base_path / bundle_name)
        validator.load_config()
        targets = validator.config.get("targets", {})
        valid_modes = ["development", "production"]
        
        for target_name, target_config in targets.items():
            if "mode" in target_config:
                mode = target_config["mode"]
                assert mode in valid_modes, \
                    f"Invalid mode in {bundle_name}.{target_name}: {mode}"


@pytest.mark.unit
@pytest.mark.resources
@pytest.mark.parametrize("bundle_name", [
    "AdHocRequests",
    "radarv1",
    "clusters",
    "GCOB_Consumer",
    "GCOB_Reportingv1"
])
def test_resources_directory_exists(bundle_base_path, bundle_name):
    """Test bundles with resource includes have a resources directory."""
    bundle_path = bundle_base_path / bundle_name
    validator = BundleValidator(bundle_path)
    validator.load_config()
    
    # Check if bundle includes resources
    includes = validator.config.get("include", [])
    if any("resources" in inc for inc in includes):
        resources_dir = bundle_path / "resources"
        assert resources_dir.exists(), f"Resources directory missing in {bundle_name}"
