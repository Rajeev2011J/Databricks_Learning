"""Unit tests for AdHocRequests bundle configuration."""
import pytest
from pathlib import Path
import sys

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "utils"))
from bundle_validator import BundleValidator


@pytest.mark.unit
@pytest.mark.adhoc_requests
@pytest.mark.validation
class TestAdHocRequestsConfig:
    """Test suite for AdHocRequests bundle configuration."""
    
    @pytest.fixture
    def bundle_path(self, bundle_base_path):
        """Path to AdHocRequests bundle."""
        return bundle_base_path / "AdHocRequests"
    
    @pytest.fixture
    def validator(self, bundle_path):
        """Bundle validator instance."""
        return BundleValidator(bundle_path)
    
    def test_yaml_syntax_valid(self, validator):
        """Test databricks.yml has valid YAML syntax."""
        is_valid, message = validator.validate_yaml_syntax()
        assert is_valid, f"Invalid YAML syntax: {message}"
    
    def test_bundle_structure_valid(self, validator):
        """Test bundle has required structure."""
        result = validator.validate_bundle_structure()
        assert result["valid"], f"Missing required files: {result['missing_required']}"
    
    def test_targets_configured(self, validator):
        """Test bundle has required targets."""
        validator.load_config()
        targets = validator.config.get("targets", {})
        assert "dev" in targets, "Missing dev target"
        assert "preprod" in targets, "Missing preprod target"
        assert "prod" in targets, "Missing prod target"
    
    def test_service_principals_configured(self, validator):
        """Test service principals are properly configured."""
        issues = validator.validate_targets()
        assert len(issues["missing_run_as"]) == 0, f"Missing run_as config: {issues['missing_run_as']}"
        assert len(issues["invalid_sp"]) == 0, f"Invalid service principal: {issues['invalid_sp']}"
    
    def test_workspace_hosts_configured(self, validator):
        """Test workspace hosts are configured for all targets."""
        issues = validator.validate_targets()
        assert len(issues["missing_workspace"]) == 0, f"Missing workspace config: {issues['missing_workspace']}"
    
    def test_resources_included(self, validator):
        """Test bundle includes resource definitions."""
        validator.load_config()
        assert "include" in validator.config, "No resource includes found"
        assert "resources/*.yml" in validator.config["include"], "Missing resources/*.yml include"
    
    def test_production_mode_set(self, validator):
        """Test production mode is properly set."""
        validator.load_config()
        targets = validator.config.get("targets", {})
        for target_name in ["dev", "preprod", "prod"]:
            mode = targets[target_name].get("mode")
            assert mode is not None, f"{target_name} target missing mode"
    
    def test_permissions_configured_for_prod(self, validator):
        """Test permissions are configured for production."""
        validator.load_config()
        prod_config = validator.config["targets"]["prod"]
        assert "permissions" in prod_config, "Missing permissions in prod target"
        permissions = prod_config["permissions"]
        assert any(p.get("group_name") == "users" for p in permissions), "Missing users group permission"


@pytest.mark.unit
@pytest.mark.adhoc_requests
class TestAdHocRequestsResources:
    """Test suite for AdHocRequests resource files."""
    
    @pytest.fixture
    def resources_path(self, bundle_base_path):
        """Path to AdHocRequests resources directory."""
        return bundle_base_path / "AdHocRequests" / "resources"
    
    def test_resources_directory_exists(self, resources_path):
        """Test resources directory exists."""
        assert resources_path.exists(), "Resources directory not found"
    
    def test_resource_files_have_valid_yaml(self, resources_path, validate_yaml_syntax):
        """Test all resource YAML files have valid syntax."""
        if not resources_path.exists():
            pytest.skip("Resources directory not found")
        
        yaml_files = list(resources_path.glob("*.yml"))
        assert len(yaml_files) > 0, "No resource files found"
        
        for yaml_file in yaml_files:
            is_valid, message = validate_yaml_syntax(yaml_file)
            assert is_valid, f"Invalid YAML in {yaml_file.name}: {message}"
