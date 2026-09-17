"""Unit tests for radarv1 bundle configuration."""
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "utils"))
from bundle_validator import BundleValidator


@pytest.mark.unit
@pytest.mark.radarv1
class TestRadarv1Config:
    """Test suite for radarv1 bundle configuration."""
    
    @pytest.fixture
    def bundle_path(self, bundle_base_path):
        """Path to radarv1 bundle."""
        return bundle_base_path / "radarv1"
    
    @pytest.fixture
    def validator(self, bundle_path):
        """Bundle validator instance."""
        return BundleValidator(bundle_path)
    
    def test_wheel_file_variables_configured(self, validator):
        """Test wheel file variables are configured."""
        validator.load_config()
        variables = validator.config.get("variables", {})
        assert "wheel_files" in variables, "Missing wheel_files variable"
        assert "library_base_path" in variables, "Missing library_base_path variable"
        assert "wheel_path" in variables, "Missing wheel_path variable"
    
    def test_wheel_files_list_not_empty(self, validator):
        """Test wheel_files variable contains files."""
        validator.load_config()
        wheel_files = validator.config["variables"]["wheel_files"]["default"]
        assert isinstance(wheel_files, list), "wheel_files should be a list"
        assert len(wheel_files) > 0, "wheel_files list is empty"
    
    def test_library_paths_per_environment(self, validator):
        """Test library paths are configured for each environment."""
        validator.load_config()
        targets = validator.config.get("targets", {})
        
        for target_name in ["dev", "preprod", "prod"]:
            assert target_name in targets, f"Missing {target_name} target"
            target_vars = targets[target_name].get("variables", {})
            assert "library_base_path" in target_vars, \
                f"Missing library_base_path in {target_name}"
    
    def test_trigger_pause_status_variable(self, validator):
        """Test trigger pause status variable is configured."""
        validator.load_config()
        variables = validator.config.get("variables", {})
        assert "BUNDLE_VAR_trigger_pause_status" in variables, \
            "Missing BUNDLE_VAR_trigger_pause_status variable"
    
    def test_dev_is_paused_preprod_prod_unpaused(self, validator):
        """Test dev is PAUSED while preprod/prod are UNPAUSED."""
        validator.load_config()
        targets = validator.config["targets"]
        
        # Dev should be PAUSED
        dev_vars = targets["dev"].get("variables", {})
        assert dev_vars.get("BUNDLE_VAR_trigger_pause_status") == "PAUSED", \
            "Dev should be PAUSED"
        
        # Preprod and prod should be UNPAUSED
        for env in ["preprod", "prod"]:
            env_vars = targets[env].get("variables", {})
            assert env_vars.get("BUNDLE_VAR_trigger_pause_status") == "UNPAUSED", \
                f"{env} should be UNPAUSED"
    
    def test_unity_catalog_volumes_referenced(self, validator):
        """Test UC volumes are properly referenced in library paths."""
        validator.load_config()
        targets = validator.config["targets"]
        
        for target_name, target_config in targets.items():
            lib_path = target_config.get("variables", {}).get("library_base_path")
            if lib_path:
                assert lib_path.startswith("/Volumes/"), \
                    f"{target_name} library path should use UC Volumes: {lib_path}"
