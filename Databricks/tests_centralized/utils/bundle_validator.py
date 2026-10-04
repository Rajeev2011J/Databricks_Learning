"""Utilities for validating Databricks Asset Bundle configurations."""
import yaml
import re
from pathlib import Path
from typing import Dict, Any, List, Tuple


class BundleValidator:
    """Validator for DAB configurations."""
    
    def __init__(self, bundle_path: Path):
        self.bundle_path = Path(bundle_path)
        self.config_path = self.bundle_path / "databricks.yml"
        self.config = None
        
    def load_config(self) -> Dict[str, Any]:
        """Load the bundle configuration."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        return self.config
    
    def validate_yaml_syntax(self) -> Tuple[bool, str]:
        """Validate YAML syntax."""
        try:
            self.load_config()
            return True, "Valid YAML syntax"
        except yaml.YAMLError as e:
            return False, f"YAML syntax error: {str(e)}"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def validate_bundle_structure(self) -> Dict[str, Any]:
        """Validate bundle has required structure."""
        result = {
            "valid": True,
            "missing_required": [],
            "missing_recommended": [],
            "errors": []
        }
        
        # Required files
        if not self.config_path.exists():
            result["valid"] = False
            result["missing_required"].append("databricks.yml")
        
        # Recommended directories
        recommended = ["resources", "src", "fixtures"]
        for dir_name in recommended:
            if not (self.bundle_path / dir_name).exists():
                result["missing_recommended"].append(dir_name)
        
        return result
    
    def validate_targets(self) -> Dict[str, List[str]]:
        """Validate target configurations."""
        if not self.config:
            self.load_config()
        
        issues = {
            "missing_workspace": [],
            "missing_run_as": [],
            "invalid_sp": [],
            "missing_mode": []
        }
        
        targets = self.config.get("targets", {})
        for target_name, target_config in targets.items():
            # Check workspace configuration
            if "workspace" not in target_config or "host" not in target_config["workspace"]:
                issues["missing_workspace"].append(target_name)
            
            # Check run_as configuration
            if "run_as" not in target_config:
                issues["missing_run_as"].append(target_name)
            elif "service_principal_name" in target_config["run_as"]:
                sp_name = target_config["run_as"]["service_principal_name"]
                if not self._is_valid_uuid(sp_name):
                    issues["invalid_sp"].append(f"{target_name}: {sp_name}")
            
            # Check mode
            if "mode" not in target_config:
                issues["missing_mode"].append(target_name)
        
        return issues
    
    def validate_resources(self) -> Dict[str, Any]:
        """Validate resource definitions."""
        if not self.config:
            self.load_config()
        
        result = {
            "jobs": [],
            "clusters": [],
            "pipelines": [],
            "total_resources": 0
        }
        
        # Check included resources
        if "include" in self.config:
            includes = self.config["include"]
            for include_path in includes:
                # Resolve wildcards
                if "*" in include_path:
                    pattern = include_path.replace("*", "")
                    base_dir = self.bundle_path / Path(pattern).parent
                    if base_dir.exists():
                        resource_files = list(base_dir.glob("*.yml"))
                        result["total_resources"] += len(resource_files)
        
        # Check inline resources
        if "resources" in self.config:
            resources = self.config["resources"]
            if "jobs" in resources:
                result["jobs"] = list(resources["jobs"].keys())
            if "clusters" in resources:
                result["clusters"] = list(resources["clusters"].keys())
            if "pipelines" in resources:
                result["pipelines"] = list(resources["pipelines"].keys())
        
        return result
    
    def validate_variables(self) -> Dict[str, List[str]]:
        """Validate variable definitions."""
        if not self.config:
            self.load_config()
        
        result = {
            "defined": [],
            "missing_defaults": [],
            "missing_descriptions": []
        }
        
        variables = self.config.get("variables", {})
        for var_name, var_config in variables.items():
            result["defined"].append(var_name)
            
            if isinstance(var_config, dict):
                if "default" not in var_config:
                    result["missing_defaults"].append(var_name)
                if "description" not in var_config:
                    result["missing_descriptions"].append(var_name)
        
        return result
    
    @staticmethod
    def _is_valid_uuid(uuid_str: str) -> bool:
        """Check if string is a valid UUID."""
        uuid_pattern = re.compile(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            re.IGNORECASE
        )
        return bool(uuid_pattern.match(uuid_str))
