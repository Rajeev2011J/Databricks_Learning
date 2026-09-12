"""
PartyCatalog/tests/test_party_catalog_config.py
================================================
Tests for the PartyCatalog bundle.

PartyCatalog manages Unity Catalog metadata (schemas, grants, tables) via
Databricks Asset Bundle resource types.  There is no Python transformation
logic — the bundle's value is in its YAML definitions being correct and
consistent across environments.

This file validates:
  1. databricks.yml structure — bundle name, includes, variables, targets.
  2. variables.yml  — all expected variables declared with correct defaults.
  3. resources/schemas.yml — Unity Catalog schema definitions present and named.
  4. Cross-file consistency — catalog_name variable matches expected pattern.

Template for future Python tests
---------------------------------
When Python helpers are added to PartyCatalog/src/ (e.g. a schema provisioner
script), import them directly here:

    import my_provisioner

    def test_schema_name_validation():
        with pytest.raises(ValueError):
            my_provisioner.validate_schema_name("invalid name!")
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_BUNDLE_ROOT    = Path(__file__).parents[1]          # …/PartyCatalog/
_DATABRICKS_YML = _BUNDLE_ROOT / "databricks.yml"
_VARIABLES_YML  = _BUNDLE_ROOT / "variables.yml"
_RESOURCES_DIR  = _BUNDLE_ROOT / "resources"
_SCHEMAS_YML    = _RESOURCES_DIR / "schemas.yml"

# Expected variable names (declared in both databricks.yml and variables.yml)
_EXPECTED_VARIABLES = {
    "catalog_name",
    "radar_schema_grants",
    "catalog_owner",
    "environment",
    "workspacename",
    "workspaceurl",
}

# Expected schema keys in resources/schemas.yml
_EXPECTED_SCHEMAS = {"gcobreporting", "RDMv3"}

# Catalog name must follow the pattern: wr_fj_parties_and_risk_assessment_{env}
_CATALOG_NAME_PATTERN = re.compile(
    r"^wr_fj_parties_and_risk_assessment_(dev|preprd|prod)$"
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def bundle_yml() -> dict:
    assert _DATABRICKS_YML.exists(), f"databricks.yml not found at {_DATABRICKS_YML}"
    with _DATABRICKS_YML.open() as fh:
        return yaml.safe_load(fh)


@pytest.fixture(scope="module")
def variables_yml() -> dict:
    assert _VARIABLES_YML.exists(), f"variables.yml not found at {_VARIABLES_YML}"
    with _VARIABLES_YML.open() as fh:
        return yaml.safe_load(fh)


@pytest.fixture(scope="module")
def schemas_yml() -> dict:
    assert _SCHEMAS_YML.exists(), f"schemas.yml not found at {_SCHEMAS_YML}"
    with _SCHEMAS_YML.open() as fh:
        return yaml.safe_load(fh)


# ===========================================================================
# databricks.yml structural validation
# ===========================================================================

@pytest.mark.unit
@pytest.mark.party_catalog
class TestPartyCatalogBundleYAML:

    def test_bundle_name_is_party_catalog(self, bundle_yml: dict) -> None:
        assert bundle_yml.get("bundle", {}).get("name") == "PartyCatalog"

    def test_includes_variables_yml(self, bundle_yml: dict) -> None:
        includes = bundle_yml.get("include", [])
        assert "variables.yml" in includes, (
            f"databricks.yml must include variables.yml; got: {includes}"
        )

    def test_all_expected_variables_declared(self, bundle_yml: dict) -> None:
        declared = set(bundle_yml.get("variables", {}).keys())
        missing  = _EXPECTED_VARIABLES - declared
        assert not missing, f"Missing variable declarations: {sorted(missing)}"

    def test_catalog_name_default_matches_dev_pattern(self, bundle_yml: dict) -> None:
        default = (
            bundle_yml.get("variables", {})
                      .get("catalog_name", {})
                      .get("default", "")
        )
        assert _CATALOG_NAME_PATTERN.match(default), (
            f"catalog_name default '{default}' must match "
            f"wr_fj_parties_and_risk_assessment_{{env}}"
        )

    def test_catalog_name_default_is_dev(self, bundle_yml: dict) -> None:
        default = (
            bundle_yml.get("variables", {})
                      .get("catalog_name", {})
                      .get("default", "")
        )
        assert default.endswith("_dev"), (
            f"catalog_name default should point to _dev catalog, got: {default!r}"
        )

    def test_environment_variable_default_is_dev(self, bundle_yml: dict) -> None:
        default = (
            bundle_yml.get("variables", {})
                      .get("environment", {})
                      .get("default", "")
        )
        assert default == "dev", (
            f"environment default must be 'dev', got: {default!r}"
        )

    def test_dev_target_present(self, bundle_yml: dict) -> None:
        targets = bundle_yml.get("targets", {})
        assert "dev" in targets, "A dev target must be defined"

    def test_dev_is_default_target(self, bundle_yml: dict) -> None:
        dev = bundle_yml.get("targets", {}).get("dev", {})
        assert dev.get("default") is True, "dev target must be the default"

    def test_dev_workspace_host_is_valid_url(self, bundle_yml: dict) -> None:
        host = (
            bundle_yml.get("targets", {})
                      .get("dev", {})
                      .get("workspace", {})
                      .get("host", "")
        )
        assert host.startswith("https://"), (
            f"dev workspace.host must be an https:// URL, got: {host!r}"
        )


# ===========================================================================
# variables.yml validation
# ===========================================================================

@pytest.mark.unit
@pytest.mark.party_catalog
class TestPartyCatalogVariablesYML:

    def test_variables_yml_parses_to_dict(self, variables_yml: dict) -> None:
        assert isinstance(variables_yml, dict)

    def test_all_expected_variables_in_variables_yml(self, variables_yml: dict) -> None:
        declared = set(variables_yml.get("variables", {}).keys())
        missing  = _EXPECTED_VARIABLES - declared
        assert not missing, (
            f"variables.yml is missing: {sorted(missing)}"
        )

    @pytest.mark.parametrize("var_name", sorted(_EXPECTED_VARIABLES))
    def test_each_variable_has_description(
        self, var_name: str, variables_yml: dict
    ) -> None:
        var = variables_yml.get("variables", {}).get(var_name, {})
        assert var.get("description"), (
            f"Variable '{var_name}' must have a description in variables.yml"
        )

    @pytest.mark.parametrize("var_name", sorted(_EXPECTED_VARIABLES))
    def test_each_variable_has_type_string(
        self, var_name: str, variables_yml: dict
    ) -> None:
        var      = variables_yml.get("variables", {}).get(var_name, {})
        var_type = var.get("type")
        # type is optional but when set it must be 'string' (all PartyCatalog vars are strings)
        if var_type is not None:
            assert var_type == "string", (
                f"Variable '{var_name}' type should be 'string', got: {var_type!r}"
            )

    def test_workspaceurl_default_points_to_dev(self, variables_yml: dict) -> None:
        url = (
            variables_yml.get("variables", {})
                         .get("workspaceurl", {})
                         .get("default", "")
        )
        # Dev workspace URL should contain the dev ADB instance ID
        assert "adb-" in url, (
            f"workspaceurl default must contain an ADB workspace URL, got: {url!r}"
        )


# ===========================================================================
# resources/schemas.yml validation
# ===========================================================================

@pytest.mark.unit
@pytest.mark.party_catalog
class TestPartyCatalogSchemasYML:

    def test_schemas_yml_parses_to_dict(self, schemas_yml: dict) -> None:
        assert isinstance(schemas_yml, dict)

    def test_resources_schemas_block_exists(self, schemas_yml: dict) -> None:
        schemas = schemas_yml.get("resources", {}).get("schemas", {})
        assert schemas, "resources.schemas block must not be empty in schemas.yml"

    def test_all_expected_schemas_present(self, schemas_yml: dict) -> None:
        defined = set(
            schemas_yml.get("resources", {}).get("schemas", {}).keys()
        )
        missing = _EXPECTED_SCHEMAS - defined
        assert not missing, f"Missing schema definitions: {sorted(missing)}"

    @pytest.mark.parametrize("schema_key", sorted(_EXPECTED_SCHEMAS))
    def test_schema_has_catalog_name_variable_reference(
        self, schema_key: str, schemas_yml: dict
    ) -> None:
        catalog = (
            schemas_yml.get("resources", {})
                       .get("schemas", {})
                       .get(schema_key, {})
                       .get("catalog_name", "")
        )
        assert "${var.catalog_name}" in catalog, (
            f"Schema '{schema_key}' must reference catalog via ${{var.catalog_name}}, "
            f"got: {catalog!r}"
        )

    @pytest.mark.parametrize("schema_key", sorted(_EXPECTED_SCHEMAS))
    def test_schema_has_name_and_comment(
        self, schema_key: str, schemas_yml: dict
    ) -> None:
        schema = (
            schemas_yml.get("resources", {})
                       .get("schemas", {})
                       .get(schema_key, {})
        )
        assert schema.get("name"),    f"Schema '{schema_key}' must have a 'name' field"
        assert schema.get("comment"), f"Schema '{schema_key}' must have a 'comment' field"

    def test_gcob_reporting_schema_name_contains_version(self, schemas_yml: dict) -> None:
        """GCOBReporting schema name should encode a version number."""
        name = (
            schemas_yml.get("resources", {})
                       .get("schemas", {})
                       .get("gcobreporting", {})
                       .get("name", "")
        )
        assert re.search(r"\d+", name), (
            f"gcobreporting schema name should contain a version number, got: {name!r}"
        )

    def test_rdmv3_schema_name_is_rdmv3(self, schemas_yml: dict) -> None:
        name = (
            schemas_yml.get("resources", {})
                       .get("schemas", {})
                       .get("RDMv3", {})
                       .get("name", "")
        )
        assert "RDM" in name or "rdm" in name.lower(), (
            f"RDMv3 schema name should contain 'RDM', got: {name!r}"
        )
