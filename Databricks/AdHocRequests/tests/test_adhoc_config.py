"""
AdHocRequests/tests/test_adhoc_config.py
==========================================
Tests for the AdHocRequests bundle.

The AdHocRequests bundle is a scaffold for one-off analytical notebooks.
Its ``src/`` directory contains 50+ date-stamped Python notebooks that are
not importable as modules (they use top-level dbutils / spark calls).

This file therefore focuses on:
  1. Bundle YAML structural validation — ensuring the databricks.yml and
     resource files are well-formed and follow team conventions.
  2. Notebook naming convention checks — all src/ files should follow the
     ``YYYY-MM-DD <description>.py`` naming pattern.
  3. Template for adding real tests as the bundle gains reusable src/ modules.

Adding real tests
-----------------
When a reusable helper is extracted to ``AdHocRequests/src/some_helper.py``:

    import some_helper

    def test_helper_function():
        result = some_helper.my_function(...)
        assert result == expected
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_BUNDLE_ROOT   = Path(__file__).parents[1]          # …/AdHocRequests/
_DATABRICKS_YML = _BUNDLE_ROOT / "databricks.yml"
_RESOURCES_DIR  = _BUNDLE_ROOT / "resources"
_SRC_DIR        = _BUNDLE_ROOT / "src"

_EXPECTED_TARGETS   = {"dev", "preprod", "prod"}
_DATE_PREFIX_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}[\s_]")


@pytest.fixture(scope="module")
def bundle_yml() -> dict:
    assert _DATABRICKS_YML.exists(), f"databricks.yml not found at {_DATABRICKS_YML}"
    with _DATABRICKS_YML.open() as fh:
        return yaml.safe_load(fh)


# ===========================================================================
# databricks.yml structural validation
# ===========================================================================

@pytest.mark.unit
@pytest.mark.adhoc
class TestAdHocBundleYAML:

    def test_bundle_name_is_adhoc_requests(self, bundle_yml: dict) -> None:
        assert bundle_yml.get("bundle", {}).get("name") == "AdHocRequests"

    def test_include_points_to_resources(self, bundle_yml: dict) -> None:
        includes = bundle_yml.get("include", [])
        assert any("resources" in inc for inc in includes), (
            f"bundle must include resources/*.yml, got: {includes}"
        )

    def test_all_expected_targets_present(self, bundle_yml: dict) -> None:
        targets = set(bundle_yml.get("targets", {}).keys())
        missing = _EXPECTED_TARGETS - targets
        assert not missing, f"Missing targets: {sorted(missing)}"

    @pytest.mark.parametrize("target", sorted(_EXPECTED_TARGETS))
    def test_each_target_has_workspace_host(self, target: str, bundle_yml: dict) -> None:
        host = (
            bundle_yml.get("targets", {})
                      .get(target, {})
                      .get("workspace", {})
                      .get("host")
        )
        assert host and host.startswith("https://"), (
            f"Target '{target}' must have a valid https:// workspace host, got: {host!r}"
        )

    @pytest.mark.parametrize("target", sorted(_EXPECTED_TARGETS))
    def test_each_target_has_run_as(self, target: str, bundle_yml: dict) -> None:
        run_as = (
            bundle_yml.get("targets", {})
                      .get(target, {})
                      .get("run_as", {})
        )
        assert run_as.get("service_principal_name"), (
            f"Target '{target}' must specify run_as.service_principal_name"
        )

    def test_dev_is_default_target(self, bundle_yml: dict) -> None:
        dev = bundle_yml.get("targets", {}).get("dev", {})
        assert dev.get("default") is True, "dev must be the default target"

    @pytest.mark.parametrize("target", ["preprod", "prod"])
    def test_prod_preprod_use_production_mode(
        self, target: str, bundle_yml: dict
    ) -> None:
        mode = bundle_yml.get("targets", {}).get(target, {}).get("mode")
        assert mode == "production", (
            f"Target '{target}' must use mode: production, got: {mode!r}"
        )

    def test_prod_grants_can_view_to_users(self, bundle_yml: dict) -> None:
        perms = (
            bundle_yml.get("targets", {})
                      .get("prod", {})
                      .get("permissions", [])
        )
        assert any(
            p.get("group_name") == "users" and p.get("level") == "CAN_VIEW"
            for p in perms
        ), "prod target must grant CAN_VIEW to 'users' group"


# ===========================================================================
# Resource file validation
# ===========================================================================

@pytest.mark.unit
@pytest.mark.adhoc
class TestAdHocResourceFiles:

    def test_resources_directory_exists(self) -> None:
        assert _RESOURCES_DIR.exists(), f"resources/ directory not found at {_RESOURCES_DIR}"

    def test_at_least_one_resource_yml_exists(self) -> None:
        yml_files = list(_RESOURCES_DIR.glob("*.yml"))
        assert yml_files, "resources/ must contain at least one .yml file"

    @pytest.mark.parametrize(
        "yml_file",
        [str(f.name) for f in Path(_RESOURCES_DIR).glob("*.yml")]
        if _RESOURCES_DIR.exists() else [],
    )
    def test_each_resource_yml_parses_without_error(self, yml_file: str) -> None:
        path = _RESOURCES_DIR / yml_file
        with path.open() as fh:
            data = yaml.safe_load(fh)
        assert isinstance(data, dict), f"{yml_file} must parse to a dict"


# ===========================================================================
# Notebook naming convention
# ===========================================================================

@pytest.mark.unit
@pytest.mark.adhoc
class TestNotebookNamingConvention:

    def test_src_directory_exists(self) -> None:
        assert _SRC_DIR.exists(), f"src/ directory not found at {_SRC_DIR}"

    def test_majority_of_notebooks_follow_date_prefix_convention(self) -> None:
        """
        At least 80% of .py files in src/ should start with a YYYY-MM-DD prefix
        (or YYYY-MM-DD_ variant).  Allows a few legacy files without failing CI.
        """
        py_files = [
            f for f in _SRC_DIR.rglob("*.py")
            if f.name != "__init__.py"
        ]
        if not py_files:
            pytest.skip("No .py files found in src/")

        conforming = [f for f in py_files if _DATE_PREFIX_PATTERN.match(f.name)]
        ratio      = len(conforming) / len(py_files)

        assert ratio >= 0.80, (
            f"Only {len(conforming)}/{len(py_files)} notebooks follow the "
            f"YYYY-MM-DD naming convention ({ratio:.0%}). "
            f"Non-conforming files: "
            + ", ".join(f.name for f in py_files if f not in conforming)
        )
