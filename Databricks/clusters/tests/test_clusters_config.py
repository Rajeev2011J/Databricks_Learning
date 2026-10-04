"""
clusters/tests/test_clusters_config.py
========================================
Tests for the clusters bundle.

The clusters bundle is pure infrastructure-as-code — it defines six shared
Spark clusters (DAB-small/medium/large + Radar-small/medium/large-UC) via
``resources/clusters_job.yml``.  There is no Python transformation logic to
unit-test, so this file:

  1. Validates the YAML structure of ``clusters_job.yml`` programmatically,
     ensuring all expected cluster keys, required fields, and environment
     variable placeholders are present.
  2. Provides a template / example showing the pattern for adding real tests
     once the clusters bundle grows Python helper scripts.

To add a real test when a src/ Python file is created:
  - Add the file under clusters/src/
  - Import it here (pythonpath includes clusters/src/ via pyproject.toml)
  - Write a test following the same pattern as test_functions_databricks.py
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

# ---------------------------------------------------------------------------
# Cluster-path tests are local_only: they read clusters_job.yml from the
# local filesystem via Path(__file__).  On a Databricks cluster the working
# directory is /databricks/driver and the YAML files are not present there.
# Run with:  pytest -m "not local_only"  to skip on cluster.
# ---------------------------------------------------------------------------
from environment import IS_DATABRICKS

pytestmark = [
    pytest.mark.local_only,
    pytest.mark.skipif(IS_DATABRICKS, reason="YAML file tests require local filesystem"),
]

# ---------------------------------------------------------------------------
# Path to the resource file under test
# ---------------------------------------------------------------------------
_BUNDLE_ROOT   = Path(__file__).parents[1]          # …/clusters/
_RESOURCES_DIR = _BUNDLE_ROOT / "resources"
_CLUSTERS_YAML = _RESOURCES_DIR / "clusters_job.yml"

# Expected cluster names defined in the YAML
_EXPECTED_CLUSTERS = {
    "dab_small_cluster",
    "dab_medium_cluster",
    "dab_large_cluster",
    "Radar_small_cluster",
    "Radar_medium_cluster",
    "Radar_large_cluster",
}

# Every cluster must carry these top-level fields
_REQUIRED_CLUSTER_FIELDS = {
    "cluster_name",
    "spark_version",
    "node_type_id",
    "autotermination_minutes",
    "spark_env_vars",
    "runtime_engine",
}

# Every cluster's spark_env_vars must declare these keys
_REQUIRED_ENV_VARS = {
    "APP_REG_APP_ID",
    "TENANT_ID",
    "ENV",
    "GDP_STORAGE_NAME",
    "CATALOG",
    "GCOB_UC_SCHEMA",
    "PYSPARK_PYTHON",
}

# UC clusters (Radar_*) must use USER_ISOLATION; legacy DAB clusters must use NONE
_UC_CLUSTERS     = {"Radar_small_cluster", "Radar_medium_cluster", "Radar_large_cluster"}
_LEGACY_CLUSTERS = {"dab_small_cluster", "dab_medium_cluster", "dab_large_cluster"}

# Expected deployment targets
_EXPECTED_TARGETS = {"dev", "preprod", "prod"}


@pytest.fixture(scope="module")
def clusters_yaml() -> dict:
    """Load and parse clusters_job.yml once for the entire module."""
    assert _CLUSTERS_YAML.exists(), (
        f"clusters_job.yml not found at {_CLUSTERS_YAML}. "
        "Run tests from the Databricks/ root directory."
    )
    with _CLUSTERS_YAML.open() as fh:
        return yaml.safe_load(fh)


@pytest.fixture(scope="module")
def cluster_definitions(clusters_yaml: dict) -> dict:
    """Return the clusters sub-dict from the YAML resources block."""
    return clusters_yaml.get("resources", {}).get("clusters", {})


# ===========================================================================
# Structural completeness
# ===========================================================================

@pytest.mark.unit
@pytest.mark.clusters
class TestClusterYAMLStructure:

    def test_yaml_loads_without_error(self, clusters_yaml: dict) -> None:
        assert isinstance(clusters_yaml, dict), "YAML root must be a dict"

    def test_resources_clusters_key_exists(self, cluster_definitions: dict) -> None:
        assert cluster_definitions, "resources.clusters block must not be empty"

    def test_all_expected_clusters_present(self, cluster_definitions: dict) -> None:
        actual   = set(cluster_definitions.keys())
        missing  = _EXPECTED_CLUSTERS - actual
        extra    = actual - _EXPECTED_CLUSTERS
        assert not missing, f"Missing cluster definitions: {sorted(missing)}"
        # Extra clusters are allowed (forward-compatible)
        _ = extra  # informational only

    @pytest.mark.parametrize("cluster_name", sorted(_EXPECTED_CLUSTERS))
    def test_required_fields_present(
        self, cluster_name: str, cluster_definitions: dict
    ) -> None:
        cluster = cluster_definitions.get(cluster_name, {})
        missing = _REQUIRED_CLUSTER_FIELDS - set(cluster.keys())
        assert not missing, (
            f"Cluster '{cluster_name}' missing required fields: {sorted(missing)}"
        )

    @pytest.mark.parametrize("cluster_name", sorted(_EXPECTED_CLUSTERS))
    def test_required_env_vars_declared(
        self, cluster_name: str, cluster_definitions: dict
    ) -> None:
        env_vars = cluster_definitions.get(cluster_name, {}).get("spark_env_vars", {})
        missing  = _REQUIRED_ENV_VARS - set(env_vars.keys())
        assert not missing, (
            f"Cluster '{cluster_name}' missing env vars: {sorted(missing)}"
        )

    def test_targets_block_present(self, clusters_yaml: dict) -> None:
        targets = clusters_yaml.get("targets", {})
        assert targets, "targets block must be present"

    def test_all_expected_targets_present(self, clusters_yaml: dict) -> None:
        targets = set(clusters_yaml.get("targets", {}).keys())
        missing = _EXPECTED_TARGETS - targets
        assert not missing, f"Missing deployment targets: {sorted(missing)}"


# ===========================================================================
# Security mode validation
# ===========================================================================

@pytest.mark.unit
@pytest.mark.clusters
class TestClusterSecurityMode:

    @pytest.mark.parametrize("cluster_name", sorted(_UC_CLUSTERS))
    def test_uc_clusters_use_user_isolation(
        self, cluster_name: str, cluster_definitions: dict
    ) -> None:
        mode = cluster_definitions.get(cluster_name, {}).get("data_security_mode")
        assert mode == "USER_ISOLATION", (
            f"UC cluster '{cluster_name}' must use USER_ISOLATION, got '{mode}'"
        )

    @pytest.mark.parametrize("cluster_name", sorted(_LEGACY_CLUSTERS))
    def test_legacy_clusters_use_none_security_mode(
        self, cluster_name: str, cluster_definitions: dict
    ) -> None:
        mode = cluster_definitions.get(cluster_name, {}).get("data_security_mode")
        assert mode == "NONE", (
            f"Legacy cluster '{cluster_name}' must use NONE, got '{mode}'"
        )


# ===========================================================================
# Spark version validation
# ===========================================================================

@pytest.mark.unit
@pytest.mark.clusters
class TestClusterSparkVersions:

    @pytest.mark.parametrize("cluster_name", sorted(_LEGACY_CLUSTERS))
    def test_legacy_clusters_on_lts_runtime(
        self, cluster_name: str, cluster_definitions: dict
    ) -> None:
        version = cluster_definitions.get(cluster_name, {}).get("spark_version", "")
        # Legacy clusters must use 14.3.x LTS
        assert version.startswith("14.3"), (
            f"Legacy cluster '{cluster_name}' should use 14.3.x, got '{version}'"
        )

    @pytest.mark.parametrize("cluster_name", sorted(_UC_CLUSTERS))
    def test_uc_clusters_on_lts_runtime(
        self, cluster_name: str, cluster_definitions: dict
    ) -> None:
        version = cluster_definitions.get(cluster_name, {}).get("spark_version", "")
        # UC clusters must use 17.3.x LTS
        assert version.startswith("17.3"), (
            f"UC cluster '{cluster_name}' should use 17.3.x, got '{version}'"
        )

    @pytest.mark.parametrize("cluster_name", sorted(_EXPECTED_CLUSTERS))
    def test_all_clusters_use_photon(
        self, cluster_name: str, cluster_definitions: dict
    ) -> None:
        engine = cluster_definitions.get(cluster_name, {}).get("runtime_engine")
        assert engine == "PHOTON", (
            f"Cluster '{cluster_name}' should use PHOTON runtime, got '{engine}'"
        )


# ===========================================================================
# Autotermination guard
# ===========================================================================

@pytest.mark.unit
@pytest.mark.clusters
class TestAutoTermination:

    @pytest.mark.parametrize("cluster_name", sorted(_EXPECTED_CLUSTERS))
    def test_autotermination_set_and_reasonable(
        self, cluster_name: str, cluster_definitions: dict
    ) -> None:
        minutes = cluster_definitions.get(cluster_name, {}).get("autotermination_minutes")
        assert minutes is not None, (
            f"Cluster '{cluster_name}' must have autotermination_minutes set"
        )
        assert 1 <= minutes <= 120, (
            f"Cluster '{cluster_name}' autotermination_minutes={minutes} is outside "
            f"the expected range 1–120"
        )


# ===========================================================================
# Per-target permission checks
# ===========================================================================

@pytest.mark.unit
@pytest.mark.clusters
class TestTargetPermissions:

    def test_dev_clusters_grant_can_manage_to_admin_group(
        self, clusters_yaml: dict
    ) -> None:
        dev_clusters = (
            clusters_yaml.get("targets", {})
                         .get("dev", {})
                         .get("resources", {})
                         .get("clusters", {})
        )
        assert dev_clusters, "dev target must override cluster permissions"

        for cluster_name, overrides in dev_clusters.items():
            perms = overrides.get("permissions", [])
            admin_perm = next(
                (p for p in perms
                 if "Admin" in p.get("group_name", "") and p.get("level") == "CAN_MANAGE"),
                None,
            )
            assert admin_perm is not None, (
                f"Dev cluster '{cluster_name}' must grant CAN_MANAGE to the Admin group"
            )

    def test_prod_clusters_restrict_to_admin_only(
        self, clusters_yaml: dict
    ) -> None:
        prod_clusters = (
            clusters_yaml.get("targets", {})
                         .get("prod", {})
                         .get("resources", {})
                         .get("clusters", {})
        )
        assert prod_clusters, "prod target must override cluster permissions"

        for cluster_name, overrides in prod_clusters.items():
            perms        = overrides.get("permissions", [])
            group_names  = [p.get("group_name", "") for p in perms]
            non_admin    = [g for g in group_names if "Admin" not in g]
            assert not non_admin, (
                f"Prod cluster '{cluster_name}' should only grant permissions to "
                f"the Admin group; found: {non_admin}"
            )
