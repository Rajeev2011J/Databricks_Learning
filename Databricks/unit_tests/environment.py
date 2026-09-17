"""
Databricks/unit_tests/environment.py
================================
Single source of truth for runtime environment detection AND repo root
path resolution.

The same pytest suite runs in three distinct environments:

  1. LOCAL-DEV  — developer laptop (Windows / Mac / Linux).
                  No cluster, no CI vars. Uses local[2] Spark.
                  Repo root = directory containing pyproject.toml.

  2. ADO-CI     — Azure DevOps Linux agent.
                  No cluster. TF_BUILD / BUILD_BUILDID are set.
                  Uses local[2] Spark.
                  Repo root = System.DefaultWorkingDirectory checkout.

  3. DATABRICKS — Inside a Databricks cluster: notebook cell, %run, or
                  Workflow task that calls the test runner notebook.
                  DATABRICKS_RUNTIME_VERSION is set automatically.
                  Uses the already-running cluster SparkSession directly
                  (SparkSession.getOrCreate() — NOT DatabricksSession).
                  Repo root resolved from the notebook's __file__ path
                  OR from DATABRICKS_REPO_ROOT env var if set.

Public API
----------
Flags (bool):
    IS_DATABRICKS       — True on a live cluster
    IS_ADO_CI           — True on an Azure DevOps agent
    IS_LOCAL_DEV        — True on a developer laptop
    NEEDS_RUNTIME_STUBS — True when Databricks SDK packages must be faked
    HAS_REAL_SPARK      — True on cluster (SparkSession already running)
    HAS_REAL_DBUTILS    — True on cluster (real dbutils available)
    HAS_UNITY_CATALOG   — True on cluster (Unity Catalog accessible)

Strings:
    RUNTIME             — "databricks" | "ado-ci" | "local-dev"
    REPO_ROOT           — absolute path to the repo root (contains pyproject.toml)
    DATABRICKS_DIR      — REPO_ROOT / "Databricks"
    TESTS_DIR           — DATABRICKS_DIR / "unit_tests"

Helper:
    describe()          — returns a multi-line diagnostic string
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# 1. Runtime detection
# ---------------------------------------------------------------------------

# Every Databricks cluster injects this variable automatically.
# Format: "14.3.x-scala2.12"  (never present on ADO or laptops)
_DATABRICKS_RUNTIME: str | None = os.environ.get("DATABRICKS_RUNTIME_VERSION")

# Azure Pipelines sets TF_BUILD=True on every agent job.
# BUILD_BUILDID is a reliable fallback (always present in ADO).
_ADO_TF_BUILD: str | None = os.environ.get("TF_BUILD")
_ADO_BUILD_ID: str | None = os.environ.get("BUILD_BUILDID")

IS_DATABRICKS: bool = _DATABRICKS_RUNTIME is not None
IS_ADO_CI:     bool = (not IS_DATABRICKS) and (
    _ADO_TF_BUILD is not None or _ADO_BUILD_ID is not None
)
IS_LOCAL_DEV:  bool = not IS_DATABRICKS and not IS_ADO_CI

RUNTIME: str = (
    "databricks" if IS_DATABRICKS
    else "ado-ci"    if IS_ADO_CI
    else "local-dev"
)

# ---------------------------------------------------------------------------
# 2. Capability flags
# ---------------------------------------------------------------------------

NEEDS_RUNTIME_STUBS: bool = not IS_DATABRICKS
"""
True on local-dev and ado-ci: inject MagicMocks into sys.modules so that
``from databricks.sdk.runtime import spark, dbutils`` in source files
resolves without ImportError.
False on a real cluster: packages are natively present — never override them.
"""

HAS_REAL_SPARK:     bool = IS_DATABRICKS
HAS_REAL_DBUTILS:   bool = IS_DATABRICKS
HAS_UNITY_CATALOG:  bool = IS_DATABRICKS

# ---------------------------------------------------------------------------
# 3. Repo root resolution
#
# Strategy (tried in order):
#
# a) Explicit override via environment variable DATABRICKS_REPO_ROOT.
#    Set this in the cluster env vars or notebook init if the automatic
#    detection produces a wrong result.
#
# b) Walk UP from this file (__file__) until we find pyproject.toml.
#    Works for all three environments:
#      LOCAL-DEV  : __file__ = .../Databricks/unit_tests/environment.py
#                   walks up two levels → .../Databricks/ → finds pyproject.toml ✓
#      ADO-CI     : same path structure after git checkout ✓
#      DATABRICKS : __file__ = /Workspace/.../Databricks/unit_tests/environment.py
#                              OR /Volumes/.../Databricks/unit_tests/environment.py
#                   walks up two levels → finds pyproject.toml ✓
#
# c) Fallback: use the directory two levels above __file__ even if
#    pyproject.toml is absent (silently continues; repo root may be wrong).
# ---------------------------------------------------------------------------

def _find_repo_root() -> Path:
    # (a) Explicit override
    explicit = os.environ.get("DATABRICKS_REPO_ROOT")
    if explicit:
        candidate = Path(explicit).resolve()
        if candidate.is_dir():
            return candidate

    # (b) Walk up from this file looking for pyproject.toml
    here = Path(__file__).resolve()
    # environment.py lives at: <repo_root>/Databricks/unit_tests/environment.py
    # so we need to go up exactly 2 levels to reach <repo_root>/Databricks/
    # and then 1 more to reach <repo_root> where pyproject.toml may live,
    # OR pyproject.toml may be inside Databricks/ — check both.
    for ancestor in [here.parent, here.parent.parent, here.parent.parent.parent]:
        if (ancestor / "pyproject.toml").exists():
            return ancestor

    # (c) Fallback — two levels up from this file = Databricks/ directory
    return here.parent.parent


_REPO_ROOT_RAW: Path = _find_repo_root()

# pyproject.toml lives inside Databricks/, so REPO_ROOT = Databricks/
# DATABRICKS_DIR is an alias that makes intent clearer in callers.
REPO_ROOT:       Path = _REPO_ROOT_RAW
"""
Absolute path to the directory that contains pyproject.toml.
This is Databricks/ — the root from which pytest must be invoked.
All testpaths and pythonpath entries in pyproject.toml are relative to this.
"""

DATABRICKS_DIR: Path = REPO_ROOT
"""Alias for REPO_ROOT — the Databricks/ bundle collection root."""

TESTS_DIR: Path = REPO_ROOT / "unit_tests"
"""Absolute path to Databricks/unit_tests/ (conftest, fixtures, utils, environment)."""

# ---------------------------------------------------------------------------
# 4. Ensure REPO_ROOT and TESTS_DIR are on sys.path
#    This is especially important on Databricks where the notebook's working
#    directory is /databricks/driver — not the repo root.
#    Adding here guarantees `import environment`, `from utils.df_helpers import`
#    etc. all resolve correctly regardless of cwd.
# ---------------------------------------------------------------------------

for _p in [str(REPO_ROOT), str(TESTS_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# 5. Debug helper
# ---------------------------------------------------------------------------

def describe() -> str:
    """Return a multi-line diagnostic string. Call from a notebook cell to verify detection."""
    return (
        f"Runtime          : {RUNTIME}\n"
        f"IS_DATABRICKS    : {IS_DATABRICKS}"
        + (f"  (DBR {_DATABRICKS_RUNTIME})" if _DATABRICKS_RUNTIME else "") + "\n"
        f"IS_ADO_CI        : {IS_ADO_CI}"
        + (f"  (BUILD_BUILDID={_ADO_BUILD_ID})" if _ADO_BUILD_ID else "") + "\n"
        f"IS_LOCAL_DEV     : {IS_LOCAL_DEV}\n"
        f"NEEDS_STUBS      : {NEEDS_RUNTIME_STUBS}\n"
        f"HAS_REAL_SPARK   : {HAS_REAL_SPARK}\n"
        f"HAS_REAL_DBUTILS : {HAS_REAL_DBUTILS}\n"
        f"REPO_ROOT        : {REPO_ROOT}\n"
        f"TESTS_DIR        : {TESTS_DIR}\n"
        f"pyproject.toml   : {'FOUND' if (REPO_ROOT / 'pyproject.toml').exists() else 'NOT FOUND'}\n"
    )


if __name__ == "__main__":
    print(describe())
