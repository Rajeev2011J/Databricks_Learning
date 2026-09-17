#!/usr/bin/env bash
# =============================================================================
# run_tests.sh  —  Linux/bash test runner for Databricks Asset Bundles
#
# Equivalent of run_tests.ps1 but for the Linux ADO agent pool and developer
# machines running macOS or Linux.
#
# Agent pool: Shared-EU-Container-Linux-Python-S-* (used in DAB-GCOB-reporting.yml)
#
# Usage
# -----
#   # First-time setup (create venv + install deps)
#   ./run_tests.sh --install
#
#   # Full suite with coverage
#   ./run_tests.sh
#
#   # Unit tests only (fast, no Spark)
#   ./run_tests.sh --marker unit
#
#   # Single bundle
#   ./run_tests.sh --bundle radarv1
#
#   # DQ tests without coverage
#   ./run_tests.sh --marker dq --no-coverage
#
#   # Parallel unit tests
#   ./run_tests.sh --marker unit --parallel
#
#   # ADO CI mode (Nexus index, JUnit XML, no venv creation)
#   ./run_tests.sh --ci
#
# CI invocation (in Azure Pipelines yaml):
#   - script: |
#       chmod +x Databricks/run_tests.sh
#       cd Databricks
#       ./run_tests.sh --ci --marker "unit or spark" --no-coverage
#     displayName: 'Run pytest'
#     workingDirectory: $(System.DefaultWorkingDirectory)
#
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
MARKER=""
BUNDLE=""
NO_COVERAGE=false
PARALLEL=false
INSTALL=false
VERBOSE=false
CI_MODE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --marker)      MARKER="$2";    shift 2 ;;
        --bundle)      BUNDLE="$2";    shift 2 ;;
        --no-coverage) NO_COVERAGE=true; shift  ;;
        --parallel)    PARALLEL=true;  shift    ;;
        --install)     INSTALL=true;   shift    ;;
        --verbose)     VERBOSE=true;   shift    ;;
        --ci)          CI_MODE=true;   shift    ;;
        *)             echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR="$SCRIPT_DIR/.venv"
PYTHON_EXE="$VENV_DIR/bin/python"
PYTEST_EXE="$VENV_DIR/bin/pytest"

# On ADO agents Python is already on PATH; skip venv for CI unless --install
if [[ "$CI_MODE" == true && "$INSTALL" == false ]]; then
    PYTHON_EXE="python3"
    PYTEST_EXE="python3 -m pytest"
fi

echo ""
echo "============================================================"
echo "  Databricks Asset Bundles — Central Test Runner (bash)"
echo "  Runtime: $(python3 --version 2>/dev/null || echo 'python3 not found')"
echo "  Mode:    $([ "$CI_MODE" = true ] && echo 'CI (Azure DevOps)' || echo 'Local')"
echo "  Root:    $SCRIPT_DIR"
echo "============================================================"
echo ""

# ---------------------------------------------------------------------------
# Virtual environment + dependencies (local-dev / --install mode only)
# ---------------------------------------------------------------------------
if [[ "$CI_MODE" == false ]]; then
    if [[ "$INSTALL" == true || ! -d "$VENV_DIR" ]]; then
        echo "[Setup] Creating virtual environment at $VENV_DIR ..."
        python3 -m venv "$VENV_DIR"
    fi

    if [[ "$INSTALL" == true || ! -f "$PYTEST_EXE" ]]; then
        echo "[Setup] Installing dependencies from requirements-dev.txt ..."

        if [[ -n "${NEXUSCLOUD_PYPI_URL:-}" ]]; then
            # Internal Nexus registry (available when ADO variable group is loaded)
            "$PYTHON_EXE" -m pip install --upgrade pip --quiet
            "$PYTHON_EXE" -m pip install \
                --index-url "$NEXUSCLOUD_PYPI_URL" \
                --extra-index-url https://pypi.org/simple/ \
                -r requirements-dev.txt --quiet
        else
            "$PYTHON_EXE" -m pip install --upgrade pip --quiet
            "$PYTHON_EXE" -m pip install -r requirements-dev.txt --quiet
        fi

        echo "[Setup] Dependencies installed."
    fi
fi

# ---------------------------------------------------------------------------
# CI mode: install deps directly into the agent Python environment
# ---------------------------------------------------------------------------
if [[ "$CI_MODE" == true ]]; then
    echo "[CI] Installing test dependencies ..."

    PIP_FLAGS="--quiet --no-warn-script-location"

    if [[ -n "${NEXUSCLOUD_PYPI_URL:-}" ]]; then
        echo "[CI] Using internal Nexus PyPI registry"
        python3 -m pip install $PIP_FLAGS \
            --index-url "$NEXUSCLOUD_PYPI_URL" \
            --extra-index-url https://pypi.org/simple/ \
            -r requirements-dev.txt
    else
        echo "[CI] Using public PyPI"
        python3 -m pip install $PIP_FLAGS -r requirements-dev.txt
    fi

    echo "[CI] Dependencies installed."
    # Use system python/pytest
    PYTHON_EXE="python3"
    PYTEST_EXE="python3 -m pytest"
fi

echo "[Setup] Python: $($PYTHON_EXE --version 2>&1)"
echo ""

# ---------------------------------------------------------------------------
# Build pytest argument list
# ---------------------------------------------------------------------------
PYTEST_ARGS=()

# ---- test path / bundle selection ----
if [[ -n "$BUNDLE" ]]; then
    BUNDLE_TEST_PATH="$SCRIPT_DIR/$BUNDLE/tests"
    if [[ ! -d "$BUNDLE_TEST_PATH" ]]; then
        echo "ERROR: Bundle test path not found: $BUNDLE_TEST_PATH"
        exit 1
    fi
    echo "[Filter] Bundle: $BUNDLE  →  $BUNDLE_TEST_PATH"
    PYTEST_ARGS+=("$BUNDLE_TEST_PATH")
    PYTEST_ARGS+=("$SCRIPT_DIR/unit_tests")   # always include shared infrastructure
else
    echo "[Filter] Running ALL bundles"
    # testpaths from pyproject.toml handle discovery
fi

# ---- marker filter ----
if [[ -n "$MARKER" ]]; then
    echo "[Filter] Marker: -m \"$MARKER\""
    PYTEST_ARGS+=("-m" "$MARKER")
fi

# ---- JUnit XML (always emit in CI, optional locally) ----
if [[ "$CI_MODE" == true ]]; then
    RESULTS_DIR="${SYSTEM_DEFAULTWORKINGDIRECTORY:-$SCRIPT_DIR}/test-results"
    mkdir -p "$RESULTS_DIR"
    PYTEST_ARGS+=("--junit-xml=$RESULTS_DIR/pytest-results.xml")
    echo "[CI] JUnit XML → $RESULTS_DIR/pytest-results.xml"
fi

# ---- coverage ----
if [[ "$NO_COVERAGE" == false ]]; then
    PYTEST_ARGS+=("--cov")

    if [[ "$CI_MODE" == true ]]; then
        # ADO: emit Cobertura XML for PublishCodeCoverageResults task
        COV_DIR="${SYSTEM_DEFAULTWORKINGDIRECTORY:-$SCRIPT_DIR}/coverage"
        mkdir -p "$COV_DIR"
        PYTEST_ARGS+=("--cov-report=xml:$COV_DIR/coverage.xml")
        PYTEST_ARGS+=("--cov-report=term-missing")
        echo "[CI] Coverage XML → $COV_DIR/coverage.xml"
    else
        PYTEST_ARGS+=("--cov-report=term-missing")
        PYTEST_ARGS+=("--cov-report=html:htmlcov")
        echo "[Coverage] HTML report → $SCRIPT_DIR/htmlcov/index.html"
    fi
else
    echo "[Coverage] Skipped (--no-coverage)"
fi

# ---- parallelism ----
if [[ "$PARALLEL" == true ]]; then
    echo "[Parallel] Running with -n auto (pytest-xdist)"
    PYTEST_ARGS+=("-n" "auto")
fi

# ---- verbosity ----
if [[ "$VERBOSE" == true ]]; then
    PYTEST_ARGS+=("-v" "-s")
else
    PYTEST_ARGS+=("-v")
fi

# ---- always ----
PYTEST_ARGS+=("--tb=short" "--strict-markers")

echo ""
echo "[Run] $PYTEST_EXE ${PYTEST_ARGS[*]}"
echo ""

# ---------------------------------------------------------------------------
# Execute pytest
# ---------------------------------------------------------------------------
START_TIME=$(date +%s)

set +e  # don't exit on pytest failure — we want to capture exit code
$PYTEST_EXE "${PYTEST_ARGS[@]}"
EXIT_CODE=$?
set -e

END_TIME=$(date +%s)
DURATION=$(( END_TIME - START_TIME ))
MINUTES=$(( DURATION / 60 ))
SECONDS=$(( DURATION % 60 ))

echo ""
echo "------------------------------------------------------------"
printf "  Duration : %02d:%02d\n" "$MINUTES" "$SECONDS"

if [[ $EXIT_CODE -eq 0 ]]; then
    echo "  Result   : PASSED"
    if [[ "$NO_COVERAGE" == false && "$CI_MODE" == false ]]; then
        echo "  Coverage : $SCRIPT_DIR/htmlcov/index.html"
    fi
elif [[ $EXIT_CODE -eq 5 ]]; then
    echo "  Result   : NO TESTS COLLECTED (exit 5)"
    echo "  Hint     : Check --marker or --bundle filter."
    EXIT_CODE=0   # treat as non-failure in CI (no tests = not a test failure)
else
    echo "  Result   : FAILED (exit $EXIT_CODE)"
fi
echo "------------------------------------------------------------"
echo ""

exit $EXIT_CODE
