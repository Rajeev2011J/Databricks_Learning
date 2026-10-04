#!/bin/bash
# filepath: /workspace/clusters_library.sh
# Install wheel files from Unity Catalog Volume at cluster startup

# Determine environment and set volume path
if [ "$ENV" == "preprod" ]; then
  VOLUME_PATH="/dbfs/Volumes/wr_fj_parties_and_risk_assessment_preprd/radar/libraries"
elif [ "$ENV" == "prod" ]; then
  VOLUME_PATH="/dbfs/Volumes/wr_fj_parties_and_risk_assessment_prod/radar/libraries"
else
  # Default to dev
  VOLUME_PATH="/dbfs/Volumes/wr_fj_parties_and_risk_assessment_dev/radar/libraries"
fi

echo "========================================"
echo "Installing packages from UC Volume"
echo "Environment: $ENV"
echo "Volume path: $VOLUME_PATH"
echo "========================================"

# Install packages using --no-index (no PyPI/Nexus access) and --find-links (UC Volume only)
/databricks/python/bin/pip install --no-index \
  --find-links "$VOLUME_PATH" \
  azure-cosmos azure-identity azure-storage-blob msal openpyxl

# Check if installation succeeded
if [ $? -eq 0 ]; then
  echo "SUCCESS: All packages installed from UC Volume"
  echo "Installed: azure-cosmos, azure-identity, azure-storage-blob, msal, openpyxl (+ dependencies)"
else
  echo "FAILED: Package installation failed"
  echo "Check that all wheel files (including dependencies) exist in: $VOLUME_PATH"
  exit 1
fi
