#!/bin/sh
set -e

# Ensure we run from the site root
cd /home/site/wwwroot

# Install from local wheels (no internet) into a dedicated, writable site-packages
SITE_PKGS="/home/site/python_packages"
WHEELS_DIR="/home/site/wwwroot/wheels"
REQ_FILE="/home/site/wwwroot/requirements.txt"

mkdir -p "$SITE_PKGS"

if [ -f "$REQ_FILE" ] && [ -d "$WHEELS_DIR" ]; then
  # DO NOT upgrade pip online; stay offline
  python3 -m pip install --no-index --find-links "$WHEELS_DIR" \
    --target "$SITE_PKGS" -r "$REQ_FILE"
else
  echo "[startup] ERROR: $REQ_FILE or $WHEELS_DIR missing"
  exit 1
fi

# Ensure Python prefers our vendored site-packages and ignores user/global
export PYTHONPATH="$SITE_PKGS:${PYTHONPATH:-}"
export PYTHONNOUSERSITE=1

# Resolve port
PORT="${PORT:-8000}"

# Launch Streamlit using python -m (avoids PATH issues)
exec python -m streamlit run /home/site/wwwroot/app.py --server.port="$PORT" --server.address=0.0.0.0