#!/bin/bash

NEXUSCLOUD_DOMAIN="nexuscloud.aws.rabo.cloud"
NEXUSCLOUD_GROUP_REPO="pypi-org"



# Construct the PyPI URL
# NEXUS_CLOUD_USERNAME and NEXUS_CLOUD_PASSWORD are set in the Spark env variables of the cluster
NEXUSCLOUD_PYPI_URL="https://${NEXUSCLOUD_USERNAME}:${NEXUSCLOUD_PASSWORD}@repo.${NEXUSCLOUD_DOMAIN}/repository/${NEXUSCLOUD_GROUP_REPO}/simple" #gitleaks:allow


# Configure pip to use the internal PyPI source
cat <<EOF > /etc/pip.conf
[global]
index-url = ${NEXUSCLOUD_PYPI_URL}
extra-index-url = ${NEXUSCLOUD_PYPI_URL}
trusted-host = $(echo ${NEXUSCLOUD_PYPI_URL} | awk -F/ '{print $3}')
EOF

