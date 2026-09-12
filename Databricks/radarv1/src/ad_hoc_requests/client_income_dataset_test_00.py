# Databricks notebook source
import os
from datetime import datetime, timedelta
import http.client
import json
import requests
import sys
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Local File Import
from RadarUtils import *

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

# COMMAND ----------

# DBTITLE 1,get token for calling Graph API
# # scope for which we are requesting a token
# scope = 'https://graph.microsoft.com/.default'

# # Authentication
# headers = {
#     "Content_Type": "application/x-www-form-urlencoded",
#     "cache-control": "no-cache",
# }
#         # Token auth value 
# values = {
#     "grant_type": "client_credentials",
#     "scope": f"{scope}",
#     "client_id": f"{app_reg_app_id}",
#     "client_secret": f"{service_credential}"
# }

# # Use http request to generate auth token
# resp = requests.post(
#     url="https://login.microsoftonline.com/6e93a626-8aca-4dc1-9191-ce291b4b75a1/oauth2/v2.0/token",
#     headers=headers,
#     data=values,
# )

# # call graph api
# token = json.loads(resp.text)["access_token"]
# header_info = {
#     "Authorization": f"Bearer {token}",
# }

# url = 'graph.microsoft.com'

# COMMAND ----------

# ==== Config ====
TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["APP_REG_APP_ID"]              # your Azure AD app registration (Application ID)
CLIENT_SECRET = f"{service_credential}"

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
HOSTNAME = "raboweb.sharepoint.com"
SITE_PATH = "/sites/ARCHIVE_PortfolioDataEA"
LIBRARY_DOCS_NAME = "Shared Documents"  # default library
FOLDER_RELATIVE_PATH = "PowerBI/Portfolio Insights Dashboards/Source Files"  # inside the library

# ==== Get token (client credentials) ====
token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
token_resp = requests.post(
    token_url,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
    },
)
token_resp.raise_for_status()
access_token = token_resp.json()["access_token"]
auth_header = {"Authorization": f"Bearer {access_token}"}

# ==== Resolve site ID ====
# GET /sites/{hostname}:/sites/{sitePath}
site_url = f"{GRAPH_BASE}/sites/{HOSTNAME}:/{SITE_PATH}"
site_resp = requests.get(site_url, headers=auth_header)
site_resp.raise_for_status()
site = site_resp.json()
site_id = site["id"]
print(f"Site ID: {site_id}")

# ==== Get default drive (document library) ====
# Most sites have one default drive for 'Shared Documents'
drive_url = f"{GRAPH_BASE}/sites/{site_id}/drive"
drive_resp = requests.get(drive_url, headers=auth_header)
drive_resp.raise_for_status()
drive = drive_resp.json()
drive_id = drive["id"]
print(f"Drive ID: {drive_id}")

# ==== List contents of the folder ====
# Path-based addressing: /drives/{driveId}/root:/Shared Documents/<path>:/children
folder_children_url = (
    f"{GRAPH_BASE}/drives/{drive_id}/root:/{LIBRARY_DOCS_NAME}/{FOLDER_RELATIVE_PATH}:/children"
)
children_resp = requests.get(folder_children_url, headers=auth_header)
if children_resp.status_code == 404:
    raise FileNotFoundError("Folder path not found. Check library and relative path.")
children_resp.raise_for_status()
items = children_resp.json().get("value", [])

print(f"Items in folder ({len(items)}):")
for item in items:
    print(f"- {item.get('name')} | type={item.get('file', 'folder')} | id={item.get('id')}")
