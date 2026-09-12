# Databricks notebook source
pip install openpyxl

# COMMAND ----------

# Databricks single cell: Read Excel from SharePoint (Teams) directly into Spark DataFrame (no DBFS writes)
import os
# ====== 0) PARAMETERS — EDIT THESE TWO SECRETS ======
def Read_file_from_Sharepoint(FILE_REL_PATH):
    TENANT_ID  = os.environ["TENANT_ID"]
    CLIENT_ID  = os.environ["APP_REG_APP_ID"]
    CLIENT_SECRET = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{CLIENT_ID}")   # change scope/secret as needed

    # Derived from your link:
    HOSTNAME      = "raboweb.sharepoint.com"
    SITE_PATH     = "teams/KYCReporting"                 # because URL path uses /teams/KYCReporting
    DRIVE_NAME    = "Documents" 

    # Excel read options
    SHEET_NAME = None   # e.g., "Sheet1"; None = first sheet
    HEADER     = 0      # 0-based header row index; set to None if no header

   

    # ====== 2) IMPORTS ======
    import io
    import msal
    import requests
    import pandas as pd
    from urllib.parse import quote

    # ====== 3) AUTH (Client Credentials) ======
    AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
    SCOPE     = ["https://graph.microsoft.com/.default"]
    GRAPH_API = "https://graph.microsoft.com/v1.0"

    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
    )
    token = app.acquire_token_for_client(scopes=SCOPE)
    
    if "access_token" not in token:
        raise RuntimeError(f"Failed to acquire token. Details: {token}")
    headers = {"Authorization": f"Bearer {token['access_token']}"}
    
    # ====== 4) RESOLVE SITE -> DRIVE (Document Library) ======
    site_res = requests.get(f"{GRAPH_API}/sites/{HOSTNAME}:/{quote(SITE_PATH)}:", headers=headers)
    site_res.raise_for_status()
    site_id = site_res.json()["id"]
   
    drives_res = requests.get(f"{GRAPH_API}/sites/{site_id}/drives", headers=headers)
    drives_res.raise_for_status()
    drives = drives_res.json().get("value", [])
    drive_id = next((d["id"] for d in drives if d.get("name") == DRIVE_NAME), None)
    if not drive_id:
        available = [d.get("name") for d in drives]
        raise ValueError(f"Drive '{DRIVE_NAME}' not found. Available: {available}")
    
    # ====== 5) DOWNLOAD EXCEL BYTES (NO DBFS WRITE) ======
    download_url = f"{GRAPH_API}/drives/{drive_id}/root:/{quote(FILE_REL_PATH)}:/content"
    resp = requests.get(download_url, headers=headers)
    resp.raise_for_status()
    excel_bytes = resp.content

    # ====== 6) PARSE EXCEL (IN-MEMORY) ======
    excel_stream = io.BytesIO(excel_bytes)
    read_kwargs = {"engine": "openpyxl", "header": HEADER}
    if SHEET_NAME is not None:
        read_kwargs["sheet_name"] = SHEET_NAME

    pdf = pd.read_excel(excel_stream, **read_kwargs)

    # ====== 7) CONVERT TO SPARK DATAFRAME ======
    df = spark.createDataFrame(pdf)

    # Show result
    display(df)
    df.count()

# COMMAND ----------

Read_file_from_Sharepoint("General/ADgroups.xlsx") #ADGroupUserdeatils,ADgroups# "General/10. Data export and analysis requests/2026_03_02 IRAP RANZ/DNB-SECTOR-MAPPING.xlsx"
