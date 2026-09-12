# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal:
# MAGIC Read data from the global file excel in Sharepointpage
# MAGIC
# MAGIC ### Logic
# MAGIC 0. Imports and install libraries.
# MAGIC 1. create functions
# MAGIC 2. call sharepoint / graph api top copy over excel
# MAGIC 3. read different tabs from the excel
# MAGIC 4. 
# MAGIC
# MAGIC ### Changelog
# MAGIC - [sprint 191?] Mahalakshmi created sample to read sharepoint through 
# MAGIC - [sprint 193] Ruud uses to connect Global File excel
# MAGIC - [sprint 19x?] Integrate into Global File Report

# COMMAND ----------

# DBTITLE 1,This needs to improve
pip install msal --index-url 'https://user:pw@repo.nexuscloud.aws.rabo.cloud/repository/pypi-org/simple'

# COMMAND ----------

# DBTITLE 1,This needs to improve
pip install openpyxl --index-url 'https://user:pw@repo.nexuscloud.aws.rabo.cloud/repository/pypi-org/simple'

# COMMAND ----------


import os
import openpyxl
import io
import msal
import requests
import pandas as pd
from urllib.parse import quote

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Link from Miglena
# MAGIC https://raboweb.sharepoint.com/teams/GlobalFileTopics/Shared%20Documents/Forms/AllItems.aspx?FolderCTID=0x01200098FB3A1B8D43974DAE75716CCF9200CC&id=%2fteams%2fGlobalFileTopics%2fShared+Documents%2fGlobal+Files+Power+App&xsdata=MDV8MDJ8UnV1ZC52YW4uTGFhckByYWJvYmFuay5jb218ODBkZjdhM2E5YjA1NDEwMzI5NDQwOGRlOTk1ODAyZWR8NmU5M2E2MjY4YWNhNGRjMTkxOTFjZTI5MWI0Yjc1YTF8MHwwfDYzOTExNjgwMDQzOTg5NjA2N3xVbmtub3dufFRXRnBiR1pzYjNkOGV5SkZiWEIwZVUxaGNHa2lPblJ5ZFdVc0lsWWlPaUl3TGpBdU1EQXdNQ0lzSWxBaU9pSlhhVzR6TWlJc0lrRk9Jam9pVFdGcGJDSXNJbGRVSWpveWZRPT18MHx8fA%3d%3d&sdata=ZGluckpRNmZuUW02QXF1QnorK0QwdkZ0dXdOdzRDRDA3K0NWTDN6dEE3dz0%3d

# COMMAND ----------

# DBTITLE 1,set the variables
SITE_PATH = 'teams/GlobalFileTopics'
DRIVE_NAME = 'Documents'
FOLDER_NAME = 'Global Files Power App'
FILE_NAME = 'FEC_CDD_Tracker_v6.xlsx'
FILE_REL_PATH = 'Global Files Power App/FEC_CDD_Tracker_v6.xlsx'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create function to read sharepoint data with app registration via graph API

# COMMAND ----------

# DBTITLE 1,1. create functions
# Databricks single cell: Read Excel from SharePoint (Teams) directly into Spark DataFrame (no DBFS writes)

# ====== 0) PARAMETERS — EDIT THESE TWO SECRETS ======
def Read_file_from_Sharepoint(FILE_REL_PATH, SITE_PATH, DRIVE_NAME):
    TENANT_ID  = os.environ["TENANT_ID"]
    CLIENT_ID  = os.environ["APP_REG_APP_ID"]
    CLIENT_SECRET = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{CLIENT_ID}")   # change scope/secret as needed

    # Derived from your link:
    HOSTNAME      = "raboweb.sharepoint.com"
    #SITE_PATH     = "teams/KYCReporting"                 # because URL path uses /teams/KYCReporting
    #DRIVE_NAME    = "Documents" 

    # Excel read options
    SHEET_NAME = None   # e.g., "Sheet1"; None = first sheet
    HEADER     = 10      # 0-based header row index; set to None if no header


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

    print(download_url)
    #print(resp.content)

    # ====== 6) PARSE EXCEL (IN-MEMORY) ======
    excel_stream = io.BytesIO(excel_bytes)
    #read_kwargs = {"engine": "openpyxl", "header": HEADER}
    #if SHEET_NAME is not None:
    #    read_kwargs["sheet_name"] = SHEET_NAME

    workbook = openpyxl.load_workbook(excel_stream, data_only=True)
    pdf = pd.read_excel(excel_stream, engine = "openpyxl" , header= 10, sheet_name = '1. MAIN SCREEN' )



    # ====== 7) CONVERT TO SPARK DATAFRAME ======
    #df = spark.createDataFrame(pdf)
    
    # Show result
    #display(df)
    #df.count()
    return workbook

# COMMAND ----------

# DBTITLE 1,list sheetnames
sheet_names = [ 
               '1. MAIN SCREEN'
               ,'CASES_DATA'
               ,'LL_DATA'
               ,'IL_DATA'
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Run the function to read Excel files

# COMMAND ----------

# DBTITLE 1,3. run the function
wb = Read_file_from_Sharepoint(FILE_REL_PATH = FILE_REL_PATH, SITE_PATH = SITE_PATH, DRIVE_NAME = DRIVE_NAME)

# COMMAND ----------

wb.active

# COMMAND ----------

wb.worksheets

# COMMAND ----------

for ws in wb.worksheets:
    if ws.tables:  # Check if the sheet has any tables
        print(f"Worksheet: {ws.title}")
        for table_name, table_obj in ws.tables.items():
            print(f"  Table Name: {table_name}")
            print(f"  Table Range: {table_obj}")
    else:
        print(f"Worksheet: {ws.title} has no tables.")

# COMMAND ----------

for ws in wb.worksheets:
    sheet = wb[ws.title]
    print(f"Displaying data from sheet: {sheet.title}\n")
        
    # Iterate over rows and print values
    for row in sheet.iter_rows(values_only=True):
        print(row)  # Each row is a tuple of cell values

    print('end of notebook')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Store the output of the function in Unity Catalog
# MAGIC - For engineer to do

# COMMAND ----------


