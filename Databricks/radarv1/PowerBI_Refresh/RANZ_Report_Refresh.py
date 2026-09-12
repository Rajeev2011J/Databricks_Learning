# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To refresh AMLWLM Report from Databricks
# MAGIC
# MAGIC #### author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |21-Nov-2025 |14157771 |First release
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import Functions
import msal
import requests
import json
from datetime import datetime, timedelta
import time
import os
import gc
import pandas as pd
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Read values from cluster
ApplicationId = os.environ['APP_REG_APP_ID']
TanentId = os.environ['TENANT_ID']
environment=os.environ['ENV']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{ApplicationId}")

# COMMAND ----------

# DBTITLE 1,Assign values bases on environment
if environment == 'dev':
    pbi_workspace = 'e1d43f70-575a-4465-ae01-52dd3ee5a4ea'
    RANZ_dataset = 'd25cde68-090f-4741-a5dd-6569f2787837'
elif environment == 'preprd':
    pbi_workspace = '90e21b66-7a8c-405f-af48-c6301923e295'
    RANZ_dataset = '3efd6a1c-c529-4a59-9f15-02e8a3843e4f'
elif environment == 'prod':
    pbi_workspace = 'ccd3631b-bb55-4b3a-895c-381430a50264'
    RANZ_dataset = 'f93869ad-3150-40d3-911a-d718c55cfa56'

# COMMAND ----------

# DBTITLE 1,Execution to refresh PowerBi
client_id = ApplicationId
client_secret = service_credential
tenant_name = TanentId

workspace_id = pbi_workspace
DatasetId = [RANZ_dataset]

authority_url = "https://login.microsoftonline.com/" + tenant_name 
scope = ["https://analysis.windows.net/powerbi/api/.default"]
url = "https://api.powerbi.com/v1.0/myorg/"


def refresh_dataset(workspace_id, dataset_id, url, headers):
    relative_url = url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    response = requests.post(relative_url, headers=headers)
    return response.status_code,response

def get_refresh_status(workspace_id, dataset_id, url, headers, check_every_seconds=100):
    #top gets only the last refresh
    relative_url = url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
    refresh_status = "Refresh Started" 
    while refresh_status!="Completed" and refresh_status!="Failed":
        time.sleep(check_every_seconds)
        response = requests.get(relative_url, headers=headers) 
        last_refresh = response.json()["value"][0]
        refresh_status=last_refresh["status"]
        if refresh_status == 'Unknown':
            refresh_status = 'Refresh In Progress'
        print("status" + " - " + refresh_status)
    return last_refresh

#Use MSAL to grab token
app = msal.ConfidentialClientApplication(client_id, authority=authority_url, client_credential=client_secret)
result = app.acquire_token_for_client(scopes=scope)


for dataset_id in DatasetId:
    if dataset_id == RANZ_dataset:
        JobName = 'RADAR_RANZ_Job'

    if 'access_token' in result:
        access_token = result['access_token']
        headers = {'Content-Type':'application/json', 'Authorization':f'Bearer {access_token}'}
        refresh_status_code,refresh_response = refresh_dataset(workspace_id, dataset_id, url, headers)
    if refresh_status_code == 202:
        print(f"{JobName} Refresh Started")
        resp = get_refresh_status(workspace_id, dataset_id, url, headers)
        status = resp["status"]
        error = ""
        if status=="Failed":
            error_json = json.loads(resp["refreshAttempts"][0]["serviceExceptionJson"])
            error = error_json["errorDescription"].replace("'","")
            print(f"{JobName} refresh failed to complete with error - {error}")
        else:
            print(f"{JobName} refresh completed successfully")
    else: 
        status = "Failed"
        error = refresh_response.json()["error"]["message"]
        print(f"refresh failed to start with error - {error}")
