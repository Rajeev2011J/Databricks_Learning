# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To refresh QC Report from Databricks
# MAGIC
# MAGIC #### author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |20-Nov-2025 |14157771 |First release
# MAGIC | Abhishek Jaiswal	 |13-Feb-2026 |14814367 |Global File Refresh
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
    MI_dataset = 'deda583c-190b-4143-95f9-054dec1598f1'
    AML_WLM_dataset = '57744263-e346-4230-bf16-18f1da21b353'
    QC_dataset = '13c944bf-5341-447b-be5e-a71d998786ce'
    SanctionedCountry_dataset = '79d46ff6-ea55-45b5-b140-9acd1e329118'
    GlobalFile_dataset = 'ab17ad7d-6211-487b-a7e5-0c22bd86687b'
elif environment == 'preprd':
    pbi_workspace = '90e21b66-7a8c-405f-af48-c6301923e295'
    MI_dataset = '543a2c9d-7f60-4919-a89b-b08e8394defb'
    AML_WLM_dataset = 'd80005a8-8295-4c79-88d2-6772fb8f74bd'
    QC_dataset = 'f458ff4c-32ef-435f-b451-0a970c72b34a'
    SanctionedCountry_dataset = '13fa30c7-4db1-4694-aefa-024e8f20198f'
    GlobalFile_dataset = 'ba671b3b-cc2b-4ccf-8cb8-7e46835d5ee9'
elif environment == 'prod':
    pbi_workspace = 'ccd3631b-bb55-4b3a-895c-381430a50264'
    MI_dataset = 'ffcb5ae2-aa74-4609-ba5a-e917a3b0ad0c'
    AML_WLM_dataset = 'ec4011bf-de7f-4d53-988a-46759fec1c96'
    QC_dataset = '15a95354-79cd-426b-a957-7b0638629858'
    SanctionedCountry_dataset = '281b027e-f7e4-4597-a9de-3fa76ac4515d'
    GlobalFile_dataset = '89f3744c-ac4b-4c86-8fbf-95e79722ad60'

# COMMAND ----------

# DBTITLE 1,Execution to refresh PowerBi
client_id = ApplicationId
client_secret = service_credential
tenant_name = TanentId

workspace_id = pbi_workspace
DatasetId = [QC_dataset , GlobalFile_dataset]

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
    if dataset_id == MI_dataset:
        JobName = 'GCOB_MI_Job'
    elif dataset_id == AML_WLM_dataset:
        JobName = 'AML_WLM_Job'
    elif dataset_id == QC_dataset:
        JobName = 'Quality_Control_Job'
    elif dataset_id == SanctionedCountry_dataset:
        JobName = 'SanctionedCountry_Job'
    elif dataset_id == GlobalFile_dataset:
        JobName = 'FlobalFile_Job'

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
