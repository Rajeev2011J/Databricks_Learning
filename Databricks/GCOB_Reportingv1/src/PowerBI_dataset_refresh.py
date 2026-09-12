# Databricks notebook source
# MAGIC %md
# MAGIC #Refreshes a PowerBI dataset
# MAGIC ###Parameters:
# MAGIC * pbi_workspace - full name of the PowerBI workspace
# MAGIC * pbi_dataset - full name of the PowerBI dataset

# COMMAND ----------

dbutils.widgets.text("pbi_workspace","")
dbutils.widgets.text("pbi_dataset","")

# COMMAND ----------

pbi_workspace =  dbutils.widgets.get("pbi_workspace")
pbi_dataset =  dbutils.widgets.get("pbi_dataset")

# COMMAND ----------

display(pbi_workspace)
display(pbi_dataset)

# COMMAND ----------

from datetime import datetime, timedelta
import time
import json
import requests
import os
import gc
import pandas as pd
from time import sleep
from pyspark.sql.functions import *

# COMMAND ----------

ApplicationId = os.environ['APP_REG_APP_ID']
TanentId = os.environ['TENANT_ID']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{ApplicationId}")

# COMMAND ----------

display(ApplicationId)
display(TanentId)
display(service_credential)

# COMMAND ----------

# Service Principal Information
client_id = ApplicationId
client_secret = service_credential
tenant_id = TanentId
base_url = f"https://api.powerbi.com/v1.0/myorg/"

# COMMAND ----------

# Function to get Access Token using App ID and Client Secret
def get_accessToken(client_id, client_secret, tenant_id):
    # Set the Token URL for Azure AD Endpoint
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
 
    # Data Request for Endpoint
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": "https://analysis.windows.net/powerbi/api",
    }
 
    # Send POS request to obtain access token
    response = requests.post(token_url, data=data)
 
    if response.status_code == 200:
        token_data = response.json()
        return token_data.get("access_token")
    else:
        response.raise_for_status()

# COMMAND ----------

# Function to get workspace ID 
def get_pbiWorkspaceId(workspace_name, base_url, headers):
    relative_url = base_url + "groups"
     
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
     
    if response.status_code == 200:
        data = response.json()
        for workspace in data["value"]:
            if workspace["name"] == workspace_name:
                return workspace["id"]
        return None

# COMMAND ----------

#Function to get Dataset ID 
def get_pbiDatasetId(workspace_id, base_url, headers, dataset_name):
    relative_url = base_url + f"groups/{workspace_id}/datasets"
 
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
     
    if response.status_code == 200:
        dataset_id = []
        data = response.json()
        for dataset in data["value"]:
            if dataset["name"] == dataset_name and dataset["isRefreshable"] == True:
                dataset_id = dataset["id"]
    return dataset_id

# COMMAND ----------

# Function to Refresh PBI Dataset
def invoke_pbiRefreshDataset(workspace_id, dataset_id, base_url, headers):
    relative_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    response = requests.post(relative_url, headers=headers)
 
    if response.status_code == 202:
        print(f"Dataset {pbi_dataset} refresh has been triggered successfully.")
    else:
        print(f"Failed to trigger dataset {pbi_dataset} refresh.")
    return response.status_code,response

# COMMAND ----------

def get_pbiRefreshStatus(workspace_id, dataset_id, base_url, headers, check_every_seconds=30):
    #top gets only the last refresh
    relative_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
 
    refresh_status = "started" 
    while refresh_status!="Completed" and refresh_status!="Failed": #check every x seconds if refresh completed
        response = requests.get(relative_url, headers=headers) #check status
        last_refresh = response.json()["value"][0]
        refresh_status=last_refresh["status"]
        print("status" + " - " + refresh_status+" "+str(datetime.now()))
        sleep(check_every_seconds)
    return last_refresh #return the last refresh json

# COMMAND ----------

access_token = get_accessToken(client_id, client_secret, tenant_id)
headers = {"Authorization": f"Bearer {access_token}"}

#print(access_token)
#print(headers)
 
# Get Workspace ID
workspace_id = get_pbiWorkspaceId(pbi_workspace, base_url,headers)
print(workspace_id)
#print(pbi_workspace)
#print(base_url)

# Get Dataset ID
dataset_id = get_pbiDatasetId(workspace_id, base_url, headers, pbi_dataset)
import time
import json
from datetime import datetime

try:
    # Invoke Refresh
    refresh_status_code, refresh_response = invoke_pbiRefreshDataset(workspace_id, dataset_id, base_url, headers)

    if refresh_status_code != 202:  # failed to start
        start_time = datetime.now()
        end_time = datetime.now()
        status = "Failed"
        try:
            error = refresh_response.json().get("error", {}).get("message", "Unknown error")
        except Exception as e:
            error = f"Error parsing response: {str(e)}"
        print(f"Refresh failed to start with error - {error}")

    else:
        time.sleep(30)  # sometimes status does not return correctly right after refresh start

        try:
            # Get Refresh Status
            resp = get_pbiRefreshStatus(workspace_id, dataset_id, base_url, headers)

            # write response json values to variables
            start_time = resp.get("startTime", "")
            end_time = resp.get("endTime", "")
            status = resp.get("status", "Unknown")
            error = ""

            if status == "Failed":
                try:
                    error_json = json.loads(resp["refreshAttempts"][0].get("serviceExceptionJson", "{}"))
                    error = error_json.get("errorDescription", "Unknown error").replace("'", "")
                except Exception as e:
                    error = f"Error parsing failure details: {str(e)}"
                print(f"Refresh failed to complete with error - {error}")
            else:
                print("Refresh completed successfully")

        except Exception as e:
            print(f"Error while checking refresh status: {str(e)}")

except Exception as e:
    print(f"Unexpected error occurred: {str(e)}")
