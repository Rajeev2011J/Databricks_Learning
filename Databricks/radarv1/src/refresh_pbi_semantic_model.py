# Databricks notebook source
import os
from dataverse.functions_dataverse import get_access_token
from functions_powerbi import refresh_pbi_semantic_model

# COMMAND ----------

# Task parameter dataset_id

dataset_id = dbutils.widgets.get("dataset_id")

if not dataset_id:
    raise ValueError(
        "Missing required task parameter 'dataset_id'")

# COMMAND ----------

environment = os.environ['ENV']

app_reg_app_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

powerbi_url = "https://analysis.windows.net/powerbi/api"

# COMMAND ----------

access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, powerbi_url)

# COMMAND ----------

group_id = "ccd3631b-bb55-4b3a-895c-381430a50264" # W&R Radar Prod

if environment == "prod":
    refresh_pbi_semantic_model(access_token, group_id, dataset_id)
else:
    print("Not in prod")
