# Databricks notebook source
import os
import requests
from functions_dataverse import get_access_token, upsert_to_dataverse_table

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

dataverse_url = os.environ['DATAVERSE_URL']
dataverse_api_url = f"{dataverse_url}/api/data/v9.1/"

# COMMAND ----------

access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)

# COMMAND ----------

query = f'''
  SELECT
      UserEmail AS rdr_useremail
    , UserName AS rdr_username
    , DateFirstSeenInGCOB AS rdr_datefirstseeningcob
    , DateLastSeenInGCOB AS rdr_datelastseeningcob
  FROM radar.userlist
'''

# COMMAND ----------

upsert_to_dataverse_table(dataverse_api_url, 'rdr_users', query, access_token, 'rdr_useremail', 'rdr_userid', 1000)
