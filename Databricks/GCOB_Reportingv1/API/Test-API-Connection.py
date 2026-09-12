# Databricks notebook source
import os
import http
import json
import requests

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------



# COMMAND ----------

env = 'preprod'
if env == 'dev':
    # client-data-api
    scope = '6b80373a-f546-4e06-9c16-9a4515c87ec5/.default'
    base_url = 'ed648b7e-5222-42e7-9271-0dd834ed4c04-nonprd.t-az-eu.api.rabo.cloud'
    base_url_http = f'http://{base_url}'

if env == 'preprod':
    # client-data-api
    scope = 'dcd4adc7-52bc-402e-8060-3eafd2dc2c01/.default'
    base_url = '110699af-f1e5-40fa-b141-ce624346e269-preprd.az-eu.api.rabo.cloud'
    base_url_http = f'http://{base_url}'

# COMMAND ----------

# Authentication
headers = {
    "Content_Type": "application/x-www-form-urlencoded",
    "cache-control": "no-cache",
}
        # Token auth value 
values = {
    "grant_type": "client_credentials",
    "scope": f"{scope}",
# Use client ID of your DB instance (Step 2)
    "client_id": f"{app_reg_app_id}",
# Retreive secret from keyvault
    "client_secret": f"{service_credential}"
}

# Use http request to generate auth token
resp = requests.post(
    url="https://login.microsoftonline.com/6e93a626-8aca-4dc1-9191-ce291b4b75a1/oauth2/v2.0/token",
    headers=headers,
    data=values,
)

# prd = /api/GetUserRoles/CI4124929?limit=2500
# prd_scope = https://713011d5-f99d-45d9-9b6c-f2d7c0c194b9.az-eu.api.rabo.cloud
# 
print("Auth response: (should be 200)")
print(resp)

# COMMAND ----------

token = json.loads(resp.text)["access_token"]
header_info = {
    "Authorization": f"Bearer {token}"
}

# COMMAND ----------

# DBTITLE 1,Use token to request API return

relative_url = '/api/clients/490785'

connection = http.client.HTTPSConnection(base_url)
connection.request(method = "GET", url=relative_url, headers = header_info)
result = connection.getresponse()
result.read()

# COMMAND ----------

bytes_obj = result.read()

# COMMAND ----------

json_str = bytes_obj.decode('utf-8')

# Parse string to JSON
json_obj = json.loads(json_str)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


