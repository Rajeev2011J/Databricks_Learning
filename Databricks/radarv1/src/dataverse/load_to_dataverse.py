# Databricks notebook source
import os
import requests
from functions_dataverse import get_access_token, post_to_dataverse_table, truncate_dataverse_table, upsert_to_dataverse_table

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

# validate Dataverse API authorization and check output to get table name for next step
headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json; charset=utf-8",
}

response = requests.get(dataverse_api_url, headers=headers)

if response.status_code == 200:
    data = response.json()  # safely decode JSON only if the request was successful
    display(data)
else:
    print(f"Error: Received response with status code {response.status_code}")
    print(response.text)  # helps to debug the actual response content

# COMMAND ----------

query = f'''SELECT 
                id AS cr7f2_pocdataid
                ,city AS cr7f2_city
                ,country AS cr7f2_country          
            FROM hive_metastore.radar.pocdata'''

# COMMAND ----------

# example to update existing records and only insert new records
upsert_to_dataverse_table(dataverse_api_url, 'cr7f2_pocdatas', query, access_token, 'cr7f2_pocdataid', 'cr7f2_pocdataid', 100)

# COMMAND ----------

# example to truncate dataverse table (delete all records)
truncate_dataverse_table(dataverse_api_url, 'cr7f2_pocdatas', access_token, 'cr7f2_pocdataid', 100)

# COMMAND ----------

# example for inserting all records
post_to_dataverse_table(dataverse_api_url, 'cr7f2_pocdatas', query, access_token, 100)
