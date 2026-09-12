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

gcdsquery = f'''
  select
     superparentgcid AS rdr_superparentgcid
    , superparentsectorteam AS rdr_sector
    , superparentgcdsclientname AS rdr_superparentfulllegalname
    , superparentgcdsglobalcolocation as rdr_branch
    , superparentgcdsglobalconame as rdr_superparentgcdsglobalconame
    , superparentBusinessLineDescription as rdr_department
    , superparentprimaryNaicsGCDS as rdr_naics
  from radar.gcdsclientsindataverse
      where superparentgcdsglobalcolocation IN ('Beijing', 'Belgium', 'China', 'France', 'Germany', 'Hong Kong', 'India', 'Ireland', 'Italy', 'Kenya', 'London', 'Malaysia', 'New York', 'Shanghai', 'Singapore', 'SINGAPORE', 'Spain', 'Utrecht') 
'''

# COMMAND ----------

upsert_to_dataverse_table(dataverse_api_url, 'rdr_gcdsclients', gcdsquery, access_token, 'rdr_superparentgcid', 'rdr_gcdsclientid', 1000)
