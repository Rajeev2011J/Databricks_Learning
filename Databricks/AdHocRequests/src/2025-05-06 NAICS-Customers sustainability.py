# Databricks notebook source
# MAGIC %md
# MAGIC ### Context
# MAGIC Requested by: Mark Kuipers
# MAGIC
# MAGIC **For Sustainability**
# MAGIC is requesting on behalf of Sustainability purpose, to find - matching flexcube trades - the right sectors for these clients

# COMMAND ----------

import os
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

saradar_write_storage = f'saradar{environment}'
spark.conf.set("fs.azure.account.auth.type."+saradar_write_storage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+saradar_write_storage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+saradar_write_storage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+saradar_write_storage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+saradar_write_storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

## TODO: Clear Region / portfolio scope per party.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rdm.party_naics limit 10

# COMMAND ----------


