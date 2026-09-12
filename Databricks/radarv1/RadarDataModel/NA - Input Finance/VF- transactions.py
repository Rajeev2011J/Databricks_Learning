# Databricks notebook source
import os
import pyspark.sql.functions as f

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------



# COMMAND ----------

ReadStorage = 'edlcorestdamprod0001'
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

#Derive the date for which data has to be processes
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
Yesterdate = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
Today = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' +Yesterdate+ '*'
print (load_dts)
Load = 'EDL_LOAD_DTS='+ datetime.today().strftime('%Y%m%d')
#print(Load)
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
EDL_LoadDate = (datetime.today()).strftime("%Y-%m-%dT%H:%m:%mZ")

# COMMAND ----------

# https://edlcorestdamprod0001.dfs.core.windows.net/afsvision/ExtObligorDDAInstructions/3/data/EDL_LOAD_DTS=2024-12-18_20241219T080239Z



# COMMAND ----------

# MAGIC %md
# MAGIC ### AFSvision
# MAGIC - https://rabobank.collibra.com/asset/018e0b6a-5750-7374-b5bf-9102f6d9bd46

# COMMAND ----------

spark.read.parquet(f'abfss://afsvision@edlcorestdamprod0001.dfs.core.windows.net//ExtObligorDDAInstructions/3/data/EDL_LOAD_DTS=2024-12-18_20241219T080239Z/').createOrReplaceTempView('ObligorDDAInstructions')

# COMMAND ----------

# DBTITLE 1,DDA Instructions
# MAGIC %sql 
# MAGIC select * from ObligorDDAInstructions limit 5

# COMMAND ----------

spark.read.parquet(f'abfss://afsvision@edlcorestdamprod0001.dfs.core.windows.net//ExtObligorDDAInstructions/3/data/EDL_LOAD_DTS=2024-12-18_20241219T080239Z/').createOrReplaceTempView('ExtObligorDDAInstructions')

spark.read.parquet(f'abfss://afsvision@edlcorestdamprod0001.dfs.core.windows.net//ExtObligorWireInstructions/3/data/EDL_LOAD_DTS=2024-12-18_*/').createOrReplaceTempView('ExtObligorWireInstructions')

# COMMAND ----------

# DBTITLE 1,Wire transfer instructions
# MAGIC %sql
# MAGIC select * from ExtObligorWireInstructions limit 10

# COMMAND ----------

### NLS VF

# COMMAND ----------

# MAGIC %md
# MAGIC ### NLS VF
# MAGIC - 

# COMMAND ----------

# https://rabobank.collibra.com/asset/c45567e3-2443-42cd-851c-a70c42c407b3
# 
spark.read.parquet(f'abfss://nlsvf@edlcorestdamprod0001.dfs.core.windows.net/nls_dbo_loanacct/2/data/EDL_LOAD_DTS=2024-12-16_*/').createOrReplaceTempView('nls_dbo_loanacct')
# 


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nls_dbo_loanacct limit 10

# COMMAND ----------


