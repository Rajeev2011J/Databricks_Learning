# Databricks notebook source
# MAGIC %md
# MAGIC # Buse - Carbon Foot Print (of collateral)

# COMMAND ----------

# DBTITLE 1,import
import os
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,connection
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

import re

# load and create temp views of all gdp_tables below
datasets_list = [
    'CFP_RE_std'
]

for item in datasets_list:
    # get the most recent version available in gdp
    path = f'abfss://edcas@{ReadStorage}.dfs.core.windows.net/{item}/snapshot/daily/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])


    # get the most recent file available in gdp
    path = f'abfss://edcas@{ReadStorage}.dfs.core.windows.net/{item}/snapshot/daily/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('ACT_DATE=')[1][:10] for file in files if 'ACT_DATE=' in file.path)

    spark.read.parquet(f'abfss://edcas@{ReadStorage}.dfs.core.windows.net/{item}/snapshot/daily/{version}/data/ACT_DATE={load_date}/*.parquet').createOrReplaceTempView(item)
