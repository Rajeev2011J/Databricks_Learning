# Databricks notebook source
# MAGIC %md
# MAGIC # FATCA CRS Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## IRS GIIN download

# COMMAND ----------

import requests

# URL of the file to be downloaded
url = "https://apps.irs.gov/app/fatcaFfiList/data/FFIListFull.csv"

# Download the file
response = requests.get(url)
response.raise_for_status()

# Save the content to DBFS using dbutils
dbfs_path = "dbfs:/FileStore/FATCA_FFI_List.csv"
with open("/tmp/FATCA_FFI_List.csv", "wb") as f:
    f.write(response.content)

# Move the file from local tmp to DBFS
dbutils.fs.mv("file:/tmp/FATCA_FFI_List.csv", dbfs_path)

# Read the file and create a temp view
spark.read.option("header", "true").csv(dbfs_path).createOrReplaceTempView("FATCA_FFI_List")

# COMMAND ----------

spark.sql('SELECT * FROM FATCA_FFI_List').write.mode('overwrite').saveAsTable('radar.FATCA_FFI_List')

# COMMAND ----------

# delete file from folder, so a new one can be saved again
dbutils.fs.rm("dbfs:/FileStore/FATCA_FFI_List.csv", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load SBI FATCA CRS converter

# COMMAND ----------

import os

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

from datetime import datetime, timedelta
import re

flexcube_objects = ['EBX_CRS_IGA_FATCA_SBI25_WR',
                    'EBX_CRS_IGA_FATCA_NAICS2022',
                    'EBX_SECT_GRP_SBI25_MAP_NAICS22']

for item in flexcube_objects:
    # get the most recent version available in gdp
    path = f'abfss://ebx@edlcorestdeuprod0001.dfs.core.windows.net/{item}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

    # get the most recent file available in gdp and create temp view
    path_file = f'{path}{version}/data/'
    files = dbutils.fs.ls(path_file)
    load_date = max(file.path.split('EDL_LOAD_DT=')[1][:8] for file in files if 'EDL_LOAD_DT=' in file.path)
    spark.read.parquet(f'{path_file}EDL_LOAD_DT={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.EBX_CRS_IGA_FATCA_SBI25_WR

# COMMAND ----------

spark.sql('SELECT * FROM EBX_CRS_IGA_FATCA_SBI25_WR').write.mode('overwrite').saveAsTable('radar.EBX_CRS_IGA_FATCA_SBI25_WR')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.EBX_CRS_IGA_FATCA_NAICS2022

# COMMAND ----------

spark.sql('SELECT * FROM EBX_CRS_IGA_FATCA_NAICS2022').write.mode('overwrite').saveAsTable('radar.EBX_CRS_IGA_FATCA_NAICS2022')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.EBX_SECT_GRP_SBI25_MAP_NAICS22

# COMMAND ----------

spark.sql('''
          SELECT DISTINCT
            RABOSBI2025CODE
            , SBI2025SHORTDESCRIPTION
            , NAICS2022CODE
            , NAICS2022DESCRIPTION
          FROM EBX_SECT_GRP_SBI25_MAP_NAICS22
''').write.mode('overwrite').saveAsTable('radar.EBX_SECT_GRP_SBI25_MAP_NAICS22')
