# Databricks notebook source
import os 
from  RadarUtils import *


# COMMAND ----------

storage_account_name = "salandingzonefecradarprd"

# COMMAND ----------

authenticate_storage_account(storage_account_name)

# COMMAND ----------

list_parqeut = dbutils.fs.ls("abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/") 

# COMMAND ----------

filtered_files = [file.path for file in list_parqeut if file.name.endswith('.parquet/') and 'T24' in file.name]

# COMMAND ----------

len(filtered_files)

# COMMAND ----------

parquet_custoer = spark.read.parquet('abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/202401101816-T24.RD.NZ.CUSTOMERS_20240110120100.parquet/*')

# COMMAND ----------

list_parqeut[:10]

# COMMAND ----------

display(parquet_custoer.limit(5))
