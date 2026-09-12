# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal
# MAGIC To prepare the T24 customers file to reference in FEC ESG notebook. 

# COMMAND ----------

import pandas as pd
import os
from datetime import datetime
import re
from pyspark.sql.functions import to_date, lit

# COMMAND ----------


app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

SALZReadStorage = 'salandingzonefecradarprd'

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage +".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage +".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage +".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage +".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage +".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

t24_files = dbutils.fs.ls("abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/")
t24_files_customer_2025 = [file.name for file in t24_files if 'CUSTOMERS_2025' in file.name]

# COMMAND ----------

# DBTITLE 1,reading latest files from landing zone.
#TODO: add businessline and region per scope.
sdf_cb_au = (spark.read.parquet("abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/202504190303-T24.CB.AU.CUSTOMERS_20250422110132.parquet/")
             .withColumn('BusinessLine', lit('Country Banking'))
             .withColumn('Location', lit('Australia')))

sdf_cb_nz = (spark.read.parquet('abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/202504190303-T24.CB.NZ.CUSTOMERS_20250422110132.parquet/')
             .withColumn('BusinessLine', lit('Country Banking'))
             .withColumn('Location', lit('New Zealand')))

sdf_rd_nz = (spark.read.parquet('abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/202504222244-T24.RD.NZ.CUSTOMERS_20250422120400.parquet/')
             .withColumn('BusinessLine', lit('Rabo Online Savings'))
             .withColumn('Location', lit('New Zealand')))

sdf_rd_au = (spark.read.parquet('abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/202504222334-T24.RD.AU.CUSTOMERS_20250422120400.parquet/')
             .withColumn('BusinessLine', lit('Rabo Online Savings'))
             .withColumn('Location', lit('Australia')))

# COMMAND ----------

ranz_customers = sdf_cb_au.union(sdf_cb_nz).union(sdf_rd_nz).union(sdf_rd_au)

# COMMAND ----------

for col in ranz_customers.columns:
    ranz_customers = ranz_customers.withColumnRenamed(col,col.replace("\\s", "_").replace("(", "").replace(")", "").replace(" ", "_"))

# COMMAND ----------

(ranz_customers
 .write
 .mode('overwrite')
 .option('overwriteSchema', True)
 .saveAsTable('FEC_ESG.T24_CUSTOMERS'))

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from
# MAGIC FEC_ESG.T24_CUSTOMERS
# MAGIC limit 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) ,  SEB_value
# MAGIC from FEC_ESG.T24_CUSTOMERS as t1
# MAGIC WHERE Customer_status = 'ACTIVE'
# MAGIC GROUP BY  SEB_value

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) ,  Primary_NAICS_value
# MAGIC from FEC_ESG.T24_CUSTOMERS as t1
# MAGIC WHERE Customer_status = 'ACTIVE'
# MAGIC GROUP BY Primary_NAICS_value

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) ,  LEFT(t1.`SIC_value`,4) AS SIC
# MAGIC from FEC_ESG.T24_CUSTOMERS as t1
# MAGIC WHERE Customer_status = 'ACTIVE'
# MAGIC GROUP BY LEFT(t1.`SIC_value`,4)
# MAGIC   

# COMMAND ----------


