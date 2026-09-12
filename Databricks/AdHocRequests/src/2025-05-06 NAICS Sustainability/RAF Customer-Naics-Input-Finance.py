# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal
# MAGIC Read RAF files from Riskshield
# MAGIC export ESG.RAF_Clients
# MAGIC export ESG.RAF_Naics
# MAGIC

# COMMAND ----------

import pandas as pd
import os
from datetime import datetime
import re

# COMMAND ----------

# DBTITLE 1,env

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

SALZReadStorage = 'salandingzonefecradarprd'
RAF_Files = dbutils.fs.ls(f"abfss://riskshield-raf@{SALZReadStorage}.dfs.core.windows.net/")

# COMMAND ----------

Raf_2025 = [files.path for files in RAF_Files if 'CUSTOMERS_2025_0701' in files.name]

# COMMAND ----------

dbutils.fs.ls(f"abfss://riskshield-raf@{SALZReadStorage}.dfs.core.windows.net/20250628/")

# COMMAND ----------

raf_cust = spark.read.parquet('abfss://riskshield-raf@salandingzonefecradarprd.dfs.core.windows.net/20250628/RDL.RAF.CUSTOMERS_20250628.parquet/')

# COMMAND ----------

raf_cust.createOrReplaceTempView('RS_RAF_CUST')

# COMMAND ----------

# DBTITLE 1,CREATE RAF customer view
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RAF_CUST AS
# MAGIC SELECT
# MAGIC   CONCAT('RAF_', Customer_ID_CIF_number) AS PartyIdentifier
# MAGIC , CASE 
# MAGIC     WHEN CUSTOMER_TYPE_ID = 'P' THEN 'Natural Person'
# MAGIC     WHEN CUSTOMER_TYPE_ID = 'C' THEN 'Legal Entity' 
# MAGIC   END AS Party_type
# MAGIC , 'RAF' AS GlobalClientOwnerLocation
# MAGIC , CASE
# MAGIC 		WHEN Branch_no ='RIL'  THEN 'Global Rural Banking'
# MAGIC 		WHEN Branch_no ='RAF-NLS'  THEN 'NA - Input Finance'
# MAGIC 		WHEN Branch_no ='RAF'  THEN 'NA - Direct Lending'
# MAGIC 		WHEN Branch_no ='RCBR'  THEN 'Wholesale Corporate'
# MAGIC 		WHEN Branch_no ='RUCA'  THEN 'Global Rural Banking'
# MAGIC 		WHEN Branch_no ='NYBR'  THEN 'Wholesale Corporate' ELSE NULL
# MAGIC 	END AS BusinessLineName
# MAGIC , 'WR' AS WR_OR_RETAIL
# MAGIC , 'NA' AS GlobalClientOwnerRegion
# MAGIC
# MAGIC , IF_LoadDate AS RefreshDate
# MAGIC , CASE 
# MAGIC     WHEN Customer_status = 'Active' THEN 'Client'
# MAGIC     -- ?? True_CUSTOMER?
# MAGIC     ELSE Customer_status
# MAGIC   END AS LifeCycleStatus
# MAGIC
# MAGIC FROM RS_RAF_CUST
# MAGIC WHERE Branch_no = 'RAF-NLS' OR Branch_no = 'RIL'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from RAF_CUST

# COMMAND ----------

# DBTITLE 1,CREATE RAF NAICS view
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RAF_NAICS AS
# MAGIC
# MAGIC SELECT CONCAT('RAF_', Customer_ID_CIF_number) AS PartyIdentifier
# MAGIC , Customer_ID_CIF_number AS LocalSystemId
# MAGIC , 'RAF'AS LocalSystemName
# MAGIC , Primary_NAICS_value AS NAICScode
# MAGIC FROM RS_RAF_CUST

# COMMAND ----------

# DBTITLE 1,Store AS Table in Catalog
(spark.table('RAF_CUST')
 .write.mode('overwrite')
 .option("mergeSchema", "true")
 .saveAsTable('FEC_ESG.RAF_Customers'))


(spark.table('RAF_NAICS')
 .write.mode('overwrite')
 .option("mergeSchema", "true")
 .saveAsTable('FEC_ESG.RAF_NAICS'))

# COMMAND ----------


