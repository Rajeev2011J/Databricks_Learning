# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal:
# MAGIC to check out data contents of Legacy 2. 
# MAGIC
# MAGIC #### Expecting:
# MAGIC - RAF
# MAGIC - NTC RAF
# MAGIC - AU
# MAGIC - NTC CCDB
# MAGIC - SA Employees
# MAGIC   - Argentina
# MAGIC   - Chile
# MAGIC
# MAGIC #### who may know more:
# MAGIC - Colin Kobes
# MAGIC - 2 l2 maintainers
# MAGIC   - Jonathan Sayce
# MAGIC   - James Mott
# MAGIC
# MAGIC
# MAGIC #### Flow of the notebook. 
# MAGIC 1. connect to GDP
# MAGIC 2. Read legacy
# MAGIC 3. Find relevant BU's to slice over, both for clients and other parties. 
# MAGIC 4. Summarize counts per party stack.

# COMMAND ----------

# DBTITLE 1,imports
import os
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils

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

# MAGIC %md
# MAGIC

# COMMAND ----------

#Derive the date for which data has to be processes

Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
Today = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'
print (load_dts)
Load = 'EDL_LOAD_DTS='+ datetime.today().strftime('%Y%m%d')
print(Load)
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
EDL_LoadDate = (datetime.today()).strftime("%Y-%m-%dT%H:%m:%mZ")

# COMMAND ----------

load_df = pd.DataFrame({'GDPname':[
'Legacy2_case_client_details'
]})
 
# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/Legacy2/{row.GDPname}/101/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Legacy2_case_client_details limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select Businesslinename, count(*) 
# MAGIC from Legacy2_case_client_details
# MAGIC group by Businesslinename

# COMMAND ----------

# MAGIC %sql
# MAGIC select GlobalClientOwnerLocation, count(*) 
# MAGIC from Legacy2_case_client_details
# MAGIC group by GlobalClientOwnerLocation

# COMMAND ----------

# MAGIC %sql
# MAGIC select ClientType, count(*) 
# MAGIC from Legacy2_case_client_details
# MAGIC group by ClientType

# COMMAND ----------

# MAGIC %sql
# MAGIC select LEFT(GCOBID,2) AS OriginalPortfolio
# MAGIC ,ClientType
# MAGIC , count(*) 
# MAGIC from Legacy2_case_client_details
# MAGIC group by LEFT(GCOBID,2), ClientType

# COMMAND ----------

# MAGIC %sql
# MAGIC select LEFT(GCOBID,2) AS OriginalPortfolio, count(*) 
# MAGIC from Legacy2_case_client_details
# MAGIC group by LEFT(GCOBID,2)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from Legacy2_case_client_details
# MAGIC where FullLegalName like '%Retamal Thomsen%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC Legacy2_case_client_details
# MAGIC where LEFT(GCOBID,2) = 'L2'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC Legacy2_case_client_details
# MAGIC where LEFT(GCOBID,2) = 'CC'

# COMMAND ----------


