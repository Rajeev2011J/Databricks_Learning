# Databricks notebook source
# MAGIC %md
# MAGIC ### Context:
# MAGIC For the topic of Sanction screening in the past 7 years, there has been a bug on the delta-delta logic. (i.e. delta on the sanction list, vs delta on the client lists). For current clients, it's been re-run. But for previous clients (and ideally their UBO's ) not. 
# MAGIC
# MAGIC We are looking for a new list of all former clients that have been off-boarded between 2017 and 2024. 
# MAGIC
# MAGIC ### Assumptions
# MAGIC - What portfolio's have had manual screening (outside of riskshield) processes
# MAGIC - We could get offboarded customers from GCDS (hopefully). Other customers could come from other systems.
# MAGIC - Most regions would have used Detica for screening. 
# MAGIC - We can only deliver W&R. no Obvion etc.
# MAGIC
# MAGIC
# MAGIC #### 
# MAGIC -- Scope
# MAGIC -- Chile (Sandra Pliveira)
# MAGIC -- China, Hong Kong, Singapore (Garry law). 
# MAGIC -- GCC (= Europe Core Lending)
# MAGIC -- FI's. 
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### Expected output. 
# MAGIC 1. All GCDS-offboarded customers of last 7 years.
# MAGIC 2. All GCOB-counterpart data if available. 
# MAGIC 3. Can we get proof of best offboarding procedure? the 'PR-before-offboarding' should also have contained this type of analysis.

# COMMAND ----------

# generic imports
import pandas as pd
from datetime import datetime
from datetime import timedelta
import os

currentDate = datetime.today().strftime('%Y%m%d')
Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')

currentYear = datetime.today().strftime('%Y')
currentMonth = datetime.today().strftime('%m')
currentDay = datetime.today().strftime('%d')

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

date_parameter = '20250423'
load_dts = 'EDL_LOAD_DTS=' + '20250423' + '*'
# = datetime.today().strftime('%Y%m%d')
#load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

#Derive the date for which data has to be processed from GDP
Today = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
gcds_df = pd.DataFrame({'definedDatasetname':[
'client_Client',
'client_KeyStoreKey',
'client_PartyRole',
'client_PartytoPartyRelationship',
'client_OnboardedLocations'
]})

# Create TempView for each loading table
for index, row in gcds_df.iterrows():
    spark.read.parquet(f'abfss://gcds@{ReadStorage}.dfs.core.windows.net/{row.definedDatasetname}/4601/data/{load_dts}/*.parquet').createOrReplaceTempView(row.definedDatasetname)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from client_OnboardedLocations limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select Branche_name , count(*) as GCIDS_WITH_OFFBOARDING_DATE from client_OnboardedLocations
# MAGIC where Offboarding_date > '2017-01-01'
# MAGIC group by Branche_name

# COMMAND ----------


