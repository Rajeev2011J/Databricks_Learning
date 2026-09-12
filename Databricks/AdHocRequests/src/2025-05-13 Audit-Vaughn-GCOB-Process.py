# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal:
# MAGIC On 2025-04-22 we exported a file with all GCOB case details. 
# MAGIC This contained .... rows.
# MAGIC
# MAGIC The goal is to make it explainable where it comes from.
# MAGIC
# MAGIC
# MAGIC 1. GCOB -> GDP Raw = copy of all tables directly.
# MAGIC     - LE cases
# MAGIC         - count le-client: 114080
# MAGIC         - count le-case: 112810
# MAGIC     - NP cases
# MAGIC         - count np client: 6934
# MAGIC         - count np case 6933
# MAGIC     - Legacy2 Cases
# MAGIC         - count: 53897
# MAGIC 2. RAW -> Defined = Defining targed data objects (integrating NP files and LE files)
# MAGIC     - GCOB: 119743 (= 112810 + 6933)
# MAGIC         - scope: LE + NP
# MAGIC         - filtered: data must exist in cases and 'clients' table
# MAGIC     - Legacy2: 48382
# MAGIC         - filtered: Cancelled cases
# MAGIC 3. Defined -> Radar
# MAGIC     - Add Legacy2 and GCOB. Remove cancelled cases.
# MAGIC         - L2 filter: Legacy2_case_client_details 
# MAGIC             - IsClient = 'true 
# MAGIC             - ClientTypeId in (1,2,3)
# MAGIC             - not a NA client without NcinoID
# MAGIC         - GCOB filter:
# MAGIC             - CaseStatusName <> 'Cancelled'
# MAGIC     - radar (exported): nr rows = 112191
# MAGIC

# COMMAND ----------

import os

# COMMAND ----------

#Fetching environment variables
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

#Retrieving Client Secret from connected Secrets
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
# Configuring Spark to access GDP Defined Storage account using OAuth authentication
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from radar.mi_cases

# COMMAND ----------

dbutils.fs.ls(f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService/party_case_client_details/101/data/EDL_LOAD_DTS=20250422/')

# COMMAND ----------



# COMMAND ----------

df = spark.read.parquet('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService/party_case_client_details/101/data/EDL_LOAD_DTS=20250422/*.parquet')

# COMMAND ----------

df.count()

# COMMAND ----------

df.createOrReplaceTempView('clientdetails')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from clientdetails where CaseStatusName = 'Cancelled'

# COMMAND ----------

df = spark.read.parquet('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/Legacy2/Legacy2_case_client_details/102/data/LOAD_DT=20250422')#.createOrReplaceTempView('l2cases') #party_case_client_details/101/data/EDL_LOAD_DTS=20250422/*.parquet')

# COMMAND ----------

df.count()

# COMMAND ----------

dbutils.fs.ls(f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/Legacy2/Legacy2_case_client_details/102/data/LOAD_DT=20250422')

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'totalcases' as topic, 
# MAGIC count(*) as  
# MAGIC from l2cases
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select 'cancelledcases' as topic, 
# MAGIC count(*) 
# MAGIC from l2cases
# MAGIC where StatusTypeName = 'Cancelled'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from l2cases limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct statustypename from l2cases

# COMMAND ----------

df.select('CaseStatusName').dropDuplicates().show()

# COMMAND ----------

display(df.summary())

# COMMAND ----------

('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService/party_local_client_Owners/101/data/EDL_LOAD_DTS=20241121/*.parquet')
