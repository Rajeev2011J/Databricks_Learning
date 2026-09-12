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

# MAGIC %md
# MAGIC ### get local export data

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df = pd.read_csv('Siebel_GCOB_Volmacht.csv', header= 0, sep= ';')

# COMMAND ----------

df.head()

# COMMAND ----------

sdf_missingvolmacht = spark.createDataFrame(df)
sdf_missingvolmacht.createOrReplaceTempView('MissingVolmachten')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### get GCOB data

# COMMAND ----------

gcob_structure = spark.read.parquet('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/LE_NP_clients/ClientStructure/101/data/EDL_LOAD_DTS=20240731/')
gcob_structure.createOrReplaceTempView('gcob_structure')

# COMMAND ----------

display(gcob_structure.limit(10))  


# COMMAND ----------

gcob_client = spark.read.parquet('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService/party_client/101/data/EDL_LOAD_DTS=20240730/')
gcob_client.createOrReplaceTempView('gcob_client')

# COMMAND ----------

display(gcob_client.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### get GCDS data

# COMMAND ----------

#gcds keystore to match against GCOB-id's - to see if already in scope of review.
gcds_keystorekey = spark.read.parquet('abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_KeyStoreKey/4601/data/EDL_LOAD_DTS=20240730*/*.parquet')

df_gcds_siebel = gcds_keystorekey.filter('KeyStore_type == "SBWRR"')
df_gcds_gcob = gcds_keystorekey.filter('KeyStore_type == "GCOBID"')

# to temp views
df_gcds_gcob.createOrReplaceTempView('gcds_gcob')
df_gcds_siebel.createOrReplaceTempView('gcds_siebel')

# To match siebelid with GcobID
df_siebel_gcob = spark.sql(""" select t1.KeyStore_value AS REL_ID, 
    t2.KeyStore_Value AS GcobId
    from gcds_siebel AS t1
    LEFT JOIN gcds_gcob AS t2 on t1.GCID = t2.GCID
    """)

df_siebel_gcob.createOrReplaceTempView('siebel_gcob')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from siebel_gcob limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gcob_client where gcobid = 48127

# COMMAND ----------

# Get GCOB cases
party_case_details = spark.read.parquet('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService/party_case_client_details/101/data/EDL_LOAD_DTS=20240730/')

party_case_details.createOrReplaceTempView('party_case_details')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_case_details limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcob_structure_with_parent_gcobid AS
# MAGIC
# MAGIC select t1.* 
# MAGIC , t2.GcobId AS ParentGCOBid
# MAGIC from gcob_structure t1
# MAGIC left join party_case_details t2 on t1.ParentIdentity = t2.ClientId AND LEFT(3, SourceClient) == 'LEC'

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### joining all together
# MAGIC
# MAGIC ** in the structure, the parentIdentity is the LEC-id, not the GCOBid.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW volmacht_to_siebel  AS
# MAGIC
# MAGIC select 
# MAGIC  t1.*
# MAGIC  ,t2.GcobId AS `ClientGcobId`
# MAGIC  ,t3.GcobId AS `GevolmachtigdeGcobId`
# MAGIC
# MAGIC from MissingVolmachten t1
# MAGIC left join siebel_gcob t2 on t1.`VM_GEVER_Siebel-ID` = t2.REL_ID
# MAGIC left join siebel_gcob t3 on t1.`GEVOLMACHTIGDE_Siebel_ID` = t3.REL_ID
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gcob_structure_with_parent_gcobid 
# MAGIC where parentGcobId is not null 
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW  volmacht_to_gcob AS
# MAGIC
# MAGIC select 
# MAGIC     t1.*
# MAGIC   , t2.ClientCaseId
# MAGIC   , t2.IsLatestApprovedVersionOfClient
# MAGIC   , t2.ParentIdentity
# MAGIC   , t2.ParentCalculatedShareholdingPercentageOnClient
# MAGIC
# MAGIC
# MAGIC from volmacht_to_siebel t1
# MAGIC left join gcob_structure t2 on t1.ClientGcobId = t2.ClientGcobId AND t1.GevolmachtigdeGcobId = t2.ParentIdentity and t2.Parenttype = 'LegalEntityClient'--cast(t2.ParentGCOBid as STRING)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gcob_structure where clientgcobid = 5379 and clientId = 51127

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from volmacht_to_gcob where IsLatestApprovedVersionOfClient = 'True'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storing/export results

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE  IF NOT EXISTS AdHocRequests

# COMMAND ----------

# storting results
sdf = spark.table('volmacht_to_gcob')
sdf.write.mode('overwrite').saveAsTable('AdHocRequests.VolmachtSiebelGcobCompare')

# COMMAND ----------


