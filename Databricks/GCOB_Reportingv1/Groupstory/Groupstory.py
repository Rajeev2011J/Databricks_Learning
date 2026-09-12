# Databricks notebook source
# MAGIC %md 
# MAGIC ## Goal:
# MAGIC to deliver a dataset object with each client and to which groups it belongs
# MAGIC
# MAGIC ## author:
# MAGIC Ruud van Laar
# MAGIC
# MAGIC ## PBI-related
# MAGIC - 0383422
# MAGIC
# MAGIC #### Flow of logic:
# MAGIC - connect to GDP clients
# MAGIC - Connect to GDP groupstories
# MAGIC - join to create one object
# MAGIC - save into the Hive metastore (hopefully a Unitiy Catalog at a later phase)

# COMMAND ----------

# DBTITLE 1,import libraries
import os
import datetime

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
Today = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'
print (load_dts)
Load = 'EDL_LOAD_DTS='+ datetime.today().strftime('%Y%m%d')
print(Load)
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
EDL_LoadDate = (datetime.today()).strftime("%Y-%m-%dT%H:%m:%mZ")

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
#'party_AllCasesReport'
'party_case_client_details'
, 'party_group_story_details'
, 'party_client'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{row.GDPname}/101/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# dbutils.fs.ls(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW client_groupnames AS (
# MAGIC select distinct 
# MAGIC     t2.LegalEntityClientGcobId as GcobId
# MAGIC   , t2.GroupStoryName
# MAGIC FROM party_client as t1
# MAGIC left join party_group_story_details as t2 on t1.GcobId = t2.LegalEntityClientGcobId
# MAGIC WHERE t2.LifeCycleStatusType = 'Active Group Story'
# MAGIC )

# COMMAND ----------

df = spark.table('client_groupnames')

# COMMAND ----------

# DBTITLE 1,Logical Test
# %sql
# select  * from client_groupnames where GcobId in (
#   select GcobId from client_groupnames group by GcobId having count(*) > 1
# ) order by GcobId

# COMMAND ----------

# DBTITLE 1,store into Hive metastore
df.write.mode('overwrite').saveAsTable('radar.clientGroups')

# COMMAND ----------



# COMMAND ----------


