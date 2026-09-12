# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal:
# MAGIC to read from storage account and show top 10 of elements
# MAGIC
# MAGIC - To use for discussions and see what is there.
# MAGIC

# COMMAND ----------

import os
from datetime import datetime


# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
EUReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
environment=os.environ['ENV']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

ReadStorage = f'saradar{environment}'

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------


EDL_LOAD_DTS = f'EDL_LOAD_DTS={str(datetime.now().strftime("%Y%m%d"))}'
saradar_container  = 'radardatamodel'
dbutils.fs.ls('abfss://'+saradar_container+'@'+ReadStorage+'.dfs.core.windows.net/')

# COMMAND ----------

objects = ['Party', 'Party_Address', 'Party_CDDCase', 'Party_CDDCase_QuestionAnswer', 'Party_Naics']
version_number = 1

for object_name in objects:
    spark.read.parquet('abfss://'+saradar_container+'@'+ReadStorage+'.dfs.core.windows.net/'+object_name+'/'+str(version_number)+'/data/'+EDL_LOAD_DTS+'/').createOrReplaceTempView(object_name)
    display(spark.sql(f"select * from {object_name} limit 4 "))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Party limit 100

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Party_NAICS
# MAGIC %sql
# MAGIC select GCID, LocalSystemId, LocalSystemName, NaicsCode
# MAGIC from party_naics limit 4
# MAGIC

# COMMAND ----------

# DBTITLE 1,Party_CASE
# MAGIC %sql
# MAGIC (SELECT * 
# MAGIC from Party_CDDCase WHERE SourceSystem = 'OCDD' limit 2)
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC (SELECT * 
# MAGIC from Party_CDDCase 
# MAGIC WHERE SourceSystem <> 'OCDD' limit 2)
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC (SELECT * 
# MAGIC from Party_CDDCase 
# MAGIC WHERE SourceSystem = 'GCOB' limit 2)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC (SELECT distinct SourceSystem from Party_CDDCase)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests
# MAGIC - Party table has unique per gcid?
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Unique GCID in items
# MAGIC %sql
# MAGIC select GCID 
# MAGIC , count(*) as NR
# MAGIC from Party 
# MAGIC group by GCID
# MAGIC order by count(*) desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from Party

# COMMAND ----------

# DBTITLE 1,All Europe clients have a UniqueGCOBid
# MAGIC %sql
# MAGIC -- select parties that are in locations europe but do not have gcob id

# COMMAND ----------

# DBTITLE 1,Location / portfolio / responsible person
# MAGIC %sql
# MAGIC -- all parties that follow W&R definition, have a location or region assigned to them
