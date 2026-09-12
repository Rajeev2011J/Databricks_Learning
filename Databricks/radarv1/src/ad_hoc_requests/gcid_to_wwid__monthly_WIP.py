# Databricks notebook source
import os

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

# DBTITLE 1,load gcds data, latest version, latest file
import re

from pyspark.sql import SparkSession
# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
    , 'client_Client'
    , 'client_PartyRole'
    # , 'client_Products'
    # , 'client_OnboardedLocations'
    # , 'client_PartytoPartyRelationship'
]

for item in gcds_tables:
  # get the most recent version available in gdp
  path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/'
  files = dbutils.fs.ls(path)
  version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
  
  # get the most recent file available in gdp
  path_file = f'{path}{version}/data/'
  files = dbutils.fs.ls(path_file)
  load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

  spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data/LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# DBTITLE 1,gcdsRawData
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcid_to_wwid AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.gcid
# MAGIC   , t2.Full_legal_name AS gcdsClientName
# MAGIC   , t3.Life_cycle_status AS gcdsClientLifeCycleStatus
# MAGIC   , t1.KeyStore_value AS WWID
# MAGIC   , t3.party_role
# MAGIC   , t2.Customer_ambition AS clientStrategy
# MAGIC   , t2.`RM-name` AS relationshipManagerName
# MAGIC   , t2.`Global_CO-name` AS gcdsGlobalCoName
# MAGIC   , t2.`Global_CO-location` AS gcdsGlobalColocation
# MAGIC
# MAGIC FROM client_KeyStoreKey t1
# MAGIC LEFT JOIN client_Client t2 ON t1.gcid = t2.gcid
# MAGIC LEFT JOIN client_PartyRole t3 ON t1.gcid = t3.gcid
# MAGIC
# MAGIC WHERE t1.keystore_type = 'WWID'
# MAGIC   -- AND t3.party_role = 'Customer'
# MAGIC   AND t1.status = 'Active'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.gcid_to_wwid

# COMMAND ----------

spark.sql('SELECT * FROM gcid_to_wwid').write.mode('overwrite').saveAsTable('radar.gcid_to_wwid')
