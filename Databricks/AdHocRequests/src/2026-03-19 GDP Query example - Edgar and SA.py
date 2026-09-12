# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal:
# MAGIC - to show Edgar and other how simple it is to query GDP directly. 
# MAGIC
# MAGIC ### flow of notebook
# MAGIC 1. set up connection to GDP
# MAGIC 2. direct query on GDP

# COMMAND ----------

import os

# COMMAND ----------


application_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
tenant_id = os.environ['TENANT_ID']
environment=os.environ['ENV']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{application_id}")


# COMMAND ----------

def authenticate_storage_account(write_storage):
    # This method is for Configuring Spark to access SARADAR Storage account using OAuth authentication
    spark.conf.set("fs.azure.account.auth.type."+write_storage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+write_storage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+write_storage+".dfs.core.windows.net", ""+application_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+write_storage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+write_storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")

    print('Connection to ' + write_storage +' storage account is successful.')

    return 

authenticate_storage_account(ReadStorage)


# COMMAND ----------

# DBTITLE 1,Example: Query snapshot data from GDP
# MAGIC %sql
# MAGIC SELECT GCID, Full_Legal_Name, EDL_LOAD_DTS
# MAGIC
# MAGIC FROM Parquet.`abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_Client/4602/data`
# MAGIC
# MAGIC WHERE LOAD_DTS = (SELECT MAX(LOAD_DTS) FROM Parquet.`abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_Client/4602/data` WHERE LOAD_DTS < '2026*')
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW wr_fj_parties_and_risk_assessment_preprd.radar.gcds_client_4602
# MAGIC
# MAGIC AS SELECT * 
# MAGIC FROM PARQUET.`abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_Client/4602/data` --/LOAD_DTS=20260225T043051Z/
