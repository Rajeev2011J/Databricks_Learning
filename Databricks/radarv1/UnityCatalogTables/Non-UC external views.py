# Databricks notebook source
import os

# COMMAND ----------

# Fetching environment variables
application_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']
environment=os.environ['ENV']

# Retrieving Client Secret from connected Secrets
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{application_id}")

def authenticate_storage_account(write_storage):
    # This method is for Configuring Spark to access SARADAR Storage account using OAuth authentication
    spark.conf.set("fs.azure.account.auth.type."+write_storage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+write_storage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+write_storage+".dfs.core.windows.net", ""+application_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+write_storage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+write_storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")

    print('Connection to ' + write_storage +' storage account is successful.')

    return 

# COMMAND ----------

authenticate_storage_account('edlcorestdeuprod0001')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW wr_fj_parties_and_risk_assessment_preprd.radar.gcds_client_4602
# MAGIC
# MAGIC AS SELECT * 
# MAGIC FROM PARQUET.`abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_Client/4602/data` --/LOAD_DTS=20260225T043051Z/

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW radar.vw_gcds_client
# MAGIC
# MAGIC AS SELECT * 
# MAGIC FROM PARQUET.`abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_Client/4602/data` --/LOAD_DTS=20260225T043051Z/

# COMMAND ----------

# DBTITLE 1,test to get max from view
# MAGIC %sql
# MAGIC select * 
# MAGIC from radar.vw_gcds_client
# MAGIC where LOAD_DTS = (SELECT MAX(LOAD_DTS) FROM radar.vw_gcds_client)
# MAGIC limit 10

# COMMAND ----------


