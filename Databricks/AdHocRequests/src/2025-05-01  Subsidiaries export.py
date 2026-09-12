# Databricks notebook source
# MAGIC %md
# MAGIC Goal: to export data to compare with subsidiaries export list
# MAGIC Author: Ruud

# COMMAND ----------

# MAGIC %sql
# MAGIC select GcobId, ClientlifecycleName, FullLegalName, NextReviewDate from radar.clients

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get GIC data from RDM

# COMMAND ----------

#check radar datamodel party
import os
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
tenant_id = TenantId
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

saradar_write_storage = f'saradar{environment}'
saradar_container = 'radardatamodel'

# COMMAND ----------

def authenticate_storage_account(environment, write_storage):
    # This method is for Configuring Spark to access SARADAR Storage account using OAuth authentication
    spark.conf.set("fs.azure.account.auth.type."+write_storage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+write_storage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+write_storage+".dfs.core.windows.net", ""+application_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+write_storage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+write_storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")

    print('Connection to ' + write_storage +' storage account is successful.')

    return 

# COMMAND ----------

application_id = os.environ['APP_REG_APP_ID']

authenticate_storage_account(environment, saradar_write_storage) 

# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls(f'abfss://'+saradar_container+'@'+saradar_write_storage+'.dfs.core.windows.net/Party/1/data')

# COMMAND ----------

df_party = spark.read.parquet('abfss://radardatamodel@saradarpreprd.dfs.core.windows.net/Party/1/data/EDL_LOAD_DTS=20250501/')

# COMMAND ----------

df_party.createOrReplaceTempView('Party')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from party 
# MAGIC Where lower(FullLegalName) like '%oak fundo%'
# MAGIC
# MAGIC limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from party 
# MAGIC Where upper(FullLegalName) like 'FUNDO DE INVESTIMENTO EM DIR%'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from party 
# MAGIC Where FullLegalName like '%Rabo Equipment%'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Maple Fundo de investimento Multimercado
# MAGIC %sql
# MAGIC select * 
# MAGIC from party 
# MAGIC Where FullLegalName like '%Maple%'

# COMMAND ----------

# DBTITLE 1,FIDC MRFG FUNDO DE INVESTIMENTOEM DIREITOS CREDITORIOS
# MAGIC %sql
# MAGIC select * 
# MAGIC from party 
# MAGIC Where FullLegalName like '%Fundo De Investimento Em Direitos Creditorios%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from party 
# MAGIC Where PartyIdentifier like '%GIC%'
# MAGIC
# MAGIC
# MAGIC limit 5

# COMMAND ----------


