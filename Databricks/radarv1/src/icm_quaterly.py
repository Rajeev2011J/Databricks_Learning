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

# MAGIC %md
# MAGIC ###  This Notebook Run 1st day of every quarter to refresh ICM dashboard.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW icmDashboard AS
# MAGIC select 
# MAGIC    t1.uniquegcobid
# MAGIC  , t1.fulllegalname
# MAGIC  , t1.globalkycportfolionew
# MAGIC  , t1.clientlifecyclename
# MAGIC  , t1.fihubindicator_derived
# MAGIC  , t1.globalreportingregion
# MAGIC  , t2.validatedrisklevel
# MAGIC  , t1.GlobalClientOwner
# MAGIC  , t1.GlobalClientOwnerLocation
# MAGIC  , t1.Overdue
# MAGIC  , t2.EDL_LoadDate
# MAGIC  , t1.SectorTeam
# MAGIC  , t1.Scope
# MAGIC  from radar.clients as t1
# MAGIC left join radar.cases as t2 on t1.SourceClient = t2.SourceClient

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.icmDashboard

# COMMAND ----------

spark.sql('SELECT * FROM icmDashboard').write.mode('overwrite').saveAsTable('radar.icmDashboard')
