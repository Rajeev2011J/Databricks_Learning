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

spark.sql('SELECT * FROM radar.tx_wr_iban').createOrReplaceTempView('cash_wriban')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cash_wriban
# MAGIC limit 100

# COMMAND ----------

# connect to firebird kyc_adb
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
jdbcHostname='firebirdsqlserverprodnla.database.windows.net'

jdbcPort = 1433
jdbcDatabase = "APP_ADB"


jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
connectionProperties = {
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication" : "ActiveDirectoryServicePrincipal",
    "UserName" : app_reg_app_id ,
    "Password" : service_credential
}

spark.read.jdbc(url=jdbcUrl,table='ext.clients',properties = connectionProperties).createOrReplaceTempView("ext_clients")
spark.read.jdbc(url=jdbcUrl,table='ext.cases',properties = connectionProperties).createOrReplaceTempView("ext_cases")
spark.read.jdbc(url=jdbcUrl,table='pa.kycmasterlistregistry',properties = connectionProperties).createOrReplaceTempView("kycmasterlistregistry")

spark.read.jdbc(url=jdbcUrl,table='pa.sira_allclients',properties = connectionProperties).createOrReplaceTempView("pa_sira_allclients")

# COMMAND ----------

# MAGIC %sql
# MAGIC select sectorteam, * from radar.clients where UniqueGcobId = 'NP_959'

# COMMAND ----------

# MAGIC %sql
# MAGIC select ReviewLocation, sectorteam, * from  KYCMasterListRegistry where GcobId = 'NP_959' and Is_ActiveRecord = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC t1.reviewlocation as firebird_reviewlocation
# MAGIC , t2.reviewlocation as radar_reviewlocation
# MAGIC -- , count(*)
# MAGIC , ClientLifeCycleName
# MAGIC , t2.UniqueGcobId
# MAGIC , t2.FullLegalName
# MAGIC , t1.`global client owner location`
# MAGIC  
# MAGIC from ext_clients t1
# MAGIC left join radar.clients t2 on t1.gcobid = t2.UniqueGcobId
# MAGIC where t1.reviewlocation <> t2.reviewlocation and t2.ClientLifeCycleName

# COMMAND ----------

# MAGIC %sql
# MAGIC select sectorteam, ReviewLocation, kycgroup, * from radar.clients where UniqueGcobId = '48774' and ClientLifeCycleName = 'Client'

# COMMAND ----------

# MAGIC %sql
# MAGIC select reviewlocation,  * from radar.clients where gcobid = '100408'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC t1.ClientCaseInitiationStart as firebird_ClientCaseInitiationStart
# MAGIC , t2.ClientCaseInitiationStart as radar_ClientCaseInitiationStart
# MAGIC -- , count(*)
# MAGIC , ClientLifeCycleName
# MAGIC , t2.UniqueGcobId
# MAGIC , t2.FullLegalName
# MAGIC
# MAGIC from ext_clients t1
# MAGIC left join radar.clients t2 on t1.gcobid = t2.UniqueGcobId
# MAGIC where t1.ClientCaseInitiationStart <> t2.ClientCaseInitiationStart

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from

# COMMAND ----------

# MAGIC %sql
# MAGIC select CDDExecution, UniqueGcobId, * from radar.clients where WHERE GCOBID IN (
# MAGIC     '56563', '8297', '75215', '3291', '5998', '17516', 
# MAGIC     '51620', '14924', '78325', '15343', '78653', '2100', 
# MAGIC     '12473', '16476', '384', '46632', '25836', '6494', 
# MAGIC     '5885', '27358', '12474'
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE ext_cases

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE radar.clients

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC T1.`Days in current case phase` as firebird
# MAGIC , t2.Daysincurrentcasephase as radar
# MAGIC , t2.UniqueGcobId
# MAGIC , t1.gcobid
# MAGIC , t2.FullLegalName
# MAGIC , ((t2.Daysincurrentcasephase) - (T1.`Days in current case phase`)) as difference
# MAGIC  
# MAGIC , t1.`Case phase` as casephase_firebird
# MAGIC , t2.casephase as casephase_radar
# MAGIC  
# MAGIC from ext_cases t1
# MAGIC LEFT JOIN radar.cases t2
# MAGIC ON CASE
# MAGIC        WHEN t1.gcobid LIKE 'NP: %' THEN REPLACE(t1.gcobid, 'NP: ', 'NP_')
# MAGIC        ELSE t1.gcobid
# MAGIC    END = t2.UniqueGcobId AND t1.caseid = t2.caseid
# MAGIC   where t1.`Case phase` <> t2.casephase and t2.ClientLifeCycleName = 'Client' and t1.`Case phase` <> 'Offboarding' and t2.casephase <> 'Product Offboarding'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE radar.cases

# COMMAND ----------

# MAGIC %sql
# MAGIC select  t1.`client lifecycle status`,  t2.clientlifecyclename,  t1.gcobid from ext_clients as t1
# MAGIC left join radar.clients as t2 on t1.gcobid = t2.UniqueGcobId
# MAGIC where  t1.`client lifecycle status` <> t2.clientlifecyclename
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select t1.gcobid, t1.reviewlocation, t2.ReviewLocation as reviewlocationRadar, t1.sectorteam, t2.sectorteam, t1.`Global client owner location`, t2.GlobalClientOwnerLocation as GlobalClientOwnerLocationRadar, t2.GlobalReportingRegion from ext_clients as t1
# MAGIC left join radar.clients as t2 on t1.gcobid = t2.UniqueGcobId
# MAGIC where t1.sectorteam != t2.sectorteam and t1.`client lifecycle status` = 'Client'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kycmasterlistregistry where gcobid = 'NL_4316'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.clients as t1
# MAGIC left join ext_clients as t2 on t1.UniqueGcobId = t2.gcobid
# MAGIC where t1.UniqueGcobId is null 

# COMMAND ----------

# MAGIC %sql
# MAGIC     SELECT uniquegcobid 
# MAGIC     FROM radar.clients 
# MAGIC     WHERE sectorteam = 'CNS' AND Clientlifecyclename = 'Client'

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.clients where gcobid = '31713'

# COMMAND ----------

# DBTITLE 1,In Firebird but not in Radar
# MAGIC %sql
# MAGIC SELECT gcobid 
# MAGIC FROM ext_clients 
# MAGIC WHERE `global kyc portfolio new` = 'NL Structured Lending'  and gcobid not like '%:%' and `client lifecycle status` = 'Client'
# MAGIC AND gcobid NOT IN (
# MAGIC     SELECT uniquegcobid 
# MAGIC     FROM radar.clients 
# MAGIC     WHERE GlobalKYCPortfolioNew = 'NL Structured Lending'  and ClientLifeCycleName = 'Client'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,In Radar but not in Firebird
# MAGIC %sql
# MAGIC SELECT uniquegcobid, GlobalKYCPortfolioNew, GlobalClientOwnerLocation
# MAGIC FROM radar.clients  
# MAGIC WHERE GlobalKYCPortfolioNew = 'NL Structured Lending' and ClientLifeCycleName = 'Client'
# MAGIC AND uniquegcobid NOT IN (
# MAGIC     SELECT gcobid 
# MAGIC     FROM ext_clients 
# MAGIC     WHERE `global kyc portfolio new` = 'NL Structured Lending' and `client lifecycle status` = 'Client' and gcobid not like '%:%'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(gcobid) from ext_clients where `sectorteam` = 'CNS' and `Client lifecycle status` = 'Client'
# MAGIC
