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

# COMMAND ----------

# Read data from FireBird Database and store in GDP Raw layer
jdbcHostname = f'{jdbcHostname}'
jdbcPort = 1433
jdbcDatabase = "APP_ADB"
 
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
connectionProperties = {
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication" : "ActiveDirectoryServicePrincipal",
    "UserName" : app_reg_app_id ,
    "Password" : service_credential
}
 
spark.read.jdbc(url=jdbcUrl,table='pa.KYCMasterListRegistry',properties = connectionProperties).createOrReplaceTempView('KYCMasterListRegistry')

 # this is for fetching the fast migration in reviewtypename

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW reviewLocationChangeLog AS
# MAGIC WITH QueryA_SC AS (
# MAGIC     SELECT DISTINCT t2.ReviewLocation AS PreviousReviewLocation, t1.ReviewLocation, t1.GCOBID, t1.RecordActive_At, t1.KYCGroup, t1.InputUser
# MAGIC     FROM KYCMasterListRegistry t1
# MAGIC     JOIN KYCMasterListRegistry t2
# MAGIC     ON t1.GCOBID = t2.GCOBID
# MAGIC     AND t2.ReviewLocation NOT IN ('KYC SC')
# MAGIC     AND t2.RecordExpired_At = t1.RecordActive_At
# MAGIC     WHERE t1.ReviewLocation = 'KYC SC'
# MAGIC     AND t1.Is_ActiveRecord = 1
# MAGIC     AND t1.RecordActive_At >= '2024-01-01'
# MAGIC ),
# MAGIC QueryB_SC AS (
# MAGIC     SELECT DISTINCT t2.ReviewLocation AS PreviousReviewLocation, t1.ReviewLocation, t1.GCOBID, t1.RecordActive_At, t1.KYCGroup, t1.InputUser
# MAGIC     FROM KYCMasterListRegistry t1
# MAGIC     JOIN KYCMasterListRegistry t2
# MAGIC     ON t1.GCOBID = t2.GCOBID
# MAGIC     AND t2.ReviewLocation NOT IN ('KYC SC')
# MAGIC     AND t2.RecordExpired_At = t1.RecordActive_At
# MAGIC     WHERE t1.ReviewLocation = 'KYC SC'
# MAGIC     AND t1.Is_ActiveRecord = 0
# MAGIC     AND t2.Is_ActiveRecord = 0
# MAGIC     AND t1.RecordActive_At > t2.RecordActive_At
# MAGIC     AND t1.RecordActive_At >= '2024-01-01'
# MAGIC ),
# MAGIC QueryA_UK AS (
# MAGIC     SELECT t2.ReviewLocation AS PreviousReviewLocation, t1.ReviewLocation, t1.GCOBID, t1.RecordActive_At, t1.KYCGroup, t1.InputUser
# MAGIC     FROM KYCMasterListRegistry t1
# MAGIC     JOIN KYCMasterListRegistry t2
# MAGIC     ON t1.GCOBID = t2.GCOBID
# MAGIC     AND t2.ReviewLocation NOT IN ('KYC UK')
# MAGIC     AND t2.RecordExpired_At = t1.RecordActive_At
# MAGIC     WHERE t1.ReviewLocation = 'KYC UK'
# MAGIC     AND t1.Is_ActiveRecord = 1
# MAGIC     AND t1.RecordActive_At >= '2024-01-01'
# MAGIC ),
# MAGIC QueryB_UK AS (
# MAGIC     SELECT t2.ReviewLocation AS PreviousReviewLocation, t1.ReviewLocation, t1.GCOBID, t1.RecordActive_At, t1.KYCGroup, t1.InputUser
# MAGIC     FROM KYCMasterListRegistry t1
# MAGIC     JOIN KYCMasterListRegistry t2
# MAGIC     ON t1.GCOBID = t2.GCOBID
# MAGIC     AND t2.ReviewLocation NOT IN ('KYC UK')
# MAGIC     AND t2.RecordExpired_At = t1.RecordActive_At
# MAGIC     WHERE t1.ReviewLocation = 'KYC UK'
# MAGIC     AND t1.Is_ActiveRecord = 0
# MAGIC     AND t2.Is_ActiveRecord = 0
# MAGIC     AND t1.RecordActive_At > t2.RecordActive_At
# MAGIC     AND t1.RecordActive_At >= '2024-01-01'
# MAGIC )
# MAGIC SELECT * FROM QueryA_SC
# MAGIC UNION ALL
# MAGIC SELECT * FROM QueryB_SC
# MAGIC WHERE NOT EXISTS (SELECT 1 FROM QueryA_SC)
# MAGIC UNION ALL
# MAGIC SELECT * FROM QueryA_UK
# MAGIC UNION ALL
# MAGIC SELECT * FROM QueryB_UK
# MAGIC WHERE NOT EXISTS (SELECT 1 FROM QueryA_UK)

# COMMAND ----------

spark.sql('select * from reviewLocationChangeLog').write.mode('overwrite').saveAsTable('radar.reviewLocationChangeLog')
