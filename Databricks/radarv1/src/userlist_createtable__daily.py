# Databricks notebook source
import os

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# DBTITLE 1,gcob_tables
import re

gcob_tables = [
    'CaseService_case_WorkItem'
    , 'CaseService_user_GcobUser'  
]

# for item in gcob_tables:
for item in gcob_tables:
    # get the most recent version available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/'
    files = dbutils.fs.ls(path)
    version = max([
        int(re.search(r'/(\d+)/$', file.path).group(1))
        for file in files
        if re.search(r'/(\d+)/$', file.path)
    ])

    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/{version}/data/'

    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOADED_DTS=')[1][:8] for file in files if 'LOADED_DTS=' in file.path)

    spark.read.parquet(f'{path}LOADED_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW userlist AS
# MAGIC
# MAGIC WITH userworkitem_cte AS (
# MAGIC   SELECT
# MAGIC     t2.DisplayName AS UserName
# MAGIC     , t2.EmailAddress AS UserEmail
# MAGIC     , t1.DateCreated
# MAGIC   FROM CaseService_case_WorkItem t1
# MAGIC   LEFT JOIN CaseService_user_GcobUser t2 ON t1.AssignedUserReferenceId = t2.UserId
# MAGIC   WHERE t1.ResponsibleRole <> 1
# MAGIC     AND LEFT(t2.DisplayName, 3) <> 'eu.'
# MAGIC     AND t2.DisplayName <> 'Unknown'
# MAGIC     AND t2.EmailAddress <> ''
# MAGIC )
# MAGIC
# MAGIC , userlis_cte AS (
# MAGIC   SELECT
# MAGIC     UserName
# MAGIC     , UserEmail
# MAGIC     , MIN(DateCreated) AS DateFirstSeenInGCOB
# MAGIC     , MAX(DateCreated) AS DateLastSeenInGCOB
# MAGIC   FROM userworkitem_cte
# MAGIC   GROUP BY UserName, UserEmail
# MAGIC )
# MAGIC
# MAGIC , ranked_username_cte AS (
# MAGIC   SELECT 
# MAGIC     UserEmail
# MAGIC     , FIRST_VALUE(UserName) OVER (PARTITION BY UserEmail ORDER BY DateLastSeenInGCOB DESC) AS UserName
# MAGIC     , DateFirstSeenInGCOB
# MAGIC     , DateLastSeenInGCOB
# MAGIC   FROM userlis_cte
# MAGIC )
# MAGIC
# MAGIC , useremail_cte AS (
# MAGIC   SELECT 
# MAGIC     UserEmail
# MAGIC     , UserName
# MAGIC     , MIN(DateFirstSeenInGCOB) AS DateFirstSeenInGCOB
# MAGIC     , MAX(DateLastSeenInGCOB) AS DateLastSeenInGCOB
# MAGIC   FROM ranked_username_cte
# MAGIC   GROUP BY UserEmail, UserName
# MAGIC )
# MAGIC
# MAGIC , ranked_useremail_cte AS (
# MAGIC   SELECT
# MAGIC     UserName
# MAGIC     , FIRST_VALUE(UserEmail) OVER (PARTITION BY UserName ORDER BY DateLastSeenInGCOB DESC) AS UserEmail
# MAGIC     , DateFirstSeenInGCOB
# MAGIC     , DateLastSeenInGCOB
# MAGIC   FROM useremail_cte
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   UserEmail
# MAGIC   , UserName
# MAGIC   , MIN(DateFirstSeenInGCOB) AS DateFirstSeenInGCOB
# MAGIC   , MAX(DateLastSeenInGCOB) AS DateLastSeenInGCOB
# MAGIC FROM ranked_useremail_cte
# MAGIC GROUP BY UserEmail, UserName

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS radar.userlist

# COMMAND ----------

spark.sql('select * from userlist').write.mode('overwrite').saveAsTable('radar.userlist')
