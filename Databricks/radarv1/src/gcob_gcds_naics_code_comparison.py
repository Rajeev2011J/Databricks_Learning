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

from pyspark.sql import SparkSession
# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
    , 'client_Client'
    , 'client_PartyRole'
    , 'client_Products'
    , 'client_OnboardedLocations'
]

for item in gcds_tables:
    # get the most recent file available in gdp
    path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4602/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4602/data/LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcds_'+ item)

# COMMAND ----------

# DBTITLE 1,Primary Nacics Code which are in GCDS but not in GCOB
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcdsNaicsCodeNotInGCOB AS 
# MAGIC WITH base AS (
# MAGIC     SELECT DISTINCT
# MAGIC         t2.KeyStore_value AS gcobid,
# MAGIC         t1.gcid,
# MAGIC         t1.Primary_NAICS AS primaryNaicsGCDS,
# MAGIC         t4.Code AS gcobNaicsCode,
# MAGIC         t3.FullLegalName as gcobClientName,
# MAGIC         t3.GlobalKYCPortfolioNew
# MAGIC     FROM gcds_client_client as t1
# MAGIC     INNER JOIN gcds_client_KeyStoreKey t2 
# MAGIC         ON t1.gcid = t2.gcid
# MAGIC     INNER JOIN radar.clients t3 
# MAGIC         ON t2.KeyStore_value = t3.uniquegcobid
# MAGIC     LEFT JOIN radar.SiraNAICS t4 
# MAGIC         ON t3.UniqueGcobId = t4.UniqueGcobId
# MAGIC     LEFT JOIN gcds_client_PartyRole t5 ON t1.gcid = t5.gcid
# MAGIC     WHERE
# MAGIC         --t3.GlobalReportingRegion = 'E&A'
# MAGIC          t3.ClientLifeCycleName = 'Client'
# MAGIC         --AND t3.GlobalKYCPortfolioNew NOT IN ('London Markets')
# MAGIC         --AND t3.FIHubIndicator_Derived = 'Corp'
# MAGIC         AND t2.keystore_type IN ('GCOBID')
# MAGIC         AND t2.status = 'Active'
# MAGIC )
# MAGIC
# MAGIC SELECT *,
# MAGIC COUNT(gcobNaicsCode) OVER (PARTITION BY gcobid) AS naicsCountInGCOB
# MAGIC FROM base b
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM base b2
# MAGIC     WHERE b2.gcobid = b.gcobid
# MAGIC       AND b2.gcobNaicsCode = b.primaryNaicsGCDS
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Primary Nacics Code which are in GCDS and also in GCOB
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcdsNaicsCodeInGCOB AS 
# MAGIC WITH base AS (
# MAGIC     SELECT DISTINCT
# MAGIC         t2.KeyStore_value AS gcobid,
# MAGIC         t1.gcid,
# MAGIC         t1.Primary_NAICS AS primaryNaicsGCDS,
# MAGIC         t4.Code AS gcobNaicsCode,
# MAGIC         t3.FullLegalName AS gcobClientName,
# MAGIC         t3.GlobalKYCPortfolioNew
# MAGIC         --COUNT(distinct t4.Code) OVER (PARTITION BY t2.KeyStore_value) AS naicsCountInGCOB
# MAGIC     FROM gcds_client_client t1
# MAGIC     INNER JOIN gcds_client_KeyStoreKey t2 
# MAGIC         ON t1.gcid = t2.gcid
# MAGIC     INNER JOIN radar.clients t3 
# MAGIC         ON t2.KeyStore_value = t3.uniquegcobid
# MAGIC     LEFT JOIN radar.SiraNAICS t4 
# MAGIC         ON t3.UniqueGcobId = t4.UniqueGcobId
# MAGIC     LEFT JOIN gcds_client_PartyRole t5 
# MAGIC         ON t1.gcid = t5.gcid
# MAGIC     WHERE --t3.GlobalReportingRegion = 'E&A'
# MAGIC        t3.ClientLifeCycleName = 'Client'
# MAGIC      -- AND t3.GlobalKYCPortfolioNew NOT IN ('London Markets')
# MAGIC       --AND t3.FIHubIndicator_Derived = 'Corp'
# MAGIC       AND t2.keystore_type = 'GCOBID'
# MAGIC       AND t2.status = 'Active'
# MAGIC       --AND t3.uniquegcobid = '2323'
# MAGIC )
# MAGIC
# MAGIC SELECT *,
# MAGIC COUNT(gcobNaicsCode) OVER (PARTITION BY gcobid) AS naicsCountInGCOB
# MAGIC FROM base b
# MAGIC WHERE  EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM base b2
# MAGIC     WHERE b2.gcobid = b.gcobid
# MAGIC       AND b2.gcobNaicsCode = b.primaryNaicsGCDS
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.gcdsNaicsCodeNotInGCOB;
# MAGIC DROP TABLE IF EXISTS radar.gcdsNaicsCodeInGCOB;

# COMMAND ----------

spark.sql('SELECT * FROM gcdsNaicsCodeNotInGCOB').write.mode('overwrite').saveAsTable('radar.gcdsNaicsCodeNotInGCOB')
spark.sql('SELECT * FROM gcdsNaicsCodeInGCOB').write.mode('overwrite').saveAsTable('radar.gcdsNaicsCodeInGCOB')
