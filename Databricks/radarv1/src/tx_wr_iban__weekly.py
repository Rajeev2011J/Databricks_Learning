# Databricks notebook source
# MAGIC %md
# MAGIC - Siebel --> Select all IBAN and rel_id for bnk_code 3000, 3400, 3508 + select also the primary rel_id to an arragement
# MAGIC - Via GCDS --> get keystore_key for rel_id / gcid / gcobid
# MAGIC - Inner join with UniqueGcobId radar.clients

# COMMAND ----------

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

# DBTITLE 1,gcds_keystore
from datetime import datetime, timedelta
import re

gcds_tables = ['client_KeyStoreKey', 'client_Client'] # client_PartyRole

for item in gcds_tables:
    # get the most recent version available in gdp
    path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

    # get the most recent file available in gdp
    path_file = f'{path}{version}/data/'
    files = dbutils.fs.ls(path_file)
    load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

    spark.read.parquet(f'{path_file}LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# DBTITLE 1,siebel_rel_x_ar to get rel_id and IBAN
# siebel is a delta table, get all valid records
columns_to_load = [
    'ar_ac_iban', 'ar_ac_ccy_code', 'edl_valid_to_dts',
    'ar_del_f', 'rel_id'
]

# manually adding version for siebel, otherwise referencing higher numbers (eg 311224)
version = '4'

df_siebel_rel_x_ar = spark.read.format('delta').load(
    f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_ar_hist/{version}/data/'
).select(*columns_to_load).filter("ar_ac_iban IS NOT NULL") # edl_valid_to_dts = '9999-12-31 00:00:00.0000000' AND ar_del_f = 'N' AND --> TAKE ALL THE IBAN, EVEN IF THEY ARE EXPIRED, THEY MIGHT MATCH OLDER TRANSACTIONS

df_siebel_rel_x_ar.createOrReplaceTempView('siebel_rel_x_ar')

# COMMAND ----------

# DBTITLE 1,siebel_org_hist to get bnk_code
# siebel table to get the bnk_code
columns_to_load = [
    'rel_id', 'bnk_code', 'edl_valid_to_dts', 'del_f'
]

# manually adding version for siebel, otherwise referencing higher numbers (eg 241224)
version = '2'

df_siebel_cdf_ggm_org_hist = spark.read.format('delta').load(
    f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_org_hist/{version}/data/'
).select(*columns_to_load) # .filter("edl_valid_to_dts = '9999-12-31 00:00:00.0000000' AND ar_del_f = 'N'")

df_siebel_cdf_ggm_org_hist.createOrReplaceTempView('siebel_org_hist')

# COMMAND ----------

# DBTITLE 1,siebel_ar_hist to get the primary rel_id to each IBAN
# # take ar_prim_org_rel_id from cdf_ggm_ar_hist to get only the primary rel_id wrt a product/iban

# # siebel table to get the bnk_code
# columns_to_load = [
#     'ar_no', 'del_f', 'edl_valid_to_dts', 'ar_prim_org_rel_id', 'ar_ac_iban'
# ]

# # manually adding version for siebel, otherwise referencing higher numbers (eg 241224)
# version = '1'

# df_cdf_ggm_ar_hist = spark.read.format('delta').load(
#     f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_ar_hist/{version}/data/'
# ).select(*columns_to_load).filter("ar_ac_iban IS NOT NULL AND ar_prim_org_rel_id IS NOT NULL")
#  # .filter("edl_valid_to_dts = '9999-12-31 00:00:00.0000000' AND ar_del_f = 'N'")

# df_cdf_ggm_ar_hist.createOrReplaceTempView('siebel_ar_hist')

# COMMAND ----------

# DBTITLE 1,tx_wr_iban_np
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tx_wr_iban_np AS
# MAGIC
# MAGIC -- select all iban and rel_id for bnk_code WR + selecting primary rel_id to an arragement
# MAGIC WITH siebel_scope_cte AS (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.rel_id
# MAGIC     , t1.ar_ac_iban
# MAGIC     -- , t3.ar_prim_org_rel_id 
# MAGIC   FROM siebel_rel_x_ar t1
# MAGIC   LEFT JOIN siebel_org_hist t2 ON t1.rel_id = t2.rel_id
# MAGIC   -- LEFT JOIN siebel_ar_hist t3 ON t1.ar_ac_iban = t3.ar_ac_iban
# MAGIC   WHERE t2.bnk_code IN (3000, 3400, 3508) -- only selecting these bnk_code -- WAIT FOR ROB!!!!!!!!
# MAGIC     AND t1.ar_ac_iban IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC -- select rel_id / gcid match only for NP
# MAGIC , gcds_keystore_cte AS (
# MAGIC   SELECT DISTINCT
# MAGIC     CAST(t1.keystore_value AS STRING) AS rel_id
# MAGIC     , t1.gcid
# MAGIC   FROM client_keystorekey t1
# MAGIC   INNER JOIN client_client t2 ON t1.gcid = t2.gcid
# MAGIC   WHERE t2.party_type = 'Natural Person'
# MAGIC     AND (t1.keystore_Type = 'SIEBEL' OR t1.keystore_Type = 'SBWRR')
# MAGIC     AND (t1.status = 'Active') -- only status Active otherwise no match with gcobid, ONLY IN THIS CASE FOR NP!!!!
# MAGIC     -- OR status IS NULL)
# MAGIC     AND t2.`Global_CO-location` IN (
# MAGIC       'Utrecht'
# MAGIC       , 'Netherlands'
# MAGIC       , 'Germany'
# MAGIC       , 'France'
# MAGIC       , 'Belgium'
# MAGIC       , 'London'
# MAGIC       , 'Italy'
# MAGIC       , 'Spain'
# MAGIC       , 'Ireland'
# MAGIC       , 'Kenya'
# MAGIC       , 'Singapore'
# MAGIC       , 'Malaysia'
# MAGIC       , 'Turkey'
# MAGIC       , 'Beijing'
# MAGIC       , '中国 (中华人民共和国)'
# MAGIC       , 'India'
# MAGIC       , 'China'
# MAGIC       , 'SINGAPORE'
# MAGIC       , 'Hong Kong'
# MAGIC       , 'Hong Kong S.A.R.'
# MAGIC       , 'TURKEY'
# MAGIC       , 'Shanghai'
# MAGIC       , 'Rural'
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.ar_ac_iban AS IBAN
# MAGIC   , t2.gcid
# MAGIC   -- , t1.ar_prim_org_rel_id --> rel_id IS ALWAYS EQUAL TO THE PRIMARY REL_ID!!
# MAGIC   , CASE 
# MAGIC       WHEN t2.gcid = 1214624 THEN 'NP_790' -- HARDCODED BECAUSE THIS IS THE ONLY ONE KNOWN FOR NOW (2025) AND KEYSTORE FOR GCOB NP ARE NOT IN GCDS FOR NOW!!!!
# MAGIC     END AS UniqueGcobId
# MAGIC FROM siebel_scope_cte t1
# MAGIC LEFT JOIN gcds_keystore_cte t2 ON t2.rel_id = t1.rel_id
# MAGIC WHERE t2.gcid = 1214624 -- HARDCODED BECAUSE THIS IS THE ONLY ONE KNOWN FOR NOW (2025) AND KEYSTORE FOR GCOB NP ARE NOT IN GCDS FOR NOW!!!!

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tx_wr_iban_le AS
# MAGIC
# MAGIC -- select all iban and rel_id for bnk_code WR + selecting primary rel_id to an arragement
# MAGIC WITH siebel_scope_cte AS (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.rel_id
# MAGIC     , t1.ar_ac_iban
# MAGIC     -- , t3.ar_prim_org_rel_id 
# MAGIC   FROM siebel_rel_x_ar t1
# MAGIC   LEFT JOIN siebel_org_hist t2 ON t1.rel_id = t2.rel_id
# MAGIC   -- LEFT JOIN siebel_ar_hist t3 ON t1.ar_ac_iban = t3.ar_ac_iban
# MAGIC   WHERE t2.bnk_code IN (3000, 3400, 3508) -- only selecting these bnk_code -- WAIT FOR ROB!!!!!!!!
# MAGIC     AND t1.ar_ac_iban IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC , gcds_keystore_cte AS (
# MAGIC     SELECT DISTINCT
# MAGIC       CAST(keystore_value AS STRING) AS rel_id
# MAGIC       , gcid
# MAGIC     FROM client_keystorekey
# MAGIC     WHERE 
# MAGIC       (keystore_type = 'SBWRR') -- keystore_type = 'SIEBEL'
# MAGIC       -- AND
# MAGIC       -- (status = 'Active')
# MAGIC       --  OR status IS NULL) -- THEN JOIN ON RADAR.CLIENTS SO DOES NOT MATTER HERE + THERE IS NO OTHER IDENTIFIER SIMILAR TO REL_ID, SO NO NEED TO SPECIFY SIEBEL AS keystore_type
# MAGIC   )
# MAGIC
# MAGIC , siebel_gcob_scope_cte AS (
# MAGIC     SELECT DISTINCT
# MAGIC       t1.ar_ac_iban
# MAGIC       , t2.gcid
# MAGIC       , t3.GcobId AS UniqueGcobId
# MAGIC     FROM siebel_scope_cte t1
# MAGIC     INNER JOIN gcds_keystore_cte t2 ON t1.rel_id = t2.rel_id -- GET GCID FROM RELID
# MAGIC     LEFT JOIN (
# MAGIC       SELECT
# MAGIC         gcid
# MAGIC         , keystore_value AS GcobId -- only gcobid for LE on GCDS
# MAGIC       FROM client_keystorekey
# MAGIC       WHERE keystore_type = 'GCOBID'
# MAGIC     ) t3 ON t2.gcid = t3.gcid
# MAGIC     WHERE t3.GcobId IS NOT NULL
# MAGIC   )
# MAGIC
# MAGIC   , iban_deduplication_cte AS (
# MAGIC       SELECT
# MAGIC         ar_ac_iban
# MAGIC         , gcid
# MAGIC         , UniqueGcobId
# MAGIC         , RANK() OVER (PARTITION BY ar_ac_iban ORDER BY UniqueGcobId DESC) AS rn --> avoiding duplication of IBAN/GcobId
# MAGIC       FROM siebel_gcob_scope_cte
# MAGIC   )
# MAGIC
# MAGIC SELECT
# MAGIC   t1.ar_ac_iban AS IBAN
# MAGIC   , t1.gcid
# MAGIC   , t1.UniqueGcobId
# MAGIC FROM iban_deduplication_cte t1
# MAGIC INNER JOIN radar.clients t2 ON t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC WHERE t1.rn = 1
# MAGIC   -- AND t2.UniqueGcobId NOT IN ('24178', '15630') -- EXCLUDING RABOBANK AND A TURKISH BANK

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tx_wr_iban AS
# MAGIC
# MAGIC SELECT * FROM tx_wr_iban_np
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT * FROM tx_wr_iban_le

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.tx_wr_iban

# COMMAND ----------

spark.sql('SELECT * FROM tx_wr_iban').write.mode('overwrite').saveAsTable('radar.tx_wr_iban')
