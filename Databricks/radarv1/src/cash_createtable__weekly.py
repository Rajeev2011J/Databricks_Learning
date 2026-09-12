# Databricks notebook source
# DBTITLE 1,connections
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

# DBTITLE 1,get siebel data
# df_AR = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_ar_hist/1/data/', format='delta')
# df_REL_X_AR = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_ar_hist/1/data/', format='delta')
# df_REL_X_REL = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_rel_hist/1/data/', format='delta')
# df_NP = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_np_hist/1/data/', format='delta')
# df_ORG = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_org_hist/1/data/', format='delta')

# COMMAND ----------

# DBTITLE 1,get current siebel data
# from datetime import datetime

# df_REL_X_AR.filter(df_REL_X_AR.edl_valid_to_dts > datetime.today()).createOrReplaceTempView('rel_x_ar')
# # df_AR_ATTR.filter(df_REL_X_AR.edl_valid_to_dts > datetime.today()).createOrReplaceTempView('ar_attr')
# # edl_valid_to_dts = date('9999-12-31')
# df_REL_X_REL.filter(df_REL_X_REL.edl_valid_to_dts > datetime.today()).createOrReplaceTempView('rel_x_rel')
# df_AR.filter(df_AR.edl_valid_to_dts > datetime.today()).createOrReplaceTempView('ar')
# df_ORG.filter(df_ORG.edl_valid_to_dts > datetime.today()).createOrReplaceTempView('org')
# df_NP.filter(df_NP.edl_valid_to_dts > datetime.today()).createOrReplaceTempView('np')

# COMMAND ----------

# DBTITLE 1,get all siebel data
spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_ar_hist/1/data/', format='delta').createOrReplaceTempView('ar')
spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_ar_hist/3/data/', format='delta').createOrReplaceTempView('rel_x_ar')
spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_rel_hist/2/data/', format='delta').createOrReplaceTempView('rel_x_rel')
spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_np_hist/2/data/', format='delta').createOrReplaceTempView('np')
spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_org_hist/2/data/', format='delta').createOrReplaceTempView('org')

# COMMAND ----------

# MAGIC %md
# MAGIC # Debit card info

# COMMAND ----------

# DBTITLE 1,connect to blob storage vasaasawrfecreportdev and load list of agents from Qiwen
# # connect to blob storage vasaasawrfecreportdev and load list of agents from Qiwen
# ReadStorage = 'vasaasawrfecreportdev'

# spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
# spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
# spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
# spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
# spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# spark.read.csv('abfss://adhoc-upload@vasaasawrfecreportdev.dfs.core.windows.net/cash/agents_jul25.csv', header=True, sep=';').createOrReplaceTempView('agents_csv')

# COMMAND ----------

# DBTITLE 1,get debit cards data
import re

item = 'DebitCard_Data_DS'

# latest version
path = f'abfss://coj-card-mgmt-data@edlcorestdeuprod0001.dfs.core.windows.net/{item}/'

files = dbutils.fs.ls(path)
version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

path_file = f'{path}{version}/data/'
files = dbutils.fs.ls(path_file)
load_date = max(file.path.split('ACT_DT=')[1][:8] for file in files if 'ACT_DT=' in file.path)

spark.read.parquet(f'{path_file}/ACT_DT={load_date}/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# DBTITLE 1,mto_cards_and_siebel_data
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW mto_cards_and_siebel_data AS
# MAGIC
# MAGIC WITH agent_name_cte AS (
# MAGIC   SELECT
# MAGIC     rel_id
# MAGIC     , FULL_GVN_NM
# MAGIC     , NM_INL
# MAGIC     , SURNM_PFX
# MAGIC     , SURNM
# MAGIC     , ROW_NUMBER() OVER (PARTITION BY rel_id ORDER BY edl_valid_from_dts DESC) AS rn
# MAGIC   FROM np
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.AR_AC_IBAN -- iban
# MAGIC   -- , t1.AR_SRC_STM_ID
# MAGIC   , t1.ADMN_GGM_CODE
# MAGIC   , t1.AR_LCS_TP_GGM_CODE -- lifecycle status
# MAGIC   , t1.AR_ST_GGM_CODE -- status of product
# MAGIC   , t1.AR_PRIM_ORG_REL_ID -- primary rel id
# MAGIC   , t1.AR_NO AS AR_NO_CCA -- AR VAN Company Current account
# MAGIC
# MAGIC   , t2.DC_CARD_NO -- debit card number
# MAGIC   , t2.DC_EXP_D -- expiration date
# MAGIC   , t2.DC_PREV_CARD_SEQ_NO -- previous card sequence number
# MAGIC   , t2.DC_HLDR_DOB -- card holder date of birth
# MAGIC   , t2.DC_PAN_SEQ_NO -- PAN sequence number --> it helps distinguish between different cards linked to the same underlying account
# MAGIC   , t2.DC_REPLC_CARD_PAN_PSN -- same as above but for the replacement card
# MAGIC   , t2.DC_SEQ_NO -- sequence number, same as PAN??
# MAGIC   , t2.DC_EMBS_LN_1_TXT -- text 1st line
# MAGIC   , t2.DC_EMBS_LN_2_TXT -- text 2nd line
# MAGIC
# MAGIC   , t2.DC_RQS_DTS -- card request date
# MAGIC   , t2.DC_RET_FM_PCSR_DTS -- physical card return by provider
# MAGIC   , t2.DC_ACTVN_TMS -- card activation date
# MAGIC   , t2.DC_RPLCMT_DTS -- card replacement date
# MAGIC   , t2.DC_CLS_TMS -- card close date
# MAGIC
# MAGIC   , t3.AR_NO AS AR_NO_CAA
# MAGIC   , t3.ar_prim_np_rel_id AS REL_ID_AGENT
# MAGIC
# MAGIC   , CONCAT_WS(' ', coalesce(t4.FULL_GVN_NM, t4.NM_INL), t4.SURNM_PFX, t4.SURNM) AS Agent_LE_name
# MAGIC
# MAGIC   -- , t4.IKB_NO -- create duplication
# MAGIC   -- , t4.REL_ST_TP_GGM_CODE -- Relation Status Type Code, create duplication
# MAGIC   -- , t4.BNK_CODE AS BNK_CODE_AGENT -- useless
# MAGIC
# MAGIC
# MAGIC FROM ar t1
# MAGIC LEFT JOIN DebitCard_Data_DS t2 ON t1.ar_ac_iban = t2.DC_HLDR_IBAN
# MAGIC -- DC_AGRM_NO -- Debit Card Agreement Number
# MAGIC LEFT JOIN ar t3 ON t2.DC_AGRM_NO = t3.AR_NO AND t3.admn_ggm_code = 1201 -- CAA Cards Administratie
# MAGIC LEFT JOIN agent_name_cte t4 ON t3.ar_prim_np_rel_id = t4.REL_ID AND rn = 1
# MAGIC WHERE t1.admn_ggm_code = '01' -- Administration Code
# MAGIC   AND t1.AR_PRIM_ORG_REL_ID IN ('000000116434369', '000000117165225', '000000115897236', '116434369', '117165225', '115897236') -- MTO scope --> should be REF data or dynamic
# MAGIC
# MAGIC   AND t2.DC_SEQ_NO IS NOT NULL -- useless otherwise
# MAGIC
# MAGIC /*
# MAGIC   Tried but not working:
# MAGIC     - contact person from ar or org
# MAGIC     - third party dataset cdf_ggm_org_t3p_rl_hist --> no access
# MAGIC     - from gcds keystore, no mapping for agents rel_id
# MAGIC     - this also does not work, as the relationship is not enforced so not good data quality
# MAGIC         t6.kvk_no
# MAGIC
# MAGIC         LEFT JOIN rel_x_rel t5 ON t4.REL_ID = t5.rlshp_to_rel_id -- REL_ID_AGENT = Relationship To Relation Id
# MAGIC         LEFT JOIN org t6 ON t5.rlshp_fm_rel_id = t6.REL_ID -- Relationship From Relation Id
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC # Load cash transactions and denomination from Brinks, Geldmaat and Rok

# COMMAND ----------

# DBTITLE 1,define dates
date_format = 'yyyy-MM-dd'
start_date_old = '2023-01-01'
end_date_old = '2024-06-01'
start_date_new = '2024-06-01'
start_date_newer = '2024-08-03'

# COMMAND ----------

from pyspark.sql.functions import lit, col, to_date
import re

# # load old path files (01-05/2024) and adjust columns
# df_old_path = (
#     spark.read.parquet('abfss://ca2unnested@edlcorestdeuprod0001.dfs.core.windows.net/CA2DGVMAINDATA/0/data/loaddate=*/*.parquet')
#     .filter((col('DCP_TXDR_CNT_END_DTS') >= to_date(lit(start_date_old), date_format)) &
#              (col('DCP_TXDR_CNT_END_DTS') < to_date(lit(end_date_old), date_format)))
#     .withColumnRenamed('LOADDATETIME', 'loadDateTime')
#     .withColumn('EDL_ACT_DTS', lit(None))
#     .withColumn('EDL_CHANGE_TYPE', lit(None))
#     .withColumn('EDL_LOAD_DTS', lit(None))
#     .drop('EDL_LOAD_DTS_UTC')
# )

# new path files from 1/6/24 onwards
df_new_path = spark.read.parquet('abfss://ca2-cash-data@edlcorestdeuprod0001.dfs.core.windows.net/CA2DGVMAINDATA/1/data/EDL_LOAD_DTS=*/*.parquet') \
    .filter(col('DCP_TXDR_CNT_END_DTS') >= to_date(lit(start_date_new), date_format))

# newer path files from 3/8/24 onwards
# get the most recent version available in gdppath
path = f'abfss://ca2-cash-data@edlcorestdeuprod0001.dfs.core.windows.net/CA2DGVMAINDATA/'
files = dbutils.fs.ls(path)
version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
df_newer_path = spark.read.parquet(f'abfss://ca2-cash-data@edlcorestdeuprod0001.dfs.core.windows.net/CA2DGVMAINDATA/{version}/data/ACT_DT=*/*.parquet') \
    .filter(col('DCP_TXDR_CNT_END_DTS') >= to_date(lit(start_date_newer), date_format))

# union all data frames after aligning columns
selected_columns = ['CASH_PCSR_NM', 'DCP_CST_AC_IBAN', 'DCP_TXDR_CNT_END_DTS', 'DCP_CST_LO_ZIP_CODE', 
                    'DCP_TXDR_TOT_COIN_AND_NOTE_AMT', 'DCP_CST_NM', 'DCP_TXDR_SEALB_ID', 'DCP_CST_AC_IBAN', 
                    'DCP_TXDR_INR_PBLS_EV_ID']

df_brinks_main = (
    df_new_path.select(*selected_columns)
    .union(df_newer_path.select(*selected_columns))
)

# create temporary view
df_brinks_main.createOrReplaceTempView('brinks_main')

# COMMAND ----------

# DBTITLE 1,brinks_denominations
from pyspark.sql.functions import lit, col, to_date
import re

# # Load and transform old path files (01-05/2024)
# df_old_path = (
#     # spark.read.parquet('abfss://ca2unnested@edlcorestdeuprod0001.dfs.core.windows.net/CA2DGVDENOMDATA/0/data/loaddate=2024-*/*.parquet')
#     spark.read.parquet('abfss://ca2unnested@edlcorestdeuprod0001.dfs.core.windows.net/CA2DGVDENOMDATA/0/data/loaddate=*/*.parquet')
#     .filter((col('LOADDATETIME') >= to_date(lit(start_date_old), date_format)) & 
#             (col('LOADDATETIME') < to_date(lit(end_date_old), date_format)))
#     .withColumnRenamed('LOADDATETIME', 'loadDateTime')
#     .withColumn('EDL_ACT_DTS', lit(None))
#     .withColumn('EDL_CHANGE_TYPE', lit(None))
#     .withColumn('EDL_LOAD_DTS', lit(None))
#     .drop('EDL_LOAD_DTS_UTC')
# )

# Load new path files (from 1/6/24 onwards)
df_new_path = (
    spark.read.parquet('abfss://ca2-cash-data@edlcorestdeuprod0001.dfs.core.windows.net/CA2DGVDENOMDATA/1/data/EDL_LOAD_DTS=*/*.parquet')
    .filter(col('loadDateTime') >= to_date(lit(start_date_new), date_format))
)

# Load newer path files (from 3/8/24 onwards)
# get the most recent version available in gdp
path = f'abfss://ca2-cash-data@edlcorestdeuprod0001.dfs.core.windows.net/CA2DGVDENOMDATA/'
files = dbutils.fs.ls(path)
version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
df_newer_path = (
    spark.read.parquet(f'abfss://ca2-cash-data@edlcorestdeuprod0001.dfs.core.windows.net/CA2DGVDENOMDATA/{version}/data/ACT_DT=*/*.parquet')
    .filter(col('loadDateTime') >= to_date(lit(start_date_newer), date_format))
)

# Union all data frames after aligning columns
selected_columns = ['DCP_TXDR_ITM_DNMN_CODE', 'DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE',
                    'DCP_TXDR_INR_PBLS_EV_ID', 'DCP_TXDR_ITM_QLY_DSC']

df_brinks_denominations = (
    # df_old_path.select(*selected_columns)
    df_new_path.select(*selected_columns)
    .union(df_newer_path.select(*selected_columns))
)

# Create temporary view
df_brinks_denominations.createOrReplaceTempView('brinks_denominations')

# COMMAND ----------

# DBTITLE 1,geldmaat_main
from pyspark.sql.functions import lit, col, to_date
import re

selected_columns = [
    'GM_CASH_TXN_TRMNL_NM', 'GM_CASH_TXN_CST_AC_IBAN', 'GM_CASH_TXN_ISSUR_REF_TXT', 'GM_CASH_TXN_STRT_DTS',
    'GM_CASH_TXN_TRMNL_LO_ZIP_CODE', 'GM_CASH_TXN_CCY_AMT', 'GM_CASH_TXN_DTL_EV_INR_PBLS_ID', 'GM_SVC_TP_CODE'
]

# # Load and transform old path files (01-05/2024)
# df_old_path = (
#     spark.read.parquet('abfss://ca2unnested@edlcorestdeuprod0001.dfs.core.windows.net/CA2ATMMAINDATA/0/data/loaddate=*/*.parquet')
#     .filter((col('GM_CASH_TXN_STRT_DTS') >= to_date(lit(start_date_old), date_format)) & 
#             (col('GM_CASH_TXN_STRT_DTS') < to_date(lit(end_date_old), date_format)))
#     .withColumnRenamed('LOADDATETIME', 'loadDateTime')
#     .withColumn('EDL_ACT_DTS', lit(None))
#     .withColumn('EDL_CHANGE_TYPE', lit(None))
#     .withColumn('EDL_LOAD_DTS', lit(None))
#     .drop('EDL_LOAD_DTS_UTC')
# )

# Load new path files (from 1/6/24 onwards)
df_new_path = (
    spark.read.parquet('abfss://ca2-cash-data@edlcorestdeuprod0001.dfs.core.windows.net/CA2ATMMAINDATA/1/data/EDL_LOAD_DTS=*/*.parquet')
    .filter(col('GM_CASH_TXN_STRT_DTS') >= to_date(lit(start_date_new), date_format))
)

# Load newer path files (from 3/8/24 onwards)
# get the most recent version available in gdp
path = f'abfss://ca2-cash-data@edlcorestdeuprod0001.dfs.core.windows.net/CA2ATMMAINDATA/'
files = dbutils.fs.ls(path)
version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
df_newer_path = (
    spark.read.parquet(f'abfss://ca2-cash-data@edlcorestdeuprod0001.dfs.core.windows.net/CA2ATMMAINDATA/{version}/data/ACT_DT=*/*.parquet')
    .filter(col('GM_CASH_TXN_STRT_DTS') >= to_date(lit(start_date_newer), date_format))
    .select(*selected_columns)
)

# Combine all data frames
df_geldmaat_main = (
    # df_old_path.select(*selected_columns)
    df_new_path.select(*selected_columns)
    .union(df_newer_path.select(*selected_columns))
)

# Create temporary view
df_geldmaat_main.createOrReplaceTempView('geldmaat_main')

# COMMAND ----------

# DBTITLE 1,geldmaat_denominations
from pyspark.sql.functions import lit, col, to_date
import re

selected_columns = [
    'GM_CASH_TXN_DTL_EV_INR_PBLS_ID', 'GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT', 'GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE', 'GM_BNKNOTE_DEP_TXN_QLY_CATE_CODE'
]

# # Load and transform old path files (01-05/2024)
# df_old_path = (
#     spark.read.parquet('abfss://ca2unnested@edlcorestdeuprod0001.dfs.core.windows.net/CA2ATMDENOMDATA/0/data/loaddate=*/*.parquet')
#     .filter((col('LOADDATETIME') >= to_date(lit(start_date_old), date_format)) & 
#             (col('LOADDATETIME') < to_date(lit(end_date_old), date_format)))
#     .withColumnRenamed('LOADDATETIME', 'loadDateTime')
#     .withColumn('EDL_ACT_DTS', lit(None))
#     .withColumn('EDL_CHANGE_TYPE', lit(None))
#     .withColumn('EDL_LOAD_DTS', lit(None))
#     .drop('EDL_LOAD_DTS_UTC')
# )

# Load new path files (from 1/6/24 onwards)
df_new_path = (
    spark.read.parquet('abfss://ca2-cash-data@edlcorestdeuprod0001.dfs.core.windows.net/CA2ATMDENOMDATA/1/data/EDL_LOAD_DTS=*/*.parquet')
    .filter(col('loadDateTime') >= to_date(lit(start_date_new), date_format))
)

# Load newer path files (from 3/8/24 onwards)
# get the most recent version available in gdp
path = f'abfss://ca2-cash-data@edlcorestdeuprod0001.dfs.core.windows.net/CA2ATMDENOMDATA/'
files = dbutils.fs.ls(path)
version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
df_newer_path = (
    spark.read.parquet(f'abfss://ca2-cash-data@edlcorestdeuprod0001.dfs.core.windows.net/CA2ATMDENOMDATA/{version}/data/ACT_DT=*/*.parquet')
    .filter(col('loadDateTime') >= to_date(lit(start_date_newer), date_format))
    .select(*selected_columns)
)

# Combine all data frames
df_geldmaat_denominations = (
    # df_old_path.select(*selected_columns)
    df_new_path.select(*selected_columns)
    .union(df_newer_path.select(*selected_columns))
)

# Create temporary view
df_geldmaat_denominations.createOrReplaceTempView('geldmaat_denominations')

# COMMAND ----------

# DBTITLE 1,rok_transactions
# RaboOmniKassa - load all from 2023 (included) and onwards

# https://rabobank.collibra.com/asset/222509e7-a240-4899-8654-160e61453dab

from pyspark.sql.functions import to_date
import re

# get the most recent version available in gdp
path = f'abfss://rok-accounting@edlcorestdeuprod0001.dfs.core.windows.net/transactions/'
files = dbutils.fs.ls(path)
version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

spark.read.parquet(f'abfss://rok-accounting@edlcorestdeuprod0001.dfs.core.windows.net/transactions/{version}/data/EDL_LOAD_DT=*/*.parquet') \
    .filter(to_date(col('ROK_TXN_DTS').substr(1, 10), 'yyyy-MM-dd') >= to_date(lit(start_date_new), 'yyyy-MM-dd')) \
    .createOrReplaceTempView('rok_transactions')

# COMMAND ----------

# DBTITLE 1,rok_deposits
# RaboOmniKassa - load all from 2023 (included) and onwards

from pyspark.sql.functions import to_date
import re

# get the most recent version available in gdp
path = f'abfss://rok-accounting@edlcorestdeuprod0001.dfs.core.windows.net/deposits/'
files = dbutils.fs.ls(path)
version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

spark.read.parquet(f'abfss://rok-accounting@edlcorestdeuprod0001.dfs.core.windows.net/deposits/{version}/data/EDL_LOAD_DT=*/*.parquet') \
    .filter(to_date(col('ROK_SLBD_DEP_DTS').substr(1, 10), 'yyyy-MM-dd') >= to_date(lit(start_date_new), 'yyyy-MM-dd')) \
    .createOrReplaceTempView('rok_deposits')

# COMMAND ----------

# DBTITLE 1,rok_denominations
# RaboOmniKassa - load all from 2023 (included) and onwards

# denominations: https://rabobank.collibra.com/asset/2bdadf5e-fff1-474f-953f-3d6e7c5328c0

from pyspark.sql.functions import to_date
import re

# get the most recent version available in gdp
path = f'abfss://rok-accounting@edlcorestdeuprod0001.dfs.core.windows.net/deposits_denominations/'
files = dbutils.fs.ls(path)
version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

spark.read.parquet(f'abfss://rok-accounting@edlcorestdeuprod0001.dfs.core.windows.net/deposits_denominations/{version}/data/EDL_LOAD_DT=*/*.parquet') \
    .filter(to_date(col('EDL_LOAD_DTS').substr(1, 10), 'yyyy-MM-dd') >= to_date(lit(start_date_new), 'yyyy-MM-dd')) \
    .createOrReplaceTempView('rok_denominations')

# COMMAND ----------

# MAGIC %md
# MAGIC # Define cash deposits and withdraws

# COMMAND ----------

# DBTITLE 1,cash_deposits
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cash_deposits AS
# MAGIC
# MAGIC -- BRINKS DEPOSITS
# MAGIC SELECT DISTINCT
# MAGIC     -- t3.IKB_NO
# MAGIC     -- , t3.REL_ID
# MAGIC     -- , t3.BNK_CODE
# MAGIC     t4.UniqueGcobid
# MAGIC     -- , t3.ORG_LGL_NM
# MAGIC     , t1.CASH_PCSR_NM AS Source
# MAGIC     , t1.DCP_CST_AC_IBAN AS BankAccountNumber -- Direct Cash Processing Customer Account IBAN
# MAGIC     , NULL AS PasNnr
# MAGIC     , NULL AS PasName
# MAGIC     , t1.DCP_TXDR_CNT_END_DTS AS TransactionDate -- Direct Cash Processing Transaction Detail Record Counting End Datetime
# MAGIC     , t1.DCP_CST_LO_ZIP_CODE AS `Location`
# MAGIC     , t1.DCP_TXDR_TOT_COIN_AND_NOTE_AMT AS Value
# MAGIC     , t1.DCP_CST_NM AS TX_Message1
# MAGIC     , CAST(NULL AS STRING) AS TX_Message2
# MAGIC     , t1.DCP_TXDR_SEALB_ID AS strippedBrinksSealbagId
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'E500' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Bilj500
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'E200' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Bilj200
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'E100' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Bilj100
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'E50' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Bilj50
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'E20' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Bilj20
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'E10' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Bilj10
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'E5' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Bilj5
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'C200' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Munt2
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'C100' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Munt1
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'C50' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Munt50Ct
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'C20' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Munt20Ct
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'C10' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Munt10Ct
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'C5' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Munt5Ct
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'C2' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Munt2Ct
# MAGIC     , SUM(CASE WHEN t2.DCP_TXDR_ITM_DNMN_CODE = 'C1' THEN t2.DCP_TXDR_ITM_NBR_OF_CNT_COIN_OR_NOTE END) AS Munt1Ct
# MAGIC FROM brinks_main AS t1
# MAGIC LEFT JOIN brinks_denominations AS t2 ON t1.DCP_TXDR_INR_PBLS_EV_ID = t2.DCP_TXDR_INR_PBLS_EV_ID
# MAGIC JOIN radar.tx_wr_iban t3 ON t1.DCP_CST_AC_IBAN = t3.IBAN
# MAGIC LEFT JOIN radar.clients t4 ON t3.UniqueGcobid = t4.UniqueGcobId
# MAGIC WHERE t2.DCP_TXDR_ITM_QLY_DSC NOT IN ('Counterfeit', 'Rejected')
# MAGIC     AND t4.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC GROUP BY
# MAGIC     -- t3.IKB_NO
# MAGIC     -- , t3.REL_ID
# MAGIC     -- , t3.BNK_CODE
# MAGIC     t4.UniqueGcobid
# MAGIC     , t3.gcid
# MAGIC     -- , t3.ORG_LGL_NM
# MAGIC     , t1.CASH_PCSR_NM
# MAGIC     , t1.DCP_CST_AC_IBAN
# MAGIC     , t1.DCP_TXDR_CNT_END_DTS
# MAGIC     , t1.DCP_CST_LO_ZIP_CODE
# MAGIC     , t1.DCP_TXDR_TOT_COIN_AND_NOTE_AMT
# MAGIC     , t1.DCP_CST_NM
# MAGIC     , t1.DCP_TXDR_SEALB_ID
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC -- GELDMAAT DEPOSITS
# MAGIC SELECT DISTINCT
# MAGIC     -- t3.IKB_NO
# MAGIC     -- , t3.REL_ID
# MAGIC     -- , t3.BNK_CODE
# MAGIC     t4.UniqueGcobId
# MAGIC     -- , t3.ORG_LGL_NM
# MAGIC     , 'Geldmaat' AS Source
# MAGIC     , CAST(t1.GM_CASH_TXN_CST_AC_IBAN AS STRING) AS BankAccountNumber
# MAGIC     -- , SUBSTRING(t1.GM_CASH_TXN_ISSUR_REF_TXT, 7, 3) AS PasNnr
# MAGIC     , t1.GM_CASH_TXN_ISSUR_REF_TXT AS PasNnr
# MAGIC     , NULL AS PasName
# MAGIC     , t1.GM_CASH_TXN_STRT_DTS AS TransactionDate
# MAGIC     , t1.GM_CASH_TXN_TRMNL_LO_ZIP_CODE AS `Location`
# MAGIC     , t1.GM_CASH_TXN_CCY_AMT AS Value
# MAGIC     , CAST(NULL AS STRING) AS TX_Message1
# MAGIC     , CAST(NULL AS STRING) AS TX_Message2
# MAGIC     , NULL AS strippedBrinksSealbagId
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '500' THEN GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE END) AS Bilj500
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '200' THEN GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE END) AS Bilj200
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '100' THEN GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE END) AS Bilj100
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '50' THEN GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE END) AS Bilj50
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '20' THEN GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE END) AS Bilj20
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '10' THEN GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE END) AS Bilj10
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '5' THEN GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE END) AS Bilj5
# MAGIC     , NULL AS Munt2
# MAGIC     , NULL AS Munt1
# MAGIC     , NULL AS Munt50Ct
# MAGIC     , NULL AS Munt20Ct
# MAGIC     , NULL AS Munt10Ct
# MAGIC     , NULL AS Munt5Ct
# MAGIC     , NULL AS Munt2Ct
# MAGIC     , NULL AS Munt1Ct
# MAGIC FROM geldmaat_main AS t1
# MAGIC LEFT JOIN geldmaat_denominations AS t2 ON t1.GM_CASH_TXN_DTL_EV_INR_PBLS_ID = t2.GM_CASH_TXN_DTL_EV_INR_PBLS_ID
# MAGIC JOIN radar.tx_wr_iban AS t3 ON t1.GM_CASH_TXN_CST_AC_IBAN = t3.IBAN
# MAGIC LEFT JOIN radar.clients t4 ON t3.UniqueGcobid = t4.UniqueGcobid
# MAGIC WHERE t1.GM_SVC_TP_CODE IN ('cd')
# MAGIC     AND t2.GM_BNKNOTE_DEP_TXN_QLY_CATE_CODE <> '2'
# MAGIC     AND t1.GM_CASH_TXN_TRMNL_LO_ZIP_CODE IS NOT NULL
# MAGIC     AND t4.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC GROUP BY
# MAGIC     -- t3.IKB_NO
# MAGIC     -- , t3.REL_ID
# MAGIC     -- , t3.BNK_CODE
# MAGIC     t4.UniqueGcobid
# MAGIC     , t3.gcid
# MAGIC     -- , t3.ORG_LGL_NM
# MAGIC     , t1.GM_CASH_TXN_TRMNL_NM
# MAGIC     , t1.GM_CASH_TXN_CST_AC_IBAN
# MAGIC     , t1.GM_CASH_TXN_ISSUR_REF_TXT
# MAGIC     , t1.GM_CASH_TXN_STRT_DTS
# MAGIC     , t1.GM_CASH_TXN_TRMNL_LO_ZIP_CODE
# MAGIC     , t1.GM_CASH_TXN_CCY_AMT
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     -- t3.IKB_NO
# MAGIC     -- , t3.REL_ID
# MAGIC     -- , t3.BNK_CODE
# MAGIC     t4.UniqueGcobId
# MAGIC     -- , t3.ORG_LGL_NM
# MAGIC     , 'Rabo Smart Pay' AS Source
# MAGIC     , CAST(t1.ROK_TXN_PY_OUT_IBAN AS STRING) AS BankAccountNumber
# MAGIC     , NULL AS PasNnr
# MAGIC     , NULL AS PasName
# MAGIC     , t1.ROK_TXN_DTS AS TransactionDate
# MAGIC     , t2b.ROK_SLBD_MCHN_LO_PST_CODE AS `Location`
# MAGIC     , t1.ROK_TXN_CCY_AMT AS Value
# MAGIC     , NULL AS TX_Message1
# MAGIC     , NULL AS TX_Message2
# MAGIC     , t1.ROK_TXN_SEALB_ID AS strippedBrinksSealbagId
# MAGIC     , SUM(t2.ROK_SLBD_500_EUR_CNT_NBR_OF_BNK_NOTE) AS Bilj500
# MAGIC     , SUM(t2.ROK_SLBD_200_EUR_CNT_NBR_OF_BNK_NOTE) AS Bilj200
# MAGIC     , SUM(t2.ROK_SLBD_100_EUR_CNT_NBR_OF_BNK_NOTE) AS Bilj100
# MAGIC     , SUM(t2.ROK_SLBD_50_EUR_CNT_NBR_OF_BNK_NOTE) AS Bilj50
# MAGIC     , SUM(t2.ROK_SLBD_20_EUR_CNT_NBR_OF_BNK_NOTE) AS Bilj20
# MAGIC     , SUM(t2.ROK_SLBD_10_EUR_CNT_NBR_OF_BNK_NOTE) AS Bilj10
# MAGIC     , SUM(t2.ROK_SLBD_5_EUR_CNT_NBR_OF_BNK_NOTE) AS Bilj5
# MAGIC     , NULL AS Munt2
# MAGIC     , NULL AS Munt1
# MAGIC     , NULL AS Munt50Ct
# MAGIC     , NULL AS Munt20Ct
# MAGIC     , NULL AS Munt10Ct
# MAGIC     , NULL AS Munt5Ct
# MAGIC     , NULL AS Munt2Ct
# MAGIC     , NULL AS Munt1Ct
# MAGIC FROM rok_transactions t1
# MAGIC JOIN rok_deposits t2b ON t1.ROK_TXN_SEALB_ID = t2b.ROK_SLBD_SEALB_ID
# MAGIC LEFT JOIN rok_denominations t2 ON t2b.ROK_SLBD_SEALB_ID = t2.ROK_SLBD_SEALB_ID
# MAGIC JOIN radar.tx_wr_iban t3 ON t1.ROK_TXN_PY_OUT_IBAN = t3.IBAN
# MAGIC LEFT JOIN radar.clients t4 ON t3.UniqueGcobid = t4.UniqueGcobid
# MAGIC WHERE t1.rok_pymt_brnd_code = 'DEPOSIT' -- NOT NEEDED
# MAGIC     AND t4.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC GROUP BY
# MAGIC     -- t3.IKB_NO
# MAGIC     -- , t3.REL_ID
# MAGIC     -- , t3.BNK_CODE
# MAGIC     t4.UniqueGcobid
# MAGIC     , t3.gcid
# MAGIC     -- , t3.ORG_LGL_NM
# MAGIC     , t1.ROK_TXN_PY_OUT_IBAN
# MAGIC     , t1.ROK_TXN_DTS
# MAGIC     , t2b.ROK_SLBD_MCHN_LO_PST_CODE
# MAGIC     , t1.ROK_TXN_CCY_AMT
# MAGIC     , t1.ROK_TXN_SEALB_ID
# MAGIC
# MAGIC /*
# MAGIC
# MAGIC -- REQUEST THE LIST OF IBAN FROM EBX DEALING WITH CASH TRANSACTIONS OVER CNA!!
# MAGIC
# MAGIC UNION
# MAGIC -- CNA Deposits
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     t2.IKB_NO
# MAGIC     , t2.REL_ID
# MAGIC     -- , t2.BNK_CODE
# MAGIC     , t2.UniqueGcobid
# MAGIC     , t2.ORG_LGL_NM
# MAGIC     , 'CNA' AS Source
# MAGIC     , t1.ACCT_ID AS BankAccountNumber -- Direct Cash Processing Customer Account IBAN
# MAGIC     , NULL AS PasNnr
# MAGIC     , NULL AS PasName
# MAGIC     , CAST(t1.BOOKG_DT AS DATE) AS TransactionDate -- Direct Cash Processing Transaction Detail Record Counting End Datetime
# MAGIC     , 'Unknown' AS `Location`
# MAGIC     , CAST(t1.bookg_AMT AS FLOAT) AS Value
# MAGIC     , t1.RMT_INF_USTRD1 AS TX_Message1
# MAGIC     , t1.RMT_INF_USTRD1_UC  AS TX_Message2
# MAGIC     , CASE WHEN regexp_extract(t1.RMT_INF_USTRD1, '\\d{12}', 0) = '' THEN NULL
# MAGIC       ELSE regexp_extract(t1.RMT_INF_USTRD1, '\\d{12}', 0)
# MAGIC   END AS strippedBrinksSealbagId
# MAGIC     , NULL AS `Bilj500`
# MAGIC     , NULL AS `Bilj200`
# MAGIC     , NULL AS `Bilj100`
# MAGIC     , NULL AS `Bilj50`
# MAGIC     , NULL AS `Bilj20`
# MAGIC     , NULL AS `Bilj10`
# MAGIC     , NULL AS `Bilj5`
# MAGIC     , NULL AS Munt2
# MAGIC     , NULL AS Munt1
# MAGIC     , NULL AS Munt50Ct
# MAGIC     , NULL AS Munt20Ct
# MAGIC     , NULL AS Munt10Ct
# MAGIC     , NULL AS Munt5Ct
# MAGIC     , NULL AS Munt2Ct
# MAGIC     , NULL AS Munt1Ct
# MAGIC from CNA as t1
# MAGIC JOIN radar.tx_wr_iban AS t2 ON t1.ACCT_ID = t2.IBAN
# MAGIC WHERE t1.BOOKG_CDT_DBT_IND = 'CRDT'
# MAGIC     */

# COMMAND ----------

# DBTITLE 1,cash_withdrawals
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cash_withdrawals AS
# MAGIC
# MAGIC -- GELDMAAT WITHDRAWALS
# MAGIC SELECT DISTINCT
# MAGIC     t4.UniqueGcobId
# MAGIC     -- , t3.REL_ID
# MAGIC     -- , t3.BNK_CODE
# MAGIC     -- , t3.gcid AS GCDSID
# MAGIC     -- , t3.ORG_LGL_NM
# MAGIC     , 'Geldmaat' AS Source
# MAGIC     , t1.GM_CASH_TXN_CST_AC_IBAN AS BankAccountNumber
# MAGIC     -- , SUBSTRING(t1.GM_CASH_TXN_ISSUR_REF_TXT, 7, 3) AS PasNnr
# MAGIC     , t1.GM_CASH_TXN_ISSUR_REF_TXT AS PasNnr
# MAGIC     , NULL AS PasName
# MAGIC     , t1.GM_CASH_TXN_STRT_DTS AS TransactionDate
# MAGIC     , t1.GM_CASH_TXN_TRMNL_LO_ZIP_CODE AS `Location`
# MAGIC     , t1.GM_CASH_TXN_CCY_AMT AS Value
# MAGIC     , CAST(NULL AS STRING) AS TX_Message1
# MAGIC     , CAST(NULL AS STRING) AS TX_Message2
# MAGIC     , NULL AS strippedBrinksSealbagId
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '500' THEN t2.GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE/2 END) AS Bilj500
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '200' THEN t2.GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE/2 END) AS Bilj200
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '100' THEN t2.GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE/2 END) AS Bilj100
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '50' THEN t2.GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE/2 END) AS Bilj50
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '20' THEN t2.GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE/2 END) AS Bilj20
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '10' THEN t2.GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE/2 END) AS Bilj10
# MAGIC     , SUM(CASE WHEN t2.GM_BNKNOTE_TXN_DNMN_VAL_CCY_AMT = '5' THEN t2.GM_BNKNOTE_TXN_CNT_NBR_OF_BNKNOTE/2 END) AS Bilj5
# MAGIC     , NULL AS Munt2
# MAGIC     , NULL AS Munt1
# MAGIC     , NULL AS Munt50Ct
# MAGIC     , NULL AS Munt20Ct
# MAGIC     , NULL AS Munt10Ct
# MAGIC     , NULL AS Munt5Ct
# MAGIC     , NULL AS Munt2Ct
# MAGIC     , NULL AS Munt1Ct
# MAGIC FROM geldmaat_main AS t1
# MAGIC LEFT JOIN geldmaat_denominations AS t2 ON t1.GM_CASH_TXN_DTL_EV_INR_PBLS_ID = t2.GM_CASH_TXN_DTL_EV_INR_PBLS_ID
# MAGIC JOIN radar.tx_wr_iban AS t3 ON t1.GM_CASH_TXN_CST_AC_IBAN = t3.IBAN
# MAGIC LEFT JOIN radar.clients t4 ON t3.UniqueGcobid = t4.UniqueGcobid
# MAGIC WHERE t1.GM_SVC_TP_CODE IN ('cw')
# MAGIC     AND t4.SourceSystemReference = 'GCOB_LegalEntity' 
# MAGIC GROUP BY
# MAGIC     t4.UniqueGcobid
# MAGIC     -- , t3.REL_ID
# MAGIC     -- , t3.BNK_CODE
# MAGIC     -- , t3.ORG_LGL_NM
# MAGIC     , t1.GM_CASH_TXN_TRMNL_NM
# MAGIC     , t1.GM_CASH_TXN_CST_AC_IBAN
# MAGIC     , t1.GM_CASH_TXN_ISSUR_REF_TXT
# MAGIC     , t1.GM_CASH_TXN_STRT_DTS
# MAGIC     , t1.GM_CASH_TXN_TRMNL_LO_ZIP_CODE
# MAGIC     , t1.GM_CASH_TXN_CCY_AMT
# MAGIC     -- , t3.gcid

# COMMAND ----------

# MAGIC %md
# MAGIC # Final cash table

# COMMAND ----------

# DBTITLE 1,cash
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cash AS
# MAGIC SELECT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.DepositWithdrawal
# MAGIC   , t1.Source
# MAGIC   , t1.BankAccountNumber
# MAGIC   , t1.PasNnr
# MAGIC   , coalesce(t3.Pasholder, t2.DC_EMBS_LN_1_TXT) AS PasName
# MAGIC   , t1.TransactionDate
# MAGIC   , 'Netherlands' AS Country
# MAGIC   , CONCAT(t1.`Location`, ', The Netherlands') AS `Location`
# MAGIC   , ROUND(CAST(REPLACE(t1.Value, ',', '.') AS DECIMAL(10,2)), 2) AS Value
# MAGIC   , CAST(t1.TX_Message1 AS STRING) AS TX_Message1
# MAGIC   , CAST(t1.TX_Message2 AS STRING) AS TX_Message2
# MAGIC   , t1.strippedBrinksSealbagId AS SealbagNrs
# MAGIC   , CAST(COALESCE(t1.Bilj500, 0) AS INT)   AS Bilj500
# MAGIC   , CAST(COALESCE(t1.Bilj200, 0) AS INT)   AS Bilj200
# MAGIC   , CAST(COALESCE(t1.Bilj100, 0) AS INT)   AS Bilj100
# MAGIC   , CAST(COALESCE(t1.Bilj50, 0) AS INT)    AS Bilj50
# MAGIC   , CAST(COALESCE(t1.Bilj20, 0) AS INT)    AS Bilj20
# MAGIC   , CAST(COALESCE(t1.Bilj10, 0) AS INT)    AS Bilj10
# MAGIC   , CAST(COALESCE(t1.Bilj5, 0) AS INT)     AS Bilj5
# MAGIC   , CAST(COALESCE(t1.Munt2, 0) AS INT)     AS Munt2
# MAGIC   , CAST(COALESCE(t1.Munt1, 0) AS INT)     AS Munt1
# MAGIC   , CAST(COALESCE(t1.Munt50Ct, 0) AS INT)  AS Munt50Ct
# MAGIC   , CAST(COALESCE(t1.Munt20Ct, 0) AS INT)  AS Munt20Ct
# MAGIC   , CAST(COALESCE(t1.Munt10Ct, 0) AS INT)  AS Munt10Ct
# MAGIC   , CAST(COALESCE(t1.Munt5Ct, 0) AS INT)   AS Munt5Ct
# MAGIC   , CAST(COALESCE(t1.Munt2Ct, 0) AS INT)   AS Munt2Ct
# MAGIC   , CAST(COALESCE(t1.Munt1Ct, 0) AS INT)   AS Munt1Ct
# MAGIC   
# MAGIC   , coalesce(t3.AgentName, t2.Agent_LE_name) AS Agent_LE_name
# MAGIC   , t2.REL_ID_AGENT
# MAGIC
# MAGIC   , t3.CoC AS CoCnr --> KVK number linked to the shop
# MAGIC
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     'Deposit' AS DepositWithdrawal
# MAGIC     , dep.*
# MAGIC   FROM cash_deposits AS dep
# MAGIC   UNION
# MAGIC   SELECT
# MAGIC     'Withdrawal' AS DepositWithdrawal
# MAGIC     , wit.*
# MAGIC   FROM cash_withdrawals AS wit
# MAGIC ) t1
# MAGIC
# MAGIC LEFT JOIN mto_cards_and_siebel_data t2 ON t1.BankAccountNumber = t2.AR_AC_IBAN
# MAGIC   -- AND CAST(t1.PasNnr AS STRING) = CAST(t2.DC_SEQ_NO AS STRING) -- changed due to XXX in PasNnr
# MAGIC   AND SUBSTRING(t1.PasNnr, 7, 3) = CAST(t2.DC_SEQ_NO AS STRING)
# MAGIC   AND t1.TransactionDate > coalesce(t2.DC_ACTVN_TMS, t2.DC_RQS_DTS)  -- transaction after the activation datetime and before the replacement/closing datetime (below) --> t1.DC_RQS_DTS is never null
# MAGIC   AND t1.TransactionDate < coalesce(t2.DC_RPLCMT_DTS, t2.DC_CLS_TMS, current_date()) -- transaction date before replacement, closing datetime or current date
# MAGIC
# MAGIC
# MAGIC LEFT JOIN radar.agents_csv t3 ON t1.BankAccountNumber = t3.IBAN
# MAGIC   -- AND t1.PasNnr = t3.PassNumber -- changed due to XXX in PasNnr
# MAGIC   AND SUBSTRING(t1.PasNnr, 7, 3)  = t3.PassNumber
# MAGIC
# MAGIC
# MAGIC /*
# MAGIC OLD JOINS:
# MAGIC LEFT JOIN bc_cashagents t3 ON SUBSTRING(t1.BankAccountNumber, -9) = t3.Rekeningnr AND t1.PasNnr = t3.Pas_Volgnummer
# MAGIC
# MAGIC LEFT JOIN bc_cashagents_jul24 t4 ON SUBSTRING(t1.BankAccountNumber, -9) = t4.Rekeningnr
# MAGIC   -- AND t1.PasNnr = t4.`Pas Volgnummer`
# MAGIC   AND TRIM(LEADING '0' FROM t1.PasNnr) = TRIM(LEADING '0' FROM t4.`Pas Volgnummer`)
# MAGIC   AND to_date(t1.TransactionDate, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'') > to_date(t4.Aanvraagdatum, 'dd-MM-yyyy')
# MAGIC   AND to_date(t1.TransactionDate, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'') < COALESCE(to_date(t4.`Vermoedelijke Vervangdatum`, 'dd-MM-yyyy'), current_date())
# MAGIC
# MAGIC LEFT JOIN bc_cashagents_oct24 t5
# MAGIC   ON TRIM(LEADING '0' FROM t1.PasNnr) = TRIM(LEADING '0' FROM t5.`PAS _ VOLGNUMMER`)
# MAGIC   AND t1.PasNnr = t5.PAS_NUMMER
# MAGIC   AND (t5.PAS_NUMMER IS NOT NULL AND t5.`PAS _ VOLGNUMMER` IS NOT NULL)
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.cash

# COMMAND ----------

spark.sql('SELECT * FROM cash').write.mode('overwrite').saveAsTable('radar.cash')
