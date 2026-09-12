# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal:
# MAGIC to get all MTO agents and their card identifiers - and potentially data about their connected phone-shops or other companies

# COMMAND ----------

import os
import datetime

# COMMAND ----------

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

# DBTITLE 1,datetime variable for file
today = (datetime.datetime.now() - datetime.timedelta(0)).strftime('%Y%m%d')

# COMMAND ----------

# DBTITLE 1,SET up GDP connect variables


# COMMAND ----------

# DBTITLE 1,Get Siebel data
df_AR = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_ar_hist/1/data/', format='delta') #.select(

df_REL_X_AR = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_ar_hist/1/data/', format='delta')
df_REL_X_REL = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_rel_hist/1/data/', format='delta')
#
df_NP = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_np_hist/1/data/', format='delta')
df_ORG = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_org_hist/1/data/', format='delta')


# COMMAND ----------

# DBTITLE 1,Get Debit Cards data
df_cards = spark.read.load(f'abfss://coj-card-mgmt-data@edlcorestdeuprod0001.dfs.core.windows.net/DebitCard_Data_DS/2/data/ACT_DT={today}/*.parquet' ,format = 'parquet')
df_cards.createOrReplaceTempView('cards')

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# DBTITLE 1,get current version of data
df_REL_X_AR.filter(df_REL_X_AR.edl_valid_to_dts > datetime.today()).createOrReplaceTempView('rel_x_ar')
# df_AR_ATTR.filter(df_REL_X_AR.edl_valid_to_dts > datetime.today()).createOrReplaceTempView('ar_attr')
# edl_valid_to_dts = date('9999-12-31')
df_REL_X_REL.filter(df_REL_X_REL.edl_valid_to_dts > datetime.today()).createOrReplaceTempView('rel_x_rel')
df_AR.filter(df_AR.edl_valid_to_dts > datetime.today()).createOrReplaceTempView('ar')
df_ORG.filter(df_ORG.edl_valid_to_dts > datetime.today()).createOrReplaceTempView('org')
df_NP.filter(df_NP.edl_valid_to_dts > datetime.today()).createOrReplaceTempView('np')

# COMMAND ----------

# DBTITLE 1,get products from MTO's and their numbers
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW MTO_CARDS_AND_SIEBEL_DATA AS
# MAGIC
# MAGIC WITH MTO_IBAN_CTE AS (
# MAGIC   select distinct ar_ac_iban
# MAGIC   , ar_prim_org_rel_id
# MAGIC   , AR_SRC_STM_ID
# MAGIC   , AR_LCS_TP_GGM_CODE
# MAGIC   , AR_PRIM_ORG_REL_ID
# MAGIC   , AR_NO
# MAGIC   , ADMN_GGM_CODE
# MAGIC   , AR_ST_GGM_CODE
# MAGIC
# MAGIC   FROM ar
# MAGIC   WHERE admn_ggm_code = '01'
# MAGIC   AND AR_PRIM_ORG_REL_ID IN ( '000000116434369',  '000000117165225' , '000000115897236' ,'116434369',  '117165225' , '115897236') --MTO Scope. Should be REF data or Dynamic
# MAGIC )
# MAGIC
# MAGIC
# MAGIC select T4.AR_AC_IBAN,
# MAGIC T4.AR_SRC_STM_ID,
# MAGIC T4.ADMN_GGM_CODE ,
# MAGIC T4.AR_LCS_TP_GGM_CODE, -- lifecycle status
# MAGIC T4.AR_ST_GGM_CODE, -- Status of product
# MAGIC T4.AR_PRIM_ORG_REL_ID,
# MAGIC t4.AR_NO AS AR_NO_CCA, --AR VAN Company Current account
# MAGIC t5.DC_SEQ_NO,
# MAGIC t5.DC_CARD_NO,
# MAGIC t5.DC_EXP_D,
# MAGIC t5.DC_PREV_CARD_SEQ_NO,
# MAGIC t5.DC_EMBS_LN_1_TXT,
# MAGIC t5.DC_HLDR_DOB,
# MAGIC t2.AR_NO AS AR_NO_CAA,
# MAGIC t1.IKB_NO,
# MAGIC t1.REL_ID AS REL_ID_AGENT,
# MAGIC t1.REL_ST_TP_GGM_CODE,
# MAGIC t1.FULL_GVN_NM,
# MAGIC t1.NM_INL,
# MAGIC t1.SURNM_PFX,
# MAGIC t1.SURNM,
# MAGIC T1.BNK_CODE AS BNK_CODE_AGENT
# MAGIC
# MAGIC from MTO_IBAN_CTE as t4
# MAGIC left join cards as t5 on t4.ar_ac_iban = t5.DC_HLDR_IBAN
# MAGIC left join ar as t2 on t5.DC_AGRM_NO = t2.AR_NO and t2.admn_ggm_code = 1201 --CAA Cards Administratie
# MAGIC left join np as t1 on t2.ar_prim_np_rel_id = t1.REL_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from MTO_CARDS_AND_SIEBEL_DATA

# COMMAND ----------

spark.sql('select * from MTO_CARDS_AND_SIEBEL_DATA').write.mode('overwrite').saveAsTable('radar.MTO_CARDS_AND_SIEBEL_DATA')

# COMMAND ----------

# DBTITLE 1,Search for Phoneshops of which these card holders have parent relationship
# MAGIC %sql
# MAGIC --select 
# MAGIC --  t2.org_cmrcl_nm
# MAGIC --, t2.org_lgl_nm
# MAGIC --, t1.* 
# MAGIC --from rel_x_rel t1
# MAGIC --
# MAGIC --left join org as t2 on t1.RLSHP_FM_REL_ID = t2.REL_ID
# MAGIC --
# MAGIC --where t1.RLSHP_TO_REL_ID IN (select distinct REL_ID_AGENT from MTO_CARDS_AND_SIEBEL_DATA)
# MAGIC
