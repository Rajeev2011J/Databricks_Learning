# Databricks notebook source
# MAGIC %md
# MAGIC ## Overview Flexcube Settlement Accounts Traffic
# MAGIC Goal: to export for the previous calender month all the Flexcube-related Transactions in CNA, so that Flexcube team can match the owner of the loan vs. who is repaying the loan. 
# MAGIC
# MAGIC
# MAGIC ###### <span style="color:orange">Request for Delivery:</span>
# MAGIC 1.	Can the fields be present in the following order - ar_ac_iban, ACCT_ID, BOOKG_AMT_NMRC, ACCT_CCY, BOOKG_DT, CTPTY_AGT_BIC, CTPTY_NM, RMT_INF_USTRD1, RMT_INF_USTRD1_UC, BOOKG_CDT_DBT_IND, YEAR_MONTH, BTCH_BOOKG.
# MAGIC 2.	Also if the amount (BOOKG_AMT_NMRC ) can be provided with decimals included
# MAGIC
# MAGIC
# MAGIC ###### <span style="color:blue">Flow of the logic:</span>
# MAGIC 1.  Connect to GDP sources
# MAGIC     - Siebel Arrangements (delta table): get all products (cmrcl_pd_tp_code) from 711-717
# MAGIC 2.  Connect CNA last month: using wildcard to select a full month of data, get the year-month combination of prev calendar month. 
# MAGIC 3.  Products and bookings both together
# MAGIC 4.  Export file into Storage place
# MAGIC 5.  Input from Samual Lazarus: In addition to the below email, could you also please check if possible to exclude VCF accounts from your list. Those would be the ones with Rekening Code – 714, 715 and 716.
# MAGIC

# COMMAND ----------

# simple command to start as test
import pandas as pd
import os

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

ReadStorage

# COMMAND ----------


from datetime import datetime
currentDate = datetime.today().strftime('%Y%m%d')
currentYear = datetime.today().strftime('%Y')
currentMonth = datetime.today().strftime('%m')
currentDay = datetime.today().strftime('%d')

print(currentDate, currentYear, currentMonth, currentDay)

# COMMAND ----------

# MAGIC %md
# MAGIC <mark color = red>**Enter Year and month manually**</mark>

# COMMAND ----------

year = '2026'

month = '06' # e.g. '01' for jan

# COMMAND ----------

ReadStorage


# code for version 2:
df_AR = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_ar_hist/2/data/', format='delta')

df_AR = df_AR.filter(df_AR.cmrcl_pd_tp_code.isin(['0711','0712','0713','0717'])).filter(df_AR.edl_valid_to_dts > datetime.today() )
     # removed '0714','0715','0716',

#df_AR.limit(10).toPandas().head(10)
# df_AR.show()
df_AR = df_AR.dropDuplicates(subset = ['ar_ac_iban'])
df_AR.createOrReplaceTempView('ar')

# COMMAND ----------

display(df_AR.limit(3))

# COMMAND ----------

# df REL_X_AR
df_REL_X_AR = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_ar_hist/3/data/', format='delta')
df_REL_X_AR = df_REL_X_AR.filter(df_REL_X_AR.edl_valid_to_dts > datetime.today() )
df_REL_X_AR = df_REL_X_AR.dropDuplicates(subset = ['ar_ac_iban'])
df_REL_X_AR.createOrReplaceTempView('rel_x_ar')

# COMMAND ----------

# df ORG
df_ORG = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_org_hist/2/data/', format='delta')
df_ORG = df_ORG.filter(df_ORG.edl_valid_to_dts > datetime.today() )
df_ORG.createOrReplaceTempView('org')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Siebel_SEL AS
# MAGIC (
# MAGIC
# MAGIC    Select t1.REL_ID
# MAGIC    --, t2.ar_ac_iban
# MAGIC    , t3.ar_ac_iban --as ar_iban
# MAGIC    , t3.cmrcl_pd_tp_code
# MAGIC    , t3.admn_ggm_code
# MAGIC    , t1.bnk_code
# MAGIC    , t3.ar_prim_org_rel_id
# MAGIC    , t3.edl_valid_to_dts
# MAGIC
# MAGIC    FROM ORG t1
# MAGIC   --LEFT JOIN rel_x_ar t2 on t1.rel_id = t2.rel_id
# MAGIC   INNER JOIN ar t3 on t1.rel_id = t3.ar_prim_org_rel_id
# MAGIC
# MAGIC   where t1.bnk_code = 3508
# MAGIC
# MAGIC )
# MAGIC

# COMMAND ----------



# COMMAND ----------

# check last 10 files of CNA and see if they are of a recent date.
dbutils.fs.ls('abfss://cna@edlcorestdeuprod0001.dfs.core.windows.net/CAC_ACG_ENTR/1/data/')[-10:]

# COMMAND ----------

# CNA is organized as daily append/increment files
df_CNA = spark.read.load('abfss://cna@edlcorestdeuprod0001.dfs.core.windows.net/CAC_ACG_ENTR/1/data/ACT_DT='+year+month+'*/*.parquet', format='parquet').select(
    "ACCT_ID", "BOOKG_AMT_NMRC", "ACCT_CCY", "BOOKG_DT", "CTPTY_AGT_BIC", "CTPTY_NM", "RMT_INF_USTRD1", "RMT_INF_USTRD1_UC", "BOOKG_CDT_DBT_IND", "YEAR_MONTH", "BTCH_BOOKG")

df_CNA.createOrReplaceTempView('CNA')
#display(df_CNA.show(5))

#df_CNA.limit(10).toPandas().head(3)

# COMMAND ----------

month

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Result AS
# MAGIC
# MAGIC   SELECT t1.*
# MAGIC   , t2.*
# MAGIC   from Siebel_SEL AS t1
# MAGIC   left join CNA AS t2 on t1.ar_ac_iban = t2.ACCT_ID

# COMMAND ----------

df_result = spark.sql('select * from Result').select("ar_ac_iban", "ACCT_ID", "BOOKG_AMT_NMRC", "ACCT_CCY", "BOOKG_DT", "CTPTY_AGT_BIC", "CTPTY_NM", "RMT_INF_USTRD1", "RMT_INF_USTRD1_UC", "BOOKG_CDT_DBT_IND", "YEAR_MONTH", "BTCH_BOOKG")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from Result

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Result limit 50

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS FlexcubeSettlementExports

# COMMAND ----------

df_result.write.mode('overwrite').saveAsTable(f'FlexcubeSettlementExports.Export{year}{month}')

# COMMAND ----------

# DBTITLE 1,yes- but account that don't connect on the IBAN anymore. so no missed transactions


