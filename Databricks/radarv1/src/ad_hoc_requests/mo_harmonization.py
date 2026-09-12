# Databricks notebook source
# MAGIC %md
# MAGIC # Mid office harmonization

# COMMAND ----------

# MAGIC %md
# MAGIC Total number of products per portfolio for E&A:
# MAGIC - Cash pools / current accounts --> Siebel data (cdf_ggm_cmrcl_pd_hist) --> total number
# MAGIC - Loans --> Flexcube (loan_NLU)
# MAGIC
# MAGIC - Guarantees --> Flexcube (letterOfCreditOrGuaranteeContract_NLU) --? rest in doga
# MAGIC - Derivatives --> Possibly Murex, but could also be Siebel (??)

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

from datetime import datetime, timedelta
path = f'abfss://core-cbt@edlcorestdeuprod0001.dfs.core.windows.net/loan_NLU/212/data/'
files = dbutils.fs.ls(path)
# get the most recent file available in gdp and create temp view gcds_keystore
load_date = max(file.path.split('BUSINESS_DTS=')[1][:8] for file in files if 'BUSINESS_DTS=' in file.path)
spark.read.parquet(f'{path}BUSINESS_DTS={load_date}*/*.parquet').createOrReplaceTempView('loan_NLU')

# COMMAND ----------

# DBTITLE 1,cdf_ggm_rel_x_ar_hist
# siebel is a delta table, get all valid records
columns_to_load = [
    'ar_ac_iban', 'ar_ac_ccy_code', 'edl_valid_to_dts',
    'ar_del_f', 'rel_id'
]

# manually adding version for siebel, otherwise referencing higher numbers (eg 311224)
version = '4'

df_cdf_ggm_rel_x_ar_hist = spark.read.format('delta').load(
    f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_ar_hist/{version}/data/'
).filter("edl_valid_to_dts = '9999-12-31 00:00:00.0000000' AND ar_del_f = 'N' AND ar_ac_iban IS NOT NULL") # THIS CONSIDER ONLY ACTIVE IBAN/CLIENTS
# .select(*columns_to_load).

df_cdf_ggm_rel_x_ar_hist.createOrReplaceTempView('cdf_ggm_rel_x_ar_hist')

# COMMAND ----------

# DBTITLE 1,cdf_ggm_rel_x_rel_hist
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! PROBABLY NOT NEEDED !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# manually adding version for siebel, otherwise referencing higher numbers (eg 311224)
version = '2'

df_cdf_ggm_rel_x_rel_hist = spark.read.format('delta').load(
    f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_rel_hist/{version}/data/'
).filter("edl_valid_to_dts = '9999-12-31 00:00:00.0000000' AND del_f = 'N'") # THIS CONSIDER ONLY ACTIVE IBAN/CLIENTS
# .select(*columns_to_load).

df_cdf_ggm_rel_x_rel_hist.createOrReplaceTempView('cdf_ggm_rel_x_rel_hist')

# COMMAND ----------

# DBTITLE 1,cdf_ggm_cmrcl_pd_hist
# # siebel is a delta table, get all valid records
# columns_to_load = [
#     'ar_ac_iban', 'ar_ac_ccy_code', 'edl_valid_to_dts',
#     'ar_del_f', 'rel_id'
# ]

# # manually adding version for siebel, otherwise referencing higher numbers (eg 311224)
# version = '2'

# df_cdf_ggm_cmrcl_pd_hist = spark.read.format('delta').load(
#     f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_cmrcl_pd_hist/{version}/data/'
# ).filter("edl_valid_to_dts = '9999-12-31 00:00:00.0000000' AND del_f = 'N'") # .select(*columns_to_load)

# df_cdf_ggm_cmrcl_pd_hist.createOrReplaceTempView('cdf_ggm_cmrcl_pd_hist')

# COMMAND ----------

from datetime import datetime, timedelta
path = f'abfss://core-cbt@edlcorestdeuprod0001.dfs.core.windows.net/letterOfCreditOrGuaranteeContract_NLU/213/data/'
files = dbutils.fs.ls(path)
# get the most recent file available in gdp and create temp view gcds_keystore
load_date = max(file.path.split('BUSINESS_DTS=')[1][:8] for file in files if 'BUSINESS_DTS=' in file.path)
spark.read.parquet(f'{path}BUSINESS_DTS={load_date}*/*.parquet').createOrReplaceTempView('guarantees')

# COMMAND ----------

# MAGIC %md
# MAGIC # Loans E&A

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW loan AS
# MAGIC
# MAGIC WITH exploded_data AS (
# MAGIC   SELECT
# MAGIC     customerNumber
# MAGIC     , facilityCodeSerial
# MAGIC     , loanReference
# MAGIC     , loanCode
# MAGIC     , posexplode(positionBalances_schedules_schedulePeriods_calculationPeriods_endDate) AS (pos, endDate)
# MAGIC     , posexplode(positionBalances_schedules_schedulePeriods_calculationPeriods_startDate) AS (pos_start, startDate)
# MAGIC     , posexplode(positionBalances_schedules_schedulePeriods_calculationPeriods_basisAmount) AS (pos_basis, basisAmount)
# MAGIC     , positionBalances_amounts_currency[0] AS currency -- get first element of currency and repeat it accross rows
# MAGIC     , tenor_bookDate
# MAGIC     , tenor_maturityDate
# MAGIC   FROM loan_NLU
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   customerNumber
# MAGIC   -- , loanReference
# MAGIC   , loanCode
# MAGIC   , facilityCodeSerial
# MAGIC   , endDate
# MAGIC   , startDate
# MAGIC   , ROUND(CAST(basisAmount AS DOUBLE), 2) AS basisAmount
# MAGIC   , currency
# MAGIC   , MIN(CAST(tenor_bookDate AS DATE)) OVER (PARTITION BY customerNumber, facilityCodeSerial) AS min_bookDate
# MAGIC   , MAX(CAST(tenor_maturityDate AS DATE)) OVER (PARTITION BY customerNumber, facilityCodeSerial) AS max_maturityDate
# MAGIC FROM exploded_data
# MAGIC WHERE 1=1
# MAGIC   AND pos = pos_start
# MAGIC   AND pos = pos_basis
# MAGIC   AND facilityCodeSerial IS NOT NULL
# MAGIC   AND CAST(endDate AS DATE) > current_date()

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(distinct t1.customerNumber), count(distinct t1.facilityCodeSerial), count(distinct loanCode)
# MAGIC from loan t1
# MAGIC inner join radar.clients t2 on t1.customernumber = t2.GCDSID
# MAGIC where t2.GlobalReportingRegion = 'E&A'
# MAGIC   and t2.ClientLifeCycleName = 'Client'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC   t1.customerNumber
# MAGIC   -- , t1.facilityCodeSerial
# MAGIC   , t1.loanCode
# MAGIC   , t2.uniquegcobid
# MAGIC   , t2.FullLegalName
# MAGIC from loan t1
# MAGIC inner join radar.clients t2 on t1.customernumber = t2.GCDSID
# MAGIC where t2.GlobalReportingRegion = 'E&A'
# MAGIC   and t2.ClientLifeCycleName = 'Client'

# COMMAND ----------

# MAGIC %md
# MAGIC # Cash pooling and current accounts E&A

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC     t1.*
# MAGIC FROM radar.siraproductandservices t1
# MAGIC LEFT JOIN radar.clients t2 ON t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC WHERE t1.ProductName = 'Current Account'
# MAGIC     AND t2.GlobalReportingRegion = 'E&A'
# MAGIC     and t2.ClientLifeCycleName = 'Client'
# MAGIC     and t1.ProductLifecycle = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC     count(distinct t1.UniqueGcobId)
# MAGIC FROM radar.siraproductandservices t1
# MAGIC LEFT JOIN radar.clients t2 ON t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC WHERE t1.ProductName = 'Current Account'
# MAGIC     AND t2.GlobalReportingRegion = 'E&A'
# MAGIC     and t2.ClientLifeCycleName = 'Client'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC     t1.*
# MAGIC FROM radar.siraproductandservices t1
# MAGIC LEFT JOIN radar.clients t2 ON t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC WHERE t1.ProductName = 'Cash Pooling'
# MAGIC     AND t2.GlobalReportingRegion = 'E&A'
# MAGIC     and t2.ClientLifeCycleName = 'Client'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC     count(distinct t1.UniqueGcobId)
# MAGIC FROM radar.siraproductandservices t1
# MAGIC LEFT JOIN radar.clients t2 ON t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC WHERE t1.ProductName like '%Loan%'
# MAGIC     AND t2.GlobalReportingRegion = 'E&A'
# MAGIC     and t2.ClientLifeCycleName = 'Client'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC     t1.*, t2.FullLegalName
# MAGIC FROM radar.siraproductandservices t1
# MAGIC LEFT JOIN radar.clients t2 ON t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC WHERE t1.ProductName = 'Cash Pooling'
# MAGIC     AND t2.GlobalReportingRegion = 'E&A'
# MAGIC     and t2.ClientLifeCycleName = 'Client'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   t1.*, t2.FullLegalName
# MAGIC from radar.tx_wr_iban t1
# MAGIC left join radar.clients t2 on t1.uniquegcobid = t2.UniqueGcobId
# MAGIC   where t2.GlobalReportingRegion = 'E&A'
# MAGIC     and t2.ClientLifeCycleName = 'Client'

# COMMAND ----------

# DBTITLE 1,difference uniquegcobid between tx and sira
# MAGIC %sql
# MAGIC select
# MAGIC   t2.UniqueGcobId
# MAGIC from radar.tx_wr_iban t1
# MAGIC left join radar.clients t2 on t1.uniquegcobid = t2.UniqueGcobId
# MAGIC   where t2.GlobalReportingRegion = 'E&A'
# MAGIC     and t2.ClientLifeCycleName = 'Client'
# MAGIC     and t2.UniqueGcobId not in (
# MAGIC       SELECT
# MAGIC         t1.UniqueGcobId
# MAGIC       FROM radar.siraproductandservices t1
# MAGIC       LEFT JOIN radar.clients t2 ON t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC       WHERE t1.ProductName = 'Current Account'
# MAGIC         AND t2.GlobalReportingRegion = 'E&A'
# MAGIC         and t2.ClientLifeCycleName = 'Client'
# MAGIC     )
