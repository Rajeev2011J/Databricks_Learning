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

# DBTITLE 1,load flexcube objects
from datetime import datetime, timedelta
import re

flexcube_objects = ['facility_NLU', 'loan_NLU']

for item in flexcube_objects:
    # get the most recent version available in gdp
    path = f'abfss://core-cbt@edlcorestdeuprod0001.dfs.core.windows.net/{item}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

    # get the most recent file available in gdp and create temp view
    path_file = f'{path}{version}/data/'
    files = dbutils.fs.ls(path_file)
    load_date = max(file.path.split('BUSINESS_DTS=')[1][:8] for file in files if 'BUSINESS_DTS=' in file.path)
    spark.read.parquet(f'{path_file}BUSINESS_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# DBTITLE 1,load latest exchange rates
from pyspark.sql import functions as F
import re

columns_to_load = [
    'PriceCurrency',
    'Close_2100CET',
    'BaseCurrency',
    'RateDate'
]

# get the most recent version available in gdp
path = f'abfss://timescape@edlcorestdeuprod0001.dfs.core.windows.net/fx_rates_2100cet/'
files = dbutils.fs.ls(path)
version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

# get the most recent file available in gdp and create temp view
path_file = f'{path}{version}/data/'
files = dbutils.fs.ls(path_file)
load_date = max(file.path.split('RATES_DT=')[1][:8] for file in files if 'RATES_DT=' in file.path)
# load only selected columns + only basecurrency = EUR
spark.read.parquet(f'{path_file}RATES_DT={load_date}*/*.parquet') \
        .select(*columns_to_load) \
        .filter(F.col('BaseCurrency') == 'EUR') \
        .withColumnRenamed('PriceCurrency', 'currency_codes') \
        .createOrReplaceTempView('exchange_rates')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW facility AS
# MAGIC WITH facilityInEuro AS (
# MAGIC SELECT DISTINCT
# MAGIC   customerNumber 
# MAGIC   , facilityCodeSerial
# MAGIC   , limit_currency
# MAGIC   , limit_limitAmount
# MAGIC   , dates_startDate
# MAGIC   , dates_expiryDate
# MAGIC   , facilityDescription
# MAGIC   , primaryFeatures_categoryDescription
# MAGIC   , primaryFeatures_categoryCode
# MAGIC   , overallUtilization_totalUtilizedAmount
# MAGIC   , primaryFeatures_committedFlag
# MAGIC   , dates_availabilityEndDate
# MAGIC   , dates_availabilityStartDate
# MAGIC   , primaryFeatures_allocatingBranch
# MAGIC   , primaryFeatures_allocatingBranchName
# MAGIC   , fundingDealGroupId
# MAGIC FROM facility_NLU as t1
# MAGIC WHERE facilityCodeSerial IS NOT NULL
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC   t1.*
# MAGIC   , CASE
# MAGIC       WHEN t1.limit_currency = 'EUR' THEN CAST(t1.limit_limitAmount AS DECIMAL(20,2))
# MAGIC       ELSE CAST(t1.limit_limitAmount / NULLIF(CAST(t2.Close_2100CET AS FLOAT), 0) AS DECIMAL(20,2))
# MAGIC     END AS limit_limitAmountEUR
# MAGIC   , CASE
# MAGIC       WHEN t1.limit_currency = 'EUR' THEN CAST(1 AS DECIMAL(20,2))
# MAGIC       ELSE CAST(NULLIF(CAST(t2.Close_2100CET AS FLOAT), 0) AS DECIMAL(20,2))
# MAGIC     END AS conversionRate -- corresponding conversation rate used to create the newAvailableAmountEuro
# MAGIC
# MAGIC FROM facilityInEuro t1
# MAGIC LEFT JOIN exchange_rates t2 ON t1.limit_currency = TRIM(t2.currency_codes)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW loan AS
# MAGIC
# MAGIC WITH exploded_data AS (
# MAGIC   SELECT
# MAGIC     customerNumber
# MAGIC     , facilityCodeSerial
# MAGIC     , posexplode(positionBalances_schedules_schedulePeriods_calculationPeriods_endDate) AS (pos, endDate)
# MAGIC     , posexplode(positionBalances_schedules_schedulePeriods_calculationPeriods_startDate) AS (pos_start, startDate)
# MAGIC     , posexplode(positionBalances_schedules_schedulePeriods_calculationPeriods_basisAmount) AS (pos_basis, basisAmount)
# MAGIC     , positionBalances_amounts_currency[0] AS currency -- get first element of currency and repeat it accross rows
# MAGIC     , tenor_bookDate
# MAGIC     , tenor_maturityDate
# MAGIC     , secondaryFeatures_financeProductTypeCode
# MAGIC     , secondaryFeatures_financeProductTypeDescription
# MAGIC     , primaryFeatures_portfolioName
# MAGIC     , primaryFeatures_portfolioCode
# MAGIC   FROM loan_NLU
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   customerNumber
# MAGIC   , facilityCodeSerial
# MAGIC   , endDate
# MAGIC   , startDate
# MAGIC   , ROUND(CAST(basisAmount AS DOUBLE), 2) AS basisAmount
# MAGIC   , currency
# MAGIC   , MIN(CAST(tenor_bookDate AS DATE)) OVER (PARTITION BY customerNumber, facilityCodeSerial) AS min_bookDate
# MAGIC   , MAX(CAST(tenor_maturityDate AS DATE)) OVER (PARTITION BY customerNumber, facilityCodeSerial) AS max_maturityDate
# MAGIC   , secondaryFeatures_financeProductTypeCode
# MAGIC   , secondaryFeatures_financeProductTypeDescription
# MAGIC   , primaryFeatures_portfolioName
# MAGIC   , primaryFeatures_portfolioCode
# MAGIC FROM exploded_data
# MAGIC WHERE 1=1
# MAGIC   AND pos = pos_start
# MAGIC   AND pos = pos_basis
# MAGIC   AND facilityCodeSerial IS NOT NULL
# MAGIC   AND CAST(endDate AS DATE) > current_date()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.greenloans_flexcube_facility;
# MAGIC drop table if exists radar.greenloans_flexcube_loan;

# COMMAND ----------

spark.sql('SELECT * FROM facility').write.mode('overwrite').saveAsTable('radar.greenloans_flexcube_facility')
spark.sql('SELECT * FROM loan').write.mode('overwrite').saveAsTable('radar.greenloans_flexcube_loan')
