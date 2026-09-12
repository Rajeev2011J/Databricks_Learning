# Databricks notebook source
# DBTITLE 1,dependencies
import os
from datetime import datetime, timedelta
import re
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,env variables
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,auth GDP
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC Loading Flexcube Data
# MAGIC - Facility_NLU, Facility_GBL and Loan_NLU, Loan_GBL is used by region E&A
# MAGIC - Other facility and loan datasets are used by region Asia

# COMMAND ----------

# DBTITLE 1,load flexcube objects


flexcube_objects = ['facility_NLU'
                    , 'facility_HKG'
                    , 'facility_CNS'
                    , 'facility_SGP'
                    , 'facility_INM'
                    , 'facility_GBL'
                    , 'facility_USU'
                    , 'facility_USN'
                    , 'facility_CAT'
                    , 'loan_NLU'
                    , 'loan_HKG'
                    , 'loan_CNS'
                    , 'loan_SGP'
                    , 'loan_INM'
                    , 'loan_GBL'
                    , 'loan_USU'
                    , 'loan_USN'
                    , 'loan_CAT'
                    ]

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

# Load flexcube data from the latest version of tables where there is 1:many relation. 
# version 248
# TODO use these as source for tables. May need to join on many other tables. 

from datetime import datetime, timedelta
import re

flexcube_objects = ['facility_facility_NLU'
                    , 'facility_facility_HKG'
                    , 'facility_facility_CNS'
                    , 'facility_facility_SGP'
  #                  , 'facility_facility_INM'
                    , 'facility_facility_GBL'
                    , 'facility_facility_USU'
                    , 'facility_facility_USN'
                    , 'facility_facility_CAT'
                    , 'loan_loan_NLU'
                    , 'loan_loan_HKG'
                    , 'loan_loan_CNS'
                    , 'loan_loan_SGP'
 #                   , 'loan_loan_INM'
                    , 'loan_loan_GBL'
                    , 'loan_loan_USU'
                    , 'loan_loan_USN'
                    , 'loan_loan_CAT'
                    ]

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

# DBTITLE 1,Load GCDS Data
from pyspark.sql import SparkSession
# load and create temp views of all gdp_tables below
gcds_tables = [
     'client_Client'
]

for item in gcds_tables:
    # get the most recent version available in gdp
    path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

    # get the most recent file available in gdp
    path_file = f'{path}{version}/data/'
    files = dbutils.fs.ls(path_file)
    load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

    spark.read.parquet(f'{path_file}LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcds_'+ item)

# COMMAND ----------

# MAGIC %md
# MAGIC - lastoverDraftDate is date of breach
# MAGIC - breach happens when limit_effectiveLimitAmount - overallUtilization_totalUtilizedAmount - overallUtilization_unavailableAmount = overallUtilization_availableAmount and overallUtilization_availableAmount < 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union Together
# MAGIC The below code unions facility for all the branches that are used by region E&A and Asia & NA

# COMMAND ----------

# DBTITLE 1,Union all northern hemisphere Facility data.
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW facility_limit_data AS
# MAGIC
# MAGIC WITH newAvailableAmount_CTE AS (
# MAGIC   SELECT * FROM (
# MAGIC     -- NLU
# MAGIC    SELECT 'facility_NLU' AS sourceSystem, * FROM (
# MAGIC     SELECT DISTINCT  
# MAGIC     CAST(t1.overallUtilization_lastOverdraftDate AS DATE) AS dateOfBreach,
# MAGIC     CAST(t1.overallUtilization_firstOverdraftDate AS DATE) AS dateOfFirstBreach,
# MAGIC     t1.customerNumber AS gcid,
# MAGIC     t1.customerName AS gcidClientName,
# MAGIC     t2.`RM-name` AS relationshipManagerName,
# MAGIC     t1.facilityCode AS facilityCode,
# MAGIC     t1.mainFacilityCode AS mainFacilityCode,
# MAGIC     t1.facilityDescription AS description,
# MAGIC     t1.primaryFeatures_categoryCode AS category,
# MAGIC     t1.statusInformation_status AS facilityStatus,
# MAGIC     t1.statusInformation_recordStatusDescription AS facilityStatusRecordDescription,
# MAGIC     CAST(t1.primaryFeatures_revolvingFlag AS STRING) AS revolving,
# MAGIC     TRIM(t1.limit_currency) AS facilityCurrency,
# MAGIC     CAST(t1.dates_startDate AS DATE) AS facilityStartDate,
# MAGIC     CAST(t1.dates_expiryDate AS DATE) AS facilityExpiryDate,
# MAGIC     CAST(t1.primaryFeatures_availabilityFlag AS STRING) AS availability,
# MAGIC     --limit amount is the limit amount aggreed with the client
# MAGIC     CAST(t1.limit_limitAmount AS DECIMAL(20,2)) AS limitAmount,
# MAGIC     CAST(t1.limit_collateralContributionAmount AS DECIMAL(20,2)) AS collateralAmount,
# MAGIC     --effectiveLimitAmount is the one that he given by the bank, and is usually not more than the limitAmount
# MAGIC     CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) AS effectiveLimitAmount,
# MAGIC
# MAGIC     --effectiveLimitAmount <> 0 and effectiveLimitAmount - utilisation as effectiveLimitAmountBreach
# MAGIC
# MAGIC     CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) <> 0 AND (  CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS effectiveLimitAmountBreach,
# MAGIC
# MAGIC     --effectiveLimitAmount = 0 and intraDayLimitAmount - utilisation as intraDayLimitAmountBreach
# MAGIC
# MAGIC     CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) = 0 AND (CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS intraDayLimitAmountBreach,
# MAGIC     
# MAGIC     CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) < 0 AS availableAmountBreach,
# MAGIC     CASE 
# MAGIC     WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) = 0 THEN CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) + CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2))
# MAGIC     WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) <> 0 THEN  CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2))
# MAGIC     END AS newAvailableAmount,
# MAGIC     CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) AS availableAmount,
# MAGIC     --CAST(t1.overallUtilization_totalUtilizedAmount AS DECIMAL(20,2)) AS oldUtilisation,
# MAGIC     CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) AS utilisation,
# MAGIC     CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) AS unavailableAmount,
# MAGIC     CAST(t1.overallUtilization_totalWithheldAmount AS DECIMAL(20,2)) AS withheldAmount,
# MAGIC     CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) AS intraDayLimitAmount,
# MAGIC     CAST(t1.dates_availabilityStartDate AS DATE) AS intraDayLimitAmountStartdate,
# MAGIC     t1.liabilityCode,
# MAGIC     t1.liabilityName,
# MAGIC     CAST(t1.dates_creditApprovalDate AS DATE) AS creditApprovalDate,
# MAGIC     CAST(t1.dates_availabilityEndDate AS DATE) AS availabilityEndDate,
# MAGIC     t1.primaryFeatures_portfolioCode AS portfolioCode,
# MAGIC     t1.primaryFeatures_portfolioName AS portfolioName,
# MAGIC     t1.primaryFeatures_riskCountry AS countryOfRisk,
# MAGIC     CAST(t1.overallUtilization_marginRatePercentage AS DECIMAL(20,2)) AS margin,
# MAGIC     t1.facilityCodeSerial,
# MAGIC     t1.primaryFeatures_committedFlag,
# MAGIC     t1.dates_availabilityEndDate,
# MAGIC     t1.dates_availabilityStartDate,
# MAGIC     t1.primaryFeatures_allocatingBranch,
# MAGIC     t1.primaryFeatures_allocatingBranchName,
# MAGIC     t1.fundingDealGroupId,
# MAGIC     t1.facilityDescription,
# MAGIC     t1.restrictions_customer_customerNumber
# MAGIC    --t1.EDL_LOAD_DTS
# MAGIC     FROM facility_NLU AS t1
# MAGIC   LEFT JOIN gcds_client_Client AS t2 ON t1.customerNumber = t2.gcid
# MAGIC   ) NLU
# MAGIC
# MAGIC   UNION ALL
# MAGIC     -- HKG
# MAGIC    SELECT 'facility_HKG' AS sourceSystem, * FROM (
# MAGIC     SELECT DISTINCT  
# MAGIC     CAST(t1.overallUtilization_lastOverdraftDate AS DATE) AS dateOfBreach,
# MAGIC     CAST(t1.overallUtilization_firstOverdraftDate AS DATE) AS dateOfFirstBreach,
# MAGIC     t1.customerNumber AS gcid,
# MAGIC     t1.customerName AS gcidClientName,
# MAGIC     t2.`RM-name` AS relationshipManagerName,
# MAGIC     t1.facilityCode AS facilityCode,
# MAGIC     t1.mainFacilityCode AS mainFacilityCode,
# MAGIC     t1.facilityDescription AS description,
# MAGIC     t1.primaryFeatures_categoryCode AS category,
# MAGIC     t1.statusInformation_status AS facilityStatus,
# MAGIC     t1.statusInformation_recordStatusDescription AS facilityStatusRecordDescription,
# MAGIC     CAST(t1.primaryFeatures_revolvingFlag AS STRING) AS revolving,
# MAGIC     TRIM(t1.limit_currency) AS facilityCurrency,
# MAGIC     CAST(t1.dates_startDate AS DATE) AS facilityStartDate,
# MAGIC     CAST(t1.dates_expiryDate AS DATE) AS facilityExpiryDate,
# MAGIC     CAST(t1.primaryFeatures_availabilityFlag AS STRING) AS availability,
# MAGIC     --limit amount is the limit amount aggreed with the client
# MAGIC     CAST(t1.limit_limitAmount AS DECIMAL(20,2)) AS limitAmount,
# MAGIC     CAST(t1.limit_collateralContributionAmount AS DECIMAL(20,2)) AS collateralAmount,
# MAGIC     --effectiveLimitAmount is the one that he given by the bank, and is usually not more than the limitAmount
# MAGIC     CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) AS effectiveLimitAmount,
# MAGIC
# MAGIC     --effectiveLimitAmount <> 0 and effectiveLimitAmount - utilisation as effectiveLimitAmountBreach
# MAGIC
# MAGIC     CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) <> 0 AND (  CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS effectiveLimitAmountBreach,
# MAGIC
# MAGIC     --effectiveLimitAmount = 0 and intraDayLimitAmount - utilisation as intraDayLimitAmountBreach
# MAGIC
# MAGIC     CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) = 0 AND (CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS intraDayLimitAmountBreach,
# MAGIC     
# MAGIC     CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) < 0 AS availableAmountBreach,
# MAGIC     CASE 
# MAGIC     WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) = 0 THEN CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) + CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2))
# MAGIC     WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) <> 0 THEN  CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2))
# MAGIC     END AS newAvailableAmount,
# MAGIC     CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) AS availableAmount,
# MAGIC     --CAST(t1.overallUtilization_totalUtilizedAmount AS DECIMAL(20,2)) AS oldUtilisation,
# MAGIC     CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) AS utilisation,
# MAGIC     CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) AS unavailableAmount,
# MAGIC     CAST(t1.overallUtilization_totalWithheldAmount AS DECIMAL(20,2)) AS withheldAmount,
# MAGIC     CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) AS intraDayLimitAmount,
# MAGIC     CAST(t1.dates_availabilityStartDate AS DATE) AS intraDayLimitAmountStartdate,
# MAGIC     t1.liabilityCode,
# MAGIC     t1.liabilityName,
# MAGIC     CAST(t1.dates_creditApprovalDate AS DATE) AS creditApprovalDate,
# MAGIC     CAST(t1.dates_availabilityEndDate AS DATE) AS availabilityEndDate,
# MAGIC     t1.primaryFeatures_portfolioCode AS portfolioCode,
# MAGIC     t1.primaryFeatures_portfolioName AS portfolioName,
# MAGIC     t1.primaryFeatures_riskCountry AS countryOfRisk,
# MAGIC     CAST(t1.overallUtilization_marginRatePercentage AS DECIMAL(20,2)) AS margin,
# MAGIC     t1.facilityCodeSerial,
# MAGIC     t1.primaryFeatures_committedFlag,
# MAGIC     t1.dates_availabilityEndDate,
# MAGIC     t1.dates_availabilityStartDate,
# MAGIC     t1.primaryFeatures_allocatingBranch,
# MAGIC     t1.primaryFeatures_allocatingBranchName,
# MAGIC     t1.fundingDealGroupId,
# MAGIC     t1.facilityDescription,
# MAGIC     t1.restrictions_customer_customerNumber
# MAGIC     --t1.EDL_LOAD_DTS
# MAGIC   FROM facility_HKG AS t1
# MAGIC   LEFT JOIN gcds_client_Client AS t2 ON t1.customerNumber = t2.gcid
# MAGIC   ) HKG
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC     -- HKG
# MAGIC    SELECT 'facility_SGP' AS sourceSystem, * FROM (
# MAGIC     SELECT DISTINCT  
# MAGIC     CAST(t1.overallUtilization_lastOverdraftDate AS DATE) AS dateOfBreach,
# MAGIC     CAST(t1.overallUtilization_firstOverdraftDate AS DATE) AS dateOfFirstBreach,
# MAGIC     t1.customerNumber AS gcid,
# MAGIC     t1.customerName AS gcidClientName,
# MAGIC     t2.`RM-name` AS relationshipManagerName,
# MAGIC     t1.facilityCode AS facilityCode,
# MAGIC     t1.mainFacilityCode AS mainFacilityCode,
# MAGIC     t1.facilityDescription AS description,
# MAGIC     t1.primaryFeatures_categoryCode AS category,
# MAGIC     t1.statusInformation_status AS facilityStatus,
# MAGIC     t1.statusInformation_recordStatusDescription AS facilityStatusRecordDescription,
# MAGIC     CAST(t1.primaryFeatures_revolvingFlag AS STRING) AS revolving,
# MAGIC     TRIM(t1.limit_currency) AS facilityCurrency,
# MAGIC     CAST(t1.dates_startDate AS DATE) AS facilityStartDate,
# MAGIC     CAST(t1.dates_expiryDate AS DATE) AS facilityExpiryDate,
# MAGIC     CAST(t1.primaryFeatures_availabilityFlag AS STRING) AS availability,
# MAGIC     --limit amount is the limit amount aggreed with the client
# MAGIC     CAST(t1.limit_limitAmount AS DECIMAL(20,2)) AS limitAmount,
# MAGIC     CAST(t1.limit_collateralContributionAmount AS DECIMAL(20,2)) AS collateralAmount,
# MAGIC     --effectiveLimitAmount is the one that he given by the bank, and is usually not more than the limitAmount
# MAGIC     CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) AS effectiveLimitAmount,
# MAGIC
# MAGIC     --effectiveLimitAmount <> 0 and effectiveLimitAmount - utilisation as effectiveLimitAmountBreach
# MAGIC
# MAGIC     CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) <> 0 AND (  CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS effectiveLimitAmountBreach,
# MAGIC
# MAGIC     --effectiveLimitAmount = 0 and intraDayLimitAmount - utilisation as intraDayLimitAmountBreach
# MAGIC
# MAGIC     CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) = 0 AND (CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS intraDayLimitAmountBreach,
# MAGIC     
# MAGIC     CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) < 0 AS availableAmountBreach,
# MAGIC     CASE 
# MAGIC     WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) = 0 THEN CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) + CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2))
# MAGIC     WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) <> 0 THEN  CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2))
# MAGIC     END AS newAvailableAmount,
# MAGIC     CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) AS availableAmount,
# MAGIC     --CAST(t1.overallUtilization_totalUtilizedAmount AS DECIMAL(20,2)) AS oldUtilisation,
# MAGIC     CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) AS utilisation,
# MAGIC     CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) AS unavailableAmount,
# MAGIC     CAST(t1.overallUtilization_totalWithheldAmount AS DECIMAL(20,2)) AS withheldAmount,
# MAGIC     CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) AS intraDayLimitAmount,
# MAGIC     CAST(t1.dates_availabilityStartDate AS DATE) AS intraDayLimitAmountStartdate,
# MAGIC     t1.liabilityCode,
# MAGIC     t1.liabilityName,
# MAGIC     CAST(t1.dates_creditApprovalDate AS DATE) AS creditApprovalDate,
# MAGIC     CAST(t1.dates_availabilityEndDate AS DATE) AS availabilityEndDate,
# MAGIC     t1.primaryFeatures_portfolioCode AS portfolioCode,
# MAGIC     t1.primaryFeatures_portfolioName AS portfolioName,
# MAGIC     t1.primaryFeatures_riskCountry AS countryOfRisk,
# MAGIC     CAST(t1.overallUtilization_marginRatePercentage AS DECIMAL(20,2)) AS margin,
# MAGIC     t1.facilityCodeSerial,
# MAGIC     t1.primaryFeatures_committedFlag,
# MAGIC     t1.dates_availabilityEndDate,
# MAGIC     t1.dates_availabilityStartDate,
# MAGIC     t1.primaryFeatures_allocatingBranch,
# MAGIC     t1.primaryFeatures_allocatingBranchName,
# MAGIC     t1.fundingDealGroupId,
# MAGIC     t1.facilityDescription,
# MAGIC     t1.restrictions_customer_customerNumber
# MAGIC     --t1.EDL_LOAD_DTS
# MAGIC   FROM facility_SGP AS t1
# MAGIC   LEFT JOIN gcds_client_Client AS t2 ON t1.customerNumber = t2.gcid
# MAGIC   ) SPG
# MAGIC
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC     -- Mumbai
# MAGIC    SELECT 'facility_INM' AS sourceSystem, * FROM (
# MAGIC     SELECT DISTINCT  
# MAGIC     CAST(t1.overallUtilization_lastOverdraftDate AS DATE) AS dateOfBreach,
# MAGIC     CAST(t1.overallUtilization_firstOverdraftDate AS DATE) AS dateOfFirstBreach,
# MAGIC     t1.customerNumber AS gcid,
# MAGIC     t1.customerName AS gcidClientName,
# MAGIC     t2.`RM-name` AS relationshipManagerName,
# MAGIC     t1.facilityCode AS facilityCode,
# MAGIC     t1.mainFacilityCode AS mainFacilityCode,
# MAGIC     t1.facilityDescription AS description,
# MAGIC     t1.primaryFeatures_categoryCode AS category,
# MAGIC     t1.statusInformation_status AS facilityStatus,
# MAGIC     t1.statusInformation_recordStatusDescription AS facilityStatusRecordDescription,
# MAGIC     CAST(t1.primaryFeatures_revolvingFlag AS STRING) AS revolving,
# MAGIC     TRIM(t1.limit_currency) AS facilityCurrency,
# MAGIC     CAST(t1.dates_startDate AS DATE) AS facilityStartDate,
# MAGIC     CAST(t1.dates_expiryDate AS DATE) AS facilityExpiryDate,
# MAGIC     CAST(t1.primaryFeatures_availabilityFlag AS STRING) AS availability,
# MAGIC     --limit amount is the limit amount aggreed with the client
# MAGIC     CAST(t1.limit_limitAmount AS DECIMAL(20,2)) AS limitAmount,
# MAGIC     CAST(t1.limit_collateralContributionAmount AS DECIMAL(20,2)) AS collateralAmount,
# MAGIC     --effectiveLimitAmount is the one that he given by the bank, and is usually not more than the limitAmount
# MAGIC     CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) AS effectiveLimitAmount,
# MAGIC
# MAGIC     --effectiveLimitAmount <> 0 and effectiveLimitAmount - utilisation as effectiveLimitAmountBreach
# MAGIC
# MAGIC     CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) <> 0 AND (  CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS effectiveLimitAmountBreach,
# MAGIC
# MAGIC     --effectiveLimitAmount = 0 and intraDayLimitAmount - utilisation as intraDayLimitAmountBreach
# MAGIC
# MAGIC     CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) = 0 AND (CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS intraDayLimitAmountBreach,
# MAGIC     
# MAGIC     CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) < 0 AS availableAmountBreach,
# MAGIC     CASE 
# MAGIC     WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) = 0 THEN CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) + CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2))
# MAGIC     WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) <> 0 THEN  CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2))
# MAGIC     END AS newAvailableAmount,
# MAGIC     CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) AS availableAmount,
# MAGIC     --CAST(t1.overallUtilization_totalUtilizedAmount AS DECIMAL(20,2)) AS oldUtilisation,
# MAGIC     CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) AS utilisation,
# MAGIC     CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) AS unavailableAmount,
# MAGIC     CAST(t1.overallUtilization_totalWithheldAmount AS DECIMAL(20,2)) AS withheldAmount,
# MAGIC     CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) AS intraDayLimitAmount,
# MAGIC     CAST(t1.dates_availabilityStartDate AS DATE) AS intraDayLimitAmountStartdate,
# MAGIC     t1.liabilityCode,
# MAGIC     t1.liabilityName,
# MAGIC     CAST(t1.dates_creditApprovalDate AS DATE) AS creditApprovalDate,
# MAGIC     CAST(t1.dates_availabilityEndDate AS DATE) AS availabilityEndDate,
# MAGIC     t1.primaryFeatures_portfolioCode AS portfolioCode,
# MAGIC     t1.primaryFeatures_portfolioName AS portfolioName,
# MAGIC     t1.primaryFeatures_riskCountry AS countryOfRisk,
# MAGIC     CAST(t1.overallUtilization_marginRatePercentage AS DECIMAL(20,2)) AS margin,
# MAGIC     t1.facilityCodeSerial,
# MAGIC     t1.primaryFeatures_committedFlag,
# MAGIC     t1.dates_availabilityEndDate,
# MAGIC     t1.dates_availabilityStartDate,
# MAGIC     t1.primaryFeatures_allocatingBranch,
# MAGIC     t1.primaryFeatures_allocatingBranchName,
# MAGIC     t1.fundingDealGroupId,
# MAGIC     t1.facilityDescription,
# MAGIC     t1.restrictions_customer_customerNumber
# MAGIC     --t1.EDL_LOAD_DTS
# MAGIC   FROM facility_INM AS t1
# MAGIC   LEFT JOIN gcds_client_Client AS t2 ON t1.customerNumber = t2.gcid
# MAGIC   ) INM
# MAGIC   
# MAGIC UNION ALL
# MAGIC -- CNS
# MAGIC SELECT 'facility_CNS' AS sourceSystem, * FROM (
# MAGIC     SELECT DISTINCT  
# MAGIC         CAST(t1.overallUtilization_lastOverdraftDate AS DATE) AS dateOfBreach,
# MAGIC         CAST(t1.overallUtilization_firstOverdraftDate AS DATE) AS dateOfFirstBreach,
# MAGIC         t1.customerNumber AS gcid,
# MAGIC         t1.customerName AS gcidClientName,
# MAGIC         t2.`RM-name` AS relationshipManagerName,
# MAGIC         t1.facilityCode AS facilityCode,
# MAGIC         t1.mainFacilityCode AS mainFacilityCode,
# MAGIC         t1.facilityDescription AS description,
# MAGIC         t1.primaryFeatures_categoryCode AS category,
# MAGIC         t1.statusInformation_status AS facilityStatus,
# MAGIC         t1.statusInformation_recordStatusDescription AS facilityStatusRecordDescription,
# MAGIC         CAST(t1.primaryFeatures_revolvingFlag AS STRING) AS revolving,
# MAGIC         TRIM(t1.limit_currency) AS facilityCurrency,
# MAGIC         CAST(t1.dates_startDate AS DATE) AS facilityStartDate,
# MAGIC         CAST(t1.dates_expiryDate AS DATE) AS facilityExpiryDate,
# MAGIC         CAST(t1.primaryFeatures_availabilityFlag AS STRING) AS availability,
# MAGIC         --limit amount is the limit amount aggreed with the client
# MAGIC         CAST(t1.limit_limitAmount AS DECIMAL(20,2)) AS limitAmount,
# MAGIC         CAST(t1.limit_collateralContributionAmount AS DECIMAL(20,2)) AS collateralAmount,
# MAGIC         --effectiveLimitAmount is the one that he given by the bank, and is usually not more than the limitAmount
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) AS effectiveLimitAmount,
# MAGIC
# MAGIC         --effectiveLimitAmount <> 0 and effectiveLimitAmount - utilisation as effectiveLimitAmountBreach
# MAGIC
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) <> 0 AND (  CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS effectiveLimitAmountBreach,
# MAGIC
# MAGIC         --effectiveLimitAmount = 0 and intraDayLimitAmount - utilisation as intraDayLimitAmountBreach
# MAGIC
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) = 0 AND (CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS intraDayLimitAmountBreach,
# MAGIC         
# MAGIC         CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) < 0 AS availableAmountBreach,
# MAGIC         CASE 
# MAGIC         WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) = 0 THEN CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) + CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2))
# MAGIC         WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) <> 0 THEN  CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2))
# MAGIC         END AS newAvailableAmount,
# MAGIC         CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) AS availableAmount,
# MAGIC         --CAST(t1.overallUtilization_totalUtilizedAmount AS DECIMAL(20,2)) AS oldUtilisation,
# MAGIC         CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) AS utilisation,
# MAGIC         CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) AS unavailableAmount,
# MAGIC         CAST(t1.overallUtilization_totalWithheldAmount AS DECIMAL(20,2)) AS withheldAmount,
# MAGIC         CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) AS intraDayLimitAmount,
# MAGIC         CAST(t1.dates_availabilityStartDate AS DATE) AS intraDayLimitAmountStartdate,
# MAGIC         t1.liabilityCode,
# MAGIC         t1.liabilityName,
# MAGIC         CAST(t1.dates_creditApprovalDate AS DATE) AS creditApprovalDate,
# MAGIC         CAST(t1.dates_availabilityEndDate AS DATE) AS availabilityEndDate,
# MAGIC         t1.primaryFeatures_portfolioCode AS portfolioCode,
# MAGIC         t1.primaryFeatures_portfolioName AS portfolioName,
# MAGIC         t1.primaryFeatures_riskCountry AS countryOfRisk,
# MAGIC         CAST(t1.overallUtilization_marginRatePercentage AS DECIMAL(20,2)) AS margin,
# MAGIC         t1.facilityCodeSerial,
# MAGIC         t1.primaryFeatures_committedFlag,
# MAGIC         t1.dates_availabilityEndDate,
# MAGIC         t1.dates_availabilityStartDate,
# MAGIC         t1.primaryFeatures_allocatingBranch,
# MAGIC         t1.primaryFeatures_allocatingBranchName,
# MAGIC         t1.fundingDealGroupId,
# MAGIC         t1.facilityDescription,
# MAGIC         t1.restrictions_customer_customerNumber
# MAGIC         -- t1.EDL_LOAD_DTS
# MAGIC       FROM facility_CNS AS t1
# MAGIC       LEFT JOIN gcds_client_Client AS t2 ON t1.customerNumber = t2.gcid
# MAGIC             ) CNS
# MAGIC
# MAGIC
# MAGIC     UNION ALL
# MAGIC     --FACILITY GREAT BRITAIN
# MAGIC     SELECT 'facility_GBL' AS sourceSystem, * FROM (
# MAGIC     SELECT DISTINCT  
# MAGIC         CAST(t1.overallUtilization_lastOverdraftDate AS DATE) AS dateOfBreach,
# MAGIC         CAST(t1.overallUtilization_firstOverdraftDate AS DATE) AS dateOfFirstBreach,
# MAGIC         t1.customerNumber AS gcid,
# MAGIC         t1.customerName AS gcidClientName,
# MAGIC         t2.`RM-name` AS relationshipManagerName,
# MAGIC         t1.facilityCode AS facilityCode,
# MAGIC         t1.mainFacilityCode AS mainFacilityCode,
# MAGIC         t1.facilityDescription AS description,
# MAGIC         t1.primaryFeatures_categoryCode AS category,
# MAGIC         t1.statusInformation_status AS facilityStatus,
# MAGIC         t1.statusInformation_recordStatusDescription AS facilityStatusRecordDescription,
# MAGIC         CAST(t1.primaryFeatures_revolvingFlag AS STRING) AS revolving,
# MAGIC         TRIM(t1.limit_currency) AS facilityCurrency,
# MAGIC         CAST(t1.dates_startDate AS DATE) AS facilityStartDate,
# MAGIC         CAST(t1.dates_expiryDate AS DATE) AS facilityExpiryDate,
# MAGIC         CAST(t1.primaryFeatures_availabilityFlag AS STRING) AS availability,
# MAGIC         --limit amount is the limit amount aggreed with the client
# MAGIC         CAST(t1.limit_limitAmount AS DECIMAL(20,2)) AS limitAmount,
# MAGIC         CAST(t1.limit_collateralContributionAmount AS DECIMAL(20,2)) AS collateralAmount,
# MAGIC         --effectiveLimitAmount is the one that he given by the bank, and is usually not more than the limitAmount
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) AS effectiveLimitAmount,
# MAGIC
# MAGIC         --effectiveLimitAmount <> 0 and effectiveLimitAmount - utilisation as effectiveLimitAmountBreach
# MAGIC
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) <> 0 AND (  CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS effectiveLimitAmountBreach,
# MAGIC
# MAGIC         --effectiveLimitAmount = 0 and intraDayLimitAmount - utilisation as intraDayLimitAmountBreach
# MAGIC
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) = 0 AND (CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS intraDayLimitAmountBreach,
# MAGIC         
# MAGIC         CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) < 0 AS availableAmountBreach,
# MAGIC         CASE 
# MAGIC         WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) = 0 THEN CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) + CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2))
# MAGIC         WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) <> 0 THEN  CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2))
# MAGIC         END AS newAvailableAmount,
# MAGIC         CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) AS availableAmount,
# MAGIC         --CAST(t1.overallUtilization_totalUtilizedAmount AS DECIMAL(20,2)) AS oldUtilisation,
# MAGIC         CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) AS utilisation,
# MAGIC         CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) AS unavailableAmount,
# MAGIC         CAST(t1.overallUtilization_totalWithheldAmount AS DECIMAL(20,2)) AS withheldAmount,
# MAGIC         CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) AS intraDayLimitAmount,
# MAGIC         CAST(t1.dates_availabilityStartDate AS DATE) AS intraDayLimitAmountStartdate,
# MAGIC         t1.liabilityCode,
# MAGIC         t1.liabilityName,
# MAGIC         CAST(t1.dates_creditApprovalDate AS DATE) AS creditApprovalDate,
# MAGIC         CAST(t1.dates_availabilityEndDate AS DATE) AS availabilityEndDate,
# MAGIC         t1.primaryFeatures_portfolioCode AS portfolioCode,
# MAGIC         t1.primaryFeatures_portfolioName AS portfolioName,
# MAGIC         t1.primaryFeatures_riskCountry AS countryOfRisk,
# MAGIC         CAST(t1.overallUtilization_marginRatePercentage AS DECIMAL(20,2)) AS margin,
# MAGIC         t1.facilityCodeSerial,
# MAGIC         t1.primaryFeatures_committedFlag,
# MAGIC         t1.dates_availabilityEndDate,
# MAGIC         t1.dates_availabilityStartDate,
# MAGIC         t1.primaryFeatures_allocatingBranch,
# MAGIC         t1.primaryFeatures_allocatingBranchName,
# MAGIC         t1.fundingDealGroupId,
# MAGIC         t1.facilityDescription,
# MAGIC         t1.restrictions_customer_customerNumber
# MAGIC         -- t1.EDL_LOAD_DTS
# MAGIC       FROM facility_GBL AS t1
# MAGIC       LEFT JOIN gcds_client_Client AS t2 ON t1.customerNumber = t2.gcid
# MAGIC             ) GBL
# MAGIC
# MAGIC       UNION ALL
# MAGIC     --FACILITY US NEW YORK
# MAGIC     SELECT 'facility_USN' AS sourceSystem, * FROM (
# MAGIC     SELECT DISTINCT  
# MAGIC         CAST(t1.overallUtilization_lastOverdraftDate AS DATE) AS dateOfBreach,
# MAGIC         CAST(t1.overallUtilization_firstOverdraftDate AS DATE) AS dateOfFirstBreach,
# MAGIC         t1.customerNumber AS gcid,
# MAGIC         t1.customerName AS gcidClientName,
# MAGIC         t2.`RM-name` AS relationshipManagerName,
# MAGIC         t1.facilityCode AS facilityCode,
# MAGIC         t1.mainFacilityCode AS mainFacilityCode,
# MAGIC         t1.facilityDescription AS description,
# MAGIC         t1.primaryFeatures_categoryCode AS category,
# MAGIC         t1.statusInformation_status AS facilityStatus,
# MAGIC         t1.statusInformation_recordStatusDescription AS facilityStatusRecordDescription,
# MAGIC         CAST(t1.primaryFeatures_revolvingFlag AS STRING) AS revolving,
# MAGIC         TRIM(t1.limit_currency) AS facilityCurrency,
# MAGIC         CAST(t1.dates_startDate AS DATE) AS facilityStartDate,
# MAGIC         CAST(t1.dates_expiryDate AS DATE) AS facilityExpiryDate,
# MAGIC         CAST(t1.primaryFeatures_availabilityFlag AS STRING) AS availability,
# MAGIC         --limit amount is the limit amount aggreed with the client
# MAGIC         CAST(t1.limit_limitAmount AS DECIMAL(20,2)) AS limitAmount,
# MAGIC         CAST(t1.limit_collateralContributionAmount AS DECIMAL(20,2)) AS collateralAmount,
# MAGIC         --effectiveLimitAmount is the one that he given by the bank, and is usually not more than the limitAmount
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) AS effectiveLimitAmount,
# MAGIC
# MAGIC         --effectiveLimitAmount <> 0 and effectiveLimitAmount - utilisation as effectiveLimitAmountBreach
# MAGIC
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) <> 0 AND (  CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS effectiveLimitAmountBreach,
# MAGIC
# MAGIC         --effectiveLimitAmount = 0 and intraDayLimitAmount - utilisation as intraDayLimitAmountBreach
# MAGIC
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) = 0 AND (CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS intraDayLimitAmountBreach,
# MAGIC         
# MAGIC         CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) < 0 AS availableAmountBreach,
# MAGIC         CASE 
# MAGIC         WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) = 0 THEN CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) + CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2))
# MAGIC         WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) <> 0 THEN  CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2))
# MAGIC         END AS newAvailableAmount,
# MAGIC         CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) AS availableAmount,
# MAGIC         --CAST(t1.overallUtilization_totalUtilizedAmount AS DECIMAL(20,2)) AS oldUtilisation,
# MAGIC         CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) AS utilisation,
# MAGIC         CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) AS unavailableAmount,
# MAGIC         CAST(t1.overallUtilization_totalWithheldAmount AS DECIMAL(20,2)) AS withheldAmount,
# MAGIC         CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) AS intraDayLimitAmount,
# MAGIC         CAST(t1.dates_availabilityStartDate AS DATE) AS intraDayLimitAmountStartdate,
# MAGIC         t1.liabilityCode,
# MAGIC         t1.liabilityName,
# MAGIC         CAST(t1.dates_creditApprovalDate AS DATE) AS creditApprovalDate,
# MAGIC         CAST(t1.dates_availabilityEndDate AS DATE) AS availabilityEndDate,
# MAGIC         t1.primaryFeatures_portfolioCode AS portfolioCode,
# MAGIC         t1.primaryFeatures_portfolioName AS portfolioName,
# MAGIC         t1.primaryFeatures_riskCountry AS countryOfRisk,
# MAGIC         CAST(t1.overallUtilization_marginRatePercentage AS DECIMAL(20,2)) AS margin,
# MAGIC         t1.facilityCodeSerial,
# MAGIC         t1.primaryFeatures_committedFlag,
# MAGIC         t1.dates_availabilityEndDate,
# MAGIC         t1.dates_availabilityStartDate,
# MAGIC         t1.primaryFeatures_allocatingBranch,
# MAGIC         t1.primaryFeatures_allocatingBranchName,
# MAGIC         t1.fundingDealGroupId,
# MAGIC         t1.facilityDescription,
# MAGIC         t1.restrictions_customer_customerNumber
# MAGIC         -- t1.EDL_LOAD_DTS
# MAGIC       FROM facility_USN AS t1
# MAGIC       LEFT JOIN gcds_client_Client AS t2 ON t1.customerNumber = t2.gcid
# MAGIC             ) USN
# MAGIC
# MAGIC       -- USU
# MAGIC
# MAGIC       UNION ALL
# MAGIC     --FACILITY US - Utrecht
# MAGIC     SELECT 'facility_USU' AS sourceSystem, * FROM (
# MAGIC     SELECT DISTINCT  
# MAGIC         CAST(t1.overallUtilization_lastOverdraftDate AS DATE) AS dateOfBreach,
# MAGIC         CAST(t1.overallUtilization_firstOverdraftDate AS DATE) AS dateOfFirstBreach,
# MAGIC         t1.customerNumber AS gcid,
# MAGIC         t1.customerName AS gcidClientName,
# MAGIC         t2.`RM-name` AS relationshipManagerName,
# MAGIC         t1.facilityCode AS facilityCode,
# MAGIC         t1.mainFacilityCode AS mainFacilityCode,
# MAGIC         t1.facilityDescription AS description,
# MAGIC         t1.primaryFeatures_categoryCode AS category,
# MAGIC         t1.statusInformation_status AS facilityStatus,
# MAGIC         t1.statusInformation_recordStatusDescription AS facilityStatusRecordDescription,
# MAGIC         CAST(t1.primaryFeatures_revolvingFlag AS STRING) AS revolving,
# MAGIC         TRIM(t1.limit_currency) AS facilityCurrency,
# MAGIC         CAST(t1.dates_startDate AS DATE) AS facilityStartDate,
# MAGIC         CAST(t1.dates_expiryDate AS DATE) AS facilityExpiryDate,
# MAGIC         CAST(t1.primaryFeatures_availabilityFlag AS STRING) AS availability,
# MAGIC         --limit amount is the limit amount aggreed with the client
# MAGIC         CAST(t1.limit_limitAmount AS DECIMAL(20,2)) AS limitAmount,
# MAGIC         CAST(t1.limit_collateralContributionAmount AS DECIMAL(20,2)) AS collateralAmount,
# MAGIC         --effectiveLimitAmount is the one that he given by the bank, and is usually not more than the limitAmount
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) AS effectiveLimitAmount,
# MAGIC
# MAGIC         --effectiveLimitAmount <> 0 and effectiveLimitAmount - utilisation as effectiveLimitAmountBreach
# MAGIC
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) <> 0 AND (  CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS effectiveLimitAmountBreach,
# MAGIC
# MAGIC         --effectiveLimitAmount = 0 and intraDayLimitAmount - utilisation as intraDayLimitAmountBreach
# MAGIC
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) = 0 AND (CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS intraDayLimitAmountBreach,
# MAGIC         
# MAGIC         CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) < 0 AS availableAmountBreach,
# MAGIC         CASE 
# MAGIC         WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) = 0 THEN CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) + CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2))
# MAGIC         WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) <> 0 THEN  CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2))
# MAGIC         END AS newAvailableAmount,
# MAGIC         CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) AS availableAmount,
# MAGIC         --CAST(t1.overallUtilization_totalUtilizedAmount AS DECIMAL(20,2)) AS oldUtilisation,
# MAGIC         CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) AS utilisation,
# MAGIC         CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) AS unavailableAmount,
# MAGIC         CAST(t1.overallUtilization_totalWithheldAmount AS DECIMAL(20,2)) AS withheldAmount,
# MAGIC         CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) AS intraDayLimitAmount,
# MAGIC         CAST(t1.dates_availabilityStartDate AS DATE) AS intraDayLimitAmountStartdate,
# MAGIC         t1.liabilityCode,
# MAGIC         t1.liabilityName,
# MAGIC         CAST(t1.dates_creditApprovalDate AS DATE) AS creditApprovalDate,
# MAGIC         CAST(t1.dates_availabilityEndDate AS DATE) AS availabilityEndDate,
# MAGIC         t1.primaryFeatures_portfolioCode AS portfolioCode,
# MAGIC         t1.primaryFeatures_portfolioName AS portfolioName,
# MAGIC         t1.primaryFeatures_riskCountry AS countryOfRisk,
# MAGIC         CAST(t1.overallUtilization_marginRatePercentage AS DECIMAL(20,2)) AS margin,
# MAGIC         t1.facilityCodeSerial,
# MAGIC         t1.primaryFeatures_committedFlag,
# MAGIC         t1.dates_availabilityEndDate,
# MAGIC         t1.dates_availabilityStartDate,
# MAGIC         t1.primaryFeatures_allocatingBranch,
# MAGIC         t1.primaryFeatures_allocatingBranchName,
# MAGIC         t1.fundingDealGroupId,
# MAGIC         t1.facilityDescription,
# MAGIC         t1.restrictions_customer_customerNumber
# MAGIC         -- t1.EDL_LOAD_DTS
# MAGIC       FROM facility_USU AS t1
# MAGIC       LEFT JOIN gcds_client_Client AS t2 ON t1.customerNumber = t2.gcid
# MAGIC             ) USU
# MAGIC
# MAGIC
# MAGIC         --CAT
# MAGIC
# MAGIC         UNION ALL
# MAGIC     --FACILITY CANADA TORONTO
# MAGIC     SELECT 'facility_CAT' AS sourceSystem, * FROM (
# MAGIC     SELECT DISTINCT  
# MAGIC         CAST(t1.overallUtilization_lastOverdraftDate AS DATE) AS dateOfBreach,
# MAGIC         CAST(t1.overallUtilization_firstOverdraftDate AS DATE) AS dateOfFirstBreach,
# MAGIC         t1.customerNumber AS gcid,
# MAGIC         t1.customerName AS gcidClientName,
# MAGIC         t2.`RM-name` AS relationshipManagerName,
# MAGIC         t1.facilityCode AS facilityCode,
# MAGIC         t1.mainFacilityCode AS mainFacilityCode,
# MAGIC         t1.facilityDescription AS description,
# MAGIC         t1.primaryFeatures_categoryCode AS category,
# MAGIC         t1.statusInformation_status AS facilityStatus,
# MAGIC         t1.statusInformation_recordStatusDescription AS facilityStatusRecordDescription,
# MAGIC         CAST(t1.primaryFeatures_revolvingFlag AS STRING) AS revolving,
# MAGIC         TRIM(t1.limit_currency) AS facilityCurrency,
# MAGIC         CAST(t1.dates_startDate AS DATE) AS facilityStartDate,
# MAGIC         CAST(t1.dates_expiryDate AS DATE) AS facilityExpiryDate,
# MAGIC         CAST(t1.primaryFeatures_availabilityFlag AS STRING) AS availability,
# MAGIC         --limit amount is the limit amount aggreed with the client
# MAGIC         CAST(t1.limit_limitAmount AS DECIMAL(20,2)) AS limitAmount,
# MAGIC         CAST(t1.limit_collateralContributionAmount AS DECIMAL(20,2)) AS collateralAmount,
# MAGIC         --effectiveLimitAmount is the one that he given by the bank, and is usually not more than the limitAmount
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) AS effectiveLimitAmount,
# MAGIC
# MAGIC         --effectiveLimitAmount <> 0 and effectiveLimitAmount - utilisation as effectiveLimitAmountBreach
# MAGIC
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) <> 0 AND (  CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS effectiveLimitAmountBreach,
# MAGIC
# MAGIC         --effectiveLimitAmount = 0 and intraDayLimitAmount - utilisation as intraDayLimitAmountBreach
# MAGIC
# MAGIC         CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) - CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) = 0 AND (CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) -  CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) < 0) AS intraDayLimitAmountBreach,
# MAGIC         
# MAGIC         CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) < 0 AS availableAmountBreach,
# MAGIC         CASE 
# MAGIC         WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) = 0 THEN CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) + CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2))
# MAGIC         WHEN CAST(t1.limit_effectiveLimitAmount AS DECIMAL(20,2)) <> 0 THEN  CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2))
# MAGIC         END AS newAvailableAmount,
# MAGIC         CAST(t1.overallUtilization_availableAmount AS DECIMAL(20,2)) AS availableAmount,
# MAGIC         --CAST(t1.overallUtilization_totalUtilizedAmount AS DECIMAL(20,2)) AS oldUtilisation,
# MAGIC         CAST(t1.overallUtilization_totalAndTankedUtilizationAmount AS DECIMAL(20,2)) AS utilisation,
# MAGIC         CAST(t1.overallUtilization_unavailableAmount AS DECIMAL(20,2)) AS unavailableAmount,
# MAGIC         CAST(t1.overallUtilization_totalWithheldAmount AS DECIMAL(20,2)) AS withheldAmount,
# MAGIC         CAST(t1.limit_intraDayLimitAmount AS DECIMAL(20,2)) AS intraDayLimitAmount,
# MAGIC         CAST(t1.dates_availabilityStartDate AS DATE) AS intraDayLimitAmountStartdate,
# MAGIC         t1.liabilityCode,
# MAGIC         t1.liabilityName,
# MAGIC         CAST(t1.dates_creditApprovalDate AS DATE) AS creditApprovalDate,
# MAGIC         CAST(t1.dates_availabilityEndDate AS DATE) AS availabilityEndDate,
# MAGIC         t1.primaryFeatures_portfolioCode AS portfolioCode,
# MAGIC         t1.primaryFeatures_portfolioName AS portfolioName,
# MAGIC         t1.primaryFeatures_riskCountry AS countryOfRisk,
# MAGIC         CAST(t1.overallUtilization_marginRatePercentage AS DECIMAL(20,2)) AS margin,
# MAGIC         t1.facilityCodeSerial,
# MAGIC         t1.primaryFeatures_committedFlag,
# MAGIC         t1.dates_availabilityEndDate,
# MAGIC         t1.dates_availabilityStartDate,
# MAGIC         t1.primaryFeatures_allocatingBranch,
# MAGIC         t1.primaryFeatures_allocatingBranchName,
# MAGIC         t1.fundingDealGroupId,
# MAGIC         t1.facilityDescription,
# MAGIC         t1.restrictions_customer_customerNumber
# MAGIC         -- t1.EDL_LOAD_DTS
# MAGIC       FROM facility_CAT AS t1
# MAGIC       LEFT JOIN gcds_client_Client AS t2 ON t1.customerNumber = t2.gcid
# MAGIC             ) CAT
# MAGIC
# MAGIC         )
# MAGIC     )
# MAGIC SELECT DISTINCT
# MAGIC     t1.*,
# MAGIC     CASE
# MAGIC         WHEN t1.facilityCurrency = 'EUR' THEN CAST(t1.newAvailableAmount AS DECIMAL(20,2))
# MAGIC         ELSE CAST(t1.newAvailableAmount / NULLIF(CAST(t2.Close_2100CET AS FLOAT), 0) AS DECIMAL(20,2))
# MAGIC     END AS newAvailableAmountEuro,
# MAGIC     CASE
# MAGIC         WHEN t1.facilityCurrency = 'EUR' THEN CAST(t1.limitAmount AS DECIMAL(20,2))
# MAGIC         ELSE CAST(t1.limitAmount / NULLIF(CAST(t2.Close_2100CET AS FLOAT), 0) AS DECIMAL(20,2))
# MAGIC     END AS limitAmountEUR,
# MAGIC     CASE
# MAGIC         WHEN t1.facilityCurrency = 'EUR' THEN CAST(t1.utilisation AS DECIMAL(20,2))
# MAGIC         ELSE CAST(t1.utilisation / NULLIF(CAST(t2.Close_2100CET AS FLOAT), 0) AS DECIMAL(20,2))
# MAGIC     END AS utilisationEUR,   
# MAGIC     CASE
# MAGIC         WHEN t1.facilityCurrency = 'EUR' THEN CAST(1 AS DECIMAL(20,2))
# MAGIC         ELSE CAST(NULLIF(CAST(t2.Close_2100CET AS FLOAT), 0) AS DECIMAL(20,2))
# MAGIC     END AS conversionRate
# MAGIC
# MAGIC FROM newAvailableAmount_CTE t1
# MAGIC LEFT JOIN exchange_rates t2 ON t1.facilityCurrency = TRIM(t2.currency_codes);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC The below code unions loan for all the branches that are used by region E&A and Asia

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW facility_loan_data AS
# MAGIC --Loan NLU
# MAGIC SELECT * FROM (
# MAGIC   SELECT
# MAGIC      'Loan_NLU' AS sourceSystem,
# MAGIC      facilityCodeSerial,
# MAGIC      secondaryFeatures_financeProductTypeCode,
# MAGIC      secondaryFeatures_financeProductTypeDescription,
# MAGIC      branch,
# MAGIC      branchName,
# MAGIC      facilityDescription,
# MAGIC      primaryFeatures_portfolioName,
# MAGIC      statusInformation_statusEffectiveDate,
# MAGIC      tenor_bookDate,
# MAGIC      tenor_maturityDate,
# MAGIC      tenor_valueDate,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_schedules_frequencyUnitDescription) > 0 
# MAGIC         THEN element_at(positionBalances_schedules_frequencyUnitDescription, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS STRING
# MAGIC     ) AS positionBalances_schedules_frequencyUnitDescription,     
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_localCurrencyPrincipalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_localCurrencyPrincipalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_localCurrencyPrincipalAmount,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_principalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_principalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_principalAmountEUR,
# MAGIC      CASE
# MAGIC        WHEN size(positionBalances_amounts_currency) > 0
# MAGIC        THEN element_at(positionBalances_amounts_currency, -1)
# MAGIC        ELSE NULL
# MAGIC      END AS positionBalances_amounts_currency,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_currentOutstandingBalanceAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_currentOutstandingBalanceAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_currentOutstandingBalanceAmount,
# MAGIC      primaryFeatures_categoryName,
# MAGIC      primaryFeatures_dealTypeName,
# MAGIC      primaryFeatures_portfolioCode,
# MAGIC      primaryFeatures_typeCode,
# MAGIC      agent_agentName,
# MAGIC      agent_agentNumber,
# MAGIC      customerName,
# MAGIC      customerNumber,
# MAGIC      loanCode,
# MAGIC       CASE
# MAGIC         WHEN size(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL)) > 0
# MAGIC         THEN element_at(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL),-1)
# MAGIC         ELSE NULL
# MAGIC       END AS positionBalances_riskFreeRate_creditAdjustmentSpread
# MAGIC   FROM loan_NLU
# MAGIC
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC UNION ALL
# MAGIC --Loan Singapore
# MAGIC   SELECT
# MAGIC      'loan_SGP' AS sourceSystem,
# MAGIC      facilityCodeSerial,
# MAGIC      secondaryFeatures_financeProductTypeCode,
# MAGIC      secondaryFeatures_financeProductTypeDescription,
# MAGIC      branch,
# MAGIC      branchName,
# MAGIC      facilityDescription,
# MAGIC      primaryFeatures_portfolioName,
# MAGIC      statusInformation_statusEffectiveDate,
# MAGIC      tenor_bookDate,
# MAGIC      tenor_maturityDate,
# MAGIC      tenor_valueDate,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_schedules_frequencyUnitDescription) > 0 
# MAGIC         THEN element_at(positionBalances_schedules_frequencyUnitDescription, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS STRING
# MAGIC     ) AS positionBalances_schedules_frequencyUnitDescription,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_localCurrencyPrincipalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_localCurrencyPrincipalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_localCurrencyPrincipalAmount,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_principalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_principalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_principalAmountEUR,
# MAGIC       CASE
# MAGIC        WHEN size(positionBalances_amounts_currency) > 0
# MAGIC        THEN element_at(positionBalances_amounts_currency, -1)
# MAGIC        ELSE NULL
# MAGIC      END AS positionBalances_amounts_currency,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_currentOutstandingBalanceAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_currentOutstandingBalanceAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_currentOutstandingBalanceAmount,
# MAGIC      primaryFeatures_categoryName,
# MAGIC      primaryFeatures_dealTypeName,
# MAGIC      primaryFeatures_portfolioCode,
# MAGIC      primaryFeatures_typeCode,
# MAGIC      agent_agentName,
# MAGIC      agent_agentNumber,
# MAGIC      customerName,
# MAGIC      customerNumber,
# MAGIC      loanCode,
# MAGIC      CASE
# MAGIC         WHEN size(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL)) > 0
# MAGIC         THEN element_at(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL),-1)
# MAGIC         ELSE NULL
# MAGIC       END AS positionBalances_riskFreeRate_creditAdjustmentSpread
# MAGIC   FROM loan_SGP
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC UNION ALL
# MAGIC --Loan Mumbai
# MAGIC   SELECT
# MAGIC      'loan_INM' AS sourceSystem,
# MAGIC      facilityCodeSerial,
# MAGIC      secondaryFeatures_financeProductTypeCode,
# MAGIC      secondaryFeatures_financeProductTypeDescription,
# MAGIC      branch,
# MAGIC      branchName,
# MAGIC      facilityDescription,
# MAGIC      primaryFeatures_portfolioName,
# MAGIC      statusInformation_statusEffectiveDate,
# MAGIC      tenor_bookDate,
# MAGIC      tenor_maturityDate,
# MAGIC      tenor_valueDate,
# MAGIC     CAST(
# MAGIC      CASE 
# MAGIC         WHEN size(positionBalances_schedules_frequencyUnitDescription) > 0 
# MAGIC         THEN element_at(positionBalances_schedules_frequencyUnitDescription, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS STRING
# MAGIC     ) AS positionBalances_schedules_frequencyUnitDescription, 
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_localCurrencyPrincipalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_localCurrencyPrincipalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_localCurrencyPrincipalAmount,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_principalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_principalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_principalAmountEUR,
# MAGIC       CASE
# MAGIC        WHEN size(positionBalances_amounts_currency) > 0
# MAGIC        THEN element_at(positionBalances_amounts_currency, -1)
# MAGIC        ELSE NULL
# MAGIC      END AS positionBalances_amounts_currency,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_currentOutstandingBalanceAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_currentOutstandingBalanceAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_currentOutstandingBalanceAmount,
# MAGIC      primaryFeatures_categoryName,
# MAGIC      primaryFeatures_dealTypeName,
# MAGIC      primaryFeatures_portfolioCode,
# MAGIC      primaryFeatures_typeCode,
# MAGIC      agent_agentName,
# MAGIC      agent_agentNumber,
# MAGIC      customerName,
# MAGIC      CAST(customerNumber AS STRING),
# MAGIC      loanCode,
# MAGIC      CASE
# MAGIC         WHEN size(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL)) > 0
# MAGIC         THEN element_at(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL),-1)
# MAGIC         ELSE NULL
# MAGIC       END AS positionBalances_riskFreeRate_creditAdjustmentSpread
# MAGIC   FROM loan_INM
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC   UNION ALL
# MAGIC --Loan Hong Kong
# MAGIC   SELECT
# MAGIC      'Loan_HKG' AS sourceSystem,
# MAGIC      facilityCodeSerial,
# MAGIC      secondaryFeatures_financeProductTypeCode,
# MAGIC      secondaryFeatures_financeProductTypeDescription,
# MAGIC      branch,
# MAGIC      branchName,
# MAGIC      facilityDescription,
# MAGIC      primaryFeatures_portfolioName,
# MAGIC      statusInformation_statusEffectiveDate,
# MAGIC      tenor_bookDate,
# MAGIC      tenor_maturityDate,
# MAGIC      tenor_valueDate,
# MAGIC     CAST(
# MAGIC      CASE 
# MAGIC         WHEN size(positionBalances_schedules_frequencyUnitDescription) > 0 
# MAGIC         THEN element_at(positionBalances_schedules_frequencyUnitDescription, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS STRING
# MAGIC     ) AS positionBalances_schedules_frequencyUnitDescription, 
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_localCurrencyPrincipalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_localCurrencyPrincipalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_localCurrencyPrincipalAmount,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_principalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_principalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_principalAmountEUR,
# MAGIC       CASE
# MAGIC        WHEN size(positionBalances_amounts_currency) > 0
# MAGIC        THEN element_at(positionBalances_amounts_currency, -1)
# MAGIC        ELSE NULL
# MAGIC      END AS positionBalances_amounts_currency,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_currentOutstandingBalanceAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_currentOutstandingBalanceAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_currentOutstandingBalanceAmount,
# MAGIC      primaryFeatures_categoryName,
# MAGIC      primaryFeatures_dealTypeName,
# MAGIC      primaryFeatures_portfolioCode,
# MAGIC      primaryFeatures_typeCode,
# MAGIC      agent_agentName,
# MAGIC      agent_agentNumber,
# MAGIC      customerName,
# MAGIC      customerNumber,
# MAGIC      loanCode,
# MAGIC      CASE
# MAGIC         WHEN size(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL)) > 0
# MAGIC         THEN element_at(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL),-1)
# MAGIC         ELSE NULL
# MAGIC       END AS positionBalances_riskFreeRate_creditAdjustmentSpread
# MAGIC   FROM loan_HKG
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC   UNION ALL
# MAGIC --Loan China
# MAGIC   SELECT
# MAGIC      'Loan_CNS' AS sourceSystem,
# MAGIC      facilityCodeSerial,
# MAGIC      secondaryFeatures_financeProductTypeCode,
# MAGIC      secondaryFeatures_financeProductTypeDescription,
# MAGIC      branch,
# MAGIC      branchName,
# MAGIC      facilityDescription,
# MAGIC      primaryFeatures_portfolioName,
# MAGIC      statusInformation_statusEffectiveDate,
# MAGIC      tenor_bookDate,
# MAGIC      tenor_maturityDate,
# MAGIC      tenor_valueDate,
# MAGIC     CAST(
# MAGIC      CASE 
# MAGIC         WHEN size(positionBalances_schedules_frequencyUnitDescription) > 0 
# MAGIC         THEN element_at(positionBalances_schedules_frequencyUnitDescription, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS STRING
# MAGIC     ) AS positionBalances_schedules_frequencyUnitDescription, 
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_localCurrencyPrincipalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_localCurrencyPrincipalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_localCurrencyPrincipalAmount,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_principalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_principalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_principalAmountEUR,
# MAGIC           CASE
# MAGIC        WHEN size(positionBalances_amounts_currency) > 0
# MAGIC        THEN element_at(positionBalances_amounts_currency, -1)
# MAGIC        ELSE NULL
# MAGIC      END AS positionBalances_amounts_currency,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_currentOutstandingBalanceAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_currentOutstandingBalanceAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_currentOutstandingBalanceAmount,
# MAGIC      primaryFeatures_categoryName,
# MAGIC      primaryFeatures_dealTypeName,
# MAGIC      primaryFeatures_portfolioCode,
# MAGIC      primaryFeatures_typeCode,
# MAGIC      agent_agentName,
# MAGIC      agent_agentNumber,
# MAGIC      customerName,
# MAGIC      customerNumber,
# MAGIC      loanCode,
# MAGIC      CASE
# MAGIC         WHEN size(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL)) > 0
# MAGIC         THEN element_at(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL),-1)
# MAGIC         ELSE NULL
# MAGIC       END AS positionBalances_riskFreeRate_creditAdjustmentSpread
# MAGIC   FROM loan_CNS
# MAGIC
# MAGIC     UNION ALL
# MAGIC     --------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC --Loan GREAT BRITAIN
# MAGIC   SELECT
# MAGIC      'Loan_GBL' AS sourceSystem,
# MAGIC      facilityCodeSerial,
# MAGIC      secondaryFeatures_financeProductTypeCode,
# MAGIC      secondaryFeatures_financeProductTypeDescription,
# MAGIC      branch,
# MAGIC      branchName,
# MAGIC      facilityDescription,
# MAGIC      primaryFeatures_portfolioName,
# MAGIC      statusInformation_statusEffectiveDate,
# MAGIC      tenor_bookDate,
# MAGIC      tenor_maturityDate,
# MAGIC      tenor_valueDate,
# MAGIC     CAST(
# MAGIC      CASE 
# MAGIC         WHEN size(positionBalances_schedules_frequencyUnitDescription) > 0 
# MAGIC         THEN element_at(positionBalances_schedules_frequencyUnitDescription, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS STRING
# MAGIC     ) AS positionBalances_schedules_frequencyUnitDescription, 
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_localCurrencyPrincipalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_localCurrencyPrincipalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_localCurrencyPrincipalAmount,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_principalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_principalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_principalAmountEUR,
# MAGIC           CASE
# MAGIC        WHEN size(positionBalances_amounts_currency) > 0
# MAGIC        THEN element_at(positionBalances_amounts_currency, -1)
# MAGIC        ELSE NULL
# MAGIC      END AS positionBalances_amounts_currency,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_currentOutstandingBalanceAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_currentOutstandingBalanceAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_currentOutstandingBalanceAmount,
# MAGIC      primaryFeatures_categoryName,
# MAGIC      primaryFeatures_dealTypeName,
# MAGIC      primaryFeatures_portfolioCode,
# MAGIC      primaryFeatures_typeCode,
# MAGIC      agent_agentName,
# MAGIC      agent_agentNumber,
# MAGIC      customerName,
# MAGIC      customerNumber,
# MAGIC      loanCode,
# MAGIC      CASE
# MAGIC         WHEN size(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL)) > 0
# MAGIC         THEN element_at(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL),-1)
# MAGIC         ELSE NULL
# MAGIC       END AS positionBalances_riskFreeRate_creditAdjustmentSpread
# MAGIC   FROM loan_GBL
# MAGIC
# MAGIC    UNION ALL
# MAGIC     --------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC --Loan USN
# MAGIC   SELECT
# MAGIC      'Loan_USN' AS sourceSystem,
# MAGIC      facilityCodeSerial,
# MAGIC      secondaryFeatures_financeProductTypeCode,
# MAGIC      secondaryFeatures_financeProductTypeDescription,
# MAGIC      branch,
# MAGIC      branchName,
# MAGIC      facilityDescription,
# MAGIC      primaryFeatures_portfolioName,
# MAGIC      statusInformation_statusEffectiveDate,
# MAGIC      tenor_bookDate,
# MAGIC      tenor_maturityDate,
# MAGIC      tenor_valueDate,
# MAGIC     CAST(
# MAGIC      CASE 
# MAGIC         WHEN size(positionBalances_schedules_frequencyUnitDescription) > 0 
# MAGIC         THEN element_at(positionBalances_schedules_frequencyUnitDescription, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS STRING
# MAGIC     ) AS positionBalances_schedules_frequencyUnitDescription, 
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_localCurrencyPrincipalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_localCurrencyPrincipalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_localCurrencyPrincipalAmount,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_principalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_principalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_principalAmountEUR,
# MAGIC           CASE
# MAGIC        WHEN size(positionBalances_amounts_currency) > 0
# MAGIC        THEN element_at(positionBalances_amounts_currency, -1)
# MAGIC        ELSE NULL
# MAGIC      END AS positionBalances_amounts_currency,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_currentOutstandingBalanceAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_currentOutstandingBalanceAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_currentOutstandingBalanceAmount,
# MAGIC      primaryFeatures_categoryName,
# MAGIC      primaryFeatures_dealTypeName,
# MAGIC      primaryFeatures_portfolioCode,
# MAGIC      primaryFeatures_typeCode,
# MAGIC      agent_agentName,
# MAGIC      agent_agentNumber,
# MAGIC      customerName,
# MAGIC      customerNumber,
# MAGIC      loanCode,
# MAGIC      CASE
# MAGIC         WHEN size(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL)) > 0
# MAGIC         THEN element_at(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL),-1)
# MAGIC         ELSE NULL
# MAGIC       END AS positionBalances_riskFreeRate_creditAdjustmentSpread
# MAGIC   FROM loan_USN
# MAGIC
# MAGIC    UNION ALL
# MAGIC     --------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC --Loan USU
# MAGIC   SELECT
# MAGIC      'Loan_USU' AS sourceSystem,
# MAGIC      facilityCodeSerial,
# MAGIC      secondaryFeatures_financeProductTypeCode,
# MAGIC      secondaryFeatures_financeProductTypeDescription,
# MAGIC      branch,
# MAGIC      branchName,
# MAGIC      facilityDescription,
# MAGIC      primaryFeatures_portfolioName,
# MAGIC      statusInformation_statusEffectiveDate,
# MAGIC      tenor_bookDate,
# MAGIC      tenor_maturityDate,
# MAGIC      tenor_valueDate,
# MAGIC     CAST(
# MAGIC      CASE 
# MAGIC         WHEN size(positionBalances_schedules_frequencyUnitDescription) > 0 
# MAGIC         THEN element_at(positionBalances_schedules_frequencyUnitDescription, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS STRING
# MAGIC     ) AS positionBalances_schedules_frequencyUnitDescription, 
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_localCurrencyPrincipalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_localCurrencyPrincipalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_localCurrencyPrincipalAmount,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_principalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_principalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_principalAmountEUR,
# MAGIC           CASE
# MAGIC        WHEN size(positionBalances_amounts_currency) > 0
# MAGIC        THEN element_at(positionBalances_amounts_currency, -1)
# MAGIC        ELSE NULL
# MAGIC      END AS positionBalances_amounts_currency,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_currentOutstandingBalanceAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_currentOutstandingBalanceAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_currentOutstandingBalanceAmount,
# MAGIC      primaryFeatures_categoryName,
# MAGIC      primaryFeatures_dealTypeName,
# MAGIC      primaryFeatures_portfolioCode,
# MAGIC      primaryFeatures_typeCode,
# MAGIC      agent_agentName,
# MAGIC      agent_agentNumber,
# MAGIC      customerName,
# MAGIC      customerNumber,
# MAGIC      loanCode,
# MAGIC      CASE
# MAGIC         WHEN size(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL)) > 0
# MAGIC         THEN element_at(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL),-1)
# MAGIC         ELSE NULL
# MAGIC       END AS positionBalances_riskFreeRate_creditAdjustmentSpread
# MAGIC   FROM loan_USU
# MAGIC
# MAGIC    UNION ALL
# MAGIC     --------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC --Loan CAT
# MAGIC   SELECT
# MAGIC      'Loan_CAT' AS sourceSystem,
# MAGIC      facilityCodeSerial,
# MAGIC      secondaryFeatures_financeProductTypeCode,
# MAGIC      secondaryFeatures_financeProductTypeDescription,
# MAGIC      branch,
# MAGIC      branchName,
# MAGIC      facilityDescription,
# MAGIC      primaryFeatures_portfolioName,
# MAGIC      statusInformation_statusEffectiveDate,
# MAGIC      tenor_bookDate,
# MAGIC      tenor_maturityDate,
# MAGIC      tenor_valueDate,
# MAGIC     CAST(
# MAGIC      CASE 
# MAGIC         WHEN size(positionBalances_schedules_frequencyUnitDescription) > 0 
# MAGIC         THEN element_at(positionBalances_schedules_frequencyUnitDescription, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS STRING
# MAGIC     ) AS positionBalances_schedules_frequencyUnitDescription, 
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_localCurrencyPrincipalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_localCurrencyPrincipalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_localCurrencyPrincipalAmount,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_principalAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_principalAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_principalAmountEUR,
# MAGIC           CASE
# MAGIC        WHEN size(positionBalances_amounts_currency) > 0
# MAGIC        THEN element_at(positionBalances_amounts_currency, -1)
# MAGIC        ELSE NULL
# MAGIC      END AS positionBalances_amounts_currency,
# MAGIC     CAST(
# MAGIC       CASE 
# MAGIC         WHEN size(positionBalances_amounts_currentOutstandingBalanceAmount) > 0 
# MAGIC         THEN element_at(positionBalances_amounts_currentOutstandingBalanceAmount, -1)
# MAGIC         ELSE NULL 
# MAGIC       END AS DECIMAL(20, 2)
# MAGIC     ) AS positionBalances_amounts_currentOutstandingBalanceAmount,
# MAGIC      primaryFeatures_categoryName,
# MAGIC      primaryFeatures_dealTypeName,
# MAGIC      primaryFeatures_portfolioCode,
# MAGIC      primaryFeatures_typeCode,
# MAGIC      agent_agentName,
# MAGIC      agent_agentNumber,
# MAGIC      customerName,
# MAGIC      customerNumber,
# MAGIC      loanCode,
# MAGIC      CASE
# MAGIC         WHEN size(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL)) > 0
# MAGIC         THEN element_at(filter(positionBalances_riskFreeRate_creditAdjustmentSpread, x -> x IS NOT NULL),-1)
# MAGIC         ELSE NULL
# MAGIC       END AS positionBalances_riskFreeRate_creditAdjustmentSpread
# MAGIC   FROM loan_CAT
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logic for Limit Breach 
# MAGIC - intraday limit amount - utilization amount < 0 ----> breach, you want to see this on the app
# MAGIC - intraday limit amount - utilization amount > 0 ----> not a breach, you dont want to see this on the app
# MAGIC - when there is no intraday limit amount then it checks effective limit amount - utilization amount < 0 ----> breach, you want to see this on the app
# MAGIC - when there is no intraday limit amount then it checks effective limit amount - utilization amount > 0 ----> not a breach, you dont want to see this on the app

# COMMAND ----------

# DBTITLE 1,Outputs Final facility dataset as a View with latest EDL load Date
# MAGIC %sql
# MAGIC --To take the latest edl load date which is used for Last Refresh Date visual in PowerBI
# MAGIC CREATE OR REPLACE TEMPORARY VIEW facility_limit_data_new AS
# MAGIC SELECT
# MAGIC   t1.*,
# MAGIC   (SELECT MAX(EDL_LOAD_DTS) FROM facility_NLU) AS EDL_LOAD_DTS
# MAGIC FROM facility_limit_data AS t1;
# MAGIC

# COMMAND ----------

spark.sql('select * from facility_limit_data_new').write.mode('overwrite').saveAsTable('wr_fj_parties_and_risk_assessment_preprd.financing.facility_limit')
spark.sql('select * from facility_loan_data').write.mode('overwrite').saveAsTable('wr_fj_parties_and_risk_assessment_preprd.financing.facility_loan')

# COMMAND ----------

display( spark.sql("""
                   SELECT
     'Loan_NLU' AS sourceSystem,
     positionBalances_schedules_frequencyUnitDescription,
     positionBalances_amounts_localCurrencyPrincipalAmount,
    CAST(
      CASE 
        WHEN size(positionBalances_amounts_localCurrencyPrincipalAmount) > 0 
        THEN element_at(positionBalances_amounts_localCurrencyPrincipalAmount, -1)
        ELSE NULL 
      END AS DECIMAL(20, 2)
    ) AS positionBalances_amounts_localCurrencyPrincipalAmount_lastElement


  FROM loan_NLU 
  """  ))

# COMMAND ----------


