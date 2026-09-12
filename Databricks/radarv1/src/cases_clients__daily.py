# Databricks notebook source
import os

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

jdbcHostname='firebirdsqlserverprodnla.database.windows.net'
ServiceKey='app-reg-databricks-wr-radar-preprd'

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

# DBTITLE 1,load gcob objects from gdp

#from datetime import datetime, timedelta
#import re

#gcob_objects = [
#    'party_case_client_details'
#    , 'party_workitem'
#    , 'party_local_client_Owners'
#    , 'party_request_for_information'
#    , 'party_client'
#    , 'party_products_and_services'
#    , 'party_trade_name'
#    , 'party_control_measures'
#]

#for item in gcob_objects:
    # get the most recent version available in gdp
#    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/'
#    files = dbutils.fs.ls(path)
#    version = max([
 #       int(re.search(r'/(\d+)/$', file.path).group(1))
  #      for file in files
#        if re.search(r'/(\d+)/$', file.path)
#    ])

#    version = 101 # TEMPORARY UNTIL THE NEW VERSION IS UP AND RUNNING!!!!

    # get the most recent file available in gdp
#    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/'
#    files = dbutils.fs.ls(path)
#    load_date = max(file.path.split('EDL_LOAD_DTS=')[1][:8] for file in files if 'EDL_LOAD_DTS=' in file.path)

#    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/EDL_LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

#EDL_LoadDate = datetime.strptime(load_date, '%Y%m%d').strftime('%Y-%m-%d')
# throw error if gdp file is older than 1 day!
#today = datetime.now().date()
#load_date_obj = datetime.strptime(load_date, '%Y%m%d').date()
#days_diff = (today - load_date_obj).days

#if days_diff > 1:
    # raise ValueError(f"Latest file date ({EDL_LoadDate}) is more than 1 day old (difference: {days_diff} days)")
#    print(f"Alert: Latest file for {item} is more than 1 day old (date: {load_date_obj}, difference: {days_diff} days)")

# COMMAND ----------

from datetime import datetime, timedelta
import re

gcob_objects = [
    'CaseService_case_ControlMeasure'
]

for item in gcob_objects:
    # get the most recent version available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/'
    files = dbutils.fs.ls(path)
    version = max([
        int(re.search(r'/(\d+)/$', file.path).group(1))
        for file in files
        if re.search(r'/(\d+)/$', file.path)
    ])

    version = 100 # TEMPORARY UNTIL THE NEW VERSION IS UP AND RUNNING!!!!

    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOADED_DTS=')[1][:8] for file in files if 'LOADED_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/{version}/data/LOADED_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

EDL_LoadDate = datetime.strptime(load_date, '%Y%m%d').strftime('%Y-%m-%d')
# throw error if gdp file is older than 1 day!
today = datetime.now().date()
load_date_obj = datetime.strptime(load_date, '%Y%m%d').date()
days_diff = (today - load_date_obj).days

if days_diff > 1:
    # raise ValueError(f"Latest file date ({EDL_LoadDate}) is more than 1 day old (difference: {days_diff} days)")
    print(f"Alert: Latest file for {item} is more than 1 day old (date: {load_date_obj}, difference: {days_diff} days)")

# COMMAND ----------

# DBTITLE 1,GCDS data for Primary Naics (select tables)
import re

from pyspark.sql import SparkSession
# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_Client',
      'client_KeyStoreKey'
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

  spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data/LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcds_'+ item)

# COMMAND ----------

from datetime import datetime, timedelta
import re

gcob_objects = [
    'party_case_client_details'
    , 'party_workitem'
    , 'party_local_client_Owners'
    , 'party_request_for_information'
    , 'party_client'
    , 'party_products_and_services'
    , 'party_trade_name'
    , 'party_control_measures'
]

for item in gcob_objects:
    # get the most recent version available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/'
    files = dbutils.fs.ls(path)
    version = max([
        int(re.search(r'/(\d+)/$', file.path).group(1))
        for file in files
        if re.search(r'/(\d+)/$', file.path)
    ])

    version = 102 # TEMPORARY UNTIL THE NEW VERSION IS UP AND RUNNING!!!!

    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOAD_DT=')[1][:8] for file in files if 'LOAD_DT=' in file.path)

    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/LOAD_DT={load_date}*/*.parquet').createOrReplaceTempView(item)

EDL_LoadDate = datetime.strptime(load_date, '%Y%m%d').strftime('%Y-%m-%d')
# throw error if gdp file is older than 1 day!
today = datetime.now().date()
load_date_obj = datetime.strptime(load_date, '%Y%m%d').date()
days_diff = (today - load_date_obj).days

if days_diff > 1:
    # raise ValueError(f"Latest file date ({EDL_LoadDate}) is more than 1 day old (difference: {days_diff} days)")
    print(f"Alert: Latest file for {item} is more than 1 day old (date: {load_date_obj}, difference: {days_diff} days)")

# COMMAND ----------

# DBTITLE 1,static tables map for CasePhase and CaseStatusType
Id_list = [i+1 for i in range(9)]
Description_list = ['Prework'
, 'Ready for assessment'
, 'Assessment in progress'
, '4-EYE check'
, 'Sign-off'
, 'Fulfilment'
, 'Completed'
, 'Cancelled']
spark.createDataFrame(zip(Id_list, Description_list), ['PhaseID', 'PhaseName']).createOrReplaceTempView('gcob_static_PhaseLookup')

StatusId_list = [i + 1 for i in range(22)]
Name_list = ['Initiation In Progress'
, 'Ready for KYC assessment'
, 'KYC assessment in progress'
, 'Ready for 4 eye check'
, '4 eye check in progress'
, 'Client owner sign off requested'
, 'Client committee sign off requested'
, 'Product fulfilment in progress'
, 'Completed'
, 'Cancelled'
, 'Migrated'
, 'Ready for identification'
, 'Identification in progress'
, 'Ready for screening'
, 'Screening in progress'
, 'GCOB Review in progress'
, 'Client Owner approval requested'
, 'Local client owner sign off requested'
, 'Ready for Product Offboarding confirmation'
, 'Product Offboarding confirmation in progress'
, 'Product Offboarding in progress'
, 'Senior management sign off requested']
spark.createDataFrame(zip(StatusId_list, Name_list), ['StatusId', 'Name']).createOrReplaceTempView('gcob_static_CaseStatusType')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix userteamregistry dupes

# COMMAND ----------

# DBTITLE 1,userteamregistry dupes
# MAGIC %sql
# MAGIC MERGE INTO radar.userteamregistry AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         id
# MAGIC         , latestTeamStartDate
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             id
# MAGIC             , userEmail
# MAGIC             , teamStartDate
# MAGIC             , MAX(teamStartDate) OVER (PARTITION BY userEmail) AS latestTeamStartDate
# MAGIC             , ROW_NUMBER() OVER (PARTITION BY userEmail ORDER BY teamStartDate DESC) AS rn
# MAGIC             , COUNT(*) OVER (PARTITION BY userEmail) AS openRecordCount
# MAGIC         FROM radar.userteamregistry
# MAGIC         WHERE active = true OR teamEndDate IS NULL
# MAGIC     ) x
# MAGIC     WHERE openRecordCount > 1
# MAGIC         AND rn > 1
# MAGIC ) AS source ON target.id = source.id
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.teamEndDate = source.latestTeamStartDate,
# MAGIC     target.active = false

# COMMAND ----------

# MAGIC %md
# MAGIC ## PortfolioPlanning

# COMMAND ----------

spark.sql(f"""
WITH rn_cte AS (
    SELECT
        t1.UniqueGcobId
        , DATEDIFF(t1.RecordActiveAt, DATE('{EDL_LoadDate}')) AS days_diff
        , t1.SectorTeam
        , t1.ReviewLocation
        , t1.KYCGroup
    FROM radar.masterlistregistry t1
    JOIN radar.onboardings t2 ON t1.UniqueGcobId = t2.UniqueGcobId
    WHERE IsActiveRecord = 1 

    UNION

    SELECT
        UniqueGcobId
        , DATEDIFF(InsertedOnDate, DATE('{EDL_LoadDate}')) AS days_diff
        , SectorTeam
        , ReviewLocation
        , KYCGroup
    FROM radar.onboardings
)

SELECT
    *
    , row_number() OVER (PARTITION BY UniqueGcobId ORDER BY days_diff DESC) AS rn
FROM rn_cte

""").createOrReplaceTempView('onboarding_days_diff')

# COMMAND ----------

# DBTITLE 1,PortfolioPlanning
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW PortfolioPlanning AS
# MAGIC
# MAGIC WITH latest_active_masterlistregistry_cte AS (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM radar.masterlistregistry
# MAGIC   WHERE IsActiveRecord = 1
# MAGIC )
# MAGIC
# MAGIC , client_information_cte AS ( -- get client information from party_case_client_details (inner join with party_client on completed or latest client id) to compare them with masterlistregistry
# MAGIC     SELECT
# MAGIC       CASE 
# MAGIC         WHEN t1.ClientType = 'Legal Entity' THEN t1.GcobId
# MAGIC         ELSE CONCAT('NP_', t1.GcobId)
# MAGIC       END AS UniqueGcobId
# MAGIC       , t1.CaseId
# MAGIC       , t1.FullLegalName
# MAGIC       , t1.ClientType
# MAGIC       , t1.NextReviewDate
# MAGIC       , t1.FIHubIndicator
# MAGIC       , t1.GlobalClientOwner
# MAGIC       , t1.GlobalClientOwnerLocation
# MAGIC       , t1.BusinessLineName
# MAGIC
# MAGIC     FROM party_case_client_details t1
# MAGIC     INNER JOIN party_client t2 ON t1.ClientId = COALESCE(t2.CompletedId, t2.LatestId) AND t1.ClientType = t2.ClientType AND t1.GcobId = t2.GcobId
# MAGIC   )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , CASE 
# MAGIC       WHEN t2.Reason = 'Global File' THEN 1
# MAGIC       WHEN t2.Reason IN ('Offboarding', 'Clean-up') THEN 2
# MAGIC       ELSE NULL
# MAGIC     END AS CategoryNr
# MAGIC   , CASE
# MAGIC       WHEN TRIM(coalesce(t3.SectorTeam, t2.SectorTeam)) = 'EF' AND t1.FIHubIndicator = 'true' THEN 'EF FI'
# MAGIC       WHEN TRIM(coalesce(t3.SectorTeam, t2.SectorTeam)) = 'EF' AND t1.FIHubIndicator = 'false' THEN 'EF Corp'
# MAGIC       
# MAGIC       WHEN TRIM(coalesce(t3.SectorTeam, t2.SectorTeam)) = 'PF' AND t1.FIHubIndicator = 'true' THEN 'PF FI'
# MAGIC       WHEN TRIM(coalesce(t3.SectorTeam, t2.SectorTeam)) = 'PF' AND t1.FIHubIndicator = 'false' THEN 'PF Corp'
# MAGIC
# MAGIC       WHEN TRIM(coalesce(t3.SectorTeam, t2.SectorTeam)) = 'TCF' AND t1.FIHubIndicator = 'true' THEN 'TCF FI'
# MAGIC       WHEN TRIM(coalesce(t3.SectorTeam, t2.SectorTeam)) = 'TCF' AND t1.FIHubIndicator = 'false' THEN 'TCF Corp'
# MAGIC
# MAGIC       WHEN coalesce(t3.SectorTeam, t2.SectorTeam) IS NOT NULL THEN coalesce(t3.SectorTeam, t2.SectorTeam) -- if SectorTeam IS NOT NULL then use SectorTeam, otherwise below conditions
# MAGIC
# MAGIC       WHEN t1.GlobalClientOwnerLocation IN ('Rabobank - USA Rabo AgriFinance', 'Rabobank - RANZ Country Banking and ROS', 'Rabobank Canada(Rural)') THEN 'Rural'
# MAGIC       WHEN t1.GlobalClientOwner = 'Dotse, MK (Mo)' THEN 'Agency'
# MAGIC       WHEN t1.GlobalClientOwnerLocation = 'Rabobank Argentina' THEN 'ARG'
# MAGIC       WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Milan', 'Rabobank Paris', 'Rabobank Madrid', 'Rabobank Dublin', 'Rabobank Antwerp', 'Rabobank Turkey', 'Rabobank Frankfurt') THEN 'Core Lending'
# MAGIC       WHEN t1.GlobalClientOwnerLocation = 'Rabobank Frankfurt' AND t1.GlobalClientOwner IN ('Meurichy de, K (Koen)') THEN 'Int. Desk'
# MAGIC       WHEN t1.GlobalClientOwner IN ('Willmott, A (Adam)', 'Oord van, JA (Marco)', 'Erkamp, M (Maarten)') THEN 'Sponsor Coverage'
# MAGIC       
# MAGIC       WHEN t1.BusinessLineName = 'Rabo Foundation' THEN 'Foundation'
# MAGIC
# MAGIC       WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Canada (RCBR)', 'Rabobank - USA Rabo AgriFinance', 'Rabo Securities USA, Inc. (RSEC)', 'Rabobank New York', 'Rabobank Canada(Rural)') AND t1.BusinessLineName IN ('GCC - Food & Agri Coverage', 'GCC - Global Loan Products Group') THEN 'Core Lending' 
# MAGIC
# MAGIC       WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Canada (RCBR)', 'Rabobank - USA Rabo AgriFinance', 'Rabo Securities USA, Inc. (RSEC)', 'Rabobank New York', 'Rabobank Canada(Rural)') AND t1.BusinessLineName IN ('GWPC - Trade & Commodity Finance') AND t1.FIHubIndicator = 'false' THEN 'TCF Corp'
# MAGIC
# MAGIC       WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Canada (RCBR)', 'Rabobank - USA Rabo AgriFinance', 'Rabo Securities USA, Inc. (RSEC)', 'Rabobank New York', 'Rabobank Canada(Rural)') AND t1.BusinessLineName IN ('GWPC - Trade & Commodity Finance') AND t1.FIHubIndicator = 'true' THEN 'TCF FI'
# MAGIC
# MAGIC       WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Canada (RCBR)', 'Rabobank - USA Rabo AgriFinance', 'Rabo Securities USA, Inc. (RSEC)', 'Rabobank New York', 'Rabobank Canada(Rural)') AND t1.BusinessLineName IN ('GWPC - Export and Project Finance') AND t1.FIHubIndicator = 'false' THEN 'PF Corp'
# MAGIC
# MAGIC       WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Canada (RCBR)', 'Rabobank - USA Rabo AgriFinance', 'Rabo Securities USA, Inc. (RSEC)', 'Rabobank New York', 'Rabobank Canada(Rural)') AND t1.BusinessLineName IN ('GWPC - Export and Project Finance') AND t1.FIHubIndicator = 'true' THEN 'PF FI'
# MAGIC
# MAGIC       WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Canada (RCBR)', 'Rabobank - USA Rabo AgriFinance', 'Rabo Securities USA, Inc. (RSEC)', 'Rabobank New York', 'Rabobank Canada(Rural)') AND t1.BusinessLineName IN ('GCC - Private Equity') THEN 'Sponsor Coverage'
# MAGIC
# MAGIC       WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Canada (RCBR)', 'Rabobank - USA Rabo AgriFinance', 'Rabo Securities USA, Inc. (RSEC)', 'Rabobank New York', 'Rabobank Canada(Rural)') AND t1.BusinessLineName IN ('GWPC - Markets') THEN 'Markets'
# MAGIC
# MAGIC       ELSE coalesce(t3.SectorTeam, t2.SectorTeam) -- including sectorteam from onboarding pl app!
# MAGIC     END AS SectorTeam
# MAGIC
# MAGIC   , coalesce(t3.KYCGroup, t2.KYCGroup) AS KYCGroup
# MAGIC   , CASE
# MAGIC       WHEN t2.ClientCaseInitiationStart IS NULL
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Monday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN t2.ClientCaseInitiationStart IS NULL
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Tuesday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 91), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN t2.ClientCaseInitiationStart IS NULL
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Wednesday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 92), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN t2.ClientCaseInitiationStart IS NULL
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Thursday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 93), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN t2.ClientCaseInitiationStart IS NULL
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Friday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 94), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN t2.ClientCaseInitiationStart IS NULL
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Saturday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 95), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN t2.ClientCaseInitiationStart IS NULL
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Sunday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 96), 'yyyy-MM-dd')
# MAGIC
# MAGIC
# MAGIC       -- added this to prevent that PID > NRD, due to change in client risk -> basically avoid now automatic overdue!!!!
# MAGIC       WHEN to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd') >= to_date(t1.NextReviewDate, 'yyyy-MM-dd')
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Monday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd') >= to_date(t1.NextReviewDate, 'yyyy-MM-dd')
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Tuesday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 91), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd') >= to_date(t1.NextReviewDate, 'yyyy-MM-dd')
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Wednesday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 92), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd') >= to_date(t1.NextReviewDate, 'yyyy-MM-dd')
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Thursday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 93), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd') >= to_date(t1.NextReviewDate, 'yyyy-MM-dd')
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Friday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 94), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd') >= to_date(t1.NextReviewDate, 'yyyy-MM-dd')
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Saturday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 95), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd') >= to_date(t1.NextReviewDate, 'yyyy-MM-dd')
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Sunday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 96), 'yyyy-MM-dd')
# MAGIC
# MAGIC   -- COMMENTING IT OUT SO MASS UPDATE IN PORTFOLIO PLANNING CAN WORK. IF IN CASE OF ISSUES, PLEASE UNCOMMENT THE BLOCK
# MAGIC --/*
# MAGIC       WHEN DATE_DIFF(to_date(t1.NextReviewDate, 'yyyy-MM-dd'), to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd')) > 300
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Monday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN DATE_DIFF(to_date(t1.NextReviewDate, 'yyyy-MM-dd'), to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd')) > 300
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Tuesday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 91), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN DATE_DIFF(to_date(t1.NextReviewDate, 'yyyy-MM-dd'), to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd')) > 300
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Wednesday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 92), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN DATE_DIFF(to_date(t1.NextReviewDate, 'yyyy-MM-dd'), to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd')) > 300
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Thursday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 93), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN DATE_DIFF(to_date(t1.NextReviewDate, 'yyyy-MM-dd'), to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd')) > 300
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Friday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 94), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN DATE_DIFF(to_date(t1.NextReviewDate, 'yyyy-MM-dd'), to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd')) > 300
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Saturday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 95), 'yyyy-MM-dd')
# MAGIC
# MAGIC       WHEN DATE_DIFF(to_date(t1.NextReviewDate, 'yyyy-MM-dd'), to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd')) > 300
# MAGIC         AND DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 90), 'EEEE') = 'Sunday'
# MAGIC       THEN DATE_FORMAT(DATE_SUB(t1.NextReviewDate, 96), 'yyyy-MM-dd')
# MAGIC
# MAGIC
# MAGIC   --*/
# MAGIC       WHEN t2.ClientCaseInitiationStart IS NOT NULL THEN to_date(t2.ClientCaseInitiationStart, 'yyyy-MM-dd')  
# MAGIC     END AS ClientCaseInitiationStart
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN LOWER(TRIM(coalesce(t3.ReviewLocation, t2.ReviewLocation))) = 'kyc uk' THEN 'KYC UK'
# MAGIC       WHEN LOWER(TRIM(coalesce(t3.ReviewLocation, t2.ReviewLocation))) = '' OR LOWER(TRIM(coalesce(t3.ReviewLocation, t2.ReviewLocation))) = ' ' THEN NULL
# MAGIC       WHEN coalesce(t3.ReviewLocation, t2.ReviewLocation) IS NOT NULL THEN coalesce(t3.ReviewLocation, t2.ReviewLocation)
# MAGIC     END AS ReviewLocation
# MAGIC
# MAGIC   , t2.LondonSectorTeam
# MAGIC   , t2.ReasonExplanation
# MAGIC   , t2.Reason
# MAGIC   --/*
# MAGIC   -- COMMENTING IT OUT SO MASS UPDATE IN PORTFOLIO PLANNING CAN WORK. IF IN CASE OF ISSUES, PLEASE UNCOMMENT THE BLOCK
# MAGIC   , CASE 
# MAGIC       WHEN t2.CDDExecution IS NOT NULL AND DATE_DIFF(to_date(t1.NextReviewDate, 'yyyy-MM-dd'), to_date(t2.CDDExecution, 'yyyy-MM-dd')) < 300 AND (to_date(t2.CDDExecution, 'yyyy-MM-dd') < to_date(t1.NextReviewDate, 'yyyy-MM-dd'))
# MAGIC         THEN t2.CDDExecution -- if more than 300 days difference between NRD and PAD is wrong
# MAGIC       ELSE DATE_ADD(to_date(t1.NextReviewDate, 'yyyy-MM-dd'), -69)
# MAGIC     END AS CDDExecution
# MAGIC     --*/
# MAGIC     -- IF UNCOMMENTING THE ABOVE CDDExecution BLOCK PLEASE REMOVE THE BELOW CDDExecution BLOCK
# MAGIC     /*, CASE 
# MAGIC       WHEN (to_date(t2.CDDExecution, 'yyyy-MM-dd') < to_date(t1.NextReviewDate, 'yyyy-MM-dd'))
# MAGIC         THEN t2.CDDExecution -- if more than 300 days difference between NRD and PAD is wrong
# MAGIC       ELSE DATE_ADD(to_date(t1.NextReviewDate, 'yyyy-MM-dd'), -69)
# MAGIC     END AS CDDExecution
# MAGIC     */
# MAGIC   , t1.FIHubIndicator
# MAGIC
# MAGIC FROM client_information_cte t1
# MAGIC LEFT JOIN latest_active_masterlistregistry_cte t2 ON t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC LEFT JOIN onboarding_days_diff t3 ON t1.UniqueGcobId = t3.UniqueGcobId AND t3.rn = 1

# COMMAND ----------

# MAGIC %md
# MAGIC # FirebirdMaster

# COMMAND ----------

# DBTITLE 1,main_case_status_type
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW main_case_status_type AS
# MAGIC SELECT
# MAGIC   t1.CaseID
# MAGIC   , t1.ClientId
# MAGIC   , t1.SourceClient
# MAGIC   , CASE 
# MAGIC       WHEN t2.Name IN ('GCOB Review in progress', 'Initiation In Progress') THEN 'Initiation'
# MAGIC       WHEN t2.Name IN ('Ready for KYC assessment') THEN 'Ready for KYC assessment'
# MAGIC       WHEN t2.Name IN ('KYC assessment in progress') THEN 'Assessment'
# MAGIC       WHEN t2.Name IN ('Product Offboarding confirmation in progress', 'Ready for Product Offboarding confirmation', 'Product Offboarding in progress') THEN 'Product Offboarding'
# MAGIC       WHEN t2.Name IN ('Ready for 4 eye check') THEN 'Ready for QC'
# MAGIC       WHEN t2.Name IN ('4 eye check in progress') THEN 'QC'
# MAGIC       WHEN t2.Name IN ('Client committee sign off requested', 'Client Owner approval requested', 'Client owner sign off requested', 'Local client owner sign off requested', 'Senior management sign off requested') THEN 'Sign-off'
# MAGIC       WHEN t2.Name IN ('Product fulfilment in progress') THEN 'Product Fulfillment'
# MAGIC       WHEN t2.Name IN ('Completed', 'Cancelled') THEN t2.Name
# MAGIC       ELSE t2.Name
# MAGIC     END AS MainCaseStatusType
# MAGIC , CASE 
# MAGIC       WHEN t2.Name IN ('GCOB Review in progress', 'Initiation In Progress') THEN 1
# MAGIC       WHEN t2.Name IN ('Ready for KYC assessment') THEN 3
# MAGIC       WHEN t2.Name IN ('KYC assessment in progress') THEN 4
# MAGIC       WHEN t2.Name IN ('Product Offboarding confirmation in progress', 'Ready for Product Offboarding confirmation', 'Product Offboarding in progress') THEN 15
# MAGIC       WHEN t2.Name IN ('Ready for 4 eye check') THEN 9
# MAGIC       WHEN t2.Name IN ('4 eye check in progress') THEN 10
# MAGIC       WHEN t2.Name IN ('Client committee sign off requested', 'Client Owner approval requested', 'Client owner sign off requested', 'Local client owner sign off requested', 'Senior management sign off requested') THEN 12
# MAGIC       WHEN t2.Name IN ('Product fulfilment in progress') THEN 13
# MAGIC       WHEN t2.Name IN ('Completed', 'Cancelled') THEN 14
# MAGIC       ELSE 13
# MAGIC     END AS MainCaseStatusSortOrder
# MAGIC
# MAGIC FROM party_workitem t1
# MAGIC LEFT JOIN gcob_static_CaseStatusType t2 ON t1.CaseCurrentStatus = t2.StatusId

# COMMAND ----------

# DBTITLE 1,workitems_status
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW workitems_status AS
# MAGIC
# MAGIC /* Stored Procedure for dml.WorkitemsStatus */
# MAGIC
# MAGIC /* Logic:
# MAGIC     - Determine the first timestamp for each status (1 to 7) using MIN(DateCreated) and group by CaseID.
# MAGIC     - Determine the last timestamp for statuses 7 and 8 using MAX(DateCreated) and group by CaseID.
# MAGIC     - Calculate the difference in days between various statuses, excluding weekends.
# MAGIC     - Determine the main status based on the first non-null value from Cancelled/Completed to Prework.
# MAGIC
# MAGIC [Prework]: MIN(DateCreated) for status 1 or 2
# MAGIC [Ready for assessment]: MIN(DateCreated) for status 3
# MAGIC [Assessment in progress]: MIN(DateCreated) for status 4
# MAGIC [4-EYE Check]: MIN(DateCreated) for status 5
# MAGIC [Sign-off]: MIN(DateCreated) for status 6
# MAGIC [Fulfillment]: MIN(DateCreated) for status 7
# MAGIC [Completed]: MAX(DateCreated) for status 7
# MAGIC [Cancelled]: MAX(DateCreated) for status 8
# MAGIC
# MAGIC [DaysInPrework]: DATEDIFF(DAY, [Prework], [Ready for assessment])
# MAGIC [DaysInReadyForAssessment]: DATEDIFF(DAY, [Ready for assessment], [Assessment in progress])
# MAGIC [DaysInAssessmentInProgress]: DATEDIFF(DAY, [Assessment in progress], [4-EYE Check])
# MAGIC [DaysIn4EYECheck]: DATEDIFF(DAY, [4-EYE Check], [Sign-off])
# MAGIC [DaysInSign-off]: DATEDIFF(DAY, [Sign-off], [Fulfillment])
# MAGIC [DaysInFulfillment]: DATEDIFF(DAY, [Fulfillment], [Completed])
# MAGIC [TotalCaseDuration]: DATEDIFF(DAY, [Prework], [Completed])
# MAGIC
# MAGIC [Main status]: CASE WHEN statement to determine the first non-null status from Cancelled/Completed to Prework
# MAGIC
# MAGIC */
# MAGIC
# MAGIC   WITH Prework AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS Prework
# MAGIC     FROM party_workitem
# MAGIC     WHERE CaseStatusTypeWhenCreated IN (1, 16)
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC
# MAGIC , ReadyForAssessment AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS ReadyForAssessment
# MAGIC     FROM party_workitem
# MAGIC     WHERE CaseStatusTypeWhenCreated = 2
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC
# MAGIC , AssessmentInProgress AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS AssessmentInProgress
# MAGIC     FROM party_workitem
# MAGIC     WHERE CaseStatusTypeWhenCreated = 3
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC
# MAGIC , ReadyFor4EYECheck AS (
# MAGIC     SELECT 
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS ReadyFor4EYECheck
# MAGIC     FROM party_workitem
# MAGIC     WHERE CaseStatusTypeWhenCreated = 4
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC
# MAGIC , 4EYECheck AS (
# MAGIC     SELECT 
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS 4EYECheck
# MAGIC     FROM party_workitem
# MAGIC     WHERE CaseStatusTypeWhenCreated = 5
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC
# MAGIC , Signoff AS (
# MAGIC     SELECT 
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS SignOff
# MAGIC     FROM party_workitem
# MAGIC     WHERE CaseStatusTypeWhenCreated IN (6, 18)
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC
# MAGIC , Fulfillment AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS Fulfillment
# MAGIC     FROM party_workitem
# MAGIC     WHERE CaseStatusTypeWhenCreated = 8
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC , Completed AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , MAX(WorkItemCompletedDate) AS Completed
# MAGIC     FROM party_workitem  
# MAGIC     WHERE CaseCurrentStatus = 9
# MAGIC       AND WorkItemCompletedDate IS NOT NULL
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC , Cancelled AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , MAX(WorkItemCompletedDate) AS Cancelled
# MAGIC     FROM party_workitem
# MAGIC     WHERE CaseStatusTypeWhenCreated = 10
# MAGIC       AND WorkItemCompletedDate IS NOT NULL
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC , QCInteractions AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , COUNT(*) AS QCInteractions
# MAGIC     FROM party_workitem
# MAGIC     WHERE CaseStatusName = 'Ready for 4 eye check'
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC , PreworkAnalystdate AS (
# MAGIC     SELECT DISTINCT
# MAGIC       t1.SourceClient
# MAGIC       , CASE WHEN t1.WorkItemAssignedUserName = 'Unknown' THEN t3.WorkItemAssignedUserName ELSE t1.WorkItemAssignedUserName END AS PreworkAnalyst
# MAGIC       , CASE WHEN t1.WorkItemAssignedUserName = 'Unknown' THEN SUBSTRING(t1.CreatingUserID, INSTR(t1.CreatingUserID, '\\') + 1) ELSE SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\') + 1) END AS PreworkAlias
# MAGIC       , MaxDate AS PreworkAnalystTeamDate
# MAGIC     FROM party_workitem t1
# MAGIC     INNER JOIN (
# MAGIC       SELECT
# MAGIC         t1.SourceClient
# MAGIC         , MAX(WorkItemCreatedDate) AS MaxDate
# MAGIC       FROM party_workitem t1
# MAGIC       LEFT JOIN (
# MAGIC         SELECT
# MAGIC           SourceClient
# MAGIC           , MIN(WorkItemCreatedDate) AS ReadyForAssessment
# MAGIC         FROM party_workitem
# MAGIC         WHERE CaseStatusTypeWhenCreated = 2
# MAGIC         GROUP BY SourceClient
# MAGIC       ) t4 ON t1.SourceClient = t4.SourceClient
# MAGIC       WHERE t1.CaseStatusTypeWhenCreated IN (1, 16) AND WorkItemCreatedDate <= COALESCE(t4.ReadyForAssessment, CURRENT_TIMESTAMP())
# MAGIC       GROUP BY t1.SourceClient
# MAGIC     ) t2 ON t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
# MAGIC     LEFT JOIN (
# MAGIC       SELECT
# MAGIC         SourceClient
# MAGIC         , WorkItemAssignedUserName
# MAGIC         , AssignedUserId
# MAGIC         , CaseStatusTypeWhenCreated
# MAGIC         , WorkItemCreatedDate
# MAGIC       FROM party_workitem
# MAGIC       WHERE CaseStatusTypeWhenCreated IN (1)
# MAGIC     ) t3 ON t2.SourceClient = t3.SourceClient AND t2.MaxDate = t3.WorkItemCreatedDate
# MAGIC     WHERE t1.CaseStatusTypeWhenCreated IN (1, 16)
# MAGIC   )
# MAGIC , AssessmentAnalystDate AS (
# MAGIC     -- select LastAnalyst on StatusAssessment
# MAGIC     SELECT DISTINCT 
# MAGIC       t1.SourceClient
# MAGIC       , CASE WHEN t1.WorkItemAssignedUserName = 'Unknown' THEN t3.WorkItemAssignedUserName ELSE t1.WorkItemAssignedUserName END AS AssessmentAnalyst
# MAGIC       , SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\') + 1) AS AssessmentAlias
# MAGIC       , t2.MaxDate AS AssessmentAnalystTeamDate
# MAGIC     FROM party_workitem t1  
# MAGIC     INNER JOIN ( 
# MAGIC         SELECT 
# MAGIC           SourceClient
# MAGIC           , MAX(WorkItemCreatedDate) AS MaxDate 
# MAGIC         FROM party_workitem 
# MAGIC         WHERE CaseStatusTypeWhenCreated IN (3, 20, 21)
# MAGIC         GROUP BY SourceClient
# MAGIC       ) t2 ON t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
# MAGIC     LEFT JOIN (
# MAGIC       SELECT
# MAGIC         SourceClient
# MAGIC         , WorkItemAssignedUserName
# MAGIC         , AssignedUserId
# MAGIC         , CaseStatusTypeWhenCreated
# MAGIC         , WorkItemCreatedDate 
# MAGIC       FROM party_workitem 
# MAGIC       WHERE CaseStatusTypeWhenCreated IN (2, 19, 20)
# MAGIC     ) t3 ON t2.SourceClient = t3.SourceClient AND t2.MaxDate = t3.WorkItemCreatedDate
# MAGIC     WHERE t1.CaseStatusTypeWhenCreated IN (3, 20, 21)
# MAGIC   )
# MAGIC , 4EYEAnalystDate AS (
# MAGIC     SELECT DISTINCT 
# MAGIC       t1.SourceClient
# MAGIC       , t1.WorkItemAssignedUserName AS 4EYEAnalyst
# MAGIC       , SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\') + 1) AS 4EYEAlias
# MAGIC       , t2.MaxDate AS 4EYEAnalystTeamDate
# MAGIC     FROM party_workitem t1 
# MAGIC     INNER JOIN ( 
# MAGIC       SELECT 
# MAGIC         t3.SourceClient
# MAGIC         , MAX(t3.WorkItemCreatedDate) AS MaxDate 
# MAGIC       FROM party_workitem t3
# MAGIC         LEFT JOIN (
# MAGIC           SELECT
# MAGIC             SourceClient
# MAGIC             , MIN(WorkItemCreatedDate) AS SignOff
# MAGIC           FROM party_workitem 
# MAGIC           WHERE CaseStatusTypeWhenCreated = 6
# MAGIC           GROUP BY SourceClient
# MAGIC         ) t4 ON t3.SourceClient = t4.SourceClient 
# MAGIC       WHERE t3.CaseStatusTypeWhenCreated = 5 AND t3.WorkItemCreatedDate <= COALESCE(t4.SignOff, CURRENT_TIMESTAMP())
# MAGIC       GROUP BY t3.SourceClient
# MAGIC       ) t2 ON t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
# MAGIC     WHERE t1.CaseStatusTypeWhenCreated = 5
# MAGIC   )
# MAGIC , LastAnalyst AS (
# MAGIC     SELECT DISTINCT
# MAGIC       t1.SourceClient
# MAGIC       , t1.WorkItemAssignedUserName AS LastAnalyst
# MAGIC       , SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\') + 1) AS LastAnalystAlias
# MAGIC       , MaxDate AS LastAnalystTeamDate
# MAGIC     FROM party_workitem t1
# MAGIC     INNER JOIN (
# MAGIC       SELECT
# MAGIC         SourceClient
# MAGIC         , MAX(WorkItemCreatedDate) AS MaxDate
# MAGIC         , MAX(CaseStatusTypeWhenCreated) AS MaxCaseStatusTypeWhenCreated
# MAGIC       FROM party_workitem
# MAGIC       WHERE ResponsibleRole IN (2, 3)
# MAGIC         AND CaseStatusTypeWhenCreated NOT IN (5, 4, 2) -- ('4 eye check in progress', 'Ready for 4 eye check', 'Ready for KYC ASsessment')
# MAGIC       GROUP BY SourceClient
# MAGIC       ) t2 ON t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate AND t1.CaseStatusTypeWhenCreated = t2.MaxCaseStatusTypeWhenCreated
# MAGIC     WHERE ResponsibleRole IN (2, 3) AND CaseStatusTypeWhenCreated NOT IN (5, 4, 2)
# MAGIC   )
# MAGIC
# MAGIC , WithLag AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     LAG(CaseStatusTypeWhenCreated) OVER (
# MAGIC       PARTITION BY SourceClient
# MAGIC       ORDER BY WorkItemCreatedDate
# MAGIC     ) AS PreviousStatus,
# MAGIC     LAG(WorkItemCreatedDate) OVER (
# MAGIC       PARTITION BY SourceClient
# MAGIC       ORDER BY WorkItemCreatedDate
# MAGIC     ) AS PreviousDate
# MAGIC   FROM party_workitem
# MAGIC ),
# MAGIC  
# MAGIC -- Prework (Status = 1)
# MAGIC PreworkFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated = 1
# MAGIC ),
# MAGIC PreworkFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus = 1 THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS LatestPreworkDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM PreworkFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC  
# MAGIC -- Ready for Assessment (Status = 2)
# MAGIC ReadyForAssessmentFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated = 2
# MAGIC ),
# MAGIC ReadyForAssessmentFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus = 2 THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS LatestReadyForAssessmentDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM ReadyForAssessmentFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- KYC Assessment In Progress (Status = 3)
# MAGIC   KycAssessmentInProgressFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated = 3
# MAGIC ),
# MAGIC KycAssessmentInProgressFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus = 3 THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS LatestAssessmentInProgressDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM KycAssessmentInProgressFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- Ready for 4 Eye Check (Status = 4)
# MAGIC   ReadyFor4EYECheckFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated = 4
# MAGIC ),
# MAGIC ReadyFor4EYECheckFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus = 4 THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS LatestReadyFor4EYECheckDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM ReadyFor4EYECheckFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- Ready for 4 Eye Check (Status = 5)
# MAGIC 4EYECheckFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated = 5
# MAGIC ),
# MAGIC 4EYECheckFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus = 5 THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS Latest4EYECheckdDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM 4EYECheckFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- Ready for Signoff (Status IN (6, 18))
# MAGIC SignoffFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated IN (6, 18)
# MAGIC ),
# MAGIC SignoffFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus IN (6, 18) THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS LatestSignoffDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM SignoffFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- Ready for Fulfillment (Status = 8)
# MAGIC FulfillmentFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated = 8
# MAGIC ),
# MAGIC FulfillmentFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus = 8 THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS LatestFulfillmentDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM FulfillmentFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.ClientId
# MAGIC   , t1.SourceClient
# MAGIC   , t1.CaseId
# MAGIC   , t1.GcobId
# MAGIC   , t2.Prework
# MAGIC   , t10.PreworkAnalyst
# MAGIC   , t10.PreworkAnalystTeamDate
# MAGIC   , t3.ReadyForAssessment
# MAGIC   , t4.AssessmentInProgress
# MAGIC   , t11.AssessmentAnalyst
# MAGIC   , t11.AssessmentAnalystTeamDate
# MAGIC   , t5.4EYECheck
# MAGIC   , t12.4EYEAnalyst
# MAGIC   , t12.4EYEAnalystTeamDate
# MAGIC   , t6.SignOff
# MAGIC   , t7.Fulfillment
# MAGIC   , t8.Completed
# MAGIC   , t9.Cancelled
# MAGIC
# MAGIC
# MAGIC   , t15.LatestPreworkDate
# MAGIC   , t16.LatestReadyForAssessmentDate
# MAGIC   , t17.LatestAssessmentInProgressDate
# MAGIC   , t18.Latest4EYECheckdDate
# MAGIC   , t19.LatestSignoffDate
# MAGIC   , t20.LatestFulfillmentDate
# MAGIC   , t21.LatestReadyFor4EYECheckDate
# MAGIC   , t22.ReadyFor4EYECheck
# MAGIC
# MAGIC /*
# MAGIC   , DATEDIFF(DAY, t2.Prework, COALESCE(t3.ReadyForAssessment, CURRENT_TIMESTAMP())) + 1 
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t2.Prework, COALESCE(t3.ReadyForAssessment, CURRENT_TIMESTAMP())) * 2)
# MAGIC         + (CASE WHEN DATE_FORMAT(t2.Prework, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC         + (CASE WHEN DATE_FORMAT(t3.ReadyForAssessment, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS DaysInPrework
# MAGIC
# MAGIC   , DATEDIFF(DAY, t3.ReadyForAssessment, COALESCE(t4.AssessmentInProgress, CURRENT_TIMESTAMP())) + 1 
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t3.ReadyForAssessment, COALESCE(t4.AssessmentInProgress, CURRENT_TIMESTAMP())) * 2) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t3.ReadyForAssessment, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t4.AssessmentInProgress, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS DaysInReadyForAssessment
# MAGIC
# MAGIC   , DATEDIFF(DAY, t4.AssessmentInProgress, COALESCE(t5.4EYECheck, CURRENT_TIMESTAMP())) + 1 
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t4.AssessmentInProgress, COALESCE(t5.4EYECheck, CURRENT_TIMESTAMP())) * 2) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t4.AssessmentInProgress, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t5.4EYECheck, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS DaysInAssessmentInProgress
# MAGIC   
# MAGIC   , DATEDIFF(DAY, t5.4EYECheck, COALESCE(t6.SignOff, CURRENT_TIMESTAMP())) + 1
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t5.4EYECheck, COALESCE(t6.SignOff, CURRENT_TIMESTAMP())) * 2) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t5.4EYECheck, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t6.SignOff, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS DaysIn4EYECheck
# MAGIC   
# MAGIC   , DATEDIFF(DAY, t6.SignOff, COALESCE(t7.Fulfillment, CURRENT_TIMESTAMP())) + 1 
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t6.SignOff, COALESCE(t7.Fulfillment, CURRENT_TIMESTAMP())) * 2) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t6.SignOff, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t7.Fulfillment, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS DaysInSignOff
# MAGIC   
# MAGIC   , DATEDIFF(DAY, t7.Fulfillment, COALESCE(t8.Completed, CURRENT_TIMESTAMP())) + 1 
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t7.Fulfillment, COALESCE(t8.Completed, CURRENT_TIMESTAMP())) * 2) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t7.Fulfillment, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t8.Completed, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS DaysInFulfillment
# MAGIC     */
# MAGIC --------------------------------------new logic for days in a current case phase start-----------------------------------------------------------
# MAGIC
# MAGIC   , DATEDIFF(DAY, t15.LatestPreworkDate, COALESCE(t16.LatestReadyForAssessmentDate, CURRENT_TIMESTAMP())) + 1 
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t15.LatestPreworkDate, COALESCE(t16.LatestReadyForAssessmentDate, CURRENT_TIMESTAMP())) * 2)
# MAGIC         + (CASE WHEN DATE_FORMAT(t15.LatestPreworkDate, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC         + (CASE WHEN DATE_FORMAT(t16.LatestReadyForAssessmentDate, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS DaysInPrework
# MAGIC
# MAGIC   , DATEDIFF(DAY, t16.LatestReadyForAssessmentDate, COALESCE(t17.LatestAssessmentInProgressDate, CURRENT_TIMESTAMP())) + 1 
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t16.LatestReadyForAssessmentDate, COALESCE(t17.LatestAssessmentInProgressDate, CURRENT_TIMESTAMP())) * 2) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t16.LatestReadyForAssessmentDate, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t17.LatestAssessmentInProgressDate, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS DaysInReadyForAssessment
# MAGIC
# MAGIC   , DATEDIFF(DAY, t17.LatestAssessmentInProgressDate, COALESCE(t21.LatestReadyFor4EYECheckDate, CURRENT_TIMESTAMP())) + 1 
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t17.LatestAssessmentInProgressDate, COALESCE(t21.LatestReadyFor4EYECheckDate, CURRENT_TIMESTAMP())) * 2) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t17.LatestAssessmentInProgressDate, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t21.LatestReadyFor4EYECheckDate, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS DaysInAssessmentInProgress
# MAGIC
# MAGIC , DATEDIFF(
# MAGIC       DAY,
# MAGIC       t21.LatestReadyFor4EYECheckDate,
# MAGIC       COALESCE(
# MAGIC         CASE
# MAGIC           WHEN t18.Latest4EYECheckdDate > t21.LatestReadyFor4EYECheckDate
# MAGIC             THEN t18.Latest4EYECheckdDate
# MAGIC         END,
# MAGIC         CURRENT_TIMESTAMP()
# MAGIC       )
# MAGIC   ) + 1
# MAGIC   - (
# MAGIC       (DATEDIFF(
# MAGIC           WEEK,
# MAGIC           t21.LatestReadyFor4EYECheckDate,
# MAGIC           COALESCE(
# MAGIC             CASE
# MAGIC               WHEN t18.Latest4EYECheckdDate > t21.LatestReadyFor4EYECheckDate
# MAGIC                 THEN t18.Latest4EYECheckdDate
# MAGIC             END,
# MAGIC             CURRENT_TIMESTAMP()
# MAGIC           )
# MAGIC         ) * 2)
# MAGIC       + (CASE
# MAGIC            WHEN DATE_FORMAT(t21.LatestReadyFor4EYECheckDate, 'EEEE') = 'Sunday'
# MAGIC              THEN 1 ELSE 0
# MAGIC          END)
# MAGIC       + (CASE
# MAGIC            WHEN DATE_FORMAT(
# MAGIC                   COALESCE(
# MAGIC                     CASE
# MAGIC                       WHEN t18.Latest4EYECheckdDate > t21.LatestReadyFor4EYECheckDate
# MAGIC                         THEN t18.Latest4EYECheckdDate
# MAGIC                     END,
# MAGIC                     CURRENT_TIMESTAMP()
# MAGIC                   ),
# MAGIC                   'EEEE'
# MAGIC                ) = 'Saturday'
# MAGIC              THEN 1 ELSE 0
# MAGIC          END)
# MAGIC     ) AS DaysInReadyFor4EYECheck
# MAGIC
# MAGIC   , DATEDIFF(DAY, t18.Latest4EYECheckdDate, COALESCE(t19.LatestSignoffDate, CURRENT_TIMESTAMP())) + 1
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t18.Latest4EYECheckdDate, COALESCE(t19.LatestSignoffDate, CURRENT_TIMESTAMP())) * 2) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t18.Latest4EYECheckdDate, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t19.LatestSignoffDate, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS DaysIn4EYECheck
# MAGIC   
# MAGIC   , DATEDIFF(DAY, t19.LatestSignoffDate, COALESCE(t20.LatestFulfillmentDate, CURRENT_TIMESTAMP())) + 1 
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t19.LatestSignoffDate, COALESCE(t20.LatestFulfillmentDate, CURRENT_TIMESTAMP())) * 2) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t19.LatestSignoffDate, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t20.LatestFulfillmentDate, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS DaysInSignOff
# MAGIC   
# MAGIC   , DATEDIFF(DAY, t20.LatestFulfillmentDate, COALESCE(t8.Completed, CURRENT_TIMESTAMP())) + 1 
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t20.LatestFulfillmentDate, COALESCE(t8.Completed, CURRENT_TIMESTAMP())) * 2) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t20.LatestFulfillmentDate, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t8.Completed, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS DaysInFulfillment
# MAGIC     
# MAGIC --------------------------------------new logic for days in a current case phase end-----------------------------------------------------------
# MAGIC
# MAGIC   , DATEDIFF(DAY, t2.Prework, COALESCE(t8.Completed, CURRENT_TIMESTAMP())) + 1 
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t2.Prework, COALESCE(t8.Completed, CURRENT_TIMESTAMP())) * 2) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t2.Prework, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC         + (CASE WHEN DATE_FORMAT(t8.Completed, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ) AS TotalCaseDuration    
# MAGIC   , CASE
# MAGIC       WHEN t9.Cancelled IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 8)
# MAGIC       WHEN t8.Completed IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 7)
# MAGIC       WHEN t7.Fulfillment IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 6)
# MAGIC       WHEN t6.SignOff IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 5)
# MAGIC       WHEN t5.4EYECheck IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 4)
# MAGIC       WHEN t4.AssessmentInProgress IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 3)
# MAGIC       WHEN t3.ReadyForAssessment IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 2)
# MAGIC       WHEN t2.Prework IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 1)
# MAGIC       ELSE 'UNKNOWN'
# MAGIC     END AS CasePhase
# MAGIC , CASE
# MAGIC     WHEN t9.Cancelled IS NOT NULL THEN 8
# MAGIC     WHEN t8.Completed IS NOT NULL THEN 7
# MAGIC     WHEN t7.Fulfillment IS NOT NULL THEN 6
# MAGIC     WHEN t6.SignOff IS NOT NULL THEN 5
# MAGIC     WHEN t5.4EYECheck IS NOT NULL THEN 4
# MAGIC     WHEN t4.AssessmentInProgress IS NOT NULL THEN 3
# MAGIC     WHEN t3.ReadyForAssessment IS NOT NULL THEN 2
# MAGIC     WHEN t2.Prework IS NOT NULL THEN 1
# MAGIC     ELSE 99999
# MAGIC   END AS CasePhaseSortOrder
# MAGIC , t13.QCInteractions
# MAGIC , t14.LastAnalyst
# MAGIC , t14.LastAnalystTeamDate
# MAGIC , t10.PreworkAlias
# MAGIC , t11.AssessmentAlias
# MAGIC , t12.4EYEAlias
# MAGIC , t14.LastAnalystAlias
# MAGIC
# MAGIC FROM party_workitem t1
# MAGIC LEFT JOIN Prework t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN Readyforassessment t3 ON t1.SourceClient = t3.SourceClient
# MAGIC LEFT JOIN AssessmentInProgress t4 ON t1.SourceClient = t4.SourceClient
# MAGIC LEFT JOIN 4EYECheck t5 ON t1.SourceClient = t5.SourceClient
# MAGIC LEFT JOIN Signoff t6 ON t1.SourceClient = t6.SourceClient
# MAGIC LEFT JOIN Fulfillment t7 ON t1.SourceClient = t7.SourceClient
# MAGIC LEFT JOIN Completed t8 ON t1.SourceClient = t8.SourceClient
# MAGIC LEFT JOIN Cancelled t9 ON t1.SourceClient = t9.SourceClient
# MAGIC LEFT JOIN PreworkAnalystdate t10 ON t1.SourceClient = t10.SourceClient
# MAGIC LEFT JOIN AssessmentAnalystDate t11 ON t1.SourceClient = t11.SourceClient
# MAGIC LEFT JOIN 4EYEAnalystDate t12 ON t1.SourceClient = t12.SourceClient
# MAGIC LEFT JOIN QCInteractions t13 ON t1.SourceClient = t13.SourceClient
# MAGIC LEFT JOIN LastAnalyst t14 ON t1.SourceClient = t14.SourceClient
# MAGIC
# MAGIC
# MAGIC LEFT JOIN PreworkFinal t15 ON t1.SourceClient = t15.SourceClient
# MAGIC LEFT JOIN ReadyForAssessmentFinal t16 ON t1.SourceClient = t16.SourceClient
# MAGIC LEFT JOIN KycAssessmentInProgressFinal t17 ON t1.SourceClient = t17.SourceClient
# MAGIC LEFT JOIN 4EYECheckFinal t18 ON t1.SourceClient = t18.SourceClient
# MAGIC LEFT JOIN SignoffFinal t19 ON t1.SourceClient = t19.SourceClient
# MAGIC LEFT JOIN FulfillmentFinal t20 ON t1.SourceClient = t20.SourceClient
# MAGIC LEFT JOIN ReadyFor4EYECheckFinal t21 ON t1.SourceClient = t21.SourceClient
# MAGIC LEFT JOIN ReadyFor4EYECheck t22 ON t1.SourceClient = t22.SourceClient
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,rfi_status
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW rfi_status AS -- dml.RFIStatus
# MAGIC SELECT DISTINCT
# MAGIC   t1.GcobId
# MAGIC   , t1.CaseId
# MAGIC   , t1.ClientId
# MAGIC   , t1.SourceClient
# MAGIC
# MAGIC   , COALESCE(t3.NumberOfRFI, 0) AS NumberOfRFI
# MAGIC   , CASE 
# MAGIC       WHEN t1.CasePhase IN ('Cancelled', 'Completed') THEN t1.CasePhase
# MAGIC       WHEN t3.RequestStatusNumber IN (1) AND t3.NumberOfRFI = 1 THEN 'Client Outreach'
# MAGIC       WHEN t3.RequestStatusNumber IN (1) AND t3.NumberOfRFI > 1 THEN 'Rebound Client Outreach'
# MAGIC       WHEN t3.RequestStatusNumber = 2 THEN 'Client Outreach Completed'
# MAGIC     END AS CasePhase
# MAGIC   , CASE
# MAGIC       WHEN t1.CasePhase <> 'Assessment in progress' THEN t1.CasePhase
# MAGIC       WHEN t1.SourceClient IS NULL THEN 'Execute Assessment' -- t2.CaseId
# MAGIC       WHEN t3.NumberOfRFI = 1 AND t3.RequestStatusNumber = 1 THEN 'Waiting for information'
# MAGIC       WHEN t3.NumberOfRFI = 1 AND t3.RequestStatusNumber = 2 THEN 'Reviewing information'
# MAGIC       WHEN t3.NumberOfRFI >= 1 AND t3.RequestStatusNumber = 3 THEN 'Finalise assessment'
# MAGIC       WHEN t3.NumberOfRFI > 1 AND t3.RequestStatusNumber = 1 THEN 'Waiting for additional information'
# MAGIC       WHEN t3.NumberOfRFI > 1 AND t3.RequestStatusNumber = 2 THEN 'Reviewing additional information'
# MAGIC     END AS CasePhaseIncludingRFI
# MAGIC   , CASE
# MAGIC       WHEN t1.CasePhase <> 'Assessment in progress' THEN t1.CasePhaseSortOrder
# MAGIC       WHEN t1.SourceClient IS NULL THEN 3.1 -- t2.CaseId
# MAGIC       WHEN t3.NumberOfRFI = 1 AND t3.RequestStatusNumber = 1 THEN 3.2
# MAGIC       WHEN t3.NumberOfRFI = 1 AND t3.RequestStatusNumber = 2 THEN 3.2
# MAGIC       WHEN t3.NumberOfRFI >= 1 AND t3.RequestStatusNumber = 3 THEN 3.3
# MAGIC       WHEN t3.NumberOfRFI > 1 AND t3.RequestStatusNumber = 1 THEN 3.4
# MAGIC       WHEN t3.NumberOfRFI > 1 AND t3.RequestStatusNumber = 2 THEN 3.5
# MAGIC     END AS CasePhaseIncludingRFISortOrder
# MAGIC   , t1.ReadyForAssessment
# MAGIC
# MAGIC   , DATEDIFF(DAY, t1.ReadyForAssessment, t4.DateCreated) + 1
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t1.ReadyForAssessment, t4.DateCreated) * 2)
# MAGIC         + (CASE WHEN DATE_FORMAT(t1.ReadyForAssessment, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC         + (CASE WHEN DATE_FORMAT(t4.DateCreated, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       ) 
# MAGIC     AS DaysInExecuteAssessment
# MAGIC
# MAGIC   , t4.DateCreated AS DateCreated
# MAGIC
# MAGIC   , DATEDIFF(DAY, t4.DateCreated, COALESCE(t4.DateResponded, CURRENT_DATE())) + 1
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t4.DateCreated, COALESCE(t4.DateResponded, CURRENT_DATE())) * 2)
# MAGIC         + (CASE WHEN DATE_FORMAT(t4.DateCreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC         + (CASE WHEN DATE_FORMAT(t4.DateResponded, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       ) 
# MAGIC     AS DaysInWaitingForInformation
# MAGIC
# MAGIC   , t4.DateResponded AS DateResponded
# MAGIC
# MAGIC   , DATEDIFF(DAY, t4.DateResponded, COALESCE(t4.DateCompleted, CURRENT_DATE())) + 1
# MAGIC     - (
# MAGIC         (DATEDIFF(WEEK, t4.DateResponded, COALESCE(t4.DateCompleted, CURRENT_DATE())) * 2)
# MAGIC         + (CASE WHEN DATE_FORMAT(t4.DateResponded, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC         + (CASE WHEN DATE_FORMAT(t4.DateCompleted, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       ) 
# MAGIC     AS DaysInReviewingInformation
# MAGIC
# MAGIC   , t4.DateCompleted AS DateCompleted
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN t4.ClientId IS NOT NULL THEN 
# MAGIC         DATEDIFF(DAY, t4.DateCompleted, t1.4EYECheck) + 1 
# MAGIC         - (
# MAGIC             (DATEDIFF(WEEK, t4.DateCompleted, t1.4EYECheck) * 2) 
# MAGIC             + (CASE WHEN DATE_FORMAT(t4.DateCompleted, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC             + (CASE WHEN DATE_FORMAT(t1.4EYECheck, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC         )
# MAGIC       WHEN t4.ClientId IS NULL THEN 
# MAGIC         DATEDIFF(DAY, t1.AssessmentInProgress, t1.4EYECheck) + 1
# MAGIC         - (
# MAGIC             (DATEDIFF(WEEK, t1.AssessmentInProgress, t1.4EYECheck) * 2)
# MAGIC             + (CASE WHEN DATE_FORMAT(t1.AssessmentInProgress, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC             + (CASE WHEN DATE_FORMAT(t1.4EYECheck, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC         )
# MAGIC       ELSE NULL
# MAGIC     END + 1 AS DaysInFinaliseAssessment
# MAGIC
# MAGIC   , CASE WHEN t4.DateCompleted IS NOT NULL THEN 'Yes' ELSE 'No' END AS RequestCompleted
# MAGIC   , t1.4EYECheck
# MAGIC
# MAGIC
# MAGIC   -- , DATEDIFF(DAY, t2.DateCreated, COALESCE(t2.DateCompleted, CURRENT_DATE())) + 1 
# MAGIC   --   - (
# MAGIC   --       (DATEDIFF(WEEK, t2.DateCreated, COALESCE(t2.DateCompleted, CURRENT_DATE())) * 2) 
# MAGIC   --       + (CASE WHEN DATE_FORMAT(t2.DateCreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC   --       + (CASE WHEN DATE_FORMAT(t2.DateCompleted, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC   --   ) AS DurationInfoRequestInWorkingDay
# MAGIC   -- , t2.RequestTypeId
# MAGIC
# MAGIC
# MAGIC FROM workitems_status t1
# MAGIC -- LEFT JOIN party_request_for_information t2 ON t1.GcobId = t2.GcobId AND t1.ClientId = t2.ClientId AND t1.CaseId = t2.CaseId
# MAGIC /*
# MAGIC LEFT JOIN (
# MAGIC   SELECT DISTINCT
# MAGIC     ClientId
# MAGIC     , Gcobid
# MAGIC     -- changing it due to error in Assessment 1. When RFI is closed, casephase should be Assessment 2.
# MAGIC     -- with MIN(DateCreated). When RFI is closed casePhase is Assessment 1 (which is not correct)
# MAGIC     --, MIN(DateCreated) AS MinDateCreated
# MAGIC     , MAX(DateCreated) AS MinDateCreated
# MAGIC     , COUNT(RequestTypeId) AS NumberOfRFI 
# MAGIC     , MIN(CASE WHEN StatusType = 'PendingResponse' THEN 1 WHEN StatusType = 'PendingReview' THEN 2 WHEN StatusType = 'Completed' THEN 3 END) As RequestStatusNumber
# MAGIC   FROM party_request_for_information 
# MAGIC   GROUP BY 1,2
# MAGIC ) t3 ON t1.ClientId = t3.ClientId AND t1.GcobId = t3.GcobId
# MAGIC LEFT JOIN (
# MAGIC   SELECT * 
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC       *
# MAGIC       , ROW_NUMBER() OVER (PARTITION BY ClientId, Gcobid ORDER BY RequestTypeId, DateCreated DESC) as rn
# MAGIC     FROM party_request_for_information
# MAGIC     WHERE DateCreated IS NOT NULL AND StatusType <> 'Cancelled'
# MAGIC   ) WHERE rn = 1
# MAGIC ) t4 ON t1.ClientId = t4.ClientId AND t1.GcobId = t4.GcobId AND t3.MinDateCreated = t4.DateCreated
# MAGIC */
# MAGIC LEFT JOIN (
# MAGIC   SELECT DISTINCT
# MAGIC     ClientId
# MAGIC     , Gcobid
# MAGIC     -- changing it due to error in Assessment 1. When RFI is closed, casephase should be Assessment 2.
# MAGIC     -- with MIN(DateCreated). When RFI is closed casePhase is Assessment 1 (which is not correct)
# MAGIC     --, MIN(DateCreated) AS MinDateCreated
# MAGIC     , LEFT(MAX(DateCreated), 16) AS MinDateCreated
# MAGIC     , COUNT(RequestTypeId) AS NumberOfRFI 
# MAGIC     , MIN(CASE WHEN StatusType = 'PendingResponse' THEN 1 WHEN StatusType = 'PendingReview' THEN 2 WHEN StatusType = 'Completed' THEN 3 END) As RequestStatusNumber
# MAGIC   FROM party_request_for_information 
# MAGIC   GROUP BY 1,2
# MAGIC ) t3 ON t1.ClientId = t3.ClientId AND t1.GcobId = t3.GcobId
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC   SELECT * 
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC       *
# MAGIC       , ROW_NUMBER() OVER (PARTITION BY ClientId, Gcobid ORDER BY RequestTypeId, DateCreated DESC) as rn
# MAGIC     FROM party_request_for_information
# MAGIC     WHERE DateCreated IS NOT NULL AND StatusType <> 'Cancelled'
# MAGIC   ) subquery WHERE rn = 1
# MAGIC ) t4 ON t1.ClientId = t4.ClientId AND t1.GcobId = t4.GcobId AND t3.MinDateCreated = LEFT(t4.DateCreated, 16)

# COMMAND ----------

# DBTITLE 1,workitems_status_final
# MAGIC %sql
# MAGIC /*
# MAGIC ------------------------------REPLACING THIS WITH A TEMP VIEW TO ADD MORE FIELDS WHICH WILL BE USED TO COUNT AND DAYS IN EACH CASE PHASE----------------------
# MAGIC CREATE OR REPLACE TEMPORARY VIEW workitems_status_final AS -- same columns found in rpt.cases
# MAGIC
# MAGIC WITH DurationInfoRequestInWorkingDay_CTE AS (
# MAGIC   SELECT
# MAGIC     ClientId
# MAGIC     , CaseId
# MAGIC     , GcobId
# MAGIC     , StatusType
# MAGIC     , DATEDIFF(DAY, DateCreated, COALESCE(DateCompleted, CURRENT_DATE())) + 1 
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, DateCreated, COALESCE(DateCompleted, CURRENT_DATE())) * 2) 
# MAGIC           + (CASE WHEN DATE_FORMAT(DateCreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC           + (CASE WHEN DATE_FORMAT(DateCompleted, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       ) AS DurationInfoRequestInWorkingDay
# MAGIC   FROM party_request_for_information
# MAGIC )
# MAGIC , CTE_ReboundClientOutreach AS (
# MAGIC     SELECT
# MAGIC       t1.SourceClient
# MAGIC       , MIN(t2.DurationInfoRequestInWorkingDay) AS DurationInfoRequestInWorkingDay
# MAGIC     FROM rfi_status t1
# MAGIC     LEFT JOIN DurationInfoRequestInWorkingDay_CTE t2 ON t1.GcobId = t2.GcobId AND t1.ClientId = t2.ClientId AND t1.CaseId = t2.CaseId
# MAGIC     WHERE 1=1
# MAGIC       AND t1.RequestCompleted = 'No'
# MAGIC       AND t2.StatusType = 'PendingResponse'
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC , CTE_LatestNextReviewDate AS (
# MAGIC   -- get the NextReviewDate per GcobId corresponding to the latest non cancelled caseid
# MAGIC     SELECT 
# MAGIC       t1.GcobId
# MAGIC       , t1.ClientType
# MAGIC       , t1.CaseId
# MAGIC       , t1.NextReviewDate
# MAGIC     FROM party_case_client_details t1
# MAGIC     WHERE t1.CaseStatusName <> 'Cancelled'
# MAGIC     AND t1.CaseId = (
# MAGIC         SELECT
# MAGIC           MAX(t2.CaseId)
# MAGIC         FROM party_case_client_details t2
# MAGIC         WHERE t2.GcobId = t1.GcobId
# MAGIC           AND t2.ClientType = t1.ClientType
# MAGIC           AND t2.CaseStatusName <> 'Cancelled'
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.ClientId
# MAGIC   , t1.SourceClient
# MAGIC   , t1.CaseId
# MAGIC   , t1.Prework
# MAGIC   , t1.PreworkAnalyst
# MAGIC   , t1.PreworkAnalystTeamDate
# MAGIC   , t1.ReadyForAssessment
# MAGIC   , t1.AssessmentInProgress
# MAGIC   , t1.AssessmentAnalyst
# MAGIC   , t1.AssessmentAnalystTeamDate
# MAGIC   , t1.4EYECheck
# MAGIC   , t1.4EYEAnalyst
# MAGIC   , t1.4EYEAnalystTeamDate
# MAGIC   , t1.SignOff
# MAGIC   , t1.Fulfillment
# MAGIC   , t1.Completed
# MAGIC   , t1.Cancelled
# MAGIC   , t1.DaysInPrework
# MAGIC   , t1.DaysInReadyForAssessment
# MAGIC   , t1.DaysInAssessmentInProgress
# MAGIC   , t1.DaysIn4EYECheck
# MAGIC   , t1.DaysInSignOff
# MAGIC   , t1.DaysInFulfillment
# MAGIC   , t1.TotalCaseDuration
# MAGIC   , t1.QCInteractions
# MAGIC   , t1.LastAnalyst
# MAGIC   , t1.LastAnalystTeamDate
# MAGIC   , t1.PreworkAlias
# MAGIC   , t1.AssessmentAlias
# MAGIC   , t1.4EYEAlias
# MAGIC   , t1.LastAnalystAlias
# MAGIC   , t3.DateResponded AS RespondedDateFirstRFI
# MAGIC   , t3.DateCompleted AS CompletedDateFirstRFI
# MAGIC   , (SELECT MIN(RFI.DateCreated) FROM party_request_for_information RFI WHERE t1.ClientId = RFI.ClientId AND t1.GcobId = RFI.GcobId AND RFI.RequestTypeID = 1) AS FirstRFICreated -- 1 = 'Request For Information'
# MAGIC   , (SELECT MAX(RFI.DateCompleted) FROM party_request_for_information RFI WHERE t1.ClientId = RFI.ClientId AND t1.GcobId = RFI.GcobId AND RFI.RequestTypeID = 1) AS LatestRFICompleted
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled') THEN t4.MainCaseStatusType
# MAGIC       WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN 'Offboarding'
# MAGIC       WHEN t4.MainCaseStatusType = 'Sign-off' THEN 'Sign-off'
# MAGIC       WHEN t3.CasePhase IN ('Rebound Client Outreach', 'Client Outreach', 'Client Outreach Completed') THEN t3.CasePhase
# MAGIC       WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 'Rebound Assessment'
# MAGIC       WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 'Assessment 1'
# MAGIC       WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 'Assessment 2'
# MAGIC       WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 'Rebound Initiation'
# MAGIC       ELSE t4.MainCaseStatusType
# MAGIC     END AS CasePhase
# MAGIC   , CASE
# MAGIC       WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled') THEN 14
# MAGIC       WHEN t4.MainCaseStatusType = 'Sign-off' THEN 12
# MAGIC       WHEN t3.CasePhase = 'Client Outreach' THEN 5
# MAGIC       WHEN t3.CasePhase = 'Rebound Client Outreach' THEN 8
# MAGIC       WHEN t3.CasePhase = 'Client Outreach Completed' THEN 6
# MAGIC       WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 11
# MAGIC       WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 4
# MAGIC       WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 7
# MAGIC       WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 2
# MAGIC       ELSE t4.MainCaseStatusSortOrder
# MAGIC     END AS CasePhaseSortOrder
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t2.ReviewTypeName LIKE '%Offboarding%' AND t4.MainCaseStatusType NOT IN ('Completed', 'Cancelled') THEN 'Offboarding'
# MAGIC       WHEN YEAR(t10.NextReviewDate) > 2025 THEN 'Completed'
# MAGIC       WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled', 'KYC assessment in progress') AND YEAR(t10.NextReviewDate) < 2026 THEN 'Not yet started'
# MAGIC       WHEN t4.MainCaseStatusType = 'Sign-off' THEN 'Sign-off'
# MAGIC       WHEN t3.CasePhase IN ('Rebound Client Outreach', 'Client Outreach', 'Client Outreach Completed') THEN t3.CasePhase
# MAGIC       WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 'Rebound Assessment'
# MAGIC       WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 'Assessment 1'
# MAGIC       WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 'Assessment 2'
# MAGIC       WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 'Rebound Initiation'
# MAGIC       ELSE t4.MainCaseStatusType
# MAGIC     END AS CasePhaseCurrentYear
# MAGIC   , CASE
# MAGIC       WHEN YEAR(t10.NextReviewDate) > 2025 THEN 14
# MAGIC       WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled', 'KYC assessment in progress') AND YEAR(t10.NextReviewDate) < 2026 THEN 0
# MAGIC       WHEN t4.MainCaseStatusType = 'Sign-off' THEN 12
# MAGIC       WHEN t3.CasePhase = 'Client Outreach' THEN 5
# MAGIC       WHEN t3.CasePhase = 'Rebound Client Outreach' THEN 8
# MAGIC       WHEN t3.CasePhase = 'Client Outreach Completed' THEN 6
# MAGIC       WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 11
# MAGIC       WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 4
# MAGIC       WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 7
# MAGIC       WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 2
# MAGIC       ELSE t4.MainCaseStatusSortOrder
# MAGIC     END AS CasePhaseSortCurrentYear
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t3.CasePhase = 'Client Outreach' THEN t3.DaysInWaitingForInformation
# MAGIC       WHEN t3.CasePhase = 'Rebound Client Outreach' THEN t5.DurationInfoRequestInWorkingDay
# MAGIC       WHEN t1.CasePhase = 'Prework' THEN t1.DaysInPrework
# MAGIC       WHEN t1.CasePhase = 'Ready for assessment' THEN t1.DaysInReadyForAssessment
# MAGIC       WHEN t1.CasePhase = 'Assessment in progress' THEN t1.DaysInAssessmentInProgress
# MAGIC       WHEN t1.CasePhase = '4-EYE check' THEN t1.DaysIn4EYECheck
# MAGIC       WHEN t1.CasePhase = 'Fulfilment' THEN t1.DaysInFulfillment
# MAGIC       WHEN t1.CasePhase = 'Sign-off' THEN t1.DaysInSignOff
# MAGIC     END AS DaysInCurrentCasePhase
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t2.CaseStatusName NOT IN ('Cancelled', 'Completed') AND CURRENT_DATE() > TO_DATE(ADD_MONTHS(t1.Prework, 3), 'yyyy-MM-dd') -- 'Migrated'
# MAGIC         AND t2.ReviewTypeName = 'Event Driven Review' THEN 'Yes' ELSE 'No'
# MAGIC     END AS EDROverdue
# MAGIC   -- , CASE 
# MAGIC   --     WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN t1.Completed
# MAGIC   --     ELSE t1.SignOff
# MAGIC   --   END AS SignoffDateTC
# MAGIC   , CASE 
# MAGIC       WHEN t2.ValidatedRiskLevel IN ('High', 'Unacceptable') THEN DATE_ADD(CAST(t10.NextReviewDate AS DATE), 60) -- Adding 60 days for 2 months
# MAGIC       WHEN t2.ReviewTypeName = 'Event Driven Review' THEN DATE_ADD(t1.Prework, 90)
# MAGIC     ELSE t10.NextReviewDate END AS TCNRD
# MAGIC   -- , t6.UserTeam AS 4EyeUserTeam
# MAGIC   , t6.Team AS 4EyeUserTeam -- UserTeam
# MAGIC   , t6.Department AS 4EyeDepartment
# MAGIC   , t7.Department AS PreworkDepartment
# MAGIC   -- , CASE 
# MAGIC   --     WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN COALESCE(t8.UserTeam, t7.UserTeam) 
# MAGIC   --     WHEN t1.CasePhase = 'Prework' THEN t7.UserTeam
# MAGIC   --     WHEN t1.CasePhase = 'Ready for assessment' THEN t7.UserTeam
# MAGIC   --     WHEN t1.CasePhase = 'Assessment in progress' THEN t8.UserTeam
# MAGIC   --     WHEN t1.CasePhase = '4-EYE check' THEN t8.UserTeam
# MAGIC   --     WHEN t1.CasePhase = 'Fulfilment' THEN t8.UserTeam
# MAGIC   --     WHEN t1.CasePhase = 'Sign-off' THEN t8.UserTeam
# MAGIC   --     WHEN t1.CasePhase = 'Completed' THEN COALESCE(t8.UserTeam, t9.UserTeam)
# MAGIC   --     WHEN t1.CasePhase = 'Cancelled' THEN t8.UserTeam
# MAGIC   --   END AS KYCUserTeam
# MAGIC   , CASE 
# MAGIC       WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN COALESCE(t8.Team, t7.Team) -- UserTeam
# MAGIC       WHEN t1.CasePhase = 'Prework' THEN t7.Team -- UserTeam
# MAGIC       WHEN t1.CasePhase = 'Ready for assessment' THEN t7.Team -- UserTeam
# MAGIC       WHEN t1.CasePhase = 'Assessment in progress' THEN t8.Team -- UserTeam
# MAGIC       WHEN t1.CasePhase = '4-EYE check' THEN t8.Team -- UserTeam
# MAGIC       WHEN t1.CasePhase = 'Fulfilment' THEN t8.Team -- UserTeam
# MAGIC       WHEN t1.CasePhase = 'Sign-off' THEN t8.Team -- UserTeam
# MAGIC       WHEN t1.CasePhase = 'Completed' THEN COALESCE(t8.Team, t9.Team) -- UserTeam
# MAGIC       WHEN t1.CasePhase = 'Cancelled' THEN t8.Team -- UserTeam
# MAGIC     END AS KYCUserTeam
# MAGIC   , CASE 
# MAGIC       WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN COALESCE(t8.Department, t7.Department) 
# MAGIC       WHEN t1.CasePhase = 'Prework' THEN t7.Department
# MAGIC       WHEN t1.CasePhase = 'Ready for assessment' THEN t7.Department
# MAGIC       WHEN t1.CasePhase = 'Assessment in progress' THEN t8.Department
# MAGIC       WHEN t1.CasePhase = '4-EYE check' THEN t8.Department
# MAGIC       WHEN t1.CasePhase = 'Fulfilment' THEN t8.Department
# MAGIC       WHEN t1.CasePhase = 'Sign-off' THEN t8.Department
# MAGIC       WHEN t1.CasePhase = 'Completed' THEN COALESCE(t8.Department, t9.Department)
# MAGIC       WHEN t1.CasePhase = 'Cancelled' THEN t8.Department
# MAGIC     END AS KYCDepartment
# MAGIC
# MAGIC   , t7.Team AS PreworkUserTeam -- UserTeam 
# MAGIC   , t8.Department AS CDDDepartment
# MAGIC   , t8.Team AS CDDUserTeam -- UserTeam
# MAGIC
# MAGIC FROM workitems_status t1
# MAGIC INNER JOIN party_case_client_details t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN rfi_status t3 ON t1.SourceClient = t3.SourceClient
# MAGIC LEFT JOIN main_case_status_type t4 ON t1.SourceClient = t4.SourceClient
# MAGIC LEFT JOIN CTE_ReboundClientOutreach t5 ON t1.SourceClient = t5.SourceClient
# MAGIC -- LEFT JOIN UserTeamRegistry t6 ON t1.4EYEAnalyst = t6.AssignedUser AND t1.4EYEAnalystTeamDate >= t6.TeamStartDate AND t1.4EYEAnalystTeamDate <= COALESCE(t6.TeamEndDate, CURRENT_DATE())
# MAGIC -- LEFT JOIN UserTeamRegistry t7 ON t1.PreworkAnalyst = t7.AssignedUser AND t1.PreworkAnalystTeamDate >= t7.TeamStartDate AND t1.PreworkAnalystTeamDate <= COALESCE(t7.TeamEndDate, CURRENT_DATE())
# MAGIC -- LEFT JOIN UserTeamRegistry t8 ON t1.AssessmentAnalyst = t8.AssignedUser AND t1.AssessmentAnalystTeamDate >= t8.TeamStartDate AND t1.AssessmentAnalystTeamDate <= COALESCE(t8.TeamEndDate, CURRENT_DATE())
# MAGIC -- LEFT JOIN UserTeamRegistry t9 ON t1.LastAnalyst = t9.AssignedUser AND t1.LastAnalystTeamDate >= t9.TeamStartDate AND t1.LastAnalystTeamDate <= COALESCE(t9.TeamEndDate, CURRENT_DATE())
# MAGIC
# MAGIC LEFT JOIN radar.userlistmapping t6um ON t1.4EYEAnalyst = t6um.UserNameOld -- mapping table 
# MAGIC LEFT JOIN radar.UserTeamRegistry t6 ON COALESCE(t6um.UserNameNew, t1.4EYEAnalyst) = t6.userName AND t1.4EYEAnalystTeamDate >= t6.TeamStartDate AND t1.4EYEAnalystTeamDate <= COALESCE(t6.TeamEndDate, CURRENT_DATE())
# MAGIC LEFT JOIN radar.userlistmapping t7um ON t1.PreworkAnalyst = t7um.UserNameOld -- mapping table
# MAGIC LEFT JOIN radar.UserTeamRegistry t7 ON COALESCE(t7um.UserNameNew, t1.PreworkAnalyst) = t7.userName AND t1.PreworkAnalystTeamDate >= t7.TeamStartDate AND t1.PreworkAnalystTeamDate <= COALESCE(t7.TeamEndDate, CURRENT_DATE())
# MAGIC LEFT JOIN radar.userlistmapping t8um ON t1.AssessmentAnalyst = t8um.UserNameOld -- mapping table
# MAGIC LEFT JOIN radar.UserTeamRegistry t8 ON COALESCE(t8um.UserNameNew,  t1.AssessmentAnalyst) = t8.userName AND t1.AssessmentAnalystTeamDate >= t8.TeamStartDate AND t1.AssessmentAnalystTeamDate <= COALESCE(t8.TeamEndDate, CURRENT_DATE())
# MAGIC LEFT JOIN radar.userlistmapping t9um ON t1.LastAnalyst = t9um.UserNameOld -- mapping table
# MAGIC LEFT JOIN radar.UserTeamRegistry t9 ON COALESCE(t9um.UserNameNew, t1.LastAnalyst) = t9.userName AND t1.LastAnalystTeamDate >= t9.TeamStartDate AND t1.LastAnalystTeamDate <= COALESCE(t9.TeamEndDate, CURRENT_DATE())
# MAGIC
# MAGIC LEFT JOIN CTE_LatestNextReviewDate t10 ON t2.GcobId = t10.GcobId AND t2.ClientType = t10.ClientType AND t2.CaseId = t10.CaseId
# MAGIC
# MAGIC */

# COMMAND ----------

# DBTITLE 1,workitems_status_final_temp
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW workitems_status_final_temp AS -- same columns found in rpt.cases
# MAGIC
# MAGIC WITH DurationInfoRequestInWorkingDay_CTE AS (
# MAGIC   SELECT
# MAGIC     ClientId
# MAGIC     , CaseId
# MAGIC     , GcobId
# MAGIC     , StatusType
# MAGIC     , DATEDIFF(DAY, DateCreated, COALESCE(DateCompleted, CURRENT_DATE())) + 1 
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, DateCreated, COALESCE(DateCompleted, CURRENT_DATE())) * 2) 
# MAGIC           + (CASE WHEN DATE_FORMAT(DateCreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC           + (CASE WHEN DATE_FORMAT(DateCompleted, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       ) AS DurationInfoRequestInWorkingDay
# MAGIC   FROM party_request_for_information
# MAGIC )
# MAGIC , CTE_ReboundClientOutreach AS (
# MAGIC     SELECT
# MAGIC       t1.SourceClient
# MAGIC       , MIN(t2.DurationInfoRequestInWorkingDay) AS DurationInfoRequestInWorkingDay
# MAGIC     FROM rfi_status t1
# MAGIC     LEFT JOIN DurationInfoRequestInWorkingDay_CTE t2 ON t1.GcobId = t2.GcobId AND t1.ClientId = t2.ClientId AND t1.CaseId = t2.CaseId
# MAGIC     WHERE 1=1
# MAGIC       AND t1.RequestCompleted = 'No'
# MAGIC       AND t2.StatusType = 'PendingResponse'
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC , CTE_LatestNextReviewDate AS (
# MAGIC   -- get the NextReviewDate per GcobId corresponding to the latest non cancelled caseid
# MAGIC     SELECT 
# MAGIC       t1.GcobId
# MAGIC       , t1.ClientType
# MAGIC       , t1.CaseId
# MAGIC       , t1.NextReviewDate
# MAGIC     FROM party_case_client_details t1
# MAGIC     WHERE t1.CaseStatusName <> 'Cancelled'
# MAGIC     AND t1.CaseId = (
# MAGIC         SELECT
# MAGIC           MAX(t2.CaseId)
# MAGIC         FROM party_case_client_details t2
# MAGIC         WHERE t2.GcobId = t1.GcobId
# MAGIC           AND t2.ClientType = t1.ClientType
# MAGIC           AND t2.CaseStatusName <> 'Cancelled'
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC , second_rfi_creation_date AS (
# MAGIC   SELECT 
# MAGIC     ClientId,
# MAGIC     GcobId,
# MAGIC     DateCreated,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY ClientId, GcobId ORDER BY DateCreated ASC) AS rn
# MAGIC   FROM party_request_for_information
# MAGIC   WHERE RequestTypeID = 1
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.ClientId
# MAGIC   , t1.SourceClient
# MAGIC   , t1.CaseId
# MAGIC   , t1.Prework
# MAGIC   , t1.PreworkAnalyst
# MAGIC   , t1.PreworkAnalystTeamDate
# MAGIC   , t1.ReadyForAssessment
# MAGIC   , t1.AssessmentInProgress
# MAGIC   , t1.AssessmentAnalyst
# MAGIC   , t1.AssessmentAnalystTeamDate
# MAGIC   , t1.4EYECheck
# MAGIC   , t1.4EYEAnalyst
# MAGIC   , t1.4EYEAnalystTeamDate
# MAGIC   , t1.SignOff
# MAGIC   , t1.Fulfillment
# MAGIC   , t1.Completed
# MAGIC   , t1.Cancelled
# MAGIC   /*
# MAGIC   creating new fields to count the number of days in each casephase depending on each occurance of that workitem
# MAGIC   , t1.DaysInPrework
# MAGIC   , t1.DaysInReadyForAssessment
# MAGIC   , t1.DaysInAssessmentInProgress
# MAGIC   , t1.DaysIn4EYECheck
# MAGIC   , t1.DaysInSignOff
# MAGIC   , t1.DaysInFulfillment
# MAGIC   */
# MAGIC   , t1.TotalCaseDuration
# MAGIC   , t1.QCInteractions
# MAGIC   , t1.LastAnalyst
# MAGIC   , t1.LastAnalystTeamDate
# MAGIC   , t1.PreworkAlias
# MAGIC   , t1.AssessmentAlias
# MAGIC   , t1.4EYEAlias
# MAGIC   , t1.LastAnalystAlias
# MAGIC   , t3.DateResponded AS RespondedDateFirstRFI
# MAGIC   , t3.DateCompleted AS CompletedDateFirstRFI
# MAGIC   , (SELECT MIN(RFI.DateCreated) FROM party_request_for_information RFI WHERE t1.ClientId = RFI.ClientId AND t1.GcobId = RFI.GcobId AND RFI.RequestTypeID = 1) AS FirstRFICreated -- 1 = 'Request For Information'
# MAGIC   , (SELECT MAX(RFI.DateCompleted) FROM party_request_for_information RFI WHERE t1.ClientId = RFI.ClientId AND t1.GcobId = RFI.GcobId AND RFI.RequestTypeID = 1) AS LatestRFICompleted,
# MAGIC   t4.MainCaseStatusType,
# MAGIC   t2.ReviewTypeName,
# MAGIC   t3.DateCreated as CreatedDateRFI,
# MAGIC   (SELECT MIN(RFI.DateCompleted) FROM party_request_for_information RFI WHERE t1.ClientId = RFI.ClientId AND t1.GcobId = RFI.GcobId AND RFI.RequestTypeID = 1) AS FirstRFICompleted 
# MAGIC , rfi.DateCreated AS SecondRFICreatedDate
# MAGIC , t1.ReadyFor4EYECheck
# MAGIC , CASE
# MAGIC     WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled') THEN t4.MainCaseStatusType
# MAGIC     WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN 'Offboarding'
# MAGIC     WHEN t4.MainCaseStatusType = 'Sign-off' THEN 'Sign-off'
# MAGIC     WHEN t3.CasePhase IN ('Rebound Client Outreach', 'Client Outreach', 'Client Outreach Completed') THEN t3.CasePhase
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 'Rebound Assessment'
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 'Assessment 1'
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 'Assessment 2'
# MAGIC     WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 'Rebound Initiation'
# MAGIC     ELSE t4.MainCaseStatusType
# MAGIC   END AS CasePhase
# MAGIC
# MAGIC , CASE
# MAGIC     WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled') THEN 14
# MAGIC     WHEN t4.MainCaseStatusType = 'Sign-off' THEN 12
# MAGIC     WHEN t3.CasePhase = 'Client Outreach' THEN 5
# MAGIC     WHEN t3.CasePhase = 'Rebound Client Outreach' THEN 8
# MAGIC     WHEN t3.CasePhase = 'Client Outreach Completed' THEN 6
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 11
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 4
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 7
# MAGIC     WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 2
# MAGIC     ELSE t4.MainCaseStatusSortOrder
# MAGIC   END AS CasePhaseSortOrder
# MAGIC
# MAGIC , CASE
# MAGIC     WHEN t2.ReviewTypeName LIKE '%Offboarding%' AND t4.MainCaseStatusType NOT IN ('Completed', 'Cancelled') THEN 'Offboarding'
# MAGIC     WHEN YEAR(t10.NextReviewDate) > YEAR(GETDATE()) THEN 'Completed'
# MAGIC     WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled', 'KYC assessment in progress') AND YEAR(t10.NextReviewDate) < YEAR(GETDATE()) + 1 THEN 'Not yet started'
# MAGIC     WHEN t4.MainCaseStatusType = 'Sign-off' THEN 'Sign-off'
# MAGIC     WHEN t3.CasePhase IN ('Rebound Client Outreach', 'Client Outreach', 'Client Outreach Completed') THEN t3.CasePhase
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 'Rebound Assessment'
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 'Assessment 1'
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 'Assessment 2'
# MAGIC     WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 'Rebound Initiation'
# MAGIC     ELSE t4.MainCaseStatusType
# MAGIC   END AS CasePhaseCurrentYear
# MAGIC
# MAGIC , CASE
# MAGIC     WHEN YEAR(t10.NextReviewDate) > YEAR(GETDATE()) THEN 14
# MAGIC     WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled', 'KYC assessment in progress') AND YEAR(t10.NextReviewDate) < YEAR(GETDATE()) + 1 THEN 0
# MAGIC     WHEN t4.MainCaseStatusType = 'Sign-off' THEN 12
# MAGIC     WHEN t3.CasePhase = 'Client Outreach' THEN 5
# MAGIC     WHEN t3.CasePhase = 'Rebound Client Outreach' THEN 8
# MAGIC     WHEN t3.CasePhase = 'Client Outreach Completed' THEN 6
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 11
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 4
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 7
# MAGIC     WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 2
# MAGIC     ELSE t4.MainCaseStatusSortOrder
# MAGIC   END AS CasePhaseSortCurrentYear
# MAGIC
# MAGIC , CASE
# MAGIC     WHEN t3.CasePhase = 'Client Outreach' THEN t3.DaysInWaitingForInformation
# MAGIC     WHEN t3.CasePhase = 'Rebound Client Outreach' THEN t5.DurationInfoRequestInWorkingDay
# MAGIC     WHEN t1.CasePhase = 'Prework' THEN t1.DaysInPrework
# MAGIC     WHEN t1.CasePhase = 'Ready for assessment' THEN t1.DaysInReadyForAssessment
# MAGIC     WHEN t4.MainCaseStatusType = 'Ready for QC' THEN t1.DaysInReadyFor4EYECheck
# MAGIC     WHEN t1.CasePhase = 'Assessment in progress' THEN t1.DaysInAssessmentInProgress
# MAGIC     WHEN t1.CasePhase = '4-EYE check' THEN t1.DaysIn4EYECheck
# MAGIC     WHEN t1.CasePhase = 'Fulfilment' THEN t1.DaysInFulfillment
# MAGIC     WHEN t1.CasePhase = 'Sign-off' THEN t1.DaysInSignOff
# MAGIC   END AS DaysInCurrentCasePhaseTemp
# MAGIC
# MAGIC , CASE
# MAGIC     WHEN t2.CaseStatusName NOT IN ('Cancelled', 'Completed') AND CURRENT_DATE() > TO_DATE(ADD_MONTHS(t1.Prework, 3), 'yyyy-MM-dd') -- 'Migrated'
# MAGIC       AND t2.ReviewTypeName = 'Event Driven Review' THEN 'Yes' ELSE 'No'
# MAGIC   END AS EDROverdue
# MAGIC -- , CASE 
# MAGIC --     WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN t1.Completed
# MAGIC --     ELSE t1.SignOff
# MAGIC --   END AS SignoffDateTC
# MAGIC , CASE 
# MAGIC     WHEN t2.ValidatedRiskLevel IN ('High', 'Unacceptable') THEN DATE_ADD(CAST(t10.NextReviewDate AS DATE), 60) -- Adding 60 days for 2 months
# MAGIC     WHEN t2.ReviewTypeName = 'Event Driven Review' THEN DATE_ADD(t1.Prework, 90)
# MAGIC   ELSE t10.NextReviewDate END AS TCNRD
# MAGIC -- , t6.UserTeam AS 4EyeUserTeam
# MAGIC , t6.Team AS 4EyeUserTeam -- UserTeam
# MAGIC , t6.Department AS 4EyeDepartment
# MAGIC , t7.Department AS PreworkDepartment
# MAGIC -- , CASE 
# MAGIC --     WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN COALESCE(t8.UserTeam, t7.UserTeam) 
# MAGIC --     WHEN t1.CasePhase = 'Prework' THEN t7.UserTeam
# MAGIC --     WHEN t1.CasePhase = 'Ready for assessment' THEN t7.UserTeam
# MAGIC --     WHEN t1.CasePhase = 'Assessment in progress' THEN t8.UserTeam
# MAGIC --     WHEN t1.CasePhase = '4-EYE check' THEN t8.UserTeam
# MAGIC --     WHEN t1.CasePhase = 'Fulfilment' THEN t8.UserTeam
# MAGIC --     WHEN t1.CasePhase = 'Sign-off' THEN t8.UserTeam
# MAGIC --     WHEN t1.CasePhase = 'Completed' THEN COALESCE(t8.UserTeam, t9.UserTeam)
# MAGIC --     WHEN t1.CasePhase = 'Cancelled' THEN t8.UserTeam
# MAGIC --   END AS KYCUserTeam
# MAGIC , CASE 
# MAGIC     WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN COALESCE(t8.Team, t7.Team) -- UserTeam
# MAGIC     WHEN t1.CasePhase = 'Prework' THEN t7.Team -- UserTeam
# MAGIC     WHEN t1.CasePhase = 'Ready for assessment' THEN t7.Team -- UserTeam
# MAGIC     WHEN t1.CasePhase = 'Assessment in progress' THEN t8.Team -- UserTeam
# MAGIC     WHEN t1.CasePhase = '4-EYE check' THEN t8.Team -- UserTeam
# MAGIC     WHEN t1.CasePhase = 'Fulfilment' THEN t8.Team -- UserTeam
# MAGIC     WHEN t1.CasePhase = 'Sign-off' THEN t8.Team -- UserTeam
# MAGIC     WHEN t1.CasePhase = 'Completed' THEN COALESCE(t8.Team, t9.Team) -- UserTeam
# MAGIC     WHEN t1.CasePhase = 'Cancelled' THEN t8.Team -- UserTeam
# MAGIC   END AS KYCUserTeam
# MAGIC , CASE 
# MAGIC     WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN COALESCE(t8.Department, t7.Department) 
# MAGIC     WHEN t1.CasePhase = 'Prework' THEN t7.Department
# MAGIC     WHEN t1.CasePhase = 'Ready for assessment' THEN t7.Department
# MAGIC     WHEN t1.CasePhase = 'Assessment in progress' THEN t8.Department
# MAGIC     WHEN t1.CasePhase = '4-EYE check' THEN t8.Department
# MAGIC     WHEN t1.CasePhase = 'Fulfilment' THEN t8.Department
# MAGIC     WHEN t1.CasePhase = 'Sign-off' THEN t8.Department
# MAGIC     WHEN t1.CasePhase = 'Completed' THEN COALESCE(t8.Department, t9.Department)
# MAGIC     WHEN t1.CasePhase = 'Cancelled' THEN t8.Department
# MAGIC   END AS KYCDepartment
# MAGIC
# MAGIC , t7.Team AS PreworkUserTeam -- UserTeam 
# MAGIC , t8.Department AS CDDDepartment
# MAGIC , t8.Team AS CDDUserTeam -- UserTeam
# MAGIC
# MAGIC FROM workitems_status t1
# MAGIC INNER JOIN party_case_client_details t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN rfi_status t3 ON t1.SourceClient = t3.SourceClient
# MAGIC LEFT JOIN main_case_status_type t4 ON t1.SourceClient = t4.SourceClient
# MAGIC LEFT JOIN CTE_ReboundClientOutreach t5 ON t1.SourceClient = t5.SourceClient
# MAGIC -- LEFT JOIN UserTeamRegistry t6 ON t1.4EYEAnalyst = t6.AssignedUser AND t1.4EYEAnalystTeamDate >= t6.TeamStartDate AND t1.4EYEAnalystTeamDate <= COALESCE(t6.TeamEndDate, CURRENT_DATE())
# MAGIC -- LEFT JOIN UserTeamRegistry t7 ON t1.PreworkAnalyst = t7.AssignedUser AND t1.PreworkAnalystTeamDate >= t7.TeamStartDate AND t1.PreworkAnalystTeamDate <= COALESCE(t7.TeamEndDate, CURRENT_DATE())
# MAGIC -- LEFT JOIN UserTeamRegistry t8 ON t1.AssessmentAnalyst = t8.AssignedUser AND t1.AssessmentAnalystTeamDate >= t8.TeamStartDate AND t1.AssessmentAnalystTeamDate <= COALESCE(t8.TeamEndDate, CURRENT_DATE())
# MAGIC -- LEFT JOIN UserTeamRegistry t9 ON t1.LastAnalyst = t9.AssignedUser AND t1.LastAnalystTeamDate >= t9.TeamStartDate AND t1.LastAnalystTeamDate <= COALESCE(t9.TeamEndDate, CURRENT_DATE())
# MAGIC
# MAGIC LEFT JOIN radar.userlistmapping t6um ON t1.4EYEAnalyst = t6um.UserNameOld -- mapping table 
# MAGIC LEFT JOIN radar.UserTeamRegistry t6 ON COALESCE(t6um.UserNameNew, t1.4EYEAnalyst) = t6.userName AND t1.4EYEAnalystTeamDate >= t6.TeamStartDate AND t1.4EYEAnalystTeamDate <= COALESCE(t6.TeamEndDate, CURRENT_DATE())
# MAGIC LEFT JOIN radar.userlistmapping t7um ON t1.PreworkAnalyst = t7um.UserNameOld -- mapping table
# MAGIC LEFT JOIN radar.UserTeamRegistry t7 ON COALESCE(t7um.UserNameNew, t1.PreworkAnalyst) = t7.userName AND t1.PreworkAnalystTeamDate >= t7.TeamStartDate AND t1.PreworkAnalystTeamDate <= COALESCE(t7.TeamEndDate, CURRENT_DATE())
# MAGIC LEFT JOIN radar.userlistmapping t8um ON t1.AssessmentAnalyst = t8um.UserNameOld -- mapping table
# MAGIC LEFT JOIN radar.UserTeamRegistry t8 ON COALESCE(t8um.UserNameNew,  t1.AssessmentAnalyst) = t8.userName AND t1.AssessmentAnalystTeamDate >= t8.TeamStartDate AND t1.AssessmentAnalystTeamDate <= COALESCE(t8.TeamEndDate, CURRENT_DATE())
# MAGIC LEFT JOIN radar.userlistmapping t9um ON t1.LastAnalyst = t9um.UserNameOld -- mapping table
# MAGIC LEFT JOIN radar.UserTeamRegistry t9 ON COALESCE(t9um.UserNameNew, t1.LastAnalyst) = t9.userName AND t1.LastAnalystTeamDate >= t9.TeamStartDate AND t1.LastAnalystTeamDate <= COALESCE(t9.TeamEndDate, CURRENT_DATE())
# MAGIC
# MAGIC LEFT JOIN CTE_LatestNextReviewDate t10 ON t2.GcobId = t10.GcobId AND t2.ClientType = t10.ClientType AND t2.CaseId = t10.CaseId
# MAGIC LEFT JOIN second_rfi_creation_date rfi ON t1.ClientId = rfi.ClientId AND t1.GcobId = rfi.GcobId AND rfi.rn = 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## logic for days in each casephase depending on the occurance of workitem

# COMMAND ----------

# DBTITLE 1,Total Days In WorkItem Status
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
from datetime import timedelta, datetime

# Step 1: Load the table
party_workitem = spark.table("party_workitem")

# Step 2: Define the statuses of interest
statuses = [
    "Initiation In Progress",
    "Ready for KYC assessment",
    "KYC assessment in progress",
    "Ready for 4 eye check",
    "4 eye check in progress",
    "Client owner sign off requested",
    "Product fulfilment in progress"
]

# Step 3: Filter for relevant statuses
filtered_df = party_workitem.filter(F.col("CaseStatusName").isin(statuses))

# Step 4: Define UDF to generate workdays
def generate_workdays(start, end):
    if start is None or end is None:
        return []
    start_date = start.date()
    end_date = end.date()
    days = []
    while start_date <= end_date:
        if start_date.weekday() < 5:  # Monday to Friday
            days.append(str(start_date))
        start_date += timedelta(days=1)
    return days

generate_workdays_udf = udf(generate_workdays, ArrayType(StringType()))

# Step 5: Apply UDF with coalesce logic
today = datetime.today()
with_workdays = filtered_df.withColumn(
    "workdays",
    generate_workdays_udf(
        F.col("WorkItemCreatedDate"),
        F.coalesce(F.col("WorkItemCompletedDate"), F.lit(today))
    )
)

# Step 6: Explode and deduplicate
exploded = with_workdays.select(
    "sourceClient", "caseId", "CaseStatusName", F.explode("workdays").alias("workday")
)

unique_days = exploded.dropDuplicates(["sourceClient", "caseId", "CaseStatusName", "workday"])

# Step 7: Pivot the result
pivoted_result = unique_days.groupBy("sourceClient", "caseId").pivot("CaseStatusName").agg(
    F.count("workday")
)

# Step 8: Rename columns
for status in statuses:
    safe_col_name = status.replace(" ", "_").replace("-", "_")
    pivoted_result = pivoted_result.withColumnRenamed(status, f"daysIn_{safe_col_name}")

# Step 9: Create a view
pivoted_result.createOrReplaceTempView("totalDaysInWorkItemStatus")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Days in Rebound Initiation

# COMMAND ----------

# DBTITLE 1,Days in Rebound Initiation
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
from datetime import timedelta

# Step 1: Load the table
party_workitem = spark.table("party_workitem")

# Step 2: Get first 'KYC assessment in progress' date per sourceClient
first_kyc = party_workitem.filter(
    F.col("CaseStatusName") == "KYC assessment in progress"
).groupBy("sourceClient").agg(
    F.min("WorkItemCreatedDate").alias("first_kyc_date")
)

# Step 3: Join with original table to get 'Initiation in Progress' records after first KYC assessment
initiation_after_kyc = party_workitem.filter(
    F.col("CaseStatusName") == "Initiation In Progress"
).join(first_kyc, on="sourceClient").filter(
    F.col("WorkItemCreatedDate") > F.col("first_kyc_date")
)

# Step 4: Define UDF to generate workdays
def generate_workdays(start, end):
    if start is None or end is None:
        return []
    start_date = start.date()
    end_date = end.date()
    days = []
    while start_date <= end_date:
        if start_date.weekday() < 5:  # Monday to Friday
            days.append(str(start_date))
        start_date += timedelta(days=1)
    return days

generate_workdays_udf = udf(generate_workdays, ArrayType(StringType()))

# Step 5: Apply UDF
initiation_with_days = initiation_after_kyc.withColumn(
    "workdays",
    generate_workdays_udf("WorkItemCreatedDate", "WorkItemCompletedDate")
)

# Step 6: Explode and count unique workdays
exploded = initiation_with_days.select(
    "sourceClient", F.explode("workdays").alias("workday")
)

unique_days = exploded.dropDuplicates(["sourceClient", "workday"])

# Step 7: Aggregate result
result = unique_days.groupBy("sourceClient").agg(
    F.count("workday").alias("daysInReboundInitiation")
)

# Step 8: Create a view
result.createOrReplaceTempView("daysInReboundInitiation")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Days in Rebound Assessment

# COMMAND ----------

# DBTITLE 1,Days in Rebound Assessment
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
from datetime import timedelta

# Step 1: Load the table
party_workitem = spark.table("party_workitem")

# Step 2: Get first 4-eye check date per sourceClient
first_4eye = party_workitem.filter(
    F.col("CaseStatusName") == "4 eye check in progress"
).groupBy("sourceClient").agg(
    F.min("WorkItemCreatedDate").alias("first_4eye_date")
)

# Step 3: Join with original table to get KYC records after first 4-eye check
kyc_after_first_4eye = party_workitem.filter(
    F.col("CaseStatusName") == "KYC assessment in progress"
).join(first_4eye, on="sourceClient").filter(
    F.col("WorkItemCreatedDate") > F.col("first_4eye_date")
)

# Step 4: Define UDF to generate workdays
def generate_workdays(start, end):
    if start is None or end is None:
        return []
    start_date = start.date()
    end_date = end.date()
    days = []
    while start_date <= end_date:
        if start_date.weekday() < 5:  # Monday to Friday
            days.append(str(start_date))
        start_date += timedelta(days=1)
    return days

generate_workdays_udf = udf(generate_workdays, ArrayType(StringType()))

# Step 5: Apply UDF
kyc_with_days = kyc_after_first_4eye.withColumn(
    "workdays",
    generate_workdays_udf("WorkItemCreatedDate", "WorkItemCompletedDate")
)

# Step 6: Explode and count unique workdays
exploded = kyc_with_days.select(
    "sourceClient", F.explode("workdays").alias("workday")
)

unique_days = exploded.dropDuplicates(["sourceClient", "workday"])

# Step 7: Aggregate result
result = unique_days.groupBy("sourceClient").agg(
    F.count("workday").alias("daysInReboundAssessment")
)

# Step 8: Create a view
result.createOrReplaceTempView("daysInReboundAssessment")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Number of rebound assessments

# COMMAND ----------

# DBTITLE 1,Number of Rebound Assessment
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 1: Filter relevant statuses
filtered = party_workitem.filter(
    F.col("CaseStatusName").isin("4 eye check in progress", "KYC assessment in progress")
)

# Step 2: Define window for ordering
window_spec = Window.partitionBy("sourceClient").orderBy("WorkItemCreatedDate")

# Step 3: Add previous status column
with_lag = filtered.withColumn(
    "prev_status", F.lag("CaseStatusName").over(window_spec)
)

# Step 4: Identify transitions from 4-eye to KYC
transitions = with_lag.filter(
    (F.col("prev_status") == "4 eye check in progress") &
    (F.col("CaseStatusName") == "KYC assessment in progress")
)

# Step 5: Count transitions per sourceClient
rebound_counts = transitions.groupBy("sourceClient").agg(
    F.count("*").alias("reboundAssessmentCount")
)

# Step 6: Create a view
rebound_counts.createOrReplaceTempView("reboundAssessmentCounts")


# COMMAND ----------

# DBTITLE 1,workitems_status_final for days in case phase with SLA Logic
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW workitems_status_daysInCurrentCasePhase AS
# MAGIC  
# MAGIC WITH latest_sprintstatus AS (
# MAGIC     SELECT
# MAGIC         CaseId,
# MAGIC         SprintStatus,
# MAGIC         CreatedOnUTC,
# MAGIC         EndDateUTC
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             CaseId,
# MAGIC             SprintStatus,
# MAGIC             CreatedOnUTC,
# MAGIC             EndDateUTC,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY CaseId ORDER BY CreatedOnUTC DESC) AS rn
# MAGIC         FROM radar.rdr_sprintstatuslog
# MAGIC     ) ranked
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC  
# MAGIC -- Aggregate outreach window per CaseId (one row per CaseId + sourceClient)
# MAGIC clientOutreachDates_CddPlanningApp AS (
# MAGIC     SELECT
# MAGIC         t1.CaseId,
# MAGIC         t1.sourceClient,
# MAGIC         ls.SprintStatus,  -- take the latest sprint status for that CaseId
# MAGIC         MIN(CASE
# MAGIC               WHEN t2.SprintStatus IN (
# MAGIC                    'Client Outreach Completed', 'Client Outreach',
# MAGIC                    'Rebound Client Outreach', 'Rebound Client Outreach Completed'
# MAGIC               ) THEN t2.CreatedOnUTC
# MAGIC             END) AS OutreachStart,
# MAGIC         MAX(CASE
# MAGIC               WHEN t2.SprintStatus IN (
# MAGIC                    'Client Outreach Completed', 'Client Outreach',
# MAGIC                    'Rebound Client Outreach', 'Rebound Client Outreach Completed'
# MAGIC               ) THEN COALESCE(t2.EndDateUTC, CURRENT_DATE)
# MAGIC             END) AS OutreachEnd
# MAGIC     FROM radar.rdr_caseplanningdetailsNew1 AS t1
# MAGIC     INNER JOIN radar.rdr_sprintstatuslog AS t2
# MAGIC         ON t1.CaseId = t2.CaseId
# MAGIC     INNER JOIN latest_sprintstatus AS ls
# MAGIC         ON t1.CaseId = ls.CaseId
# MAGIC     WHERE
# MAGIC        t2.SprintStatus IN (
# MAGIC             'Client Outreach Completed', 'Client Outreach',
# MAGIC             'Rebound Client Outreach', 'Rebound Client Outreach Completed'
# MAGIC       )
# MAGIC     GROUP BY t1.CaseId, t1.sourceClient, ls.SprintStatus
# MAGIC ),
# MAGIC  
# MAGIC  
# MAGIC clientOutreachDuration_CddPlanningApp AS (
# MAGIC     SELECT
# MAGIC         CaseId,
# MAGIC         sourceClient,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY sourceClient
# MAGIC             ORDER BY OutreachStart DESC, OutreachEnd DESC, CaseId DESC
# MAGIC         ) AS rn,
# MAGIC         CASE
# MAGIC             WHEN OutreachStart IS NOT NULL THEN
# MAGIC                 DATEDIFF(DAY, OutreachStart, COALESCE(OutreachEnd, CURRENT_TIMESTAMP())) + 1
# MAGIC                 - (DATEDIFF(WEEK, OutreachStart, COALESCE(OutreachEnd, CURRENT_TIMESTAMP())) * 2)
# MAGIC                 + CASE WHEN DATE_FORMAT(OutreachStart,'EEEE') = 'Sunday' THEN 1 ELSE 0 END
# MAGIC                 + CASE WHEN DATE_FORMAT(OutreachEnd, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END
# MAGIC             ELSE NULL
# MAGIC         END AS DaysInClientOutreach
# MAGIC     FROM clientOutreachDates_CddPlanningApp
# MAGIC QUALIFY rn = 1
# MAGIC ),
# MAGIC reboundclientOutreachDates_CddPlanningApp AS (
# MAGIC     SELECT
# MAGIC         t1.CaseId,
# MAGIC         t1.sourceClient,
# MAGIC         MIN(CASE WHEN SprintStatus = 'Rebound Client Outreach' THEN CreatedOnUTC END) AS OutreachStart,
# MAGIC         MAX(CASE WHEN SprintStatus IN  ('Rebound Client Outreach Completed', 'Rebound Client Outreach') THEN
# MAGIC             COALESCE(EndDateUTC, CURRENT_DATE) END) AS OutreachEnd
# MAGIC     from radar.rdr_caseplanningdetailsNew1 AS t1
# MAGIC     LEFT JOIN radar.rdr_sprintstatuslog AS t2 ON t1.CaseId = t2.CaseId
# MAGIC     WHERE SprintStatus IN ('Rebound Client Outreach', 'Rebound Client Outreach Completed')
# MAGIC     GROUP BY t1.CaseId, t1.sourceClient
# MAGIC ),
# MAGIC reboundclientOutreachDuration_CddPlanningApp AS (
# MAGIC     SELECT
# MAGIC         CaseId,
# MAGIC         sourceClient,
# MAGIC         CASE
# MAGIC             WHEN OutreachStart IS NOT NULL THEN
# MAGIC                 DATEDIFF(DAY, OutreachStart, COALESCE(OutreachEnd, CURRENT_TIMESTAMP())) + 1
# MAGIC                 - (DATEDIFF(WEEK, OutreachStart, COALESCE(OutreachEnd, CURRENT_TIMESTAMP())) * 2)
# MAGIC                 + CASE WHEN DATE_FORMAT(OutreachStart,'EEEE') = 'Sunday' THEN 1 ELSE 0 END
# MAGIC                 + CASE WHEN DATE_FORMAT(OutreachEnd, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END
# MAGIC             ELSE NULL
# MAGIC         END AS DaysInReboundClientOutreach
# MAGIC     FROM reboundclientOutreachDates_CddPlanningApp
# MAGIC ),
# MAGIC MLRO_CddPlanningApp AS (
# MAGIC     SELECT
# MAGIC         t1.CaseId,
# MAGIC         t1.sourceClient,
# MAGIC         ls.SprintStatus,
# MAGIC         MIN(CASE WHEN t2.SprintStatus = 'With CC secretaries' THEN t2.CreatedOnUTC END) AS OutreachStart,
# MAGIC         MAX(CASE WHEN t2.SprintStatus IN ('With CC secretaries') THEN
# MAGIC             COALESCE(t2.EndDateUTC, CURRENT_DATE) END) AS OutreachEnd
# MAGIC     FROM radar.rdr_caseplanningdetailsNew1 AS t1
# MAGIC     INNER JOIN radar.rdr_sprintstatuslog AS t2
# MAGIC         ON t1.CaseId = t2.CaseId
# MAGIC     INNER JOIN latest_sprintstatus AS ls
# MAGIC         ON t1.CaseId = ls.CaseId
# MAGIC     WHERE t2.SprintStatus IN ('With CC secretaries')
# MAGIC     GROUP BY t1.CaseId, t1.sourceClient, ls.SprintStatus
# MAGIC ),
# MAGIC MLRODuration_CddPlanningApp AS (
# MAGIC     SELECT
# MAGIC         CaseId,
# MAGIC         sourceClient,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY sourceClient
# MAGIC             ORDER BY OutreachStart DESC, OutreachEnd DESC, CaseId DESC
# MAGIC         ) AS rn,
# MAGIC         CASE
# MAGIC             WHEN OutreachStart IS NOT NULL THEN
# MAGIC                 DATEDIFF(DAY, OutreachStart, COALESCE(OutreachEnd, CURRENT_TIMESTAMP())) + 1
# MAGIC                 - (DATEDIFF(WEEK, OutreachStart, COALESCE(OutreachEnd, CURRENT_TIMESTAMP())) * 2)
# MAGIC                 + CASE WHEN DATE_FORMAT(OutreachStart,'EEEE') = 'Sunday' THEN 1 ELSE 0 END
# MAGIC                 + CASE WHEN DATE_FORMAT(OutreachEnd, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END
# MAGIC             ELSE NULL
# MAGIC         END AS DaysInMLRO_SLA
# MAGIC     FROM MLRO_CddPlanningApp
# MAGIC     QUALIFY rn = 1
# MAGIC ),
# MAGIC
# MAGIC sentToFosForOutreach_CddPlanningApp AS (
# MAGIC     SELECT
# MAGIC         t1.CaseId,
# MAGIC         t1.sourceClient,
# MAGIC         ls.SprintStatus,
# MAGIC         MIN(CASE WHEN t2.SprintStatus = 'Sent to FoS for Outreach' THEN t2.CreatedOnUTC END) AS OutreachStart,
# MAGIC         MAX(CASE WHEN t2.SprintStatus IN ('Sent to FoS for Outreach') THEN
# MAGIC             COALESCE(t2.EndDateUTC, CURRENT_DATE) END) AS OutreachEnd
# MAGIC     FROM radar.rdr_caseplanningdetailsNew1 AS t1
# MAGIC     INNER JOIN radar.rdr_sprintstatuslog AS t2
# MAGIC         ON t1.CaseId = t2.CaseId
# MAGIC     INNER JOIN latest_sprintstatus AS ls
# MAGIC         ON t1.CaseId = ls.CaseId
# MAGIC     WHERE t2.SprintStatus IN ('Sent to FoS for Outreach')
# MAGIC     GROUP BY t1.CaseId, t1.sourceClient, ls.SprintStatus
# MAGIC ),
# MAGIC sentToFosForOutreachDuration_CddPlanningApp AS (
# MAGIC     SELECT
# MAGIC         CaseId,
# MAGIC         sourceClient,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY sourceClient
# MAGIC             ORDER BY OutreachStart DESC, OutreachEnd DESC, CaseId DESC
# MAGIC         ) AS rn,
# MAGIC         CASE
# MAGIC             WHEN OutreachStart IS NOT NULL THEN
# MAGIC                 DATEDIFF(DAY, OutreachStart, COALESCE(OutreachEnd, CURRENT_TIMESTAMP())) + 1
# MAGIC                 - (DATEDIFF(WEEK, OutreachStart, COALESCE(OutreachEnd, CURRENT_TIMESTAMP())) * 2)
# MAGIC                 + CASE WHEN DATE_FORMAT(OutreachStart,'EEEE') = 'Sunday' THEN 1 ELSE 0 END
# MAGIC                 + CASE WHEN DATE_FORMAT(OutreachEnd, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END
# MAGIC             ELSE NULL
# MAGIC         END AS DaysInSentToFosForOutreach_SLA
# MAGIC     FROM sentToFosForOutreach_CddPlanningApp
# MAGIC     QUALIFY rn = 1
# MAGIC )
# MAGIC
# MAGIC SELECT distinct
# MAGIC   t1.*
# MAGIC , t2.daysInReboundAssessment
# MAGIC , t3.daysIn_4_eye_check_in_progress as daysInQC
# MAGIC , t3.daysIn_Client_owner_sign_off_requested as daysInSignOff
# MAGIC , t3.daysIn_Product_fulfilment_in_progress as daysInFulfillment
# MAGIC , t3.daysIn_Ready_for_4_eye_check as daysInReadyForQC
# MAGIC , t3.daysIn_Ready_for_KYC_assessment as daysInReadyForKYCAssessment
# MAGIC , t5.reboundAssessmentCount as numberOfReboundAssessment
# MAGIC , COALESCE(t3.daysIn_Initiation_In_Progress, 0) + COALESCE(t4.daysInReboundInitiation, 0) AS daysInPrework_Sla--(Sum of daysInInitiation & daysInReboundInitiation)
# MAGIC  
# MAGIC -- Days in Assessment 1 (first assessment, no RFI)
# MAGIC ,  CASE
# MAGIC     WHEN --MainCaseStatusType = 'Assessment'
# MAGIC          --AND t3.DateCreated IS NULL
# MAGIC          AssessmentInProgress IS NOT NULL THEN
# MAGIC          --AND t1.ReadyFor4EYECheck IS NULL THEN
# MAGIC       DATEDIFF(DAY, AssessmentInProgress, COALESCE(FirstRFICreated, ReadyFor4EYECheck,
# MAGIC         CURRENT_TIMESTAMP())) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, AssessmentInProgress, COALESCE(FirstRFICreated, ReadyFor4EYECheck,
# MAGIC             CURRENT_TIMESTAMP()
# MAGIC           )) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(AssessmentInProgress, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(FirstRFICreated, ReadyFor4EYECheck,CURRENT_TIMESTAMP()
# MAGIC           ), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       )
# MAGIC   END AS DaysInAssessment1
# MAGIC  
# MAGIC --Number of days in Assessment 2 (when RFI exists and is completed)
# MAGIC --Days in Assessment2 is sum of days in assessment2 + rebound assessment
# MAGIC , (coalesce(t2.daysInReboundAssessment, 0) +
# MAGIC    CASE
# MAGIC      WHEN CreatedDateRFI IS NOT NULL AND FirstRFICompleted IS NOT NULL THEN
# MAGIC        DATEDIFF(DAY, FIRSTRFICompleted, COALESCE(ReadyFor4EYECheck, CURRENT_TIMESTAMP())) + 1
# MAGIC        - (
# MAGIC            (DATEDIFF(WEEK, FIRSTRFICompleted, COALESCE(ReadyFor4EYECheck, CURRENT_TIMESTAMP())) * 2)
# MAGIC            + (CASE WHEN DATE_FORMAT(FIRSTRFICompleted, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC            + (CASE WHEN DATE_FORMAT(COALESCE(ReadyFor4EYECheck, CURRENT_TIMESTAMP()), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC          )
# MAGIC      ELSE 0
# MAGIC    END
# MAGIC ) AS daysInAssessment2SLA
# MAGIC
# MAGIC , (
# MAGIC    CASE
# MAGIC      WHEN CreatedDateRFI IS NOT NULL AND FirstRFICompleted IS NOT NULL THEN
# MAGIC        DATEDIFF(DAY, FIRSTRFICompleted, COALESCE(ReadyFor4EYECheck, CURRENT_TIMESTAMP())) + 1
# MAGIC        - (
# MAGIC            (DATEDIFF(WEEK, FIRSTRFICompleted, COALESCE(ReadyFor4EYECheck, CURRENT_TIMESTAMP())) * 2)
# MAGIC            + (CASE WHEN DATE_FORMAT(FIRSTRFICompleted, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC            + (CASE WHEN DATE_FORMAT(COALESCE(ReadyFor4EYECheck, CURRENT_TIMESTAMP()), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC          )
# MAGIC      ELSE 0
# MAGIC    END
# MAGIC ) AS daysInAssessment2
# MAGIC  
# MAGIC -- Number of days in Client Outreach (from RFI creation to response)
# MAGIC , CASE
# MAGIC     WHEN CreatedDateRFI is not null  THEN
# MAGIC       DATEDIFF(DAY, FirstRFICreated,  COALESCE(FIRSTRFICompleted , CURRENT_TIMESTAMP())) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, FirstRFICreated, COALESCE(FIRSTRFICompleted , CURRENT_TIMESTAMP()))) * 2)
# MAGIC           --(DATEDIFF(WEEK,  t3.DateCreated,  COALESCE(t3.DateResponded, CURRENT_TIMESTAMP())) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(FirstRFICreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(FIRSTRFICompleted , CURRENT_TIMESTAMP()), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ELSE NULL
# MAGIC   END AS DaysInClientOutreach
# MAGIC  
# MAGIC  
# MAGIC , CASE
# MAGIC     WHEN CreatedDateRFI is not null  THEN
# MAGIC       DATEDIFF(DAY, FirstRFICreated,  FIRSTRFICompleted) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, FirstRFICreated, FIRSTRFICompleted)) * 2)
# MAGIC           --(DATEDIFF(WEEK,  t3.DateCreated,  COALESCE(t3.DateResponded, CURRENT_TIMESTAMP())) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(FirstRFICreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(FIRSTRFICompleted, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ELSE NULL
# MAGIC   END AS DaysInClientOutreachCompleted
# MAGIC  
# MAGIC  
# MAGIC , CASE
# MAGIC     WHEN --t3.CasePhase IN ('Rebound Client Outreach')
# MAGIC            FIRSTRFICompleted IS NOT NULL AND COALESCE(SecondRFICreatedDate, LatestRFICompleted) >  FIRSTRFICompleted THEN
# MAGIC       DATEDIFF(DAY,  COALESCE(SecondRFICreatedDate, FIRSTRFICompleted), COALESCE(CompletedDateFirstRFI, CURRENT_TIMESTAMP())) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, COALESCE(SecondRFICreatedDate, FIRSTRFICompleted), COALESCE(CompletedDateFirstRFI, CURRENT_TIMESTAMP())) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(SecondRFICreatedDate, FIRSTRFICompleted), 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(CompletedDateFirstRFI, CURRENT_TIMESTAMP()), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       )
# MAGIC    ELSE NULL
# MAGIC   END AS DaysInReboundClientOutreach
# MAGIC  
# MAGIC -- Below days are calulated using CDD planning app data coming from PowerApps
# MAGIC --, t6.DaysInClientOutreach + t7.DaysInReboundClientOutreach AS DaysInRFI_SLA --(Sum of daysInClientOutreach & daysInReboundClientOutreach)
# MAGIC , t6.DaysInClientOutreach AS DaysInRFI_SLA
# MAGIC , t8.DaysInMLRO_SLA
# MAGIC , t9.daysinsenttofosforoutreach_sla
# MAGIC  
# MAGIC FROM workitems_status_final_temp AS t1
# MAGIC LEFT JOIN daysInReboundAssessment AS t2 ON t1.sourceClient = t2.sourceClient
# MAGIC LEFT JOIN totalDaysInWorkItemStatus AS t3 ON t1.sourceClient = t3.sourceClient
# MAGIC LEFT JOIN daysInReboundInitiation AS t4 ON t1.sourceClient = t4.sourceClient
# MAGIC LEFT JOIN reboundAssessmentCounts AS t5 ON t1.sourceClient = t5.sourceClient
# MAGIC LEFT JOIN clientOutreachDuration_CddPlanningApp AS t6 ON t1.sourceClient = t6.sourceClient
# MAGIC LEFT JOIN reboundclientOutreachDuration_CddPlanningApp AS t7 ON t1.sourceClient = t7.sourceClient
# MAGIC LEFT JOIN MLRODuration_CddPlanningApp AS t8 ON t1.sourceClient = t8.sourceClient
# MAGIC LEFT JOIN sentToFosForOutreachDuration_CddPlanningApp AS t9 ON t1.sourceClient = t9.sourceClient

# COMMAND ----------

# DBTITLE 1,OLD LOGIC workitems_status_final for days in case phase
# MAGIC %sql
# MAGIC /* 
# MAGIC -----------------------------------------------------OLD LOGIC--------------------------------------------------------
# MAGIC CREATE OR REPLACE TEMPORARY VIEW workitems_status_daysInCurrentCasePhase AS
# MAGIC SELECT 
# MAGIC   t1.*
# MAGIC , t2.daysInReboundAssessment
# MAGIC , t3.daysIn_4_eye_check_in_progress as daysInQC
# MAGIC , t3.daysIn_Client_owner_sign_off_requested as daysInSignOff
# MAGIC --, t3.daysIn_KYC_assessment_in_progress as daysInKYCAssessmentInProgress
# MAGIC , t3.daysIn_Product_fulfilment_in_progress as daysInFulfillment
# MAGIC , t3.daysIn_Ready_for_4_eye_check as daysInReadyForQC
# MAGIC , t3.daysIn_Ready_for_KYC_assessment as daysInReadyForKYCAssessment
# MAGIC --, t3.daysIn_Initiation_In_Progress as daysInInitiation
# MAGIC --, t4.daysInReboundInitiation
# MAGIC , t5.reboundAssessmentCount as numberOfReboundAssessment
# MAGIC , t3.daysIn_Initiation_In_Progress + t4.daysInReboundInitiation AS daysInInitiation --(Sum of daysInInitiation & daysInReboundInitiation)
# MAGIC -- Days in Assessment 1 (first assessment, no RFI)
# MAGIC ,  CASE
# MAGIC     WHEN --MainCaseStatusType = 'Assessment'
# MAGIC          --AND t3.DateCreated IS NULL
# MAGIC          AssessmentInProgress IS NOT NULL THEN
# MAGIC          --AND t1.ReadyFor4EYECheck IS NULL THEN
# MAGIC       DATEDIFF(DAY, AssessmentInProgress, COALESCE(FirstRFICreated, ReadyFor4EYECheck,
# MAGIC         CURRENT_TIMESTAMP())) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, AssessmentInProgress, COALESCE(FirstRFICreated, ReadyFor4EYECheck,
# MAGIC             CURRENT_TIMESTAMP()
# MAGIC           )) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(AssessmentInProgress, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(FirstRFICreated, ReadyFor4EYECheck,CURRENT_TIMESTAMP()
# MAGIC           ), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       )
# MAGIC   END AS DaysInAssessment1
# MAGIC
# MAGIC   
# MAGIC   -- Number of days in Assessment 2 (when RFI exists and is completed)
# MAGIC , CASE
# MAGIC     WHEN CreatedDateRFI IS NOT NULL AND FirstRFICompleted IS NOT NULL THEN
# MAGIC       DATEDIFF(DAY, FIRSTRFICompleted, COALESCE(ReadyFor4EYECheck, CURRENT_TIMESTAMP())) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, FIRSTRFICompleted, COALESCE(ReadyFor4EYECheck, CURRENT_TIMESTAMP())) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(FIRSTRFICompleted, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(ReadyFor4EYECheck, CURRENT_TIMESTAMP()), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       )
# MAGIC     ELSE NULL
# MAGIC   END AS DaysInAssessment2
# MAGIC
# MAGIC
# MAGIC -- Number of days in Client Outreach (from RFI creation to response)
# MAGIC , CASE
# MAGIC     WHEN CreatedDateRFI is not null  THEN
# MAGIC       DATEDIFF(DAY, FirstRFICreated,  COALESCE(FIRSTRFICompleted , CURRENT_TIMESTAMP())) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, FirstRFICreated, COALESCE(FIRSTRFICompleted , CURRENT_TIMESTAMP()))) * 2)
# MAGIC           --(DATEDIFF(WEEK,  t3.DateCreated,  COALESCE(t3.DateResponded, CURRENT_TIMESTAMP())) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(FirstRFICreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(FIRSTRFICompleted , CURRENT_TIMESTAMP()), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ELSE NULL
# MAGIC   END AS DaysInClientOutreach
# MAGIC
# MAGIC
# MAGIC , CASE
# MAGIC     WHEN CreatedDateRFI is not null  THEN
# MAGIC       DATEDIFF(DAY, FirstRFICreated,  FIRSTRFICompleted) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, FirstRFICreated, FIRSTRFICompleted)) * 2)
# MAGIC           --(DATEDIFF(WEEK,  t3.DateCreated,  COALESCE(t3.DateResponded, CURRENT_TIMESTAMP())) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(FirstRFICreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(FIRSTRFICompleted, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ELSE NULL
# MAGIC   END AS DaysInClientOutreachCompleted
# MAGIC
# MAGIC , CASE
# MAGIC     WHEN --t3.CasePhase IN ('Rebound Client Outreach')
# MAGIC            FIRSTRFICompleted IS NOT NULL AND COALESCE(SecondRFICreatedDate, LatestRFICompleted) >  FIRSTRFICompleted THEN
# MAGIC       DATEDIFF(DAY,  COALESCE(SecondRFICreatedDate, FIRSTRFICompleted), COALESCE(CompletedDateFirstRFI, CURRENT_TIMESTAMP())) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, COALESCE(SecondRFICreatedDate, FIRSTRFICompleted), COALESCE(CompletedDateFirstRFI, CURRENT_TIMESTAMP())) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(SecondRFICreatedDate, FIRSTRFICompleted), 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(CompletedDateFirstRFI, CURRENT_TIMESTAMP()), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       )
# MAGIC    ELSE NULL
# MAGIC   END AS DaysInReboundClientOutreach
# MAGIC
# MAGIC   FROM workitems_status_final_temp as t1
# MAGIC   LEFT JOIN daysInReboundAssessment as t2 on t1.sourceClient = t2.sourceClient
# MAGIC   LEFT JOIN totalDaysInWorkItemStatus AS t3 on t1.sourceClient = t3.sourceClient
# MAGIC   LEFT JOIN daysInReboundInitiation AS t4 on t1.sourceClient = t4.sourceClient
# MAGIC   LEFT JOIN reboundAssessmentCounts as t5 on t1.sourceClient = t5.sourceClient
# MAGIC   */

# COMMAND ----------

# DBTITLE 1,workitems_status_final
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW workitems_status_final AS 
# MAGIC SELECT
# MAGIC *,
# MAGIC    CASE
# MAGIC     WHEN CasePhase = 'Assessment 1' THEN DaysInAssessment1 
# MAGIC     WHEN CasePhase = 'Assessment 2' THEN DaysInAssessment2 
# MAGIC    ELSE DaysInCurrentCasePhaseTemp
# MAGIC   END AS DaysInCurrentCasePhase 
# MAGIC FROM workitems_status_daysInCurrentCasePhase
# MAGIC

# COMMAND ----------

# DBTITLE 1,TC_CTE - to define completed on time!
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW TC_CTE AS
# MAGIC
# MAGIC /*
# MAGIC For all cases, logic is evaluated in this order and the first situation that applies is the 'CompletedOnTime' value for the entity.
# MAGIC
# MAGIC 1. HR next review dates are 14 months after sign-off.
# MAGIC 2. Report in the quarter of the next review date of a client (not earlier even if completed earlier).
# MAGIC 3. If case A is followed by case B with the same next-review-date, case B is out of scope for KCI.
# MAGIC 4. If a PR is followed by an EDR in time, only check if the EDR is completed in 90 days. If EDR is completed after NRD, the PR is not completed in time. Check if EDR is followed up timely by the renewed review calendar.
# MAGIC 5. No report on timely completion if review is followed by offboarding. If offboarding is late, it is registered as not timely completed in the quarter of its NRD.
# MAGIC 6. 'QC EDR' is used for PR, amendment, QC EDR, and to amend a risk/next review date. If review A is followed by QC EDR with a new NRD B, assume NRD of review A should be NRD of case B.
# MAGIC
# MAGIC EDR completed in time:
# MAGIC 1. EDR takes over 90 days --> 0
# MAGIC 2. EDR takes under 90 days --> 1
# MAGIC
# MAGIC   - EDR has 2 levels of timeliness: 1. completed within 90 days, 2. next review signed off before NRD.
# MAGIC   - Timely completion if the next case is completed before the NRD.
# MAGIC   - Select only relevant cases with correct dates.
# MAGIC   - HR can take 14 months logic, while NRD is based on 2
# MAGIC */
# MAGIC
# MAGIC WITH TC_CTE1 AS (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.SourceClient
# MAGIC     , t1.ReviewTypeName
# MAGIC     , t2.TotalCaseDuration
# MAGIC     , CASE 
# MAGIC         WHEN t1.ValidatedRiskLevel = 'High' THEN ADD_MONTHS(t1.NextReviewDate, 2)
# MAGIC         WHEN t1.ReviewTypeName = 'Event Driven Review' THEN DATE_ADD(t2.Prework, 90) -- set an NRD for an EDR
# MAGIC         ELSE t1.NextReviewDate
# MAGIC       END AS TC_NRD
# MAGIC     , CASE 
# MAGIC         WHEN t1.ReviewTypeName LIKE '%Offboarding%' THEN t2.Completed
# MAGIC         ELSE t1.FinalDecisionDate -- SignOffDate
# MAGIC       END AS SignOffDate_TC
# MAGIC     , CASE 
# MAGIC         WHEN t1.EdrReason IN ('Quality Control Review', 'First Line Monitoring Review') THEN 'QC EDR'
# MAGIC         WHEN t1.ReviewTypeName LIKE '%Offboarding%' THEN 'Offboarding'
# MAGIC         ELSE t1.ReviewTypeName
# MAGIC       END AS CaseReviewType -- same as Firebird in dml.WorkitemsStatus
# MAGIC
# MAGIC   FROM party_case_client_details t1
# MAGIC   LEFT JOIN workitems_status_final t2 ON t1.SourceClient = t2.SourceClient
# MAGIC   WHERE t1.ReviewTypeName IN ('Periodic Review', 'Initial On-Boarding', 'Event Driven Review', 'Product Offboarding', 'Product Offboarding (Resume)') -- 'QC EDR'
# MAGIC     AND t1.CaseStatusName = 'Completed' -- OR t1.SourceSystem = 'GCOB Legacy' -- 2nd is the same as completed in legacy
# MAGIC )
# MAGIC
# MAGIC /*
# MAGIC ReviewTypeName:
# MAGIC   - Fast Migration
# MAGIC   - Periodic Review
# MAGIC   - Change of Client Owner
# MAGIC   - Event Driven Review
# MAGIC   - Product Offboarding
# MAGIC   - Product Offboarding (Resume)
# MAGIC   - Amendment
# MAGIC   - Initial On-Boarding
# MAGIC   - Tailored Event Assessment
# MAGIC */
# MAGIC
# MAGIC /*
# MAGIC EdrReason:
# MAGIC   - Change in products and/or services
# MAGIC   - Correspondent relation does not process a transaction
# MAGIC   - There are indications that the customer may be involved in TF
# MAGIC   - There are doubts about the truthfulness or adequacy of (previously) obtained customer identification data
# MAGIC   - There are indications that the customer may be involved in a direct/indirect sanction risk
# MAGIC   - Changes in the legal status of the customer
# MAGIC   - (Potentially) suspicious transactions from post transaction monitoring
# MAGIC   - Changes in the customer's listed or regulated status
# MAGIC   - null
# MAGIC   - Relevant warrant received
# MAGIC   - Change in Ring Fence Status
# MAGIC   - True hits from transaction screening
# MAGIC   - There are indications that the customer may be involved in criminal activities related to financial crime
# MAGIC   - Reporting confirmation further to SAR report to the Financial Intelligence Unit.
# MAGIC   - Other
# MAGIC   - There are indications that the customer may be involved in ML
# MAGIC   - Material change in the customer’s related parties
# MAGIC   - Changes in the customer’s business activity and/or customers industry risk
# MAGIC   - Changes in the country risk level
# MAGIC   - Issued criminal seizure
# MAGIC   - Quality Control Review
# MAGIC   - Changes in high risk product (significant change)
# MAGIC   - True hits from customer name screening
# MAGIC   - Material changes in the customer's UBO
# MAGIC   - Material adverse news concerning the customer or related parties
# MAGIC   - Changes in the risk indicators
# MAGIC   - Identification of a (new) PEP
# MAGIC   - Change of customers address/incorporation to a non-low risk country
# MAGIC   - First Line Monitoring Review
# MAGIC */
# MAGIC
# MAGIC , TC_CTE2 AS (
# MAGIC     SELECT DISTINCT
# MAGIC       SourceClient
# MAGIC       , CaseReviewType
# MAGIC       , TC_NRD
# MAGIC       , SignOffDate_TC
# MAGIC       , TotalCaseDuration
# MAGIC       , LAG(TC_NRD, 1, NULL) OVER (PARTITION BY SourceClient ORDER BY SignOffDate_TC) AS FormerNRD -- point 3 above
# MAGIC     FROM TC_CTE1
# MAGIC   )
# MAGIC
# MAGIC -- point 6 and 3 above
# MAGIC , TC_CTE3 AS (
# MAGIC     SELECT DISTINCT
# MAGIC       SourceClient
# MAGIC       , SignOffDate_TC
# MAGIC       , LEAD(SignOffDate_TC, 1, NULL) OVER (PARTITION BY SourceClient ORDER BY SignOffDate_TC) AS NextSignOffDate
# MAGIC       , CaseReviewType
# MAGIC       , TC_NRD
# MAGIC       , TotalCaseDuration
# MAGIC     FROM TC_CTE2
# MAGIC     WHERE SourceClient NOT IN (
# MAGIC       SELECT
# MAGIC         SourceClient 
# MAGIC       FROM TC_CTE2
# MAGIC       WHERE TC_NRD = FormerNRD AND (CaseReviewType <> 'Offboarding')
# MAGIC     )
# MAGIC     AND CaseReviewType <> 'QC EDR'
# MAGIC   )
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC   SourceClient
# MAGIC   , SignOffDate_TC
# MAGIC   , NextSignOffDate
# MAGIC   -- , CaseReviewType
# MAGIC   , TC_NRD
# MAGIC   , CASE
# MAGIC       WHEN CaseReviewType = 'Event Driven Review' AND TotalCaseDuration <= 90 THEN 1
# MAGIC       WHEN CaseReviewType = 'Event Driven Review' AND TotalCaseDuration > 90 THEN 0 -- error in firebird that reported <= instead of >
# MAGIC       WHEN NextSignOffDate <= TC_NRD THEN 1
# MAGIC       WHEN NextSignOffDate > TC_NRD THEN 0
# MAGIC       WHEN TC_NRD < GETDATE() AND NextSignOffDate IS NULL THEN 0
# MAGIC       ELSE NULL
# MAGIC     END AS CompletedOnTime
# MAGIC FROM (SELECT * FROM TC_CTE3 WHERE CaseReviewType <> 'QC EDR')

# COMMAND ----------

# DBTITLE 1,firebird_master
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW firebird_master AS
# MAGIC
# MAGIC WITH onb_pl_cte ( -- onboarding pl cte to get the onboarding type at case level starting from UniqueGcobId, based on InsertedOnDate diff with Prework date
# MAGIC   SELECT
# MAGIC     t1.SourceClient
# MAGIC     , t3.UniqueGcobid
# MAGIC     , t2.Prework
# MAGIC     , t3.InsertedOnDate
# MAGIC     , t1.ReviewTypeName
# MAGIC     , t3.OnboardingType
# MAGIC     , ABS(datediff(t2.Prework, t3.InsertedOnDate)) AS diff_days
# MAGIC     , ROW_NUMBER() OVER (PARTITION BY t1.SourceClient ORDER BY ABS(datediff(t2.Prework, t3.InsertedOnDate))) AS rn
# MAGIC
# MAGIC   FROM party_case_client_details t1
# MAGIC   LEFT JOIN workitems_status_final t2 ON t1.SourceClient = t2.SourceClient AND t1.CaseId = t2.CaseId
# MAGIC   JOIN radar.onboardings t3 ON CASE
# MAGIC         WHEN t1.ClientType = 'Legal Entity' THEN t1.GcobId
# MAGIC         WHEN t1.ClientType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person') THEN CONCAT('NP_', t1.GcobId)
# MAGIC       END = t3.UniqueGcobId
# MAGIC   WHERE t1.ReviewTypeName = 'Initial On-Boarding'
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.SourceSystem
# MAGIC   , t1.ClientId
# MAGIC   , t1.CaseId
# MAGIC   , t1.GcobId
# MAGIC   , t1.FullLegalName
# MAGIC   , t1.TeaOtherReason
# MAGIC   , t1.TEAReviewReasonDescription
# MAGIC   , t1.ReviewTypeName
# MAGIC   , t1.CaseStatusName
# MAGIC   , t1.BusinessLineName
# MAGIC   , t1.GlobalClientOwner
# MAGIC   , t1.GlobalClientOwnerLocation
# MAGIC   , t1.GlobalClientOwnerOfficeLocation
# MAGIC   , t1.ClientLifeCycleName
# MAGIC   , t1.CddType
# MAGIC   , t1.EdrReason
# MAGIC   , t1.NextReviewDate
# MAGIC   , t1.RingFenced
# MAGIC   , t1.OwnerType
# MAGIC   , t1.ScheduledCompletionDate
# MAGIC   , t1.IsEligibleForFatcaAssessment
# MAGIC   , t1.FatcaClassification
# MAGIC   , t1.FatcaDateOfIssue
# MAGIC   , t1.IsEligibleForCrsAssessment
# MAGIC   , t1.CrsClassification
# MAGIC   , t1.CrsFormSignedDate
# MAGIC   , t1.FullLegalNameInLocalLanguage
# MAGIC   , t1.DateOfBirth
# MAGIC   , t1.RegisteredStreet
# MAGIC   , t1.RegisteredNumber
# MAGIC   , t1.RegisteredPostalCode
# MAGIC   , t1.RegisteredCity
# MAGIC   , t1.RegisteredRegion
# MAGIC   , t1.OperatingStreet
# MAGIC   , t1.OperatingNumber
# MAGIC   , t1.OperatingPostalCode
# MAGIC   , t1.OperatingCity
# MAGIC   , t1.OperatingRegion
# MAGIC   , t1.OffBoardingReason
# MAGIC   , t1.SalesforceClientID_nCino
# MAGIC   , t1.SubmitToClientCommittee
# MAGIC   , t1.CountryOfRegistration
# MAGIC   , t1.RegisteredCountryIsoCode
# MAGIC   , t1.OperatingCountryIsoCode
# MAGIC   , t1.CountryOfOperation
# MAGIC   , t1.IncorporationNumber
# MAGIC   , t1.IsIncorporated
# MAGIC   , t1.IncorporationDate
# MAGIC   , t1.IsClientListed
# MAGIC   , t1.IsClientRegulated
# MAGIC   , t1.FIHubIndicator
# MAGIC   , t1.HasTaxForm
# MAGIC   , t1.IsTaxIntegrityMaterial
# MAGIC   , t1.ValidatedRiskLevel
# MAGIC   , t1.ClientType
# MAGIC   , t1.IsOperatingAddressDifferentToRegisteredAddress
# MAGIC   , t1.Comments
# MAGIC   , t1.ClientApprovalDate
# MAGIC   , t1.AmendmentReason
# MAGIC   , t1.CaseDecisionMotivation
# MAGIC   , t1.ClientOwnerChangeReason
# MAGIC   , t1.EdrOtherReason
# MAGIC   , t1.ExecutiveSummary
# MAGIC   , t1.ReviewReason
# MAGIC   , t1.ReviewReasonOtherExplanation
# MAGIC   , t1.IsLatestApprovedVersionOfClient
# MAGIC   , t1.ClientCitizenship
# MAGIC   , t1.ClientNationality
# MAGIC   , t1.WWID
# MAGIC   , t1.ACBS
# MAGIC   , t1.RUTID
# MAGIC   , t1.ISB
# MAGIC   , t1.GCDSID
# MAGIC   , t1.NameOfExchange
# MAGIC   , t1.CountryOfExchange
# MAGIC   , t1.NameOfRegulator
# MAGIC   , t1.CountryOfRegulator
# MAGIC   , t1.4EyeCheckReviewer
# MAGIC   , t1.CurrentAssignee
# MAGIC   , t1.Last4EyeCheckReviewer
# MAGIC   , t1.LastKYCAnalyst
# MAGIC   , t1.LastSentFor4EyeCheck
# MAGIC   , t1.LastSentForKYCAssesment
# MAGIC   , t1.InitiationInProgressAssignee
# MAGIC   , t1.CaseCompletedDate
# MAGIC   , t1.CaseCreationDate
# MAGIC   , t1.DateSubmittedFor4EyeCheck
# MAGIC   , t1.DateSubmittedForSignOff
# MAGIC   , t1.KYCAssessmentInProgressAssignee
# MAGIC   , t1.ReadyForKYCAssessmentDate
# MAGIC   , t1.ClientOwnerSignOffDate
# MAGIC   , t1.FinalDecisionDate
# MAGIC   , t1.LastProductOffboardingAnalyst
# MAGIC   , t1.SourceClient
# MAGIC   , t1.RiskModelName
# MAGIC   , t1.ModelCalculatedRiskLevel
# MAGIC   , t1.ModelRecalculatedRiskLevel
# MAGIC   , t1.GeographicalRiskLevel
# MAGIC   , t1.EntityTypeRiskLevel
# MAGIC   , t1.StructureRiskLevel
# MAGIC   , t1.SectorRiskLevel
# MAGIC   , t1.ProductAndServiceRiskLevel
# MAGIC   , t1.PEPRiskLevel
# MAGIC   , t1.TransactionRiskLevel
# MAGIC   , t1.DistributionRiskLevel
# MAGIC   , t1.ThirdPartyRiskLevel
# MAGIC   , t1.AdverseInfoRiskLevel
# MAGIC   , t1.OtherRiskLevel
# MAGIC   , t1.GeographicalApplicableRisk
# MAGIC   , t1.EntityTypeApplicableRisk
# MAGIC   , t1.StructureApplicableRisk
# MAGIC   , t1.SectorApplicableRisk
# MAGIC   , t1.ProductsApplicableRisk
# MAGIC   , t1.PoliticallyExposedPersonsApplicableRisk
# MAGIC   , t1.TransactionApplicableRisk
# MAGIC   , t1.DistributionChannelApplicableRisk
# MAGIC   , t1.ThirdPartyApplicableRisk
# MAGIC   , t1.AdverseInfoApplicableRisk
# MAGIC   , t1.OtherApplicableRisk
# MAGIC   , t1.EDL_LOAD_DTS AS EDL_LoadDate
# MAGIC
# MAGIC
# MAGIC   , CASE WHEN t1.ClientId = t3.LastFullReviewId THEN 'Yes' ELSE 'No' END AS LatestFullReviewVersion
# MAGIC   , CASE WHEN t1.CaseStatusName NOT IN ('Cancelled', 'Completed', 'Migrated') THEN 'Yes' ELSE 'No' END AS ActiveCase
# MAGIC   , t4.CompletedOnTime
# MAGIC   , CASE
# MAGIC       WHEN t8.SourceClient IS NOT NULL THEN t8.OnboardingType
# MAGIC       WHEN t1.EdrReason IN ('Quality Control Review', 'First Line Monitoring Review') THEN 'QC EDR'
# MAGIC       WHEN t1.ReviewTypeName LIKE '%Offboarding%' THEN 'Offboarding'
# MAGIC       ELSE t1.ReviewTypeName
# MAGIC     END AS CaseReviewType
# MAGIC
# MAGIC   -- , CASE WHEN t1.ClientLifeCycleName <> 'FormerClient' THEN 0 ELSE 1 END AS DuplicateOffboarding -- wtf?
# MAGIC   , CASE 
# MAGIC       WHEN t1.ReviewTypeName LIKE '%Offboarding%'
# MAGIC         AND t1.CaseStatusName = 'Completed' 
# MAGIC         AND t1.ClientLifeCycleName <> 'FormerClient' 
# MAGIC       THEN 1 
# MAGIC       ELSE 0 
# MAGIC     END AS DuplicateOffboarding
# MAGIC
# MAGIC   , t2.Prework
# MAGIC   , weekofyear(t2.Prework) AS StartWeekPrework
# MAGIC   , t2.PreworkAnalyst
# MAGIC   , t2.PreworkAnalystTeamDate
# MAGIC   , t2.ReadyForAssessment
# MAGIC   , t2.AssessmentInProgress
# MAGIC   , t2.AssessmentAnalyst
# MAGIC   , t2.AssessmentAnalystTeamDate
# MAGIC   , t2.4EYECheck
# MAGIC   , t2.ReadyFor4EYECheck
# MAGIC   , t2.4EYEAnalyst
# MAGIC   , t2.4EYEAnalystTeamDate
# MAGIC   , t2.SignOff
# MAGIC   , t2.Fulfillment
# MAGIC   , t2.Completed
# MAGIC   -- , t2.Cancelled
# MAGIC   /*
# MAGIC   these fields are replaced with the fields below in the select clause
# MAGIC   , t2.DaysInPrework
# MAGIC   , t2.DaysInReadyForAssessment
# MAGIC   , t2.DaysInAssessmentInProgress
# MAGIC   , t2.DaysIn4EYECheck
# MAGIC   , t2.DaysInSignoff
# MAGIC   , t2.DaysInFulfillment
# MAGIC   */
# MAGIC   , t2.TotalCaseDuration
# MAGIC   -- , t2.QCInteractions
# MAGIC   -- , t2.LastAnalyst
# MAGIC   -- , t2.LastAnalystTeamDate
# MAGIC   -- , t2.PreworkAlias
# MAGIC   -- , t2.AssessmentAlias
# MAGIC   -- , t2.4EYEAlias
# MAGIC   -- , t2.LastAnalystAlias
# MAGIC   , t2.RespondedDateFirstRFI
# MAGIC   , t2.CompletedDateFirstRFI
# MAGIC   , t2.FirstRFICreated
# MAGIC   , t2.LatestRFICompleted
# MAGIC   , t2.CasePhase
# MAGIC   , t2.CasePhaseSortOrder
# MAGIC   -- , t2.CasePhase2023
# MAGIC   -- , t2.CasePhaseSortOrder2023
# MAGIC   -- , t2.CasePhase2024
# MAGIC   , t2.CasePhaseCurrentYear
# MAGIC   -- , t2.CasePhaseSortOrder2024
# MAGIC   , t2.CasePhaseSortCurrentYear
# MAGIC   , t2.DaysInCurrentCasePhase
# MAGIC   , t2.EDRoverdue
# MAGIC   -- , t2.SignoffDateTC
# MAGIC   -- , t2.TCNRD
# MAGIC   , t3.LatestId
# MAGIC   , t3.LastFullReviewId
# MAGIC   , t3.CompletedId
# MAGIC   , t3.LatestCaseId
# MAGIC   , t5.ProductName
# MAGIC   , t5.BookingEntityLocation
# MAGIC   , t5.ProductOfferingLocation
# MAGIC   , t5.ProductLifeCycleStatus
# MAGIC   , t3.LatestCompletedCaseId
# MAGIC   , t2.4EyeUserTeam
# MAGIC   , t2.4EyeDepartment
# MAGIC   , t2.PreworkDepartment
# MAGIC   , t2.KYCUserTeam
# MAGIC   , t2.KYCDepartment
# MAGIC   , t2.PreworkUserTeam
# MAGIC   , t2.CDDDepartment
# MAGIC   , t2.CDDUserTeam
# MAGIC   , t3.LastFullReviewCaseId
# MAGIC   , t3.PragmaticCaseId
# MAGIC   ----------------new fields added for days in casephase logic start---------------------------------------
# MAGIC   , t2.daysInReboundAssessment
# MAGIC   , t2.DaysInAssessment1
# MAGIC   , t2.DaysInAssessment2
# MAGIC   , t2.DaysInClientOutreach
# MAGIC   , t2.DaysInClientOutreachCompleted
# MAGIC   , t2.DaysInReboundClientOutreach
# MAGIC   , t2.daysInQC
# MAGIC   , t2.daysInSignOff
# MAGIC   , t2.daysInReadyForKYCAssessment
# MAGIC   , t2.daysInFulfillment
# MAGIC   , t2.daysInReadyForQC
# MAGIC   , t2.daysInPrework_Sla
# MAGIC   , t2.numberOfReboundAssessment
# MAGIC   , t2.DaysInMLRO_SLA
# MAGIC   , t2.DaysInRFI_SLA
# MAGIC   , t2.daysInAssessment2SLA
# MAGIC   , t2.daysinsenttofosforoutreach_sla
# MAGIC     ----------------new fields added for days in casephase logic end---------------------------------------
# MAGIC   , CASE
# MAGIC       WHEN t1.ClientType = 'Legal Entity' THEN t1.GcobId
# MAGIC       WHEN t1.ClientType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person') THEN CONCAT('NP_', t1.GcobId)
# MAGIC     END AS UniqueGcobId
# MAGIC
# MAGIC   , t7.LocalClientOwnerName
# MAGIC   , t7.Location AS LocalClientOwnerLocation
# MAGIC
# MAGIC FROM party_case_client_details t1
# MAGIC LEFT JOIN workitems_status_final t2 ON t1.SourceClient = t2.SourceClient AND t1.CaseId = t2.CaseId
# MAGIC LEFT JOIN party_client t3 ON t1.GcobId = t3.GcobId AND t1.ClientType = t3.ClientType
# MAGIC -- LEFT JOIN next_signoff_date t4 ON t1.SourceClient = t4.SourceClient
# MAGIC LEFT JOIN TC_CTE t4 ON t1.SourceClient = t4.SourceClient
# MAGIC LEFT JOIN party_products_and_services t5 ON t1.SourceClient = t5.SourceClient
# MAGIC LEFT JOIN party_local_client_Owners t7 ON t1.SourceClient = t7.SourceClient
# MAGIC
# MAGIC LEFT JOIN onb_pl_cte t8 ON t1.SourceClient = t8.SourceClient AND rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS radar

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.firebird_master

# COMMAND ----------

spark.sql('SELECT * FROM firebird_master').write.mode('overwrite').saveAsTable('radar.firebird_master')

# COMMAND ----------

# MAGIC %md
# MAGIC # Cases

# COMMAND ----------

# DBTITLE 1,Cases_temp - prior to adding all previous risk levels in the next cell
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Cases_temp AS -- prior to adding all previous risk levels in the next cell
# MAGIC
# MAGIC -- create UniqueGcobId
# MAGIC WITH party_case_client_details_uniquegcobid_cte AS (
# MAGIC   SELECT DISTINCT
# MAGIC     CASE
# MAGIC       WHEN ClientType = 'Legal Entity' then Gcobid
# MAGIC       WHEN ClientType IN ('Natural Person acting in a Professional Capacity (NPPC)', 'Natural Person') THEN concat('NP_', Gcobid)
# MAGIC     END AS UniqueGcobId
# MAGIC     -- , Gcobid
# MAGIC     , ClientType
# MAGIC     , SourceClient
# MAGIC     , ClientId
# MAGIC     , CaseId
# MAGIC     , CaseStatusName
# MAGIC     -- , SourceSystem
# MAGIC     , TO_DATE(FinalDecisionDate, "dd-MM-yyyy") AS FinalDecisionDate
# MAGIC     , TO_DATE(NextReviewDate, "dd-MM-yyyy") AS NextReviewDate
# MAGIC     , TO_DATE(ClientApprovalDate, "dd-MM-yyyy") AS ClientApprovalDate
# MAGIC   FROM party_case_client_details
# MAGIC   -- WHERE CaseStatusName <> 'Cancelled'
# MAGIC )
# MAGIC
# MAGIC , LatestCompletedFlag_cte AS (
# MAGIC   SELECT DISTINCT
# MAGIC     t2.SourceClient AS SourceClientLatestCompleted
# MAGIC   FROM radar.firebird_master t1
# MAGIC   INNER JOIN radar.firebird_master t2 ON t1.LatestCompletedCaseId = t2.CaseId AND t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC )
# MAGIC
# MAGIC , FilteredLag AS (
# MAGIC     SELECT 
# MAGIC       UniqueGcobId
# MAGIC       , ClientType
# MAGIC       , SourceClient
# MAGIC       , CaseId
# MAGIC       , ClientId
# MAGIC       , COALESCE(FinalDecisionDate, NextReviewDate, ClientApprovalDate) AS OrderDate
# MAGIC       , ROW_NUMBER() OVER (PARTITION BY UniqueGcobId ORDER BY COALESCE(FinalDecisionDate, NextReviewDate, ClientApprovalDate), CAST(CaseId AS INT)) AS RowNum
# MAGIC     FROM party_case_client_details_uniquegcobid_cte
# MAGIC     WHERE CaseStatusName <> 'Cancelled'
# MAGIC )
# MAGIC , PreviousCaseId_cte (
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , ClientType
# MAGIC       , SourceClient
# MAGIC       , CaseId
# MAGIC       , ClientId
# MAGIC       , LAG(CaseId, 1) OVER (PARTITION BY UniqueGcobId ORDER BY RowNum) AS PreviousCaseId
# MAGIC       , LAG(ClientId, 1) OVER (PARTITION BY UniqueGcobId ORDER BY RowNum) AS PreviousClientId
# MAGIC     FROM FilteredLag
# MAGIC )
# MAGIC
# MAGIC , LocalClientOwner_cte AS (
# MAGIC     SELECT DISTINCT
# MAGIC       SourceClient
# MAGIC       , concat_ws(', ', sort_array(collect_set(struct(LocalClientOwnerName))).LocalClientOwnerName) as LocalClientOwnerName
# MAGIC       , concat_ws(', ', sort_array(collect_set(struct(LocalClientOwnerLocation))).LocalClientOwnerLocation) as LocalClientOwnerLocation
# MAGIC     FROM firebird_master
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.SourceSystem
# MAGIC   , t1.ClientId
# MAGIC   , t1.SourceClient
# MAGIC   , CASE
# MAGIC       WHEN SUBSTR(t1.SourceClient,0,3) = 'LEC' AND t1.SourceSystem = 'GCOB' THEN 'GCOB_LegalEntity'
# MAGIC       WHEN SUBSTR(t1.SourceClient,0,2) = 'NP' AND t1.SourceSystem = 'GCOB' THEN 'GCOB_NP-NPPC'
# MAGIC       -- WHEN SUBSTR(t1.SourceClient,0,6) = 'L2_LEC' AND t1.SourceSystem = 'Legacy2' THEN 'Legacy2_LegalEntity'
# MAGIC       -- WHEN SUBSTR(t1.SourceClient,0,5) = 'L2_NP' AND t1.SourceSystem = 'Legacy2' THEN 'Legacy2_NP-NPPC'
# MAGIC     END AS SourceSystemReference
# MAGIC   , t1.CaseId
# MAGIC   , t1.GcobId
# MAGIC   , t1.FullLegalName
# MAGIC   , t1.TeaOtherReason
# MAGIC   , t1.TEAReviewReasonDescription
# MAGIC   -- , t1.ReviewTypeName
# MAGIC   , t1.CaseReviewType
# MAGIC   , t1.CaseStatusName
# MAGIC   , t1.BusinessLineName
# MAGIC   , t1.OwnerType
# MAGIC   , t1.GlobalClientOwner
# MAGIC   , t1.GlobalClientOwnerLocation
# MAGIC   , t1.GlobalClientOwnerOfficeLocation
# MAGIC   , t1.ClientLifeCycleName
# MAGIC   , t1.CddType
# MAGIC   , t1.EdrReason
# MAGIC   , TO_DATE(t1.NextReviewDate, "dd-MM-yyyy") AS NextReviewDate
# MAGIC   , t1.RingFenced
# MAGIC   , t1.ScheduledCompletionDate
# MAGIC   , t1.IsEligibleForFatcaAssessment
# MAGIC   , t1.FatcaClassification
# MAGIC   , t1.FatcaDateOfIssue
# MAGIC   , t1.IsEligibleForCrsAssessment
# MAGIC   , t1.CrsClassification
# MAGIC   , t1.CrsFormSignedDate
# MAGIC   , t1.FullLegalNameInLocalLanguage
# MAGIC   , t1.DateOfBirth
# MAGIC   , t1.RegisteredStreet
# MAGIC   , t1.RegisteredNumber
# MAGIC   , t1.RegisteredPostalCode
# MAGIC   , t1.RegisteredCity
# MAGIC   , t1.RegisteredRegion
# MAGIC   , t1.OperatingStreet
# MAGIC   , t1.OperatingNumber
# MAGIC   , t1.OperatingPostalCode
# MAGIC   , t1.OperatingCity
# MAGIC   , t1.OperatingRegion
# MAGIC   , t1.OffBoardingReason
# MAGIC   --, t1.SalesforceClientID_nCino
# MAGIC   , t1.SubmitToClientCommittee
# MAGIC   , t1.CountryOfRegistration
# MAGIC   , t1.RegisteredCountryIsoCode
# MAGIC   , t1.CountryOfOperation
# MAGIC   , t1.OperatingCountryIsoCode
# MAGIC   -- , t1.ContactPersonEmailAddress
# MAGIC   -- , t1.ContactPersonTelephoneNumber
# MAGIC   , t1.IncorporationNumber
# MAGIC   , t1.IsIncorporated
# MAGIC   , t1.IncorporationDate
# MAGIC   , t1.IsClientListed
# MAGIC   , t1.IsClientRegulated
# MAGIC   -- , t1.FIHubIndicator
# MAGIC   , CASE
# MAGIC       WHEN t1.FIHubIndicator = 1 THEN 'FI'
# MAGIC       WHEN t1.FIHubIndicator = 0 THEN 'Corp'
# MAGIC       ELSE NULL
# MAGIC     END AS FIHubIndicator_Derived
# MAGIC   , t1.HasTaxForm
# MAGIC   , t1.IsTaxIntegrityMaterial
# MAGIC   , t1.ValidatedRiskLevel
# MAGIC   , t1.ClientType
# MAGIC   , t1.IsLatestApprovedVersionOfClient
# MAGIC   -- , t1.ClientCitizenship
# MAGIC   , t1.ClientNationality
# MAGIC   , t1.WWID
# MAGIC   , t1.ACBS
# MAGIC   , t1.RUTID
# MAGIC   , t1.ISB
# MAGIC   , t1.GCDSID
# MAGIC   -- , t1.NameOfExchange
# MAGIC   -- , t1.CountryOfExchange
# MAGIC   -- , t1.NameOfRegulator
# MAGIC   -- , t1.CountryOfRegulator
# MAGIC   -- , t1.4EyeCheckReviewer
# MAGIC   -- , t1.CurrentAssignee
# MAGIC   -- , t1.Last4EyeCheckReviewer
# MAGIC   -- , t1.LastKYCAnalyst
# MAGIC   -- , t1.LastSentFor4EyeCheck
# MAGIC   -- , t1.LastSentForKYCAssesment
# MAGIC   -- , t1.InitiationInProgressAssignee
# MAGIC   , t1.CaseCompletedDate
# MAGIC   -- , t1.CaseCreationDate
# MAGIC   -- , t1.DateSubmittedFor4EyeCheck
# MAGIC   -- , t1.DateSubmittedForSignOff
# MAGIC   -- , t1.KYCAssessmentInProgressAssignee
# MAGIC   -- , t1.ReadyForKYCAssessmentDate
# MAGIC   -- , t1.ClientOwnerSignOffDate
# MAGIC   , t1.FinalDecisionDate
# MAGIC   , t1.RiskModelName
# MAGIC   , t1.ModelCalculatedRiskLevel
# MAGIC   , t1.ModelRecalculatedRiskLevel
# MAGIC   , t1.GeographicalRiskLevel
# MAGIC   , t1.EntityTypeRiskLevel
# MAGIC   , t1.StructureRiskLevel
# MAGIC   , t1.SectorRiskLevel
# MAGIC   , t1.ProductAndServiceRiskLevel
# MAGIC   , t1.PEPRiskLevel
# MAGIC   , t1.TransactionRiskLevel
# MAGIC   , t1.DistributionRiskLevel
# MAGIC   , t1.ThirdPartyRiskLevel
# MAGIC   , t1.AdverseInfoRiskLevel
# MAGIC   , t1.OtherRiskLevel
# MAGIC   , t1.GeographicalApplicableRisk
# MAGIC   , t1.EntityTypeApplicableRisk
# MAGIC   , t1.StructureApplicableRisk
# MAGIC   , t1.SectorApplicableRisk
# MAGIC   , t1.ProductsApplicableRisk
# MAGIC   , t1.PoliticallyExposedPersonsApplicableRisk AS PEPApplicableRisk
# MAGIC   , t1.TransactionApplicableRisk
# MAGIC   , t1.DistributionChannelApplicableRisk
# MAGIC   , t1.ThirdPartyApplicableRisk
# MAGIC   , t1.AdverseInfoApplicableRisk
# MAGIC   , t1.OtherApplicableRisk
# MAGIC   , t3.LocalClientOwnerName
# MAGIC   , t3.LocalClientOwnerLocation
# MAGIC   , t1.LatestFullReviewversion
# MAGIC   , t1.Activecase
# MAGIC   , t1.CompletedOnTime
# MAGIC   , t1.DuplicateOffboarding
# MAGIC   -- , t1.GCCKYCobligation
# MAGIC   , t1.Prework
# MAGIC   , t1.StartWeekPrework
# MAGIC   , t1.Preworkanalyst
# MAGIC   , t1.Preworkanalystteamdate
# MAGIC   , t1.Readyforassessment
# MAGIC   , t1.Assessmentinprogress
# MAGIC   , t1.Assessmentanalyst
# MAGIC   , t1.Assessmentanalystteamdate
# MAGIC   -- , t1.4EYECheck
# MAGIC   , t1.4EYEanalyst
# MAGIC   , t1.4EYEanalystteamdate
# MAGIC   , t1.Signoff
# MAGIC   , t1.Fulfillment
# MAGIC   , t1.Completed
# MAGIC   -- , t1.Cancelled
# MAGIC     /*
# MAGIC   these fields are replaced with the fields below in the select clause
# MAGIC   , t2.DaysInPrework
# MAGIC   , t2.DaysInReadyForAssessment
# MAGIC   , t2.DaysInAssessmentInProgress
# MAGIC   , t2.DaysIn4EYECheck
# MAGIC   , t2.DaysInSignoff
# MAGIC   , t2.DaysInFulfillment
# MAGIC   */
# MAGIC   , t1.TotalCaseDuration
# MAGIC   -- , t1.QCInteractions
# MAGIC   -- , t1.Lastanalyst
# MAGIC   -- , t1.Lastanalystteamdate
# MAGIC   -- , t1.PreworkAlias
# MAGIC   -- , t1.AssessmentAlias
# MAGIC   -- , t1.4EYEAlias
# MAGIC   -- , t1.LastAnalystAlias
# MAGIC   , t1.RespondeddatefirstRFI
# MAGIC   , t1.CompleteddatefirstRFI
# MAGIC   , t1.FirstRFIcreated
# MAGIC   , t1.LatestRFIcompleted
# MAGIC   , t1.Casephase
# MAGIC   , t1.Casephasesortorder
# MAGIC   -- , t1.Casephase2023
# MAGIC   -- , t1.Casephasesortorder2023
# MAGIC   -- , t1.Casephase2024
# MAGIC   , t1.CasePhaseCurrentYear
# MAGIC   -- , t1.Casephasesortorder2024
# MAGIC   , t1.CasePhaseSortCurrentYear
# MAGIC   , t1.Daysincurrentcasephase
# MAGIC   , t1.EDRoverdue
# MAGIC   -- , t1.TCNRD
# MAGIC   , t1.4EyeUserTeam
# MAGIC   , t1.4EyeDepartment
# MAGIC   , t1.PreworkDepartment
# MAGIC   , t1.KYCUserTeam
# MAGIC   , t1.KYCDepartment
# MAGIC   -- , t1.ConsultationRequired
# MAGIC   , t1.UniqueGcobId
# MAGIC   , t1.EDL_LoadDate
# MAGIC   , TO_DATE(t1.FinalDecisionDate, 'dd-MM-yyyy') AS SignOffDate
# MAGIC   , TO_DATE(t1.ClientApprovalDate, 'dd-MM-yyyy') AS ClientApprovalDate
# MAGIC   , t2.PreviousCaseId
# MAGIC   , t2.PreviousClientId
# MAGIC   , t1.ReviewReason
# MAGIC   , t1.ReviewReasonOtherExplanation
# MAGIC   , t1.PreworkUserTeam
# MAGIC   , t1.CDDDepartment
# MAGIC   , t1.CDDUserTeam
# MAGIC    ----------------new fields added for days in casephase logic start---------------------------------------
# MAGIC   , t1.daysInReboundAssessment
# MAGIC   , t1.DaysInAssessment1
# MAGIC   , t1.DaysInAssessment2
# MAGIC   , t1.DaysInClientOutreachCompleted
# MAGIC   , t1.daysInQC
# MAGIC   , t1.daysInSignOff
# MAGIC   , t1.daysInReadyForKYCAssessment
# MAGIC   , t1.daysInFulfillment
# MAGIC   , t1.daysInReadyForQC
# MAGIC   , t1.daysInPrework_Sla
# MAGIC   --, t1.daysInReboundInitiation
# MAGIC   , t1.numberOfReboundAssessment
# MAGIC   , t1.DaysInClientOutreach
# MAGIC   , t1.DaysInReboundClientOutreach
# MAGIC   , t1.DaysInMLRO_SLA
# MAGIC   , t1.DaysInRFI_SLA
# MAGIC   , t1.daysInAssessment2SLA
# MAGIC   , t1.daysinsenttofosforoutreach_sla
# MAGIC     ----------------new fields added for days in casephase logic end---------------------------------------
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN t4.SourceClientLatestCompleted IS NOT NULL THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS LatestCompletedFlag
# MAGIC
# MAGIC FROM radar.firebird_master t1
# MAGIC LEFT JOIN PreviousCaseId_cte t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN LocalClientOwner_cte t3 ON t1.SourceClient = t3.SourceClient
# MAGIC
# MAGIC LEFT JOIN LatestCompletedFlag_cte t4 on t1.SourceClient = t4.SourceClientLatestCompleted
# MAGIC -- WHERE CaseStatusName <> 'Cancelled'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.Cases_temp

# COMMAND ----------

spark.sql('SELECT * FROM Cases_temp').write.mode('overwrite').saveAsTable('radar.Cases_temp')

# COMMAND ----------

# DBTITLE 1,GlobalAndInvolvedFiles
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GlobalAndInvolvedFiles AS
# MAGIC
# MAGIC WITH LocationMappingRegion AS (
# MAGIC   SELECT * FROM VALUES
# MAGIC     ('Rabobank Netherlands', 'E&A'),
# MAGIC     ('Rabobank London', 'E&A'),
# MAGIC     ('Rabobank Antwerp', 'E&A'),
# MAGIC     ('Rabobank Dublin', 'E&A'),
# MAGIC     ('Rabobank Milan', 'E&A'),
# MAGIC     ('Rabobank Madrid', 'E&A'),
# MAGIC     ('Rabobank Paris', 'E&A'),
# MAGIC     ('Rabobank Frankfurt', 'E&A'),
# MAGIC     ('Rabobank Turkey', 'E&A'),
# MAGIC     ('Rabobank Kenya', 'E&A'),
# MAGIC
# MAGIC     ('Rabobank Hong Kong', 'Asia'),
# MAGIC     ('Rabobank HongKong', 'Asia'),
# MAGIC     ('Rabobank China', 'Asia'),
# MAGIC     ('Rabobank India', 'Asia'),
# MAGIC     ('Rabobank Singapore', 'Asia'),
# MAGIC     ('Rabobank Indonesia', 'Asia'),
# MAGIC
# MAGIC     ('Rabobank New York', 'North America'),
# MAGIC     ('Rabobank Canada', 'North America'),
# MAGIC     ('Rabobank Canada (RCBR)', 'North America'),
# MAGIC     ('Rabo Securities USA, Inc. (RSEC)', 'North America'),
# MAGIC     ('Rabo Securities Canada, Inc. (RSCI)', 'North America'),
# MAGIC     ('Rabobank - USA Rabo AgriFinance', 'North America'),
# MAGIC     ('Rabobank Canada(Rural)', 'North America'),
# MAGIC
# MAGIC     ('Rabobank Chile', 'South America'),
# MAGIC     ('Rabobank Brazil', 'South America'),
# MAGIC     ('Rabobank Argentina', 'South America'),
# MAGIC
# MAGIC     ('Rabobank New Zealand', 'RANZ'),
# MAGIC     ('Rabobank Australia', 'RANZ'),
# MAGIC     ('Rabobank - RANZ Country Banking and ROS', 'RANZ'),
# MAGIC
# MAGIC     ('Rabobank Foundation', 'Rabobank Foundation')
# MAGIC   AS LocationMappingRegion(Name, Region)
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SourceClient
# MAGIC   , t1.ClientType
# MAGIC   , t1.GcobId
# MAGIC   , t1.ClientId
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC         CASE
# MAGIC           WHEN t3.Region <> t4.Region
# MAGIC             OR t3.Region <> t5.Region
# MAGIC             AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress')
# MAGIC           THEN 1
# MAGIC           ELSE 0
# MAGIC         END
# MAGIC       ) > 0 THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS GlobalFiles
# MAGIC
# MAGIC   /*
# MAGIC     Location involved
# MAGIC   */
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Netherlands' OR t2.BookingEntityLocation = 'Rabobank Netherlands') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1 ELSE 0
# MAGIC   --   END AS NetherlandsInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Singapore' OR t2.BookingEntityLocation = 'Rabobank Singapore') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1 ELSE 0
# MAGIC   --   END AS SingaporeInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Hong Kong' OR t2.BookingEntityLocation = 'Rabobank Hong Kong') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1 ELSE 0
# MAGIC   -- END AS HongKongInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank India' OR t2.BookingEntityLocation = 'Rabobank India') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1 ELSE 0
# MAGIC   -- END AS IndiaInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank China' OR t2.BookingEntityLocation = 'Rabobank China') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1 ELSE 0
# MAGIC   -- END AS ChinaInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank London' OR t2.BookingEntityLocation = 'Rabobank London') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1ELSE 0
# MAGIC   --   END AS LondonInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank New Zealand' OR t2.BookingEntityLocation = 'Rabobank New Zealand') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1ELSE 0
# MAGIC   --   END AS NewZealandInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Australia' OR t2.BookingEntityLocation = 'Rabobank Australia') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1 ELSE 0
# MAGIC   --   END AS AustraliaInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank New York' OR t2.BookingEntityLocation = 'Rabobank New York') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1ELSE 0
# MAGIC   --   END AS NewYorkInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabo Securities USA, Inc. (RSEC)' OR t2.BookingEntityLocation = 'Rabo Securities USA, Inc. (RSEC)') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1ELSE 0
# MAGIC   --   END AS RaboSecuritiesUSA_RSEC_Involved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Canada (RCBR)' OR t2.BookingEntityLocation = 'Rabobank Canada (RCBR)') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1ELSE 0
# MAGIC   --   END AS CanadaInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Paris' OR t2.BookingEntityLocation = 'Rabobank Paris') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1ELSE 0
# MAGIC   --   END AS ParisInvolved
# MAGIC   -- , CASE
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Antwerp' OR t2.BookingEntityLocation = 'Rabobank Antwerp') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1ELSE 0
# MAGIC   --   END AS AntwerpInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Madrid' OR t2.BookingEntityLocation = 'Rabobank Madrid') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1ELSE 0
# MAGIC   --   END AS MadridInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Milan' OR t2.BookingEntityLocation = 'Rabobank Milan') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1ELSE 0
# MAGIC   --   END AS MilanInvolved
# MAGIC   -- , CASE 
# MAGIC   --     WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Frankfurt' OR t2.BookingEntityLocation = 'Rabobank Frankfurt') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC   --       THEN 1 ELSE 0 
# MAGIC   --     END) > 0 THEN 1ELSE 0
# MAGIC   --   END AS FrankfurtInvolved
# MAGIC
# MAGIC   /*
# MAGIC     Region involved
# MAGIC   */
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC         CASE 
# MAGIC           WHEN (t2.ProductOfferingLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Kenya') 
# MAGIC           OR t2.BookingEntityLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Kenya')) AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC           THEN 1 ELSE 0
# MAGIC         END
# MAGIC       ) > 0 
# MAGIC       THEN 1 ELSE 0
# MAGIC     END AS EAInvolved
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC         CASE 
# MAGIC           WHEN (t2.ProductOfferingLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') 
# MAGIC           OR t2.BookingEntityLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)')) AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC           THEN 1 ELSE 0 
# MAGIC         END
# MAGIC       ) > 0 
# MAGIC       THEN 1 ELSE 0
# MAGIC     END AS NAInvolved
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC           CASE 
# MAGIC             WHEN (t2.ProductOfferingLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') 
# MAGIC             OR t2.BookingEntityLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia'))
# MAGIC             AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC             THEN 1 ELSE 0 
# MAGIC           END
# MAGIC       ) > 0 
# MAGIC       THEN 1 ELSE 0
# MAGIC     END AS AsiaInvolved
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC           CASE 
# MAGIC             WHEN (t2.ProductOfferingLocation IN ('Rabobank Chile', 'Rabobank Brazil', 'Rabobank Argentina') 
# MAGIC             OR t2.BookingEntityLocation IN ('Rabobank Chile', 'Rabobank Brazil', 'Rabobank Argentina')) AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC             THEN 1 ELSE 0
# MAGIC           END
# MAGIC       ) > 0 
# MAGIC       THEN 1 ELSE 0
# MAGIC     END AS SAInvolved
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC         CASE 
# MAGIC           WHEN (t2.ProductOfferingLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') 
# MAGIC           OR t2.BookingEntityLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS')) AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC           THEN 1 ELSE 0
# MAGIC         END
# MAGIC       ) > 0 
# MAGIC       THEN 1 ELSE 0
# MAGIC     END AS RANZInvolved
# MAGIC
# MAGIC
# MAGIC FROM radar.Cases_temp t1
# MAGIC LEFT JOIN party_products_and_services t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN LocationMappingRegion t3 ON t1.GlobalClientOwnerLocation = t3.Name
# MAGIC LEFT JOIN LocationMappingRegion t4 ON t2.BookingEntityLocation = t4.Name
# MAGIC LEFT JOIN LocationMappingRegion t5 ON t2.ProductOfferingLocation = t5.Name
# MAGIC GROUP BY 1,2,3,4,5

# COMMAND ----------

# DBTITLE 1,Cases_final
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Cases_final AS -- this includes all previous risk levels
# MAGIC
# MAGIC WITH AsiaProductsInvolvementChange_cte AS (
# MAGIC   SELECT
# MAGIC     t1.SourceClient
# MAGIC     , CASE
# MAGIC         WHEN SUM(
# MAGIC           CASE 
# MAGIC             WHEN (t2.ProductOfferingLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') 
# MAGIC             OR t2.BookingEntityLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia'))
# MAGIC             AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC             THEN 1 ELSE 0
# MAGIC           END
# MAGIC         ) > 0 
# MAGIC         THEN 1 ELSE 0
# MAGIC       END AS AsiaInvolved
# MAGIC   FROM radar.Cases_temp t1
# MAGIC   LEFT JOIN party_products_and_services t2 ON t1.SourceClient = t2.SourceClient
# MAGIC   GROUP BY t1.SourceClient
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   t1.*
# MAGIC   , t2.ValidatedRiskLevel AS PreviousValidatedRiskLevel
# MAGIC   , t2.GeographicalRiskLevel AS PreviousGeographicalRiskLevel
# MAGIC   , t2.EntityTypeRiskLevel AS PreviousEntityTypeRiskLevel
# MAGIC   , t2.StructureRiskLevel AS PreviousStructureRiskLevel
# MAGIC   , t2.SectorRiskLevel AS PreviousSectorRiskLevel
# MAGIC   , t2.ProductAndServiceRiskLevel AS PreviousProductAndServiceRiskLevel
# MAGIC   , t2.PEPRiskLevel AS PreviousPEPRiskLevel
# MAGIC   , t2.TransactionRiskLevel AS PreviousTransactionRiskLevel
# MAGIC   , t2.DistributionRiskLevel AS PreviousDistributionRiskLevel
# MAGIC   , t2.ThirdPartyRiskLevel AS PreviousThirdPartyRiskLevel
# MAGIC   , t2.AdverseInfoRiskLevel AS PreviousAdverseInfoRiskLevel
# MAGIC   , t2.OtherRiskLevel AS PreviousOtherRiskLevel
# MAGIC   , t2.FatcaClassification AS PreviousFatcaClassification
# MAGIC   , t2.CrsClassification AS PreviousCrsClassification
# MAGIC   , t2.NextReviewDate AS PreviousNextReviewDate
# MAGIC   , CASE
# MAGIC       WHEN t2.NextReviewDate IS NULL OR t1.Completed IS NULL THEN 'N/A'
# MAGIC       WHEN t2.NextReviewDate < t1.Completed THEN 'No'
# MAGIC       ELSE 'Yes'
# MAGIC     END AS PRCompletedOnTime
# MAGIC   , CASE
# MAGIC       WHEN (
# MAGIC         (t1.GeographicalRiskLevel       = t2.GeographicalRiskLevel) AND
# MAGIC         (t1.EntityTypeRiskLevel         = t2.EntityTypeRiskLevel) AND
# MAGIC         (t1.SectorRiskLevel 			      = t2.SectorRiskLevel) AND
# MAGIC         (t1.ProductAndServiceRiskLevel  = t2.ProductAndServiceRiskLevel) AND
# MAGIC         (t1.StructureRiskLevel 			    = t2.StructureRiskLevel) AND
# MAGIC         (t1.TransactionRiskLevel			  = t2.TransactionRiskLevel) AND
# MAGIC         (t1.DistributionRiskLevel 	    = t2.DistributionRiskLevel) AND
# MAGIC         (t1.ThirdPartyRiskLevel 			  = t2.ThirdPartyRiskLevel) AND
# MAGIC         (t1.AdverseInfoRiskLevel			  = t2.AdverseInfoRiskLevel) AND
# MAGIC         (t1.PEPRiskLevel					      = t2.PEPRiskLevel) AND
# MAGIC         (t1.ValidatedRiskLevel  				= t2.ValidatedRiskLevel)
# MAGIC       ) THEN 0
# MAGIC       ELSE 1
# MAGIC     END AS CaseHasChangedRisk
# MAGIC   , CASE
# MAGIC       WHEN t1.CaseStatusName = 'Completed' -- when asia client and case is completed
# MAGIC       THEN
# MAGIC         CASE
# MAGIC           WHEN t3.AsiaInvolved = 0 AND t4.AsiaInvolved = 1 THEN -1
# MAGIC           WHEN t3.AsiaInvolved = 1 AND t4.AsiaInvolved = 0 THEN 1
# MAGIC           WHEN t3.AsiaInvolved = t4.AsiaInvolved THEN 0
# MAGIC         END
# MAGIC       ELSE NULL
# MAGIC     END AS AsiaProductsInvolvementChange
# MAGIC
# MAGIC     , CASE
# MAGIC         WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t5.AsiaInvolved = 1 THEN 'Lead - Products Involved'
# MAGIC         WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t5.AsiaInvolved = 0 THEN 'Lead - No Products'
# MAGIC         WHEN t1.GlobalClientOwnerLocation NOT IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t5.AsiaInvolved = 1 THEN 'Non-Lead - Products Involved'
# MAGIC         WHEN t1.GlobalClientOwnerLocation NOT IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t5.AsiaInvolved = 0 THEN 'Non-Lead - No Products'
# MAGIC       END AS AsiaProductsInvolvmentType
# MAGIC
# MAGIC FROM radar.Cases_temp t1
# MAGIC LEFT JOIN radar.Cases_temp t2 ON t1.PreviousClientId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC LEFT JOIN AsiaProductsInvolvementChange_cte t3 ON t1.SourceClient = t3.SourceClient
# MAGIC LEFT JOIN AsiaProductsInvolvementChange_cte t4 ON t2.SourceClient = t4.SourceClient
# MAGIC
# MAGIC LEFT JOIN GlobalAndInvolvedFiles t5 ON t1.SourceClient = t5.SourceClient

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.Cases

# COMMAND ----------

spark.sql('SELECT * FROM Cases_final').write.mode('overwrite').saveAsTable('radar.Cases')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if SourceClient duplication
duplicated_sourceclient = spark.sql('''
    SELECT SourceClient 
    FROM radar.cases 
    GROUP BY SourceClient 
    HAVING count(*) > 1
''')

# check for duplicationload_date
if not duplicated_sourceclient.isEmpty():
    raise Exception('Duplicated SourceClient in cases. Job stopped.')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the month!
from pyspark.sql.functions import lit, to_date

# if it is the first of the month, change the EDL_LoadDate column and append it to the historical dataset

if datetime.strptime(EDL_LoadDate, '%Y-%m-%d').date().day == 1:
    
    # check if the date already exists in the historical table
    check_date = (spark.table("radar.cases_historical").filter(f"EDL_LoadDate = '{EDL_LoadDate}'"))

    # append to historical dataset only if EDL_LoadDate does not already exist
    if check_date.count() == 0:
        spark.table("radar.cases").withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.cases_historical')

# COMMAND ----------

# MAGIC %md
# MAGIC # Clients

# COMMAND ----------

# DBTITLE 1,Client_TradeName
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Client_TradeName AS
# MAGIC SELECT DISTINCT 
# MAGIC   Id
# MAGIC   , concat_ws(', ', sort_array(collect_set(struct(ClientTradeName))).ClientTradeName) as TradeName
# MAGIC
# MAGIC FROM party_trade_name
# MAGIC WHERE ResultType = 'LegalEntityClient'
# MAGIC GROUP BY Id

# COMMAND ----------

# DBTITLE 1,Clients
# MAGIC %sql
# MAGIC /*
# MAGIC 	Per client the active case is used to determine the client information. First a selection of all the unique GcobId's is combined with the relevant case id. All case information is than left joined to this list of GcobId and Case ID.
# MAGIC 	[Number of ISB's]: All ISB's relevant to the client are filled in the field ISB, using comma as a separator. This function counts the amount of comma's in the cell and adds 1. 
# MAGIC 	[Overdue]: IIF function. IF the next review date is before the timestamp the query ran, the client is overdue. 
# MAGIC 	[Days overdue]: Datediff function 
# MAGIC 	[Regulatory overdue]: IIF function. If the risk level is 'High' and the next review date is more than 2 months before the timestamp the query ran, the client is regulatory overdue. 
# MAGIC 	[Days regulatory overdue]: 
# MAGIC 	[Regulatory overdue EDR]: IIF function. If date of prework is more than 90 days before the timestamp the query ran and the case review type is 'Event Driven Review' and the case is not complete yet, the client is reg. overdue EDR. 
# MAGIC 	[Active case]: IIF function. If the client has an active case, this will return Yes, else No. 
# MAGIC 	[GCC KYC Obligation]: IIF and Left function. If left 3 letters of Businessline equal 'GCC', then Yes. Else no
# MAGIC   [GCID] best fitting GCID.
# MAGIC */
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Clients AS
# MAGIC
# MAGIC WITH group_cte AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     LOWER(t3.KYCGroup) AS KYCGroup
# MAGIC     , SUM(CASE WHEN t2.CDDtype = 'Listed Corporate' THEN 1 ELSE 0 END) AS ListedCorporatesInGroup
# MAGIC     , COUNT(DISTINCT t2.UniqueGcobId) AS TotalEntitiesInGroup -- counting DISTINCT UniqueGcobId because ProductName leads to duplications
# MAGIC   FROM radar.firebird_master t1
# MAGIC   INNER JOIN radar.firebird_master t2 ON t1.LatestId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId -- max(CaseId) not cancelled
# MAGIC   LEFT JOIN PortfolioPlanning t3 ON t2.UniqueGcobId = t3.UniqueGcobId
# MAGIC   WHERE t3.KYCGroup IS NOT NULL
# MAGIC   GROUP BY LOWER(t3.KYCGroup)
# MAGIC )
# MAGIC
# MAGIC -- -- CTE to select the NRD of the latest completed NRD-type case
# MAGIC -- , nrd_cte AS (
# MAGIC --   WITH RankedCases AS (
# MAGIC --     SELECT
# MAGIC --       UniqueGcobId
# MAGIC --       , NextReviewDate
# MAGIC --       , FinalDecisionDate
# MAGIC --       , ROW_NUMBER() OVER (PARTITION BY UniqueGcobId ORDER BY CAST(CaseId AS INT) DESC) AS rn
# MAGIC --     FROM radar.cases
# MAGIC --     WHERE CaseReviewType IN ('Event Driven Review', 'Periodic Review', 'Onboarding', 'Initial On-boarding')
# MAGIC --       AND CaseStatusName = 'Completed'
# MAGIC --   )
# MAGIC --   SELECT
# MAGIC --     UniqueGcobId
# MAGIC --     , NextReviewDate
# MAGIC --     , FinalDecisionDate
# MAGIC --   FROM RankedCases
# MAGIC --   WHERE rn = 1
# MAGIC -- )
# MAGIC
# MAGIC , ReviewLocation_cte AS ( -- made a cte out of this because ReviewLocation is needed for FOSTeam
# MAGIC     SELECT
# MAGIC       t2.UniqueGcobId
# MAGIC       , CASE
# MAGIC         -- COB Greater China
# MAGIC           WHEN LOWER(t3.SectorTeam) = 'int. desk' AND LOWER(t2.GlobalClientOwnerLocation) = 'rabobank china' THEN 'COB Greater China'
# MAGIC           WHEN LOWER(t3.SectorTeam) IN ('pf corp', 'tcf corp', 'core lending') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank hong kong', 'rabobank china') THEN 'COB Greater China'
# MAGIC           WHEN LOWER(t3.SectorTeam) IN ('tcf corp', 'core lending') AND LOWER(t2.GlobalClientOwnerLocation) = 'rabobank singapore' THEN 'COB SG'
# MAGIC
# MAGIC         -- KYC SC
# MAGIC           WHEN LOWER(t2.GlobalClientOwnerLocation) = 'rabobank antwerp' AND LOWER(t3.SectorTeam) = 'core lending' THEN 'KYC SC'
# MAGIC
# MAGIC         -- KYC UK
# MAGIC           WHEN LOWER(t3.SectorTeam) = 'tcf corp' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank kenya', 'rabobank netherlands') THEN 'KYC UK'
# MAGIC     
# MAGIC         -- Corp CDD Hub
# MAGIC           WHEN LOWER(t3.SectorTeam) IN ('int. desk', 'core lending') AND LOWER(t2.GlobalClientOwnerLocation) = 'rabobank london' THEN 'Corp CDD Hub'
# MAGIC           WHEN LOWER(t3.SectorTeam) = 'sponsor coverage' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank london', 'rabobank netherlands', 'rabobank antwerp') THEN 'Corp CDD Hub'
# MAGIC           WHEN LOWER(t3.SectorTeam) = 'vcf' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank london') THEN 'Corp CDD Hub'
# MAGIC           WHEN LOWER(t2.GlobalClientOwner) IN ('willmott, a (adam)', 'oord van, ja (marco)', 'erkamp, m (maarten)') THEN 'Corp CDD Hub'
# MAGIC           
# MAGIC         -- Foundation    
# MAGIC           WHEN LOWER(t3.SectorTeam) = 'foundation' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank foundation', 'rabobank netherlands') THEN 'Foundation'
# MAGIC
# MAGIC         -- Global FI Hub
# MAGIC           WHEN LOWER(t3.SectorTeam) = 'agency' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank netherlands', 'rabobank hong kong') THEN 'Global FI Hub'
# MAGIC           WHEN LOWER(t3.SectorTeam) IN ('correspondent banking', 'fig', 'fig - orphan desk') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank netherlands') THEN 'Global FI Hub'
# MAGIC           WHEN LOWER(t3.SectorTeam) IN ('markets') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank australia', 'rabobank hong kong', 'rabobank london', 'rabobank netherlands', 'rabobank new zealand') THEN 'Global FI Hub'
# MAGIC           WHEN LOWER(t3.SectorTeam) = 'subsidiaries' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank london', 'rabobank netherlands') THEN 'Global FI Hub'
# MAGIC           WHEN LOWER(t3.SectorTeam) = 'tcf fi' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank singapore', 'rabobank netherlands') THEN 'Global FI Hub'
# MAGIC           WHEN LOWER(t3.SectorTeam) = 'treasury' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank australia', 'rabobank china', 'rabobank hong kong', 'rabobank new zealand') THEN 'Global FI Hub'
# MAGIC
# MAGIC         -- KYC EU Hub
# MAGIC           WHEN LOWER(t3.SectorTeam) = 'core lending' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank dublin', 'rabobank frankfurt', 'rabobank madrid', 'rabobank milan', 'rabobank paris') THEN 'KYC EU Hub'
# MAGIC           WHEN LOWER(t3.SectorTeam) IN ('ef corp') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank netherlands') THEN 'KYC EU Hub'
# MAGIC           WHEN LOWER(t3.SectorTeam) = 'int. desk' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank frankfurt') AND LOWER(t2.GlobalClientOwnerLocation) = 'meurichy de, k (koen)' THEN 'KYC EU Hub'
# MAGIC           WHEN LOWER(t3.SectorTeam) IN ('tcf corp') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank turkey') THEN 'KYC EU Hub'
# MAGIC     
# MAGIC         -- NY COBS
# MAGIC           WHEN LOWER(t3.SectorTeam) IN ('epp', 'fas', 'frr', 'markets', 'markets - fig', 'pf corp', 'sponsor coverage', 'tcf corp', 'treasury', 'vcf', 'core lending') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabo securities usa, inc. (rsec)', 'rabobank new york', 'rabobank canada (rcbr)', 'rabo securities usa, inc. (rsec), rabobank new york') THEN 'NY COBS'
# MAGIC
# MAGIC         -- Rural
# MAGIC           WHEN LOWER(t3.SectorTeam) IN ('rural') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank - usa rabo agrifinance', 'rabobank - ranz country banking and ros', 'rabobank chile', 'rabobank new york', 'rabobank new zealand') THEN 'Rural'
# MAGIC
# MAGIC         -- Wholesale KYC and Onboarding
# MAGIC           WHEN LOWER(t3.SectorTeam) IN ('int. desk', 'vcf', 'core lending') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank australia') THEN 'Wholesale KYC and Onboarding'
# MAGIC
# MAGIC         --GCO Rabobank Argentina
# MAGIC           WHEN LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank argentina') THEN NULL
# MAGIC
# MAGIC         -- Onboarding pipeline
# MAGIC           -- WHEN t3.ReviewLocation = '' THEN COALESCE(ONB.Location, '')
# MAGIC           ELSE t3.ReviewLocation
# MAGIC         END AS ReviewLocation
# MAGIC
# MAGIC     FROM radar.firebird_master t1
# MAGIC     INNER JOIN radar.firebird_master t2 ON t1.LatestId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId -- max(CaseId) not cancelled
# MAGIC     LEFT JOIN PortfolioPlanning t3 ON t2.UniqueGcobId = t3.UniqueGcobId
# MAGIC )
# MAGIC
# MAGIC , onboarding_cte AS (
# MAGIC   SELECT
# MAGIC     MAX(ClientApprovalDate) AS OnboardingDate
# MAGIC     , UniqueGcobId
# MAGIC   FROM radar.cases
# MAGIC   WHERE CaseReviewType = 'Initial On-Boarding'
# MAGIC   GROUP BY UniqueGcobId
# MAGIC )
# MAGIC
# MAGIC , N2KLocationsList_cte AS (
# MAGIC   SELECT
# MAGIC     t2.UniqueGcobId
# MAGIC     , CONCAT_WS(', ', ARRAY_DISTINCT(ARRAY_AGG(
# MAGIC         CASE
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabo Securities USA, Inc. (RSEC)' OR t2.BookingEntityLocation = 'Rabo Securities USA, Inc. (RSEC)') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabo Securities USA, Inc. (RSEC)'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank - RANZ Country Banking and ROS' OR t2.BookingEntityLocation = 'Rabobank - RANZ Country Banking and ROS') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank - RANZ Country Banking and ROS'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank - Smallholder Agroforestry Finance (SAF)' OR t2.BookingEntityLocation = 'Rabobank - Smallholder Agroforestry Finance (SAF)') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank - Smallholder Agroforestry Finance (SAF)'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank - USA Rabo AgriFinance' OR t2.BookingEntityLocation = 'Rabobank - USA Rabo AgriFinance') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank - USA Rabo AgriFinance'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Antwerp' OR t2.BookingEntityLocation = 'Rabobank Antwerp') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Antwerp'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Argentina' OR t2.BookingEntityLocation = 'Rabobank Argentina') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Argentina'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Australia' OR t2.BookingEntityLocation = 'Rabobank Australia') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Australia'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Brazil' OR t2.BookingEntityLocation = 'Rabobank Brazil') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Brazil'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Canada (RCBR)' OR t2.BookingEntityLocation = 'Rabobank Canada (RCBR)') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Canada (RCBR)'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Canada(Rural)' OR t2.BookingEntityLocation = 'Rabobank Canada(Rural)') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Canada(Rural)'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Chile' OR t2.BookingEntityLocation = 'Rabobank Chile') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Chile'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank China' OR t2.BookingEntityLocation = 'Rabobank China') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank China'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Dublin' OR t2.BookingEntityLocation = 'Rabobank Dublin') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Dublin'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Foundation' OR t2.BookingEntityLocation = 'Rabobank Foundation') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Foundation'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Frankfurt' OR t2.BookingEntityLocation = 'Rabobank Frankfurt') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Frankfurt'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Hong Kong' OR t2.BookingEntityLocation = 'Rabobank Hong Kong') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Hong Kong'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank India' OR t2.BookingEntityLocation = 'Rabobank India') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank India'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Indonesia' OR t2.BookingEntityLocation = 'Rabobank Indonesia') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Indonesia'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Kenya' OR t2.BookingEntityLocation = 'Rabobank Kenya') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Kenya'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank London' OR t2.BookingEntityLocation = 'Rabobank London') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank London'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Madrid' OR t2.BookingEntityLocation = 'Rabobank Madrid') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Madrid'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Malaysia' OR t2.BookingEntityLocation = 'Rabobank Malaysia') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Malaysia'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Milan' OR t2.BookingEntityLocation = 'Rabobank Milan') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Milan'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Netherlands' OR t2.BookingEntityLocation = 'Rabobank Netherlands') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Netherlands'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank New York' OR t2.BookingEntityLocation = 'Rabobank New York') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank New York'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank New Zealand' OR t2.BookingEntityLocation = 'Rabobank New Zealand') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank New Zealand'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Paris' OR t2.BookingEntityLocation = 'Rabobank Paris') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Paris'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Singapore' OR t2.BookingEntityLocation = 'Rabobank Singapore') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Singapore'
# MAGIC           WHEN (t2.ProductOfferingLocation = 'Rabobank Turkey' OR t2.BookingEntityLocation = 'Rabobank Turkey') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Turkey'
# MAGIC         END))) AS N2KLocationsList
# MAGIC   FROM radar.firebird_master t1
# MAGIC   INNER JOIN radar.firebird_master t2 ON t1.LatestId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t2.ClientId
# MAGIC   , t2.SourceClient
# MAGIC   , CASE
# MAGIC       WHEN SUBSTR(t2.SourceClient,0,3) = 'LEC' AND t2.SourceSystem = 'GCOB' THEN 'GCOB_LegalEntity'
# MAGIC       WHEN SUBSTR(t2.SourceClient,0,2) = 'NP' AND t2.SourceSystem = 'GCOB' THEN 'GCOB_NP-NPPC'
# MAGIC       -- WHEN SUBSTR(t2.SourceClient,0,6) = 'L2_LEC' AND t2.SourceSystem = 'Legacy2' THEN 'Legacy2_LegalEntity'
# MAGIC       -- WHEN SUBSTR(t2.SourceClient,0,5) = 'L2_NP' AND t2.SourceSystem = 'Legacy2' THEN 'Legacy2_NP-NPPC'
# MAGIC     END AS SourceSystemReference
# MAGIC   , t2.SourceSystem
# MAGIC   , t2.CaseStatusName
# MAGIC   , t2.GcobId
# MAGIC   , t2.LatestCompletedCaseId -- AS ApprovedCaseId
# MAGIC   , t2.LatestCaseId
# MAGIC   , t2.LastFullReviewId
# MAGIC   , t2.CompletedId
# MAGIC   , t2.LatestId
# MAGIC   , t2.ClientType
# MAGIC   , t2.UniqueGcobId
# MAGIC   , t2.FullLegalName
# MAGIC   , t2.GlobalClientOwner
# MAGIC   , t2.GlobalClientOwnerLocation
# MAGIC   , t2.RiskModelName
# MAGIC   , t2.BusinessLineName
# MAGIC   , t2.CddType
# MAGIC   , CASE WHEN t2.CddType = 'Listed Corporate' THEN 1 ELSE 0 END AS ListedCorporation
# MAGIC   -- , t2.FIHubIndicator
# MAGIC   , CASE WHEN t2.FIHubIndicator = 1 THEN 'FI' when t2.FIHubIndicator = 0 THEN 'Corp' ELSE NULL END AS FIHubIndicator_Derived
# MAGIC   , t2.FatcaDateOfIssue
# MAGIC   , t2.CrsFormSignedDate
# MAGIC   , CASE
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Kenya') THEN 'E&A'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') THEN 'Asia'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') THEN 'North America'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Chile', 'Rabobank Brazil', 'Rabobank Argentina') THEN 'South America'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') THEN 'RANZ'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Foundation') THEN 'Rabobank Foundation'
# MAGIC     END AS GlobalReportingRegion
# MAGIC   , t2.CountryOfOperation
# MAGIC   , t2.ClientLifeCycleName
# MAGIC   , t2.ISB
# MAGIC   , TO_DATE(t13.NextReviewDate, 'dd-MM-yyyy') AS NextReviewDate -- used to be t2.NextReviewDate
# MAGIC   -- , t2.Signoff
# MAGIC   , CASE 
# MAGIC       WHEN t13.NextReviewDate < CAST(CURRENT_DATE() AS DATE) THEN 'Yes'
# MAGIC       WHEN t13.NextReviewDate < ADD_MONTHS(CURRENT_DATE(), 1) THEN 'Next month'
# MAGIC       ELSE 'No'
# MAGIC     END AS Overdue
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t13.NextReviewDate < CAST(CURRENT_DATE() AS DATE) THEN 2
# MAGIC       WHEN t13.NextReviewDate < ADD_MONTHS(CURRENT_DATE(), 1) THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS OverdueCategoryNr
# MAGIC   , CASE
# MAGIC       WHEN t13.ValidatedRiskLevel = 'High' AND ADD_MONTHS(t13.NextReviewDate, 2) < CAST(CURRENT_DATE() AS DATE) THEN 'Yes'
# MAGIC       ELSE 'No'
# MAGIC     END AS RegulatoryOverdue
# MAGIC   -- , CASE
# MAGIC   --     WHEN (t13.ValidatedRiskLevel = 'High') AND (ADD_MONTHS(t13.NextReviewDate, 2) < CAST(CURRENT_DATE() AS DATE)) THEN 'Yes'
# MAGIC   --     WHEN (t13.ValidatedRiskLevel <> 'High') AND (t13.NextReviewDate < CAST(CURRENT_DATE() AS DATE)) THEN 'Yes'
# MAGIC   --     ELSE 'No'
# MAGIC   --   END AS ExternalReportingOverdue
# MAGIC   , CASE
# MAGIC       WHEN t2.ReviewTypeName = 'Event Driven Review' THEN TO_DATE(ADD_MONTHS(t2.Prework, 3), 'yyyy-MM-dd')
# MAGIC       ELSE NULL
# MAGIC     END AS EDRDuedate
# MAGIC   , t2.Activecase
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands') AND t3.SectorTeam IN ('Acorn') THEN 'Acorn'
# MAGIC
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank London') AND t3.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Sector Banking') THEN 'London Advisory & Investments'  
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank London') AND t3.SectorTeam IN ('Core Lending', 'Treasury Corp') THEN 'London Core Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank London') AND t3.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP', 'VCF') THEN 'London Structured Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank London') AND t3.SectorTeam IN ('Int. Desk') THEN 'London International Services'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank London') AND t3.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'London Markets'
# MAGIC
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabobank Canada(Rural)', 'Rabobank - USA Rabo AgriFinance') AND t3.SectorTeam IN ('Rural') THEN 'North America Rural'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND t3.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Sector Banking') THEN 'North America Advisory & Investments'  
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND t3.SectorTeam IN ('Core Lending', 'AF') THEN 'North America Core Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND t3.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP', 'VCF') THEN 'North America Structured Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND t3.SectorTeam IN ('Int. Desk') THEN 'North America International Services'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND t3.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'North America Markets'
# MAGIC
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND t3.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Sector Banking') THEN 'AsiaAdvisory & Investments'  
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND t3.SectorTeam IN ('Core Lending') THEN 'Asia Core Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND t3.SectorTeam IN ('Int. Desk') THEN 'Asia International Services'        
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND t3.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'Asia Markets'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND t3.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP', 'VCF') THEN 'Asia Structured Lending'
# MAGIC
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Antwerp') AND t3.SectorTeam IN ('Core Lending') THEN 'Antwerp Core Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Antwerp') AND t3.SectorTeam IN ('Sponsor Coverage') THEN 'Antwerp Advisory & Investments'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Dublin') AND t3.SectorTeam IN ('Core Lending') THEN 'Dublin Core Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Frankfurt') AND t3.SectorTeam IN ('Core Lending') THEN 'Frankfurt Core Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Frankfurt') AND t3.SectorTeam IN ('Int. Desk') THEN 'Frankfurt International Services'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Madrid') AND t3.SectorTeam IN ('Core Lending') THEN 'Madrid Core Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Milan') AND t3.SectorTeam IN ('Core Lending') THEN 'Milan Core Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Paris') AND t3.SectorTeam IN ('Core Lending') THEN 'Paris Core Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya') AND t3.SectorTeam IN ('TCF Corp', 'PF Corp', 'ABF', 'PF FI', 'TCF FI', 'SIP', 'VCF') THEN 'NL Structured Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya') AND t3.SectorTeam IN ('FA', 'ETC', 'HTD', 'REF', 'CNS', 'AF', 'TRST', 'EF Corp', 'TM', 'EF FI')  THEN 'NL Core Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands') AND t3.SectorTeam IN ('M&A') THEN 'NL M&A + ECM'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands') AND t3.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Rabo Frontier Ventures', 'RCI', 'Sector Banking') THEN 'NL Advisory & Investments'    
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands') AND t3.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'NL Markets'
# MAGIC       
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') AND t3.SectorTeam IN ('Rural') THEN 'RANZ Rural'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND t3.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Sector Banking') THEN 'RANZ Advisory & Investments'  
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND t3.SectorTeam IN ('Core Lending') THEN 'RANZ Core Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND t3.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP', 'VCF') THEN 'RANZ Structured Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND t3.SectorTeam IN ('Int. Desk') THEN 'RANZ International Services'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND t3.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'RANZ Markets'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Argentina') AND t3.SectorTeam IN ('ARG') THEN 'South America Core Lending'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Foundation', 'Rabobank Netherlands') AND t3.SectorTeam IN ('Foundation') THEN 'Foundation'
# MAGIC     END AS GlobalKYCPortfolioNew
# MAGIC   , CASE
# MAGIC        WHEN lower(t3.SectorTeam) IN ('af', 'cns', 'ef corp', 'etc', 'fa', 'htd', 'ref', 'tm', 'trst', 'vcf') AND lower(t2.GlobalClientOwnerLocation) IN ('rabobank netherlands', 'rabobank kenya') THEN 'NL&A GCC'
# MAGIC
# MAGIC         WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Antwerp') AND t3.SectorTeam IN ('Core Lending') THEN 'Belgium'
# MAGIC
# MAGIC         WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Paris') AND t3.SectorTeam IN ('Core Lending') THEN 'France'
# MAGIC         WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Dublin') AND t3.SectorTeam IN ('Core Lending') THEN 'Dublin'
# MAGIC         
# MAGIC         WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya') AND t3.SectorTeam IN ('PF Corp') THEN 'PF Corp'
# MAGIC         WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Madrid') AND t3.SectorTeam IN ('Core Lending') THEN 'Madrid'
# MAGIC         WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya') AND t3.SectorTeam IN ('TCF Corp') THEN 'TCF Corp'
# MAGIC         WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Antwerp', 'Rabobank London') AND t3.SectorTeam IN ('Sponsor Coverage') THEN 'Sponsor Coverage'
# MAGIC
# MAGIC         WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Frankfurt') AND t3.SectorTeam IN ('Core Lending', 'Int. Desk') THEN 'Frankfurt'
# MAGIC         WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Milan') AND t3.SectorTeam IN ('Core Lending') THEN 'Milan'
# MAGIC         WHEN t2.GlobalClientOwnerLocation IN ('Rabobank London') AND t3.SectorTeam IN ('Core Lending', 'VCF', 'Int. Desk' ) THEN 'London'
# MAGIC         
# MAGIC    END AS CamsMi
# MAGIC   , CASE
# MAGIC       WHEN lower(t3.SectorTeam) IN ('pf corp', 'core lending', 'fa', 'vcf', 'htd', 'tcf corp', 'cns', 'tm', 'etc', 'ef corp', 'trst', 'int. desk', 'ref', 'af', 'sponsor coverage', 'core lending etc', 'core lending fa') AND 
# MAGIC           lower(t2.GlobalClientOwnerLocation) IN ('rabobank netherlands', 'rabobank kenya', 'rabobank london', 'rabobank frankfurt', 'rabobank madrid', 'rabobank milan', 'rabobank antwerp', 'rabobank paris', 'rabobank dublin') 
# MAGIC       THEN 'backToGreen'
# MAGIC     END AS Scope
# MAGIC   , CASE
# MAGIC       WHEN lower(t3.SectorTeam) IN ('acorn') THEN 'Acorn'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('sponsor coverage', 'fas', 'sector banking') THEN 'CFO'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('af', 'arg', 'cns', 'core lending', 'ef corp', 'ef fi', 'etc', 'fa', 'htd', 'ref', 'trst', 'tm') THEN 'Core Lending'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('int. desk') THEN 'International Services'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('m&a') THEN 'M&A + ECM'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('agency', 'correspondent banking', 'fig', 'fig - orphan desk', 'markets', 'markets/fig', 'psp coverage', 'subsidiaries') THEN 'Markets/FIG'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('treasury') THEN 'Treasury'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('rabo frontier ventures', 'rci') THEN 'Rabo Corp Investment'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('abf', 'vcf') THEN 'VCF'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('tcf corp', 'tcf fi') THEN 'TCF'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('pf corp', 'pf fi') THEN 'PF'
# MAGIC       WHEN lower(t3.SectorTeam) in ('rural') THEN 'Rural'
# MAGIC     END AS GlobalBusinessLine
# MAGIC   , CASE
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank - RANZ Country Banking AND ROS','Rabobank - USA Rabo AgriFinance','Rabobank Australia','Rabobank New Zealand') AND t2.SourceClient like 'NP%' THEN 'NP'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Argentina', 'Rabobank Milan', 'Rabobank Paris', 'Rabobank Madrid', 'Rabobank Dublin', 'Rabobank Antwerp', 'Rabobank Turkey', 'Rabobank Frankfurt') AND (t3.KYCGroup IS NULL) THEN t2.FullLegalName
# MAGIC       ELSE t3.KYCGroup
# MAGIC     END AS KYCGroup
# MAGIC   , t4.ListedCorporatesInGroup
# MAGIC   , t4.TotalEntitiesInGroup
# MAGIC   , t3.SectorTeam
# MAGIC   , t3.ClientCaseInitiationStart
# MAGIC   , t3.CDDExecution
# MAGIC   -- , t3.CDDExecutionDaysincurrentcasephase
# MAGIC   , CASE
# MAGIC       WHEN t2.ClientType <> 'Legal Entity' THEN 'Natural Person' -- hardcoding Reason = Natural Person for all NP parties
# MAGIC       ELSE t3.Reason
# MAGIC     END AS Reason
# MAGIC   , CASE
# MAGIC       WHEN t2.ClientType <> 'Legal Entity' THEN 3 -- hardcoding CategoryNr = 3 for all NP parties
# MAGIC       WHEN t3.Reason = 'Associated Entity' THEN 4
# MAGIC       ELSE COALESCE(t3.CategoryNr, t9.GlobalFiles)
# MAGIC     END AS CategoryNr
# MAGIC   , t3.ReasonExplanation AS Explanation
# MAGIC   , t5.OnboardingDate
# MAGIC   , t9.GlobalFiles
# MAGIC   , t10.GlobalFiles AS ApprovedGlobalFiles
# MAGIC   , t3.LondonSectorTeam
# MAGIC   , t6.TradeName
# MAGIC   , t2.ClientApprovalDate
# MAGIC   , t2.EDL_LoadDate
# MAGIC   , TO_DATE(COALESCE(t2.FinalDecisionDate, t13.FinalDecisionDate), 'dd-MM-yyyy') AS SignOffDate -- used to be t12.FinalDecisionDate
# MAGIC   , t11.ReviewLocation
# MAGIC   , CASE 
# MAGIC       WHEN lower(t3.SectorTeam) IN ('trst', 'etc', 'tm', 'pf corp', 'ef corp', 'ef', 'fi', 'vcf', 'ef fi', 'core lending etc', 'tcf corp') THEN 'FOS ETC&TRUST'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('cns', 'ref') THEN 'FOS CNS&REF'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('fa', 'core lending fa') OR (lower(t3.SectorTeam) = 'core lending' AND lower(t11.ReviewLocation) IN ('kyc eu hub', 'kyc sc', 'kyc uk')) THEN 'FOS FA'
# MAGIC       WHEN lower(t3.SectorTeam) IN ('htd', 'sponsor coverage', 'af') THEN 'FOS HTD'
# MAGIC     END AS FOSTeamScope
# MAGIC   , COALESCE(t13.ValidatedRiskLevel, t2.ValidatedRiskLevel) AS ValidatedRiskLevel
# MAGIC     
# MAGIC   /*
# MAGIC     Region products involvement type
# MAGIC   */
# MAGIC   , CASE
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Kenya') AND t9.EAInvolved = 1 THEN 'Lead - Products Involved'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Kenya') AND t9.EAInvolved = 0 THEN 'Lead - No Products'
# MAGIC       WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Kenya') AND t9.EAInvolved = 1 THEN 'Non-Lead - Products Involved'
# MAGIC       WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Kenya') AND t9.EAInvolved = 0 THEN 'Non-Lead - No Products'
# MAGIC     END AS EAProductsInvolvmentType
# MAGIC   , CASE
# MAGIC       WHEN t2_asia.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9_asia.AsiaInvolved = 1 THEN 'Lead - Products Involved'
# MAGIC       WHEN t2_asia.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9_asia.AsiaInvolved = 0 THEN 'Lead - No Products'
# MAGIC       WHEN t2_asia.GlobalClientOwnerLocation NOT IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9_asia.AsiaInvolved = 1 THEN 'Non-Lead - Products Involved'
# MAGIC       WHEN t2_asia.GlobalClientOwnerLocation NOT IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9_asia.AsiaInvolved = 0 THEN 'Non-Lead - No Products'
# MAGIC     END AS AsiaProductsInvolvmentTypeLatestCompleted -- only for Asia, involvment type only considering lasted and completed cases
# MAGIC   , CASE
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9.AsiaInvolved = 1 THEN 'Lead - Products Involved'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9.AsiaInvolved = 0 THEN 'Lead - No Products'
# MAGIC       WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9.AsiaInvolved = 1 THEN 'Non-Lead - Products Involved'
# MAGIC       WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9.AsiaInvolved = 0 THEN 'Non-Lead - No Products'
# MAGIC     END AS AsiaProductsInvolvmentType
# MAGIC   , CASE
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') AND t9.NAInvolved = 1 THEN 'Lead - Products Involved'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') AND t9.NAInvolved = 0 THEN 'Lead - No Products'
# MAGIC       WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') AND t9.NAInvolved = 1 THEN 'Non-Lead - Products Involved'
# MAGIC       WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') AND t9.NAInvolved = 0 THEN 'Non-Lead - No Products'
# MAGIC     END AS NAProductsInvolvmentType
# MAGIC   , CASE
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Chile', 'Rabobank Brazil', 'Rabobank Argentina') AND t9.SAInvolved = 1 THEN 'Lead - Products Involved'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Chile', 'Rabobank Brazil', 'Rabobank Argentina') AND t9.SAInvolved = 0 THEN 'Lead - No Products'
# MAGIC       WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank Chile', 'Rabobank Brazil', 'Rabobank Argentina') AND t9.SAInvolved = 1 THEN 'Non-Lead - Products Involved'
# MAGIC       WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank Chile', 'Rabobank Brazil', 'Rabobank Argentina') AND t9.SAInvolved = 0 THEN 'Non-Lead - No Products'
# MAGIC     END AS SAProductsInvolvmentType
# MAGIC   , CASE
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') AND t9.RANZInvolved = 1 THEN 'Lead - Products Involved'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') AND t9.RANZInvolved = 0 THEN 'Lead - No Products'
# MAGIC       WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') AND t9.RANZInvolved = 1 THEN 'Non-Lead - Products Involved'
# MAGIC       WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') AND t9.RANZInvolved = 0 THEN 'Non-Lead - No Products'
# MAGIC     END AS RANZProductsInvolvmentType
# MAGIC   
# MAGIC   , t12.N2KLocationsList
# MAGIC   , t2.GCDSID
# MAGIC   , datediff(t3.CDDExecution, t2.Prework) as preworkcheckSLA
# MAGIC -- OLD VERSION
# MAGIC -- FROM Unique_Client_Gcob t1 -- max(CaseId) not cancelled
# MAGIC -- INNER JOIN radar.firebird_master t2 ON t1.GcobId = t2.GcobId AND t1.CaseId = t2.CaseId AND t1.ClientType = t2.ClientType
# MAGIC
# MAGIC FROM radar.firebird_master t1
# MAGIC INNER JOIN radar.firebird_master t2 ON t1.LatestId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC -- lastet completed file to define n2k in Asia
# MAGIC LEFT JOIN radar.firebird_master t2_asia ON t1.LatestCompletedCaseId = t2_asia.CaseId AND t1.UniqueGcobId = t2_asia.UniqueGcobId
# MAGIC
# MAGIC LEFT JOIN PortfolioPlanning t3 ON t2.UniqueGcobId = t3.UniqueGcobId
# MAGIC LEFT JOIN group_cte t4 ON LOWER(t3.KYCGroup) = t4.KYCGroup -- this is all in lower cases for grouping the kycgroup correctly
# MAGIC
# MAGIC LEFT JOIN onboarding_cte t5 ON t2.UniqueGcobId = t5.UniqueGcobId
# MAGIC LEFT JOIN Client_TradeName t6 ON t2.ClientId = t6.Id AND t2.ClientType = 'Legal Entity'
# MAGIC LEFT JOIN GlobalAndInvolvedFiles t9 ON t2.SourceClient = t9.SourceClient
# MAGIC -- lastet completed file to define n2k in Asia
# MAGIC LEFT JOIN GlobalAndInvolvedFiles t9_asia ON t1.CompletedId = t9_asia.ClientId AND t1.UniqueGcobId = t9_asia.UniqueGcobId
# MAGIC
# MAGIC LEFT JOIN GlobalAndInvolvedFiles t10 ON COALESCE(t2.LastFullReviewId, t2.CompletedId, t2.LatestId) = t10.ClientId AND t2.GcobId = t10.GcobId AND t2.ClientType = t10.ClientType -- this is for ApprovedGlobalFiles
# MAGIC
# MAGIC LEFT JOIN ReviewLocation_cte t11 ON t2.UniqueGcobId = t11.UniqueGcobId
# MAGIC
# MAGIC LEFT JOIN N2KLocationsList_cte t12 ON t2.UniqueGcobId = t12.UniqueGcobId
# MAGIC
# MAGIC -- LEFT JOIN nrd_cte t122 ON t2.UniqueGcobId = t122.UniqueGcobId -- nrd and signoff date of the latest completed case with a nrd
# MAGIC -- this join is for all the risks categories + NRD //// substituting t122 above
# MAGIC LEFT JOIN radar.firebird_master t13 ON COALESCE(t2.CompletedId, t2.LatestId) = t13.ClientId AND t2.UniqueGcobId = t13.UniqueGcobId -- for ValidatedRiskLevel

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.Clients

# COMMAND ----------

spark.sql('SELECT * FROM Clients').write.mode('overwrite').saveAsTable('radar.Clients')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if UniqueGcobId duplication
duplicated_uniquegcobid = spark.sql('''
    SELECT UniqueGcobId 
    FROM radar.clients 
    GROUP BY UniqueGcobId 
    HAVING count(*) > 1
''')

# check for duplication
if not duplicated_uniquegcobid.isEmpty():
    raise Exception('Duplicated UniqueGcobId in clients. Job stopped.')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the month!
from pyspark.sql.functions import lit, to_date

# if it is the first of the month, change the EDL_LoadDate column and append it to the historical dataset

if datetime.strptime(EDL_LoadDate, '%Y-%m-%d').date().day == 1:

    # check if the date already exists in the historical table
    check_date = (spark.table("radar.clients_historical").filter(f"EDL_LoadDate = '{EDL_LoadDate}'"))

    # append to historical dataset only if EDL_LoadDate does not already exist
    if check_date.count() == 0:
        spark.table("radar.clients").withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.clients_historical')

# COMMAND ----------

# DBTITLE 1,dropping cases_temp not needed anymore
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.Cases_temp -- dropping cases_temp not needed anymore

# COMMAND ----------

# MAGIC %md
# MAGIC # n2klocations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW n2klocations AS
# MAGIC
# MAGIC WITH n2k_cte AS (
# MAGIC     SELECT DISTINCT
# MAGIC         t1.SourceClient
# MAGIC         , t2.ProductOfferingLocation AS N2KLocation
# MAGIC         , t2.ProductLifeCycleStatus
# MAGIC     FROM radar.cases t1
# MAGIC     LEFT JOIN party_products_and_services t2 ON t1.SourceClient = t2.SourceClient
# MAGIC     WHERE t2.ProductLifeCycleStatus IN ('Active', 'Exit In Progress') -- , 'Inactive'
# MAGIC
# MAGIC     UNION 
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC         t1.SourceClient
# MAGIC         , t2.BookingEntityLocation AS N2KLocation
# MAGIC         , t2.ProductLifeCycleStatus
# MAGIC     FROM radar.cases t1
# MAGIC     LEFT JOIN party_products_and_services t2 ON t1.SourceClient = t2.SourceClient
# MAGIC     WHERE t2.ProductLifeCycleStatus IN ('Active', 'Exit In Progress') -- , 'Inactive'
# MAGIC
# MAGIC     UNION 
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC         SourceClient
# MAGIC         , GlobalClientOwnerLocation AS N2KLocation
# MAGIC         , 'GCO Location' AS ProductLifeCycleStatus
# MAGIC     FROM radar.cases
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     t1.SourceClient
# MAGIC     , t1.N2KLocation
# MAGIC     , t1.ProductLifeCycleStatus
# MAGIC     , CASE 
# MAGIC         WHEN t1.N2KLocation = t2.GlobalClientOwnerLocation THEN 1 
# MAGIC         ELSE 0
# MAGIC       END AS IsLeadLocation
# MAGIC     , CASE 
# MAGIC         WHEN t1.N2KLocation = t2.GlobalClientOwnerLocation THEN 'Lead' 
# MAGIC         ELSE 'Involved' 
# MAGIC       END AS LeadOrInvolved
# MAGIC     , CASE 
# MAGIC         WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') THEN 'Asia Lead'
# MAGIC         ELSE 'Asia Involved' 
# MAGIC       END AS AsiaLeadOrInvolved
# MAGIC
# MAGIC FROM n2k_cte t1
# MAGIC LEFT JOIN radar.cases t2 ON t1.SourceClient = t2.SourceClient

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.n2klocations

# COMMAND ----------

spark.sql('SELECT * FROM n2klocations').write.mode('overwrite').saveAsTable('radar.n2klocations')

# COMMAND ----------

# MAGIC %md
# MAGIC # ProductsAndServices

# COMMAND ----------

# DBTITLE 1,Look-up table for NAICS codes
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW gcobid_naics_lookup AS
# MAGIC
# MAGIC SELECT
# MAGIC     c.GCOBID,
# MAGIC     MAX(g.Primary_NAICS) AS Primary_NAICS
# MAGIC FROM radar.cases c
# MAGIC INNER JOIN gcds_client_Client g
# MAGIC     ON c.GCDSID = g.GCID
# MAGIC WHERE g.Primary_NAICS IS NOT NULL
# MAGIC GROUP BY c.GCOBID

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW productsandservices AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.SourceClient
# MAGIC   , t2.ProductName
# MAGIC   , t2.ProductDomain
# MAGIC   , t2.BusinessUnit
# MAGIC   , t2.BookingEntityLocation
# MAGIC   , t2.ProductOfferingLocation
# MAGIC   , t2.IsOtc
# MAGIC   , t2.ProductLifeCycleStatus
# MAGIC   , n.Primary_NAICS
# MAGIC FROM radar.cases t1
# MAGIC LEFT JOIN party_products_and_services t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN gcobid_naics_lookup n ON t1.GCOBID = n.GCOBID
# MAGIC WHERE t2.ProductLifeCycleStatus IN ('Active', 'Exit In Progress', 'Inactive')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.productsandservices

# COMMAND ----------

spark.sql('SELECT * FROM productsandservices').write.mode('overwrite').saveAsTable('radar.productsandservices')

# COMMAND ----------

# MAGIC %md
# MAGIC # ControlMeasures

# COMMAND ----------

# DBTITLE 1,view controlmeasures
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW controlmeasures AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.SourceClient
# MAGIC   , t1.ControlMeasureId
# MAGIC   , t1.ControlMeasureType
# MAGIC   , t1.Description
# MAGIC   , t1.Status
# MAGIC   , CASE
# MAGIC       WHEN t1.FrequencyTypeId = 1 THEN 'One-off'
# MAGIC       WHEN t1.FrequencyTypeId = 2 THEN 'Monthly'
# MAGIC       WHEN t1.FrequencyTypeId = 3 THEN 'Quaterly'
# MAGIC       WHEN t1.FrequencyTypeId = 4 THEN 'Bi-Yearly'
# MAGIC       WHEN t1.FrequencyTypeId = 5 THEN 'Yearly'
# MAGIC     END AS Frequency
# MAGIC   , t1.AssignedRole
# MAGIC   , t1.StartDate
# MAGIC   , t1.ExecutionDate
# MAGIC   , t1.EndDate
# MAGIC   , t1.ModifiedDate
# MAGIC   , CASE
# MAGIC       WHEN t1.Status = 'Ongoing'
# MAGIC         AND t1.ClientId = MAX(t2.ClientId) OVER (PARTITION BY t2.UniqueGcobid)
# MAGIC       THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS IsOngoingControlMeasure
# MAGIC   , CASE WHEN 
# MAGIC     t3.IsApprovedByClientCommittee = true THEN 'Yes'
# MAGIC     ELSE 'No'
# MAGIC     END AS IsApprovedByClientCommittee
# MAGIC
# MAGIC FROM party_control_measures t1
# MAGIC LEFT JOIN radar.cases t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN  CaseService_case_ControlMeasure AS t3 on t1.ControlMeasureId = t3.`id`
# MAGIC WHERE t1.ExpiredDate IS NULL

# COMMAND ----------

# DBTITLE 1,drop and store controlmeasures table
spark.sql('DROP TABLE IF EXISTS radar.controlmeasures')
spark.sql('SELECT * FROM controlmeasures').write.mode('overwrite').saveAsTable('radar.controlmeasures')

# COMMAND ----------

# DBTITLE 1,Duplicated ongoing control measure per SourceClient. Job stopped.
duplicated_ongoing_control_measure = spark.sql('''
    SELECT ControlMeasureId 
    FROM radar.controlmeasures
    WHERE IsOngoingControlMeasure = 1
    GROUP BY ControlMeasureId 
    HAVING count(*) > 1
''')

# check for duplication
if not duplicated_ongoing_control_measure.isEmpty():
    raise Exception('Duplicated ongoing control measure per ControlMeasureId. Job stopped.')

# COMMAND ----------

# MAGIC %md
# MAGIC # run pid_pad_on_time if first of the month - historical dataset

# COMMAND ----------

# DBTITLE 1,run notebook pid_pad_on_time if first of the month
from datetime import datetime

if datetime.strptime(EDL_LoadDate, '%Y-%m-%d').date().day == 1:
    dbutils.notebook.run("./pid_pad_on_time", timeout_seconds=3600)

# COMMAND ----------

# MAGIC %md
# MAGIC # Adding rdr_caseplanningdetails to cases

# COMMAND ----------

# DBTITLE 1,Get yst data for casephase
from datetime import datetime, timedelta
import re

gcob_objects = [
    'party_case_client_details'
    , 'party_workitem'
    , 'party_request_for_information'
]

for item in gcob_objects:
    # get the most recent version available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/'
    files = dbutils.fs.ls(path)
    version = max([
        int(re.search(r'/(\d+)/$', file.path).group(1))
        for file in files
        if re.search(r'/(\d+)/$', file.path)
    ])

    version = 102 # TEMPORARY UNTIL THE NEW VERSION IS UP AND RUNNING!!!!

    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOAD_DT=')[1][:8] for file in files if 'LOAD_DT=' in file.path)
    #get yst date
    yst_date = (datetime.strptime(load_date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')

    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/LOAD_DT={yst_date}*/*.parquet').createOrReplaceTempView(f"{item}_yst")


# COMMAND ----------

# DBTITLE 1,casephase for yst
# MAGIC %sql
# MAGIC --main_case_status_type_yst
# MAGIC CREATE OR REPLACE TEMPORARY VIEW main_case_status_type_yst AS
# MAGIC SELECT
# MAGIC   t1.CaseID
# MAGIC   , t1.ClientId
# MAGIC   , t1.SourceClient
# MAGIC   , CASE 
# MAGIC       WHEN t2.Name IN ('GCOB Review in progress', 'Initiation In Progress') THEN 'Initiation'
# MAGIC       WHEN t2.Name IN ('Ready for KYC assessment') THEN 'Ready for KYC assessment'
# MAGIC       WHEN t2.Name IN ('KYC assessment in progress') THEN 'Assessment'
# MAGIC       WHEN t2.Name IN ('Product Offboarding confirmation in progress', 'Ready for Product Offboarding confirmation', 'Product Offboarding in progress') THEN 'Product Offboarding'
# MAGIC       WHEN t2.Name IN ('Ready for 4 eye check') THEN 'Ready for QC'
# MAGIC       WHEN t2.Name IN ('4 eye check in progress') THEN 'QC'
# MAGIC       WHEN t2.Name IN ('Client committee sign off requested', 'Client Owner approval requested', 'Client owner sign off requested', 'Local client owner sign off requested', 'Senior management sign off requested') THEN 'Sign-off'
# MAGIC       WHEN t2.Name IN ('Product fulfilment in progress') THEN 'Product Fulfillment'
# MAGIC       WHEN t2.Name IN ('Completed', 'Cancelled') THEN t2.Name
# MAGIC       ELSE t2.Name
# MAGIC     END AS MainCaseStatusType
# MAGIC
# MAGIC FROM party_workitem_yst t1
# MAGIC LEFT JOIN gcob_static_CaseStatusType t2 ON t1.CaseCurrentStatus = t2.StatusId
# MAGIC ;
# MAGIC -- Workitems_status_yst
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW workitems_status_yst AS
# MAGIC
# MAGIC   WITH Prework AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS Prework
# MAGIC     FROM party_workitem_yst
# MAGIC     WHERE CaseStatusTypeWhenCreated IN (1, 16)
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC
# MAGIC , ReadyForAssessment AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS ReadyForAssessment
# MAGIC     FROM party_workitem_yst
# MAGIC     WHERE CaseStatusTypeWhenCreated = 2
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC
# MAGIC , AssessmentInProgress AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS AssessmentInProgress
# MAGIC     FROM party_workitem_yst
# MAGIC     WHERE CaseStatusTypeWhenCreated = 3
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC
# MAGIC , ReadyFor4EYECheck AS (
# MAGIC     SELECT 
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS ReadyFor4EYECheck
# MAGIC     FROM party_workitem_yst
# MAGIC     WHERE CaseStatusTypeWhenCreated = 4
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC
# MAGIC , 4EYECheck AS (
# MAGIC     SELECT 
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS 4EYECheck
# MAGIC     FROM party_workitem_yst
# MAGIC     WHERE CaseStatusTypeWhenCreated = 5
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC
# MAGIC , Signoff AS (
# MAGIC     SELECT 
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS SignOff
# MAGIC     FROM party_workitem_yst
# MAGIC     WHERE CaseStatusTypeWhenCreated IN (6, 18)
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC
# MAGIC , Fulfillment AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , MIN(WorkItemCreatedDate) AS Fulfillment
# MAGIC     FROM party_workitem_yst
# MAGIC     WHERE CaseStatusTypeWhenCreated = 8
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC , Completed AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , MAX(WorkItemCompletedDate) AS Completed
# MAGIC     FROM party_workitem_yst  
# MAGIC     WHERE CaseCurrentStatus = 9
# MAGIC       AND WorkItemCompletedDate IS NOT NULL
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC , Cancelled AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , MAX(WorkItemCompletedDate) AS Cancelled
# MAGIC     FROM party_workitem_yst
# MAGIC     WHERE CaseStatusTypeWhenCreated = 10
# MAGIC       AND WorkItemCompletedDate IS NOT NULL
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC , QCInteractions AS (
# MAGIC     SELECT
# MAGIC       SourceClient
# MAGIC       , COUNT(*) AS QCInteractions
# MAGIC     FROM party_workitem_yst
# MAGIC     WHERE CaseStatusName = 'Ready for 4 eye check'
# MAGIC     GROUP BY SourceClient
# MAGIC   )
# MAGIC , PreworkAnalystdate AS (
# MAGIC     SELECT DISTINCT
# MAGIC       t1.SourceClient
# MAGIC       , CASE WHEN t1.WorkItemAssignedUserName = 'Unknown' THEN t3.WorkItemAssignedUserName ELSE t1.WorkItemAssignedUserName END AS PreworkAnalyst
# MAGIC       , CASE WHEN t1.WorkItemAssignedUserName = 'Unknown' THEN SUBSTRING(t1.CreatingUserID, INSTR(t1.CreatingUserID, '\\') + 1) ELSE SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\') + 1) END AS PreworkAlias
# MAGIC       , MaxDate AS PreworkAnalystTeamDate
# MAGIC     FROM party_workitem_yst t1
# MAGIC     INNER JOIN (
# MAGIC       SELECT
# MAGIC         t1.SourceClient
# MAGIC         , MAX(WorkItemCreatedDate) AS MaxDate
# MAGIC       FROM party_workitem_yst t1
# MAGIC       LEFT JOIN (
# MAGIC         SELECT
# MAGIC           SourceClient
# MAGIC           , MIN(WorkItemCreatedDate) AS ReadyForAssessment
# MAGIC         FROM party_workitem_yst
# MAGIC         WHERE CaseStatusTypeWhenCreated = 2
# MAGIC         GROUP BY SourceClient
# MAGIC       ) t4 ON t1.SourceClient = t4.SourceClient
# MAGIC       WHERE t1.CaseStatusTypeWhenCreated IN (1, 16) AND WorkItemCreatedDate <= COALESCE(t4.ReadyForAssessment, CURRENT_TIMESTAMP())
# MAGIC       GROUP BY t1.SourceClient
# MAGIC     ) t2 ON t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
# MAGIC     LEFT JOIN (
# MAGIC       SELECT
# MAGIC         SourceClient
# MAGIC         , WorkItemAssignedUserName
# MAGIC         , AssignedUserId
# MAGIC         , CaseStatusTypeWhenCreated
# MAGIC         , WorkItemCreatedDate
# MAGIC       FROM party_workitem_yst
# MAGIC       WHERE CaseStatusTypeWhenCreated IN (1)
# MAGIC     ) t3 ON t2.SourceClient = t3.SourceClient AND t2.MaxDate = t3.WorkItemCreatedDate
# MAGIC     WHERE t1.CaseStatusTypeWhenCreated IN (1, 16)
# MAGIC   )
# MAGIC , AssessmentAnalystDate AS (
# MAGIC     -- select LastAnalyst on StatusAssessment
# MAGIC     SELECT DISTINCT 
# MAGIC       t1.SourceClient
# MAGIC       , CASE WHEN t1.WorkItemAssignedUserName = 'Unknown' THEN t3.WorkItemAssignedUserName ELSE t1.WorkItemAssignedUserName END AS AssessmentAnalyst
# MAGIC       , SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\') + 1) AS AssessmentAlias
# MAGIC       , t2.MaxDate AS AssessmentAnalystTeamDate
# MAGIC     FROM party_workitem_yst t1  
# MAGIC     INNER JOIN ( 
# MAGIC         SELECT 
# MAGIC           SourceClient
# MAGIC           , MAX(WorkItemCreatedDate) AS MaxDate 
# MAGIC         FROM party_workitem_yst 
# MAGIC         WHERE CaseStatusTypeWhenCreated IN (3, 20, 21)
# MAGIC         GROUP BY SourceClient
# MAGIC       ) t2 ON t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
# MAGIC     LEFT JOIN (
# MAGIC       SELECT
# MAGIC         SourceClient
# MAGIC         , WorkItemAssignedUserName
# MAGIC         , AssignedUserId
# MAGIC         , CaseStatusTypeWhenCreated
# MAGIC         , WorkItemCreatedDate 
# MAGIC       FROM party_workitem_yst 
# MAGIC       WHERE CaseStatusTypeWhenCreated IN (2, 19, 20)
# MAGIC     ) t3 ON t2.SourceClient = t3.SourceClient AND t2.MaxDate = t3.WorkItemCreatedDate
# MAGIC     WHERE t1.CaseStatusTypeWhenCreated IN (3, 20, 21)
# MAGIC   )
# MAGIC , 4EYEAnalystDate AS (
# MAGIC     SELECT DISTINCT 
# MAGIC       t1.SourceClient
# MAGIC       , t1.WorkItemAssignedUserName AS 4EYEAnalyst
# MAGIC       , SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\') + 1) AS 4EYEAlias
# MAGIC       , t2.MaxDate AS 4EYEAnalystTeamDate
# MAGIC     FROM party_workitem_yst t1 
# MAGIC     INNER JOIN ( 
# MAGIC       SELECT 
# MAGIC         t3.SourceClient
# MAGIC         , MAX(t3.WorkItemCreatedDate) AS MaxDate 
# MAGIC       FROM party_workitem_yst t3
# MAGIC         LEFT JOIN (
# MAGIC           SELECT
# MAGIC             SourceClient
# MAGIC             , MIN(WorkItemCreatedDate) AS SignOff
# MAGIC           FROM party_workitem_yst 
# MAGIC           WHERE CaseStatusTypeWhenCreated = 6
# MAGIC           GROUP BY SourceClient
# MAGIC         ) t4 ON t3.SourceClient = t4.SourceClient 
# MAGIC       WHERE t3.CaseStatusTypeWhenCreated = 5 AND t3.WorkItemCreatedDate <= COALESCE(t4.SignOff, CURRENT_TIMESTAMP())
# MAGIC       GROUP BY t3.SourceClient
# MAGIC       ) t2 ON t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
# MAGIC     WHERE t1.CaseStatusTypeWhenCreated = 5
# MAGIC   )
# MAGIC , LastAnalyst AS (
# MAGIC     SELECT DISTINCT
# MAGIC       t1.SourceClient
# MAGIC       , t1.WorkItemAssignedUserName AS LastAnalyst
# MAGIC       , SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\') + 1) AS LastAnalystAlias
# MAGIC       , MaxDate AS LastAnalystTeamDate
# MAGIC     FROM party_workitem_yst t1
# MAGIC     INNER JOIN (
# MAGIC       SELECT
# MAGIC         SourceClient
# MAGIC         , MAX(WorkItemCreatedDate) AS MaxDate
# MAGIC         , MAX(CaseStatusTypeWhenCreated) AS MaxCaseStatusTypeWhenCreated
# MAGIC       FROM party_workitem_yst
# MAGIC       WHERE ResponsibleRole IN (2, 3)
# MAGIC         AND CaseStatusTypeWhenCreated NOT IN (5, 4, 2) -- ('4 eye check in progress', 'Ready for 4 eye check', 'Ready for KYC ASsessment')
# MAGIC       GROUP BY SourceClient
# MAGIC       ) t2 ON t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate AND t1.CaseStatusTypeWhenCreated = t2.MaxCaseStatusTypeWhenCreated
# MAGIC     WHERE ResponsibleRole IN (2, 3) AND CaseStatusTypeWhenCreated NOT IN (5, 4, 2)
# MAGIC   )
# MAGIC
# MAGIC , WithLag AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     LAG(CaseStatusTypeWhenCreated) OVER (
# MAGIC       PARTITION BY SourceClient
# MAGIC       ORDER BY WorkItemCreatedDate
# MAGIC     ) AS PreviousStatus,
# MAGIC     LAG(WorkItemCreatedDate) OVER (
# MAGIC       PARTITION BY SourceClient
# MAGIC       ORDER BY WorkItemCreatedDate
# MAGIC     ) AS PreviousDate
# MAGIC   FROM party_workitem_yst
# MAGIC ),
# MAGIC  
# MAGIC -- Prework (Status = 1)
# MAGIC PreworkFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated = 1
# MAGIC ),
# MAGIC PreworkFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus = 1 THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS LatestPreworkDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM PreworkFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC  
# MAGIC -- Ready for Assessment (Status = 2)
# MAGIC ReadyForAssessmentFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated = 2
# MAGIC ),
# MAGIC ReadyForAssessmentFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus = 2 THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS LatestReadyForAssessmentDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM ReadyForAssessmentFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- KYC Assessment In Progress (Status = 3)
# MAGIC   KycAssessmentInProgressFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated = 3
# MAGIC ),
# MAGIC KycAssessmentInProgressFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus = 3 THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS LatestAssessmentInProgressDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM KycAssessmentInProgressFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- Ready for 4 Eye Check (Status = 4)
# MAGIC   ReadyFor4EYECheckFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated = 4
# MAGIC ),
# MAGIC ReadyFor4EYECheckFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus = 4 THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS LatestReadyFor4EYECheckDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM ReadyFor4EYECheckFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- Ready for 4 Eye Check (Status = 5)
# MAGIC 4EYECheckFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated = 5
# MAGIC ),
# MAGIC 4EYECheckFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus = 5 THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS Latest4EYECheckdDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM 4EYECheckFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- Ready for Signoff (Status IN (6, 18))
# MAGIC SignoffFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated IN (6, 18)
# MAGIC ),
# MAGIC SignoffFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus IN (6, 18) THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS LatestSignoffDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM SignoffFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- Ready for Fulfillment (Status = 8)
# MAGIC FulfillmentFiltered AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     WorkItemCreatedDate,
# MAGIC     PreviousDate,
# MAGIC     CaseStatusTypeWhenCreated,
# MAGIC     PreviousStatus
# MAGIC   FROM WithLag
# MAGIC   WHERE CaseStatusTypeWhenCreated = 8
# MAGIC ),
# MAGIC FulfillmentFinal AS (
# MAGIC   SELECT
# MAGIC     SourceClient,
# MAGIC     CASE
# MAGIC       WHEN PreviousStatus = 8 THEN PreviousDate
# MAGIC       ELSE WorkItemCreatedDate
# MAGIC     END AS LatestFulfillmentDate
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY SourceClient
# MAGIC              ORDER BY WorkItemCreatedDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM FulfillmentFiltered
# MAGIC   ) sub
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.ClientId
# MAGIC   , t1.SourceClient
# MAGIC   , t1.CaseId
# MAGIC   , t1.GcobId
# MAGIC   , t2.Prework
# MAGIC   , t10.PreworkAnalyst
# MAGIC   , t10.PreworkAnalystTeamDate
# MAGIC   , t3.ReadyForAssessment
# MAGIC   , t4.AssessmentInProgress
# MAGIC   , t11.AssessmentAnalyst
# MAGIC   , t11.AssessmentAnalystTeamDate
# MAGIC   , t5.4EYECheck
# MAGIC   , t12.4EYEAnalyst
# MAGIC   , t12.4EYEAnalystTeamDate
# MAGIC   , t6.SignOff
# MAGIC   , t7.Fulfillment
# MAGIC   , t8.Completed
# MAGIC   , t9.Cancelled
# MAGIC
# MAGIC
# MAGIC   , t15.LatestPreworkDate
# MAGIC   , t16.LatestReadyForAssessmentDate
# MAGIC   , t17.LatestAssessmentInProgressDate
# MAGIC   , t18.Latest4EYECheckdDate
# MAGIC   , t19.LatestSignoffDate
# MAGIC   , t20.LatestFulfillmentDate
# MAGIC   , t21.LatestReadyFor4EYECheckDate
# MAGIC   , t22.ReadyFor4EYECheck
# MAGIC
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t9.Cancelled IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 8)
# MAGIC       WHEN t8.Completed IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 7)
# MAGIC       WHEN t7.Fulfillment IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 6)
# MAGIC       WHEN t6.SignOff IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 5)
# MAGIC       WHEN t5.4EYECheck IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 4)
# MAGIC       WHEN t4.AssessmentInProgress IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 3)
# MAGIC       WHEN t3.ReadyForAssessment IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 2)
# MAGIC       WHEN t2.Prework IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 1)
# MAGIC       ELSE 'UNKNOWN'
# MAGIC     END AS CasePhase
# MAGIC
# MAGIC , t13.QCInteractions
# MAGIC , t14.LastAnalyst
# MAGIC , t14.LastAnalystTeamDate
# MAGIC , t10.PreworkAlias
# MAGIC , t11.AssessmentAlias
# MAGIC , t12.4EYEAlias
# MAGIC , t14.LastAnalystAlias
# MAGIC
# MAGIC FROM party_workitem_yst t1
# MAGIC LEFT JOIN Prework t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN Readyforassessment t3 ON t1.SourceClient = t3.SourceClient
# MAGIC LEFT JOIN AssessmentInProgress t4 ON t1.SourceClient = t4.SourceClient
# MAGIC LEFT JOIN 4EYECheck t5 ON t1.SourceClient = t5.SourceClient
# MAGIC LEFT JOIN Signoff t6 ON t1.SourceClient = t6.SourceClient
# MAGIC LEFT JOIN Fulfillment t7 ON t1.SourceClient = t7.SourceClient
# MAGIC LEFT JOIN Completed t8 ON t1.SourceClient = t8.SourceClient
# MAGIC LEFT JOIN Cancelled t9 ON t1.SourceClient = t9.SourceClient
# MAGIC LEFT JOIN PreworkAnalystdate t10 ON t1.SourceClient = t10.SourceClient
# MAGIC LEFT JOIN AssessmentAnalystDate t11 ON t1.SourceClient = t11.SourceClient
# MAGIC LEFT JOIN 4EYEAnalystDate t12 ON t1.SourceClient = t12.SourceClient
# MAGIC LEFT JOIN QCInteractions t13 ON t1.SourceClient = t13.SourceClient
# MAGIC LEFT JOIN LastAnalyst t14 ON t1.SourceClient = t14.SourceClient
# MAGIC
# MAGIC
# MAGIC LEFT JOIN PreworkFinal t15 ON t1.SourceClient = t15.SourceClient
# MAGIC LEFT JOIN ReadyForAssessmentFinal t16 ON t1.SourceClient = t16.SourceClient
# MAGIC LEFT JOIN KycAssessmentInProgressFinal t17 ON t1.SourceClient = t17.SourceClient
# MAGIC LEFT JOIN 4EYECheckFinal t18 ON t1.SourceClient = t18.SourceClient
# MAGIC LEFT JOIN SignoffFinal t19 ON t1.SourceClient = t19.SourceClient
# MAGIC LEFT JOIN FulfillmentFinal t20 ON t1.SourceClient = t20.SourceClient
# MAGIC LEFT JOIN ReadyFor4EYECheckFinal t21 ON t1.SourceClient = t21.SourceClient
# MAGIC LEFT JOIN ReadyFor4EYECheck t22 ON t1.SourceClient = t22.SourceClient
# MAGIC ;
# MAGIC -- rfi_status_yst
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW rfi_status_yst AS -- dml.RFIStatus
# MAGIC SELECT DISTINCT
# MAGIC   t1.GcobId
# MAGIC   , t1.CaseId
# MAGIC   , t1.ClientId
# MAGIC   , t1.SourceClient
# MAGIC   , t4.DateCreated AS DateCreated
# MAGIC   , COALESCE(t3.NumberOfRFI, 0) AS NumberOfRFI
# MAGIC   , CASE 
# MAGIC       WHEN t1.CasePhase IN ('Cancelled', 'Completed') THEN t1.CasePhase
# MAGIC       WHEN t3.RequestStatusNumber IN (1) AND t3.NumberOfRFI = 1 THEN 'Client Outreach'
# MAGIC       WHEN t3.RequestStatusNumber IN (1) AND t3.NumberOfRFI > 1 THEN 'Rebound Client Outreach'
# MAGIC       WHEN t3.RequestStatusNumber = 2 THEN 'Client Outreach Completed'
# MAGIC     END AS CasePhase
# MAGIC   , CASE
# MAGIC       WHEN t1.CasePhase <> 'Assessment in progress' THEN t1.CasePhase
# MAGIC       WHEN t1.SourceClient IS NULL THEN 'Execute Assessment' -- t2.CaseId
# MAGIC       WHEN t3.NumberOfRFI = 1 AND t3.RequestStatusNumber = 1 THEN 'Waiting for information'
# MAGIC       WHEN t3.NumberOfRFI = 1 AND t3.RequestStatusNumber = 2 THEN 'Reviewing information'
# MAGIC       WHEN t3.NumberOfRFI >= 1 AND t3.RequestStatusNumber = 3 THEN 'Finalise assessment'
# MAGIC       WHEN t3.NumberOfRFI > 1 AND t3.RequestStatusNumber = 1 THEN 'Waiting for additional information'
# MAGIC       WHEN t3.NumberOfRFI > 1 AND t3.RequestStatusNumber = 2 THEN 'Reviewing additional information'
# MAGIC     END AS CasePhaseIncludingRFI
# MAGIC
# MAGIC
# MAGIC FROM workitems_status_yst t1
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC   SELECT DISTINCT
# MAGIC     ClientId
# MAGIC     , Gcobid
# MAGIC     -- changing it due to error in Assessment 1. When RFI is closed, casephase should be Assessment 2.
# MAGIC     -- with MIN(DateCreated). When RFI is closed casePhase is Assessment 1 (which is not correct)
# MAGIC     --, MIN(DateCreated) AS MinDateCreated
# MAGIC     , LEFT(MAX(DateCreated), 16) AS MinDateCreated
# MAGIC     , COUNT(RequestTypeId) AS NumberOfRFI 
# MAGIC     , MIN(CASE WHEN StatusType = 'PendingResponse' THEN 1 WHEN StatusType = 'PendingReview' THEN 2 WHEN StatusType = 'Completed' THEN 3 END) As RequestStatusNumber
# MAGIC   FROM party_request_for_information_yst 
# MAGIC   GROUP BY 1,2
# MAGIC ) t3 ON t1.ClientId = t3.ClientId AND t1.GcobId = t3.GcobId
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC   SELECT * 
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC       *
# MAGIC       , ROW_NUMBER() OVER (PARTITION BY ClientId, Gcobid ORDER BY RequestTypeId, DateCreated DESC) as rn
# MAGIC     FROM party_request_for_information_yst
# MAGIC     WHERE DateCreated IS NOT NULL AND StatusType <> 'Cancelled'
# MAGIC   ) subquery WHERE rn = 1
# MAGIC ) t4 ON t1.ClientId = t4.ClientId AND t1.GcobId = t4.GcobId AND t3.MinDateCreated = LEFT(t4.DateCreated, 16)
# MAGIC ;
# MAGIC --workitem_status_final_temp_yst
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW workitems_status_final_temp_yst AS -- same columns found in rpt.cases
# MAGIC
# MAGIC WITH DurationInfoRequestInWorkingDay_CTE AS (
# MAGIC   SELECT
# MAGIC     ClientId
# MAGIC     , CaseId
# MAGIC     , GcobId
# MAGIC     , StatusType
# MAGIC     , DATEDIFF(DAY, DateCreated, COALESCE(DateCompleted, CURRENT_DATE())) + 1 
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, DateCreated, COALESCE(DateCompleted, CURRENT_DATE())) * 2) 
# MAGIC           + (CASE WHEN DATE_FORMAT(DateCreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
# MAGIC           + (CASE WHEN DATE_FORMAT(DateCompleted, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       ) AS DurationInfoRequestInWorkingDay
# MAGIC   FROM party_request_for_information_yst
# MAGIC )
# MAGIC
# MAGIC
# MAGIC , second_rfi_creation_date AS (
# MAGIC   SELECT 
# MAGIC     ClientId,
# MAGIC     GcobId,
# MAGIC     DateCreated,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY ClientId, GcobId ORDER BY DateCreated ASC) AS rn
# MAGIC   FROM party_request_for_information_yst
# MAGIC   WHERE RequestTypeID = 1
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.SourceClient
# MAGIC
# MAGIC  
# MAGIC , CASE
# MAGIC     WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled') THEN t4.MainCaseStatusType
# MAGIC     WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN 'Offboarding'
# MAGIC     WHEN t4.MainCaseStatusType = 'Sign-off' THEN 'Sign-off'
# MAGIC     WHEN t3.CasePhase IN ('Rebound Client Outreach', 'Client Outreach', 'Client Outreach Completed') THEN t3.CasePhase
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 'Rebound Assessment'
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 'Assessment 1'
# MAGIC     WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 'Assessment 2'
# MAGIC     WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 'Rebound Initiation'
# MAGIC     ELSE t4.MainCaseStatusType
# MAGIC   END AS CasePhase
# MAGIC
# MAGIC
# MAGIC FROM workitems_status_yst t1
# MAGIC INNER JOIN party_case_client_details_yst t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN rfi_status_yst t3 ON t1.SourceClient = t3.SourceClient
# MAGIC LEFT JOIN main_case_status_type_yst t4 ON t1.SourceClient = t4.SourceClient
# MAGIC

# COMMAND ----------

# DBTITLE 1,create temp view cases_v2
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW cases_v2 AS
# MAGIC SELECT DISTINCT
# MAGIC   t1.*
# MAGIC   , CASE
# MAGIC       WHEN t1.SourceSystemReference = 'GCOB_NP-NPPC' THEN concat('NP_', t1.CaseId)
# MAGIC       ELSE t1.CaseId
# MAGIC     END AS CDDCaseId
# MAGIC   , CASE
# MAGIC       WHEN t1.daysInReboundAssessment > 0 THEN 'Yes'
# MAGIC       ELSE 'No'
# MAGIC     END AS WentToReboundAssessment
# MAGIC   , CASE
# MAGIC       WHEN t1.DaysInReboundClientOutreach > 0 THEN 'Yes'
# MAGIC       ELSE 'No'
# MAGIC     END AS WentToReboundOutreach
# MAGIC
# MAGIC -- columns for QC FEC OPS
# MAGIC   , CASE
# MAGIC       WHEN t1.Casephase = 'Completed' THEN 'Completed'
# MAGIC       ELSE 'Not Completed'
# MAGIC     END AS GCOBCaseStatus
# MAGIC   , t2.ConsultationRequired
# MAGIC   , CASE 
# MAGIC       WHEN t2.ConsultationRequired = TRUE 
# MAGIC         OR t1.SubmitToClientCommittee = 'Yes, Client Committee Approval' 
# MAGIC         OR t1.SubmitToClientCommittee = 'Yes, Senior Management Approval'
# MAGIC       THEN 
# MAGIC         CASE 
# MAGIC           WHEN (t1.numberOfReboundAssessment - 1) = -1 THEN 0
# MAGIC           ELSE (t1.numberOfReboundAssessment - 1)
# MAGIC         END
# MAGIC       ELSE 
# MAGIC         CASE 
# MAGIC           WHEN (t1.numberOfReboundAssessment = -1) THEN 0
# MAGIC           ELSE t1.numberOfReboundAssessment
# MAGIC         END
# MAGIC       END AS NewLogic_numberOfReboundAssessment
# MAGIC
# MAGIC
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN t3.NotAdequateMaterial IS NOT NULL THEN 'Not Adequate - Material'
# MAGIC       
# MAGIC       WHEN t3.NotAdequateNonMaterial IS NOT NULL 
# MAGIC         AND t3.NotAdequateMaterial IS NULL THEN 'Not Adequate - Non Material'
# MAGIC       
# MAGIC       WHEN t3.DependentError IS NOT NULL 
# MAGIC         AND t3.NotAdequateMaterial IS NULL 
# MAGIC         AND t3.NotAdequateNonMaterial IS NULL THEN 'Dependent Error'
# MAGIC
# MAGIC       WHEN (
# MAGIC         t3.DependentError IS NULL AND
# MAGIC         t3.HousekeepingCompleted IS NULL AND
# MAGIC         t3.NotAdequateMaterial IS NULL AND
# MAGIC         t3.NotAdequateNonMaterial IS NULL AND
# MAGIC         t3.Overturned IS NULL AND
# MAGIC         (t3.SourceClient IS NULL OR
# MAGIC         t3.HousekeepingCompleted IS NOT NULL) AND
# MAGIC         t3.NotAdequateMaterial IS NULL AND
# MAGIC         t3.NotAdequateNonMaterial IS NULL AND
# MAGIC         t3.DependentError IS NULL OR
# MAGIC         t3.Overturned IS NOT NULL AND
# MAGIC         t3.NotAdequateMaterial IS NULL AND
# MAGIC         t3.NotAdequateNonMaterial IS NULL AND
# MAGIC         t3.DependentError IS NULL
# MAGIC       ) THEN 'First Time Right'
# MAGIC       ELSE 'N/A'
# MAGIC     END AS CaseResult
# MAGIC
# MAGIC   , t4.ReadyForKYCAssessmentDate
# MAGIC   , t5.CasePhase AS CasePhase_yst
# MAGIC   
# MAGIC FROM radar.cases t1
# MAGIC LEFT JOIN radar.QualityControl t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN radar.PivotQualityControl t3 ON t1.SourceClient = t3.SourceClient
# MAGIC LEFT JOIN radar.firebird_master t4 ON t1.SourceClient = t4.SourceClient
# MAGIC LEFT JOIN workitems_status_final_temp_yst t5 ON t1.SourceClient = t5.SourceClient

# COMMAND ----------

# DBTITLE 1,save table cases_v2
spark.sql('DROP TABLE IF EXISTS radar.cases_v2')

# write to intermediate table
spark.table('cases_v2').write.mode('overwrite').saveAsTable('radar.cases_v2')

# drop cases
spark.sql('DROP TABLE IF EXISTS radar.cases')


# rename the cases_v2 to cases
spark.sql('ALTER TABLE radar.cases_v2 RENAME TO radar.cases')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if SourceClient duplication
duplicated_sourceclient = spark.sql('''
    SELECT SourceClient 
    FROM radar.cases 
    GROUP BY SourceClient 
    HAVING count(*) > 1
''')

# check for duplicationload_date
if not duplicated_sourceclient.isEmpty():
    raise Exception('Duplicated SourceClient in cases. Job stopped.')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.firebird_master

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW casesCompletedSla AS
# MAGIC SELECT *
# MAGIC FROM radar.cases
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY uniquegcobid
# MAGIC     ORDER BY CaseCompletedDate DESC
# MAGIC ) = 1
# MAGIC
# MAGIC

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS radar.casesCompletedSla')
spark.sql('SELECT * FROM casesCompletedSla').write.mode('overwrite').saveAsTable('radar.casesCompletedSla')

# COMMAND ----------

# MAGIC %md
# MAGIC # Create clients with current cases information

# COMMAND ----------

# DBTITLE 1,old
# MAGIC %sql
# MAGIC /*
# MAGIC CREATE OR REPLACE TEMPORARY VIEW clients_v2 AS
# MAGIC
# MAGIC SELECT
# MAGIC   t1.*
# MAGIC   , t2.PreviousNextReviewDate
# MAGIC   , t2.Casephase
# MAGIC   , t2.Daysincurrentcasephase
# MAGIC   , t2.4EYEanalyst
# MAGIC   , t2.4EyeUserTeam
# MAGIC   , t2.Assessmentanalyst
# MAGIC   , t2.Completed
# MAGIC   , t2.KYCUserTeam
# MAGIC   , t2.Preworkanalyst
# MAGIC   , t2.PreworkUserTeam
# MAGIC   , t2.CaseReviewType
# MAGIC   , t2.CasePhaseCurrentYear
# MAGIC   , t2.4EyeDepartment
# MAGIC   , t2.AsiaProductsInvolvementChange
# MAGIC   , t2.CDDDepartment
# MAGIC   , t2.PreworkDepartment
# MAGIC   , CASE 
# MAGIC       WHEN t2.FirstRFIcreated IS NULL OR t2.CompleteddatefirstRFI IS NULL THEN 'Not in client outreach'
# MAGIC       WHEN DATEDIFF(DAY, t2.FirstRFIcreated, t2.CompleteddatefirstRFI) > 28 THEN 'Too late'
# MAGIC       ELSE 'On time'
# MAGIC     END AS ClientOutreachStatus
# MAGIC   , CASE
# MAGIC       WHEN DATEDIFF(DAY, t2.Prework, GETDATE()) >= 60 AND t2.EDRoverdue <> 'Yes' THEN 'Yes'
# MAGIC       ELSE 'No'
# MAGIC     END AS ApproachingOverdue
# MAGIC   , t2.DaysInClientOutreach
# MAGIC   , t2.DaysInReboundClientOutreach
# MAGIC   , t2.KYCDepartment
# MAGIC   , t2.Prework
# MAGIC   , t2.Signoff
# MAGIC   , t2.TotalCaseDuration
# MAGIC   , t2.EDRoverdue
# MAGIC   -- , CASE
# MAGIC   --     WHEN PreviousNextReviewDate IS NULL OR Completed IS NULL THEN 'N/A'
# MAGIC   --     WHEN PreviousNextReviewDate < Completed THEN 'No'
# MAGIC   --     ELSE 'Yes'
# MAGIC   --   END AS PRCompletedOnTime
# MAGIC   , CASE
# MAGIC       WHEN t1.Overdue = 'Yes' AND t2.EDRoverdue = 'Yes' THEN 'No'
# MAGIC       ELSE t1.Overdue
# MAGIC     END AS NewPROverdueLogic
# MAGIC   , t2.CasePhaseSortCurrentYear
# MAGIC   , t2.CDDCaseId
# MAGIC   -- , CASE 
# MAGIC   --     WHEN t2.CurrentStatus IS NULL THEN 'N/A'
# MAGIC   --     WHEN EXISTS (
# MAGIC   --       SELECT 1
# MAGIC   --       FROM radar.rdr_sprintstatuscasephasemapping AS mapping
# MAGIC   --       WHERE mapping.SprintStatus = t2.CurrentStatus
# MAGIC   --         AND mapping.CasePhase = t2.Casephase
# MAGIC   --     ) THEN 'Yes'
# MAGIC   --     ELSE 'No'
# MAGIC   --   END AS CorrectStatus
# MAGIC
# MAGIC FROM radar.clients t1
# MAGIC INNER JOIN radar.cases t2 ON t1.SourceClient = t2.SourceClient
# MAGIC */

# COMMAND ----------

# DBTITLE 1,NEW CLIENT_V2 WITH SLA STEPS
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW clients_v2 AS
# MAGIC WITH latest_sprintstatus AS (
# MAGIC     SELECT
# MAGIC         CaseId,
# MAGIC         SprintStatus,
# MAGIC         CreatedOnUTC
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             CaseId,
# MAGIC             SprintStatus,
# MAGIC             CreatedOnUTC,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY CaseId ORDER BY CreatedOnUTC DESC) AS rn
# MAGIC         FROM radar.rdr_sprintstatuslog
# MAGIC     ) ranked
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC     t1.*,
# MAGIC     t2.PreviousNextReviewDate,
# MAGIC     t2.Casephase,
# MAGIC     t2.Daysincurrentcasephase,
# MAGIC     t2.4EYEanalyst,
# MAGIC     t2.4EyeUserTeam,
# MAGIC     t2.Assessmentanalyst,
# MAGIC     t2.Completed,
# MAGIC     t2.KYCUserTeam,
# MAGIC     t2.Preworkanalyst,
# MAGIC     t2.PreworkUserTeam,
# MAGIC     t2.CaseReviewType,
# MAGIC     t2.CasePhaseCurrentYear,
# MAGIC     t2.4EyeDepartment,
# MAGIC     t2.AsiaProductsInvolvementChange,
# MAGIC     t2.CDDDepartment,
# MAGIC     t2.PreworkDepartment,
# MAGIC     CASE 
# MAGIC         WHEN t2.FirstRFIcreated IS NULL OR t2.CompleteddatefirstRFI IS NULL THEN 'Not in client outreach'
# MAGIC         WHEN DATEDIFF(DAY, t2.FirstRFIcreated, t2.CompleteddatefirstRFI) > 28 THEN 'Too late'
# MAGIC         ELSE 'On time'
# MAGIC     END AS ClientOutreachStatus,
# MAGIC     CASE
# MAGIC         WHEN DATEDIFF(DAY, t2.Prework, GETDATE()) >= 60 AND t2.EDRoverdue <> 'Yes' THEN 'Yes'
# MAGIC         ELSE 'No'
# MAGIC     END AS ApproachingOverdue,
# MAGIC     t2.DaysInClientOutreach,
# MAGIC     t2.DaysInReboundClientOutreach,
# MAGIC     t2.KYCDepartment,
# MAGIC     t2.Prework,
# MAGIC     t2.Signoff,
# MAGIC     t2.TotalCaseDuration,
# MAGIC     t2.EDRoverdue,
# MAGIC     CASE
# MAGIC         WHEN t1.Overdue = 'Yes' AND t2.EDRoverdue = 'Yes' THEN 'No'
# MAGIC         ELSE t1.Overdue
# MAGIC     END AS NewPROverdueLogic,
# MAGIC     t2.CasePhaseSortCurrentYear,
# MAGIC     t2.CDDCaseId,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN t4.SprintStatus IN ('Client Outreach', 'Rebound Client Outreach') THEN 'RFI'
# MAGIC         WHEN t4.SprintStatus IN ('With CC secretaries') THEN 'MLRO/CC/SMSO'
# MAGIC         WHEN t4.SprintStatus IN ('Sent to FoS for Outreach') THEN 'Sent to FoS for Outreach'
# MAGIC         WHEN t2.casephase IN ('Initiation', 'Rebound Initiation') THEN 'Prework'
# MAGIC         WHEN t2.casephase IN ('Assessment 1') THEN 'Assessment 1'
# MAGIC         WHEN t2.casephase IN ('Assessment 2', 'Rebound Assessment') THEN 'Assessment 2'
# MAGIC         WHEN t2.casephase IN ('Sign-off') THEN 'Sign-off'
# MAGIC         WHEN t2.casephase IN ('Product Fulfillment') THEN 'Product Fulfillment'
# MAGIC         WHEN t2.casephase IN ('QC') THEN 'QC'    
# MAGIC         WHEN t2.casephase IN ('Ready for QC') THEN 'Ready for QC'    
# MAGIC         WHEN t2.casephase IN ('Ready for KYC assessment') THEN 'Ready for KYC assessment'
# MAGIC         WHEN (t2.casephase IN ('Client Outreach Completed')) OR (t4.SprintStatus IN ('Client Outreach Completed', 'Rebound Client Outreach Completed')) THEN 'Client Outreach Completed'
# MAGIC     END AS CasePhaseSLA,
# MAGIC             
# MAGIC     CASE
# MAGIC         WHEN t4.SprintStatus IN ('Client Outreach', 'Rebound Client Outreach') THEN 5
# MAGIC         WHEN t4.SprintStatus IN ('With CC secretaries') THEN 10
# MAGIC         WHEN t4.SprintStatus IN ('Sent to FoS for Outreach') THEN 4
# MAGIC         WHEN t2.casephase IN ('Initiation', 'Rebound Initiation') THEN 1
# MAGIC         WHEN t2.casephase IN ('Assessment 1') THEN 3
# MAGIC         WHEN t2.casephase IN ('Assessment 2', 'Rebound Assessment') THEN 7
# MAGIC         WHEN t2.casephase IN ('Sign-off') THEN 11
# MAGIC         WHEN t2.casephase IN ('Product Fulfillment') THEN 12
# MAGIC         WHEN t2.casephase IN ('Ready for KYC assessment') THEN 2
# MAGIC         WHEN t2.casephase IN ('Ready for QC') THEN 8
# MAGIC         WHEN t2.casephase IN ('QC') THEN 9
# MAGIC         WHEN (t2.casephase IN ('Client Outreach Completed')) OR (t4.SprintStatus IN ('Client Outreach Completed', 'Rebound Client Outreach Completed')) THEN 6
# MAGIC     END AS CasePhaseSortOrderSLA,
# MAGIC     CASE 
# MAGIC         WHEN t4.SprintStatus IN ('Client Outreach', 'Client Outreach Completed','Rebound Client Outreach', 'Rebound Client Outreach Completed') THEN t2.DaysInRFI_SLA
# MAGIC         WHEN t4.SprintStatus IN ('With CC secretaries') THEN t2.DaysInMLRO_SLA
# MAGIC         WHEN t4.SprintStatus IN ('Sent to FoS for Outreach') THEN t2.daysinsenttofosforoutreach_sla
# MAGIC         WHEN t2.casephase IN ('Initiation', 'Rebound Initiation') THEN t2.daysInPrework_Sla
# MAGIC         WHEN t2.casephase IN ('Assessment 1') THEN t2.DaysInAssessment1
# MAGIC         WHEN t2.casephase IN ('Assessment 2', 'Rebound Assessment') THEN t2.daysInAssessment2SLA
# MAGIC         WHEN t2.casephase IN ('Sign-off') THEN t2.daysInSignOff
# MAGIC         WHEN t2.casephase IN ('Product Fulfillment') THEN t2.daysInFulfillment
# MAGIC         WHEN t2.casephase IN ('QC') THEN t2.daysInQC
# MAGIC         WHEN t2.casephase IN ('Ready for QC') THEN t2.daysInReadyForQC 
# MAGIC         WHEN t2.casephase IN ('Ready for KYC assessment') THEN t2.daysInReadyForKYCAssessment
# MAGIC         WHEN t2.casephase IN ('Client Outreach Completed') THEN t2.DaysInClientOutreachCompleted
# MAGIC     END AS daysInCasePhaseSLA,
# MAGIC     COALESCE (t2.DaysInAssessment1, 0) + COALESCE(t2.daysInAssessment2SLA, 0) AS daysInAssessment,
# MAGIC     CASE
# MAGIC         WHEN (COALESCE(t2.DaysInAssessment1, 0) + COALESCE(t2.daysInAssessment2SLA, 0)) > 35 THEN 'More than 35 days'
# MAGIC         WHEN (COALESCE(t2.DaysInAssessment1, 0) + COALESCE(t2.daysInAssessment2SLA, 0)) BETWEEN 28 AND 35 THEN '28 to 35 days'
# MAGIC         WHEN (COALESCE(t2.DaysInAssessment1, 0) + COALESCE(t2.daysInAssessment2SLA, 0)) BETWEEN 0 AND 27 THEN '0 to 27 days '
# MAGIC     END AS TotalAssessment
# MAGIC     , t5.FileComplexity
# MAGIC     , t5.ComplexFileTypes
# MAGIC FROM radar.clients t1
# MAGIC INNER JOIN radar.cases t2 ON t1.SourceClient = t2.SourceClient
# MAGIC --LEFT JOIN radar.rdr_caseplanningdetailsNew1 AS t3 ON t1.SourceClient = t3.SourceClient
# MAGIC LEFT JOIN latest_sprintstatus AS t4 ON t2.CDDCaseId = t4.CaseId 
# MAGIC LEFT JOIN radar.complexfiles AS t5 ON t1.UniqueGcobId = t5.uniquegcobid

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS radar.clients_v2')

# write to intermediate table
spark.table('clients_v2').write.mode('overwrite').saveAsTable('radar.clients_v2')

# drop clients
spark.sql('DROP TABLE IF EXISTS radar.clients')


# rename the clients_v2 to clients
spark.sql('ALTER TABLE radar.clients_v2 RENAME TO radar.clients')

# COMMAND ----------

# DBTITLE 1,Duplicates?
duplicated_uniquegcobid = spark.sql('''
    SELECT UniqueGcobId 
    FROM radar.clients
    GROUP BY UniqueGcobId 
    HAVING count(*) > 1
''')

# check for duplication
if not duplicated_uniquegcobid.isEmpty():
    raise Exception('Duplicated UniqueGcobId in clients. Job stopped.')
