# Databricks notebook source
dbutils.widgets.text('date_param', '', label = None)

# COMMAND ----------

# DBTITLE 1,set up date_param
from datetime import datetime

if dbutils.widgets.get('date_param') == '': # if empty, set up default value = 20240101
    date_parameter = '20240101'
else:
    date_parameter = dbutils.widgets.get('date_param')

load_date = date_parameter
EDL_LoadDate = datetime.strptime(date_parameter, '%Y%m%d').strftime('%Y-%m-%d')

# COMMAND ----------

import os

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

# COMMAND ----------

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
]

for item in gcob_objects:
    version = 102 # change to LOAD_DT instead of EDL_LOAD_DTS

    # version = 101 # change to EDL_LOAD_DTS

    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/LOAD_DT={load_date}*/*.parquet').createOrReplaceTempView(item)

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
# MAGIC ## PortfolioPlanning

# COMMAND ----------

# DBTITLE 1,onboarding_days_diff
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
    WHERE t1.RecordActiveAt < DATE('{EDL_LoadDate}') -- consider only records prior to EDL_LoadDate

    UNION

    SELECT
        UniqueGcobId
        , DATEDIFF(InsertedOnDate, DATE('{EDL_LoadDate}')) AS days_diff
        , SectorTeam
        , ReviewLocation
        , KYCGroup
    FROM radar.onboardings
    WHERE InsertedOnDate < DATE('{EDL_LoadDate}') -- consider only records prior to EDL_LoadDate
)

SELECT
    *
    , row_number() OVER (PARTITION BY UniqueGcobId ORDER BY days_diff DESC) AS rn
FROM rn_cte

""").createOrReplaceTempView('onboarding_days_diff_historical')

# COMMAND ----------

# DBTITLE 1,latest_active_masterlistregistry_historical
spark.sql(f"""
SELECT 
  *
FROM (
  SELECT
    *
    , DATEDIFF(RecordActiveAt, DATE('{EDL_LoadDate}'))
    , ROW_NUMBER() OVER (PARTITION BY UniqueGcobId ORDER BY DATEDIFF(RecordActiveAt, DATE('{EDL_LoadDate}')) DESC) AS rn
  FROM radar.masterlistregistry
  WHERE RecordActiveAt < DATE('{EDL_LoadDate}') -- consider only record prior to EDL_LoadDate
) WHERE rn = 1 -- take only the record prior and closest to EDL_LoadDate
""").createOrReplaceTempView('latest_active_masterlistregistry_historical')

# COMMAND ----------

# DBTITLE 1,PortfolioPlanning
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW PortfolioPlanning AS
# MAGIC
# MAGIC WITH client_information_cte AS ( -- get client information from party_case_client_details (inner join with party_client on completed or latest client id) to compare them with masterlistregistry
# MAGIC   SELECT
# MAGIC     CASE 
# MAGIC       WHEN t1.ClientType = 'Legal Entity' THEN t1.GcobId
# MAGIC       ELSE CONCAT('NP_', t1.GcobId)
# MAGIC     END AS UniqueGcobId
# MAGIC     , t1.CaseId
# MAGIC     , t1.FullLegalName
# MAGIC     , t1.ClientType
# MAGIC     , t1.NextReviewDate
# MAGIC     , t1.FIHubIndicator
# MAGIC     , t1.GlobalClientOwner
# MAGIC     , t1.GlobalClientOwnerLocation
# MAGIC     , t1.BusinessLineName
# MAGIC
# MAGIC   
# MAGIC   FROM party_case_client_details t1
# MAGIC   INNER JOIN party_client t2 ON t1.ClientId = COALESCE(t2.CompletedId, t2.LatestId) AND t1.ClientType = t2.ClientType AND t1.GcobId = t2.GcobId
# MAGIC )
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
# MAGIC
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
# MAGIC
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
# MAGIC
# MAGIC   -- COMMENTING IT OUT SO MASS UPDATE IN PORTFOLIO PLANNING CAN WORK. IF IN CASE OF ISSUES, PLEASE UNCOMMENT THE BLOCK
# MAGIC   , CASE 
# MAGIC       WHEN t2.CDDExecution IS NOT NULL AND DATE_DIFF(to_date(t1.NextReviewDate, 'yyyy-MM-dd'), to_date(t2.CDDExecution, 'yyyy-MM-dd')) < 300 AND (to_date(t2.CDDExecution, 'yyyy-MM-dd') < to_date(t1.NextReviewDate, 'yyyy-MM-dd'))
# MAGIC         THEN t2.CDDExecution -- if more than 300 days difference between NRD and PAD is wrong
# MAGIC       ELSE DATE_ADD(to_date(t1.NextReviewDate, 'yyyy-MM-dd'), -69)
# MAGIC     END AS CDDExecution
# MAGIC   , t1.FIHubIndicator
# MAGIC
# MAGIC FROM client_information_cte t1
# MAGIC LEFT JOIN latest_active_masterlistregistry_historical t2 ON t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC LEFT JOIN onboarding_days_diff_historical t3 ON t1.UniqueGcobId = t3.UniqueGcobId AND t3.rn = 1

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
spark.sql(f"""
          
  WITH Prework AS (
    SELECT
      SourceClient
      , MIN(WorkItemCreatedDate) AS Prework
    FROM party_workitem
    WHERE CaseStatusTypeWhenCreated IN (1, 16)
    GROUP BY SourceClient
  )
, ReadyForAssessment AS (
    SELECT
      SourceClient
      , MIN(WorkItemCreatedDate) AS ReadyForAssessment
    FROM party_workitem
    WHERE CaseStatusTypeWhenCreated = 2
    GROUP BY SourceClient
  )
, AssessmentInProgress AS (
    SELECT
      SourceClient
      , MIN(WorkItemCreatedDate) AS AssessmentInProgress
    FROM party_workitem
    WHERE CaseStatusTypeWhenCreated = 3
    GROUP BY SourceClient
  )

, ReadyFor4EYECheck AS (
    SELECT 
      SourceClient
      , MIN(WorkItemCreatedDate) AS ReadyFor4EYECheck
    FROM party_workitem
    WHERE CaseStatusTypeWhenCreated = 4
    GROUP BY SourceClient
  )

, 4EYECheck AS (
    SELECT 
      SourceClient
      , MIN(WorkItemCreatedDate) AS 4EYECheck
    FROM party_workitem
    WHERE CaseStatusTypeWhenCreated = 5
    GROUP BY SourceClient
  )
, Signoff AS (
    SELECT 
      SourceClient
      , MIN(WorkItemCreatedDate) AS SignOff
    FROM party_workitem
    WHERE CaseStatusTypeWhenCreated IN (6, 18)
    GROUP BY SourceClient
  )
, Fulfillment AS (
    SELECT
      SourceClient
      , MIN(WorkItemCreatedDate) AS Fulfillment
    FROM party_workitem
    WHERE CaseStatusTypeWhenCreated = 8
    GROUP BY SourceClient
  )
, Completed AS (
    SELECT
      SourceClient
      , MAX(WorkItemCompletedDate) AS Completed
    FROM party_workitem  
    WHERE CaseCurrentStatus = 9
      AND WorkItemCompletedDate IS NOT NULL
    GROUP BY SourceClient
  )
, Cancelled AS (
    SELECT
      SourceClient
      , MAX(WorkItemCompletedDate) AS Cancelled
    FROM party_workitem
    WHERE CaseStatusTypeWhenCreated = 10
      AND WorkItemCompletedDate IS NOT NULL
    GROUP BY SourceClient
  )
, QCInteractions AS (
    SELECT
      SourceClient
      , COUNT(*) AS QCInteractions
    FROM party_workitem
    WHERE CaseStatusName = 'Ready for 4 eye check'
    GROUP BY SourceClient
  )
, PreworkAnalystdate AS (
    SELECT DISTINCT
      t1.SourceClient
      , CASE WHEN t1.WorkItemAssignedUserName = 'Unknown' THEN t3.WorkItemAssignedUserName ELSE t1.WorkItemAssignedUserName END AS PreworkAnalyst
      , CASE WHEN t1.WorkItemAssignedUserName = 'Unknown' THEN SUBSTRING(t1.CreatingUserID, INSTR(t1.CreatingUserID, '\\\\') + 1) ELSE SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\\\') + 1) END AS PreworkAlias
      , MaxDate AS PreworkAnalystTeamDate
    FROM party_workitem t1
    INNER JOIN (
      SELECT
        t1.SourceClient
        , MAX(WorkItemCreatedDate) AS MaxDate
      FROM party_workitem t1
      LEFT JOIN (
        SELECT
          SourceClient
          , MIN(WorkItemCreatedDate) AS ReadyForAssessment
        FROM party_workitem
        WHERE CaseStatusTypeWhenCreated = 2
        GROUP BY SourceClient
      ) t4 ON t1.SourceClient = t4.SourceClient
      WHERE t1.CaseStatusTypeWhenCreated IN (1, 16) AND WorkItemCreatedDate <= COALESCE(t4.ReadyForAssessment, DATE('{EDL_LoadDate}'))
      GROUP BY t1.SourceClient
    ) t2 ON t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
    LEFT JOIN (
      SELECT
        SourceClient
        , WorkItemAssignedUserName
        , AssignedUserId
        , CaseStatusTypeWhenCreated
        , WorkItemCreatedDate
      FROM party_workitem
      WHERE CaseStatusTypeWhenCreated IN (1)
    ) t3 ON t2.SourceClient = t3.SourceClient AND t2.MaxDate = t3.WorkItemCreatedDate
    WHERE t1.CaseStatusTypeWhenCreated IN (1, 16)
  )
, AssessmentAnalystDate AS (
    -- select LastAnalyst on StatusAssessment
    SELECT DISTINCT 
      t1.SourceClient
      , CASE WHEN t1.WorkItemAssignedUserName = 'Unknown' THEN t3.WorkItemAssignedUserName ELSE t1.WorkItemAssignedUserName END AS AssessmentAnalyst
      , SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\\\') + 1) AS AssessmentAlias
      , t2.MaxDate AS AssessmentAnalystTeamDate
    FROM party_workitem t1  
    INNER JOIN ( 
        SELECT 
          SourceClient
          , MAX(WorkItemCreatedDate) AS MaxDate 
        FROM party_workitem 
        WHERE CaseStatusTypeWhenCreated IN (3, 20, 21)
        GROUP BY SourceClient
      ) t2 ON t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
    LEFT JOIN (
      SELECT
        SourceClient
        , WorkItemAssignedUserName
        , AssignedUserId
        , CaseStatusTypeWhenCreated
        , WorkItemCreatedDate 
      FROM party_workitem 
      WHERE CaseStatusTypeWhenCreated IN (2, 19, 20)
    ) t3 ON t2.SourceClient = t3.SourceClient AND t2.MaxDate = t3.WorkItemCreatedDate
    WHERE t1.CaseStatusTypeWhenCreated IN (3, 20, 21)
  )
, 4EYEAnalystDate AS (
    SELECT DISTINCT 
      t1.SourceClient
      , t1.WorkItemAssignedUserName AS 4EYEAnalyst
      , SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\\\') + 1) AS 4EYEAlias
      , t2.MaxDate AS 4EYEAnalystTeamDate
    FROM party_workitem t1 
    INNER JOIN ( 
      SELECT 
        t3.SourceClient
        , MAX(t3.WorkItemCreatedDate) AS MaxDate 
      FROM party_workitem t3
        LEFT JOIN (
          SELECT
            SourceClient
            , MIN(WorkItemCreatedDate) AS SignOff
          FROM party_workitem 
          WHERE CaseStatusTypeWhenCreated = 6
          GROUP BY SourceClient
        ) t4 ON t3.SourceClient = t4.SourceClient 
      WHERE t3.CaseStatusTypeWhenCreated = 5 AND t3.WorkItemCreatedDate <= COALESCE(t4.SignOff, DATE('{EDL_LoadDate}'))
      GROUP BY t3.SourceClient
      ) t2 ON t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
    WHERE t1.CaseStatusTypeWhenCreated = 5
  )
, LastAnalyst AS (
    SELECT DISTINCT
      t1.SourceClient
      , t1.WorkItemAssignedUserName AS LastAnalyst
      , SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\\\') + 1) AS LastAnalystAlias
      , MaxDate AS LastAnalystTeamDate
    FROM party_workitem t1
    INNER JOIN (
      SELECT
        SourceClient
        , MAX(WorkItemCreatedDate) AS MaxDate
        , MAX(CaseStatusTypeWhenCreated) AS MaxCaseStatusTypeWhenCreated
      FROM party_workitem
      WHERE ResponsibleRole IN (2, 3)
        AND CaseStatusTypeWhenCreated NOT IN (5, 4, 2) -- ('4 eye check in progress', 'Ready for 4 eye check', 'Ready for KYC ASsessment')
      GROUP BY SourceClient
      ) t2 ON t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate AND t1.CaseStatusTypeWhenCreated = t2.MaxCaseStatusTypeWhenCreated
    WHERE ResponsibleRole IN (2, 3) AND CaseStatusTypeWhenCreated NOT IN (5, 4, 2)
  )
, WithLag AS (
  SELECT
    SourceClient,
    WorkItemCreatedDate,
    CaseStatusTypeWhenCreated,
    LAG(CaseStatusTypeWhenCreated) OVER (
      PARTITION BY SourceClient
      ORDER BY WorkItemCreatedDate
    ) AS PreviousStatus,
    LAG(WorkItemCreatedDate) OVER (
      PARTITION BY SourceClient
      ORDER BY WorkItemCreatedDate
    ) AS PreviousDate
  FROM party_workitem
),
 
-- Prework (Status = 1)
PreworkFiltered AS (
  SELECT
    SourceClient,
    WorkItemCreatedDate,
    PreviousDate,
    CaseStatusTypeWhenCreated,
    PreviousStatus
  FROM WithLag
  WHERE CaseStatusTypeWhenCreated = 1
),
PreworkFinal AS (
  SELECT
    SourceClient,
    CASE
      WHEN PreviousStatus = 1 THEN PreviousDate
      ELSE WorkItemCreatedDate
    END AS LatestPreworkDate
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY SourceClient
             ORDER BY WorkItemCreatedDate DESC
           ) AS rn
    FROM PreworkFiltered
  ) sub
  WHERE rn = 1
),
 
-- Ready for Assessment (Status = 2)
ReadyForAssessmentFiltered AS (
  SELECT
    SourceClient,
    WorkItemCreatedDate,
    PreviousDate,
    CaseStatusTypeWhenCreated,
    PreviousStatus
  FROM WithLag
  WHERE CaseStatusTypeWhenCreated = 2
),
ReadyForAssessmentFinal AS (
  SELECT
    SourceClient,
    CASE
      WHEN PreviousStatus = 2 THEN PreviousDate
      ELSE WorkItemCreatedDate
    END AS LatestReadyForAssessmentDate
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY SourceClient
             ORDER BY WorkItemCreatedDate DESC
           ) AS rn
    FROM ReadyForAssessmentFiltered
  ) sub
  WHERE rn = 1
),

-- KYC Assessment In Progress (Status = 3)
  KycAssessmentInProgressFiltered AS (
  SELECT
    SourceClient,
    WorkItemCreatedDate,
    PreviousDate,
    CaseStatusTypeWhenCreated,
    PreviousStatus
  FROM WithLag
  WHERE CaseStatusTypeWhenCreated = 3
),
KycAssessmentInProgressFinal AS (
  SELECT
    SourceClient,
    CASE
      WHEN PreviousStatus = 3 THEN PreviousDate
      ELSE WorkItemCreatedDate
    END AS LatestAssessmentInProgressDate
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY SourceClient
             ORDER BY WorkItemCreatedDate DESC
           ) AS rn
    FROM KycAssessmentInProgressFiltered
  ) sub
  WHERE rn = 1
),

-- Ready for 4 Eye Check (Status = 4)
  ReadyFor4EYECheckFiltered AS (
  SELECT
    SourceClient,
    WorkItemCreatedDate,
    PreviousDate,
    CaseStatusTypeWhenCreated,
    PreviousStatus
  FROM WithLag
  WHERE CaseStatusTypeWhenCreated = 4
),
ReadyFor4EYECheckFinal AS (
  SELECT
    SourceClient,
    CASE
      WHEN PreviousStatus = 4 THEN PreviousDate
      ELSE WorkItemCreatedDate
    END AS LatestReadyFor4EYECheckDate
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY SourceClient
             ORDER BY WorkItemCreatedDate DESC
           ) AS rn
    FROM ReadyFor4EYECheckFiltered
  ) sub
  WHERE rn = 1
),

-- Ready for 4 Eye Check (Status = 5)
4EYECheckFiltered AS (
  SELECT
    SourceClient,
    WorkItemCreatedDate,
    PreviousDate,
    CaseStatusTypeWhenCreated,
    PreviousStatus
  FROM WithLag
  WHERE CaseStatusTypeWhenCreated = 5
),
4EYECheckFinal AS (
  SELECT
    SourceClient,
    CASE
      WHEN PreviousStatus = 5 THEN PreviousDate
      ELSE WorkItemCreatedDate
    END AS Latest4EYECheckdDate
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY SourceClient
             ORDER BY WorkItemCreatedDate DESC
           ) AS rn
    FROM 4EYECheckFiltered
  ) sub
  WHERE rn = 1
),

-- Ready for Signoff (Status IN (6, 18))
SignoffFiltered AS (
  SELECT
    SourceClient,
    WorkItemCreatedDate,
    PreviousDate,
    CaseStatusTypeWhenCreated,
    PreviousStatus
  FROM WithLag
  WHERE CaseStatusTypeWhenCreated IN (6, 18)
),
SignoffFinal AS (
  SELECT
    SourceClient,
    CASE
      WHEN PreviousStatus IN (6, 18) THEN PreviousDate
      ELSE WorkItemCreatedDate
    END AS LatestSignoffDate
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY SourceClient
             ORDER BY WorkItemCreatedDate DESC
           ) AS rn
    FROM SignoffFiltered
  ) sub
  WHERE rn = 1
),

-- Ready for Fulfillment (Status = 8)
FulfillmentFiltered AS (
  SELECT
    SourceClient,
    WorkItemCreatedDate,
    PreviousDate,
    CaseStatusTypeWhenCreated,
    PreviousStatus
  FROM WithLag
  WHERE CaseStatusTypeWhenCreated = 8
),
FulfillmentFinal AS (
  SELECT
    SourceClient,
    CASE
      WHEN PreviousStatus = 8 THEN PreviousDate
      ELSE WorkItemCreatedDate
    END AS LatestFulfillmentDate
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY SourceClient
             ORDER BY WorkItemCreatedDate DESC
           ) AS rn
    FROM FulfillmentFiltered
  ) sub
  WHERE rn = 1
)

SELECT DISTINCT
  t1.ClientId
  , t1.SourceClient
  , t1.CaseId
  , t1.GcobId
  , t2.Prework
  , t10.PreworkAnalyst
  , t10.PreworkAnalystTeamDate
  , t3.ReadyForAssessment
  , t4.AssessmentInProgress
  , t11.AssessmentAnalyst
  , t11.AssessmentAnalystTeamDate
  , t5.4EYECheck
  , t12.4EYEAnalyst
  , t12.4EYEAnalystTeamDate
  , t6.SignOff
  , t7.Fulfillment
  , t8.Completed
  , t9.Cancelled

  , t15.LatestPreworkDate
  , t16.LatestReadyForAssessmentDate
  , t17.LatestAssessmentInProgressDate
  , t18.Latest4EYECheckdDate
  , t19.LatestSignoffDate
  , t20.LatestFulfillmentDate
  , t21.LatestReadyFor4EYECheckDate
  , t22.ReadyFor4EYECheck

/*
  , DATEDIFF(DAY, t2.Prework, COALESCE(t3.ReadyForAssessment, DATE('{EDL_LoadDate}'))) + 1 
    - (
        (DATEDIFF(WEEK, t2.Prework, COALESCE(t3.ReadyForAssessment, DATE('{EDL_LoadDate}'))) * 2)
        + (CASE WHEN DATE_FORMAT(t2.Prework, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
        + (CASE WHEN DATE_FORMAT(t3.ReadyForAssessment, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS DaysInPrework

  , DATEDIFF(DAY, t3.ReadyForAssessment, COALESCE(t4.AssessmentInProgress, DATE('{EDL_LoadDate}'))) + 1 
    - (
        (DATEDIFF(WEEK, t3.ReadyForAssessment, COALESCE(t4.AssessmentInProgress, DATE('{EDL_LoadDate}'))) * 2) 
        + (CASE WHEN DATE_FORMAT(t3.ReadyForAssessment, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
        + (CASE WHEN DATE_FORMAT(t4.AssessmentInProgress, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS DaysInReadyForAssessment

  , DATEDIFF(DAY, t4.AssessmentInProgress, COALESCE(t5.4EYECheck, DATE('{EDL_LoadDate}'))) + 1 
    - (
        (DATEDIFF(WEEK, t4.AssessmentInProgress, COALESCE(t5.4EYECheck, DATE('{EDL_LoadDate}'))) * 2) 
        + (CASE WHEN DATE_FORMAT(t4.AssessmentInProgress, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
        + (CASE WHEN DATE_FORMAT(t5.4EYECheck, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS DaysInAssessmentInProgress
  
  , DATEDIFF(DAY, t5.4EYECheck, COALESCE(t6.SignOff, DATE('{EDL_LoadDate}'))) + 1
    - (
        (DATEDIFF(WEEK, t5.4EYECheck, COALESCE(t6.SignOff, DATE('{EDL_LoadDate}'))) * 2) 
        + (CASE WHEN DATE_FORMAT(t5.4EYECheck, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
        + (CASE WHEN DATE_FORMAT(t6.SignOff, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS DaysIn4EYECheck
  
  , DATEDIFF(DAY, t6.SignOff, COALESCE(t7.Fulfillment, DATE('{EDL_LoadDate}'))) + 1 
    - (
        (DATEDIFF(WEEK, t6.SignOff, COALESCE(t7.Fulfillment, DATE('{EDL_LoadDate}'))) * 2) 
        + (CASE WHEN DATE_FORMAT(t6.SignOff, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
        + (CASE WHEN DATE_FORMAT(t7.Fulfillment, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS DaysInSignOff
  
  , DATEDIFF(DAY, t7.Fulfillment, COALESCE(t8.Completed, DATE('{EDL_LoadDate}'))) + 1 
    - (
        (DATEDIFF(WEEK, t7.Fulfillment, COALESCE(t8.Completed, DATE('{EDL_LoadDate}'))) * 2) 
        + (CASE WHEN DATE_FORMAT(t7.Fulfillment, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
        + (CASE WHEN DATE_FORMAT(t8.Completed, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS DaysInFulfillment
  */
  --------------------------------------new logic for days in a current case phase start-----------------------------------------------------------

  , DATEDIFF(DAY, t15.LatestPreworkDate, COALESCE(t16.LatestReadyForAssessmentDate, DATE('{EDL_LoadDate}'))) + 1 
    - (
        (DATEDIFF(WEEK, t15.LatestPreworkDate, COALESCE(t16.LatestReadyForAssessmentDate, DATE('{EDL_LoadDate}'))) * 2)
        + (CASE WHEN DATE_FORMAT(t15.LatestPreworkDate, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
        + (CASE WHEN DATE_FORMAT(t16.LatestReadyForAssessmentDate, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS DaysInPrework

  , DATEDIFF(DAY, t16.LatestReadyForAssessmentDate, COALESCE(t17.LatestAssessmentInProgressDate, DATE('{EDL_LoadDate}'))) + 1 
    - (
        (DATEDIFF(WEEK, t16.LatestReadyForAssessmentDate, COALESCE(t17.LatestAssessmentInProgressDate, DATE('{EDL_LoadDate}'))) * 2) 
        + (CASE WHEN DATE_FORMAT(t16.LatestReadyForAssessmentDate, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
        + (CASE WHEN DATE_FORMAT(t17.LatestAssessmentInProgressDate, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS DaysInReadyForAssessment

  , DATEDIFF(DAY, t17.LatestAssessmentInProgressDate, COALESCE(t21.LatestReadyFor4EYECheckDate, DATE('{EDL_LoadDate}'))) + 1 
    - (
        (DATEDIFF(WEEK, t17.LatestAssessmentInProgressDate, COALESCE(t21.LatestReadyFor4EYECheckDate, DATE('{EDL_LoadDate}'))) * 2) 
        + (CASE WHEN DATE_FORMAT(t17.LatestAssessmentInProgressDate, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
        + (CASE WHEN DATE_FORMAT(t21.LatestReadyFor4EYECheckDate, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS DaysInAssessmentInProgress

  , DATEDIFF(
        DAY,
        t21.LatestReadyFor4EYECheckDate,
        COALESCE(
          CASE
            WHEN t18.Latest4EYECheckdDate > t21.LatestReadyFor4EYECheckDate
              THEN t18.Latest4EYECheckdDate
          END,
          DATE('{EDL_LoadDate}')
        )
    ) + 1
    - (
        (DATEDIFF(
            WEEK,
            t21.LatestReadyFor4EYECheckDate,
            COALESCE(
              CASE
                WHEN t18.Latest4EYECheckdDate > t21.LatestReadyFor4EYECheckDate
                  THEN t18.Latest4EYECheckdDate
              END,
              DATE('{EDL_LoadDate}')
            )
          ) * 2)
        + (CASE
            WHEN DATE_FORMAT(t21.LatestReadyFor4EYECheckDate, 'EEEE') = 'Sunday'
              THEN 1 ELSE 0
          END)
        + (CASE
            WHEN DATE_FORMAT(
                    COALESCE(
                      CASE
                        WHEN t18.Latest4EYECheckdDate > t21.LatestReadyFor4EYECheckDate
                          THEN t18.Latest4EYECheckdDate
                      END,
                      DATE('{EDL_LoadDate}')
                    ),
                    'EEEE'
                ) = 'Saturday'
              THEN 1 ELSE 0
          END)
      ) AS DaysInReadyFor4EYECheck

  , DATEDIFF(DAY, t18.Latest4EYECheckdDate, COALESCE(t19.LatestSignoffDate, DATE('{EDL_LoadDate}'))) + 1
    - (
        (DATEDIFF(WEEK, t18.Latest4EYECheckdDate, COALESCE(t19.LatestSignoffDate, DATE('{EDL_LoadDate}'))) * 2) 
        + (CASE WHEN DATE_FORMAT(t18.Latest4EYECheckdDate, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
        + (CASE WHEN DATE_FORMAT(t19.LatestSignoffDate, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS DaysIn4EYECheck
  
  , DATEDIFF(DAY, t19.LatestSignoffDate, COALESCE(t20.LatestFulfillmentDate, DATE('{EDL_LoadDate}'))) + 1 
    - (
        (DATEDIFF(WEEK, t19.LatestSignoffDate, COALESCE(t20.LatestFulfillmentDate, DATE('{EDL_LoadDate}'))) * 2) 
        + (CASE WHEN DATE_FORMAT(t19.LatestSignoffDate, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
        + (CASE WHEN DATE_FORMAT(t20.LatestFulfillmentDate, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS DaysInSignOff
  
  , DATEDIFF(DAY, t20.LatestFulfillmentDate, COALESCE(t8.Completed, DATE('{EDL_LoadDate}'))) + 1 
    - (
        (DATEDIFF(WEEK, t20.LatestFulfillmentDate, COALESCE(t8.Completed, DATE('{EDL_LoadDate}'))) * 2) 
        + (CASE WHEN DATE_FORMAT(t20.LatestFulfillmentDate, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
        + (CASE WHEN DATE_FORMAT(t8.Completed, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS DaysInFulfillment

  --------------------------------------new logic for days in a current case phase end-----------------------------------------------------------
  , DATEDIFF(DAY, t2.Prework, COALESCE(t8.Completed, DATE('{EDL_LoadDate}'))) + 1 
    - (
        (DATEDIFF(WEEK, t2.Prework, COALESCE(t8.Completed, DATE('{EDL_LoadDate}'))) * 2) 
        + (CASE WHEN DATE_FORMAT(t2.Prework, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
        + (CASE WHEN DATE_FORMAT(t8.Completed, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ) AS TotalCaseDuration  
  
  , CASE
      WHEN t9.Cancelled IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 8)
      WHEN t8.Completed IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 7)
      WHEN t7.Fulfillment IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 6)
      WHEN t6.SignOff IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 5)
      WHEN t5.4EYECheck IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 4)
      WHEN t4.AssessmentInProgress IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 3)
      WHEN t3.ReadyForAssessment IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 2)
      WHEN t2.Prework IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 1)
      ELSE 'UNKNOWN'
    END AS CasePhase
, CASE
    WHEN t9.Cancelled IS NOT NULL THEN 8
    WHEN t8.Completed IS NOT NULL THEN 7
    WHEN t7.Fulfillment IS NOT NULL THEN 6
    WHEN t6.SignOff IS NOT NULL THEN 5
    WHEN t5.4EYECheck IS NOT NULL THEN 4
    WHEN t4.AssessmentInProgress IS NOT NULL THEN 3
    WHEN t3.ReadyForAssessment IS NOT NULL THEN 2
    WHEN t2.Prework IS NOT NULL THEN 1
    ELSE 99999
  END AS CasePhaseSortOrder
, t13.QCInteractions
, t14.LastAnalyst
, t14.LastAnalystTeamDate
, t10.PreworkAlias
, t11.AssessmentAlias
, t12.4EYEAlias
, t14.LastAnalystAlias

FROM party_workitem t1
LEFT JOIN Prework t2 ON t1.SourceClient = t2.SourceClient
LEFT JOIN Readyforassessment t3 ON t1.SourceClient = t3.SourceClient
LEFT JOIN AssessmentInProgress t4 ON t1.SourceClient = t4.SourceClient
LEFT JOIN 4EYECheck t5 ON t1.SourceClient = t5.SourceClient
LEFT JOIN Signoff t6 ON t1.SourceClient = t6.SourceClient
LEFT JOIN Fulfillment t7 ON t1.SourceClient = t7.SourceClient
LEFT JOIN Completed t8 ON t1.SourceClient = t8.SourceClient
LEFT JOIN Cancelled t9 ON t1.SourceClient = t9.SourceClient
LEFT JOIN PreworkAnalystdate t10 ON t1.SourceClient = t10.SourceClient
LEFT JOIN AssessmentAnalystDate t11 ON t1.SourceClient = t11.SourceClient
LEFT JOIN 4EYEAnalystDate t12 ON t1.SourceClient = t12.SourceClient
LEFT JOIN QCInteractions t13 ON t1.SourceClient = t13.SourceClient
LEFT JOIN LastAnalyst t14 ON t1.SourceClient = t14.SourceClient

LEFT JOIN PreworkFinal t15 ON t1.SourceClient = t15.SourceClient
LEFT JOIN ReadyForAssessmentFinal t16 ON t1.SourceClient = t16.SourceClient
LEFT JOIN KycAssessmentInProgressFinal t17 ON t1.SourceClient = t17.SourceClient
LEFT JOIN 4EYECheckFinal t18 ON t1.SourceClient = t18.SourceClient
LEFT JOIN SignoffFinal t19 ON t1.SourceClient = t19.SourceClient
LEFT JOIN FulfillmentFinal t20 ON t1.SourceClient = t20.SourceClient
LEFT JOIN ReadyFor4EYECheckFinal t21 ON t1.SourceClient = t21.SourceClient
LEFT JOIN ReadyFor4EYECheck t22 ON t1.SourceClient = t22.SourceClient
""").createOrReplaceTempView('workitems_status')

# COMMAND ----------

# DBTITLE 1,rfi_status
spark.sql(f"""
          
SELECT DISTINCT
  t1.GcobId
  , t1.CaseId
  , t1.ClientId
  , t1.SourceClient

  , COALESCE(t3.NumberOfRFI, 0) AS NumberOfRFI
  , CASE 
      WHEN t1.CasePhase IN ('Cancelled', 'Completed') THEN t1.CasePhase
      WHEN t3.RequestStatusNumber IN (1) AND t3.NumberOfRFI = 1 THEN 'Client Outreach'
      WHEN t3.RequestStatusNumber IN (1) AND t3.NumberOfRFI > 1 THEN 'Rebound Client Outreach'
      WHEN t3.RequestStatusNumber = 2 THEN 'Client Outreach Completed'
    END AS CasePhase
  , CASE
      WHEN t1.CasePhase <> 'Assessment in progress' THEN t1.CasePhase
      WHEN t1.SourceClient IS NULL THEN 'Execute Assessment' -- t2.CaseId
      WHEN t3.NumberOfRFI = 1 AND t3.RequestStatusNumber = 1 THEN 'Waiting for information'
      WHEN t3.NumberOfRFI = 1 AND t3.RequestStatusNumber = 2 THEN 'Reviewing information'
      WHEN t3.NumberOfRFI >= 1 AND t3.RequestStatusNumber = 3 THEN 'Finalise assessment'
      WHEN t3.NumberOfRFI > 1 AND t3.RequestStatusNumber = 1 THEN 'Waiting for additional information'
      WHEN t3.NumberOfRFI > 1 AND t3.RequestStatusNumber = 2 THEN 'Reviewing additional information'
    END AS CasePhaseIncludingRFI
  , CASE
      WHEN t1.CasePhase <> 'Assessment in progress' THEN t1.CasePhaseSortOrder
      WHEN t1.SourceClient IS NULL THEN 3.1 -- t2.CaseId
      WHEN t3.NumberOfRFI = 1 AND t3.RequestStatusNumber = 1 THEN 3.2
      WHEN t3.NumberOfRFI = 1 AND t3.RequestStatusNumber = 2 THEN 3.2
      WHEN t3.NumberOfRFI >= 1 AND t3.RequestStatusNumber = 3 THEN 3.3
      WHEN t3.NumberOfRFI > 1 AND t3.RequestStatusNumber = 1 THEN 3.4
      WHEN t3.NumberOfRFI > 1 AND t3.RequestStatusNumber = 2 THEN 3.5
    END AS CasePhaseIncludingRFISortOrder
  , t1.ReadyForAssessment

  , DATEDIFF(DAY, t1.ReadyForAssessment, t4.DateCreated) + 1
    - (
        (DATEDIFF(WEEK, t1.ReadyForAssessment, t4.DateCreated) * 2)
        + (CASE WHEN DATE_FORMAT(t1.ReadyForAssessment, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
        + (CASE WHEN DATE_FORMAT(t4.DateCreated, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
      ) 
    AS DaysInExecuteAssessment

  , t4.DateCreated AS DateCreated

  , DATEDIFF(DAY, t4.DateCreated, COALESCE(t4.DateResponded, DATE('{EDL_LoadDate}'))) + 1
    - (
        (DATEDIFF(WEEK, t4.DateCreated, COALESCE(t4.DateResponded, DATE('{EDL_LoadDate}'))) * 2)
        + (CASE WHEN DATE_FORMAT(t4.DateCreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
        + (CASE WHEN DATE_FORMAT(t4.DateResponded, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
      ) 
    AS DaysInWaitingForInformation

  , t4.DateResponded AS DateResponded

  , DATEDIFF(DAY, t4.DateResponded, COALESCE(t4.DateCompleted, DATE('{EDL_LoadDate}'))) + 1
    - (
        (DATEDIFF(WEEK, t4.DateResponded, COALESCE(t4.DateCompleted, DATE('{EDL_LoadDate}'))) * 2)
        + (CASE WHEN DATE_FORMAT(t4.DateResponded, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
        + (CASE WHEN DATE_FORMAT(t4.DateCompleted, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
      ) 
    AS DaysInReviewingInformation

  , t4.DateCompleted AS DateCompleted

  , CASE 
      WHEN t4.ClientId IS NOT NULL THEN 
        DATEDIFF(DAY, t4.DateCompleted, t1.4EYECheck) + 1 
        - (
            (DATEDIFF(WEEK, t4.DateCompleted, t1.4EYECheck) * 2) 
            + (CASE WHEN DATE_FORMAT(t4.DateCompleted, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
            + (CASE WHEN DATE_FORMAT(t1.4EYECheck, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
        )
      WHEN t4.ClientId IS NULL THEN 
        DATEDIFF(DAY, t1.AssessmentInProgress, t1.4EYECheck) + 1
        - (
            (DATEDIFF(WEEK, t1.AssessmentInProgress, t1.4EYECheck) * 2)
            + (CASE WHEN DATE_FORMAT(t1.AssessmentInProgress, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
            + (CASE WHEN DATE_FORMAT(t1.4EYECheck, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
        )
      ELSE NULL
    END + 1 AS DaysInFinaliseAssessment

  , CASE WHEN t4.DateCompleted IS NOT NULL THEN 'Yes' ELSE 'No' END AS RequestCompleted
  , t1.4EYECheck


FROM workitems_status t1
-- LEFT JOIN party_request_for_information t2 ON t1.GcobId = t2.GcobId AND t1.ClientId = t2.ClientId AND t1.CaseId = t2.CaseId
/*
LEFT JOIN (
  SELECT DISTINCT
    ClientId
    , Gcobid
    , MIN(DateCreated) AS MinDateCreated
    , COUNT(RequestTypeId) AS NumberOfRFI 
    , MIN(CASE WHEN StatusType = 'PendingResponse' THEN 1 WHEN StatusType = 'PendingReview' THEN 2 WHEN StatusType = 'Completed' THEN 3 END) As RequestStatusNumber
  FROM party_request_for_information 
  GROUP BY 1,2
) t3 ON t1.ClientId = t3.ClientId AND t1.GcobId = t3.GcobId
LEFT JOIN (
  SELECT * 
  FROM (
    SELECT 
      *
      , ROW_NUMBER() OVER (PARTITION BY ClientId, Gcobid ORDER BY RequestTypeId, DateCreated DESC) as rn
    FROM party_request_for_information
    WHERE DateCreated IS NOT NULL AND StatusType <> 'Cancelled'
  ) WHERE rn = 1
) t4 ON t1.ClientId = t4.ClientId AND t1.GcobId = t4.GcobId AND t3.MinDateCreated = t4.DateCreated
*/

LEFT JOIN (
  SELECT DISTINCT
    ClientId
    , Gcobid
    -- changing it due to error in Assessment 1. When RFI is closed, casephase should be Assessment 2.
    -- with MIN(DateCreated). When RFI is closed casePhase is Assessment 1 (which is not correct)
    --, MIN(DateCreated) AS MinDateCreated
    , LEFT(MAX(DateCreated), 16) AS MinDateCreated
    , COUNT(RequestTypeId) AS NumberOfRFI 
    , MIN(CASE WHEN StatusType = 'PendingResponse' THEN 1 WHEN StatusType = 'PendingReview' THEN 2 WHEN StatusType = 'Completed' THEN 3 END) As RequestStatusNumber
  FROM party_request_for_information 
  GROUP BY 1,2
) t3 ON t1.ClientId = t3.ClientId AND t1.GcobId = t3.GcobId

LEFT JOIN (
  SELECT * 
  FROM (
    SELECT 
      *
      , ROW_NUMBER() OVER (PARTITION BY ClientId, Gcobid ORDER BY RequestTypeId, DateCreated DESC) as rn
    FROM party_request_for_information
    WHERE DateCreated IS NOT NULL AND StatusType <> 'Cancelled'
  ) subquery WHERE rn = 1
) t4 ON t1.ClientId = t4.ClientId AND t1.GcobId = t4.GcobId AND t3.MinDateCreated = LEFT(t4.DateCreated, 16)

""").createOrReplaceTempView('rfi_status')

# COMMAND ----------

# DBTITLE 1,workitems_status_final_OLD
# MAGIC %sql
# MAGIC /*          
# MAGIC WITH DurationInfoRequestInWorkingDay_CTE AS (
# MAGIC   SELECT
# MAGIC     ClientId
# MAGIC     , CaseId
# MAGIC     , GcobId
# MAGIC     , StatusType
# MAGIC     , DATEDIFF(DAY, DateCreated, COALESCE(DateCompleted, DATE('{EDL_LoadDate}'))) + 1 
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, DateCreated, COALESCE(DateCompleted, DATE('{EDL_LoadDate}'))) * 2) 
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
# MAGIC       WHEN t2.CaseStatusName NOT IN ('Cancelled', 'Completed') AND DATE('{EDL_LoadDate}') >= DATE_ADD(t1.Prework, 90) -- 'Migrated'
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
# MAGIC
# MAGIC LEFT JOIN radar.userlistmapping t6um ON t1.4EYEAnalyst = t6um.UserNameOld -- mapping table 
# MAGIC LEFT JOIN radar.UserTeamRegistry t6 ON COALESCE(t6um.UserNameNew, t1.4EYEAnalyst) = t6.userName AND t1.4EYEAnalystTeamDate >= t6.TeamStartDate AND t1.4EYEAnalystTeamDate <= COALESCE(t6.TeamEndDate, DATE('{EDL_LoadDate}'))
# MAGIC LEFT JOIN radar.userlistmapping t7um ON t1.PreworkAnalyst = t7um.UserNameOld -- mapping table
# MAGIC LEFT JOIN radar.UserTeamRegistry t7 ON COALESCE(t7um.UserNameNew, t1.PreworkAnalyst) = t7.userName AND t1.PreworkAnalystTeamDate >= t7.TeamStartDate AND t1.PreworkAnalystTeamDate <= COALESCE(t7.TeamEndDate, DATE('{EDL_LoadDate}'))
# MAGIC LEFT JOIN radar.userlistmapping t8um ON t1.AssessmentAnalyst = t8um.UserNameOld -- mapping table
# MAGIC LEFT JOIN radar.UserTeamRegistry t8 ON COALESCE(t8um.UserNameNew,  t1.AssessmentAnalyst) = t8.userName AND t1.AssessmentAnalystTeamDate >= t8.TeamStartDate AND t1.AssessmentAnalystTeamDate <= COALESCE(t8.TeamEndDate, DATE('{EDL_LoadDate}'))
# MAGIC LEFT JOIN radar.userlistmapping t9um ON t1.LastAnalyst = t9um.UserNameOld -- mapping table
# MAGIC LEFT JOIN radar.UserTeamRegistry t9 ON COALESCE(t9um.UserNameNew, t1.LastAnalyst) = t9.userName AND t1.LastAnalystTeamDate >= t9.TeamStartDate AND t1.LastAnalystTeamDate <= COALESCE(t9.TeamEndDate, DATE('{EDL_LoadDate}'))
# MAGIC
# MAGIC LEFT JOIN CTE_LatestNextReviewDate t10 ON t2.GcobId = t10.GcobId AND t2.ClientType = t10.ClientType AND t2.CaseId = t10.CaseId
# MAGIC */
# MAGIC

# COMMAND ----------

# DBTITLE 1,workitems_status_final_temp
spark.sql(f"""
WITH DurationInfoRequestInWorkingDay_CTE AS (
  SELECT
    ClientId
    , CaseId
    , GcobId
    , StatusType
    , DATEDIFF(DAY, DateCreated, COALESCE(DateCompleted, DATE('{EDL_LoadDate}'))) + 1 
      - (
          (DATEDIFF(WEEK, DateCreated, COALESCE(DateCompleted, DATE('{EDL_LoadDate}'))) * 2) 
          + (CASE WHEN DATE_FORMAT(DateCreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END) 
          + (CASE WHEN DATE_FORMAT(DateCompleted, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
      ) AS DurationInfoRequestInWorkingDay
  FROM party_request_for_information
)
, CTE_ReboundClientOutreach AS (
    SELECT
      t1.SourceClient
      , MIN(t2.DurationInfoRequestInWorkingDay) AS DurationInfoRequestInWorkingDay
    FROM rfi_status t1
    LEFT JOIN DurationInfoRequestInWorkingDay_CTE t2 ON t1.GcobId = t2.GcobId AND t1.ClientId = t2.ClientId AND t1.CaseId = t2.CaseId
    WHERE 1=1
      AND t1.RequestCompleted = 'No'
      AND t2.StatusType = 'PendingResponse'
    GROUP BY 1
)
, CTE_LatestNextReviewDate AS (
  -- get the NextReviewDate per GcobId corresponding to the latest non cancelled caseid
    SELECT 
      t1.GcobId
      , t1.ClientType
      , t1.CaseId
      , t1.NextReviewDate
    FROM party_case_client_details t1
    WHERE t1.CaseStatusName <> 'Cancelled'
    AND t1.CaseId = (
        SELECT
          MAX(t2.CaseId)
        FROM party_case_client_details t2
        WHERE t2.GcobId = t1.GcobId
          AND t2.ClientType = t1.ClientType
          AND t2.CaseStatusName <> 'Cancelled'
    )
)

, second_rfi_creation_date AS (
  SELECT 
    ClientId,
    GcobId,
    DateCreated,
    ROW_NUMBER() OVER (PARTITION BY ClientId, GcobId ORDER BY DateCreated ASC) AS rn
  FROM party_request_for_information
  WHERE RequestTypeID = 1
)

SELECT DISTINCT
  t1.ClientId
  , t1.SourceClient
  , t1.CaseId
  , t1.Prework
  , t1.PreworkAnalyst
  , t1.PreworkAnalystTeamDate
  , t1.ReadyForAssessment
  , t1.AssessmentInProgress
  , t1.AssessmentAnalyst
  , t1.AssessmentAnalystTeamDate
  , t1.4EYECheck
  , t1.4EYEAnalyst
  , t1.4EYEAnalystTeamDate
  , t1.SignOff
  , t1.Fulfillment
  , t1.Completed
  , t1.Cancelled
    /*
  creating new fields to count the number of days in each casephase depending on each occurance of that workitem
  , t1.DaysInPrework
  , t1.DaysInReadyForAssessment
  , t1.DaysInAssessmentInProgress
  , t1.DaysIn4EYECheck
  , t1.DaysInSignOff
  , t1.DaysInFulfillment
   */
  , t1.TotalCaseDuration
  , t1.QCInteractions
  , t1.LastAnalyst
  , t1.LastAnalystTeamDate
  , t1.PreworkAlias
  , t1.AssessmentAlias
  , t1.4EYEAlias
  , t1.LastAnalystAlias
  , t3.DateResponded AS RespondedDateFirstRFI
  , t3.DateCompleted AS CompletedDateFirstRFI
  , (SELECT MIN(RFI.DateCreated) FROM party_request_for_information RFI WHERE t1.ClientId = RFI.ClientId AND t1.GcobId = RFI.GcobId AND RFI.RequestTypeID = 1) AS FirstRFICreated -- 1 = 'Request For Information'
  , (SELECT MAX(RFI.DateCompleted) FROM party_request_for_information RFI WHERE t1.ClientId = RFI.ClientId AND t1.GcobId = RFI.GcobId AND RFI.RequestTypeID = 1) AS LatestRFICompleted,
  t4.MainCaseStatusType,
  t2.ReviewTypeName,
  t3.DateCreated as CreatedDateRFI,
  (SELECT MIN(RFI.DateCompleted) FROM party_request_for_information RFI WHERE t1.ClientId = RFI.ClientId AND t1.GcobId = RFI.GcobId AND RFI.RequestTypeID = 1) AS FirstRFICompleted 
, rfi.DateCreated AS SecondRFICreatedDate
, t1.ReadyFor4EYECheck
, CASE
    WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled') THEN t4.MainCaseStatusType
    WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN 'Offboarding'
    WHEN t4.MainCaseStatusType = 'Sign-off' THEN 'Sign-off'
    WHEN t3.CasePhase IN ('Rebound Client Outreach', 'Client Outreach', 'Client Outreach Completed') THEN t3.CasePhase
    WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 'Rebound Assessment'
    WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 'Assessment 1'
    WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 'Assessment 2'
    WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 'Rebound Initiation'
    ELSE t4.MainCaseStatusType
  END AS CasePhase

, CASE
    WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled') THEN 14
    WHEN t4.MainCaseStatusType = 'Sign-off' THEN 12
    WHEN t3.CasePhase = 'Client Outreach' THEN 5
    WHEN t3.CasePhase = 'Rebound Client Outreach' THEN 8
    WHEN t3.CasePhase = 'Client Outreach Completed' THEN 6
    WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 11
    WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 4
    WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 7
    WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 2
    ELSE t4.MainCaseStatusSortOrder
  END AS CasePhaseSortOrder

, CASE
    WHEN t2.ReviewTypeName LIKE '%Offboarding%' AND t4.MainCaseStatusType NOT IN ('Completed', 'Cancelled') THEN 'Offboarding'
    WHEN YEAR(t10.NextReviewDate) > YEAR(GETDATE()) THEN 'Completed'
    WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled', 'KYC assessment in progress') AND YEAR(t10.NextReviewDate) < YEAR(GETDATE()) + 1 THEN 'Not yet started'
    WHEN t4.MainCaseStatusType = 'Sign-off' THEN 'Sign-off'
    WHEN t3.CasePhase IN ('Rebound Client Outreach', 'Client Outreach', 'Client Outreach Completed') THEN t3.CasePhase
    WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 'Rebound Assessment'
    WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 'Assessment 1'
    WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 'Assessment 2'
    WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 'Rebound Initiation'
    ELSE t4.MainCaseStatusType
  END AS CasePhaseCurrentYear
, CASE
    WHEN YEAR(t10.NextReviewDate) > YEAR(GETDATE()) THEN 14
    WHEN t4.MainCaseStatusType IN ('Completed', 'Cancelled', 'KYC assessment in progress') AND YEAR(t10.NextReviewDate) < YEAR(GETDATE()) + 1 THEN 0
    WHEN t4.MainCaseStatusType = 'Sign-off' THEN 12
    WHEN t3.CasePhase = 'Client Outreach' THEN 5
    WHEN t3.CasePhase = 'Rebound Client Outreach' THEN 8
    WHEN t3.CasePhase = 'Client Outreach Completed' THEN 6
    WHEN t4.MainCaseStatusType = 'Assessment' AND t1.4EYECheck IS NOT NULL THEN 11
    WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NULL THEN 4
    WHEN t4.MainCaseStatusType = 'Assessment' AND t3.DateCreated IS NOT NULL THEN 7
    WHEN t4.MainCaseStatusType = 'Initiation' AND t1.AssessmentInProgress IS NOT NULL THEN 2
    ELSE t4.MainCaseStatusSortOrder
  END AS CasePhaseSortCurrentYear

, CASE
    WHEN t3.CasePhase = 'Client Outreach' THEN t3.DaysInWaitingForInformation
    WHEN t3.CasePhase = 'Rebound Client Outreach' THEN t5.DurationInfoRequestInWorkingDay
    WHEN t1.CasePhase = 'Prework' THEN t1.DaysInPrework
    WHEN t1.CasePhase = 'Ready for assessment' THEN t1.DaysInReadyForAssessment
    WHEN t4.MainCaseStatusType = 'Ready for QC' THEN t1.DaysInReadyFor4EYECheck
    WHEN t1.CasePhase = 'Assessment in progress' THEN t1.DaysInAssessmentInProgress
    WHEN t1.CasePhase = '4-EYE check' THEN t1.DaysIn4EYECheck
    WHEN t1.CasePhase = 'Fulfilment' THEN t1.DaysInFulfillment
    WHEN t1.CasePhase = 'Sign-off' THEN t1.DaysInSignOff
  END AS DaysInCurrentCasePhaseTemp

, CASE
    WHEN t2.CaseStatusName NOT IN ('Cancelled', 'Completed') AND DATE('{EDL_LoadDate}') > TO_DATE(ADD_MONTHS(t1.Prework, 3), 'yyyy-MM-dd') -- 'Migrated'
      AND t2.ReviewTypeName = 'Event Driven Review' THEN 'Yes' ELSE 'No'
  END AS EDROverdue
-- , CASE 
--     WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN t1.Completed
--     ELSE t1.SignOff
--   END AS SignoffDateTC
, CASE 
    WHEN t2.ValidatedRiskLevel IN ('High', 'Unacceptable') THEN DATE_ADD(CAST(t10.NextReviewDate AS DATE), 60) -- Adding 60 days for 2 months
    WHEN t2.ReviewTypeName = 'Event Driven Review' THEN DATE_ADD(t1.Prework, 90)
  ELSE t10.NextReviewDate END AS TCNRD
-- , t6.UserTeam AS 4EyeUserTeam
, t6.Team AS 4EyeUserTeam -- UserTeam
, t6.Department AS 4EyeDepartment
, t7.Department AS PreworkDepartment
-- , CASE 
--     WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN COALESCE(t8.UserTeam, t7.UserTeam) 
--     WHEN t1.CasePhase = 'Prework' THEN t7.UserTeam
--     WHEN t1.CasePhase = 'Ready for assessment' THEN t7.UserTeam
--     WHEN t1.CasePhase = 'Assessment in progress' THEN t8.UserTeam
--     WHEN t1.CasePhase = '4-EYE check' THEN t8.UserTeam
--     WHEN t1.CasePhase = 'Fulfilment' THEN t8.UserTeam
--     WHEN t1.CasePhase = 'Sign-off' THEN t8.UserTeam
--     WHEN t1.CasePhase = 'Completed' THEN COALESCE(t8.UserTeam, t9.UserTeam)
--     WHEN t1.CasePhase = 'Cancelled' THEN t8.UserTeam
--   END AS KYCUserTeam
, CASE 
    WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN COALESCE(t8.Team, t7.Team) -- UserTeam
    WHEN t1.CasePhase = 'Prework' THEN t7.Team -- UserTeam
    WHEN t1.CasePhase = 'Ready for assessment' THEN t7.Team -- UserTeam
    WHEN t1.CasePhase = 'Assessment in progress' THEN t8.Team -- UserTeam
    WHEN t1.CasePhase = '4-EYE check' THEN t8.Team -- UserTeam
    WHEN t1.CasePhase = 'Fulfilment' THEN t8.Team -- UserTeam
    WHEN t1.CasePhase = 'Sign-off' THEN t8.Team -- UserTeam
    WHEN t1.CasePhase = 'Completed' THEN COALESCE(t8.Team, t9.Team) -- UserTeam
    WHEN t1.CasePhase = 'Cancelled' THEN t8.Team -- UserTeam
  END AS KYCUserTeam
, CASE 
    WHEN t2.ReviewTypeName LIKE '%Offboarding%' THEN COALESCE(t8.Department, t7.Department) 
    WHEN t1.CasePhase = 'Prework' THEN t7.Department
    WHEN t1.CasePhase = 'Ready for assessment' THEN t7.Department
    WHEN t1.CasePhase = 'Assessment in progress' THEN t8.Department
    WHEN t1.CasePhase = '4-EYE check' THEN t8.Department
    WHEN t1.CasePhase = 'Fulfilment' THEN t8.Department
    WHEN t1.CasePhase = 'Sign-off' THEN t8.Department
    WHEN t1.CasePhase = 'Completed' THEN COALESCE(t8.Department, t9.Department)
    WHEN t1.CasePhase = 'Cancelled' THEN t8.Department
  END AS KYCDepartment

, t7.Team AS PreworkUserTeam -- UserTeam 
, t8.Department AS CDDDepartment
, t8.Team AS CDDUserTeam -- UserTeam

FROM workitems_status t1
INNER JOIN party_case_client_details t2 ON t1.SourceClient = t2.SourceClient
LEFT JOIN rfi_status t3 ON t1.SourceClient = t3.SourceClient
LEFT JOIN main_case_status_type t4 ON t1.SourceClient = t4.SourceClient
LEFT JOIN CTE_ReboundClientOutreach t5 ON t1.SourceClient = t5.SourceClient


LEFT JOIN radar.userlistmapping t6um ON t1.4EYEAnalyst = t6um.UserNameOld -- mapping table 
LEFT JOIN radar.UserTeamRegistry t6 ON COALESCE(t6um.UserNameNew, t1.4EYEAnalyst) = t6.userName AND t1.4EYEAnalystTeamDate >= t6.TeamStartDate AND t1.4EYEAnalystTeamDate <= COALESCE(t6.TeamEndDate, DATE('{EDL_LoadDate}'))
LEFT JOIN radar.userlistmapping t7um ON t1.PreworkAnalyst = t7um.UserNameOld -- mapping table
LEFT JOIN radar.UserTeamRegistry t7 ON COALESCE(t7um.UserNameNew, t1.PreworkAnalyst) = t7.userName AND t1.PreworkAnalystTeamDate >= t7.TeamStartDate AND t1.PreworkAnalystTeamDate <= COALESCE(t7.TeamEndDate, DATE('{EDL_LoadDate}'))
LEFT JOIN radar.userlistmapping t8um ON t1.AssessmentAnalyst = t8um.UserNameOld -- mapping table
LEFT JOIN radar.UserTeamRegistry t8 ON COALESCE(t8um.UserNameNew,  t1.AssessmentAnalyst) = t8.userName AND t1.AssessmentAnalystTeamDate >= t8.TeamStartDate AND t1.AssessmentAnalystTeamDate <= COALESCE(t8.TeamEndDate, DATE('{EDL_LoadDate}'))
LEFT JOIN radar.userlistmapping t9um ON t1.LastAnalyst = t9um.UserNameOld -- mapping table
LEFT JOIN radar.UserTeamRegistry t9 ON COALESCE(t9um.UserNameNew, t1.LastAnalyst) = t9.userName AND t1.LastAnalystTeamDate >= t9.TeamStartDate AND t1.LastAnalystTeamDate <= COALESCE(t9.TeamEndDate, DATE('{EDL_LoadDate}'))

LEFT JOIN CTE_LatestNextReviewDate t10 ON t2.GcobId = t10.GcobId AND t2.ClientType = t10.ClientType AND t2.CaseId = t10.CaseId
LEFT JOIN second_rfi_creation_date rfi ON t1.ClientId = rfi.ClientId AND t1.GcobId = rfi.GcobId AND rfi.rn = 2
""").createOrReplaceTempView('workitems_status_final_temp')

# COMMAND ----------

# MAGIC %md
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

with_workdays = filtered_df.withColumn(
    "workdays",
    generate_workdays_udf(
        F.col("WorkItemCreatedDate"),
        F.coalesce(
            F.col("WorkItemCompletedDate"),
            F.to_date(F.expr("DATE('${EDL_LoadDate}')"))
        )
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
with_workdays = filtered_df.withColumn(
    "workdays",
    generate_workdays_udf(
        F.col("WorkItemCreatedDate"),
        F.coalesce(
            F.col("WorkItemCompletedDate"),
            F.to_date(F.expr("DATE('${EDL_LoadDate}')"))
        )
    )
)

# Step 6: Explode and count unique workdays
exploded = with_workdays.select(
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
spark.sql(f"""
          
WITH latest_sprintstatus AS (
    SELECT
        CaseId,
        SprintStatus,
        CreatedOnUTC,
        EndDateUTC
    FROM (
        SELECT
            CaseId,
            SprintStatus,
            CreatedOnUTC,
            EndDateUTC,
            ROW_NUMBER() OVER (PARTITION BY CaseId ORDER BY CreatedOnUTC DESC) AS rn
        FROM radar.rdr_sprintstatuslog
    ) ranked
    WHERE rn = 1
),
-- Aggregate outreach window per CaseId (one row per CaseId + sourceClient)
clientOutreachDates_CddPlanningApp AS (
    SELECT
        t1.CaseId,
        t1.sourceClient,
        ls.SprintStatus,  -- take the latest sprint status for that CaseId
        MIN(CASE
              WHEN t2.SprintStatus IN (
                   'Client Outreach Completed', 'Client Outreach',
                   'Rebound Client Outreach', 'Rebound Client Outreach Completed'
              ) THEN t2.CreatedOnUTC
            END) AS OutreachStart,
        MAX(CASE
              WHEN t2.SprintStatus IN (
                   'Client Outreach Completed', 'Client Outreach',
                   'Rebound Client Outreach', 'Rebound Client Outreach Completed'
              ) THEN COALESCE(t2.EndDateUTC, DATE('{EDL_LoadDate}'))
            END) AS OutreachEnd
    FROM radar.rdr_caseplanningdetailsNew1 AS t1
    INNER JOIN radar.rdr_sprintstatuslog AS t2
        ON t1.CaseId = t2.CaseId
    INNER JOIN latest_sprintstatus AS ls
        ON t1.CaseId = ls.CaseId
    WHERE
       t2.SprintStatus IN (
            'Client Outreach Completed', 'Client Outreach',
            'Rebound Client Outreach', 'Rebound Client Outreach Completed'
      )
    GROUP BY t1.CaseId, t1.sourceClient, ls.SprintStatus
),
clientOutreachDuration_CddPlanningApp AS (
    SELECT
        CaseId,
        sourceClient,
        ROW_NUMBER() OVER (
            PARTITION BY sourceClient
            ORDER BY OutreachStart DESC, OutreachEnd DESC, CaseId DESC
        ) AS rn,
        CASE
            WHEN OutreachStart IS NOT NULL THEN
                DATEDIFF(DAY, OutreachStart, COALESCE(OutreachEnd, DATE('{EDL_LoadDate}'))) + 1
                - (DATEDIFF(WEEK, OutreachStart, COALESCE(OutreachEnd, DATE('{EDL_LoadDate}'))) * 2)
                + CASE WHEN DATE_FORMAT(OutreachStart,'EEEE') = 'Sunday' THEN 1 ELSE 0 END
                + CASE WHEN DATE_FORMAT(OutreachEnd, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END
            ELSE NULL
        END AS DaysInClientOutreach
    FROM clientOutreachDates_CddPlanningApp
QUALIFY rn = 1
),
reboundclientOutreachDates_CddPlanningApp AS (
    SELECT
        t1.CaseId,
        t1.sourceClient,
        MIN(CASE WHEN SprintStatus = 'Rebound Client Outreach' THEN CreatedOnUTC END) AS OutreachStart,
        MAX(CASE WHEN SprintStatus IN  ('Rebound Client Outreach Completed', 'Rebound Client Outreach') THEN
            COALESCE(EndDateUTC, DATE('{EDL_LoadDate}')) END) AS OutreachEnd
    from radar.rdr_caseplanningdetailsNew1 AS t1
    LEFT JOIN radar.rdr_sprintstatuslog AS t2 ON t1.CaseId = t2.CaseId
    WHERE SprintStatus IN ('Rebound Client Outreach', 'Rebound Client Outreach Completed')
    GROUP BY t1.CaseId, t1.sourceClient
),
reboundclientOutreachDuration_CddPlanningApp AS (
    SELECT
        CaseId,
        sourceClient,
        CASE 
            WHEN OutreachStart IS NOT NULL THEN
                DATEDIFF(DAY, OutreachStart, COALESCE(OutreachEnd,DATE('{EDL_LoadDate}'))) + 1
                - (DATEDIFF(WEEK, OutreachStart, COALESCE(OutreachEnd, DATE('{EDL_LoadDate}'))) * 2)
                + CASE WHEN DATE_FORMAT(OutreachStart,'EEEE') = 'Sunday' THEN 1 ELSE 0 END
                + CASE WHEN DATE_FORMAT(OutreachEnd, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END
            ELSE NULL
        END AS DaysInReboundClientOutreach
    FROM reboundclientOutreachDates_CddPlanningApp
),
MLRO_CddPlanningApp AS (
    SELECT
        t1.CaseId,
        t1.sourceClient,
        ls.SprintStatus,
        MIN(CASE WHEN t2.SprintStatus = 'With CC secretaries' THEN t2.CreatedOnUTC END) AS OutreachStart,
        MAX(CASE WHEN t2.SprintStatus IN ('With CC secretaries') THEN
            COALESCE(t2.EndDateUTC, DATE('{EDL_LoadDate}')) END) AS OutreachEnd
    FROM radar.rdr_caseplanningdetailsNew1 AS t1
    INNER JOIN radar.rdr_sprintstatuslog AS t2
        ON t1.CaseId = t2.CaseId
    INNER JOIN latest_sprintstatus AS ls
        ON t1.CaseId = ls.CaseId
    WHERE t2.SprintStatus IN ('With CC secretaries')
    GROUP BY t1.CaseId, t1.sourceClient, ls.SprintStatus
),
MLRODuration_CddPlanningApp AS (
    SELECT
        CaseId,
        sourceClient,
        ROW_NUMBER() OVER (
            PARTITION BY sourceClient
            ORDER BY OutreachStart DESC, OutreachEnd DESC, CaseId DESC
        ) AS rn,
        CASE
            WHEN OutreachStart IS NOT NULL THEN
                DATEDIFF(DAY, OutreachStart, COALESCE(OutreachEnd, DATE('{EDL_LoadDate}'))) + 1
                - (DATEDIFF(WEEK, OutreachStart, COALESCE(OutreachEnd, DATE('{EDL_LoadDate}'))) * 2)
                + CASE WHEN DATE_FORMAT(OutreachStart,'EEEE') = 'Sunday' THEN 1 ELSE 0 END
                + CASE WHEN DATE_FORMAT(OutreachEnd, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END
            ELSE NULL
        END AS DaysInMLRO_SLA
    FROM MLRO_CddPlanningApp
    QUALIFY rn = 1
),
sentToFosForOutreach_CddPlanningApp AS (
    SELECT
        t1.CaseId,
        t1.sourceClient,
        ls.SprintStatus,
        MIN(CASE WHEN t2.SprintStatus = 'Sent to FoS for Outreach' THEN t2.CreatedOnUTC END) AS OutreachStart,
        MAX(CASE WHEN t2.SprintStatus IN ('Sent to FoS for Outreach') THEN
            COALESCE(t2.EndDateUTC,  DATE('{EDL_LoadDate}')) END) AS OutreachEnd
    FROM radar.rdr_caseplanningdetailsNew1 AS t1
    INNER JOIN radar.rdr_sprintstatuslog AS t2
        ON t1.CaseId = t2.CaseId
    INNER JOIN latest_sprintstatus AS ls
        ON t1.CaseId = ls.CaseId
    WHERE t2.SprintStatus IN ('Sent to FoS for Outreach')
    GROUP BY t1.CaseId, t1.sourceClient, ls.SprintStatus
),
sentToFosForOutreachDuration_CddPlanningApp AS (
    SELECT
        CaseId,
        sourceClient,
        ROW_NUMBER() OVER (
            PARTITION BY sourceClient
            ORDER BY OutreachStart DESC, OutreachEnd DESC, CaseId DESC
        ) AS rn,
        CASE
            WHEN OutreachStart IS NOT NULL THEN
                DATEDIFF(DAY, OutreachStart, COALESCE(OutreachEnd,  DATE('{EDL_LoadDate}'))) + 1
                - (DATEDIFF(WEEK, OutreachStart, COALESCE(OutreachEnd,  DATE('{EDL_LoadDate}'))) * 2)
                + CASE WHEN DATE_FORMAT(OutreachStart,'EEEE') = 'Sunday' THEN 1 ELSE 0 END
                + CASE WHEN DATE_FORMAT(OutreachEnd, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END
            ELSE NULL
        END AS DaysInSentToFosForOutreach_SLA
    FROM sentToFosForOutreach_CddPlanningApp
    QUALIFY rn = 1
)
SELECT DISTINCT
  t1.*
, t2.daysInReboundAssessment
, t3.daysIn_4_eye_check_in_progress as daysInQC
, t3.daysIn_Client_owner_sign_off_requested as daysInSignOff
, t3.daysIn_Product_fulfilment_in_progress as daysInFulfillment
, t3.daysIn_Ready_for_4_eye_check as daysInReadyForQC
, t3.daysIn_Ready_for_KYC_assessment as daysInReadyForKYCAssessment
, t5.reboundAssessmentCount as numberOfReboundAssessment
, COALESCE(t3.daysIn_Initiation_In_Progress, 0) + COALESCE(t4.daysInReboundInitiation, 0) AS daysInPrework_Sla--(Sum of daysInInitiation & daysInReboundInitiation)

-- Days in Assessment 1 (first assessment, no RFI)
,  CASE
    WHEN --MainCaseStatusType = 'Assessment'
         --AND t3.DateCreated IS NULL
         AssessmentInProgress IS NOT NULL THEN
         --AND t1.ReadyFor4EYECheck IS NULL THEN
      DATEDIFF(DAY, AssessmentInProgress, COALESCE(FirstRFICreated, ReadyFor4EYECheck,
        DATE('{EDL_LoadDate}'))) + 1
      - (
          (DATEDIFF(WEEK, AssessmentInProgress, COALESCE(FirstRFICreated, ReadyFor4EYECheck,
            DATE('{EDL_LoadDate}')
          )) * 2)
          + (CASE WHEN DATE_FORMAT(AssessmentInProgress, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
          + (CASE WHEN DATE_FORMAT(COALESCE(FirstRFICreated, ReadyFor4EYECheck,DATE('{EDL_LoadDate}')
          ), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
      )
  END AS DaysInAssessment1

--Number of days in Assessment 2 (when RFI exists and is completed)
--Days in Assessment2 is sum of days in assessment2 + rebound assessment
, (coalesce(t2.daysInReboundAssessment, 0) + 
   CASE 
     WHEN CreatedDateRFI IS NOT NULL AND FirstRFICompleted IS NOT NULL THEN
       DATEDIFF(DAY, FIRSTRFICompleted, COALESCE(ReadyFor4EYECheck, DATE('{EDL_LoadDate}'))) + 1
       - (
           (DATEDIFF(WEEK, FIRSTRFICompleted, COALESCE(ReadyFor4EYECheck, DATE('{EDL_LoadDate}'))) * 2)
           + (CASE WHEN DATE_FORMAT(FIRSTRFICompleted, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
           + (CASE WHEN DATE_FORMAT(COALESCE(ReadyFor4EYECheck, DATE('{EDL_LoadDate}')), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
         )
     ELSE 0
   END
) AS daysInAssessment2SLA

, (
   CASE 
     WHEN CreatedDateRFI IS NOT NULL AND FirstRFICompleted IS NOT NULL THEN
       DATEDIFF(DAY, FIRSTRFICompleted, COALESCE(ReadyFor4EYECheck, DATE('{EDL_LoadDate}'))) + 1
       - (
           (DATEDIFF(WEEK, FIRSTRFICompleted, COALESCE(ReadyFor4EYECheck, DATE('{EDL_LoadDate}'))) * 2)
           + (CASE WHEN DATE_FORMAT(FIRSTRFICompleted, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
           + (CASE WHEN DATE_FORMAT(COALESCE(ReadyFor4EYECheck, DATE('{EDL_LoadDate}')), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
         )
     ELSE 0
   END
) AS daysInAssessment2


-- Number of days in Client Outreach (from RFI creation to response)
, CASE
    WHEN CreatedDateRFI is not null  THEN
      DATEDIFF(DAY, FirstRFICreated,  COALESCE(FIRSTRFICompleted , DATE('{EDL_LoadDate}'))) + 1
      - (
          (DATEDIFF(WEEK, FirstRFICreated, COALESCE(FIRSTRFICompleted , DATE('{EDL_LoadDate}')))) * 2)
          --(DATEDIFF(WEEK,  t3.DateCreated,  COALESCE(t3.DateResponded, DATE('{EDL_LoadDate}'))) * 2)
          + (CASE WHEN DATE_FORMAT(FirstRFICreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
          + (CASE WHEN DATE_FORMAT(COALESCE(FIRSTRFICompleted , DATE('{EDL_LoadDate}')), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ELSE NULL
  END AS DaysInClientOutreach

, CASE
    WHEN CreatedDateRFI is not null  THEN
      DATEDIFF(DAY, FirstRFICreated,  FIRSTRFICompleted) + 1
      - (
          (DATEDIFF(WEEK, FirstRFICreated, FIRSTRFICompleted)) * 2)
          --(DATEDIFF(WEEK,  t3.DateCreated,  COALESCE(t3.DateResponded, DATE('{EDL_LoadDate}'))) * 2)
          + (CASE WHEN DATE_FORMAT(FirstRFICreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
          + (CASE WHEN DATE_FORMAT(FIRSTRFICompleted, 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
    ELSE NULL
  END AS DaysInClientOutreachCompleted
  
, CASE
    WHEN --t3.CasePhase IN ('Rebound Client Outreach')
           FIRSTRFICompleted IS NOT NULL AND COALESCE(SecondRFICreatedDate, LatestRFICompleted) >  FIRSTRFICompleted THEN
      DATEDIFF(DAY,  COALESCE(SecondRFICreatedDate, FIRSTRFICompleted), COALESCE(CompletedDateFirstRFI, DATE('{EDL_LoadDate}'))) + 1
      - (
          (DATEDIFF(WEEK, COALESCE(SecondRFICreatedDate, FIRSTRFICompleted), COALESCE(CompletedDateFirstRFI, DATE('{EDL_LoadDate}'))) * 2)
          + (CASE WHEN DATE_FORMAT(COALESCE(SecondRFICreatedDate, FIRSTRFICompleted), 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
          + (CASE WHEN DATE_FORMAT(COALESCE(CompletedDateFirstRFI, DATE('{EDL_LoadDate}')), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
      )
   ELSE NULL
  END AS DaysInReboundClientOutreach

-- Below days are calulated using CDD planning app data coming from PowerApps
--, t6.DaysInClientOutreach + t7.DaysInReboundClientOutreach AS DaysInRFI_SLA --(Sum of daysInClientOutreach & daysInReboundClientOutreach)
, t6.DaysInClientOutreach AS DaysInRFI_SLA
, t8.DaysInMLRO_SLA
, t9.daysinsenttofosforoutreach_sla

  FROM workitems_status_final_temp as t1
  LEFT JOIN daysInReboundAssessment as t2 on t1.sourceClient = t2.sourceClient
  LEFT JOIN totalDaysInWorkItemStatus AS t3 on t1.sourceClient = t3.sourceClient
  LEFT JOIN daysInReboundInitiation AS t4 on t1.sourceClient = t4.sourceClient
  LEFT JOIN reboundAssessmentCounts as t5 on t1.sourceClient = t5.sourceClient
  LEFT JOIN clientOutreachDuration_CddPlanningApp AS t6 ON t1.sourceClient = t6.sourceClient
  LEFT JOIN reboundclientOutreachDuration_CddPlanningApp AS t7 ON t1.sourceClient = t7.sourceClient
  LEFT JOIN MLRODuration_CddPlanningApp AS t8 ON t1.sourceClient = t8.sourceClient
  LEFT JOIN sentToFosForOutreachDuration_CddPlanningApp AS t9 ON t1.sourceClient = t9.sourceClient
  """).createOrReplaceTempView('workitems_status_daysInCurrentCasePhase')

# COMMAND ----------

# DBTITLE 1,OLD workitems_status_final for days in case phase
# MAGIC %sql
# MAGIC /*
# MAGIC SELECT 
# MAGIC   t1.*
# MAGIC , t2.daysInReboundAssessment
# MAGIC , t3.daysIn_4_eye_check_in_progress as daysInQC
# MAGIC , t3.daysIn_Client_owner_sign_off_requested as daysInSignOff
# MAGIC --, t3.daysIn_KYC_assessment_in_progress as daysInKYCAssessmentInProgress
# MAGIC , t3.daysIn_Product_fulfilment_in_progress as daysInFulfillment
# MAGIC , t3.daysIn_Ready_for_4_eye_check as daysInReadyForQC
# MAGIC , t3.daysIn_Ready_for_KYC_assessment as daysInReadyForKYCAssessment
# MAGIC , t3.daysIn_Initiation_In_Progress as daysInInitiation
# MAGIC , t4.daysInReboundInitiation
# MAGIC , t5.reboundAssessmentCount as numberOfReboundAssessment
# MAGIC -- Days in Assessment 1 (first assessment, no RFI)
# MAGIC ,  CASE
# MAGIC     WHEN --MainCaseStatusType = 'Assessment'
# MAGIC          --AND t3.DateCreated IS NULL
# MAGIC          AssessmentInProgress IS NOT NULL THEN
# MAGIC          --AND t1.ReadyFor4EYECheck IS NULL THEN
# MAGIC       DATEDIFF(DAY, AssessmentInProgress, COALESCE(FirstRFICreated, ReadyFor4EYECheck,
# MAGIC         DATE('{EDL_LoadDate}'))) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, AssessmentInProgress, COALESCE(FirstRFICreated, ReadyFor4EYECheck,
# MAGIC             DATE('{EDL_LoadDate}')
# MAGIC           )) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(AssessmentInProgress, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(FirstRFICreated, ReadyFor4EYECheck,DATE('{EDL_LoadDate}')
# MAGIC           ), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       )
# MAGIC   END AS DaysInAssessment1
# MAGIC
# MAGIC   
# MAGIC   -- Number of days in Assessment 2 (when RFI exists and is completed)
# MAGIC , CASE
# MAGIC     WHEN CreatedDateRFI IS NOT NULL AND FirstRFICompleted IS NOT NULL THEN
# MAGIC       DATEDIFF(DAY, FIRSTRFICompleted, COALESCE(ReadyFor4EYECheck, DATE('{EDL_LoadDate}'))) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, FIRSTRFICompleted, COALESCE(ReadyFor4EYECheck, DATE('{EDL_LoadDate}'))) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(FIRSTRFICompleted, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(ReadyFor4EYECheck, DATE('{EDL_LoadDate}')), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       )
# MAGIC     ELSE NULL
# MAGIC   END AS DaysInAssessment2
# MAGIC
# MAGIC
# MAGIC -- Number of days in Client Outreach (from RFI creation to response)
# MAGIC , CASE
# MAGIC     WHEN CreatedDateRFI is not null  THEN
# MAGIC       DATEDIFF(DAY, FirstRFICreated,  COALESCE(FIRSTRFICompleted , DATE('{EDL_LoadDate}'))) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, FirstRFICreated, COALESCE(FIRSTRFICompleted , DATE('{EDL_LoadDate}')))) * 2)
# MAGIC           --(DATEDIFF(WEEK,  t3.DateCreated,  COALESCE(t3.DateResponded, DATE('{EDL_LoadDate}'))) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(FirstRFICreated, 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(FIRSTRFICompleted , DATE('{EDL_LoadDate}')), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC     ELSE NULL
# MAGIC   END AS DaysInClientOutreach
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
# MAGIC       DATEDIFF(DAY,  COALESCE(SecondRFICreatedDate, FIRSTRFICompleted), COALESCE(CompletedDateFirstRFI, DATE('{EDL_LoadDate}'))) + 1
# MAGIC       - (
# MAGIC           (DATEDIFF(WEEK, COALESCE(SecondRFICreatedDate, FIRSTRFICompleted), COALESCE(CompletedDateFirstRFI, DATE('{EDL_LoadDate}'))) * 2)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(SecondRFICreatedDate, FIRSTRFICompleted), 'EEEE') = 'Sunday' THEN 1 ELSE 0 END)
# MAGIC           + (CASE WHEN DATE_FORMAT(COALESCE(CompletedDateFirstRFI, DATE('{EDL_LoadDate}')), 'EEEE') = 'Saturday' THEN 1 ELSE 0 END)
# MAGIC       )
# MAGIC    ELSE NULL
# MAGIC   END AS DaysInReboundClientOutreach
# MAGIC   
# MAGIC   FROM workitems_status_final_temp as t1
# MAGIC   LEFT JOIN daysInReboundAssessment as t2 on t1.sourceClient = t2.sourceClient
# MAGIC   LEFT JOIN totalDaysInWorkItemStatus AS t3 on t1.sourceClient = t3.sourceClient
# MAGIC   LEFT JOIN daysInReboundInitiation AS t4 on t1.sourceClient = t4.sourceClient
# MAGIC   LEFT JOIN reboundAssessmentCounts as t5 on t1.sourceClient = t5.sourceClient
# MAGIC   """).createOrReplaceTempView('workitems_status_daysInCurrentCasePhase')
# MAGIC   */

# COMMAND ----------

# DBTITLE 1,workitems_status_final
spark.sql(f"""
SELECT
*,
   CASE
    WHEN CasePhase = 'Assessment 1' THEN DaysInAssessment1 
    WHEN CasePhase = 'Assessment 2' THEN DaysInAssessment2 
   ELSE DaysInCurrentCasePhaseTemp
  END AS DaysInCurrentCasePhase 
FROM workitems_status_daysInCurrentCasePhase
  """).createOrReplaceTempView('workitems_status_final')

# COMMAND ----------

# DBTITLE 1,TC_CTE - to define completed on time!
spark.sql(f"""

WITH TC_CTE1 AS (
  SELECT DISTINCT
    t1.SourceClient
    , t1.ReviewTypeName
    , t2.TotalCaseDuration
    , CASE 
        WHEN t1.ValidatedRiskLevel = 'High' THEN ADD_MONTHS(t1.NextReviewDate, 2)
        WHEN t1.ReviewTypeName = 'Event Driven Review' THEN DATE_ADD(t2.Prework, 90) -- set an NRD for an EDR
        ELSE t1.NextReviewDate
      END AS TC_NRD
    , CASE 
        WHEN t1.ReviewTypeName LIKE '%Offboarding%' THEN t2.Completed
        ELSE t1.FinalDecisionDate -- SignOffDate
      END AS SignOffDate_TC
    , CASE 
        WHEN t1.EdrReason IN ('Quality Control Review', 'First Line Monitoring Review') THEN 'QC EDR'
        WHEN t1.ReviewTypeName LIKE '%Offboarding%' THEN 'Offboarding'
        ELSE t1.ReviewTypeName
      END AS CaseReviewType -- same as Firebird in dml.WorkitemsStatus

  FROM party_case_client_details t1
  LEFT JOIN workitems_status_final t2 ON t1.SourceClient = t2.SourceClient
  WHERE t1.ReviewTypeName IN ('Periodic Review', 'Initial On-Boarding', 'Event Driven Review', 'Product Offboarding', 'Product Offboarding (Resume)') -- 'QC EDR'
    AND t1.CaseStatusName = 'Completed' -- OR t1.SourceSystem = 'GCOB Legacy' -- 2nd is the same as completed in legacy
)

, TC_CTE2 AS (
    SELECT DISTINCT
      SourceClient
      , CaseReviewType
      , TC_NRD
      , SignOffDate_TC
      , TotalCaseDuration
      , LAG(TC_NRD, 1, NULL) OVER (PARTITION BY SourceClient ORDER BY SignOffDate_TC) AS FormerNRD -- point 3 above
    FROM TC_CTE1
  )

-- point 6 and 3 above
, TC_CTE3 AS (
    SELECT DISTINCT
      SourceClient
      , SignOffDate_TC
      , LEAD(SignOffDate_TC, 1, NULL) OVER (PARTITION BY SourceClient ORDER BY SignOffDate_TC) AS NextSignOffDate
      , CaseReviewType
      , TC_NRD
      , TotalCaseDuration
    FROM TC_CTE2
    WHERE SourceClient NOT IN (
      SELECT
        SourceClient 
      FROM TC_CTE2
      WHERE TC_NRD = FormerNRD AND (CaseReviewType <> 'Offboarding')
    )
    AND CaseReviewType <> 'QC EDR'
  )


SELECT
  SourceClient
  , SignOffDate_TC
  , NextSignOffDate
  -- , CaseReviewType
  , TC_NRD
  , CASE
      WHEN CaseReviewType = 'Event Driven Review' AND TotalCaseDuration <= 90 THEN 1
      WHEN CaseReviewType = 'Event Driven Review' AND TotalCaseDuration > 90 THEN 0 -- error in firebird that reported <= instead of >
      WHEN NextSignOffDate <= TC_NRD THEN 1
      WHEN NextSignOffDate > TC_NRD THEN 0
      WHEN TC_NRD < DATE('{EDL_LoadDate}') AND NextSignOffDate IS NULL THEN 0
      ELSE NULL
    END AS CompletedOnTime
FROM (SELECT * FROM TC_CTE3 WHERE CaseReviewType <> 'QC EDR')

""").createOrReplaceTempView('TC_CTE')

# COMMAND ----------

# DBTITLE 1,firebird_master
from pyspark.sql.functions import lit, to_date
spark.sql(f"""

    WITH onb_pl_cte ( -- onboarding pl cte to get the onboarding type at case level starting from UniqueGcobId, based on InsertedOnDate diff with Prework date
    SELECT
        t1.SourceClient
        , t3.UniqueGcobid
        , t2.Prework
        , t3.InsertedOnDate
        , t1.ReviewTypeName
        , t3.OnboardingType
        , ABS(datediff(t2.Prework, t3.InsertedOnDate)) AS diff_days
        , ROW_NUMBER() OVER (PARTITION BY t1.SourceClient ORDER BY ABS(datediff(t2.Prework, t3.InsertedOnDate))) AS rn

    FROM party_case_client_details t1
    LEFT JOIN workitems_status_final t2 ON t1.SourceClient = t2.SourceClient AND t1.CaseId = t2.CaseId
    JOIN radar.onboardings t3 ON CASE
            WHEN t1.ClientType = 'Legal Entity' THEN t1.GcobId
            WHEN t1.ClientType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person') THEN CONCAT('NP_', t1.GcobId)
        END = t3.UniqueGcobId
    WHERE t1.ReviewTypeName = 'Initial On-Boarding'
    )

    SELECT DISTINCT
    t1.SourceSystem
    , t1.ClientId
    , t1.CaseId
    , t1.GcobId
    , t1.FullLegalName
    , t1.TeaOtherReason
    , t1.TEAReviewReasonDescription
    , t1.ReviewTypeName
    , t1.CaseStatusName
    , t1.BusinessLineName
    , t1.GlobalClientOwner
    , t1.GlobalClientOwnerLocation
    , t1.GlobalClientOwnerOfficeLocation
    , t1.ClientLifeCycleName
    , t1.CddType
    , t1.EdrReason
    , t1.NextReviewDate
    , t1.RingFenced
    , t1.OwnerType
    , t1.ScheduledCompletionDate
    , t1.IsEligibleForFatcaAssessment
    , t1.FatcaClassification
    , t1.FatcaDateOfIssue
    , t1.IsEligibleForCrsAssessment
    , t1.CrsClassification
    , t1.CrsFormSignedDate
    , t1.FullLegalNameInLocalLanguage
    , t1.DateOfBirth
    , t1.RegisteredStreet
    , t1.RegisteredNumber
    , t1.RegisteredPostalCode
    , t1.RegisteredCity
    , t1.RegisteredRegion
    , t1.OperatingStreet
    , t1.OperatingNumber
    , t1.OperatingPostalCode
    , t1.OperatingCity
    , t1.OperatingRegion
    , t1.OffBoardingReason
    , t1.SalesforceClientID_nCino
    , t1.SubmitToClientCommittee
    , t1.CountryOfRegistration
    , t1.RegisteredCountryIsoCode
    , t1.OperatingCountryIsoCode
    , t1.CountryOfOperation
    , t1.IncorporationNumber
    , t1.IsIncorporated
    , t1.IncorporationDate
    , t1.IsClientListed
    , t1.IsClientRegulated
    , t1.FIHubIndicator
    , t1.HasTaxForm
    , t1.IsTaxIntegrityMaterial
    , t1.ValidatedRiskLevel
    , t1.ClientType
    , t1.IsOperatingAddressDifferentToRegisteredAddress
    , t1.Comments
    , t1.ClientApprovalDate
    , t1.AmendmentReason
    , t1.CaseDecisionMotivation
    , t1.ClientOwnerChangeReason
    , t1.EdrOtherReason
    , t1.ExecutiveSummary
    , t1.ReviewReason
    , t1.ReviewReasonOtherExplanation
    , t1.IsLatestApprovedVersionOfClient
    , t1.ClientCitizenship
    , t1.ClientNationality
    , t1.WWID
    , t1.ACBS
    , t1.RUTID
    , t1.ISB
    , t1.GCDSID
    , t1.NameOfExchange
    , t1.CountryOfExchange
    , t1.NameOfRegulator
    , t1.CountryOfRegulator
    , t1.4EyeCheckReviewer
    , t1.CurrentAssignee
    , t1.Last4EyeCheckReviewer
    , t1.LastKYCAnalyst
    , t1.LastSentFor4EyeCheck
    , t1.LastSentForKYCAssesment
    , t1.InitiationInProgressAssignee
    , t1.CaseCompletedDate
    , t1.CaseCreationDate
    , t1.DateSubmittedFor4EyeCheck
    , t1.DateSubmittedForSignOff
    , t1.KYCAssessmentInProgressAssignee
    , t1.ReadyForKYCAssessmentDate
    , t1.ClientOwnerSignOffDate
    , t1.FinalDecisionDate
    , t1.LastProductOffboardingAnalyst
    , t1.SourceClient
    , t1.RiskModelName
    , t1.ModelCalculatedRiskLevel
    , t1.ModelRecalculatedRiskLevel
    , t1.GeographicalRiskLevel
    , t1.EntityTypeRiskLevel
    , t1.StructureRiskLevel
    , t1.SectorRiskLevel
    , t1.ProductAndServiceRiskLevel
    , t1.PEPRiskLevel
    , t1.TransactionRiskLevel
    , t1.DistributionRiskLevel
    , t1.ThirdPartyRiskLevel
    , t1.AdverseInfoRiskLevel
    , t1.OtherRiskLevel
    , t1.GeographicalApplicableRisk
    , t1.EntityTypeApplicableRisk
    , t1.StructureApplicableRisk
    , t1.SectorApplicableRisk
    , t1.ProductsApplicableRisk
    , t1.PoliticallyExposedPersonsApplicableRisk
    , t1.TransactionApplicableRisk
    , t1.DistributionChannelApplicableRisk
    , t1.ThirdPartyApplicableRisk
    , t1.AdverseInfoApplicableRisk
    , t1.OtherApplicableRisk


    , CASE WHEN t1.ClientId = t3.LastFullReviewId THEN 'Yes' ELSE 'No' END AS LatestFullReviewVersion
    , CASE WHEN t1.CaseStatusName NOT IN ('Cancelled', 'Completed', 'Migrated') THEN 'Yes' ELSE 'No' END AS ActiveCase
    , t4.CompletedOnTime
    , CASE
        WHEN t8.SourceClient IS NOT NULL THEN t8.OnboardingType
        WHEN t1.EdrReason IN ('Quality Control Review', 'First Line Monitoring Review') THEN 'QC EDR'
        WHEN t1.ReviewTypeName LIKE '%Offboarding%' THEN 'Offboarding'
        ELSE t1.ReviewTypeName
        END AS CaseReviewType

    -- , CASE WHEN t1.ClientLifeCycleName <> 'FormerClient' THEN 0 ELSE 1 END AS DuplicateOffboarding -- wtf?
    , CASE 
        WHEN t1.ReviewTypeName LIKE '%Offboarding%'
            AND t1.CaseStatusName = 'Completed' 
            AND t1.ClientLifeCycleName <> 'FormerClient' 
        THEN 1 
        ELSE 0 
        END AS DuplicateOffboarding

    , t2.Prework
    , weekofyear(t2.Prework) AS StartWeekPrework
    , t2.PreworkAnalyst
    , t2.PreworkAnalystTeamDate
    , t2.ReadyForAssessment
    , t2.AssessmentInProgress
    , t2.AssessmentAnalyst
    , t2.AssessmentAnalystTeamDate
    , t2.4EYECheck
    , t2.4EYEAnalyst
    , t2.4EYEAnalystTeamDate
    , t2.SignOff
    , t2.Fulfillment
    , t2.Completed
    -- , t2.Cancelled
     /*
  these fields are replaced with the fields below in the select clause
    , t2.DaysInPrework
    , t2.DaysInReadyForAssessment
    , t2.DaysInAssessmentInProgress
    , t2.DaysIn4EYECheck
    , t2.DaysInSignoff
    , t2.DaysInFulfillment
    */
    , t2.TotalCaseDuration
    -- , t2.QCInteractions
    -- , t2.LastAnalyst
    -- , t2.LastAnalystTeamDate
    -- , t2.PreworkAlias
    -- , t2.AssessmentAlias
    -- , t2.4EYEAlias
    -- , t2.LastAnalystAlias
    , t2.RespondedDateFirstRFI
    , t2.CompletedDateFirstRFI
    , t2.FirstRFICreated
    , t2.LatestRFICompleted
    , t2.CasePhase
    , t2.CasePhaseSortOrder
    -- , t2.CasePhase2023
    -- , t2.CasePhaseSortOrder2023
    -- , t2.CasePhase2024
    , t2.CasePhaseCurrentYear
    -- , t2.CasePhaseSortOrder2024
    , t2.CasePhaseSortCurrentYear
    , t2.DaysInCurrentCasePhase
    , t2.EDRoverdue
    -- , t2.SignoffDateTC
    -- , t2.TCNRD
    , t3.LatestId
    , t3.LastFullReviewId
    , t3.CompletedId
    , t3.LatestCaseId
    -- , t5.ProductName -- commenting this out as it creates duplicates and it is not needed anywhere else
    , t5.BookingEntityLocation -- commenting this out as it creates duplicates and it is not needed anywhere else
    , t5.ProductOfferingLocation -- commenting this out as it creates duplicates and it is not needed anywhere else
    , t5.ProductLifeCycleStatus -- commenting this out as it creates duplicates and it is not needed anywhere else
    , t3.LatestCompletedCaseId
    , t2.4EyeUserTeam
    , t2.4EyeDepartment
    , t2.PreworkDepartment
    , t2.KYCUserTeam
    , t2.KYCDepartment
    , t2.PreworkUserTeam
    , t2.CDDDepartment
    , t2.CDDUserTeam
    , t3.LastFullReviewCaseId
    , t3.PragmaticCaseId
      ----------------new fields added for days in casephase logic start---------------------------------------
    , t2.daysInReboundAssessment
    , t2.DaysInAssessment1
    , t2.DaysInAssessment2
    , t2.DaysInClientOutreach
    , t2.DaysInClientOutreachCompleted
    , t2.DaysInReboundClientOutreach
    , t2.daysInQC
    , t2.daysInSignOff
    , t2.daysInReadyForKYCAssessment
    , t2.daysInFulfillment
    , t2.daysInReadyForQC
    , t2.daysInPrework_Sla
    , t2.numberOfReboundAssessment
    , t2.DaysInMLRO_SLA
    , t2.DaysInRFI_SLA
    , t2.daysInAssessment2SLA
    , t2.daysinsenttofosforoutreach_sla
    ----------------new fields added for days in casephase logic end---------------------------------------

    , CASE
        WHEN t1.ClientType = 'Legal Entity' THEN t1.GcobId
        WHEN t1.ClientType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person') THEN CONCAT('NP_', t1.GcobId)
        END AS UniqueGcobId

    , t7.LocalClientOwnerName
    , t7.Location AS LocalClientOwnerLocation

    FROM party_case_client_details t1
    LEFT JOIN workitems_status_final t2 ON t1.SourceClient = t2.SourceClient AND t1.CaseId = t2.CaseId
    LEFT JOIN party_client t3 ON t1.GcobId = t3.GcobId AND t1.ClientType = t3.ClientType
    -- LEFT JOIN next_signoff_date t4 ON t1.SourceClient = t4.SourceClient
    LEFT JOIN TC_CTE t4 ON t1.SourceClient = t4.SourceClient
    LEFT JOIN party_products_and_services t5 ON t1.SourceClient = t5.SourceClient
    LEFT JOIN party_local_client_Owners t7 ON t1.SourceClient = t7.SourceClient

    LEFT JOIN onb_pl_cte t8 ON t1.SourceClient = t8.SourceClient AND rn = 1

""").withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.firebird_master')

# COMMAND ----------

# MAGIC %md
# MAGIC # Cases_historical

# COMMAND ----------

# DBTITLE 1,Cases_temp_historical - prior to adding all previous risk levels in the next cell
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Cases_temp_historical AS -- prior to adding all previous risk levels in the next cell
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
# MAGIC     FROM radar.firebird_master
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
# MAGIC       /*
# MAGIC   these fields are replaced with the fields below in the select clause
# MAGIC   , t1.DaysInPrework
# MAGIC   , t1.DaysInReadyForAssessment
# MAGIC   , t1.DaysInAssessmentInProgress
# MAGIC   , t1.DaysIn4EYECheck
# MAGIC   , t1.DaysInSignoff
# MAGIC   , t1.DaysInFulfillment
# MAGIC     */
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
# MAGIC CREATE SCHEMA IF NOT EXISTS radar

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.Cases_temp_historical

# COMMAND ----------

spark.sql('SELECT * FROM Cases_temp_historical').write.mode('overwrite').saveAsTable('radar.Cases_temp_historical')

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
# MAGIC     Region involved
# MAGIC   */
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC         CASE 
# MAGIC           WHEN (t2.ProductOfferingLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya') 
# MAGIC           OR t2.BookingEntityLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya')) AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
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
# MAGIC             WHEN (t2.ProductOfferingLocation IN ('Rabobank Chile', 'Rabobank Brazil') 
# MAGIC             OR t2.BookingEntityLocation IN ('Rabobank Chile', 'Rabobank Brazil')) AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
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
# MAGIC FROM radar.Cases_temp_historical t1
# MAGIC LEFT JOIN party_products_and_services t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN LocationMappingRegion t3 ON t1.GlobalClientOwnerLocation = t3.Name
# MAGIC LEFT JOIN LocationMappingRegion t4 ON t2.BookingEntityLocation = t4.Name
# MAGIC LEFT JOIN LocationMappingRegion t5 ON t2.ProductOfferingLocation = t5.Name
# MAGIC GROUP BY 1,2,3,4,5

# COMMAND ----------

# DBTITLE 1,Cases_final_historical
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Cases_final_historical AS -- this includes all previous risk levels
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
# MAGIC   FROM radar.Cases_temp_historical t1
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
# MAGIC   , CASE
# MAGIC       WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t5.AsiaInvolved = 1 THEN 'Lead - Products Involved'
# MAGIC       WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t5.AsiaInvolved = 0 THEN 'Lead - No Products'
# MAGIC       WHEN t1.GlobalClientOwnerLocation NOT IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t5.AsiaInvolved = 1 THEN 'Non-Lead - Products Involved'
# MAGIC       WHEN t1.GlobalClientOwnerLocation NOT IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t5.AsiaInvolved = 0 THEN 'Non-Lead - No Products'
# MAGIC     END AS AsiaProductsInvolvmentType
# MAGIC
# MAGIC FROM radar.Cases_temp_historical t1
# MAGIC LEFT JOIN radar.Cases_temp_historical t2 ON t1.PreviousClientId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC LEFT JOIN AsiaProductsInvolvementChange_cte t3 ON t1.SourceClient = t3.SourceClient
# MAGIC LEFT JOIN AsiaProductsInvolvementChange_cte t4 ON t2.SourceClient = t4.SourceClient
# MAGIC
# MAGIC LEFT JOIN GlobalAndInvolvedFiles t5 ON t2.SourceClient = t5.SourceClient

# COMMAND ----------

# DBTITLE 1,drop and create table only if EDL_LoadDate == '2024-01-01', otherwise append
if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.Cases_historical')
    spark.sql('SELECT * FROM Cases_final_historical').write.mode('overwrite').saveAsTable('radar.Cases_historical')

else:
    spark.sql('SELECT * FROM Cases_final_historical').write.mode('append').saveAsTable('radar.Cases_historical')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if SourceClient duplication
duplicated_sourceclient = spark.sql(f"""
    SELECT SourceClient 
    FROM radar.Cases_historical
    WHERE DATE(EDL_LoadDate) = DATE('{EDL_LoadDate}')
    GROUP BY SourceClient 
    HAVING count(*) > 1
""")

# check for duplication
if not duplicated_sourceclient.isEmpty():
    raise Exception('Duplicated SourceClient in cases. Job stopped.')

# COMMAND ----------

# MAGIC %md
# MAGIC # Clients_historical

# COMMAND ----------

# DBTITLE 1,cases snapshot historical for below cells
spark.sql(f"""
    SELECT
        *
    FROM radar.Cases_historical
    WHERE DATE(EDL_LoadDate) = DATE('{EDL_LoadDate}')
""").createOrReplaceTempView('cases_snapshot')

# COMMAND ----------

# DBTITLE 1,Client_TradeName
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Client_TradeName AS
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   Id
# MAGIC   , concat_ws(', ', sort_array(collect_set(struct(ClientTradeName))).ClientTradeName) as TradeName
# MAGIC FROM party_trade_name
# MAGIC WHERE ResultType = 'LegalEntityClient'
# MAGIC GROUP BY Id

# COMMAND ----------

# DBTITLE 1,Clients_historical
spark.sql(f"""

WITH group_cte AS (
  SELECT DISTINCT 
    LOWER(t3.KYCGroup) AS KYCGroup
    , SUM(CASE WHEN t2.CDDtype = 'Listed Corporate' THEN 1 ELSE 0 END) AS ListedCorporatesInGroup
    , COUNT(DISTINCT t2.UniqueGcobId) AS TotalEntitiesInGroup -- counting DISTINCT UniqueGcobId because ProductName leads to duplications
  FROM radar.firebird_master t1
  INNER JOIN radar.firebird_master t2 ON t1.LatestId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId -- max(CaseId) not cancelled
  LEFT JOIN PortfolioPlanning t3 ON t2.UniqueGcobId = t3.UniqueGcobId
  WHERE t3.KYCGroup IS NOT NULL
  GROUP BY LOWER(t3.KYCGroup)
)

, ReviewLocation_cte AS ( -- made a cte out of this because ReviewLocation is needed for FOSTeam
    SELECT
      t2.UniqueGcobId
      , CASE
        -- COB Greater China
          WHEN LOWER(t3.SectorTeam) = 'int. desk' AND LOWER(t2.GlobalClientOwnerLocation) = 'rabobank china' THEN 'COB Greater China'
          WHEN LOWER(t3.SectorTeam) IN ('pf corp', 'tcf corp', 'core lending') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank hong kong', 'rabobank china') THEN 'COB Greater China'
          WHEN LOWER(t3.SectorTeam) IN ('tcf corp', 'core lending') AND LOWER(t2.GlobalClientOwnerLocation) = 'rabobank singapore' THEN 'COB SG'

        -- KYC SC
          WHEN LOWER(t2.GlobalClientOwnerLocation) = 'rabobank antwerp' AND LOWER(t3.SectorTeam) = 'core lending' THEN 'KYC SC'
        -- KYC UK
          WHEN LOWER(t3.SectorTeam) = 'tcf corp' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank kenya', 'rabobank netherlands') THEN 'KYC UK'
        -- Corp CDD Hub
          WHEN LOWER(t3.SectorTeam) IN ('int. desk', 'core lending') AND LOWER(t2.GlobalClientOwnerLocation) = 'rabobank london' THEN 'Corp CDD Hub'
          WHEN LOWER(t3.SectorTeam) = 'sponsor coverage' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank london', 'rabobank netherlands', 'rabobank antwerp') THEN 'Corp CDD Hub'
          WHEN LOWER(t3.SectorTeam) = 'vcf' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank london') THEN 'Corp CDD Hub'
          WHEN LOWER(t2.GlobalClientOwner) IN ('willmott, a (adam)', 'oord van, ja (marco)', 'erkamp, m (maarten)') THEN 'Corp CDD Hub'
          
        -- Foundation    
          WHEN LOWER(t3.SectorTeam) = 'foundation' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank foundation', 'rabobank netherlands') THEN 'Foundation'

        -- Global FI Hub
          WHEN LOWER(t3.SectorTeam) = 'agency' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank netherlands', 'rabobank hong kong') THEN 'Global FI Hub'
          WHEN LOWER(t3.SectorTeam) IN ('correspondent banking', 'fig', 'fig - orphan desk') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank netherlands') THEN 'Global FI Hub'
          WHEN LOWER(t3.SectorTeam) IN ('markets') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank australia', 'rabobank hong kong', 'rabobank london', 'rabobank netherlands', 'rabobank new zealand') THEN 'Global FI Hub'
          WHEN LOWER(t3.SectorTeam) = 'subsidiaries' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank london', 'rabobank netherlands') THEN 'Global FI Hub'
          WHEN LOWER(t3.SectorTeam) = 'tcf fi' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank singapore', 'rabobank netherlands') THEN 'Global FI Hub'
          WHEN LOWER(t3.SectorTeam) = 'treasury' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank australia', 'rabobank china', 'rabobank hong kong', 'rabobank new zealand') THEN 'Global FI Hub'

        -- KYC EU Hub
          WHEN LOWER(t3.SectorTeam) = 'core lending' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank dublin', 'rabobank frankfurt', 'rabobank madrid', 'rabobank milan', 'rabobank paris') THEN 'KYC EU Hub'
          WHEN LOWER(t3.SectorTeam) IN ('ef corp') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank netherlands') THEN 'KYC EU Hub'
          WHEN LOWER(t3.SectorTeam) = 'int. desk' AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank frankfurt') AND LOWER(t2.GlobalClientOwnerLocation) = 'meurichy de, k (koen)' THEN 'KYC EU Hub'
          WHEN LOWER(t3.SectorTeam) IN ('tcf corp') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank turkey') THEN 'KYC EU Hub'
    
        -- NY COBS
          WHEN LOWER(t3.SectorTeam) IN ('epp', 'fas', 'frr', 'markets', 'markets - fig', 'pf corp', 'sponsor coverage', 'tcf corp', 'treasury', 'vcf', 'core lending') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabo securities usa, inc. (rsec)', 'rabobank new york', 'rabobank canada (rcbr)', 'rabo securities usa, inc. (rsec), rabobank new york') THEN 'NY COBS'

        -- Rural
          WHEN LOWER(t3.SectorTeam) IN ('rural') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank - usa rabo agrifinance', 'rabobank - ranz country banking and ros', 'rabobank chile', 'rabobank new york', 'rabobank new zealand') THEN 'Rural'

        -- Wholesale KYC and Onboarding
          WHEN LOWER(t3.SectorTeam) IN ('int. desk', 'vcf', 'core lending') AND LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank australia') THEN 'Wholesale KYC and Onboarding'

        -- GCO Rabobank Argentina
        --   WHEN LOWER(t2.GlobalClientOwnerLocation) IN ('rabobank argentina') THEN NULL

          ELSE t3.ReviewLocation
        END AS ReviewLocation
    
    FROM radar.firebird_master t1
    INNER JOIN radar.firebird_master t2 ON t1.LatestId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId -- max(CaseId) not cancelled
    LEFT JOIN PortfolioPlanning t3 ON t2.UniqueGcobId = t3.UniqueGcobId -- considering onboarding pl as well!!
)

, onboarding_cte AS (
    SELECT
      MAX(ClientApprovalDate) AS OnboardingDate
      , UniqueGcobId
    FROM cases_snapshot
    WHERE CaseReviewType = 'Initial On-Boarding'
    GROUP BY UniqueGcobId
)

, N2KLocationsList_cte AS (
  SELECT
    t2.UniqueGcobId
    , CONCAT_WS(', ', ARRAY_DISTINCT(ARRAY_AGG(
        CASE
          WHEN (t2.ProductOfferingLocation = 'Rabo Securities USA, Inc. (RSEC)' OR t2.BookingEntityLocation = 'Rabo Securities USA, Inc. (RSEC)') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabo Securities USA, Inc. (RSEC)'
          WHEN (t2.ProductOfferingLocation = 'Rabobank - RANZ Country Banking and ROS' OR t2.BookingEntityLocation = 'Rabobank - RANZ Country Banking and ROS') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank - RANZ Country Banking and ROS'
          WHEN (t2.ProductOfferingLocation = 'Rabobank - Smallholder Agroforestry Finance (SAF)' OR t2.BookingEntityLocation = 'Rabobank - Smallholder Agroforestry Finance (SAF)') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank - Smallholder Agroforestry Finance (SAF)'
          WHEN (t2.ProductOfferingLocation = 'Rabobank - USA Rabo AgriFinance' OR t2.BookingEntityLocation = 'Rabobank - USA Rabo AgriFinance') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank - USA Rabo AgriFinance'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Antwerp' OR t2.BookingEntityLocation = 'Rabobank Antwerp') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Antwerp'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Argentina' OR t2.BookingEntityLocation = 'Rabobank Argentina') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Argentina'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Australia' OR t2.BookingEntityLocation = 'Rabobank Australia') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Australia'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Brazil' OR t2.BookingEntityLocation = 'Rabobank Brazil') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Brazil'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Canada (RCBR)' OR t2.BookingEntityLocation = 'Rabobank Canada (RCBR)') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Canada (RCBR)'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Canada(Rural)' OR t2.BookingEntityLocation = 'Rabobank Canada(Rural)') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Canada(Rural)'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Chile' OR t2.BookingEntityLocation = 'Rabobank Chile') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Chile'
          WHEN (t2.ProductOfferingLocation = 'Rabobank China' OR t2.BookingEntityLocation = 'Rabobank China') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank China'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Dublin' OR t2.BookingEntityLocation = 'Rabobank Dublin') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Dublin'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Foundation' OR t2.BookingEntityLocation = 'Rabobank Foundation') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Foundation'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Frankfurt' OR t2.BookingEntityLocation = 'Rabobank Frankfurt') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Frankfurt'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Hong Kong' OR t2.BookingEntityLocation = 'Rabobank Hong Kong') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Hong Kong'
          WHEN (t2.ProductOfferingLocation = 'Rabobank India' OR t2.BookingEntityLocation = 'Rabobank India') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank India'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Indonesia' OR t2.BookingEntityLocation = 'Rabobank Indonesia') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Indonesia'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Kenya' OR t2.BookingEntityLocation = 'Rabobank Kenya') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Kenya'
          WHEN (t2.ProductOfferingLocation = 'Rabobank London' OR t2.BookingEntityLocation = 'Rabobank London') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank London'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Madrid' OR t2.BookingEntityLocation = 'Rabobank Madrid') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Madrid'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Malaysia' OR t2.BookingEntityLocation = 'Rabobank Malaysia') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Malaysia'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Milan' OR t2.BookingEntityLocation = 'Rabobank Milan') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Milan'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Netherlands' OR t2.BookingEntityLocation = 'Rabobank Netherlands') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Netherlands'
          WHEN (t2.ProductOfferingLocation = 'Rabobank New York' OR t2.BookingEntityLocation = 'Rabobank New York') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank New York'
          WHEN (t2.ProductOfferingLocation = 'Rabobank New Zealand' OR t2.BookingEntityLocation = 'Rabobank New Zealand') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank New Zealand'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Paris' OR t2.BookingEntityLocation = 'Rabobank Paris') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Paris'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Singapore' OR t2.BookingEntityLocation = 'Rabobank Singapore') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Singapore'
          WHEN (t2.ProductOfferingLocation = 'Rabobank Turkey' OR t2.BookingEntityLocation = 'Rabobank Turkey') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') THEN 'Rabobank Turkey'
        END))) AS N2KLocationsList
  FROM radar.firebird_master t1
  INNER JOIN radar.firebird_master t2 ON t1.LatestId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId -- max(CaseId) not cancelled
  GROUP BY 1
)

SELECT DISTINCT
  t2.ClientId
  , t2.SourceClient
  , CASE
      WHEN SUBSTR(t2.SourceClient,0,3) = 'LEC' AND t2.SourceSystem = 'GCOB' THEN 'GCOB_LegalEntity'
      WHEN SUBSTR(t2.SourceClient,0,2) = 'NP' AND t2.SourceSystem = 'GCOB' THEN 'GCOB_NP-NPPC'
    END AS SourceSystemReference
  , t2.SourceSystem
  , t2.CaseStatusName
  , t2.GcobId
  , t2.LatestCompletedCaseId -- AS ApprovedCaseId
  , t2.LatestCaseId
  , t2.LastFullReviewId
  , t2.CompletedId
  , t2.LatestId
  , t2.ClientType
  , t2.UniqueGcobId
  , t2.FullLegalName
  , t2.GlobalClientOwner
  , t2.GlobalClientOwnerLocation
  , t2.RiskModelName
  , t2.BusinessLineName
  , t2.CddType
  , CASE WHEN t2.CddType = 'Listed Corporate' THEN 1 ELSE 0 END AS ListedCorporation
  -- , t2.FIHubIndicator
  , CASE WHEN t2.FIHubIndicator = 1 THEN 'FI' when t2.FIHubIndicator = 0 THEN 'Corp' ELSE NULL END AS FIHubIndicator_Derived
  , t2.FatcaDateOfIssue
  , t2.CrsFormSignedDate
  , CASE
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya') THEN 'E&A'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') THEN 'Asia'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') THEN 'North America'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Chile', 'Rabobank Brazil') THEN 'South America'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') THEN 'RANZ'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Foundation') THEN 'Rabobank Foundation'
    END AS GlobalReportingRegion
  , t2.CountryOfOperation
  , t2.ClientLifeCycleName
  , t2.ISB
  , TO_DATE(t13.NextReviewDate, 'dd-MM-yyyy') AS NextReviewDate -- used to be t2.NextReviewDate
  -- , t2.Signoff
  , CASE 
      WHEN t13.NextReviewDate < DATE('{EDL_LoadDate}') THEN 'Yes'
      WHEN t13.NextReviewDate < ADD_MONTHS(DATE('{EDL_LoadDate}'), 1) THEN 'Next month'
      ELSE 'No'
    END AS Overdue

  , CASE
      WHEN t13.NextReviewDate < DATE('{EDL_LoadDate}') THEN 2
      WHEN t13.NextReviewDate < ADD_MONTHS(DATE('{EDL_LoadDate}'), 1) THEN 1
      ELSE 0
    END AS OverdueCategoryNr
  , CASE
      WHEN t13.ValidatedRiskLevel = 'High' AND ADD_MONTHS(t13.NextReviewDate, 2) < DATE('{EDL_LoadDate}') THEN 'Yes'
      ELSE 'No'
    END AS RegulatoryOverdue
  , CASE
      WHEN t2.ReviewTypeName = 'Event Driven Review' THEN TO_DATE(ADD_MONTHS(t2.Prework, 3), 'yyyy-MM-dd')
      ELSE NULL
    END AS EDRDuedate
  , t2.Activecase

  , CASE
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands') AND t3.SectorTeam IN ('Acorn') THEN 'Acorn'

      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank London') AND t3.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Sector Banking') THEN 'London Advisory & Investments'  
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank London') AND t3.SectorTeam IN ('Core Lending', 'Treasury Corp') THEN 'London Core Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank London') AND t3.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP', 'VCF') THEN 'London Structured Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank London') AND t3.SectorTeam IN ('Int. Desk') THEN 'London International Services'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank London') AND t3.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'London Markets'

      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabobank Canada(Rural)', 'Rabobank - USA Rabo AgriFinance') AND t3.SectorTeam IN ('Rural') THEN 'North America Rural'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND t3.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Sector Banking') THEN 'North America Advisory & Investments'  
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND t3.SectorTeam IN ('Core Lending', 'AF') THEN 'North America Core Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND t3.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP', 'VCF') THEN 'North America Structured Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND t3.SectorTeam IN ('Int. Desk') THEN 'North America International Services'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND t3.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'North America Markets'

      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND t3.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Sector Banking') THEN 'AsiaAdvisory & Investments'  
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND t3.SectorTeam IN ('Core Lending') THEN 'Asia Core Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND t3.SectorTeam IN ('Int. Desk') THEN 'Asia International Services'        
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND t3.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'Asia Markets'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND t3.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP', 'VCF') THEN 'Asia Structured Lending'

      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Antwerp') AND t3.SectorTeam IN ('Core Lending') THEN 'Antwerp Core Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Antwerp') AND t3.SectorTeam IN ('Sponsor Coverage') THEN 'Antwerp Advisory & Investments'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Dublin') AND t3.SectorTeam IN ('Core Lending') THEN 'Dublin Core Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Frankfurt') AND t3.SectorTeam IN ('Core Lending') THEN 'Frankfurt Core Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Frankfurt') AND t3.SectorTeam IN ('Int. Desk') THEN 'Frankfurt International Services'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Madrid') AND t3.SectorTeam IN ('Core Lending') THEN 'Madrid Core Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Milan') AND t3.SectorTeam IN ('Core Lending') THEN 'Milan Core Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Paris') AND t3.SectorTeam IN ('Core Lending') THEN 'Paris Core Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya') AND t3.SectorTeam IN ('TCF Corp', 'PF Corp', 'ABF', 'PF FI', 'TCF FI', 'SIP', 'VCF') THEN 'NL Structured Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya', 'Rabobank Argentina') AND t3.SectorTeam IN ('FA', 'ETC', 'HTD', 'REF', 'CNS', 'AF', 'TRST', 'EF Corp', 'TM', 'ARG', 'EF FI')  THEN 'NL Core Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands') AND t3.SectorTeam IN ('M&A') THEN 'NL M&A + ECM'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands') AND t3.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Rabo Frontier Ventures', 'RCI', 'Sector Banking') THEN 'NL Advisory & Investments'    
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands') AND t3.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'NL Markets'
      
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') AND t3.SectorTeam IN ('Rural') THEN 'RANZ Rural'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND t3.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Sector Banking') THEN 'RANZ Advisory & Investments'  
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND t3.SectorTeam IN ('Core Lending') THEN 'RANZ Core Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND t3.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP', 'VCF') THEN 'RANZ Structured Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND t3.SectorTeam IN ('Int. Desk') THEN 'RANZ International Services'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND t3.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'RANZ Markets'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Argentina') AND t3.SectorTeam IN ('ARG') THEN 'South America Core Lending'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Foundation', 'Rabobank Netherlands') AND t3.SectorTeam IN ('Foundation') THEN 'Foundation'
    END AS GlobalKYCPortfolioNew

  , CASE
      WHEN lower(t3.SectorTeam) IN ('af', 'cns', 'ef corp', 'etc', 'fa', 'htd', 'ref', 'tm', 'trst', 'vcf') AND lower(t2.GlobalClientOwnerLocation) IN ('rabobank netherlands', 'rabobank kenya') THEN 'NL&A GCC'

      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Antwerp') AND t3.SectorTeam IN ('Core Lending') THEN 'Belgium'

      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Paris') AND t3.SectorTeam IN ('Core Lending') THEN 'France'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Dublin') AND t3.SectorTeam IN ('Core Lending') THEN 'Dublin'
      
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya') AND t3.SectorTeam IN ('PF Corp') THEN 'PF Corp'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Madrid') AND t3.SectorTeam IN ('Core Lending') THEN 'Madrid'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya') AND t3.SectorTeam IN ('TCF Corp') THEN 'TCF Corp'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Antwerp', 'Rabobank London') AND t3.SectorTeam IN ('Sponsor Coverage') THEN 'Sponsor Coverage'

      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Frankfurt') AND t3.SectorTeam IN ('Core Lending', 'Int. Desk') THEN 'Frankfurt'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Milan') AND t3.SectorTeam IN ('Core Lending') THEN 'Milan'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank London') AND t3.SectorTeam IN ('Core Lending', 'VCF', 'Int. Desk' ) THEN 'London'
    END AS CamsMi

  , CASE
      WHEN lower(t3.SectorTeam) IN ('pf corp', 'core lending', 'fa', 'vcf', 'htd', 'tcf corp', 'cns', 'tm', 'etc', 'ef corp', 'trst', 'int. desk', 'ref', 'arg', 'af', 'sponsor coverage', 'core lending etc', 'core lending fa') AND 
          lower(t2.GlobalClientOwnerLocation) IN ('rabobank netherlands', 'rabobank kenya', 'rabobank london', 'rabobank frankfurt', 'rabobank madrid', 'rabobank milan', 'rabobank antwerp', 'rabobank paris', 'rabobank dublin', 'rabobank argentina') 
      THEN 'backToGreen'
    END AS Scope
  , CASE
      WHEN lower(t3.SectorTeam) IN ('acorn') THEN 'Acorn'
      WHEN lower(t3.SectorTeam) IN ('sponsor coverage', 'fas', 'sector banking') THEN 'CFO'
      WHEN lower(t3.SectorTeam) IN ('af', 'arg', 'cns', 'core lending', 'ef corp', 'ef fi', 'etc', 'fa', 'htd', 'ref', 'trst', 'tm') THEN 'Core Lending'
      WHEN lower(t3.SectorTeam) IN ('int. desk') THEN 'International Services'
      WHEN lower(t3.SectorTeam) IN ('m&a') THEN 'M&A + ECM'
      WHEN lower(t3.SectorTeam) IN ('agency', 'correspondent banking', 'fig', 'fig - orphan desk', 'markets', 'markets/fig', 'psp coverage', 'subsidiaries') THEN 'Markets/FIG'
      WHEN lower(t3.SectorTeam) IN ('treasury') THEN 'Treasury'
      WHEN lower(t3.SectorTeam) IN ('rabo frontier ventures', 'rci') THEN 'Rabo Corp Investment'
      WHEN lower(t3.SectorTeam) IN ('abf', 'vcf') THEN 'VCF'
      WHEN lower(t3.SectorTeam) IN ('tcf corp', 'tcf fi') THEN 'TCF'
      WHEN lower(t3.SectorTeam) IN ('pf corp', 'pf fi') THEN 'PF'
      WHEN lower(t3.SectorTeam) in ('rural') THEN 'Rural'
    END AS GlobalBusinessLine
  , CASE
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank - RANZ Country Banking AND ROS','Rabobank - USA Rabo AgriFinance','Rabobank Australia','Rabobank New Zealand') AND t2.SourceClient like 'NP%' THEN 'NP'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Argentina', 'Rabobank Milan', 'Rabobank Paris', 'Rabobank Madrid', 'Rabobank Dublin', 'Rabobank Antwerp', 'Rabobank Turkey', 'Rabobank Frankfurt') AND (t3.KYCGroup IS NULL) THEN t2.FullLegalName
      ELSE t3.KYCGroup
    END AS KYCGroup
  , t4.ListedCorporatesInGroup
  , t4.TotalEntitiesInGroup
  , t3.SectorTeam
  , t3.ClientCaseInitiationStart
  , t3.CDDExecution
  -- , t3.CDDExecutionDaysincurrentcasephase
  , CASE
      WHEN t2.ClientType <> 'Legal Entity' THEN 'Natural Person' -- hardcoding Reason = Natural Person for all NP parties
      ELSE t3.Reason
    END AS Reason
  , CASE
      WHEN t2.ClientType <> 'Legal Entity' THEN 3 -- hardcoding CategoryNr = 3 for all NP parties
      WHEN t3.Reason = 'Associated Entity' THEN 4
      ELSE COALESCE(t3.CategoryNr, t9.GlobalFiles)
    END AS CategoryNr
  , t3.ReasonExplanation AS Explanation
  , t5.OnboardingDate
  , t9.GlobalFiles
  , t10.GlobalFiles AS ApprovedGlobalFiles
  , t3.LondonSectorTeam
  , t6.TradeName
  , t2.ClientApprovalDate
  , t2.EDL_LoadDate
  , TO_DATE(COALESCE(t2.FinalDecisionDate, t13.FinalDecisionDate), 'dd-MM-yyyy') AS SignOffDate -- used to be t12.FinalDecisionDate
  , t11.ReviewLocation
  , CASE 
      WHEN lower(t3.SectorTeam) IN ('trst', 'etc', 'tm', 'pf corp', 'ef corp', 'ef', 'fi', 'vcf', 'ef fi', 'core lending etc', 'tcf corp') THEN 'FOS ETC&TRUST'
      WHEN lower(t3.SectorTeam) IN ('cns', 'ref') THEN 'FOS CNS&REF'
      WHEN lower(t3.SectorTeam) IN ('fa', 'core lending fa') OR (lower(t3.SectorTeam) = 'core lending' AND lower(t11.ReviewLocation) IN ('kyc eu hub', 'kyc sc', 'kyc uk')) THEN 'FOS FA'
      WHEN lower(t3.SectorTeam) IN ('htd', 'sponsor coverage', 'af') THEN 'FOS HTD'
    END AS FOSTeamScope
  , COALESCE(t13.ValidatedRiskLevel, t2.ValidatedRiskLevel) AS ValidatedRiskLevel
    
  /*
    Region products involvement type
  */
  , CASE
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya') AND t9.EAInvolved = 1 THEN 'Lead - Products Involved'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya') AND t9.EAInvolved = 0 THEN 'Lead - No Products'
      WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya') AND t9.EAInvolved = 1 THEN 'Non-Lead - Products Involved'
      WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya') AND t9.EAInvolved = 0 THEN 'Non-Lead - No Products'
    END AS EAProductsInvolvmentType
  , CASE
      WHEN t2_asia.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9_asia.AsiaInvolved = 1 THEN 'Lead - Products Involved'
      WHEN t2_asia.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9_asia.AsiaInvolved = 0 THEN 'Lead - No Products'
      WHEN t2_asia.GlobalClientOwnerLocation NOT IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9_asia.AsiaInvolved = 1 THEN 'Non-Lead - Products Involved'
      WHEN t2_asia.GlobalClientOwnerLocation NOT IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9_asia.AsiaInvolved = 0 THEN 'Non-Lead - No Products'
    END AS AsiaProductsInvolvmentTypeLatestCompleted -- only for Asia, involvment type only considering lasted and completed cases
  , CASE
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9.AsiaInvolved = 1 THEN 'Lead - Products Involved'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9.AsiaInvolved = 0 THEN 'Lead - No Products'
      WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9.AsiaInvolved = 1 THEN 'Non-Lead - Products Involved'
      WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') AND t9.AsiaInvolved = 0 THEN 'Non-Lead - No Products'
    END AS AsiaProductsInvolvmentType
  , CASE
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') AND t9.NAInvolved = 1 THEN 'Lead - Products Involved'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') AND t9.NAInvolved = 0 THEN 'Lead - No Products'
      WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') AND t9.NAInvolved = 1 THEN 'Non-Lead - Products Involved'
      WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') AND t9.NAInvolved = 0 THEN 'Non-Lead - No Products'
    END AS NAProductsInvolvmentType
  , CASE
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Chile', 'Rabobank Brazil') AND t9.SAInvolved = 1 THEN 'Lead - Products Involved'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Chile', 'Rabobank Brazil') AND t9.SAInvolved = 0 THEN 'Lead - No Products'
      WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank Chile', 'Rabobank Brazil') AND t9.SAInvolved = 1 THEN 'Non-Lead - Products Involved'
      WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank Chile', 'Rabobank Brazil') AND t9.SAInvolved = 0 THEN 'Non-Lead - No Products'
    END AS SAProductsInvolvmentType
  , CASE
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') AND t9.RANZInvolved = 1 THEN 'Lead - Products Involved'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') AND t9.RANZInvolved = 0 THEN 'Lead - No Products'
      WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') AND t9.RANZInvolved = 1 THEN 'Non-Lead - Products Involved'
      WHEN t2.GlobalClientOwnerLocation NOT IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') AND t9.RANZInvolved = 0 THEN 'Non-Lead - No Products'
    END AS RANZProductsInvolvmentType
  
  , t12.N2KLocationsList
  , t2.GCDSID
  , datediff(t3.CDDExecution, t2.Prework) as preworkcheckSLA
FROM radar.firebird_master t1
INNER JOIN radar.firebird_master t2 ON t1.LatestId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId
-- lastet completed file to define n2k in Asia
LEFT JOIN radar.firebird_master t2_asia ON t1.LatestCompletedCaseId = t2_asia.CaseId AND t1.UniqueGcobId = t2_asia.UniqueGcobId

LEFT JOIN PortfolioPlanning t3 ON t2.UniqueGcobId = t3.UniqueGcobId
LEFT JOIN group_cte t4 ON LOWER(t3.KYCGroup) = t4.KYCGroup -- this is all in lower cases for grouping the kycgroup correctly

LEFT JOIN onboarding_cte t5 ON t2.UniqueGcobId = t5.UniqueGcobId
LEFT JOIN Client_TradeName t6 ON t2.ClientId = t6.Id AND t2.ClientType = 'Legal Entity'
LEFT JOIN GlobalAndInvolvedFiles t9 ON t2.SourceClient = t9.SourceClient
-- lastet completed file to define n2k in Asia
LEFT JOIN GlobalAndInvolvedFiles t9_asia ON t1.CompletedId = t9_asia.ClientId AND t1.UniqueGcobId = t9_asia.UniqueGcobId

LEFT JOIN GlobalAndInvolvedFiles t10 ON COALESCE(t2.LastFullReviewId, t2.CompletedId, t2.LatestId) = t10.ClientId AND t2.GcobId = t10.GcobId AND t2.ClientType = t10.ClientType -- this is for ApprovedGlobalFiles

LEFT JOIN ReviewLocation_cte t11 ON t2.UniqueGcobId = t11.UniqueGcobId

LEFT JOIN N2KLocationsList_cte t12 ON t2.UniqueGcobId = t12.UniqueGcobId

-- LEFT JOIN nrd_cte t122 ON t2.UniqueGcobId = t122.UniqueGcobId -- nrd and signoff date of the latest completed case with a nrd
-- this join is for all the risks categories + NRD //// substituting t122 above
LEFT JOIN radar.firebird_master t13 ON COALESCE(t2.CompletedId, t2.LatestId) = t13.ClientId AND t2.UniqueGcobId = t13.UniqueGcobId -- for ValidatedRiskLevel 

""").createOrReplaceTempView('clients_historical')

# COMMAND ----------

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.clients_historical')
    spark.sql('SELECT * FROM clients_historical').write.mode('overwrite').saveAsTable('radar.clients_historical')

else:
    spark.sql('SELECT * FROM clients_historical').write.mode('append').saveAsTable('radar.clients_historical')

# COMMAND ----------

# DBTITLE 1,dropping cases_temp not needed anymore
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.Cases_temp_historical -- dropping Cases_temp_historical not needed anymore

# COMMAND ----------

# DBTITLE 1,drop radar.firebird_master_latests_caseid_temp
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.firebird_master

# COMMAND ----------

# DBTITLE 1,raise error and stop job if UniqueGcobId duplication
duplicated_uniquegcobid = spark.sql(f"""
    SELECT UniqueGcobId 
    FROM radar.clients_historical 
    WHERE DATE(EDL_LoadDate) = DATE('{EDL_LoadDate}')
    GROUP BY UniqueGcobId 
    HAVING count(*) > 1
""")

# check for duplication
if not duplicated_uniquegcobid.isEmpty():
    raise Exception('Duplicated UniqueGcobId in clients. Job stopped.')

# COMMAND ----------

# MAGIC %md
# MAGIC ### n2klocation and productandservices NO need for historical tables, since is based on sourceclient it is possible to retrieve the information from the normal tables!!
