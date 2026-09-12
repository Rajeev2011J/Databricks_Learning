# Databricks notebook source
# DBTITLE 1,create date widget
dbutils.widgets.text('date_param', '', label = None)

# COMMAND ----------

# DBTITLE 1,define current load_date from widget
from datetime import datetime

if dbutils.widgets.get('date_param') == '': # if empty, set up default value = 20240101
    date_parameter = '20240101'
else:
    date_parameter = dbutils.widgets.get('date_param')

load_date = date_parameter
EDL_LoadDate = datetime.strptime(date_parameter, '%Y%m%d').strftime('%Y-%m-%d')

# COMMAND ----------

# DBTITLE 1,import packages and define connections
import os
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
from pyspark.sql import functions as F

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

# DBTITLE 1,Load GCOB tables
import re

# load and create temp views of all gdp_tables below
gcob_tables = [
    'CaseService_case_Case'
    , 'RiskModel_dbo_InstanceAnswer'
    , 'RiskModel_dbo_PossibleAnswer'
    , 'CaseService_dbo_CddType'
    , 'CaseService_case_DynamicRiskModelInstanceReference'
    , 'CaseService_NaturalPerson_DynamicRiskModelInstanceReference'
    , 'CaseService_dbo_Country'
    , 'CaseService_case_HighRiskProductReference'
    , 'CaseService_NaturalPerson_HighRiskProductReference'
    , 'CaseService_case_LegalEntityClientStructureSnapshot'
    , 'CaseService_snapshot_ClientStructureSnapshotRelationshipDetail'
    , 'CaseService_snapshot_ClientStructureSnapshotCalculatedUboShareholdingDetail'
    , 'CaseService_case_RelatedNaturalPersonParty'
    , 'CaseService_case_PoliticallyExposedPersonStatusReference'
    , 'CaseService_case_RelatedNaturalPersonPartyNationality'
    , 'CaseService_case_Address'
    , 'CaseService_case_ScreeningResults'
    , 'CaseService_case_ProductProvidedToLegalEntity'
    , 'CaseService_dbo_RabobankEntity'
    , 'CaseService_dbo_Product'
    , 'CaseService_NaturalPerson_ProductProvidedToNaturalPerson'
    , 'CaseService_case_LegalEntityBusinessActivity'
    , 'CaseService_dbo_Naics'
    , 'CaseService_NaturalPerson_NaturalPersonBusinessActivity'
    , 'CaseService_case_LegalEntityClient'
]

# mapping missing dates in GDP
date_mapping = {
    '20250301': '20250228',
    '20241101': '20241031',
    '20240301': '20240228',
    '20260401': '20260331',
}

load_date_adjusted = date_mapping.get(load_date, load_date)

for item in gcob_tables:
    version = 100

    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/{version}/data/LOADED_DTS={load_date_adjusted}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# DBTITLE 1,Load party_products_and_sevices
import re

item = 'party_products_and_services'

# mapping missing dates in GDP
date_mapping = {
    '20250301': '20250228',
    '20241101': '20241031',
    '20240301': '20240228',
}

load_date_adjusted = date_mapping.get(load_date, load_date)

version = 102

spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/LOAD_DT={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/'
files = dbutils.fs.ls(path)
max(file.path.split('LOAD_DT=')[1][:8] for file in files if 'LOAD_DT=' in file.path)

# COMMAND ----------

# DBTITLE 1,ebx_static_country_list_raw
version = 1

spark.read.parquet(f'abfss://ebx@{ReadStorage}.dfs.core.windows.net/EBX_FECCountryRiskList/{version}/data/EDL_LOAD_DT={load_date}/*.parquet').createOrReplaceTempView('ebx_static_country_list_raw')

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraCountryList

# COMMAND ----------

# DBTITLE 1,SiraCountryList
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraCountryList AS
# MAGIC
# MAGIC SELECT
# MAGIC   COUNTRYNAMEENGLISH AS country_name
# MAGIC   , COUNTRYCODE AS ISO_code
# MAGIC   , MONEYLAUNDERINGRISK AS Money_Laundering_Risk
# MAGIC   , TERRORISMFINANCINGRISK AS Terrorism_Financing_Risk
# MAGIC   , SANCTIONSRISK AS Sanctions_Risk
# MAGIC   , CORRUPTIONRISK AS Corruption_Risk
# MAGIC   , TAXINTEGRITYRISK AS Tax_Integrity_Risk
# MAGIC   , ECNONCOOPERATIVEJURISDICTIONS AS EC_Non_Cooperative_Jurisdictions
# MAGIC   , HIGHFATFRISK AS High_FATF_Risk
# MAGIC   , ECHIGHRISK AS EC_High_Risk
# MAGIC   , TOTALITARIANREGIMESRISK AS Totalitarian_Regimes
# MAGIC FROM ebx_static_country_list_raw
# MAGIC WHERE VALIDTILL IS NULL

# COMMAND ----------

# DBTITLE 1,save table SiraCountryList
from pyspark.sql.functions import lit, to_date

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.SiraCountryList_historical')
    spark.sql('SELECT * FROM SiraCountryList').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.SiraCountryList_historical')

else:
    spark.sql('SELECT * FROM SiraCountryList').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.SiraCountryList_historical')

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraClients

# COMMAND ----------

# DBTITLE 1,SiraClients
spark.sql(f"""

WITH last_completed_case_no_ea AS (
  SELECT * FROM (
    SELECT
      *
      , ROW_NUMBER() OVER (PARTITION BY UniqueGcobId ORDER BY ClientId DESC) AS rn
    FROM radar.cases_historical
    WHERE CaseStatusName = 'Completed'
      AND (CaseReviewType IS NULL OR CaseReviewType <> 'Event Assessment')
      AND EDL_LoadDate = DATE('{EDL_LoadDate}')
  ) subquery WHERE rn = 1
)

SELECT
  t2.UniqueGcobId
  , t2.SourceSystemReference

  , t2.ClientId AS SiraClientId -- considering last completed case, excluding EA
  -- , COALESCE(t1.CompletedId, t1.LatestId) AS SiraClientId --  ApprovedCaseId (previously including t1.LastFullReviewId) -- 22/09/25 removed t1.LastFullReviewId to capture all completed cases

  , t2.SourceClient

  , t2.FullLegalName
  , t1.KYCGroup
  , t1.SectorTeam
  , t1.GlobalKYCPortfolioNew
  , t2.FIHubIndicator_Derived
  , t2.BusinessLineName
  , t2.ClientLifeCycleName

  , t2.ValidatedRiskLevel -- from radar.clients where it is based on coalesce(completedid, latestid)

  , t2.NextReviewDate
  , t2.GlobalClientOwner
  , t2.GlobalClientOwnerLocation
-- validated risks level, meaning last completed/latest caseid
  , t2.GeographicalRiskLevel
  , t2.EntityTypeRiskLevel
  , t2.StructureRiskLevel
  , t2.SectorRiskLevel
  , t2.ProductAndServiceRiskLevel
  , t2.PEPRiskLevel
  , t2.TransactionRiskLevel
  , t2.DistributionRiskLevel
  , t2.ThirdPartyRiskLevel
  , t2.AdverseInfoRiskLevel
  , t2.OtherRiskLevel
  , t2.FatcaClassification
  , t2.CrsClassification

  , CASE 
      WHEN t2.SourceSystemReference = 'GCOB_NP-NPPC' THEN
        CASE 
          WHEN t2.GeographicalRiskLevel = 'Incomplete' OR t2.GeographicalRiskLevel IS NULL
            OR t2.SectorRiskLevel = 'Incomplete' OR t2.SectorRiskLevel IS NULL
            OR t2.ProductAndServiceRiskLevel = 'Incomplete' OR t2.ProductAndServiceRiskLevel IS NULL
            OR t2.PEPRiskLevel = 'Incomplete' OR t2.PEPRiskLevel IS NULL
            OR t2.TransactionRiskLevel = 'Incomplete' OR t2.TransactionRiskLevel IS NULL
            OR t2.DistributionRiskLevel = 'Incomplete' OR t2.DistributionRiskLevel IS NULL
            OR t2.ThirdPartyRiskLevel = 'Incomplete' OR t2.ThirdPartyRiskLevel IS NULL
            OR t2.AdverseInfoRiskLevel = 'Incomplete' OR t2.AdverseInfoRiskLevel IS NULL
            THEN 0 
          WHEN t1.LatestCompletedCaseId IS NULL THEN 0 
          ELSE 1
        END
      ELSE 
        CASE 
          WHEN t2.GeographicalRiskLevel = 'Incomplete' OR t2.GeographicalRiskLevel IS NULL
            OR t2.EntityTypeRiskLevel = 'Incomplete' OR t2.EntityTypeRiskLevel IS NULL
            OR t2.StructureRiskLevel = 'Incomplete' OR t2.StructureRiskLevel IS NULL
            OR t2.SectorRiskLevel = 'Incomplete' OR t2.SectorRiskLevel IS NULL
            OR t2.ProductAndServiceRiskLevel = 'Incomplete' OR t2.ProductAndServiceRiskLevel IS NULL
            OR t2.PEPRiskLevel = 'Incomplete' OR t2.PEPRiskLevel IS NULL
            OR t2.TransactionRiskLevel = 'Incomplete' OR t2.TransactionRiskLevel IS NULL
            OR t2.DistributionRiskLevel = 'Incomplete' OR t2.DistributionRiskLevel IS NULL
            OR t2.ThirdPartyRiskLevel = 'Incomplete' OR t2.ThirdPartyRiskLevel IS NULL
            OR t2.AdverseInfoRiskLevel = 'Incomplete' OR t2.AdverseInfoRiskLevel IS NULL
            THEN 0 
          WHEN t1.LatestCompletedCaseId IS NULL THEN 0 
          ELSE 1 
        END
    END AS EntityHasAllData

  , t1.OnboardingDate
  , t2.CddType
  , t1.Reason
  , CASE
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Kenya') THEN 'E&A'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') THEN 'Asia'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') THEN 'North America'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Chile', 'Rabobank Brazil', 'Rabobank Argentina') THEN 'South America'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') THEN 'RANZ'
      WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Foundation') THEN 'Rabobank Foundation'
    END AS GlobalReportingRegion
  , t1.GlobalBusinessLine

FROM radar.clients_historical t1
INNER JOIN last_completed_case_no_ea t2 ON t1.UniqueGcobId = t2.UniqueGcobId -- 08/01/26 considering last completed cases, excluding Event Assessment which is does not contain risk and other information
-- LEFT JOIN radar.cases_historical t2 ON COALESCE(t1.CompletedId, t1.LatestId) = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId
-- INNER JOIN radar.cases_historical t2 ON t1.CompletedId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId -- 22/09/25 removed this t1.LastFullReviewId + 16/10/2025 also t1.LatestId to show only completed

WHERE t1.EDL_LoadDate = DATE('{EDL_LoadDate}')
  -- AND t2.EDL_LoadDate = DATE('{EDL_LoadDate}')

""").createOrReplaceTempView('SiraClients')

# COMMAND ----------

# DBTITLE 1,save table SiraClients
from pyspark.sql.functions import lit, to_date

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.SiraClients_historical')
    spark.sql('SELECT * FROM SiraClients').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.SiraClients_historical')

else:
    spark.sql('SELECT * FROM SiraClients').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.SiraClients_historical')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if UniqueGcobId duplication
duplicated_uniquegcobid = spark.sql(f"""
    SELECT UniqueGcobId 
    FROM radar.siraclients_historical
    WHERE DATE(EDL_LoadDate) = DATE('{EDL_LoadDate}')
    GROUP BY UniqueGcobId 
    HAVING count(*) > 1
""")

# check for duplication
if not duplicated_uniquegcobid.isEmpty():
    raise Exception('Duplicated UniqueGcobId in siraclients. Job stopped.')

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraClientRisks

# COMMAND ----------

# DBTITLE 1,InstancePerLEClientId
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW InstancePerLEClientId AS
# MAGIC WITH CTE_InstanceIdPerLegalEntityId AS (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.MaxId
# MAGIC     , t2.NotExpiredId
# MAGIC     , t1.LegalEntityClientId
# MAGIC     , t3.InstanceId
# MAGIC   FROM (
# MAGIC     SELECT DISTINCT
# MAGIC       MAX(Id) AS MaxId
# MAGIC       , LegalEntityClientId
# MAGIC     FROM CaseService_case_DynamicRiskModelInstanceReference 
# MAGIC     GROUP BY LegalEntityClientId
# MAGIC   ) AS t1
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       MAX(Id) AS NotExpiredId
# MAGIC       , LegalEntityClientId
# MAGIC     FROM CaseService_case_DynamicRiskModelInstanceReference t1
# MAGIC     WHERE Expired IS NULL
# MAGIC     GROUP BY LegalEntityClientId
# MAGIC   ) AS t2 ON t2.LegalEntityClientId = t1.LegalEntityClientId
# MAGIC   LEFT JOIN CaseService_case_DynamicRiskModelInstanceReference t3 ON t3.Id = COALESCE(t2.NotExpiredId, t1.MaxId)
# MAGIC )
# MAGIC SELECT
# MAGIC     LegalEntityClientId
# MAGIC     , InstanceId
# MAGIC FROM CTE_InstanceIdPerLegalEntityId

# COMMAND ----------

# DBTITLE 1,InstancePerNPClientId
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW InstancePerNPClientId AS
# MAGIC WITH CTE_InstanceIdPerNaturalPersonClientId AS (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.MaxId
# MAGIC     , t2.NotExpiredId
# MAGIC     , t1.NaturalPersonClientId
# MAGIC     , t3.InstanceId
# MAGIC   FROM (
# MAGIC     SELECT DISTINCT
# MAGIC       MAX(Id) AS MaxId
# MAGIC       , NaturalPersonClientId
# MAGIC     FROM CaseService_NaturalPerson_DynamicRiskModelInstanceReference 
# MAGIC     GROUP BY NaturalPersonClientId
# MAGIC   ) AS t1
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       MAX(Id) AS NotExpiredId
# MAGIC       , NaturalPersonClientId
# MAGIC     FROM CaseService_NaturalPerson_DynamicRiskModelInstanceReference t1
# MAGIC     WHERE Expired IS NULL
# MAGIC     GROUP BY NaturalPersonClientId
# MAGIC   ) AS t2 ON t2.NaturalPersonClientId = t1.NaturalPersonClientId
# MAGIC   LEFT JOIN CaseService_NaturalPerson_DynamicRiskModelInstanceReference t3 ON t3.Id = COALESCE(t2.NotExpiredId, t1.MaxId)
# MAGIC )
# MAGIC SELECT
# MAGIC     NaturalPersonClientId
# MAGIC     , InstanceId
# MAGIC FROM CTE_InstanceIdPerNaturalPersonClientId

# COMMAND ----------

# DBTITLE 1,SiraClientRisks_LE
spark.sql(f"""

    WITH TxAlerts_CTE AS (
    SELECT 
        t1.SiraClientId
        , MIN(t4.Text) AS HasTxAlert
    FROM radar.siraclients_historical t1
    LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId 
    LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
    LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
    WHERE (t3.QuestionId BETWEEN 51 AND 60 OR t3.QuestionId BETWEEN 387 AND 396) 
        AND t4.Text = 'Yes'
        AND t1.SourceSystemReference = 'GCOB_LegalEntity'
        AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')
    GROUP BY t1.SiraClientId
    )

    SELECT DISTINCT
    pvt.*
    , COALESCE(t2.HasTxAlert, 'No') AS HasTxAlert
    FROM (
    SELECT
        t1.UniqueGcobId
        , t1.SiraClientId
        , t1.CddType
        , MAX(CASE WHEN t3.QuestionId IN (61, 383, 600) THEN t4.Text END) AS Face2FaceContact
        , MAX(CASE WHEN t3.QuestionId IN (66, 80, 220, 382, 606) THEN t4.Text END) AS AdverseInfo
        , MAX(CASE WHEN t3.QuestionId IN (5, 81, 221, 349, 540, 995) THEN t4.Text END) AS EntityType
        , MAX(CASE WHEN t3.QuestionId IN (18, 342, 552, 768) THEN t4.Text END) AS ComplexStructure
    FROM radar.siraclients_historical t1
    LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId
    LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
    LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
    WHERE t3.QuestionId IN (5, 18, 61, 66, 80, 81, 220, 221, 342, 349, 382, 383, 540, 552, 600, 606, 768, 995)
        AND t1.SourceSystemReference = 'GCOB_LegalEntity'
        AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')
    GROUP BY 1,2,3
    ) pvt -- pivot
    LEFT JOIN TxAlerts_CTE t2 ON pvt.SiraClientId = t2.SiraClientId

""").createOrReplaceTempView('SiraClientRisks_LE')

# COMMAND ----------

# DBTITLE 1,SiraClientRisks_NP
spark.sql(f"""

    WITH TxAlerts_CTE AS (
    SELECT 
        t1.SiraClientId
        , MIN(t4.Text) AS HasTxAlert
    FROM radar.siraclients_historical t1
    LEFT JOIN InstancePerNPClientId t2 ON t1.SiraClientId = t2.NaturalPersonClientId
    LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
    LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
    WHERE t3.QuestionId BETWEEN 281 AND 288
        AND t4.Text = 'Yes'
        AND t1.SourceSystemReference = 'GCOB_NP-NPPC'
        AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')
    GROUP BY t1.SiraClientId
    )

    SELECT DISTINCT
    pvt.UniqueGcobId
    , pvt.SiraClientId
    , pvt.CddType
    , MAX(CASE WHEN pvt.Question = 'Face2FaceContact' THEN pvt.Text ELSE '' END) AS Face2FaceContact
    , MAX(CASE WHEN pvt.Question = 'AdverseInfo' THEN pvt.Text ELSE '' END) AS AdverseInfo
    , pvt.CddType AS EntityType -- t4.QuestionId IN (516)
    , 'No' AS ComplexStructure -- t4.QuestionId IN (364)
    , MAX(COALESCE(t2.HasTxAlert, 'No')) AS HasTxAlert -- MAX to just avoid grouping by?
    FROM (
        SELECT
        t1.UniqueGcobId
        , t1.SiraClientId
        , t1.CddType
        , CASE 
            WHEN t3.QuestionId IN (261, 470, 674, 728) THEN 'Face2FaceContact'
            WHEN t3.QuestionId IN (264, 515, 676, 732) THEN 'AdverseInfo'
            END AS Question
        , t4.Text
        FROM radar.siraclients_historical t1
        LEFT JOIN InstancePerNPClientId t2 ON t1.SiraClientId = t2.NaturalPersonClientId
        LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t3.InstanceId = t3.InstanceId
        LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
        WHERE t3.QuestionId IN (264, 515, 261, 470, 676, 732, 674, 728)
            AND t1.SourceSystemReference = 'GCOB_NP-NPPC'
            AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')
    ) pvt
    LEFT JOIN TxAlerts_CTE t2 ON pvt.SiraClientId = t2.SiraClientId
    GROUP BY 1,2,3

""").createOrReplaceTempView('SiraClientRisks_NP')

# COMMAND ----------

# DBTITLE 1,SiraClientRisks
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraClientRisks AS
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM SiraClientRisks_LE
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM SiraClientRisks_NP

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.SiraClientRisks_historical')
    spark.sql('SELECT * FROM SiraClientRisks').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.SiraClientRisks_historical')

else:
    spark.sql('SELECT * FROM SiraClientRisks').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.SiraClientRisks_historical')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if UniqueGcobId duplication
duplicated_uniquegcobid = spark.sql('''
    SELECT UniqueGcobId 
    FROM radar.SiraClientRisks_historical
    WHERE DATE(EDL_LoadDate) = DATE('{EDL_LoadDate}')
    GROUP BY UniqueGcobId 
    HAVING count(*) > 1
''')

# check for duplication
if not duplicated_uniquegcobid.isEmpty():
    raise Exception('Duplicated UniqueGcobId in SiraClientRisks. Job stopped.')

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraGeoActivities

# COMMAND ----------

# DBTITLE 1,SiraGeoActivities
spark.sql(f"""

SELECT DISTINCT
  t1.UniqueGcobId
  , t1.SiraClientId
  , t3.QuestionId
  , CASE
      WHEN t3.QuestionId IN (1, 346, 423, 480, 527) THEN 'Geo Registration' -- 480 NP, 527 ??
      WHEN t3.QuestionId IN (4, 348, 426, 483, 531) THEN 'Geo Activities' -- 483 NP, 531
      WHEN t3.QuestionId IN (424, 528) THEN 'Geo UBO(s)' -- check 528
      WHEN t3.QuestionId IN (3, 425) THEN 'Geo Parent(s)'
      ELSE ''
    END AS QuestionName
  , t4.Text
  , t4.Value
  , t5.RiskScore
  -- , COALESCE(t6.Overall_Risk, '#N/A') AS OverallRisk
  , COALESCE(t6.Sanctions_Risk, '#N/A') AS SanctionRisk
  , COALESCE(t6.Money_Laundering_Risk, '#N/A') AS MLRisk
  , COALESCE(t6.Terrorism_Financing_Risk, '#N/A') AS TFRisk
  , COALESCE(t6.Tax_Integrity_Risk, '#N/A') AS TaxIntegrityRiskJurisdictions
  , COALESCE(t6.Corruption_Risk, '#N/A') AS CorruptionRisk
  , COALESCE(t6.EC_High_Risk, '#N/A') AS ECHighRiskThirdCountry

FROM radar.siraclients_historical t1
LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId
LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
LEFT JOIN CaseService_dbo_Country t5 ON t4.Value = t5.IsoCode
LEFT JOIN radar.siracountrylist_historical t6 ON t4.Value = t6.ISO_code AND t6.EDL_LoadDate = DATE('{EDL_LoadDate}')
WHERE t3.QuestionId IN (1, 346, 423, 4, 348, 426, 424, 3, 425, 531, 527, 480, 483, 528) -- what is q245 and q246?
  AND t5.Expired IS NULL
  AND t1.SourceSystemReference = 'GCOB_LegalEntity'
  AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

UNION

SELECT DISTINCT
  t1.UniqueGcobId
  , t1.SiraClientId
  , t3.QuestionId
  , CASE
      WHEN t3.QuestionId IN (245, 480, 695) THEN 'Geo Registration'
      WHEN t3.QuestionId IN (247, 483, 696) THEN 'Geo Activities'
      -- WHEN t3.QuestionId IN (2, 347, 481, 528) THEN 'Geo UBO(s)'
      -- WHEN t3.QuestionId IN (409, 482) THEN 'Geo Parent(s)'
      ELSE ''
  END AS QuestionName
  , t4.Text
  , t4.Value
  , t5.RiskScore
  -- , COALESCE(t6.Overall_Risk, '#N/A') AS OverallRisk
  , COALESCE(t6.Sanctions_Risk, '#N/A') AS SanctionRisk
  , COALESCE(t6.Money_Laundering_Risk, '#N/A') AS MLRisk
  , COALESCE(t6.Terrorism_Financing_Risk, '#N/A') AS TFRisk
  , COALESCE(t6.Tax_Integrity_Risk, '#N/A') AS TaxIntegrityRiskJurisdictions
  , COALESCE(t6.Corruption_Risk, '#N/A') AS CorruptionRisk
  , COALESCE(t6.EC_High_Risk, '#N/A') AS ECHighRiskThirdCountry

FROM radar.siraclients_historical t1
LEFT JOIN InstancePerNPClientId t2 ON t1.SiraClientId = t2.NaturalPersonClientId
LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id --? and t3.QuestionId = t4.QuestionId
LEFT JOIN CaseService_dbo_Country t5 ON t4.Value = t5.IsoCode
LEFT JOIN radar.siracountrylist_historical t6 ON t4.Value = t6.ISO_code AND t6.EDL_LoadDate = DATE('{EDL_LoadDate}')
WHERE t3.QuestionId IN (245, 480, 247, 483, 695, 696) -- 2, 347, 481, 528, 409, 482 & what is q245 and q246?
  AND t5.Expired IS NULL
  AND t1.SourceSystemReference = 'GCOB_NP-NPPC'
  AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

""").createOrReplaceTempView('SiraGeoActivities')

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.SiraGeoActivities_historical')
    spark.sql('SELECT * FROM SiraGeoActivities').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.SiraGeoActivities_historical')

else:
    spark.sql('SELECT * FROM SiraGeoActivities').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.SiraGeoActivities_historical')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if UniqueGcobId duplication
duplicated_uniquegcobid = spark.sql(f"""
    SELECT UniqueGcobId 
    FROM (
        SELECT DISTINCT
            UniqueGcobid
            , QuestionName
            , TRIM(REPLACE(Text, '(the)', '')) AS TextClean
            , Value
        FROM radar.SiraGeoActivities
        WHERE DATE(EDL_LoadDate) = DATE('{EDL_LoadDate}')
    )
    WHERE QuestionName = 'Geo Registration' -- geo activities can have multiple results!!
    -- AND UniqueGcobId NOT IN ('12476', '8831') -- excluding those two gcobids having double questionids for geo registration
    GROUP BY UniqueGcobId
    HAVING COUNT(*) > 1
""")

# check for duplication
if not duplicated_uniquegcobid.isEmpty():
    raise Exception('Duplicated UniqueGcobId in clients. Job stopped.')

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraHighRiskProducts

# COMMAND ----------

# DBTITLE 1,SiraHighRiskProducts
spark.sql(f"""

SELECT DISTINCT
  t1.UniqueGcobId
  , t1.SiraClientId
  , t3.QuestionId
  , CASE
      WHEN t3.QuestionId IN (26, 77, 405, 568, 702) THEN 'High Risk Products'
      WHEN t3.QuestionId IN (32, 407, 570, 783) THEN 'Products Commensurate'
      WHEN t3.QuestionId IN (34, 408, 571, 785) THEN 'Product Source Of Fund Consistent'
      ELSE ''
    END AS QuestionName
  , CASE
      WHEN t3.QuestionId IN (26, 77, 405, 568, 702) AND t1.UniqueGcobId <> '86773' THEN COALESCE(t5.HighRiskProductName, t4.Value)
      WHEN t3.QuestionId IN (26, 77, 405, 568, 702) AND t1.UniqueGcobId = '86773' THEN 'None' -- hard coded for this gcobid containing multiple possible answer in RiskModel_dbo_InstanceAnswer!!
      ELSE ''
    END AS HighRiskProductName
  , CASE
      WHEN t3.QuestionId IN (26, 77, 405, 568, 702) AND t1.UniqueGcobId <> '86773' THEN COALESCE(t5.HighRiskProductRiskScore, t4.Score)
      WHEN t3.QuestionId IN (26, 77, 405, 568, 702) AND t1.UniqueGcobId = '86773' THEN '0.0000' -- hard coded for this gcobid containing multiple possible answer in RiskModel_dbo_InstanceAnswer!!
      ELSE ''
    END AS HighRiskProductRiskScore
  , CASE
      WHEN t3.QuestionId IN (32, 407, 570, 783, 34, 408, 571, 785) AND t1.UniqueGcobId <> '86773' THEN t4.Text
      WHEN t3.QuestionId IN (32, 407, 570, 783, 34, 408, 571, 785) AND t1.UniqueGcobId = '86773' THEN 'Yes' -- hard coded for this gcobid containing multiple possible answer in RiskModel_dbo_InstanceAnswer!!
      ELSE ''
    END AS ProductsCommensurateANDSourceOfFunds

FROM radar.siraclients_historical t1
LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId
LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
LEFT JOIN CaseService_case_HighRiskProductReference t5 ON CAST(t4.Value AS STRING) = CAST(t5.HighRiskProductId AS STRING)
WHERE t3.QuestionId IN (26, 77, 405, 568, 702, 32, 407, 570, 34, 408, 571, 783, 785)
  AND t1.SourceSystemReference = 'GCOB_LegalEntity'
  AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

UNION

SELECT DISTINCT
  t1.UniqueGcobId
  , t1.SiraClientId
  , t3.QuestionId
  , CASE
      WHEN t3.QuestionId IN (484, 652, 702) THEN 'High Risk Products'
      WHEN t3.QuestionId IN (32, 251, 486, 653, 703, 834) THEN 'Products Commensurate'
      WHEN t3.QuestionId IN (34, 254, 487, 654, 704, 785) THEN 'Product Source Of Fund Consistent'
      ELSE ''
  END AS QuestionName
  , CASE
      WHEN t3.QuestionId IN (484, 652, 702) THEN COALESCE(t5.HighRiskProductName, t4.Value)
      ELSE ''
  END AS HighRiskProductName
  , CASE
      WHEN t3.QuestionId IN (484, 652, 702) THEN t4.Score
      ELSE ''
  END AS HighRiskProductRiskScore
  , CASE
      WHEN t3.QuestionId IN (32, 251, 486, 653, 703, 834, 34, 254, 487, 654, 704, 785) THEN t4.Text
      ELSE ''
  END AS ProductsCommensurateANDSourceOfFunds

FROM radar.siraclients_historical t1
LEFT JOIN InstancePerNPClientId t2 ON t1.SiraClientId = t2.NaturalPersonClientId
LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
LEFT JOIN CaseService_NaturalPerson_HighRiskProductReference t5 ON CAST(t4.Value AS STRING) = CAST(t5.HighRiskProductId AS STRING)
WHERE t3.QuestionId IN (484, 652, 702, 32, 251, 486, 653, 703, 834, 34, 254, 487, 654, 704, 785)
  AND t1.SourceSystemReference = 'GCOB_NP-NPPC'
  AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

""").createOrReplaceTempView('SiraHighRiskProducts')

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.SiraHighRiskProducts_historical')
    spark.sql('SELECT * FROM SiraHighRiskProducts').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.SiraHighRiskProducts_historical')

else:
    spark.sql('SELECT * FROM SiraHighRiskProducts').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.SiraHighRiskProducts_historical')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **NO CHECKS ON radar.SiraHighRiskProducts!!**

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraPEPUBO

# COMMAND ----------

# DBTITLE 1,SiraPEPUBO
spark.sql(f"""

WITH MaxClientStructureSnapshotId_CTE AS (
  SELECT 
    LegalEntityClientId
    , MAX(ClientStructureSnapshotId) AS MaxClientStructureSnapshotId
  FROM CaseService_case_LegalEntityClientStructureSnapshot
  GROUP BY LegalEntityClientId
)

SELECT DISTINCT
  t1.UniqueGcobId
  , t1.SiraClientId
  , t2.MaxClientStructureSnapshotId
  -- , t3.RelationshipId
  -- , t3.ChildIdentity
  , t3.ParentIdentity
  -- , t3.ParentType
  , t4.UboThroughReasonReferenceId
  , t4.IsUbo
  , CASE 
      WHEN t4.UboThroughReasonReferenceId = 1 THEN 'Ownership'
      WHEN t4.UboThroughReasonReferenceId = 2 THEN 'Voting Rights'
      WHEN t4.UboThroughReasonReferenceId = 3 THEN '...'
      WHEN t4.UboThroughReasonReferenceId = 4 THEN 'Factual Effective Control'
      WHEN t4.UboThroughReasonReferenceId = 5 THEN 'Senior Managing Official'
  END AS UboReason
  , t5.Identity
  , t5.PoliticallyExposedPersonStatusId
  , t6.Name AS PEPStatus
  , CASE
      WHEN t5.PoliticallyExposedPersonStatusId IN (1,2,4,5) THEN 'PEP'
      ELSE 'Not PEP'
    END AS IsPEP
  , t12.SanctionsOrExternalWatchlist AS SanctionsOrExternalWatchlist
  , t7.CountryReferenceId
  , t8.Name AS PEPNationality
  , t10.Name AS ResidentialAddressCountry
  , t11.Corruption_Risk AS CorruptionRisk
  -- , t11.Overall_Risk AS OverallRiskOfPEPResidentialAddress
  , t11.EC_High_Risk AS ECHighRiskThirdCountry

FROM radar.siraclients_historical t1
LEFT JOIN MaxClientStructureSnapshotId_CTE t2 ON t1.SiraClientId = t2.LegalEntityClientId
LEFT JOIN CaseService_snapshot_ClientStructureSnapshotRelationshipDetail t3 ON t2.MaxClientStructureSnapshotId = t3.ClientStructureSnapshotId
LEFT JOIN CaseService_snapshot_ClientStructureSnapshotCalculatedUboShareholdingDetail t4 ON t2.MaxClientStructureSnapshotId = t4.ClientStructureSnapshotId
  AND t3.ParentIdentity = t4.RelatedPartyIdentity
LEFT JOIN CaseService_case_RelatedNaturalPersonParty t5 ON t3.ParentIdentity = t5.RelatedNaturalPersonPartyId
LEFT JOIN CaseService_case_PoliticallyExposedPersonStatusReference t6 ON t5.PoliticallyExposedPersonStatusId = t6.PoliticallyExposedPersonStatusId
LEFT JOIN CaseService_case_RelatedNaturalPersonPartyNationality t7 ON t5.RelatedNaturalPersonPartyId = t7.RelatedNaturalPersonPartyId
LEFT JOIN CaseService_dbo_Country t8 ON t7.CountryReferenceId = t8.Id
LEFT JOIN CaseService_case_Address t9 ON t5.AddressId = t9.AddressId
LEFT JOIN CaseService_dbo_Country t10 ON t9.CountryReferenceId = t10.Id
LEFT JOIN radar.SiraCountryList t11 ON t10.IsoCode = t11.ISO_code
LEFT JOIN CaseService_case_ScreeningResults t12 ON t5.ScreeningResultsId = t12.ScreeningResultsId
WHERE t1.SourceSystemReference = 'GCOB_LegalEntity'
  AND t3.ParentType = 2 
  AND (t5.PoliticallyExposedPersonStatusId IN (1,2,4,5) OR t4.IsUbo = 1)
  AND t3.Expired IS NULL
  AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')
  -- AND t5.Expired IS NULL -- added this!! -> WRONG!!

-- REMOVED BELOW CONDITIONS OTHERWISE IDENTITY IS LINKED TO AN EXPIRED CountryReferenceId AND NOT SHOWN!!
  -- AND t8.Expired IS NULL
  -- AND t10.Expired IS NULL

""").createOrReplaceTempView('SiraPEPUBO')

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.SiraPEPUBO_historical')
    spark.sql('SELECT * FROM SiraPEPUBO').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.SiraPEPUBO_historical')

else:
    spark.sql('SELECT * FROM SiraPEPUBO').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.SiraPEPUBO_historical')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if duplicated identity in each UniqueGcobId
duplicated_identity = spark.sql('''
    SELECT UniqueGcobId, Identity 
    FROM (SELECT DISTINCT UniqueGcobId, Identity FROM radar.SiraPEPUBO_historical WHERE DATE(EDL_LoadDate) = DATE('{EDL_LoadDate}'))
    GROUP BY UniqueGcobId, Identity 
    HAVING COUNT(*) > 1
''')

# check for duplication
if not duplicated_identity.isEmpty():
    raise Exception('Duplicated Identity per UniqueGcobId. Job stopped.')

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraProductAndServices

# COMMAND ----------

# DBTITLE 1,SiraProductAndServices
spark.sql(f"""

SELECT DISTINCT
  t1.UniqueGcobId
  , t1.SiraClientId
  , t2.ProductReferenceId
  , t2.IsOtc
  , t2.ProductLifecycle
  , t3.Name AS ProductOfferingLocation
  , t4.Name AS BookingEntityLocation
  , t5.Name AS ProductName

FROM radar.siraclients_historical t1
LEFT JOIN CaseService_case_ProductProvidedToLegalEntity t2 ON t1.SiraClientId = t2.LegalEntityId
LEFT JOIN CaseService_dbo_RabobankEntity t3 ON t2.RabobankEntityReferenceIdOfProductLocation = t3.Id
LEFT JOIN CaseService_dbo_RabobankEntity t4 ON t2.RabobankEntityReferenceIdOfBookingLocation = t4.Id
LEFT JOIN CaseService_dbo_Product t5 ON t2.ProductReferenceId = t5.Id
WHERE t1.SourceSystemReference = 'GCOB_LegalEntity'
    AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

UNION ALL

SELECT DISTINCT
  t1.UniqueGcobId
  , t1.SiraClientId
  , t2.ProductReferenceId
  , t2.IsOtc
  , t2.ProductLifecycle
  , t3.Name AS ProductOfferingLocation
  , t4.Name AS BookingEntityLocation
  , t5.Name AS ProductName

FROM radar.siraclients_historical t1
LEFT JOIN CaseService_NaturalPerson_ProductProvidedToNaturalPerson t2 ON t1.SiraClientId = t2.NaturalPersonClientId
LEFT JOIN CaseService_dbo_RabobankEntity t3 ON t2.RabobankEntityReferenceIdOfProductLocation = t3.Id
LEFT JOIN CaseService_dbo_RabobankEntity t4 ON t2.RabobankEntityReferenceIdOfBookingLocation = t4.Id
LEFT JOIN CaseService_dbo_Product t5 ON t2.ProductReferenceId = t5.Id
WHERE t1.SourceSystemReference = 'GCOB_NP-NPPC'
    AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

""").createOrReplaceTempView('SiraProductAndServices')

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.SiraProductAndServices_historical')
    spark.sql('SELECT * FROM SiraProductAndServices').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.SiraProductAndServices_historical')

else:
    spark.sql('SELECT * FROM SiraProductAndServices').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.SiraProductAndServices_historical')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if duplicated product per UniqueGcobId
duplicated_product = spark.sql('''
    SELECT UniqueGcobId, ProductReferenceId 
    FROM (SELECT DISTINCT UniqueGcobId, ProductReferenceId FROM radar.SiraProductAndServices_historical WHERE DATE(EDL_LoadDate) = DATE('{EDL_LoadDate}'))
    GROUP BY UniqueGcobId, ProductReferenceId 
    HAVING COUNT(*) > 1
''')

# check for duplication
if not duplicated_product.isEmpty():
    raise Exception('Duplicated ProductReferenceId per UniqueGcobId. Job stopped.')

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraNAICS

# COMMAND ----------

# DBTITLE 1,SiraNAICS
spark.sql(f"""

SELECT DISTINCT
  t1.UniqueGcobId
  , t1.SiraClientId
  , t2.NaicsReferenceId
  , t3.Id
  , t3.Name
  , t3.Code
  , t3.SectorGroup
  , t3.IndustryGroup
  , t3.IndustrySector
  , t3.HasRiskOfCorruption
  , t3.HasRiskOfMoneyLaunderingOrTerroristFinancing
  , t3.HasLicenseRequirements
  , t3.HasReputationRisk
  , t3.HasSanctions
  , t3.RiskScore
  , t3.Version
  , t3.Created
  , t3.Expired
  , t3.Previous

FROM radar.siraclients_historical t1
LEFT JOIN CaseService_case_LegalEntityBusinessActivity t2 ON t1.SiraClientId = t2.LegalEntityId
LEFT JOIN CaseService_dbo_Naics t3 ON t2.NaicsReferenceId = t3.Id
WHERE t1.SourceSystemReference = 'GCOB_LegalEntity'
  AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

UNION ALL

SELECT DISTINCT
  t1.UniqueGcobId
  , t1.SiraClientId
  , t2.NaturalPersonNaicsReferenceId AS NaicsReferenceId
  , t3.Id
  , t3.Name
  , t3.Code
  , t3.SectorGroup
  , t3.IndustryGroup
  , t3.IndustrySector
  , t3.HasRiskOfCorruption
  , t3.HasRiskOfMoneyLaunderingOrTerroristFinancing
  , t3.HasLicenseRequirements
  , t3.HasReputationRisk
  , t3.HasSanctions
  , t3.RiskScore
  , t3.Version
  , t3.Created
  , t3.Expired
  , t3.Previous

FROM radar.siraclients_historical t1
LEFT JOIN CaseService_NaturalPerson_NaturalPersonBusinessActivity t2 ON t1.SiraClientId = t2.NaturalPersonId
LEFT JOIN CaseService_dbo_Naics t3 ON t2.NaturalPersonNaicsReferenceId = t3.Id
WHERE t1.SourceSystemReference = 'GCOB_NP-NPPC'
  AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

""").createOrReplaceTempView('SiraNAICS')

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.SiraNAICS_historical')
    spark.sql('SELECT * FROM SiraNAICS').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.SiraNAICS_historical')

else:
    spark.sql('SELECT * FROM SiraNAICS').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.SiraNAICS_historical')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if duplicated naics code per UniqueGcobId
duplicated_naics = spark.sql('''
    SELECT UniqueGcobId, NaicsReferenceId 
    FROM (SELECT DISTINCT UniqueGcobId, NaicsReferenceId FROM radar.SiraNAICS_historical WHERE DATE(EDL_LoadDate) = DATE('{EDL_LoadDate}'))
    GROUP BY UniqueGcobId, NaicsReferenceId 
    HAVING COUNT(*) > 1
''')

# check for duplication
if not duplicated_naics.isEmpty():
    raise Exception('Duplicated NaicsReferenceId per UniqueGcobId. Job stopped.')

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraHighRiskActivities

# COMMAND ----------

# DBTITLE 1,SiraHighRiskActivities
spark.sql(f"""

SELECT DISTINCT
   t1.UniqueGcobId
  , t1.SiraClientId
  , t3.QuestionId
  , t4.Text
  , t4.Value
FROM radar.siraclients_historical t1
LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId
LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id -- and t3.QuestionId = t4.QuestionId?
WHERE t3.QuestionId IN (23, 418, 559, 774)
  AND t1.SourceSystemReference = 'GCOB_LegalEntity'
  AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

UNION ALL

SELECT DISTINCT 
  t1.UniqueGcobId
  , t1.SiraClientId
  , t3.QuestionId
  , t4.Text
  , t4.Value
FROM radar.siraclients_historical t1
LEFT JOIN InstancePerNPClientId t2 ON t1.SiraClientId = t2.NaturalPersonClientId
LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id -- and t3.QuestionId = t4.QuestionId?
WHERE t3.QuestionId IN (248, 475, 647)
  AND t1.SourceSystemReference = 'GCOB_NP-NPPC'
  AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

""").createOrReplaceTempView('SiraHighRiskActivities')

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.SiraHighRiskActivities_historical')
    spark.sql('SELECT * FROM SiraHighRiskActivities').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.SiraHighRiskActivities_historical')

else:
    spark.sql('SELECT * FROM SiraHighRiskActivities').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.SiraHighRiskActivities_historical')

# COMMAND ----------

# MAGIC %md
# MAGIC ### NO CHECKS ON radar.SiraHighRiskActivities!!

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraHeatMapCAMS

# COMMAND ----------

# DBTITLE 1,SiraHeatMapCAMS
spark.sql(f"""

SELECT UniqueGcobId, 'Geographical' AS RiskFactor, GeographicalRiskLevel AS RiskNr FROM radar.siraclients_historical WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')
UNION
SELECT UniqueGcobId, 'Entity Type', EntityTypeRiskLevel FROM radar.siraclients_historical WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')
UNION
SELECT UniqueGcobId, 'Structure', StructureRiskLevel FROM radar.siraclients_historical WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')
UNION
SELECT UniqueGcobId, 'Sector', SectorRiskLevel FROM radar.siraclients_historical WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')
UNION
SELECT UniqueGcobId, 'Products And Services', ProductAndServiceRiskLevel FROM radar.siraclients_historical WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')
UNION
SELECT UniqueGcobId, 'PEP', PEPRiskLevel FROM radar.siraclients_historical WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')
UNION
SELECT UniqueGcobId, 'Transaction', TransactionRiskLevel FROM radar.siraclients_historical WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')
UNION
SELECT UniqueGcobId, 'Distribution Channel', DistributionRiskLevel FROM radar.siraclients_historical WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')
UNION
SELECT UniqueGcobId, 'Third Party', ThirdPartyRiskLevel FROM radar.siraclients_historical WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')
UNION
SELECT UniqueGcobId, 'Adverse Info', AdverseInfoRiskLevel FROM radar.siraclients_historical WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')
UNION
SELECT UniqueGcobId, 'Other', OtherRiskLevel FROM radar.siraclients_historical WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')
UNION
SELECT UniqueGcobId, 'Overall Risk level', ValidatedRiskLevel FROM radar.siraclients_historical WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')

""").createOrReplaceTempView('SiraHeatMapCAMS')

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.SiraHeatMapCAMS_historical')
    spark.sql('SELECT * FROM SiraHeatMapCAMS').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.SiraHeatMapCAMS_historical')

else:
    spark.sql('SELECT * FROM SiraHeatMapCAMS').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.SiraHeatMapCAMS_historical')

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraHighRiskSector

# COMMAND ----------

# DBTITLE 1,SiraHighRiskSector
spark.sql(f"""

SELECT DISTINCT
  t1.UniqueGcobId
  , t1.SiraClientId
    , CASE
        WHEN t3.QuestionId IN (23, 418, 559, 774) THEN 'RiskySector'
        WHEN t3.QuestionId IN (520, 558, 646, 997) THEN 'HighRiskfromNAICS'
        WHEN t3.QuestionId IN (21, 397, 557, 773) THEN 'NAICSCodes'
    END AS QuestionName
    , t4.Text
    -- , CASE
    --     WHEN t3.QuestionId IN (23, 418, 559, 774) THEN t4.Text
    -- END AS RiskySector
    -- , CASE
    --     WHEN t3.QuestionId IN (520, 558, 646, 997) THEN t4.Text
    -- END AS HighRiskfromNAICS
    -- , CASE
    --     WHEN t3.QuestionId IN (21, 397, 557, 773) THEN t4.Text
    -- END AS NAICSCodes
    , t3.QuestionId

FROM radar.siraclients_historical t1
LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId
LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id -- and t3.QuestionId = t4.QuestionId?
WHERE t4.QuestionId IN (23, 418, 559, 774, 520, 558, 646, 21, 397, 557, 773, 997)
  AND t1.SourceSystemReference = 'GCOB_LegalEntity'
  AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

UNION

SELECT DISTINCT 
  t1.UniqueGcobId
  , t1.SiraClientId
    , CASE
        WHEN t3.QuestionId IN (248, 475, 647) THEN 'RiskySector'
        WHEN t3.QuestionId IN (519, 646, 997) THEN 'HighRiskfromNAICS'
        WHEN t3.QuestionId IN (474, 645, 996) THEN 'NAICSCodes'
    END AS QuestionName
    , t4.Text
    -- , CASE
    --     WHEN t3.QuestionId IN (248, 475, 647) THEN t4.Text
    -- END AS RiskySector
    -- , CASE
    --     WHEN t3.QuestionId IN (519, 646, 997) THEN t4.Text
    --     -- ELSE ''
    -- END AS HighRiskfromNAICS
    -- , CASE
    --     WHEN t3.QuestionId IN (474, 645, 996) THEN t4.Text
    --     -- ELSE ''
    -- END AS NAICSCodes
    , t3.QuestionId

FROM radar.siraclients_historical t1
LEFT JOIN InstancePerNPClientId t2 ON t1.SiraClientId = t2.NaturalPersonClientId
LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id -- and t3.QuestionId = t4.QuestionId?
WHERE t4.QuestionId IN (248, 475, 647, 519, 646, 474, 645, 996, 997)
  AND t1.SourceSystemReference = 'GCOB_NP-NPPC'
  AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

""").createOrReplaceTempView('SiraHighRiskSector')

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.SiraHighRiskSector_historical')
    spark.sql('SELECT * FROM SiraHighRiskSector').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.SiraHighRiskSector_historical')

else:
    spark.sql('SELECT * FROM SiraHighRiskSector').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.SiraHighRiskSector_historical')

# COMMAND ----------

# MAGIC %md
# MAGIC ### NO CHECKS ON radar.SiraHighRiskSector!!

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraN2KLocations

# COMMAND ----------

# DBTITLE 1,SiraN2KLocations
spark.sql(f"""

WITH n2k_cte AS (
    SELECT DISTINCT
        t1.SourceClient
        , t2.ProductOfferingLocation AS N2KLocation
        , t2.ProductLifeCycleStatus
    FROM radar.cases_historical t1
    LEFT JOIN party_products_and_services t2 ON t1.SourceClient = t2.SourceClient
    WHERE t2.ProductLifeCycleStatus IN ('Active', 'Exit In Progress')
        AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

    UNION 

    SELECT DISTINCT
        t1.SourceClient
        , t2.BookingEntityLocation AS N2KLocation
        , t2.ProductLifeCycleStatus
    FROM radar.cases_historical t1
    LEFT JOIN party_products_and_services t2 ON t1.SourceClient = t2.SourceClient
    WHERE t2.ProductLifeCycleStatus IN ('Active', 'Exit In Progress')
        AND t1.EDL_LoadDate = DATE('{EDL_LoadDate}')

    UNION 

    SELECT DISTINCT
        SourceClient
        , GlobalClientOwnerLocation AS N2KLocation
        , 'GCO Location' AS ProductLifeCycleStatus
    FROM radar.cases_historical
    WHERE EDL_LoadDate = DATE('{EDL_LoadDate}')
)

SELECT
    t1.SourceClient
    , t1.N2KLocation
    , t1.ProductLifeCycleStatus
    , CASE 
        WHEN t1.N2KLocation = t2.GlobalClientOwnerLocation THEN 1 
        ELSE 0
      END AS IsLeadLocation
    , CASE 
        WHEN t1.N2KLocation = t2.GlobalClientOwnerLocation THEN 'Lead' 
        ELSE 'Involved' 
      END AS LeadOrInvolved
    , CASE 
        WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') THEN 'Asia Lead'
        ELSE 'Asia Involved' 
      END AS AsiaLeadOrInvolved

FROM n2k_cte t1
LEFT JOIN radar.cases_historical t2 ON t1.SourceClient = t2.SourceClient
WHERE t2.EDL_LoadDate = DATE('{EDL_LoadDate}')

""").createOrReplaceTempView('SiraN2KLocations')

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

if EDL_LoadDate == '2024-01-01':
    spark.sql('DROP TABLE IF EXISTS radar.SiraN2KLocations_historical')
    spark.sql('SELECT * FROM SiraN2KLocations').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('overwrite').saveAsTable('radar.SiraN2KLocations_historical')

else:
    spark.sql('SELECT * FROM SiraN2KLocations').withColumn('EDL_LoadDate', to_date(lit(EDL_LoadDate), 'yyyy-MM-dd')).write.mode('append').saveAsTable('radar.SiraN2KLocations_historical')
