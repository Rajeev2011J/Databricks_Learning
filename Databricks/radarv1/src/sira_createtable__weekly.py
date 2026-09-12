# Databricks notebook source
import os
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
from pyspark.sql import functions as F

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

for item in gcob_tables:
    # get the most recent version available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])


    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOADED_DTS=')[1][:8] for file in files if 'LOADED_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/{version}/data/LOADED_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraCountryList

# COMMAND ----------

# DBTITLE 1,SiraCountryList
# get the most recent version available in gdp
path = f'abfss://ebx@{ReadStorage}.dfs.core.windows.net/EBX_FECCountryRiskList/'
files = dbutils.fs.ls(path)
version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])


# get the most recent file available in gdp
path = f'abfss://ebx@{ReadStorage}.dfs.core.windows.net/EBX_FECCountryRiskList/{version}/data/'
files = dbutils.fs.ls(path)
load_date_ebx = max(file.path.split('EDL_LOAD_DT=')[1][:8] for file in files if 'EDL_LOAD_DT=' in file.path)

spark.read.parquet(f'abfss://ebx@{ReadStorage}.dfs.core.windows.net/EBX_FECCountryRiskList/{version}/data/EDL_LOAD_DT={load_date_ebx}/*.parquet').createOrReplaceTempView('ebx_static_country_list_raw')

# COMMAND ----------

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

# MAGIC %sql
# MAGIC drop table if exists radar.SiraCountryList

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

spark.sql('select * from SiraCountryList').withColumn('EDL_LoadDate', to_date(lit(load_date_ebx), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.SiraCountryList')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the quarter!
from pyspark.sql.functions import lit, to_date
from datetime import datetime

# if it is the first of the quarter, change the EDL_LoadDate column and append it to the historical dataset

date_obj = datetime.strptime(load_date_ebx, '%Y%m%d').date()

if date_obj.day == 1 and date_obj.month in [1, 4, 7, 10]:
    (spark.table("radar.SiraCountryList")
         .withColumn('EDL_LoadDate', to_date(lit(load_date_ebx), 'yyyyMMdd'))
         .write
         .mode('append')
         .saveAsTable('radar.SiraCountryList_historical')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraClients

# COMMAND ----------

# DBTITLE 1,SiraClients
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraClients AS
# MAGIC
# MAGIC WITH last_completed_case_no_ea AS (
# MAGIC   SELECT * FROM (
# MAGIC     SELECT
# MAGIC       *
# MAGIC       , ROW_NUMBER() OVER (PARTITION BY UniqueGcobId ORDER BY ClientId DESC) AS rn
# MAGIC     FROM radar.cases
# MAGIC     WHERE CaseStatusName = 'Completed'
# MAGIC       AND (CaseReviewType IS NULL OR CaseReviewType <> 'Event Assessment')
# MAGIC   ) subquery WHERE rn = 1
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   t2.UniqueGcobId
# MAGIC   , t2.SourceSystemReference
# MAGIC
# MAGIC   , t2.ClientId AS SiraClientId -- considering last completed case, excluding EA
# MAGIC   -- , COALESCE(t1.CompletedId, t1.LatestId) AS SiraClientId --  ApprovedCaseId (previously including t1.LastFullReviewId) -- 22/09/25 removed t1.LastFullReviewId to capture all completed cases
# MAGIC
# MAGIC   , t2.SourceClient
# MAGIC
# MAGIC   , t2.FullLegalName
# MAGIC   , t1.KYCGroup
# MAGIC   , t1.SectorTeam
# MAGIC   , t1.GlobalKYCPortfolioNew
# MAGIC   , t2.FIHubIndicator_Derived
# MAGIC   , t2.BusinessLineName
# MAGIC   , t2.ClientLifeCycleName
# MAGIC
# MAGIC   , t2.ValidatedRiskLevel -- from radar.clients where it is based on coalesce(completedid, latestid)
# MAGIC
# MAGIC   , t2.NextReviewDate
# MAGIC   , t2.GlobalClientOwner
# MAGIC   , t2.GlobalClientOwnerLocation
# MAGIC -- validated risks level, meaning last completed/latest caseid
# MAGIC   , t2.GeographicalRiskLevel
# MAGIC   , t2.EntityTypeRiskLevel
# MAGIC   , t2.StructureRiskLevel
# MAGIC   , t2.SectorRiskLevel
# MAGIC   , t2.ProductAndServiceRiskLevel
# MAGIC   , t2.PEPRiskLevel
# MAGIC   , t2.TransactionRiskLevel
# MAGIC   , t2.DistributionRiskLevel
# MAGIC   , t2.ThirdPartyRiskLevel
# MAGIC   , t2.AdverseInfoRiskLevel
# MAGIC   , t2.OtherRiskLevel
# MAGIC   , t2.FatcaClassification
# MAGIC   , t2.CrsClassification
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN t2.SourceSystemReference = 'GCOB_NP-NPPC' THEN
# MAGIC         CASE 
# MAGIC           WHEN t2.GeographicalRiskLevel = 'Incomplete' OR t2.GeographicalRiskLevel IS NULL
# MAGIC             OR t2.SectorRiskLevel = 'Incomplete' OR t2.SectorRiskLevel IS NULL
# MAGIC             OR t2.ProductAndServiceRiskLevel = 'Incomplete' OR t2.ProductAndServiceRiskLevel IS NULL
# MAGIC             OR t2.PEPRiskLevel = 'Incomplete' OR t2.PEPRiskLevel IS NULL
# MAGIC             OR t2.TransactionRiskLevel = 'Incomplete' OR t2.TransactionRiskLevel IS NULL
# MAGIC             OR t2.DistributionRiskLevel = 'Incomplete' OR t2.DistributionRiskLevel IS NULL
# MAGIC             OR t2.ThirdPartyRiskLevel = 'Incomplete' OR t2.ThirdPartyRiskLevel IS NULL
# MAGIC             OR t2.AdverseInfoRiskLevel = 'Incomplete' OR t2.AdverseInfoRiskLevel IS NULL
# MAGIC             THEN 0 
# MAGIC           WHEN t1.LatestCompletedCaseId IS NULL THEN 0 
# MAGIC           ELSE 1
# MAGIC         END
# MAGIC       ELSE 
# MAGIC         CASE 
# MAGIC           WHEN t2.GeographicalRiskLevel = 'Incomplete' OR t2.GeographicalRiskLevel IS NULL
# MAGIC             OR t2.EntityTypeRiskLevel = 'Incomplete' OR t2.EntityTypeRiskLevel IS NULL
# MAGIC             OR t2.StructureRiskLevel = 'Incomplete' OR t2.StructureRiskLevel IS NULL
# MAGIC             OR t2.SectorRiskLevel = 'Incomplete' OR t2.SectorRiskLevel IS NULL
# MAGIC             OR t2.ProductAndServiceRiskLevel = 'Incomplete' OR t2.ProductAndServiceRiskLevel IS NULL
# MAGIC             OR t2.PEPRiskLevel = 'Incomplete' OR t2.PEPRiskLevel IS NULL
# MAGIC             OR t2.TransactionRiskLevel = 'Incomplete' OR t2.TransactionRiskLevel IS NULL
# MAGIC             OR t2.DistributionRiskLevel = 'Incomplete' OR t2.DistributionRiskLevel IS NULL
# MAGIC             OR t2.ThirdPartyRiskLevel = 'Incomplete' OR t2.ThirdPartyRiskLevel IS NULL
# MAGIC             OR t2.AdverseInfoRiskLevel = 'Incomplete' OR t2.AdverseInfoRiskLevel IS NULL
# MAGIC             THEN 0 
# MAGIC           WHEN t1.LatestCompletedCaseId IS NULL THEN 0 
# MAGIC           ELSE 1 
# MAGIC         END
# MAGIC     END AS EntityHasAllData
# MAGIC
# MAGIC   , t1.OnboardingDate
# MAGIC   , t2.CddType
# MAGIC   , t1.Reason
# MAGIC   , CASE
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Kenya') THEN 'E&A'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') THEN 'Asia'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') THEN 'North America'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Chile', 'Rabobank Brazil', 'Rabobank Argentina') THEN 'South America'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') THEN 'RANZ'
# MAGIC       WHEN t2.GlobalClientOwnerLocation IN ('Rabobank Foundation') THEN 'Rabobank Foundation'
# MAGIC     END AS GlobalReportingRegion
# MAGIC   , t1.GlobalBusinessLine
# MAGIC
# MAGIC FROM radar.clients t1
# MAGIC INNER JOIN last_completed_case_no_ea t2 ON t1.UniqueGcobId = t2.UniqueGcobId -- 08/01/26 considering last completed cases, excluding Event Assessment which is does not contain risk and other information
# MAGIC
# MAGIC -- INNER JOIN radar.cases t2 ON t1.CompletedId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId -- 22/09/25 removed this t1.LastFullReviewId + 16/10/2025 also t1.LatestId to show only completed

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.SiraClients

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

spark.sql('select * from SiraClients').withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.SiraClients')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if UniqueGcobId duplication
duplicated_uniquegcobid = spark.sql('''
    SELECT UniqueGcobId 
    FROM radar.SiraClients 
    GROUP BY UniqueGcobId 
    HAVING count(*) > 1
''')

# check for duplication
if not duplicated_uniquegcobid.isEmpty():
    raise Exception('Duplicated UniqueGcobId in clients. Job stopped.')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the quarter!
from pyspark.sql.functions import lit, to_date
from datetime import datetime

# if it is the first of the quarter, change the EDL_LoadDate column and append it to the historical dataset

date_obj = datetime.strptime(load_date, '%Y%m%d').date()

if date_obj.day == 1 and date_obj.month in [1, 4, 7, 10]:
    (spark.table("radar.SiraClients")
         .withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd'))
         .write
         .mode('append')
         .saveAsTable('radar.SiraClients_historical')
    )

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

# DBTITLE 1,SiraClientRisks
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraClientRisks_LE AS
# MAGIC
# MAGIC WITH TxAlerts_CTE AS (
# MAGIC   SELECT 
# MAGIC     t1.SiraClientId
# MAGIC     , MIN(t4.Text) AS HasTxAlert
# MAGIC   FROM radar.siraclients t1
# MAGIC   LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId 
# MAGIC   LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC   LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
# MAGIC   WHERE (t3.QuestionId BETWEEN 51 AND 60 OR t3.QuestionId BETWEEN 387 AND 396) 
# MAGIC     AND t4.Text = 'Yes'
# MAGIC     AND t1.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC   GROUP BY t1.SiraClientId
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   pvt.*
# MAGIC   , COALESCE(t2.HasTxAlert, 'No') AS HasTxAlert
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     t1.UniqueGcobId
# MAGIC     , t1.SiraClientId
# MAGIC     , t1.CddType
# MAGIC     , MAX(CASE WHEN t3.QuestionId IN (61, 383, 600) THEN t4.Text END) AS Face2FaceContact
# MAGIC     , MAX(CASE WHEN t3.QuestionId IN (66, 80, 220, 382, 606) THEN t4.Text END) AS AdverseInfo
# MAGIC     , MAX(CASE WHEN t3.QuestionId IN (5, 81, 221, 349, 540, 995) THEN t4.Text END) AS EntityType
# MAGIC     , MAX(CASE WHEN t3.QuestionId IN (18, 342, 552, 768) THEN t4.Text END) AS ComplexStructure
# MAGIC   FROM radar.siraclients t1
# MAGIC   LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId
# MAGIC   LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC   LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
# MAGIC   WHERE t3.QuestionId IN (5, 18, 61, 66, 80, 81, 220, 221, 342, 349, 382, 383, 540, 552, 600, 606, 768, 995)
# MAGIC     AND t1.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC   GROUP BY 1,2,3
# MAGIC ) pvt -- pivot
# MAGIC LEFT JOIN TxAlerts_CTE t2 ON pvt.SiraClientId = t2.SiraClientId

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraClientRisks_NP AS
# MAGIC
# MAGIC WITH TxAlerts_CTE AS (
# MAGIC   SELECT 
# MAGIC     t1.SiraClientId
# MAGIC     , MIN(t4.Text) AS HasTxAlert
# MAGIC   FROM radar.siraclients t1
# MAGIC   LEFT JOIN InstancePerNPClientId t2 ON t1.SiraClientId = t2.NaturalPersonClientId
# MAGIC   LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC   LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
# MAGIC   WHERE t3.QuestionId BETWEEN 281 AND 288
# MAGIC     AND t4.Text = 'Yes'
# MAGIC     AND t1.SourceSystemReference = 'GCOB_NP-NPPC'
# MAGIC   GROUP BY t1.SiraClientId
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   pvt.UniqueGcobId
# MAGIC   , pvt.SiraClientId
# MAGIC   , pvt.CddType
# MAGIC   , MAX(CASE WHEN pvt.Question = 'Face2FaceContact' THEN pvt.Text ELSE '' END) AS Face2FaceContact
# MAGIC   , MAX(CASE WHEN pvt.Question = 'AdverseInfo' THEN pvt.Text ELSE '' END) AS AdverseInfo
# MAGIC   , pvt.CddType AS EntityType -- t4.QuestionId IN (516)
# MAGIC   , 'No' AS ComplexStructure -- t4.QuestionId IN (364)
# MAGIC   , MAX(COALESCE(t2.HasTxAlert, 'No')) AS HasTxAlert -- MAX to just avoid grouping by?
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC       t1.UniqueGcobId
# MAGIC       , t1.SiraClientId
# MAGIC       , t1.CddType
# MAGIC       , CASE 
# MAGIC           WHEN t3.QuestionId IN (261, 470, 674, 728) THEN 'Face2FaceContact'
# MAGIC           WHEN t3.QuestionId IN (264, 515, 676, 732) THEN 'AdverseInfo'
# MAGIC         END AS Question
# MAGIC       , t4.Text
# MAGIC     FROM radar.siraclients t1
# MAGIC     LEFT JOIN InstancePerNPClientId t2 ON t1.SiraClientId = t2.NaturalPersonClientId
# MAGIC     LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t3.InstanceId = t3.InstanceId
# MAGIC     LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
# MAGIC     WHERE t3.QuestionId IN (264, 515, 261, 470, 676, 732, 674, 728)
# MAGIC       AND t1.SourceSystemReference = 'GCOB_NP-NPPC'
# MAGIC ) pvt
# MAGIC LEFT JOIN TxAlerts_CTE t2 ON pvt.SiraClientId = t2.SiraClientId
# MAGIC GROUP BY 1,2,3

# COMMAND ----------

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

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.SiraClientRisks

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the quarter!
from pyspark.sql.functions import lit, to_date

spark.sql('SELECT * FROM SiraClientRisks').withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.SiraClientRisks')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if UniqueGcobId duplication
duplicated_uniquegcobid = spark.sql('''
    SELECT UniqueGcobId 
    FROM radar.SiraClientRisks
    GROUP BY UniqueGcobId 
    HAVING COUNT(*) > 1
''')

# check for duplication
if not duplicated_uniquegcobid.isEmpty():
    raise Exception('Duplicated UniqueGcobId in clients. Job stopped.')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the quarter!
from pyspark.sql.functions import lit, to_date
from datetime import datetime

# if it is the first of the quarter, change the EDL_LoadDate column and append it to the historical dataset

date_obj = datetime.strptime(load_date, '%Y%m%d').date()

if date_obj.day == 1 and date_obj.month in [1, 4, 7, 10]:
    (spark.table("radar.SiraClientRisks")
         .withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd'))
         .write
         .mode('append')
         .saveAsTable('radar.SiraClientRisks_historical')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraGeoActivities

# COMMAND ----------

# DBTITLE 1,SiraGeoActivities
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraGeoActivities AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC   , t3.QuestionId
# MAGIC   , CASE
# MAGIC       WHEN t3.QuestionId IN (1, 346, 423, 480, 527) THEN 'Geo Registration' -- 480 NP, 527 ??
# MAGIC       WHEN t3.QuestionId IN (4, 348, 426, 483, 531) THEN 'Geo Activities' -- 483 NP, 531
# MAGIC       WHEN t3.QuestionId IN (424, 528) THEN 'Geo UBO(s)' -- check 528
# MAGIC       WHEN t3.QuestionId IN (3, 425) THEN 'Geo Parent(s)'
# MAGIC       ELSE ''
# MAGIC     END AS QuestionName
# MAGIC   , t4.Text
# MAGIC   , t4.Value
# MAGIC   , t5.RiskScore
# MAGIC   -- , COALESCE(t6.Overall_Risk, '#N/A') AS OverallRisk
# MAGIC   , COALESCE(t6.Sanctions_Risk, '#N/A') AS SanctionRisk
# MAGIC   , COALESCE(t6.Money_Laundering_Risk, '#N/A') AS MLRisk
# MAGIC   , COALESCE(t6.Terrorism_Financing_Risk, '#N/A') AS TFRisk
# MAGIC   , COALESCE(t6.Tax_Integrity_Risk, '#N/A') AS TaxIntegrityRiskJurisdictions
# MAGIC   , COALESCE(t6.Corruption_Risk, '#N/A') AS CorruptionRisk
# MAGIC   , COALESCE(t6.EC_High_Risk, '#N/A') AS ECHighRiskThirdCountry
# MAGIC
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId
# MAGIC LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
# MAGIC LEFT JOIN CaseService_dbo_Country t5 ON t4.Value = t5.IsoCode
# MAGIC LEFT JOIN radar.SiraCountryList t6 ON t4.Value = t6.ISO_code
# MAGIC WHERE t3.QuestionId IN (1, 346, 423, 4, 348, 426, 424, 3, 425, 531, 527, 480, 483, 528) -- what is q245 and q246?
# MAGIC   AND t5.Expired IS NULL
# MAGIC   AND t1.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC   , t3.QuestionId
# MAGIC   , CASE
# MAGIC       WHEN t3.QuestionId IN (245, 480, 695) THEN 'Geo Registration'
# MAGIC       WHEN t3.QuestionId IN (247, 483, 696) THEN 'Geo Activities'
# MAGIC       -- WHEN t3.QuestionId IN (2, 347, 481, 528) THEN 'Geo UBO(s)'
# MAGIC       -- WHEN t3.QuestionId IN (409, 482) THEN 'Geo Parent(s)'
# MAGIC       ELSE ''
# MAGIC   END AS QuestionName
# MAGIC   , t4.Text
# MAGIC   , t4.Value
# MAGIC   , t5.RiskScore
# MAGIC   -- , COALESCE(t6.Overall_Risk, '#N/A') AS OverallRisk
# MAGIC   , COALESCE(t6.Sanctions_Risk, '#N/A') AS SanctionRisk
# MAGIC   , COALESCE(t6.Money_Laundering_Risk, '#N/A') AS MLRisk
# MAGIC   , COALESCE(t6.Terrorism_Financing_Risk, '#N/A') AS TFRisk
# MAGIC   , COALESCE(t6.Tax_Integrity_Risk, '#N/A') AS TaxIntegrityRiskJurisdictions
# MAGIC   , COALESCE(t6.Corruption_Risk, '#N/A') AS CorruptionRisk
# MAGIC   , COALESCE(t6.EC_High_Risk, '#N/A') AS ECHighRiskThirdCountry
# MAGIC
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN InstancePerNPClientId t2 ON t1.SiraClientId = t2.NaturalPersonClientId
# MAGIC LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id --? and t3.QuestionId = t4.QuestionId
# MAGIC LEFT JOIN CaseService_dbo_Country t5 ON t4.Value = t5.IsoCode
# MAGIC LEFT JOIN radar.SiraCountryList t6 ON t4.Value = t6.ISO_code
# MAGIC WHERE t3.QuestionId IN (245, 480, 247, 483, 695, 696) -- 2, 347, 481, 528, 409, 482 & what is q245 and q246?
# MAGIC   AND t5.Expired IS NULL
# MAGIC   AND t1.SourceSystemReference = 'GCOB_NP-NPPC'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.SiraGeoActivities

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

spark.sql('select * from SiraGeoActivities').withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.SiraGeoActivities')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if UniqueGcobId duplication
duplicated_uniquegcobid = spark.sql('''
    SELECT UniqueGcobId 
    FROM (
        SELECT DISTINCT
            UniqueGcobid
            , QuestionName
            , TRIM(REPLACE(Text, '(the)', '')) AS TextClean
            , Value
        FROM radar.SiraGeoActivities
    )
    WHERE QuestionName = 'Geo Registration' -- geo activities can have multiple results!!
    -- AND UniqueGcobId NOT IN ('12476', '8831') -- excluding those two gcobids having double questionids for geo registration
    GROUP BY UniqueGcobId
    HAVING COUNT(*) > 1
''')

# check for duplication
if not duplicated_uniquegcobid.isEmpty():
    raise Exception('Duplicated UniqueGcobId in clients. Job stopped.')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the quarter!
from pyspark.sql.functions import lit, to_date
from datetime import datetime

# if it is the first of the quarter, change the EDL_LoadDate column and append it to the historical dataset

date_obj = datetime.strptime(load_date, '%Y%m%d').date()

if date_obj.day == 1 and date_obj.month in [1, 4, 7, 10]:
    (spark.table("radar.SiraGeoActivities")
         .withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd'))
         .write
         .mode('append')
         .saveAsTable('radar.SiraGeoActivities_historical')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraHighRiskProducts

# COMMAND ----------

# DBTITLE 1,SiraHighRiskProducts
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraHighRiskProducts AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC   , t3.QuestionId
# MAGIC   , CASE
# MAGIC       WHEN t3.QuestionId IN (26, 77, 405, 568, 702) THEN 'High Risk Products'
# MAGIC       WHEN t3.QuestionId IN (32, 407, 570, 783) THEN 'Products Commensurate'
# MAGIC       WHEN t3.QuestionId IN (34, 408, 571, 785) THEN 'Product Source Of Fund Consistent'
# MAGIC       ELSE ''
# MAGIC     END AS QuestionName
# MAGIC   , CASE
# MAGIC       WHEN t3.QuestionId IN (26, 77, 405, 568, 702) AND t1.UniqueGcobId <> '86773' THEN COALESCE(t5.HighRiskProductName, t4.Value)
# MAGIC       WHEN t3.QuestionId IN (26, 77, 405, 568, 702) AND t1.UniqueGcobId = '86773' THEN 'None' -- hard coded for this gcobid containing multiple possible answer in RiskModel_dbo_InstanceAnswer!!
# MAGIC       ELSE ''
# MAGIC     END AS HighRiskProductName
# MAGIC   , CASE
# MAGIC       WHEN t3.QuestionId IN (26, 77, 405, 568, 702) AND t1.UniqueGcobId <> '86773' THEN COALESCE(t5.HighRiskProductRiskScore, t4.Score)
# MAGIC       WHEN t3.QuestionId IN (26, 77, 405, 568, 702) AND t1.UniqueGcobId = '86773' THEN '0.0000' -- hard coded for this gcobid containing multiple possible answer in RiskModel_dbo_InstanceAnswer!!
# MAGIC       ELSE ''
# MAGIC     END AS HighRiskProductRiskScore
# MAGIC   , CASE
# MAGIC       WHEN t3.QuestionId IN (32, 407, 570, 783, 34, 408, 571, 785) AND t1.UniqueGcobId <> '86773' THEN t4.Text
# MAGIC       WHEN t3.QuestionId IN (32, 407, 570, 783, 34, 408, 571, 785) AND t1.UniqueGcobId = '86773' THEN 'Yes' -- hard coded for this gcobid containing multiple possible answer in RiskModel_dbo_InstanceAnswer!!
# MAGIC       ELSE ''
# MAGIC     END AS ProductsCommensurateANDSourceOfFunds
# MAGIC
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId
# MAGIC LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
# MAGIC LEFT JOIN CaseService_case_HighRiskProductReference t5 ON CAST(t4.Value AS STRING) = CAST(t5.HighRiskProductId AS STRING)
# MAGIC WHERE t3.QuestionId IN (26, 77, 405, 568, 702, 32, 407, 570, 34, 408, 571, 783, 785)
# MAGIC   AND t1.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC   , t3.QuestionId
# MAGIC   , CASE
# MAGIC       WHEN t3.QuestionId IN (484, 652, 702) THEN 'High Risk Products'
# MAGIC       WHEN t3.QuestionId IN (32, 251, 486, 653, 703, 834) THEN 'Products Commensurate'
# MAGIC       WHEN t3.QuestionId IN (34, 254, 487, 654, 704, 785) THEN 'Product Source Of Fund Consistent'
# MAGIC       ELSE ''
# MAGIC   END AS QuestionName
# MAGIC   , CASE
# MAGIC       WHEN t3.QuestionId IN (484, 652, 702) THEN COALESCE(t5.HighRiskProductName, t4.Value)
# MAGIC       ELSE ''
# MAGIC   END AS HighRiskProductName
# MAGIC   , CASE
# MAGIC       WHEN t3.QuestionId IN (484, 652, 702) THEN t4.Score
# MAGIC       ELSE ''
# MAGIC   END AS HighRiskProductRiskScore
# MAGIC   , CASE
# MAGIC       WHEN t3.QuestionId IN (32, 251, 486, 653, 703, 834, 34, 254, 487, 654, 704, 785) THEN t4.Text
# MAGIC       ELSE ''
# MAGIC   END AS ProductsCommensurateANDSourceOfFunds
# MAGIC
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN InstancePerNPClientId t2 ON t1.SiraClientId = t2.NaturalPersonClientId
# MAGIC LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
# MAGIC LEFT JOIN CaseService_NaturalPerson_HighRiskProductReference t5 ON CAST(t4.Value AS STRING) = CAST(t5.HighRiskProductId AS STRING)
# MAGIC WHERE t3.QuestionId IN (484, 652, 702, 32, 251, 486, 653, 703, 834, 34, 254, 487, 654, 704, 785)
# MAGIC   AND t1.SourceSystemReference = 'GCOB_NP-NPPC'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.SiraHighRiskProducts

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

spark.sql('select * from SiraHighRiskProducts').withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.SiraHighRiskProducts')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the quarter!
from pyspark.sql.functions import lit, to_date
from datetime import datetime

# if it is the first of the quarter, change the EDL_LoadDate column and append it to the historical dataset

date_obj = datetime.strptime(load_date, '%Y%m%d').date()

if date_obj.day == 1 and date_obj.month in [1, 4, 7, 10]:
    (spark.table("radar.SiraHighRiskProducts")
         .withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd'))
         .write
         .mode('append')
         .saveAsTable('radar.SiraHighRiskProducts_historical')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### **NO CHECKS ON radar.SiraHighRiskProducts!!**

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraPEPUBO

# COMMAND ----------

# DBTITLE 1,SiraPEPUBO
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraPEPUBO AS
# MAGIC
# MAGIC WITH MaxClientStructureSnapshotId_CTE AS (
# MAGIC   SELECT 
# MAGIC     LegalEntityClientId
# MAGIC     , MAX(ClientStructureSnapshotId) AS MaxClientStructureSnapshotId
# MAGIC   FROM CaseService_case_LegalEntityClientStructureSnapshot
# MAGIC   GROUP BY LegalEntityClientId
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC   , t2.MaxClientStructureSnapshotId
# MAGIC   -- , t3.RelationshipId
# MAGIC   -- , t3.ChildIdentity
# MAGIC   , t3.ParentIdentity
# MAGIC   -- , t3.ParentType
# MAGIC   , t4.UboThroughReasonReferenceId
# MAGIC   , t4.IsUbo
# MAGIC   , CASE 
# MAGIC       WHEN t4.UboThroughReasonReferenceId = 1 THEN 'Ownership'
# MAGIC       WHEN t4.UboThroughReasonReferenceId = 2 THEN 'Voting Rights'
# MAGIC       WHEN t4.UboThroughReasonReferenceId = 3 THEN '...'
# MAGIC       WHEN t4.UboThroughReasonReferenceId = 4 THEN 'Factual Effective Control'
# MAGIC       WHEN t4.UboThroughReasonReferenceId = 5 THEN 'Senior Managing Official'
# MAGIC   END AS UboReason
# MAGIC   , t5.Identity
# MAGIC   , t5.PoliticallyExposedPersonStatusId
# MAGIC   , t6.Name AS PEPStatus
# MAGIC   , CASE
# MAGIC       WHEN t5.PoliticallyExposedPersonStatusId IN (1,2,4,5) THEN 'PEP'
# MAGIC       ELSE 'Not PEP'
# MAGIC     END AS IsPEP
# MAGIC   , t12.SanctionsOrExternalWatchlist AS SanctionsOrExternalWatchlist
# MAGIC   , t7.CountryReferenceId
# MAGIC   , t8.Name AS PEPNationality
# MAGIC   , t10.Name AS ResidentialAddressCountry
# MAGIC   , t11.Corruption_Risk AS CorruptionRisk
# MAGIC   -- , t11.Overall_Risk AS OverallRiskOfPEPResidentialAddress
# MAGIC   , t11.EC_High_Risk AS ECHighRiskThirdCountry
# MAGIC
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN MaxClientStructureSnapshotId_CTE t2 ON t1.SiraClientId = t2.LegalEntityClientId
# MAGIC LEFT JOIN CaseService_snapshot_ClientStructureSnapshotRelationshipDetail t3 ON t2.MaxClientStructureSnapshotId = t3.ClientStructureSnapshotId
# MAGIC LEFT JOIN CaseService_snapshot_ClientStructureSnapshotCalculatedUboShareholdingDetail t4 ON t2.MaxClientStructureSnapshotId = t4.ClientStructureSnapshotId
# MAGIC   AND t3.ParentIdentity = t4.RelatedPartyIdentity
# MAGIC LEFT JOIN CaseService_case_RelatedNaturalPersonParty t5 ON t3.ParentIdentity = t5.RelatedNaturalPersonPartyId
# MAGIC LEFT JOIN CaseService_case_PoliticallyExposedPersonStatusReference t6 ON t5.PoliticallyExposedPersonStatusId = t6.PoliticallyExposedPersonStatusId
# MAGIC LEFT JOIN CaseService_case_RelatedNaturalPersonPartyNationality t7 ON t5.RelatedNaturalPersonPartyId = t7.RelatedNaturalPersonPartyId
# MAGIC LEFT JOIN CaseService_dbo_Country t8 ON t7.CountryReferenceId = t8.Id
# MAGIC LEFT JOIN CaseService_case_Address t9 ON t5.AddressId = t9.AddressId
# MAGIC LEFT JOIN CaseService_dbo_Country t10 ON t9.CountryReferenceId = t10.Id
# MAGIC LEFT JOIN radar.SiraCountryList t11 ON t10.IsoCode = t11.ISO_code
# MAGIC LEFT JOIN CaseService_case_ScreeningResults t12 ON t5.ScreeningResultsId = t12.ScreeningResultsId
# MAGIC WHERE t1.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC   AND t3.ParentType = 2 
# MAGIC   AND (t5.PoliticallyExposedPersonStatusId IN (1,2,4,5) OR t4.IsUbo = 1)
# MAGIC   AND t3.Expired IS NULL
# MAGIC   -- AND t5.Expired IS NULL -- added this!! -> WRONG!!
# MAGIC
# MAGIC -- REMOVED BELOW CONDITIONS OTHERWISE IDENTITY IS LINKED TO AN EXPIRED CountryReferenceId AND NOT SHOWN!!
# MAGIC   -- AND t8.Expired IS NULL
# MAGIC   -- AND t10.Expired IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.SiraPEPUBO

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

spark.sql('select * from SiraPEPUBO').withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.SiraPEPUBO')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if duplicated identity in each UniqueGcobId
duplicated_identity = spark.sql('''
    SELECT UniqueGcobId, Identity 
    FROM (SELECT DISTINCT UniqueGcobId, Identity FROM radar.SiraPEPUBO)
    GROUP BY UniqueGcobId, Identity 
    HAVING COUNT(*) > 1
''')

# check for duplication
if not duplicated_identity.isEmpty():
    raise Exception('Duplicated Identity per UniqueGcobId. Job stopped.')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the quarter!
from pyspark.sql.functions import lit, to_date
from datetime import datetime

# if it is the first of the quarter, change the EDL_LoadDate column and append it to the historical dataset

date_obj = datetime.strptime(load_date, '%Y%m%d').date()

if date_obj.day == 1 and date_obj.month in [1, 4, 7, 10]:
    (spark.table("radar.SiraPEPUBO")
        .withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd'))
        .write
        .mode('append')
        .saveAsTable('radar.SiraPEPUBO_historical')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraProductAndServices

# COMMAND ----------

# DBTITLE 1,SiraProductAndServices
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraProductAndServices AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC   , t2.ProductReferenceId
# MAGIC   , t2.IsOtc
# MAGIC   , t2.ProductLifecycle
# MAGIC   , t3.Name AS ProductOfferingLocation
# MAGIC   , t4.Name AS BookingEntityLocation
# MAGIC   , t5.Name AS ProductName
# MAGIC
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN CaseService_case_ProductProvidedToLegalEntity t2 ON t1.SiraClientId = t2.LegalEntityId
# MAGIC LEFT JOIN CaseService_dbo_RabobankEntity t3 ON t2.RabobankEntityReferenceIdOfProductLocation = t3.Id
# MAGIC LEFT JOIN CaseService_dbo_RabobankEntity t4 ON t2.RabobankEntityReferenceIdOfBookingLocation = t4.Id
# MAGIC LEFT JOIN CaseService_dbo_Product t5 ON t2.ProductReferenceId = t5.Id
# MAGIC WHERE t1.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC   , t2.ProductReferenceId
# MAGIC   , t2.IsOtc
# MAGIC   , t2.ProductLifecycle
# MAGIC   , t3.Name AS ProductOfferingLocation
# MAGIC   , t4.Name AS BookingEntityLocation
# MAGIC   , t5.Name AS ProductName
# MAGIC
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN CaseService_NaturalPerson_ProductProvidedToNaturalPerson t2 ON t1.SiraClientId = t2.NaturalPersonClientId
# MAGIC LEFT JOIN CaseService_dbo_RabobankEntity t3 ON t2.RabobankEntityReferenceIdOfProductLocation = t3.Id
# MAGIC LEFT JOIN CaseService_dbo_RabobankEntity t4 ON t2.RabobankEntityReferenceIdOfBookingLocation = t4.Id
# MAGIC LEFT JOIN CaseService_dbo_Product t5 ON t2.ProductReferenceId = t5.Id
# MAGIC WHERE t1.SourceSystemReference = 'GCOB_NP-NPPC'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.SiraProductAndServices

# COMMAND ----------

spark.sql('select * from SiraProductAndServices').write.mode('overwrite').saveAsTable('radar.SiraProductAndServices')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if duplicated product per UniqueGcobId
duplicated_product = spark.sql('''
    SELECT UniqueGcobId, ProductReferenceId 
    FROM (SELECT DISTINCT UniqueGcobId, ProductReferenceId FROM radar.SiraProductAndServices)
    GROUP BY UniqueGcobId, ProductReferenceId 
    HAVING COUNT(*) > 1
''')

# check for duplication
if not duplicated_product.isEmpty():
    raise Exception('Duplicated ProductReferenceId per UniqueGcobId. Job stopped.')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the quarter!
from pyspark.sql.functions import lit, to_date
from datetime import datetime

# if it is the first of the quarter, change the EDL_LoadDate column and append it to the historical dataset

date_obj = datetime.strptime(load_date, '%Y%m%d').date()

if date_obj.day == 1 and date_obj.month in [1, 4, 7, 10]:
    (spark.table("radar.SiraProductAndServices")
        .withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd'))
        .write
        .mode('append')
        .saveAsTable('radar.SiraProductAndServices_historical')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraNAICS

# COMMAND ----------

# DBTITLE 1,SiraNAICS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraNAICS AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC   , t2.NaicsReferenceId
# MAGIC   , t3.Id
# MAGIC   , t3.Name
# MAGIC   , t3.Code
# MAGIC   , t3.SectorGroup
# MAGIC   , t3.IndustryGroup
# MAGIC   , t3.IndustrySector
# MAGIC   , t3.HasRiskOfCorruption
# MAGIC   , t3.HasRiskOfMoneyLaunderingOrTerroristFinancing
# MAGIC   , t3.HasLicenseRequirements
# MAGIC   , t3.HasReputationRisk
# MAGIC   , t3.HasSanctions
# MAGIC   , t3.RiskScore
# MAGIC   , t3.Version
# MAGIC   , t3.Created
# MAGIC   , t3.Expired
# MAGIC   , t3.Previous
# MAGIC
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN CaseService_case_LegalEntityBusinessActivity t2 ON t1.SiraClientId = t2.LegalEntityId
# MAGIC LEFT JOIN CaseService_dbo_Naics t3 ON t2.NaicsReferenceId = t3.Id
# MAGIC WHERE t1.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC   , t2.NaturalPersonNaicsReferenceId AS NaicsReferenceId
# MAGIC   , t3.Id
# MAGIC   , t3.Name
# MAGIC   , t3.Code
# MAGIC   , t3.SectorGroup
# MAGIC   , t3.IndustryGroup
# MAGIC   , t3.IndustrySector
# MAGIC   , t3.HasRiskOfCorruption
# MAGIC   , t3.HasRiskOfMoneyLaunderingOrTerroristFinancing
# MAGIC   , t3.HasLicenseRequirements
# MAGIC   , t3.HasReputationRisk
# MAGIC   , t3.HasSanctions
# MAGIC   , t3.RiskScore
# MAGIC   , t3.Version
# MAGIC   , t3.Created
# MAGIC   , t3.Expired
# MAGIC   , t3.Previous
# MAGIC
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN CaseService_NaturalPerson_NaturalPersonBusinessActivity t2 ON t1.SiraClientId = t2.NaturalPersonId
# MAGIC LEFT JOIN CaseService_dbo_Naics t3 ON t2.NaturalPersonNaicsReferenceId = t3.Id
# MAGIC WHERE t1.SourceSystemReference = 'GCOB_NP-NPPC'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.SiraNAICS

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

spark.sql('select * from SiraNAICS').withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.SiraNAICS')

# COMMAND ----------

# DBTITLE 1,raise error and stop job if duplicated naics code per UniqueGcobId
duplicated_naics = spark.sql('''
    SELECT UniqueGcobId, NaicsReferenceId 
    FROM (SELECT DISTINCT UniqueGcobId, NaicsReferenceId FROM radar.SiraNAICS)
    GROUP BY UniqueGcobId, NaicsReferenceId 
    HAVING COUNT(*) > 1
''')

# check for duplication
if not duplicated_naics.isEmpty():
    raise Exception('Duplicated NaicsReferenceId per UniqueGcobId. Job stopped.')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the quarter!
from pyspark.sql.functions import lit, to_date
from datetime import datetime

# if it is the first of the quarter, change the EDL_LoadDate column and append it to the historical dataset

date_obj = datetime.strptime(load_date, '%Y%m%d').date()

if date_obj.day == 1 and date_obj.month in [1, 4, 7, 10]:
    (spark.table("radar.SiraNAICS")
        .withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd'))
        .write
        .mode('append')
        .saveAsTable('radar.SiraNAICS_historical')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraHighRiskActivities

# COMMAND ----------

# DBTITLE 1,SiraHighRiskActivities
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraHighRiskActivities AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC    t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC   , t3.QuestionId
# MAGIC   , t4.Text
# MAGIC   , t4.Value
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId
# MAGIC LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id -- and t3.QuestionId = t4.QuestionId?
# MAGIC WHERE t3.QuestionId IN (23, 418, 559, 774)
# MAGIC   AND t1.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC   , t3.QuestionId
# MAGIC   , t4.Text
# MAGIC   , t4.Value
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN InstancePerNPClientId t2 ON t1.SiraClientId = t2.NaturalPersonClientId
# MAGIC LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id -- and t3.QuestionId = t4.QuestionId?
# MAGIC WHERE t3.QuestionId IN (248, 475, 647) -- 819
# MAGIC   AND t1.SourceSystemReference = 'GCOB_NP-NPPC'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.SiraHighRiskActivities

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

spark.sql('select * from SiraHighRiskActivities').withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.SiraHighRiskActivities')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the quarter!
from pyspark.sql.functions import lit, to_date
from datetime import datetime

# if it is the first of the quarter, change the EDL_LoadDate column and append it to the historical dataset

date_obj = datetime.strptime(load_date, '%Y%m%d').date()

if date_obj.day == 1 and date_obj.month in [1, 4, 7, 10]:
    (spark.table("radar.SiraHighRiskActivities")
        .withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd'))
        .write
        .mode('append')
        .saveAsTable('radar.SiraHighRiskActivities_historical')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### NO CHECKS ON radar.SiraHighRiskActivities!!

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraHeatMapCAMS

# COMMAND ----------

# DBTITLE 1,SiraHeatMapCAMS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraHeatMapCAMS AS
# MAGIC
# MAGIC SELECT UniqueGcobId, 'Geographical' AS RiskFactor, GeographicalRiskLevel AS RiskNr FROM radar.SiraClients
# MAGIC UNION
# MAGIC SELECT UniqueGcobId, 'Entity Type', EntityTypeRiskLevel FROM radar.SiraClients
# MAGIC UNION
# MAGIC SELECT UniqueGcobId, 'Structure', StructureRiskLevel FROM radar.SiraClients
# MAGIC UNION
# MAGIC SELECT UniqueGcobId, 'Sector', SectorRiskLevel FROM radar.SiraClients
# MAGIC UNION
# MAGIC SELECT UniqueGcobId, 'Products And Services', ProductAndServiceRiskLevel FROM radar.SiraClients
# MAGIC UNION
# MAGIC SELECT UniqueGcobId, 'PEP', PEPRiskLevel FROM radar.SiraClients
# MAGIC UNION
# MAGIC SELECT UniqueGcobId, 'Transaction', TransactionRiskLevel FROM radar.SiraClients
# MAGIC UNION
# MAGIC SELECT UniqueGcobId, 'Distribution Channel', DistributionRiskLevel FROM radar.SiraClients
# MAGIC UNION
# MAGIC SELECT UniqueGcobId, 'Third Party', ThirdPartyRiskLevel FROM radar.SiraClients
# MAGIC UNION
# MAGIC SELECT UniqueGcobId, 'Adverse Info', AdverseInfoRiskLevel FROM radar.SiraClients
# MAGIC UNION
# MAGIC SELECT UniqueGcobId, 'Other', OtherRiskLevel FROM radar.SiraClients
# MAGIC UNION
# MAGIC SELECT UniqueGcobId, 'Overall Risk level', ValidatedRiskLevel FROM radar.SiraClients

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.SiraHeatMapCAMS

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

spark.sql('SELECT * FROM SiraHeatMapCAMS').withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.SiraHeatMapCAMS')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the quarter!
from pyspark.sql.functions import lit, to_date
from datetime import datetime

# if it is the first of the quarter, change the EDL_LoadDate column and append it to the historical dataset

date_obj = datetime.strptime(load_date, '%Y%m%d').date()

if date_obj.day == 1 and date_obj.month in [1, 4, 7, 10]:
    (spark.table("radar.SiraHeatMapCAMS")
        .withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd'))
        .write
        .mode('append')
        .saveAsTable('radar.SiraHeatMapCAMS_historical')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # SiraHighRiskSector

# COMMAND ----------

# DBTITLE 1,SiraHighRiskSector
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraHighRiskSector AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC     , CASE
# MAGIC         WHEN t3.QuestionId IN (23, 418, 559, 774) THEN 'RiskySector'
# MAGIC         WHEN t3.QuestionId IN (520, 558, 646, 997) THEN 'HighRiskfromNAICS'
# MAGIC         WHEN t3.QuestionId IN (21, 397, 557, 773) THEN 'NAICSCodes'
# MAGIC     END AS QuestionName
# MAGIC     , t4.Text
# MAGIC     -- , CASE
# MAGIC     --     WHEN t3.QuestionId IN (23, 418, 559, 774) THEN t4.Text
# MAGIC     -- END AS RiskySector
# MAGIC     -- , CASE
# MAGIC     --     WHEN t3.QuestionId IN (520, 558, 646, 997) THEN t4.Text
# MAGIC     -- END AS HighRiskfromNAICS
# MAGIC     -- , CASE
# MAGIC     --     WHEN t3.QuestionId IN (21, 397, 557, 773) THEN t4.Text
# MAGIC     -- END AS NAICSCodes
# MAGIC     , t3.QuestionId
# MAGIC
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId
# MAGIC LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id -- and t3.QuestionId = t4.QuestionId?
# MAGIC WHERE t4.QuestionId IN (23, 418, 559, 774, 520, 558, 646, 21, 397, 557, 773, 997)
# MAGIC   AND t1.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SiraClientId
# MAGIC     , CASE
# MAGIC         WHEN t3.QuestionId IN (248, 475, 647) THEN 'RiskySector'
# MAGIC         WHEN t3.QuestionId IN (519, 646, 997) THEN 'HighRiskfromNAICS'
# MAGIC         WHEN t3.QuestionId IN (474, 645, 996) THEN 'NAICSCodes'
# MAGIC     END AS QuestionName
# MAGIC     , t4.Text
# MAGIC     -- , CASE
# MAGIC     --     WHEN t3.QuestionId IN (248, 475, 647) THEN t4.Text
# MAGIC     -- END AS RiskySector
# MAGIC     -- , CASE
# MAGIC     --     WHEN t3.QuestionId IN (519, 646, 997) THEN t4.Text
# MAGIC     --     -- ELSE ''
# MAGIC     -- END AS HighRiskfromNAICS
# MAGIC     -- , CASE
# MAGIC     --     WHEN t3.QuestionId IN (474, 645, 996) THEN t4.Text
# MAGIC     --     -- ELSE ''
# MAGIC     -- END AS NAICSCodes
# MAGIC     , t3.QuestionId
# MAGIC
# MAGIC FROM radar.siraclients t1
# MAGIC LEFT JOIN InstancePerNPClientId t2 ON t1.SiraClientId = t2.NaturalPersonClientId
# MAGIC LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id -- and t3.QuestionId = t4.QuestionId?
# MAGIC WHERE t4.QuestionId IN (248, 475, 647, 519, 646, 474, 645, 996, 997)
# MAGIC   AND t1.SourceSystemReference = 'GCOB_NP-NPPC'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.SiraHighRiskSector

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

spark.sql('SELECT * FROM SiraHighRiskSector').withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.SiraHighRiskSector')

# COMMAND ----------

# DBTITLE 1,append to historical dataset if it is the first of the quarter!
from pyspark.sql.functions import lit, to_date
from datetime import datetime

# if it is the first of the quarter, change the EDL_LoadDate column and append it to the historical dataset

date_obj = datetime.strptime(load_date, '%Y%m%d').date()

if date_obj.day == 1 and date_obj.month in [1, 4, 7, 10]:
    (spark.table("radar.SiraHighRiskSector")
        .withColumn('EDL_LoadDate', to_date(lit(load_date), 'yyyyMMdd'))
        .write
        .mode('append')
        .saveAsTable('radar.SiraHighRiskSector_historical')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### NO CHECKS ON radar.SiraHighRiskSector!!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complex Files Logic

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_StructureElements AS
# MAGIC
# MAGIC select SourceClient, count(*) AS NrOfTrustOrFundElements
# MAGIC from radar.allrelationships
# MAGIC where `HasGeneralPartner` = 'True'  --fund
# MAGIC OR`HasLimitedPartner` = 'True' --fund
# MAGIC OR  HasFundManagedBy = 'True' --fund
# MAGIC OR  HasBeneficiary = 'True' --trust
# MAGIC OR  HasSettlorFounder = 'True' --trust
# MAGIC OR  HasTrustee = 'True' --trust
# MAGIC OR  HasProtector= 'True' --trust
# MAGIC GROUP BY SourceClient

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ComplexFiles AS
# MAGIC WITH Base AS (
# MAGIC     SELECT
# MAGIC         t1.uniquegcobid,
# MAGIC
# MAGIC         -- ==========================
# MAGIC         -- 1) File Complexity Logic
# MAGIC         -- ==========================
# MAGIC         CASE 
# MAGIC         --S1: Involves EC High Risk 3rd Countries and is a High-Risk File
# MAGIC             WHEN 
# MAGIC             t3.questionid = '530' 
# MAGIC             AND t1.ValidatedRiskLevel = 'High' 
# MAGIC             AND t4.text <> 'None'
# MAGIC             THEN 'High Complexity File'
# MAGIC         --S2: SAR in the past
# MAGIC             WHEN 
# MAGIC             (t3.questionid = '862' AND t4.text = 'No') 
# MAGIC             AND t1.TransactionRiskLevel = 'High' 
# MAGIC             THEN 'High Complexity File'
# MAGIC
# MAGIC         --S3: Involve Exposure to High Risk Sanction country or/and Dual use products
# MAGIC             WHEN 
# MAGIC             (t3.questionid = '529' AND t4.text IN ('Belarus','Cuba','Iran','Korea North','North Korea','Russia','Sudan','Syria')) OR (t3.questionid IN ('559','774','819','647') AND t4.text = 'Dual Use')
# MAGIC             --AND t1.ValidatedRiskLevel = 'High' 
# MAGIC             THEN 'High Complexity File'
# MAGIC
# MAGIC         --S4: Involve EC High Risk 3rd Countries and it is a Medium-Risk File and Involve Private Equity and/or Trust
# MAGIC             WHEN 
# MAGIC             (t3.questionid = '530' AND t1.ValidatedRiskLevel = 'Medium' AND t4.text <> 'None') 
# MAGIC             AND ( t1.cddtype IN ('Fund / Collective investment scheme', 'Limited Partnership')OR t5.NrOfTrustOrFundElements > 0)
# MAGIC             THEN 'High Complexity File'
# MAGIC
# MAGIC         --S5: Involve EC High Risk 3rd Countries and it is a Medium-Risk File and Involves PEP
# MAGIC             WHEN 
# MAGIC             (t3.questionid = '530' AND t1.ValidatedRiskLevel = 'Medium' AND t4.text <> 'None')
# MAGIC             AND t6.IsPEP = 'PEP'
# MAGIC             AND t1.PEPRiskLevel = 'High'
# MAGIC             THEN 'High Complexity File'
# MAGIC
# MAGIC         --S6: Involve Private Equity and/or Trust and Involves PEP
# MAGIC             WHEN 
# MAGIC             (t1.cddtype IN ('Fund / Collective investment scheme', 'Limited Partnership') OR t5.NrOfTrustOrFundElements > 0)
# MAGIC             AND t3.questionid = '786'
# MAGIC             AND t1.PEPRiskLevel = 'High'
# MAGIC             THEN 'High Complexity File'
# MAGIC
# MAGIC         --S7: Involves EC High Risk 3rd Countries, and is a Medium-Risk File
# MAGIC             WHEN 
# MAGIC             t3.questionid = '530' AND t1.ValidatedRiskLevel = 'Medium' 
# MAGIC             AND t4.text <> 'None'
# MAGIC             THEN 'Medium Complexity file'
# MAGIC
# MAGIC         --S8: Involves PEP
# MAGIC             WHEN 
# MAGIC             t3.questionid = '786' 
# MAGIC             AND t1.PEPRiskLevel = 'High'
# MAGIC             THEN 'Medium Complexity file'
# MAGIC
# MAGIC         --S9: Involve Private Equity and/or Trust
# MAGIC             WHEN 
# MAGIC             t1.cddtype IN ('Fund / Collective investment scheme', 'Limited Partnership')
# MAGIC             OR t5.NrOfTrustOrFundElements > 0
# MAGIC             THEN 'Medium Complexity file'
# MAGIC         END AS FileComplexity,
# MAGIC
# MAGIC         -- ==========================
# MAGIC         -- 2) S1–S9 Flags Logic
# MAGIC         -- ==========================
# MAGIC         CASE 
# MAGIC         --S1: Involves EC High Risk 3rd Countries and is a High-Risk File
# MAGIC             WHEN t3.questionid = '530' AND t1.ValidatedRiskLevel = 'High' AND t4.text <> 'None' THEN 'S1'
# MAGIC         
# MAGIC         --S2: SAR in the past            
# MAGIC             WHEN (t3.questionid = '862' AND t4.text = 'No') 
# MAGIC             AND t1.TransactionRiskLevel = 'High'
# MAGIC             THEN 'S2'
# MAGIC         
# MAGIC         --S3: Involve Exposure to High Risk Sanction country or/and Dual use products            
# MAGIC             WHEN (t3.questionid = '529' AND t4.text IN ('Belarus','Cuba','Iran','Korea North','North Korea','Russia','Sudan','Syria')) OR (t3.questionid IN ('559','774','819','647') AND t4.text = 'Dual Use')
# MAGIC             --AND t1.ValidatedRiskLevel = 'High'
# MAGIC             THEN 'S3'
# MAGIC
# MAGIC         --S4: Involve EC High Risk 3rd Countries and it is a Medium-Risk File and Involve Private Equity and/or Trust
# MAGIC             WHEN (t3.questionid = '530' AND t1.ValidatedRiskLevel = 'Medium' AND t4.text <> 'None')
# MAGIC             AND (t1.cddtype IN ('Fund / Collective investment scheme', 'Limited Partnership') OR t5.NrOfTrustOrFundElements > 0)
# MAGIC             THEN 'S4'
# MAGIC
# MAGIC         --S5: Involve EC High Risk 3rd Countries and it is a Medium-Risk File and Involves PEP
# MAGIC             WHEN (t3.questionid = '530' AND t1.ValidatedRiskLevel = 'Medium' and t4.text <> 'None')
# MAGIC             AND t6.IsPEP = 'PEP'
# MAGIC             AND t1.PEPRiskLevel = 'High'
# MAGIC             THEN 'S5'
# MAGIC
# MAGIC         --S6: Involve Private Equity and/or Trust and Involves PEP
# MAGIC             WHEN (t1.cddtype IN ('Fund / Collective investment scheme', 'Limited Partnership') OR t5.NrOfTrustOrFundElements > 0)
# MAGIC             AND t3.questionid = '786'
# MAGIC             AND t1.PEPRiskLevel = 'High'
# MAGIC             THEN 'S6'
# MAGIC
# MAGIC         --S7: Involves EC High Risk 3rd Countries, and is a Medium-Risk File    
# MAGIC             WHEN t3.questionid = '530' AND t1.ValidatedRiskLevel = 'Medium' AND t4.text <> 'None' THEN 'S7'
# MAGIC
# MAGIC         --S8: Involves PEP    
# MAGIC             WHEN t3.questionid = '786' AND t1.PEPRiskLevel = 'High' THEN 'S8'
# MAGIC
# MAGIC         --S9: Involve Private Equity and/or Trust   
# MAGIC             WHEN 
# MAGIC             t1.cddtype IN ('Fund / Collective investment scheme', 'Limited Partnership')
# MAGIC             OR t5.NrOfTrustOrFundElements > 0
# MAGIC             THEN 'S9'
# MAGIC         
# MAGIC         END AS ComplexFileType
# MAGIC     FROM radar.siraclients t1
# MAGIC     LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId
# MAGIC     LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC     LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
# MAGIC     LEFT JOIN GCOB_StructureElements t5 ON t1.SourceClient = t5.SourceClient
# MAGIC     LEFT JOIN radar.sirapepubo t6 ON t1.UniqueGcobId = t6.UniqueGcobId
# MAGIC )
# MAGIC
# MAGIC SELECT  
# MAGIC     uniquegcobid,
# MAGIC     -- ==========================
# MAGIC     -- Final File Complexity (High > Medium > NULL)
# MAGIC     -- ==========================
# MAGIC     CASE 
# MAGIC         WHEN MAX(CASE WHEN FileComplexity = 'High Complexity File' THEN 3
# MAGIC                       WHEN FileComplexity = 'Medium Complexity file' THEN 2
# MAGIC                       ELSE 1 END) = 3
# MAGIC             THEN 'High Complexity File'
# MAGIC         WHEN MAX(CASE WHEN FileComplexity = 'High Complexity File' THEN 3
# MAGIC                       WHEN FileComplexity = 'Medium Complexity file' THEN 2
# MAGIC                       ELSE 1 END) = 2
# MAGIC             THEN 'Medium Complexity file'
# MAGIC         ELSE NULL
# MAGIC     END AS FileComplexity,
# MAGIC     -- ==========================
# MAGIC     -- All applicable S1–S9 flags
# MAGIC     -- ==========================
# MAGIC     concat_ws('|', sort_array(collect_set(ComplexFileType))) AS ComplexFileTypes
# MAGIC
# MAGIC FROM Base
# MAGIC GROUP BY uniquegcobid
# MAGIC */

# COMMAND ----------

# DBTITLE 1,Final Version with EBX list from GDP
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ComplexFiles AS
# MAGIC WITH Base AS (
# MAGIC     SELECT
# MAGIC         t1.uniquegcobid,
# MAGIC         t1.sourceClient,
# MAGIC         -- ==========================
# MAGIC         -- 1) File Complexity Logic
# MAGIC         -- ==========================
# MAGIC         CASE 
# MAGIC         --S1: Involves EC High Risk 3rd Countries and is a High-Risk File
# MAGIC             WHEN 
# MAGIC             t3.questionid = '530' 
# MAGIC             AND t1.ValidatedRiskLevel = 'High' 
# MAGIC             AND t4.text <> 'None'
# MAGIC             THEN 'High Complexity File'
# MAGIC         --S2: SAR in the past
# MAGIC             WHEN 
# MAGIC             (t3.questionid = '862' AND t4.text = 'No') 
# MAGIC             AND t1.TransactionRiskLevel = 'High' 
# MAGIC             THEN 'High Complexity File'
# MAGIC
# MAGIC         --S3: Involve Exposure to High Risk Sanction country or/and Dual use products
# MAGIC             WHEN 
# MAGIC             (t3.questionid = '529' AND t7.Sanctions_Risk = 'High') OR (t3.questionid IN ('559','774','819','647') AND t4.text = 'Dual Use')
# MAGIC             --AND t1.ValidatedRiskLevel = 'High' 
# MAGIC             THEN 'High Complexity File'
# MAGIC
# MAGIC         --S4: Involve EC High Risk 3rd Countries and it is a Medium-Risk File and Involve Private Equity and/or Trust
# MAGIC             WHEN 
# MAGIC             (t3.questionid = '530' AND t1.ValidatedRiskLevel = 'Medium' AND t4.text <> 'None') 
# MAGIC             AND ( t1.cddtype IN ('Fund / Collective investment scheme', 'Limited Partnership')OR t5.NrOfTrustOrFundElements > 0)
# MAGIC             THEN 'High Complexity File'
# MAGIC
# MAGIC         --S5: Involve EC High Risk 3rd Countries and it is a Medium-Risk File and Involves PEP
# MAGIC             WHEN 
# MAGIC             (t3.questionid = '530' AND t1.ValidatedRiskLevel = 'Medium' AND t4.text <> 'None')
# MAGIC             AND t6.IsPEP = 'PEP'
# MAGIC             AND t1.PEPRiskLevel = 'High'
# MAGIC             THEN 'High Complexity File'
# MAGIC
# MAGIC         --S6: Involve Private Equity and/or Trust and Involves PEP
# MAGIC             WHEN 
# MAGIC             (t1.cddtype IN ('Fund / Collective investment scheme', 'Limited Partnership') OR t5.NrOfTrustOrFundElements > 0)
# MAGIC             AND t3.questionid = '786'
# MAGIC             AND t1.PEPRiskLevel = 'High'
# MAGIC             THEN 'High Complexity File'
# MAGIC
# MAGIC         --S7: Involves EC High Risk 3rd Countries, and is a Medium-Risk File
# MAGIC             WHEN 
# MAGIC             t3.questionid = '530' AND t1.ValidatedRiskLevel = 'Medium' 
# MAGIC             AND t4.text <> 'None'
# MAGIC             THEN 'Medium Complexity file'
# MAGIC
# MAGIC         --S8: Involves PEP
# MAGIC             WHEN 
# MAGIC             t3.questionid = '786' 
# MAGIC             AND t1.PEPRiskLevel = 'High'
# MAGIC             THEN 'Medium Complexity file'
# MAGIC
# MAGIC         --S9: Involve Private Equity and/or Trust
# MAGIC             WHEN 
# MAGIC             t1.cddtype IN ('Fund / Collective investment scheme', 'Limited Partnership')
# MAGIC             OR t5.NrOfTrustOrFundElements > 0
# MAGIC             THEN 'Medium Complexity file'
# MAGIC         END AS FileComplexity,
# MAGIC
# MAGIC         -- ==========================
# MAGIC         -- 2) S1–S9 Flags Logic
# MAGIC         -- ==========================
# MAGIC         CASE 
# MAGIC         --S1: Involves EC High Risk 3rd Countries and is a High-Risk File
# MAGIC             WHEN t3.questionid = '530' AND t1.ValidatedRiskLevel = 'High' AND t4.text <> 'None' THEN 'S1'
# MAGIC         
# MAGIC         --S2: SAR in the past            
# MAGIC             WHEN (t3.questionid = '862' AND t4.text = 'No') 
# MAGIC             AND t1.TransactionRiskLevel = 'High'
# MAGIC             THEN 'S2'
# MAGIC         
# MAGIC         --S3: Involve Exposure to High Risk Sanction country or/and Dual use products            
# MAGIC             WHEN (t3.questionid = '529' AND t7.Sanctions_Risk = 'High') OR (t3.questionid IN ('559','774','819','647') AND t4.text = 'Dual Use')
# MAGIC             --AND t1.ValidatedRiskLevel = 'High'
# MAGIC             THEN 'S3'
# MAGIC
# MAGIC         --S4: Involve EC High Risk 3rd Countries and it is a Medium-Risk File and Involve Private Equity and/or Trust
# MAGIC             WHEN (t3.questionid = '530' AND t1.ValidatedRiskLevel = 'Medium' AND t4.text <> 'None')
# MAGIC             AND (t1.cddtype IN ('Fund / Collective investment scheme', 'Limited Partnership') OR t5.NrOfTrustOrFundElements > 0)
# MAGIC             THEN 'S4'
# MAGIC
# MAGIC         --S5: Involve EC High Risk 3rd Countries and it is a Medium-Risk File and Involves PEP
# MAGIC             WHEN (t3.questionid = '530' AND t1.ValidatedRiskLevel = 'Medium' and t4.text <> 'None')
# MAGIC             AND t6.IsPEP = 'PEP'
# MAGIC             AND t1.PEPRiskLevel = 'High'
# MAGIC             THEN 'S5'
# MAGIC
# MAGIC         --S6: Involve Private Equity and/or Trust and Involves PEP
# MAGIC             WHEN (t1.cddtype IN ('Fund / Collective investment scheme', 'Limited Partnership') OR t5.NrOfTrustOrFundElements > 0)
# MAGIC             AND t3.questionid = '786'
# MAGIC             AND t1.PEPRiskLevel = 'High'
# MAGIC             THEN 'S6'
# MAGIC
# MAGIC         --S7: Involves EC High Risk 3rd Countries, and is a Medium-Risk File    
# MAGIC             WHEN t3.questionid = '530' AND t1.ValidatedRiskLevel = 'Medium' AND t4.text <> 'None' THEN 'S7'
# MAGIC
# MAGIC         --S8: Involves PEP    
# MAGIC             WHEN t3.questionid = '786' AND t1.PEPRiskLevel = 'High' THEN 'S8'
# MAGIC
# MAGIC         --S9: Involve Private Equity and/or Trust   
# MAGIC             WHEN 
# MAGIC             t1.cddtype IN ('Fund / Collective investment scheme', 'Limited Partnership')
# MAGIC             OR t5.NrOfTrustOrFundElements > 0
# MAGIC             THEN 'S9'
# MAGIC         
# MAGIC         END AS ComplexFileType
# MAGIC     FROM radar.siraclients t1
# MAGIC     LEFT JOIN InstancePerLEClientId t2 ON t1.SiraClientId = t2.LegalEntityClientId
# MAGIC     LEFT JOIN RiskModel_dbo_InstanceAnswer t3 ON t2.InstanceId = t3.InstanceId
# MAGIC     LEFT JOIN RiskModel_dbo_PossibleAnswer t4 ON t3.PossibleAnswerId = t4.Id
# MAGIC     LEFT JOIN GCOB_StructureElements t5 ON t1.SourceClient = t5.SourceClient
# MAGIC     LEFT JOIN radar.sirapepubo t6 ON t1.UniqueGcobId = t6.UniqueGcobId
# MAGIC     LEFT JOIN radar.SiraCountryList t7 on t4.Value = t7.ISO_code
# MAGIC )
# MAGIC
# MAGIC SELECT  
# MAGIC     uniquegcobid,
# MAGIC     sourceClient,
# MAGIC     -- ==========================
# MAGIC     -- Final File Complexity (High > Medium > NULL)
# MAGIC     -- ==========================
# MAGIC     CASE 
# MAGIC         WHEN MAX(CASE WHEN FileComplexity = 'High Complexity File' THEN 3
# MAGIC                       WHEN FileComplexity = 'Medium Complexity file' THEN 2
# MAGIC                       ELSE 1 END) = 3
# MAGIC             THEN 'High Complexity File'
# MAGIC         WHEN MAX(CASE WHEN FileComplexity = 'High Complexity File' THEN 3
# MAGIC                       WHEN FileComplexity = 'Medium Complexity file' THEN 2
# MAGIC                       ELSE 1 END) = 2
# MAGIC             THEN 'Medium Complexity file'
# MAGIC         ELSE NULL
# MAGIC     END AS FileComplexity,
# MAGIC     -- ==========================
# MAGIC     -- All applicable S1–S9 flags
# MAGIC     -- ==========================
# MAGIC     concat_ws('|', sort_array(collect_set(ComplexFileType))) AS ComplexFileTypes
# MAGIC
# MAGIC FROM Base
# MAGIC GROUP BY uniquegcobid, sourceClient

# COMMAND ----------

spark.sql('drop table if exists radar.ComplexFiles')
spark.sql('SELECT * FROM ComplexFiles').write.mode('overwrite').saveAsTable('radar.ComplexFiles')
