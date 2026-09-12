# Databricks notebook source
# MAGIC %sql
# MAGIC select * from radar.siraclientrisks where UniqueGcobId = 126136

# COMMAND ----------

# MAGIC %sql
# MAGIC select CaseReviewType, UniqueGcobId, clientid from radar.cases where clientid = 126136

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.siraclientrisks_historical where UniqueGcobId = 126136

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct face2facecontact, count(*) from radar.siraclientrisks_historical group by face2facecontact

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.GlobalKYCPortfolioNew AS GlobalFECPortfolio
# MAGIC   , t1.SectorTeam
# MAGIC   , t1.ReviewLocation AS ReviewTeam
# MAGIC   , t1.FullLegalName AS ClientName
# MAGIC   , t1.KYCGroup AS GroupName
# MAGIC   , t1.ValidatedRiskLevel AS RiskLevel
# MAGIC   , t2.CasePhase
# MAGIC   , t1.ClientCaseInitiationStart
# MAGIC   , t2.Prework AS PreworkStartDate
# MAGIC   , t1.NextReviewDate
# MAGIC   , t2.TotalCaseDuration
# MAGIC   , t2.DaysInCurrentCasePhase
# MAGIC   , t1.GlobalClientOwner
# MAGIC   , t1.GlobalClientOwnerLocation
# MAGIC   , t1.BusinessLineName
# MAGIC
# MAGIC   , t1.Overdue
# MAGIC
# MAGIC   , datediff(t1.EDL_LoadDate, t1.NextReviewDate) AS DaysInOverdue
# MAGIC
# MAGIC   , t1.EDL_LoadDate AS YearMonthDay
# MAGIC   
# MAGIC
# MAGIC FROM radar.clients_historical t1
# MAGIC LEFT JOIN radar.cases_historical t2 ON t1.SourceClient = t2.SourceClient AND t1.EDL_LoadDate = t2.EDL_LoadDate
# MAGIC
# MAGIC WHERE t1.Scope = 'backToGreen'
# MAGIC   AND t2.CaseReviewType = 'Periodic Review'
# MAGIC   AND t1.ClientLifeCycleName = 'Client'
# MAGIC   AND t1.Overdue = 'Yes'
# MAGIC
# MAGIC   AND YEAR(t1.EDL_LoadDate) = 2025
# MAGIC   AND t1.EDL_LoadDate <> '2025-11-01'
# MAGIC
# MAGIC ORDER BY t1.EDL_LoadDate DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.GlobalKYCPortfolioNew AS GlobalFECPortfolio
# MAGIC   , t1.SectorTeam
# MAGIC   , t1.ReviewLocation AS ReviewTeam
# MAGIC   , t1.FullLegalName AS ClientName
# MAGIC   , t1.KYCGroup AS GroupName
# MAGIC   , t1.ValidatedRiskLevel AS RiskLevel
# MAGIC   , t2.CasePhase
# MAGIC   , t2.Prework AS EDRStartDate
# MAGIC   , t1.EDRDueDate
# MAGIC   , t2.TotalCaseDuration
# MAGIC   , t2.DaysInCurrentCasePhase
# MAGIC   , t1.NextReviewDate
# MAGIC   , t1.GlobalClientOwner
# MAGIC   , t1.GlobalClientOwnerLocation
# MAGIC   , t1.BusinessLineName
# MAGIC   , t2.EDROverdue
# MAGIC
# MAGIC   , datediff(t1.EDL_LoadDate, t1.EDRDueDate) AS DaysInOverdue
# MAGIC
# MAGIC   , t1.EDL_LoadDate AS YearMonthDay
# MAGIC   
# MAGIC
# MAGIC FROM radar.clients_historical t1
# MAGIC LEFT JOIN radar.cases_historical t2 ON t1.SourceClient = t2.SourceClient AND t1.EDL_LoadDate = t2.EDL_LoadDate
# MAGIC
# MAGIC WHERE t1.Scope = 'backToGreen'
# MAGIC   -- AND t2.CaseReviewType = 'Event Driven Review'
# MAGIC   AND t2.EDROverdue = 'Yes'
# MAGIC   AND t1.ClientLifeCycleName = 'Client'
# MAGIC
# MAGIC   AND YEAR(t1.EDL_LoadDate) = 2025
# MAGIC   AND t1.EDL_LoadDate <> '2025-11-01'
# MAGIC
# MAGIC ORDER BY t1.EDL_LoadDate DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select 4EYEanalyst from radar.cases where SourceClient = ''

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW test_historical AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   UniqueGcobId
# MAGIC   , AttributeValue
# MAGIC FROM radar.n2k_clientattribute
# MAGIC WHERE
# MAGIC   LEFT(UniqueGcobId, 4) <> 'NP_N'
# MAGIC   AND LEFT(UniqueGcobId, 3) <> 'LE_'
# MAGIC   AND AttributeValue IN ('RegionEA', 'RegionRANZ', 'RegionSA', 'RegionNA', 'RegionAsia')
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   UniqueGcobId
# MAGIC   , 'RegionRANZ' AS AttributeValue
# MAGIC FROM radar.clients_historical
# MAGIC WHERE RANZProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved')
# MAGIC   AND UniqueGcobId NOT IN (
# MAGIC     SELECT DISTINCT
# MAGIC       UniqueGcobId
# MAGIC     FROM radar.n2k_clientattribute
# MAGIC     WHERE
# MAGIC       LEFT(UniqueGcobId, 4) <> 'NP_N'
# MAGIC       AND LEFT(UniqueGcobId, 3) <> 'LE_'
# MAGIC       AND AttributeValue IN ('RegionEA', 'RegionRANZ', 'RegionSA', 'RegionNA', 'RegionAsia')
# MAGIC   )
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   UniqueGcobId
# MAGIC   , 'RegionSA' AS AttributeValue
# MAGIC FROM radar.clients_historical
# MAGIC WHERE SAProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved')
# MAGIC   AND UniqueGcobId NOT IN (
# MAGIC     SELECT DISTINCT
# MAGIC       UniqueGcobId
# MAGIC     FROM radar.n2k_clientattribute
# MAGIC     WHERE
# MAGIC       LEFT(UniqueGcobId, 4) <> 'NP_N'
# MAGIC       AND LEFT(UniqueGcobId, 3) <> 'LE_'
# MAGIC       AND AttributeValue IN ('RegionEA', 'RegionRANZ', 'RegionSA', 'RegionNA', 'RegionAsia')
# MAGIC   )
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   UniqueGcobId
# MAGIC   , 'RegionNA' AS AttributeValue
# MAGIC FROM radar.clients_historical
# MAGIC WHERE NAProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved')
# MAGIC   AND UniqueGcobId NOT IN (
# MAGIC     SELECT DISTINCT
# MAGIC       UniqueGcobId
# MAGIC     FROM radar.n2k_clientattribute
# MAGIC     WHERE
# MAGIC       LEFT(UniqueGcobId, 4) <> 'NP_N'
# MAGIC       AND LEFT(UniqueGcobId, 3) <> 'LE_'
# MAGIC       AND AttributeValue IN ('RegionEA', 'RegionRANZ', 'RegionSA', 'RegionNA', 'RegionAsia')
# MAGIC   )
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   UniqueGcobId
# MAGIC   , 'RegionAsia' AS AttributeValue
# MAGIC FROM radar.clients_historical
# MAGIC WHERE AsiaProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved')
# MAGIC   AND UniqueGcobId NOT IN (
# MAGIC     SELECT DISTINCT
# MAGIC       UniqueGcobId
# MAGIC     FROM radar.n2k_clientattribute
# MAGIC     WHERE
# MAGIC       LEFT(UniqueGcobId, 4) <> 'NP_N'
# MAGIC       AND LEFT(UniqueGcobId, 3) <> 'LE_'
# MAGIC       AND AttributeValue IN ('RegionEA', 'RegionRANZ', 'RegionSA', 'RegionNA', 'RegionAsia')
# MAGIC   )
# MAGIC    
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   UniqueGcobId
# MAGIC   , 'RegionEA' AS AttributeValue
# MAGIC FROM radar.clients_historical
# MAGIC WHERE EAProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved')
# MAGIC   AND UniqueGcobId NOT IN (
# MAGIC     SELECT DISTINCT
# MAGIC       UniqueGcobId
# MAGIC     FROM radar.n2k_clientattribute
# MAGIC     WHERE
# MAGIC       LEFT(UniqueGcobId, 4) <> 'NP_N'
# MAGIC       AND LEFT(UniqueGcobId, 3) <> 'LE_'
# MAGIC       AND AttributeValue IN ('RegionEA', 'RegionRANZ', 'RegionSA', 'RegionNA', 'RegionAsia')
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from test_historical

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.clients where SourceClient = 'LEC_116733'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.userteamregistry where userName = 'Fortune, J (Jamie)'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct GlobalReportingRegion, count(*) from radar.clients group by GlobalReportingRegion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   UPN
# MAGIC   , AttributeValue
# MAGIC FROM radar.n2k_userattribute
# MAGIC WHERE
# MAGIC   LEFT(UPN, 4) <> 'NP_N'
# MAGIC   AND LEFT(UPN, 3) <> 'LE_'
# MAGIC   AND AttributeValue IN ('RegionEA', 'RegionRANZ', 'RegionSA', 'RegionNA', 'RegionAsia', 'MTO', 'FIHub')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   UniqueGcobId
# MAGIC   , AttributeValue
# MAGIC FROM radar.n2k_clientattribute
# MAGIC WHERE
# MAGIC   LEFT(UniqueGcobId, 4) <> 'NP_N'
# MAGIC   AND LEFT(UniqueGcobId, 3) <> 'LE_'
# MAGIC   AND AttributeValue IN ('RegionEA', 'RegionRANZ', 'RegionSA', 'RegionNA', 'RegionAsia', 'MTO', 'FIHub')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   UPN
# MAGIC   , CASE
# MAGIC       WHEN AttributeValue = 'RegionEA' THEN 'E&A'
# MAGIC       WHEN AttributeValue = 'RegionRANZ' THEN 'RANZ'
# MAGIC       WHEN AttributeValue = 'RegionSA' THEN 'South America'
# MAGIC       WHEN AttributeValue = 'RegionNA' THEN 'North America'
# MAGIC       WHEN AttributeValue = 'RegionAsia' THEN 'Asia'
# MAGIC     END AS AttributeValue
# MAGIC FROM radar.n2k_userattribute
# MAGIC WHERE
# MAGIC   AttributeValue IN ('RegionEA', 'RegionRANZ', 'RegionSA', 'RegionNA', 'RegionAsia')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AttributeValue, UPN from radar.n2k_gcid_userattribute where AttributeValue in (
# MAGIC     'RegionAsia'
# MAGIC     , 'RegionNA'
# MAGIC     , 'DepartmentTCF'
# MAGIC     , 'RegionRANZ'
# MAGIC     , 'RegionEA'
# MAGIC     , 'RegionSA'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AttributeValue, GCID from radar.n2k_gcid_clientattribute where AttributeValue in (
# MAGIC     'RegionAsia'
# MAGIC     , 'RegionNA'
# MAGIC     , 'DepartmentTCF'
# MAGIC     , 'RegionRANZ'
# MAGIC     , 'RegionEA'
# MAGIC     , 'RegionSA'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   t1.UPN
# MAGIC   , t1.AttributeValue
# MAGIC FROM radar.n2k_userattribute t1
# MAGIC WHERE
# MAGIC   t1.AttributeValue IN ('RegionEA', 'RegionRANZ', 'RegionSA', 'RegionNA', 'RegionAsia', 'MTO', 'RegionFoundation', 'FIHub', 'DepartmentTCF')
# MAGIC   -- include other values only if UPN is not in RegionEA_CTE
# MAGIC   -- OR (
# MAGIC   --   t1.UPN NOT IN (
# MAGIC   --     SELECT DISTINCT UPN
# MAGIC   --     FROM radar.n2k_userattribute
# MAGIC   --     WHERE AttributeValue = 'RegionEA'
# MAGIC   --   )
# MAGIC   --   AND t1.AttributeValue IN (
# MAGIC   --     'Rabobank Netherlands', 'Rabobank London', 'Rabobank Paris',
# MAGIC   --     'Rabobank Frankfurt', 'Rabobank Kenya', 'Rabobank Dublin',
# MAGIC   --     'Rabobank Madrid', 'Rabobank Milan', 'Rabobank Turkey',
# MAGIC   --     'Rabobank Antwerp'
# MAGIC   --   )
# MAGIC   -- )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   t1.UPN
# MAGIC   , t1.AttributeValue
# MAGIC FROM radar.n2k_userattribute t1
# MAGIC WHERE
# MAGIC   t1.AttributeValue IN ('RegionEA', 'RegionRANZ', 'RegionSA', 'RegionNA', 'RegionAsia', 'MTO', 'RegionFoundation', 'FIHub', 'DepartmentTCF')
# MAGIC   -- include other values only if UPN is not in RegionEA_CTE
# MAGIC   OR (
# MAGIC     t1.UPN NOT IN (
# MAGIC       SELECT DISTINCT UPN
# MAGIC       FROM radar.n2k_userattribute
# MAGIC       WHERE AttributeValue = 'RegionEA'
# MAGIC     )
# MAGIC     AND t1.AttributeValue IN (
# MAGIC       'Rabobank Netherlands', 'Rabobank London', 'Rabobank Paris',
# MAGIC       'Rabobank Frankfurt', 'Rabobank Kenya', 'Rabobank Dublin',
# MAGIC       'Rabobank Madrid', 'Rabobank Milan', 'Rabobank Turkey',
# MAGIC       'Rabobank Antwerp'
# MAGIC     )
# MAGIC   )
# MAGIC   AND t1.UPN IN (
# MAGIC     'Yvonne.Frew@rabobank.com'
# MAGIC     , 'Coen.van.Henten@rabobank.com'
# MAGIC     , 'Adela-Mariana.Stefanescu@rabobank.com'
# MAGIC     , 'Gordillio.LV.Clemens01@rabobank.com'
# MAGIC     , 'Eric.Vorstenbosch@rabobank.nl'
# MAGIC     , 'Dzenan.Masic@rabobank.nl'
# MAGIC     , 'Dirk.van.der.Heijden@rabobank.nl'
# MAGIC     , 'Dean.Toms@rabobank.com'
# MAGIC     , 'Paul.Bowerman@rabobank.com'
# MAGIC     , 'Renny.Johnson@rabobank.com'
# MAGIC     , 'Hannah.Metcalfe@rabobank.com'
# MAGIC     , 'Nadine.Williams@rabobank.com'
# MAGIC     , 'Priscilla.Berende@rabobank.nl'
# MAGIC     , 'Chris.van.den.Eijnden@rabobank.com'
# MAGIC     , 'Martine.Martens@rabobank.nl'
# MAGIC     , 'Jamie.Fortune@rabobank.com'
# MAGIC     , 'Lizzie.Brooking@rabobank.com'
# MAGIC     , 'Valentina.Paini@rabobank.com'
# MAGIC     , 'Salih.Karagoz@rabobank.nl'
# MAGIC     , 'Sinead.McCormack@rabobank.com'
# MAGIC     , 'Gulce.Atak@rabobank.com'
# MAGIC     , 'Fruzsina.Szabo@rabobank.nl'
# MAGIC     , 'Maarten.Platvoet@rabobank.nl'
# MAGIC     , 'Henrieke.Dielen@rabobank.nl'
# MAGIC     , 'Neil.Regelous@rabobank.com'
# MAGIC     , 'Khalid.Daif@rabobank.com'
# MAGIC     , 'Gino.Cohen@rabobank.com'
# MAGIC     , 'Paul.Searle@rabobank.com'
# MAGIC     , 'Rupert.Evans@rabobank.com'
# MAGIC     , 'Chris.Halford@rabobank.com'
# MAGIC     , 'Connor.Scott@rabobank.com'
# MAGIC     , 'Mizan.Miah@rabobank.com'
# MAGIC     , 'Rinke.Blom@rabobank.nl'
# MAGIC     , 'Radjanne.Coffie@rabobank.com'
# MAGIC     , 'Martin.Johnson@rabobank.com'
# MAGIC     , 'Ashley.van.Hoof@rabobank.com'
# MAGIC     , 'Gregory.Egbobawaye@rabobank.com'
# MAGIC     , 'Paul.van.Schaik@rabobank.com'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW clientattribute AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.AttributeValue
# MAGIC FROM radar.n2k_clientattribute t1
# MAGIC WHERE 
# MAGIC   LEFT(t1.UniqueGcobId, 4) <> 'NP_N'
# MAGIC   AND LEFT(t1.UniqueGcobId, 3) <> 'LE_'
# MAGIC   AND (
# MAGIC     -- include all non-RegionEA values only if UniqueGcobId is not in RegionEA set
# MAGIC     t1.AttributeValue IN ('RegionEA', 'RegionRANZ', 'RegionSA', 'RegionNA', 'RegionAsia', 'MTO', 'RegionFoundation', 'FIHub', 'DepartmentTCF')
# MAGIC     OR (
# MAGIC       t1.UniqueGcobId NOT IN (
# MAGIC         SELECT DISTINCT UniqueGcobId
# MAGIC         FROM radar.n2k_clientattribute
# MAGIC         WHERE AttributeValue = 'RegionEA'
# MAGIC       )
# MAGIC       AND t1.AttributeValue IN (
# MAGIC         'Rabobank Netherlands', 'Rabobank London', 'Rabobank Paris',
# MAGIC         'Rabobank Frankfurt', 'Rabobank Kenya', 'Rabobank Dublin',
# MAGIC         'Rabobank Madrid', 'Rabobank Milan', 'Rabobank Turkey',
# MAGIC         'Rabobank Antwerp'
# MAGIC       )
# MAGIC     )
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SiraClients AS
# MAGIC
# MAGIC SELECT
# MAGIC   t2.UniqueGcobId
# MAGIC   , t2.SourceSystemReference
# MAGIC
# MAGIC   , t1.CompletedId AS SiraClientId --  ApprovedCaseId (previously including t1.LastFullReviewId) -- 22/09/25 removed t1.LastFullReviewId to capture all completed cases
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
# MAGIC   -- , t1.GlobalReportingRegion
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
# MAGIC INNER JOIN radar.cases t2 ON t1.CompletedId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId -- 22/09/25 removed this t1.LastFullReviewId + 16/10/2025 also t1.LatestId to show only completed

# COMMAND ----------

# MAGIC %sql
# MAGIC select GlobalClientOwnerLocation, count(*) from SiraClients where GlobalReportingRegion = 'E&A' group by GlobalClientOwnerLocation

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   UniqueGcobId
# MAGIC   , GlobalKYCPortfolioNew AS GlobalFECPortfolio
# MAGIC   , SectorTeam
# MAGIC   , KYCGroup AS GroupName
# MAGIC   , ValidatedRiskLevel
# MAGIC   , CaseStatusName AS Casephase
# MAGIC   , NextReviewDate
# MAGIC   , CDDExecution AS PlannedAssessmentDate
# MAGIC   , GlobalClientOwner
# MAGIC   , GlobalClientOwnerLocation
# MAGIC   , Preworkanalyst
# MAGIC   , Assessmentanalyst
# MAGIC   , CaseReviewType
# MAGIC   , BusinessLineName
# MAGIC   , GlobalFiles
# MAGIC   , OverdueCategoryNr
# MAGIC
# MAGIC from radar.clients_historical
# MAGIC where GlobalClientOwnerLocation = 'Rabobank Madrid'
# MAGIC   and ClientLifeCycleName = 'Client'
# MAGIC   and EDL_LoadDate = '2025-10-01'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct EDL_LoadDate from radar.clients_historical

# COMMAND ----------

# MAGIC %sql
# MAGIC describe radar.clients_historical

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC   UniqueGcobId
# MAGIC   , FullLegalName
# MAGIC   , CaseId
# MAGIC   , GlobalClientOwner
# MAGIC   , GlobalClientOwnerLocation
# MAGIC   , ClientLifeCycleName
# MAGIC   , CaseHasChangedRisk
# MAGIC   , Completed
# MAGIC   , CaseReviewType
# MAGIC   , ClientLifeCycleName
# MAGIC
# MAGIC   , GeographicalRiskLevel
# MAGIC   , EntityTypeRiskLevel
# MAGIC   , SectorRiskLevel  
# MAGIC   , ProductAndServiceRiskLevel
# MAGIC   , StructureRiskLevel		    
# MAGIC   , TransactionRiskLevel
# MAGIC   , DistributionRiskLevel
# MAGIC   , ThirdPartyRiskLevel			  
# MAGIC   , AdverseInfoRiskLevel
# MAGIC   , PEPRiskLevel
# MAGIC   , ValidatedRiskLevel
# MAGIC
# MAGIC   , PreviousGeographicalRiskLevel
# MAGIC   , PreviousEntityTypeRiskLevel
# MAGIC   , PreviousSectorRiskLevel  
# MAGIC   , PreviousProductAndServiceRiskLevel
# MAGIC   , PreviousStructureRiskLevel		    
# MAGIC   , PreviousTransactionRiskLevel
# MAGIC   , PreviousDistributionRiskLevel
# MAGIC   , PreviousThirdPartyRiskLevel			  
# MAGIC   , PreviousAdverseInfoRiskLevel
# MAGIC   , PreviousPEPRiskLevel
# MAGIC   , PreviousValidatedRiskLevel
# MAGIC
# MAGIC   
# MAGIC   -- , NextReviewDate
# MAGIC   -- , CaseReviewType
# MAGIC   -- , SignOffDate
# MAGIC   -- , Completed
# MAGIC   -- , CountryOfOperation
# MAGIC
# MAGIC from radar.cases
# MAGIC where CaseHasChangedRisk = 1
# MAGIC   AND Completed BETWEEN DATE('2024-10-06') AND DATE('2025-10-05')
# MAGIC   and GlobalClientOwnerLocation = 'Rabobank Madrid'
# MAGIC   and ClientLifeCycleName = 'Client'
# MAGIC
# MAGIC -- where IsPEP = "PEP"
# MAGIC -- 
# MAGIC -- 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   t1.UniqueGcobId
# MAGIC   , t2.FullLegalName
# MAGIC   , t1.SiraClientId as CaseId
# MAGIC   , t2.GlobalClientOwner
# MAGIC   , t2.GlobalClientOwnerLocation
# MAGIC   , t2.ClientLifeCycleName
# MAGIC   -- , t2.NextReviewDate
# MAGIC   -- , t2.CaseReviewType
# MAGIC   -- , t2.SignOffDate
# MAGIC   -- , t2.Completed
# MAGIC   -- , t2.CountryOfOperation
# MAGIC   , t1.Identity as PEPIdentity
# MAGIC   , t1.PEPNationality
# MAGIC   , t1.ResidentialAddressCountry as PEPResidentialAddressCountry
# MAGIC
# MAGIC from radar.sirapepubo t1
# MAGIC left join radar.clients t2 on t1.uniquegcobid = t2.uniquegcobid
# MAGIC
# MAGIC where t1.IsPEP = "PEP"
# MAGIC and t2.GlobalClientOwnerLocation = 'Rabobank Madrid'
# MAGIC and t2.ClientLifeCycleName = 'Client'

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SourceClient from radar.cases where iscurrentcase = 1 order by SourceClient desc limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC describe radar.cases

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC
# MAGIC -- sum(AmountEuro)
# MAGIC -- , UniqueGcobid
# MAGIC *
# MAGIC
# MAGIC from radar.tx where TransactionMonthYear = '2025-06-01' and CreditDebitIndicator = 'Credit' and UniqueGcobid = 66104
# MAGIC -- group by UniqueGcobid
# MAGIC order by AmountEuro desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.tx limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from radar.tx

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from radar.tx

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.cash where PasNnr like "%X%"

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH LatestCases AS (
# MAGIC     SELECT 
# MAGIC         UniqueGcobId,
# MAGIC         MAX(CaseId) AS MaxCaseId
# MAGIC     FROM radar.cases
# MAGIC     WHERE CaseStatusName = 'Completed'
# MAGIC     GROUP BY UniqueGcobId
# MAGIC )
# MAGIC SELECT DISTINCT c.CaseReviewType
# MAGIC FROM radar.cases c
# MAGIC JOIN LatestCases lc ON c.UniqueGcobId = lc.UniqueGcobId AND c.CaseId = lc.MaxCaseId;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check how many completed files (change of CO or product added) there have been after q2
# MAGIC select distinct
# MAGIC   t1.uniquegcobid
# MAGIC   , t1.FullLegalName
# MAGIC   , t1.siraclientid
# MAGIC   , t2.ClientId
# MAGIC   , t2.Completed
# MAGIC   , t2.ReviewReason
# MAGIC   , t2.ReviewReasonOtherExplanation
# MAGIC   , t2.CaseReviewType
# MAGIC   , t1.GlobalClientOwnerLocation
# MAGIC   , t2.GlobalClientOwnerLocation
# MAGIC from radar.siraclients_historical t1
# MAGIC left join radar.cases t2 on t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC   where t2.ClientId > t1.SiraClientId
# MAGIC   and t2.Completed < "2025-07-01"
# MAGIC   and t1.EDL_LoadDate = "2025-07-01"
# MAGIC   and t1.ClientLifeCycleName = 'Client'
# MAGIC   and t1.GlobalReportingRegion in ("E&A", "Asia")
# MAGIC
# MAGIC   and t2.CaseReviewType in ("Tailored Event Assessment") -- 
# MAGIC
# MAGIC   and t2.ReviewReason like "%roduct%"
# MAGIC
# MAGIC   and t1.GlobalClientOwnerLocation <> t2.GlobalClientOwnerLocation
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- order by t1.UniqueGcobId desc
# MAGIC
# MAGIC -- group by t2.CaseReviewType

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC   count(distinct t1.UniqueGcobId)
# MAGIC   -- , t1.GlobalReportingRegion
# MAGIC from radar.siraclients_historical t1
# MAGIC left join radar.cases t2 on t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC   where 1=1
# MAGIC   -- and t2.ClientId > t1.SiraClientId
# MAGIC   -- and t2.Completed < "2025-07-01"
# MAGIC   and t1.EDL_LoadDate = "2025-07-01"
# MAGIC   and t1.ClientLifeCycleName = 'Client'
# MAGIC   and t1.GlobalReportingRegion in ("E&A", "Asia")
# MAGIC
# MAGIC -- group by t1.GlobalReportingRegion
# MAGIC
# MAGIC   -- and t2.CaseReviewType in ("Tailored Event Assessment") -- 
# MAGIC
# MAGIC   -- and t2.ReviewReason like "%roduct%"
# MAGIC
# MAGIC   -- and t1.GlobalClientOwnerLocation <> t2.GlobalClientOwnerLocation

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.cases where ClientId = 115466

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_cases AS (
# MAGIC     SELECT 
# MAGIC       CaseId
# MAGIC       , GCID
# MAGIC       , DateCompleted
# MAGIC       , PerformanceScore
# MAGIC       , ROW_NUMBER() OVER (PARTITION BY GCID ORDER BY DateCompleted DESC) AS rn
# MAGIC     FROM radar.planetcases
# MAGIC     WHERE datecompleted < '2025-04-01'
# MAGIC )
# MAGIC
# MAGIC select DISTINCT
# MAGIC   t1.CaseId
# MAGIC   , t1.GCID
# MAGIC   , t1.DateCompleted
# MAGIC   , t2.FullLegalName
# MAGIC   , t1.PerformanceScore
# MAGIC   , t2.Location
# MAGIC   , t2.BusinessLine
# MAGIC   , t3.gcdsClientlifecyclestatus as ClientLifeCycleStatus
# MAGIC
# MAGIC  from latest_cases t1
# MAGIC LEFT JOIN radar.planetclients t2 ON t1.gcid = t2.gcid
# MAGIC LEFT JOIN radar.gcdsrawdata t3 ON t1.gcid = t3.gcid
# MAGIC
# MAGIC where t1.rn = 1
# MAGIC   and t2.location = 'Spain'
# MAGIC   and t3.gcdsClientlifecyclestatus not in ('Former Prospect', 'Prospect', 'Former Client')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct t1.sourceclient), month(t1.Completed) from radar.cases t1
# MAGIC left join radar.clients t2 on t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC
# MAGIC where t1.CaseReviewType = 'Tailored Event Assessment'
# MAGIC and year(t1.Completed) = 2024
# MAGIC and t2.Scope = 'backToGreen'
# MAGIC and t1.PreworkDepartment = "Front Office KYC"
# MAGIC and t1.PreworkUserTeam <> "FOS Signals"
# MAGIC
# MAGIC
# MAGIC -- and t1.KYCDepartment IN ("Corp CDD Hub UK","Corp CDD Hub NL")
# MAGIC -- and t1.KYCUserTeam IN ("CDD Hub Europe & EF/PF", "CDD Hub UK,TCF,SC","Corp NL - Team 1","Corp NL - Team 2","Corp NL - Team 3","Corp NL - Team 4","Corp UK - Team 1","Corp UK - Team 2","Corp UK - Team 3","Corp UK - Team 4","Corp UK - Team 5","Corp UK - Team 6","Green House")
# MAGIC
# MAGIC
# MAGIC
# MAGIC group by month(t1.Completed)
# MAGIC order by month(t1.Completed) asc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct t1.Preworkanalyst) from radar.cases t1
# MAGIC left join radar.clients t2 on t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC where year(t1.Readyforassessment) = 2025
# MAGIC and t1.PreworkDepartment = "Front Office KYC"
# MAGIC and t1.PreworkUserTeam <> "FOS Signals"
# MAGIC and t1.CaseReviewType in ("Event Driven Review", "Initial On-Boarding","Periodic Review")
# MAGIC and t2.Scope = 'backToGreen'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC     EdrReason
# MAGIC   , OffBoardingReason
# MAGIC   , ReviewReason
# MAGIC   , ReviewReasonOtherExplanation
# MAGIC   , TeaOtherReason
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN NULLIF(TRIM(EdrReason), '') IS NOT NULL AND TRIM(LOWER(EdrReason)) != 'other' THEN EdrReason
# MAGIC       WHEN NULLIF(TRIM(OffBoardingReason), '') IS NOT NULL AND TRIM(LOWER(OffBoardingReason)) != 'other' THEN OffBoardingReason
# MAGIC       WHEN NULLIF(TRIM(ReviewReason), '') IS NOT NULL AND TRIM(LOWER(ReviewReason)) != 'other' THEN ReviewReason
# MAGIC       WHEN NULLIF(TRIM(ReviewReasonOtherExplanation), '') IS NOT NULL AND TRIM(LOWER(ReviewReasonOtherExplanation)) != 'other' THEN ReviewReasonOtherExplanation
# MAGIC       WHEN NULLIF(TRIM(TeaOtherReason), '') IS NOT NULL AND TRIM(LOWER(TeaOtherReason)) != 'other' THEN TeaOtherReason
# MAGIC       WHEN 
# MAGIC           -- if all non-empty values contain "Other"
# MAGIC           (
# MAGIC             (NULLIF(TRIM(EdrReason), '') IS NULL OR TRIM(LOWER(EdrReason)) = 'other') AND
# MAGIC             (NULLIF(TRIM(OffBoardingReason), '') IS NULL OR TRIM(LOWER(OffBoardingReason)) = 'other') AND
# MAGIC             (NULLIF(TRIM(ReviewReason), '') IS NULL OR TRIM(LOWER(ReviewReason)) = 'other') AND
# MAGIC             (NULLIF(TRIM(ReviewReasonOtherExplanation), '') IS NULL OR TRIM(LOWER(ReviewReasonOtherExplanation)) = 'other') AND
# MAGIC             (NULLIF(TRIM(TeaOtherReason), '') IS NULL OR TRIM(LOWER(TeaOtherReason)) = 'other')
# MAGIC           )
# MAGIC           AND (
# MAGIC             NULLIF(TRIM(EdrReason), '') IS NOT NULL OR
# MAGIC             NULLIF(TRIM(OffBoardingReason), '') IS NOT NULL OR
# MAGIC             NULLIF(TRIM(ReviewReason), '') IS NOT NULL OR
# MAGIC             NULLIF(TRIM(ReviewReasonOtherExplanation), '') IS NOT NULL OR
# MAGIC             NULLIF(TRIM(TeaOtherReason), '') IS NOT NULL
# MAGIC           )
# MAGIC       THEN 'Other'
# MAGIC       ELSE NULL
# MAGIC     END AS Reason
# MAGIC
# MAGIC FROM radar.cases

# COMMAND ----------

import os
from datetime import datetime, timedelta
import re


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


gcob_objects = [
    'case_ClientProfile'
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

    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/{version}/data/LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# MAGIC %sql
# MAGIC select t1.*
# MAGIC , t2.UniqueGcobId
# MAGIC , t2.FullLegalName
# MAGIC , t2.caseid
# MAGIC , t3.ClientLifeCycleName
# MAGIC  from case_ClientProfile t1
# MAGIC left join radar.cases t2 on t1.legalentityclientid = t2.clientid and t2.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC left join radar.clients t3 on t2.UniqueGcobId = t3.UniqueGcobId
# MAGIC
# MAGIC and t3.ClientLifeCycleName = 'Client'
# MAGIC order by caseid desc
# MAGIC  limit 1000

# COMMAND ----------

import sys
import os
# Get the current working directory and go up one level
sys.path.append(os.path.dirname(os.getcwd()))
from functions_databricks import load_from_gdp



load_from_gdp('gcob', 'party_case_client_details')

# COMMAND ----------

# MAGIC %sql
# MAGIC select AsiaProductsInvolvementChange, count(*) from radar.cases group by AsiaProductsInvolvementChange

# COMMAND ----------

# MAGIC %sql
# MAGIC select AsiaProductsInvolvementChange, count(*) from radar.cases group by AsiaProductsInvolvementChange

# COMMAND ----------

# MAGIC %sql
# MAGIC select UniqueGcobId, clientid, SourceClient, AsiaProductsInvolvementChange, CaseStatusName, GlobalClientOwnerLocation from radar.cases where UniqueGcobId in (select uniquegcobid from radar.cases where AsiaProductsInvolvementChange = 1) order by UniqueGcobId, clientid
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC -- t2.UniqueGcobId, t2.FullLegalName, t2.SourceClient, t2.EDL_LoadDate, t2.ClientLifeCycleName, t2.Scope, t2.SectorTeam, t1.CaseReviewType, t1.Casephase
# MAGIC count(distinct t1.SourceClient)
# MAGIC from radar.clients_historical t2
# MAGIC left join radar.cases_historical t1 on t1.uniquegcobid = t2.uniquegcobid and t1.EDL_LoadDate = t2.EDL_LoadDate
# MAGIC
# MAGIC
# MAGIC where 1=1
# MAGIC
# MAGIC
# MAGIC and t2.Scope = 'backToGreen'
# MAGIC and t1.CaseReviewType in ('Periodic Review') --, 'Event Driven Review')
# MAGIC and t1.Casephase in ('Client Outreach', 'Rebound Client Outreach')
# MAGIC and t1.EDL_LoadDate = '2025-07-01'
# MAGIC
# MAGIC -- and t2.GlobalKYCPortfolioNew not in ('London Markets')
# MAGIC -- and t2.SectorTeam not in ('Acorn', 'Agency', 'Correspondent Banking', 'FIG - Orphan Desk', 'FIG', 'Foundation', 'Markets', 'PSP Coverage', 'Rabo Frontier Ventures', 'RCI', 'Subsidiaries', 'Treasury', 'ARG', 'TCF FI')
# MAGIC
# MAGIC -- and t2.FullLegalName not in ('Kadans Science Partners II B.V.', 'Daarnhouwer & Co B.V.', 'THE GOLDMAN SACHS GROUP, INC.', 'HSBC HOLDINGS PLC')
# MAGIC -- and t2.SectorTeam not in ('ARG')
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- and t2.EDL_LoadDate = '2025-07-01'
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct t2.SourceClient) 
# MAGIC from radar.clients t2
# MAGIC left join radar.cases t1 on t1.sourceclient = t2.sourceclient
# MAGIC
# MAGIC
# MAGIC where t2.GlobalReportingRegion = 'E&A'
# MAGIC and t2.ClientLifeCycleName = 'Client'
# MAGIC and t2.GlobalKYCPortfolioNew not in ('London Markets')
# MAGIC and t2.SectorTeam not in ('Acorn', 'Agency', 'Correspondent Banking', 'FIG - Orphan Desk', 'FIG', 'Foundation', 'Markets', 'PSP Coverage', 'Rabo Frontier Ventures', 'RCI', 'Subsidiaries', 'Treasury', 'ARG', 'TCF FI')
# MAGIC and t2.FullLegalName not in ('Kadans Science Partners II B.V.', 'Daarnhouwer & Co B.V.', 'The Goldman Sachs Group, Inc.', 'HSBC HOLDINGS PLC')
# MAGIC
# MAGIC
# MAGIC and t1.CaseReviewType = 'Periodic Review'
# MAGIC and t1.casephase2025 = 'Not yet started'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct t2.SourceClient) 
# MAGIC from radar.clients t2
# MAGIC left join radar.cases t1 on t1.sourceclient = t2.sourceclient
# MAGIC
# MAGIC
# MAGIC where t2.GlobalReportingRegion = 'E&A'
# MAGIC and t2.ClientLifeCycleName = 'Client'
# MAGIC and t2.GlobalKYCPortfolioNew not in ('London Markets')
# MAGIC and t2.SectorTeam not in ('Acorn', 'Agency', 'Correspondent Banking', 'FIG - Orphan Desk', 'FIG', 'Foundation', 'Markets', 'PSP Coverage', 'Rabo Frontier Ventures', 'RCI', 'Subsidiaries', 'Treasury', 'ARG', 'TCF FI')
# MAGIC and t2.FullLegalName not in ('Kadans Science Partners II B.V.', 'Daarnhouwer & Co B.V.', 'The Goldman Sachs Group, Inc.', 'HSBC HOLDINGS PLC')
# MAGIC
# MAGIC
# MAGIC and t1.CaseReviewType = 'Periodic Review'
# MAGIC and t1.Casephase = 'Completed'
# MAGIC and year(t1.NextReviewDate) = 2025

# COMMAND ----------

# DBTITLE 1,edr overdue
# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.EDRDuedate
# MAGIC   , t2.EDRoverdue
# MAGIC
# MAGIC , CASE
# MAGIC       WHEN t2.CaseStatusName NOT IN ('Cancelled', 'Completed') AND CURRENT_DATE() > TO_DATE(ADD_MONTHS(t2.Prework, 3), 'yyyy-MM-dd') -- 'Migrated'
# MAGIC         AND t2.CaseReviewType = 'Event Driven Review' THEN 'Yes' ELSE 'No'
# MAGIC   END AS EDROverdue_NEW
# MAGIC
# MAGIC from radar.clients t1
# MAGIC inner join radar.cases t2 on t1.sourceclient = t2.SourceClient
# MAGIC
# MAGIC where 1=1
# MAGIC -- t2.EDRoverdue = 'Yes'
# MAGIC and month(t1.EDRDuedate) = 7
# MAGIC and year(t1.EDRDuedate) = 2025
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- , CASE
# MAGIC --       WHEN t2.CaseStatusName NOT IN ('Cancelled', 'Completed') AND CURRENT_DATE() >= DATE_ADD(t1.Prework, 90) -- 'Migrated'
# MAGIC --         AND t2.ReviewTypeName = 'Event Driven Review' THEN 'Yes' ELSE 'No'
# MAGIC --     END AS EDROverdue
# MAGIC
# MAGIC
# MAGIC -- , CASE
# MAGIC --       WHEN t2.ReviewTypeName = 'Event Driven Review' THEN TO_DATE(ADD_MONTHS(t2.Prework, 3), 'yyyy-MM-dd')
# MAGIC --       ELSE NULL
# MAGIC --     END AS EDRDuedate

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,exchange rates
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

from pyspark.sql import functions as F
import re

columns_to_load = [
    'PriceCurrency', 
    'Close_2100CET', 
    'BaseCurrency', 
    'RateDate'
]

path = f'abfss://timescape@edlcorestdeuprod0001.dfs.core.windows.net/fx_rates_2100cet/'
files = dbutils.fs.ls(path)
version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

path_file = f'{path}{version}/data/'
files = dbutils.fs.ls(path_file)
load_date = max(file.path.split('RATES_DT=')[1][:8] for file in files if 'RATES_DT=' in file.path)

df = spark.read.parquet(f'{path_file}/RATES_DT={load_date}/*.parquet').select(*columns_to_load) # .filter(F.col('BaseCurrency') == 'EUR')

# rename pricecurrency to currency_codes
df = df.withColumnRenamed('PriceCurrency', 'currency_codes')

df.createOrReplaceTempView('exchange_rates')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.cases where uniquegcobid = 23896 and caseid = 113170

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   
# MAGIC   COUNT(DISTINCT t1.SourceClient)
# MAGIC   -- , t1.KYCDepartment
# MAGIC   -- , t1.EDL_LoadDate
# MAGIC
# MAGIC FROM radar.cases_historical t1
# MAGIC INNER JOIN radar.clients_historical t2 ON t1.SourceClient = t2.SourceClient
# MAGIC WHERE
# MAGIC   YEAR(t1.Completed) = 2025
# MAGIC   AND t1.Completed IS NOT NULL
# MAGIC   AND YEAR(t1.EDL_LoadDate)  = YEAR(GETDATE())
# MAGIC   AND MONTH(t1.EDL_LoadDate) = MONTH(GETDATE())
# MAGIC   AND t1.KYCDepartment IN ('Corp CDD Hub NL', 'Corp CDD Hub UK')
# MAGIC   AND t1.CaseReviewType IN ('Event Driven Review', 'Initial On-Boarding', 'Periodic Review')
# MAGIC   AND t2.Scope = 'backToGreen'
# MAGIC   AND t2.SectorTeam <> 'ARG'
# MAGIC
# MAGIC   -- AND t1.FullLegalName NOT IN ('Kadans Science Partner II B.V.', 'Daarnhouwer & Co B.V.', 'HSBC HOLDINGS PLC', 'THE GOLDMAN SACHS GROUP, INC.')
# MAGIC
# MAGIC -- GROUP BY 2,3

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC UniqueGcobId, 
# MAGIC -- FullLegalName, 
# MAGIC -- BusinessLineName, GlobalBusinessLine, SectorTeam, EDL_LoadDate, GlobalReportingRegion,
# MAGIC SectorTeam,
# MAGIC FIHubIndicator_Derived
# MAGIC -- ClientLifeCycleName, GlobalClientOwnerLocation
# MAGIC
# MAGIC -- count(distinct UniqueGcobId)
# MAGIC
# MAGIC -- SectorTeam
# MAGIC -- , count(distinct UniqueGcobId)
# MAGIC
# MAGIC from radar.siraclients_historical
# MAGIC
# MAGIC where 1=1
# MAGIC
# MAGIC -- and FIHubIndicator_Derived is not null
# MAGIC
# MAGIC and ClientLifeCycleName = 'Client'
# MAGIC and GlobalReportingRegion = 'E&A'
# MAGIC -- and FIHubIndicator_Derived = 'Corp'
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- and GlobalClientOwnerLocation not in ('Rabobank - RANZ Country Banking and ROS', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)', 'Rabobank Foundation')
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- and BusinessLineName not in ('RANZ Country Banking')
# MAGIC
# MAGIC -- and SectorTeam not in ('Acorn', 'Foundation', 'PSP Coverage') -- 'Screening'
# MAGIC
# MAGIC
# MAGIC -- and UniqueGcobId = 'NP_790' 
# MAGIC and EDL_LoadDate = '2025-04-01'
# MAGIC
# MAGIC
# MAGIC
# MAGIC and UniqueGcobId in (
# MAGIC   108529,
# MAGIC   109649,
# MAGIC   110863,
# MAGIC   36935,
# MAGIC   39091,
# MAGIC   40457,
# MAGIC   43068,
# MAGIC   48294,
# MAGIC   48411,
# MAGIC   5689,
# MAGIC   7755,
# MAGIC   786
# MAGIC )
# MAGIC
# MAGIC
# MAGIC -- group by SectorTeam
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC UniqueGcobId, 
# MAGIC -- FullLegalName, 
# MAGIC -- BusinessLineName, GlobalBusinessLine, SectorTeam, EDL_LoadDate, GlobalReportingRegion,
# MAGIC SectorTeam,
# MAGIC FIHubIndicator_Derived
# MAGIC -- ClientLifeCycleName, GlobalClientOwnerLocation
# MAGIC
# MAGIC -- count(distinct UniqueGcobId)
# MAGIC
# MAGIC -- SectorTeam
# MAGIC -- , count(distinct UniqueGcobId)
# MAGIC
# MAGIC from radar.siraclients_historical
# MAGIC
# MAGIC where 1=1
# MAGIC
# MAGIC -- and FIHubIndicator_Derived is not null
# MAGIC
# MAGIC and ClientLifeCycleName = 'Client'
# MAGIC and GlobalReportingRegion = 'E&A'
# MAGIC -- and FIHubIndicator_Derived = 'Corp'
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- and GlobalClientOwnerLocation not in ('Rabobank - RANZ Country Banking and ROS', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)', 'Rabobank Foundation')
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- and BusinessLineName not in ('RANZ Country Banking')
# MAGIC
# MAGIC -- and SectorTeam not in ('Acorn', 'Foundation', 'PSP Coverage') -- 'Screening'
# MAGIC
# MAGIC
# MAGIC -- and UniqueGcobId = 'NP_790' 
# MAGIC and EDL_LoadDate = '2025-04-01'
# MAGIC
# MAGIC
# MAGIC
# MAGIC and UniqueGcobId in (
# MAGIC   'NP_1302',
# MAGIC   'NP_1404',
# MAGIC   'NP_1405',
# MAGIC   'NP_1715',
# MAGIC   'NP_1716',
# MAGIC   'NP_2171',
# MAGIC   'NP_2172',
# MAGIC   'NP_2173',
# MAGIC   'NP_2174',
# MAGIC   'NP_2175',
# MAGIC   'NP_5224',
# MAGIC   'NP_667',
# MAGIC   'NP_790',
# MAGIC   'NP_950'
# MAGIC )
# MAGIC
# MAGIC
# MAGIC -- group by SectorTeam
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select UniqueGcobId, FIHubIndicator_Derived from radar.clients where UniqueGcobId like 'NP_%'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - NP are not included because the FIHubIndicator is null --> include them in the filters
# MAGIC - LE are not included because 5 are FI, and the rest are PSP and Foundation as sector team --> to be verified

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH rn_cte AS (
# MAGIC   SELECT
# MAGIC     t1.UniqueGcobId
# MAGIC     , t2.SignOffDate
# MAGIC     , t2.Prework
# MAGIC     , t2.ClientId
# MAGIC     , ROW_NUMBER() OVER (PARTITION BY t1.UniqueGcobId ORDER BY t1.clientid DESC) AS rn
# MAGIC   FROM radar.clients t1
# MAGIC   LEFT JOIN radar.cases t2 ON t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC )
# MAGIC SELECT
# MAGIC   UniqueGcobId
# MAGIC   , SignOffDate
# MAGIC   , Prework
# MAGIC   , ClientId
# MAGIC FROM rn_cte
# MAGIC WHERE rn = 2
# MAGIC
# MAGIC and UniqueGcobId = 5306

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH rn_cte AS (
# MAGIC   SELECT
# MAGIC     t2.UniqueGcobId
# MAGIC     , t2.SignOffDate
# MAGIC     , t2.Signoff
# MAGIC     , t2.Prework
# MAGIC     , t2.ClientId
# MAGIC     , ROW_NUMBER() OVER (PARTITION BY t1.UniqueGcobId ORDER BY CAST(t2.ClientId AS INT) DESC) AS rn
# MAGIC   FROM radar.clients t1
# MAGIC   LEFT JOIN radar.cases t2 ON t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   UniqueGcobId
# MAGIC   , SignOffDate AS PreviousCaseSignOffDate --> needs to be fixed, once it is fixed you should use this one instead of the one below
# MAGIC   -- , Signoff AS PreviousCaseSignOffDate -- this is not the best to use but until we have the above fixed we have to use this
# MAGIC   , Prework AS PreviousCasePrework
# MAGIC   , ClientId AS PreviousCaseId
# MAGIC FROM rn_cte
# MAGIC -- WHERE rn = 2
# MAGIC where UniqueGcobId = 10030
# MAGIC ORDER BY UniqueGcobId

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC
# MAGIC   t1.uniquegcobid as GCOBID
# MAGIC   , t1.FullLegalName as ClientName
# MAGIC   , t1.LatestCaseId as CaseId
# MAGIC   , t1.GlobalClientOwner
# MAGIC   , t1.GlobalClientOwnerLocation
# MAGIC   , t1.ClientLifeCycleName as LifecycleStatus
# MAGIC   , t2.IsUbo
# MAGIC   , t2.UboReason
# MAGIC   , t2.Identity
# MAGIC   , t2.IsPEP
# MAGIC   , t2.PEPStatus
# MAGIC   , t2.PEPNationality
# MAGIC   , t2.ResidentialAddressCountry
# MAGIC   -- , t2.Identity
# MAGIC   -- , t2.IsPEP
# MAGIC   -- , t2.IsUbo
# MAGIC   
# MAGIC
# MAGIC
# MAGIC from radar.clients_historical t1
# MAGIC left join radar.sirapepubo_historical t2 on t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC
# MAGIC where t1.EDL_LoadDate = '2025-01-01'
# MAGIC and t1.ClientLifeCycleName = 'Client'
# MAGIC and t1.GlobalClientOwnerLocation = 'Rabobank Madrid'
# MAGIC
# MAGIC and t2.IsPEP = 'PEP'
# MAGIC and t2.EDL_LoadDate = '2025-01-01'
# MAGIC -- and t1.UniqueGcobId = 2707

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC
# MAGIC   t1.uniquegcobid as GCOBID
# MAGIC   , t1.FullLegalName as ClientName
# MAGIC   , t1.LatestCaseId as CaseId
# MAGIC   , t1.KYCGroup
# MAGIC   , t1.GlobalClientOwner
# MAGIC   , t1.GlobalClientOwnerLocation
# MAGIC   , t1.ClientLifeCycleName as LifecycleStatus
# MAGIC   , t1.ValidatedRiskLevel
# MAGIC   , t2.CaseReviewType
# MAGIC   , t1.NextReviewDate
# MAGIC   , t1.SignOffDate
# MAGIC   , t2.CountryOfRegistration
# MAGIC   -- , t2.IsUbo
# MAGIC   -- , t2.UboReason
# MAGIC   -- , t2.Identity
# MAGIC   -- , t2.IsPEP
# MAGIC   -- , t2.PEPStatus
# MAGIC   -- , t2.PEPNationality
# MAGIC   -- , t2.ResidentialAddressCountry
# MAGIC   -- , t2.Identity
# MAGIC   -- , t2.IsPEP
# MAGIC   -- , t2.IsUbo
# MAGIC   
# MAGIC
# MAGIC
# MAGIC from radar.clients_historical t1
# MAGIC left join radar.cases_historical t2 on t1.UniqueGcobId = t2.UniqueGcobId and t1.LatestCaseId = t2.caseid
# MAGIC -- left join radar.sirapepubo_historical t2 on t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC
# MAGIC where t1.EDL_LoadDate = '2025-01-01'
# MAGIC
# MAGIC and t1.ClientLifeCycleName = 'Client'
# MAGIC and t1.GlobalClientOwnerLocation = 'Rabobank Madrid'
# MAGIC -- and t2.CountryOfRegistration = 'Spain'
# MAGIC
# MAGIC -- and t2.IsPEP = 'PEP'
# MAGIC and t2.EDL_LoadDate = '2025-01-01'
# MAGIC -- and t1.UniqueGcobId = 2707

# COMMAND ----------

# MAGIC %md
# MAGIC # PID on time - historical dataset

# COMMAND ----------

# DBTITLE 1,casescount
# MAGIC %sql
# MAGIC SELECT
# MAGIC   MONTH(ClientCaseInitiationStart) AS Month
# MAGIC   , COUNT(DISTINCT UniqueGcobId) AS CasesCount
# MAGIC FROM radar.clients_historical
# MAGIC WHERE
# MAGIC   EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1)
# MAGIC   AND YEAR(ClientCaseInitiationStart) = YEAR(CURRENT_DATE)
# MAGIC   AND Scope = 'backToGreen'
# MAGIC   AND ClientLifeCycleName = 'Client'
# MAGIC GROUP BY MONTH(ClientCaseInitiationStart)
# MAGIC ORDER BY MONTH(ClientCaseInitiationStart) ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pid_on_time_overview_historical AS
# MAGIC
# MAGIC -- EXCLUDE CLIENTS WITH OFFBOARDING AS THE LATEST CASE REVIEW TYPE
# MAGIC WITH offboarded_cte AS (
# MAGIC   WITH max_cases_cte AS (
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Offboarding' THEN CaseId END) AS max_offboarding_id
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Periodic Review' THEN CaseId END) AS max_periodic_id
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType IN ('Offboarding','Periodic Review')
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   , max_periodic_prework AS (
# MAGIC     -- get the latest prework date for each periodic review
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(Prework) AS max_prework
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType = 'Periodic Review'
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   SELECT DISTINCT
# MAGIC     t1.UniqueGcobId
# MAGIC
# MAGIC   FROM radar.clients_historical t1
# MAGIC   JOIN max_cases_cte t2 ON t2.UniqueGcobId = t1.UniqueGcobId
# MAGIC     -- only keep those whose max caseid for offboarding > max caseid for periodic review
# MAGIC     AND t2.max_offboarding_id > t2.max_periodic_id
# MAGIC   LEFT JOIN max_periodic_prework t3 ON t1.UniqueGcobId = t3.UniqueGcobId
# MAGIC
# MAGIC   WHERE t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1) -- 2025-01-01
# MAGIC     AND YEAR(t1.ClientCaseInitiationStart) = YEAR(CURRENT_DATE) -- 2025
# MAGIC     AND t1.Scope = 'backToGreen'
# MAGIC     AND t1.ClientLifeCycleName = 'Client'
# MAGIC     AND MONTH(t1.ClientCaseInitiationStart) BETWEEN MONTH(make_date(YEAR(CURRENT_DATE), 1, 1)) AND MONTH(DATEADD(MONTH,-1,(SELECT MAX(edl_loaddate) FROM radar.clients_historical))) -- PID after or equal to 2025-01-01 AND less or equal to current month
# MAGIC
# MAGIC     -- only if the year(max(t3.Prework)) <> YEAR(CURRENT_DATE) --> exclude the clients that actually had a PR in the current year
# MAGIC     AND t3.max_prework IS NOT NULL
# MAGIC     AND YEAR(t3.max_prework) <> YEAR(CURRENT_DATE)
# MAGIC )
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SourceClient
# MAGIC   , t1.ClientCaseInitiationStart AS ClientCaseInitiationStart_old
# MAGIC   , MAX(t2.SignOff) AS SignOff_new
# MAGIC   , MAX(t2.Prework) AS Prework_new
# MAGIC   , datediff(t1.ClientCaseInitiationStart, MAX(t2.Prework)) as DateDiff_PID_Prework
# MAGIC   , t3.ClientCaseInitiationStart AS ClientCaseInitiationStart_new
# MAGIC   , t1.EDL_LoadDate
# MAGIC   -- , t2.CaseReviewType
# MAGIC   -- , t2.CaseId
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t1.UniqueGcobId IN (SELECT * FROM offboarded_cte) THEN 'Offboarding'
# MAGIC
# MAGIC       WHEN DATEADD(DAY, -1, MAX(t2.Prework)) > t1.ClientCaseInitiationStart THEN 'Not on time' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC       WHEN MAX(t2.Prework) IS NULL THEN 'Not on time' -- DOUBLE CHECK THIS, MIGHT BE OFFBOARDING CASES!!!!
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.Prework)) < t1.ClientCaseInitiationStart)
# MAGIC         AND (t1.ClientCaseInitiationStart <> t3.ClientCaseInitiationStart)
# MAGIC         AND (MAX(t2.SignOff) IS NOT NULL AND MAX(t2.SignOff) < t1.ClientCaseInitiationStart)
# MAGIC       THEN 'On time' -- or rescheduled' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.Prework)) < t1.ClientCaseInitiationStart)
# MAGIC         AND (MAX(t2.SignOff) IS NULL OR MAX(t2.SignOff) > t1.ClientCaseInitiationStart)
# MAGIC       THEN 'On time'
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.Prework)) < t1.ClientCaseInitiationStart)
# MAGIC         AND (DATEADD(DAY, -1, MAX(t2.Prework)) > MAX(t2.SignOff))
# MAGIC       THEN 'On time'
# MAGIC
# MAGIC       ELSE 'Not on time' -- - to be verified'
# MAGIC
# MAGIC     END AS OnTime
# MAGIC
# MAGIC FROM radar.clients_historical t1
# MAGIC LEFT JOIN radar.cases_historical t2 ON t1.UniqueGcobId = t2.UniqueGcobId AND t2.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND t2.CaseReviewType in ('Periodic Review', 'Offboarding')
# MAGIC LEFT JOIN radar.clients_historical t3 ON t1.UniqueGcobId = t3.UniqueGcobId AND t3.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical)
# MAGIC
# MAGIC WHERE t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1) -- 2025-01-01
# MAGIC   AND YEAR(t1.ClientCaseInitiationStart) = YEAR(CURRENT_DATE) -- 2025
# MAGIC   AND t1.Scope = 'backToGreen'
# MAGIC   AND t1.ClientLifeCycleName = 'Client'
# MAGIC   AND MONTH(t1.ClientCaseInitiationStart) BETWEEN MONTH(make_date(YEAR(CURRENT_DATE), 1, 1)) AND MONTH(DATEADD(MONTH,-1,(SELECT MAX(edl_loaddate) FROM radar.clients_historical))) -- PID after or equal to 2025-01-01 AND less or equal to current month
# MAGIC
# MAGIC GROUP BY 1,2,3,7,8

# COMMAND ----------

# MAGIC %sql
# MAGIC select UniqueGcobId
# MAGIC   , SourceClient
# MAGIC   -- , ClientCaseInitiationStart
# MAGIC   , SignOff AS SignOff
# MAGIC   , Prework AS Prework
# MAGIC   , CaseReviewType  
# MAGIC   
# MAGIC from radar.cases where UniqueGcobId = 4934
# MAGIC order by Prework desc

# COMMAND ----------

# DBTITLE 1,pid_on_time_historical
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pid_on_time_historical AS
# MAGIC
# MAGIC -- EXCLUDE CLIENTS WITH OFFBOARDING AS THE LATEST CASE REVIEW TYPE
# MAGIC WITH offboarded_cte AS (
# MAGIC   WITH max_cases_cte AS (
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Offboarding' THEN CaseId END) AS max_offboarding_id
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Periodic Review' THEN CaseId END) AS max_periodic_id
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType IN ('Offboarding','Periodic Review')
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   , max_periodic_prework AS (
# MAGIC     -- get the latest prework date for each periodic review
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(Prework) AS max_prework
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType = 'Periodic Review'
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   SELECT DISTINCT
# MAGIC     t1.UniqueGcobId
# MAGIC
# MAGIC   FROM radar.clients_historical t1
# MAGIC   JOIN max_cases_cte t2 ON t2.UniqueGcobId = t1.UniqueGcobId
# MAGIC     -- only keep those whose max caseid for offboarding > max caseid for periodic review
# MAGIC     AND t2.max_offboarding_id > t2.max_periodic_id
# MAGIC   LEFT JOIN max_periodic_prework t3 ON t1.UniqueGcobId = t3.UniqueGcobId
# MAGIC
# MAGIC   WHERE t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1) -- 2025-01-01
# MAGIC     AND YEAR(t1.ClientCaseInitiationStart) = YEAR(CURRENT_DATE) -- 2025
# MAGIC     AND t1.Scope = 'backToGreen'
# MAGIC     AND t1.ClientLifeCycleName = 'Client'
# MAGIC     AND MONTH(t1.ClientCaseInitiationStart) BETWEEN MONTH(make_date(YEAR(CURRENT_DATE), 1, 1)) AND MONTH(DATEADD(MONTH,-1,(SELECT MAX(edl_loaddate) FROM radar.clients_historical))) -- PID after or equal to 2025-01-01 AND less or equal to current month
# MAGIC
# MAGIC     -- only if the year(max(t3.Prework)) <> YEAR(CURRENT_DATE) --> exclude the clients that actually had a PR in the current year
# MAGIC     AND t3.max_prework IS NOT NULL
# MAGIC     AND YEAR(t3.max_prework) <> YEAR(CURRENT_DATE)
# MAGIC )
# MAGIC
# MAGIC , pid_on_time_overview_cte AS (
# MAGIC     SELECT
# MAGIC       t1.UniqueGcobId
# MAGIC       , t1.SourceClient
# MAGIC       , t1.ClientCaseInitiationStart AS ClientCaseInitiationStart_old
# MAGIC       , MAX(t2.SignOff) AS SignOff_new
# MAGIC       , MAX(t2.Prework) AS Prework_new
# MAGIC       , datediff(t1.ClientCaseInitiationStart, MAX(t2.Prework)) as DateDiff_PID_Prework
# MAGIC       , t3.ClientCaseInitiationStart AS ClientCaseInitiationStart_new
# MAGIC
# MAGIC       , CASE
# MAGIC           WHEN t1.UniqueGcobId IN (SELECT * FROM offboarded_cte) THEN 'Offboarding'
# MAGIC
# MAGIC           WHEN DATEADD(DAY, -1, MAX(t2.Prework)) > t1.ClientCaseInitiationStart THEN 'Not on time' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC           WHEN MAX(t2.Prework) IS NULL THEN 'Not on time' -- DOUBLE CHECK THIS, MIGHT BE OFFBOARDING CASES!!!!
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.Prework)) < t1.ClientCaseInitiationStart)
# MAGIC             AND (t1.ClientCaseInitiationStart <> t3.ClientCaseInitiationStart)
# MAGIC             AND (MAX(t2.SignOff) IS NOT NULL AND MAX(t2.SignOff) < t1.ClientCaseInitiationStart)
# MAGIC           THEN 'On time' -- or rescheduled' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.Prework)) < t1.ClientCaseInitiationStart)
# MAGIC             AND (MAX(t2.SignOff) IS NULL OR MAX(t2.SignOff) > t1.ClientCaseInitiationStart)
# MAGIC           THEN 'On time'
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.Prework)) < t1.ClientCaseInitiationStart)
# MAGIC             AND (DATEADD(DAY, -1, MAX(t2.Prework)) > MAX(t2.SignOff))
# MAGIC           THEN 'On time'
# MAGIC
# MAGIC           ELSE 'Not on time' -- - to be verified'
# MAGIC
# MAGIC         END AS OnTime
# MAGIC
# MAGIC     FROM radar.clients_historical t1
# MAGIC     LEFT JOIN radar.cases_historical t2 ON t1.UniqueGcobId = t2.UniqueGcobId AND t2.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND t2.CaseReviewType in ('Periodic Review') --, 'Offboarding')
# MAGIC     LEFT JOIN radar.clients_historical t3 ON t1.UniqueGcobId = t3.UniqueGcobId AND t3.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical)
# MAGIC
# MAGIC     WHERE
# MAGIC         t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1)
# MAGIC         AND YEAR(t1.ClientCaseInitiationStart) = YEAR(CURRENT_DATE)
# MAGIC         AND t1.Scope = 'backToGreen'
# MAGIC         AND t1.ClientLifeCycleName = 'Client'
# MAGIC     GROUP BY 1,2,3,7
# MAGIC   )
# MAGIC
# MAGIC SELECT 
# MAGIC   MONTH(ClientCaseInitiationStart_old) AS Month
# MAGIC   , COUNT(DISTINCT UniqueGcobId) AS CasesCount
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN MONTH(ClientCaseInitiationStart_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC         AND MONTH(ClientCaseInitiationStart_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC       THEN COUNT(DISTINCT CASE 
# MAGIC         WHEN OnTime IN ('On time') 
# MAGIC         THEN UniqueGcobId 
# MAGIC         ELSE NULL 
# MAGIC       END)
# MAGIC       ELSE NULL
# MAGIC     END AS CasesOnTimeCount
# MAGIC
# MAGIC   -- , CASE 
# MAGIC   --     WHEN MONTH(ClientCaseInitiationStart_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC   --       AND MONTH(ClientCaseInitiationStart_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC   --     THEN COUNT(DISTINCT CASE 
# MAGIC   --       WHEN OnTime IN ('On time or rescheduled') 
# MAGIC   --       THEN UniqueGcobId 
# MAGIC   --       ELSE NULL 
# MAGIC   --     END)
# MAGIC   --     ELSE NULL
# MAGIC   --   END AS CasesOnTimeOrRescheduledCount
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN MONTH(ClientCaseInitiationStart_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC         AND MONTH(ClientCaseInitiationStart_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC       THEN COUNT(DISTINCT CASE 
# MAGIC         WHEN OnTime IN ('Not on time') 
# MAGIC         THEN UniqueGcobId 
# MAGIC         ELSE NULL 
# MAGIC       END)
# MAGIC       ELSE NULL
# MAGIC     END AS CasesNotOnTimeCount
# MAGIC
# MAGIC   -- , CASE 
# MAGIC   --     WHEN MONTH(ClientCaseInitiationStart_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC   --       AND MONTH(ClientCaseInitiationStart_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC   --     THEN COUNT(DISTINCT CASE 
# MAGIC   --       WHEN OnTime IN ('Not on time - to be verified') 
# MAGIC   --       THEN UniqueGcobId 
# MAGIC   --       ELSE NULL 
# MAGIC   --     END)
# MAGIC   --     ELSE NULL
# MAGIC   --   END AS CasesNotOnTimeToBeVerifiedCount
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN MONTH(ClientCaseInitiationStart_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC         AND MONTH(ClientCaseInitiationStart_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC       THEN COUNT(DISTINCT CASE 
# MAGIC         WHEN OnTime IN ('Offboarding') 
# MAGIC         THEN UniqueGcobId 
# MAGIC         ELSE NULL 
# MAGIC       END)
# MAGIC       ELSE NULL
# MAGIC     END AS CasesOffboardedCount
# MAGIC
# MAGIC FROM pid_on_time_overview_cte
# MAGIC GROUP BY MONTH(ClientCaseInitiationStart_old)
# MAGIC ORDER BY MONTH(ClientCaseInitiationStart_old) ASC

# COMMAND ----------

# MAGIC %md
# MAGIC # PAD on time - historical dataset

# COMMAND ----------

# DBTITLE 1,casescount
# MAGIC %sql
# MAGIC SELECT
# MAGIC   MONTH(CDDExecution) AS Month
# MAGIC   , COUNT(DISTINCT UniqueGcobId) AS CasesCount
# MAGIC FROM radar.clients_historical
# MAGIC WHERE
# MAGIC   EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1)
# MAGIC   AND YEAR(CDDExecution) = YEAR(CURRENT_DATE)
# MAGIC   AND Scope = 'backToGreen'
# MAGIC   AND ClientLifeCycleName = 'Client'
# MAGIC GROUP BY MONTH(CDDExecution)
# MAGIC ORDER BY MONTH(CDDExecution) ASC

# COMMAND ----------

# DBTITLE 1,pad_on_time_overview_historical
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pad_on_time_overview_historical AS
# MAGIC
# MAGIC -- EXCLUDE CLIENTS WITH OFFBOARDING AS THE LATEST CASE REVIEW TYPE
# MAGIC WITH offboarded_cte AS (
# MAGIC   WITH max_cases_cte AS (
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Offboarding' THEN CaseId END) AS max_offboarding_id
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Periodic Review' THEN CaseId END) AS max_periodic_id
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType IN ('Offboarding','Periodic Review')
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   , max_periodic_prework AS (
# MAGIC     -- get the latest prework date for each periodic review
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(Prework) AS max_prework
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType = 'Periodic Review'
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   SELECT DISTINCT
# MAGIC     t1.UniqueGcobId
# MAGIC
# MAGIC   FROM radar.clients_historical t1
# MAGIC   JOIN max_cases_cte t2 ON t2.UniqueGcobId = t1.UniqueGcobId
# MAGIC     -- only keep those whose max caseid for offboarding > max caseid for periodic review
# MAGIC     AND t2.max_offboarding_id > t2.max_periodic_id
# MAGIC   LEFT JOIN max_periodic_prework t3 ON t1.UniqueGcobId = t3.UniqueGcobId
# MAGIC
# MAGIC   WHERE t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1) -- 2025-01-01
# MAGIC     AND YEAR(t1.ClientCaseInitiationStart) = YEAR(CURRENT_DATE) -- 2025
# MAGIC     AND t1.Scope = 'backToGreen'
# MAGIC     AND t1.ClientLifeCycleName = 'Client'
# MAGIC     AND MONTH(t1.ClientCaseInitiationStart) BETWEEN MONTH(make_date(YEAR(CURRENT_DATE), 1, 1)) AND MONTH(DATEADD(MONTH,-1,(SELECT MAX(edl_loaddate) FROM radar.clients_historical))) -- PID after or equal to 2025-01-01 AND less or equal to current month
# MAGIC
# MAGIC     -- only if the year(max(t3.Prework)) <> YEAR(CURRENT_DATE) --> exclude the clients that actually had a PR in the current year
# MAGIC     AND t3.max_prework IS NOT NULL
# MAGIC     AND YEAR(t3.max_prework) <> YEAR(CURRENT_DATE)
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SourceClient
# MAGIC   , t1.CDDExecution AS CDDExecution_old
# MAGIC   , MAX(t2.SignOff) AS SignOff_new
# MAGIC   , MAX(t2.ReadyForAssessment) AS ReadyForAssessment_new
# MAGIC   , datediff(t1.CDDExecution, MAX(t2.ReadyForAssessment)) as DateDiff_CDDExecution_ReadyForAssessment
# MAGIC   , t3.CDDExecution AS CDDExecution_new
# MAGIC   , t1.EDL_LoadDate
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t1.UniqueGcobId IN (SELECT * FROM offboarded_cte) THEN 'Offboarding'
# MAGIC
# MAGIC       WHEN DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) > t1.CDDExecution THEN 'Not on time' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC       WHEN MAX(t2.ReadyForAssessment) IS NULL THEN 'Not on time' -- DOUBLE CHECK THIS, MIGHT BE OFFBOARDING CASES!!!!
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC         AND (t1.CDDExecution <> t3.CDDExecution)
# MAGIC         AND (MAX(t2.SignOff) IS NOT NULL AND MAX(t2.SignOff) < t1.CDDExecution)
# MAGIC       THEN 'On time' -- or rescheduled' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC         AND (MAX(t2.SignOff) IS NULL OR MAX(t2.SignOff) > t1.CDDExecution)
# MAGIC       THEN 'On time'
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC         AND (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) > MAX(t2.SignOff))
# MAGIC       THEN 'On time'
# MAGIC
# MAGIC       ELSE 'Not on time' -- - to be verified'
# MAGIC
# MAGIC     END AS OnTime
# MAGIC
# MAGIC FROM radar.clients_historical t1
# MAGIC LEFT JOIN radar.cases_historical t2 ON t1.UniqueGcobId = t2.UniqueGcobId AND t2.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND t2.CaseReviewType in ('Periodic Review') --, 'Offboarding')
# MAGIC LEFT JOIN radar.clients_historical t3 ON t1.UniqueGcobId = t3.UniqueGcobId AND t3.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical)
# MAGIC
# MAGIC WHERE
# MAGIC     t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1)
# MAGIC     AND YEAR(t1.CDDExecution) = YEAR(CURRENT_DATE)
# MAGIC     AND t1.Scope = 'backToGreen'
# MAGIC     AND t1.ClientLifeCycleName = 'Client'
# MAGIC     AND MONTH(t1.CDDExecution) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC     AND MONTH(t1.CDDExecution) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC
# MAGIC GROUP BY 1,2,3,7,8

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pad_on_time_historical AS
# MAGIC
# MAGIC -- EXCLUDE CLIENTS WITH OFFBOARDING AS THE LATEST CASE REVIEW TYPE
# MAGIC WITH offboarded_cte AS (
# MAGIC   WITH max_cases_cte AS (
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Offboarding' THEN CaseId END) AS max_offboarding_id
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Periodic Review' THEN CaseId END) AS max_periodic_id
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType IN ('Offboarding','Periodic Review')
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   , max_periodic_prework AS (
# MAGIC     -- get the latest prework date for each periodic review
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(Prework) AS max_prework
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType = 'Periodic Review'
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   SELECT DISTINCT
# MAGIC     t1.UniqueGcobId
# MAGIC
# MAGIC   FROM radar.clients_historical t1
# MAGIC   JOIN max_cases_cte t2 ON t2.UniqueGcobId = t1.UniqueGcobId
# MAGIC     -- only keep those whose max caseid for offboarding > max caseid for periodic review
# MAGIC     AND t2.max_offboarding_id > t2.max_periodic_id
# MAGIC   LEFT JOIN max_periodic_prework t3 ON t1.UniqueGcobId = t3.UniqueGcobId
# MAGIC
# MAGIC   WHERE t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1) -- 2025-01-01
# MAGIC     AND YEAR(t1.ClientCaseInitiationStart) = YEAR(CURRENT_DATE) -- 2025
# MAGIC     AND t1.Scope = 'backToGreen'
# MAGIC     AND t1.ClientLifeCycleName = 'Client'
# MAGIC     AND MONTH(t1.ClientCaseInitiationStart) BETWEEN MONTH(make_date(YEAR(CURRENT_DATE), 1, 1)) AND MONTH(DATEADD(MONTH,-1,(SELECT MAX(edl_loaddate) FROM radar.clients_historical))) -- PID after or equal to 2025-01-01 AND less or equal to current month
# MAGIC
# MAGIC     -- only if the year(max(t3.Prework)) <> YEAR(CURRENT_DATE) --> exclude the clients that actually had a PR in the current year
# MAGIC     AND t3.max_prework IS NOT NULL
# MAGIC     AND YEAR(t3.max_prework) <> YEAR(CURRENT_DATE)
# MAGIC )
# MAGIC
# MAGIC , pad_on_time_overview_cte AS (
# MAGIC     SELECT
# MAGIC       t1.UniqueGcobId
# MAGIC       , t1.SourceClient
# MAGIC       , t1.CDDExecution AS CDDExecution_old
# MAGIC       , MAX(t2.SignOff) AS SignOff_new
# MAGIC       , MAX(t2.ReadyForAssessment) AS ReadyForAssessment_new
# MAGIC       , datediff(t1.CDDExecution, MAX(t2.ReadyForAssessment)) as DateDiff_CDDExecution_ReadyForAssessment
# MAGIC       , t3.CDDExecution AS CDDExecution_new
# MAGIC
# MAGIC       , CASE
# MAGIC           WHEN t1.UniqueGcobId IN (SELECT * FROM offboarded_cte) THEN 'Offboarding'
# MAGIC
# MAGIC           WHEN DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) > t1.CDDExecution THEN 'Not on time' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC           WHEN MAX(t2.ReadyForAssessment) IS NULL THEN 'Not on time' -- DOUBLE CHECK THIS, MIGHT BE OFFBOARDING CASES!!!!
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC             AND (t1.CDDExecution <> t3.CDDExecution)
# MAGIC             AND (MAX(t2.SignOff) IS NOT NULL AND MAX(t2.SignOff) < t1.CDDExecution)
# MAGIC           THEN 'On time' -- or rescheduled' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC             AND (MAX(t2.SignOff) IS NULL OR MAX(t2.SignOff) > t1.CDDExecution)
# MAGIC           THEN 'On time'
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC             AND (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) > MAX(t2.SignOff))
# MAGIC           THEN 'On time'
# MAGIC
# MAGIC           ELSE 'Not on time' -- - to be verified'
# MAGIC
# MAGIC         END AS OnTime
# MAGIC
# MAGIC     FROM radar.clients_historical t1
# MAGIC     LEFT JOIN radar.cases_historical t2 ON t1.UniqueGcobId = t2.UniqueGcobId AND t2.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND t2.CaseReviewType in ('Periodic Review') --, 'Offboarding')
# MAGIC     LEFT JOIN radar.clients_historical t3 ON t1.UniqueGcobId = t3.UniqueGcobId AND t3.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical)
# MAGIC
# MAGIC     WHERE
# MAGIC         t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1)
# MAGIC         AND YEAR(t1.CDDExecution) = YEAR(CURRENT_DATE)
# MAGIC         AND t1.Scope = 'backToGreen'
# MAGIC         AND t1.ClientLifeCycleName = 'Client'
# MAGIC     GROUP BY 1,2,3,7
# MAGIC   )
# MAGIC
# MAGIC SELECT 
# MAGIC   MONTH(CDDExecution_old) AS Month
# MAGIC   , COUNT(DISTINCT UniqueGcobId) AS CasesCount
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN MONTH(CDDExecution_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC         AND MONTH(CDDExecution_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC       THEN COUNT(DISTINCT CASE 
# MAGIC         WHEN OnTime IN ('On time') 
# MAGIC         THEN UniqueGcobId 
# MAGIC         ELSE NULL 
# MAGIC       END)
# MAGIC       ELSE NULL
# MAGIC     END AS CasesOnTimeCount
# MAGIC
# MAGIC   -- , CASE 
# MAGIC   --     WHEN MONTH(CDDExecution_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC   --       AND MONTH(CDDExecution_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC   --     THEN COUNT(DISTINCT CASE 
# MAGIC   --       WHEN OnTime IN ('On time or rescheduled') 
# MAGIC   --       THEN UniqueGcobId 
# MAGIC   --       ELSE NULL 
# MAGIC   --     END)
# MAGIC   --     ELSE NULL
# MAGIC   --   END AS CasesOnTimeOrRescheduledCount
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN MONTH(CDDExecution_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC         AND MONTH(CDDExecution_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC       THEN COUNT(DISTINCT CASE 
# MAGIC         WHEN OnTime IN ('Not on time') 
# MAGIC         THEN UniqueGcobId 
# MAGIC         ELSE NULL 
# MAGIC       END)
# MAGIC       ELSE NULL
# MAGIC     END AS CasesNotOnTimeCount
# MAGIC
# MAGIC   -- , CASE 
# MAGIC   --     WHEN MONTH(CDDExecution_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC   --       AND MONTH(CDDExecution_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC   --     THEN COUNT(DISTINCT CASE 
# MAGIC   --       WHEN OnTime IN ('Not on time - to be verified') 
# MAGIC   --       THEN UniqueGcobId 
# MAGIC   --       ELSE NULL 
# MAGIC   --     END)
# MAGIC   --     ELSE NULL
# MAGIC   --   END AS CasesNotOnTimeToBeVerifiedCount
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN MONTH(CDDExecution_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC         AND MONTH(CDDExecution_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC       THEN COUNT(DISTINCT CASE 
# MAGIC         WHEN OnTime IN ('Offboarding') 
# MAGIC         THEN UniqueGcobId 
# MAGIC         ELSE NULL 
# MAGIC       END)
# MAGIC       ELSE NULL
# MAGIC     END AS CasesOffboardedCount
# MAGIC
# MAGIC FROM pad_on_time_overview_cte
# MAGIC GROUP BY MONTH(CDDExecution_old)
# MAGIC ORDER BY MONTH(CDDExecution_old) ASC

# COMMAND ----------

# DBTITLE 1,drop tables
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.pid_on_time_overview_historical;
# MAGIC DROP TABLE IF EXISTS radar.pid_on_time_historical;
# MAGIC DROP TABLE IF EXISTS radar.pad_on_time_overview_historical;
# MAGIC DROP TABLE IF EXISTS radar.pad_on_time_historical;

# COMMAND ----------

# DBTITLE 1,store tables
spark.sql('SELECT * FROM pid_on_time_overview_historical').write.mode('overwrite').saveAsTable('radar.pid_on_time_overview_historical')
spark.sql('SELECT * FROM pid_on_time_historical').write.mode('overwrite').saveAsTable('radar.pid_on_time_historical')
spark.sql('SELECT * FROM pad_on_time_overview_historical').write.mode('overwrite').saveAsTable('radar.pad_on_time_overview_historical')
spark.sql('SELECT * FROM pad_on_time_historical').write.mode('overwrite').saveAsTable('radar.pad_on_time_historical')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.pid_on_time_overview_historical

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.pid_on_time_historical

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pid_on_time_overview_historical

# COMMAND ----------

# MAGIC %sql
# MAGIC select UniqueGcobId, caseid, Prework, Completed, CaseReviewType from radar.cases where UniqueGcobId = 18911

# COMMAND ----------

# MAGIC %sql
# MAGIC select UniqueGcobId
# MAGIC   , SourceClient
# MAGIC   -- , ClientCaseInitiationStart
# MAGIC   , SignOff AS SignOff
# MAGIC   , Prework AS Prework
# MAGIC   , CaseReviewType  
# MAGIC   
# MAGIC from radar.cases where UniqueGcobId = 24379
# MAGIC order by Prework desc

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     DATEDIFF(day, t2.prework, t1.ClientCaseInitiationStart) AS difference_in_days
# MAGIC     , t1.uniquegcobid
# MAGIC     , t2.prework, t1.ClientCaseInitiationStart
# MAGIC FROM radar.clients t1
# MAGIC LEFT JOIN radar.cases t2 ON t1.SourceClient = t2.SourceClient
# MAGIC
# MAGIC where t1.scope = 'backToGreen'
# MAGIC
# MAGIC ORDER BY difference_in_days DESC
# MAGIC
# MAGIC
# MAGIC If completed exists, then if
# MAGIC - PID is in the future wrt to EDL_loaddate --> case not started but all good
# MAGIC - PID is in the past wrt to EDL_loaddate --> case NOT on time
# MAGIC
# MAGIC If completed does not exists, then if
# MAGIC - prework is before PID --> case ON time
# MAGIC - prework is after PID --> case NOT on time

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC count(distinct t1.Assessmentanalyst)
# MAGIC -- Assessmentanalyst
# MAGIC -- , Readyforassessment
# MAGIC from radar.cases_historical t1
# MAGIC left join radar.clients_historical t2 on t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC where t1.Readyforassessment like '%2025%'
# MAGIC and t2.Scope = 'backToGreen'
# MAGIC and t1.EDL_LoadDate = (select max(EDL_LoadDate) from radar.cases_historical)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   -- month(CDDExecution) AS execution_month
# MAGIC   UniqueGcobId
# MAGIC   , FullLegalName
# MAGIC   , NextReviewDate
# MAGIC   , GlobalKYCPortfolioNew
# MAGIC   -- , GlobalReportingRegion
# MAGIC   , GlobalClientOwner
# MAGIC   , GlobalClientOwnerLocation
# MAGIC   
# MAGIC   , CDDExecution
# MAGIC   , Scope
# MAGIC   -- , count(*)
# MAGIC
# MAGIC from radar.clients_historical
# MAGIC where EDL_LoadDate = '2025-01-01'
# MAGIC
# MAGIC and CDDExecution like '%2025%'
# MAGIC and scope = 'backToGreen'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   -- -- month(CDDExecution) AS execution_month
# MAGIC   -- UniqueGcobId
# MAGIC   -- , FullLegalName
# MAGIC   -- , NextReviewDate
# MAGIC   -- , GlobalKYCPortfolioNew
# MAGIC   -- -- , GlobalReportingRegion
# MAGIC   -- , GlobalClientOwner
# MAGIC   -- , GlobalClientOwnerLocation
# MAGIC   
# MAGIC   -- , CDDExecution
# MAGIC   -- , Scope
# MAGIC   -- -- , count(*)
# MAGIC
# MAGIC   count(*)
# MAGIC   
# MAGIC
# MAGIC from radar.clients_historical
# MAGIC where EDL_LoadDate = '2025-02-01'
# MAGIC and SignOffDate like '2025%'
# MAGIC -- and CDDExecution like '%2025%'
# MAGIC -- and  like '%2025%'
# MAGIC and scope = 'backToGreen'
# MAGIC
# MAGIC -- group by 1

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC -- EDL_LoadDate
# MAGIC count(distinct uniquegcobid)
# MAGIC -- , ValidatedRiskLevel 
# MAGIC from radar.clients_historical
# MAGIC
# MAGIC  where EDL_LoadDate = '2024-11-01'
# MAGIC --  and NextReviewDate like '%2025%'
# MAGIC  and (NextReviewDate like '%2024%' or ClientCaseInitiationStart like '%2025%')
# MAGIC --  and overdue = 'Yes'
# MAGIC --  between '2024-09-30' and '2025-11-01'
# MAGIC  and Scope = 'backToGreen'
# MAGIC  and ClientLifeCycleName = 'Client'
# MAGIC
# MAGIC -- group by ValidatedRiskLevel

# COMMAND ----------

# MAGIC %sql
# MAGIC describe radar.clients

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC -- EDL_LoadDate
# MAGIC distinct ClientCaseInitiationStart
# MAGIC -- , ValidatedRiskLevel 
# MAGIC from radar.clients_historical limit 10
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT userName, department 
# MAGIC FROM radar.UserTeamRegistry
# MAGIC WHERE 1=1 
# MAGIC   -- AND CAST(TeamStartDate AS DATE) > date('2025-04-01')
# MAGIC   AND teamEndDate is null
# MAGIC   AND department = 'Corp CDD Hub UK'
# MAGIC   and team like '%eam%'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT userName, department 
# MAGIC FROM radar.UserTeamRegistry
# MAGIC WHERE 1=1 
# MAGIC   -- AND CAST(TeamStartDate AS DATE) > date('2025-04-01')
# MAGIC   AND teamEndDate is null
# MAGIC   AND department = 'Corp CDD Hub NL'
# MAGIC   and team like '%eam%'

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import os 

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC # Financed emission

# COMMAND ----------

spark.read.parquet(f'abfss://calculusgdp@{ReadStorage}.dfs.core.windows.net/FEWholesale/1/data/FiscalYear=*/*.parquet').createOrReplaceTempView('financed_emission')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   BusinessEffective_Datetimestamp,
# MAGIC   Load_Datetimestamp,
# MAGIC   FiscalYear,
# MAGIC   Portfolio,
# MAGIC   ClientId,
# MAGIC   -- Convert numeric columns to string with comma as decimal separator
# MAGIC   regexp_replace(cast(Scope1Emissions as string), '\\.', ',') as Scope1Emissions,
# MAGIC   regexp_replace(cast(Scope2Emissions as string), '\\.', ',') as Scope2Emissions,
# MAGIC   regexp_replace(cast(TotalEmissionsScope1_and_2 as string), '\\.', ',') as TotalEmissionsScope1_and_2,
# MAGIC   SourceScope1_and_2Emissions,
# MAGIC   regexp_replace(cast(Scope3Emissions as string), '\\.', ',') as Scope3Emissions,
# MAGIC   SourceScope3Emissions,
# MAGIC   regexp_replace(cast(RabobankFeScope1 as string), '\\.', ',') as RabobankFeScope1,
# MAGIC   regexp_replace(cast(RabobankFeScope2 as string), '\\.', ',') as RabobankFeScope2,
# MAGIC   regexp_replace(cast(RabobankFeScope1_and_2 as string), '\\.', ',') as RabobankFeScope1_and_2,
# MAGIC   regexp_replace(cast(RabobankFeScope3 as string), '\\.', ',') as RabobankFeScope3,
# MAGIC   regexp_replace(cast(IntensityScope1_and_2 as string), '\\.', ',') as IntensityScope1_and_2,
# MAGIC   regexp_replace(cast(IntensityScope3 as string), '\\.', ',') as IntensityScope3,
# MAGIC   regexp_replace(cast(DataQualityScoreScope1_and_2 as string), '\\.', ',') as DataQualityScoreScope1_and_2,
# MAGIC   regexp_replace(cast(DataQualityScoreScope3 as string), '\\.', ',') as DataQualityScoreScope3,
# MAGIC   RunVersion,
# MAGIC   EDL_LOAD_DTS,
# MAGIC   EDL_ACT_DTS
# MAGIC FROM financed_emission
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from financed_emission

# COMMAND ----------



# COMMAND ----------

# connect to firebird kyc_adb
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
jdbcHostname='firebirdsqlserverprodnla.database.windows.net'

jdbcPort = 1433
jdbcDatabase = "APP_ADB"


jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
connectionProperties = {
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication" : "ActiveDirectoryServicePrincipal",
    "UserName" : app_reg_app_id ,
    "Password" : service_credential
}

spark.read.jdbc(url=jdbcUrl,table='ext.clients',properties = connectionProperties).createOrReplaceTempView("ext_clients")
spark.read.jdbc(url=jdbcUrl,table='ext.cases',properties = connectionProperties).createOrReplaceTempView("ext_cases")

# sira tables
spark.read.jdbc(url=jdbcUrl,table='pa.sira_allclients',properties = connectionProperties).createOrReplaceTempView("pa_sira_allclients")
spark.read.jdbc(url=jdbcUrl,table='pa.sira_allproductandservices',properties = connectionProperties).createOrReplaceTempView("pa_sira_allproductandservices")
spark.read.jdbc(url=jdbcUrl,table='pa.sira_allhighriskproducts',properties = connectionProperties).createOrReplaceTempView("pa_sira_allhighriskproducts")
spark.read.jdbc(url=jdbcUrl,table='pa.sira_allhighriskactivities',properties = connectionProperties).createOrReplaceTempView("pa_sira_allhighriskactivities")
spark.read.jdbc(url=jdbcUrl,table='pa.sira_allgeoactivities',properties = connectionProperties).createOrReplaceTempView("pa_sira_allgeoactivities")
spark.read.jdbc(url=jdbcUrl,table='pa.sira_allpepubo',properties = connectionProperties).createOrReplaceTempView("pa_sira_allpepubo")

spark.read.jdbc(url=jdbcUrl,table='pa.KYCMasterListRegistry',properties = connectionProperties).createOrReplaceTempView('KYCMasterListRegistry')

# COMMAND ----------

# MAGIC %sql
# MAGIC select GcobId, `Global Reporting Region`, `Client name`, `Next review date`, SectorTeam, `Client lifecycle status` from ext_clients where SectorTeam is null and `Client lifecycle status` = 'Client'

# COMMAND ----------

# MAGIC %sql
# MAGIC select GcobId, `Global Reporting Region`, `Client name`, `Next review date`, SectorTeam, `Client lifecycle status` from ext_clients where GcobId = 22146
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # sira clients firebird vs radar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   CAST(t1.`identity` AS STRING) AS firebird
# MAGIC   , CAST(t2.identity AS STRING) AS radar
# MAGIC   , t2.UniqueGcobId
# MAGIC   , t2.ParentIdentity
# MAGIC   , t2.ParentType
# MAGIC
# MAGIC FROM pa_sira_allpepubo t1
# MAGIC FULL OUTER JOIN radar.sirapepubo t2 ON
# MAGIC   CASE 
# MAGIC     WHEN t1.gcobid LIKE 'NP: %' THEN REPLACE(t1.gcobid, 'NP: ', 'NP_') 
# MAGIC     ELSE t1.gcobid 
# MAGIC   END = t2.UniqueGcobId
# MAGIC   AND t1.ParentIdentity = t2.ParentIdentity
# MAGIC
# MAGIC WHERE 
# MAGIC     COALESCE(CAST(lower(trim(t1.`identity`)) AS STRING), '')
# MAGIC     <> 
# MAGIC     COALESCE(CAST(lower(trim(t2.identity)) AS STRING), '')
# MAGIC
# MAGIC   and t2.UniqueGcobId is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   CAST(t1.`onboardingdate` AS STRING) AS firebird
# MAGIC   , CAST(t2.OnboardingDate AS STRING) AS radar
# MAGIC   , t2.ClientLifeCycleName
# MAGIC   , t2.UniqueGcobId
# MAGIC   , t2.FullLegalName
# MAGIC   -- , t1.gcobid
# MAGIC
# MAGIC FROM pa_sira_allclients t1
# MAGIC FULL OUTER JOIN radar.siraclients t2 ON
# MAGIC   CASE 
# MAGIC     WHEN t1.gcobid LIKE 'NP: %' THEN REPLACE(t1.gcobid, 'NP: ', 'NP_') 
# MAGIC     ELSE t1.gcobid 
# MAGIC   END = t2.UniqueGcobId
# MAGIC
# MAGIC WHERE 
# MAGIC     COALESCE(CAST(lower(trim(t1.`OnboardingDate`)) AS STRING), '')
# MAGIC
# MAGIC --     -- Case when t1.`Entity Type Risk Level` = 2 then 'low'
# MAGIC --     -- when t1.`Entity Type Risk Level` = 3 then 'medium'
# MAGIC --     -- when t1.`Entity Type Risk Level` = 4 then 'high'
# MAGIC --     -- end
# MAGIC     <> 
# MAGIC     COALESCE(CAST(lower(trim(t2.OnboardingDate)) AS STRING), '')
# MAGIC
# MAGIC   and t2.clientlifecyclename = 'Client'
# MAGIC   and t2.OnboardingDate is null
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   -- COALESCE(MAX(SignOffDate), MAX(ClientApprovalDate)) AS OnboardingDate
# MAGIC   SignOffDate
# MAGIC   , ClientApprovalDate
# MAGIC   , UniqueGcobId
# MAGIC FROM radar.cases
# MAGIC WHERE CaseReviewType = 'Initial On-Boarding'
# MAGIC and UniqueGcobId = '18933'
# MAGIC -- GROUP BY UniqueGcobId

# COMMAND ----------

# MAGIC %md
# MAGIC # clients firebird vs radar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   CAST(t1.`OnboardingDate` AS STRING) AS firebird 
# MAGIC   , CAST(t2.OnboardingDate AS STRING) AS radar  
# MAGIC   , t1.`next review date`
# MAGIC   , t2.NextReviewDate
# MAGIC   -- , date_add(t1.`next review date`, -69)              
# MAGIC   , t2.ClientLifeCycleName                                                                   
# MAGIC   , t2.UniqueGcobId                               
# MAGIC   , t2.FullLegalName
# MAGIC
# MAGIC FROM ext_clients t1
# MAGIC FULL OUTER JOIN radar.clients t2 ON
# MAGIC   CASE 
# MAGIC     WHEN t1.gcobid LIKE 'NP: %' THEN REPLACE(t1.gcobid, 'NP: ', 'NP_') 
# MAGIC     ELSE t1.gcobid 
# MAGIC   END = t2.UniqueGcobId
# MAGIC WHERE 
# MAGIC     COALESCE(CAST(t1.`OnboardingDate` AS STRING), '') 
# MAGIC     <> 
# MAGIC     COALESCE(CAST(t2.OnboardingDate AS STRING), '')
# MAGIC
# MAGIC     AND t2.UniqueGcobId IS NOT NULL
# MAGIC     and t2.ClientLifeCycleName = 'Client'
# MAGIC     -- and 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC     CAST(t1.totalentitiesingroup AS STRING) AS firebird 
# MAGIC     , CAST(t2.totalentitiesingroup AS STRING) AS radar    
# MAGIC     , t1.KYCGroup AS firebird_kycgroup                   
# MAGIC     , t2.KYCGroup AS radar_kycgroup                      
# MAGIC     , t2.ClientLifeCycleName                             
# MAGIC     , t1.gcobid                                          
# MAGIC     , t2.UniqueGcobId
# MAGIC     , ABS(COALESCE(t1.totalentitiesingroup, 0) - COALESCE(t2.totalentitiesingroup, 0))  as difference                                  
# MAGIC     , t2.FullLegalName                                    
# MAGIC FROM ext_clients t1
# MAGIC FULL OUTER JOIN radar.clients t2 ON
# MAGIC     CASE 
# MAGIC         WHEN t1.gcobid LIKE 'NP: %' THEN REPLACE(t1.gcobid, 'NP: ', 'NP_') 
# MAGIC         ELSE t1.gcobid 
# MAGIC       END = t2.UniqueGcobId
# MAGIC WHERE 
# MAGIC     COALESCE(CAST(t1.totalentitiesingroup AS STRING), '') 
# MAGIC     <> 
# MAGIC     COALESCE(CAST(t2.totalentitiesingroup AS STRING), '')
# MAGIC
# MAGIC     AND ABS(COALESCE(t1.totalentitiesingroup, 0) - COALESCE(t2.totalentitiesingroup, 0)) > 2
# MAGIC
# MAGIC     AND t2.UniqueGcobId IS NOT NULL
# MAGIC
# MAGIC -- ORDER BY ABS(COALESCE(t1.totalentitiesingroup, 0) - COALESCE(t2.totalentitiesingroup, 0)) DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # cases firebird vs radar

# COMMAND ----------

# MAGIC %sql
# MAGIC describe ext_cases

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with cte as (
# MAGIC     select 
# MAGIC 	t1.*
# MAGIC     , CASE
# MAGIC     WHEN (
# MAGIC     ( t1.`Geographical risk level` 			= t2.`Geographical risk level`) AND
# MAGIC     ( t1.`Entity type risk level` 			= t2.`Entity type risk level`) AND
# MAGIC     ( t1.`Sector risk level` 				= t2.`Sector risk level`) AND
# MAGIC     ( t1.`Products and services risk level`	= t2.`Products and services risk level`	  ) AND
# MAGIC     ( t1.`Structured risk level` 			= t2.`Structured risk level`  ) AND
# MAGIC     ( t1.`Transaction risk level` 			= t2.`Transaction risk level` ) AND
# MAGIC     ( t1.`Distribution channel risk level` 	= t2.`Distribution channel risk level` ) AND
# MAGIC     ( t1.`Third party risk level` 			= t2.`Third party risk level` ) AND
# MAGIC     ( t1.`Adverse info risk level`			= t2.`Adverse info risk level`	 ) AND
# MAGIC     ( t1.`PEP risk level` 					= t2.`PEP risk level` ) AND
# MAGIC     ( t1.`Risk level` 						= t2.`Risk level` )) THEN 0
# MAGIC 	ELSE 1
# MAGIC 	END AS casehaschangedrisk
# MAGIC 	-- , t2.CaseId 								as `Previous CaseId`
# MAGIC 	, t2.`Geographical risk level`				as `Previous Geographical risk level`
# MAGIC 	, t2.`Entity type risk level`				as `Previous Entity type risk level`
# MAGIC 	, t2.`Sector risk level`					as `Previous Sector risk level`
# MAGIC 	, t2.`Products and services risk level`	 	as `Previous Products and services risk level`
# MAGIC 	, t2.`Structured risk level`	 			as `Previous Structured risk level`
# MAGIC 	, t2.`Transaction risk level`				as `Previous Transaction risk level`
# MAGIC 	, t2.`Distribution channel risk level` 		as `Previous Distribution channel risk level`
# MAGIC 	, t2.`Third party risk level`				as `Previous Third party risk level`
# MAGIC 	, t2.`Adverse info risk level`	 			as `Previous Adverse info risk level`
# MAGIC 	, t2.`PEP risk level`						as `Previous PEP risk level`
# MAGIC 	, t2.`Risk level` 							AS `Previous Risk level`
# MAGIC
# MAGIC from ext_cases t1
# MAGIC left join ext_cases t2 on t1.`Previous CaseId` = t2.`CaseId`
# MAGIC where t1.`completed date` > MAKE_TIMESTAMP(2021, 11, 1, 0, 0, 0)
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
# MAGIC select distinct
# MAGIC     t1.`casehaschangedrisk` as firebird
# MAGIC     , t2.casehaschangedrisk as radar
# MAGIC     , t2.UniqueGcobId
# MAGIC     , t2.CaseStatusName
# MAGIC     , t1.`case phase`
# MAGIC     , t1.caseid
# MAGIC     , t2.FullLegalName
# MAGIC     , t1.`Previous CaseId`
# MAGIC     , t2.PreviousCaseId
# MAGIC     , t2.PreviousSectorRiskLevel
# MAGIC     , t1.`Previous Sector risk level`
# MAGIC     -- , t2.ClientLifeCycleName
# MAGIC     -- , t1.`completed date`
# MAGIC     -- , t1.`client lifecycle status`
# MAGIC     -- , t2.IsLatestApprovedVersionOfClient
# MAGIC
# MAGIC from cte t1
# MAGIC LEFT JOIN radar.cases t2 
# MAGIC     ON CASE 
# MAGIC            WHEN t1.gcobid LIKE 'NP: %' THEN REPLACE(t1.gcobid, 'NP: ', 'NP_') 
# MAGIC            ELSE t1.gcobid 
# MAGIC        END = t2.UniqueGcobId 
# MAGIC        AND t1.caseid = t2.caseid
# MAGIC
# MAGIC where coalesce(CAST(trim(lower(t1.`casehaschangedrisk`)) AS STRING), '') 
# MAGIC     <> coalesce(CAST(trim(lower(t2.casehaschangedrisk)) AS string), '')
# MAGIC
# MAGIC -- and t2.UniqueGcobId = '2486' and t2.caseid = 85203
# MAGIC
# MAGIC -- and t1.`Previous CaseId` not like '%Legac%'
# MAGIC -- and t2.CaseStatusName <> 'Cancelled'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with cte as (
# MAGIC     select 
# MAGIC 	t1.*
# MAGIC     , CASE
# MAGIC     WHEN (
# MAGIC     ( t1.`Geographical risk level` 			= t2.`Geographical risk level`) AND
# MAGIC     ( t1.`Entity type risk level` 			= t2.`Entity type risk level`) AND
# MAGIC     ( t1.`Sector risk level` 				= t2.`Sector risk level`) AND
# MAGIC     ( t1.`Products and services risk level`	= t2.`Products and services risk level`	  ) AND
# MAGIC     ( t1.`Structured risk level` 			= t2.`Structured risk level`  ) AND
# MAGIC     ( t1.`Transaction risk level` 			= t2.`Transaction risk level` ) AND
# MAGIC     ( t1.`Distribution channel risk level` 	= t2.`Distribution channel risk level` ) AND
# MAGIC     ( t1.`Third party risk level` 			= t2.`Third party risk level` ) AND
# MAGIC     ( t1.`Adverse info risk level`			= t2.`Adverse info risk level`	 ) AND
# MAGIC     ( t1.`PEP risk level` 					= t2.`PEP risk level` ) AND
# MAGIC     ( t1.`Risk level` 						= t2.`Risk level` )) THEN 0
# MAGIC 	ELSE 1
# MAGIC 	END AS casehaschangedrisk
# MAGIC 	-- , t2.CaseId 								as `Previous CaseId`
# MAGIC 	, t2.`Geographical risk level`				as `Previous Geographical risk level`
# MAGIC 	, t2.`Entity type risk level`				as `Previous Entity type risk level`
# MAGIC 	, t2.`Sector risk level`					as `Previous Sector risk level`
# MAGIC 	, t2.`Products and services risk level`	 	as `Previous Products and services risk level`
# MAGIC 	, t2.`Structured risk level`	 			as `Previous Structured risk level`
# MAGIC 	, t2.`Transaction risk level`				as `Previous Transaction risk level`
# MAGIC 	, t2.`Distribution channel risk level` 		as `Previous Distribution channel risk level`
# MAGIC 	, t2.`Third party risk level`				as `Previous Third party risk level`
# MAGIC 	, t2.`Adverse info risk level`	 			as `Previous Adverse info risk level`
# MAGIC 	, t2.`PEP risk level`						as `Previous PEP risk level`
# MAGIC 	, t2.`Risk level` 							AS `Previous Risk level`
# MAGIC
# MAGIC from ext_cases t1
# MAGIC left join ext_cases t2 on t1.`Previous CaseId` = t2.`CaseId`
# MAGIC where t1.`completed date` > MAKE_TIMESTAMP(2021, 11, 1, 0, 0, 0)
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
# MAGIC select distinct
# MAGIC     t1.`casehaschangedrisk` as firebird
# MAGIC     , t2.casehaschangedrisk as radar
# MAGIC     , t2.UniqueGcobId
# MAGIC     , t2.CaseStatusName
# MAGIC     , t1.`case phase`
# MAGIC     , t1.caseid
# MAGIC     , t2.FullLegalName
# MAGIC     , t1.`Previous CaseId`
# MAGIC     , t2.PreviousCaseId
# MAGIC     , t2.PreviousSectorRiskLevel
# MAGIC     , t1.`Previous Sector risk level`
# MAGIC     -- , t2.ClientLifeCycleName
# MAGIC     -- , t1.`completed date`
# MAGIC     -- , t1.`client lifecycle status`
# MAGIC     -- , t2.IsLatestApprovedVersionOfClient
# MAGIC
# MAGIC from cte t1
# MAGIC LEFT JOIN radar.cases t2 
# MAGIC     ON CASE 
# MAGIC            WHEN t1.gcobid LIKE 'NP: %' THEN REPLACE(t1.gcobid, 'NP: ', 'NP_') 
# MAGIC            ELSE t1.gcobid 
# MAGIC        END = t2.UniqueGcobId 
# MAGIC        AND t1.caseid = t2.caseid
# MAGIC
# MAGIC where coalesce(CAST(trim(lower(t1.`casehaschangedrisk`)) AS STRING), '') 
# MAGIC     <> coalesce(CAST(trim(lower(t2.casehaschangedrisk)) AS string), '')
# MAGIC
# MAGIC -- and t2.UniqueGcobId = '2486' and t2.caseid = 85203
# MAGIC
# MAGIC -- and t1.`Previous CaseId` not like '%Legac%'
# MAGIC -- and t2.CaseStatusName <> 'Cancelled'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with cte as (
# MAGIC     select 
# MAGIC 	t1.*
# MAGIC     , CASE
# MAGIC     WHEN (
# MAGIC     ( t1.`Geographical risk level` 			= t2.`Geographical risk level`) AND
# MAGIC     ( t1.`Entity type risk level` 			= t2.`Entity type risk level`) AND
# MAGIC     ( t1.`Sector risk level` 				= t2.`Sector risk level`) AND
# MAGIC     ( t1.`Products and services risk level`	= t2.`Products and services risk level`	  ) AND
# MAGIC     ( t1.`Structured risk level` 			= t2.`Structured risk level`  ) AND
# MAGIC     ( t1.`Transaction risk level` 			= t2.`Transaction risk level` ) AND
# MAGIC     ( t1.`Distribution channel risk level` 	= t2.`Distribution channel risk level` ) AND
# MAGIC     ( t1.`Third party risk level` 			= t2.`Third party risk level` ) AND
# MAGIC     ( t1.`Adverse info risk level`			= t2.`Adverse info risk level`	 ) AND
# MAGIC     ( t1.`PEP risk level` 					= t2.`PEP risk level` ) AND
# MAGIC     ( t1.`Risk level` 						= t2.`Risk level` )) THEN 0
# MAGIC 	ELSE 1
# MAGIC 	END AS casehaschangedrisk
# MAGIC 	-- , t2.CaseId 								as `Previous CaseId`
# MAGIC 	, t2.`Geographical risk level`				as `Previous Geographical risk level`
# MAGIC 	, t2.`Entity type risk level`				as `Previous Entity type risk level`
# MAGIC 	, t2.`Sector risk level`					as `Previous Sector risk level`
# MAGIC 	, t2.`Products and services risk level`	 	as `Previous Products and services risk level`
# MAGIC 	, t2.`Structured risk level`	 			as `Previous Structured risk level`
# MAGIC 	, t2.`Transaction risk level`				as `Previous Transaction risk level`
# MAGIC 	, t2.`Distribution channel risk level` 		as `Previous Distribution channel risk level`
# MAGIC 	, t2.`Third party risk level`				as `Previous Third party risk level`
# MAGIC 	, t2.`Adverse info risk level`	 			as `Previous Adverse info risk level`
# MAGIC 	, t2.`PEP risk level`						as `Previous PEP risk level`
# MAGIC 	, t2.`Risk level` 							AS `Previous Risk level`
# MAGIC
# MAGIC from ext_cases t1
# MAGIC left join ext_cases t2 on t1.`Previous CaseId` = t2.`CaseId`
# MAGIC where t1.`completed date` > MAKE_TIMESTAMP(2021, 11, 1, 0, 0, 0)
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
# MAGIC select distinct
# MAGIC     t1.`Previous CaseId` as firebird
# MAGIC     , t2.PreviousCaseId as radar
# MAGIC     , t2.UniqueGcobId
# MAGIC     , t2.CaseStatusName
# MAGIC     , t1.`case phase`
# MAGIC     , t1.caseid
# MAGIC     , t2.FullLegalName
# MAGIC     , t1.`casehaschangedrisk` = t2.casehaschangedrisk as same_casehaschangedrisk
# MAGIC
# MAGIC     , t3.`case phase` as previous_casephase_firebird
# MAGIC     , t4.CaseStatusName as previous_casephase_radar
# MAGIC     -- , t2.ClientLifeCycleName
# MAGIC     -- , t1.`completed date`
# MAGIC     -- , t1.`client lifecycle status`
# MAGIC     -- , t2.IsLatestApprovedVersionOfClient
# MAGIC
# MAGIC from cte t1
# MAGIC LEFT JOIN radar.cases t2 
# MAGIC     ON CASE 
# MAGIC            WHEN t1.gcobid LIKE 'NP: %' THEN REPLACE(t1.gcobid, 'NP: ', 'NP_') 
# MAGIC            ELSE t1.gcobid 
# MAGIC        END = t2.UniqueGcobId 
# MAGIC        AND t1.caseid = t2.caseid
# MAGIC
# MAGIC
# MAGIC LEFT JOIN ext_cases t3 on t1.`Previous CaseId` = t3.caseid
# MAGIC LEFT JOIN radar.cases t4 ON t2.PreviousClientId = t4.ClientId AND t2.UniqueGcobId = t4.UniqueGcobId
# MAGIC
# MAGIC
# MAGIC where coalesce(CAST(trim(lower(t1.`Previous CaseId`)) AS STRING), '') 
# MAGIC     <> coalesce(CAST(trim(lower(t2.PreviousCaseId)) AS string), '')
# MAGIC
# MAGIC -- and t2.UniqueGcobId = '2486' and t2.caseid = 85203
# MAGIC
# MAGIC -- and t1.`Previous CaseId` not like '%Legac%'
# MAGIC -- and t2.CaseStatusName <> 'Cancelled'
# MAGIC
# MAGIC
# MAGIC and t1.`Previous CaseId` not like '%Lega%'
# MAGIC and t3.`case phase` <> 'Cancelled'
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# load and create temp views of all gdp_tables below
gcob_tables = [
    'CaseService_case_LegalEntityClient'
    , 'CaseService_case_Case'
    , 'CaseService_NaturalPerson_NaturalPersonClient'
    , 'CaseService_NaturalPerson_NaturalPersonCase'
    , 'RiskModel_dbo_Model'
    , 'CaseService_case_DynamicRiskModelInstanceReference'
    , 'RiskModel_dbo_InstanceCalculation'
    , 'RiskModel_dbo_Instance'
    , 'CaseService_NaturalPerson_DynamicRiskModelInstanceReference'
    , 'CaseService_case_CddRiskOverview'
]

for item in gcob_tables:
    # get the most recent file available in gdp
    path = f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/{item}/100/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOADED_DTS=')[1][:8] for file in files if 'LOADED_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/{item}/100/data/LOADED_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

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

SourceId = [0,1,2,3,4,5,6,7]
DestinationId = [0,0,1,2,3,3,3,4]

spark.createDataFrame(zip(SourceId, DestinationId), ['SourceId', 'DestinationId']).createOrReplaceTempView('gcob_static_RiskLevelMapping')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW client_risklevel AS
# MAGIC
# MAGIC WITH max_clientid_cte AS (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.GcobId
# MAGIC     , max(t1.Id) AS max_clientid
# MAGIC   FROM CaseService_case_LegalEntityClient t1
# MAGIC   LEFT JOIN CaseService_case_Case t2 ON t1.Id = t2.LegalEntityClientId
# MAGIC   WHERE t2.CurrentStatus = 9 
# MAGIC   GROUP BY GcobId
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   t1.gcobid
# MAGIC   , t3.Description as risklevel
# MAGIC from max_clientid_cte t1
# MAGIC left join CaseService_case_LegalEntityClient t11 on t1.max_clientid = t11.Id
# MAGIC LEFT JOIN CaseService_case_CddRiskOverview t2 ON t11.CddRiskOverviewId = t2.CddRiskOverviewId
# MAGIC LEFT JOIN gcob_static_RiskLevel t3 ON t2.ValidatedCddRisk = t3.Id

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC t1.UniqueGcobId
# MAGIC ,t1.validatedrisklevel
# MAGIC ,t2.risklevel
# MAGIC from radar.clients t1
# MAGIC left join client_risklevel t2 on t1.UniqueGcobId = t2.GcobId
# MAGIC where t1.ValidatedRiskLevel <> t2.risklevel

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from radar.siraclients where OnboardingDate is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC t1.CaseCompletedDate
# MAGIC , t1.duplicateoffboarding
# MAGIC , t1.kycdepartment
# MAGIC , t1.GlobalClientOwnerLocation
# MAGIC , t2.SectorTeam
# MAGIC
# MAGIC from radar.cases t1
# MAGIC left join radar.clients t2 on t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC where 1=1
# MAGIC and t1.CaseCompletedDate > CAST('2022-12-31' AS DATE) 
# MAGIC and t1.GlobalClientOwnerLocation in ('Rabobank Netherlands', 'Rabobank Argentina', 'Rabobank Kenya')
# MAGIC and t2.SectorTeam not in ('Agency', 'Correspondent Banking', 'FIG', 'LOKA', 'London', 'London FI', 'Markets', 'Treasury', 'TCF FI')
# MAGIC and t1.kycdepartment in ('Corp CDD HUB NL', 'Corp CDD Hub UK')
# MAGIC and duplicateoffboarding = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from radar.cases where UniqueGcobId like 'R%'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select clientid, SourceClient, SourceSystemReference, UniqueGcobId, SourceSystem from radar.clients where UniqueGcobId in (
# MAGIC select uniquegcobid from radar.clients group by UniqueGcobId having count(*) > 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC select clientid, SourceClient, SourceSystemReference, UniqueGcobId, SourceSystem from radar.cases where UniqueGcobId in (
# MAGIC select uniquegcobid from radar.cases group by UniqueGcobId having count(*) > 1)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.siraclients where PartyHasAllData = 1
# MAGIC and ClientLifeCycleName = 'Client'
# MAGIC and GlobalClientOwnerLocation not in ('Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)', 'Rabobank Chile', 'Rabobank Foundation')
# MAGIC and GlobalClientOwnerLocation is not null
# MAGIC and GlobalReportingRegionNew is not null
# MAGIC and SectorTeam not in ('Acorn', 'Rural')
# MAGIC and FIHubIndicator_Derived = 'Corp'
# MAGIC and GlobalReportingRegion = 'E&A'
# MAGIC and OnboardingDate > cast('2021-12-31' as date)
# MAGIC and OnboardingDate < cast('2023-01-01' as date)
# MAGIC
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(t1.HighRiskProductName) from radar.sirahighriskproducts t1
# MAGIC left join radar.siraclients t2 on t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC where 1=1
# MAGIC and t2.PartyHasAllData = 1
# MAGIC and t2.ClientLifeCycleName = 'Client'
# MAGIC and t2.GlobalClientOwnerLocation not in ('Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)', 'Rabobank Chile', 'Rabobank Foundation')
# MAGIC and t2.GlobalClientOwnerLocation is not null
# MAGIC and t2.GlobalReportingRegionNew is not null
# MAGIC and t2.SectorTeam not in ('Acorn', 'Rural')
# MAGIC and t2.FIHubIndicator_Derived = 'Corp'
# MAGIC and t2.GlobalReportingRegion = 'E&A'
# MAGIC and t1.HighRiskProductName <> 'None'
# MAGIC and t1.HighRiskProductName is not null
# MAGIC
# MAGIC and t1.QuestionName = 'High Risk Products'
# MAGIC and t1.QuestionId in (26, 77, 405)
# MAGIC and t1.
# MAGIC
# MAGIC
# MAGIC ProductName
# MAGIC Borrowing Base Finance TCF (BB-TCF)
# MAGIC Documentary Collections
# MAGIC Documentary Credit (Letter of Credit / LC)
# MAGIC Documentary Credit (Letter of Credit)
# MAGIC Guarantee / Standby LC
# MAGIC Guarantee Standby LC
# MAGIC Pre Export Finance (PXF)
# MAGIC Pre Payment Finance (PPF)
# MAGIC Structured Inventory Products
# MAGIC Structured Inventory Products (SIP)
# MAGIC Trade Finance
# MAGIC Transactional Trade Finance (TTF)
# MAGIC
# MAGIC
# MAGIC
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct QuestionId from radar.sirahighriskproducts where questionname = 'High Risk Products'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.sirahighriskproducts where QuestionId in (26, 77, 405)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.clients where SourceClient in (select SourceClient from radar.clients group by SourceClient having count(*) > 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.clients where UniqueGcobId in (select uniquegcobid from radar.clients group by UniqueGcobId having count(*) > 1)
