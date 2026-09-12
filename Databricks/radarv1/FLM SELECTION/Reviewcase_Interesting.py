# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal: 
# MAGIC To assign/flag for each review case that have increased risks of having gone wrong, these elements.
# MAGIC
# MAGIC ###### Author
# MAGIC init: Ruud van Laar
# MAGIC
# MAGIC ###### Uses:
# MAGIC 1. Input for FLM selection
# MAGIC 2. Input for risk-based deepdives
# MAGIC 3. Input for decision tracking
# MAGIC 4. Input for selecting cases in which newly implemented procedures and work instructions (should) have been applicable.
# MAGIC 5. Keeping FEC risks in control
# MAGIC 6. Automated data quality or process quality
# MAGIC
# MAGIC ###### Examples:
# MAGIC 1. QC colleague is a new colleague
# MAGIC 2. Include all EDRs (but check GCOB whether this is a full EDR)
# MAGIC 3. Include files with change in overall risk (as compared to the previous PR)
# MAGIC 4. Include files with changes in risk indicators
# MAGIC 5. Include files where Risk Level differs from Calculated risk
# MAGIC 6. Include files with Manual risk change
# MAGIC 7. Include files with >4 risk indicators
# MAGIC 8. Include files with Unacceptable risk(s)
# MAGIC 9. Include Global files (more locations involved)
# MAGIC 10. Include best effort PRs before offboarding
# MAGIC 11. Include files with PEP risk
# MAGIC 12. Include files with Control Measures
# MAGIC 13. Include files with Adverse media signal from daily screening
# MAGIC 14. Include different locations
# MAGIC 15. Include files with High Structure risk (TCSP, complex structures etc)
# MAGIC 16. Include files with High Geographic risk (ECHR3c nexus)
# MAGIC 17. Include (occasional) special requests by TLs (to select certain files or to select files checked by new-joiner QCs)
# MAGIC 18. Include completed BAU QEP files (list received from Noni)
# MAGIC
# MAGIC ### FLM-specific application
# MAGIC - Select only 1 per group, and not a group already passed this year
# MAGIC - Spread over list of available FLM-ers, evenly by businessline and risk level
# MAGIC - Exclude some files
# MAGIC - Balance selection based on QCs (no more than 3 files checked by the same QC, combine Lending UK and UK Hub buckets)
# MAGIC - Balance selection based on Risk (low, medium, high files)
# MAGIC - Balance selection on File type (EDR/PR/ONB)
# MAGIC
# MAGIC
# MAGIC o   Wholesale NL – select all ACORN files (unless they belong to the same group), target 6-7 per quarter
# MAGIC
# MAGIC o   Wholesale NL /UK Lending – prioritise ARG, RCI, Private Equity and Rabobank Subsidiaries
# MAGIC
# MAGIC o   Wholesale NL/UK Lending – select different locations (NL, Kenya, Argentina)
# MAGIC
# MAGIC o   Wholesale NL/Lending UK/UK Hub – select different sectors (different per bucket)
# MAGIC
# MAGIC o   UK Hub – select different locations (NL, Kenya, Frankfurt, London, Madrid, Milan, Paris, Antwerp, Dublin)
# MAGIC
# MAGIC o   UK Hub - prioritise London files (at least 2)
# MAGIC
# MAGIC o   PSPs: 2 per month (6 per quarter and 3 TEAs) depending on number of completions and other rules
# MAGIC
# MAGIC o   PSPs: minimize group overlaps, focus on Stichting entities
# MAGIC
# MAGIC o   PSPs: exclude FLM/SLM repairs
# MAGIC
# MAGIC o   PSPs: select 1 MR/LR per quarter
# MAGIC
# MAGIC o   Global FIs: 17 EDR/PR/ONB and 3 TEAs
# MAGIC
# MAGIC o   Global FIs - select max 20% Agency files (2-3 per month)
# MAGIC
# MAGIC o   Global Fis - select 1-2 MRA-Only (depending on number of selected PSPs)
# MAGIC
# MAGIC o   Global FIs - exclude NY files if NY is the only location
# MAGIC
# MAGIC o   Global FIs - prioritise files with 4+ locations (including NL)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Goal 2: Automate FLM Selection
# MAGIC
# MAGIC ### FLM-specific application
# MAGIC - Select only 1 per group, and not a group already passed this year
# MAGIC - Spread over list of available FLM-ers, evenly by businessline and risk level
# MAGIC - Exclude some files
# MAGIC - Balance selection based on QCs (no more than 3 files checked by the same QC, combine Lending UK and UK Hub buckets)
# MAGIC - Balance selection based on Risk (low, medium, high files)
# MAGIC - Balance selection on File type (EDR/PR/ONB)
# MAGIC
# MAGIC
# MAGIC ### Business rules
# MAGIC - Wholesale NL – select all ACORN files (unless they belong to the same group), target 6-7 per quarter
# MAGIC - Wholesale NL /UK Lending – prioritise ARG, RCI, Private Equity and Rabobank Subsidiaries
# MAGIC - Wholesale NL/UK Lending – select different locations (NL, Kenya, Argentina)
# MAGIC - Wholesale NL/Lending UK/UK Hub – select different sectors (different per bucket)
# MAGIC - UK Hub – select different locations (NL, Kenya, Frankfurt, London, Madrid, Milan, Paris, Antwerp, Dublin)
# MAGIC - UK Hub - prioritise London files (at least 2)
# MAGIC - PSPs: 2 per month (6 per quarter and 3 TEAs) depending on number of completions and other rules
# MAGIC - PSPs: minimize group overlaps, focus on Stichting entities
# MAGIC - PSPs: exclude FLM/SLM repairs
# MAGIC - PSPs: select 1 MR/LR per quarter
# MAGIC - Global FIs: 17 EDR/PR/ONB and 3 TEAs
# MAGIC - Global FIs - select max 20% Agency files (2-3 per month)
# MAGIC - Global Fis - select 1-2 MRA-Only (depending on number of selected PSPs)
# MAGIC - Global FIs - exclude NY files if NY is the only location
# MAGIC - Global FIs - prioritise files with 4+ locations (including NL)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rules FLM Latest
# MAGIC
# MAGIC 1. Select cases with Unacceptable risk (in any risk element)
# MAGIC 2. Exclude cases with reason FLM/SLM/Audit correction advice
# MAGIC 3. EDR: Select all (remaining) EDRs 
# MAGIC 4. PR: exclude Groups monitored in the past year
# MAGIC Acorn: exclude Groups monitored in the past quarter?
# MAGIC 5. PSP: prioritise Stichting Derdengelden over BV
# MAGIC 6. Global FI: select Correspondent Banking (if any)
# MAGIC 7. Global FI: select max 2 RMA-Only (light CDD)
# MAGIC 8. Global FI: select max 20% Agency files (2-3 per month) 
# MAGIC 9. **Groups: select one entity per group
# MAGIC 10. Include Global files (2+ locations involved)
# MAGIC 11. Include cases with change in Overall risk
# MAGIC 12. Include cases with difference in Recalculated vs Risk level (manual risk override)
# MAGIC 13. Include cases with 4+ High risk indicators
# MAGIC 14. Include PRs/EDRs with 3+ notch change in risk elements (up or down)
# MAGIC 15. Include cases with change in Adverse Media risk
# MAGIC 16. Include PR/EDR before off-boarding 
# MAGIC 17. Include cases with High Geographic risk (ECHR3c/Sanctions)
# MAGIC 18. Include cases with High/Medium Structure risk (TCSP, complex structures, Nominee Shareholder etc)
# MAGIC 19. Include cases with High Product risk 
# MAGIC 20. Include cases with High/Medium PEP risk
# MAGIC 21. Include cases with High/Medium Transaction risk**
# MAGIC 22. Minimum 2 files per Location (if available)
# MAGIC 23. Balance risk level (High, Medium, Low)
# MAGIC 24. Balance Review Type (ONB/PR)
# MAGIC 25. Balance Sector Team
# MAGIC 26. Balance QC: not more than 3x same QC per month
# MAGIC 27. Balance reasons for TEA: change in: structure, adverse media / ONS, transaction risk etc

# COMMAND ----------

import pandas as pd
import os
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
import pyspark.sql.functions as F



# COMMAND ----------

# MAGIC %md
# MAGIC #### Initiate connect to GDP

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

Today = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'
print (load_dts)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
#'party_AllCasesReport'
'party_case_client_details'
, 'party_LocalRequirement'
, 'party_local_client_Owners'
, 'party_client'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{row.GDPname}/101/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_case_client_details limit 10
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply Scenario Rules
# MAGIC - EDR
# MAGIC - riskChange?
# MAGIC - RiskLevelDifference
# MAGIC - ? BestEffortPR?
# MAGIC - ControlMeasures

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Scenario_EDR AS
# MAGIC select * from party_case_client_details 
# MAGIC where ClientOwnerSignOffDate > dateadd(MONTH,-1,current_date())
# MAGIC and ReviewTypeName = 'Event Driven Review'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Scenario_EDR 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Scenario_Consultation AS
# MAGIC select * from party_case_client_details 
# MAGIC where ClientOwnerSignOffDate > dateadd(MONTH,-1,current_date())
# MAGIC AND ConsultationRequired = true

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Scenario_Consultation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Scenario_ClientCommittee AS
# MAGIC select * from party_case_client_details 
# MAGIC where ClientOwnerSignOffDate > dateadd(MONTH,-1,current_date())
# MAGIC AND LEFT(SubmitToClientCommittee,3) = 'Yes'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Scenario_ClientCommittee 

# COMMAND ----------

#? Scenario: Client commiittee, but not further consultation?


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Scenario_ValidatedRiskIsNotCalculatedRisk AS
# MAGIC select * from party_case_client_details 
# MAGIC where ClientOwnerSignOffDate > dateadd(MONTH,-1,current_date())
# MAGIC AND ModelRecalculatedRiskLevel <> ValidatedRiskLevel

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Scenario_ValidatedRiskIsNotCalculatedRisk

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Scenario_4CategoriesNotLowRisk AS
# MAGIC select * from party_case_client_details 
# MAGIC where ClientOwnerSignOffDate > dateadd(MONTH,-1,current_date())
# MAGIC AND ValidatedRiskLevel
# MAGIC
# MAGIC  'GeographicalRiskLevel',
# MAGIC  'EntityTypeRiskLevel',
# MAGIC  'StructureRiskLevel',
# MAGIC  'SectorRiskLevel',
# MAGIC  'ProductAndServiceRiskLevel',
# MAGIC  'PEPRiskLevel',
# MAGIC  'TransactionRiskLevel',
# MAGIC  'DistributionRiskLevel',
# MAGIC  'ThirdPartyRiskLevel',
# MAGIC  'AdverseInfoRiskLevel',
# MAGIC  'OtherRiskLevel',
# MAGIC  'GeographicalApplicableRisk',
# MAGIC  'EntityTypeApplicableRisk',
# MAGIC  'StructureApplicableRisk',
# MAGIC  'SectorApplicableRisk',
# MAGIC  'ProductsApplicableRisk',
# MAGIC  'PoliticallyExposedPersonsApplicableRisk',
# MAGIC  'TransactionApplicableRisk',
# MAGIC  'DistributionChannelApplicableRisk',
# MAGIC  'ThirdPartyApplicableRisk',
# MAGIC  'AdverseInfoApplicableRisk',
# MAGIC  'OtherApplicableRisk',

# COMMAND ----------

spark.table('party_case_client_details').columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Scenario: RiskNotApplicable

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CTE_PreviousRisk_Case AS
# MAGIC with CaseClientDetails as 
# MAGIC (
# MAGIC     select distinct CaseId,ClientId,SourceClient,GcobId,ValidatedRiskLevel,FullLegalName,clienttype,SourceSystem,
# MAGIC     AdverseInfoRiskLevel,GeographicalRiskLevel,EntityTypeRiskLevel,StructureRiskLevel,SectorRiskLevel,ProductAndServiceRiskLevel,PEPRiskLevel,TransactionRiskLevel,DistributionRiskLevel,ThirdPartyRiskLevel
# MAGIC     ,FatcaClassification,CrsClassification
# MAGIC     From party_case_client_details
# MAGIC )
# MAGIC
# MAGIC ,PreviousRiskLevel as (
# MAGIC SELECT DISTINCT 
# MAGIC     CaseId,ClientId,SourceClient,GcobId--,ValidatedRiskLevel,FullLegalName
# MAGIC     --,LAG(ValidatedRiskLevel, 1) OVER (PARTITION BY GcobId ORDER BY SourceSystem desc, COALESCE(FinalDecisionDate, NextReviewDate) desc) AS `Previous risk level`
# MAGIC     --,LAG(CaseId, 1) OVER (PARTITION BY GcobId ORDER BY SourceSystem desc, COALESCE(NextReviewDate, ClientOwnerSignOffDate) desc) AS PreviousCaseId
# MAGIC     --,row_number() over (partition by GcobId ORDER BY SourceSystem desc, COALESCE(FinalDecisionDate, NextReviewDate) desc) as Seq
# MAGIC     ,LAG(ValidatedRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS `Previous risk level`
# MAGIC     ,LAG(CaseId, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousCaseId
# MAGIC     ,LAG(AdverseInfoRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousAdverseInfoRiskLevel
# MAGIC     ,LAG(GeographicalRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousGeographicalRiskLevel
# MAGIC     ,LAG(EntityTypeRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousEntityTypeRiskLevel
# MAGIC     ,LAG(StructureRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousStructureRiskLevel
# MAGIC     ,LAG(SectorRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousSectorRiskLevel
# MAGIC     ,LAG(ProductAndServiceRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousProductAndServiceRiskLevel
# MAGIC     ,LAG(PEPRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousPEPRiskLevel
# MAGIC     ,LAG(TransactionRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousTransactionRiskLevel
# MAGIC     ,LAG(DistributionRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousDistributionRiskLevel
# MAGIC     ,LAG(ThirdPartyRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousThirdPartyRiskLevel
# MAGIC     ,LAG(FatcaClassification, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousFatcaClassification
# MAGIC     ,LAG(CrsClassification, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousCrsClassification
# MAGIC     ,row_number() over (partition by GcobId,clienttype ORDER BY CaseId asc,SourceSystem desc) as Seq
# MAGIC     FROM CaseClientDetails
# MAGIC --where gcobid in (926)--,3435,926)
# MAGIC --SourceClient in ('NP_NPPC_3435','LEC_1476') 
# MAGIC )
# MAGIC select distinct
# MAGIC GcobId
# MAGIC ,CaseId
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousCaseId end as PreviousCaseId
# MAGIC ,ClientId,SourceClient
# MAGIC ,case when CaseId = PreviousCaseId then null else `Previous risk level` end as `Previous risk level`
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousAdverseInfoRiskLevel end as PreviousAdverseInfoRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousGeographicalRiskLevel end as PreviousGeographicalRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousEntityTypeRiskLevel end as PreviousEntityTypeRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousStructureRiskLevel end as PreviousStructureRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousSectorRiskLevel end as PreviousSectorRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousProductAndServiceRiskLevel end as PreviousProductAndServiceRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousPEPRiskLevel end as PreviousPEPRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousTransactionRiskLevel end as PreviousTransactionRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousDistributionRiskLevel end as PreviousDistributionRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousThirdPartyRiskLevel end as PreviousThirdPartyRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousFatcaClassification end as PreviousFatcaClassification
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousCrsClassification end as PreviousCrsClassification
# MAGIC --,FullLegalName,ValidatedRiskLevel,Seq 
# MAGIC from PreviousRiskLevel 
# MAGIC --where Seq = 2
# MAGIC order by CaseId

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 2: From scenario's to sampling
# MAGIC
# MAGIC ###### step 1: Create one overview with all files and top-risks
# MAGIC Now that we have some scenarios, we'll create one client overview with all scenario data. 
# MAGIC And we'll apply some rules to get the top risks out of them.
# MAGIC
# MAGIC
# MAGIC ###### step 2:
# MAGIC - split into relevant sub-portfolios
# MAGIC
# MAGIC
# MAGIC ###### step 3:
# MAGIC - per portfolio: apply selection rules
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Apply numbers and bring together. 
# MAGIC WITH CTE_AllScenarios AS (
# MAGIC   select 100 as ScenarioRisk, SourceClient FROM
# MAGIC
# MAGIC   select 100 as ScenarioRisk, SourceClient FROM
# MAGIC
# MAGIC   select 100 as ScenarioRisk, SourceClient FROM
# MAGIC
# MAGIC   select 100 as ScenarioRisk, SourceClient FROM
# MAGIC
# MAGIC   select 100 as ScenarioRisk, SourceClient FROM
# MAGIC
# MAGIC )
# MAGIC
# MAGIC -- 2. SUM and pivot
# MAGIC
# MAGIC
# MAGIC
# MAGIC --3. order by.
# MAGIC
