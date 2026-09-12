# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal: 
# MAGIC To assign/flag for each review case that have increased risks of having gone wrong, these elements.
# MAGIC
# MAGIC ###### Author
# MAGIC init: Ruud van Laar
# MAGIC
# MAGIC ### Selection scope.
# MAGIC Weekly selection is temporary. And only rule-based.
# MAGIC Only Risk-based;
# MAGIC QC Feedback is now getting much faster feedback.
# MAGIC
# MAGIC - 8
# MAGIC - 1 TEA NL
# MAGIC - 1 TEA UK
# MAGIC
# MAGIC
# MAGIC ### Monthly Selection. Has different numbers
# MAGIC Monthly Selection has more files. 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO
# MAGIC - TEA reason division lacking
# MAGIC - at low samples, dividing over sector teams doesnt work
# MAGIC - BestPRBeforeOffboarding
# MAGIC - 
# MAGIC - 2025-05-15:
# MAGIC   -   Should we have a max. of EDR's?
# MAGIC   -   EDR's proportional to nr of files
# MAGIC   -   Make sure we have 3 files random
# MAGIC   -   make sure we have 2 onboards.
# MAGIC   -   fill until 15 with random selection
# MAGIC     - random add onb, pr
# MAGIC   - For onboarding, they can be from thesame group
# MAGIC   - For EDR, they should not be from thesame group
# MAGIC   - more hard focus on 8 for NL and 8 for UK.
# MAGIC
# MAGIC It's oke if we go over the 10. FLM can optimize what from a broader selection
# MAGIC
# MAGIC - Biggest item: duplicate groups selection.

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
# MAGIC --> To recognize via product: LORO Account
# MAGIC 7. Global FI: select max 2 RMA-Only (light CDD). Try at least 1. (context: Risk is that there is a new process. RMA could be followed while full onboarding/ pr had been required. Adverse media for these files are only relevant if it's on the swift NW. other AM is discarded)
# MAGIC 8. Global FI: Agency select max 2-3 per month. But at least 1 --> Interesting if Adverse Media.
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

# MAGIC %md
# MAGIC ## TODO: FOR EDR: Choose 2, but prioritize when either validated risk is different from calculated risk, or when validated risk is different from previous vali

# COMMAND ----------

import pandas as pd
import os
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
import pyspark.sql.functions as F



# COMMAND ----------

# DBTITLE 1,Ensure this only runs on thursdays
today = datetime.today().weekday() # Monday is 0, Sunday is 6

# Only run on thursdays
if today != 3:
    print("Not thursdays. Skipping task.")
    dbutils.notebook.exit('stopping-not-thursdays')


# COMMAND ----------

NR_FILES_TO_SELECT = 7
NR_FILES_TO_SELECT_NL = 9
NR_FILES_TO_SELECT_UK = 9
NR_FILES_TO_SELECT_RANDOM = 3

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

Today = (datetime.today()-timedelta(0)).strftime('%Y%m%d')
load_dts = 'LOAD_DT=' + Today + '*'
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
, 'party_products_and_services'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{row.GDPname}/102/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# DBTITLE 1,Getting the right daterange for selection period
currentdate = datetime.now()
weekday = currentdate.weekday()

# minusdays is the days until the end of the period, relative from now
# Anchor to the most recent Thursday (if today is Thursday, otherwise last Thursday)
minusdays = (weekday - 3) % 7

EndOfDateRange = (currentdate -timedelta(minusdays)).date()
EndOfDateRange_str = EndOfDateRange.strftime('%Y-%m-%d')
print('EndOfDateRange :', EndOfDateRange)

StartOfDateRange = EndOfDateRange - timedelta(days = 7)
StartOfDateRange_str = StartOfDateRange.strftime('%Y-%m-%d')
print('StartOfDateRange :', StartOfDateRange_str)

# COMMAND ----------

# DBTITLE 1,DEFINE THE SELECTION SCOPE
# selectionscope defined

spark.sql(f""" 
--Groups: select one entity per group
Select t1.*,
 CASE 
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND (t1.ReviewReason like '%structure%' OR t1.TeaOtherReason like '%structure%') THEN 'Structure'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND (t1.ReviewReason like '%Adverse information%' OR t1.TeaOtherReason like '%Adverse information%')THEN 'AM'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND (t1.ReviewReason like '%transaction%' OR t1.TeaOtherReason like '%transaction%')THEN 'Transaction'
    ELSE ''
    END AS TEA_Review_Reason_Derived
    ,  CASE 
    WHEN t1.CaseReviewType = 'Event Assessment' AND (t1.ReviewReason like '%structure%' OR t1.TeaOtherReason like '%structure%') THEN 'Structure'
    WHEN t1.CaseReviewType = 'Event Assessment' AND (t1.ReviewReason like '%Adverse information%' OR t1.TeaOtherReason like '%Adverse information%')THEN 'AM'
    WHEN t1.CaseReviewType = 'Event Assessment' AND (t1.ReviewReason like '%transaction%' OR t1.TeaOtherReason like '%transaction%')THEN 'Transaction'
    ELSE ''
    END AS EA_Review_Reason_Derived
    , t2.SectorTeam
    , t2.KYCGroup

, Case
   -- 2025-05-21 - EDR is not automatically selected anymore
   --WHEN  t1.CaseReviewType = 'Event Driven Review' THEN 1 
   WHEN  t1.ValidatedRiskLevel = 'Unacceptable' THEN 1

   END AS SelectionIncludeFlag

, CASE 
   WHEN 1=1 THEN 0
   END AS SelectionExcludeFlag
 from radar.cases t1 
 left join radar.clients t2 on t1.UniqueGcobId = t2.uniquegcobid
where t1.CaseCompletedDate between date('{StartOfDateRange_str}') and date('{EndOfDateRange_str}')
AND t1.CaseReviewType NOT IN ( 'Amendment', 'Change of Client Owner', 'Product Off-boarding', 'Offboarding')
AND t1.KYCDepartment IN ('Corp CDD Hub UK', 'Corp CDD Hub NL')
AND t2.SectorTeam not in ('PSP', 'Acorn', 'Foundation', 'PSP Coverage', 'RCI') -- which sector teams to exclude? , 'Rabo Frontier Ventures'
-- the below excludes specific flm reparis/remediatins/corrections
AND ( 
   (t1.TEAReviewReasonDescription is null ) 
   OR 
      ((lower(t1.ReviewReason) not like '%flm%'
AND lower(t1.ReviewReason) not like '%slm%'
AND lower(t1.ReviewReason) not like '%repair%'
AND lower(t1.ReviewReason) not like '%remediation%'
AND lower(t1.ReviewReason) not like '%correction%' ))) """).createOrReplaceTempView('SelectionScope')


# COMMAND ----------

# DBTITLE 1,One File Per Group
# One File Per Group
df_OneFilePerGroup = spark.sql('''  SELECT 
    t1.SourceClient
  , t1.UniqueGcobId -- slightly challenging that this is not thesame as GDP unique GcobID
  , t1.KYCGroup
  FROM SelectionScope t1

  WHERE 1=1 ''' ).dropDuplicates(subset=['KYCGroup'])

df_OneFilePerGroup.createOrReplaceTempView('OneFilePerGroup')

# COMMAND ----------

# DBTITLE 1,Define All Selection Criteria
# MAGIC %sql
# MAGIC --Groups: select one entity per group
# MAGIC --Include Global files (2+ locations involved)
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW GlobalFile AS 
# MAGIC (
# MAGIC SELECT 
# MAGIC     t1.SourceClient
# MAGIC   , t1.UniqueGcobId
# MAGIC   , t2.KYCGroup
# MAGIC   FROM SelectionScope t1
# MAGIC   LEFT JOIN radar.clients as t2 on t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC   Where T2.GlobalFiles = 1
# MAGIC   );
# MAGIC
# MAGIC --Include cases with change in Overall risk
# MAGIC CREATE OR REPLACE TEMP VIEW ValidatedRiskChange AS 
# MAGIC (
# MAGIC   SELECT 
# MAGIC     t1.SourceClient
# MAGIC   , t1.UniqueGcobId 
# MAGIC   FROM SelectionScope t1
# MAGIC   WHERE t1.ValidatedRiskLevel <> t1.PreviousValidatedRiskLevel
# MAGIC );
# MAGIC
# MAGIC --Include cases with difference in Recalculated vs Risk level (manual risk override)
# MAGIC CREATE OR REPLACE TEMP VIEW ManualRiskOverride AS 
# MAGIC (
# MAGIC   SELECT 
# MAGIC     t1.SourceClient
# MAGIC   , t1.UniqueGcobId
# MAGIC   FROM SelectionScope t1
# MAGIC   WHERE t1.ValidatedRiskLevel <> t1.ModelRecalculatedRiskLevel
# MAGIC );
# MAGIC
# MAGIC --Include cases with 4+ High risk indicators
# MAGIC CREATE OR REPLACE TEMP VIEW FourHRIndicators AS 
# MAGIC --? translate all to number, 1 if high, and then see if adds up to more than 4?
# MAGIC   (
# MAGIC     WITH CTE_risknr AS (
# MAGIC       SELECT 
# MAGIC         t1.SourceClient
# MAGIC       , IFF(t1.GeographicalRiskLevel = 'High', 1,0) AS GEONr
# MAGIC       , IFF(t1.EntityTypeRiskLevel = 'High', 1,0) AS ENTNr 
# MAGIC       , IFF(t1.StructureRiskLevel = 'High', 1,0) AS STRNr
# MAGIC       , IFF(t1.SectorRiskLevel = 'High', 1,0) AS SECNr 
# MAGIC       , IFF(t1.ProductAndServiceRiskLevel = 'High', 1,0) AS PSNr 
# MAGIC       , IFF(t1.PEPRiskLevel = 'High', 1,0) AS PepNr 
# MAGIC       , IFF(t1.TransactionRiskLevel = 'High', 1,0) AS TXNr 
# MAGIC       , IFF(t1.DistributionRiskLevel = 'High', 1,0) AS DSTRNr 
# MAGIC       , IFF(t1.ThirdPartyRiskLevel = 'High', 1,0) AS TPNr 
# MAGIC       , IFF(t1.AdverseInfoRiskLevel = 'High', 1,0) AS ADVNr
# MAGIC       , IFF(t1.OtherRiskLevel = 'High', 1,0) AS OTHNr				
# MAGIC       FROM SelectionScope t1 )
# MAGIC
# MAGIC     select * 
# MAGIC     from CTE_risknr
# MAGIC     where (OTHNr + ADVNr + TPNr + DSTRNr + TXNr + PepNr +  PSNr + SECNr + STRNr + ENTNr + GEONr) > 3
# MAGIC   );
# MAGIC -- check below.
# MAGIC
# MAGIC --Include PRs/EDRs with 3+ notch change in risk elements (up or down)
# MAGIC CREATE OR REPLACE TEMP VIEW ThreePlusNotchChange AS 
# MAGIC   (
# MAGIC     WITH CTE_risknr AS (
# MAGIC       SELECT 
# MAGIC         t1.SourceClient
# MAGIC       , IFF(t1.GeographicalRiskLevel <> t1.PreviousGeographicalRiskLevel, 1,0) AS GEONr
# MAGIC       , IFF(t1.EntityTypeRiskLevel <> t1.PreviousEntityTypeRiskLevel, 1,0) AS ENTNr 
# MAGIC       , IFF(t1.StructureRiskLevel <> t1.PreviousStructureRiskLevel, 1,0) AS STRNr
# MAGIC       , IFF(t1.SectorRiskLevel <> t1.PreviousSectorRiskLevel, 1,0) AS SECNr 
# MAGIC       , IFF(t1.ProductAndServiceRiskLevel <> t1.PreviousProductAndServiceRiskLevel, 1,0) AS PSNr 
# MAGIC       , IFF(t1.PEPRiskLevel <> t1.PreviousPEPRiskLevel, 1,0) AS PepNr 
# MAGIC       , IFF(t1.TransactionRiskLevel <> t1.PreviousTransactionRiskLevel, 1,0) AS TXNr 
# MAGIC       , IFF(t1.DistributionRiskLevel <> t1.PreviousDistributionRiskLevel, 1,0) AS DSTRNr 
# MAGIC       , IFF(t1.ThirdPartyRiskLevel <> t1.PreviousThirdPartyRiskLevel, 1,0) AS TPNr 
# MAGIC       , IFF(t1.AdverseInfoRiskLevel <> t1.PreviousAdverseInfoRiskLevel, 1,0) AS ADVNr
# MAGIC       , IFF(t1.OtherRiskLevel <> t1.PreviousOtherRiskLevel, 1,0) AS OTHNr				
# MAGIC       FROM SelectionScope t1 )
# MAGIC
# MAGIC     select * 
# MAGIC     from CTE_risknr
# MAGIC     where (OTHNr + ADVNr + TPNr + DSTRNr + TXNr + PepNr +  PSNr + SECNr + STRNr + ENTNr + GEONr) > 2
# MAGIC   );
# MAGIC
# MAGIC --Include cases with change in Adverse Media risk
# MAGIC CREATE OR REPLACE TEMP VIEW AdverseMediaRiskChange AS 
# MAGIC (
# MAGIC SELECT 
# MAGIC         t1.SourceClient
# MAGIC         FROM SelectionScope t1
# MAGIC         where t1.AdverseInfoRiskLevel <> t1.PreviousAdverseInfoRiskLevel
# MAGIC );
# MAGIC
# MAGIC --Include PR/EDR before off-boarding
# MAGIC -- TODO;
# MAGIC CREATE OR REPLACE TEMP VIEW PreOffboardingReview AS
# MAGIC (
# MAGIC SELECT 
# MAGIC         t1.SourceClient
# MAGIC         FROM SelectionScope t1
# MAGIC         where t1.AdverseInfoRiskLevel <> t1.PreviousAdverseInfoRiskLevel
# MAGIC );
# MAGIC
# MAGIC --Include cases with High Geographic risk (ECHR3c/Sanctions)
# MAGIC CREATE OR REPLACE TEMP VIEW GeoRiskHigh AS
# MAGIC (
# MAGIC SELECT 
# MAGIC         t1.SourceClient
# MAGIC         FROM SelectionScope t1
# MAGIC         where t1.GeographicalRiskLevel = 'High'
# MAGIC );
# MAGIC
# MAGIC --Include cases with High/Medium Structure risk (TCSP, complex structures, Nominee Shareholder etc)
# MAGIC CREATE OR REPLACE TEMP VIEW HighMedStructureRisk AS
# MAGIC (
# MAGIC SELECT 
# MAGIC         t1.SourceClient
# MAGIC         FROM SelectionScope t1
# MAGIC         where t1.StructureRiskLevel IN ('High', 'Medium')
# MAGIC );
# MAGIC
# MAGIC --Include cases with High Product risk
# MAGIC CREATE OR REPLACE TEMP VIEW HighProductRisk AS
# MAGIC (
# MAGIC SELECT 
# MAGIC         t1.SourceClient
# MAGIC         FROM SelectionScope t1
# MAGIC         where t1.ProductAndServiceRiskLevel IN ('High')
# MAGIC );
# MAGIC
# MAGIC --Include cases with High/Medium PEP risk
# MAGIC CREATE OR REPLACE TEMP VIEW HighMedPEPRisk AS
# MAGIC (
# MAGIC SELECT 
# MAGIC         t1.SourceClient
# MAGIC         FROM SelectionScope t1
# MAGIC         where t1.PEPRiskLevel IN ('High', 'Medium')
# MAGIC );
# MAGIC
# MAGIC --Include cases with High/Medium Transaction risk**
# MAGIC CREATE OR REPLACE TEMP VIEW HighMedTransactionRisk AS
# MAGIC (
# MAGIC SELECT 
# MAGIC         t1.SourceClient
# MAGIC         FROM SelectionScope t1
# MAGIC         where t1.TransactionRiskLevel IN ('High', 'Medium')
# MAGIC );
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Get all cases and what criteria it matches
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW AllReviewsPhase3 AS
# MAGIC
# MAGIC Select SourceClient
# MAGIC , 'OneFilePerGroup' AS SELECTION_CRITERIA
# MAGIC FROM OneFilePerGroup
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select SourceClient
# MAGIC , 'GlobalFile' AS SELECTION_CRITERIA
# MAGIC FROM GlobalFile
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select SourceClient
# MAGIC , 'ValidatedRiskChange' AS SELECTION_CRITERIA
# MAGIC FROM ValidatedRiskChange
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select SourceClient
# MAGIC , 'FourHRIndicators' AS SELECTION_CRITERIA
# MAGIC FROM FourHRIndicators
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select SourceClient
# MAGIC , 'ManualRiskOverride' AS SELECTION_CRITERIA
# MAGIC FROM ManualRiskOverride
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select SourceClient
# MAGIC , 'AdverseMediaRiskChange' AS SELECTION_CRITERIA
# MAGIC FROM AdverseMediaRiskChange
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select SourceClient
# MAGIC , 'ThreePlusNotchChange' AS SELECTION_CRITERIA
# MAGIC FROM ThreePlusNotchChange
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select SourceClient
# MAGIC , 'GeoRiskHigh' AS SELECTION_CRITERIA
# MAGIC FROM GeoRiskHigh
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select SourceClient
# MAGIC , 'HighMedStructureRisk' AS SELECTION_CRITERIA
# MAGIC FROM HighMedStructureRisk
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select SourceClient
# MAGIC , 'HighProductRisk' AS SELECTION_CRITERIA
# MAGIC FROM HighProductRisk
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select SourceClient
# MAGIC , 'HighMedPEPRisk' AS SELECTION_CRITERIA
# MAGIC FROM HighMedPEPRisk
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select SourceClient
# MAGIC , 'HighMedTransactionRisk' AS SELECTION_CRITERIA
# MAGIC FROM HighMedTransactionRisk
# MAGIC
# MAGIC
# MAGIC -- 
# MAGIC --Global FI: select Correspondent Banking (if any)
# MAGIC --Global FI: select max 2 RMA-Only (light CDD)
# MAGIC --Global FI: select max 20% Agency files (2-3 per month)
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,ALL_SUMMARY_ITEMS
# MAGIC %sql
# MAGIC select count(t1.sourceClient) AS COUNT,
# MAGIC t1.SELECTION_CRITERIA 
# MAGIC FROM AllReviewsPhase3 t1
# MAGIC --LEFT JOIN radar.cases on t1.sourceclient = t2.sourceclient
# MAGIC GROUP BY t1.SELECTION_CRITERIA

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE1 AS (
# MAGIC select count(t1.sourceClient) AS COUNT,
# MAGIC t1.SELECTION_CRITERIA 
# MAGIC FROM AllReviewsPhase3 t1
# MAGIC --LEFT JOIN radar.cases on t1.sourceclient = t2.sourceclient
# MAGIC GROUP BY t1.SELECTION_CRITERIA
# MAGIC )
# MAGIC select SUM(`COUNT`) FROM CTE1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(t1.sourceclient)) AS COUNT
# MAGIC FROM AllReviewsPhase3 t1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4: Balanced Selection
# MAGIC
# MAGIC **flow of logic**
# MAGIC 1. select one from each rule at random
# MAGIC   - set status 'SelectedFlag' as 1
# MAGIC   
# MAGIC Now until All files have been selected:
# MAGIC - 2. check balance for Sectorteams
# MAGIC   - select a file from high risk for rebalance sector team
# MAGIC   - should be relative to number of completion per sector
# MAGIC - 3. Get 2 files per location
# MAGIC   - Argentina has been transferred to South America (brazil)
# MAGIC - 4. check balance for PR / ONB
# MAGIC  - should be relative to number of completion per sector
# MAGIC - 5. check balance for Risk level - select a file from lowest nr
# MAGIC   - should be relative to number of completion per sector
# MAGIC - 6. Balance TEA reasons, pick new TEA
# MAGIC   - should be relative to number of completion per sector
# MAGIC
# MAGIC At the end
# MAGIC - count per QC analyst
# MAGIC - if too many for some analysts, remove files and pick new at random
# MAGIC
# MAGIC Minimum 2 files per Location (if available)
# MAGIC Balance risk level (High, Medium, Low)
# MAGIC Balance Review Type (ONB/PR)
# MAGIC Balance Sector Team
# MAGIC Balance QC: not more than 3x same QC per month
# MAGIC Balance reasons for TEA: change in: 
# MAGIC Higher TEA prio; structure
# MAGIC           , adverse media / ONS
# MAGIC           , transaction risk 
# MAGIC
# MAGIC Lower TEA prio; data change, new client name.
# MAGIC     

# COMMAND ----------

# AllReviewsPhase3
# radar.cases
# SelectionScope

# COMMAND ----------

# DBTITLE 1,Create Final Curated Version of SelectionScope
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SelectionScope_Final AS
# MAGIC
# MAGIC -- this deliberately creates duplicates for some cases that have multiple risk indicators, so increase the likely hood of them being selected.
# MAGIC select t1.* , t2.SELECTION_CRITERIA
# MAGIC From SelectionScope t1
# MAGIC LEFT JOIN AllReviewsPhase3 as t2 on t1.SourceClient = t2.SourceClient
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SelectionScope_RiskBased AS
# MAGIC
# MAGIC -- this deliberately creates duplicates for some cases that have multiple risk indicators, so increase the likely hood of them being selected.
# MAGIC select t1.* 
# MAGIC From SelectionScope t1
# MAGIC where t1.Sourceclient in (Select distinct Sourceclient from AllReviewsPhase3)

# COMMAND ----------

# DBTITLE 1,Initiate 1 file per rule for each rule
df_RiskBasedSelection = spark.sql('select * from AllReviewsPhase3').toPandas()

all_rules = df_RiskBasedSelection.SELECTION_CRITERIA.unique()
df_selectionscope = spark.sql('select * from SelectionScope where ClientId in (select distinct clientId from SelectionScope_Final)').toPandas()

df_SelectionScope_Final = spark.sql('select * from SelectionScope_Final').toPandas()
CurrentSelection = pd.DataFrame().reindex(columns=df_selectionscope.columns)

#for rule in all_rules:
    # select random file from rules
    # apply SelectedFlag to file
    # add file to CurrentSelection

# COMMAND ----------

#df_selectionscope.head()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding files in the scope
# MAGIC - EDR
# MAGIC - TEA
# MAGIC - risk-based selection
# MAGIC - random selection
# MAGIC - EA

# COMMAND ----------

# we do not want PR's of rules that we already selected.
# we should spreak accross QC analysts.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create dict for dispersion per group

# COMMAND ----------

LIST_OF_DISPERSION_ATTRIBUTES = ['SectorTeam','CaseReviewType', 'ValidatedRiskLevel', 'KYCDepartment', 'GlobalClientOwnerLocation']#, 'QC analyst'] # add QC analyst. 
# 'TEA_Review_Reason_Derived'] # not too much Control measure
# 'TEA Derived.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 100-random-selection mechanism

# COMMAND ----------

# DBTITLE 1,Function to calculate relative distribution of attributes
# Determine dispersion for selection scope
def calc_attribute_dispersion(dataframe, LIST_OF_ATTRIBUTES):
    tot_dict = {}
    len_df = len(dataframe)
    for element in LIST_OF_DISPERSION_ATTRIBUTES:
        dataframe.groupby(element).count()

        df_abs = dataframe[['SourceSystem', element]].groupby(element).count()
        df_abs[element + '_rel'] = df_abs['SourceSystem'].apply(lambda x: x/len_df)

        dict_rel = df_abs[[element + '_rel']].to_dict()[element + '_rel']
        tot_dict[element] = dict_rel

    return tot_dict

# COMMAND ----------

# DBTITLE 1,Function to compare 2 relative distributions

def compare_attribute_dispersion(tot_dict, sub_dict):
    dist = 0
    attribute_dict = {key: ''  for key in tot_dict.keys()}

    for attribute_key in tot_dict.keys():
        dict1 = tot_dict[attribute_key]
        dict2 = sub_dict[attribute_key]
        
        # print(attribute_key)
        # print(dict1)
        # print(dict2)


        #get abs distances per key
        result = {key: abs(dict1[key] - dict2.get(key, 0)) for key in dict1}
        # print(result)

        # add abs distances per key
        sub_res = sum(result.values())
        # print(sub_res)
        # TODO: a key with many variables could have a larger dispersion (?). Should we compensate for a key with many variables ?
        

        # return unique number for abs distances add to attr_dict
        attribute_dict[attribute_key] = sub_res
    
    # attribute_dict:

    return attribute_dict


# COMMAND ----------

# MAGIC %md
# MAGIC #### Add 2 TEA to selectionscope and remove the rest

# COMMAND ----------

# DBTITLE 1,Get TEA id's to include
# Assigning UK TEA id
UK_TEA = df_selectionscope[(df_selectionscope.CaseReviewType == 'Tailored Event Assessment') &  (df_selectionscope.KYCDepartment == 'Corp CDD Hub UK')].sort_values(['TEA_Review_Reason_Derived'])

# Taking into account the option there might be zero
if len(UK_TEA) > 1:
    UK_TEA_ALL = UK_TEA[0:2].SourceClient.values[0:2]
    UK_TEA_ID_1 = UK_TEA_ALL[0]
    UK_TEA_ID_2 = UK_TEA_ALL[1]
    df_selectionscope.loc[df_selectionscope.SourceClient == UK_TEA_ID_1, 'SelectionIncludeFlag'] = 1
    df_selectionscope.loc[df_selectionscope.SourceClient == UK_TEA_ID_2, 'SelectionIncludeFlag'] = 1
elif len(UK_TEA) > 0:
    UK_TEA_ALL = UK_TEA[0:1].SourceClient.values[0:1]
    UK_TEA_ID_1 = UK_TEA_ALL[0]
    df_selectionscope.loc[df_selectionscope.SourceClient == UK_TEA_ID_1, 'SelectionIncludeFlag'] = 1



# Assigning NL_TEA_ID
NL_TEA = df_selectionscope[(df_selectionscope.CaseReviewType == 'Tailored Event Assessment') &  (df_selectionscope.KYCDepartment == 'Corp CDD Hub NL')].sort_values(['TEA_Review_Reason_Derived'])

# Taking into account the option there might be zero
if len(NL_TEA) > 1:
    NL_TEA_ALL = NL_TEA[0:2].SourceClient.values[0:2]
    NL_TEA_ID_1 = NL_TEA_ALL[0]
    NL_TEA_ID_2 = NL_TEA_ALL[1]
    df_selectionscope.loc[df_selectionscope.SourceClient == NL_TEA_ID_1, 'SelectionIncludeFlag'] = 1
    df_selectionscope.loc[df_selectionscope.SourceClient == NL_TEA_ID_2, 'SelectionIncludeFlag'] = 1
elif len(NL_TEA) > 0:
    NL_TEA_ALL = NL_TEA[0:1].SourceClient.values[0:1]
    NL_TEA_ID_1 = NL_TEA_ALL[0]
    df_selectionscope.loc[df_selectionscope.SourceClient == NL_TEA_ID_1, 'SelectionIncludeFlag'] = 1


# COMMAND ----------

# DBTITLE 1,Excluding all other TEAs
# Exclude rest of TEA's from selection
df_selectionscope.loc[(df_selectionscope.SelectionIncludeFlag != 1) & (df_selectionscope.CaseReviewType == 'Tailored Event Assessment'), 'SelectionExcludeFlag'] = 1

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Add 4 Event Assessment
# MAGIC Since Okt 10, this review type is replacing TEA
# MAGIC
# MAGIC We want spread across UK and NL
# MAGIC We want spreak across EA reason

# COMMAND ----------

# Assigning UK EA id
UK_EA = df_selectionscope[(df_selectionscope.CaseReviewType == 'Event Assessment') &  (df_selectionscope.KYCDepartment == 'Corp CDD Hub UK')].sort_values(['EA_Review_Reason_Derived'])

# Taking into account the option there might be zero
if len(UK_EA) > 1:
    UK_EA_ALL = UK_EA[0:2].SourceClient.values[0:2]
    UK_EA_ID_1 = UK_EA_ALL[0]
    UK_EA_ID_2 = UK_EA_ALL[1]
    df_selectionscope.loc[df_selectionscope.SourceClient == UK_EA_ID_1, 'SelectionIncludeFlag'] = 1
    df_selectionscope.loc[df_selectionscope.SourceClient == UK_EA_ID_2, 'SelectionIncludeFlag'] = 1
elif len(UK_EA) > 0:
    UK_EA_ALL = UK_EA[0:1].SourceClient.values[0:1]
    UK_EA_ID_1 = UK_EA_ALL[0]
    df_selectionscope.loc[df_selectionscope.SourceClient == UK_EA_ID_1, 'SelectionIncludeFlag'] = 1



# Assigning NL_TEA_ID
NL_EA = df_selectionscope[(df_selectionscope.CaseReviewType == 'Event Assessment') &  (df_selectionscope.KYCDepartment == 'Corp CDD Hub NL')].sort_values(['EA_Review_Reason_Derived'])

# Taking into account the option there might be zero
if len(NL_EA) > 1:
    NL_EA_ALL = NL_EA[0:2].SourceClient.values[0:2]
    NL_EA_ID_1 = NL_EA_ALL[0]
    NL_EA_ID_2 = NL_EA_ALL[1]
    df_selectionscope.loc[df_selectionscope.SourceClient == NL_EA_ID_1, 'SelectionIncludeFlag'] = 1
    df_selectionscope.loc[df_selectionscope.SourceClient == NL_EA_ID_2, 'SelectionIncludeFlag'] = 1
elif len(NL_EA) > 0:
    NL_EA_ALL = NL_EA[0:1].SourceClient.values[0:1]
    NL_EA_ID_1 = NL_EA_ALL[0]
    df_selectionscope.loc[df_selectionscope.SourceClient == NL_EA_ID_1, 'SelectionIncludeFlag'] = 1
#else:
#    #select from UK instead of NL if available
#    NL_EA_ID_1 = df_selectionscope[(df_selectionscope.CaseReviewType == 'Event Assessment') &  (df_selectionscope.KYCDepartment == 'Corp CDD Hub UK') & #df_selectionscope.SelectionIncludeFlag != 1].sort_values(['EA_Review_Reason_Derived'])[0:1].SourceClient.values[0]



# COMMAND ----------

# Exclude rest of TEA's from selection
df_selectionscope.loc[(df_selectionscope.SelectionIncludeFlag != 1) & (df_selectionscope.CaseReviewType == 'Event Assessment'), 'SelectionExcludeFlag'] = 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add 2 EDR to selection scope and still allow the rest

# COMMAND ----------

# DBTITLE 1,Select 2 EDR
TWO_EDR_ID = df_selectionscope[(df_selectionscope.CaseReviewType == 'Event Driven Review')][0:2].SourceClient.values
if len(TWO_EDR_ID) > 0:
    df_selectionscope.loc[df_selectionscope.SourceClient == TWO_EDR_ID[0], 'SelectionIncludeFlag'] = 1
if len(TWO_EDR_ID) > 1:
    df_selectionscope.loc[df_selectionscope.SourceClient == TWO_EDR_ID[1], 'SelectionIncludeFlag'] = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure 2 onboards in total selection

# COMMAND ----------

# DBTITLE 1,Select 2 ONB
TWO_ONB_ID = df_selectionscope[(df_selectionscope.CaseReviewType == 'Initial On-Boarding')][0:2].SourceClient.values


df_selectionscope.loc[df_selectionscope.SourceClient == TWO_ONB_ID[0], 'SelectionIncludeFlag'] = 1
df_selectionscope.loc[df_selectionscope.SourceClient == TWO_ONB_ID[1], 'SelectionIncludeFlag'] = 1

# COMMAND ----------

len(df_selectionscope.KYCGroup.unique())

# COMMAND ----------

#df_selectionscope[((df_selectionscope.SelectionIncludeFlag != 1) & (df_selectionscope.SelectionExcludeFlag != 1) & (df_selectionscope.KYCDepartment == 'Corp CDD Hub NL'))]

# COMMAND ----------

# DBTITLE 1,Make 100 selections - apply rules
"""RUNNING SELECTION
the below runs the actual selection
"""

selection_base = df_selectionscope[df_selectionscope.SelectionIncludeFlag == 1]

# Now removing all TEA from selection scope
#NR_FILES_TO_SELECT_NL = 8
#NR_FILES_TO_SELECT_UK = 8

# seeing how many left to select
#NR_ALREADY_SELECTED_NL = len(selection_base[selection_base.KYCDepartment == 'Corp CDD Hub UK'])
#NR_ALREADY_SELECTED_UK = len(selection_base[selection_base.KYCDepartment == 'Corp CDD Hub NL'])
NR_ALREADY_SELECTED_NL = len(selection_base[selection_base.KYCDepartment == 'Corp CDD Hub NL'])
NR_ALREADY_SELECTED_UK = len(selection_base[selection_base.KYCDepartment == 'Corp CDD Hub UK'])

NR_LEFT_TO_SELECT_NL = (NR_FILES_TO_SELECT_NL - NR_ALREADY_SELECTED_NL)
NR_LEFT_TO_SELECT_UK = (NR_FILES_TO_SELECT_UK - NR_ALREADY_SELECTED_UK)

#calculate dispersion of entire selection.
tot_dict = calc_attribute_dispersion(df_selectionscope, 
                                     LIST_OF_DISPERSION_ATTRIBUTES) 


# createEmptyDF to append all selections in. 
df_AllSelections = pd.DataFrame().reindex(columns=df_selectionscope.columns)

# add column for 'Iteration'.
df_AllSelections['Iteration'] = 0
iteration_dict = {}

if (NR_LEFT_TO_SELECT_NL > 0) & (NR_LEFT_TO_SELECT_UK > 0):
    for i in range(0,1000):
        # get files to add to base selection
        # select_add = df_SelectionScope_Final[(df_SelectionScope_Final.SelectionIncludeFlag != 1) & (df_SelectionScope_Final.SelectionExcludeFlag != 1)].sample(n= NR_LEFT_TO_SELECT )
        select_add_NL = df_selectionscope[((df_selectionscope.SelectionIncludeFlag != 1) & 
                                           (df_selectionscope.SelectionExcludeFlag != 1) & 
                                           (df_selectionscope.KYCDepartment == 'Corp CDD Hub NL'))].sample(n= NR_LEFT_TO_SELECT_NL )

        select_add_UK = df_selectionscope[((df_selectionscope.SelectionIncludeFlag != 1) &
                                           (df_selectionscope.SelectionExcludeFlag != 1) &
                                           (df_selectionscope.KYCDepartment == 'Corp CDD Hub UK'))].sample(n= NR_LEFT_TO_SELECT_UK )


        # add the Selection Include to the sample
        selection = pd.concat([selection_base,
                                select_add_NL, select_add_UK] )


        dict_sel = calc_attribute_dispersion(selection, 
                                        LIST_OF_DISPERSION_ATTRIBUTES)
        
        comp = compare_attribute_dispersion(tot_dict, dict_sel)


        #Store i, which files were selected. 
        selection['Iteration'] = i
        selection['DistinctGroups'] = len(selection.KYCGroup.unique())
        selection['AtrributeDispersionValue'] = sum(comp.values())

        df_AllSelections = pd.concat([df_AllSelections, selection])

        #store in dict: i and the number of distance to the optimal.
        iteration_dict[i] = sum(comp.values())
        print(i, end = '\r' )
    
else:
    print('amount of base selection exceeds total selection')

    #act as if only 0th iteration of loop
    i = 0

    #create a df_All_selections 
    selection = selection_base.loc[:,:]
    selection.loc[:,'Iteration'] = i
    df_AllSelections = selection.loc[:,:]

    #doing the dict items
    dict_sel = calc_attribute_dispersion(selection, 
                                        LIST_OF_DISPERSION_ATTRIBUTES)
    comp = compare_attribute_dispersion(tot_dict, dict_sel)
    iteration_dict[i] = sum(comp.values())

    # check distinct groups per iteration.
    

# COMMAND ----------

# DBTITLE 1,Make 100 selections - apply rules - by Abhishek
# MAGIC %skip
# MAGIC """RUNNING SELECTION
# MAGIC the below runs the actual selection
# MAGIC """
# MAGIC
# MAGIC selection_base = df_selectionscope[df_selectionscope.SelectionIncludeFlag == 1]
# MAGIC
# MAGIC #FIX 1: Correct UK/NL counts (was swapped earlier)
# MAGIC NR_ALREADY_SELECTED_NL = len(selection_base[selection_base.KYCDepartment == 'Corp CDD Hub NL'])
# MAGIC NR_ALREADY_SELECTED_UK = len(selection_base[selection_base.KYCDepartment == 'Corp CDD Hub UK'])
# MAGIC
# MAGIC NR_LEFT_TO_SELECT_NL = (NR_FILES_TO_SELECT_NL - NR_ALREADY_SELECTED_NL)
# MAGIC NR_LEFT_TO_SELECT_UK = (NR_FILES_TO_SELECT_UK - NR_ALREADY_SELECTED_UK)
# MAGIC
# MAGIC # calculate dispersion of entire selection.
# MAGIC tot_dict = calc_attribute_dispersion(df_selectionscope, LIST_OF_DISPERSION_ATTRIBUTES)
# MAGIC
# MAGIC # createEmptyDF to append all selections in.
# MAGIC df_AllSelections = pd.DataFrame().reindex(columns=df_selectionscope.columns)
# MAGIC
# MAGIC # add column for 'Iteration'.
# MAGIC df_AllSelections['Iteration'] = 0
# MAGIC iteration_dict = {}
# MAGIC
# MAGIC #Prepare available pools once
# MAGIC available_NL = df_selectionscope[
# MAGIC     (df_selectionscope.SelectionIncludeFlag != 1) &
# MAGIC     (df_selectionscope.SelectionExcludeFlag != 1) &
# MAGIC     (df_selectionscope.KYCDepartment == 'Corp CDD Hub NL')
# MAGIC ]
# MAGIC
# MAGIC available_UK = df_selectionscope[
# MAGIC     (df_selectionscope.SelectionIncludeFlag != 1) &
# MAGIC     (df_selectionscope.SelectionExcludeFlag != 1) &
# MAGIC     (df_selectionscope.KYCDepartment == 'Corp CDD Hub UK')
# MAGIC ]
# MAGIC
# MAGIC if (NR_LEFT_TO_SELECT_NL > 0) & (NR_LEFT_TO_SELECT_UK > 0):
# MAGIC
# MAGIC     for i in range(0, 1000):
# MAGIC
# MAGIC         #FIX 2: Cap selection to available records
# MAGIC         n_NL = min(NR_LEFT_TO_SELECT_NL, len(available_NL))
# MAGIC         n_UK = min(NR_LEFT_TO_SELECT_UK, len(available_UK))
# MAGIC
# MAGIC         #Sampling safely
# MAGIC         select_add_NL = available_NL.sample(n=n_NL, random_state=i)
# MAGIC         select_add_UK = available_UK.sample(n=n_UK, random_state=i)
# MAGIC
# MAGIC         #combine with base
# MAGIC         selection = pd.concat([selection_base, select_add_NL, select_add_UK])
# MAGIC
# MAGIC         dict_sel = calc_attribute_dispersion(selection, LIST_OF_DISPERSION_ATTRIBUTES)
# MAGIC         comp = compare_attribute_dispersion(tot_dict, dict_sel)
# MAGIC
# MAGIC         # Store iteration data
# MAGIC         selection['Iteration'] = i
# MAGIC         selection['DistinctGroups'] = len(selection.KYCGroup.unique())
# MAGIC         selection['AtrributeDispersionValue'] = sum(comp.values())
# MAGIC
# MAGIC         df_AllSelections = pd.concat([df_AllSelections, selection])
# MAGIC
# MAGIC         iteration_dict[i] = sum(comp.values())
# MAGIC         print(i, end='\r')
# MAGIC
# MAGIC else:
# MAGIC     print('amount of base selection exceeds total selection')
# MAGIC
# MAGIC     i = 0
# MAGIC
# MAGIC     selection = selection_base.loc[:, :]
# MAGIC     selection.loc[:, 'Iteration'] = i
# MAGIC     df_AllSelections = selection.loc[:, :]
# MAGIC
# MAGIC     dict_sel = calc_attribute_dispersion(selection, LIST_OF_DISPERSION_ATTRIBUTES)
# MAGIC     comp = compare_attribute_dispersion(tot_dict, dict_sel)
# MAGIC     iteration_dict[i] = sum(comp.values())

# COMMAND ----------

df_AllSelections.DistinctGroups.unique()

# COMMAND ----------

# DBTITLE 1,Get the right key
#get keys with lowest target metric
keys = [k for k, v in iteration_dict.items() if v == min(iteration_dict.values())]
print(iteration_dict[keys[0]])



# COMMAND ----------

# DBTITLE 1,Show the best performing selection on the target metrics
#display the output with the right key.
display(df_AllSelections[df_AllSelections.Iteration == keys[0]])

# COMMAND ----------

# MAGIC %md
# MAGIC ### save for powerbi selection

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS FLM

# COMMAND ----------

SelectionColumns = [
 'SampleStartPeriod'
, 'SampleEndPeriod'
, 'SampleScope'
, 'KYCDepartment'
, 'GcobId'
, 'FullLegalName'
, 'KYCGroup'
, 'GlobalClientOwnerLocation'
# , 'InvolvedLocations'
, 'ValidatedRiskLevel'
, 'CaseReviewType'
, 'Prework'
, 'CaseCompletedDate'
, 'Preworkanalyst'
, 'Assessmentanalyst'
, '4EYEanalyst'
, 'GlobalClientOwner'
, 'SectorTeam']
# TODO: SampleMechanism



# COMMAND ----------

df_best_sample = df_AllSelections[df_AllSelections.Iteration == keys[0]]

df_best_sample['SampleStartPeriod'] = StartOfDateRange
df_best_sample['SampleEndPeriod'] = EndOfDateRange
df_best_sample['SampleScope'] = 'CORP UK-NL'
#df_best_sample[['TEAReviewReasonDescription']].fillna('none', inplace = True)

sdf_best_sample = spark.createDataFrame(df_best_sample[SelectionColumns])

#spark.sql("DROP TABLE FLM.samples2")


sdf_best_sample.write.mode('append').option("mergeSchema", "true").saveAsTable('FLM.samples2')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FLM.samples2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Visualize. 
# MAGIC - step 1. Get out of the Original selection, the devision over key characteristics
# MAGIC - step 2. Get for the sample, the division of key characteristics.
# MAGIC - step 3. combine together
# MAGIC - step 4. show in a plot.

# COMMAND ----------

# MAGIC %md
# MAGIC ### For doing the selection ourselves

# COMMAND ----------

# DBTITLE 1,SectorBalance
# # Balance definition
# SectorBalance = spark.sql("""select t1.SectorTeam , count(t2.GcobId) AS TotalPerSector
#                           from (select distinct SectorTeam from AllReviewsPhase3)  t1
#                           left join selectionclients t2 on t1.SectorTEam = t2.SectorTeam
#                           where t2.SelectedFlag = 1
#                           GROUP BY SectorTeam""")

# # TODO: Compare to relative division of all relevant sectors in the selection. Then take the least (relative) represented sector

# COMMAND ----------

# DBTITLE 1,ReviewTypeBalance
# #
# ReviewTypeBalance = spark.sql("""select t1.ReviewType , count(t2.GcobId) AS TotalPerReviewType
#                           from (select distinct ReviewType from AllReviewsPhase3)  t1
#                           left join selectionclients t2 on t1.ReviewType = t2.ReviewType
#                           where t2.SelectedFlag = 1
#                           GROUP BY ReviewType""")

# COMMAND ----------

# DBTITLE 1,RiskLevelBalance
# RiskLevelBalance = spark.sql("""select t1.ValidatedRiskLevel , count(t2.GcobId) AS TotalPerRiskLevel
#                           from (select distinct ValidatedRiskLevel from AllReviewsPhase3)  t1
#                           left join selectionclients t2 on t1.ValidatedRiskLevel = t2.ValidatedRiskLevel
#                           where t2.SelectedFlag = 1
#                           GROUP BY ValidatedRiskLevel""")

# COMMAND ----------

# DBTITLE 1,TEA reason balance
# # We can balance TEA reason on them Having a comment on 'Has structure%' OR ReviewReason like '%adverse media%' OR ReviewReason like '%transaction in the name."
# TEA_Reason = spark.sql("""select t1.ValidatedRiskLevel , count(t2.GcobId) AS TotalPerRiskLevel
#                           from (select distinct ValidatedRiskLevel from AllReviewsPhase3)  t1
#                           left join selectionclients t2 on t1.ValidatedRiskLevel = t2.ValidatedRiskLevel
#                           where t2.SelectedFlag = 1
#                           GROUP BY ValidatedRiskLevel""")
