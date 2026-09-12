# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal: 
# MAGIC To assign/flag for each review case that have increased risks of having gone wrong, these elements.
# MAGIC
# MAGIC ###### Author
# MAGIC init: Ruud van Laar
# MAGIC
# MAGIC ### Selection scope.
# MAGIC Monthly selection; 
# MAGIC
# MAGIC Fi-rules and; 16 FI, 3 TEA's.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Monthly Selection. Has different numbers
# MAGIC Monthly Selection has more files. 
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC  Developer   |  Date          |  PBI No.     |   Changes done
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal    |   31-March-2026   |  15572506    |   Rabobank Foundation data in FLM Selection Report
# MAGIC | Abhishek Jaiswal    |   21-May-2026   |  16157360    | Add RCI (Rabobank Corporate Investment) as a separate bucket in the  Monthly Selection
# MAGIC | Abhishek Jaiswal    |   26-May-2026   |  16156012    | Bugs in selection - Suzana confirmed over email to include EDR for all selection buckets

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO
# MAGIC - TEA reason division lacking
# MAGIC - at low samples, dividing over sector teams doesnt work
# MAGIC - BestPRBeforeOffboarding
# MAGIC - 2025-05-15
# MAGIC   - No Agency TEA's required / no RMA selection
# MAGIC   - updated rules table
# MAGIC   - was missing EDR's on the rule. 
# MAGIC     - --> find out what went wrong
# MAGIC - 2025 - 08- 01
# MAGIC   - include the Rabo Foundation files with a rule  2 files to be selected
# MAGIC   - Why there was no London files selected in the last monthly selection
# MAGIC - make sure no double groups.
# MAGIC - Limit Correspondent Banking. Will be gone in July anyway. 
# MAGIC - Files on control measures.
# MAGIC   - Control measures dashboard -> look on completed. From dashboard try to search for files with completed measures
# MAGIC   - 10 files with completed measures. 
# MAGIC   - .....
# MAGIC - Try to diversify locations more
# MAGIC

# COMMAND ----------

# MAGIC %md
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

# DBTITLE 1,import libraries
import pandas as pd
import os
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType


# COMMAND ----------

# DBTITLE 1,Stop if not the first of the month
today = datetime.today().weekday() # Monday is 0, Sunday is 6
day_of_month = datetime.today().day

# Only run on Monday
if day_of_month != 1:
    print(f"Not first of month. Skipping task. daynumber = {day_of_month}")
    dbutils.notebook.exit('stopping not first of the month')

# COMMAND ----------

# DBTITLE 1,SET Notebook VARIABLES
NR_FILES_TO_SELECT = 20
NR_FILES_TO_SELECT_RANDOM = 4
TEA_TO_SELECT = 5
EA_TO_SELECT =5

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initiate connect to GDP

# COMMAND ----------

# DBTITLE 1,Get environment variables
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,Connect to GDP
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
, 'Party_AllParty_LocationCoverage'
, 'party_control_measures'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{row.GDPname}/102/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view involved_location as
# MAGIC select UniquePartyId, collect_set(Location) as involved_loc from Party_AllParty_LocationCoverage where LeadOrInvolved='Involved' group by UniquePartyId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Smaller su-selections
# MAGIC - Croeesp banking
# MAGIC - RMA
# MAGIC - KYC SectorTeam correspondent Banking
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,RMA only.
# MAGIC %sql
# MAGIC -- SELECTION FOR RMA
# MAGIC CREATE OR REPLACE TEMP VIEW RMA_ONLY_PARTIES AS
# MAGIC
# MAGIC     WITH All_Products AS
# MAGIC     (
# MAGIC         select ClientId
# MAGIC         , count(*) as count_all_products 
# MAGIC         from party_products_and_services 
# MAGIC         where productLifecycleStatus = 'Active'
# MAGIC         group by ClientId
# MAGIC     )
# MAGIC
# MAGIC     ,All_Products_RMA AS
# MAGIC     (
# MAGIC         select ClientId
# MAGIC         , count(*) as count_RMA_products 
# MAGIC         from party_products_and_services 
# MAGIC         where ProductName IN ('RMA Only (Non Product)', 'TCF/TF RMA')
# MAGIC         and productLifecycleStatus = 'Active'
# MAGIC         group by ClientId
# MAGIC     )
# MAGIC -- the below selects only clientIds where all products are RMA products.
# MAGIC
# MAGIC select t1.ClientId, t1.count_all_products, t2.count_RMA_products from 
# MAGIC All_Products as t1
# MAGIC inner join All_Products_RMA as t2 on t1.clientId = t2.ClientId and t1.count_all_products = t2.count_RMA_products

# COMMAND ----------

# DBTITLE 1,def corresp banking
# MAGIC %sql
# MAGIC -- SELECTION FOR CORRESPONDENT BANKING
# MAGIC CREATE OR REPLACE TEMP VIEW CORRESPONDENT_BANKING_PARTIES AS
# MAGIC
# MAGIC select * from party_products_and_services 
# MAGIC where ProductName like '%Loro%'
# MAGIC OR ProductName like 'Correspondent Relationship (with third party payments)'

# COMMAND ----------

# DBTITLE 1,set dates for start and end of period
currentdate = datetime.now()
EndOfDateRange = (currentdate.replace(day=1) -timedelta(1)).date()
EndOfDateRange_str = EndOfDateRange.strftime('%Y-%m-%d')

print('EndOfDateRange :', EndOfDateRange)
StartOfDateRange = EndOfDateRange.replace(day=1)
StartOfDateRange_str = StartOfDateRange.strftime('%Y-%m-%d')

print('StartOfDateRange :', StartOfDateRange)

# COMMAND ----------

# DBTITLE 1,DEFINE THE SELECTION SCOPE FI
# This defines the selection scope and 

spark.sql(f""" 
          
Select t1.*,
 CASE 
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%structure%' THEN 'Structure'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%adverse media%' THEN 'AM'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%transaction%' THEN 'Transaction'
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
   --WHEN  t1.CaseReviewType = 'Event Driven Review' THEN 1
   WHEN  t1.ValidatedRiskLevel = 'Unacceptable' THEN 1
   END AS SelectionIncludeFlag

, CASE 
   WHEN 1=1 THEN 0
   END AS SelectionExcludeFlag

 from radar.cases t1 
 left join radar.clients t2 on t1.UniqueGcobId = t2.uniquegcobid
 left anti join RMA_ONLY_PARTIES as t3 on t1.clientid = t3.clientid -- excluding RMA from default.

where t1.CaseCompletedDate between date('{StartOfDateRange_str}') and date('{EndOfDateRange_str}')
AND t1.CaseReviewType NOT IN ( 'Amendment', 'Change of Client Owner', 'Offboarding')
-- un-select FLM TEA's.
AND ( 
      (t1.TEAReviewReasonDescription is null ) 
      OR 
      ((lower(t1.ReviewReason) not like '%flm%'
         AND lower(t1.ReviewReason) not like '%slm%'
         AND lower(t1.ReviewReason) not like '%repair%'
         AND lower(t1.ReviewReason) not like '%remediation%'
         AND lower(t1.ReviewReason) not like '%FATCA%'
         AND lower(t1.ReviewReason) not like '%Client Owner%'
         AND lower(t1.ReviewReason) not like '%Change Location%'
         AND lower(t1.ReviewReason) not like '%correction%' )))
   -- NO review reason as 'change location', 
   -- NO review reason 'change of client owner'. 
   -- no review reason Fatca / CRS.
-- only FI hub-reviewed files         
AND t1.KYCDepartment IN ('Global FI CDD Hub')
-- TODO 11-24; exclude acorn, corresp banking, , PSP here
AND t2.SectorTeam not in ('Acorn', 'PSP Coverage')

""").createOrReplaceTempView('SelectionScope')


# COMMAND ----------

# DBTITLE 1,SELECTION SCOPE ACORN
# This defines the selection scope and 

spark.sql(f""" 
          
Select t1.*,
 CASE 
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%structure%' THEN 'Structure'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%adverse media%' THEN 'AM'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%transaction%' THEN 'Transaction'
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
   --WHEN  t1.CaseReviewType = 'Event Driven Review' THEN 1
   WHEN  t1.ValidatedRiskLevel = 'Unacceptable' THEN 1
   END AS SelectionIncludeFlag

, CASE 
   WHEN 1=1 THEN 0
   END AS SelectionExcludeFlag

 from radar.cases t1 
 left join radar.clients t2 on t1.UniqueGcobId = t2.uniquegcobid
where t1.CaseCompletedDate between date('{StartOfDateRange_str}') and date('{EndOfDateRange_str}')
AND t1.CaseReviewType NOT IN ( 'Amendment', 'Change of Client Owner', 'Offboarding')
--Based on email confirmation from Suzana on 26-May-2026 including 'Event Driven Review' and removing from CaseReviewType
-- un-select FLM TEA's.
AND ( 
   (t1.TEAReviewReasonDescription is null ) 
   OR 
      ((lower(t1.ReviewReason) not like '%flm%'
AND lower(t1.ReviewReason) not like '%slm%'
AND lower(t1.ReviewReason) not like '%repair%'
AND lower(t1.ReviewReason) not like '%remediation%'
AND lower(t1.ReviewReason) not like '%correction%' )))
AND t2.SectorTeam = 'Acorn' 

""").createOrReplaceTempView('SelectionScopeAcorn')

# COMMAND ----------

# DBTITLE 1,SELECTION SCOPE PSP
# This defines the selection scope and 

spark.sql(f""" 
          
Select t1.*,
 CASE 
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%structure%' THEN 'Structure'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%adverse media%' THEN 'AM'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%transaction%' THEN 'Transaction'
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
   --WHEN  t1.CaseReviewType = 'Event Driven Review' THEN 1
   WHEN  t1.ValidatedRiskLevel = 'Unacceptable' THEN 1
   -- Prioritize Derdengelden over BV.
   WHEN t1.FullLegalName LIKE '%Derdengelden%' THEN 1
   END AS SelectionIncludeFlag

, CASE 
   WHEN 1=1 THEN 0
   END AS SelectionExcludeFlag

 from radar.cases t1 
 left join radar.clients t2 on t1.UniqueGcobId = t2.uniquegcobid
where t1.CaseCompletedDate between date('{StartOfDateRange_str}') and date('{EndOfDateRange_str}')
AND t1.CaseReviewType NOT IN ( 'Amendment', 'Change of Client Owner', 'Offboarding')
--Based on email confirmation from Suzana on 26-May-2026 including 'Event Driven Review' and removing from CaseReviewType
-- un-select FLM TEA's.
AND ( 
   (t1.TEAReviewReasonDescription is null ) 
   OR 
      ((lower(t1.ReviewReason) not like '%flm%'
AND lower(t1.ReviewReason) not like '%slm%'
AND lower(t1.ReviewReason) not like '%repair%'
AND lower(t1.ReviewReason) not like '%remediation%'
AND lower(t1.ReviewReason) not like '%correction%' )))
AND t2.SectorTeam = 'PSP Coverage'

""").createOrReplaceTempView('SelectionScopePSP')

# COMMAND ----------

# DBTITLE 1,Selection scope RMA only

# This defines the selection scope and 

spark.sql(f""" 
          
Select t1.*,
 CASE 
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%structure%' THEN 'Structure'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%adverse media%' THEN 'AM'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%transaction%' THEN 'Transaction'
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
   --WHEN  t1.CaseReviewType = 'Event Driven Review' THEN 1
   WHEN  t1.ValidatedRiskLevel = 'Unacceptable' THEN 1
   END AS SelectionIncludeFlag

, CASE 
   WHEN 1=1 THEN 0
   END AS SelectionExcludeFlag

 from radar.cases t1 
 left join radar.clients t2 on t1.UniqueGcobId = t2.uniquegcobid
inner join RMA_ONLY_PARTIES as t3 on t1.ClientId = t3.ClientId

where t1.CaseCompletedDate between date('{StartOfDateRange_str}') and date('{EndOfDateRange_str}')
AND t1.CaseReviewType NOT IN ( 'Amendment', 'Change of Client Owner', 'Offboarding', 'Event Assessment')
--Based on email confirmation from Suzana on 26-May-2026 including 'Event Driven Review' and removing from CaseReviewType
-- un-select FLM TEA's.
AND ( 
   (t1.TEAReviewReasonDescription is null ) 
   OR 
      ((lower(t1.ReviewReason) not like '%flm%'
AND lower(t1.ReviewReason) not like '%slm%'
AND lower(t1.ReviewReason) not like '%repair%'
AND lower(t1.ReviewReason) not like '%remediation%'
AND lower(t1.ReviewReason) not like '%correction%' )))
 

""").createOrReplaceTempView('SelectionScopeRMA')

# COMMAND ----------

# DBTITLE 1,Selection scope Rabo Foundation
# This defines the selection scope and 

spark.sql(f""" 
          
Select t1.*,
 CASE 
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%structure%' THEN 'Structure'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%adverse media%' THEN 'AM'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%transaction%' THEN 'Transaction'
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
   --WHEN  t1.CaseReviewType = 'Event Driven Review' THEN 1
   WHEN  t1.ValidatedRiskLevel = 'Unacceptable' THEN 1
   -- Prioritize Derdengelden over BV.
   WHEN t1.FullLegalName LIKE '%Derdengelden%' THEN 1
   END AS SelectionIncludeFlag

, CASE 
   WHEN 1=1 THEN 0
   END AS SelectionExcludeFlag

 from radar.cases t1 
 inner join radar.clients t2 on t1.UniqueGcobId = t2.uniquegcobid
where t1.CaseCompletedDate between date('{StartOfDateRange_str}') and date('{EndOfDateRange_str}')
AND t1.CaseReviewType NOT IN ( 'Amendment', 'Change of Client Owner', 'Offboarding')
--Based on email confirmation from Suzana on 26-May-2026 including 'Event Driven Review' and removing from CaseReviewType
-- un-select FLM TEA's.
AND ( 
   (t1.TEAReviewReasonDescription is null ) 
   OR 
      ((lower(t1.ReviewReason) not like '%flm%'
AND lower(t1.ReviewReason) not like '%slm%'
AND lower(t1.ReviewReason) not like '%repair%'
AND lower(t1.ReviewReason) not like '%remediation%'
AND lower(t1.ReviewReason) not like '%correction%' )))
AND t1.GlobalClientOwnerLocation = 'Rabobank Foundation'
--AND t2.SectorTeam= 'Foundation'

""").createOrReplaceTempView('SelectionScopeRaboFoundation')

# COMMAND ----------

# DBTITLE 1,Selection scope RCI
# This defines the selection scope and 

spark.sql(f""" 
          
Select t1.*,
 CASE 
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%structure%' THEN 'Structure'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%adverse media%' THEN 'AM'
    WHEN t1.CaseReviewType = 'Tailored Event Assessment' AND t1.ReviewReason like '%transaction%' THEN 'Transaction'
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
   --WHEN  t1.CaseReviewType = 'Event Driven Review' THEN 1
   WHEN  t1.ValidatedRiskLevel = 'Unacceptable' THEN 1
   -- Prioritize Derdengelden over BV.
   WHEN t1.FullLegalName LIKE '%Derdengelden%' THEN 1
   END AS SelectionIncludeFlag

, CASE 
   WHEN 1=1 THEN 0
   END AS SelectionExcludeFlag

 from radar.cases t1 
 inner join radar.clients t2 on t1.UniqueGcobId = t2.uniquegcobid
where t1.CaseCompletedDate between date('{StartOfDateRange_str}') and date('{EndOfDateRange_str}') 
AND t1.CaseReviewType NOT IN ( 'Amendment', 'Change of Client Owner', 'Offboarding')
--Based on email confirmation from Suzana on 26-May-2026 including 'Event Driven Review' and removing from CaseReviewType
-- un-select FLM TEA's.
AND ( 
   (t1.TEAReviewReasonDescription is null ) 
   OR 
      ((lower(t1.ReviewReason) not like '%flm%'
AND lower(t1.ReviewReason) not like '%slm%'
AND lower(t1.ReviewReason) not like '%repair%'
AND lower(t1.ReviewReason) not like '%remediation%'
AND lower(t1.ReviewReason) not like '%correction%' )))
AND t2.SectorTeam= 'RCI'

""").createOrReplaceTempView('SelectionScopeRCI')


# COMMAND ----------

# DBTITLE 1,Check For Correspondent Banking parties
# MAGIC %sql
# MAGIC select distinct t1.* 
# MAGIC from SelectionScope  as t1
# MAGIC inner join CORRESPONDENT_BANKING_PARTIES as t2 on t1.ClientId = t2.ClientId

# COMMAND ----------

# DBTITLE 1,Check 2 for Correspondent Banking
# MAGIC %sql 
# MAGIC select * from SelectionScope where SectorTeam = 'Correspondent Banking'

# COMMAND ----------

# DBTITLE 1,One File Per Group
# One File Per Group
df_OneFilePerGroup = spark.sql('''  SELECT 
    t1.SourceClient
  , t1.UniqueGcobId -- slightly challenging that this is not thesame as GDP unique GcobID
  , t2.KYCGroup
  FROM SelectionScope t1
  LEFT JOIN radar.clients as t2 on t1.UniqueGcobId = t2.UniqueGcobId
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

# MAGIC
# MAGIC
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
# MAGIC
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

# MAGIC %sql
# MAGIC select * from SelectionScope where SelectionIncludeFlag = 1

# COMMAND ----------

# DBTITLE 1,Add Key Data Elements to AllReviewsPhase3
#AllReviewsPhase3
#df_RiskBasedSelection.head(5)

# COMMAND ----------

#all_rules = df_RiskBasedSelection.SELECTION_CRITERIA.unique()

# COMMAND ----------

# DBTITLE 1,Selectionscope _ Riskbased
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SelectionScope_RiskBased AS
# MAGIC
# MAGIC -- this deliberately creates duplicates for some cases that have multiple risk indicators, so increase the likely hood of them being selected.
# MAGIC select t1.* 
# MAGIC From SelectionScope t1
# MAGIC where t1.Sourceclient in (Select distinct Sourceclient from AllReviewsPhase3)

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

# DBTITLE 1,Get basic DataFrames
df_RiskBasedSelection = spark.sql('select * from AllReviewsPhase3').toPandas()

all_rules = df_RiskBasedSelection.SELECTION_CRITERIA.unique()
df_selectionscope = spark.sql('select * from SelectionScope_RiskBased').toPandas()

df_SelectionScope_Final = spark.sql('select * from SelectionScope_Final').toPandas()
CurrentSelection = pd.DataFrame().reindex(columns=df_selectionscope.columns)

#for rule in all_rules:
    # select random file from rules
    # apply SelectedFlag to file
    # add file to CurrentSelection

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. adding selections to the final scope
# MAGIC - All of the Correspondent banking
# MAGIC - 2 RMA only
# MAGIC - 2 Agency
# MAGIC - 3 TEA

# COMMAND ----------

# DBTITLE 1,Select TEA FI
#TODO: get one of each TEA for tea reason
TEA_ID_LIST = list(df_selectionscope[((df_selectionscope.CaseReviewType == 'Tailored Event Assessment') &
                                       (df_selectionscope.SectorTeam != 'Agency')
                                       )][0:TEA_TO_SELECT].SourceClient.values)

df_selectionscope.loc[df_selectionscope.SourceClient.isin(TEA_ID_LIST), 'SelectionIncludeFlag'] = 1

#now set all TEA to Exlude flag = 1.
df_selectionscope.loc[(df_selectionscope.CaseReviewType == 'Tailored Event Assessment') & (
    df_selectionscope.SelectionIncludeFlag != 1), 'SelectionExcludeFlag'] = 1

# df_selectionscope[df_selectionscope.SelectionExcludeFlag == 1]
# TEA_ID_LIST
# df_selectionscope[df_selectionscope.SelectionIncludeFlag == 1]

# COMMAND ----------

# DBTITLE 1,SELECT EA FI
# Assigning FI EA
FI_EA = df_selectionscope[(df_selectionscope.CaseReviewType == 'Event Assessment') &
                          (df_selectionscope.SelectionExcludeFlag != 1) &
                          (df_selectionscope.SectorTeam != 'Agency')]

# how many EA we can select. not more than the max, but also not more than possible.
nr_FI_EA = min(len(FI_EA), EA_TO_SELECT)

# Taking into account the option there might be zero
if nr_FI_EA > 0:
    FI_EA_ALL = FI_EA[0:nr_FI_EA].SourceClient.values[0:nr_FI_EA]

    for i in range(0,nr_FI_EA):
        df_selectionscope.loc[df_selectionscope.SourceClient == FI_EA_ALL[i], 'SelectionIncludeFlag'] = 1

# COMMAND ----------

# DBTITLE 1,Exclude non-select EA from scope
# Exclude rest of TEA's from selection
df_selectionscope.loc[(df_selectionscope.SelectionIncludeFlag != 1) & 
                      (df_selectionscope.CaseReviewType == 'Event Assessment')
                      , 'SelectionExcludeFlag'] = 1

# COMMAND ----------

# DBTITLE 1,CorrespondentBanking
df_selectionscope.loc[((df_selectionscope.SectorTeam == 'Correspondent Banking')
                       & (df_selectionscope.CaseReviewType != 'Tailored Event Assessment')), 'SelectionIncludeFlag'] = 1

# COMMAND ----------

# DBTITLE 1,Agency
#set 2 agency files to include
AGENCY_ID_LIST = list(df_selectionscope[(df_selectionscope.SectorTeam == 'Agency') & 
                                         (df_selectionscope.CaseReviewType != 'Tailored Event Assessment') &
                                         (df_selectionscope.CaseReviewType != 'Event Assessment') &
                                         (df_selectionscope.SelectionExcludeFlag != 1)]
                      [0:2].SourceClient.values)
df_selectionscope.loc[df_selectionscope.SourceClient.isin(AGENCY_ID_LIST), 'SelectionIncludeFlag'] = 1

#set rest of agency to exclude
df_selectionscope.loc[(df_selectionscope.SectorTeam == 'Agency') & 
                      (df_selectionscope.SelectionIncludeFlag != 1), 'SelectionExcludeFlag'] = 1
    



# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up for additional risk-based selection

# COMMAND ----------

# DBTITLE 1,define list of attributes to optimize spread distribution
LIST_OF_DISPERSION_ATTRIBUTES = ['SectorTeam','CaseReviewType', 'ValidatedRiskLevel', 'GlobalClientOwnerLocation']#, 'QC analyst'] # add QC analyst. 
# 'TEA_Review_Reason_Derived'] # not too much Control measure
# 'TEA Derived.

# COMMAND ----------

# DBTITLE 1,Function for calculate distribution of attributes in domain values
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

# DBTITLE 1,function for compare sample vs. original set distribution
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

# DBTITLE 1,Set Selection base and calculate how much left to select
selection_base = df_selectionscope[df_selectionscope.SelectionIncludeFlag == 1]
NR_ALREADY_SELECTED = len(selection_base)

NR_LEFT_TO_SELECT = (NR_FILES_TO_SELECT - NR_ALREADY_SELECTED) + TEA_TO_SELECT
NR_LEFT_TO_SELECT

# COMMAND ----------

# DBTITLE 1,Running selection of parties
"""RUNNING SELECTION
the below runs the actual selection
"""

import numpy as np

# calculate dispersion of entire selection.
tot_dict = calc_attribute_dispersion(
    df_selectionscope,
    LIST_OF_DISPERSION_ATTRIBUTES
)

iteration_dict = {}

# Build UniquePartyId
df_selectionscope['UniquePartyId'] = np.where(
    df_selectionscope['ClientType'] == 'Legal Entity',
    'LEC_' + df_selectionscope['GcobId'].astype(str),
    'NP_NPPC_' + df_selectionscope['GcobId'].astype(str)
)

# Load involved location data
df_involved_location = spark.sql("""
select UniquePartyId, involved_loc
from involved_location
""").toPandas()

df_selectionscope = df_selectionscope.merge(
    df_involved_location,
    on='UniquePartyId',
    how='left'
)

df_selectionscope['involved_loc'] = (
    df_selectionscope['involved_loc']
    .apply(
        lambda x: x.tolist()
        if isinstance(x, np.ndarray)
        else x
        if isinstance(x, list)
        else []
    )
)

df_selectionscope['InvolvedLocationCount'] = (
    df_selectionscope['involved_loc'].apply(len)
)

df_selectionscope['HasRabobankNL'] = (
    df_selectionscope['involved_loc']
    .apply(
        lambda x: 1 if any(
            str(loc).strip().lower() == 'rabobank netherlands'
            for loc in x
        ) else 0
    )
)

df_selectionscope.drop(
    columns=['involved_loc'],
    inplace=True,
    errors='ignore'
)

df_AllSelections = pd.DataFrame().reindex(
    columns=df_selectionscope.columns
)

df_AllSelections['Iteration'] = 0

if NR_LEFT_TO_SELECT > 0:

    for i in range(0, 1000):

        # Prefer records having more involved locations-----------------
        candidate_pool = df_selectionscope[
            (df_selectionscope.SelectionIncludeFlag != 1) &(df_selectionscope.SelectionExcludeFlag != 1)].copy()
        
        candidate_pool["sample_weight"] = ((candidate_pool["InvolvedLocationCount"].fillna(0) + 1) * 10+ candidate_pool["HasRabobankNL"])

        select_add = candidate_pool.sample( n=NR_LEFT_TO_SELECT,weights="sample_weight",replace=False)

        selection = pd.concat(
            [selection_base, select_add]
        )

        dict_sel = calc_attribute_dispersion(
            selection,
            LIST_OF_DISPERSION_ATTRIBUTES
        )

        comp = compare_attribute_dispersion(
            tot_dict,
            dict_sel
        )

        # Store iteration
        selection['Iteration'] = i

        df_AllSelections = pd.concat(
            [df_AllSelections, selection]
        )

        iteration_dict[i] = sum(comp.values())

        print(i, end='\r')

else:

    print(
        'amount of base selection exceeds total selection'
    )

    i = 0

    selection = selection_base.copy()

    selection['Iteration'] = i

    df_AllSelections = selection.copy()

    dict_sel = calc_attribute_dispersion(
        selection,
        LIST_OF_DISPERSION_ATTRIBUTES
    )

    comp = compare_attribute_dispersion(
        tot_dict,
        dict_sel
    )

    iteration_dict[i] = sum(comp.values())


# COMMAND ----------

print(comp)

# COMMAND ----------

# DBTITLE 1,Get the right key
#get keys with lowest target metric
keys = [k for k, v in iteration_dict.items() if v == min(iteration_dict.values())]
print(iteration_dict[keys[0]])

# COMMAND ----------

# DBTITLE 1,optional display for checks
display(df_AllSelections[df_AllSelections.Iteration == keys[0]])

# COMMAND ----------

# DBTITLE 1,Create Database if not Exists
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS FLM

# COMMAND ----------

# DBTITLE 1,Define columns to store
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

# DBTITLE 1,Store FI hub selection in database
df_best_sample = df_AllSelections[df_AllSelections.Iteration == keys[0]]

df_best_sample['SampleStartPeriod'] = StartOfDateRange
df_best_sample['SampleEndPeriod'] = EndOfDateRange
df_best_sample['SampleScope'] = 'Global FI Hub'
#df_best_sample[['TEAReviewReasonDescription']].fillna('none', inplace = True)

sdf_best_sample = spark.createDataFrame(df_best_sample[SelectionColumns])


sdf_best_sample.write.mode('append').option("mergeSchema", "true").saveAsTable('FLM.samples2')

# COMMAND ----------

# DBTITLE 1,Store Acorn Selection in Database
df_acorn = spark.sql('Select * from selectionScopeAcorn').toPandas()

df_acorn['SampleStartPeriod'] = StartOfDateRange
df_acorn['SampleEndPeriod'] = EndOfDateRange
df_acorn['SampleScope'] = 'Acorn'
#df_best_sample[['TEAReviewReasonDescription']].fillna('none', inplace = True)

if len(df_acorn) > 0:
    sdf_acorn = spark.createDataFrame(df_acorn[SelectionColumns])
    sdf_acorn = sdf_acorn.withColumn("GcobId", sdf_acorn["GcobId"].cast(DoubleType()))
    sdf_acorn.write.mode('append').option("mergeSchema", "true").saveAsTable('FLM.samples2')

# COMMAND ----------

# DBTITLE 1,Store PSP selection in Database
df_PSP = spark.sql('Select * from selectionScopePSP').toPandas()

df_PSP['SampleStartPeriod'] = StartOfDateRange
df_PSP['SampleEndPeriod'] = EndOfDateRange
df_PSP['SampleScope'] = 'PSP'

if len(df_PSP) > 0:
    sdf_PSP = spark.createDataFrame(df_PSP[SelectionColumns])
    sdf_PSP = sdf_PSP.withColumn("GcobId", sdf_PSP["GcobId"].cast(DoubleType()))

    sdf_PSP.write.mode('append').option("mergeSchema", "true").saveAsTable('FLM.samples2')


# COMMAND ----------

# DBTITLE 1,Store RMA specific
df_RMA = spark.sql('Select * from selectionScopeRMA').toPandas()
df_RMA['SampleStartPeriod'] = StartOfDateRange
df_RMA['SampleEndPeriod'] = EndOfDateRange
df_RMA['SampleScope'] = 'RMA'

if len(df_RMA) > 0:
    sdf_RMA = spark.createDataFrame(df_RMA[SelectionColumns])
    sdf_RMA = sdf_RMA.withColumn("GcobId", sdf_RMA["GcobId"].cast(DoubleType()))
    sdf_RMA.write.mode('append').option("mergeSchema", "true").saveAsTable('FLM.samples2')

# COMMAND ----------

# DBTITLE 1,Store Rabo Foundation in database
df_foundation = spark.sql('Select * from SelectionScopeRaboFoundation').toPandas()

df_foundation['SampleStartPeriod'] = StartOfDateRange
df_foundation['SampleEndPeriod'] = EndOfDateRange
df_foundation['SampleScope'] = 'Rabo Foundation'

if len(df_foundation) > 0:
    sdf_foundation = spark.createDataFrame(df_foundation[SelectionColumns])
    sdf_foundation = sdf_foundation.withColumn("GcobId", sdf_foundation["GcobId"].cast(DoubleType()))
    sdf_foundation.write.mode('append').option("mergeSchema", "true").saveAsTable('FLM.samples2')


# COMMAND ----------

# DBTITLE 1,Store RCI in database
df_RCI = spark.sql('Select * from SelectionScopeRCI').toPandas()

df_RCI['SampleStartPeriod'] = StartOfDateRange
df_RCI['SampleEndPeriod'] = EndOfDateRange
df_RCI['SampleScope'] = 'RCI'

if len(df_RCI) > 0:
    sdf_RCI = spark.createDataFrame(df_RCI[SelectionColumns])
    sdf_RCI = sdf_RCI.withColumn("GcobId", sdf_RCI["GcobId"].cast(DoubleType()))
    sdf_RCI.write.mode('append').option("mergeSchema", "true").saveAsTable('FLM.samples2')

