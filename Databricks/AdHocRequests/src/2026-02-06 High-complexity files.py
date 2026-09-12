# Databricks notebook source
import os

# COMMAND ----------

#Fetching environment variables
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

#Retrieving Client Secret from connected Secrets
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
# Configuring Spark to access GDP Defined Storage account using OAuth authentication
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# DBTITLE 1,Select cases
# MAGIC %sql
# MAGIC select * from radar.cases 
# MAGIC where CDDDepartment LIKE '%Corp%'
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW Databases

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.CDDRiskdetails limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## selection elements
# MAGIC - cases.validatedRisk =  'High'
# MAGIC - 1. Sanctions GEO-Q6 -> should not be none
# MAGIC - 2. ECHR#C: Geo ECHR3C. : GEO-Q7 is not 'None' or empty
# MAGIC - 3. Dual use: SEC-Q2 contains 'Dual Use'
# MAGIC - 4.SAR. TX-T2-Q1.1.1 : if NO then HR
# MAGIC - 5a. Case.CDD type = either 'Limited Partnership' or 'Fund / Collective Investment Scheme'
# MAGIC - 5b. Any of the following exist in structure snapshot GUI - relationshiptype: 'General Partner/Fund Manager/Fund Managed by/Limited Partners linked to the client'
# MAGIC - 6. Trusts .radar.allrelationships. (Planet One Holding Limited (95730))
# MAGIC     - Relationship types/link (FGR): Fund Manager/Custodian/Title holder/Investors/Participants/Founders/Protectors
# MAGIC       - HasGeneralPartner
# MAGIC       - HasLimitedPartner
# MAGIC       - HasFundManagedBy
# MAGIC       - ?custodian ? Titleholder ? Investor ? Participant ? Founders ? Protectors
# MAGIC     - Relationship type/link (Trust) : Beneficiaries/ Trustees/ Settlors/ Protector(s)
# MAGIC       - HasBeneficiary
# MAGIC       - HasSettlorFounder
# MAGIC       - HasTrustee
# MAGIC       - HasProtector
# MAGIC - 7. PEP: Any type of pep. PEP-Q3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.GCOB_CDDRiskdetails limit 10

# COMMAND ----------

# DBTITLE 1,prepare essentials from Client structure
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
# MAGIC --OR  = 'True'
# MAGIC
# MAGIC GROUP BY SourceClient
# MAGIC --limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GCOB_StructureElements

# COMMAND ----------

# DBTITLE 1,selection from CDDRiskDetails.
# MAGIC %sql
# MAGIC select 
# MAGIC t1.GcobId
# MAGIC , t1.SourceClient
# MAGIC , t1.casereviewtype
# MAGIC , t1.CDDDepartment
# MAGIC , t1.FullLegalName
# MAGIC , t2.ValidatedRiskLevel
# MAGIC , t1.CaseCompletedDate
# MAGIC , t2.`GEO-Q6` -- Sanctions
# MAGIC , t2.`GEO-Q7` -- ECHR 3R
# MAGIC , t2.`SEC-Q2` -- Dual use
# MAGIC , t2.`TX-Q21.1.1` -- SAR filing Q 862
# MAGIC , t1.`CDDType` --either 'Limited Partnership' or 'Fund / Collective Investment Scheme'
# MAGIC , t2.`PEP-Q3` --If any form of PEP is there
# MAGIC , t3.`NrOfTrustOrFundElements`
# MAGIC -- the following for case statements for HR or not.
# MAGIC ,CASE
# MAGIC   WHEN  t2.ValidatedRiskLevel = 'High' AND t2.`GEO-Q7` NOT IN ('None', '') AND t2.`GEO-Q7` IS NOT NULL THEN 'High Complexity - S1' --scenario 1
# MAGIC   WHEN  t2.`TX-T2-Q1.1.1` = 'No' THEN 'High Complexity - S2' --scenario 2
# MAGIC   WHEN  ( t2.`GEO-Q6` NOT IN ('None', '') AND t2.`GEO-Q6` IS NOT NULL ) OR 
# MAGIC         ( t2.`SEC-Q2` LIKE '%Dual%' ) THEN 'High Complexity - S3'-- scenario 3
# MAGIC   WHEN  (t2.ValidatedRiskLevel = 'Medium' AND t2.`GEO-Q7` NOT IN ('None', '') AND t2.`GEO-Q7` IS NOT NULL)
# MAGIC         AND (t1.`CDDType` IN ('Fund / Collective investment scheme', 'Limited Partnership') OR t3.`NrOfTrustOrFundElements` > 0 )
# MAGIC         THEN 'High Complexity - S4'-- scenario 4 tbd
# MAGIC   WHEN  t2.ValidatedRiskLevel = 'Medium' AND t2.`GEO-Q7` NOT IN ('None', '') AND t2.`GEO-Q7` IS NOT NULL 
# MAGIC         AND t2.`PEP-Q3` NOT IN ('None', '', 'No PEP identified') AND   t2.`PEP-Q3` IS NOT NULL THEN 'High Complexity - S5'--scenario 5
# MAGIC   WHEN  (t2.`PEP-Q3` NOT IN ('None', '', 'No PEP identified')  AND  t2.`PEP-Q3` IS NOT NULL) AND 
# MAGIC         ( t1.`CDDType` IN ('Fund / Collective investment scheme', 'Limited Partnership') OR t3.`NrOfTrustOrFundElements` > 0 ) 
# MAGIC         THEN 'High Complexity - S6' -- scenario 6
# MAGIC
# MAGIC END AS FileComplexity
# MAGIC
# MAGIC from radar.cases t1
# MAGIC left join  radar.GCOB_CDDRiskdetails t2 on t1.SourceClient = t2.SourceClient
# MAGIC left join GCOB_StructureElements as t3 on t1.SourceClient = t3.SourceClient
# MAGIC inner join radar.clients as t4 on t1.SourceClient = t4.SourceClient
# MAGIC where t1.casereviewtype IN ('Event Driven Review', 'Periodic Review')
# MAGIC --and t1.CDDDepartment LIKE '%Corp%'
# MAGIC and t1.CasePhase = 'Completed'
# MAGIC and t1.GcobId = '103962'
# MAGIC --and t1.CaseCompletedDate > to_date('2025-01-01')
# MAGIC

# COMMAND ----------


