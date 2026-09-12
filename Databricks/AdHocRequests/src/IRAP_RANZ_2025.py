# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal
# MAGIC To answer RAF related questions on customer types
# MAGIC
# MAGIC - GR.17.01; [customer company legal form is:] Trust (anglo-saxon)
# MAGIC - GR.17.02;[customer company legal form is:] Foundation or other similar foreign legal form
# MAGIC - GR.17.05; [customer company legal form is:] Limited liability partnership (LLP) and/or Limited Partnership (LP)
# MAGIC - GR.17.06: [the customer structure has a] Nominee shareholder
# MAGIC - GR.17.07: [the company gives out] Bearer shares
# MAGIC
# MAGIC
# MAGIC ### Flow of logic
# MAGIC - Legacy 2
# MAGIC - GCOB

# COMMAND ----------

import os
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the files

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

#Derive the date for which data has to be processes
load_dts = 'EDL_LOAD_DTS=20250101*'

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
#'party_AllCasesReport'
    'party_case_client_details'
    , 'party_client'
   # , 'Party_AllParty_LocationCoverage'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{row.GDPname}/101/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
#'party_AllCasesReport'
    'Party_RiskModelInstanceQuestionAnswers'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/RISKMODEL/{row.GDPname}/101/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
    'Legacy2_GCOB_ApprovedVersion'
    , 'Legacy2_case_client_details'
    , 'Legacy2_risk'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/Legacy2/{row.GDPname}/101/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# MAGIC %md
# MAGIC ## RANZ data
# MAGIC - To provide data from MDM
# MAGIC - 
# MAGIC - 

# COMMAND ----------

ReadStorage = 'salandingzonefecradarprd'

#Retrieving Client Secret from connected Secrets
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
# Configuring Spark to access GDP Defined Storage account using OAuth authentication
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# DBTITLE 1,MDM as VIEW
from pyspark.sql.functions import to_date, lit

latest_mdm_df = spark.read \
    .format('csv') \
    .option('header','true') \
    .option('inferSchema', 'true') \
    .load('abfss://ranz-mdm@salandingzonefecradarprd.dfs.core.windows.net/SIRA_REPORT_20250101.csv') \
    .createOrReplaceTempView('MDM')

# COMMAND ----------

# MAGIC %sql
# MAGIC --for sample
# MAGIC select * from MDM limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input from the Region
# MAGIC received
# MAGIC
# MAGIC `Hi Rick,
# MAGIC
# MAGIC As per further discussion with Isabel,
# MAGIC
# MAGIC GR.17.06: [the customer structure has a] Nominee shareholder  à Relationship Type NBEN
# MAGIC GR.17.07: [the company gives out] Bearer shares  à We don’t proceed with Onboarding if we find customer with Bearer Shares è this can be defaulted to N
# MAGIC
# MAGIC Below attributes can be derived from Legal Entity Type Code
# MAGIC
# MAGIC GR.17.01; [customer company legal form is:] Trust (anglo-saxon)
# MAGIC GR.17.02;[customer company legal form is:] Foundation or other similar foreign legal form
# MAGIC GR.17.05; [customer company legal form is:] Limited liability partnership (LLP) and/or Limited Partnership (LP)`

# COMMAND ----------

# MAGIC %md
# MAGIC ### to count:
# MAGIC
# MAGIC For the columns highlighted in green, discussed with Anil
# MAGIC - (i)ISSUES_BEARER_SHARES:  We don’t proceed with Onboarding if we find customer with Bearer Shares  hence it will be always 'N'
# MAGIC - (ii)NOMINEE_SHAREHOLDER: They are derived from NBEN(Non-Beneficial/Nominee Shareholder) relationship
# MAGIC - (iii)CDD_ENTITY_TYPE : GRAM (MDM does not have it)

# COMMAND ----------

# DBTITLE 1,SELECT SCOPE FROM TOTAL MDM File
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW MDM_SEL AS
# MAGIC (
# MAGIC   select * from MDM
# MAGIC   WHERE CLIENT_STATUS = 'Active Client'
# MAGIC   AND (REL_TYPE_CD like '%PHOLD%' )--OR REL_TYPE_CD like '%AOWN%')
# MAGIC )

# COMMAND ----------

# DBTITLE 1,active contracts in MDM file
# MAGIC %sql
# MAGIC --PHOLD only
# MAGIC select count(distinct(Contract_Id)) from MDM 
# MAGIC WHERE CLIENT_STATUS = 'Active Client'
# MAGIC --AND (REL_TYPE_CD like '%PHOLD%' )

# COMMAND ----------

# DBTITLE 1,Breakdown per customer type
# MAGIC %sql
# MAGIC select 
# MAGIC  --'GR.17.01_IsTrust' AS QuestionName
# MAGIC -- , Client_BUSINESS_LINE
# MAGIC   count(distinct(Contract_Id)) as Count_Contracts
# MAGIC  , CUSTOMER_TYPE_CD
# MAGIC from MDM_SEL
# MAGIC --WHERE (CUSTOMER_TYPE_CD = 'TRUS' OR CUSTOMER_TYPE_CD = 'SMSF')
# MAGIC GROUP BY CUSTOMER_TYPE_CD

# COMMAND ----------

# DBTITLE 1,Count of TRUST files
# MAGIC %sql
# MAGIC select 
# MAGIC  'GR.17.01_IsTrust' AS QuestionName
# MAGIC -- , Client_BUSINESS_LINE
# MAGIC  , count(distinct(Contract_Id)) as count_parties
# MAGIC from MDM_SEL
# MAGIC WHERE (CUSTOMER_TYPE_CD = 'TRUS')-- OR CUSTOMER_TYPE_CD = 'SMSF')
# MAGIC --GROUP BY Client_BUSINESS_LINE

# COMMAND ----------

# DBTITLE 1,Totals GR17
# MAGIC %sql
# MAGIC select 
# MAGIC  'GR.17.00_TotalClients' AS QuestionName
# MAGIC  , Client_BUSINESS_LINE
# MAGIC  , count(distinct(Contract_Id)) as count_parties
# MAGIC from MDM_SEL
# MAGIC GROUP BY Client_BUSINESS_LINE
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC select 
# MAGIC  'GR.17.01_IsTrust' AS QuestionName
# MAGIC  , Client_BUSINESS_LINE
# MAGIC  , count(distinct(Contract_Id)) as count_parties
# MAGIC from MDM_SEL
# MAGIC WHERE (CUSTOMER_TYPE_CD = 'TRUS' )--OR CUSTOMER_TYPE_CD = 'SMSF')
# MAGIC GROUP BY Client_BUSINESS_LINE
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC -- '2608127' has name The Lindsay and Heather Staier Foundation
# MAGIC select  'GR.17.02_IsStichting' AS QuestionName
# MAGIC , Client_BUSINESS_LINE
# MAGIC , count(distinct(Contract_Id)) as count_parties
# MAGIC  from MDM_SEL where Global_Client_ID IN (2608127 ,2591655,2612665,2615110,2912234,2931150) 
# MAGIC AND (CUSTOMER_TYPE_CD <> 'TRUS' )
# MAGIC GROUP BY Client_BUSINESS_LINE
# MAGIC union
# MAGIC
# MAGIC
# MAGIC select 'GR.17.05_LP_LLP' AS QuestionName
# MAGIC  , Client_BUSINESS_LINE
# MAGIC  , count(distinct(Contract_Id)) as count_parties
# MAGIC from MDM_SEL
# MAGIC WHERE CUSTOMER_TYPE_CD = 'LPAR' 
# MAGIC GROUP BY Client_BUSINESS_LINE
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select 'GR.17.06_NomineeShareholders' AS QuestionName 
# MAGIC  , Client_BUSINESS_LINE
# MAGIC  , count(distinct(Contract_Id)) as count_parties
# MAGIC  from MDM_SEL
# MAGIC WHERE REL_TYPE_CD Like '%NBEN%'
# MAGIC GROUP BY Client_BUSINESS_LINE
# MAGIC
# MAGIC -- skipping bearer because 0 anyway.
# MAGIC /*union
# MAGIC
# MAGIC select 'GR.17.07_IsBearerShares' AS QuestionName, count(*)
# MAGIC from MDM_SEL
# MAGIC WHERE 
# MAGIC GROUP BY Client_BUSINESS_LINE
# MAGIC
# MAGIC */
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### GCOB Based

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_RANZ_Selection AS
# MAGIC  
# MAGIC  Select CDDtype, sourceclient, globalclientownerlocation, gcobid
# MAGIC
# MAGIC  from party_case_client_details
# MAGIC  where globalclientownerlocation IN ('Rabobank - RANZ Country Banking and ROS', 'Rabobank New Zealand', 'Rabobank Australia')
# MAGIC  and clientlifecycleName = 'Client'
# MAGIC  and IslatestApprovedVersionOfClient = true

# COMMAND ----------

# DBTITLE 1,GCOB bearer shares
# MAGIC %sql
# MAGIC --CREATE OR REPLACE TEMP VIEW RANZ_riskquestions AS
# MAGIC
# MAGIC SELECT
# MAGIC     t1.GcobId
# MAGIC ,   t1.SourceClient
# MAGIC     ,t2.AnswerText AS BearerShares
# MAGIC
# MAGIC from GCOB_RANZ_Selection as t1
# MAGIC LEFT JOIN Party_RiskModelInstanceQuestionAnswers t2 on t1.sourceclient = t2.sourceclient
# MAGIC WHERE 
# MAGIC t2.QuestionId in (71,551,17,363,341) --bearer
# MAGIC AND t2.AnswerText <> 'No'

# COMMAND ----------

# DBTITLE 1,COUNT Nominee Shareholder
# MAGIC %sql
# MAGIC select COUNT(DISTINCT PARTY_ID) AS NomineeShareholders
# MAGIC , CLIENT_BUSINESS_LINE
# MAGIC from MDM
# MAGIC WHERE REL_TYPE_CD Like '%NBEN%'
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC GROUP BY CLIENT_BUSINESS_LINE

# COMMAND ----------

# DBTITLE 1,MDM Limited Partnerships
# MAGIC %sql
# MAGIC select 
# MAGIC --, CDD_CLIENT_TYPE
# MAGIC  count(*) as count
# MAGIC , CUSTOMER_TYPE_CD
# MAGIC from MDM
# MAGIC WHERE (CUSTOMER_TYPE_CD = 'LPAR' --OR CUSTOMER_TYPE_CD = 'SMSF'
# MAGIC )
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC --AND Client_BUSINESS_LINE IN ('RANZ AU_CB', 'RANZ NZ_CB')
# MAGIC AND REL_TYPE_CD = 'PHOLD'
# MAGIC GROUP BY CUSTOMER_TYPE_CD;
# MAGIC

# COMMAND ----------

# DBTITLE 1,GCDS data for RANZ
## GCDS

# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
    , 'client_Client'
    , 'client_PartyRole'
    , 'client_Products'
    , 'client_OnboardedLocations'
    , 'client_PartytoPartyRelationship'
    , 'client_RMA'
]

for item in gcds_tables:
    # get the most recent file available in gdp
    path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4601/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('EDL_LOAD_DTS=')[1][:8] for file in files if 'EDL_LOAD_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4601/data/EDL_LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcds_'+ item)

# COMMAND ----------

# DBTITLE 1,MDM
# MAGIC %sql
# MAGIC select Legal_Form, 
# MAGIC * 
# MAGIC from gcds_client_client
# MAGIC where `Global_CO-location_code` = 'AUS'
# MAGIC and Full_legal_name like '%Foundation%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select Legal_Form, GCID, Full_legal_name
# MAGIC from gcds_client_client
# MAGIC where `Global_CO-location_code` = 'NZL'
# MAGIC and Full_legal_name like '%Foundation%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### GCOB and Legacy RANZ
# MAGIC
# MAGIC same as RAF. 
# MAGIC GCOB part
# MAGIC Legacy2 part

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_RANZ_Selection AS
# MAGIC  
# MAGIC  Select CDDtype, sourceclient, globalclientownerlocation, gcobid
# MAGIC
# MAGIC  from party_case_client_details
# MAGIC  where globalclientownerlocation IN ('Rabobank - RANZ Country Banking and ROS', 'Rabobank New Zealand', 'Rabobank Australia')
# MAGIC  and clientlifecycleName = 'Client'
# MAGIC  and IslatestApprovedVersionOfClient = true

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RANZ_riskquestions AS
# MAGIC
# MAGIC SELECT
# MAGIC     t1.GcobId
# MAGIC ,   t1.SourceClient
# MAGIC     , CASE
# MAGIC         WHEN t2.QuestionId in (14, 361, 549) THEN t2.AnswerText
# MAGIC         ELSE '' 
# MAGIC         END AS NomineeShareholders
# MAGIC
# MAGIC       , CASE
# MAGIC         WHEN t2.QuestionId in (71,551,17,363,341) THEN t2.AnswerText
# MAGIC         ELSE ''
# MAGIC         END AS BearerShares
# MAGIC
# MAGIC from GCOB_RANZ_Selection as t1
# MAGIC LEFT JOIN Party_RiskModelInstanceQuestionAnswers t2 on t1.sourceclient = t2.sourceclient
# MAGIC WHERE 
# MAGIC t2.QuestionId in (71,551,17,363,341) --bearer
# MAGIC OR t2.QuestionId in (14, 361, 549) --nominee shareholders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RANZ_riskquestions_pivot AS
# MAGIC
# MAGIC select t1.SourceClient, t1.NomineeShareholders, t2.BearerShares
# MAGIC
# MAGIC FROM
# MAGIC (
# MAGIC Select distinct SourceClient
# MAGIC , IFF(NomineeShareholders = 'Yes', 1, 0) AS NomineeShareholders 
# MAGIC from RANZ_riskquestions
# MAGIC WHERE NomineeShareholders <> '') as T1
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC
# MAGIC (
# MAGIC Select distinct SourceClient
# MAGIC , IFF(BearerShares = 'Yes', 1, 0) AS BearerShares 
# MAGIC   from RANZ_riskquestions
# MAGIC WHERE BearerShares <> '') as T2 ON t1.Sourceclient = t2.sourceclient

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCOBTotalSelectionRANZ AS
# MAGIC SELECT 
# MAGIC
# MAGIC t1.* 
# MAGIC  ,    CASE 
# MAGIC         WHEN CddType in ('Trust') THEN 1
# MAGIC         END AS Has_Trust_Flag
# MAGIC ,    CASE 
# MAGIC         WHEN CddType in ('Limited Liability Limited Partnership', 'Limited Partnership', 'Limited Liability Partnership') THEN 1
# MAGIC         END AS Has_LP_LLP_Flag
# MAGIC
# MAGIC ,     CASE 
# MAGIC         WHEN CddType in ('Foundation') THEN 1
# MAGIC         END AS Has_Foundation_Flag
# MAGIC ,t2.NomineeShareholders
# MAGIC ,t2.BearerShares
# MAGIC
# MAGIC from GCOB_RANZ_Selection t1
# MAGIC LEFT JOIN RANZ_riskquestions_pivot AS t2 on t1.SourceClient = t2.SourceClient

# COMMAND ----------

# DBTITLE 1,GCOB_FINAL_NUMBERS
# MAGIC %sql
# MAGIC select
# MAGIC --SourceClient
# MAGIC  COUNT(*) AS total_count
# MAGIC , SUM(NomineeShareholders) AS NomineeShareholdersCount
# MAGIC , SUM(BearerShares) AS BearerSharesCount
# MAGIC , SUM(Has_Foundation_Flag) AS Has_Foundation
# MAGIC , SUM(Has_LP_LLP_Flag) AS Has_LP_LLP
# MAGIC , SUM(Has_Trust_Flag) AS Has_Trust
# MAGIC FROM GCOBTotalSelectionRANZ

# COMMAND ----------

# MAGIC %md
# MAGIC #### Legacy2 results.
# MAGIC
# MAGIC - get A1 files from legacy 2
# MAGIC - same logic as RAF.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client_RANZ AS
# MAGIC select * from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)
# MAGIC and GlobalClientOwnerLocation IN ('Australia', 'New Zealand')
# MAGIC and ClientLifeCycleName = 'Client'
# MAGIC and IsClient = 'true'
# MAGIC and IsLatestApprovedVersionOfClient = 'True'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Legacy2RANZ AS
# MAGIC
# MAGIC   select DISTINCT
# MAGIC   GCOBid,
# MAGIC   Legacy2_client_RANZ.ClientId,
# MAGIC   FullLegalName,
# MAGIC   CddType, 
# MAGIC   Legacy2_client_RANZ.ClientType, 
# MAGIC   CASE 
# MAGIC   WHEN CddType in ('Limited partnership', 'Limited liability partnership' , 'Limited liability limited partnership') THEN 1
# MAGIC     End AS HAS_LP_LLP_FLAG
# MAGIC     , CASE WHEN CddType in ('Business trust', 'Trust') THEN 1
# MAGIC       END AS Has_Trust_Flag
# MAGIC     , IFF(t2.StructureNomineeShareholders = true,1,0) AS StructureNomineeShareholders
# MAGIC     , IFF(t2.StructureIssuedBearerShares = true,1,0) AS StructureIssuedBearerShares
# MAGIC   -- ,*
# MAGIC
# MAGIC   from Legacy2_client_RANZ
# MAGIC   left join Legacy2_risk  as t2 on Legacy2_client_RANZ.ClientId = t2.ClientId

# COMMAND ----------

# DBTITLE 1,SHOW totals from GCOB and Legacy together
# MAGIC %sql
# MAGIC select 'RANZ-Legacy2' AS Portfolio
# MAGIC , count(DISTINCT ClientId) AS Clients
# MAGIC , 0 AS Foundation_Count
# MAGIC , SUM(HAS_LP_LLP_FLAG) AS LP_LLP_Count
# MAGIC , SUM(Has_Trust_Flag) AS Trust_Count
# MAGIC , SUM(StructureNomineeShareholders) AS StructureNomineeShareholders_Count
# MAGIC , SUM(StructureIssuedBearerShares) AS StructureIssuedBearerShares_Count
# MAGIC
# MAGIC FROM Legacy2RANZ
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select
# MAGIC 'RANZ-GCOB' AS Portfolio
# MAGIC ,  COUNT(*) AS total_count
# MAGIC , SUM(Has_Foundation_Flag) AS Has_Foundation
# MAGIC , SUM(Has_LP_LLP_Flag) AS Has_LP_LLP
# MAGIC , SUM(Has_Trust_Flag) AS Has_Trust
# MAGIC , SUM(NomineeShareholders) AS StructureNomineeShareholders_Count
# MAGIC , SUM(BearerShares) AS StructureIssuedBearerShares_Count
# MAGIC
# MAGIC FROM GCOBTotalSelectionRANZ

# COMMAND ----------

# DBTITLE 1,common sense data checks
# MAGIC %sql
# MAGIC select * from Legacy2RANZ 
# MAGIC where Has_Trust_Flag = 1
# MAGIC order by GcobId
# MAGIC limit 3

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### OCDD-GRAM numbers

# COMMAND ----------

# DBTITLE 1,Load from Radar - structure
# MAGIC %sql
# MAGIC select QuestionText, count(distinct(ClientID)) from radar.RanzMDMGramCDDQuestionsAnswers 
# MAGIC WHERE (QuestionId in (71,551,17,363,341) --bearer
# MAGIC OR QuestionId in (14, 361, 549)) --nominee shareholders
# MAGIC AND AnswerValue = 'TRUE'
# MAGIC GROUP BY QuestionText

# COMMAND ----------

# DBTITLE 1,Radar - check entity types (GRAM imcomplete)
# MAGIC %sql
# MAGIC select QuestionText, * 
# MAGIC from radar.RanzMDMGramCDDQuestionsAnswers 
# MAGIC WHERE QuestionId  = 540
# MAGIC limit 4
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct AnswerText
# MAGIC from radar.RanzMDMGramCDDQuestionsAnswers 
# MAGIC WHERE QuestionId  = 540

# COMMAND ----------

# MAGIC %sql
# MAGIC select QuestionText, count(*), AnswerText
# MAGIC from radar.RanzMDMGramCDDQuestionsAnswers 
# MAGIC WHERE QuestionId  = 540
# MAGIC GROUP BY QuestionText, AnswerText
