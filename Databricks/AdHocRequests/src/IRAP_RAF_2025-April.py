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
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
Today = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'
print (load_dts)
Load = 'EDL_LOAD_DTS='+ datetime.today().strftime('%Y%m%d')
print(Load)
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
EDL_LoadDate = (datetime.today()).strftime("%Y-%m-%dT%H:%m:%mZ")

load_dts = 'EDL_LOAD_DTS=20250101*'

# COMMAND ----------



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
# MAGIC ## GCOB - RAF
# MAGIC Get RAF clients, latest approved case there

# COMMAND ----------

# MAGIC %md
# MAGIC ### GRAM.

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_RAF_Selection AS
# MAGIC  
# MAGIC  Select CDDtype, sourceclient, globalclientownerlocation, gcobid
# MAGIC
# MAGIC  from party_case_client_details
# MAGIC  where globalclientownerlocation = 'Rabobank - USA Rabo AgriFinance'
# MAGIC  and clientlifecycleName = 'Client'
# MAGIC  and IslatestApprovedVersionOfClient = true

# COMMAND ----------

# MAGIC %sql
# MAGIC  Select CDDtype, sourceclient, globalclientownerlocation, gcobid, *
# MAGIC
# MAGIC  from party_case_client_details limit 3

# COMMAND ----------

# MAGIC %sql
# MAGIC  Select *
# MAGIC
# MAGIC  from Party_RiskModelInstanceQuestionAnswers
# MAGIC  limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RAF_riskquestions AS
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
# MAGIC from GCOB_RAF_Selection as t1
# MAGIC LEFT JOIN Party_RiskModelInstanceQuestionAnswers t2 on t1.sourceclient = t2.sourceclient
# MAGIC WHERE 
# MAGIC t2.QuestionId in (71,551,17,363,341) --bearer
# MAGIC OR t2.QuestionId in (14, 361, 549) --nominee shareholders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RAF_riskquestions_pivot AS
# MAGIC
# MAGIC select t1.SourceClient, t1.NomineeShareholders, t2.BearerShares
# MAGIC
# MAGIC FROM
# MAGIC (
# MAGIC Select distinct SourceClient
# MAGIC , IFF(NomineeShareholders = 'Yes', 1, 0) AS NomineeShareholders 
# MAGIC from RAF_riskquestions
# MAGIC WHERE NomineeShareholders <> '') as T1
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC
# MAGIC (
# MAGIC Select distinct SourceClient
# MAGIC , IFF(BearerShares = 'Yes', 1, 0) AS BearerShares 
# MAGIC   from RAF_riskquestions
# MAGIC WHERE BearerShares <> '') as T2 ON t1.Sourceclient = t2.sourceclient
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from GCOB_RAF_Selection where sourceclient = 'LEC_63739'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from RAF_riskquestions where sourceclient = 'LEC_63739'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from RAF_riskquestions_pivot

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCOBTotalSelection AS
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
# MAGIC from GCOB_RAF_Selection t1
# MAGIC LEFT JOIN RAF_riskquestions_pivot AS t2 on t1.SourceClient = t2.SourceClient
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC --SourceClient
# MAGIC  COUNT(*) AS total_count
# MAGIC , SUM(NomineeShareholders) AS NomineeShareholdersCount
# MAGIC , SUM(BearerShares) AS BearerSharesCount
# MAGIC , SUM(Has_Foundation_Flag) AS Has_Foundation
# MAGIC , SUM(Has_LP_LLP_Flag) AS Has_LP_LLP
# MAGIC , SUM(Has_Trust_Flag) AS Has_Trust
# MAGIC FROM GCOBTotalSelection

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GCOB_RAF_Selection where cddType = 'Trust'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct cddtype from GCOB_RAF_Selection

# COMMAND ----------

# MAGIC %sql
# MAGIC  Select distinct QuestionText, QuestionId
# MAGIC
# MAGIC  from Party_RiskModelInstanceQuestionAnswers
# MAGIC  where QuestionText like '%bear%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct globalclientownerlocation 
# MAGIC from party_case_client_details

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GCOB_RAF_Selection

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(CddType) 
# MAGIC from Legacy2_case_client_details

# COMMAND ----------

# MAGIC %md
# MAGIC ### Legacy 2
# MAGIC
# MAGIC - GR.17.01; [customer company legal form is:] Trust (anglo-saxon)
# MAGIC - GR.17.02;[customer company legal form is:] Foundation or other similar foreign legal form
# MAGIC - GR.17.05; [customer company legal form is:] Limited liability partnership (LLP) and/or Limited Partnership (LP)
# MAGIC - GR.17.06: [the customer structure has a] Nominee shareholder
# MAGIC - GR.17.07: [the company gives out] Bearer shares

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from Legacy2_case_client_details;--25926
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%'; --3006

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Legacy2_case_client_details  where GcobId = 'RA: 10067' limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client_RAF AS
# MAGIC select * from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)
# MAGIC and GlobalClientOwnerLocation = 'Rabobank - USA Rabo AgriFinance'
# MAGIC and ClientLifeCycleName = 'Client'
# MAGIC and IsClient = 'true'
# MAGIC and IsLatestApprovedVersionOfClient = 'True'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Legacy2RAF AS
# MAGIC
# MAGIC   select 
# MAGIC   GCOBid,
# MAGIC   Legacy2_client_RAF.ClientId,
# MAGIC   FullLegalName,
# MAGIC   CddType, 
# MAGIC   Legacy2_client_RAF.ClientType, 
# MAGIC   CASE 
# MAGIC   WHEN CddType in ('Limited partnership', 'Limited liability partnership' , 'Limited liability limited partnership') THEN 1
# MAGIC     End AS HAS_LP_LLP_FLAG
# MAGIC     , CASE WHEN CddType in ('Business trust', 'Trust') THEN 1
# MAGIC       END AS Has_Trust_Flag
# MAGIC     , IFF(t2.StructureNomineeShareholders = true,1,0) AS StructureNomineeShareholders
# MAGIC     , IFF(t2.StructureIssuedBearerShares = true,1,0) AS StructureIssuedBearerShares
# MAGIC   -- ,*
# MAGIC
# MAGIC   from Legacy2_client_RAF
# MAGIC   left join Legacy2_risk  as t2 on Legacy2_client_RAF.ClientId = t2.ClientId
# MAGIC  

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select 'RAF-Legacy2' AS Portfolio
# MAGIC , count(ClientId) AS Clients
# MAGIC , 0 AS Foundation_Count
# MAGIC , SUM(HAS_LP_LLP_FLAG) AS LP_LLP_Count
# MAGIC , SUM(Has_Trust_Flag) AS Trust_Count
# MAGIC , SUM(StructureNomineeShareholders) AS StructureNomineeShareholders_Count
# MAGIC , SUM(StructureIssuedBearerShares) AS StructureIssuedBearerShares_Count
# MAGIC
# MAGIC FROM Legacy2RAF
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select
# MAGIC 'RAF-GCOB' AS Portfolio
# MAGIC ,  COUNT(*) AS total_count
# MAGIC , SUM(Has_Foundation_Flag) AS Has_Foundation
# MAGIC , SUM(Has_LP_LLP_Flag) AS Has_LP_LLP
# MAGIC , SUM(Has_Trust_Flag) AS Has_Trust
# MAGIC , SUM(NomineeShareholders) AS StructureNomineeShareholders_Count
# MAGIC , SUM(BearerShares) AS StructureIssuedBearerShares_Count
# MAGIC
# MAGIC FROM GCOBTotalSelection

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Legacy2RAF
# MAGIC limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(CddType) 
# MAGIC from Legacy2RAF
# MAGIC -- WHERE HAS_LP_LLP_FLAG = 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from Legacy2RAF
# MAGIC WHERE (HAS_LP_LLP_FLAG = 1
# MAGIC AND CddType not in('Limited partnership', 'Limited liability partnership' , 'Limited liability company', 'Limited liability limited partnership') )
# MAGIC OR CddType = 'Limited liability professional association'

# COMMAND ----------

# DBTITLE 1,Legacy2 Structure Questions
# MAGIC %sql
# MAGIC -- CddType
# MAGIC -- FullLegalName ?
# MAGIC -- ClientType
# MAGIC -- 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Legacy2_risk limit 10
# MAGIC -- StructureNomineeShareholders
# MAGIC -- StructureIssuedBearerShares
# MAGIC -- EntityTypeCharacterizationTypeText = 'Trust, (Dutch) fund or similar legal arrangements'

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

MDM_Files = dbutils.fs.ls(f"abfss://ranz-mdm@{ReadStorage}.dfs.core.windows.net/")

# COMMAND ----------

MDM_Files

# COMMAND ----------

# Loop through the list and print the name of each file
import re

CurrentYearMonth = datetime.today().strftime('%Y%m')
list_count = 0

for file_info in MDM_Files:

    FileName = file_info.name
    split_list = re.split(r"[_,.]", FileName)
    file_recieval_date = split_list[2]

    if file_recieval_date[:6] == CurrentYearMonth:
        break

    list_count += 1

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
# MAGIC --for export
# MAGIC select * from MDM

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input from the Region
# MAGIC
# MAGIC Hi Rick,
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
# MAGIC GR.17.05; [customer company legal form is:] Limited liability partnership (LLP) and/or Limited Partnership (LP)

# COMMAND ----------

# MAGIC %md
# MAGIC ### to count:
# MAGIC
# MAGIC For the columns highlighted in green, discussed with Anil
# MAGIC - (i)ISSUES_BEARER_SHARES:  We don’t proceed with Onboarding if we find customer with Bearer Shares  hence it will be always 'N'
# MAGIC - (ii)NOMINEE_SHAREHOLDER: They are derived from NBEN(Non-Beneficial/Nominee Shareholder) relationship
# MAGIC - (iii)CDD_ENTITY_TYPE : GRAM (MDM does not have it)

# COMMAND ----------

# MAGIC %md
# MAGIC ### RANZ results :
# MAGIC
# MAGIC **(CB only)**
# MAGIC - Trust: 6788 
# MAGIC - Foundation: 0
# MAGIC - LP_LLP: 55
# MAGIC - ISSUES_BEARER_SHARES: 0
# MAGIC - NOMINEE_SHAREHOLDER: 185
# MAGIC
# MAGIC
# MAGIC **ROS**
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Wholesale**
# MAGIC GCOB+Legacy2
# MAGIC

# COMMAND ----------

# DBTITLE 1,MDM select 10
# MAGIC %sql
# MAGIC select * from radar.mdm_ranz 
# MAGIC WHERE CUSTOMER_TYPE_CD = 'PART'
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,MDM Bearer Shares?
# MAGIC %sql
# MAGIC -- These Id's were previously marked as having ISSUES BEARER SHARES
# MAGIC select * 
# MAGIC --CUSTOMER_TYPE_CD
# MAGIC from MDM
# MAGIC WHERE party_ID in (4020329,
# MAGIC 4023192,
# MAGIC 4024907,
# MAGIC 16810069,
# MAGIC 20912898,
# MAGIC 4006233,
# MAGIC 4006452,
# MAGIC 4036055
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC CUSTOMER_TYPE_CD
# MAGIC FROM MDM

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

# MAGIC %sql
# MAGIC select * from mdm limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct CDD_CLIENT_TYPE FROM MDM

# COMMAND ----------

# MAGIC %sql
# MAGIC --PHOLD only
# MAGIC select count(distinct(Contract_Id)) from MDM 
# MAGIC WHERE CLIENT_STATUS = 'Active Client'
# MAGIC AND (REL_TYPE_CD like '%PHOLD%' )

# COMMAND ----------

# MAGIC %sql
# MAGIC --PHOLD only
# MAGIC select count(distinct(Contract_Id)) from MDM_04
# MAGIC WHERE CLIENT_STATUS = 'Active Client'
# MAGIC AND (REL_TYPE_CD like '%PHOLD%' )

# COMMAND ----------

# MAGIC %sql
# MAGIC --PHOLD only
# MAGIC select count(distinct(party_Id)) from MDM 
# MAGIC WHERE CLIENT_STATUS = 'Active Client'
# MAGIC AND (REL_TYPE_CD like '%PHOLD%' OR REL_TYPE_CD like '%AOWN%')

# COMMAND ----------

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
# MAGIC  , count(distinct(party_Id)) as count_parties
# MAGIC from MDM_SEL
# MAGIC GROUP BY Client_BUSINESS_LINE
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC select 
# MAGIC  'GR.17.01_IsTrust' AS QuestionName
# MAGIC  , Client_BUSINESS_LINE
# MAGIC  , count(distinct(party_Id)) as count_parties
# MAGIC from MDM_SEL
# MAGIC WHERE (CUSTOMER_TYPE_CD = 'TRUS' )--OR CUSTOMER_TYPE_CD = 'SMSF')
# MAGIC GROUP BY Client_BUSINESS_LINE
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC -- '2608127' has name The Lindsay and Heather Staier Foundation
# MAGIC select  'GR.17.02_IsStichting' AS QuestionName
# MAGIC , Client_BUSINESS_LINE
# MAGIC , count(distinct(party_Id)) as count_parties
# MAGIC  from MDM_SEL where Global_Client_ID IN (2608127 ,2591655,2612665,2615110,2912234,2931150) 
# MAGIC AND (CUSTOMER_TYPE_CD <> 'TRUS' )
# MAGIC GROUP BY Client_BUSINESS_LINE
# MAGIC union
# MAGIC
# MAGIC
# MAGIC select 'GR.17.05_LP_LLP' AS QuestionName
# MAGIC  , Client_BUSINESS_LINE
# MAGIC  , count(distinct(party_Id)) as count_parties
# MAGIC from MDM_SEL
# MAGIC WHERE CUSTOMER_TYPE_CD = 'LPAR' 
# MAGIC GROUP BY Client_BUSINESS_LINE
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select 'GR.17.06_NomineeShareholders' AS QuestionName 
# MAGIC  , Client_BUSINESS_LINE
# MAGIC  , count(distinct(party_Id)) as count_parties
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



# COMMAND ----------

# DBTITLE 1,TRUSTS
# MAGIC %sql
# MAGIC select CLIENT_BUSINESS_LINE
# MAGIC --, CDD_CLIENT_TYPE
# MAGIC , count(*) as count
# MAGIC , CUSTOMER_TYPE_CD
# MAGIC from MDM
# MAGIC WHERE (CUSTOMER_TYPE_CD = 'TRUS' --OR CUSTOMER_TYPE_CD = 'SMSF'
# MAGIC )
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC --AND REL_TYPE_CD = 'PHOLD'
# MAGIC GROUP BY CLIENT_BUSINESS_LINE, CUSTOMER_TYPE_CD;
# MAGIC
# MAGIC
# MAGIC select 
# MAGIC --, CDD_CLIENT_TYPE
# MAGIC  count(*) as count
# MAGIC , CUSTOMER_TYPE_CD
# MAGIC from MDM
# MAGIC WHERE (CUSTOMER_TYPE_CD = 'TRUS' OR CUSTOMER_TYPE_CD = 'SMSF'
# MAGIC )
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC AND (REL_TYPE_CD like '%PHOLD%' OR REL_TYPE_CD like '%AOWN%')
# MAGIC --AND Client_BUSINESS_LINE IN ('RANZ AU_CB', 'RANZ NZ_CB')
# MAGIC GROUP BY CUSTOMER_TYPE_CD
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC distinct REL_TYPE_CD, REL_ROL_DESC
# MAGIC FROM MDM

# COMMAND ----------

# DBTITLE 1,Nominee Shareholder - Non-Beneficial
# MAGIC %sql
# MAGIC select CLIENT_BUSINESS_LINE
# MAGIC --, CDD_CLIENT_TYPE
# MAGIC , CUSTOMER_TYPE_CD
# MAGIC , REL_TYPE_CD
# MAGIC , REL_ROL_DESC
# MAGIC , *
# MAGIC from MDM
# MAGIC WHERE REL_TYPE_CD Like '%NBEN%'
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC

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

# DBTITLE 1,Limited Partnerships
# MAGIC %sql
# MAGIC select CLIENT_BUSINESS_LINE
# MAGIC --, CDD_CLIENT_TYPE
# MAGIC , count(*) as count
# MAGIC , CUSTOMER_TYPE_CD
# MAGIC from MDM
# MAGIC WHERE (CUSTOMER_TYPE_CD = 'LPAR' --OR CUSTOMER_TYPE_CD = 'SMSF'
# MAGIC )
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC AND REL_TYPE_CD = 'PHOLD'
# MAGIC GROUP BY CLIENT_BUSINESS_LINE, CUSTOMER_TYPE_CD;
# MAGIC
# MAGIC
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

# DBTITLE 1,Tot direct customer per business line
# MAGIC %sql
# MAGIC select count(distinct(PARTY_ID)) as Total_count 
# MAGIC --, CLIENT_BUSINESS_LINE
# MAGIC from MDM
# MAGIC where CLIENT_STATUS = 'Active Client'
# MAGIC AND REL_TYPE_CD = 'PHOLD'
# MAGIC --group by CLIENT_BUSINESS_LINE

# COMMAND ----------

# DBTITLE 1,LPAR All
# MAGIC %sql
# MAGIC Select * 
# MAGIC from
# MAGIC MDM
# MAGIC WHERE (CUSTOMER_TYPE_CD = 'LPAR' --OR CUSTOMER_TYPE_CD = 'SMSF'
# MAGIC )
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC AND REL_TYPE_CD = 'PHOLD'
# MAGIC --AND Client_BUSINESS_LINE IN ('RANZ AU_CB', 'RANZ NZ_CB')

# COMMAND ----------

# DBTITLE 1,LPAR distinct Contracts
# MAGIC %sql
# MAGIC Select distinct CONTRACT_ID 
# MAGIC from
# MAGIC MDM
# MAGIC WHERE (CUSTOMER_TYPE_CD = 'LPAR' --OR CUSTOMER_TYPE_CD = 'SMSF'
# MAGIC )
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC AND REL_TYPE_CD = 'PHOLD'
# MAGIC AND Client_BUSINESS_LINE IN ('RANZ AU_CB', 'RANZ NZ_CB')

# COMMAND ----------

# DBTITLE 1,Select One Foundation
# MAGIC %sql
# MAGIC -- '2608127' has name The Lindsay and Heather Staier Foundation
# MAGIC select * from MDM where Global_Client_ID IN (2608127 ,2591655,
# MAGIC 2612665,
# MAGIC 2615110,
# MAGIC 2912234,
# MAGIC 2931150)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,RANZ
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

# MAGIC %sql
# MAGIC select distinct `Global_CO-location_code` from gcds_client_client

# COMMAND ----------

# MAGIC %md
# MAGIC ### GCOB and Legacy RANZ
# MAGIC
# MAGIC same as RAF. 
# MAGIC GCOB part
# MAGIC Legacy2 part

# COMMAND ----------

# DBTITLE 1,Check which GCO locations to see.
# MAGIC %sql
# MAGIC select distinct globalclientownerlocation
# MAGIC  from party_case_client_details

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
# MAGIC --select count(*) from Legacy2_case_client_details;--25926
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%'; --3006

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct GlobalClientOwnerLocation from Legacy2_case_client_details

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

# DBTITLE 1,Radar - check entity types
# MAGIC %sql
# MAGIC select QuestionText, * 
# MAGIC from radar.RanzMDMGramCDDQuestionsAnswers 
# MAGIC WHERE QuestionId  = 540
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct AnswerText
# MAGIC from radar.RanzMDMGramCDDQuestionsAnswers 
# MAGIC WHERE QuestionId  = 540

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Brazil loads
# MAGIC
# MAGIC Input from Bruno": In Brazil, the Limited Liability Partnership (LLP) structure is not common. Instead, we have the Sociedade Limitada (LTDA), which is the closest form of an LLP. The LTDA offers limited liability protection to partners, like what an LLP offers in other countries.

# COMMAND ----------

import os
import pandas as pd
import datetime

# COMMAND ----------

# DBTITLE 1,Brazil
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

SALZReadStorage = 'salandingzonefecradarprd'


spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Connect to SA GDP
SAGDPStorage = 'edlcorestdbrprod0001'

spark.conf.set("fs.azure.account.auth.type."+SAGDPStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SAGDPStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SAGDPStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SAGDPStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SAGDPStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

dbutils.fs.ls(f'abfss://kn1@{SAGDPStorage}.dfs.core.windows.net/ADRBB_RESPOSTAS_ESCOLHIDAS/1/data/LOADED_DTS=20250407T000825Z')

# COMMAND ----------

ThisDay = (datetime.datetime.today() - datetime.timedelta(0)).strftime('%Y%m%d')
gic_load_dts = 'LOADED_DTS=' +ThisDay+ '*'

# COMMAND ----------

# DBTITLE 1,Read GIC files
load_df = pd.DataFrame({'GDPname':[
'workflow_cadastral_detalhe'
,'pessoa_tipo_cadastro_status'
,'pessoa_tipo_cadastro_risco_dd'
,'pessoa_tipo_cadastro_cliente_detalhe'
,'pessoa_regulatorio_global_tipo_regulatorio'
,'pessoa_regulatorio_global'
,'pessoa_perfil_investidor'
,'pessoa_linha_negocio'
,'pessoa_levantamento_patrimonial'
,'pessoa_email'
,'pessoa_cliente_lembrete_atualizacao' 
,'cliente_tipo_segmento_cliente'
,'cliente_tipo_categoria_cliente'
,'cliente_classificacao_risco'
,'cad_tipo_regulatorio'
,'avalista_cliente'
,'pessoa_complemento'
,'pessoa_agencia_rbb'
,'pessoa'
,'pessoa_duplo_check'
,'pessoa_fisica'
,'pessoa_juridica'
,'endereco'
,'pessoa_endereco'
,'tipo_endereco'
,'tipo_sociedade'
,'cad_municipio'
,'cad_pais'
,'status_tipo_cadastro'
]})


#gic_load_dts
# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gic@{SAGDPStorage}.dfs.core.windows.net/{row.GDPname}/1/data/{gic_load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

#gic_load_dts
# Create TempView for each loading table

spark.read.parquet(f'abfss://gic@{SAGDPStorage}.dfs.core.windows.net/tipo_sociedade/1/data/{gic_load_dts}/*.parquet').createOrReplaceTempView('tipo_sociedade')

# COMMAND ----------

# DBTITLE 1,sample pesso
# MAGIC %sql
# MAGIC select t1.*
# MAGIC  from pessoa t1
# MAGIC  
# MAGIC where nom_completo like '%Limitada%' or nom_completo like '%LTDA%'
# MAGIC -- limit 10
# MAGIC order by COD_INSTITUCIONAL

# COMMAND ----------

# DBTITLE 1,Juridisch type
# MAGIC %sql
# MAGIC select * from PESSOA_JURIDICA limit 10 null

# COMMAND ----------

# DBTITLE 1,Company type
# MAGIC %sql
# MAGIC select * from tipo_sociedade

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC t1.SEQ_PESSOA
# MAGIC , t1.SEQ_HISTORICO
# MAGIC , t1.SEQ_TIPO_SOCIEDADE 
# MAGIC , t2.DES_TIPO_SOCIEDADE
# MAGIC , t5.SEQ_STATUS_TIPO_CADASTRO
# MAGIC from 
# MAGIC PESSOA_JURIDICA t1
# MAGIC LEFT JOIN tipo_sociedade t2 on t1.SEQ_TIPO_SOCIEDADE  = t2.SEQ_TIPO_SOCIEDADE 
# MAGIC INNER JOIN (Select SEQ_PESSOA, MAX(SEQ_HISTORICO) AS SEQ_HISTORICO_MAX FROM PESSOA_JURIDICA GROUP BY SEQ_PESSOA ) as t3 on t1.SEQ_HISTORICO = t3.SEQ_HISTORICO_MAX AND t1.SEQ_PESSOA = t3.SEQ_PESSOA
# MAGIC LEFT JOIN pessoa_tipo_cadastro_status t5 on t1.SEQ_PESSOA = t5.SEQ_PESSOA
# MAGIC INNER JOIN (Select SEQ_PESSOA, MAX(SEQ_HISTORICO) AS SEQ_HISTORICO_MAX FROM pessoa_tipo_cadastro_status GROUP BY SEQ_PESSOA ) as t4 on t5.SEQ_HISTORICO = t4.SEQ_HISTORICO_MAX AND t4.SEQ_PESSOA = t5.SEQ_PESSOA
# MAGIC
# MAGIC where  t1.SEQ_TIPO_SOCIEDADE = 1
# MAGIC and t5.SEQ_STATUS_TIPO_CADASTRO = 1 -- Ativo

# COMMAND ----------

# DBTITLE 1,pesso type?
# MAGIC %sql
# MAGIC select * from pessoa_complemento limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pessoa_tipo_cadastro_status limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pessoa_tipo_cadastro_cliente_detalhe limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(SEQ_TIPO_CADASTRO) from pessoa_tipo_cadastro_cliente_detalhe

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from status_tipo_cadastro

# COMMAND ----------

Customer_status_type ={'Ativo': 'Client',
          'Inativo': 'Former Client',
          'Bloqueado' : 'Blocked Client (Ring-fenced?)',
          'Pré Cadastro' : 'Prospect',
          'Reativação' : 'Re-Onboarding'}


# COMMAND ----------

# DBTITLE 1,Read KN1 files
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'ADRBB_RESPOSTAS_ESCOLHIDAS'
, 'ADRBB_QUESTOES'
, 'ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS'
, 'ADKYC_QUESTOES_COMPORT_CLIENTES'
, 'ADKYC_TIPOS_CONTRAPARTES'
, 'ADKYC_TP_CADASTRAIS'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://kn1@{SAGDPStorage}.dfs.core.windows.net/{row.GDPname}/1/data/LOADED_DTS=20250407*/*').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ADKYC_TIPOS_CONTRAPARTES

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from ADRBB_QUESTOES

# COMMAND ----------

# QuestionId 141 and QuestionId 294 seem to relate to trust question in the structure.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from ADRBB_QUESTOES
# MAGIC where DE_QUESTAO like '%Empresa%'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS limit 3

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ADRBB_RESPOSTAS_ESCOLHIDAS
# MAGIC where texto like '%caract%'
# MAGIC --WHERE ID_Questao = 2
# MAGIC --AND VALOR <> 33

# COMMAND ----------

df_ADRBB_RESPOSTAS_ESCOLHIDAS = spark.sql('select * from ADRBB_RESPOSTAS_ESCOLHIDAS')

# COMMAND ----------

# DBTITLE 1,Connect to DWH
jdbcHostname = f'asafecreportprd.sql.azuresynapse.net'
jdbcPort = 1433
jdbcDatabase = "reportingdwhprd"

jdbcTestTables =["ADRBB_RESPOSTAS_ESCOLHIDAS"]
Service_Principal_Id = f'{app_reg_app_id}'
Service_Principal_Secret = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
connectionProperties = {
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication" : "ActiveDirectoryServicePrincipal",
    "UserName" : Service_Principal_Id ,
    "Password" : Service_Principal_Secret
 }



# COMMAND ----------

# DBTITLE 1,attempt to read table first
spark.read.jdbc(url=jdbcUrl,table="KN1.ADKYC_TIPOS_CONTRAPARTES",properties = connectionProperties)

# COMMAND ----------

df_party_types = spark.sql('select * from ADKYC_TIPOS_CONTRAPARTES')

# COMMAND ----------

# DBTITLE 1,write kn1 party types
create_table = """
CREATE TABLE [KN1].[ADKYC_TIPOS_CONTRAPARTES]
(
	[ID_TIPO_CONTRAPARTE] [int] NULL,
	[DS_TIPO_CONTRAPARTE] [varchar](200) NULL,
	[FL_DESATIVADO] [int]  NULL,
	[EDL_LOAD_DTS] [datetime] NULL,
	[EDL_ACT_DTS] [datetime] NULL

)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
""" 

df_party_types.write.option("truncate",True).mode('append').jdbc(url=jdbcUrl,table="KN1.ADKYC_TIPOS_CONTRAPARTES",properties = connectionProperties)

# COMMAND ----------

df_party_types.schema

# COMMAND ----------

Service_Principal_Id

# COMMAND ----------

from pyspark.sql.functions import substring, length

# COMMAND ----------

# DBTITLE 1,Reshaping texto column to have save-able lenght for synapse
#limit lenght
df_ADRBB_RESPOSTAS_ESCOLHIDAS = df_ADRBB_RESPOSTAS_ESCOLHIDAS.withColumn("TEXTO", substring("TEXTO", 0,1900))

# also filter longer ones
df_ADRBB_RESPOSTAS_ESCOLHIDAS_filtered = df_ADRBB_RESPOSTAS_ESCOLHIDAS.filter((length(df_ADRBB_RESPOSTAS_ESCOLHIDAS["TEXTO"]) <= 255))

# COMMAND ----------

spark.read.jdbc(url=jdbcUrl,table="KN1.ADRBB_RESPOSTAS_ESCOLHIDAS",properties = connectionProperties)

# COMMAND ----------

# DBTITLE 1,Write KN1 into DWH

df_ADRBB_RESPOSTAS_ESCOLHIDAS_filtered.write.mode('append').jdbc(url=jdbcUrl,table="KN1.ADRBB_RESPOSTAS_ESCOLHIDAS",properties = connectionProperties)



# COMMAND ----------

display(df_ADRBB_RESPOSTAS_ESCOLHIDAS.limit(10))

# COMMAND ----------

df_ADRBB_RESPOSTAS_ESCOLHIDAS.schema

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,KN!-GRAM export for Bruno
# MAGIC %sql
# MAGIC -- Brazil export for Bruno.
# MAGIC -- To get all elements from KN1 calling GRAM where questionId in 540, 
# MAGIC --(QuestionId in (71,551,17,363,341) --bearer
# MAGIC -- OR QuestionId in (14, 361, 549)) --nominee shareholders
# MAGIC
# MAGIC Select * 
# MAGIC from Party_RiskModelInstanceQuestionAnswers
# MAGIC WHERE QuestionId in (71,551,17,363,341 ,14, 361, 549, 540)
# MAGIC AND SourceSystemName like '%KN1%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### GIC data from Synapse

# COMMAND ----------

jdbcHostname = f'asafecreportprd.sql.azuresynapse.net'
jdbcPort = 1433
jdbcDatabase = "reportingdwhprd"

jdbcTestTables =["ADRBB_RESPOSTAS_ESCOLHIDAS"]
Service_Principal_Id = f'{app_reg_app_id}'
Service_Principal_Secret = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
connectionProperties = {
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication" : "ActiveDirectoryServicePrincipal",
    "UserName" : Service_Principal_Id ,
    "Password" : Service_Principal_Secret
 }



# COMMAND ----------

# DBTITLE 1,Load from Synapse: GIC / KN1
## Loading from Barends view
## [GEN].[vw_DNB_GR17_Brazil_20241231]

df_DNB_GR17_Brazil_20241231 = spark.read.jdbc(url=jdbcUrl,table="GEN.vw_DNB_GR17_Brazil_20241231",properties = connectionProperties)

# COMMAND ----------

df_DNB_GR17_Brazil_20241231 = df_DNB_GR17_Brazil_20241231.toDF(*[c.replace(' ', '_').replace(',', '_').replace(';', '_').replace('{', '_').replace('}', '_').replace('(', '_').replace(')', '_').replace('\n', '_').replace('\t', '_').replace('=', '_') for c in df_DNB_GR17_Brazil_20241231.columns]); df_DNB_GR17_Brazil_20241231.write.saveAsTable("radar.DNB_GR17_Brazil_20241231")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.DNB_GR17_Brazil_20241231 limit 3

# COMMAND ----------

GR.17.02_IsStichting
0
0
0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 'GR.17.01_IsTrust' AS QuestionName, count(*) from  radar.DNB_GR17_Brazil_20241231 where `GR.17.01_IsTrust` = 1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select 'GR.17.02_IsStichting' AS QuestionName, count(*) from  radar.DNB_GR17_Brazil_20241231 where `GR.17.02_IsStichting` =1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select 'GR.17.05_LP_LLP' AS QuestionName, count(*) from  radar.DNB_GR17_Brazil_20241231 where `GR.17.05_isLLPandorLP`  =1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select 'GR.17.06_NomineeShareholders' AS QuestionName, count(*) from  radar.DNB_GR17_Brazil_20241231 where `GR.17.06_NomineeShareholders` = 1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select 'GR.17.07_IsBearerShares' AS QuestionName, count(*) from  radar.DNB_GR17_Brazil_20241231 where `GR.17.07_IsBearerShares` =1
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


