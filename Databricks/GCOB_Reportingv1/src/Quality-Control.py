# Databricks notebook source
import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

QC_dataobject = 'QC_Dashboard'

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
#'party_AllCasesReport'
'party_case_client_details'
, 'party_workitem'
, 'party_quality_control_answers'
, 'party_request_for_information'
, 'party_client'
, 'party_products_and_services'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW QC_Join AS
# MAGIC select distinct 
# MAGIC Qc.*
# MAGIC ,substr(Qc.questiontext,0,100) as substr_questiontext
# MAGIC ,clients.GlobalKycPortfolioNew
# MAGIC ,c.FIHubIndicator
# MAGIC ,c.CaseCreationDate
# MAGIC ,c.CaseStatusName
# MAGIC ,c.ReviewTypeName
# MAGIC ,'Case_Count' as dummy
# MAGIC ,case 
# MAGIC   when Qc.ReviewerElement = 'adverse-info' then 'CDD - AdverseInfo'
# MAGIC   when Qc.questiontext like '%EDR was initiated in time%' then 'Case Info : Integral review'
# MAGIC   when Qc.ReviewerElement = 'client-behaviour' then 'Client Behaviour - Expected Transaction pattern'
# MAGIC   --when Qc.ReviewerElement = 'client-structure' then 'client-structure'
# MAGIC   
# MAGIC   when Qc.questiontext like '%identity and authority the of authorized representatives%' then 'Client Structure - Identity and authority the of authorized representatives'
# MAGIC   when Qc.questiontext like '%background screening has been performed%' then 'Client Structure - Background screening'
# MAGIC   when Qc.questiontext like '%client structure (i.e. UBO%' then 'Client structure - UBO, Authorised Reps, Directors, Intermediate Beneficial Owners and Guarantors'
# MAGIC   when Qc.questiontext like '%Adverse information screening was conducted%' then 'Client Structure - Adverse Information  Screening'
# MAGIC   when Qc.questiontext like '%PEP screening was conducted%' then 'Client Structure - PEP Screening'
# MAGIC   when Qc.questiontext like '%all Related parties, with ties%' then 'Client Structure - Related Parties - Tie to Rabobank customer'
# MAGIC   when Qc.questiontext like '%minimum two directors have been identified%' then 'Client structure - Minimum 2 directors'
# MAGIC   when Qc.questiontext like '%screening for Sanctions%Supporting documentation/evidence has been placed on file%' then 'client-structure - Rabobank Internal Watchlist'
# MAGIC   when Qc.questiontext like '%Client ownership tree and Ultimate Beneficial%' then 'client-structure - Client ownership tree and Ultimate Beneficial Owner(s) Identification'
# MAGIC   when Qc.questiontext like '%UBO(s) are verified in all circumstances%' then 'client-structure - UBO Verification'
# MAGIC   when Qc.questiontext like '%authorized representatives are identified%' then 'Client Structure - authorized representatives are identified and recorded properly'
# MAGIC   
# MAGIC   when Qc.ReviewerElement = 'crs' then 'CRS'
# MAGIC   --when Qc.questiontext like '%identified, determine whether, all fields%' or Qc.questiontext like '%case of identified risks the relevant follow-up questions%' then 'EDD - Assessment'
# MAGIC   when Qc.questiontext like '%Entity Type CDD Risk indicators%' or Qc.questiontext like '%question regarding Entity type risk%' then 'CDD - Entity Type - Reliable Investigations with no contradiction'
# MAGIC   when Qc.questiontext like '%FATCA%' then 'FATCA'
# MAGIC   when Qc.questiontext like '%Geographical%' and Qc.ReviewerElement = 'geographical' then 'CDD - Geographical - Reliable Investigations with no contradiction'
# MAGIC   when Qc.questiontext like '%(If applicable)%' and Qc.ReviewerElement = 'id-and-verify' then 'id-and-verify - Determine relationship to other clients'
# MAGIC   when Qc.questiontext like '%Determine that the client%' and Qc.ReviewerElement = 'id-and-verify' then 'id-and-verify - client entity information is up-to-date'
# MAGIC   when Qc.questiontext like '%information on client type%' and Qc.ReviewerElement = 'id-and-verify' then 'id-and-verify - information on client type, stock exchange'
# MAGIC   when Qc.questiontext like '%minimum client information%' and Qc.ReviewerElement = 'id-and-verify' then 'id-and-verify - Minimum client information'
# MAGIC   when Qc.questiontext like '%Determine whether Adverse information%' and Qc.ReviewerElement = 'id-and-verify' then 'id-and-verify - Adverse information screening'
# MAGIC   when Qc.questiontext like '%Determine whether screening%' and Qc.ReviewerElement = 'id-and-verify' then 'id-and-verify - screening for Sanctions and the Rabobank Internal Watchlist'
# MAGIC   when Qc.questiontext like '%required information has been obtained and recorded%' and Qc.ReviewerElement = 'id-and-verify' then 'id-and-verify - Customer ID verification'
# MAGIC   when Qc.questiontext like '%basis of documents, data or information from independent and reliable sources%' and Qc.ReviewerElement = 'id-and-verify' then 'id-and-verify - Customer ID verification based on documents'
# MAGIC   when Qc.questiontext like '%local regulatory requirements%' and Qc.ReviewerElement = 'local-reqs' then 'local-reqs - Local Regulatory Requirements'
# MAGIC   when Qc.questiontext like '%MiFID%' and Qc.ReviewerElement = 'mifid' then 'mifid'
# MAGIC   when Qc.ReviewerElement = 'other-cdd' then 'Other CDD - Reliable Investigations with no contradiction'
# MAGIC   when Qc.ReviewerElement = 'overview' then 'validated CDD - Overview'
# MAGIC   when Qc.ReviewerElement = 'pep' then 'CDD - PEP- Reliable Investigations with no contradiction'
# MAGIC   when Qc.ReviewerElement = 'products-provided' then 'products and services are recorded correctly and completely'
# MAGIC   when Qc.ReviewerElement = 'products-services' then 'CDD - Products-services - CDD assessment with no contradiction'
# MAGIC   when Qc.questiontext like '%Distribution Channel%' and Qc.ReviewerElement = 'profile' then 'Profile - Distribution Channel'
# MAGIC   when (Qc.questiontext like '%Products & Services CDD Risk%' or Qc.questiontext like '%regarding Product and Service risk%') and Qc.ReviewerElement = 'profile' then 'Profile - products & Services'
# MAGIC   when Qc.questiontext like '%Sector CDD Risk%' and Qc.ReviewerElement = 'profile' then 'Profile - Sector'
# MAGIC   when Qc.questiontext like '%Purpose and envisaged%' and Qc.ReviewerElement = 'profile' then 'Profile - Purpose and envisaged Nature of the customer'
# MAGIC   when Qc.questiontext like '%Funds/Assets%' and Qc.ReviewerElement = 'profile' then 'Profile - Source of Funds/Assets to be used in the business relationship'
# MAGIC   when Qc.questiontext like '%Source of Wealth%' and Qc.ReviewerElement = 'profile' then 'Profile -  Source of Wealth'
# MAGIC   when Qc.questiontext like '%regarding Sector risk%' and Qc.ReviewerElement = 'profile' then 'Profile - Sector Risk'
# MAGIC   when Qc.ReviewerElement = 'rabobank-personnel' then 'CO - Recorded Correct and complete'
# MAGIC   when Qc.ReviewerElement = 'sector' then 'CDD -Sector - CDD assessment with no contradiction'
# MAGIC   when Qc.questiontext like '%control measures%' and Qc.ReviewerElement = 'sign-off' then 'Sign off - Control Measures'
# MAGIC   when Qc.questiontext like '%the client committee%' and Qc.ReviewerElement = 'sign-off' then 'Sign off - decision, to submit the client file to the client committee or Senior Management'
# MAGIC   when Qc.questiontext like '%overall opinion%' and Qc.ReviewerElement = 'sign-off' then 'Sign off - Overall opinion'
# MAGIC   when Qc.ReviewerElement = 'structure' then 'CDD -Structure - CDD assessment with no contradiction'
# MAGIC   when Qc.ReviewerElement = 'tax-residencies' then 'Tax residencies'
# MAGIC   when Qc.ReviewerElement = 'third-party' then 'CDD - Third Party - CDD Assesment with no contradiction'
# MAGIC   when Qc.ReviewerElement = 'transaction' then 'CDD - Transaction - CDD Risk accuracy with no contradiction'
# MAGIC   when Qc.ReviewerElement = 'distribution' then 'CDD - Distribution'
# MAGIC   when Qc.ReviewerElement = 'edd-adverse-info' then 'edd-adverse-info'
# MAGIC   when Qc.ReviewerElement = 'edd-distribution' then 'edd-distribution'
# MAGIC   when Qc.ReviewerElement = 'edd-entity-type' then 'edd-entity-type'
# MAGIC   when Qc.ReviewerElement = 'edd-general' then 'edd-general'
# MAGIC   when Qc.ReviewerElement = 'edd-geographical' then 'edd-geographical'
# MAGIC   when Qc.ReviewerElement = 'edd-pep' then 'edd-pep'
# MAGIC   when Qc.ReviewerElement = 'edd-products-services' then 'edd-products-services'
# MAGIC   when Qc.ReviewerElement = 'edd-sector' then 'edd-sector'
# MAGIC   when Qc.ReviewerElement = 'edd-structure' then 'edd-structure'
# MAGIC   when Qc.ReviewerElement = 'edd-third-party' then 'edd-third-party'
# MAGIC   when Qc.ReviewerElement = 'edd-transaction' then 'edd-transaction'
# MAGIC   when Qc.ReviewerElement = 'edd-risk' then 'edd-risk'
# MAGIC else Qc.questiontext
# MAGIC end as Question_Catagory
# MAGIC ,c.IsLatestApprovedVersionOfClient
# MAGIC ,Cases.4EyeUserTeam
# MAGIC ,Cases.KYCUserTeam
# MAGIC ,c.clienttype
# MAGIC From party_quality_control_answers qc
# MAGIC Left join party_case_client_details c on qc.sourceclient = c.sourceclient
# MAGIC Left join radar.clients clients on qc.gcobid = clients.GcobId
# MAGIC Left join radar.Cases Cases on qc.SourceClient = Cases.SourceClient
# MAGIC where 
# MAGIC qc.ReviewerElementConclusion in ('Adequate','NotAdequate')
# MAGIC and c.CaseStatusName not in ('Cancelled')--'Completed',
# MAGIC and (
# MAGIC   c.FIHubIndicator = 'true'
# MAGIC   OR clients.GlobalKycPortfolioNew in
# MAGIC   (
# MAGIC     'Acorn',
# MAGIC         'Antwerp Core Lending',
# MAGIC         'Dublin Core Lending',
# MAGIC         'Frankfurt Core Lending',
# MAGIC         'Frankfurt International Services',
# MAGIC         'London Advisory & Investments',
# MAGIC         'London Core Lending',
# MAGIC         'London International Services',
# MAGIC         'Madrid Core Lending',
# MAGIC         'Milan Core Lending',
# MAGIC         'NL Advisory & Investments',
# MAGIC         'NL Core Lending',
# MAGIC         'NL Structured Lending',
# MAGIC         'Paris Core Lending'
# MAGIC   )
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW UniqueCase AS
# MAGIC select distinct gcobid,caseID,sourceClient,DateCreated
# MAGIC From QC_Join
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ReviewNumber AS
# MAGIC select 
# MAGIC row_number() over (partition by caseID order by DateCreated asc) as review_Number
# MAGIC ,* 
# MAGIC from UniqueCase 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Quality_Control AS
# MAGIC select distinct 
# MAGIC c.*
# MAGIC ,t.review_Number
# MAGIC from QC_Join c 
# MAGIC left join ReviewNumber t on c.sourceclient = t.sourceclient and c.DateCreated = t.DateCreated
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW case_count AS
# MAGIC select  'Case_Count' as dummy
# MAGIC ,count(distinct(CaseId)) as Case_Count
# MAGIC from Quality_Control 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW QC AS
# MAGIC select distinct 
# MAGIC qc.GCOBID
# MAGIC ,qc.ClientId
# MAGIC ,qc.caseID
# MAGIC ,qc.fulllegalname
# MAGIC ,qc.QC_Analyst
# MAGIC ,qc.questiontext
# MAGIC ,qc.ReviewerElement
# MAGIC ,qc.ReviewerElementConclusion
# MAGIC ,qc.ReviewerElementText
# MAGIC ,qc.DateCreated
# MAGIC ,qc.DateCompleted
# MAGIC ,qc.SourceClient
# MAGIC ,qc.substr_questiontext
# MAGIC ,qc.GlobalKycPortfolioNew
# MAGIC ,qc.FIHubIndicator
# MAGIC ,qc.CaseCreationDate
# MAGIC ,qc.CaseStatusName
# MAGIC ,qc.ReviewTypeName
# MAGIC --,adn.ReviewerElement as QuestionGroup_ReviewerElement
# MAGIC --,adn.ReviewerElementConclusion as QuestionGroup_ReviewerElementConclusion
# MAGIC --,adn.ad_notad_count
# MAGIC ,cc.Case_Count as Total_Case_Count
# MAGIC --,(adn.ad_notad_count/cc.Case_Count) as test_element_Percent
# MAGIC --,adre.ReviewerElement as ReGroup_ReviewerElement
# MAGIC --,adre.ReviewerElementConclusion as ReGroup_ReviewerElementConclusion
# MAGIC --,adre.ad_notad_RE_count
# MAGIC ,qc.review_Number
# MAGIC ,qc.Question_Catagory
# MAGIC ,qc.KYC_Analyst
# MAGIC ,qc.IsLatestApprovedVersionOfClient
# MAGIC ,qc.4EyeUserTeam as QCTeam
# MAGIC ,qc.KYCUserTeam as CDDTeam
# MAGIC ,qc.clienttype
# MAGIC ,case   
# MAGIC     when qc.clienttype='Legal Entity' then concat('LE_', qc.Gcobid)
# MAGIC     when qc.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',qc.Gcobid) else 'NA' 
# MAGIC end as UniqueGcobId
# MAGIC From Quality_Control qc
# MAGIC --Inner join ad_notad_Q_count adn on qc.substr_questiontext = adn.substr_questiontext
# MAGIC Inner join case_count cc on qc.dummy = cc.dummy
# MAGIC --Inner join ad_notad_RE_count adre on qc.ReviewerElement = adre.ReviewerElement

# COMMAND ----------

df_QC=spark.table('QC')

# COMMAND ----------

df_QC=df_QC.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

save_to_saradar_storage_account(df_QC, QC_dataobject)

# COMMAND ----------

# df_QC.createOrReplaceTempView("QC")

# COMMAND ----------

# %sql
# drop table IF EXISTS radar.QC_Dashboard

# COMMAND ----------

# spark.sql('select * from QC').write.mode('overwrite').saveAsTable('radar.QC_Dashboard')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW QC_Location AS
# MAGIC SELECT distinct
# MAGIC pc.SourceClient, pc.FullLegalName, pc.ClientType, pc.globalclientownerlocation, ps.ProductOfferingLocation,ps.BookingEntityLocation
# MAGIC FROM party_case_client_details AS pc
# MAGIC INNER JOIN party_products_and_services AS ps
# MAGIC   ON pc.SourceClient = ps.SourceClient
# MAGIC WHERE pc.globalclientownerlocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya') 
# MAGIC       OR ps.ProductOfferingLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya') 
# MAGIC       OR ps.BookingEntityLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya') 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW QC_Europe_Africa AS
# MAGIC select distinct
# MAGIC q.* 
# MAGIC From QC q
# MAGIC inner join QC_Location l on q.SourceClient = l.SourceClient

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS E_and_A

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS E_and_A.QC_Dashboard

# COMMAND ----------

spark.sql('select * from QC_Europe_Africa').write.mode('overwrite').saveAsTable('E_and_A.QC_Dashboard')

# COMMAND ----------

#if app_reg_app_id == '8216d5d3-c254-44ea-829e-6d19cba36fa3':
#    spark.sql('select * from QC').write.mode('overwrite').saveAsTable('radar.QC_Dashboard')
#    print("Data Loaded in prod")
#else:
#    spark.sql('select * from QC_Europe_Africa').write.mode('overwrite').saveAsTable('radar.QC_Dashboard')
#    print("Data loaded in lower enviornment")
