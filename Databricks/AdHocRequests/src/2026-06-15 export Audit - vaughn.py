# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal:
# MAGIC - export a few files for Audit of Vaughn. 
# MAGIC - call with Joost van Lier on 2026-06-17
# MAGIC
# MAGIC #### Data request
# MAGIC We would like to perform data analysis. For this purpose, we initially request the following data for the period 1/1/2026 to 11/6/2026. We intend to extend this period later to 30/6/2026 so that we have 6 months of recent data.
# MAGIC
# MAGIC - Data request 1: Last year we already requested data called the “All Cases Report” in Radar (see Excel). We would like to receive the same data.
# MAGIC - Data request 2: All cases where the GCOB question “Should the client be submitted for further approval?” is marked “yes” (see screenshot 1), including whether an attachment was added (yes/no).
# MAGIC - Data request 3: All cases where the CDD Risk has been increased (see screenshot 2), including the GCOB field “Is AML, CTF and Sanctions Officer consultation required?” (yes/no) (see screenshot 3).
# MAGIC - Data request 3.1: For all cases from data request 3, we additionally request the GCOB field “Please select at least one of the consultation options” (see screenshot 4).
# MAGIC - Data request 4: All cases where the classification is “special class high” (i.e., high risk based on business rules).
# MAGIC  
# MAGIC ##### contents
# MAGIC For data requests 2 through 4, we would also like to receive the basic client details, such as: ClientID, Full Legal Name, Registered Street and Number, Registered City, and Registered Country
# MAGIC
# MAGIC - In addition to the items discussed, we would also like to receive:  responses to the individual/underlying CDD (yes/no) questions per risk component (not the contents of textfields).Would this be possible?
# MAGIC - For the attached documents in GCOB (actually filenet), we understood that it would be possible to also receive the ‘document type(s)’ and filename(s) in the data.
# MAGIC
# MAGIC Joost van Lier
# MAGIC
# MAGIC Auditor FEC

# COMMAND ----------

import os
import re
from datetime import datetime, timedelta

#Importing pyspark libraries and modules
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark import StorageLevel

#Local File Import
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Set up connect to latest GCOB data from v102
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

#Derive the date for which data has to be processes
date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

SALZReadStorage = 'edlcorestdeuprod0001'

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage +".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage +".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage +".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage +".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage +".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")


# COMMAND ----------

# DBTITLE 1,create latest GCOB GDP data as tables.
# print list of strings for loading spark dfs from GDP
# https://rabobank.collibra.com/asset/018f4e5d-e2b3-7203-b462-8793a5bd1545
load_df = [
'party_case_client_details'
, 'party_documents'
, 'ConsultationType'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# List of dataobjects from GDP
load_df = [
'Party_RiskModelInstanceQuestionAnswers',
'Party_RiskModelInstance'
, 'party_WRCDDModel'
]
#RISKMODEL/Party_Cdd_Case_EventAssessmentQuestionandAnswer
#RISKMODEL/Party_RiskModelCategories
#RISKMODEL/Party_RiskModelInstance
#RISKMODEL/Party_RiskModelInstanceQuestionAnswers
#RISKMODEL/party_WRCDDModel

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')
    
# Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject='client_KeyStoreKey')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_case_client_details 
# MAGIC limit 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from ConsultationType
# MAGIC where consultationRequired = True
# MAGIC limit 3

# COMMAND ----------

# DBTITLE 1,party documents
# MAGIC %sql
# MAGIC select * 
# MAGIC from party_documents limit 3

# COMMAND ----------

# DBTITLE 1,define consultation per source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW ConsultationPerSourceClient AS 
# MAGIC
# MAGIC SELECT
# MAGIC   SourceClient,
# MAGIC   ConsultationRequired,
# MAGIC   concat_ws(',', collect_list(ConsultationType)) AS ConsultationTypes
# MAGIC FROM ConsultationType
# MAGIC GROUP BY SourceClient, ConsultationRequired
# MAGIC

# COMMAND ----------

# DBTITLE 1,select All-cases
# MAGIC %sql 
# MAGIC select GcobId
# MAGIC , GCDSID
# MAGIC , t1.sourceclient
# MAGIC , fulllegalname
# MAGIC -- , RegisteredCity
# MAGIC -- , CountryOfRegistration
# MAGIC -- , casecompletedDate
# MAGIC -- , reviewtypename
# MAGIC -- , casestatusName
# MAGIC -- , FIHubIndicator
# MAGIC -- , globalclientownerlocation
# MAGIC -- , clientlifecyclename
# MAGIC -- , ValidatedRiskLevel
# MAGIC -- , RiskDeviationReason
# MAGIC -- , ClientType
# MAGIC -- , ModelCalculatedRiskLevel	
# MAGIC -- , ModelRecalculatedRiskLevel	
# MAGIC -- , GeographicalRiskLevel	
# MAGIC -- , EntityTypeRiskLevel	
# MAGIC -- , StructureRiskLevel	
# MAGIC -- , SectorRiskLevel	
# MAGIC -- , ProductAndServiceRiskLevel	
# MAGIC -- , PEPRiskLevel	
# MAGIC -- , TransactionRiskLevel	
# MAGIC -- , DistributionRiskLevel	
# MAGIC -- , ThirdPartyRiskLevel
# MAGIC -- , AdverseInfoRiskLevel	
# MAGIC -- , OtherRiskLevel
# MAGIC , SubmitToClientCommittee
# MAGIC , t2.ConsultationRequired
# MAGIC , t2.ConsultationTypes
# MAGIC --, 
# MAGIC --, 
# MAGIC from party_case_client_details as t1
# MAGIC left join ConsultationPerSourceClient as t2 on t1.sourceclient = t2.sourceclient
# MAGIC
# MAGIC where CaseCompletedDate > date('2026-01-01')
# MAGIC

# COMMAND ----------

# DBTITLE 1,select Documents
# MAGIC %sql 
# MAGIC select 
# MAGIC t1.sourceclient
# MAGIC , t2.DocumentSubTypeName	
# MAGIC , t2.DocumentTypeName	
# MAGIC , t2.DocumentFileName
# MAGIC , t2.UploadDate	
# MAGIC , t2.ExpiryDate	
# MAGIC , t2.Purpose
# MAGIC from party_case_client_details as t1
# MAGIC left join party_documents as t2 on t1.sourceclient = t2.source_Entity
# MAGIC
# MAGIC where CaseCompletedDate > date('2026-01-01')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### part with Risk questions and answers
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from Party_RiskModelInstanceQuestionAnswers
# MAGIC limit 10

# COMMAND ----------

df_cases_with_QA = spark.sql("""
                             select 
                                t1.sourceclient
                                , t2.QuestionId
                                , t2.QuestionText
                                , t2.AnswerValue
                                , t2.AnswerText
                                from Party_Case_client_details as t1
                                left join Party_RiskModelInstanceQuestionAnswers as t2 on t1.sourceclient = t2.SourceClient
                                where t1.CaseCompletedDate > date('2026-01-01')

                             """) 




# COMMAND ----------

df_cases_with_QA.write.saveAsTable('AdHocRequests.AuditCDDModel2026_CaseQA')

# COMMAND ----------

# DBTITLE 1,top 3 questionandAnswers
# MAGIC %sql
# MAGIC select Distinct(ModelType)
# MAGIC from Party_RiskModelInstanceQuestionAnswers
# MAGIC limit 3
# MAGIC
# MAGIC -- where AnswerValue in ('TRUE', 'FALSE')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct( CategoryScoreCalculationMethod ) from party_WRCDDModel
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_WRCDDModel
# MAGIC limit 20
# MAGIC
# MAGIC -- of QuestionID, QuestionAnswerStyle = 'Checkbox'
# MAGIC -- 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from Party_RiskModelInstance
# MAGIC limit 3

# COMMAND ----------

# DBTITLE 1,Select Risk-question-Answers for questiontype = boolean.


# COMMAND ----------

# DBTITLE 1,Select Risk-Instances where Special-Case-High-Risk is triggered

