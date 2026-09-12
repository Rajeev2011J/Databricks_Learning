# Databricks notebook source
# MAGIC %md
# MAGIC ## GCOB Legal Entity Client
# MAGIC Goal: to create GCOB All Cases data and store in Delta format into synapse
# MAGIC
# MAGIC ###### <span style="color:orange">Authors:</span>
# MAGIC 1.  abhishek.r.jaiswal@rabobank.com
# MAGIC
# MAGIC ###### <span style="color:orange">Business value:</span>
# MAGIC 1.	
# MAGIC
# MAGIC
# MAGIC ###### <span style="color:blue">Flow of the logic:</span>
# MAGIC 1.  Load and connect to data (create a more specific md cell at the start of that step)
# MAGIC     - GDP sources
# MAGIC 2.  Transform data (create a more specific md cell at that stage)
# MAGIC 3.  Final step exporting data (create a more specific md cell at that stage)

# COMMAND ----------

import pandas as pd
from pandas.tseries.offsets import MonthEnd
import numpy as np
import glob
import pyodbc
import urllib
import os
import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import *

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



# COMMAND ----------

from datetime import datetime, timedelta

currentDate = datetime.today().strftime('%Y%m%d')
currentYear = datetime.today().strftime('%Y')
currentMonth = datetime.today().strftime('%m')
currentDay = datetime.today().strftime('%d')
yesterdayDay = (datetime.today() - timedelta(1)).strftime('%d')

print(currentDate, currentYear, currentMonth, currentDay)

# COMMAND ----------

currentDate = datetime.today().strftime('%Y%m%d')
Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
load_dts = 'LOADED_DTS=' + Yesterdate + 'T000000Z'
print (load_dts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LE Tables - Cases (from GDP)
# MAGIC - CaseService_case_LegalEntityClient
# MAGIC - CaseService_case_Case
# MAGIC - RiskModel_dbo_Model

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = ['CaseService_case_LegalEntityClient'
, 'CaseService_case_DynamicRiskModelInstanceReference'
, 'CaseService_case_Case'
, 'CaseService_case_CddRiskOverview'
, 'CaseService_case_LegalEntitySupervisingExchange'
, 'CaseService_case_ExchangeReference'
, 'CaseService_case_InvolvedStaffMember'
, 'CaseService_case_BusinessLineReference'
, 'CaseService_user_GcobUser'
, 'CaseService_case_CddTypeReference'
, 'CaseService_dbo_RabobankEntity'
, 'RiskModel_dbo_Category'
, 'RiskModel_dbo_InstanceCalculation'
, 'RiskModel_dbo_InstanceCalculationCategory'
, 'RiskModel_dbo_Model'
, 'RiskModel_dbo_Instance'
, 'CaseService_dbo_TeaReason'
, 'CaseService_case_OfficeReference'
, 'CaseService_case_Address'
, 'CaseService_case_CountryReference'
, 'CaseService_case_WorkItem'
, 'CaseService_dbo_EdrReason'
, 'CaseService_case_FatcaAssessment'
, 'CaseService_case_FatcaClassificationReference'
, 'CaseService_case_CrsAssessment'
, 'CaseService_case_CrsClassificationReference'
, 'CaseService_case_LegalEntityClientIdentifier'
, 'CaseService_dbo_SystemIdType'
, 'CaseService_case_ContactPerson'
, 'RiskModel_dbo_InstanceCategory'
, 'CaseService_case_OffBoardingReasonReference'
, 'CaseService_case_RegulatorReference'
, 'CaseService_case_LegalEntitySupervisingRegulator'
, 'CaseService_case_LocalRequirementNorthAmericaWholesale'
, 'CaseService_case_TaxAssessment'
]

# TODO make paramets for GDP load_dts

#for item in load_df:
    #print('df_' + item + ' = spark.read.load(\'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/' + item + '/100/data/' + load_dts + '/*.parquet\', format=\'parquet\')') # .toPandas()

# COMMAND ----------

df_CaseService_case_LegalEntityClient = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_LegalEntityClient/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_DynamicRiskModelInstanceReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_DynamicRiskModelInstanceReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_Case = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_Case/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_CddRiskOverview = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_CddRiskOverview/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_LegalEntitySupervisingExchange = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_LegalEntitySupervisingExchange/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_ExchangeReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_ExchangeReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_InvolvedStaffMember = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_InvolvedStaffMember/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_BusinessLineReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_BusinessLineReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_user_GcobUser = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_user_GcobUser/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_CddTypeReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_CddTypeReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_dbo_RabobankEntity = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_dbo_RabobankEntity/100/data/' + load_dts + '/*.parquet', format='parquet')
df_RiskModel_dbo_Category = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/RiskModel_dbo_Category/100/data/' + load_dts + '/*.parquet', format='parquet')
df_RiskModel_dbo_InstanceCalculation = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/RiskModel_dbo_InstanceCalculation/100/data/' + load_dts + '/*.parquet', format='parquet')
df_RiskModel_dbo_InstanceCalculationCategory = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/RiskModel_dbo_InstanceCalculationCategory/100/data/' + load_dts + '/*.parquet', format='parquet')
df_RiskModel_dbo_Model = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/RiskModel_dbo_Model/100/data/' + load_dts + '/*.parquet', format='parquet')
df_RiskModel_dbo_Instance = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/RiskModel_dbo_Instance/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_dbo_TeaReason = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_dbo_TeaReason/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_OfficeReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_OfficeReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_Address = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_Address/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_CountryReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_CountryReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_WorkItem = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_WorkItem/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_dbo_EdrReason = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_dbo_EdrReason/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_FatcaAssessment = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_FatcaAssessment/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_FatcaClassificationReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_FatcaClassificationReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_CrsAssessment = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_CrsAssessment/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_CrsClassificationReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_CrsClassificationReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_LegalEntityClientIdentifier = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_LegalEntityClientIdentifier/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_dbo_SystemIdType = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_dbo_SystemIdType/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_ContactPerson = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_ContactPerson/100/data/' + load_dts + '/*.parquet', format='parquet')
df_RiskModel_dbo_InstanceCategory = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/RiskModel_dbo_InstanceCategory/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_OffBoardingReasonReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_OffBoardingReasonReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_RegulatorReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_RegulatorReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_LegalEntitySupervisingRegulator = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_LegalEntitySupervisingRegulator/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_LocalRequirementNorthAmericaWholesale = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_LocalRequirementNorthAmericaWholesale/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_TaxAssessment = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_TaxAssessment/100/data/' + load_dts + '/*.parquet', format='parquet')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating GCOB_LE_Cases

# COMMAND ----------

# create list for temp view
#for item in load_df:
#    print('df_' + item + '.' + 'createOrReplaceTempView(\'gcob_' + item + '\')')

# COMMAND ----------

df_CaseService_case_LegalEntityClient.createOrReplaceTempView('gcob_CaseService_case_LegalEntityClient')
df_CaseService_case_DynamicRiskModelInstanceReference.createOrReplaceTempView('gcob_CaseService_case_DynamicRiskModelInstanceReference')
df_CaseService_case_Case.createOrReplaceTempView('gcob_CaseService_case_Case')
df_CaseService_case_CddRiskOverview.createOrReplaceTempView('gcob_CaseService_case_CddRiskOverview')
df_CaseService_case_LegalEntitySupervisingExchange.createOrReplaceTempView('gcob_CaseService_case_LegalEntitySupervisingExchange')
df_CaseService_case_ExchangeReference.createOrReplaceTempView('gcob_CaseService_case_ExchangeReference')
df_CaseService_case_InvolvedStaffMember.createOrReplaceTempView('gcob_CaseService_case_InvolvedStaffMember')
df_CaseService_case_BusinessLineReference.createOrReplaceTempView('gcob_CaseService_case_BusinessLineReference')
df_CaseService_user_GcobUser.createOrReplaceTempView('gcob_CaseService_user_GcobUser')
df_CaseService_case_CddTypeReference.createOrReplaceTempView('gcob_CaseService_case_CddTypeReference')
df_CaseService_dbo_RabobankEntity.createOrReplaceTempView('gcob_CaseService_dbo_RabobankEntity')
df_RiskModel_dbo_Category.createOrReplaceTempView('gcob_RiskModel_dbo_Category')
df_RiskModel_dbo_InstanceCalculation.createOrReplaceTempView('gcob_RiskModel_dbo_InstanceCalculation')
df_RiskModel_dbo_InstanceCalculationCategory.createOrReplaceTempView('gcob_RiskModel_dbo_InstanceCalculationCategory')
df_RiskModel_dbo_Model.createOrReplaceTempView('gcob_RiskModel_dbo_Model')
df_RiskModel_dbo_Instance.createOrReplaceTempView('gcob_RiskModel_dbo_Instance')
df_CaseService_dbo_TeaReason.createOrReplaceTempView('gcob_CaseService_dbo_TeaReason')
df_CaseService_case_OfficeReference.createOrReplaceTempView('gcob_CaseService_case_OfficeReference')
df_CaseService_case_Address.createOrReplaceTempView('gcob_CaseService_case_Address')
df_CaseService_case_CountryReference.createOrReplaceTempView('gcob_CaseService_case_CountryReference')
df_CaseService_case_WorkItem.createOrReplaceTempView('gcob_CaseService_case_WorkItem')
df_CaseService_dbo_EdrReason.createOrReplaceTempView('gcob_CaseService_dbo_EdrReason')
df_CaseService_case_FatcaAssessment.createOrReplaceTempView('gcob_CaseService_case_FatcaAssessment')
df_CaseService_case_FatcaClassificationReference.createOrReplaceTempView('gcob_CaseService_case_FatcaClassificationReference')
df_CaseService_case_CrsAssessment.createOrReplaceTempView('gcob_CaseService_case_CrsAssessment')
df_CaseService_case_CrsClassificationReference.createOrReplaceTempView('gcob_CaseService_case_CrsClassificationReference')
df_CaseService_case_LegalEntityClientIdentifier.createOrReplaceTempView('gcob_CaseService_case_LegalEntityClientIdentifier')
df_CaseService_dbo_SystemIdType.createOrReplaceTempView('gcob_CaseService_dbo_SystemIdType')
df_CaseService_case_ContactPerson.createOrReplaceTempView('gcob_CaseService_case_ContactPerson')
df_RiskModel_dbo_InstanceCategory.createOrReplaceTempView('gcob_RiskModel_dbo_InstanceCategory')
df_CaseService_case_OffBoardingReasonReference.createOrReplaceTempView('gcob_CaseService_case_OffBoardingReasonReference')
df_CaseService_case_RegulatorReference.createOrReplaceTempView('gcob_CaseService_case_RegulatorReference')
df_CaseService_case_LegalEntitySupervisingRegulator.createOrReplaceTempView('gcob_CaseService_case_LegalEntitySupervisingRegulator')
df_CaseService_case_LocalRequirementNorthAmericaWholesale.createOrReplaceTempView('gcob_CaseService_case_LocalRequirementNorthAmericaWholesale')
df_CaseService_case_TaxAssessment.createOrReplaceTempView('gcob_CaseService_case_TaxAssessment')


# COMMAND ----------

# create static dfs and temp view

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
# create pyspark dataframe from lists
df_gcob_static_CaseStatusType = spark.createDataFrame(zip(StatusId_list, Name_list), ['StatusId', 'Name'])
df_gcob_static_CaseStatusType.createOrReplaceTempView('gcob_static_CaseStatusType')




Id_list = [i for i in range(5)]
Description_list = ['Prospect', 'Client', 'FormerProspect', 'ExitClient', 'FormerClient']
# create pyspark dataframe from lists
df_gcob_static_ClientLifeCycleStatus = spark.createDataFrame(zip(Id_list, Description_list), ['Id', 'Description'])
df_gcob_static_ClientLifeCycleStatus.createOrReplaceTempView('gcob_static_ClientLifeCycleStatus')




Id_list = [i+1 for i in range(9)]
Description_list = ['Initial On-Boarding'
, 'Amendment'
, 'Periodic Review'
, 'Event Driven Review'
, 'Client Offboarding'
, 'Change of Client Owner'
, 'Product Offboarding'
, 'Product Offboarding (Resume)'
, 'Tailored Event Assessment']
# create pyspark dataframe from lists
df_gcob_static_ReviewType = spark.createDataFrame(zip(Id_list, Description_list), ['Id', 'Description'])
df_gcob_static_ReviewType.createOrReplaceTempView('gcob_static_ReviewType')



#Static Risk Level
Risk_list = [-1,1,2,3,4,0]
Risk_Description = ['Unknown', 'Low', 'Medium', 'High', 'Unacceptable','Incomplete']
# create pyspark dataframe from lists
df_gcob_static_RiskLevel = spark.createDataFrame(zip(Risk_list, Risk_Description), ['Id', 'Description'])
df_gcob_static_RiskLevel.createOrReplaceTempView('gcob_static_RiskLevel')



#Static Risk Level Mapping
SourceId = [0,1,2,3,4,5,6,7]
DestinationId = [0,0,1,2,3,3,3,4]
# create pyspark dataframe from lists
df_gcob_static_RiskLevelMapping = spark.createDataFrame(zip(SourceId, DestinationId), ['SourceId', 'DestinationId'])
df_gcob_static_RiskLevelMapping.createOrReplaceTempView('gcob_static_RiskLevelMapping')


#Static Further Approval Type
Approval_list = [-1,0,1,2]
Approval_Description = ['Unknown', 'No', 'Yes, Client Committee Approval', 'Yes, Senior Management Approval']
# create pyspark dataframe from lists
df_gcob_static_FurtherApprovalType = spark.createDataFrame(zip(Approval_list, Approval_Description), ['Id', 'Description'])
df_gcob_static_FurtherApprovalType.createOrReplaceTempView('gcob_static_FurtherApprovalType')


# COMMAND ----------

sql_LE_cases = """
WITH CTE_WorkItem AS
(
    select c.LegalEntityClientId, c.CaseReviewType,c.CurrentStatus, w.*
    From gcob_CaseService_case_WorkItem w
    INNER JOIN gcob_CaseService_case_Case AS c ON w.CaseId = c.CaseId
    INNER JOIN gcob_CaseService_case_LegalEntityClient AS cl ON cl.Id = c.LegalEntityClientId
),

WorkItem_Row as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY LegalEntityClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM CTE_WorkItem 
),


LastKYC as
(
select * from CTE_WorkItem where CaseStatusTypeWhenCreated = 3
),
LastKYC_Row as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY LegalEntityClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM LastKYC 
),

LastSentForKYC_Base as
(
select * from CTE_WorkItem where CaseStatusTypeWhenCreated = 2
),
LastSentForKYC as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY LegalEntityClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM LastSentForKYC_Base 
),


INITInPro as
(
select * from CTE_WorkItem where CaseStatusTypeWhenCreated = 1
),
INITInPro_Row as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY LegalEntityClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM INITInPro 
),


Reviewer as
(
    Select * from CTE_WorkItem where CaseStatusTypeWhenCreated = 5
),
Unique_Reviewer as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY LegalEntityClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM Reviewer 
),


LastSent4Eye as
(
    Select * from CTE_WorkItem where CaseStatusTypeWhenCreated = 4
),
LastSent4Eye_Date as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY LegalEntityClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM LastSent4Eye 
),

CompletedDateBase as
(
    select * FROM CTE_WorkItem
    where
    CurrentStatus = 9
    AND
    (
        (CaseReviewType = 1 AND CaseStatusTypeWhenCreated = 8) OR
        (CaseReviewType = 2 AND CaseStatusTypeWhenCreated = 5) OR
        (CaseReviewType = 3 AND CaseStatusTypeWhenCreated = 8) OR
        (CaseReviewType = 4 AND CaseStatusTypeWhenCreated = 8) OR
        (CaseReviewType = 6 AND CaseStatusTypeWhenCreated = 17) OR
        (CaseReviewType = 7 AND CaseStatusTypeWhenCreated = 20) OR
        (CaseReviewType = 8 AND CaseStatusTypeWhenCreated = 21) OR
        (CaseReviewType = 9 AND CaseStatusTypeWhenCreated = 8) 
    )
),
CompletedDate_derive as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY LegalEntityClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM CompletedDateBase 
),
CompletedDate
(
    select * from CompletedDate_derive where ROWNUM = 1
),

DateCreated_Row as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY LegalEntityClientId ORDER BY DateCreated ASC) AS ROWNUM
    FROM CTE_WorkItem 
),

DateComplted_Row as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY LegalEntityClientId ORDER BY DateCompleted DESC) AS ROWNUM
    FROM CTE_WorkItem 
),

ApprovalDateBase
(
    select * FROM CTE_WorkItem
    where
    CaseStatusTypeWhenCreated = 6 
    OR
    (CaseReviewType = 6 AND CaseStatusTypeWhenCreated = 17) 
    
),
ApprovalDate as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY LegalEntityClientId ORDER BY DateCompleted DESC) AS ROWNUM
    FROM ApprovalDateBase 
),

DateSubForSignBase
(
    select * FROM CTE_WorkItem
    where
    CaseStatusTypeWhenCreated in(6,7)   
),
DateSubForSign as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY LegalEntityClientId ORDER BY DateCompleted DESC) AS ROWNUM
    FROM DateSubForSignBase 
),

DateCreated
(
    select * from DateCreated_Row where ROWNUM = 1
),

Risk_Level_Model
(
    select distinct cl.Id as LegalEntityClientId,drmi.ModelId,rlmMc.DestinationId AS ModelCalculatedRiskLevel
    ,rlmRc.DestinationId AS ModelRecalculatedRiskLevel
     FROM
        gcob_CaseService_case_LegalEntityClient cl
        LEFT OUTER JOIN gcob_CaseService_case_DynamicRiskModelInstanceReference drmref 
            ON drmref.LegalEntityClientId = cl.Id
            AND drmref.Expired IS NULL
        LEFT OUTER JOIN gcob_RiskModel_dbo_InstanceCalculation drmic ON drmic.InstanceId = drmref.InstanceId 
            AND drmic.Expired IS NULL
        LEFT OUTER JOIN gcob_RiskModel_dbo_Instance drmi ON drmi.Id = drmref.InstanceId
        LEFT OUTER JOIN gcob_static_RiskLevelMapping rlmMc 
            on rlmMc.SourceId = drmic.CalculatedRiskLevelId
        LEFT OUTER JOIN gcob_static_RiskLevelMapping rlmRc -- mapping model recalculated risk
            on rlmRc.SourceId = drmic.RecalculatedRiskLevelId
),

CTE_RiskCategories AS
(
SELECT ir.LegalEntityClientId
            , i.Id
            , c.Name as CategoryName
            , rlm.DestinationId as CalculatedRiskLevelId
        FROM 
        gcob_RiskModel_dbo_Instance i
        inner join gcob_RiskModel_dbo_InstanceCalculation ic
            on ic.InstanceId = i.Id
        inner join gcob_RiskModel_dbo_InstanceCalculationCategory icc
            on icc.InstanceCalculationId = ic.id
        inner join gcob_CaseService_case_DynamicRiskModelInstanceReference ir
            on ir.InstanceId = i.Id and ir.Expired is null
        inner join gcob_RiskModel_dbo_Category c
            on c.Id = icc.CategoryId
        inner join gcob_static_RiskLevelMapping rlm
            on rlm.SourceId = icc.CalculatedRiskLevelId
        left outer join gcob_RiskModel_dbo_InstanceCategory icat
            on icat.CategoryId = icc.CategoryId
            and icat.InstanceId = i.Id
        where ic.Expired is null
)
, CTE_RiskFactors AS 
   (

    select LegalEntityClientId 
    ,Geographical AS GeoRisk
    ,`Entity Type` AS EntityTypeRisk
    ,Structure AS StructureRisk
    ,Sector AS SectorRisk
    ,`Products and Services` AS ProductRisk
    ,PEP AS PEPRisk
    ,`Transaction` AS TransactionRisk
    ,`Distribution Channel` AS DistributionRisk
    ,`Third Party` AS ThirdPartyRisk
    ,`Adverse Info` AS AdverseInfoRisk
    ,Other AS OtherRisk

    FROM (
    select LegalEntityClientId
    , MAX(CASE WHEN CategoryName = 'Adverse Info' then CalculatedRiskLevelId else NULL END) as `Adverse Info`
    , MAX(CASE WHEN CategoryName = 'Distribution Channel' then CalculatedRiskLevelId else NULL END) as `Distribution Channel`
    , MAX(CASE WHEN CategoryName = 'Entity Type' then CalculatedRiskLevelId else NULL END) as `Entity Type`
    , MAX(CASE WHEN CategoryName = 'General' then CalculatedRiskLevelId else NULL END) as General
    , MAX(CASE WHEN CategoryName = 'Geographical' then CalculatedRiskLevelId else NULL END) as Geographical
    , MAX(CASE WHEN CategoryName = 'Other' then CalculatedRiskLevelId else NULL END) as Other
    , MAX(CASE WHEN CategoryName = 'PEP' then CalculatedRiskLevelId else NULL END) as PEP
    , MAX(CASE WHEN CategoryName = 'Products and Services' then CalculatedRiskLevelId else NULL END) as `Products and Services`
    , MAX(CASE WHEN CategoryName = 'Sector' then CalculatedRiskLevelId else NULL END) as Sector
    , MAX(CASE WHEN CategoryName = 'Structure' then CalculatedRiskLevelId else NULL END) as Structure
    , MAX(CASE WHEN CategoryName = 'Third Party' then CalculatedRiskLevelId else NULL END) as `Third Party`
    , MAX(CASE WHEN CategoryName = 'Transaction' then CalculatedRiskLevelId else NULL END) as `Transaction`

    FROM
        CTE_RiskCategories
    GROUP BY
        LegalEntityClientId
    )
	)

    ,ApprovedClientVersions(GcobId, LatestApprovedVersionNumber) AS (
    SELECT        cl.GcobId, MAX(cl.Version) AS LatestApprovedVersionNumber
    FROM            gcob_CaseService_case_LegalEntityClient AS cl 
    INNER JOIN gcob_CaseService_case_Case AS c ON c.LegalEntityClientId = cl.Id
    WHERE        (c.CurrentStatus in (8,9))
       GROUP BY cl.GcobId), ClientIdentifiers AS
    (SELECT        ci.LegalEntityId, st.Name, ci.ValueOfIdentifier
      FROM            gcob_CaseService_case_LegalEntityClientIdentifier AS ci INNER JOIN
                                gcob_CaseService_dbo_SystemIdType AS st ON st.Id = ci.SystemTypeReferenceId)
    ,

    FinalDecisionDate as 
    (
        select distinct cl.gcobid,max(c.FinalDecisionDate) as FinalDecisionDate
        from gcob_CaseService_case_Case c
        INNER JOIN gcob_CaseService_case_LegalEntityClient cl on cl.Id = c.LegalEntityClientId
        group by cl.gcobid
    ),


    CTE_ApplicableRiskCategories as
    (
        select distinct
			e.id as LegalEntityClientId
			, c.Name as CategoryName
            ,cast(ict.IsRiskMaterial as int) as CalculatedRiskApplId
		FROM gcob_RiskModel_dbo_Instance i
		JOIN gcob_RiskModel_dbo_InstanceCalculation ic 
			on ic.InstanceId = i.Id
		JOIN gcob_CaseService_case_DynamicRiskModelInstanceReference ir
            on ir.InstanceId = i.Id and ir.Expired is null
		JOIN gcob_CaseService_case_LegalEntityClient e 
			on e.Id=ir.LegalEntityClientId
		JOIN gcob_RiskModel_dbo_InstanceCategory ict  
			on ict.InstanceId = i.Id
		JOIN gcob_RiskModel_dbo_Category c 
			on c.Id=ict.CategoryId
		WHERE ic.Expired is null
    ),

    CTE_ApplicableRisk AS 
    (

    select LegalEntityClientId 
    ,Geographical AS GeographicalApplicableRisk
    ,`Entity Type` AS EntityTypeApplicableRisk
    ,Structure AS StructureApplicableRisk
    ,Sector AS SectorApplicableRisk
    ,`Products and Services` AS ProductsApplicableRisk
    ,PEP AS PoliticallyExposedPersonsApplicableRisk
    ,`Transaction` AS TransactionApplicableRisk
    ,`Distribution Channel` AS DistributionChannelApplicableRisk
    ,`Third Party` AS ThirdPartyApplicableRisk
    ,`Adverse Info` AS AdverseInfoApplicableRisk
    ,Other AS OtherApplicableRisk
    FROM  (
    select LegalEntityClientId
    , MAX(CASE WHEN CategoryName = 'Adverse Info' then CalculatedRiskApplId else NULL END) as `Adverse Info`
    , MAX(CASE WHEN CategoryName = 'Distribution Channel' then CalculatedRiskApplId else NULL END) as `Distribution Channel`
    , MAX(CASE WHEN CategoryName = 'Entity Type' then CalculatedRiskApplId else NULL END) as `Entity Type`
    , MAX(CASE WHEN CategoryName = 'General' then CalculatedRiskApplId else NULL END) as General
    , MAX(CASE WHEN CategoryName = 'Geographical' then CalculatedRiskApplId else NULL END) as Geographical
    , MAX(CASE WHEN CategoryName = 'Other' then CalculatedRiskApplId else NULL END) as Other
    , MAX(CASE WHEN CategoryName = 'PEP' then CalculatedRiskApplId else NULL END) as PEP
    , MAX(CASE WHEN CategoryName = 'Products and Services' then CalculatedRiskApplId else NULL END) as `Products and Services`
    , MAX(CASE WHEN CategoryName = 'Sector' then CalculatedRiskApplId else NULL END) as Sector
    , MAX(CASE WHEN CategoryName = 'Structure' then CalculatedRiskApplId else NULL END) as Structure
    , MAX(CASE WHEN CategoryName = 'Third Party' then CalculatedRiskApplId else NULL END) as `Third Party`
    , MAX(CASE WHEN CategoryName = 'Transaction' then CalculatedRiskApplId else NULL END) as `Transaction`
    FROM
        CTE_ApplicableRiskCategories
    GROUP BY
        LegalEntityClientId
    ))
    ,

    Exchange as
    (
        select distinct
        cl.Id as LegalEntityId
        ,efr.ExchangeName
        ,con2.Name ExchangeCountry
        From gcob_CaseService_case_LegalEntityClient AS cl
        Left Join gcob_CaseService_case_LegalEntitySupervisingExchange lecse    
	        On cl.Id = lecse.LegalEntityId
        Left Join gcob_CaseService_case_ExchangeReference efr
	        On lecse.ExchangeReferenceId = efr.ExchangeId
        Left Join gcob_CaseService_case_CountryReference con2
	        On efr.CountryReferenceId = con2.CountryId
        )
    ,ExchangeName AS (
        SELECT DISTINCT 
        LegalEntityId
        ,concat_ws(',', sort_array(collect_list(struct(ExchangeName))).ExchangeName) as ExchangeName
        ,concat_ws(',', sort_array(collect_list(struct(ExchangeCountry))).ExchangeCountry) as ExchangeCountry
        FROM
        Exchange AS ci
		group by LegalEntityId
    ),

    Regulator as
    (
        select distinct
        cl.Id as LegalEntityId
        ,rf.RegulatorName
        ,con1.Name RegulatorCountry
        From gcob_CaseService_case_LegalEntityClient AS cl
        Left Join gcob_CaseService_case_LegalEntitySupervisingRegulator lecsr
	        On cl.Id = lecsr.LegalEntityId
        Left Join gcob_CaseService_case_RegulatorReference rf
	        On lecsr.RegulatorReferenceId = rf.RegulatorId
        Left Join gcob_CaseService_case_CountryReference con1
	        On rf.CountryReferenceId = con1.CountryId
    )

    ,RegulatorName AS (
    SELECT DISTINCT 
        LegalEntityId
		,concat_ws(',', sort_array(collect_list(struct(RegulatorName))).RegulatorName) as RegulatorName
        ,concat_ws(',', sort_array(collect_list(struct(RegulatorCountry))).RegulatorCountry) as RegulatorCountry
    FROM
        Regulator AS ci
		group by LegalEntityId
    )

    ,ClientIdentifiersJoined AS (
        SELECT DISTINCT 
        LegalEntityId
        ,Name
        ,concat_ws(',', sort_array(collect_list(struct(ValueOfIdentifier))).ValueOfIdentifier) as Identifiers
    FROM
        ClientIdentifiers AS ci
	    group by LegalEntityId,Name
)


Select distinct
          c.CaseId
        , cl.FullLegalName
        , cl.GcobId
		, cl.Id AS Identity
        , c.CaseReviewType
        , c.CurrentStatus AS CaseStatusType
		, cl.ClientApprovalDate
        , c.EdrOtherReason
		, c.FinalDecisionDate AS SignOffDate
        , c.DecisionMotivation AS SignOffDecision
        , cl.RingFenceReason
		, cl.IsOperatingAddressDifferentToRegisteredAddress
        , m.Name as RiskModelName
        , rw.Description as ReviewTypeName
        , tr.Description as TeaOtherReason
        , case when c.CaseReviewType=9 then c.TeaOtherReason else null end as TEAReviewReasonDescription
        , cs.Name as CaseStatusName
        , bl.Name as BusinessLineName
        , u1.DisplayName AS GlobalClientOwner
        , re.name AS GlobalClientOwnerLocation
        , o.OfficeName as GlobalClientOwnerOfficeLocation
        , regcr.Name AS CountryOfRegistration
        , CASE WHEN cl.IsOperatingAddressDifferentToRegisteredAddress = 1 THEN opcr.Name ELSE regcr.Name END AS CountryOfOperation
        , lcs.Description as ClientLifeCycleName
        , cdd.CddTypeName AS CddType
        , u2.DisplayName AS 4EyeCheckReviewer
        , edr.Description as EdrReason
        , u3.DisplayName AS CurrentAssignee
        , u2.DisplayName AS Last4EyeCheckReviewer
        , u4.DisplayName AS LastKYCAnalyst
        , 4eye.DateCreated as LastSentFor4EyeCheck
        , asses.DateCreated as LastSentForKYCAssesment
        , cal.Description as ModelCalculatedRiskLevel
        , rcal.Description as ModelRecalculatedRiskLevel
        , cl.NextReviewDateString as NextReviewDate
        , u5.DisplayName AS InitiationInProgressAssignee
        , comp.DateCompleted as CaseCompletedDate
        , crea.DateCreated as CaseCreationDate
        , 4eye.DateCreated as DateSubmittedFor4EyeCheck
        , dsubs.DateCreated as DateSubmittedForSignOff
        , geo.Description as GeographicalRiskLevel
        , Ent.Description as EntityTypeRiskLevel
        , stru.Description as StructureRiskLevel
        , sec.Description as SectorRiskLevel
        , prod.Description as ProductAndServiceRiskLevel
        , pep.Description as PEPRiskLevel
        , tran.Description as TransactionRiskLevel
        , dist.Description as DistributionRiskLevel
        , thir.Description as ThirdPartyRiskLevel
        , adv.Description as AdverseInfoRiskLevel
        , oth.Description as OtherRiskLevel
        , u4.DisplayName AS KYCAssessmentInProgressAssignee
        , asses.DateCreated as ReadyForKYCAssessmentDate
        , cl.IsRingFenced
        , c.ScheduledCompletionDate as ScheduledCompletionDate
        , fatca.IsEligible AS IsEligibleForFatcaAssessment
        , cl.IsEligibleForCrsAssessment
        , fc.Name as FatcaClassification
        , crs.Name as CrsClassification
        , 'GCOB' as SourceSystem
        , val.Description as ValidatedRiskLevel
        , CASE 
            WHEN (appvd.LatestApprovedVersionNumber = cl.Version AND c.CurrentStatus = 8) 
			THEN 'True'
			WHEN (appvd.LatestApprovedVersionNumber = cl.Version AND c.CurrentStatus = 9)
            THEN 'True' ELSE 'False' END 
          AS IsLatestApprovedVersionOfClient
        , cast(fd.FinalDecisionDate as date)
        , fat.Description as SubmitToClientCommittee
        , cp.EmailAddress AS ContactPersonEmailAddress
        , cp.TelephoneNumber AS ContactPersonTelephoneNumber
        , cl.IncorporationNumber
        , cl.FullLegalNameInLocalLanguage
        , cl.IsIncorporated
        , cl.IncorporationDateString as IncorporationDate
        , rega.Street as RegisteredStreet
        , rega.Number as RegisteredNumber
        , rega.PostalCode as RegisteredPostalCode
        , rega.City as RegisteredCity
        , rega.Region as RegisteredRegion
        , opa.Street as OperatingStreet
        , opa.Number as OperatingNumber
        , opa.PostalCode as OperatingPostalCode
        , opa.City as OperatingCity
        , opa.Region as OperatingRegion
        , cl.IsClientListed
        , cl.IsClientRegulated
        , off.Description as OffBoardingReason
        , ad.DateCompleted as ClientOwnerSignOffDate
        , iden.valueOfIdentifier as SalesforceClientID_nCino 
        , cl.FIHubIndicator
        , case when appl.GeographicalApplicableRisk = 1 then 'True' when appl.GeographicalApplicableRisk = 0 then 'False' else null end as GeographicalApplicableRisk
        , case when appl.EntityTypeApplicableRisk = 1 then 'True' when appl.EntityTypeApplicableRisk = 0 then 'False' else null end as EntityTypeApplicableRisk
        , case when appl.StructureApplicableRisk = 1 then 'True' when appl.StructureApplicableRisk = 0 then 'False' else null end as StructureApplicableRisk
        , case when appl.SectorApplicableRisk = 1 then 'True' when appl.SectorApplicableRisk = 0 then 'False' else null end as SectorApplicableRisk
        , case when appl.ProductsApplicableRisk = 1 then 'True' when appl.ProductsApplicableRisk = 0 then 'False' else null end as ProductsApplicableRisk
        , case when appl.PoliticallyExposedPersonsApplicableRisk = 1 then 'True' when appl.PoliticallyExposedPersonsApplicableRisk = 0 then 'False' else null end as PoliticallyExposedPersonsApplicableRisk
        , case when appl.TransactionApplicableRisk = 1 then 'True' when appl.TransactionApplicableRisk = 0 then 'False' else null end as TransactionApplicableRisk
        , case when appl.DistributionChannelApplicableRisk = 1 then 'True' when appl.DistributionChannelApplicableRisk = 0 then 'False' else null end as DistributionChannelApplicableRisk
        , case when appl.ThirdPartyApplicableRisk = 1 then 'True' when appl.ThirdPartyApplicableRisk = 0 then 'False' else null end as ThirdPartyApplicableRisk
        , case when appl.AdverseInfoApplicableRisk = 1 then 'True' when appl.AdverseInfoApplicableRisk = 0 then 'False' else null end as AdverseInfoApplicableRisk
        , case when appl.OtherApplicableRisk = 1 then 'True' when appl.OtherApplicableRisk = 0 then 'False' else null end as OtherApplicableRisk      
        , exn.ExchangeName as NameOfExchange
        , exn.ExchangeCountry as CountryOfExchange
        , reg.RegulatorName as NameOfRegulator
        , reg.RegulatorCountry as CountryOfRegulator
        , wwid.Identifiers AS WWID
        , acbsid.Identifiers AS ACBS
        , rutid.Identifiers AS RUTID
        , isbid.Identifiers AS ISB
        , localNW.HasTaxForm
        , c.LegalEntityClientId
        , taxa.IsTaxIntegrityMaterial
        

FROM 
        gcob_CaseService_case_Case AS c 
        INNER JOIN gcob_CaseService_case_LegalEntityClient AS cl ON cl.Id = c.LegalEntityClientId
        LEFT OUTER JOIN Risk_Level_Model rlm ON cl.Id = rlm.LegalEntityClientId
        LEFT OUTER JOIN gcob_RiskModel_dbo_Model m ON rlm.ModelId = m.Id
        LEFT OUTER JOIN gcob_static_ReviewType rw ON c.CaseReviewType = rw.Id
        LEFT OUTER JOIN gcob_CaseService_dbo_TeaReason tr ON c.ReasonId=tr.Id and c.CaseReviewType=9
        LEFT OUTER JOIN gcob_static_CaseStatusType cs ON c.CurrentStatus = cs.StatusId
        LEFT OUTER JOIN gcob_CaseService_case_InvolvedStaffMember AS sm ON sm.InvolvedStaffMemberId = cl.GlobalClientOwnerId 
        LEFT OUTER JOIN gcob_CaseService_case_BusinessLineReference AS bl ON bl.BusinessLineId = sm.BusinessLineReferenceId
        LEFT OUTER JOIN gcob_CaseService_user_GcobUser u1 on sm.UserReferenceId = u1.UserId
        LEFT OUTER JOIN gcob_CaseService_dbo_RabobankEntity re on sm.RabobankEntityReferenceId = re.Id
        LEFT OUTER JOIN gcob_CaseService_case_OfficeReference o on o.OfficeId = sm.OfficeReferenceId
        LEFT OUTER JOIN gcob_CaseService_case_Address AS opa ON opa.AddressId = cl.OperatingAddressId 
        LEFT OUTER JOIN gcob_CaseService_case_CountryReference AS opcr ON opcr.CountryId = opa.CountryReferenceId 
        LEFT OUTER JOIN gcob_CaseService_case_Address AS rega ON rega.AddressId = cl.RegisteredAddressId 
        LEFT OUTER JOIN gcob_CaseService_case_CountryReference AS regcr ON regcr.CountryId = rega.CountryReferenceId
        LEFT OUTER JOIN gcob_static_ClientLifeCycleStatus lcs on cl.ClientLifeCycleStatusType = lcs.Id
        LEFT OUTER JOIN gcob_CaseService_case_CddTypeReference cdd on cl.cddtypeId = cdd.CddTypeId
        LEFT OUTER JOIN Unique_Reviewer ur ON c.LegalEntityClientId = ur.LegalEntityClientId and ur.ROWNUM = 1 
        LEFT OUTER JOIN gcob_CaseService_user_GcobUser u2 on ur.AssignedUserReferenceId = u2.UserId and ur.ROWNUM = 1
        LEFT OUTER JOIN CompletedDate comp ON c.LegalEntityClientId = comp.LegalEntityClientId
        LEFT OUTER JOIN DateCreated crea ON c.LegalEntityClientId = crea.LegalEntityClientId
        LEFT OUTER JOIN gcob_CaseService_dbo_EdrReason edr on c.ReasonId = edr.id and c.CaseReviewType = 4
        LEFT OUTER JOIN DateComplted_Row dcr ON c.LegalEntityClientId = dcr.LegalEntityClientId and dcr.ROWNUM = 1 
        LEFT OUTER JOIN gcob_CaseService_user_GcobUser u3 on dcr.AssignedUserReferenceId = u3.UserId and dcr.ROWNUM = 1
        LEFT OUTER JOIN LastKYC_Row ana ON c.LegalEntityClientId = ana.LegalEntityClientId and ana.ROWNUM = 1 
        LEFT OUTER JOIN gcob_CaseService_user_GcobUser u4 on ana.AssignedUserReferenceId = u4.UserId and ana.ROWNUM = 1 
        LEFT OUTER JOIN LastSent4Eye_Date 4eye ON c.LegalEntityClientId = 4eye.LegalEntityClientId and 4eye.ROWNUM = 1 
        LEFT OUTER JOIN LastSentForKYC asses ON c.LegalEntityClientId = asses.LegalEntityClientId and asses.ROWNUM = 1 
        LEFT OUTER JOIN gcob_static_RiskLevel cal ON rlm.ModelCalculatedRiskLevel = cal.Id
        LEFT OUTER JOIN gcob_static_RiskLevel rcal ON rlm.ModelRecalculatedRiskLevel = rcal.Id
        LEFT OUTER JOIN INITInPro_Row init ON c.LegalEntityClientId = init.LegalEntityClientId and init.ROWNUM = 1 
        LEFT OUTER JOIN gcob_CaseService_user_GcobUser u5 on init.AssignedUserReferenceId = u5.UserId and init.ROWNUM = 1 
        LEFT OUTER JOIN DateSubForSign dsubs ON c.LegalEntityClientId = dsubs.LegalEntityClientId and dsubs.ROWNUM = 1  
        LEFT OUTER JOIN CTE_RiskFactors rf ON cl.Id = rf.LegalEntityClientId
        LEFT OUTER JOIN gcob_static_RiskLevel geo ON rf.GeoRisk = geo.Id
        LEFT OUTER JOIN gcob_static_RiskLevel Ent ON rf.EntityTypeRisk = Ent.Id
        LEFT OUTER JOIN gcob_static_RiskLevel stru ON rf.StructureRisk = stru.Id
        LEFT OUTER JOIN gcob_static_RiskLevel sec ON rf.SectorRisk = sec.Id
        LEFT OUTER JOIN gcob_static_RiskLevel prod ON rf.ProductRisk = prod.Id
        LEFT OUTER JOIN gcob_static_RiskLevel pep ON rf.PEPRisk = pep.Id
        LEFT OUTER JOIN gcob_static_RiskLevel tran ON rf.TransactionRisk = tran.Id
        LEFT OUTER JOIN gcob_static_RiskLevel dist ON rf.DistributionRisk = dist.Id
        LEFT OUTER JOIN gcob_static_RiskLevel thir ON rf.ThirdPartyRisk = thir.Id
        LEFT OUTER JOIN gcob_static_RiskLevel adv ON rf.AdverseInfoRisk = adv.Id
        LEFT OUTER JOIN gcob_static_RiskLevel oth ON rf.OtherRisk = oth.Id
        LEFT OUTER JOIN gcob_CaseService_case_FatcaAssessment fatca ON cl.FatcaAssessmentId = fatca.FatcaAssessmentId
        LEFT OUTER JOIN gcob_CaseService_case_FatcaClassificationReference fc ON fatca.FatcaClassificationId = fc.FatcaClassificationId
        LEFT OUTER JOIN gcob_CaseService_case_CrsAssessment cra ON cl.CrsAssessmentId = cra.CrsAssessmentId
        LEFT OUTER JOIN gcob_CaseService_case_CrsClassificationReference crs ON cra.CrsClassificationReferenceId = crs.CrsClassificationId
        LEFT OUTER JOIN ApprovedClientVersions AS appvd ON appvd.GcobId = cl.GcobId
        LEFT OUTER JOIN FinalDecisionDate as fd ON fd.gcobid = cl.gcobid
        LEFT OUTER JOIN gcob_static_FurtherApprovalType fat ON fat.Id = c.FurtherApprovalTypeId
        LEFT OUTER JOIN gcob_CaseService_case_ContactPerson cp ON cp.ContactPersonId = cl.ContactPersonId
        LEFT OUTER JOIN gcob_CaseService_case_OffBoardingReasonReference off ON c.reasonId = off.OffBoardingReasonReferenceId and c.CaseReviewType in(6,7)
        LEFT OUTER JOIN ApprovalDate ad ON c.LegalEntityClientId = ad.LegalEntityClientId and ad.ROWNUM = 1
        LEFT OUTER JOIN gcob_CaseService_case_LegalEntityClientIdentifier iden ON c.LegalEntityClientId = iden.LegalEntityId and iden.SystemTypeReferenceId = 129
        LEFT OUTER JOIN CTE_ApplicableRisk appl ON cl.Id = appl.LegalEntityClientId
        LEFT OUTER JOIN ExchangeName exn ON c.LegalEntityClientId = exn.LegalEntityId
        LEFT OUTER JOIN RegulatorName reg ON c.LegalEntityClientId = reg.LegalEntityId
        LEFT OUTER JOIN ClientIdentifiersJoined AS wwid ON wwid.LegalEntityId = cl.Id AND wwid.Name = 'WWID' 
        LEFT OUTER JOIN ClientIdentifiersJoined AS acbsid ON acbsid.LegalEntityId = cl.Id AND acbsid.Name = 'ACBS' 
        LEFT OUTER JOIN ClientIdentifiersJoined AS rutid ON rutid.LegalEntityId = cl.Id AND rutid.Name = 'RUT ID' 
        LEFT OUTER JOIN ClientIdentifiersJoined AS isbid ON isbid.LegalEntityId = cl.Id AND isbid.Name = 'ISB'
        LEFT OUTER JOIN gcob_CaseService_case_LocalRequirementNorthAmericaWholesale AS localNW ON localNW.LocalRequirementId = cl.LocalRequirementNorthAmericaWholesaleId
        LEFT OUTER JOIN gcob_CaseService_case_CddRiskOverview AS cddo ON cddo.CddRiskOverviewId = cl.CddRiskOverviewId
        LEFT OUTER JOIN gcob_static_RiskLevel val ON cddo.ValidatedCddRisk = val.Id
        LEFT OUTER JOIN gcob_CaseService_case_TaxAssessment taxa on cl.TaxAssessmentId = taxa.TaxAssessmentId
"""

# COMMAND ----------

df_LE_cases = spark.sql(sql_LE_cases)

# COMMAND ----------

df_LE_cases= df_LE_cases.withColumn("BusinessDate", F.lit(BusinessDate))

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS WR_RADAR" )
#spark.sql("CREATE SCHEMA IF NOT EXISTS GCOB" )
spark.sql("DROP TABLE IF EXISTS GCOB.LE_Cases" )
df_LE_cases.write.mode("overwrite").saveAsTable("WR_RADAR.LE_Cases")

# COMMAND ----------


