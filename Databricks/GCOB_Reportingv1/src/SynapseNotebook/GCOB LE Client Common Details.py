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
import os
from datetime import *
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
, 'CaseService_case_Case'
, 'CaseService_case_InvolvedStaffMember'
, 'CaseService_case_BusinessLineReference'
, 'CaseService_user_GcobUser'
, 'CaseService_dbo_RabobankEntity'
, 'CaseService_case_OfficeReference'
, 'CaseService_case_Address'
, 'CaseService_case_CountryReference'
, 'CaseService_case_LegalEntityClientIdentifier'
, 'CaseService_dbo_SystemIdType'
]

# TODO make paramets for GDP load_dts

#for item in load_df:
#    print('df_' + item + ' = spark.read.load(\'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/' + item + '/100/data/' + load_dts + '/*.parquet\', format=\'parquet\')') # .toPandas()

# COMMAND ----------

df_CaseService_case_LegalEntityClient = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_LegalEntityClient/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_Case = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_Case/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_InvolvedStaffMember = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_InvolvedStaffMember/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_BusinessLineReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_BusinessLineReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_user_GcobUser = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_user_GcobUser/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_dbo_RabobankEntity = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_dbo_RabobankEntity/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_OfficeReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_OfficeReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_Address = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_Address/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_CountryReference = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_CountryReference/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_case_LegalEntityClientIdentifier = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_case_LegalEntityClientIdentifier/100/data/' + load_dts + '/*.parquet', format='parquet')
df_CaseService_dbo_SystemIdType = spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService_dbo_SystemIdType/100/data/' + load_dts + '/*.parquet', format='parquet')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating GCOB_LE_Cases

# COMMAND ----------

# create list for temp view
#for item in load_df:
#    print('df_' + item + '.' + 'createOrReplaceTempView(\'gcob_' + item + '\')')

# COMMAND ----------

df_CaseService_case_LegalEntityClient.createOrReplaceTempView('gcob_CaseService_case_LegalEntityClient')
df_CaseService_case_Case.createOrReplaceTempView('gcob_CaseService_case_Case')
df_CaseService_case_InvolvedStaffMember.createOrReplaceTempView('gcob_CaseService_case_InvolvedStaffMember')
df_CaseService_case_BusinessLineReference.createOrReplaceTempView('gcob_CaseService_case_BusinessLineReference')
df_CaseService_user_GcobUser.createOrReplaceTempView('gcob_CaseService_user_GcobUser')
df_CaseService_dbo_RabobankEntity.createOrReplaceTempView('gcob_CaseService_dbo_RabobankEntity')
df_CaseService_case_OfficeReference.createOrReplaceTempView('gcob_CaseService_case_OfficeReference')
df_CaseService_case_Address.createOrReplaceTempView('gcob_CaseService_case_Address')
df_CaseService_case_CountryReference.createOrReplaceTempView('gcob_CaseService_case_CountryReference')
df_CaseService_case_LegalEntityClientIdentifier.createOrReplaceTempView('gcob_CaseService_case_LegalEntityClientIdentifier')
df_CaseService_dbo_SystemIdType.createOrReplaceTempView('gcob_CaseService_dbo_SystemIdType')



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



# COMMAND ----------

sql_LE_Details = """
WITH 
    ApprovedClientVersions(GcobId, LatestApprovedVersionNumber) AS (
    SELECT        cl.GcobId, MAX(cl.Version) AS LatestApprovedVersionNumber
    FROM            gcob_CaseService_case_LegalEntityClient AS cl 
    INNER JOIN gcob_CaseService_case_Case AS c ON c.LegalEntityClientId = cl.Id
    WHERE        (c.CurrentStatus in (8,9))
       GROUP BY cl.GcobId), ClientIdentifiers AS
    (SELECT        ci.LegalEntityId, st.Name, ci.ValueOfIdentifier
      FROM            gcob_CaseService_case_LegalEntityClientIdentifier AS ci INNER JOIN
                                gcob_CaseService_dbo_SystemIdType AS st ON st.Id = ci.SystemTypeReferenceId)


Select distinct
          c.CaseId
        , cl.FullLegalName
        , cl.GcobId
		, cl.Id AS Identity
        , rw.Description as ReviewTypeName
        , cs.Name as CaseStatusName
        , bl.Name as BusinessLineName
        , u1.DisplayName AS GlobalClientOwner
        , re.name AS GlobalClientOwnerLocation
        , o.OfficeName as GlobalClientOwnerOfficeLocation
        , regcr.Name AS CountryOfRegistration
        , CASE WHEN cl.IsOperatingAddressDifferentToRegisteredAddress = 1 THEN opcr.Name ELSE regcr.Name END AS CountryOfOperation
        , lcs.Description as ClientLifeCycleName
        , CASE 
            WHEN (appvd.LatestApprovedVersionNumber = cl.Version AND c.CurrentStatus = 8) 
			THEN 'True'
			WHEN (appvd.LatestApprovedVersionNumber = cl.Version AND c.CurrentStatus = 9)
            THEN 'True' ELSE 'False' END 
          AS IsLatestApprovedVersionOfClient
         , c.LegalEntityClientId
        
FROM 
        gcob_CaseService_case_Case AS c 
        INNER JOIN gcob_CaseService_case_LegalEntityClient AS cl ON cl.Id = c.LegalEntityClientId
        LEFT OUTER JOIN gcob_static_ReviewType rw ON c.CaseReviewType = rw.Id
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
        LEFT OUTER JOIN ApprovedClientVersions AS appvd ON appvd.GcobId = cl.GcobId
 
"""

# COMMAND ----------

df_LE_Common_Details = spark.sql(sql_LE_Details)

# COMMAND ----------

df_LE_Common_Details= df_LE_Common_Details.withColumn("BusinessDate", F.lit(BusinessDate))

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS WR_RADAR" )
#spark.sql("CREATE SCHEMA IF NOT EXISTS GCOB" )
spark.sql("DROP TABLE IF EXISTS dbo.LE_Common_Details" )
df_LE_Common_Details.write.mode("overwrite").saveAsTable("WR_RADAR.LE_Common_Details")

# COMMAND ----------


