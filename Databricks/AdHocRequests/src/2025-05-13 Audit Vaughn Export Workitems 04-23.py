# Databricks notebook source
import os

import pandas as pd
from pyspark.sql import functions as F
from datetime import datetime, timedelta

#Local File Import

# COMMAND ----------

## This file will contain generic functions that cover functionality for multiple notebooks / activities.
# Function to get latest file from that time

def fetch_latest_file(Object, Date, Storage, Dataversion=None, Legacy2=False, CaseService=False, RISKMODEL=False):
    # Function that returns latest version dynamically for Defined Data Objects
    print(Object)
    base_path = f'abfss://gcob@{Storage}.dfs.core.windows.net/'
    path_suffix = 'Legacy2/' if Legacy2 else 'CaseService/' if CaseService else 'RISKMODEL/' if RISKMODEL else ''

    # Fetch the latest DataVersion
    if Dataversion is None:
        all_versions = []
        versionFiles = dbutils.fs.ls(f'{base_path}/{path_suffix}/{Object}/')
        for file in versionFiles:
            all_versions.append(re.split("/", file.name)[0])
        # Condition check to bring only numbers in list
        all_versions = [int(item) for item in all_versions if item.isdigit()]
        current_version = str(max(all_versions))
    else:
        current_version = Dataversion

    #To find if the file exists for today and extract load_dts
    currentVersionPath = dbutils.fs.ls(f'{base_path}/{path_suffix}/{Object}/{current_version}/data/')

    getLoad_dts = [file for file in currentVersionPath if Date in file.name][0]

    
    load_dts = re.split("/", getLoad_dts.name)[0] + '*'
    final_path = f'{base_path}/{path_suffix}/{Object}/{current_version}/data/{load_dts}/*.parquet'

    return spark.read.parquet(final_path).createOrReplaceTempView(Object)

# COMMAND ----------

date_parameter = '20250423'
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
# = datetime.today().strftime('%Y%m%d')
#load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

import re

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details'
,'party_client'
,'party_workitem'
,'party_documents'
,'party_client_identifiers'
,'party_tax_info_fatca_crs'
,'party_local_client_Owners'
]

# Create TempView for each loading table
for Object in load_df:
    fetch_latest_file(Object, date_parameter, ReadStorage, CaseService=True)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Legacy2_GCOB_ApprovedVersion'
,'Legacy2_case_client_details'
,'Legacy2_workitem'
,'Legacy2_tax_info'
,'Legacy2_client_identifiers'
]

# Create TempView for each loading table
for Object in load_df:
    fetch_latest_file(Object, date_parameter, ReadStorage, Legacy2=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from Legacy2_case_client_details;--25926
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%'; --3006

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select * from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_ClientDetails AS
# MAGIC select c.* 
# MAGIC --,case when (c.ApprovedInGcob = 0 and GCOB_TRUE.CaseId is null and c.StatusTypeName = 'Complete') then 'True' else 'False' end as IsLatestApprovedVersionOfClient
# MAGIC ,case when IsClient = 'true' and ClientTypeId = 1 then concat('L2_LEC_',c.ClientId)
# MAGIC       when IsClient = 'true' and ClientTypeId in (2,3) then concat('L2_NP_NPPC_',c.ClientId)
# MAGIC       when IsClient = 'false' and ClientTypeId = 1 then concat('L2_RLEP_',c.ClientId)
# MAGIC       when IsClient = 'false' and ClientTypeId in (2,3) then concat('L2_RNPP_',c.ClientId)
# MAGIC end as SourceClient
# MAGIC from Legacy2_client c
# MAGIC Left outer join Legacy2_GCOB_ApprovedVersion GCOB_TRUE on c.GcobCaseId = GCOB_TRUE.CaseId
# MAGIC where c.IsClient = 'true'
# MAGIC and ClientTypeId in (1,2,3)
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_WorkItem_Details AS
# MAGIC select Distinct
# MAGIC c.SourceClient
# MAGIC ,null as CaseId
# MAGIC ,c.GcobId
# MAGIC ,w.DateCreated
# MAGIC ,w.DateAssigned
# MAGIC ,w.WorkItemAssignedUserName
# MAGIC ,w.ReviewType
# MAGIC ,w.CaseStatusName
# MAGIC ,w.DateCompleted
# MAGIC from Legacy2_workitem w
# MAGIC Inner join Legacy2_ClientDetails c on w.ClientId = c.ClientId
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW L2_Case_ClientDetails AS
# MAGIC select distinct
# MAGIC c.SourceSystem
# MAGIC ,c.CaseId
# MAGIC ,c.GcobId
# MAGIC ,c.FullLegalName
# MAGIC ,c.ReviewTypeName
# MAGIC ,c.CaseStatusName
# MAGIC ,c.GlobalClientOwner
# MAGIC ,c.GlobalClientOwnerLocation
# MAGIC ,c.ClientLifeCycleName
# MAGIC ,c.CddType
# MAGIC ,c.ClientType
# MAGIC ,c.IsLatestApprovedVersionOfClient
# MAGIC ,c.FIHubIndicator
# MAGIC ,c.SourceClient
# MAGIC ,c.IsEligibleForFatcaAssessment
# MAGIC ,c.FatcaClassification
# MAGIC ,crs.GIIN
# MAGIC ,crs.Ein
# MAGIC ,crs.DateOfIssue as FatcaDateOfIssue
# MAGIC ,c.IsEligibleForCrsAssessment
# MAGIC ,c.CrsClassification
# MAGIC ,crs.FormSignedDate as CrsFormSignedDate
# MAGIC ,c.IsClientListed
# MAGIC ,c.BusinessLineName
# MAGIC ,c.NextReviewDate
# MAGIC ,concat(COALESCE(c.RegisteredStreet,''), ' ', COALESCE(c.RegisteredNumber,''), ' ',COALESCE(c.RegisteredCity,''), ' ',COALESCE(c.RegisteredRegion,''), ' ',COALESCE(c.RegisteredPostalCode,''), ' ',COALESCE(c.CountryOfRegistration,'')) as RegisteredAddress
# MAGIC ,c.RegisteredStreet
# MAGIC ,c.RegisteredNumber
# MAGIC ,c.RegisteredCity
# MAGIC ,c.RegisteredRegion
# MAGIC ,c.RegisteredPostalCode
# MAGIC ,c.CountryOfRegistration
# MAGIC ,case when c.IsOperatingAddressDifferentToRegisteredAddress = 1 then concat(COALESCE(c.OperatingStreet,''), ' ', COALESCE(c.OperatingNumber,''), ' ',COALESCE(c.OperatingCity,''), ' ',COALESCE(c.OperatingRegion,''), ' ',COALESCE(c.OperatingPostalCode,''), ' ',COALESCE(c.CountryOfOperation,'')) else concat(COALESCE(c.RegisteredStreet,''), ' ', COALESCE(c.RegisteredNumber,''), ' ',COALESCE(c.RegisteredCity,''), ' ',COALESCE(c.RegisteredRegion,''), ' ',COALESCE(c.RegisteredPostalCode,''), ' ',COALESCE(c.CountryOfRegistration,''))  end as OperatingAddress
# MAGIC ,cid.SystemIdType
# MAGIC ,cid.value as SystemIdValue
# MAGIC ,crs.TinOrEquivalent
# MAGIC ,crs.CountryOfTaxResidence as CountryOfTaxResidencies
# MAGIC ,crs.CRSComments
# MAGIC ,c.CaseCompletedDate
# MAGIC ,crs.FatcaComments
# MAGIC ,c.CountryOfOperation
# MAGIC ,case when c.clienttype='Legal Entity' then concat('LE_', c.Gcobid)
# MAGIC     when c.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',c.Gcobid) else 'NA' end as  UniqueGcobId
# MAGIC from party_case_client_details c
# MAGIC Left join party_tax_info_fatca_crs crs on c.SourceClient = crs.SourceClient
# MAGIC Left Join party_client_identifiers cid on c.SourceClient = cid.SourceClient
# MAGIC
# MAGIC union
# MAGIC
# MAGIC Select distinct
# MAGIC c.SourceSystem
# MAGIC ,null as CaseId
# MAGIC ,c.GcobId
# MAGIC ,c.FullLegalName
# MAGIC ,c.ReviewTypeName
# MAGIC ,c.StatusTypeName
# MAGIC ,c.GlobalClientOwner
# MAGIC ,c.GlobalClientOwnerLocation
# MAGIC ,c.ClientLifeCycleName
# MAGIC ,c.CddType
# MAGIC ,c.ClientType
# MAGIC ,c.IsLatestApprovedVersionOfClient
# MAGIC ,null as FIHubIndicator
# MAGIC ,c.SourceClient
# MAGIC ,c.IsEligibleForFatcaAssessment
# MAGIC ,c.FatcaClassification
# MAGIC ,c.GIIN
# MAGIC ,c.Ein
# MAGIC ,null as FatcaDateOfIssue
# MAGIC ,c.IsEligibleForCrsAssessment
# MAGIC ,c.CrsClassification
# MAGIC ,null as CrsFormSignedDate
# MAGIC ,c.IsListed as IsClientListed
# MAGIC ,c.BusinessLineName
# MAGIC ,c.NextReviewDate
# MAGIC ,concat(COALESCE(c.RegisteredStreet,''), ' ', COALESCE(c.RegisteredAddressNumber,''), ' ',COALESCE(c.RegisteredCity,''), ' ',COALESCE(c.RegisteredRegion,''), ' ',COALESCE(c.RegisteredPostcode,''), ' ',COALESCE(c.RegisteredCountry,'')) as RegisteredAddress
# MAGIC ,c.RegisteredStreet
# MAGIC ,c.RegisteredAddressNumber as RegisteredNumber
# MAGIC ,c.RegisteredCity
# MAGIC ,c.RegisteredRegion
# MAGIC ,c.RegisteredPostcode as RegisteredPostalCode
# MAGIC ,c.RegisteredCountry as CountryOfRegistration
# MAGIC ,CASE WHEN c.PrincipalAddressDifferentFromRegistered = 1 THEN concat(COALESCE(c.OperationalStreet,''), ' ', COALESCE(c.OperationalNumber,''), ' ',COALESCE(c.OperationalCity,''), ' ',COALESCE(c.OperationalRegion,''), ' ',COALESCE(c.OperationalPostcode,''), ' ',COALESCE(c.OperationalCountry,'')) ELSE concat(COALESCE(c.RegisteredStreet,''), ' ', COALESCE(c.RegisteredAddressNumber,''), ' ',COALESCE(c.RegisteredCity,''), ' ',COALESCE(c.RegisteredRegion,''), ' ',COALESCE(c.RegisteredPostcode,''), ' ',COALESCE(c.RegisteredCountry,'')) END AS OperatingAddress
# MAGIC --,concat(COALESCE(c.OperationalStreet,''), ' ', COALESCE(c.OperationalNumber,''), ' ',COALESCE(c.OperationalCity,''), ' ',COALESCE(c.OperationalRegion,''), ' ',COALESCE(c.OperationalPostcode,''), ' ',COALESCE(c.OperationalCountry,'')) as OperatingAddress
# MAGIC ,cid.SystemIdType
# MAGIC ,cid.value as SystemIdValue
# MAGIC ,tax.TinOrEquivalent
# MAGIC ,tax.CountryOfTaxResidencies
# MAGIC ,null as CRSComments
# MAGIC ,c.CaseCompletedDate
# MAGIC ,null as FatcaComments
# MAGIC ,CASE WHEN c.PrincipalAddressDifferentFromRegistered = 1 THEN c.OperationalCountry ELSE c.RegisteredCountry END AS 
# MAGIC CountryOfOperation
# MAGIC ,case when c.clienttype='Legal Entity' then concat('LE_', c.Gcobid)
# MAGIC     when c.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',c.Gcobid) else 'NA' end as  UniqueGcobId
# MAGIC From Legacy2_ClientDetails c
# MAGIC Left join Legacy2_client_identifiers cid on c.ClientId = cid.ClientId
# MAGIC Left join Legacy2_tax_info as tax on c.ClientId = tax.ClientId
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW L2_Case_WorkItemDetails AS
# MAGIC select distinct
# MAGIC SourceClient
# MAGIC ,CaseId
# MAGIC ,GcobId
# MAGIC ,WorkItemCreatedDate
# MAGIC ,WorkItemAssignedDate
# MAGIC ,WorkItemAssignedUserName
# MAGIC ,CaseStatusName as CaseStatusWhenCreated
# MAGIC ,WorkItemCompletedDate
# MAGIC ,ReviewTypeName
# MAGIC From party_workitem
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC SourceClient
# MAGIC ,null as CaseId
# MAGIC ,GcobId
# MAGIC ,DateCreated as WorkItemCreatedDate
# MAGIC ,DateAssigned as WorkItemAssignedDate
# MAGIC ,WorkItemAssignedUserName
# MAGIC ,CaseStatusName as CaseStatusWhenCreated
# MAGIC ,DateCompleted as WorkItemCompletedDate
# MAGIC ,ReviewType as ReviewTypeName
# MAGIC From Legacy2_WorkItem_Details
# MAGIC

# COMMAND ----------

df_party_case_client_details=spark.table('L2_Case_ClientDetails')

# COMMAND ----------

import pyspark.sql.functions as F
df_party_case_client_details=df_party_case_client_details.withColumn("BusinessDate",F.lit(BusinessDate))

# COMMAND ----------

df_party_case_client_details.createOrReplaceTempView("L2_Case_ClientDetails")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW WorkItemDetails AS
# MAGIC select Distinct
# MAGIC c.BusinessDate
# MAGIC ,w.SourceClient
# MAGIC ,w.CaseId
# MAGIC ,w.GcobId
# MAGIC ,c.FullLegalName
# MAGIC ,c.CaseStatusName
# MAGIC ,w.ReviewTypeName
# MAGIC ,c.ClientLifeCycleName
# MAGIC ,w.WorkItemCreatedDate
# MAGIC ,w.WorkItemAssignedDate
# MAGIC ,w.WorkItemAssignedUserName
# MAGIC ,w.CaseStatusWhenCreated
# MAGIC ,w.WorkItemCompletedDate
# MAGIC ,c.IsLatestApprovedVersionOfClient
# MAGIC ,c.FIHubIndicator
# MAGIC ,c.SourceSystem
# MAGIC ,case when substr(w.SourceClient,0,3) = 'LEC' and c.SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(w.SourceClient,0,2) = 'NP' and c.SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(w.SourceClient,0,6) = 'L2_LEC' and c.SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(w.SourceClient,0,5) = 'L2_NP' and c.SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC ,c.UniqueGcobId
# MAGIC from L2_Case_WorkItemDetails w
# MAGIC Inner Join L2_Case_ClientDetails c on w.SourceClient = c.SourceClient

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS radar

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from WorkItemDetails
