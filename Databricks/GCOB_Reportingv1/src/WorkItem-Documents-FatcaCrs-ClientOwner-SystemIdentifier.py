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

WorkItemDetails_dataobject = 'WorkItemDetails'
Documents_dataobject = 'DocumentDetails'
FatcaCrs_dataobject = 'FatcaCrsDetails'
SystemIdentifier_dataobject = 'SystemIdentifier'
ClientOwnership_dataobject = 'ClientOwnership'

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

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
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

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
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

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

df_party_case_client_details=df_party_case_client_details.withColumn("BusinessDate",lit(BusinessDate))

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

df_WorkItemDetails = spark.table('WorkItemDetails')
save_to_saradar_storage_account(df_WorkItemDetails, WorkItemDetails_dataobject)

# COMMAND ----------

# %sql
# CREATE SCHEMA IF NOT EXISTS radar

# COMMAND ----------

# %sql
# drop table IF EXISTS radar.WorkItemDetails

# COMMAND ----------

# spark.sql('select * from WorkItemDetails').write.mode('overwrite').saveAsTable('radar.WorkItemDetails')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Documents AS
# MAGIC select distinct
# MAGIC c.BusinessDate
# MAGIC ,c.CaseId
# MAGIC ,c.GcobId
# MAGIC ,c.FullLegalName
# MAGIC ,case when d.EntityType in (0,3) then c.FullLegalName else null end as DocumentAssociatedTo
# MAGIC ,case when d.EntityType = 0 then 'Legal Entity Client' 
# MAGIC       when d.EntityType = 1 then 'Related Legal Entity Party'
# MAGIC       when d.EntityType = 2 then 'Related Natural Person Party'
# MAGIC       when d.EntityType = 3 then 'Natural Person Client'
# MAGIC       else null end as DocumentAssociatedType
# MAGIC ,d.DocumentExpiryDate
# MAGIC ,d.DocumentFileName
# MAGIC ,case when d.DocumentExpiryDate is null then 'False' 
# MAGIC       when cast(d.DocumentExpiryDate as date)  < cast(c.BusinessDate as date) then 'False' 
# MAGIC end as IsExpired
# MAGIC ,c.GlobalClientOwner
# MAGIC ,c.BusinessLineName
# MAGIC ,d.DocumentSubTypeName as DocumentTypeName
# MAGIC ,c.NextReviewDate
# MAGIC ,c.IsLatestApprovedVersionOfClient
# MAGIC ,c.CaseStatusName
# MAGIC ,c.ReviewTypeName
# MAGIC ,c.ClientLifeCycleName
# MAGIC ,c.SourceSystem
# MAGIC ,c.SourceClient
# MAGIC ,case when substr(c.SourceClient,0,3) = 'LEC' and c.SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(c.SourceClient,0,2) = 'NP' and c.SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(c.SourceClient,0,6) = 'L2_LEC' and c.SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(c.SourceClient,0,5) = 'L2_NP' and c.SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC ,c.UniqueGcobId
# MAGIC ,d.UploadedBy
# MAGIC ,d.UploadDate
# MAGIC ,d.ExpiryDate
# MAGIC ,d.Purpose
# MAGIC from party_documents d 
# MAGIC Inner Join L2_Case_ClientDetails c on d.Source_Entity = c.SourceClient
# MAGIC

# COMMAND ----------

df_Documents = spark.table('Documents')
save_to_saradar_storage_account(df_Documents, Documents_dataobject)

# COMMAND ----------

# %sql
# drop table IF EXISTS radar.DocumentDetails

# COMMAND ----------

# spark.sql('select * from Documents').write.mode('overwrite').saveAsTable('radar.DocumentDetails')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW FatcaCrs AS
# MAGIC select distinct 
# MAGIC BusinessDate
# MAGIC ,GcobId
# MAGIC ,CaseId
# MAGIC ,FullLegalName
# MAGIC ,RegisteredAddress
# MAGIC ,OperatingAddress
# MAGIC ,IsEligibleForFatcaAssessment
# MAGIC ,FatcaClassification
# MAGIC ,Giin
# MAGIC ,Ein
# MAGIC ,FatcaDateOfIssue
# MAGIC ,IsEligibleForCrsAssessment
# MAGIC ,CrsClassification
# MAGIC ,CrsFormSignedDate
# MAGIC ,ClientLifeCycleName
# MAGIC ,IsClientListed
# MAGIC ,ReviewTypeName
# MAGIC ,CaseStatusName
# MAGIC ,RegisteredStreet
# MAGIC ,RegisteredNumber
# MAGIC ,RegisteredCity
# MAGIC ,RegisteredRegion
# MAGIC ,RegisteredPostalCode
# MAGIC ,CountryOfRegistration
# MAGIC ,SystemIdType
# MAGIC ,SystemIdValue
# MAGIC ,TinOrEquivalent
# MAGIC ,CountryOfTaxResidencies
# MAGIC ,CRSComments
# MAGIC ,FatcaComments
# MAGIC ,CaseCompletedDate
# MAGIC ,IsLatestApprovedVersionOfClient
# MAGIC ,SourceSystem
# MAGIC ,SourceClient
# MAGIC ,case when substr(SourceClient,0,3) = 'LEC' and SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(SourceClient,0,2) = 'NP' and SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(SourceClient,0,6) = 'L2_LEC' and SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(SourceClient,0,5) = 'L2_NP' and SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC ,UniqueGcobId
# MAGIC ,IGA.IGA_Flag
# MAGIC ,IGA.In_force_as_of
# MAGIC ,IGA.In_force_until
# MAGIC from L2_Case_ClientDetails
# MAGIC LEFT JOIN RADAR.igacountries as IGA
# MAGIC ON CountryOfTaxResidencies = IGA.CountryCode2

# COMMAND ----------

df_FatcaCrs = spark.table('FatcaCrs')
save_to_saradar_storage_account(df_FatcaCrs, FatcaCrs_dataobject)

# COMMAND ----------

# %sql
# drop table IF EXISTS radar.FatcaCrsDetails

# COMMAND ----------

# spark.sql('select * from FatcaCrs').write.mode('overwrite').saveAsTable('radar.FatcaCrsDetails')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SystemIdentifier AS
# MAGIC select distinct 
# MAGIC BusinessDate
# MAGIC ,GcobId
# MAGIC ,CaseId
# MAGIC ,FullLegalName
# MAGIC ,ClientLifeCycleName
# MAGIC ,ReviewTypeName
# MAGIC ,CaseStatusName
# MAGIC ,SystemIdType
# MAGIC ,SystemIdValue
# MAGIC ,IsLatestApprovedVersionOfClient
# MAGIC ,GlobalClientOwnerLocation
# MAGIC ,SourceSystem
# MAGIC ,SourceClient
# MAGIC ,case when substr(SourceClient,0,3) = 'LEC' and SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(SourceClient,0,2) = 'NP' and SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(SourceClient,0,6) = 'L2_LEC' and SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(SourceClient,0,5) = 'L2_NP' and SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC ,UniqueGcobId
# MAGIC from L2_Case_ClientDetails

# COMMAND ----------

df_SystemIdentifier = spark.table('SystemIdentifier')
save_to_saradar_storage_account(df_SystemIdentifier, SystemIdentifier_dataobject)

# COMMAND ----------

# %sql
# drop table IF EXISTS radar.SystemIdentifier

# COMMAND ----------

# spark.sql('select * from SystemIdentifier').write.mode('overwrite').saveAsTable('radar.SystemIdentifier')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ClientOwnership AS
# MAGIC select distinct 
# MAGIC BusinessDate
# MAGIC ,GcobId
# MAGIC ,CaseId
# MAGIC ,FullLegalName
# MAGIC ,ClientLifeCycleName
# MAGIC ,ReviewTypeName
# MAGIC ,CaseStatusName
# MAGIC ,CountryOfRegistration
# MAGIC ,CountryOfOperation
# MAGIC ,GlobalClientOwner as OwnerName
# MAGIC ,GlobalClientOwnerLocation as OwnerLocation
# MAGIC ,'Global' as OwnerType
# MAGIC ,IsLatestApprovedVersionOfClient
# MAGIC ,SourceSystem
# MAGIC ,SourceClient
# MAGIC ,case when substr(SourceClient,0,3) = 'LEC' and SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(SourceClient,0,2) = 'NP' and SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(SourceClient,0,6) = 'L2_LEC' and SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(SourceClient,0,5) = 'L2_NP' and SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC ,UniqueGcobId
# MAGIC from L2_Case_ClientDetails
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct 
# MAGIC c.BusinessDate
# MAGIC ,l.GcobId
# MAGIC ,l.CaseId
# MAGIC ,l.FullLegalName
# MAGIC ,l.ClientLifeCycleName
# MAGIC ,l.ReviewTypeName
# MAGIC ,l.CaseStatusName
# MAGIC ,l.CountryOfRegistration
# MAGIC ,l.CountryOfOperation
# MAGIC ,l.LocalClientOwnerName as OwnerName
# MAGIC ,l.Location as OwnerLocation
# MAGIC ,l.OwnerType
# MAGIC ,c.IsLatestApprovedVersionOfClient
# MAGIC ,c.SourceSystem
# MAGIC ,c.SourceClient
# MAGIC ,case when substr(c.SourceClient,0,3) = 'LEC' and c.SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(c.SourceClient,0,2) = 'NP' and c.SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(c.SourceClient,0,6) = 'L2_LEC' and c.SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(c.SourceClient,0,5) = 'L2_NP' and c.SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC ,c.UniqueGcobId
# MAGIC from party_local_client_Owners l
# MAGIC left join L2_Case_ClientDetails c on concat('LEC_',l.LegalEntityClientId) = c.SourceClient

# COMMAND ----------

df_ClientOwnership = spark.table('ClientOwnership')
save_to_saradar_storage_account(df_ClientOwnership, ClientOwnership_dataobject)

# COMMAND ----------

# %sql
# drop table IF EXISTS radar.ClientOwnership

# COMMAND ----------

# spark.sql('select * from ClientOwnership').write.mode('overwrite').saveAsTable('radar.ClientOwnership')
