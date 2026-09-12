# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To have a mapping for different sources to compare with GCDS system
# MAGIC
# MAGIC #### author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC - Prajit.Tatari@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Take GCDS data
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek	 |8-July-2025  |12757497    | NLSVF data commented and not capturing. 
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load
# MAGIC | Abhishek	 |10-Dec-2025  |14372938    | Extract RANZ data from GRAM 
# MAGIC | Mahalakshmi V | 11-Feb-2025 |15058773 | Included MDM RANZ Source
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read Files from GDP

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
import os
import re
import pandas as pd
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
AU_GDP_Defined_Storage_Account=os.environ['AU_GDP_Defined_Storage_Account']

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils file
from RadarUtils import *

# COMMAND ----------

dbutils.widgets.text("Load_Date", "")
dbutils.widgets.text("RunType", "daily")  # default is daily

Load_Date = dbutils.widgets.get("Load_Date")
RunType = dbutils.widgets.get("RunType").lower()
if Load_Date:
    RunType="historical"
    
if not Load_Date:
    Load_Date = datetime.today().strftime('%Y%m%d')

print(f"Running for Load Date: {Load_Date}")

# COMMAND ----------

# DBTITLE 1,Defining the dataobjects' name
party_dataobject='Party_SystemIdentifier'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(AU_GDP_Defined_Storage_Account)

# COMMAND ----------

# DBTITLE 1,Read GCDS data from GDP defined layer
# print list of strings for loading spark dfs from GDP
gcds_df = pd.DataFrame({'definedDatasetname':[
'client_Client',
'client_KeyStoreKey',
'client_PartyRole',
'client_PartytoPartyRelationship'
]})

# Create TempView for each loading table
for index, row in gcds_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=row.definedDatasetname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read GCOB data from GDP defined layer
# print list of strings for loading spark dfs from GDP
load_df =[
'party_case_client_details',
'party_AllPartyDetails'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading GRAM Dataobjects from GDP
# List of dataobjects from GDP
load_df = [
'Party_RiskModelInstanceQuestionAnswers'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read Legacy2 data from GDP defined layer
# List of datasets from GDP
load_df =[
'Legacy2_case_client_details'
]
# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read GIC data from GDP defined layer
import pandas as pd # print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'pessoa'
]})
#gic_load_dts
# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read Siebel data from GDP defined layer
siebel_df = pd.DataFrame({'definedDatasetname':[
'cdf_ggm_org_hist'
]})

for index, row in siebel_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='Siebel', Dataobject=row.definedDatasetname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read RANZ MDM data from GDP defined layer
Read_GDP_Defined_DataObjects_RANZ(Source='RANZ-MDM', Dataobject='c_b_party_xref',Load_Date='')

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - Other Sources Data Prepration

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_CaseClientDetails As
# MAGIC select * 
# MAGIC ,case when ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,Case
# MAGIC       when ClientType = 'Legal Entity' then concat('LEC_', GcobId)
# MAGIC       when ClientType in (
# MAGIC         'Natural Person',
# MAGIC         'Natural Person acting in a Professional Capacity (NPPC)'
# MAGIC       ) then concat('NP_NPPC_', GcobId)
# MAGIC     End as UniquePartyId
# MAGIC from party_case_client_details
# MAGIC where CaseStatusName <> 'Cancelled'
# MAGIC -- and IsLatestApprovedVersionofclient = 'True'

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllPartyDetails_Row As
# MAGIC select p.UniquePartyId
# MAGIC ,p.gcdsid
# MAGIC ,p.gcobid
# MAGIC ,case when p.ClientType = 'LegalEntityClient' then 'Legal Entity' 
# MAGIC       when p.ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when p.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity' 
# MAGIC       when p.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY p.gcobid,p.ClientType ORDER BY CASE WHEN p.ClientLifeCycleStatus='Client' THEN 1 ELSE 2 END ASC, c.CaseCompletedDate DESC) AS ROWNUM
# MAGIC from party_AllPartyDetails p
# MAGIC Inner join Gcob_CaseClientDetails c on p.UniquePartyId = c.UniqueGcobId
# MAGIC --on CASE WHEN p.ClientType='LegalEntityClient' THEN concat('LEC_',p.Id) WHEN p.ClientType IN ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_NPPC_',p.Id) end = c.SourceClient
# MAGIC where Status = 'Live'
# MAGIC --and p.CaseStatusName <> 'Cancelled'
# MAGIC -- and p.IsLatestApprovedVersionofclient = 'True'

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllParty As
# MAGIC With details as 
# MAGIC (
# MAGIC select p.* 
# MAGIC ,case when p.ClientType = 'LegalEntityClient' then 'Legal Entity' 
# MAGIC       when p.ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when p.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity' 
# MAGIC       when p.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,c.SalesforceClientID_nCino
# MAGIC ,c.gcobid as ncino_gcobid
# MAGIC ,case when p.CaseStatusName is null then 'RelatedParty' else c.CaseStatusName end as StatusName
# MAGIC from party_AllPartyDetails p
# MAGIC LEFT JOIN Gcob_CaseClientDetails c on p.UniquePartyId = c.UniqueGcobId
# MAGIC --on CASE WHEN p.ClientType='LegalEntityClient' THEN concat('LEC_',p.Id) WHEN p.ClientType IN ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_NPPC_',p.Id) end = c.SourceClient and p.ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)')
# MAGIC where Status = 'Live'
# MAGIC -- and (p.IsLatestApprovedVersionofclient = 'True' or p.IsLatestApprovedVersionofclient is null)
# MAGIC )
# MAGIC select distinct
# MAGIC *
# MAGIC from details
# MAGIC where StatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,Read GCDS "GCOB"-"GCOB-NCINO"-"GIC"-"SIEBEL" Client data
# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_Clients As
# MAGIC select distinct
# MAGIC k.KeyStore_value as identifier,k.KeyStore_type,k.status AS status,c.*, pr.Party_role, pr.Life_cycle_status
# MAGIC ,rel.`Relationship-Type` as RelationshipType,rel.`Relationship-Value` as RelationshipValue_GCID
# MAGIC from client_KeyStoreKey k
# MAGIC inner join client_Client c on c.GCID = k.GCID
# MAGIC left join client_PartyRole pr on k.GCID = pr.GCID
# MAGIC left join client_PartytoPartyRelationship rel on k.GCID = rel.GCID

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllPartyDetails As
# MAGIC select * from Gcob_AllPartyDetails_Row

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%';
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select * from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2 AS
# MAGIC select distinct 
# MAGIC clientId,SalesforceClientID_nCino
# MAGIC ,CASE
# MAGIC   WHEN IsClient = 'true' and  ClientTypeId = 1 THEN concat('LE_', GcobId)
# MAGIC   WHEN IsClient = 'true' and ClientTypeId in (2,3) THEN concat('NP_NPPC_', GcobId)
# MAGIC   WHEN IsClient = 'false' and ClientTypeId = 1 THEN concat('RLE_', GcobId)
# MAGIC   WHEN IsClient = 'false' and ClientTypeId in (2,3) THEN concat('RNP_', GcobId)
# MAGIC END as Legacy2_Identifier
# MAGIC ,case when ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when ClientType = 'Related Legal Entity' then 'Related Legal Entity' 
# MAGIC       when ClientType = 'Related Natural Person' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC from Legacy2_client 
# MAGIC where ClientTypeId in (1,2,3)
# MAGIC -- and IsLatestApprovedVersionofclient = 'True'

# COMMAND ----------

# DBTITLE 1,SIEBEL WRR Clients update to join
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW SIEBEL_ClientDetails AS
# MAGIC select distinct
# MAGIC s.rel_id as siebelId
# MAGIC FROM cdf_ggm_org_hist AS s
# MAGIC WHERE       
# MAGIC   s.Edl_valid_to_dts like '9999-12-31%'
# MAGIC   AND s.bnk_code IN ('3000', '3400', '3508')
# MAGIC   AND del_f = 'N'
# MAGIC   AND rel_st_tp_ggm_code = 'C'

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view GIC_Clients As
# MAGIC select distinct
# MAGIC SEQ_PESSOA 
# MAGIC ,COD_INSTITUCIONAL
# MAGIC ,CASE WHEN SEQ_TIPO_PESSOA = '2' THEN 'Natural Person' else 'Legal Entity' END AS Party_type
# MAGIC from pessoa

# COMMAND ----------

# DBTITLE 1,GRAM-RANZ data preparation
# %sql
# CREATE OR REPLACE TEMPORARY VIEW GRAM_RANZ AS
# select distinct 
# split_part(SourceSystemReferenceId, '_', 1) as CaseID
# ,split_part(SourceSystemReferenceId, '_', 2) as ClientId
# ,SourceSystemReferenceId
# from Party_RiskModelInstanceQuestionAnswers 
# where 
# SourceSystemName = 'RANZ - PegaCdd'
# --and len(SourceSystemReferenceId) >= 15
# and split_part(SourceSystemReferenceId, '_', 2) <> ''

# COMMAND ----------

# MAGIC %md
# MAGIC ## GCDS Data

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_Party AS --GCDS_Local_Party As
# MAGIC select distinct
# MAGIC CONCAT('GCDS_',gcid) as PartyIdentifier
# MAGIC ,'GCOB' as Application 
# MAGIC ,gcob.UniquePartyId as LocalSystemIdentifier
# MAGIC from GCDS_Clients gcds
# MAGIC INNER JOIN Gcob_AllPartyDetails gcob on gcds.identifier = gcob.GcobId
# MAGIC         --and gcds.GCID = gcob.GCDSID
# MAGIC         and gcob.Party_type = 'Legal Entity'
# MAGIC         --and gcds.party_role = 'Customer'
# MAGIC         and gcds.KeyStore_type = 'GCOBID'
# MAGIC where
# MAGIC --gcds.party_role = 'Customer' and 
# MAGIC gcds.Party_type <> 'Natural Person'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select distinct
# MAGIC CONCAT('GCDS_',gcid) as PartyIdentifier
# MAGIC ,'GCOB' as Application
# MAGIC ,c.UniquePartyId as LocalSystemIdentifier
# MAGIC from GCDS_Clients gcds
# MAGIC INNER JOIN Gcob_CaseClientDetails c on c.SalesforceClientID_nCino = gcds.identifier 
# MAGIC         and gcds.KeyStore_type = 'NCINOID' 
# MAGIC where
# MAGIC --gcds.party_role = 'Customer' and
# MAGIC c.Party_type = 'Natural Person'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select distinct
# MAGIC CONCAT('GCDS_',gcid) as PartyIdentifier 
# MAGIC ,'LEGACY2' as Application
# MAGIC ,l2.Legacy2_Identifier as LocalSystemIdentifier
# MAGIC from GCDS_Clients gcds
# MAGIC INNER JOIN Legacy2 l2 on l2.SalesforceClientID_nCino = gcds.identifier 
# MAGIC         and l2.Party_type = gcds.Party_type
# MAGIC         and gcds.KeyStore_type = 'NCINOID'
# MAGIC --where
# MAGIC --gcds.party_role = 'Customer'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select distinct
# MAGIC CONCAT('GCDS_',gcid) as PartyIdentifier
# MAGIC ,'GIC' as Application
# MAGIC ,gic.COD_INSTITUCIONAL as LocalSystemIdentifier
# MAGIC from GCDS_Clients gcds
# MAGIC INNER JOIN GIC_Clients gic on gcds.identifier = gic.COD_INSTITUCIONAL
# MAGIC         and gcds.KeyStore_type = 'GIC'
# MAGIC --where
# MAGIC --gcds.party_role = 'Customer'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select distinct
# MAGIC CONCAT('GCDS_',gcid) as PartyIdentifier
# MAGIC ,'SIEBEL' as Application
# MAGIC ,s.SiebelId as LocalSystemIdentifier 
# MAGIC from GCDS_Clients gcds
# MAGIC INNER JOIN SIEBEL_ClientDetails s on gcds.identifier = s.SiebelId
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select distinct
# MAGIC CONCAT('GCDS_',a.gcid) as PartyIdentifier
# MAGIC ,'GCDS' as Application
# MAGIC ,a.gcid as LocalSystemIdentifier 
# MAGIC from GCDS_Clients a
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC CONCAT('GCDS_',gcid) as PartyIdentifier
# MAGIC ,'RANZ' as Application
# MAGIC ,party_ranz.SRC_PARTY_ID as LocalSystemIdentifier 
# MAGIC from GCDS_Clients gcds
# MAGIC INNER JOIN c_b_party_xref party_ranz on gcds.identifier = party_ranz.SRC_PARTY_ID and gcds.KeyStore_type = 'CB RANZ'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Non GCDS Data

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view GCOB_NonGCDS As
# MAGIC with GCOB as(
# MAGIC select distinct
# MAGIC CONCAT('GCOB_',t1.UniquePartyId) as PartyIdentifier
# MAGIC ,'GCOB' as Application
# MAGIC ,t1.UniquePartyId as LocalSystemIdentifier
# MAGIC ,t1.Party_Type
# MAGIC ,t1.caseid
# MAGIC ,t1.InternalWatchlist
# MAGIC from 
# MAGIC Gcob_AllParty t1
# MAGIC )
# MAGIC
# MAGIC select distinct
# MAGIC PartyIdentifier
# MAGIC ,Application
# MAGIC ,LocalSystemIdentifier
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY PartyIdentifier,Party_Type ORDER BY caseid desc,InternalWatchlist desc) AS ROWNUM
# MAGIC from GCOB 

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view NonGCDS_Party As
# MAGIC with NonGCDS as 
# MAGIC (
# MAGIC select * from GCOB_NonGCDS
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC CONCAT('GIC_',t1.COD_INSTITUCIONAL) as PartyIdentifier
# MAGIC ,'GIC' as Application
# MAGIC ,t1.COD_INSTITUCIONAL as LocalSystemIdentifier
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY t1.COD_INSTITUCIONAL,t1.Party_Type ORDER BY t1.COD_INSTITUCIONAL DESC) AS ROWNUM
# MAGIC From GIC_Clients t1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC CONCAT('LEGACY2_',t1.Legacy2_Identifier) as PartyIdentifier
# MAGIC ,'LEGACY2' as Application
# MAGIC ,t1.Legacy2_Identifier AS LocalSystemIdentifier
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY t1.Legacy2_Identifier ORDER BY t1.clientid DESC) AS ROWNUM
# MAGIC from Legacy2 as t1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC CONCAT('SIEBEL_',t1.SiebelId) as PartyIdentifier
# MAGIC ,'SIEBEL' as Application
# MAGIC ,t1.SiebelId AS LocalSystemIdentifier
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY t1.SiebelId ORDER BY t1.SiebelId DESC) AS ROWNUM
# MAGIC from SIEBEL_ClientDetails as t1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC CONCAT('RANZ_',SRC_PARTY_ID) as PartyIdentifier
# MAGIC ,'RANZ' as Application
# MAGIC ,SRC_PARTY_ID as LocalSystemIdentifier
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY SRC_PARTY_ID ORDER BY SRC_PARTY_ID DESC) AS ROWNUM 
# MAGIC from c_b_party_xref 
# MAGIC
# MAGIC )
# MAGIC select  distinct
# MAGIC t1.PartyIdentifier
# MAGIC ,t1.Application
# MAGIC ,t1.LocalSystemIdentifier
# MAGIC from NonGCDS as t1 where t1.ROWNUM = 1
# MAGIC /*
# MAGIC union
# MAGIC
# MAGIC select distinct 
# MAGIC CONCAT('NLSVF_',CIFNUMBER) as PartyIdentifier
# MAGIC ,'NLSVF' as Application
# MAGIC ,CIFNUMBER AS LocalSystemIdentifier
# MAGIC From nls_dbo_cif
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view NonGCDS_UniqueParty As
# MAGIC select * 
# MAGIC from NonGCDS_Party a
# MAGIC left anti join GCDS_Party b
# MAGIC on concat(a.Application,'_',a.LocalSystemIdentifier) = concat(b.Application,'_',b.LocalSystemIdentifier)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final Data

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view SystemIdentifier_final As
# MAGIC select * from GCDS_Party
# MAGIC union
# MAGIC select * from NonGCDS_UniqueParty

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_SystemIdentifier = spark.table('SystemIdentifier_final')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
    save_to_saradar_storage_account(df_SystemIdentifier, party_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_SystemIdentifier, party_dataobject, radar_datamodel_version_number, environment)
