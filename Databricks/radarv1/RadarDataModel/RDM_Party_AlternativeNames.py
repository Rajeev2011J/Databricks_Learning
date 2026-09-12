# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To create a dataobject to capture the party alternative names
# MAGIC
# MAGIC #### Authors
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### Flow of Logic
# MAGIC - Reading GCOB,GCDS,GIC dataobjects from GDP
# MAGIC - Fetching the alternative names from different SourceSystems
# MAGIC - Loading the final dataobject to SA RADAR Storage account
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |17-Sept-205 |12646357 |First Release
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####READING FILES FROM GDP

# COMMAND ----------

# DBTITLE 1,Importing the required packages
import os
import pandas as pd
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Importing modules from RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
app_reg_app_id = os.environ["APP_REG_APP_ID"]
ReadStorage = os.environ["GDP_STORAGE_NAME"]
TenantId = os.environ["TENANT_ID"]
GIC_ReadStorage = os.environ["GDP_SA_STORAGE_NAME"]
RANZ_ReadStorage = os.environ['AU_GDP_Defined_Storage_Account']
environment = os.environ["ENV"]
radar_datamodel_version_number = 1

# COMMAND ----------

# DBTITLE 1,Defining Load_Date and RunType
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

# DBTITLE 1,Defining the dataobject name
party_AlternativeName_dataobject = "Party_AlternativeNames"

# COMMAND ----------

# DBTITLE 1,Defining the SARADAR Storage account
SARADAR = "saradar" + environment

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(SARADAR)
authenticate_storage_account(RANZ_ReadStorage)
#authenticate_storage_account(GIC_ReadStorage)

# COMMAND ----------

# DBTITLE 1,Deriving required date parameters
# Derive the date for which data has to be processed from GDP
load_dts = f"EDL_LOAD_DTS={Load_Date}*"
print (load_dts)

# COMMAND ----------

# DBTITLE 1,Reading GCDS DataObjects from GDP
# List of datasets from GDP
gcds_df = pd.DataFrame(
    {
        "definedDatasetname": [
            "client_Client",
            "client_KeyStoreKey"
        ]
    }
)

# Create TempView for each loading table
for index, row in gcds_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=row.definedDatasetname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading GCOB CaseService Dataobjects from GDP
# List of datasets from GDP
load_df = [
            "party_case_client_details",
            "party_AllPartyDetails",
            "party_Alias",
            "party_trade_name",
            "party_client_structure_GUI"
        ]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier dataobject
df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/1/data/{load_dts}/*.parquet"
)

# COMMAND ----------

# DBTITLE 1,Read Legacy2 data from GDP defined layer
# List of datasets from GDP
load_df = [
'Legacy2_case_client_details'
,'Legacy2_trade_name'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read RANZ data from GDP defined layer
load_df = [
    'c_b_party_xref'
,'c_b_contract_xref'
,'c_b_contr_rol_party_xref'
]

for Dataobject in load_df:
    Read_GDP_Defined_DataObjects_RANZ('RANZ-MDM' , Dataobject, Load_Date)

# COMMAND ----------

# MAGIC %md
# MAGIC #####TRANSFORMATION TO GET Party_Coverage OBJECT 

# COMMAND ----------

# DBTITLE 1,Updating GCOB Case Client Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_CaseClientDetails As
# MAGIC select distinct *
# MAGIC ,case when ClientType = 'Legal Entity' then 'Legal Entity'
# MAGIC   when ClientType in ('Natural Person', 'Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person'
# MAGIC   else null
# MAGIC end as Party_type
# MAGIC ,Case when ClientType = 'Legal Entity' then concat('GCOB_LEC_', GcobId)
# MAGIC   when ClientType in ('Natural Person', 'Natural Person acting in a Professional Capacity (NPPC)') then concat('GCOB_NP_NPPC_', GcobId)
# MAGIC End as LocalSystemIdentifier
# MAGIC from party_case_client_details
# MAGIC where CaseStatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,Fetching Details for GCOB Parties
# MAGIC %sql
# MAGIC CREATE OR REPLaCE TEMP VIEW Gcob_AllPartyDetails as
# MAGIC with PartyDetails as 
# MAGIC (
# MAGIC Select distinct p.*
# MAGIC ,Case when p.ClientType = 'LegalEntityClient' then concat('LEC_',Id)
# MAGIC       when p.ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then concat('NP_NPPC_',Id)
# MAGIC End as SourceClient 
# MAGIC ,case when p.CaseStatusName is null then 'RelatedParty' else c.CaseStatusName end as StatusName
# MAGIC ,case when p.ClientType = 'LegalEntityClient' then 'Legal Entity' 
# MAGIC    when p.ClientType in ('NaturalPersonClient', 'Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person'
# MAGIC    when p.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity'
# MAGIC    when p.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person'
# MAGIC    else null
# MAGIC end as Party_type
# MAGIC from party_AllPartyDetails p
# MAGIC LEFT JOIN party_case_client_details c on p.UniquePartyId = c.UniqueGcobId
# MAGIC where Status = 'Snapshot' 
# MAGIC )
# MAGIC ,AllPartyDetails as
# MAGIC (
# MAGIC select * from PartyDetails where StatusName not in ('Cancelled','RelatedParty')
# MAGIC )
# MAGIC ,Client_MaxSnap as
# MAGIC (
# MAGIC select papd.UniquePartyId,max(CS.ClientStructureSnapshotId) as Max_SnapshotId
# MAGIC FROM PartyDetails papd 
# MAGIC LEFT JOIN party_client_structure_GUI cs ON papd.Sourceclient=cs.Sourceclient
# MAGIC where papd.StatusName <> 'Cancelled'
# MAGIC group by papd.UniquePartyId
# MAGIC )
# MAGIC ,RP_MaxSnap as
# MAGIC (
# MAGIC select papd.UniquePartyId,max(CS.ClientStructureSnapshotId) as Max_SnapshotId
# MAGIC FROM PartyDetails papd 
# MAGIC LEFT JOIN party_client_structure_GUI cs ON papd.PartyId = cs.ParentEntityId and papd.UniquePartyId = cs.UniqueParentPartyId
# MAGIC where papd.StatusName <> 'Cancelled'
# MAGIC group by papd.UniquePartyId
# MAGIC )
# MAGIC ,Client as 
# MAGIC (
# MAGIC select distinct
# MAGIC papd.GcobId
# MAGIC ,papd.UniquePartyId
# MAGIC ,CS.ClientStructureSnapshotId
# MAGIC ,NULLIF(alias.Alias, '  ') as Alias
# MAGIC ,tr.ClientTradeName as TradeName
# MAGIC ,NULLIF(papd.FullLegalNameInLocalLanguage, '') as FullLegalNameInLocalLanguage
# MAGIC ,papd.Party_type
# MAGIC ,papd.PartyId
# MAGIC ,papd.FirstName , papd.MiddleName , papd.LastName
# MAGIC FROM AllPartyDetails papd 
# MAGIC LEFT JOIN party_client_structure_GUI cs ON papd.Sourceclient=cs.Sourceclient
# MAGIC LEFT JOIN party_Alias alias ON papd.Id = alias.Id AND papd.ClientType = alias.ResultType --papd.PartyId = alias.PartyId AND
# MAGIC LEFT JOIN party_trade_name tr ON papd.Id = tr.Id AND papd.ClientType = tr.ResultType --papd.PartyId = tr.PartyId AND
# MAGIC )
# MAGIC ,Client_Details as
# MAGIC (
# MAGIC select distinct c.GcobId,c.UniquePartyId,c.Alias,c.TradeName,c.FullLegalNameInLocalLanguage,c.Party_type,c.PartyId
# MAGIC ,c.FirstName , c.MiddleName , c.LastName
# MAGIC from Client c
# MAGIC inner join Client_MaxSnap m on c.UniquePartyId = m.UniquePartyId and COALESCE(c.ClientStructureSnapshotId,00) = COALESCE(m.Max_SnapshotId,00)
# MAGIC where Alias is not null or TradeName is not null or FullLegalNameInLocalLanguage is not null or c.FirstName is not null or c.MiddleName is not null or c.LastName is not null
# MAGIC )
# MAGIC ,RelatedPrty as 
# MAGIC (
# MAGIC select distinct
# MAGIC papd.GcobId
# MAGIC ,papd.UniquePartyId
# MAGIC ,CS.ClientStructureSnapshotId
# MAGIC ,NULLIF(alias.Alias, '  ') as Alias
# MAGIC ,tr.ClientTradeName as TradeName
# MAGIC ,NULLIF(papd.FullLegalNameInLocalLanguage, '') as FullLegalNameInLocalLanguage
# MAGIC ,case when papd.ClientType = 'LegalEntityClient' then 'Legal Entity' 
# MAGIC    when papd.ClientType in ('NaturalPersonClient', 'Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person'
# MAGIC    when papd.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity'
# MAGIC    when papd.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person'
# MAGIC    else null
# MAGIC end as Party_type
# MAGIC ,papd.PartyId
# MAGIC ,papd.FirstName , papd.MiddleName , papd.LastName
# MAGIC FROM party_client_structure_GUI cs
# MAGIC LEFT JOIN party_AllPartyDetails papd ON cs.ParentEntityId = papd.PartyId and cs.UniqueParentPartyId=papd.UniquePartyId
# MAGIC LEFT JOIN party_Alias alias ON papd.Id = alias.Id AND papd.ClientType = alias.ResultType --papd.PartyId = alias.PartyId AND
# MAGIC LEFT JOIN party_trade_name tr ON papd.Id = tr.Id AND papd.ClientType = tr.ResultType --papd.PartyId = tr.PartyId AND
# MAGIC where papd.Status='Snapshot' 
# MAGIC )
# MAGIC ,RelatedPrty_Details as
# MAGIC (
# MAGIC select distinct c.GcobId,c.UniquePartyId,c.Alias,c.TradeName,c.FullLegalNameInLocalLanguage,c.Party_type,c.PartyId
# MAGIC ,c.FirstName , c.MiddleName , c.LastName
# MAGIC from RelatedPrty c
# MAGIC inner join RP_MaxSnap m on c.UniquePartyId = m.UniquePartyId and COALESCE(c.ClientStructureSnapshotId,00) = COALESCE(m.Max_SnapshotId,00)
# MAGIC where Alias is not null or TradeName is not null or FullLegalNameInLocalLanguage is not null or c.FirstName is not null or c.MiddleName is not null or c.LastName is not null
# MAGIC )
# MAGIC ,Client_RelatedPrty as 
# MAGIC (
# MAGIC select * from Client_Details
# MAGIC union
# MAGIC select * from RelatedPrty_Details
# MAGIC )
# MAGIC select distinct 
# MAGIC CONCAT('GCOB_', UniquePartyId) AS LocalSystemIdentifier
# MAGIC ,GcobId,UniquePartyId,Alias,TradeName,FullLegalNameInLocalLanguage,Party_type,PartyId
# MAGIC ,FirstName , MiddleName , LastName
# MAGIC from Client_RelatedPrty
# MAGIC

# COMMAND ----------

# DBTITLE 1,Legacy2 All Party Details
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
# MAGIC with L2_Details as
# MAGIC (
# MAGIC select distinct 
# MAGIC c.clientId,SalesforceClientID_nCino
# MAGIC ,CASE
# MAGIC   WHEN c.IsClient = 'true' and  c.ClientTypeId = 1 THEN concat('LE_', GcobId)
# MAGIC   WHEN c.IsClient = 'true' and c.ClientTypeId in (2,3) THEN concat('NP_NPPC_', GcobId)
# MAGIC   WHEN c.IsClient = 'false' and c.ClientTypeId = 1 THEN concat('RLE_', GcobId)
# MAGIC   WHEN c.IsClient = 'false' and c.ClientTypeId in (2,3) THEN concat('RNP_', GcobId)
# MAGIC END as Legacy2_Identifier
# MAGIC ,case when c.ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when c.ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when c.ClientType = 'Related Legal Entity' then 'Related Legal Entity' 
# MAGIC       when c.ClientType = 'Related Natural Person' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,concat('LEGACY2_',Legacy2_Identifier) as LocalSystemIdentifier
# MAGIC ,c.FullLegalNameLocalLanguage as FullLegalNameInLocalLanguage 
# MAGIC ,tr.TradeName
# MAGIC ,case when c.ClientType not in ('Related Legal Entity','Legal Entity') then ContactFirstName else null end as FirstName
# MAGIC ,case when c.ClientType not in ('Related Legal Entity','Legal Entity') then ContactMiddleName else null end as MiddleName
# MAGIC ,case when c.ClientType not in ('Related Legal Entity','Legal Entity') then ContactLastName else null end as LastName
# MAGIC from Legacy2_client c
# MAGIC LEFT JOIN Legacy2_trade_name tr on c.ClientId = tr.ClientId
# MAGIC where ClientTypeId in (1,2,3)
# MAGIC )
# MAGIC select * from L2_Details
# MAGIC where FullLegalNameInLocalLanguage is not null or FirstName is not null or MiddleName is not null or LastName is not null or TradeName is not null

# COMMAND ----------

# DBTITLE 1,Fetching GCDS Client information
# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_Clients As
# MAGIC with gcds as
# MAGIC (
# MAGIC select distinct
# MAGIC k.KeyStore_value as identifier
# MAGIC ,k.KeyStore_type
# MAGIC ,c.Party_Type
# MAGIC ,c.GCID
# MAGIC ,c.Trade_name as TradeName
# MAGIC ,case when c.Party_type <> 'Natural Person' then c.Local_name else c.Person_LocalName end FullLegalNameInLocalLanguage
# MAGIC from client_KeyStoreKey k
# MAGIC inner join client_Client c on c.GCID = k.GCID
# MAGIC )
# MAGIC select * from gcds where TradeName is not null or FullLegalNameInLocalLanguage is not null

# COMMAND ----------

# DBTITLE 1,Obtaining all details for Parties present in GCDS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Details_GCDS AS
# MAGIC SELECT distinct
# MAGIC CASE WHEN gcds.identifier = gcob.GcobId AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' 
# MAGIC      THEN gcob.LocalSystemIdentifier
# MAGIC     WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person' THEN ncino.LocalSystemIdentifier
# MAGIC     WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID' THEN l2.LocalSystemIdentifier
# MAGIC     else CONCAT('GCDS_', gcds.GCID)
# MAGIC END AS LocalSystemIdentifier
# MAGIC ,CASE WHEN gcds.identifier = gcob.GcobId AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' THEN 'GCOB'
# MAGIC     WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person' THEN 'GCOB'
# MAGIC     WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC     THEN 'Legacy2'
# MAGIC     Else 'GCDS'
# MAGIC END AS Application
# MAGIC ,COALESCE(gcds.FullLegalNameInLocalLanguage,gcob.FullLegalNameInLocalLanguage, ncino.FullLegalNameInLocalLanguage, l2.FullLegalNameInLocalLanguage) AS FullLegalNameInLocalLanguage
# MAGIC ,COALESCE(gcob.Alias, ncino.Alias) AS Alias
# MAGIC ,COALESCE(gcds.TradeName,gcob.TradeName, ncino.TradeName, l2.TradeName) AS TradeName
# MAGIC ,COALESCE(gcob.FirstName, ncino.FirstName, l2.FirstName) AS FirstName
# MAGIC ,COALESCE(gcob.MiddleName, ncino.MiddleName, l2.MiddleName) AS MiddleName
# MAGIC ,COALESCE(gcob.LastName, ncino.LastName, l2.LastName) AS LastName
# MAGIC FROM GCDS_cLIENTS gcds
# MAGIC LEFT JOIN Gcob_AllPartyDetails gcob ON gcds.identifier = gcob.GcobId AND gcob.Party_type = 'Legal Entity' 
# MAGIC   AND gcds.KeyStore_type = 'GCOBID'
# MAGIC LEFT JOIN Gcob_CaseClientDetails c ON c.SalesforceClientID_nCino = gcds.identifier AND gcds.KeyStore_type = 'NCINOID'
# MAGIC LEFT JOIN Gcob_AllPartyDetails ncino ON c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person'
# MAGIC LEFT JOIN Legacy2 l2 on l2.SalesforceClientID_nCino = gcds.identifier and l2.Party_type = gcds.Party_type 
# MAGIC   and gcds.KeyStore_type = 'NCINOID'

# COMMAND ----------

# DBTITLE 1,data of Sources not in GCDS
# MAGIC %sql
# MAGIC Create or replace temporary view Details_NonGCDS As
# MAGIC with NonGCDS as (
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC ,'GCOB' as Application
# MAGIC ,FullLegalNameInLocalLanguage
# MAGIC ,Alias
# MAGIC ,TradeName
# MAGIC ,FirstName 
# MAGIC ,MiddleName 
# MAGIC ,LastName
# MAGIC from Gcob_AllPartyDetails
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC t1.LocalSystemIdentifier
# MAGIC ,'Legacy2' as Application
# MAGIC ,t1.FullLegalNameInLocalLanguage
# MAGIC ,t1.TradeName as Alias
# MAGIC ,null as TradeName
# MAGIC ,t1.FirstName 
# MAGIC ,t1.MiddleName 
# MAGIC ,t1.LastName
# MAGIC From Legacy2 t1
# MAGIC )
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC ,Application
# MAGIC ,FullLegalNameInLocalLanguage
# MAGIC ,Alias
# MAGIC ,TradeName
# MAGIC ,FirstName 
# MAGIC ,MiddleName 
# MAGIC ,LastName
# MAGIC from NonGCDS 

# COMMAND ----------

# DBTITLE 1,Comparing GCDS and Non GCDS data
# MAGIC %sql
# MAGIC Create or replace temporary view NonGCDS_UniqueDetails As
# MAGIC select * 
# MAGIC from Details_NonGCDS a
# MAGIC left anti join Details_GCDS b
# MAGIC on a.LocalSystemIdentifier = b.LocalSystemIdentifier
# MAGIC

# COMMAND ----------

# DBTITLE 1,RANZ AlternativeNames
# MAGIC %sql
# MAGIC create or Replace Temporary view Ranz_AlternativeNames as
# MAGIC select distinct
# MAGIC CONCAT('RANZ_',cbp.PKEY_SRC_OBJECT) LocalSystemIdentifier,
# MAGIC 'MDM-RANZ' as Application,
# MAGIC null FullLegalNameInLocalLanguage,
# MAGIC null as Alias,
# MAGIC cbc.TRADE_NAME as TradeName,
# MAGIC null FirstName,
# MAGIC null MiddleName,
# MAGIC null LastName
# MAGIC FROM c_b_party_xref as cbp
# MAGIC JOIN c_b_contr_rol_party_xref as crp
# MAGIC     ON crp.FK_PARTY_ID = cbp.ROWID_XREF
# MAGIC JOIN c_b_contract_xref as cbc
# MAGIC     ON cbc.CONTR_ID = crp.FK_CONTR_ID

# COMMAND ----------

# DBTITLE 1,Combining GCDS and Non GCDS details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW AlternativeNames AS
# MAGIC SELECT * FROM Details_GCDS WHERE Application IS NOT NULL
# MAGIC UNION
# MAGIC select * from NonGCDS_UniqueDetails
# MAGIC union
# MAGIC SELECT * from Ranz_AlternativeNames

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_AlternativeNames = spark.table("AlternativeNames")

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_f_AlternativeNames = add_party_identifier(df_AlternativeNames, df_Party_SystemIdentifier)
df_f_AlternativeNames = df_f_AlternativeNames.filter("PartyIdentifier IS NOT NULL")

# COMMAND ----------

# DBTITLE 1,Create table from dataframe
df_f_AlternativeNames.createOrReplaceTempView('Alternative')

# COMMAND ----------

# DBTITLE 1,Logic for Party_AlternativeNames dataobject
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Party_AlternativeNames AS
# MAGIC select distinct
# MAGIC PartyIdentifier
# MAGIC ,Application as SourceApplication
# MAGIC ,'FullLegalNameInLocalLanguage' as PartyNameTypeCode
# MAGIC ,FullLegalNameInLocalLanguage as FullNameValue
# MAGIC ,FirstName 
# MAGIC ,MiddleName 
# MAGIC ,LastName
# MAGIC from Alternative
# MAGIC where FullLegalNameInLocalLanguage is not null or FirstName is not null or MiddleName is not null or LastName is not null
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select distinct
# MAGIC PartyIdentifier
# MAGIC ,Application as SourceApplication
# MAGIC ,'Alias' as PartyNameTypeCode
# MAGIC ,Alias as FullNameValue
# MAGIC ,null as FirstName 
# MAGIC ,null as MiddleName 
# MAGIC ,null as LastName
# MAGIC from Alternative
# MAGIC where Alias is not null
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select distinct
# MAGIC PartyIdentifier
# MAGIC ,Application as SourceApplication
# MAGIC ,'TradeName' as PartyNameTypeCode
# MAGIC ,TradeName as FullNameValue
# MAGIC ,null as FirstName 
# MAGIC ,null as MiddleName 
# MAGIC ,null as LastName
# MAGIC from Alternative
# MAGIC where TradeName is not null

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CREATING A FINAL DATAFRAME BY COMBINING ALL THE INFORMATION FROM DIFFERENT SOURCESYSTEMS AND LOADING THE FINAL DATAOBJECT TO SA RADAR STORAGE ACCOUNT

# COMMAND ----------

df_final_AlternativeNames = spark.table("Party_AlternativeNames")

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
    save_to_saradar_storage_account(df_final_AlternativeNames, party_AlternativeName_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_final_AlternativeNames, party_AlternativeName_dataobject, radar_datamodel_version_number, environment)
