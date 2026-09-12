# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To Achive Data Model result by considering All the source systems where W&R Clients are available and GCDS source
# MAGIC
# MAGIC #### author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Take W&R clients data from different source systems
# MAGIC - Take GCDS data
# MAGIC - Join on GCDSId to further derive required output
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC  Developer   |  Date          |  PBI No.     |   Changes done
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek    |   8-July-2025   |  12757497    |   NLSVF data commented and not capturing
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load 
# MAGIC |Rhea Gupta | 30-Mar-2026 |15580816 | Included RANZ in version 3
# MAGIC | Mahalakshmi V | 20-May-2026 | 16071492 | Included RANZ V4
# MAGIC | Prasad Gadidala | 15-jun-2026 | 16284963 | fixed country ISO codes those are coming as null.
# MAGIC | Gopi Prasad K | 11-Aug-2026 | 16107896 | Party type changes for Legacy2 Source system | 3
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read Files from GDP

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
import os
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
RANZ_ReadStorage = os.environ['AU_GDP_Defined_Storage_Account']
environment=os.environ['ENV']
radar_datamodel_version_number=3
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
SARADAR = "saradar" + environment

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils file
from RadarUtils import *

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

# DBTITLE 1,Defining the dataobjects' name
party_address_dataobject='Party_Address'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(RANZ_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Define Date variables
#Derive the date for which data has to be processes
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
load_dts = f"EDL_LOAD_DTS={Load_Date}*"
print (load_dts)

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
,'endereco'
,'pessoa_endereco'
,'tipo_endereco'
,'cad_municipio'
,'cad_pais'
]})
#gic_load_dts
# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read NLSVF data from GDP defined layer
# import pandas as pd # print list of strings for loading spark dfs from GDP
# load_df = pd.DataFrame({'GDPname':[
#  'nls_dbo_cif_detail'
# ,'nls_dbo_cif'
# ,'nls_dbo_cif_addressbook'
# ]})
# #gic_load_dts
# # Create TempView for each loading table
# for index, row in load_df.iterrows():
#     Read_GDP_Defined_DataObjects(Source='NLSVF', Dataobject=row.GDPname)

# COMMAND ----------

# List of datasets from GDP
load_df =[
'EBX_ISO3166COUNTRYCODES'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='EBX', Dataobject=Object,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading RANZ data from GDP
load_df = [
'c_b_party_rel_addr_xref'
,'c_b_party_xref']
load_ranz_v4_rdm_tables(load_df, Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier dataobject
df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/3/data/{load_dts}/*.parquet"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - Other Sources Data Prepration

# COMMAND ----------

# DBTITLE 1,GIC Clients update to join
# MAGIC %sql
# MAGIC Create or replace temporary view GIC_ClientDetails As
# MAGIC with SEQ_ENDERECO as 
# MAGIC (
# MAGIC   select max(SEQ_ENDERECO) as SEQ_ENDERECO , SEQ_PESSOA from PESSOA_ENDERECO group by SEQ_PESSOA
# MAGIC )
# MAGIC ,Address as 
# MAGIC (
# MAGIC   -- address type 5 and person type 1.
# MAGIC select distinct
# MAGIC c.SEQ_PESSOA
# MAGIC ,COALESCE(DES_MUNICIPIO_ESTRANGEIRO,DES_MUNICIPIO_IBGE) AS RegisteredAddress_City
# MAGIC ,NOM_PAIS as RegisteredAddress_Country
# MAGIC ,COD_COUNTRY_RISK_MTGT as RegisteredAddress_CountryCode
# MAGIC ,NUM_LOGRADOURO as RegisteredAddress_HouseNumber
# MAGIC ,NUM_CEP as RegisteredAddress_Zipcode
# MAGIC ,NOM_LOGRADOURO as RegisteredAddress_StreetLine1
# MAGIC ,DES_COMPLEMENTO as RegisteredAddress_StreetLine2
# MAGIC ,NOM_BAIRRO as RegisteredAddress_StreetLine3
# MAGIC ,null as ResidentialAddress_City
# MAGIC ,null as ResidentialAddress_Country
# MAGIC ,null as ResidentialAddress_CountryCode
# MAGIC ,null as ResidentialAddress_HouseNumber
# MAGIC ,null as ResidentialAddress_Zipcode
# MAGIC ,null as ResidentialAddress_StreetLine1
# MAGIC ,null as ResidentialAddress_StreetLine2
# MAGIC ,null as ResidentialAddress_StreetLine3
# MAGIC from pessoa c
# MAGIC left join SEQ_ENDERECO pn on c.SEQ_PESSOA = pn.SEQ_PESSOA
# MAGIC left join endereco en on pn.SEQ_ENDERECO = en.SEQ_ENDERECO --and en.SEQ_TIPO_ENDERECO in (5)
# MAGIC left join TIPO_ENDERECO tn on en.SEQ_TIPO_ENDERECO = tn.SEQ_TIPO_ENDERECO
# MAGIC left join CAD_MUNICIPIO mu on en.SEQ_MUNICIPIO = mu.SEQ_MUNICIPIO
# MAGIC left join CAD_PAIS pa on en.SEQ_PAIS = pa.SEQ_PAIS
# MAGIC where c.SEQ_TIPO_PESSOA = 1 
# MAGIC
# MAGIC union
# MAGIC
# MAGIC -- getting addresses for client type = 2
# MAGIC select distinct
# MAGIC c.SEQ_PESSOA
# MAGIC ,null as RegisteredAddress_City
# MAGIC ,null as RegisteredAddress_Country
# MAGIC ,null as RegisteredAddress_CountryCode
# MAGIC ,null as RegisteredAddress_HouseNumber
# MAGIC ,null as RegisteredAddress_Zipcode
# MAGIC ,null as RegisteredAddress_StreetLine1
# MAGIC ,null as RegisteredAddress_StreetLine2
# MAGIC ,null as RegisteredAddress_StreetLine3
# MAGIC ,COALESCE(DES_MUNICIPIO_ESTRANGEIRO,DES_MUNICIPIO_IBGE) AS ResidentialAddress_City
# MAGIC ,NOM_PAIS as ResidentialAddress_Country
# MAGIC ,COD_COUNTRY_RISK_MTGT as ResidentialAddress_CountryCode
# MAGIC ,NUM_LOGRADOURO as ResidentialAddress_HouseNumber
# MAGIC ,NUM_CEP as ResidentialAddress_Zipcode
# MAGIC ,NOM_LOGRADOURO as ResidentialAddress_StreetLine1
# MAGIC ,DES_COMPLEMENTO as ResidentialAddress_StreetLine2
# MAGIC ,NOM_BAIRRO as ResidentialAddress_StreetLine3
# MAGIC from pessoa c
# MAGIC left join SEQ_ENDERECO pn on c.SEQ_PESSOA = pn.SEQ_PESSOA
# MAGIC left join endereco en on pn.SEQ_ENDERECO = en.SEQ_ENDERECO --and en.SEQ_TIPO_ENDERECO in (4)
# MAGIC left join TIPO_ENDERECO tn on en.SEQ_TIPO_ENDERECO = tn.SEQ_TIPO_ENDERECO
# MAGIC left join CAD_MUNICIPIO mu on en.SEQ_MUNICIPIO = mu.SEQ_MUNICIPIO
# MAGIC left join CAD_PAIS pa on en.SEQ_PAIS = pa.SEQ_PAIS
# MAGIC where c.SEQ_TIPO_PESSOA = 2 
# MAGIC )
# MAGIC select distinct
# MAGIC p.SEQ_PESSOA 
# MAGIC ,p.COD_INSTITUCIONAL
# MAGIC ,p.NOM_COMPLETO
# MAGIC ,CASE WHEN p.SEQ_TIPO_PESSOA = '2' THEN 'Natural Person' else 'Legal Entity' END AS Party_type
# MAGIC ,ad.RegisteredAddress_City
# MAGIC ,ad.RegisteredAddress_Country
# MAGIC ,ad.RegisteredAddress_CountryCode
# MAGIC ,ad.RegisteredAddress_HouseNumber
# MAGIC ,ad.RegisteredAddress_Zipcode
# MAGIC ,ad.RegisteredAddress_StreetLine1
# MAGIC ,ad.RegisteredAddress_StreetLine2
# MAGIC ,ad.RegisteredAddress_StreetLine3
# MAGIC ,ad.ResidentialAddress_City
# MAGIC ,ad.ResidentialAddress_Country
# MAGIC ,ad.ResidentialAddress_CountryCode
# MAGIC ,ad.ResidentialAddress_HouseNumber
# MAGIC ,ad.ResidentialAddress_Zipcode
# MAGIC ,ad.ResidentialAddress_StreetLine1
# MAGIC ,ad.ResidentialAddress_StreetLine2
# MAGIC ,ad.ResidentialAddress_StreetLine3
# MAGIC ,CONCAT('GIC_', p.COD_INSTITUCIONAL) as LocalSystemIdentifier
# MAGIC from pessoa p
# MAGIC INNER JOIN Address ad on p.SEQ_PESSOA = ad.SEQ_PESSOA
# MAGIC

# COMMAND ----------

# DBTITLE 1,GCOB CaseClientDetails update to join on client type
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_CaseClientDetails As
# MAGIC select * 
# MAGIC ,case when ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,Case when ClientType = 'Legal Entity' then concat('GCOB_LEC_', GcobId)
# MAGIC       when ClientType in ('Natural Person', 'Natural Person acting in a Professional Capacity (NPPC)') then concat('GCOB_NP_NPPC_', GcobId)
# MAGIC End as LocalSystemIdentifier
# MAGIC from party_case_client_details
# MAGIC where CaseStatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,GCOB All Party Details update to join on client type
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllPartyDetails_Row As
# MAGIC select p.* 
# MAGIC ,case when p.ClientType = 'LegalEntityClient' then 'Legal Entity' 
# MAGIC       when p.ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when p.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity' 
# MAGIC       when p.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,CONCAT('GCOB_', p.UniquePartyId) AS LocalSystemIdentifier
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY p.gcobid,p.ClientType ORDER BY CASE WHEN p.ClientLifeCycleStatus='Client' THEN 1 ELSE 2 END ASC, c.CaseCompletedDate DESC) AS ROWNUM
# MAGIC from party_AllPartyDetails p
# MAGIC Inner join Gcob_CaseClientDetails c on p.UniquePartyId = c.UniqueGcobId
# MAGIC where Status = 'Live'

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
# MAGIC ,c.CaseCompletedDate
# MAGIC ,CONCAT('GCOB_', p.UniquePartyId) AS LocalSystemIdentifier
# MAGIC ,case when p.CaseStatusName is null then 'RelatedParty' else c.CaseStatusName end as StatusName
# MAGIC from party_AllPartyDetails p
# MAGIC LEFT JOIN Gcob_CaseClientDetails c on p.UniquePartyId = c.UniqueGcobId
# MAGIC where Status = 'Live'
# MAGIC )
# MAGIC select distinct
# MAGIC *
# MAGIC ,concat(GCOBId,'-',Party_Type) as GCOB_Party
# MAGIC ,concat(SalesforceClientID_nCino,'-',Party_Type) as GCOB_NCINO_Party
# MAGIC from details
# MAGIC where StatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,GCOB All Party Details Unique GCOBId
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllPartyDetails As
# MAGIC select * from Gcob_AllPartyDetails_Row 

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
# MAGIC select distinct 
# MAGIC clientId,SalesforceClientID_nCino
# MAGIC ,CASE
# MAGIC   WHEN IsClient = 'true' and  ClientTypeId = 1 THEN concat('LEC_', GcobId)
# MAGIC   WHEN IsClient = 'true' and ClientTypeId in (2,3) THEN concat('NP_NPPC_', GcobId)
# MAGIC   WHEN IsClient = 'false' and ClientTypeId = 1 THEN concat('RLEP_', GcobId)
# MAGIC   WHEN IsClient = 'false' and ClientTypeId in (2,3) THEN concat('RNPP_', GcobId)
# MAGIC END as Legacy2_Identifier
# MAGIC ,case when ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when ClientType = 'Related Legal Entity' then 'Related Legal Entity' 
# MAGIC       when ClientType = 'Related Natural Person' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,concat('LEGACY2_',Legacy2_Identifier) as LocalSystemIdentifier
# MAGIC ,RegisteredAddressNumber as RegisteredNumber,RegisteredStreet,RegisteredCity,RegisteredRegion,RegisteredPostcode as RegisteredPostalCode,RegisteredCountry as RegisteredCountryName,null as RegisteredCountryIsoCode,OperationalNumber as OperatingNumber,OperationalStreet as OperatingStreet,OperationalCity as OperatingCity,OperationalRegion as OperatingRegion,OperationalPostcode as OperatingPostalCode,OperationalCountry as OperatingCountryName,null as OperatingCountryIsoCode
# MAGIC from Legacy2_client 
# MAGIC where ClientTypeId in (1,2,3)

# COMMAND ----------

# DBTITLE 1,Read GCDS "GCOB"-"GCOB-NCINO"-"GIC"-Client data
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

# MAGIC %md
# MAGIC #####GCDS - Other Sources Address Data

# COMMAND ----------

# DBTITLE 1,GCDS Address
# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_Address As
# MAGIC With GC_Address as 
# MAGIC (
# MAGIC select distinct 
# MAGIC CASE WHEN gcds.identifier = gcob.GcobId 
# MAGIC      AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' THEN gcob.LocalSystemIdentifier
# MAGIC      WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person' THEN ncino.LocalSystemIdentifier
# MAGIC      WHEN gcds.identifier = gic.COD_INSTITUCIONAL AND gcds.KeyStore_type = 'GIC' THEN gic.LocalSystemIdentifier
# MAGIC      WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC      THEN l2.LocalSystemIdentifier
# MAGIC      else concat('GCDS_',gcds.gcid)
# MAGIC END AS LocalSystemIdentifier
# MAGIC ,'GCDS' as AddressSystem
# MAGIC ,gcds.gcid as AddressSystem_ID
# MAGIC ,gcds.Party_Type
# MAGIC ,'Registered' as AddressType
# MAGIC ,gcds.Registered_address_house_nr as HouseNumber
# MAGIC ,concat(gcds.Registered_address_streetLine1,'-',gcds.Registered_address_streetLine2,'-',gcds.Registered_address_streetLine3) as Street
# MAGIC ,gcds.Registered_address_city as City
# MAGIC ,gcds.Registered_address_region as Region
# MAGIC ,gcds.Registered_address_zipcode as PostalCode
# MAGIC ,gcds.Registered_address_country as Country
# MAGIC ,gcds.Registered_address_country_code as CountryCode
# MAGIC From GCDS_Clients gcds
# MAGIC LEFT JOIN Gcob_AllPartyDetails gcob on gcds.identifier = gcob.GcobId
# MAGIC         and gcob.Party_type = 'Legal Entity'
# MAGIC         and gcds.KeyStore_type = 'GCOBID'
# MAGIC LEFT JOIN Gcob_CaseClientDetails c on c.SalesforceClientID_nCino = gcds.identifier 
# MAGIC         and gcds.KeyStore_type = 'NCINOID'
# MAGIC LEFT JOIN Gcob_AllPartyDetails ncino on c.ClientId = ncino.PartyId 
# MAGIC         and ncino.Party_type = 'Natural Person' 
# MAGIC LEFT JOIN GIC_ClientDetails gic on gcds.identifier = gic.COD_INSTITUCIONAL
# MAGIC         and gcds.KeyStore_type = 'GIC'
# MAGIC LEFT JOIN Legacy2 l2 on l2.SalesforceClientID_nCino = gcds.identifier 
# MAGIC         and l2.Party_type = gcds.Party_type
# MAGIC         and gcds.KeyStore_type = 'NCINOID'
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC CASE WHEN gcds.identifier = gcob.GcobId
# MAGIC      AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' THEN gcob.LocalSystemIdentifier
# MAGIC      WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person' THEN ncino.LocalSystemIdentifier
# MAGIC      WHEN gcds.identifier = gic.COD_INSTITUCIONAL AND gcds.KeyStore_type = 'GIC' THEN gic.LocalSystemIdentifier
# MAGIC      WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC      THEN l2.LocalSystemIdentifier
# MAGIC      else concat('GCDS_',gcds.gcid)
# MAGIC END AS LocalSystemIdentifier
# MAGIC ,'GCDS' as AddressSystem
# MAGIC ,gcds.gcid as AddressSystem_ID
# MAGIC ,gcds.Party_Type
# MAGIC ,'Operational' as AddressType
# MAGIC ,gcds.Principal_address_house_nr as HouseNumber
# MAGIC ,concat(gcds.Principal_address_streetLine1,'-',gcds.Principal_address_streetLine2,'-',gcds.Principal_address_streetLine3) as Street
# MAGIC ,gcds.Principal_address_city as City
# MAGIC ,gcds.Principal_address_region as Region
# MAGIC ,gcds.Principal_address_zipcode as PostalCode
# MAGIC ,gcds.Principal_address_country as Country
# MAGIC ,gcds.Principal_address_country_code as CountryCode
# MAGIC From GCDS_Clients gcds
# MAGIC LEFT JOIN Gcob_AllPartyDetails gcob on gcds.identifier = gcob.GcobId
# MAGIC         and gcob.Party_type = 'Legal Entity'
# MAGIC         and gcds.KeyStore_type = 'GCOBID'
# MAGIC LEFT JOIN Gcob_CaseClientDetails c on c.SalesforceClientID_nCino = gcds.identifier 
# MAGIC         and gcds.KeyStore_type = 'NCINOID'
# MAGIC LEFT JOIN Gcob_AllPartyDetails ncino on c.ClientId = ncino.PartyId 
# MAGIC         and ncino.Party_type = 'Natural Person' 
# MAGIC LEFT JOIN GIC_ClientDetails gic on gcds.identifier = gic.COD_INSTITUCIONAL
# MAGIC         and gcds.KeyStore_type = 'GIC'
# MAGIC LEFT JOIN Legacy2 l2 on l2.SalesforceClientID_nCino = gcds.identifier 
# MAGIC         and l2.Party_type = gcds.Party_type
# MAGIC         and gcds.KeyStore_type = 'NCINOID'
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC CASE WHEN gcds.identifier = gcob.GcobId
# MAGIC      AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' THEN gcob.LocalSystemIdentifier
# MAGIC      WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person' THEN ncino.LocalSystemIdentifier
# MAGIC      WHEN gcds.identifier = gic.COD_INSTITUCIONAL AND gcds.KeyStore_type = 'GIC' THEN gic.LocalSystemIdentifier
# MAGIC      WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC      THEN l2.LocalSystemIdentifier
# MAGIC      else concat('GCDS_',gcds.gcid)
# MAGIC END AS LocalSystemIdentifier
# MAGIC ,'GCDS' as AddressSystem
# MAGIC ,gcds.gcid as AddressSystem_ID
# MAGIC ,gcds.Party_Type
# MAGIC ,'Residential' as AddressType
# MAGIC ,gcds.ResidentialAddress_HouseNumber as HouseNumber
# MAGIC ,concat(gcds.ResidentialAddress_streetLine1,'-',gcds.ResidentialAddress_streetLine2,'-',gcds.ResidentialAddress_streetLine3) as Street
# MAGIC ,gcds.ResidentialAddress_city as City
# MAGIC ,gcds.ResidentialAddress_region as Region
# MAGIC ,gcds.ResidentialAddress_zipcode as PostalCode
# MAGIC ,gcds.ResidentialAddress_country as Country
# MAGIC ,gcds.ResidentialAddress_CountryCode as CountryCode
# MAGIC From GCDS_Clients gcds
# MAGIC LEFT JOIN Gcob_AllPartyDetails gcob on gcds.identifier = gcob.GcobId
# MAGIC         and gcob.Party_type = 'Legal Entity'
# MAGIC         and gcds.KeyStore_type = 'GCOBID'
# MAGIC LEFT JOIN Gcob_CaseClientDetails c on c.SalesforceClientID_nCino = gcds.identifier 
# MAGIC         and gcds.KeyStore_type = 'NCINOID'
# MAGIC LEFT JOIN Gcob_AllPartyDetails ncino on c.ClientId = ncino.PartyId 
# MAGIC         and ncino.Party_type = 'Natural Person' 
# MAGIC LEFT JOIN GIC_ClientDetails gic on gcds.identifier = gic.COD_INSTITUCIONAL
# MAGIC         and gcds.KeyStore_type = 'GIC'
# MAGIC LEFT JOIN Legacy2 l2 on l2.SalesforceClientID_nCino = gcds.identifier 
# MAGIC         and l2.Party_type = gcds.Party_type
# MAGIC         and gcds.KeyStore_type = 'NCINOID'
# MAGIC )
# MAGIC select * from GC_Address

# COMMAND ----------

# DBTITLE 1,GCOB Address
# MAGIC %sql
# MAGIC Create or replace temporary view GCOB_Address As
# MAGIC with G_Address as
# MAGIC (
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC ,'GCOB' as AddressSystem
# MAGIC ,UniquePartyId as AddressSystem_ID
# MAGIC ,Party_Type
# MAGIC ,'Registered' as AddressType
# MAGIC ,RegisteredNumber as HouseNumber
# MAGIC ,RegisteredStreet as Street
# MAGIC ,RegisteredCity as City
# MAGIC ,RegisteredRegion as Region
# MAGIC ,RegisteredPostalCode as PostalCode
# MAGIC ,RegisteredCountryName as Country
# MAGIC ,RegisteredCountryIsoCode as CountryCode
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY gcobid,ClientType ORDER BY caseid desc) AS ROWNUM
# MAGIC From Gcob_AllParty
# MAGIC where party_Type in ('Legal Entity','Related Legal Entity')
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC ,'GCOB' as AddressSystem
# MAGIC ,UniquePartyId as AddressSystem_ID
# MAGIC ,Party_Type
# MAGIC ,'Operational' as AddressType
# MAGIC ,OperatingNumber as HouseNumber
# MAGIC ,OperatingStreet as Street
# MAGIC ,OperatingCity as City
# MAGIC ,OperatingRegion as Region
# MAGIC ,OperatingPostalCode as PostalCode
# MAGIC ,OperatingCountryName as Country
# MAGIC ,OperatingCountryIsoCode as CountryCode
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY gcobid,ClientType ORDER BY caseid desc) AS ROWNUM
# MAGIC From Gcob_AllParty
# MAGIC where party_Type in ('Legal Entity','Related Legal Entity')
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC , 'GCOB' as AddressSystem
# MAGIC ,UniquePartyId as AddressSystem_ID
# MAGIC ,Party_Type
# MAGIC ,'Operational' as AddressType
# MAGIC ,OperatingNumber as HouseNumber
# MAGIC ,OperatingStreet as Street
# MAGIC ,OperatingCity as City
# MAGIC ,OperatingRegion as Region
# MAGIC ,OperatingPostalCode as PostalCode
# MAGIC ,OperatingCountryName as Country
# MAGIC ,OperatingCountryIsoCode as CountryCode
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY gcobid,ClientType ORDER BY caseid desc) AS ROWNUM
# MAGIC From Gcob_AllParty
# MAGIC where party_Type in ('Related Natural Person','Natural Person')
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC ,'GCOB' as AddressSystem
# MAGIC ,UniquePartyId as AddressSystem_ID
# MAGIC ,Party_Type
# MAGIC ,'Residential' as AddressType
# MAGIC ,RegisteredNumber as HouseNumber
# MAGIC ,RegisteredStreet as Street
# MAGIC ,RegisteredCity as City
# MAGIC ,RegisteredRegion as Region
# MAGIC ,RegisteredPostalCode as PostalCode
# MAGIC ,RegisteredCountryName as Country
# MAGIC ,RegisteredCountryIsoCode as CountryCode
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY gcobid,ClientType ORDER BY caseid desc) AS ROWNUM
# MAGIC From Gcob_AllParty
# MAGIC where party_Type in ('Related Natural Person','Natural Person')
# MAGIC )
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC ,AddressSystem
# MAGIC ,AddressSystem_ID
# MAGIC ,Party_Type
# MAGIC ,AddressType
# MAGIC ,HouseNumber
# MAGIC ,Street
# MAGIC ,City
# MAGIC ,Region
# MAGIC ,PostalCode
# MAGIC ,Country
# MAGIC ,CountryCode
# MAGIC from G_Address --gcob
# MAGIC where ROWNUM = 1

# COMMAND ----------

# DBTITLE 1,GIC Address
# MAGIC %sql
# MAGIC Create or replace temporary view GIC_Address As
# MAGIC With GIC_Address as 
# MAGIC (
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC ,'GIC' as AddressSystem
# MAGIC ,COD_INSTITUCIONAL as AddressSystem_ID
# MAGIC ,Party_Type
# MAGIC ,'Registered' as AddressType
# MAGIC ,RegisteredAddress_HouseNumber as HouseNumber
# MAGIC ,concat(Registeredaddress_streetLine1,'-',Registeredaddress_streetLine2,'-',Registeredaddress_streetLine3) as Street
# MAGIC ,Registeredaddress_city as City
# MAGIC ,null as Region
# MAGIC ,Registeredaddress_zipcode as PostalCode
# MAGIC ,Registeredaddress_country as Country
# MAGIC ,RegisteredAddress_CountryCode as CountryCode
# MAGIC From GIC_ClientDetails
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC ,'GIC' as AddressSystem
# MAGIC ,COD_INSTITUCIONAL as AddressSystem_ID
# MAGIC ,Party_Type
# MAGIC ,'Residential' as AddressType
# MAGIC ,ResidentialAddress_HouseNumber as HouseNumber
# MAGIC ,concat(ifnull(ResidentialAddress_streetLine1,''),'-',ifnull(ResidentialAddress_streetLine2,''),'-',ifnull(ResidentialAddress_streetLine3,'')) as Street
# MAGIC ,ResidentialAddress_city as City
# MAGIC ,null as Region
# MAGIC ,ResidentialAddress_zipcode as PostalCode
# MAGIC ,ResidentialAddress_country as Country
# MAGIC ,ResidentialAddress_CountryCode as CountryCode
# MAGIC From GIC_ClientDetails
# MAGIC )
# MAGIC select distinct
# MAGIC gic.* 
# MAGIC from GIC_Address gic

# COMMAND ----------

# DBTITLE 1,Legacy2 Address
# MAGIC %sql
# MAGIC Create or replace temporary view Legacy2_Address As
# MAGIC with L_Address as
# MAGIC (
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC ,'Legacy2' as AddressSystem
# MAGIC ,Legacy2_Identifier as AddressSystem_ID
# MAGIC ,Party_Type
# MAGIC ,'Registered' as AddressType
# MAGIC ,RegisteredNumber as HouseNumber
# MAGIC ,RegisteredStreet as Street
# MAGIC ,RegisteredCity as City
# MAGIC ,RegisteredRegion as Region
# MAGIC ,RegisteredPostalCode as PostalCode
# MAGIC ,RegisteredCountryName as Country
# MAGIC ,RegisteredCountryIsoCode as CountryCode
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY Legacy2_Identifier ORDER BY clientid DESC) AS ROWNUM
# MAGIC From Legacy2
# MAGIC where party_Type in ('Legal Entity','Related Legal Entity')
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC ,'Legacy2' as AddressSystem
# MAGIC ,Legacy2_Identifier as AddressSystem_ID
# MAGIC ,Party_Type
# MAGIC ,'Operational' as AddressType
# MAGIC ,OperatingNumber as HouseNumber
# MAGIC ,OperatingStreet as Street
# MAGIC ,OperatingCity as City
# MAGIC ,OperatingRegion as Region
# MAGIC ,OperatingPostalCode as PostalCode
# MAGIC ,OperatingCountryName as Country
# MAGIC ,OperatingCountryIsoCode as CountryCode
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY Legacy2_Identifier ORDER BY clientid DESC) AS ROWNUM
# MAGIC From Legacy2
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC ,'Legacy2' as AddressSystem
# MAGIC ,Legacy2_Identifier as AddressSystem_ID
# MAGIC ,Party_Type
# MAGIC ,'Residential' as AddressType
# MAGIC ,RegisteredNumber as HouseNumber
# MAGIC ,RegisteredStreet as Street
# MAGIC ,RegisteredCity as City
# MAGIC ,RegisteredRegion as Region
# MAGIC ,RegisteredPostalCode as PostalCode
# MAGIC ,RegisteredCountryName as Country
# MAGIC ,RegisteredCountryIsoCode as CountryCode
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY Legacy2_Identifier ORDER BY clientid DESC) AS ROWNUM
# MAGIC From Legacy2
# MAGIC where party_Type in ('Related Natural Person','Natural Person')
# MAGIC )
# MAGIC select distinct
# MAGIC LocalSystemIdentifier
# MAGIC ,AddressSystem
# MAGIC ,AddressSystem_ID
# MAGIC ,Party_Type
# MAGIC ,AddressType
# MAGIC ,HouseNumber
# MAGIC ,Street
# MAGIC ,City
# MAGIC ,Region
# MAGIC ,PostalCode
# MAGIC ,Country
# MAGIC ,CountryCode
# MAGIC from L_Address 
# MAGIC where ROWNUM = 1

# COMMAND ----------

# DBTITLE 1,NLSVF Address
# MAGIC %sql
# MAGIC /*
# MAGIC Create or replace temporary view NLSVF_Address As
# MAGIC select distinct
# MAGIC concat('NLSVF_',c.CIFNUMBER) as LocalSystemIdentifier
# MAGIC ,'NLSVF' as AddressSystem
# MAGIC ,c.CIFNUMBER as AddressSystem_ID
# MAGIC ,'Registered' as AddressType
# MAGIC ,null as HouseNumber
# MAGIC ,concat(ifnull(a.STREET_ADDRESS1,''),'-',ifnull(a.STREET_ADDRESS2,'')) as Street
# MAGIC ,a.City as City
# MAGIC ,a.STATE as Region
# MAGIC ,a.Zip as PostalCode
# MAGIC ,a.COUNTRY as Country
# MAGIC ,a.COUNTRY as CountryCode
# MAGIC From nls_dbo_cif_addressbook a
# MAGIC inner join nls_dbo_cif c on a.CIFNO = c.CIFNO
# MAGIC */

# COMMAND ----------

# DBTITLE 1,RANZ Address
country_list=[("AU","Australia"),
              ("NZ","New Zealand")]
country_column=["country_code",
                "country_name"]

df=spark.createDataFrame(country_list,country_column)
df.createOrReplaceTempView("country_mapping")

# COMMAND ----------

Region_list = [
    ("NSW", "New South Wales"),
    ("QLD", "Queensland"),
    ("SA", "South Australia"),
    ("TAS", "Tasmania"),
    ("VIC", "Victoria"),
    ("WA", "Western Australia"),

    # New Zealand Regions
    ("AUK", "Auckland"),
    ("BOP", "Bay of Plenty"),
    ("CAN", "Canterbury"),
    ("GIS", "Gisborne"),
    ("HKB", "Hawke's Bay"),
    ("MWT", "Manawatu-Wanganui"),
    ("MBH", "Marlborough"),
    ("NSN", "Nelson"),
    ("NTL", "Northland"),
    ("OTA", "Otago"),
    ("STL", "Southland"),
    ("TKI", "Taranaki"),
    ("TAS", "Tasman"),
    ("WKO", "Waikato"),
    ("WGN", "Wellington"),
    ("WTC", "West Coast")
]

Region_column = ["Region_code", "Region_name"]

df = spark.createDataFrame(Region_list, Region_column)
df.createOrReplaceTempView("Region_mapping")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view ranz_address as
# MAGIC (
# MAGIC select
# MAGIC distinct CONCAT('RANZ_',cbp.PKEY_SRC_OBJECT) LocalSystemIdentifier,
# MAGIC 'MDM-RANZ' AddressSystem,
# MAGIC pra.PKEY_SRC_OBJECT AddressSystem_ID,
# MAGIC ---CONCAT('GCDS_', gcid) AddressSystem_ID,
# MAGIC CASE WHEN pra.ADDR_TYPE_CD = "RESD" THEN "Residential" 
# MAGIC      WHEN pra.ADDR_TYPE_CD = "REGD" THEN "Registered"
# MAGIC      WHEN pra.ADDR_TYPE_CD = null THEN ""
# MAGIC      Else "Operational"
# MAGIC      END as AddressType,
# MAGIC pra.CITY City,
# MAGIC cm.country_name as Country,
# MAGIC pra.CNTRY_CD as CountryCode,
# MAGIC Null as HouseNumber,
# MAGIC pra.POSTAL_CD as PostalCode,
# MAGIC rm.Region_name as Region,
# MAGIC --pra.ADDR_LN1 Street
# MAGIC concat(ifnull(pra.ADDR_LN1,''),'-',ifnull(pra.ADDR_LN2,'')) as Street
# MAGIC from c_b_party cbp
# MAGIC left join c_b_party_rel_addr pra on cbp.ROWID_XREF=pra.FK_PARTY_ID
# MAGIC left join country_mapping cm on cm.country_code=pra.CNTRY_CD
# MAGIC left join Region_mapping rm on rm.Region_code=pra.STATE_CD
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Non GCDS Address
# MAGIC %sql
# MAGIC Create or replace temporary view NonGCDS_Derived_Address As
# MAGIC select distinct AddressSystem ,AddressSystem_ID ,LocalSystemIdentifier , AddressType ,HouseNumber ,Street ,City ,Region ,PostalCode ,Country ,CountryCode  from GCOB_Address
# MAGIC union
# MAGIC select distinct AddressSystem ,AddressSystem_ID ,LocalSystemIdentifier , AddressType ,HouseNumber ,Street ,City ,Region ,PostalCode ,Country ,CountryCode  from GIC_Address
# MAGIC union
# MAGIC select distinct AddressSystem ,AddressSystem_ID ,LocalSystemIdentifier , AddressType ,HouseNumber ,Street ,City ,Region ,PostalCode ,Country ,CountryCode  from Legacy2_Address
# MAGIC union
# MAGIC select distinct AddressSystem ,AddressSystem_ID ,LocalSystemIdentifier , AddressType ,HouseNumber ,Street ,City ,Region ,PostalCode ,Country ,CountryCode  from ranz_address

# COMMAND ----------

# DBTITLE 1,Unique Local System Identifier
# MAGIC %sql
# MAGIC Create or replace temporary view NonGCDS_UniquePartyAddress As
# MAGIC select * 
# MAGIC from NonGCDS_Derived_Address a
# MAGIC left anti join GCDS_Address b
# MAGIC on a.LocalSystemIdentifier = b.LocalSystemIdentifier
# MAGIC

# COMMAND ----------

# DBTITLE 1,Party Address
# MAGIC %sql
# MAGIC Create or replace temporary view Party_Address As
# MAGIC select distinct AddressSystem ,AddressSystem_ID ,LocalSystemIdentifier , AddressType ,HouseNumber ,Street ,City ,Region ,PostalCode ,Country ,CountryCode from GCDS_Address
# MAGIC union
# MAGIC select distinct AddressSystem ,AddressSystem_ID ,LocalSystemIdentifier , AddressType ,HouseNumber ,Street ,City ,Region ,PostalCode ,Country ,CountryCode from NonGCDS_UniquePartyAddress

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_address=spark.table('Party_Address')

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_final_party_address = add_party_identifier(df_party_address, df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 1,Read DF to table
df_final_party_address.createOrReplaceTempView('Address')

# COMMAND ----------

# DBTITLE 1,Removing record where PartyIdentifier is null
# MAGIC %sql
# MAGIC Create or replace temporary view Party_Address_Final As
# MAGIC With RowNumber as 
# MAGIC (
# MAGIC select distinct
# MAGIC PartyIdentifier 
# MAGIC ,AddressSystem 
# MAGIC ,AddressSystem_ID 
# MAGIC ,AddressType 
# MAGIC ,HouseNumber 
# MAGIC ,Street 
# MAGIC ,City 
# MAGIC ,Region 
# MAGIC ,PostalCode 
# MAGIC ,Country 
# MAGIC ,CountryCode
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY PartyIdentifier,AddressType ORDER BY AddressSystem DESC) AS ROWNUM
# MAGIC from Address 
# MAGIC where PartyIdentifier is not null
# MAGIC )
# MAGIC select  
# MAGIC distinct
# MAGIC PartyIdentifier 
# MAGIC ,AddressSystem 
# MAGIC ,AddressSystem_ID 
# MAGIC ,AddressType 
# MAGIC ,HouseNumber 
# MAGIC ,Street 
# MAGIC ,City 
# MAGIC ,Region 
# MAGIC ,PostalCode 
# MAGIC ,Country 
# MAGIC ,CountryCode
# MAGIC from RowNumber where ROWNUM = 1 

# COMMAND ----------

# DBTITLE 1,Create final dataframe
df_Party_Address_Final=spark.table('Party_Address_Final')

# COMMAND ----------

# DBTITLE 1,Country ISO static table those are not available in EBX country table
Country_ISO_list = [
    ("Cayman Islands", "KY"),
    ("Russian Federation", "RU"),
    ("United States of America", "US"),
    ("United Kingdom of Great Britain and Northern Ireland", "GB"),
    ("NONE", ""),
    ("United Kingdom", "GB"),
    ("Tanzania, United Republic of", "TZ"),
    ("Netherlands", "NL"),
    ("Korea (South)", "KR"),
    ("United Arab Emirates", "AE"),
    ("Turkey", "TR"),
    ("Korea, Republic of", "KR"),
    ("United States", "US"),
    ("Bahamas", "BS"),
    ("Iran, Islamic Republic of", "IR"),
    ("Philippines", "PH"),
    ("Taiwan, Province of China", "TW"),
    ("Lao People's Democratic Republic", "LA"),
    ("United States Minor Outlying Islands", "UM")
]
df = spark.createDataFrame(Country_ISO_list, ["country", "iso"])
df.createOrReplaceTempView("Staticmapping_country_iso")


# COMMAND ----------

# DBTITLE 1,final address table join with EBX and static mapping ISO codes
# MAGIC %sql
# MAGIC Create or replace temporary view PartyAddress As
# MAGIC select 
# MAGIC pf.PartyIdentifier,
# MAGIC pf.AddressSystem,
# MAGIC pf.AddressSystem_ID,
# MAGIC pf.AddressType,
# MAGIC pf.HouseNumber,
# MAGIC pf.Street,
# MAGIC pf.City,
# MAGIC pf.Region,
# MAGIC pf.PostalCode,
# MAGIC pf.Country,
# MAGIC CASE WHEN pf.Country IS NOT NULL AND pf.CountryCode IS NULL THEN COALESCE(ebx.ALPHA_2_CODE, scm.iso)
# MAGIC ELSE pf.CountryCode  END AS CountryCode
# MAGIC from party_address_final  pf
# MAGIC left join EBX_ISO3166COUNTRYCODES ebx on pf.country=ebx.SHORT_NAME_EN
# MAGIC left join Staticmapping_country_iso scm on scm.country=pf.country

# COMMAND ----------

df_Party_Address=spark.table('PartyAddress')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
    save_to_saradar_storage_account(df_Party_Address, party_address_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_Party_Address, party_address_dataobject, radar_datamodel_version_number, environment)