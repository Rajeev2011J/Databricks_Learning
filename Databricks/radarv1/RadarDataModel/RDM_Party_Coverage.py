# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To create a dataobject to capture the staffs and coverage teams responsible for a party
# MAGIC
# MAGIC #### Authors
# MAGIC - Mahalakshmi.Vengateswaran@rabobank.com
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC - Prajit.tatari@rabobank.com
# MAGIC
# MAGIC ##### Flow of Logic
# MAGIC - Reading GCOB,GCDS,GIC dataobjects from GDP
# MAGIC - Fetching the coverage details across different dimensions from different SourceSystems
# MAGIC - Loading the final dataobject to SA RADAR Storage account
# MAGIC
# MAGIC   ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |22-July-205 |13228980 |Used explode_outer and other logic change
# MAGIC | Abhishek Jaiswal   |25-July-205 | 13147779| Added Legacy2 and other changes	
# MAGIC | Prajit Tatari   |04-Sept-205 | 13327403| Added LocationCountryCode
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load

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

# DBTITLE 1,Read environment variables from cluster
app_reg_app_id = os.environ["APP_REG_APP_ID"]
ReadStorage = os.environ["GDP_STORAGE_NAME"]
TenantId = os.environ["TENANT_ID"]
GIC_ReadStorage = os.environ["GDP_SA_STORAGE_NAME"]
environment = os.environ["ENV"]
radar_datamodel_version_number = 1

# COMMAND ----------

# DBTITLE 1,Defining the dataobject name
party_Coverage_dataobject = "Party_Coverage"

# COMMAND ----------

# DBTITLE 1,Defining the SARADAR Storage account
SARADAR = "saradar" + environment

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(SARADAR)

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
            "client_KeyStoreKey",
            "client_PartyRole",
            "client_Products",
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
            "party_local_client_Owners",
            "party_products_and_services",
            "party_allcountries"
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

# DBTITLE 1,Reading GIC Dataobjects from GDP
# List of datasets from GDP
load_df = pd.DataFrame({"GDPname": [
'pessoa_linha_negocio'
,'pessoa'
,'vwgic_rdl_pessoa_linha_negocio'
,'vwgic_rdl_pessoa_tipo_cadastro'
,'pessoa_email'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read Legacy2 data from GDP defined layer
# List of datasets from GDP
load_df = [
'Legacy2_case_client_details'
,'Legacy2_products_and_services'
,'Legacy2_local_client_Owners'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2',Load_Date=Load_Date)

# COMMAND ----------

from pyspark.sql.functions import explode, array, lit

# Define the mapping between CountryISO and LocationCode
mapping = {
    'AR': ['ARG'],
    'HK': ['HKG', 'ASIA','ASRM'],
    'US': ['ATL', 'DAL', 'USANA','CHI','NARM','NEY','RAF','RDS','RNA','SAF','RSEC'],
    'AU': ['AUS', 'AUSA'],
    'CN': ['BEI', 'CHN', 'CHNDBU', 'SHA', 'SHAU'],
    'BE': ['BEL'],
    'BR': ['BRA', 'BRS', 'USASA'],
    'CA': ['CAN','CARUR'],
    'CL': ['CHL'],
    'CW': ['CUW'],
    'DE': ['DEU'],
    'ES': ['ESP'],
    'FR': ['FRA'],
    'GB': ['GBR','EURA', 'LOG', 'LONR'],
    'NL': ['GLOA', 'HOFE', 'HOFH','HOFA', 'RTN','UOTH','UPE', 'UTR', 'UTRA', 'URF', 'URFO', 'URMB','UTG','FOUND'],
    'HU': ['HUN'],
    'IE': ['IAC', 'IRL'],
    'ID': ['IDN'],
    'IN': ['IND', 'REA'],
    'IT': ['ITA'],
    'JP': ['JPN'],
    'KE': ['KEN'],
    'KR': ['KOR'],
    'MX': ['MEX', 'MEXS'],
    'MY': ['MYS'],
    'NZ': ['NZL'],
    'PE': ['PER'],
    'PL': ['POL', 'PBG'],
    'RU': ['RUS','ZAO'],
    'SG': ['SGP', 'SGPDBU'],
    'TW': ['TAI'],
    'TH': ['THA'],
    'TR': ['TUR', 'TURAS'],
    'VN': ['VNM'],
    'ZA': ['ZAF']
}

# Convert the mapping dictionary to a list of tuples
data = [(iso, loc) for iso, locs in mapping.items() for loc in locs]
# Create Spark DataFrame
df = spark.createDataFrame(data, ["CountryISO", "LocationCode"])
df.createOrReplaceTempView('ISOCode_static_table')
# display(df)

# COMMAND ----------

# Define the mapping between RegionName and LocationISOCode
mapping = {
    'Asia': ['HK','CN','ID','IN','JP','KR','MY','SG','TW','TH','TR','VN'],
    'Australia Pacific': ['AU', 'NZ'],
    'Europe': ['BE', 'DE', 'ES','GB','FR','HU','IE','IT','KE','PL','NL','RU','ZA'],
    'North America': ['US', 'CA','MX'],
    'South America': ['AR', 'BR', 'CL', 'CW', 'PE']
}

# Convert the mapping dictionary to a list of tuples
data = [(region, iso) for region, isos in mapping.items() for iso in isos]
df = spark.createDataFrame(data, ["RegionName", "LocationISOCode"])
df.createOrReplaceTempView('RegionISO_static_table')

# display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC #####TRANSFORMATION TO GET Party_Coverage OBJECT 

# COMMAND ----------

# DBTITLE 1,Updating GCOB Case Client Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_CaseClientDetails As
# MAGIC select
# MAGIC   *,
# MAGIC   case
# MAGIC     when ClientType = 'Legal Entity' then 'Legal Entity'
# MAGIC     when
# MAGIC       ClientType in ('Natural Person', 'Natural Person acting in a Professional Capacity (NPPC)')
# MAGIC     then
# MAGIC       'Natural Person'
# MAGIC     else null
# MAGIC   end as Party_type,
# MAGIC   Case
# MAGIC     when ClientType = 'Legal Entity' then concat('GCOB_LEC_', GcobId)
# MAGIC     when
# MAGIC       ClientType in ('Natural Person', 'Natural Person acting in a Professional Capacity (NPPC)')
# MAGIC     then
# MAGIC       concat('GCOB_NP_NPPC_', GcobId)
# MAGIC   End as LocalSystemIdentifier
# MAGIC from
# MAGIC   party_case_client_details
# MAGIC where
# MAGIC   CaseStatusName <> 'Cancelled'
# MAGIC   --and IsLatestApprovedVersionofclient = 'True'

# COMMAND ----------

# DBTITLE 1,Fetching all GCOB Client Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllPartyDetails_Row As
# MAGIC select distinct
# MAGIC   p.GlobalClientOwnerEmail as GlobalClientOwnerName,
# MAGIC   p.GlobalClientOwnerName as GCO,
# MAGIC   p.GlobalClientOwnerLocation,
# MAGIC   p.UniquePartyId,
# MAGIC   p.gcdsid,
# MAGIC   p.GcobId,
# MAGIC   p.PartyId,
# MAGIC   p.CaseId,
# MAGIC   case
# MAGIC     when p.ClientType = 'LegalEntityClient' then 'Legal Entity'
# MAGIC     when
# MAGIC       p.ClientType in (
# MAGIC         'NaturalPersonClient', 'Natural Person acting in a Professional Capacity (NPPC)'
# MAGIC       )
# MAGIC     then
# MAGIC       'Natural Person'
# MAGIC     when p.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity'
# MAGIC     when p.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person'
# MAGIC     else null
# MAGIC   end as Party_type,
# MAGIC   CONCAT('GCOB_', p.UniquePartyId) AS LocalSystemIdentifier,
# MAGIC   c.BusinessLineName as BusinessLine,
# MAGIC   lco.EmailAddress as LocalClientOwnerName,
# MAGIC   lco.Location as LocalClientOwnerLocation,
# MAGIC   pp.ProductDomain as `Product Domain`,
# MAGIC   pp.BusinessUnit as `Product Business Unit`,
# MAGIC   pp.BookingEntityLocation as BookingLocation,
# MAGIC   pp.ProductOfferingLocation as ProductLocation,
# MAGIC   ROW_NUMBER() OVER (
# MAGIC       PARTITION BY p.gcobid, p.ClientType
# MAGIC       ORDER BY
# MAGIC         CASE
# MAGIC           WHEN p.ClientLifeCycleStatus = 'Client' THEN 1
# MAGIC           ELSE 2
# MAGIC         END ASC,
# MAGIC         c.CaseCompletedDate DESC
# MAGIC     ) AS ROWNUM
# MAGIC from
# MAGIC   party_AllPartyDetails p
# MAGIC     Inner join Gcob_CaseClientDetails c on p.UniquePartyId = c.UniqueGcobId
# MAGIC       --on CASE WHEN p.ClientType = 'LegalEntityClient' THEN concat('LEC_', p.Id) WHEN p.ClientType IN (            'NaturalPersonClient', 'Natural Person acting in a Professional Capacity (NPPC)' ) THEN concat('NP_NPPC_', p.Id) end = c.SourceClient
# MAGIC     LEFT JOIN party_local_client_Owners lco
# MAGIC       on c.Sourceclient = lco.Sourceclient
# MAGIC     LEFT JOIN party_products_and_services pp
# MAGIC       on c.Sourceclient = pp.Sourceclient
# MAGIC where
# MAGIC   Status = 'Live'
# MAGIC   --and p.CaseStatusName <> 'Cancelled'
# MAGIC   --and p.IsLatestApprovedVersionofclient = 'True'

# COMMAND ----------

# DBTITLE 1,Fetching all GCOB Party Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllParty As
# MAGIC with gcob as 
# MAGIC (
# MAGIC select distinct
# MAGIC   p.GlobalClientOwnerEmail as GlobalClientOwnerName,
# MAGIC   p.GlobalClientOwnerName as GCO,
# MAGIC   p.GlobalClientOwnerLocation,
# MAGIC   p.UniquePartyId,
# MAGIC   p.gcdsid,
# MAGIC   p.CaseId,
# MAGIC   p.InternalWatchlist,
# MAGIC   case
# MAGIC     when p.ClientType = 'LegalEntityClient' then 'Legal Entity'
# MAGIC     when
# MAGIC       p.ClientType in (
# MAGIC         'NaturalPersonClient', 'Natural Person acting in a Professional Capacity (NPPC)'
# MAGIC       )
# MAGIC     then
# MAGIC       'Natural Person'
# MAGIC     when p.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity'
# MAGIC     when p.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person'
# MAGIC     else null
# MAGIC   end as Party_type,
# MAGIC   CONCAT('GCOB_', p.UniquePartyId) AS LocalSystemIdentifier,
# MAGIC   c.BusinessLineName as BusinessLine,
# MAGIC   lco.EmailAddress as LocalClientOwnerName,
# MAGIC   lco.Location as LocalClientOwnerLocation,
# MAGIC   pp.ProductDomain as `Product Domain`,
# MAGIC   pp.BusinessUnit as `Product Business Unit`,
# MAGIC   pp.BookingEntityLocation as BookingLocation,
# MAGIC   pp.ProductOfferingLocation as ProductLocation,
# MAGIC   c.SalesforceClientID_nCino,
# MAGIC   c.gcobid as ncino_gcobid,
# MAGIC   p.gcobid
# MAGIC   ,case when p.CaseStatusName is null then 'RelatedParty' else c.CaseStatusName end as CaseStatusName
# MAGIC from
# MAGIC   party_AllPartyDetails p
# MAGIC     LEFT JOIN Gcob_CaseClientDetails c on p.UniquePartyId = c.UniqueGcobId
# MAGIC       --on CASE WHEN p.ClientType = 'LegalEntityClient' THEN concat('LEC_', p.Id) WHEN p.ClientType IN (            'NaturalPersonClient', 'Natural Person acting in a Professional Capacity (NPPC)' ) THEN concat('NP_NPPC_', p.Id) end = c.SourceClientand p.ClientType in ('NaturalPersonClient', 'Natural Person acting in a Professional Capacity (NPPC)')
# MAGIC     LEFT JOIN party_local_client_Owners lco
# MAGIC       on c.Sourceclient = lco.Sourceclient
# MAGIC     LEFT JOIN party_products_and_services pp
# MAGIC       on c.Sourceclient = pp.Sourceclient
# MAGIC where
# MAGIC   Status = 'Live'
# MAGIC   --and (p.IsLatestApprovedVersionofclient = 'True' or p.IsLatestApprovedVersionofclient is null )
# MAGIC )
# MAGIC select * from gcob
# MAGIC where CaseStatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,Logic to have Unique GCOB ids
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllPartyDetails As
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Gcob_AllPartyDetails_Row
# MAGIC --where
# MAGIC --  ROWNUM <> 2

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
# MAGIC ,c.GlobalClientOwner as GCO
# MAGIC ,c.GlobalClientOwnerEmail as GlobalClientOwnerName
# MAGIC ,c.GlobalClientOwnerLocation
# MAGIC ,c.BusinessLineName as BusinessLine
# MAGIC ,lco.LocalOwnerEmailAddress as LocalClientOwnerName
# MAGIC ,lco.LocalClientOwnerCountry as LocalClientOwnerLocation
# MAGIC ,null as `Product Domain`
# MAGIC ,null as `Product Business Unit`
# MAGIC ,pp.BookingLocation as BookingLocation
# MAGIC ,pp.ProductLocation as ProductLocation
# MAGIC from Legacy2_client c
# MAGIC LEFT JOIN Legacy2_local_client_Owners lco on c.ClientId = lco.ClientId
# MAGIC     LEFT JOIN Legacy2_products_and_services pp on c.ClientId = pp.ClientId
# MAGIC where ClientTypeId in (1,2,3)
# MAGIC --and IsLatestApprovedVersionofclient = 'True'

# COMMAND ----------

# DBTITLE 1,Fetching GIC Client information
# MAGIC %sql
# MAGIC Create or replace temporary view GIC_ClientDetails As
# MAGIC With BusinessLine as
# MAGIC (
# MAGIC select distinct 
# MAGIC vng.COD_INSTITUCIONAL
# MAGIC ,vng.DES_LINHA_NEGOCIO as BusinessLine
# MAGIC From vwgic_rdl_pessoa_linha_negocio vng 
# MAGIC LEFT JOIN vwgic_rdl_pessoa_tipo_cadastro cad on cad.SEQ_PESSOA = vng.SEQ_PESSOA 
# MAGIC where vng.DTA_DESATIVACAO is null
# MAGIC and cad.SEQ_TIPO_CADASTRO = 1 
# MAGIC and SEQ_STATUS_TIPO_CADASTRO in (1,3)
# MAGIC )
# MAGIC select distinct
# MAGIC   p.COD_INSTITUCIONAL,
# MAGIC   CASE
# MAGIC     WHEN p.SEQ_TIPO_PESSOA = '2' THEN 'Natural Person'
# MAGIC     else 'Legal Entity'
# MAGIC   END AS Party_type,
# MAGIC   CONCAT('GIC_', p.COD_INSTITUCIONAL) as LocalSystemIdentifier
# MAGIC   ,'Rabobank Brazil' as GlobalClientOwnerLocation
# MAGIC   ,gco.NOM_COMPLETO as GCO
# MAGIC   ,e.DES_EMAIL as GlobalClientOwnerName
# MAGIC   ,b.BusinessLine
# MAGIC from
# MAGIC   pessoa p
# MAGIC   LEFT JOIN PESSOA_LINHA_NEGOCIO ng on p.SEQ_PESSOA = ng.SEQ_PESSOA
# MAGIC   LEFT JOIN pessoa gco  on ng.SEQ_PESSOA_RM = gco.SEQ_PESSOA
# MAGIC   LEFT JOIN pessoa_email e on gco.SEQ_PESSOA = e.SEQ_PESSOA
# MAGIC   LEFT JOIN BusinessLine b on p.COD_INSTITUCIONAL = b.COD_INSTITUCIONAL 

# COMMAND ----------

# DBTITLE 1,Fetching GCDS Client information
# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_Clients As
# MAGIC select distinct
# MAGIC   k.KeyStore_value as identifier,
# MAGIC   k.KeyStore_type,
# MAGIC   c.Party_Type,
# MAGIC   pr.Party_role,
# MAGIC   c.GCID,
# MAGIC   c.`Global_CO-email` AS GlobalClientOwnerName,
# MAGIC   c.`Global_CO-name` as GCO,
# MAGIC   c.`CO-email` as LocalClientOwnerName,
# MAGIC   `Global_CO-businessline_description`,
# MAGIC   `Global_CO-businessline_code`,
# MAGIC   c.`Global_CO-location`,
# MAGIC   c.`CO-location`,
# MAGIC   P.BOOKING_LOCATION,
# MAGIC   P.PRODUCT_LOCATION,
# MAGIC   null as `Product Domain`,
# MAGIC   P.BusinessUnit as `Product Business Unit`,
# MAGIC   c.`Global_CO-location_code`
# MAGIC from
# MAGIC   client_KeyStoreKey k
# MAGIC     inner join client_Client c
# MAGIC       on c.GCID = k.GCID
# MAGIC     left join client_PartyRole pr
# MAGIC       on k.GCID = pr.GCID
# MAGIC     left join client_Products p
# MAGIC       on p.GCID = c.GCID
# MAGIC --where
# MAGIC --  KeyStore_type in ('GCOBID', 'NCINOID', 'GIC', 'CB RANZ')
# MAGIC   --and k.KeyStore_value not in (select distinct gcobid from Gcob_AllPartyDetails_Row where ROWNUM = 2 and gcdsid is not null )

# COMMAND ----------

# DBTITLE 1,Obtaining all Coverage details for Parties present in GCDS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Coverage_GCDS AS
# MAGIC WITH BaseCoverage AS (
# MAGIC SELECT
# MAGIC   CASE WHEN gcds.identifier = gcob.GcobId AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID'
# MAGIC        THEN gcob.LocalSystemIdentifier
# MAGIC        --WHEN c.SalesforceClientID_nCino = gcds.identifier AND gcds.KeyStore_type = 'NCINOID' THEN c.LocalSystemIdentifier
# MAGIC        WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person'
# MAGIC        THEN ncino.LocalSystemIdentifier
# MAGIC        WHEN gcds.identifier = gic.COD_INSTITUCIONAL AND gcds.KeyStore_type = 'GIC'
# MAGIC        THEN gic.LocalSystemIdentifier
# MAGIC        WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC        THEN l2.LocalSystemIdentifier
# MAGIC        else CONCAT('GCDS_', gcds.GCID)
# MAGIC   END AS LocalSystemIdentifier,
# MAGIC   CASE WHEN gcds.identifier = gcob.GcobId AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID'
# MAGIC        THEN 'GCOB'
# MAGIC        --WHEN c.SalesforceClientID_nCino = gcds.identifier AND gcds.KeyStore_type = 'NCINOID' THEN 'GCOB'
# MAGIC        WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person'
# MAGIC        THEN 'GCOB'
# MAGIC        WHEN gcds.identifier = gic.COD_INSTITUCIONAL AND gcds.KeyStore_type = 'GIC'
# MAGIC        THEN 'GIC'
# MAGIC        WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC        THEN 'Legacy2'
# MAGIC        Else 'GCDS'
# MAGIC   END AS Application,
# MAGIC   --COALESCE(gcob.GlobalClientOwnerName, ncino.GlobalClientOwnerName, gic.GlobalClientOwner, gcds.GlobalClientOwnerName) AS GlobalClientOwnerName,
# MAGIC     array_distinct(
# MAGIC       filter(
# MAGIC         array(gcob.LocalClientOwnerName, ncino.LocalClientOwnerName, gic.GlobalClientOwnerName, l2.LocalClientOwnerName, gcds.LocalClientOwnerName),
# MAGIC         x -> x IS NOT NULL
# MAGIC       )
# MAGIC     ) AS LocalClientOwnerNameArray,
# MAGIC   --COALESCE(gcds.`Global_CO-businessline_description`, gcob.BusinessLine, ncino.BusinessLine) AS BusinessLine,
# MAGIC   --COALESCE(gcob.GlobalClientOwnerLocation, ncino.GlobalClientOwnerLocation, gic.GlobalClientOwnerLocation, gcds.`Global_CO-location`) AS GlobalClientOwnerLocation,
# MAGIC     array_distinct(
# MAGIC       filter(
# MAGIC         array(gcob.LocalClientOwnerLocation, ncino.LocalClientOwnerLocation, gic.GlobalClientOwnerLocation,l2.LocalClientOwnerLocation, gcds.`CO-location`),
# MAGIC         x -> x IS NOT NULL
# MAGIC       )
# MAGIC     ) AS LocalClientOwnerLocationArray,
# MAGIC     array_distinct(
# MAGIC       filter(
# MAGIC         array(gcds.BOOKING_LOCATION, gcob.BookingLocation, ncino.BookingLocation, l2.BookingLocation),
# MAGIC         x -> x IS NOT NULL
# MAGIC       )
# MAGIC     ) AS BookingLocationArray,
# MAGIC     array_distinct(
# MAGIC       filter(
# MAGIC         array(gcds.PRODUCT_LOCATION, gcob.ProductLocation, ncino.ProductLocation, l2.ProductLocation),
# MAGIC         x -> x IS NOT NULL
# MAGIC       )
# MAGIC     ) AS ProductLocationArray,
# MAGIC     COALESCE(
# MAGIC       gcds.`Product Business Unit`, gcob.`Product Business Unit`, ncino.`Product Business Unit`
# MAGIC     ) AS `Product Business Unit`,
# MAGIC     COALESCE(gcob.`Product Domain`, ncino.`Product Domain`) AS `Product Domain`
# MAGIC     --,gcds.GCID
# MAGIC     ,COALESCE(gcob.GCO, ncino.GCO, gic.GCO, l2.GCO, gcds.GCO) AS GCO
# MAGIC     ,COALESCE(gcob.GlobalClientOwnerName, ncino.GlobalClientOwnerName, gic.GlobalClientOwnerName, l2.GlobalClientOwnerName, gcds.GlobalClientOwnerName) AS GlobalClientOwnerName
# MAGIC     ,COALESCE(gcds.`Global_CO-businessline_description`, gcob.BusinessLine, ncino.BusinessLine,gic.BusinessLine) AS BusinessLine
# MAGIC     ,COALESCE(gcob.GlobalClientOwnerLocation, ncino.GlobalClientOwnerLocation, gic.GlobalClientOwnerLocation, l2.GlobalClientOwnerLocation, gcds.`Global_CO-location`) AS GlobalClientOwnerLocation,
# MAGIC     gcds.`Global_CO-location_code` as Location_code
# MAGIC   FROM
# MAGIC     GCDS_cLIENTS gcds
# MAGIC       LEFT JOIN Gcob_AllPartyDetails gcob
# MAGIC         ON gcds.identifier = gcob.GcobId
# MAGIC         --AND gcds.Party_type = gcob.Party_type
# MAGIC         --AND gcds.GCID = gcob.GCDSID
# MAGIC         AND gcob.Party_type = 'Legal Entity'
# MAGIC         --AND gcds.party_role = 'Customer'
# MAGIC         AND gcds.KeyStore_type = 'GCOBID'
# MAGIC       LEFT JOIN Gcob_CaseClientDetails c
# MAGIC         ON c.SalesforceClientID_nCino = gcds.identifier
# MAGIC         --AND c.Party_type = gcds.Party_type
# MAGIC         AND gcds.KeyStore_type = 'NCINOID'
# MAGIC         --AND gcds.party_role = 'Customer'
# MAGIC       LEFT JOIN Gcob_AllPartyDetails ncino
# MAGIC         ON c.ClientId = ncino.PartyId
# MAGIC         AND ncino.Party_type = 'Natural Person'
# MAGIC         --AND gcds.party_role = 'Customer'
# MAGIC       LEFT JOIN GIC_ClientDetails gic
# MAGIC         ON gcds.identifier = gic.COD_INSTITUCIONAL
# MAGIC         AND gcds.KeyStore_type = 'GIC'
# MAGIC         --AND gcds.party_role = 'Customer'
# MAGIC       LEFT JOIN Legacy2 l2 on l2.SalesforceClientID_nCino = gcds.identifier 
# MAGIC         and l2.Party_type = gcds.Party_type
# MAGIC         and gcds.KeyStore_type = 'NCINOID'
# MAGIC         --and gcds.party_role = 'Customer' 
# MAGIC )
# MAGIC SELECT distinct
# MAGIC   LocalSystemIdentifier,
# MAGIC   Application,
# MAGIC   GCO,
# MAGIC   GlobalClientOwnerName,
# MAGIC   explode_outer(LocalClientOwnerNameArray) AS LocalClientOwnerName,
# MAGIC   BusinessLine,
# MAGIC   GlobalClientOwnerLocation,
# MAGIC   explode_outer(LocalClientOwnerLocationArray) AS LocalClientOwnerLocation,
# MAGIC   explode_outer(BookingLocationArray) AS `Booking Location`,
# MAGIC   explode_outer(ProductLocationArray) AS `Product Location`,
# MAGIC   `Product Business Unit`,
# MAGIC   `Product Domain`,
# MAGIC   Location_code
# MAGIC   --,GCID
# MAGIC FROM
# MAGIC   BaseCoverage

# COMMAND ----------

# DBTITLE 1,GCOB Coverage data for Non GCDS Parties
# MAGIC %sql
# MAGIC Create or replace temporary view GCOB_Coverage_NonGCDS As
# MAGIC with GCOB as (
# MAGIC   select distinct
# MAGIC     t1.LocalSystemIdentifier,
# MAGIC     'GCOB' as Application,
# MAGIC     t1.Party_Type,
# MAGIC     t1.CaseId,
# MAGIC     t1.InternalWatchlist,
# MAGIC     t1.GCO,
# MAGIC     t1.GlobalClientOwnerName,
# MAGIC     t1.LocalClientOwnerName,
# MAGIC     t1.BusinessLine,
# MAGIC     t1.GlobalClientOwnerLocation,
# MAGIC     t1.LocalClientOwnerLocation,
# MAGIC     t1.BookingLocation,
# MAGIC     t1.ProductLocation,
# MAGIC     t1.`Product Business Unit`,
# MAGIC     t1.`Product Domain`
# MAGIC   from
# MAGIC     Gcob_AllParty t1
# MAGIC   where
# MAGIC     t1.Party_Type <> 'Natural Person'
# MAGIC   union
# MAGIC   select distinct
# MAGIC     t1.LocalSystemIdentifier,
# MAGIC     'GCOB' as Application,
# MAGIC     t1.Party_Type,
# MAGIC     t1.CaseId,
# MAGIC     t1.InternalWatchlist,
# MAGIC     t1.GCO,
# MAGIC     t1.GlobalClientOwnerName,
# MAGIC     t1.LocalClientOwnerName,
# MAGIC     t1.BusinessLine,
# MAGIC     t1.GlobalClientOwnerLocation,
# MAGIC     t1.LocalClientOwnerLocation,
# MAGIC     t1.BookingLocation,
# MAGIC     t1.ProductLocation,
# MAGIC     t1.`Product Business Unit`,
# MAGIC     t1.`Product Domain`
# MAGIC   from
# MAGIC     Gcob_AllParty t1
# MAGIC   where
# MAGIC     t1.Party_Type = 'Natural Person'
# MAGIC )
# MAGIC select distinct
# MAGIC   LocalSystemIdentifier,
# MAGIC   Application,
# MAGIC   GCO,
# MAGIC   GlobalClientOwnerName,
# MAGIC   LocalClientOwnerName,
# MAGIC   BusinessLine,
# MAGIC   GlobalClientOwnerLocation,
# MAGIC   LocalClientOwnerLocation,
# MAGIC   BookingLocation,
# MAGIC   ProductLocation,
# MAGIC   `Product Business Unit`,
# MAGIC   `Product Domain`,
# MAGIC   ROW_NUMBER() OVER (
# MAGIC       PARTITION BY LocalSystemIdentifier, Party_Type
# MAGIC       ORDER BY caseid desc, InternalWatchlist desc
# MAGIC     ) AS ROWNUM
# MAGIC from
# MAGIC   GCOB

# COMMAND ----------

# DBTITLE 1,Coverage data of Sources not in GCDS
# MAGIC %sql
# MAGIC Create or replace temporary view Coverage_NonGCDS As
# MAGIC with NonGCDS as (
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     GCOB_Coverage_NonGCDS
# MAGIC   union
# MAGIC   select distinct
# MAGIC     t1.LocalSystemIdentifier,
# MAGIC     'GIC' as Application,
# MAGIC     t1.GCO,
# MAGIC     t1.GlobalClientOwnerName as GlobalClientOwnerName,
# MAGIC     t1.GlobalClientOwnerName as LocalClientOwnerName,
# MAGIC     null as BusinessLine,
# MAGIC     t1.GlobalClientOwnerLocation as GlobalClientOwnerLocation,
# MAGIC     t1.GlobalClientOwnerLocation as LocalClientOwnerLocation,
# MAGIC     null as BookingLocation,
# MAGIC     null as ProductLocation,
# MAGIC     null as Product_Business_Unit,
# MAGIC     null as Product_Domain,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         PARTITION BY t1.COD_INSTITUCIONAL, t1.Party_Type
# MAGIC         ORDER BY t1.COD_INSTITUCIONAL DESC
# MAGIC       ) AS ROWNUM
# MAGIC   From
# MAGIC     GIC_ClientDetails t1
# MAGIC
# MAGIC union
# MAGIC   select distinct
# MAGIC     t1.LocalSystemIdentifier,
# MAGIC     'Legacy2' as Application,
# MAGIC     t1.GCO,
# MAGIC     t1.GlobalClientOwnerName as GlobalClientOwnerName,
# MAGIC     t1.LocalClientOwnerName as LocalClientOwnerName,
# MAGIC     t1.BusinessLine as BusinessLine,
# MAGIC     t1.GlobalClientOwnerLocation as GlobalClientOwnerLocation,
# MAGIC     t1.LocalClientOwnerLocation as LocalClientOwnerLocation,
# MAGIC     t1.BookingLocation as BookingLocation,
# MAGIC     t1.ProductLocation as ProductLocation,
# MAGIC     null as Product_Business_Unit,
# MAGIC     null as Product_Domain,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY t1.Legacy2_Identifier ORDER BY t1.clientid DESC) AS ROWNUM
# MAGIC   From
# MAGIC     Legacy2 t1
# MAGIC )
# MAGIC select distinct
# MAGIC   LocalSystemIdentifier,
# MAGIC   Application,
# MAGIC   GCO,
# MAGIC   GlobalClientOwnerName,
# MAGIC   LocalClientOwnerName,
# MAGIC   BusinessLine,
# MAGIC   GlobalClientOwnerLocation,
# MAGIC   LocalClientOwnerLocation,
# MAGIC   BookingLocation,
# MAGIC   ProductLocation,
# MAGIC   `Product Business Unit`,
# MAGIC   `Product Domain`,
# MAGIC   null as Location_code
# MAGIC from
# MAGIC   NonGCDS as t1
# MAGIC where
# MAGIC   t1.ROWNUM = 1

# COMMAND ----------

# DBTITLE 1,Comparing GCDS and Non GCDS data
# MAGIC %sql
# MAGIC Create or replace temporary view NonGCDS_UniqueCoverage As
# MAGIC select * 
# MAGIC from Coverage_NonGCDS a
# MAGIC left anti join Coverage_GCDS b
# MAGIC on a.LocalSystemIdentifier = b.LocalSystemIdentifier
# MAGIC

# COMMAND ----------

# DBTITLE 1,Combining GCDS and Non GCDS details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Coverage AS
# MAGIC SELECT * FROM Coverage_GCDS WHERE Application IS NOT NULL
# MAGIC UNION
# MAGIC --SELECT * FROM Coverage_NonGCDS WHERE Localsystemidentifier NOT IN (SELECT Localsystemidentifier FROM Coverage_GCDS );
# MAGIC select * from NonGCDS_UniqueCoverage

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_Coverage = spark.table("Coverage")

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_f_party_Coverage = add_party_identifier(df_party_Coverage, df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 1,Create table from dataframe
df_f_party_Coverage.createOrReplaceTempView('Party_Coverage')

# COMMAND ----------

# DBTITLE 1,Attribute based on system preference
# MAGIC %sql
# MAGIC Create or replace temporary view SystemPrefAttribute As
# MAGIC WITH ranked_data AS (
# MAGIC SELECT distinct
# MAGIC PartyIdentifier,
# MAGIC GCO,
# MAGIC ROW_NUMBER() OVER (PARTITION BY PartyIdentifier ORDER BY
# MAGIC   CASE
# MAGIC   WHEN GCO IS NOT NULL AND Application = 'GCOB' THEN 1
# MAGIC   WHEN GCO IS NOT NULL AND Application = 'Legacy2' THEN 2
# MAGIC   WHEN GCO IS NOT NULL AND Application = 'GIC' THEN 3
# MAGIC   WHEN GCO IS NOT NULL AND Application = 'GCDS' THEN 4
# MAGIC   ELSE 10 END 
# MAGIC ) AS rn_GCO,
# MAGIC GlobalClientOwnerLocation,
# MAGIC ROW_NUMBER() OVER (PARTITION BY PartyIdentifier ORDER BY
# MAGIC   CASE
# MAGIC   WHEN GlobalClientOwnerLocation IS NOT NULL AND Application = 'GCOB' THEN 1
# MAGIC   WHEN GlobalClientOwnerLocation IS NOT NULL AND Application = 'Legacy2' THEN 2
# MAGIC   WHEN GlobalClientOwnerLocation IS NOT NULL AND Application = 'GIC' THEN 3
# MAGIC   WHEN GlobalClientOwnerLocation IS NOT NULL AND Application = 'GCDS' THEN 4
# MAGIC   ELSE 10 END
# MAGIC ) AS rn_GlobalClientOwnerLocation,
# MAGIC BusinessLine,
# MAGIC ROW_NUMBER() OVER (PARTITION BY PartyIdentifier ORDER BY
# MAGIC   CASE
# MAGIC   WHEN BusinessLine IS NOT NULL AND Application = 'GCOB' THEN 1
# MAGIC   WHEN BusinessLine IS NOT NULL AND Application = 'Legacy2' THEN 2
# MAGIC   WHEN BusinessLine IS NOT NULL AND Application = 'GIC' THEN 3
# MAGIC   WHEN BusinessLine IS NOT NULL AND Application = 'GCDS' THEN 4
# MAGIC   ELSE 10 END
# MAGIC ) AS rn_BusinessLine,
# MAGIC Application
# MAGIC FROM Party_Coverage
# MAGIC )
# MAGIC
# MAGIC select distinct 
# MAGIC PartyIdentifier
# MAGIC ,MAX(CASE WHEN rn_GCO = 1 THEN GCO END) AS GCO
# MAGIC ,MAX(CASE WHEN rn_GlobalClientOwnerLocation = 1 THEN GlobalClientOwnerLocation END) AS GlobalClientOwnerLocation
# MAGIC ,MAX(CASE WHEN rn_BusinessLine = 1 THEN BusinessLine END) AS BusinessLine
# MAGIC from ranked_data
# MAGIC where PartyIdentifier is not null
# MAGIC group by PartyIdentifier

# COMMAND ----------

# DBTITLE 1,Get email for GCO
# MAGIC %sql
# MAGIC Create or replace temporary view GCO_Email As
# MAGIC select distinct
# MAGIC c.PartyIdentifier,
# MAGIC c.Application,
# MAGIC a.GCO,
# MAGIC c.GlobalClientOwnerName
# MAGIC from Party_Coverage c
# MAGIC Inner join SystemPrefAttribute a on c.PartyIdentifier = a.PartyIdentifier and c.GCO = a.GCO
# MAGIC where c.GlobalClientOwnerName is not null

# COMMAND ----------

# DBTITLE 1,Combine all data with correct attribute
# MAGIC %sql
# MAGIC Create or replace temporary view P_Coverage As
# MAGIC
# MAGIC with all_countries as (
# MAGIC   select distinct split(Name, ' \\(the\\)')[0] as Name, ISOCode from party_allcountries
# MAGIC order by Name
# MAGIC ),
# MAGIC GCDS_countries as (
# MAGIC   select distinct `Global_CO-location` AS GCDS_Location,`Global_CO-location_code` AS LocationCode,i.CountryISO as LocationISOCode
# MAGIC from client_Client cc
# MAGIC left join ISOCode_static_table i on i.LocationCode = cc.`Global_CO-location_code`
# MAGIC where `Global_CO-location_code` is not null
# MAGIC qualify row_number() over(partition by `Global_CO-location` order by `Global_CO-location_code` desc) = 1
# MAGIC )
# MAGIC select distinct
# MAGIC c.PartyIdentifier,
# MAGIC c.Application,
# MAGIC e.GlobalClientOwnerName,
# MAGIC c.LocalClientOwnerName,
# MAGIC a.BusinessLine,
# MAGIC a.GlobalClientOwnerLocation,
# MAGIC coalesce(ac1.ISOCode, ctr1.LocationISOCode) AS GCO_Location_ISOCode,
# MAGIC c.LocalClientOwnerLocation,
# MAGIC coalesce(ac2.ISOCode, ctr2.LocationISOCode) AS LCO_Location_ISOCode,
# MAGIC c.`Booking Location`,
# MAGIC ac3.ISOCode AS Booking_Location_ISOCode,
# MAGIC c.`Product Location`,
# MAGIC ac4.ISOCode AS Product_Location_ISOCode,
# MAGIC c.`Product Business Unit`,
# MAGIC c.`Product Domain`,
# MAGIC c.Location_code as LocationCode
# MAGIC from Party_Coverage c
# MAGIC left join GCO_Email e on c.PartyIdentifier = e.PartyIdentifier
# MAGIC left join SystemPrefAttribute a on c.PartyIdentifier = a.PartyIdentifier
# MAGIC left join all_countries ac1 on a.GlobalClientOwnerLocation = ac1.Name
# MAGIC left join all_countries ac2 on c.LocalClientOwnerLocation = ac2.Name
# MAGIC left join all_countries ac3 on c.`Booking Location` = ac3.Name
# MAGIC left join all_countries ac4 on c.`Product Location` = ac4.Name
# MAGIC left join GCDS_countries ctr1 on a.GlobalClientOwnerLocation = ctr1.GCDS_Location
# MAGIC left join GCDS_countries ctr2 on c.LocalClientOwnerLocation = ctr2.GCDS_Location
# MAGIC --where c.PartyIdentifier = 'GCDS_2723942'

# COMMAND ----------

# DBTITLE 1,Logic to get Involved Location
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW InvolvedLocation AS
# MAGIC select distinct
# MAGIC   PartyIdentifier,
# MAGIC   Application,
# MAGIC   'Location' as CoverageType,
# MAGIC   'Involved Location' as CoverageTypeDescription,
# MAGIC   `Product Location` as CoverageValue,
# MAGIC   Product_Location_ISOCode AS LocationCountryCode,
# MAGIC   null as LocationCode
# MAGIC from
# MAGIC   P_Coverage
# MAGIC Union
# MAGIC select distinct
# MAGIC   PartyIdentifier,
# MAGIC   Application,
# MAGIC   'Location' as CoverageType,
# MAGIC   'Involved Location' as CoverageTypeDescription,
# MAGIC   `Booking Location` as CoverageValue,
# MAGIC   Booking_Location_ISOCode AS LocationCountryCode,
# MAGIC   null as LocationCode
# MAGIC from
# MAGIC   P_Coverage

# COMMAND ----------

# DBTITLE 1,Logic for Coverage dataobject
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Coverages AS
# MAGIC select distinct
# MAGIC   PartyIdentifier,
# MAGIC   Application,
# MAGIC   'GCO' as CoverageType,
# MAGIC   'Global Client Owner' as CoverageTypeDescription,
# MAGIC   GlobalClientOwnerName as CoverageValue,
# MAGIC   NULL AS LocationCountryCode,
# MAGIC   null as LocationCode
# MAGIC from
# MAGIC   P_Coverage
# MAGIC UNION
# MAGIC select distinct
# MAGIC   PartyIdentifier,
# MAGIC   Application,
# MAGIC   'LCO' as CoverageType,
# MAGIC   'Local Client Owner' as CoverageTypeDescription,
# MAGIC   LocalClientOwnerName as CoverageValue,
# MAGIC   NULL AS LocationCountryCode,
# MAGIC   null as LocationCode
# MAGIC from
# MAGIC   P_Coverage
# MAGIC UNION
# MAGIC select distinct
# MAGIC   PartyIdentifier,
# MAGIC   Application,
# MAGIC   'Business Line' as CoverageType,
# MAGIC   '' as CoverageTypeDescription,
# MAGIC   BusinessLine as CoverageValue,
# MAGIC   NULL AS LocationCountryCode,
# MAGIC   null as LocationCode
# MAGIC from
# MAGIC   P_Coverage
# MAGIC UNION
# MAGIC select distinct
# MAGIC   PartyIdentifier,
# MAGIC   Application,
# MAGIC   'Location' as CoverageType,
# MAGIC   'GCO Location' as CoverageTypeDescription,
# MAGIC   GlobalClientOwnerLocation as CoverageValue,
# MAGIC   GCO_Location_ISOCode AS LocationCountryCode,
# MAGIC   LocationCode
# MAGIC from
# MAGIC   P_Coverage
# MAGIC UNION
# MAGIC select distinct
# MAGIC   PartyIdentifier,
# MAGIC   Application,
# MAGIC   'Location' as CoverageType,
# MAGIC   'LCO Location' as CoverageTypeDescription,
# MAGIC   LocalClientOwnerLocation as CoverageValue,
# MAGIC   LCO_Location_ISOCode AS LocationCountryCode,
# MAGIC   LocationCode
# MAGIC from
# MAGIC   P_Coverage
# MAGIC UNION
# MAGIC select distinct
# MAGIC   PartyIdentifier,
# MAGIC   Application,
# MAGIC   'Product Business Unit' as CoverageType,
# MAGIC   '' as CoverageTypeDescription,
# MAGIC   `Product Business Unit` as CoverageValue,
# MAGIC   NULL AS LocationCountryCode,
# MAGIC   null as LocationCode
# MAGIC from
# MAGIC   P_Coverage
# MAGIC Union
# MAGIC select distinct
# MAGIC   PartyIdentifier,
# MAGIC   Application,
# MAGIC   'Product Domain' as CoverageType,
# MAGIC   '' as CoverageTypeDescription,
# MAGIC   `Product Domain` as CoverageValue,
# MAGIC   NULL AS LocationCountryCode,
# MAGIC   null as LocationCode
# MAGIC from
# MAGIC   P_Coverage
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   PartyIdentifier,
# MAGIC   Application,
# MAGIC   CoverageType,
# MAGIC   CoverageTypeDescription,
# MAGIC   CoverageValue,
# MAGIC   LocationCountryCode,
# MAGIC   LocationCode
# MAGIC FROM
# MAGIC   InvolvedLocation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Coverage_Final AS
# MAGIC select c.*,s.RegionName
# MAGIC from Coverages c
# MAGIC left join RegionISO_static_table s on c.LocationCountryCode = s.LocationISOCode

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CREATING A FINAL DATAFRAME BY COMBINING ALL THE INFORMATION FROM DIFFERENT SOURCESYSTEMS AND LOADING THE FINAL DATAOBJECT TO SA RADAR STORAGE ACCOUNT

# COMMAND ----------

df_final_party_Coverage = spark.table("Coverage_Final")

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
    save_to_saradar_storage_account(df_final_party_Coverage, party_Coverage_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_final_party_Coverage, party_Coverage_dataobject, radar_datamodel_version_number, environment)
