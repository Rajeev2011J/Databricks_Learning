# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To provide all the Latest approved Clients and its related Parties involved in a particular LOB
# MAGIC
# MAGIC #### Authors
# MAGIC - Mahalakshmi.Vengateswaran@rabobank.com
# MAGIC
# MAGIC #### Flow of Logic
# MAGIC - Reading GCOB dataobjects from GDP
# MAGIC - Apply Party Selection Criteria
# MAGIC - Split the Parties based on their LOB 
# MAGIC - Deduplicate parties to have only one line item per party
# MAGIC - Loading all the different LOB files to GNS Storage account
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |12-Nov-2025 |14100005 |Adding 6 TradeNames, 4 map with Alias same column		

# COMMAND ----------

# MAGIC %md
# MAGIC #####IMPORTING THE LIBRARIES AND READING GCOB DATAOBJECTS FROM GDP

# COMMAND ----------

# DBTITLE 1,Import required packages
import os
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta, timezone
from pyspark.sql.functions import col
import requests
import json


# COMMAND ----------

def authenticate_storage_account(write_storage):
    service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{application_id}")
    # This method is for Configuring Spark to access SARADAR Storage account using OAuth authentication
    spark.conf.set("fs.azure.account.auth.type."+write_storage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+write_storage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+write_storage+".dfs.core.windows.net", ""+application_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+write_storage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+write_storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")

    print('Connection to ' + write_storage +' storage account is successful.')

    return

# COMMAND ----------

# DBTITLE 1,Flag based on Env.
if environment == 'dev':
  TradeFlag = 'True'
elif environment == 'preprd':
  TradeFlag = 'True'
elif environment == 'prod':
  TradeFlag = 'False'

# COMMAND ----------

# DBTITLE 1,Deriving the date parameters
date_parameter = datetime.today().strftime('%Y%m%d')
print(date_parameter)
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'

# COMMAND ----------

load_dts = 'LOAD_DT=20251028'

# COMMAND ----------

# DBTITLE 1,Reading environment variables
GDP_EU_Storage_Account = os.environ['GDP_STORAGE_NAME']
GNS_STORAGE_ACCOUNT= os.environ['GNS_STORAGE_ACCOUNT']
GNS_CONTAINER= os.environ['GNS_CONTAINER']
environment=os.environ['ENV']
SARadarStorage = f'saradar{environment}'
application_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']


# COMMAND ----------

# DBTITLE 1,Authenticating Storage accounts
authenticate_storage_account(SARadarStorage)
authenticate_storage_account(GDP_EU_Storage_Account)

# COMMAND ----------

# DBTITLE 1,Reading GCOB GDP Defined DataObjects
# List of dataobjects to load from from GDP
from pyspark.sql.functions import * 

load_df = [
'party_client_structure_GUI',
'party_case_client_details',
'party_AllPartyDetails',
'party_products_and_sevices',
'party_Alias',
'Party_AllParty_LocationCoverage',
'party_trade_name'
]

# Create TempView for each loading table
for Object in load_df:
    #Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')
    final_path = f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService/{Object}/102/data/{load_dts}/*.parquet'
    spark.read.parquet(final_path).createOrReplaceTempView(Object)
    

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####PARTY SELECTION TRANSFORMATION FOR GNS

# COMMAND ----------

# DBTITLE 1,Deriving required columns for Lead/Involved Location
# MAGIC %sql
# MAGIC create or replace temporary view lead_location as
# MAGIC select UniquePartyId, collect_set(Location) as lead_loc from Party_AllParty_LocationCoverage where LeadOrInvolved='Lead' group by UniquePartyId;
# MAGIC
# MAGIC create or replace temporary view involved_location as
# MAGIC select UniquePartyId, collect_set(Location) as involved_loc from Party_AllParty_LocationCoverage where LeadOrInvolved='Involved' group by UniquePartyId;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Derving Aliases
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW AliasDetails AS
# MAGIC select PartyId, Id, ResultType, Alias_1, Alias_2, Alias_3, Alias_4 from (
# MAGIC select DISTINCT
# MAGIC alias.PartyId,
# MAGIC alias.Id,
# MAGIC alias.ResultType,
# MAGIC alias.Alias,
# MAGIC Row_Number() over (partition by alias.PartyId,alias.Id,alias.ResultType order by alias.Alias) as RowN
# MAGIC FROM party_Alias alias
# MAGIC )
# MAGIC PIVOT(
# MAGIC     max(Alias) for RowN in (1 Alias_1,2 Alias_2,3 Alias_3,4 Alias_4)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Derving TradeName
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW TradeName AS
# MAGIC SELECT PartyId
# MAGIC     , Id
# MAGIC     , ResultType
# MAGIC     , TradeName_1
# MAGIC     , TradeName_2
# MAGIC     , TradeName_3
# MAGIC     , TradeName_4
# MAGIC     , TradeName_5
# MAGIC     , TradeName_6
# MAGIC FROM (
# MAGIC         SELECT DISTINCT
# MAGIC             TradeName.PartyId
# MAGIC             ,TradeName.Id
# MAGIC             ,TradeName.ResultType
# MAGIC             ,TradeName.ClientTradeName
# MAGIC             ,Row_Number() over (PARTITION BY TradeName.PartyId,TradeName.Id,TradeName.ResultType
# MAGIC                                 ORDER BY TradeName.ClientTradeName
# MAGIC                                 ) as RowN
# MAGIC         FROM party_trade_name AS TradeName
# MAGIC )
# MAGIC PIVOT(
# MAGIC     max(ClientTradeName) for RowN in (1 TradeName_1,2 TradeName_2,3 TradeName_3,4 TradeName_4,5 TradeName_5,6 TradeName_6)
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Combine Alias & TradeName based on Flag
if TradeFlag == 'True':
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW Alias AS
        SELECT distinct PartyId, Id, ResultType, Alias_1, Alias_2, Alias_3, Alias_4, null as TradeName_5, null as TradeName_6 FROM AliasDetails
        UNION ALL
        SELECT distinct PartyId, Id, ResultType, TradeName_1, TradeName_2, TradeName_3, TradeName_4, TradeName_5, TradeName_6 FROM TradeName
    """)
else:
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW Alias AS
        SELECT distinct PartyId, Id, ResultType, Alias_1, Alias_2, Alias_3, Alias_4, '' as TradeName_5, '' as TradeName_6  FROM AliasDetails
    """)

# COMMAND ----------

# DBTITLE 1,Deriving Product and Booking Location
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Product_Booking_Location AS
# MAGIC select distinct SourceClient,ClientId,gcobid,ClientType,ClientLifeCycleName,IsLatestApprovedVersionOfClient,ProductOfferingLocation,BookingEntityLocation,Case when ClientType = 'LegalEntityClient' then concat('LEC_',GcobId)
# MAGIC       when ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then concat('NP_NPPC_',GcobId)
# MAGIC End as UniquePartyId  from party_products_and_sevices
# MAGIC

# COMMAND ----------

# DBTITLE 1,Logic to get all Active main Clients
# MAGIC %sql
# MAGIC CREATE OR REPLaCE TEMP VIEW Active_MainClients as
# MAGIC Select *,Case when ClientType = 'LegalEntityClient' then concat('LEC_',Id)
# MAGIC       when ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then concat('NP_NPPC_',Id)
# MAGIC End as SourceClient from party_AllPartyDetails papd where papd.CaseStatusName = 'Completed' AND papd.ClientLifeCycleStatus='Client' and  papd.IsLatestApprovedVersionOfClient=True 

# COMMAND ----------

# DBTITLE 1,Logic to get all the details of Active Main Clients
# MAGIC %sql
# MAGIC --Active Main Clients
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GNS_Child AS
# MAGIC SELECT DISTINCT 
# MAGIC CASE WHEN papd.GCDSID IS NULL AND papd.ClientType='LegalEntityClient' THEN concat('LE_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType IN ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedLegalEntity' THEN concat('RLEP_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedNaturalPerson' THEN concat('RNPP_',papd.GcobId)
# MAGIC ELSE papd.GCDSID END AS GcobId,
# MAGIC papd.UniquePartyId,
# MAGIC CS.ClientStructureSnapshotId,
# MAGIC papd.FullLegalName,
# MAGIC CASE WHEN papd.ClientType='LegalEntityClient' THEN 'E'
# MAGIC WHEN papd.ClientType='NaturalPersonClient' THEN 'I'
# MAGIC WHEN papd.ClientType='Natural Person acting in a Professional Capacity (NPPC)' THEN 'I' 
# MAGIC WHEN papd.ClientType='RelatedLegalEntity' THEN 'E'
# MAGIC WHEN papd.ClientType='RelatedNaturalPerson' THEN 'I'
# MAGIC END AS EntityType,
# MAGIC papd.IncorporationNumber,
# MAGIC to_date(trim(papd.IncorporationDate),'dd-MM-yyyy') AS IncorporationDate,
# MAGIC papd.PEPStatus,
# MAGIC regexp_replace(trim(papd.RegisteredNumber), '[\n\r\t]', ' ') AS RegisteredNumber,
# MAGIC regexp_replace(trim(papd.RegisteredStreet), '[\n\r\t]', ' ') AS RegisteredStreet,
# MAGIC regexp_replace(trim(papd.RegisteredCity), '[\n\r\t]', ' ') AS RegisteredCity,
# MAGIC regexp_replace(trim(papd.RegisteredRegion), '[\n\r\t]', ' ') AS RegisteredRegion,
# MAGIC regexp_replace(trim(papd.RegisteredPostalCode), '[\n\r\t]', ' ') AS RegisteredPostalCode,
# MAGIC regexp_replace(trim(papd.RegisteredCountryIsoCode), '[\n\r\t]', ' ') AS RegisteredCountryIsoCode,
# MAGIC regexp_replace(trim(papd.OperatingNumber), '[\n\r\t]', ' ') AS OperatingNumber,
# MAGIC regexp_replace(trim(papd.OperatingStreet), '[\n\r\t]', ' ') AS OperatingStreet,
# MAGIC regexp_replace(trim(papd.OperatingCity), '[\n\r\t]', ' ') AS OperatingCity,
# MAGIC regexp_replace(trim(papd.OperatingRegion), '[\n\r\t]', ' ') AS OperatingRegion,
# MAGIC regexp_replace(trim(papd.OperatingPostalCode), '[\n\r\t]', ' ') AS OperatingPostalCode,
# MAGIC regexp_replace(trim(papd.OperatingCountryIsoCode), '[\n\r\t]', ' ') AS OperatingCountryIsoCode,
# MAGIC papd.GlobalClientOwnerLocation,
# MAGIC papd.DateOfBirth,
# MAGIC alias.Alias_1,
# MAGIC alias.Alias_2,
# MAGIC alias.Alias_3,
# MAGIC alias.Alias_4,
# MAGIC pbl.ProductOfferingLocation,
# MAGIC pbl.BookingEntityLocation,
# MAGIC cs.FIHubIndicator,
# MAGIC null as ParentEntityId,
# MAGIC null as UniqueParentPartyId,
# MAGIC papd.PartyId,
# MAGIC array_join(lloc.lead_loc, ', ') AS LeadLocation,
# MAGIC array_join(iloc.involved_loc, ', ') AS InvoledLocation,
# MAGIC alias.TradeName_5, 
# MAGIC alias.TradeName_6
# MAGIC FROM Active_MainClients papd 
# MAGIC LEFT OUTER JOIN party_client_structure_GUI cs ON papd.Sourceclient=cs.Sourceclient
# MAGIC LEFT OUTER JOIN Alias alias ON papd.PartyId = alias.PartyId AND papd.Id = alias.Id AND papd.ClientType = alias.ResultType
# MAGIC LEFT OUTER JOIN Product_Booking_Location pbl ON pbl.SourceClient = papd.Sourceclient
# MAGIC LEFT OUTER JOIN lead_location lloc on lloc.UniquePartyId=papd.UniquePartyId
# MAGIC LEFT OUTER JOIN involved_location iloc on iloc.UniquePartyId=papd.UniquePartyId
# MAGIC WHERE  (cs.CaseStatusName = 'Completed' or cs.CasestatusName is null) and  (cs.ClientLifeCycleName='Client' or cs.ClientLifeCycleName is null ) and  ( cs.IsLatestApprovedVersionOfClient=True or cs.IsLatestApprovedVersionOfClient is null)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temp view parent_Client_LOB as 
# MAGIC WITH LocationAgg AS (
# MAGIC   SELECT 
# MAGIC     papd.UniquePartyId,
# MAGIC     array_distinct(flatten(array(
# MAGIC       collect_list(pbl.ProductOfferingLocation),
# MAGIC       collect_list(pbl.BookingEntityLocation),
# MAGIC       collect_list(pccd.GlobalClientOwnerLocation)
# MAGIC     ))) AS CombinedLocations
# MAGIC   FROM party_client_structure_GUI cs
# MAGIC   LEFT JOIN party_AllPartyDetails papd ON cs.ParentEntityId = papd.PartyId AND cs.UniqueParentPartyId = papd.UniquePartyId
# MAGIC   LEFT JOIN party_case_client_details pccd ON cs.SourceClient = pccd.SourceClient
# MAGIC   LEFT JOIN Product_Booking_Location pbl ON pbl.SourceClient = cs.SourceClient
# MAGIC   WHERE cs.CaseStatusName = 'Completed'
# MAGIC     AND cs.ClientLifeCycleName = 'Client'
# MAGIC     AND cs.IsLatestApprovedVersionOfClient = TRUE
# MAGIC     AND papd.Status = 'Snapshot'
# MAGIC     AND papd.ClientLifeCycleStatus = 'Client'
# MAGIC   GROUP BY papd.UniquePartyId
# MAGIC )
# MAGIC   SELECT 
# MAGIC     la.UniquePartyId,
# MAGIC     array_join(
# MAGIC       array_distinct(
# MAGIC         array_union(
# MAGIC           filter(la.CombinedLocations, x -> NOT array_contains(lloc.lead_loc, x)),
# MAGIC           CASE 
# MAGIC             WHEN iloc.involved_loc IS NOT NULL THEN iloc.involved_loc
# MAGIC             ELSE array()
# MAGIC           END
# MAGIC         )
# MAGIC       ), ', '
# MAGIC     ) AS FinalInvolvedLocation
# MAGIC   FROM LocationAgg la
# MAGIC   LEFT JOIN lead_location lloc ON lloc.UniquePartyId = la.UniquePartyId
# MAGIC   LEFT JOIN involved_location iloc ON iloc.UniquePartyId = la.UniquePartyId
# MAGIC

# COMMAND ----------

# DBTITLE 1,Logic to get all the details of Related Parties attached to an active Main Clients
# MAGIC %sql
# MAGIC --Parent
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GNS_Parent AS
# MAGIC SELECT DISTINCT 
# MAGIC CASE WHEN papd.GCDSID IS NULL AND papd.ClientType='LegalEntityClient' THEN concat('LE_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType IN ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedLegalEntity' THEN concat('RLEP_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedNaturalPerson' THEN concat('RNPP_',papd.GcobId)
# MAGIC ELSE papd.GCDSID END AS GcobId,
# MAGIC papd.UniquePartyId,
# MAGIC CS.ClientStructureSnapshotId,
# MAGIC papd.FullLegalName,
# MAGIC CASE WHEN papd.ClientType='LegalEntityClient' THEN 'E'
# MAGIC WHEN papd.ClientType='NaturalPersonClient' THEN 'I'
# MAGIC WHEN papd.ClientType='Natural Person acting in a Professional Capacity (NPPC)' THEN 'I' 
# MAGIC WHEN papd.ClientType='RelatedLegalEntity' THEN 'E'
# MAGIC WHEN papd.ClientType='RelatedNaturalPerson' THEN 'I'
# MAGIC END AS EntityType,
# MAGIC papd.IncorporationNumber,
# MAGIC to_date(trim(papd.IncorporationDate),'dd-MM-yyyy') AS IncorporationDate,
# MAGIC papd.PEPStatus,
# MAGIC regexp_replace(trim(papd.RegisteredNumber), '[\n\r\t]', ' ') AS RegisteredNumber,
# MAGIC regexp_replace(trim(papd.RegisteredStreet), '[\n\r\t]', ' ') AS RegisteredStreet,
# MAGIC regexp_replace(trim(papd.RegisteredCity), '[\n\r\t]', ' ') AS RegisteredCity,
# MAGIC regexp_replace(trim(papd.RegisteredRegion), '[\n\r\t]', ' ') AS RegisteredRegion,
# MAGIC regexp_replace(trim(papd.RegisteredPostalCode), '[\n\r\t]', ' ') AS RegisteredPostalCode,
# MAGIC regexp_replace(trim(papd.RegisteredCountryIsoCode), '[\n\r\t]', ' ') AS RegisteredCountryIsoCode,
# MAGIC regexp_replace(trim(papd.OperatingNumber), '[\n\r\t]', ' ') AS OperatingNumber,
# MAGIC regexp_replace(trim(papd.OperatingStreet), '[\n\r\t]', ' ') AS OperatingStreet,
# MAGIC regexp_replace(trim(papd.OperatingCity), '[\n\r\t]', ' ') AS OperatingCity,
# MAGIC regexp_replace(trim(papd.OperatingRegion), '[\n\r\t]', ' ') AS OperatingRegion,
# MAGIC regexp_replace(trim(papd.OperatingPostalCode), '[\n\r\t]', ' ') AS OperatingPostalCode,
# MAGIC regexp_replace(trim(papd.OperatingCountryIsoCode), '[\n\r\t]', ' ') AS OperatingCountryIsoCode,
# MAGIC pccd.GlobalClientOwnerLocation,
# MAGIC papd.DateOfBirth,
# MAGIC alias.Alias_1,
# MAGIC alias.Alias_2,
# MAGIC alias.Alias_3,
# MAGIC alias.Alias_4,
# MAGIC pbl.ProductOfferingLocation,
# MAGIC pbl.BookingEntityLocation,
# MAGIC cs.FIHubIndicator,
# MAGIC cs.ParentEntityId,
# MAGIC cs.UniqueParentPartyId,
# MAGIC papd.PartyId,
# MAGIC array_join(lloc.lead_loc, ', ') AS LeadLocation,
# MAGIC CASE 
# MAGIC WHEN papd.ClientLifeCycleStatus = 'Client' THEN fl.FinalInvolvedLocation
# MAGIC ELSE array_join(iloc.involved_loc, ', ')
# MAGIC END AS InvoledLocation,
# MAGIC alias.TradeName_5, 
# MAGIC alias.TradeName_6
# MAGIC FROM party_client_structure_GUI cs
# MAGIC LEFT OUTER JOIN party_AllPartyDetails papd ON cs.ParentEntityId = papd.PartyId and cs.UniqueParentPartyId=papd.UniquePartyId
# MAGIC LEFT OUTER JOIN party_case_client_details pccd ON cs.SourceClient=pccd.Sourceclient
# MAGIC LEFT OUTER JOIN Alias alias ON papd.PartyId = alias.PartyId AND papd.Id = alias.Id AND papd.ClientType = alias.ResultType
# MAGIC LEFT OUTER JOIN Product_Booking_Location pbl ON pbl.SourceClient = cs.SourceClient
# MAGIC LEFT OUTER JOIN lead_location lloc on lloc.UniquePartyId=papd.UniquePartyId
# MAGIC LEFT OUTER JOIN involved_location iloc on iloc.UniquePartyId=papd.UniquePartyId
# MAGIC LEFT OUTER JOIN parent_Client_LOB fl ON fl.UniquePartyId = papd.UniquePartyId
# MAGIC WHERE cs.CaseStatusName = 'Completed' AND cs.ClientLifeCycleName='Client' AND cs.IsLatestApprovedVersionOfClient=True AND papd.Status='Snapshot' 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Unioning GNS Child and Parent
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GNS AS
# MAGIC SELECT * from GNS_Child
# MAGIC UNION
# MAGIC SELECT * from GNS_Parent

# COMMAND ----------

# MAGIC %md
# MAGIC ### GNS deepdive

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Active_MainClients where gcobid = "115913" OR FullLegalName = 'TLK Dairy, Inc.'

# COMMAND ----------

# DBTITLE 1,3. GNS deepdive here.
# MAGIC %sql
# MAGIC select ClientLifeCycleName, CaseStatusName, IsLatestApprovedVersionOfClient, FullLegalNAme
# MAGIC from party_case_client_details where gcobid = 115913

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_AllPartyDetails papd 
# MAGIC where papd.CaseStatusName = 'Completed' 
# MAGIC AND papd.ClientLifeCycleStatus='Client' 
# MAGIC and  papd.IsLatestApprovedVersionOfClient=True 
# MAGIC

# COMMAND ----------

# check for this specific party here;
# -- what was the filec 2726604 TLK Dairy, Inc. (115913)
display(spark.sql('select * from GNS where gcobid like "%115913%"'))

# COMMAND ----------

# DBTITLE 1,Aligning the attributes accordingly as requested by GNS
df_Final = spark.sql("""
SELECT DISTINCT
GcobId AS ListUid,
UniquePartyId,
'' AS Detail,
'' AS Gender,
EntityType AS Type,
'Wholesale' AS Category,
'' AS SubCategory,
PEPStatus AS Status,
'' AS RiskValue,
'' AS `Name.Title.1`,
FullLegalName AS `Name.Last.1`,
'' AS `Name.Title.2`,
'' AS `Name.first.2`,
'' AS `Name.Middle.2`,
TradeName_5 AS `Name.Last.2`,
'' AS `Name.Suffix.2`,
'' AS `Name.Title.3`,
'' AS `Name.first.3`,
'' AS `Name.middle.3`,
TradeName_6 AS `Name.Last.3`,
'' AS `Name.Title.4`,
'' AS `Name.Last.4`,
'' AS `Name.first.5`,
Alias_1 AS `Name.Last.5`,
Alias_2 AS `Name.Last.6`,
Alias_3 AS `Name.Last.7`,
Alias_4 AS `Name.Last.8`,
CASE WHEN EntityType='E' THEN 'Registered address' WHEN EntityType='I' THEN 'Residential address'
END AS `Address.TypeName.1`,
RegisteredStreet AS `Address.Line1.1`,
RegisteredNumber AS `Address.Line2.1`,
'' AS `Address.Line3.1`,
'' AS `Address.Line4.1`,
'' AS `Address.Line5.1`,
'' AS `Address.Line6.1`,
RegisteredCity AS `Address.City.1`,
RegisteredRegion AS `Address.State.1`,
RegisteredCountryIsoCode AS `Address.Country.1`,
RegisteredPostalCode AS `Address.PostalCode.1`,
'' AS `Address.LocationString.1`,
CASE WHEN EntityType='E' THEN 'Correspondence address' ELSE '' END AS `Address.TypeName.2`,
OperatingStreet AS `Address.Line1.2`,
OperatingNumber AS `Address.Line2.2`,
'' AS `Address.Line3.2`,
'' AS `Address.Line4.2`,
'' AS `Address.Line5.2`,
'' AS `Address.Line6.2`,
OperatingCity AS `Address.City.2`,
OperatingRegion AS `Address.State.2`,
OperatingCountryIsoCode AS `Address.Country.2`,
OperatingPostalCode AS `Address.PostalCode.2`,
'' AS `Address.LocationString.2`,
'Residence Address' AS `Address.TypeName.3`,
RegisteredCity AS `Address.City.3`,
'' AS `Address.Country.3`,
'' AS `Address.LocationString.3`,
'' AS `Address.TypeName.4`,
'' AS `Address.Line1.4`,
'' AS `Address.Line2.4`,
'' AS `Address.Line3.4`,
'' AS `Address.Line4.4`,
'' AS `Address.Line5.4`,
'' AS `Address.Line6.4`,
'' AS `Address.City.4`,
'' AS `Address.State.4`,
'' AS `Address.Country.4`,
'' AS `Address.PostalCode.4`,
'' AS `Address.LocationString.4`,
'' AS `Address.TypeName.5`,
'' AS `Address.Line1.5`,
'' AS `Address.Line2.5`,
'' AS `Address.Line3.5`,
'' AS `Address.Line4.5`,
'' AS `Address.Line5.5`,
'' AS `Address.Line6.5`,
'' AS `Address.City.5`,
'' AS `Address.State.5`,
'' AS `Address.Country.5`,
'' AS `Address.PostalCode.5`,
'' AS `Address.LocationString.5`,
'' AS `Identifier.Type.1`,
'' AS `Identifier.TypeName.1`,
'' AS `Identifier.IdentifierValue.1`,
'' AS `Identifier.Type.2`,
'' AS `Identifier.TypeName.2`,
'' AS `Identifier.IdentifierValue.2`,
'' AS `Identifier.Type.3`,
'' AS `Identifier.TypeName.3`,
'' AS `Identifier.IdentifierValue.3`,
'o' AS `Identifier.Type.4`,
CASE WHEN EntityType='E' THEN 'Incorporation Number' ELSE '' END AS `Identifier.TypeName.4`,
IncorporationNumber AS `Identifier.IdentifierValue.4`,
CASE WHEN EntityType='E' THEN RegisteredCity ELSE '' END AS `Identifier.IdentifierState.4`,
CASE WHEN EntityType='E' THEN RegisteredCountryIsoCode ELSE '' END AS `Identifier.IdentifierCountry.4`,
'' AS `Identifier.Type.5`,
'' AS `Identifier.TypeName.5`,
'' AS `Identifier.IdentifierValue.5`,
'' AS `Identifier.IdentifierState.5`,
'' AS `Identifier.IdentifierCountry.5`,
'' AS `Identifier.Type.6`,
'' AS `Identifier.TypeName.6`,
'' AS `Identifier.IdentifierValue.6`,
'' AS `Identifier.IdentifierState.6`,
'' AS `Identifier.IdentifierCountry.6`,
'' AS `Identifier.Type.7`,
'' AS `Identifier.TypeName.7`,
'' AS `Identifier.IdentifierValue.7`,
'' AS `Identifier.IdentifierState.7`,
'' AS `Identifier.IdentifierCountry.7`,
'' AS `Identifier.Type.8`,
'' AS `Identifier.TypeName.8`,
'' AS `Identifier.IdentifierValue.8`,
'' AS `Identifier.Type.9`,
'' AS `Identifier.TypeName.9`,
'' AS `Identifier.IdentifierValue.9`,
'' AS `Identifier.IdentifierState.9`,
'' AS `Identifier.IdentifierCountry.9`,
'' AS `Identifier.Type.10`,
'' AS `Identifier.TypeName.10`,
'' AS `Identifier.IdentifierValue.10`,
'o' AS `Identifier.Type.11`,
'' AS `Identifier.TypeName.11`,
'' AS `Identifier.IdentifierValue.11`,
'o' AS `Identifier.Type.12`,
'Lead Location' AS `Identifier.TypeName.12`,
LeadLocation AS `Identifier.IdentifierValue.12`, 
'o' AS `Identifier.Type.13`,
'Involved Location' AS `Identifier.TypeName.13`,
InvoledLocation AS `Identifier.IdentifierValue.13`,
'' AS `Identifier.Type.14`,
'' AS `Identifier.TypeName.14`,
'' AS `Identifier.IdentifierValue.14`,
DateOfBirth AS `DOB.1`,
IncorporationDate AS `IncorporationDate.1`,
'' AS `IncorporationDate.2`,
'' AS `IncorporationDate.3`,
'' AS `Citizenship.1`,
'' AS `Citizenship.2`,
'' AS `DOD.1`,
'' AS `PlaceOfBirth.City.1`,
'' AS `PlaceOfBirth.Country.1`,
'' AS `PhoneNumber.type.1`,
'' AS `PhoneNumber.Number.1`,
'' AS `Keyword.1`,
GlobalClientOwnerLocation,
ProductOfferingLocation,
BookingEntityLocation,
FIHubIndicator,
ClientStructureSnapshotId,
ParentEntityId, 
UniqueParentPartyId,
PartyId
FROM GNS

""")

df_Final.count() 

# COMMAND ----------

df_Final.createOrReplaceTempView("Final")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Party_AllParty_LocationCoverage_Derived AS
# MAGIC -- Keep all original rows (Lead and Involved)
# MAGIC SELECT DISTINCT 
# MAGIC   Final.ListUid,
# MAGIC   pc.Location,
# MAGIC   pc.LeadOrInvolved
# MAGIC FROM Party_AllParty_LocationCoverage pc
# MAGIC INNER JOIN Final 
# MAGIC   ON pc.UniquePartyId = Final.UniquePartyId
# MAGIC UNION
# MAGIC -- Add new Involved rows, excluding those already present as Lead
# MAGIC SELECT DISTINCT 
# MAGIC   Final.ListUid,
# MAGIC   loc AS Location,
# MAGIC   'Involved' AS LeadOrInvolved
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     cs.UniqueParentPartyId AS UniquePartyId,
# MAGIC     array_distinct(flatten(array(
# MAGIC       collect_list(pbl.ProductOfferingLocation),
# MAGIC       collect_list(pbl.BookingEntityLocation),
# MAGIC       collect_list(pccd.GlobalClientOwnerLocation)
# MAGIC     ))) AS CombinedLocations
# MAGIC   FROM party_client_structure_GUI cs
# MAGIC   LEFT JOIN party_case_client_details pccd ON cs.SourceClient = pccd.SourceClient
# MAGIC   LEFT JOIN Product_Booking_Location pbl ON pbl.SourceClient = cs.SourceClient
# MAGIC   WHERE cs.CaseStatusName = 'Completed'
# MAGIC     AND cs.ClientLifeCycleName = 'Client'
# MAGIC     AND cs.IsLatestApprovedVersionOfClient = TRUE
# MAGIC   GROUP BY cs.UniqueParentPartyId
# MAGIC ) locAgg
# MAGIC INNER JOIN Final ON locAgg.UniquePartyId = Final.UniquePartyId
# MAGIC LEFT JOIN party_AllPartyDetails papd ON locAgg.UniquePartyId = papd.UniquePartyId
# MAGIC LATERAL VIEW explode(locAgg.CombinedLocations) AS loc
# MAGIC WHERE papd.ClientLifeCycleStatus = 'Client'
# MAGIC   AND loc IS NOT NULL AND loc != ''
# MAGIC   AND NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM Party_AllParty_LocationCoverage pc2
# MAGIC     WHERE pc2.UniquePartyId = locAgg.UniquePartyId
# MAGIC       AND pc2.Location = loc
# MAGIC       AND pc2.LeadOrInvolved = 'Lead')

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #####GENERATING ALL LOB FILES BY SPLITTING THE PARTIES BASED ON THEIR LOB

# COMMAND ----------

# DBTITLE 1,Derving the filename for each LOB
import datetime
Load_Date = datetime.datetime.now().strftime("%Y%m%d")
Current_Date = (datetime.datetime.now() - timedelta(0)).strftime("%Y%m%dT%H%M%S")

# NORTHAMERICA
WR001_File_Name = 'Rabo_WR001_'+Current_Date+'.txt'
# CHILE
WR002_File_Name = 'Rabo_WR002_'+Current_Date+'.txt'
# # BRAZIL
# WR003_File_Name = 'Rabo_WR003_'+Current_Date+'.txt'
# # HONGKONG
# WR004_File_Name = 'Rabo_WR004_'+Current_Date+'.txt'
# # SINGAPORE
# WR005_File_Name = 'Rabo_WR005_'+Current_Date+'.txt'
# # CHINA
# WR006_File_Name = 'Rabo_WR006_'+Current_Date+'.txt'
# # INDIA
# INDIA_File_Name = 'Rabo_INDIA_'+Current_Date+'.txt'
# # EUROPEAFRICA
# EUROPEAFRICA_File_Name = 'Rabo_EUROPEAFRICA_'+Current_Date+'.txt'
# # AUSTRALIANEWZEALAND
# WR009_File_Name = 'Rabo_WR009_'+Current_Date+'.txt'
# # FIHUB
# WR010_File_Name = 'Rabo_WR010_'+Current_Date+'.txt'
# AllPartyLocationCoverage
AllPartyLocationCoverage_File_Name = 'Rabo_AllPartyLocationCoverage_'+Current_Date+'.txt'
print(WR001_File_Name)
print(WR002_File_Name)
# print(WR003_File_Name)
# print(WR004_File_Name)
# print(WR005_File_Name)
# print(WR006_File_Name)
# print(INDIA_File_Name)
# print(EUROPEAFRICA_File_Name)
# print(WR009_File_Name)
# print(WR010_File_Name)
print(AllPartyLocationCoverage_File_Name)


# COMMAND ----------

dbutils.fs.rm(f"dbfs:/GNS/",True)

# COMMAND ----------

# DBTITLE 1,Writing the party to the respective LOB after deduplication to both SARADAR Storage and DBFS
from datetime import datetime
Load_Date = datetime.today().strftime('%Y%m%d')

# Columns to drop
columns_to_drop_saradar = ['GlobalClientOwnerLocation','ProductOfferingLocation', 'BookingEntityLocation']
columns_to_drop = ['GlobalClientOwnerLocation','ProductOfferingLocation', 'BookingEntityLocation','FIHubIndicator','ClientStructureSnapshotId','UniquePartyId','ParentEntityId','UniqueParentPartyId','PartyId']


# NORTHAMERICA
WR001 = ['Rabo Securities Canada, Inc. (RSCI)','Rabo Securities USA, Inc. (RSEC)','Rabobank - USA Rabo AgriFinance','Rabobank Canada (RCBR)','Rabobank Canada(Rural)','Rabobank New York']
# CHILE
WR002 = ['Rabobank Chile']
# # BRAZIL
# WR003 = ['Rabobank Brazil']
# # HONGKONG
# WR004 = ['Rabobank Hong Kong']
# # SINGAPORE
# WR005 = ['Rabobank Singapore']
# # CHINA
# WR006 = ['Rabobank China']
# # INDIA
# INDIA = ['Rabobank India']
# # EUROPEAFRICA
# EUROPEAFRICA = ['Rabobank Antwerp','Rabobank Dublin','Rabobank Frankfurt','Rabobank London','Rabobank Argentina','Rabobank Madrid','Rabobank Milan','Rabobank Netherlands','Rabobank Kenya','Rabobank Paris','Rabobank Foundation','Rabobank - Smallholder Agroforestry Finance (SAF)','Rabobank Turkey']
# # AUSTRALIANEWZEALAND
# WR009 = ['Rabobank Australia','Rabobank New Zealand','Rabobank - RANZ Country Banking and ROS']
# # FIHUB
# WR010 = ['True']

# NORTHAMERICA
df_WR001 = df_Final.filter((df_Final.GlobalClientOwnerLocation.isin(WR001) | df_Final.ProductOfferingLocation.isin(WR001) | df_Final.BookingEntityLocation.isin(WR001)))
df_max_snapshot_WR001=df_WR001.groupBy("ListUid").agg(max("ClientStructureSnapshotId").alias("max_ClientStructureSnapshotId"))

# Writing into saradar SA
df_WR001_saradar = df_WR001.join(df_max_snapshot_WR001,(df_WR001.ListUid == df_max_snapshot_WR001.ListUid) &((df_WR001.ClientStructureSnapshotId == df_max_snapshot_WR001.max_ClientStructureSnapshotId) |(col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR001["*"]).drop(*columns_to_drop_saradar).dropDuplicates(["ListUid"])
df_WR001_saradar.repartition(1).write.format("parquet").mode("overwrite").save(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/WR001/Load_date{Load_Date}.parquet")
print(f"SARadar file WR001 Count: {df_WR001_saradar.count()}")
print(WR001_File_Name+'File Has Been loaded To SARadar SA ')

# Writing into DBFS
df_WR001 = df_WR001.join(df_max_snapshot_WR001,(df_WR001.ListUid == df_max_snapshot_WR001.ListUid) &((df_WR001.ClientStructureSnapshotId == df_max_snapshot_WR001.max_ClientStructureSnapshotId) |(col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR001["*"]).drop(*columns_to_drop).dropDuplicates(["ListUid"])
df_WR001.repartition(1).write.format("csv").options(header='true',quoteAll ='true',delimiter='|').mode("overwrite").save(f"dbfs:/GNS/WR001/")
filenames = dbutils.fs.ls(f"dbfs:/GNS/WR001/")
WR001_files = [dbutils.fs.mv(f"dbfs:/GNS/WR001/"+filename.name, f"dbfs:/GNS/WR001/{WR001_File_Name}") for filename in filenames if filename.name.endswith('.csv')]
print('File Has Been Rename To: '+WR001_File_Name)
Recon_WR001_Count = 'Row Count = '+str(df_WR001.count())
dbutils.fs.put(f"dbfs:/GNS/WR001/Recon_{WR001_File_Name}",contents=Recon_WR001_Count,overwrite=True)
print('WR001 Count: '+str(df_WR001.count()), end="\n\n")

# CHILE
df_WR002 = df_Final.filter((df_Final.GlobalClientOwnerLocation.isin(WR002) | df_Final.ProductOfferingLocation.isin(WR002) | df_Final.BookingEntityLocation.isin(WR002)))
df_max_snapshot_WR002 = df_WR002.groupBy("ListUid").agg(max("ClientStructureSnapshotId").alias("max_ClientStructureSnapshotId"))

# Writing into saradar SA
df_WR002_saradar = df_WR002.join(df_max_snapshot_WR002,(df_WR002.ListUid == df_max_snapshot_WR002.ListUid) &((df_WR002.ClientStructureSnapshotId == df_max_snapshot_WR002.max_ClientStructureSnapshotId) |(col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR002["*"]).drop(*columns_to_drop_saradar).dropDuplicates(["ListUid"])
df_WR002_saradar.repartition(1).write.format("parquet").mode("overwrite").save(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/WR002/Load_date{Load_Date}.parquet")
print(f"SARadar file WR002 Count: {df_WR002_saradar.count()}")
print(WR002_File_Name+'File Has Been loaded To SARadar SA ')

# Writing into DBFS
df_WR002 = df_WR002.join(df_max_snapshot_WR002,(df_WR002.ListUid == df_max_snapshot_WR002.ListUid) &((df_WR002.ClientStructureSnapshotId == df_max_snapshot_WR002.max_ClientStructureSnapshotId) |(col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR002["*"]).drop(*columns_to_drop).dropDuplicates(["ListUid"])
df_WR002.repartition(1).write.format("csv").options(header='true',quoteAll ='true',delimiter='|').mode("overwrite").save(f"dbfs:/GNS/WR002/")
filenames = dbutils.fs.ls(f"dbfs:/GNS/WR002/")
WR002_files = [dbutils.fs.mv(f"dbfs:/GNS/WR002/"+filename.name, f"dbfs:/GNS/WR002/{WR002_File_Name}") for filename in filenames if filename.name.endswith('.csv')]
print('File Has Been Rename To: '+WR002_File_Name)
Recon_WR002_Count = 'Row Count = '+str(df_WR002.count())
dbutils.fs.put(f"dbfs:/GNS/WR002/Recon_{WR002_File_Name}",contents=Recon_WR002_Count,overwrite=True)
print('WR002 Count: '+str(df_WR002.count()), end="\n\n")

# # BRAZIL
# df_WR003 = df_Final.filter((df_Final.GlobalClientOwnerLocation.isin(WR003) | df_Final.ProductOfferingLocation.isin(WR003) | df_Final.BookingEntityLocation.isin(WR003)))
# df_max_snapshot_WR003 = df_WR003.groupBy("ListUid").agg(max("ClientStructureSnapshotId").alias("max_ClientStructureSnapshotId"))

# # Writing into saradar SA
# df_WR003_saradar = df_WR003.join(df_max_snapshot_WR003, (df_WR003.ListUid == df_max_snapshot_WR003.ListUid) & ((df_WR003.ClientStructureSnapshotId == df_max_snapshot_WR003.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR003["*"]).drop(*columns_to_drop_saradar).dropDuplicates(["ListUid"])
# df_WR003_saradar.repartition(1).write.format("parquet").mode("overwrite").save(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/WR003/Load_date{Load_Date}.parquet")
# print(f"SARadar file WR003 Count: {df_WR003_saradar.count()}")
# print(WR003_File_Name+'File Has Been loaded To SARadar SA ')

# # Writing into DBFS
# df_WR003 = df_WR003.join(df_max_snapshot_WR003, (df_WR003.ListUid == df_max_snapshot_WR003.ListUid) & ((df_WR003.ClientStructureSnapshotId == df_max_snapshot_WR003.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR003["*"]).drop(*columns_to_drop).dropDuplicates(["ListUid"])
# df_WR003.repartition(1).write.format("csv").options(header='true',quoteAll ='true',delimiter='|').mode("overwrite").save(f"dbfs:/GNS/WR003/")
# filenames = dbutils.fs.ls(f"dbfs:/GNS/WR003/")
# WR003_files = [dbutils.fs.mv(f"dbfs:/GNS/WR003/"+filename.name, f"dbfs:/GNS/WR003/{WR003_File_Name}") for filename in filenames if filename.name.endswith('.csv')]
# print('File Has Been Rename To: '+WR003_File_Name)
# Recon_WR003_Count = 'Row Count = '+str(df_WR003.count())
# dbutils.fs.put(f"dbfs:/GNS/WR003/Recon_{WR003_File_Name}",contents=Recon_WR003_Count,overwrite=True)
# print('WR003 Count: '+str(df_WR003.count()), end="\n\n")

# # HONGKONG
# df_WR004 = df_Final.filter((df_Final.GlobalClientOwnerLocation.isin(WR004) | df_Final.ProductOfferingLocation.isin(WR004) | df_Final.BookingEntityLocation.isin(WR004)))
# df_max_snapshot_WR004 = df_WR004.groupBy("ListUid").agg(max("ClientStructureSnapshotId").alias("max_ClientStructureSnapshotId"))

# # Writing into saradar SA
# df_WR004_saradar = df_WR004.join(df_max_snapshot_WR004,(df_WR004.ListUid == df_max_snapshot_WR004.ListUid) &((df_WR004.ClientStructureSnapshotId == df_max_snapshot_WR004.max_ClientStructureSnapshotId) |(col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR004["*"]).drop(*columns_to_drop_saradar).dropDuplicates(["ListUid"])

# # Writing into DBFS
# df_WR004_saradar.repartition(1).write.format("parquet").mode("overwrite").save(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/WR004/Load_date{Load_Date}.parquet")
# print(f"SARadar file WR004 Count: {df_WR004_saradar.count()}")
# print(WR004_File_Name+'File Has Been loaded To SARadar SA ')
# df_WR004 = df_WR004.join(df_max_snapshot_WR004,(df_WR004.ListUid == df_max_snapshot_WR004.ListUid) &((df_WR004.ClientStructureSnapshotId == df_max_snapshot_WR004.max_ClientStructureSnapshotId) |(col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR004["*"]).drop(*columns_to_drop).dropDuplicates(["ListUid"])
# df_WR004.repartition(1).write.format("csv").options(header='true',quoteAll ='true',delimiter='|').mode("overwrite").save(f"dbfs:/GNS/WR004/")
# filenames = dbutils.fs.ls(f"dbfs:/GNS/WR004/")
# WR004_files = [dbutils.fs.mv(f"dbfs:/GNS/WR004/"+filename.name, f"dbfs:/GNS/WR004/{WR004_File_Name}") for filename in filenames if filename.name.endswith('.csv')]
# print('File Has Been Rename To: '+WR004_File_Name)
# Recon_WR004_Count = 'Row Count = '+str(df_WR004.count())
# dbutils.fs.put(f"dbfs:/GNS/WR004/Recon_{WR004_File_Name}",contents=Recon_WR004_Count,overwrite=True)
# print('WR004 Count: '+str(df_WR004.count()), end="\n\n")

# # SINGAPORE
# df_WR005 = df_Final.filter((df_Final.GlobalClientOwnerLocation.isin(WR005) | df_Final.ProductOfferingLocation.isin(WR005) | df_Final.BookingEntityLocation.isin(WR005)))
# df_max_snapshot_WR005 = df_WR005.groupBy("ListUid").agg(max("ClientStructureSnapshotId").alias("max_ClientStructureSnapshotId"))

# # Writing into saradar SA
# df_WR005_saradar = df_WR005.join(df_max_snapshot_WR005,(df_WR005.ListUid == df_max_snapshot_WR005.ListUid) &((df_WR005.ClientStructureSnapshotId == df_max_snapshot_WR005.max_ClientStructureSnapshotId) |(col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR005["*"]).drop(*columns_to_drop_saradar).dropDuplicates(["ListUid"])

# # Writing into DBFS
# df_WR005_saradar.repartition(1).write.format("parquet").mode("overwrite").save(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/WR005/Load_date{Load_Date}.parquet")
# print(f"SARadar file WR005 Count: {df_WR005_saradar.count()}")
# print(WR005_File_Name+'File Has Been loaded To SARadar SA ')
# df_WR005 = df_WR005.join(df_max_snapshot_WR005, (df_WR005.ListUid == df_max_snapshot_WR005.ListUid) & ((df_WR005.ClientStructureSnapshotId == df_max_snapshot_WR005.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR005["*"]).drop(*columns_to_drop).dropDuplicates(["ListUid"])
# df_WR005.repartition(1).write.format("csv").options(header='true',quoteAll ='true',delimiter='|').mode("overwrite").save(f"dbfs:/GNS/WR005/")
# filenames = dbutils.fs.ls(f"dbfs:/GNS/WR005/")
# WR005_files = [dbutils.fs.mv(f"dbfs:/GNS/WR005/"+filename.name, f"dbfs:/GNS/WR005/{WR005_File_Name}") for filename in filenames if filename.name.endswith('.csv')]
# print('File Has Been Rename To: '+WR005_File_Name)
# Recon_WR005_Count = 'Row Count = '+str(df_WR005.count())
# dbutils.fs.put(f"dbfs:/GNS/WR005/Recon_{WR005_File_Name}",contents=Recon_WR005_Count,overwrite=True)
# print('WR005 Count: '+str(df_WR005.count()), end="\n\n")

# # CHINA
# df_WR006 = df_Final.filter((df_Final.GlobalClientOwnerLocation.isin(WR006) | df_Final.ProductOfferingLocation.isin(WR006) | df_Final.BookingEntityLocation.isin(WR006)))
# df_max_snapshot_WR006 = df_WR006.groupBy("ListUid").agg(max("ClientStructureSnapshotId").alias("max_ClientStructureSnapshotId"))

# # Writing into saradar SA
# df_WR006_saradar = df_WR006.join(df_max_snapshot_WR006, (df_WR006.ListUid == df_max_snapshot_WR006.ListUid) & ((df_WR006.ClientStructureSnapshotId == df_max_snapshot_WR006.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR006["*"]).drop(*columns_to_drop_saradar).dropDuplicates(["ListUid"])
# df_WR006_saradar.repartition(1).write.format("parquet").mode("overwrite").save(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/WR006/Load_date{Load_Date}.parquet")
# print(f"SARadar file WR006 Count: {df_WR006_saradar.count()}")
# print(WR006_File_Name+'File Has Been loaded To SARadar SA ')

# # Writing into DBFS
# df_WR006 = df_WR006.join(df_max_snapshot_WR006, (df_WR006.ListUid == df_max_snapshot_WR006.ListUid) & ((df_WR006.ClientStructureSnapshotId == df_max_snapshot_WR006.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR006["*"]).drop(*columns_to_drop).dropDuplicates(["ListUid"])
# df_WR006.repartition(1).write.format("csv").options(header='true',quoteAll ='true',delimiter='|').mode("overwrite").save(f"dbfs:/GNS/WR006/")
# filenames = dbutils.fs.ls(f"dbfs:/GNS/WR006/")
# WR006_files = [dbutils.fs.mv(f"dbfs:/GNS/WR006/"+filename.name, f"dbfs:/GNS/WR006/{WR006_File_Name}") for filename in filenames if filename.name.endswith('.csv')]
# print('File Has Been Rename To: '+WR006_File_Name)
# Recon_WR006_Count = 'Row Count = '+str(df_WR006.count())
# dbutils.fs.put(f"dbfs:/GNS/WR006/Recon_{WR006_File_Name}",contents=Recon_WR006_Count,overwrite=True)
# print('WR006 Count: '+str(df_WR006.count()), end="\n\n")

# # INDIA
# df_INDIA = df_Final.filter((df_Final.GlobalClientOwnerLocation.isin(INDIA) | df_Final.ProductOfferingLocation.isin(INDIA) | df_Final.BookingEntityLocation.isin(INDIA)))
# df_max_snapshot_INDIA = df_INDIA.groupBy("ListUid").agg(max("ClientStructureSnapshotId").alias("max_ClientStructureSnapshotId"))

# # Writing into saradar SA
# df_INDIA_saradar = df_INDIA.join(df_max_snapshot_INDIA, (df_INDIA.ListUid == df_max_snapshot_INDIA.ListUid) & ((df_INDIA.ClientStructureSnapshotId == df_max_snapshot_INDIA.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_INDIA["*"]).drop(*columns_to_drop_saradar).dropDuplicates(["ListUid"])
# df_INDIA_saradar.repartition(1).write.format("parquet").mode("overwrite").save(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/INDIA/Load_date{Load_Date}.parquet")
# print(f"SARadar file INDIA Count: {df_INDIA_saradar.count()}")
# print(INDIA_File_Name+'File Has Been loaded To SARadar SA ')

# # Writing into DBFS
# df_INDIA = df_INDIA.join(df_max_snapshot_INDIA, (df_INDIA.ListUid == df_max_snapshot_INDIA.ListUid) & ((df_INDIA.ClientStructureSnapshotId == df_max_snapshot_INDIA.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_INDIA["*"]).drop(*columns_to_drop).dropDuplicates(["ListUid"])
# df_INDIA.repartition(1).write.format("csv").options(header='true',quoteAll ='true',delimiter='|').mode("overwrite").save(f"dbfs:/GNS/INDIA/")
# filenames = dbutils.fs.ls(f"dbfs:/GNS/INDIA/")
# India_files = [dbutils.fs.mv(f"dbfs:/GNS/INDIA/"+filename.name, f"dbfs:/GNS/INDIA/{INDIA_File_Name}") for filename in filenames if filename.name.endswith('.csv')]
# print('File Has Been Rename To: '+INDIA_File_Name)
# Recon_INDIA_Count = 'Row Count = '+str(df_INDIA.count())
# dbutils.fs.put(f"dbfs:/GNS/INDIA/Recon_{INDIA_File_Name}",contents=Recon_INDIA_Count,overwrite=True)
# print('INDIA Count: '+str(df_INDIA.count()), end="\n\n")

# # EUROPEAFRICA
# df_EUROPEAFRICA = df_Final.filter((df_Final.GlobalClientOwnerLocation.isin(EUROPEAFRICA) | df_Final.ProductOfferingLocation.isin(EUROPEAFRICA) | df_Final.BookingEntityLocation.isin(EUROPEAFRICA)))
# df_max_snapshot_EUROPEAFRICA = df_EUROPEAFRICA.groupBy("ListUid").agg(max("ClientStructureSnapshotId").alias("max_ClientStructureSnapshotId"))

# # Writing into saradar SA
# df_EUROPEAFRICA_saradar = df_EUROPEAFRICA.join(df_max_snapshot_EUROPEAFRICA, (df_EUROPEAFRICA.ListUid == df_max_snapshot_EUROPEAFRICA.ListUid) & ((df_EUROPEAFRICA.ClientStructureSnapshotId == df_max_snapshot_EUROPEAFRICA.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_EUROPEAFRICA["*"]).drop(*columns_to_drop_saradar).dropDuplicates(["ListUid"])
# df_EUROPEAFRICA_saradar.repartition(1).write.format("parquet").mode("overwrite").save(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/EUROPEAFRICA/Load_date{Load_Date}.parquet")
# print(f"SARadar file EUROPEAFRICA Count: {df_EUROPEAFRICA_saradar.count()}")
# print(EUROPEAFRICA_File_Name+'File Has Been loaded To SARadar SA ')

# # Writing into DBFS
# df_EUROPEAFRICA = df_EUROPEAFRICA.join(df_max_snapshot_EUROPEAFRICA, (df_EUROPEAFRICA.ListUid == df_max_snapshot_EUROPEAFRICA.ListUid) & ((df_EUROPEAFRICA.ClientStructureSnapshotId == df_max_snapshot_EUROPEAFRICA.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_EUROPEAFRICA["*"]).drop(*columns_to_drop).dropDuplicates(["ListUid"])
# df_EUROPEAFRICA.repartition(1).write.format("csv").options(header='true',quoteAll ='true',delimiter='|').mode("overwrite").save(f"dbfs:/GNS/EUROPEAFRICA/")
# filenames = dbutils.fs.ls(f"dbfs:/GNS/EUROPEAFRICA/")
# EUROPEAFRICA_files = [dbutils.fs.mv(f"dbfs:/GNS/EUROPEAFRICA/"+filename.name, f"dbfs:/GNS/EUROPEAFRICA/{EUROPEAFRICA_File_Name}") for filename in filenames if filename.name.endswith('.csv')]
# print('File Has Been Rename To: '+EUROPEAFRICA_File_Name)
# Recon_EUROPEAFRICA_Count = 'Row Count = '+str(df_EUROPEAFRICA.count())
# dbutils.fs.put(f"dbfs:/GNS/EUROPEAFRICA/Recon_{EUROPEAFRICA_File_Name}",contents=Recon_EUROPEAFRICA_Count,overwrite=True)
# print('EUROPEAFRICA Count: '+str(df_EUROPEAFRICA.count()), end="\n\n")

# # AUSTRALIANEWZEALAND
# df_WR009 = df_Final.filter((df_Final.GlobalClientOwnerLocation.isin(WR009) | df_Final.ProductOfferingLocation.isin(WR009) | df_Final.BookingEntityLocation.isin(WR009)))
# df_max_snapshot_WR009 = df_WR009.groupBy("ListUid").agg(max("ClientStructureSnapshotId").alias("max_ClientStructureSnapshotId"))

# # Writing into saradar SA
# df_WR009_saradar = df_WR009.join(df_max_snapshot_WR009, (df_WR009.ListUid == df_max_snapshot_WR009.ListUid) & ((df_WR009.ClientStructureSnapshotId == df_max_snapshot_WR009.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR009["*"]).drop(*columns_to_drop_saradar).dropDuplicates(["ListUid"])
# df_WR009_saradar.repartition(1).write.format("parquet").mode("overwrite").save(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/WR009/Load_date{Load_Date}.parquet")
# print(f"SARadar file WR009 Count: {df_WR009_saradar.count()}")
# print(WR009_File_Name+'File Has Been loaded To SARadar SA ')

# # Writing into DBFS
# df_WR009 = df_WR009.join(df_max_snapshot_WR009, (df_WR009.ListUid == df_max_snapshot_WR009.ListUid) & ((df_WR009.ClientStructureSnapshotId == df_max_snapshot_WR009.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR009["*"]).drop(*columns_to_drop).dropDuplicates(["ListUid"])
# df_WR009.repartition(1).write.format("csv").options(header='true',quoteAll ='true',delimiter='|').mode("overwrite").save(f"dbfs:/GNS/WR009/")
# filenames = dbutils.fs.ls(f"dbfs:/GNS/WR009/")
# WR009_files = [dbutils.fs.mv(f"dbfs:/GNS/WR009/"+filename.name, f"dbfs:/GNS/WR009/{WR009_File_Name}") for filename in filenames if filename.name.endswith('.csv')]
# print('File Has Been Rename To: '+WR009_File_Name)
# Recon_WR009_Count = 'Row Count = '+str(df_WR009.count())
# dbutils.fs.put(f"dbfs:/GNS/WR009/Recon_{WR009_File_Name}",contents=Recon_WR009_Count,overwrite=True)
# print('WR009 Count: '+str(df_WR009.count()), end="\n\n")

# # FIHUB
# df_WR010 = df_Final.filter((df_Final.FIHubIndicator.isin(WR010)))
# df_max_snapshot_WR010 = df_WR010.groupBy("ListUid").agg(max("ClientStructureSnapshotId").alias("max_ClientStructureSnapshotId"))

# # Writing into saradar SA
# df_WR010_saradar = df_WR010.join(df_max_snapshot_WR010, (df_WR010.ListUid == df_max_snapshot_WR010.ListUid) & ((df_WR010.ClientStructureSnapshotId == df_max_snapshot_WR010.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR010["*"]).drop(*columns_to_drop_saradar).dropDuplicates(["ListUid"])
# df_WR010_saradar.repartition(1).write.format("parquet").mode("overwrite").save(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/WR010/Load_date{Load_Date}.parquet")
# print(f"SARadar file WR010 Count: {df_WR010_saradar.count()}")
# print(WR010_File_Name+'File Has Been loaded To SARadar SA ')

# # Writing into DBFS
# df_WR010 = df_WR010.join(df_max_snapshot_WR010, (df_WR010.ListUid == df_max_snapshot_WR010.ListUid) & ((df_WR010.ClientStructureSnapshotId == df_max_snapshot_WR010.max_ClientStructureSnapshotId) | (col("ClientStructureSnapshotId").isNull() & col("max_ClientStructureSnapshotId").isNull()))).select(df_WR010["*"]).drop(*columns_to_drop).dropDuplicates(["ListUid"])
# df_WR010.repartition(1).write.format("csv").options(header='true',quoteAll ='true',delimiter='|').mode("overwrite").save(f"dbfs:/GNS/WR010/")
# filenames = dbutils.fs.ls(f"dbfs:/GNS/WR010/")
# WR010_files = [dbutils.fs.mv(f"dbfs:/GNS/WR010/"+filename.name, f"dbfs:/GNS/WR010/{WR010_File_Name}") for filename in filenames if filename.name.endswith('.csv')]
# print('File Has Been Rename To: '+WR010_File_Name)
# Recon_WR010_Count = 'Row Count = '+str(df_WR010.count())
# dbutils.fs.put(f"dbfs:/GNS/WR010/Recon_{WR010_File_Name}",contents=Recon_WR010_Count,overwrite=True)
# print('WR010 Count: '+str(df_WR010.count()))

#AllParty_LocationCoverage
# Writing into DBFS
df_AllParty_LocationCoverage=spark.table('Party_AllParty_LocationCoverage_Derived')
df_AllParty_LocationCoverage.repartition(1).write.format("csv").options(header='true',quoteAll ='true',delimiter='|').mode("overwrite").save(f"dbfs:/GNS/AllPartyLocationCoverage/")
filenames = dbutils.fs.ls(f"dbfs:/GNS/AllPartyLocationCoverage/")
AllPartyLocationCoverage_files = [dbutils.fs.mv(f"dbfs:/GNS/AllPartyLocationCoverage/"+filename.name, f"dbfs:/GNS/AllPartyLocationCoverage/{AllPartyLocationCoverage_File_Name}") for filename in filenames if filename.name.endswith('.csv')]
print('File Has Been Rename To: '+AllPartyLocationCoverage_File_Name)
Recon_AllPartyLocationCoverage_Count = 'Row Count = '+str(df_AllParty_LocationCoverage.count())
dbutils.fs.put(f"dbfs:/GNS/AllPartyLocationCoverage/Recon_{AllPartyLocationCoverage_File_Name}",contents=Recon_AllPartyLocationCoverage_Count,overwrite=True)
print('AllPartyLocationCoverage Count: '+str(df_AllParty_LocationCoverage.count()))


# COMMAND ----------

# MAGIC %md
# MAGIC #####WRITING ALL THE FILES TO GNS STORAGE ACCOUNT

# COMMAND ----------

from datetime import datetime
# Determine run context
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

if context.jobId().isDefined():
    job_id = context.jobId().get()
    run_type = "workflow_job"

    # Step 3: Fetch environment variables
    application_id = os.environ['APP_REG_APP_ID']
    tenant_id = os.environ['TENANT_ID']
    # Step 4: Retrieve client secret securely
    service_credential = dbutils.secrets.get(scope="connectedsecrets", key=f"appreg-{application_id}")
    # Step 5: Get Azure AD access token
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": application_id,
        "client_secret": service_credential,
        "scope": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
    }
    token_resp = requests.post(token_url, data=payload)
    access_token = token_resp.json()["access_token"]
    # Step 6: Define job and task details
    workspace_url = f"""https://{spark.conf.get("spark.databricks.workspaceUrl")}"""
    job_id_to_check = job_id
    task_key_to_find = "GlobalNameScreening"
    # Step 7: Get today's UTC start time in ms
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_time_ms = int(today.timestamp() * 1000)
    # Step 8: List all runs for the job
    resp = requests.get(
        f"{workspace_url}/api/2.1/jobs/runs/list",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"job_id": job_id_to_check, "start_time_from": start_time_ms}
    )
    run_data = resp.json()
    runs = run_data.get("runs", [])
    matching_runs = []
    # Step 9: Collect successful runs of the task
    for run in runs:
        run_id = run.get("run_id")
        run_detail_resp = requests.get(
            f"{workspace_url}/api/2.1/jobs/runs/get",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"run_id": run_id}
        )
        
        run_detail = run_detail_resp.json()
        tasks = run_detail.get("tasks", [])
        
        for t in tasks:
            if t.get("task_key") == task_key_to_find:
                state = t.get("state", {})
                if state.get("life_cycle_state") == "TERMINATED" and state.get("result_state") == "SUCCESS":
                    matching_runs.append({
                        "run_id": run_id,
                        "start_time": run_detail.get("start_time"),
                        "end_time": run_detail.get("end_time"),
                        "state": state
                    })
    # Step 10: Print results or proceed
    if matching_runs:
        dbutils.notebook.exit(f"""✅ Found {len(matching_runs)} successful runs of task '{task_key_to_find}' today
        📌 Notebook Run Context:
        - Run Type : {run_type}
        - Job ID   : {job_id}
        - Run ID   : {run_id}
        - Time     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}""")
    else:
        print(f"🚫 No successful runs of task '{task_key_to_find}' found today.")
        print("🚀 Starting task execution...")
else:
    run_type = "manual_run"
    job_id=None
    run_id=None
    # Step 2: Manual run — print and exit
    dbutils.notebook.exit(f"""
    📌 Notebook Run Context:
    - Run Type : {run_type}
    - Job ID   : {job_id}
    - Run ID   : {run_id}
    - Time     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    🛑 Manual run detected. Exiting notebook.
    """)

# COMMAND ----------

# DBTITLE 1,Function to write to GNS Storage Account
from azure.storage.blob import BlobServiceClient

Account_name= GNS_STORAGE_ACCOUNT
container_name= GNS_CONTAINER

def Upload_File_Blob_Container(Account_name,container_name,local_path,blob_name):
    account_url = f"https://{Account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url, credential=dbutils.secrets.get(scope="connectedsecrets", key="gns"))
    container_client = blob_service_client.get_container_client(container=container_name)
    
    blob_client = container_client.get_blob_client(blob_name)
    
    with open(local_path, "rb") as data:
        blob_client.upload_blob(data,overwrite=True)
    return print(blob_name+" File Uploaded Successfully")

# COMMAND ----------

# DBTITLE 1,Loading WR001 File to GNS Storage
# NORTHAMERICA (WR001)
# Data File Upload
Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR001/{WR001_File_Name}",blob_name= WR001_File_Name)

# Recon File Upload
Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR001/Recon_{WR001_File_Name}",blob_name=f"Recon_{WR001_File_Name}")

# COMMAND ----------

# DBTITLE 1,Loading WR002 File to GNS Storage
# CHILE (WR002)
# Data File Upload
Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR002/{WR002_File_Name}",blob_name= WR002_File_Name)

# Recon File Upload
Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR002/Recon_{WR002_File_Name}",blob_name= f"Recon_{WR002_File_Name}")

# COMMAND ----------

# DBTITLE 1,Loading WR003 File to GNS Storage
# # BRAZIL (WR003)
# # Data File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR003/{WR003_File_Name}",blob_name= WR003_File_Name)

# # Recon File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR003/Recon_{WR003_File_Name}",blob_name= f"Recon_{WR003_File_Name}")

# COMMAND ----------

# DBTITLE 1,Loading WR004 File to GNS Storage
# # HONGKONG (WR004)
# # Data File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR004/{WR004_File_Name}",blob_name= WR004_File_Name)

# # Recon File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR004/Recon_{WR004_File_Name}",blob_name= f"Recon_{WR004_File_Name}")

# COMMAND ----------

# DBTITLE 1,Loading WR005 File to GNS Storage
# # SINGAPORE (WR005)
# # Data File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR005/{WR005_File_Name}",blob_name= WR005_File_Name)

# # Recon File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR005/Recon_{WR005_File_Name}",blob_name= f"Recon_{WR005_File_Name}")

# COMMAND ----------

# DBTITLE 1,Loading WR006 File to GNS Storage
# # CHINA (WR006)
# # Data File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR006/{WR006_File_Name}",blob_name= WR006_File_Name)

# # Recon File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR006/Recon_{WR006_File_Name}",blob_name= f"Recon_{WR006_File_Name}")

# COMMAND ----------

# DBTITLE 1,Loading India File to GNS Storage
# # INDIA ()
# # Data File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/INDIA/{INDIA_File_Name}",blob_name= INDIA_File_Name)

# # Recon File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/INDIA/Recon_{INDIA_File_Name}",blob_name=f"Recon_{INDIA_File_Name}")

# COMMAND ----------

# DBTITLE 1,Loading EUROPEAFRICA File to GNS Storage
# # EUROPEAFRICA ()
# # Data File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/EUROPEAFRICA/{EUROPEAFRICA_File_Name}",blob_name=f"{EUROPEAFRICA_File_Name}")

# # Recon File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/EUROPEAFRICA/Recon_{EUROPEAFRICA_File_Name}",blob_name=f"Recon_{EUROPEAFRICA_File_Name}")

# COMMAND ----------

# DBTITLE 1,Loading WR009 File to GNS Storage
# # AUSTRALIANEWZEALAND (WR009)
# # Data File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR009/{WR009_File_Name}",blob_name= WR009_File_Name)

# # Recon File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR009/Recon_{WR009_File_Name}",blob_name= f"Recon_{WR009_File_Name}")

# COMMAND ----------

# DBTITLE 1,Loading WR010 File to GNS Storage
# # FIHUB (WR010)
# # Data File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR010/{WR010_File_Name}",blob_name= WR010_File_Name)

# # Recon File Upload
# Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/WR010/Recon_{WR010_File_Name}",blob_name=f"Recon_{WR010_File_Name}")

# COMMAND ----------

# DBTITLE 1,Loading AllParty_LocationCoverage File to GNS Storage
# AllParty_LocationCoverage
# Data File Upload
Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/AllPartyLocationCoverage/{AllPartyLocationCoverage_File_Name}",blob_name= AllPartyLocationCoverage_File_Name)

# Recon File Upload
Upload_File_Blob_Container(Account_name=Account_name,container_name=container_name,local_path= rf"/dbfs/GNS/AllPartyLocationCoverage/Recon_{AllPartyLocationCoverage_File_Name}",blob_name=f"Recon_{AllPartyLocationCoverage_File_Name}")
