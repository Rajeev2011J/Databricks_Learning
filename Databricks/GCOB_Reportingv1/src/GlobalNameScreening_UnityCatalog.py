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
# MAGIC | Ruud van Laar      |5-Dec-2025  |14493930 | Increasing selection scope 1. to have Latest approved cases of Prospect AND client, 2. Changing de-duplication mechanism to prioritize latest-approved-customer data over customer data as part of structure_snapshot.
# MAGIC | Prasad Gadidala     |27-FEB-2026  |15363073 | As a Screening analyst in NA I want to see all customers where the party is part of the structure.So that I have a clear understanding on the context of the involvement of this party with rabobank
# MAGIC | Prajit Tatari      |4-Mar-2026 |15477177 |Adding 20 TradeNames, 4 map with Alias same column
# MAGIC | Ruud van Laar | 18 may 2026 | Only kept until the big export to store for lineage in Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC #####IMPORTING THE LIBRARIES AND READING GCOB DATAOBJECTS FROM GDP

# COMMAND ----------

# DBTITLE 1,Import required packages
import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from pyspark.sql.functions import *
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import requests
import json
from azure.storage.blob import BlobServiceClient

#Local File Import
from RadarUtils import *

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

# DBTITLE 1,Reading environment variables
GDP_EU_Storage_Account = os.environ['GDP_STORAGE_NAME']
GNS_STORAGE_ACCOUNT= os.environ['GNS_STORAGE_ACCOUNT']
GNS_CONTAINER= os.environ['GNS_CONTAINER']
environment=os.environ['ENV']
SARadarStorage = f'saradar{environment}'

# COMMAND ----------

# DBTITLE 1,Authenticating Storage accounts
authenticate_storage_account(SARadarStorage)
authenticate_storage_account(GDP_EU_Storage_Account)

# COMMAND ----------

# DBTITLE 1,Reading GCOB GDP Defined DataObjects
# List of dataobjects to load from from GDP


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
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')
    #final_path = f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/{base_path}/{path_suffix}/{Object}/102/data/{load_dts}/*.parquet'
    

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from party_client_structure_GUI
# MAGIC where ClientStructureSnapshotID = 698311 --616298

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from party_client_structure_GUI
# MAGIC where ClientStructureSnapshotID = 698194 --616298

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_products_and_sevices where gcobid = 19421

# COMMAND ----------

# MAGIC %sql
# MAGIC select ListUid
# MAGIC , `Name.Last.1` 
# MAGIC , PartyId
# MAGIC , concat('https://gcob.rabonet.com/relatedLegalEntityPartySnapshot/', PartyId) AS GCOBLinkRLEP
# MAGIC , ClientStructureSnapshotId
# MAGIC ,  `Identifier.Type.12`	,`Identifier.TypeName.12`	,`Identifier.IdentifierValue.12`	
# MAGIC ,`Identifier.Type.13`,	`Identifier.TypeName.13`,	`Identifier.IdentifierValue.13`
# MAGIC ,*
# MAGIC
# MAGIC from `wr_fj_parties_and_risk_assessment_preprd`.`globalnamescreening`.`fullgcobdailygnsextract` 
# MAGIC
# MAGIC where ListUid IN ('698311')

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
# MAGIC     , TradeName_7
# MAGIC     , TradeName_8
# MAGIC     , TradeName_9
# MAGIC     , TradeName_10
# MAGIC     , TradeName_11
# MAGIC     , TradeName_12
# MAGIC     , TradeName_13
# MAGIC     , TradeName_14
# MAGIC     , TradeName_15
# MAGIC     , TradeName_16
# MAGIC     , TradeName_17
# MAGIC     , TradeName_18
# MAGIC     , TradeName_19
# MAGIC     , TradeName_20
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
# MAGIC     max(ClientTradeName) for RowN in (1 TradeName_1,2 TradeName_2,3 TradeName_3,4 TradeName_4,5 TradeName_5,6 TradeName_6,7 TradeName_7, 8 TradeName_8,9 TradeName_9,10 TradeName_10,11 TradeName_11,12 TradeName_12,13 TradeName_13,14 TradeName_14,15 TradeName_15,16 TradeName_16,17 TradeName_17,18 TradeName_18,19 TradeName_19,20 TradeName_20)
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Combine Alias & TradeName based on Flag
if TradeFlag == 'True':
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW Alias AS
        SELECT distinct PartyId, Id, ResultType, Alias_1, Alias_2, Alias_3, Alias_4, null as TradeName_1, null as TradeName_2, null as TradeName_3,null as TradeName_8, null as TradeName_9, null as TradeName_10, null as TradeName_11, null as TradeName_12, null as TradeName_13, null as TradeName_14, null as TradeName_15, null as TradeName_16, null as TradeName_17, null as TradeName_18, null as TradeName_19, null as TradeName_20 FROM AliasDetails
        UNION ALL
        SELECT distinct PartyId, Id, ResultType, TradeName_4, TradeName_5, TradeName_6, TradeName_7, TradeName_1, TradeName_2, TradeName_3, TradeName_8, TradeName_9, TradeName_10, TradeName_11, TradeName_12, TradeName_13, TradeName_14, TradeName_15, TradeName_16, TradeName_17, TradeName_18, TradeName_19, TradeName_20 FROM TradeName
    """)
else:
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW Alias AS
        SELECT distinct PartyId, Id, ResultType, Alias_1, Alias_2, Alias_3, Alias_4, '' as TradeName_1, '' as TradeName_2, '' as TradeName_3,'' as TradeName_8, '' as TradeName_9, '' as TradeName_10, '' as TradeName_11, '' as TradeName_12, '' as TradeName_13, '' as TradeName_14, '' as TradeName_15, '' as TradeName_16, '' as TradeName_17, '' as TradeName_18, '' as TradeName_19, '' as TradeName_20 FROM AliasDetails
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
# MAGIC End as SourceClient from party_AllPartyDetails papd 
# MAGIC
# MAGIC where papd.ClientLifeCycleStatus IN ('Client', 'Prospect')
# MAGIC and  papd.IsLatestApprovedVersionOfClient=True 

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
# MAGIC try_to_date(trim(papd.IncorporationDate),'dd-MM-yyyy') AS IncorporationDate,
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
# MAGIC alias.TradeName_1,
# MAGIC alias.TradeName_2,
# MAGIC alias.TradeName_3,
# MAGIC alias.TradeName_8,
# MAGIC alias.TradeName_9,
# MAGIC alias.TradeName_10,
# MAGIC alias.TradeName_11,
# MAGIC alias.TradeName_12,
# MAGIC alias.TradeName_13,
# MAGIC alias.TradeName_14,
# MAGIC alias.TradeName_15,
# MAGIC alias.TradeName_16,
# MAGIC alias.TradeName_17,
# MAGIC alias.TradeName_18,
# MAGIC alias.TradeName_19,
# MAGIC alias.TradeName_20,
# MAGIC 1 AS `DirectRepresentationOfClientFlag`
# MAGIC FROM Active_MainClients papd 
# MAGIC LEFT OUTER JOIN party_client_structure_GUI cs ON papd.Sourceclient=cs.Sourceclient
# MAGIC LEFT OUTER JOIN Alias alias ON papd.PartyId = alias.PartyId AND papd.Id = alias.Id AND papd.ClientType = alias.ResultType
# MAGIC LEFT OUTER JOIN Product_Booking_Location pbl ON pbl.SourceClient = papd.Sourceclient
# MAGIC LEFT OUTER JOIN lead_location lloc on lloc.UniquePartyId=papd.UniquePartyId
# MAGIC LEFT OUTER JOIN involved_location iloc on iloc.UniquePartyId=papd.UniquePartyId
# MAGIC WHERE   (cs.ClientLifeCycleName IN ('Client', 'Prospect') or cs.ClientLifeCycleName is null ) and  
# MAGIC -- why is IsLatestApprovedVersionOfClient as null also valid?
# MAGIC ( cs.IsLatestApprovedVersionOfClient=True or cs.IsLatestApprovedVersionOfClient is null)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Selects the lead location of all parents in the structure that are also an active client.
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
# MAGIC   WHERE cs.ClientLifeCycleName = 'Client'
# MAGIC     AND cs.IsLatestApprovedVersionOfClient = TRUE
# MAGIC     AND papd.Status = 'Snapshot'
# MAGIC     AND papd.ClientLifeCycleStatus IN ('Client', 'Prospect')
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
# MAGIC try_to_date(trim(papd.IncorporationDate),'dd-MM-yyyy') AS IncorporationDate,
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
# MAGIC alias.TradeName_1,
# MAGIC alias.TradeName_2,
# MAGIC alias.TradeName_3,
# MAGIC alias.TradeName_8,
# MAGIC alias.TradeName_9,
# MAGIC alias.TradeName_10,
# MAGIC alias.TradeName_11,
# MAGIC alias.TradeName_12,
# MAGIC alias.TradeName_13,
# MAGIC alias.TradeName_14,
# MAGIC alias.TradeName_15,
# MAGIC alias.TradeName_16,
# MAGIC alias.TradeName_17,
# MAGIC alias.TradeName_18,
# MAGIC alias.TradeName_19,
# MAGIC alias.TradeName_20,
# MAGIC 0 AS `DirectRepresentationOfClientFlag`
# MAGIC
# MAGIC FROM party_client_structure_GUI cs
# MAGIC LEFT OUTER JOIN party_AllPartyDetails papd ON cs.ParentEntityId = papd.PartyId and cs.UniqueParentPartyId=papd.UniquePartyId
# MAGIC LEFT OUTER JOIN party_case_client_details pccd ON cs.SourceClient=pccd.Sourceclient
# MAGIC LEFT OUTER JOIN Alias alias ON papd.PartyId = alias.PartyId AND papd.Id = alias.Id AND papd.ClientType = alias.ResultType
# MAGIC LEFT OUTER JOIN Product_Booking_Location pbl ON pbl.SourceClient = cs.SourceClient
# MAGIC LEFT OUTER JOIN lead_location lloc on lloc.UniquePartyId=papd.UniquePartyId
# MAGIC LEFT OUTER JOIN involved_location iloc on iloc.UniquePartyId=papd.UniquePartyId
# MAGIC LEFT OUTER JOIN parent_Client_LOB fl ON fl.UniquePartyId = papd.UniquePartyId
# MAGIC WHERE 
# MAGIC     cs.ClientLifeCycleName IN ('Client', 'Prospect') 
# MAGIC     AND cs.IsLatestApprovedVersionOfClient=True 
# MAGIC     AND papd.Status='Snapshot' 
# MAGIC

# COMMAND ----------

# DBTITLE 1,GNS Partystructure client and related parties
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GNS_Partystructure AS
# MAGIC SELECT DISTINCT 
# MAGIC CS.ClientGcobId,
# MAGIC CS.ClientCaseId,
# MAGIC CS.ClientFullLegalName,
# MAGIC CS.ClientType,
# MAGIC CASE WHEN papd.GCDSID IS NULL AND papd.ClientType='LegalEntityClient' THEN concat('LE_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType IN ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedLegalEntity' THEN concat('RLEP_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedNaturalPerson' THEN concat('RNPP_',papd.GcobId)
# MAGIC ELSE papd.GCDSID END AS ListUid,
# MAGIC CS.ParentIdentityName,
# MAGIC pccd.GlobalClientOwnerLocation,
# MAGIC pbl.ProductOfferingLocation,
# MAGIC pbl.BookingEntityLocation
# MAGIC
# MAGIC FROM party_client_structure_GUI cs
# MAGIC LEFT OUTER JOIN party_AllPartyDetails papd ON cs.ParentEntityId = papd.PartyId and cs.UniqueParentPartyId=papd.UniquePartyId
# MAGIC LEFT OUTER JOIN party_case_client_details pccd ON cs.SourceClient=pccd.Sourceclient
# MAGIC LEFT OUTER JOIN Product_Booking_Location pbl ON pbl.SourceClient = cs.SourceClient
# MAGIC WHERE 
# MAGIC     cs.ClientLifeCycleName IN ('Client', 'Prospect') 
# MAGIC     AND cs.IsLatestApprovedVersionOfClient=True 
# MAGIC     AND papd.Status='Snapshot' 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Unioning GNS Child and Parent
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GNS AS
# MAGIC SELECT * from GNS_Child
# MAGIC UNION
# MAGIC SELECT * from GNS_Parent

# COMMAND ----------

# DBTITLE 1,Aligning the attributes accordingly as requested by GNS
df_Final = spark.sql(f"""
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
TradeName_1 AS `Name.Last.2`,
'' AS `Name.Suffix.2`,
'' AS `Name.Title.3`,
'' AS `Name.first.3`,
'' AS `Name.middle.3`,
TradeName_2 AS `Name.Last.3`,
'' AS `Name.Title.4`,
TradeName_3 AS `Name.Last.4`,
'' AS `Name.first.5`,
Alias_1 AS `Name.Last.5`,
Alias_2 AS `Name.Last.6`,
Alias_3 AS `Name.Last.7`,
Alias_4 AS `Name.Last.8`,
TradeName_8 AS `Name.Last.9`,
TradeName_9 AS `Name.Last.10`,
TradeName_10 AS `Name.Last.11`,
TradeName_11 AS `Name.Last.12`,
TradeName_12 AS `Name.Last.13`,
TradeName_13 AS `Name.Last.14`,
TradeName_14 AS `Name.Last.15`,
TradeName_15 AS `Name.Last.16`,
TradeName_16 AS `Name.Last.17`,
TradeName_17 AS `Name.Last.18`,
TradeName_18 AS `Name.Last.19`,
TradeName_19 AS `Name.Last.20`,
TradeName_20 AS `Name.Last.21`,
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
--LeadLocation AS `Identifier.IdentifierValue.12`,
REPLACE(LeadLocation, 'Rabobank', 'WR') AS `Identifier.IdentifierValue.12`,
'o' AS `Identifier.Type.13`,
'Involved Location' AS `Identifier.TypeName.13`,
--InvoledLocation AS `Identifier.IdentifierValue.13`,
REPLACE(InvoledLocation, 'Rabobank', 'WR') AS `Identifier.IdentifierValue.13`, 
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
PartyId,
DirectRepresentationOfClientFlag
FROM GNS
 
""")
 
df_Final.count()
 

# COMMAND ----------

trade_columns_to_drop=['Name.Last.9', 'Name.Last.10', 'Name.Last.11', 'Name.Last.12','Name.Last.13', 'Name.Last.14','Name.Last.15','Name.Last.16','Name.Last.17','Name.Last.18', 'Name.Last.19', 'Name.Last.20','Name.Last.21']
if TradeFlag != 'True':
    df_Final = df_Final.drop(*trade_columns_to_drop)

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
# MAGIC   WHERE cs.ClientLifeCycleName IN ('Client', 'Prospect')
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
# MAGIC #### STORING FOUNDATIONAL TABLE INSIDE UNITY CATALOG

# COMMAND ----------

# DBTITLE 1,get the relevant catalog name for this environment
radar_catalog_name = os.environ['Radar_Catalog_Name']

# COMMAND ----------

print(radar_catalog_name)

# COMMAND ----------

# DBTITLE 1,Store df_final
df_Final.write.mode.mode('overwrite').saveAsTable(f'{radar_catalog_name}.GlobalNameScreening.FullGCOBDailyGNSExtract')

# COMMAND ----------

# DBTITLE 1,Store Party location coverage derived
df_Party_Location_Coverage_Derived=spark.sql("select * from Party_AllParty_LocationCoverage_Derived")

df_Party_Location_Coverage_Derived.write.mode('overwrite').saveAsTable(f'{radar_catalog_name}.GlobalNameScreening.Party_Location_Coverage_Derived')


# COMMAND ----------


