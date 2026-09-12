# Databricks notebook source
from azure.storage.blob import BlobServiceClient
import os
import re
import builtins

from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime, timedelta

# COMMAND ----------

file_loc = rf"/dbfs/GNS/WR001/"

# COMMAND ----------

Account_name="gnsuat"
container_name="uatsource"

# COMMAND ----------

# DBTITLE 1,define last NA file -
# The value can be found in today's logs/prints of "Job: non_MI_reporting_job, Run: GlobalNameScreeningRun."
WR001_File_Name = 'Rabo_WR001_20250411T043115.txt'

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/GNS/WR001")

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").option("delimiter", "|").load("dbfs:/GNS/WR001/Rabo_WR001_20250411T043115.txt")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from NA where `Name.Last.1` like '%JBT%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from NA where `Name.Last.1` like '%John Bean%'

# COMMAND ----------

# MAGIC %md
# MAGIC ## GCOB Selection

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

Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
Today = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'
print (load_dts)
Load = 'EDL_LOAD_DTS='+ datetime.today().strftime('%Y%m%d')
#print(Load)
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
EDL_LoadDate = (datetime.today()).strftime("%Y-%m-%dT%H:%m:%mZ")

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'definedDatasetname':[
'party_client_structure_GUI',
'party_case_client_details',
'party_AllPartyDetails',
# 'party_trade_name',
'party_products_and_sevices',
'party_Alias',
# 'party_RelatedPartyIdentificationDocument'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():

    #Fetch the latest DataVersion
    all_versions = []
    versionFiles = dbutils.fs.ls(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{row.definedDatasetname}/')

    for file in versionFiles:
        all_versions.append(re.split("/", file.name)[0])
    all_versions = [int(item) for item in all_versions if item.isdigit()]
    current_version = str(builtins.max(all_versions))

    #To find if the file exists for today and extract load_dts
    currentVersionPath = dbutils.fs.ls(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{row.definedDatasetname}/{current_version}/data/')

    getLoad_dts = [file for file in currentVersionPath if Today in file.name][0]
    load_dts = re.split("/", getLoad_dts.name)[0] + '*'
    
    #Read the file
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{row.definedDatasetname}/{current_version}/data/{load_dts}/*.parquet').createOrReplaceTempView(row.definedDatasetname)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_AllPartyDetails limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_AllPartyDetails where FullLegalName like '%JBT Marel%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_AllPartyDetails where FullLegalName like '%John Bean Technologies Corporation%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_client_structure_GUI
# MAGIC where clientGcobId = 32128

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Alias AS
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

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Product_Booking_Location AS
# MAGIC select distinct SourceClient,ClientId,gcobid,ClientType,ClientLifeCycleName,IsLatestApprovedVersionOfClient,ProductOfferingLocation,BookingEntityLocation  from party_products_and_sevices

# COMMAND ----------

# MAGIC %sql
# MAGIC --Child
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GNS_Child AS
# MAGIC SELECT DISTINCT 
# MAGIC CASE WHEN papd.GCDSID IS NULL AND papd.ClientType='LegalEntityClient' THEN concat('LE_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType IN ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedLegalEntity' THEN concat('RLEP_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedNaturalPerson' THEN concat('RNPP_',papd.GcobId)
# MAGIC ELSE papd.GCDSID END AS GcobId,
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
# MAGIC -- pccd.ValidatedRiskLevel AS CDDRating,
# MAGIC regexp_replace(trim(papd.RegisteredNumber), '[\n\r\t]', ' ') AS RegisteredNumber,
# MAGIC regexp_replace(trim(papd.RegisteredStreet), '[\n\r\t]', ' ') AS RegisteredStreet,
# MAGIC regexp_replace(trim(papd.RegisteredCity), '[\n\r\t]', ' ') AS RegisteredCity,
# MAGIC regexp_replace(trim(papd.RegisteredRegion), '[\n\r\t]', ' ') AS RegisteredRegion,
# MAGIC regexp_replace(trim(papd.RegisteredPostalCode), '[\n\r\t]', ' ') AS RegisteredPostalCode,
# MAGIC -- regexp_replace(trim(papd.RegisteredCountryName), '[\n\r\t]', ' ') AS RegisteredCountryName,
# MAGIC regexp_replace(trim(papd.RegisteredCountryIsoCode), '[\n\r\t]', ' ') AS RegisteredCountryIsoCode,
# MAGIC regexp_replace(trim(papd.OperatingNumber), '[\n\r\t]', ' ') AS OperatingNumber,
# MAGIC regexp_replace(trim(papd.OperatingStreet), '[\n\r\t]', ' ') AS OperatingStreet,
# MAGIC regexp_replace(trim(papd.OperatingCity), '[\n\r\t]', ' ') AS OperatingCity,
# MAGIC regexp_replace(trim(papd.OperatingRegion), '[\n\r\t]', ' ') AS OperatingRegion,
# MAGIC regexp_replace(trim(papd.OperatingPostalCode), '[\n\r\t]', ' ') AS OperatingPostalCode,
# MAGIC -- regexp_replace(trim(papd.OperatingCountryName), '[\n\r\t]', ' ') AS OperatingCountryName,
# MAGIC regexp_replace(trim(papd.OperatingCountryIsoCode), '[\n\r\t]', ' ') AS OperatingCountryIsoCode,
# MAGIC papd.GlobalClientOwnerLocation,
# MAGIC -- papd.GlobalClientOwnerName,
# MAGIC -- to_date(trim(papd.DateOfBirth),'dd-MM-yyyy') AS DateOfBirth,
# MAGIC papd.DateOfBirth,
# MAGIC -- papd.Nationality,
# MAGIC -- papd.Citizenship,
# MAGIC -- trade.ClientTradeName,
# MAGIC -- pol.ProductOfferingLocation,
# MAGIC -- pccd.WWID,
# MAGIC -- papd.FullLegalNameInLocalLanguage,
# MAGIC -- papd.FirstName,
# MAGIC -- papd.MiddleName,
# MAGIC -- papd.LastName,
# MAGIC alias.Alias_1,
# MAGIC alias.Alias_2,
# MAGIC alias.Alias_3,
# MAGIC alias.Alias_4,
# MAGIC -- Ident.PassportIdNumber,
# MAGIC -- Ident.PassportPlaceOfIssue,
# MAGIC -- Ident.PassportCountryOfIssue,
# MAGIC -- Ident.OtherIdNumber,
# MAGIC -- Ident.OtherPlaceOfIssue,
# MAGIC -- Ident.OtherCountryOfIssue,
# MAGIC -- Ident.NationalIdNumber,
# MAGIC -- Ident.NationalPlaceOfIssue,
# MAGIC -- Ident.NationalCountryOfIssue,
# MAGIC -- tof.TypesOfRelation,
# MAGIC pbl.ProductOfferingLocation,
# MAGIC pbl.BookingEntityLocation,
# MAGIC cs.FIHubIndicator
# MAGIC FROM party_client_structure_GUI cs
# MAGIC INNER JOIN party_AllPartyDetails papd ON cs.ChildEntityId = papd.PartyId and cs.UniqueChildPartyId=papd.UniquePartyId --AND cs.ChildType = CASE WHEN papd.ClientType='Natural Person acting in a Professional Capacity (NPPC)' THEN 'NaturalPersonClient' ELSE papd.ClientType END --AND trim(papd.FullLegalName) = trim(cs.ChildIdentityName) 
# MAGIC -- LEFT OUTER JOIN party_case_client_details pccd ON pccd.ClientID = papd.Id AND papd.GcobId = pccd.GcobId AND pccd.CaseId = papd.CaseId AND pccd.FullLegalName = papd.FullLegalName
# MAGIC -- LEFT OUTER JOIN ProductOfferingLocation pol ON pol.ClientId = papd.Id AND papd.GcobId = pol.gcobid AND papd.ClientType = pol.ClientType
# MAGIC -- LEFT OUTER JOIN TradeName trade ON papd.PartyId = trade.PartyId AND papd.Id = trade.Id AND papd.ClientType = trade.ResultType
# MAGIC LEFT OUTER JOIN Alias alias ON papd.PartyId = alias.PartyId AND papd.Id = alias.Id AND papd.ClientType = alias.ResultType
# MAGIC -- LEFT OUTER JOIN IdentificationDocument Ident ON papd.PartyId = Ident.PartyId AND papd.Id = Ident.Id AND papd.ClientType = Ident.ClientType AND papd.GcobId = Ident.GcobId
# MAGIC -- LEFT OUTER JOIN Parent_TypesOfRelation tof ON papd.GcobId = tof.ParentIdentity AND CASE WHEN papd.ClientType='Natural Person acting in a Professional Capacity (NPPC)' THEN 'NaturalPersonClient' ELSE papd.ClientType END = tof.ParentType AND papd.FullLegalName = tof.ParentIdentityName
# MAGIC LEFT OUTER JOIN Product_Booking_Location pbl ON pbl.SourceClient = cs.SourceClient AND pbl.ClientId = cs.ClientID  
# MAGIC WHERE cs.CaseStatusName = 'Completed' AND cs.ClientLifeCycleName='Client' AND cs.IsLatestApprovedVersionOfClient=True AND papd.Status='Snapshot' --AND papd.Expired IS NULL
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Parent
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GNS_Parent AS
# MAGIC SELECT DISTINCT 
# MAGIC CASE WHEN papd.GCDSID IS NULL AND papd.ClientType='LegalEntityClient' THEN concat('LE_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType IN ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedLegalEntity' THEN concat('RLEP_',papd.GcobId)
# MAGIC WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedNaturalPerson' THEN concat('RNPP_',papd.GcobId)
# MAGIC ELSE papd.GCDSID END AS GcobId,
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
# MAGIC -- pccd.ValidatedRiskLevel AS CDDRating,
# MAGIC regexp_replace(trim(papd.RegisteredNumber), '[\n\r\t]', ' ') AS RegisteredNumber,
# MAGIC regexp_replace(trim(papd.RegisteredStreet), '[\n\r\t]', ' ') AS RegisteredStreet,
# MAGIC regexp_replace(trim(papd.RegisteredCity), '[\n\r\t]', ' ') AS RegisteredCity,
# MAGIC regexp_replace(trim(papd.RegisteredRegion), '[\n\r\t]', ' ') AS RegisteredRegion,
# MAGIC regexp_replace(trim(papd.RegisteredPostalCode), '[\n\r\t]', ' ') AS RegisteredPostalCode,
# MAGIC -- regexp_replace(trim(papd.RegisteredCountryName), '[\n\r\t]', ' ') AS RegisteredCountryName,
# MAGIC regexp_replace(trim(papd.RegisteredCountryIsoCode), '[\n\r\t]', ' ') AS RegisteredCountryIsoCode,
# MAGIC regexp_replace(trim(papd.OperatingNumber), '[\n\r\t]', ' ') AS OperatingNumber,
# MAGIC regexp_replace(trim(papd.OperatingStreet), '[\n\r\t]', ' ') AS OperatingStreet,
# MAGIC regexp_replace(trim(papd.OperatingCity), '[\n\r\t]', ' ') AS OperatingCity,
# MAGIC regexp_replace(trim(papd.OperatingRegion), '[\n\r\t]', ' ') AS OperatingRegion,
# MAGIC regexp_replace(trim(papd.OperatingPostalCode), '[\n\r\t]', ' ') AS OperatingPostalCode,
# MAGIC -- regexp_replace(trim(papd.OperatingCountryName), '[\n\r\t]', ' ') AS OperatingCountryName,
# MAGIC regexp_replace(trim(papd.OperatingCountryIsoCode), '[\n\r\t]', ' ') AS OperatingCountryIsoCode,
# MAGIC papd.GlobalClientOwnerLocation,
# MAGIC -- papd.GlobalClientOwnerName,
# MAGIC -- to_date(trim(papd.DateOfBirth),'dd-MM-yyyy') AS DateOfBirth,
# MAGIC papd.DateOfBirth,
# MAGIC -- papd.Nationality,
# MAGIC -- papd.Citizenship,
# MAGIC -- trade.ClientTradeName,
# MAGIC -- pol.ProductOfferingLocation,
# MAGIC -- pccd.WWID,
# MAGIC -- papd.FullLegalNameInLocalLanguage,
# MAGIC -- papd.FirstName,
# MAGIC -- papd.MiddleName,
# MAGIC -- papd.LastName,
# MAGIC alias.Alias_1,
# MAGIC alias.Alias_2,
# MAGIC alias.Alias_3,
# MAGIC alias.Alias_4,
# MAGIC -- Ident.PassportIdNumber,
# MAGIC -- Ident.PassportPlaceOfIssue,
# MAGIC -- Ident.PassportCountryOfIssue,
# MAGIC -- Ident.OtherIdNumber,
# MAGIC -- Ident.OtherPlaceOfIssue,
# MAGIC -- Ident.OtherCountryOfIssue,
# MAGIC -- Ident.NationalIdNumber,
# MAGIC -- Ident.NationalPlaceOfIssue,
# MAGIC -- Ident.NationalCountryOfIssue,
# MAGIC -- tof.TypesOfRelation,
# MAGIC pbl.ProductOfferingLocation,
# MAGIC pbl.BookingEntityLocation,
# MAGIC cs.FIHubIndicator
# MAGIC FROM party_client_structure_GUI cs
# MAGIC INNER JOIN party_AllPartyDetails papd ON cs.ParentEntityId = papd.PartyId and cs.UniqueParentPartyId=papd.UniquePartyId --AND cs.ParentType = CASE WHEN papd.ClientType='Natural Person acting in a Professional Capacity (NPPC)' THEN 'NaturalPersonClient' ELSE papd.ClientType END --AND trim(papd.FullLegalName) = trim(cs.ParentIdentityName) 
# MAGIC -- LEFT OUTER JOIN party_case_client_details pccd ON pccd.ClientID = papd.Id AND papd.GcobId = pccd.GcobId AND pccd.CaseId = papd.CaseId AND pccd.FullLegalName = papd.FullLegalName
# MAGIC -- LEFT OUTER JOIN ProductOfferingLocation pol ON pol.ClientId = papd.Id AND papd.GcobId = pol.gcobid AND papd.ClientType = pol.ClientType
# MAGIC -- LEFT OUTER JOIN TradeName trade ON papd.PartyId = trade.PartyId AND papd.Id = trade.Id AND papd.ClientType = trade.ResultType
# MAGIC LEFT OUTER JOIN Alias alias ON papd.PartyId = alias.PartyId AND papd.Id = alias.Id AND papd.ClientType = alias.ResultType
# MAGIC -- LEFT OUTER JOIN IdentificationDocument Ident ON papd.PartyId = Ident.PartyId AND papd.Id = Ident.Id AND papd.ClientType = Ident.ClientType AND papd.GcobId = Ident.GcobId
# MAGIC -- LEFT OUTER JOIN Parent_TypesOfRelation tof ON papd.GcobId = tof.ParentIdentity AND CASE WHEN papd.ClientType='Natural Person acting in a Professional Capacity (NPPC)' THEN 'NaturalPersonClient' ELSE papd.ClientType END = tof.ParentType AND papd.FullLegalName = tof.ParentIdentityName
# MAGIC LEFT OUTER JOIN Product_Booking_Location pbl ON pbl.SourceClient = cs.SourceClient AND pbl.ClientId = cs.ClientID
# MAGIC WHERE cs.CaseStatusName = 'Completed' AND cs.ClientLifeCycleName='Client' AND cs.IsLatestApprovedVersionOfClient=True AND papd.Status='Snapshot' --AND papd.Expired IS NULL
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GNS AS
# MAGIC SELECT * from GNS_Child
# MAGIC UNION
# MAGIC SELECT * from GNS_Parent

# COMMAND ----------

# DBTITLE 1,search for whitney bank
# MAGIC %sql
# MAGIC select * from GNS where FullLegalName like '%Whitney%'

# COMMAND ----------

# DBTITLE 1,Select GNS selection for checks
# MAGIC %sql
# MAGIC select * from GNS where FullLegalName like '%JBT Marel%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GNS_child where FullLegalName like '%JBT Marel%'

# COMMAND ----------

# DBTITLE 1,Dedub for Marel happens here
# MAGIC %sql
# MAGIC select * from GNS where GcobId = 40500

# COMMAND ----------

# DBTITLE 1,Duplicate for Windy
# MAGIC %sql
# MAGIC select * from GNS where GcobId = 2654352

# COMMAND ----------

df_Final = spark.sql("""

SELECT DISTINCT
GcobId AS ListUid,
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
'' AS `Name.Last.2`,
'' AS `Name.Suffix.2`,
'' AS `Name.Title.3`,
'' AS `Name.first.3`,
'' AS `Name.middle.3`,
'' AS `Name.Last.3`,
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
'' AS `Identifier.Type.11`,
'' AS `Identifier.TypeName.11`,
'' AS `Identifier.IdentifierValue.11`,
'o' AS `Identifier.Type.12`,
'Global Client Owner Location' AS `Identifier.TypeName.12`,
'' AS `Identifier.IdentifierValue.12`, 
'' AS `Identifier.Type.13`,
'' AS `Identifier.TypeName.13`,
'' AS `Identifier.IdentifierValue.13`,
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
FIHubIndicator
FROM GNS

""")
# df_Final.createOrReplaceTempView('GNS')
df_Final.count() 

# COMMAND ----------



# COMMAND ----------

['Rabo Securities Canada, Inc. (RSCI)','Rabo Securities USA, Inc. (RSEC)','Rabobank - USA Rabo AgriFinance','Rabobank Canada (RCBR)','Rabobank Canada(Rural)','Rabobank New York']
