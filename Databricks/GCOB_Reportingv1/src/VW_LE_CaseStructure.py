# Databricks notebook source
# MAGIC %md
# MAGIC ## **VW_LE_CaseStructure**
# MAGIC
# MAGIC Goal: Creating New View as part of Reporting 
# MAGIC  
# MAGIC ###### <span style="color:orange">Authors:</span>
# MAGIC 1.  hemasundar.nuthalapati@rabobank.com
# MAGIC  
# MAGIC ###### <span style="color:orange">Business value:</span>
# MAGIC  
# MAGIC     Create View VW_LE_CaseStructure from GDP Definer layer columns  to Hive Catalog
# MAGIC  
# MAGIC  
# MAGIC ###### <span style="color:blue">Flow of the logic:</span>
# MAGIC 1. Read data from GDP Defined layer
# MAGIC 2. Create Finale view with all required Column 
# MAGIC 4. Load the data to Hive catalog
# MAGIC

# COMMAND ----------

import os
from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime, timedelta

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

#jdbcHostname='firebirdsqlserverprodnla.database.windows.net'
ServiceKey='app-reg-databricks-wr-radar-preprd'

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
Today = datetime.today().strftime('%Y%m%d')
load_dts = 'LOADED_DTS=' + Yesterdate + '*'
print (load_dts)
Load = 'EDL_LOAD_DTS='+ datetime.today().strftime('%Y%m%d')
print(Load)
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
print(BusinessDate)
EDL_LoadDate = (datetime.today()).strftime("%Y-%m-%dT%H:%m:%mZ")

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'CaseService_case_LegalEntityClient',
'CaseService_case_Case',
'CaseService_case_LegalEntityClientStructureSnapshot',
'CaseService_case_RelatedLegalEntityParty',
'CaseService_case_Address',
'CaseService_case_RelatedLegalEntityPartySupervisingExchange',
'CaseService_case_LegalEntitySupervisingExchange',
'CaseService_case_ExchangeReference',
'CaseService_dbo_Country',
'CaseService_case_RelatedNaturalPersonParty',
'CaseService_case_PoliticallyExposedPersonStatusReference',
'CaseService_case_ScreeningResults',
'CaseService_case_RelatedNaturalPersonPartyNationality',
'CaseService_snapshot_ClientStructureSnapshotDirectorshipRelationshipComponentDetail',
'CaseService_snapshot_ClientStructureSnapshotEndOfChainOwnershipShareholdingRelationshipComponentDetail',
'CaseService_snapshot_ClientStructureSnapshotCalculatedUboShareholdingDetail',
'CaseService_case_UboThroughReasonRelationshipComponent'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{row.GDPname}/100/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

Today = datetime.today().strftime('%Y%m%d')
load_dts = 'LOAD_DTS=' + Today + '*'

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'snapshot_ClientStructureSnapshotRelationshipDetail'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{row.GDPname}/0/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW LatestClientStructSnapshot AS
# MAGIC     SELECT DISTINCT 
# MAGIC     MAX(t1.ClientStructureSnapshotId) AS MaxClientStructureSnapshotId
# MAGIC     ,MAX(t1.EDL_LOAD_DTS) AS EDL_LOAD_DTS
# MAGIC     , t1.LegalEntityClientId 
# MAGIC     FROM CaseService_case_LegalEntityClientStructureSnapshot t1
# MAGIC     GROUP BY t1.LegalEntityClientId
# MAGIC 	

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ClientOnRecognizedStockExchange AS
# MAGIC SELECT 
# MAGIC     t1.LegalEntityId, 
# MAGIC     MAX(CAST(t2.IsRecognisedExchange AS INT)) AS ClientOnRecognisedExchange
# MAGIC FROM 
# MAGIC     CaseService_case_LegalEntitySupervisingExchange t1
# MAGIC LEFT JOIN 
# MAGIC     CaseService_case_ExchangeReference t2 
# MAGIC ON 
# MAGIC     t1.ExchangeReferenceId = t2.ExchangeId
# MAGIC GROUP BY 
# MAGIC     t1.LegalEntityId;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RelatedEntityOnRecognizedStockExchange AS 
# MAGIC     SELECT 
# MAGIC         t1.RelatedLegalEntityPartyId, 
# MAGIC         MAX(CAST(t2.IsRecognisedExchange AS INT)) AS ClientOnRecognisedExchange
# MAGIC     FROM 
# MAGIC         CaseService_case_RelatedLegalEntityPartySupervisingExchange t1
# MAGIC     LEFT JOIN 
# MAGIC         CaseService_case_ExchangeReference t2 
# MAGIC     ON 
# MAGIC         t1.ExchangeReferenceId = t2.ExchangeId
# MAGIC     GROUP BY 
# MAGIC         t1.RelatedLegalEntityPartyId
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW  VW_LE_CaseStructure
# MAGIC  AS 
# MAGIC
# MAGIC SELECT t1a.CaseId,
# MAGIC     t1.GcobId,
# MAGIC     t1.Id as LegalEntityClientId,
# MAGIC     
# MAGIC     t2.MaxClientStructureSnapshotId,
# MAGIC     COALESCE(t4.FullLegalName, t5.FullLegalName) AS RelatedPartyParentName,
# MAGIC     t3.ParentType,
# MAGIC     CASE
# MAGIC         WHEN ParentType = 0 THEN concat('LE: ', t5.Id)
# MAGIC         WHEN ParentType = 1 THEN concat('RLE: ', t4.Identity)
# MAGIC         WHEN ParentType = 2 THEN concat('RNP: ', t6.Identity)
# MAGIC     END AS ParentIdentity,
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(t4.FullLegalName, t5.FullLegalName) LIKE '%IQ EQ%' THEN 1
# MAGIC         WHEN COALESCE(t4.FullLegalName, t5.FullLegalName) LIKE '%intertrust%' THEN 1
# MAGIC         WHEN COALESCE(t4.FullLegalName, t5.FullLegalName) LIKE '%TMF%' THEN 1
# MAGIC         WHEN COALESCE(t4.FullLegalName, t5.FullLegalName) LIKE '%Vistra%' THEN 1
# MAGIC         ELSE 0 
# MAGIC     END AS HasTrustParent,
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(t4c.IsoCode, t5c.IsoCode) IN ('AS', 'AI', 'BS', 'BH', 'BB', 'BM', 'KY', 'CR', 'FJ', 'GU', 'GG', 'IM', 'JE', 'MH', 'PW', 'PA', 'RU', 'WS', 'TT', 'TM', 'TC', 'AE', 'VU', 'VG', 'VI') THEN 1 
# MAGIC         ELSE 0 
# MAGIC     END AS HasTaxCountry,
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(t4c.Name, t5c.Name) IN ('Luxembourg', 'Cayman Islands', 'Liechtenstein', 'Curaçao', 'Bonaire, Sint Eustatius and Saba') THEN 1 
# MAGIC         ELSE 0 
# MAGIC     END AS HasSuspiciousTCSPCountry,
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(t4b.Region, t5b.Region) = 'Delaware' THEN 1 
# MAGIC         ELSE 0 
# MAGIC     END AS HasDelaware,
# MAGIC     COALESCE(t4c.Name, t5c.Name) AS ParentCountry,
# MAGIC     COALESCE(t4.IsClientListed, t5.IsClientListed) AS IsParentListed,
# MAGIC     COALESCE(CAST(t4d.ClientOnRecognisedExchange AS INT), CAST(t5d.ClientOnRecognisedExchange AS INT)) AS PartyOnRecognizedExchange,
# MAGIC     t11.RelationshipDetailId AS HasDirector,
# MAGIC     t12.ShareholdingPercentage AS ShareholdingPercentageOfDirectChild,
# MAGIC     t13.CalculatedShareholdingPercentage AS CalculatedShareholdingPercentageOnClient,
# MAGIC     t14.UboThroughReasonReferenceId,
# MAGIC     t6c.Name AS PEPStatus,
# MAGIC     t6e.Name AS NP_Nationality,
# MAGIC     t6g.Name AS NP_Residential_Address_Country
# MAGIC FROM CaseService_case_LegalEntityClient t1
# MAGIC JOIN CaseService_case_Case t1a ON t1a.LegalEntityClientId = t1.Id 
# MAGIC INNER JOIN LatestClientStructSnapshot t2 ON t2.LegalEntityClientId = t1.Id
# MAGIC LEFT JOIN snapshot_ClientStructureSnapshotRelationshipDetail t3 ON t2.MaxClientStructureSnapshotId = t3.ClientStructureSnapshotId 
# MAGIC LEFT JOIN CaseService_case_RelatedLegalEntityParty t4 ON t3.ParentIdentity = t4.RelatedLegalEntityPartyId AND ParentType = 1
# MAGIC LEFT JOIN CaseService_case_Address t4b ON t4b.AddressId = t4.RegisteredAddressId
# MAGIC LEFT JOIN CaseService_dbo_Country t4c ON t4b.CountryReferenceId = t4c.Id
# MAGIC LEFT JOIN RelatedEntityOnRecognizedStockExchange t4d ON t4.RelatedLegalEntityPartyId = t4d.RelatedLegalEntityPartyId
# MAGIC LEFT JOIN CaseService_case_LegalEntityClient t5 ON t3.ParentIdentity = CAST(t5.Id AS VARCHAR(255)) AND ParentType = 0
# MAGIC LEFT JOIN CaseService_case_Address t5b ON t5b.AddressId = t5.RegisteredAddressId
# MAGIC LEFT JOIN CaseService_dbo_Country t5c ON t5b.CountryReferenceId = t5c.Id
# MAGIC LEFT JOIN ClientOnRecognizedStockExchange t5d ON t5.Id = t5d.LegalEntityId
# MAGIC LEFT JOIN CaseService_case_RelatedNaturalPersonParty t6 ON t6.RelatedNaturalPersonPartyId = t3.ParentIdentity AND ParentType = 2
# MAGIC LEFT JOIN CaseService_case_Address t6b ON t6b.AddressId = t6.AddressId
# MAGIC LEFT JOIN CaseService_case_PoliticallyExposedPersonStatusReference t6c ON t6.PoliticallyExposedPersonStatusId = t6c.PoliticallyExposedPersonStatusId
# MAGIC LEFT JOIN CaseService_case_RelatedNaturalPersonPartyNationality t6d ON t6.RelatedNaturalPersonPartyId = t6d.RelatedNaturalPersonPartyId
# MAGIC LEFT JOIN CaseService_dbo_Country t6e ON t6d.CountryReferenceId = t6e.Id
# MAGIC LEFT JOIN CaseService_case_Address t6f ON t6.AddressId = t6f.AddressId
# MAGIC LEFT JOIN CaseService_dbo_Country t6g ON t6f.CountryReferenceId = t6g.Id
# MAGIC LEFT JOIN CaseService_case_ScreeningResults t6h ON t6.ScreeningResultsId = t6h.ScreeningResultsId
# MAGIC LEFT JOIN CaseService_snapshot_ClientStructureSnapshotDirectorshipRelationshipComponentDetail t11 ON t3.RelationshipDetailId = t11.RelationshipDetailId
# MAGIC LEFT JOIN CaseService_snapshot_ClientStructureSnapshotEndOfChainOwnershipShareholdingRelationshipComponentDetail t12 ON t3.RelationshipDetailId = t12.RelationshipDetailId
# MAGIC LEFT JOIN CaseService_snapshot_ClientStructureSnapshotCalculatedUboShareholdingDetail t13 ON t3.ParentIdentity = t13.RelatedPartyIdentity AND t3.ParentType = t13.RelationshipEndTypeId AND t13.ClientStructureSnapshotId = t2.MaxClientStructureSnapshotId
# MAGIC LEFT JOIN (SELECT * FROM CaseService_case_UboThroughReasonRelationshipComponent WHERE UboThroughReasonReferenceId IN (2,4)) t14 ON t14.RelationshipIdentity = t3.RelationshipId
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS radar

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS radar.VW_LE_Casestructure

# COMMAND ----------


spark.sql('select * from VW_LE_Casestructure').write.mode('overwrite').saveAsTable('radar.VW_LE_Casestructure')


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from VW_LE_CaseStructure where GCOBId='2629'
