# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To Achive Party Identification Document details from GCOB, Legacy2 For now
# MAGIC
# MAGIC #### author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC - Prajit.tatari@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Read Data from GCOB and Legacy2 to further transform if needed
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |3-July-2025 |12950919 |First release
# MAGIC | Prajit	 |11-Aug-2025 |13353847 |Bug Fixes
# MAGIC | Abhishek Jaiswal	 |16-Sep-2025 |13584702 |Added CountryOfIssueIsoCode
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load
# MAGIC
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
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
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
party_dataobject='Party_Identification'

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

# DBTITLE 1,Read GCOB data from GDP defined layer
# print list of strings for loading spark dfs from GDP
load_df =[
'party_RelatedPartyIdentificationDocument'
,'party_case_client_details'
,'party_allcountries'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read Legacy2 data from GDP defined layer
# List of datasets from GDP
load_df = [
'Legacy2_ClientIdentification',
'Legacy2_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier dataobject
df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/1/data/{load_dts}/*.parquet"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - Other Sources Data Prepration

# COMMAND ----------

# DBTITLE 1,GCOB Identification Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_IdentificationDocument As
# MAGIC with Indentification as 
# MAGIC (
# MAGIC select distinct Ident.*
# MAGIC ,case when c.CaseStatusName is null then 'RelatedParty' else c.CaseStatusName end as StatusName
# MAGIC FROM party_RelatedPartyIdentificationDocument AS Ident 
# MAGIC inner join (select distinct UniquePartyId, max(partyid) as Partyid from party_RelatedPartyIdentificationDocument
# MAGIC where expired is null
# MAGIC group by UniquePartyId) identification
# MAGIC on Ident.UniquePartyId = identification.UniquePartyId and Ident.Partyid = identification.Partyid
# MAGIC left join party_case_client_details c on Ident.UniquePartyId = c.UniqueGcobId
# MAGIC WHERE Ident.IdType IS NOT NULL and identification.UniquePartyId is not null
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC CONCAT('GCOB_', Ident.UniquePartyId) AS LocalSystemIdentifier,
# MAGIC 'GCOB' as Application,
# MAGIC Ident.IdType,
# MAGIC Ident.DescriptionOfOtherIdentificationType as IdTypeOtherDescription,
# MAGIC Ident.IdNumber,
# MAGIC Ident.PlaceOfIssue,
# MAGIC Ident.CountryOfIssue,
# MAGIC to_date(Ident.IssueDate, 'dd-MM-yyyy') as IssueDate,
# MAGIC to_date(Ident.ExpiryDate, 'dd-MM-yyyy') as ExpiryDate
# MAGIC FROM Indentification AS Ident 
# MAGIC WHERE StatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,Legacy2 Identification Details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%';
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select * 
# MAGIC ,CASE
# MAGIC   WHEN IsClient = 'true' and  ClientTypeId = 1 THEN concat('LE_', GcobId)
# MAGIC   WHEN IsClient = 'true' and ClientTypeId in (2,3) THEN concat('NP_NPPC_', GcobId)
# MAGIC   WHEN IsClient = 'false' and ClientTypeId = 1 THEN concat('RLE_', GcobId)
# MAGIC   WHEN IsClient = 'false' and ClientTypeId in (2,3) THEN concat('RNP_', GcobId)
# MAGIC END as Legacy2_Identifier
# MAGIC from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_IdentificationDocument AS
# MAGIC select distinct
# MAGIC concat('LEGACY2_',c.Legacy2_Identifier) as LocalSystemIdentifier,
# MAGIC 'Legacy2' as Application,
# MAGIC Ident.IdType,
# MAGIC null as IdTypeOtherDescription,
# MAGIC Ident.ContactIdentificationNumber as IdNumber,
# MAGIC Ident.ContactPlaceOfIdIssue as PlaceOfIssue,
# MAGIC null as CountryOfIssue,
# MAGIC -- Ident.ContactDateOfIdIssue as IssueDate,
# MAGIC -- Ident.ContactIdExpiryDate as ExpiryDate
# MAGIC to_date(Ident.ContactDateOfIdIssue, 'dd-MM-yyyy') as IssueDate,
# MAGIC to_date(Ident.ContactIdExpiryDate, 'dd-MM-yyyy') as ExpiryDate
# MAGIC from Legacy2_ClientIdentification Ident
# MAGIC inner join Legacy2_client c on Ident.id = c.ClientId

# COMMAND ----------

# DBTITLE 1,Union Data
# MAGIC %sql
# MAGIC Create or replace temporary view Party_Identifier As
# MAGIC select * From Gcob_IdentificationDocument
# MAGIC union
# MAGIC select * From Legacy2_IdentificationDocument

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_Identifier=spark.table('Party_Identifier')

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_party_Identifier = add_party_identifier(df_party_Identifier, df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 1,Convert DF to view
df_party_Identifier.createOrReplaceTempView('Identifier')

# COMMAND ----------

# DBTITLE 1,Data Qulaity check
# MAGIC %sql
# MAGIC Create or replace temporary view Final_Party_Identifier As
# MAGIC with non_legacy2_duplicates as (
# MAGIC select distinct * from Identifier 
# MAGIC where PartyIdentifier is not null
# MAGIC qualify rank() over(partition by PartyIdentifier order by Application) = 1
# MAGIC )
# MAGIC ,all_countries as (
# MAGIC   select distinct Name as CountryName, ISOCode from party_allcountries
# MAGIC order by CountryName
# MAGIC )
# MAGIC select distinct
# MAGIC d.*
# MAGIC ,c.ISOCode as CountryOfIssueISOCode
# MAGIC from non_legacy2_duplicates d
# MAGIC left join all_countries c on d.CountryOfIssue = c.CountryName
# MAGIC qualify row_number() over(partition by PartyIdentifier,IDNumber order by IDNumber, issuedate desc) = 1

# COMMAND ----------

df_Final_Identifier=spark.table('Final_Party_Identifier')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account

if RunType == "historical":
    save_to_saradar_storage_account(df_Final_Identifier, party_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_Final_Identifier, party_dataobject, radar_datamodel_version_number, environment)
