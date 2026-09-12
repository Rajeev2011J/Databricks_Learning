# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To Achive Party Nationality and Citizenship details from GCOB, Legacy2 For now
# MAGIC
# MAGIC #### author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Read Data from GCOB and Legacy2 to further transform if needed
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC   - Developer   :   Date          :   PBI No.     :   Changes done
# MAGIC   - Abhishek    :   4-July-2025   :   13017598    :   First release 
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek | 4-July-2025 |13017598 |First release 
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load
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
party_dataobject='Party_CountryAffiliation'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(RANZ_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Deriving required date parameters
# Derive the date for which data has to be processed from GDP
load_dts = f"EDL_LOAD_DTS={Load_Date}*"
print (load_dts)

# COMMAND ----------

# DBTITLE 1,Read GCOB data from GDP defined layer
# print list of strings for loading spark dfs from GDP
load_df = [
'party_AllPartyDetails'
,'party_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read Legacy2 data from GDP defined layer
# List of datasets from GDP
load_df =[
'Legacy2_case_client_details',
'Legacy2_Nationality_Citizenship'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier dataobject
df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/1/data/{load_dts}/*.parquet")

# COMMAND ----------

load_df = [
'c_b_party_xref'
,'c_b_party_dom_cntry_xref']

for Dataobject in load_df:
    Read_GDP_Defined_DataObjects_RANZ('RANZ-MDM' , Dataobject, Load_Date)

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - Other Sources Data Prepration

# COMMAND ----------

# DBTITLE 1,GCOB Identification Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_CountryAffiliation As
# MAGIC with nat_cit as 
# MAGIC (
# MAGIC select distinct p.*
# MAGIC ,case when p.CaseStatusName is null then 'RelatedParty' else c.CaseStatusName end as StatusName
# MAGIC from party_AllPartyDetails p
# MAGIC LEFT JOIN party_case_client_details c on p.UniquePartyId = c.UniqueGcobId
# MAGIC where p.Status = 'Live'
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC concat('GCOB_', UniquePartyId) AS LocalSystemIdentifier,
# MAGIC 'Nationality' as NationalAffiliationType,
# MAGIC NationalityIsoCode as CountryISOcode
# MAGIC FROM nat_cit
# MAGIC WHERE NationalityIsoCode IS NOT NULL
# MAGIC and StatusName <> 'Cancelled'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC concat('GCOB_', UniquePartyId) AS LocalSystemIdentifier,
# MAGIC 'Citizenship' as NationalAffiliationType,
# MAGIC CitizenshipIsoCode as CountryISOcode
# MAGIC FROM nat_cit
# MAGIC WHERE CitizenshipIsoCode IS NOT NULL
# MAGIC and StatusName <> 'Cancelled'

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_CountryAffiliation AS
# MAGIC SELECT DISTINCT
# MAGIC concat('LEGACY2_',c.Legacy2_Identifier) as LocalSystemIdentifier,
# MAGIC 'Nationality' as NationalAffiliationType,
# MAGIC n.ClientNationalityIsoCode as CountryISOcode
# MAGIC FROM Legacy2_Nationality_Citizenship n
# MAGIC inner join Legacy2_client c on n.ClientId = c.ClientId
# MAGIC WHERE n.ClientNationalityIsoCode IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC concat('LEGACY2_',c.Legacy2_Identifier) as LocalSystemIdentifier,
# MAGIC 'Citizenship' as NationalAffiliationType,
# MAGIC cz.ClientCitizenshipIsoCode as CountryISOcode
# MAGIC FROM Legacy2_Nationality_Citizenship cz
# MAGIC inner join Legacy2_client c on cz.ClientId = c.ClientId
# MAGIC WHERE cz.ClientCitizenshipIsoCode IS NOT NULL

# COMMAND ----------

# DBTITLE 1,RANZ Identification Details
# MAGIC %sql
# MAGIC create or Replace temporary view RANZ_countryAffiliation as
# MAGIC  select  distinct
# MAGIC  CONCAT('RANZ_',cbp.PKEY_SRC_OBJECT) as LocalSystemIdentifier,
# MAGIC  null NationalAffiliationType,
# MAGIC  pdc.CNTRY_CD as CountryISOcode
# MAGIC  from c_b_party_xref cbp
# MAGIC  left join c_b_party_dom_cntry_xref pdc 
# MAGIC  on pdc.FK_PARTY_ID = cbp.ROWID_XREF

# COMMAND ----------

# DBTITLE 1,Union Data
# MAGIC %sql
# MAGIC Create or replace temporary view Party_CountryAffiliation As
# MAGIC select * From Gcob_CountryAffiliation
# MAGIC union
# MAGIC select * From Legacy2_CountryAffiliation
# MAGIC union
# MAGIC select * from RANZ_countryAffiliation

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_CountryAffiliation=spark.table('Party_CountryAffiliation')

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_party_CountryAffiliation = add_party_identifier(df_party_CountryAffiliation, df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 1,Convert DF to view
df_party_CountryAffiliation.createOrReplaceTempView('Affiliation')

# COMMAND ----------

# DBTITLE 1,Data Qulaity check
# MAGIC %sql
# MAGIC Create or replace temporary view Final_Party_Affiliation As
# MAGIC select distinct PartyIdentifier,NationalAffiliationType,CountryISOcode from Affiliation where PartyIdentifier is not null

# COMMAND ----------

# DBTITLE 1,Create final DF
df_Final_Affiliation=spark.table('Final_Party_Affiliation')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
    save_to_saradar_storage_account(df_Final_Affiliation, party_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_Final_Affiliation, party_dataobject, radar_datamodel_version_number, environment)
