# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To Achive Party Document details from GCOB for now
# MAGIC
# MAGIC #### author
# MAGIC - Sowmyashree.parashivamurthy.sudha@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Read Data from GCOB to further transform if needed
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC   |Developer   |   Date          |   PBI No.     |  Changes done|
# MAGIC   |---|--|---|---|
# MAGIC   |Sowmyashree | 25-July-2025    |   13024196    |  First release| 
# MAGIC   | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load
# MAGIC  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read Files from GDP

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
import os
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=3
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
party_document_object='Party_Documents'

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
load_df = [
'party_documents'
,'party_case_client_details'
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
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/3/data/{load_dts}/*.parquet"
)

# COMMAND ----------

# DBTITLE 1,GCOB Identification Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_Documents As
# MAGIC with Document as 
# MAGIC (
# MAGIC select distinct pd.*
# MAGIC ,case when c.CaseStatusName is null then 'RelatedParty' else c.CaseStatusName end as StatusName
# MAGIC FROM party_documents AS pd 
# MAGIC left join party_case_client_details c on pd.UniquePartyId = c.UniqueGcobId
# MAGIC WHERE pd.DocumentId IS NOT NULL
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC CONCAT('GCOB_', Doc.UniquePartyId) AS LocalSystemIdentifier,
# MAGIC Doc.ClientId,
# MAGIC Doc.DocumentId,
# MAGIC Doc.DocumentExpiryDate,
# MAGIC -- Doc.EntityType,
# MAGIC -- Doc.EntityId,
# MAGIC Doc.DocumentSubTypeName,
# MAGIC Doc.DocumentTypeName,
# MAGIC Doc.DocumentFileName,
# MAGIC Doc.DocumentStoreIdentifier,
# MAGIC Doc.UploadedBy,
# MAGIC Doc.UploadDate,
# MAGIC Doc.ExpiryDate,
# MAGIC Doc.Purpose
# MAGIC FROM Document AS Doc 
# MAGIC WHERE StatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,Union Data
# MAGIC %sql
# MAGIC Create or replace temporary view Party_Documents_Identifier As
# MAGIC select * From Gcob_Documents

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_documents_Identifier=spark.table('Party_Documents_Identifier')

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_party_documents_Identifier = add_party_identifier(df_party_documents_Identifier, df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 1,Convert DF to view
df_party_documents_Identifier.createOrReplaceTempView('Identifier')

# COMMAND ----------

# DBTITLE 1,Data Qulaity check
# MAGIC %sql
# MAGIC Create or replace temporary view Final_Party_documents_Identifier As
# MAGIC select * from Identifier where PartyIdentifier is not null

# COMMAND ----------

df_Final_Documents=spark.table('Final_Party_documents_Identifier')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
    save_to_saradar_storage_account(df_Final_Documents, party_document_object, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_Final_Documents, party_document_object, radar_datamodel_version_number, environment)
