# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To Achive Bank code for party for now, in future other identifiers will be added
# MAGIC
# MAGIC #### author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Read Data from GCDS and identify the bank codes
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
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
party_dataobject='Party_Selection'

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

# DBTITLE 1,Read GCDS data from GDP defined layer
# print list of strings for loading spark dfs from GDP
gcds_dataobjects_list = [
'client_Client',
'client_KeyStoreKey',
'client_PartyRole',
'client_PartytoPartyRelationship'
]

# Create TempView for each loading table
for dataobject in gcds_dataobjects_list:
    Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=dataobject,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier dataobject
df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/3/data/{load_dts}/*.parquet"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - Data Preparation

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_Clients As
# MAGIC select distinct
# MAGIC   k.KeyStore_value as identifier
# MAGIC , k.KeyStore_type
# MAGIC , k.status AS status
# MAGIC , k.bank_code
# MAGIC , c.*
# MAGIC , pr.Party_role
# MAGIC , pr.Life_cycle_status
# MAGIC , rel.`Relationship-Type` as RelationshipType
# MAGIC , rel.`Relationship-Value` as RelationshipValue_GCID
# MAGIC , case when c.Party_type <> 'Natural Person' then c.Full_legal_name else c.Person_Name end Full_Name
# MAGIC from client_KeyStoreKey k
# MAGIC inner join client_Client c on c.GCID = k.GCID
# MAGIC left join client_PartyRole pr on k.GCID = pr.GCID
# MAGIC left join client_PartytoPartyRelationship rel on k.GCID = rel.GCID

# COMMAND ----------

# DBTITLE 1,GCDS Bank Codes
# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_BankCode_Selection As
# MAGIC SELECT DISTINCT
# MAGIC CONCAT('GCDS_',gcid) AS LocalSystemIdentifier,
# MAGIC 'bank_code' as SelectionType,
# MAGIC bank_code as SelectionValue
# MAGIC FROM GCDS_Clients
# MAGIC WHERE bank_code IS NOT NULL

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_Selection=spark.table('GCDS_BankCode_Selection')

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_party_Selection = add_party_identifier(df_party_Selection, df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 1,Convert DF to view
df_party_Selection.createOrReplaceTempView('PartySelection')

# COMMAND ----------

# DBTITLE 1,Data Qulaity check
# MAGIC %sql
# MAGIC Create or replace temporary view Party_Selection As
# MAGIC select * from PartySelection where PartyIdentifier is not null

# COMMAND ----------

# DBTITLE 1,Create final DF
df_Final_Selection=spark.table('Party_Selection')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
    save_to_saradar_storage_account(df_Final_Selection, party_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_Final_Selection, party_dataobject, radar_datamodel_version_number, environment)
