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
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |13-Oct-2025 |13890287 |First release
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read Files from GDP

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
import os
from pyspark.sql.functions import regexp_extract
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=3
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
AU_GDP_Defined_Storage_Account=os.environ['AU_GDP_Defined_Storage_Account']
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
party_dataobject='Party_RadarKeyStore'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(SARADAR)
authenticate_storage_account(AU_GDP_Defined_Storage_Account)

# COMMAND ----------

# DBTITLE 1,Deriving required date parameters
# Derive the date for which data has to be processed from GDP
load_dts = f"EDL_LOAD_DTS={Load_Date}*"
print (load_dts)

# COMMAND ----------

# DBTITLE 1,Read GCDS data from GDP defined layer
# print list of strings for loading spark dfs from GDP
gcds_dataobjects_list = [
'client_KeyStoreKey'
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

# DBTITLE 1,Reading RANZ Data object
# List of dataobjects from GDP
RANZ_Dataobjects_List = [
'c_b_party'
,'c_b_contract'
,'c_b_contr_rol_party'
]

# Create TempView for each loading table
load_ranz_v4_rdm_tables(RANZ_Dataobjects_List, Load_Date)

# COMMAND ----------

# DBTITLE 1,CB T24
# MAGIC %sql
# MAGIC Create or replace temporary view MDM_CB_KeyStore As
# MAGIC select distinct 
# MAGIC CONCAT('RANZ_',Ranz_Party.SRC_PARTY_ID) as LocalSystemIdentifier
# MAGIC ,'CONTR_ID' as KeyStore_Type
# MAGIC ,Ranz_Contract.CONTR_ID  as KeyStore_Value
# MAGIC ,'Active' as Key_Status
# MAGIC from c_b_party Ranz_Party
# MAGIC left join c_b_contr_rol_party RANZ_rol on RANZ_rol.FK_PARTY_ID=Ranz_Party.ROWID_XREF AND RANZ_rol.ROLE_TYPE_CD ='PHOLD'
# MAGIC left join c_b_contract Ranz_Contract on Ranz_Contract.CONTR_ID=RANZ_rol.FK_CONTR_ID

# COMMAND ----------

# DBTITLE 1,ROS T24
# MAGIC %sql
# MAGIC Create or replace temporary view MDM_ROS_KeyStore As
# MAGIC select distinct 
# MAGIC CONCAT('RANZ_',Ranz_Party.SRC_PARTY_ID) as LocalSystemIdentifier
# MAGIC ,'RESERVED_CONTR_ID' as KeyStore_Type
# MAGIC ,Ranz_Contract.RESERVED_CONTR_ID  as KeyStore_Value
# MAGIC ,'Active' as Key_Status
# MAGIC from c_b_party Ranz_Party
# MAGIC left join c_b_contr_rol_party RANZ_rol on RANZ_rol.FK_PARTY_ID=Ranz_Party.ROWID_XREF AND RANZ_rol.ROLE_TYPE_CD ='PHOLD'
# MAGIC left join c_b_contract Ranz_Contract on Ranz_Contract.CONTR_ID=RANZ_rol.FK_CONTR_ID

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - Data Preparation

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_KeyStore As
# MAGIC select distinct
# MAGIC KeyStore_value
# MAGIC ,KeyStore_type
# MAGIC ,status
# MAGIC ,GCID
# MAGIC from client_KeyStoreKey
# MAGIC

# COMMAND ----------

# DBTITLE 1,GCDS Bank Codes
# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_RADAR_KeyStore As
# MAGIC SELECT DISTINCT
# MAGIC CONCAT('GCDS_',gcid) AS LocalSystemIdentifier
# MAGIC ,KeyStore_type as KeyStore_Type
# MAGIC ,KeyStore_value as KeyStore_Value
# MAGIC ,status as Key_Status
# MAGIC FROM GCDS_KeyStore

# COMMAND ----------

# DBTITLE 1,Union of MDM and GCDS keystore
# MAGIC %sql
# MAGIC create or replace temporary view RADAR_KeyStore as
# MAGIC Select * from MDM_CB_KeyStore
# MAGIC union
# MAGIC Select * from MDM_ROS_KeyStore
# MAGIC union
# MAGIC Select * from GCDS_RADAR_KeyStore

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_RADAR_KeyStore=spark.table('RADAR_KeyStore')

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject and then removing duplicates
df_RADAR_KeyStore = add_party_identifier(df_RADAR_KeyStore, df_Party_SystemIdentifier)
df_RADAR_KeyStore = df_RADAR_KeyStore.distinct()

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
    save_to_saradar_storage_account(df_RADAR_KeyStore, party_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_RADAR_KeyStore, party_dataobject, radar_datamodel_version_number, environment)
