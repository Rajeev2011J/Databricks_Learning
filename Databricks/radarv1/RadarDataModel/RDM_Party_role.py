# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To Achive Data Model result by considering All the source systems where W&R Clients are available and GCDS source
# MAGIC
# MAGIC #### author
# MAGIC - Aayushi.jain@rabobank.nl
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Take W&R clients data from different source systems
# MAGIC - Take GCDS data
# MAGIC - Join on GCDSId to further derive required output
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------
# MAGIC | Aayushi Jain | 19-JAN-2026 | 14630418 | [Radar Data Model] - Create  new object - Party_Role

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read Files from GDP

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
import os
# from pyspark.sql.functions import regexp_extract
from pyspark.sql import SparkSession
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
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
party_dataobject='Party_Role'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Define Date variables
#Derive the date for which data has to be processes
# import pandas as pd
from datetime import datetime, timedelta
# from pyspark.dbutils import DBUtils
load_dts = f"EDL_LOAD_DTS={Load_Date}*"
print (load_dts)

# COMMAND ----------

# DBTITLE 1,Read GCDS data from GDP defined layer
# print list of strings for loading spark dfs from GDP
gcds_object_list = [
'client_KeyStoreKey',
'client_PartyRole',
"client_Client"
]

# Create TempView for each loading table
for dataobject in gcds_object_list:
    Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=dataobject,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read GIC data from GDP defined layer
# print list of strings for loading spark dfs from GDP
gic_object_list = [
'pessoa'
,'vwgic_rdl_pessoa_tipo_cadastro'
]
#gic_load_dts
# Create TempView for each loading table
for dataobject in gic_object_list:
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=dataobject,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier and Party dataobject
df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/1/data/{load_dts}/*.parquet"
)
df_Party = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party/2/data/{load_dts}/*.parquet"
)

# COMMAND ----------

# DBTITLE 1,Party: filtering for GIC and GCDS, selecting required attributes
df_Party_filter = df_Party.filter((df_Party.PartyIdentifier.like('GCDS%')) | (df_Party.PartyIdentifier.like('GIC%'))).select("PartyIdentifier")

# COMMAND ----------

# DBTITLE 1,GIC Clients update to join
# MAGIC %sql
# MAGIC Create or replace temporary view GIC_ClientDetails As
# MAGIC select distinct
# MAGIC p.COD_INSTITUCIONAL
# MAGIC ,CONCAT('GIC_', p.COD_INSTITUCIONAL) as LocalSystemIdentifier
# MAGIC ,c.DES_TIPO_CADASTRO as PartyRoleType
# MAGIC ,c.DTA_CADASTRO AS PartyRoleLifecycleStartDate
# MAGIC ,c.DTA_EFETIVACAO AS PartyRoleLifecycleChangeDate
# MAGIC ,c.DES_STATUS_TIPO_CADASTRO as PartyLifeCycleStatus
# MAGIC from pessoa p
# MAGIC LEFT JOIN vwgic_rdl_pessoa_tipo_cadastro c on p.COD_INSTITUCIONAL = c.COD_INSTITUCIONAL

# COMMAND ----------

# DBTITLE 1,Read GCDS data
# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_Clients As
# MAGIC select distinct
# MAGIC client_keystore.gcid
# MAGIC , client_keystore.KeyStore_value as identifier
# MAGIC , client_keystore.KeyStore_type
# MAGIC , client_partyrole.Party_role AS PartyRoleType 
# MAGIC , client_partyrole.`PartyRole-StartDate` AS PartyRoleLifecycleStartDate
# MAGIC , client_partyrole.`PartyRole-ChangeDate` AS PartyRoleLifecycleChangeDate
# MAGIC , client_partyrole.Life_cycle_status as PartyLifeCycleStatus 
# MAGIC from client_KeyStoreKey client_keystore 
# MAGIC inner join client_Client client on client.GCID = client_keystore.GCID
# MAGIC left join client_PartyRole client_partyrole on client_keystore.GCID = client_partyrole.GCID

# COMMAND ----------

# DBTITLE 1,Read gcds & gic data to generate local identifier
# MAGIC %sql
# MAGIC Create or replace temporary view party As
# MAGIC select distinct
# MAGIC CASE WHEN gcds_client.identifier = gic_clientdetails.COD_INSTITUCIONAL AND gcds_client.KeyStore_type = 'GIC' THEN gic_clientdetails.LocalSystemIdentifier
# MAGIC      else CONCAT('GCDS_',gcds_client.gcid)END AS LocalSystemIdentifier
# MAGIC ,COALESCE(gcds_client.PartyRoleType,gic_clientdetails.PartyRoleType) as PartyRoleType
# MAGIC ,COALESCE(gcds_client.PartyRoleLifecycleStartDate,gic_clientdetails.PartyRoleLifecycleStartDate) as PartyRoleLifecycleStartDate
# MAGIC ,COALESCE(gcds_client.PartyRoleLifecycleChangeDate,gic_clientdetails.PartyRoleLifecycleChangeDate) as PartyRoleLifecycleChangeDate
# MAGIC ,COALESCE(gcds_client.PartyLifeCycleStatus,gic_clientdetails.PartyLifeCycleStatus) as PartyLifeCycleStatus
# MAGIC from GCDS_Clients gcds_client
# MAGIC left join GIC_ClientDetails gic_clientdetails
# MAGIC on gcds_client.identifier = gic_clientdetails.COD_INSTITUCIONAL
# MAGIC and gcds_client.KeyStore_type = 'GIC'

# COMMAND ----------

# DBTITLE 1,Unique Local System Identifier
# MAGIC %sql
# MAGIC Create or replace temporary view NonGCDS_UniqueParty As
# MAGIC select distinct gic_clientdetails.LocalSystemIdentifier,  gic_clientdetails.PartyRoleType
# MAGIC      ,gic_clientdetails.PartyRoleLifecycleStartDate
# MAGIC      ,gic_clientdetails.PartyRoleLifecycleChangeDate
# MAGIC      ,gic_clientdetails.PartyLifeCycleStatus
# MAGIC from GIC_ClientDetails gic_clientdetails
# MAGIC left anti join party gcds_party
# MAGIC on gic_clientdetails.LocalSystemIdentifier = gcds_party.LocalSystemIdentifier

# COMMAND ----------

# DBTITLE 1,Union GCDS and GIC
df_party_role_gic_gcds= spark.sql("""select *
from party
union
select *
from NonGCDS_UniqueParty""")


# COMMAND ----------

# DBTITLE 1,Add party identifier
df_party_role_ = add_party_identifier(df_party_role_gic_gcds, df_Party_SystemIdentifier)


# COMMAND ----------

# DBTITLE 1,party and party_role join to extract role data
df_party_role = df_Party_filter.join(df_party_role_, "PartyIdentifier", how="left").distinct()

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
    save_to_saradar_storage_account(df_party_role, party_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_party_role, party_dataobject, radar_datamodel_version_number, environment)
