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
# MAGIC | Abhishek Jaiswal | 08-MAY-2026 | 16070041 | Update Radar Data Model Party_Role object from RANZ V4
# MAGIC |Hari | 23-June-2026 |16314785 | RANZ Lookup Table Handling Enhancement]

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
radar_datamodel_version_number=3
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
SARADAR = "saradar" + environment
RANZ_ReadStorage = os.environ['AU_GDP_Defined_Storage_Account']

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
authenticate_storage_account(RANZ_ReadStorage)

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
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/3/data/{load_dts}/*.parquet"
)
df_Party = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party/3/data/{load_dts}/*.parquet"
)

# COMMAND ----------

# DBTITLE 1,Party: filtering for GIC and GCDS, selecting required attributes
df_Party_filter = df_Party.filter((df_Party.PartyIdentifier.like('GCDS%')) | (df_Party.PartyIdentifier.like('GIC%'))).select("PartyIdentifier")

# COMMAND ----------

# DBTITLE 1,Read RANZ data from GDP defined
load_df = [
'c_b_party_xref'
,'c_b_contr_rol_party_xref'
,'c_b_contract_xref'
,'C_LKP_PRTY_RELT_ROL_TYPE'
,'c_lkp_client_life_cycle'
]

#for Dataobject in load_df:
load_ranz_v4_rdm_tables(load_df, Load_Date)#.createOrReplaceTempView(Dataobject)

# COMMAND ----------

# DBTITLE 1,Outdated RANZ_Client_Lifecycle_Status_Mapping
# Ranz_Pep_Evaluation_Static_Dict ={'01'	:'PEP',
# '02':	'Close associate of a PEP',
# '03':	'Immediate family member of a PEP',
# '04':	'Not a PEP'}

# sdf_Ranz_Pep_Evaluation_Static_Mapping =spark.createDataFrame([(code, description) for code, description in Ranz_Pep_Evaluation_Static_Dict.items()],['PEP_EVAL_CD','PEP_EVAL_DESCRIPTION'])
# sdf_Ranz_Pep_Evaluation_Static_Mapping.createOrReplaceTempView('Ranz_Pep_Evaluation_Static_Mapping')

# Ranz_Client_Lifecycle_Status_Dict = {
#     "-2": "UNKNOWN",
#     "AC": "Active Client",
#     "ACPR": "Active-Pending Risk Client",
#     "AP": "Applicant Client",
#     "BL": "Blocked Client",
#     "BL01": "Blocked - Post No Debits",
#     "BL02": "Blocked - Post No Credits",
#     "BL03": "Blocked - Post No Entries",
#     "BL04": "Blocked - Pending Documentation",
#     "BL05": "Blocked - Deceased Estate",
#     "BL06": "Blocked - Closure Quoted",
#     "BL07": "Blocked - Account on Referral List",
#     "BL08": "Blocked - Account on Referral List - CR",
#     "BL09": "Blocked - Account on Referral List - DR",
#     "BL10": "Blocked - Post Credits to Savings a/c",
#     "BL11": "Blocked - Post Debits to Current a/c",
#     "BL12": "Blocked - Customer Deceased",
#     "BL20": "Blocked - Hold Debits: Funds Held as Security",
#     "BL50": "Blocked - Setting up of company",
#     "BL51": "Blocked - Overdraw not allowed",
#     "BL52": "Blocked - Management authorisation",
#     "BL53": "Blocked - General Debit Block",
#     "BL54": "Blocked - General DR & CR block",
#     "BL55": "Blocked - Several blocking codes",
#     "BL56": "Blocked - Judicial instructions",
#     "BL57": "Blocked - Missing documents",
#     "BL58": "Blocked - Temporary blocking",
#     "BL59": "Blocked - Multiple blocking",
#     "BL60": "Blocked - ATO/IRD request",
#     "BL61": "Blocked - Litigation",
#     "BL62": "Blocked - Centrelink",
#     "BL63": "Blocked - Pending client onboarding",
#     "BL64": "Blocked - Incomplete CDD",
#     "BL65": "Blocked - Client request block",
#     "BL66": "Blocked - Multiple blocking",
#     "BL67": "Blocked - Inactive customer",
#     "BL68": "Blocked - Overdue remediation",
#     "BL69": "Blocked - Bulk account closure remove",
#     "BL70": "Blocked - Account being closed",
#     "BL71": "Blocked - Pending Client exit",
#     "BL72": "Blocked - Pending Client exit DB",
#     "BL73": "Blocked - Pending Client exit CR",
#     "BL74": "Blocked - Fraud",
#     "BL75": "Blocked - For failed authentication",
#     "BL76": "Blocked - For disabled end user",
#     "BL77": "Blocked - Login Cancelled",
#     "BL78": "Blocked - Login Not Used",
#     "BL79": "Blocked - Login Cancelled - 1 to 7 years",
#     "BL80": "Blocked - Login Cancelled - 0 to 1 years",
#     "BL90": "Blocked - Automatic Closure",
#     "BL91": "Blocked - Temp Auto Closing",
#     "BL99": "Blocked - Account Closure",
#     "CL": "Closed Client",
#     "DC": "Declined Client",
#     "DL": "Deleted",
#     "PR": "Prospect Client",
#     "WD": "Withdrawn Client",
#     "WD01": "Withdrawn - Lost to competitor",
#     "WD02": "Withdrawn - Structural conditions could not be met",
#     "WD03": "Withdrawn - Not proceeding with funding request",
#     "WD04": "Withdrawn - Time constraints",
#     "WD05": "Withdrawn - To resubmit",
#     "WD06": "Withdrawn - Other",
#     "WD07": "Withdrawn - To apply for low cost/no fee account",
#     "DC01": "Declined - Credit declined",
#     "DC02": "Declined - Unacceptable AML",
#     "DC03": "Declined - Other"
# }

# sdf_RANZ_Client_Lifecycle_Status_Mapping = spark.createDataFrame([(code, desc) for code, desc in Ranz_Client_Lifecycle_Status_Dict.items()],["LIFECYCLE_STATUS_CD", "CLIENT_LIFECYCLE_STATUS_DESCRIPTION"])

# sdf_RANZ_Client_Lifecycle_Status_Mapping.createOrReplaceTempView("RANZ_Client_Lifecycle_Status_Mapping")

# COMMAND ----------

# DBTITLE 1,RANZ Clients update to join
# MAGIC %sql
# MAGIC create or Replace Temporary view Ranz_ClientDetails as
# MAGIC select distinct
# MAGIC CONCAT('RANZ_',cbp.PKEY_SRC_OBJECT) LocalSystemIdentifier
# MAGIC ,cbp.SRC_PARTY_ID
# MAGIC ,'RANZ' as Application
# MAGIC ,rt.PRTY_RELT_ROLE_TYPE_DESC as PartyRoleType
# MAGIC ,crp.START_DT as PartyRoleLifecycleStartDate
# MAGIC ,crp.END_DT as PartyRoleLifecycleChangeDate
# MAGIC -- ,Lifecycle_Status.CLIENT_LIFECYCLE_STATUS_DESCRIPTION as PartyLifeCycleStatus  
# MAGIC ,Lifecycle_Status.CLIENT_LIFE_CYCLE_DESC as PartyLifeCycleStatus
# MAGIC FROM c_b_party as cbp
# MAGIC INNER JOIN c_b_contr_rol_party as crp
# MAGIC     ON crp.FK_PARTY_ID = cbp.ROWID_XREF
# MAGIC lEFT JOIN c_b_contract Ranz_Contract on Ranz_Contract.CONTR_ID=crp.FK_CONTR_ID
# MAGIC -- lEFT JOIN RANZ_Client_Lifecycle_Status_Mapping Lifecycle_Status ON Lifecycle_Status.LIFECYCLE_STATUS_CD=Ranz_Contract.LIFECYCLE_STATUS_CD
# MAGIC left join c_lkp_client_life_cycle Lifecycle_Status ON Lifecycle_Status.CLIENT_LIFE_CYCLE_CODE=Ranz_Contract.LIFECYCLE_STATUS_CD
# MAGIC left join C_LKP_PRTY_RELT_ROL_TYPE rt on rt.PRTY_RELT_ROLE_TYPE_CODE=crp.ROLE_TYPE_CD

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
# MAGIC CASE 
# MAGIC      WHEN gcds_client.identifier = gic_clientdetails.COD_INSTITUCIONAL AND gcds_client.KeyStore_type = 'GIC' THEN gic_clientdetails.LocalSystemIdentifier
# MAGIC      WHEN gcds_client.identifier = ranz.SRC_PARTY_ID AND gcds_client.KeyStore_type = 'CB RANZ' THEN ranz.LocalSystemIdentifier
# MAGIC      else CONCAT('GCDS_',gcds_client.gcid)
# MAGIC END AS LocalSystemIdentifier
# MAGIC ,COALESCE(gcds_client.PartyRoleType,gic_clientdetails.PartyRoleType,ranz.PartyRoleType) as PartyRoleType
# MAGIC ,COALESCE(gcds_client.PartyRoleLifecycleStartDate,gic_clientdetails.PartyRoleLifecycleStartDate,ranz.PartyRoleLifecycleStartDate) as PartyRoleLifecycleStartDate
# MAGIC ,COALESCE(gcds_client.PartyRoleLifecycleChangeDate,gic_clientdetails.PartyRoleLifecycleChangeDate,ranz.PartyRoleLifecycleChangeDate) as PartyRoleLifecycleChangeDate
# MAGIC ,COALESCE(gcds_client.PartyLifeCycleStatus,gic_clientdetails.PartyLifeCycleStatus,ranz.PartyLifeCycleStatus) as PartyLifeCycleStatus
# MAGIC from GCDS_Clients gcds_client
# MAGIC left join GIC_ClientDetails gic_clientdetails
# MAGIC on gcds_client.identifier = gic_clientdetails.COD_INSTITUCIONAL
# MAGIC and gcds_client.KeyStore_type = 'GIC'
# MAGIC left join Ranz_ClientDetails ranz on gcds_client.identifier = ranz.SRC_PARTY_ID 
# MAGIC      and gcds_client.KeyStore_type = 'CB RANZ'   

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
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct 
# MAGIC ranz.LocalSystemIdentifier
# MAGIC ,ranz.PartyRoleType
# MAGIC ,ranz.PartyRoleLifecycleStartDate
# MAGIC ,ranz.PartyRoleLifecycleChangeDate
# MAGIC ,ranz.PartyLifeCycleStatus
# MAGIC From Ranz_ClientDetails ranz
# MAGIC left anti join party gcds_party
# MAGIC on ranz.LocalSystemIdentifier = gcds_party.LocalSystemIdentifier

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
