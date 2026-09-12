# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To redesign GCOB consumer and create temp viewes which will be used thoughout the solution
# MAGIC
# MAGIC #### author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Read Data from GCOB and Legacy2 to further transform if needed
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal | 26-Aug-2026 |17043381 |First release 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os

import pandas as pd
from datetime import datetime, timedelta
#Local File Import
from GcobUtils import write_to_unity_catalog, authenticate_storage_account, read_gdp_defined_dataobjects

# COMMAND ----------

# DBTITLE 1,Date parameters
date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

# DBTITLE 1,Authentication
ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# DBTITLE 1,Read Gcob GDP defined objects
# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details',
'party_local_client_Owners',
"party_client",
"party_products_and_services",
"party_GcobUsers",
]

# Create TempView for each loading table
for Object in load_df:
    read_gdp_defined_dataobjects(source='GCOB', dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# DBTITLE 1,Read GCDS GDP defined data object
# Load GCDS Keystore
read_gdp_defined_dataobjects(source="GCDS", dataobject="client_KeyStoreKey")

# COMMAND ----------

# DBTITLE 1,Read Legacy2 GDP defined objects
# print list of strings for loading spark dfs from GDP
load_df = [
'Legacy2_risk',
"Legacy2_local_client_Owners",
"Legacy2_ClientStructure",
"Legacy2_case_client_details",
"Legacy2_products_and_services",
"Legacy2_client_identifiers",
]

# Create TempView for each loading table
for Object in load_df:
    read_gdp_defined_dataobjects(source='Legacy2', dataobject=Object,path_prefix='Legacy2')

# COMMAND ----------

# DBTITLE 1,Global Temp View For GCOB status
Id_list = [i+1 for i in range(9)]
Description_list = ['Prework'
, 'Ready for assessment'
, 'Assessment in progress'
, '4-EYE check'
, 'Sign-off'
, 'Fulfilment'
, 'Completed'
, 'Cancelled']
# create pyspark dataframe from lists
spark.createDataFrame(zip(Id_list, Description_list), ['PhaseID', 'PhaseName']).createOrReplaceGlobalTempView('gcob_static_PhaseLookup')

StatusId_list = [i + 1 for i in range(22)]
Name_list = ['Initiation In Progress'
, 'Ready for KYC assessment'
, 'KYC assessment in progress'
, 'Ready for 4 eye check'
, '4 eye check in progress'
, 'Client owner sign off requested'
, 'Client committee sign off requested'
, 'Product fulfilment in progress'
, 'Completed'
, 'Cancelled'
, 'Migrated'
, 'Ready for identification'
, 'Identification in progress'
, 'Ready for screening'
, 'Screening in progress'
, 'GCOB Review in progress'
, 'Client Owner approval requested'
, 'Local client owner sign off requested'
, 'Ready for Product Offboarding confirmation'
, 'Product Offboarding confirmation in progress'
, 'Product Offboarding in progress'
, 'Senior management sign off requested']
# create pyspark dataframe from lists
spark.createDataFrame(zip(StatusId_list, Name_list), ['StatusId', 'Name']).createOrReplaceGlobalTempView('gcob_static_CaseStatusType')


# COMMAND ----------

# DBTITLE 1,Identify Non SFDC Legacy2 clients
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from global_temp.Legacy2_case_client_details where isclient = 'True' and GcobCaseId is null and Value is null and GcobId like 'RA:%'; 

# COMMAND ----------

# DBTITLE 1,Create Global Temp View for Legacy2 Clients
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW Legacy2_client AS
# MAGIC select * 
# MAGIC ,case when clienttype='Legal Entity' then concat('LE_', Gcobid)
# MAGIC     when clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',Gcobid) else 'NA' end as UniqueGcobId
# MAGIC ,case when IsClient = 'true' and ClientTypeId = 1 then concat('L2_LEC_',ClientId)
# MAGIC       when IsClient = 'true' and ClientTypeId in (2,3) then concat('L2_NP_NPPC_',ClientId)
# MAGIC       when IsClient = 'false' and ClientTypeId = 1 then concat('L2_RLEP_',ClientId)
# MAGIC       when IsClient = 'false' and ClientTypeId in (2,3) then concat('L2_RNPP_',ClientId)
# MAGIC end as SourceClient
# MAGIC from global_temp.Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from global_temp.NonSFDCID_Legacy2_Client)
# MAGIC and ClientTypeId in (1,2,3) and IsClient = 'true'

# COMMAND ----------

# DBTITLE 1,Identify Protected Account GcobIds
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW Gcob_protectedClients AS
# MAGIC SELECT DISTINCT GcobId
# MAGIC ,case when ClientType = 'Legal Entity' then concat('LE_',GcobId) else concat('NP_NPPC_',GcobId) end as UniqueGcobId
# MAGIC FROM global_temp.party_case_client_details 
# MAGIC WHERE IsprotectedAccount = 'True'
# MAGIC
