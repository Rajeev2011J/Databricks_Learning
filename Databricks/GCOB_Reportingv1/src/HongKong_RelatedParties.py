# Databricks notebook source
# MAGIC %md
# MAGIC ## Goal
# MAGIC - To get related party details for HongKong and transfer the file to required location. Do not store output in catalog.
# MAGIC
# MAGIC ## Author
# MAGIC - Abhishek
# MAGIC
# MAGIC ### Flow of logic
# MAGIC - For GCOB
# MAGIC   - Get the GCO, Product and Booking location for HongKong
# MAGIC   - Join with PartyClientStructure_GUI and Party_AllPartyDeatails Live to get related party details
# MAGIC
# MAGIC ##### Expected output
# MAGIC columns:
# MAGIC   - GcobId
# MAGIC   - GCOB_ClickedEntityName
# MAGIC   - GCOB_Type
# MAGIC   - GCOB_ClientOwner
# MAGIC   - GCOB_ChineseName
# MAGIC   - RelatedPartyId
# MAGIC   - GCOB_ClickedName
# MAGIC   - GCOB_ClickedName_ChineseName
# MAGIC   - GCOB_PEPStatus
# MAGIC   - GCOB_FirstName
# MAGIC   - GCOB_LastName
# MAGIC   - GCOB_DOB
# MAGIC
# MAGIC
# MAGIC ## Updated By
# MAGIC
# MAGIC ## Updated On Date
# MAGIC

# COMMAND ----------

import os

from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime, timedelta
import shutil

#Local File Import
from RadarUtils import *

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

# COMMAND ----------

SALZReadStorage = os.environ['SALZReadStorage']
if SALZReadStorage.lower()!='salandingzonefecradarprd':
    print("Execution stopped Storage account does not exist")
    dbutils.notebook.exit("Terminated early: Non-prod storage account")

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details'
,'party_products_and_services'
,'party_client_structure_GUI'
,'party_AllPartyDetails'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HongKong_Clients AS
# MAGIC SELECT DISTINCT pc.*
# MAGIC FROM party_case_client_details AS pc
# MAGIC INNER JOIN party_products_and_services AS ps ON pc.SourceClient = ps.SourceClient
# MAGIC WHERE 
# MAGIC   pc.globalclientownerlocation IN ('Rabobank Hong Kong') 
# MAGIC   OR ps.ProductOfferingLocation IN ('Rabobank Hong Kong') 
# MAGIC   OR ps.BookingEntityLocation IN ('Rabobank Hong Kong')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HongKong_RelatedParties AS
# MAGIC select distinct
# MAGIC s.ClientGcobId as GcobId
# MAGIC ,s.ClientFullLegalName as GCOB_ClickedEntityName
# MAGIC ,'GCOB' as GCOB_Type
# MAGIC ,c.GlobalClientOwner as GCOB_ClientOwner
# MAGIC ,c.FullLegalNameInLocalLanguage as GCOB_ChineseName
# MAGIC ,s.ParentIdentity as RelatedPartyId
# MAGIC ,s.ParentIdentityName as GCOB_ClickedName
# MAGIC ,p.FullLegalNameInLocalLanguage as GCOB_ClickedName_ChineseName
# MAGIC ,s.ParentPEP as GCOB_PEPStatus
# MAGIC --,p.PEPStatus
# MAGIC ,s.ParentFirstName as GCOB_FirstName
# MAGIC ,s.ParentLastName as GCOB_LastName
# MAGIC ,p.DateOfBirth as GCOB_DOB
# MAGIC ,c.ClientLifeCycleName as GCOB_ClientLifeCycleStatus
# MAGIC From party_client_structure_GUI s
# MAGIC inner join HongKong_Clients c on s.SourceClient = c.SourceClient
# MAGIC left join party_AllPartyDetails p on s.UniqueParentPartyId = p.UniquePartyId and p.status = 'Live'
# MAGIC where s.IsLatestApprovedVersionOfClient = 'True'

# COMMAND ----------

df_HongKong_RelatedParties = spark.table('HongKong_RelatedParties')

# COMMAND ----------

# Removing the yesterday files in live folder
dbutils.fs.rm(f'abfss://hongkong-relatedparties@{SALZReadStorage}.dfs.core.windows.net/live/', recurse=True)

# COMMAND ----------

# Get the current date

current_date = datetime.now().strftime("%Y-%m-%d")

# Define the file name dynamically

file_name = f"GCOB HK Extracts Report_{current_date}.csv"

# Construct the full file path in SA Landing zone 

sa_path = f"abfss://hongkong-relatedparties@{SALZReadStorage}.dfs.core.windows.net/live/{file_name}"
sa_path_hist =f"abfss://hongkong-relatedparties@{SALZReadStorage}.dfs.core.windows.net/history/{file_name}"


# Define a temporary directory in DBFS
temp_dir = f"dbfs:/tmp/hongkong_relatedparties_temp"

# Write the DataFrame to the temporary directory in DBFS
df_HongKong_RelatedParties.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(temp_dir)

# List files in the temporary directory
files = dbutils.fs.ls(temp_dir)

# Find the part file in the temporary directory
for file in files:
    if file.name.startswith("part-") and file.name.endswith(".csv"):
        temp_file_path = file.path
        break

# Rename the part file to the desired file name
final_temp_path = os.path.join(temp_dir, file_name)
dbutils.fs.mv(temp_file_path, final_temp_path)

# Move the renamed file to the final destination in SA landing zone

dbutils.fs.cp(final_temp_path, sa_path)
dbutils.fs.cp(final_temp_path, sa_path_hist)

# Clean up the temporary directory
dbutils.fs.rm(temp_dir, recurse=True)
