# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal:
# MAGIC Export all Madrid offboardings from GCOB happened in 2019.
# MAGIC
# MAGIC #### Stakeholder
# MAGIC .. Reporting into Rob Steenbeeke
# MAGIC - agustin.fracchia@rabobank.com
# MAGIC
# MAGIC ##### Definitions
# MAGIC - Spain files: all with registered address or operating in Spain
# MAGIC - CIF: registration in the relevant chamber of commerce or tax authority. It is basically an identification number for that given client. As mentioned by the Spanish auditors, for Spanish clients that number is known as the CIF.
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - connect to all cases
# MAGIC - select all of type offboarding, compelted in 2019, and where address in spain
# MAGIC
# MAGIC ##### output
# MAGIC -  Name and surname or company name.
# MAGIC -  Date of registration. (onboarding)
# MAGIC -  Date of cancellation. (offboarding)
# MAGIC -  Number of accreditation document. (CIF number)
# MAGIC -  Related Product. (products used by the client)
# MAGIC -  Risk assigned to the client.
# MAGIC - Address
# MAGIC - Operating Address
# MAGIC - Risk Rating
# MAGIC - Client Owner
# MAGIC

# COMMAND ----------

import os
from datetime import datetime



# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

#Derive the date for which data has to be processes
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
Today = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'party_client'
, 'party_case_client_details'
, 'party_local_client_Owners'
,'party_products_and_sevices'
, 'party_GcobUsers'

]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{row.GDPname}/101/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW SPAIN_OFFBORDING_2019 AS
# MAGIC
# MAGIC   SELECT 
# MAGIC   UniqueGcobId,
# MAGIC   FullLegalName,
# MAGIC   SourceSystem,
# MAGIC   ReviewTypeName,
# MAGIC   CaseStatusName,
# MAGIC
# MAGIC   RegisteredCountryIsoCode,
# MAGIC   OperatingCountryIsoCode,
# MAGIC   IncorporationNumber,
# MAGIC   ValidatedRiskLevel,
# MAGIC   ClientApprovalDate,
# MAGIC   CaseCompletedDate,
# MAGIC   SourceClient
# MAGIC   
# MAGIC  FROM party_case_client_details
# MAGIC
# MAGIC  WHERE
# MAGIC     (ReviewTypeName = 'Product Offboarding'
# MAGIC     OR ReviewTypeName = 'Product Offboarding (Resume)')
# MAGIC AND CaseStatusName = 'Completed'
# MAGIC AND (RegisteredCountryIsoCode = 'ES' OR OperatingCountryIsoCode = 'ES')
# MAGIC --AND CaseCompletedDate BETWEEN '01-01-2019' AND '01-01-2020'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SPAIN_OFFBORDING_2019

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT 
# MAGIC   SourceClient,
# MAGIC   UniqueGcobId,
# MAGIC   FullLegalName,
# MAGIC   SourceSystem,
# MAGIC   ReviewTypeName,
# MAGIC   CaseStatusName,
# MAGIC
# MAGIC   RegisteredCountryIsoCode,
# MAGIC   OperatingCountryIsoCode,
# MAGIC   IncorporationNumber,
# MAGIC   ValidatedRiskLevel,
# MAGIC   ClientApprovalDate,
# MAGIC   CaseCompletedDate
# MAGIC   
# MAGIC   
# MAGIC  FROM party_case_client_details
# MAGIC
# MAGIC  WHERE
# MAGIC     ReviewTypeName = 'Product Offboarding (Resume)'
# MAGIC AND CaseStatusName = 'Completed'
# MAGIC AND (RegisteredCountryIsoCode = 'ES' OR OperatingCountryIsoCode = 'ES')
# MAGIC --AND CaseCompletedDate BETWEEN '01-01-2019' AND '01-01-2020'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_case_client_details limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct ReviewTypeName from party_case_client_details

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Legacy1 AS
# MAGIC -- check for LEgacy2 won't contain anything. Should be in Legacy1.
# MAGIC SELECT '1' AS NoClientsInTher

# COMMAND ----------


