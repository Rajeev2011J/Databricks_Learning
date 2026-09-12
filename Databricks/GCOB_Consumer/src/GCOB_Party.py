# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To redesign GCOB consumer Party tasks in alignment with new data model to load in unity catalog
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
from pyspark.sql.functions import lit
from datetime import datetime, timedelta

#Local File Import
from GcobUtils import write_to_unity_catalog, authenticate_storage_account, read_gdp_defined_dataobjects
catalog= os.environ['CATALOG']
schema = os.environ['GCOB_UC_SCHEMA']

# COMMAND ----------

# DBTITLE 1,Date parameters
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

# DBTITLE 1,Final Object Name
Party_dataobject = 'Party'

# COMMAND ----------

# DBTITLE 1,Combine Gcob and Legacy2
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Gcob_Legacy2_Clients AS
# MAGIC select distinct
# MAGIC cast(GcobId as string) as GcobId
# MAGIC ,case when ClientType = 'Legal Entity' then concat('LE_',GcobId) else concat('NP_NPPC_',GcobId) end as UniqueGcobId
# MAGIC ,'GCOB' as SourceSystem
# MAGIC ,FullLegalName 
# MAGIC ,ClientType as PartyType
# MAGIC from global_temp.party_case_client_details 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC cast(GcobId as string) as GcobId
# MAGIC ,UniqueGcobId
# MAGIC ,'Legacy2' as SourceSystem
# MAGIC ,FullLegalName 
# MAGIC ,ClientType as PartyType
# MAGIC from global_temp.Legacy2_client

# COMMAND ----------

# DBTITLE 1,Apply IsProtected Logic
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Party AS
# MAGIC select distinct
# MAGIC c.GcobId
# MAGIC ,c.UniqueGcobId
# MAGIC ,c.SourceSystem
# MAGIC ,case when p.UniqueGcobId is not null then CONCAT('protected account ', p.gcobid) else c.FullLegalName end as FullLegalName
# MAGIC ,c.PartyType
# MAGIC from Gcob_Legacy2_Clients c
# MAGIC left join global_temp.Gcob_protectedClients p on c.UniqueGcobId = p.UniqueGcobId
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create DataFrame to derive Business Date
df_Party=spark.table('Party')

# COMMAND ----------

# DBTITLE 1,Add BusinessDate column
df_Party=df_Party.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

# DBTITLE 1,Write to Unity Catalog
write_to_unity_catalog(df_Party,catalog,schema, Party_dataobject)
