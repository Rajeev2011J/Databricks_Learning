# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To redesign GCOB consumer Party Cases tasks in alignment with new data model to load in unity catalog
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
RefreshDate = (datetime.today()).strftime('%m/%d/%Y')

# COMMAND ----------

# DBTITLE 1,Final Object Name
PartyCase_dataobject = 'PartyCase'

# COMMAND ----------

# DBTITLE 1,Combine Gcob and Legacy2
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Gcob_Legacy2_Cases AS
# MAGIC With combine_gcob_Legacy2 AS
# MAGIC (
# MAGIC select distinct
# MAGIC 'GCOB' as SourceSystem
# MAGIC ,case when ClientType = 'Legal Entity' then concat('LE_',GcobId) else concat('NP_NPPC_',GcobId) end as UniqueGcobId
# MAGIC ,SourceClient
# MAGIC ,CaseId
# MAGIC ,CaseStatusName
# MAGIC ,ReviewTypeName
# MAGIC ,NextReviewDate
# MAGIC ,FinalDecisionDate
# MAGIC ,IsLatestApprovedVersionOfClient
# MAGIC ,CaseCompletedDate
# MAGIC from global_temp.party_case_client_details 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Legacy2' as SourceSystem
# MAGIC ,UniqueGcobId
# MAGIC ,SourceClient
# MAGIC ,null as CaseId
# MAGIC ,StatusTypeName as CaseStatusName
# MAGIC ,ReviewTypeName
# MAGIC ,NextReviewDate
# MAGIC ,FinalDecisionDate
# MAGIC ,IsLatestApprovedVersionOfClient
# MAGIC ,CaseCompletedDate
# MAGIC from global_temp.Legacy2_client
# MAGIC )
# MAGIC select distinct 
# MAGIC UniqueGcobId
# MAGIC ,SourceClient
# MAGIC ,CaseId
# MAGIC ,CaseStatusName
# MAGIC ,ReviewTypeName
# MAGIC ,NextReviewDate
# MAGIC ,FinalDecisionDate
# MAGIC ,IsLatestApprovedVersionOfClient
# MAGIC ,case when substr(SourceClient,0,3) = 'LEC' and SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(SourceClient,0,2) = 'NP' and SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(SourceClient,0,6) = 'L2_LEC' and SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(SourceClient,0,5) = 'L2_NP' and SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC ,CaseCompletedDate
# MAGIC From combine_gcob_Legacy2

# COMMAND ----------

# DBTITLE 1,Create DataFrame to derive Business Date
df_PartyCases=spark.table('Gcob_Legacy2_Cases')

# COMMAND ----------

# DBTITLE 1,Add BusinessDate column
df_PartyCases=df_PartyCases.withColumn("BusinessDate",lit(BusinessDate))
df_PartyCases=df_PartyCases.withColumn("RefreshDate",lit(RefreshDate))

# COMMAND ----------

# DBTITLE 1,Write to Unity Catalog
write_to_unity_catalog(df_PartyCases,catalog,schema, PartyCase_dataobject)
