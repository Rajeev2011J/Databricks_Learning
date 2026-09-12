# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To redesign GCOB consumer Party Case Ownership tasks in alignment with new data model to load in unity catalog
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
catalog= os.environ['CATALOG']
schema = os.environ['GCOB_UC_SCHEMA']

# COMMAND ----------

# DBTITLE 1,Final Object Name
PartyCaseOwnership_dataobject = 'PartyCaseOwnership'

# COMMAND ----------

# DBTITLE 1,Combine Gcob and Legacy2
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Gcob_Legacy2_GlobalOwner AS
# MAGIC select distinct
# MAGIC SourceClient
# MAGIC ,'Global' as OwnerType
# MAGIC ,GlobalClientOwner
# MAGIC ,GlobalClientOwnerLocation
# MAGIC from global_temp.party_case_client_details 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC SourceClient
# MAGIC ,'Global' as OwnerType
# MAGIC ,GlobalClientOwner
# MAGIC ,GlobalClientOwnerLocation
# MAGIC from global_temp.Legacy2_client

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW CaseOwner AS
# MAGIC select distinct
# MAGIC SourceClient
# MAGIC ,OwnerType
# MAGIC ,GlobalClientOwner as OwnerName
# MAGIC ,GlobalClientOwnerLocation as OwnerLocation
# MAGIC from Gcob_Legacy2_GlobalOwner
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC l.SourceClient
# MAGIC ,l.OwnerType
# MAGIC ,l.LocalClientOwnerName as OwnerName
# MAGIC ,l.Location as OwnerLocation
# MAGIC from global_temp.party_local_client_Owners l
# MAGIC left join global_temp.party_case_client_details c on l.SourceClient = c.SourceClient

# COMMAND ----------

# DBTITLE 1,Create DataFrame to write to Unity Catalog
df_PartyOwner=spark.table('CaseOwner')

# COMMAND ----------

# DBTITLE 1,Write to Unity Catalog
write_to_unity_catalog(df_PartyOwner,catalog,schema, PartyCaseOwnership_dataobject)
