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
from datetime import datetime, timedelta

#Local File Import
from GcobUtils import write_to_unity_catalog, authenticate_storage_account, read_gdp_defined_dataobjects
catalog= os.environ['CATALOG']
schema = os.environ['GCOB_UC_SCHEMA']

# COMMAND ----------

# DBTITLE 1,Final Object Name
PartyCaseRiskCategories_dataobject = 'PartyCaseRiskCategories'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_Derived_Risk AS
# MAGIC select distinct c.SourceClient 
# MAGIC ,r.RiskModelName
# MAGIC ,r.ValidatedRiskLevel
# MAGIC ,r.CalculatedRiskLevel
# MAGIC ,r.ReCalculatedRiskLevel
# MAGIC ,r.GeographicalRetainedRiskLevel
# MAGIC ,r.EntityTypeRetainedRiskLevel
# MAGIC ,r.StructureRetainedRiskLevel
# MAGIC ,r.SectorRetainedRiskLevel
# MAGIC ,r.ProductsRetainedRiskLevel
# MAGIC ,r.PoliticallyExposedPersonsRetainedRiskLevel
# MAGIC ,r.ThirdPartyRetainedRiskLevel
# MAGIC ,r.TransactionRetainedRiskLevel
# MAGIC ,r.DistributionChannelRetainedRiskLevel
# MAGIC ,r.AdverseInfoRetainedRiskLevel
# MAGIC from global_temp.Legacy2_client c
# MAGIC Inner join global_temp.Legacy2_risk r on c.ClientId = r.ClientId

# COMMAND ----------

# DBTITLE 1,Combine Gcob and Legacy2
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Party_Cases_Risk AS
# MAGIC select distinct
# MAGIC SourceClient
# MAGIC ,RiskModelName
# MAGIC ,ValidatedRiskLevel
# MAGIC ,GeographicalRiskLevel
# MAGIC ,EntityTypeRiskLevel
# MAGIC ,StructureRiskLevel
# MAGIC ,SectorRiskLevel
# MAGIC ,ProductAndServiceRiskLevel
# MAGIC ,PEPRiskLevel
# MAGIC ,TransactionRiskLevel
# MAGIC ,DistributionRiskLevel
# MAGIC ,ThirdPartyRiskLevel
# MAGIC ,AdverseInfoRiskLevel
# MAGIC ,OtherRiskLevel
# MAGIC from global_temp.party_case_client_details 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC SourceClient
# MAGIC ,RiskModelName
# MAGIC ,ValidatedRiskLevel
# MAGIC ,GeographicalRetainedRiskLevel as GeographicalRiskLevel
# MAGIC ,EntityTypeRetainedRiskLevel as EntityTypeRiskLevel
# MAGIC ,StructureRetainedRiskLevel as StructureRiskLevel
# MAGIC ,SectorRetainedRiskLevel as SectorRiskLevel
# MAGIC ,ProductsRetainedRiskLevel as ProductAndServiceRiskLevel
# MAGIC ,PoliticallyExposedPersonsRetainedRiskLevel as PEPRiskLevel
# MAGIC ,TransactionRetainedRiskLevel as TransactionRiskLevel
# MAGIC ,DistributionChannelRetainedRiskLevel as DistributionRiskLevel
# MAGIC ,ThirdPartyRetainedRiskLevel as ThirdPartyRiskLevel
# MAGIC ,AdverseInfoRetainedRiskLevel as AdverseInfoRiskLevel
# MAGIC ,null as OtherRiskLevel
# MAGIC from Legacy2_Derived_Risk

# COMMAND ----------

# DBTITLE 1,Create DataFrame to write to unity catalog
df_PartyCasesRisk=spark.table('Party_Cases_Risk')

# COMMAND ----------

# DBTITLE 1,Write to Unity Catalog
write_to_unity_catalog(df_PartyCasesRisk,catalog,schema, PartyCaseRiskCategories_dataobject)
