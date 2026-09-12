# Databricks notebook source
# MAGIC %md
# MAGIC  This Notebook generating below listed dataset.
# MAGIC
# MAGIC  List of BusinessActivities(NAICS)for GCOB and Legacy2 Data.
# MAGIC
# MAGIC

# COMMAND ----------

import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

BusinessActivities_dataobject = 'BusinessActivities'

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_business_activities',
'party_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Legacy2_business_activities',
'Legacy2_case_client_details',
'Legacy2_GCOB_ApprovedVersion',
'Legacy2_ClientStructure'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformations

# COMMAND ----------

# Creating spark Sql tables
df_party_business_activities=spark.table('party_business_activities')
df_party_case_client_details =spark.table('party_case_client_details')
df_Legacy2_business_activities=spark.table('Legacy2_business_activities')

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from Legacy2_case_client_details;--25926
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select * from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)

# COMMAND ----------

#adding addtional column BusinessDate  to dataframe 
df_party_business_activities=df_party_business_activities.withColumn("BusinessDate",lit(BusinessDate))
df_Legacy2_business_activities=df_Legacy2_business_activities.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

#creating the temp view using the dataframes. 
df_party_business_activities.createOrReplaceTempView("party_business_activities")
df_party_case_client_details.createOrReplaceTempView("party_case_client_details")

# COMMAND ----------

#creating the temp view using the dataframes.
df_Legacy2_business_activities.createOrReplaceTempView('Legacy2_business_activities')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW BusinessActivities AS
# MAGIC SELECT DISTINCT 
# MAGIC   b.BusinessDate,
# MAGIC   c.CaseId,
# MAGIC   c.GcobId,
# MAGIC   c.FullLegalName,
# MAGIC   b.NAICSCode,
# MAGIC   b.NAICSName,
# MAGIC   b.SectorGroup AS NAICSSectorGroup,
# MAGIC   b.IndustryGroup AS NAICSIndustryGroup,  
# MAGIC   b.IndustrySector AS NAICSIndustrySector,  
# MAGIC   c.IsLatestApprovedVersionOfClient,
# MAGIC   c.ReviewTypeName,  
# MAGIC   c.CaseStatusName,  
# MAGIC   c.ClientLifeCycleName,
# MAGIC   c.SourceSystem,  
# MAGIC   c.FIHubIndicator,
# MAGIC   b.SourceClient,
# MAGIC   CASE 
# MAGIC     WHEN substr(b.SourceClient,0,3) = 'LEC' THEN 'LegalEntity' 
# MAGIC     ELSE 'NP-NPPC' 
# MAGIC   END AS SourceSystemReference,
# MAGIC   c.ClientType,
# MAGIC   case when c.clienttype='Legal Entity' then concat('LE_', c.Gcobid)
# MAGIC   when c.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',c.Gcobid) else 'NA' end as  UniqueGcobId
# MAGIC FROM party_business_activities b   
# MAGIC INNER JOIN party_case_client_details c ON c.SourceClient = b.SourceClient
# MAGIC where c.ClientType in('Legal Entity',
# MAGIC 'Natural Person',
# MAGIC 'Natural Person acting in a Professional Capacity (NPPC)')
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   lb.BusinessDate,
# MAGIC   Null AS CaseID,
# MAGIC   lc.GcobId,
# MAGIC   lc.FullLegalName,
# MAGIC   lb.NAICSCode,
# MAGIC   lb.NAICSName,
# MAGIC   lb.SectorGroup AS NAICSSectorGroup,
# MAGIC   lb.IndustryGroup AS NAICSIndustryGroup,  
# MAGIC   lb.IndustrySector AS NAICSIndustrySector,      
# MAGIC   CASE 
# MAGIC     WHEN (lc.ApprovedInGcob = 0 AND GCOB_TRUE.CaseId IS NULL AND lc.StatusTypeName = 'Completed') THEN 'True' 
# MAGIC     ELSE 'False' 
# MAGIC   END AS IsLatestApprovedVersionOfClient,
# MAGIC   lc.ReviewTypeName,  
# MAGIC   lc.StatusTypeName AS CaseStatusName,  
# MAGIC   lc.ClientLifeCycleName,
# MAGIC   lc.SourceSystem,  
# MAGIC   Null AS FIHubIndicator,
# MAGIC   CASE 
# MAGIC     WHEN lc.IsClient = 'true' AND lc.ClientTypeId = 1 THEN concat('L2_LEC_',lc.ClientId)
# MAGIC     WHEN lc.IsClient = 'true' AND lc.ClientTypeId IN (2,3) THEN concat('L2_NP_NPPC_',lc.ClientId)
# MAGIC     WHEN lc.IsClient = 'false' AND lc.ClientTypeId = 1 THEN concat('L2_RLEP_',lc.ClientId)
# MAGIC     WHEN lc.IsClient = 'false' AND lc.ClientTypeId IN (2,3) THEN concat('L2_RNPP_',lc.ClientId)
# MAGIC   END AS SourceClient,
# MAGIC   lc.SourceSystem AS SourceSystemReference,
# MAGIC   lb.ClientType,
# MAGIC   case when lb.clienttype='Legal Entity' then concat('LE_', lc.Gcobid)
# MAGIC   when lb.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',lc.Gcobid) else 'NA' end as  UniqueGcobId
# MAGIC FROM Legacy2_business_activities lb   
# MAGIC INNER JOIN Legacy2_case_client_details lc ON lc.ClientId = lb.ClientId
# MAGIC LEFT OUTER JOIN Legacy2_GCOB_ApprovedVersion GCOB_TRUE ON lc.GcobCaseId = GCOB_TRUE.CaseId
# MAGIC
# MAGIC where lb.ClientType in('Legal Entity',
# MAGIC 'Natural Person',
# MAGIC 'Natural Person acting in a Professional Capacity (NPPC)')

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Data

# COMMAND ----------

df_BusinessActivities = spark.table('BusinessActivities')
save_to_saradar_storage_account(df_BusinessActivities, BusinessActivities_dataobject)

# COMMAND ----------

# # Check if the table exists
# if spark.catalog.tableExists("radar.BusinessActivities"):
#     # Drop the table if it exists
#     spark.sql("DROP TABLE radar.BusinessActivities")

# # Create the table with the new data
# spark.sql("SELECT * FROM BusinessActivites").write.mode("overwrite").saveAsTable("radar.BusinessActivities")
