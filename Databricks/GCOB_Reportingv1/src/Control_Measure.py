# Databricks notebook source
# MAGIC %md
# MAGIC # Configurations and Data Ingestion

# COMMAND ----------

# DBTITLE 1,Importing Libraries
import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Date Variables
date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

# DBTITLE 1,Outh2 Configuration
ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

MI_Control_Measure_dataobject = 'MI_Control_Measure'

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details'
, 'party_control_measures'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW control_measure AS
# MAGIC select distinct
# MAGIC to_date(dateadd(day,-1,getdate()),'yyyy-MM-dd') as BusinessDate,
# MAGIC b.FullLegalName as ClientFullLegalName,
# MAGIC b.GcobId as GcobId,
# MAGIC b.CaseId as CaseId, 
# MAGIC a.ControlMeasureId as ControlMeasureId,
# MAGIC a.ControlMeasureType as MeasureType,
# MAGIC a.Description,
# MAGIC CASE WHEN a.FrequencyTypeId=1 then 'One-off'
# MAGIC      WHEN a.FrequencyTypeId=2 then 'Monthly'
# MAGIC      WHEN a.FrequencyTypeId=3 then 'Quaterly'
# MAGIC      WHEN a.FrequencyTypeId=4 then 'Bi-Yearly'
# MAGIC      WHEN a.FrequencyTypeId=5 then 'Yearly'
# MAGIC      END as Frequency,
# MAGIC a.AssignedRole as AssignedRole,
# MAGIC to_date(date(a.StartDate),'yyyy-MM-dd') as StartDate,
# MAGIC to_date(date(a.ExecutionDate),'yyyy-MM-dd') as ExecutionDate,
# MAGIC to_date(date(a.EndDate),'yyyy-MM-dd') as EndDate,
# MAGIC CASE WHEN a.HasAdverseInformationRisk=1 THEN 'True' ELSE 'False' END as HasAdverseInformationRisk,
# MAGIC CASE WHEN a.HasDistributionRisk=1 THEN 'True' ELSE 'False' END as HasDistributionRisk,
# MAGIC CASE WHEN a.HasEntityTypeRisk=1 THEN 'True' ELSE 'False' END HasEntityTypeRisk,
# MAGIC CASE WHEN a.HasGeographicalRisk=1 THEN 'True' ELSE 'False' END as HasGeographicalRisk,
# MAGIC CASE WHEN a.HasPEPRisk=1 THEN 'True' ELSE 'False' END as HasPEPRisk,
# MAGIC CASE WHEN a.HasProductsAndServicesRisk=1 THEN 'True' ELSE 'False' END as HasProductsAndServicesRisk,
# MAGIC CASE WHEN a.HasSectorRisk=1 THEN 'True' ELSE 'False' END as HasSectorRisk,
# MAGIC CASE WHEN a.HasStructureRisk=1 THEN 'True' ELSE 'False' END as HasStructureRisk,
# MAGIC CASE WHEN a.HasThirdPartyRisk=1 THEN 'True' ELSE 'False' END as HasThirdPartyRisk,
# MAGIC CASE WHEN a.HasTransactionRisk=1 THEN 'True' ELSE 'False' END as HasTransactionRisk,
# MAGIC CASE WHEN a.HasGeneralRisk=1 THEN 'True' ELSE 'False' END as HasGeneralRisk,
# MAGIC CASE WHEN a.HasOtherRisk=1 THEN 'True' ELSE 'False' END as HasOtherRisk,
# MAGIC CASE WHEN a.HasIdentificationAndVerificationRisk=1 THEN 'True' ELSE 'False' END as HasIdentificationAndVerificationRisk,
# MAGIC CASE WHEN a.HasClientProfileRisk=1 THEN 'True' ELSE 'False' END as HasClientProfileRisk,
# MAGIC CASE WHEN a.HasScreeningRisk=1 THEN 'True' ELSE 'False' END as HasScreeningRisk,
# MAGIC CASE WHEN a.HasTimelyDueDiligenceRisk=1 THEN 'True' ELSE 'False' END as HasTimelyDueDiligenceRisk,
# MAGIC a.Country,
# MAGIC a.CounterParties ,
# MAGIC b.IsLatestApprovedVersionOfClient as IsLatestApprovedVersionoftheClient,
# MAGIC b.ReviewTypeName as CaseReview,
# MAGIC b.CaseStatusName as CaseStatus,
# MAGIC b.ClientLifeCycleName as ClientLifeCycle,
# MAGIC b.SourceSystem as SourceSystem,
# MAGIC case when substr(b.SourceClient,0,3) = 'LEC' and b.SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC      when substr(b.SourceClient,0,2) = 'NP' and b.SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC      when substr(b.SourceClient,0,6) = 'L2_LEC' and b.SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC      when substr(b.SourceClient,0,5) = 'L2_NP' and b.SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC     end as SourceSystemReference,
# MAGIC b.SourceClient,
# MAGIC b.ClientType,
# MAGIC b.FIHubIndicator as FIHubIndicator,
# MAGIC to_date(date(a.ModifiedDate),'yyyy-MM-dd') as ModifiedDate,
# MAGIC a.Status,
# MAGIC a.IsApprovedByClientCommittee
# MAGIC from party_control_measures a
# MAGIC inner join party_case_client_details b on a.SourceClient = b.SourceClient where a.ExpiredDate is null

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Data

# COMMAND ----------

df_MI_Control_Measures = spark.table('control_measure')
save_to_saradar_storage_account(df_MI_Control_Measures, MI_Control_Measure_dataobject)

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS radar.MI_Control_Measure;

# COMMAND ----------

# spark.sql('select * from Control_Measure').write.mode('overwrite').saveAsTable('radar.MI_Control_Measure')
