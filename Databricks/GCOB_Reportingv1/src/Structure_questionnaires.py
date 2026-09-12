# Databricks notebook source
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

Structure_Questionnaire_dataobject = 'Structure_Questionnaire'

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_client_structure_GUI'
,'party_structure_Questionnaire'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW structure_qus AS
# MAGIC WITH vary AS (
# MAGIC     select 
# MAGIC c.ClientID,
# MAGIC c.ClientGcobId,
# MAGIC c.ClientCaseId,
# MAGIC c.ClientFullLegalName,
# MAGIC c.CaseStatusName,
# MAGIC c.ReviewTypeName,
# MAGIC c.ClientLifeCycleName,
# MAGIC c.IsLatestApprovedVersionOfClient,
# MAGIC c.clienttype,
# MAGIC c.SourceSystem,
# MAGIC c.SourceClient,
# MAGIC sq.UboThresholdIsTrust,
# MAGIC sq.UboThresholdHasNomineeShareholder,
# MAGIC sq.UboThresholdIsRegisteredTrust,
# MAGIC sq.UboThresholdIsBearerShareCompany,
# MAGIC sq.UboThresholdIsRegisteredInHighRiskCountry,
# MAGIC sq.UboThresholdIsInHighRiskCountry,
# MAGIC sq.UboThresholdIsParentRegisteredInHighRiskCountry,
# MAGIC sq.UboThresholdHasTransactionsInHighRiskCountry,
# MAGIC sq.UboThresholdHasSanctionedEntity,
# MAGIC sq.UboThresholdHasActivitiesInSanctionedCountry,
# MAGIC sq.UboThresholdHasFiduciaryDeposits,
# MAGIC sq.UboThresholdIsInNonCooperativeJurisdiction,
# MAGIC sq.UboThresholdIsInHighRiskThirdCountry,
# MAGIC sq.UboThresholdPercentage,
# MAGIC case when c.clienttype='Legal Entity' then concat('LE_', c.ClientGcobId)
# MAGIC     when c.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',c.ClientGcobId) else 'NA' end as  UniqueGcobId
# MAGIC     -- ,ROW_NUMBER() OVER (PARTITION BY c.ClientGcobId ORDER BY c.ClientStructureSnapshotId DESC) AS ROWNUM
# MAGIC
# MAGIC from party_structure_Questionnaire sq
# MAGIC left join party_client_structure_gui c on sq.SourceClient = c.SourceClient
# MAGIC )
# MAGIC SELECT 
# MAGIC ClientGcobId as GcobId,
# MAGIC ClientCaseId as CaseId,
# MAGIC ClientFullLegalName as FullLegalName,
# MAGIC CaseStatusName as CaseStatus,
# MAGIC ReviewTypeName as ReviewType,
# MAGIC ClientLifeCycleName as ClientLifeCycleStatus,
# MAGIC IsLatestApprovedVersionOfClient,
# MAGIC clienttype,
# MAGIC SourceClient,
# MAGIC UboThresholdHasActivitiesInSanctionedCountry,
# MAGIC UboThresholdHasFiduciaryDeposits,
# MAGIC UboThresholdHasNomineeShareholder,
# MAGIC UboThresholdHasSanctionedEntity,
# MAGIC UboThresholdHasTransactionsInHighRiskCountry,
# MAGIC UboThresholdIsBearerShareCompany,
# MAGIC UboThresholdIsInHighRiskCountry,
# MAGIC UboThresholdIsInHighRiskThirdCountry,
# MAGIC UboThresholdIsInNonCooperativeJurisdiction,
# MAGIC UboThresholdIsParentRegisteredInHighRiskCountry,
# MAGIC UboThresholdIsRegisteredInHighRiskCountry,
# MAGIC UboThresholdIsRegisteredTrust,
# MAGIC UboThresholdIsTrust,
# MAGIC SourceSystem
# MAGIC --UboThresholdPercentage
# MAGIC    
# MAGIC FROM 
# MAGIC     vary 
# MAGIC WHERE 
# MAGIC     -- ROWNUM = 1 and 
# MAGIC     CaseStatusName <> 'Cancelled'

# COMMAND ----------

df_struct_qus=spark.table('structure_qus')

# COMMAND ----------

df_struct_qus=df_struct_qus.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

save_to_saradar_storage_account(df_struct_qus, Structure_Questionnaire_dataobject)

# COMMAND ----------

# df_struct_qus.createOrReplaceTempView("Final_struct_qus")

# COMMAND ----------

# %sql
# drop table IF EXISTS radar.Structure_Questionnaire

# COMMAND ----------

# spark.sql('select * from Final_struct_qus').write.mode('overwrite').saveAsTable('radar.Structure_Questionnaire')
