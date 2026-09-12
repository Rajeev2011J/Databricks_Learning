# Databricks notebook source
import os 

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

# load and create temp views of all gdp_tables below
gcob_tables = [
    'CaseService_case_LegalEntityClient'
    , 'CaseService_case_Case'
    , 'CaseService_NaturalPerson_NaturalPersonClient'
    , 'CaseService_NaturalPerson_NaturalPersonCase'
    , 'RiskModel_dbo_Model'
    , 'CaseService_case_DynamicRiskModelInstanceReference'
    , 'RiskModel_dbo_InstanceCalculation'
    , 'RiskModel_dbo_Instance'
    , 'CaseService_NaturalPerson_DynamicRiskModelInstanceReference'
]

for item in gcob_tables:
    # get the most recent file available in gdp
    path = f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/{item}/100/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOADED_DTS=')[1][:8] for file in files if 'LOADED_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/{item}/100/data/LOADED_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

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

spark.createDataFrame(zip(StatusId_list, Name_list), ['StatusId', 'Name']).createOrReplaceTempView('gcob_static_CaseStatusType')

# COMMAND ----------

SourceId = [0,1,2,3,4,5,6,7]
DestinationId = [0,0,1,2,3,3,3,4]

spark.createDataFrame(zip(SourceId, DestinationId), ['SourceId', 'DestinationId']).createOrReplaceTempView('gcob_static_RiskLevelMapping')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW latest_riskmodel_le AS
# MAGIC
# MAGIC WITH max_clientid_cte AS (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.GcobId
# MAGIC     , max(t1.Id) AS max_clientid
# MAGIC   FROM CaseService_case_LegalEntityClient t1
# MAGIC   LEFT JOIN CaseService_case_Case t2 ON t1.Id = t2.LegalEntityClientId
# MAGIC   WHERE t2.CurrentStatus <> 10 
# MAGIC   GROUP BY GcobId
# MAGIC )
# MAGIC
# MAGIC , CTE_InstancePerLE AS (
# MAGIC   SELECT
# MAGIC     t1.LegalEntityClientId
# MAGIC     , MAX(t1.InstanceId) AS InstanceId
# MAGIC     , MAX(t2.Id) AS InstanceCalculationId
# MAGIC   FROM CaseService_case_DynamicRiskModelInstanceReference t1
# MAGIC   LEFT JOIN RiskModel_dbo_InstanceCalculation t2 ON t1.InstanceId = t2.InstanceId
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC
# MAGIC , Risk_Level_Model AS
# MAGIC (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.LegalEntityClientId
# MAGIC     , t5.ModelId
# MAGIC     , t3.DestinationId AS ModelCalculatedRiskLevel
# MAGIC     , t4.DestinationId AS ModelRecalculatedRiskLevel
# MAGIC   FROM CTE_InstancePerLE t1
# MAGIC   LEFT JOIN RiskModel_dbo_InstanceCalculation t2 ON t1.InstanceCalculationId = t2.Id
# MAGIC   LEFT JOIN gcob_static_RiskLevelMapping t3 ON t2.CalculatedRiskLevelId = t3.SourceId
# MAGIC   LEFT JOIN gcob_static_RiskLevelMapping t4 ON t2.RecalculatedRiskLevelId = t4.SourceId
# MAGIC   LEFT JOIN RiskModel_dbo_Instance t5 ON t1.InstanceId = t5.Id
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.GcobId
# MAGIC   , t2.CaseId
# MAGIC   , t5.Name as CaseStatusName
# MAGIC   , t3.ModelId
# MAGIC   , t4.Name AS RiskModelName
# MAGIC FROM max_clientid_cte t1
# MAGIC LEFT JOIN CaseService_case_Case t2 ON t1.max_clientid = t2.LegalEntityClientId
# MAGIC LEFT JOIN Risk_Level_Model t3 ON t1.max_clientid = t3.LegalEntityClientId
# MAGIC LEFT JOIN RiskModel_dbo_Model t4 ON t3.ModelId = t4.Id
# MAGIC LEFT JOIN gcob_static_CaseStatusType t5 ON t2.CurrentStatus = t5.StatusId
# MAGIC WHERE t3.ModelId IN (
# MAGIC   22
# MAGIC   , 23
# MAGIC   , 24
# MAGIC   , 25
# MAGIC   , 26
# MAGIC   , 27
# MAGIC )
# MAGIC ORDER BY t1.GcobId ASC 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW latest_riskmodel_np AS
# MAGIC
# MAGIC WITH max_clientid_cte AS (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.GcobId
# MAGIC     , max(t1.Id) AS max_clientid
# MAGIC   FROM CaseService_NaturalPerson_NaturalPersonClient t1
# MAGIC   LEFT JOIN CaseService_NaturalPerson_NaturalPersonCase t2 ON t1.Id = t2.NaturalPersonClientId
# MAGIC   WHERE t2.CurrentStatus <> 10 
# MAGIC   GROUP BY GcobId
# MAGIC )
# MAGIC
# MAGIC , Risk_Level_Model AS
# MAGIC (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.Id AS NaturalPersonClientId
# MAGIC     , t3.ModelId
# MAGIC   FROM CaseService_NaturalPerson_NaturalPersonClient t1
# MAGIC   LEFT JOIN CaseService_NaturalPerson_DynamicRiskModelInstanceReference t2 ON t2.NaturalPersonClientId = t1.Id AND t2.Expired IS NULL
# MAGIC   LEFT JOIN RiskModel_dbo_Instance t3 ON t3.Id = t2.InstanceId
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.GcobId
# MAGIC   , t1.max_clientid AS CaseId
# MAGIC   , t5.Name as CaseStatusName
# MAGIC   , t3.ModelId
# MAGIC   , t4.Name AS RiskModelName
# MAGIC FROM max_clientid_cte t1
# MAGIC LEFT JOIN CaseService_NaturalPerson_NaturalPersonCase t2 ON t1.max_clientid = t2.NaturalPersonClientId
# MAGIC LEFT JOIN Risk_Level_Model t3 ON t1.max_clientid = t3.NaturalPersonClientId
# MAGIC LEFT JOIN RiskModel_dbo_Model t4 ON t3.ModelId = t4.Id
# MAGIC LEFT JOIN gcob_static_CaseStatusType t5 ON t2.CurrentStatus = t5.StatusId
# MAGIC WHERE t3.ModelId IN (
# MAGIC   22
# MAGIC   , 23
# MAGIC   , 24
# MAGIC   , 25
# MAGIC   , 26
# MAGIC   , 27
# MAGIC )
# MAGIC ORDER BY t1.GcobId ASC 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t1.*, t2.NextReviewDate, t2.ReviewLocation, t2.ClientLifeCycleName FROM latest_riskmodel_le t1
# MAGIC LEFT JOIN radar.clients t2 on t1.gcobId = t2.GcobId
# MAGIC where t2.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT t1.*, t2.NextReviewDate, t2.ReviewLocation, t2.ClientLifeCycleName FROM latest_riskmodel_np t1
# MAGIC LEFT JOIN radar.clients t2 on t1.gcobId = t2.GcobId
# MAGIC where t2.SourceSystemReference = 'GCOB_NP-NPPC'
# MAGIC
# MAGIC ORDER BY GcobId ASC
