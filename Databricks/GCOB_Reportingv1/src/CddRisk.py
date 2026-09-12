# Databricks notebook source
import os
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta
#Local File Import
from RadarUtils import *

spark.conf.set("spark.sql.shuffle.partitions", "100")
spark.conf.set("spark.sql.adaptive.enabled","true")

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

L2_CddRisk_dataobject = 'Legacy2_CddRiskDetails'
RiskModelQuestionMapping_dataobject = 'RiskModelQuestionMapping'
GCOB_CddRisk_dataobject = 'GCOB_CddRiskDetails'
GCOB_Cdd_Case_EAQuestionAnswer_dataobject = 'Cdd_Case_EventAssessmentQuestionandAnswer'
EAQuestionMapping_dataobject = 'EAQuestionMapping' 

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Party_RiskModelInstanceQuestionAnswers'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Party_Cdd_Case_EventAssessmentQuestionandAnswer'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Legacy2_GCOB_ApprovedVersion'
, 'Legacy2_case_client_details'
, 'Legacy2_risk'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Legacy2 CDD Risk

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from Legacy2_case_client_details;--25926
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%'; --3006

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select * from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_RiskDetails AS
# MAGIC with risk AS 
# MAGIC (
# MAGIC select r.* 
# MAGIC ,c.FullLegalName
# MAGIC ,c.GcobId
# MAGIC ,c.ReviewTypeName
# MAGIC ,c.StatusTypeName
# MAGIC ,c.GlobalClientOwner
# MAGIC ,c.GlobalClientOwnerLocation
# MAGIC ,c.ClientLifeCycleName
# MAGIC ,c.CddType
# MAGIC ,c.CaseCompletedDate
# MAGIC -- ,case when (c.ApprovedInGcob = 0 and GCOB_TRUE.CaseId is null and c.StatusTypeName = 'Complete') then 'True' else 'False' end as IsLatestApprovedVersionOfClient
# MAGIC ,c.IsLatestApprovedVersionOfClient
# MAGIC ,case when IsClient = 'true' and ClientTypeId = 1 then concat('L2_LEC_',c.ClientId)
# MAGIC       when IsClient = 'true' and ClientTypeId in (2,3) then concat('L2_NP_NPPC_',c.ClientId)
# MAGIC       when IsClient = 'false' and ClientTypeId = 1 then concat('L2_RLEP_',c.ClientId)
# MAGIC       when IsClient = 'false' and ClientTypeId in (2,3) then concat('L2_RNPP_',c.ClientId)
# MAGIC end as SourceClient
# MAGIC ,c.sourcesystem
# MAGIC ,case when c.clienttype='Legal Entity' then concat('LE_', c.Gcobid)
# MAGIC     when c.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',c.Gcobid) else 'NA' end as  UniqueGcobId
# MAGIC from Legacy2_risk r
# MAGIC Inner join Legacy2_client c on r.ClientId = c.ClientId
# MAGIC Left outer join Legacy2_GCOB_ApprovedVersion GCOB_TRUE on c.GcobCaseId = GCOB_TRUE.CaseId
# MAGIC where c.IsClient = 'true'
# MAGIC and ClientTypeId in (1,2,3)
# MAGIC )
# MAGIC select * 
# MAGIC ,case when substr(SourceClient,0,3) = 'LEC' and SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(SourceClient,0,2) = 'NP' and SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(SourceClient,0,6) = 'L2_LEC' and SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(SourceClient,0,5) = 'L2_NP' and SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC from risk
# MAGIC ;

# COMMAND ----------

df_L2_CddRisk=spark.table('Legacy2_RiskDetails')
df_L2_CddRisk=df_L2_CddRisk.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

save_to_saradar_storage_account(df_L2_CddRisk, L2_CddRisk_dataobject)

# COMMAND ----------

# MAGIC %md
# MAGIC ## RiskModel InstanceQuestionAnswers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW InstanceQuestionAnswers AS
# MAGIC select distinct
# MAGIC InstanceId,SourceSystemReferenceId,SourceSystemName,QuestionId,Text,AnswerValue,AnswerText,AnswerRiskScore,QuestionText
# MAGIC ,QuestionCode,ClientId,SourceClient,CaseStatusName,IsLatestApprovedVersionOfClient
# MAGIC from Party_RiskModelInstanceQuestionAnswers
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RiskModelInstanceQuestionAnswers AS
# MAGIC select * from InstanceQuestionAnswers where SourceSystemName = 'GCOB-CaseService' and ClientId is not null and QuestionCode like '%-%';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RiskModelQuestionMapping AS
# MAGIC select distinct 
# MAGIC c.RiskModelName,i.QuestionCode,trim(i.QuestionText) as QuestionText,i.QuestionId,c.sourcesystem
# MAGIC from RiskModelInstanceQuestionAnswers i
# MAGIC Inner join party_case_client_details c on i.SourceClient = c.SourceClient
# MAGIC where c.RiskModelName is not null
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select distinct 
# MAGIC riskmodelName,'n/a' as QuestionCode,'n/a' as QuestionText,'n/a' as QuestionId,sourcesystem
# MAGIC From Legacy2_RiskDetails
# MAGIC where RiskModelName is not null
# MAGIC
# MAGIC --order by c.RiskModelName,i.QuestionCode

# COMMAND ----------

df_RiskModelQuestionMapping = spark.table('RiskModelQuestionMapping')
save_to_saradar_storage_account(df_RiskModelQuestionMapping, RiskModelQuestionMapping_dataobject)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GCOB CDD Risk

# COMMAND ----------

df_RiskModelInstanceQuestionAnswers=spark.table('RiskModelInstanceQuestionAnswers')

# COMMAND ----------

df_RiskModelInstanceQuestionAnswers_pivotValues = [row[0] for row in df_RiskModelInstanceQuestionAnswers.select('QuestionCode').distinct().collect()]

# COMMAND ----------

df_RiskAnswersPivot = df_RiskModelInstanceQuestionAnswers.repartition(100, 'SourceClient', 'InstanceId').groupby('SourceClient', 'InstanceId').pivot('QuestionCode', df_RiskModelInstanceQuestionAnswers_pivotValues).agg(array_join(collect_set('AnswerText'), ','))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW MaterialQuestionAnswers AS
# MAGIC select distinct
# MAGIC MaterialInstanceId,SourceSystemReferenceId,SourceSystemName,MaterialQuestionId,MaterialQuestionCode,IsMaterial
# MAGIC ,ClientId,SourceClient,CaseStatusName,IsLatestApprovedVersionOfClient
# MAGIC from Party_RiskModelInstanceQuestionAnswers
# MAGIC where MaterialInstanceId is not null and SourceSystemName = 'GCOB-CaseService' and ClientId is not null and MaterialQuestionCode like '%-%'
# MAGIC ;

# COMMAND ----------

df_MaterialQuestionAnswers=spark.table('MaterialQuestionAnswers').withColumn('QuestionCodeNew',concat(col('MaterialQuestionCode'),lit("_MaterialRisk")))

# COMMAND ----------

df_MaterialQuestionAnswers_pivotValues = [row[0] for row in df_MaterialQuestionAnswers.select('QuestionCodeNew').distinct().collect()]

# COMMAND ----------

df_RiskMaterialPivot = df_MaterialQuestionAnswers.repartition(100, 'SourceClient', 'MaterialInstanceId').groupby('SourceClient','MaterialInstanceId').pivot('QuestionCodeNew', df_MaterialQuestionAnswers_pivotValues).agg(array_join(collect_set('IsMaterial'), ','))

# COMMAND ----------

# Join this with df_RiskMaterialPivot
sdf_AllCddRisk = df_RiskAnswersPivot.join(df_RiskMaterialPivot, on = 'SourceClient', how = 'left')

# COMMAND ----------

sdf_AllCddRisk.createOrReplaceTempView("InstanceQuestionAnswers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CddRisk AS
# MAGIC select c.FullLegalName,c.GcobId,c.CaseId,c.ReviewTypeName,c.CaseStatusName,c.RiskModelName,c.IsLatestApprovedVersionOfClient
# MAGIC ,c.ModelCalculatedRiskLevel,c.ModelRecalculatedRiskLevel,c.ValidatedRiskLevel,c.GeographicalRiskLevel,c.EntityTypeRiskLevel,c.StructureRiskLevel,c.SectorRiskLevel,c.ProductAndServiceRiskLevel,c.PEPRiskLevel,c.TransactionRiskLevel,c.DistributionRiskLevel,c.ThirdPartyRiskLevel,c.AdverseInfoRiskLevel,c.OtherRiskLevel,c.GeographicalApplicableRisk,c.EntityTypeApplicableRisk,c.StructureApplicableRisk,c.SectorApplicableRisk,c.ProductsApplicableRisk,c.PoliticallyExposedPersonsApplicableRisk,c.TransactionApplicableRisk,c.DistributionChannelApplicableRisk,c.ThirdPartyApplicableRisk,c.AdverseInfoApplicableRisk,c.OtherApplicableRisk,c.SourceSystem,c.GlobalClientOwner,c.GlobalClientOwnerLocation,c.ClientLifeCycleName,c.ClientType,c.CaseCompletedDate
# MAGIC ,case when c.clienttype='Legal Entity' then concat('LE_', c.Gcobid)
# MAGIC     when c.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',c.Gcobid) else 'NA' end as  UniqueGcobId
# MAGIC ,i.* 
# MAGIC ,case when substr(i.SourceClient,0,3) = 'LEC' and SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(i.SourceClient,0,2) = 'NP' and SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(i.SourceClient,0,6) = 'L2_LEC' and SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(i.SourceClient,0,5) = 'L2_NP' and SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC from InstanceQuestionAnswers i
# MAGIC left join party_case_client_details c on i.SourceClient = c.SourceClient
# MAGIC where c.CaseStatusName <> 'Cancelled'

# COMMAND ----------

df_GCOB_CddRisk=spark.table('CddRisk')
df_GCOB_CddRisk=df_GCOB_CddRisk.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

save_to_saradar_storage_account(df_GCOB_CddRisk, GCOB_CddRisk_dataobject)

# COMMAND ----------

# MAGIC %md
# MAGIC **Cdd Case Event Assessment Question and Answer**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CddCaseEAInstanceQuestionAnswers AS
# MAGIC select distinct
# MAGIC InstanceId,GcobId as GCOBID,Case_ID as CaseID,FullLegalName,ReviewTypeName as ReviewType,CaseStatusName, ClientLifeCycleName, SourceSystemReferenceId,SourceSystemName,QuestionId,Text,AnswerValue,AnswerText,AnswerRiskScore,QuestionText,QuestionCode,ClientId,SourceClient,IsLatestApprovedVersionOfClient
# MAGIC from Party_Cdd_Case_EventAssessmentQuestionandAnswer
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CddCaseEventAssessmentInstanceQuestionAnswers AS
# MAGIC select * from CddCaseEAInstanceQuestionAnswers where SourceSystemName = 'GCOB-CaseService' and ClientId is not null and QuestionCode like '%-%';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW EAQuestionMapping AS
# MAGIC select distinct 
# MAGIC c.RiskModelName,i.QuestionCode,trim(i.QuestionText) as QuestionText,i.QuestionId,c.sourcesystem
# MAGIC from CddCaseEventAssessmentInstanceQuestionAnswers i
# MAGIC Inner join party_case_client_details c on i.SourceClient = c.SourceClient
# MAGIC where c.RiskModelName is not null
# MAGIC

# COMMAND ----------

df_EAQuestionMapping = spark.table('EAQuestionMapping')
save_to_saradar_storage_account(df_EAQuestionMapping, EAQuestionMapping_dataobject)

# COMMAND ----------

df_CddCaseEventAssessmentInstanceQuestionAnswers=spark.table('CddCaseEventAssessmentInstanceQuestionAnswers')

# COMMAND ----------

df_df_CddCaseEventAssessmentInstanceQuestionAnswers_pivotValues = [row[0] for row in df_CddCaseEventAssessmentInstanceQuestionAnswers.select('QuestionCode').distinct().collect()]

# COMMAND ----------

df_EAAnswersPivot = df_CddCaseEventAssessmentInstanceQuestionAnswers.repartition(100, 'SourceClient', 'InstanceId').groupby('SourceClient', 'InstanceId').pivot('QuestionCode', df_df_CddCaseEventAssessmentInstanceQuestionAnswers_pivotValues).agg(array_join(collect_set('AnswerText'), ','))

# COMMAND ----------

df_EAAnswersPivot.createOrReplaceTempView('CddCaseEventAssessmentQA')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CddCaseEventAssessmentQuestionMapping AS
# MAGIC select distinct 
# MAGIC c.RiskModelName, c.GCOBID,c.CaseID,c.FullLegalName,c.ReviewTypeName,c.CaseStatusName, c.ClientLifeCycleName,c.SourceSystem,c.ClientId,c.IsLatestApprovedVersionOfClient, i.*
# MAGIC from CddCaseEventAssessmentQA i
# MAGIC Inner join party_case_client_details c on i.SourceClient = c.SourceClient
# MAGIC where c.RiskModelName is not null

# COMMAND ----------

df_GCOB_CddCase=spark.table('CddCaseEventAssessmentQuestionMapping')
df_GCOB_CddCase=df_GCOB_CddCase.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

save_to_saradar_storage_account(df_GCOB_CddCase, GCOB_Cdd_Case_EAQuestionAnswer_dataobject)
