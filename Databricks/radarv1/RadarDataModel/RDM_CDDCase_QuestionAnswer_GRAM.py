# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To create a dataobject to have CDD Case Question and Answer for all W&R Sourcesystems
# MAGIC
# MAGIC #### Authors
# MAGIC - Abhishek
# MAGIC - Mahalakshmi.Vengateswaran@rabobank.com
# MAGIC - Prasad.gadidala@rabobank.com
# MAGIC
# MAGIC ##### Flow of Logic
# MAGIC - Reading GCOB,Legacy2,GCDS,GIC,KN1 Caseservice,Party_RiskModelInstanceQuestionAnswers dataobjects from GDP
# MAGIC - Fetching the Question and Answer details of GCOB Clients from GRAM
# MAGIC - Fetching the Question and Answer details of Legacy2 Clients from Legacy2_risk
# MAGIC - Fetching the Question and Answer details of GIC Clients from GRAM
# MAGIC - Combining all the Question and Answer related information from different sourcesystems
# MAGIC - Loading the final dataobject to SA RADAR Storage account
# MAGIC
# MAGIC #### Note: 
# MAGIC   - Currently this notebook has scope of GCOB,Legacy2,GCDS,KN1 SourceSystems
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Prasad  Gadidala	 |11-Aug-2025 |13357503 |KN1 Cases Instance question and answers those are not aviabale in GRAM and        brefore feb 2024 data issue fix. 
# MAGIC    
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####READING FILES FROM GDP

# COMMAND ----------

# DBTITLE 1,Importing the required packages
import os
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
SARADAR='saradar'+environment

# COMMAND ----------

# DBTITLE 1,Importing modules from RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Defining the dataobject name
party_CDDcasequestionanswers_dataobject='Party_CDDCase_QuestionAnswer'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Deriving required date parameters
Today = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'

# COMMAND ----------

# DBTITLE 1,Reading GCOB CaseService Dataobjects from GDP
# List of datasets from GDP
load_df = [
'party_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# DBTITLE 1,Reading Legacy2 Dataobjects from GDP
# List of dataobjects from GDP
load_df = [
'Party_RiskModelInstanceQuestionAnswers'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')

# COMMAND ----------

# DBTITLE 1,Reading Legacy2 Dataobjects from GDP
# List of datasets from GDP
load_df =[
'Legacy2_case_client_details',
'Legacy2_risk']

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

df_Party_SystemIdentifier=spark.read.parquet(f'abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/1/data/{load_dts}/*.parquet')

# COMMAND ----------

# DBTITLE 1,Reading GIC Dataobjects from GDP
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'pessoa'
]})
#gic_load_dts
# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname)

# COMMAND ----------

# DBTITLE 1,Reading KN1 Dataobjects from GDP
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'ADKYC_CONTRAPARTES'
,'ADKYC_CONTRAPARTES_COMPL'
,'ADRBB_CONTRAPARTES_GRAM'
,'ADRBB_CONTRAPARTES_PONTUACOES_ABAS'
,'ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS'
,'ADRBB_QUESTOES'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='KN1', Dataobject=row.GDPname)

# COMMAND ----------

# DBTITLE 1,Reading  KN1 Cases  Object
df_Party_KN1Cases = spark.read.parquet(f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_KN1Cases/1/data/{load_dts}/*.parquet")
df_Party_KN1Cases.createOrReplaceTempView("KN1Cases")

# COMMAND ----------

# MAGIC %md
# MAGIC #####TRANSFORMATION TO GET Party_CDDCase_QuestionAnswer OBJECT 

# COMMAND ----------

# DBTITLE 1,Adding additional attributes to GCOB CaseClient Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_CaseClientDetails As
# MAGIC select * 
# MAGIC ,case when ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       else null end as Party_type,Case
# MAGIC       when ClientType = 'Legal Entity' then concat('LEC_', GcobId)
# MAGIC       when ClientType in (
# MAGIC         'Natural Person',
# MAGIC         'Natural Person acting in a Professional Capacity (NPPC)'
# MAGIC       ) then concat('NP_NPPC_', GcobId)
# MAGIC     End as UniquePartyId
# MAGIC from party_case_client_details
# MAGIC where CaseStatusName <> 'Cancelled'

# COMMAND ----------

# MAGIC %md
# MAGIC ######GCOB QUESTION AND ANSWER

# COMMAND ----------

# DBTITLE 1,Logic to get GCOB Question and Answer
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_QuestionAnswer AS
# MAGIC Select
# MAGIC   c.SourceSystem as Application,
# MAGIC   CONCAT('GCOB_', c.UniquePartyId) AS LocalSystemIdentifier,
# MAGIC   c.ClientId,
# MAGIC   c.CaseID,
# MAGIC   CASE
# MAGIC     WHEN c.ClientType like 'Legal%' THEN concat(c.SourceSystem, '_LE_', c.CaseId)
# MAGIC     WHEN c.ClientType LIKE '%Natural%' THEN concat(c.SourceSystem, '_NP_NPPC_', c.CaseId)
# MAGIC   END as UniqueCaseId,
# MAGIC   rmqa.InstanceId,
# MAGIC   rmqa.QuestionId,
# MAGIC   rmqa.QuestionText,
# MAGIC   rmqa.QuestionCode,
# MAGIC   rmqa.AnswerValue,
# MAGIC   rmqa.AnswerText
# MAGIC from
# MAGIC   Gcob_CaseClientDetails c
# MAGIC     inner join Party_RiskModelInstanceQuestionAnswers rmqa
# MAGIC       on c.SourceClient = rmqa.SourceClient
# MAGIC where
# MAGIC   rmqa.SourceSystemName = 'GCOB-CaseService'

# COMMAND ----------

# MAGIC %md
# MAGIC ######LEGACY2 QUESTION AND ANSWER

# COMMAND ----------

# DBTITLE 1,Unpivoting Legacy2 Questions and Answers
sdf=spark.table('Legacy2_risk')
columns = [
    "GeographicalRegisteredCountry",
    "GeographicalUboCountries",
    "GeographicalUpCountries",
    "GeographicalHighRiskCountries",
    "EntityTypeCharacterizationReferenceType",
    "EntityTypeCharacterizationTypeText",
    "StructureIssuedBearerShares",
    "StructureMoreThanFourLayers",
    "StructureIsTrust",
    "StructureNomineeShareholders",
    "StructureMultiEntityUltimateBeneficialOwners",
    "StructureNonResidentialMajorityShellShareholder",
    "StructureBearerAlertUsername",
    "StructureBearerAlertDateTime",
    "StructureTrustAndCompanyServiceProvider",
    "StructureTrustAndCompanyServiceProviderExplanation",
    "StructureTrustAndCompanyServiceProviderLogicalLegitimate",
    "StructureTrustAndCompanyServiceProviderLicensedRegulated",
    "StructureUnusualOrComplex",
    "StructureRegistrationIncreasedTaxIntegrityRiskJurisdiction",
    "StructureBroaderOrganizationalChart",
    "SectorBusinessIndustry",
    "SectorBusinessActivitiesDescription",
    "SectorOtherHighRiskActivities",
    "SectorAssociatedRisks",
    "ProductsCommensurateWithClientProfile",
    "ProductsSourceOfRepaymentCommensurate",
    "ProductsAccountPurposeDescription",
    "ProductsExpectedUseDescription",
    "ProductsSourceOfFundsDescription",
    "ProductsSourceOfFundsNADescription",
    "ProductsSourceOfWealthDescription",
    "PoliticallyExposedPersonStatus",
    "TransactionBackToBack",
    "TransactionUnsual",
    "TransactionLoanBack",
    "TransactionFalsifiedRevenues",
    "TransactionNoApparentPurpose",
    "TransactionNonTransparent",
    "TransactionRealEstateRelated",
    "TransactionTaxEvasionFacilitating",
    "TransactionHighRisk",
    "TransactionAnonymousUse",
    "TransactionIsRatioCashElectronicMoney",
    "TransactionRatioCashElectronicMoneyNote",
    "TransactionIsTotalTransactionsInLast12Months",
    "TransactionTotalTransactionsInLast12MonthsNote",
    "TransactionIsTotalTransactionsInLast12MVsLast36Months",
    "TransactionTotalTransactionsInLast12MVsLast36MNote",
    "TransactionIsGeographyOutgoingAndIncomingTransactions",
    "TransactionGeographyOutgoingAndIncomingTransactionsNote",
    "TransactionIsInstrumentsUsedByClientInTransactions",
    "TransactionInstrumentsUsedByClientInTransactionsNote",
    "TransactionIsTotalValueTransactionsInLast12MVsLast36Months",
    "TransactionTotalValueTransactionsInLast12MVsLast36MNote",
    "DistributionFaceToFace",
    "DistributionFaceToFaceStaffName",
    "DistributionFaceToFaceDate",
    "DistributionThirdParty",
    "ThirdPartyActorReferenceId",
    "ThirdPartyCDDbyThirdParty",
    "ThirdPartyWWFT",
    "AdverseInformationTypeReference",
    "OtherRiskNotAnalysed",
    "OtherRiskNotAnalysedComment",
    "OtherLocalLaws",
    "OtherLocalLawsComment",
]

# Create Spark DataFrame by selecting columns from temporary view 'a'
#sdf = spark.sql(f"SELECT {', '.join(columns)} FROM Legacy2_risk")


# Cast all columns to string type
sdf_casted = sdf.select(
    [F.col(c).cast("string").alias(c) for c in sdf.columns]
)

# Convert columns to rows
melted_sdf = sdf_casted.select(
    "ClientId",
    F.explode(
        F.array(
            *[
                F.struct(
                    F.lit(col).alias("QuestionText"), F.col(col).alias("AnswerText")
                )
                for col in columns
            ]
        )
    ).alias("QA"),
).select("ClientId", "QA.QuestionText", "QA.AnswerText")


# Show the result
melted_sdf.createOrReplaceTempView('Legacy2_questions')

# COMMAND ----------

# DBTITLE 1,Fetching NonSFDCID_Legacy2_Clients
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%';

# COMMAND ----------

# DBTITLE 1,Filtering out NonSFDCID_Legacy2_Clients
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select * from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)

# COMMAND ----------

# DBTITLE 1,Logic to get Legacy2 Question and Answer
# MAGIC
# MAGIC   %sql
# MAGIC   CREATE OR REPLACE TEMPORARY VIEW Legacy2_QuestionAnswer AS
# MAGIC   Select
# MAGIC     c.SourceSystem as Application,
# MAGIC
# MAGIC  CONCAT('LEGACY2_', CASE
# MAGIC       WHEN IsClient = 'true'
# MAGIC       and ClientTypeId = 1 THEN concat('LE_', c.GcobId)
# MAGIC       WHEN IsClient = 'true'
# MAGIC       and ClientTypeId in (2, 3) THEN concat('NP_NPPC_', c.GcobId)
# MAGIC       WHEN IsClient = 'false'
# MAGIC       and ClientTypeId = 1 THEN concat('RLE_', c.GcobId)
# MAGIC       WHEN IsClient = 'false'
# MAGIC     and ClientTypeId in (2, 3) THEN concat('RNP_', c.GcobId) END) as LocalSystemIdentifier,
# MAGIC     c.ClientId,
# MAGIC     null as CaseID,
# MAGIC     CASE
# MAGIC       WHEN
# MAGIC         c.IsClient = 'true'
# MAGIC         and c.ClientTypeId = 1
# MAGIC       THEN
# MAGIC         concat(c.SourceSystem, '_LEC_', c.ClientId)
# MAGIC       WHEN
# MAGIC         c.IsClient = 'true'
# MAGIC         and c.ClientTypeId in (2, 3)
# MAGIC       THEN
# MAGIC         concat(c.SourceSystem, '_NP_NPPC_', c.ClientId)
# MAGIC       WHEN
# MAGIC         c.IsClient = 'false'
# MAGIC         and c.ClientTypeId = 1
# MAGIC       THEN
# MAGIC         concat(c.SourceSystem, '_RLEP_', c.ClientId)
# MAGIC       WHEN
# MAGIC         c.IsClient = 'false'
# MAGIC         and c.ClientTypeId in (2, 3)
# MAGIC       THEN
# MAGIC         concat(c.SourceSystem, '_RNPP_', c.ClientId)
# MAGIC     END as UniqueCaseId,
# MAGIC     null as InstanceId,
# MAGIC     null as QuestionId,
# MAGIC     rmqa.QuestionText,
# MAGIC     null as QuestionCode,
# MAGIC     null as AnswerValue,
# MAGIC     rmqa.AnswerText
# MAGIC     from
# MAGIC     Legacy2_questions rmqa
# MAGIC     left join Legacy2_client c on c.ClientId = rmqa.ClientId 
# MAGIC   Where 
# MAGIC   c.ClientTypeId in (1,2,3)

# COMMAND ----------

# DBTITLE 1,Direct KN1 Logic to get KN1 Question and Answer
 
  # %sql
  # CREATE
  # OR REPLACE TEMPORARY VIEW KN1_QuestionAnswer AS
  # select
  #   distinct 'KN1' as Application,
  # CONCAT('GIC_',gic.COD_INSTITUCIONAL) as LocalSystemIdentifier,
  #   gic.SEQ_PESSOA as ClientID,
  #   kn_cont.ID_CONTRAPARTE as CaseID,
  #   CASE
  #     WHEN gic.SEQ_TIPO_PESSOA = 2 THEN concat('KN1_NP_NPPC_', CaseId)
  #     ELSE concat('KN1_LE_', CaseId)
  #   END as UniqueCaseId,
  #   gram.InstanceId,
  #   gram.QuestionId,
  #   gram.QuestionText,
  #   gram.QuestionCode,
  #   gram.AnswerValue,
  #   gram.AnswerText
  # from
  #   ADKYC_CONTRAPARTES kn_cont
  #   left join ADRBB_CONTRAPARTES_GRAM stat on kn_cont.ID_CONTRAPARTE = stat.ID_CONTRAPARTE
  #   LEFT join ADKYC_CONTRAPARTES_COMPL kncompl on kn_cont.ID_CONTRAPARTE = kncompl.ID_CONTRAPARTE
  #   INNER JOIN pessoa gic on kncompl.CD_CONTRAPARTE = gic.COD_INSTITUCIONAL
  #   left join Party_RiskModelInstanceQuestionAnswers gram on stat.ID_GRAM_IDENTITY = gram.sourcesystemreferenceid
  #   and SourceSystemName = 'Brazil - KN1'


# COMMAND ----------

# DBTITLE 1,Old Logic to get KN1 Question and Answer
 
  # %sql
  # CREATE
  # OR REPLACE TEMPORARY VIEW KN1_QuestionAnswer AS
  # select
  #   distinct 'KN1' as Application,
  # CONCAT('GIC_',gic.COD_INSTITUCIONAL) as LocalSystemIdentifier,
  #   gic.COD_INSTITUCIONAL as ClientID,
  #   kc.CaseID,
  #   CASE
  #     WHEN gic.SEQ_TIPO_PESSOA = 2 THEN concat('KN1_NP_NPPC_', kc.CaseId)
  #     ELSE concat('KN1_LE_', kc.CaseId)
  #   END as UniqueCaseId,
  #   gram.InstanceId,
  #   gram.QuestionId,
  #   gram.QuestionText,
  #   gram.QuestionCode,
  #   gram.AnswerValue,
  #   gram.AnswerText
  # from
  #   ADKYC_CONTRAPARTES kn_cont
  #   Inner join KN1Cases kc on kn_cont.ID_CONTRAPARTE = kc.CaseID
  #   left  join ADRBB_CONTRAPARTES_GRAM stat on kn_cont.ID_CONTRAPARTE = stat.ID_CONTRAPARTE
  #   INNER JOIN pessoa gic on kc.gicid = gic.COD_INSTITUCIONAL
  #   left  join Party_RiskModelInstanceQuestionAnswers gram on stat.ID_GRAM_IDENTITY = gram.InstanceId
  #   and SourceSystemName = 'Brazil - KN1'


# COMMAND ----------

# DBTITLE 1,New Logic to get KN1 Question and Answer
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW KN1_QuestionAnswer AS
# MAGIC -- Get all cases with CaseCompletedDate
# MAGIC WITH kn1_cases AS (
# MAGIC   SELECT CaseID AS CaseId, GicID AS ClientId, IsLatestCase, CaseCompletedDate
# MAGIC   FROM KN1Cases
# MAGIC ),
# MAGIC
# MAGIC -- Get GRAM answers for cases
# MAGIC gram_answers AS (
# MAGIC   SELECT kc.CaseId, kc.ClientId, kc.IsLatestCase, kc.CaseCompletedDate,
# MAGIC          gram.InstanceId, gram.QuestionId, gram.QuestionText,
# MAGIC          gram.QuestionCode, gram.AnswerValue, gram.AnswerText
# MAGIC   FROM kn1_cases kc
# MAGIC   LEFT JOIN ADRBB_CONTRAPARTES_GRAM stat ON kc.CaseId = stat.ID_CONTRAPARTE
# MAGIC   LEFT JOIN Party_RiskModelInstanceQuestionAnswers gram
# MAGIC          ON stat.ID_GRAM_IDENTITY = gram.InstanceId
# MAGIC         AND gram.SourceSystemName = 'Brazil - KN1'
# MAGIC   WHERE gram.InstanceId IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC -- Identify cases with GRAM answers
# MAGIC gram_cases AS (
# MAGIC   SELECT DISTINCT CaseId FROM gram_answers
# MAGIC ),
# MAGIC
# MAGIC -- Get KN1 answers
# MAGIC kn1_answers AS (
# MAGIC   SELECT a.ID_CONTRAPARTE AS CaseId,
# MAGIC          CAST(a.ID_CONTRAPARTE AS STRING) AS InstanceId,
# MAGIC          q.ID_QUESTAO AS QuestionId, q.DE_QUESTAO AS QuestionText,
# MAGIC          q.CD_QUESTAO AS QuestionCode,
# MAGIC          COALESCE(CAST(a.CD_RESPOSTA AS STRING), CAST(a.VL_PONTUACAO_RESP AS STRING)) AS AnswerValue,
# MAGIC          COALESCE(NULLIF(TRIM(a.DE_RESPOSTA_COMPL), ''),
# MAGIC                   NULLIF(TRIM(a.DE_RESPOSTA), ''),
# MAGIC                   CAST(a.CD_RESPOSTA AS STRING)) AS AnswerText
# MAGIC   FROM ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS a
# MAGIC   LEFT JOIN ADRBB_QUESTOES q ON q.ID_QUESTAO = a.ID_QUESTAO
# MAGIC ),
# MAGIC
# MAGIC -- KN1 fallback for cases without GRAM
# MAGIC kn1_fallback AS (
# MAGIC   SELECT kc.ClientId, kc.CaseId, kc.IsLatestCase, kc.CaseCompletedDate,
# MAGIC          ka.InstanceId, ka.QuestionId, ka.QuestionText,
# MAGIC          ka.QuestionCode, ka.AnswerValue, ka.AnswerText
# MAGIC   FROM kn1_cases kc
# MAGIC   LEFT JOIN gram_cases gc ON gc.CaseId = kc.CaseId
# MAGIC   LEFT JOIN kn1_answers ka ON ka.CaseId = kc.CaseId
# MAGIC   WHERE gc.CaseId IS NULL
# MAGIC ),
# MAGIC
# MAGIC -- UNION all rows, keeping SourceSystem and CaseCompletedDate only within this CTE
# MAGIC finalqueryunion AS (
# MAGIC   SELECT
# MAGIC       CONCAT('GIC_', gic.COD_INSTITUCIONAL) AS LocalSystemIdentifier,
# MAGIC       gic.COD_INSTITUCIONAL AS ClientID,
# MAGIC       g.CaseId, 
# MAGIC       CASE WHEN gic.SEQ_TIPO_PESSOA = 2 THEN CONCAT('KN1_NP_NPPC_', g.CaseId)
# MAGIC            ELSE CONCAT('KN1_LE_', g.CaseId) END AS UniqueCaseId,
# MAGIC       'GRAM' AS SourceSystem,
# MAGIC       g.InstanceId, g.QuestionId, g.QuestionText,
# MAGIC       g.QuestionCode, g.AnswerValue, g.AnswerText,
# MAGIC       g.IsLatestCase,
# MAGIC       g.CaseCompletedDate
# MAGIC   FROM gram_answers g
# MAGIC   INNER JOIN pessoa gic ON g.ClientId = gic.COD_INSTITUCIONAL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC       CONCAT('GIC_', gic.COD_INSTITUCIONAL) AS LocalSystemIdentifier,
# MAGIC       gic.COD_INSTITUCIONAL AS ClientID,
# MAGIC       k.CaseId, 
# MAGIC       CASE WHEN gic.SEQ_TIPO_PESSOA = 2 THEN CONCAT('KN1_NP_NPPC_', k.CaseId)
# MAGIC            ELSE CONCAT('KN1_LE_', k.CaseId) END AS UniqueCaseId,
# MAGIC       'KN1' AS SourceSystem,
# MAGIC       k.InstanceId, k.QuestionId, k.QuestionText,
# MAGIC       k.QuestionCode, k.AnswerValue, k.AnswerText,
# MAGIC       k.IsLatestCase,
# MAGIC       k.CaseCompletedDate
# MAGIC   FROM kn1_fallback k
# MAGIC   INNER JOIN pessoa gic ON k.ClientId = gic.COD_INSTITUCIONAL
# MAGIC )
# MAGIC
# MAGIC -- Final selected columns
# MAGIC SELECT Distinct
# MAGIC     'KN1' as Application,
# MAGIC     LocalSystemIdentifier,
# MAGIC     ClientID,
# MAGIC     CaseId,
# MAGIC     UniqueCaseId,
# MAGIC     InstanceId,
# MAGIC     QuestionId,
# MAGIC     QuestionText,
# MAGIC     QuestionCode,
# MAGIC     AnswerValue,
# MAGIC     AnswerText
# MAGIC FROM finalqueryunion
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ######KN1 QUESTION AND ANSWER

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ##### CREATING A DATAFRAME BY COMBINING ALL THE QUESTION ANSWER INFORMATION FROM DIFFERENT SOURCESYSTEMS AND LOADING THE FINAL DATAOBJECT TO SA RADAR STORAGE ACCOUNT

# COMMAND ----------

# DBTITLE 1,Combining Question Answer from all SourceSystems
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CDD_CaseQuestionAnswers AS
# MAGIC select * FROM GCOB_QuestionAnswer
# MAGIC UNION
# MAGIC select * FROM Legacy2_QuestionAnswer
# MAGIC UNION
# MAGIC select * from KN1_QuestionAnswer

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_CDDcasequestionanswers=spark.table('CDD_CaseQuestionAnswers')

# COMMAND ----------

df_final_party_CDDcasequestionanswers=add_party_identifier(df_party_CDDcasequestionanswers,df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 1,Write Data to SA storage
save_to_saradar_storage_account(df_final_party_CDDcasequestionanswers, party_CDDcasequestionanswers_dataobject, radar_datamodel_version_number, environment)
