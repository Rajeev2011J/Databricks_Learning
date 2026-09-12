# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To create a dataobject to have all information about a CDD Case  for all W&R Sourcesystems
# MAGIC
# MAGIC #### Authors
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC - Mahalakshmi.Vengateswaran@rabobank.com
# MAGIC - Prasad.gadidala@rabobank.com
# MAGIC
# MAGIC ##### Flow of Logic
# MAGIC - Reading GCOB,Legacy2,GCDS,GIC,KN1 Caseservice dataobjects from GDP
# MAGIC - Fetching the Cases details of GCOB Clients from party_case_client_details
# MAGIC - Fetching the Cases details of Legacy2 Clients from Legacy2_case_client_details
# MAGIC - Fetching the Cases details of GIC Clients from KN1 Dataobjects
# MAGIC - Combining all the case related information from different sourcesystems
# MAGIC - Loading the final dataobject to SA RADAR Storage account
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC   - Developer   :   Date          :   PBI No.     :   Changes done
# MAGIC   - Abhishek    :   9-July-2025   :   13019364    :   Changes CaseStatusName for KN1 
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

# COMMAND ----------

# DBTITLE 1,Importing modules from RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1

# COMMAND ----------

# DBTITLE 1,Defining the dataobject name
party_CDDcase_dataobject='Party_CDDCase'

# COMMAND ----------

# DBTITLE 1,Defining the SARADAR Storage account
SARADAR='saradar'+environment

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

# DBTITLE 1,Reading Party_SystemIdentifier dataobject
df_Party_SystemIdentifier=spark.read.parquet(f'abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/1/data/{load_dts}/*.parquet')

# COMMAND ----------

# DBTITLE 1,Reading Legacy2 Dataobjects from GDP
# List of datasets from GDP
load_df = [
'Legacy2_case_client_details',
'Legacy2_risk']

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

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
,'ADKYC_CONTRAPARTES_FASES'
,'ADKYC_SITUACOES'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='KN1', Dataobject=row.GDPname)

# COMMAND ----------

# DBTITLE 1,Reading KN1 Cases
df_Party_KN1Cases = spark.read.parquet(f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_KN1Cases/1/data/{load_dts}/*.parquet")
df_Party_KN1Cases.createOrReplaceTempView("KN1Cases")

# COMMAND ----------

# MAGIC %md
# MAGIC #####TRANSFORMATION TO GET Party_CDDCase OBJECT 

# COMMAND ----------

# DBTITLE 1,Risk level mapping for GIC
#Static Other Risk Level
Risk_list = [0,1,2,3,4,9999]
Risk_Description = ['Risk not assigned', 'Unacceptable',  'High', 'Medium','Low','Unknown']
# create pyspark dataframe from lists
df_gic_static_RiskLevel = spark.createDataFrame(zip(Risk_list, Risk_Description), ['GIC_Risk_Id', 'GIC_Risk_Description'])
df_gic_static_RiskLevel.createOrReplaceTempView('gic_static_Other_RiskLevel')

# COMMAND ----------

# DBTITLE 1,Adding additional attributes to GCOB CaseClient Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_CaseClientDetails As
# MAGIC select * 
# MAGIC ,Case
# MAGIC       when ClientType = 'Legal Entity' then concat('LEC_', GcobId)
# MAGIC       when ClientType in (
# MAGIC         'Natural Person',
# MAGIC         'Natural Person acting in a Professional Capacity (NPPC)'
# MAGIC       ) then concat('NP_NPPC_', GcobId)
# MAGIC     End as UniquePartyId
# MAGIC , MAX(CASE WHEN IsLatestApprovedVersionOfClient = true THEN CaseId END) OVER (PARTITION BY GcobId) AS LatestApprovedCaseId
# MAGIC
# MAGIC from party_case_client_details
# MAGIC where CaseStatusName <> 'Cancelled'

# COMMAND ----------

# MAGIC %md
# MAGIC ######GCOB CASE INFORMATION

# COMMAND ----------

# DBTITLE 1,Logic to get GCOB Cases Information
# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW GCOB_Cases AS
# MAGIC Select
# MAGIC   DISTINCT c.SourceSystem as Application,
# MAGIC   CONCAT('GCOB_',c.UniquePartyId) AS LocalSystemIdentifier,
# MAGIC   c.ClientId,
# MAGIC   c.CaseId,
# MAGIC   CASE
# MAGIC     WHEN c.ClientType like 'Legal%' THEN concat(c.SourceSystem, '_LE_', c.CaseId)
# MAGIC     WHEN c.ClientType LIKE 'Natural%' THEN concat(c.SourceSystem, '_NP_NPPC_', c.CaseId)
# MAGIC   END as UniqueCaseId,
# MAGIC   c.LatestApprovedCaseId,
# MAGIC   c.ReviewTypeName,
# MAGIC   c.CaseStatusName,
# MAGIC   c.ReviewReason,
# MAGIC   c.ReviewReasonOtherExplanation,
# MAGIC   CASE
# MAGIC     WHEN c.ReviewTypeName = 'Event Driven Review' THEN c.EdrReason
# MAGIC     WHEN c.ReviewTypeName = 'Amendment' THEN c.AmendmentReason
# MAGIC     WHEN c.ReviewTypeName = 'Change of Client Owner' THEN c.ClientOwnerChangeReason
# MAGIC     WHEN c.ReviewTypeName = 'Product Offboarding' THEN c.OffBoardingReason
# MAGIC     WHEN c.ReviewTypeName = 'Product Offboarding (Resume)' THEN c.OffBoardingReason
# MAGIC     WHEN c.ReviewTypeName = 'Tailored Event Assessment' THEN c.TEAReviewReasonDescription
# MAGIC     ELSE NULL
# MAGIC   END AS ReviewTypeReason,
# MAGIC   CASE
# MAGIC     WHEN c.ReviewTypeName = 'Event Driven Review' THEN c.EdrOtherReason
# MAGIC     WHEN c.ReviewTypeName = 'Tailored Event Assessment' THEN c.TeaOtherReason
# MAGIC     ELSE NULL
# MAGIC   END AS ReviewTypeOtherReason,
# MAGIC   c.NextReviewDate,
# MAGIC   c.SubmitToClientCommittee,
# MAGIC   c.CaseDecisionMotivation,
# MAGIC   c.CurrentAssignee,
# MAGIC   c.InitiationInProgressAssignee,
# MAGIC   c.ReadyForKYCAssessmentDate,
# MAGIC   c.KYCAssessmentInProgressAssignee,
# MAGIC   c.LastKYCAnalyst,
# MAGIC   c.LastSentForKYCAssesment,
# MAGIC   c.DateSubmittedFor4EyeCheck,
# MAGIC   c.4EyeCheckReviewer,
# MAGIC   c.Last4EyeCheckReviewer,
# MAGIC   c.LastSentFor4EyeCheck,
# MAGIC   c.DateSubmittedForSignOff,
# MAGIC   c.ClientOwnerSignOffDate,
# MAGIC   c.LastProductOffboardingAnalyst,
# MAGIC   c.ValidatedRiskLevel,
# MAGIC   c.CaseCreationDate,
# MAGIC   c.ScheduledCompletionDate,
# MAGIC   c.CaseCompletedDate,
# MAGIC   c.FinalDecisionDate
# MAGIC from
# MAGIC   Gcob_CaseClientDetails c
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ######LEGACY2 CASE INFORMATION

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

# DBTITLE 1,Logic to get Legacy2 Case Information
# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW Legacy2_Cases AS
# MAGIC Select
# MAGIC   distinct 'LEGACY2' as Application,
# MAGIC  CONCAT('LEGACY2_', CASE
# MAGIC     WHEN IsClient = 'true'
# MAGIC     and ClientTypeId = 1 THEN concat('LE_', c.GcobId)
# MAGIC     WHEN IsClient = 'true'
# MAGIC     and ClientTypeId in (2, 3) THEN concat('NP_NPPC_', c.GcobId)
# MAGIC     WHEN IsClient = 'false'
# MAGIC     and ClientTypeId = 1 THEN concat('RLE_', c.GcobId)
# MAGIC     WHEN IsClient = 'false'
# MAGIC     and ClientTypeId in (2, 3) THEN concat('RNP_', c.GcobId) END) as LocalSystemIdentifier,
# MAGIC   c.ClientId,
# MAGIC   null as CaseId,
# MAGIC   CASE
# MAGIC     WHEN c.IsClient = 'true'
# MAGIC     and c.ClientTypeId = 1 THEN concat(c.SourceSystem, '_LEC_', c.ClientId)
# MAGIC     WHEN c.IsClient = 'true'
# MAGIC     and c.ClientTypeId in (2, 3) THEN concat(c.SourceSystem, '_NP_NPPC_', c.ClientId)
# MAGIC     WHEN c.IsClient = 'false'
# MAGIC     and c.ClientTypeId = 1 THEN concat(c.SourceSystem, '_RLEP_', c.ClientId)
# MAGIC     WHEN c.IsClient = 'false'
# MAGIC     and c.ClientTypeId in (2, 3) THEN concat(c.SourceSystem, '_RNPP_', c.ClientId)
# MAGIC   END as UniqueCaseId,
# MAGIC   c.ClientId as LatestApprovedCaseId,
# MAGIC   c.ReviewTypeName,
# MAGIC   c.StatusTypeName as CaseStatusName,
# MAGIC   null as ReviewReason,
# MAGIC   null as ReviewReasonOtherExplanation,
# MAGIC   CASE
# MAGIC     WHEN c.ReviewTypeName = 'Client Offboarding' THEN c.OffBoardingReasonDescription
# MAGIC     WHEN c.ReviewTypeName = 'Tailored Event Assessment' THEN c.TEAReviewReasonDescription
# MAGIC     ELSE NULL
# MAGIC   END AS ReviewTypeReason,
# MAGIC   CASE
# MAGIC     WHEN c.ReviewTypeName = 'Tailored Event Assessment' THEN c.TeaOtherReason
# MAGIC     ELSE NULL
# MAGIC   END AS ReviewTypeOtherReason,
# MAGIC   c.NextReviewDate,
# MAGIC   null as SubmitToClientCommittee,
# MAGIC   null as CaseDecisionMotivation,
# MAGIC   c.CurrentAssignee,
# MAGIC   c.InitiationInProgressAssignee,
# MAGIC   c.ReadyForKYCAssessmentDate,
# MAGIC   c.KYCAssessmentInProgressAssignee,
# MAGIC   c.LastKYCAnalyst,
# MAGIC   c.LastSentForKYCAssesment,
# MAGIC   c.DateSubmittedFor4EyeCheck,
# MAGIC   c.Eye4CheckReviewer as 4EyeCheckReviewer,
# MAGIC   c.Last4EyeCheckReviewer,
# MAGIC   c.LastSentFor4EyeCheck,
# MAGIC   c.DateSubmittedForSignOff,
# MAGIC   c.ClientOwnerSignOffDate,
# MAGIC   null as LastProductOffboardingAnalyst,
# MAGIC   r.ValidatedRiskLevel,
# MAGIC   c.CaseCreationDate,
# MAGIC   c.ScheduledCompletionDate,
# MAGIC   c.CaseCompletedDate,
# MAGIC   c.FinalDecisionDate
# MAGIC from
# MAGIC   Legacy2_client c
# MAGIC   left join Legacy2_risk r on r.ClientId = c.ClientId
# MAGIC where
# MAGIC   c.IsClient = 'true'
# MAGIC   and c.ClientTypeId in (1, 2, 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ######KN1 CASE INFORMATION

# COMMAND ----------

# DBTITLE 1,Logic to get KN1 Case Information
# %sql
# CREATE
# OR REPLACE TEMPORARY VIEW KN1_Cases AS
# select 
#   distinct 'KN1' as Application,
#   CONCAT('GIC_',gic.COD_INSTITUCIONAL) as LocalSystemIdentifier,
#   gic.COD_INSTITUCIONAL as ClientID,
#   kn_cont.ID_CONTRAPARTE as CaseID,
#   CASE
#     WHEN gic.SEQ_TIPO_PESSOA = 2 THEN concat('KN1_NP_NPPC_', CaseId)
#     ELSE concat('KN1_LE_', CaseId)
#   END as UniqueCaseId,
#   MAX( kn_cont.ID_CONTRAPARTE) OVER (PARTITION BY gic.SEQ_PESSOA) AS LatestApprovedCaseId,
#   null as ReviewTypeName,
#   --ast.NM_SITUACAO as CaseStatusName,
#   case when kn_cont.CD_SITUACAO = 0 then 'In progress'
#        when kn_cont.CD_SITUACAO = 1 then 'Approved'
#        when kn_cont.CD_SITUACAO = 2 then 'Not Approved'
#        when kn_cont.CD_SITUACAO = 3 then 'Cancelled'
#        else 'Undefined'
#   end as CaseStatusName,
#   --cs.CaseStatus,
#   --acf.DE_ANALISE as ReviewReason,
#   --acf.DE_RESULTADO_SISTEMA as ReviewReasonOtherExplanation,
#   null as ReviewTypeReason,
#   null as ReviewTypeOtherReason,
#   CASE
#     WHEN kn_cont.DT_RENOVACAO = '1900-01-01 00:00:00.000' THEN ifnull(kn_cont.DT_RENOVACAO, '')
#     ELSE kn_cont.DT_RENOVACAO
#   END AS NextReviewDate,
#   null as SubmitToClientCommittee,
#   null as CaseDecisionMotivation,
#   null as CurrentAssignee,
#   null as InitiationInProgressAssignee,
#   null as ReadyForKYCAssessmentDate,
#   null as KYCAssessmentInProgressAssignee,
#   null as LastKYCAnalyst,
#   null as LastSentForKYCAssesment,
#   null as DateSubmittedFor4EyeCheck,
#   null as 4EyeCheckReviewer,
#   null as Last4EyeCheckReviewer,
#   null as LastSentFor4EyeCheck,
#   kn_cont.DTHR_FIM as DateSubmittedForSignOff,
#   null as ClientOwnerSignOffDate,
#   null as LastProductOffboardingAnalyst,
#   gicrs.GIC_Risk_Description as ValidatedRiskLevel,
#   kn_cont.DTHR_INICIO as CaseCreationDate,
#   null as ScheduledCompletionDate,
#   kn_cont.DTHR_FIM as CaseCompletedDate,
#   null as FinalDecisionDate
# from
#   ADKYC_CONTRAPARTES kn_cont
#   Inner join adkyc_contrapartes_compl kncompl on kn_cont.ID_CONTRAPARTE = kncompl.ID_CONTRAPARTE
#   Inner  JOIN pessoa gic on kncompl.CD_CONTRAPARTE = gic.COD_INSTITUCIONAL
#   Left join gic_static_Other_RiskLevel gicrs on kn_cont.CD_RISCO_MANUAL=gicrs.GIC_Risk_Id
#   Left join  ADKYC_CONTRAPARTES_FASES acf on acf.ID_CONTRAPARTE=kn_cont.ID_CONTRAPARTE
#   --Left join ADKYC_SITUACOES ast on ast.CD_SITUACAO=kn_cont.CD_SITUACAO


# COMMAND ----------

# DBTITLE 1,Logic to get KN1 Case Information
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW KN1_Cases AS
# MAGIC select 
# MAGIC   distinct 'KN1' as Application,
# MAGIC   CONCAT('GIC_',gic.COD_INSTITUCIONAL) as LocalSystemIdentifier,
# MAGIC   gic.COD_INSTITUCIONAL as ClientID,
# MAGIC   cs.CaseID,
# MAGIC   CASE
# MAGIC     WHEN gic.SEQ_TIPO_PESSOA = 2 THEN concat('KN1_NP_NPPC_', cs.CaseId)
# MAGIC     ELSE concat('KN1_LE_', cs.CaseId)
# MAGIC   END as UniqueCaseId,
# MAGIC   MAX( cs.CaseID) OVER (PARTITION BY gic.SEQ_PESSOA) AS LatestApprovedCaseId,
# MAGIC   cs.ReviewType as ReviewTypeName,
# MAGIC   cs.CaseStatus as CaseStatusName,
# MAGIC   acf.DE_ANALISE as ReviewReason,
# MAGIC   acf.DE_RESULTADO_SISTEMA as ReviewReasonOtherExplanation,
# MAGIC   null as ReviewTypeReason,
# MAGIC   null as ReviewTypeOtherReason,
# MAGIC   cs.NextReviewDate,
# MAGIC   null as SubmitToClientCommittee,
# MAGIC   null as CaseDecisionMotivation,
# MAGIC   null as CurrentAssignee,
# MAGIC   null as InitiationInProgressAssignee,
# MAGIC   null as ReadyForKYCAssessmentDate,
# MAGIC   null as KYCAssessmentInProgressAssignee,
# MAGIC   null as LastKYCAnalyst,
# MAGIC   null as LastSentForKYCAssesment,
# MAGIC   null as DateSubmittedFor4EyeCheck,
# MAGIC   null as 4EyeCheckReviewer,
# MAGIC   null as Last4EyeCheckReviewer,
# MAGIC   null as LastSentFor4EyeCheck,
# MAGIC   kn_cont.DTHR_FIM as DateSubmittedForSignOff,
# MAGIC   null as ClientOwnerSignOffDate,
# MAGIC   null as LastProductOffboardingAnalyst,
# MAGIC   gicrs.GIC_Risk_Description as ValidatedRiskLevel,
# MAGIC   cs.CaseCreationDate,
# MAGIC   null as ScheduledCompletionDate,
# MAGIC   cs.CaseCompletedDate,
# MAGIC   cs.FinalDecisionDate
# MAGIC from KN1Cases cs
# MAGIC   Inner join ADKYC_CONTRAPARTES kn_cont on cs.Caseid= kn_cont.ID_CONTRAPARTE 
# MAGIC   Inner JOIN pessoa gic on cs.GicId = gic.COD_INSTITUCIONAL
# MAGIC   Left join gic_static_Other_RiskLevel gicrs on kn_cont.CD_RISCO_ATRIBUIDO=gicrs.GIC_Risk_Id
# MAGIC   Left join  ADKYC_CONTRAPARTES_FASES acf on acf.ID_CONTRAPARTE=kn_cont.ID_CONTRAPARTE

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CREATING A DATAFRAME BY COMBINING ALL THE CASE INFORMATION FROM DIFFERENT SOURCESYSTEMS AND LOADING THE FINAL DATAOBJECT TO SA RADAR STORAGE ACCOUNT
# MAGIC

# COMMAND ----------

# DBTITLE 1,Combining Case Information from all SourceSystems
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CDD_Cases AS
# MAGIC select * FROM GCOB_Cases
# MAGIC UNION
# MAGIC select * from Legacy2_Cases
# MAGIC UNION
# MAGIC select * from KN1_Cases

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_CDDcase=spark.table('CDD_Cases')

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_final_party_CDDcase=add_party_identifier(df_party_CDDcase,df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
save_to_saradar_storage_account(df_final_party_CDDcase, party_CDDcase_dataobject, radar_datamodel_version_number, environment)
