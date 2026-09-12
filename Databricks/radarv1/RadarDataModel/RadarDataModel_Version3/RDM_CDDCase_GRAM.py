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
# MAGIC - Prajit.tatari@rabobank.com
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
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done | Version
# MAGIC |----------|----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |9-July-2025 |13019364 |Changes CaseStatusName for KN1	| 1	
# MAGIC | Prajit Tatari	 |29-Aug-2025 |13538827 |Adding RiskModelName  | 2
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load | 2
# MAGIC | Prajit Tatari	 |11-Feb-2026 |15058674 |Adding MDM RANZ data from GDP  | 2
# MAGIC |Rhea Gupta | 30-Mar-2026 |15580816 | Included RANZ in version 3 | 3
# MAGIC | Mahalakshmi V | 20-May-2026 | 16071492 | Included RANZ V4 |4
# MAGIC | Gopi Prasad K | 11-Aug-2026 | 16107896 | Party type changes for Legacy2 Source system | 3

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
RANZ_ReadStorage = os.environ['AU_GDP_Defined_Storage_Account']
environment=os.environ['ENV']
radar_datamodel_version_number=3

# COMMAND ----------

# DBTITLE 1,Defining Load_Date and RunType
dbutils.widgets.text("Load_Date", "")
dbutils.widgets.text("RunType", "daily")  # default is daily

Load_Date = dbutils.widgets.get("Load_Date")
RunType = dbutils.widgets.get("RunType").lower()
if Load_Date:
    RunType="historical"
    
if not Load_Date:
    Load_Date = datetime.today().strftime('%Y%m%d')

print(f"Running for Load Date: {Load_Date}")

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
authenticate_storage_account(RANZ_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Deriving required date parameters
# Derive the date for which data has to be processed from GDP
load_dts = f"EDL_LOAD_DTS={Load_Date}*"
print (load_dts)

# COMMAND ----------

# DBTITLE 1,Reading GCOB CaseService Dataobjects from GDP
# List of datasets from GDP
load_df = [
'party_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier dataobject
df_Party_SystemIdentifier=spark.read.parquet(f'abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/3/data/{load_dts}/*.parquet')

# COMMAND ----------

# DBTITLE 1,Reading Legacy2 Dataobjects from GDP
# List of datasets from GDP
load_df = [
'Legacy2_case_client_details',
'Legacy2_risk']

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading GIC Dataobjects from GDP
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'pessoa'
]})
#gic_load_dts
# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading KN1 Dataobjects from GDP
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'ADKYC_CONTRAPARTES'
,'ADRBB_CONTRAPARTES_GRAM'
,'ADKYC_CONTRAPARTES_COMPL'
,'ADKYC_CONTRAPARTES_FASES'
,'ADKYC_SITUACOES'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='KN1', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# List of dataobjects from GDP
load_df =[
# 'Party_RiskModelInstanceQuestionAnswers'
# , 'Party_RiskModelCategories'
'Party_RiskModelInstance'
# , 'party_WRCDDModel'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading KN1 Cases
df_Party_KN1Cases = spark.read.parquet(f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_KN1Cases/3/data/{load_dts}/*.parquet")
df_Party_KN1Cases.createOrReplaceTempView("KN1Cases")

# COMMAND ----------

# DBTITLE 1,MDM RANZ Cases
load_df = [
    'c_b_party_xref'
,'c_lkp_cdd_risk_rating_xref'
,'c_b_due_diligence_xref'
,'c_b_contract_xref'
,'c_b_contr_rol_party_xref'
]
load_ranz_v4_rdm_tables(load_df, Load_Date)

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

#Creating Static view for Risk Level
Risk_list = [0,1,2,3,4,5,6,7]
Risk_Description = ['Undefined', 'No', 'Low', 'Medium', 'High','Super','Extra High','Unacceptable']
#Creating spark dataframe from lists
df_gcob_static_RiskLevel = spark.createDataFrame(zip(Risk_list, Risk_Description), ['Id', 'Description'])
df_gcob_static_RiskLevel.createOrReplaceTempView('static_RiskLevel')

#Creating Static view for Risk Level
Risk_list = [0,1,2,3,4,9999]
Risk_Description = ['RiskNotAssigned','Unacceptable', 'High','Medium', 'Low', 'Unknown']
df_KN1_static_RiskLevel = spark.createDataFrame(zip(Risk_list, Risk_Description), ['Id', 'Description'])
df_KN1_static_RiskLevel.createOrReplaceTempView('kn1_static_RiskLevel')

#Creating Static view for KN1 Risk Level
Risk_list = [0,1,2,3,4]
Risk_Description = ['Incomplete', 'Low', 'Medium', 'High','Unacceptable']
#Creating spark dataframe from lists
df_gcob_RiskLevel = spark.createDataFrame(zip(Risk_list, Risk_Description), ['RiskModelRiskLevelId', 'DisplayName'])
df_gcob_RiskLevel.createOrReplaceTempView('RiskModelRiskLevel')

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
# MAGIC   CONCAT('GCOB_',c.UniquePartyId) AS LocalSystemIdentifier,
# MAGIC   c.ClientId,
# MAGIC   c.CaseId,
# MAGIC   CASE
# MAGIC     WHEN c.ClientType like 'Legal%' THEN concat(c.SourceSystem, '_LEC_', c.CaseId)
# MAGIC     WHEN c.ClientType LIKE 'Natural%' THEN concat(c.SourceSystem, '_NP_NPPC_', c.CaseId)
# MAGIC   END as UniqueCaseId,
# MAGIC   c.LatestApprovedCaseId,
# MAGIC   c.ReviewTypeName,
# MAGIC   c.CaseStatusName,
# MAGIC   c.ReviewReason,
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
# MAGIC     WHEN c.ReviewTypeName = 'Event Driven Review' and c.EdrReason='Other' THEN c.EdrOtherReason
# MAGIC     WHEN c.ReviewTypeName = 'Tailored Event Assessment' and c.TEAReviewReasonDescription = 'Other' THEN c.TeaOtherReason
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
# MAGIC   c.FinalDecisionDate,
# MAGIC   c.RiskModelName,
# MAGIC   c.ModelCalculatedRiskLevel as CalculatedRiskLevel,
# MAGIC   c.ModelRecalculatedRiskLevel as ReCalculatedRiskLevel,
# MAGIC   c.RiskDeviationReason,
# MAGIC   null as DeactivationDate
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
# MAGIC  CONCAT('LEGACY2_', CASE
# MAGIC     WHEN IsClient = 'true'
# MAGIC     and ClientTypeId = 1 THEN concat('LEC_', c.GcobId)
# MAGIC     WHEN IsClient = 'true'
# MAGIC     and ClientTypeId in (2, 3) THEN concat('NP_NPPC_', c.GcobId)
# MAGIC     WHEN IsClient = 'false'
# MAGIC     and ClientTypeId = 1 THEN concat('RLEP_', c.GcobId)
# MAGIC     WHEN IsClient = 'false'
# MAGIC     and ClientTypeId in (2, 3) THEN concat('RNPP_', c.GcobId) END) as LocalSystemIdentifier,
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
# MAGIC   c.FinalDecisionDate,
# MAGIC   r.RiskModelName,
# MAGIC   r.CalculatedRiskLevel,
# MAGIC   r.ReCalculatedRiskLevel,
# MAGIC   r.RiskDeviationReason,
# MAGIC   null as DeactivationDate
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
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW KN1_Cases AS
# MAGIC select 
# MAGIC   CONCAT('GIC_',gic.COD_INSTITUCIONAL) as LocalSystemIdentifier,
# MAGIC   gic.COD_INSTITUCIONAL as ClientID,
# MAGIC   cs.CaseID,
# MAGIC   CASE
# MAGIC     WHEN gic.SEQ_TIPO_PESSOA = 2 THEN concat('KN1_NP_NPPC_', cs.CaseId)
# MAGIC     ELSE concat('KN1_LEC_', cs.CaseId)
# MAGIC   END as UniqueCaseId,
# MAGIC   MAX(CASE WHEN cs.CaseStatus <> 'Cancelled' THEN cs.CaseID END) OVER (PARTITION BY gic.SEQ_PESSOA) AS LatestApprovedCaseId,
# MAGIC   cs.ReviewType as ReviewTypeName,
# MAGIC   cs.CaseStatus as CaseStatusName,
# MAGIC   acf.DE_ANALISE as ReviewReason,
# MAGIC   null as ReviewTypeReason,
# MAGIC   acf.DE_RESULTADO_SISTEMA as ReviewTypeOtherReason,
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
# MAGIC   cs.FinalDecisionDate,
# MAGIC   ri.RiskModelName,
# MAGIC   r.Description as CalculatedRiskLevel,
# MAGIC   rl.Description as RecalculatedRiskLevel,
# MAGIC   ri.RiskDeviationReason,
# MAGIC   gic.DTA_DESATIVACAO as DeactivationDate
# MAGIC from KN1Cases cs
# MAGIC   Inner join ADKYC_CONTRAPARTES kn_cont on cs.Caseid= kn_cont.ID_CONTRAPARTE
# MAGIC   Inner JOIN pessoa gic on cs.GicId = gic.COD_INSTITUCIONAL
# MAGIC   left join ADRBB_CONTRAPARTES_GRAM stat on trim(kn_cont.ID_CONTRAPARTE) = trim(stat.ID_CONTRAPARTE)
# MAGIC   Left join gic_static_Other_RiskLevel gicrs on kn_cont.CD_RISCO_ATRIBUIDO=gicrs.GIC_Risk_Id
# MAGIC   Left join  ADKYC_CONTRAPARTES_FASES acf on acf.ID_CONTRAPARTE=kn_cont.ID_CONTRAPARTE
# MAGIC   left join Party_RiskModelInstance ri on trim(stat.ID_GRAM_IDENTITY) = trim(ri.InstanceId)
# MAGIC   left join kn1_static_RiskLevel r on trim(kn_cont.CD_RISCO_MANUAL) = trim(r.Id)
# MAGIC   left join kn1_static_RiskLevel rl on trim(kn_cont.CD_RISCO_CALCULADO) = trim(rl.Id)
# MAGIC     and trim(ri.SourceSystemName) = 'Brazil - KN1'

# COMMAND ----------

# MAGIC %md
# MAGIC MDM RANZ Cases

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW MDM_RANZ_Cases AS
# MAGIC select distinct
# MAGIC CONCAT('RANZ_',p.PKEY_SRC_OBJECT) LocalSystemIdentifier,
# MAGIC p.PKEY_SRC_OBJECT as ClientID,
# MAGIC dd.CASE_ID as CaseID,
# MAGIC null as UniqueCaseId, 
# MAGIC null as LatestApprovedCaseId,
# MAGIC null as ReviewTypeName,
# MAGIC 'Completed' as CaseStatusName,
# MAGIC null as ReviewReason,
# MAGIC null as ReviewTypeReason,
# MAGIC null as ReviewTypeOtherReason,
# MAGIC dd.CDD_NEXT_REVIEW_DT as NextReviewDate,
# MAGIC null as SubmitToClientCommittee,
# MAGIC null as CaseDecisionMotivation,
# MAGIC c.CONTR_NAME as CurrentAssignee,
# MAGIC null as InitiationInProgressAssignee,
# MAGIC c.START_DT as ReadyForKYCAssessmentDate,
# MAGIC null as KYCAssessmentInProgressAssignee,
# MAGIC null as LastKYCAnalyst,
# MAGIC null as LastSentForKYCAssesment,
# MAGIC null as DateSubmittedFor4EyeCheck,
# MAGIC null as 4EyeCheckReviewer,
# MAGIC null as Last4EyeCheckReviewer,
# MAGIC null as LastSentFor4EyeCheck,
# MAGIC null as DateSubmittedForSignOff,
# MAGIC dd.CDD_SIGN_OFF_DT as ClientOwnerSignOffDate,
# MAGIC null as LastProductOffboardingAnalyst,
# MAGIC c.CDD_RISK_RATING_DESC as ValidatedRiskLevel,
# MAGIC dd.START_DT as CaseCreationDate,
# MAGIC dd.CDD_COMPLETION_DT as ScheduledCompletionDate,
# MAGIC dd.END_DT as CaseCompletedDate,
# MAGIC dd.CDD_STRATEGIC_REVIEW_DT as FinalDecisionDate,
# MAGIC  d.RiskModelName,
# MAGIC  e.Description as CalculatedRiskLevel,
# MAGIC  f.Description as RecalculatedRiskLevel,
# MAGIC  d.RiskDeviationReason,
# MAGIC null as DeactivationDate
# MAGIC FROM c_b_party as p
# MAGIC JOIN c_b_contr_rol_party as crp
# MAGIC   ON crp.FK_PARTY_ID = p.ROWID_XREF
# MAGIC JOIN c_b_contract as c
# MAGIC   ON c.CONTR_ID = crp.FK_CONTR_ID
# MAGIC LEFT JOIN c_b_due_diligence as dd
# MAGIC   ON dd.FK_CONTR_ID = c.CONTR_ID
# MAGIC join c_lkp_cdd_risk_rating c
# MAGIC on c.CDD_RISK_RATING_CODE = dd.CDD_RISK_RTG_CD
# MAGIC left join Party_RiskModelInstance d
# MAGIC on d.InstanceId = dd.GRAM_INSTANCE_ID
# MAGIC  and d.SourceSystemName = 'RANZ - PegaCdd'
# MAGIC  left join static_RiskLevel e
# MAGIC  on d.OverallCalculatedRiskLevelId = e.Id
# MAGIC   left join static_RiskLevel f
# MAGIC  on d.OverallRecalculatedRiskLevelId = f.Id

# COMMAND ----------

# MAGIC %skip
# MAGIC select *
# MAGIC FROM c_b_party as p
# MAGIC JOIN c_b_contr_rol_party as crp
# MAGIC     ON crp.FK_PARTY_ID = p.ROWID_XREF
# MAGIC JOIN c_b_contract as c
# MAGIC     ON c.CONTR_ID = crp.FK_CONTR_ID
# MAGIC LEFT JOIN c_b_due_diligence as dd
# MAGIC     ON dd.FK_CONTR_ID = c.CONTR_ID;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CREATING A DATAFRAME BY COMBINING ALL THE CASE INFORMATION FROM DIFFERENT SOURCESYSTEMS AND LOADING THE FINAL DATAOBJECT TO SA RADAR STORAGE ACCOUNT
# MAGIC

# COMMAND ----------

# DBTITLE 1,Combining Case Information from all SourceSystems
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CDD_Cases AS
# MAGIC with combinedData as(
# MAGIC select * FROM GCOB_Cases
# MAGIC UNION
# MAGIC select * from Legacy2_Cases
# MAGIC UNION
# MAGIC select * from KN1_Cases
# MAGIC UNION
# MAGIC select * from MDM_RANZ_Cases
# MAGIC )
# MAGIC
# MAGIC Select *, CASE
# MAGIC     WHEN CaseStatusName in("Approved","Completed") Then "Completed"
# MAGIC     WHEN CaseStatusName in("Cancelled") Then "Cancelled"
# MAGIC     WHEN CaseStatusName in("Migrated") Then "Migrated"
# MAGIC     ELSE "InProgress"
# MAGIC   END as StandardCaseStatus from combinedData

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CDD_Cases_final AS
# MAGIC (
# MAGIC   select 
# MAGIC   LocalSystemIdentifier,
# MAGIC   CASE 
# MAGIC     WHEN ClientId RLIKE '^[0-9]+$' THEN CAST(ClientId AS INT)
# MAGIC     ELSE NULL
# MAGIC   END AS ClientId,
# MAGIC   CAST(ClientID AS STRING) AS ClientId_string,
# MAGIC   CASE 
# MAGIC     WHEN CaseId RLIKE '^[0-9]+$' THEN CAST(CaseId AS INT)
# MAGIC     ELSE NULL
# MAGIC   END AS CaseId,
# MAGIC   CAST(CaseId AS STRING) AS CaseId_string,
# MAGIC   UniqueCaseId,
# MAGIC   LatestApprovedCaseId,
# MAGIC   CASE
# MAGIC     WHEN CaseId IS NOT NULL AND LatestApprovedCaseId IS NOT NULL AND CaseId = LatestApprovedCaseId
# MAGIC     THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS IsLatestApprovedCase,
# MAGIC   ReviewTypeName,
# MAGIC   CaseStatusName,
# MAGIC   array_sort(collect_set(nullif(trim(ReviewReason), ''))) AS ReviewReason,
# MAGIC   ReviewTypeReason,
# MAGIC   array_sort(collect_set(nullif(trim(ReviewTypeOtherReason), ''))) AS ReviewTypeOtherReason,
# MAGIC   NextReviewDate,
# MAGIC   SubmitToClientCommittee,
# MAGIC   CaseDecisionMotivation,
# MAGIC   CurrentAssignee,
# MAGIC   InitiationInProgressAssignee,
# MAGIC   ReadyForKYCAssessmentDate,
# MAGIC   KYCAssessmentInProgressAssignee,
# MAGIC   LastKYCAnalyst,
# MAGIC   LastSentForKYCAssesment,
# MAGIC   DateSubmittedFor4EyeCheck,
# MAGIC   4EyeCheckReviewer,
# MAGIC   Last4EyeCheckReviewer,
# MAGIC   LastSentFor4EyeCheck,
# MAGIC   DateSubmittedForSignOff,
# MAGIC   ClientOwnerSignOffDate,
# MAGIC   LastProductOffboardingAnalyst,
# MAGIC   ValidatedRiskLevel,
# MAGIC   CaseCreationDate,
# MAGIC   ScheduledCompletionDate,
# MAGIC   CaseCompletedDate,
# MAGIC   FinalDecisionDate,
# MAGIC   RiskModelName,
# MAGIC   CalculatedRiskLevel,
# MAGIC   ReCalculatedRiskLevel,
# MAGIC   RiskDeviationReason,
# MAGIC   StandardCaseStatus,
# MAGIC   DeactivationDate
# MAGIC from CDD_Cases
# MAGIC GROUP BY
# MAGIC   LocalSystemIdentifier,
# MAGIC   ClientID,
# MAGIC   CaseID,
# MAGIC   UniqueCaseId,
# MAGIC   LatestApprovedCaseId,
# MAGIC   IsLatestApprovedCase,
# MAGIC   ReviewTypeName,
# MAGIC   CaseStatusName,
# MAGIC   ReviewTypeReason,
# MAGIC   NextReviewDate,
# MAGIC   SubmitToClientCommittee,
# MAGIC   CaseDecisionMotivation,
# MAGIC   CurrentAssignee,
# MAGIC   InitiationInProgressAssignee,
# MAGIC   ReadyForKYCAssessmentDate,
# MAGIC   KYCAssessmentInProgressAssignee,
# MAGIC   LastKYCAnalyst,
# MAGIC   LastSentForKYCAssesment,
# MAGIC   DateSubmittedFor4EyeCheck,
# MAGIC   `4EyeCheckReviewer`,
# MAGIC   Last4EyeCheckReviewer,
# MAGIC   LastSentFor4EyeCheck,
# MAGIC   DateSubmittedForSignOff,
# MAGIC   ClientOwnerSignOffDate,
# MAGIC   LastProductOffboardingAnalyst,
# MAGIC   ValidatedRiskLevel,
# MAGIC   CaseCreationDate,
# MAGIC   ScheduledCompletionDate,
# MAGIC   CaseCompletedDate,
# MAGIC   FinalDecisionDate,
# MAGIC   RiskModelName,
# MAGIC   CalculatedRiskLevel,
# MAGIC   ReCalculatedRiskLevel,
# MAGIC   RiskDeviationReason,
# MAGIC   StandardCaseStatus,
# MAGIC   DeactivationDate
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_CDDcase=spark.table('CDD_Cases_final')

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_final_party_CDDcase=add_party_identifier(df_party_CDDcase,df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 1,Function for Resolve Duplicate LatestApprovedCaseId
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def resolve_latest_approved_case_per_party(df):
    """
    Resolves duplicate IsLatestApprovedCase entries per PartyIdentifier.
    The logic is applied ONLY when a PartyIdentifier has
    more than one IsLatestApprovedCase = TRUE.

    Preference rules:
      1. Source system priority:
         GCOB > KN1 > LEGACY2 > RANZ
      2. If same system, latest CaseCompletedDate gets priority
    """

    #Identifying rows with more than 1 IsLatestApprovedCase

    duplicate_parties = (
        df.filter(F.col("IsLatestApprovedCase") == True)
          .groupBy("PartyIdentifier")
          .count()
          .filter(F.col("count") > 1)
          .select("PartyIdentifier")
    )

    # Add a helper column (NeedsFix) to flag PartyIdentifiers that have more than one latest-approved case.
    #Coalesce is used to replace nulls with False, so every row has a clear True/False indicator. 

    df = (
        df.join(
            duplicate_parties.withColumn("NeedsFix", F.lit(True)),
            on="PartyIdentifier",
            how="left"
        )
        .withColumn("NeedsFix", F.coalesce(F.col("NeedsFix"), F.lit(False)))
    )

    #Adding SourceSystem column which extracts the system name from UniqueCaseId column only for required rows

    df = df.withColumn(
        "SourceSystem",
        F.when(F.col("NeedsFix") == False, None)
         .when(F.col("UniqueCaseId").startswith("GCOB"), "GCOB")
         .when(F.col("UniqueCaseId").startswith("KN1"), "KN1")
         .when(F.col("UniqueCaseId").startswith("Legacy2"), "LEGACY2")
         .when(F.col("UniqueCaseId").startswith("RANZ"), "RANZ")
         .otherwise("UNKNOWN")
    )

    #Assigning Priority GCOB > KN1 > LEGACY2 > RANZ

    df = df.withColumn(
        "SystemPriority",
        F.when(F.col("NeedsFix") == False, None)
         .when(F.col("SourceSystem") == "GCOB", 1)
         .when(F.col("SourceSystem") == "KN1", 2)
         .when(F.col("SourceSystem") == "LEGACY2", 3)
         .when(F.col("SourceSystem") == "RANZ", 4)
         .otherwise(99)
    )

    #Defining a Window function which ranks cases for each PartyIdentifier. Case ordering is first by System Priority (Gcob gets ranked higher than Kn1) and then by the most recent CaseCompletedDate.

    window_spec = Window.partitionBy("PartyIdentifier").orderBy(
        F.col("SystemPriority").asc(),
        F.col("CaseCompletedDate").desc_nulls_last()
    )

    #Apply row number only to rows that need fixing and IsLatestApprovedCase =True. 
    #LatestRank helper column helps decide which row gets final priority. 
      
    df = df.withColumn(
        "LatestRank",
        F.when(
            (F.col("NeedsFix") == True) &
            (F.col("IsLatestApprovedCase") == True),
            F.row_number().over(window_spec)
        )
    )

    #Helper Column FinalLatestApprovedCaseId contains CaseId for cases ranked as the final LatestApprovedCase

    final_latest_approved_cases = (
        df.filter(F.col("LatestRank") == 1)
          .select(
              "PartyIdentifier",
              F.col("CaseId").alias("FinalLatestApprovedCaseId")
          )
    )

    #Updating LatestApprovedCaseId to show only one latest approved case for each PartyIdentifier. 
    df = (
        df.join(final_latest_approved_cases, on="PartyIdentifier", how="left")
        .withColumn(
            "LatestApprovedCaseId",
            F.when(
                F.col("NeedsFix") == True,
                F.col("FinalLatestApprovedCaseId")
            ).otherwise(F.col("LatestApprovedCaseId"))
        )
    )

    #Re-deriving IsLatestApprovedCase flag. 
    df = df.withColumn(
        "IsLatestApprovedCase",
        F.when(
            (F.col("CaseId").isNotNull()) &
            (F.col("LatestApprovedCaseId").isNotNull()) &
            (F.col("CaseId") == F.col("LatestApprovedCaseId")),
            True
        ).otherwise(False)
    )
 
    #Dropping Helper Columns
    df = df.drop(
        "NeedsFix",
        "SourceSystem",
        "SystemPriority",
        "LatestRank",
        "FinalLatestApprovedCaseId"
    )

    return df

# COMMAND ----------

#Using function to resolve duplicate values of LatestApprovedCaseId
df_final_party_CDDcase = resolve_latest_approved_case_per_party(df_final_party_CDDcase)

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
    save_to_saradar_storage_account(df_final_party_CDDcase, party_CDDcase_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_final_party_CDDcase, party_CDDcase_dataobject, radar_datamodel_version_number, environment)