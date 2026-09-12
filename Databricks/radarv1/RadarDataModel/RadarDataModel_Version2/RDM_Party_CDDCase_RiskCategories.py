# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal:
# MAGIC - To create a dataobject to have all information about a CDD Case risk categories for all W&R Sourcesystems
# MAGIC
# MAGIC #### Author
# MAGIC - Devi.Chennareddy@rabobank.com
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC #### Flow Of Logic
# MAGIC
# MAGIC - Take GCOB, KN1, Legacy2 & NLSVF cases, along with their risk categories from GDP objects.
# MAGIC - Unioning all the case risk categories information from different sourcesystems
# MAGIC - Save the final output in saradar storage account
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |25-July-2025 |13218266 |KN1 Risk calculation based on Barend's solution		
# MAGIC | Abhishek Jaiswal	 |01-Aug-2025 |13277933 |Adding Material risk
# MAGIC | Abhishek Jaiswal	 |14-Aug-2025 |13420151 |Bug fix Material risk
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load
# MAGIC | Prajit Tatari	 |11-Feb-2026 |15058674 |Adding MDM RANZ data from GDP  | 2

# COMMAND ----------

# DBTITLE 1,Importing the required packages
import os
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
import os
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
# RANZ_ReadStorage = os.environ['AU_GDP_Defined_Storage_Account']
environment=os.environ['ENV']
radar_datamodel_version_number=2
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Defining SA RADAR Storage account
SARADAR='saradar'+environment

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account('edlcorestdauprod0001') #RANZ_ReadStorage
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(SARADAR)

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

# DBTITLE 1,Defining the dataobjects' name
party_CDDCases_RiskCategory_dataobject = 'Party_CDDCase_RiskCategories'

# COMMAND ----------

# DBTITLE 1,Define Date variables
load_dts = f"EDL_LOAD_DTS={Load_Date}*"
print (load_dts)

# COMMAND ----------

# DBTITLE 1,Read GCOB Risk data from GDP defined layer
# List of dataobjects from GDP
load_df =[
'Party_RiskModelInstanceQuestionAnswers'
, 'Party_RiskModelCategories'
, 'Party_RiskModelInstance'
, 'party_WRCDDModel'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read GCOB Client data from GDP defined layer
# List of datasets from GDP
load_df = [
'party_case_client_details',
'party_AllPartyDetails' 
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read GIC data from GDP defined layer
import pandas as pd # print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'pessoa'
,'vwgic_rdl_pessoa_juridica_identificacao'
,'tipo_sociedade'
,'vwgic_rdl_pessoa'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read KN1 data from GDP defined layer

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'ADKYC_CONTRAPARTES'
,'ADKYC_CONTRAPARTES_COMPL'
,'ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS'
,'ADKYC_SITUACOES'
,'ADRBB_CONTRAPARTES_GRAM'
,'ADKYC_RISCOS'
,'ADRBB_CONTRAPARTES_PONTUACOES_ABAS'
,'ADRBB_DOMINIOS_QUESTOES'
,'ADRBB_QUESTOES'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='KN1', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read Legacy2 data from GDP defined layer
# print list of strings for loading spark dfs from GDP
load_df = [
'Legacy2_case_client_details'
, 'Legacy2_risk'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier & KN1 Cases dataobject
df_Party_SystemIdentifier=spark.read.parquet(f'abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/1/data/{load_dts}/*.parquet')
df_Party_KN1Cases=spark.read.parquet(f'abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_KN1Cases/1/data/{load_dts}/*.parquet')
df_Party_KN1Cases.createOrReplaceTempView('KN1Cases')

# COMMAND ----------

# DBTITLE 1,RANZ Objects
load_df = [
    'c_b_party_xref'
,'c_lkp_cdd_risk_rating_xref'
,'c_b_due_diligence_xref'
,'c_b_contract_xref'
,'c_b_contr_rol_party_xref'
]

for Dataobject in load_df:
    Read_GDP_Defined_DataObjects_RANZ('RANZ-MDM' , Dataobject, Load_Date)

# COMMAND ----------

# DBTITLE 1,Read NLSVF data from GDP defined layer
# import pandas as pd # print list of strings for loading spark dfs from GDP
# load_df = pd.DataFrame({'GDPname':[
# 'nls_dbo_cif'
# ,'nls_dbo_loanacct'
# ,'nls_dbo_loanacct_detail'
# ]})
# #gic_load_dts
# # Create TempView for each loading table
# for index, row in load_df.iterrows():
#     Read_GDP_Defined_DataObjects(Source='NLSVF', Dataobject=row.GDPname)

# COMMAND ----------

# DBTITLE 1,Derive Static Risk Table for Description
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

# DBTITLE 1,GRAM risk categories
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RiskModelCategories AS
# MAGIC with InstanceCategory as (
# MAGIC   select
# MAGIC     InstanceId,
# MAGIC     Geographical AS GeoRisk,
# MAGIC     `Entity Type` AS EntityTypeRisk,
# MAGIC     Structure AS StructureRisk,
# MAGIC     Sector AS SectorRisk,
# MAGIC     `Products and Services` AS ProductRisk,
# MAGIC     PEP AS PEPRisk,
# MAGIC     `Transaction` AS TransactionRisk,
# MAGIC     `Distribution Channel` AS DistributionRisk,
# MAGIC     `Third Party` AS ThirdPartyRisk,
# MAGIC     `Adverse Info` AS AdverseInfoRisk,
# MAGIC     Other AS OtherRisk
# MAGIC   FROM
# MAGIC     (
# MAGIC       select
# MAGIC         InstanceId,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Adverse Info' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as `Adverse Info`,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Distribution Channel' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as `Distribution Channel`,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Entity Type' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as `Entity Type`,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'General' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as General,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Geographical' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as Geographical,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Other' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as Other,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'PEP' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as PEP,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Products and Services' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as `Products and Services`,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Sector' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as Sector,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Structure' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as Structure,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Third Party' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as `Third Party`,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Transaction' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as `Transaction`
# MAGIC       FROM
# MAGIC         Party_RiskModelCategories
# MAGIC       GROUP BY
# MAGIC         InstanceId
# MAGIC     )
# MAGIC )
# MAGIC select distinct
# MAGIC   geo.Description as GeographicalRiskLevel,
# MAGIC   Ent.Description as EntityTypeRiskLevel,
# MAGIC   stru.Description as StructureRiskLevel,
# MAGIC   sec.Description as SectorRiskLevel,
# MAGIC   prod.Description as ProductAndServiceRiskLevel,
# MAGIC   pep.Description as PEPRiskLevel,
# MAGIC   tran.Description as TransactionRiskLevel,
# MAGIC   dist.Description as DistributionRiskLevel,
# MAGIC   thir.Description as ThirdPartyRiskLevel,
# MAGIC   adv.Description as AdverseInfoRiskLevel,
# MAGIC   oth.Description as OtherRiskLevel,
# MAGIC   InstanceId
# MAGIC FROM
# MAGIC   InstanceCategory AS rf
# MAGIC     LEFT OUTER JOIN static_RiskLevel geo
# MAGIC       ON rf.GeoRisk = geo.Id
# MAGIC     LEFT OUTER JOIN static_RiskLevel Ent
# MAGIC       ON rf.EntityTypeRisk = Ent.Id
# MAGIC     LEFT OUTER JOIN static_RiskLevel stru
# MAGIC       ON rf.StructureRisk = stru.Id
# MAGIC     LEFT OUTER JOIN static_RiskLevel sec
# MAGIC       ON rf.SectorRisk = sec.Id
# MAGIC     LEFT OUTER JOIN static_RiskLevel prod
# MAGIC       ON rf.ProductRisk = prod.Id
# MAGIC     LEFT OUTER JOIN static_RiskLevel pep
# MAGIC       ON rf.PEPRisk = pep.Id
# MAGIC     LEFT OUTER JOIN static_RiskLevel tran
# MAGIC       ON rf.TransactionRisk = tran.Id
# MAGIC     LEFT OUTER JOIN static_RiskLevel dist
# MAGIC       ON rf.DistributionRisk = dist.Id
# MAGIC     LEFT OUTER JOIN static_RiskLevel thir
# MAGIC       ON rf.ThirdPartyRisk = thir.Id
# MAGIC     LEFT OUTER JOIN static_RiskLevel adv
# MAGIC       ON rf.AdverseInfoRisk = adv.Id
# MAGIC     LEFT OUTER JOIN static_RiskLevel oth
# MAGIC       ON rf.otherRisk = oth.Id

# COMMAND ----------

# DBTITLE 1,GCOB Data Preparation
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_CaseClientDetails As
# MAGIC select
# MAGIC   *,
# MAGIC   case
# MAGIC     when ClientType = 'Legal Entity' then 'Legal Entity'
# MAGIC     when
# MAGIC       ClientType in ('Natural Person', 'Natural Person acting in a Professional Capacity (NPPC)')
# MAGIC     then
# MAGIC       'Natural Person'
# MAGIC     else null
# MAGIC   end as Party_type,
# MAGIC   Case
# MAGIC     when ClientType = 'Legal Entity' then concat('LEC_', GcobId)
# MAGIC     when
# MAGIC       ClientType in ('Natural Person', 'Natural Person acting in a Professional Capacity (NPPC)')
# MAGIC     then
# MAGIC       concat('NP_NPPC_', GcobId)
# MAGIC   End as UniquePartyId
# MAGIC from
# MAGIC   party_case_client_details
# MAGIC where
# MAGIC   CaseStatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,GCOB Applicable Risk
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_ApplicableRisk As
# MAGIC select distinct
# MAGIC SourceClient
# MAGIC ,GeographicalApplicableRisk as GeographicalIsMaterial
# MAGIC ,EntityTypeApplicableRisk as EntityTypeIsMaterial
# MAGIC ,StructureApplicableRisk as StructureIsMaterial
# MAGIC ,SectorApplicableRisk as SectorIsMaterial
# MAGIC ,ProductsApplicableRisk as ProductsAndServicesIsMaterial
# MAGIC ,PoliticallyExposedPersonsApplicableRisk as PEPIsMaterial
# MAGIC ,TransactionApplicableRisk as TransactionIsMaterial
# MAGIC ,DistributionChannelApplicableRisk as DistributionChannelIsMaterial
# MAGIC ,ThirdPartyApplicableRisk as ThirdPartyIsMaterial
# MAGIC ,AdverseInfoApplicableRisk as AdverseInfoIsMaterial
# MAGIC ,OtherApplicableRisk as OtherIsMaterial
# MAGIC from Gcob_CaseClientDetails

# COMMAND ----------

# DBTITLE 1,GCOB Material Risk
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_MaterialRisk AS
# MAGIC with QuestionLevelMateriality_material as 
# MAGIC (
# MAGIC select distinct
# MAGIC qa.InstanceId
# MAGIC ,case when qa.MaterialQuestionCode like 'GEO%' then 'Geographical'
# MAGIC       when qa.MaterialQuestionCode like 'STR%' then 'Structure'
# MAGIC       when qa.MaterialQuestionCode like 'SEC%' then 'Sector'
# MAGIC       when qa.MaterialQuestionCode like 'TX%' then 'Transaction'
# MAGIC       when qa.MaterialQuestionCode like 'PS%' then 'Products and Services'
# MAGIC       when qa.MaterialQuestionCode like 'ADVR%' then 'Adverse Info'
# MAGIC       when qa.MaterialQuestionCode like 'ENT%' then 'Entity Type'
# MAGIC       when qa.MaterialQuestionCode like 'PEP%' then 'PEP'
# MAGIC       when qa.MaterialQuestionCode like '3RD%' then 'Third Party'
# MAGIC       when qa.MaterialQuestionCode like 'DIST%' then 'Distribution Channel'
# MAGIC       when qa.MaterialQuestionCode like 'OTH%' then 'Other'
# MAGIC   end as CategoryName
# MAGIC ,qa.IsMaterial
# MAGIC ,qa.ClientId,qa.SourceClient,qa.CaseStatusName,qa.IsLatestApprovedVersionOfClient
# MAGIC from Party_RiskModelInstanceQuestionAnswers qa
# MAGIC where qa.SourceSystemName = 'GCOB-CaseService' and qa.ClientId is not null and qa.ModelType = 'QuestionLevelMateriality'
# MAGIC )
# MAGIC ,QuestionLevelMateriality AS
# MAGIC (
# MAGIC select distinct
# MAGIC InstanceId,SourceClient,
# MAGIC MAX(CASE WHEN CategoryName = 'Adverse Info' then IsMaterial else NULL END ) as AdverseInfoIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Distribution Channel' then IsMaterial else NULL END ) as DistributionChannelIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Entity Type' then IsMaterial else NULL END ) as EntityTypeIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Geographical' then IsMaterial else NULL END ) as GeographicalIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Other' then IsMaterial else NULL END ) as OtherIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'PEP' then IsMaterial else NULL END ) as PEPIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Products and Services' then IsMaterial else NULL END ) as ProductsAndServicesIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Sector' then IsMaterial else NULL END ) as SectorIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Structure' then IsMaterial else NULL END ) as StructureIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Third Party' then IsMaterial else NULL END ) as ThirdPartyIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Transaction' then IsMaterial else NULL END ) as TransactionIsMaterial
# MAGIC FROM QuestionLevelMateriality_material
# MAGIC GROUP BY InstanceId,SourceClient
# MAGIC )
# MAGIC ,defaultModel as
# MAGIC (
# MAGIC select distinct 
# MAGIC d.InstanceId,
# MAGIC a.SourceClient,
# MAGIC cast(a.AdverseInfoIsMaterial as BOOLEAN) as AdverseInfoIsMaterial,
# MAGIC cast(a.DistributionChannelIsMaterial as BOOLEAN) as DistributionChannelIsMaterial,
# MAGIC cast(a.EntityTypeIsMaterial as BOOLEAN) as EntityTypeIsMaterial,
# MAGIC cast(a.GeographicalIsMaterial as BOOLEAN) as GeographicalIsMaterial,
# MAGIC cast(a.OtherIsMaterial as BOOLEAN) as OtherIsMaterial,
# MAGIC cast(a.PEPIsMaterial as BOOLEAN) as PEPIsMaterial,
# MAGIC cast(a.ProductsAndServicesIsMaterial as BOOLEAN) as ProductsAndServicesIsMaterial,
# MAGIC cast(a.SectorIsMaterial as BOOLEAN) as SectorIsMaterial,
# MAGIC cast(a.StructureIsMaterial as BOOLEAN) as StructureIsMaterial,
# MAGIC cast(a.ThirdPartyIsMaterial as BOOLEAN) as ThirdPartyIsMaterial,
# MAGIC cast(a.TransactionIsMaterial as BOOLEAN) as TransactionIsMaterial
# MAGIC from Party_RiskModelInstanceQuestionAnswers d
# MAGIC inner join Gcob_ApplicableRisk a on d.SourceClient = a.SourceClient
# MAGIC where d.SourceSystemName = 'GCOB-CaseService' and d.ClientId is not null and d.ModelType = 'Default'
# MAGIC )
# MAGIC select * from QuestionLevelMateriality
# MAGIC union
# MAGIC select * from defaultModel

# COMMAND ----------

# DBTITLE 1,KN1 Dim Cases
# MAGIC %sql
# MAGIC Create or replace temporary view KN1_Dim_Case As
# MAGIC with KN1_Fianl_cases as 
# MAGIC (
# MAGIC SELECT distinct
# MAGIC 'KN1' AS SourceSystem
# MAGIC , GicID AS ClientId
# MAGIC , PartyLifeCycleStatus2 AS ClientLifeCycleStatus
# MAGIC , CaseID AS CaseId
# MAGIC , NULL AS CaseClientID
# MAGIC , ReviewType
# MAGIC , NULL AS IsFullReview
# MAGIC , CASE WHEN b.CLientID IS NULL  THEN 0 ELSE 1 END AS IsLatestID
# MAGIC , CaseCreationDate
# MAGIC , CaseStatus
# MAGIC , CaseCompletedDate
# MAGIC , CaseEndDate
# MAGIC , NULL AS FinalDecisionDate_From
# MAGIC , NULL AS FinalDecisionDate_To
# MAGIC , NULL AS ClientApprovalDate_From
# MAGIC , NULL AS ClientApprovalDate_To
# MAGIC , NULL AS Expired
# MAGIC , NextReviewDate
# MAGIC FROM KN1Cases a
# MAGIC LEFT JOIN ( 
# MAGIC SELECT 
# MAGIC GicID AS ClientID
# MAGIC , Max(CaseCompletedDate) AS LatestDate
# MAGIC FROM KN1Cases 
# MAGIC WHERE
# MAGIC (CaseStatus != 'Cancelled' and PartyLifeCycleStatus2 = 'Client')
# MAGIC GROUP BY GicID
# MAGIC  ) b 
# MAGIC ON a.CaseCompletedDate = b.LatestDate 
# MAGIC AND b.ClientID=a.GicID
# MAGIC
# MAGIC WHERE 
# MAGIC CaseCompletedDate <= COALESCE(OffboardingDate_PartyRole, '2199-12-31')
# MAGIC )
# MAGIC select distinct 
# MAGIC   cases.SourceSystem
# MAGIC , cases.Clientid
# MAGIC , cases.ClientLifeCycleStatus 
# MAGIC , cases.CaseId
# MAGIC , cases.CaseClientID
# MAGIC , cases.ReviewType as CaseReviewType
# MAGIC , cases.IsFullReview
# MAGIC , cases.IsLatestID--0 or 1
# MAGIC , cases.CaseCreationDate as CaseCreatedDate
# MAGIC , cases.CaseStatus as CurrentCasestatus
# MAGIC , case
# MAGIC wHEN cases.CaseStatus = 'Cancelled' THEN 'Cancelled'
# MAGIC wHEN cases.CaseStatus in ('Offboarding','Approved','Completed','Reapproved','Reactivated') THEN 'Completed'
# MAGIC ELSE 'In Progress' END AS StandardCaseStatus
# MAGIC , cases.CaseCompletedDate as DateCompleted_From
# MAGIC , cases.CaseEndDate as DateCompleted_To
# MAGIC , cases.FinalDecisionDate_From
# MAGIC , cases.FinalDecisionDate_To
# MAGIC , cases.ClientApprovalDate_From
# MAGIC , cases.ClientApprovalDate_To
# MAGIC , cases.Expired
# MAGIC , cases.NextReviewDate
# MAGIC from KN1_Fianl_cases as cases

# COMMAND ----------

# DBTITLE 1,KN1 Gram Case Details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW KN1_GRAM_Casesdetails AS
# MAGIC select distinct
# MAGIC   CONCAT('GIC_',c.ClientId) as LocalSystemIdentifier,
# MAGIC   c.ClientId,
# MAGIC   kn_cont.ID_CONTRAPARTE as CaseID,
# MAGIC   r.Description as CalculatedRiskLevel,
# MAGIC   rl.Description as RecalculatedRiskLevel,
# MAGIC   gram.InstanceId,
# MAGIC   gram.MaterialInstanceId,
# MAGIC   i.Status as GRAM_status
# MAGIC from
# MAGIC   ADKYC_CONTRAPARTES kn_cont
# MAGIC   Inner join ADRBB_CONTRAPARTES_GRAM stat on trim(kn_cont.ID_CONTRAPARTE) = trim(stat.ID_CONTRAPARTE)
# MAGIC   INNER JOIN KN1_Dim_Case c on trim(kn_cont.ID_CONTRAPARTE) = trim(c.CaseId)
# MAGIC   INNER JOIN Party_RiskModelInstanceQuestionAnswers gram on trim(stat.ID_GRAM_IDENTITY) = trim(gram.InstanceId)
# MAGIC     and trim(gram.SourceSystemName) = 'Brazil - KN1'
# MAGIC   Left Join Party_RiskModelInstance i on gram.InstanceId = i.InstanceId
# MAGIC   left join kn1_static_RiskLevel r on trim(kn_cont.CD_RISCO_MANUAL) = trim(r.Id)
# MAGIC   left join kn1_static_RiskLevel rl on trim(kn_cont.CD_RISCO_CALCULADO) = trim(rl.Id)
# MAGIC   

# COMMAND ----------

# DBTITLE 1,KN1 GRAM specific risk data
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW KN1_GRAM_Risk AS
# MAGIC select distinct
# MAGIC   knc.LocalSystemIdentifier,
# MAGIC   knc.ClientID,
# MAGIC   knc.CaseID,
# MAGIC   c.GeographicalRiskLevel,
# MAGIC   c.EntityTypeRiskLevel,
# MAGIC   c.StructureRiskLevel,
# MAGIC   c.SectorRiskLevel,
# MAGIC   c.ProductAndServiceRiskLevel,
# MAGIC   c.PEPRiskLevel,
# MAGIC   c.TransactionRiskLevel,
# MAGIC   c.DistributionRiskLevel,
# MAGIC   c.ThirdPartyRiskLevel,
# MAGIC   c.AdverseInfoRiskLevel,
# MAGIC   knc.CalculatedRiskLevel,
# MAGIC   knc.RecalculatedRiskLevel,
# MAGIC   m.GeographicalIsMaterial as GeographicalMaterialRisk,
# MAGIC   m.EntityTypeIsMaterial as EntityTypeMaterialRisk,
# MAGIC   m.StructureIsMaterial as StructureMaterialRisk,
# MAGIC   m.SectorIsMaterial as SectorMaterialRisk,
# MAGIC   m.ProductsAndServicesIsMaterial as ProductAndServiceMatrialRisk,
# MAGIC   m.PEPIsMaterial as PEPMaterialRisk,
# MAGIC   m.TransactionIsMaterial as TransactionMaterialRisk,
# MAGIC   m.DistributionChannelIsMaterial as DistributionMatrialRisk,
# MAGIC   m.ThirdPartyIsMaterial as ThirdPartyMaterialRisk,
# MAGIC   m.AdverseInfoIsMaterial as AdverseInfoMaterialRisk,
# MAGIC   knc.InstanceId as GRAM_InstanceID,
# MAGIC   knc.GRAM_status
# MAGIC from
# MAGIC KN1_GRAM_Casesdetails knc
# MAGIC left join RiskModelCategories c on c.InstanceId = knc.InstanceId
# MAGIC left join GCOB_MaterialRisk m on m.InstanceId = knc.MaterialInstanceId

# COMMAND ----------

# DBTITLE 1,KN1 Risk and GRAM risk combine
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW KN1_GRAM_DerivedRisk AS
# MAGIC with cte  as (
# MAGIC 		 SELECT 'Baixo' as Portugese, 'Low' as English UNION
# MAGIC 		 SELECT 'Médio', 'Medium' UNION
# MAGIC 		 SELECT 'Alto','High' UNION
# MAGIC 		 SELECT 'Novo', 'Onboarding' UNION
# MAGIC 		 SELECT 'Renovação', 'Renewal' UNION
# MAGIC 		 SELECT 'Aprovado', 'Approved' UNION
# MAGIC 		 SELECT 'Reativado', 'Reactivated' UNION
# MAGIC 		 SELECT 'Ativo', 'Client' UNION 
# MAGIC 		 SELECT 'Bloqueado', 'Blocked' UNION
# MAGIC 		 SELECT 'Reativação', 'Client' UNION--'Reactivated' 
# MAGIC 		 SELECT 'Inativo', 'Former Client' UNION
# MAGIC 		 SELECT 'Pré Cadastro', 'Prospect' UNION
# MAGIC 		 SELECT 'Risco Não Atribuído', 'Unassigned'
# MAGIC 	 )
# MAGIC
# MAGIC SELECT distinct
# MAGIC DC.SourceSystem
# MAGIC , DC.ClientId
# MAGIC , DC.PartyLifeCycleStatus
# MAGIC , DC.CurrentCasestatus
# MAGIC , DC.CaseCreatedDate
# MAGIC , DC.NextReviewDate
# MAGIC , DC.DateCompleted_From
# MAGIC , DC.DateCompleted_To
# MAGIC , RISF.English AS ValidatedRiskDescription
# MAGIC , RISF.riskmodelrisklevelid AS ValidatedRisk
# MAGIC , legalent.DES_TIPO_SOCIEDADE as GIC_entitytype
# MAGIC , CASE WHEN person.SEQ_TIPO_PESSOA = '2' THEN 'Yes' ELSE 'No' END AS isNaturalPerson
# MAGIC , SEQ_TIPO_SOCIEDADE
# MAGIC , B.*
# MAGIC
# MAGIC FROM (
# MAGIC SELECT SourceSystem
# MAGIC , ClientId
# MAGIC , ClientLifeCycleStatus AS PartyLifeCycleStatus
# MAGIC , CaseId
# MAGIC , CurrentCasestatus
# MAGIC , CaseCreatedDate
# MAGIC , NextReviewDate
# MAGIC , DateCompleted_From
# MAGIC , DateCompleted_To
# MAGIC FROM KN1_Dim_Case
# MAGIC WHERE sourcesystem = 'KN1'
# MAGIC AND StandardCaseStatus = 'Completed'
# MAGIC ) DC
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC SELECT ID_CONTRAPARTE
# MAGIC , CD_RISCO_ATRIBUIDO
# MAGIC FROM ADKYC_CONTRAPARTES
# MAGIC ) AS COPA
# MAGIC ON COPA.ID_CONTRAPARTE = DC.CaseId
# MAGIC LEFT JOIN (
# MAGIC SELECT NM_RISCO
# MAGIC , CD_RISCO
# MAGIC , cte.ENGLISH
# MAGIC , rl.riskmodelrisklevelid
# MAGIC , rl.DisplayName
# MAGIC FROM ADKYC_RISCOS
# MAGIC LEFT JOIN CTE cte
# MAGIC ON NM_RISCO = cte.portugese
# MAGIC LEFT JOIN RiskModelRiskLevel rl
# MAGIC ON replace(rl.DisplayName, ' risk', '') = cte.English
# MAGIC ) RISF
# MAGIC ON COPA.CD_RISCO_ATRIBUIDO = RISF.CD_RISCO
# MAGIC LEFT JOIN (
# MAGIC SELECT COD_INSTITUCIONAL
# MAGIC , ident.SEQ_TIPO_SOCIEDADE
# MAGIC , DES_TIPO_SOCIEDADE
# MAGIC FROM vwgic_rdl_pessoa_juridica_identificacao ident
# MAGIC LEFT JOIN (
# MAGIC SELECT SEQ_TIPO_SOCIEDADE
# MAGIC ,DES_TIPO_SOCIEDADE
# MAGIC FROM tipo_sociedade
# MAGIC ) soc
# MAGIC ON soc.SEQ_TIPO_SOCIEDADE = ident.SEQ_TIPO_SOCIEDADE
# MAGIC ) legalent
# MAGIC ON DC.ClientId = legalent.COD_INSTITUCIONAL
# MAGIC LEFT JOIN (
# MAGIC SELECT 
# MAGIC COD_INSTITUCIONAL
# MAGIC ,SEQ_TIPO_PESSOA
# MAGIC FROM vwgic_rdl_pessoa
# MAGIC ) person
# MAGIC ON DC.ClientId= person.COD_INSTITUCIONAL
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC  select 
# MAGIC  ROW_NUMBER() OVER (PARTITION BY CaseId ORDER BY CASE WHEN GRAM_status IN ('Complete', 'CompleteLocked') THEN 0 ELSE 1 END
# MAGIC 				,CASE WHEN risksourcesystem = 'KN1' THEN 1 WHEN risksourcesystem = 'GRAM' THEN 2 END
# MAGIC 			) as RN
# MAGIC  ,C.*
# MAGIC 	FROM (
# MAGIC
# MAGIC SELECT
# MAGIC 'KN1' AS risksourcesystem
# MAGIC , dc.CaseId
# MAGIC , NULL AS GRAM_InstanceID
# MAGIC , NULL AS GRAM_status
# MAGIC , null as CalculatedRisk
# MAGIC , RISC.name AS CalculatedRiskDescription
# MAGIC , null as ReCalculatedRisk
# MAGIC , RISI.name AS RecalculatedRiskDescription
# MAGIC , RADV.ID AS AdverseInfoRisk
# MAGIC , RADV.DE_PONTUACAO_ABA_EN AS AdverseInfoRiskDescription
# MAGIC , RDIS.ID AS DistributionRisk
# MAGIC , RDIS.DE_PONTUACAO_ABA_EN AS DistributionRiskDescription
# MAGIC , RENT.ID AS EntityRisk
# MAGIC , RENT.DE_PONTUACAO_ABA_EN AS EntityRiskDescription
# MAGIC , RGEO.ID AS GeoRisk
# MAGIC , RGEO.DE_PONTUACAO_ABA_EN AS GeoRiskDescription
# MAGIC , NULL AS OtherRisk
# MAGIC , NULL AS OtherRiskDescription
# MAGIC , RPEP.ID AS PEPRisk
# MAGIC , RPEP.DE_PONTUACAO_ABA_EN AS PEPRiskDescription
# MAGIC , RPRO.ID AS ProductRisk
# MAGIC , RPRO.DE_PONTUACAO_ABA_EN AS ProductRiskDescription
# MAGIC , RSEC.ID AS SectorRisk
# MAGIC , RSEC.DE_PONTUACAO_ABA_EN AS SectorRiskDescription
# MAGIC , RSTR.ID AS StructureRisk
# MAGIC , RSTR.DE_PONTUACAO_ABA_EN AS StructureRiskDescription
# MAGIC , RTHP.ID AS ThirdPartyRisk
# MAGIC , RTHP.DE_PONTUACAO_ABA_EN AS ThirdPartyRiskDescription
# MAGIC , RTRA.ID AS TransactionRisk
# MAGIC , RTRA.DE_PONTUACAO_ABA_EN AS TransactionRiskDescription
# MAGIC , NULL AS AdverseInfoIsMaterial
# MAGIC , NULL AS DistributionChannelIsMaterial
# MAGIC , NULL AS EntityTypeIsMaterial
# MAGIC , NULL AS GeographicalIsMaterial
# MAGIC , NULL AS OtherIsMaterial
# MAGIC , NULL AS PEPIsMaterial
# MAGIC , NULL AS ProductsAndServicesIsMaterial
# MAGIC , NULL AS SectorIsMaterial
# MAGIC , NULL AS StructureIsMaterial
# MAGIC , NULL AS ThirdPartyIsMaterial
# MAGIC , NULL AS TransactionIsMaterial
# MAGIC , NULL AS STR4Layers
# MAGIC , NULL AS StructureTax
# MAGIC , IsTrust as IsTrust
# MAGIC , TCSP AS StructureTCSP
# MAGIC , ComplexStructure AS StructureOverlyComplex
# MAGIC , FaceToFaceContact AS `Face2Face Contact`
# MAGIC , adverseinfo AS AdverseInfo
# MAGIC , IssuesBearerShares AS IssuesBearerShares
# MAGIC , NomineeShareholders AS NomineeShareholders
# MAGIC , entityType AS EntityType 
# MAGIC from (
# MAGIC SELECT SourceSystem
# MAGIC , ClientId
# MAGIC , CaseId
# MAGIC , DateCompleted_From
# MAGIC , DateCompleted_To
# MAGIC FROM KN1_Dim_Case
# MAGIC WHERE
# MAGIC sourcesystem = 'KN1'
# MAGIC AND StandardCaseStatus = 'Completed'
# MAGIC ) dc
# MAGIC LEFT JOIN (
# MAGIC SELECT ID_CONTRAPARTE
# MAGIC , CD_RISCO_CALCULADO
# MAGIC , CD_RISCO_MANUAL
# MAGIC FROM ADKYC_CONTRAPARTES
# MAGIC ) AS COPA
# MAGIC ON COPA.ID_CONTRAPARTE = DC.CaseId
# MAGIC LEFT JOIN (
# MAGIC SELECT NM_RISCO
# MAGIC , CD_RISCO
# MAGIC , rl.id
# MAGIC , rl.Description as name
# MAGIC FROM ADKYC_RISCOS
# MAGIC left join kn1_static_RiskLevel rl on CD_RISCO = rl.Id
# MAGIC -- LEFT JOIN CTE cte
# MAGIC -- ON NM_RISCO = cte.portugese
# MAGIC -- LEFT JOIN static_RiskLevel rl
# MAGIC -- ON replace(rl.Description, ' risk', '') = cte.English
# MAGIC ) RISC
# MAGIC ON COPA.CD_RISCO_MANUAL = RISC.CD_RISCO
# MAGIC LEFT JOIN (
# MAGIC SELECT NM_RISCO
# MAGIC , CD_RISCO
# MAGIC , rl.id
# MAGIC , rl.Description as name
# MAGIC FROM ADKYC_RISCOS
# MAGIC left join kn1_static_RiskLevel rl on CD_RISCO = rl.Id
# MAGIC -- LEFT JOIN CTE cte
# MAGIC -- ON NM_RISCO = cte.portugese
# MAGIC -- LEFT JOIN static_RiskLevel rl
# MAGIC -- ON replace(rl.Description, ' risk', '') = cte.English
# MAGIC ) RISI
# MAGIC ON COPA.CD_RISCO_CALCULADO = RISI.CD_RISCO
# MAGIC LEFT JOIN (
# MAGIC SELECT ABAS.ID_CONTRAPARTE
# MAGIC , ABAS.DE_PONTUACAO_ABA
# MAGIC , DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO
# MAGIC , cte.English AS DE_PONTUACAO_ABA_EN
# MAGIC , rl.ID
# MAGIC FROM ADRBB_CONTRAPARTES_PONTUACOES_ABAS ABAS
# MAGIC LEFT JOIN ADRBB_DOMINIOS_QUESTOES DOMIN
# MAGIC ON DOMIN.CD_DOMINIO_QUESTAO = ABAS.CD_DOMINIO_QUESTAO
# MAGIC LEFT JOIN CTE cte
# MAGIC ON ABAS.DE_PONTUACAO_ABA = cte.portugese
# MAGIC LEFT JOIN static_RiskLevel rl
# MAGIC ON replace(rl.Description, ' risk', '') = cte.English
# MAGIC WHERE DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO = 'Geográfico'
# MAGIC ) RGEO
# MAGIC ON RGEO.ID_CONTRAPARTE = dc.CaseId
# MAGIC LEFT JOIN (
# MAGIC SELECT ABAS.ID_CONTRAPARTE
# MAGIC , ABAS.DE_PONTUACAO_ABA
# MAGIC , DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO
# MAGIC , cte.English AS DE_PONTUACAO_ABA_EN
# MAGIC , rl.ID
# MAGIC FROM ADRBB_CONTRAPARTES_PONTUACOES_ABAS ABAS
# MAGIC LEFT JOIN ADRBB_DOMINIOS_QUESTOES DOMIN
# MAGIC ON DOMIN.CD_DOMINIO_QUESTAO = ABAS.CD_DOMINIO_QUESTAO
# MAGIC LEFT JOIN CTE cte
# MAGIC ON ABAS.DE_PONTUACAO_ABA = cte.portugese
# MAGIC LEFT JOIN static_RiskLevel rl
# MAGIC ON replace(rl.Description, ' risk', '') = cte.English
# MAGIC WHERE DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO = 'Tipos de Empresas'
# MAGIC ) RENT
# MAGIC ON RENT.ID_CONTRAPARTE = dc.CaseId
# MAGIC LEFT JOIN (
# MAGIC SELECT ABAS.ID_CONTRAPARTE
# MAGIC , ABAS.DE_PONTUACAO_ABA
# MAGIC , DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO
# MAGIC , cte.English AS DE_PONTUACAO_ABA_EN
# MAGIC , rl.ID
# MAGIC FROM ADRBB_CONTRAPARTES_PONTUACOES_ABAS ABAS
# MAGIC LEFT JOIN ADRBB_DOMINIOS_QUESTOES DOMIN
# MAGIC ON DOMIN.CD_DOMINIO_QUESTAO = ABAS.CD_DOMINIO_QUESTAO
# MAGIC LEFT JOIN CTE cte
# MAGIC ON ABAS.DE_PONTUACAO_ABA = cte.portugese
# MAGIC LEFT JOIN static_RiskLevel rl
# MAGIC ON replace(rl.Description, ' risk', '') = cte.English
# MAGIC WHERE DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO = 'Estrutural'
# MAGIC ) RSTR
# MAGIC ON RSTR.ID_CONTRAPARTE = dc.CaseId
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC SELECT ABAS.ID_CONTRAPARTE
# MAGIC , ABAS.DE_PONTUACAO_ABA
# MAGIC , DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO
# MAGIC , cte.English AS DE_PONTUACAO_ABA_EN
# MAGIC , rl.ID
# MAGIC FROM ADRBB_CONTRAPARTES_PONTUACOES_ABAS ABAS
# MAGIC LEFT JOIN ADRBB_DOMINIOS_QUESTOES DOMIN
# MAGIC ON DOMIN.CD_DOMINIO_QUESTAO = ABAS.CD_DOMINIO_QUESTAO
# MAGIC LEFT JOIN CTE cte
# MAGIC ON ABAS.DE_PONTUACAO_ABA = cte.portugese
# MAGIC LEFT JOIN static_RiskLevel rl
# MAGIC ON replace(rl.Description, ' risk', '') = cte.English
# MAGIC WHERE DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO = 'Setores'
# MAGIC ) RSEC
# MAGIC ON RSEC.ID_CONTRAPARTE = dc.CaseId
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC SELECT ABAS.ID_CONTRAPARTE
# MAGIC , ABAS.DE_PONTUACAO_ABA
# MAGIC , DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO
# MAGIC , cte.English AS DE_PONTUACAO_ABA_EN
# MAGIC , rl.ID
# MAGIC FROM ADRBB_CONTRAPARTES_PONTUACOES_ABAS ABAS
# MAGIC LEFT JOIN ADRBB_DOMINIOS_QUESTOES DOMIN
# MAGIC ON DOMIN.CD_DOMINIO_QUESTAO = ABAS.CD_DOMINIO_QUESTAO
# MAGIC LEFT JOIN CTE cte
# MAGIC ON ABAS.DE_PONTUACAO_ABA = cte.portugese
# MAGIC LEFT JOIN static_RiskLevel rl
# MAGIC ON replace(rl.Description, ' risk', '') = cte.English
# MAGIC WHERE DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO = 'Produtos'
# MAGIC ) RPRO
# MAGIC ON RPRO.ID_CONTRAPARTE = dc.CaseId
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC SELECT ABAS.ID_CONTRAPARTE
# MAGIC , ABAS.DE_PONTUACAO_ABA
# MAGIC , DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO
# MAGIC , cte.English AS DE_PONTUACAO_ABA_EN
# MAGIC , rl.ID
# MAGIC FROM ADRBB_CONTRAPARTES_PONTUACOES_ABAS ABAS
# MAGIC LEFT JOIN ADRBB_DOMINIOS_QUESTOES DOMIN
# MAGIC ON DOMIN.CD_DOMINIO_QUESTAO = ABAS.CD_DOMINIO_QUESTAO
# MAGIC LEFT JOIN CTE cte
# MAGIC ON ABAS.DE_PONTUACAO_ABA = cte.portugese
# MAGIC LEFT JOIN static_RiskLevel rl
# MAGIC ON replace(rl.Description, ' risk', '') = cte.English
# MAGIC WHERE DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO = 'PEP'
# MAGIC ) RPEP
# MAGIC ON RPEP.ID_CONTRAPARTE = dc.CaseId
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC SELECT ABAS.ID_CONTRAPARTE
# MAGIC , ABAS.DE_PONTUACAO_ABA
# MAGIC , DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO
# MAGIC , cte.English AS DE_PONTUACAO_ABA_EN
# MAGIC , rl.ID
# MAGIC FROM ADRBB_CONTRAPARTES_PONTUACOES_ABAS ABAS
# MAGIC LEFT JOIN ADRBB_DOMINIOS_QUESTOES DOMIN
# MAGIC ON DOMIN.CD_DOMINIO_QUESTAO = ABAS.CD_DOMINIO_QUESTAO
# MAGIC LEFT JOIN CTE cte
# MAGIC ON ABAS.DE_PONTUACAO_ABA = cte.portugese
# MAGIC LEFT JOIN static_RiskLevel rl
# MAGIC ON replace(rl.Description, ' risk', '') = cte.English
# MAGIC WHERE DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO = 'Transacional'
# MAGIC ) RTRA
# MAGIC ON RTRA.ID_CONTRAPARTE = dc.CaseId
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC SELECT ABAS.ID_CONTRAPARTE
# MAGIC , ABAS.DE_PONTUACAO_ABA
# MAGIC , DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO
# MAGIC , cte.English AS DE_PONTUACAO_ABA_EN
# MAGIC , rl.ID
# MAGIC FROM ADRBB_CONTRAPARTES_PONTUACOES_ABAS ABAS
# MAGIC LEFT JOIN ADRBB_DOMINIOS_QUESTOES DOMIN
# MAGIC ON DOMIN.CD_DOMINIO_QUESTAO = ABAS.CD_DOMINIO_QUESTAO
# MAGIC LEFT JOIN CTE cte
# MAGIC ON ABAS.DE_PONTUACAO_ABA = cte.portugese
# MAGIC LEFT JOIN static_RiskLevel rl
# MAGIC ON replace(rl.Description, ' risk', '') = cte.English
# MAGIC WHERE DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO = 'Canal de Distribuição'
# MAGIC ) RDIS
# MAGIC ON RDIS.ID_CONTRAPARTE = dc.CaseId
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC SELECT ABAS.ID_CONTRAPARTE
# MAGIC , ABAS.DE_PONTUACAO_ABA
# MAGIC , DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO
# MAGIC , cte.English AS DE_PONTUACAO_ABA_EN
# MAGIC , rl.ID
# MAGIC FROM ADRBB_CONTRAPARTES_PONTUACOES_ABAS ABAS
# MAGIC LEFT JOIN ADRBB_DOMINIOS_QUESTOES DOMIN
# MAGIC ON DOMIN.CD_DOMINIO_QUESTAO = ABAS.CD_DOMINIO_QUESTAO
# MAGIC LEFT JOIN CTE cte
# MAGIC ON ABAS.DE_PONTUACAO_ABA = cte.portugese
# MAGIC LEFT JOIN static_RiskLevel rl
# MAGIC ON replace(rl.Description, ' risk', '') = cte.English
# MAGIC WHERE DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO = 'Terceiros'
# MAGIC ) RTHP
# MAGIC ON RTHP.ID_CONTRAPARTE = dc.CaseId
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC SELECT ABAS.ID_CONTRAPARTE
# MAGIC , ABAS.DE_PONTUACAO_ABA
# MAGIC , DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO
# MAGIC , cte.English AS DE_PONTUACAO_ABA_EN
# MAGIC , rl.ID
# MAGIC FROM ADRBB_CONTRAPARTES_PONTUACOES_ABAS ABAS
# MAGIC LEFT JOIN ADRBB_DOMINIOS_QUESTOES DOMIN
# MAGIC ON DOMIN.CD_DOMINIO_QUESTAO = ABAS.CD_DOMINIO_QUESTAO
# MAGIC LEFT JOIN CTE cte
# MAGIC ON ABAS.DE_PONTUACAO_ABA = cte.portugese
# MAGIC LEFT JOIN static_RiskLevel rl
# MAGIC ON replace(rl.Description, ' risk', '') = cte.English
# MAGIC WHERE DOMIN.DE_SUB_ABA_DOMINIO_QUESTAO = 'Informação Adversa'
# MAGIC ) RADV
# MAGIC ON RADV.ID_CONTRAPARTE = dc.CaseId
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC SELECT distinct
# MAGIC ID_CONTRAPARTE, 
# MAGIC MAX(CASE WHEN cat = 'ComplexStructure' THEN Answer END) AS ComplexStructure,
# MAGIC MAX(CASE WHEN cat = 'NomineeShareholders' THEN Answer END) AS NomineeShareholders,
# MAGIC MAX(CASE WHEN cat = 'IssuesBearerShares' THEN Answer END) AS IssuesBearerShares,
# MAGIC MAX(CASE WHEN cat = 'IsTrust' THEN Answer END) AS IsTrust,
# MAGIC MAX(CASE WHEN cat = 'TCSP' THEN Answer END) AS TCSP,
# MAGIC MAX(CASE WHEN cat = 'FaceToFaceContact' THEN Answer END) AS FaceToFaceContact,
# MAGIC MAX(CASE WHEN cat = 'adverseinfo' THEN Answer END) AS adverseinfo,
# MAGIC MAX(CASE WHEN cat = 'entityType' THEN Answer END) AS entityType
# MAGIC FROM (
# MAGIC SELECT ID_CONTRAPARTE
# MAGIC , cat
# MAGIC , Answer
# MAGIC FROM (
# MAGIC SELECT ROW_NUMBER() OVER (PARTITION BY cppc.ID_CONTRAPARTE, Q_cat.cat ORDER BY CPPC.CD_RESPOSTA DESC) as RN
# MAGIC , cppc.CD_RESPOSTA
# MAGIC , cppc.ID_QUESTAO
# MAGIC , cppc.ID_CONTRAPARTE
# MAGIC , doqu.CD_QUESTAO
# MAGIC , Q_cat.cat
# MAGIC , CASE 
# MAGIC WHEN doqu.CD_QUESTAO = 'Q47' AND CPPC.CD_RESPOSTA = 2 THEN 'Yes'
# MAGIC WHEN doqu.CD_QUESTAO = 'Q47' AND CPPC.CD_RESPOSTA != 2 THEN NULL
# MAGIC when tl.EN is not null then tl.EN else CD_RESPOSTA END AS Answer
# MAGIC FROM ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS CPPC
# MAGIC INNER JOIN (
# MAGIC SELECT ID_QUESTAO
# MAGIC , CD_QUESTAO 
# MAGIC FROM ADRBB_QUESTOES
# MAGIC ) DOQU
# MAGIC ON DOQU.ID_QUESTAO = CPPC.ID_QUESTAO
# MAGIC INNER JOIN (
# MAGIC SELECT 'Q286' AS CD_QUESTAO
# MAGIC , 'ComplexStructure' AS cat
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 'Q292', 'NomineeShareholders'
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT 'Q314', 'IssuesBearerShares'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 'Q311', 'IsTrust'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 'Q287', 'TCSP'
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT 'Q42', 'FaceToFaceContact'
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT 'Q77', 'FaceToFaceContact'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 'Q47', 'adverseinfo' 
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT 'Q5', 'entityType'
# MAGIC
# MAGIC ) Q_cat
# MAGIC ON Q_cat.CD_QUESTAO = DOQU.CD_QUESTAO
# MAGIC LEFT JOIN (
# MAGIC SELECT 'Não' AS BR, 'No' AS EN
# MAGIC UNION
# MAGIC SELECT 'Sim' AS BR, 'Yes' AS EN
# MAGIC ) tl
# MAGIC ON tl.BR = case when DOQU.CD_QUESTAO in ('Q311', 'Q312', 'Q314') then de_resposta else cppc.CD_RESPOSTA end
# MAGIC ) a
# MAGIC ) b
# MAGIC GROUP BY ID_CONTRAPARTE
# MAGIC ) charact
# MAGIC ON charact.ID_CONTRAPARTE = dc.CaseId
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC select distinct 
# MAGIC 'GRAM' AS risksourcesystem
# MAGIC ,CaseID
# MAGIC ,GRAM_InstanceID
# MAGIC ,GRAM_status as GRAM_status
# MAGIC ,null as id_CalculatedRisk
# MAGIC ,CalculatedRiskLevel
# MAGIC ,null as id_ReCalculatedRisk
# MAGIC ,RecalculatedRiskLevel
# MAGIC ,null as id_AdverseInfoRiskLevel
# MAGIC ,AdverseInfoRiskLevel
# MAGIC ,null as id_DistributionRiskLevel
# MAGIC ,DistributionRiskLevel
# MAGIC ,null as id_EntityTypeRiskLevel
# MAGIC ,EntityTypeRiskLevel
# MAGIC ,null as id_GeographicalRiskLevel
# MAGIC ,GeographicalRiskLevel
# MAGIC ,null as id_OtherRiskLevel
# MAGIC ,null as OtherRiskLevel
# MAGIC ,null as id_PEPRiskLevel
# MAGIC ,PEPRiskLevel
# MAGIC ,null as id_ProductAndServiceRiskLevel
# MAGIC ,ProductAndServiceRiskLevel
# MAGIC ,null as id_SectorRiskLevel
# MAGIC ,SectorRiskLevel
# MAGIC ,null as id_StructureRiskLevel
# MAGIC ,StructureRiskLevel
# MAGIC ,null as id_ThirdPartyRiskLevel
# MAGIC ,ThirdPartyRiskLevel
# MAGIC ,null as id_TransactionRiskLevel
# MAGIC ,TransactionRiskLevel
# MAGIC ,AdverseInfoMaterialRisk
# MAGIC ,DistributionMatrialRisk
# MAGIC ,EntityTypeMaterialRisk
# MAGIC ,GeographicalMaterialRisk
# MAGIC ,null as OtherIsMaterial
# MAGIC ,PEPMaterialRisk
# MAGIC ,ProductAndServiceMatrialRisk
# MAGIC ,SectorMaterialRisk
# MAGIC ,StructureMaterialRisk
# MAGIC ,ThirdPartyMaterialRisk
# MAGIC ,TransactionMaterialRisk
# MAGIC , null as STR4Layers
# MAGIC , null as StructureTax
# MAGIC , null as IsTrust
# MAGIC , null as StructureTCSP
# MAGIC , null as StructureOverlyComplex
# MAGIC , null as Face2FaceContact
# MAGIC , null as AdverseInfo
# MAGIC , null as IssuesBearerShares
# MAGIC , null as NomineeShareholders
# MAGIC , null as EntityType
# MAGIC from KN1_GRAM_Risk
# MAGIC ) C
# MAGIC ) B
# MAGIC ON B.CaseId = DC.CaseId
# MAGIC WHERE RN = 1
# MAGIC

# COMMAND ----------

# DBTITLE 1,KN1 Risk categories
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW KN1_Risk AS
# MAGIC select distinct
# MAGIC CASE 
# MAGIC   WHEN TRIM(ClientId) = '' THEN CONCAT('KN1_',CaseId) 
# MAGIC   ELSE CONCAT('GIC_',ClientId) 
# MAGIC END as LocalSystemIdentifier, -- PBI 14525246
# MAGIC CASE 
# MAGIC   WHEN TRIM(ClientId) = '' THEN NULL
# MAGIC   ELSE ClientId 
# MAGIC END as ClientId -- PBI 14525246
# MAGIC ,CaseId
# MAGIC ,case when isNaturalPerson = 'No' then concat('KN1_NP_NPPC_', CaseId) ELSE concat('KN1_LEC_', CaseId) END AS UniqueCaseId
# MAGIC ,GeoRiskDescription as GeographicalRiskLevel
# MAGIC ,EntityRiskDescription as EntityTypeRiskLevel
# MAGIC ,StructureRiskDescription as StructureRiskLevel
# MAGIC ,SectorRiskDescription as SectorRiskLevel
# MAGIC ,ProductRiskDescription as ProductAndServiceRiskLevel
# MAGIC ,PEPRiskDescription as PEPRiskLevel
# MAGIC ,TransactionRiskDescription as TransactionRiskLevel
# MAGIC ,DistributionRiskDescription as DistributionRiskLevel
# MAGIC ,ThirdPartyRiskDescription as ThirdPartyRiskLevel
# MAGIC ,AdverseInfoRiskDescription as AdverseInfoRiskLevel
# MAGIC ,CalculatedRiskDescription as CalculatedRiskLevel
# MAGIC ,RecalculatedRiskDescription as RecalculatedRiskLevel
# MAGIC ,OtherRiskDescription as OtherRiskLevel
# MAGIC ,AdverseInfoIsMaterial
# MAGIC ,DistributionChannelIsMaterial
# MAGIC ,EntityTypeIsMaterial
# MAGIC ,GeographicalIsMaterial
# MAGIC ,PEPIsMaterial
# MAGIC ,ProductsAndServicesIsMaterial
# MAGIC ,SectorIsMaterial
# MAGIC ,StructureIsMaterial
# MAGIC ,ThirdPartyIsMaterial
# MAGIC ,TransactionIsMaterial
# MAGIC ,OtherIsMaterial
# MAGIC from KN1_GRAM_DerivedRisk

# COMMAND ----------

# DBTITLE 1,GCOB cases risk categories
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcob_casesrisk AS
# MAGIC select distinct
# MAGIC   CONCAT('GCOB_',c.UniquePartyId) AS LocalSystemIdentifier,
# MAGIC   c.ClientId,
# MAGIC   c.CaseId,
# MAGIC   case
# MAGIC     when c.ClientType = 'Legal Entity' then concat('GCOB_LE_', c.CaseId)
# MAGIC     when
# MAGIC       c.ClientType in ('Natural Person', 'Natural Person acting in a Professional Capacity (NPPC)')
# MAGIC     then
# MAGIC       concat('GCOB_NP_NPPC_', c.CaseId)
# MAGIC     else null
# MAGIC   end as UnqiueCaseId,
# MAGIC   c.GeographicalRiskLevel,
# MAGIC   c.EntityTypeRiskLevel,
# MAGIC   c.StructureRiskLevel,
# MAGIC   c.SectorRiskLevel,
# MAGIC   c.ProductAndServiceRiskLevel,
# MAGIC   c.PEPRiskLevel,
# MAGIC   c.TransactionRiskLevel,
# MAGIC   c.DistributionRiskLevel,
# MAGIC   c.ThirdPartyRiskLevel,
# MAGIC   c.AdverseInfoRiskLevel,
# MAGIC   c.ModelCalculatedRiskLevel as CalculatedRiskLevel,
# MAGIC   c.ModelRecalculatedRiskLevel as RecalculatedRiskLevel
# MAGIC   ,c.OtherRiskLevel
# MAGIC   ,m.AdverseInfoIsMaterial
# MAGIC   ,m.DistributionChannelIsMaterial
# MAGIC   ,m.EntityTypeIsMaterial
# MAGIC   ,m.GeographicalIsMaterial
# MAGIC   ,m.PEPIsMaterial
# MAGIC   ,m.ProductsAndServicesIsMaterial
# MAGIC   ,m.SectorIsMaterial
# MAGIC   ,m.StructureIsMaterial
# MAGIC   ,m.ThirdPartyIsMaterial
# MAGIC   ,m.TransactionIsMaterial
# MAGIC   ,null as OtherIsMaterial
# MAGIC from Gcob_CaseClientDetails c
# MAGIC left join GCOB_MaterialRisk m on c.SourceClient = m.SourceClient
# MAGIC where
# MAGIC   c.CaseStatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,Legacy2 risk category details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Legacy2_case_client_details
# MAGIC where
# MAGIC   isclient = 1
# MAGIC   and GcobCaseId is null
# MAGIC   and Value is null
# MAGIC   and GcobId like 'RA:%';
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Legacy2_case_client_details
# MAGIC where
# MAGIC   ClientId not in (
# MAGIC     select
# MAGIC       ClientId
# MAGIC     from
# MAGIC       NonSFDCID_Legacy2_Client
# MAGIC   );
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_riskdetails AS
# MAGIC select distinct
# MAGIC   CONCAT('LEGACY2_', CASE
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
# MAGIC   case
# MAGIC     when c.clienttype = 'Legal Entity' then concat('LEC_', c.ClientId)
# MAGIC     when
# MAGIC       c.clienttype in ('Natural Person acting in a Professional Capacity (NPPC)', 'Natural Person')
# MAGIC     then
# MAGIC       concat('NP_NPPC_', c.ClientId)
# MAGIC     when c.ClientType = 'Related Legal Entity' then concat('RLE_', c.ClientId)
# MAGIC     when c.ClientType = 'Related Natural Person' then concat('RNP_', c.ClientId)
# MAGIC     else null
# MAGIC   end as UniqueCaseId,
# MAGIC   r.GeographicalRetainedRiskLevel as GeographicalRiskLevel,
# MAGIC   r.EntityTypeRetainedRiskLevel as EntityTypeRiskLevel,
# MAGIC   r.StructureRetainedRiskLevel as StructureRiskLevel,
# MAGIC   r.SectorRetainedRiskLevel as SectorRiskLevel,
# MAGIC   r.ProductsRetainedRiskLevel as ProductAndServiceRiskLevel,
# MAGIC   r.PoliticallyExposedPersonsRetainedRiskLevel as PEPRiskLevel,
# MAGIC   r.TransactionRetainedRiskLevel as TransactionRiskLevel,
# MAGIC   r.DistributionChannelRetainedRiskLevel as DistributionRiskLevel,
# MAGIC   r.ThirdPartyRetainedRiskLevel as ThirdPartyRiskLevel,
# MAGIC   r.AdverseInfoRetainedRiskLevel as AdverseInfoRiskLevel,
# MAGIC   r.CalculatedRiskLevel,
# MAGIC   r.RecalculatedRiskLevel
# MAGIC   ,null as OtherRiskLevel
# MAGIC   ,cast(r.AdverseInfoApplicableRisk as BOOLEAN) as AdverseInfoIsMaterial
# MAGIC   ,cast(r.DistributionChannelApplicableRisk as BOOLEAN) as DistributionChannelIsMaterial
# MAGIC   ,cast(r.EntityTypeApplicableRisk as BOOLEAN) as EntityTypeIsMaterial
# MAGIC   ,cast(r.GeographicalApplicableRisk as BOOLEAN) as GeographicalIsMaterial
# MAGIC   ,cast(r.PoliticallyExposedPersonsApplicableRisk as BOOLEAN) as PEPIsMaterial
# MAGIC   ,cast(r.ProductsApplicableRisk as BOOLEAN) as ProductsAndServicesIsMaterial
# MAGIC   ,cast(r.SectorApplicableRisk as BOOLEAN) as SectorIsMaterial
# MAGIC   ,cast(r.StructureApplicableRisk as BOOLEAN) as StructureIsMaterial
# MAGIC   ,cast(r.ThirdPartyApplicableRisk as BOOLEAN) as ThirdPartyIsMaterial
# MAGIC   ,cast(r.TransactionApplicableRisk as BOOLEAN) as TransactionIsMaterial
# MAGIC   ,cast(null as BOOLEAN) as OtherIsMaterial
# MAGIC from
# MAGIC   Legacy2_client c
# MAGIC     Inner join Legacy2_risk r
# MAGIC       on r.ClientId = c.ClientId
# MAGIC where
# MAGIC   c.IsClient = 'true'
# MAGIC   and c.ClientTypeId in (1, 2, 3)

# COMMAND ----------

# DBTITLE 1,NLSVF risk category details
# %sql
# CREATE OR REPLACE TEMPORARY VIEW NLSVF_Client AS
# select distinct
# concat('NLSVF_',c.CIFNUMBER) as LocalSystemIdentifier
# ,c.CIFNUMBER as ClientId
# ,c.CIFNUMBER as CaseId
# ,case when c.ENTITY = 'Individual' then concat('NP_NPPC_', c.CIFNUMBER) else concat('LEC_', c.CIFNUMBER) end as UniqueCaseId
# ,acc.userdef09 as GeographicalRiskLevel
# ,acc.userdef10 as EntityTypeRiskLevel
# ,acc.userdef11 as StructureRiskLevel
# ,acc.userdef12 as SectorRiskLevel
# ,acc.userdef13 as ProductAndServiceRiskLevel
# ,acc.userdef14 as PEPRiskLevel
# ,acc.userdef15 as TransactionRiskLevel
# ,acc.userdef16 as DistributionRiskLevel
# ,acc.userdef16 as ThirdPartyRiskLevel
# ,acc.userdef18 as AdverseInfoRiskLevel
# ,acc.userdef02 as CalculatedRiskLevel
# ,acc.userdef03 as RecalculatedRiskLevel
# from nls_dbo_cif c
# inner join nls_dbo_loanacct ac on c.CIFNO = ac.CIFNO
# inner join nls_dbo_loanacct_detail acc on ac.acctrefno = acc.acctrefno

# COMMAND ----------

# MAGIC %md
# MAGIC MDM RANZ Risk Categories

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RANZ_MaterialRisk AS
# MAGIC with QuestionLevelMateriality_material as 
# MAGIC (
# MAGIC select distinct
# MAGIC qa.InstanceId--,qa.SourceSystemReferenceId,qa.SourceSystemName,qa.MaterialQuestionId,qa.MaterialQuestionCode
# MAGIC ,case when qa.MaterialQuestionCode like 'GEO%' then 'Geographical'
# MAGIC       when qa.MaterialQuestionCode like 'STR%' then 'Structure'
# MAGIC       when qa.MaterialQuestionCode like 'SEC%' then 'Sector'
# MAGIC       when qa.MaterialQuestionCode like 'TX%' then 'Transaction'
# MAGIC       when qa.MaterialQuestionCode like 'PS%' then 'Products and Services'
# MAGIC       when qa.MaterialQuestionCode like 'ADVR%' then 'Adverse Info'
# MAGIC       when qa.MaterialQuestionCode like 'ENT%' then 'Entity Type'
# MAGIC       when qa.MaterialQuestionCode like 'PEP%' then 'PEP'
# MAGIC       when qa.MaterialQuestionCode like '3RD%' then 'Third Party'
# MAGIC       when qa.MaterialQuestionCode like 'DIST%' then 'Distribution Channel'
# MAGIC       when qa.MaterialQuestionCode like 'OTH%' then 'Other'
# MAGIC   end as CategoryName
# MAGIC ,qa.IsMaterial,qa.CaseStatusName, qa.SourceClient
# MAGIC from Party_RiskModelInstanceQuestionAnswers qa
# MAGIC where qa.SourceSystemName = 'RANZ - PegaCdd' and qa.ModelType = 'QuestionLevelMateriality'
# MAGIC )
# MAGIC ,QuestionLevelMateriality AS
# MAGIC (
# MAGIC select distinct
# MAGIC InstanceId,SourceClient,
# MAGIC MAX(CASE WHEN CategoryName = 'Adverse Info' then IsMaterial else NULL END ) as AdverseInfoIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Distribution Channel' then IsMaterial else NULL END ) as DistributionChannelIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Entity Type' then IsMaterial else NULL END ) as EntityTypeIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Geographical' then IsMaterial else NULL END ) as GeographicalIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Other' then IsMaterial else NULL END ) as OtherIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'PEP' then IsMaterial else NULL END ) as PEPIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Products and Services' then IsMaterial else NULL END ) as ProductsAndServicesIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Sector' then IsMaterial else NULL END ) as SectorIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Structure' then IsMaterial else NULL END ) as StructureIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Third Party' then IsMaterial else NULL END ) as ThirdPartyIsMaterial,
# MAGIC MAX(CASE WHEN CategoryName = 'Transaction' then IsMaterial else NULL END ) as TransactionIsMaterial
# MAGIC FROM QuestionLevelMateriality_material
# MAGIC GROUP BY InstanceId,SourceClient
# MAGIC )
# MAGIC select * from QuestionLevelMateriality

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RANZ_riskdetails AS
# MAGIC select distinct
# MAGIC CONCAT('RANZ_',p.PKEY_SRC_OBJECT) LocalSystemIdentifier,
# MAGIC p.PKEY_SRC_OBJECT as ClientID,
# MAGIC dd.CASE_ID as CaseID,
# MAGIC null as UniqueCaseId, 
# MAGIC  rc.GeographicalRiskLevel,
# MAGIC  rc.EntityTypeRiskLevel,
# MAGIC  rc.StructureRiskLevel,
# MAGIC  rc.SectorRiskLevel,
# MAGIC  rc.ProductAndServiceRiskLevel,
# MAGIC  rc.PEPRiskLevel,
# MAGIC  rc.TransactionRiskLevel,
# MAGIC  rc.DistributionRiskLevel,
# MAGIC  rc.ThirdPartyRiskLevel,
# MAGIC  rc.AdverseInfoRiskLevel,
# MAGIC   f.Description as CalculatedRiskLevel,
# MAGIC  g.Description as RecalculatedRiskLevel,
# MAGIC  rc.OtherRiskLevel,
# MAGIC  d.AdverseInfoIsMaterial,
# MAGIC  d.DistributionChannelIsMaterial,
# MAGIC  d.EntityTypeIsMaterial,
# MAGIC  d.GeographicalIsMaterial,
# MAGIC  d.PEPIsMaterial,
# MAGIC  d.ProductsAndServicesIsMaterial,
# MAGIC  d.SectorIsMaterial,
# MAGIC  d.StructureIsMaterial,
# MAGIC  d.ThirdPartyIsMaterial,
# MAGIC  d.TransactionIsMaterial,
# MAGIC  d.OtherIsMaterial
# MAGIC FROM c_b_party_xref as p
# MAGIC JOIN c_b_contr_rol_party_xref as crp
# MAGIC     ON crp.FK_PARTY_ID = p.ROWID_XREF
# MAGIC JOIN c_b_contract_xref as c
# MAGIC     ON c.CONTR_ID = crp.FK_CONTR_ID
# MAGIC LEFT JOIN c_b_due_diligence_xref as dd
# MAGIC     ON dd.FK_CONTR_ID = c.CONTR_ID
# MAGIC left join Party_RiskModelInstanceQuestionAnswers b
# MAGIC on dd.GRAM_INSTANCE_ID = b.InstanceId
# MAGIC  and b.SourceSystemName = 'RANZ - PegaCdd'
# MAGIC left join RiskModelCategories rc
# MAGIC on dd.GRAM_INSTANCE_ID = rc.InstanceId
# MAGIC left join RANZ_MaterialRisk d
# MAGIC on b.MaterialInstanceId = d.InstanceId
# MAGIC left join Party_RiskModelInstance e
# MAGIC on dd.GRAM_INSTANCE_ID = e.InstanceId
# MAGIC  and e.SourceSystemName = 'RANZ - PegaCdd'
# MAGIC  left join static_RiskLevel f
# MAGIC  on e.OverallCalculatedRiskLevelId = f.Id
# MAGIC   left join static_RiskLevel g
# MAGIC  on e.OverallRecalculatedRiskLevelId = g.Id

# COMMAND ----------

# DBTITLE 1,union GIC and GCOB
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Case_RiskCategory AS
# MAGIC select * FROM KN1_Risk
# MAGIC UNION
# MAGIC select * from gcob_CasesRisk
# MAGIC UNION
# MAGIC select * from Legacy2_riskdetails
# MAGIC UNION
# MAGIC select * from RANZ_riskdetails

# COMMAND ----------

df_party_CDDCases_RiskCategory=spark.table('Case_RiskCategory')

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_final_party_CDDcase_RiskCategory=add_party_identifier(df_party_CDDCases_RiskCategory,df_Party_SystemIdentifier)

# COMMAND ----------

if RunType == "historical":
    save_to_saradar_storage_account(df_final_party_CDDcase_RiskCategory, party_CDDCases_RiskCategory_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_final_party_CDDcase_RiskCategory, party_CDDCases_RiskCategory_dataobject, radar_datamodel_version_number, environment)
