# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from RadarUtils import *

# COMMAND ----------

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

authenticate_storage_account('edlcorestdauprod0001')
authenticate_storage_account('edlcorestdeuprod0001')
authenticate_storage_account('saradarpreprod')

# COMMAND ----------

def read_files(container, storage, object):

    
    # authenticate_storage_account(storage)
    folder = f"abfss://{container}@{storage}.dfs.core.windows.net/{object}/1/data"

    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(folder)
    ).withColumn('Row', row_number().over(Window.partitionBy(col("PKEY_SRC_OBJECT")).orderBy(desc(col("EDL_LOAD_DTS"))))).filter("Row == 1")
    
    exclude = ['EDL_LOAD_DTS','EDL_ACT_DTS','EDL_ACT_DTS_UTC', 'Row']
    cols = [c for c in df.columns if c not in exclude]
    
    df.select(cols).distinct().createOrReplaceTempView(object)

# COMMAND ----------

def Read_GDP_Defined_DataObjects_RANZ(Source , Dataobject,Load_Date=''):
    print(f"{Source}: Processing \033[1m{Dataobject}\033[0m")
 
    AU_GDP_Defined_Storage_Account='edlcorestdauprod0001' # To load from environment variable
 
    # Build the base path
    base_path = f"abfss://mdm-ranz@{AU_GDP_Defined_Storage_Account}.dfs.core.windows.net/{Dataobject}"
    # Build version mapping
    version_mapping_by_source = {
        'RANZ-MDM': {
            2: ['c_lkp_naics_xref','c_lkp_naics_sector_xref','c_lkp_cdd_risk_rating_xref','c_b_party_xref','c_b_party_rel_party_xref','c_b_party_rel_addr_xref','c_b_party_pep_am_xref','c_b_party_naics_xref','c_b_party_dom_cntry_xref','c_b_due_diligence_xref','c_b_contr_rol_party_xref','c_b_contract_xref']
        } }
    Primary_Key_dict = {
    "ROWID_OBJECT": [
        "c_b_party_rel_addr_xref",
        "c_b_contract_xref",
        "c_b_due_diligence_xref",
        "c_b_party_dom_cntry_xref",
        "c_b_party_naics_xref",
        "c_b_party_pep_am_xref",
        "c_b_party_rel_party_xref",
        "c_lkp_cdd_risk_rating_xref",
        "c_lkp_naics_sector_xref",
        "c_lkp_naics_xref",
        "c_b_contr_rol_party_xref"
    ],
    "SRC_PARTY_ID": [
        "c_b_party_xref"
    ]
    }
    try:
        # List files in the base path
        files = dbutils.fs.ls(base_path)
 
        # Determine version
        if Source in version_mapping_by_source:
            source_versions = version_mapping_by_source[Source]
            version = next((v for v, objs in source_versions.items() if Dataobject in objs),None)
            if version is None:
                raise ValueError(f"{Source} Dataobject '{Dataobject}' not found in version mapping.")
 
        print(f"Selected version : {version}")
 
        # Detect partition folder containing Load_Date
        partition_base_path = f"{base_path}/{version}/data/"
 
        print(f"Reading from path: {partition_base_path}")
 
        df_ranz=spark.read.format('delta').load(partition_base_path)
       
        # Detect primary key
        pk_column = None
        for pk, table_list in Primary_Key_dict.items():
            if Dataobject in table_list:
                pk_column = pk
                break
 
        if pk_column is None:
            raise ValueError(f"No primary key found for Dataobject {Dataobject} in Primary_Key_dict")
 
        print(f"Using Primary Key for window: {pk_column}")
 
        df_ranz.withColumn("rn", row_number().over(Window.partitionBy(pk_column).orderBy(col("EDL_ACT_DTS").desc()))).filter(col("rn") == 1).drop("rn").createOrReplaceTempView(Dataobject)
 
        # Print row count
        row_count = spark.sql(f"SELECT COUNT(*) FROM {Dataobject}").collect()[0][0]
        print(f"No. of rows read : {row_count}")
        print(f"{Dataobject} read successfully\n")
 
    except Exception as e:
        raise RuntimeError(f"Error reading {Dataobject} from {Source}: {e}")

# COMMAND ----------

load_df = [
    'c_lkp_naics_xref'
,'c_b_party_rel_party_xref'
,'c_b_party_rel_addr_xref'
,'c_lkp_cdd_risk_rating_xref'
,'c_lkp_naics_sector_xref'
,'c_b_party_xref'
,'c_b_party_naics_xref'
,'c_b_party_pep_am_xref'
,'c_b_party_dom_cntry_xref'
,'c_b_due_diligence_xref'
,'c_b_contract_xref']

for Dataobject in load_df:
    Read_GDP_Defined_DataObjects_RANZ('RANZ-MDM' , Dataobject, Load_Date)

# COMMAND ----------

# dbutils.fs.ls(f"abfss://mdm-ranz@edlcorestdauprod0001.dfs.core.windows.net/c_b_party_xref/2/data/")#65629
 

# COMMAND ----------

# List of dataobjects from GDP
load_df =[
'Party_RiskModelInstanceQuestionAnswers'
, 'Party_RiskModelCategories'
, 'Party_RiskModelInstance'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Party CddCase

# COMMAND ----------

Risk_list = [0,1,2,3,4,5,6,7]
Risk_Description = ['Undefined', 'No', 'Low', 'Medium', 'High','Super','Extra High','Unacceptable']
#Creating spark dataframe from lists
df_gcob_static_RiskLevel = spark.createDataFrame(zip(Risk_list, Risk_Description), ['Id', 'Description'])
df_gcob_static_RiskLevel.createOrReplaceTempView('static_RiskLevel')

# COMMAND ----------

spark.read.format('delta').load('abfss://fec-radar@edlcorestdeuprod0001.dfs.core.windows.net/RadarDataModel/Party_CDDCase/2/data/LOAD_DT=20251230/').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from c_b_due_diligence_xref a
# MAGIC join c_b_contract_xref b 
# MAGIC on a.FK_CONTR_ID = b.CONTR_ID
# MAGIC join c_lkp_cdd_risk_rating_xref c
# MAGIC on a.CDD_RISK_RTG_CD = c.CDD_RISK_RATING_CODE
# MAGIC left join Party_RiskModelInstance d
# MAGIC on a.GRAM_INSTANCE_ID = d.InstanceId
# MAGIC -- join c_b_party_xref c
# MAGIC -- on a.pkey_src_object = c.SRC_PARTY_ID
# MAGIC -- where a.pkey_src_object = 4004660

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC null as PartyIdentifier,
# MAGIC a.PKEY_SRC_OBJECT as ClientID,
# MAGIC a.CASE_ID as CaseID,
# MAGIC null as UniqueCaseId, 
# MAGIC null as LatestApprovedCaseId,
# MAGIC null as ReviewTypeName,
# MAGIC null as CaseStatusName,
# MAGIC null as ReviewReason,
# MAGIC null as ReviewTypeReason,
# MAGIC null as ReviewTypeOtherReason,
# MAGIC a.CDD_NEXT_REVIEW_DT as NextReviewDate,
# MAGIC null as SubmitToClientCommittee,
# MAGIC null as CaseDecisionMotivation,
# MAGIC b.CONTR_NAME as CurrentAssignee,
# MAGIC null as InitiationInProgressAssignee,
# MAGIC b.START_DT as ReadyForKYCAssessmentDate,
# MAGIC null as KYCAssessmentInProgressAssignee,
# MAGIC null as LastKYCAnalyst,
# MAGIC null as LastSentForKYCAssesment,
# MAGIC null as DateSubmittedFor4EyeCheck,
# MAGIC null as 4EyeCheckReviewer,
# MAGIC null as Last4EyeCheckReviewer,
# MAGIC null as LastSentFor4EyeCheck,
# MAGIC null as DateSubmittedForSignOff,
# MAGIC a.CDD_SIGN_OFF_DT as ClientOwnerSignOffDate,
# MAGIC null as LastProductOffboardingAnalyst,
# MAGIC c.CDD_RISK_RATING_DESC as ValidatedRiskLevel,
# MAGIC a.START_DT as CaseCreationDate,
# MAGIC a.CDD_COMPLETION_DT as ScheduledCompletionDate,
# MAGIC a.END_DT as CaseCompletedDate,
# MAGIC a.CDD_STRATEGIC_REVIEW_DT as FinalDecisionDate,
# MAGIC  d.RiskModelName,
# MAGIC  e.Description as CalculatedRiskLevel,
# MAGIC  f.Description as ReCalculatedRiskLevel,
# MAGIC  d.RiskDeviationReason,
# MAGIC d.Status as StandardCaseStatus,
# MAGIC null as DeactivationDate
# MAGIC   from c_b_due_diligence_xref a
# MAGIC join c_b_contract_xref b 
# MAGIC on a.FK_CONTR_ID = b.CONTR_ID
# MAGIC join c_lkp_cdd_risk_rating_xref c
# MAGIC on a.CDD_RISK_RTG_CD = c.CDD_RISK_RATING_CODE
# MAGIC left join Party_RiskModelInstance d
# MAGIC on a.GRAM_INSTANCE_ID = d.InstanceId
# MAGIC  and d.SourceSystemName = 'RANZ - PegaCdd'
# MAGIC  left join static_RiskLevel e
# MAGIC  on d.OverallCalculatedRiskLevelId = e.Id
# MAGIC   left join static_RiskLevel f
# MAGIC  on d.OverallRecalculatedRiskLevelId = f.Id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Party_CDDCase_QuestionAnswers

# COMMAND ----------

spark.read.format('delta').load('abfss://fec-radar@edlcorestdeuprod0001.dfs.core.windows.net/RadarDataModel/Party_CDDCase_QuestionAnswer/2/data/LOAD_DT=20251230/').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC null as PartyIdentifier,
# MAGIC a.PKEY_SRC_OBJECT as ClientID,
# MAGIC a.CASE_ID as CaseID,
# MAGIC null as UniqueCaseId, 
# MAGIC a.GRAM_INSTANCE_ID as InstanceId,
# MAGIC b.QuestionId,
# MAGIC b.QuestionText,
# MAGIC b.QuestionCode,
# MAGIC b.AnswerValue,
# MAGIC b.AnswerText,
# MAGIC b.IsMaterial as QuestionIsMaterialFlag
# MAGIC from c_b_due_diligence_xref a
# MAGIC left join Party_RiskModelInstanceQuestionAnswers b
# MAGIC on a.GRAM_INSTANCE_ID = b.InstanceId
# MAGIC  and b.SourceSystemName = 'RANZ - PegaCdd'

# COMMAND ----------

# MAGIC %md
# MAGIC ##Party_CDDCase_RiskCategories

# COMMAND ----------

spark.read.format('delta').load('abfss://fec-radar@edlcorestdeuprod0001.dfs.core.windows.net/RadarDataModel/Party_CDDCase_RiskCategories/2/data/LOAD_DT=20251230/').display()

# COMMAND ----------

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
# MAGIC select
# MAGIC null as PartyIdentifier,
# MAGIC a.PKEY_SRC_OBJECT as ClientID,
# MAGIC a.CASE_ID as CaseID,
# MAGIC null as UniqueCaseId, 
# MAGIC  c.GeographicalRiskLevel,
# MAGIC  c.EntityTypeRiskLevel,
# MAGIC  c.StructureRiskLevel,
# MAGIC  c.SectorRiskLevel,
# MAGIC  c.ProductAndServiceRiskLevel,
# MAGIC  c.PEPRiskLevel,
# MAGIC  c.TransactionRiskLevel,
# MAGIC  c.DistributionRiskLevel,
# MAGIC  c.ThirdPartyRiskLevel,
# MAGIC  c.AdverseInfoRiskLevel,
# MAGIC   f.Description as CalculatedRiskLevel,
# MAGIC  g.Description as RecalculatedRiskLevel,
# MAGIC  c.OtherRiskLevel,
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
# MAGIC from c_b_due_diligence_xref a
# MAGIC left join Party_RiskModelInstanceQuestionAnswers b
# MAGIC on a.GRAM_INSTANCE_ID = b.InstanceId
# MAGIC  and b.SourceSystemName = 'RANZ - PegaCdd'
# MAGIC left join RiskModelCategories c
# MAGIC on a.GRAM_INSTANCE_ID = c.InstanceId
# MAGIC left join RANZ_MaterialRisk d
# MAGIC on b.MaterialInstanceId = d.InstanceId
# MAGIC left join Party_RiskModelInstance e
# MAGIC on a.GRAM_INSTANCE_ID = e.InstanceId
# MAGIC  and e.SourceSystemName = 'RANZ - PegaCdd'
# MAGIC  left join static_RiskLevel f
# MAGIC  on e.OverallCalculatedRiskLevelId = f.Id
# MAGIC   left join static_RiskLevel g
# MAGIC  on e.OverallRecalculatedRiskLevelId = g.Id

# COMMAND ----------


