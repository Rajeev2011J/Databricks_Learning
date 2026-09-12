# Databricks notebook source
# MAGIC %md
# MAGIC ####Goal
# MAGIC To create a separate dataobject to have CDD risk categories for RANZ (RABOBANK AUSTRALIA AND NEW ZEALAND)
# MAGIC
# MAGIC ####Authors
# MAGIC - Devi.Chennareddy@rabobank.com
# MAGIC
# MAGIC ####Flow of Logic
# MAGIC - Reading MDM Data from SA Landind Zone storage account where we get MDM RANZ files every month
# MAGIC - Reading the OCDD data from SA Landing Zone storage account
# MAGIC - Joining MDMRANZ Clients with GRAM to get the required cdd risk categories
# MAGIC - Loading the final dataobject to SA RADAR Storage account
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |22-Aug-205 |11998029 |RANZ Data Preparation for PowerBI

# COMMAND ----------

# DBTITLE 1,Importing required Packages
import os
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
import re
from pyspark.sql.functions import to_date, lit

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
import os
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
SALZReadStorage = 'salandingzonefecradarprd'

# COMMAND ----------

# DBTITLE 1,Importing modules from RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Defining the dataobjects' name
df_party_CDDCases_RiskCategory_RANZ_dataobject = 'party_CDDCases_RiskCategory_RANZ'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(SALZReadStorage)

# COMMAND ----------

# DBTITLE 1,ReadingRisk Dataobject
# List of dataobjects from GDP
load_df = [
'Party_RiskModelInstanceQuestionAnswers'
, 'Party_RiskModelCategories'
, 'Party_RiskModelInstance'
, 'party_WRCDDModel'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')

# COMMAND ----------

# DBTITLE 1,Reading GCOB Client Dataobject
# List of datasets from GDP
load_df = [
'party_case_client_details',
'party_AllPartyDetails' 
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# DBTITLE 1,Read GCDS data from GDP defined layer
# print list of strings for loading spark dfs from GDP
gcds_df = pd.DataFrame({'definedDatasetname':[
'client_Client',
'client_KeyStoreKey',
'client_OnboardedLocations',
'client_PartyRole',
'client_PartytoPartyRelationship',
'client_Products'
]})

# Create TempView for each loading table
for index, row in gcds_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=row.definedDatasetname)

# COMMAND ----------

# DBTITLE 1,GCDS Data
# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_Clients As
# MAGIC select distinct
# MAGIC   k.KeyStore_value as identifier
# MAGIC , k.KeyStore_type
# MAGIC , k.status AS status
# MAGIC , k.bank_code
# MAGIC , c.*
# MAGIC , pr.Party_role
# MAGIC , pr.Life_cycle_status
# MAGIC , rel.`Relationship-Type` as RelationshipType
# MAGIC , rel.`Relationship-Value` as RelationshipValue_GCID
# MAGIC , case when c.Party_type <> 'Natural Person' then c.Full_legal_name else c.Person_Name end Full_Name
# MAGIC from client_KeyStoreKey k
# MAGIC inner join client_Client c on c.GCID = k.GCID
# MAGIC left join client_PartyRole pr on k.GCID = pr.GCID
# MAGIC left join client_PartytoPartyRelationship rel on k.GCID = rel.GCID
# MAGIC where KeyStore_type in ('CB RANZ')

# COMMAND ----------

# DBTITLE 1,Reading MDM file from SA Landing zoneStorage Account
MDM_Files = dbutils.fs.ls(f"abfss://ranz-mdm@{SALZReadStorage}.dfs.core.windows.net/")

CurrentYearMonth = datetime.today().strftime('%Y%m')
list_count = 0

for file_info in MDM_Files:

    FileName = file_info.name
    split_list = re.split(r"[_,.]", FileName)
    file_recieval_date = split_list[2]

    if file_recieval_date[:6] == CurrentYearMonth:
        break

    list_count += 1


latest_mdm_df = spark.read \
    .format('csv') \
    .option('header','true') \
    .option('inferSchema', 'true') \
    .load(MDM_Files[list_count].path) \
    .withColumn('FILE_RECIEVAL_DATE', to_date(lit(file_recieval_date), 'yyyyMMdd')) \
    .createOrReplaceTempView('MDM_LATEST')

# COMMAND ----------

# DBTITLE 1,Reading OCDD CSV files from SA Landing Zone Storage account
#Reading OCDD files from storage and creating temp views

#csv_files_OCDD=["OCDD_Case_SummaryNZ","OCDD_Resolved_Case_ReportAU"]
#for file in csv_files_OCDD:
#    spark.read.csv('abfss://ranz-ocdd@salandingzonefecradarprd.dfs.core.windows.net/'+file, header=True).createOrReplaceTempView(file)


# COMMAND ----------

# DBTITLE 1,Reading OCDD Parquet files from SA Landing Zone Storage account
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'definedDatasetname':[
'OCDD_Resolved_Case_ReportAU'
,'OCDD_Case_SummaryNZ'
]})
 
# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://ranz-ocdd@salandingzonefecradarprd.dfs.core.windows.net/{row.definedDatasetname}/*.parquet').createOrReplaceTempView(row.definedDatasetname)

# COMMAND ----------

# DBTITLE 1,Create Static View
#Creating Static view for Risk Level
Risk_list = [0,1,2,3,4,5,6,7]
Risk_Description = ['Undefined', 'No', 'Low', 'Medium', 'High','Super','Extra High','Unacceptable']
#Creating spark dataframe from lists
df_gcob_static_RiskLevel = spark.createDataFrame(zip(Risk_list, Risk_Description), ['Id', 'Description'])
df_gcob_static_RiskLevel.createOrReplaceTempView('static_RiskLevel')

# COMMAND ----------

# DBTITLE 1,Fetching required columns from MDM
# MAGIC %sql
# MAGIC --Creating MDMRANZ temp view
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW MDMRANZ AS
# MAGIC select distinct
# MAGIC CONTRACT_ID
# MAGIC from MDM_LATEST

# COMMAND ----------

# DBTITLE 1,Fetching required columns from OCDD and merging AU and NZ
# MAGIC %sql
# MAGIC --Creating OCDD AU and NZ temp view
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW OCDDAUNZ AS
# MAGIC select distinct
# MAGIC `Case ID` as CaseId,
# MAGIC `Client ID` as ClientId
# MAGIC from OCDD_Case_SummaryNZ
# MAGIC union all
# MAGIC select distinct
# MAGIC `Case ID` as CaseId,
# MAGIC `Client ID` as ClientId
# MAGIC from OCDD_Resolved_Case_ReportAU

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW OCDD_Cases AS
# MAGIC select distinct
# MAGIC 'OCDD' as SourceSystem
# MAGIC ,OCDD.CaseID
# MAGIC ,OCDD.ClientID
# MAGIC ,gram.InstanceId
# MAGIC ,r.Description as CalculatedRiskLevelId
# MAGIC ,rl.Description as ReCalculatedRiskLevelId
# MAGIC
# MAGIC from MDMRANZ ranz
# MAGIC inner join OCDDAUNZ OCDD on ranz.contract_Id = OCDD.ClientID
# MAGIC LEFT join Party_RiskModelInstanceQuestionAnswers gram on OCDD.CaseID = gram.SourceSystemReferenceId
# MAGIC left join Party_RiskModelInstance rmi on gram.instanceid = rmi.instanceid
# MAGIC left join static_RiskLevel r on rmi.OverallCalculatedRiskLevelId = r.Id
# MAGIC left join static_RiskLevel rl on rmi.OverallRecalculatedRiskLevelId = rl.Id
# MAGIC   and gram.SourceSystemName = 'RANZ - PegaCdd'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RiskModelCategories AS
# MAGIC with InstanceCategory as (
# MAGIC      select InstanceId 
# MAGIC     ,Geographical AS GeoRisk
# MAGIC     ,`Entity Type` AS EntityTypeRisk
# MAGIC     ,Structure AS StructureRisk
# MAGIC     ,Sector AS SectorRisk
# MAGIC     ,`Products and Services` AS ProductRisk
# MAGIC     ,PEP AS PEPRisk
# MAGIC     ,`Transaction` AS TransactionRisk
# MAGIC     ,`Distribution Channel` AS DistributionRisk
# MAGIC     ,`Third Party` AS ThirdPartyRisk
# MAGIC     ,`Adverse Info` AS AdverseInfoRisk
# MAGIC     ,Other AS OtherRisk
# MAGIC
# MAGIC     FROM (
# MAGIC     select InstanceId
# MAGIC     , MAX(CASE WHEN CategoryName = 'Adverse Info' then CalculatedRiskLevelId else NULL END) as `Adverse Info`
# MAGIC     , MAX(CASE WHEN CategoryName = 'Distribution Channel' then CalculatedRiskLevelId else NULL END) as `Distribution Channel`
# MAGIC     , MAX(CASE WHEN CategoryName = 'Entity Type' then CalculatedRiskLevelId else NULL END) as `Entity Type`
# MAGIC     , MAX(CASE WHEN CategoryName = 'General' then CalculatedRiskLevelId else NULL END) as General
# MAGIC     , MAX(CASE WHEN CategoryName = 'Geographical' then CalculatedRiskLevelId else NULL END) as Geographical
# MAGIC     , MAX(CASE WHEN CategoryName = 'Other' then CalculatedRiskLevelId else NULL END) as Other
# MAGIC     , MAX(CASE WHEN CategoryName = 'PEP' then CalculatedRiskLevelId else NULL END) as PEP
# MAGIC     , MAX(CASE WHEN CategoryName = 'Products and Services' then CalculatedRiskLevelId else NULL END) as `Products and Services`
# MAGIC     , MAX(CASE WHEN CategoryName = 'Sector' then CalculatedRiskLevelId else NULL END) as Sector
# MAGIC     , MAX(CASE WHEN CategoryName = 'Structure' then CalculatedRiskLevelId else NULL END) as Structure
# MAGIC     , MAX(CASE WHEN CategoryName = 'Third Party' then CalculatedRiskLevelId else NULL END) as `Third Party`
# MAGIC     , MAX(CASE WHEN CategoryName = 'Transaction' then CalculatedRiskLevelId else NULL END) as `Transaction`
# MAGIC
# MAGIC     FROM
# MAGIC         Party_RiskModelCategories
# MAGIC     GROUP BY
# MAGIC         InstanceId
# MAGIC     )
# MAGIC     )
# MAGIC select distinct
# MAGIC geo.Description as GeographicalRiskLevel
# MAGIC , Ent.Description as EntityTypeRiskLevel
# MAGIC , stru.Description as StructureRiskLevel
# MAGIC , sec.Description as SectorRiskLevel
# MAGIC , prod.Description as ProductAndServiceRiskLevel
# MAGIC , pep.Description as PEPRiskLevel
# MAGIC , tran.Description as TransactionRiskLevel
# MAGIC , dist.Description as DistributionRiskLevel
# MAGIC , thir.Description as ThirdPartyRiskLevel
# MAGIC , adv.Description as AdverseInfoRiskLevel
# MAGIC ,InstanceId
# MAGIC FROM 
# MAGIC InstanceCategory AS rf
# MAGIC LEFT OUTER JOIN static_RiskLevel geo ON rf.GeoRisk = geo.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel Ent ON rf.EntityTypeRisk = Ent.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel stru ON rf.StructureRisk = stru.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel sec ON rf.SectorRisk = sec.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel prod ON rf.ProductRisk = prod.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel pep ON rf.PEPRisk = pep.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel tran ON rf.TransactionRisk = tran.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel dist ON rf.DistributionRisk = dist.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel thir ON rf.ThirdPartyRisk = thir.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel adv ON rf.AdverseInfoRisk = adv.Id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating temp view for generating Risk dataobject
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW OCDD_Risk AS
# MAGIC select distinct 
# MAGIC CASE 
# MAGIC     WHEN g.identifier is not null THEN CONCAT('GCDS_',gcid)
# MAGIC     ELSE CONCAT('MDM_', oc.ClientID)
# MAGIC END AS PartyIdentifier
# MAGIC ,oc.CaseID
# MAGIC ,oc.ClientID
# MAGIC ,c.GeographicalRiskLevel
# MAGIC ,c.EntityTypeRiskLevel
# MAGIC ,c.StructureRiskLevel
# MAGIC ,c.SectorRiskLevel
# MAGIC ,c.ProductAndServiceRiskLevel
# MAGIC ,c.PEPRiskLevel
# MAGIC ,c.TransactionRiskLevel
# MAGIC ,c.DistributionRiskLevel
# MAGIC ,c.ThirdPartyRiskLevel
# MAGIC ,c.AdverseInfoRiskLevel
# MAGIC ,oc.CalculatedRiskLevelId
# MAGIC ,oc.ReCalculatedRiskLevelId
# MAGIC From OCDD_Cases oc
# MAGIC left join RiskModelCategories c on oc.InstanceId = c.InstanceId
# MAGIC left join GCDS_Clients g on oc.ClientID = g.identifier and g.KeyStore_type = 'CB RANZ'

# COMMAND ----------

df_party_CDDCases_RiskCategory_RANZ=spark.table('OCDD_Risk')

# COMMAND ----------

save_to_saradar_storage_account(df_party_CDDCases_RiskCategory_RANZ, df_party_CDDCases_RiskCategory_RANZ_dataobject, radar_datamodel_version_number, environment)
