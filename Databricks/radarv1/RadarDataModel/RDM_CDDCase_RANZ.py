# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To create a separate dataobject to have all information about a CDD Case  for RANZ (RABOBANK AUSTRALIA AND NEW ZEALAND)
# MAGIC
# MAGIC #### Authors
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC - Mahalakshmi.Vengateswaran@rabobank.com
# MAGIC
# MAGIC ##### Flow of Logic
# MAGIC - Reading Party_RiskModelInstanceQuestionAnswers from GRAM GDP
# MAGIC - Reading MDM Data from SA Landind Zone storage account where we get MDM RANZ files every month
# MAGIC - Reading the OCDD data from SA Landing Zone storage account
# MAGIC - Joining MDMRANZ Clients with GRAM to get the required details on a review Case
# MAGIC - Loading the final dataobject to SA RADAR Storage account
# MAGIC
# MAGIC #### Note: 
# MAGIC   - This dataobject(Party_CDDCase_RANZ) should be merged with Party_CDDCase once the RANZ(MDM OCCD) is in GDP and should be read from GDP
# MAGIC   - This object does not have all the details on a review case as we have only limited information and must be revisited
# MAGIC   - UniqueCaseId and Primary Key logic is not implemented yet
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |22-Aug-205 |11998029 |RANZ Data Preparation for PowerBI
# MAGIC    
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #####READING FILES FROM GDP

# COMMAND ----------

# DBTITLE 1,Importing required Packages
import os
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.functions import to_date, lit
import re


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

# DBTITLE 1,Defining the dataobjects' name
party_CDDcase_RANZ_dataobject='Party_CDDCase_RANZ'

# COMMAND ----------

# DBTITLE 1,Defining SA Landing Zone Storage Account
SALZReadStorage = 'salandingzonefecradarprd'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(SALZReadStorage)

# COMMAND ----------

# DBTITLE 1,Reading GRAM Dataobject
# List of dataobjects from GDP
load_df = [
'Party_RiskModelInstanceQuestionAnswers'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')

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

# MAGIC %md
# MAGIC %md
# MAGIC #####TRANSFORMATION LOGIC TO GET OCDD_CDDCASE OBJECT

# COMMAND ----------

# DBTITLE 1,Fetching required columns from MDM
# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW MDMRANZ AS
# MAGIC select distinct *
# MAGIC from MDM_LATEST

# COMMAND ----------

# DBTITLE 1,Fetching required columns from OCDD and merging AU and NZ
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW OCDDAUNZ AS
# MAGIC select distinct
# MAGIC `Case ID` as CaseId,
# MAGIC `Client ID` as ClientId,
# MAGIC `Review Type` as ReviewType,
# MAGIC `Next Review Date` as NextReviewDate,
# MAGIC `Risk Rating` as RiskRating,
# MAGIC `Previous Risk Rating` as PreviousRiskRating,
# MAGIC `Client Type` as ClientType,
# MAGIC `Completed On` CompletedOn,
# MAGIC `Account Manager` as AccountManager
# MAGIC from OCDD_Case_SummaryNZ
# MAGIC union all
# MAGIC select distinct
# MAGIC `Case ID` as CaseId,
# MAGIC `Client ID` as ClientId,
# MAGIC `Review Type` as ReviewType,
# MAGIC `Next Review Date` as NextReviewDate,
# MAGIC `Risk Rating` as RiskRating,
# MAGIC `Previous Risk Rating` as PreviousRiskRating,
# MAGIC `Client Type` as ClientType,
# MAGIC `Completed On` CompletedOn,
# MAGIC `Account Manager` as AccountManager
# MAGIC from OCDD_Resolved_Case_ReportAU

# COMMAND ----------

# DBTITLE 1,Logic to get OCDD Cases Information
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW OCDD_Cases AS
# MAGIC select distinct
# MAGIC 'OCDD' as Application
# MAGIC ,CASE 
# MAGIC     WHEN g.identifier is not null THEN CONCAT('GCDS_',gcid)
# MAGIC     ELSE CONCAT('MDM_', ranz.contract_Id)
# MAGIC END AS PartyIdentifier
# MAGIC ,OCDD.ClientID
# MAGIC ,OCDD.CaseID
# MAGIC ,OCDD.ReviewType
# MAGIC ,OCDD.NextReviewDate
# MAGIC ,OCDD.RiskRating
# MAGIC ,OCDD.PreviousRiskRating
# MAGIC ,OCDD.ClientType
# MAGIC ,OCDD.CompletedOn
# MAGIC ,OCDD.AccountManager
# MAGIC --,ranz.GRAM_INSTANCE_ID
# MAGIC from MDMRANZ ranz
# MAGIC inner join OCDDAUNZ OCDD on ranz.contract_Id = OCDD.ClientId
# MAGIC --inner join Party_RiskModelInstanceQuestionAnswers gram on OCDD.CaseID = gram.SourceSystemReferenceId and gram.SourceSystemName = 'RANZ - PegaCdd'
# MAGIC left join GCDS_Clients g on ranz.contract_Id = g.identifier and g.KeyStore_type = 'CB RANZ'

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ##### CREATING A DATAFRAME AND LOADING THE FINAL DATAOBJECT TO SA RADAR STORAGE ACCOUNT

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_CDDcase_RANZ = spark.table('OCDD_Cases')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
save_to_saradar_storage_account(
  df_party_CDDcase_RANZ, 
  party_CDDcase_RANZ_dataobject, 
  radar_datamodel_version_number, 
  environment
)
