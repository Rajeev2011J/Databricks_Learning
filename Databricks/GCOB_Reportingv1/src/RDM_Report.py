# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal:
# MAGIC - To create a dataobject for RADAR Data Model Reporting
# MAGIC
# MAGIC #### Author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC #### Flow Of Logic
# MAGIC
# MAGIC - Read the RADAR Data Model objects and transform to report on it

# COMMAND ----------

# DBTITLE 1,Importing Libraries
import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Date Variables
date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(0)).strftime('%m/%d/%Y')
#display(BusinessDate)

# COMMAND ----------

# DBTITLE 1,Outh2 Configuration
ReadStorage = os.environ['GDP_STORAGE_NAME']
SALZReadStorage = 'saradar'+ environment
saradar_container = 'radardatamodel'
authenticate_storage_account(ReadStorage)
authenticate_storage_account(SALZReadStorage)

# COMMAND ----------

# DBTITLE 1,Read Objects on SA Landing Zone Storage
# # List of dataobjects from GDP
# load_df = pd.DataFrame({"GDPname": 
#     ['Party_CDDCase_RANZ'
#      ,'party_CDDCases_RiskCategory_RANZ'
#      ,'Party_RANZ'
#      ,'Party_RANZ_Naics'
#      ]})

# # Looping through the list of dataobjects and reading the data from GDP
# for index, row in load_df.iterrows():
#     spark.read.parquet(
#         f"abfss://{saradar_container}@{SALZReadStorage}.dfs.core.windows.net/{row.GDPname}/1/data/{load_dts}/*.parquet"
#     ).createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# DBTITLE 1,Read Objects from GDP Defined storage
# print list of strings for loading spark dfs from GDP
load_df = [
'Party'
,'Party_Address'
,'Party_CDDCase'
,'Party_CDDCase_QuestionAnswer'
,'Party_CDDCase_RiskCategories'
,'Party_Naics'
, 'Party_Structure'
# ,'Party_SystemIdentifier'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='RadarDataModel', Dataobject=Object, path_prefix='RadarDataModel', version_num=3)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Party_SystemIdentifier',
'Party_ClientOwnership'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='RadarDataModel', Dataobject=Object, path_prefix='RadarDataModel', version_num=3)

# COMMAND ----------

# DBTITLE 1,Define Object Names
RDM_Party_dataobject = 'RDM_Party'
RDM_structure_dataobject = 'RDM_PartyStructure'
RDM_PartyAddress_dataobject = 'RDM_PartyAddress'
RDM_PartyCDDCase_dataobject = 'RDM_PartyCDDCase'
RDM_PartyCDDCase_QuestionAnswer_dataobject = 'RDM_PartyCDDCase_QuestionAnswer'
RDM_PartyNaics_dataobject = 'RDM_PartyNaics'
# RDM_PartyCDDCase_RANZ_dataobject = 'RDM_PartyCDDCase_RANZ'
RDM_PartySystemIdentifier_dataobject = 'RDM_PartySystemIdentifier'
# RDM_CDDCases_RiskCategory_RANZ = 'RDM_PartyCDDCase_RiskCategory_RANZ'
RDM_CDDCase_RiskCategories = 'RDM_PartyCDDCase_RiskCategories'
# RDM_Party_RANZ_dataobject = 'RDM_Party_RANZ'
# RDM_Party_RANZ_Naics_dataobject = 'RDM_Party_RANZ_Naics'
RDM_Party_ClientOwnership_dataobject = 'RDM_Party_ClientOwnership'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

# DBTITLE 1,Derive Model Structure
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RDM_Structure AS
# MAGIC select s.*
# MAGIC ,child.FullLegalName as PartyChildFullLegalName,child_add.AddressType as child_AddressType,child_add.HouseNumber as child_HouseNumber,child_add.Street as child_Street,child_add.City as child_City,child_add.Region as child_Region,child_add.PostalCode as child_PostalCode,child_add.Country as child_Country
# MAGIC ,parent.FullLegalName as PartyParentFullLegalName,parent_add.AddressType as parent_AddressType,parent_add.HouseNumber as parent_HouseNumber,parent_add.Street as parent_Street,parent_add.City as parent_City,parent_add.Region as parent_Region,parent_add.PostalCode as parent_PostalCode,parent_add.Country as parent_Country
# MAGIC from Party_Structure s
# MAGIC left join Party child on s.PartyChildIdentifier = child.PartyIdentifier
# MAGIC left join Party_Address child_add on s.PartyChildIdentifier = child_add.PartyIdentifier
# MAGIC left join Party parent on s.PartyParentIdentifier = parent.PartyIdentifier
# MAGIC left join Party_Address parent_add on s.PartyParentIdentifier = parent_add.PartyIdentifier

# COMMAND ----------

# %sql
# CREATE OR REPLACE TEMPORARY VIEW RDM_CDDCase_RANZ AS
# select distinct 
# *
# ,case when RiskRating <> PreviousRiskRating and (RiskRating is not null OR PreviousRiskRating is not null) then 'Yes' else 'No' end as RiskDeviation
# from Party_CDDCase_RANZ 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RDM_ClientOwnership AS
# MAGIC Select distinct PartyIdentifier,ClientOwnerLocationCountryISOCode as GlobalClientOwnerLocation, ClientOwnerBusinessLineName as BusinessLineName from Party_ClientOwnership where ClientOwnerType='GlobalClientOwner' and PartyIdentifier is not null

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Data

# COMMAND ----------

# DBTITLE 1,Load Party in Catalog
df_Party = spark.table('Party')
df_Party=df_Party.withColumn("RefreshDate",lit(BusinessDate))
save_to_saradar_storage_account(df_Party, RDM_Party_dataobject)

# COMMAND ----------

# DBTITLE 1,Load Structure in Catalog
df_PartyStructure = spark.table('RDM_Structure')
save_to_saradar_storage_account(df_PartyStructure, RDM_structure_dataobject )

# COMMAND ----------

# DBTITLE 1,Load Address in Catalog
df_PartyAddress = spark.table('Party_Address')
save_to_saradar_storage_account(df_PartyAddress, RDM_PartyAddress_dataobject)

# COMMAND ----------

# DBTITLE 1,Load CDDCase in Catalog
df_PartyCDDCase = spark.table('Party_CDDCase')
save_to_saradar_storage_account(df_PartyCDDCase, RDM_PartyCDDCase_dataobject)

# COMMAND ----------

# DBTITLE 1,Load CDDCase_QuestionAnswer in Catalog
df_PartyCDDCase_QuestionAnswer = spark.table('Party_CDDCase_QuestionAnswer')
save_to_saradar_storage_account(df_PartyCDDCase_QuestionAnswer, RDM_PartyCDDCase_QuestionAnswer_dataobject)

# COMMAND ----------

# DBTITLE 1,Load Naics in Catalog
df_PartyNaics = spark.table('Party_Naics')
save_to_saradar_storage_account(df_PartyNaics, RDM_PartyNaics_dataobject)

# COMMAND ----------

# DBTITLE 1,Load Party RANZ in Catalog
# df_Party_RANZ = spark.table('Party_RANZ')
# df_Party_RANZ=df_Party_RANZ.withColumn("RefreshDate",lit(BusinessDate))
# save_to_saradar_storage_account(df_Party_RANZ, RDM_Party_RANZ_dataobject)

# COMMAND ----------

# DBTITLE 1,Load RANZ Naics in Catalog
# df_RANZ_Naics = spark.table('Party_RANZ_Naics')
# df_RANZ_Naics=df_RANZ_Naics.withColumn("RefreshDate",lit(BusinessDate))
# save_to_saradar_storage_account(df_RANZ_Naics, RDM_Party_RANZ_Naics_dataobject)

# COMMAND ----------

# DBTITLE 1,Load CDDCase_RANZ in Catalog
# df_PartyCDDCase_RANZ = spark.table('RDM_CDDCase_RANZ')
# df_PartyCDDCase_RANZ=df_PartyCDDCase_RANZ.withColumn("RefreshDate",lit(BusinessDate))
# save_to_saradar_storage_account(df_PartyCDDCase_RANZ, RDM_PartyCDDCase_RANZ_dataobject)

# COMMAND ----------

# DBTITLE 1,Load RiskCategory RANZ in Catalog
# df_RiskCategory_RANZ = spark.table('party_CDDCases_RiskCategory_RANZ')
# df_RiskCategory_RANZ=df_RiskCategory_RANZ.withColumn("RefreshDate",lit(BusinessDate))
# save_to_saradar_storage_account(df_RiskCategory_RANZ, RDM_CDDCases_RiskCategory_RANZ)

# COMMAND ----------

# DBTITLE 1,Load SystemIdentifier in Catalog
df_PartySystemIdentifier = spark.table('Party_SystemIdentifier')
save_to_saradar_storage_account(df_PartySystemIdentifier, RDM_PartySystemIdentifier_dataobject)

# COMMAND ----------


df_PartyCDDCaseRiskCategories = spark.table('Party_CDDCase_RiskCategories')
save_to_saradar_storage_account(df_PartyCDDCaseRiskCategories, RDM_CDDCase_RiskCategories)

# COMMAND ----------

df_PartyClientOwnership = spark.table('RDM_ClientOwnership')
save_to_saradar_storage_account(df_PartyClientOwnership, RDM_Party_ClientOwnership_dataobject)