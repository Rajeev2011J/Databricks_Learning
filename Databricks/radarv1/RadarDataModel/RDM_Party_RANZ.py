# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To create a separate dataobject to have all information about a Party  for RANZ (RABOBANK AUSTRALIA AND NEW ZEALAND)
# MAGIC
# MAGIC #### Authors
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### Flow of Logic
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |22-Aug-205 |11998029 |Initial Draft : RANZ Data Preparation for PowerBI
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
party_RANZ_dataobject='Party_RANZ'

# COMMAND ----------

# DBTITLE 1,Defining SA Landing Zone Storage Account
SALZReadStorage = 'salandingzonefecradarprd'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(SALZReadStorage)

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW MDMRANZ AS
# MAGIC select distinct * from MDM_LATEST

# COMMAND ----------

# DBTITLE 1,Fetching required columns from OCDD and merging AU and NZ
# MAGIC %sql
# MAGIC --Creating OCDD AU and NZ temp view
# MAGIC CREATE OR REPLACE TEMPORARY VIEW OCDDAUNZ AS
# MAGIC select distinct
# MAGIC `Client ID` as ClientId,
# MAGIC `Client Type` as ClientType,
# MAGIC upper(`Client Name`) as FullLegalName,
# MAGIC Country
# MAGIC from OCDD_Case_SummaryNZ
# MAGIC union all
# MAGIC select distinct
# MAGIC `Client ID` as ClientId,
# MAGIC `Client Type` as ClientType,
# MAGIC upper(`Client Name`) as FullLegalName,
# MAGIC Country
# MAGIC from OCDD_Resolved_Case_ReportAU

# COMMAND ----------

# DBTITLE 1,Logic to get OCDD Cases Information
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW OCDD_Clients AS
# MAGIC select distinct
# MAGIC --CONCAT('MDM_',OCDD.ClientID) as PartyIdentifier
# MAGIC CASE 
# MAGIC     WHEN g.identifier is not null THEN CONCAT('GCDS_',gcid)
# MAGIC     ELSE CONCAT('MDM_', ranz.contract_Id)
# MAGIC END AS PartyIdentifier
# MAGIC ,OCDD.ClientID
# MAGIC ,OCDD.ClientType
# MAGIC ,ranz.CLIENT_STATUS
# MAGIC --,ranz.CLIENT_BUSINESS_LINE
# MAGIC ,COALESCE(ranz.CLIENT_BUSINESS_LINE, g.`CO-businessline_description` ) AS CLIENT_BUSINESS_LINE
# MAGIC --,ranz.ISUBO
# MAGIC --,OCDD.FullLegalName
# MAGIC ,COALESCE(g.Full_Name, OCDD.FullLegalName) AS FullLegalName
# MAGIC ,OCDD.Country
# MAGIC --,ranz.PARTY_ID
# MAGIC from MDMRANZ ranz
# MAGIC inner join OCDDAUNZ OCDD on ranz.contract_Id = OCDD.ClientId
# MAGIC left join GCDS_Clients g on ranz.contract_Id = g.identifier and g.KeyStore_type = 'CB RANZ'

# COMMAND ----------

# DBTITLE 1,GCDS-RANZ
# MAGIC %sql
# MAGIC /*
# MAGIC Create or replace temporary view GCDS_RANZ As
# MAGIC select distinct
# MAGIC CONCAT('GCDS_',gcds.gcid) as PartyIdentifier
# MAGIC ,m.ClientID
# MAGIC ,m.ClientType
# MAGIC ,m.CLIENT_STATUS
# MAGIC ,COALESCE(m.CLIENT_BUSINESS_LINE, gcds.`CO-businessline_description` ) AS CLIENT_BUSINESS_LINE
# MAGIC ,COALESCE(gcds.Full_Name, m.FullLegalName) AS FullLegalName
# MAGIC ,m.Country
# MAGIC from GCDS_Clients gcds
# MAGIC INNER JOIN OCDD_Clients m on gcds.identifier = m.ClientID and gcds.KeyStore_type = 'CB RANZ'
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Create or replace temporary view NonGCDS_UniqueParty As
# MAGIC select * 
# MAGIC from OCDD_Clients a
# MAGIC left anti join GCDS_RANZ b
# MAGIC on a.ClientID = b.ClientID
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Create or replace temporary view All_RANZ_Party As
# MAGIC select distinct * from GCDS_RANZ 
# MAGIC union
# MAGIC select distinct * from NonGCDS_UniqueParty 
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view RANZ_Party As
# MAGIC select distinct
# MAGIC PartyIdentifier
# MAGIC ,MAX(CASE WHEN ClientID IS NOT NULL THEN ClientID END) AS ClientID
# MAGIC ,MAX(CASE WHEN ClientType IS NOT NULL THEN ClientType END) AS ClientType
# MAGIC ,MAX(CASE WHEN CLIENT_STATUS IS NOT NULL THEN CLIENT_STATUS END) AS CLIENT_STATUS
# MAGIC ,MAX(CASE WHEN CLIENT_BUSINESS_LINE IS NOT NULL THEN CLIENT_BUSINESS_LINE END) AS CLIENT_BUSINESS_LINE
# MAGIC ,MAX(CASE WHEN FullLegalName IS NOT NULL THEN FullLegalName END) AS FullLegalName
# MAGIC ,MAX(CASE WHEN Country IS NOT NULL THEN Country END) AS Country
# MAGIC from OCDD_Clients 
# MAGIC group by PartyIdentifier

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ##### CREATING A DATAFRAME AND LOADING THE FINAL DATAOBJECT TO SA RADAR STORAGE ACCOUNT

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_RANZ = spark.table('RANZ_Party')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
save_to_saradar_storage_account(
  df_party_RANZ, 
  party_RANZ_dataobject, 
  radar_datamodel_version_number, 
  environment
)
