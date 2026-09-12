# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal:
# MAGIC For RANZ - to get all business activities per party
# MAGIC - through MDM / T24
# MAGIC
# MAGIC #### Authors
# MAGIC - Ruud.van.Laar@rabobank.com
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ### components
# MAGIC 1. set up connections
# MAGIC 2. load data, write into default model
# MAGIC - Gcid
# MAGIC - other systemid
# MAGIC - other systemname
# MAGIC - region
# MAGIC - country
# MAGIC - businessline.
# MAGIC 3. bring all together and export
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |22-Aug-205 |11998029 |Initial Draft : RANZ Data Preparation for PowerBI
# MAGIC

# COMMAND ----------

# DBTITLE 1,Importing required Packages
#import libraries
import os
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.functions import to_date, lit
import re
from RadarUtils import *


# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
SARADAR = "saradar" + environment
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
SALZReadStorage = 'salandingzonefecradarprd'

# COMMAND ----------

# DBTITLE 1,Defining the dataobjects' name
party_RANZ_Naics_dataobject='Party_RANZ_Naics'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(SARADAR)
authenticate_storage_account(SALZReadStorage)

# COMMAND ----------

# DBTITLE 1,Read RANZ_QuestionAnswer from SARADRA Storage
Today = datetime.today().strftime("%Y%m%d")
load_dts = "EDL_LOAD_DTS=" + Today + "*"

df_RANZ_QuestionAnswer = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_CDDCase_QuestionAnswer_RANZ/1/data/{load_dts}/*.parquet"
)
df_RANZ_QuestionAnswer.createOrReplaceTempView('RANZ_QuestionAnswer')

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET FROM GCDS

# COMMAND ----------

# DBTITLE 1,Read GCDS data from GDP defined layer
# print list of strings for loading spark dfs from GDP
gcds_df = pd.DataFrame({'definedDatasetname':[
'client_Client',
'client_KeyStoreKey',
'client_PartyRole',
'client_PartytoPartyRelationship'
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
# MAGIC where KeyStore_type in ('CB RANZ','T24 AUZ','T24')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Get from MDM

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

from pyspark.sql.functions import to_date, lit

latest_mdm_df = spark.read \
    .format('csv') \
    .option('header','true') \
    .option('inferSchema', 'true') \
    .load(MDM_Files[list_count].path) \
    .withColumn('FILE_RECIEVAL_DATE', to_date(lit(file_recieval_date), 'yyyyMMdd')) \
    .createOrReplaceTempView('MDM')

# COMMAND ----------

# DBTITLE 1,TODO: replace with direct connact from MDM to GRAM. without OCDD below
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RDM_OCDD_QA AS
# MAGIC select 
# MAGIC CaseId,
# MAGIC --CONTRACT_ID as ClientId,
# MAGIC ClientId,
# MAGIC 'OCDD-GRAM' AS LocalSystemName,
# MAGIC AnswerValue AS NaicsCode,
# MAGIC AnswerText As NaicsDescription
# MAGIC from RANZ_QuestionAnswer--hive_metastore.radar.ranzmdmgramcddquestionsanswers
# MAGIC Where QuestionCode = 'SEC-Q1'

# COMMAND ----------

# DBTITLE 1,Prepare MDM data
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RDM_Client_MDM AS
# MAGIC select distinct    
# MAGIC CASE 
# MAGIC     WHEN g.identifier is not null THEN CONCAT('GCDS_',gcid)
# MAGIC     ELSE CONCAT('MDM_', CONTRACT_ID)
# MAGIC END AS PartyIdentifier
# MAGIC , CONTRACT_ID as ClientId
# MAGIC , 'MDM' AS Party_Type
# MAGIC , 'RANZ' AS GlobalClientOwnerLocation
# MAGIC , CLIENT_BUSINESS_LINE AS BusinessLineName
# MAGIC , 'W&R' AS WR_OR_RETAIL
# MAGIC , 'RANZ' AS GlobalClientOwnerRegion
# MAGIC , CASE 
# MAGIC     WHEN RIGHT(CLIENT_BUSINESS_LINE,2) = 'CB' THEN 'RANZ Country Banking'
# MAGIC     WHEN RIGHT(CLIENT_BUSINESS_LINE,2) = 'OS' THEN 'RANZ Rabo Online Savings'
# MAGIC     END AS Region_Derived
# MAGIC , null as RefreshDate
# MAGIC , REPLACE(Client_Status, 'Active Client','Client') AS Client_Status
# MAGIC ,g.KeyStore_type
# MAGIC FROM MDM
# MAGIC left join GCDS_Clients g on MDM.CONTRACT_ID = g.identifier and g.KeyStore_type = 'CB RANZ'
# MAGIC WHERE PARTY_ID = CONTRACT_ID
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC AND REL_TYPE_CD = 'PHOLD'
# MAGIC --and ((CONCAT('GCDS_',Global_Client_ID) not in (select distinct PartyIdentifier from hive_metastore.radar.RDM_Party )) OR (Global_Client_ID is null))

# COMMAND ----------

# DBTITLE 1,Create MDM-OCDD-Naics View
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW MDM_Naics AS
# MAGIC select distinct
# MAGIC 'MDM' as Application
# MAGIC ,m.PartyIdentifier
# MAGIC ,m.ClientId
# MAGIC ,m.Party_Type
# MAGIC --,m.GlobalClientOwnerLocation
# MAGIC ,m.WR_OR_RETAIL
# MAGIC ,m.GlobalClientOwnerRegion
# MAGIC ,m.Region_Derived
# MAGIC ,m.Client_Status
# MAGIC ,o.NaicsCode
# MAGIC ,TRIM(SUBSTRING_INDEX(o.NaicsDescription, '-', -1)) AS NaicsDescription
# MAGIC From RDM_Client_MDM m
# MAGIC inner join RDM_OCDD_QA o on m.ClientId = o.ClientId

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get from T24

# COMMAND ----------

# DBTITLE 1,ToDo: Reac Correct latest data from EBX - Read EBX File
#Read SIC to NAICS table from RANZ EBX
SIC_TO_NAICS = pd.read_csv('/Workspace/data/ESG_Input/2025-05-06 NAICS Sustainability/ANZSIC_To_NAICS_EBX.csv', delimiter = ';')

# TODO: potentially preprocess ANZSIC to have 4 digit always? add leading zeros if required?

# create spark sql view out of it
spark.createDataFrame(SIC_TO_NAICS).createOrReplaceTempView('SIC_TO_NAICS')

# COMMAND ----------

# DBTITLE 1,Read T24 list from container
t24_files = dbutils.fs.ls("abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/")
t24_files_customer_2025 = [file.name for file in t24_files if 'CUSTOMERS_2025' in file.name]

# COMMAND ----------

#print(t24_files_customer_2025)

# COMMAND ----------

# DBTITLE 1,Read T24 CUSTOMERS_2025
sdf_cb_au = (spark.read.parquet("abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/202504190303-T24.CB.AU.CUSTOMERS_20250422110132.parquet/")
             .withColumn('BusinessLine', lit('Country Banking'))
             .withColumn('Location', lit('Australia')))

sdf_cb_nz = (spark.read.parquet('abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/202504190303-T24.CB.NZ.CUSTOMERS_20250422110132.parquet/')
             .withColumn('BusinessLine', lit('Country Banking'))
             .withColumn('Location', lit('New Zealand')))

sdf_rd_nz = (spark.read.parquet('abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/202504222244-T24.RD.NZ.CUSTOMERS_20250422120400.parquet/')
             .withColumn('BusinessLine', lit('Rabo Online Savings'))
             .withColumn('Location', lit('New Zealand')))

sdf_rd_au = (spark.read.parquet('abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/202504222334-T24.RD.AU.CUSTOMERS_20250422120400.parquet/')
             .withColumn('BusinessLine', lit('Rabo Online Savings'))
             .withColumn('Location', lit('Australia')))

# COMMAND ----------

# DBTITLE 1,Union T24 dataframes
# union above together in one dataframe.
ranz_customers = sdf_cb_au.union(sdf_cb_nz).union(sdf_rd_nz).union(sdf_rd_au)

# remove spaces in column names so it can be stored as delta
for col in ranz_customers.columns:
    ranz_customers = ranz_customers.withColumnRenamed(col,col.replace("\\s", "_").replace("(", "").replace(")", "").replace(" ", "_"))

# COMMAND ----------

# DBTITLE 1,Select all customers from RANZ t24
ranz_customers.createOrReplaceTempView('T24_CUST')
## Potentially filter on CUSTOMER STATUS = 'ACTIVE'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW T24_Client AS
# MAGIC Select
# MAGIC CASE 
# MAGIC     WHEN g.identifier is not null THEN CONCAT('GCDS_',gcid)
# MAGIC     ELSE CONCAT('T24_', Customer_IDCIF_number)
# MAGIC END AS PartyIdentifier,
# MAGIC Customer_IDCIF_number as ClientId,
# MAGIC  'RANZ_T24' AS Party_type,
# MAGIC `Location` AS GlobalClientOwnerLocation,
# MAGIC BusinessLine AS BusinessLineName,
# MAGIC 'W&R' AS WR_OR_RETAIL,
# MAGIC 'empty' AS GlobalClientOwnerRegion,
# MAGIC 'RANZ' AS Region_Derived,
# MAGIC null as RefreshDate,
# MAGIC Customer_status AS LifeCycleStatus
# MAGIC from t24_cust
# MAGIC left join GCDS_Clients g on t24_cust.Customer_IDCIF_number = g.identifier and g.KeyStore_type in ('T24 AUZ','T24')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW T24_CUST_NAICS AS
# MAGIC   SELECT distinct t1.Branch_no, 
# MAGIC    t1.Customer_IDCIF_number as ClientId,
# MAGIC    t1.`SIC_value`,
# MAGIC    t2.`NAICS Code - Code` AS NAICSCODE,
# MAGIC    t2.`NAICS Desc` as NaicsDescription
# MAGIC   FROM  T24_CUST as T1
# MAGIC   -- SIC value sometimes has A or B, and is always 4 digit including trailing zero. Mapping table needs to have trailing zero added and is sometimes lenght 3 of it's own.
# MAGIC   INNER JOIN SIC_TO_NAICS as t2 on LEFT(t1.`SIC_value`,4) = RIGHT(CONCAT('00',t2.`ANZSIC Code`),4)
# MAGIC   WHERE Customer_status = 'ACTIVE'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW T24_Naics AS
# MAGIC select distinct
# MAGIC 'T24' as Application
# MAGIC ,m.PartyIdentifier
# MAGIC ,m.ClientId
# MAGIC ,m.Party_Type
# MAGIC --,m.GlobalClientOwnerLocation
# MAGIC ,m.WR_OR_RETAIL
# MAGIC ,m.GlobalClientOwnerRegion
# MAGIC ,m.Region_Derived
# MAGIC --,m.LifeCycleStatus as Client_Status
# MAGIC ,REPLACE(m.LifeCycleStatus, 'ACTIVE','Client') AS Client_Status
# MAGIC ,o.NaicsCode
# MAGIC ,o.NaicsDescription
# MAGIC From T24_Client m
# MAGIC inner join T24_CUST_NAICS o on m.ClientId = o.ClientId

# COMMAND ----------

# DBTITLE 1,Union MDM & T24 Naics
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW MDM_T24_Naics AS
# MAGIC select * from MDM_Naics
# MAGIC union
# MAGIC select * from T24_Naics

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CREATING A DATAFRAME AND LOADING THE FINAL DATAOBJECT TO SA RADAR STORAGE ACCOUNT

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_MDM_T24_Naics=spark.table('MDM_T24_Naics')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
save_to_saradar_storage_account(df_MDM_T24_Naics, party_RANZ_Naics_dataobject, radar_datamodel_version_number, environment)
