# Databricks notebook source
# DBTITLE 1,TODO
# MAGIC %md
# MAGIC ###### TODO: 
# MAGIC **1. When getting T24 customers (in the other notebook), get Filename to assign businessline and location**
# MAGIC -- done.
# MAGIC
# MAGIC **2. when mapping SIC to NAICS, see what happens when naics is not exactly 4 characters**
# MAGIC --> done.
# MAGIC **3. Clean up parts with mapping that are old.**
# MAGIC ?
# MAGIC **FI split E&A & Asia**
# MAGIC --> FIhubIndicator. done
# MAGIC
# MAGIC **5.. Many empty Naics RANZ**
# MAGIC --> Did some, but some ANZSIC codes area not in the ANZSIC - to NAICS list.
# MAGIC
# MAGIC **6 clients not mapping to anywhere?**
# MAGIC --> TODO
# MAGIC
# MAGIC
# MAGIC **7. boom. refresh dashboard.**
# MAGIC See local notebook (OneNote): Analytics Engineering -> ESG Naics
# MAGIC
# MAGIC **Add NLS customers.**
# MAGIC - either NLSvf or RAF_CUSTOMERS
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Context
# MAGIC Requested by: Mark Kuipers
# MAGIC
# MAGIC **For Sustainability**
# MAGIC is requesting on behalf of Sustainability purpose, to find - matching flexcube trades - the right sectors for these clients.
# MAGIC
# MAGIC #### Flow of logic
# MAGIC 1. Connect to sources
# MAGIC - GDP
# MAGIC - RDM
# MAGIC - RDM - OCDD
# MAGIC 2. Define base tables
# MAGIC - Clients
# MAGIC - NAICS
# MAGIC - Mapping tables
# MAGIC 3. build final tables
# MAGIC - Clients
# MAGIC - NAICS_ESG
# MAGIC 4. store final tables
# MAGIC 5. (load semantic model)
# MAGIC 6. (update report)
# MAGIC
# MAGIC
# MAGIC - 1. Get Party: all clients
# MAGIC   - 1.1 get mapping of location/ bl to the right portfolio
# MAGIC - 2. Get Party_naics
# MAGIC   - 2.1 Get mapping of naics to sustainability- related sectors
# MAGIC - 3. Process data
# MAGIC   - Clients table; to have all customer-related context
# MAGIC   - Client_Naics table; to have Naics data per party in such a way that the pivot in PowerBI will work.

# COMMAND ----------

# DBTITLE 1,Needed for loading excel file
pip install openpyxl

# COMMAND ----------

import pandas as pd
import os
from datetime import datetime
import re
import openpyxl




# COMMAND ----------

# MAGIC %md
# MAGIC ### Connecting Sources

# COMMAND ----------


app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,connect saradar
saradar_write_storage = f'saradar{environment}'
spark.conf.set("fs.azure.account.auth.type."+saradar_write_storage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+saradar_write_storage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+saradar_write_storage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+saradar_write_storage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+saradar_write_storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# DBTITLE 1,connect sa landingzone
SALZReadStorage = 'salandingzonefecradarprd'

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage +".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage +".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage +".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage +".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage +".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

# DBTITLE 1,connect edlcore
SALZReadStorage = 'edlcorestdeuprod0001'

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage +".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage +".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage +".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage +".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage +".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initiate Base Tables
# MAGIC - Radar RDM
# MAGIC - MDM/OCDD/GRAM naics.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.radar.RDM_Party limit 3

# COMMAND ----------

# DBTITLE 1,RDM_Client_Naics
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RDM_NAICS AS
# MAGIC
# MAGIC select PartyIdentifier
# MAGIC , LocalSystemId
# MAGIC , LocalSystemName
# MAGIC , NAICScode
# MAGIC
# MAGIC from hive_metastore.radar.rdm_partynaics
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading GCDS

# COMMAND ----------

# DBTITLE 1,Loading GCDS
# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
      , 'client_Client'
      , 'client_PartyRole'
]

for item in gcds_tables:
  # get the most recent file available in gdp
  path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4601/data/'
  files = dbutils.fs.ls(path)
  load_date = max(file.path.split('EDL_LOAD_DTS=')[1][:8] for file in files if 'EDL_LOAD_DTS=' in file.path)

  spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4601/data/EDL_LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RANZ specific

# COMMAND ----------

# DBTITLE 1,MDM / OCDD Questions Answers for NAICS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RDM_OCDD_QA AS
# MAGIC
# MAGIC select 
# MAGIC CaseId,
# MAGIC CONTRACT_ID,
# MAGIC 'OCDD-GRAM' AS LocalSystemName,
# MAGIC AnswerValue AS NaicsCode
# MAGIC
# MAGIC from hive_metastore.radar.ranzmdmgramcddquestionsanswers
# MAGIC Where QuestionCode = 'SEC-Q1'

# COMMAND ----------

display(Naics_mapping_raw)

# COMMAND ----------

# DBTITLE 1,Loading Naics Mapping
# 
Naics_mapping_raw = pd.read_excel('/Workspace/data/ESG_Input/2025-05-06 NAICS Sustainability/20250327 NAICS Mapping.xlsx')

# Unpivot the matrix / table
Naics_mapping_stage1 = pd.melt(Naics_mapping_raw, id_vars = ['NAICS 2017', 'NAICS 2017 Description', 'NAICS 2022',
       'NAICS 2022 Description'] , value_vars = ['D&LC', 'Climate',
       'Human Rights', 'Labor Rights', 'Land Governance', 'Nature', 'AC&F',
       'Agri. Comm. Deriv. Trad.', 'Agrochemicals', 'Animal Welfare',
       'Aquaculture and fisheries', 'Armaments industry', 'Energy',
       'Metals, Minerals and Mining', 'Plant Gene Technology',
       'Ship Breaking and Recycling', 'Tobacco'], var_name = 'ESG_THEME')

# drop null values
Naics_mapping_processed = Naics_mapping_stage1[~Naics_mapping_stage1.value.isna()]

Naics_mapping_processed_ACF_DLC = Naics_mapping_processed.loc[Naics_mapping_processed.ESG_THEME.isin(['AC&F', 'D&LC'])]
Naics_mapping_processed_ACF_DLC.ESG_THEME = 'ACF & DLC'
Naics_mapping_processed = pd.concat([Naics_mapping_processed, Naics_mapping_processed_ACF_DLC])

#create spark sql view out of it
spark.createDataFrame(Naics_mapping_processed).createOrReplaceTempView('NAICS_TO_ESG_TOPICS')

# COMMAND ----------

# DBTITLE 1,Loading SIC-NAICS mapping
#Read SIC to NAICS table from RANZ EBX
SIC_TO_NAICS = pd.read_csv('/Workspace/data/ESG_Input/2025-05-06 NAICS Sustainability/ANZSIC_To_NAICS_EBX.csv', delimiter = ';')

# TODO: potentially preprocess ANZSIC to have 4 digit always? add leading zeros if required?

# create spark sql view out of it
spark.createDataFrame(SIC_TO_NAICS).createOrReplaceTempView('SIC_TO_NAICS')

# COMMAND ----------

SIC_TO_NAICS

# COMMAND ----------

# DBTITLE 1,T24_customers
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW T24_CUST AS
# MAGIC
# MAGIC   SELECT Branch_no, 
# MAGIC   Customer_IDCIF_number,
# MAGIC   Customer_Country,
# MAGIC   Customer_status,
# MAGIC   `Location`,
# MAGIC   BusinessLine
# MAGIC
# MAGIC   FROM  FEC_ESG.T24_CUSTOMERS
# MAGIC   WHERE Customer_status = 'ACTIVE'

# COMMAND ----------

# DBTITLE 1,T24 NAICS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW T24_CUST_NAICS AS
# MAGIC
# MAGIC   SELECT t1.Branch_no, 
# MAGIC    t1.Customer_IDCIF_number,
# MAGIC    t1.`SIC_value`,
# MAGIC    t2.`NAICS Code - Code` AS NAICSCODE
# MAGIC
# MAGIC   FROM  FEC_ESG.T24_CUSTOMERS as T1
# MAGIC   -- SIC value sometimes has A or B, and is always 4 digit including trailing zero. Mapping table needs to have trailing zero added and is sometimes lenght 3 of it's own.
# MAGIC   LEFT JOIN SIC_TO_NAICS as t2 on LEFT(t1.`SIC_value`,4) = RIGHT(CONCAT('00',t2.`ANZSIC Code`),4)
# MAGIC   WHERE Customer_status = 'ACTIVE'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SIC_TO_NAICS 
# MAGIC where `ANZSIC Code` in (211, 119)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from T24_cust_naics 
# MAGIC where NAICSCODE IS NULL
# MAGIC AND SIC_value is not null
# MAGIC --limit 3

# COMMAND ----------

# DBTITLE 1,get a feel for numbers in RANZ naics
# MAGIC %sql
# MAGIC select count(t1.GCID) as count
# MAGIC --,t1.Primary_NAICS
# MAGIC --, t1.`Global_CO-location_code` 
# MAGIC
# MAGIC from client_Client t1
# MAGIC inner join client_PartyRole t2 on t1.gcid = t2.gcid
# MAGIC Where  t1.`Global_CO-location_code`in ('AUS', 'NZL')
# MAGIC and t2.Life_cycle_status = 'Client'
# MAGIC And t2.Party_role = 'Customer'
# MAGIC
# MAGIC --GROUP BY  `Global_CO-location_code`, Primary_NAICS
# MAGIC

# COMMAND ----------

"""
%sql
select GCID
, Primary_NAICS
, Full_Legal_name
, `Global_CO-location_code` 

from client_Client
Where  `Global_CO-location_code`in ('AUS', 'NZL')
"""

# COMMAND ----------

# DBTITLE 1,ADD: MDM / OCDD-GRAM NAICS Input.
## Adding RANZ naics
"""
The logic is:
If available in OCDD-GRAM, then take those. 
Else, take from old MDM export start 2023.

"""
SALZReadStorage = 'salandingzonefecradarprd'
MDM_Files = dbutils.fs.ls(f"abfss://ranz-mdm@{SALZReadStorage}.dfs.core.windows.net/")

# Loop through the list and print the name of each file

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

# DBTITLE 1,MDM clients
# MAGIC
# MAGIC %sql
# MAGIC -- MDM selection for NAICS
# MAGIC select Contract_ID
# MAGIC , PARTY_ID
# MAGIC , Client_Status
# MAGIC , CLIENT_BUSINESS_LINE
# MAGIC , GLOBAL_CLIENT_ID
# MAGIC , REL_TYPE_CD
# MAGIC , FILE_RECIEVAL_DATE
# MAGIC FROM MDM
# MAGIC WHERE PARTY_ID = CONTRACT_ID
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC AND REL_TYPE_CD = 'PHOLD'
# MAGIC LIMIT 3
# MAGIC -- question? Where are naics compared to other MDM file in OneLab that does appear to have NAICS?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Mapping tables
# MAGIC - naics to topics
# MAGIC - location to region
# MAGIC - naics definitions
# MAGIC
# MAGIC
# MAGIC ##### question
# MAGIC How to map MDM contract id / partyId to party_identifier

# COMMAND ----------

# DBTITLE 1,Location - region mapping
location_region_mapping = {'ARG': 'SA',
'ATL': 'NA',
'AUS': 'RANZ',
'BEL': 'E&A',
'BRA': 'SA',
'CAN': 'NA',
'CHI': 'SA',
'CHL': 'SA',
'DEU': 'E&A',
'ESP': 'E&A',
'FRA': 'E&A',
'GBR': 'E&A',
'HKG': 'Asia',
'Input Finance - North America': 'Rural - NA (InputFinance)',
'IRL': 'E&A',
'ITA': 'E&A',
'KEN': 'E&A',
'MEX': 'NA',
'Netherlands': 'E&A',
'NEY': 'NA',
'null': '',
'NZL': 'RANZ',
'RAF': 'Rural - NA',
'SGP': 'Asia',
'SHA': 'Asia',
'UTR': 'E&A',

'Rabo Securities USA, Inc. (RSEC)': 'NA',
'Rabobank - RANZ Country Banking and ROS': 'Rural- RANZ',
'Rabobank - USA Rabo AgriFinance': 'Rural - NA',
'Rabobank Antwerp': 'E&A',
'Rabobank Argentina': 'SA',
'Rabobank Australia': 'RANZ',
'Rabobank Canada (RCBR)': 'NA',
'Rabobank Canada(Rural)': 'Rural - NA',
'Rabobank Chile': 'SA',
'Rabobank China': 'Asia',
'Rabobank Dublin': 'E&A',
'Rabobank Foundation': 'not rabo',
'Rabobank Frankfurt': 'E&A',
'Rabobank Hong Kong': 'Asia',
'Rabobank India': 'Asia',
'Rabobank Kenya': 'E&A',
'Rabobank London': 'E&A',
'Rabobank Madrid': 'E&A',
'Rabobank Milan': 'E&A',
'Rabobank Netherlands': 'E&A',
'Rabobank New York': 'NA',
'Rabobank New Zealand': 'RANZ',
'Rabobank Paris': 'E&A',
'Rabobank Singapore': 'Asia',
'Rabobank Turkey': 'E&A',

'RANZ Country Banking and ROS': 'Rural- RANZ',
'USA Rabo AgriFinance': 'Rural - NA',
'Antwerp': 'E&A',
'Argentina': 'SA',
'Australia': 'RANZ',
'Canada (RCBR)': 'NA',
'Canada(Rural)': 'Rural - NA',
'Chile': 'SA',
'China': 'Asia',
'Dublin': 'E&A',
'Foundation': 'not rabo',
'Frankfurt': 'E&A',
'Hong Kong': 'Asia',
'India': 'Asia',
'Kenya': 'E&A',
'London': 'E&A',
'Madrid': 'E&A',
'Milan': 'E&A',
'Netherlands': 'E&A',
'New York': 'NA',
'New Zealand': 'RANZ',
'Paris': 'E&A',
'Singapore': 'Asia',
'Turkey': 'E&A',
'Shanghai': 'Asia',
'Germany': 'E&A',
'France': 'E&A',
'Belgium': 'E&A',
'Italy': 'E&A',
'Spain': 'E&A',
'Ireland': 'E&A',
'UTRECHT': 'E&A',
'Utrecht': 'E&A',

'Rabobank Brazil': 'SA',
'Brazil': 'SA',
'Chicago': 'NA',
'Atlanta': 'NA',
'Mexico': 'NA',
'Canada': 'NA',
'Rabobank Canada (Rural)': 'Rural - NA'
}

df_locationToRegionMapping = pd.DataFrame.from_dict(columns=['Region'], data = location_region_mapping, orient = 'Index')
df_locationToRegionMapping.reset_index(inplace = True)
df_locationToRegionMapping.columns = ['Location','Region']
spark.createDataFrame(df_locationToRegionMapping).createOrReplaceTempView('locationToRegionMapping')


# COMMAND ----------

# DBTITLE 1,Process Naics Data
# MAGIC %sql
# MAGIC select distinct LocalSystemName from  hive_metastore.radar.RDM_PartyNaics 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from T24_cust_naics limit 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Building final tables.

# COMMAND ----------

# DBTITLE 1,ALL NAICS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RDM_NAICS_ESG AS
# MAGIC
# MAGIC WITH NAICS AS 
# MAGIC (
# MAGIC select PartyIdentifier
# MAGIC , LocalSystemId
# MAGIC , LocalSystemName
# MAGIC , NAICScode
# MAGIC
# MAGIC from RDM_NAICS
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select 
# MAGIC     CASE 
# MAGIC         WHEN t2.Global_Client_ID is not null THEN CONCAT('GCDS_',t2.Global_Client_ID)
# MAGIC         ELSE CONCAT('MDM_', t1.CONTRACT_ID)
# MAGIC     END AS PartyIdentifier
# MAGIC     ,   t1.CONTRACT_ID AS LocalSystemId
# MAGIC     ,   t1.LocalSystemName
# MAGIC     ,   t1.NaicsCode
# MAGIC
# MAGIC FROM RDM_OCDD_QA AS t1
# MAGIC LEFT JOIN MDM t2 on t1.Contract_ID = t2.Contract_ID
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select 
# MAGIC         CONCAT('T24_', Customer_IDCIF_number) AS PartyIdentifier
# MAGIC     ,   t1.Customer_IDCIF_number AS LocalSystemId
# MAGIC     ,   'T24' AS LocalSystemName
# MAGIC     ,   t1.NaicsCode
# MAGIC
# MAGIC FROM T24_cust_naics AS t1
# MAGIC
# MAGIC UNION
# MAGIC Select PartyIdentifier
# MAGIC , LocalSystemId
# MAGIC , LocalSystemName
# MAGIC , NaicsCode
# MAGIC FROM FEC_ESG.RAF_NAICS
# MAGIC
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC         t1.*
# MAGIC     ,   t2.ESG_THEME AS ESG_RELATED_TOPIC
# MAGIC     ,   t2.`NAICS 2022 Description` AS NaicsDescription
# MAGIC from NAICS AS t1
# MAGIC LEFT JOIN NAICS_TO_ESG_TOPICS AS t2 on t1.NaicsCode = t2.`NAICS 2022`
# MAGIC --LEFT JOIN Naics_topic_mapping AS t2 on t1.NAICScode = t2.Code
# MAGIC --LEFT JOIN NaicsDescriptions AS t3 on t2.Code = t3.NaicsCode
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from RDM_NAICS_ESG 
# MAGIC where PartyIdentifier like 'T24%'
# MAGIC And NaicsCode is not null
# MAGIC limit 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from locationToRegionMapping limit 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct FIHubIndicator from hive_metastore.radar.RDM_Party

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.radar.RDM_Party limit 3

# COMMAND ----------

# DBTITLE 1,ALL CLIENTS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RDM_Client AS
# MAGIC
# MAGIC ((
# MAGIC select PartyIdentifier
# MAGIC , Party_type
# MAGIC ,  GlobalClientOwnerLocation
# MAGIC ,  BusinessLineName
# MAGIC , CASE 
# MAGIC     WHEN GlobalClientOwnerLocation = 'Utrecht' THEN 'Retail'
# MAGIC     ELSE `W&RORRetail` 
# MAGIC     END AS WR_OR_RETAIL
# MAGIC ,'empty' AS GlobalClientOwnerRegion
# MAGIC , CASE  
# MAGIC         WHEN (FIHubIndicator = true and (GlobalClientOwnerLocation IN  ('Rabobank China', 'Rabobank Hong Kong', 'Rabobank Singapore'))) THEN 'Asia FI'
# MAGIC         WHEN FIHubIndicator = true THEN 'FI'
# MAGIC         WHEN PartyIdentifier like '%LE_RF%' THEN 'Rabo Foundation'
# MAGIC         WHEN BusinessLineName = 'RANZ Country Banking' THEN 'RANZ' --'RANZ Country Banking'
# MAGIC         WHEN BusinessLineName = 'RANZ Rabo Online Savings (ROS)' THEN 'RANZ' --THEN 'RANZ Rabo Online Savings'
# MAGIC         ELSE COALESCE(t2.Region)--, t1.GlobalClientOwnerRegion) 
# MAGIC         END AS Region_Derived
# MAGIC , RefreshDate
# MAGIC , CASE 
# MAGIC     WHEN (GlobalClientOwnerLocation IN  ('Shanghai','China', 'Hong Kong', 'Singapore') AND LifeCycleStatus = 'Active') THEN 'Not a Client'
# MAGIC     ELSE LifeCycleStatus
# MAGIC     END AS LifeCycleStatus
# MAGIC         
# MAGIC from hive_metastore.radar.RDM_Party as t1
# MAGIC LEFT JOIN locationToRegionMapping as t2 on t1.GlobalClientOwnerLocation = t2.Location
# MAGIC )
# MAGIC --WHERE LifeCycleStatus = 'Client' )
# MAGIC )
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC (
# MAGIC Select
# MAGIC CONCAT('T24_', Customer_IDCIF_number) AS PartyIdentifier,
# MAGIC  'RANZ_T24' AS Party_type,
# MAGIC `Location` AS GlobalClientOwnerLocation,
# MAGIC BusinessLine AS BusinessLineName,
# MAGIC 'WR' AS WR_OR_RETAIL,
# MAGIC 'empty' AS GlobalClientOwnerRegion,
# MAGIC 'RANZ' AS Region_Derived, 
# MAGIC null as RefreshDate,
# MAGIC Customer_status AS LifeCycleStatus
# MAGIC from t24_cust
# MAGIC )
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC (
# MAGIC SELECT
# MAGIC     PartyIdentifier
# MAGIC ,  Party_type
# MAGIC ,  GlobalClientOwnerLocation
# MAGIC ,  BusinessLineName
# MAGIC ,  WR_OR_RETAIL
# MAGIC ,  GlobalClientOwnerRegion
# MAGIC , 'NA - RAF Input/Vendor Finance' AS Region_Derived
# MAGIC , RefreshDate
# MAGIC , LifeCycleStatus
# MAGIC from hive_metastore.FEC_ESG.RAF_Customers as t1
# MAGIC )
# MAGIC
# MAGIC /*
# MAGIC UNION
# MAGIC -- RANZ MDM based clients
# MAGIC (
# MAGIC select     
# MAGIC     CASE 
# MAGIC         WHEN t2.Global_Client_ID is not null THEN CONCAT('GCDS_',t2.Global_Client_ID)
# MAGIC         ELSE CONCAT('MDM_', t2.CONTRACT_ID)
# MAGIC         END AS PartyIdentifier
# MAGIC --, PARTY_ID
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
# MAGIC , REPLACE(Client_Status, 'Active Client','Client') AS LifeCycleStatus
# MAGIC FROM MDM AS t2
# MAGIC WHERE PARTY_ID = CONTRACT_ID
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC AND REL_TYPE_CD = 'PHOLD'
# MAGIC and ((CONCAT('GCDS_',t2.Global_Client_ID) not in (select distinct PartyIdentifier from hive_metastore.radar.RDM_Party )) OR (t2.Global_Client_ID is null))
# MAGIC */
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from RDM_Client

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from t24_cust limit 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.rdm_party
# MAGIC order by PartyIdentifier
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RDM_Client_MDM AS
# MAGIC select     
# MAGIC     CASE 
# MAGIC         WHEN Global_Client_ID is not null THEN CONCAT('GCDS_',Global_Client_ID)
# MAGIC         ELSE CONCAT('MDM_', CONTRACT_ID)
# MAGIC         END AS PartyIdentifier
# MAGIC --, PARTY_ID
# MAGIC , 'MDM' AS Party_Type
# MAGIC , 'RANZ' AS GlobalClientOwnerLocation
# MAGIC , CLIENT_BUSINESS_LINE AS BusinessLineName
# MAGIC , 'W&R' AS WR_OR_RETAIL
# MAGIC , 'RANZ' AS GlobalClientOwnerRegion
# MAGIC , CASE 
# MAGIC     WHEN RIGHT(CLIENT_BUSINESS_LINE,2) = 'CB' THEN 'RANZ' --'RANZ Country Banking'
# MAGIC     WHEN RIGHT(CLIENT_BUSINESS_LINE,2) = 'OS' THEN 'RANZ' --'RANZ Rabo Online Savings'
# MAGIC     END AS Region_Derived
# MAGIC , null as RefreshDate
# MAGIC , REPLACE(Client_Status, 'Active Client','Client') AS Client_Status
# MAGIC FROM MDM
# MAGIC WHERE PARTY_ID = CONTRACT_ID
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC AND REL_TYPE_CD = 'PHOLD'
# MAGIC and ((CONCAT('GCDS_',Global_Client_ID) not in (select distinct PartyIdentifier from hive_metastore.radar.RDM_Party )) OR (Global_Client_ID is null))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.rdm_party 
# MAGIC where LifeCycleStatus is null
# MAGIC --and GlobalClientOwnerRegion = 'RANZ'

# COMMAND ----------

# MAGIC %sql
# MAGIC select GlobalClientOwnerRegion
# MAGIC , count(distinct(PartyIdentifier))
# MAGIC from RDM_client
# MAGIC group by GlobalClientOwnerRegion

# COMMAND ----------

# MAGIC %sql
# MAGIC select LifeCycleStatus 
# MAGIC , count(distinct(PartyIdentifier))
# MAGIC from RDM_client
# MAGIC where Region_derived = 'RANZ'
# MAGIC group by LifeCycleStatus
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(Client_Business_line) from MDM

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from MDM where RIGHT(CLIENT_BUSINESS_LINE,2) = 'OS'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving final Tables
# MAGIC - create db if not exists
# MAGIC - create rdm_clients
# MAGIC - create rdm_naics_esg

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS FEC_ESG

# COMMAND ----------

spark.table('RDM_Client').write.mode('overwrite').option("mergeSchema", "true").saveAsTable('FEC_ESG.RDM_Client')

# COMMAND ----------

# DBTITLE 1,Addition to Naics table of Customers not currently matching any Naics
df_add_missing_clients_to_naics = spark.sql("""
        Select t1.PartyIdentifier
        , t1.PartyIdentifier AS LocalSystemId
        , 'Not-Available' AS LocalSystemName
        , '' AS NAICScode
        , '' AS ESG_RELATED_TOPIC
        , '' AS NaicsDescriptioin
        From RDM_Client as t1
        left anti join RDM_NAICS_ESG as T2 on t1.PartyIdentifier = t2.PartyIdentifier
        Where T1.LifeCycleStatus IN ('Client', 'Active', 'ACTIVE')
""")

# COMMAND ----------

# DBTITLE 1,Store RDM_Naics_ESG

(spark.table('RDM_NAICS_ESG')
 .union(df_add_missing_clients_to_naics)
 .write.mode('overwrite')
 .option("mergeSchema", "true")
 .saveAsTable('FEC_ESG.RDM_NAICS_ESG'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct ESG_RELATED_TOPIC from FEC_ESG.RDM_NAICS_ESG

# COMMAND ----------

# MAGIC %md
# MAGIC ### Further control queries / checks

# COMMAND ----------

# stopping here because lower items are only for other checks.
dbutils.notebook.exit('running only the relevant part of notebook')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FEC_ESG.RDM_Client 
# MAGIC where GlobalClientOwnerRegion <> 'RANZ'
# MAGIC
# MAGIC limit 20

# COMMAND ----------

# DBTITLE 1,Queries RDM client
# MAGIC %sql
# MAGIC DESCRIBE FEC_ESG.RDM_Client

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FEC_ESG.RDM_Client
# MAGIC WHERE Region_Derived in ('ASIA')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FEC_ESG.RDM_Client
# MAGIC WHERE Region_Derived is null
# MAGIC and Party_type <> 'Partnership'
# MAGIC and partyIdentifier like '%LE_RF%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct GlobalClientOwnerLocation
# MAGIC from FEC_ESG.RDM_Client
# MAGIC WHERE Region_Derived in ('Rural- RANZ')
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Region_Derived ,count(*)
# MAGIC from FEC_ESG.RDM_Client
# MAGIC GROUP BY Region_Derived

# COMMAND ----------


