# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal:
# MAGIC To get all business Activities together for clients, as expressed in NAICS codes. 
# MAGIC To compare against a reference table.
# MAGIC
# MAGIC
# MAGIC #### Author
# MAGIC - Ruud.van.Laar@rabobank.com
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC - Prasad.Gadidala@rabobank.com
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
# MAGIC ### Output
# MAGIC filled in this:
# MAGIC
# MAGIC |     Topic |Region	   | Country |	Business line |
# MAGIC |----------|----------|----------|----------|
# MAGIC | clients	 | | |		
# MAGIC | clients per client type/ CDD type (listed/ non listed/ NPPC/ natural person/FI)	| | |		
# MAGIC | clients engaged in the following industries/ sectors/ NAICs | | |			
# MAGIC |----------|----------|----------|----------|			
# MAGIC |Agro chemicals (market access, production, distribution and use)	| | |		
# MAGIC |Fish industry (fishing, fishing related activities and processing)	| | |		
# MAGIC |Wood industry (log, timber,  purchasing of logging equipment  etc)	| | |		
# MAGIC |Weapon and defense industry (production, research, development, services, management, system integration, testing, distribution and maintenance of or selling)	| | |		
# MAGIC |Tobacco manufacturing	 | | |		
# MAGIC |Government agencies	| | |	 	
# MAGIC |Cattle breeding/dairy/any activity that must comply to animal welfare regulations	| | |	
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Prasad  Gadidala	 |09-Sep-2025 |13412721 | Added neew columns as partof V2 version NaicsDescription and PrimaryNaicsFlag and removed columns GCID,BusinessLine,Location,Region. 
# MAGIC | Abhishek Jaiswal	 |09-Oct-2025 |13843194 | Added neew column NAICSPercentage. 
# MAGIC | Abhishek Jaiswal	 |06-Nov-2025 |14152761 | Naics for NP party should be considered. 
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load

# COMMAND ----------

# DBTITLE 1,Import libraries
#import libraries
import os
import pandas as pd
from datetime import datetime, timedelta
import re


# COMMAND ----------

# DBTITLE 1,Set received translation list
#NaicsList
# Data From Data W&R_ESG_2025
NAICS_REF_TABLE = {111421: 'Nursery and Tree Production ',
111422: 'Floriculture Production ',
111910: 'Tobacco Farming',
111940: 'Hay Farming ',
112111: 'Beef Cattle Ranching and Farming ',
112112: 'Cattle Feedlots ',
112120: 'Dairy Cattle and Milk Production',
112130: 'Dual-Purpose Cattle Ranching and Farming ',
112210: 'Hog and Pig Farming ',
112330: 'Turkey Production',
112340: 'Poultry Hatcheries',
112410: 'Sheep Farming',
112420: 'Goat Farming',
112511: 'Finfish Farming and Fish Hatcheries ',
112512: 'Shellfish Farming ',
112519: 'Other Aquaculture ',
112930: 'Fur-Bearing Animal and Rabbit Production',
113210: 'Forest Nurseries and Gathering of Forest Products ',
113310: 'Logging ',
114111: 'Finfish Fishing ',
114112: 'Shellfish Fishing ',
114114: 'Freshwater fishing',
114119: 'Other Marine Fishing ',
114210: 'Hunting and Trapping',
115113: 'Crop Harvesting, Primarily by Machine ',
115115: 'Farm Labor Contractors and Crew Leaders ',
115116: 'Farm Management Services ',
115210: 'Support Activities for Animal Production',
211120: 'Crude Petroleum Extraction ',
211130: 'Natural Gas Extraction ',
212220: 'Gold Ore and Silver Ore Mining ',
212290: 'Other Metal Ore Mining ',
212322: 'Industrial Sand Mining ',
213111: 'Drilling Oil and Gas Wells',
221113: 'Nuclear Electric Power Generation ',
221210: 'Natural Gas Distribution ',
237210: 'Land Subdivision ',
311225: 'Fats and Oils Refining and Blending ',
311612: 'Meat Processed from Carcasses ',
311615: 'Poultry Processing ',
312230: 'Tobacco Manufacturing ',
321215: 'Engineered Wood Member Manufacturing ',
321912: 'Cut Stock, Resawing Lumber, and Planing ',
324110: 'Petroleum Refineries',
325110: 'Petrochemical Manufacturing',
325120: 'Industrial Gas Manufacturing',
325199: 'All Other Basic Organic Chemical Manufacturing ',
325220: 'Artificial and Synthetic Fibers and Filaments Manufacturing',
325312: 'Phosphatic Fertilizer Manufacturing ',
325314: 'Fertilizer (Mixing Only) Manufacturing ',
325998: 'All Other Miscellaneous Chemical Product and Preparation Manufacturing ',
327993: 'Mineral Wool Manufacturing ',
332111: 'Iron and Steel Forging ',
332312: 'Fabricated Structural Metal Manufacturing ',
332992: 'Small Arms Ammunition Manufacturing ',
332994: 'Small Arms, Ordnance, and Ordnance Accessories Manufacturing ',
332999: 'All Other Miscellaneous Fabricated Metal Product Manufacturing ',
333111: 'Farm Machinery and Equipment Manufacturing ',
333131: 'Mining Machinery and Equipment Manufacturing ',
333132: 'Oil and Gas Field Machinery and Equipment Manufacturing ',
334512: 'Automatic Environmental Control Manufacturing for Residential, Commercial, and Appliance Use ',
335991: 'Carbon and Graphite Product Manufacturing ',
336992: 'Military Armored Vehicle, Tank, and Tank Component Manufacturing ',
337212: 'Custom Architectural Woodwork and Millwork Manufacturing ',
423820: 'Farm and Garden Machinery and Equipment Merchant Wholesalers ',
424430: 'Dairy Product (except Dried or Canned) Merchant Wholesalers ',
424460: 'Fish and Seafood Merchant Wholesalers ',
424470: 'Meat and Meat Product Merchant Wholesalers ',
424510: 'Grain and Field Bean Merchant Wholesalers ',
424520: 'Livestock Merchant Wholesalers ',
424590: 'Other Farm Product Raw Material Merchant Wholesalers ',
424930: 'Flower, Nursery Stock, and Florists Supplies Merchant Wholesalers ',
424940: 'Tobacco Product and Electronic Cigarette Merchant Wholesalers ',
444180: 'Other Building Material Dealers ',
444240: 'Nursery, Garden Center, and Farm Supply Retailers ',
445240: 'Meat Retailers ',
459310: 'Florists ',
459991: 'Tobacco, Electronic Cigarette, and Other Smoking Supplies Retailers ',
459991: 'Tobacco, Electronic Cigarette, and Other Smoking Supplies Retailers ',
457210: 'Fuel Dealers ',
532412: 'Construction, Mining, and Forestry Machinery and Equipment Rental and Leasing ',
541320: 'Landscape Architectural Services',
541511: 'Custom Computer Programming Services ',
541620: 'Environmental Consulting Services',
561710: 'Exterminating and Pest Control Services',
562112: 'Hazardous Waste Collection ',
562211: 'Hazardous Waste Treatment and Disposal ',
562219: 'Other Nonhazardous Waste Treatment and Disposal ',
712190: 'Nature Parks and Other Similar Institutions',
813312: 'Environment, Conservation and Wildlife Organizations',
921190: 'Other General Government Support'}




# COMMAND ----------

# DBTITLE 1,Create REF NAICS table
df_sust = pd.DataFrame.from_dict(columns=['NaicsDescription'], data = NAICS_REF_TABLE, orient = 'Index')
df_sust.reset_index(inplace = True)
df_sust.columns = ['NaicsCode','NaicsDescription']
spark.createDataFrame(df_sust).createOrReplaceTempView('NAICS_REF_TABLE')

# COMMAND ----------

# DBTITLE 1,Environment Variables
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
environment=os.environ['ENV']
radar_datamodel_version_number=2
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
SARADAR = "saradar" + environment
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
RANZ_ReadStorage = os.environ['AU_GDP_Defined_Storage_Account']

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils file
from RadarUtils import *

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
party_Naics_dataobject='Party_Naics'

# COMMAND ----------

# DBTITLE 1,Storage Account Authentication
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(RANZ_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Loading Data
# MAGIC - a. GCOB
# MAGIC - b. Legacy
# MAGIC - c. GCDS
# MAGIC - d. (skip RANZ) - only via GCDS
# MAGIC - e. GIC/KN1
# MAGIC - f. NLSvf

# COMMAND ----------

DateToday = datetime.now().strftime('%Y%m%d')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.0 GCDS Keystore for reference of all Keys to GCDS
# MAGIC
# MAGIC Context: the goal will be to represent one unique key that matches the unique key in the party table for all the relevant parties in our scope. 
# MAGIC
# MAGIC We shall get:
# MAGIC - for each GIC id, 1 GCID
# MAGIC - for each Legacy2 id, 1 GCID
# MAGIC - for each GCOB id, 1 GCID
# MAGIC - for each MDM id, 1 GCID
# MAGIC - for NLS - not expecting any GCID.
# MAGIC
# MAGIC There is a chance that some source systems do not match to a GCID. Since this is 1 central logic to map all the keys, we will append those with their own sourcesystem-identifier combination to be sure

# COMMAND ----------

#temp='20241129'
load_dts = f"EDL_LOAD_DTS={Load_Date}*"
print (load_dts)
load_date_part = load_dts

# COMMAND ----------

# DBTITLE 1,Load GCOB Objects
# print list of strings for loading spark dfs from GDP

#GCOB CASESERVICE
load_df =[
  'party_business_activities'
  ,'party_case_client_details'
  ,'party_client'
]
# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService',Load_Date=Load_Date)
#GRAM
load_df = [
    'Party_RiskModelInstanceQuestionAnswers'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL',Load_Date=Load_Date)



# COMMAND ----------

# DBTITLE 1,Load GCOB Legacy2 Objects
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
 'Legacy2_case_client_details'
,'Legacy2_business_activities'
]})

for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=row.GDPname,path_prefix='Legacy2',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Load GCDS Objects
# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
    , 'client_Client'
    , 'client_PartyRole'
]

for item in gcds_tables:
  Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=item,Load_Date=Load_Date)



# COMMAND ----------

# DBTITLE 1,Load KN1 Objects
# print list of strings for loading spark dfs from GDP
kn1_datepart = f'LOADED_DTS={DateToday}*'

load_df = pd.DataFrame({'GDPname':[
'ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS'
, 'ADKYC_CONTRAPARTES_COMPL'
, 'ADKYC_CONTRAPARTES'
, 'ADRBB_COB_NAICS'
, 'ADRBB_CONTRAPARTES_GRAM'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='KN1', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Load GIC Objects
gic_load_dts = f'LOADED_DTS={DateToday}*'

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
    'pessoa'
    , 'pessoa_complemento'
    , 'naics'
]})
#gic_load_dts
# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

load_df = [
    'c_lkp_naics_xref'
,'c_b_party_xref'
,'c_b_party_naics_xref']

for Dataobject in load_df:
    Read_GDP_Defined_DataObjects_RANZ('RANZ-MDM' , Dataobject, Load_Date)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 2z some quality checks
# MAGIC The following to get a gcid with double active ncino id
# MAGIC
# MAGIC ```
# MAGIC %sql
# MAGIC select * from client_KeystoreKey
# MAGIC where gcid = 2716388
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,GCDS Client Logic
# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_Clients As
# MAGIC select distinct
# MAGIC k.KeyStore_value as identifier
# MAGIC ,k.KeyStore_type
# MAGIC ,k.status AS status
# MAGIC ,c.GCID
# MAGIC ,case when c.Party_Type = 'Natural Person' then c.NP_Primary_NAICS else c.Primary_NAICS end as NAICSCode
# MAGIC ,case when c.Party_Type = 'Natural Person' then c.NP_Primary_NAICS_Description else c.Primary_Naics_description end as NAICSDescription
# MAGIC ,case when c.Party_Type = 'Natural Person' then c.NP_Primary_NAICS_Percentage else c.Primary_NAICS_percentage end as NAICSPercentage
# MAGIC ,c.Party_Type
# MAGIC from client_KeyStoreKey k
# MAGIC inner join client_Client c on c.GCID = k.GCID

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 2a. GCOB Selection

# COMMAND ----------

# DBTITLE 1,GCOB Clients Naics
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCOB_Client_NAICS AS 
# MAGIC SELECT DISTINCT
# MAGIC   CONCAT('GCOB', '_', CAST(t1.UniqueGcobId AS STRING)) as LocalSystemIdentifier
# MAGIC ,  t1.UniqueGcobId AS LocalSystemId
# MAGIC , 'GCOB' AS LocalSystemName
# MAGIC ,  t3.NAICSCode
# MAGIC ,  t3.NaicsName as NAICSDescription
# MAGIC ,CASE
# MAGIC     WHEN t3.NaicsCode IS NULL OR gcds.NAICSCode IS NULL THEN NULL
# MAGIC     WHEN gcds.NAICSCode = t3.NaicsCode  AND gcds.identifier=t2.SalesforceClientID_nCino THEN 'True'
# MAGIC     ELSE 'False'
# MAGIC   END AS PrimaryNaicsFlag
# MAGIC ,gcds.NAICSPercentage
# MAGIC FROM party_client AS t1
# MAGIC LEFT JOIN party_case_client_details AS t2 on t1.uniquegcobid = t2.uniquegcobid
# MAGIC LEFT JOIN party_business_activities as t3 on t2.Sourceclient = t3.SourceClient
# MAGIC LEFT Join GCDS_Clients gcds on gcds.identifier=t2.SalesforceClientID_nCino 
# MAGIC WHERE t2.IslatestApprovedVersionOfClient = 'True' and t2.ClientType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person') AND  gcds.KeyStore_type='NCINOID'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC CONCAT('GCOB', '_', CAST(t1.UniqueGcobId AS STRING)) as LocalSystemIdentifier
# MAGIC , t1.UniqueGcobId AS LocalSystemId
# MAGIC , 'GCOB' AS LocalSystemName
# MAGIC , t3.NAICSCode
# MAGIC , t3.NaicsName as NAICSDescription 
# MAGIC ,CASE
# MAGIC     WHEN t3.NaicsCode IS NULL OR gcds.NAICSCode IS NULL THEN NULL
# MAGIC     WHEN gcds.NAICSCode = t3.NaicsCode  AND gcds.identifier=t2.gcobid THEN 'True'
# MAGIC     ELSE 'False'
# MAGIC   END AS PrimaryNaicsFlag
# MAGIC ,gcds.NAICSPercentage
# MAGIC FROM party_client AS t1
# MAGIC LEFT JOIN party_case_client_details AS t2 on t1.uniquegcobid = t2.uniquegcobid
# MAGIC LEFT JOIN party_business_activities as t3 on t2.Sourceclient = t3.SourceClient
# MAGIC LEFT Join GCDS_Clients gcds on gcds.identifier=t2.gcobid 
# MAGIC WHERE t2.IslatestApprovedVersionOfClient = 'True' and t2.ClientType in ('Legal Entity') AND KeyStore_type='GCOBID'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. GCOB Legacy Selection

# COMMAND ----------

# DBTITLE 1,GCOB_LEGACY2_CLIENT_NAICS
# MAGIC %sql
# MAGIC -- # RANZ and RAF should be in this scope
# MAGIC CREATE OR REPLACE TEMP VIEW Legacy2_Client_NAICS AS
# MAGIC select 
# MAGIC    CONCAT('LEGACY2_', CAST(t1.Gcobid AS STRING)) as LocalSystemIdentifier
# MAGIC   ,t1.Gcobid AS LocalSystemId 
# MAGIC   , 'Legacy2' AS LocalSystemName
# MAGIC   , t2.NAICSCode
# MAGIC   , t2.NaicsName AS NAICSDescription
# MAGIC   ,CASE WHEN t2.NaicsCode IS NULL OR gcds.NAICSCode IS NULL THEN NULL
# MAGIC   WHEN gcds.NaicsCode = t2.NaicsCode AND 
# MAGIC   gcds.Identifier=t1.SalesforceClientID_nCino  THEN 'True'ELSE 'False'
# MAGIC   END AS PrimaryNaicsFlag
# MAGIC ,gcds.NAICSPercentage
# MAGIC FROM Legacy2_case_client_details t1
# MAGIC LEFT JOIN Legacy2_business_activities t2 on t1.clientId = t2.clientId
# MAGIC LEft join GCds_Clients gcds on gcds.Identifier=t1.SalesforceClientID_nCino 
# MAGIC WHERE t1.ClientlifecycleName = 'Client' AND  gcds.KeyStore_type='NCINOID'
# MAGIC AND t1.ApprovedInGcob = false
# MAGIC AND t1.IsClient = true
# MAGIC AND NOT (t1.GlobalclientownerLocation = 'Rabobank - USA Rabo AgriFinance' AND t1.SalesforceClientID_nCino IS NULL)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2c. GCDS selection

# COMMAND ----------

# DBTITLE 1,GCDS_Clients_Naics
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCDS_Client_NAICS AS
# MAGIC select DISTINCT
# MAGIC    CONCAT('GCDS_',t1.GCID) AS LocalSystemIdentifier
# MAGIC   ,t1.GCID AS LocalSystemId
# MAGIC   , 'GCDS' AS LocalSystemName
# MAGIC   ,t1.NAICSCode
# MAGIC   ,t1.NAICSDescription
# MAGIC   ,CASE
# MAGIC     WHEN t1.NAICSCode IS NULL  THEN NULL
# MAGIC     WHEN t1.NAICSDescription IS NOT NULL THEN 'True'
# MAGIC     ELSE 'False'
# MAGIC END AS PrimaryNaicsFlag
# MAGIC ,t1.NAICSPercentage
# MAGIC FROM GCDS_Clients t1
# MAGIC LEFT JOIN client_PartyRole t2 on t1.GCID = t2.GCID
# MAGIC WHERE 1=1
# MAGIC AND (t2.Party_role IN ('Client', 'Customer') AND t2.Life_cycle_status in ('Client', 'Active'))
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2d. MDM (RANZ)
# MAGIC Not possible from GDP yet. must be separate

# COMMAND ----------

# DBTITLE 1,?? MDM RANZ data
# MAGIC %md
# MAGIC MDM_Files = dbutils.fs.ls(f"abfss://ranz-mdm@{SALZReadStorage}.dfs.core.windows.net/")
# MAGIC
# MAGIC # Loop through the list and print the name of each file
# MAGIC
# MAGIC CurrentYearMonth = datetime.today().strftime('%Y%m')
# MAGIC list_count = 0
# MAGIC
# MAGIC for file_info in MDM_Files:
# MAGIC
# MAGIC     FileName = file_info.name
# MAGIC     split_list = re.split(r"[_,.]", FileName)
# MAGIC     file_recieval_date = split_list[2]
# MAGIC
# MAGIC     if file_recieval_date[:6] == CurrentYearMonth:
# MAGIC         break
# MAGIC
# MAGIC     list_count += 1
# MAGIC
# MAGIC from pyspark.sql.functions import to_date, lit
# MAGIC
# MAGIC latest_mdm_df = spark.read \
# MAGIC     .format('csv') \
# MAGIC     .option('header','true') \
# MAGIC     .option('inferSchema', 'true') \
# MAGIC     .load(MDM_Files[list_count].path) \
# MAGIC     .withColumn('FILE_RECIEVAL_DATE', to_date(lit(file_recieval_date), 'yyyyMMdd')) \
# MAGIC     .createOrReplaceTempView('MDM')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or Replace Temporary view RANZ_Naics as
# MAGIC   select distinct 
# MAGIC CONCAT('RANZ_',cbp.PKEY_SRC_OBJECT) as LocalSystemIdentifier,
# MAGIC null PrimaryNaicsFlag,
# MAGIC cpn.NAICS_EXP_PCT as NAICSPercentage,
# MAGIC cln.NAICS_DESC as NAICSDescription,
# MAGIC cln.NAICS_CODE as NAICSCode,
# MAGIC 'MDM-RANZ' as LocalSystemName,
# MAGIC cpn.ROWID_OBJECT as LocalSystemId
# MAGIC from c_b_party_xref cbp
# MAGIC left join c_b_party_naics_xref cpn 
# MAGIC   on cpn.FK_PARTY_ID = cbp.ROWID_XREF
# MAGIC left join c_lkp_naics_xref cln 
# MAGIC   on cln.NAICS_CODE=cpn.NAICS_CD

# COMMAND ----------

# MAGIC %md
# MAGIC %sql
# MAGIC -- MDM selection for NAICS
# MAGIC select Contract_ID
# MAGIC , PARTY_ID
# MAGIC , Client_Status
# MAGIC , CLIENT_BUSINESS_LINE
# MAGIC , GLOBAL_CLIENT_ID
# MAGIC , REL_TYPE_CD
# MAGIC FROM MDM
# MAGIC WHERE PARTY_ID = CONTRACT_ID
# MAGIC AND CLIENT_STATUS = 'Active Client'
# MAGIC AND REL_TYPE_CD = 'PHOLD'
# MAGIC LIMIT 3
# MAGIC -- question? Where are naics compared to other MDM file in OneLab that does appear to have NAICS?

# COMMAND ----------

# DBTITLE 1,GIC_Naics
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GIC_NAICS AS
# MAGIC
# MAGIC WITH Latest_pessoa_complemento AS (
# MAGIC   select t1.* 
# MAGIC   from PESSOA_complemento t1 
# MAGIC   INNER JOIN 
# MAGIC   (select max(SEQ_HISTORICO) as MAX_SEQ_HISTORICO,SEQ_PESSOA from PESSOA_complemento group by SEQ_PESSOA) as t2 
# MAGIC     on t1.seq_pessoa = t2.SEQ_PESSOA and t1.Seq_historico = t2.MAX_SEQ_HISTORICO
# MAGIC )
# MAGIC select 
# MAGIC CONCAT('GIC_', CAST(t1.COD_INSTITUCIONAL AS STRING)) as LocalSystemIdentifier
# MAGIC ,  t1.COD_INSTITUCIONAL AS LocalSystemId
# MAGIC , 'GIC' AS LocalSystemName
# MAGIC , t3.COD_NAICS as NAICSCode
# MAGIC , t3.DES_NAICS as NAICSDescription
# MAGIC , CASE
# MAGIC     WHEN t3.COD_NAICS IS NULL OR gcds.NAICSCode IS NULL THEN NULL
# MAGIC     WHEN gcds.NAICSCode = t3.COD_NAICS AND gcds.Identifier=t1.COD_INSTITUCIONAL THEN 'True'
# MAGIC     ELSE 'False'
# MAGIC   END AS PrimaryNaicsFlag
# MAGIC ,gcds.NAICSPercentage
# MAGIC from pessoa as t1
# MAGIC left join Latest_pessoa_complemento as t2 on t1.seq_pessoa = t2.seq_pessoa
# MAGIC left join naics as t3 on t2.SEQ_NAICS = t3.SEQ_NAICS
# MAGIC Left join GCDS_Clients as gcds on gcds.Identifier=t1.COD_INSTITUCIONAL
# MAGIC and gcds.KeyStore_type = 'GIC'
# MAGIC

# COMMAND ----------

# DBTITLE 1,KN1_Naics only
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW KN1_NAICS AS
# MAGIC -- GET GIC customer
# MAGIC -- GET KN1 cases - with latest case
# MAGIC
# MAGIC select 
# MAGIC CONCAT('GIC_', CAST(t1.COD_INSTITUCIONAL AS STRING)) as LocalSystemIdentifier
# MAGIC , t1.COD_INSTITUCIONAL AS LocalSystemId
# MAGIC ,'KN1' AS LocalSystemName
# MAGIC ,t4.CD_COB_NAICS AS Naicscode
# MAGIC ,t4.DE_COB_NAICS AS NAICSDescription
# MAGIC ,CASE
# MAGIC     WHEN t4.CD_COB_NAICS IS NULL OR gcds.NAICSCode IS NULL THEN NULL
# MAGIC     WHEN gcds.NAICSCode = t4.CD_COB_NAICS  AND gcds.Identifier=t1.COD_INSTITUCIONAL THEN 'True'
# MAGIC     ELSE 'False'
# MAGIC   END AS PrimaryNaicsFlag
# MAGIC ,gcds.NAICSPercentage
# MAGIC from pessoa as t1 
# MAGIC LEFT JOIN adkyc_contrapartes_compl t12 on t1.COD_INSTITUCIONAL = t12.CD_CONTRAPARTE
# MAGIC LEFT JOIN ADKYC_CONTRAPARTES t2 on t12.ID_CONTRAPARTE = t2.ID_CONTRAPARTE 
# MAGIC LEFT JOIN ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS t3 
# MAGIC 			ON t2.ID_CONTRAPARTE = t3.ID_CONTRAPARTE
# MAGIC 			AND t3.ID_QUESTAO IN (12, 390)
# MAGIC LEFT JOIN ADRBB_COB_NAICS t4 on t3.CD_RESPOSTA = t4.CD_COB_NAICS
# MAGIC Left join GCds_Clients gcds on gcds.Identifier=t1.COD_INSTITUCIONAL
# MAGIC and gcds.KeyStore_type = 'GIC'
# MAGIC WHERE t2.ID_CONTRAPARTE_RENOVACAO = -1

# COMMAND ----------

# DBTITLE 1,KN1_GRAM Code
# MAGIC %sql
# MAGIC -- GET GIC
# MAGIC -- GET KN1 link gic-kn1
# MAGIC -- get latest kn1 case
# MAGIC -- get gram link
# MAGIC -- get rm q and a
# MAGIC CREATE OR REPLACE TEMP VIEW KN1_GRAM_NAICS AS
# MAGIC
# MAGIC select 
# MAGIC  CONCAT('GIC_', CAST(t1.COD_INSTITUCIONAL AS STRING)) as LocalSystemIdentifier
# MAGIC , t1.COD_INSTITUCIONAL AS LocalSystemId
# MAGIC , 'KN1-GRAM' AS LocalSystemName
# MAGIC ,  t4.AnswerValue AS Naicscode
# MAGIC ,  t4.AnswerText as NaicsDescription
# MAGIC ,  CASE
# MAGIC     WHEN t4.AnswerValue IS NULL OR gcds.NAICSCode IS NULL THEN NULL
# MAGIC     WHEN gcds.NAICSCode = t4.AnswerValue AND  gcds.Identifier=t1.COD_INSTITUCIONAL THEN 'True'
# MAGIC     ELSE 'False'
# MAGIC   END AS PrimaryNaicsFlag
# MAGIC ,gcds.NAICSPercentage
# MAGIC from pessoa as t1 
# MAGIC LEFT JOIN adkyc_contrapartes_compl t12 on t1.COD_INSTITUCIONAL = t12.CD_CONTRAPARTE
# MAGIC LEFT JOIN ADKYC_CONTRAPARTES t2 on t12.ID_CONTRAPARTE = t2.ID_CONTRAPARTE --TODO: are we joining a case id on GIC id here? does this make sense?
# MAGIC LEFT JOIN ADRBB_CONTRAPARTES_GRAM t3 ON t2.ID_CONTRAPARTE = t3.ID_CONTRAPARTE
# MAGIC LEFT JOIN Party_RiskModelInstanceQuestionAnswers t4 on t3.ID_GRAM_IDENTITY = t4.InstanceId 
# MAGIC Left join GCds_Clients gcds on gcds.Identifier=t1.COD_INSTITUCIONAL
# MAGIC and gcds.KeyStore_type = 'GIC'
# MAGIC where t2.ID_CONTRAPARTE_RENOVACAO = -1
# MAGIC AND  YEAR(t2.DTHR_FIM) <= 2024
# MAGIC and t4.SourceSystemName = 'Brazil - KN1'
# MAGIC --and t4.QuestionText like '%(NAICS)%'
# MAGIC and (QuestionId in (557, 773, 397 ) --TODO: WARNING! Must use reference data up-to-date per riskmodel version to get this right.
# MAGIC OR QuestionCode = 'SEC-Q1' )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RDM_PARTY_NAICS AS
# MAGIC Select (*)from GCOB_Client_NAICS
# MAGIC UNION
# MAGIC select (*) from Legacy2_Client_NAICS
# MAGIC UNION
# MAGIC select (*) from GCDS_Client_NAICS
# MAGIC UNION
# MAGIC select (*) from GIC_NAICS
# MAGIC UNION
# MAGIC select (*)from KN1_NAICS
# MAGIC UNION
# MAGIC select (*) from KN1_GRAM_NAICS
# MAGIC UNION
# MAGIC select * from RANZ_Naics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. pull everything together
# MAGIC
# MAGIC For each element, data is available as temp views:
# MAGIC - NAICS_REF_TABLE
# MAGIC - 2a: GCOB_Client_NAICS
# MAGIC - 2b": Legacy2_Client_NAICS
# MAGIC - 2c: GCDS_Client_NAICS
# MAGIC - 2d: 
# MAGIC - 2e: gic/kn1
# MAGIC - 2e- kn1 old
# MAGIC - 2e kn1 GRAM
# MAGIC - 2f: NLS_Client_NAICS

# COMMAND ----------

df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/1/data/{load_date_part}/*.parquet"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Storing in Storage account AS EXTERNAL table and interface table

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_Naics=spark.table('RDM_PARTY_NAICS')

# COMMAND ----------

df_party_Naics = add_party_identifier(df_party_Naics, df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 1,Read DF to table
df_party_Naics.createOrReplaceTempView('Naicsfinal')

# COMMAND ----------

# DBTITLE 1,Removing record where PartyIdentifier is null
# MAGIC %sql
# MAGIC Create or replace temporary view Party_Naics_Final As
# MAGIC select distinct
# MAGIC PartyIdentifier
# MAGIC ,LocalSystemId
# MAGIC ,LocalSystemName
# MAGIC ,NAICSCode
# MAGIC ,NAICSDescription
# MAGIC ,PrimaryNaicsFlag
# MAGIC ,NAICSPercentage
# MAGIC from Naicsfinal 
# MAGIC where PartyIdentifier is not null and NAICSCode is not null

# COMMAND ----------

df_Party_Naics_Final=spark.table('Party_Naics_Final')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
   save_to_saradar_storage_account(df_Party_Naics_Final, party_Naics_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_Party_Naics_Final, party_Naics_dataobject, radar_datamodel_version_number, environment)
