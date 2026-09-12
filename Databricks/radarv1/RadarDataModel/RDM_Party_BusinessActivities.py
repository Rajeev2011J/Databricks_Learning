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
# MAGIC
# MAGIC
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

# COMMAND ----------

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

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
SARADAR = "saradar" + environment
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Defining the dataobjects' name
party_Naics_dataobject='Party_Naics'

# COMMAND ----------

# DBTITLE 1,Storage Account Authentication
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
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

# DBTITLE 1,Connect GCDS keystore
gcds_load_dts = 'LOAD_DTS=' +DateToday+ '*'
# print list of strings for loading spark dfs from GDP
gcds_df = pd.DataFrame({'definedDatasetname':[
'client_KeyStoreKey'
]})

# Create TempView for each loading table
for index, row in gcds_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=row.definedDatasetname)

# COMMAND ----------

temp='20241129'
load_date_part = f'EDL_LOAD_DTS={DateToday}*'
date_parameter = datetime.today().strftime('%Y%m%d')
print(date_parameter)

# COMMAND ----------

# DBTITLE 1,Load GCOB
# print list of strings for loading spark dfs from GDP

#GCOB CASESERVICE
load_df =[
  'party_business_activities'
  ,'party_case_client_details'
  ,'party_client'
]
# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')
#GRAM
load_df = [
    'Party_RiskModelInstanceQuestionAnswers'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')



# COMMAND ----------

# DBTITLE 1,Load GCOB Legacy
print('Legacy2')
# print list of strings for loading spark dfs from GDP
load_df = [
 'Legacy2_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

# COMMAND ----------

# DBTITLE 1,2.0a GIC-GCID
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCID_GIC AS
# MAGIC
# MAGIC select GCID, KeyStore_Value as GIC_id 
# MAGIC from client_KeyStoreKey 
# MAGIC Where Keystore_type = 'GIC'
# MAGIC AND Status =  'Active'
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,2.0c GCID - GCOB
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCID_GCOB AS
# MAGIC
# MAGIC WITH GCID_NCINCO AS (
# MAGIC select GCID, min(Keystore_value) AS NCINO_ID
# MAGIC from client_KeyStoreKey 
# MAGIC Where Keystore_type = 'NCINOID'
# MAGIC --And status = 'Active'
# MAGIC GROUP BY GCID
# MAGIC )
# MAGIC
# MAGIC , CTE_COMBI AS (
# MAGIC select 
# MAGIC 'Legacy2' AS SourceSystem
# MAGIC , GcobId AS UniqueGcobId
# MAGIC , SalesforceClientID_nCino
# MAGIC , t2.NCINO_ID
# MAGIC , t2.GCID AS GCIDviaNcino
# MAGIC , t2.GCID AS GCID
# MAGIC from Legacy2_case_client_details as t1
# MAGIC
# MAGIC LEFT JOIN GCID_NCINCO t2 on t1.SalesforceClientID_nCino = t2.NCINO_ID
# MAGIC where IsLatestApprovedVersionofclient = 'True'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select
# MAGIC  'GCOB' AS SourceSystem
# MAGIC , t1.uniqueGcobId
# MAGIC , t1.SalesforceClientID_nCino
# MAGIC , t2.NCINO_ID
# MAGIC , t2.GCID AS GCIDviaNcino
# MAGIC , COALESCE(t1.gcdsid, t2.GCID) AS GCID
# MAGIC from party_case_client_details as t1
# MAGIC LEFT JOIN GCID_NCINCO t2 on t1.SalesforceClientID_nCino = t2.NCINO_ID
# MAGIC where IsLatestApprovedVersionofclient = 'True'
# MAGIC
# MAGIC -- TODO: ROOM for manual union for NaturalPersons GCID-GCOBId's in NL
# MAGIC )
# MAGIC
# MAGIC -- select 1 gcobid per gcid
# MAGIC select * 
# MAGIC FROM CTE_COMBI 
# MAGIC where GCID is NOT null
# MAGIC
# MAGIC
# MAGIC

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

# MAGIC %md 
# MAGIC ### 2a. GCOB Selection

# COMMAND ----------

# DBTITLE 1,GCOB_CLIENT_NAICS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCOB_Client_NAICS AS 
# MAGIC (
# MAGIC select 
# MAGIC  t1.GCDSID AS GCID
# MAGIC , t1.UniqueGcobId AS LocalSystemId
# MAGIC , 'GCOB' AS LocalSystemName
# MAGIC --, t2.SourceClient
# MAGIC --, t3.SourceClient
# MAGIC , t3.NaicsCode
# MAGIC --, t3.NaicsName
# MAGIC , t2.BusinesslineName AS BusinessLine
# MAGIC , t2.GlobalClientOwnerLocation AS `Location`
# MAGIC , t2.IsLatestApprovedVersionofclient = 'True'
# MAGIC , '' AS `Region`
# MAGIC , t2.SalesforceClientID_nCino
# MAGIC
# MAGIC FROM party_client AS t1
# MAGIC LEFT JOIN party_case_client_details AS t2 on t1.uniquegcobid = t2.uniquegcobid
# MAGIC LEFT JOIN party_business_activities as t3 on t2.Sourceclient = t3.SourceClient
# MAGIC WHERE t2.IslatestApprovedVersionOfClient = 'True'
# MAGIC -- AND ... lifecyclestatus = 'Client'.
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. GCOB Legacy Selection

# COMMAND ----------

print('Legacy2')
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
 'Legacy2_case_client_details'
  ,'Legacy2_business_activities'
]})

for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=row.GDPname,path_prefix='Legacy2')

# COMMAND ----------

# DBTITLE 1,LEGACY2_CLIENT_NAICS
# MAGIC %sql
# MAGIC -- # RANZ and RAF should be in this scope
# MAGIC CREATE OR REPLACE TEMP VIEW Legacy2_Client_NAICS AS
# MAGIC select 
# MAGIC   '' AS GCID
# MAGIC   , t1.Gcobid AS LocalSystemId 
# MAGIC   , 'Legacy2' AS LocalSystemName
# MAGIC   , t2.NAICScode
# MAGIC   , t1.BusinessLineName AS BusinessLine
# MAGIC   , t1.GlobalclientownerLocation AS `Location`
# MAGIC   , '' AS `Region`
# MAGIC   , t1.SalesforceClientID_nCino -- to match this against GCID.
# MAGIC FROM Legacy2_case_client_details t1
# MAGIC LEFT JOIN Legacy2_business_activities t2 on t1.clientId = t2.clientId
# MAGIC WHERE t1.ClientlifecycleName = 'Client'
# MAGIC AND t1.ApprovedInGcob = false
# MAGIC AND t1.IsClient = true
# MAGIC --and t1.IslatestApprovedVersionOfClient = 'True'
# MAGIC AND NOT (t1.GlobalclientownerLocation = 'Rabobank - USA Rabo AgriFinance' AND t1.SalesforceClientID_nCino IS NULL)--raf client with null ncino id
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2c. GCDS selection

# COMMAND ----------

# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
    , 'client_Client'
    , 'client_PartyRole'
]

for item in gcds_tables:
  Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=item)



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCDS_Client_NAICS AS
# MAGIC select 
# MAGIC   t1.GCID
# MAGIC   ,t1.GCID AS LocalSystemId
# MAGIC   , 'GCDS' AS LocalSystemName
# MAGIC   ,t1.`Primary_NAICS` AS NaicsCode
# MAGIC   ,t1.`Global_CO-businessline_code` AS BusinessLine
# MAGIC   ,t1.`Global_CO-Location_code` AS `Location`
# MAGIC  , '' AS `Region`
# MAGIC FROM client_Client t1
# MAGIC LEFT JOIN client_PartyRole t2 on t1.GCID = t2.GCID
# MAGIC WHERE 1=1
# MAGIC -- ncino id exists for RAF clients
# MAGIC AND (t2.Party_role IN ('Client', 'Customer') AND t2.Life_cycle_status in ('Client', 'Active'))
# MAGIC
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

# MAGIC %md
# MAGIC ## 2e. GIC and KN1 data (SA)
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC skipping for now - Should be part of GCDS.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Connect KN1
# print list of strings for loading spark dfs from GDP
kn1_datepart = f'LOADED_DTS={DateToday}*'

load_df = pd.DataFrame({'GDPname':[
'ADRBB_RESPOSTAS_ESCOLHIDAS'
, 'ADRBB_QUESTOES'
, 'ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS'
, 'ADKYC_QUESTOES_COMPORT_CLIENTES'
, 'ADKYC_TIPOS_CONTRAPARTES'
, 'ADKYC_TP_CADASTRAIS'
, 'ADKYC_CONTRAPARTES_COMPL'
, 'ADKYC_CONTRAPARTES'
, 'ADRBB_COB_NAICS'
, 'ADRBB_CONTRAPARTES_GRAM'
#, 'ADKYC_SITUACIOS'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='KN1', Dataobject=row.GDPname)

# COMMAND ----------

# DBTITLE 1,Connect GIC
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
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname)

# COMMAND ----------

# DBTITLE 1,GIC based naics
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
# MAGIC
# MAGIC
# MAGIC select '' AS GCID
# MAGIC , t1.COD_INSTITUCIONAL AS LocalSystemId
# MAGIC , 'GIC' AS LocalSystemName
# MAGIC --, t1.seq_pessoa AS LocalSystemName
# MAGIC --, t1.nom_completo
# MAGIC --, t2.seq_naics
# MAGIC , t3.COD_NAICS as NAICSCode
# MAGIC , 'Brazil' AS BusinessLine
# MAGIC , 'Brazil' AS `Location`
# MAGIC , 'SA' AS `Region`
# MAGIC from pessoa as t1
# MAGIC left join Latest_pessoa_complemento as t2 on t1.seq_pessoa = t2.seq_pessoa
# MAGIC left join naics as t3 on t2.SEQ_NAICS = t3.SEQ_NAICS
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,KN1 based selection (just kn1)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW KN1_NAICS AS
# MAGIC
# MAGIC -- GET GIC customer
# MAGIC -- GET KN1 cases - with latest case
# MAGIC
# MAGIC
# MAGIC select 
# MAGIC '' AS GCID
# MAGIC ,t1.COD_INSTITUCIONAL AS LocalSystemId
# MAGIC , 'KN1' AS LocalSystemName
# MAGIC ,t4.CD_COB_NAICS AS Naicscode
# MAGIC , 'Brazil' AS BusinessLine
# MAGIC , 'Brazil' AS `Location`
# MAGIC , 'SA' AS `Region`
# MAGIC
# MAGIC from pessoa as t1 
# MAGIC LEFT JOIN adkyc_contrapartes_compl t12 on t1.COD_INSTITUCIONAL = t12.CD_CONTRAPARTE
# MAGIC LEFT JOIN ADKYC_CONTRAPARTES t2 on t12.ID_CONTRAPARTE = t2.ID_CONTRAPARTE 
# MAGIC LEFT JOIN ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS t3 
# MAGIC 			ON t2.ID_CONTRAPARTE = t3.ID_CONTRAPARTE
# MAGIC 			AND t3.ID_QUESTAO IN (12, 390)
# MAGIC LEFT JOIN ADRBB_COB_NAICS t4 on t3.CD_RESPOSTA = t4.CD_COB_NAICS
# MAGIC 			
# MAGIC WHERE t2.ID_CONTRAPARTE_RENOVACAO = -1

# COMMAND ----------

# DBTITLE 1,KN1-GRAM based selection
# MAGIC %sql
# MAGIC -- GET GIC
# MAGIC -- GET KN1 link gic-kn1
# MAGIC -- get latest kn1 case
# MAGIC -- get gram link
# MAGIC -- get rm q and a
# MAGIC CREATE OR REPLACE TEMP VIEW KN1_GRAM_NAICS AS
# MAGIC
# MAGIC select 
# MAGIC '' AS GCID
# MAGIC , t1.COD_INSTITUCIONAL AS LocalSystemId
# MAGIC , 'KN1-GRAM' AS LocalSystemName
# MAGIC --,t1.NOM_COMPLETO
# MAGIC --,t2.NM_CONTRAPARTE
# MAGIC --,t2.ID_CONTRAPARTE AS KN1_CaseID
# MAGIC --,t4.InstanceId AS GRAM_InstanceID
# MAGIC --,t4.QuestionId
# MAGIC --,t4.QuestionText
# MAGIC ,t4.AnswerValue AS Naicscode
# MAGIC --,t4.AnswerText
# MAGIC --,t4.QuestionCode
# MAGIC , 'Brazil' AS BusinessLine
# MAGIC , 'Brazil' AS `Location`
# MAGIC , 'SA' AS `Region`
# MAGIC
# MAGIC from pessoa as t1 
# MAGIC LEFT JOIN adkyc_contrapartes_compl t12 on t1.COD_INSTITUCIONAL = t12.CD_CONTRAPARTE
# MAGIC LEFT JOIN ADKYC_CONTRAPARTES t2 on t12.ID_CONTRAPARTE = t2.ID_CONTRAPARTE --TODO: are we joining a case id on GIC id here? does this make sense?
# MAGIC LEFT JOIN ADRBB_CONTRAPARTES_GRAM t3 ON t2.ID_CONTRAPARTE = t3.ID_CONTRAPARTE
# MAGIC LEFT JOIN Party_RiskModelInstanceQuestionAnswers t4 on t3.ID_GRAM_IDENTITY = t4.InstanceId 
# MAGIC
# MAGIC where t2.ID_CONTRAPARTE_RENOVACAO = -1
# MAGIC AND  YEAR(t2.DTHR_FIM) <= 2024
# MAGIC and t4.SourceSystemName = 'Brazil - KN1'
# MAGIC --and t4.QuestionText like '%(NAICS)%'
# MAGIC and (QuestionId in (557, 773, 397 ) --TODO: WARNING! Must use reference data up-to-date per riskmodel version to get this right.
# MAGIC OR QuestionCode = 'SEC-Q1' )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2f. North America - (NLSvf data for Input Finance Scope)

# COMMAND ----------

nlsdt = (datetime.now()-timedelta(1)).strftime('%Y-%m-%d')
nls_load_dts = f'EDL_LOAD_DTS={nlsdt}*'
nls_load_dts

# COMMAND ----------

# DBTITLE 1,NA - NLSvf
# load_df = pd.DataFrame({'GDPname':[
#    'nls_dbo_cif_detail'
#    ,'nls_dbo_cif'
#    , 'nls_dbo_cif_loan_relationship'
#    , 'nls_dbo_relationship_codes'
#    , 'nls_dbo_loanacct'
#    , 'nls_dbo_loan_status_codes'
# ]})

# #Create TempView for each loading table
# for index, row in load_df.iterrows():
#   Read_GDP_Defined_DataObjects(Source='NLSVF', Dataobject=row.GDPname)


# COMMAND ----------

# MAGIC %sql
# MAGIC /* 
# MAGIC sql
# MAGIC select 
# MAGIC t1.CIFNO
# MAGIC , t1.CURRENT_PAYOFF_BALANCE
# MAGIC , t1.STATUS_CODE_NO
# MAGIC , t1.Name
# MAGIC --, LEFT(t2.userdef01, 6) AS NaicsCode  
# MAGIC , CASE 
# MAGIC     WHEN substring(t2.userdef01, 5, 1) = '/' THEN LEFT(t2.userdef16, 6) 
# MAGIC     ELSE LEFT(t2.userdef01, 6)
# MAGIC     END AS NaicsCode 
# MAGIC , t2.userdef01
# MAGIC , t2.userdef02
# MAGIC , t2.userdef16
# MAGIC , t3.status_code
# MAGIC , t3.status_code_description
# MAGIC from nls_dbo_loanacct  as t1
# MAGIC left join nls_dbo_cif_detail t2 on t1.cifno = t2.cifno
# MAGIC left join nls_dbo_loan_status_codes t3 on t1.STATUS_CODE_NO = t3.STATUS_CODE_NO
# MAGIC where t1.closed_date is null
# MAGIC and t1.status_code_no  = 0
# MAGIC and current_payoff_balance > 0
# MAGIC limit 60
# MAGIC
# MAGIC */

# COMMAND ----------

# MAGIC %md 
# MAGIC sql
# MAGIC select 
# MAGIC t1.CIFNO
# MAGIC , t1.CURRENT_PAYOFF_BALANCE
# MAGIC , t1.STATUS_CODE_NO
# MAGIC , t1.Name
# MAGIC --, LEFT(t2.userdef01, 6) AS NaicsCode  
# MAGIC , CASE 
# MAGIC     WHEN substring(t2.userdef01, 5, 1) = '/' THEN LEFT(t2.userdef16, 6) 
# MAGIC     ELSE LEFT(t2.userdef01, 6)
# MAGIC     END AS NaicsCode 
# MAGIC , t2.userdef01
# MAGIC , t2.userdef02
# MAGIC , t2.userdef16
# MAGIC , t3.status_code
# MAGIC , t3.status_code_description
# MAGIC from nls_dbo_loanacct  as t1
# MAGIC left join nls_dbo_cif_detail t2 on t1.cifno = t2.cifno
# MAGIC left join nls_dbo_loan_status_codes t3 on t1.STATUS_CODE_NO = t3.STATUS_CODE_NO
# MAGIC where t1.closed_date is null
# MAGIC and t1.status_code_no  = 0
# MAGIC and current_payoff_balance > 0

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE OR REPLACE TEMP VIEW NLS_Client_NAICS AS
# MAGIC select 
# MAGIC '' AS GCID
# MAGIC , t1.CIFNO AS LocalSystemId
# MAGIC , 'NLSvf' AS LocalSystemName  
# MAGIC , CASE 
# MAGIC     WHEN substring(t2.userdef01, 5, 1) = '/' THEN LEFT(t2.userdef16, 6) 
# MAGIC     ELSE LEFT(t2.userdef01, 6)
# MAGIC     END AS NaicsCode 
# MAGIC , 'InputFinance' as BusinessLine
# MAGIC , 'Input Finance - North America' AS `Location`
# MAGIC , 'NA' AS `Region`
# MAGIC from nls_dbo_loanacct  as t1
# MAGIC left join nls_dbo_cif_detail t2 on t1.cifno = t2.cifno
# MAGIC left join nls_dbo_loan_status_codes t3 on t1.STATUS_CODE_NO = t3.STATUS_CODE_NO
# MAGIC where t1.closed_date is null
# MAGIC and t1.status_code_no  = 0
# MAGIC

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

# DBTITLE 1,Union all NAICS logic elements
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RDM_PARTY_NAICS AS
# MAGIC
# MAGIC -- GCOB
# MAGIC SELECT  
# MAGIC  --CASE 
# MAGIC  -- WHEN  (t1.GCID is not null AND t1.GCID <> '' ) THEN CONCAT('GCDS_', t1.GCID)
# MAGIC  -- WHEN  (t2.GCID is not null AND t2.GCID <> '' ) THEN CONCAT('GCDS_', t2.GCID)
# MAGIC  -- ELSE CONCAT(t1.LocalSystemName, '_', CAST(t1.LocalSystemId AS STRING))
# MAGIC  -- END AS  PartyIdentifier
# MAGIC   CONCAT(t1.LocalSystemName, '_', CAST(t1.LocalSystemId AS STRING)) as LocalSystemIdentifier
# MAGIC   ,  t1.GCID
# MAGIC   ,  t1.LocalSystemId 
# MAGIC   ,  t1.LocalSystemName
# MAGIC   ,  t1.NAICScode
# MAGIC   ,  t1.BusinessLine
# MAGIC   ,  t1.`Location`
# MAGIC   ,  t1.`Region` 
# MAGIC   FROM GCOB_Client_NAICS as t1
# MAGIC   LEFT JOIN GCID_GCOB t2 on t1.LocalSystemId = t2.UniqueGcobId and t2.SourceSystem = 'GCOB'
# MAGIC
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC -- LEGACY2
# MAGIC -- TODO: what of Legacy2-MDM? now just as Legacy2_.... What options to match against GCDS?
# MAGIC SELECT 
# MAGIC  --CASE 
# MAGIC  -- WHEN  (t1.GCID is not null AND t1.GCID <> '' ) THEN CONCAT('GCDS_', t1.GCID)
# MAGIC  -- WHEN  (t2.GCID is not null AND t2.GCID <> '' ) THEN CONCAT('GCDS_', t2.GCID)
# MAGIC  -- ELSE CONCAT(t1.LocalSystemName, '_', CAST(LocalSystemId AS STRING))
# MAGIC  -- END AS  PartyIdentifier
# MAGIC   CONCAT('LEGACY2_', CAST(LocalSystemId AS STRING)) as LocalSystemIdentifier
# MAGIC   ,  t1.GCID
# MAGIC   ,  t1.LocalSystemId -- 
# MAGIC   ,  t1.LocalSystemName
# MAGIC   ,  t1.NAICScode
# MAGIC   ,  t1.BusinessLine
# MAGIC   ,  t1.`Location`
# MAGIC   ,  t1.`Region` 
# MAGIC FROM Legacy2_Client_NAICS t1
# MAGIC LEFT JOIN GCID_GCOB t2 on t1.LocalSystemId = t2.UniqueGcobId and   t2.SourceSystem = 'Legacy2'
# MAGIC -- MUST DO BETTER to match a PartyId here.
# MAGIC
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC
# MAGIC -- GCDS
# MAGIC SELECT   
# MAGIC    CONCAT('GCDS_',GCID) AS LocalSystemIdentifier
# MAGIC   , GCID
# MAGIC   , LocalSystemId 
# MAGIC   , LocalSystemName
# MAGIC   , NAICScode
# MAGIC   , BusinessLine
# MAGIC   , `Location`
# MAGIC   , `Region`  FROM GCDS_Client_NAICS
# MAGIC
# MAGIC -- skipping NLS for now
# MAGIC /*
# MAGIC UNION
# MAGIC
# MAGIC -- NLS
# MAGIC SELECT   
# MAGIC   CONCAT('NLS_',CAST(CAST(LocalSystemId AS INT) AS STRING) AS PartyIdentifier
# MAGIC   ,  GCID
# MAGIC   , CAST(LocalSystemId AS INT)
# MAGIC   , LocalSystemName
# MAGIC   , NAICScode
# MAGIC   , BusinessLine
# MAGIC   , `Location`
# MAGIC   , `Region`  
# MAGIC FROM NLS_Client_NAICS
# MAGIC */
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC -- KN1GIC_id
# MAGIC SELECT   
# MAGIC --   CASE 
# MAGIC --  WHEN  (t1.GCID is not null AND t1.GCID <> '' ) THEN CONCAT('GCDS_', t1.GCID)
# MAGIC --  WHEN  (t2.GCID is not null AND t2.GCID <> '' ) THEN CONCAT('GCDS_', t2.GCID)
# MAGIC --  ELSE CONCAT('GIC_', CAST(LocalSystemId AS STRING))
# MAGIC --  END AS  PartyIdentifier
# MAGIC   CONCAT('GIC_', CAST(LocalSystemId AS STRING)) as LocalSystemIdentifier
# MAGIC   , t1.GCID
# MAGIC   , LocalSystemId 
# MAGIC   , LocalSystemName
# MAGIC   , NAICScode
# MAGIC   , BusinessLine
# MAGIC   , `Location`
# MAGIC   , `Region`  
# MAGIC   FROM KN1_NAICS t1
# MAGIC   LEFT JOIN GCID_GIC as t2 ON t1.LocalSystemId = t2.GIC_id
# MAGIC
# MAGIC   where Naicscode is not null
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC -- KN1-GRAM
# MAGIC SELECT     
# MAGIC --CASE 
# MAGIC --  WHEN  (t1.GCID is not null AND t1.GCID <> '' ) THEN CONCAT('GCDS_', t1.GCID)
# MAGIC --  WHEN  (t2.GCID is not null AND t2.GCID <> '' ) THEN CONCAT('GCDS_', t2.GCID)
# MAGIC --  ELSE CONCAT('GIC_', CAST(LocalSystemId AS STRING))
# MAGIC --  END AS  PartyIdentifier
# MAGIC   CONCAT('GIC_', CAST(LocalSystemId AS STRING)) as LocalSystemIdentifier
# MAGIC    , t1.GCID
# MAGIC   , LocalSystemId 
# MAGIC   , LocalSystemName
# MAGIC   , NAICScode
# MAGIC   , BusinessLine
# MAGIC   , `Location`
# MAGIC   , `Region`  FROM KN1_GRAM_NAICS  as t1
# MAGIC   LEFT JOIN GCID_GIC as t2 ON t1.LocalSystemId = t2.GIC_id
# MAGIC
# MAGIC   where Naicscode is not null
# MAGIC
# MAGIC --TODO; see if GIC and GCDS are thesame prim naics

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
df_party_Naics.createOrReplaceTempView('Naics')

# COMMAND ----------

# DBTITLE 1,Removing record where PartyIdentifier is null
# MAGIC %sql
# MAGIC Create or replace temporary view Party_Naics_Final As
# MAGIC select * from Naics where PartyIdentifier is not null

# COMMAND ----------

df_Party_Naics_Final=spark.table('Party_Naics_Final')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
save_to_saradar_storage_account(df_Party_Naics_Final, party_Naics_dataobject, radar_datamodel_version_number, environment)
