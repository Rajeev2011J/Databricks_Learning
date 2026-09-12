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
# MAGIC |Rhea Gupta | 30-Mar-2026 |15580816 | Included RANZ in version 3
# MAGIC | Mahalakshmi V | 20-May-2026 | 16071492 | Included RANZ V4
# MAGIC | Abhishek Jaiswal	 |21-June-2026 |16311861 | Restructure notebook to follow RADAR way of design
# MAGIC | Rhea Gupta	 |06-Aug-2026 |16705384 | Reading GCDS v4801, Added version parameter in Radarutils- Read_GDP_defined
# MAGIC | Gopi Prasad K | 11-Aug-2026 | 16107896 | Party type changes for Legacy2 Source system | 3

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
radar_datamodel_version_number=3
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

# DBTITLE 1,Defining Dataversion
#Dataversion has been added as a Parameter to Read_GDP_Defined_DataObjects function in Radar Utils. Only for v4801, we want to pass a Dataversion. v4801 starts on 20260623

Dataversion = 4801 if Load_Date >= '20260623' else None

# COMMAND ----------

# DBTITLE 1,Storage Account Authentication
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(RANZ_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

DateToday = datetime.now().strftime('%Y%m%d')

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
  Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=item,Load_Date=Load_Date, Dataversion= Dataversion)

#Check for 2-3 different load dates. 23rd july before etc


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
load_ranz_v4_rdm_tables(load_df, Load_Date)

# COMMAND ----------

# DBTITLE 1,GCDS Client Logic Old
#This is the gcds logic for versions before v4801

gcds_sql_old= """
Create or replace temporary view GCDS_Clients As
select distinct
k.KeyStore_value as identifier
,k.KeyStore_type
,k.status AS status
,c.GCID
,'Primary' as NAICSLevel
,case when c.Party_Type = 'Natural Person' then c.NP_Primary_NAICS else c.Primary_NAICS end as NAICSCode
,case when c.Party_Type = 'Natural Person' then c.NP_Primary_NAICS_Description else c.Primary_Naics_description end as NAICSDescription
,case when c.Party_Type = 'Natural Person' then c.NP_Primary_NAICS_Percentage else c.Primary_NAICS_percentage end as NAICSPercentage
,c.Party_Type
from client_KeyStoreKey k
inner join client_Client c on c.GCID = k.GCID """

# COMMAND ----------

#This is the new gcds logic for v4801

gcds_sql_new = """CREATE OR REPLACE TEMPORARY VIEW GCDS_Clients AS

SELECT DISTINCT
    k.KeyStore_Value AS identifier,
    k.KeyStore_Type,
    k.Status AS status,
    c.GCID,
    c.Party_Type,
    NAICSLevel,
    NAICSCode,
    NAICSDescription,
    NAICSPercentage

FROM client_KeyStoreKey k
INNER JOIN client_Client c
    ON c.GCID = k.GCID

LATERAL VIEW STACK(
    5,

    'Primary',
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Primary_NAICS
        ELSE c.Primary_NAICS END,
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Primary_NAICS_Description
        ELSE c.Primary_NAICS_Description END,
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Primary_NAICS_Percentage
        ELSE c.Primary_NAICS_Percentage END,

    'Second',
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Second_NAICS
        ELSE c.Second_NAICS END,
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Second_NAICS_Description
        ELSE c.Second_NAICS_Description END,
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Second_NAICS_Percentage
        ELSE c.Second_NAICS_Percentage END,

    'Third',
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Third_NAICS
        ELSE c.Third_NAICS END,
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Third_NAICS_Description
        ELSE c.Third_NAICS_Description END,
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Third_NAICS_Percentage
        ELSE c.Third_NAICS_Percentage END,

    'Fourth',
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Fourth_NAICS
        ELSE c.Fourth_NAICS END,
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Fourth_NAICS_Description
        ELSE c.Fourth_NAICS_Description END,
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Fourth_NAICS_Percentage
        ELSE c.Fourth_NAICS_Percentage END,

    'Fifth',
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Fifth_NAICS
        ELSE c.Fifth_NAICS END,
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Fifth_NAICS_Description
        ELSE c.Fifth_NAICS_Description END,
    CASE WHEN c.Party_Type = 'Natural Person'
        THEN c.NP_Fifth_NAICS_Percentage
        ELSE c.Fifth_NAICS_Percentage END

) s AS NAICSLevel, NAICSCode, NAICSDescription, NAICSPercentage

WHERE NOT (
    NAICSCode IS NULL
    AND NAICSLevel IN ('Second', 'Third', 'Fourth', 'Fifth')
) """

# COMMAND ----------

#This will run one of the queries abive. Dataversion is set to None or 4801 depending on the Load_Date. If it is None, then the older query is run. Else, the new query with 5 NAICS codes. 

if Dataversion is None:
    spark.sql(gcds_sql_old)
else:
    spark.sql(gcds_sql_new)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 2a. GCOB Selection

# COMMAND ----------

# DBTITLE 1,GCOB Clients Naics
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCOB_Client_NAICS AS 
# MAGIC SELECT DISTINCT
# MAGIC CONCAT('GCOB_', CAST(t1.UniqueGcobId AS STRING)) as LocalSystemIdentifier
# MAGIC ,t1.UniqueGcobId AS LocalSystemId
# MAGIC ,'GCOB' AS LocalSystemName
# MAGIC ,t3.NAICSCode
# MAGIC ,t3.NaicsName as NAICSDescription
# MAGIC ,t2.GcobId
# MAGIC ,case when t2.ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when t2.ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC FROM party_client AS t1
# MAGIC LEFT JOIN party_case_client_details AS t2 on t1.uniquegcobid = t2.uniquegcobid
# MAGIC LEFT JOIN party_business_activities as t3 on t2.Sourceclient = t3.SourceClient
# MAGIC WHERE t2.IslatestApprovedVersionOfClient = 'True'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. GCOB Legacy Selection

# COMMAND ----------

# DBTITLE 1,GCOB_LEGACY2_CLIENT_NAICS
# MAGIC %sql
# MAGIC -- # RANZ and RAF should be in this scope
# MAGIC CREATE OR REPLACE TEMP VIEW Legacy2_Client_NAICS AS
# MAGIC select 
# MAGIC CASE
# MAGIC   WHEN t1.IsClient = 'true' and  t1.ClientTypeId = 1 THEN concat('LEC_', GcobId)
# MAGIC   WHEN t1.IsClient = 'true' and t1.ClientTypeId in (2,3) THEN concat('NP_NPPC_', GcobId)
# MAGIC   WHEN t1.IsClient = 'false' and t1.ClientTypeId = 1 THEN concat('RLEP_', GcobId)
# MAGIC   WHEN t1.IsClient = 'false' and t1.ClientTypeId in (2,3) THEN concat('RNPP_', GcobId)
# MAGIC END as Legacy2_Identifier,
# MAGIC concat('LEGACY2_',Legacy2_Identifier) as LocalSystemIdentifier
# MAGIC ,t1.Gcobid AS LocalSystemId 
# MAGIC ,'Legacy2' AS LocalSystemName
# MAGIC ,t2.NAICSCode
# MAGIC ,t2.NaicsName AS NAICSDescription
# MAGIC ,t1.SalesforceClientID_nCino
# MAGIC ,case when t1.ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when t1.ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when t1.ClientType = 'Related Legal Entity' then 'Related Legal Entity' 
# MAGIC       when t1.ClientType = 'Related Natural Person' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC FROM Legacy2_case_client_details t1
# MAGIC LEFT JOIN Legacy2_business_activities t2 on t1.clientId = t2.clientId
# MAGIC WHERE t1.ClientlifecycleName = 'Client'
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
# MAGIC ,k.KeyStore_value as identifier
# MAGIC FROM GCDS_Clients t1
# MAGIC LEFT JOIN client_PartyRole t2 on t1.GCID = t2.GCID
# MAGIC LEFT JOIN client_KeyStoreKey k on t1.GCID = t2.GCID
# MAGIC WHERE 1=1
# MAGIC AND (t2.Party_role IN ('Client', 'Customer') AND t2.Life_cycle_status in ('Client', 'Active'))
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or Replace Temporary view RANZ_Naics as
# MAGIC   select distinct 
# MAGIC CONCAT('RANZ_',cbp.PKEY_SRC_OBJECT) as LocalSystemIdentifier,
# MAGIC cpn.ROWID_OBJECT as LocalSystemId,
# MAGIC 'MDM-RANZ' as LocalSystemName,
# MAGIC cln.NAICS_CODE as NAICSCode,
# MAGIC cln.NAICS_DESC as NAICSDescription,
# MAGIC null PrimaryNaicsFlag,
# MAGIC cpn.NAICS_EXP_PCT as NAICSPercentage
# MAGIC ,cbp.SRC_PARTY_ID
# MAGIC from c_b_party cbp
# MAGIC left join c_b_party_naics cpn 
# MAGIC   on cpn.FK_PARTY_ID = cbp.ROWID_XREF
# MAGIC left join c_lkp_naics cln 
# MAGIC   on cln.NAICS_CODE=cpn.NAICS_CD

# COMMAND ----------

# DBTITLE 1,GIC_Naics
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GIC_NAICS AS
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
# MAGIC from pessoa as t1
# MAGIC left join Latest_pessoa_complemento as t2 on t1.seq_pessoa = t2.seq_pessoa
# MAGIC left join naics as t3 on t2.SEQ_NAICS = t3.SEQ_NAICS

# COMMAND ----------

# DBTITLE 1,KN1_Naics only
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW KN1_NAICS AS
# MAGIC -- GET GIC customer
# MAGIC -- GET KN1 cases - with latest case
# MAGIC select 
# MAGIC CONCAT('GIC_', CAST(t1.COD_INSTITUCIONAL AS STRING)) as LocalSystemIdentifier
# MAGIC , t1.COD_INSTITUCIONAL AS LocalSystemId
# MAGIC ,'KN1' AS LocalSystemName
# MAGIC ,t4.CD_COB_NAICS AS Naicscode
# MAGIC ,t4.DE_COB_NAICS AS NAICSDescription
# MAGIC from pessoa as t1 
# MAGIC LEFT JOIN adkyc_contrapartes_compl t12 on t1.COD_INSTITUCIONAL = t12.CD_CONTRAPARTE
# MAGIC LEFT JOIN ADKYC_CONTRAPARTES t2 on t12.ID_CONTRAPARTE = t2.ID_CONTRAPARTE 
# MAGIC LEFT JOIN ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS t3 
# MAGIC 			ON t2.ID_CONTRAPARTE = t3.ID_CONTRAPARTE
# MAGIC 			AND t3.ID_QUESTAO IN (12, 390)
# MAGIC LEFT JOIN ADRBB_COB_NAICS t4 on t3.CD_RESPOSTA = t4.CD_COB_NAICS
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
# MAGIC from pessoa as t1 
# MAGIC LEFT JOIN adkyc_contrapartes_compl t12 on t1.COD_INSTITUCIONAL = t12.CD_CONTRAPARTE
# MAGIC LEFT JOIN ADKYC_CONTRAPARTES t2 on t12.ID_CONTRAPARTE = t2.ID_CONTRAPARTE --TODO: are we joining a case id on GIC id here? does this make sense?
# MAGIC LEFT JOIN ADRBB_CONTRAPARTES_GRAM t3 ON t2.ID_CONTRAPARTE = t3.ID_CONTRAPARTE
# MAGIC LEFT JOIN Party_RiskModelInstanceQuestionAnswers t4 on t3.ID_GRAM_IDENTITY = t4.InstanceId 
# MAGIC where t2.ID_CONTRAPARTE_RENOVACAO = -1
# MAGIC AND  YEAR(t2.DTHR_FIM) <= 2024
# MAGIC and t4.SourceSystemName = 'Brazil - KN1'
# MAGIC --and t4.QuestionText like '%(NAICS)%'
# MAGIC and (QuestionId in (557, 773, 397 ) --TODO: WARNING! Must use reference data up-to-date per riskmodel version to get this right.
# MAGIC OR QuestionCode = 'SEC-Q1' )

# COMMAND ----------

# DBTITLE 1,All GIC-KN1-GRAM combine
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GIC_KN1_GRAM_NAICS AS
# MAGIC select (*) from GIC_NAICS
# MAGIC UNION
# MAGIC select (*)from KN1_NAICS
# MAGIC UNION
# MAGIC select (*) from KN1_GRAM_NAICS

# COMMAND ----------

# DBTITLE 1,Drop table if exist
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Temp_GIC_KN1_GRAM_NAICS;

# COMMAND ----------

# DBTITLE 1,Create temp table
spark.sql('SELECT * FROM GIC_KN1_GRAM_NAICS').write.mode('overwrite').saveAsTable('Temp_GIC_KN1_GRAM_NAICS')

# COMMAND ----------

# DBTITLE 1,Other sources combine with GCDS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCDS_OtherSources_NAICS AS
# MAGIC select distinct
# MAGIC CASE WHEN gcds.identifier = gcob_le.GcobId 
# MAGIC       AND gcob_le.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' THEN gcob_le.LocalSystemIdentifier
# MAGIC      WHEN gcds.identifier = gcob_np.GcobId 
# MAGIC       AND gcob_np.Party_type = 'Natural Person' AND gcds.KeyStore_type = 'GCOBID' THEN gcob_np.LocalSystemIdentifier
# MAGIC      WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC      THEN l2.LocalSystemIdentifier
# MAGIC      WHEN gcds.identifier = gic.LocalSystemId AND gcds.KeyStore_type = 'GIC' THEN gic.LocalSystemIdentifier
# MAGIC      WHEN gcds.identifier = RANZ.SRC_PARTY_ID and gcds.KeyStore_type = 'CB RANZ' THEN RANZ.LocalSystemIdentifier
# MAGIC      else CONCAT('GCDS_',gcds.gcid)
# MAGIC END AS LocalSystemIdentifier
# MAGIC ,CASE WHEN gcds.identifier = gcob_le.GcobId 
# MAGIC       AND gcob_le.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' THEN gcob_le.LocalSystemId
# MAGIC      --WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person' THEN ncino.LocalSystemId
# MAGIC      WHEN gcds.identifier = gcob_np.GcobId 
# MAGIC       AND gcob_np.Party_type = 'Natural Person' AND gcds.KeyStore_type = 'GCOBID' THEN gcob_np.LocalSystemId
# MAGIC      WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC      THEN l2.LocalSystemId
# MAGIC      WHEN gcds.identifier = gic.LocalSystemId AND gcds.KeyStore_type = 'GIC' THEN gic.LocalSystemId
# MAGIC      WHEN gcds.identifier = RANZ.SRC_PARTY_ID and gcds.KeyStore_type = 'CB RANZ' THEN RANZ.LocalSystemId
# MAGIC      else CONCAT('GCDS_',gcds.gcid)
# MAGIC END AS LocalSystemId
# MAGIC ,CASE WHEN gcds.identifier = gcob_le.GcobId 
# MAGIC       AND gcob_le.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' THEN 'GCOB'
# MAGIC      WHEN gcds.identifier = gcob_np.GcobId 
# MAGIC       AND gcob_np.Party_type = 'Natural Person' AND gcds.KeyStore_type = 'GCOBID' THEN 'GCOB'
# MAGIC      WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC      THEN 'Legacy2'
# MAGIC      WHEN gcds.identifier = gic.LocalSystemId AND gcds.KeyStore_type = 'GIC' THEN gic.LocalSystemName
# MAGIC      WHEN gcds.identifier = RANZ.SRC_PARTY_ID and gcds.KeyStore_type = 'CB RANZ' THEN 'RANZ'
# MAGIC      else 'GCDS'
# MAGIC END AS LocalSystemName
# MAGIC ,COALESCE(gcds.NAICSCode, gcob_le.NAICSCode, gcob_np.NAICSCode, l2.NAICSCode, gic.NAICSCode, RANZ.NAICSCode) AS NAICSCode
# MAGIC ,COALESCE(gcds.NAICSDescription, gcob_le.NAICSDescription, gcob_np.NAICSDescription, l2.NAICSDescription, gic.NAICSDescription, RANZ.NAICSDescription) AS NAICSDescription
# MAGIC ,Case when gcds.NAICSCode IS NULL AND gcob_le.NAICSCode IS NULL AND gcob_np.NAICSCode IS NULL AND l2.NAICSCode IS NULL AND gic.NAICSCode IS NULL AND RANZ.NAICSCode IS NULL THEN NULL
# MAGIC       WHEN gcds.identifier = gcob_le.GcobId AND gcob_le.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' AND gcds.NAICSLevel = 'Primary' AND gcds.NAICSCode = gcob_le.NAICSCode THEN 'True'
# MAGIC      WHEN gcds.identifier = gcob_np.GcobId AND gcob_np.Party_type = 'Natural Person' AND gcds.KeyStore_type = 'GCOBID' AND gcds.NAICSLevel = 'Primary' AND gcds.NAICSCode = gcob_np.NAICSCode THEN 'True'
# MAGIC      WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID' AND gcds.NAICSLevel = 'Primary' AND gcds.NAICSCode = l2.NAICSCode THEN 'True'
# MAGIC      WHEN gcds.identifier = gic.LocalSystemId AND gcds.KeyStore_type = 'GIC' AND gcds.NAICSLevel = 'Primary' AND gcds.NAICSCode = gic.NAICSCode THEN 'True'
# MAGIC      WHEN gcds.identifier = RANZ.SRC_PARTY_ID and gcds.KeyStore_type = 'CB RANZ' AND gcds.NAICSLevel = 'Primary' AND gcds.NAICSCode = RANZ.NAICSCode THEN 'True'
# MAGIC     WHEN gcds.NAICSLevel = 'Primary' AND gcds.NAICSCode IS NOT NULL THEN 'True'
# MAGIC      ELSE 'False'
# MAGIC END as PrimaryNaicsFlag
# MAGIC ,gcds.NAICSPercentage
# MAGIC from GCDS_Clients gcds
# MAGIC left join GCOB_Client_NAICS gcob_le on gcds.identifier = gcob_le.GcobId
# MAGIC     and gcob_le.Party_type = 'Legal Entity'
# MAGIC     and gcds.KeyStore_type = 'GCOBID'
# MAGIC     and gcds.Party_type <> 'Natural Person'
# MAGIC left join GCOB_Client_NAICS gcob_np on gcds.identifier = gcob_np.GcobId
# MAGIC     and gcob_np.Party_type = 'Natural Person' 
# MAGIC     and gcds.KeyStore_type = 'GCOBID'
# MAGIC     and gcds.Party_type = 'Natural Person'
# MAGIC left join Legacy2_Client_NAICS l2 on gcds.Identifier=l2.SalesforceClientID_nCino
# MAGIC     and l2.Party_type = gcds.Party_type
# MAGIC     and gcds.KeyStore_type = 'NCINOID'
# MAGIC left join RANZ_Naics RANZ on gcds.identifier = RANZ.SRC_PARTY_ID 
# MAGIC          and gcds.KeyStore_type = 'CB RANZ'
# MAGIC left join hive_metastore.default.Temp_GIC_KN1_GRAM_NAICS gic on gcds.identifier = gic.LocalSystemId
# MAGIC         and gcds.KeyStore_type = 'GIC'

# COMMAND ----------

# DBTITLE 1,Non GCDS sources
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Non_GCDS AS
# MAGIC Select distinct LocalSystemIdentifier, LocalSystemId, LocalSystemName, NAICSCode, NAICSDescription
# MAGIC ,case when NaicsCode is null then null else 'False' end as PrimaryNaicsFlag 
# MAGIC ,null as NAICSPercentage from GCOB_Client_NAICS
# MAGIC UNION
# MAGIC select distinct LocalSystemIdentifier, LocalSystemId, LocalSystemName, NAICSCode, NAICSDescription
# MAGIC ,case when NaicsCode is null then null else 'False' end as PrimaryNaicsFlag 
# MAGIC ,null as NAICSPercentage from Legacy2_Client_NAICS
# MAGIC UNION
# MAGIC select distinct LocalSystemIdentifier, LocalSystemId, LocalSystemName, NAICSCode, NAICSDescription
# MAGIC ,case when NaicsCode is null then null else 'False' end as PrimaryNaicsFlag 
# MAGIC ,null as NAICSPercentage from hive_metastore.default.Temp_GIC_KN1_GRAM_NAICS
# MAGIC UNION
# MAGIC select distinct LocalSystemIdentifier, LocalSystemId, LocalSystemName, NAICSCode, NAICSDescription
# MAGIC ,case when NaicsCode is null then null else 'False' end as PrimaryNaicsFlag 
# MAGIC ,null as NAICSPercentage from RANZ_Naics

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view NonGCDS_Naics As
# MAGIC select * 
# MAGIC from Non_GCDS a
# MAGIC left anti join GCDS_OtherSources_NAICS b
# MAGIC on a.LocalSystemIdentifier = b.LocalSystemIdentifier and a.NaicsCode = b.NaicsCode

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view All_Naics As
# MAGIC select * from GCDS_OtherSources_NAICS 
# MAGIC union
# MAGIC select * from NonGCDS_Naics 

# COMMAND ----------

df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/3/data/{load_date_part}/*.parquet"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Storing in Storage account AS EXTERNAL table and interface table

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_Naics=spark.table('All_Naics')

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
# MAGIC where PartyIdentifier is not null --and NAICSCode is not null

# COMMAND ----------

df_Party_Naics_Final=spark.table('Party_Naics_Final')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
   save_to_saradar_storage_account(df_Party_Naics_Final, party_Naics_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_Party_Naics_Final, party_Naics_dataobject, radar_datamodel_version_number, environment)