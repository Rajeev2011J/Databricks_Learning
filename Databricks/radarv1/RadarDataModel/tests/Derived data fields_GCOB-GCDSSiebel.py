# Databricks notebook source
# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC #### Goal:
# MAGIC To produce integrated data objects to properly map between GCOB, GCDS, and Siebel
# MAGIC
# MAGIC ##### Author:
# MAGIC Ruud
# MAGIC
# MAGIC ##### Flow of logic
# MAGIC - connect GDP
# MAGIC - connect Siebel-GCDS-GCOB
# MAGIC
# MAGIC
# MAGIC ##### Business rules:
# MAGIC - party type: 'Sub account' are not real clients
# MAGIC   - e.g. LIGM: the real gcid is 16911. But where is the link to Sub Accounts?
# MAGIC   - sub account is rare, but appears to have no upward relationship assigned.
# MAGIC - party type: 'Organisation Unit' are not real clients
# MAGIC   - relation to sub clients:
# MAGIC     - Organisation Unit of
# MAGIC - party type: 'Managed Fund' are not real clients
# MAGIC   - relation to sub client:
# MAGIC     - Managed By
# MAGIC     - Has Fund Manager
# MAGIC
# MAGIC
# MAGIC ##### Output:
# MAGIC - Clients (incl former clients)
# MAGIC   - Use primary key as aggregation of sourcesystem (GCDS,GCOB,SIEBEL) and it's Identity in there.
# MAGIC   - Linked to GCOB (UniqueGCOBId) - Both NP and LE
# MAGIC - Client_CDDcase
# MAGIC - Client_Accounts (per GCID/GCOB/SiebelID, link to all accounts.)
# MAGIC
# MAGIC
# MAGIC ##### Controls
# MAGIC - Transactions that don't match clients
# MAGIC - Siebel Parties 3000/3400/3508 that don't match GCID
# MAGIC - GCOB parties that don't match GCID.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connecting EU GDP Sources
# MAGIC - GCOB
# MAGIC - GCDS
# MAGIC -
# MAGIC -
# MAGIC

# COMMAND ----------

# DBTITLE 1,load variables
import os
import pandas as pd
from datetime import datetime, timedelta

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
environment=os.environ['ENV']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,connect EU GDP
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Load GCDS
# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
    , 'client_Client'
    , 'client_PartyRole'
    , 'client_Products'
    , 'client_OnboardedLocations'
    , 'client_PartytoPartyRelationship'
    , 'client_RMA'
]

for item in gcds_tables:
    # get the most recent file available in gdp
    path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4602/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4602/data/LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcds_'+ item)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connect SA sources

# COMMAND ----------

# DBTITLE 1,Connect SA GDP
SAGDPStorage = 'edlcorestdbrprod0001'

spark.conf.set("fs.azure.account.auth.type."+SAGDPStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SAGDPStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SAGDPStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SAGDPStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SAGDPStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

dbutils.fs.ls(f'abfss://kn1@{SAGDPStorage}.dfs.core.windows.net/ADRBB_RESPOSTAS_ESCOLHIDAS/1/data/LOADED_DTS=20250407T000825Z')

# COMMAND ----------

# DBTITLE 1,Set Load date variable.
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

# DBTITLE 1,Connect KN1
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'ADRBB_RESPOSTAS_ESCOLHIDAS'
, 'ADRBB_QUESTOES'
, 'ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS'
, 'ADKYC_QUESTOES_COMPORT_CLIENTES'
, 'ADKYC_TIPOS_CONTRAPARTES'
, 'ADKYC_TP_CADASTRAIS'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://kn1@{SAGDPStorage}.dfs.core.windows.net/{row.GDPname}/1/data/LOADED_DTS=20250407*/*').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

gic_load_dts = 'LOADED_DTS=20250407*'

# COMMAND ----------

# DBTITLE 1,Connect GIC
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'pessoa'
]})
#gic_load_dts
# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gic@{SAGDPStorage}.dfs.core.windows.net/{row.GDPname}/1/data/{gic_load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connecting NA GDP

# COMMAND ----------

# DBTITLE 1,Connect NA GDP
NAReadStorage = 'edlcorestdamprod0001'
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

dbutils.fs.ls(f'abfss://nlsvf@{NAReadStorage}.dfs.core.windows.net/nls_dbo_cif/2/data/')

# COMMAND ----------

#NA load dts
na_load_dts = 'EDL_LOAD_DTS=2025-04-08*'

# COMMAND ----------

# DBTITLE 1,Connect NLS
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'nls_dbo_cif'
,'nls_dbo_cif_detail'
,'nls_dbo_cif_loan_relationship'
,'nls_dbo_loanacct'
,'nls_dbo_loanacct_detail'
,'nls_dbo_loanacct_rate'
,'nls_dbo_loanacct_trans_history'
,'nls_dbo_loan_status_codes'
,'nls_dbo_loan_group'
,'nls_dbo_loanacct_creditline'
,'nls_dbo_loan_payment_method'
,'nls_dbo_loanacct_payment'
,'nls_dbo_loan_class'
,'nls_dbo_base_rate'
,'nls_dbo_loan_port_codes'
,'nls_dbo_general_ledger_accounts'
,'nls_dbo_loanacct_gl_trans'
,'nls_dbo_loan_transaction_codes'
,'nls_DOL_Program'
# ,'NLS_NFRA_EXTRACTS_dbo_fdw_cash_flow_extract_US'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://nlsvf@{NAReadStorage}.dfs.core.windows.net/{row.GDPname}/2/data/{na_load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# DBTITLE 1,Connect NCINO
## Need to re? check access.

# COMMAND ----------

# DBTITLE 1,Connect AFS


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer-related Queries.
# MAGIC
# MAGIC ### Diving into GCDS. 
# MAGIC
# MAGIC
# MAGIC #### On the party itself
# MAGIC 1. How to determine LeadGCID
# MAGIC   - loop over relationships
# MAGIC 2. How to Determine WR_or_Retail_Client
# MAGIC   - onboarded locations only memberbank, or by businessline
# MAGIC 3. LifeCycleStatus
# MAGIC   - GCDS-based
# MAGIC 4. Legal_status
# MAGIC   - ?? not requested yet
# MAGIC 5. Legal_Form_EBX
# MAGIC   - just get Legal Form for now
# MAGIC 6.  Legal_Name
# MAGIC   - go with GCDS in basic
# MAGIC 7.  Customer_Type?
# MAGIC   - To see if relevant. could be like trust. / NP NPPC Organisation?
# MAGIC 8.  CDD_ClientType
# MAGIC   - specific set of client
# MAGIC 9.  CDD_SpecialEntityType (from GRAM)
# MAGIC   - GRAM question on entity type
# MAGIC
# MAGIC #### On relation between rabo and the party
# MAGIC
# MAGIC - client coverage
# MAGIC   - local co (=party)
# MAGIC   - global co (=party)
# MAGIC   - support department (=party)
# MAGIC   - involved locations
# MAGIC   - lead locations
# MAGIC - party_sector_involvement
# MAGIC   - Radar SectorTeam
# MAGIC   - Aetos-sectors
# MAGIC   - F&A - what local branch
# MAGIC   - portfolio
# MAGIC

# COMMAND ----------

# Diving into GCDS. 

#1. How to determine LeadGCID
#2. How to Determine WR_or_Retail_Client

#3. LifeCycleStatus
#4. 

# Legal_status
# Legal_Form_EBX
# Legal_Name

# Customer_Type?

# CCD_ClientType
# CDD_EntityRisk_Type

#

# COMMAND ----------

# DBTITLE 1,GCDS_ORG_UNIT
# MAGIC %sql
# MAGIC select t1.* 
# MAGIC ,   t2.Full_legal_name AS GCDSClientName,
# MAGIC     t2.ResidentialAddress_Country AS GCDSResidentialAddressCountry,
# MAGIC     t2.Principal_address_country AS GCDSPrincipalAddressCountry,
# MAGIC     t2.Party_type AS GCDSPartyType,
# MAGIC     t2.`CDD-entitytype` AS GCDS_CDDentitytype,
# MAGIC     t2.`CDD-next_reviewdate` AS GCDSCDDNextReviewdate,
# MAGIC     t2.`CDD-risk_rating` AS GCDSCDDRiskRating,
# MAGIC     t2.`Global_CO-name`AS GCDSGlobalCOname,
# MAGIC     t2.`Global_CO-location` AS GCDSGlobalCOlocation,
# MAGIC     t2.`Global_CO-email` AS GlobalCOemail,
# MAGIC     t2.`CO-businessline_description` AS GCDSCObusinesslinedescription,
# MAGIC     t2.`Global_CO-Serviced_by` AS GCDSGlobalCOServicedBy,
# MAGIC     t2.`RM-name` AS GCDSRMName,
# MAGIC     t2.`RM-location` AS GCDSRMLocation,
# MAGIC     t2.`RM-email` AS GCDSRMEmail,
# MAGIC     t2.`RM-businessline_description` AS GCDSRMbusinesslineDescription
# MAGIC from gcds_client_PartytoPartyRelationship as t1 
# MAGIC left join gcds_client_Client as t2 on t1.GCID = t2.GCID    
# MAGIC where t1.`Relationship-Type`  = 'Organisation Unit of'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GcdsClient AS
# MAGIC SELECT DISTINCT
# MAGIC     t2.GCID,
# MAGIC     t2.Full_legal_name AS GCDSClientName,
# MAGIC     t2.ResidentialAddress_Country AS GCDSResidentialAddressCountry,
# MAGIC     t2.Principal_address_country AS GCDSPrincipalAddressCountry,
# MAGIC     t2.Party_type AS GCDSPartyType,
# MAGIC     t2.`CDD-entitytype` AS GCDS_CDDentitytype,
# MAGIC     t2.`CDD-next_reviewdate` AS GCDSCDDNextReviewdate,
# MAGIC     t2.`CDD-risk_rating` AS GCDSCDDRiskRating,
# MAGIC     t2.`Global_CO-name`AS GCDSGlobalCOname,
# MAGIC     t2.`Global_CO-location` AS GCDSGlobalCOlocation,
# MAGIC     t2.`Global_CO-email` AS GlobalCOemail,
# MAGIC     t2.`CO-businessline_description` AS GCDSCObusinesslinedescription,
# MAGIC     t2.`Global_CO-Serviced_by` AS GCDSGlobalCOServicedBy,
# MAGIC     t2.`RM-name` AS GCDSRMName,
# MAGIC     t2.`RM-location` AS GCDSRMLocation,
# MAGIC     t2.`RM-email` AS GCDSRMEmail,
# MAGIC     t2.`RM-businessline_description` AS GCDSRMbusinesslineDescription,
# MAGIC     t2.`RM-_Serviced_by` AS GCDSRMServicedBy,
# MAGIC     --t3.Life_cycle_status AS GCDSClientlifecyclestatus,
# MAGIC     t2.`CO-email` AS GCDSGCOemail,
# MAGIC     t2.Registered_address_country AS GCDSRegisteredCountry,
# MAGIC     t2.`CDD-risk_rating` AS GCDSCDDRating
# MAGIC    
# MAGIC
# MAGIC FROM 
# MAGIC     GCDS_client_Client AS t2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get LEAD GCID per party

# COMMAND ----------

# DBTITLE 1,Define GCDS_LeadGCID
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCDS_LeadGCID AS
# MAGIC
# MAGIC WITH REL_Selection AS 
# MAGIC
# MAGIC (select * 
# MAGIC from gcds_client_PartytoPartyRelationship 
# MAGIC where `relationship-type` in ('Branch of', 'Organisation Unit of', 'Sub-fund of') )
# MAGIC
# MAGIC , REL_hierarchy AS
# MAGIC (
# MAGIC select 
# MAGIC  t1.Gcid, 
# MAGIC  coalesce(t8.`Relationship-Value` , t7.`Relationship-Value`, t6.`Relationship-Value`, t5.`Relationship-Value` , t4.`Relationship-Value` ,t3.`Relationship-Value`, t2.`Relationship-Value`, t1.`Relationship-Value`) AS PartyLeadGCID
# MAGIC  
# MAGIC , t2.`Relationship-Value` AS t2val
# MAGIC , t3.`Relationship-Value` AS t3val
# MAGIC , t4.`Relationship-Value` AS t4val
# MAGIC , t6.`Relationship-Value` AS t6val
# MAGIC , t8.`Relationship-Value` AS t8val
# MAGIC
# MAGIC from REL_Selection as t1
# MAGIC left join REL_Selection t2 on t1.`Relationship-Value` = t2.Gcid
# MAGIC left join REL_Selection t3 on t2.`Relationship-Value` = t3.Gcid
# MAGIC left join REL_Selection t4 on t3.`Relationship-Value` = t4.Gcid
# MAGIC left join REL_Selection t5 on t4.`Relationship-Value` = t5.Gcid
# MAGIC left join REL_Selection t6 on t5.`Relationship-Value` = t6.Gcid
# MAGIC left join REL_Selection t7 on t6.`Relationship-Value` = t7.Gcid
# MAGIC left join REL_Selection t8 on t7.`Relationship-Value` = t8.Gcid
# MAGIC )
# MAGIC
# MAGIC Select t1.GCID
# MAGIC , COALESCE(t2.PartyLeadGCID, t1.GCID) AS LeadGCID
# MAGIC FROM GCDS_client_client t1
# MAGIC LEFT JOIN REL_hierarchy t2 on t1.GCID = t2.Gcid
# MAGIC

# COMMAND ----------

# DBTITLE 1,GCDS_LeadGCID2
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCDS_LeadGCID2 AS
# MAGIC
# MAGIC SELECT
# MAGIC       cast(
# MAGIC         CASE
# MAGIC           WHEN RC3.`Relationship-Value` IS NOT NULL AND C3.Party_type <> 'Legal Entity' THEN RC3.`Relationship-Value`
# MAGIC           WHEN C4.GCID IS NOT NULL THEN C4.GCID
# MAGIC           WHEN RC2.`Relationship-Value` IS NOT NULL AND C2.Party_type <> 'Legal Entity' THEN RC2.`Relationship-Value`
# MAGIC           WHEN C3.GCID IS NOT NULL THEN C3.GCID
# MAGIC           WHEN RC1.`Relationship-Value` IS NOT NULL AND C1.Party_type <> 'Legal Entity' THEN RC1.`Relationship-Value`
# MAGIC           WHEN C2.GCID IS NOT NULL THEN C2.GCID
# MAGIC 				  WHEN RC.`Relationship-Value` IS NOT NULL AND C.Party_type <> 'Legal Entity' THEN RC.`Relationship-Value`
# MAGIC   				WHEN C1.GCID IS NULL THEN C.GCID
# MAGIC   				ELSE R.`Relationship-Value`
# MAGIC         END AS INT
# MAGIC       ) LegalEntity
# MAGIC       , R.`Relationship-Value`
# MAGIC       , R.`Relationship-Type`
# MAGIC       , C.GCID
# MAGIC     
# MAGIC     FROM GCDS_client_Client C                                                       
# MAGIC     LEFT JOIN GCDS_client_PartytoPartyRelationship RC ON RC.GCID = C.GCID AND RC.`Relationship-Type` = 'Credit'
# MAGIC     LEFT JOIN GCDS_client_PartytoPartyRelationship R ON C.GCID = R.GCID -- AND RC.`Relationship-Value` IS NULL
# MAGIC       AND (
# MAGIC         (C.Party_type IN ('Branch','Foreign Branch') AND R.`Relationship-Type` = 'Branch of')
# MAGIC         OR (C.Party_type = 'Sub Account' AND R.`Relationship-Type` = 'Main Account')
# MAGIC         OR (C.Party_type = 'Organisation Unit' AND R.`Relationship-Type` = 'Organisation Unit of')
# MAGIC         OR (C.Party_type = 'Managed Fund' AND R.`Relationship-Type` = 'Managed by')
# MAGIC         OR (C.Party_type = 'Sub-fund' AND R.`Relationship-Type` = 'Sub-fund of')
# MAGIC   		)
# MAGIC     LEFT JOIN GCDS_client_Client C1 ON C1.GCID = R.`Relationship-Value`
# MAGIC       AND ((C.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C1.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
# MAGIC       OR (C.Party_type IN ('Branch','Foreign Branch') AND C1.Party_type = 'Legal Entity'))
# MAGIC     LEFT JOIN GCDS_client_PartytoPartyRelationship RC1 ON RC1.GCID = C1.GCID AND RC1.`Relationship-Type` = 'Credit'
# MAGIC     LEFT JOIN GCDS_client_PartytoPartyRelationship R1 ON R1.GCID = C1.GCID -- AND RC1.`Relationship-Value` IS NULL
# MAGIC       AND R1.`Relationship-Value` <> R1.GCID
# MAGIC       AND (
# MAGIC         (C1.Party_type IN ('Branch','Foreign Branch') AND R1.`Relationship-Type` = 'Branch of')
# MAGIC         OR (C1.Party_type = 'Sub Account' AND R1.`Relationship-Type` = 'Main Account')
# MAGIC         OR (C1.Party_type = 'Organisation Unit' AND R1.`Relationship-Type` = 'Organisation Unit of')
# MAGIC         OR (C1.Party_type = 'Managed Fund' AND R1.`Relationship-Type` = 'Managed by')
# MAGIC         OR (C1.Party_type = 'Sub-fund' AND R1.`Relationship-Type` = 'Sub-fund of')
# MAGIC   		)
# MAGIC     LEFT JOIN GCDS_client_Client C2 ON C2.GCID = R1.`Relationship-Value`
# MAGIC       AND ((C1.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C2.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
# MAGIC       OR (C1.Party_type IN ('Branch','Foreign Branch') AND C2.Party_type = 'Legal Entity')) 
# MAGIC     LEFT JOIN GCDS_client_PartytoPartyRelationship RC2 ON RC2.GCID = C2.GCID AND RC2.`Relationship-Type` = 'Credit'
# MAGIC     LEFT JOIN GCDS_client_PartytoPartyRelationship R2 ON R2.GCID = C2.GCID -- AND RC2.`Relationship-Value` IS NULL
# MAGIC       AND R2.`Relationship-Value` <> R2.GCID
# MAGIC       AND (
# MAGIC         (C2.Party_type IN ('Branch','Foreign Branch') AND R2.`Relationship-Type` = 'Branch of')
# MAGIC         OR (C2.Party_type = 'Sub Account' AND R2.`Relationship-Type` = 'Main Account')
# MAGIC         OR (C2.Party_type = 'Organisation Unit' AND R2.`Relationship-Type` = 'Organisation Unit of')
# MAGIC         OR (C2.Party_type = 'Managed Fund' AND R2.`Relationship-Type` = 'Managed by')
# MAGIC         OR (C2.Party_type = 'Sub-fund' AND R2.`Relationship-Type` = 'Sub-fund of')
# MAGIC       )
# MAGIC     LEFT JOIN GCDS_client_Client C3 ON C3.GCID = R2.`Relationship-Value`
# MAGIC       AND ((C2.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C3.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
# MAGIC       OR ( C2.Party_type IN ('Branch','Foreign Branch') AND C3.Party_type = 'Legal Entity')) 
# MAGIC     LEFT JOIN GCDS_client_PartytoPartyRelationship RC3 ON RC3.GCID = C3.GCID 
# MAGIC       AND RC3.`Relationship-Type` = 'Credit'
# MAGIC     LEFT JOIN GCDS_client_PartytoPartyRelationship R3 ON R3.GCID = R2.`Relationship-Value` -- AND RC3.`Relationship-Value` IS NULL
# MAGIC       AND R3.`Relationship-Value` <> R3.GCID
# MAGIC       AND (
# MAGIC         (C3.Party_type IN ('Branch','Foreign Branch') AND R3.`Relationship-Type` = 'Branch of')
# MAGIC         OR (C3.Party_type = 'Sub Account' AND R3.`Relationship-Type` = 'Main Account')
# MAGIC         OR (C3.Party_type = 'Organisation Unit' AND R3.`Relationship-Type` = 'Organisation Unit of')
# MAGIC         OR (C3.Party_type = 'Managed Fund' AND R3.`Relationship-Type` = 'Managed by')
# MAGIC         OR (C3.Party_type = 'Sub-fund' AND R3.`Relationship-Type` = 'Sub-fund of')
# MAGIC       )
# MAGIC     LEFT JOIN GCDS_client_Client C4 ON C4.GCID = R2.`Relationship-Value`
# MAGIC       AND ((C3.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C4.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
# MAGIC       OR (C3.Party_type IN ('Branch','Foreign Branch') AND C4.Party_type = 'Legal Entity')) 

# COMMAND ----------

# DBTITLE 1,Check for Managed By
# MAGIC %sql
# MAGIC select * from gcds_client_PartytoPartyRelationship where `Relationship-Value` = '16911'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get the Most updated GCID per party.

# COMMAND ----------

# DBTITLE 1,Define what GCID has updated to what other GCID
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCDS_UpdatedGCID AS
# MAGIC
# MAGIC WITH REL_Selection AS 
# MAGIC (select * from gcds_client_keystorekey where `keystore_type` in ('Global Client ID') )
# MAGIC
# MAGIC select * FROM REL_Selection

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GCDS_UpdatedGCID where status <> 'Inactive'

# COMMAND ----------

# MAGIC %sql
# MAGIC select gcid, 
# MAGIC * from gcds_client_client where gcid in ('2494844', '75630')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logic for Retail or W&R Client
# MAGIC Input from Raymond Nijeboer:
# MAGIC
# MAGIC Within GCDS ownership can be determined based on attributes Business Line and Client Owning Location.​
# MAGIC
# MAGIC - Question: if a memberbank is using W&R for a specific product. Why would the current account be set up in Siebel 3000/3400, and not on the memberbank bank_code?

# COMMAND ----------

# DBTITLE 1,select gcds customer with no CO ownership
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW gcds_no_co AS
# MAGIC
# MAGIC select t2.life_cycle_status
# MAGIC , t2.party_role
# MAGIC ,t1.* 
# MAGIC from GcdsClient t1
# MAGIC left join gcds_client_PartyRole t2 on t1.gcid = t2.gcid
# MAGIC
# MAGIC where t1.gcdsCObusinesslinedescription is null
# MAGIC and t1.gcdsGlobalCOlocation is null
# MAGIC and GCDSPartyType = 'Legal Entity'
# MAGIC and t2.life_cycle_status = 'Client'
# MAGIC and t2.Party_role = 'Customer'

# COMMAND ----------

# DBTITLE 1,select no CO ownership, and their onboarded locations.
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RetailViaOnboardedLocation AS 
# MAGIC WITH CTE_RetailOnboardedOnly AS 
# MAGIC
# MAGIC   ( Select T1.GCID
# MAGIC     , t1.NrActiveOnboardedLocationsPerGCID
# MAGIC     , t1.SET_BRANCH_CODES
# MAGIC     , t2.NrActiveNLM
# MAGIC     , CASE 
# MAGIC       WHEN t1.NrActiveOnboardedLocationsPerGCID = t2.NrActiveNLM THEN 'Retail'
# MAGIC       ELSE 'W&R'
# MAGIC       END AS `WholeSaleOrRetailParty`
# MAGIC     FROM
# MAGIC     (select GCID, count(*) NrActiveOnboardedLocationsPerGCID, collect_set(branch_code) AS SET_BRANCH_CODES
# MAGIC     from gcds_client_OnboardedLocations where Status = 'Active'
# MAGIC     GROUP BY GCID) AS T1
# MAGIC   LEFT JOIN 
# MAGIC     (select GCID AS GCID2, count(*) as NrActiveNLM
# MAGIC     from gcds_client_OnboardedLocations where branch_code = 'NLM' and Status = 'Active'
# MAGIC     GROUP BY GCID2) as t2 on t1.GCID = t2.GCID2 )
# MAGIC
# MAGIC select 
# MAGIC  t2.`WholeSaleOrRetailParty`
# MAGIC  ,t2.SET_BRANCH_CODES
# MAGIC  ,t1.*
# MAGIC from gcds_no_co as t1
# MAGIC left join CTE_RetailOnboardedOnly t2 on t1.gcid = t2.gcid
# MAGIC

# COMMAND ----------

# DBTITLE 1,SQL for Retail-BU owned products only
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RetailViaProducts AS 
# MAGIC WITH CTE_RetailProductsOnly AS 
# MAGIC
# MAGIC   ( Select T1.GCID
# MAGIC     , t1.NrActiveProductsPerGCID
# MAGIC     , t1.SET_BUSINESS_UNITS
# MAGIC     , t2.NrActiveNLMProducts
# MAGIC     , CASE 
# MAGIC       WHEN t1.NrActiveProductsPerGCID = t2.NrActiveNLMProducts THEN 'Retail'
# MAGIC       ELSE 'W&R'
# MAGIC       END AS `WholeSaleOrRetailParty`
# MAGIC     FROM
# MAGIC     (select GCID, count(*) NrActiveProductsPerGCID, collect_set(BusinessUnit) AS SET_BUSINESS_UNITS
# MAGIC     from gcds_client_products where Status = 'Active'
# MAGIC     GROUP BY GCID) AS T1
# MAGIC   LEFT JOIN 
# MAGIC     (select GCID AS GCID2, count(*) as NrActiveNLMProducts
# MAGIC     from gcds_client_products where BusinessUnit = 'Memberbank NL' and Status = 'Active'
# MAGIC     GROUP BY GCID2) as t2 on t1.GCID = t2.GCID2 )
# MAGIC
# MAGIC select 
# MAGIC  t2.`WholeSaleOrRetailParty`
# MAGIC  ,t2.SET_BUSINESS_UNITS
# MAGIC  ,t2.NrActiveProductsPerGCID
# MAGIC  ,t1.*
# MAGIC from gcds_no_co as t1
# MAGIC left join CTE_RetailProductsOnly t2 on t1.gcid = t2.gcid

# COMMAND ----------

# DBTITLE 1,check difference via products and onboarded locations
# MAGIC %sql
# MAGIC select t1.*
# MAGIC , t2.NrActiveProductsPerGCID
# MAGIC , t2.SET_BUSINESS_UNITS
# MAGIC from ((select distinct gcid from RetailViaOnboardedLocation where WholeSaleOrRetailParty = 'Retail')
# MAGIC minus 
# MAGIC (select distinct gcid from RetailViaProducts where WholeSaleOrRetailParty = 'Retail')) as t1
# MAGIC
# MAGIC left join RetailViaProducts t2 on t1.gcid = t2.gcid
# MAGIC
# MAGIC -- Conclusion: only Inactive products, but still as Active and Member-bank onboarded party.
# MAGIC

# COMMAND ----------

# DBTITLE 1,check retail parties-only via product, but onboarded in wr
# MAGIC %sql
# MAGIC select *
# MAGIC from
# MAGIC (select distinct gcid from RetailViaProducts where WholeSaleOrRetailParty = 'Retail')
# MAGIC minus 
# MAGIC  (select distinct gcid from RetailViaOnboardedLocation where WholeSaleOrRetailParty = 'Retail')
# MAGIC
# MAGIC -- Conclusion: only Inactive products, but still as Active and Member-bank onboarded party.

# COMMAND ----------

# MAGIC %sql
# MAGIC select t1.* 
# MAGIC from gcds_no_co t1
# MAGIC where (t1.gcid not in (select distinct(gcid) from RetailViaProducts where WholeSaleOrRetailParty = 'Retail')) 
# MAGIC AND (t1.gcid not in (select distinct(gcid) from RetailViaOnboardedLocation where WholeSaleOrRetailParty = 'Retail'))

# COMMAND ----------

# DBTITLE 1,TODO: get first Onboarded location for clients (mostly RANZ) without CO and location
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCDS_ALTERNATIVE_GCO_LOCATION AS
# MAGIC
# MAGIC SELECT GCID
# MAGIC , MIN(Branche_name) AS BRANCH_NAME
# MAGIC , count(distinct branche_name) AS NR_OF_BRANCHES_ONBOARDED
# MAGIC from gcds_client_OnboardedLocations 
# MAGIC where Status = 'Active' AND branch_code <> 'NLM'
# MAGIC
# MAGIC AND GCID IN (select t1.GCID 
# MAGIC
# MAGIC from gcds_no_co t1
# MAGIC
# MAGIC where (t1.gcid not in (select distinct(gcid) from RetailViaProducts where WholeSaleOrRetailParty = 'Retail')) 
# MAGIC AND (t1.gcid not in (select distinct(gcid) from RetailViaOnboardedLocation where WholeSaleOrRetailParty = 'Retail')))
# MAGIC
# MAGIC GROUP BY GCID

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GCDS_ALTERNATIVE_GCO_LOCATION
# MAGIC -- 1103523 is both AU and NZ
# MAGIC -- 921193 is both AU and NZ

# COMMAND ----------

# DBTITLE 1,Check for which GCIDS are not in scope of alternative GCO fix
# MAGIC %sql
# MAGIC select t1.GCID
# MAGIC from gcds_no_co t1
# MAGIC where (t1.gcid not in (select distinct(gcid) from RetailViaProducts where WholeSaleOrRetailParty = 'Retail')) 
# MAGIC AND (t1.gcid not in (select distinct(gcid) from RetailViaOnboardedLocation where WholeSaleOrRetailParty = 'Retail'))
# MAGIC
# MAGIC MINUS
# MAGIC
# MAGIC select GCID from GCDS_ALTERNATIVE_GCO_LOCATION

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT GCID
# MAGIC , MIN(Branche_name) AS BRANCH_NAME
# MAGIC
# MAGIC from gcds_client_OnboardedLocations 
# MAGIC where Status = 'Active' AND branch_code <> 'NLM'
# MAGIC --AND GCID IN (1260499, 48358, 1290007)
# MAGIC GROUP BY GCID
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select GCDSID
# MAGIC ,  SectorTeam AS KYCSector
# MAGIC , KYCGroup
# MAGIC , GlobalReportingRegion
# MAGIC , GlobalClientOwnerLocation AS KYC_GCO_Location
# MAGIC , 
# MAGIC  from radar.clients limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gcds_client_OnboardedLocations  limit 3

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion on MemberBank clients
# MAGIC
# MAGIC - 1. If there is no GCO
# MAGIC - 2. If all onboarded locations are Memberbank
# MAGIC - 3. If all active products are booked on memberbank. 
# MAGIC
# MAGIC Else: Wholesale party.

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct gcid from RetailViaOnboardedLocation

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gcds_client_products limit 4

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct branch_code from gcds_client_OnboardedLocations

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gcds_client_OnboardedLocations limit 4

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC t1.`CO-businessline_description` AS gcdsCObusinesslinedescription
# MAGIC , t1.`Global_CO-location` AS gcdsGlobalCOlocation
# MAGIC from gcds_client_client as t1
# MAGIC order by gcdsCObusinesslinedescription, gcdsGlobalCOlocation
# MAGIC
# MAGIC -- businessline = 'Grootbedrijf', ?International Desks , ?International Services, Other RN Divisions

# COMMAND ----------

# DBTITLE 1,questionable businesslines
# MAGIC %sql
# MAGIC select * from GcdsClient 
# MAGIC where gcdsCObusinesslinedescription in ('Other RN Divisions' , 'Retail Banking', 'Rabo Investments', 'International Desks')
# MAGIC and GCDSGlobalCOLocation = 'Utrecht'
# MAGIC AND GCDSPartyType = 'Legal Entity'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC t1.`CO-businessline_description` AS gcdsCObusinesslinedescription
# MAGIC , t1.`Global_CO-location` AS gcdsGlobalCOlocation
# MAGIC from gcds_client_client as t1
# MAGIC order by gcdsCObusinesslinedescription, gcdsGlobalCOlocation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query for likely Retail clients?
# MAGIC select distinct
# MAGIC
# MAGIC t4.party_role AS gcdsPartyRole
# MAGIC , t4.Life_cycle_status AS gcdsClientlifecyclestatus
# MAGIC , t1.*
# MAGIC from GcdsClient as t1
# MAGIC LEFT JOIN gcds_client_PartyRole t4 ON t1.gcid = t4.gcid
# MAGIC
# MAGIC where t1.gcdsGlobalCOlocation is null
# MAGIC and t1.gcdspartytype = 'Legal Entity'
# MAGIC and t4.party_Role = 'Customer'
# MAGIC and t4.Life_cycle_status = 'Client'

# COMMAND ----------

# DBTITLE 1,Select all parties for who ALL onboarded locations are Memberbanks
# MAGIC %sql
# MAGIC --|TODO; for each gcid, group by onb location and count all. Also group by onboarding location where branch code is memberbank and count all. Then select where the numbers are the same.
# MAGIC select * from gcds_client_OnboardedLocations limit 10

# COMMAND ----------

# DBTITLE 1,TODO: select all clients where all the products are of Busines Unit MemberBank NL
# MAGIC %sql
# MAGIC -- similar as above for client_Products'.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Next relevant topic ... ClientCoverage

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gcds_client_keystorekey limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct keystore_type from gcds_client_keystorekey

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get customer status
# MAGIC GCDS_client_PartyRole 

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC --select distinct `Party_type` from gcds_client_client
# MAGIC select t2.Life_cycle_status,
# MAGIC t2.Party_role,
# MAGIC t3.`Relationship-Type`,
# MAGIC     t1.Full_legal_name AS GCDSClientName,
# MAGIC     t1.Party_type,
# MAGIC t1.* 
# MAGIC from gcds_client_client t1
# MAGIC left join GCDS_client_PartyRole t2 on t1.gcid = t2.gcid
# MAGIC left join gcds_client_PartytoPartyRelationship t3 on t1.gcid = t3.gcid
# MAGIC  where t3.`Relationship-Value` = '16911'
# MAGIC --and t1.`Party_type` = 'Sub Account' --and t2.life_cycle_status <> 'Former Client'
# MAGIC -- Organisation Unit of

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct `Relationship-Type` from gcds_client_PartytoPartyRelationship

# COMMAND ----------

# MAGIC %sql
# MAGIC --select distinct `Party_type` from gcds_client_client
# MAGIC select t2.*,
# MAGIC     t1.Full_legal_name AS GCDSClientName,
# MAGIC t1.* 
# MAGIC from gcds_client_client t1
# MAGIC left join GCDS_client_PartyRole t2 on t1.gcid = t2.gcid
# MAGIC where t1.`Party_type` = 'Sub Account' and t2.life_cycle_status <> 'Former Client'
# MAGIC -- Organisation Unit of
# MAGIC -- 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gcds_client_PartytoPartyRelationship where `gcid`= 19124 19124

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GcdsClientDataWithAllPartyRole AS
# MAGIC SELECT DISTINCT
# MAGIC     t1.gcid,
# MAGIC     t1.keyStore_type,
# MAGIC     t1.keyStore_value AS GCOBID,
# MAGIC     t1.status AS keyStore_value_status,
# MAGIC     t2.Full_legal_name AS GCDSClientName,
# MAGIC     t2.ResidentialAddress_Country AS GCDSResidentialAddressCountry,
# MAGIC     t2.Principal_address_country AS GCDSPrincipalAddressCountry,
# MAGIC     t2.Party_type AS GCDSPartyType,
# MAGIC     t2.`CDD-entitytype` AS GCDS_CDDentitytype,
# MAGIC     t2.`CDD-next_reviewdate` AS GCDSCDDNextReviewdate,
# MAGIC     t2.`CDD-risk_rating` AS GCDSCDDRiskRating,
# MAGIC     t2.`Global_CO-name`AS GCDSGlobalCOname,
# MAGIC     t2.`Global_CO-location` AS GCDSGlobalCOlocation,
# MAGIC     t2.`Global_CO-email` AS GlobalCOemail,
# MAGIC     t2.`CO-businessline_description` AS GCDSCObusinesslinedescription,
# MAGIC     t2.`Global_CO-Serviced_by` AS GCDSGlobalCOServicedBy,
# MAGIC     t2.`RM-name` AS GCDSRMName,
# MAGIC     t2.`RM-location` AS GCDSRMLocation,
# MAGIC     t2.`RM-email` AS GCDSRMEmail,
# MAGIC     t2.`RM-businessline_description` AS GCDSRMbusinesslineDescription,
# MAGIC     t2.`RM-_Serviced_by` AS GCDSRMServicedBy,
# MAGIC     t3.Life_cycle_status AS GCDSClientlifecyclestatus,
# MAGIC     t2.`CO-email` AS GCDSGCOemail,
# MAGIC     t2.Registered_address_country AS GCDSRegisteredCountry,
# MAGIC     t2.`CDD-risk_rating` AS GCDSCDDRating,
# MAGIC     t1.EDL_LOAD_DTS AS GCDSLastRefresh
# MAGIC FROM 
# MAGIC     GCDS_client_KeyStoreKey AS t1
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_Client AS t2 ON t1.gcid = t2.gcid
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_PartyRole AS t3 ON t1.gcid = t3.gcid
# MAGIC LEFT JOIN
# MAGIC     GCDS_client_Products AS t4 ON t1.gcid = t4.gcid
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_OnboardedLocations AS t5 ON t1.gcid = t5.gcid
# MAGIC WHERE 
# MAGIC     t1.KeyStore_type = 'GCOBID'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GcdsClientDataWithAllPartyRole AS
# MAGIC SELECT DISTINCT
# MAGIC     t1.gcid,
# MAGIC     t1.keyStore_type,
# MAGIC     t1.keyStore_value AS GCOBID,
# MAGIC     t1.status AS keyStore_value_status,
# MAGIC     t2.Full_legal_name AS GCDSClientName,
# MAGIC     t2.ResidentialAddress_Country AS GCDSResidentialAddressCountry,
# MAGIC     t2.Principal_address_country AS GCDSPrincipalAddressCountry,
# MAGIC     t2.Party_type AS GCDSPartyType,
# MAGIC     t2.`CDD-entitytype` AS GCDS_CDDentitytype,
# MAGIC     t2.`CDD-next_reviewdate` AS GCDSCDDNextReviewdate,
# MAGIC     t2.`CDD-risk_rating` AS GCDSCDDRiskRating,
# MAGIC     t2.`Global_CO-name`AS GCDSGlobalCOname,
# MAGIC     t2.`Global_CO-location` AS GCDSGlobalCOlocation,
# MAGIC     t2.`Global_CO-email` AS GlobalCOemail,
# MAGIC     t2.`CO-businessline_description` AS GCDSCObusinesslinedescription,
# MAGIC     t2.`Global_CO-Serviced_by` AS GCDSGlobalCOServicedBy,
# MAGIC     t2.`RM-name` AS GCDSRMName,
# MAGIC     t2.`RM-location` AS GCDSRMLocation,
# MAGIC     t2.`RM-email` AS GCDSRMEmail,
# MAGIC     t2.`RM-businessline_description` AS GCDSRMbusinesslineDescription,
# MAGIC     t2.`RM-_Serviced_by` AS GCDSRMServicedBy,
# MAGIC     t3.Life_cycle_status AS GCDSClientlifecyclestatus,
# MAGIC     t2.`CO-email` AS GCDSGCOemail,
# MAGIC     t2.Registered_address_country AS GCDSRegisteredCountry,
# MAGIC     t2.`CDD-risk_rating` AS GCDSCDDRating,
# MAGIC     t1.EDL_LOAD_DTS AS GCDSLastRefresh
# MAGIC FROM 
# MAGIC     GCDS_client_KeyStoreKey AS t1
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_Client AS t2 ON t1.gcid = t2.gcid
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_PartyRole AS t3 ON t1.gcid = t3.gcid
# MAGIC LEFT JOIN
# MAGIC     GCDS_client_Products AS t4 ON t1.gcid = t4.gcid
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_OnboardedLocations AS t5 ON t1.gcid = t5.gcid
# MAGIC WHERE 
# MAGIC     t1.KeyStore_type = 'GCOBID'

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


