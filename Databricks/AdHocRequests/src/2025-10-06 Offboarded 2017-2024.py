# Databricks notebook source
# MAGIC %md
# MAGIC ### request:
# MAGIC From Vincent Steginga. 
# MAGIC
# MAGIC #### Context
# MAGIC This is follow-up of notebook: 2025-04-28_GCDS-Riskshield-Offboardings-7yrs
# MAGIC
# MAGIC But this request:
# MAGIC belangrijkste vraag is of we de cijfers van FI en GCC kunnen krijgen
# MAGIC
# MAGIC Number of offboarded clients for FI portfolio (Global FI hub -> Europe and Asia, and GCC (NLA) clients.)
# MAGIC
# MAGIC ##### Author: Ruud van Laar
# MAGIC
# MAGIC ##### Flow:
# MAGIC - check GCDS. 
# MAGIC - per location. GCIDS on former client who have offboarded date on recent years. 

# COMMAND ----------

catalogname = 'wr_fj_parties_and_risk_assessment_preprd'

# COMMAND ----------

# generic imports
import pandas as pd
from datetime import datetime
from datetime import timedelta
import os

currentDate = datetime.today().strftime('%Y%m%d')
Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')

currentYear = datetime.today().strftime('%Y')
currentMonth = datetime.today().strftime('%m')
currentDay = datetime.today().strftime('%d')

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

date_parameter = '20250423'
load_dts = 'EDL_LOAD_DTS=' + '20250423' + '*'
# = datetime.today().strftime('%Y%m%d')
#load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

#Derive the date for which data has to be processed from GDP
Today = (datetime.today() - timedelta(90)).strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
gcds_df = pd.DataFrame({'definedDatasetname':[
'client_Client',
'client_KeyStoreKey',
'client_PartyRole',
'client_PartytoPartyRelationship',
'client_OnboardedLocations'
]})

# Create TempView for each loading table
for index, row in gcds_df.iterrows():
    spark.read.parquet(f'abfss://gcds@{ReadStorage}.dfs.core.windows.net/{row.definedDatasetname}/4601/data/{load_dts}/*.parquet').createOrReplaceTempView(row.definedDatasetname)

# COMMAND ----------

dbutils.fs.ls('abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_Client/4601/data/')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from client_OnboardedLocations limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select Branche_name , count(*) as GCIDS_WITH_OFFBOARDING_DATE from client_OnboardedLocations
# MAGIC where Offboarding_date > '2017-01-01'
# MAGIC group by Branche_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from client_client

# COMMAND ----------

# MAGIC %sql
# MAGIC -- query all parties that have been offboarded, and per location. 
# MAGIC select t2.Branche_name, count(distinct(t1.GCID)) as GCID_COUNT, t1.entity_classification
# MAGIC
# MAGIC from client_client as t1
# MAGIC left join client_OnboardedLocations as t2 on t1.gcid = t2.gcid
# MAGIC left join client_PartyRole as t3 on t1.gcid = t3.gcid
# MAGIC where t1.party_type = 'Legal Entity'
# MAGIC and t2.Offboarding_date > '2017-01-01' and t2.Offboarding_date < '2024-01-01'
# MAGIC and t3.party_role = 'Customer'
# MAGIC and t3.Life_cycle_status = 'Former Client'
# MAGIC and t2.Branche_name IN ('Rabobank Antwerp', 
# MAGIC 'Rabobank Netherlands', 
# MAGIC 'Rabobank London', 
# MAGIC 'Rabobank Kenya', 
# MAGIC 'Rabobank Netherlands', 
# MAGIC 'London', 
# MAGIC 'Rabobank Turkey', 
# MAGIC 'Rabobank Milan', 
# MAGIC 'Rabobank Frankfurt', 
# MAGIC 'Italy', 
# MAGIC 'Rabobank Dublin', 
# MAGIC 'Spain', 
# MAGIC 'Rabobank Milan', 
# MAGIC 'Rabobank Frankfurt', 
# MAGIC 'Rabobank Paris', 
# MAGIC 'Rabobank Frankfurt', 
# MAGIC 'Rabobank London', 
# MAGIC 'Rabobank Madrid', 
# MAGIC 'Rabobank Argentina', 
# MAGIC 'Rabobank Kenya', 
# MAGIC 'Rabobank Dublin', 
# MAGIC 'Rabobank Madrid', 
# MAGIC 'Rabobank Antwerp', 
# MAGIC 'Rabobank Argentina', 
# MAGIC 'Rabobank Milan', 
# MAGIC 'Rabobank Netherlands', 
# MAGIC 'Rabobank Paris', null) 
# MAGIC
# MAGIC GROUP BY t2.Branche_name, t1.entity_classification

# COMMAND ----------

# DBTITLE 1,Select offboarded cliens to check gcds
# MAGIC %sql
# MAGIC select --t1.`global_co-location_code`, 
# MAGIC count(t1.GCID)
# MAGIC , t1.entity_classification
# MAGIC
# MAGIC from client_client as t1
# MAGIC left join client_OnboardedLocations as t2 on t1.gcid = t2.gcid
# MAGIC left join client_PartyRole as t3 on t1.gcid = t3.gcid
# MAGIC where t1.party_type = 'Legal Entity'
# MAGIC and t2.Offboarding_date > '2017-01-01' and t2.Offboarding_date < '2024-01-01'
# MAGIC and t3.party_role = 'Customer'
# MAGIC and t3.Life_cycle_status = 'Former Client'
# MAGIC and `global_co-location_code` IN ('UTR', 'ARG', 'BEL', 'DEU',
# MAGIC 'ESP', 'FRA' ,'GBR' ,'IRL',
# MAGIC 'ITA', 'KEN', 'UTR')
# MAGIC group by  t1.entity_classification --t1.`global_co-location_code`,

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from client_client where gcid = 15395

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct `Global_CO-location_code`
# MAGIC , count(*) 
# MAGIC from client_client
# MAGIC group by `Global_CO-location_code` ('UTR', 'ARG', 'BEL', 'DEU',
# MAGIC 'ESP', 'FRA' ,'GBR' ,'IRL',
# MAGIC 'ITA', 'KEN', 'UTR')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- query all parties that have been offboarded, and per location. 
# MAGIC select t2.Branche_name, count(distinct(t1.GCID)) as GCID_COUNT, t1.entity_classification
# MAGIC
# MAGIC from client_client as t1
# MAGIC left join client_OnboardedLocations as t2 on t1.gcid = t2.gcid
# MAGIC left join client_PartyRole as t3 on t1.gcid = t3.gcid
# MAGIC where t1.party_type = 'Legal Entity'
# MAGIC and t2.Offboarding_date > '2017-01-01' and t2.Offboarding_date < '2024-01-01'
# MAGIC and t3.party_role = 'Customer'
# MAGIC and t3.Life_cycle_status = 'Former Client'
# MAGIC
# MAGIC GROUP BY t2.Branche_name, t1.entity_classification

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from client_PartyRole limit 3

# COMMAND ----------

GCC_Branches = (
'Rabobank Antwerp', 
'Rabobank Netherlands', 
'Rabobank London', 
'Rabobank Kenya', 
'Rabobank Netherlands', 
'London', 
'Rabobank Turkey', 
'Rabobank Milan', 
'Rabobank Frankfurt', 
'Italy', 
'Rabobank Dublin', 
'Spain', 
'Rabobank Milan', 
'Rabobank Frankfurt', 
'Rabobank Paris', 
'Rabobank Frankfurt', 
'Rabobank London', 
'Rabobank Madrid', 
'Rabobank Argentina', 
'Rabobank Kenya', 
'Rabobank Dublin', 
'Rabobank Madrid', 
'Rabobank Antwerp', 
'Rabobank Argentina', 
'Rabobank Milan', 
'Rabobank Netherlands', 
'Rabobank Paris')


# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct branche_name from client_OnboardedLocations

# COMMAND ----------

# DBTITLE 1,writing into catalog
#spark.sql("""select Branche_name , count(*) as GCIDS_WITH_OFFBOARDING_DATE from client_OnboardedLocations
#where Offboarding_date > '2017-01-01'
#group by Branche_name""").write.mode('overwrite').saveAsTable(f'{catalogname}.radar.#adhoc_offboardedclients')

# COMMAND ----------


display(spark.sql(f"""SHOW GRANTS ON SCHEMA {catalogname}.radar"""))

# COMMAND ----------

# spark.sql(f"""
# CREATE TABLE {catalogname}.radar.EXT_GCDS_client_OnboardedLocations
# LOCATION 'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_Client/4601/data/'
# """)

# COMMAND ----------


