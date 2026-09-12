# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal :
# MAGIC Take the latest file in GDP and load into Synapse DWH
# MAGIC #### author :
# MAGIC Devi.Chennareddy@rabobank.com
# MAGIC #### Flow of logic
# MAGIC Load EBX tables from GDP, determine its target Synapse table name, and write it to Synapse.

# COMMAND ----------

import os
import re
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
environment=os.environ['ENV']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")


# COMMAND ----------

from RiskShieldUtils import *

# COMMAND ----------


GDPStorage ='edlcorestdeuprod0001'
authenticate_storage_account(GDPStorage)


# COMMAND ----------

#Derive the date for which data has to be processed from GDP
from datetime import datetime, timedelta
ThisDay = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
Today = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DT=' + ThisDay
print (load_dts)

# COMMAND ----------

# DBTITLE 1,Load ebx data into synapse
import pandas as pd
ebx_df = pd.DataFrame({'GDP':[
 'EBX_EXISTGEOCOUNTRYLST',
'EBX_NAICSCODESRISKSCORES',
'EBX_ISO3166COUNTRYCODES',
'EBX_FECCountryRiskList',
'EBX_Industries_NAICS_Mapping',
'EBX_Instellingen',
'EBX_INDUSTRIES_SBI',
'EBX_SanctionsCircumventionRASOilAndGas',
'EBX_SanctionsCircumventionRASUTurnCountries',
'EBX_SanctionsCircumventionRASEconomicallyCriticalGoods',
'EBX_SanctionsCircumventionRASDualUseSectors'
]})

ebx_dataframes = {}

for index, row in ebx_df.iterrows():
   ebx_dataframes =spark.read.parquet(f'abfss://ebx@{GDPStorage}.dfs.core.windows.net/{row.GDP}/1/data/{load_dts}/*.parquet')
   display(row.GDP)

   if row.GDP == 'EBX_EXISTGEOCOUNTRYLST':
      file_name = 'ebx.EBX_Compliance_CAMS_EXISTGEOCOUNTRYLST'
   elif row.GDP == 'EBX_NAICSCODESRISKSCORES':
      file_name = 'ebx.EBX_Compliance_CAMS_NAICSCODESRISKSCORES'
   elif row.GDP == 'EBX_ISO3166COUNTRYCODES':
      file_name = 'ebx.EBX_Countries_ISO3166COUNTRYCODES'
   elif row.GDP == 'EBX_Industries_NAICS_Mapping':
      file_name = 'ebx.EBX_Industries_NAICS_Mapping'
   elif row.GDP == 'EBX_FECCountryRiskList':
      file_name = 'ebx.EBX_FECCountryRiskList'
   elif row.GDP == 'EBX_Instellingen':
      file_name ='ebx.EBX_Instellingen'
   elif row.GDP == 'EBX_INDUSTRIES_SBI':
      file_name ='ebx.EBX_Sector_Codes_INDUSTRIES_SBI'
   elif row.GDP == 'EBX_SanctionsCircumventionRASOilAndGas':
      file_name ='ebx.SanctionsCircumventionNAICS_Oil_Gas'
   elif row.GDP == 'EBX_SanctionsCircumventionRASEconomicallyCriticalGoods':
      file_name = 'ebx.SanctionsCircumventionNAICS_EconomicallyCriticalGoods'
   elif row.GDP =='EBX_SanctionsCircumventionRASDualUseSectors':
      file_name = 'ebx.SanctionsCircumventionNAICS_DualUse Goods'
   else:
      file_name='EBX.SanctionsCircumventionRASUTurnCountries'

   Load_from_Databricks_to_Synapse_Dedicated_SQL_Pool(ebx_dataframes, file_name ,"ebx","overwrite")



