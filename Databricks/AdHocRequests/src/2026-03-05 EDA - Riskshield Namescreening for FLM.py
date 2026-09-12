# Databricks notebook source
# MAGIC %md
# MAGIC ## Goal:
# MAGIC To do data exploration for generating automated selections of Namescreening alerts
# MAGIC
# MAGIC #### Flow of logic:
# MAGIC - connect to tables
# MAGIC - read all of them
# MAGIC - get unique values for columns per domain value
# MAGIC - allign on columns that should match the selection criteria for FLM
# MAGIC
# MAGIC ##### PBI related
# MAGIC PBI: 13917138: [FLM automated selection] Select the scope of Namescreening alerts from the Riskshield GDP data and apply selection logic
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### Selection logic requirements
# MAGIC Percentage of alerts per portfolio
# MAGIC UK: 7%
# MAGIC NL 3.5%
# MAGIC FI 3,5%
# MAGIC 1. Client Groups: Minimize overlaps
# MAGIC 2. NS Close codes classifications: include all different classifications that can be identified
# MAGIC 3. Inclusion of different Match Score percentages
# MAGIC  --> Go for an even spread at first, but prepare ourselves for a situation where we want more risk-based on percentage. 
# MAGIC 4. Inclusion of Legal entities and natural persons
# MAGIC 5. Over Night Screening team: An even spread of Analysts in the sample
# MAGIC 6. Alerts which are out of scope of screening teams/signals team excluded from the data received.
# MAGIC 7. Percentage per portfolio
# MAGIC a) 7% of population of each cathegory for UK and BP : Sanction/AM/PEP
# MAGIC b) 3.5% of population of each cathergory for NL and FI: Sanction/AM/PEP
# MAGIC 8. Exclude NSAAS alerts. (column 'Filename' = NSAAS )
# MAGIC 9. Exclude "no match/not relevant"* (column = .. )
# MAGIC

# COMMAND ----------

import os
import re
from datetime import datetime, timedelta

#Importing pyspark libraries and modules
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark import StorageLevel
import pandas as pd

# COMMAND ----------


application_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
tenant_id = os.environ['TENANT_ID']
environment=os.environ['ENV']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{application_id}")




# COMMAND ----------

def authenticate_storage_account(write_storage):
    # This method is for Configuring Spark to access SARADAR Storage account using OAuth authentication
    spark.conf.set("fs.azure.account.auth.type."+write_storage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+write_storage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+write_storage+".dfs.core.windows.net", ""+application_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+write_storage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+write_storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")

    print('Connection to ' + write_storage +' storage account is successful.')

    return 

# COMMAND ----------

authenticate_storage_account(ReadStorage)

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

load_df = pd.DataFrame({'GDPname':[
'rcmevent_v_gdp'
,'trx_v_alert_gdp'
,'trx_v_report_gdp'
]})
 
# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://rs-kjr@{ReadStorage}.dfs.core.windows.net/{row.GDPname}/1/data/').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# MAGIC  %sql
# MAGIC  select distinct CLOSECODESHORTNAME
# MAGIC  from rcmevent_v_gdp 
# MAGIC  --WHERE TENANT_NAME LIKE '%Wholesale%'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * 
# MAGIC from rcmevent_v_gdp 
# MAGIC WHERE TENANT_NAME LIKE '%Wholesale%'
# MAGIC AND CUSTOMER_ID LIKE '%RNP%'
# MAGIC limit 10
# MAGIC -- CUSTOMER_ID,? 
# MAGIC -- type would be RNP, RLE or number (but CUSTOMER TYPE will distinguish between Customer NP and Customer LE)

# COMMAND ----------

# MAGIC %md
# MAGIC - UK hub: Clients and RP related to where assigned ReviewLocation (via E&A portfolio) is: Corp Hub UK
# MAGIC   - CH London
# MAGIC   - Corporate Hub (not in current list - to ask Data Steward of system)
# MAGIC - NL hub: Clients and RP related to where assigned ReviewLocation (via E&A portfolio) is: Corp Hub NL
# MAGIC   - CH Dublin
# MAGIC   - NL Wholesale (comes with Dublin included)
# MAGIC - FI hub: Clients and RP related to where assigned ReviewLocation (via E&A portfolio) is: FI Hub
# MAGIC   - Financial Institutions
# MAGIC - Asia:
# MAGIC   - China 1% (might change)
# MAGIC   - Hong kong 1% (might change)
# MAGIC   - Singapore. 5%
# MAGIC - Other
# MAGIC   - Rabo Foundation
# MAGIC   - Rabo Impact Foundation

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct(client_Name) from trx_v_alert_gdp

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from trx_v_alert_gdp limit 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct Filename from trx_v_alert_gdp Where Filename like '%NSAAS%'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from trx_v_report_gdp limit 10

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


