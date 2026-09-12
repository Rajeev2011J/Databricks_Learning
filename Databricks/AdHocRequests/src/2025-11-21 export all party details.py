# Databricks notebook source
import os
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta, timezone
from pyspark.sql.functions import col
import requests
import json

#Local File Import
from RadarUtils import *

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
print(date_parameter)
load_dts = 'LOAD_DT=' + date_parameter + '*'

# COMMAND ----------

GDP_EU_Storage_Account = os.environ['GDP_STORAGE_NAME']
GNS_STORAGE_ACCOUNT= os.environ['GNS_STORAGE_ACCOUNT']
GNS_CONTAINER= os.environ['GNS_CONTAINER']
environment=os.environ['ENV']
SARadarStorage = f'saradar{environment}'

# COMMAND ----------

authenticate_storage_account(SARadarStorage)
authenticate_storage_account(GDP_EU_Storage_Account)

# COMMAND ----------

load_df = pd.DataFrame({'GDPname':[
'party_client_structure_GUI',
'party_case_client_details',
'party_AllPartyDetails',
'party_products_and_sevices',
'party_Alias',
'Party_AllParty_LocationCoverage'
]})
 
# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{GDP_EU_Storage_Account}.dfs.core.windows.net/CaseService/{row.GDPname}/102/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

GDP_EU_Storage_Account

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_case_client_details where IsLatestApprovedVersionofClient = True

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_AllPartyDetails limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from party_AllPartyDetails

# COMMAND ----------


