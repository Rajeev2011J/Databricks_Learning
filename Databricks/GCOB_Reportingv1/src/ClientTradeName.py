# Databricks notebook source
import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

clienttradename_dataobject = 'mi_clienttradename'

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_client_structure_GUI',
'party_client_structure',
'party_trade_name',
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW mi_clienttradename AS
# MAGIC select Distinct 'BusinessDate' AS BusinessDate,ClientCaseId as CaseId,ClientGcobID as GcobId,ClientFullLegalName as FullLegalName,ClientTradeName as TradeName,IsLatestApprovedVersionOfClient,ClientLifeCycleName,SourceSystem As SourceSystem,CaseStatusName,ReviewTypeName,clienttype,SourceClient from party_client_structure_gui where caseStatusName!='Cancelled' 
# MAGIC

# COMMAND ----------

df_mi_clienttradename=spark.table('mi_clienttradename')
df_mi_clienttradename=df_mi_clienttradename.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data

# COMMAND ----------

save_to_saradar_storage_account(df_mi_clienttradename, clienttradename_dataobject)

# COMMAND ----------

# %sql
# --drop table IF EXISTS radar.ClientTradeReport
# drop table IF EXISTS radar.mi_clienttradename

# COMMAND ----------

# spark.sql('select * from mi_clienttradename').write.mode('overwrite').saveAsTable('radar.mi_clienttradename')
