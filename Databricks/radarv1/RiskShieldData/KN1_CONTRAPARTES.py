# Databricks notebook source
# generic imports
import os
import re
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from RiskShieldUtils import *

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

env = os.environ['ENV']
if env.lower()!='prod':
    print("Execution stopped Storage account does not exist")
    dbutils.notebook.exit("Terminated early: Non-prod workspace")

# COMMAND ----------

# DBTITLE 1,Read stoarge
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
# saradar = 'saradarprod'

authenticate_storage_account(GIC_ReadStorage)
# authenticate_storage_account(saradar)

# COMMAND ----------

# DBTITLE 1,Write to Synapse
List = ['ADKYC_CONTRAPARTES_TITULARES','ADRBB_CONTRAPARTES_QUALIFICACAO_PEP']
for tab in List:
    load_current_dt=datetime.today().strftime('%Y%m%d')

    base_path = f"abfss://kn1@edlcorestdbrprod0001.dfs.core.windows.net/{tab}/1/data/"
    snapshot_path = f"{base_path}/LOADED_DTS={load_current_dt}*/"
    try:
        final_df = spark.read.parquet(snapshot_path)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "PathNotFound" in str(e):
            final_df=None
            file_exists='No'
            final_df_count=0
        else:
            raise e
            
    if final_df is not None :
            
        final_df_count=final_df.count()
        file_exists='Yes'

        tab = tab[:-4] if '_PEP' in tab else tab
        Load_from_Databricks_to_Synapse_Dedicated_SQL_Pool(final_df, f"GEN.KN1_{tab}" , f"{tab}_TEMP","overwrite")

        print(f"Table: GEN.KN1_{tab} written successfully on {load_current_dt}",end='\n')

    #Update logs
    log_df=spark.createDataFrame([(tab,load_current_dt, file_exists, final_df_count)],["File_Name","IF_LoadDate","File_Exist","Record_Count"])   
    log_df.write.mode("append").saveAsTable('radar.riskshield_logs')

# COMMAND ----------

List = ['ADKYC_PROCESSOS','ADRBB_DOMINIOS_QUESTOES']
for tab in List:
    load_current_dt=datetime.today().strftime('%Y%m%d')

    base_path = f"abfss://kn1@edlcorestdbrprod0001.dfs.core.windows.net/{tab}/1/data/"
    snapshot_path = f"{base_path}/LOADED_DTS={load_current_dt}*/"
    try:
        final_df = spark.read.parquet(snapshot_path)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "PathNotFound" in str(e):
            final_df=None
            file_exists='No'
            final_df_count=0
        else:
            raise e
            
    if final_df is not None :
            
        final_df_count=final_df.count()
        file_exists='Yes'

        Load_from_Databricks_to_Synapse_Dedicated_SQL_Pool(final_df, f"KN1.{tab}" , f"{tab}_TEMP","overwrite")

        print(f"Table: KN1.{tab} written successfully on {load_current_dt}",end='\n')

    #Update logs
    log_df=spark.createDataFrame([(tab,load_current_dt, file_exists, final_df_count)],["File_Name","IF_LoadDate","File_Exist","Record_Count"])   
    log_df.write.mode("append").saveAsTable('radar.riskshield_logs')

# COMMAND ----------

# from datetime import datetime, timedelta
# from delta.tables import DeltaTable

# load_files = ['ADKYC_CONTRAPARTES_TITULARES','ADRBB_CONTRAPARTES_QUALIFICACAO_PEP']

# for name in load_files:
#     LoadDate = datetime.today().strftime('%Y%m%d')

#     base_path = f"abfss://kn1@edlcorestdbrprod0001.dfs.core.windows.net/{object['Object_name']}/1/data/"
#     snapshot_path = f"{base_path}/LOADED_DTS={LoadDate}*/"

#     # Load snapshot
#     df_snapshot = spark.read.parquet(snapshot_path)
#     df_snapshot = df_snapshot.drop(col('EDL_LOAD_DTS'),col('EDL_ACT_DTS'))

#     HashCols = df_snapshot.columns

#     df_snapshot = df_snapshot.withColumn("Hash", lit(sha2(concat_ws("~", *HashCols), 256))) \
#         .withColumn("StartDate", to_date(lit(LoadDate), "yyyyMMdd")) \
#         .withColumn("EndDate",to_date(lit("9999-12-31"),"yyyy-MM-dd")) \
#         .withColumn("Active_status",lit("Y"))        

#     df_snapshot = df_snapshot.drop_duplicates(subset = ['Hash'])

#     target_path = f"abfss://kn1@saradarprod.dfs.core.windows.net/{name}_scd2"

#     # Check if Delta table exists
#     if not DeltaTable.isDeltaTable(spark, target_path):
#         df_snapshot.write.format("delta").mode("overwrite").save(target_path)
#     else:
#         # Load target Delta table
#         delta_table = DeltaTable.forPath(spark, target_path)

#         delta_table.alias('target').merge(df_snapshot.alias('source'), 'target.Hash = source.Hash and target.Active_status = "Y"') \
#                     .whenNotMatchedBySourceUpdate(condition = 'target.Active_status = "Y"', 
#                         set = {"target.EndDate": to_date(lit(LoadDate), "yyyyMMdd"), 
#                                "target.Active_status": "'N'" 
#                                }) \
#                     .whenNotMatchedInsertAll().execute()

#     print(f"Load successful for {name} on {LoadDate}")

# COMMAND ----------

# file_list =  ['ADKYC_CONTRAPARTES_TITULARES','ADRBB_CONTRAPARTES_QUALIFICACAO_PEP']

# for name in file_list:
#     table_name = name
#     table_name1 = table_name.upper()
#     df = spark.read.format('delta').load(f'abfss://kn1@saradarprod.dfs.core.windows.net/{table_name}_scd2')
#     Load_from_Databricks_to_Synapse_Dedicated_SQL_Pool(df, f"SCD2.KN1_{table_name1}" , f"{table_name1}_TEMP","overwrite")
