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

# DBTITLE 1,Read stoarge
SALZReadStorage = 'salandingzonefecradarprd'
saradar = 'saradarprod'

authenticate_storage_account(SALZReadStorage)
authenticate_storage_account(saradar)

# COMMAND ----------

from pyspark.sql.functions import *

def rename_columns(df):
    return df.select(
        when(col('_c0').contains("0"), 0)
        .otherwise(
            when(col('_c0').contains("1"), 1)
            .otherwise(
                when(col('_c0').contains("9"), 9)
                .otherwise(col('_c0'))
            )).alias('NR_Record_code'),
    col('_c1').alias('NR_Branch_no'),
    col('_c2').alias('NR_Customer_ID'),
    col('_c3').alias('RS_DealNumber'),
    col('_c4').alias('NR_Account_type'),
    col('_c5').alias('NR_Currency'),
    col('_c6').alias('NR_IBAN'),
    col('_c7').alias('NR_Date_opened'),
    col('_c8').alias('NR_Date_closed'),
    col('_c9').alias('NR_Dormant_flag'),
    col('_c10').alias('NR_DR_Blocked'),
    col('_c11').alias('NR_CR_Blocked'),
    col('_c12').alias('NR_Balance'),
    col('_c13').alias('NR_BRANCH_LCY'),
    col('_c14').alias('RS_BUSINESS_DATE'),
    regexp_replace(col("_c15"), "[')]", "").alias('NR_SOURCE_SYSTEM')
)

# COMMAND ----------

# Get schema for ODS RBB Accounts
# TODO make generic function that gets schema from deleting x amount of first columns

def ReadRiskshieldCSV(filepaths_flexcube_eur_nr_accounts):

 n_skip_rows = 1
 n_skip_rows_from_end = 1

 row_rdd = spark.read.text(filepaths_flexcube_eur_nr_accounts).rdd.zipWithIndex()

 count = row_rdd.count()
 print(filepaths_flexcube_eur_nr_accounts)
 print(count)
 if count>4:
    row_rdd = row_rdd.filter(lambda row: row[1] >= n_skip_rows) \
       .filter(lambda row: row[1] <= (count - n_skip_rows_from_end - n_skip_rows)) \
          .map(lambda row: row[0])
    flexcube_eur_nr_accounts = spark.read.options(delimiter="|").csv(row_rdd)
    df_renamed_flexcube_eur_nr_accounts = rename_columns(flexcube_eur_nr_accounts)
    return df_renamed_flexcube_eur_nr_accounts

# COMMAND ----------

tab = 'FLEXCUBE.EUR.NR_ACCOUNTS'
tab_name = tab.lower().replace('.', '_')

IF_LoadDate=datetime.today() - timedelta(days=1)
IF_LoadDate=IF_LoadDate.strftime('%Y%m%d')
load_current_dt=datetime.today().strftime('%Y%m%d')
table = tab + '_' + IF_LoadDate

filepaths_flexcube_eur_nr_accounts = 'abfss://riskshield-flexcube-eu@salandingzonefecradarprd.dfs.core.windows.net/'+ table + '*.csv'

try:
    final_df = ReadRiskshieldCSV(filepaths_flexcube_eur_nr_accounts)
        
except Exception as e:
    if "PATH_NOT_FOUND" in str(e) or "PathNotFound" in str(e):
        final_df=None
        file_exists='No'
        final_df_count=0
    else:
        raise e
            
if final_df is not None :
    final_df = final_df.withColumn("IF_LoadDate", lit(IF_LoadDate))
    final_df_count=final_df.count()
    file_exists='Yes'

    final_df.coalesce(1).write.parquet("abfss://riskshield-flexcube-eu@salandingzonefecradarprd.dfs.core.windows.net/" +load_current_dt + '/' + tab + '_' + load_current_dt + '.parquet', mode='overwrite')
        
    save_to_saradar_storage_account(final_df, tab_name, 101, environment , IF_LoadDate)  

#Update logs
log_df=spark.createDataFrame([(tab_name,IF_LoadDate, file_exists, final_df_count)],["File_Name","IF_LoadDate","File_Exist","Record_Count"])   
log_df.write.mode("append").saveAsTable('radar.riskshield_logs')

# COMMAND ----------

from datetime import datetime, timedelta
from delta.tables import DeltaTable

load_files = ['flexcube_eur_nr_accounts']

for name in load_files:
    LoadDate = LoadDate = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

    base_path = f"abfss://riskshield@saradarprod.dfs.core.windows.net/{name}/101/data/"
    snapshot_path = f"{base_path}/EDL_LOAD_DTS={LoadDate}/"

    # Load snapshot
    df_snapshot = spark.read.parquet(snapshot_path)
    df_snapshot = df_snapshot.drop(col('IF_LoadDate'))

    HashCols = df_snapshot.columns

    df_snapshot = df_snapshot.withColumn("Hash", lit(sha2(concat_ws("~", *HashCols), 256))) \
        .withColumn("StartDate", to_date(lit(LoadDate), "yyyyMMdd")) \
        .withColumn("EndDate",to_date(lit("9999-12-31"),"yyyy-MM-dd")) \
        .withColumn("Active_status",lit("Y"))        

    df_snapshot = df_snapshot.drop_duplicates(subset = ['Hash'])

    target_path = f"abfss://riskshield@saradarprod.dfs.core.windows.net/{name}_scd2"

    # Check if Delta table exists
    if not DeltaTable.isDeltaTable(spark, target_path):
        df_snapshot.write.format("delta").mode("overwrite").save(target_path)
    else:
        # Load target Delta table
        delta_table = DeltaTable.forPath(spark, target_path)

        delta_table.alias('target').merge(df_snapshot.alias('source'), 'target.Hash = source.Hash and target.Active_status = "Y"') \
                    .whenNotMatchedBySourceUpdate(condition = 'target.Active_status = "Y"', 
                        set = {"target.EndDate": to_date(lit(LoadDate), "yyyyMMdd"), 
                               "target.Active_status": "'N'" 
                               }) \
                    .whenNotMatchedInsertAll().execute()

    print(f"Load successful for {name} on {LoadDate}")

# COMMAND ----------

file_list = ['flexcube_eur_nr_accounts']

for name in file_list:
    table_name = name
    table_name1 = table_name.upper()
    df = spark.read.format('delta').load(f'abfss://riskshield@saradarprod.dfs.core.windows.net/{table_name}_scd2')
    Load_from_Databricks_to_Synapse_Dedicated_SQL_Pool(df, f"SCD2.{table_name1}" , f"{table_name1}_TEMP","overwrite")
