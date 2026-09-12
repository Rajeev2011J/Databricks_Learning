# Databricks notebook source
# generic imports
import os
import re
from datetime import datetime, timedelta
import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.types import StructType
from RiskShieldUtils import *

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,Read stoarge
# SALZReadStorage Configuartions
SALZReadStorage = 'salandingzonefecradarprd'

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

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
            )).alias('Record_code'),
    col('_c1').alias('Branch_no'),
    col('_c2').alias('Customer_ID'),
    col('_c3').alias('Account_number'),
    col('_c4').alias('Account_type'),
    col('_c5').alias('Currency_ID'),
    col('_c6').alias('IBAN_id'),
    col('_c7').alias('Closed_flag'),
    col('_c8').alias('Date_open'),
    col('_c9').alias('Date_closed'),
    col('_c10').alias('Dormant_flag'),
    col('_c11').alias('Blocked_DR'),
    col('_c12').alias('Blocked_CR'),
    col('_c13').alias('Expected_Payment_Frequency'),
    col('_c14').alias('Expected_Payment_Amount'),
    col('_c15').alias('Drawdown_Amount'),
    col('_c16').alias('Maturity_Date'),
    regexp_replace(col("_c17"), "[')]", "").alias('Linked_Nominated_Account_number')
)

# COMMAND ----------

# Function that gets schema from deleting x amount of first columns

def ReadRiskshieldCSV(filepaths_aps_lnd_acc):

 n_skip_rows = 1
 n_skip_rows_from_end = 1

 row_rdd = spark.read.text(filepaths_aps_lnd_acc).rdd.zipWithIndex()

 count = row_rdd.count()
 print(count)

 if count>2:
    row_rdd = row_rdd.filter(lambda row: row[1] >= n_skip_rows) \
       .filter(lambda row: row[1] <= (count - n_skip_rows_from_end - n_skip_rows)) \
          .map(lambda row: row[0])
    aps_lnd_acc = spark.read.csv(row_rdd)
    df_renamed_aps_lnd_acc = rename_columns(aps_lnd_acc)
    return df_renamed_aps_lnd_acc

# COMMAND ----------

tab = 'APS.LND.ACC'
tab_name = tab.lower().replace('.', '_')

IF_LoadDate=datetime.today() - timedelta(days=1)
IF_LoadDate=IF_LoadDate.strftime('%Y%m%d')
load_current_dt=datetime.today().strftime('%Y%m%d')
table = tab + '_' + IF_LoadDate

filepaths_aps_lnd_acc = 'abfss://riskshield-flexcube-aps@salandingzonefecradarprd.dfs.core.windows.net/'+ table + '*.csv'

try:
    final_df = ReadRiskshieldCSV(filepaths_aps_lnd_acc)
        
except Exception as e:
    if "PATH_NOT_FOUND" in str(e) or "PathNotFound" in str(e):
        final_df=None
        file_exists='No'
        final_df_count=0
    else:
        raise e
            
if final_df is not None :
    final_df = final_df.withColumn("IF_LoadDate", F.lit(IF_LoadDate))
    final_df_count=final_df.count()
    file_exists='Yes'

    final_df.coalesce(1).write.parquet("abfss://riskshield-flexcube-aps@salandingzonefecradarprd.dfs.core.windows.net/" +load_current_dt + '/' + tab + '_' + load_current_dt + '.parquet', mode='overwrite')
        
    save_to_saradar_storage_account(final_df, tab_name, 101, environment , IF_LoadDate)  

#Update logs
log_df=spark.createDataFrame([(tab_name,IF_LoadDate, file_exists, final_df_count)],["File_Name","IF_LoadDate","File_Exist","Record_Count"])   
log_df.write.mode("append").saveAsTable('radar.riskshield_logs')
