# Databricks notebook source
# generic imports
import os
import re
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField
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
            )).alias('RS_BRANCH'),
    col('_c1').alias('RS_DEALNUMBER'),
    col('_c2').alias('RS_BUSINESSDATE'),
    col('_c3').alias('RS_COMMODITY'),
    col('_c4').alias('RS_AMOUNT'),
    col('_c5').alias('RS_LCY_AMT'),
    col('_c6').alias('RS_DEALTDATE'),
    col('_c7').alias('RS_VALUEDATE'),
    col('_c8').alias('RS_MATURITYDATE'),
    col('_c9').alias('RS_TRADINGENTITY'),
    col('_c10').alias('RS_COUNTERPARTY'),
    col('_c11').alias('RS_IDENTIFIER'),
    col('_c12').alias('RS_PARTYTYPE'),
    col('_c13').alias('RS_DEALTYPE'),
    col('_c14').alias('RS_DEALTYPE_DESCRIPTION'),
    col('_c15').alias('RS_DEALER'),
    col('_c16').alias('RS_BUSINESSAREA'),
    col('_c17').alias('RS_SOURCESYSTEM'),
    col('_c18').alias('RS_CANCELLATIONSTATUS'),
    col('_c19').alias('RS_MODULE'),
    col('_c20').alias('RS_RELATEDCONTRACT'),
    col('_c21').alias('NR_FREQUENCY'),
    regexp_replace(col("_c22"), "[')]", "").alias('NR_PAY_AMOUNT') )

# COMMAND ----------

# Get schema for ODS RBB Accounts
# TODO make generic function that gets schema from deleting x amount of first columns

def ReadRiskshieldCSV(filepaths_flexcube_eur_nr_trades):

 n_skip_rows = 1
 n_skip_rows_from_end = 1

 row_rdd = spark.read.text(filepaths_flexcube_eur_nr_trades).rdd.zipWithIndex()

 count = row_rdd.count()
 print(filepaths_flexcube_eur_nr_trades)
 print(count)
 if count>4:
    row_rdd = row_rdd.filter(lambda row: row[1] >= n_skip_rows) \
       .filter(lambda row: row[1] <= (count - n_skip_rows_from_end - n_skip_rows)) \
          .map(lambda row: row[0])
    flexcube_eur_nr_trades = spark.read.options(delimiter="|").csv(row_rdd)
    df_renamed_flexcube_eur_nr_trades = rename_columns(flexcube_eur_nr_trades)
    return df_renamed_flexcube_eur_nr_trades

# COMMAND ----------

tab = 'FLEXCUBE.EUR.NR_TRADES'
tab_name = tab.lower().replace('.', '_')

IF_LoadDate=datetime.today() - timedelta(days=1)
IF_LoadDate=IF_LoadDate.strftime('%Y%m%d')
load_current_dt=datetime.today().strftime('%Y%m%d')
table = tab + '_' + IF_LoadDate

filepaths_flexcube_eur_nr_trades = 'abfss://riskshield-flexcube-eu@salandingzonefecradarprd.dfs.core.windows.net/'+ table + '*.csv'

try:
    final_df = ReadRiskshieldCSV(filepaths_flexcube_eur_nr_trades)
        
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
