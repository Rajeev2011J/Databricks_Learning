# Databricks notebook source
# generic imports
import os
import re
from datetime import datetime
from datetime import timedelta
import pandas as pd
from datetime import datetime, timedelta
import pyspark.sql.functions as f
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
            )).alias('Record_code'),
    col('_c1').alias('Branch_no'),
    col('_c2').alias('Customer_ID_CIF_number'),
    col('_c3').alias('Account_number'),
    col('_c4').alias('Timestamp'),
    col('_c5').alias('Source_code'),
    col('_c6').alias('Transaction_type'),
    col('_c7').alias('Transaction_ID'),
    col('_c8').alias('Original_currency_code'),
    col('_c9').alias('Original_transaction_amount'),
    col('_c10').alias('Credit_debit'),
    col('_c11').alias('Department'),
    col('_c12').alias('Counterparty_bank_name'),
    col('_c13').alias('Counterparty_bank_country_code'),
    col('_c14').alias('Counterparty_account_number'),
    col('_c15').alias('Counterparty_name'),
    col('_c16').alias('Country_of_ordering_party'),
    col('_c17').alias('Country_of_beneficiary'),
    col('_c18').alias('Item_type'),
    col('_c19').alias('Description_part_1'),
    col('_c20').alias('Description_part_2'),
    col('_c21').alias('Intermediary_bank_name_1'),
    col('_c22').alias('Intermediary_bank_BIC_1'),
    col('_c23').alias('Intermediary_bank_Address_1'),
    col('_c24').alias('Intermediary_bank_country_code_1'),
    col('_c25').alias('Intermediary_bank_name_2'),
    col('_c26').alias('Intermediary_bank_BIC_2'),
    col('_c27').alias('Intermediary_bank_Address_2'),
    col('_c28').alias('Intermediary_bank_country_code_2'),
    col('_c29').alias('Intermediary_bank_name_3'),
    col('_c30').alias('Intermediary_bank_BIC_3'),
    col('_c31').alias('Intermediary_bank_Address_3'),
    col('_c32').alias('Intermediary_bank_country_code_3'),
    col('_c33').alias('Intermediary_bank_name_4'),
    col('_c34').alias('Intermediary_bank_BIC_4'),
    col('_c35').alias('Intermediary_bank_Address_4'),
    col('_c36').alias('Intermediary_bank_country_code_4'),
    col('_c37').alias('Originator_bank_name'),
    col('_c38').alias('Originator_bank_BIC'),
    col('_c39').alias('Originator_bank_Address'),
    col('_c40').alias('Originator_bank_country_code'),
    col('_c41').alias('Beneficiary_bank_name'),
    col('_c42').alias('Beneficiary_bank_BIC'),
    col('_c43').alias('Beneficiary_bank_Address'),
    col('_c44').alias('Beneficiary_bank_country_code'),
    col('_c45').alias('Whole_Swift_message'),
    regexp_replace(col("_c46"), "[')]", "").alias('Counterparty_Tax_ID')
)

# COMMAND ----------

# Function that gets schema from deleting x amount of first columns

def ReadRiskshieldCSV(filepaths_aps_lnd_traf):

 n_skip_rows = 1
 n_skip_rows_from_end = 1

 row_rdd = spark.read.text(filepaths_aps_lnd_traf).rdd.zipWithIndex()

 count = row_rdd.count()
 print(count)

 if count>2:
    row_rdd = row_rdd.filter(lambda row: row[1] >= n_skip_rows) \
       .filter(lambda row: row[1] <= (count - n_skip_rows_from_end - n_skip_rows)) \
          .map(lambda row: row[0])
    aps_lnd_traf = spark.read.csv(row_rdd)
    df_renamed_aps_lnd_traf = rename_columns(aps_lnd_traf)
    return df_renamed_aps_lnd_traf

# COMMAND ----------

tab = 'APS.LND.TRAF'
tab_name = tab.lower().replace('.', '_')

IF_LoadDate=datetime.today() - timedelta(days=1)
IF_LoadDate=IF_LoadDate.strftime('%Y%m%d')
load_current_dt=datetime.today().strftime('%Y%m%d')
table = tab + '_' + IF_LoadDate

filepaths_aps_lnd_traf = 'abfss://riskshield-flexcube-aps@salandingzonefecradarprd.dfs.core.windows.net/'+ table + '*.csv'

try:
    final_df = ReadRiskshieldCSV(filepaths_aps_lnd_traf)
        
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
