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
            )).alias('NR_RECORD_CODE'),
    col('_c1').alias('NR_BRANCH'),
    col('_c2').alias('RS_DEALNUMBER'),
    col('_c3').alias('NR_SERIAL_NUMBER'),
    col('_c4').alias('TM_TXN_DT'),
    col('_c5').alias('RS_TRADING_ENTITY'),
    col('_c6').alias('RS_IDENTIFIER'),
    col('_c7').alias('NR_CUSTOMER'),
    col('_c8').alias('RS_COUNTERPARTY'),
    col('_c9').alias('RS_PARTY_TYPE'),
    col('_c10').alias('NR_ACCOUNT_NUMBER'),
    col('_c11').alias('TM_ACCOUNT_DESCR'),
    col('_c12').alias('TM_ACCOUNT_CLASS'),
    col('_c13').alias('NR_VALUE_DATE'),
    col('_c14').alias('NR_SOURCE_CODE'),
    col('_c15').alias('RS_DEAL_TYPE_DESCRIPTION'),
    col('_c16').alias('NR_TRANSACTION_TYPE'),
    col('_c17').alias('NR_ORIG_CCY'),
    col('_c18').alias('NR_ORIG_TXN_AMOUNT'),
    col('_c19').alias('NR_CRDR_IND'),
    col('_c20').alias('NR_COUNTRY_CCY'),
    col('_c21').alias('NR_BBASE_CCY'),
    col('_c22').alias('NR_DEPARTMENT'),
    col('_c23').alias('TM_PRODTYPE'),
    col('_c24').alias('NR_COUNTERPARTY_BANK'),
    col('_c25').alias('NR_COUNTERPARTY_COUNTRY'),
    col('_c26').alias('NR_COUNTERPARTY_AC_NO'),
    col('_c27').alias('NR_COUNTERPARTY_NAME'),
    col('_c28').alias('NR_ORDERING_PARTY_COUNTRY'),
    col('_c29').alias('NR_BENEFICIARY_COUNTRY'),
    col('_c30').alias('NR_PAYMENT_DETAILS1'),
    col('_c31').alias('NR_PAYMENT_DETAILS2'),
    col('_c32').alias('NR_ORIGINATOR_BANK_NAME'),
    col('_c33').alias('NR_BENEFICIARY_BANK_NAME'),
    col('_c34').alias('RS_BUSINESS_DATE'),
    col('_c35').alias('RS_MOVEMENT_TYPE'),
    col('_c36').alias('SI_OUR_CORRESPONDENT'),
    col('_c37').alias('SI_RECEIVER'),
    col('_c38').alias('SI_RCVR_CORRESP1'),
    col('_c39').alias('SI_RCVR_CORRESP2'),
    col('_c40').alias('SI_RCVR_CORRESP3'),
    col('_c41').alias('SI_RCVR_CORRESP4'),
    col('_c42').alias('SI_RCVR_CORRESP5'),
    col('_c43').alias('SI_INTERMEDIARY1'),
    col('_c44').alias('SI_INTERMEDIARY2'),
    col('_c45').alias('SI_INTERMEDIARY3'),
    col('_c46').alias('SI_INTERMEDIARY4'),
    col('_c47').alias('SI_INTERMEDIARY5'),
    col('_c48').alias('SI_ACC_WITH_INSTN1'),
    col('_c49').alias('SI_ACC_WITH_INSTN2'),
    col('_c50').alias('SI_ACC_WITH_INSTN3'),
    col('_c51').alias('SI_ACC_WITH_INSTN4'),
    col('_c52').alias('SI_SNDR_TO_RCVR1'),
    col('_c53').alias('SI_SNDR_TO_RCVR2'),
    col('_c54').alias('SI_SNDR_TO_RCVR3'),
    col('_c55').alias('SI_SNDR_TO_RCVR4'),
    col('_c56').alias('SI_SNDR_TO_RCVR5'),
    col('_c57').alias('SI_SNDR_TO_RCVR6'),
    col('_c58').alias('SI_ORDERING_INSTIT1'),
    col('_c59').alias('SI_ORDERING_INSTIT2'),
    col('_c60').alias('SI_ORDERING_INSTIT3'),
    col('_c61').alias('SI_ORDERING_INSTIT4'),
    col('_c62').alias('SI_ORDERING_INSTIT5'),
    col('_c63').alias('SI_BENEF_INSTIT1'),
    col('_c64').alias('SI_BENEF_INSTIT2'),
    col('_c65').alias('SI_BENEF_INSTIT3'),
    col('_c66').alias('SI_BENEF_INSTIT4'),
    col('_c67').alias('SI_ULT_BENEFICIARY1'),
    col('_c68').alias('SI_ULT_BENEFICIARY2'),
    col('_c69').alias('SI_ULT_BENEFICIARY3'),
    col('_c70').alias('SI_ULT_BENEFICIARY4'),
    col('_c71').alias('SI_ULT_BENEFICIARY5'),
    col('_c72').alias('SI_ORDERING_CUST1'),
    col('_c73').alias('SI_ORDERING_CUST2'),
    col('_c74').alias('SI_ORDERING_CUST3'),
    col('_c75').alias('SI_ORDERING_CUST4'),
    col('_c76').alias('SI_ORDERING_CUST5'),
    col('_c77').alias('FT_INTERNAL_REMARKS'),
    regexp_replace(col("_c78"), "[')]", "").alias('FT_UNIQUE_ETE_TXNREF')
)

# COMMAND ----------

#function that gets schema from deleting x amount of first columns

def ReadRiskshieldCSV(filepaths_flexcube_eur_nr_account_traffic):

 n_skip_rows = 1
 n_skip_rows_from_end = 1

 row_rdd = spark.read.text(filepaths_flexcube_eur_nr_account_traffic).rdd.zipWithIndex()
 count = row_rdd.count()

 if count>4:
    row_rdd = row_rdd.filter(lambda row: row[1] >= n_skip_rows) \
       .filter(lambda row: row[1] <= (count - n_skip_rows_from_end - n_skip_rows)) \
          .map(lambda row: row[0])
    flexcube_eur_nr_account_traffic = spark.read.options(delimiter="|").csv(row_rdd)
    df_renamed_flexcube_eur_nr_account_traffic = rename_columns(flexcube_eur_nr_account_traffic)
    return df_renamed_flexcube_eur_nr_account_traffic

# COMMAND ----------

tab = 'FLEXCUBE.EUR.NR_ACCOUNT.TRAFFIC'
tab_name = tab.lower().replace('.', '_')

IF_LoadDate=datetime.today() - timedelta(days=1)
IF_LoadDate=IF_LoadDate.strftime('%Y%m%d')
load_current_dt=datetime.today().strftime('%Y%m%d')
table = tab + '_' + IF_LoadDate

filepaths_flexcube_eur_nr_account_traffic = 'abfss://riskshield-flexcube-eu@salandingzonefecradarprd.dfs.core.windows.net/'+ table + '*.csv'

try:
    final_df = ReadRiskshieldCSV(filepaths_flexcube_eur_nr_account_traffic)
        
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
