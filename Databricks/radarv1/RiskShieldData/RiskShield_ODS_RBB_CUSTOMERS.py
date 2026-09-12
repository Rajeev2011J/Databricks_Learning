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
            )).alias('Record_code'),
    col('_c1').alias('Branch_no'),
    col('_c2').alias('Customer_ID'),
    col('_c3').alias('Ultimate_parent_ID'),
    col('_c4').alias('Customer_Name'),
    col('_c5').alias('Customer_Address'),
    col('_c6').alias('Customer_City'),
    col('_c7').alias('Customer_State'),
    col('_c8').alias('Customer_Postal_code'),
    col('_c9').alias('Customer_Country'),
    col('_c10').alias('CDD_Risk_Rating'),
    col('_c11').alias('Next_CDD_Review'),
    col('_c12').alias('Country_Risk_ID'),
    col('_c13').alias('Customer_type_ID'),
    col('_c14').alias('Customer_sub_type_ID'),
    col('_c15').alias('SIC_value'),
    col('_c16').alias('SIC_description'),
    col('_c17').alias('SEB_value'),
    col('_c18').alias('SEB_description'),
    col('_c19').alias('Primary_NAICS_value'),
    col('_c20').alias('Primary_NAICS_description'),
    col('_c21').alias('Secondary_NAICS1_value'),
    col('_c22').alias('Secondary_NAICS1_description'),
    col('_c23').alias('Secondary_NAICS2_value'),
    col('_c24').alias('Secondary_NAICS2_description'),
    col('_c25').alias('DUNS_code'),
    col('_c26').alias('Hierarchy_Type_value'),
    col('_c27').alias('Hierarchy_Type_description'),
    col('_c28').alias('Country_of_Residence'),
    col('_c29').alias('Customer_status'),
    col('_c30').alias('True_customer'),
    col('_c31').alias('Asset'),
    col('_c32').alias('Income'),
    col('_c33').alias('Relationship_Manager'),
    regexp_replace(col("_c34"), "[')]", "").alias('Customer_Tax_ID')
)

# COMMAND ----------

# function that gets schema from deleting x amount of first columns

def ReadRiskshieldCSV(filepaths_ods_rbb_customers):

 n_skip_rows = 1
 n_skip_rows_from_end = 1

 row_rdd = spark.read.text(filepaths_ods_rbb_customers).rdd.zipWithIndex()

 count = row_rdd.count()
 print(filepaths_ods_rbb_customers)
 print(count)
 if count>4:
    row_rdd = row_rdd.filter(lambda row: row[1] >= n_skip_rows) \
       .filter(lambda row: row[1] <= (count - n_skip_rows_from_end - n_skip_rows)) \
          .map(lambda row: row[0])
    ods_rbb_customers = spark.read.csv(row_rdd)
    df_renamed_ods_rbb_customers = rename_columns(ods_rbb_customers)
    return df_renamed_ods_rbb_customers

# COMMAND ----------

tab = 'ODS.RBB.CUSTOMERS'
tab_name = tab.lower().replace('.', '_')

IF_LoadDate=datetime.today() - timedelta(days=1)
IF_LoadDate=IF_LoadDate.strftime('%Y%m%d')
load_current_dt=datetime.today().strftime('%Y%m%d')
table = tab + '_' + IF_LoadDate

filepaths_ods_rbb_customers = 'abfss://riskshield-ods-rbb@salandingzonefecradarprd.dfs.core.windows.net/'+ table + '*.csv'

try:
    final_df = ReadRiskshieldCSV(filepaths_ods_rbb_customers)
        
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

    final_df.coalesce(1).write.parquet("abfss://riskshield-ods-rbb@salandingzonefecradarprd.dfs.core.windows.net/" +load_current_dt + '/' + tab + '_' + load_current_dt + '.parquet', mode='overwrite')
        
    save_to_saradar_storage_account(final_df, tab_name, 101, environment , IF_LoadDate)  

#Update logs
log_df=spark.createDataFrame([(tab_name,IF_LoadDate, file_exists, final_df_count)],["File_Name","IF_LoadDate","File_Exist","Record_Count"])   
log_df.write.mode("append").saveAsTable('radar.riskshield_logs')
