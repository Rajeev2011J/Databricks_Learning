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
            )
        ).alias('Record_code'),
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
    col('_c34').alias('Customer_Tax_ID'),
    col('_c35').alias('DBA_Name'),
    col('_c36').alias('Creation_Date'),
    col('_c37').alias('Deactivation_Date'),
    col('_c38').alias('Date_of_Birth'),
    when(
        regexp_replace(col("_c39"), """[')"]""", "") == '', 
        lit(None).cast('string')
    ).otherwise(
        regexp_replace(col("_c39"), """[')]""", "")
    ).alias('Occupation')
)

# COMMAND ----------

# function that gets schema from deleting x amount of first columns

def ReadRiskshieldCSV(filepaths_t24_customers):

 n_skip_rows = 1
 n_skip_rows_from_end = 1

 row_rdd = spark.read.text(filepaths_t24_customers).rdd.zipWithIndex()

 count = row_rdd.count()
 print(count)
 if count>4:
    row_rdd = row_rdd.filter(lambda row: row[1] >= n_skip_rows) \
       .filter(lambda row: row[1] <= (count - n_skip_rows_from_end - n_skip_rows)) \
          .map(lambda row: row[0])
    t24_customers = spark.read.csv(row_rdd)
    df_renamed_t24_customers= rename_columns(t24_customers)
    return df_renamed_t24_customers

# COMMAND ----------


List =['T24.RD.AU.CUSTOMERS', 'T24.RD.NZ.CUSTOMERS']
for tab in List:
    tab_name = tab.lower().replace('.', '_')
    IF_LoadDate=datetime.today() - timedelta(days=1)
    IF_LoadDate=IF_LoadDate.strftime('%Y%m%d')
    load_current_dt=datetime.today().strftime('%Y%m%d')
    table = tab + '_' + IF_LoadDate

    print(f"Processing table: {table}")

    files = dbutils.fs.ls("abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/")
    matching_files = [f for f in files if table in f.name and f.name.endswith(".CSV")]

    if not matching_files:
        print(f"No files found for {table}")
        continue

    # Pick the latest file based on filename
    latest_file = sorted(matching_files, key=lambda x: x.name, reverse=True)[0]
    print("Latest file path:", latest_file.path)
    try:
        final_df = ReadRiskshieldCSV(latest_file.path)
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


        final_df.coalesce(1).write.parquet("abfss://riskshield-t24@salandingzonefecradarprd.dfs.core.windows.net/" +load_current_dt + '/' + tab + '_' + load_current_dt + '.parquet', mode='overwrite')
        
        save_to_saradar_storage_account(final_df, tab_name, 101, environment , IF_LoadDate)  

    #Update logs
    log_df=spark.createDataFrame([(tab_name,IF_LoadDate, file_exists, final_df_count)],["File_Name","IF_LoadDate","File_Exist","Record_Count"])   
    log_df.write.mode("append").saveAsTable('radar.riskshield_logs')

# COMMAND ----------

from datetime import datetime, timedelta
from delta.tables import DeltaTable

load_files = ['t24_rd_au_customers','t24_rd_nz_customers']

for name in load_files:
    LoadDate = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

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

file_list = ['t24_rd_au_customers','t24_rd_nz_customers']

for name in file_list:
    table_name = name
    table_name1 = table_name.upper()
    df = spark.read.format('delta').load(f'abfss://riskshield@saradarprod.dfs.core.windows.net/{table_name}_scd2')
    Load_from_Databricks_to_Synapse_Dedicated_SQL_Pool(df, f"SCD2.{table_name1}" , f"{table_name1}_TEMP","overwrite")
