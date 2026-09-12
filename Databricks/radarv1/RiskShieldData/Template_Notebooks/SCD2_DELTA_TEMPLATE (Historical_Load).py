# Databricks notebook source
# MAGIC %md
# MAGIC This template can be used to load scd2 files once all the files are present in parquet <br>
# MAGIC Official documentation: https://docs.delta.io/latest/delta-update.html#-merge-in-scd-type-2&language-sql

# COMMAND ----------

# DBTITLE 1,Importing Libraries
# generic imports
import os
import re
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DateType, ArrayType
from RiskShieldUtils import *
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Connection Details
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,Outh Connection
saradar = 'saradarprod'
authenticate_storage_account(saradar)

# COMMAND ----------

def file_list_dates(table, initial_loaddate, last_loaddate):
    file = dbutils.fs.ls(f'abfss://riskshield@saradarprod.dfs.core.windows.net/{table}/101/data/')
    
    date_format = "%Y%m%d"

    # Extract and clean dates
    dates = [datetime.strptime(re.split(r"[=,/]", f.name)[1][:8], date_format) for f in file]

    # Sort dates
    dates.sort()
    
    # Get initial and last date
    initial_date, last_date = datetime.strptime(initial_loaddate, date_format), datetime.strptime(last_loaddate, date_format)

    # Generate all dates between initial_date and last_date
    all_dates = set(initial_date + timedelta(days=x) for x in range((last_date - initial_date).days + 1))
    
    # Find missing dates
    missing_dates = sorted(all_dates - set(dates))

    remaining_dates = [date.strftime('%Y%m%d') for date in sorted(all_dates) if date not in missing_dates]
    
    # Create new table
    new_table = (table, initial_date, last_date, missing_dates, remaining_dates)
    
    return new_table

# COMMAND ----------

# MAGIC %md
# MAGIC Capture the dates for which files are present

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField('Object_name', StringType(), True),
    StructField('Initial_date', DateType(), True),
    StructField('Last_date', DateType(), True),
    StructField('Missing_dates', ArrayType(DateType()), True),
    StructField('Remaining_dates', ArrayType(StringType()), True)
])

# Initialize the DataFrame
dates_df = spark.createDataFrame([], schema=schema)

#Required variables
load_table = ['t24_cb_au_customers','t24_cb_nz_customers','t24_rd_au_customers','t24_rd_nz_customers', 'palermo_nyw_accounts','aps_ant_acc','aps_frf_acc','aps_lnd_acc','flexcube_eur_nr_accounts']
initial_loaddate = '20231108'
last_loaddate = '20250704'

# Process each row and add to DataFrame
new_rows = [file_list_dates(row, initial_loaddate, last_loaddate) for row in load_table]
new_df = spark.createDataFrame(new_rows, schema=schema)

# Union with the initial DataFrame
dates_df = dates_df.union(new_df)

dates_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC SCD2 Logic

# COMMAND ----------

for object in dates_df.collect():
    for date in object['Remaining_dates']:
        base_path = f"abfss://riskshield@saradarprod.dfs.core.windows.net/{object['Object_name']}/101/data/"
        snapshot_path = f"{base_path}/EDL_LOAD_DTS={date}/"
        
        
        date_format = datetime.strptime(date, "%Y%m%d")

        # Load snapshot
        df_snapshot = spark.read.parquet(snapshot_path)
        df_snapshot = df_snapshot.drop(col('IF_LoadDate'))

        HashCols = df_snapshot.columns
        # HashCols = [col for col in df_snapshot.columns if col not in  ['IF_LoadDate']]

        df_snapshot = df_snapshot.withColumn("Hash", lit(sha2(concat_ws("~", *HashCols), 256))) \
            .withColumn("StartDate", to_date(lit(date), "yyyyMMdd")) \
            .withColumn("EndDate",to_date(lit("9999-12-31"),"yyyy-MM-dd")) \
            .withColumn("Active_status",lit("Y"))
        
        df_snapshot = df_snapshot.drop_duplicates(subset = ['Hash'])

        target_path = f"abfss://riskshield@saradarprod.dfs.core.windows.net/{object['Object_name']}_scd2"

        # Check if Delta table exists
        if not DeltaTable.isDeltaTable(spark, target_path):
            df_snapshot.write.format("delta").mode("overwrite").save(target_path)
        else:
            # Load target Delta table
            delta_table = DeltaTable.forPath(spark, target_path)

            delta_table.alias('target').merge(df_snapshot.alias('source'), 'target.Hash = source.Hash and target.Active_status = "Y"') \
                        .whenNotMatchedBySourceUpdate(condition = 'target.Active_status = "Y"', 
                        set = {"target.EndDate": f'"{date_format}"', "target.Active_status": "'N'" }) \
                        .whenNotMatchedInsertAll().execute()

        print(f"Load successful for {object['Object_name']} on {date}")

# COMMAND ----------

# MAGIC %md
# MAGIC Synapse SCD2 Table Load

# COMMAND ----------

for object in dates_df.collect():
    table_name = object['Object_name']
    table_name1 = table_name.upper()
    df = spark.read.format('delta').load(f'abfss://riskshield@saradarprod.dfs.core.windows.net/{table_name}_scd2')
    Load_from_Databricks_to_Synapse_Dedicated_SQL_Pool(df, f"SCD2.{table_name1}" , f"{table_name1}_TEMP","overwrite")

    print(f"Table: SCD2.{table_name1} loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC template to load data and capture errors without terminating loop

# COMMAND ----------

for date in lst:
    try:
        table_name = 'CAC_ACG_ENTR'

        base_path = f"abfss://cna@edlcorestdeuprod0001.dfs.core.windows.net/{table_name}/0/data/"
        snapshot_path = f"{base_path}/loaddate={date}*/"

        df = spark.read.parquet(snapshot_path).withColumn('IF_LoadDate', to_date(lit(date), 'yyyy-MM-dd')).select(select_lst)
        print(f"Successfully read data")

        Load_from_Databricks_to_Synapse_Dedicated_SQL_Pool(df, f"CNA.BOOKINGINFORMATION_NEW" , f"BOOKINGINFORMATION_NEW","append")

        print(f"Table: CNA.BOOKINGINFORMATION loaded successfully on {date}")
    
    except Exception as e:
        print(f"Failed to read/write data on {date}: {e}")
        continue
