# Databricks notebook source
import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

# MDM_RANZ_dataobject = 'MDM_RANZ'

# COMMAND ----------

ReadStorage = 'salandingzonefecradarprd'
authenticate_storage_account(ReadStorage)

# COMMAND ----------

#Derive the date for which data has to be processed from GDP
Today = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'
print (load_dts)

# COMMAND ----------

MDM_Files = dbutils.fs.ls(f"abfss://ranz-mdm@{ReadStorage}.dfs.core.windows.net/")

# COMMAND ----------

# DBTITLE 1,Reading Environment Variables
ENV=os.getenv("ENV")

# COMMAND ----------

# Loop through the list and print the name of each file
import re

CurrentYearMonth = datetime.today().strftime('%Y%m')
list_count = 0

for file_info in MDM_Files:

    FileName = file_info.name
    split_list = re.split(r"[_,.]", FileName)
    file_recieval_date = split_list[2]

    if file_recieval_date[:6] == CurrentYearMonth:
        break

    list_count += 1

# COMMAND ----------

from pyspark.sql.functions import to_date, lit

latest_mdm_df = spark.read \
    .format('csv') \
    .option('header','true') \
    .option('inferSchema', 'true') \
    .load(MDM_Files[list_count].path) \
    .withColumn('FILE_RECIEVAL_DATE', to_date(lit(file_recieval_date), 'yyyyMMdd')) \
    .createOrReplaceTempView('MDM')

# COMMAND ----------

# DBTITLE 1,Loading data to Synapse Dedicated SQL Pool for SIRA
if ENV =='prod':

    # Read the latest MDM file
    latest_mdm_df = spark.read \
        .format('csv') \
        .option('header','true') \
        .option('inferSchema', 'true') \
        .load(MDM_Files[list_count].path) \
        .withColumn('FILE_RECEIVAL_DATE', date_format(to_date(lit(file_recieval_date), 'yyyyMMdd'), 'dd/MM/yyyy')) \
        .withColumn("PREVIOUS_REVIEW_DATE", lit(None)) 

    # Reorder columns to match the target table
    target_columns = [
        "CONTRACT_ID", "PARTY_ID", "CLIENT_STATUS", "CLIENT_BUSINESS_LINE",
        "LAST_REVIEW_DATE", "PREVIOUS_REVIEW_DATE", "DISTRIBUTION_CHANNEL",
        "CUSTOMER_TYPE_CD", "CDD_CLIENT_TYPE", "CUSTOMER_COUNTRY", "RISK_RATING",
        "NEXT_CDD_REVIEW", "ONBOARDING_DATE", "OFFBOARDING_DATE", "FACE_TO_FACE",
        "GLOBAL_CLIENT_ID", "REL_TYPE_CD", "REL_ROL_DESC", "ISUBO", "ADVERSE_MEDIA",
        "PEP_EVALUATION_DESC", "PEP_TYPE_DESC", "PEP_NATIONALITY_ISO_CODE",
        "PEP_NATIONALITY", "RESIDENTIAL_ADDRESS_ISOCODE", "RESIDENTIAL_ADDRESS_COUNTRY",
        "FILE_RECEIVAL_DATE"
    ]

    # Cast all selected columns to string
    latest_mdm_df = latest_mdm_df.select([col(c).cast("string").alias(c) for c in target_columns])

    # Load the dataframe to Synapse Dedicated SQL Pool
    Load_from_Databricks_to_Synapse_Dedicated_SQL_Pool(latest_mdm_df, "mdm.ranz_2023","RANZ_MDM","append")


# COMMAND ----------

# save_to_saradar_storage_account(latest_mdm_df, MDM_RANZ_dataobject)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.MDM_RANZ;

# COMMAND ----------

spark.sql('select * from MDM').write.mode('overwrite').saveAsTable('radar.MDM_RANZ')
