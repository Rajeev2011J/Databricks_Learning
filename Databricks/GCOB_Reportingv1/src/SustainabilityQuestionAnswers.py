# Databricks notebook source
# MAGIC %md
# MAGIC This notebook saves Sustainability Questions Answers data to SA RADAR. No transformations are applied except selecting the relevant columns

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame

#Local File Import
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Date Parameters
date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

# DBTITLE 1,DataObject Name
Sustainability_dataobject = 'Sustainability_Question_Answers'

# COMMAND ----------

# DBTITLE 1,Authenticates storage account
ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# DBTITLE 1,Read data Object
# print list of strings for loading spark dfs from GDP
load_df = [
'Party_case_sustainabilityQuestionsAndAnswers'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# DBTITLE 1,Final data object
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Sustainabilit_Question_Answer AS 
# MAGIC SELECT DISTINCT
# MAGIC SourceClient,
# MAGIC SustainabilityCriteriaOutcome,
# MAGIC SustainabilityTopic,
# MAGIC SustainabilityNonAcceptanceRequirement
# MAGIC
# MAGIC FROM Party_case_sustainabilityQuestionsAndAnswers

# COMMAND ----------

df_Sustainability_Question_Answers= spark.table('Sustainabilit_Question_Answer')

# COMMAND ----------

# DBTITLE 1,Write Dataframe directly to Unity Catalog
catalog = os.environ['CATALOG']
schema = "gcobreportingv1"
object_name = "Sustainability_Question_Answers"

# Create schema if needed
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# drop table if needed
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{object_name}")

df_Sustainability_Question_Answers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        f"{catalog}.{schema}.{object_name}"
    )

# COMMAND ----------

# DBTITLE 1,Write dataframe to SARadar storage
save_to_saradar_storage_account(df_Sustainability_Question_Answers, Sustainability_dataobject)
