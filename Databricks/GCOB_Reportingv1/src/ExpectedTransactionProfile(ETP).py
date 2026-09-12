# Databricks notebook source
# DBTITLE 1,Import libraries

import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.storagelevel import StorageLevel

#Local File Import
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Date Parameters
date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
load_dt = 'LOAD_DT=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

# DBTITLE 1,DataObject Names

ETPQuestionMapping_dataobject = 'ETPQuestionMapping'
ETP_dataobject = 'ETPQuestionandanswers'


# COMMAND ----------

# DBTITLE 1,Authenticates storage account
ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# DBTITLE 1,Read data Object
# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# DBTITLE 1,Read ETP data object from GDP
# print list of strings for loading spark dfs from GDP
load_df = [
'ExpectedTransactionProfile_API_Questionnaires'
]

# Create TempView for each loading table
for Object in load_df:
    fetch_latest_file(Object, date_parameter, ReadStorage, Dataversion=101)

# COMMAND ----------

# DBTITLE 1,ETP Question mapping
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ETPQuestionMapping AS
# MAGIC select distinct QuestionId,QuestionName from ExpectedTransactionProfile_API_Questionnaires 

# COMMAND ----------

# DBTITLE 1,Write  ETP Mapping into SA Radar
df_ETPQuestionMapping= spark.table('ETPQuestionMapping')
save_to_saradar_storage_account(df_ETPQuestionMapping, ETPQuestionMapping_dataobject)


# COMMAND ----------

# DBTITLE 1,create dataframe
etpdf=spark.table('ExpectedTransactionProfile_API_Questionnaires')

# COMMAND ----------

# DBTITLE 1,Pivot Logic

# Enable AQE optimizations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")


df = (
    etpdf.select(
        F.trim(F.col("SourceClient")).alias("SourceClient"),
        F.trim(F.col("QuestionId")).alias("QuestionId"),
        F.when(
            F.col("PossibleAnswerValue").isNotNull(),
            F.trim(F.col("PossibleAnswerValue"))
        ).otherwise(F.trim(F.col("FreeFormAnswerValue"))).alias("AnswerValue")
    )
    .filter(
        F.col("SourceClient").isNotNull()
        & F.col("QuestionId").isNotNull()
        & F.col("AnswerValue").isNotNull()
    )
)

# Aggregate answers per QuestionId
agg_long = (
    df
    .groupBy("SourceClient", "QuestionId")
    .agg(F.collect_set("AnswerValue").alias("answers_set"))
)


question_sorted = (
    agg_long
    .select("QuestionId").distinct()
    .withColumn("QuestionNum", regexp_extract(col("QuestionId"), r"^Q(\d+)$", 1).cast("long"))
    .orderBy(col("QuestionNum").asc_nulls_last(), col("QuestionId").asc())
    .drop("QuestionNum")
)

# Create concatenated text for answers
agg_long = agg_long.withColumn("answers_text", F.concat_ws(", ", F.col("answers_set")))

agg_long.persist(StorageLevel.MEMORY_AND_DISK)

# Get distinct QuestionId list dynamically
question_ids = [r["QuestionId"] for r in question_sorted.collect()]

# Repartition by client for performance
agg_long = agg_long.repartition(100, "SourceClient")

# Pivot on the pre-aggregated table
etp = (
    agg_long
    .groupBy("SourceClient")
    .pivot("QuestionId", question_ids)
    .agg(F.first("answers_text"))
)


etp.createOrReplaceTempView("ETPQA")


# COMMAND ----------

# DBTITLE 1,Final data object
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ETP AS
# MAGIC select c.GcobId,c.CaseId,c.FullLegalName,c.ReviewTypeName,c.CaseStatusName,c.SourceSystem,c.IsLatestApprovedVersionOfClient,c.CaseCompletedDate,c.ClientLifeCycleName,c.ClientType,i.*
# MAGIC from ETPQA i 
# MAGIC left join party_case_client_details c on i.SourceClient = c.SourceClient
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Dataframe and add Businessdate column
df_ETP=spark.table('ETP')
df_ETP=df_ETP.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

# DBTITLE 1,Write dataframe to SARadar storage
save_to_saradar_storage_account(df_ETP, ETP_dataobject)
