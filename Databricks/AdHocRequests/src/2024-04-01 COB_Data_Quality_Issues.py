# Databricks notebook source
# MAGIC %md
# MAGIC # GCOB Data Possible Quality Issues
# MAGIC **Author:** Sema Kapusizoglu \
# MAGIC **Created On:** 26-04-2024
# MAGIC
# MAGIC ### Context
# MAGIC GCOB data will be added to Kratos Data Model as a source for customer block. Kratos is the new solution to do WR transaction monitoring. In this notebook we will point out some possible data quality issues. 
# MAGIC - ValidatedCDDRisk
# MAGIC     - ValidatedCDDRisk always null for a case
# MAGIC     - ValidatedCDDRisk disappears from a case
# MAGIC     - Multiple ValidatedCddRisk for the same case
# MAGIC - ApprovalDate
# MAGIC     - ApprovalDate appearing different for the same case
# MAGIC     - ApprovalDate is inconsistent in cdd_outcome and risk_category tables for the same case
# MAGIC - Missing Data
# MAGIC     - Data is missing from risk category table after 19th February
# MAGIC     - Missing partitions
# MAGIC     - Randomly Missing Data
# MAGIC - Duplicated Data
# MAGIC
# MAGIC ### Methodology:
# MAGIC
# MAGIC - **Analysis period:** No analysis period is defined. 
# MAGIC - **Assumptions:** [if any assumptions are made, list them here]
# MAGIC - **Components used:** No KDM components are used.
# MAGIC - **List of data sources used:** \
# MAGIC     - legal_entity_party_cdd_outcome
# MAGIC - **Steps to execute analysis:**  
# MAGIC - **Deliverable (output path if applicable):**
# MAGIC
# MAGIC ### Executive Summary
# MAGIC
# MAGIC ### Further Discussion & Next Steps
# MAGIC Outline potential actions or recommendations for future steps. [if applicable]

# COMMAND ----------

# Import relevant packages & notebooks
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import lead, coalesce, lit

from typing import List


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data

# COMMAND ----------

# read data from sources

# cdd outcome
cdd_outcome_101 = spark.read.parquet('/mnt/gcob/legal_entity_party_cdd_outcome/101/data/')\
    .withColumn("EDL_LOAD_DTS", F.to_timestamp("EDL_LOAD_DTS", "yyyyMMdd'T'HHmmss'Z'"))\
    .withColumn("version", F.lit("101"))
cdd_outcome_100 = spark.read.parquet('/mnt/gcob/legal_entity_party_cdd_outcome/100/data/')\
    .withColumnRenamed("ClientName", "FullLegalName")\
    .withColumn("EDL_LOAD_DTS", F.to_timestamp("EDL_LOAD_DTS", "yyyyMMdd'T'HHmmss'Z'"))\
    .withColumn("version", F.lit("100"))

cdd_outcome = cdd_outcome_101.unionByName(cdd_outcome_100).withColumn("EDL_LOAD_DATE", F.to_date("EDL_LOAD_DTS", "yyyyMMdd'T'HHmmss'Z'"))
cdd_outcome.createOrReplaceTempView('cdd_outcome')

# Risk Category
risk_category_101 = spark.read.parquet('/mnt/gcob/legal_entity_party_cdd_risk_category_level/101/data/')\
    .withColumn("EDL_LOAD_DTS", F.to_timestamp("EDL_LOAD_DTS", "yyyyMMdd'T'HHmmss'Z'"))\
    .withColumn("version", F.lit("101"))
risk_category_100 = spark.read.parquet('/mnt/gcob/legal_entity_party_cdd_risk_category_level/100/data/')\
    .withColumn("EDL_LOAD_DTS", F.to_timestamp("EDL_LOAD_DTS", "yyyyMMdd'T'HHmmss'Z'"))\
    .withColumn("version", F.lit("100"))
risk_category = risk_category_100.unionByName(risk_category_101).withColumn("EDL_LOAD_DATE", F.to_date("EDL_LOAD_DTS", "yyyyMMdd'T'HHmmss'Z'"))
risk_category.createOrReplaceTempView('risk_category')

cdd_model = spark.read.parquet('/mnt/gcob/wr_cdd_model/100/data/')
cdd_model.createOrReplaceTempView('cdd_model')

cdd_qa_101 = spark.read.parquet('/mnt/gcob/legal_entity_party_cdd_risk_question_answer/101/data/').withColumn("version", F.lit("101"))
cdd_qa_100 = spark.read.parquet('/mnt/gcob/legal_entity_party_cdd_risk_question_answer/100/data/').withColumnRenamed("ClientName", "FullLegalName").withColumn("version", F.lit("100"))
# cdd_qa = cdd_qa_101.unionByName(cdd_qa_100)
cdd_qa_101.createOrReplaceTempView('cdd_qa_101')

case_information = spark.read.parquet('/mnt/gcob/legal_entity_case_information/100/data/')
case_information.createOrReplaceTempView('case_information')
work_item_information = spark.read.parquet('/mnt/gcob/legal_entity_work_item_information/101/data/')
work_item_information.createOrReplaceTempView('work_item_information')


# COMMAND ----------

# MAGIC %md
# MAGIC # ValidatedCDDRisk Related Issues

# COMMAND ----------

cdd_quality_issues = (
    cdd_outcome_101.groupBy("GcobId", "GCID", "CaseId")
    .agg(
        F.collect_set("ValidatedCddRisk").alias("all_cdds")
    )
    .filter(F.size("all_cdds") > 1)
    .withColumn("contains_empty_string", F.lit(True))
    .union(
    cdd_outcome_101.groupBy("GcobId", "GCID", "CaseId")
    .agg(
        F.collect_set("ValidatedCddRisk").alias("all_cdds")
    )
    .filter(F.size("all_cdds") == 1)
    .withColumn("contains_empty_string", F.expr("array_contains(all_cdds, '')"))
    .filter(F.col("contains_empty_string") == True)
    )
)
# cdd_quality_issues.select("GcobId", "GCID", "CaseId", "all_cdds").display()

cdd_quality_issues.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ValidatedCDDRisk always null for a case:
# MAGIC
# MAGIC Current workaround: Imputing cases that never had ValidatedCDDRisk to "ImputedHighrisk"
# MAGIC

# COMMAND ----------

(
    cdd_outcome_101
    .groupBy("GcobId", "GCID", "CaseId")
    .agg(
        F.collect_set("ValidatedCddRisk").alias("all_cdds")
    )
    .filter(F.size("all_cdds") == 1)
    .withColumn("contains_empty_string", F.expr("array_contains(all_cdds, '')"))
    .filter(F.col("contains_empty_string") == True)
    .distinct()
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ValidatedCDDRisk disappears from a case

# COMMAND ----------


(
    cdd_outcome_101
    .groupBy("GcobId", "GCID", "CaseId")
    .agg(
        F.collect_set("ValidatedCddRisk").alias("all_cdds")
    )
    .filter(F.size("all_cdds") == 2)
    .withColumn("contains_empty_string", F.expr("array_contains(all_cdds, '')"))
    .filter(F.col("contains_empty_string") == True)
    # .select("GcobId", "CaseId", "GCID", "ValidatedCddRisk", "ApprovalDate", "EDL_LOAD_DTS")
    # .orderBy("EDL_LOAD_DTS")
    .count()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Multiple ValidatedCddRisk for the same case

# COMMAND ----------

(
    cdd_outcome_101
    .groupBy("GcobId", "GCID", "CaseId")
    .agg(
        F.collect_set("ValidatedCddRisk").alias("all_cdds")
    )
    .filter(F.size("all_cdds") > 1)
    .withColumn("contains_empty_string", F.expr("array_contains(all_cdds, '')"))
    .filter(F.col("contains_empty_string") == False)
    # .select("GcobId", "CaseId", "GCID", "ValidatedCddRisk", "ApprovalDate", "EDL_LOAD_DTS")
    # .orderBy("EDL_LOAD_DTS")
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ApprovalDate appearing different for the same case
# MAGIC
# MAGIC Current workaround: Casting it to date 

# COMMAND ----------

(
    cdd_outcome_101
    .groupBy("GcobId", "GCID", "CaseId")
    .agg(
        F.collect_set("ApprovalDate").alias("all_dates")
    )
    .filter(F.size("all_dates") > 1)
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approval date inconsistent accross cdd_outcome and risk_categoty tables

# COMMAND ----------

(
    cdd_outcome_101.alias("cdd_outcome")
   .join(
        (
            risk_category_101
            .groupBy("gcobid", "gcid", "caseid", "approvaldate").pivot("categoryname").agg(F.first("CategoryCalculatedRiskLevel"))
            .select("gcobid","gcid","caseid", "approvaldate")
            .withColumnRenamed("approvaldate", "risk_cat_approvaldate")
        ).alias("risk_category")
        , [cdd_outcome_101.GCID == risk_category_101.GCID
          , cdd_outcome_101.GcobId == risk_category_101.GcobId
          , cdd_outcome_101.CaseId == risk_category_101.CaseId]
        , "left"
    )
   .filter(F.col("approvaldate") != F.col("risk_cat_approvaldate"))
   .withColumn("diff_in_approval_dates", F.datediff(F.col("approvaldate"), F.col("risk_cat_approvaldate")))
   .select("approvaldate", "risk_cat_approvaldate", "diff_in_approval_dates")
   .groupBy("diff_in_approval_dates")
   .count()
   .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Missing Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data is missing from risk category table after 19th February

# COMMAND ----------

display(
    (
          risk_category_100
            .select(
                F.to_date(F.min('EDL_LOAD_DTS'), "yyyyMMdd'T'HHmmss'Z'"). alias('MinLoadDate'), 
                F.to_date(F.max('EDL_LOAD_DTS'),"yyyyMMdd'T'HHmmss'Z'" ).alias('MaxLoadDate')
            ).distinct()
            .withColumn('Table', F.lit('legal_entity_party_cdd_risk_category_level'))
            .withColumn('Version', F.lit('100'))        
        ).union(
           risk_category_101
            .select(
                F.to_date(F.min('EDL_LOAD_DTS'), "yyyyMMdd'T'HHmmss'Z'"). alias('MinLoadDate'), 
                F.to_date(F.max('EDL_LOAD_DTS'),"yyyyMMdd'T'HHmmss'Z'" ).alias('MaxLoadDate')
            ).distinct()
            .withColumn('Table', F.lit('legal_entity_party_cdd_risk_category_level'))
            .withColumn('Version', F.lit('101'))
        ).union(
          cdd_outcome_100
            .select(
                F.to_date(F.min('EDL_LOAD_DTS'), "yyyyMMdd'T'HHmmss'Z'"). alias('MinLoadDate'), 
                F.to_date(F.max('EDL_LOAD_DTS'),"yyyyMMdd'T'HHmmss'Z'" ).alias('MaxLoadDate')
            ).distinct()
            .withColumn('Table', F.lit('cdd_outcome'))
            .withColumn('Version', F.lit('100'))        
        ).union(
           cdd_outcome_101
            .select(
                F.to_date(F.min('EDL_LOAD_DTS'), "yyyyMMdd'T'HHmmss'Z'"). alias('MinLoadDate'), 
                F.to_date(F.max('EDL_LOAD_DTS'),"yyyyMMdd'T'HHmmss'Z'" ).alias('MaxLoadDate')
            ).distinct()
            .withColumn('Table', F.lit('cdd_outcome'))
            .withColumn('Version', F.lit('101'))
        ).union(
          cdd_model
            .select(
                F.to_date(F.min('EDL_LOAD_DTS'), "yyyyMMdd'T'HHmmss'Z'"). alias('MinLoadDate'), 
                F.to_date(F.max('EDL_LOAD_DTS'),"yyyyMMdd'T'HHmmss'Z'" ).alias('MaxLoadDate')
            ).distinct()
            .withColumn('Table', F.lit('cdd_model'))
            .withColumn('Version', F.lit('100'))        
        ).union(
           cdd_qa_100
            .select(
                F.to_date(F.min('EDL_LOAD_DTS'), "yyyyMMdd'T'HHmmss'Z'"). alias('MinLoadDate'), 
                F.to_date(F.max('EDL_LOAD_DTS'),"yyyyMMdd'T'HHmmss'Z'" ).alias('MaxLoadDate')
            ).distinct()
            .withColumn('Table', F.lit('cdd_qa_100'))
            .withColumn('Version', F.lit('100'))
        ).union(
           cdd_qa_101
            .select(
                F.to_date(F.min('EDL_LOAD_DTS'), "yyyyMMdd'T'HHmmss'Z'"). alias('MinLoadDate'), 
                F.to_date(F.max('EDL_LOAD_DTS'),"yyyyMMdd'T'HHmmss'Z'" ).alias('MaxLoadDate')
            ).distinct()
            .withColumn('Table', F.lit('cdd_qa_101'))
            .withColumn('Version', F.lit('101'))
        )
        
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Duplicated Data

# COMMAND ----------

(
    risk_category_101
    .groupBy("GcobId", "GCID", "CaseId", "CategoryName", "EDL_LOAD_DTS")
    .count()
    .filter("count > 1")
    .select("GcobId", "GCID", "CaseId")
    .distinct()
    .count()
)


# COMMAND ----------

# MAGIC %md
# MAGIC # Missing Partitions
# MAGIC
# MAGIC GCOB data is loaded to GDP everyday with only completed cases. That means when we check the EDL_LOAD_DTS, we should be having data for everyday
# MAGIC

# COMMAND ----------

# DBTITLE 1,Missing partitions
cdd_outcome_missing_partitions = spark.sql("""
                                        select
                                        explode(sequence(MIN(EDL_LOAD_DATE),MAX(EDL_LOAD_DATE),interval 1 day))
                                        from cdd_outcome
                                        MINUS
                                        select 
                                        distinct(EDL_LOAD_DATE) 
                                        from cdd_outcome
                                        """).rdd.flatMap(lambda x: x).collect()
len(cdd_outcome_missing_partitions)

# COMMAND ----------

cdd_outcome.filter(F.col("EDL_LOAD_DATE").isin(cdd_outcome_missing_partitions)).count()

# COMMAND ----------

risk_category_missing_partitions = spark.sql("""
                                           select
                                            explode(sequence(MIN(EDL_LOAD_DATE),MAX(EDL_LOAD_DATE),interval 1 day))
                                            from risk_category
                                            MINUS
                                            select 
                                            distinct(EDL_LOAD_DATE) 
                                            from risk_category
                                           """).rdd.flatMap(lambda x: x).collect()
len(risk_category_missing_partitions)

# COMMAND ----------

risk_category.filter(F.col("EDL_LOAD_DATE").isin(risk_category_missing_partitions)).count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Randomly Missing Data
# MAGIC
# MAGIC There are some gaps in load dates and gaps do not seem to be as lost partitions. Randomly missing data indicates that the partition exists for a specific day, but some of the cases that existed previous day is not present in the partition. The case appears the next day. But these gaps are consistent. If data is not there for Gcobid and caseid for a specific day it is not there in other gcob tables as well.
# MAGIC

# COMMAND ----------

# DBTITLE 1,helper function
def create_full_load_range(df: SparkDataFrame, keys: List[str], load_dts: str):
    """
    Creates a fake full load date for a given set of keys, considering data is loaded to GDP everyday.
    """
    df_load_dates = df.groupBy(keys)\
                        .agg(F.min('EDL_LOAD_DTS').alias('min_load_date')
                            , F.max('EDL_LOAD_DTS').alias('max_load_date')
                            , F.count('*').alias('record_cnt'))\
                        .withColumn('existing_days', 
                                    F.datediff(
                                        F.to_timestamp(F.col('max_load_date'), "yyyyMMdd'T'hhmmss'Z'"),
                                        F.to_timestamp(F.col('min_load_date'), "yyyyMMdd'T'hhmmss'Z'")
                                        )
                        )\
                        .withColumn('missing_days', F.col('existing_days')+1-F.col('record_cnt'))\
                        .filter("missing_days!= 0")
    
    keys.extend(["min_load_date", "max_load_date"])
    df_all_dates = df_load_dates.select(*keys, "missing_days")\
        .withColumn('generated_load_date',F.expr("explode(sequence(to_date(min_load_date),to_date(max_load_date),interval 1 day))"))
    
    return df_load_dates, df_all_dates

# COMMAND ----------

# DBTITLE 1,Checking CDD Outcome
cdd_outcome_load_dates, cdd_outcome_full = create_full_load_range(cdd_outcome, keys=["GcobId", "GCID", "CaseId"], load_dts="EDL_LOAD_DTS")

cdd_outcome_full = cdd_outcome_full\
    .withColumnRenamed("GcobId", "GcobId_generated")\
    .withColumnRenamed("CaseId", "CaseId_generated")\
    .withColumnRenamed("GCID", "GCID_generated")
    
cdd_outcome_missing = cdd_outcome_full.join(cdd_outcome, how="left", 
        on=[cdd_outcome.GcobId == cdd_outcome_full.GcobId_generated,
            cdd_outcome.GCID == cdd_outcome_full.GCID_generated,
            cdd_outcome.CaseId == cdd_outcome_full.CaseId_generated,
            cdd_outcome.EDL_LOAD_DATE == cdd_outcome_full.generated_load_date
            ])\
            .filter("GcobId is Null")\
            .withColumn("generated_load_date", F.to_date("generated_load_date"))\
            .select("GcobId_generated", "CaseId_generated", "GCID_generated", "generated_load_date")\
            .filter(~F.col("generated_load_date").isin(cdd_outcome_missing_partitions))\

# Randomly missing: Data is not included in the partition for some reason, but partition exists            
print(f"Percentage of randomly missing data: {cdd_outcome_missing.count() * 100 / cdd_outcome_full.count()} percent")

# COMMAND ----------

# DBTITLE 1,Checking Risk Category
risk_category_load_dates, risk_category_full = create_full_load_range(risk_category, keys=["GcobId", "GCID", "CaseId", "CategoryName"], load_dts="EDL_LOAD_DTS")

risk_category_full = risk_category_full\
    .withColumnRenamed("GcobId", "GcobId_generated")\
    .withColumnRenamed("CaseId", "CaseId_generated")\
    .withColumnRenamed("GCID", "GCID_generated")\
    .withColumnRenamed("CategoryName", "CategoryName_generated") 

risk_category_missing = risk_category_full.join(risk_category, how="left", 
        on=[risk_category.GcobId == risk_category_full.GcobId_generated,
            risk_category.GCID == risk_category_full.GCID_generated,
            risk_category.CaseId == risk_category_full.CaseId_generated,
            risk_category.CategoryName == risk_category_full.CategoryName_generated,
            risk_category.EDL_LOAD_DATE == risk_category_full.generated_load_date
            ])\
            .filter("GcobId is Null")\
            .withColumn("generated_load_date", F.to_date("generated_load_date"))\
            .select("GcobId_generated", "CaseId_generated", "GCID_generated", "generated_load_date")\
            .filter(~F.col("generated_load_date").isin(risk_category_missing_partitions))\

# Randomly missing: Data is not included in the partition for some reason, but partition exists            
print(f"Percentage of randomly missing data: {risk_category_missing.count() * 100 / risk_category_full.count()} percent")
