# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal: to add FATCA / CRS related information to GCIDS in GCDS
# MAGIC
# MAGIC #### Flow
# MAGIC 1. Load data from GCDS
# MAGIC 2. Load data from GCOB
# MAGIC 3. Export Products and Services per GCID of latest approved case
# MAGIC 4. Export FATCA / CRS status per latest approved case.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW DNB_NAICS_MAPPING AS 
# MAGIC
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     read_files(
# MAGIC       'file:/Workspace/data/List of GCIDs GCOB check.csv',
# MAGIC       header => "True",
# MAGIC       sep => ";"
# MAGIC     )
# MAGIC     WHERE `Include` = 'Yes'

# COMMAND ----------



# COMMAND ----------


