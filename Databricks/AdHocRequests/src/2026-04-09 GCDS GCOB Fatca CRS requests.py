# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal:
# MAGIC
# MAGIC To export products and services + Fatca/CRS data from GCOB to fix GCDS fatca/crs migration
# MAGIC
# MAGIC
# MAGIC ### Flow
# MAGIC 1. connect to manual file
# MAGIC 2. connect to GCOB data
# MAGIC 3. Query and Export
# MAGIC
# MAGIC
# MAGIC ### Export
# MAGIC - GCID
# MAGIC - GCOBID + latest case
# MAGIC - ...
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,1 connect to manual file
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCIDS_FATCA_CRS AS
# MAGIC
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     read_files(
# MAGIC       'file:/Workspace/data/List of GCIDs GCOB check.csv',
# MAGIC       header => "True",
# MAGIC       sep => ";"
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GCIDS_FATCA_CRS

# COMMAND ----------

# MAGIC %sql
# MAGIC select t0.*
# MAGIC , t1.GCDSID
# MAGIC , t1.LatestCompletedCaseId
# MAGIC , t2.FatcaClassification
# MAGIC , t2.IsEligibleForFatcaAssessment
# MAGIC , t2.CrsClassification
# MAGIC , t2.IsEligibleForCrsAssessment
# MAGIC , t3.*
# MAGIC from GCIDS_FATCA_CRS as t0
# MAGIC left join radar.clients as t1 on t0.`GCOB ID` = t1.GcobId
# MAGIC left join radar.cases as t2 on t1.LatestCompletedCaseId = t2.CaseId
# MAGIC left join radar.productsandservices as t3 on t2.sourceclient = t3.sourceclient
# MAGIC order by t0.GCID
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN SCHEMA wr_radar

# COMMAND ----------


