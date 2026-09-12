# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal:
# MAGIC To export the latest file

# COMMAND ----------

dbutils.fs.ls('dbfs:/GNS/WR001_structure/')

# COMMAND ----------

df = spark.read.csv('dbfs:/GNS/WR001_structure/Rabo_WR001_AssociatedClientsPerParty_20260413T014631.txt', sep = '|', header = True)

# COMMAND ----------

# df.write.saveAsTable('wr_fj_parties_and_risk_assessment_preprd.gns.adhoc_WR001_AssociatedClientsPerParty_20260413T014631')

df.write.saveAsTable('hive_metastore.adhocrequests.GNS_WR001_AssociatedClientsPerParty_20260413T014631')

# COMMAND ----------

display(df) 

# COMMAND ----------


