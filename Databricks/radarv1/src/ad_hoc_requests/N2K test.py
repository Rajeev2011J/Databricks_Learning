# Databricks notebook source
# MAGIC %md
# MAGIC ### goal:
# MAGIC to check and compare expected N2K for dashboard

# COMMAND ----------



# COMMAND ----------

## Key usecase: eu.aut.AADRadarRegionEuropeAfrica.us
# dashboard: [RADAR] - Europe Dashboard
# key tables:
# user-attribute
# attribute-client
# radar.clients

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.n2k_userattribute where upn = 'Rob.Steenbeeke@rabobank.com'

# COMMAND ----------

# MAGIC %md
# MAGIC <p style="color:red">why not MTO and why not E&A?</p>

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.n2k_clientattribute where AttributeValue = 'Rabobank Kenya'

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.clients where GcobId = '913'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.n2k_userattribute where upn = 'Jan.Dorman@rabobank.com'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.n2k_clientattribute where AttributeValue = 'RegionEA'

# COMMAND ----------


