# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal:
# MAGIC this notebook should create external tables within Unity Catalog for GIC
# MAGIC - GIC is set up as external source. 
# MAGIC - We may still have to create a Schema for it.
# MAGIC
# MAGIC also see: 
# MAGIC 1. https://assets.docs.databricks.com/_extras/notebooks/source/unity-catalog-external-table-example-azure.html
# MAGIC 2. https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/azure-managed-identities#grant

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS

# COMMAND ----------

# DBTITLE 1,GCDS SCHEMA
# MAGIC %sql
# MAGIC CREATE SCHEMA wr_fj_parties_and_risk_assessment_preprd.gcds

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE view oi_unity.xyz.silver_siebel_view_test AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   parquet.`abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_avy_hist/2/data/LOAD_DTS=20260225T043051Z/`

# COMMAND ----------

dbutils.fs.ls('abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_Client/4602/data/LOAD_DTS=20260225T043051Z/')

# COMMAND ----------

dbutils.fs.ls('abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_Client/4701/data/')

# COMMAND ----------

dbutils.fs.ls('abfss://gic@edlcorestdbrprod0001.dfs.core.windows.net/pessoa/1/data/')

# COMMAND ----------

dbutils.fs.ls('abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_Client/4602/data/LOAD_DTS=20260225T043051Z/')

# COMMAND ----------

# DBTITLE 1,GCDS as view?
# MAGIC %sql
# MAGIC --CREATE VIEW IF NOT EXISTS wr_fj_parties_and_risk_assessment_preprd.radar.gcds_client
# MAGIC
# MAGIC --AS SELECT * 
# MAGIC --FROM PARQUET.'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_Client/4701/data/'
# MAGIC --WITH CREDENTIAL 'wr_fec_radar'

# COMMAND ----------



# COMMAND ----------





# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,GCDS
# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE wr_fj_parties_and_risk_assessment_preprd.radar.gcds_client
# MAGIC USING PARQUET
# MAGIC LOCATION 'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_Client/4701/data/'
# MAGIC --WITH CREDENTIAL 'wr_fec_radar'
# MAGIC

# COMMAND ----------

# DBTITLE 1,GIC
# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE wr_fj_parties_and_risk_assessment_preprd.radar.gic_pessoa 
# MAGIC
# MAGIC LOCATION 'abfss://gic@edlcorestdbrprod0001.dfs.core.windows.net/pessoa/1/data/'

# COMMAND ----------



# COMMAND ----------

https://edlcorestdbrprod0001.dfs.core.windows.net/gic/pessoa/1/data/LOADED_DTS=20260211T001122Z

