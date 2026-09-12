# Databricks notebook source
# MAGIC %md
# MAGIC ### Context
# MAGIC - Request during Area call as input for organizing a single product-book. 
# MAGIC - As input for discussion on product books (maybe to allign on only 1!)
# MAGIC
# MAGIC ##### Requested by:
# MAGIC Lucille
# MAGIC
# MAGIC ##### picked up by:
# MAGIC Ruud

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from radar.cases t1
# MAGIC left join (select SourceClient,  count(distinct(ProductName)) as DistinctProducts
# MAGIC from radar.productsandservices 
# MAGIC Group by SourceClient) as t2 on t1.SourceClient = t2.SourceClient
# MAGIC where t1.IsLatestApprovedVersionOfClient = 'True'
# MAGIC --and DistinctProducts = 7
# MAGIC order by t2.DistinctProducts desc
