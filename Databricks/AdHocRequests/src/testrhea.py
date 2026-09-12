# Databricks notebook source
print("Hello World")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM radar.clients LIMIT 10

# COMMAND ----------

df=spark.sql("SELECT * FROM radar.clients LIMIT 10")

# COMMAND ----------

df.display()
