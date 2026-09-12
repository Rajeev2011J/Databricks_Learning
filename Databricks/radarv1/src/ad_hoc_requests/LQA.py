# Databricks notebook source
# MAGIC %md
# MAGIC # LQA information monthly transform

# COMMAND ----------

# DBTITLE 1,Setting up environment variables
import os

app_reg_app_id = os.environ['APP_REG_APP_ID']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,Connect to blob storage: salandingzonefecradarprd
ReadStorage = "salandingzonefecradarprd"

spark.conf.set(f"fs.azure.account.auth.type.{ReadStorage}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{ReadStorage}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{ReadStorage}.dfs.core.windows.net", app_reg_app_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{ReadStorage}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{ReadStorage}.dfs.core.windows.net", f"https://login.microsoftonline.com/{TenantId}/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Exploring what's inside the blob storage
dbutils.fs.ls("abfss://lqa@salandingzonefecradarprd.dfs.core.windows.net/")
