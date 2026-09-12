# Databricks notebook source
# simple command to start as test
import pandas as pd
import os
from datetime import datetime, timedelta

# COMMAND ----------

SaveDate = (datetime.today()).strftime("%Y-%m-%dT%H:%m:%mZ")

# COMMAND ----------

SaveDate

# COMMAND ----------

#
WriteStorage = 'sawrradardev'
WriteContainer = 'consumer-to-producer'

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

    spark.conf.set("fs.azure.account.auth.type."+WriteStorage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+WriteStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+WriteStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+WriteStorage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+WriteStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

dbutils.fs.ls(f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/testsample/load_dts=2025-02-06T14:02:02Z/')

# COMMAND ----------

# test df
# Create an RDD
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
rdd = spark.sparkContext.parallelize(data)

# Convert RDD to DataFrame
df = rdd.toDF(["language", "users_count"])

# COMMAND ----------

df.write.format('delta').save(f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/testsample/load_dts={SaveDate}/')

# COMMAND ----------


