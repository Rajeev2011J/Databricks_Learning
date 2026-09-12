# Databricks notebook source
import pandas as pd

# COMMAND ----------

source_full_storage_account_name = "salandingzonefecradarprd.dfs.core.windows.net" 


# COMMAND ----------

import os
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

SALZReadStorage = 'salandingzonefecradarprd'

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

containers = ['aps'
 , 'dnb-meeting-upload'
 , 'ranz-mdm'
 , 'ranz-ocdd'
 , 'riskshield'
 , 'riskshield-calypso-au'
 , 'riskshield-flexcube-aps'
 , 'riskshield-flexcube-eu'
 , 'riskshield-murex'
 , 'riskshield-nyw'
 , 'riskshield-ods-rbb'
 , 'riskshield-raf'
 , 'riskshield-t24'
 , 'riskshield-tm'
, 'siebel' ]

# COMMAND ----------

tot_df = pd.DataFrame()
for container in containers:
    content = dbutils.fs.ls(f"abfss://{container}@{SALZReadStorage}.dfs.core.windows.net/")
    print(container)

    df = pd.DataFrame({'FileInfo': content})
    df['Container'] = container

    tot_df = pd.concat([df, tot_df])


# COMMAND ----------

tot_df['path'] = tot_df.FileInfo.apply(lambda x: x.path)
tot_df['name'] = tot_df.FileInfo.apply(lambda x: x.name)
tot_df['size'] = tot_df.FileInfo.apply(lambda x: x.size)
tot_df['modificationDate'] = tot_df.FileInfo.apply(lambda x: datetime.fromtimestamp(x.modificationTime/ 1000).strftime('%Y-%m-%d'))


# COMMAND ----------

sdf = spark.createDataFrame(tot_df)
sdf.write.saveAsTable('adhocrequests.landingzonecontents')
