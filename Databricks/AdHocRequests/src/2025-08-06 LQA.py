# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal:
# MAGIC For Lydia Saekan, she receives LQA files for further processing in LQA container through fuse routes. 
# MAGIC LQA = Loan Quality Assessment
# MAGIC
# MAGIC ### Flow
# MAGIC 1. Connect to LQA
# MAGIC 2. put files together:
# MAGIC 'Op zich kunnen alle boekingskantoren wel in 1 file maar ik heb wel de M en Y separaat nodig.  De Q file niet perse'
# MAGIC

# COMMAND ----------

# DBTITLE 1,import libraries
import pandas as pd
import os
from datetime import datetime
import re

# COMMAND ----------

# DBTITLE 1,Authenticate storage account

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,prod landing zone
SALZReadStorage = 'salandingzonefecradarprd'

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage +".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage +".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage +".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage +".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage +".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------



# COMMAND ----------

datepart = '2025-08-31'

# COMMAND ----------

all_lqa = dbutils.fs.ls("abfss://lqa@salandingzonefecradarprd.dfs.core.windows.net/")

# COMMAND ----------

lqa_filepaths = [file.path for file in all_lqa if datepart in file.path]

# COMMAND ----------

lqa_filepaths

# COMMAND ----------

#M-files
lqa_filepaths_M = [file.path for file in all_lqa if f'M-{datepart}' in file.path]


#Y-files
lqa_filepaths_Y = [file.path for file in all_lqa if f'Y-{datepart}' in file.path]


#Q-files
lqa_filepaths_R = [file.path for file in all_lqa if f'R-{datepart}' in file.path]



# COMMAND ----------

# DBTITLE 1,combining paths
def combine_lqa_files(list_of_paths):
    #initiate empty df to concat all others into
    sum_df = pd.DataFrame()
    
    # loop through all the files in the list
    for file in list_of_paths:
        #read one file to spark df
        df_add = (spark.read.option('delimiter', ';')
                    .option('header', True)
                    .csv(file))
        
        # rework to pandas for easy concat
        df_add_pd = df_add.toPandas()

        #add to the total df
        sum_df = pd.concat([sum_df,df_add_pd])

        print(file)


    return sum_df

# COMMAND ----------

df_lqa_M = combine_lqa_files(lqa_filepaths_M)
df_lqa_R = combine_lqa_files(lqa_filepaths_R)
df_lqa_Y = combine_lqa_files(lqa_filepaths_Y)

# COMMAND ----------

display(df_lqa_M)

# COMMAND ----------

display(df_lqa_Y)

# COMMAND ----------

display(df_lqa_R)

# COMMAND ----------


