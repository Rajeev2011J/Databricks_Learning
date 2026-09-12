# Databricks notebook source
# MAGIC %md
# MAGIC # Loading and storing NL_Energy_Information_Real_Estate

# COMMAND ----------

# DBTITLE 1,connection
import os

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

# DBTITLE 1,load energyinfo table
import re

# manually adding version otherwise referencing higher number
version = '3'

path = f'abfss://energyinfo@edlcorestdeuprod0001.dfs.core.windows.net/NL_Energy_Information_Real_Estate/{version}/data/'

spark.read.format('delta').load(f'{path}').createOrReplaceTempView('energyinfo')

# COMMAND ----------

# DBTITLE 1,store table
spark.sql('DROP TABLE IF EXISTS radar.energyinfo')

spark.sql('''
    SELECT DISTINCT
        Bag_Addressable_Object_Id
        , Bag_Addressable_Object_Street_Name
        , Bag_Addressable_Object_House_Number
        , Bag_Addressable_Object_House_Letter
        , Bag_Addressable_Object_House_Number_Addition
        , Bag_Addressable_Object_Postal_Code
        , Bag_Addressable_Object_City_Name
        , Bag_Addressable_Object_Building_Construction_Year
        , EPC_OBJ_CONSTR_YR
        , EPC_ENRG_LBL_CODE
        , LOADED_DT
        , REPLACE(
            CONCAT_WS(
            '-',
            UPPER(Bag_Addressable_Object_House_Number),
            UPPER(Bag_Addressable_Object_House_Letter),
            UPPER(Bag_Addressable_Object_House_Number_Addition),
            UPPER(Bag_Addressable_Object_Postal_Code)
            ),
            ' ',
            ''
        ) AS EnergyInfoIdentifier

    FROM energyinfo
''').write.mode('overwrite').saveAsTable('radar.energyinfo')

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Bag_Addressable_Objects

# COMMAND ----------

from datetime import datetime, timedelta
import re

# https://edlcorestdeuprod0001.dfs.core.windows.net/kadaster-bag-data/Bag_Addressable_Objects/3/data/EDL_LOAD_DT=2026-02-10

flexcube_objects = ['Bag_Addressable_Objects'] # ['Bag_Addressable_Objects']

for item in flexcube_objects:
    # get the most recent version available in gdp
    path = f'abfss://kadaster-bag-data@edlcorestdeuprod0001.dfs.core.windows.net/{item}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
    version = 3 # force versioning

    # get the most recent file available in gdp and create temp view
    path_file = f'{path}{version}/data/'
    files = dbutils.fs.ls(path_file)
    load_date = max(file.path.split('EDL_LOAD_DT=')[1][:10] for file in files if 'EDL_LOAD_DT=' in file.path)
    spark.read.parquet(f'{path_file}EDL_LOAD_DT={load_date}/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS radar.bag_vbo')

spark.sql('''
    SELECT
        Bag_Addressable_Object_Id
        , Bag_Addressable_Object_Street_Name
        , Bag_Addressable_Object_House_Number
        , Bag_Addressable_Object_House_Letter
        , Bag_Addressable_Object_House_Number_Addition
        , Bag_Addressable_Object_Postal_Code
        , Bag_Addressable_Object_City_Name
        , REPLACE(
            CONCAT_WS(
                '-',
                UPPER(Bag_Addressable_Object_House_Number),
                UPPER(Bag_Addressable_Object_House_Letter),
                UPPER(Bag_Addressable_Object_House_Number_Addition),
                UPPER(Bag_Addressable_Object_Postal_Code)
            ),
            ' ',
            ''
            ) AS Bag_Identifier
        , EDL_LOAD_DTS
    FROM Bag_Addressable_Objects
''').write.mode('overwrite').saveAsTable('radar.bag_vbo')
