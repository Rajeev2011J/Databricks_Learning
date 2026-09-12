# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview
# MAGIC
# MAGIC #### Goal:
# MAGIC To load content of Dataverse / Powerapps each night and prepare for storage in the GDP.
# MAGIC
# MAGIC ### Flow of notebook:
# MAGIC 1. load the right functions
# MAGIC 2. get data from sources
# MAGIC   - portfolio planning
# MAGIC     - rdr_masterlistregistry
# MAGIC     - Databricks synchronized once a day
# MAGIC     - from notebook: load_masterlistregistry from dataverse. 
# MAGIC   - 'planning' app / workflow registration (cdd planning app)
# MAGIC     - no existing notebook yet
# MAGIC     - rdr_caseplanningdetails
# MAGIC     - rdr_caseremark
# MAGIC     - rdr_sprintstatus
# MAGIC     - rdr_sprintstatuslog
# MAGIC     - rdr_ ...
# MAGIC   - userteam app
# MAGIC     - rdr_userteamregistries
# MAGIC     - rdr_teams
# MAGIC     - rdr_users
# MAGIC     - from notebook: load_userteamregistry from dataverse
# MAGIC   - Save to Radar storage; databricks@saradar/dataverse/.../1/data/LOAD_DT=
# MAGIC
# MAGIC
# MAGIC ### Dependencies:
# MAGIC - note dependencies on functions_databrikcs folder
# MAGIC - load_userteamregistry_from_dataverse -> 'radar'.'userteamregistry'
# MAGIC - load_masterlistregistry_from_dataverse -> 'radar'.'masterlistregistry'

# COMMAND ----------

# DBTITLE 1,import libraries
import os
import requests
from RadarUtils import *
from datetime import datetime


# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']
environment = os.environ['ENV']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,connect to dataverse
#dataverse_url = os.environ['DATAVERSE_URL']
#dataverse_api_url = f"{dataverse_url}/api/data/v9.1/"
#access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)

# COMMAND ----------

# DBTITLE 1,Connecting storage account
saradar_container = 'databricks'
saradar_write_storage = f'saradar{environment}'
authenticate_storage_account(environment, saradar_write_storage)

# COMMAND ----------

# DBTITLE 1,Set Datae
#set date
DateToday = datetime.now().strftime('%Y%m%d')

# COMMAND ----------

# DBTITLE 1,Create dataverse table in metastore if not exists
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Dataverse

# COMMAND ----------

# DBTITLE 1,drop old mlr (external) table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Dataverse.Masterlistregistry

# COMMAND ----------

# DBTITLE 1,Store MasterlistRegistry
df_name = 'Masterlistregistry'

df_masterlistregistry = spark.sql('select * from radar.masterlistregistry')

(df_masterlistregistry
 .write
 .mode('overwrite')
 .option('path', f'abfss://'+saradar_container+'@'+saradar_write_storage+f'.dfs.core.windows.net/DATAVERSE/{df_name}/1/data/LOAD_DT={DateToday}')
 .saveAsTable(f'Dataverse.{df_name}'))

# COMMAND ----------

# DBTITLE 1,drop old userlistregistry external table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Dataverse.Userteamregistry

# COMMAND ----------

# DBTITLE 1,Store UserteamRegistry
df_name = 'Userteamregistry'

df_Userteamregistry = spark.sql('select * from radar.Userteamregistry')

(df_Userteamregistry
 .write
 .mode('overwrite')
 .option('path', f'abfss://'+saradar_container+'@'+saradar_write_storage+f'.dfs.core.windows.net/DATAVERSE/{df_name}/1/data/LOAD_DT={DateToday}')
 .saveAsTable(f'Dataverse.Userteamregistry'))

# COMMAND ----------


