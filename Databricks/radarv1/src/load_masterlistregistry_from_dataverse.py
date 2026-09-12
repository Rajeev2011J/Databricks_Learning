# Databricks notebook source
import os
import requests
from dataverse.functions_dataverse import get_access_token, get_dataverse_data
from functions_databricks import upsert_to_databricks_table

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

dataverse_url = os.environ['DATAVERSE_URL']
dataverse_api_url = f"{dataverse_url}/api/data/v9.1/"

# COMMAND ----------

access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_masterlistregistries')
df.createOrReplaceTempView("MasterListRegistryView")

# COMMAND ----------

query = '''   
  SELECT DISTINCT
     rdr_masterlistregistryid AS Id
    ,rdr_uniquegcobid AS UniqueGcobId
    ,rdr_kycgroup AS KYCGroup
    ,rdr_sectorteam AS SectorTeam
    ,rdr_reviewlocation AS ReviewLocation
    ,CAST(rdr_cddexecution AS DATE) AS CDDExecution
    ,CAST(rdr_clientcaseinitiationstart AS DATE) AS ClientCaseInitiationStart
    ,rdr_reason AS Reason
    ,rdr_reasonexplanation AS ReasonExplanation
    ,rdr_londonsectorteam AS LondonSectorTeam
    ,CAST(rdr_recordactiveat AS TIMESTAMP) AS RecordActiveAt
    ,CAST(rdr_recordexpiredat AS TIMESTAMP) AS RecordExpiredAt 
    ,CASE rdr_isactiverecord 
      WHEN 'true' THEN 1
      WHEN 'false' THEN 0
     END AS IsActiveRecord
    ,rdr_source AS Source
  FROM MasterListRegistryView
'''

masterlistregistry_df = spark.sql(query)

# COMMAND ----------

# create table if not exist based on df
table_name = "masterlistregistry"

columns = ",\n    ".join([f"{col} {dtype.upper()}" for col, dtype in masterlistregistry_df.dtypes])

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS radar.{table_name} (
        {columns}
    )
"""

spark.sql(create_table_query)


# COMMAND ----------

upsert_to_databricks_table(masterlistregistry_df, 'radar', 'masterlistregistry', ['id'])
