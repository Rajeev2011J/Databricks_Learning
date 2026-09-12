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

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_sggdashboardappcomparisondetailses')
df.createOrReplaceTempView("sggappview")

# COMMAND ----------

query = '''   
SELECT 
    rdr_sggdashboardappcomparisondetailsid AS id,
    rdr_uniqueid AS sggAppUniqueID,
    rdr_comment AS comment,
    `rdr_comparisontype@OData.Community.Display.V1.FormattedValue` AS comparisonType,
    `rdr_status@OData.Community.Display.V1.FormattedValue` AS status,
    `statecode@OData.Community.Display.V1.FormattedValue` AS isActive,
    `_modifiedby_value@OData.Community.Display.V1.FormattedValue` AS modifiedBY,
    `statuscode@OData.Community.Display.V1.FormattedValue` AS statusCodeFormatted,
    CAST(rdr_actionowner AS String) AS actionOwner
FROM sggappview
where `statecode@OData.Community.Display.V1.FormattedValue` = 'Active'
'''
sggapp_df = spark.sql(query)

# COMMAND ----------

# create table if not exist based on df
table_name = "sggapp"

columns = ",\n    ".join([f"{col} {dtype.upper()}" for col, dtype in sggapp_df.dtypes])

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS radar.{table_name} (
        {columns}
    )
"""

spark.sql(create_table_query)

# COMMAND ----------

upsert_to_databricks_table(sggapp_df, 'radar', 'sggapp', ['id']) 
