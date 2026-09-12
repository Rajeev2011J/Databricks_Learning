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

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_fosonboardingpipelines')
df.createOrReplaceTempView("OnboardingsView")

# COMMAND ----------

query = '''   
  SELECT 
     rdr_fosonboardingpipelineid AS Id
    ,rdr_name AS ClientName 
    ,rdr_uniquegcobid AS UniqueGcobId    
    ,rdr_sectorteam AS SectorTeam
    ,rdr_reviewlocation AS ReviewLocation   
    ,`rdr_currentstatus@OData.Community.Display.V1.FormattedValue` AS CurrentStatus
    ,rdr_deadline AS Deadline
    ,rdr_globalclientowner AS GlobalClientOwner
    ,CAST(rdr_insertedondate AS Date) AS InsertedOnDate
    ,CAST(rdr_kycplannedstartdate AS Date) AS KYCPlannedStartDate
    ,CAST(NULL AS String) AS MainProductCategory -- rdr_mainproductcategory@OData.Community.Display.V1.FormattedValue when data in this column
    ,`rdr_onboardingreason@OData.Community.Display.V1.FormattedValue` AS OnboardingReason 
    ,`rdr_onboardingtype@OData.Community.Display.V1.FormattedValue` AS OnboardingType
    ,rdr_parent AS KYCGroup
    ,rdr_remark AS Remark
    ,'Onboarding Pipeline App' AS Source
  FROM OnboardingsView
'''

onboardings_df = spark.sql(query)

# COMMAND ----------

# create table if not exist based on df
table_name = "onboardings"

columns = ",\n    ".join([f"{col} {dtype.upper()}" for col, dtype in onboardings_df.dtypes])

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS radar.{table_name} (
        {columns}
    )
"""

spark.sql(create_table_query)

# COMMAND ----------

upsert_to_databricks_table(onboardings_df, 'radar', 'onboardings', ['id'])
