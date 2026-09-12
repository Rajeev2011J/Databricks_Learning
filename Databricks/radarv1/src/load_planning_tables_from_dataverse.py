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

# MAGIC %md
# MAGIC # rdr_caseplanningdetails

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_caseplanningdetailses')
df.createOrReplaceTempView("rdr_caseplanningdetails")

# COMMAND ----------

query = '''   
SELECT 
    rdr_CaseId AS CaseId
    , rdr_CurrentStatus AS CurrentStatus
    , rdr_CurrentCDDDepartment AS CurrentCDDDepartment
FROM rdr_caseplanningdetails
'''
rdr_caseplanningdetails_df = spark.sql(query)

# COMMAND ----------

# DBTITLE 1,For SLA
query = '''   
SELECT 
    rdr_CaseId AS CaseId
    , rdr_CurrentStatus AS CurrentStatus
    , rdr_CurrentCDDDepartment AS CurrentCDDDepartment
    , `_rdr_clients_value@OData.Community.Display.V1.FormattedValue` AS sourceClient
FROM rdr_caseplanningdetails
'''
rdr_caseplanningdetailsNew1_df = spark.sql(query)

# COMMAND ----------

# create table if not exist based on df
table_name = "rdr_caseplanningdetails"

columns = ",\n    ".join([f"{col} {dtype.upper()}" for col, dtype in rdr_caseplanningdetails_df.dtypes])

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS radar.{table_name} (
        {columns}
    )
"""

spark.sql(create_table_query)

# COMMAND ----------

# DBTITLE 1,For SLA
# create table if not exist based on df
table_name = "rdr_caseplanningdetailsNew1"

columns = ",\n    ".join([f"{col} {dtype.upper()}" for col, dtype in rdr_caseplanningdetailsNew1_df.dtypes])

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS radar.{table_name} (
        {columns}
    )
"""

spark.sql(create_table_query)

# COMMAND ----------

upsert_to_databricks_table(rdr_caseplanningdetails_df, 'radar', 'rdr_caseplanningdetails', ['CaseId'])

# COMMAND ----------

# DBTITLE 1,For SLA
upsert_to_databricks_table(rdr_caseplanningdetailsNew1_df, 'radar', 'rdr_caseplanningdetailsNew1', ['CaseId'])

# COMMAND ----------

# MAGIC %md
# MAGIC # rdr_sprintstatuscasephasemapping

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_sprintstatuscasephasemappings')
df.createOrReplaceTempView("rdr_sprintstatuscasephasemapping")

# COMMAND ----------

query = '''   
SELECT
    rdr_SprintStatusCasePhaseMappingId AS SprintStatusCasePhaseMappingId
    , rdr_SortOrder AS SortOrder
    , rdr_SprintStatus AS SprintStatus
    , rdr_CasePhase AS CasePhase
FROM rdr_sprintstatuscasephasemapping
'''
rdr_sprintstatuscasephasemapping_df = spark.sql(query)

# COMMAND ----------

# create table if not exist based on df
table_name = "rdr_sprintstatuscasephasemapping"

columns = ",\n    ".join([f"{col} {dtype.upper()}" for col, dtype in rdr_sprintstatuscasephasemapping_df.dtypes])

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS radar.{table_name} (
        {columns}
    )
"""

spark.sql(create_table_query)

# COMMAND ----------

upsert_to_databricks_table(rdr_sprintstatuscasephasemapping_df, 'radar', 'rdr_sprintstatuscasephasemapping', ['SprintStatusCasePhaseMappingId'])

# COMMAND ----------

# MAGIC %md
# MAGIC # rdr_sprintstatuslog

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_sprintstatuslogs')
df.createOrReplaceTempView("rdr_sprintstatuslog")

# COMMAND ----------

query = '''   
SELECT 
    rdr_CaseId AS CaseId
    , CreatedOn AS CreatedOnUTC
    , rdr_DaysIn AS DaysIn
    , rdr_EndDate AS EndDateUTC
    , `_rdr_sprintstatus_value@OData.Community.Display.V1.FormattedValue` AS SprintStatus
    , rdr_SprintStatusDepartment AS SprintStatusDepartment
    , rdr_SprintStatusLogId
    , CASE 
        WHEN rdr_CaseId LIKE 'NP_%' THEN REPLACE(rdr_CaseId, 'NP_', 'NP_NPPC_')
        ELSE CONCAT('LEC_', rdr_CaseId)
      END AS NewCaseId

FROM rdr_sprintstatuslog
'''
rdr_sprintstatuslog_df = spark.sql(query)

# COMMAND ----------

# create table if not exist based on df
table_name = "rdr_sprintstatuslog"

columns = ",\n    ".join([f"{col} {dtype.upper()}" for col, dtype in rdr_sprintstatuslog_df.dtypes])

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS radar.{table_name} (
        {columns}
    )
"""

spark.sql(create_table_query)

# COMMAND ----------

upsert_to_databricks_table(rdr_sprintstatuslog_df, 'radar', 'rdr_sprintstatuslog', ['rdr_SprintStatusLogId'])
