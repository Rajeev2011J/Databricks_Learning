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

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_userteamregistries')
df.createOrReplaceTempView("UserTeamRegistryView")

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_teams')
df.createOrReplaceTempView("TeamsView")

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_users')
df.createOrReplaceTempView("UsersView")

# COMMAND ----------

query = '''
  SELECT DISTINCT
     rdr_userteamregistryid AS id
    ,utr.rdr_useremail AS userEmail
    ,rdr_username AS userName
    ,rdr_teamname AS team
    ,CAST(rdr_teamstartdate AS DATE) AS teamStartDate
    ,CAST(rdr_teamenddate AS DATE) AS teamEndDate
    ,rdr_department AS department
    ,rdr_cdddepartment AS cddDepartment
    ,CAST(rdr_defaulthours AS INT) AS defaultHours
    ,CASE        
      WHEN utr.rdr_active = 'Yes' THEN true
      WHEN utr.rdr_active = 'No' THEN false
     END AS active
  FROM UserTeamRegistryView utr
    JOIN TeamsView t
        ON utr._rdr_team_value = t.rdr_teamid
    JOIN UsersView u
        ON utr._rdr_username_value = u.rdr_userid  
'''

userteamregistry_df = spark.sql(query)

# COMMAND ----------

# create table if not exist based on df
table_name = "userteamregistry"

columns = ",\n    ".join([f"{col} {dtype.upper()}" for col, dtype in userteamregistry_df.dtypes])

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS radar.{table_name} (
        {columns}
    )
"""

spark.sql(create_table_query)


# COMMAND ----------

upsert_to_databricks_table(userteamregistry_df, 'radar', 'userteamregistry', ['id'])
