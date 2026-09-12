# Databricks notebook source
import os
import requests
from functions_dataverse import get_access_token, get_dataverse_data, truncate_dataverse_table, post_to_dataverse_table

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

jdbcHostname='firebirdsqlserverprodnla.database.windows.net'
ServiceKey='app-reg-databricks-wr-radar-preprd'

# COMMAND ----------

jdbcHostname = f'{jdbcHostname}'
jdbcPort = 1433
jdbcDatabase = "APP_ADB"

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
connectionProperties = {
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication" : "ActiveDirectoryServicePrincipal",
    "UserName" : app_reg_app_id ,
    "Password" : service_credential
}

spark.read.jdbc(url=jdbcUrl,table='pa.UserTeamRegistry',properties = connectionProperties).createOrReplaceTempView('UserTeamRegistry')

# COMMAND ----------

dataverse_url = os.environ['DATAVERSE_URL']
dataverse_api_url = f"{dataverse_url}/api/data/v9.1/"

# COMMAND ----------

access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_teams')
df.createOrReplaceTempView("Teams")

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_users')
df.createOrReplaceTempView("Users")

# COMMAND ----------

# view/check non migrated records
not_migrated = '''
  SELECT DISTINCT
       AssignedUser
      ,UserTeam
      ,TeamStartDate
      ,TeamEndDate
      ,Department
  FROM UserTeamRegistry utr
    LEFT JOIN Teams t
        ON utr.UserTeam = t.rdr_teamname
    LEFT JOIN radar.userlistmapping ulm
        ON utr.AssignedUser = ulm.UserNameOld
    LEFT JOIN Users u
        ON COALESCE(ulm.UserNameNew, utr.AssignedUser) = u.rdr_username
  WHERE u.rdr_username IS NULL
  ORDER BY AssignedUser  
'''

df = spark.sql(not_migrated)

display(df)

# COMMAND ----------

query = '''
  SELECT DISTINCT
     rdr_useremail
    ,CONCAT('/rdr_users(', rdr_userid, ')') AS rdr_UserName_lookup
    ,CONCAT('/rdr_teams(', rdr_teamid, ')') AS rdr_Team_lookup
    ,TeamStartDate as rdr_teamstartdate
    ,TeamEndDate as rdr_teamenddate
  FROM UserTeamRegistry utr
    JOIN Teams t
        ON utr.UserTeam = t.rdr_teamname
    LEFT JOIN radar.userlistmapping ulm
        ON utr.AssignedUser = ulm.UserNameOld
    JOIN Users u
        ON COALESCE(ulm.UserNameNew, utr.AssignedUser) = u.rdr_username
  ORDER BY rdr_teamstartdate  
'''

# COMMAND ----------

truncate_dataverse_table(dataverse_api_url, 'rdr_userteamregistries', access_token, 'rdr_userteamregistryid', 100)

# COMMAND ----------

post_to_dataverse_table(dataverse_api_url, 'rdr_userteamregistries', query, access_token, 100)
