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

# MAGIC %md
# MAGIC SQL Database

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

spark.read.jdbc(url=jdbcUrl,table='ext.Cases',properties = connectionProperties).createOrReplaceTempView('ExtCases')

spark.read.jdbc(url=jdbcUrl,table='ext.Clients',properties = connectionProperties).createOrReplaceTempView('ExtClients')

# COMMAND ----------

# MAGIC %md
# MAGIC Dataverse

# COMMAND ----------

dataverse_url = os.environ['DATAVERSE_URL']
dataverse_api_url = f"{dataverse_url}/api/data/v9.1/"

# COMMAND ----------

access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_clientssectorteamses')
df.createOrReplaceTempView("SectorTeam")

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_clientsreviewlocationses')
df.createOrReplaceTempView("ReviewLocation")

# COMMAND ----------

# MAGIC %md
# MAGIC Onboardings

# COMMAND ----------

# non matching sector team
non_matching_sector_team = '''
  SELECT DISTINCT
    TRIM(SectorTeam) AS SectorTeam
  FROM ExtCases ca
	  LEFT JOIN ExtClients cl
		  ON TRIM(ca.GcobId) = TRIM(cl.GcobId)
    LEFT JOIN SectorTeam st
        ON st.rdr_name = TRIM(cl.SectorTeam) 
  WHERE st.rdr_name IS NULL
    AND TRIM(cl.SectorTeam) IS NOT NULL
    AND `Case review type` = 'Fast Migration' 
  ORDER BY SectorTeam
'''

df = spark.sql(non_matching_sector_team)

display(df)

# COMMAND ----------

# non matching review location
non_matching_review_location = ''' 
  SELECT DISTINCT
    TRIM(ReviewLocation) AS ReviewLocation
  FROM ExtCases ca
	  LEFT JOIN ExtClients cl
		  ON TRIM(ca.GcobId) = TRIM(cl.GcobId)
    LEFT JOIN ReviewLocation rl
        ON rl.rdr_reviewlocation = TRIM(cl.ReviewLocation) 
  WHERE rl.rdr_reviewlocation IS NULL
    AND TRIM(cl.ReviewLocation ) IS NOT NULL
    AND `Case review type` = 'Fast Migration' 
  ORDER BY ReviewLocation 
'''

df = spark.sql(non_matching_review_location)

display(df)

# COMMAND ----------

query_onboardings = '''
  SELECT
     CAST(ROW_NUMBER() OVER (ORDER BY TRIM(ca.GcobId), `Start date prework`) AS STRING) AS Id
    ,TRIM(ca.`Client name`) AS ClientName
    ,TRIM(ca.GcobId) AS UniqueGcobId
    ,TRIM(SectorTeam) AS SectorTeam
    ,TRIM(ReviewLocation) AS ReviewLocation
    ,CAST(NULL AS STRING) AS CurrentStatus
    ,CAST(NULL AS STRING) AS Deadline
    ,ca.`Global Client Owner` AS GlobalClientOwner
    ,`Start date prework` AS InsertedOnDate
    ,CAST(NULL AS DATE) AS KYCPlannedStartDate
    ,CAST(NULL AS STRING) AS MainProductCategory
    ,CAST(NULL AS STRING) AS OnboardingReason
    ,'Fast Migration' AS OnboardingType
    ,KYCGroup
    ,CAST(NULL AS STRING)AS Remark
    ,'Firebird' AS Source	
  FROM ExtCases ca
    LEFT JOIN ExtClients cl
      ON TRIM(ca.GcobId) = TRIM(cl.GcobId)
  WHERE `Case review type` = 'Fast Migration' 
'''

onboardings_df = spark.sql(query_onboardings)

display(onboardings_df)

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
