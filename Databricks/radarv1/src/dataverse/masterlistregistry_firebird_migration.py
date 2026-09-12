# Databricks notebook source
import os
import requests
from functions_dataverse import get_access_token, get_dataverse_data, truncate_dataverse_table, post_to_dataverse_table

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

spark.read.jdbc(url=jdbcUrl,table='pa.KYCMasterListRegistry_DataMigration',properties = connectionProperties).createOrReplaceTempView('MasterListRegistry')

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

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_clientsreasons')
df.createOrReplaceTempView("Reason")

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_clientses')
df.createOrReplaceTempView("Clients")

# COMMAND ----------

# MAGIC %md
# MAGIC MasterListRegistry

# COMMAND ----------

# view non matching GcobId's
non_matching_gcob = '''
  SELECT DISTINCT
     REPLACE(mlr.GCOBID, 'NP: ', 'NP_') AS GcobId
    ,MIN(recordactive_at) AS MinRecordActiveAt
    ,MAX(recordactive_at) AS MaxRecordActiveAt
    ,MIN(recordexpired_at) AS MinRecordExpiredAt
    ,MAX(recordexpired_at) AS MaxRecordExpiredAt
  FROM MasterListRegistry mlr    
    LEFT JOIN radar.cases ca
        ON TRIM(ca.UniqueGcobId) = REPLACE(TRIM(mlr.GCOBID), 'NP: ', 'NP_')  
  WHERE ca.UniqueGcobId IS NULL
  GROUP BY REPLACE(mlr.GCOBID, 'NP: ', 'NP_')
  ORDER BY GcobId
'''

df = spark.sql(non_matching_gcob)

display(df)

# COMMAND ----------

# non matching sector team
non_matching_sector_team_mlr = '''
  SELECT DISTINCT
     SectorTeam
  FROM MasterListRegistry mlr    
    LEFT JOIN SectorTeam st
        ON st.rdr_name = mlr.SectorTeam  
  WHERE st.rdr_name IS NULL
    AND mlr.SectorTeam IS NOT NULL
  ORDER BY SectorTeam
'''

df = spark.sql(non_matching_sector_team_mlr)

display(df)

# COMMAND ----------

# non matching review location
non_matching_review_location_mlr = '''
  SELECT DISTINCT
     ReviewLocation
  FROM MasterListRegistry mlr    
    LEFT JOIN ReviewLocation rl
        ON rl.rdr_reviewlocation = mlr.ReviewLocation  
  WHERE rl.rdr_reviewlocation IS NULL
    AND mlr.ReviewLocation IS NOT NULL
  ORDER BY ReviewLocation
'''

df = spark.sql(non_matching_review_location_mlr)

display(df)

# COMMAND ----------

# non matching reason
non_matching_reason_mlr = '''
  SELECT DISTINCT
     Reason
  FROM MasterListRegistry mlr    
    LEFT JOIN Reason rl
        ON rl.rdr_reason = mlr.Reason
  WHERE rl.rdr_reason IS NULL
    AND mlr.Reason IS NOT NULL
  ORDER BY Reason
'''

df = spark.sql(non_matching_reason_mlr)

display(df)

# COMMAND ----------

query_masterlistregistry = '''
  SELECT DISTINCT
     mlr.GCOBID AS rdr_uniquegcobid
    ,CONCAT('/rdr_clientses(', rdr_clientsid, ')') AS rdr_SourceClient_lookup
    ,KYCGroup AS rdr_kycgroup
    ,SectorTeam AS rdr_sectorteam
    ,ReviewLocation AS rdr_reviewlocation
    ,CDDExecution AS rdr_cddexecution
    ,ClientCaseInitiationStart AS rdr_clientcaseinitiationstart
    ,Reason AS rdr_reason
    ,Explanation AS rdr_reasonexplanation
    ,`London Sector Team` AS rdr_londonsectorteam
    ,RecordActive_At AS rdr_recordactiveat
    ,RecordExpired_At AS rdr_recordexpiredat
    ,"Firebird" AS rdr_source
  FROM MasterListRegistry mlr    
    JOIN radar.cases ca
        ON TRIM(ca.UniqueGcobId) = TRIM(mlr.GCOBID)  
    LEFT JOIN Clients cl
      ON mlr.GCOBId = cl.rdr_uniquegcobid
'''

# COMMAND ----------

truncate_dataverse_table(dataverse_api_url, 'rdr_masterlistregistries', access_token, 'rdr_masterlistregistryid', 1000)

# COMMAND ----------

post_to_dataverse_table(dataverse_api_url, 'rdr_masterlistregistries', query_masterlistregistry, access_token, 1000)

# COMMAND ----------

# MAGIC %md
# MAGIC Ext.Clients

# COMMAND ----------

# non matching sector team
non_matching_sector_team_exc = '''
  SELECT DISTINCT
     SectorTeam
  FROM ExtClients ec  
    LEFT JOIN SectorTeam st
        ON st.rdr_name = ec.SectorTeam  
  WHERE st.rdr_name IS NULL
    AND ec.SectorTeam IS NOT NULL
  ORDER BY SectorTeam
'''

df = spark.sql(non_matching_sector_team_exc)

display(df)

# COMMAND ----------

# non matching review location
non_matching_review_location_exc = '''
  SELECT DISTINCT
     ReviewLocation
  FROM ExtClients ec    
    LEFT JOIN ReviewLocation rl
        ON rl.rdr_reviewlocation = ec.ReviewLocation  
  WHERE rl.rdr_reviewlocation IS NULL
    AND ec.ReviewLocation IS NOT NULL
  ORDER BY ReviewLocation
'''

df = spark.sql(non_matching_review_location_exc)

display(df)

# COMMAND ----------

# non matching reason
non_matching_reason_exc = '''
  SELECT DISTINCT
     Reason
  FROM ExtClients ec    
    LEFT JOIN Reason rl
        ON rl.rdr_reason = ec.Reason
  WHERE rl.rdr_reason IS NULL
    AND ec.Reason IS NOT NULL
  ORDER BY Reason
'''

df = spark.sql(non_matching_reason_exc)

display(df)

# COMMAND ----------

query_extclients = '''
  WITH MinSignOffDateCTE AS
    (
      SELECT
          UniqueGcobId
         ,MIN(SignOff) AS MinDate         
      FROM radar.cases
      WHERE CaseReviewType IN ('Initial On-Boarding', 'Urgent Onboarding', 'Migration Onboarding', 'Fast Migration')
      GROUP BY UniqueGcobId
    ),

  MinPreworkDateCTE AS
    (
      SELECT
          UniqueGcobId
         ,MIN(Prework) AS MinDate         
      FROM radar.cases
      WHERE CaseReviewType NOT IN ('Initial On-Boarding', 'Urgent Onboarding', 'Migration Onboarding', 'Fast Migration')
      GROUP BY UniqueGcobId
    ),

  ActiveDateCTE AS
    (   
      SELECT *
      FROM MinSignoffDateCTE
        
      UNION ALL
        
      SELECT *
      FROM MinPreworkDateCTE
     ),  

  MinActiveDateCTE
    (
      SELECT
         UniqueGcobId
        ,MIN(MinDate) AS MinActiveDate
      FROM ActiveDateCTE
      GROUP BY UniqueGcobId
    )

  SELECT DISTINCT
     REPLACE(exc.GcobId, 'NP: ', 'NP_') AS rdr_uniquegcobid
    ,CONCAT('/rdr_clientses(', rdr_clientsid, ')') AS rdr_SourceClient_lookup
    ,exc.KYCGroup AS rdr_kycgroup
    ,exc.SectorTeam AS rdr_sectorteam
    ,exc.ReviewLocation AS rdr_reviewlocation
    ,exc.CDDExecution AS rdr_cddexecution
    ,exc.ClientCaseInitiationStart AS rdr_clientcaseinitiationstart
    ,exc.Reason AS rdr_reason
    ,exc.Explanation AS rdr_reasonexplanation
    ,exc.`London Sector Team` AS rdr_londonsectorteam    
    ,CASE 
      WHEN a.MinActiveDate IS NULL THEN '2025-03-01 00:00:00'
      ELSE a.MinActiveDate
     END AS rdr_recordactiveat
    ,"rpt.clients" AS rdr_source
  FROM ExtClients exc     
    JOIN radar.cases ca
        ON TRIM(ca.UniqueGcobId) = TRIM(REPLACE(exc.GcobId, 'NP: ', 'NP_'))  
    LEFT JOIN Clients cl
      ON REPLACE(exc.GcobId, 'NP: ', 'NP_') = cl.rdr_uniquegcobid
    LEFT JOIN MasterListRegistry mlr
      ON REPLACE(mlr.GcobId, 'NP: ', 'NP_') = REPLACE(exc.GcobId, 'NP: ', 'NP_') 
    LEFT JOIN MinActiveDateCTE a
      ON TRIM(REPLACE(exc.GcobId, 'NP: ', 'NP_')) = TRIM(a.UniqueGcobId)
   WHERE REPLACE(mlr.GcobId, 'NP: ', 'NP_') IS NULL
    AND exc.SectorTeam IS NOT NULL
'''

df = spark.sql(query_extclients)

display(df)

# COMMAND ----------

post_to_dataverse_table(dataverse_api_url, 'rdr_masterlistregistries', query_extclients, access_token, 1000)
