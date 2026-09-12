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

spark.read.jdbc(url=jdbcUrl,table='pa.PlanetClientData',properties = connectionProperties).createOrReplaceTempView('PlanetClientData')
spark.read.jdbc(url=jdbcUrl,table='pa.PlanetAnswerData',properties = connectionProperties).createOrReplaceTempView('PlanetAnswerData')

# COMMAND ----------

dataverse_url = os.environ['DATAVERSE_URL']
dataverse_api_url = f"{dataverse_url}/api/data/v9.1/"

# COMMAND ----------

access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_projectofficers')
df.createOrReplaceTempView("ProjectOfficer")

# COMMAND ----------

# MAGIC %md
# MAGIC PlanetClientData

# COMMAND ----------

# view/check non migrated records
not_migrated_planet_client_data = '''
  SELECT
     pcd.CASEID AS CaseID
    ,QCAnswerdBy
    ,CASE QCCompleted
      WHEN 1 THEN 'true'
      WHEN 0 THEN 'false'
     END AS QCCompleted
    ,QCCompletedBy
    ,QCCompletedDate
  FROM PlanetClientData pcd
    LEFT JOIN radar.planetcases pc
      ON pc.CaseID = pcd.CASEID
  WHERE pc.CaseID IS NULL   
'''

df = spark.sql(not_migrated_planet_client_data)

display(df)

# COMMAND ----------

query_planetclientdata = '''
  SELECT
     pcd.CASEID AS rdr_caseid
    ,CONCAT('/rdr_projectofficers(', rdr_projectofficerid, ')') AS rdr_ProjectOfficer_lookup
    ,CASE QCCompleted
      WHEN 1 THEN 'true'
      WHEN 0 THEN 'false'
     END AS rdr_qccompleted
    ,QCCompletedBy AS rdr_qccompletedby
    ,CASE
        WHEN QCCompletedDate BETWEEN '2024-03-31' AND '2024-10-27' THEN DATEADD(hour, -2, QCCompletedDate) -- Summer time
        ELSE DATEADD(hour, -1, QCCompletedDate) -- Winter time
     END AS rdr_qccompleteddate
  FROM PlanetClientData pcd
    JOIN radar.planetcases pc
      ON pc.CaseID = pcd.CASEID
    JOIN ProjectOfficer po
      ON pcd.QCAnswerdBy = po.rdr_projectofficername  
'''

# COMMAND ----------

truncate_dataverse_table(dataverse_api_url, 'rdr_planetcasedatas', access_token, 'rdr_planetcasedataid', 100)

# COMMAND ----------

post_to_dataverse_table(dataverse_api_url, 'rdr_planetcasedatas', query_planetclientdata, access_token, 100)

# COMMAND ----------

# MAGIC %md
# MAGIC PlanetAnswerData

# COMMAND ----------

# view/check non migrated records
not_migrated_planet_answer_data = '''
  SELECT
     CONCAT(pad.CASEID, '-', pad.QuestionId) AS ID
    ,pad.CASEID AS CaseID
    ,pad.QuestionId
    ,CASE QCAssessment
      WHEN 1 THEN 'true'
      WHEN 0 THEN 'false'
     END AS QCAssessment
    ,CASE QCPolicySelection
      WHEN 1 THEN 'true'
      WHEN 0 THEN 'false'
     END AS QCPolicySelection
    ,QCCreatedBy
    ,QCCreationDate
    ,QCObservationFollowUp
    ,QCRemarks AS QCRemark
  FROM PlanetAnswerData pad
    LEFT JOIN radar.planetcases pc
      ON pc.CaseID = pad.CASEID
    LEFT JOIN radar.planetquestions pq
      ON pq.QuestionID = pad.QuestionID
  WHERE pc.CaseID IS NULL
    OR pq.QuestionID IS NULL  
'''

df = spark.sql(not_migrated_planet_answer_data)

display(df)

# COMMAND ----------

query_planetanswerdata = '''
  SELECT
     CONCAT(pad.CASEID, '-', pad.QuestionId) AS rdr_id
    ,pad.CASEID AS rdr_caseid
    ,pad.QuestionId AS rdr_questionid
    ,CASE QCAssessment
      WHEN 1 THEN 'true'
      WHEN 0 THEN 'false'
     END AS rdr_qcassessment
    ,CASE QCPolicySelection
      WHEN 1 THEN 'true'
      WHEN 0 THEN 'false'
     END AS rdr_qcpolicyselection
    ,QCCreatedBy AS rdr_qccreatedby
    ,CASE
        WHEN QCCreationDate BETWEEN '2024-03-31' AND '2024-10-27' THEN DATEADD(hour, -2, QCCreationDate) -- Summer time
        ELSE DATEADD(hour, -1, QCCreationDate) -- Winter time
     END AS rdr_qccreationdate
    ,CASE QCObservationFollowUp 
        WHEN 'No observation' THEN 1
        WHEN 'Immediate follow up' THEN 2
        WHEN 'Follow up next review' THEN 3
        WHEN 'No follow up needed' THEN 4        
     END AS rdr_qcobservationfollowup
    ,QCRemarks AS rdr_qcremark
  FROM PlanetAnswerData pad
    JOIN radar.planetcases pc
      ON pc.CaseID = pad.CASEID 
    JOIN radar.planetquestions pq
      ON pq.QuestionID = pad.QuestionID
'''

# COMMAND ----------

truncate_dataverse_table(dataverse_api_url, 'rdr_planetanswerdatas', access_token, 'rdr_planetanswerdataid', 1000)

# COMMAND ----------

post_to_dataverse_table(dataverse_api_url, 'rdr_planetanswerdatas', query_planetanswerdata, access_token, 1000)
