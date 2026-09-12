# Databricks notebook source
import os
import requests
from functions_dataverse import get_access_token, upsert_to_dataverse_table, get_dataverse_data

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
# MAGIC Planet Questions

# COMMAND ----------

query_planetquestions = f'''
  SELECT
      QuestionID AS rdr_questionid
    , Question AS rdr_question
    , Theme AS rdr_theme
    , SubTheme AS rdr_subtheme
    , QuestionType AS rdr_questiontype
  FROM radar.planetquestions
'''

# COMMAND ----------

upsert_to_dataverse_table(dataverse_api_url, 'rdr_planetquestionses', query_planetquestions, access_token, 'rdr_questionid', 'rdr_planetquestionsid', 100)

# COMMAND ----------

# MAGIC %md
# MAGIC Planet Clients

# COMMAND ----------

query_planetclients = f'''
  SELECT
      CAST(GCID AS STRING) AS rdr_gcid
    , FullLegalName AS rdr_fulllegalname
    , Region AS rdr_region
    , Location AS rdr_location
    , BusinessLine AS rdr_businessline
    , GlobalClientOwner AS rdr_globalclientowner
    , Sector AS rdr_sector
    , SubSector AS rdr_subsector
    , NAICSCode AS rdr_naicscode
    , NAICSDescription AS rdr_naicsdescription
    , Listed AS rdr_listed
  FROM radar.planetclients
'''

# COMMAND ----------

upsert_to_dataverse_table(dataverse_api_url, 'rdr_planetclientses', query_planetclients, access_token, 'rdr_gcid', 'rdr_planetclientsid', 100)

# COMMAND ----------

# MAGIC %md
# MAGIC Planet Cases

# COMMAND ----------

query_planetcases = f'''
  SELECT
      CaseId AS rdr_caseid
    , CAST(GCID AS STRING) AS rdr_gcid
    , QCQuestionsTotal AS rdr_qcquestionstotal
    , DateCompleted AS rdr_datecompleted
    , DateCreated AS rdr_datecreated
    , ExpirationDate AS rdr_expirationdate
    , Assessor AS rdr_assessor
    , Reviewer AS rdr_reviewer
    , ISAApplicable AS rdr_isaapplicable
    , ISAConclusion AS rdr_isaconclusion
    , PerformanceScore AS rdr_performancescore
    , PolicyScore AS rdr_policyscore
    , StatusCode AS rdr_statuscode
  FROM radar.planetcases
'''

# COMMAND ----------

upsert_to_dataverse_table(dataverse_api_url, 'rdr_planetcaseses', query_planetcases, access_token, 'rdr_caseid', 'rdr_planetcasesid', 100)

# COMMAND ----------

# MAGIC %md
# MAGIC Planet Answers

# COMMAND ----------

# fetch data from Dataverse
df = get_dataverse_data(access_token, dataverse_api_url, 'rdr_planetanswerses')

# check if the Dataverse table is empty and handle accordingly
if df is None: # upsert from full Databricks table
    query_planetanswers = f'''
            SELECT
                  ID AS rdr_id
                , CaseID AS rdr_caseid
                , QuestionID AS rdr_questionid
                , Answer AS rdr_answer
                , SubThemeScorePerformance AS rdr_subthemescoreperformance
                , SubThemeScorePolicy AS rdr_subthemescorepolicy
            FROM radar.planetanswers              
            '''    

    upsert_to_dataverse_table(dataverse_api_url, 'rdr_planetanswerses', query_planetanswers, access_token, 'rdr_id', 'rdr_planetanswersid', 1000)
else: # only upsert missing Databricks rows in Dataverse
    df.createOrReplaceTempView("df_view")

    df_planetanswers_dataverse = spark.sql(f'''
            SELECT
                   rdr_id AS ID
                ,  rdr_caseid AS CaseID
                ,  rdr_questionid AS QuestionID
                ,  rdr_answer AS Answer
                ,  rdr_subthemescoreperformance AS SubThemeScorePerformance
                ,  rdr_subthemescorepolicy AS SubThemeScorePolicy                     
            FROM df_view            
        ''')

    df_planetanswers_databricks = spark.sql(f'''
            SELECT
                  ID AS rdr_id
                , CaseID AS rdr_caseid
                , QuestionID AS rdr_questionid
                , Answer AS rdr_answer
                , CAST(SubThemeScorePerformance AS LONG) AS rdr_subthemescoreperformance
                , CASE
                    WHEN SubThemeScorePolicy = '' THEN NULL
                    ELSE SubThemeScorePolicy
                  END AS rdr_subthemescorepolicy
            FROM radar.planetanswers            
            ''')
    
    # find rows that are in df_planetanswers_databricks but not in df_planetanswers_dataverse
    diff = df_planetanswers_databricks.fillna(value="").exceptAll(df_planetanswers_dataverse.fillna(value=""))

    diff.createOrReplaceTempView("diff_view")

    query_planetanswers = f'''
            SELECT
                  rdr_id
                , rdr_caseid
                , rdr_questionid
                , rdr_answer
                , rdr_subthemescoreperformance
                , rdr_subthemescorepolicy
            FROM diff_view             
            '''   

    upsert_to_dataverse_table(dataverse_api_url, 'rdr_planetanswerses', query_planetanswers, access_token, 'rdr_id', 'rdr_planetanswersid', 1000)  

