# Databricks notebook source
import os

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

from datetime import datetime, timedelta
import re

planet_tables = [
    'Assessment'
    , 'ClientSnapShot'
    , 'ISA'
    , 'Questionnaire'
    , 'User'
]

for table in planet_tables:
    # get the most recent version available in gdp
    path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

    # adjusting for Questionnaire version
    if table == 'Questionnaire':
        version = 1

    # get the first file available in gdp
    path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = min(file.path.split('LOADED_DTS=')[1][:8] for file in files if 'LOADED_DTS=' in file.path)

    # load_date = '20241005' # min date after planet FIX

    spark.read.parquet(f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/{version}/data/LOADED_DTS={load_date}*/*.parquet').createOrReplaceTempView(table)

# COMMAND ----------

# DBTITLE 1,check_table_not_exists
from functions_databricks import check_table_not_exists

# COMMAND ----------

# DBTITLE 1,PlanetClients
from pyspark.sql.functions import lit, to_date

if check_table_not_exists('radar.planetclients'):
    spark.sql('''
            SELECT DISTINCT
                CAST(GCID AS INT)
                , FullLegalName
                , Region
                , `Location`
                , BusinessLine
                , GlobalClientOwner
                , Sector
                , SubSector
                , CAST(NAICSCode AS INT)
                , NAICSDescription
                , IsListed AS Listed
                -- , UltimateParent
            FROM ClientSnapShot
    ''').withColumn('LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.planetclients')


# COMMAND ----------

# DBTITLE 1,PlanetCases
from pyspark.sql.functions import lit, to_date

if check_table_not_exists('radar.planetcases'):
    spark.sql('''
            SELECT DISTINCT
                t1.PrefixCaseID AS CaseID
                , CAST(t2.GCID AS INT)
                , COALESCE(t6.QCQuestionsTotal, 0) AS QCQuestionsTotal
                , t1.CompleteDate AS DateCompleted
                , t1.AssessmentCreatedDate AS DateCreated
                , t1.ExpirationDate
                , t3.FullName AS Assessor
                , t4.FullName AS Reviewer
                , t5.Applicable AS ISAApplicable
                , t5.Conclusion AS ISAConclusion
                , t1.PerformanceScore AS PerformanceScore
                , t1.PolicyScore
                , t1.Status AS StatusCode
                
            FROM Assessment t1
            LEFT JOIN ClientSnapShot t2 ON t1.ID = t2.Assessment_ClientSnapShot
            LEFT JOIN (SELECT * FROM User WHERE UserRole = 'Assessor') t3 ON t1.ID = t3.Account_Assessment
            LEFT JOIN (SELECT * FROM User WHERE UserRole = 'Reviewer') t4 ON t1.ID = t4.Account_Assessment
            LEFT JOIN ISA t5 ON t1.ID = t5.Assessment_ISA
            LEFT JOIN (
                SELECT 
                    Questionnaire_Assessment
                    , COUNT(*) AS QCQuestionsTotal 
                FROM Questionnaire
                WHERE AnswerOption IS NOT NULL
                GROUP BY Questionnaire_Assessment
            ) t6 ON t1.ID = t6.Questionnaire_Assessment
    ''').withColumn('LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.planetcases')


# COMMAND ----------

# DBTITLE 1,PlanetAnswers
from pyspark.sql.functions import lit, to_date

if check_table_not_exists('radar.planetanswers'):
    spark.sql('''
            SELECT DISTINCT
                CONCAT(t1.PrefixCaseID, t2.QuestionID) AS ID
                , t1.PrefixCaseID AS CaseID
                , t2.QuestionID
                , t2.AnswerOption AS Answer
                , t2.PerformanceScore AS SubThemeScorePerformance
                , t2.PolicyScore AS SubThemeScorePolicy
                
            FROM Assessment t1
            LEFT JOIN Questionnaire t2 ON t1.ID = t2.Questionnaire_Assessment
            WHERE t2.AnswerOption IS NOT NULL
    ''').withColumn('LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.planetanswers')


# COMMAND ----------

# DBTITLE 1,PlanetQuestions
from pyspark.sql.functions import lit, to_date

if check_table_not_exists('radar.planetquestions'):
    spark.sql('''
            SELECT DISTINCT
                t1.QuestionID
                , t1.QuestionTitle AS Question
                , t1.ThemeTitle AS Theme
                , t1.SubThemeOrPolicyTitle AS SubTheme
                , t1.QuestionType
            FROM Questionnaire t1
            JOIN (
                SELECT 
                    QuestionID
                    , MAX(QuestionnaireCreateData) AS MostRecentDate
                FROM 
                    Questionnaire
                GROUP BY 
                    QuestionID
            ) t2 ON t1.QuestionID = t2.QuestionID AND t1.QuestionnaireCreateData = t2.MostRecentDate
    ''').withColumn('LoadDate', to_date(lit(load_date), 'yyyyMMdd')).write.mode('overwrite').saveAsTable('radar.planetquestions')
