# Databricks notebook source
# DBTITLE 1,run planet_createtables if one of the planet tables does not exist
from pyspark.sql.utils import AnalysisException

spark.sql('CREATE DATABASE IF NOT EXISTS radar')

def check_table_not_exists(table_name: str) -> bool:
    try:
        spark.table(table_name)
        return False
    except AnalysisException:
        return True

if check_table_not_exists('radar.planetclients') or check_table_not_exists('radar.planetcases') or check_table_not_exists('radar.planetquestions') or check_table_not_exists('radar.planetanswers'): # table does not exist
    dbutils.notebook.run('./planet_createtables', timeout_seconds = 3600)
# else:
#     print('All planet tables exist')

# COMMAND ----------

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

# DBTITLE 1,upsert_to_databricks_table
from functions_databricks import upsert_to_databricks_table

# COMMAND ----------

# DBTITLE 1,PlanetClients
from pyspark.sql import functions as F
from datetime import datetime
import re

# get max loaddate radar.PlanetClients
max_loaddate = spark.sql('SELECT MAX(LoadDate) as MaxLoadDate FROM radar.PlanetClients').collect()[0]['MaxLoadDate']

# get all loaddate grom gdp greater than max_loaddate
table = 'ClientSnapShot'
path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/1/data/'
files = dbutils.fs.ls(path)

# get all loaddate_gdp > max_loaddate
filtered_loaddate = sorted(
    {
        date for file in files
        if (date := file.path.split('LOADED_DTS=')[1][:8]) > max_loaddate.strftime('%Y%m%d')
    },
    key=lambda x: datetime.strptime(x, '%Y%m%d') # sort by date
)

for loaddate in filtered_loaddate:
    # get the most recent version available in gdp
    path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
    spark.read.parquet(f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/{version}/data/LOADED_DTS={loaddate}*/*.parquet').createOrReplaceTempView(table)
    

    df_PlanetClients_new = spark.sql(f'''
        SELECT DISTINCT
            CAST(GCID AS INT) AS GCID
            , FullLegalName
            , Region
            , Location
            , BusinessLine
            , GlobalClientOwner
            , Sector
            , SubSector
            , CAST(NAICSCode AS INT) AS NAICSCode
            , NAICSDescription
            , IsListed AS Listed
            -- , UltimateParent
            , to_date('{loaddate}', 'yyyyMMdd') AS LoadDate
        FROM ClientSnapShot
    ''')

    upsert_to_databricks_table(df_PlanetClients_new, 'radar', 'planetclients', ['GCID'])

# COMMAND ----------

# DBTITLE 1,PlanetCases
from pyspark.sql import functions as F
from datetime import datetime
import re

# get max loaddate radar.PlanetCases
max_loaddate = spark.sql('SELECT MAX(LoadDate) as MaxLoadDate FROM radar.PlanetCases').collect()[0]['MaxLoadDate']

# get all loaddate grom gdp greater than max_loaddate
table = 'Assessment'
path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/1/data/'
files = dbutils.fs.ls(path)

# get all loaddate_gdp > max_loaddate
filtered_loaddate = sorted(
    {
        date for file in files
        if (date := file.path.split('LOADED_DTS=')[1][:8]) > max_loaddate.strftime('%Y%m%d')
    },
    key=lambda x: datetime.strptime(x, '%Y%m%d') # sort by date
)


loaddate_fix = datetime.strptime('20241005', '%Y%m%d') # min date after planet FIX

planet_tables = ['Assessment', 'ClientSnapShot', 'User', 'Questionnaire', 'ISA']

for loaddate in filtered_loaddate:
    loaddate_dt = datetime.strptime(loaddate, '%Y%m%d')
    if loaddate_dt >= loaddate_fix:
        continue

    for table in planet_tables:
        # get the most recent version available in gdp
        path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/'
        files = dbutils.fs.ls(path)
        version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
        
        # adjusting for Questionnaire version, version 1 ultil 20250130
        if table == 'Questionnaire' and loaddate_dt < datetime(2025, 1, 31):
            version = 1

        spark.read.parquet(f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/{version}/data/LOADED_DTS={loaddate}*/*.parquet').createOrReplaceTempView(table)


    df_PlanetCases_new = spark.sql(f'''
        SELECT DISTINCT
            t1.PrefixCaseID AS CaseID
            , CAST(t2.GCID AS INT) AS GCID
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
            , to_date('{loaddate}', 'yyyyMMdd') AS LoadDate
            
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
    ''')

    upsert_to_databricks_table(df_PlanetCases_new, 'radar', 'planetcases', ['CaseID', 'Reviewer'])

# fix some historical cases
spark.sql("DELETE FROM radar.planetcases WHERE Reviewer = 'Bajaj, P (Prateek)' AND CaseId = 'CR956'")
spark.sql("DELETE FROM radar.planetcases WHERE Reviewer = 'Bramer, L (Lodewijk)' AND CaseId IN ('CR1938', 'CR2064', 'CR2805', 'CR3053', 'CR3205', 'CR344', 'CR351', 'CR353', 'CR375', 'CR5019', 'CR5349', 'SA4921')")
spark.sql("DELETE FROM radar.planetcases WHERE Reviewer = 'Hopkins, J (Jonathan)' AND CaseId IN ('CR1264', 'CR1266', 'CR1500', 'CR1992', 'CR424', 'NCO731')")
spark.sql("DELETE FROM radar.planetcases WHERE Reviewer = 'Minni, P (Piyush)' AND CaseId = 'NCO2880'")
spark.sql("DELETE FROM radar.planetcases WHERE Reviewer = 'Stahl, J (Joanna)' AND CaseId IN ('CR1831', 'EE1830', 'EE2722')")
spark.sql("UPDATE radar.planetcases SET Assessor = 'Hopkins, J (Jonathan)' WHERE CaseId IN ('CR1264', 'CR1266', 'CR1500', 'CR424')")
spark.sql("UPDATE radar.planetcases SET Assessor = 'Bajaj, P (Prateek)' WHERE CaseId IN ('CR956', 'NCO2880')")


for loaddate in filtered_loaddate:
    loaddate_dt = datetime.strptime(loaddate, '%Y%m%d')
    if loaddate_dt < loaddate_fix:
        continue
    
    # track tables successfully loaded
    loaded_tables = []

    for table in planet_tables:
        try:
            # get the most recent version available in gdp
            path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/'
            files = dbutils.fs.ls(path)
            version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

            # adjusting for Questionnaire version, version 1 ultil 20250130
            if table == 'Questionnaire' and loaddate_dt < datetime(2025, 1, 31):
                version = 1

            table_path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/{version}/data/LOADED_DTS={loaddate}*/*.parquet'
            spark.read.parquet(table_path).createOrReplaceTempView(table)
            loaded_tables.append(table)
            
        except Exception as e:
            print(f"Warning: Could not load {table} for loaddate {loaddate}: {str(e)}")
            continue
        

    # proceed only if all required tables are loaded
    if all(table in loaded_tables for table in planet_tables):
    
        df_PlanetCases_new = spark.sql(f'''
            SELECT DISTINCT
                t1.PrefixCaseID AS CaseID
                , CAST(t2.GCID AS INT) AS GCID
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
                , to_date('{loaddate}', 'yyyyMMdd') AS LoadDate
                
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
        ''')

        upsert_to_databricks_table(df_PlanetCases_new, 'radar', 'planetcases', ['CaseID'])

# COMMAND ----------

# DBTITLE 1,PlanetAnswers
from pyspark.sql import functions as F
from datetime import datetime
import re

# get max loaddate radar.PlanetAnswers
max_loaddate = spark.sql('SELECT MAX(LoadDate) as MaxLoadDate FROM radar.PlanetAnswers').collect()[0]['MaxLoadDate']

# get all loaddate grom gdp greater than max_loaddate
table = 'Assessment'
path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/1/data/'
files = dbutils.fs.ls(path)

# get all loaddate_gdp > max_loaddate
filtered_loaddate = sorted(
    {
        date for file in files
        if (date := file.path.split('LOADED_DTS=')[1][:8]) > max_loaddate.strftime('%Y%m%d')
    },
    key=lambda x: datetime.strptime(x, '%Y%m%d') # sort by date
)

planet_tables = ['Assessment', 'Questionnaire']

for loaddate in filtered_loaddate:
    loaddate_dt = datetime.strptime(loaddate, '%Y%m%d')

    # track tables successfully loaded
    loaded_tables = []
    
    for table in planet_tables:
        try:
            # get the most recent version available in gdp
            path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/'
            files = dbutils.fs.ls(path)
            version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

            # adjusting for Questionnaire version, version 1 ultil 20250130
            if table == 'Questionnaire' and loaddate_dt < datetime(2025, 1, 31):
                version = 1

            table_path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/{version}/data/LOADED_DTS={loaddate}*/*.parquet'
            spark.read.parquet(table_path).createOrReplaceTempView(table)
            loaded_tables.append(table)

        except Exception as e:
            print(f"Warning: Could not load {table} for loaddate {loaddate}: {str(e)}")
            continue
    
    # proceed only if all required tables are loaded
    if all(table in loaded_tables for table in planet_tables):
        df_PlanetAnswers_new = spark.sql(f'''
            SELECT DISTINCT
                CONCAT(t1.PrefixCaseID, t2.QuestionID) AS ID
                , t1.PrefixCaseID AS CaseID
                , t2.QuestionID
                , t2.AnswerOption AS Answer
                , t2.PerformanceScore AS SubThemeScorePerformance
                , t2.PolicyScore AS SubThemeScorePolicy
                , to_date('{loaddate}', 'yyyyMMdd') AS LoadDate

            FROM Assessment t1
            LEFT JOIN Questionnaire t2 ON t1.ID = t2.Questionnaire_Assessment
            WHERE AnswerOption IS NOT NULL
        ''')

        upsert_to_databricks_table(df_PlanetAnswers_new, 'radar', 'PlanetAnswers', ['ID'])

# COMMAND ----------

# DBTITLE 1,PlanetQuestions
from pyspark.sql import functions as F
from datetime import datetime
import re

# get max loaddate radar.PlanetQuestions
max_loaddate = spark.sql('SELECT MAX(LoadDate) as MaxLoadDate FROM radar.PlanetQuestions').collect()[0]['MaxLoadDate']

# get all loaddate grom gdp greater than max_loaddate
table = 'Questionnaire'
path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/1/data/'
files = dbutils.fs.ls(path)

# get all loaddate_gdp > max_loaddate
filtered_loaddate = sorted(
    {
        date for file in files
        if (date := file.path.split('LOADED_DTS=')[1][:8]) > max_loaddate.strftime('%Y%m%d')
    },
    key=lambda x: datetime.strptime(x, '%Y%m%d') # sort by date
)

for loaddate in filtered_loaddate:
    loaddate_dt = datetime.strptime(loaddate, '%Y%m%d')

    path = f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

    # adjusting for Questionnaire version, version 1 ultil 20250130
    if table == 'Questionnaire' and loaddate_dt < datetime(2025, 1, 31):
        version = 1

    spark.read.parquet(f'abfss://planet@edlcorestdeuprod0001.dfs.core.windows.net/{table}/{version}/data/LOADED_DTS={loaddate}*/*.parquet').createOrReplaceTempView(table)
    

    df_PlanetQuestions_new = spark.sql(f'''
        SELECT DISTINCT
            t1.QuestionID
            , t1.QuestionTitle AS Question
            , t1.ThemeTitle AS Theme
            , t1.SubThemeOrPolicyTitle AS SubTheme
            , t1.QuestionType
            , to_date('{loaddate}', 'yyyyMMdd') AS LoadDate
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
    ''')

    upsert_to_databricks_table(df_PlanetQuestions_new, 'radar', 'PlanetQuestions', ['QuestionID'])

# COMMAND ----------

# %sql
# drop table if exists radar.PlanetAttributeClient

# COMMAND ----------

# DBTITLE 1,PlanetAttributeClient
# spark.sql('''
# SELECT DISTINCT
#   GCID
#   , 'Region' AS AttributeType
#   , `Region` AS AttributeValue
# FROM radar.planetclients

# UNION ALL

# SELECT DISTINCT
#   GCID
#   , 'BusinessLine' AS AttributeType
#   , BusinessLine AS AttributeValue
# FROM radar.planetclients
# ''').write.mode('overwrite').saveAsTable('radar.PlanetAttributeClient')
