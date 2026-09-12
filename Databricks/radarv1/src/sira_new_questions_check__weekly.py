# Databricks notebook source
import os
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
from pyspark.sql import functions as F

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

import re

# load and create temp views of all gdp_tables below
gcob_tables = [
    'RiskModel_dbo_Question'
    , 'RiskModel_dbo_Category'
    , 'RiskModel_dbo_Model'
    , 'RiskModel_dbo_CddType'
    # , 'RiskModel_dbo_GroupQuestion'
    # , 'RiskModel_dbo_Classification'
    # , 'RiskModel_dbo_CategoryQuestion'
    # , 'RiskModel_dbo_ModelRiskLevel'
    # , 'RiskModel_dbo_QuestionType'
    # , 'RiskModel_dbo_Theme'
]

for item in gcob_tables:
    # get the most recent version available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])


    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOADED_DTS=')[1][:8] for file in files if 'LOADED_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/{version}/data/LOADED_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW riskmodel_question_list AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.Id AS QuestionId
# MAGIC   , t1.Title
# MAGIC   , t1.Text AS Question
# MAGIC   , t1.QuestionCode
# MAGIC   , t1.HasMaterialRiskQuestion
# MAGIC   , t2.name AS CategoryName
# MAGIC   , t2.type AS CategoryType
# MAGIC   , t3.Name AS ModelName
# MAGIC   , t4.ValidForLegalEntity
# MAGIC   , t4.ValidForNaturalPerson
# MAGIC   , t4.Description AS CddTypeDescription
# MAGIC FROM RiskModel_dbo_Question t1
# MAGIC LEFT JOIN RiskModel_dbo_Category t2 ON t1.id = t2.id
# MAGIC LEFT JOIN RiskModel_dbo_Model t3 ON t2.ModelId = t3.id
# MAGIC LEFT JOIN RiskModel_dbo_CddType t4 ON t3.id = t4.id
# MAGIC
# MAGIC -- THIS SHOULD BE 900 SOMEHTING QUESTIONS!!

# COMMAND ----------

def get_table_count(primary_query, fallback_query):
    try:
        return spark.sql(primary_query).collect()[0]['cnt']
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"Primary table not found. Trying fallback: {fallback_query}")
            return spark.sql(fallback_query).collect()[0]['cnt']
        else:
            raise

# query list
query_old = "SELECT COUNT(*) AS cnt FROM radar.riskmodel_question_list"
query_new = "SELECT COUNT(*) AS cnt FROM riskmodel_question_list"

count_old = get_table_count(query_old, query_new) # fallback to query_new only for the first time where the table does not exists

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.riskmodel_question_list

# COMMAND ----------

spark.sql('SELECT * FROM riskmodel_question_list').write.mode('overwrite').saveAsTable('radar.riskmodel_question_list')

# COMMAND ----------

count_new = get_table_count(query_new, query_new) # no fallback needed for new

# compare and fail job if mismatch in count
if count_new != count_old:
    raise ValueError(f'There are {count_new - count_old} new Risk Model questions.')
