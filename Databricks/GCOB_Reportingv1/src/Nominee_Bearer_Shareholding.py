# Databricks notebook source
# MAGIC %md
# MAGIC # Configurations and Data Ingestion

# COMMAND ----------

# DBTITLE 1,Importing Libraries
import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Date Variables
date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

NB_Shareholding_dataobject = 'Nominee_Bearer_Shareholding'

# COMMAND ----------

# DBTITLE 1,Outh2 Configuration
ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Party_RiskModelInstanceQuestionAnswers'
, 'Party_RiskModelCategories'
, 'Party_RiskModelInstance'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details'
,'party_structure_Questionnaire'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Transformation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NB_InstanceQuestionAnswers AS
# MAGIC select distinct
# MAGIC InstanceId
# MAGIC ,AnswerText
# MAGIC ,QuestionText
# MAGIC ,QuestionCode
# MAGIC ,SourceClient
# MAGIC from Party_RiskModelInstanceQuestionAnswers
# MAGIC WHERE SourceSystemName = 'GCOB-CaseService' 
# MAGIC   and ClientId is not null 
# MAGIC   and QuestionCode in ('STR-G3.9','STR-G3.3','STR-G3.12','STR-Q1')
# MAGIC   and (QuestionText LIKE "%Nominee shareholders and/or nominee directors not being a TCSP%"
# MAGIC         OR QuestionText LIKE "%Is there one or more nominee shareholders%"
# MAGIC         OR QuestionText LIKE "%The presence of bearer shares in the structure of the customer%"
# MAGIC         OR QuestionText LIKE "%Does the client issue bearer shares?%"
# MAGIC   );

# COMMAND ----------

df_RiskModelInstanceQuestionAnswers=spark.table('NB_InstanceQuestionAnswers')
df_RiskModelInstanceQuestionAnswers.groupby('SourceClient', 'InstanceId').pivot('QuestionCode').agg(array_join(collect_set('AnswerText'), ',')).createOrReplaceTempView('NB_RiskModelInstanceQuestionAnswers')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NB_RiskModel AS
# MAGIC SELECT DISTINCT
# MAGIC rsk.*
# MAGIC ,pccd.GcobId
# MAGIC ,pccd.CaseId
# MAGIC ,pccd.RiskModelName
# MAGIC ,pccd.FullLegalName
# MAGIC ,pccd.CaseStatusName
# MAGIC ,pccd.ReviewTypeName
# MAGIC ,pccd.ClientLifeCycleName
# MAGIC ,pccd.IsLatestApprovedVersionOfClient
# MAGIC ,str_qst.UboThresholdHasNomineeShareholder
# MAGIC ,str_qst.UboThresholdIsBearerShareCompany
# MAGIC from NB_RiskModelInstanceQuestionAnswers as rsk
# MAGIC LEFT JOIN party_case_client_details as pccd
# MAGIC on rsk.SourceClient = pccd.SourceClient
# MAGIC LEFT JOIN (
# MAGIC   select sourceclient
# MAGIC   ,UboThresholdHasNomineeShareholder
# MAGIC   ,UboThresholdIsBearerShareCompany
# MAGIC   from party_structure_Questionnaire
# MAGIC ) as str_qst
# MAGIC on pccd.SourceClient = str_qst.SourceClient
# MAGIC WHERE pccd.CaseStatusName <> 'Cancelled';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Entity_risk AS
# MAGIC SELECT DISTINCT
# MAGIC a.SourceClient,
# MAGIC c.RiskModelname,
# MAGIC CASE WHEN b.AnswerText  LIKE "%Legal entity that issued bearer shares%" THEN 'Yes' else 'No'
# MAGIC END AS `ENT-Q1`
# MAGIC FROM Party_RiskModelCategories AS a
# MAGIC INNER JOIN Party_RiskModelInstanceQuestionAnswers AS b on a.SourceClient = b.SourceClient
# MAGIC INNER JOIN party_case_client_details as c on c.SourceClient = b.SourceClient
# MAGIC where a.categoryname = 'Entity Type'
# MAGIC and b.QuestionText LIKE "%Does the client fall under one of the following characterisation%"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NB_Shareholding AS
# MAGIC SELECT DISTINCT
# MAGIC to_date(date_sub(current_date(), 1), 'yyyy-MM-dd') AS BusinessDate
# MAGIC ,NB_risk.GcobId
# MAGIC ,NB_risk.CaseId
# MAGIC ,NB_risk.SourceClient
# MAGIC ,NB_risk.RiskModelName
# MAGIC ,NB_risk.FullLegalName
# MAGIC ,NB_risk.CaseStatusName
# MAGIC ,NB_risk.ReviewTypeName
# MAGIC ,NB_risk.ClientLifeCycleName
# MAGIC ,NB_risk.IsLatestApprovedVersionOfClient
# MAGIC ,NB_risk.`STR-G3.9` AS `Nominee shareholders and/or nominee directors not being a TCSP`
# MAGIC ,NB_risk.`STR-G3.3` AS `Is there one or more nominee shareholders`
# MAGIC ,CASE WHEN NB_risk.UboThresholdHasNomineeShareholder IS TRUE THEN 'Yes'
# MAGIC   WHEN NB_risk.UboThresholdHasNomineeShareholder IS FALSE THEN 'No' 
# MAGIC   END AS `Nominee shareholder(s) in the organizational structure of the customer`
# MAGIC ,CASE WHEN 
# MAGIC   NB_risk.`STR-G3.9` = 'Yes' 
# MAGIC   OR NB_risk.`STR-G3.3` = 'Yes' 
# MAGIC   OR NB_risk.UboThresholdHasNomineeShareholder IS TRUE THEN 'Yes'
# MAGIC ELSE 'No' END AS `Nominee shareholder`
# MAGIC ,NB_risk.`STR-G3.12` AS `The presence of bearer shares in the structure of the customer`
# MAGIC ,NB_risk.`STR-Q1` AS `Does the client issue bearer shares?`
# MAGIC ,CASE WHEN NB_risk.UboThresholdIsBearerShareCompany IS TRUE THEN 'Yes'
# MAGIC   WHEN NB_risk.UboThresholdIsBearerShareCompany IS FALSE THEN 'No'
# MAGIC   END AS `Customer is a bearer share company`
# MAGIC ,ety.`ENT-Q1` AS `Does the client fall under one of the following characterisation?`
# MAGIC ,CASE WHEN ety.`ENT-Q1` = 'Yes' THEN 'Yes'
# MAGIC   WHEN NB_risk.`STR-G3.12` = 'Yes'
# MAGIC     OR NB_risk.`STR-Q1` = 'Yes'
# MAGIC     OR NB_risk.UboThresholdIsBearerShareCompany  IS TRUE THEN 'Yes'
# MAGIC   ELSE 'No' END AS `Bearer share`
# MAGIC FROM NB_RiskModel AS NB_risk
# MAGIC LEFT JOIN Entity_risk AS ety
# MAGIC ON NB_risk.SourceClient = ety.sourceclient

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Data

# COMMAND ----------

df_NB_Shareholding = spark.table('NB_Shareholding')
save_to_saradar_storage_account(df_NB_Shareholding, NB_Shareholding_dataobject)
