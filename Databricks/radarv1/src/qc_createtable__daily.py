# Databricks notebook source
# MAGIC %md
# MAGIC # Load and create tables for QC dashboard

# COMMAND ----------

# DBTITLE 1,connection to gdp
import os

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

jdbcHostname='firebirdsqlserverprodnla.database.windows.net'
ServiceKey='app-reg-databricks-wr-radar-preprd'

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

# DBTITLE 1,load QC review
from datetime import datetime, timedelta
import re

item = 'party_case_QualityControlReview_Details'
version = 101

# get the most recent file available in gdp
path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/'
files = dbutils.fs.ls(path)
load_date = max(file.path.split('LOAD_DT=')[1][:8] for file in files if 'LOAD_DT=' in file.path)

spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/LOAD_DT={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# DBTITLE 1,load ConsultationType
from datetime import datetime, timedelta
import re

item = 'ConsultationType'
version = 101

# get the most recent file available in gdp
path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/'
files = dbutils.fs.ls(path)
load_date = max(file.path.split('EDL_LOAD_DTS=')[1][:8] for file in files if 'EDL_LOAD_DTS=' in file.path)

spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/EDL_LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# DBTITLE 1,create QualityControl view
# MAGIC %sql
# MAGIC /*
# MAGIC CREATE OR REPLACE TEMPORARY VIEW QualityControl AS
# MAGIC
# MAGIC WITH Consultation AS (
# MAGIC     SELECT GcobId, CaseId, ConsultationRequired, count(ConsultationType) AS NumberOfConsultationTypes
# MAGIC     FROM ConsultationType
# MAGIC     GROUP BY GcobId, CaseId, ConsultationRequired
# MAGIC     ORDER BY GcobId, CaseId, ConsultationRequired
# MAGIC )
# MAGIC , QC_with_consultation AS (
# MAGIC SELECT A.*, CAST(A.PostCreatedDate AS date) AS PostCreatedShortDate, ConsultationRequired, NumberOfConsultationTypes
# MAGIC FROM party_case_QualityControlReview_Details A
# MAGIC LEFT JOIN Consultation B
# MAGIC     ON A.GcobId = B.GcobId
# MAGIC     AND A.CaseId = B.CaseId
# MAGIC )
# MAGIC , ReviewNumber AS (
# MAGIC SELECT concat(GcobId, '_',CaseId) AS UniqueId,*, ROW_NUMBER() OVER (PARTITION BY A.GcobId, A.TestElementName, A.IssueTitle ORDER BY A.PostCreatedDate DESC) AS ReviewNumber
# MAGIC FROM QC_with_consultation A
# MAGIC )
# MAGIC SELECT A.*, B.Team AS QCTeam, B.Department AS QCDepartment
# MAGIC FROM ReviewNumber A
# MAGIC LEFT JOIN radar.UserTeamRegistry B
# MAGIC     ON A.PostCreatedBy = B.UserName
# MAGIC     AND A.PostCreatedShortDate BETWEEN B.TeamStartDate AND COALESCE(B.TeamEndDate,'9999-12-31');
# MAGIC     */

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW QualityControl AS
# MAGIC
# MAGIC -- Step 1: Aggregate consultation data
# MAGIC WITH Consultation AS (
# MAGIC     SELECT 
# MAGIC         GcobId, 
# MAGIC         CaseId, 
# MAGIC         ConsultationRequired, 
# MAGIC         COUNT(ConsultationType) AS NumberOfConsultationTypes
# MAGIC     FROM ConsultationType
# MAGIC     GROUP BY GcobId, CaseId, ConsultationRequired
# MAGIC     ORDER BY GcobId, CaseId, ConsultationRequired
# MAGIC )
# MAGIC
# MAGIC -- Step 2: Join consultation and team registry
# MAGIC , QC_with_team AS (
# MAGIC     SELECT 
# MAGIC         A.*, 
# MAGIC         CAST(A.PostCreatedDate AS DATE) AS PostCreatedShortDate, 
# MAGIC         B.ConsultationRequired, 
# MAGIC         B.NumberOfConsultationTypes,
# MAGIC         C.Team AS QCTeam, 
# MAGIC         C.Department AS QCDepartment,
# MAGIC         CASE 
# MAGIC             WHEN A.FinalMarking IS NOT NULL THEN 'No'
# MAGIC             ELSE 'Yes'
# MAGIC         END AS FirstTimeRight
# MAGIC     FROM party_case_QualityControlReview_Details A
# MAGIC     LEFT JOIN Consultation B
# MAGIC         ON A.GcobId = B.GcobId AND A.CaseId = B.CaseId
# MAGIC     LEFT JOIN radar.UserTeamRegistry C
# MAGIC         ON A.PostCreatedBy = C.UserName
# MAGIC         AND CAST(A.PostCreatedDate AS DATE) BETWEEN C.TeamStartDate AND COALESCE(C.TeamEndDate, '9999-12-31')
# MAGIC )
# MAGIC
# MAGIC -- Step 3: Assign ReviewNumber only for allowed teams
# MAGIC , ReviewNumberFiltered AS (
# MAGIC     SELECT 
# MAGIC         concat(GcobId, '_', CaseId) AS UniqueId,
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY concat(GcobId, '_', CaseId), TestElementName
# MAGIC             ORDER BY PostCreatedDate
# MAGIC         ) AS ReviewNumber
# MAGIC     FROM QC_with_team
# MAGIC     WHERE QCTeam NOT IN ('QC - FI', 'QC - NL', 'QC - UK')
# MAGIC )
# MAGIC
# MAGIC -- Step 4: Merge ReviewNumber back to full dataset
# MAGIC SELECT 
# MAGIC     A.*
# MAGIC     -- , CONCAT(A.SourceClient, A.TestElementDescription) AS SourceClient_TestElementDescription
# MAGIC     , COALESCE(B.ReviewNumber, NULL) AS ReviewNumber
# MAGIC FROM QC_with_team A
# MAGIC LEFT JOIN ReviewNumberFiltered B
# MAGIC     ON A.GcobId = B.GcobId 
# MAGIC     AND A.CaseId = B.CaseId 
# MAGIC     AND A.TestElementName = B.TestElementName 
# MAGIC     AND A.PostCreatedDate = B.PostCreatedDate;
# MAGIC

# COMMAND ----------

# DBTITLE 1,store table to catalog
spark.sql('DROP TABLE IF EXISTS radar.QualityControl')

spark.sql('SELECT * FROM QualityControl').write.mode('overwrite').saveAsTable('radar.QualityControl')

# COMMAND ----------

# DBTITLE 1,PivotQualityControl
# MAGIC %sql
# MAGIC /*
# MAGIC Create pivot of above table
# MAGIC */
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW PivotQualityControl AS
# MAGIC
# MAGIC SELECT
# MAGIC   SourceClient
# MAGIC   , `Not adequate - material` AS NotAdequateMaterial
# MAGIC   , `Housekeeping - completed` AS HousekeepingCompleted
# MAGIC   , `Overturned` AS Overturned
# MAGIC   , `Dependent error` AS DependentError
# MAGIC   , `Not adequate - non material` AS NotAdequateNonMaterial
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     SourceClient
# MAGIC     , FinalMarking
# MAGIC   FROM radar.QualityControl
# MAGIC   -- WHERE FinalMarking IS NOT NULL
# MAGIC ) grouped
# MAGIC PIVOT (
# MAGIC   COUNT(FinalMarking)
# MAGIC   FOR FinalMarking IN (
# MAGIC     'Not adequate - material'
# MAGIC     , 'Housekeeping - completed'
# MAGIC     , 'Overturned'
# MAGIC     , 'Dependent error'
# MAGIC     , 'Not adequate - non material'
# MAGIC   )
# MAGIC )

# COMMAND ----------

# DBTITLE 1,store PivotQualityControl with SignOff from cases
spark.sql('DROP TABLE IF EXISTS radar.PivotQualityControl')

spark.sql('''
        SELECT
            t1.*
            , t2.Signoff
        FROM PivotQualityControl t1
        LEFT JOIN radar.cases t2 ON t1.SourceClient = t2.SourceClient
''').write.mode('overwrite').saveAsTable('radar.PivotQualityControl')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create N2K_combined table by combining N2k_UserAttribute and N2K_ClientAttribute

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS radar.N2K_combined')

# spark.sql('''
#         SELECT DISTINCT
#             t2.UPN
#             , t1.UniqueGcobId
#             , t1.AttributeValue
#         FROM radar.N2K_ClientAttribute t1
#         LEFT JOIN radar.N2k_UserAttribute t2 ON t1.AttributeValue = t2.AttributeValue
#         WHERE t1.AttributeValue IN (
#             'RegionRANZ'
#             , 'RegionSA'
#             , 'RegionNA'
#             , 'RegionAsia'
#             , 'RegionEA'
#             , 'RegionRANZ'
#             , 'RegionSA'
#             , 'MTO'
#             , 'RegionFoundation'
#             , 'FIHub'
#             , 'Rabobank Netherlands'
#             , 'Rabobank London'
#             , 'Rabobank Paris'
#             , 'Rabobank Frankfurt'
#             , 'Rabobank Kenya'
#             , 'Rabobank Dublin'
#             , 'Rabobank Madrid'
#             , 'Rabobank Milan'
#             , 'Rabobank Turkey'
#             , 'Rabobank Antwerp'
#             , 'DepartmentTCF'
#         )
# ''').write.mode('overwrite').saveAsTable('radar.N2K_combined')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create QCResultsPerTestElement instead of using measures in dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC PREVIOUS PERIOD WILL ALWAYS BE BLANK PER DEFINITION, SO SKIP IT!!
# MAGIC
# MAGIC - '#' 2. Total Material (Test Element Level) = 
# MAGIC   CALCULATE(
# MAGIC       COUNTROWS('qualitycontrol'),
# MAGIC     'qualitycontrol'[FinalMarking] = "Not adequate - material"
# MAGIC   )
# MAGIC
# MAGIC - % 1.2 Difference (Test Element Level)(Material (previous period) - Material) = [% 1.1 Material (Test Element Level)(previous period)]-[% 1. Material (Test Element Level)]
# MAGIC
# MAGIC - % 2.2 Difference (Test Element Level)(Non Material(previous period)-Non Material) = [% 2.1 Non Material (Test Element Level) (previous period)]- [% 2. Non Material (Test Element Level)]
# MAGIC
# MAGIC - '#' 3. Total Non Material (Test Element Level)
# MAGIC
# MAGIC - '#' 4. Total Overtuned (Test Element Level) = 
# MAGIC   CALCULATE(
# MAGIC       COUNTROWS('qualitycontrol'),
# MAGIC       qualitycontrol[FinalMarking] ="Overturned"
# MAGIC   )
# MAGIC
# MAGIC - % 3.2 Difference Overturned (Test Element Level)(Overturned previous period)-Overturned = [% 3.1. Overturned (Test Element Level)(previous period)]- [% 3. Overtuned (Test Element Level)]

# COMMAND ----------

# %sql
# /*
# PREVIOUS PERIOD WILL ALWAYS BE BLANK PER DEFINITION, SO SKIP IT!!

# # 2. Total Material (Test Element Level) = 
#   CALCULATE(
#       COUNTROWS('qualitycontrol'),
#     'qualitycontrol'[FinalMarking] = "Not adequate - material"
#   )

# % 1.2 Difference (Test Element Level)(Material (previous period) - Material) = [% 1.1 Material (Test Element Level)(previous period)]-[% 1. Material (Test Element Level)]

# % 2.2 Difference (Test Element Level)(Non Material(previous period)-Non Material) = [% 2.1 Non Material (Test Element Level) (previous period)]- [% 2. Non Material (Test Element Level)]

# # 3. Total Non Material (Test Element Level)

# # 4. Total Overtuned (Test Element Level) = 
#   CALCULATE(
#       COUNTROWS('qualitycontrol'),
#       qualitycontrol[FinalMarking] ="Overturned"
#   )

# % 3.2 Difference Overturned (Test Element Level)(Overturned previous period)-Overturned = [% 3.1. Overturned (Test Element Level)(previous period)]- [% 3. Overtuned (Test Element Level)]

# */
# CREATE OR REPLACE TEMPORARY VIEW QCResultsPerTestElement AS

# WITH TotalMaterial_CTE AS (
#   SELECT
#     SourceClient
#     , TestElementDescription
#     , COUNT(*) AS TotalMaterial_TestElementLevel
#   FROM radar.qualitycontrol
#   WHERE FinalMarking = 'Not adequate - material'
#   GROUP BY 1,2
# )
# , MaterialDifference_CTE AS (
#     SELECT
#       SourceClient
#       , TestElementDescription
#       , ROUND(-1 * SUM(CASE WHEN FinalMarking = 'Not adequate - material' THEN 1 ELSE 0 END) / COUNT(*), 2) AS MaterialDifference_TestElementLevel
#     FROM radar.qualitycontrol
#     GROUP BY 1,2
#   )
# , TotalNonMaterial_CTE AS (
#     SELECT
#       SourceClient
#       , TestElementDescription
#       , COUNT(*) AS TotalNonMaterial_TestElementLevel
#     FROM radar.qualitycontrol
#     WHERE FinalMarking = 'Not adequate - non material'
#     GROUP BY 1,2
#     ORDER BY TestElementDescription ASC
#   )
# , NonMaterialDifference_CTE AS (
#     SELECT
#       SourceClient
#       , TestElementDescription
#       , ROUND(-1 * SUM(CASE WHEN FinalMarking = 'Not adequate - non material' THEN 1 ELSE 0 END) / COUNT(*), 2) AS NonMaterialDifference_TestElementLevel
#     FROM radar.qualitycontrol
#     GROUP BY 1,2
#     ORDER BY TestElementDescription ASC
#   )
# , TotalOverturned_CTE AS (
#     SELECT
#       SourceClient
#       , TestElementDescription
#       , COUNT(*) AS TotalOverturned_TestElementLevel
#     FROM radar.qualitycontrol
#     WHERE FinalMarking = 'Overturned'
#     GROUP BY 1,2
#     ORDER BY TestElementDescription ASC
#   ) 
# , OverturnedDifference_CTE AS (
#     SELECT
#       SourceClient
#       , TestElementDescription
#       , ROUND(-1 * SUM(CASE WHEN FinalMarking = 'Overturned' THEN 1 ELSE 0 END) / COUNT(*), 2) AS OverturnedDifference_TestElementLevel
#     FROM radar.qualitycontrol
#     GROUP BY 1,2
#     ORDER BY TestElementDescription ASC
#   )

# SELECT
#   t1.SourceClient
#   , t1.TestElementDescription
#   , t1.MaterialDifference_TestElementLevel
#   , t2.TotalMaterial_TestElementLevel
#   , t3.TotalNonMaterial_TestElementLevel
#   , t4.NonMaterialDifference_TestElementLevel
#   , t5.TotalOverturned_TestElementLevel
#   , t6.OverturnedDifference_TestElementLevel

# FROM MaterialDifference_CTE t1
# LEFT JOIN TotalMaterial_CTE t2 ON t1.SourceClient = t2.SourceClient
# LEFT JOIN TotalNonMaterial_CTE t3 ON t1.SourceClient = t3.SourceClient
# LEFT JOIN NonMaterialDifference_CTE t4 ON t1.SourceClient = t4.SourceClient
# LEFT JOIN TotalOverturned_CTE t5 ON t1.SourceClient = t5.SourceClient
# LEFT JOIN OverturnedDifference_CTE t6 ON t1.SourceClient = t6.SourceClient

# -- ORDER BY t1.TestElementDescription ASC

# COMMAND ----------

# # QualityControl_TotalMaterial
# spark.sql('DROP TABLE IF EXISTS radar.QualityControl_TotalMaterial')
# spark.sql('''
#         SELECT
#             CONCAT(SourceClient, TestElementDescription) AS SourceClient_TestElementDescription
#             , COUNT(*) AS TotalMaterial_TestElementLevel
#         FROM radar.qualitycontrol
#         WHERE FinalMarking = 'Not adequate - material'
#         GROUP BY 1
# ''').write.mode('overwrite').saveAsTable('radar.QualityControl_TotalMaterial')


# # QualityControl_TotalNonMaterial
# spark.sql('DROP TABLE IF EXISTS radar.QualityControl_TotalNonMaterial')
# spark.sql('''
#         SELECT
#             CONCAT(SourceClient, TestElementDescription) AS SourceClient_TestElementDescription
#             , COUNT(*) AS TotalNonMaterial_TestElementLevel
#         FROM radar.qualitycontrol
#         WHERE FinalMarking = 'Not adequate - non material'
#         GROUP BY 1
# ''').write.mode('overwrite').saveAsTable('radar.QualityControl_TotalNonMaterial')


# # QualityControl_TotalOverturned
# spark.sql('DROP TABLE IF EXISTS radar.QualityControl_TotalOverturned')
# spark.sql('''
#         SELECT
#             CONCAT(SourceClient, TestElementDescription) AS SourceClient_TestElementDescription
#             , COUNT(*) AS TotalOverturned_TestElementLevel
#         FROM radar.qualitycontrol
#         WHERE FinalMarking = 'Overturned'
#         GROUP BY 1
# ''').write.mode('overwrite').saveAsTable('radar.QualityControl_TotalOverturned')

# COMMAND ----------

# TO DO
# QualityControl_MaterialDifference
# QualityControl_NonMaterialDifference
# QualityControl_OverturnedDifference

# COMMAND ----------

# DBTITLE 1,QualityControl_MaterialDifference_previous
# spark.sql('DROP TABLE IF EXISTS radar.QualityControl_MaterialDifference_previous')
# spark.sql('''
#         WITH period_bounds AS (
#         SELECT 
#             MIN(DATE(Signoff)) AS current_min
#             , MAX(DATE(Signoff)) AS current_max
#         FROM radar.cases
#         WHERE DATE(Signoff) > '2025-04-30'
#         )

#         , date_ranges AS (
#             SELECT 
#             DATE_SUB(current_min, DATEDIFF(current_max, current_min)) AS previous_start_date
#             , current_min AS previous_end_date
#             FROM period_bounds
#         )

#         -- previous period material ratio
#         SELECT
#             t1.TestElementDescription
#             , CONCAT(t1.SourceClient, t1.TestElementDescription) AS SourceClient_TestElementDescription
#             , COUNT(CASE WHEN t1.FinalMarking = 'Not adequate - material' THEN 1 END) AS material_prev_count
#             , COUNT(*) AS total_material_prev_count
#         FROM radar.qualitycontrol t1
#         JOIN radar.cases t2 ON t1.SourceClient = t2.SourceClient
#         JOIN date_ranges ON TRUE
#         WHERE DATE(t2.Signoff) >= previous_start_date
#         AND DATE(t2.Signoff) < previous_end_date
#         GROUP BY 1,2

# ''').write.mode('overwrite').saveAsTable('radar.QualityControl_MaterialDifference_previous')

# COMMAND ----------

# DBTITLE 1,QualityControl_MaterialDifference_current
# spark.sql('DROP TABLE IF EXISTS radar.QualityControl_MaterialDifference_current')
# spark.sql('''
#         -- current period material ratio
#         SELECT
#             TestElementDescription
#             , CONCAT(SourceClient, TestElementDescription) AS SourceClient_TestElementDescription
#             , COUNT(CASE WHEN FinalMarking = 'Not adequate - material' THEN 1 END) AS material_count
#             , COUNT(*) AS total_material_count
#         FROM radar.qualitycontrol
#         GROUP BY 1,2
# ''').write.mode('overwrite').saveAsTable('radar.QualityControl_MaterialDifference_current')



# # (CAST(t2.material_prev_count AS DOUBLE) / t2.total_prev_count) - (CAST(t1.material_count AS DOUBLE) / t1.total_count) AS MaterialDifference_TestElementLevel
