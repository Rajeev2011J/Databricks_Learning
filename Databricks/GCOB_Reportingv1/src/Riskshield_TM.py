# Databricks notebook source
# MAGIC %md
# MAGIC ## Goal:
# MAGIC The following Riskshield files need to be created for TM on top of GDP defined layers:
# MAGIC
# MAGIC - Client_static_data_TM    -        This file tells about the static information about clients
# MAGIC - structure_more_than_4_TM -        This file tells if the structure is more than 4 from a  CDD perspective 
# MAGIC - System_Identifiers_TM -           This file informs all the identifiers linked to a CDD file for the same client in GCOB
# MAGIC - NAICS_TM  -                       This file tells about the NAICS / business activities of the clients
# MAGIC - Product_Report_TM -                This file tells about the products assigned to the client
# MAGIC - Adverse_Client_Information_TM  -  This file tells about the adverse information about the client
# MAGIC - PEP_TM                            This file tells about the PEPs present in client structure in GCOB
# MAGIC and save into the salandingzone storage account.
# MAGIC
# MAGIC ## author:
# MAGIC Devi Chennareddy
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |25-March-2026 |15511733 |Riskshield Extracts - Add additional columns to PEP file

# COMMAND ----------

import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Flag based on Env.
if environment == 'dev':
  FeatureFlag = 'True'
elif environment == 'preprd':
  FeatureFlag = 'True'
elif environment == 'prod':
  FeatureFlag = 'False'

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')

if datetime.today().weekday() >= 5:
  dbutils.notebook.exit("Execution stopped: Today is not a weekday.")
else:
  load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
  BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

# COMMAND ----------

SALZReadStorage = os.environ['SALZReadStorage']
if SALZReadStorage.lower()!='salandingzonefecradarprd':
    print("Execution stopped Storage account does not exist")
    dbutils.notebook.exit("Terminated early: Non-prod storage account")

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details'
,'party_client_identifiers'
# ,'party_tax_info_fatca_crs'
# ,'party_documents'
,'party_products_and_services'
,'party_AllPartyDetails'
# ,'party_client_structure'
,'party_business_activities'
,'party_control_measures'
# ,'party_RelatedPartyParentAddresses'
# ,'party_local_client_Owners'
,'party_client_structure_GUI'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Party_RiskModelInstanceQuestionAnswers'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transformations

# COMMAND ----------

df_RiskModelInstanceQuestionAnswers=spark.table('Party_RiskModelInstanceQuestionAnswers')

# COMMAND ----------

df_RiskModelInstanceQuestionAnswers_pivotValues = [row[0] for row in df_RiskModelInstanceQuestionAnswers.select('QuestionText').distinct().collect()]

# COMMAND ----------

# df_RiskAnswersPivot = df_RiskModelInstanceQuestionAnswers.groupby('SourceClient', 'InstanceId').pivot('QuestionText').agg(array_join(collect_set('AnswerValue'), ','))

# COMMAND ----------

df_RiskAnswersPivot = df_RiskModelInstanceQuestionAnswers.repartition(100, 'SourceClient', 'InstanceId').groupby('SourceClient', 'InstanceId').pivot('QuestionText', df_RiskModelInstanceQuestionAnswers_pivotValues).agg(array_join(collect_set('AnswerValue'), ','))

# COMMAND ----------

#adding addtional column StructureMoreThanFourLayers with existed column values to dataframe 
df_RiskAnswersPivot = df_RiskAnswersPivot.withColumn("StructureMoreThanFourLayers", col('a) More than four layers in the structure up to the UBO (the customer and UBO each count as one layer)'))

# COMMAND ----------

df_RiskAnswersPivot.createOrReplaceTempView("InstanceQuestion")

# COMMAND ----------

# df_RiskAnswersAnswerPivot = df_RiskModelInstanceQuestionAnswers.groupby('SourceClient', 'InstanceId').pivot('QuestionText').agg(array_join(collect_set('AnswerText'), ','))

# COMMAND ----------

df_RiskAnswersAnswerPivot = df_RiskModelInstanceQuestionAnswers.repartition(100, 'SourceClient', 'InstanceId').groupby('SourceClient', 'InstanceId').pivot('QuestionText', df_RiskModelInstanceQuestionAnswers_pivotValues).agg(array_join(collect_set('AnswerText'), ','))

# COMMAND ----------

#adding addtional column AdverseInformationType to dataframe 
df_RiskAnswersAnswerPivot = df_RiskAnswersAnswerPivot.withColumn("AdverseInformationType", col('Has any relevant adverse information about the Client, Directors, Authorized Representatives, Ultimate Beneficial Owners, Related parties or Third parties been identified?'))


# COMMAND ----------

df_RiskAnswersAnswerPivot.createOrReplaceTempView("InstanceQuestionText")

# COMMAND ----------

df_party_case_client_details=spark.table('party_case_client_details')

# COMMAND ----------

df_party_case_client_details=df_party_case_client_details.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

df_party_case_client_details.createOrReplaceTempView("temp_party_case_client_details")

# COMMAND ----------

# MAGIC %md
# MAGIC The following code is designed to create Client_static_data object

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Client_static_data AS
# MAGIC select DISTINCT
# MAGIC BusinessDate as `Business Date`,
# MAGIC CaseId,
# MAGIC GcobId,
# MAGIC Replace(FullLegalName,'"','') as `Full Legal Name`,
# MAGIC NextReviewDate as `Next Review Date`,
# MAGIC ValidatedRiskLevel as `Validated Risk Level`,
# MAGIC IsLatestApprovedVersionOfClient as `Is Latest Approved Version Of Client`,
# MAGIC ReviewTypeName as `Review Type Name`,
# MAGIC CaseStatusName as `Case Status Name`,
# MAGIC GlobalClientOwnerLocation as `Global Client Owner Location`,
# MAGIC AdverseInfoRiskLevel as `Adverse Information Risk Level`,
# MAGIC DistributionRiskLevel as `Distribution Channel Risk Level`,
# MAGIC EntityTypeRiskLevel as `Entity Type Risk Level`,
# MAGIC GeographicalRiskLevel as `Geographical Risk Level`,
# MAGIC PEPRiskLevel as `PEP Risk Level`,
# MAGIC ProductAndServiceRiskLevel as `Product And Service Risk Level`,
# MAGIC SectorRiskLevel as `Sector Risk Level` ,
# MAGIC StructureRiskLevel as `Structure Risk Level`,
# MAGIC ThirdPartyRiskLevel as `Third Party Risk Level`,
# MAGIC TransactionRiskLevel as `Transaction Risk Level`,
# MAGIC FinalDecisionDate as `Last Sign Off Date`,
# MAGIC BusinessLineName as `Business Line`
# MAGIC
# MAGIC from temp_party_case_client_details

# COMMAND ----------

df_Client_static = spark.table('Client_static_data')

# COMMAND ----------

# Removing the yesterday files in live folder
dbutils.fs.rm(f'abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/live/', recurse=True)

# COMMAND ----------

temp_dir = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/temp/Client_static_data_{date_parameter}"
final_path_live = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/live/Client_static_data_{date_parameter}.csv"
final_path_history = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/history/Client_static_data_{date_parameter}.csv"

# Write DataFrame to temporary directory
df_Client_static.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(temp_dir)

files = dbutils.fs.ls(temp_dir)

# Find the single CSV file
csv_file = [file.path for file in files if file.name.endswith(".csv")][0]
# Move the CSV file to final paths
dbutils.fs.cp(csv_file, final_path_live)
dbutils.fs.cp(csv_file, final_path_history)

# Remove the temporary directory
dbutils.fs.rm(temp_dir, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC The following code is designed to create structur_more_than_4 object

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW structure_more_than_4 AS
# MAGIC select distinct
# MAGIC c.BusinessDate as PERIOD,
# MAGIC c.CaseId,
# MAGIC c.GcobId,
# MAGIC Replace(c.FullLegalName,'"','') as `Full Legal Name`,
# MAGIC --c.ClientId,
# MAGIC --s.SourceClient,
# MAGIC CASE WHEN s.StructureMoreThanFourLayers <> False THEN true ELSE false END as `Structure: More Than Four Layers`,
# MAGIC c.IsLatestApprovedVersionOfClient as `Is Latest Approved Version Of Client`
# MAGIC from temp_party_case_client_details c
# MAGIC left join InstanceQuestion s on c.SourceClient=s.SourceClient where c.IsLatestApprovedVersionOfClient <> False and c.CaseStatusName <> 'Cancelled' --and c.GcobId ='79030'
# MAGIC
# MAGIC

# COMMAND ----------

df_structure_more_than_4 = spark.table('structure_more_than_4')

# COMMAND ----------

temp_dir = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/temp/structure_more_than_4_{date_parameter}"
final_path_live = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/live/structure_more_than_4_{date_parameter}.csv"
final_path_history = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/history/structure_more_than_4_{date_parameter}.csv"

# Write DataFrame to temporary directory
df_structure_more_than_4.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(temp_dir)

files = dbutils.fs.ls(temp_dir)

# Find the single CSV file
csv_file = [file.path for file in files if file.name.endswith(".csv")][0]
# Move the CSV file to final paths
dbutils.fs.cp(csv_file, final_path_live)
dbutils.fs.cp(csv_file, final_path_history)

# Remove the temporary directory
dbutils.fs.rm(temp_dir, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC The following code is designed to create system_identifiers object

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view System_Identifiers as
# MAGIC select distinct 
# MAGIC c.CaseId
# MAGIC  ,c.GcobId
# MAGIC  ,Replace(c.FullLegalName,'"','') as `Full Legal Name`
# MAGIC  ,c.SourceSystem as `Source System`
# MAGIC  ,id.SystemIdType as `System Id Type`
# MAGIC  ,id.value
# MAGIC from party_client_identifiers id
# MAGIC inner join party_case_client_details c on id.ClientId = c.ClientId where c.IsLatestApprovedVersionOfClient is true 

# COMMAND ----------

df_System_Identifiers = spark.table("System_Identifiers")

# COMMAND ----------

temp_dir = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/temp/System_Identifiers_{date_parameter}"
final_path_live = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/live/System_Identifiers_{date_parameter}.csv"
final_path_history = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/history/System_Identifiers_{date_parameter}.csv"

# Write DataFrame to temporary directory
df_System_Identifiers.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(temp_dir)

files = dbutils.fs.ls(temp_dir)

# Find the single CSV file
csv_file = [file.path for file in files if file.name.endswith(".csv")][0]
# Move the CSV file to final paths
dbutils.fs.cp(csv_file, final_path_live)
dbutils.fs.cp(csv_file, final_path_history)

# Remove the temporary directory
dbutils.fs.rm(temp_dir, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC The following code is designed to create NAICS_TM

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view NAICS_TM as
# MAGIC select 
# MAGIC distinct 
# MAGIC c.GlobalClientOwnerLocation as AssessmentUnitName
# MAGIC ,c.CaseId
# MAGIC --,n.ClientId
# MAGIC ,c.GcobId as LocalClientId
# MAGIC --,c.SourceClient
# MAGIC ,n.NaicsCode as IndustryCode
# MAGIC ,n.NaicsName as `NAICS Name`
# MAGIC ,n.SectorGroup as `NAICS SectorGroup`
# MAGIC ,n.IndustrySector as `NAICS IndustrySector`
# MAGIC ,n.IndustryGroup as `NAICS IndustryGroup`
# MAGIC  from party_case_client_details c
# MAGIC  left join party_business_activities n on c.ClientId=n.ClientId 
# MAGIC  left join party_control_measures AS cm on cm.SourceClient = c.SourceClient
# MAGIC  where c.IsLatestApprovedVersionOfClient is true and c.CaseStatusName <> 'Cancelled' and cm.ExpiredDate IS NULL

# COMMAND ----------

df_NAICS_TM=spark.table("NAICS_TM")

# COMMAND ----------

temp_dir = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/temp/NAICS_TM_{date_parameter}"
final_path_live = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/live/NAICS_TM_{date_parameter}.csv"
final_path_history = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/history/NAICS_TM_{date_parameter}.csv"

# Write DataFrame to temporary directory
df_NAICS_TM.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(temp_dir)

files = dbutils.fs.ls(temp_dir)

# Find the single CSV file
csv_file = [file.path for file in files if file.name.endswith(".csv")][0]
# Move the CSV file to final paths
dbutils.fs.cp(csv_file, final_path_live)
dbutils.fs.cp(csv_file, final_path_history)

# Remove the temporary directory
dbutils.fs.rm(temp_dir, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC The following code is designed to create product and report TM

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view Product_Report_TM as
# MAGIC select distinct 
# MAGIC a.GcobId
# MAGIC ,ProductDomain
# MAGIC ,ProductName as `product OR Service`
# MAGIC ,BusinessUnit
# MAGIC ,ProductOfferingLocation as `Product Location`
# MAGIC ,BookingEntityLocation as `Business Entity` 
# MAGIC from party_products_and_services a
# MAGIC left join party_control_measures AS cm on cm.SourceClient = a.SourceClient
# MAGIC where a.IsLatestApprovedVersionOfClient is true and a.CaseStatusName <> 'Cancelled' and cm.ExpiredDate IS NULL

# COMMAND ----------

df_Product_Report_TM= spark.table("Product_Report_TM")

# COMMAND ----------

temp_dir = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/temp/Product_Report_TM_{date_parameter}"
final_path_live = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/live/Product_Report_TM_{date_parameter}.csv"
final_path_history = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/history/Product_Report_TM_{date_parameter}.csv"

# Write DataFrame to temporary directory
df_Product_Report_TM.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(temp_dir)

files = dbutils.fs.ls(temp_dir)

# Find the single CSV file
csv_file = [file.path for file in files if file.name.endswith(".csv")][0]
# Move the CSV file to final paths
dbutils.fs.cp(csv_file, final_path_live)
dbutils.fs.cp(csv_file, final_path_history)

# Remove the temporary directory
dbutils.fs.rm(temp_dir, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC The following code is designed to create PEP

# COMMAND ----------

# %sql
# --Taking the latest snapshot for each client, prioritizing active clients and the most recent expired ones
#  CREATE OR REPLACE TEMPORARY VIEW all_party  AS
#  select * 
#  from (
#     select a.*,  row_number() over (
#               partition by gcobid,clienttype,caseid 
#                         order by 
#               case when expired is null then 1 else 2 end, 
#               expired desc) as rn
#     from party_AllPartyDetails a 
#       where status='Snapshot')
#        where rn=1
   

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW PEP_TM  AS
# MAGIC SELECT DISTINCT
# MAGIC cs.ClientGcobId AS GcobId,
# MAGIC cs.ClientCaseId AS CaseId,
# MAGIC Replace(cs.ClientFullLegalName,'"','') as `Full Legal Name`,
# MAGIC cs.ParentIdentityName AS `Related Natural Person Party`,
# MAGIC papd.Nationality AS `Countries Of Nationality`,
# MAGIC papd.Citizenship AS `Countries Of Citizenship`,
# MAGIC papd.HasSourceOfWealth as `Has Source Of Wealth`,
# MAGIC papd.PEPStatus AS `Related Person PEP Status`,
# MAGIC cs.ClientCountryOfOperation AS `Country Of Operations`,
# MAGIC cs.ClientCountryOfRegistration AS `Country Of Registration`,
# MAGIC pccd.CddType as `Cdd Type`,
# MAGIC --CASE WHEN cs.ParentType IN ('RelatedNaturalPerson')  THEN true ELSE false END AS `Is Directly Related To Client`,
# MAGIC CASE WHEN cs.ClientID=cs.ChildEntityId  THEN true ELSE false END AS `Is Directly Related To Client`,
# MAGIC pccd.GlobalClientOwner as `Global Client Owner`,
# MAGIC pccd.GlobalClientOwnerLocation as `Global Client Owner Location`,
# MAGIC cs.IsUBO
# MAGIC -- papd.Status
# MAGIC FROM party_client_structure_GUI AS cs
# MAGIC left outer JOIN party_case_client_details pccd ON cs.SourceClient = pccd.SourceClient
# MAGIC left outer JOIN party_AllPartyDetails papd ON cs.UniqueParentPartyId = papd.UniquePartyId AND papd.ClientType IN ('RelatedNaturalPerson') 
# MAGIC WHERE cs.ParentType IN ('RelatedNaturalPerson') AND papd.PEPStatus <> 'Not a PEP' AND cs.CaseStatusName <> 'Cancelled' AND cs.IsLatestApprovedVersionOfClient=true and papd.Status='Live'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS radar.PEP_TM

# COMMAND ----------

# Create the table with the new data
# spark.sql('select * from PEP_TM').write.format("delta").option("delta.columnMapping.mode", "name").mode('overwrite').saveAsTable('radar.PEP_TM')


# COMMAND ----------

df_PEP_TM = spark.table("PEP_TM")

# COMMAND ----------

temp_dir = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/temp/PEP_TM_{date_parameter}"
final_path_live = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/live/PEP_TM_{date_parameter}.csv"
final_path_history = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/history/PEP_TM_{date_parameter}.csv"

# Write DataFrame to temporary directory
df_PEP_TM.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(temp_dir)

files = dbutils.fs.ls(temp_dir)

# Find the single CSV file
csv_file = [file.path for file in files if file.name.endswith(".csv")][0]
# Move the CSV file to final paths
dbutils.fs.cp(csv_file, final_path_live)
dbutils.fs.cp(csv_file, final_path_history)

# Remove the temporary directory
dbutils.fs.rm(temp_dir, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC The following code is designed to create the Adverse_Information

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view Adverse_Client_Information as
# MAGIC select 
# MAGIC distinct 
# MAGIC c.GcobId 
# MAGIC ,c.GlobalClientOwnerLocation as `Assessment Unit`
# MAGIC ,Replace(c.FullLegalName,'"','') as `Full Legal Name`
# MAGIC ,it.AdverseInformationType as `Adverse Information: Adverse Information Type`
# MAGIC ,c.IsLatestApprovedVersionOfClient as `Is Latest Approved Version Of Client`
# MAGIC
# MAGIC  from party_case_client_details c
# MAGIC  left join InstanceQuestionText it on  c.SourceClient=it.SourceClient
# MAGIC  left join party_control_measures AS cm on cm.SourceClient = c.SourceClient
# MAGIC  where c.IsLatestApprovedVersionOfClient is true and c.CaseStatusName <> 'Cancelled' and cm.ExpiredDate IS NULL

# COMMAND ----------

df_Adverse_Client_Information = spark.table("Adverse_Client_Information")

# COMMAND ----------

temp_dir = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/temp/Adverse_Client_Information_{date_parameter}"
final_path_live = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/live/Adverse_Client_Information_{date_parameter}.csv"
final_path_history = f"abfss://riskshield-tm@{SALZReadStorage}.dfs.core.windows.net/history/Adverse_Client_Information_{date_parameter}.csv"

# Write DataFrame to temporary directory
df_Adverse_Client_Information.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(temp_dir)

files = dbutils.fs.ls(temp_dir)

# Find the single CSV file
csv_file = [file.path for file in files if file.name.endswith(".csv")][0]
# Move the CSV file to final paths
dbutils.fs.cp(csv_file, final_path_live)
dbutils.fs.cp(csv_file, final_path_history)

# Remove the temporary directory
dbutils.fs.rm(temp_dir, recurse=True)
