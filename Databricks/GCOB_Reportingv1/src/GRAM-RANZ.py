# Databricks notebook source
import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

# Fetching environment variables
app_reg_app_id = os.environ["APP_REG_APP_ID"]
ReadStorage = os.environ["GDP_STORAGE_NAME"]
TenantId = os.environ["TENANT_ID"]

# COMMAND ----------

#Retrieving Client Secret from connected Secrets
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
# Configuring Spark to access GDP Defined Storage account using OAuth authentication
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# List of dataobjects from GDP
load_df = pd.DataFrame({'GDPname':[
'Party_RiskModelInstanceQuestionAnswers'
, 'Party_RiskModelCategories'
, 'Party_RiskModelInstance'
, 'party_WRCDDModel'
]})

# Looping through the list of dataobjects and reading the data from GDP
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=row.GDPname, path_prefix='RISKMODEL')

# COMMAND ----------

# List of datasets from GDP
load_df = [
'party_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

#Defining the storage account details
container_name='adhoc-emailattachments'
storage_account_name='rgwrfecreportingpro8d42'
storage_account_key=dbutils.secrets.get(scope="connectedsecrets", key = "rgwrfecreportingpro8d42")

# COMMAND ----------

# Unmounting the container
dbutils.fs.unmount('/mnt/adhoc-emailattachments')

# COMMAND ----------

# Mounting the container
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = f"/mnt/{container_name}",
  extra_configs = {
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net":storage_account_key
  })

# COMMAND ----------

# spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",dbutils.secrets.get(scope="connectedsecrets", key = "rgwrfecreportingpro8d42"))

# COMMAND ----------

# MAGIC %pip install openpyxl pandas

# COMMAND ----------

#Reading excel files from storage and creating temp views
import pandas as pd
Excel_files_Ranz=["MDM-August","MDM-September","OCDD_Case_Summary - NZ","OCDD_Resolved_Case_Report - AU"]
# Defining  the path to the Excel file
for file in Excel_files_Ranz:
    excel_file_path = f"/dbfs/mnt/adhoc-emailattachments/{file}.xlsx"
    df=pd.read_excel(excel_file_path,engine='openpyxl')
    sdf=spark.createDataFrame(df)
    sdf.createOrReplaceTempView(file.replace("-","").replace(" ",""))  

# COMMAND ----------

#Extracting the data from the zip files and reading the csv files
import zipfile
zipfile3='SIRA_REPORT_OCT_2024.zip'
zip_file_path= f"/dbfs/mnt/adhoc-emailattachments/{zipfile3}"
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    print(zip_ref.namelist())
    with zip_ref.open('SIRA_REPORT_OCT_2024.csv') as file:
      df=pd.read_csv(file)
sdf=spark.createDataFrame(df)
sdf.createOrReplaceTempView("SIRA_REPORT_OCT_2024")

zipfiles=[ 'SIRA_2023_REPORT_NEW','SIRA_LATEST_REPORT_NEW']
for files in zipfiles:
  zip_file_path= f"/dbfs/mnt/adhoc-emailattachments/{files}.zip"
  with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
      print(zip_ref.namelist())
      with zip_ref.open(f'{zip_ref.namelist()[1]}') as file:
        df=pd.read_csv(file)
        sdf=spark.createDataFrame(df)
        sdf.createOrReplaceTempView(files.replace("-","").replace(" ","")) 


# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating MDMRANZ temp view
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW MDMRANZ AS
# MAGIC select
# MAGIC   CONTRACT_ID,
# MAGIC   PARTY_ID,
# MAGIC   CLIENT_STATUS,
# MAGIC   CLIENT_BUSINESS_LINE,
# MAGIC   LAST_REVIEW_DATE,
# MAGIC   DISTRIBUTION_CHANNEL,
# MAGIC   CUSTOMER_TYPE_CD,
# MAGIC   CDD_CLIENT_TYPE,
# MAGIC   CUSTOMER_COUNTRY,
# MAGIC   RISK_RATING,
# MAGIC   NEXT_CDD_REVIEW,
# MAGIC   ONBOARDING_DATE,
# MAGIC   OFFBOARDING_DATE,
# MAGIC   FACE_TO_FACE,
# MAGIC   GLOBAL_CLIENT_ID,
# MAGIC   REL_TYPE_CD,
# MAGIC   REL_ROL_DESC,
# MAGIC   ISUBO,
# MAGIC   ADVERSE_MEDIA,
# MAGIC   PEP_EVALUATION_DESC,
# MAGIC   PEP_TYPE_DESC,
# MAGIC   PEP_NATIONALITY_ISO_CODE,
# MAGIC   PEP_NATIONALITY,
# MAGIC   RESIDENTIAL_ADDRESS_ISOCODE,
# MAGIC   RESIDENTIAL_ADDRESS_COUNTRY
# MAGIC from
# MAGIC   MDMAugust
# MAGIC union
# MAGIC select
# MAGIC   CONTRACT_ID,
# MAGIC   PARTY_ID,
# MAGIC   CLIENT_STATUS,
# MAGIC   CLIENT_BUSINESS_LINE,
# MAGIC   LAST_REVIEW_DATE,
# MAGIC   DISTRIBUTION_CHANNEL,
# MAGIC   CUSTOMER_TYPE_CD,
# MAGIC   CDD_CLIENT_TYPE,
# MAGIC   CUSTOMER_COUNTRY,
# MAGIC   RISK_RATING,
# MAGIC   NEXT_CDD_REVIEW,
# MAGIC   ONBOARDING_DATE,
# MAGIC   OFFBOARDING_DATE,
# MAGIC   FACE_TO_FACE,
# MAGIC   GLOBAL_CLIENT_ID,
# MAGIC   REL_TYPE_CD,
# MAGIC   REL_ROL_DESC,
# MAGIC   ISUBO,
# MAGIC   ADVERSE_MEDIA,
# MAGIC   PEP_EVALUATION_DESC,
# MAGIC   PEP_TYPE_DESC,
# MAGIC   PEP_NATIONALITY_ISO_CODE,
# MAGIC   PEP_NATIONALITY,
# MAGIC   RESIDENTIAL_ADDRESS_ISOCODE,
# MAGIC   RESIDENTIAL_ADDRESS_COUNTRY
# MAGIC from
# MAGIC   MDMSeptember
# MAGIC union
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   SIRA_REPORT_OCT_2024
# MAGIC union
# MAGIC select
# MAGIC   CONTRACT_ID,
# MAGIC   PARTY_ID,
# MAGIC   CLIENT_STATUS,
# MAGIC   CLIENT_BUSINESS_LINE,
# MAGIC   LAST_REVIEW_DATE,
# MAGIC   DISTRIBUTION_CHANNEL,
# MAGIC   CUSTOMER_TYPE_CD,
# MAGIC   CDD_CLIENT_TYPE,
# MAGIC   CUSTOMER_COUNTRY,
# MAGIC   RISK_RATING,
# MAGIC   NEXT_CDD_REVIEW,
# MAGIC   ONBOARDING_DATE,
# MAGIC   OFFBOARDING_DATE,
# MAGIC   FACE_TO_FACE,
# MAGIC   GLOBAL_CLIENT_ID,
# MAGIC   REL_TYPE_CD,
# MAGIC   REL_ROL_DESC,
# MAGIC   ISUBO,
# MAGIC   ADVERSE_MEDIA,
# MAGIC   PEP_EVALUATION_DESC,
# MAGIC   PEP_TYPE_DESC,
# MAGIC   PEP_NATIONALITY_ISO_CODE,
# MAGIC   PEP_NATIONALITY,
# MAGIC   RESIDENTIAL_ADDRESS_ISOCODE,
# MAGIC   RESIDENTIAL_ADDRESS_COUNTRY
# MAGIC from
# MAGIC   SIRA_2023_REPORT_NEW
# MAGIC union
# MAGIC select
# MAGIC   CONTRACT_ID,
# MAGIC   PARTY_ID,
# MAGIC   CLIENT_STATUS,
# MAGIC   CLIENT_BUSINESS_LINE,
# MAGIC   LAST_REVIEW_DATE,
# MAGIC   DISTRIBUTION_CHANNEL,
# MAGIC   CUSTOMER_TYPE_CD,
# MAGIC   CDD_CLIENT_TYPE,
# MAGIC   CUSTOMER_COUNTRY,
# MAGIC   RISK_RATING,
# MAGIC   NEXT_CDD_REVIEW,
# MAGIC   ONBOARDING_DATE,
# MAGIC   OFFBOARDING_DATE,
# MAGIC   FACE_TO_FACE,
# MAGIC   GLOBAL_CLIENT_ID,
# MAGIC   REL_TYPE_CD,
# MAGIC   REL_ROL_DESC,
# MAGIC   ISUBO,
# MAGIC   ADVERSE_MEDIA,
# MAGIC   PEP_EVALUATION_DESC,
# MAGIC   PEP_TYPE_DESC,
# MAGIC   PEP_NATIONALITY_ISO_CODE,
# MAGIC   PEP_NATIONALITY,
# MAGIC   RESIDENTIAL_ADDRESS_ISOCODE,
# MAGIC   RESIDENTIAL_ADDRESS_COUNTRY
# MAGIC from
# MAGIC   SIRA_LATEST_REPORT_NEW

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating OCDD AU and NZ temp view
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW OCDDAUNZ AS
# MAGIC select
# MAGIC   `Case ID`,
# MAGIC   `Client ID`,
# MAGIC   `Client Name`,
# MAGIC   `Review Type`,
# MAGIC   `Current Risk Rating` as `Risk Rating`,
# MAGIC   `Review Due Date`,
# MAGIC   `Country`,
# MAGIC   `LOB`,
# MAGIC   `Account Manager`,
# MAGIC   `Branch`
# MAGIC from
# MAGIC   OCDD_Case_SummaryNZ
# MAGIC union all
# MAGIC select
# MAGIC   `Case ID`,
# MAGIC   `Client ID`,
# MAGIC   `Client Name`,
# MAGIC   `Review Type`,
# MAGIC   `Risk Rating`,
# MAGIC   `Next Review Date` as `Review Due Date`,
# MAGIC   `Country`,
# MAGIC   `LOB`,
# MAGIC   `Account Manager`,
# MAGIC   `Branch`
# MAGIC from
# MAGIC   OCDD_Resolved_Case_ReportAU

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating temp view for generating Client dataobject
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW Client AS
# MAGIC select
# MAGIC   distinct OCDD.`Case ID` as CaseID,
# MAGIC   OCDD.`Client ID` as ClientID,
# MAGIC   OCDD.`Client Name` as ClientName,
# MAGIC   OCDD.`Review Type` as ReviewType,
# MAGIC   OCDD.`Risk Rating` as RiskRating,
# MAGIC   OCDD.`Review Due Date`as ReviewDueDate,
# MAGIC   OCDD.`Country`,
# MAGIC   OCDD.`LOB`,
# MAGIC   OCDD.`Account Manager` as AccountManager,
# MAGIC   OCDD.`Branch`,
# MAGIC   ranz.CONTRACT_ID,
# MAGIC   ranz.PARTY_ID,
# MAGIC   ranz.CLIENT_STATUS,
# MAGIC   ranz.CLIENT_BUSINESS_LINE,
# MAGIC   ranz.LAST_REVIEW_DATE,
# MAGIC   ranz.DISTRIBUTION_CHANNEL,
# MAGIC   ranz.CUSTOMER_TYPE_CD,
# MAGIC   ranz.CDD_CLIENT_TYPE,
# MAGIC   ranz.CUSTOMER_COUNTRY,
# MAGIC   ranz.RISK_RATING,
# MAGIC   ranz.NEXT_CDD_REVIEW,
# MAGIC   ranz.ONBOARDING_DATE,
# MAGIC   ranz.OFFBOARDING_DATE,
# MAGIC   ranz.FACE_TO_FACE,
# MAGIC   ranz.GLOBAL_CLIENT_ID,
# MAGIC   ranz.REL_TYPE_CD,
# MAGIC   ranz.REL_ROL_DESC,
# MAGIC   ranz.ISUBO,
# MAGIC   ranz.ADVERSE_MEDIA,
# MAGIC   ranz.PEP_EVALUATION_DESC,
# MAGIC   ranz.PEP_TYPE_DESC,
# MAGIC   ranz.PEP_NATIONALITY_ISO_CODE,
# MAGIC   ranz.PEP_NATIONALITY,
# MAGIC   ranz.RESIDENTIAL_ADDRESS_ISOCODE,
# MAGIC   ranz.RESIDENTIAL_ADDRESS_COUNTRY,
# MAGIC   gram.SourceSystemReferenceId,
# MAGIC   gram.SourceSystemName,
# MAGIC   gram.InstanceId
# MAGIC from
# MAGIC   MDMRANZ ranz
# MAGIC   left join OCDDAUNZ OCDD on ranz.contract_Id = OCDD.`Client Id`
# MAGIC   inner join Party_RiskModelInstanceQuestionAnswers gram on OCDD.`Case ID` = gram.SourceSystemReferenceId
# MAGIC   and gram.SourceSystemName = 'RANZ - PegaCdd'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS radar.RanzMDMGramClient

# COMMAND ----------

#Loading the temp view to catalog
spark.sql('select * from Client').write.mode('overwrite').saveAsTable('radar.RanzMDMGramClient')

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating temp view for generating CDDQuestionsAnswers dataobject
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW CDDQuestionsAnswers AS
# MAGIC select
# MAGIC   distinct OCDD.`Case ID` as CaseID,
# MAGIC   OCDD.`Client ID` as ClientID,
# MAGIC   OCDD.`Client Name` as ClientName,
# MAGIC   ranz.CONTRACT_ID,
# MAGIC   ranz.PARTY_ID,
# MAGIC   gram.InstanceId,
# MAGIC   gram.QuestionId,
# MAGIC   gram.QuestionText,
# MAGIC   gram.QuestionCode,
# MAGIC   gram.AnswerValue,
# MAGIC   gram.AnswerText
# MAGIC from
# MAGIC   MDMRANZ ranz
# MAGIC   left join OCDDAUNZ OCDD on ranz.contract_Id = OCDD.`Client Id`
# MAGIC   inner join Party_RiskModelInstanceQuestionAnswers gram on OCDD.`Case ID` = gram.SourceSystemReferenceId
# MAGIC   and gram.SourceSystemName = 'RANZ - PegaCdd'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS radar.RanzMDMGramCDDQuestionsAnswers

# COMMAND ----------

spark.sql('select * from CDDQuestionsAnswers').write.mode('overwrite').saveAsTable('radar.RanzMDMGramCDDQuestionsAnswers')

# COMMAND ----------

#Creating Static view for Risk Level
Risk_list = [0,1,2,3,4,5,6,7]
Risk_Description = ['Undefined', 'No Risk', 'Low Risk', 'Medium Risk', 'High Risk','Super High Risk','Extra High Risk','Unacceptable Risk']
#Creating spark dataframe from lists
df_gcob_static_RiskLevel = spark.createDataFrame(zip(Risk_list, Risk_Description), ['Id', 'Description'])
df_gcob_static_RiskLevel.createOrReplaceTempView('static_RiskLevel')

# COMMAND ----------

# MAGIC %sql
# MAGIC --Finding Risk Level Category wise
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW RiskModelCategories AS with InstanceCategory as (
# MAGIC   select
# MAGIC     InstanceId,
# MAGIC     Geographical AS GeoRisk,
# MAGIC     `Entity Type` AS EntityTypeRisk,
# MAGIC     Structure AS StructureRisk,
# MAGIC     Sector AS SectorRisk,
# MAGIC     `Products and Services` AS ProductRisk,
# MAGIC     PEP AS PEPRisk,
# MAGIC     `Transaction` AS TransactionRisk,
# MAGIC     `Distribution Channel` AS DistributionRisk,
# MAGIC     `Third Party` AS ThirdPartyRisk,
# MAGIC     `Adverse Info` AS AdverseInfoRisk,
# MAGIC     Other AS OtherRisk
# MAGIC   FROM
# MAGIC     (
# MAGIC       select
# MAGIC         InstanceId,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Adverse Info' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as `Adverse Info`,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Distribution Channel' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as `Distribution Channel`,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Entity Type' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as `Entity Type`,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'General' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as General,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Geographical' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as Geographical,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Other' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as Other,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'PEP' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as PEP,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Products and Services' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as `Products and Services`,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Sector' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as Sector,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Structure' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as Structure,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Third Party' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as `Third Party`,
# MAGIC         MAX(
# MAGIC           CASE
# MAGIC             WHEN CategoryName = 'Transaction' then CalculatedRiskLevelId
# MAGIC             else NULL
# MAGIC           END
# MAGIC         ) as `Transaction`
# MAGIC       FROM
# MAGIC         Party_RiskModelCategories
# MAGIC       GROUP BY
# MAGIC         InstanceId
# MAGIC     )
# MAGIC )
# MAGIC select
# MAGIC   distinct InstanceId,
# MAGIC   geo.Description as GeographicalRiskLevel,
# MAGIC   Ent.Description as EntityTypeRiskLevel,
# MAGIC   stru.Description as StructureRiskLevel,
# MAGIC   sec.Description as SectorRiskLevel,
# MAGIC   prod.Description as ProductAndServiceRiskLevel,
# MAGIC   pep.Description as PEPRiskLevel,
# MAGIC   tran.Description as TransactionRiskLevel,
# MAGIC   dist.Description as DistributionRiskLevel,
# MAGIC   thir.Description as ThirdPartyRiskLevel,
# MAGIC   adv.Description as AdverseInfoRiskLevel
# MAGIC FROM
# MAGIC   InstanceCategory AS rf
# MAGIC   LEFT OUTER JOIN static_RiskLevel geo ON rf.GeoRisk = geo.Id
# MAGIC   LEFT OUTER JOIN static_RiskLevel Ent ON rf.EntityTypeRisk = Ent.Id
# MAGIC   LEFT OUTER JOIN static_RiskLevel stru ON rf.StructureRisk = stru.Id
# MAGIC   LEFT OUTER JOIN static_RiskLevel sec ON rf.SectorRisk = sec.Id
# MAGIC   LEFT OUTER JOIN static_RiskLevel prod ON rf.ProductRisk = prod.Id
# MAGIC   LEFT OUTER JOIN static_RiskLevel pep ON rf.PEPRisk = pep.Id
# MAGIC   LEFT OUTER JOIN static_RiskLevel tran ON rf.TransactionRisk = tran.Id
# MAGIC   LEFT OUTER JOIN static_RiskLevel dist ON rf.DistributionRisk = dist.Id
# MAGIC   LEFT OUTER JOIN static_RiskLevel thir ON rf.ThirdPartyRisk = thir.Id
# MAGIC   LEFT OUTER JOIN static_RiskLevel adv ON rf.AdverseInfoRisk = adv.Id

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating temp view for generating Risk dataobject
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW Risk AS
# MAGIC select
# MAGIC   distinct Client.CaseID,
# MAGIC   Client.ClientID,
# MAGIC   Client.PARTY_ID,
# MAGIC   Client.RISK_RATING as ValidatedRiskLevel,
# MAGIC   -- gramCase.ModelCalculatedRiskLevel,
# MAGIC   -- gramCase.ModelReCalculatedRiskLevel,
# MAGIC   riskcategories.*
# MAGIC From
# MAGIC   Client
# MAGIC   left join RiskModelCategories riskcategories on Client.InstanceId = riskcategories.InstanceId

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS radar.RanzMDMGramRisk

# COMMAND ----------

spark.sql('select * from Risk').write.mode('overwrite').saveAsTable('radar.RanzMDMGramRisk')
