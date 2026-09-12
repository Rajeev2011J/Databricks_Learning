# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC As a reporting analyst part of AMLR implementation project , I would need the below information 
# MAGIC
# MAGIC * The total number of NP/NPPC clients within the WR portfolio per region ? 
# MAGIC * Of these NP/NPPC clients, how many have a high-risk rating (please provide data per region)? 
# MAGIC * Is the net worth of NP/NPPC clients captured somewhere in the system? 
# MAGIC * Is the value of the clients assets more than 1.5 million Euro (ROS) or 3 million Euro (Country Banking)?
# MAGIC
# MAGIC AUTHOR- sowmyashree.parashivamurthy.sudha@rabobank.com

# COMMAND ----------

# DBTITLE 1,Importing Libraries
import os
import re
from datetime import datetime, timedelta

#Importing pyspark libraries and modules
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark import StorageLevel

#Local File Import
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Date Variables
#Derive the date for which data has to be processes
date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

# DBTITLE 1,Outh2 Configuration
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
subscriptionId=os.environ['subscriptionId']
ResourceGroupName=os.environ['resourceGroup_API']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
ServiceKey='app-reg-databricks-wr-radar-preprd'

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# DBTITLE 1,Source Data Ingestion
# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details'
, 'party_AllPartyDetails'
, 'party_client_structure_GUI' 
, 'party_client_identifiers'
, 'party_products_and_sevices'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# List of dataobjects from GDP
load_df = [
'Party_RiskModelInstanceQuestionAnswers'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')
    
# Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject='client_KeyStoreKey')

# COMMAND ----------

# DBTITLE 1,Required Dataframes
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW structure AS
# MAGIC SELECT Sourceclient
# MAGIC   , ClientID
# MAGIC   , ClientGcobid
# MAGIC   , ClientFullLegalName AS FullLegalName
# MAGIC   , ChildIdentity
# MAGIC   , ChildEntityId
# MAGIC   , UniqueChildPartyId
# MAGIC   , ChildIdentityName
# MAGIC   , ChildType
# MAGIC   , ParentIdentity
# MAGIC   , ParentEntityId
# MAGIC   , UniqueParentPartyId
# MAGIC   , ParentIdentityName
# MAGIC   , ParentType
# MAGIC FROM party_client_structure_GUI
# MAGIC WHERE IsLatestApprovedVersionOfClient='True'
# MAGIC --sourcesystem='GCOB' AND 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_case_client_details limit 5

# COMMAND ----------

# DBTITLE 1,Child Address
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW address_child_view AS 
# MAGIC Select
# MAGIC cs.UniqueChildPartyId,
# MAGIC papd.GcobId,
# MAGIC cs.ChildType,
# MAGIC regexp_replace(trim(papd.RegisteredCity), '[\n\r\t]', ' ') AS RegisteredCity,
# MAGIC regexp_replace(trim(papd.OperatingCity), '[\n\r\t]', ' ') AS OperatingCity
# MAGIC FROM structure AS cs 
# MAGIC LEFT OUTER JOIN party_AllPartyDetails AS papd 
# MAGIC ON cs.UniqueChildPartyId = papd.UniquePartyId and papd.status='Live'

# COMMAND ----------

# DBTITLE 1,Parent Address
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW address_parent_view AS 
# MAGIC Select
# MAGIC cs.UniqueParentPartyId,
# MAGIC papd.GcobId,
# MAGIC cs.ParentType,
# MAGIC regexp_replace(trim(papd.RegisteredCity), '[\n\r\t]', ' ') AS RegisteredCity,
# MAGIC regexp_replace(trim(papd.OperatingCity), '[\n\r\t]', ' ') AS OperatingCity
# MAGIC FROM structure as cs 
# MAGIC LEFT OUTER JOIN party_AllPartyDetails AS papd 
# MAGIC ON cs.UniqueParentPartyId = papd.UniquePartyId and papd.status='Live'

# COMMAND ----------

# DBTITLE 1,Party Child
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Parties_child AS
# MAGIC select DISTINCT
# MAGIC cs.ClientGcobid,
# MAGIC cs.FullLegalName AS ClientFullLegalName,
# MAGIC CASE WHEN cs.ChildType='LegalEntityClient' then 'Legal Entity'
# MAGIC      WHEN cs.ChildType='RelatedLegalEntity' then 'Related Legal Entity'
# MAGIC      WHEN cs.ChildType='NaturalPersonClient' then 'Natural Person'
# MAGIC      WHEN cs.ChildType='RelatedNaturalPerson' then 'Related Natural Person' end AS PartyType,
# MAGIC int(cs.ChildIdentity) AS GCOBID,
# MAGIC -- COALESCE(cs.DateOfBirthofChild, TO_CHAR(TO_DATE(TRIM(papd.IncorporationDate), 'dd-MM-yyyy'), 'dd-MM-yyyy')) AS `DOB/IncorporationDate`,
# MAGIC cs.ChildIdentityName AS FullLegalName,
# MAGIC acd.RegisteredCity,
# MAGIC acd.OperatingCity,
# MAGIC cs.SourceClient
# MAGIC FROM  structure cs
# MAGIC LEFT OUTER JOIN address_child_view AS acd 
# MAGIC      ON acd.UniqueChildPartyId = cs.UniqueChildPartyId

# COMMAND ----------

# DBTITLE 1,Party Parent
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Parties_parent AS
# MAGIC select distinct
# MAGIC cs.ClientGcobid,
# MAGIC cs.FullLegalName AS ClientFullLegalName,
# MAGIC CASE WHEN cs.ParentType='LegalEntityClient' then 'Legal Entity'
# MAGIC      WHEN cs.ParentType='RelatedLegalEntity' then 'Related Legal Entity'
# MAGIC      WHEN cs.ParentType='NaturalPersonClient' then 'Natural Person'
# MAGIC      WHEN cs.ParentType='RelatedNaturalPerson' then 'Related Natural Person' end AS PartyType,
# MAGIC int(cs.ParentIdentity) AS GCOBID,
# MAGIC cs.ParentIdentityName AS FullLegalName,
# MAGIC acd.RegisteredCity,
# MAGIC acd.OperatingCity,
# MAGIC cs.SourceClient
# MAGIC FROM  structure cs
# MAGIC LEFT OUTER JOIN address_parent_view AS acd 
# MAGIC      ON acd.UniqueParentPartyId=cs.UniqueParentPartyId

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary view for Parties with the combined query
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Parties AS
# MAGIC   SELECT DISTINCT * 
# MAGIC   FROM Parties_parent 
# MAGIC   WHERE ClientGcobid <> GCOBID
# MAGIC   UNION
# MAGIC   SELECT DISTINCT * 
# MAGIC   FROM Parties_child  
# MAGIC   WHERE ClientGcobid <> GCOBID
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view parties_address as 
# MAGIC select SourceClient, ClientGcobid,PartyType as RelatedPartyType, GCOBID as RelatedPartyGcobId, 
# MAGIC RegisteredCity as RelatedPartyRegisteredCity , OperatingCity as RelatedPartyOperatingCity, FullLegalName as RelatedPartyFullLegalName from Parties 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Final_Document AS
# MAGIC SELECT DISTINCT 
# MAGIC 'GCOB' as SourceSystem,
# MAGIC pc.CaseId,
# MAGIC pc.CaseStatusName,
# MAGIC pc.ReviewTypeName,
# MAGIC pc.gcobid as ClientGcobId,
# MAGIC pc.ClientType ,
# MAGIC pc.FullLegalName as ClientFullLegalName, 
# MAGIC pc.ClientLifeCycleName,
# MAGIC pc.ClientNationality,
# MAGIC pc.RegisteredCity as ClientRegisteredCity,
# MAGIC pc.OperatingCity as ClientOperatingCity,
# MAGIC -- d.RelatedPartyGcobId,
# MAGIC -- d.RelatedPartyType,
# MAGIC -- d.RelatedPartyFullLegalName,
# MAGIC -- d.RelatedPartyRegisteredCity,
# MAGIC -- d.RelatedPartyOperatingCity,
# MAGIC pc.ValidatedRiskLevel,
# MAGIC pns.BookingEntityLocation,
# MAGIC pc.GlobalClientOwnerLocation,
# MAGIC pns.ProductOfferingLocation,
# MAGIC qa.questionid,
# MAGIC qa.answertext
# MAGIC FROM  party_case_client_details AS pc
# MAGIC LEFT OUTER JOIN party_products_and_sevices pns on pc.SourceClient = pns.SourceClient
# MAGIC -- LEFT OUTER JOIN parties_address AS d ON d.SourceClient = pc.SourceClient
# MAGIC LEFT OUTER JOIN Party_RiskModelInstanceQuestionAnswers qa on qa.SourceClient=pc.SourceClient and qa.questionid in (253, 705) 
# MAGIC where (pc.ClientType like '%Person%') --or (d.RelatedPartyType like '%Person%' )
# MAGIC and pc.IsLatestApprovedVersionOfClient='True'
# MAGIC and pc.CaseStatusName<>'Cancelled'
# MAGIC and pc.ClientLifeCycleName   <>'FormerClient'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from Final_Document

# COMMAND ----------

# MAGIC %sql
# MAGIC select ClientNationality, count(ClientFullLegalName) from Final_Document group by ClientNationality

# COMMAND ----------

df=spark.table('Final_Document')

# COMMAND ----------

#NORTHAMERICA
WR001 = ['Rabo Securities Canada, Inc. (RSCI)','Rabo Securities USA, Inc. (RSEC)','Rabobank - USA Rabo AgriFinance','Rabobank Canada (RCBR)','Rabobank Canada(Rural)','Rabobank New York']

# CHILE
WR002 = ['Rabobank Chile']

# BRAZIL
WR003 = ['Rabobank Brazil']

# HONGKONG
WR004 = ['Rabobank Hong Kong']

# SINGAPORE
WR005 = ['Rabobank Singapore']

# CHINA
WR006 = ['Rabobank China']

# INDIA
INDIA = ['Rabobank India']

# EUROPEAFRICA
EUROPEAFRICA = ['Rabobank Antwerp','Rabobank Dublin','Rabobank Frankfurt','Rabobank London','Rabobank Argentina','Rabobank Madrid','Rabobank Milan','Rabobank Netherlands','Rabobank Kenya','Rabobank Paris','Rabobank Foundation','Rabobank - Smallholder Agroforestry Finance (SAF)','Rabobank Turkey']

# AUSTRALIANEWZEALAND
WR009 = ['Rabobank Australia','Rabobank New Zealand','Rabobank - RANZ Country Banking and ROS']

# COMMAND ----------


from functools import reduce
from pyspark.sql import functions as F

cols=['BookingEntityLocation','GlobalClientOwnerLocation','ProductOfferingLocation']

groups = {
    "WR001": list(WR001),
    "WR002": list(WR002),
    "WR003": list(WR003),
    "WR004": list(WR004),
    "WR005": list(WR005),
    "WR006": list(WR006),
    "WR009": list(WR009),
    "INDIA": list(INDIA),
    "EUROPEAFRICA": list(EUROPEAFRICA),
}

def build_condition(cols, values):
    # OR across columns: col1 in values OR col2 in values OR ...
    return reduce(lambda a, b: a | b, (F.col(c).isin(values) for c in cols))

results = {}

for name, values in groups.items():
    condition = build_condition(cols, values)
    count_val = df.filter(condition).count()
    results[name] = count_val
    print(f"count_{name} = {count_val}")

for name, values in groups.items():
    condition = build_condition(cols, values)
    count_val = df.filter(col('ValidatedRiskLevel')=="High").filter(condition).count()
    results[name] = count_val
    print(f"highRisk_count_{name} = {count_val}")

for name, values in groups.items():
    condition = build_condition(cols, values)
    count_val = df.filter(col('questionid')=="253").filter(col('answertext')=='Yes').filter(condition).count()
    results[name] = count_val
    print(f"count_>1million_{name} = {count_val}")

for name, values in groups.items():
    condition = build_condition(cols, values)
    count_val = df.filter(col('questionid')=="705").filter(col('answertext')=='Yes').filter(condition).count()
    results[name] = count_val
    print(f"count_>1.5m_to_3m_{name} = {count_val}")

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,WR001
# MAGIC %sql
# MAGIC select count(*) from final_document where 
# MAGIC BookingEntityLocation in ('Rabo Securities Canada, Inc. (RSCI)','Rabo Securities USA, Inc. (RSEC)','Rabobank - USA Rabo AgriFinance','Rabobank Canada (RCBR)','Rabobank Canada(Rural)','Rabobank New York')
# MAGIC or GlobalClientOwnerLocation in  ('Rabo Securities Canada, Inc. (RSCI)','Rabo Securities USA, Inc. (RSEC)','Rabobank - USA Rabo AgriFinance','Rabobank Canada (RCBR)','Rabobank Canada(Rural)','Rabobank New York')
# MAGIC or ProductOfferingLocation in  ('Rabo Securities Canada, Inc. (RSCI)','Rabo Securities USA, Inc. (RSEC)','Rabobank - USA Rabo AgriFinance','Rabobank Canada (RCBR)','Rabobank Canada(Rural)','Rabobank New York')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from final_document where 
# MAGIC ValidatedRiskLevel=='High' and 
# MAGIC (
# MAGIC BookingEntityLocation in ('Rabo Securities Canada, Inc. (RSCI)','Rabo Securities USA, Inc. (RSEC)','Rabobank - USA Rabo AgriFinance','Rabobank Canada (RCBR)','Rabobank Canada(Rural)','Rabobank New York')
# MAGIC or GlobalClientOwnerLocation in  ('Rabo Securities Canada, Inc. (RSCI)','Rabo Securities USA, Inc. (RSEC)','Rabobank - USA Rabo AgriFinance','Rabobank Canada (RCBR)','Rabobank Canada(Rural)','Rabobank New York')
# MAGIC or ProductOfferingLocation in  ('Rabo Securities Canada, Inc. (RSCI)','Rabo Securities USA, Inc. (RSEC)','Rabobank - USA Rabo AgriFinance','Rabobank Canada (RCBR)','Rabobank Canada(Rural)','Rabobank New York'))
