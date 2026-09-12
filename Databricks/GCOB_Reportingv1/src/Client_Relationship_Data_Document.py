# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Document Structure
# MAGIC
# MAGIC ## Client Information
# MAGIC - SourceClient
# MAGIC - GCID
# MAGIC - GCOBID
# MAGIC - PartyType
# MAGIC - UniqueGCOBID
# MAGIC - FullLegalName
# MAGIC - ClientOwner
# MAGIC - ClientLifeCycleName
# MAGIC - DOB/IncorporationDate
# MAGIC - Nationality []
# MAGIC - Citizenship []
# MAGIC - Addresses [{}]
# MAGIC     - AddressType
# MAGIC     - Street
# MAGIC     - Number
# MAGIC     - PostalCode
# MAGIC     - City
# MAGIC     - Region
# MAGIC     - CountryName
# MAGIC     - CountryISO
# MAGIC - Identification
# MAGIC - IdentificationDocuments [{}]
# MAGIC     - DocumentId
# MAGIC     - DocumentTypeName
# MAGIC     - DocumentFileName
# MAGIC     - Documentstoreidentifier
# MAGIC - KYC_NAICS [{}]
# MAGIC     - NaicsCode
# MAGIC     - NaicsName
# MAGIC
# MAGIC ## Related Party Information
# MAGIC - Relationships [{}]
# MAGIC     - ChildIdentity
# MAGIC     - ParentIdentity
# MAGIC     - Relation [{}]
# MAGIC         - RelationType
# MAGIC         - ShareholdingPercentage
# MAGIC         - VotingRightsPercentage
# MAGIC - CommentsOnStructure
# MAGIC - Parties
# MAGIC     - GCID
# MAGIC     - GCOBID
# MAGIC     - PartyType
# MAGIC     - UniqueGCOBID
# MAGIC     - FullLegalName
# MAGIC     - DOB/IncorporationDate
# MAGIC     - Nationality []
# MAGIC     - Citizenship []
# MAGIC     - Addresses [{}]
# MAGIC         - AddressType
# MAGIC         - Street
# MAGIC         - Number
# MAGIC         - PostalCode
# MAGIC         - City
# MAGIC         - Region
# MAGIC         - CountryName
# MAGIC         - CountryISO
# MAGIC     - Identification
# MAGIC     - IdentificationDocuments [{}]
# MAGIC         - DocumentId
# MAGIC         - DocumentTypeName
# MAGIC         - DocumentFileName
# MAGIC         - Documentstoreidentifier

# COMMAND ----------

# MAGIC %md
# MAGIC # Configurations and Data Ingestion

# COMMAND ----------

# pip install azure-cosmos
# %pip install com.azure.cosmos.spark:azure-cosmos-spark_3-5_2-12:4.34.0

# COMMAND ----------

#%pip install azure-cosmos azure-identity

# COMMAND ----------

# spark.catalog.clearCache()

# COMMAND ----------

# DBTITLE 1,Importing Libraries
import os
import re
import time
import pandas as pd
from datetime import datetime, timedelta

#Importing pyspark libraries and modules
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark import StorageLevel

#Local File Import
from RadarUtils import *

#Importing azure cosmos libraries and modules
from azure.cosmos import CosmosClient, exceptions, PartitionKey
from azure.identity import ClientSecretCredential
from azure.cosmos import CosmosClient, PartitionKey
from pyspark.sql.functions import col
from pyspark.storagelevel import StorageLevel

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
CosmosAccount=os.environ['CosmosAccount']
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
, 'party_documents'
, 'party_AllPartyDetails'
, 'party_client_structure_GUI'
, 'party_business_activities' 
, 'party_structure_Questionnaire'
, 'party_client_identifiers'
, 'party_RelatedPartyIdentificationDocument'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject='client_KeyStoreKey')


# COMMAND ----------

# MAGIC %md
# MAGIC #Data Transformation

# COMMAND ----------

# DBTITLE 1,Required Dataframes
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW structure AS
# MAGIC SELECT Sourceclient
# MAGIC   , ClientID
# MAGIC   , ClientGcobid
# MAGIC   , ClientFullLegalName AS FullLegalName
# MAGIC   , cast(ShareholdingPercentage as VARCHAR(10)) as  ShareholdingPercentage
# MAGIC   , cast(VotingRightPercentage as VARCHAR(10)) as VotingRightPercentage
# MAGIC   , MainClientPep
# MAGIC   , UboReason
# MAGIC   , ChildIdentity
# MAGIC   , ChildEntityId
# MAGIC   , UniqueChildPartyId
# MAGIC   , ChildIdentityName
# MAGIC   , ChildType
# MAGIC   , CASE WHEN ChildPEP = '' THEN NULL
# MAGIC     ELSE ChildPEP END AS ChildPEP
# MAGIC   , ParentIdentity
# MAGIC   , ParentEntityId
# MAGIC   , UniqueParentPartyId
# MAGIC   , ParentIdentityName
# MAGIC   , ParentType
# MAGIC   , CASE WHEN ParentPEP = '' THEN NULL
# MAGIC     ELSE ParentPEP END AS ParentPEP
# MAGIC   , concat(ParentDobDay,ParentDobMonth,ParentDobYear) as DateOfBirthofParent
# MAGIC   , concat(ChildDobDay,ChildDobMonth,ChildDobYear) as DateOfBirthofChild
# MAGIC   , TypesOfRelation 
# MAGIC FROM party_client_structure_GUI
# MAGIC WHERE sourcesystem='GCOB' AND IsLatestApprovedVersionOfClient='True'

# COMMAND ----------

# MAGIC %md
# MAGIC ##Address

# COMMAND ----------

# DBTITLE 1,Client Address
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW address_client_view AS 
# MAGIC SELECT
# MAGIC sourceclient
# MAGIC , 'Registered' AS AddressType
# MAGIC , regexp_replace(trim(RegisteredStreet), '[\n\r\t]', ' ') AS Street
# MAGIC , regexp_replace(trim(RegisteredNumber), '[\n\r\t]', ' ') AS Number
# MAGIC , regexp_replace(trim(RegisteredPostalCode), '[\n\r\t]', ' ') AS PostalCode
# MAGIC , regexp_replace(trim(RegisteredCity), '[\n\r\t]', ' ') AS City
# MAGIC , regexp_replace(trim(RegisteredRegion), '[\n\r\t]', ' ') AS Region
# MAGIC , CountryOfRegistration AS CountryName
# MAGIC , RegisteredCountryIsoCode AS CountryISO
# MAGIC FROM party_case_client_details where (clienttype like "Legal%")  and (IsLatestApprovedVersionOfClient='True') and (RegisteredStreet is not null or RegisteredNumber is not null or RegisteredPostalCode is not null or RegisteredCity is not null or RegisteredRegion is not null ) 
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select
# MAGIC sourceclient
# MAGIC , 'Residential' AS AddressType
# MAGIC , regexp_replace(trim(RegisteredStreet), '[\n\r\t]', ' ') AS Street
# MAGIC , regexp_replace(trim(RegisteredNumber), '[\n\r\t]', ' ') AS Number
# MAGIC , regexp_replace(trim(RegisteredPostalCode), '[\n\r\t]', ' ') AS PostalCode
# MAGIC , regexp_replace(trim(RegisteredCity), '[\n\r\t]', ' ') AS City
# MAGIC , regexp_replace(trim(RegisteredRegion), '[\n\r\t]', ' ') AS Region
# MAGIC , CountryOfRegistration AS CountryName
# MAGIC , RegisteredCountryIsoCode AS CountryISO
# MAGIC FROM party_case_client_details where (clienttype not like "Legal%") and (IsLatestApprovedVersionOfClient='True') and (RegisteredStreet is not null or RegisteredNumber is not null or RegisteredPostalCode is not null or RegisteredCity is not null or RegisteredRegion is not null  )
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT
# MAGIC sourceclient
# MAGIC , 'Operating' AS AddressType
# MAGIC , regexp_replace(trim(OperatingStreet), '[\n\r\t]', ' ') AS Street
# MAGIC , regexp_replace(trim(OperatingNumber), '[\n\r\t]', ' ') AS Number
# MAGIC , regexp_replace(trim(OperatingPostalCode), '[\n\r\t]', ' ') AS PostalCode
# MAGIC , regexp_replace(trim(OperatingCity), '[\n\r\t]', ' ') AS City
# MAGIC , regexp_replace(trim(OperatingRegion), '[\n\r\t]', ' ') AS Region
# MAGIC , CountryOfOperation AS CountryName
# MAGIC , OperatingCountryIsoCode AS CountryISO
# MAGIC FROM party_case_client_details where (clienttype like "Legal%") and (IsLatestApprovedVersionOfClient='True') and (OperatingStreet is not null or OperatingNumber is not null or OperatingPostalCode is not null or OperatingCity is not null or OperatingRegion is not null ) 

# COMMAND ----------

# Load the client address and replace empty strings with None (null) values
address_df_nulls = spark.table('address_client_view') \
                        .select(*[when(col(c) == "", None).otherwise(col(c)).alias(c) for c in spark.table('address_client_view').columns])

# Drop rows where all specified address-related columns are null
address_dict_df = address_df_nulls.dropna(how='all', subset=['Street', 'Number', 'PostalCode', 'City', 'CountryName', 'CountryISO'])

# Create a new column as a struct containing the specified address fields
address_dict_df = address_dict_df.withColumn('address_dict', struct("AddressType", "Street", "Number", "PostalCode", "City", "Region", "CountryName", "CountryISO"))

address_df = address_dict_df.groupBy('SourceClient').agg(array_sort(collect_set('address_dict')).alias('address_list'))

# Create or replace a temporary view 'address_df' with the resulting DataFrame
address_df.createOrReplaceTempView("client_address")

# COMMAND ----------

# DBTITLE 1,Child Address
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW address_child_view AS 
# MAGIC Select
# MAGIC cs.UniqueChildPartyId,
# MAGIC papd.GcobId,
# MAGIC 'Registered' AS AddressType,
# MAGIC regexp_replace(trim(papd.RegisteredNumber), '[\n\r\t]', ' ') AS Number,
# MAGIC regexp_replace(trim(papd.RegisteredStreet), '[\n\r\t]', ' ') AS Street,
# MAGIC regexp_replace(trim(papd.RegisteredCity), '[\n\r\t]', ' ') AS City,
# MAGIC regexp_replace(trim(papd.RegisteredRegion), '[\n\r\t]', ' ') as Region,
# MAGIC regexp_replace(trim(papd.RegisteredPostalCode), '[\n\r\t]', ' ') AS PostalCode,
# MAGIC regexp_replace(trim(papd.RegisteredCountryName), '[\n\r\t]', ' ') AS CountryName,
# MAGIC regexp_replace(trim(papd.RegisteredCountryIsoCode), '[\n\r\t]', ' ') AS CountryISO
# MAGIC FROM structure AS cs 
# MAGIC LEFT OUTER JOIN party_AllPartyDetails AS papd 
# MAGIC ON cs.UniqueChildPartyId = papd.UniquePartyId AND (cs.ChildType like "%Legal%") 
# MAGIC and papd.status='Live'
# MAGIC where (papd.RegisteredNumber is not null OR papd.RegisteredStreet is not null OR papd.RegisteredCity is not null OR papd.RegisteredRegion is not null OR papd.RegisteredPostalCode is not null OR papd.RegisteredCountryName is not null )
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select
# MAGIC cs.UniqueChildPartyId,
# MAGIC papd.GcobId,
# MAGIC 'Operating' AS AddressType,
# MAGIC regexp_replace(trim(papd.OperatingNumber), '[\n\r\t]', ' ') AS Number,
# MAGIC regexp_replace(trim(papd.OperatingStreet), '[\n\r\t]', ' ') AS Street,
# MAGIC regexp_replace(trim(papd.OperatingCity), '[\n\r\t]', ' ') AS City,
# MAGIC regexp_replace(trim(papd.OperatingRegion), '[\n\r\t]', ' ') AS Region,
# MAGIC regexp_replace(trim(papd.OperatingPostalCode), '[\n\r\t]', ' ') AS PostalCode,
# MAGIC regexp_replace(trim(papd.OperatingCountryName), '[\n\r\t]', ' ') AS CountryName,
# MAGIC regexp_replace(trim(papd.OperatingCountryIsoCode), '[\n\r\t]', ' ') AS CountryISO
# MAGIC FROM structure AS cs 
# MAGIC LEFT OUTER JOIN party_AllPartyDetails AS papd 
# MAGIC ON cs.UniqueChildPartyId = papd.UniquePartyId
# MAGIC and (cs.ChildType like "%Legal%") 
# MAGIC and papd.status='Live'
# MAGIC where (papd.OperatingNumber is not null OR papd.OperatingStreet is not null OR papd.OperatingCity is not null OR papd.OperatingRegion is not null OR papd.OperatingPostalCode is not null)
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select
# MAGIC cs.UniqueChildPartyId,
# MAGIC papd.GcobId,
# MAGIC 'Residential' AS AddressType,
# MAGIC regexp_replace(trim(papd.RegisteredNumber), '[\n\r\t]', ' ') AS Number,
# MAGIC regexp_replace(trim(papd.RegisteredStreet), '[\n\r\t]', ' ') AS Street,
# MAGIC regexp_replace(trim(papd.RegisteredCity), '[\n\r\t]', ' ') AS City,
# MAGIC regexp_replace(trim(papd.RegisteredRegion), '[\n\r\t]', ' ') as Region,
# MAGIC regexp_replace(trim(papd.RegisteredPostalCode), '[\n\r\t]', ' ') AS PostalCode,
# MAGIC regexp_replace(trim(papd.RegisteredCountryName), '[\n\r\t]', ' ') AS CountryName,
# MAGIC regexp_replace(trim(papd.RegisteredCountryIsoCode), '[\n\r\t]', ' ') AS CountryISO
# MAGIC FROM structure AS cs 
# MAGIC LEFT OUTER JOIN party_AllPartyDetails AS papd 
# MAGIC ON cs.UniqueChildPartyId = papd.UniquePartyId
# MAGIC and (cs.ChildType not like "%Legal%")
# MAGIC and papd.status='Live'
# MAGIC where (papd.RegisteredNumber is not null OR papd.RegisteredStreet is not null OR papd.RegisteredCity is not null OR papd.RegisteredRegion is not null OR papd.RegisteredPostalCode is not null OR papd.RegisteredCountryName is not null)

# COMMAND ----------

# Convert the address structs into a sorted array
address_child_dict_df = spark.table('address_child_view') \
                        .withColumn('address_dict',struct("AddressType","street", "Number","PostalCode","City","Region","CountryName","CountryISO"))

address_child_df = address_child_dict_df.groupBy('UniqueChildPartyId') \
                                        .agg(array_sort(collect_set('address_dict')).alias('address_list'))

# Creating a final view
address_child_df.createOrReplaceTempView("child_address")

# COMMAND ----------

# DBTITLE 1,Parent Address
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW address_parent_view AS 
# MAGIC Select
# MAGIC cs.UniqueParentPartyId,
# MAGIC papd.GcobId,
# MAGIC 'Registered' AS AddressType,
# MAGIC regexp_replace(trim(papd.RegisteredNumber), '[\n\r\t]', ' ') AS Number,
# MAGIC regexp_replace(trim(papd.RegisteredStreet), '[\n\r\t]', ' ') AS Street,
# MAGIC regexp_replace(trim(papd.RegisteredCity), '[\n\r\t]', ' ') AS City,
# MAGIC regexp_replace(trim(papd.RegisteredRegion), '[\n\r\t]', ' ') as Region,
# MAGIC regexp_replace(trim(papd.RegisteredPostalCode), '[\n\r\t]', ' ') AS PostalCode,
# MAGIC regexp_replace(trim(papd.RegisteredCountryName), '[\n\r\t]', ' ') AS CountryName,
# MAGIC regexp_replace(trim(papd.RegisteredCountryIsoCode), '[\n\r\t]', ' ') AS CountryISO
# MAGIC FROM structure AS cs 
# MAGIC LEFT OUTER JOIN party_AllPartyDetails AS papd 
# MAGIC ON cs.UniqueParentPartyId = papd.UniquePartyId
# MAGIC AND (cs.ParentType like "%Legal%") 
# MAGIC and papd.status='Live'
# MAGIC where (papd.RegisteredNumber is not null OR papd.RegisteredStreet is not null OR papd.RegisteredCity is not null OR papd.RegisteredRegion is not null OR papd.RegisteredPostalCode is not null OR papd.RegisteredCountryName is not null )
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select
# MAGIC cs.UniqueParentPartyId,
# MAGIC papd.GcobId,
# MAGIC 'Operating' AS AddressType,
# MAGIC regexp_replace(trim(papd.OperatingNumber), '[\n\r\t]', ' ') AS Number,
# MAGIC regexp_replace(trim(papd.OperatingStreet), '[\n\r\t]', ' ') AS Street,
# MAGIC regexp_replace(trim(papd.OperatingCity), '[\n\r\t]', ' ') AS City,
# MAGIC regexp_replace(trim(papd.OperatingRegion), '[\n\r\t]', ' ') AS Region,
# MAGIC regexp_replace(trim(papd.OperatingPostalCode), '[\n\r\t]', ' ') AS PostalCode,
# MAGIC regexp_replace(trim(papd.OperatingCountryName), '[\n\r\t]', ' ') AS CountryName,
# MAGIC regexp_replace(trim(papd.OperatingCountryIsoCode), '[\n\r\t]', ' ') AS CountryISO
# MAGIC FROM structure AS cs 
# MAGIC LEFT OUTER JOIN party_AllPartyDetails AS papd 
# MAGIC ON cs.UniqueParentPartyId = papd.UniquePartyId
# MAGIC AND  (cs.ParentType like "%Legal%")
# MAGIC and papd.status='Live'
# MAGIC where (papd.OperatingNumber is not null OR papd.OperatingStreet is not null OR papd.OperatingCity is not null OR papd.OperatingRegion is not null OR papd.OperatingPostalCode is not null)
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select
# MAGIC cs.UniqueParentPartyId,
# MAGIC papd.GcobId,
# MAGIC 'Residential' AS AddressType,
# MAGIC regexp_replace(trim(papd.RegisteredNumber), '[\n\r\t]', ' ') AS Number,
# MAGIC regexp_replace(trim(papd.RegisteredStreet), '[\n\r\t]', ' ') AS Street,
# MAGIC regexp_replace(trim(papd.RegisteredCity), '[\n\r\t]', ' ') AS City,
# MAGIC regexp_replace(trim(papd.RegisteredRegion), '[\n\r\t]', ' ') as Region,
# MAGIC regexp_replace(trim(papd.RegisteredPostalCode), '[\n\r\t]', ' ') AS PostalCode,
# MAGIC regexp_replace(trim(papd.RegisteredCountryName), '[\n\r\t]', ' ') AS CountryName,
# MAGIC regexp_replace(trim(papd.RegisteredCountryIsoCode), '[\n\r\t]', ' ') AS CountryISO
# MAGIC FROM structure AS cs 
# MAGIC LEFT OUTER JOIN party_AllPartyDetails AS papd 
# MAGIC ON cs.UniqueParentPartyId = papd.UniquePartyId 
# MAGIC AND (cs.ParentType not like "%Legal%") 
# MAGIC and papd.status='Live'
# MAGIC where (papd.RegisteredNumber is not null OR papd.RegisteredStreet is not null OR papd.RegisteredCity is not null OR papd.RegisteredRegion is not null OR papd.RegisteredPostalCode is not null OR papd.RegisteredCountryName is not null )

# COMMAND ----------

# Convert the address structs into a sorted array
address_parent_dict_df = spark.table('address_parent_view') \
                            .withColumn('address_dict',struct("AddressType","street", "Number","PostalCode","City","Region","CountryName","CountryISO"))

address_parent_df = address_parent_dict_df.groupBy('UniqueParentPartyId') \
                                        .agg(array_sort(collect_set('address_dict')).alias('address_list'))

# Creating a final view
address_parent_df.createOrReplaceTempView("parent_address")

# COMMAND ----------

# MAGIC %md
# MAGIC ##GCID

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary view for Client GCID
# MAGIC CREATE OR REPLACE TEMP VIEW Client_GCID AS
# MAGIC SELECT DISTINCT c.SourceClient, i.GCID
# MAGIC FROM party_case_client_details AS c
# MAGIC INNER JOIN client_KeyStoreKey AS i 
# MAGIC     ON i.KeyStore_type = 'GCOBID' 
# MAGIC     AND i.KeyStore_value = c.gcobid  
# MAGIC     AND c.ClientType LIKE '%Legal%'
# MAGIC UNION
# MAGIC SELECT DISTINCT c.SourceClient, i.GCID
# MAGIC FROM client_KeyStoreKey AS i
# MAGIC INNER JOIN party_client_identifiers AS b 
# MAGIC     ON i.KeyStore_value = b.value 
# MAGIC     AND i.KeyStore_type = 'NCINOID' 
# MAGIC     AND b.SystemIdType = 'SalesForce Client ID (nCino)'
# MAGIC INNER JOIN party_case_client_details AS c 
# MAGIC     ON b.clientid = c.gcobid 
# MAGIC     AND c.ClientType NOT LIKE '%Legal%';

# COMMAND ----------

# MAGIC %md
# MAGIC ##Naics

# COMMAND ----------

# DBTITLE 1,Client Naics Data
# Convert the address structs into a sorted array
NAICS_dict_df = spark.table("party_business_activities")\
                    .withColumn("naics_dict",(struct("NaicsCode", "NaicsName"))) \
                    .select("SourceClient",col("naics_dict"))

NAICS_df=NAICS_dict_df.groupBy('sourceclient') \
                    .agg(array_sort(collect_set('naics_dict')).alias('naics_list'))

# Creating a final view
NAICS_df.createOrReplaceTempView("Client_Naics")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Identification

# COMMAND ----------

# DBTITLE 1,Client Identification
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW IdentificationDocument_Client AS
# MAGIC SELECT DISTINCT
# MAGIC Ident.Gcobid,
# MAGIC CASE WHEN Clienttype='LegalEntityClient' THEN 'Legal Entity'
# MAGIC      WHEN Clienttype='RelatedLegalEntity' THEN 'Related Legal Entity'
# MAGIC      WHEN Clienttype='NaturalPersonClient' THEN 'Natural Person'
# MAGIC      WHEN Clienttype='RelatedNaturalPerson' THEN 'Related Natural Person'
# MAGIC      ELSE Clienttype END AS Clienttype,
# MAGIC Ident.IdType,
# MAGIC Ident.DescriptionOfOtherIdentificationType as IdTypeOtherDescription,
# MAGIC Ident.IdNumber,
# MAGIC Ident.PlaceOfIssue,
# MAGIC Ident.CountryOfIssue,
# MAGIC Ident.IssueDate,
# MAGIC Ident.ExpiryDate
# MAGIC FROM party_RelatedPartyIdentificationDocument AS Ident 
# MAGIC WHERE Ident.IdType IS NOT NULL

# COMMAND ----------

#Collecting Client Identification in a structure 
Ident_client_dict_df = spark.table("IdentificationDocument_Client") \
                            .withColumn("Ident_dict", struct("IdType","IdTypeOtherDescription","IdNumber","PlaceOfIssue","CountryOfIssue","IssueDate","ExpiryDate"))

Ident_client_df = Ident_client_dict_df.groupBy('Gcobid','Clienttype') \
                                    .agg(array_sort(collect_set('Ident_dict')).alias('Identification'))

# Creating a view
Ident_client_df.createOrReplaceTempView("Ident_client")

# COMMAND ----------

# DBTITLE 1,Related Party Identification
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW IdentificationDocument_RelatedParty AS
# MAGIC SELECT DISTINCT
# MAGIC Ident.UniquePartyId,
# MAGIC Ident.IdType,
# MAGIC Ident.DescriptionOfOtherIdentificationType AS IdTypeOtherDescription,
# MAGIC Ident.IdNumber,
# MAGIC Ident.PlaceOfIssue,
# MAGIC Ident.CountryOfIssue,
# MAGIC Ident.IssueDate,
# MAGIC Ident.ExpiryDate
# MAGIC FROM party_RelatedPartyIdentificationDocument AS Ident 
# MAGIC WHERE Ident.IdType IS NOT NULL

# COMMAND ----------

#Collecting RelatedParty Identification in a structure 
Ident_RelatedParty_dict_df = spark.table("IdentificationDocument_RelatedParty") \
                                .withColumn("Ident_dict",(struct("IdType","IdTypeOtherDescription","IdNumber","PlaceOfIssue","CountryOfIssue","IssueDate","ExpiryDate")))

Ident_RelatedParty_df = Ident_RelatedParty_dict_df.groupBy('UniquePartyId') \
                                                .agg(array_sort(collect_set('Ident_dict')).alias('Identification'))

# Creating a view
Ident_RelatedParty_df.createOrReplaceTempView("Ident_RelatedParty")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Nationality & Citizenship

# COMMAND ----------

# DBTITLE 1,Client Nationality and Citizenship
# MAGIC %sql
# MAGIC -- Create a temporary view for client
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Nationality_Citizenship_Client AS
# MAGIC SELECT
# MAGIC   pc.SourceClient, pc.ClientType, papd.GCDSID, papd.IncorporationDate,
# MAGIC   sort_array(collect_set(papd.Nationality)) AS Nationality,
# MAGIC   sort_array(collect_set(papd.Citizenship)) AS Citizenship
# MAGIC FROM
# MAGIC   party_case_client_details pc
# MAGIC     LEFT OUTER JOIN party_AllPartyDetails papd
# MAGIC       ON pc.Gcobid = papd.GcobId
# MAGIC       AND trim(pc.FullLegalName) = trim(papd.FullLegalName)
# MAGIC       AND papd.Status = 'Live'
# MAGIC GROUP BY
# MAGIC   pc.SourceClient, pc.ClientType, papd.GCDSID, papd.IncorporationDate;

# COMMAND ----------

# DBTITLE 1,Related Party Nationality and Citizenship
# MAGIC %sql
# MAGIC -- Create a temporary view for related child
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Nationality_Citizenship_Child AS
# MAGIC SELECT
# MAGIC   papd.UniquePartyId, cs.ChildType, papd.GCDSID, papd.IncorporationDate,
# MAGIC   sort_array(collect_set(papd.Nationality)) AS Nationality,
# MAGIC   sort_array(collect_set(papd.Citizenship)) AS Citizenship
# MAGIC FROM
# MAGIC   structure AS cs
# MAGIC     LEFT OUTER JOIN party_AllPartyDetails AS papd
# MAGIC       ON cs.UniqueChildPartyId = papd.UniquePartyId
# MAGIC       AND papd.Status = 'Live'
# MAGIC GROUP BY papd.UniquePartyId, cs.ChildType, papd.GCDSID, papd.IncorporationDate;
# MAGIC
# MAGIC -- Create a temporary view for related parent
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Nationality_Citizenship_Parent AS
# MAGIC SELECT
# MAGIC   papd.UniquePartyId, cs.ParentType, papd.GCDSID, papd.IncorporationDate,
# MAGIC   sort_array(collect_set(papd.Nationality)) AS Nationality,
# MAGIC   sort_array(collect_set(papd.Citizenship)) AS Citizenship
# MAGIC FROM
# MAGIC   structure AS cs
# MAGIC     LEFT OUTER JOIN party_AllPartyDetails AS papd
# MAGIC       ON cs.UniqueParentPartyId = papd.UniquePartyId
# MAGIC       AND papd.Status = 'Live'
# MAGIC GROUP BY papd.UniquePartyId, cs.ParentType, papd.GCDSID, papd.IncorporationDate;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documents

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Documents AS
# MAGIC select
# MAGIC   distinct d.GcobId,
# MAGIC   d.ClientID,
# MAGIC   d.EntityId,
# MAGIC   d.Source_Entity as SourceClient,
# MAGIC   d.UniquePartyId,
# MAGIC   CASE
# MAGIC     when d.EntityType = 0 then "LegalEntityClient"
# MAGIC     when d.EntityType = 1 then "RelatedLegalEntity"
# MAGIC     when d.EntityType = 2 then "RelatedNaturalPerson"
# MAGIC     when d.EntityType = 3 then "NaturalPersonClient"
# MAGIC   END as ClientType,
# MAGIC   d.DocumentId,
# MAGIC   d.DocumentSubTypeName as DocumentTypeName,
# MAGIC   d.DocumentFileName,
# MAGIC   d.Documentstoreidentifier
# MAGIC FROM party_documents AS d
# MAGIC where
# MAGIC   d.purpose like 'Verification of the identity%'

# COMMAND ----------

# DBTITLE 1,Client Documents
# MAGIC %sql
# MAGIC -- Create a temporary view for client documents
# MAGIC CREATE OR REPLACE TEMPORARY VIEW client_documents AS
# MAGIC SELECT DISTINCT a.gcobid AS identity, a.SourceClient, b.clientType, 
# MAGIC                 a.DocumentId, a.DocumentTypeName, a.DocumentFileName, a.Documentstoreidentifier
# MAGIC FROM Documents a
# MAGIC INNER JOIN party_case_client_details b ON a.SourceClient = b.SourceClient;

# COMMAND ----------

#Collecting client Documents in a structure
party_document_dict_df = spark.table('client_documents') \
                            .withColumn('document_dict',struct("DocumentId","DocumentTypeName","DocumentFileName","Documentstoreidentifier"))

party_document_df = party_document_dict_df.groupBy('SourceClient','identity') \
                                        .agg(array_sort(collect_set('document_dict')).alias('IdentificationDocuments'))

#Creating a final view
party_document_df.createOrReplaceTempView("party_client_documents")

# COMMAND ----------

# DBTITLE 1,Related Party Documents
# MAGIC %sql
# MAGIC
# MAGIC -- Create a temporary view for combined related parties
# MAGIC CREATE OR REPLACE TEMP VIEW combined_relatedParties AS
# MAGIC SELECT DISTINCT ClientGcobId, UniqueChildPartyId AS UniquePartyId, ChildEntityId AS EntityId
# MAGIC FROM structure
# MAGIC UNION
# MAGIC SELECT DISTINCT ClientGcobId, UniqueParentPartyId AS UniquePartyId, ParentEntityId AS EntityId
# MAGIC FROM structure;
# MAGIC
# MAGIC -- Create a temporary view for documents with combined related parties
# MAGIC CREATE OR REPLACE TEMP VIEW relatedParty_documents AS
# MAGIC SELECT DISTINCT b.ClientGcobId, a.UniquePartyId,
# MAGIC                 a.DocumentId, a.DocumentTypeName, a.DocumentFileName, a.Documentstoreidentifier
# MAGIC FROM Documents AS a
# MAGIC INNER JOIN combined_relatedParties AS b 
# MAGIC   ON a.UniquePartyId = b.UniquePartyId
# MAGIC WHERE a.Clienttype IN ('RelatedLegalEntity','RelatedNaturalPerson')
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT b.ClientGcobId, a.UniquePartyId,
# MAGIC                 a.DocumentId, a.DocumentTypeName, a.DocumentFileName, a.Documentstoreidentifier
# MAGIC FROM Documents AS a
# MAGIC INNER JOIN combined_relatedParties AS b 
# MAGIC   ON a.UniquePartyId = b.UniquePartyId
# MAGIC     and a.Gcobid = b.ClientGcobId
# MAGIC     and a.EntityId = b.EntityId
# MAGIC WHERE a.Clienttype IN ('LegalEntityClient','NaturalPersonClient');

# COMMAND ----------

# Collecting relatedParty Documents in a structure
relatedParty_documents_dict_df = spark.table('relatedParty_documents') \
                                    .withColumn('document_dict',struct("DocumentId","DocumentTypeName","DocumentFileName","Documentstoreidentifier"))

relatedParty_documents_df = relatedParty_documents_dict_df.groupBy('UniquePartyId','ClientGcobId') \
                                                        .agg(array_sort(collect_set('document_dict')).alias('IdentificationDocuments'))

#Creating a final view
relatedParty_documents_df.createOrReplaceTempView("related_party_documents")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Relationship

# COMMAND ----------

# DBTITLE 1,Client Relationship
relationship_df = spark.sql("""
    SELECT  a.sourceclient, a.ClientGcobid,a.FullLegalName,
    a.UniqueChildPartyId,
    a.UniqueparentPartyId,
    a.TypesOfRelation AS RelationType, 
    a.UboReason,
        CASE WHEN a.TypesOfRelation IN ('Shareholding', 'UboThroughReasonShareholding', 'UboShareholding') THEN b.ShareholdingPercentage 
            ELSE NULL END AS ShareholdingPercentage,
        CASE WHEN a.TypesOfRelation IN ('Shareholding', 'UboThroughReasonShareholding', 'UboShareholding') THEN b.VotingRightPercentage 
            ELSE NULL END AS VotingRightPercentage
    FROM structure a 
    INNER JOIN structure b 
        ON a.sourceclient = b.sourceclient 
        AND a.childidentity = b.childidentity 
        AND a.parentidentity = b.parentidentity 
        AND a.TypesOfRelation = b.TypesOfRelation
""")

relationship_dict_df = relationship_df\
.withColumn('relationship_dict', struct("RelationType","UboReason", "ShareholdingPercentage", "VotingRightPercentage"))\
.select('SourceClient', 'UniqueChildPartyId', 'UniqueParentPartyId', 'relationship_dict')

relationship_group1_df = relationship_dict_df\
.groupBy('SourceClient', 'UniqueChildPartyId', 'UniqueParentPartyId')\
.agg(array_sort(collect_set('relationship_dict')).alias('Relation'))

relationship_group2_df=relationship_group1_df.withColumn('Relationships',struct("UniqueChildPartyId","UniqueParentPartyId", "Relation")).select('SourceClient', 'Relationships')

relationship_df = relationship_group2_df\
.groupBy('SourceClient')\
.agg(array_sort(collect_set('Relationships')).alias('Relationships'))

relationship_df.createOrReplaceTempView("client_relationship")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Party

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
# MAGIC COALESCE(cs.DateOfBirthofChild, TO_CHAR(TO_DATE(TRIM(papd.IncorporationDate), 'dd-MM-yyyy'), 'dd-MM-yyyy')) AS `DOB/IncorporationDate`,
# MAGIC cs.ChildIdentityName AS FullLegalName,
# MAGIC papd.Nationality,
# MAGIC papd.Citizenship,
# MAGIC cs.ChildPEP AS PEP_Status,
# MAGIC acd.address_list AS Addresses,
# MAGIC cs.SourceClient,
# MAGIC papd.GCDSID AS GCID,
# MAGIC Ident.Identification,
# MAGIC pd.IdentificationDocuments
# MAGIC
# MAGIC FROM  structure cs
# MAGIC LEFT OUTER JOIN child_address AS acd 
# MAGIC      ON acd.UniqueChildPartyId = cs.UniqueChildPartyId
# MAGIC LEFT OUTER JOIN related_party_documents AS pd 
# MAGIC      ON pd.UniquePartyId = cs.UniqueChildPartyId and pd.ClientGcobid = cs.ClientGcobid
# MAGIC LEFT OUTER JOIN Nationality_Citizenship_Child AS papd 
# MAGIC      ON papd.UniquePartyId = cs.UniqueChildPartyId
# MAGIC LEFT OUTER JOIN Ident_RelatedParty AS Ident 
# MAGIC      ON Ident.UniquePartyId = cs.UniqueChildPartyId

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
# MAGIC COALESCE(cs.DateOfBirthofParent, TO_CHAR(TO_DATE(TRIM(papd.IncorporationDate), 'dd-MM-yyyy'), 'dd-MM-yyyy')) AS `DOB/IncorporationDate`,
# MAGIC cs.ParentIdentityName AS FullLegalName,
# MAGIC papd.Nationality,
# MAGIC papd.Citizenship,
# MAGIC cs.ParentPEP AS PEP_Status,
# MAGIC acd.address_list AS Addresses,
# MAGIC cs.SourceClient,
# MAGIC papd.GCDSID AS GCID,
# MAGIC Ident.Identification ,
# MAGIC pd.IdentificationDocuments
# MAGIC
# MAGIC FROM  structure cs
# MAGIC LEFT OUTER JOIN parent_address AS acd 
# MAGIC      ON acd.UniqueParentPartyId=cs.UniqueParentPartyId
# MAGIC LEFT OUTER JOIN related_party_documents AS pd 
# MAGIC      ON pd.UniquePartyId = cs.UniqueParentPartyId and pd.ClientGcobid = cs.ClientGcobid
# MAGIC LEFT OUTER JOIN Nationality_Citizenship_Parent AS papd 
# MAGIC      ON papd.UniquePartyId = cs.UniqueParentPartyId
# MAGIC LEFT OUTER JOIN Ident_RelatedParty AS Ident 
# MAGIC      ON Ident.UniquePartyId = cs.UniqueParentPartyId

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary view for Parties with the combined query
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Parties AS
# MAGIC SELECT *,
# MAGIC     CASE 
# MAGIC         WHEN PartyType = 'Natural Person acting in a Professional Capacity (NPPC)' THEN CONCAT('NP_NPPC_', GCOBID)
# MAGIC         WHEN PartyType = 'Legal Entity' THEN CONCAT('LE_', GCOBID)
# MAGIC         WHEN PartyType = 'Natural Person' THEN CONCAT('NP_', GCOBID)
# MAGIC         WHEN PartyType = 'Related Legal Entity' THEN CONCAT('RLE_', GCOBID)
# MAGIC         WHEN PartyType = 'Related Natural Person' THEN CONCAT('RNP_', GCOBID)
# MAGIC     END AS UniqueGCOBID
# MAGIC FROM (
# MAGIC   SELECT DISTINCT * 
# MAGIC   FROM Parties_parent 
# MAGIC   WHERE ClientGcobid <> GCOBID
# MAGIC   UNION
# MAGIC   SELECT DISTINCT * 
# MAGIC   FROM Parties_child  
# MAGIC   WHERE ClientGcobid <> GCOBID
# MAGIC );

# COMMAND ----------

related_parties_dict_df = spark.table('Parties') \
                    .withColumn('Parties',struct("GCID","GCOBID","PartyType","UniqueGCOBID", "FullLegalName","DOB/IncorporationDate","Nationality","Citizenship","PEP_Status","Addresses","Identification" ,"IdentificationDocuments")).distinct()

related_parties_df = related_parties_dict_df.groupBy('SourceClient') \
                                    .agg(array_sort(collect_set('Parties')).alias('parties_list'))

related_parties_df.createOrReplaceTempView("related_parties")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Final_Document AS
# MAGIC SELECT DISTINCT 
# MAGIC pc.SourceClient,
# MAGIC i.GCID , 
# MAGIC pc.gcobid as GCOBID,
# MAGIC pc.ClientType as PartyType,
# MAGIC CASE WHEN pc.ClientType = 'Natural Person acting in a Professional Capacity (NPPC)' THEN CONCAT('NP_NPPC_', pc.GCOBID)
# MAGIC     WHEN pc.ClientType = 'Legal Entity' THEN CONCAT('LE_', pc.GCOBID)
# MAGIC     WHEN pc.ClientType = 'Natural Person' THEN CONCAT('NP_', pc.GCOBID)
# MAGIC     WHEN pc.ClientType = 'Related Legal Entity' THEN CONCAT('RLE_', pc.GCOBID)
# MAGIC     WHEN pc.ClientType = 'Related Natural Person' THEN CONCAT('RNP_', pc.GCOBID)
# MAGIC     END AS UniqueGCOBID,
# MAGIC pc.FullLegalName, 
# MAGIC pc.GLOBALCLIENTOWNER AS ClientOwner,
# MAGIC pc.ClientLifeCycleName,
# MAGIC COALESCE(pc.DateOfBirth, TO_CHAR(TO_DATE(TRIM(ncc.IncorporationDate), 'dd-MM-yyyy'), 'dd-MM-yyyy')) AS `DOB/IncorporationDate`,
# MAGIC ncc.Nationality,
# MAGIC ncc.Citizenship,
# MAGIC ps.MainClientPEP AS PEP_Status,
# MAGIC pc.FIHubIndicator,
# MAGIC d.address_list AS Addresses,
# MAGIC Ident.Identification,
# MAGIC pd.IdentificationDocuments,
# MAGIC e.naics_list AS KYC_NAICS,
# MAGIC g.Relationships AS Relationships,
# MAGIC regexp_replace(st.CommentsOnStructure, "<[^>]+>", "") AS CommentsOnStructure,
# MAGIC h.parties_list AS Parties,
# MAGIC pc.CaseCompletedDate 
# MAGIC
# MAGIC FROM  party_case_client_details AS pc
# MAGIC LEFT OUTER JOIN structure AS ps
# MAGIC     ON ps.SourceClient = pc.SourceClient
# MAGIC LEFT OUTER JOIN party_client_documents AS pd 
# MAGIC     ON pd.SourceClient = pc.SourceClient
# MAGIC LEFT OUTER JOIN Nationality_Citizenship_Client AS ncc 
# MAGIC     ON pc.SourceClient = ncc.SourceClient
# MAGIC LEFT OUTER JOIN Ident_client AS Ident 
# MAGIC     ON Ident.GcobId = pc.GcobId AND Ident.ClientType = pc.ClientType
# MAGIC LEFT OUTER JOIN client_address AS d
# MAGIC     ON d.SourceClient = pc.SourceClient
# MAGIC LEFT OUTER JOIN Client_Naics AS e 
# MAGIC     ON e.SourceClient = pc.SourceClient
# MAGIC LEFT OUTER JOIN party_structure_Questionnaire AS st 
# MAGIC     ON st.clientGcobId = pc.gcobid AND st.IsLatestApprovedVersionOfClient='True' AND st.SourceClient=pc.SourceClient
# MAGIC LEFT OUTER JOIN client_relationship AS g 
# MAGIC     ON g.SourceClient = pc.SourceClient
# MAGIC LEFT OUTER JOIN related_parties AS h 
# MAGIC     ON h.SourceClient=pc.SourceClient
# MAGIC INNER JOIN Client_GCID AS i 
# MAGIC     ON i.SourceClient = pc.SourceClient
# MAGIC WHERE pc.IsLatestApprovedVersionOfClient='True';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate the record size for each record in final Document
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Record_Size AS
# MAGIC SELECT DISTINCT *,
# MAGIC     LENGTH(COALESCE(CAST(TO_JSON(Parties) AS STRING), '')) +
# MAGIC     LENGTH(COALESCE(GCID, '')) +
# MAGIC     LENGTH(COALESCE(GCOBID, '')) +
# MAGIC     LENGTH(COALESCE(ClientOwner, '')) +
# MAGIC     LENGTH(COALESCE(FullLegalName, '')) +
# MAGIC     LENGTH(COALESCE(PartyType, '')) +
# MAGIC     LENGTH(COALESCE(ClientLifeCycleName, '')) +
# MAGIC     LENGTH(COALESCE(CAST(TO_JSON(addresses) AS STRING), '')) +
# MAGIC     LENGTH(COALESCE(CAST(TO_JSON(KYC_NAICS) AS STRING), '')) +
# MAGIC     LENGTH(COALESCE(CAST(TO_JSON(Relationships) AS STRING), '')) +
# MAGIC     LENGTH(COALESCE(CommentsOnStructure, '')) AS record_size,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY GCID ORDER BY CASE WHEN clientlifecyclename='Client' THEN 1 ELSE 2 END ASC, CaseCompletedDate DESC) AS ROWNUM 
# MAGIC FROM Final_Document;

# COMMAND ----------

final_data = spark.sql("""
    SELECT * 
    FROM Record_Size 
    WHERE record_size < 2000000 and ROWNUM=1
""").drop("record_size","CaseCompletedDate","ROWNUM")
final_data.createOrReplaceTempView('final_data')

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Data

# COMMAND ----------

# Load data from Databricks

window_spec = Window.orderBy("GCID")
# final_data = final_data.withColumn("id",row_number().over(window_spec).cast("string"))
final_data = final_data.withColumn("id",col('GCID').cast("string"))
final_data = final_data.repartition(240)
final_data.persist(StorageLevel.DISK_ONLY)

# COMMAND ----------

from functools import reduce

yesterday_df=spark.table("radar.cosmos_data").withColumn("id", col("GCID"))
today_df=final_data

#new records which are not present in the yesterday's data
new_records_df=today_df.join(yesterday_df, on='GCID',how='left_anti')

#deleted records
deleted_records_df=yesterday_df.join(today_df, on='GCID',how='left_anti')
deleted_gcid_list= [row['GCID'] for row in deleted_records_df.collect()]

#updated/modified records
joined_df=today_df.alias("t").join(yesterday_df.alias("y"), on='GCID',how="inner")

cols_to_compare=[col for col in today_df.columns if col!='GCID']

diff_condition=reduce(lambda a,b: a|b, [(col(f"t.{c}")!=col(f"y.{c}")) for c in cols_to_compare])

changed_records_df= joined_df.filter(diff_condition).select("t.*")

#combine new records and updated records
upsert_records_df=new_records_df.unionByName(changed_records_df)

# COMMAND ----------

print("Yesterday's records :", yesterday_df.count())
print("Today's records :", today_df.count())
print("deleted records :", deleted_records_df.count())
print("New records :", new_records_df.count())
print("upsert records :", upsert_records_df.count())

# COMMAND ----------

# DBTITLE 1,SPN Authentication to Load Data
from azure.identity import ClientSecretCredential
from azure.cosmos import CosmosClient, PartitionKey
from pyspark.sql import SparkSession

# Cosmos DB configuration
CosmosDatabaseEndpoint = f"https://{CosmosAccount}.documents.azure.com:443/"
CosmosDatabaseName = 'CosmosDocDB'
CosmosContainerName = 'ClientData'

# Authenticate using SPN
credential = ClientSecretCredential(TenantId, app_reg_app_id, service_credential)

# Initialize the Cosmos client with SPN
client = CosmosClient(CosmosDatabaseEndpoint, credential)

# Create database and container
database = client.create_database_if_not_exists(CosmosDatabaseName)
container = database.create_container_if_not_exists(
    id=CosmosContainerName,
    partition_key=PartitionKey(path="/GCID"),
    offer_throughput=5000
)

# Cosmos DB configurations for Spark
CosmosConfig = {
    "spark.cosmos.accountEndpoint": CosmosDatabaseEndpoint,
    "spark.cosmos.auth.type": "ServicePrincipal",
    "spark.cosmos.account.subscriptionId": subscriptionId,
    "spark.cosmos.account.tenantId": TenantId,
    "spark.cosmos.auth.aad.clientId": app_reg_app_id,
    "spark.cosmos.auth.aad.clientSecret": service_credential,
    "spark.cosmos.database": CosmosDatabaseName,
    "spark.cosmos.container": CosmosContainerName,
    "spark.cosmos.write.strategy": "ItemOverwrite",
    "spark.cosmos.read.inferSchema.enabled": "true",
    "spark.cosmos.write.bulk.enabled": "false",
    "spark.cosmos.write.bulk.maxPendingOperations": "1000",
    "spark.cosmos.account.resourceGroupName": ResourceGroupName
}

# Setting Spark configurations
spark = SparkSession.builder.appName("CosmosDBIntegration").getOrCreate()
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.auth.type", "ServicePrincipal")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.account.subscriptionId", subscriptionId)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.account.tenantId", TenantId)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.auth.aad.clientId", app_reg_app_id)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.auth.aad.clientSecret", service_credential)

total_records = final_data.count()
print("total_records :", total_records)  #28417

# Loop to Upsert records
for row in upsert_records_df.collect():
    doc=row.asDict(recursive=True)
    # doc['id']=doc['GCID']
    container.upsert_item(doc)

# Loop to delete records 
for gcid in deleted_gcid_list:
    try:
        container.delete_item(item=gcid, partition_key=gcid)
    except Exception as e:
        print(f"Failed to delete GCID {gcid}:{str(e)}")

# write_batch(final_data, CosmosConfig)
print("written all batches successfully")


# COMMAND ----------

# DBTITLE 1,Create table
final_data.write.mode('overwrite').saveAsTable("radar.cosmos_data")

# COMMAND ----------

# DBTITLE 1,Deprecated code with batch load
# # Cosmos DB configuration

# CosmosAccount = 'radarapicosmossqlapipreprd'
# CosmosDatabaseEndpoint = f"https://{CosmosAccount}.documents.azure.com:443/"
# CosmosDatabaseName = 'CosmosDocDB'
# CosmosContainerName = 'ClientData'

# # Authenticate using SPN

# credential = ClientSecretCredential(TenantId, app_reg_app_id, service_credential)

# # Initialize the Cosmos client with SPN

# client = CosmosClient(CosmosDatabaseEndpoint, credential)

# # Create database and container
# database = client.create_database_if_not_exists(CosmosDatabaseName)
# container = database.create_container_if_not_exists(
#     id=CosmosContainerName,
#     partition_key=PartitionKey(path="/GCID"),
#     offer_throughput=5000
# )

# #print("Database and container created")

# # Cosmos DB configurations for Spark

# CosmosConfig = {
#     "spark.cosmos.accountEndpoint": CosmosDatabaseEndpoint,
#     "spark.cosmos.auth.type": "ServicePrincipal",
#     "spark.cosmos.account.subscriptionId": "caedb2e4-7e6b-4054-8811-623ac1e27802",
#     "spark.cosmos.account.tenantId": TenantId,
#     "spark.cosmos.auth.aad.clientId": app_reg_app_id,
#     "spark.cosmos.auth.aad.clientSecret": service_credential,
#     "spark.cosmos.database": CosmosDatabaseName,
#     "spark.cosmos.container": CosmosContainerName,
#     # "spark.cosmos.write.upsertEnabled": "true",
#     "spark.cosmos.write.strategy": "ItemOverwrite",
#     "spark.cosmos.read.inferSchema.enabled": "true",
#     "spark.cosmos.write.bulk.enabled": "true",
#     "spark.cosmos.write.bulk.maxPendingOperations": "1000",
#     "spark.cosmos.account.resourceGroupName": "rg-wr-radar-api-preprod" 
#     }

# # Setting Spark configurations

# spark = SparkSession.builder.appName("CosmosDBIntegration").getOrCreate()
# spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
# spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.auth.type", "ServicePrincipal")
# spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.account.subscriptionId", "caedb2e4-7e6b-4054-8811-623ac1e27802")
# spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.account.tenantId", TenantId)
# spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.auth.aad.clientId", app_reg_app_id)
# spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.auth.aad.clientSecret", service_credential)


# # Function to write single batch df to Cosmos DB
# def write_batch(batch_df, CosmosConfig):
#     try:
#         batch_df.write.format("cosmos.oltp").options(**CosmosConfig).mode("append").save()
#     except Exception as e:
#         print(f"Error writing batch: {e}")


# # Batch Processing
# batch_size = 10
# total_records = final_data.count()
# num_batches = (total_records // batch_size) + (1 if total_records % batch_size > 0 else 0)
# print("Total_records: ", total_records, ", Num_Batches: ", num_batches)

# for i in range(num_batches):
#     start_index = i * batch_size
#     end_index = start_index + batch_size if start_index + batch_size <= total_records else total_records

#     print(f"batch_num={i+1} and start_index={start_index} and end_index={end_index}")
#     batch_df = final_data.filter(((col('id').cast('int')) >= start_index) & ((col('id').cast('int')) <= end_index))

#     batch_df.persist(StorageLevel.MEMORY_AND_DISK)
#     write_batch(batch_df, CosmosConfig)
#     batch_df.unpersist()

# print("written all batches successfully")

# COMMAND ----------

# DBTITLE 1,Deprecated Code
# access: 

#Dev
#CosmosAccount = ''
#CosmosDatabaseEndpoint=f"https://{CosmosAccount}.documents.azure.com:443/"
#CosmosDatabaseName = 'CosmosDocDB'
#CosmosContainerName = 'ClientData'
#CosmosDatabasePrimaryKey=f'Key removed'

# #PreProd
# CosmosAccount = ''
# CosmosDatabaseEndpoint=f"https://{CosmosAccount}.documents.azure.com:443/"
# CosmosDatabaseName = 'CosmosDocDB'
# CosmosContainerName = 'ClientData'
# CosmosDatabasePrimaryKey=f'Key removed'

# COMMAND ----------

# #Initialize the cosmos client
# client=CosmosClient(CosmosDatabaseEndpoint, CosmosDatabasePrimaryKey)

# #create database
# database = client.create_database_if_not_exists(CosmosDatabaseName)

# #create container with partition key
# container = database.create_container_if_not_exists(id=CosmosContainerName, partition_key=PartitionKey(path="/GCID"),offer_throughput=5000)

# print("Database and container created")

# COMMAND ----------

# # Initialize Spark session
# spark = SparkSession.builder\
#     .appName("WriteToCosmosDB") \
#     .config("spark.jars.packages","com.azure.cosmos.spark:azure-cosmos-spark_3-4_2-12:4.25.0")\
#     .getOrCreate()

# Load data from Databricks
# window_spec = Window.orderBy("GCID")
# final_data = final_data.withColumn("id",row_number().over(window_spec).cast("string"))

# final_data = final_data.repartition(240)
# final_data.persist(StorageLevel.DISK_ONLY)

# COMMAND ----------


# This code is used to write data into Cosmos DB using AccountKey.

# batch_size=100

# #Cosmos DB configurations
# CosmosConfig = {
#   "spark.cosmos.accountEndpoint": CosmosDatabaseEndpoint,
#   "spark.cosmos.accountKey": CosmosDatabasePrimaryKey,
#   "spark.cosmos.database": CosmosDatabaseName,
#   "spark.cosmos.container": CosmosContainerName,
#   "spark.cosmos.write.strategy": "ItemOverwrite",
#   "spark.cosmos.read.inferSchema.enabled": "true",
#   "spark.cosmos.write.bulk.enabled": "true",
#   "spark.cosmos.write.bulk.maxPendingOperations": "1000",
#   # "spark.cosmos.write.maxRetryAttemptsInSeconds": "60",
#   # "spark.cosmos.write.maxRetryAttemptsOnThrottledRequests": "9",
#   # "spark.cosmos.consistency.level": "Session"
# }

# #function to write single batch df to cosmos DB
# def write_batch(batch_df,CosmosConfig):
#     batch_df.write.format("cosmos.oltp").options(**CosmosConfig).mode("append").save()

# #Batch Processing

# total_records=final_data.count()
# # total_records=27644
# num_batches=(total_records//batch_size)+(1 if total_records%batch_size>0 else 0)
# print("Total_records: ",total_records,",Num_Batches: ",num_batches)

# for i in range(num_batches):
#     #create a batch dataframe

#     start_index=i*batch_size
#     end_index=(start_index+batch_size if start_index+batch_size<=total_records else total_records)

#     print(f"batch_num={i+1} and start_index={start_index} and end_index={end_index}")
#     batch_df=final_data.filter(((col('id').cast('int'))>start_index) & ((col('id').cast('int'))<=end_index))

#     #write batch dataframe to cosmos DB
#     batch_df.persist(StorageLevel.MEMORY_AND_DISK)
#     write_batch(batch_df,CosmosConfig)
#     batch_df.unpersist()

# print("written all batches successfully")
