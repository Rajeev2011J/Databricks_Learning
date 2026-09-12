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
# MAGIC - Birth_Incorporation_Date
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
# MAGIC     - Birth_Incorporation_Date
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
# MAGIC #### Goal
# MAGIC - To bring Radar API Document details from existing Radar objects
# MAGIC
# MAGIC #### author
# MAGIC - Sowmyashree.Parashivamurthy.Sudha@rabobank.com
# MAGIC - Prajit.Tatari@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Read Data from Radar objects and build the API similar to GCOB
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Sowmyashree	 |3-June-2025 |- |First release
# MAGIC | Prajit	 |10-July-2025 |- |Additional Requirements
# MAGIC | Abhishek Jaiswal	 |16-Sep-2025 |13584702 |Added CountryOfIssueIsoCode
# MAGIC | Abhishek Jaiswal	 |02-Oct-2025 |13883964 |Read version 2 for Naics and get Naics description
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Configurations and Data Ingestion

# COMMAND ----------

# pip install azure-cosmos
# %pip install com.azure.cosmos.spark:azure-cosmos-spark_3-5_2-12:4.34.0
# %pip install azure-cosmos azure-identity
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
# date_parameter = datetime.today().strftime('%Y%m%d')
date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

CosmosAccount=os.environ['CosmosAccount']
subscriptionId=os.environ['subscriptionId']
ResourceGroupName=os.environ['resourceGroup_API']
environment=os.environ['ENV']

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,Outh2 Configuration
ReadStorage = f'saradar{environment}'
authenticate_storage_account(ReadStorage)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# DBTITLE 1,Source Data Ingestion
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'Party_Structure',
'Party',
'Party_Address',
'Party_SystemIdentifier',
'Party_CountryAffiliation',
'Party_Coverage',
'Party_Identification',
'Party_Documents',
'Party_Naics'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://radardatamodel@{ReadStorage}.dfs.core.windows.net/{row.GDPname}/3/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relationship

# COMMAND ----------

relationship_df = spark.sql("""
    SELECT a.PartyIdentifier ,a.PartyChildIdentifier, a.PartyParentIdentifier, a.TypesOfRelation as RelationType,
    CASE WHEN a.TypesOfRelation IN ('Shareholding', 'UboThroughReasonShareholding', 'UboShareholding') THEN 
    CAST(b.ShareholdingPercentage AS STRING) END as ShareholdingPercentage , 
    CASE WHEN a.TypesOfRelation IN ('Shareholding', 'UboThroughReasonShareholding', 'UboShareholding') THEN
    CAST(b.VotingRightPercentage AS STRING) END as VotingRightPercentage
    FROM Party_structure a
    INNER JOIN Party_structure b 
        ON a.PartyIdentifier = b.PartyIdentifier
        AND a.PartyChildIdentifier = b.PartyChildIdentifier 
        AND a.PartyParentIdentifier = b.PartyParentIdentifier 
        AND a.TypesOfRelation = b.TypesOfRelation """)

relationship_dict_df = relationship_df\
.withColumn('relationship_dict', struct("RelationType", "ShareholdingPercentage", "VotingRightPercentage"))\
.select('PartyIdentifier','PartyChildIdentifier', 'PartyParentIdentifier', 'relationship_dict')

relationship_group1_df = relationship_dict_df\
.groupBy('PartyIdentifier','PartyChildIdentifier', 'PartyParentIdentifier')\
.agg(array_sort(collect_set('relationship_dict')).alias('Relation'))

relationship_group2_df = relationship_group1_df\
    .withColumn('Relationships',struct('PartyChildIdentifier', 'PartyParentIdentifier', 'Relation'))\
    .select('PartyIdentifier', 'Relationships')

relationship_df = relationship_group2_df\
.groupBy('PartyIdentifier')\
.agg(array_sort(collect_set('Relationships')).alias('Relationships'))

relationship_df.createOrReplaceTempView("party_Relationship")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Address

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW address_client_view AS 
# MAGIC SELECT
# MAGIC PartyIdentifier
# MAGIC , AddressType
# MAGIC , regexp_replace(trim(Street), '[\n\r\t]', ' ') AS Street
# MAGIC , regexp_replace(trim(HouseNumber), '[\n\r\t]', ' ') AS Number
# MAGIC , regexp_replace(trim(PostalCode), '[\n\r\t]', ' ') AS PostalCode
# MAGIC , regexp_replace(trim(City), '[\n\r\t]', ' ') AS City
# MAGIC , regexp_replace(trim(Region), '[\n\r\t]', ' ') AS Region
# MAGIC , Country AS CountryName
# MAGIC , CountryCode AS CountryISO
# MAGIC FROM Party_Address
# MAGIC where Street IS NOT NULL
# MAGIC   OR HouseNumber IS NOT NULL
# MAGIC   OR PostalCode IS NOT NULL
# MAGIC   OR City IS NOT NULL
# MAGIC   OR Region IS NOT NULL

# COMMAND ----------

# Load the client address and replace empty strings with None (null) values
address_df_nulls = spark.table('address_client_view') \
                        .select(*[when(col(c) == "", None).otherwise(col(c)).alias(c) for c in spark.table('address_client_view').columns])

# Drop rows where all specified address-related columns are null
address_dict_df = address_df_nulls.dropna(how='all', subset=['Street', 'Number', 'PostalCode', 'City', 'CountryName', 'CountryISO'])

# Create a new column as a struct containing the specified address fields
address_dict_df = address_dict_df.withColumn('address_dict', struct("AddressType", "Street", "Number", "PostalCode", "City", "Region", "CountryName", "CountryISO"))

address_df = address_dict_df.groupBy('PartyIdentifier').agg(array_sort(collect_set('address_dict')).alias('address_list'))

# Create or replace a temporary view 'address_df' with the resulting DataFrame
address_df.createOrReplaceTempView("party_Address")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Naics

# COMMAND ----------

# Convert the address structs into a sorted array
from pyspark.sql.functions import col, struct


NAICS_dict_df = spark.table("party_naics").withColumn(
    "naics_dict",
    struct(col("NAICSCode").alias("Code"),
        col("NAICSDescription").alias("Description")))\
    .select(col("PartyIdentifier"),
    col("naics_dict"))


NAICS_df=NAICS_dict_df.groupBy('PartyIdentifier') \
                    .agg(array_sort(collect_set('naics_dict')).alias('naics_list'))

# Creating a final view
NAICS_df.createOrReplaceTempView("party_Naics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## National Affiliation

# COMMAND ----------

# Convert the address structs into a sorted array
CountryAffiliation_dict_df = spark.table("Party_CountryAffiliation")\
                    .withColumn("CountryAffiliation_dict",when(col("NationalAffiliationType") == 'Nationality', 
                                                   struct(col("NationalAffiliationType").alias("Type"), col("CountryISOcode").alias("CountryCode") )
                                                   ).when(col("NationalAffiliationType") == 'Citizenship', 
                                                   struct(col("NationalAffiliationType").alias("Type"), col("CountryISOcode").alias("CountryCode")))) \
                    .select("PartyIdentifier",col("CountryAffiliation_dict"))

CountryAffiliation_df = CountryAffiliation_dict_df.groupBy('PartyIdentifier') \
                    .agg(array_sort(collect_set('CountryAffiliation_dict')).alias('CountryAffiliation_list'))

# Creating a final view
CountryAffiliation_df.createOrReplaceTempView("party_NationalAffiliation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ownership Coverage

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW OwnershipCoverage AS
# MAGIC SELECT DISTINCT pc1.PartyIdentifier
# MAGIC , pc1.CoverageTypeDescription as Type
# MAGIC , pc1.CoverageValue AS Email
# MAGIC , pc2.CoverageValue AS Location
# MAGIC FROM party_coverage AS pc1
# MAGIC JOIN party_coverage AS pc2
# MAGIC   ON pc1.PartyIdentifier = pc2.PartyIdentifier
# MAGIC WHERE (
# MAGIC     pc1.coverageType = 'GCO' AND 
# MAGIC     pc2.coverageType = 'Location' AND 
# MAGIC     pc2.CoverageTypeDescription = 'GCO Location'
# MAGIC ) OR (
# MAGIC     pc1.coverageType = 'LCO' AND 
# MAGIC     pc2.coverageType = 'Location' AND 
# MAGIC     pc2.CoverageTypeDescription = 'LCO Location'
# MAGIC );

# COMMAND ----------

# Convert the Ownership structs into a sorted array
OwnershipCoverage_dict_df = spark.table("OwnershipCoverage")\
                    .withColumn("OwnershipCoverage_dict", struct('Type','Email','Location')) \
                    .select("PartyIdentifier",col("OwnershipCoverage_dict"))

OwnershipCoverage_df = OwnershipCoverage_dict_df.groupBy('PartyIdentifier') \
                    .agg(array_sort(collect_set('OwnershipCoverage_dict')).alias('OwnershipCoverage_list'))

# Creating a final view
OwnershipCoverage_df.createOrReplaceTempView("party_OwnershipCoverage")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Identification

# COMMAND ----------

#Collecting RelatedParty Identification in a structure 
Ident_Party_dict_df = spark.table("Party_Identification") \
                                .withColumn("Ident_dict",(struct("IdType","IdTypeOtherDescription","IdNumber","PlaceOfIssue","CountryOfIssue","CountryOfIssueISOCode",col("IssueDate").cast('String'),col("ExpiryDate").cast('String'))))

Ident_Party_df = Ident_Party_dict_df.groupBy('PartyIdentifier') \
                                                .agg(array_sort(collect_set('Ident_dict')).alias('Identification'))

# Creating a view
Ident_Party_df.createOrReplaceTempView("party_Identification")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documents

# COMMAND ----------

#Collecting Party Documents 
Docs_Party_dict_df = spark.table("party_Documents").filter("purpose like 'Verification of the identity%'") \
    .withColumn("Docs_dict",(struct("DocumentId",col("DocumentSubTypeName").alias('DocumentTypeName'),"DocumentFileName","Documentstoreidentifier")))

Docs_Party_df = Docs_Party_dict_df.groupBy('PartyIdentifier','ClientId') \
                                    .agg(array_sort(collect_set('Docs_dict')).alias('IdentificationDocuments'))

Docs_Party_max = Docs_Party_df.groupBy('PartyIdentifier').agg(max('ClientId').alias('ClientId'))

Docs_Party_final = Docs_Party_df.join(Docs_Party_max, on=['PartyIdentifier','ClientId']).select('PartyIdentifier','IdentificationDocuments')

# Creating a view
Docs_Party_final.createOrReplaceTempView("party_IdentificationDocuments")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Party

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Parties_child AS
# MAGIC select DISTINCT
# MAGIC ps.PartyIdentifier,
# MAGIC psi.LocalSystemIdentifier as GCID,
# MAGIC CASE WHEN p.Party_Type IN ('Legal Entity','Related Legal Entity') then NULL ELSE p.FirstName END AS FirstName,
# MAGIC CASE WHEN p.Party_Type IN ('Legal Entity','Related Legal Entity') then NULL ELSE p.MiddleName END AS MiddleName,
# MAGIC CASE WHEN p.Party_Type IN ('Legal Entity','Related Legal Entity') then NULL ELSE p.LastName END AS LastName,
# MAGIC p.FullLegalName AS FullLegalName,
# MAGIC p.Party_Type AS PartyType,
# MAGIC ps.PartyChildIdentifier AS PartyChildIdentifier,
# MAGIC -- COALESCE(p.DateOfBirth, TO_CHAR(TO_DATE(TRIM(p.IncorporationDate), 'dd-MM-yyyy'), 'dd-MM-yyyy')) AS `Birth_Incorporation_Date`,
# MAGIC CASE WHEN p.DateOfBirth IS NOT NULL THEN p.DateOfBirth ELSE TO_CHAR(TO_DATE(TRIM(p.IncorporationDate), 'dd-MM-yyyy'), 'dd-MM-yyyy') END AS Birth_Incorporation_Date,
# MAGIC cna.CountryAffiliation_list AS NationalAffiliation,
# MAGIC p.PEPStatus AS PEPStatus,
# MAGIC ps.IsUBO,
# MAGIC ps.UBOReason,
# MAGIC ca.address_list AS Addresses,
# MAGIC id.Identification,
# MAGIC pid.IdentificationDocuments
# MAGIC
# MAGIC FROM  Party p
# MAGIC INNER JOIN  Party_Structure  ps 
# MAGIC     on p.PartyIdentifier = ps.PartyChildIdentifier
# MAGIC LEFT OUTER JOIN party_Address ca 
# MAGIC     on p.PartyIdentifier = ca.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_NationalAffiliation cna 
# MAGIC     ON p.PartyIdentifier = cna.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_Identification AS id 
# MAGIC     ON p.PartyIdentifier= id.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_IdentificationDocuments AS pid 
# MAGIC     ON p.PartyIdentifier= pid.PartyIdentifier
# MAGIC LEFT OUTER JOIN Party_SystemIdentifier psi
# MAGIC     ON p.PartyIdentifier=psi.PartyIdentifier and Application='GCDS'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Parties_parent AS
# MAGIC select DISTINCT
# MAGIC ps.PartyIdentifier,
# MAGIC psi.LocalSystemIdentifier as GCID,
# MAGIC CASE WHEN p.Party_Type IN ('Legal Entity','Related Legal Entity') then NULL ELSE p.FirstName END AS FirstName,
# MAGIC CASE WHEN p.Party_Type IN ('Legal Entity','Related Legal Entity') then NULL ELSE p.MiddleName END AS MiddleName,
# MAGIC CASE WHEN p.Party_Type IN ('Legal Entity','Related Legal Entity') then NULL ELSE p.LastName END AS LastName,
# MAGIC p.FullLegalName AS FullLegalName,
# MAGIC p.Party_Type AS PartyType,
# MAGIC ps.PartyParentIdentifier AS PartyParentIdentifier,
# MAGIC CASE WHEN p.DateOfBirth IS NOT NULL THEN p.DateOfBirth ELSE TO_CHAR(TO_DATE(TRIM(p.IncorporationDate), 'dd-MM-yyyy'), 'dd-MM-yyyy') END AS Birth_Incorporation_Date,
# MAGIC cna.CountryAffiliation_list AS NationalAffiliation,
# MAGIC p.PEPStatus AS PEPStatus,
# MAGIC ps.IsUBO,
# MAGIC ps.UBOReason,
# MAGIC ca.address_list AS Addresses,
# MAGIC id.Identification,
# MAGIC pid.IdentificationDocuments
# MAGIC
# MAGIC FROM  Party p
# MAGIC INNER JOIN  Party_Structure  ps 
# MAGIC     on p.PartyIdentifier = ps.PartyParentIdentifier
# MAGIC LEFT OUTER JOIN party_Address ca 
# MAGIC     on p.PartyIdentifier = ca.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_NationalAffiliation cna 
# MAGIC     ON p.PartyIdentifier = cna.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_Identification AS id 
# MAGIC     ON p.PartyIdentifier= id.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_IdentificationDocuments AS pid 
# MAGIC     ON p.PartyIdentifier= pid.PartyIdentifier
# MAGIC LEFT OUTER JOIN Party_SystemIdentifier psi
# MAGIC     ON p.PartyIdentifier=psi.PartyIdentifier AND Application='GCDS'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary view for Parties with the combined query
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Parties AS
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT DISTINCT PartyIdentifier,GCID,PartyType,PartyParentIdentifier as RelatedPartyIdentifier,Birth_Incorporation_Date,FirstName, MiddleName, LastName,FullLegalName, NationalAffiliation, PEPStatus, IsUBO, UBOReason, Addresses, Identification, IdentificationDocuments
# MAGIC   FROM Parties_parent 
# MAGIC   WHERE PartyIdentifier <> PartyParentIdentifier
# MAGIC   UNION
# MAGIC   SELECT DISTINCT PartyIdentifier,GCID,PartyType, PartyChildIdentifier as RelatedPartyIdentifier,Birth_Incorporation_Date,FirstName, MiddleName, LastName, FullLegalName, NationalAffiliation, PEPStatus, IsUBO, UBOReason, Addresses, Identification, IdentificationDocuments
# MAGIC   FROM Parties_child  
# MAGIC   WHERE PartyIdentifier <> PartyChildIdentifier
# MAGIC );

# COMMAND ----------

related_parties_dict_df = spark.table('Parties') \
                    .withColumn('Parties',struct("RelatedPartyIdentifier","GCID","PartyType","Birth_Incorporation_Date","FirstName", "MiddleName", "LastName","FullLegalName","NationalAffiliation","PEPStatus","IsUBO","UBOReason","Addresses","Identification" ,"IdentificationDocuments")).distinct()

related_parties_df = related_parties_dict_df.groupBy('PartyIdentifier') \
                                    .agg(array_sort(collect_set('Parties')).alias('parties_list'))

related_parties_df.createOrReplaceTempView("related_parties")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Final_Data AS
# MAGIC SELECT DISTINCT 
# MAGIC p.PartyIdentifier,
# MAGIC psi.LocalSystemIdentifier as GCID,
# MAGIC p.Party_Type AS PartyType,
# MAGIC CASE WHEN p.Party_Type IN ('Legal Entity','Related Legal Entity') then NULL ELSE p.FirstName END AS FirstName,
# MAGIC CASE WHEN p.Party_Type IN ('Legal Entity','Related Legal Entity') then NULL ELSE p.MiddleName END AS MiddleName,
# MAGIC CASE WHEN p.Party_Type IN ('Legal Entity','Related Legal Entity') then NULL ELSE p.LastName END AS LastName,
# MAGIC p.FullLegalName,
# MAGIC p.CustomerLifeCycleStatus as LifeCycleStatus,
# MAGIC CASE WHEN p.DateOfBirth IS NOT NULL THEN p.DateOfBirth ELSE TO_DATE(TRIM(p.IncorporationDate), 'dd-MM-yyyy') END AS Birth_Incorporation_Date,
# MAGIC cna.CountryAffiliation_list AS NationalAffiliation,
# MAGIC oc.OwnershipCoverage_list AS ClientOwnerList,
# MAGIC p.PEPStatus,
# MAGIC p.FIHubIndicator,
# MAGIC d.address_list AS Addresses,
# MAGIC id.Identification,
# MAGIC pid.IdentificationDocuments,
# MAGIC e.naics_list AS NAICS,
# MAGIC g.Relationships AS Relationships,
# MAGIC "null" as CommentsOnStructure,
# MAGIC h.parties_list AS Parties
# MAGIC
# MAGIC FROM  Party AS p
# MAGIC LEFT OUTER JOIN Party_Structure AS ps
# MAGIC     ON ps.PartyIdentifier = p.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_NationalAffiliation AS cna 
# MAGIC     ON p.PartyIdentifier = cna.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_OwnershipCoverage oc 
# MAGIC     ON oc.PartyIdentifier=p.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_Address AS d
# MAGIC     ON d.PartyIdentifier = p.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_Naics AS e 
# MAGIC     ON e.PartyIdentifier = p.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_Relationship AS g 
# MAGIC     ON g.PartyIdentifier = p.PartyIdentifier
# MAGIC LEFT OUTER JOIN related_parties AS h 
# MAGIC     ON h.PartyIdentifier=p.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_Identification id 
# MAGIC     ON p.PartyIdentifier= id.PartyIdentifier
# MAGIC LEFT OUTER JOIN party_IdentificationDocuments AS pid 
# MAGIC     ON p.PartyIdentifier= pid.PartyIdentifier
# MAGIC LEFT OUTER JOIN Party_SystemIdentifier psi
# MAGIC     ON p.PartyIdentifier=psi.PartyIdentifier and Application='GCDS'
# MAGIC --WHERE p.IsLatestApprovedVersionOfClient='True';

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Data

# COMMAND ----------

# Load data from Databricks
final_data=spark.table("final_data")
window_spec = Window.orderBy("PartyIdentifier")
# final_data = final_data.withColumn("id",row_number().over(window_spec).cast("string"))
final_data = final_data.withColumn("id",col('PartyIdentifier').cast("string"))
final_data = final_data.repartition(240)
final_data.persist(StorageLevel.DISK_ONLY)

# COMMAND ----------

from functools import reduce

yesterday_df=spark.table("radar.cosmos_rdm_data").withColumn("id", col("PartyIdentifier"))
today_df=final_data

#new records which are not present in the yesterday's data
new_records_df=today_df.join(yesterday_df, on='PartyIdentifier',how='left_anti')

#deleted records
deleted_records_df=yesterday_df.join(today_df, on='PartyIdentifier',how='left_anti')
deleted_PartyIdentifier_list= [row['PartyIdentifier'] for row in deleted_records_df.collect()]

#updated/modified records
joined_df=today_df.alias("t").join(yesterday_df.alias("y"), on='PartyIdentifier',how="inner")

cols_to_compare=[col for col in today_df.columns if col!='PartyIdentifier']

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
from decimal import Decimal

# Cosmos DB configuration
CosmosDatabaseEndpoint = f"https://{CosmosAccount}.documents.azure.com:443/"
CosmosDatabaseName = 'CosmosDocDB'
CosmosContainerName = 'RDMClientData'

# Authenticate using SPN
credential = ClientSecretCredential(TenantId, app_reg_app_id, service_credential)

# Initialize the Cosmos client with SPN
client = CosmosClient(CosmosDatabaseEndpoint, credential)

# Create database and container
database = client.create_database_if_not_exists(CosmosDatabaseName)
container = database.create_container_if_not_exists(
    id=CosmosContainerName,
    partition_key=PartitionKey(path="/PartyIdentifier"),
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
print("total_records :", total_records) 

# Loop to Upsert records
for row in upsert_records_df.collect():
    doc = row.asDict(recursive=True)
    container.upsert_item(doc)

# Loop to delete records 
for PartyIdentifier in deleted_PartyIdentifier_list:
    try:
        container.delete_item(item=PartyIdentifier, partition_key=PartyIdentifier)
    except Exception as e:
        print(f"Failed to delete PartyIdentifier {PartyIdentifier}:{str(e)}")

# write_batch(final_data, CosmosConfig)
print("written all batches successfully")

# COMMAND ----------

# DBTITLE 1,FIRST LOAD
# from azure.identity import ClientSecretCredential
# from azure.cosmos import CosmosClient, PartitionKey
# from pyspark.sql import SparkSession

# # Cosmos DB configuration
# CosmosDatabaseEndpoint = f"https://{CosmosAccount}.documents.azure.com:443/"
# CosmosDatabaseName = 'CosmosDocDB'
# CosmosContainerName = 'RDMClientData'

# # Authenticate using SPN
# credential = ClientSecretCredential(TenantId, app_reg_app_id, service_credential)

# # Initialize the Cosmos client with SPN
# client = CosmosClient(CosmosDatabaseEndpoint, credential)

# # Create database and container
# database = client.create_database_if_not_exists(CosmosDatabaseName)
# container = database.create_container_if_not_exists(
#     id=CosmosContainerName,
#     partition_key=PartitionKey(path="/PartyIdentifier"),
#     offer_throughput=5000
# )

# # Cosmos DB configurations for Spark
# CosmosConfig = {
#     "spark.cosmos.accountEndpoint": CosmosDatabaseEndpoint,
#     "spark.cosmos.auth.type": "ServicePrincipal",
#     "spark.cosmos.account.subscriptionId": subscriptionId,
#     "spark.cosmos.account.tenantId": TenantId,
#     "spark.cosmos.auth.aad.clientId": app_reg_app_id,
#     "spark.cosmos.auth.aad.clientSecret": service_credential,
#     "spark.cosmos.database": CosmosDatabaseName,
#     "spark.cosmos.container": CosmosContainerName,
#     "spark.cosmos.write.strategy": "ItemOverwrite",
#     "spark.cosmos.read.inferSchema.enabled": "true",
#     "spark.cosmos.write.bulk.enabled": "false",
#     "spark.cosmos.write.bulk.maxPendingOperations": "1000",
#     "spark.cosmos.account.resourceGroupName": ResourceGroupName
# }

# # Setting Spark configurations
# spark = SparkSession.builder.appName("CosmosDBIntegration").getOrCreate()
# spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
# spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.auth.type", "ServicePrincipal")
# spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.account.subscriptionId", subscriptionId)
# spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.account.tenantId", TenantId)
# spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.auth.aad.clientId", app_reg_app_id)
# spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.auth.aad.clientSecret", service_credential)

# total_records = final_data.count()
# print("total_records :", total_records)  #37685

# final_data.write.format("cosmos.oltp").options(**CosmosConfig).mode("append").save()

# COMMAND ----------

# DBTITLE 1,Create table
final_data.write.mode('overwrite').saveAsTable("radar.cosmos_rdm_data")
