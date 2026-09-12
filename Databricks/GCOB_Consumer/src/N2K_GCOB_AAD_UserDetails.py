# Databricks notebook source
# DBTITLE 1,import libraries
from pyspark.sql import SparkSession
import os
from datetime import datetime, timedelta
import http.client
import json
import requests
import sys
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, LongType
import pyspark.sql.functions as F
from pyspark.sql.functions import col, explode

# Local File Import
from GcobUtils import (
    write_to_unity_catalog,
    authenticate_storage_account,
    read_gdp_defined_dataobjects,
    get_group_members_df,
    service_credential,
)

catalog = os.environ["CATALOG"]
schema = os.environ["GCOB_UC_SCHEMA"]

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

date_parameter = datetime.today().strftime("%Y%m%d")
load_dts = "EDL_LOAD_DTS=" + date_parameter + "*"
BusinessDate = (datetime.today() - timedelta(1)).strftime("%m/%d/%Y")

# COMMAND ----------

N2k_UserAttribute_dataobject = "N2k_UserAttribute"
N2K_ClientAttribute_dataobject = "N2K_ClientAttribute"

# COMMAND ----------

# DBTITLE 1,import variables
app_reg_app_id = os.environ["APP_REG_APP_ID"]
ReadStorage = os.environ["GDP_STORAGE_NAME"]
TenantId = os.environ["TENANT_ID"]

# COMMAND ----------

ReadStorage = os.environ["GDP_STORAGE_NAME"]
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# DBTITLE 1,set AMS variables
env = "pre-prod"
graph_scope = "https://graph.microsoft.com/.default"
graph_base_url = "graph.microsoft.com"

if env == "pre-prod" or env == "prd":
    # AMS
    scope = "b464faba-cd8b-4ba9-9765-db19a0e5f790/.default"
    ams_base_url = "https://713011d5-f99d-45d9-9b6c-f2d7c0c194b9.az-eu.api.rabo.cloud"
    ams_url = "713011d5-f99d-45d9-9b6c-f2d7c0c194b9.az-eu.api.rabo.cloud"

    # GRAPH

elif env == "dev":
    # bearer token AMS
    scope = "d2477ba1-a335-4cca-bc27-3e481bca3eb6/.default"
    ams_base_url = (
        "https://1b68e6f5-ed25-4e07-adbd-46d4b6a5c57c-nonprd.t-az-eu.api.rabo.cloud"
    )
    ams_url = "1b68e6f5-ed25-4e07-adbd-46d4b6a5c57c-nonprd.t-az-eu.api.rabo.cloud"



# COMMAND ----------

# MAGIC %md
# MAGIC ### AMS via Client-Credentials grant
# MAGIC

# COMMAND ----------

# DBTITLE 1,Get Token to authenticate to AMS
# get ams api token from generic provides

# scope
# scope = 'd2477ba1-a335-4cca-bc27-3e481bca3eb6/.default'
# scope_prd https://713011d5-f99d-45d9-9b6c-f2d7c0c194b9.az-eu.api.rabo.cloud
# scope_metadataapi = https://wapp-edl-cap-metadata-api-westeurope-test/.default'
# prod: b464faba-cd8b-4ba9-9765-db19a0e5f790/.default
# test: d2477ba1-a335-4cca-bc27-3e481bca3eb6/.default
# uat: 9107156f-399b-4cd8-8cb9-83efaa3c1cd6/.default

# Authentication
headers = {
    "Content_Type": "application/x-www-form-urlencoded",
    "cache-control": "no-cache",
}
# Token auth value
values = {
    "grant_type": "client_credentials",
    "scope": f"{scope}",
    # Use client ID of your DB instance (Step 2)
    "client_id": f"{app_reg_app_id}",
    # Retreive secret from keyvault
    "client_secret": f"{service_credential}",
}

# Use https request to generate auth token
resp = requests.post(
    url="https://login.microsoftonline.com/6e93a626-8aca-4dc1-9191-ce291b4b75a1/oauth2/v2.0/token",
    headers=headers,
    data=values,
)
#
print("Auth response: (should be 200)")
print(resp)

# COMMAND ----------

# DBTITLE 1,Get API response from central
# call ams api with toke
# CI4124929 = GCOB
token = json.loads(resp.text)["access_token"]
header_info = {"Authorization": f"Bearer {token}"}

res = requests.get(
    f"{ams_base_url}/api/GetUserRoles/CI4124929?limit=2500", headers=header_info
)

# currently forbidden to access azure app gateway

# COMMAND ----------

# DBTITLE 1,attempt using http library instead of request (like the Metadata API?)
relative_url = "/api/GetUserRoles/CI4124929?limit=2500"  # RoleId=='COB'|| RoleId=='GC-FLM'|| RoleId=='GC-AUD'|| RoleId=='GC-COM'|| RoleId=='GC-DS'|| RoleId=='GC-COS'|| RoleId=='GCEO'

connection = http.client.HTTPSConnection(ams_url)
connection.request(method="GET", url=relative_url, headers=header_info)
result = connection.getresponse()
# print (f'body={json.dumps(param_values)}')

# COMMAND ----------

print(result)

# COMMAND ----------

# DBTITLE 1,AMS API DATA
All_AMS_df = pd.DataFrame()

# Validate and parse AMS response
try:
    ams_resp_json = json.loads(result.read())
except json.JSONDecodeError as e:
    raise ValueError(
f"Failed to parse AMS API JSON response: {e.msg}"
) from e

# Validate response structure
if "user_details" not in ams_resp_json:
    raise ValueError("AMS API response missing 'user_details' key")

if not isinstance(ams_resp_json["user_details"], list):
    raise ValueError("AMS API 'user_details' is not a list")
# print(ams_resp_json)
AMSreqdf = pd.DataFrame.from_dict(ams_resp_json["user_details"], orient="columns")
df_spark = spark.createDataFrame(AMSreqdf)
# display(df_spark)
df_ams = df_spark.select(
    col("user_id"),
    col("upn"),
    explode("roles").alias("rolesdetails"),
    col("rolesdetails.id"),
    col("rolesdetails.role_id"),
    col("rolesdetails.context"),
    col("rolesdetails.context.client"),
).drop("rolesdetails")
df_ams.createOrReplaceTempView("AMS_API")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Graph API

# COMMAND ----------

# DBTITLE 1,get token for calling Graph API
# scope for which we are requesting a token
scope = "https://graph.microsoft.com/.default"

# Authentication
headers = {
    "Content_Type": "application/x-www-form-urlencoded",
    "cache-control": "no-cache",
}
# Token auth value
values = {
    "grant_type": "client_credentials",
    "scope": f"{scope}",
    "client_id": f"{app_reg_app_id}",
    "client_secret": f"{service_credential}",
}

# Use https request to generate auth token
resp = requests.post(
    url="https://login.microsoftonline.com/6e93a626-8aca-4dc1-9191-ce291b4b75a1/oauth2/v2.0/token",
    headers=headers,
    data=values,
)

# call graph api
token = json.loads(resp.text)["access_token"]
header_info = {
    "Authorization": f"Bearer {token}",
}

url = "graph.microsoft.com"

# COMMAND ----------

allgcobgroups = requests.get(
    "https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName,'eu.aut.AADGCOBPRDRoleClientOwnerSupport.us') or startswith(displayName,'eu.aut.AADGCOBPRDLocation') or startswith(displayName,'eu.aut.AADGCOBPRDGlobalFIHub.us')",
    headers=header_info,
)  # ?$search="Name:eu.aut.AADGCOB"
try:
    GCOBLocationGroups = allgcobgroups.json()
    if "value" not in GCOBLocationGroups:
        raise ValueError("Response missing 'value' key")
except json.JSONDecodeError as e:
    raise ValueError(f"Failed to parse AMS API JSON response: {e.msg}") from e

# COMMAND ----------

df_gcob_loc_group_users = get_group_members_df(
    spark=spark, groups=GCOBLocationGroups, headers=header_info
)

# COMMAND ----------

df_gcob_loc_group_users.createOrReplaceTempView("AllGroupUsers")

# COMMAND ----------

schema_aad_mapping = StructType(
    [
        StructField("ExtractedLocationStringFromGCOB", StringType(), True),
        StructField("RabobankEntityGcobId", StringType(), True),
        StructField("GcobLocationName", StringType(), True),
    ]
)

gcob_aad_mapping = [
    {
        "ExtractedLocationStringFromGCOB": "Foundation",
        "RabobankEntityGcobId": 28,
        "GcobLocationName": "Rabobank Foundation",
    },
    {
        "ExtractedLocationStringFromGCOB": "RANZZROZ",
        "RabobankEntityGcobId": 32,
        "GcobLocationName": "Rabobank - RANZ Country Banking and ROS",
    },
    {
        "ExtractedLocationStringFromGCOB": "CanadaRural",
        "RabobankEntityGcobId": 29,
        "GcobLocationName": "Rabobank Canada(Rural)",
    },
    {
        "ExtractedLocationStringFromGCOB": "RANZROZ",
        "RabobankEntityGcobId": 32,
        "GcobLocationName": "Rabobank - RANZ Country Banking and ROS",
    },
    {
        "ExtractedLocationStringFromGCOB": "SAF",
        "RabobankEntityGcobId": 31,
        "GcobLocationName": "Rabobank - Smallholder Agroforestry Finance (SAF)",
    },
    {
        "ExtractedLocationStringFromGCOB": "Milan",
        "RabobankEntityGcobId": 15,
        "GcobLocationName": "Rabobank Milan",
    },
    {
        "ExtractedLocationStringFromGCOB": "NewZealand",
        "RabobankEntityGcobId": 20,
        "GcobLocationName": "Rabobank New Zealand",
    },
    {
        "ExtractedLocationStringFromGCOB": "Turkey",
        "RabobankEntityGcobId": 23,
        "GcobLocationName": "Rabobank Turkey",
    },
    {
        "ExtractedLocationStringFromGCOB": "Netherlands",
        "RabobankEntityGcobId": 19,
        "GcobLocationName": "Rabobank Netherlands",
    },
    {
        "ExtractedLocationStringFromGCOB": "Antwerp",
        "RabobankEntityGcobId": 3,
        "GcobLocationName": "Rabobank Antwerp",
    },
    {
        "ExtractedLocationStringFromGCOB": "Indonesia",
        "RabobankEntityGcobId": 13,
        "GcobLocationName": "Rabobank Indonesia",
    },
    {
        "ExtractedLocationStringFromGCOB": "HongKong",
        "RabobankEntityGcobId": 11,
        "GcobLocationName": "Rabobank Hong Kong",
    },
    {
        "ExtractedLocationStringFromGCOB": "London",
        "RabobankEntityGcobId": 24,
        "GcobLocationName": "Rabobank London",
    },
    {
        "ExtractedLocationStringFromGCOB": "Paris",
        "RabobankEntityGcobId": 9,
        "GcobLocationName": "Rabobank Paris",
    },
    {
        "ExtractedLocationStringFromGCOB": "Kenya",
        "RabobankEntityGcobId": 16,
        "GcobLocationName": "Rabobank Kenya",
    },
    {
        "ExtractedLocationStringFromGCOB": "Frankfurt",
        "RabobankEntityGcobId": 10,
        "GcobLocationName": "Rabobank Frankfurt",
    },
    {
        "ExtractedLocationStringFromGCOB": "Australia",
        "RabobankEntityGcobId": 2,
        "GcobLocationName": "Rabobank Australia",
    },
    {
        "ExtractedLocationStringFromGCOB": "Malaysia",
        "RabobankEntityGcobId": 17,
        "GcobLocationName": "Rabobank Malaysia",
    },
    {
        "ExtractedLocationStringFromGCOB": "Canada",
        "RabobankEntityGcobId": 5,
        "GcobLocationName": "Rabobank Canada (RCBR)",
    },
    {
        "ExtractedLocationStringFromGCOB": "",
        "RabobankEntityGcobId": 30,
        "GcobLocationName": "Global FI Hub",
    },
    {
        "ExtractedLocationStringFromGCOB": "China",
        "RabobankEntityGcobId": 8,
        "GcobLocationName": "Rabobank China",
    },
    {
        "ExtractedLocationStringFromGCOB": "India",
        "RabobankEntityGcobId": 12,
        "GcobLocationName": "Rabobank India",
    },
    {
        "ExtractedLocationStringFromGCOB": "Singapore",
        "RabobankEntityGcobId": 21,
        "GcobLocationName": "Rabobank Singapore",
    },
    {
        "ExtractedLocationStringFromGCOB": "AgriFinance",
        "RabobankEntityGcobId": 27,
        "GcobLocationName": "Rabobank - USA Rabo AgriFinance",
    },
    {
        "ExtractedLocationStringFromGCOB": "Madrid",
        "RabobankEntityGcobId": 22,
        "GcobLocationName": "Rabobank Madrid",
    },
    {
        "ExtractedLocationStringFromGCOB": "Argentina",
        "RabobankEntityGcobId": 1,
        "GcobLocationName": "Rabobank Argentina",
    },
    {
        "ExtractedLocationStringFromGCOB": "NewYork",
        "RabobankEntityGcobId": 25,
        "GcobLocationName": "Rabobank New York",
    },
    {
        "ExtractedLocationStringFromGCOB": "SecuritiesCanada",
        "RabobankEntityGcobId": 6,
        "GcobLocationName": "Rabo Securities Canada, Inc. (RSCI)",
    },
    {
        "ExtractedLocationStringFromGCOB": "SecuritiesUSA",
        "RabobankEntityGcobId": 26,
        "GcobLocationName": "Rabo Securities USA, Inc. (RSEC)",
    },
    {
        "ExtractedLocationStringFromGCOB": "Chile",
        "RabobankEntityGcobId": 7,
        "GcobLocationName": "Rabobank Chile",
    },
    {
        "ExtractedLocationStringFromGCOB": "Dublin",
        "RabobankEntityGcobId": 14,
        "GcobLocationName": "Rabobank Dublin",
    },
]
# create dataframe for gcob_aad_mapping
df_gcob_aad_mapping = spark.createDataFrame(gcob_aad_mapping, schema=schema_aad_mapping)

# COMMAND ----------

df_gcob_aad_mapping.createOrReplaceTempView("gcob_aad_mapping")

# COMMAND ----------


gcob_loc_users_enriched = df_gcob_loc_group_users.withColumn(
    "LocationCode",
    F.regexp_extract(F.col("GroupName"), r"(?<=Location)(.*)(?=\.us)", 0),
)
df_filtered_gcob_loc = df_gcob_loc_group_users.filter(
    "GroupName == 'eu.aut.AADGCOBPRDGlobalFIHub.us'"
).select(
    F.lit("FI").cast(StringType()).alias("RabobankEntityGcobId"),
    F.col("userPrincipalName"),
    F.lit("FI").alias("GcobLocationName"),
)
df_UserLocationAttribute = (
    (
        df_gcob_aad_mapping.alias("t1").join(
            gcob_loc_users_enriched.alias("t2"),
            F.col("t1.ExtractedLocationStringFromGCOB") == F.col("t2.LocationCode"),
            "left",
        )
    )
    .select(
        df_gcob_aad_mapping.RabobankEntityGcobId,
        gcob_loc_users_enriched.userPrincipalName,
        df_gcob_aad_mapping.GcobLocationName,
    )
    .unionByName(df_filtered_gcob_loc)
)

# COMMAND ----------

df_UserLocationAttribute.createOrReplaceTempView("Location_UserAttribute")

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view AMS_ALL As
# MAGIC select distinct
# MAGIC   ams_api.user_id,
# MAGIC   ams_api.Upn,
# MAGIC   ams_api.Role_Id,
# MAGIC   ams_api.client as GCDSID,
# MAGIC   t2.KeyStore_value AS GCOBID,
# MAGIC   case
# MAGIC     when pccd.clienttype is null then pcd.clienttype
# MAGIC     else pccd.clienttype
# MAGIC   end as clienttype
# MAGIC from
# MAGIC   AMS_API as ams_api
# MAGIC     left join (
# MAGIC       select
# MAGIC         *
# MAGIC       from
# MAGIC         global_temp.client_KeyStoreKey
# MAGIC       where
# MAGIC         KeyStore_type = 'GCOBID'
# MAGIC     ) t2
# MAGIC       on ams_api.client = t2.GCID
# MAGIC     left join global_temp.party_case_client_details pccd
# MAGIC       on t2.GCID = pccd.gcdsid
# MAGIC     left join global_temp.party_case_client_details pcd
# MAGIC       on t2.KeyStore_value = pcd.gcobid
# MAGIC where
# MAGIC   ams_api.Role_Id = 'GCEO'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW AllCosUsers AS
# MAGIC SELECT DISTINCT
# MAGIC   mail,
# MAGIC   userPrincipalName
# MAGIC FROM
# MAGIC   AllGroupUsers
# MAGIC WHERE
# MAGIC   GroupName = 'eu.aut.AADGCOBPRDRoleClientOwnerSupport.us'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcobcsrole AS
# MAGIC Select distinct
# MAGIC   ag.userPrincipalName AS `UPN`,
# MAGIC   `Role Type Code` AS `AttributeType`,
# MAGIC   SupportedClientOwnerMailAdress AS `AttributeValue`
# MAGIC FROM
# MAGIC   global_temp.party_GcobUsers gu
# MAGIC     left join AllCosUsers ag
# MAGIC       on gu.UserMailAdress = ag.mail
# MAGIC where
# MAGIC   ag.userPrincipalName is not null

# COMMAND ----------

# For Both Fecradar and AADRadar group
AADRadargrp = requests.get(
    "https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName,'eu.aut.AADRadar')",
    headers=header_info,
)  # ?$search="Name:eu.aut.AADGCOB"
# RegionEurope
# eu.aut.AADRadar
# eu.aut.AADRadarRegionEurope 9c87aec0-9bd3-418a-bc4c-836584b7e558
if AADRadargrp.status_code != 200:
    raise ValueError(
        f"Failed to fetch FECRadar groups. HTTP status: {AADRadargrp.status_code}"
    )
try:
    AADRadarGroups = AADRadargrp.json()

    if "value" not in AADRadarGroups:
        raise ValueError("AADRadar response missing 'value' key")

except json.JSONDecodeError as ex:
    raise ValueError("Failed to parse AADRadar JSON response") from ex

####
FecRadargrp = requests.get(
    "https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName,'eu.aut.AADFECradar')",
    headers=header_info,
)  # ?$search="Name:eu.aut.AADGCOB"

if FecRadargrp.status_code != 200:
    raise ValueError(
        f"Failed to fetch FECRadar groups. HTTP status: {FecRadargrp.status_code}"
    )
try:
    FecRadarGroups = FecRadargrp.json()

    if "value" not in FecRadarGroups:
        raise ValueError("FECRadar response missing 'value' key")

except json.JSONDecodeError as ex:
    raise ValueError("Failed to parse FECRadar JSON response") from ex

# COMMAND ----------

# For AADRadar gruops and related information
df_AllAADRadarGroupUsers = get_group_members_df(
    spark=spark, groups=AADRadarGroups, headers=header_info
)
# -- For FECRadar gruops and related information
df_AllFecAADRadarGroupUsers = get_group_members_df(
    spark=spark, groups=FecRadarGroups, headers=header_info
)

# COMMAND ----------

df_AllAADRadarGroupUsers.createOrReplaceTempView("AllAADRadarGroupUsers")
df_AllFecAADRadarGroupUsers.createOrReplaceTempView("AllFecDRadarGroupUsers")

# COMMAND ----------

# DBTITLE 1,api call for user location n2k for users in oneeurope aad group - n2k location based!!
# ===== UPDATED CODE - With validation for Checkmarx =====
# Maximum number of pagination pages to prevent DoS attacks
MAX_PAGINATION_PAGES = 1000

df_oneeurope_users = pd.DataFrame()

url = "https://graph.microsoft.com/v1.0/groups/9fe5dc45-cc96-43b5-851f-d400dbcc9a0b/members?$count=true"

# Fetch users from oneeurope aad group with validation
response = requests.get(url=url, headers=header_info)

if response.status_code != 200:
    raise RuntimeError(f"Failed to fetch OneEurope group members: Status {response.status_code}, Response: {response.text}"
)

try:
    oneeurope_user_list = response.json()
    if "value" not in oneeurope_user_list:
        raise ValueError("OneEurope response missing 'value' key")
except json.JSONDecodeError as e:
    raise ValueError(f"Failed to parse OneEurope JSON response: {e.msg}") from e

for user in oneeurope_user_list["value"]:
    # Validate user structure
    if not isinstance(user, dict) or "id" not in user:
        print(f"Skipping invalid OneEurope user entry: {user}")
        continue

    id = user["id"]
    username = user.get("displayName", "Unknown")

    url = f"https://graph.microsoft.com/v1.0/users/{id}?$select=displayName,givenName,userPrincipalName,companyName,country"

    # Add iteration counter to prevent infinite loops
    page_count = 0

    while url and page_count < MAX_PAGINATION_PAGES:
        page_count += 1
        try:
            user_response = requests.get(url=url, headers=header_info)

            # Check HTTP status
            if user_response.status_code != 200:
                print(
                    f"Failed to get details for user {username}: Status {user_response.status_code}"
                )
                break

            oneeurope_user_response = user_response.json()
            df_oneeurope_users = pd.concat(
                [pd.DataFrame([oneeurope_user_response]), df_oneeurope_users],
                ignore_index=True,
            )

            # Check for next page
            url = oneeurope_user_response.get("@odata.nextLink", None)

        except json.JSONDecodeError as e:
            print(f"JSON decode error for user {username}: {str(e)}")
            break
        except Exception as e:
            print(f"Error processing user {username}: {str(e)}")
            break

    # Log if max pages reached
    if page_count >= MAX_PAGINATION_PAGES:
        print(
            f"Warning: Reached maximum pagination limit ({MAX_PAGINATION_PAGES}) for user {username}"
        )

# COMMAND ----------

# DBTITLE 1,create oneeurope_users_raw tempview for n2k based on location
from pyspark.sql.types import StructType, StructField, StringType

# handle empty df
if df_oneeurope_users.empty:
    schema = StructType(
        [
            StructField("userPrincipalName", StringType(), True),
            StructField("companyName", StringType(), True),
        ]
    )
    df_oneeurope_users_list = spark.createDataFrame([], schema)
else:
    df_oneeurope_users_list = spark.createDataFrame(df_oneeurope_users)

df_oneeurope_users_list.createOrReplaceTempView("oneeurope_users_raw")

# COMMAND ----------

# DBTITLE 1,oneeurope_users
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW oneeurope_users AS
# MAGIC SELECT DISTINCT
# MAGIC   userPrincipalName AS UPN,
# MAGIC   'LocationOneEurope' AS AttributeType,
# MAGIC   CASE
# MAGIC     WHEN companyName = 'Coöperatieve Rabobank U.A.' THEN 'Rabobank Netherlands'
# MAGIC     WHEN companyName = 'Rabobank London' THEN 'Rabobank London'
# MAGIC     WHEN companyName = 'Rabobank Paris' THEN 'Rabobank Paris'
# MAGIC     WHEN companyName = 'Rabobank Frankfurt' THEN 'Rabobank Frankfurt'
# MAGIC     WHEN companyName = 'Rabobank Antwerp' THEN 'Rabobank Antwerp'
# MAGIC     WHEN companyName = 'Rabobank Kenya' THEN 'Rabobank Kenya'
# MAGIC     WHEN companyName = 'Rabobank Dublin' THEN 'Rabobank Dublin'
# MAGIC     WHEN companyName = 'Rabobank Madrid' THEN 'Rabobank Madrid'
# MAGIC     WHEN companyName = 'Rabobank Milan' THEN 'Rabobank Milan'
# MAGIC     WHEN companyName = 'Rabobank Turkey' THEN 'Rabobank Turkey'
# MAGIC     ELSE
# MAGIC       NULL
# MAGIC   END AS AttributeValue
# MAGIC FROM
# MAGIC   oneeurope_users_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view ALL_Fec_Radar_GroupInfo As
# MAGIC select
# MAGIC   userprincipalname,
# MAGIC   GroupName
# MAGIC from
# MAGIC   AllFecDRadarGroupUsers
# MAGIC union all
# MAGIC select
# MAGIC   userprincipalname,
# MAGIC   GroupName
# MAGIC from
# MAGIC   AllAADRadarGroupUsers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Region_UserInfo AS
# MAGIC SELECT DISTINCT
# MAGIC   userprincipalname AS UPN,
# MAGIC   'PortfolioRegion' AS AttributeType,
# MAGIC   CASE
# MAGIC     WHEN GroupName = 'eu.aut.AADRadarRegionAsia.us' THEN 'RegionAsia'
# MAGIC     WHEN GroupName = 'eu.aut.AADRadarRegionEuropeAfrica.us' THEN 'RegionEA'
# MAGIC     WHEN GroupName = 'eu.aut.AADRadarRegionNA.us' THEN 'RegionNA'
# MAGIC     WHEN GroupName = 'eu.aut.AADRadarRegionRANZ.us' THEN 'RegionRANZ'
# MAGIC     WHEN GroupName = 'eu.aut.AADRadarRegionSA.us' THEN 'RegionSA'
# MAGIC     WHEN GroupName = 'eu.aut.AADRadarCashMTOInsights.us' THEN 'MTO'
# MAGIC     WHEN GroupName = 'eu.aut.AADFECRadarMTODashboardEUA.us' THEN 'MTO'
# MAGIC     WHEN GroupName = 'eu.aut.AADRadarRegionFoundation.us' THEN 'RegionFoundation'
# MAGIC     WHEN GroupName = 'eu.aut.AADRadarDepartmentTCF.us' THEN 'DepartmentTCF'
# MAGIC     WHEN GroupName = 'eu.aut.AADGCOBPRDGlobalFIHub.us' THEN 'FIHub'
# MAGIC     WHEN GroupName = 'Eu.aut.AADRadarRegionGlobalFI.us' THEN 'FIHub'
# MAGIC     ELSE NULL
# MAGIC   END AS AttributeValue
# MAGIC FROM
# MAGIC   ALL_Fec_Radar_GroupInfo
# MAGIC WHERE
# MAGIC   GroupName IN (
# MAGIC     'eu.aut.AADRadarRegionAsia.us',
# MAGIC     'eu.aut.AADRadarRegionEuropeAfrica.us',
# MAGIC     'eu.aut.AADRadarRegionNA.us',
# MAGIC     'eu.aut.AADRadarRegionRANZ.us',
# MAGIC     'eu.aut.AADRadarRegionSA.us',
# MAGIC     'eu.aut.AADRadarCashMTOInsights.us',
# MAGIC     'eu.aut.AADRadarRegionFoundation.us',
# MAGIC     'eu.aut.AADFECRadarMTODashboardEUA.us',
# MAGIC     'eu.aut.AADRadarDepartmentTCF.us',
# MAGIC     'eu.aut.AADGCOBPRDGlobalFIHub.us',
# MAGIC     'Eu.aut.AADRadarRegionGlobalFI.us'
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,Hardcoded Retail-NL users for MTO
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RETAIL_NL_MTO AS
# MAGIC SELECT
# MAGIC   'Priscilla.Profijt@rabobank.nl' AS UPN,
# MAGIC   'RetailNLMTO' AS AttributeType,
# MAGIC   'MTO' AS AttributeValue
# MAGIC UNION
# MAGIC SELECT
# MAGIC   'Bianca.M.Gobbens@rabobank.nl' AS UPN,
# MAGIC   'RetailNLMTO' AS AttributeType,
# MAGIC   'MTO' AS AttributeValue
# MAGIC -- These 2 persons are Retail NL persons who need to use the MTO dashboard for review
# MAGIC -- added by Ruud.van.laar 2025-04-29

# COMMAND ----------

# DBTITLE 1,N2KUserAttribute
df_UserAttribute = spark.sql("""
    select DISTINCT
    UserPrincipalName AS `UPN`
    , 'GcobLocation' AS `AttributeType`
    , GcobLocationName AS `AttributeValue`
    FROM
    Location_UserAttribute
 
    UNION
 
   select distinct
    Upn as UPN
    , role_id AS `AttributeType`
    , Upn AS AttributeValue
    FROM AMS_ALL
 
    UNION

    Select distinct
     UPN AS `UPN`
    ,AttributeType AS `AttributeType`
    , AttributeValue AS `AttributeValue`

    FROM
    gcobcsrole

    --- Adding Region Data
    union

    select Distinct
        UPN
        , AttributeType
        , Attributevalue
    from Region_UserInfo

    -- N2K based on location for One Europe users
    UNION

    SELECT DISTINCT
        UPN
        , AttributeType
        , AttributeValue
    FROM oneeurope_users
    
    union
    select DISTINCT
     UPN
    , AttributeType
    , AttributeValue
    FROM RETAIL_NL_MTO
""")
df_UserAttribute.createOrReplaceTempView("N2k_UserAttribute")

# COMMAND ----------

# DBTITLE 1,N2KClientAttribute
# MAGIC %sql
# MAGIC /*
# MAGIC   FOR THE FUTURE: align definition of UniqueGcobId among views - eg. LE_ + GcobId everywhere
# MAGIC */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW N2K_ClientAttribute AS
# MAGIC SELECT
# MAGIC cast(GcobId as string) as GcobId,
# MAGIC AttributeValue,
# MAGIC N2KReason,
# MAGIC UniqueGcobId
# MAGIC from 
# MAGIC (select distinct
# MAGIC   cast(GcobId as STRING) as GCOBid,
# MAGIC   GlobalClientOwnerLocation AS AttributeValue,
# MAGIC   'GCO Location' AS `N2KReason`,
# MAGIC   case
# MAGIC     when pc.clienttype = 'Legal Entity' then concat('LE_', GcobId)
# MAGIC     when
# MAGIC       pc.clienttype in ('Natural Person acting in a Professional Capacity (NPPC)', 'Natural Person')
# MAGIC     then
# MAGIC       concat('NP_NPPC_', GcobId)
# MAGIC     else 'NA'
# MAGIC   end as UniqueGcobId
# MAGIC FROM
# MAGIC   global_temp.party_case_client_details pc --done
# MAGIC WHERE
# MAGIC   pc.casestatusname != 'Cancelled'
# MAGIC UNION
# MAGIC select distinct
# MAGIC   cast(t1.GcobId as STRING) as GCOBid,
# MAGIC   t2.ProductOfferingLocation AS AttributeValue,
# MAGIC   'ProductLocation' AS N2KReason,
# MAGIC   case
# MAGIC     when t1.clienttype = 'Legal Entity' then concat('LE_', t1.GcobId)
# MAGIC     when
# MAGIC       t1.clienttype in ('Natural Person acting in a Professional Capacity (NPPC)', 'Natural Person')
# MAGIC     then
# MAGIC       concat('NP_NPPC_', t1.GcobId)
# MAGIC     else 'NA'
# MAGIC   end as UniqueGcobId
# MAGIC from
# MAGIC   global_temp.party_case_client_details t1
# MAGIC     LEFT JOIN global_temp.party_products_and_services t2
# MAGIC       on t1.SourceClient = t2.SourceClient
# MAGIC WHERE
# MAGIC   t2.ProductLifecyclestatus = 'Active'
# MAGIC   and t1.casestatusname != 'Cancelled' --done
# MAGIC UNION
# MAGIC select distinct
# MAGIC   cast(t1.GcobId as STRING) as GCOBid,
# MAGIC   t2.BookingEntityLocation AS AttributeValue,
# MAGIC   'BookingLocation' AS N2KReason,
# MAGIC   case
# MAGIC     when t1.clienttype = 'Legal Entity' then concat('LE_', t1.GcobId)
# MAGIC     when
# MAGIC       t1.clienttype in ('Natural Person acting in a Professional Capacity (NPPC)', 'Natural Person')
# MAGIC     then
# MAGIC       concat('NP_NPPC_', t1.GcobId)
# MAGIC     else 'NA'
# MAGIC   end as UniqueGcobId
# MAGIC from
# MAGIC   global_temp.party_case_client_details t1
# MAGIC     LEFT JOIN global_temp.party_products_and_services t2
# MAGIC       on t1.SourceClient = t2.SourceClient
# MAGIC WHERE
# MAGIC   t2.ProductLifecyclestatus = 'Active'
# MAGIC   and t1.casestatusname != 'Cancelled' --done
# MAGIC UNION
# MAGIC select distinct
# MAGIC   cast(GcobId as STRING),
# MAGIC   pgu.SupportedClientOwnerMailAdress AS AttributeValue,
# MAGIC   'GCOB-GCO' AS `N2KReason`,
# MAGIC   case
# MAGIC     when pccd.clienttype = 'Legal Entity' then concat('LE_', Gcobid)
# MAGIC     when
# MAGIC       pccd.clienttype in (
# MAGIC         'Natural Person acting in a Professional Capacity (NPPC)', 'Natural Person'
# MAGIC       )
# MAGIC     then
# MAGIC       concat('NP_NPPC_', Gcobid)
# MAGIC     else 'NA'
# MAGIC   end as UniqueGcobId
# MAGIC FROM
# MAGIC   global_temp.party_case_client_details pccd
# MAGIC     join global_temp.party_GcobUsers pgu
# MAGIC       on pccd.globalclientowner = pgu.usernamre
# MAGIC       and pccd.ownertype = 'Global' --done
# MAGIC where
# MAGIC   pccd.casestatusname != 'Cancelled'
# MAGIC UNION
# MAGIC select Distinct
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   Upn AS AttributeValue,
# MAGIC   role_id AS `N2KReason`,
# MAGIC   case
# MAGIC     when al.clienttype = 'Legal Entity' then concat('LE_', Gcobid)
# MAGIC     when
# MAGIC       al.clienttype in ('Natural Person acting in a Professional Capacity (NPPC)', 'Natural Person')
# MAGIC     then
# MAGIC       concat('NP_NPPC_', Gcobid)
# MAGIC     else 'NA'
# MAGIC   end as UniqueGcobId
# MAGIC FROM
# MAGIC   AMS_all al
# MAGIC where
# MAGIC   al.clienttype is not null -- done
# MAGIC -----------------Legacy2 Data-----------
# MAGIC Union
# MAGIC select distinct
# MAGIC   cast(GcobId as STRING),
# MAGIC   --GlobalClientOwner,
# MAGIC   GlobalClientOwnerLocation AS AttributeValue,
# MAGIC   'GCO Location' AS `N2KReason`,
# MAGIC   case
# MAGIC     when cd.clienttypeid = 1 then concat('LE_', GcobId)
# MAGIC     when cd.clienttypeid in (2, 3) then concat('NP_NPPC_', GcobId)
# MAGIC     else 'NA'
# MAGIC   end As UniqueGcobId
# MAGIC from
# MAGIC   global_temp.Legacy2_case_client_details cd
# MAGIC where
# MAGIC   isclient = 'true'
# MAGIC   and cd.clienttypeid in (1, 2, 3) ---done
# MAGIC Union
# MAGIC select distinct
# MAGIC   cast(t1.GcobId as STRING) as GCOBid,
# MAGIC   t2.productlocation AS AttributeValue,
# MAGIC   'ProductLocation' AS N2KReason,
# MAGIC   case
# MAGIC     when t1.clienttypeid = 1 then concat('LE_', GcobId)
# MAGIC     when t1.clienttypeid in (2, 3) then concat('NP_NPPC_', GcobId)
# MAGIC     else 'NA'
# MAGIC   end As UniqueGcobId
# MAGIC from
# MAGIC   global_temp.Legacy2_case_client_details t1
# MAGIC     LEFT JOIN global_temp.Legacy2_products_and_services t2
# MAGIC       on t1.clientid = t2.clientid
# MAGIC where
# MAGIC   isclient = 'true'
# MAGIC   and t1.clienttypeid in (1, 2, 3) ---done
# MAGIC UNION
# MAGIC select distinct
# MAGIC   cast(t1.GcobId as STRING) as GCOBid,
# MAGIC   t2.BookingLocation AS AttributeValue,
# MAGIC   'BookingLocation' AS N2KReason,
# MAGIC   case
# MAGIC     when t1.clienttypeid = 1 then concat('LE_', GcobId)
# MAGIC     when t1.clienttypeid in (2, 3) then concat('NP_NPPC_', GcobId)
# MAGIC     else 'NA'
# MAGIC   end As UniqueGcobId
# MAGIC from
# MAGIC   global_temp.Legacy2_case_client_details t1
# MAGIC     LEFT JOIN global_temp.Legacy2_products_and_services t2
# MAGIC       on t1.clientid = t2.clientid
# MAGIC where
# MAGIC   isclient = 'true'
# MAGIC   and t1.clienttypeid in (1, 2, 3) --done
# MAGIC union
# MAGIC select distinct
# MAGIC   cast(GcobId as STRING) as GCOBid,
# MAGIC   pgu.SupportedClientOwnerMailAdress AS AttributeValue,
# MAGIC   'GCOB-GCO' AS `N2KReason`,
# MAGIC   case
# MAGIC     when pccd.clienttypeid = 1 then concat('LE_', GcobId)
# MAGIC     when pccd.clienttypeid in (2, 3) then concat('NP_NPPC_', GcobId)
# MAGIC     else 'NA'
# MAGIC   end As UniqueGcobId
# MAGIC FROM
# MAGIC   global_temp.Legacy2_case_client_details pccd
# MAGIC     join global_temp.party_GcobUsers pgu
# MAGIC       on pccd.globalclientowner = pgu.usernamre
# MAGIC where
# MAGIC   isclient = 'true'
# MAGIC   and pccd.clienttypeid in (1, 2, 3)
# MAGIC   ) --done)
# MAGIC UNION
# MAGIC SELECT
# MAGIC cast(cast(GcobId as STRING) as  STRING) as GcobId,
# MAGIC AttributeValue,
# MAGIC N2KReason,
# MAGIC UniqueGcobId
# MAGIC FROM(
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'RegionRANZ' AS AttributeValue,
# MAGIC   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   'ProductsInvolvment' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC where
# MAGIC   RANZProductsInvolvmentType IN (
# MAGIC     'Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved'
# MAGIC   )
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'RegionSA' AS AttributeValue,
# MAGIC   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   'ProductsInvolvment' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC where
# MAGIC   SAProductsInvolvmentType IN (
# MAGIC     'Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved'
# MAGIC   )
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'RegionNA' AS AttributeValue,
# MAGIC   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   'ProductsInvolvment' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC where
# MAGIC   NAProductsInvolvmentType IN (
# MAGIC     'Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved'
# MAGIC   )
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'RegionAsia' AS AttributeValue,
# MAGIC   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   'ProductsInvolvment' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC where
# MAGIC   AsiaProductsInvolvmentType IN (
# MAGIC     'Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved'
# MAGIC   )
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'RegionEA' AS AttributeValue,
# MAGIC   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   'ProductsInvolvment' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   (
# MAGIC     EAProductsInvolvmentType IN (
# MAGIC       'Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved'
# MAGIC     )
# MAGIC     OR BusinessLineName IN ('Rabo Foundation') -- added Foundation scope
# MAGIC   )
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'MTO' AS AttributeValue,
# MAGIC   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   'AADRadarCashMTOInsights' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   UniqueGcobId IN ('15784', '48411', '5689')
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'RegionFoundation' AS AttributeValue,
# MAGIC   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   'AADRadarRegionFoundation' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   GlobalReportingRegion IN ('Rabobank Foundation')
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'FIHub' AS AttributeValue,
# MAGIC   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   'FI' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   FIHubIndicator_Derived = 'FI'
# MAGIC -- n2k based on locaiton for one europe dashboard - only E&A locations, with Kenya seen by both Kenya and NL
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'Rabobank Netherlands' AS AttributeValue,
# MAGIC   'OneEurope' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya')
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'Rabobank London' AS AttributeValue,
# MAGIC   'OneEurope' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   GlobalClientOwnerLocation IN ('Rabobank London')
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'Rabobank Paris' AS AttributeValue,
# MAGIC   'OneEurope' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   GlobalClientOwnerLocation IN ('Rabobank Paris')
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'Rabobank Frankfurt' AS AttributeValue,
# MAGIC   'OneEurope' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   GlobalClientOwnerLocation IN ('Rabobank Frankfurt')
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'Rabobank Kenya' AS AttributeValue,
# MAGIC   'OneEurope' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   GlobalClientOwnerLocation IN ('Rabobank Kenya')
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'Rabobank Dublin' AS AttributeValue,
# MAGIC   'OneEurope' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   GlobalClientOwnerLocation IN ('Rabobank Dublin')
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'Rabobank Madrid' AS AttributeValue,
# MAGIC   'OneEurope' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   GlobalClientOwnerLocation IN ('Rabobank Madrid')
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'Rabobank Milan' AS AttributeValue,
# MAGIC   'OneEurope' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   GlobalClientOwnerLocation IN ('Rabobank Milan')
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) as  GcobId,
# MAGIC   'Rabobank Turkey' AS AttributeValue,
# MAGIC   'OneEurope' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   GlobalClientOwnerLocation IN ('Rabobank Turkey')
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC   cast(GcobId as STRING) AS GcobId,
# MAGIC   'Rabobank Antwerp' AS AttributeValue,
# MAGIC   'OneEurope' AS N2KReason,
# MAGIC   UniqueGcobId
# MAGIC FROM
# MAGIC   radar.clients
# MAGIC WHERE
# MAGIC   GlobalClientOwnerLocation IN ('Rabobank Antwerp'))

# COMMAND ----------

df_N2k_UserAttribute = spark.table("N2k_UserAttribute")
write_to_unity_catalog(
    df_N2k_UserAttribute, catalog, schema, N2k_UserAttribute_dataobject
)

# COMMAND ----------

df_N2K_ClientAttribute = spark.table("N2K_ClientAttribute")
write_to_unity_catalog(
    df_N2K_ClientAttribute, catalog, schema, N2K_ClientAttribute_dataobject
)
