# Databricks notebook source
# DBTITLE 1,import libraries
import requests 
import os
from datetime import datetime, timedelta
import http.client
import json
import requests
import sys
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Local File Import
from RadarUtils import *

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

N2k_UserAttribute_dataobject = 'N2k_UserAttribute'
N2K_ClientAttribute_dataobject = 'N2K_ClientAttribute'

# COMMAND ----------

# DBTITLE 1,import variables
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# Load GCDS Keystore
Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject='client_KeyStoreKey')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
  'party_client'
, 'party_case_client_details'
, 'party_local_client_Owners'
, 'party_products_and_services'
, 'party_GcobUsers'
, 'Party_AuthorizedStaff'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# Loading Legacy2 data from GDP  
load_df = [
'Legacy2_local_client_Owners'
, 'Legacy2_ClientStructure'
, 'Legacy2_case_client_details'
, 'Legacy2_products_and_services'
, 'Legacy2_client_identifiers'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

# COMMAND ----------

# DBTITLE 1,set AMS variables
env = 'pre-prod'
graph_scope = 'https://graph.microsoft.com/.default'
graph_base_url = 'graph.microsoft.com'

if env == 'pre-prod':
    # AMS
    scope = 'b464faba-cd8b-4ba9-9765-db19a0e5f790/.default'
    ams_base_url = 'https://713011d5-f99d-45d9-9b6c-f2d7c0c194b9.az-eu.api.rabo.cloud'
    ams_url = '713011d5-f99d-45d9-9b6c-f2d7c0c194b9.az-eu.api.rabo.cloud'

    # GRAPH

elif env == 'dev':
    #bearer token AMS
    scope = 'd2477ba1-a335-4cca-bc27-3e481bca3eb6/.default'
    ams_base_url = 'https://1b68e6f5-ed25-4e07-adbd-46d4b6a5c57c-nonprd.t-az-eu.api.rabo.cloud'
    ams_url = '1b68e6f5-ed25-4e07-adbd-46d4b6a5c57c-nonprd.t-az-eu.api.rabo.cloud'

    # GRAPH
elif env == 'prd':
    scope = 'b464faba-cd8b-4ba9-9765-db19a0e5f790/.default'
    ams_base_url = 'https://713011d5-f99d-45d9-9b6c-f2d7c0c194b9.az-eu.api.rabo.cloud'
    ams_url = '713011d5-f99d-45d9-9b6c-f2d7c0c194b9.az-eu.api.rabo.cloud'






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
    "client_secret": f"{service_credential}"
}

# Use https request to generate auth token
resp = requests.post(
    url="https://login.microsoftonline.com/6e93a626-8aca-4dc1-9191-ce291b4b75a1/oauth2/v2.0/token",
    headers=headers,
    data=values,
)

# prd = /api/GetUserRoles/CI4124929?limit=2500
# prd_scope = https://713011d5-f99d-45d9-9b6c-f2d7c0c194b9.az-eu.api.rabo.cloud
# 
print("Auth response: (should be 200)")
print(resp)


# COMMAND ----------

# DBTITLE 1,Get API response from central
# call ams api with toke
# CI4124929 = GCOB
token = json.loads(resp.text)["access_token"]
header_info = {
    "Authorization": f"Bearer {token}"
}

res = requests.get(f'{ams_base_url}/api/GetUserRoles/CI4124929?limit=2500' , headers=header_info)

# currently forbidden to access azure app gateway

# COMMAND ----------

print(res)

# COMMAND ----------

# DBTITLE 1,attempt using http library instead of request (like the Metadata API?)
relative_url = '/api/GetUserRoles/CI4124929?limit=2500' #RoleId=='COB'|| RoleId=='GC-FLM'|| RoleId=='GC-AUD'|| RoleId=='GC-COM'|| RoleId=='GC-DS'|| RoleId=='GC-COS'|| RoleId=='GCEO'

connection = http.client.HTTPSConnection(ams_url)
connection.request(method = "GET", url=relative_url, headers = header_info)
result = connection.getresponse()
#print (f'body={json.dumps(param_values)}') 


# COMMAND ----------

# DBTITLE 1,AMS API DATA
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import  explode, col
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
All_AMS_df = pd.DataFrame()

# ===== UPDATED CODE - With validation for Checkmarx =====
# Validate and parse AMS response
try:
    ams_resp_json = json.loads(result.read())
except json.JSONDecodeError as e:
    raise Exception(f"Failed to parse AMS API JSON response: {str(e)}")

# Validate response structure
if 'user_details' not in ams_resp_json:
    raise ValueError("AMS API response missing 'user_details' key")

if not isinstance(ams_resp_json['user_details'], list):
    raise ValueError("AMS API 'user_details' is not a list")

#print(ams_resp_json)
AMSreqdf = pd.DataFrame.from_dict(ams_resp_json['user_details'], orient='columns')
df_spark = spark.createDataFrame(AMSreqdf)
#display(df_spark)
df_ams=df_spark.select(col("user_id"),col("upn"),explode("roles").alias("rolesdetails"),col("rolesdetails.id"),col("rolesdetails.role_id"),col("rolesdetails.context"),col("rolesdetails.context.client")).drop("rolesdetails")
df_ams.createOrReplaceTempView('AMS_API')

# COMMAND ----------

df_ams.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Graph API

# COMMAND ----------

# DBTITLE 1,get token for calling Graph API
# scope for which we are requesting a token
scope = 'https://graph.microsoft.com/.default'

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
    "client_secret": f"{service_credential}"
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

url = 'graph.microsoft.com'

# COMMAND ----------

# DBTITLE 1,Get Azure AD groups with validation - Rajeev
# ===== UPDATED CODE - With validation for Checkmarx =====
# Get GCOB groups with validation

allgcobgroups = requests.get("https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName,'eu.aut.AADGCOBPRDRoleClientOwnerSupport.us') or startswith(displayName,'eu.aut.AADGCOBPRDLocation') or startswith(displayName,'eu.aut.AADGCOBPRDGlobalFIHub.us')", headers = header_info) #?$search="Name:eu.aut.AADGCOB"

# Validate response
if allgcobgroups.status_code != 200:
    raise Exception(f"Failed to fetch GCOB groups: Status {allgcobgroups.status_code}, Response: {allgcobgroups.text}")

try:
    GCOBLocationGroups = allgcobgroups.json()
    if 'value' not in GCOBLocationGroups:
        raise ValueError("Response missing 'value' key")
except json.JSONDecodeError as e:
    raise Exception(f"Failed to parse JSON response: {str(e)}")

# COMMAND ----------

# #res2 = requests.get("https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName,'eu.aut.AADGCOBPRDLocation')", headers = header_info) #?$search="Name:eu.aut.AADGCOB"
# #GCOBLocationGroups = json.loads(res2.text)

# res2 = requests.get("https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName,'eu.aut.AADGCOBPRDRoleClientOwnerSupport.us')", headers = header_info) #?$search="Name:eu.aut.AADGCOB"
# #GCOBLocationGroups = json.loads(res2.text)

# #display(GCOBLocationGroups)
# AADReturnGrp = pd.DataFrame.from_dict(GCOBLocationGroups['value'], orient='columns')
# #display(AMSreqdf) 
# AADreturnGrp_output = spark.createDataFrame(AADReturnGrp)
# #display(AADreturnGrp_output)
# #AMSreqdf_output.createOrReplaceTempView('AMSreqdf_output_view')


# COMMAND ----------

# DBTITLE 1,Working Group details with user ,AS expected
df_AllGroupUsers = pd.DataFrame()
# Maximum number of pagination pages to prevent DoS attacks
MAX_PAGINATION_PAGES = 1000

# Validate input before loop
if not isinstance(GCOBLocationGroups.get('value'), list):
    raise ValueError("GCOBLocationGroups missing valid 'value' list")

# ===== UPDATED CODE - With validation for Checkmarx =====
for group in GCOBLocationGroups['value']:
    # Validate group structure
    if not isinstance(group, dict) or 'id' not in group or 'displayName' not in group:
        print(f"Skipping invalid group entry: {group}")
        continue
        
    id = group['id']
    gpname = group['displayName']
    url = f"https://graph.microsoft.com/v1.0/groups/{id}/members?$count=true"
    
    # Add iteration counter to prevent infinite loops
    page_count = 0
    
    while url and page_count < MAX_PAGINATION_PAGES:
        page_count += 1
        try:
            response = requests.get(url=url, headers=header_info)
            
            # Check HTTP status
            if response.status_code != 200:
                print(f"Failed to get members for group {gpname}: Status {response.status_code}")
                break
            
            graph_result = response.json()
            
            # Validate response structure
            if 'value' not in graph_result:
                print(f"Invalid response for group {gpname}: missing 'value' key")
                break
            
            UsersPerGroupDf = pd.DataFrame.from_dict(graph_result['value'], orient='columns')
            UsersPerGroupDf['GroupName'] = gpname
            df_AllGroupUsers = pd.concat([df_AllGroupUsers, UsersPerGroupDf], ignore_index=True)
            
            # Check for next page
            url = graph_result.get('@odata.nextLink', None)
            
        except json.JSONDecodeError as e:
            print(f"JSON decode error for group {gpname}: {str(e)}")
            break
        except Exception as e:
            print(f"Error processing group {gpname}: {str(e)}")
            break
    
    # Log if max pages reached
    if page_count >= MAX_PAGINATION_PAGES:
        print(f"Warning: Reached maximum pagination limit ({MAX_PAGINATION_PAGES}) for group {gpname}")

# COMMAND ----------

df_spark1 = spark.createDataFrame(df_AllGroupUsers )
df_spark1.createOrReplaceTempView('AllGroupUsers')

# COMMAND ----------

df_gcob_aad_mapping = pd.DataFrame({'ExtractedLocationStringFromGCOB': ['Foundation', 'RANZZROZ', 'CanadaRural', 'RANZROZ', 'SAF', 'Milan', 'NewZealand', 'Turkey', 'Netherlands', 'Antwerp', 'Indonesia', 'HongKong', 'London', 'Paris', 'Kenya', 'Frankfurt', 'Australia', 'Malaysia', 'Canada', '', 'China', 'India', 'Singapore', 'AgriFinance', 'Madrid', 'Argentina', 'NewYork', 'SecuritiesCanada', 'SecuritiesUSA', 'Chile', 'Dublin']
                                    , 'RabobankEntityGcobId':[28, 32, 29, 32, 31, 15, 20, 23, 19, 3, 13, 11, 24, 9, 16, 10, 2, 17, 5, 30, 8, 12, 21, 27, 22, 1, 25, 6, 26, 7, 14] 
                                    , 'GcobLocationName' : ['Rabobank Foundation', 'Rabobank - RANZ Country Banking and ROS', 'Rabobank Canada(Rural)', 'Rabobank - RANZ Country Banking and ROS', 'Rabobank - Smallholder Agroforestry Finance (SAF)', 'Rabobank Milan', 'Rabobank New Zealand', 'Rabobank Turkey', 'Rabobank Netherlands', 'Rabobank Antwerp', 'Rabobank Indonesia', 'Rabobank Hong Kong', 'Rabobank London', 'Rabobank Paris', 'Rabobank Kenya', 'Rabobank Frankfurt', 'Rabobank Australia', 'Rabobank Malaysia', 'Rabobank Canada (RCBR)', 'Global FI Hub', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Madrid', 'Rabobank Argentina', 'Rabobank New York', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabobank Chile', 'Rabobank Dublin']})

sdf_gcob_aad_mapping = spark.createDataFrame(df_gcob_aad_mapping)
sdf_gcob_aad_mapping.createOrReplaceTempView('gcob_aad_mapping')

# COMMAND ----------

df_UserLocationAttribute = spark.sql("""
    -- select all extracted location string from GCOB mapping
select DISTINCT T1.RabobankEntityGcobId , t2.userPrincipalName, T1.GcobLocationName
From  gcob_aad_mapping   as T1
 left JOIN  AllGroupUsers  as T2 ON regexp_extract(t2.`GroupName`, '(?<=Location)(.*)(?=\.us)', 0) = t1.`ExtractedLocationStringFromGCOB`

UNION

SELECT DISTINCT
    'FI' AS RabobankEntityGcobId
    , userPrincipalName
    , 'FI' AS GcobLocationName
FROM AllGroupUsers
WHERE GroupName = 'eu.aut.AADGCOBPRDGlobalFIHub.us'
 """)

# COMMAND ----------

df_UserLocationAttribute.createOrReplaceTempView('Location_UserAttribute')

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view AMS_ALL As
# MAGIC select distinct
# MAGIC ams_api.user_id,
# MAGIC ams_api.Upn,
# MAGIC ams_api.Role_Id,
# MAGIC ams_api.client as GCDSID,
# MAGIC t2.KeyStore_value AS GCOBID,
# MAGIC case when pccd.clienttype is null then pcd.clienttype else pccd.clienttype end as  clienttype
# MAGIC from  AMS_API as ams_api
# MAGIC left join (select * from client_KeyStoreKey where KeyStore_type = 'GCOBID') t2 on ams_api.client = t2.GCID
# MAGIC left join party_case_client_details pccd on t2.GCID=pccd.gcdsid
# MAGIC left join party_case_client_details pcd on t2.KeyStore_value=pcd.gcobid
# MAGIC where ams_api.Role_Id='GCEO'  
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW AllCosUsers AS
# MAGIC  
# MAGIC SELECT DISTINCT
# MAGIC     mail
# MAGIC     , userPrincipalName
# MAGIC FROM AllGroupUsers
# MAGIC WHERE GroupName = 'eu.aut.AADGCOBPRDRoleClientOwnerSupport.us'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcobcsrole AS
# MAGIC
# MAGIC Select distinct
# MAGIC     --UserMailAdress AS `UPN_Other`,
# MAGIC     ag.userPrincipalName AS `UPN`
# MAGIC     --,case when ag.userPrincipalName is null then
# MAGIC     ,`Role Type Code` AS `AttributeType`
# MAGIC     , SupportedClientOwnerMailAdress AS `AttributeValue`
# MAGIC     
# MAGIC     FROM
# MAGIC     party_GcobUsers gu
# MAGIC     left join AllCosUsers ag on gu.UserMailAdress=ag.mail
# MAGIC     where 
# MAGIC     ag.userPrincipalName is not null
# MAGIC     --UserMailAdress like 'Constanza.Acevedo%'
# MAGIC     --UserMailAdress like '%Matthew%Edwards%'
# MAGIC
# MAGIC     

# COMMAND ----------

# DBTITLE 1,Get Radar groups with validation
# ===== UPDATED CODE - With validation for Checkmarx =====

# For Both Fecradar and AADRadar group
# Get AADRadar groups with validation
AADRadargrp = requests.get("https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName,'eu.aut.AADRadar')", headers = header_info) #?$search="Name:eu.aut.AADGCOB"
#RegionEurope
#eu.aut.AADRadar
#eu.aut.AADRadarRegionEurope 9c87aec0-9bd3-418a-bc4c-836584b7e558

if AADRadargrp.status_code != 200:
    raise Exception(f"Failed to fetch AADRadar groups: Status {AADRadargrp.status_code}, Response: {AADRadargrp.text}")

try:
    AADRadarGroups = AADRadargrp.json()
    if 'value' not in AADRadarGroups:
        raise ValueError("AADRadar response missing 'value' key")
except json.JSONDecodeError as e:
    raise Exception(f"Failed to parse AADRadar JSON response: {str(e)}")

#print(RadarGroups)
AADReturnGrpradar = pd.DataFrame.from_dict(AADRadarGroups['value'], orient='columns')
#display(AMSreqdf) 
AADreturnGrp = spark.createDataFrame(AADReturnGrpradar)
AADreturnGrp=AADreturnGrp.filter(AADreturnGrp.displayName.contains("Region"))
#display(AADreturnGrp)

####
# Get FECRadar groups with validation
FecRadargrp = requests.get("https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName,'eu.aut.AADFECradar')", headers = header_info) #?$search="Name:eu.aut.AADGCOB"

if FecRadargrp.status_code != 200:
    raise Exception(f"Failed to fetch FECRadar groups: Status {FecRadargrp.status_code}, Response: {FecRadargrp.text}")

try:
    FecRadarGroups = FecRadargrp.json()
    if 'value' not in FecRadarGroups:
        raise ValueError("FECRadar response missing 'value' key")
except json.JSONDecodeError as e:
    raise Exception(f"Failed to parse FECRadar JSON response: {str(e)}")

#print(RadarGroups)
FecReturnGrp = pd.DataFrame.from_dict(FecRadarGroups['value'], orient='columns')
#display(AMSreqdf) 
FecReturnGrp = spark.createDataFrame(FecReturnGrp)
FecReturnGrp=FecReturnGrp.filter(FecReturnGrp.displayName.contains("Region"))
#display(FecReturnGrp)
AADFinalReturnGrp=AADreturnGrp.unionAll(FecReturnGrp)
#display(AADFinalReturnGrp)

# COMMAND ----------

# DBTITLE 1,Process AADRadar group members with validation
# ===== UPDATED CODE - With validation for Checkmarx =====
# Maximum number of pagination pages to prevent DoS attacks
MAX_PAGINATION_PAGES = 1000

df_AllAADRadarGroupUsers = pd.DataFrame()

# Validate input before loop
if not isinstance(AADRadarGroups.get('value'), list):
    raise ValueError("AADRadarGroups missing valid 'value' list")

for group in AADRadarGroups['value']:
    # Validate group structure
    if not isinstance(group, dict) or 'id' not in group or 'displayName' not in group:
        print(f"Skipping invalid AADRadar group entry: {group}")
        continue
        
    id = group['id']
    gpname = group['displayName']    
    url = f"https://graph.microsoft.com/v1.0/groups/{id}/members?$count=true"
    
    # Add iteration counter to prevent infinite loops
    page_count = 0
    
    while url and page_count < MAX_PAGINATION_PAGES:
        page_count += 1
        try:
            response = requests.get(url=url, headers=header_info)
            
            # Check HTTP status
            if response.status_code != 200:
                print(f"Failed to get members for AADRadar group {gpname}: Status {response.status_code}")
                break
            
            AADresult = response.json()
            
            # Validate response structure
            if 'value' not in AADresult:
                print(f"Invalid response for AADRadar group {gpname}: missing 'value' key")
                break
            
            UsersPerGroupDf = pd.DataFrame.from_dict(AADresult['value'], orient='columns')
            UsersPerGroupDf['GroupName'] = gpname
            df_AllAADRadarGroupUsers = pd.concat([df_AllAADRadarGroupUsers, UsersPerGroupDf], ignore_index=True)
            
            # Check for next page
            url = AADresult.get('@odata.nextLink', None)
            
        except json.JSONDecodeError as e:
            print(f"JSON decode error for AADRadar group {gpname}: {str(e)}")
            break
        except Exception as e:
            print(f"Error processing AADRadar group {gpname}: {str(e)}")
            break
    
    # Log if max pages reached
    if page_count >= MAX_PAGINATION_PAGES:
        print(f"Warning: Reached maximum pagination limit ({MAX_PAGINATION_PAGES}) for AADRadar group {gpname}")
                           

# COMMAND ----------

df_spark1 = spark.createDataFrame(df_AllAADRadarGroupUsers)
df_spark1.createOrReplaceTempView('AllAADRadarGroupUsers')

# COMMAND ----------

# DBTITLE 1,Process FECRadar group members with validation
# ===== UPDATED CODE - With validation for Checkmarx =====
# Maximum number of pagination pages to prevent DoS attacks
MAX_PAGINATION_PAGES = 1000
df_AllFecAADRadarGroupUsers = pd.DataFrame()

# Validate input before loop
if not isinstance(FecRadarGroups.get('value'), list):
    raise ValueError("FecRadarGroups missing valid 'value' list")

for group in FecRadarGroups['value']:
    # Validate group structure
    if not isinstance(group, dict) or 'id' not in group or 'displayName' not in group:
        print(f"Skipping invalid FECRadar group entry: {group}")
        continue
        
    id = group['id']
    gpname = group['displayName']
    url = f"https://graph.microsoft.com/v1.0/groups/{id}/members?$count=true"
    
    # Add iteration counter to prevent infinite loops
    page_count = 0
    
    while url and page_count < MAX_PAGINATION_PAGES:
        page_count += 1
        try:
            response = requests.get(url=url, headers=header_info)
            
            # Check HTTP status
            if response.status_code != 200:
                print(f"Failed to get members for FECRadar group {gpname}: Status {response.status_code}")
                break
            
            FecAADRadar_result = response.json()
            
            # Validate response structure
            if 'value' not in FecAADRadar_result:
                print(f"Invalid response for FECRadar group {gpname}: missing 'value' key")
                break
            
            UsersPerGroupDf = pd.DataFrame.from_dict(FecAADRadar_result['value'], orient='columns')
            UsersPerGroupDf['GroupName'] = gpname
            df_AllFecAADRadarGroupUsers = pd.concat([df_AllFecAADRadarGroupUsers, UsersPerGroupDf], ignore_index=True)
            
            # Check for next page
            url = FecAADRadar_result.get('@odata.nextLink', None)
            
        except json.JSONDecodeError as e:
            print(f"JSON decode error for FECRadar group {gpname}: {str(e)}")
            break
        except Exception as e:
            print(f"Error processing FECRadar group {gpname}: {str(e)}")
            break
    
    # Log if max pages reached
    if page_count >= MAX_PAGINATION_PAGES:
        print(f"Warning: Reached maximum pagination limit ({MAX_PAGINATION_PAGES}) for FECRadar group {gpname}")
                        
      

# COMMAND ----------

df_spark1 = spark.createDataFrame(df_AllFecAADRadarGroupUsers)
df_spark1.createOrReplaceTempView('AllFecDRadarGroupUsers')

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
    raise Exception(f"Failed to fetch OneEurope group members: Status {response.status_code}, Response: {response.text}")

try:
    oneeurope_user_list = response.json()
    if 'value' not in oneeurope_user_list:
        raise ValueError("OneEurope response missing 'value' key")
except json.JSONDecodeError as e:
    raise Exception(f"Failed to parse OneEurope JSON response: {str(e)}")

for user in oneeurope_user_list['value']:
    # Validate user structure
    if not isinstance(user, dict) or 'id' not in user:
        print(f"Skipping invalid OneEurope user entry: {user}")
        continue
        
    id = user['id']
    username = user.get('displayName', 'Unknown')

    url = f"https://graph.microsoft.com/v1.0/users/{id}?$select=displayName,givenName,userPrincipalName,companyName,country"

    # Add iteration counter to prevent infinite loops
    page_count = 0
    
    while url and page_count < MAX_PAGINATION_PAGES:
        page_count += 1
        try:
            user_response = requests.get(url=url, headers=header_info)
            
            # Check HTTP status
            if user_response.status_code != 200:
                print(f"Failed to get details for user {username}: Status {user_response.status_code}")
                break
            
            oneeurope_user_response = user_response.json()
            df_oneeurope_users = pd.concat([pd.DataFrame([oneeurope_user_response]), df_oneeurope_users], ignore_index=True)
            
            # Check for next page
            url = oneeurope_user_response.get('@odata.nextLink', None)
         
        except json.JSONDecodeError as e:
            print(f"JSON decode error for user {username}: {str(e)}")
            break
        except Exception as e:
            print(f"Error processing user {username}: {str(e)}")
            break
    
    # Log if max pages reached
    if page_count >= MAX_PAGINATION_PAGES:
        print(f"Warning: Reached maximum pagination limit ({MAX_PAGINATION_PAGES}) for user {username}")

# COMMAND ----------

# DBTITLE 1,create oneeurope_users_raw tempview for n2k based on location
from pyspark.sql.types import StructType, StructField, StringType

# handle empty df
if df_oneeurope_users.empty:
    schema = StructType([
        StructField("userPrincipalName", StringType(), True),
        StructField("companyName",     StringType(), True),
    ])
    spark_df = spark.createDataFrame([], schema)
else:
    spark_df = spark.createDataFrame(df_oneeurope_users)

spark_df.createOrReplaceTempView("oneeurope_users_raw")

# COMMAND ----------

# DBTITLE 1,oneeurope_users
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW oneeurope_users AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   userPrincipalName AS UPN
# MAGIC   , 'LocationOneEurope' AS AttributeType
# MAGIC   , CASE
# MAGIC       WHEN companyName = 'Coöperatieve Rabobank U.A.' THEN 'Rabobank Netherlands'
# MAGIC       WHEN companyName = 'Rabobank London' THEN 'Rabobank London'
# MAGIC       WHEN companyName = 'Rabobank Paris' THEN 'Rabobank Paris' 
# MAGIC       WHEN companyName = 'Rabobank Frankfurt' THEN 'Rabobank Frankfurt'
# MAGIC       WHEN companyName = 'Rabobank Antwerp' THEN 'Rabobank Antwerp'
# MAGIC       WHEN companyName = 'Rabobank Kenya' THEN 'Rabobank Kenya'
# MAGIC       WHEN companyName = 'Rabobank Dublin' THEN 'Rabobank Dublin'
# MAGIC       WHEN companyName = 'Rabobank Madrid' THEN 'Rabobank Madrid'
# MAGIC       WHEN companyName = 'Rabobank Milan' THEN 'Rabobank Milan'
# MAGIC       WHEN companyName = 'Rabobank Turkey' THEN 'Rabobank Turkey'
# MAGIC       -- WHEN companyName = 'Cooperatieve Rabobank Argentina, Oficina de Representación' THEN 'Rabobank Argentina'
# MAGIC       ELSE NULL
# MAGIC     END AS AttributeValue
# MAGIC FROM oneeurope_users_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view ALL_Fec_Radar_GroupInfo As
# MAGIC select userprincipalname,GroupName from AllFecDRadarGroupUsers
# MAGIC union all
# MAGIC select userprincipalname,GroupName from AllAADRadarGroupUsers 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Region_UserInfo AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   userprincipalname AS UPN
# MAGIC   , 'PortfolioRegion' AS AttributeType
# MAGIC   , CASE
# MAGIC       WHEN GroupName = 'eu.aut.AADRadarRegionAsia.us' THEN 'RegionAsia'
# MAGIC       WHEN GroupName = 'eu.aut.AADRadarRegionEuropeAfrica.us' THEN 'RegionEA'
# MAGIC       WHEN GroupName = 'eu.aut.AADRadarRegionNA.us' THEN 'RegionNA' 
# MAGIC       WHEN GroupName = 'eu.aut.AADRadarRegionRANZ.us' THEN 'RegionRANZ' 
# MAGIC       WHEN GroupName = 'eu.aut.AADRadarRegionSA.us' THEN 'RegionSA'
# MAGIC       WHEN GroupName = 'eu.aut.AADRadarCashMTOInsights.us' THEN 'MTO'
# MAGIC       WHEN GroupName = 'eu.aut.AADFECRadarMTODashboardEUA.us' THEN 'MTO'
# MAGIC       WHEN GroupName = 'eu.aut.AADRadarRegionFoundation.us' THEN 'RegionFoundation'
# MAGIC       WHEN GroupName = 'eu.aut.AADRadarDepartmentTCF.us' THEN 'DepartmentTCF'
# MAGIC       WHEN GroupName = 'eu.aut.AADGCOBPRDGlobalFIHub.us' THEN 'FIHub'
# MAGIC       WHEN GroupName = 'Eu.aut.AADRadarRegionGlobalFI.us' THEN 'FIHub'
# MAGIC       ELSE NULL
# MAGIC     END AS AttributeValue
# MAGIC FROM ALL_Fec_Radar_GroupInfo
# MAGIC WHERE GroupName IN ('eu.aut.AADRadarRegionAsia.us','eu.aut.AADRadarRegionEuropeAfrica.us','eu.aut.AADRadarRegionNA.us','eu.aut.AADRadarRegionRANZ.us','eu.aut.AADRadarRegionSA.us','eu.aut.AADRadarCashMTOInsights.us', 'eu.aut.AADRadarRegionFoundation.us', 'eu.aut.AADFECRadarMTODashboardEUA.us', 'eu.aut.AADRadarDepartmentTCF.us', 'eu.aut.AADGCOBPRDGlobalFIHub.us', 'Eu.aut.AADRadarRegionGlobalFI.us')

# COMMAND ----------

# DBTITLE 1,Hardcoded Retail-NL users for MTO
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RETAIL_NL_MTO AS
# MAGIC SELECT 
# MAGIC 'Priscilla.Profijt@rabobank.nl' AS UPN,
# MAGIC 'RetailNLMTO' AS AttributeType,
# MAGIC 'MTO' AS AttributeValue
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT
# MAGIC 'Bianca.M.Gobbens@rabobank.nl' AS UPN,
# MAGIC 'RetailNLMTO' AS AttributeType,
# MAGIC 'MTO' AS AttributeValue
# MAGIC
# MAGIC -- These 2 persons are Retail NL persons who need to use the MTO dashboard for review
# MAGIC -- added by Ruud.van.laar 2025-04-29

# COMMAND ----------

# DBTITLE 1,Get the protected users Details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ProtectedAccount AS
# MAGIC SELECT DISTINCT
# MAGIC     t2.EmailAddress as UPN,
# MAGIC     'ProtectedAccount' AS AttributeType,
# MAGIC     'IsprotectedAccount' AS AttributeValue
# MAGIC FROM party_case_client_details t1
# MAGIC INNER JOIN Party_AuthorizedStaff t2
# MAGIC ON t2.LegalEntityClientGcobId = t1.GCObid
# MAGIC WHERE t1.IsprotectedAccount = 'True'

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

    UNION   

    -- N2K for 2 MTO dashboard users untill we see how they should use AD. / LEX / AAS elements.
    SELECT DISTINCT
        UPN
        , AttributeType
        , AttributeValue
    FROM RETAIL_NL_MTO
-- To see the only authorised staff per Is protected clients
    UNION
    
    SELECT DISTINCT
        UPN
        , AttributeType
        , AttributeValue
    FROM ProtectedAccount
    
""")
df_UserAttribute.createOrReplaceTempView('N2k_UserAttribute')

# COMMAND ----------

# DBTITLE 1,get the Protected clients
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW protectedClientids AS
# MAGIC SELECT DISTINCT GcobId
# MAGIC FROM party_case_client_details 
# MAGIC WHERE IsprotectedAccount = 'True'
# MAGIC

# COMMAND ----------

# DBTITLE 1,N2KClientAttribute
# MAGIC %sql
# MAGIC /*
# MAGIC   FOR THE FUTURE: align definition of UniqueGcobId among views - eg. LE_ + GcobId everywhere
# MAGIC */
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW N2KClientAttribute AS
# MAGIC
# MAGIC select distinct
# MAGIC     GcobId,
# MAGIC     --GlobalClientOwner,
# MAGIC     GlobalClientOwnerLocation AS AttributeValue,
# MAGIC     'GCO Location' AS `N2KReason`,
# MAGIC      case when pc.clienttype='Legal Entity' then concat('LE_', GcobId) 
# MAGIC     when pc.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',GcobId) 
# MAGIC         else 'NA' end as  UniqueGcobId
# MAGIC     FROM party_case_client_details pc  --done
# MAGIC     WHERE pc.casestatusname!='Cancelled'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC     select  distinct 
# MAGIC         t1.GcobId,
# MAGIC         t2.ProductOfferingLocation AS AttributeValue,
# MAGIC         'ProductLocation' AS N2KReason,
# MAGIC         case when t1.clienttype='Legal Entity' then concat('LE_', t1.GcobId) 
# MAGIC     when t1.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',t1.GcobId) 
# MAGIC         else 'NA' end as  UniqueGcobId
# MAGIC     from party_case_client_details t1
# MAGIC     LEFT JOIN party_products_and_services t2 on t1.SourceClient = t2.SourceClient
# MAGIC     WHERE t2.ProductLifecyclestatus = 'Active' and t1.casestatusname!='Cancelled' --done
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC     select distinct
# MAGIC         t1.GcobId,
# MAGIC         t2.BookingEntityLocation AS AttributeValue,
# MAGIC         'BookingLocation' AS N2KReason,
# MAGIC         case when t1.clienttype='Legal Entity' then concat('LE_', t1.GcobId) 
# MAGIC     when t1.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',t1.GcobId) 
# MAGIC         else 'NA' end as  UniqueGcobId
# MAGIC     from party_case_client_details t1
# MAGIC     LEFT JOIN party_products_and_services t2 on t1.SourceClient = t2.SourceClient
# MAGIC     WHERE t2.ProductLifecyclestatus = 'Active' and t1.casestatusname!='Cancelled'--done
# MAGIC     
# MAGIC UNION
# MAGIC
# MAGIC   select distinct
# MAGIC         GcobId,
# MAGIC          pgu.SupportedClientOwnerMailAdress AS AttributeValue,
# MAGIC         'GCOB-GCO' AS `N2KReason`,
# MAGIC         case when pccd.clienttype='Legal Entity' then concat('LE_', Gcobid) 
# MAGIC     when pccd.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',Gcobid) 
# MAGIC         else 'NA' end as  UniqueGcobId
# MAGIC         FROM party_case_client_details pccd
# MAGIC          join party_GcobUsers pgu
# MAGIC         on pccd.globalclientowner=pgu.usernamre
# MAGIC             and  pccd.ownertype='Global'--done 
# MAGIC         where pccd.casestatusname!='Cancelled'
# MAGIC             
# MAGIC  UNION
# MAGIC
# MAGIC     select Distinct
# MAGIC         GCOBID AS GcobId
# MAGIC       , Upn AS AttributeValue
# MAGIC        , role_id AS `N2KReason`,
# MAGIC        case when al.clienttype='Legal Entity' then concat('LE_', Gcobid) 
# MAGIC     when al.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',Gcobid) 
# MAGIC         else 'NA' end as  UniqueGcobId
# MAGIC         FROM AMS_all  al where al.clienttype is not null -- done
# MAGIC
# MAGIC         -----------------Legacy2 Data-----------
# MAGIC     Union 
# MAGIC
# MAGIC     select distinct
# MAGIC     GcobId,
# MAGIC     --GlobalClientOwner,
# MAGIC     GlobalClientOwnerLocation AS AttributeValue,
# MAGIC     'GCO Location' AS `N2KReason`,
# MAGIC     case when cd.clienttypeid= 1 then concat('LE_', GcobId)
# MAGIC     when cd.clienttypeid in(2,3) then concat('NP_NPPC_', GcobId)  else 'NA' end As UniqueGcobId from Legacy2_case_client_details cd
# MAGIC     where isclient='true' and cd.clienttypeid in (1,2,3) ---done
# MAGIC   
# MAGIC     union
# MAGIC
# MAGIC  select  distinct 
# MAGIC         t1.GCOBid,
# MAGIC         t2.productlocation AS AttributeValue,
# MAGIC         'ProductLocation' AS N2KReason,
# MAGIC         case when t1.clienttypeid= 1 then concat('LE_', GcobId)
# MAGIC     when t1.clienttypeid in(2,3) then concat('NP_NPPC_', GcobId)  else 'NA' end As UniqueGcobId
# MAGIC     from Legacy2_case_client_details t1
# MAGIC     LEFT JOIN Legacy2_products_and_services t2 on t1.clientid = t2.clientid 
# MAGIC      where isclient='true' and t1.clienttypeid in (1,2,3) ---done
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC    select distinct
# MAGIC         t1.GCOBid,
# MAGIC         t2.BookingLocation AS AttributeValue,
# MAGIC         'BookingLocation' AS N2KReason,
# MAGIC         case when t1.clienttypeid= 1 then concat('LE_', GcobId)
# MAGIC     when t1.clienttypeid in(2,3) then concat('NP_NPPC_', GcobId)  else 'NA' end As UniqueGcobId
# MAGIC     from Legacy2_case_client_details t1
# MAGIC     LEFT JOIN Legacy2_products_and_services t2 on t1.clientid = t2.clientid 
# MAGIC     where isclient='true' and t1.clienttypeid in (1,2,3) --done
# MAGIC     union
# MAGIC    select distinct
# MAGIC         GcobId,
# MAGIC          pgu.SupportedClientOwnerMailAdress AS AttributeValue,
# MAGIC         'GCOB-GCO' AS `N2KReason`,
# MAGIC         case when pccd.clienttypeid= 1 then concat('LE_', GcobId)
# MAGIC     when pccd.clienttypeid in(2,3) then concat('NP_NPPC_', GcobId)  else 'NA' end As UniqueGcobId
# MAGIC         FROM Legacy2_case_client_details pccd
# MAGIC          join party_GcobUsers pgu
# MAGIC         on pccd.globalclientowner=pgu.usernamre 
# MAGIC         where isclient='true' and pccd.clienttypeid in (1,2,3)  --done 
# MAGIC
# MAGIC
# MAGIC   UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId as GcobId
# MAGIC       , 'RegionRANZ' AS AttributeValue
# MAGIC     --, RANZProductsInvolvmentType AS N2KReason
# MAGIC       , 'ProductsInvolvment' AS N2KReason
# MAGIC       , UniqueGcobId 
# MAGIC     FROM radar.clients
# MAGIC     where RANZProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved' )
# MAGIC
# MAGIC   UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId as GcobId
# MAGIC       , 'RegionSA' AS AttributeValue
# MAGIC     --, RANZProductsInvolvmentType AS N2KReason
# MAGIC       , 'ProductsInvolvment' AS N2KReason
# MAGIC       , UniqueGcobId 
# MAGIC     FROM radar.clients
# MAGIC     where SAProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved' ) 
# MAGIC     
# MAGIC      UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId as GcobId
# MAGIC     , 'RegionNA' AS AttributeValue
# MAGIC     --, RANZProductsInvolvmentType AS N2KReason
# MAGIC       , 'ProductsInvolvment' AS N2KReason
# MAGIC       , UniqueGcobId 
# MAGIC     FROM radar.clients
# MAGIC     where NAProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved' )  
# MAGIC     
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId as GcobId
# MAGIC     , 'RegionAsia' AS AttributeValue
# MAGIC     --, RANZProductsInvolvmentType AS N2KReason
# MAGIC       , 'ProductsInvolvment' AS N2KReason
# MAGIC       , UniqueGcobId 
# MAGIC     FROM radar.clients
# MAGIC     where AsiaProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved' )  
# MAGIC     
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId as GcobId
# MAGIC     , 'RegionEA' AS AttributeValue
# MAGIC     --, RANZProductsInvolvmentType AS N2KReason
# MAGIC       , 'ProductsInvolvment' AS N2KReason
# MAGIC       , UniqueGcobId 
# MAGIC     FROM radar.clients
# MAGIC     WHERE
# MAGIC       (
# MAGIC         EAProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved')
# MAGIC         OR BusinessLineName IN ('Rabo Foundation') -- added Foundation scope
# MAGIC       )
# MAGIC
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId as GcobId
# MAGIC       , 'MTO' AS AttributeValue
# MAGIC     --, RANZProductsInvolvmentType AS N2KReason
# MAGIC       , 'AADRadarCashMTOInsights' AS N2KReason
# MAGIC       , UniqueGcobId 
# MAGIC     FROM radar.clients
# MAGIC     WHERE UniqueGcobId IN ('15784', '48411', '5689')
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId as GcobId
# MAGIC       , 'RegionFoundation' AS AttributeValue
# MAGIC     --, RANZProductsInvolvmentType AS N2KReason
# MAGIC       , 'AADRadarRegionFoundation' AS N2KReason
# MAGIC       , UniqueGcobId 
# MAGIC     FROM radar.clients
# MAGIC     WHERE GlobalReportingRegion IN ('Rabobank Foundation')
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId as GcobId
# MAGIC       , 'FIHub' AS AttributeValue
# MAGIC     --, RANZProductsInvolvmentType AS N2KReason
# MAGIC       , 'FI' AS N2KReason
# MAGIC       , UniqueGcobId 
# MAGIC     FROM radar.clients
# MAGIC     WHERE FIHubIndicator_Derived = 'FI'
# MAGIC
# MAGIC
# MAGIC     -- n2k based on locaiton for one europe dashboard - only E&A locations, with Kenya seen by both Kenya and NL
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId AS GcobId
# MAGIC       , 'Rabobank Netherlands' AS AttributeValue
# MAGIC       , 'OneEurope' AS N2KReason
# MAGIC       , UniqueGcobId
# MAGIC     FROM radar.clients
# MAGIC     WHERE GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya')
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId AS GcobId
# MAGIC       , 'Rabobank London' AS AttributeValue
# MAGIC       , 'OneEurope' AS N2KReason
# MAGIC       , UniqueGcobId
# MAGIC     FROM radar.clients
# MAGIC     WHERE GlobalClientOwnerLocation IN ('Rabobank London')
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId AS GcobId
# MAGIC       , 'Rabobank Paris' AS AttributeValue
# MAGIC       , 'OneEurope' AS N2KReason
# MAGIC       , UniqueGcobId
# MAGIC     FROM radar.clients
# MAGIC     WHERE GlobalClientOwnerLocation IN ('Rabobank Paris')
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId AS GcobId
# MAGIC       , 'Rabobank Frankfurt' AS AttributeValue
# MAGIC       , 'OneEurope' AS N2KReason
# MAGIC       , UniqueGcobId
# MAGIC     FROM radar.clients
# MAGIC     WHERE GlobalClientOwnerLocation IN ('Rabobank Frankfurt')
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId AS GcobId
# MAGIC       , 'Rabobank Kenya' AS AttributeValue
# MAGIC       , 'OneEurope' AS N2KReason
# MAGIC       , UniqueGcobId
# MAGIC     FROM radar.clients
# MAGIC     WHERE GlobalClientOwnerLocation IN ('Rabobank Kenya')
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId AS GcobId
# MAGIC       , 'Rabobank Dublin' AS AttributeValue
# MAGIC       , 'OneEurope' AS N2KReason
# MAGIC       , UniqueGcobId
# MAGIC     FROM radar.clients
# MAGIC     WHERE GlobalClientOwnerLocation IN ('Rabobank Dublin')
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId AS GcobId
# MAGIC       , 'Rabobank Madrid' AS AttributeValue
# MAGIC       , 'OneEurope' AS N2KReason
# MAGIC       , UniqueGcobId
# MAGIC     FROM radar.clients
# MAGIC     WHERE GlobalClientOwnerLocation IN ('Rabobank Madrid')
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId AS GcobId
# MAGIC       , 'Rabobank Milan' AS AttributeValue
# MAGIC       , 'OneEurope' AS N2KReason
# MAGIC       , UniqueGcobId
# MAGIC     FROM radar.clients
# MAGIC     WHERE GlobalClientOwnerLocation IN ('Rabobank Milan')
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId AS GcobId
# MAGIC       , 'Rabobank Turkey' AS AttributeValue
# MAGIC       , 'OneEurope' AS N2KReason
# MAGIC       , UniqueGcobId
# MAGIC     FROM radar.clients
# MAGIC     WHERE GlobalClientOwnerLocation IN ('Rabobank Turkey')
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC       GcobId AS GcobId
# MAGIC       , 'Rabobank Antwerp' AS AttributeValue
# MAGIC       , 'OneEurope' AS N2KReason
# MAGIC       , UniqueGcobId
# MAGIC     FROM radar.clients
# MAGIC     WHERE GlobalClientOwnerLocation IN ('Rabobank Antwerp')
# MAGIC
# MAGIC
# MAGIC -- CAUSING SLOW DOWN BELOW!
# MAGIC   --   /*
# MAGIC   --   Building below n2k using UniqueGcobId_historical for all historical dataset to captured the right portfolio at the right point in time
# MAGIC   --   */
# MAGIC
# MAGIC   --   UNION
# MAGIC
# MAGIC   --   SELECT DISTINCT
# MAGIC   --     GcobId as GcobId
# MAGIC   --     , 'RegionRANZ' AS AttributeValue
# MAGIC   --   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   --     , 'ProductsInvolvment' AS N2KReason
# MAGIC   --     , CONCAT(UniqueGcobId, date_format(EDL_LoadDate, 'dd/MM/yyyy')) AS UniqueGcobId
# MAGIC   --   FROM radar.clients_historical
# MAGIC   --   where RANZProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved' )
# MAGIC
# MAGIC   -- UNION
# MAGIC
# MAGIC   --   SELECT DISTINCT
# MAGIC   --     GcobId as GcobId
# MAGIC   --     , 'RegionSA' AS AttributeValue
# MAGIC   --   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   --     , 'ProductsInvolvment' AS N2KReason
# MAGIC   --     , CONCAT(UniqueGcobId, date_format(EDL_LoadDate, 'dd/MM/yyyy')) AS UniqueGcobId 
# MAGIC   --   FROM radar.clients_historical
# MAGIC   --   where SAProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved' ) 
# MAGIC     
# MAGIC   --    UNION
# MAGIC
# MAGIC   --   SELECT DISTINCT
# MAGIC   --     GcobId as GcobId
# MAGIC   --   , 'RegionNA' AS AttributeValue
# MAGIC   --   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   --     , 'ProductsInvolvment' AS N2KReason
# MAGIC   --     , CONCAT(UniqueGcobId, date_format(EDL_LoadDate, 'dd/MM/yyyy')) AS UniqueGcobId 
# MAGIC   --   FROM radar.clients_historical
# MAGIC   --   where NAProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved' )  
# MAGIC     
# MAGIC   --   UNION
# MAGIC
# MAGIC   --   SELECT DISTINCT
# MAGIC   --     GcobId as GcobId
# MAGIC   --   , 'RegionAsia' AS AttributeValue
# MAGIC   --   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   --     , 'ProductsInvolvment' AS N2KReason
# MAGIC   --     , CONCAT(UniqueGcobId, date_format(EDL_LoadDate, 'dd/MM/yyyy')) AS UniqueGcobId 
# MAGIC   --   FROM radar.clients_historical
# MAGIC   --   where AsiaProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved' )  
# MAGIC     
# MAGIC   --   UNION
# MAGIC
# MAGIC   --   SELECT DISTINCT
# MAGIC   --     GcobId as GcobId
# MAGIC   --   , 'RegionEA' AS AttributeValue
# MAGIC   --   --, RANZProductsInvolvmentType AS N2KReason
# MAGIC   --     , 'ProductsInvolvment' AS N2KReason
# MAGIC   --     , CONCAT(UniqueGcobId, date_format(EDL_LoadDate, 'dd/MM/yyyy')) AS UniqueGcobId 
# MAGIC   --   FROM radar.clients_historical
# MAGIC   --   where EAProductsInvolvmentType IN ('Non-Lead - Products Involved', 'Lead - No Products', 'Lead - Products Involved' )

# COMMAND ----------

# DBTITLE 1,Exclude the id's is protected to show only authorised staff
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW N2K_ClientAttribute AS
# MAGIC SELECT DISTINCT * FROM N2KClientAttribute  WHERE NOT (GcobId IN (SELECT GcobId FROM protectedClientids) AND UniqueGcobId NOT LIKE 'NP_NPPC__%')
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC     t1.GcobId,
# MAGIC     'IsprotectedAccount' AS AttributeValue,
# MAGIC     'ProtectedAccount' AS N2KReason,
# MAGIC     CASE WHEN t1.clienttype = 'Legal Entity' THEN CONCAT('LE_', t1.GcobId) WHEN t1.clienttype IN ('Natural Person acting in a Professional Capacity (NPPC)', 'Natural Person') THEN CONCAT('NP_NPPC_', t1.GcobId) ELSE 'NA' END AS UniqueGcobId
# MAGIC FROM party_case_client_details t1
# MAGIC INNER JOIN Party_AuthorizedStaff t2 
# MAGIC     ON t2.LegalEntityClientGcobId = t1.GcobId
# MAGIC WHERE t1.IsprotectedAccount = 'True'

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS radar.N2K_ClientAttribute;
# DROP TABLE IF EXISTS radar.N2k_UserAttribute;

# COMMAND ----------

# #writting to Delta table
# spark.sql('select * from N2k_UserAttribute').write.mode('overwrite').saveAsTable('radar.N2k_UserAttribute')
# spark.sql('select * from N2K_ClientAttribute').write.mode('overwrite').saveAsTable('radar.N2K_ClientAttribute')

# COMMAND ----------

df_N2k_UserAttribute = spark.table('N2k_UserAttribute')
save_to_saradar_storage_account(df_N2k_UserAttribute, N2k_UserAttribute_dataobject)

# COMMAND ----------

df_N2K_ClientAttribute = spark.table('N2K_ClientAttribute')
save_to_saradar_storage_account(df_N2K_ClientAttribute, N2K_ClientAttribute_dataobject)
