# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal:
# MAGIC To provide the right teams with the right access to client scopes
# MAGIC
# MAGIC ### Systems referenced
# MAGIC - GCDS:
# MAGIC   - GCO and GCO Location and Local CO location
# MAGIC - AMS: GCO
# MAGIC - Radar region-groups + GCDS
# MAGIC
# MAGIC ### output:
# MAGIC - One table User-Attribute
# MAGIC   - UPN
# MAGIC   - Attribute attached to user that can be connected to a scope of GCID's
# MAGIC - One table Attribute-Client
# MAGIC   - Attribute
# MAGIC   - Description of how this attribute is involved with this specific GCID.
# MAGIC   - GCID

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up Connections

# COMMAND ----------

# DBTITLE 1,import libraries
import requests 
import os
from datetime import datetime
import http.client
import json
import requests
import sys
import pandas as pd
from pyspark.sql.functions import col,lit
from pyspark.sql.types import *
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,import variables
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
env = os.environ['ENV']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# DBTITLE 1,GCDS tables
import re

from pyspark.sql import SparkSession
# load and create temp views of all gdp_tables below
gcds_tables = [
    'client_KeyStoreKey'
    , 'client_Client'
    , 'client_PartyRole'
    , 'client_Products'
    , 'client_OnboardedLocations'
]

for item in gcds_tables:
  # get the most recent version available in gdp
  path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/'
  files = dbutils.fs.ls(path)
  version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
  
  # get the most recent file available in gdp
  path_file = f'{path}{version}/data/'
  files = dbutils.fs.ls(path_file)
  load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

  spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data/LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcds_'+ item)

# COMMAND ----------

# DBTITLE 1,GCOB tables
from datetime import datetime, timedelta
import re

gcob_objects = [
    'party_client'
    , 'party_case_client_details'
    , 'party_local_client_Owners'
    , 'party_products_and_services'
    , 'party_GcobUsers'
]

for item in gcob_objects:
# get the most recent version available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/'
    files = dbutils.fs.ls(path)
    version = max([
        int(re.search(r'/(\d+)/$', file.path).group(1))
        for file in files
        if re.search(r'/(\d+)/$', file.path)
    ])

    version = 102 # TEMPORARY UNTIL THE NEW VERSION IS UP AND RUNNING!!!!


    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOAD_DT=')[1][:8] for file in files if 'LOAD_DT=' in file.path)

    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/LOAD_DT={load_date}*/*.parquet').createOrReplaceTempView(item)

BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
EDL_LoadDate = (datetime.today()).strftime("%Y-%m-%dT%H:%m:%mZ")

# COMMAND ----------

# DBTITLE 1,set AMS variables
graph_scope = 'https://graph.microsoft.com/.default'
graph_base_url = 'graph.microsoft.com'

if env == 'preprd':
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
elif env == 'prod':
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

# Use http request to generate auth token
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
ams_resp_json = json.loads(result.read())
#print(ams_resp_json)
AMSreqdf = pd.DataFrame.from_dict(ams_resp_json['user_details'], orient='columns')
df_spark = spark.createDataFrame(AMSreqdf)
#display(df_spark)
df_ams=df_spark.select(col("user_id"),col("upn"),explode("roles").alias("rolesdetails"),col("rolesdetails.id"),col("rolesdetails.role_id"),col("rolesdetails.context"),col("rolesdetails.context.client")).drop("rolesdetails")
df_ams.createOrReplaceTempView('AMS_API')

# COMMAND ----------

# %sql
# select * from AMS_API where role_id in ('GC-COS')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Graph API for RADAR Groups

# COMMAND ----------

# DBTITLE 1,Get token for Graph / AAD API
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

# Use http request to generate auth token
resp = requests.post(
    url="https://login.microsoftonline.com/6e93a626-8aca-4dc1-9191-ce291b4b75a1/oauth2/v2.0/token",
    headers=headers,
    data=values,
)

# call graph api
token = json.loads(resp.text)["access_token"]
header_info = {
    "Authorization": f"Bearer {token}"}

# COMMAND ----------

# DBTITLE 1,Radar Region groups
AADRadargrp = requests.get("https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName,'eu.aut.AADRadar')", headers = header_info) #?
AADRadarGroups = json.loads(AADRadargrp.text)
AADReturnGrpradar = pd.DataFrame.from_dict(AADRadarGroups['value'], orient='columns')
AADreturnGrp = spark.createDataFrame(AADReturnGrpradar)

# capturing users in both aad group Region and Department!!
RadarRegions = AADreturnGrp.filter(AADreturnGrp.displayName.contains("Region") | AADreturnGrp.displayName.contains("Department")).toPandas()

# COMMAND ----------

# DBTITLE 1,Get members from Region groups Entra ID
#-- For AADRadar groups and related information
df_AllAADRadarGroupUsers = pd.DataFrame()
for index, group in RadarRegions.iterrows():
    id=group['id']
    gpname=group['displayName']    
    url="https://graph.microsoft.com/v1.0/groups/"+id+"/members?$count=true"
    #print(url)
    #print(gpname)
    while url:
                        
        try:
            #request group members from entra ID
            AADresult = requests.get(url=url, headers=header_info).json()
            UsersPerGroupDf = pd.DataFrame.from_dict(AADresult['value'], orient='columns')
            UsersPerGroupDf['GroupName'] = gpname

            # Append on the all groupds dataframe
            df_AllAADRadarGroupUsers=pd.concat([df_AllAADRadarGroupUsers,UsersPerGroupDf])
            url = AADresult['@odata.nextLink']
        
        except:
            break

# COMMAND ----------

spark.createDataFrame(df_AllAADRadarGroupUsers).createOrReplaceTempView('RadarRegionGroups')

# COMMAND ----------

# DBTITLE 1,Region User Info
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
# MAGIC       -- WHEN GroupName = 'eu.aut.AADRadarCashMTOInsights.us' THEN 'MTO'
# MAGIC       -- WHEN GroupName = 'eu.aut.AADFECRadarMTODashboardEUA.us' THEN 'MTO'
# MAGIC       WHEN GroupName = 'eu.aut.AADRadarRegionFoundation.us' THEN 'RegionFoundation'
# MAGIC       WHEN GroupName = 'eu.aut.AADRadarDepartmentTCF.us' THEN 'DepartmentTCF'
# MAGIC       ELSE NULL
# MAGIC     END AS AttributeValue
# MAGIC FROM RadarRegionGroups
# MAGIC WHERE GroupName IN ('eu.aut.AADRadarRegionAsia.us','eu.aut.AADRadarRegionEuropeAfrica.us','eu.aut.AADRadarRegionNA.us','eu.aut.AADRadarRegionRANZ.us','eu.aut.AADRadarRegionSA.us','eu.aut.AADRadarCashMTOInsights.us', 'eu.aut.AADRadarRegionFoundation.us', 'eu.aut.AADFECRadarMTODashboardEUA.us', 'eu.aut.AADRadarDepartmentTCF.us')

# COMMAND ----------

# DBTITLE 1,Connect Groups to GCDS GCIDS
# %sql
# select * from gcds_client_OnboardedLocations 
# limit 20

# COMMAND ----------

# %sql
# select distinct Branch_code, Branche_name from gcds_client_OnboardedLocations order by Branch_code

# COMMAND ----------

#list(spark.sql('select distinct Branch_code from gcds_client_OnboardedLocations order by Branch_code').toPandas()['Branch_code'])
#%sql
#select distinct Branch_code from gcds_client_OnboardedLocations order by Branch_code

# COMMAND ----------

Branch_Region_Mapping = {'ARG': 'RegionEA'
                         ,  'ATL':'RegionNA' 
                         ,  'AU' :'RegionNA' 
                         ,  'AUS':'RegionNA' 
                         ,  'BEL': 'RegionEA'
                         ,  'BRA':'RegionSA'
                         ,  'CAN':'RegionNA'
                         ,  'CHL':'RegionSA'
                         ,  'CHN':'RegionAsia'
                         ,  'DEU':'RegionEA'
                         ,  'ESP':'RegionEA'
                         ,  'FRA':'RegionEA'
                         ,  'GBR':'RegionEA'
                         ,  'HKG':'RegionAsia'
                         ,  'IDN':'RegionAsia'
                         ,  'IND':'RegionAsia'
                         ,  'IRL':'RegionEA'
                         ,  'ITA':'RegionEA'
                         ,  'KEN':'RegionEA'
                         ,  'MEX':'RegionNA' 
                         ,  'MYS':'RegionAsia'
                         ,  'NEY':'RegionNA' 
                         ,  'NLM':'RegionEA'
                         ,  'NZ' :'RegionRANZ' 
                         ,  'NZL':'RegionRANZ' 
                         ,  'RAF':'RegionNA' 
                         ,  'RSC':'RegionNA'
                         ,  'RSU':'RegionNA'
                         ,  'RUS':'RegionAsia'
                         ,  'SGP':'RegionAsia'
                         ,  'SHA':'RegionAsia'
                         ,  'TUR':'RegionEA'
                         ,  'UTR':'RegionEA'}

df_brm = pd.DataFrame.from_dict(Branch_Region_Mapping, orient= 'index')#, columns= ['Branch_code', 'Region_Name']  )
df_brm.reset_index(inplace = True)
df_brm.columns= ['Branch_code', 'Region_Name'] 
spark.createDataFrame(df_brm).createOrReplaceTempView('BranchRegionMapping')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCDS_AttributeClient AS
# MAGIC Select t1.GCID
# MAGIC   --, t1.Branch_code
# MAGIC   , 'OnboardedLocation' AS `N2KReason`
# MAGIC   , t2.Region_name AS AttributeValue
# MAGIC   
# MAGIC from gcds_client_OnboardedLocations as t1
# MAGIC left join BranchRegionMapping AS t2 on t1.Branch_code = t2.Branch_code

# COMMAND ----------

# DBTITLE 1,GCDS client attribute based on CO location from client_client
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCDS_AttributeClient_CO AS
# MAGIC
# MAGIC SELECT
# MAGIC   GCID
# MAGIC   , 'COLocation' AS N2KReason
# MAGIC   , CASE
# MAGIC       WHEN `Global_CO-location` IN ('Utrecht', 'Germany', 'France', 'Belgium', 'London', 'Italy', 'Spain', 'Ireland', 'Kenya') THEN 'RegionEA'
# MAGIC       WHEN `Global_CO-location` IN ('China', 'Singapore', 'Malaysia', 'Beijing', 'India', 'Hong Kong', 'Shanghai', 'SINGAPORE') THEN 'RegionAsia'
# MAGIC       WHEN `Global_CO-location` IN ('San Francisco', 'Chicago', 'Atlanta', 'Mexico', 'Canada', 'New York') THEN 'RegionNA'
# MAGIC       WHEN `Global_CO-location` IN ('Argentina', 'Chile', 'Brazil') THEN 'RegionSA'
# MAGIC       WHEN `Global_CO-location` IN ('Australia', 'New Zealand', 'RAF') THEN 'RegionRANZ'
# MAGIC       ELSE 'Other'
# MAGIC     END AS AttributeValue
# MAGIC   
# MAGIC FROM gcds_client_client

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Facility_Regions_Limit AS
# MAGIC -- all planet regions
# MAGIC SELECT DISTINCT
# MAGIC   facilityCodeSerial
# MAGIC   , 'FacilityRegion' AS N2KReason
# MAGIC   , portfolioName
# MAGIC   , gcid
# MAGIC   , CASE
# MAGIC       WHEN portfolioName LIKE 'Buenos Aires%' OR portfolioName LIKE 'Lima%' OR portfolioName LIKE 'Santiago%' OR portfolioName LIKE 'Sao Paulo%' THEN 'RegionSA'
# MAGIC       WHEN portfolioName LIKE 'Hong Kong%' OR portfolioName LIKE 'Mumbai%' OR portfolioName LIKE 'Shanghai%' OR portfolioName LIKE 'Singapore%' THEN 'RegionAsia'
# MAGIC       ELSE 'RegionEA'
# MAGIC     END AS AttributeValue
# MAGIC FROM radar.facility_limit 

# COMMAND ----------

# DBTITLE 1,Planet specific client attribute
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Planet_AttributeClient AS
# MAGIC -- all planet regions
# MAGIC SELECT DISTINCT
# MAGIC   GCID
# MAGIC   , 'PlanetRegion' AS N2KReason
# MAGIC   , CASE
# MAGIC       WHEN `Region` = 'Europe & Africa' THEN 'RegionEA'
# MAGIC       WHEN `Region` = 'North America' THEN 'RegionNA'
# MAGIC       WHEN `Region` = 'Australia & New Zealand' THEN 'RegionRANZ'
# MAGIC       WHEN `Region` = 'South America' THEN 'RegionSA'
# MAGIC       WHEN `Region` = 'Asia' THEN 'RegionAsia'
# MAGIC     END AS AttributeValue
# MAGIC FROM radar.planetclients
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC -- specific for TCF business lines
# MAGIC SELECT DISTINCT
# MAGIC   GCID
# MAGIC   , 'PlanetTCF' AS N2KReason
# MAGIC   , CASE
# MAGIC       WHEN BusinessLine LIKE '%Trade%' THEN 'DepartmentTCF'
# MAGIC   END AS AttributeValue
# MAGIC FROM radar.planetclients
# MAGIC WHERE BusinessLine LIKE '%Trade%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bring everything Together
# MAGIC From each source create 1) the UserAttribute, and 2) AttributeGCID

# COMMAND ----------

# DBTITLE 1,N2KUserAttribute
df_UserAttribute = spark.sql("""
    -- Radar Groups
    SELECT DISTINCT
        UPN
        , AttributeType
        , AttributeValue
    FROM
    Region_UserInfo
 
--     UNION
 
--     -- AMS
--    SELECT DISTINCT
--         ams_api.Upn AS UPN
--         , ams_api.Role_Id AS `AttributeType`
--         --ams_api.client as GCDSID,
--         , ams_api.Upn AS AttributeValue
--     from  AMS_API as ams_api
--     where ams_api.Role_Id='GCEO' 
 
""")

df_UserAttribute.createOrReplaceTempView('N2k_UserAttribute')

# COMMAND ----------

# DBTITLE 1,N2KClientAttribute
df_AttributeClient = spark.sql("""
    -- AMS
        -- SELECT DISTINCT
        --     Upn AS AttributeValue
        --     , role_id AS `N2KReason`
        --     , client AS GCID

        -- FROM AMS_API

    -- UNION 
    

    -- REMOVED ATTRIBUTE CLIENT BASED ON ONBOARDING IN GCDS, INCORRECT!

    -- -- GCDS
    --     SELECT DISTINCT
    --         AttributeValue
    --         , N2KReason
    --         , GCID
    --     FROM GCDS_AttributeClient

    -- REMOVED BELOW BECASUE GCDS CO LOCATION GIVE DIFFERENT RESULTS ON REGION WRT PLANET!!!!!!!!!!!!
    -- UNION

    --     SELECT DISTINCT
    --         AttributeValue
    --         , N2KReason
    --         , GCID
    --     FROM GCDS_AttributeClient_CO

    -- UNION

    -- Planet specific
        SELECT
            AttributeValue
            , N2KReason
            , GCID
        FROM Planet_AttributeClient
    """)

df_AttributeClient.createOrReplaceTempView('N2K_ClientAttribute')

# COMMAND ----------

# DBTITLE 1,N2KClientAttribute Facility
df_AttributeClient = spark.sql("""
    -- AMS
        -- SELECT DISTINCT
        --     Upn AS AttributeValue
        --     , role_id AS `N2KReason`
        --     , client AS GCID

        -- FROM AMS_API

    -- UNION 
    

    -- REMOVED ATTRIBUTE CLIENT BASED ON ONBOARDING IN GCDS, INCORRECT!

    -- -- GCDS
    --     SELECT DISTINCT
    --         AttributeValue
    --         , N2KReason
    --         , GCID
    --     FROM GCDS_AttributeClient

    -- REMOVED BELOW BECASUE GCDS CO LOCATION GIVE DIFFERENT RESULTS ON REGION WRT PLANET!!!!!!!!!!!!
    -- UNION

    --     SELECT DISTINCT
    --         AttributeValue
    --         , N2KReason
    --         , GCID
    --     FROM GCDS_AttributeClient_CO

    -- UNION
        
    SELECT AttributeValue
        , N2KReason
        , facilityCodeSerial
        , gcid
    FROM Facility_Regions_Limit

    """)

df_AttributeClient.createOrReplaceTempView('N2K_FacilityClientAttribute')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.N2K_GCID_ClientAttribute;
# MAGIC DROP TABLE IF EXISTS radar.N2k_GCID_UserAttribute;
# MAGIC DROP TABLE IF EXISTS radar.N2K_FacilityClientAttribute;

# COMMAND ----------

#writting to Delta table
spark.sql('select * from N2k_UserAttribute').write.mode('overwrite').saveAsTable('radar.N2k_GCID_UserAttribute')
spark.sql('select * from N2K_ClientAttribute').write.mode('overwrite').saveAsTable('radar.N2K_GCID_ClientAttribute')
spark.sql('select * from N2K_FacilityClientAttribute').write.mode('overwrite').saveAsTable('radar.N2K_FacilityClientAttribute')

# COMMAND ----------

# %sql
# -- amount of parties PER region
# select count(distinct(GCID)) AS count
# , AttributeValue 
# FROM radar.N2k_GCID_ClientAttribute
# GROUP BY AttributeValue
# order by count(distinct(GCID)) desc
