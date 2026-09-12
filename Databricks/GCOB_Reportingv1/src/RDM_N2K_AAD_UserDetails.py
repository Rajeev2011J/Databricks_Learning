# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To Achive Needtoknow Access (N2k)for Radar Datamodel
# MAGIC
# MAGIC #### author
# MAGIC - Prasad.Gadidala@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Get User details from AADGroup using graph API
# MAGIC
# MAGIC ##### Expected output
# MAGIC   - One table  for User-Attribute
# MAGIC UPN
# MAGIC Attribute and Region
# MAGIC - One table for Attribute-Client
# MAGIC Attribute Type and partyidentifier
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,import libraries
import requests 
import os
import time
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

# DBTITLE 1,import variables
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# DBTITLE 1,Read Objects from GDP Defined storage
# print list of strings for loading spark dfs from GDP
load_df = [
'Party',
'Party_Coverage',
'Party_ClientOwnership'
]

# Create TempView for each loading table
for Object in load_df:
# <<<<<<< HEAD
    Read_GDP_Defined_DataObjects(Source='RadarDataModel', Dataobject=Object, path_prefix='RadarDataModel')
# =======
#     Read_GDP_Defined_DataObjects(Source='RadarDataModel', Dataobject=Object, path_prefix='RadarDataModel', version_num=1)
# >>>>>>> origin/development

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

# Security limits for group discovery
MAX_GROUPS_PER_PREFIX = 100  # Maximum number of groups to process per prefix
MAX_TOTAL_GROUPS = 500  # Maximum total groups across all prefixes

group_prefixes = [
    "eu.aut.AADRadarRegionAsia.us",
    "eu.aut.AADRadarRegionGlobalFI.us",
    "eu.aut.AADRadarRegionNA.us",
    "eu.aut.AADRadarRegionEuropeAfrica.us",
    "eu.aut.AADRadarRegionRANZ.us",
    "eu.aut.AADRadarRegionSA.us"
]

total_groups_processed = 0

for prefix in group_prefixes:

    print("\nChecking groups starting with:", prefix)
    # Check if we've exceeded total limit
    if total_groups_processed >= MAX_TOTAL_GROUPS:
        print(f"[WARN] Reached maximum total groups limit ({MAX_TOTAL_GROUPS}). Stopping discovery.")
        break
    
    try:
        # Your original call — just made dynamic
        allSAgroupusers = requests.get(
            f"https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName,'{prefix}')",
            headers=header_info,
            timeout=30  # Add timeout to prevent hanging requests
        )
        allSAgroupusers.raise_for_status()
        
        SALocationGroup = json.loads(allSAgroupusers.text)
        
        # Validate response structure
        if not isinstance(SALocationGroup, dict):
            raise ValueError(f"Unexpected API response format for prefix {prefix}")
        
        groups = SALocationGroup.get("value", [])
        
        # Validate array size before iteration
        if not isinstance(groups, list):
            raise ValueError(f"Expected 'value' to be a list, got {type(groups)}")
        
        if len(groups) > MAX_GROUPS_PER_PREFIX:
            raise ValueError(
                f"Response contains {len(groups)} groups for prefix '{prefix}', "
                f"exceeding limit of {MAX_GROUPS_PER_PREFIX}. Potential DoS attack or misconfiguration."
            )
        
        # Check if processing these groups would exceed total limit
        remaining_capacity = MAX_TOTAL_GROUPS - total_groups_processed
        groups_to_process = groups[:remaining_capacity]  # Only process what fits within the limit
        
        if len(groups_to_process) < len(groups):
            print(f"[WARN] Truncating to {len(groups_to_process)} groups to stay within total limit")
        
        # Loop groups returned (now with validated size)
        for grp in groups_to_process:
            if not isinstance(grp, dict):
                print(f"[WARN] Skipping invalid group entry: {grp}")
                continue
            print("Group Name:", grp.get("displayName"))
            print("Group ID:", grp.get("id"))
            total_groups_processed += 1
        
        print(f"Processed {len(groups_to_process)} groups for prefix '{prefix}' (Total: {total_groups_processed})")
    
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed for prefix '{prefix}': {e}")
        continue
    except (ValueError, KeyError, json.JSONDecodeError) as e:
        print(f"[ERROR] Invalid response for prefix '{prefix}': {e}")
        continue

print(f"\n[INFO] Group discovery complete. Total groups processed: {total_groups_processed}")

# COMMAND ----------

# DBTITLE 1,get the user details from AAD region groups
# Define Target Azure AD Groups
EXPLICIT_GROUP_NAMES = [
    "eu.aut.AADRadarRegionAsia.us",
    "eu.aut.AADRadarRegionGlobalFI.us",
    "eu.aut.AADRadarRegionNA.us",
    "eu.aut.AADRadarRegionEuropeAfrica.us",
    "eu.aut.AADRadarRegionRANZ.us",
    "eu.aut.AADRadarRegionSA.us"
]

GRAPH = "https://graph.microsoft.com/v1.0"
header_info = {
    "Authorization":  f"Bearer {token}",
    "Content-Type": "application/json"
}

# Pagination Handler with safety limits
def get_all_pages(url, headers, max_pages=100, max_items_per_page=1000, max_total_items=10000):
    """
    Fetch paginated results with validation to prevent DoS attacks.
    
    Args:
        url: Initial URL to fetch
        headers: Request headers
        max_pages: Maximum number of pages to fetch (default: 100)
        max_items_per_page: Maximum items per page (default: 1000)
        max_total_items: Maximum total items to collect (default: 10000)
    
    Returns:
        List of results
    
    Raises:
        ValueError: If limits are exceeded
    """
    results = []
    page_count = 0
    
    while url:
        # Check page limit
        page_count += 1
        if page_count > max_pages:
            raise ValueError(f"Exceeded maximum page limit of {max_pages}. Potential DoS attack or misconfiguration.")
        
        r = requests.get(url, headers=headers)
        if r.status_code == 429:
            # Validate retry-after to prevent excessive waits
            retry_after = r.headers.get("Retry-After", "5")
            wait = int(retry_after)
            if wait > 300:  # Don't wait more than 5 minutes
                raise ValueError(f"Retry-After value too large: {wait} seconds")
            time.sleep(wait)
            continue
        r.raise_for_status()
        data = r.json()
        
        # Validate page data size
        page_items = data.get("value", [])
        if len(page_items) > max_items_per_page:
            raise ValueError(f"Page contains {len(page_items)} items, exceeding limit of {max_items_per_page}")
        
        # Check total size before extending
        if len(results) + len(page_items) > max_total_items:
            raise ValueError(f"Total items would exceed limit of {max_total_items}. Retrieved {len(results)} so far.")
        
        results.extend(page_items)
        url = data.get("@odata.nextLink")
    
    return results

# Resolve Group ID From Name
def resolve_group_by_name(name: str):
   
    url = f"{GRAPH}/groups?$select=id,displayName&$filter=displayName eq '{name}'"
    items = get_all_pages(url, header_info)
    return items[0] if items else None

# Get All Transitive Members
def get_group_users_transitive(group_id: str):
    # MUST NOT include @odata.type in $select
    url = (
        f"{GRAPH}/groups/{group_id}/transitiveMembers"
        f"?$select=id,displayName,userPrincipalName,mail,userType,givenName,companyName,country"
    )
    members = get_all_pages(url, header_info)

    users, seen = [], set()
    for m in members:
        if m.get("@odata.type", "").lower().endswith(".user"):
            if m["id"] not in seen:
                seen.add(m["id"])
                users.append({
                    "id": m["id"],
                    "groupDisplayName": group_id,
                    "displayName": m.get("displayName"),
                    "givenName": m.get("givenName"),
                    "userPrincipalName": m.get("userPrincipalName"),
                    "companyName": m.get("companyName"),
                    "country": m.get("country")
                })
    return users

# Remove prefix and suffix
def extract_region(group_name: str):
    return group_name.replace("eu.aut.AADRadar", "").replace(".us", "")

# -------------------------------
# COLLECT ALL GROUP USERS 
# Collect All Users From All Groups
# -------------------------------
all_rows = []

for group_name in EXPLICIT_GROUP_NAMES:
    grp = resolve_group_by_name(group_name)
    if not grp:
        print(f"[WARN] Group not found: {group_name}")
        continue

    users = get_group_users_transitive(grp["id"])

    for u in users:
        all_rows.append({
            "groupName": extract_region(group_name),
            "displayName": u.get("displayName"),
            "givenName": u.get("givenName"),
            "userPrincipalName": u.get("userPrincipalName"),
            "companyName": u.get("companyName"),
            "country": u.get("country")
        })

# Convert to Pandas -> Spark DataFrame
pdf = pd.DataFrame(all_rows)
dfUserAttribute = spark.createDataFrame(pdf)
dfUserAttribute.createOrReplaceTempView("group_users_raw")
# Display result in Databricks
#display(dfUserAttribute)

# COMMAND ----------

# DBTITLE 1,Create radarN2K_UserAttribute table
dfUserAttribute = spark.sql("""
    SELECT DISTINCT
        userPrincipalName as UPN
        , groupName as AttributeValue
        , companyName as AttributeName
    FROM group_users_raw
    """)
dfUserAttribute.createOrReplaceTempView('radarN2K_UserAttribute')

# COMMAND ----------

# Define the mapping between RegionName and LocationISOCode
mapping = {
    'Asia': ['HK','CN','ID','IN','JP','KR','MY','SG','TW','TH','TR','VN'],
    'Australia Pacific': ['AU', 'NZ'],
    'Europe': ['BE', 'DE', 'ES','GB','FR','HU','IE','IT','KE','PL','NL','RU','ZA'],
    'North America': ['US', 'CA','MX'],
    'South America': ['AR', 'BR', 'CL', 'CW', 'PE']
}

# Convert the mapping dictionary to a list of tuples
data = [(region, iso) for region, isos in mapping.items() for iso in isos]
df = spark.createDataFrame(data, ["RegionName", "LocationISOCode"])
df.createOrReplaceTempView('RegionISO_static_table')

GCO_DF=spark.sql("""select pco.PartyIdentifier, CASE
    WHEN st.RegionName = 'Europe' THEN 'RegionEuropeAfrica'
    WHEN st.RegionName = 'Asia' THEN 'RegionAsia'
    WHEN st.RegionName ='North America'THEN 'RegionNA'
    WHEN st.RegionName ='South America'THEN'RegionSA'
    When st.RegionName= 'Australia Pacific' THEN 'RegionRANZ'
  END AS RegionGroup from Party_ClientOwnership pco 
                 left outer join RegionISO_static_table st 
                 on st.LocationISOCode=pco.ClientOwnerLocationCountryISOCode and pco.ClientOwnerType='GlobalClientOwner' """)
GCO_DF.createOrReplaceTempView("GCO_location")             

# COMMAND ----------

# DBTITLE 1,Party coverage query to get the Location based clients
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW involed_location AS
# MAGIC SELECT DISTINCT pc.PartyIdentifier,
# MAGIC   CASE
# MAGIC     WHEN pc.RegionName = 'Europe' THEN 'RegionEuropeAfrica'
# MAGIC     WHEN pc.RegionName = 'Asia' THEN 'RegionAsia'
# MAGIC     WHEN pc.RegionName ='North America'THEN 'RegionNA'
# MAGIC     WHEN pc.RegionName ='South America'THEN'RegionSA'
# MAGIC     When pc.RegionName= 'Australia Pacific' THEN 'RegionRANZ'
# MAGIC   END AS RegionGroup
# MAGIC   from party_coverage pc
# MAGIC   where pc.CoverageTypeDescription in ('Involved Location')
# MAGIC

# COMMAND ----------

# MAGIC  %sql
# MAGIC create or replace temporary view radarcoveragefinal as
# MAGIC select distinct PartyIdentifier, RegionGroup from involed_location
# MAGIC union
# MAGIC select distinct PartyIdentifier, RegionGroup from GCO_location

# COMMAND ----------

# DBTITLE 1,Join location nd user tables to get final table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RDM_N2KClientUserAttribute AS
# MAGIC SELECT DISTINCT rc.PartyIdentifier,rc.RegionGroup as AttributeValue
# MAGIC FROM radarcoveragefinal rc
# MAGIC Inner join radarN2K_UserAttribute ua on ua.AttributeValue =rc.RegionGroup

# COMMAND ----------

RDM_N2kClientUserattribute_dataobject = 'RDM_N2KClientUserAttribute'

# COMMAND ----------

# DBTITLE 1,Load data object into SA Radar
df_N2KClientUserAttribute = spark.table("RDM_N2KClientUserAttribute")
df_N2KClientUserAttribute=df_N2KClientUserAttribute.withColumn("RefreshDate",lit(BusinessDate))
save_to_saradar_storage_account(df_N2KClientUserAttribute, RDM_N2kClientUserattribute_dataobject)
