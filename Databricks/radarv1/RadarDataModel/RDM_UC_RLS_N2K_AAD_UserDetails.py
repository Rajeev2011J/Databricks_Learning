# Databricks notebook source
# DBTITLE 1,Importing Python & Spark Libraries
import requests,time 
import os
from datetime import datetime, timedelta
import http.client
import json
import requests
import sys
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from RadarUtils import *
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# DBTITLE 1,Initialize Date Variables
date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

# DBTITLE 1,Load Secrets & Environment Variables
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
SARADAR = "saradar" + environment

# COMMAND ----------

# DBTITLE 1,Authenticate to Data Storage
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Reusable function to read/write any UC table
def read_uc_table(catalog, schema, table):
    return spark.table(f"{catalog}.{schema}.{table}")

def write_to_unity_catalog(
    df: DataFrame,
    catalog: str,
    schema: str,
    table: str,
    mode: str = "overwrite",
    partition_by: list = None,
    comment: str = None
):
    """
    Writes a Spark DataFrame to a Unity Catalog table.
    
    Parameters:
        df (DataFrame)        : Input Spark DataFrame
        catalog (str)         : Unity Catalog catalog name
        schema (str)          : Schema name inside catalog
        table (str)           : Target table name
        mode (str)            : write mode: overwrite | append | errorIfExists | ignore
        partition_by (list)   : Optional list of partition columns
        comment (str)         : Optional table comment
    
    Returns:
        str : Full table name written.
    """
    
    full_table_name = f"{catalog}.{schema}.{table}"

    # 1 — Create schema if not exists
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    except Exception as e:
        print(f"Schema creation failed: {e}")

    # 2 — Write the data
    writer = df.write.format("delta").mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.saveAsTable(full_table_name)

    # 3 — Add table comment if provided
    if comment:
        spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{comment}'")

    print(f"Successfully written to Unity Catalog table: {full_table_name}")
    return full_table_name

# COMMAND ----------

# DBTITLE 1,Load Party
df_Party = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party/1/data/{load_dts}/*.parquet"
)

write_to_unity_catalog(
    df=df_Party,
    catalog="wr_fj_parties_and_risk_assessment_preprd",
    schema="radar",
    table="Party",
    mode="overwrite",
    comment="Party Data Object"
)

df_party = read_uc_table("wr_fj_parties_and_risk_assessment_preprd", "radar", "Party")
display(df_party)

# COMMAND ----------

# DBTITLE 1,Load Party Address
df_Party_address = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_Address/1/data/{load_dts}/*.parquet"
)

write_to_unity_catalog(
    df=df_Party_address,
    catalog="wr_fj_parties_and_risk_assessment_preprd",
    schema="radar",
    table="Party_Address",
    mode="overwrite",
    comment="Party Address Data Object"
)

PartyAddress_df = read_uc_table("wr_fj_parties_and_risk_assessment_preprd", "radar", "Party_Address")
display(PartyAddress_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS wr_fj_parties_and_risk_assessment_preprd.radar.Party_Naics;

# COMMAND ----------

# DBTITLE 1,Load Party_Naics
df_Party_Naics =spark.read.parquet(
    "abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_Naics/1/data/{load_dts}/*.parquet"
)

write_to_unity_catalog(
    df=df_Party_address,
    catalog="wr_fj_parties_and_risk_assessment_preprd",
    schema="radar",
    table="Party_Naics",
    mode="overwrite",
    comment="Party Naics Data Object"
)

Party_Naics_df = read_uc_table("wr_fj_parties_and_risk_assessment_preprd", "radar", "Party_Naics")
display(Party_Naics_df)

# COMMAND ----------

# DBTITLE 1,Show all UC tables
spark.sql("SHOW TABLES IN wr_fj_parties_and_risk_assessment_preprd.radar").show()


# COMMAND ----------

# DBTITLE 1,Microsoft Graph Authentication Setup
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
    "Authorization": f"Bearer {token}",
}

url = 'graph.microsoft.com'

# COMMAND ----------

# DBTITLE 1,Helper Functions to Call Microsoft Graph API
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

# Pagination Handler
def get_all_pages(url, headers):
    results = []
    while url:
        r = requests.get(url, headers=headers)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "5"))
            time.sleep(wait)
            continue
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("value", []))
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
df = spark.createDataFrame(pdf)
df.createOrReplaceTempView("group_users_raw")
# Display result in Databricks
display(df)

# COMMAND ----------

# DBTITLE 1,SQL Mapping of Region Names
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW All_users AS
# MAGIC SELECT DISTINCT 
# MAGIC   userPrincipalName AS UPN
# MAGIC   , CASE
# MAGIC       WHEN groupName = 'RegionAsia' THEN 'Asia'
# MAGIC       WHEN groupName = 'RegionGlobalFI' THEN 'Global'
# MAGIC       WHEN groupName = 'RegionNA' THEN 'NorthAmerica'
# MAGIC       WHEN groupName = 'RegionEuropeAfrica' THEN 'EuropeAfrica'
# MAGIC       WHEN groupName = 'RegionRANZ' THEN 'RANZ'
# MAGIC       WHEN groupName = 'RegionSA' THEN 'SouthAmerica'
# MAGIC       ELSE NULL 
# MAGIC     END AS AttributeType
# MAGIC   , groupName AS AttributeValue
# MAGIC FROM group_users_raw

# COMMAND ----------

# DBTITLE 1,Generates the user attribute table.
df_UserAttribute = spark.sql("""
    SELECT DISTINCT
        UPN
        , AttributeType
        , AttributeValue
    FROM All_users
    """)
df_UserAttribute.createOrReplaceTempView('radarN2K_UserAttribute')

# COMMAND ----------

RDM_N2kUserattribute_dataobject = 'RDM_N2KUserAttribute'

# COMMAND ----------

df_UserAttribute = spark.table("radarN2K_UserAttribute")
df_UserAttribute=df_UserAttribute.withColumn("RefreshDate",lit(BusinessDate))

write_to_unity_catalog(
    df=df_UserAttribute,
    catalog="wr_fj_parties_and_risk_assessment_preprd",
    schema="radar",
    table="RDM_N2KUserAttribute",
    mode="overwrite",
    comment="radarN2K_UserAttribute"
)

RDM_N2KUserAttribute_df = read_uc_table("wr_fj_parties_and_risk_assessment_preprd", "radar", "RDM_N2KUserAttribute")
display(RDM_N2KUserAttribute_df)

# COMMAND ----------

df_UserAttribute.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Row Filter Function

# COMMAND ----------

# DBTITLE 1,Create a Lookup Table: location_region_lookup
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS wr_fj_parties_and_risk_assessment_preprd.radar.location_region_lookup;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS wr_fj_parties_and_risk_assessment_preprd.radar.location_region_lookup (
# MAGIC   GlobalClientOwnerLocation STRING,
# MAGIC   Region STRING
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Insert mappings based on business logic:
# MAGIC %sql
# MAGIC INSERT INTO wr_fj_parties_and_risk_assessment_preprd.radar.location_region_lookup (GlobalClientOwnerLocation, Region) VALUES
# MAGIC ('Rabobank Indonesia', 'RegionAsia'),
# MAGIC ('Singapore', 'RegionAsia'),
# MAGIC ('Malaysia', 'RegionAsia'),
# MAGIC ('Turkey', 'RegionEuropeAfrica'),
# MAGIC ('Beijing', 'RegionAsia'),
# MAGIC ('Germany', 'RegionEuropeAfrica'),
# MAGIC ('Rabobank New York', 'RegionNA'),
# MAGIC ('France', 'RegionEuropeAfrica'),
# MAGIC ('Rabobank Netherlands', 'RegionEuropeAfrica'),
# MAGIC ('Rabobank London', 'RegionEuropeAfrica'),
# MAGIC ('null', 'RegionGlobalFI'),
# MAGIC ('San Francisco', 'RegionNA'),
# MAGIC ('Argentina', 'RegionSA'),
# MAGIC ('Belgium', 'RegionEuropeAfrica'),
# MAGIC ('Rabobank India', 'RegionAsia'),
# MAGIC ('Rabobank Paris', 'RegionEuropeAfrica'),
# MAGIC ('?? (???????)', 'RegionGlobalFI'),
# MAGIC ('London', 'RegionEuropeAfrica'),
# MAGIC ('India', 'RegionAsia'),
# MAGIC ('China', 'RegionAsia'),
# MAGIC ('Chile', 'RegionSA'),
# MAGIC ('"Rabo Securities USA, Inc. (RSEC)"', 'RegionNA'),
# MAGIC ('Italy', 'RegionEuropeAfrica'),
# MAGIC ('Chicago', 'RegionNA'),
# MAGIC ('RAF', 'RegionGlobalFI'),
# MAGIC ('Rabobank Canada (RCBR)', 'RegionNA'),
# MAGIC ('Spain', 'RegionEuropeAfrica'),
# MAGIC ('Ireland', 'RegionEuropeAfrica'),
# MAGIC ('Rabobank Frankfurt', 'RegionEuropeAfrica'),
# MAGIC ('Hong Kong', 'RegionAsia'),
# MAGIC ('Atlanta', 'RegionNA'),
# MAGIC ('Rabobank Australia', 'RegionRANZ'),
# MAGIC ('Rabobank - Smallholder Agroforestry Finance (SAF)', 'RegionGlobalFI'),
# MAGIC ('Rabobank - USA Rabo AgriFinance', 'RegionNA'),
# MAGIC ('Rabobank New Zealand', 'RegionRANZ'),
# MAGIC ('Mexico', 'RegionNA'),
# MAGIC ('Rabobank Brazil', 'RegionSA'),
# MAGIC ('Rabobank Foundation', 'RegionGlobalFI'),
# MAGIC ('Rabobank Turkey', 'RegionEuropeAfrica'),
# MAGIC ('UTRECHT', 'RegionEuropeAfrica'),
# MAGIC ('Rabobank Chile', 'RegionSA'),
# MAGIC ('Rabobank Malaysia', 'RegionAsia'),
# MAGIC ('Rabobank Argentina', 'RegionSA'),
# MAGIC ('Utrecht', 'RegionEuropeAfrica'),
# MAGIC ('Rabobank Kenya', 'RegionEuropeAfrica'),
# MAGIC ('Rabobank Dublin', 'RegionEuropeAfrica'),
# MAGIC ('Hong Kong S.A.R.', 'RegionAsia'),
# MAGIC ('Rabobank - RegionRANZ Country Banking and ROS', 'RegionRANZ'),
# MAGIC ('Rabobank Madrid', 'RegionEuropeAfrica'),
# MAGIC ('United Kingdom of Great Britain and Northern Ireland', 'RegionEuropeAfrica'),
# MAGIC ('Rabobank Antwerp', 'RegionEuropeAfrica'),
# MAGIC ('TURKEY', 'RegionEuropeAfrica'),
# MAGIC ('Canada', 'RegionNA'),
# MAGIC ('Rabobank Milan', 'RegionEuropeAfrica'),
# MAGIC ('Brazil', 'RegionSA'),
# MAGIC ('Kenya', 'RegionEuropeAfrica'),
# MAGIC ('New Zealand', 'RegionRANZ'),
# MAGIC ('Australia', 'RegionRANZ'),
# MAGIC ('Rabobank Canada (Rural)', 'RegionNA'),
# MAGIC ('Rabobank Hong Kong', 'RegionAsia'),
# MAGIC ('Rabobank Singapore', 'RegionAsia'),
# MAGIC ('Rabobank China', 'RegionAsia'),
# MAGIC ('Rabobank Canada(Rural)', 'RegionNA'),
# MAGIC ('New York', 'RegionNA'),
# MAGIC ('Netherlands', 'RegionEuropeAfrica'),
# MAGIC ('Shanghai', 'RegionAsia');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wr_fj_parties_and_risk_assessment_preprd.radar.location_region_lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS wr_fj_parties_and_risk_assessment_preprd.radar.fn_party_row_filter;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION wr_fj_parties_and_risk_assessment_preprd.radar.fn_party_row_filter(
# MAGIC    globalClientOwnerLocation STRING
# MAGIC )
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM wr_fj_parties_and_risk_assessment_preprd.radar.location_region_lookup l
# MAGIC     JOIN wr_fj_parties_and_risk_assessment_preprd.radar.rdm_n2kuserattribute u
# MAGIC       ON LOWER(u.AttributeValue) = LOWER(l.Region)
# MAGIC     WHERE LOWER(u.UPN) = LOWER(current_user())
# MAGIC       AND l.GlobalClientOwnerLocation = globalClientOwnerLocation
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP FUNCTION IF EXISTS wr_fj_parties_and_risk_assessment_preprd.radar.fn_region_row_filter;
# MAGIC
# MAGIC CREATE FUNCTION wr_fj_parties_and_risk_assessment_preprd.radar.fn_region_row_filter(
# MAGIC   row_region STRING
# MAGIC )
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM wr_fj_parties_and_risk_assessment_preprd.radar.rdm_n2kuserattribute u
# MAGIC   WHERE LOWER(u.UPN) = LOWER(current_user())
# MAGIC     AND LOWER(u.AttributeType) = LOWER(row_region)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Attach the row filter to Party
# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE wr_fj_parties_and_risk_assessment_preprd.radar.party
# MAGIC SET ROW FILTER wr_fj_parties_and_risk_assessment_preprd.radar.fn_party_row_filter
# MAGIC ON (GlobalClientOwnerLocation);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED wr_fj_parties_and_risk_assessment_preprd.radar.party;

# COMMAND ----------

# DBTITLE 1,Attach to the tables
# MAGIC %sql
# MAGIC ALTER TABLE wr_fj_parties_and_risk_assessment_preprd.radar.party_address
# MAGIC SET ROW FILTER wr_fj_parties_and_risk_assessment_preprd.radar.fn_region_row_filter
# MAGIC ON (Region);
# MAGIC
# MAGIC ALTER TABLE wr_fj_parties_and_risk_assessment_preprd.radar.party_naics
# MAGIC SET ROW FILTER wr_fj_parties_and_risk_assessment_preprd.radar.fn_region_row_filter
# MAGIC ON (Region);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED wr_fj_parties_and_risk_assessment_preprd.radar.party_address;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED wr_fj_parties_and_risk_assessment_preprd.radar.party_naics;
