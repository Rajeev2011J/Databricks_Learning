# Databricks notebook source
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

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

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

# DBTITLE 1,No Need already we have N2K  for RDM Model
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
#display(df)

# COMMAND ----------

RDM_N2KUserAttribute_df = read_uc_table("wr_fj_parties_and_risk_assessment_preprd", "radar", "RDM_N2KUserAttribute")
#display(RDM_N2KUserAttribute_df)

# COMMAND ----------

df_location_region_lookup = read_uc_table("wr_fj_parties_and_risk_assessment_preprd", "radar", "location_region_lookup")
display(df_location_region_lookup)

# COMMAND ----------

# DBTITLE 1,Reusable helper: does the current user have access to this party's region
# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS wr_fj_parties_and_risk_assessment_preprd.radar.fn_can_view_dob;
# MAGIC --  Entitlement is granted if the user has AttributeValue = party_region
# MAGIC CREATE OR REPLACE FUNCTION wr_fj_parties_and_risk_assessment_preprd.radar.fn_can_view_dob(party_region STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC RETURN EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM wr_fj_parties_and_risk_assessment_preprd.radar.RDM_N2KUserAttribute u
# MAGIC   WHERE lower(u.UPN) = lower(current_user())
# MAGIC     AND (
# MAGIC          u.AttributeValue = party_region
# MAGIC       OR u.AttributeValue = 'RegionGlobal' -- Global Users
# MAGIC     )
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Reusable masking function for DateOfBirth
# MAGIC %sql
# MAGIC -- Returns the real DOB when permitted; otherwise, masked
# MAGIC CREATE OR REPLACE FUNCTION wr_fj_parties_and_risk_assessment_preprd.radar.fn_mask_dob_region(
# MAGIC     dob DATE,
# MAGIC     party_region STRING
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC RETURN
# MAGIC CASE
# MAGIC   WHEN wr_fj_parties_and_risk_assessment_preprd.radar.fn_can_view_dob(party_region) THEN CAST(dob AS STRING)
# MAGIC   ELSE '****-**-**'
# MAGIC END;

# COMMAND ----------

# DBTITLE 1,Enforced Column-Level Security via a SECURE VIEW
# MAGIC %sql
# MAGIC -- Joins Party with location_region_lookup to get the party's Region,
# MAGIC -- then applies the masking function to DateOfBirth.
# MAGIC -- Secure View for enforcing Column-Level Security on DateOfBirth
# MAGIC -- The unmasked DateOfBirth column is intentionally removed
# MAGIC -- Only the masked version is exposed to end users
# MAGIC
# MAGIC CREATE OR REPLACE VIEW wr_fj_parties_and_risk_assessment_preprd.radar.Party_CLS AS
# MAGIC SELECT
# MAGIC     p.PartyIdentifier,
# MAGIC     p.gcid,
# MAGIC     p.GCOB_Identifier,
# MAGIC     p.GIC_Identifier,
# MAGIC     p.Legacy2_Identifier,
# MAGIC     p.NLSVF_Identifier,
# MAGIC     p.FullLegalName,
# MAGIC     -- Masked DOB replaces original unmasked column
# MAGIC     wr_fj_parties_and_risk_assessment_preprd.radar.fn_mask_dob_region(p.DateOfBirth, lr.Region)
# MAGIC         AS DateOfBirth,
# MAGIC     p.FirstName,
# MAGIC     p.MiddleName,
# MAGIC     p.LastName,
# MAGIC     p.Party_type,
# MAGIC     p.GlobalClientOwnerName,
# MAGIC     p.GlobalClientOwnerLocation,
# MAGIC     p.IsEligibleForFatcaAssessment,
# MAGIC     p.FatcaClassification,
# MAGIC     p.GIIN,
# MAGIC     p.EIN,
# MAGIC     p.FatcaDateOfIssue,
# MAGIC     p.FatcaComments,
# MAGIC     p.IsEligibleForCrsAssessment,
# MAGIC     p.CrsClassification,
# MAGIC     p.CrsFormSignedDate,
# MAGIC     p.CrsComments,
# MAGIC     p.LegalForm,
# MAGIC     p.LifeCycleStatus,
# MAGIC     p.StatusName,
# MAGIC     p.IsLatestApprovedVersionOfClient,
# MAGIC     p.FullLegalNameInLocalLanguage,
# MAGIC     p.IsIncorporated,
# MAGIC     p.IncorporationNumber,
# MAGIC     p.IncorporationDate,
# MAGIC     p.BusinessLineName,
# MAGIC     p.HasSourceOfWealth,
# MAGIC     p.SanctionsOrExternalWatchlist,
# MAGIC     p.InternalWatchlist,
# MAGIC     p.AdverseInformationOrMedia,
# MAGIC     p.StatedFindings,
# MAGIC     p.PEPStatus,
# MAGIC     p.IsTrust,
# MAGIC     p.TypeOfTrust,
# MAGIC     p.IsClientRegulated,
# MAGIC     p.RegulatorName,
# MAGIC     p.RegulatorCountry,
# MAGIC     p.HasRecognisedRegulator,
# MAGIC     p.IsClientListed,
# MAGIC     p.ExchangeName,
# MAGIC     p.ExchangeCountry,
# MAGIC     p.HasRecognisedExchange,
# MAGIC     p.CountryOfTaxResidence,
# MAGIC     p.TinAvailable,
# MAGIC     p.TinOrEquivalent,
# MAGIC     p.TinUnavailabilityReason,
# MAGIC     p.ExplanationForTinBeingUnavailable,
# MAGIC     p.SourceOfIdentificationDocument,
# MAGIC     p.IdentificationDocumentNumber,
# MAGIC     p.SourceOfVerifiedDocument,
# MAGIC     p.VerifiedDocumentNumber,
# MAGIC     p.CddType,
# MAGIC     p.Nationality,
# MAGIC     p.NationalityIsoCode,
# MAGIC     p.Citizenship,
# MAGIC     p.CitizenshipIsoCode,
# MAGIC     p.FIHubIndicator,
# MAGIC     p.Bank_code,
# MAGIC     p.`W&RORRetail`,
# MAGIC     p.Application,
# MAGIC     p.Party_LeadingPartyIdentifier
# MAGIC FROM wr_fj_parties_and_risk_assessment_preprd.radar.Party p
# MAGIC LEFT JOIN wr_fj_parties_and_risk_assessment_preprd.radar.location_region_lookup lr
# MAGIC     ON p.GlobalClientOwnerLocation = lr.GlobalClientOwnerLocation;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select DateOfBirth from wr_fj_parties_and_risk_assessment_preprd.radar.Party_CLS

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE wr_fj_parties_and_risk_assessment_preprd.radar.Party;

# COMMAND ----------

# DBTITLE 1,Grant Secure View Access
# MAGIC %sql
# MAGIC -- 5) Grant access to the secure view (choose one)
# MAGIC -- A) To all account users:
# MAGIC -- B) Or only to specific AAD groups you manage in UC:
# MAGIC USE CATALOG wr_fj_parties_and_risk_assessment_preprd;
# MAGIC
# MAGIC -- Grant to each AAD group separately (safer across runtimes)
# MAGIC GRANT SELECT ON VIEW radar.Party_CLS TO `eu.aut.AADRadarRegionAsia.us`;
# MAGIC GRANT SELECT ON VIEW radar.Party_CLS TO `eu.aut.AADRadarRegionGlobalFI.us`;
# MAGIC GRANT SELECT ON VIEW radar.Party_CLS TO `eu.aut.AADRadarRegionNA.us`;
# MAGIC GRANT SELECT ON VIEW radar.Party_CLS TO `eu.aut.AADRadarRegionEuropeAfrica.us`;
# MAGIC GRANT SELECT ON VIEW radar.Party_CLS TO `eu.aut.AADRadarRegionRANZ.us`;
# MAGIC GRANT SELECT ON VIEW radar.Party_CLS TO `eu.aut.AADRadarRegionSA.us`;

# COMMAND ----------

# DBTITLE 1,Revoke Direct Table Access
# MAGIC %sql
# MAGIC REVOKE SELECT ON TABLE wr_fj_parties_and_risk_assessment_preprd.radar.Party
# MAGIC FROM `eu.aut.AADRadarRegionAsia.us`;
# MAGIC
# MAGIC REVOKE SELECT ON TABLE wr_fj_parties_and_risk_assessment_preprd.radar.Party
# MAGIC FROM `eu.aut.AADRadarRegionGlobalFI.us`;
# MAGIC
# MAGIC REVOKE SELECT ON TABLE wr_fj_parties_and_risk_assessment_preprd.radar.Party
# MAGIC FROM `eu.aut.AADRadarRegionNA.us`;
# MAGIC
# MAGIC REVOKE SELECT ON TABLE wr_fj_parties_and_risk_assessment_preprd.radar.Party
# MAGIC FROM `eu.aut.AADRadarRegionEuropeAfrica.us`;
# MAGIC
# MAGIC REVOKE SELECT ON TABLE wr_fj_parties_and_risk_assessment_preprd.radar.Party
# MAGIC FROM `eu.aut.AADRadarRegionRANZ.us`;
# MAGIC
# MAGIC REVOKE SELECT ON TABLE wr_fj_parties_and_risk_assessment_preprd.radar.Party
# MAGIC FROM `eu.aut.AADRadarRegionSA.us`;
