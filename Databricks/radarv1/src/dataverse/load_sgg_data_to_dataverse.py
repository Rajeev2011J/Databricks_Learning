# Databricks notebook source
import os
import requests
from functions_dataverse import get_access_token, upsert_to_dataverse_table

# COMMAND ----------

# DBTITLE 1,dataverse access token
app_reg_app_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

dataverse_url = os.environ['DATAVERSE_URL']
dataverse_api_url = f"{dataverse_url}/api/data/v9.1/"

access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)

# COMMAND ----------

# DBTITLE 1,identify primaryi id column in dataverse target table
# r = requests.get(
#     f"{dataverse_api_url}EntityDefinitions(LogicalName='rdr_sggcomparisons')?$select=PrimaryIdAttribute",
#     headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"})
# print(r.json()["PrimaryIdAttribute"])

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Code to upsert all the mismatches to dataverse, its also upserting almost matches (regarded as 3 in most cases) also, so only perfect matches (regarded as 1) are not upserted

# COMMAND ----------

sggmismatchesdataquery_01 = f'''

-- client name comparison
with cte_gcobgcdsclientnamematch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobGcdsClientNameMatch AS rdr_gcobgcdsclientnamematch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
where t2.gcdsclientlifecyclestatus = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t2.memberBank = 'excludeMemberBank'
and t1.gcobGcdsClientNameMatch <> 1
) --  duplication of gcid!!
, cte_gcdsSiebelClientNameMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcdsSiebelClientNameMatch AS rdr_gcdsSiebelClientNameMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t3.siebelclientlifecyclename = 'Klant'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t2.memberBank = 'excludeMemberBank'
and t1.gcdsSiebelClientNameMatch <> 1
)
, cte_gcobSiebelClientNameMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobSiebelClientNameMatch AS rdr_gcobSiebelClientNameMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t3.siebelclientlifecyclename = 'Klant'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t2.memberBank = 'excludeMemberBank'
and t1.gcobSiebelClientNameMatch <> 1
)
, cte_gcobgcdsclientnamematch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobGcdsClientNameMatch AS rdr_gcobgcdsclientnamematch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t3.clientlifecyclename = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t3.uniquegcobid not like 'NP_%'
and t2.memberBank = 'excludeMemberBank'
and t1.gcobGcdsClientNameMatch <> 1
)
, cte_gcdsSiebelClientNameMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcdsSiebelClientNameMatch AS rdr_gcdsSiebelClientNameMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t3.siebelclientlifecyclename = 'Klant'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t2.memberBank = 'excludeMemberBank'
and t1.siebelid is not null
and t1.gcdsSiebelClientNameMatch <> 1
)
, cte_gcobSiebelClientNameMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobSiebelClientNameMatch AS rdr_gcobSiebelClientNameMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t3.clientlifecyclename = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t2.memberBank = 'excludeMemberBank'
and t1.siebelid is not null
and t1.gcobSiebelClientNameMatch <> 1
)
-- client lifecycle status
, cte_gcobGcdsClientLifeCycleStatusMatch (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobGcdsClientLifeCycleStatusMatch AS rdr_gcobGcdsClientLifeCycleStatusMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t2.gcdsclientlifecyclestatus = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t3.uniquegcobid not like 'NP_%'
and t1.gcobGcdsClientLifeCycleStatusMatch <> 1
)
, cte_gcdsSiebelClientLifeCycleStatusMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcdsSiebelClientLifeCycleStatusMatch AS rdr_gcdsSiebelClientLifeCycleStatusMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t3.siebelclientlifecyclename = 'Klant'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t1.gcdsSiebelClientLifeCycleStatusMatch <> 1
)
, cte_gcobSiebelClientLifeCycleStatusMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobSiebelClientLifeCycleStatusMatch AS rdr_gcobSiebelClientLifeCycleStatusMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t3.siebelclientlifecyclename = 'Klant'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t1.gcobSiebelClientLifeCycleStatusMatch <> 1
)
, cte_gcobGcdsClientLifeCycleStatusMatch_v2 (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobGcdsClientLifeCycleStatusMatch AS rdr_gcobGcdsClientLifeCycleStatusMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t3.clientlifecyclename = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t3.uniquegcobid not like 'NP_%'
and t1.gcobGcdsClientLifeCycleStatusMatch <> 1
)
, cte_gcdsSiebelClientLifeCycleStatusMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcdsSiebelClientLifeCycleStatusMatch AS rdr_gcdsSiebelClientLifeCycleStatusMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t2.gcdsclientlifecyclestatus = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t1.siebelid is not null
and t1.gcdsSiebelClientLifeCycleStatusMatch <> 1
)
, cte_gcobSiebelClientLifeCycleStatusMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobSiebelClientLifeCycleStatusMatch AS rdr_gcobSiebelClientLifeCycleStatusMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t3.clientlifecyclename = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t1.siebelid is not null
and t1.gcobSiebelClientLifeCycleStatusMatch <> 1
)
-- client address
, cte_gcdsGcobAddressMatch (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcdsGcobAddressMatch AS rdr_gcdsGcobAddressMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t2.gcdsclientlifecyclestatus = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t3.uniquegcobid not like 'NP_%'
and t2.GCDSFIOrCorp = 'GCDS Corp'
and t2.gcdsglobalcolocation <> 'London'
and t2.gcdsreportingregion = 'E&A'
and t2.memberBank = 'excludeMemberBank'
and t1.gcdsGcobAddressMatch <> 1
)
, cte_siebelGcdsAddressMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.siebelGcdsAddressMatch AS rdr_siebelGcdsAddressMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t3.siebelclientlifecyclename = 'Klant'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t2.GCDSFIOrCorp = 'GCDS Corp'
and t2.gcdsglobalcolocation <> 'London'
and t2.gcdsreportingregion = 'E&A'
and t2.memberBank = 'excludeMemberBank'
and t1.siebelGcdsAddressMatch <> 1
) -- 951
, cte_siebelGcobAddressMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.siebelGcobAddressMatch AS rdr_siebelGcobAddressMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t3.siebelclientlifecyclename = 'Klant'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t2.GCDSFIOrCorp = 'GCDS Corp'
and t2.gcdsglobalcolocation <> 'London'
and t2.gcdsreportingregion = 'E&A'
and t2.memberBank = 'excludeMemberBank'
and t1.siebelGcobAddressMatch <> 1
) -- 1422
, cte_gcdsGcobAddressMatch_v2 (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcdsGcobAddressMatch AS rdr_gcdsGcobAddressMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t3.clientlifecyclename = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t3.uniquegcobid not like 'NP_%'
and t2.GCDSFIOrCorp = 'GCDS Corp'
and t2.gcdsglobalcolocation <> 'London'
and t2.gcdsreportingregion = 'E&A'
and t2.memberBank = 'excludeMemberBank'
and t1.gcdsGcobAddressMatch <> 1
) -- 1926
, cte_siebelGcdsAddressMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.siebelGcdsAddressMatch AS rdr_siebelGcdsAddressMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t2.gcdsclientlifecyclestatus = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t1.siebelid is not null
and t2.GCDSFIOrCorp = 'GCDS Corp'
and t2.gcdsglobalcolocation <> 'London'
and t2.gcdsreportingregion = 'E&A'
and t2.memberBank = 'excludeMemberBank'
and t1.siebelGcdsAddressMatch <> 1
) -- 2381
, cte_siebelGcobAddressMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.siebelGcobAddressMatch AS rdr_siebelGcobAddressMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t3.clientlifecyclename = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t1.siebelid is not null
and t2.GCDSFIOrCorp = 'GCDS Corp'
and t2.gcdsglobalcolocation <> 'London'
and t2.gcdsreportingregion = 'E&A'
and t2.memberBank = 'excludeMemberBank'
and t1.siebelGcobAddressMatch <> 1
) -- 2850
-- client street name
, cte_gcdsGcobStreetNameMatch (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcdsGcobStreetNameMatch AS rdr_gcdsGcobStreetNameMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t2.gcdsclientlifecyclestatus = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t3.uniquegcobid not like 'NP_%'
and t2.memberBank = 'excludeMemberBank'
and t1.gcdsGcobStreetNameMatch <> 1
) -- 4483
, cte_gcdsSiebelStreetNameMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcdsSiebelStreetNameMatch AS rdr_gcdsSiebelStreetNameMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t3.siebelclientlifecyclename = 'Klant'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t2.memberBank = 'excludeMemberBank'
and t1.gcdsSiebelStreetNameMatch <> 1
) -- 545
, cte_gcobSiebelStreetNameMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobSiebelStreetNameMatch AS rdr_gcobSiebelStreetNameMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t3.siebelclientlifecyclename = 'Klant'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t2.memberBank = 'excludeMemberBank'
and t1.gcobSiebelStreetNameMatch <> 1
) -- 592
, cte_gcdsGcobStreetNameMatch_v2 (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcdsGcobStreetNameMatch AS rdr_gcdsGcobStreetNameMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t3.clientlifecyclename = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t3.uniquegcobid not like 'NP_%'
and t2.memberBank = 'excludeMemberBank'
and t1.gcdsGcobStreetNameMatch <> 1
) -- 4752
, cte_gcdsSiebelStreetNameMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcdsSiebelStreetNameMatch AS rdr_gcdsSiebelStreetNameMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t2.gcdsclientlifecyclestatus = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t1.siebelid is not null
and t2.memberBank = 'excludeMemberBank'
and t1.gcdsSiebelStreetNameMatch <> 1
) -- 534
, cte_gcobSiebelStreetNameMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobSiebelStreetNameMatch AS rdr_gcobSiebelStreetNameMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t3.clientlifecyclename = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t1.siebelid is not null
and t2.memberBank = 'excludeMemberBank'
and t1.gcobSiebelStreetNameMatch <> 1
) -- 584

-- email address
, cte_gcobSiebelEmailAddressMatch (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobSiebelEmailAddressMatch AS rdr_gcobSiebelEmailAddressMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE
    WHEN ((t4.GlobalKYCPortfolioNew IS NULL OR t4.GlobalKYCPortfolioNew IN (
            'Antwerp advisory & Investments',
            'Antwerp Core Lending',
            'Dublin Core Lending',
            'Foundation',
            'Frankfurt Core Lending',
            'Frankfurt International Services',
            'Madrid Core Lending',
            'Milan Core Lending',
            'NL Advisory & Investments',
            'NL Core Lending',
            'NL Structured Lending',
            'Paris Core Lending'
         ))
        AND t4.GlobalReportingRegion = 'E&A'
        AND t4.SectorTeam <> 'RCI'
    )
        THEN TRUE
        ELSE FALSE
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid

LEFT JOIN radar.sgggcobdata t4 ON t1.uniquegcobid = t4.uniquegcobid

where t3.siebelclientlifecyclename = 'Klant'
and t2.keystore_type = 'GCOBID'
and t2.memberBank = 'excludeMemberBank'
and t1.gcobSiebelEmailAddressMatch <> 1
) -- 7626
, cte_gcobSiebelEmailAddressMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobSiebelEmailAddressMatch AS rdr_gcobSiebelEmailAddressMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE
    WHEN ((t3.GlobalKYCPortfolioNew IS NULL OR t3.GlobalKYCPortfolioNew IN (
            'Antwerp advisory & Investments',
            'Antwerp Core Lending',
            'Dublin Core Lending',
            'Foundation',
            'Frankfurt Core Lending',
            'Frankfurt International Services',
            'Madrid Core Lending',
            'Milan Core Lending',
            'NL Advisory & Investments',
            'NL Core Lending',
            'NL Structured Lending',
            'Paris Core Lending'
         ))
        AND t3.GlobalReportingRegion = 'E&A'
        AND t3.SectorTeam <> 'RCI'
    )
        THEN TRUE
        ELSE FALSE
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t3.clientlifecyclename = 'Client'
and t2.keystore_type = 'SBWRR'
and t2.memberBank = 'excludeMemberBank'
and t1.gcobSiebelEmailAddressMatch <> 1
) -- 7588

-- GCO location and name
, cte_gcobGcdsGlobalClientOwnerMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobGcdsGlobalClientOwnerMatch AS rdr_gcobGcdsGlobalClientOwnerMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , false AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
where t2.gcdsclientlifecyclestatus = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t1.uniquegcobid is not null
and t1.gcobGcdsGlobalClientOwnerMatch <> 1
) -- 5812
, cte_gcobGcdsGlobalClientOwnerMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobGcdsGlobalClientOwnerMatch AS rdr_gcobGcdsGlobalClientOwnerMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , false AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t3.clientlifecyclename = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t1.uniquegcobid is not null
and t1.gcobGcdsGlobalClientOwnerMatch <> 1
) -- 6105
-- GCO Location
, cte_gcobGcdsGlobalClientOwnerLocationMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobGcdsGlobalClientOwnerLocationMatch AS rdr_gcobGcdsGlobalClientOwnerLocationMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , false AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
where t2.gcdsclientlifecyclestatus = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t1.uniquegcobid is not null
and t1.gcobGcdsGlobalClientOwnerLocationMatch <> 1
) -- 3374
, cte_gcobGcdsGlobalClientOwnerLocationMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobGcdsGlobalClientOwnerLocationMatch AS rdr_gcobGcdsGlobalClientOwnerLocationMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , false AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t3.clientlifecyclename = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t1.uniquegcobid is not null
and t1.gcobGcdsGlobalClientOwnerLocationMatch <> 1
) -- 3360



/*
KVK number
*/
, cte_gcobGcdsDutchIncorporationNumberMatch (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobGcdsDutchIncorporationNumberMatch AS rdr_gcobGcdsDutchIncorporationNumberMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t2.gcdsclientlifecyclestatus = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t2.gcdsRegisteredCountry in ('Netherlands', 'Netherlands (the)')
and t3.RegisteredCountryIsoCode = 'NL'
and t3.uniquegcobid not like 'NP_%'
and t2.memberBank = 'excludeMemberBank'
and t1.gcobGcdsDutchIncorporationNumberMatch <> 1
) -- 193
, cte_siebelGcdsDutchIncorporationNumberMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.siebelGcdsDutchIncorporationNumberMatch AS rdr_siebelGcdsDutchIncorporationNumberMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t3.siebelclientlifecyclename = 'Klant'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'SBWRR'
and t3.entityType = 'Dutch Entity'
and t2.gcdsRegisteredCountry in ('Netherlands', 'Netherlands (the)')
and t2.memberBank = 'excludeMemberBank'
and t1.siebelGcdsDutchIncorporationNumberMatch <> 1
) -- 26
, cte_siebelGcobDutchIncorporationNumberMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.siebelGcobDutchIncorporationNumberMatch AS rdr_siebelGcobDutchIncorporationNumberMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
JOIN radar.sgggcobdata t4 ON t1.uniquegcobid = t4.uniquegcobid

where t3.siebelclientlifecyclename = 'Klant' --
and t3.entityType = 'Dutch Entity' --
and t2.keystore_type = 'GCOBID' --
and t4.RegisteredCountryIsoCode = 'NL' --
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity') --
and t2.memberBank = 'excludeMemberBank' --
and t1.siebelGcobDutchIncorporationNumberMatch <> 1
) -- 27
, cte_siebelGcdsDutchIncorporationNumberMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.siebelGcdsDutchIncorporationNumberMatch AS rdr_siebelGcdsDutchIncorporationNumberMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t2.gcdsclientlifecyclestatus = 'Client' -- 
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity') -- 
and t2.keystore_type = 'SBWRR' -- 
and t3.entityType = 'Dutch Entity'
and t2.gcdsRegisteredCountry in ('Netherlands', 'Netherlands (the)') -- 
and t2.memberBank = 'excludeMemberBank' --
and t1.siebelGcdsDutchIncorporationNumberMatch <> 1
) -- 23
, cte_gcobGcdsDutchIncorporationNumberMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobGcdsDutchIncorporationNumberMatch AS rdr_gcobGcdsDutchIncorporationNumberMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t3.clientlifecyclename = 'Client'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.keystore_type = 'GCOBID'
and t2.gcdsRegisteredCountry in ('Netherlands', 'Netherlands (the)')
and t3.RegisteredCountryIsoCode = 'NL'
and t3.uniquegcobid not like 'NP_%'
and t2.memberBank = 'excludeMemberBank'
and t1.gcobGcdsDutchIncorporationNumberMatch <> 1
) -- 209
, cte_siebelGcobDutchIncorporationNumberMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.siebelGcobDutchIncorporationNumberMatch AS rdr_siebelGcobDutchIncorporationNumberMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
JOIN radar.sgggcobdata t4 ON t1.uniquegcobid = t4.uniquegcobid

where t4.clientlifecyclename = 'Client' --
and t3.entityType = 'Dutch Entity' --
and t2.keystore_type = 'SBWRR' --
and t4.RegisteredCountryIsoCode = 'NL' --
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity') --
and t2.memberBank = 'excludeMemberBank' --
and t1.siebelGcobDutchIncorporationNumberMatch <> 1
) -- 22



/*
INCORPORATION NUMBER
*/
, cte_gcobGcdsNonDutchIncorporationNumberMatch (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobGcdsNonDutchIncorporationNumberMatch AS rdr_gcobGcdsNonDutchIncorporationNumberMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid
where t2.gcdsclientlifecyclestatus = 'Client' -- 
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity') -- 
and t2.gcdsRegisteredCountry not in ('Netherlands', 'Netherlands (the)') --
and t2.keystore_type = 'GCOBID' --
and t3.RegisteredCountryIsoCode <> 'NL' --
and t3.uniquegcobid not like 'NP_%'
and t2.memberBank = 'excludeMemberBank'
and t1.gcobGcdsNonDutchIncorporationNumberMatch <> 1
) -- 12246
, cte_siebelGcdsNonDutchIncorporationNumberMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.siebelGcdsNonDutchIncorporationNumberMatch AS rdr_siebelGcdsNonDutchIncorporationNumberMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t3.siebelclientlifecyclename = 'Klant' --
and t3.entityType <> 'Dutch Entity' --
and t2.keystore_type = 'SBWRR' --
and t2.gcdsRegisteredCountry not in ('Netherlands', 'Netherlands (the)') --
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.memberBank = 'excludeMemberBank'
and t1.siebelGcdsNonDutchIncorporationNumberMatch <> 1
) -- 160
, cte_siebelGcobNonDutchIncorporationNumberMatch as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.siebelGcobNonDutchIncorporationNumberMatch AS rdr_siebelGcobNonDutchIncorporationNumberMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
JOIN radar.sgggcobdata t4 ON t1.uniquegcobid = t4.uniquegcobid

where t3.siebelclientlifecyclename = 'Klant' --
and t3.entityType <> 'Dutch Entity' --
and t2.keystore_type = 'GCOBID' --
AND (t4.RegisteredCountryIsoCode <> 'NL' OR t4.RegisteredCountryIsoCode IS NULL)
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity') --
and t2.memberBank = 'excludeMemberBank' --
and t1.siebelGcobNonDutchIncorporationNumberMatch <> 1
) -- 449
, cte_siebelGcdsNonDutchIncorporationNumberMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.siebelGcdsNonDutchIncorporationNumberMatch AS rdr_siebelGcdsNonDutchIncorporationNumberMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
where t2.gcdsclientlifecyclestatus = 'Client' -- 
and t3.entityType <> 'Dutch Entity' --
and t2.keystore_type = 'SBWRR' --
and t2.gcdsRegisteredCountry not in ('Netherlands', 'Netherlands (the)') --
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t2.memberBank = 'excludeMemberBank'
and t1.siebelGcdsNonDutchIncorporationNumberMatch <> 1
) -- 157
, cte_gcobGcdsNonDutchIncorporationNumberMatch_v2 (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcobGcdsNonDutchIncorporationNumberMatch AS rdr_gcobGcdsNonDutchIncorporationNumberMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.sgggcobdata t3 ON t1.uniquegcobid = t3.uniquegcobid

where t3.clientlifecyclename = 'Client' --
and t2.gcdsRegisteredCountry not in ('Netherlands', 'Netherlands (the)') --
and t2.keystore_type = 'GCOBID' --
AND (t3.RegisteredCountryIsoCode <> 'NL' OR t3.RegisteredCountryIsoCode IS NULL)
and t3.uniquegcobid not like 'NP_%'
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity') -- 
and t2.memberBank = 'excludeMemberBank'
and t1.gcobGcdsNonDutchIncorporationNumberMatch <> 1
) -- 12646
, cte_siebelGcobNonDutchIncorporationNumberMatch_v2 as (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.siebelGcobNonDutchIncorporationNumberMatch AS rdr_siebelGcobNonDutchIncorporationNumberMatch
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t2.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t2.gcdsReportingRegion = 'E&A' AND t2.gcdsGlobalCOlocation <> 'London' AND t2.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcdsrawdata t2 ON t1.gcid = t2.gcid
JOIN radar.siebelclientinformation t3 ON t1.siebelid = t3.siebelid
JOIN radar.sgggcobdata t4 ON t1.uniquegcobid = t4.uniquegcobid

where t4.clientlifecyclename = 'Client' --
and t3.entityType <> 'Dutch Entity' --
and t2.keystore_type = 'SBWRR' --
AND (t4.RegisteredCountryIsoCode <> 'NL' OR t4.RegisteredCountryIsoCode IS NULL)
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity') --
and t2.memberBank = 'excludeMemberBank' --
and t1.siebelGcobNonDutchIncorporationNumberMatch <> 1
) -- 449

/*
Siebel client not in GCDS
*/
, cte_siebelNotInGcds (
SELECT DISTINCT
    t3.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t3.gcid AS rdr_gcid
    , t3.uniquegcobid AS rdr_uniquegcobid
    , t3.siebelid AS rdr_siebelid
    , null AS rdr_GCDSFIOrCorp

    , true AS rdr_sebastiaanscope

FROM radar.siebelclientnotingcds t1
JOIN radar.siebelclientinformation t2 ON t1.siebelid = t2.siebelid
JOIN radar.siebelgcobgcdsclientcomparison t3 ON t1.siebelid = t3.siebelid
WHERE t2.siebelclientlifecyclename = 'Klant'
AND t3.gcid is null
)
, cte_siebelNotInGcob (
SELECT DISTINCT
    t3.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t3.gcid AS rdr_gcid
    , t3.uniquegcobid AS rdr_uniquegcobid
    , t3.siebelid AS rdr_siebelid
    , t4.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t4.gcdsReportingRegion = 'E&A' AND t4.gcdsGlobalCOlocation <> 'London' AND t4.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope    

FROM radar.siebelclientnotingcob t1
JOIN radar.siebelclientinformation t2 ON t1.siebelid = t2.siebelid
JOIN radar.siebelgcobgcdsclientcomparison t3 ON t1.siebelid = t3.siebelid
JOIN radar.gcdsrawdata t4 ON t1.gcid = t4.gcid
WHERE t2.siebelclientlifecyclename = 'Klant'
AND t3.uniquegcobid is null
and t4.gcdsPartyType in ('Foreign Branch', 'Legal Entity') --
)
, cte_gcobNotInGcds (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , null AS rdr_GCDSFIOrCorp

    , CASE
    WHEN ((t2.GlobalKYCPortfolioNew IS NULL OR t2.GlobalKYCPortfolioNew IN (
            'Antwerp advisory & Investments',
            'Antwerp Core Lending',
            'Dublin Core Lending',
            'Foundation',
            'Frankfurt Core Lending',
            'Frankfurt International Services',
            'Madrid Core Lending',
            'Milan Core Lending',
            'NL Advisory & Investments',
            'NL Core Lending',
            'NL Structured Lending',
            'Paris Core Lending'
         ))
        AND t2.GlobalReportingRegion = 'E&A'
        AND t2.SectorTeam <> 'RCI'
    )
        THEN TRUE
        ELSE FALSE
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.sgggcobdata t2 ON t1.uniquegcobid = t2.uniquegcobid
where t2.clientlifecyclename = 'Client' -- 
AND t1.gcid is null
and t1.uniquegcobid not like 'NP_%'
and t1.uniquegcobid is not null
)
, cte_gcobWithoutGcid (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t4.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , true AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1
JOIN radar.gcobwithoutgcidingcob t11 ON t1.uniquegcobid = t11.uniquegcobid
JOIN radar.sgggcobdata t2 ON t1.uniquegcobid = t2.uniquegcobid
JOIN radar.gcdsrawdata t4 ON t1.gcid = t4.gcid
where t2.clientlifecyclename in ('Client','Prospect')
and t4.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
and t4.gcdsclientlifecyclestatus in ('Client','Prospect')
)
, cte_gcdsNotInGcob (
SELECT DISTINCT
    t1.sggUniqueIdentifier AS rdr_sgguniqueidentifier
    , t1.gcid AS rdr_gcid
    , t1.uniquegcobid AS rdr_uniquegcobid
    , t1.siebelid AS rdr_siebelid
    , t4.GCDSFIOrCorp AS rdr_GCDSFIOrCorp

    , CASE WHEN (t4.gcdsReportingRegion = 'E&A' AND t4.gcdsGlobalCOlocation <> 'London' AND t4.gcdsFiOrCorp = 'GCDS Corp') THEN true
    ELSE false
    END AS rdr_sebastiaanscope

FROM radar.siebelgcobgcdsclientcomparison t1 
JOIN radar.gcdsclients t2 ON t1.gcid = t2.gcid
JOIN radar.gcdsrawdata t4 ON t1.gcid = t4.gcid
WHERE t1.uniquegcobid is null
AND t2.gcdsclientlifecyclestatus in ('Client')
and t2.gcdsPartyType in ('Foreign Branch', 'Legal Entity')
)


-- combine all ctes
, cte_combined as (
    -- client name comparison
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobgcdsclientnamematch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcdsSiebelClientNameMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobSiebelClientNameMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobgcdsclientnamematch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcdsSiebelClientNameMatch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobSiebelClientNameMatch_v2
    -- client lifecycle status
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobGcdsClientLifeCycleStatusMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcdsSiebelClientLifeCycleStatusMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobSiebelClientLifeCycleStatusMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobGcdsClientLifeCycleStatusMatch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcdsSiebelClientLifeCycleStatusMatch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobSiebelClientLifeCycleStatusMatch_v2
    -- client address
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcdsGcobAddressMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelGcdsAddressMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelGcobAddressMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcdsGcobAddressMatch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelGcdsAddressMatch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelGcobAddressMatch_v2
    -- client street name
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcdsGcobStreetNameMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcdsSiebelStreetNameMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobSiebelStreetNameMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcdsGcobStreetNameMatch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcdsSiebelStreetNameMatch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobSiebelStreetNameMatch_v2
    -- email address
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobSiebelEmailAddressMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobSiebelEmailAddressMatch_v2

    -- GCO name & location
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobGcdsGlobalClientOwnerMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobGcdsGlobalClientOwnerMatch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobGcdsGlobalClientOwnerLocationMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobGcdsGlobalClientOwnerLocationMatch_v2

    -- KVK
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobGcdsDutchIncorporationNumberMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelGcdsDutchIncorporationNumberMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelGcobDutchIncorporationNumberMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobGcdsDutchIncorporationNumberMatch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelGcdsDutchIncorporationNumberMatch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelGcobDutchIncorporationNumberMatch_v2

    -- incorporation number
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobGcdsNonDutchIncorporationNumberMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelGcdsNonDutchIncorporationNumberMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelGcobNonDutchIncorporationNumberMatch
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobGcdsNonDutchIncorporationNumberMatch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelGcdsNonDutchIncorporationNumberMatch_v2
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelGcobNonDutchIncorporationNumberMatch_v2

    -- missing clients
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelNotInGcob
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_siebelNotInGcds
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobNotInGcds
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcobWithoutGcid
    UNION ALL
    SELECT DISTINCT rdr_sgguniqueidentifier, rdr_gcid, rdr_uniquegcobid, rdr_siebelid, rdr_GCDSFIOrCorp, rdr_sebastiaanscope
    FROM cte_gcdsNotInGcob
)

SELECT
    t1.*
    -- client name comparison
    , coalesce(t2.rdr_gcobgcdsclientnamematch, t5.rdr_gcobgcdsclientnamematch) AS rdr_gcobgcdsclientnamematch
    , coalesce(t3.rdr_gcdsSiebelClientNameMatch, t6.rdr_gcdsSiebelClientNameMatch) AS rdr_gcdsSiebelClientNameMatch
    , coalesce(t4.rdr_gcobSiebelClientNameMatch, t7.rdr_gcobSiebelClientNameMatch) AS rdr_gcobSiebelClientNameMatch
    -- client lifecycle status
    , coalesce(t8.rdr_gcobGcdsClientLifeCycleStatusMatch, t11.rdr_gcobGcdsClientLifeCycleStatusMatch) AS rdr_gcobGcdsClientLifeCycleStatusMatch
    , coalesce(t9.rdr_gcdsSiebelClientLifeCycleStatusMatch, t12.rdr_gcdsSiebelClientLifeCycleStatusMatch) AS rdr_gcdsSiebelClientLifeCycleStatusMatch
    , coalesce(t10.rdr_gcobSiebelClientLifeCycleStatusMatch, t13.rdr_gcobSiebelClientLifeCycleStatusMatch) AS rdr_gcobSiebelClientLifeCycleStatusMatch
    -- client address
    , coalesce(t14.rdr_gcdsGcobAddressMatch, t17.rdr_gcdsGcobAddressMatch) AS rdr_gcdsGcobAddressMatch
    , coalesce(t15.rdr_siebelGcdsAddressMatch, t18.rdr_siebelGcdsAddressMatch) AS rdr_siebelGcdsAddressMatch
    , coalesce(t16.rdr_siebelGcobAddressMatch, t19.rdr_siebelGcobAddressMatch) AS rdr_siebelGcobAddressMatch
    -- client street name
    , coalesce(t20.rdr_gcdsGcobStreetNameMatch, t23.rdr_gcdsGcobStreetNameMatch) AS rdr_gcdsGcobStreetNameMatch
    , coalesce(t21.rdr_gcdsSiebelStreetNameMatch, t24.rdr_gcdsSiebelStreetNameMatch) AS rdr_gcdsSiebelStreetNameMatch
    , coalesce(t22.rdr_gcobSiebelStreetNameMatch, t25.rdr_gcobSiebelStreetNameMatch) AS rdr_gcobSiebelStreetNameMatch
    -- email address
    , coalesce(t26.rdr_gcobSiebelEmailAddressMatch, t27.rdr_gcobSiebelEmailAddressMatch) AS rdr_gcobSiebelEmailAddressMatch
    -- GCO name & location
    , coalesce(t28.rdr_gcobGcdsGlobalClientOwnerMatch, t29.rdr_gcobGcdsGlobalClientOwnerMatch) AS rdr_gcobGcdsGlobalClientOwnerMatch
    , coalesce(t30.rdr_gcobGcdsGlobalClientOwnerLocationMatch, t31.rdr_gcobGcdsGlobalClientOwnerLocationMatch) AS rdr_gcobGcdsGlobalClientOwnerLocationMatch
    -- KVK
    , coalesce(t32.rdr_gcobGcdsDutchIncorporationNumberMatch, t35.rdr_gcobGcdsDutchIncorporationNumberMatch) AS rdr_gcobGcdsDutchIncorporationNumberMatch
    , coalesce(t33.rdr_siebelGcdsDutchIncorporationNumberMatch, t36.rdr_siebelGcdsDutchIncorporationNumberMatch) AS rdr_siebelGcdsDutchIncorporationNumberMatch
    , coalesce(t34.rdr_siebelGcobDutchIncorporationNumberMatch, t37.rdr_siebelGcobDutchIncorporationNumberMatch) AS rdr_siebelGcobDutchIncorporationNumberMatch
    -- incorporation number
    , coalesce(t38.rdr_gcobGcdsNonDutchIncorporationNumberMatch, t41.rdr_gcobGcdsNonDutchIncorporationNumberMatch) AS rdr_gcobGcdsNonDutchIncorporationNumberMatch
    , coalesce(t39.rdr_siebelGcdsNonDutchIncorporationNumberMatch, t42.rdr_siebelGcdsNonDutchIncorporationNumberMatch) AS rdr_siebelGcdsNonDutchIncorporationNumberMatch
    , coalesce(t40.rdr_siebelGcobNonDutchIncorporationNumberMatch, t43.rdr_siebelGcobNonDutchIncorporationNumberMatch) AS rdr_siebelGcobNonDutchIncorporationNumberMatch
    -- missing clients
    , CASE WHEN t44.rdr_sgguniqueidentifier IS NOT NULL THEN TRUE ELSE FALSE END AS rdr_SiebelNotInGCOB
    , CASE WHEN t45.rdr_sgguniqueidentifier IS NOT NULL THEN TRUE ELSE FALSE END AS rdr_SiebelNotInGCDS
    , CASE WHEN t46.rdr_sgguniqueidentifier IS NOT NULL THEN TRUE ELSE FALSE END AS rdr_GCOBNotInGCDS
    , CASE WHEN t47.rdr_sgguniqueidentifier IS NOT NULL THEN TRUE ELSE FALSE END AS rdr_GCOBWithoutGCID
    , CASE WHEN t48.rdr_sgguniqueidentifier IS NOT NULL THEN TRUE ELSE FALSE END AS rdr_GCDSNotInGCOB

FROM cte_combined t1
-- client name comparison 1731
LEFT JOIN cte_gcobgcdsclientnamematch t2 ON t1.rdr_sgguniqueidentifier = t2.rdr_sgguniqueidentifier
LEFT JOIN cte_gcdsSiebelClientNameMatch t3 ON t1.rdr_sgguniqueidentifier = t3.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobSiebelClientNameMatch t4 ON t1.rdr_sgguniqueidentifier = t4.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobgcdsclientnamematch_v2 t5 ON t1.rdr_sgguniqueidentifier = t5.rdr_sgguniqueidentifier
LEFT JOIN cte_gcdsSiebelClientNameMatch_v2 t6 ON t1.rdr_sgguniqueidentifier = t6.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobSiebelClientNameMatch_v2 t7 ON t1.rdr_sgguniqueidentifier = t7.rdr_sgguniqueidentifier
-- client lifecycle status 1047
LEFT JOIN cte_gcobGcdsClientLifeCycleStatusMatch t8 ON t1.rdr_sgguniqueidentifier = t8.rdr_sgguniqueidentifier
LEFT JOIN cte_gcdsSiebelClientLifeCycleStatusMatch t9 ON t1.rdr_sgguniqueidentifier = t9.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobSiebelClientLifeCycleStatusMatch t10 ON t1.rdr_sgguniqueidentifier = t10.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobGcdsClientLifeCycleStatusMatch_v2 t11 ON t1.rdr_sgguniqueidentifier = t11.rdr_sgguniqueidentifier
LEFT JOIN cte_gcdsSiebelClientLifeCycleStatusMatch_v2 t12 ON t1.rdr_sgguniqueidentifier = t12.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobSiebelClientLifeCycleStatusMatch_v2 t13 ON t1.rdr_sgguniqueidentifier = t13.rdr_sgguniqueidentifier
-- client address 2850
LEFT JOIN cte_gcdsGcobAddressMatch t14 ON t1.rdr_sgguniqueidentifier = t14.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelGcdsAddressMatch t15 ON t1.rdr_sgguniqueidentifier = t15.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelGcobAddressMatch t16 ON t1.rdr_sgguniqueidentifier = t16.rdr_sgguniqueidentifier
LEFT JOIN cte_gcdsGcobAddressMatch_v2 t17 ON t1.rdr_sgguniqueidentifier = t17.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelGcdsAddressMatch_v2 t18 ON t1.rdr_sgguniqueidentifier = t18.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelGcobAddressMatch_v2 t19 ON t1.rdr_sgguniqueidentifier = t19.rdr_sgguniqueidentifier
-- client street name 11490 (tot 17118)
LEFT JOIN cte_gcdsGcobStreetNameMatch t20 ON t1.rdr_sgguniqueidentifier = t20.rdr_sgguniqueidentifier
LEFT JOIN cte_gcdsSiebelStreetNameMatch t21 ON t1.rdr_sgguniqueidentifier = t21.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobSiebelStreetNameMatch t22 ON t1.rdr_sgguniqueidentifier = t22.rdr_sgguniqueidentifier
LEFT JOIN cte_gcdsGcobStreetNameMatch_v2 t23 ON t1.rdr_sgguniqueidentifier = t23.rdr_sgguniqueidentifier
LEFT JOIN cte_gcdsSiebelStreetNameMatch_v2 t24 ON t1.rdr_sgguniqueidentifier = t24.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobSiebelStreetNameMatch_v2 t25 ON t1.rdr_sgguniqueidentifier = t25.rdr_sgguniqueidentifier
-- email address 15214 (tot 32332) -> 32265
LEFT JOIN cte_gcobSiebelEmailAddressMatch t26 ON t1.rdr_sgguniqueidentifier = t26.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobSiebelEmailAddressMatch_v2 t27 ON t1.rdr_sgguniqueidentifier = t27.rdr_sgguniqueidentifier
-- GCO name & location 18651 -> 50929 
LEFT JOIN cte_gcobGcdsGlobalClientOwnerMatch t28 ON t1.rdr_sgguniqueidentifier = t28.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobGcdsGlobalClientOwnerMatch_v2 t29 ON t1.rdr_sgguniqueidentifier = t29.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobGcdsGlobalClientOwnerLocationMatch t30 ON t1.rdr_sgguniqueidentifier = t30.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobGcdsGlobalClientOwnerLocationMatch_v2 t31 ON t1.rdr_sgguniqueidentifier = t31.rdr_sgguniqueidentifier
-- KVK 500
LEFT JOIN cte_gcobGcdsDutchIncorporationNumberMatch t32 ON t1.rdr_sgguniqueidentifier = t32.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelGcdsDutchIncorporationNumberMatch t33 ON t1.rdr_sgguniqueidentifier = t33.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelGcobDutchIncorporationNumberMatch t34 ON t1.rdr_sgguniqueidentifier = t34.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobGcdsDutchIncorporationNumberMatch_v2 t35 ON t1.rdr_sgguniqueidentifier = t35.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelGcdsDutchIncorporationNumberMatch_v2 t36 ON t1.rdr_sgguniqueidentifier = t36.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelGcobDutchIncorporationNumberMatch_v2 t37 ON t1.rdr_sgguniqueidentifier = t37.rdr_sgguniqueidentifier
-- incorporation number 26107 --> 77536
LEFT JOIN cte_gcobGcdsNonDutchIncorporationNumberMatch t38 ON t1.rdr_sgguniqueidentifier = t38.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelGcdsNonDutchIncorporationNumberMatch t39 ON t1.rdr_sgguniqueidentifier = t39.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelGcobNonDutchIncorporationNumberMatch t40 ON t1.rdr_sgguniqueidentifier = t40.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobGcdsNonDutchIncorporationNumberMatch_v2 t41 ON t1.rdr_sgguniqueidentifier = t41.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelGcdsNonDutchIncorporationNumberMatch_v2 t42 ON t1.rdr_sgguniqueidentifier = t42.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelGcobNonDutchIncorporationNumberMatch_v2 t43 ON t1.rdr_sgguniqueidentifier = t43.rdr_sgguniqueidentifier

-- missing clients --> 23040
LEFT JOIN cte_siebelNotInGcob t44 ON t1.rdr_sgguniqueidentifier = t44.rdr_sgguniqueidentifier
LEFT JOIN cte_siebelNotInGcds t45 ON t1.rdr_sgguniqueidentifier = t45.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobNotInGcds t46 ON t1.rdr_sgguniqueidentifier = t46.rdr_sgguniqueidentifier
LEFT JOIN cte_gcobWithoutGcid t47 ON t1.rdr_sgguniqueidentifier = t47.rdr_sgguniqueidentifier
LEFT JOIN cte_gcdsNotInGcob t48 ON t1.rdr_sgguniqueidentifier = t48.rdr_sgguniqueidentifier

'''

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS radar.sggmismatchesdata')
spark.sql(sggmismatchesdataquery_01).write.mode('overwrite').saveAsTable('radar.sggmismatchesdata')

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS radar.sggmismatchesdata_v2')
spark.sql(f'''

    with cte_finalquery_v2 AS (
    SELECT DISTINCT
        t1.rdr_sgguniqueidentifier
        , t1.rdr_gcid
        , t1.rdr_uniquegcobid
        , t1.rdr_siebelid
        , t1.rdr_gcdsfiorcorp
        , t1.rdr_sebastiaanscope
        , t1.rdr_gcobgcdsclientnamematch
        , t1.rdr_gcdssiebelclientnamematch
        , t1.rdr_gcobsiebelclientnamematch
        , t1.rdr_gcobgcdsclientlifecyclestatusmatch
        , t1.rdr_gcdssiebelclientlifecyclestatusmatch
        , t1.rdr_gcobsiebelclientlifecyclestatusmatch
        , t1.rdr_gcdsgcobaddressmatch
        , t1.rdr_siebelgcdsaddressmatch
        , t1.rdr_siebelgcobaddressmatch
        , t1.rdr_gcdsgcobstreetnamematch
        , t1.rdr_gcdssiebelstreetnamematch
        , t1.rdr_gcobsiebelstreetnamematch
        , t1.rdr_gcobsiebelemailaddressmatch
        , t1.rdr_gcobgcdsglobalclientownermatch
        , t1.rdr_gcobgcdsglobalclientownerlocationmatch
        , t1.rdr_gcobgcdsdutchincorporationnumbermatch
        , t1.rdr_siebelgcdsdutchincorporationnumbermatch
        , t1.rdr_siebelgcobdutchincorporationnumbermatch
        , t1.rdr_gcobgcdsnondutchincorporationnumbermatch
        , t1.rdr_siebelgcdsnondutchincorporationnumbermatch
        , t1.rdr_siebelgcobnondutchincorporationnumbermatch
        , t1.rdr_siebelnotingcob
        , t1.rdr_siebelnotingcds
        , t1.rdr_gcobnotingcds
        , t1.rdr_gcobwithoutgcid
        , t1.rdr_gcdsnotingcob
        , t2.FullLegalName as rdr_gcobclientname
        , t3.SiebelClientName as rdr_siebelclientname
        , t4.gcdsClientName as rdr_gcdsclientname
    FROM radar.sggmismatchesdata t1
    LEFT JOIN radar.sgggcobdata t2 ON t1.rdr_uniquegcobid = t2.uniquegcobid
    LEFT JOIN radar.siebelclientinformation t3 ON t1.rdr_siebelid = t3.siebelid
    LEFT JOIN radar.gcdsrawdata t4 ON t1.rdr_gcid = t4.gcid AND t4.gcdsPartyType IN ('Foreign Branch', 'Legal Entity')
)
, cte_duplicates AS (
    SELECT rdr_sgguniqueidentifier
    FROM cte_finalquery_v2
    GROUP BY rdr_sgguniqueidentifier
    HAVING COUNT(*) > 1
)


SELECT *
FROM cte_finalquery_v2
WHERE rdr_sgguniqueidentifier NOT IN (
    SELECT rdr_sgguniqueidentifier
    FROM cte_duplicates
)

UNION

SELECT DISTINCT
    t1.rdr_sgguniqueidentifier,
    t1.rdr_gcid,
    t1.rdr_uniquegcobid,
    t1.rdr_siebelid,
    t1.rdr_gcdsfiorcorp,
    TRUE AS rdr_sebastiaanscope,
    t1.rdr_gcobgcdsclientnamematch,
    t1.rdr_gcdssiebelclientnamematch,
    t1.rdr_gcobsiebelclientnamematch,
    t1.rdr_gcobgcdsclientlifecyclestatusmatch,
    t1.rdr_gcdssiebelclientlifecyclestatusmatch,
    t1.rdr_gcobsiebelclientlifecyclestatusmatch,
    t1.rdr_gcdsgcobaddressmatch,
    t1.rdr_siebelgcdsaddressmatch,
    t1.rdr_siebelgcobaddressmatch,
    t1.rdr_gcdsgcobstreetnamematch,
    t1.rdr_gcdssiebelstreetnamematch,
    t1.rdr_gcobsiebelstreetnamematch,
    t1.rdr_gcobsiebelemailaddressmatch,
    t1.rdr_gcobgcdsglobalclientownermatch,
    t1.rdr_gcobgcdsglobalclientownerlocationmatch,
    t1.rdr_gcobgcdsdutchincorporationnumbermatch,
    t1.rdr_siebelgcdsdutchincorporationnumbermatch,
    t1.rdr_siebelgcobdutchincorporationnumbermatch,
    t1.rdr_gcobgcdsnondutchincorporationnumbermatch,
    t1.rdr_siebelgcdsnondutchincorporationnumbermatch,
    t1.rdr_siebelgcobnondutchincorporationnumbermatch,
    t1.rdr_siebelnotingcob,
    t1.rdr_siebelnotingcds,
    t1.rdr_gcobnotingcds,
    t1.rdr_gcobwithoutgcid,
    t1.rdr_gcdsnotingcob,
    t1.rdr_gcobclientname,
    t1.rdr_siebelclientname,
    t1.rdr_gcdsclientname
FROM cte_finalquery_v2 t1
JOIN cte_duplicates t2 ON t1.rdr_sgguniqueidentifier = t2.rdr_sgguniqueidentifier;

'''
).write.mode('overwrite').saveAsTable('radar.sggmismatchesdata_v2')

# COMMAND ----------

upsert_to_dataverse_table(dataverse_api_url,'rdr_sggcomparisonses', 'select * from radar.sggmismatchesdata_v2', access_token, 'rdr_sgguniqueidentifier',  'rdr_sggcomparisonsid', 1000) 

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS radar.sggmismatchesdata')
spark.sql('DROP TABLE IF EXISTS radar.sggmismatchesdata_v2')
