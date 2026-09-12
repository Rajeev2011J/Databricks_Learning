# Databricks notebook source
# Fetching environment variables
import os
app_reg_app_id = os.environ['APP_REG_APP_ID']
TenantId = os.environ['TENANT_ID']
environment=os.environ['ENV']
ReadStorage = f'saradar{environment}'
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

#Local File Import
from RadarUtils import *

# COMMAND ----------

#Connection to SARADAR Storage account
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

storage_accnt = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(storage_accnt)

# COMMAND ----------

#Derive the date for which data has to be processes
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
Today = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
load_dts = 'Load_date' + Today

print (load_dts)

date_parameter = datetime.today().strftime('%Y%m%d')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_AllPartyDetails',
'party_client_structure_GUI',
'party_case_client_details',
'party_products_and_services',
]

# Create TempView for each loading table
for Object in load_df:
    fetch_latest_file(Object, date_parameter, storage_accnt, CaseService=True)

# COMMAND ----------

load_df = pd.DataFrame({'RadarDataobject': ['WR001_v2', 'WR002']})
# ['WR003','WR004','WR005','WR006','INDIA','EUROPEAFRICA','WR009','WR010']

for index, row in load_df.iterrows():
    radar_id = row['RadarDataobject']
    path = f'abfss://ad-hoc-exports@{ReadStorage}.dfs.core.windows.net/gns/{radar_id}/{load_dts}.parquet'
    spark.read.parquet(path).createOrReplaceTempView(radar_id.split('_', 1)[0])


# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS testing.GNS_test_Log (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   timestamp TIMESTAMP,
# MAGIC   test_scenario STRING,
# MAGIC   status STRING,
# MAGIC   message STRING
# MAGIC );
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("GNSValidationLogger").getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

def log_to_gns_test_log(test_scenario, status, message):
    log_df = spark.createDataFrame(
        [(datetime.now(), test_scenario, status, message)],
        ["timestamp", "test_scenario", "status", "message"]
    )
    log_df.write.mode("append").saveAsTable("testing.GNS_test_Log")


# COMMAND ----------

# DBTITLE 1,Active_MainClients
# MAGIC %sql
# MAGIC CREATE OR REPLaCE TEMP VIEW Active_MainClients as
# MAGIC Select *,Case when ClientType = 'LegalEntityClient' then concat('LEC_',Id)
# MAGIC       when ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then concat('NP_NPPC_',Id)
# MAGIC End as SourceClient from party_AllPartyDetails papd where papd.CaseStatusName = 'Completed' AND papd.ClientLifeCycleStatus='Client' and  papd.IsLatestApprovedVersionOfClient=True 

# COMMAND ----------

# DBTITLE 1,Final_GNS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Final_GNS AS
# MAGIC Select * from WR001
# MAGIC UNION
# MAGIC Select * from WR002
# MAGIC -- UNION
# MAGIC -- Select * from WR003
# MAGIC -- UNION
# MAGIC -- Select * from WR004
# MAGIC -- UNION
# MAGIC -- Select * from WR005
# MAGIC -- UNION
# MAGIC -- Select * from WR006
# MAGIC -- UNION
# MAGIC -- Select * from INDIA
# MAGIC -- UNION
# MAGIC -- Select * from EUROPEAFRICA
# MAGIC -- UNION
# MAGIC -- Select * from WR009
# MAGIC -- UNION
# MAGIC -- Select * from WR010

# COMMAND ----------

# MAGIC %md
# MAGIC ##Active clients in GNS file Check

# COMMAND ----------

# DBTITLE 1,Validate_partyids function
def validate_partyids(table_location_map):
    """
    Validates PartyIds for each WR table using its specific location filters.
    Dynamically uses each WR table to fetch ListUids.

    Parameters:
    - table_location_map: Dictionary where keys are WR table names and values are lists of location strings.

    Returns:
    - Dictionary with validation results per table.
    """
    results = {}

    for table_name, locations in table_location_map.items():
        # Step 1: Prepare location filters
        quoted_locations = ", ".join([f"'{loc}'" for loc in locations])

        step1_filter = (
            f"pccd.GlobalClientOwnerLocation IN ({quoted_locations}) OR "
            f"pbl.ProductOfferingLocation IN ({quoted_locations}) OR "
            f"pbl.BookingEntityLocation IN ({quoted_locations})"
        )
        step2_filter = (
            f"cs.GlobalClientOwnerLocation IN ({quoted_locations}) OR "
            f"pbl.ProductOfferingLocation IN ({quoted_locations}) OR "
            f"pbl.BookingEntityLocation IN ({quoted_locations})"
        )

        # Step 2: Get Related Parties
        related_parties = spark.sql(f"""
            WITH PartyMap AS (                                    
            SELECT DISTINCT cs.UniqueParentPartyId AS PartyId,
            CASE WHEN papd.GCDSID IS NULL AND papd.ClientType='LegalEntityClient' THEN concat('LE_',papd.GcobId)
            WHEN papd.GCDSID IS NULL AND papd.ClientType IN ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_',papd.GcobId)
            WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedLegalEntity' THEN concat('RLEP_',papd.GcobId)
            WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedNaturalPerson' THEN concat('RNPP_',papd.GcobId)
            ELSE papd.GCDSID END AS ListUid,        
            cs.ClientStructureSnapshotId, papd.uniquepartyid,papd.gcdsid
            FROM party_client_structure_GUI cs
            INNER JOIN party_AllPartyDetails papd ON cs.ParentEntityId = papd.PartyId AND cs.UniqueParentPartyId = papd.UniquePartyId
            LEFT JOIN party_case_client_details pccd ON cs.SourceClient = pccd.SourceClient
            LEFT JOIN party_products_and_services pbl ON cs.SourceClient = pbl.SourceClient
            WHERE cs.CaseStatusName = 'Completed'
              AND cs.ClientLifeCycleName = 'Client'
              AND cs.IsLatestApprovedVersionOfClient = TRUE
              AND ({step1_filter})

            UNION

             SELECT DISTINCT cs.UniqueChildPartyId AS PartyId,
            CASE WHEN papd.GCDSID IS NULL AND papd.ClientType='LegalEntityClient' THEN concat('LE_',papd.GcobId)
            WHEN papd.GCDSID IS NULL AND papd.ClientType IN ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_',papd.GcobId)
            WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedLegalEntity' THEN concat('RLEP_',papd.GcobId)
            WHEN papd.GCDSID IS NULL AND papd.ClientType='RelatedNaturalPerson' THEN concat('RNPP_',papd.GcobId)
            ELSE papd.GCDSID END AS ListUid,        
            cs.ClientStructureSnapshotId, papd.uniquepartyid,papd.gcdsid
            FROM party_client_structure_GUI cs
            INNER JOIN party_AllPartyDetails papd ON cs.ChildEntityId = papd.PartyId AND cs.UniqueChildPartyId = papd.UniquePartyId
            LEFT JOIN party_case_client_details pccd ON cs.SourceClient = pccd.SourceClient
            LEFT JOIN party_products_and_services pbl ON cs.SourceClient = pbl.SourceClient
            WHERE cs.CaseStatusName = 'Completed'
              AND cs.ClientLifeCycleName = 'Client'
              AND cs.IsLatestApprovedVersionOfClient = TRUE
              AND ({step1_filter})

            ),
            LatestSnapshot AS (
            SELECT ListUid, MAX(ClientStructureSnapshotId) AS MaxSnapshotId
            FROM PartyMap
            GROUP BY ListUid
            )
            SELECT DISTINCT PartyMap.uniquepartyid as PartyId
            FROM PartyMap JOIN LatestSnapshot
            ON PartyMap.ListUid = LatestSnapshot.ListUid
            AND PartyMap.ClientStructureSnapshotId = LatestSnapshot.MaxSnapshotId  
        """)

        # Step 3: Get Active Clients
        active_clients = spark.sql(f"""
            SELECT DISTINCT cs.UniquePartyId AS PartyId
            FROM Active_MainClients cs
            INNER JOIN party_products_and_services pbl ON cs.SourceClient = pbl.SourceClient
            WHERE ({step2_filter})
        """)

        # Step 4: Combine Expected PartyIds
        expected_partyids = related_parties.union(active_clients).distinct()
        expected_partyids.createOrReplaceTempView("expected_partyids")
        expected_count = expected_partyids.count()

        # Step 5: Get Actual PartyIds from WR table
        actual_partyids = spark.sql(f"SELECT DISTINCT UniquePartyId FROM {table_name}")
        actual_count = actual_partyids.count()

        # Step 6: Find Missing PartyIds
        missing_partyids = expected_partyids.subtract(actual_partyids)
        missing_count = missing_partyids.count()

        if missing_count == 0:
            status = "Pass"
            message = f"✅ All expected PartyIds ({expected_count}) are present in {table_name}."
        else:
            missing_partyids.createOrReplaceTempView("missing_partyids")

            # Step 7: Get GCDSIds for missing PartyIds
            missing_gcdsids = spark.sql("""
                SELECT DISTINCT P.GCDSId
                FROM expected_partyids E LEFT JOIN party_allpartydetails P 
                ON P.UniquePartyId = E.PartyId
                WHERE E.PartyId IN (SELECT PartyId FROM missing_partyids)
            """)

            # Step 8: Get ListUids from the same WR table
            wr_listuids = spark.sql(f"SELECT DISTINCT ListUid FROM {table_name}")

            # Step 9: Find Missing ListUids
            missing_listuids = missing_gcdsids.subtract(wr_listuids)
            missing_listuid_count = missing_listuids.count()

            if missing_listuid_count > 0:
                missing_listuids_str = ", ".join([str(row["GCDSId"]) for row in missing_listuids.collect()])
                status = "Fail"
                message = (
                    f"❌ Found {missing_listuid_count} ListUids corresponding to missing PartyIds not present in {table_name}.\n"
                    f"Missing ListUids: {missing_listuids_str}"
                )
            else:
                status = "Pass"
                message = f"✅ All ListUids corresponding to missing PartyIds are present in {table_name}."

        # Step 10: Log and store result
        log_to_gns_test_log(
            test_scenario=f"{table_name}_PartyIdCheck",
            status=status,
            message=message
        )

        results[table_name] = {
            "status": status,
            "message": message,
            "expected_count": expected_count,
            "actual_count": actual_count,
            "missing_partyids_count": missing_count
        }

    return results

# COMMAND ----------

# DBTITLE 1,calling the function
table_location_map = {
    "WR001": ["Rabo Securities Canada, Inc. (RSCI)",
        "Rabo Securities USA, Inc. (RSEC)",
        "Rabobank - USA Rabo AgriFinance",
        "Rabobank Canada (RCBR)",
        "Rabobank Canada(Rural)",
        "Rabobank New York"],
    "WR002": ["Rabobank Chile"],
    # "WR003": ["Rabobank Brazil"],
    # "WR004": ['Rabobank Hong Kong'],
    # "WR005": ['Rabobank Singapore'],
    # "WR006": ['Rabobank China'],
    # "INDIA": ['Rabobank India'],
    # "EUROPEAFRICA": ['Rabobank Antwerp','Rabobank Dublin','Rabobank Frankfurt','Rabobank London','Rabobank Argentina','Rabobank Madrid','Rabobank Milan','Rabobank Netherlands','Rabobank Kenya','Rabobank Paris','Rabobank Foundation','Rabobank - Smallholder Agroforestry Finance (SAF)','Rabobank Turkey'],
    # "WR009": ['Rabobank Australia','Rabobank New Zealand','Rabobank - RANZ Country Banking and ROS']
}

results = validate_partyids(table_location_map)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Active clients validation in WR010 file

# COMMAND ----------

# DBTITLE 1,validate_partyids function for WR010
# def validate_partyids_WR010(WR010):
#     results = {}

#     # Step 1: Get Related Parties
#     related_parties = spark.sql("""
#         WITH PartyMap AS (
#             SELECT DISTINCT cs.UniqueParentPartyId AS PartyId,
#                 CASE 
#                     WHEN papd.GCDSID IS NULL AND papd.ClientType = 'LegalEntityClient' THEN concat('LE_', papd.GcobId)
#                     WHEN papd.GCDSID IS NULL AND papd.ClientType IN ('NaturalPersonClient', 'Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_', papd.GcobId)
#                     WHEN papd.GCDSID IS NULL AND papd.ClientType = 'RelatedLegalEntity' THEN concat('RLEP_', papd.GcobId)
#                     WHEN papd.GCDSID IS NULL AND papd.ClientType = 'RelatedNaturalPerson' THEN concat('RNPP_', papd.GcobId)
#                     ELSE papd.GCDSID 
#                 END AS ListUid,
#                 cs.ClientStructureSnapshotId, papd.uniquepartyid, papd.gcdsid
#             FROM party_client_structure_GUI cs
#             INNER JOIN party_AllPartyDetails papd 
#                 ON cs.ParentEntityId = papd.PartyId AND cs.UniqueParentPartyId = papd.UniquePartyId
#             LEFT JOIN party_case_client_details pccd ON cs.SourceClient = pccd.SourceClient
#             LEFT JOIN party_products_and_services pbl ON cs.SourceClient = pbl.SourceClient
#             WHERE cs.CaseStatusName = 'Completed'
#               AND cs.ClientLifeCycleName = 'Client'
#               AND cs.IsLatestApprovedVersionOfClient = TRUE
#               AND cs.FIHubIndicator = TRUE

#             UNION

#             SELECT DISTINCT cs.UniqueChildPartyId AS PartyId,
#                 CASE 
#                     WHEN papd.GCDSID IS NULL AND papd.ClientType = 'LegalEntityClient' THEN concat('LE_', papd.GcobId)
#                     WHEN papd.GCDSID IS NULL AND papd.ClientType IN ('NaturalPersonClient', 'Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_', papd.GcobId)
#                     WHEN papd.GCDSID IS NULL AND papd.ClientType = 'RelatedLegalEntity' THEN concat('RLEP_', papd.GcobId)
#                     WHEN papd.GCDSID IS NULL AND papd.ClientType = 'RelatedNaturalPerson' THEN concat('RNPP_', papd.GcobId)
#                     ELSE papd.GCDSID 
#                 END AS ListUid,
#                 cs.ClientStructureSnapshotId, papd.uniquepartyid, papd.gcdsid
#             FROM party_client_structure_GUI cs
#             INNER JOIN party_AllPartyDetails papd 
#                 ON cs.ChildEntityId = papd.PartyId AND cs.UniqueChildPartyId = papd.UniquePartyId
#             LEFT JOIN party_case_client_details pccd ON cs.SourceClient = pccd.SourceClient
#             LEFT JOIN party_products_and_services pbl ON cs.SourceClient = pbl.SourceClient
#             WHERE cs.CaseStatusName = 'Completed'
#               AND cs.ClientLifeCycleName = 'Client'
#               AND cs.IsLatestApprovedVersionOfClient = TRUE
#               AND cs.FIHubIndicator = TRUE
#         ),
#         LatestSnapshot AS (
#             SELECT ListUid, MAX(ClientStructureSnapshotId) AS MaxSnapshotId
#             FROM PartyMap
#             GROUP BY ListUid
#         )
#         SELECT DISTINCT PartyMap.uniquepartyid AS PartyId
#         FROM PartyMap 
#         JOIN LatestSnapshot ON PartyMap.ListUid = LatestSnapshot.ListUid
#             AND PartyMap.ClientStructureSnapshotId = LatestSnapshot.MaxSnapshotId
#     """)

#     # Step 2: Get Active Clients
#     active_clients = spark.sql("""
#         SELECT DISTINCT cs.UniquePartyId AS PartyId
#         FROM Active_MainClients cs
#         INNER JOIN party_products_and_services pbl ON cs.SourceClient = pbl.SourceClient
#         LEFT JOIN party_case_client_details pccd ON cs.SourceClient = pccd.SourceClient
#         --LEFT JOIN party_client_structure_GUI pccd ON cs.SourceClient = pccd.SourceClient
#         where pccd.FIHubIndicator = TRUE
#     """)

#     # Step 3: Combine Expected PartyIds
#     expected_partyids = related_parties.union(active_clients).distinct()
#     expected_partyids.createOrReplaceTempView("expected_partyids")
#     expected_count = expected_partyids.count()

#     # Step 4: Get Actual PartyIds from WR010 table
#     actual_partyids = spark.sql(f"SELECT DISTINCT UniquePartyId FROM {WR010}")
#     actual_count = actual_partyids.count()

#     # Step 5: Find Missing PartyIds
#     missing_partyids = expected_partyids.subtract(actual_partyids)
#     missing_count = missing_partyids.count()

#     if missing_count == 0:
#         status = "Pass"
#         message = f"✅ All expected PartyIds ({expected_count}) are present in {WR010}."
#     else:
#         missing_partyids.createOrReplaceTempView("missing_partyids")
#         missing_partyids_list = [str(row["PartyId"]) for row in missing_partyids.collect()]
#         missing_partyids_str = ", ".join(missing_partyids_list)
#         status = "Fail"
#         message = (
#         f"❌ Missing {missing_count} UniquePartyIds in {WR010}.\n"
#         f"Missing PartyIds: {missing_partyids_str}"
#     )
    
#     # Step 9: Log and store result
#     log_to_gns_test_log(
#         test_scenario=f"{WR010}_PartyIdCheck",
#         status=status,
#         message=message
#     )

#     results[WR010] = {
#         "status": status,
#         "message": message,
#         "expected_count": expected_count,
#         "actual_count": actual_count,
#         "missing_partyids_count": missing_count
#     }

#     return results
    
# results = validate_partyids_WR010("WR010")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplicate Listuid Check

# COMMAND ----------

def check_duplicates(table_name):
    # Query to find duplicates
    duplicate_rows = spark.sql(f"""
        SELECT ListUid, ClientStructureSnapshotId, COUNT(*) AS Listuid_count
        FROM {table_name}
        GROUP BY ListUid, ClientStructureSnapshotId
        HAVING COUNT(*) > 1
    """)
    
    duplicate_count = duplicate_rows.count()
    
    # Prepare status and message
    if duplicate_count > 0:
        status = "Fail"
        duplicates_list = duplicate_rows.collect()
        formatted_duplicates = "\n".join(
            [f"ListUid: {row['ListUid']}, ClientStructureSnapshotId: {row['ClientStructureSnapshotId']}, Count: {row['Listuid_count']}" for row in duplicates_list]
        )
        message = f"❌ Found {duplicate_count} duplicate (ListUid, ClientStructureSnapshotId) combinations in {table_name}:\n{formatted_duplicates}"
    else:
        status = "Pass"
        message = f"✅ No duplicate (ListUid, ClientStructureSnapshotId) combinations found in {table_name}."
    
    # Log result
    log_to_gns_test_log(
        test_scenario=f"{table_name}_DuplicateCheck",
        status=status,
        message=message
    )

#calling the function for each files
tables = ['WR001','WR002']
#  ['WR003','WR004','WR005','WR006','INDIA','EUROPEAFRICA','WR009','WR010']

for table in tables:
    check_duplicates(table)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Listuid NULL Check

# COMMAND ----------

# Step 1: Get rows where ListUid is NULL
null_listuid_rows = spark.sql("""
    SELECT *
    FROM Final_GNS
    WHERE ListUid IS NULL
""")

# Step 2: Count how many such rows exist
null_count = null_listuid_rows.count()

# Step 3: Collect and format rows for message
if null_count > 0:
    status = "Fail"
    rows = null_listuid_rows.collect()
    row_strings = [str(row.asDict()) for row in rows]
    formatted_rows = "\n".join(row_strings)
    message = (
        f"❌ Found {null_count} rows in Final_GNS where ListUid is NULL.\n"
        f"Rows:\n{formatted_rows}"
    )
else:
    status = "Pass"
    message = "✅ No rows in Final_GNS have NULL ListUid."

# Step 4: Log the result
log_to_gns_test_log(
    test_scenario="GNS_NullListUidCheck",
    status=status,
    message=message
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GNS_DuplicatedCommaSeparatedListUidCheck

# COMMAND ----------

# Step 1: Get rows where ListUid has duplicated comma‑separated values
comma_dup_listuid_rows = spark.sql("""
    SELECT *
    FROM Final_GNS
    WHERE size(split(ListUid, ',')) > size(array_distinct(split(ListUid, ',')))
""")

# Step 2: Count such rows
duplicate_count = comma_dup_listuid_rows.count()

# Step 3: Collect and format rows for message
if duplicate_count > 0:
    status = "Fail"
    rows = comma_dup_listuid_rows.collect()
    row_strings = [str(row.asDict()) for row in rows]
    formatted_rows = "\n".join(row_strings)
    message = (
        f"❌ Found {duplicate_count} rows in Final_GNS where ListUid has duplicated comma-separated values.\n"
        f"Rows:\n{formatted_rows}"
    )
else:
    status = "Pass"
    message = "✅ No duplicated comma-separated values found in ListUid."

# Step 4: Log the result
log_to_gns_test_log(
    test_scenario="GNS_DuplicatedCommaSeparatedListUidCheck",
    status=status,
    message=message
)
