## This file will contain generic functions that cover functionality for multiple notebooks / activities.
# Version & Changes

#|     Developer    |Date	      | PBI/Bug No   |	Changes done |
#|----------        |----------   |----------    |----------|
#| Rhea Gupta       | 1-Sept-2026 |              |First release 
# Import Required Functions

import os
import re
import requests
from datetime import datetime,timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col, concat_ws, regexp_extract, to_date, lit, date_format
from pyspark.sql import functions as F
import logging
from typing import Any


# required for authenticate part
from databricks.sdk.runtime import dbutils, spark

# Fetching environment variables
application_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']
environment=os.environ['ENV']

# Retrieving Client Secret from connected Secrets
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{application_id}")


def authenticate_storage_account(write_storage):
    #Declaring constants to reduce duplication
    account_url = f"{write_storage}.dfs.core.windows.net"
    
    # This method is for Configuring Spark to access SARADAR Storage account using OAuth authentication
    spark.conf.set(f"fs.azure.account.auth.type.{account_url}","OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{account_url}", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_url}", application_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_url}", service_credential) 
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{account_url}", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

    print('Connection to ' + write_storage +' storage account is successful.')

def get_load_date(source):
    today = datetime.today()
    if source in ['GCOB', 'Legacy2', 'GIC', 'KN1', 'Siebel', 'RadarDataModel','GCDS']:
        load_date = today.strftime('%Y%m%d')
    elif source == 'NLSVF':
        load_date = (today - timedelta(days=2)).strftime('%Y-%m-%d')
    else:
        raise ValueError(f"Unknown source '{source}' for load_date logic.")

    return load_date

def determine_version(source, dataobject, files, version_mapping_by_source, version_num):
    
    if source=='RadarDataModel':
        return version_num
    
    if source in version_mapping_by_source:
        source_versions = version_mapping_by_source[source]
        version = next((v for v, objs in source_versions.items() if dataobject in objs),None)
        
        if version is None:
            raise ValueError(f"{source} dataobject '{dataobject}' not found in version mapping.")

        return version
        
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
    return version

def get_matching_partition(partition_folders, load_date, source, dataobject, base_path):
    
    partition_folders_df = spark.createDataFrame(partition_folders)

    row = partition_folders_df \
        .filter(col("name") != "_delta_log/") \
        .withColumn("ymd", regexp_extract(col("name"), r"=(\d{8})", 1)) \
        .filter(col("ymd") != "") \
        .withColumn("folder_date", to_date(col("ymd"), "yyyyMMdd")) \
        .filter(col("folder_date").isNotNull()) \
        .filter(col("folder_date") == to_date(lit(load_date), "yyyyMMdd")) \
        .head(1)

    if row:
        matching_partition = row[0]["name"]
    else:
        matching_partition = None
        print("No valid matching_partition found.")
            
    if source == 'GCDS' and matching_partition is None:
        previous_date = (datetime.strptime(load_date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')
        matching_partition = next((file.name for file in partition_folders if previous_date in file.name),None)

    if not matching_partition:
        raise RuntimeError(
            f"\nPartition file not found for,\n"
            f"source: {source}\n"
            f"load_date: {load_date}\n"
            f"dataobjectName: {dataobject}\n"
            f"Path: {base_path}\n"
        )

    return matching_partition
            
def read_gdp_defined_dataobjects(source , dataobject, path_prefix='', version_num=3):

    print(f"{source}: Processing \033[1m{dataobject}\033[0m")
    # Determine load_date based on source
    load_date=get_load_date(source)

    # Read storage account names from environment
    eu_gdp_defined_storage = os.environ['GDP_STORAGE_NAME']
    sa_gdp_defined_storage = os.environ['GDP_SA_STORAGE_NAME']
    na_gdp_defined_storage = os.environ['GDP_NA_STORAGE_NAME']


    # Map sources to containers
    container_map = {
        'GCDS': 'gcds',
        'GCOB': 'gcob',
        'Legacy2': 'gcob',
        'GIC': 'gic',
        'KN1': 'kn1',
        'NLSVF': 'nlsvf',
        'Siebel': 'siebel-idaa-cdf',
        'RadarDataModel' : 'fec-radar'
    }

    # Map storage accounts to sources
    storage_sources_map = {
        eu_gdp_defined_storage: ['GCDS', 'GCOB', 'Legacy2', 'Siebel','RadarDataModel'] , 
        sa_gdp_defined_storage: ['GIC', 'KN1'],
        na_gdp_defined_storage: ['NLSVF']
    }

    # Determine the correct storage account for the source
    storage_account = next((account for account, sources in storage_sources_map.items() if source in sources), None)
    if not storage_account:
        raise ValueError(f"source '{source}' not mapped to any known storage account.")

    # Determine the correct container
    container = container_map.get(source)
    if not container:
        raise ValueError(f"source '{source}' not mapped to any known container.")

    # Build the base path
    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path_prefix}/{dataobject}"
    
    # Mapping for Siebel data objects to their specific versions
    version_mapping_by_source = {
    'Siebel': {
        3: ['cdf_ggm_rel_x_ar_hist'],
        2: ['cdf_ggm_org_hist', 'cdf_ggm_ar_hist', 'cdf_ggm_rel_x_rel_hist', 'cdf_ggm_np_hist']
    },
    'GCDS': {
        4602: ['client_Client', 'client_KeyStoreKey', 'client_PartyRole', 'client_PartytoPartyRelationship','client_ClientOwnersProduct','client_ClientOwnersLocal','client_OnboardedLocations','client_RMA' ,'client_Products']
    }
    }
    try:
        # List files in the base path
        files = dbutils.fs.ls(base_path)

        # Determine version
        version = determine_version(source, dataobject, files, version_mapping_by_source, version_num)
    
        # Detect partition folder containing load_date
        partition_base_path = f"{base_path}/{version}/data/"
        partition_folders = dbutils.fs.ls(partition_base_path)

        matching_partition = get_matching_partition(partition_folders, load_date, source, dataobject, base_path)

        full_path = f"{partition_base_path}{matching_partition}"
        print(f"Reading from path: {full_path}")


        # Read data using appropriate format
        if source == 'Siebel' or (source=='GCOB' and version==103) or (source=='RadarDataModel' and version>=2):
            spark.read.format('delta').load(full_path).createOrReplaceGlobalTempView(dataobject)
        else:
            spark.read.parquet(f"{full_path}/*.parquet").createOrReplaceGlobalTempView(dataobject)

        # Print row count
        row_count = spark.sql(f"SELECT COUNT(*) FROM global_temp.{dataobject}").collect()[0][0]
        print(f"No. of rows read : {row_count}")
        print(f"{dataobject} read successfully\n")

    except Exception as e:
        raise RuntimeError(f"Error reading {dataobject} from {source}: {e}")


def write_to_unity_catalog(df, catalog, schema, table):
    #Generic function to write dataframes to Unity Catalog tables. 

    if not spark.catalog.databaseExists(f"{catalog}.{schema}"):
        raise ValueError(f"Target schema {catalog}.{schema} does not exist.")

    target_table = f"{catalog}.{schema}.{table}"

    #Here, overwriteSchema is at the Table level. This means we can add, remove, and change data types of columns in a table.
    df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(target_table)

    print(f'Succesfully wrote {df.count()} rows to {target_table}')

    # Fetch all records from a paginated Microsoft Graph endpoint.
def get_graph_paginated_data(url: str, headers: dict[str, str], timeout: int = 30
                             ) -> list[dict[str, Any]]:
    """
    Fetch all records from a paginated Microsoft Graph endpoint.

    Args:
        url: Initial Graph API URL.
        headers: Request headers.
        timeout: Request timeout in seconds.

    Returns:
        List of records from all pages.
    """
    LOGGER = logging.getLogger(__name__)
    records: list[dict[str, Any]] = []

    while url:
        try:
            response = requests.get(url=url, headers=headers, timeout=timeout)
            response.raise_for_status()

            result = response.json()

            records.extend(result.get("value", []))
            url = result.get("@odata.nextLink")

        except requests.exceptions.RequestException as exc:
            LOGGER.error("Graph API request failed: %s", exc)
            break

    return records


def get_group_members(groups: dict[str, Any], headers: dict[str, str]
                      ) -> list[dict[str, Any]]:
    """
    Retrieve all members for all groups.
    """
    GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0/groups"
    all_members: list[dict[str, Any]] = []

    for group in groups.get("value", []):
        group_id = group.get("id")
        group_name = group.get("displayName")

        if not group_id:
            continue

        endpoint = f"{GRAPH_BASE_URL}/{group_id}/members?$count=true"

        members = get_graph_paginated_data(url=endpoint, headers=headers)

        for member in members:
            member["GroupName"] = group_name

        all_members.extend(members)

    return all_members

def get_group_members_df(spark: SparkSession, groups: dict, headers: dict[str, str]
                         ) -> DataFrame:
    records = get_group_members(groups=groups, headers=headers)

    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("displayName", StringType(), True),
            StructField("userPrincipalName", StringType(), True),
            StructField("mail", StringType(), True),
            StructField("GroupName", StringType(), True),
        ]
    )
    return spark.createDataFrame(records, schema=schema)
