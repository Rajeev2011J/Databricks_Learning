# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal:
# MAGIC To Load the Powerapps Dataverse planning tables data into radar storage accountont.
# MAGIC ### Authors
# MAGIC - Prasad.Gadidala@rabobank.com
# MAGIC ### Flow of Notebook Logic:
# MAGIC - load the right functions.
# MAGIC - read the data from dataverese API and normalize the data and load into Radar storage account.
# MAGIC - The list of tables form dataveres related to planning tables only.
# MAGIC - The storage account used is Radar storage and the path is databricks@saradar/dataverse/.../1/data/LOAD_DT=

# COMMAND ----------

# DBTITLE 1,Import libraries
import os
import requests
from functions_dataverse import get_access_token, upsert_to_dataverse_table, get_dataverse_data
from RadarUtils import *
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# DBTITLE 1,get the secrets
app_reg_app_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,Connecting Storage Account
saradar_container = 'databricks'
saradar_write_storage = f'saradar{environment}'
authenticate_storage_account(environment, saradar_write_storage)

# COMMAND ----------

# DBTITLE 1,Connecting PowerappsDataverese API url
dataverse_url = os.environ['DATAVERSE_URL']
dataverse_api_url = f"{dataverse_url}/api/data/v9.1/"

# COMMAND ----------

# DBTITLE 1,get Access token
access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)

# COMMAND ----------

# DBTITLE 1,Define today's date for partitioning
DateToday = datetime.today().strftime('%Y-%m-%d')

# COMMAND ----------

# DBTITLE 1,final code to read data from dataverse and load into storage
# Define headers for the API request
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0",
    "Accept": "application/json"
}

# List of Power Apps Dataverse logical table names
table_names = [
    "rdr_caseplanningdetailses",
    "rdr_caseremarks",
    "rdr_cdddepartments",
    "rdr_cddplanningincludedcaseses",
    "rdr_departments",
    "rdr_sprintstatuses",
    "rdr_sprintstatuscasephasemappings",
    "rdr_sprintstatuslogs",
    "rdr_teams",
    "rdr_userteamregistries"
]

# System metadata fields to exclude
system_metadata_fields = [
    "@odata.etag",
    "_createdby_value",
    "_owninguser_value",
    "_ownerid_value",
    "statecode",
    "statuscode",
    "versionnumber",
    "_owningteam_value",
    "_modifiedby_value",
    "_modifiedonbehalfby_value",
    "_createdonbehalfby_value",
    "_owningbusinessunit_value",
    #"createdon",
    "modifiedon",
    "utcconversiontimezonecode",
    "timezoneruleversionnumber",
    "overriddencreatedon",
    "importsequencenumber"
]

# Function to fetch all records with pagination
def fetch_all_records(table_name, headers, dataverse_api_url):
    all_data = []
    url = f"{dataverse_api_url}{table_name}"
    while url:
        response = requests.get(url, headers=headers)
        json_data = response.json()
        all_data.extend(json_data.get('value', []))
        url = json_data.get('@odata.nextLink', None)
    return all_data

# Loop through all tables and write to ADLS
for table_name in table_names:
    data = fetch_all_records(table_name, headers, dataverse_api_url)
    if data:
        # Get all unique keys and exclude metadata fields
        all_keys = set().union(*(row.keys() for row in data))
        filtered_keys = [
            key for key in all_keys
            if not key.startswith('@odata.')
            and key not in system_metadata_fields
        ]

        # Normalize rows to include only filtered (business) fields
        normalized_data = [
            {key: str(row.get(key, None)) if row.get(key, None) is not None else None for key in filtered_keys}
            for row in data
        ]

        # Create Spark DataFrame with filtered columns as StringType
        schema = StructType([StructField(key, StringType(), True) for key in filtered_keys])
        sdf = spark.createDataFrame(normalized_data, schema=schema)

        # Create temp view and write to Parquet
        sdf.createOrReplaceTempView(table_name)
        output_path = f"abfss://{saradar_container}@{saradar_write_storage}.dfs.core.windows.net/DATAVERSE/{table_name}/1/data/LOAD_DT={DateToday}"
        sdf.write.mode("overwrite").parquet(output_path)

        # Try block to verify write success
        try:
            verify_df = spark.read.parquet(output_path)
            print(f"Data written and verified for table: {table_name} — {verify_df.count()} rows.")
        except Exception as e:
            print(f"Data write verification failed for table: {table_name}")
            print(f"Details: {str(e)}")
