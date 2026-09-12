# Databricks notebook source
# generic imports
import os
import re
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from RiskShieldUtils import *

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, generate_container_sas, ContainerSasPermissions
from azure.core.exceptions import ResourceNotFoundError

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

authenticate_storage_account(ReadStorage)
# authenticate_storage_account('wrkycadlsprod0001')
authenticate_storage_account('saradarpreprd')
authenticate_storage_account('saradarprod')

# COMMAND ----------

# MAGIC %md
# MAGIC Code to Create, Delete and List the containers of a storage account.

# COMMAND ----------

account_url = "https://saradarprod.blob.core.windows.net"

credential = ClientSecretCredential(TenantId, app_reg_app_id, service_credential)


blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)


# # Create a new container
# container_name = "radar"
# container_client = blob_service_client.create_container(container_name)


# # Delete the container
# # Warning: make sure to double check the container name. Do not try to delete the wrong container
# container_name = "radar"
# blob_service_client.delete_container(container_name)


containers = blob_service_client.list_containers()
for container in containers:
    print(container['name'])

# COMMAND ----------

def read_files(container, storage):
    authenticate_storage_account(storage)
    folder = f"abfss://{container}@{storage}.dfs.core.windows.net/"
    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .format("binaryFile")
        .load(folder)
    )

    return df.withColumn('path',split(col('path'),'.net/')[1]).select('path','length')

# COMMAND ----------

# MAGIC %md
# MAGIC Code to copy entire container data from one storage account to another

# COMMAND ----------

# Replace with your actual connection strings and container names
source_storage_name = "saradarpreprd"
destination_storage_name = "saradarprod"
source_connection_string = f"https://{source_storage_name}.blob.core.windows.net"
destination_connection_string = f"https://{destination_storage_name}.blob.core.windows.net"
source_container_name = "legacybackup"
destination_container_name = "legacy2backup"
credential = ClientSecretCredential(TenantId, app_reg_app_id, service_credential)


source_blob_service_client = BlobServiceClient(account_url=source_connection_string, credential=credential)
destination_blob_service_client = BlobServiceClient(account_url=destination_connection_string, credential=credential)

# # Generate SAS token for source container
# sas_token = generate_container_sas(
#     account_name=source_blob_service.account_name,
#     container_name=source_container_name,
#     account_key=source_blob_service.credential.account_key,
#     permission=ContainerSasPermissions(read=True, list=True),
#     expiry=datetime.utcnow() + timedelta(hours=1)
# )

# Get container clients
source_container_client = source_blob_service_client.get_container_client(source_container_name)
destination_container_client = destination_blob_service_client.get_container_client(destination_container_name)



# Create destination container if it doesn't exist
try:
    destination_container_client.create_container()
except Exception:
    pass  # Container already exists


# List blobs in the source container
# blobs_list = source_container_client.list_blobs()

source_blobs_list = read_files(source_container_name, source_storage_name)
destination_blobs_list = read_files(destination_container_name, destination_storage_name)

remaining_blobs = source_blobs_list.join(destination_blobs_list, on=["path"], how="left_anti").select('path').rdd.flatMap(lambda x: x).collect()

# for blob in blobs_list:
#     source_blob_client = source_container_client.get_blob_client(blob.name)
#     destination_blob_client = destination_container_client.get_blob_client(blob.name)
#     print(blob.name)

    ##Method1
    # # Get the source blob URL
    # sas_token = 'token'
    # source_blob_url = f"{source_connection_string}/{source_container_name}/{blob.name}?{sas_token}"
   
    # try:
    #     destination_blob_client.start_copy_from_url(source_blob_url)
    #     print(f"Copied: {blob.name}")
    # except Exception as e:
    #     print(f"Failed to copy {blob.name}: {e}")


## Method 2
# for blob in blobs_list:
#     if not blob.name.endswith(".parquet"):
#         continue

#     source_blob_client = source_container_client.get_blob_client(blob.name)
#     destination_blob_client = destination_container_client.get_blob_client(blob.name)

#     try:
#         # Download from source
#         download_stream = source_blob_client.download_blob()
#         data = download_stream.readall()

#         # Delete destination blob if it exists
#         try:
#             destination_blob_client.delete_blob()
#             print(f"Deleted existing blob: {blob.name}")
#         except ResourceNotFoundError:
#             pass  # Blob doesn't exist, no problem

#         # Upload to destination
#         destination_blob_client.upload_blob(data, overwrite=True)
#         print(f"Uploaded: {blob.name}")

#     except Exception as e:
#         print(f"Failed to upload {blob.name}: {e}")


for blob in remaining_blobs:
    # if not blob.endswith(".parquet"):
    #     continue

    source_blob_client = source_container_client.get_blob_client(blob)
    destination_blob_client = destination_container_client.get_blob_client(blob)

    try:
        # Download from source
        download_stream = source_blob_client.download_blob()
        data = download_stream.readall()
        source_blob_size = len(data)

        # Check if destination blob exists
        try:
            dest_properties = destination_blob_client.get_blob_properties()
            dest_blob_size = dest_properties.size

            if source_blob_size == dest_blob_size:
                print(f"Skipped (size match): {blob}")
                continue
            else:
                destination_blob_client.delete_blob()
                print(f"Deleted mismatched blob: {blob}")
        except ResourceNotFoundError:
            pass  # Blob doesn't exist, proceed to upload

        # Upload to destination
        destination_blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded: {blob}")

    except Exception as e:
        print(f"Failed to process {blob}: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC Test script for validating copy activity

# COMMAND ----------

# Replace with your actual connection strings and container names
source_connection_string = "https://saradarpreprd.blob.core.windows.net"
destination_connection_string = "https://saradarprod.blob.core.windows.net"
source_container_name = "legacybackup"
destination_container_name = "legacy2backup"
credential = ClientSecretCredential(TenantId, app_reg_app_id, service_credential)


source_blob_service_client = BlobServiceClient(account_url=source_connection_string, credential=credential)
destination_blob_service_client = BlobServiceClient(account_url=destination_connection_string, credential=credential)

# Get container clients
source_container_client = source_blob_service_client.get_container_client(source_container_name)
destination_container_client = destination_blob_service_client.get_container_client(destination_container_name)


# List blobs in the source container
source_blobs_list = source_container_client.list_blobs()
destination_blobs_list = destination_container_client.list_blobs()

# source_blob_names = [src_blob.name for src_blob in source_blobs_list]
# destination_blob_names = [dest_blob.name for dest_blob in destination_blobs_list]

# for dest_name in destination_blob_names:
#     if dest_name not in source_blob_names:
#         print(dest_name)

# print('Test Completed')



# Build blob dictionaries
source_blobs = {blob.name: blob.size for blob in source_container_client.list_blobs()}
destination_blobs = {blob.name: blob.size for blob in destination_container_client.list_blobs()}

# Compare
missing_blobs = []
size_mismatches = []

for name, size in source_blobs.items():
    if name not in destination_blobs:
        missing_blobs.append(name)
    elif destination_blobs[name] != size:
        size_mismatches.append((name, size, destination_blobs[name]))

# Report
print(f"Total blobs in source: {len(source_blobs)}")
print(f"Total blobs in destination: {len(destination_blobs)}")
print(f"Missing blobs: {len(missing_blobs)}")
print(f"Size mismatches: {len(size_mismatches)}")

if missing_blobs:
    print("\nMissing blobs:")
    for name in missing_blobs:
        print(f"- {name}")

if size_mismatches:
    print("\nSize mismatches:")
    for name, src_size, dst_size in size_mismatches:
        print(f"- {name}: source = {src_size}, destination = {dst_size}")

