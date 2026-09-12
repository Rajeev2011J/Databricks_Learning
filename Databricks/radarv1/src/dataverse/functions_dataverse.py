import requests
import json
import pandas as pd
import uuid
import time
import concurrent
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import DataFrame
from databricks.sdk.runtime import spark
from pyspark.sql import functions as F

def get_access_token(tenant_id, app_reg_app_id, service_credential, url) -> str:
    """
    Get API access token
    """

    authority = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    # create the data payload for token request
    payload = {
        'client_id': app_reg_app_id,
        'client_secret': service_credential,
        'scope': f"{url}/.default",
        'grant_type': 'client_credentials'
    }

    # make the POST request to obtain the token
    response = requests.post(authority, data=payload)
    
    # raise an exception if the request failed
    response.raise_for_status()

    # parse the access token from the response
    token_response = response.json()
    access_token = token_response.get('access_token')

    return access_token


# will continue to loop until all the records are retrieved. @odata.nextLink is a field in the result-set that tells to grab the next 5.000 records.    
def get_dataverse_data(access_token, dataverse_api_url, table_name: str) -> DataFrame:
    """
    Get data from Dataverse table by looping through the data
    """  

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "Prefer": "odata.include-annotations=\"OData.Community.Display.V1.FormattedValue\"",
    }  

    response = requests.get(f'{dataverse_api_url}/{table_name}', headers=headers)
    data = response.json()

    # initialize a list to store the data
    all_records = []

    # deal with empty Dataverse table
    if 'value' in data and data['value']:
        # add the first page of data
        all_records.extend(data['value'])
        
        # loop through the responses until @odata.nextLink is gone
        while "@odata.nextLink" in response.json():
            # request the @odata.nextLink URL
            response = requests.get(response.json()["@odata.nextLink"], headers=headers)
            next_page_data = response.json()

            # append the data returned by the endpoint to the list
            if 'value' in next_page_data:
                all_records.extend(next_page_data['value'])

        # use pandas to help infer schema or directly specify schema if known
        pandas_df = pd.DataFrame(all_records)
        
        # convert to Spark DataFrame
        df = spark.createDataFrame(pandas_df)
        return df
    else:
        # handle the empty or missing data
        print("The Dataverse table is empty")         


def truncate_dataverse_table(dataverse_api_url, table_name: str, access_token: str, primary_key: str, batch_size: int):
    """
    Truncate Dataverse table by deleting all the records in batches, with parallel processing for improved performance.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # URL to query records
    query_url = f"{dataverse_api_url}/{table_name}"
    delete_count = 0

    while True:
        # retrieve a batch of records
        response = requests.get(f"{query_url}?$top={batch_size}", headers=headers)
        
        if response.status_code != 200:
            print(f"Failed to retrieve data: {response.status_code} - {response.text}")
            return

        data = response.json()
        records = data.get('value', [])
        
        if not records:
            print("No more records found to delete.")
            break  # exit loop if there are no more records

        # delete each record in the current batch asynchronously
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_record = {
                executor.submit(_delete_record, query_url, record[primary_key], headers): record for record in records
            }

            for future in concurrent.futures.as_completed(future_to_record):
                try:
                    if future.result():
                        delete_count += 1  # increment counter if the record is deleted successfully
                except Exception as exc:
                    record = future_to_record[future]
                    print(f"Error deleting record {record[primary_key]}: {exc}")

    print(f"{delete_count} records deleted successfully.")


def post_to_dataverse_table(dataverse_api_url, target_table: str, query: str, access_token, batch_size: int):
    """
    Full load to existing Dataverse table using batch inserts with multipart/mixed format.
    """
    # fetch JSON data from Unity Catalog
    data_json = _convert_from_catalog_to_json(query)

    # common headers for batch requests
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Prefer": "return=representation"
    }

    # initialize counter for inserted records and session
    post_count = 0
    session = requests.Session()
    session.headers.update(headers)

    # process the records in batches
    for i in range(0, len(data_json), batch_size):
        batch = data_json[i:i + batch_size]  # get the current batch of records

        # prepare the multipart request
        boundary = f"batch_{uuid.uuid4()}"
        batch_body = _prepare_multipart_body(batch, target_table, boundary)

        # send the batch request
        try:
            response = session.post(
                f'{dataverse_api_url}/$batch',
                headers={
                    "Content-Type": f"multipart/mixed; boundary={boundary}",
                    "Authorization": f"Bearer {access_token}"
                },
                data=batch_body
            )
            response.raise_for_status()
            post_count += len(batch)  # increment the counter by the size of the batch

        except requests.exceptions.RequestException as e:
            print(f"Failed to insert batch starting with index {i}: {e}")
            print(f"Response content: {response.content}")  # log response content for debugging
            continue

    # print a summary message
    print(f"{post_count} records inserted successfully")


def upsert_to_dataverse_table(dataverse_api_url, target_table: str, query: str, access_token, unique_column_dataverse: str, unique_identifier_dataverse: str, batch_size: int):
    """
    Upsert in Dataverse table. If a record with the specified unique column exists, it updates it; otherwise, it inserts a new record.
    """

    data_json = _convert_from_catalog_to_json(query)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }

    # cache to store already checked unique values and their primary keys
    search_cache = {}

    upsert_count, error_count = 0, 0

    # process in batches
    for batch_start in range(0, len(data_json), batch_size):

        batch_records = data_json[batch_start:batch_start + batch_size]
        
        # step 1: check for existing records in parallel
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(_search_and_cache, dataverse_api_url, target_table, headers, unique_column_dataverse, record, search_cache): record for record in batch_records}
            for future in as_completed(futures):
                record = futures[future]
                try:
                    future.result()  # populate search_cache
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 401:
                        print("Access token is invalid or expired. Stopping process.")
                        return
                    print(f"Search failed for record {record}: {e}")
                    error_count += 1

        # step 2: perform upserts in parallel
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(_upsert_record, dataverse_api_url, target_table, headers, record, unique_column_dataverse, unique_identifier_dataverse, search_cache): record for record in batch_records}
            for future in as_completed(futures):
                record = futures[future]
                try:
                    result = future.result()
                    if result:
                        upsert_count += 1
                    else:
                        error_count += 1
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 401:
                        print("Access token is invalid or expired. Stopping process.")
                        return
                    print(f"Upsert failed for record {record}: {e}")
                    error_count += 1

    print(f"{upsert_count} records upserted successfully, {error_count} errors encountered.")


def _delete_record(query_url, record_id, headers):
    """
    Send a delete request for a single record with retry logic.
    """
    delete_url = f"{query_url}({record_id})"
    max_retries = 3
    for attempt in range(max_retries):
        response = requests.delete(delete_url, headers=headers)
        if response.status_code == 204:
            return True  # successfully deleted
        elif response.status_code in {429, 503}:
            time.sleep(2 ** attempt)  # exponential backoff for rate limits or service unavailable
        else:
            print(f"Failed to delete record {record_id}: {response.status_code} - {response.text}")
            return False
    return False


def _prepare_multipart_body(batch, target_table, boundary):
    """
    Prepare the multipart body for the batch insert.
    """
    parts = []
    for record in batch:
        part = (
            f"--{boundary}\r\n"
            f"Content-Type: application/http\r\n"
            f"Content-Transfer-Encoding: binary\r\n\r\n"
            f"POST /api/data/v9.1/{target_table} HTTP/1.1\r\n"
            f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
            f"{json.dumps(record)}\r\n"
        )
        parts.append(part)
    return ''.join(parts) + f"--{boundary}--\r\n"


def _search_and_cache(api_url, table, headers, unique_column, record, cache):
    unique_value = record.get(unique_column)
    if unique_value and unique_value not in cache:
        response = requests.get(f"{api_url}/{table}?$filter={unique_column} eq '{unique_value}'", headers=headers)
        if response.status_code == 200:
            results = response.json().get('value', [])
            cache[unique_value] = results[0] if results else {}  # cache results


def _upsert_record(api_url, table, headers, record, unique_column, primary_key_column, cache):
    unique_value = record.get(unique_column)
    if not unique_value:
        print("Skipping record with missing unique value.")
        return False

    existing_record = cache.get(unique_value)

    if existing_record:
        # update record if it exists
        primary_key_value = existing_record.get(primary_key_column)
        if primary_key_value:
            response = _update_record(api_url, table, headers, primary_key_value, record)
            return response and response.status_code in [200, 204]
        else:
            print(f"Primary key '{primary_key_column}' not found in search results.")
            return False
    else:
        # create new record if it does not exist
        response = _create_record(api_url, table, headers, record)
        return response and response.status_code in [201, 204]


def _update_record(api_url, table, headers, primary_key, record):
    try:
        # Ensure null values are handled explicitly
        record = {key: (None if value is None else value) for key, value in record.items()}

        response = requests.patch(
            f'{api_url}/{table}({primary_key})',
            headers=headers,
            data=json.dumps(record)
        )
        if response.status_code not in [200, 204]:
            print(f"Failed to update record with primary key {primary_key}: {response.status_code} - {response.text}")
        return response
    except requests.RequestException as e:
        print(f"Request exception during update: {e}")
        return None
    

def _create_record(api_url, table, headers, record):
    try:
        # Ensure null values are handled explicitly
        record = {key: (None if value is None else value) for key, value in record.items()}

        response = requests.post(
            f'{api_url}/{table}',
            headers=headers,
            data=json.dumps(record)
        )
        if response.status_code not in [201, 204]:
            print(f"Failed to create record: {response.status_code} - {response.text}")
        return response
    except requests.RequestException as e:
        print(f"Request exception during create: {e}")
        return None
    

def _convert_from_catalog_to_json(query: str):
    """
    Helper function to convert a Spark SQL query to a JSON format suitable for Dataverse.
    Ensures that null values are explicitly handled for date and numeric fields.
    """

    df_specific = spark.sql(query)

    # process columns to handle nulls
    for col_name, dtype in df_specific.dtypes:
        if dtype in ['date', 'timestamp']:
            # replace nulls in date fields with None (explicitly represented as null in JSON)
            df_specific = df_specific.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), F.lit(None)).otherwise(F.col(col_name))
            )
        elif dtype in ['int', 'bigint', 'long']:
            # replace nulls in whole number fields with an empty string to represent blank in Dataverse
            df_specific = df_specific.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), F.lit("")).otherwise(F.col(col_name))
            )
        elif dtype in ['double', 'float'] or "decimal" in dtype:
            # replace nulls in decimal fields with an 0
            df_specific = df_specific.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), F.lit(0)).otherwise(F.col(col_name))
            )

    # handle lookup fields for OData binding
    for col_name in df_specific.columns:
        # check if the column is a lookup field (contains '_lookup' in the name)
        if '_lookup' in col_name:
            # remove '_lookup' and append '@odata.bind' to create the JSON field name
            new_col_name = col_name.replace('_lookup', '') + '@odata.bind'

            df_specific = df_specific.withColumnRenamed(col_name, new_col_name)

    # convert the DataFrame to JSON format
    data_json = df_specific.toJSON().collect()

    # convert JSON strings to dictionaries, ensuring Python's None becomes null in JSON
    data_json = [
        {key: (None if value in [None, "null", ""] else value) for key, value in json.loads(record).items()}
        for record in data_json
    ]

    return data_json
