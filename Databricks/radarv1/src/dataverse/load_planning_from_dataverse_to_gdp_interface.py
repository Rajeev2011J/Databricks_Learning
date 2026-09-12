# Databricks notebook source
import os
import requests
from functions_dataverse import get_access_token, get_dataverse_data
from Databricks.radarv1.src.functions_databricks import append_to_databricks_table, full_load_to_databricks_table, upsert_to_databricks_table

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

dataverse_url = os.environ['DATAVERSE_URL']
dataverse_api_url = f"{dataverse_url}/api/data/v9.1/"

# COMMAND ----------

access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)

# COMMAND ----------

# validate Dataverse API authorization and check output to get table name for next step
headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json; charset=utf-8",
}
response = requests.get(dataverse_api_url, headers=headers)

if response.status_code == 200:
    data = response.json()  # safely decode JSON only if the request was successful
    display(data)
else:
    print(f"Error: Received response with status code {response.status_code}")
    print(response.text)  # helps to debug the actual response content

# COMMAND ----------

df = get_dataverse_data(access_token, dataverse_api_url, 'cr7f2_pocdatas')


# COMMAND ----------

df.createOrReplaceTempView("df_view")

# COMMAND ----------

df_specific = spark.sql(f'''
        SELECT 
             cr7f2_pocdataid AS id
            ,cr7f2_city AS city
            ,cr7f2_country AS country           
        FROM df_view
    ''')    

display(df_specific)

# COMMAND ----------

# example to update existing records and only insert new records
upsert_to_databricks_table(df_specific, 'radar', 'pocdata', ['id'])

# COMMAND ----------

# example for inserting all records (again)
append_to_databricks_table(df_specific, 'radar', 'pocdata')

# COMMAND ----------

from pyspark.sql.functions import col, count

# identify columns with '@' in their names
cols_to_drop = [col_name for col_name in df.columns if '@' in col_name]

# drop columns with '@' in their names
df_cleaned = df.drop(*cols_to_drop)

# create a list of columns to keep (those that don't have all NULLs)
columns_to_keep = [column for column in df_cleaned.columns if df_cleaned.filter(col(column).isNotNull()).count() > 0]

# select only the columns that have at least one non-null value (otherwise error in catalog)
df_cleaned = df_cleaned.select(*columns_to_keep)

display(df_cleaned)


# COMMAND ----------

# example of creating table and loading all the data into it
full_load_to_databricks_table(df_cleaned, "radar", "pocdata_all")
