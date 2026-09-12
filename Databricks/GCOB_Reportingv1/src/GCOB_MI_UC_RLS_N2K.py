# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To Achive RLS in unity catalog tables for user to direct access UC tables to query
# MAGIC
# MAGIC #### Author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC |Abhishek Jaiswal |7-Apr-2026 |15582341|Initial Release|
# MAGIC |Hari |13-July-2026 |16463606|Updated Preprd read function|
# MAGIC

# COMMAND ----------

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
import re

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
environment=os.environ['ENV']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
SARADAR = "saradar" + environment

# COMMAND ----------

# DBTITLE 1,Function to load data only to preprd
if environment.lower() != "preprd":
    dbutils.notebook.exit(
        f"Notebook skipped. Current environment is '{environment}'. Execution allowed only in Preprod."
    )

# COMMAND ----------

# DBTITLE 1,Authenticate to Data Storage
authenticate_storage_account(ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Function to Read and Write in UC

def clean_column_name(col_name: str) -> str:
    """
    Clean Delta Lake column names:
    - Lowercase
    - Replace invalid characters with underscore
    - Remove leading/trailing underscores
    """
    # Replace invalid characters with underscore
    cleaned = re.sub(r"[ ,;{}()\n\t=]", "_", col_name)
    # Replace consecutive underscores with one
    cleaned = re.sub(r"__+", "_", cleaned)
    # Strip leading/trailing underscores
    cleaned = cleaned.strip("_")
    return cleaned.lower()


def process_object(object_name: str,
                   catalog: str,
                   schema: str):

    print(f"\n Processing {object_name}")

    # Build lake path
    lake_path = (
        f"abfss://radarconsumerdata@{SARADAR}.dfs.core.windows.net/"
        f"EXTERNAL_TABLE/{object_name}/*.parquet"
    )

    print(f"Reading from: {lake_path}")

    try:
        df = spark.read.parquet(lake_path)
    except Exception as e:
        print(f"ERROR reading object {object_name}: {e}")
        return
    
    
    # Clean column names
    cleaned_cols = [clean_column_name(c) for c in df.columns]
    df = df.toDF(*cleaned_cols)


    # Write to UC
    full_table_name = f"{catalog}.{schema}.{object_name}"
    print(f"Writing to UC table: {full_table_name}")

    # Create schema if needed
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # drop table if needed
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{object_name}")

    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(full_table_name)

    print(f"Successfully written: {full_table_name}")

    # Return UC dataframe
    return spark.table(full_table_name)


# COMMAND ----------

# DBTITLE 1,Skipping RDM objects

base_path = f"abfss://radarconsumerdata@{SARADAR}.dfs.core.windows.net/EXTERNAL_TABLE/"

print("Discovering objects in SARADAR...")

objects_to_process = []

for item in dbutils.fs.ls(base_path):
    if item.isDir():
        folder_name = item.name.replace("/", "")  # remove trailing slash
        
        # Skip ANY folder starting with RDM_ (case insensitive)
        if folder_name.lower().startswith("rdm_"):
            print(f"Skipping RDM table: {folder_name}")
            continue

        objects_to_process.append(folder_name)

print(f"Discovered {len(objects_to_process)} objects:")
print(objects_to_process)


# COMMAND ----------

# DBTITLE 1,Call Function to Read and Write in UC catalog

catalog = "wr_fj_parties_and_risk_assessment_preprd"
schema = "gcobreporting"

uc_results = {}

for obj in objects_to_process:
    try:
        df_uc = process_object(obj, catalog, schema)
        uc_results[obj] = df_uc
    except Exception as e:
        print(f"Failed processing {obj}: {e}")

print("COMPLETED INGEST OF ALL SARADAR TABLES INTO UNITY CATALOG")


# COMMAND ----------

# DBTITLE 1,Redefine existing N2K logic in one table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GcobReport_N2KClientUserAttribute AS
# MAGIC SELECT DISTINCT 
# MAGIC rc.*
# MAGIC ,ua.AttributeType
# MAGIC ,ua.UPN
# MAGIC ,mc.SourceClient
# MAGIC FROM wr_fj_parties_and_risk_assessment_preprd.gcobreporting.N2K_ClientAttribute rc
# MAGIC Inner join wr_fj_parties_and_risk_assessment_preprd.gcobreporting.N2k_UserAttribute ua on ua.AttributeValue =rc.AttributeValue
# MAGIC Inner join wr_fj_parties_and_risk_assessment_preprd.gcobreporting.MI_Cases mc ON rc.UniqueGcobId = mc.UniqueGcobId;
# MAGIC
# MAGIC --order by ua.UPN --rc.GcobId

# COMMAND ----------

# DBTITLE 1,Write Refined N2K object to UC
# ---------------------------------------------------------
# Write Derived Temp View to Unity Catalog
# ---------------------------------------------------------

catalog = "wr_fj_parties_and_risk_assessment_preprd"
schema = "gcobreporting"
table_name = "GcobReport_N2KClientUserAttribute"

# 1. Convert temp view to DataFrame
df_gcob = spark.table(table_name)

# 2. Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# drop table if needed
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}")

# 3. Write to Unity Catalog
full_table = f"{catalog}.{schema}.{table_name}"

df_gcob.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(full_table)

print(f"Successfully written derived table to UC: {full_table}")

# 4. Optional: display UC table
#display(spark.table(full_table))

# COMMAND ----------

# DBTITLE 1,Function for UC RLS
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC wr_fj_parties_and_risk_assessment_preprd.gcobreporting.fn_can_see_sourceclient(src STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM wr_fj_parties_and_risk_assessment_preprd.gcobreporting.GcobReport_N2KClientUserAttribute ua
# MAGIC     WHERE ua.SourceClient = src
# MAGIC       AND LOWER(ua.UPN) = LOWER(CURRENT_USER())
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Applying RLS to UC Tabled
# Databricks notebook (Python) — executes SQL commands dynamically

catalog = "wr_fj_parties_and_risk_assessment_preprd"
schema = "gcobreporting"
rls_fn = f"{catalog}.{schema}.fn_can_see_sourceclient"


# Tables that must never have RLS applied
exclude_tables = {
    "gcobreport_n2kclientuserattribute",
    "n2k_clientattribute",
    "n2k_userattribute",
    "client_keystorekey"
}

# Fetch all tables in the schema
tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
display(tables)

for row in tables.collect():
    table_name = row.tableName.lower()
    full_table = f"{catalog}.{schema}.{table_name}"
    
    # Skip authorization tables
    if table_name in exclude_tables:
        print(f"Skipping auth table: {full_table}")
        continue

    # Get table columns
    cols = [c.name.lower() for c in spark.table(full_table).schema]

    # Only apply RLS if SourceClient exists
    if "sourceclient" in cols:
        print(f"Applying RLS to: {full_table}")
        spark.sql(f"""
            ALTER TABLE {full_table}
            SET ROW FILTER {rls_fn}
            ON (SourceClient)
        """)
    else:
        print(f"Skipping (no SourceClient column): {full_table}")

print("RLS successfully applied to all eligible tables!")

# COMMAND ----------

# DBTITLE 1,Granting AAD read access to UC tables
catalog = "wr_fj_parties_and_risk_assessment_preprd"
schema = "gcobreporting"
group = "eu.aut.AADWRRadarBusinessDataCitizen.us"

tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`")

for row in tables.collect():
    table_name = row.tableName
    full_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    print(f"Granting SELECT on {full_name}")
    spark.sql(f"GRANT SELECT ON TABLE {full_name} TO `{group}`")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --GRANT USAGE ON CATALOG wr_fj_parties_and_risk_assessment_preprd TO `eu.aut.AADWRRadarBusinessDataCitizen.us`;
# MAGIC --GRANT USAGE ON SCHEMA wr_fj_parties_and_risk_assessment_preprd.gcobreporting TO `eu.aut.AADWRRadarBusinessDataCitizen.us`;
# MAGIC --GRANT SELECT ON ALL TABLES IN SCHEMA wr_fj_parties_and_risk_assessment_preprd.gcobreporting TO `eu.aut.AADWRRadarBusinessDataCitizen.us`;
# MAGIC --GRANT SELECT ON FUTURE TABLES IN SCHEMA wr_fj_parties_and_risk_assessment_preprd.gcobreporting TO `eu.aut.AADWRRadarBusinessDataCitizen.us`;
# MAGIC --GRANT USE WAREHOUSE ON WAREHOUSE your_sql_warehouse_name TO `eu.aut.AADWRRadarBusinessDataCitizen.us`;
