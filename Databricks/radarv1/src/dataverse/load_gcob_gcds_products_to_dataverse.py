# Databricks notebook source
import os
import requests
from functions_dataverse import get_access_token, upsert_to_dataverse_table

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

gcobproducts = f'''
  SELECT
    uniquegcobid AS rdr_uniquegcobid,
    gcid AS rdr_gcid,
    gcobProductName AS rdr_gcobproductname,
    gcobProductBookingLocation AS rdr_gcobproductbookinglocation,
    gcobProductLocation AS rdr_gcobproductlocation,
    gcobGlobalClientOwnerLocation AS rdr_gcobglobalclientownerlocation,
    gcobClientName AS rdr_gcobclientname,
    GlobalReportingRegion AS rdr_globalreportingregion,
    product_in_gcds AS rdr_productingcds,
    cddresponsiblelocation AS rdr_cddresponsiblelocation
  FROM radar.gcobProductsNotInGCDS
  WHERE product_in_gcds = 'does not exist'
'''

# COMMAND ----------

gcdsproducts = f'''
  SELECT
    gcid AS rdr_gcid,
    gcdsClientName AS rdr_gcdsclientname,
    gcdsGlobalClientOwnerLocation AS rdr_gcdsglobalclientownerlocation,
    uniquegcobid AS rdr_uniquegcobid,
    gcdsProductName AS rdr_gcdsproductname,
    gcdsProductlocation AS rdr_gcdsproductlocation,
    gcdsBookinglocation AS rdr_gcdsbookinglocation,
    product_in_gcob AS rdr_productingcob,
    cddresponsiblelocation AS rdr_cddresponsiblelocation
  FROM radar.gcdsProductsNotInGCOB
  WHERE product_in_gcob = 'does not exist'
'''

# COMMAND ----------

upsert_to_dataverse_table(dataverse_api_url,'rdr_productcomparisonappgcobproductsnotingcdses', gcobproducts, access_token, 'rdr_uniquegcobid', 'rdr_productcomparisonappgcobproductsnotingcdsid', 1000)

# COMMAND ----------

upsert_to_dataverse_table(dataverse_api_url,'rdr_productcomparisonappgcdsproductsnotingcobs', gcdsproducts, access_token, 'rdr_gcid', 'rdr_productcomparisonappgcdsproductsnotingcobid', 1000)
