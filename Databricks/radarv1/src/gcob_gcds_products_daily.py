# Databricks notebook source
import os

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Load GCOB Data
from datetime import datetime, timedelta
import re

gcob_objects = [
    'party_case_client_details'
    , 'party_workitem'
    , 'party_local_client_Owners'
    , 'party_request_for_information'
    , 'party_client'
    , 'party_products_and_sevices'
    , 'party_trade_name'
    , 'party_control_measures'
]

for item in gcob_objects:
    # get the most recent version available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/'
    files = dbutils.fs.ls(path)
    version = max([
        int(re.search(r'/(\d+)/$', file.path).group(1))
        for file in files
        if re.search(r'/(\d+)/$', file.path)
    ])

    version = 102 # TEMPORARY UNTIL THE NEW VERSION IS UP AND RUNNING!!!!

    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOAD_DT=')[1][:8] for file in files if 'LOAD_DT=' in file.path)

    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/LOAD_DT={load_date}*/*.parquet').createOrReplaceTempView(item)

EDL_LoadDate = datetime.strptime(load_date, '%Y%m%d').strftime('%Y-%m-%d')
# throw error if gdp file is older than 1 day!
today = datetime.now().date()
load_date_obj = datetime.strptime(load_date, '%Y%m%d').date()
days_diff = (today - load_date_obj).days

if days_diff > 1:
    # raise ValueError(f"Latest file date ({EDL_LoadDate}) is more than 1 day old (difference: {days_diff} days)")
    print(f"Alert: Latest file for {item} is more than 1 day old (date: {load_date_obj}, difference: {days_diff} days)")

# COMMAND ----------

# DBTITLE 1,Load GCDS Data of Today
import re

from pyspark.sql import SparkSession
# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
    , 'client_Client'
    , 'client_PartyRole'
    , 'client_Products'
    , 'client_OnboardedLocations'
    , 'client_PartytoPartyRelationship'
]

for item in gcds_tables:
  # get the most recent version available in gdp
  path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/'
  files = dbutils.fs.ls(path)
  version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
  
  # get the most recent file available in gdp
  path_file = f'{path}{version}/data/'
  files = dbutils.fs.ls(path_file)
  load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

  spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data/LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcds_'+ item)

# COMMAND ----------

# MAGIC %md
# MAGIC Products in GCDS but not in GCOB

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists  radar.product_name_map

# COMMAND ----------

# DBTITLE 1,Manual Product Mapping
# MAGIC %sql
# MAGIC --The Below code is used to produce a manual mapping.
# MAGIC --The logic for the mapping is provided by our stakeholders 
# MAGIC CREATE TABLE radar.product_name_map (
# MAGIC     gcob_product STRING,
# MAGIC     gcds_product STRING
# MAGIC );
# MAGIC --in the code below first element is GCOB product and second element is GCDS Product
# MAGIC INSERT INTO radar.product_name_map VALUES
# MAGIC ('Corporate Current Account', 'Current Account'),
# MAGIC ('Revolving Credit facility', 'Credit facility'),
# MAGIC ('Current Account', 'Zakelijke rekening (rekening-courant)'),
# MAGIC ('FX Swap', 'FX Call / Put Option'),
# MAGIC ('Bankgaranties (Large Corporates)', 'Guarantee / Standby LC'),
# MAGIC ('Current Account', 'Foreign Currency Account'),
# MAGIC ('Current Account', 'In Country Account'),
# MAGIC ('Short Term Advance Facility', 'Loan Facility'),
# MAGIC ('Structured Inventory Products', 'Structured Inventory Products (SIP)'),
# MAGIC ('Export Credit Agency Cover', 'Loan Facility'),
# MAGIC ('Bankgaranties (Large Corporates)', 'EF - Guarantee Facility'),
# MAGIC ('PF - Revolving Credit Facility', 'Revolving Credit Facility'),
# MAGIC ('Revolving Credit Facility', 'Roll-Over Loan Facility'),
# MAGIC ('Real Estate Finance Loan Facility', 'Loan Facility'),
# MAGIC ('Term Loan', 'Loan Facility'),
# MAGIC ('Term Loan', 'Revolving Credit Facility'),
# MAGIC ('Interest Rate Swap', 'Interest Rate Swap with embedded floor'),
# MAGIC ('Interest rate Swap', 'Interest Rate Swap with embedded floor'),
# MAGIC ('Credit Facility', 'Loan Facility'),
# MAGIC ('Loan Facility', 'Credit Facility'),
# MAGIC ('Guarantee Facility', 'EF - Guarantee Facility'),
# MAGIC ('PF - Term Loan', 'Loan Facility'),
# MAGIC ('Leveraged Loan: 2nd Lien', 'Leveraged Loan: Senior'),
# MAGIC ('Loan Facility', 'Revolving Credit Facility'),
# MAGIC ('Securitised Asset Based Lending', 'Securitized Asset Based Lending'),
# MAGIC ('Bankgaranties (Large Corporates)', 'Letter of Credit'),
# MAGIC ('Capital Call Facility', 'Call Loan Facility'),
# MAGIC (concat('PF - Guarantees / LC', chr(39), 's'),'Documentary Credit (Letter of Credit / LC)')

# COMMAND ----------

# DBTITLE 1,GCDS N2k For Asia
# MAGIC %sql
# MAGIC --GCDS N2K based on the product location of client
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcdsN2KAsia AS
# MAGIC     SELECT DISTINCT
# MAGIC         t1.gcid,
# MAGIC         --t1.Full_legal_name AS gcdsClientName,
# MAGIC         --t1.`Global_CO-location` AS gcdsGlobalClientOwnerLocation,
# MAGIC         t3.KeyStore_Value AS gcob_unique_id,
# MAGIC         t2.Product_location AS N2KLocation
# MAGIC     FROM gcds_client_client AS t1
# MAGIC     LEFT JOIN gcds_client_Products     AS t2 ON t1.gcid = t2.gcid
# MAGIC     LEFT JOIN gcds_client_KeyStoreKey  AS t3 ON t1.gcid = t3.gcid
# MAGIC     LEFT JOIN gcds_client_PartyRole    AS t4 ON t1.gcid = t4.gcid
# MAGIC     WHERE t3.KeyStore_type = 'GCOBID'
# MAGIC       AND t4.Life_cycle_status = 'Client'
# MAGIC       AND t2.status = 'Active' and t3.status = 'Active'
# MAGIC       AND t2.Product_location IN ('Rabobank Hong Kong','Rabobank Singapore','Rabobank China')

# COMMAND ----------

# DBTITLE 1,Products in GCDS but not in GCOB with manual mapping
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW gcdsProductsNotInGCOB AS
# MAGIC WITH gcds_base AS (
# MAGIC     SELECT 
# MAGIC         t1.gcid,
# MAGIC         t1.Full_legal_name AS gcdsClientName,
# MAGIC         t1.`Global_CO-location` AS gcdsGlobalClientOwnerLocation,
# MAGIC         t2.Product_location,
# MAGIC         t2.TypeDescription      AS gcds_ProductName,
# MAGIC         t2.Booking_location,
# MAGIC         t3.KeyStore_Value       AS gcob_unique_id,
# MAGIC         t2.BusinessUnit
# MAGIC     FROM gcds_client_client AS t1
# MAGIC     LEFT JOIN gcds_client_Products    AS t2 ON t1.gcid = t2.gcid
# MAGIC     LEFT JOIN gcds_client_KeyStoreKey AS t3 ON t1.gcid = t3.gcid
# MAGIC     LEFT JOIN gcds_client_PartyRole   AS t4 ON t1.gcid = t4.gcid
# MAGIC     WHERE t3.KeyStore_type = 'GCOBID'
# MAGIC       AND t4.Life_cycle_status = 'Client'
# MAGIC       AND t2.status = 'Active'
# MAGIC ),
# MAGIC gcob_scope AS (
# MAGIC     SELECT 
# MAGIC         t1.UniqueGcobId,
# MAGIC         t1.SourceClient,
# MAGIC         t2.ProductName,
# MAGIC         t2.ProductOfferingLocation,
# MAGIC         t2.BookingEntityLocation,
# MAGIC         t1.ReviewLocation
# MAGIC     FROM radar.clients AS t1
# MAGIC     LEFT JOIN radar.productsandservices AS t2
# MAGIC       ON t1.SourceClient = t2.SourceClient
# MAGIC     WHERE t1.ClientLifeCycleName = 'Client'
# MAGIC       AND t2.ProductLifeCycleStatus = 'Active'
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     t2.gcid,
# MAGIC     t2.gcdsClientName,
# MAGIC     t2.gcdsGlobalClientOwnerLocation,
# MAGIC     t2.gcob_unique_id AS uniquegcobid,
# MAGIC     t2.gcds_ProductName AS gcdsProductName,
# MAGIC     t2.Product_location AS gcdsProductlocation,
# MAGIC     t2.Booking_location AS gcdsBookinglocation,
# MAGIC     t2.BusinessUnit,
# MAGIC     t1.ReviewLocation AS CDDResponsibleLocation,
# MAGIC     CASE 
# MAGIC         WHEN EXISTS (
# MAGIC             SELECT 1
# MAGIC             FROM gcob_scope s
# MAGIC             WHERE s.UniqueGcobId = t2.gcob_unique_id
# MAGIC               AND LOWER(TRIM(s.ProductName)) = LOWER(TRIM(t2.gcds_ProductName))
# MAGIC               AND LOWER(TRIM(s.ProductOfferingLocation)) = LOWER(TRIM(t2.Product_location))
# MAGIC               AND LOWER(TRIM(s.BookingEntityLocation)) = LOWER(TRIM(t2.Booking_location))
# MAGIC
# MAGIC         )
# MAGIC         OR EXISTS (
# MAGIC             SELECT 1
# MAGIC             FROM gcob_scope s
# MAGIC             JOIN radar.product_name_map m
# MAGIC               ON LOWER(TRIM(s.ProductName)) = LOWER(TRIM(m.gcob_product))
# MAGIC              AND LOWER(TRIM(t2.gcds_ProductName)) = LOWER(TRIM(m.gcds_product))
# MAGIC             WHERE s.UniqueGcobId = t2.gcob_unique_id
# MAGIC               AND LOWER(TRIM(s.ProductOfferingLocation)) = LOWER(TRIM(t2.Product_location))
# MAGIC               AND LOWER(TRIM(s.BookingEntityLocation)) = LOWER(TRIM(t2.Booking_location))
# MAGIC         )
# MAGIC         THEN 'exists'
# MAGIC         ELSE 'does not exist'
# MAGIC     END AS product_in_gcob
# MAGIC     ,case when
# MAGIC     t2.gcds_ProductName = t3.HighRiskProductName
# MAGIC     then 'High Risk Product'
# MAGIC     else 'Medium/Low Risk Product'
# MAGIC   END AS ProductRiskLevel
# MAGIC FROM gcds_base t2
# MAGIC INNER JOIN gcob_scope AS t1
# MAGIC   ON t1.UniqueGcobId = t2.gcob_unique_id
# MAGIC LEFT JOIN radar.sirahighriskproducts_historical AS t3 on t2.gcds_ProductName = t3.HighRiskProductName

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products in GCOB but not in GCDS

# COMMAND ----------

# DBTITLE 1,Products in GCOB but not in GCDS with manual mapping
# MAGIC
# MAGIC %sql
# MAGIC -- Per-gcid check: does the GCOB product exist in GCDS for the mapped GCOB client?
# MAGIC CREATE OR REPLACE TEMP VIEW gcobProductsNotInGCDS AS
# MAGIC WITH gcds_base AS (
# MAGIC     SELECT 
# MAGIC         t1.gcid,
# MAGIC         --t1.Full_legal_name AS gcdsClientName,
# MAGIC         --t1.`Global_CO-location` AS gcdsGlobalClientOwnerLocation,        
# MAGIC         t2.Product_location,
# MAGIC         t2.TypeDescription      AS gcds_ProductName,
# MAGIC         t2.Booking_location,
# MAGIC         t3.KeyStore_Value       AS gcob_unique_id,
# MAGIC         t2.BusinessUnit
# MAGIC     FROM gcds_client_client AS t1
# MAGIC     LEFT JOIN gcds_client_Products    AS t2 ON t1.gcid = t2.gcid
# MAGIC     LEFT JOIN gcds_client_KeyStoreKey AS t3 ON t1.gcid = t3.gcid
# MAGIC     LEFT JOIN gcds_client_PartyRole   AS t4 ON t1.gcid = t4.gcid
# MAGIC     WHERE t3.KeyStore_type = 'GCOBID'
# MAGIC       AND t4.Life_cycle_status = 'Client'
# MAGIC       AND t2.status = 'Active'
# MAGIC ),
# MAGIC gcob_scope AS (
# MAGIC     SELECT 
# MAGIC         t1.UniqueGcobId,
# MAGIC         t1.SourceClient,
# MAGIC         t2.ProductName,
# MAGIC         t1.FullLegalName,
# MAGIC         t2.BookingEntityLocation,
# MAGIC         t2.ProductOfferingLocation,
# MAGIC         t2.BusinessUnit,
# MAGIC         t1.GlobalClientOwnerLocation,
# MAGIC         t1.GlobalReportingRegion,
# MAGIC         t1.ReviewLocation
# MAGIC     FROM radar.clients AS t1
# MAGIC     LEFT JOIN radar.productsandservices AS t2
# MAGIC       ON t1.SourceClient = t2.SourceClient
# MAGIC     WHERE t1.ClientLifeCycleName = 'Client'
# MAGIC       AND t2.ProductLifeCycleStatus = 'Active'
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     t1.UniqueGcobId AS uniquegcobid,
# MAGIC     t2.gcid,
# MAGIC     t1.ProductName AS gcobProductName,
# MAGIC     t1.BookingEntityLocation AS gcobProductBookingLocation,
# MAGIC     t1.ProductOfferingLocation AS gcobProductLocation,
# MAGIC     t1.GlobalClientOwnerLocation AS gcobGlobalClientOwnerLocation,
# MAGIC     t1.FullLegalName AS gcobClientName,
# MAGIC     t1.BusinessUnit,
# MAGIC     t1.GlobalReportingRegion, 
# MAGIC     t1.ReviewLocation AS CDDResponsibleLocation,
# MAGIC     CASE 
# MAGIC         WHEN EXISTS (
# MAGIC             -- Direct normalized match
# MAGIC             SELECT 1
# MAGIC             FROM gcds_base gb
# MAGIC             WHERE gb.gcob_unique_id = t1.UniqueGcobId
# MAGIC               AND LOWER(TRIM(gb.gcds_ProductName)) = LOWER(TRIM(t1.ProductName))
# MAGIC               AND LOWER(TRIM(t1.ProductOfferingLocation)) = LOWER(TRIM(gb.Product_location))
# MAGIC               AND LOWER(TRIM(t1.BookingEntityLocation)) = LOWER(TRIM(gb.Booking_location))
# MAGIC         )
# MAGIC         OR EXISTS (
# MAGIC             -- Synonym match via permanent mapping table
# MAGIC             SELECT 1
# MAGIC             FROM gcds_base gb
# MAGIC             JOIN radar.product_name_map m
# MAGIC               ON LOWER(TRIM(t1.ProductName)) = LOWER(TRIM(m.gcob_product))
# MAGIC              AND LOWER(TRIM(gb.gcds_ProductName)) = LOWER(TRIM(m.gcds_product))
# MAGIC             WHERE gb.gcob_unique_id = t1.UniqueGcobId
# MAGIC               AND LOWER(TRIM(t1.ProductOfferingLocation)) = LOWER(TRIM(gb.Product_location))
# MAGIC               AND LOWER(TRIM(t1.BookingEntityLocation)) = LOWER(TRIM(gb.Booking_location))
# MAGIC         )
# MAGIC         THEN 'exists'
# MAGIC         ELSE 'does not exist'
# MAGIC     END AS product_in_gcds
# MAGIC     , case when 
# MAGIC     t1.productname = t3.HighRiskProductName
# MAGIC     then 'High Risk Product'
# MAGIC     else 'Medium/Low Risk Product'
# MAGIC     END AS ProductRiskLevel
# MAGIC FROM gcob_scope t1
# MAGIC INNER JOIN gcds_base t2
# MAGIC   ON t1.UniqueGcobId = t2.gcob_unique_id
# MAGIC   LEFT JOIN radar.sirahighriskproducts_historical AS t3 on t1.productname = t3.HighRiskProductName

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.gcdsProductsNotInGCOB;
# MAGIC drop table if exists radar.gcobProductsNotInGCDS;

# COMMAND ----------

spark.sql('select * from gcdsProductsNotInGCOB').write.mode('overwrite').saveAsTable('radar.gcdsProductsNotInGCOB')
spark.sql('select * from gcobProductsNotInGCDS').write.mode('overwrite').saveAsTable('radar.gcobProductsNotInGCDS')

# COMMAND ----------

# DBTITLE 1,Products with perfect match in GCOB&GCDS
# %sql
# --NOT USED ANYWHERE, WAS USED FOR ANALYSIS BEFORE THE PROJECT BEGAN
# CREATE OR REPLACE TEMP VIEW gcobGcdsPerfectMatchProducts AS
# WITH gcds_base AS (
#     SELECT DISTINCT
#         t1.gcid,
#        -- t2.Product_location,
#         t2.TypeDescription      AS gcds_ProductName,   -- swap if GCDS has ProductName
#         --t2.Booking_location,
#         t3.KeyStore_Value        AS gcob_unique_id      -- <- your GCOBID from GCDS
#         --t2.BusinessUnit
#     FROM gcds_client_client AS t1
#     LEFT JOIN gcds_client_Products    AS t2 ON t1.gcid = t2.gcid
#     LEFT JOIN gcds_client_KeyStoreKey AS t3 ON t1.gcid = t3.gcid
#     LEFT JOIN gcds_client_PartyRole AS t4 ON t1.gcid = t4.gcid
#     WHERE t3.KeyStore_type = 'GCOBID' AND t4.Life_cycle_status = 'Client'  AND t2.status = 'Active'
# ),
# gcob_scope AS (
#     SELECT DISTINCT
#         t1.UniqueGcobId,
#         t1.SourceClient,
#         t2.ProductName
#     FROM radar.clients as t1
#     left join radar.productsandservices as t2 on t1.SourceClient = t2.SourceClient
#     where t1.ClientLifeCycleName = 'Client' and t2.ProductLifeCycleStatus = 'Active'
# )

# SELECT 
#   t1.gcds_productname,
#   t2.productname
#   from gcds_base as t1
#   INNER JOIN gcob_scope as t2 on trim(t1.gcds_ProductName) = trim(t2.ProductName)

# COMMAND ----------

# DBTITLE 1,GCDS PRODUCTS THAT ARE NOT IN THE PERFECT MATCH PART PER REGION
# %sql
# --NOT USED ANYWHERE, WAS USED FOR ANALYSIS BEFORE THE PROJECT BEGAN
# CREATE OR REPLACE TEMP VIEW gcdsProductNotAMatch AS
# select distinct 
# t2.TypeDescription,
# t5.globalreportingregion,
# count(t5.globalreportingregion) as clientsWithThisProduct
# FROM gcds_client_client AS t1
# LEFT JOIN gcds_client_Products    AS t2 ON t1.gcid = t2.gcid
# LEFT JOIN gcds_client_KeyStoreKey AS t3 ON t1.gcid = t3.gcid
# LEFT JOIN gcds_client_PartyRole AS t4 ON t1.gcid = t4.gcid
# LEFT JOIN radar.clients AS t5 on t3.KeyStore_value = t5.uniquegcobid
# WHERE t3.KeyStore_type = 'GCOBID' AND t4.Life_cycle_status = 'Client'  AND t2.status = 'Active' and
#  t2.TypeDescription NOT IN (
#       SELECT gcds_productname
#       FROM gcobGcdsPerfectMatchProducts
# )
# group by t2.TypeDescription, t5.globalreportingregion

# COMMAND ----------

# DBTITLE 1,GCOB PRODUCTS THAT ARE NOT IN THE PERFECT MATCH PART
# %sql
# --NOT USED ANYWHERE, WAS USED FOR ANALYSIS BEFORE THE PROJECT BEGAN
# CREATE OR REPLACE TEMP VIEW gcobProductNotAMatch AS
#     SELECT DISTINCT
#         t2.ProductName
#     FROM radar.clients as t1
#     left join radar.productsandservices as t2 on t1.SourceClient = t2.SourceClient
#     where t1.ClientLifeCycleName = 'Client' and t2.ProductLifeCycleStatus = 'Active' AND
#       t2.ProductName NOT IN (
#       SELECT productname
#       FROM gcobGcdsPerfectMatchProducts
# )


# COMMAND ----------

# DBTITLE 1,GCOB PRODUCTS THAT ARE NOT IN THE PERFECT MATCH PART PER REGION
# %sql
# --NOT USED ANYWHERE, WAS USED FOR ANALYSIS BEFORE THE PROJECT BEGAN
# CREATE OR REPLACE TEMP VIEW gcobProductNotAMatchPerRegion AS
#     SELECT DISTINCT
#         t2.ProductName,
#         t1.globalreportingregion,
#         count(t1.globalreportingregion) as clientsWithThisProduct
#     FROM radar.clients as t1
#     left join radar.productsandservices as t2 on t1.SourceClient = t2.SourceClient
#     where t1.ClientLifeCycleName = 'Client' and t2.ProductLifeCycleStatus = 'Active' AND
#       t2.ProductName NOT IN (
#       SELECT productname
#       FROM gcobGcdsPerfectMatchProducts
# )
#    group by t2.ProductName, t1.globalreportingregion


# COMMAND ----------

# DBTITLE 1,All GCOB Products
# MAGIC %sql
# MAGIC --NOT USED IN THE DASHBOARD.
# MAGIC CREATE OR REPLACE TEMP VIEW allGcobProducts AS
# MAGIC     SELECT DISTINCT
# MAGIC         t2.ProductName
# MAGIC     FROM radar.clients as t1
# MAGIC     left join radar.productsandservices as t2 on t1.SourceClient = t2.SourceClient
# MAGIC     where t1.ClientLifeCycleName = 'Client' and t2.ProductLifeCycleStatus = 'Active'

# COMMAND ----------

# DBTITLE 1,All GCDS Products
# MAGIC %sql
# MAGIC --USED IN THE DASHBOARD TO SHOW THE NUMBER OF ACTIVE PRODUCTS AND GCDS CLIENTS ASSOCIATED TO IT
# MAGIC CREATE OR REPLACE TEMP VIEW allGcdsProducts AS
# MAGIC     SELECT DISTINCT
# MAGIC         t1.gcid,
# MAGIC         t2.Product_location,
# MAGIC         t2.TypeDescription      AS gcds_ProductName,   -- swap if GCDS has ProductName
# MAGIC         t2.Booking_location,
# MAGIC         t3.KeyStore_Value        AS gcob_unique_id,      -- <- your GCOBID from GCDS
# MAGIC         t2.BusinessUnit,
# MAGIC         t1.`Global_CO-location`,
# MAGIC         t1.`Global_CO-name`,
# MAGIC         t1.Full_legal_name AS gcdsClientName
# MAGIC     FROM gcds_client_client AS t1
# MAGIC     LEFT JOIN gcds_client_Products    AS t2 ON t1.gcid = t2.gcid
# MAGIC     LEFT JOIN gcds_client_KeyStoreKey AS t3 ON t1.gcid = t3.gcid
# MAGIC     LEFT JOIN gcds_client_PartyRole AS t4 ON t1.gcid = t4.gcid
# MAGIC     WHERE t3.KeyStore_type = 'GCOBID' AND t4.Life_cycle_status = 'Client'  AND t2.status = 'Active'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.allGcdsProducts;

# COMMAND ----------

spark.sql('select * from allGcdsProducts').write.mode('overwrite').saveAsTable('radar.allGcdsProducts')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.gcdsN2KAsia;

# COMMAND ----------

spark.sql('select * from gcdsN2KAsia').write.mode('overwrite').saveAsTable('radar.gcdsN2KAsia')
