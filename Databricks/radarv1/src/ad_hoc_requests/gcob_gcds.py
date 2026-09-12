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

# DBTITLE 1,Load GCDS Tables
from pyspark.sql import SparkSession
# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
    , 'client_Client'
    , 'client_PartyRole'
    , 'client_Products'
    , 'client_OnboardedLocations'
]

for item in gcds_tables:
    # get the most recent file available in gdp
    path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4602/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4602/data/LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcds_'+ item)

# COMMAND ----------

# DBTITLE 1,Load GCOB Tables
# load and create temp views of all gdp_tables below
gcob_tables = [
      'CaseService_case_LegalEntityClient'
    , 'CaseService_case_Case'
    , 'CaseService_case_LegalEntityClientIdentifier'
    , 'CaseService_NaturalPerson_NaturalPersonClientIdentifier'
    , 'CaseService_case_SystemTypeReference'
    , 'CaseService_NaturalPerson_SystemTypeReference'
]

for item in gcob_tables:
    # get the most recent file available in gdp
    path = f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/{item}/100/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOADED_DTS=')[1][:8] for file in files if 'LOADED_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/{item}/100/data/LOADED_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcob_'+item)

# COMMAND ----------

# DBTITLE 1,GCOB Products Per Booking Location
# MAGIC %sql
# MAGIC SELECT
# MAGIC     t2.ProductName as GCOBProductName,
# MAGIC     concat_ws(', ', collect_set(t2.BookingEntityLocation)) AS BookingEntityLocations
# MAGIC FROM radar.clients AS t1
# MAGIC LEFT JOIN radar.productsandservices AS t2 
# MAGIC     ON t1.SourceClient = t2.SourceClient
# MAGIC WHERE 
# MAGIC     t1.ClientLifeCycleName = 'Client' 
# MAGIC     and t2.ProductLifeCycleStatus = 'Active'
# MAGIC     AND t2.BookingEntityLocation IN ('Rabobank Netherlands','Rabobank London','Rabobank Paris','Rabobank Frankfurt','Rabobank Kenya','Rabobank Dublin','Rabobank Madrid','Rabobank Antwerp','Rabobank Milan')
# MAGIC     --AND t1.globalreportingregion = 'E&A'
# MAGIC GROUP BY 
# MAGIC     t2.ProductName;

# COMMAND ----------

# DBTITLE 1,GCOB Products Per Product Offering Location
# MAGIC %sql
# MAGIC SELECT
# MAGIC     t2.ProductName as GCOBProductName,
# MAGIC     concat_ws(', ', collect_set(t2.ProductOfferingLocation)) AS ProductOfferingLocations
# MAGIC FROM radar.clients AS t1
# MAGIC LEFT JOIN radar.productsandservices AS t2 
# MAGIC     ON t1.SourceClient = t2.SourceClient
# MAGIC WHERE 
# MAGIC     t1.ClientLifeCycleName = 'Client'
# MAGIC     and t2.ProductLifeCycleStatus = 'Active'
# MAGIC     AND t2.ProductOfferingLocation IN ('Rabobank Netherlands','Rabobank London','Rabobank Paris','Rabobank Frankfurt','Rabobank Kenya','Rabobank Dublin','Rabobank Madrid','Rabobank Antwerp','Rabobank Milan')
# MAGIC     --AND t1.globalreportingregion = 'E&A'
# MAGIC GROUP BY 
# MAGIC     t2.ProductName

# COMMAND ----------

# DBTITLE 1,Rob's Request 12-03-2026
# MAGIC %sql
# MAGIC     SELECT DISTINCT
# MAGIC         t1.gcid,
# MAGIC         t1.Full_legal_name AS gcdsClientName,
# MAGIC         --t5.FullLegalName,
# MAGIC         t3.KeyStore_Value       AS GCOBID,
# MAGIC         --t1.`Global_CO-location` AS gcdsGlobalClientOwnerLocation,
# MAGIC         --t2.Product_location,
# MAGIC         --t2.TypeDescription      AS gcds_ProductName,
# MAGIC         --t2.Booking_location,
# MAGIC         --t2.BusinessUnit,
# MAGIC         t4.Life_cycle_status AS gcdsClientLifecycleName,
# MAGIC         t5.clientlifecyclename AS gcobClientLifecycleName,
# MAGIC         t5.NextReviewDate ,
# MAGIC         --t5.FullLegalName,
# MAGIC         t5.GlobalReportingRegion AS GcobGlobalReportingRegion,
# MAGIC         t5.GlobalClientOwnerLocation AS GcobGlobalClientOwnerLocation,
# MAGIC         t5.Overdue AS PROverdue
# MAGIC         --t5.SectorTeam,
# MAGIC         --t5.GlobalKYCPortfolioNew
# MAGIC     FROM gcds_client_client AS t1
# MAGIC     LEFT JOIN gcds_client_Products    AS t2 ON t1.gcid = t2.gcid
# MAGIC     LEFT JOIN gcds_client_KeyStoreKey AS t3 ON t1.gcid = t3.gcid
# MAGIC     LEFT JOIN gcds_client_PartyRole   AS t4 ON t1.gcid = t4.gcid
# MAGIC     left join radar.clients as t5 on t3.KeyStore_Value = t5.uniquegcobid
# MAGIC     WHERE t3.KeyStore_type = 'GCOBID'
# MAGIC     and t1.gcid IN (1915, 3448, 7141, 8314, 9314, 15384, 16088, 20342, 20490, 20850, 21381, 21983, 28659, 28851, 29632, 31306, 32892, 34603, 35974, 36423, 40836, 42183, 42296, 48192, 77954, 78773, 113347, 137197, 142882, 163194, 170555, 193953, 454793, 672930, 697735, 748103, 757594, 852877, 922725, 1262377, 1265647, 1303505, 1305748, 1307097, 1307116, 1307587, 2026934, 2028335, 2035654, 2035658, 2039012, 2079751, 2093372, 2180691, 2181111, 2263931, 2281298, 2373311, 2394432, 2420933, 2484879)
# MAGIC     and t4.Party_role = 'Customer'
# MAGIC       --AND t4.Life_cycle_status = 'Client'
# MAGIC       --AND t2.status = 'Active'

# COMMAND ----------

# DBTITLE 1,Primary Nacics Code which are in GCDS but not in GCOB
# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC     SELECT DISTINCT
# MAGIC         t2.KeyStore_value AS gcobid,
# MAGIC         t1.gcid,
# MAGIC         t1.Primary_NAICS AS primaryNaicsGCDS,
# MAGIC         t4.Code AS gcobNaicsCode,
# MAGIC         t3.FullLegalName as gcobClientName,
# MAGIC         t3.GlobalKYCPortfolioNew
# MAGIC     FROM gcds_client_client as t1
# MAGIC     INNER JOIN gcds_client_KeyStoreKey t2 
# MAGIC         ON t1.gcid = t2.gcid
# MAGIC     INNER JOIN radar.clients t3 
# MAGIC         ON t2.KeyStore_value = t3.uniquegcobid
# MAGIC     LEFT JOIN radar.SiraNAICS t4 
# MAGIC         ON t3.UniqueGcobId = t4.UniqueGcobId
# MAGIC     LEFT JOIN gcds_client_PartyRole t5 ON t1.gcid = t5.gcid
# MAGIC     WHERE
# MAGIC         t3.GlobalReportingRegion = 'E&A'
# MAGIC         AND t3.ClientLifeCycleName = 'Client'
# MAGIC         AND t3.GlobalKYCPortfolioNew NOT IN ('London Markets')
# MAGIC         AND t3.FIHubIndicator_Derived = 'Corp'
# MAGIC         AND t2.keystore_type IN ('GCOBID')
# MAGIC         AND t2.status = 'Active'
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM base b
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM base b2
# MAGIC     WHERE b2.gcobid = b.gcobid
# MAGIC       AND b2.gcobNaicsCode = b.primaryNaicsGCDS
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.clients where UniqueGcobId = '1006'

# COMMAND ----------

# MAGIC %sql
# MAGIC     SELECT DISTINCT
# MAGIC         t2.KeyStore_value AS gcobid,
# MAGIC         t1.gcid,
# MAGIC         t1.Primary_NAICS AS primaryNaicsGCDS,
# MAGIC         t4.Code AS gcobNaicsCode,
# MAGIC         t3.FullLegalName AS gcobClientName,
# MAGIC         t3.GlobalKYCPortfolioNew,
# MAGIC         COUNT(t4.Code) OVER (PARTITION BY t2.KeyStore_value) AS naicsCountInGCOB
# MAGIC     FROM gcds_client_client t1
# MAGIC     INNER JOIN gcds_client_KeyStoreKey t2 
# MAGIC         ON t1.gcid = t2.gcid
# MAGIC     INNER JOIN radar.clients t3 
# MAGIC         ON t2.KeyStore_value = t3.uniquegcobid
# MAGIC     LEFT JOIN radar.SiraNAICS t4 
# MAGIC         ON t3.UniqueGcobId = t4.UniqueGcobId
# MAGIC     LEFT JOIN gcds_client_PartyRole t5 
# MAGIC         ON t1.gcid = t5.gcid
# MAGIC     WHERE t3.GlobalReportingRegion = 'E&A'
# MAGIC       AND t3.ClientLifeCycleName = 'Client'
# MAGIC       AND t3.GlobalKYCPortfolioNew NOT IN ('London Markets')
# MAGIC       AND t3.FIHubIndicator_Derived = 'Corp'
# MAGIC       AND t2.keystore_type = 'GCOBID'
# MAGIC       AND t2.status = 'Active'
# MAGIC       --AND t1.gcid = '7161'

# COMMAND ----------

# DBTITLE 1,Primary Nacics Code which are in GCDS and also in GCOB
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcdsClientinGCOBInitial AS 
# MAGIC WITH base AS (
# MAGIC     SELECT DISTINCT
# MAGIC         t2.KeyStore_value AS gcobid,
# MAGIC         t1.gcid,
# MAGIC         t1.Primary_NAICS AS primaryNaicsGCDS,
# MAGIC         t4.Code AS gcobNaicsCode,
# MAGIC         t3.FullLegalName AS gcobClientName,
# MAGIC         t3.GlobalKYCPortfolioNew
# MAGIC         --COUNT(distinct t4.Code) OVER (PARTITION BY t2.KeyStore_value) AS naicsCountInGCOB
# MAGIC     FROM gcds_client_client t1
# MAGIC     INNER JOIN gcds_client_KeyStoreKey t2 
# MAGIC         ON t1.gcid = t2.gcid
# MAGIC     INNER JOIN radar.clients t3 
# MAGIC         ON t2.KeyStore_value = t3.uniquegcobid
# MAGIC     LEFT JOIN radar.SiraNAICS t4 
# MAGIC         ON t3.UniqueGcobId = t4.UniqueGcobId
# MAGIC     LEFT JOIN gcds_client_PartyRole t5 
# MAGIC         ON t1.gcid = t5.gcid
# MAGIC     WHERE --t3.GlobalReportingRegion = 'E&A'
# MAGIC        t3.ClientLifeCycleName = 'Client'
# MAGIC      -- AND t3.GlobalKYCPortfolioNew NOT IN ('London Markets')
# MAGIC       AND t3.FIHubIndicator_Derived = 'Corp'
# MAGIC       AND t2.keystore_type = 'GCOBID'
# MAGIC       AND t2.status = 'Active'
# MAGIC       --AND t3.uniquegcobid = '2323'
# MAGIC )
# MAGIC
# MAGIC SELECT *,
# MAGIC COUNT( gcobNaicsCode) OVER (PARTITION BY gcobid) AS naicsCountInGCOB
# MAGIC FROM base b
# MAGIC WHERE  EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM base b2
# MAGIC     WHERE b2.gcobid = b.gcobid
# MAGIC       AND b2.gcobNaicsCode = b.primaryNaicsGCDS
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gcdsClientinGCOBInitial

# COMMAND ----------

# MAGIC %sql
# MAGIC select COUNT( gcobNaicsCode) OVER (PARTITION BY gcobid) AS naicsCountInGCOB,
# MAGIC *
# MAGIC  from gcdsClientinGCOBInitial
# MAGIC  where gcobid = '2323'

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH naics_dedup AS (
# MAGIC     -- Ensure one row per UniqueGcobId + Code
# MAGIC     SELECT DISTINCT
# MAGIC         UniqueGcobId,
# MAGIC         Code
# MAGIC     FROM radar.SiraNAICS
# MAGIC ),
# MAGIC naics_counts AS (
# MAGIC     SELECT
# MAGIC         UniqueGcobId,
# MAGIC         COUNT(*) AS naicsCountInGCOB
# MAGIC     FROM naics_dedup
# MAGIC     GROUP BY UniqueGcobId
# MAGIC ),
# MAGIC base AS (
# MAGIC     SELECT
# MAGIC         t2.KeyStore_value AS gcobid,
# MAGIC         t1.gcid,
# MAGIC         t1.Primary_NAICS AS primaryNaicsGCDS,
# MAGIC         nd.Code AS gcobNaicsCode,
# MAGIC         t3.FullLegalName AS gcobClientName,
# MAGIC         t3.GlobalKYCPortfolioNew,
# MAGIC         nc.naicsCountInGCOB
# MAGIC     FROM gcds_client_client t1
# MAGIC     INNER JOIN gcds_client_KeyStoreKey t2 
# MAGIC         ON t1.gcid = t2.gcid
# MAGIC     INNER JOIN radar.clients t3 
# MAGIC         ON t2.KeyStore_value = t3.uniquegcobid
# MAGIC     LEFT JOIN naics_dedup nd
# MAGIC         ON t3.UniqueGcobId = nd.UniqueGcobId
# MAGIC     LEFT JOIN naics_counts nc
# MAGIC         ON t3.UniqueGcobId = nc.UniqueGcobId
# MAGIC     WHERE t3.GlobalReportingRegion = 'E&A'
# MAGIC       AND t3.ClientLifeCycleName = 'Client'
# MAGIC       -- AND t3.GlobalKYCPortfolioNew NOT IN ('London Markets')
# MAGIC       AND t3.FIHubIndicator_Derived = 'Corp'
# MAGIC       AND t2.keystore_type = 'GCOBID'
# MAGIC       AND t2.status = 'Active'
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM base b
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM base b2
# MAGIC     WHERE b2.gcobid = b.gcobid
# MAGIC       AND b2.gcobNaicsCode = b.primaryNaicsGCDS
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- contains all GCDS data with a condition that there is a GCOBID in keystore 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GcdsClientData AS 
# MAGIC SELECT DISTINCT
# MAGIC     t1.gcid,
# MAGIC     t1.keyStore_type,
# MAGIC     t1.keyStore_value AS GCOBID,
# MAGIC     t2.Full_legal_name AS `GCDSClientName`,
# MAGIC     t2.ResidentialAddress_Country AS `GCDSResidentialAddressCountry`,
# MAGIC     t2.Principal_address_country AS `GCDSPrincipalAddressCountry`,
# MAGIC     t2.Party_type AS `GCDSPartyType`,
# MAGIC     t2.`CDD-entitytype` AS `GCDS_CDDentitytype`,
# MAGIC     t2.`CDD-next_reviewdate` AS `GCDSCDDNextReviewdate`,
# MAGIC     t2.`CDD-risk_rating` AS `GCDSCDDRiskRating`,
# MAGIC     t2.`Global_CO-name` AS `GCDSGlobalCOname`,
# MAGIC     t2.`Global_CO-location` AS `GCDSGlobalCOlocation`,
# MAGIC     t2.`Global_CO-email` AS `GlobalCOemail`,
# MAGIC     t2.`CO-businessline_description` AS `GCDSCObusinesslinedescription`,
# MAGIC     t2.`Global_CO-Serviced_by` AS `GCDSGlobalCOServicedBy`,
# MAGIC     t2.`RM-name` AS `GCDSRMName`,
# MAGIC     t2.`RM-location` AS `GCDSRMLocation`,
# MAGIC     t2.`RM-email` AS `GCDSRMEmail`,
# MAGIC     t2.`RM-businessline_description` AS `GCDSRMBusinesslineDescription`,
# MAGIC     t2.`RM-_Serviced_by` AS `GCDSRMServicedBy`,
# MAGIC     t3.Life_cycle_status AS `GCDSClientlifecyclestatus`,
# MAGIC     t2.`CO-email` AS `GCDSGCOemail`,
# MAGIC     t2.Registered_address_country AS `GCDSRegisteredCountry`,
# MAGIC     t2.`CDD-risk_rating` AS `GCDSCDDRating`,
# MAGIC     t3.EDL_LOAD_DTS AS `GCDSLastRefresh`
# MAGIC FROM 
# MAGIC     GCDS_client_KeyStoreKey AS t1
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_Client AS t2 ON t1.gcid = t2.gcid
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_PartyRole AS t3 ON t1.gcid = t3.gcid
# MAGIC WHERE 
# MAGIC     t3.party_role = 'Customer' AND t1.KeyStore_type = 'GCOBID'

# COMMAND ----------

# DBTITLE 1,GCDS TCF Portfolio
# MAGIC %sql
# MAGIC --Tiffany Request
# MAGIC select distinct
# MAGIC   t1.GCID,
# MAGIC   --t2.KeyStore_type,
# MAGIC   t2.KeyStore_value as WWID,
# MAGIC   t1.Full_legal_name AS gcdsClientName,
# MAGIC   --t3.party_role,
# MAGIC   --t3.Life_cycle_status AS partyRoleStatus,
# MAGIC   `Global_CO-businessline_description`AS businessLineDescription,
# MAGIC   `Global_CO-name` AS globalClientOwnerName,
# MAGIC   `Global_CO-location` AS globalClientOwnerLocation
# MAGIC  from gcds_client_client as t1
# MAGIC  LEFT JOIN gcds_client_KeyStoreKey AS t2 ON t1.gcid = t2.gcid
# MAGIC  LEFT JOIN gcds_client_PartyRole t3 ON t1.gcid = t3.gcid
# MAGIC  --LEFT JOIN hierarchy AS t4 ON t1.gcid = t4.gcid
# MAGIC where `Global_CO-businessline_description` IN ('ET Traders', 'F&A Traders', 'Other Traders', 'TCF Agri', 'TCF Commodities', 'TCF Energy and Metals', 'TCF Trade Finance', 'TCF Trade Finance Corporate Sales', 'TCF Energy & Metals')
# MAGIC AND `Global_CO-location` IN ('Utrecht', 'Singapore', 'Hong Kong') and t2.KeyStore_type = 'WWID' and t2.status = 'Active'

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC --contains raw GCDS data, view used mainly for analysis
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GcdsRawData AS
# MAGIC SELECT DISTINCT
# MAGIC     t1.gcid,
# MAGIC     t1.keyStore_type,
# MAGIC     t1.KeyStore_value,
# MAGIC     t1.status AS `keyStore_value_status`,
# MAGIC     t2.Full_legal_name AS `GCDSClientName`,
# MAGIC     t2.Person_Name AS `GCDSNaturalPersonName`,
# MAGIC     t2.ResidentialAddress_Country AS `GCDSResidentialAddressCountry`,
# MAGIC     t2.Principal_address_country AS `GCDSPrincipalAddressCountry`,
# MAGIC     t2.Party_type AS `GCDSPartyType`,
# MAGIC     t2.`CDD-entitytype` AS `GCDSCDD-entitytype`,
# MAGIC     t2.`CDD-next_reviewdate` AS `GCDSCDDNextReviewdate`,
# MAGIC     t2.`CDD-risk_rating` AS `GCDSCDDRiskRating`,
# MAGIC     t2.`Global_CO-name` AS `GCDSGlobalCOname`,
# MAGIC     t2.`Global_CO-location` AS `GCDSGlobalCOlocation`,
# MAGIC     t2.`Global_CO-email` AS `GlobalCOemail`,
# MAGIC     t2.`CO-businessline_description` AS `GCDSCObusinesslinedescription`,
# MAGIC     t2.`Global_CO-Serviced_by` AS `GCDSGlobalCOServicedBy`,
# MAGIC     t2.`RM-name` AS `GCDSRMName`,
# MAGIC     t2.`RM-location` AS `GCDSRMLocation`,
# MAGIC     t2.`RM-email` AS `GCDSRMEmail`,
# MAGIC     t2.`RM-businessline_description` AS `GCDSRMBusinesslineDescription`,
# MAGIC     t2.`RM-_Serviced_by` AS `GCDSRMServicedBy`,
# MAGIC     t3.Life_cycle_status AS `GCDSClientlifecyclestatus`,
# MAGIC     t2.`CO-email` AS `GCDSGCOemail`,
# MAGIC     t2.Registered_address_country AS `GCDSRegisteredCountry`,
# MAGIC     t2.`CDD-risk_rating` AS `GCDSCDDRating`,
# MAGIC     t1.EDL_LOAD_DTS AS `GCDSLastRefresh`,
# MAGIC     t3.party_role,
# MAGIC     t2.Customer_ambition AS `ClientStrategy`
# MAGIC FROM 
# MAGIC     GCDS_client_KeyStoreKey AS t1
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_Client AS t2 ON t1.gcid = t2.gcid
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_PartyRole AS t3 ON t1.gcid = t3.gcid
# MAGIC LEFT JOIN
# MAGIC     GCDS_client_Products AS t4 ON t1.gcid = t4.gcid
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_OnboardedLocations AS t5 ON t1.gcid = t5.gcid

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GcdsClientDataWithAllPartyRole AS
# MAGIC SELECT DISTINCT
# MAGIC     t1.gcid,
# MAGIC     t1.keyStore_type,
# MAGIC     t1.keyStore_value AS GCOBID,
# MAGIC     t1.status AS keyStore_value_status,
# MAGIC     t2.Full_legal_name AS GCDSClientName,
# MAGIC     t2.ResidentialAddress_Country AS GCDSResidentialAddressCountry,
# MAGIC     t2.Principal_address_country AS GCDSPrincipalAddressCountry,
# MAGIC     t2.Party_type AS GCDSPartyType,
# MAGIC     t2.`CDD-entitytype` AS GCDS_CDDentitytype,
# MAGIC     t2.`CDD-next_reviewdate` AS GCDSCDDNextReviewdate,
# MAGIC     t2.`CDD-risk_rating` AS GCDSCDDRiskRating,
# MAGIC     t2.`Global_CO-name`AS GCDSGlobalCOname,
# MAGIC     t2.`Global_CO-location` AS GCDSGlobalCOlocation,
# MAGIC     t2.`Global_CO-email` AS GlobalCOemail,
# MAGIC     t2.`CO-businessline_description` AS GCDSCObusinesslinedescription,
# MAGIC     t2.`Global_CO-Serviced_by` AS GCDSGlobalCOServicedBy,
# MAGIC     t2.`RM-name` AS GCDSRMName,
# MAGIC     t2.`RM-location` AS GCDSRMLocation,
# MAGIC     t2.`RM-email` AS GCDSRMEmail,
# MAGIC     t2.`RM-businessline_description` AS GCDSRMbusinesslineDescription,
# MAGIC     t2.`RM-_Serviced_by` AS GCDSRMServicedBy,
# MAGIC     t3.Life_cycle_status AS GCDSClientlifecyclestatus,
# MAGIC     t2.`CO-email` AS GCDSGCOemail,
# MAGIC     t2.Registered_address_country AS GCDSRegisteredCountry,
# MAGIC     t2.`CDD-risk_rating` AS GCDSCDDRating,
# MAGIC     t1.EDL_LOAD_DTS AS GCDSLastRefresh
# MAGIC FROM 
# MAGIC     GCDS_client_KeyStoreKey AS t1
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_Client AS t2 ON t1.gcid = t2.gcid
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_PartyRole AS t3 ON t1.gcid = t3.gcid
# MAGIC LEFT JOIN
# MAGIC     GCDS_client_Products AS t4 ON t1.gcid = t4.gcid
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_OnboardedLocations AS t5 ON t1.gcid = t5.gcid
# MAGIC WHERE 
# MAGIC     t1.KeyStore_type = 'GCOBID'

# COMMAND ----------

# DBTITLE 1,GCDS GCOB Comparison
# MAGIC %sql
# MAGIC --View to compare the fields between GCOB and GCDS
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GcdsGcobComparison AS
# MAGIC SELECT DISTINCT
# MAGIC     t1.*,
# MAGIC     t2.GcobId AS GCOBGcobid, 
# MAGIC     t2.ClientLifeCycleName AS GCOBClientLifecycleStatus,
# MAGIC     t2.GlobalClientOwner AS GCOBGlobalclientowner,
# MAGIC     t3.CountryOfRegistration AS GCOBRegisteredCountry,
# MAGIC     CASE WHEN t1.GCDSClientlifecyclestatus = 'Client' AND t2.ClientLifeCycleName <> 'Client' THEN 1 ELSE 0 END AS ClientinGCDSandnotaclientinGCOB,
# MAGIC     CASE WHEN t1.GCDSGlobalCOname <> t2.GlobalClientOwner THEN 1 ELSE 0 END AS DifferenceinGCOname,
# MAGIC     CASE WHEN t1.GCDSClientName <> t2.FullLegalName THEN 1 ELSE 0 END AS DifferenceinClientName,
# MAGIC     CASE WHEN t1.GCDSCOBusinesslinedescription <> t2.BusinessLineName OR (t1.GCDSCOBusinesslinedescription IS NULL OR t2.BusinessLineName IS NULL) THEN 1 ELSE 0 END AS DifferenceinBusinessline,
# MAGIC     CASE WHEN t1.GCDSCDDNextReviewdate <> t2.NextReviewDate THEN 1 ELSE 0 END AS DifferenceinNRD,
# MAGIC     CASE WHEN t1.GCDSGlobalCOlocation <> t2.GlobalClientOwner OR (t1.GCDSGlobalCOlocation IS NULL OR t2.GlobalClientOwner IS NULL) THEN 1 ELSE 0 END AS DifferenceinGlobalclientownerLocation,
# MAGIC     CASE WHEN t1.GCDSRegisteredCountry <> t3.CountryOfRegistration THEN 1 ELSE 0 END AS DifferenceinRegisteredCountry,
# MAGIC     CASE WHEN t1.GCDSCDDRating <> t2.ValidatedRiskLevel THEN 1 ELSE 0 END AS DifferenceinCDDRating
# MAGIC FROM radar.Clients AS t2
# MAGIC LEFT JOIN GcdsClientData AS t1 ON t1.GCOBID = t2.GcobId 
# MAGIC LEFT JOIN radar.cases AS t3 ON t2.LatestCaseId = t3.CaseId
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GcobidWithGcidInGcob AS 
# MAGIC WITH CTE_MaxClientId AS (
# MAGIC     SELECT 
# MAGIC         t1.GcobId,
# MAGIC         MAX(t1.Id) AS LegalEntityClientId
# MAGIC     FROM gcob_CaseService_case_LegalEntityClient AS t1
# MAGIC     LEFT JOIN gcob_CaseService_case_Case AS t2 ON t1.Id = t2.LegalEntityClientId
# MAGIC     WHERE t2.CurrentStatus <> 10
# MAGIC     GROUP BY t1.GcobId
# MAGIC ),
# MAGIC CTE_MAX_ActiveLegalEntityClientID AS (
# MAGIC     SELECT 
# MAGIC         CAST(t1.GcobId AS STRING) AS `GCOBIDinGCOB`,
# MAGIC         t3.ValueOfIdentifier AS `GCIDinGCOB`
# MAGIC     FROM CTE_MaxClientId t1
# MAGIC     LEFT JOIN gcob_CaseService_case_LegalEntityClientIdentifier t3 ON t1.LegalEntityClientId = t3.LegalEntityId
# MAGIC     WHERE t3.SystemTypeReferenceId IN ('124', '123')
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC     `GCOBIDinGCOB`,
# MAGIC     `GCIDinGCOB`
# MAGIC FROM CTE_MAX_ActiveLegalEntityClientID

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCIDInGCOBWithoutGCIDInGCDS AS
# MAGIC select `GCIDinGCOB`, t2.gcobid from GcobidWithGcidInGcob  as t1
# MAGIC left join radar.clients as t2 on t1.`GCOBIDinGCOB` = t2.GcobId
# MAGIC where `GCIDinGCOB`not in (select gcid from GcdsClientDataWithAllPartyRole) and t2.ClientLifeCycleName = 'Client' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select `GCIDinGCOB`, t2.gcobid from GcobidWithGcidInGcob  as t1
# MAGIC left join radar.clients as t2 on t1.`GCOBIDinGCOB` = t2.GcobId
# MAGIC where `GCIDinGCOB`not in (select gcid from GcdsClientDataWithAllPartyRole) and t2.ClientLifeCycleName = 'Client'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GcobidWithoutGcidInGcob AS
# MAGIC SELECT GcobId FROM radar.clients WHERE GcobId NOT IN (SELECT `GCOBIDinGCOB` FROM GcobidWithGcidInGcob) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select t1.gcobid, t2.FullLegalName, t2.GlobalKYCPortfolioNew, t2.GlobalReportingRegion from GcobidWithoutGcidInGcob as t1
# MAGIC left join radar.clients as t2 on t1.gcobid = t2.gcobid
# MAGIC where t2.SourceSystemReference = 'GCOB_LegalEntity' and t2.ClientLifeCycleName = 'Client' and t2.GlobalReportingRegion = 'E&A'

# COMMAND ----------

# MAGIC %sql
# MAGIC select gcobid, gcdsid from radar.clients where gcdsid is null and gcobid not in (select gcobid from GcobidWithoutGcidInGcob)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GcobidWithoutGcidInGcob

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HistoricalGCID AS
# MAGIC SELECT 
# MAGIC     t1.`GCIDinGCOB` AS `GCIDfromGCOB`, 
# MAGIC     t1.`GCOBIDinGCOB`, 
# MAGIC     t2.*
# MAGIC FROM 
# MAGIC     GcobidWithGcidInGcob AS t1
# MAGIC LEFT JOIN 
# MAGIC     GCDS_client_KeyStoreKey AS t2 ON t1.`GCIDinGCOB` = t2.`KeyStore_value`
# MAGIC WHERE 
# MAGIC     t2.KeyStore_type = 'Global Client ID'
