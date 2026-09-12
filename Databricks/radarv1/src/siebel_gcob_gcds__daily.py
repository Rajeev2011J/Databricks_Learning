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

# DBTITLE 1,load financed_emissions
spark.read.parquet(f'abfss://calculusgdp@{ReadStorage}.dfs.core.windows.net/FEWholesale/1/data/FiscalYear=*/*.parquet').createOrReplaceTempView('financed_emissions_raw')

# COMMAND ----------

# DBTITLE 1,Load GCOB Data
from datetime import datetime, timedelta
import re

gcob_objects = [
    'party_case_client_details'
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

# COMMAND ----------

# DBTITLE 1,Load Sibel Data
from pyspark.sql import SparkSession
# manually indicating version here, otherwise it does not work for siebel that references a higher number than 2
siebel_tables = {
    'cdf_ggm_rel_x_ar_hist': '4',
    'cdf_ggm_org_hist': '2',
    'cdf_ggm_ar_hist': '2',
    'cdf_ggm_rel_x_pst_addr_hist':'2',
    'cdf_ggm_pst_addr_hist':'2',
    'cdf_ggm_eml_addr_hist': '2',
    'cdf_ggm_rel_x_eml_addr_hist': '2',
    'cdf_ggm_rel_x_rel_hist': '2',
    'cdf_ggm_np_hist': '2',
    'cdf_ggm_rel_x_tel_no_hist': '2',
    'cdf_ggm_tel_no_hist':'2'
    # RISK LEVEL IN SIEBEL
    # , 'cdf_ggm_org_cdd_hist':'3'
    # , 'cdf_ggm_np_cdd_hist':'3'
}
for item, version in siebel_tables.items():
    # Get the most recent file available in gdp
    path = f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('edl_partition_date=')[1][:8] for file in files if 'edl_partition_date=' in file.path)
    spark.read.format('delta').load(f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data').createOrReplaceTempView('siebel_' + item)

# COMMAND ----------

# DBTITLE 1,Load GCDS Data
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
# MAGIC **GCDS Data**

# COMMAND ----------

# MAGIC %sql
# MAGIC --GCDS CLIENTS: Comparing Client Availability Across Different Source Systems
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcdsClients AS
# MAGIC WITH gcds_client_PartytoPartyRelationship_no_manager_cte AS (
# MAGIC   SELECT
# MAGIC     GCID
# MAGIC   FROM gcds_client_PartytoPartyRelationship
# MAGIC   WHERE `Relationship-Type` = 'Has Fund Manager'
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.gcid
# MAGIC , t1.Full_legal_name AS gcdsClientName
# MAGIC , t1.Additional_name AS gcdsAdditionalName
# MAGIC , t4.Life_cycle_status AS gcdsClientlifecyclestatus
# MAGIC , t4.party_role AS gcdsPartyRole
# MAGIC , t2.keystore_type
# MAGIC -- , t1.ORGANISATIONENTITYLEGALFORMLEGALTYPECODE
# MAGIC -- , t1.ORGANISATIONENTITYLEGALFORMLEGALTYPEDESCRIPTION1
# MAGIC -- , t1.ORGANISATIONENTITYLEGALFORMLEGALTYPEDESCRIPTION2
# MAGIC -- , t1.ORGANISATIONENTITYLEGALFORMLEGALTYPEDESCRIPTION3
# MAGIC , GPRAID
# MAGIC , SBWRR as siebelID
# MAGIC , CIF
# MAGIC , NCINOID
# MAGIC --, SIEBEL
# MAGIC , GCOBID
# MAGIC , t3.`Relationship-Type` AS relationshipType
# MAGIC --, t5.uniquegcobid
# MAGIC --, t5.clientlifecyclename
# MAGIC , t1.Party_type AS gcdsPartyType
# MAGIC ,t1.`Global_CO-location`
# MAGIC , t1.Principal_address_country AS gcdsPrincipalAddressCountry
# MAGIC , t1.Registered_address_country AS gcdsRegisteredCountry
# MAGIC , t1.`RM-businessline_description` AS gcdsRMBusinesslineDescription
# MAGIC --, t1.`CO-businessline_description` AS gcdsCObusinesslinedescription
# MAGIC , t1.`Global_CO-businessline_description` AS gcdsGCObusinesslinedescription
# MAGIC , t1.`NationalID-type`
# MAGIC , REPLACE(
# MAGIC       REPLACE(t1.`NationalID-value`, '.', ''),
# MAGIC       ' ',
# MAGIC       ''
# MAGIC   ) AS `NationalID-value`
# MAGIC , CASE 
# MAGIC     WHEN t1.`Global_CO-location` IN ('Utrecht', 'Germany', 'France', 'Belgium', 'London', 'Italy', 'Spain', 'Ireland', 'Kenya') THEN 'E&A'
# MAGIC     WHEN t1.`Global_CO-location` IN ('China', 'Singapore', 'Malaysia', 'Beijing', 'India', 'Hong Kong', 'Shanghai', 'SINGAPORE') THEN 'Asia'
# MAGIC     WHEN t1.`Global_CO-location` IN ('San Francisco', 'Chicago', 'Atlanta', 'Mexico', 'Canada', 'New York') THEN 'North America'
# MAGIC     WHEN t1.`Global_CO-location` IN ('Argentina', 'Chile', 'Brazil') THEN 'South America'
# MAGIC     WHEN t1.`Global_CO-location` IN ('Australia', 'New Zealand', 'RAF') THEN 'RANZ'
# MAGIC     ELSE 'Other'
# MAGIC END AS gcdsReportingRegion
# MAGIC , CASE 
# MAGIC     WHEN t1.`Global_CO-businessline_description` IN ('Leveraged Lending', 'Value Chain Finance', 'B4NL Sponsor Coverage', 'B4NL Coverage', 'B4NL Construction Real Estate', 'B4NL High Tech Digital', 'B4NL Network Coverage', 'B4NL Real Estate Finance Coverage', 'Fund Finance', 'Capital Structuring & Advisory', 'Specialized Lending', 'Corporate Finance', 'Coverage Large Corporates', 'F&A Corporates', 'Credit and Loans', 'ET Corporates', 'ET Developers', 'ET Sponsor Coverage', 'ET Transport and Mobility', 'ET Traders', 'F&A Sponsor Coverage', 'F&A Traders', 'Grootbedrijf', 'Transaction Banking', 'Corporate Lending - Specialized Lending', 'Other Corporates', 'Other Developers', 'Other Traders', 'Publieke Sector', 'Project Finance', 'Real Estate Finance W&R', 'Senior Relationship Banking', 'Sponsor Coverage', 'TCF Agri', 'TCF Commodities', 'Export Finance', 'TCF Energy & Metals', 'TCF Trade Finance Corporate Sales', 'Export Finance Coverage', 'B4EU Construction & Real Estate', 'B4EU Network Coverage', 'B4NL Construction & Real Estate', 'B4EU High Tech Digital', 'B4EU Coverage', 'B4EU HT&D Developers', 'B4EU Real Estate Finance Coverage', 'B4EU Transport and Mobility', 'ET Developers (outside ET Sector)', 'ET Traders (outside ET sector)', 'TCF Agri Commodities', 'TCF Energy', 'TCF Metals & Minerals') THEN 'GCDS Corp'
# MAGIC
# MAGIC     WHEN t1.`Global_CO-businessline_description` IN ('FI & SSA Origination & Syndication', 'FI Lending - Specialized Lending', 'FI Relationship Management', 'FI Solutions Sales', 'Fixed Income Bonds', 'IRR Management', 'Loan Capital Markets', 'MM Derivatives', 'Bond Syndication', 'FI Lending', 'Securitisation & Covered Bonds', 'Securities Finance', 'Liquidity Management', 'Long Term Funding', 'PSP Coverage', 'TCF Trade Finance', 'Investment Grade Bonds', 'Loan Syndication') THEN 'GCDS FI'
# MAGIC
# MAGIC     WHEN t1.`Global_CO-businessline_description` IN ('International Desks', 'Rabo Foundation', 'RNAB', 'Other RN Divisions', 'Smallholder Agroforestry Finance', 'Retail Banking') THEN 'GCDS Retail'
# MAGIC
# MAGIC     WHEN t1.`Global_CO-businessline_description` IN ('Rural Lending') THEN 'GCDS Rural'
# MAGIC     
# MAGIC     WHEN t1.`Global_CO-businessline_description` IN ('Country & FI Risk', 'CFO Corporate Development', 'CRTM', 'DLL', 'FoodBytes', 'Inno Acorn', 'International Payment Services', 'International Services', 'RI CFRO') THEN 'GCDS Other'
# MAGIC END AS gcdsFiOrCorp
# MAGIC FROM gcds_client_client AS t1
# MAGIC LEFT JOIN gcds_client_KeyStoreKey AS t2 ON t1.gcid = t2.gcid
# MAGIC LEFT JOIN gcds_client_PartytoPartyRelationship AS t3 ON t1.gcid = t3.gcid
# MAGIC LEFT JOIN gcds_client_PartyRole t4 ON t1.gcid = t4.gcid
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS GPRAID, status AS GPRAID_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'GPRAID'
# MAGIC ) AS GPRAID ON t1.gcid = GPRAID.gcid
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS SBWRR, status AS SBWRR_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'SBWRR'
# MAGIC ) AS SBWRR ON t1.gcid = SBWRR.gcid
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS CIF, status AS CIF_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'CIF'
# MAGIC ) AS CIF ON t1.gcid = CIF.gcid
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS NCINOID, status AS NCINOID_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'NCINOID'
# MAGIC ) AS NCINOID ON t1.gcid = NCINOID.gcid
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS SIEBEL, status AS SIEBEL_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'SIEBEL'
# MAGIC ) AS SIEBEL ON t1.gcid = SIEBEL.gcid
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS GCOBID, status AS GCOBID_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'GCOBID' and status = 'Active'
# MAGIC ) AS GCOBID ON t1.gcid = GCOBID.gcid
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS GIC, status AS GIC_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'GIC' and status = 'Active'
# MAGIC ) AS GIC ON t1.gcid = GIC.gcid
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS T24, status AS GCOBID_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'T24' and status = 'Active'
# MAGIC ) AS T24 ON t1.gcid = T24.gcid
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS T24_AUZ, status AS GCOBID_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'T24 AUZ' and status = 'Active'
# MAGIC ) AS T24_AUZ ON t1.gcid = T24_AUZ.gcid
# MAGIC
# MAGIC WHERE ((Rabobank_entity <> 'Y') or (Rabobank_entity is null))
# MAGIC AND t2.status = 'Active'
# MAGIC AND t2.keystore_type IN ('GPRAID', 'CIF', 'SBWRR', 'NCINOID', 'SIEBEL', 'GCOBID')
# MAGIC AND T24 IS NULL
# MAGIC AND GIC IS NULL
# MAGIC AND T24_AUZ IS NULL
# MAGIC AND NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM gcds_client_KeyStoreKey AS k
# MAGIC     WHERE k.gcid = t1.gcid AND k.keyStore_type = 'SIEBEL' AND k.status = 'Active'
# MAGIC     AND NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM gcds_client_KeyStoreKey AS k2
# MAGIC         WHERE k2.gcid = k.gcid AND k2.keyStore_type = 'SBWRR' and k2.status = 'Active'
# MAGIC     )
# MAGIC )
# MAGIC AND t1.gcid NOT IN (
# MAGIC     SELECT GCID
# MAGIC     FROM gcds_client_PartytoPartyRelationship_no_manager_cte
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GCDS RAW TABLE: Comparing Table Fields Across Different Source Systems
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcdsRawData AS
# MAGIC
# MAGIC WITH GCOBID_StatusRanked  (
# MAGIC     SELECT distinct
# MAGIC         gcid, 
# MAGIC         KeyStore_value AS GCOBID, 
# MAGIC         status AS GCOBID_status,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY KeyStore_value 
# MAGIC             ORDER BY 
# MAGIC                 CASE 
# MAGIC                     WHEN status = 'Active' THEN 1 
# MAGIC                     WHEN status = 'Inactive' THEN 2 
# MAGIC                     ELSE 3 
# MAGIC                 END
# MAGIC         ) AS rn
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'GCOBID'
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC       t1.gcid
# MAGIC     , t1.Full_legal_name AS gcdsClientName
# MAGIC     , t1.Additional_name AS gcdsAdditionalName
# MAGIC     , t4.Life_cycle_status AS gcdsClientlifecyclestatus
# MAGIC     , t2.keystore_type
# MAGIC     , t1.ORGANISATIONENTITYLEGALFORMLEGALTYPECODE
# MAGIC     , t1.ORGANISATIONENTITYLEGALFORMLEGALTYPEDESCRIPTION1
# MAGIC     -- , t1.ORGANISATIONENTITYLEGALFORMLEGALTYPEDESCRIPTION2
# MAGIC     -- , t1.ORGANISATIONENTITYLEGALFORMLEGALTYPEDESCRIPTION3
# MAGIC     , GPRAID
# MAGIC     , SBWRRID as siebelID
# MAGIC     , CIF
# MAGIC     , NCINOID
# MAGIC     , GCOBID
# MAGIC     , t3.`Relationship-Type` AS relationshipType
# MAGIC     , t1.ResidentialAddress_Country AS gcdsResidentialAddressCountry
# MAGIC     , t1.Principal_address_country AS gcdsPrincipalAddressCountry
# MAGIC     , t1.Party_type AS gcdsPartyType
# MAGIC     , t1.`CDD-entitytype` AS gcdsCddEntitytype
# MAGIC     , t1.`CDD-next_reviewdate` AS gcdsCDDNextReviewdate
# MAGIC     , t1.`CDD-risk_rating` AS gcdsCDDRiskRating
# MAGIC     , t1.`Global_CO-name` AS gcdsGlobalCOname
# MAGIC     , t1.`Global_CO-location` AS gcdsGlobalCOlocation
# MAGIC     , t1.`Global_CO-email` AS globalCOemail
# MAGIC     --, t1.`CO-businessline_description` AS gcdsCObusinesslinedescription
# MAGIC     , t1.`Global_CO-Serviced_by` AS gcdsGlobalCOServicedBy
# MAGIC     , t1.`RM-name` AS gcdsRMName
# MAGIC     , t1.`RM-location` AS gcdsRMLocation
# MAGIC     , t1.`RM-email` AS gcdsRMEmail
# MAGIC     , t1.`RM-businessline_description` AS gcdsRMBusinesslineDescription
# MAGIC     , t1.`RM-_Serviced_by` AS gcdsRMServicedBy
# MAGIC     --, t1.`GCO-email` AS gcdsGCOemail
# MAGIC     , t1.Registered_address_country AS gcdsRegisteredCountry
# MAGIC     , t1.`CDD-risk_rating` AS gcdsCDDRating
# MAGIC     --, t3.Life_cycle_status AS gcdsClientlifecyclestatus
# MAGIC     , t4.party_role AS gcdsPartyRole
# MAGIC     --, t1.Customer_ambition AS clientStrategy
# MAGIC     , t1.Registered_address_streetLine1 as gcdsStreetName
# MAGIC     , t1.Registered_address_house_nr as gcdsHouseNumber
# MAGIC     , t1.Registered_address_zipcode
# MAGIC     , t1.Registered_address_city
# MAGIC     , t1.Registered_address_region
# MAGIC     , t1.Principal_address_country_code
# MAGIC     , CONCAT(
# MAGIC         REPLACE(REPLACE(t1.Registered_address_zipcode, ' ', ''), ',', ''),
# MAGIC         REPLACE(REPLACE(t1.Registered_address_house_nr, ' ', ''), ',', '')
# MAGIC     ) AS gcdsAddress
# MAGIC     , CONCAT(t1.Registered_address_city, t1.Registered_address_region) AS gcdsCityRegion
# MAGIC     , t1.`Global_CO-businessline_description` AS gcdsGCObusinesslinedescription
# MAGIC     , t1.`NationalID-type`
# MAGIC     , REPLACE(
# MAGIC         REPLACE(t1.`NationalID-value`, '.', ''),
# MAGIC         ' ',
# MAGIC         ''
# MAGIC     ) AS `NationalID-value`
# MAGIC   --, t4.`Relationship-Type` AS relationshipType
# MAGIC     , CASE 
# MAGIC         WHEN t1.`Global_CO-location` IN ('Utrecht', 'Germany', 'France', 'Belgium', 'London', 'Italy', 'Spain', 'Ireland', 'Kenya') THEN 'E&A'
# MAGIC         WHEN t1.`Global_CO-location` IN ('China', 'Singapore', 'Malaysia', 'Beijing', 'India', 'Hong Kong', 'Shanghai', 'SINGAPORE') THEN 'Asia'
# MAGIC         WHEN t1.`Global_CO-location` IN ('San Francisco', 'Chicago', 'Atlanta', 'Mexico', 'Canada', 'New York') THEN 'North America'
# MAGIC         WHEN t1.`Global_CO-location` IN ('Argentina', 'Chile', 'Brazil') THEN 'South America'
# MAGIC         WHEN t1.`Global_CO-location` IN ('Australia', 'New Zealand', 'RAF') THEN 'RANZ'
# MAGIC         ELSE 'Other'
# MAGIC     END AS gcdsReportingRegion
# MAGIC     , CASE 
# MAGIC         WHEN t1.`Global_CO-businessline_description` IN ('Leveraged Lending', 'Value Chain Finance', 'B4NL Sponsor Coverage', 'B4NL Coverage', 'B4NL Construction Real Estate', 'B4NL High Tech Digital', 'B4NL Network Coverage', 'B4NL Real Estate Finance Coverage', 'Fund Finance', 'Capital Structuring & Advisory', 'Specialized Lending', 'Corporate Finance', 'Coverage Large Corporates', 'F&A Corporates', 'Credit and Loans', 'ET Corporates', 'ET Developers', 'ET Sponsor Coverage', 'ET Transport and Mobility', 'ET Traders', 'F&A Sponsor Coverage', 'F&A Traders', 'Grootbedrijf', 'Transaction Banking', 'Corporate Lending - Specialized Lending', 'Other Corporates', 'Other Developers', 'Other Traders', 'Publieke Sector', 'Project Finance', 'Real Estate Finance W&R', 'Senior Relationship Banking', 'Sponsor Coverage', 'TCF Agri', 'TCF Commodities', 'Export Finance', 'TCF Energy & Metals', 'TCF Trade Finance Corporate Sales', 'Export Finance Coverage', 'B4EU Construction & Real Estate', 'B4EU Network Coverage', 'B4NL Construction & Real Estate', 'B4EU High Tech Digital', 'B4EU Coverage', 'B4EU HT&D Developers', 'B4EU Real Estate Finance Coverage', 'B4EU Transport and Mobility', 'ET Developers (outside ET Sector)', 'ET Traders (outside ET sector)', 'TCF Agri Commodities', 'TCF Energy', 'TCF Metals & Minerals') THEN 'GCDS Corp'
# MAGIC
# MAGIC         WHEN t1.`Global_CO-businessline_description` IN ('FI & SSA Origination & Syndication', 'FI Lending - Specialized Lending', 'FI Relationship Management', 'FI Solutions Sales', 'Fixed Income Bonds', 'IRR Management', 'Loan Capital Markets', 'MM Derivatives', 'Bond Syndication', 'FI Lending', 'Securitisation & Covered Bonds', 'Securities Finance', 'Liquidity Management', 'Long Term Funding', 'PSP Coverage', 'TCF Trade Finance', 'Investment Grade Bonds', 'Loan Syndication') THEN 'GCDS FI'
# MAGIC
# MAGIC         WHEN t1.`Global_CO-businessline_description` IN ('International Desks', 'Rabo Foundation', 'RNAB', 'Other RN Divisions', 'Smallholder Agroforestry Finance', 'Retail Banking') THEN 'GCDS Retail'
# MAGIC
# MAGIC         WHEN t1.`Global_CO-businessline_description` IN ('Rural Lending') THEN 'GCDS Rural'
# MAGIC         
# MAGIC         WHEN t1.`Global_CO-businessline_description` IN ('Country & FI Risk', 'CFO Corporate Development', 'CRTM', 'DLL', 'FoodBytes', 'Inno Acorn', 'International Payment Services', 'International Services', 'RI CFRO') THEN 'GCDS Other'
# MAGIC END AS gcdsFiOrCorp
# MAGIC
# MAGIC ,CASE
# MAGIC     WHEN NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM gcds_client_KeyStoreKey AS k
# MAGIC         WHERE k.gcid = t1.gcid
# MAGIC           AND k.keyStore_type = 'SIEBEL'
# MAGIC           AND k.status = 'Active'
# MAGIC           AND NOT EXISTS (
# MAGIC               SELECT 1
# MAGIC               FROM gcds_client_KeyStoreKey AS k2
# MAGIC               WHERE k2.gcid = k.gcid
# MAGIC                 AND k2.keyStore_type = 'SBWRR'
# MAGIC                 AND k2.status = 'Active'
# MAGIC           )
# MAGIC     )
# MAGIC     THEN 'excludeMemberBank'
# MAGIC     ELSE 'IncludeMemberBank'
# MAGIC END AS memberBank
# MAGIC
# MAGIC
# MAGIC FROM  gcds_client_client t1
# MAGIC LEFT JOIN gcds_client_KeyStoreKey t2 ON t1.gcid = t2.gcid
# MAGIC LEFT JOIN gcds_client_PartytoPartyRelationship t3 ON t1.gcid = t3.gcid
# MAGIC LEFT JOIN gcds_client_PartyRole t4 ON t1.gcid = t4.gcid
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS GPRAID, status AS GPRAID_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'GPRAID'
# MAGIC ) AS GPRAID ON t1.gcid = GPRAID.gcid
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS SBWRRID, status AS SBWRR_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'SBWRR'
# MAGIC ) AS SBWRRID ON t1.gcid = SBWRRID.gcid
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS CIF, status AS CIF_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'CIF'
# MAGIC ) AS CIF ON t1.gcid = CIF.gcid
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS NCINOID, status AS NCINOID_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'NCINOID'
# MAGIC ) AS NCINOID ON t1.gcid = NCINOID.gcid
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS SIEBEL, status AS SIEBEL_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'SIEBEL'
# MAGIC ) AS SIEBEL ON t1.gcid = SIEBEL.gcid
# MAGIC
# MAGIC /*
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, KeyStore_value AS GCOBID, status AS GCOBID_status
# MAGIC     FROM gcds_client_KeyStoreKey
# MAGIC     WHERE keyStore_type = 'GCOBID' and status = 'Active'
# MAGIC ) AS GCOBID ON t1.gcid = GCOBID.gcid
# MAGIC */
# MAGIC LEFT JOIN (
# MAGIC     SELECT gcid, GCOBID, GCOBID_status
# MAGIC     FROM GCOBID_StatusRanked
# MAGIC     WHERE rn = 1
# MAGIC ) AS GCOBID ON t1.gcid = GCOBID.gcid
# MAGIC
# MAGIC
# MAGIC WHERE --((Rabobank_entity <> 'Y') or (Rabobank_entity is null))
# MAGIC --AND ((t3.`Relationship-Type` <> 'Has Fund Manager') or (t3.`Relationship-Type` is NULL))
# MAGIC --AND t2.status = 'Active'
# MAGIC  t4.party_role = 'Customer'
# MAGIC --AND t2.keystore_type IN ('SBWRR', 'GCOBID')
# MAGIC AND t2.keystore_type IN ('GPRAID', 'CIF', 'SBWRR', 'NCINOID', 'SIEBEL', 'GCOBID')
# MAGIC

# COMMAND ----------

# DBTITLE 1,AMLR REQUEST
# %sql
# SELECT DISTINCT 
#   t1.gcid
#   , t1.GcobId
#   , t1.GCDSClientlifecyclestatus
#   , t1.ORGANISATIONENTITYLEGALFORMLEGALTYPECODE
#   , t1.ORGANISATIONENTITYLEGALFORMLEGALTYPEDESCRIPTION1
#   , t1.GCDSReportingRegion
#   , t2.GlobalReportingRegion AS GCOBReportingRegion
#   , t2.KYCGroup
#   , t1.gcdsPrincipalAddressCountry AS GCDSRegisteredCountry
#   , t3.CountryOfRegistration AS GCOBRegisteredCountry

# FROM GCDSRawData t1
# JOIN radar.clients t2 ON t1.gcobid = t2.UniqueGcobId 
# LEFT JOIN radar.cases t3 ON t2.sourceclient = t3.SourceClient

# COMMAND ----------

# MAGIC %md
# MAGIC **GCOB Data**

# COMMAND ----------

# DBTITLE 1,GCOB Data
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sggGcobData AS
# MAGIC
# MAGIC select distinct 
# MAGIC   t1.uniquegcobid,
# MAGIC   t1.FullLegalName,
# MAGIC   t1.clientlifecyclename,
# MAGIC   REPLACE(
# MAGIC       REPLACE(t2.IncorporationNumber, '.', ''),
# MAGIC       ' ',
# MAGIC       ''
# MAGIC   ) AS IncorporationNumber,
# MAGIC   CONCAT(
# MAGIC       REPLACE(REPLACE(t2.RegisteredPostalCode, ' ', ''), ',', ''),
# MAGIC       REPLACE(REPLACE(t2.RegisteredNumber, ' ', ''), ',', '')
# MAGIC   ) AS gcobAddress,
# MAGIC   CONCAT(t2.RegisteredCity, t2.RegisteredRegion) AS gcobCityRegion,
# MAGIC   t2.RegisteredStreet as gcobSteetName,
# MAGIC   t2.RegisteredNumber as gcobHouseNumber,
# MAGIC   t2.RegisteredCountryIsoCode,
# MAGIC   t1.GlobalClientOwner,
# MAGIC   t1.GlobalClientOwnerLocation,
# MAGIC   t1.nextreviewdate,
# MAGIC   t2.ValidatedRiskLevel,
# MAGIC   t1.BusinessLineName,
# MAGIC   t1.GlobalKYCPortfolioNew,
# MAGIC   t1.SectorTeam,
# MAGIC   t1.KYCGroup,
# MAGIC   t1.GlobalReportingRegion,
# MAGIC   t1.ReviewLocation, 
# MAGIC   t3.ContactPersonEmailAddress, 
# MAGIC   t3.ContactPersonTelephoneNumber, 
# MAGIC   t1.SourceClient, 
# MAGIC   t1.GCDSID as gcidInGcob,
# MAGIC   t1.ClientCaseInitiationStart,
# MAGIC   t1.CDDExecution
# MAGIC from radar.clients as t1
# MAGIC left join radar.cases as t2 on t1.SourceClient = t2.SourceClient
# MAGIC left join party_case_client_details as t3 on t1.SourceClient = t3.SourceClient

# COMMAND ----------

# MAGIC %md
# MAGIC **Siebel Data**

# COMMAND ----------

# DBTITLE 1,Siebel Client Information
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW siebelClientInformation AS
# MAGIC select distinct
# MAGIC   t1.rel_id as siebelId,
# MAGIC   t1.ORG_LGL_NM AS siebelClientName,
# MAGIC   t1.rel_st_tp_ggm_dsc AS siebelClientLifeCycleName,
# MAGIC   REPLACE(
# MAGIC       REPLACE(t1.KVK_NO, '.', ''),
# MAGIC       ' ',
# MAGIC       ''
# MAGIC   ) AS siebelKvkNumber,
# MAGIC   t1.bnk_code as siebelBankCode,
# MAGIC   t1.iddoc_no, t1.frgn_nat_org_id,
# MAGIC   REPLACE(
# MAGIC       REPLACE(t1.frgn_nat_org_id, '.', ''),
# MAGIC       ' ',
# MAGIC       ''
# MAGIC   ) AS siebelKVKForiegnEntity,
# MAGIC   --t1.iddoc_tp_ggm_dsc as siebelKVKType,
# MAGIC   t1.org_incor_cty_code as siebelCountryOfRegistration,
# MAGIC case 
# MAGIC   when  t1.org_incor_cty_code = 'NL' then 'Dutch Entity'
# MAGIC   when  t1.org_incor_cty_code <> 'NL' and t1.org_incor_cty_code <> ''  then 'Non Dutch Entity'
# MAGIC   --else 'Non Dutch Entity'
# MAGIC   end as entityType
# MAGIC FROM siebel_cdf_ggm_org_hist AS t1
# MAGIC WHERE       
# MAGIC   t1.Edl_valid_to_dts like '9999-12-31%'
# MAGIC   AND t1.bnk_code IN ('3000', '3400') AND t1.del_f = 'N'

# COMMAND ----------

# DBTITLE 1,siebelDummyDirector
# MAGIC %sql
# MAGIC -- bucket 6 from Firebird SGG project
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW siebelDummyDirector AS
# MAGIC SELECT DISTINCT
# MAGIC   t1.rel_id as siebelId
# MAGIC   , t1.ORG_LGL_NM AS siebelClientName
# MAGIC   , t1.rel_st_tp_ggm_dsc AS siebelClientLifeCycleName
# MAGIC   ,  REPLACE(
# MAGIC         REPLACE(t1.KVK_NO, '.', ''),
# MAGIC         ' ',
# MAGIC         ''
# MAGIC     ) AS siebelKvkNumber
# MAGIC   , t2.rel_x_rel_rlshp_tp_ggm_code -- relationship type, eg director
# MAGIC   , t3.full_gvn_nm
# MAGIC   , t3.surnm
# MAGIC   , t5.uniquegcobid
# MAGIC   , t5.GlobalClientOwner
# MAGIC   , t5.GlobalClientOwnerLocation
# MAGIC   , t5.GlobalKYCPortfolioNew
# MAGIC   , t5.SectorTeam
# MAGIC   , t5.KYCGroup
# MAGIC   , t5.GlobalReportingRegion
# MAGIC
# MAGIC FROM siebel_cdf_ggm_org_hist t1
# MAGIC LEFT JOIN siebel_cdf_ggm_rel_x_rel_hist t2 ON t1.rel_id = t2.rlshp_fm_rel_id
# MAGIC LEFT JOIN siebel_cdf_ggm_np_hist t3 ON t2.rlshp_to_rel_id = t3.rel_id
# MAGIC LEFT JOIN gcdsrawdata t4 ON t1.rel_id = t4.siebelID
# MAGIC LEFT JOIN sggGcobData t5 ON t4.gcobid = t5.uniquegcobid
# MAGIC
# MAGIC WHERE t1.Edl_valid_to_dts LIKE '9999-12-31%'
# MAGIC   AND t1.bnk_code IN ('3000', '3400')
# MAGIC   AND LOWER(TRIM(t3.surnm)) LIKE '%umm%'

# COMMAND ----------

# DBTITLE 1,Siebel Product Information
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW siebelProductInformation AS
# MAGIC select distinct
# MAGIC   t1.rel_id as siebelId,
# MAGIC   t1.ORG_LGL_NM AS siebelClientName,
# MAGIC   -- t1.KVK_NO AS siebelKvkNumber, -- MOVED TO siebelClientInformation
# MAGIC   t1.rel_st_tp_ggm_dsc AS siebelClientLifeCycleName,
# MAGIC   t2.cmrcl_pd_tp_ctlg_nm AS siebelProductName,
# MAGIC   t2.ar_st_ggm_dsc AS siebelProductLifeCycleName,
# MAGIC
# MAGIC   --Siebel Former client but with active products
# MAGIC   CASE
# MAGIC     WHEN LOWER(TRIM(t1.rel_st_tp_ggm_dsc)) = 'ex-klant' AND LOWER(TRIM(t2.ar_st_ggm_dsc)) = 'active' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS siebelFormerClientWithActiveProducts
# MAGIC   
# MAGIC FROM siebel_cdf_ggm_org_hist AS t1
# MAGIC LEFT JOIN siebel_cdf_ggm_rel_x_ar_hist AS t2 ON t1.rel_id = t2.rel_id
# MAGIC WHERE       
# MAGIC   t1.Edl_valid_to_dts like '9999-12-31%'
# MAGIC   AND t2.Edl_valid_to_dts like '9999-12-31%'
# MAGIC   AND t1.bnk_code IN ('3000', '3400')
# MAGIC   AND t2.del_f = 'N'

# COMMAND ----------

# DBTITLE 1,Siebel Address Information
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW siebelAddressInformation AS
# MAGIC SELECT 
# MAGIC     t1.rel_id AS siebelId,
# MAGIC     REPLACE(t3.pst_code, ' ', '') AS siebelAddressPostalCode,
# MAGIC     t3.hs_no AS siebelAddressHouseNumber,
# MAGIC     t3.hs_no_exn AS siebelAddressHouseNumberExtension,
# MAGIC     t3.str_sufx AS siebelLocatieOmschrijving,
# MAGIC     t3.str_nm AS siebelAddressStreetName,
# MAGIC     t3.city_nm AS siebelAddressCityName,
# MAGIC     t3.str_addr_line AS siebelStreetAddressLine,
# MAGIC     REPLACE(
# MAGIC     CONCAT(
# MAGIC         REPLACE(t3.pst_code, ' ', ''),
# MAGIC         REPLACE(t3.hs_no, ' ', ''),
# MAGIC         REPLACE(COALESCE(t3.hs_no_exn, ''), ' ', ''),
# MAGIC         REPLACE(COALESCE(t3.str_sufx, ''), ' ', '')
# MAGIC     ),
# MAGIC     ',', ''
# MAGIC     ) AS siebelAddress,
# MAGIC     t3.edl_valid_from_dts
# MAGIC FROM siebel_cdf_ggm_org_hist AS t1 
# MAGIC LEFT JOIN siebel_cdf_ggm_rel_x_pst_addr_hist AS t2 on t1.rel_id =t2.rel_id
# MAGIC JOIN siebel_cdf_ggm_pst_addr_hist AS t3 
# MAGIC     ON t3.addr_id = t2.addr_id
# MAGIC     AND t3.edl_valid_to_dts = '9999-12-31'
# MAGIC     AND t3.del_f = 'N'    -- actual record only
# MAGIC WHERE 
# MAGIC     t2.prim_f = 'Y'       -- (set for addr_type_cd = '1' = 'Woon-/Vestigingsadres')
# MAGIC     AND t2.edl_valid_to_dts = '9999-12-31'
# MAGIC     AND t2.del_f = 'N'    -- actual record only
# MAGIC     AND t1.bnk_code IN ('3000', '3400')
# MAGIC     AND t1.edl_valid_to_dts = '9999-12-31'

# COMMAND ----------

# DBTITLE 1,Siebel Email Address Information - OLD, INCORRECT
# %sql
# CREATE OR REPLACE TEMPORARY VIEW siebelEmailAddressInformation AS
# SELECT DISTINCT
#   t1.rel_id AS siebelId,
#   t3.eml_addr AS siebelEmailAddress,
#   t1.org_incor_cty_code,
#   t1.rgst_offc_city_nm


# FROM 
#   siebel_cdf_ggm_org_hist AS t1
# --LEFT JOIN 
#  -- siebel_cdf_ggm_rel_x_eml_addr_hist AS t2 ON t1.rel_id = t2.rel_id
# FULL OUTER JOIN 
#   siebel_cdf_ggm_eml_addr_hist AS t3 ON t3.addr_id = t1.eml_addr_verfd_by_empe_id AND t3.del_f = 'N' AND t3.Edl_valid_to_dts LIKE '9999-12-31%' -- t3.addr_id, t1.prim_eml_addr_id
# WHERE       
#   t1.Edl_valid_to_dts LIKE '9999-12-31%'
#   AND t1.bnk_code IN ('3000', '3400')
#   --AND t2.Edl_valid_to_dts LIKE '9999-12-31%'
#   --AND t2.addr_type_cd = '1'
#   AND t1.del_f = 'N'
# AND t1.rel_st_tp_ggm_dsc = 'Klant'

# COMMAND ----------

# DBTITLE 1,Siebel Email Address Information - NEW LOGIC ON REL X
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW siebelEmailAddressInformation AS
# MAGIC SELECT DISTINCT
# MAGIC   t1.rel_id AS siebelId
# MAGIC   , t3.eml_addr AS siebelEmailAddress
# MAGIC   , t1.org_incor_cty_code
# MAGIC   , t1.rgst_offc_city_nm
# MAGIC   
# MAGIC FROM siebel_cdf_ggm_org_hist t1
# MAGIC
# MAGIC LEFT JOIN siebel_cdf_ggm_rel_x_eml_addr_hist t2 ON t1.rel_id = t2.rel_id AND t2.edl_valid_to_dts = '9999-12-31' AND t2.del_f = 'N' -- get email address for the actual record
# MAGIC LEFT JOIN siebel_cdf_ggm_eml_addr_hist t3 ON t3.addr_id = t2.addr_id AND t3.edl_valid_to_dts = '9999-12-31' AND t3.del_f = 'N' -- get email address for the actual record
# MAGIC
# MAGIC where 1=1
# MAGIC   AND t1.edl_valid_to_dts = '9999-12-31'  --actual record
# MAGIC   AND t1.bnk_code IN ('3000', '3400')
# MAGIC   AND t1.del_f = 'N'
# MAGIC   AND t1.rel_st_tp_ggm_dsc = 'Klant'
# MAGIC   -- and t1.addr_id IS NOT NULL

# COMMAND ----------

# DBTITLE 1,Siebel Email Address Information: New Logic WIP:
# MAGIC %sql
# MAGIC /*
# MAGIC --THIS LOGIC IS SIMILAR TO THE ONE AS Siebel Address Information BUT THE NUMBER OF BLANKS ARE VERY LESS
# MAGIC --PLEASE RE-VISIT THE LOGIC FOR THIS ONE.
# MAGIC CREATE OR REPLACE TEMPORARY VIEW siebelEmailAddressInformationtest AS
# MAGIC SELECT DISTINCT
# MAGIC   t1.rel_id AS siebelId,
# MAGIC   t3.eml_addr AS siebelEmailAddress,
# MAGIC   t1.org_incor_cty_code,
# MAGIC   t1.rgst_offc_city_nm
# MAGIC FROM
# MAGIC   siebel_cdf_ggm_org_hist AS t1
# MAGIC   LEFT JOIN siebel_cdf_ggm_rel_x_eml_addr_hist AS t2 on t1.rel_id =t2.rel_id -- try otherwise full outer
# MAGIC --LEFT JOIN
# MAGIC  -- siebel_cdf_ggm_rel_x_eml_addr_hist AS t2 ON t1.rel_id = t2.rel_id
# MAGIC LEFT JOIN siebel_cdf_ggm_eml_addr_hist AS t3 ON t3.addr_id = t2.addr_id -- JOIN
# MAGIC  AND t3.edl_valid_to_dts = '9999-12-31'
# MAGIC     AND t3.del_f = 'N'    -- actual record only
# MAGIC WHERE
# MAGIC     t2.edl_valid_to_dts = '9999-12-31'
# MAGIC     AND t2.del_f = 'N'    -- actual record only
# MAGIC     AND t1.bnk_code IN ('3000', '3400')
# MAGIC     AND t1.edl_valid_to_dts = '9999-12-31'
# MAGIC AND t1.rel_st_tp_ggm_dsc = 'Klant'
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC **Comparing Clients in one scource system but not in another**

# COMMAND ----------

# DBTITLE 1,GCOBID without GCID in GCOB
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcobWithoutGcidInGcob AS
# MAGIC SELECT DISTINCT
# MAGIC     t1.uniquegcobid,
# MAGIC     t2.gcid,
# MAGIC     t1.gcidInGcob
# MAGIC FROM sggGcobData AS t1
# MAGIC LEFT JOIN
# MAGIC (SELECT gcid, keystore_value FROM gcds_client_KeyStoreKey
# MAGIC WHERE keyStore_type = 'GCOBID') AS t2
# MAGIC ON
# MAGIC t1.UniqueGcobId = t2.keystore_value
# MAGIC WHERE t1.gcidInGcob IS NULL 
# MAGIC   AND t1.UniqueGcobId NOT LIKE '%NP%' 
# MAGIC

# COMMAND ----------

# DBTITLE 1,GCOB client missing in GCDS
# MAGIC %sql
# MAGIC --CLIENTS IN GCOB BUT NOT IN GCDS
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcobClientNotInGcds AS
# MAGIC select distinct uniquegcobid, t2.gcid
# MAGIC from sggGcobData as t1
# MAGIC left join gcdsClients as t2 on t1.uniquegcobid = t2.gcobid
# MAGIC where t2.gcid is null and t1.uniquegcobid not like '%NP%' 
# MAGIC

# COMMAND ----------

# DBTITLE 1,GCDS client missing in GCOB
# MAGIC %sql
# MAGIC --CLIENTS IN GCDS BUT NOT IN GCOB
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcdsClientNotInGcob AS
# MAGIC select distinct uniquegcobid, t1.gcid
# MAGIC from  gcdsClients as t1
# MAGIC left join sggGcobData as t2 on t2.uniquegcobid = t1.gcobid
# MAGIC where t2.uniquegcobid is null

# COMMAND ----------

# DBTITLE 1,GCOB client missing in Siebel
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gobClientNotInSiebel AS
# MAGIC select distinct t1.uniquegcobid, t2.gcid, t3.siebelID, t1.clientlifecyclename from sggGcobData as t1
# MAGIC left join gcdsrawdata as t2 on t1.uniquegcobid = t2.gcobid 
# MAGIC left join siebelclientinformation as t3 on t2.siebelID = t3.siebelID
# MAGIC where t3.siebelid is null 

# COMMAND ----------

# DBTITLE 1,SIEBEL Client missing in GCDS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW siebelClientNotInGcds AS
# MAGIC select t1.siebelId, t2.siebelId as gcdsSiebelID from siebelClientInformation t1
# MAGIC left join gcdsrawdata t2 on t1.siebelId = t2.siebelId
# MAGIC where t2.gcid is null 

# COMMAND ----------

# DBTITLE 1,SIEBEL Client missing in GCOB
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW siebelClientNotInGcob AS
# MAGIC select distinct t1.siebelID, t2.gcid, t2.gcdsClientName, t2.gcdsAdditionalName, t2.gcdsPartyType, t2.gcdsPartyRole from siebelclientinformation as t1
# MAGIC left join gcdsrawdata as t2 on t1.siebelID = t2.siebelID 
# MAGIC left join sggGcobData as t3 on t2.gcobid = t3.uniqueGcobid
# MAGIC where t3.uniqueGcobid is null 

# COMMAND ----------

# MAGIC %md
# MAGIC **Comparing table fields from different scource systems**

# COMMAND ----------

# DBTITLE 1,siebelGcobGcdsClientComparisonTemp:Depricated on 9th April, 2026 then restored
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW siebelGcobGcdsClientComparisonTemp AS
# MAGIC
# MAGIC SELECT DISTINCT  
# MAGIC CASE
# MAGIC     WHEN t2.siebelID IS NOT NULL AND t2.siebelID <> '' 
# MAGIC     THEN t2.siebelID || '_' 
# MAGIC     ELSE '' 
# MAGIC END ||
# MAGIC COALESCE(t5.UniqueGcobId, '') ||
# MAGIC CASE 
# MAGIC     WHEN t1.gcid IS NOT NULL AND t1.gcid <> '' 
# MAGIC     THEN '_' || t1.gcid 
# MAGIC     ELSE '' 
# MAGIC END AS sggUniqueIdentifier
# MAGIC
# MAGIC
# MAGIC   , t1.gcid 
# MAGIC   , t2.siebelID
# MAGIC   , t5.UniqueGcobId
# MAGIC   , t1.gcdsPartyRole
# MAGIC
# MAGIC
# MAGIC   -- NAME COMPARISON
# MAGIC   , CASE
# MAGIC       WHEN LOWER(TRIM(t1.gcdsClientName)) = LOWER(TRIM(t5.FullLegalName)) THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           REGEXP_REPLACE(
# MAGIC             REGEXP_REPLACE(
# MAGIC               REGEXP_REPLACE(
# MAGIC                 REGEXP_REPLACE(
# MAGIC                   TRANSLATE(t1.gcdsClientName, 
# MAGIC                     'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC                     'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC                   ), 
# MAGIC                   'DLL', 'De Lage Landen'
# MAGIC                 ), 
# MAGIC                 'DAC', 'Designated Activity Company'
# MAGIC               ), 
# MAGIC               '(?i)\\bLtd\\.?', 'Limited'
# MAGIC             ), 
# MAGIC             '(?i)\\bLtda\\.?', 'Limitada'
# MAGIC           ),
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) = LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           REGEXP_REPLACE(
# MAGIC             REGEXP_REPLACE(
# MAGIC               REGEXP_REPLACE(
# MAGIC                 REGEXP_REPLACE(
# MAGIC                   TRANSLATE(t5.FullLegalName, 
# MAGIC                     'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC                     'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC                   ), 
# MAGIC                   'DLL', 'De Lage Landen'
# MAGIC                 ), 
# MAGIC                 'DAC', 'Designated Activity Company'
# MAGIC               ), 
# MAGIC               '(?i)\\bLtd\\.?', 'Limited'
# MAGIC             ), 
# MAGIC             '(?i)\\bLtda\\.?', 'Limitada'
# MAGIC           ),
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) THEN 3 -- special characters removed, string match
# MAGIC       WHEN t1.gcdsClientName IS NULL OR t5.FullLegalName IS NULL OR TRIM(t1.gcdsClientName) = '' OR TRIM(t5.FullLegalName) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS gcobGcdsClientNameMatch
# MAGIC
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN LOWER(TRIM(t2.siebelClientName)) = LOWER(TRIM(t5.FullLegalName)) THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           REGEXP_REPLACE(
# MAGIC             REGEXP_REPLACE(
# MAGIC               REGEXP_REPLACE(
# MAGIC                 REGEXP_REPLACE(
# MAGIC                   TRANSLATE(t2.siebelClientName, 
# MAGIC                     'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC                     'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC                   ), 
# MAGIC                   'DLL', 'De Lage Landen'
# MAGIC                 ), 
# MAGIC                 'DAC', 'Designated Activity Company'
# MAGIC               ), 
# MAGIC               '(?i)\\bLtd\\.?', 'Limited'
# MAGIC             ), 
# MAGIC             '(?i)\\bLtda\\.?', 'Limitada'
# MAGIC           ),
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) = LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           REGEXP_REPLACE(
# MAGIC             REGEXP_REPLACE(
# MAGIC               REGEXP_REPLACE(
# MAGIC                 REGEXP_REPLACE(
# MAGIC                   TRANSLATE(t5.FullLegalName, 
# MAGIC                     'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC                     'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC                   ), 
# MAGIC                   'DLL', 'De Lage Landen'
# MAGIC                 ), 
# MAGIC                 'DAC', 'Designated Activity Company'
# MAGIC               ), 
# MAGIC               '(?i)\\bLtd\\.?', 'Limited'
# MAGIC             ), 
# MAGIC             '(?i)\\bLtda\\.?', 'Limitada'
# MAGIC           ),
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) THEN 3 -- special characters removed, string match
# MAGIC      WHEN t2.siebelClientName IS NULL OR t5.FullLegalName IS NULL OR TRIM(t2.siebelClientName) = '' OR TRIM(t5.FullLegalName) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS gcobSiebelClientNameMatch
# MAGIC
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN LOWER(TRIM(t2.siebelClientName)) = LOWER(TRIM(t1.gcdsClientName)) THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           REGEXP_REPLACE(
# MAGIC             REGEXP_REPLACE(
# MAGIC               REGEXP_REPLACE(
# MAGIC                 REGEXP_REPLACE(
# MAGIC                   TRANSLATE(t2.siebelClientName, 
# MAGIC                     'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC                     'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC                   ), 
# MAGIC                   'DLL', 'De Lage Landen'
# MAGIC                 ), 
# MAGIC                 'DAC', 'Designated Activity Company'
# MAGIC               ), 
# MAGIC               '(?i)\\bLtd\\.?', 'Limited'
# MAGIC             ), 
# MAGIC             '(?i)\\bLtda\\.?', 'Limitada'
# MAGIC           ),
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) = LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           REGEXP_REPLACE(
# MAGIC             REGEXP_REPLACE(
# MAGIC               REGEXP_REPLACE(
# MAGIC                 REGEXP_REPLACE(
# MAGIC                   TRANSLATE(t1.gcdsClientName, 
# MAGIC                     'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC                     'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC                   ), 
# MAGIC                   'DLL', 'De Lage Landen'
# MAGIC                 ), 
# MAGIC                 'DAC', 'Designated Activity Company'
# MAGIC               ), 
# MAGIC               '(?i)\\bLtd\\.?', 'Limited'
# MAGIC             ), 
# MAGIC             '(?i)\\bLtda\\.?', 'Limitada'
# MAGIC           ),
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) THEN 3 -- special characters removed, string match
# MAGIC       WHEN t2.siebelClientName IS NULL OR t1.gcdsClientName IS NULL OR TRIM(t2.siebelClientName) = '' OR TRIM(t1.gcdsClientName) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS gcdsSiebelClientNameMatch
# MAGIC
# MAGIC
# MAGIC   -- Client lifecycle status comparison
# MAGIC   , CASE 
# MAGIC       WHEN LOWER(TRIM(t1.gcdsClientlifecyclestatus)) = 'client' AND LOWER(TRIM(t5.clientlifecyclename)) = 'client' THEN 1  -- string match
# MAGIC       WHEN LOWER(TRIM(t1.gcdsClientlifecyclestatus)) = 'former client' AND LOWER(TRIM(t5.clientlifecyclename)) = 'formerclient' THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(t1.gcdsClientlifecyclestatus)) = 'prospect' AND LOWER(TRIM(t5.clientlifecyclename)) = 'prospect' THEN 1 -- string match
# MAGIC       WHEN t1.gcdsClientlifecyclestatus IS NULL OR t5.clientlifecyclename IS NULL OR TRIM(t1.gcdsClientlifecyclestatus) = '' OR TRIM(t5.clientlifecyclename) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS gcobGcdsClientLifeCycleStatusMatch
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN LOWER(TRIM(t2.siebelClientLifeCycleName)) = 'klant' AND LOWER(TRIM(t5.clientlifecyclename)) = 'client' THEN 1 
# MAGIC       WHEN LOWER(TRIM(t2.siebelClientLifeCycleName)) = 'ex-klant' AND LOWER(TRIM(t5.clientlifecyclename)) = 'formerclient' THEN 1 
# MAGIC       WHEN LOWER(TRIM(t2.siebelClientLifeCycleName)) = 'prospect' AND LOWER(TRIM(t5.clientlifecyclename)) = 'prospect' THEN 1
# MAGIC       WHEN ((t2.siebelClientLifeCycleName IS NULL) OR (t5.clientlifecyclename IS NULL)) OR (TRIM(t2.siebelClientLifeCycleName) = '' OR TRIM(t5.clientlifecyclename) = '') THEN 2 -- empty or null
# MAGIC       ELSE 0 
# MAGIC     END AS gcobSiebelClientLifeCycleStatusMatch
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN LOWER(TRIM(t2.siebelClientLifeCycleName)) = 'klant' AND LOWER(TRIM(t1.gcdsClientlifecyclestatus)) = 'client' THEN 1 
# MAGIC       WHEN LOWER(TRIM(t2.siebelClientLifeCycleName)) = 'ex-klant' AND LOWER(TRIM(t1.gcdsClientlifecyclestatus)) = 'former client' THEN 1 
# MAGIC       WHEN LOWER(TRIM(t2.siebelClientLifeCycleName)) = 'prospect' AND LOWER(TRIM(t1.gcdsClientlifecyclestatus)) = 'prospect' THEN 1
# MAGIC      WHEN t2.siebelClientLifeCycleName IS NULL OR t1.gcdsClientlifecyclestatus IS NULL OR TRIM(t2.siebelClientLifeCycleName) = '' OR TRIM(t1.gcdsClientlifecyclestatus) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 
# MAGIC     END AS gcdsSiebelClientLifeCycleStatusMatch
# MAGIC
# MAGIC
# MAGIC   -- -- KVK Number and Gcob incorporation number comparison
# MAGIC   -- , IFF((TRIM(t2.siebelKvkNumber) = TRIM(t5.IncorporationNumber)) OR (TRIM(t2.siebelKvkNumber) = '' AND TRIM(t5.RegisteredCountryIsoCode) != 'NL'), 1, 0) AS siebelGcobIncorporationNumberMatch
# MAGIC     ,CASE
# MAGIC         WHEN TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) = TRIM(t5.IncorporationNumber)
# MAGIC              AND t2.entitytype = 'Dutch Entity'
# MAGIC              AND t5.RegisteredCountryIsoCode = 'NL'
# MAGIC              THEN 1
# MAGIC         WHEN ((TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) IS NULL OR TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) = '')
# MAGIC              OR (TRIM(t5.IncorporationNumber) IS NULL OR TRIM(t5.IncorporationNumber) = '') )
# MAGIC              AND t2.entitytype = 'Dutch Entity'
# MAGIC              AND t5.RegisteredCountryIsoCode = 'NL'
# MAGIC              THEN 2
# MAGIC         ELSE 0
# MAGIC     END AS siebelGcobDutchIncorporationNumberMatch,
# MAGIC
# MAGIC     -- 2. Siebel vs GCOB (Non-Dutch)
# MAGIC     CASE
# MAGIC         WHEN TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) = TRIM(t5.IncorporationNumber)
# MAGIC             --  AND t2.entitytype <> 'Dutch Entity'
# MAGIC             --  AND t5.RegisteredCountryIsoCode <> 'NL'
# MAGIC              THEN 1
# MAGIC         WHEN ( (TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) IS NULL OR TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) = '')
# MAGIC              OR (TRIM(t5.IncorporationNumber) IS NULL OR TRIM(t5.IncorporationNumber) = '') )
# MAGIC             --  AND t2.entitytype <> 'Dutch Entity'
# MAGIC             --  AND t5.RegisteredCountryIsoCode <> 'NL'
# MAGIC              THEN 2
# MAGIC         ELSE 0
# MAGIC     END AS siebelGcobNonDutchIncorporationNumberMatch,
# MAGIC
# MAGIC     -- 3. Siebel vs GCDS (Dutch)
# MAGIC     CASE
# MAGIC         WHEN TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) = TRIM(t1.`NationalID-value`)
# MAGIC              AND t1.`NationalID-type` IN ('Kamer van Koophandel', 'Kamer van Koophandel nummer', 'Chamber of Commerce Registration')
# MAGIC              AND t2.entitytype = 'Dutch Entity'
# MAGIC              AND t1.gcdsRegisteredCountry IN ('Netherlands (the)', 'Netherlands') THEN 1
# MAGIC         WHEN ( (TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) IS NULL OR TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) = '')
# MAGIC              OR (TRIM(t1.`NationalID-value`) IS NULL OR TRIM(t1.`NationalID-value`) = '') )
# MAGIC              AND t2.entitytype = 'Dutch Entity'
# MAGIC              AND t1.gcdsRegisteredCountry IN ('Netherlands (the)', 'Netherlands') THEN 2
# MAGIC         WHEN TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) != TRIM(t1.`NationalID-value`)
# MAGIC              AND t1.`NationalID-type`IN ('Kamer van Koophandel', 'Kamer van Koophandel nummer', 'Chamber of Commerce Registration')
# MAGIC              AND TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) != ''
# MAGIC              AND TRIM(t1.`NationalID-value`) != ''
# MAGIC              AND t2.entitytype = 'Dutch Entity'
# MAGIC              AND t1.gcdsRegisteredCountry IN ('Netherlands (the)', 'Netherlands') THEN 0
# MAGIC         ELSE 3 -- Default for cases where NationalID-type is not Kamer van Koophandel
# MAGIC     END AS siebelGcdsDutchIncorporationNumberMatch,
# MAGIC
# MAGIC     -- 4. Siebel vs GCDS (Non-Dutch)
# MAGIC     CASE
# MAGIC       WHEN TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) = TRIM(t1.`NationalID-value`)
# MAGIC         --AND t1.`NationalID-type` IN ('Kamer van Koophandel', 'Kamer van Koophandel nummer', 'Chamber of Commerce Registration')
# MAGIC         -- AND t2.entitytype <> 'Dutch Entity'
# MAGIC         -- AND t1.gcdsRegisteredCountry NOT IN ('Netherlands (the)', 'Netherlands')
# MAGIC         THEN 1
# MAGIC       WHEN ( (TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) IS NULL OR TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) = '')
# MAGIC         OR (TRIM(t1.`NationalID-value`) IS NULL OR TRIM(t1.`NationalID-value`) = '') )
# MAGIC         THEN 2
# MAGIC       WHEN TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) != TRIM(t1.`NationalID-value`)
# MAGIC         --AND t1.`NationalID-type` IN ('Kamer van Koophandel', 'Kamer van Koophandel nummer', 'Chamber of Commerce Registration')
# MAGIC         AND TRIM(coalesce(t2.siebelKVKForiegnEntity, t2.siebelKvkNumber)) != ''
# MAGIC         AND TRIM(t1.`NationalID-value`) != ''
# MAGIC         -- AND t2.entitytype <> 'Dutch Entity'
# MAGIC         -- AND t1.gcdsRegisteredCountry NOT IN ('Netherlands (the)', 'Netherlands')
# MAGIC         THEN 0
# MAGIC       ELSE 3
# MAGIC     END AS siebelGcdsNonDutchIncorporationNumberMatch,
# MAGIC
# MAGIC     -- 5. GCOB vs GCDS (Dutch)
# MAGIC     CASE
# MAGIC         WHEN TRIM(t5.IncorporationNumber) = TRIM(t1.`NationalID-value`)
# MAGIC              AND t1.`NationalID-type` IN ('Kamer van Koophandel', 'Kamer van Koophandel nummer', 'Chamber of Commerce Registration')
# MAGIC              AND t1.gcdsRegisteredCountry IN ('Netherlands (the)', 'Netherlands')
# MAGIC              AND t5.RegisteredCountryIsoCode = 'NL' THEN 1
# MAGIC         WHEN ( (TRIM(t5.IncorporationNumber) IS NULL OR TRIM(t5.IncorporationNumber) = '')
# MAGIC              OR (TRIM(t1.`NationalID-value`) IS NULL OR TRIM(t1.`NationalID-value`) = '') )
# MAGIC              AND t1.gcdsRegisteredCountry IN ('Netherlands (the)', 'Netherlands')
# MAGIC              AND t5.RegisteredCountryIsoCode = 'NL' THEN 2
# MAGIC         WHEN TRIM(t5.IncorporationNumber) != TRIM(t1.`NationalID-value`)
# MAGIC              AND t1.`NationalID-type` IN ('Kamer van Koophandel', 'Kamer van Koophandel nummer', 'Chamber of Commerce Registration')
# MAGIC              AND TRIM(t5.IncorporationNumber) != ''
# MAGIC              AND TRIM(t1.`NationalID-value`) != ''
# MAGIC              AND t1.gcdsRegisteredCountry IN ('Netherlands (the)', 'Netherlands')
# MAGIC              AND t5.RegisteredCountryIsoCode = 'NL' THEN 0
# MAGIC         ELSE 3
# MAGIC     END AS gcobGcdsDutchIncorporationNumberMatch,
# MAGIC
# MAGIC     -- 6. GCOB vs GCDS (Non-Dutch)
# MAGIC     CASE
# MAGIC         WHEN TRIM(t5.IncorporationNumber) = TRIM(t1.`NationalID-value`)
# MAGIC              --AND t1.`NationalID-type` IN ('Kamer van Koophandel', 'Kamer van Koophandel nummer', 'Chamber of Commerce Registration')
# MAGIC             --  AND t1.gcdsRegisteredCountry NOT IN ('Netherlands (the)', 'Netherlands')
# MAGIC             --  AND t5.RegisteredCountryIsoCode <> 'NL'
# MAGIC              THEN 1
# MAGIC         WHEN ( (TRIM(t5.IncorporationNumber) IS NULL OR TRIM(t5.IncorporationNumber) = '')
# MAGIC              OR (TRIM(t1.`NationalID-value`) IS NULL OR TRIM(t1.`NationalID-value`) = '') )
# MAGIC             --  AND t1.gcdsRegisteredCountry NOT IN ('Netherlands (the)', 'Netherlands')
# MAGIC             --  AND t5.RegisteredCountryIsoCode <> 'NL'
# MAGIC              THEN 2
# MAGIC         WHEN TRIM(t5.IncorporationNumber) != TRIM(t1.`NationalID-value`)
# MAGIC              --AND t1.`NationalID-type` IN ('Kamer van Koophandel', 'Kamer van Koophandel nummer', 'Chamber of Commerce Registration')
# MAGIC              AND TRIM(t5.IncorporationNumber) != ''
# MAGIC              AND TRIM(t1.`NationalID-value`) != ''
# MAGIC             --  AND t1.gcdsRegisteredCountry NOT IN ('Netherlands (the)', 'Netherlands')
# MAGIC             --  AND t5.RegisteredCountryIsoCode <> 'NL'
# MAGIC              THEN 0
# MAGIC         ELSE 3
# MAGIC     END AS gcobGcdsNonDutchIncorporationNumberMatch
# MAGIC
# MAGIC
# MAGIC   -- Siebel and GCOB address match
# MAGIC   , CASE
# MAGIC       WHEN LOWER(TRIM(t4.siebelAddress)) = LOWER(TRIM(t5.gcobAddress)) THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(
# MAGIC           REGEXP_REPLACE(
# MAGIC             TRANSLATE(t4.siebelAddress, 
# MAGIC               'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC               'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC             ), 
# MAGIC             '[^a-zA-Z0-9]', ''
# MAGIC           )
# MAGIC         )) = LOWER(TRIM(
# MAGIC           REGEXP_REPLACE(
# MAGIC             TRANSLATE(t5.gcobAddress, 
# MAGIC               'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC               'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC             ), 
# MAGIC             '[^a-zA-Z0-9]', ''
# MAGIC           )
# MAGIC         ))
# MAGIC       THEN 3 -- special characters removed, string match
# MAGIC       WHEN t4.siebelAddress IS NULL OR t5.gcobAddress IS NULL OR TRIM(t4.siebelAddress) = '' OR TRIM(t5.gcobAddress) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS siebelGcobAddressMatch
# MAGIC
# MAGIC
# MAGIC   -- GCOB and GCDS address match
# MAGIC   , CASE
# MAGIC       WHEN LOWER(TRIM(t1.gcdsAddress)) = LOWER(TRIM(t5.gcobAddress)) THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(
# MAGIC           REGEXP_REPLACE(
# MAGIC             TRANSLATE(t1.gcdsAddress, 
# MAGIC               'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC               'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC             ), 
# MAGIC             '[^a-zA-Z0-9]', ''
# MAGIC           )
# MAGIC         )) = LOWER(TRIM(
# MAGIC           REGEXP_REPLACE(
# MAGIC             TRANSLATE(t5.gcobAddress, 
# MAGIC               'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC               'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC             ), 
# MAGIC             '[^a-zA-Z0-9]', ''
# MAGIC           )
# MAGIC         )) THEN 3 -- special characters removed, string match
# MAGIC       WHEN t1.gcdsAddress IS NULL OR t5.gcobAddress IS NULL OR TRIM(t1.gcdsAddress) = '' OR TRIM(t5.gcobAddress) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS gcdsGcobAddressMatch
# MAGIC
# MAGIC
# MAGIC   -- GCDS and Siebel Address match
# MAGIC   , CASE
# MAGIC       WHEN LOWER(TRIM(t1.gcdsAddress)) = LOWER(TRIM(t4.siebelAddress)) THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t1.gcdsAddress, 
# MAGIC             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ), 
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) = LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t4.siebelAddress, 
# MAGIC             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ), 
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) THEN 3 -- special characters removed, string match
# MAGIC       WHEN t1.gcdsAddress IS NULL OR t4.siebelAddress IS NULL OR TRIM(t1.gcdsAddress) = '' OR TRIM(t4.siebelAddress) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS siebelGcdsAddressMatch
# MAGIC
# MAGIC   -- GCOB and GCDS Steet Name match
# MAGIC
# MAGIC   , CASE
# MAGIC     WHEN LOWER(TRIM(t1.gcdsStreetName)) = LOWER(TRIM(t5.gcobSteetName)) THEN 1 -- string match
# MAGIC     WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t1.gcdsStreetName,
# MAGIC              'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ',
# MAGIC              'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ),
# MAGIC            '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) = LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t5.gcobSteetName,
# MAGIC              'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ',
# MAGIC              'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ),
# MAGIC            '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) THEN 3 -- special characters removed, string match
# MAGIC     WHEN t1.gcdsStreetName IS NULL OR t5.gcobSteetName IS NULL OR TRIM(t1.gcdsStreetName) = '' OR TRIM(t5.gcobSteetName) = '' THEN 2 -- empty or null
# MAGIC     ELSE 0 -- no match
# MAGIC   END AS gcdsGcobStreetNameMatch
# MAGIC
# MAGIC   -- GCDS and SIEBEL Steet Name match
# MAGIC   , CASE
# MAGIC     WHEN LOWER(TRIM(t1.gcdsStreetName)) = LOWER(TRIM(t4.siebelAddressStreetName)) THEN 1 -- string match
# MAGIC     WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t1.gcdsStreetName,
# MAGIC              'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ',
# MAGIC              'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ),
# MAGIC            '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) = LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t4.siebelAddressStreetName,
# MAGIC              'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ',
# MAGIC              'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ),
# MAGIC            '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) THEN 3 -- special characters removed, string match
# MAGIC     WHEN t1.gcdsStreetName IS NULL OR t4.siebelAddressStreetName IS NULL OR TRIM(t1.gcdsStreetName) = '' OR TRIM(t4.siebelAddressStreetName) = '' THEN 2 -- empty or null
# MAGIC     ELSE 0 -- no match
# MAGIC   END AS gcdsSiebelStreetNameMatch
# MAGIC
# MAGIC   -- GCOB and SIEBEL Steet Name match
# MAGIC   , CASE
# MAGIC     WHEN LOWER(TRIM(t5.gcobSteetName)) = LOWER(TRIM(t4.siebelAddressStreetName)) THEN 1 -- string match
# MAGIC     WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t5.gcobSteetName,
# MAGIC              'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ',
# MAGIC              'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ),
# MAGIC            '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) = LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t4.siebelAddressStreetName,
# MAGIC              'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ',
# MAGIC              'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ),
# MAGIC            '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) THEN 3 -- special characters removed, string match
# MAGIC     WHEN t5.gcobSteetName IS NULL OR t4.siebelAddressStreetName IS NULL OR TRIM(t5.gcobSteetName) = '' OR TRIM(t4.siebelAddressStreetName) = '' THEN 2 -- empty or null
# MAGIC     ELSE 0 -- no match
# MAGIC   END AS gcobSiebelStreetNameMatch
# MAGIC
# MAGIC   -- GCOB and GCDS City and Region match
# MAGIC   , CASE
# MAGIC       WHEN LOWER(TRIM(t1.gcdsCityRegion)) = LOWER(TRIM(gcobCityRegion)) THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t1.gcdsCityRegion, 
# MAGIC             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ), 
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) = LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(gcobCityRegion, 
# MAGIC             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ), 
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) THEN 3 -- special characters removed, string match
# MAGIC       WHEN t1.gcdsCityRegion IS NULL OR gcobCityRegion IS NULL OR TRIM(t1.gcdsCityRegion) = '' OR TRIM(gcobCityRegion) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS gcdsGcobCityRegionMatch
# MAGIC
# MAGIC
# MAGIC   -- GCOB & GCDS Country of registration
# MAGIC   , CASE
# MAGIC       WHEN LOWER(TRIM(t5.RegisteredCountryIsoCode)) = LOWER(TRIM(t1.Principal_address_country_code)) THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t5.RegisteredCountryIsoCode, 
# MAGIC             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ), 
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) = LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t1.Principal_address_country_code, 
# MAGIC             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ), 
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) THEN 3 -- special characters removed, string match
# MAGIC      WHEN t5.RegisteredCountryIsoCode IS NULL OR t1.Principal_address_country_code IS NULL OR TRIM(t5.RegisteredCountryIsoCode) = '' OR TRIM(t1.Principal_address_country_code) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS gcobGcdsCountryOfRegistrationMatch
# MAGIC
# MAGIC
# MAGIC   -- GCOB & GCDS GCO
# MAGIC , CASE 
# MAGIC   WHEN LOWER(TRIM(t5.GlobalClientOwner)) = LOWER(TRIM(t1.gcdsGlobalCOname)) THEN 1 -- exact string match
# MAGIC   WHEN 
# MAGIC   -- Extract last name (before comma) and first name (inside parentheses) from one field
# MAGIC   -- Extract first and last name (separated by space) from the other field
# MAGIC   -- Compare if they match
# MAGIC   (
# MAGIC     -- Case 1: t5.GlobalClientOwner has comma format, t1.gcdsGlobalCOname has space format
# MAGIC     (
# MAGIC       CHARINDEX(',', t5.GlobalClientOwner) > 0 
# MAGIC       AND CHARINDEX('(', t5.GlobalClientOwner) > 0
# MAGIC       AND CHARINDEX(' ', t1.gcdsGlobalCOname) > 0
# MAGIC       AND
# MAGIC       UPPER(TRIM(SUBSTRING(t5.GlobalClientOwner, 1, CHARINDEX(',', t5.GlobalClientOwner) - 1))) = 
# MAGIC       UPPER(TRIM(SUBSTRING(t1.gcdsGlobalCOname, CHARINDEX(' ', t1.gcdsGlobalCOname) + 1, LEN(t1.gcdsGlobalCOname))))
# MAGIC       AND
# MAGIC       UPPER(TRIM(SUBSTRING(t5.GlobalClientOwner, CHARINDEX('(', t5.GlobalClientOwner) + 1, CHARINDEX(')', t5.GlobalClientOwner) - CHARINDEX('(', t5.GlobalClientOwner) - 1))) = 
# MAGIC       UPPER(TRIM(SUBSTRING(t1.gcdsGlobalCOname, 1, CHARINDEX(' ', t1.gcdsGlobalCOname) - 1)))
# MAGIC     )
# MAGIC     OR
# MAGIC     -- Case 2: t1.gcdsGlobalCOname has comma format, t5.GlobalClientOwner has space format
# MAGIC     (
# MAGIC       CHARINDEX(',', t1.gcdsGlobalCOname) > 0 
# MAGIC       AND CHARINDEX('(', t1.gcdsGlobalCOname) > 0
# MAGIC       AND CHARINDEX(' ', t5.GlobalClientOwner) > 0
# MAGIC       AND
# MAGIC       UPPER(TRIM(SUBSTRING(t1.gcdsGlobalCOname, 1, CHARINDEX(',', t1.gcdsGlobalCOname) - 1))) = 
# MAGIC       UPPER(TRIM(SUBSTRING(t5.GlobalClientOwner, CHARINDEX(' ', t5.GlobalClientOwner) + 1, LEN(t5.GlobalClientOwner))))
# MAGIC       AND
# MAGIC       UPPER(TRIM(SUBSTRING(t1.gcdsGlobalCOname, CHARINDEX('(', t1.gcdsGlobalCOname) + 1, CHARINDEX(')', t1.gcdsGlobalCOname) - CHARINDEX('(', t1.gcdsGlobalCOname) - 1))) = 
# MAGIC       UPPER(TRIM(SUBSTRING(t5.GlobalClientOwner, 1, CHARINDEX(' ', t5.GlobalClientOwner) - 1)))
# MAGIC     )
# MAGIC   )
# MAGIC THEN 4 -- advanced name match
# MAGIC
# MAGIC   
# MAGIC   WHEN t5.GlobalClientOwner IS NULL OR t1.gcdsGlobalCOname IS NULL OR TRIM(t5.GlobalClientOwner) = '' OR TRIM(t1.gcdsGlobalCOname) = '' THEN 2 -- empty or null
# MAGIC   
# MAGIC   ELSE 0 -- no match
# MAGIC END AS gcobGcdsGlobalClientOwnerMatch
# MAGIC
# MAGIC -- GCOB & GCDS GCO location
# MAGIC   , CASE 
# MAGIC     -- Custom mappings first
# MAGIC     WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) IN ('RABOBANK NETHERLANDS', 'RABOBANK FOUNDATION')
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'UTRECHT'
# MAGIC     THEN 1 
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK ANTWERP' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'BELGIUM' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK ARGENTINA' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'ARGENTINA' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) IN ('RABOBANK AUSTRALIA', 'RABOBANK - RANZ COUNTRY BANKING AND ROS') 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'AUSTRALIA' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK BRAZIL' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'BRAZIL' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) IN ('RABOBANK CANADA (RCBR)', 'RABOBANK CANADA(RURAL)') 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'CANADA' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK CHILE' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'CHILE' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK CHINA' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'SHANGHAI' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK DUBLIN' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'IRELAND' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK FRANKFURT' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'GERMANY' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK HONG KONG' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'HONG KONG' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK KENYA' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'KENYA' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK LONDON' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'LONDON' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK MADRID' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'SPAIN' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK MILAN' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'ITALY' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK PARIS' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'FRANCE' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK SINGAPORE' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'SINGAPORE' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK NEW ZEALAND' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'NEW ZEALAND' THEN 1
# MAGIC      
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) IN ('RABOBANK NEW YORK', 'RABO SECURITIES USA, INC. (RSEC)')
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) IN ('NEW YORK', 'CHICAGO', 'ATLANTA') THEN 1
# MAGIC      
# MAGIC WHEN UPPER((IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK - USA RABO AGRIFINANCE' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'RAF' THEN 1
# MAGIC
# MAGIC WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK TURKEY' 
# MAGIC      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'TURKEY' THEN 1
# MAGIC      
# MAGIC WHEN t5.GlobalClientOwnerLocation IS NULL 
# MAGIC      OR t1.gcdsGlobalCOlocation IS NULL 
# MAGIC      OR TRIM(IFNULL(t5.GlobalClientOwnerLocation, '')) = '' 
# MAGIC      OR TRIM(IFNULL(t1.gcdsGlobalCOlocation, '')) = '' THEN 2 -- empty or null
# MAGIC      
# MAGIC ELSE 0 -- no match
# MAGIC END AS gcobGcdsGlobalClientOwnerLocationMatch
# MAGIC
# MAGIC
# MAGIC   -- GCOB & GCDS Next Review Date
# MAGIC   , CASE
# MAGIC       WHEN t5.nextreviewdate = t1.gcdsCDDNextReviewdate THEN 1 
# MAGIC       WHEN t5.nextreviewdate IS NULL OR t1.gcdsCDDNextReviewdate IS NULL OR TRIM(t5.nextreviewdate) = '' OR TRIM(t1.gcdsCDDNextReviewdate) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 
# MAGIC     END AS gcobGcdsNextReviewDateMatch
# MAGIC
# MAGIC
# MAGIC   -- GCOB & GCDS Risk Level
# MAGIC   , CASE
# MAGIC       WHEN t5.ValidatedRiskLevel = t1.gcdsCDDRating THEN 1
# MAGIC       WHEN t5.ValidatedRiskLevel IS NULL OR t1.gcdsCDDRating IS NULL OR TRIM(t5.ValidatedRiskLevel) = '' OR TRIM(t1.gcdsCDDRating) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 
# MAGIC     END AS gcobGcdsValidatedRiskLevelMatch
# MAGIC   
# MAGIC   -- GCOB AND SIEBEL Email address
# MAGIC   , CASE
# MAGIC       WHEN TRIM(t5.ContactPersonEmailAddress) = TRIM(t6.siebelEmailAddress) THEN 1
# MAGIC       WHEN  TRIM(t5.ContactPersonEmailAddress) IS NULL OR TRIM(t6.siebelEmailAddress) IS NULL OR TRIM(t5.ContactPersonEmailAddress) = '' OR TRIM(t6.siebelEmailAddress) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 
# MAGIC     END AS gcobSiebelEmailAddressMatch
# MAGIC
# MAGIC   -- GCOB & GCDS Business Line
# MAGIC
# MAGIC FROM gcdsRawData t1
# MAGIC FULL OUTER JOIN siebelClientInformation t2 ON t1.siebelID = t2.siebelID
# MAGIC FULL OUTER JOIN siebelProductInformation t3 ON t1.siebelID = t3.siebelID
# MAGIC FULL OUTER JOIN siebelAddressInformation t4 ON t1.siebelID = t4.siebelID
# MAGIC FULL OUTER JOIN siebelEmailAddressInformation t6 on t1.siebelID = t6.siebelID
# MAGIC FULL OUTER JOIN sggGcobData t5 ON t1.gcobid = t5.UniqueGcobId

# COMMAND ----------

# DBTITLE 1,Client Data Comparison old code
# --THE LOGIC FOR INCORPORATION AND KVK MISMATCH LOGIC IS CHANGED, THE NEW LOGIC IS IN PRODUCTION (ABOVE CODE BLOCK). IF ERRORS ARAISE, PLEASE REVERT BACK TO THIS CODE.

# /*
# CREATE OR REPLACE TEMPORARY VIEW siebelGcobGcdsClientComparisonTemp AS

# SELECT DISTINCT  
# CASE
#     WHEN t2.siebelID IS NOT NULL AND t2.siebelID <> '' 
#     THEN t2.siebelID || '_' 
#     ELSE '' 
# END ||
# COALESCE(t5.UniqueGcobId, '') ||
# CASE 
#     WHEN t1.gcid IS NOT NULL AND t1.gcid <> '' 
#     THEN '_' || t1.gcid 
#     ELSE '' 
# END AS sggUniqueIdentifier


#   , t1.gcid 
#   , t2.siebelID
#   , t5.UniqueGcobId
#   , t1.gcdsPartyRole


#   -- NAME COMPARISON
#   , CASE
#       WHEN LOWER(TRIM(t1.gcdsClientName)) = LOWER(TRIM(t5.FullLegalName)) THEN 1 -- string match
#       WHEN LOWER(TRIM(
#         REGEXP_REPLACE(
#           REGEXP_REPLACE(
#             REGEXP_REPLACE(
#               REGEXP_REPLACE(
#                 REGEXP_REPLACE(
#                   TRANSLATE(t1.gcdsClientName, 
#                     'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#                     'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#                   ), 
#                   'DLL', 'De Lage Landen'
#                 ), 
#                 'DAC', 'Designated Activity Company'
#               ), 
#               '(?i)\\bLtd\\.?', 'Limited'
#             ), 
#             '(?i)\\bLtda\\.?', 'Limitada'
#           ),
#           '[^a-zA-Z0-9]', ''
#         )
#       )) = LOWER(TRIM(
#         REGEXP_REPLACE(
#           REGEXP_REPLACE(
#             REGEXP_REPLACE(
#               REGEXP_REPLACE(
#                 REGEXP_REPLACE(
#                   TRANSLATE(t5.FullLegalName, 
#                     'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#                     'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#                   ), 
#                   'DLL', 'De Lage Landen'
#                 ), 
#                 'DAC', 'Designated Activity Company'
#               ), 
#               '(?i)\\bLtd\\.?', 'Limited'
#             ), 
#             '(?i)\\bLtda\\.?', 'Limitada'
#           ),
#           '[^a-zA-Z0-9]', ''
#         )
#       )) THEN 3 -- special characters removed, string match
#       WHEN t1.gcdsClientName IS NULL OR t5.FullLegalName IS NULL OR TRIM(t1.gcdsClientName) = '' OR TRIM(t5.FullLegalName) = '' THEN 2 -- empty or null
#       ELSE 0 -- no match
#     END AS gcobGcdsClientNameMatch


#   , CASE
#       WHEN LOWER(TRIM(t2.siebelClientName)) = LOWER(TRIM(t5.FullLegalName)) THEN 1 -- string match
#       WHEN LOWER(TRIM(
#         REGEXP_REPLACE(
#           REGEXP_REPLACE(
#             REGEXP_REPLACE(
#               REGEXP_REPLACE(
#                 REGEXP_REPLACE(
#                   TRANSLATE(t2.siebelClientName, 
#                     'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#                     'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#                   ), 
#                   'DLL', 'De Lage Landen'
#                 ), 
#                 'DAC', 'Designated Activity Company'
#               ), 
#               '(?i)\\bLtd\\.?', 'Limited'
#             ), 
#             '(?i)\\bLtda\\.?', 'Limitada'
#           ),
#           '[^a-zA-Z0-9]', ''
#         )
#       )) = LOWER(TRIM(
#         REGEXP_REPLACE(
#           REGEXP_REPLACE(
#             REGEXP_REPLACE(
#               REGEXP_REPLACE(
#                 REGEXP_REPLACE(
#                   TRANSLATE(t5.FullLegalName, 
#                     'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#                     'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#                   ), 
#                   'DLL', 'De Lage Landen'
#                 ), 
#                 'DAC', 'Designated Activity Company'
#               ), 
#               '(?i)\\bLtd\\.?', 'Limited'
#             ), 
#             '(?i)\\bLtda\\.?', 'Limitada'
#           ),
#           '[^a-zA-Z0-9]', ''
#         )
#       )) THEN 3 -- special characters removed, string match
#      WHEN t2.siebelClientName IS NULL OR t5.FullLegalName IS NULL OR TRIM(t2.siebelClientName) = '' OR TRIM(t5.FullLegalName) = '' THEN 2 -- empty or null
#       ELSE 0 -- no match
#     END AS gcobSiebelClientNameMatch


#   , CASE
#       WHEN LOWER(TRIM(t2.siebelClientName)) = LOWER(TRIM(t1.gcdsClientName)) THEN 1 -- string match
#       WHEN LOWER(TRIM(
#         REGEXP_REPLACE(
#           REGEXP_REPLACE(
#             REGEXP_REPLACE(
#               REGEXP_REPLACE(
#                 REGEXP_REPLACE(
#                   TRANSLATE(t2.siebelClientName, 
#                     'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#                     'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#                   ), 
#                   'DLL', 'De Lage Landen'
#                 ), 
#                 'DAC', 'Designated Activity Company'
#               ), 
#               '(?i)\\bLtd\\.?', 'Limited'
#             ), 
#             '(?i)\\bLtda\\.?', 'Limitada'
#           ),
#           '[^a-zA-Z0-9]', ''
#         )
#       )) = LOWER(TRIM(
#         REGEXP_REPLACE(
#           REGEXP_REPLACE(
#             REGEXP_REPLACE(
#               REGEXP_REPLACE(
#                 REGEXP_REPLACE(
#                   TRANSLATE(t1.gcdsClientName, 
#                     'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#                     'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#                   ), 
#                   'DLL', 'De Lage Landen'
#                 ), 
#                 'DAC', 'Designated Activity Company'
#               ), 
#               '(?i)\\bLtd\\.?', 'Limited'
#             ), 
#             '(?i)\\bLtda\\.?', 'Limitada'
#           ),
#           '[^a-zA-Z0-9]', ''
#         )
#       )) THEN 3 -- special characters removed, string match
#       WHEN t2.siebelClientName IS NULL OR t1.gcdsClientName IS NULL OR TRIM(t2.siebelClientName) = '' OR TRIM(t1.gcdsClientName) = '' THEN 2 -- empty or null
#       ELSE 0 -- no match
#     END AS gcdsSiebelClientNameMatch


#   -- Client lifecycle status comparison
#   , CASE 
#       WHEN LOWER(TRIM(t1.gcdsClientlifecyclestatus)) = 'client' AND LOWER(TRIM(t5.clientlifecyclename)) = 'client' THEN 1  -- string match
#       WHEN LOWER(TRIM(t1.gcdsClientlifecyclestatus)) = 'former client' AND LOWER(TRIM(t5.clientlifecyclename)) = 'formerclient' THEN 1 -- string match
#       WHEN LOWER(TRIM(t1.gcdsClientlifecyclestatus)) = 'prospect' AND LOWER(TRIM(t5.clientlifecyclename)) = 'prospect' THEN 1 -- string match
#       WHEN t1.gcdsClientlifecyclestatus IS NULL OR t5.clientlifecyclename IS NULL OR TRIM(t1.gcdsClientlifecyclestatus) = '' OR TRIM(t5.clientlifecyclename) = '' THEN 2 -- empty or null
#       ELSE 0 -- no match
#     END AS gcobGcdsClientLifeCycleStatusMatch

#   , CASE 
#       WHEN LOWER(TRIM(t2.siebelClientLifeCycleName)) = 'klant' AND LOWER(TRIM(t5.clientlifecyclename)) = 'client' THEN 1 
#       WHEN LOWER(TRIM(t2.siebelClientLifeCycleName)) = 'ex-klant' AND LOWER(TRIM(t5.clientlifecyclename)) = 'formerclient' THEN 1 
#       WHEN LOWER(TRIM(t2.siebelClientLifeCycleName)) = 'prospect' AND LOWER(TRIM(t5.clientlifecyclename)) = 'prospect' THEN 1
#       WHEN ((t2.siebelClientLifeCycleName IS NULL) OR (t5.clientlifecyclename IS NULL)) OR (TRIM(t2.siebelClientLifeCycleName) = '' OR TRIM(t5.clientlifecyclename) = '') THEN 2 -- empty or null
#       ELSE 0 
#     END AS gcobSiebelClientLifeCycleStatusMatch

#   , CASE 
#       WHEN LOWER(TRIM(t2.siebelClientLifeCycleName)) = 'klant' AND LOWER(TRIM(t1.gcdsClientlifecyclestatus)) = 'client' THEN 1 
#       WHEN LOWER(TRIM(t2.siebelClientLifeCycleName)) = 'ex-klant' AND LOWER(TRIM(t1.gcdsClientlifecyclestatus)) = 'former client' THEN 1 
#       WHEN LOWER(TRIM(t2.siebelClientLifeCycleName)) = 'prospect' AND LOWER(TRIM(t1.gcdsClientlifecyclestatus)) = 'prospect' THEN 1
#      WHEN t2.siebelClientLifeCycleName IS NULL OR t1.gcdsClientlifecyclestatus IS NULL OR TRIM(t2.siebelClientLifeCycleName) = '' OR TRIM(t1.gcdsClientlifecyclestatus) = '' THEN 2 -- empty or null
#       ELSE 0 
#     END AS gcdsSiebelClientLifeCycleStatusMatch


#   -- -- KVK Number and Gcob incorporation number comparison
#   -- , IFF((TRIM(t2.siebelKvkNumber) = TRIM(t5.IncorporationNumber)) OR (TRIM(t2.siebelKvkNumber) = '' AND TRIM(t5.RegisteredCountryIsoCode) != 'NL'), 1, 0) AS siebelGcobIncorporationNumberMatch
#     ,CASE
#         WHEN 
#             TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) = TRIM(t5.IncorporationNumber)
#             AND COALESCE(t2.entitytype, '') = 'Dutch Entity'
#             AND COALESCE(t5.RegisteredCountryIsoCode, '') = 'NL'
#         THEN 1

#         WHEN 
#             (
#                 TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) IS NULL
#                 OR TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) = ''
#                 OR TRIM(t5.IncorporationNumber) IS NULL
#                 OR TRIM(t5.IncorporationNumber) = ''
#             )
#             AND COALESCE(t2.entitytype, '') = 'Dutch Entity'
#             AND COALESCE(t5.RegisteredCountryIsoCode, '') = 'NL'
#         THEN 2

#         ELSE 0
#     END AS siebelGcobDutchIncorporationNumberMatch

#     -- 2. Siebel vs GCOB (Non-Dutch)
#     ,CASE
#         WHEN 
#             TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) = TRIM(t5.IncorporationNumber)
#             AND COALESCE(t2.entitytype, '') <> 'Dutch Entity'
#             AND COALESCE(t5.RegisteredCountryIsoCode, '') <> 'NL'
#         THEN 1

#         WHEN 
#             (
#                 TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) IS NULL
#                 OR TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) = ''
#                 OR TRIM(t5.IncorporationNumber) IS NULL
#                 OR TRIM(t5.IncorporationNumber) = ''
#             )
#             AND COALESCE(t2.entitytype, '') <> 'Dutch Entity'
#             AND COALESCE(t5.RegisteredCountryIsoCode, '') <> 'NL'
#         THEN 2

#         ELSE 0
#     END AS siebelGcobNonDutchIncorporationNumberMatch

#     -- 3. Siebel vs GCDS (Dutch)
#     , CASE
#       --1: Exact match on KVK vs NationalID
#       WHEN 
#           TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) =
#           TRIM(COALESCE(t1.`NationalID-value`, ''))
#           AND t1.`NationalID-type` IN (
#               'Kamer van Koophandel',
#               'Kamer van Koophandel nummer',
#               'Chamber of Commerce Registration'
#           )
#           AND COALESCE(t2.entitytype, '') = 'Dutch Entity'
#           AND COALESCE(t1.gcdsRegisteredCountry, '') IN ('Netherlands (the)', 'Netherlands')
#       THEN 1

#       --2: Missing KVK or missing NationalID  both considered incomplete
#       WHEN 
#           (
#               TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) = ''
#               OR TRIM(COALESCE(t1.`NationalID-value`, '')) = ''
#           )
#           AND COALESCE(t2.entitytype, '') = 'Dutch Entity'
#           AND COALESCE(t1.gcdsRegisteredCountry, '') IN ('Netherlands (the)', 'Netherlands')
#       THEN 2

#       --3: Both present but NOT equal  mismatch
#       WHEN 
#           TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) <> ''
#           AND TRIM(COALESCE(t1.`NationalID-value`, '')) <> ''
#           AND TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) <>
#               TRIM(COALESCE(t1.`NationalID-value`, ''))
#           AND t1.`NationalID-type` IN (
#               'Kamer van Koophandel',
#               'Kamer van Koophandel nummer',
#               'Chamber of Commerce Registration'
#           )
#           AND COALESCE(t2.entitytype, '') = 'Dutch Entity'
#           AND COALESCE(t1.gcdsRegisteredCountry, '') IN ('Netherlands (the)', 'Netherlands')
#       THEN 0

#       --Anything else (e.g., NationalID-type not KVK)
#       ELSE 3
#   END AS siebelGcdsDutchIncorporationNumberMatch

#     -- 4. Siebel vs GCDS (Non-Dutch)
#   , CASE
#       --Exact match (Non‑Dutch + both values present)
#       WHEN 
#           TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) =
#           TRIM(COALESCE(t1.`NationalID-value`, ''))
#           AND COALESCE(t2.entitytype, '') <> 'Dutch Entity'
#           AND COALESCE(t1.gcdsRegisteredCountry, '') NOT IN ('Netherlands (the)', 'Netherlands')
#       THEN 1

#       --Missing value on either side  incomplete
#       WHEN
#           TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) = ''
#           OR TRIM(COALESCE(t1.`NationalID-value`, '')) = ''
#       THEN 2

#       --Both present but not equal  mismatch
#       WHEN 
#           TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) <> ''
#           AND TRIM(COALESCE(t1.`NationalID-value`, '')) <> ''
#           AND TRIM(COALESCE(t2.siebelKvkNumber, t2.siebelKVKForiegnEntity)) <>
#               TRIM(COALESCE(t1.`NationalID-value`, ''))
#           AND COALESCE(t2.entitytype, '') <> 'Dutch Entity'
#           AND COALESCE(t1.gcdsRegisteredCountry, '') NOT IN ('Netherlands (the)', 'Netherlands')
#       THEN 0

#       --All other cases (e.g. NationalID-type not relevant, or Dutch)
#       ELSE 3
#   END AS siebelGcdsNonDutchIncorporationNumberMatch

#     -- 5. GCOB vs GCDS (Dutch)
#   ,CASE
#       --Exact match — Dutch entity + correct NationalID type + Dutch country
#       WHEN 
#           TRIM(COALESCE(t5.IncorporationNumber, '')) =
#           TRIM(COALESCE(t1.`NationalID-value`, ''))
#           AND t1.`NationalID-type` IN (
#               'Kamer van Koophandel',
#               'Kamer van Koophandel nummer',
#               'Chamber of Commerce Registration'
#           )
#           AND COALESCE(t1.gcdsRegisteredCountry, '') IN ('Netherlands (the)', 'Netherlands')
#           AND COALESCE(t5.RegisteredCountryIsoCode, '') = 'NL'
#       THEN 1

#       --Missing either IncorporationNumber or NationalID-value  incomplete
#       WHEN 
#           TRIM(COALESCE(t5.IncorporationNumber, '')) = ''
#           OR TRIM(COALESCE(t1.`NationalID-value`, '')) = ''
#           AND COALESCE(t1.gcdsRegisteredCountry, '') IN ('Netherlands (the)', 'Netherlands')
#           AND COALESCE(t5.RegisteredCountryIsoCode, '') = 'NL'
#       THEN 2

#       --Both present but not equal  mismatch
#       WHEN 
#           TRIM(COALESCE(t5.IncorporationNumber, '')) <> ''
#           AND TRIM(COALESCE(t1.`NationalID-value`, '')) <> ''
#           AND TRIM(COALESCE(t5.IncorporationNumber, '')) <>
#               TRIM(COALESCE(t1.`NationalID-value`, ''))
#           AND t1.`NationalID-type` IN (
#               'Kamer van Koophandel',
#               'Kamer van Koophandel nummer',
#               'Chamber of Commerce Registration'
#           )
#           AND COALESCE(t1.gcdsRegisteredCountry, '') IN ('Netherlands (the)', 'Netherlands')
#           AND COALESCE(t5.RegisteredCountryIsoCode, '') = 'NL'
#       THEN 0

#       --Everything else (e.g., wrong NationalID-type)
#       ELSE 3
#   END AS gcobGcdsDutchIncorporationNumberMatch
#     -- 6. GCOB vs GCDS (Non-Dutch)
#   ,CASE
#       --Exact match (Non‑Dutch + both numbers match)
#       WHEN 
#           TRIM(COALESCE(t5.IncorporationNumber, '')) =
#           TRIM(COALESCE(t1.`NationalID-value`, ''))
#           AND COALESCE(t1.gcdsRegisteredCountry, '') NOT IN ('Netherlands (the)', 'Netherlands')
#           AND COALESCE(t5.RegisteredCountryIsoCode, '') <> 'NL'
#       THEN 1

#       --Missing either incorporation number or national ID  incomplete
#       WHEN 
#           TRIM(COALESCE(t5.IncorporationNumber, '')) = ''
#           OR TRIM(COALESCE(t1.`NationalID-value`, '')) = ''
#           AND COALESCE(t1.gcdsRegisteredCountry, '') NOT IN ('Netherlands (the)', 'Netherlands')
#           AND COALESCE(t5.RegisteredCountryIsoCode, '') <> 'NL'
#       THEN 2

#       --Both present but mismatch
#       WHEN 
#           TRIM(COALESCE(t5.IncorporationNumber, '')) <> ''
#           AND TRIM(COALESCE(t1.`NationalID-value`, '')) <> ''
#           AND TRIM(COALESCE(t5.IncorporationNumber, '')) <>
#               TRIM(COALESCE(t1.`NationalID-value`, ''))
#           AND COALESCE(t1.gcdsRegisteredCountry, '') NOT IN ('Netherlands (the)', 'Netherlands')
#           AND COALESCE(t5.RegisteredCountryIsoCode, '') <> 'NL'
#       THEN 0

#       --Everything else irrelevant NationalID-type or mismatched regions
#       ELSE 3
#   END AS gcobGcdsNonDutchIncorporationNumberMatch


#   -- Siebel and GCOB address match
#   , CASE
#       WHEN LOWER(TRIM(t4.siebelAddress)) = LOWER(TRIM(t5.gcobAddress)) THEN 1 -- string match
#       WHEN LOWER(TRIM(
#           REGEXP_REPLACE(
#             TRANSLATE(t4.siebelAddress, 
#               'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#               'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#             ), 
#             '[^a-zA-Z0-9]', ''
#           )
#         )) = LOWER(TRIM(
#           REGEXP_REPLACE(
#             TRANSLATE(t5.gcobAddress, 
#               'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#               'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#             ), 
#             '[^a-zA-Z0-9]', ''
#           )
#         ))
#       THEN 3 -- special characters removed, string match
#       WHEN t4.siebelAddress IS NULL OR t5.gcobAddress IS NULL OR TRIM(t4.siebelAddress) = '' OR TRIM(t5.gcobAddress) = '' THEN 2 -- empty or null
#       ELSE 0 -- no match
#     END AS siebelGcobAddressMatch


#   -- GCOB and GCDS address match
#   , CASE
#       WHEN LOWER(TRIM(t1.gcdsAddress)) = LOWER(TRIM(t5.gcobAddress)) THEN 1 -- string match
#       WHEN LOWER(TRIM(
#           REGEXP_REPLACE(
#             TRANSLATE(t1.gcdsAddress, 
#               'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#               'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#             ), 
#             '[^a-zA-Z0-9]', ''
#           )
#         )) = LOWER(TRIM(
#           REGEXP_REPLACE(
#             TRANSLATE(t5.gcobAddress, 
#               'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#               'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#             ), 
#             '[^a-zA-Z0-9]', ''
#           )
#         )) THEN 3 -- special characters removed, string match
#       WHEN t1.gcdsAddress IS NULL OR t5.gcobAddress IS NULL OR TRIM(t1.gcdsAddress) = '' OR TRIM(t5.gcobAddress) = '' THEN 2 -- empty or null
#       ELSE 0 -- no match
#     END AS gcdsGcobAddressMatch


#   -- GCDS and Siebel Address match
#   , CASE
#       WHEN LOWER(TRIM(t1.gcdsAddress)) = LOWER(TRIM(t4.siebelAddress)) THEN 1 -- string match
#       WHEN LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t1.gcdsAddress, 
#             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ), 
#           '[^a-zA-Z0-9]', ''
#         )
#       )) = LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t4.siebelAddress, 
#             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ), 
#           '[^a-zA-Z0-9]', ''
#         )
#       )) THEN 3 -- special characters removed, string match
#       WHEN t1.gcdsAddress IS NULL OR t4.siebelAddress IS NULL OR TRIM(t1.gcdsAddress) = '' OR TRIM(t4.siebelAddress) = '' THEN 2 -- empty or null
#       ELSE 0 -- no match
#     END AS siebelGcdsAddressMatch

#   -- GCOB and GCDS Steet Name match

#   , CASE
#     WHEN LOWER(TRIM(t1.gcdsStreetName)) = LOWER(TRIM(t5.gcobSteetName)) THEN 1 -- string match
#     WHEN LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t1.gcdsStreetName,
#              'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ',
#              'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ),
#            '[^a-zA-Z0-9]', ''
#         )
#       )) = LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t5.gcobSteetName,
#              'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ',
#              'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ),
#            '[^a-zA-Z0-9]', ''
#         )
#       )) THEN 3 -- special characters removed, string match
#     WHEN t1.gcdsStreetName IS NULL OR t5.gcobSteetName IS NULL OR TRIM(t1.gcdsStreetName) = '' OR TRIM(t5.gcobSteetName) = '' THEN 2 -- empty or null
#     ELSE 0 -- no match
#   END AS gcdsGcobStreetNameMatch

#   -- GCDS and SIEBEL Steet Name match
#   , CASE
#     WHEN LOWER(TRIM(t1.gcdsStreetName)) = LOWER(TRIM(t4.siebelAddressStreetName)) THEN 1 -- string match
#     WHEN LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t1.gcdsStreetName,
#              'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ',
#              'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ),
#            '[^a-zA-Z0-9]', ''
#         )
#       )) = LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t4.siebelAddressStreetName,
#              'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ',
#              'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ),
#            '[^a-zA-Z0-9]', ''
#         )
#       )) THEN 3 -- special characters removed, string match
#     WHEN t1.gcdsStreetName IS NULL OR t4.siebelAddressStreetName IS NULL OR TRIM(t1.gcdsStreetName) = '' OR TRIM(t4.siebelAddressStreetName) = '' THEN 2 -- empty or null
#     ELSE 0 -- no match
#   END AS gcdsSiebelStreetNameMatch

#   -- GCOB and SIEBEL Steet Name match
#   , CASE
#     WHEN LOWER(TRIM(t5.gcobSteetName)) = LOWER(TRIM(t4.siebelAddressStreetName)) THEN 1 -- string match
#     WHEN LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t5.gcobSteetName,
#              'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ',
#              'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ),
#            '[^a-zA-Z0-9]', ''
#         )
#       )) = LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t4.siebelAddressStreetName,
#              'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ',
#              'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ),
#            '[^a-zA-Z0-9]', ''
#         )
#       )) THEN 3 -- special characters removed, string match
#     WHEN t5.gcobSteetName IS NULL OR t4.siebelAddressStreetName IS NULL OR TRIM(t5.gcobSteetName) = '' OR TRIM(t4.siebelAddressStreetName) = '' THEN 2 -- empty or null
#     ELSE 0 -- no match
#   END AS gcobSiebelStreetNameMatch

#   -- GCOB and GCDS City and Region match
#   , CASE
#       WHEN LOWER(TRIM(t1.gcdsCityRegion)) = LOWER(TRIM(gcobCityRegion)) THEN 1 -- string match
#       WHEN LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t1.gcdsCityRegion, 
#             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ), 
#           '[^a-zA-Z0-9]', ''
#         )
#       )) = LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(gcobCityRegion, 
#             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ), 
#           '[^a-zA-Z0-9]', ''
#         )
#       )) THEN 3 -- special characters removed, string match
#       WHEN t1.gcdsCityRegion IS NULL OR gcobCityRegion IS NULL OR TRIM(t1.gcdsCityRegion) = '' OR TRIM(gcobCityRegion) = '' THEN 2 -- empty or null
#       ELSE 0 -- no match
#     END AS gcdsGcobCityRegionMatch


#   -- GCOB & GCDS Country of registration
#   , CASE
#       WHEN LOWER(TRIM(t5.RegisteredCountryIsoCode)) = LOWER(TRIM(t1.Principal_address_country_code)) THEN 1 -- string match
#       WHEN LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t5.RegisteredCountryIsoCode, 
#             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ), 
#           '[^a-zA-Z0-9]', ''
#         )
#       )) = LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t1.Principal_address_country_code, 
#             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ), 
#           '[^a-zA-Z0-9]', ''
#         )
#       )) THEN 3 -- special characters removed, string match
#      WHEN t5.RegisteredCountryIsoCode IS NULL OR t1.Principal_address_country_code IS NULL OR TRIM(t5.RegisteredCountryIsoCode) = '' OR TRIM(t1.Principal_address_country_code) = '' THEN 2 -- empty or null
#       ELSE 0 -- no match
#     END AS gcobGcdsCountryOfRegistrationMatch


#   -- GCOB & GCDS GCO
# , CASE 
#   WHEN LOWER(TRIM(t5.GlobalClientOwner)) = LOWER(TRIM(t1.gcdsGlobalCOname)) THEN 1 -- exact string match
#   WHEN 
#   -- Extract last name (before comma) and first name (inside parentheses) from one field
#   -- Extract first and last name (separated by space) from the other field
#   -- Compare if they match
#   (
#     -- Case 1: t5.GlobalClientOwner has comma format, t1.gcdsGlobalCOname has space format
#     (
#       CHARINDEX(',', t5.GlobalClientOwner) > 0 
#       AND CHARINDEX('(', t5.GlobalClientOwner) > 0
#       AND CHARINDEX(' ', t1.gcdsGlobalCOname) > 0
#       AND
#       UPPER(TRIM(SUBSTRING(t5.GlobalClientOwner, 1, CHARINDEX(',', t5.GlobalClientOwner) - 1))) = 
#       UPPER(TRIM(SUBSTRING(t1.gcdsGlobalCOname, CHARINDEX(' ', t1.gcdsGlobalCOname) + 1, LEN(t1.gcdsGlobalCOname))))
#       AND
#       UPPER(TRIM(SUBSTRING(t5.GlobalClientOwner, CHARINDEX('(', t5.GlobalClientOwner) + 1, CHARINDEX(')', t5.GlobalClientOwner) - CHARINDEX('(', t5.GlobalClientOwner) - 1))) = 
#       UPPER(TRIM(SUBSTRING(t1.gcdsGlobalCOname, 1, CHARINDEX(' ', t1.gcdsGlobalCOname) - 1)))
#     )
#     OR
#     -- Case 2: t1.gcdsGlobalCOname has comma format, t5.GlobalClientOwner has space format
#     (
#       CHARINDEX(',', t1.gcdsGlobalCOname) > 0 
#       AND CHARINDEX('(', t1.gcdsGlobalCOname) > 0
#       AND CHARINDEX(' ', t5.GlobalClientOwner) > 0
#       AND
#       UPPER(TRIM(SUBSTRING(t1.gcdsGlobalCOname, 1, CHARINDEX(',', t1.gcdsGlobalCOname) - 1))) = 
#       UPPER(TRIM(SUBSTRING(t5.GlobalClientOwner, CHARINDEX(' ', t5.GlobalClientOwner) + 1, LEN(t5.GlobalClientOwner))))
#       AND
#       UPPER(TRIM(SUBSTRING(t1.gcdsGlobalCOname, CHARINDEX('(', t1.gcdsGlobalCOname) + 1, CHARINDEX(')', t1.gcdsGlobalCOname) - CHARINDEX('(', t1.gcdsGlobalCOname) - 1))) = 
#       UPPER(TRIM(SUBSTRING(t5.GlobalClientOwner, 1, CHARINDEX(' ', t5.GlobalClientOwner) - 1)))
#     )
#   )
# THEN 4 -- advanced name match

  
#   WHEN t5.GlobalClientOwner IS NULL OR t1.gcdsGlobalCOname IS NULL OR TRIM(t5.GlobalClientOwner) = '' OR TRIM(t1.gcdsGlobalCOname) = '' THEN 2 -- empty or null
  
#   ELSE 0 -- no match
# END AS gcobGcdsGlobalClientOwnerMatch

# -- GCOB & GCDS GCO location
#   , CASE 
#     -- Custom mappings first
#     WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) IN ('RABOBANK NETHERLANDS', 'RABOBANK FOUNDATION')
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'UTRECHT'
#     THEN 1 
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK ANTWERP' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'BELGIUM' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK ARGENTINA' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'ARGENTINA' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) IN ('RABOBANK AUSTRALIA', 'RABOBANK - RANZ COUNTRY BANKING AND ROS') 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'AUSTRALIA' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK BRAZIL' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'BRAZIL' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) IN ('RABOBANK CANADA (RCBR)', 'RABOBANK CANADA(RURAL)') 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'CANADA' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK CHILE' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'CHILE' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK CHINA' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'SHANGHAI' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK DUBLIN' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'IRELAND' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK FRANKFURT' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'GERMANY' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK HONG KONG' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'HONG KONG' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK KENYA' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'KENYA' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK LONDON' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'LONDON' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK MADRID' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'SPAIN' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK MILAN' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'ITALY' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK PARIS' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'FRANCE' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK SINGAPORE' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'SINGAPORE' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK NEW ZEALAND' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'NEW ZEALAND' THEN 1
     
# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) IN ('RABOBANK NEW YORK', 'RABO SECURITIES USA, INC. (RSEC)')
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) IN ('NEW YORK', 'CHICAGO', 'ATLANTA') THEN 1
     
# WHEN UPPER((IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK - USA RABO AGRIFINANCE' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'RAF' THEN 1

# WHEN UPPER(TRIM(IFNULL(t5.GlobalClientOwnerLocation, ''))) = 'RABOBANK TURKEY' 
#      AND UPPER(TRIM(IFNULL(t1.gcdsGlobalCOlocation, ''))) = 'TURKEY' THEN 1
     
# WHEN t5.GlobalClientOwnerLocation IS NULL 
#      OR t1.gcdsGlobalCOlocation IS NULL 
#      OR TRIM(IFNULL(t5.GlobalClientOwnerLocation, '')) = '' 
#      OR TRIM(IFNULL(t1.gcdsGlobalCOlocation, '')) = '' THEN 2 -- empty or null
     
# ELSE 0 -- no match
# END AS gcobGcdsGlobalClientOwnerLocationMatch


#   -- GCOB & GCDS Next Review Date
#   , CASE
#       WHEN t5.nextreviewdate = t1.gcdsCDDNextReviewdate THEN 1 
#       WHEN t5.nextreviewdate IS NULL OR t1.gcdsCDDNextReviewdate IS NULL OR TRIM(t5.nextreviewdate) = '' OR TRIM(t1.gcdsCDDNextReviewdate) = '' THEN 2 -- empty or null
#       ELSE 0 
#     END AS gcobGcdsNextReviewDateMatch


#   -- GCOB & GCDS Risk Level
#   , CASE
#       WHEN t5.ValidatedRiskLevel = t1.gcdsCDDRating THEN 1
#       WHEN t5.ValidatedRiskLevel IS NULL OR t1.gcdsCDDRating IS NULL OR TRIM(t5.ValidatedRiskLevel) = '' OR TRIM(t1.gcdsCDDRating) = '' THEN 2 -- empty or null
#       ELSE 0 
#     END AS gcobGcdsValidatedRiskLevelMatch
  
#   -- GCOB AND SIEBEL Email address
#   , CASE
#       WHEN TRIM(t5.ContactPersonEmailAddress) = TRIM(t6.siebelEmailAddress) THEN 1
#       WHEN  TRIM(t5.ContactPersonEmailAddress) IS NULL OR TRIM(t6.siebelEmailAddress) IS NULL OR TRIM(t5.ContactPersonEmailAddress) = '' OR TRIM(t6.siebelEmailAddress) = '' THEN 2 -- empty or null
#       ELSE 0 
#     END AS gcobSiebelEmailAddressMatch

#   -- GCOB & GCDS Business Line
#   /*
#   , CASE
#       WHEN LOWER(TRIM(t5.BusinessLineName)) = LOWER(TRIM(t1.gcdsCObusinesslinedescription)) THEN 1 -- string match
#       WHEN LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t5.BusinessLineName, 
#             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ), 
#           '[^a-zA-Z0-9]', ''
#         )
#       )) = LOWER(TRIM(
#         REGEXP_REPLACE(
#           TRANSLATE(t1.gcdsCObusinesslinedescription, 
#             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
#             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
#           ), 
#           '[^a-zA-Z0-9]', ''
#         )
#       )) THEN 3 -- special characters removed, string match
#       WHEN t5.BusinessLineName IS NULL OR t1.gcdsCObusinesslinedescription IS NULL OR TRIM(t5.BusinessLineName) = '' OR TRIM(t1.gcdsCObusinesslinedescription) = '' THEN 2 -- empty or null
#       ELSE 0 -- no match
#     END AS gcobGcdsBusinessLineMatch
# */
# FROM gcdsRawData t1
# FULL OUTER JOIN siebelClientInformation t2 ON t1.siebelID = t2.siebelID
# FULL OUTER JOIN siebelProductInformation t3 ON t1.siebelID = t3.siebelID
# FULL OUTER JOIN siebelAddressInformation t4 ON t1.siebelID = t4.siebelID
# FULL OUTER JOIN siebelEmailAddressInformation t6 on t1.siebelID = t6.siebelID
# FULL OUTER JOIN sggGcobData t5 ON t1.gcobid = t5.UniqueGcobId
# */

# COMMAND ----------

# MAGIC  %sql
# MAGIC  CREATE OR REPLACE TEMPORARY VIEW siebelGcobGcdsClientComparison AS
# MAGIC  SELECT DISTINCT
# MAGIC    t1.*
# MAGIC  , t2.*
# MAGIC from siebelGcobGcdsClientComparisonTemp as t1
# MAGIC  LEFT JOIN radar.sggapp as t2 on t1.sggUniqueIdentifier = t2.sggAppUniqueID

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcobGcdsOverview AS
# MAGIC SELECT
# MAGIC t1.uniquegcobid,
# MAGIC t1.ClientLifeCycleName,
# MAGIC t1.FullLegalName,
# MAGIC t1.globalkycportfolionew, 
# MAGIC t1.globalreportingregion, 
# MAGIC t1.globalclientownerlocation, 
# MAGIC t1.GlobalClientOwner, 
# MAGIC t1.sectorteam, 
# MAGIC t1.kycgroup, 
# MAGIC t1.nextreviewdate,
# MAGIC t1.validatedrisklevel, 
# MAGIC t1.FIHubIndicator_Derived, 
# MAGIC t1.gcdsid as gcidInGcob,
# MAGIC t1.BusinessLineName,
# MAGIC t2.gcid as gcidInGcds
# MAGIC FROM
# MAGIC radar.clients AS t1
# MAGIC LEFT JOIN
# MAGIC (SELECT gcid, keystore_value FROM gcds_client_KeyStoreKey
# MAGIC WHERE keyStore_type = 'GCOBID') AS t2
# MAGIC ON
# MAGIC t1.UniqueGcobId = t2.keystore_value

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC   2025-05-21 code by Trouw, G (Gerben) 
# MAGIC */
# MAGIC CREATE OR REPLACE TEMP VIEW hierarchy AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   CASE
# MAGIC     WHEN Com_child.GCID IS NULL OR Com_parent.GCID IS NULL THEN UltAndDown.UltimateParent
# MAGIC     ELSE Com_parent.GCID
# MAGIC   END SuperParent
# MAGIC   , UltAndDown.* 
# MAGIC
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     cast(
# MAGIC       CASE
# MAGIC         WHEN L8P.`Relationship-Value` IS NOT NULL AND L8P.`Relationship-Type` = 'Credit' THEN L8P.`Relationship-Value`
# MAGIC         WHEN L8O.`Relationship-Value` IS NOT NULL AND L8O.`Relationship-Type` = 'Operational Hierarchy' THEN L8O.`Relationship-Value`
# MAGIC         WHEN L7P.`Relationship-Value` IS NOT NULL AND L7P.`Relationship-Type` = 'Credit' THEN L7P.`Relationship-Value`
# MAGIC         WHEN L7O.`Relationship-Value` IS NOT NULL AND L7O.`Relationship-Type` = 'Operational Hierarchy' THEN L7O.`Relationship-Value`
# MAGIC         WHEN L6P.`Relationship-Value` IS NOT NULL AND L6P.`Relationship-Type` = 'Credit' THEN L6P.`Relationship-Value`
# MAGIC         WHEN L6O.`Relationship-Value` IS NOT NULL AND L6O.`Relationship-Type` = 'Operational Hierarchy' THEN L6O.`Relationship-Value`
# MAGIC         WHEN L5P.`Relationship-Value` IS NOT NULL AND L5P.`Relationship-Type` = 'Credit' THEN L5P.`Relationship-Value`
# MAGIC         WHEN L5O.`Relationship-Value` IS NOT NULL AND L5O.`Relationship-Type` = 'Operational Hierarchy' THEN L5O.`Relationship-Value`
# MAGIC         WHEN L4P.`Relationship-Value` IS NOT NULL AND L4P.`Relationship-Type` = 'Credit' THEN L4P.`Relationship-Value`
# MAGIC         WHEN L4O.`Relationship-Value` IS NOT NULL AND L4O.`Relationship-Type` = 'Operational Hierarchy' THEN L4O.`Relationship-Value`
# MAGIC         WHEN L3P.`Relationship-Value` IS NOT NULL AND L3P.`Relationship-Type` = 'Credit' THEN L3P.`Relationship-Value`
# MAGIC         WHEN L3O.`Relationship-Value` IS NOT NULL AND L3O.`Relationship-Type` = 'Operational Hierarchy' THEN L3O.`Relationship-Value`
# MAGIC         WHEN L2P.`Relationship-Value` IS NOT NULL AND L2P.`Relationship-Type` = 'Credit' THEN L2P.`Relationship-Value`
# MAGIC         WHEN L2O.`Relationship-Value` IS NOT NULL AND L2O.`Relationship-Type` = 'Operational Hierarchy' THEN L2O.`Relationship-Value`
# MAGIC         WHEN L1P.`Relationship-Value` IS NOT NULL AND L1P.`Relationship-Type` = 'Credit' THEN L1P.`Relationship-Value`
# MAGIC         WHEN L1O.`Relationship-Value` IS NOT NULL AND L1O.`Relationship-Type` = 'Operational Hierarchy' THEN L1O.`Relationship-Value`
# MAGIC         ELSE Legal1.LegalEntity
# MAGIC       END AS INT
# MAGIC     ) AS UltimateParent
# MAGIC
# MAGIC     , Legal1.LegalEntity
# MAGIC     , Legal1.`Relationship-Value`	AS ParentGlobalClientID
# MAGIC     , Legal1.`Relationship-Type`	AS RelationshipType
# MAGIC     , Legal1.GCID AS GCID
# MAGIC     , C.Full_legal_name
# MAGIC     , coalesce(L8P.`Relationship-Value`, L8O.`Relationship-Value`) AS L8P
# MAGIC     , coalesce(L8P.`Relationship-Type`, L8O.`Relationship-Type`  ) AS L8PType
# MAGIC     , coalesce(L7P.`Relationship-Value`, L7O.`Relationship-Value`) AS L7P
# MAGIC     , coalesce(L7P.`Relationship-Type`, L7O.`Relationship-Type`  ) AS L7PType
# MAGIC     , coalesce(L6P.`Relationship-Value`, L6O.`Relationship-Value`) AS L6P
# MAGIC     , coalesce(L6P.`Relationship-Type`, L6O.`Relationship-Type`  ) AS L6PType
# MAGIC     , coalesce(L5P.`Relationship-Value`, L5O.`Relationship-Value`) AS L5P
# MAGIC     , coalesce(L5P.`Relationship-Type`, L5O.`Relationship-Type`  ) AS L5PType
# MAGIC     , coalesce(L4P.`Relationship-Value`, L4O.`Relationship-Value`) AS L4P
# MAGIC     , coalesce(L4P.`Relationship-Type`, L4O.`Relationship-Type`  ) AS L4PType
# MAGIC     , coalesce(L3P.`Relationship-Value`, L3O.`Relationship-Value`) AS L3P
# MAGIC     , coalesce(L3P.`Relationship-Type`, L3O.`Relationship-Type`  ) AS L3PType
# MAGIC     , coalesce(L2P.`Relationship-Value`, L2O.`Relationship-Value`) AS L2P
# MAGIC     , coalesce(L2P.`Relationship-Type`, L2O.`Relationship-Type`  ) AS L2PType
# MAGIC     , coalesce(L1P.`Relationship-Value`, L1O.`Relationship-Value`) AS L1P
# MAGIC     , coalesce(L1P.`Relationship-Type`, L1O.`Relationship-Type`  ) AS L1PTyp
# MAGIC
# MAGIC   
# MAGIC   FROM gcds_client_Client C
# MAGIC   LEFT JOIN ( 
# MAGIC     SELECT
# MAGIC       cast(
# MAGIC         CASE
# MAGIC           WHEN RC3.`Relationship-Value` IS NOT NULL AND C3.Party_type <> 'Legal Entity' THEN RC3.`Relationship-Value`
# MAGIC           WHEN C4.GCID IS NOT NULL THEN C4.GCID
# MAGIC           WHEN RC2.`Relationship-Value` IS NOT NULL AND C2.Party_type <> 'Legal Entity' THEN RC2.`Relationship-Value`
# MAGIC           WHEN C3.GCID IS NOT NULL THEN C3.GCID
# MAGIC           WHEN RC1.`Relationship-Value` IS NOT NULL AND C1.Party_type <> 'Legal Entity' THEN RC1.`Relationship-Value`
# MAGIC           WHEN C2.GCID IS NOT NULL THEN C2.GCID
# MAGIC 				  WHEN RC.`Relationship-Value` IS NOT NULL AND C.Party_type <> 'Legal Entity' THEN RC.`Relationship-Value`
# MAGIC   				WHEN C1.GCID IS NULL THEN C.GCID
# MAGIC   				ELSE R.`Relationship-Value`
# MAGIC         END AS INT
# MAGIC       ) LegalEntity
# MAGIC       , R.`Relationship-Value`
# MAGIC       , R.`Relationship-Type`
# MAGIC       , C.GCID
# MAGIC     
# MAGIC     FROM gcds_client_Client C                                                       
# MAGIC     LEFT JOIN gcds_client_PartytoPartyRelationship RC ON RC.GCID = C.GCID AND RC.`Relationship-Type` = 'Credit'
# MAGIC     LEFT JOIN gcds_client_PartytoPartyRelationship R ON C.GCID = R.GCID -- AND RC.`Relationship-Value` IS NULL
# MAGIC       AND (
# MAGIC         (C.Party_type IN ('Branch','Foreign Branch') AND R.`Relationship-Type` = 'Branch of')
# MAGIC         OR (C.Party_type = 'Sub Account' AND R.`Relationship-Type` = 'Main Account')
# MAGIC         OR (C.Party_type = 'Organisation Unit' AND R.`Relationship-Type` = 'Organisation Unit of')
# MAGIC         OR (C.Party_type = 'Managed Fund' AND R.`Relationship-Type` = 'Managed by')
# MAGIC         OR (C.Party_type = 'Sub-fund' AND R.`Relationship-Type` = 'Sub-fund of')
# MAGIC   		)
# MAGIC     LEFT JOIN gcds_client_Client C1 ON C1.GCID = R.`Relationship-Value`
# MAGIC       AND ((C.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C1.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
# MAGIC       OR (C.Party_type IN ('Branch','Foreign Branch') AND C1.Party_type = 'Legal Entity'))
# MAGIC     LEFT JOIN gcds_client_PartytoPartyRelationship RC1 ON RC1.GCID = C1.GCID AND RC1.`Relationship-Type` = 'Credit'
# MAGIC     LEFT JOIN gcds_client_PartytoPartyRelationship R1 ON R1.GCID = C1.GCID -- AND RC1.`Relationship-Value` IS NULL
# MAGIC       AND R1.`Relationship-Value` <> R1.GCID
# MAGIC       AND (
# MAGIC         (C1.Party_type IN ('Branch','Foreign Branch') AND R1.`Relationship-Type` = 'Branch of')
# MAGIC         OR (C1.Party_type = 'Sub Account' AND R1.`Relationship-Type` = 'Main Account')
# MAGIC         OR (C1.Party_type = 'Organisation Unit' AND R1.`Relationship-Type` = 'Organisation Unit of')
# MAGIC         OR (C1.Party_type = 'Managed Fund' AND R1.`Relationship-Type` = 'Managed by')
# MAGIC         OR (C1.Party_type = 'Sub-fund' AND R1.`Relationship-Type` = 'Sub-fund of')
# MAGIC   		)
# MAGIC     LEFT JOIN gcds_client_Client C2 ON C2.GCID = R1.`Relationship-Value`
# MAGIC       AND ((C1.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C2.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
# MAGIC       OR (C1.Party_type IN ('Branch','Foreign Branch') AND C2.Party_type = 'Legal Entity')) 
# MAGIC     LEFT JOIN gcds_client_PartytoPartyRelationship RC2 ON RC2.GCID = C2.GCID AND RC2.`Relationship-Type` = 'Credit'
# MAGIC     LEFT JOIN gcds_client_PartytoPartyRelationship R2 ON R2.GCID = C2.GCID -- AND RC2.`Relationship-Value` IS NULL
# MAGIC       AND R2.`Relationship-Value` <> R2.GCID
# MAGIC       AND (
# MAGIC         (C2.Party_type IN ('Branch','Foreign Branch') AND R2.`Relationship-Type` = 'Branch of')
# MAGIC         OR (C2.Party_type = 'Sub Account' AND R2.`Relationship-Type` = 'Main Account')
# MAGIC         OR (C2.Party_type = 'Organisation Unit' AND R2.`Relationship-Type` = 'Organisation Unit of')
# MAGIC         OR (C2.Party_type = 'Managed Fund' AND R2.`Relationship-Type` = 'Managed by')
# MAGIC         OR (C2.Party_type = 'Sub-fund' AND R2.`Relationship-Type` = 'Sub-fund of')
# MAGIC       )
# MAGIC     LEFT JOIN gcds_client_Client C3 ON C3.GCID = R2.`Relationship-Value`
# MAGIC       AND ((C2.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C3.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
# MAGIC       OR ( C2.Party_type IN ('Branch','Foreign Branch') AND C3.Party_type = 'Legal Entity')) 
# MAGIC     LEFT JOIN gcds_client_PartytoPartyRelationship RC3 ON RC3.GCID = C3.GCID 
# MAGIC       AND RC3.`Relationship-Type` = 'Credit'
# MAGIC     LEFT JOIN gcds_client_PartytoPartyRelationship R3 ON R3.GCID = R2.`Relationship-Value` -- AND RC3.`Relationship-Value` IS NULL
# MAGIC       AND R3.`Relationship-Value` <> R3.GCID
# MAGIC       AND (
# MAGIC         (C3.Party_type IN ('Branch','Foreign Branch') AND R3.`Relationship-Type` = 'Branch of')
# MAGIC         OR (C3.Party_type = 'Sub Account' AND R3.`Relationship-Type` = 'Main Account')
# MAGIC         OR (C3.Party_type = 'Organisation Unit' AND R3.`Relationship-Type` = 'Organisation Unit of')
# MAGIC         OR (C3.Party_type = 'Managed Fund' AND R3.`Relationship-Type` = 'Managed by')
# MAGIC         OR (C3.Party_type = 'Sub-fund' AND R3.`Relationship-Type` = 'Sub-fund of')
# MAGIC       )
# MAGIC     LEFT JOIN gcds_client_Client C4 ON C4.GCID = R2.`Relationship-Value`
# MAGIC       AND ((C3.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C4.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
# MAGIC       OR (C3.Party_type IN ('Branch','Foreign Branch') AND C4.Party_type = 'Legal Entity')) 
# MAGIC                                                                        
# MAGIC   ) Legal1 ON Legal1.GCID = C.GCID
# MAGIC                                                 
# MAGIC   LEFT JOIN gcds_client_Client LegalC1 ON LegalC1.GCID = Legal1.LegalEntity
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L1P ON  L1P.GCID = Legal1.LegalEntity
# MAGIC     AND LegalC1.Party_type in ('Legal Entity','Natural Person')
# MAGIC     AND L1P.`Relationship-Type` = 'Credit'
# MAGIC     AND L1P.`Relationship-Value` <> L1P.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L1O ON  L1O.GCID = Legal1.LegalEntity
# MAGIC     AND LegalC1.Party_type in ('Legal Entity','Natural Person')
# MAGIC     AND L1O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L1O.`Relationship-Value` <> L1O.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L2P ON L2P.GCID = L1P.`Relationship-Value` 
# MAGIC     AND L2P.`Relationship-Type` = 'Credit'
# MAGIC     AND L2P.`Relationship-Value` <> L2P.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L2O ON L2O.GCID = L1O.`Relationship-Value`
# MAGIC     AND L2O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L2O.`Relationship-Value` <> L2O.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L3P ON L3P.GCID = L2P.`Relationship-Value`
# MAGIC     AND L3P.`Relationship-Type` = 'Credit'
# MAGIC     AND L3P.`Relationship-Value` <> L3P.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L3O ON L3O.GCID = L2O.`Relationship-Value` 
# MAGIC     AND L3O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L3O.`Relationship-Value` <> L3O.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L4P ON L4P.GCID = L3P.`Relationship-Value`
# MAGIC     AND L4P.`Relationship-Type` = 'Credit'
# MAGIC     AND L4P.`Relationship-Value` <> L4P.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L4O ON L4O.GCID = L3O.`Relationship-Value`
# MAGIC     AND L4O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L4O.`Relationship-Value` <> L4O.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L5P ON L5P.GCID = L4P.`Relationship-Value`
# MAGIC     AND L5P.`Relationship-Type` = 'Credit'
# MAGIC     AND L5P.`Relationship-Value` <> L5P.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L5O ON L5O.GCID = L4O.`Relationship-Value`
# MAGIC     AND L5O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L5O.`Relationship-Value` <> L5O.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L6P ON L6P.GCID = L5P.`Relationship-Value`
# MAGIC     AND L6P.`Relationship-Type` = 'Credit'
# MAGIC     AND L6P.`Relationship-Value` <> L6P.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L6O ON L6O.GCID = L5O.`Relationship-Value`
# MAGIC     AND L6O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L6O.`Relationship-Value` <> L6O.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L7P ON L7P.GCID = L6P.`Relationship-Value`
# MAGIC     AND L7P.`Relationship-Type` = 'Credit'
# MAGIC     AND L7P.`Relationship-Value` <> L7P.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L7O ON L7O.GCID = L6O.`Relationship-Value`
# MAGIC     AND L7O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L7O.`Relationship-Value` <> L7O.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L8P ON L8P.GCID = L7P.`Relationship-Value`
# MAGIC     AND L8P.`Relationship-Type` = 'Credit'
# MAGIC     AND L8P.`Relationship-Value` <> L8P.GCID
# MAGIC
# MAGIC   LEFT JOIN gcds_client_PartytoPartyRelationship L8O ON L8O.GCID = L7O.`Relationship-Value`
# MAGIC     AND L8O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L8O.`Relationship-Value` <> L8O.GCID
# MAGIC
# MAGIC ) UltAndDown
# MAGIC
# MAGIC LEFT JOIN gcds_client_PartytoPartyRelationship Commercial ON Commercial.GCID = UltAndDown.UltimateParent AND Commercial.`Relationship-Type` = 'Commercial'
# MAGIC LEFT JOIN gcds_client_Client Com_Parent ON Com_Parent.GCID = Commercial.`Relationship-Value` AND Com_Parent.Party_type in ('Legal Entity','Natural Person')
# MAGIC LEFT JOIN gcds_client_Client Com_child ON Com_child.GCID = Commercial.GCID AND Com_child.Party_type in ('Legal Entity','Natural Person')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcdsClientsFinancedEmission AS
# MAGIC SELECT DISTINCT
# MAGIC /*
# MAGIC     t1.gcid, 
# MAGIC     t1.Full_legal_name AS gcdsClientName, 
# MAGIC     t1.`Global_CO-name` AS gcdsGlobalCOname, 
# MAGIC     t1.`Global_CO-location` AS gcdsGlobalCoLocation, 
# MAGIC     t1.`Global_CO-businessline_description` AS businessLineDescription, 
# MAGIC     t1.`Global_CO-Serviced_by` AS servicedBy, 
# MAGIC     */
# MAGIC     t2.superparent AS superparentGcid, 
# MAGIC     t3.Full_legal_name AS superparentGcdsClientName,
# MAGIC     t3.`Global_CO-name` AS superparentGcdsGlobalCOname, 
# MAGIC     t3.`Global_CO-location` AS superparentGcdsGlobalCoLocation, 
# MAGIC     t3.`Global_CO-businessline_description` AS superparentBusinessLineDescription, 
# MAGIC     t3.`Global_CO-Serviced_by` AS superparentSectorTeam,
# MAGIC     t4.Life_cycle_status,
# MAGIC     naics.Primary_NAICS AS superparentprimaryNaicsGCDS
# MAGIC
# MAGIC FROM gcds_client_client AS t1 
# MAGIC LEFT JOIN hierarchy AS t2 ON t1.gcid = t2.gcid
# MAGIC LEFT JOIN gcds_client_client AS t3 ON t2.superparent = t3.gcid
# MAGIC LEFT JOIN gcds_client_PartyRole t4 ON t1.gcid = t4.gcid
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT
# MAGIC         c.gcid,
# MAGIC         c.Primary_NAICS
# MAGIC     FROM gcds_client_client c
# MAGIC     LEFT JOIN gcds_client_KeyStoreKey k
# MAGIC         ON c.gcid = k.gcid
# MAGIC         AND k.keystore_type = 'GCOBID'
# MAGIC         AND k.status = 'Active'
# MAGIC     LEFT JOIN radar.clients r
# MAGIC         ON k.KeyStore_value = r.uniquegcobid
# MAGIC         AND r.ClientLifeCycleName = 'Client'
# MAGIC     LEFT JOIN gcds_client_PartyRole p 
# MAGIC         ON c.gcid = p.gcid    
# MAGIC     LEFT JOIN radar.gcdsclients g
# MAGIC         ON c.gcid = g.gcid
# MAGIC ) naics
# MAGIC     ON t2.superparent = naics.gcid
# MAGIC
# MAGIC where t4.Life_cycle_status = 'Client'

# COMMAND ----------

# MAGIC %md
# MAGIC **dropping and re-creating tables**

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.gcdsClients
# MAGIC

# COMMAND ----------

spark.sql('select * from gcdsClients').write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('radar.gcdsClients')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.gcdsRawData

# COMMAND ----------

spark.sql('select * from gcdsRawData').write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('radar.gcdsRawData')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.siebelClientInformation

# COMMAND ----------

spark.sql('select * from siebelClientInformation').write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('radar.siebelClientInformation')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.siebelProductInformation

# COMMAND ----------

spark.sql('select * from siebelProductInformation').write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('radar.siebelProductInformation')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.siebelAddressInformation

# COMMAND ----------

spark.sql('select * from siebelAddressInformation').write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('radar.siebelAddressInformation')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.sggGcobData

# COMMAND ----------

spark.sql('select * from sggGcobData').write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('radar.sggGcobData')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.siebelGcobGcdsClientComparison

# COMMAND ----------

spark.sql('select * from siebelGcobGcdsClientComparison').write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('radar.siebelGcobGcdsClientComparison')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.siebelDummyDirector

# COMMAND ----------

spark.sql('select * from siebelDummyDirector').write.mode('overwrite').saveAsTable('radar.siebelDummyDirector')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.siebelEmailAddressInformation

# COMMAND ----------

spark.sql('select * from siebelEmailAddressInformation').write.mode('overwrite').saveAsTable('radar.siebelEmailAddressInformation')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.gcobClientNotInGcds

# COMMAND ----------

spark.sql('select * from gcobClientNotInGCDS').write.mode('overwrite').saveAsTable('radar.gcobClientNotInGcds')


# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.siebelClientNotInGcds

# COMMAND ----------

spark.sql('select * from siebelClientNotInGCDS').write.mode('overwrite').saveAsTable('radar.siebelClientNotInGcds')


# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.gobClientNotInSiebel

# COMMAND ----------

spark.sql('select * from gobClientNotInSiebel').write.mode('overwrite').saveAsTable('radar.gobClientNotInSiebel')


# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.siebelClientNotInGcob

# COMMAND ----------

spark.sql('select * from siebelClientNotInGcob').write.mode('overwrite').saveAsTable('radar.siebelClientNotInGcob')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.gcdsClientNotInGcob

# COMMAND ----------

spark.sql('select * from gcdsClientNotInGcob').write.mode('overwrite').saveAsTable('radar.gcdsClientNotInGcob')


# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.gcobWithoutGcidInGcob

# COMMAND ----------

spark.sql('select * from gcobWithoutGcidInGcob').write.mode('overwrite').saveAsTable('radar.gcobWithoutGcidInGcob')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.gcobGcdsOverview

# COMMAND ----------

spark.sql('select * from gcobGcdsOverview').write.mode('overwrite').saveAsTable('radar.gcobGcdsOverview')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.gcdsClientsInDataverse

# COMMAND ----------

spark.sql('select * from gcdsClientsFinancedEmission').write.mode('overwrite').option("mergeSchema", "true").saveAsTable('radar.gcdsClientsInDataverse')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.gcdsclientsindataverse

# COMMAND ----------

# DBTITLE 1,GCID to WWID mapping for missing entities
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcdsaetosforplanet AS
# MAGIC
# MAGIC SELECT DISTINCT t1.GCID, t2.KeyStore_value AS WWID, t4.Life_cycle_status, t1.Full_legal_name AS GCDS_Client_Name
# MAGIC FROM  gcds_client_client t1
# MAGIC LEFT JOIN gcds_client_KeyStoreKey t2 ON t1.gcid = t2.gcid
# MAGIC LEFT JOIN gcds_client_PartytoPartyRelationship t3 ON t1.gcid = t3.gcid
# MAGIC LEFT JOIN gcds_client_PartyRole t4 ON t1.gcid = t4.gcid
# MAGIC WHERE t2.KeyStore_type = 'WWID' AND t2.Status = 'Active' AND t4.party_role IN ('Customer', 'Related Party')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW best_status AS
# MAGIC
# MAGIC SELECT
# MAGIC     gcid,
# MAGIC     life_cycle_status AS best_life_cycle_status
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         gcid,
# MAGIC         life_cycle_status,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY gcid
# MAGIC             ORDER BY CASE life_cycle_status
# MAGIC                         WHEN 'Client' THEN 1
# MAGIC                         WHEN 'Former Client' THEN 2
# MAGIC                         WHEN 'Prospect' THEN 3
# MAGIC                         WHEN 'Former Prospect' THEN 4
# MAGIC                         WHEN 'Exit Client' THEN 5
# MAGIC                         WHEN 'Active' THEN 6
# MAGIC                         WHEN 'Inactive' THEN 7
# MAGIC                         ELSE 99
# MAGIC                     END
# MAGIC         ) AS rn
# MAGIC     FROM gcdsaetosforplanet
# MAGIC )
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcdsaetosplanet AS
# MAGIC
# MAGIC SELECT
# MAGIC     gcid,
# MAGIC     FIRST(WWID) AS WWID,                     
# MAGIC     best_life_cycle_status AS life_cycle_status,
# MAGIC     FIRST(GCDS_Client_Name) AS GCDS_Client_Name
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         b.*,
# MAGIC         s.best_life_cycle_status,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY b.gcid
# MAGIC             ORDER BY b.WWID ASC NULLS LAST   
# MAGIC         ) AS rn
# MAGIC     FROM gcdsaetosforplanet b
# MAGIC     LEFT JOIN best_status s
# MAGIC         ON b.GCID = s.GCID
# MAGIC )
# MAGIC WHERE rn = 1
# MAGIC GROUP BY gcid, best_life_cycle_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.gcdsaetosplanet

# COMMAND ----------

spark.sql('select * from gcdsaetosplanet').write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('radar.gcdsaetosplanet')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,AD-HOC REQUEST: Alexia - holding companies
# %sql
# WITH cte AS (
# SELECT
#     t1.UniqueGcobId,
#     t2.FullLegalName AS ClientName,
#     t2.GCDSID AS GCID,

#     concat_ws('; ',
#         sort_array(collect_list(cast(t1.Code AS string)))
#     ) AS NAICSCode,

#     concat_ws('; ',
#         sort_array(collect_list(t1.Name))
#     ) AS NAICSDescription,

#     t2.SignOff,
#     t2.Completed,
#     t2.NextReviewDate,
#     t2.GlobalClientOwnerLocation

# FROM radar.SiraNAICS t1
# LEFT JOIN radar.Clients t2 
#     ON t1.UniqueGcobId = t2.UniqueGcobId

# WHERE t1.Code IN (551111, 551112, 551114)
#   AND t2.ClientLifeCycleName = 'Client'
#   AND t2.globalreportingregion IN ('E&A', 'Asia')

# GROUP BY
#     t1.UniqueGcobId,
#     t2.FullLegalName,
#     t2.GCDSID,
#     t2.SignOff,
#     t2.Completed,
#     t2.NextReviewDate,
#     t2.GlobalClientOwnerLocation

# ORDER BY t1.UniqueGcobId
# )
# , cte2 as (
#     SELECT DISTINCT
#     k.gcid
#     , c.Primary_NAICS
#     , c.Primary_NAICS_description
# FROM gcds_client_client c
# LEFT JOIN gcds_client_KeyStoreKey k
#     ON c.gcid = k.gcid
#     AND k.keystore_type = 'GCOBID'
#     AND k.status = 'Active'
# LEFT JOIN radar.clients r
#     ON k.KeyStore_value = r.uniquegcobid
#     AND r.ClientLifeCycleName = 'Client'
# LEFT JOIN gcds_client_PartyRole p 
#     ON c.gcid = p.gcid    
# LEFT JOIN radar.gcdsclients g
#     ON c.gcid = g.gcid
# )

# SELECT
#     t1.*
#     , t2.Primary_NAICS
#     , t2.Primary_NAICS_description
# FROM cte t1
# left join cte2 t2 on t1.gcid = t2.gcid


# where t1.gcid in (
#     3016784
#     , 3038777
#     , 3026490
#     , 3044310
#     , 3037348
#     , 3042214
#     , 3062112
#     , 3068963
#     , 3088096
#     , 3116598
#     , 2480538
#     , 3114747
#     , 3116367
#     , 2267071
#     , 2774732
#     , 2637124
#     , 2718428
#     , 1289399
#     , 2967803
#     , 2980530
#     , 2851564
#     , 2890299)

# COMMAND ----------

# %sql
# SELECT
#     t1.gcid
#     -- , t2.KeyStore_value
#     , t1.Primary_NAICS
#     , t1.Primary_NAICS_description
# FROM gcds_client_client t1

# where t1.gcid in (2267071,
# 2774732,
# 2637124,
# 2480538)


# -- LEFT JOIN gcds_client_KeyStoreKey t2 ON t1.gcid = t2.gcid AND t2.keystore_type = 'GCOBID' AND t2.status = 'Active'

# -- where t2.KeyStore_value IN (
# --     108862
# --     , 111532
# --     , 113372
# --     , 114711
# --     , 115583
# --     , 115838
# --     , 121467
# --     , 123952
# --     , 129083
# --     , 135844
# --     , 14016
# --     , 140948
# --     , 141791
# --     , 30397
# --     , 42801
# --     , 58924
# --     , 6229
# --     , 8729
# --     , 87398
# --     , 88335
# --     , 89309
# --     , 89485
# -- )

# -- LEFT JOIN radar.clients r
# --     ON k.KeyStore_value = r.uniquegcobid
# --     AND r.ClientLifeCycleName = 'Client'
# -- LEFT JOIN gcds_client_PartyRole p 
# --     ON gcid = p.gcid    
# -- LEFT JOIN radar.gcdsclients g
# --     ON gcid = g.gcid

# -- where t1.gcid in (1289399)
#     -- 3016784
#     -- , 3038777
#     -- , 3026490
#     -- , 3044310
#     -- , 3037348
#     -- , 3042214
#     -- , 3062112
#     -- , 3068963
#     -- , 3088096
#     -- , 3116598
#     -- , 2480538
#     -- , 3114747
#     -- , 3116367
#     -- , 2267071
#     -- , 2774732
#     -- , 2637124
#     -- , 2718428
#     -- , 1289399
#     -- , 2967803
#     -- , 2980530
#     -- , 2851564
#     -- , 2890299)

# COMMAND ----------

# DBTITLE 1,AD-HOC REQUEST: Nora - Onboarding GCOB-Siebel
# Nora - Onboarding GCOB-Siebel

# %sql
# select distinct
#     t1.UniqueGcobId
#     , t1.FullLegalName as GCOBClientName
#     , t1.ClientLifeCycleName as GCOBClientLifeCycleName
#     , t1.CaseReviewType
#     , t1.Completed as CompletedDate
#     , t1.GlobalClientOwnerLocation
#     , t1.ValidatedRiskLevel as GCOBValidatedRiskLevel
#     , t2.siebelID
#     , t3.siebelClientLifeCycleName
#     , t3.siebelClientName
#     , t3.siebelBankCode

#     , t4.cdd_rtg_ggm_dsc AS SiebelRisicocategorie
#     , t4.cdd_rtg_st_ggm_dsc AS SiebelStatus


# from radar.cases t1
# left join radar.siebelgcobgcdsclientcomparison t2 on t1.UniqueGcobId = t2.UniqueGcobId
# left join radar.siebelclientinformation t3 on t2.siebelID = t3.siebelID
# left join siebel_cdf_ggm_org_cdd_hist t4 on t2.siebelID = t4.rel_id and t4.edl_valid_to_dts='9999-12-31' and t4.cdd_rtg_prim_f = 'Y'

# where 1=1
#     and t1.CaseReviewType in ('Initial On-Boarding', 'Fast Migration')
#     -- and t1.CountryOfRegistration = 'Netherlands (the)'
#     and t1.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Kenya')
#     and year(t1.Completed) >= 2024

#     -- and t2.siebelID is not null

# -- select * from radar.siebelgcobgcdsclientcomparison limit 2
# -- select * from radar.siebelclientinformation limit 2
# -- select distinct CaseReviewType from radar.cases
