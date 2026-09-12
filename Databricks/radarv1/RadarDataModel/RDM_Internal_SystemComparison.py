# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To Achive Data Model result by considering All the source systems where W&R Clients are available and GCDS source
# MAGIC
# MAGIC #### author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Take W&R clients data from different source systems
# MAGIC - Take GCDS data
# MAGIC - Join on GCDSId to further derive required output
# MAGIC
# MAGIC #### Expected output
# MAGIC   - Column List is added at the end of this notebook 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read Files from GDP

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
import os
import pandas as pd
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Defining the dataobjects' name
party_dataobject='Internal_SystemComparison'

# COMMAND ----------

# DBTITLE 1,Connection to GDP defined layer
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# DBTITLE 1,Read GCDS data from GDP defined layer
# print list of strings for loading spark dfs from GDP
gcds_df = pd.DataFrame({'definedDatasetname':[
'client_Client',
'client_ClientOwnersProduct',
# 'client_AttributeExtensionsAttribute',
'client_ClientOwnersLocal',
'client_KeyStoreKey',
'client_OnboardedLocations',
'client_PartyRole',
'client_RMA' ,
'client_PartytoPartyRelationship',
'client_Products'
]})

# Create TempView for each loading table
for index, row in gcds_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=row.definedDatasetname)

# COMMAND ----------

# DBTITLE 1,Read GCOB data from GDP defined layer
# print list of strings for loading spark dfs from GDP
load_df =[
'party_case_client_details',
'party_client_structure_GUI',
'party_RelatedPartyParentAddresses',
'party_client',
'party_trade_name',
'party_Alias',
'party_AllPartyDetails' 
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - GCOB Attribute Comparison

# COMMAND ----------

# DBTITLE 1,gcdsRawData for Comparison
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcdsRawData AS
# MAGIC SELECT DISTINCT
# MAGIC   t1.gcid
# MAGIC   , t1.keystore_type
# MAGIC   , t1.KeyStore_value AS gcobid
# MAGIC   , t1.status
# MAGIC   , t2.Full_legal_name AS gcdsClientName
# MAGIC   , t2.ResidentialAddress_Country AS gcdsResidentialAddressCountry
# MAGIC   , t2.Principal_address_country AS gcdsPrincipalAddressCountry
# MAGIC   , t2.Party_type AS gcdsPartyType
# MAGIC   , t2.`CDD-entitytype` AS gcdsCddEntitytype
# MAGIC   , t2.`CDD-next_reviewdate` AS gcdsCDDNextReviewdate
# MAGIC   , t2.`CDD-risk_rating` AS gcdsCDDRiskRating
# MAGIC   , t2.`Global_CO-name` AS gcdsGlobalCOname
# MAGIC   , t2.`Global_CO-location` AS gcdsGlobalCOlocation
# MAGIC   , t2.`Global_CO-email` AS globalCOemail
# MAGIC   , t2.`CO-businessline_description` AS gcdsCObusinesslinedescription
# MAGIC   , t2.`Global_CO-Serviced_by` AS gcdsGlobalCOServicedBy
# MAGIC   , t2.`RM-name` AS gcdsRMName
# MAGIC   , t2.`RM-location` AS gcdsRMLocation
# MAGIC   , t2.`RM-email` AS gcdsRMEmail
# MAGIC   , t2.`RM-businessline_description` AS gcdsRMBusinesslineDescription
# MAGIC   , t2.`RM-_Serviced_by` AS gcdsRMServicedBy
# MAGIC   , t2.`CO-email` AS gcdsGCOemail
# MAGIC   , t2.Registered_address_country AS gcdsRegisteredCountry
# MAGIC   , t2.`CDD-risk_rating` AS gcdsCDDRating
# MAGIC   , t3.Life_cycle_status AS gcdsClientlifecyclestatus
# MAGIC   , t3.party_role AS gcdsPartyRole
# MAGIC   , t2.Customer_ambition AS clientStrategy
# MAGIC   , t2.Registered_address_streetLine1
# MAGIC   , t2.Registered_address_house_nr
# MAGIC   , t2.Registered_address_zipcode
# MAGIC   , t2.Registered_address_city
# MAGIC   , t2.Registered_address_region
# MAGIC   , t2.Principal_address_country_code
# MAGIC   , CONCAT(t2.Registered_address_zipcode, t2.Registered_address_house_nr) AS gcdsAddress
# MAGIC   , CONCAT(t2.Registered_address_city, t2.Registered_address_region) AS gcdsCityRegion
# MAGIC   , t4.`Relationship-Type` AS relationshipType
# MAGIC
# MAGIC FROM client_KeyStoreKey t1
# MAGIC LEFT JOIN client_client t2 ON t1.gcid = t2.gcid
# MAGIC LEFT JOIN client_PartyRole t3 ON t1.gcid = t3.gcid
# MAGIC LEFT JOIN client_PartytoPartyRelationship t4 ON t1.gcid = t4.gcid
# MAGIC WHERE t1.keystore_type IN ('GCOBID')
# MAGIC   AND t1.status = 'Active'
# MAGIC   AND t3.party_role = 'Customer'
# MAGIC   --AND t4.`Relationship-Type` <> 'Has Fund Manager'

# COMMAND ----------

# DBTITLE 1,GCOB Data for Comparison
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sggGcobData AS
# MAGIC select distinct 
# MAGIC   t1.uniquegcobid,
# MAGIC   t1.FullLegalName,
# MAGIC   t1.clientlifecyclename,
# MAGIC   t2.IncorporationNumber,
# MAGIC   CONCAT(REPLACE(t2.RegisteredPostalCode, ' ', ''), t2.RegisteredNumber) AS gcobAddress,
# MAGIC   CONCAT(t2.RegisteredCity, t2.RegisteredRegion) AS gcobCityRegion,
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
# MAGIC   t3.ContactPersonTelephoneNumber
# MAGIC from radar.clients as t1
# MAGIC left join radar.cases as t2 on t1.SourceClient = t2.SourceClient
# MAGIC left join party_case_client_details as t3 on t1.SourceClient = t3.SourceClient

# COMMAND ----------

# DBTITLE 1,GCOB-GCDS Client Data Comparison
# MAGIC %sql
# MAGIC /*
# MAGIC Description for the values
# MAGIC 0: No Match
# MAGIC 1: String Matching
# MAGIC 2: Empty or Null
# MAGIC 3: Special characters removed then string match
# MAGIC */
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GcobGcdsClientComparison AS
# MAGIC SELECT DISTINCT
# MAGIC   CONCAT(t5.UniqueGcobId, '_', t1.gcid) AS sggUniqueIdentifier
# MAGIC   , t1.gcid 
# MAGIC   , t5.UniqueGcobId
# MAGIC   , t1.gcdsPartyRole
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
# MAGIC   -- GCOB and GCDS City and Region match
# MAGIC   , CASE
# MAGIC       WHEN LOWER(TRIM(gcdsCityRegion)) = LOWER(TRIM(gcobCityRegion)) THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(gcdsCityRegion, 
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
# MAGIC       WHEN gcdsCityRegion IS NULL OR gcobCityRegion IS NULL OR TRIM(gcdsCityRegion) = '' OR TRIM(gcobCityRegion) = '' THEN 2 -- empty or null
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
# MAGIC       WHEN t5.RegisteredCountryIsoCode IS NULL OR t1.Principal_address_country_code IS NULL OR TRIM(t5.RegisteredCountryIsoCode) = '' OR TRIM(t1.Principal_address_country_code) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS gcobGcdsCountryOfRegistrationMatch
# MAGIC
# MAGIC
# MAGIC   -- GCOB & GCDS GCO
# MAGIC   , CASE
# MAGIC       WHEN LOWER(TRIM(t5.GlobalClientOwner)) = LOWER(TRIM(t1.gcdsGlobalCOname)) THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t5.GlobalClientOwner, 
# MAGIC             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ), 
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) = LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t1.gcdsGlobalCOname, 
# MAGIC             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ), 
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) THEN 3 -- special characters removed, string match
# MAGIC       WHEN t5.GlobalClientOwner IS NULL OR t1.gcdsGlobalCOname IS NULL OR TRIM(t5.GlobalClientOwner) = '' OR TRIM(t1.gcdsGlobalCOname) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS gcobGcdsGlobalClientOwnerMatch
# MAGIC
# MAGIC
# MAGIC   -- GCOB & GCDS GCO location
# MAGIC   , CASE
# MAGIC       WHEN LOWER(TRIM(t5.GlobalClientOwnerLocation)) = LOWER(TRIM(t1.gcdsGlobalCOlocation)) THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t5.GlobalClientOwnerLocation, 
# MAGIC             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ), 
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) = LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t1.gcdsGlobalCOlocation, 
# MAGIC             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ), 
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) THEN 3 -- special characters removed, string match
# MAGIC       WHEN t5.GlobalClientOwnerLocation IS NULL OR t1.gcdsGlobalCOlocation IS NULL OR TRIM(t5.GlobalClientOwnerLocation) = '' OR TRIM(t1.gcdsGlobalCOlocation) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS gcobGcdsGlobalClientOwnerLocationMatch
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
# MAGIC   -- GCOB & GCDS Business Line
# MAGIC   , CASE
# MAGIC       WHEN LOWER(TRIM(t5.BusinessLineName)) = LOWER(TRIM(t1.gcdsCObusinesslinedescription)) THEN 1 -- string match
# MAGIC       WHEN LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t5.BusinessLineName, 
# MAGIC             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ), 
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) = LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           TRANSLATE(t1.gcdsCObusinesslinedescription, 
# MAGIC             'áàäâãåéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ', 
# MAGIC             'aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN'
# MAGIC           ), 
# MAGIC           '[^a-zA-Z0-9]', ''
# MAGIC         )
# MAGIC       )) THEN 3 -- special characters removed, string match
# MAGIC       WHEN t5.BusinessLineName IS NULL OR t1.gcdsCObusinesslinedescription IS NULL OR TRIM(t5.BusinessLineName) = '' OR TRIM(t1.gcdsCObusinesslinedescription) = '' THEN 2 -- empty or null
# MAGIC       ELSE 0 -- no match
# MAGIC     END AS gcobGcdsBusinessLineMatch
# MAGIC
# MAGIC FROM gcdsRawData t1
# MAGIC FULL OUTER JOIN sggGcobData t5 ON t1.gcobid = t5.UniqueGcobId

# COMMAND ----------

df_InternalUse=spark.table('GcobGcdsClientComparison')

# COMMAND ----------

save_to_saradar_storage_account(df_InternalUse, party_dataobject, radar_datamodel_version_number, environment)
