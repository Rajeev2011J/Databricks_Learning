# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - As a screening analyst in NA should be able to see the parties i.e Client and all related parties (ALL roles) in GCOB (RAF Direct Lending + North America Wholesale)So that I will be informed about any country risk screening for these parties 
# MAGIC
# MAGIC #### author
# MAGIC - Prasad.Gadidala@rabobank.com
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Take GCOB ID and fulllegalnmae and Countrycode and Name and ISO and Nationality and citzenship Details from party all party Details
# MAGIC - Take static Data from RBL list and SanctionCountry list from NA team.
# MAGIC - Join on country details info with staicdata and create final delta table with Sanctioncountrylist.
# MAGIC - Using the final table build power Bi report
# MAGIC
# MAGIC #### Expected output
# MAGIC   - Column List is added at the end of this notebook 
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Prasad  Gadidala	 |25-Aug-2025 |12810354 | Added CountryType and countryNames as Nationality and Citizenship and NationalityIsoCode and CitizenshipIsoCode and Build a New PowerBI report
# MAGIC | Abhishek Jaiswal	 |13-Oct-2025 |13915407 | column definition and creation: Alert date & Business date
# MAGIC | Abhishek Jaiswal	 |23-Oct-2025 |14061590 | Related Parties to Main client combination fix  
# MAGIC | Abhishek Jaiswal	 |19-Dec-2025 |14740457 | New contry list so added Active flag to maintain history 

# COMMAND ----------

# DBTITLE 1,Importing Libraries
import os
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Date Variables
date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

# DBTITLE 1,Outh2 Configuration
ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

SanctionCountryScreeningNAW_dataobject = 'SanctionCountryScreeningNAW'
MatchedSanctionCountry_dataobject = 'MatchedSanctionCountry'

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details',
'party_AllPartyDetails',
'party_products_and_services',
'party_client_structure_GUI'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# DBTITLE 1,get the Approved Clients from partydetails
# MAGIC %sql
# MAGIC CREATE OR REPLaCE TEMP VIEW party_AllPartyDetailsAppovedclients as
# MAGIC Select *,Case when ClientType = 'LegalEntityClient' then concat('LEC_',Id)
# MAGIC       when ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then concat('NP_NPPC_',Id)
# MAGIC End as SourceClient from party_AllPartyDetails papd where  papd.IsLatestApprovedVersionOfClient=True

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOPorductBookingNAWLocations AS
# MAGIC SELECT DISTINCT 
# MAGIC SourceClient
# MAGIC FROM  party_case_client_details 
# MAGIC where
# MAGIC  GlobalClientOwnerLocation IN (
# MAGIC     'Rabobank Canada (RCBR)',
# MAGIC     'Rabobank Canada(Rural)',
# MAGIC     'Rabo Securities Canada, Inc. (RSCI)',
# MAGIC     'Rabo Securities USA, Inc. (RSEC)',
# MAGIC     'Rabobank - USA Rabo AgriFinance',
# MAGIC     'Rabobank New York'
# MAGIC )
# MAGIC AND IsLatestApprovedVersionOfClient='True'
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT SourceClient  
# MAGIC FROM party_products_and_services
# MAGIC WHERE ProductOfferingLocation IN (
# MAGIC     'Rabobank Canada (RCBR)',
# MAGIC     'Rabobank Canada(Rural)',
# MAGIC     'Rabo Securities Canada, Inc. (RSCI)',
# MAGIC     'Rabo Securities USA, Inc. (RSEC)',
# MAGIC     'Rabobank - USA Rabo AgriFinance',
# MAGIC     'Rabobank New York'
# MAGIC )
# MAGIC OR BookingEntityLocation IN (
# MAGIC     'Rabobank Canada (RCBR)',
# MAGIC     'Rabobank Canada(Rural)',
# MAGIC     'Rabo Securities Canada, Inc. (RSCI)',
# MAGIC     'Rabo Securities USA, Inc. (RSEC)',
# MAGIC     'Rabobank - USA Rabo AgriFinance',
# MAGIC     'Rabobank New York'
# MAGIC )
# MAGIC
# MAGIC AND IsLatestApprovedVersionOfClient='True'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Get the NAW based clients
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ApprovedclientsStructureNAW AS
# MAGIC SELECT DISTINCT
# MAGIC     cs.ClientGcobid,
# MAGIC     cs.ClientCaseId,
# MAGIC     cs.ClientFullLegalName,
# MAGIC     cs.ClientType,
# MAGIC     cs.ClientLifeCycleName,
# MAGIC     Null as  Gcobid,
# MAGIC     Null as ParentIdentityName,
# MAGIC     Null AS PartyType,
# MAGIC     pd.RegisteredCountryIsoCode,
# MAGIC     pd.RegisteredCountryName,
# MAGIC     pd.OperatingCountryIsoCode,
# MAGIC     pd.OperatingCountryName,
# MAGIC     pd.Nationality,
# MAGIC     pd.Citizenship,
# MAGIC     pd.CitizenshipIsoCode,
# MAGIC     pd.NationalityIsoCode,
# MAGIC     cs.SourceSystem,
# MAGIC     cs.ClientStructureSnapshotId,
# MAGIC     pd.UniquePartyID as UniqueParentPartyId,
# MAGIC     Null as ParentEntityId
# MAGIC FROM party_client_structure_GUI cs 
# MAGIC Inner join party_AllPartyDetailsAppovedclients pd ON cs.Sourceclient=pd.Sourceclient
# MAGIC Inner JOIN GCOPorductBookingNAWLocations pbl ON pbl.SourceClient = pd.Sourceclient
# MAGIC WHERE  cs.IsLatestApprovedVersionOfClient='True' AND pd.Status='Snapshot'

# COMMAND ----------

# DBTITLE 1,Get the related parties information from Client Structure
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ApprovedclientsStructureNAWClient AS
# MAGIC SELECT DISTINCT
# MAGIC     cs.ClientGcobid,
# MAGIC     cs.ClientCaseId,
# MAGIC     cs.ClientFullLegalName,
# MAGIC     cs.ClientType,
# MAGIC     cs.ClientLifeCycleName,
# MAGIC     cs.ParentIdentity AS Gcobid,
# MAGIC     cs.ParentIdentityName,
# MAGIC     cs.ParentType AS PartyType,
# MAGIC     Null as RegisteredCountryIsoCode,
# MAGIC     Null as RegisteredCountryName,
# MAGIC     Null as OperatingCountryIsoCode,
# MAGIC     Null as OperatingCountryName,
# MAGIC     Null as Nationality,
# MAGIC     Null as Citizenship,
# MAGIC     Null as CitizenshipIsoCode,
# MAGIC     Null as NationalityIsoCode,
# MAGIC     cs.SourceSystem,
# MAGIC     cs.ClientStructureSnapshotId,
# MAGIC     cs.UniqueParentPartyId,
# MAGIC     cs.ParentEntityId
# MAGIC FROM party_client_structure_GUI cs 
# MAGIC Inner join party_AllPartyDetailsAppovedclients pd ON cs.Sourceclient=pd.Sourceclient
# MAGIC Inner JOIN GCOPorductBookingNAWLocations pbl ON pbl.SourceClient = pd.Sourceclient
# MAGIC WHERE  cs.IsLatestApprovedVersionOfClient='True' AND pd.Status='Snapshot'

# COMMAND ----------

# DBTITLE 1,Get the Related parties  from allpartydetails
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RelatedParties AS
# MAGIC SELECT 
# MAGIC     ps.ClientGcobid,
# MAGIC     ps.ClientCaseId,
# MAGIC     ps.ClientFullLegalName,
# MAGIC     ps.ClientType,
# MAGIC     ps.ClientLifeCycleName,
# MAGIC     ps.GcobId,
# MAGIC     ps.ParentIdentityName,
# MAGIC     pd.ClientType as PartyType,
# MAGIC     pd.RegisteredCountryIsoCode,
# MAGIC     pd.RegisteredCountryName,
# MAGIC     pd.OperatingCountryIsoCode,
# MAGIC     pd.OperatingCountryName,
# MAGIC     pd.Nationality,
# MAGIC     pd.Citizenship,
# MAGIC     pd.CitizenshipIsoCode,
# MAGIC     pd.NationalityIsoCode,
# MAGIC     pd.SourceSystem,
# MAGIC     ps.ClientStructureSnapshotId,
# MAGIC     ps.UniqueParentPartyId,
# MAGIC     ps.ParentEntityId
# MAGIC     
# MAGIC FROM party_AllPartyDetails pd
# MAGIC Inner join ApprovedclientsStructureNAWClient ps ON  pd.PartyId=ps.ParentEntityId and ps.UniqueParentPartyId=pd.UniquePartyId
# MAGIC WHERE pd.Status='Snapshot' 
# MAGIC

# COMMAND ----------

# DBTITLE 1,create final party temp table union the clients and related parties
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW AllParties AS
# MAGIC select *from ApprovedclientsStructureNAW
# MAGIC union
# MAGIC select* from RelatedParties
# MAGIC

# COMMAND ----------

# DBTITLE 1,Union all country and citizenship details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW allCountrydetails AS
# MAGIC SELECT DISTINCT
# MAGIC     ap.ClientGcobId,
# MAGIC     ap.ClientCaseId,
# MAGIC     ap.ClientFullLegalName,
# MAGIC     ap.GcobId,
# MAGIC     ap.ParentIdentityName,
# MAGIC     ap.ClientType,
# MAGIC     ap.ClientLifeCycleName,
# MAGIC     ap.PartyType,
# MAGIC     ap.RegisteredCountryIsoCode AS CountryIsoCode,
# MAGIC     'RegisteredCountry' AS CountryType,
# MAGIC     ap.RegisteredCountryName AS CountryName,
# MAGIC     ap.UniqueParentPartyId as UniquePartyId,
# MAGIC     ap.SourceSystem,
# MAGIC     date_format(current_date(), 'dd-MM-yyyy') AS LastLoadDate
# MAGIC     ,ap.ClientStructureSnapshotId
# MAGIC FROM AllParties ap
# MAGIC
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     ap.ClientGcobId,
# MAGIC     ap.ClientCaseId,
# MAGIC     ap.ClientFullLegalName,
# MAGIC     ap.GcobId,
# MAGIC     ap.ParentIdentityName,
# MAGIC     ap.ClientType,
# MAGIC     ap.ClientLifeCycleName,
# MAGIC     ap.PartyType,
# MAGIC     ap.OperatingCountryIsoCode AS CountryIsoCode,
# MAGIC     'OperatingCountry' AS CountryType,
# MAGIC     ap.OperatingCountryName AS CountryName,
# MAGIC     ap.UniqueParentPartyId as UniquePartyId,
# MAGIC     ap.SourceSystem,
# MAGIC     date_format(current_date(), 'dd-MM-yyyy') AS LastLoadDate
# MAGIC     ,ap.ClientStructureSnapshotId
# MAGIC FROM AllParties ap
# MAGIC
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     ap.ClientGcobId,
# MAGIC     ap.ClientCaseId,
# MAGIC     ap.ClientFullLegalName,
# MAGIC     ap.GcobId,
# MAGIC     ap.ParentIdentityName,
# MAGIC     ap.ClientType,
# MAGIC     ap.ClientLifeCycleName,
# MAGIC     ap.PartyType,
# MAGIC     ap.NationalityIsoCode AS CountryIsoCode,
# MAGIC     'Nationality' AS CountryType,
# MAGIC     ap.Nationality AS CountryName,
# MAGIC     ap.UniqueParentPartyId as UniquePartyId,
# MAGIC     ap.SourceSystem,
# MAGIC     date_format(current_date(), 'dd-MM-yyyy') AS LastLoadDate
# MAGIC     ,ap.ClientStructureSnapshotId
# MAGIC FROM AllParties ap
# MAGIC
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     ap.ClientGcobId,
# MAGIC     ap.ClientCaseId,
# MAGIC     ap.ClientFullLegalName,
# MAGIC     ap.GcobId,
# MAGIC     ap.ParentIdentityName,
# MAGIC     ap.ClientType,
# MAGIC     ap.ClientLifeCycleName,
# MAGIC     ap.PartyType,
# MAGIC     ap.CitizenshipIsoCode AS CountryIsoCode,
# MAGIC     'Citizenship' AS CountryType,
# MAGIC     ap.Citizenship AS CountryName,
# MAGIC     ap.UniqueParentPartyId as UniquePartyId,
# MAGIC     ap.SourceSystem,
# MAGIC     date_format(current_date(), 'dd-MM-yyyy') AS LastLoadDate
# MAGIC     ,ap.ClientStructureSnapshotId
# MAGIC FROM AllParties ap
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Get the ClientStructureSnapshotIdflag
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW allCountrydetailsfinal AS
# MAGIC SELECT distinct  *,
# MAGIC        CASE WHEN ClientStructureSnapshotId = MAX(ClientStructureSnapshotId) OVER (PARTITION BY UniquePartyId,ClientGcobId)
# MAGIC             THEN 1 ELSE 0 END AS ClientStructureSnapshotIdflag
# MAGIC FROM allCountrydetails

# COMMAND ----------

# DBTITLE 1,Write data into Catalog
#df_allCountrydetails = spark.table('allCountrydetailsfinal')
#save_to_saradar_storage_account(df_allCountrydetails, SanctionCountryScreeningNAW_dataobject)

# COMMAND ----------

# DBTITLE 1,Static SanctionCountryList from manual Excel

#This static view can be replaced with reading from excel which is not available for now at any specific storage and no connectivity with any network
#Rabobank Sanctioned Country List  :   You filter on the field -  ISO and LANDNAAM
#Restricted and Blocked Locations (OFAC) - You filter on the field - NAME  and ALIAS. But you also filter  Entity Type =  1  only
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import to_date

data = [
("IQ", "iraq~irak~iraque~eraq~iraqi~irakien~irakienne~iraqui~iraakse~irakische~iraquiano~iracheno~irachana~irakli", "Rabobank Sanc Country List","2025-08-08","N"),
("SY", "syria~syrian~suriyyah~siria~suriya~syrienne~sirijskaja~syrie~syrien~suriye~syrisch~sirio~siriano", "Rabobank Sanc Country List","2025-08-08","N"),
("LB", "lebanon~liban~lubnan~libano~lebanese~lubnaniyyah~libanaise~libanesa~livan~livanskaja~libanesische~libanees~lubnanli", "Rabobank Sanc Country List","2025-08-08","N"),
("VE", "venezuela~venezuelien~venezuelienne~venezolano~venezolaans~venezolanische~venezuelano~venezuelali", "Rabobank Sanc Country List","2025-08-08","N"),
("UA", "l""ukraine~oekraien~oekraiener~oekraine~ucrania~ucraniana~ucraniano~ukraine~ukrainian~ukrainische~ukrajina~ukrayna~ukraynali", "Rabobank Sanc Country List","2025-08-08","N"),
("BY", "belarus~bielarus~wit rusland~bielorrusia~bielorusso~belarussisch~belaruslu", "Rabobank Sanc Country List","2025-08-08","N"),
("ZW", "zimbabwe;zimbabwean;zimbabweene;zimbabuense;zimbabwaanse;simbabwische;zimbabueano;zimbabwano;zimbabveli", "Rabobank Sanc Country List","2025-08-08","N"),
("LY", "libya~libye~libie~libia~libyen~libiya~livija~libyan~libyenne~libio~libische~libyerinnen~libyer~libica~libico~libyali", "Rabobank Sanc Country List","2025-08-08","N"),
("IR", "iran~irao~iranian~iranischen~iranienne~irani~iraans~iranische~iranli~persian", "Rabobank Sanc Country List","2025-08-08","N"),
("YE", "yemen~jemen~yaman~yamaniyyah~jemenskaja~yemeni~yemenite~jemenitische", "Rabobank Sanc Country List","2025-08-08","N"),
("CU", "cuba~kuba~cubaanse~cuban~cubain~kubanische~cubano~cubana~kubali", "Rabobank Sanc Country List","2025-08-08","N"),
("SS", "sudan~sudao~soudan~soedan~sudsudan~sudanese~sudanais~sudanaise~sursudanes~soedanees~sudsudanesische~sudanli", "Rabobank Sanc Country List","2025-08-08","N"),
("SD", "sudan~soudan~soedan~sudao~sudanese~soudanais~sudanes~soedanees~sudanesinnen~sudanli", "Rabobank Sanc Country List","2025-08-08","N"),
("KP", "choson~korea~coree~corea~korejskaja~nordkorea~coreia~kore~koreeanse~korean~coreen~coreenne~norcoreano~nordkoreanische~nordcoreana~koreli~dprk", "Rabobank Sanc Country List","2025-08-08","N"),
("CD", "congo~kongo~congolese~congolais~congoleno~kongolesische~kongolu~drc", "Rabobank Sanc Country List","2025-08-08","N"),
("MM", "myanmar~pyidaungzu thammada myanma naingngandaw~mjanma~burmese~birmanes~myanmarese~myanmarische~birmani~myanmarli~burma", "Rabobank Sanc Country List","2025-08-08","N"),
("CF", "central african~afrikaanse~centrafricana~centraal afrikaans~zentralafrikanische~afrika centrafricaine~africana~beafrika~centralnoafrikanskaja~afrikali", "Rabobank Sanc Country List","2025-08-08","N"),
("SO", "somalia~soomaaliya~sumal~sommalie~sommallienne~somalische~somalo~somalili", "Rabobank Sanc Country List","2025-08-08","N"),
("AF", "afghanistan~afeganistao~afganistan~afghaans~afganische~afegao~afgan", "Rabobank Sanc Country List","2025-08-08","N"),
("RU", "russian federation~ russia~ russische federatie~ federacion de rusia~ russie~ rossija~ rossijskaja federacija~ federazione russa~rusya federasnou~ russian~ rusland~ russe~ russo~russische", "Rabobank Sanc Country List","2025-08-08","N"),

("AF", "afghanistan~afeganistao~afganistan~afghaans~afganische~afegao~afgan","Rabobank Sanc Country List","2025-12-19","Y"),
("BY", "belarus~bielarus~wit rusland~bielorrusia~bielorusso~belarussisch~belaruslu","Rabobank Sanc Country List","2025-12-19","Y"),
("CD", "congo~kongo~congolese~congolais~congoleno~kongolesische~kongolu~drc","Rabobank Sanc Country List","2025-12-19","Y"),
("CF", "central african~afrikaanse~centrafricana~centraal afrikaans~zentralafrikanische~afrika centrafricaine~africana~beafrika~centralnoafrikanskaja~afrikali","Rabobank Sanc Country List","2025-12-19","Y"),
("CU", "cuba~kuba~cubaanse~cuban~cubain~kubanische~cubano~cubana~kubali","Rabobank Sanc Country List","2025-12-19","Y"),
("IQ", "iraq~irak~iraque~eraq~iraqi~irakien~irakienne~iraqui~iraakse~irakische~iraquiano~iracheno~irachana~irakli","Rabobank Sanc Country List","2025-12-19","Y"),
("IR", "iran~irao~iranian~iranischen~iranienne~irani~iraans~iranische~iranli~persian","Rabobank Sanc Country List","2025-12-19","Y"),
("KP", "choson~korea~coree~corea~korejskaja~nordkorea~coreia~kore~koreeanse~korean~coreen~coreenne~norcoreano~nordkoreanische~nordcoreana~koreli~dprk","Rabobank Sanc Country List","2025-12-19","Y"),
("LB", "lebanon~liban~lubnan~libano~lebanese~lubnaniyyah~libanaise~libanesa~livan~livanskaja~libanesische~libanees~lubnanli","Rabobank Sanc Country List","2025-12-19","Y"),
("LY", "libya~libye~libie~libia~libyen~libiya~livija~libyan~libyenne~libio~libische~libyerinnen~libyer~libica~libico~libyali","Rabobank Sanc Country List","2025-12-19","Y"),
("MM", "myanmar~pyidaungzu thammada myanma naingngandaw~mjanma~burmese~birmanes~myanmarese~myanmarische~birmani~myanmarli~burma","Rabobank Sanc Country List","2025-12-19","Y"),
("RU", "russian federation~ russia~ russische federatie~ federacion de rusia~ russie~ rossija~ rossijskaja federacija~ federazione russa~rusya federasnou~ russian~ rusland~ russe~ russo~russische","Rabobank Sanc Country List","2025-12-19","Y"),
("SD", "sudan~soudan~soedan~sudao~sudanese~soudanais~sudanes~soedanees~sudanesinnen~sudanli","Rabobank Sanc Country List","2025-12-19","Y"),
("SO", "somalia~soomaaliya~sumal~sommalie~sommallienne~somalische~somalo~somalili","Rabobank Sanc Country List","2025-12-19","Y"),
("SS", "sudan~sudao~soudan~soedan~sudsudan~sudanese~sudanais~sudanaise~sursudanes~soedanees~sudsudanesische~sudanli","Rabobank Sanc Country List","2025-12-19","Y"),
("SY", "syria~syrian~suriyyah~siria~suriya~syrienne~sirijskaja~syrie~syrien~suriye~syrisch~sirio~siriano","Rabobank Sanc Country List","2025-12-19","Y"),
("UA", "l'ukraine~oekraien~oekraiener~oekraine~ucrania~ucraniana~ucraniano~ukraine~ukrainian~ukrainische~ukrajina~ukrayna~ukraynali","Rabobank Sanc Country List","2025-12-19","Y"),
("VE", "venezuela~venezuelien~venezuelienne~venezolano~venezolaans~venezolanische~venezuelano~venezuelali","Rabobank Sanc Country List","2025-12-19","Y"),
("YE", "yemen~jemen~yaman~yamaniyyah~jemenskaja~yemeni~yemenite~jemenitische","Rabobank Sanc Country List","2025-12-19","Y"),
("ZW", "zimbabwe;zimbabwean;zimbabweene;zimbabuense;zimbabwaanse;simbabwische;zimbabueano;zimbabwano;zimbabveli","Rabobank Sanc Country List","2025-12-19","Y"),

("Unavailable", "korea democratic people's republic", "RBL List","2025-08-08","N"),
("Unavailable", "syria", "RBL List","2025-08-08","N"),
("Unavailable", "russia", "RBL List","2025-08-08","N"),
("Unavailable", "iran", "RBL List","2025-08-08","N"),
("Unavailable", "crimea", "RBL List","2025-08-08","N"),
("Unavailable", "luhansk people's republic", "RBL List","2025-08-08","N"),
("Unavailable", "donetsk people's republic", "RBL List","2025-08-08","N"),
("Unavailable", "cuba", "RBL List","2025-08-08","N"),

('Unavailable',"korea democratic people's republic", "RBL List","2025-12-19","Y"),
('Unavailable',"cuba", "RBL List","2025-12-19","Y"),
('Unavailable',"iran", "RBL List","2025-12-19","Y"),
('Unavailable',"donetsk people's republic", "RBL List","2025-12-19","Y"),
('Unavailable',"luhansk people's republic", "RBL List","2025-12-19","Y"),
('Unavailable',"russia", "RBL List","2025-12-19","Y")
]

schema = StructType([
    StructField("ISO", StringType(), True),
    StructField("LANDNAAM", StringType(), True),
    StructField("TabName", StringType(), True),
    StructField("FileDate", StringType(), True),
    StructField("ActiveFlag", StringType(), True),
])

df = spark.createDataFrame(data, schema).withColumn("FileDate", to_date("FileDate"))
df.createOrReplaceTempView("SanctionCountryList")


# COMMAND ----------

# DBTITLE 1,Get Country Name per line item based on delimiter "~"
# MAGIC %sql
# MAGIC -- Create a temp view that explodes LANDNAAM into individual names
# MAGIC CREATE OR REPLACE TEMP VIEW ExplodedSanctionCountryList AS
# MAGIC SELECT 
# MAGIC   ISO,
# MAGIC   name AS LANDNAAM_PART,
# MAGIC   LANDNAAM,
# MAGIC   TabName,
# MAGIC   FileDate,
# MAGIC   ActiveFlag
# MAGIC FROM 
# MAGIC   SanctionCountryList
# MAGIC LATERAL VIEW explode(split(LANDNAAM, '~')) AS name;

# COMMAND ----------

# DBTITLE 1,Match CountryName with Static list to get final view
# MAGIC %sql
# MAGIC -- Now match CountryName against each LANDNAAM_PART
# MAGIC CREATE OR REPLACE TEMP VIEW Matched_SanctionCountryList AS
# MAGIC with Country_Match_RSC as
# MAGIC (
# MAGIC SELECT distinct
# MAGIC a.ClientGcobId
# MAGIC ,a.ClientCaseId as CaseId
# MAGIC ,a.ClientFullLegalName
# MAGIC ,a.ClientType
# MAGIC ,a.ClientLifeCycleName
# MAGIC ,CASE WHEN a.GcobId IS NULL THEN a.clientgcobid ELSE a.GcobId END AS GcobId
# MAGIC ,CASE WHEN a.ParentIdentityName IS NULL THEN a.ClientFullLegalName ELSE a.ParentIdentityName END AS PartyFullLegalName
# MAGIC ,a.UniquePartyId as UniquePartyId
# MAGIC ,CASE WHEN a.PartyType IS NULL THEN a.ClientType ELSE a.PartyType END AS PartyType 
# MAGIC ,a.CountryType
# MAGIC ,a.CountryName
# MAGIC ,a.SourceSystem
# MAGIC ,e.TabName as ListName_HitBelongTo
# MAGIC ,a.LastLoadDate as AlertDate
# MAGIC ,'100%' as PercentageScreening
# MAGIC ,CASE WHEN e.LANDNAAM_PART IS NOT NULL THEN 'Match Found' ELSE 'No Match' END AS MatchStatus
# MAGIC ,e.LANDNAAM
# MAGIC ,date_format(current_date()-1, 'dd-MM-yyyy') AS BusinessDate
# MAGIC ,a.LastLoadDate as RefreshDate
# MAGIC FROM allCountrydetailsfinal a
# MAGIC LEFT JOIN ExplodedSanctionCountryList e
# MAGIC ON LOWER(a.CountryName) = LOWER(e.LANDNAAM_PART) OR LOWER(a.CountryIsoCode) = LOWER(e.ISO)
# MAGIC where e.TabName = 'Rabobank Sanc Country List' AND a.ClientStructureSnapshotIdflag=1 and e.ActiveFlag = 'Y'
# MAGIC )
# MAGIC ,
# MAGIC Country_Match_RBL as
# MAGIC (
# MAGIC SELECT distinct
# MAGIC a.ClientGcobId
# MAGIC ,a.ClientCaseId as CaseId
# MAGIC ,a.ClientFullLegalName
# MAGIC ,a.ClientType
# MAGIC ,a.ClientLifeCycleName
# MAGIC ,CASE WHEN a.GcobId IS NULL THEN a.clientgcobid ELSE a.GcobId END AS GcobId
# MAGIC ,CASE WHEN a.ParentIdentityName IS NULL THEN a.ClientFullLegalName ELSE a.ParentIdentityName END AS PartyFullLegalName
# MAGIC ,a.UniquePartyId as UniquePartyId
# MAGIC ,CASE WHEN a.PartyType IS NULL THEN a.ClientType ELSE a.PartyType END AS PartyType 
# MAGIC ,a.CountryType
# MAGIC ,a.CountryName
# MAGIC ,a.SourceSystem
# MAGIC ,e.TabName as ListName_HitBelongTo
# MAGIC ,a.LastLoadDate as AlertDate
# MAGIC ,'100%' as PercentageScreening
# MAGIC ,CASE WHEN e.LANDNAAM_PART IS NOT NULL THEN 'Match Found' ELSE 'No Match' END AS MatchStatus
# MAGIC ,e.LANDNAAM
# MAGIC ,date_format(current_date()-1, 'dd-MM-yyyy') AS BusinessDate
# MAGIC ,a.LastLoadDate as RefreshDate
# MAGIC FROM allCountrydetailsfinal a
# MAGIC LEFT JOIN ExplodedSanctionCountryList e
# MAGIC ON LOWER(a.CountryName) = LOWER(e.LANDNAAM_PART) 
# MAGIC where e.TabName = 'RBL List' AND a.ClientStructureSnapshotIdflag=1 and e.ActiveFlag = 'Y'
# MAGIC )
# MAGIC select * from Country_Match_RSC where MatchStatus = 'Match Found'
# MAGIC union
# MAGIC select * from Country_Match_RBL where MatchStatus = 'Match Found'

# COMMAND ----------

# DBTITLE 1,write & Overwrite matched data in storage & Catalog
df_Matched_SanctionCountryList = spark.table('Matched_SanctionCountryList')
save_to_saradar_storage_account(df_Matched_SanctionCountryList, MatchedSanctionCountry_dataobject)

# COMMAND ----------

# DBTITLE 1,Compare Histroy data with current data for new clients hit
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Matched_SanctionCountryListfinal AS
# MAGIC SELECT distinct
# MAGIC ms.ClientGcobId
# MAGIC ,ms.CaseId
# MAGIC ,ms.ClientFullLegalName
# MAGIC ,ms.ClientType
# MAGIC ,ms.ClientLifeCycleName
# MAGIC ,ms.GcobId
# MAGIC ,ms.PartyFullLegalName
# MAGIC ,ms.UniquePartyId
# MAGIC ,ms.PartyType 
# MAGIC ,ms.CountryType
# MAGIC ,ms.CountryName
# MAGIC ,ms.SourceSystem
# MAGIC ,ms.ListName_HitBelongTo
# MAGIC ,ms.PercentageScreening
# MAGIC ,ms.MatchStatus
# MAGIC ,ms.LANDNAAM
# MAGIC ,ms.BusinessDate
# MAGIC ,ms.RefreshDate
# MAGIC ,CASE WHEN sc.UniquePartyId IS NULL THEN 1 ELSE 0 END AS NewParty
# MAGIC ,CASE WHEN sc.UniquePartyId IS NULL THEN ms.AlertDate ELSE sc.AlertDate END AS AlertDate
# MAGIC FROM Matched_SanctionCountryList ms
# MAGIC LEFT JOIN radar.SanctionCountryList sc
# MAGIC   ON sc.UniquePartyId = ms.UniquePartyId
# MAGIC

# COMMAND ----------

# DBTITLE 1,Append data in catalog table
# MAGIC %sql
# MAGIC --Create Catalog table if it does not exist
# MAGIC CREATE TABLE IF NOT EXISTS radar.SanctionCountryList (
# MAGIC ClientGcobId int,
# MAGIC CaseId int,
# MAGIC ClientFullLegalName string,
# MAGIC ClientType string,
# MAGIC ClientLifeCycleName string,
# MAGIC GcobId string,
# MAGIC PartyFullLegalName string,
# MAGIC UniquePartyId string,
# MAGIC PartyType string,
# MAGIC CountryType string,
# MAGIC CountryName string,
# MAGIC SourceSystem string,
# MAGIC ListName_HitBelongTo string,
# MAGIC PercentageScreening string,
# MAGIC MatchStatus string,
# MAGIC LANDNAAM string,
# MAGIC BusinessDate string,
# MAGIC RefreshDate string,
# MAGIC NewParty int,
# MAGIC AlertDate string
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- -- --Delete data for catalog table if current date data is already available
# MAGIC delete from radar.SanctionCountryList where RefreshDate = (select max(LastLoadDate) from allCountrydetailsfinal);
# MAGIC
# MAGIC --Insert and Append data in catalog table
# MAGIC INSERT INTO radar.SanctionCountryList
# MAGIC SELECT * FROM Matched_SanctionCountryListfinal

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS radar.Derived_SanctionCountryList;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Dr_SanctionCountryList AS
# MAGIC SELECT *
# MAGIC FROM radar.SanctionCountryList
# MAGIC WHERE TO_DATE(RefreshDate, 'dd-MM-yyyy') BETWEEN date_sub(current_date(), 1) AND current_date();
# MAGIC

# COMMAND ----------

spark.sql('select * from Dr_SanctionCountryList').write.mode('overwrite').saveAsTable('radar.Derived_SanctionCountryList')
