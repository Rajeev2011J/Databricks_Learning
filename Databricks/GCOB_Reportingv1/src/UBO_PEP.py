# Databricks notebook source
# MAGIC %md
# MAGIC ## UBO PEP REPORT
# MAGIC Goal: To extract the PEP and UBO Status of NP/NPPC/RNP Related parties and their static information.
# MAGIC  
# MAGIC ###### <span style="color:orange">Authors:</span>
# MAGIC 1.  Mahalakshmi.Vengateswaran@rabobank.com
# MAGIC  
# MAGIC ###### <span style="color:blue">Flow of the logic:</span>
# MAGIC GCOB:
# MAGIC   1. Get client structure (party_client_structure_GUI)
# MAGIC   2. Get GlobalClientOwner Details (party_case_client_details)
# MAGIC   3. Get details of Addresses, Nationality, Citizenship, TIN details for Related Parties' in structure (party_AllPartyDetails)
# MAGIC
# MAGIC LEGACY2:
# MAGIC   1. Get client structure (Legacy2_ClientStructure)
# MAGIC   2. Get GlobalClientOwner Details (Legacy2_case_client_details)
# MAGIC   3. Get details of Addresses, Nationality, Citizenship, TIN details for Related Parties' in structure (Legacy2_Address, Legacy2_Nationality_Citizenship, Legacy2_ClientIdentification, Legacy2_tax_info)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Processing and Configuration Steps
# MAGIC

# COMMAND ----------

# DBTITLE 1,Importing packages
import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

UBO_PEP_Report_dataobject = 'RelatedNaturalPersonParty_UBO_PEP_Report'

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# DBTITLE 1,Reading GCOB GDP Defined data objects
# Reading dataobjects from the GDP defined layer and creating temporary views for each dataobject
load_df = [
            "party_client_structure_GUI",
            "party_case_client_details",
            "party_AllPartyDetails",
]

for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# DBTITLE 1,Reading Legacy2 GDP Defined data objects
# Reading dataobjects from the GDP defined layer and creating temporary views for each dataobject
load_df = [
            "Legacy2_ClientStructure",
            "Legacy2_case_client_details",
            "Legacy2_tax_info",
            "Legacy2_Nationality_Citizenship",
            "Legacy2_ClientIdentification",
            "Legacy2_Address",
        ]

for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation for SourceSystem GCOB

# COMMAND ----------

# DBTITLE 1,Handling Multiple Nationalities and Citizenships for Related Parties
# MAGIC %sql
# MAGIC -- Since Related Parties can have multiple Nationality and Citizenship , this query concatinates distinct Nationality and Citizenship using a comma and groups them by PartyId where Status is Snapshot
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_NP_RNPP_Nationality_Citizenship AS
# MAGIC SELECT DISTINCT
# MAGIC   PartyId,
# MAGIC   UniquePartyId,
# MAGIC   array_join(Collect_Set(Nationality), ', ') as Nationality,
# MAGIC   array_join(Collect_Set(Citizenship), ', ') AS Citizenship,
# MAGIC   Status
# MAGIC FROM
# MAGIC   party_AllPartyDetails
# MAGIC GROUP BY
# MAGIC   PartyId,
# MAGIC   UniquePartyId,
# MAGIC   Status
# MAGIC having
# MAGIC   Status = 'Snapshot'

# COMMAND ----------

# DBTITLE 1,Deriving PEP and UBO Status Deriving PEP, UBO Status, and Potentially Required Related Attributes for PartiesParties in Client Structure
# MAGIC %sql
# MAGIC -- Code to derive the PEP Status and UBO Status of NP/NPC/RNP Parties within the Client Structure for SourceSystem GCOB
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_Party_UBO_PEP_Report AS
# MAGIC SELECT DISTINCT
# MAGIC   cs.SourceSystem,
# MAGIC   cs.ClientGcobId AS GcobId,
# MAGIC   cs.ClientCaseId AS CaseId,
# MAGIC   cs.ClientFullLegalName AS FullLegalName,
# MAGIC   cs.ClientLifeCycleName AS ClientLifecycleStatusType,
# MAGIC   cs.CaseStatusName AS CaseStatusType,
# MAGIC   cs.ReviewTypeName AS CaseReviewType,
# MAGIC   cs.IsLatestApprovedVersionOfClient,
# MAGIC   cs.FIHubIndicator,
# MAGIC   cs.ClientCountryOfRegistration AS CountryOfRegistration,
# MAGIC   cs.ClientCountryOfOperation AS CountryOfOperations,
# MAGIC   pccd.CddType,
# MAGIC   pccd.GlobalClientOwner,
# MAGIC   pccd.GlobalClientOwnerLocation,
# MAGIC   pccd.BusinessLineName,
# MAGIC   cs.ParentIdentity AS PartyGCOBID,
# MAGIC   cs.ParentIdentityName AS PartyFullName,
# MAGIC   cs.IsUbo,
# MAGIC   -- boolean(CASE WHEN cs.IsUbo IS NOT NULL THEN cs.IsUbo ELSE 'false' END) AS IsUbo,
# MAGIC   CASE
# MAGIC     WHEN cs.ClientGcobId = cs.ChildIdentity and 
# MAGIC     cs.ParentType IN ('RelatedNaturalPerson','NaturalPersonClient') THEN true
# MAGIC     ELSE false
# MAGIC   END AS IsDirectlyRelatedToClient,
# MAGIC   cs.ParentPEP AS PartyPEPStatus,
# MAGIC   papd.DateOfBirth,
# MAGIC   papd.RegisteredNumber AS ResidentialNumber,
# MAGIC   papd.RegisteredStreet AS ResidentialStreet,
# MAGIC   papd.RegisteredCity AS ResidentialCity,
# MAGIC   papd.RegisteredRegion AS ResidentialRegion,
# MAGIC   papd.RegisteredPostalCode AS ResidentialPostalCode,
# MAGIC   papd.RegisteredCountryName AS ResidentialCountry,
# MAGIC   apdnatciti.Nationality AS CountriesOfNationality,
# MAGIC   apdnatciti.Citizenship AS CountriesOfCitizenship,
# MAGIC   cs.UboReason AS UboThroughReason,
# MAGIC   cs.CalculatedShareholdingPercentage AS ShareholdingPercentage,
# MAGIC   papd.CountryOfTaxResidence,
# MAGIC   papd.TinAvailable,
# MAGIC   papd.TinOrEquivalent,
# MAGIC   papd.TinUnavailabilityReason AS MissingTinReason,
# MAGIC   cs.SourceClient,
# MAGIC   case
# MAGIC     when
# MAGIC       cs.SourceClient like 'LEC%'
# MAGIC       and cs.SourceSystem = 'GCOB'
# MAGIC     then
# MAGIC       'GCOB_LegalEntity'
# MAGIC     when
# MAGIC       cs.SourceClient like 'NP%'
# MAGIC       and cs.SourceSystem = 'GCOB'
# MAGIC     then
# MAGIC       'GCOB_NP-NPPC'
# MAGIC   end as SourceSystemReference
# MAGIC FROM
# MAGIC   party_client_structure_GUI AS cs
# MAGIC     LEFT OUTER JOIN party_case_client_details pccd
# MAGIC       ON cs.SourceClient = pccd.SourceClient
# MAGIC     LEFT OUTER JOIN party_AllPartyDetails papd
# MAGIC       on papd.PartyId = cs.ParentEntityId
# MAGIC       and cs.UniqueParentPartyId = papd.UniquePartyId
# MAGIC       AND papd.Status = 'Snapshot'
# MAGIC     LEFT OUTER JOIN GCOB_NP_RNPP_Nationality_Citizenship apdnatciti
# MAGIC       on apdnatciti.PartyId = cs.ParentEntityId
# MAGIC       and cs.UniqueParentPartyId = apdnatciti.UniquePartyId
# MAGIC WHERE
# MAGIC   cs.ParentType like '%Natural%'
# MAGIC   AND cs.CaseStatusName = 'Completed'
# MAGIC   AND cs.IsLatestApprovedVersionOfClient = true
# MAGIC qualify row_number() over(partition by cs.ClientGcobId, cs.ClientCaseId, cs.ParentIdentity order by IsDirectlyRelatedToClient desc) = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation for SourceSystem LEGACY2

# COMMAND ----------

# DBTITLE 1,Fetching NonSFDCID_Legacy2_Clients
# MAGIC %sql
# MAGIC --Fetching NonSFDCID_Legacy2_Clients
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%';
# MAGIC

# COMMAND ----------

# DBTITLE 1,Filtering out NonSFDCID_Legacy2_Clients
# MAGIC %sql
# MAGIC --Filtering out NonSFDCID_Legacy2_Clients
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Legacy2_case_client_details
# MAGIC where
# MAGIC   ClientId not in (
# MAGIC     select
# MAGIC       ClientId
# MAGIC     from
# MAGIC       NonSFDCID_Legacy2_Client
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,Handling Multiple Nationalities and Citizenships for Related Parties
# MAGIC %sql
# MAGIC -- Since Related Parties can have multiple Nationality and Citizenship , this query concatinates distinct Nationality and Citizenship using a comma and groups them by ClientId
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_NP_RNPP_Nationality_Citizenship AS
# MAGIC SELECT DISTINCT
# MAGIC   ClientId,
# MAGIC   array_join(Collect_Set(ClientNationality), ', ') as CountriesOfNationality,
# MAGIC   array_join(Collect_Set(ClientCitizenship), ', ') AS CountriesOfCitizenship
# MAGIC FROM
# MAGIC   Legacy2_Nationality_Citizenship
# MAGIC GROUP BY
# MAGIC   ClientId

# COMMAND ----------

# DBTITLE 1,Deriving PEP and UBO Status Deriving PEP, UBO Status, and Potentially Required Related Attributes for PartiesParties in Client Structure
# MAGIC %sql
# MAGIC -- Code to derive the PEP Status and UBO Status of NP/NPC/RNP Parties within the Client Structure for SourceSystem Legacy2
# MAGIC CREATE 
# MAGIC OR REPLACE TEMPORARY VIEW Legacy2_Party_UBO_PEP_Report AS
# MAGIC SELECT DISTINCT
# MAGIC   C.SourceSystem,
# MAGIC   lcs.ClientGcobId AS GcobId,
# MAGIC   null AS CaseId,
# MAGIC   lcs.ClientFullLegalName AS FullLegalName,
# MAGIC   C.ClientLifeCycleName as ClientLifecycleStatusType,
# MAGIC   C.StatusTypeName AS CaseStatusType,
# MAGIC   C.ReviewTypeName AS CaseReviewType,
# MAGIC   C.IslatestApprovedVersionOfClient,
# MAGIC   null as FIHubIndicator,
# MAGIC   C.RegisteredCountry AS CountryOfRegistration,
# MAGIC   C.OperationalCountry AS CountryOfOperations,
# MAGIC   C.CddType,
# MAGIC   C.GlobalClientOwner,
# MAGIC   C.GlobalClientOwnerLocation,
# MAGIC   C.BusinessLineName,
# MAGIC   lcs.ParentIdentity AS PartyGCOBID,
# MAGIC   lcs.ParentIdentityName AS PartyFullName,
# MAGIC   boolean(CASE WHEN lcs.IsUbo IS NOT NULL THEN lcs.IsUbo ELSE 'false' END) AS IsUbo,
# MAGIC   -- boolean(lcs.IsUbo),
# MAGIC    CASE
# MAGIC     WHEN lcs.ClientGcobId = lcs.ChildIdentity and lcs.ParentType like '%Natural%' THEN true
# MAGIC     ELSE false
# MAGIC   END AS IsDirectlyRelatedToClient,
# MAGIC   lcs.ParentPEP AS PartyPEPStatus,
# MAGIC   ClIdentity.ContactDateOfBirth as DateOfBirth,
# MAGIC   addr.RegisteredAddressNumber as ResidentialNumber,
# MAGIC   addr.RegisteredStreet as ResidentialStreet,
# MAGIC   addr.RegisteredCity as ResidentialCity,
# MAGIC   addr.RegisteredRegion as ResidentialRegion,
# MAGIC   addr.RegisteredPostCode as ResidentialPostalCode,
# MAGIC   addr.RegisteredCountry as ResidentialCountry,
# MAGIC   Nty.CountriesOfNationality,
# MAGIC   Nty.CountriesOfCitizenship,
# MAGIC   lcs.UboThroughReason,
# MAGIC   lcs.Shareholding AS ShareholdingPercentage,
# MAGIC   tax.CountryOfTaxResidencies as CountryOfTaxResidence,
# MAGIC   tax.TinAvailable,
# MAGIC   tax.TinOrEquivalent,
# MAGIC   tax.Explanation AS MissingTinReason,
# MAGIC    case when C.IsClient = 'true' and C.ClientTypeId = 1 then concat('L2_LEC_',C.ClientId)
# MAGIC       when C.IsClient = 'true' and C.ClientTypeId in (2,3) then concat('L2_NP_NPPC_',C.ClientId)
# MAGIC   end AS SourceClient
# MAGIC FROM
# MAGIC   Legacy2_ClientStructure lcs
# MAGIC   LEFT JOIN Legacy2_Address addr ON addr.ClientId =lcs.ParentIdentity
# MAGIC   LEFT JOIN Legacy2_NP_RNPP_Nationality_Citizenship nty ON lcs.ParentIdentity = nty.ClientId
# MAGIC   LEFT JOIN Legacy2_ClientIdentification ClIdentity ON lcs.ParentIdentity = ClIdentity.Id
# MAGIC   LEFT JOIN Legacy2_tax_info tax ON lcs.ParentIdentity = tax.ClientId
# MAGIC   INNER JOIN Legacy2_client C on C.ClientId = lcs.ClientId
# MAGIC where
# MAGIC   lcs.ParentType like '%Natural%'
# MAGIC   AND C.IslatestApprovedVersionOfClient = True 
# MAGIC   AND C.StatusTypeName = 'Completed'
# MAGIC qualify row_number() over(partition by lcs.ClientGcobId, lcs.ParentIdentity order by IsDirectlyRelatedToClient desc) = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparing Final View

# COMMAND ----------

# DBTITLE 1,Combining GCOB and Legacy2
# MAGIC %sql
# MAGIC --This query creates a temporary view combining data from GCOB and Legacy2 reports
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Party_UBO_PEP_Report AS
# MAGIC select * from GCOB_Party_UBO_PEP_Report
# MAGIC UNION ALL
# MAGIC Select *, case when substr(SourceClient,0,6) = 'L2_LEC' then 'Legacy2_LegalEntity'
# MAGIC       when substr(SourceClient,0,5) = 'L2_NP' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference from Legacy2_Party_UBO_PEP_Report

# COMMAND ----------

# DBTITLE 1,Preparation of Final Temporary View
# Creating a dataframe from Tempview
df_RelatedNaturalPersonParty_UBO_PEP_Report=spark.table('Party_UBO_PEP_Report')
# Adding additional column BusinessDate to the dataframe 
df_RelatedNaturalPersonParty_UBO_PEP_Report = df_RelatedNaturalPersonParty_UBO_PEP_Report.withColumn("BusinessDate",lit(BusinessDate))
# Creating a Tempview from dataframe
# df_RelatedNaturalPersonParty_UBO_PEP_Report.createOrReplaceTempView('Final_RelatedNaturalPersonParty_UBO_PEP_Report')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Data into Catalog

# COMMAND ----------

save_to_saradar_storage_account(df_RelatedNaturalPersonParty_UBO_PEP_Report, UBO_PEP_Report_dataobject)

# COMMAND ----------

# DBTITLE 1,Dropping Existing Table
# %sql
# -- Dropping the table if it already exists
# DROP TABLE IF EXISTS radar.RelatedNaturalPersonParty_UBO_PEP_Report

# COMMAND ----------

# DBTITLE 1,Writing Report to Catalog
# # Writing report as a table to catalog
# spark.sql("SELECT * FROM Final_RelatedNaturalPersonParty_UBO_PEP_Report").write.mode("overwrite").saveAsTable("radar.RelatedNaturalPersonParty_UBO_PEP_Report")

