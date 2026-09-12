# Databricks notebook source
# MAGIC %md
# MAGIC # Configurations and Data Ingestion

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

'''
app_reg_app_id_dev = '5864572e-dc77-4105-b900-4f72ab4b0cd8'
app_reg_app_id_prep = '1ad6fac9-ea83-41de-9152-f455e2bf9f60'
app_reg_app_id_prd = ''
ReadStorage = 'edlcorestdeuprod0001'
TenantId = '6e93a626-8aca-4dc1-9191-ce291b4b75a1'
'''

# COMMAND ----------

allrelationship_dataobject = 'AllRelationships'
UniqueID_DQ_dataobject = 'UniqueID_DQ'

# COMMAND ----------

# DBTITLE 1,Outh2 Configuration
ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details',
'party_client_structure_GUI',
'party_AllPartyDetails',
'party_RelatedPartyIdentificationDocument',
'party_RelatedPartyParentAddresses',
'party_uniqueID_DQ'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Legacy2_ClientStructure',
'Legacy2_case_client_details',
'Legacy2_GCOB_ApprovedVersion',
'Legacy2_trade_name',
'Legacy2_tax_info',
'Legacy2_Nationality_Citizenship',
'Legacy2_GCOB_ApprovedVersion',
'Legacy2_ClientIdentification'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Transformation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_IdentificationDocument AS
# MAGIC SELECT DISTINCT
# MAGIC PartyId,
# MAGIC Id,
# MAGIC Gcobid,
# MAGIC CASE WHEN Clienttype='LegalEntityClient' THEN 'Legal Entity'
# MAGIC      WHEN Clienttype='RelatedLegalEntity' THEN 'Related Legal Entity'
# MAGIC      WHEN Clienttype='NaturalPersonClient' THEN 'Natural Person'
# MAGIC      WHEN Clienttype='RelatedNaturalPerson' THEN 'Related Natural Person'
# MAGIC      ELSE Clienttype END AS Clienttype,
# MAGIC UniquePartyId,
# MAGIC IdType,
# MAGIC IdNumber,
# MAGIC ExpiryDate
# MAGIC FROM party_RelatedPartyIdentificationDocument 
# MAGIC WHERE IdType IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_Addresses AS
# MAGIC
# MAGIC WITH Allparty_Snapshot AS (
# MAGIC SELECT PartyId
# MAGIC  , UniquePartyId
# MAGIC  , RegisteredStreet
# MAGIC  , RegisteredNumber
# MAGIC  , RegisteredPostalCode
# MAGIC  , RegisteredCity
# MAGIC  , RegisteredRegion
# MAGIC  , RegisteredCountryName
# MAGIC  , FullLegalNameInLocalLanguage
# MAGIC  , CountryOfTaxResidence
# MAGIC  , array_join(Collect_Set(Nationality), ', ') as Nationality
# MAGIC  , array_join(Collect_Set(Citizenship), ', ') AS Citizenship
# MAGIC  , DateOfBirth
# MAGIC FROM party_AllPartyDetails
# MAGIC WHERE Status = 'Snapshot'
# MAGIC GROUP BY ALL),
# MAGIC
# MAGIC RelatedParty_Details AS (
# MAGIC SELECT RelatedPartyKey AS PartyId
# MAGIC  , UniquePartyId
# MAGIC  , RegisteredStreet
# MAGIC  , RegisteredNumber
# MAGIC  , RegisteredPostalCode
# MAGIC  , RegisteredCity
# MAGIC  , RegisteredRegion
# MAGIC  , RegisteredCountryName
# MAGIC  , NULL AS FullLegalNameInLocalLanguage
# MAGIC  , NULL AS CountryOfTaxResidence
# MAGIC  , NULL AS Nationality
# MAGIC  , NULL AS Citizenship
# MAGIC  , NULL AS DateOfBirth
# MAGIC FROM party_RelatedPartyParentAddresses)
# MAGIC
# MAGIC SELECT 
# MAGIC     COALESCE(t1.PartyId, t2.PartyId) AS PartyId,
# MAGIC     COALESCE(t1.UniquePartyId, t2.UniquePartyId) AS UniquePartyId,
# MAGIC     COALESCE(t1.RegisteredStreet, t2.RegisteredStreet) AS RegisteredStreet,
# MAGIC     COALESCE(t1.RegisteredNumber, t2.RegisteredNumber) AS RegisteredNumber,
# MAGIC     COALESCE(t1.RegisteredPostalCode, t2.RegisteredPostalCode) AS RegisteredPostalCode,
# MAGIC     COALESCE(t1.RegisteredCity, t2.RegisteredCity) AS RegisteredCity,
# MAGIC     COALESCE(t1.RegisteredRegion, t2.RegisteredRegion) AS RegisteredRegion,
# MAGIC     COALESCE(t1.RegisteredCountryName, t2.RegisteredCountryName) AS RegisteredCountryName,
# MAGIC     COALESCE(t1.FullLegalNameInLocalLanguage, t2.FullLegalNameInLocalLanguage) AS FullLegalNameInLocalLanguage,
# MAGIC     COALESCE(t1.CountryOfTaxResidence, t2.CountryOfTaxResidence) AS CountryOfTaxResidence,
# MAGIC     COALESCE(t1.Nationality, t2.Nationality) AS Nationality,
# MAGIC     COALESCE(t1.Citizenship, t2.Citizenship) AS Citizenship,
# MAGIC     COALESCE(t1.DateOfBirth, t2.DateOfBirth) AS DateOfBirth
# MAGIC FROM Allparty_Snapshot AS t1
# MAGIC FULL OUTER JOIN RelatedParty_Details AS t2 
# MAGIC     ON t1.PartyId = t2.PartyId 
# MAGIC     AND t1.UniquePartyId = t2.UniquePartyId;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW relationshiptype AS
# MAGIC SELECT
# MAGIC     SourceClient,
# MAGIC     UniqueChildPartyId,
# MAGIC     UniqueParentPartyId,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Agent' THEN 'True' ELSE 'False' END) AS HasAgent,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Authorised Representative' THEN 'True' ELSE 'False' END) AS HasAuthorisedRepresentative,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Beneficiary' THEN 'True' ELSE 'False' END) AS HasBeneficiary,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'BranchOf' THEN 'True' ELSE 'False' END) AS HasBranchOf,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'CDD Rule Certifier' THEN 'True' ELSE 'False' END) AS HasCddRuleCertifier,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Directorship' THEN 'True' ELSE 'False' END) AS HasDirectorship,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Shareholding' THEN 'True' ELSE 'False' END) AS HasEndofChainShareholding,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'FundManager' THEN 'True' ELSE 'False' END) AS HasFundManagedBy,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Guarantor' THEN 'True' ELSE 'False' END) AS HasGuarantor,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Protector' THEN 'True' ELSE 'False' END) AS HasProtector,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Settlor/Founder' THEN 'True' ELSE 'False' END) AS HasSettlorFounder,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'SubAccountOf' THEN 'True' ELSE 'False' END) AS HasSubAccountOf,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Trustee' THEN 'True' ELSE 'False' END) AS HasTrustee,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'UBO' THEN 'True' ELSE 'False' END) AS HasUBO,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'UboThroughReasonShareholding' THEN 'True' ELSE 'False' END) AS HasUboThroughReasonShareholding,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'UboShareholding' THEN 'True' ELSE 'False' END) AS HasUboShareholding,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Integrator' THEN 'True' ELSE 'False' END) AS HasIntegrator,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Other' THEN 'True' ELSE 'False' END) AS HasOther,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'SpvParticipant' THEN 'True' ELSE 'False' END) AS HasSpvParticipant,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Certificate Holder' THEN 'True' ELSE 'False' END) AS HasCertificateHolder,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Limited Partner' THEN 'True' ELSE 'False' END) AS HasLimitedPartner,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'Originator' THEN 'True' ELSE 'False' END) AS HasOriginator,
# MAGIC     MAX(CASE WHEN TypesOfRelation = 'General Partner' THEN 'True' ELSE 'False' END) AS HasGeneralPartner
# MAGIC FROM
# MAGIC     party_client_structure_GUI
# MAGIC GROUP BY
# MAGIC     SourceClient,
# MAGIC     UniqueChildPartyId,
# MAGIC     UniqueParentPartyId

# COMMAND ----------

# DBTITLE 1,GCOB Relationship
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcoballrelationship AS
# MAGIC SELECT DISTINCT
# MAGIC     to_date(date_sub(current_date(), 1), 'yyyy-MM-dd') AS BusinessDate,		  
# MAGIC     cs.ClientCaseId AS CaseId,
# MAGIC     cs.ClientGcobid,
# MAGIC     cs.ClientFullLegalName AS FullLegalName,
# MAGIC     cs.CaseStatusName,
# MAGIC     cs.IsLatestApprovedVersionOfClient,
# MAGIC     cs.ReviewTypename AS CaseReview,
# MAGIC     cs.ClientLifeCycleName AS ClientLifeCycle,
# MAGIC     cs.SourceSystem,
# MAGIC     cs.ClientType,
# MAGIC     cs.MainClientPep,
# MAGIC     cs.SourceClient,
# MAGIC     case when cs.clienttype='Legal Entity' then concat('LE_', cs.ClientGcobid)
# MAGIC     when cs.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',cs.ClientGcobid) else 'NA' end as  UniqueGcobId,
# MAGIC     cl_id.IdType AS IdentificationTypeofClient,
# MAGIC     cl_id.IdNumber AS IdentificationNumberofClient,
# MAGIC     cl_id.ExpiryDate AS IdentificationExpiryDateofClient,
# MAGIC     cs.ChildIdentity AS ChildIdentity,
# MAGIC     cs.ChildIdentityName AS ChildFullLegalName,
# MAGIC     ch_adr.FullLegalNameInLocalLanguage AS ChildFullnameinlocallanguage,
# MAGIC     cs.ChildType AS ChildEntityType,
# MAGIC     cs.ChildPEP,
# MAGIC     ch_adr.DateOfBirth as DateOfBirthofChild,
# MAGIC     cs.ClientCountryOfRegistration AS CountryofRegistration,
# MAGIC     ch_adr.Citizenship AS CitizenshipofChild,	
# MAGIC     ch_adr.Nationality AS NationalityofChild,
# MAGIC     ch_adr.RegisteredStreet AS RegisteredStreetofChild,
# MAGIC     ch_adr.RegisteredNumber AS RegisteredNumberofChild,
# MAGIC     ch_adr.RegisteredPostalCode AS RegisteredPostalCodeofchild,
# MAGIC     ch_adr.RegisteredCity AS RegisteredCityofChild,
# MAGIC     ch_adr.RegisteredRegion AS RegisteredRegionofChild,
# MAGIC     ch_adr.RegisteredCountryName AS RegisteredCountryofChild,
# MAGIC     cs.ParentIdentity AS ParentIdentity,
# MAGIC     cs.ParentIdentityName AS ParentFullLegalName,
# MAGIC     pr_adr.FullLegalNameInLocalLanguage AS Fullnameinlocallanguage,
# MAGIC     cs.ParentType AS ParentEntityType,
# MAGIC     cs.ParentPEP,
# MAGIC     pr_adr.DateOfBirth as DateOfBirthofParent,
# MAGIC     pr_adr.CountryOfTaxResidence AS CountryoftaxresidenceofParent,	
# MAGIC 	pr_adr.Nationality  AS NationalityofParent,
# MAGIC 	pr_adr.Citizenship  AS CitizenshipofParent,	
# MAGIC     pr_adr.RegisteredStreet AS RegisteredStreetofParent,
# MAGIC     pr_adr.RegisteredNumber AS RegisteredNumberofParent,
# MAGIC     pr_adr.RegisteredPostalCode AS RegisteredPostalCodeofParent,
# MAGIC     pr_adr.RegisteredCity AS RegisteredCityofParent,
# MAGIC     pr_adr.RegisteredRegion AS RegisteredRegionofParent,
# MAGIC     pr_adr.RegisteredCountryName AS RegisteredCountryofParent,
# MAGIC     rp_id.IdType AS IdentificationTypeofParent,
# MAGIC     rp_id.IdNumber AS IdentificationNumberofParent,
# MAGIC     rp_id.ExpiryDate AS IdentificationExpiryDateofParent,
# MAGIC     cs.IsUbo,
# MAGIC     cs.CalculatedShareholdingPercentage,
# MAGIC     cs.CalculatedVotingRightsPercentage,
# MAGIC     cs.UboReason AS UboThroughReason,
# MAGIC     rt.HasAgent,
# MAGIC 	rt.HasAuthorisedRepresentative,
# MAGIC 	rt.HasBeneficiary,
# MAGIC 	rt.HasBranchOf,
# MAGIC 	rt.HasCddRuleCertifier,
# MAGIC 	rt.HasDirectorship,
# MAGIC 	rt.HasEndOfChainShareholding,
# MAGIC 	rt.HasFundManagedBy,	
# MAGIC 	rt.HasProtector,
# MAGIC 	rt.HasSettlorFounder,	
# MAGIC 	rt.HasSubAccountOf,	
# MAGIC 	rt.HasTrustee,
# MAGIC     rt.HasUBO,
# MAGIC     rt.HasUboThroughReasonShareholding,
# MAGIC 	rt.HasUboShareholding,
# MAGIC     rt.HasGuarantor,
# MAGIC     rt.HasIntegrator,
# MAGIC 	rt.HasOther,
# MAGIC     rt.HasSpvParticipant,
# MAGIC     rt.HasCertificateHolder,
# MAGIC     rt.HasLimitedPartner,
# MAGIC     rt.HasOriginator,
# MAGIC     rt.HasGeneralPartner,
# MAGIC     '' as HasControllingPerson
# MAGIC     
# MAGIC FROM party_client_structure_GUI AS cs
# MAGIC INNER JOIN relationshiptype AS rt
# MAGIC     ON rt.SourceClient = cs.SourceClient 
# MAGIC     AND rt.UniqueChildPartyId = cs.UniqueChildPartyId
# MAGIC     AND rt.UniqueParentPartyId = cs.UniqueParentPartyId
# MAGIC INNER JOIN party_case_client_details AS cd
# MAGIC     ON cd.SourceClient = cs.SourceClient
# MAGIC LEFT JOIN GCOB_Addresses AS ch_adr
# MAGIC     ON ch_adr.PartyId = cs.ChildEntityId
# MAGIC     AND ch_adr.UniquePartyId = cs.UniqueChildPartyId
# MAGIC LEFT JOIN GCOB_Addresses AS pr_adr
# MAGIC     ON pr_adr.PartyId = cs.ParentEntityId
# MAGIC     AND pr_adr.UniquePartyId = cs.UniqueParentPartyId
# MAGIC LEFT JOIN GCOB_IdentificationDocument AS cl_id
# MAGIC     ON cl_id.GcobId = cs.ClientGcobId 
# MAGIC     AND cl_id.UniquePartyId = case when cs.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',cs.ClientGcobid) else 'NA' end
# MAGIC LEFT JOIN GCOB_IdentificationDocument AS rp_id
# MAGIC     ON rp_id.UniquePartyId = cs.UniqueParentPartyId 
# MAGIC     AND rp_id.PartyId = cs.ParentEntityId;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Legacy2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_IdentificationDocument AS
# MAGIC SELECT DISTINCT
# MAGIC Id,
# MAGIC IdType,
# MAGIC ContactIdentificationNumber,
# MAGIC date_format(to_date(ContactIdExpiryDate, 'dd MMM yyyy'), 'dd-MM-yyyy') AS ContactIdExpiryDate
# MAGIC FROM Legacy2_ClientIdentification
# MAGIC WHERE IdType IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select * from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)

# COMMAND ----------

# DBTITLE 1,Legacy2 Relationship
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2allrelationships AS
# MAGIC SELECT DISTINCT
# MAGIC 	to_date(date_sub(current_date(), 1), 'yyyy-MM-dd') AS BusinessDate,
# MAGIC   '' AS CaseId,
# MAGIC 	cd.Gcobid AS ClientGcobid,
# MAGIC   cs.ClientFullLegalName AS FullLegalName,
# MAGIC 	cd.StatusTypeName AS CaseStatusName,
# MAGIC 	cd.IsLatestApprovedVersionOfClient,
# MAGIC 	cd.ReviewTypename AS CaseReview,
# MAGIC 	cd.ClientLifeCycleName AS ClientLifeCycle,
# MAGIC 	cd.SourceSystem,
# MAGIC 	cd.ClientType,
# MAGIC   cs.MainClientPep,
# MAGIC   case when cd.IsClient = 'true' and cd.ClientTypeId = 1 then concat('L2_LEC_',cd.ClientId)
# MAGIC       when cd.IsClient = 'true' and cd.ClientTypeId in (2,3) then concat('L2_NP_NPPC_',cd.ClientId)
# MAGIC       when cd.IsClient = 'false' and cd.ClientTypeId = 1 then concat('L2_RLEP_',cd.ClientId)
# MAGIC       when cd.IsClient = 'false' and cd.ClientTypeId in (2,3) then concat('L2_RNPP_',cd.ClientId)
# MAGIC   end AS SourceClient,
# MAGIC   case when cd.clienttype='Legal Entity' then concat('LE_', cd.Gcobid)
# MAGIC     when cd.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',cd.Gcobid) else 'NA' end AS  UniqueGcobId,
# MAGIC   cl_id.IdType AS IdentificationTypeofClient,
# MAGIC   cl_id.ContactIdentificationNumber AS IdentificationNumberofClient,
# MAGIC   cl_id.ContactIdExpiryDate AS IdentificationExpiryDateofClient,
# MAGIC   cs.ChildIdentity,
# MAGIC   cs.ChildIdentityName AS ChildFullLegalName,
# MAGIC   '' AS ChildFullnameinlocallanguage,
# MAGIC   cs.ChildClientType AS ChildEntityType,
# MAGIC   cs.ChildPep AS ChildPEP,
# MAGIC   CASE WHEN cs.ChildClientType NOT IN ('Legal Entity','Related Legal Entity') THEN date_format(to_date(cc.dateofbirth, 'dd MMM yyyy'), 'dd-MM-yyyy') END AS DateOfBirthofChild,
# MAGIC   '' AS CountryofRegistration, --get the value 
# MAGIC   nc.ClientCitizenship AS CitizenshipofChild,	
# MAGIC   nc.ClientNationality AS NationalityofChild,
# MAGIC   cc.RegisteredStreet AS RegisteredStreetofChild,
# MAGIC   cc.RegisteredAddressNumber AS RegisteredNumberofChild,
# MAGIC   cc.RegisteredPostCode AS RegisteredPostalCodeofchild,
# MAGIC   cc.RegisteredCity AS RegisteredCityofChild,
# MAGIC   cc.RegisteredRegion AS RegisteredRegionofChild,
# MAGIC   cc.RegisteredCountry AS RegisteredCountryofChild,
# MAGIC   cs.ParentIdentity,
# MAGIC   cs.ParentIdentityName AS ParentFullLegalName,
# MAGIC 	'' AS Fullnameinlocallanguage,	
# MAGIC   cs.ParentType AS ParentEntityType,
# MAGIC   cs.ParentPEP,
# MAGIC   CASE WHEN cs.ParentType NOT IN ('Legal Entity','Related Legal Entity') THEN date_format(to_date(cp.dateofbirth, 'dd MMM yyyy'), 'dd-MM-yyyy') END AS DateOfBirthofParent,
# MAGIC   ti.CountryOfTaxResidencies AS CountryoftaxresidenceofParent,	
# MAGIC 	nc.ClientNationality AS NationalityofParent,
# MAGIC 	nc.ClientCitizenship AS CitizenshipofParent,	
# MAGIC   cp.RegisteredStreet AS RegisteredStreetofParent,
# MAGIC   cp.RegisteredAddressNumber AS RegisteredNumberofParent,
# MAGIC   cp.RegisteredPostCode AS RegisteredPostalCodeofParent,
# MAGIC   cp.RegisteredCity AS RegisteredCityofParent,
# MAGIC   cp.RegisteredRegion AS RegisteredRegionofParent,
# MAGIC   cp.RegisteredCountry AS RegisteredCountryofParent,
# MAGIC   rp_id.IdType AS IdentificationTypeofParent,
# MAGIC   rp_id.ContactIdentificationNumber AS IdentificationNumberofParent,
# MAGIC   rp_id.ContactIdExpiryDate AS IdentificationExpiryDateofParent,
# MAGIC   cast(cs.IsUbo AS BOOLEAN) AS IsUbo,
# MAGIC   cs.Shareholding AS CalculatedShareholdingPercentage,
# MAGIC   '' AS CalculatedVotingRightsPercentage,
# MAGIC   cs.UboThroughReason,
# MAGIC   cs.HasAgent,
# MAGIC   cs.HasAuthorisedRepresentative,
# MAGIC 	cs.HasBeneficiary,
# MAGIC 	'' AS HasBranchOf,
# MAGIC 	'' AS HasCddRuleCertifier,
# MAGIC 	cs.HasDirector AS HasDirectorship,
# MAGIC 	cs.HasEndOfChainShareholding,
# MAGIC 	'' AS HasFundManagedBy,
# MAGIC 	'' AS HasProtector,
# MAGIC 	cs.HasSettlorFounder,
# MAGIC 	'' AS HasSubAccountOf,
# MAGIC 	cs.HasTrustee,
# MAGIC   '' AS HasUBO,
# MAGIC   '' AS HasUboThroughReasonShareholding,
# MAGIC 	cs.HasUboShareholding,
# MAGIC 	cs.HasGuarantor,
# MAGIC   cs.HasIntegrator,
# MAGIC   cs.HasOther,
# MAGIC 	'' AS HasSpvParticipant,
# MAGIC   '' AS HasCertificateHolder,
# MAGIC   '' AS HasLimitedPartner,
# MAGIC   '' AS HasOriginator,
# MAGIC   '' AS HasGeneralPartner,
# MAGIC 	cs.HasControllingPerson
# MAGIC
# MAGIC FROM Legacy2_ClientStructure AS cs
# MAGIC INNER JOIN Legacy2_client AS cd
# MAGIC   ON cd.ClientId = cs.ClientId    
# MAGIC LEFT JOIN Legacy2_GCOB_ApprovedVersion AS GCOB_TRUE
# MAGIC   ON cd.GcobCaseId = GCOB_TRUE.CaseId
# MAGIC Left JOIN Legacy2_client AS cp
# MAGIC   ON cp.FullLegalname=cs.ParentIdentityName
# MAGIC Left JOIN  Legacy2_client AS cc
# MAGIC   ON cc.gcobid = cs.ChildIdentity
# MAGIC   AND cc.FullLegalname = cs.ChildIdentityName
# MAGIC LEFT JOIN Legacy2_Nationality_Citizenship AS nc 
# MAGIC   ON nc.ClientId = cs.clientid
# MAGIC LEFT JOIN Legacy2_tax_info AS ti 
# MAGIC   ON ti.clientId = cs.ClientId
# MAGIC LEFT JOIN Legacy2_IdentificationDocument AS cl_id
# MAGIC   ON cl_id.Id = cs.ClientId
# MAGIC LEFT JOIN Legacy2_IdentificationDocument AS rp_id
# MAGIC   ON rp_id.Id = cs.ParentIdentity

# COMMAND ----------

# DBTITLE 1,Final Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW allrelationship AS
# MAGIC
# MAGIC Select * from gcoballrelationship
# MAGIC UNION 
# MAGIC select * from Legacy2allrelationships

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Data

# COMMAND ----------

df_AllRelationships = spark.table('allrelationship')
save_to_saradar_storage_account(df_AllRelationships, allrelationship_dataobject)

# COMMAND ----------

df_party_uniqueID_DQ = spark.table('party_uniqueID_DQ')
save_to_saradar_storage_account(df_party_uniqueID_DQ, UniqueID_DQ_dataobject)

# COMMAND ----------

# # Check if the table exists
# if spark.catalog.tableExists("radar.AllRelationships"):
#     # Drop the table if it exists
#    spark.sql("DROP TABLE radar.AllRelationships")

# # Create the table with the new data
# spark.sql("SELECT * FROM allrelationship").write.mode("overwrite").saveAsTable("radar.AllRelationships")

# COMMAND ----------

# # Check if the table exists
# if spark.catalog.tableExists("radar.UniqueID_DQ"):
#     # Drop the table if it exists
#    spark.sql("DROP TABLE radar.UniqueID_DQ")

# # Create the table with the new data
# spark.sql("SELECT * FROM party_uniqueID_DQ").write.mode("overwrite").saveAsTable("radar.UniqueID_DQ")
