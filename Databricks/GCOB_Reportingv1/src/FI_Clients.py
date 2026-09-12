# Databricks notebook source
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

FI_HubIndicator_dataobject = 'MI_FI_HubIndicator'
HongkongDepositScheme_dataobject = 'MI_HongkongDepositScheme'

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details'
, 'party_local_client_Owners'
, 'party_products_and_services'
,'party_business_activities'
,'ConsultationType'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Legacy2_GCOB_ApprovedVersion'
, 'Legacy2_case_client_details'
, 'Legacy2_workitem'
, 'Legacy2_products_and_services'
, 'Legacy2_risk'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations

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

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client_details AS
# MAGIC select c.* 
# MAGIC ,r.RiskModelName
# MAGIC ,r.ValidatedRiskLevel
# MAGIC ,r.CalculatedRiskLevel
# MAGIC ,r.ReCalculatedRiskLevel
# MAGIC ,r.GeographicalRetainedRiskLevel
# MAGIC ,r.EntityTypeRetainedRiskLevel
# MAGIC ,r.StructureRetainedRiskLevel
# MAGIC ,r.SectorRetainedRiskLevel
# MAGIC ,r.ProductsRetainedRiskLevel
# MAGIC ,r.PoliticallyExposedPersonsRetainedRiskLevel
# MAGIC ,r.ThirdPartyRetainedRiskLevel
# MAGIC ,r.TransactionRetainedRiskLevel
# MAGIC ,r.DistributionChannelRetainedRiskLevel
# MAGIC ,r.AdverseInfoRetainedRiskLevel
# MAGIC ,r.GeographicalApplicableRisk
# MAGIC ,r.EntityTypeApplicableRisk
# MAGIC ,r.StructureApplicableRisk
# MAGIC ,r.SectorApplicableRisk
# MAGIC ,r.ProductsApplicableRisk
# MAGIC ,r.PoliticallyExposedPersonsApplicableRisk
# MAGIC ,r.TransactionApplicableRisk
# MAGIC ,r.DistributionChannelApplicableRisk
# MAGIC ,r.ThirdPartyApplicableRisk
# MAGIC ,r.AdverseInfoApplicableRisk
# MAGIC --,case when (c.ApprovedInGcob = 0 and GCOB_TRUE.CaseId is null and c.StatusTypeName = 'Completed') then 'True' else 'False' end as IsLatestApprovedVersionOfClient
# MAGIC ,case when IsClient = 'true' and ClientTypeId = 1 then concat('L2_LEC_',c.ClientId)
# MAGIC       when IsClient = 'true' and ClientTypeId in (2,3) then concat('L2_NP_NPPC_',c.ClientId)
# MAGIC       when IsClient = 'false' and ClientTypeId = 1 then concat('L2_RLEP_',c.ClientId)
# MAGIC       when IsClient = 'false' and ClientTypeId in (2,3) then concat('L2_RNPP_',c.ClientId)
# MAGIC end as SourceClient
# MAGIC from Legacy2_client c
# MAGIC Left outer join Legacy2_GCOB_ApprovedVersion GCOB_TRUE on c.GcobCaseId = GCOB_TRUE.CaseId
# MAGIC left join Legacy2_risk r on c.ClientId = r.ClientId
# MAGIC where c.IsClient = 'true'
# MAGIC and ClientTypeId in (1,2,3)
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_Product AS
# MAGIC select pns.* 
# MAGIC ,c.GcobId
# MAGIC ,c.ClientLifeCycleName
# MAGIC ,IsClient
# MAGIC ,ClientType
# MAGIC ,c.IsLatestApprovedVersionOfClient
# MAGIC ,c.SourceClient
# MAGIC from Legacy2_products_and_services pns
# MAGIC Inner join Legacy2_client_details c on pns.ClientId = c.ClientId
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW clientdetails_consultation AS
# MAGIC select p.*
# MAGIC ,ct.ConsultationRequired
# MAGIC ,ct.ConsultationType
# MAGIC FROM party_case_client_details p
# MAGIC LEFT OUTER JOIN
# MAGIC ConsultationType ct on p.SourceClient = ct.SourceClient
# MAGIC where CaseStatusName <> 'Cancelled'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW case_client_details AS
# MAGIC Select distinct
# MAGIC SourceSystem
# MAGIC ,ClientId
# MAGIC ,CaseId
# MAGIC ,cast(GcobId as string) as GcobId
# MAGIC ,FullLegalName
# MAGIC ,TeaOtherReason
# MAGIC ,TEAReviewReasonDescription
# MAGIC ,ReviewTypeName
# MAGIC ,CaseStatusName
# MAGIC ,BusinessLineName
# MAGIC ,OwnerType
# MAGIC ,GlobalClientOwner
# MAGIC ,GlobalClientOwnerLocation
# MAGIC ,GlobalClientOwnerOfficeLocation
# MAGIC ,ClientLifeCycleName
# MAGIC ,CddType
# MAGIC ,EdrReason
# MAGIC ,NextReviewDate
# MAGIC ,RingFenced
# MAGIC ,ScheduledCompletionDate
# MAGIC ,IsEligibleForFatcaAssessment
# MAGIC ,FatcaClassification
# MAGIC ,FatcaDateOfIssue
# MAGIC ,IsEligibleForCrsAssessment
# MAGIC ,CrsClassification
# MAGIC ,CrsFormSignedDate
# MAGIC ,FullLegalNameInLocalLanguage
# MAGIC ,DateOfBirth
# MAGIC ,RegisteredStreet
# MAGIC ,RegisteredNumber
# MAGIC ,RegisteredPostalCode
# MAGIC ,RegisteredCity
# MAGIC ,RegisteredRegion
# MAGIC ,OperatingStreet
# MAGIC ,OperatingNumber
# MAGIC ,OperatingPostalCode
# MAGIC ,OperatingCity
# MAGIC ,OperatingRegion
# MAGIC ,OffBoardingReason
# MAGIC ,SalesforceClientID_nCino
# MAGIC ,SubmitToClientCommittee
# MAGIC ,CountryOfRegistration
# MAGIC ,RegisteredCountryIsoCode
# MAGIC ,CountryOfOperation
# MAGIC ,OperatingCountryIsoCode
# MAGIC ,ContactPersonEmailAddress
# MAGIC ,ContactPersonTelephoneNumber
# MAGIC ,IncorporationNumber
# MAGIC ,IsIncorporated
# MAGIC ,IncorporationDate
# MAGIC ,IsClientListed
# MAGIC ,IsClientRegulated
# MAGIC ,FIHubIndicator
# MAGIC ,HasTaxForm
# MAGIC ,IsTaxIntegrityMaterial
# MAGIC ,ValidatedRiskLevel
# MAGIC ,ClientType
# MAGIC ,IsLatestApprovedVersionOfClient
# MAGIC ,ClientCitizenship
# MAGIC ,ClientNationality
# MAGIC ,WWID
# MAGIC ,ACBS
# MAGIC ,RUTID
# MAGIC ,ISB
# MAGIC ,GCDSID
# MAGIC ,NameOfExchange
# MAGIC ,CountryOfExchange
# MAGIC ,NameOfRegulator
# MAGIC ,CountryOfRegulator
# MAGIC ,4EyeCheckReviewer
# MAGIC ,CurrentAssignee
# MAGIC ,Last4EyeCheckReviewer
# MAGIC ,LastKYCAnalyst
# MAGIC ,LastSentFor4EyeCheck
# MAGIC ,LastSentForKYCAssesment
# MAGIC ,InitiationInProgressAssignee
# MAGIC ,CaseCompletedDate
# MAGIC ,CaseCreationDate
# MAGIC ,DateSubmittedFor4EyeCheck
# MAGIC ,DateSubmittedForSignOff
# MAGIC ,KYCAssessmentInProgressAssignee
# MAGIC ,ReadyForKYCAssessmentDate
# MAGIC ,ClientOwnerSignOffDate
# MAGIC ,FinalDecisionDate
# MAGIC ,SourceClient
# MAGIC ,RiskModelName
# MAGIC ,ModelCalculatedRiskLevel
# MAGIC ,ModelRecalculatedRiskLevel
# MAGIC ,GeographicalRiskLevel
# MAGIC ,EntityTypeRiskLevel
# MAGIC ,StructureRiskLevel
# MAGIC ,SectorRiskLevel
# MAGIC ,ProductAndServiceRiskLevel
# MAGIC ,PEPRiskLevel
# MAGIC ,TransactionRiskLevel
# MAGIC ,DistributionRiskLevel
# MAGIC ,ThirdPartyRiskLevel
# MAGIC ,AdverseInfoRiskLevel
# MAGIC ,OtherRiskLevel
# MAGIC ,GeographicalApplicableRisk
# MAGIC ,EntityTypeApplicableRisk
# MAGIC ,StructureApplicableRisk
# MAGIC ,SectorApplicableRisk
# MAGIC ,ProductsApplicableRisk
# MAGIC ,PoliticallyExposedPersonsApplicableRisk
# MAGIC ,TransactionApplicableRisk
# MAGIC ,DistributionChannelApplicableRisk
# MAGIC ,ThirdPartyApplicableRisk
# MAGIC ,AdverseInfoApplicableRisk
# MAGIC ,OtherApplicableRisk
# MAGIC ,ConsultationRequired
# MAGIC ,ConsultationType
# MAGIC ,case when clienttype='Legal Entity' then concat('LE_', Gcobid)
# MAGIC     when clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',Gcobid) else 'NA' end as UniqueGcobId
# MAGIC ,LastProductOffboardingAnalyst
# MAGIC FROM clientdetails_consultation
# MAGIC where CaseStatusName <> 'Cancelled'
# MAGIC
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC Select DISTINCT 
# MAGIC 	SourceSystem
# MAGIC ,	ClientId
# MAGIC ,	null as CaseId
# MAGIC ,	GcobId
# MAGIC ,	FullLegalName
# MAGIC ,	TeaOtherReason
# MAGIC ,	TEAReviewReasonDescription
# MAGIC ,	ReviewTypeName
# MAGIC ,	StatusTypeName as CaseStatusName
# MAGIC ,	BusinessLineName
# MAGIC ,	null as OwnerType
# MAGIC ,	GlobalClientOwner
# MAGIC ,	GlobalClientOwnerLocation
# MAGIC ,	null as GlobalClientOwnerOfficeLocation
# MAGIC ,	ClientLifeCycleName
# MAGIC ,	CddType
# MAGIC ,	cast(legacy2EdrReasonId as string) as EdrReason
# MAGIC ,	NextReviewDate
# MAGIC ,	IsRingFenced as RingFenced
# MAGIC --,null as RingFenced
# MAGIC ,	ScheduledCompletionDate
# MAGIC ,	IsEligibleForFatcaAssessment
# MAGIC ,	cast(legacy2FatcaClassificationId as string) as FatcaClassification
# MAGIC ,	null as FatcaDateOfIssue
# MAGIC ,	IsEligibleForCrsAssessment
# MAGIC ,	cast(legacy2CrsClassificationId as string) as CrsClassification
# MAGIC ,	null as CrsFormSignedDate
# MAGIC ,	null as FullLegalNameInLocalLanguage
# MAGIC --,	DateOfBirth
# MAGIC ,null as DateOfBirth
# MAGIC ,	RegisteredStreet
# MAGIC ,	RegisteredAddressNumber as RegisteredNumber
# MAGIC ,	RegisteredPostcode as RegisteredPostalCode
# MAGIC ,	RegisteredCity
# MAGIC ,	RegisteredRegion
# MAGIC ,	OperationalStreet as OperatingStreet
# MAGIC ,	OperationalNumber as OperatingNumber
# MAGIC ,	OperationalPostcode as OperatingPostalCode
# MAGIC ,	OperationalCity as OperatingCity
# MAGIC ,	OperationalRegion as OperatingRegion
# MAGIC ,	OffBoardingReasonDescription as OffBoardingReason
# MAGIC ,	SalesforceClientID_nCino
# MAGIC ,	null as SubmitToClientCommittee
# MAGIC ,	RegisteredCountry as CountryOfRegistration
# MAGIC ,	null as RegisteredCountryIsoCode
# MAGIC ,	OperationalCountry as CountryOfOperation
# MAGIC ,	null as OperatingCountryIsoCode
# MAGIC ,	ContactEmailAddress as ContactPersonEmailAddress
# MAGIC ,	ContactPhoneNumber as ContactPersonTelephoneNumber
# MAGIC ,	IncorporationNumber
# MAGIC ,	IsIncorporated
# MAGIC ,	null as IncorporationDate
# MAGIC ,	IsListed as IsClientListed
# MAGIC ,	IsRegulated as IsClientRegulated
# MAGIC ,	null as FIHubIndicator
# MAGIC ,	null as HasTaxForm
# MAGIC ,	null as IsTaxIntegrityMaterial
# MAGIC ,	ValidatedRiskLevel
# MAGIC ,	ClientType
# MAGIC ,	IsLatestApprovedVersionOfClient
# MAGIC ,	null as ClientCitizenship
# MAGIC ,	null as ClientNationality
# MAGIC ,	null as WWID
# MAGIC ,	null as ACBS
# MAGIC ,	null as RUTID
# MAGIC ,	null as ISB
# MAGIC ,	null as GCDSID
# MAGIC ,	null as NameOfExchange
# MAGIC ,	null as CountryOfExchange
# MAGIC ,	null as NameOfRegulator
# MAGIC ,	null as CountryOfRegulator
# MAGIC ,	Eye4CheckReviewer
# MAGIC ,	CurrentAssignee
# MAGIC ,	Last4EyeCheckReviewer
# MAGIC ,	LastKYCAnalyst
# MAGIC ,	LastSentFor4EyeCheck
# MAGIC ,	LastSentForKYCAssesment
# MAGIC ,	InitiationInProgressAssignee
# MAGIC ,	CaseCompletedDate
# MAGIC ,	CaseCreationDate
# MAGIC ,	DateSubmittedFor4EyeCheck
# MAGIC ,	DateSubmittedForSignOff
# MAGIC ,	KYCAssessmentInProgressAssignee
# MAGIC ,	ReadyForKYCAssessmentDate
# MAGIC ,	ClientOwnerSignOffDate
# MAGIC ,	FinalDecisionDate
# MAGIC ,	SourceClient
# MAGIC ,	RiskModelName
# MAGIC ,	CalculatedRiskLevel as ModelCalculatedRiskLevel
# MAGIC ,	ReCalculatedRiskLevel as ModelRecalculatedRiskLevel
# MAGIC ,	GeographicalRetainedRiskLevel as GeographicalRiskLevel
# MAGIC ,	EntityTypeRetainedRiskLevel as EntityTypeRiskLevel
# MAGIC ,	StructureRetainedRiskLevel as StructureRiskLevel
# MAGIC ,	SectorRetainedRiskLevel as SectorRiskLevel
# MAGIC ,	ProductsRetainedRiskLevel as ProductAndServiceRiskLevel
# MAGIC ,	PoliticallyExposedPersonsRetainedRiskLevel as PEPRiskLevel
# MAGIC ,	TransactionRetainedRiskLevel as TransactionRiskLevel
# MAGIC ,	DistributionChannelRetainedRiskLevel as DistributionRiskLevel
# MAGIC ,	ThirdPartyRetainedRiskLevel as ThirdPartyRiskLevel
# MAGIC ,	AdverseInfoRetainedRiskLevel as AdverseInfoRiskLevel
# MAGIC ,	null as OtherRiskLevel
# MAGIC ,	GeographicalApplicableRisk
# MAGIC , EntityTypeApplicableRisk
# MAGIC , StructureApplicableRisk
# MAGIC , SectorApplicableRisk
# MAGIC , ProductsApplicableRisk
# MAGIC , PoliticallyExposedPersonsApplicableRisk
# MAGIC , TransactionApplicableRisk
# MAGIC , DistributionChannelApplicableRisk
# MAGIC , ThirdPartyApplicableRisk
# MAGIC , AdverseInfoApplicableRisk
# MAGIC ,null as OtherApplicableRisk
# MAGIC ,null as ConsultationRequired
# MAGIC ,null as ConsultationType
# MAGIC ,case when clienttype='Legal Entity' then concat('LE_', Gcobid)
# MAGIC     when clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',Gcobid) else 'NA' end as UniqueGcobId
# MAGIC ,null as LastProductOffboardingAnalyst
# MAGIC From Legacy2_client_details

# COMMAND ----------

df_case_client_details=spark.table('party_case_client_details')

# COMMAND ----------

df_case_client_details=df_case_client_details.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

df_case_client_details.createOrReplaceTempView("party_case_client_details")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW products_and_sevices AS
# MAGIC select distinct
# MAGIC ClientId
# MAGIC ,gcobid
# MAGIC ,ProductReferenceId
# MAGIC ,IsOtc
# MAGIC ,ProductLifecyclestatus
# MAGIC ,ProductOfferingLocation
# MAGIC ,BookingEntityLocation
# MAGIC ,ProductName
# MAGIC ,ProductDomain
# MAGIC ,BusinessUnit
# MAGIC ,ClientLifeCycleName
# MAGIC ,IsBookingEntity
# MAGIC ,SourceClient
# MAGIC From party_products_and_services
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC ClientId
# MAGIC ,gcobid
# MAGIC ,null as ProductReferenceId
# MAGIC ,Otc as IsOtc
# MAGIC ,null as ProductLifecyclestatus
# MAGIC ,ProductLocation as ProductOfferingLocation
# MAGIC ,BookingLocation as BookingEntityLocation
# MAGIC ,ProductServiceName as ProductName
# MAGIC ,null as ProductDomain
# MAGIC ,null as BusinessUnit
# MAGIC ,ClientLifeCycleName
# MAGIC ,null as IsBookingEntity
# MAGIC ,SourceClient
# MAGIC
# MAGIC From Legacy2_Product

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Final Required Data.
# MAGIC CREATE OR REPLACE TEMPORARY VIEW MI_FI_HubIndicator AS 
# MAGIC SELECT ccd.GcobId
# MAGIC   , ccd.GCDSID
# MAGIC   , ccd.FullLegalName
# MAGIC   , lc.LocalClientOwnerName AS LocalClientOwner
# MAGIC   , ccd.GlobalClientOwner
# MAGIC   , ccd.CddType
# MAGIC   , ccd.CountryOfRegistration AS RegisteredCountry
# MAGIC   , ccd.CountryOfOperation AS OperationalCountry
# MAGIC   , ccd.ValidatedRiskLevel AS RiskLevel
# MAGIC   , ccd.BusinessLineName AS BusinessLine
# MAGIC   , ccd.NextReviewDate
# MAGIC   , ccd.GlobalClientOwnerLocation
# MAGIC   , ps.ProductName
# MAGIC   , ps.BookingEntityLocation AS BookingLocation
# MAGIC   , ps.ProductOfferingLocation AS ProductLocation
# MAGIC   , lc.Location AS LocalClientOwnerLocation
# MAGIC   , ba.NaicsCode
# MAGIC   , ba.NaicsName
# MAGIC   , ccd.SourceSystem
# MAGIC   , ccd.CaseStatusName AS CaseStatusType
# MAGIC   , ccd.ReviewTypeName
# MAGIC   , ccd.ClientLifeCycleName AS ClientLifeCycle
# MAGIC   , ccd.IsLatestApprovedVersionOfClient
# MAGIC   , ccd.FIHubIndicator
# MAGIC   , ccd.SourceClient
# MAGIC   , case when substr(ccd.SourceClient,0,3) = 'LEC' and ccd.SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(ccd.SourceClient,0,2) = 'NP' and ccd.SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(ccd.SourceClient,0,6) = 'L2_LEC' and ccd.SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(ccd.SourceClient,0,5) = 'L2_NP' and ccd.SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC     end as SourceSystemReference
# MAGIC   , to_date(date_sub(current_date(), 1), 'yyyy-MM-dd') AS BusinessDate
# MAGIC FROM case_client_details AS ccd
# MAGIC LEFT JOIN party_local_client_Owners AS lc 
# MAGIC ON ccd.ClientId = lc.LegalEntityClientId and
# MAGIC     ccd.ClientType = 'Legal Entity'
# MAGIC LEFT JOIN products_and_sevices AS ps
# MAGIC ON ccd.SourceClient = ps.SourceClient
# MAGIC LEFT JOIN party_business_activities AS ba
# MAGIC ON ccd.SourceClient = ba.SourceClient

# COMMAND ----------

df_MI_FI_HubIndicator = spark.table('MI_FI_HubIndicator')
save_to_saradar_storage_account(df_MI_FI_HubIndicator, FI_HubIndicator_dataobject)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS radar.MI_FI_HubIndicator;

# COMMAND ----------

# spark.sql('select * from MI_FI_HubIndicator').write.mode('overwrite').saveAsTable('radar.MI_FI_HubIndicator')

# COMMAND ----------

# MAGIC %md
# MAGIC ## HongkongDepositScheme

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW MI_HongkongDepositScheme AS
# MAGIC select distinct
# MAGIC to_date(c.BusinessDate, "MM/dd/yyyy") as BusinessDate 
# MAGIC ,c.GcobId
# MAGIC ,c.CaseId
# MAGIC ,c.FullLegalName
# MAGIC ,c.GlobalClientOwner
# MAGIC ,c.GlobalClientOwnerLocation
# MAGIC ,c.ClientLifeCycleName
# MAGIC ,c.ReviewTypeName
# MAGIC ,c.CaseStatusName
# MAGIC ,c.IsLatestApprovedVersionOfClient
# MAGIC ,l.MoneyLender 
# MAGIC ,c.SourceSystem
# MAGIC ,c.SourceClient
# MAGIC ,c.BusinessLineName AS `BusinessLineName`
# MAGIC ,c.CountryofRegistration,
# MAGIC c.ValidatedRiskLevel AS `ClientCDDRisk`
# MAGIC ,c.ModelCalculatedRiskLevel
# MAGIC ,c.ModelRecalculatedRiskLevel
# MAGIC ,c.GeographicalRiskLevel
# MAGIC ,c.EntityTypeRiskLevel
# MAGIC ,c.StructureRiskLevel
# MAGIC ,c.SectorRiskLevel
# MAGIC ,c.ProductAndServiceRiskLevel
# MAGIC ,c.PEPRiskLevel
# MAGIC ,c.TransactionRiskLevel
# MAGIC ,c.DistributionRiskLevel
# MAGIC ,c.ThirdPartyRiskLevel
# MAGIC ,c.AdverseInfoRiskLevel
# MAGIC ,c.OtherRiskLevel
# MAGIC ,c.NextReviewDate
# MAGIC ,c.CaseCompletedDate
# MAGIC ,c.CaseCreationDate
# MAGIC ,c.ClientOwnerSignOffDate
# MAGIC ,c.ScheduledCompletionDate
# MAGIC ,c.CountryOfOperation
# MAGIC ,c.ValidatedRiskLevel as LatestValidatedRisk
# MAGIC ,c.LastKYCAnalyst
# MAGIC ,pns.ProductOfferingLocation as ProductLocation
# MAGIC ,pns.BookingEntityLocation as BookingLocation
# MAGIC ,pns.ProductName
# MAGIC ,c.FinalDecisionDate
# MAGIC ,c.CddType
# MAGIC ,c.RegisteredNumber as Number 
# MAGIC ,c.RegisteredStreet as Street
# MAGIC ,c.RegisteredPostalCode as PostalCode
# MAGIC ,c.RegisteredCity as City
# MAGIC ,c.RegisteredRegion as Region
# MAGIC ,c.IncorporationNumber
# MAGIC ,c.ContactPersonTelephoneNumber
# MAGIC ,c.GCDSID
# MAGIC from party_case_client_details c
# MAGIC left join radar.MI_LocalRequirement l on c.ClientId = l.ClientId and l.LocalRequirementCountry = 'HongKong'
# MAGIC left join products_and_sevices pns on c.sourceclient = pns.sourceclient
# MAGIC where c.CaseStatusName <> 'Cancelled'
# MAGIC

# COMMAND ----------

df_MI_HongkongDepositScheme = spark.table('MI_HongkongDepositScheme')
save_to_saradar_storage_account(df_MI_HongkongDepositScheme, HongkongDepositScheme_dataobject)

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS radar.MI_HongkongDepositScheme;

# COMMAND ----------

# spark.sql('select * from MI_HongkongDepositScheme').write.mode('overwrite').saveAsTable('radar.MI_HongkongDepositScheme')
