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

ProductAndService_dataobject = 'ProdusctAndService'
Cases_dataobject = 'MI_Cases'

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
#'party_AllCasesReport'
'party_case_client_details'
, 'party_workitem'
, 'party_local_client_Owners'
, 'party_request_for_information'
, 'party_client'
, 'party_products_and_services'
, 'party_trade_name'
, 'ConsultationType'
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
# MAGIC --select count(*) from Legacy2_case_client_details;--25926
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%'; --3006

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
# MAGIC -- where CaseStatusName <> 'Cancelled'
# MAGIC

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
# MAGIC ,EdrOtherReason
# MAGIC ,ReviewReason
# MAGIC ,ReviewReasonOtherExplanation
# MAGIC ,ExecutiveSummary
# MAGIC ,ClientCDDResponsibleLocation
# MAGIC ,ReveiewReasonMultpleSelection
# MAGIC ,RingFenceReason
# MAGIC FROM clientdetails_consultation
# MAGIC -- where CaseStatusName <> 'Cancelled'
# MAGIC --and sourceclient <> 'NP_NPPC_3435' --This client has 2 SalesforceClientID_nCino Ids due to which we are getting duplicate values. Sameer will discuss with Gcob to fix it. Once it is fixed this client from filter condition will be removed
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
# MAGIC ,null as EdrOtherReason
# MAGIC ,ReviewReason
# MAGIC ,ReviewReasonOtherExplanation
# MAGIC ,null as ExecutiveSummary
# MAGIC ,Null ClientCDDResponsibleLocation
# MAGIC ,null as ReveiewReasonMultpleSelection
# MAGIC ,RingFenceReason
# MAGIC From Legacy2_client_details

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
# MAGIC ,UndefinedProductCategory as UndefinedProductCategoryName
# MAGIC ,OtherProductInformation as OtherInformation
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
# MAGIC ,null  as UndefinedProductCategoryName
# MAGIC ,null as OtherInformation
# MAGIC From Legacy2_Product

# COMMAND ----------

df_case_client_details=spark.table('case_client_details')

# COMMAND ----------

df_case_client_details=df_case_client_details.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

df_case_client_details.createOrReplaceTempView("party_case_client_details")

# COMMAND ----------

Id_list = [i+1 for i in range(9)]
Description_list = ['Prework'
, 'Ready for assessment'
, 'Assessment in progress'
, '4-EYE check'
, 'Sign-off'
, 'Fulfilment'
, 'Completed'
, 'Cancelled']
# create pyspark dataframe from lists
spark.createDataFrame(zip(Id_list, Description_list), ['PhaseID', 'PhaseName']).createOrReplaceTempView('gcob_static_PhaseLookup')

StatusId_list = [i + 1 for i in range(22)]
Name_list = ['Initiation In Progress'
, 'Ready for KYC assessment'
, 'KYC assessment in progress'
, 'Ready for 4 eye check'
, '4 eye check in progress'
, 'Client owner sign off requested'
, 'Client committee sign off requested'
, 'Product fulfilment in progress'
, 'Completed'
, 'Cancelled'
, 'Migrated'
, 'Ready for identification'
, 'Identification in progress'
, 'Ready for screening'
, 'Screening in progress'
, 'GCOB Review in progress'
, 'Client Owner approval requested'
, 'Local client owner sign off requested'
, 'Ready for Product Offboarding confirmation'
, 'Product Offboarding confirmation in progress'
, 'Product Offboarding in progress'
, 'Senior management sign off requested']
# create pyspark dataframe from lists
spark.createDataFrame(zip(StatusId_list, Name_list), ['StatusId', 'Name']).createOrReplaceTempView('gcob_static_CaseStatusType')


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ProdusctAndService AS
# MAGIC select distinct
# MAGIC acr.BusinessDate
# MAGIC ,acr.ClientId
# MAGIC ,acr.SourceClient
# MAGIC ,case when substr(acr.SourceClient,0,3) = 'LEC' and SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(acr.SourceClient,0,2) = 'NP' and SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(acr.SourceClient,0,6) = 'L2_LEC' and SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(acr.SourceClient,0,5) = 'L2_NP' and SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC ,acr.ClientType
# MAGIC ,acr.GcobId
# MAGIC ,acr.CaseId 
# MAGIC ,acr.FullLegalName
# MAGIC ,acr.ClientLifeCycleName
# MAGIC ,acr.ReviewTypeName
# MAGIC ,acr.CaseStatusName
# MAGIC ,acr.IsLatestApprovedVersionOfClient
# MAGIC ,acr.FIHubIndicator
# MAGIC ,case when FIHubIndicator = 1 then 'FI' when FIHubIndicator = 0 then 'Corp' else null end as FIHubIndicator_Derived
# MAGIC ,acr.SourceSystem
# MAGIC ,pns.ProductDomain
# MAGIC ,pns.BusinessUnit
# MAGIC ,pns.ProductName
# MAGIC ,pns.BookingEntityLocation
# MAGIC ,pns.ProductOfferingLocation
# MAGIC ,pns.IsOtc
# MAGIC ,pns.ProductLifecyclestatus
# MAGIC ,acr.GlobalClientOwnerLocation
# MAGIC ,acr.ConsultationRequired
# MAGIC ,acr.ConsultationType
# MAGIC ,acr.UniqueGcobId
# MAGIC ,pns.UndefinedProductCategoryName
# MAGIC ,pns.OtherInformation
# MAGIC From party_case_client_details acr
# MAGIC INNER JOIN products_and_sevices pns on acr.ClientId = pns.ClientId and acr.SourceClient = pns.SourceClient 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW UnionProductAndBookingLocation AS
# MAGIC select distinct SourceClient,ProductOfferingLocation as P_B_location from ProdusctAndService
# MAGIC where ProductLifecyclestatus = 'Active'
# MAGIC Union 
# MAGIC select distinct SourceClient,BookingEntityLocation as P_B_location from ProdusctAndService
# MAGIC where ProductLifecyclestatus = 'Active'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW DistinctProductAndBookingLocation AS
# MAGIC select distinct SourceClient,P_B_location from UnionProductAndBookingLocation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ProductBookingLocation AS
# MAGIC SELECT DISTINCT SourceClient, concat_ws(',', sort_array(collect_list(struct(P_B_location))).P_B_location) AS InvolvedLocations
# MAGIC FROM DistinctProductAndBookingLocation AS ci
# MAGIC --where SourceSystem = 'GCOB'
# MAGIC GROUP BY SourceClient
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW AllOfferingLocation AS
# MAGIC select distinct SourceClient,P_B_location as OfferingLocation from DistinctProductAndBookingLocation
# MAGIC Union 
# MAGIC select distinct SourceClient,GlobalClientOwnerLocation as OfferingLocation from ProdusctAndService
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW DistinctOfferingLocation AS
# MAGIC select distinct SourceClient,OfferingLocation from AllOfferingLocation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ProductAndService AS
# MAGIC select distinct 
# MAGIC pns.*
# MAGIC ,ol.OfferingLocation
# MAGIC From ProdusctAndService pns
# MAGIC left join DistinctOfferingLocation ol on pns.SourceClient = ol.SourceClient
# MAGIC --where GcobId = 1 and SourceSystem = 'GCOB' order by CaseId

# COMMAND ----------

df_ProductAndService = spark.table('ProductAndService')
save_to_saradar_storage_account(df_ProductAndService, ProductAndService_dataobject)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW impacted_loc AS
# MAGIC (SELECT CaseId, SourceClient, concat_ws(',', collect_set(ClientImpactedLocation)) AS ClientImpactedLocation
# MAGIC FROM radar.party_case_impacted_location
# MAGIC GROUP BY CaseId, SourceClient)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Cases AS
# MAGIC select distinct
# MAGIC to_date(BusinessDate, "MM/dd/yyyy") as BusinessDate 
# MAGIC ,SourceSystem
# MAGIC ,ClientId
# MAGIC ,c.SourceClient
# MAGIC ,case when substr(c.SourceClient,0,3) = 'LEC' and SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(c.SourceClient,0,2) = 'NP' and SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(c.SourceClient,0,6) = 'L2_LEC' and SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(c.SourceClient,0,5) = 'L2_NP' and SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC ,c.CaseId
# MAGIC ,GcobId
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
# MAGIC ,datediff(CaseCompletedDate,CaseCreationDate) as CaseDuration
# MAGIC ,DateSubmittedFor4EyeCheck
# MAGIC ,DateSubmittedForSignOff
# MAGIC ,KYCAssessmentInProgressAssignee
# MAGIC ,ReadyForKYCAssessmentDate
# MAGIC ,ClientOwnerSignOffDate
# MAGIC ,FinalDecisionDate
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
# MAGIC ,UniqueGcobId
# MAGIC ,GlobalClientOwnerLocation as LeadLocation
# MAGIC ,il.InvolvedLocations
# MAGIC ,iloc.ClientImpactedLocation
# MAGIC ,c.LastProductOffboardingAnalyst
# MAGIC ,c.EdrOtherReason
# MAGIC ,c.ReviewReason
# MAGIC ,c.ReviewReasonOtherExplanation
# MAGIC ,c.ExecutiveSummary
# MAGIC ,c.ClientCDDResponsibleLocation
# MAGIC ,c.ReveiewReasonMultpleSelection
# MAGIC ,c.RingFenceReason
# MAGIC From party_case_client_details c
# MAGIC Left join ProductBookingLocation il on c.SourceClient = il.SourceClient
# MAGIC left join impacted_loc iloc on c.SourceClient=iloc.SourceClient
# MAGIC --where CaseStatusName <> 'Cancelled'

# COMMAND ----------

df_Cases = spark.table('Cases')
save_to_saradar_storage_account(df_Cases, Cases_dataobject)
