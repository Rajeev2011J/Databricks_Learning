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

MI_LocalRequirement_dataobject = 'MI_LocalRequirement'

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
#'party_AllCasesReport'
'party_case_client_details'
, 'party_LocalRequirement'
, 'party_local_client_Owners'
, 'party_client'
, 'party_products_and_services'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

df_case_client_details=spark.table('party_case_client_details')

# COMMAND ----------

df_case_client_details=df_case_client_details.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

df_case_client_details.createOrReplaceTempView("party_case_client_details")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW LocalRequirement AS
# MAGIC select distinct ClientId,LocalRequirementId,LocalRequirementCountry,QuestionName,AnswerText,Name
# MAGIC From party_LocalRequirement
# MAGIC where LocalRequirementCountry <> 'Singapore'

# COMMAND ----------

df_LocalRequirement=spark.table('LocalRequirement')

# COMMAND ----------

from pyspark.sql.functions import array_join, collect_set
df_LocalRequirementPivot = df_LocalRequirement.groupby('ClientId', 'LocalRequirementId','LocalRequirementCountry','Name').pivot('QuestionName').agg(array_join(collect_set('AnswerText'), ','))

# COMMAND ----------

df_LocalRequirementPivot.createOrReplaceTempView("LocalRequirementClients")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW LocalRequirement_Singapore AS
# MAGIC select distinct ClientId,LocalRequirementId,LocalRequirementCountry,QuestionName,AnswerText
# MAGIC From party_LocalRequirement
# MAGIC where LocalRequirementCountry = 'Singapore'

# COMMAND ----------

df_LocalRequirement_S=spark.table('LocalRequirement_Singapore')

# COMMAND ----------

from pyspark.sql.functions import array_join, collect_set
df_LocalRequirement_S_Pivot = df_LocalRequirement_S.groupby('ClientId', 'LocalRequirementId','LocalRequirementCountry').pivot('QuestionName').agg(array_join(collect_set('AnswerText'), ','))

# COMMAND ----------

df_LocalRequirement_S_Pivot.createOrReplaceTempView("LocalRequirementSingaporeClients")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW All_LocalRequirement AS
# MAGIC select distinct
# MAGIC ClientId,
# MAGIC LocalRequirementId,
# MAGIC LocalRequirementCountry,
# MAGIC AccountSweepPercentage,
# MAGIC AffiliatedEnterprise1,
# MAGIC AffiliatedEnterprise2,
# MAGIC AffiliatedEnterprise3,
# MAGIC AttestationClientStructureReceived,
# MAGIC BackOfficeComment,
# MAGIC BankLicenseRestrictions,
# MAGIC BankLinesOfBusiness,
# MAGIC BankOperatesOffshore,
# MAGIC BankPayableThroughAccounts,
# MAGIC BankProprietaryActivityOnly,
# MAGIC Characteristic,
# MAGIC ClientConsent,
# MAGIC ConsentGivenDate,
# MAGIC ConsentGivenDateString,
# MAGIC ConsentType,
# MAGIC CtiRequestCreatedDate,
# MAGIC CtiResult,
# MAGIC CtiValidationErrors,
# MAGIC CustomerBehaviour,
# MAGIC CustomerInteraction,
# MAGIC CustomerStructure,
# MAGIC CustomerType,
# MAGIC Customers,
# MAGIC CustomersIdentificationInformation,
# MAGIC DomiciledInATaxHaven,
# MAGIC EligibleUKClientConsent,
# MAGIC EnterpriseScale,
# MAGIC FinCenExemptionReferenceId,
# MAGIC FrontOfficeComment,
# MAGIC FtzEnterprise,
# MAGIC FundAmlDueDiligenceRegulated,
# MAGIC FundSelfRegulatory,
# MAGIC FundSelfRegulatoryEvidence,
# MAGIC FundSourceOfPensionPlan,
# MAGIC FurtherInformationJustification,
# MAGIC HasAccountSweeps,
# MAGIC HasTaxForm,
# MAGIC HighInfluentialIndividuals,
# MAGIC HighInfluentialIndividualsExist,
# MAGIC HoldMail,
# MAGIC IsFinCenExempt,
# MAGIC LicenseExpires,
# MAGIC LicenseExpiry,
# MAGIC LicenseExpiryDateString,
# MAGIC LicenseNumber,
# MAGIC LicenseType,
# MAGIC MifidNonClassifiedNotes,
# MAGIC MoneyLender,
# MAGIC NegativeTaxRelatedNews,
# MAGIC NfpDonationSource,
# MAGIC NfpDonationUse,
# MAGIC NfpPublicDonations,
# MAGIC NfpRegisteredCharity,
# MAGIC Nra,
# MAGIC OrganizationContributors,
# MAGIC OrganizationControls,
# MAGIC OrganizationMonitorsFunds,
# MAGIC OrganizationTypeOfMonitoring,
# MAGIC OtcDerivatives,
# MAGIC PhoenixClassification,
# MAGIC ProjectName,
# MAGIC PspClientBusiness,
# MAGIC PspOffersServices,
# MAGIC PspOffersServicesAcknowledgement,
# MAGIC PspOther,
# MAGIC PspOtherAcknowledgement,
# MAGIC PspTransferFunds as `Account_used_to_send/receive_funds_from_PSP`,
# MAGIC PspTransferFundsAcknowledgement,
# MAGIC PspTransferFundsRationale,
# MAGIC ReasonForNoTaxForm,
# MAGIC SanctionsPepAdverseNotes,
# MAGIC SourceOfFundsWealth,
# MAGIC SummaryOfFaceToFaceVisit,
# MAGIC SuspiciousTransactions,
# MAGIC TradeFinanceCountryNone,
# MAGIC null as AdverseTaxFound,
# MAGIC null as ClientConsentId,
# MAGIC null as ComplexOwnership,
# MAGIC null as DomiciledTaxHaven,
# MAGIC null as HighTaxRiskAccountNotes,
# MAGIC null as Singapore_Holdmail,
# MAGIC null as IndulgesInTransferPricing,
# MAGIC null as IsCustomerHighTaxRisk,
# MAGIC null as PepOrPepRelated,
# MAGIC null as SpecificPurpose,
# MAGIC null as SummaryOfClientFaceToFace,
# MAGIC Name as `PSPs_Used`
# MAGIC FROM LocalRequirementClients
# MAGIC
# MAGIC UNION all
# MAGIC
# MAGIC Select distinct
# MAGIC ClientId,
# MAGIC LocalRequirementId,
# MAGIC LocalRequirementCountry,
# MAGIC null as AccountSweepPercentage,
# MAGIC null as AffiliatedEnterprise1,
# MAGIC null as AffiliatedEnterprise2,
# MAGIC null as AffiliatedEnterprise3,
# MAGIC null as AttestationClientStructureReceived,
# MAGIC null as BackOfficeComment,
# MAGIC null as BankLicenseRestrictions,
# MAGIC null as BankLinesOfBusiness,
# MAGIC null as BankOperatesOffshore,
# MAGIC null as BankPayableThroughAccounts,
# MAGIC null as BankProprietaryActivityOnly,
# MAGIC null as Characteristic,
# MAGIC null as ClientConsent,
# MAGIC null as ConsentGivenDate,
# MAGIC null as ConsentGivenDateString,
# MAGIC null as ConsentType,
# MAGIC null as CtiRequestCreatedDate,
# MAGIC null as CtiResult,
# MAGIC null as CtiValidationErrors,
# MAGIC null as CustomerBehaviour,
# MAGIC null as CustomerInteraction,
# MAGIC null as CustomerStructure,
# MAGIC null as CustomerType,
# MAGIC null as Customers,
# MAGIC null as CustomersIdentificationInformation,
# MAGIC null as DomiciledInATaxHaven,
# MAGIC null as EligibleUKClientConsent,
# MAGIC null as EnterpriseScale,
# MAGIC null as FinCenExemptionReferenceId,
# MAGIC null as FrontOfficeComment,
# MAGIC null as FtzEnterprise,
# MAGIC null as FundAmlDueDiligenceRegulated,
# MAGIC null as FundSelfRegulatory,
# MAGIC null as FundSelfRegulatoryEvidence,
# MAGIC null as FundSourceOfPensionPlan,
# MAGIC null as FurtherInformationJustification,
# MAGIC null as HasAccountSweeps,
# MAGIC null as HasTaxForm,
# MAGIC null as HighInfluentialIndividuals,
# MAGIC null as HighInfluentialIndividualsExist,
# MAGIC null as HoldMail,
# MAGIC null as IsFinCenExempt,
# MAGIC null as LicenseExpires,
# MAGIC null as LicenseExpiry,
# MAGIC null as LicenseExpiryDateString,
# MAGIC null as LicenseNumber,
# MAGIC null as LicenseType,
# MAGIC null as MifidNonClassifiedNotes,
# MAGIC null as MoneyLender,
# MAGIC null as NegativeTaxRelatedNews,
# MAGIC null as NfpDonationSource,
# MAGIC null as NfpDonationUse,
# MAGIC null as NfpPublicDonations,
# MAGIC null as NfpRegisteredCharity,
# MAGIC null as Nra,
# MAGIC null as OrganizationContributors,
# MAGIC null as OrganizationControls,
# MAGIC null as OrganizationMonitorsFunds,
# MAGIC null as OrganizationTypeOfMonitoring,
# MAGIC null as OtcDerivatives,
# MAGIC null as PhoenixClassification,
# MAGIC null as ProjectName,
# MAGIC null as PspClientBusiness,
# MAGIC null as PspOffersServices,
# MAGIC null as PspOffersServicesAcknowledgement,
# MAGIC null as PspOther,
# MAGIC null as PspOtherAcknowledgement,
# MAGIC null as `Account_used_to_send/receive_funds_from_PSP`,
# MAGIC null as PspTransferFundsAcknowledgement,
# MAGIC null as PspTransferFundsRationale,
# MAGIC null as ReasonForNoTaxForm,
# MAGIC null as SanctionsPepAdverseNotes,
# MAGIC null as SourceOfFundsWealth,
# MAGIC null as SummaryOfFaceToFaceVisit,
# MAGIC null as SuspiciousTransactions,
# MAGIC null as TradeFinanceCountryNone,
# MAGIC AdverseTaxFound,
# MAGIC ClientConsentId,
# MAGIC ComplexOwnership,
# MAGIC DomiciledTaxHaven,
# MAGIC HighTaxRiskAccountNotes,
# MAGIC Holdmail as Singapore_Holdmail,
# MAGIC IndulgesInTransferPricing,
# MAGIC IsCustomerHighTaxRisk,
# MAGIC PepOrPepRelated,
# MAGIC SpecificPurpose,
# MAGIC SummaryOfClientFaceToFace,
# MAGIC 'Null' as `PSPs_Used`
# MAGIC From LocalRequirementSingaporeClients

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW party_client_details AS
# MAGIC select * from party_case_client_details 
# MAGIC where sourceclient like 'LEC%'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ValidatedRisk_Change AS
# MAGIC with second_highest as
# MAGIC (
# MAGIC select distinct t.* from 
# MAGIC (
# MAGIC select fc.GcobId, fc.CaseId ,fc.ValidatedRiskLevel
# MAGIC ,fc.CaseStatusName, row_number() over (partition by fc.GcobId order by fc.CaseId desc) as seqnum
# MAGIC from party_client_details fc
# MAGIC where 
# MAGIC fc.CaseStatusName = 'Completed'
# MAGIC ) t
# MAGIC where seqnum = 2
# MAGIC and ValidatedRiskLevel <> 'High'
# MAGIC )
# MAGIC  
# MAGIC ,
# MAGIC Highest as
# MAGIC (select distinct t.* from 
# MAGIC (
# MAGIC select fc.GcobId, fc.CaseId ,fc.ValidatedRiskLevel
# MAGIC ,fc.CaseStatusName, row_number() over (partition by fc.GcobId order by fc.CaseId desc) as seqnum
# MAGIC from party_client_details fc
# MAGIC where 
# MAGIC fc.CaseStatusName = 'Completed'
# MAGIC ) t
# MAGIC where seqnum = 1
# MAGIC and ValidatedRiskLevel = 'High'
# MAGIC )
# MAGIC  
# MAGIC SELECT distinct 
# MAGIC fc.ClientId
# MAGIC ,fc.caseid
# MAGIC ,case when fc.ValidatedRiskLevel = 'High' and sh.ValidatedRiskLevel <> fh.ValidatedRiskLevel then 'Other To High'
# MAGIC else null
# MAGIC end as ValidatedRisk_Change
# MAGIC ,sh.ValidatedRiskLevel LastValidatedRisk
# MAGIC ,fh.ValidatedRiskLevel as LatestValidatedRisk
# MAGIC FROM party_client_details fc
# MAGIC   left join second_highest sh 
# MAGIC 	on fc.GcobId = sh.GcobId
# MAGIC   left join Highest fh 
# MAGIC 	on fc.GcobId = fh.GcobId
# MAGIC order by fc.CaseId desc

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW MI_LocalRequirement AS
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
# MAGIC ,l.* 
# MAGIC ,'GCOB' as SourceSystem
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
# MAGIC ,prl.LatestValidatedRisk as LatestValidatedRisk_High
# MAGIC ,prl.LastValidatedRisk
# MAGIC ,prl.ValidatedRisk_Change
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
# MAGIC from All_LocalRequirement l
# MAGIC left join party_client_details c on l.ClientId = c.ClientId
# MAGIC LEFT JOIN ValidatedRisk_Change prl on l.clientid = prl.clientid
# MAGIC left join party_products_and_services pns on c.sourceclient = pns.sourceclient

# COMMAND ----------

df_MI_LocalRequirement = spark.table('MI_LocalRequirement')
save_to_saradar_storage_account(df_MI_LocalRequirement, MI_LocalRequirement_dataobject)

# COMMAND ----------

# %sql
# CREATE SCHEMA IF NOT EXISTS radar

# COMMAND ----------

# %sql
# drop table IF EXISTS radar.MI_LocalRequirement

# COMMAND ----------

# spark.sql('select * from MI_LocalRequirement').write.mode('overwrite').saveAsTable('radar.MI_LocalRequirement')
