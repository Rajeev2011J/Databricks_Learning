# Databricks notebook source
import os

import pandas as pd
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

jdbcHostname='firebirdsqlserverprodnla.database.windows.net'
ServiceKey='app-reg-databricks-wr-radar-preprd'

# COMMAND ----------

#Read data from FireBird Database and store in GDP Raw layer
jdbcHostname = f'{jdbcHostname}'
jdbcPort = 1433
jdbcDatabase = "APP_ADB"
schemaName='pa'
#jdbcTables =["KYCMasterListRegistry","UserTeamRegistry"]
KYCMasterListRegistry = "pa.KYCMasterListRegistry"
#UserTeamRegistry = "pa.UserTeamRegistry"
portfolioplanning = "dbo.VW_PortfolioPlanning"
UserTeamRegistry = "dbo.VW_UserTeams"
#Service_Principal_Id = f'{app_reg_app_id}'
#Service_Principal_Secret = dbutils.secrets.get(scope = "connectedsecrets", key = f'{ServiceKey}')
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
connectionProperties = {
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication" : "ActiveDirectoryServicePrincipal",
    "UserName" : app_reg_app_id ,
    "Password" : service_credential
}
df_KYCMasterListRegistry = spark.read.jdbc(url=jdbcUrl,table=KYCMasterListRegistry,properties = connectionProperties)
df_UserTeamRegistry = spark.read.jdbc(url=jdbcUrl,table=UserTeamRegistry,properties = connectionProperties)
df_portfolioplanning = spark.read.jdbc(url=jdbcUrl,table=portfolioplanning,properties = connectionProperties)

df_KYCMasterListRegistry.createOrReplaceTempView("KYCMasterListRegistry")
df_UserTeamRegistry.createOrReplaceTempView("UserTeamRegistry")
df_portfolioplanning.createOrReplaceTempView("portfolioplanning")


# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

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
]

# Create TempView for each loading table
for Object in load_df:
    fetch_latest_file(Object, date_parameter, ReadStorage, CaseService=True)

# COMMAND ----------

# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
      , 'client_PartyRole'
]

for item in gcds_tables:
  # get the most recent file available in gdp
  path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4601/data/'
  files = dbutils.fs.ls(path)
  load_date = max(file.path.split('EDL_LOAD_DTS=')[1][:8] for file in files if 'EDL_LOAD_DTS=' in file.path)

  spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4601/data/EDL_LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

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
    fetch_latest_file(Object, date_parameter, ReadStorage, Legacy2=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from Legacy2_case_client_details;--25926
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null; --3006

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
# MAGIC from Legacy2_products_and_sevices pns
# MAGIC Inner join Legacy2_client_details c on pns.ClientId = c.ClientId
# MAGIC ;

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
# MAGIC --,SalesforceClientID_nCino
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
# MAGIC ,'' as ConsultationRequired
# MAGIC ,'' as ConsultationType
# MAGIC ,ClientApprovalDate
# MAGIC ,EDL_LOAD_DTS as EDL_LoadDate 
# MAGIC FROM party_case_client_details
# MAGIC --where 
# MAGIC --CaseStatusName <> 'Cancelled'
# MAGIC --sourceclient <> 'NP_NPPC_3435' --This client has 2 SalesforceClientID_nCino Ids due to which we are getting duplicate values. Sameer will discuss with Gcob to fix it. Once it is fixed this client from filter condition will be removed
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
# MAGIC ,	ScheduledCompletionDate
# MAGIC ,	IsEligibleForFatcaAssessment
# MAGIC ,	cast(legacy2FatcaClassificationId as string) as FatcaClassification
# MAGIC ,	null as FatcaDateOfIssue
# MAGIC ,	IsEligibleForCrsAssessment
# MAGIC ,	cast(legacy2CrsClassificationId as string) as CrsClassification
# MAGIC ,	null as CrsFormSignedDate
# MAGIC ,	null as FullLegalNameInLocalLanguage
# MAGIC ,null as DateOfBirth
# MAGIC --,	DateOfBirth
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
# MAGIC --,	SalesforceClientID_nCino
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
# MAGIC ,null as ClientApprovalDate
# MAGIC ,EDL_LOAD_DTS as EDL_LoadDate 
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

df_case_client_details=spark.table('case_client_details')

# COMMAND ----------

import pyspark.sql.functions as F
df_case_client_details=df_case_client_details.withColumn("BusinessDate",F.lit(BusinessDate))

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Unique_Client_Gcob AS
# MAGIC select gcobid,max(caseid) as caseId,clientType,sourcesystem from party_case_client_details where CaseStatusName <> 'Cancelled' group by gcobid,clientType,sourcesystem;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CTE_MainCaseStatusType AS
# MAGIC SELECT
# MAGIC         c.CaseID,c.ClientId,c.SourceClient
# MAGIC         , CASE 
# MAGIC             WHEN cs.Name IN ('GCOB Review in progress', 'Initiation In Progress') THEN 'Initiation'
# MAGIC             WHEN cs.Name IN ('Ready for KYC assessment') THEN 'Ready for KYC assessment'
# MAGIC             WHEN cs.Name IN ('KYC assessment in progress') THEN 'Assessment'
# MAGIC             WHEN cs.Name IN ('Product Offboarding confirmation in progress', 'Ready for Product Offboarding confirmation', 'Product Offboarding in progress') THEN 'Product Offboarding'
# MAGIC             WHEN cs.Name IN ('Ready for 4 eye check') THEN 'Ready for QC'
# MAGIC             WHEN cs.Name IN ('4 eye check in progress') THEN 'QC'
# MAGIC             WHEN cs.Name IN ('Client committee sign off requested', 'Client Owner approval requested', 'Client owner sign off requested', 'Local client owner sign off requested', 'Senior management sign off requested') THEN 'Sign-off'
# MAGIC             WHEN cs.Name IN ('Product fulfilment in progress') THEN 'Product Fulfillment'
# MAGIC             WHEN cs.Name IN ('Completed', 'Cancelled') THEN cs.Name
# MAGIC             ELSE cs.Name
# MAGIC         END AS MainCaseStatusType
# MAGIC       , CASE 
# MAGIC             WHEN cs.Name IN ('GCOB Review in progress', 'Initiation In Progress') THEN 1
# MAGIC             WHEN cs.Name IN ('Ready for KYC assessment') THEN 3
# MAGIC             WHEN cs.Name IN ('KYC assessment in progress') THEN 4
# MAGIC             WHEN cs.Name IN ('Product Offboarding confirmation in progress', 'Ready for Product Offboarding confirmation', 'Product Offboarding in progress') THEN 15
# MAGIC             WHEN cs.Name IN ('Ready for 4 eye check') THEN 9
# MAGIC             WHEN cs.Name IN ('4 eye check in progress') THEN 10
# MAGIC             WHEN cs.Name IN ('Client committee sign off requested', 'Client Owner approval requested', 'Client owner sign off requested', 'Local client owner sign off requested', 'Senior management sign off requested') THEN 12
# MAGIC             WHEN cs.Name IN ('Product fulfilment in progress') THEN 13
# MAGIC             WHEN cs.Name IN ('Completed', 'Cancelled') THEN 14
# MAGIC             ELSE 13
# MAGIC         END AS MainCaseStatusSortOrder
# MAGIC     FROM party_workitem c
# MAGIC     LEFT JOIN gcob_static_CaseStatusType cs ON c.CaseCurrentStatus = cs.StatusId

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcob_workitems_status AS
# MAGIC With Prework as 
# MAGIC (
# MAGIC   SELECT ClientId,SourceClient, MIN(WorkItemCreatedDate) AS Prework
# MAGIC   FROM party_workitem
# MAGIC   WHERE CaseStatusTypeWhenCreated IN (1, 16)
# MAGIC   GROUP BY ClientId,SourceClient
# MAGIC )
# MAGIC ,
# MAGIC Readyforassessment as
# MAGIC (
# MAGIC   SELECT ClientId,SourceClient, MIN(WorkItemCreatedDate) AS `Ready for assessment`
# MAGIC   FROM party_workitem
# MAGIC   WHERE CaseStatusTypeWhenCreated = 2
# MAGIC   GROUP BY ClientId,SourceClient
# MAGIC )
# MAGIC ,
# MAGIC Assessmentinprogress AS
# MAGIC (
# MAGIC   SELECT ClientId,SourceClient, MIN(WorkItemCreatedDate) AS `Assessment in progress`
# MAGIC   FROM party_workitem
# MAGIC   WHERE CaseStatusTypeWhenCreated = 3
# MAGIC   GROUP BY ClientId,SourceClient
# MAGIC )
# MAGIC ,
# MAGIC 4EYECheck AS
# MAGIC (
# MAGIC   SELECT ClientId,SourceClient, MIN(WorkItemCreatedDate) AS `4-EYE Check`
# MAGIC   FROM party_workitem
# MAGIC   WHERE CaseStatusTypeWhenCreated = 5
# MAGIC   GROUP BY ClientId,SourceClient
# MAGIC )
# MAGIC ,
# MAGIC Signoff as
# MAGIC (
# MAGIC   --SELECT ClientId,SourceClient, MIN(WorkItemCreatedDate) AS `Sign-off`
# MAGIC   SELECT ClientId,SourceClient, MAX(WorkItemCompletedDate) AS `Sign-off`
# MAGIC   FROM party_workitem
# MAGIC   WHERE CaseStatusTypeWhenCreated IN (6, 18)
# MAGIC   GROUP BY ClientId,SourceClient
# MAGIC )
# MAGIC ,
# MAGIC Fulfillment as
# MAGIC (
# MAGIC   SELECT ClientId,SourceClient, MIN(WorkItemCreatedDate) AS Fulfillment
# MAGIC   FROM party_workitem
# MAGIC   WHERE CaseStatusTypeWhenCreated = 8
# MAGIC   GROUP BY ClientId,SourceClient
# MAGIC )
# MAGIC ,
# MAGIC Completed as
# MAGIC (
# MAGIC   SELECT ClientId,SourceClient, MAX(WorkItemCompletedDate) AS Completed
# MAGIC     FROM party_workitem  
# MAGIC     WHERE CaseCurrentStatus = 9 AND WorkItemCompletedDate IS NOT NULL
# MAGIC     GROUP BY ClientId,SourceClient
# MAGIC )
# MAGIC ,
# MAGIC Cancelled as
# MAGIC (
# MAGIC   SELECT ClientId,SourceClient, MAX(WorkItemCompletedDate) AS Cancelled
# MAGIC   FROM party_workitem
# MAGIC   WHERE CaseStatusTypeWhenCreated = 10 AND WorkItemCompletedDate IS NOT NULL
# MAGIC   GROUP BY ClientId,SourceClient
# MAGIC )
# MAGIC ,
# MAGIC QCInteractions as
# MAGIC (
# MAGIC   SELECT ClientId,SourceClient, COUNT(*) AS QCInteractions
# MAGIC   FROM party_workitem
# MAGIC   WHERE CaseStatusName = 'Ready for 4 eye check'
# MAGIC   GROUP BY ClientId,SourceClient
# MAGIC )
# MAGIC ,
# MAGIC PreworkAnalystdate as
# MAGIC (
# MAGIC   SELECT DISTINCT t1.ClientId,t1.SourceClient
# MAGIC ,CASE WHEN t1.WorkItemAssignedUserName = 'Unknown' THEN t3.WorkItemAssignedUserName ELSE t1.WorkItemAssignedUserName END AS `Prework analyst`
# MAGIC ,CASE WHEN t1.WorkItemAssignedUserName = 'Unknown' THEN SUBSTRING(t1.CreatingUserID, INSTR(t1.CreatingUserID, '\\') + 1)     ELSE SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\') + 1) END AS `Prework Alias`
# MAGIC , MaxDate AS `Prework analyst team date`
# MAGIC FROM party_workitem t1
# MAGIC INNER JOIN (
# MAGIC   SELECT t1.ClientId,t1.SourceClient, MAX(WorkItemCreatedDate) AS MaxDate
# MAGIC   FROM party_workitem t1
# MAGIC     LEFT JOIN (
# MAGIC     SELECT ClientId,SourceClient, MIN(WorkItemCreatedDate) AS `Ready for assessment`
# MAGIC     FROM party_workitem
# MAGIC     WHERE CaseStatusTypeWhenCreated = 2
# MAGIC     GROUP BY ClientId,SourceClient
# MAGIC     ) t4 ON t1.ClientId = t4.ClientId and t1.SourceClient = t4.SourceClient
# MAGIC   WHERE t1.CaseStatusTypeWhenCreated IN (1, 16) AND WorkItemCreatedDate <= COALESCE(T4.`Ready for assessment`, current_date())
# MAGIC   GROUP BY t1.ClientId,t1.SourceClient
# MAGIC   ) t2 ON t1.ClientId = t2.ClientId and t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
# MAGIC   LEFT JOIN (
# MAGIC   SELECT ClientId,SourceClient, WorkItemAssignedUserName, AssignedUserId, CaseStatusTypeWhenCreated, WorkItemCreatedDate
# MAGIC   FROM party_workitem
# MAGIC   WHERE CaseStatusTypeWhenCreated IN (1)
# MAGIC   ) t3 ON t2.ClientId = t3.ClientId and t2.SourceClient = t3.SourceClient AND t2.MaxDate = t3.WorkItemCreatedDate
# MAGIC WHERE t1.CaseStatusTypeWhenCreated IN (1, 16)
# MAGIC )
# MAGIC ,
# MAGIC AssessmentAnalystDate AS
# MAGIC (
# MAGIC   -- selects the last analyst on Status Assessment
# MAGIC   SELECT DISTINCT 
# MAGIC     t1.ClientId,t1.SourceClient,
# MAGIC     CASE WHEN t1.WorkItemAssignedUserName = 'Unknown' THEN t3.WorkItemAssignedUserName ELSE t1.WorkItemAssignedUserName END AS `Assessment analyst`,
# MAGIC     SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\') + 1) AS `Assessment Alias`,
# MAGIC     t2.MaxDate AS `Assessment analyst team date`
# MAGIC   FROM party_workitem t1  
# MAGIC   INNER JOIN ( 
# MAGIC         SELECT ClientId,SourceClient, MAX(WorkItemCreatedDate) AS MaxDate 
# MAGIC         FROM party_workitem 
# MAGIC         WHERE CaseStatusTypeWhenCreated IN (3, 20, 21)
# MAGIC         GROUP BY ClientId,SourceClient
# MAGIC     ) t2 ON t1.ClientId = t2.ClientId and t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
# MAGIC   LEFT JOIN (SELECT ClientId,SourceClient, WorkItemAssignedUserName, AssignedUserId, CaseStatusTypeWhenCreated, WorkItemCreatedDate 
# MAGIC              FROM party_workitem 
# MAGIC              WHERE CaseStatusTypeWhenCreated IN (2, 19, 20)) t3 
# MAGIC   ON t2.ClientId = t3.ClientId and t2.SourceClient = t3.SourceClient AND t2.MaxDate = t3.WorkItemCreatedDate
# MAGIC   WHERE t1.CaseStatusTypeWhenCreated IN (3, 20, 21)
# MAGIC )
# MAGIC ,
# MAGIC 4EYEAnalystDate as
# MAGIC (
# MAGIC   SELECT DISTINCT 
# MAGIC   t1.ClientId,t1.SourceClient,
# MAGIC   t1.WorkItemAssignedUserName AS `4-EYE analyst`,
# MAGIC   SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\') + 1) AS `4-EYE Alias`,
# MAGIC   t2.MaxDate AS `4-EYE analyst team date`
# MAGIC   FROM party_workitem t1 
# MAGIC   INNER JOIN ( 
# MAGIC     SELECT 
# MAGIC     t3.ClientId, t3.SourceClient,
# MAGIC     MAX(t3.WorkItemCreatedDate) AS MaxDate 
# MAGIC     FROM party_workitem t3
# MAGIC       LEFT JOIN (
# MAGIC       SELECT 
# MAGIC       ClientId, SourceClient,
# MAGIC       MIN(WorkItemCreatedDate) AS `Sign-off` 
# MAGIC       FROM party_workitem 
# MAGIC       WHERE CaseStatusTypeWhenCreated = 6 
# MAGIC       GROUP BY ClientId, SourceClient
# MAGIC       ) t4 ON t3.ClientId = t4.ClientId and t3.SourceClient = t4.SourceClient 
# MAGIC     WHERE t3.CaseStatusTypeWhenCreated = 5 AND t3.WorkItemCreatedDate <= COALESCE(t4.`Sign-off`, CURRENT_DATE())
# MAGIC     GROUP BY t3.ClientId,t3.SourceClient
# MAGIC     ) t2 ON t1.ClientId = t2.ClientId and t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
# MAGIC   WHERE t1.CaseStatusTypeWhenCreated = 5
# MAGIC )
# MAGIC ,
# MAGIC LastAnalyst as
# MAGIC (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.ClientId, t1.SourceClient,
# MAGIC     t1.WorkItemAssignedUserName AS `Last analyst`,
# MAGIC     SUBSTRING(t1.AssignedUserID, INSTR(t1.AssignedUserId, '\\') + 1) AS `Last Analyst Alias`,
# MAGIC     MaxDate AS `Last analyst team date`
# MAGIC   FROM party_workitem t1
# MAGIC   INNER JOIN (
# MAGIC         SELECT ClientId,SourceClient, MAX(WorkItemCreatedDate) AS MaxDate
# MAGIC         FROM party_workitem
# MAGIC         WHERE ResponsibleRole IN (2, 3)
# MAGIC           AND CaseStatusTypeWhenCreated NOT IN (5, 4, 2) -- ('4 eye check in progress', 'Ready for 4 eye check', 'Ready for KYC assessment')
# MAGIC         GROUP BY ClientId,SourceClient
# MAGIC     ) t2 ON t1.ClientId = t2.ClientId and t1.SourceClient = t2.SourceClient AND t1.WorkItemCreatedDate = t2.MaxDate
# MAGIC   WHERE ResponsibleRole IN (2, 3) AND CaseStatusTypeWhenCreated NOT IN (5, 4, 2)
# MAGIC )
# MAGIC
# MAGIC select distinct
# MAGIC t1.ClientId
# MAGIC , t1.SourceClient
# MAGIC , t1.CaseId
# MAGIC , t2.Prework
# MAGIC , t10.`Prework analyst`
# MAGIC , t10.`Prework analyst team date`
# MAGIC , t3.`Ready for assessment`
# MAGIC , t4.`Assessment in progress`
# MAGIC , t11.`Assessment analyst`
# MAGIC , t11.`Assessment analyst team date`
# MAGIC , t5.`4-EYE Check`
# MAGIC , t12.`4-EYE analyst`
# MAGIC , t12.`4-EYE analyst team date`
# MAGIC , t6.`Sign-off`
# MAGIC , t7.Fulfillment
# MAGIC , t8.Completed
# MAGIC , t9.Cancelled
# MAGIC ,case when DATEDIFF(COALESCE(t3.`Ready for assessment`, current_date()), t2.Prework) = 0 then 1 else DATEDIFF(COALESCE(t3.`Ready for assessment`, current_date()), t2.Prework) end as DaysInPrework
# MAGIC ,case when DATEDIFF(COALESCE(t4.`Assessment in progress`, current_date()), t3.`Ready for assessment`)= 0 then 1 else DATEDIFF(COALESCE(t4.`Assessment in progress`, current_date()), t3.`Ready for assessment`) end as DaysInReadyForAssessment
# MAGIC ,case when DATEDIFF(COALESCE(t5.`4-EYE Check`, current_date()), t4.`Assessment in progress`)= 0 then 1 else DATEDIFF(COALESCE(t5.`4-EYE Check`, current_date()), t4.`Assessment in progress`) end as DaysInAssessmentInProgress
# MAGIC ,case when DATEDIFF(COALESCE(t6.`Sign-off`, current_date()), t5.`4-EYE Check`)= 0 then 1 else DATEDIFF(COALESCE(t6.`Sign-off`, current_date()), t5.`4-EYE Check`) end as DaysIn4EYECheck
# MAGIC ,case when DATEDIFF(COALESCE(t7.Fulfillment, current_date()), t6.`Sign-off`)= 0 then 1 else DATEDIFF(COALESCE(t7.Fulfillment, current_date()), t6.`Sign-off`) end as `DaysInSign-off`
# MAGIC ,case when DATEDIFF(COALESCE(t8.Completed, current_date()), t7.Fulfillment)= 0 then 1 else DATEDIFF(COALESCE(t8.Completed, current_date()), t7.Fulfillment) end as DaysInFulfillment
# MAGIC ,case when DATEDIFF(COALESCE(t8.Completed, current_date()), t2.Prework)= 0 then 1 else DATEDIFF(COALESCE(t8.Completed, current_date()), t2.Prework) end as TotalCaseDuration
# MAGIC , CASE
# MAGIC   WHEN t9.Cancelled IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 8)
# MAGIC   WHEN t8.Completed IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 7)
# MAGIC   WHEN t7.Fulfillment IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 6)
# MAGIC   WHEN t6.`Sign-off` IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 5)
# MAGIC   WHEN t5.`4-EYE Check` IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 4)
# MAGIC   WHEN t4.`Assessment in progress` IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 3)
# MAGIC   WHEN t3.`Ready for assessment` IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 2)
# MAGIC   WHEN t2.Prework IS NOT NULL THEN (SELECT PhaseName FROM gcob_static_PhaseLookup WHERE PhaseID = 1)
# MAGIC   ELSE 'UNKNOWN'
# MAGIC   END AS `Case phase`  
# MAGIC , CASE
# MAGIC   WHEN t9.Cancelled IS NOT NULL THEN 8
# MAGIC   WHEN t8.Completed IS NOT NULL THEN 7
# MAGIC   WHEN t7.Fulfillment IS NOT NULL THEN 6
# MAGIC   WHEN t6.`Sign-off` IS NOT NULL THEN 5
# MAGIC   WHEN t5.`4-EYE Check` IS NOT NULL THEN 4
# MAGIC   WHEN t4.`Assessment in progress` IS NOT NULL THEN 3
# MAGIC   WHEN t3.`Ready for assessment` IS NOT NULL THEN 2
# MAGIC   WHEN t2.Prework IS NOT NULL THEN 1
# MAGIC   ELSE 99999
# MAGIC   END AS `Case phase sort order`
# MAGIC , t13.QCInteractions
# MAGIC , t14.`Last analyst`
# MAGIC , t14.`Last analyst team date`
# MAGIC , t10.`Prework Alias`
# MAGIC , t11.`Assessment Alias`
# MAGIC , t12.`4-EYE Alias`
# MAGIC , t14.`Last Analyst Alias`
# MAGIC
# MAGIC from party_workitem t1
# MAGIC LEFT JOIN Prework t2 ON t1.ClientId = t2.ClientId AND t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN Readyforassessment t3 ON t1.ClientId = t3.ClientId AND t1.SourceClient = t3.SourceClient
# MAGIC LEFT JOIN Assessmentinprogress t4 ON t1.ClientId = t4.ClientId AND t1.SourceClient = t4.SourceClient
# MAGIC LEFT JOIN 4EYECheck t5 ON t1.ClientId = t5.ClientId AND t1.SourceClient = t5.SourceClient
# MAGIC LEFT JOIN Signoff t6 ON t1.ClientId = t6.ClientId AND t1.SourceClient = t6.SourceClient
# MAGIC LEFT JOIN Fulfillment t7 ON t1.ClientId = t7.ClientId AND t1.SourceClient = t7.SourceClient
# MAGIC LEFT JOIN Completed t8 ON t1.ClientId = t8.ClientId AND t1.SourceClient = t8.SourceClient
# MAGIC LEFT JOIN Cancelled t9 ON t1.ClientId = t9.ClientId AND t1.SourceClient = t9.SourceClient
# MAGIC LEFT JOIN QCInteractions t13 ON t1.ClientId = t13.ClientId AND t1.SourceClient = t13.SourceClient
# MAGIC LEFT JOIN PreworkAnalystdate t10 ON t1.ClientId = t10.ClientId AND t1.SourceClient = t10.SourceClient
# MAGIC LEFT JOIN AssessmentAnalystDate t11 ON t1.ClientId = t11.ClientId AND t1.SourceClient = t11.SourceClient
# MAGIC LEFT JOIN 4EYEAnalystDate t12 ON t1.ClientId = t12.ClientId AND t1.SourceClient = t12.SourceClient
# MAGIC LEFT JOIN LastAnalyst t14 ON t1.ClientId = t14.ClientId AND t1.SourceClient = t14.SourceClient
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Original_gcob_rfi_status AS
# MAGIC SELECT DISTINCT
# MAGIC     T3.RequestStatusNumber,T3.`Number of RFI` as NumberofRFI,
# MAGIC     T1.CaseId,T1.ClientId,T1.SourceClient,
# MAGIC     COALESCE(T3.`Number of RFI`, 0) AS `Number of RFI`,
# MAGIC     CASE 
# MAGIC         WHEN T1.`Case phase` IN ('Cancelled', 'Completed') THEN T1.`Case phase`
# MAGIC         WHEN T3.RequestStatusNumber IN (1) AND T3.`Number of RFI` = 1 THEN 'Client Outreach'
# MAGIC         WHEN T3.RequestStatusNumber IN (1) AND T3.`Number of RFI` > 1 THEN 'Rebound Client Outreach'
# MAGIC         WHEN T3.RequestStatusNumber = 2 THEN 'Client Outreach Completed'
# MAGIC     END AS `Case phase`,
# MAGIC     CASE
# MAGIC         WHEN T1.`Case phase` <> 'Assessment in progress' THEN T1.`Case phase`
# MAGIC         WHEN T2.CaseId IS NULL THEN 'Execute Assessment'
# MAGIC         WHEN T3.`Number of RFI` = 1 AND T3.RequestStatusNumber = 1 THEN 'Waiting for information'
# MAGIC         WHEN T3.`Number of RFI` = 1 AND T3.RequestStatusNumber = 2 THEN 'Reviewing information'
# MAGIC         WHEN T3.`Number of RFI` >= 1 AND T3.RequestStatusNumber = 3 THEN 'Finalise assessment'
# MAGIC         WHEN T3.`Number of RFI` > 1 AND T3.RequestStatusNumber = 1 THEN 'Waiting for additional information'
# MAGIC         WHEN T3.`Number of RFI` > 1 AND T3.RequestStatusNumber = 2 THEN 'Reviewing additional information'
# MAGIC     END AS `Case phase including RFI`,
# MAGIC     CASE
# MAGIC         WHEN T1.`Case phase` <> 'Assessment in progress' THEN T1.`Case phase sort order`
# MAGIC         WHEN T2.CaseId IS NULL THEN 3.1
# MAGIC         WHEN T3.`Number of RFI` = 1 AND T3.RequestStatusNumber = 1 THEN 3.2
# MAGIC         WHEN T3.`Number of RFI` = 1 AND T3.RequestStatusNumber = 2 THEN 3.2
# MAGIC         WHEN T3.`Number of RFI` >= 1 AND T3.RequestStatusNumber = 3 THEN 3.3
# MAGIC         WHEN T3.`Number of RFI` > 1 AND T3.RequestStatusNumber = 1 THEN 3.4
# MAGIC         WHEN T3.`Number of RFI` > 1 AND T3.RequestStatusNumber = 2 THEN 3.5
# MAGIC     END AS `Case phase including RFI sort order`,
# MAGIC     T1.`Ready for assessment`,
# MAGIC     (DATEDIFF(T4.DateCreated, T1.`Ready for assessment`) + 1) AS `Days in Execute Assessment`,
# MAGIC     T4.DateCreated AS DateCreated,
# MAGIC     (DATEDIFF(COALESCE(T4.DateResponded, current_date()), T4.DateCreated) + 1) AS `Days in Waiting for information`,
# MAGIC     T4.DateResponded AS DateResponded,
# MAGIC     (DATEDIFF(COALESCE(T4.DateCompleted, current_date()), T4.DateResponded) + 1) AS `Days in Reviewing information`,
# MAGIC     T4.DateCompleted AS DateCompleted,
# MAGIC     (
# MAGIC         CASE 
# MAGIC             WHEN T4.CaseID IS NOT NULL THEN 
# MAGIC                 DATEDIFF(T1.`4-EYE Check`, T4.DateCompleted)
# MAGIC             ELSE 
# MAGIC                 DATEDIFF(T1.`4-EYE Check`, T1.`Assessment in progress`)
# MAGIC         END
# MAGIC     ) + 1 AS `Days in Finalise assessment`,
# MAGIC     T1.`4-EYE Check`
# MAGIC ,(DATEDIFF(IFNULL(T2.DateCompleted, CURRENT_DATE()), T2.DateCreated) + 1) AS `DurationInfoRequestInDay`,
# MAGIC CASE WHEN T2.DateCompleted IS NOT NULL THEN 'Yes' ELSE 'No' END AS `Request completed`,
# MAGIC T2.RequestTypeId
# MAGIC FROM gcob_workitems_status T1
# MAGIC LEFT JOIN party_request_for_information T2 ON T1.CaseId = T2.CaseId
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT
# MAGIC         CaseId, 
# MAGIC         MIN(DateCreated) AS MinDateCreated, 
# MAGIC         COUNT(DISTINCT RequestTypeId) AS `Number of RFI`, 
# MAGIC         MIN(CASE WHEN StatusType = 'PendingResponse' THEN 1 WHEN StatusType = 'PendingReview'  THEN 2 WHEN StatusType = 'Completed' THEN 3 END) As RequestStatusNumber
# MAGIC         --MIN(StatusId) AS RequestStatusNumber 
# MAGIC     FROM party_request_for_information 
# MAGIC     GROUP BY CaseId
# MAGIC ) T3 ON T2.CaseId = T3.CaseId
# MAGIC LEFT JOIN (
# MAGIC     SELECT * 
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY CaseId, DateCreated ORDER BY RequestTypeId DESC) as rn
# MAGIC         FROM party_request_for_information
# MAGIC         WHERE DateCreated IS NOT NULL
# MAGIC     ) tmp
# MAGIC     WHERE rn = 1
# MAGIC ) T4 ON T3.CaseId = T4.CaseId AND T3.MinDateCreated = T4.DateCreated
# MAGIC where T1.SourceClient like 'LEC%'
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gcob_rfi_status AS
# MAGIC SELECT DISTINCT
# MAGIC     --T3.RequestStatusNumber,T3.`Number of RFI` as NumberofRFI,
# MAGIC     T1.CaseId,T1.ClientId,T1.SourceClient,
# MAGIC     COALESCE(T3.`Number of RFI`, 0) AS `Number of RFI`,
# MAGIC     CASE 
# MAGIC         WHEN T1.`Case phase` IN ('Cancelled', 'Completed') THEN T1.`Case phase`
# MAGIC         WHEN T3.RequestStatusNumber IN (1) AND T3.`Number of RFI` = 1 THEN 'Client Outreach'
# MAGIC         WHEN T3.RequestStatusNumber IN (1) AND T3.`Number of RFI` > 1 THEN 'Rebound Client Outreach'
# MAGIC         WHEN T3.RequestStatusNumber = 2 THEN 'Client Outreach Completed'
# MAGIC     END AS `Case phase`,
# MAGIC     CASE
# MAGIC         WHEN T1.`Case phase` <> 'Assessment in progress' THEN T1.`Case phase`
# MAGIC         WHEN T2.CaseId IS NULL THEN 'Execute Assessment'
# MAGIC         WHEN T3.`Number of RFI` = 1 AND T3.RequestStatusNumber = 1 THEN 'Waiting for information'
# MAGIC         WHEN T3.`Number of RFI` = 1 AND T3.RequestStatusNumber = 2 THEN 'Reviewing information'
# MAGIC         WHEN T3.`Number of RFI` >= 1 AND T3.RequestStatusNumber = 3 THEN 'Finalise assessment'
# MAGIC         WHEN T3.`Number of RFI` > 1 AND T3.RequestStatusNumber = 1 THEN 'Waiting for additional information'
# MAGIC         WHEN T3.`Number of RFI` > 1 AND T3.RequestStatusNumber = 2 THEN 'Reviewing additional information'
# MAGIC     END AS `Case phase including RFI`,
# MAGIC     CASE
# MAGIC         WHEN T1.`Case phase` <> 'Assessment in progress' THEN T1.`Case phase sort order`
# MAGIC         WHEN T2.CaseId IS NULL THEN 3.1
# MAGIC         WHEN T3.`Number of RFI` = 1 AND T3.RequestStatusNumber = 1 THEN 3.2
# MAGIC         WHEN T3.`Number of RFI` = 1 AND T3.RequestStatusNumber = 2 THEN 3.2
# MAGIC         WHEN T3.`Number of RFI` >= 1 AND T3.RequestStatusNumber = 3 THEN 3.3
# MAGIC         WHEN T3.`Number of RFI` > 1 AND T3.RequestStatusNumber = 1 THEN 3.4
# MAGIC         WHEN T3.`Number of RFI` > 1 AND T3.RequestStatusNumber = 2 THEN 3.5
# MAGIC     END AS `Case phase including RFI sort order`,
# MAGIC     T1.`Ready for assessment`,
# MAGIC     --(DATEDIFF(T4.DateCreated, T1.`Ready for assessment`) + 1) AS `Days in Execute Assessment`,
# MAGIC     DATEDIFF(DAY, T1.`Ready for assessment`, T4.DateCreated) + 1 - (((DATEDIFF(T1.`Ready for assessment`, T4.DateCreated) * 2) + (CASE WHEN date_format(T1.`Ready for assessment`, "EEEE")   = 'Sunday'  THEN 1 ELSE 0 END) + (CASE WHEN date_format(T4.DateCreated, "EEEE") = 'Saturday'    THEN 1 ELSE 0 END))) AS `Days in Execute Assessment`,
# MAGIC     T4.DateCreated AS DateCreated,
# MAGIC     --(DATEDIFF(COALESCE(T4.DateResponded, current_date()), T4.DateCreated) + 1) AS `Days in Waiting for information`,
# MAGIC     DATEDIFF(DAY, T4.DateCreated, IFNULL(T4.DateResponded, GETDATE())) + 1 - (((DATEDIFF(T4.DateCreated, IFNULL(T4.DateResponded, GETDATE())) * 2) + (CASE WHEN date_format(T4.DateCreated, "EEEE") = 'Sunday'  THEN 1 ELSE 0 END) + (CASE WHEN date_format(T4.DateResponded, "EEEE") = 'Saturday'  THEN 1 ELSE 0 END))) AS `Days in Waiting for information`,
# MAGIC     T4.DateResponded AS DateResponded,
# MAGIC     --(DATEDIFF(COALESCE(T4.DateCompleted, current_date()), T4.DateResponded) + 1) AS `Days in Reviewing information`,
# MAGIC     DATEDIFF(DAY, T4.DateResponded, IFNULL(T4.DateCompleted,GETDATE())) + 1 - (((DATEDIFF(T4.DateResponded, IFNULL(T4.DateCompleted,GETDATE())) * 2) + (CASE WHEN date_format(T4.DateResponded, "EEEE") = 'Sunday'  THEN 1 ELSE 0 END) + (CASE WHEN date_format(T4.DateCompleted, "EEEE") = 'Saturday'  THEN 1 ELSE 0 END))) AS `Days in Reviewing information`,
# MAGIC     T4.DateCompleted AS DateCompleted,
# MAGIC     --(CASE WHEN T4.CaseID IS NOT NULL THEN DATEDIFF(T1.`4-EYE Check`, T4.DateCompleted) ELSE DATEDIFF(T1.`4-EYE Check`, T1.`Assessment in progress`) END ) + 1 AS `Days in Finalise assessment`,
# MAGIC     (Case when T4.CaseID IS NOT NULL then DATEDIFF(DAY, T4.DateCompleted, T1.`4-EYE Check`) + 1 - (((DATEDIFF(T4.DateCompleted, T1.`4-EYE Check`) * 2) + (CASE WHEN  date_format(T4.DateCompleted, "EEEE")    = 'Sunday'  THEN 1 ELSE 0 END) + (CASE WHEN date_format(T1.`4-EYE Check`, "EEEE") = 'Saturday' THEN 1 ELSE 0 END)))
# MAGIC       when T4.CaseID IS NULL then DATEDIFF(DAY, T1.`Assessment in progress`, T1.`4-EYE Check`) + 1 - (((DATEDIFF(T1.`Assessment in progress`, T1.`4-EYE Check`) * 2) + (CASE WHEN date_format(T1.`Assessment in progress`, "EEEE") = 'Sunday'  THEN 1 ELSE 0 END) + (CASE WHEN date_format(T1.`4-EYE Check`, "EEEE") = 'Saturday' THEN 1 ELSE 0 END)))
# MAGIC     else NULL end) + 1 AS `Days in Finalise assessment`,
# MAGIC     T1.`4-EYE Check`
# MAGIC --,(DATEDIFF(IFNULL(T2.DateCompleted, CURRENT_DATE()), T2.DateCreated) + 1) AS `DurationInfoRequestInDay`
# MAGIC ,DATEDIFF(DAY, T2.DateCreated, IFNULL(T2.DateCompleted, GETDATE())) + 1 - (((DATEDIFF(T2.DateCreated, IFNULL(T2.DateCompleted, GETDATE())) * 2) + (CASE WHEN date_format(T2.DateCreated, "EEEE")  = 'Sunday'  THEN 1 ELSE 0 END) + (CASE WHEN date_format(T2.DateCompleted, "EEEE") = 'Saturday'  THEN 1 ELSE 0 END))) AS DurationInfoRequestInDay
# MAGIC ,CASE WHEN T2.DateCompleted IS NOT NULL THEN 'Yes' ELSE 'No' END AS `Request completed`,
# MAGIC T2.RequestTypeId
# MAGIC FROM gcob_workitems_status T1
# MAGIC --inner join Unique_Client_Gcob ucg on T1.Caseid = ucg.Caseid
# MAGIC LEFT JOIN party_request_for_information T2 ON T1.CaseId = T2.CaseId
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT
# MAGIC         CaseId, 
# MAGIC         MIN(DateCreated) AS MinDateCreated, 
# MAGIC         COUNT(RequestTypeId) AS `Number of RFI`, 
# MAGIC         MIN(CASE WHEN StatusType = 'PendingResponse' THEN 1 WHEN StatusType = 'PendingReview'  THEN 2 WHEN StatusType = 'Completed' THEN 3 END) As RequestStatusNumber
# MAGIC         --MIN(StatusId) AS RequestStatusNumber 
# MAGIC     FROM party_request_for_information 
# MAGIC     GROUP BY CaseId
# MAGIC ) T3 ON T2.CaseId = T3.CaseId
# MAGIC /*
# MAGIC LEFT JOIN (select * from party_request_for_information where RequestTypeId NOT IN 
# MAGIC -- The following De-selects the later one of the duplicated RFI's in GCOB
# MAGIC (select distinct t1.RequestTypeId
# MAGIC from party_request_for_information t1
# MAGIC inner join (
# MAGIC select CaseId, DateCreated, count(*) as cnt
# MAGIC from party_request_for_information
# MAGIC Where DateCreated is not null
# MAGIC group by CaseId, DateCreated
# MAGIC HAVING count(*) > 1) t2 on t1.Caseid = t2.CaseId and t1.DateCreated = t2.DateCreated
# MAGIC )) T4 ON T3.CaseId = T4.CaseId AND T3.MinDateCreated = T4.DateCreated
# MAGIC */
# MAGIC LEFT JOIN (
# MAGIC     SELECT * 
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY CaseId, DateCreated ORDER BY RequestTypeId DESC) as rn
# MAGIC         FROM party_request_for_information
# MAGIC         WHERE DateCreated IS NOT NULL
# MAGIC     ) tmp
# MAGIC     WHERE rn = 1
# MAGIC ) T4 ON T3.CaseId = T4.CaseId AND T3.MinDateCreated = T4.DateCreated
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CTE_PreviousRisk_Case AS
# MAGIC with CaseClientDetails as 
# MAGIC (
# MAGIC     select distinct CaseId,ClientId,SourceClient,GcobId,ValidatedRiskLevel,FullLegalName,clienttype,SourceSystem,
# MAGIC     AdverseInfoRiskLevel,GeographicalRiskLevel,EntityTypeRiskLevel,StructureRiskLevel,SectorRiskLevel,ProductAndServiceRiskLevel,PEPRiskLevel,TransactionRiskLevel,DistributionRiskLevel,ThirdPartyRiskLevel
# MAGIC     ,FatcaClassification,CrsClassification
# MAGIC     From party_case_client_details
# MAGIC )
# MAGIC
# MAGIC ,PreviousRiskLevel as (
# MAGIC SELECT DISTINCT 
# MAGIC     CaseId,ClientId,SourceClient,GcobId--,ValidatedRiskLevel,FullLegalName
# MAGIC     --,LAG(ValidatedRiskLevel, 1) OVER (PARTITION BY GcobId ORDER BY SourceSystem desc, COALESCE(FinalDecisionDate, NextReviewDate) desc) AS `Previous risk level`
# MAGIC     --,LAG(CaseId, 1) OVER (PARTITION BY GcobId ORDER BY SourceSystem desc, COALESCE(NextReviewDate, ClientOwnerSignOffDate) desc) AS PreviousCaseId
# MAGIC     --,row_number() over (partition by GcobId ORDER BY SourceSystem desc, COALESCE(FinalDecisionDate, NextReviewDate) desc) as Seq
# MAGIC     ,LAG(ValidatedRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS `Previous risk level`
# MAGIC     ,LAG(CaseId, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousCaseId
# MAGIC     ,LAG(AdverseInfoRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousAdverseInfoRiskLevel
# MAGIC     ,LAG(GeographicalRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousGeographicalRiskLevel
# MAGIC     ,LAG(EntityTypeRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousEntityTypeRiskLevel
# MAGIC     ,LAG(StructureRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousStructureRiskLevel
# MAGIC     ,LAG(SectorRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousSectorRiskLevel
# MAGIC     ,LAG(ProductAndServiceRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousProductAndServiceRiskLevel
# MAGIC     ,LAG(PEPRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousPEPRiskLevel
# MAGIC     ,LAG(TransactionRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousTransactionRiskLevel
# MAGIC     ,LAG(DistributionRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousDistributionRiskLevel
# MAGIC     ,LAG(ThirdPartyRiskLevel, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousThirdPartyRiskLevel
# MAGIC     ,LAG(FatcaClassification, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousFatcaClassification
# MAGIC     ,LAG(CrsClassification, 1) OVER (PARTITION BY GcobId,clienttype ORDER BY CaseId asc, SourceSystem desc, clienttype asc) AS PreviousCrsClassification
# MAGIC     ,row_number() over (partition by GcobId,clienttype ORDER BY CaseId asc,SourceSystem desc) as Seq
# MAGIC     FROM CaseClientDetails
# MAGIC --where gcobid in (926)--,3435,926)
# MAGIC --SourceClient in ('NP_NPPC_3435','LEC_1476') 
# MAGIC )
# MAGIC select distinct
# MAGIC GcobId
# MAGIC ,CaseId
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousCaseId end as PreviousCaseId
# MAGIC ,ClientId,SourceClient
# MAGIC ,case when CaseId = PreviousCaseId then null else `Previous risk level` end as `Previous risk level`
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousAdverseInfoRiskLevel end as PreviousAdverseInfoRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousGeographicalRiskLevel end as PreviousGeographicalRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousEntityTypeRiskLevel end as PreviousEntityTypeRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousStructureRiskLevel end as PreviousStructureRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousSectorRiskLevel end as PreviousSectorRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousProductAndServiceRiskLevel end as PreviousProductAndServiceRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousPEPRiskLevel end as PreviousPEPRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousTransactionRiskLevel end as PreviousTransactionRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousDistributionRiskLevel end as PreviousDistributionRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousThirdPartyRiskLevel end as PreviousThirdPartyRiskLevel
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousFatcaClassification end as PreviousFatcaClassification
# MAGIC ,case when CaseId = PreviousCaseId then null else PreviousCrsClassification end as PreviousCrsClassification
# MAGIC --,FullLegalName,ValidatedRiskLevel,Seq 
# MAGIC from PreviousRiskLevel 
# MAGIC --where Seq = 2
# MAGIC order by CaseId

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW workitems_status AS
# MAGIC With CTE_ReboundClientOutreach AS (
# MAGIC SELECT
# MAGIC MIN(DurationInfoRequestInDay) AS DurationInfoRequestInDay
# MAGIC , CaseID, ClientId,SourceClient
# MAGIC FROM gcob_rfi_status
# MAGIC WHERE
# MAGIC DateCompleted IS NULL and `Request completed` = 'No' -- DateCompleted IS NULL --> `Request status` = 'PendingResponse'
# MAGIC GROUP BY CaseID, ClientId,SourceClient
# MAGIC )
# MAGIC /*
# MAGIC ,
# MAGIC CTE_PreviousRiskLevel AS (
# MAGIC     SELECT DISTINCT 
# MAGIC     CaseId,ClientId,SourceClient,
# MAGIC     LAG(ValidatedRiskLevel, 1) OVER (
# MAGIC         PARTITION BY GcobId 
# MAGIC         ORDER BY 
# MAGIC             COALESCE(FinalDecisionDate, NextReviewDate) ASC,
# MAGIC             CaseId ASC
# MAGIC     ) AS `Previous risk level`
# MAGIC     FROM
# MAGIC         party_case_client_details
# MAGIC )
# MAGIC */
# MAGIC Select DISTINCT
# MAGIC --w.*,
# MAGIC w.ClientId
# MAGIC ,w.SourceClient
# MAGIC ,w.CaseId
# MAGIC ,w.Prework
# MAGIC ,w.`Prework analyst`
# MAGIC ,w.`Prework analyst team date`
# MAGIC ,w.`Ready for assessment`
# MAGIC ,w.`Assessment in progress`
# MAGIC ,w.`Assessment analyst`
# MAGIC ,w.`Assessment analyst team date`
# MAGIC ,w.`4-EYE Check`
# MAGIC ,w.`4-EYE analyst`
# MAGIC ,w.`4-EYE analyst team date`
# MAGIC ,w.`Sign-off`
# MAGIC ,w.Fulfillment
# MAGIC ,w.Completed
# MAGIC ,w.Cancelled
# MAGIC ,w.DaysInPrework
# MAGIC ,w.DaysInReadyForAssessment
# MAGIC ,w.DaysInAssessmentInProgress
# MAGIC ,w.DaysIn4EYECheck
# MAGIC ,w.`DaysInSign-off`
# MAGIC ,w.DaysInFulfillment
# MAGIC ,w.TotalCaseDuration
# MAGIC --,w.`Case phase sort order`
# MAGIC ,w.QCInteractions
# MAGIC ,w.`Last analyst`
# MAGIC ,w.`Last analyst team date`
# MAGIC ,w.`Prework Alias`
# MAGIC ,w.`Assessment Alias`
# MAGIC ,w.`4-EYE Alias`
# MAGIC ,w.`Last Analyst Alias`
# MAGIC , t40.DateResponded AS `Responded date first RFI`
# MAGIC , t40.DateCompleted AS `Completed date first RFI`
# MAGIC , (SELECT MIN(RFI.DateCreated) FROM gcob_rfi_status RFI WHERE w.CaseID =  RFI.CaseID AND RequestTypeID = 1) AS `First RFI created` -- 1 = 'Request For Information'
# MAGIC , (SELECT MAX(RFI.DateCompleted) FROM gcob_rfi_status RFI WHERE w.CaseID =  RFI.CaseID AND RequestTypeID = 1) AS `Latest RFI completed` -- 1 = 'Request For Information'
# MAGIC , CASE
# MAGIC   WHEN t41.MainCaseStatusType IN ('Completed', 'Cancelled') THEN t41.MainCaseStatusType
# MAGIC   WHEN acr.ReviewTypeName = 'Client Offboarding' THEN 'Offboarding'
# MAGIC   WHEN t41.MainCaseStatusType = 'Sign-off' THEN 'Sign-off'
# MAGIC   WHEN t40.`Case phase` IN ('Rebound Client Outreach', 'Client Outreach', 'Client Outreach Completed') THEN t40.`Case phase`
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND w.`4-EYE Check` IS NOT NULL THEN 'Rebound Assessment'
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND t40.DateCreated IS NULL THEN 'Assessment 1'
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND t40.DateCreated IS NOT NULL THEN 'Assessment 2'
# MAGIC   WHEN t41.MainCaseStatusType = 'Initiation' AND w.`Assessment in progress` IS NOT NULL THEN 'Rebound Initiation'
# MAGIC   ELSE t41.MainCaseStatusType
# MAGIC END AS `Case phase`
# MAGIC , CASE
# MAGIC   WHEN t41.MainCaseStatusType IN ('Completed', 'Cancelled') THEN 14
# MAGIC   WHEN t41.MainCaseStatusType = 'Sign-off' THEN 12
# MAGIC   WHEN t40.`Case phase` = 'Client Outreach' THEN 5
# MAGIC   WHEN t40.`Case phase` = 'Rebound Client Outreach' THEN 8
# MAGIC   WHEN t40.`Case phase` = 'Client Outreach Completed' THEN 6
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND w.`4-EYE Check` IS NOT NULL THEN 11
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND t40.DateCreated IS NULL THEN 4
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND t40.DateCreated IS NOT NULL THEN 7
# MAGIC   WHEN t41.MainCaseStatusType = 'Initiation' AND w.`Assessment in progress` IS NOT NULL THEN 2
# MAGIC   ELSE t41.MainCaseStatusSortOrder
# MAGIC END AS `Case phase sort order`
# MAGIC , CASE
# MAGIC   WHEN acr.ReviewTypeName = 'Client Offboarding' AND t41.MainCaseStatusType NOT IN ('Completed', 'Cancelled') THEN 'Offboarding'
# MAGIC   WHEN YEAR(TO_DATE(acr.NextReviewDate, 'dd-MM-yyyy')) > 2023 THEN 'Completed'
# MAGIC   WHEN t41.MainCaseStatusType IN ('Completed', 'Cancelled', 'KYC assessment in progress') AND YEAR(TO_DATE(acr.NextReviewDate, 'dd-MM-yyyy')) < 2024 THEN 'Not yet started'
# MAGIC   WHEN t41.MainCaseStatusType = 'Sign-off' THEN 'Sign-off'
# MAGIC   WHEN t40.`Case phase` IN ('Rebound Client Outreach', 'Client Outreach', 'Client Outreach Completed') THEN t40.`Case phase`
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND w.`4-EYE Check` IS NOT NULL THEN 'Rebound Assessment'
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND t40.DateCreated IS NULL THEN 'Assessment 1'
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND t40.DateCreated IS NOT NULL THEN 'Assessment 2'
# MAGIC   WHEN t41.MainCaseStatusType = 'Initiation' AND w.`Assessment in progress` IS NOT NULL THEN 'Rebound Initiation'
# MAGIC   ELSE t41.MainCaseStatusType
# MAGIC END AS `Case phase 2023`
# MAGIC , CASE
# MAGIC   WHEN acr.ReviewTypeName = 'Client Offboarding' AND t41.MainCaseStatusType NOT IN ('Completed', 'Cancelled') THEN 'Offboarding'
# MAGIC   WHEN YEAR(TO_DATE(acr.NextReviewDate, 'dd-MM-yyyy')) > 2024 THEN 'Completed'
# MAGIC   WHEN t41.MainCaseStatusType IN ('Completed', 'Cancelled', 'KYC assessment in progress') AND YEAR(TO_DATE(acr.NextReviewDate, 'dd-MM-yyyy')) < 2025 THEN 'Not yet started'
# MAGIC   WHEN t41.MainCaseStatusType = 'Sign-off' THEN 'Sign-off'
# MAGIC   WHEN t40.`Case phase` IN ('Rebound Client Outreach', 'Client Outreach', 'Client Outreach Completed') THEN t40.`Case phase`
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND w.`4-EYE Check` IS NOT NULL THEN 'Rebound Assessment'
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND t40.DateCreated IS NULL THEN 'Assessment 1'
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND t40.DateCreated IS NOT NULL THEN 'Assessment 2'
# MAGIC   WHEN t41.MainCaseStatusType = 'Initiation' AND w.`Assessment in progress` IS NOT NULL THEN 'Rebound Initiation'
# MAGIC   ELSE t41.MainCaseStatusType
# MAGIC END AS `Case phase 2024`
# MAGIC , CASE
# MAGIC   WHEN YEAR(TO_DATE(acr.NextReviewDate, 'dd-MM-yyyy')) > 2024 THEN 14
# MAGIC   WHEN t41.MainCaseStatusType IN ('Completed', 'Cancelled', 'KYC assessment in progress') AND YEAR(TO_DATE(acr.NextReviewDate, 'dd-MM-yyyy')) < 2025 THEN 0
# MAGIC   WHEN t41.MainCaseStatusType = 'Sign-off' THEN 12
# MAGIC   WHEN t40.`Case phase` = 'Client Outreach' THEN 5
# MAGIC   WHEN t40.`Case phase` = 'Rebound Client Outreach' THEN 8
# MAGIC   WHEN t40.`Case phase` = 'Client Outreach Completed' THEN 6
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND w.`4-EYE Check` IS NOT NULL THEN 11
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND t40.DateCreated IS NULL THEN 4
# MAGIC   WHEN t41.MainCaseStatusType = 'Assessment' AND t40.DateCreated IS NOT NULL THEN 7
# MAGIC   WHEN t41.MainCaseStatusType = 'Initiation' AND w.`Assessment in progress` IS NOT NULL THEN 2
# MAGIC   ELSE t41.MainCaseStatusSortOrder
# MAGIC END AS `Case phase sort order 2024`
# MAGIC , CASE
# MAGIC   WHEN t40.`Case phase` = 'Client Outreach' THEN t40.`Days in Waiting for information`
# MAGIC   WHEN t40.`Case phase` = 'Rebound Client Outreach' THEN t43.DurationInfoRequestInDay
# MAGIC   WHEN w.`Case phase` = 'Prework' THEN w.DaysInPrework
# MAGIC   WHEN w.`Case phase` = 'Ready for assessment' THEN w.DaysInReadyForAssessment
# MAGIC   WHEN w.`Case phase` = 'Assessment in progress' THEN w.DaysInAssessmentInProgress
# MAGIC   WHEN w.`Case phase` = '4-EYE check' THEN w.DaysIn4EYECheck
# MAGIC   WHEN w.`Case phase` = 'Fulfilment' THEN w.DaysInFulfillment
# MAGIC   WHEN w.`Case phase` = 'Sign-off' THEN w.`DaysInSign-off`
# MAGIC END AS `Days in current case phase`
# MAGIC , prl.`Previous risk level`
# MAGIC , prl.PreviousCaseId
# MAGIC ,prl.PreviousAdverseInfoRiskLevel
# MAGIC ,prl.PreviousGeographicalRiskLevel
# MAGIC ,prl.PreviousEntityTypeRiskLevel
# MAGIC ,prl.PreviousStructureRiskLevel
# MAGIC ,prl.PreviousSectorRiskLevel
# MAGIC ,prl.PreviousProductAndServiceRiskLevel
# MAGIC ,prl.PreviousPEPRiskLevel
# MAGIC ,prl.PreviousTransactionRiskLevel
# MAGIC ,prl.PreviousDistributionRiskLevel
# MAGIC ,prl.PreviousThirdPartyRiskLevel
# MAGIC ,prl.PreviousFatcaClassification
# MAGIC ,prl.PreviousCrsClassification
# MAGIC ,CASE WHEN acr.CaseStatusName NOT IN ('Cancelled', 'Completed', 'Migrated') AND current_date() >= DATE_ADD(w.Prework, 90)
# MAGIC       AND acr.ReviewTypeName = 'Event Driven Review' THEN 'Yes' ELSE 'No' END AS `EDR overdue`
# MAGIC ,CASE 
# MAGIC   WHEN acr.ReviewTypeName = 'Client Offboarding' THEN w.Completed
# MAGIC   ELSE w.`Sign-off` END AS `SignoffDate TC`
# MAGIC ,CASE 
# MAGIC   WHEN acr.ValidatedRiskLevel IN ('High','Unacceptable') THEN DATE_ADD(CAST(acr.NextReviewDate AS DATE), 60) -- Adding 60 days for 2 months
# MAGIC   WHEN acr.ReviewTypeName = 'Event Driven Review' THEN DATE_ADD(w.Prework, 90)
# MAGIC   ELSE acr.NextReviewDate END AS `TC NRD`
# MAGIC ,t47.UserTeam AS `4-Eye User Team`
# MAGIC ,t47.Department AS `4-Eye Department`
# MAGIC ,t48.Department AS `Prework Department`
# MAGIC ,CASE 
# MAGIC   WHEN acr.ReviewTypeName = 'Client Offboarding' THEN COALESCE(t49.UserTeam, t48.UserTeam) 
# MAGIC   WHEN w.`Case phase` = 'Prework' THEN t48.UserTeam
# MAGIC   WHEN w.`Case phase` = 'Ready for assessment' THEN t48.UserTeam
# MAGIC   WHEN w.`Case phase` = 'Assessment in progress' THEN t49.UserTeam
# MAGIC   WHEN w.`Case phase` = '4-EYE check' THEN t49.UserTeam
# MAGIC   WHEN w.`Case phase` = 'Fulfilment' THEN t49.UserTeam
# MAGIC   WHEN w.`Case phase` = 'Sign-off' THEN t49.UserTeam
# MAGIC   WHEN w.`Case phase` = 'Completed' THEN COALESCE(t49.UserTeam, t50.UserTeam)
# MAGIC   WHEN w.`Case phase` = 'Cancelled' THEN t49.UserTeam
# MAGIC END AS `KYC User Team`
# MAGIC ,CASE 
# MAGIC   WHEN acr.ReviewTypeName = 'Client Offboarding' THEN COALESCE(t49.Department, t48.Department) 
# MAGIC   WHEN w.`Case phase` = 'Prework' THEN t48.Department
# MAGIC   WHEN w.`Case phase` = 'Ready for assessment' THEN t48.Department
# MAGIC   WHEN w.`Case phase` = 'Assessment in progress' THEN t49.Department
# MAGIC   WHEN w.`Case phase` = '4-EYE check' THEN t49.Department
# MAGIC   WHEN w.`Case phase` = 'Fulfilment' THEN t49.Department
# MAGIC   WHEN w.`Case phase` = 'Sign-off' THEN t49.Department
# MAGIC   WHEN w.`Case phase` = 'Completed' THEN COALESCE(t49.Department, t50.Department)
# MAGIC   WHEN w.`Case phase` = 'Cancelled' THEN t49.Department
# MAGIC END AS `KYC Department`
# MAGIC From gcob_workitems_status w
# MAGIC Inner Join party_case_client_details acr on w.clientid = acr.clientid and w.SourceClient = acr.SourceClient
# MAGIC Left join gcob_rfi_status t40 on w.clientid = t40.clientid and w.SourceClient = t40.SourceClient
# MAGIC LEFT JOIN CTE_MainCaseStatusType t41 ON w.clientid = t41.clientid and w.SourceClient = t41.SourceClient
# MAGIC LEFT JOIN CTE_ReboundClientOutreach t43 on w.clientid = t43.clientid and w.SourceClient = t43.SourceClient 
# MAGIC LEFT JOIN CTE_PreviousRisk_Case prl on w.clientid = prl.clientid and w.SourceClient = prl.SourceClient 
# MAGIC LEFT JOIN UserTeamRegistry t47 ON w.`4-EYE analyst` = t47.AssignedUser AND w.`4-EYE analyst team date` >= t47.TeamStartDate AND w.`4-EYE analyst team date` <= COALESCE(t47.TeamEndDate, current_date())
# MAGIC LEFT JOIN UserTeamRegistry t48 ON w.`Prework analyst` = t48.AssignedUser AND w.`Prework analyst team date` >= t48.TeamStartDate AND w.`Prework analyst team date` <= COALESCE(t48.TeamEndDate, current_date())
# MAGIC LEFT JOIN UserTeamRegistry t49 ON w.`Assessment analyst` = t49.AssignedUser AND w.`Assessment analyst team date` >= t49.TeamStartDate AND w.`Assessment analyst team date` <= COALESCE(t49.TeamEndDate, current_date())
# MAGIC LEFT JOIN UserTeamRegistry t50 ON w.`Last analyst` = t50.AssignedUser AND w.`Last analyst team date` >= t50.TeamStartDate AND w.`Last analyst team date` <= COALESCE(t50.TeamEndDate, current_date())
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NextSignOffDate AS
# MAGIC /*
# MAGIC With Main AS 
# MAGIC (
# MAGIC select distinct w.ClientId , w.SourceClient,
# MAGIC LEAD(w.`Signoffdate TC`, 1, NULL) OVER (PARTITION BY c.Gcobid,c.ClientType ORDER BY w.`Signoffdate TC`) AS NextSignOffDate,
# MAGIC LEAD(c.`ReviewTypeName`, 1, NULL) OVER (PARTITION BY c.Gcobid,c.ClientType ORDER BY w.`Signoffdate TC`) AS `NextCaseReviewType_withQC`,
# MAGIC LEAD(w.`TC NRD`, 1, NULL) OVER (PARTITION BY c.Gcobid,c.ClientType ORDER BY w.`Signoffdate TC`) AS `Next NRD`,
# MAGIC w.`TC NRD` as TCNRD
# MAGIC from workitems_status w
# MAGIC Inner join party_case_client_details c on w.ClientId = c.ClientId and w.SourceClient = c.SourceClient
# MAGIC )
# MAGIC select 
# MAGIC *
# MAGIC ,CASE WHEN NextCaseReviewType_withQC = 'QC EDR' AND `Next NRD` <> TCNRD THEN `Next NRD` ELSE TCNRD END AS `TC NRD`
# MAGIC from Main
# MAGIC */
# MAGIC
# MAGIC With CaseClientDetails as 
# MAGIC (
# MAGIC     select distinct CaseId,ClientId,SourceClient,GcobId,FullLegalName,clienttype,SourceSystem,ReviewTypeName
# MAGIC     From party_case_client_details
# MAGIC )
# MAGIC
# MAGIC ,Main AS 
# MAGIC (
# MAGIC select distinct w.ClientId ,c.CaseId, w.SourceClient,w.`Signoffdate TC`,
# MAGIC LEAD(c.CaseId, 1, NULL) OVER (PARTITION BY c.Gcobid,c.ClientType ORDER BY w.`Signoffdate TC`,c.CaseId asc, SourceSystem desc, clienttype asc) AS Lead_CaseId,
# MAGIC LEAD(w.`Signoffdate TC`, 1, NULL) OVER (PARTITION BY c.Gcobid,c.ClientType ORDER BY w.`Signoffdate TC`,c.CaseId asc, SourceSystem desc, clienttype asc) AS NextSignOffDate,
# MAGIC LEAD(c.`ReviewTypeName`, 1, NULL) OVER (PARTITION BY c.Gcobid,c.ClientType ORDER BY w.`Signoffdate TC`,c.CaseId asc, SourceSystem desc, clienttype asc) AS `NextCaseReviewType_withQC`,
# MAGIC LEAD(w.`TC NRD`, 1, NULL) OVER (PARTITION BY c.Gcobid,c.ClientType ORDER BY w.`Signoffdate TC`,c.CaseId asc, SourceSystem desc, clienttype asc) AS `Next NRD`,
# MAGIC w.`TC NRD` as TCNRD
# MAGIC from workitems_status w
# MAGIC Inner join CaseClientDetails c on w.ClientId = c.ClientId and w.SourceClient = c.SourceClient
# MAGIC )
# MAGIC select distinct
# MAGIC ClientId , SourceClient
# MAGIC ,case when CaseId = Lead_CaseId then null else NextSignOffDate end as NextSignOffDate
# MAGIC ,case when CaseId = Lead_CaseId then null else `NextCaseReviewType_withQC` end as `NextCaseReviewType_withQC`
# MAGIC ,case when CaseId = Lead_CaseId then null else `Next NRD` end as `Next NRD`
# MAGIC ,TCNRD
# MAGIC ,CASE WHEN NextCaseReviewType_withQC = 'QC EDR' AND `Next NRD` <> TCNRD THEN `Next NRD` ELSE TCNRD END AS `TC NRD`
# MAGIC from Main

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Firebird_Master AS
# MAGIC select distinct
# MAGIC acr.*
# MAGIC --,o.LocalClientOwnerName
# MAGIC --,o.Location as Localclientownerlocation
# MAGIC ,CASE WHEN acr.ClientId = c.LastFullReviewId THEN 'Yes' ELSE 'No' END AS LatestFullReviewversion
# MAGIC ,CASE WHEN acr.CaseStatusName NOT IN ('Cancelled', 'Completed', 'Migrated') THEN 'Yes' Else 'No' END AS Activecase
# MAGIC ,CASE 
# MAGIC   WHEN acr.ReviewTypeName = 'Event Driven Review' AND w.TotalCaseDuration <= 90 THEN 1
# MAGIC   WHEN nso.NextSignOffDate <= nso.`TC NRD` OR (nso.`TC NRD` < CURRENT_DATE() AND nso.NextSignOffDate IS NULL) THEN 1
# MAGIC ELSE 0 END AS CompletedOnTime
# MAGIC ,CASE WHEN acr.ClientLifeCycleName <> 'FormerClient' THEN 1 ELSE 0 END AS DuplicateOffboarding
# MAGIC ,CASE
# MAGIC   WHEN (
# MAGIC     acr.BusinessLineName IN ('Global F&A Banking', 'GWPC - Acquisition Finance', 'LPG', 'GWPC - Export and Project Finance')
# MAGIC     OR SUBSTR(acr.BusinessLineName, 1, 3) = 'GCC'
# MAGIC     ) AND acr.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya')  OR acr.GlobalClientOwnerLocation = 'Rabobank Argentina'
# MAGIC   THEN 'Yes' ELSE 'No'
# MAGIC END AS GCCKYCobligation
# MAGIC ,w.Prework
# MAGIC ,w.`Prework analyst` as Preworkanalyst
# MAGIC ,w.`Prework analyst team date` as Preworkanalystteamdate
# MAGIC ,w.`Ready for assessment` as Readyforassessment
# MAGIC ,w.`Assessment in progress` as Assessmentinprogress
# MAGIC ,w.`Assessment analyst` as Assessmentanalyst
# MAGIC ,w.`Assessment analyst team date` as Assessmentanalystteamdate
# MAGIC ,w.`4-EYE Check` as 4EYECheck
# MAGIC ,w.`4-EYE analyst` as 4EYEanalyst
# MAGIC ,w.`4-EYE analyst team date` as 4EYEanalystteamdate
# MAGIC ,w.`Sign-off` as Signoff
# MAGIC ,w.Fulfillment
# MAGIC ,w.Completed
# MAGIC ,w.Cancelled
# MAGIC ,w.DaysInPrework
# MAGIC ,w.DaysInReadyForAssessment
# MAGIC ,w.DaysInAssessmentInProgress
# MAGIC ,w.DaysIn4EYECheck
# MAGIC ,w.`DaysInSign-off` as DaysInSignoff
# MAGIC ,w.DaysInFulfillment
# MAGIC ,w.TotalCaseDuration
# MAGIC ,w.QCInteractions
# MAGIC ,w.`Last analyst` as Lastanalyst
# MAGIC ,w.`Last analyst team date` as Lastanalystteamdate
# MAGIC ,w.`Prework Alias` as PreworkAlias
# MAGIC ,w.`Assessment Alias` as AssessmentAlias
# MAGIC ,w.`4-EYE Alias` as 4EYEAlias
# MAGIC ,w.`Last Analyst Alias` as LastAnalystAlias
# MAGIC ,w.`Responded date first RFI` as RespondeddatefirstRFI
# MAGIC ,w.`Completed date first RFI` as CompleteddatefirstRFI
# MAGIC ,w.`First RFI created` as FirstRFIcreated
# MAGIC ,w.`Latest RFI completed` as LatestRFIcompleted
# MAGIC ,w.`Case phase` as Casephase
# MAGIC ,w.`Case phase sort order` as Casephasesortorder
# MAGIC ,w.`Case phase 2023` as Casephase2023
# MAGIC ,w.`Case phase 2024` as Casephase2024
# MAGIC ,w.`Case phase sort order 2024` as Casephasesortorder2024
# MAGIC ,w.`Days in current case phase` as Daysincurrentcasephase
# MAGIC ,w.`Previous risk level` as Previousrisklevel
# MAGIC ,w.PreviousCaseId
# MAGIC ,w.PreviousAdverseInfoRiskLevel
# MAGIC ,w.PreviousGeographicalRiskLevel
# MAGIC ,w.PreviousEntityTypeRiskLevel
# MAGIC ,w.PreviousStructureRiskLevel
# MAGIC ,w.PreviousSectorRiskLevel
# MAGIC ,w.PreviousProductAndServiceRiskLevel
# MAGIC ,w.PreviousPEPRiskLevel
# MAGIC ,w.PreviousTransactionRiskLevel
# MAGIC ,w.PreviousDistributionRiskLevel
# MAGIC ,w.PreviousThirdPartyRiskLevel
# MAGIC ,w.PreviousFatcaClassification
# MAGIC ,w.PreviousCrsClassification
# MAGIC ,w.`EDR overdue` as EDRoverdue
# MAGIC ,w.`SignoffDate TC` as SignoffDateTC
# MAGIC ,w.`TC NRD` as TCNRD
# MAGIC ,c.LatestId
# MAGIC ,c.LastFullReviewId
# MAGIC ,c.CompletedId
# MAGIC ,c.LatestCaseId
# MAGIC --,acr.GCDSID
# MAGIC --,acr.ISB
# MAGIC ,pns.ProductName
# MAGIC ,pns.BookingEntityLocation
# MAGIC ,pns.ProductOfferingLocation
# MAGIC ,c.LatestCompletedCaseId
# MAGIC --,acr.ClientType
# MAGIC ,w.`4-Eye User Team` as 4EyeUserTeam
# MAGIC ,w.`4-Eye Department` as 4EyeDepartment
# MAGIC ,w.`Prework Department` as PreworkDepartment
# MAGIC ,w.`KYC User Team` as KYCUserTeam
# MAGIC ,w.`KYC Department` as KYCDepartment
# MAGIC ,c.LastFullReviewCaseId
# MAGIC ,c.PragmaticCaseId
# MAGIC ,case when acr.clienttype='Legal Entity' then concat('LE_', acr.Gcobid)
# MAGIC     when acr.clienttype in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',acr.Gcobid) else 'NA' end as  UniqueGcobId
# MAGIC From party_case_client_details acr
# MAGIC --Left Join party_local_client_Owners o on acr.GcobId = o.GcobId and acr.ClientId = o.LegalEntityClientId
# MAGIC LEFT JOIN workitems_status w on acr.ClientId = w.ClientId and acr.SourceClient = w.SourceClient and acr.CaseId = w.CaseId
# MAGIC LEFT JOIN party_client c on acr.Gcobid = c.gcobid and acr.ClientType = c.ClientType
# MAGIC --acr.ClientId = c.ClientId and acr.SourceClient = c.SourceClient
# MAGIC LEFT JOIN NextSignOffDate nso on acr.ClientId = nso.ClientId and acr.SourceClient = nso.SourceClient
# MAGIC LEFT JOIN party_products_and_services pns on acr.ClientId = pns.ClientId and acr.SourceClient = pns.SourceClient
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Cases AS
# MAGIC select distinct
# MAGIC to_date(BusinessDate, "MM/dd/yyyy") as BusinessDate 
# MAGIC ,SourceSystem
# MAGIC ,ClientId
# MAGIC ,SourceClient
# MAGIC ,case when substr(SourceClient,0,3) = 'LEC' and SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(SourceClient,0,2) = 'NP' and SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(SourceClient,0,6) = 'L2_LEC' and SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(SourceClient,0,5) = 'L2_NP' and SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC ,CaseId
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
# MAGIC --,NextReviewDate
# MAGIC ,to_date(NextReviewDate, "dd-MM-yyyy") as NextReviewDate
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
# MAGIC --,SalesforceClientID_nCino
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
# MAGIC ,case when FIHubIndicator = 1 then 'FI' when FIHubIndicator = 0 then 'Corp' else null end as FIHubIndicator_Derived
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
# MAGIC --,LocalClientOwnerName
# MAGIC --,Localclientownerlocation
# MAGIC ,LatestFullReviewversion
# MAGIC ,Activecase
# MAGIC ,CompletedOnTime
# MAGIC ,DuplicateOffboarding
# MAGIC ,GCCKYCobligation
# MAGIC ,Prework
# MAGIC ,Preworkanalyst
# MAGIC ,Preworkanalystteamdate
# MAGIC ,Readyforassessment
# MAGIC ,Assessmentinprogress
# MAGIC ,Assessmentanalyst
# MAGIC ,Assessmentanalystteamdate
# MAGIC ,4EYECheck
# MAGIC ,4EYEanalyst
# MAGIC ,4EYEanalystteamdate
# MAGIC ,Signoff
# MAGIC ,Fulfillment
# MAGIC ,Completed
# MAGIC ,Cancelled
# MAGIC ,DaysInPrework
# MAGIC ,DaysInReadyForAssessment
# MAGIC ,DaysInAssessmentInProgress
# MAGIC ,DaysIn4EYECheck
# MAGIC ,DaysInSignoff
# MAGIC ,DaysInFulfillment
# MAGIC ,TotalCaseDuration
# MAGIC ,QCInteractions
# MAGIC ,Lastanalyst
# MAGIC ,Lastanalystteamdate
# MAGIC ,PreworkAlias
# MAGIC ,AssessmentAlias
# MAGIC ,4EYEAlias
# MAGIC ,LastAnalystAlias
# MAGIC ,RespondeddatefirstRFI
# MAGIC ,CompleteddatefirstRFI
# MAGIC ,FirstRFIcreated
# MAGIC ,LatestRFIcompleted
# MAGIC ,Casephase
# MAGIC ,Casephasesortorder
# MAGIC ,Casephase2023
# MAGIC ,Casephase2024
# MAGIC ,Casephasesortorder2024
# MAGIC ,Daysincurrentcasephase
# MAGIC ,Previousrisklevel
# MAGIC ,PreviousCaseId
# MAGIC ,EDRoverdue
# MAGIC ,SignoffDateTC
# MAGIC ,TCNRD
# MAGIC ,4EyeUserTeam
# MAGIC ,4EyeDepartment
# MAGIC ,PreworkDepartment
# MAGIC ,KYCUserTeam
# MAGIC ,KYCDepartment
# MAGIC ,ConsultationRequired
# MAGIC ,UniqueGcobId
# MAGIC ,EDL_LoadDate
# MAGIC From Firebird_Master
# MAGIC --where CaseStatusName <> 'Cancelled'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS radar

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS radar.Cases

# COMMAND ----------

spark.sql('select * from Cases').write.mode('overwrite').saveAsTable('radar.Cases')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW LatestClient AS
# MAGIC SELECT distinct
# MAGIC t1.GcobId, 
# MAGIC t1.ClientId, 
# MAGIC COALESCE(CAST(t1.LatestId AS STRING), t1.ClientId) AS LatestClientId
# MAGIC ,t1.ClientType
# MAGIC FROM party_client t1
# MAGIC LEFT ANTI JOIN (
# MAGIC     SELECT DISTINCT a.GcobId,a.ClientType
# MAGIC     FROM (
# MAGIC         SELECT GcobId,ClientType, MIN(CAST(ClientId AS INT)) AS FirstClientID
# MAGIC         FROM party_client
# MAGIC         GROUP BY GcobId,ClientType
# MAGIC         ) a
# MAGIC INNER JOIN party_case_client_details b ON a.FirstClientID = CAST(b.ClientId AS INT) --and c.SourceClient = b.SourceClient
# MAGIC WHERE b.CaseStatusName = 'Cancelled' 
# MAGIC AND b.ClientLifeCycleName = 'Client'
# MAGIC ) c ON t1.GcobId = c.GcobId and t1.ClientType = c.ClientType
# MAGIC WHERE t1.GcobId NOT IN (28095, 5333) 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from portfolioplanning where gcobid like 'np%'
# MAGIC --select distinct KYCGroup from portfolioplanning

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GROUP_portfolioplanning AS
# MAGIC SELECT
# MAGIC   t1.KYCGroup
# MAGIC   ,SUM(CASE WHEN t3.CDDtype = 'Listed Corporate' THEN 1 ELSE 0 END) AS ListedCorporatesInGroup,
# MAGIC   COUNT(*) AS TotalEntitiesInGroup
# MAGIC FROM portfolioplanning t1
# MAGIC LEFT JOIN LatestClient t2 ON t1.GcobId = t2.GcobId
# MAGIC LEFT JOIN party_case_client_details t3 ON t2.ClientId = t3.ClientId
# MAGIC WHERE t1.KYCGROUP NOT IN ('', '--')
# MAGIC GROUP BY t1.KYCGroup
# MAGIC     --UserTeamRegistry
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ONBOARDING AS
# MAGIC WITH onboarding_clientid_cte AS (
# MAGIC SELECT distinct
# MAGIC GcobId
# MAGIC ,MAX(ClientId) AS max_clientid
# MAGIC ,SourceSystem
# MAGIC FROM party_case_client_details
# MAGIC WHERE CaseStatusName = 'Completed'
# MAGIC AND ReviewTypeName = 'Initial On-Boarding'
# MAGIC and ClientType= 'Legal Entity'
# MAGIC GROUP BY GcobId,SourceSystem
# MAGIC )
# MAGIC ,LE_onboarding as (
# MAGIC SELECT
# MAGIC t1.GcobId
# MAGIC ,t1.SourceSystem
# MAGIC ,CAST(t2.ClientApprovalDate AS DATE) AS OnboardingDate
# MAGIC FROM onboarding_clientid_cte t1
# MAGIC JOIN party_case_client_details t2 on t1.max_clientid = t2.ClientId and t1.SourceSystem = t2.SourceSystem
# MAGIC WHERE t2.ClientType= 'Legal Entity' and t2.CaseStatusName = 'Completed' and t2.ReviewTypeName = 'Initial On-Boarding'
# MAGIC )
# MAGIC ,NP_onboarding_clientid_cte as
# MAGIC (
# MAGIC SELECT
# MAGIC MIN(ClientId) AS min_clientid
# MAGIC ,GcobId
# MAGIC ,SourceSystem
# MAGIC FROM Firebird_Master
# MAGIC WHERE ReviewTypeName in ('Initial On-Boarding','Periodic Review')
# MAGIC and CaseStatusName = 'Completed'
# MAGIC and ClientType <> 'Legal Entity'
# MAGIC GROUP BY GcobId,SourceSystem
# MAGIC )
# MAGIC ,NP_onboarding as
# MAGIC (
# MAGIC SELECT
# MAGIC MAX(t2.Signoff) AS `Sign-off date`
# MAGIC ,t1.GcobId
# MAGIC ,t1.SourceSystem
# MAGIC FROM NP_onboarding_clientid_cte t1
# MAGIC JOIN Firebird_Master t2 on t1.min_clientid = t2.ClientId and t1.SourceSystem = t2.SourceSystem
# MAGIC WHERE t2.ClientType <> 'Legal Entity' and t2.ReviewTypeName in ('Initial On-Boarding','Periodic Review') and t2.CaseStatusName = 'Completed'
# MAGIC GROUP BY t1.GcobId,t1.SourceSystem
# MAGIC )
# MAGIC select distinct
# MAGIC m.GcobId
# MAGIC ,m.ClientType
# MAGIC ,m.SourceSystem
# MAGIC ,case when m.ClientType = 'Legal Entity' then le.OnboardingDate else np.`Sign-off date` end as `Sign-off date`
# MAGIC From Unique_Client_Gcob m
# MAGIC left join LE_onboarding le on m.gcobid = le.gcobid and m.SourceSystem = le.SourceSystem and m.ClientType= 'Legal Entity'
# MAGIC left join NP_onboarding np on m.gcobid = np.gcobid and m.SourceSystem = np.SourceSystem and m.ClientType <> 'Legal Entity'

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC WITH onboarding_clientid_cte AS (
# MAGIC   SELECT
# MAGIC     GcobId
# MAGIC     , MAX(ClientId) AS max_clientid
# MAGIC   FROM party_case_client_details
# MAGIC   WHERE CaseStatusName = 'Completed'
# MAGIC     AND ReviewTypeName = 'Periodic Review'
# MAGIC     and ClientType= 'Legal Entity'
# MAGIC   GROUP BY GcobId
# MAGIC )
# MAGIC SELECT
# MAGIC   t1.GcobId
# MAGIC   , CAST(t2.ClientApprovalDate AS DATE) AS OnboardingDate
# MAGIC FROM onboarding_clientid_cte t1
# MAGIC JOIN party_case_client_details t2 on t1.max_clientid = t2.ClientId
# MAGIC */
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GlobalFiles AS
# MAGIC SELECT 
# MAGIC   t2.GcobId
# MAGIC   ,1 AS `Global Files`
# MAGIC FROM party_case_client_details t1
# MAGIC INNER JOIN LatestClient t2 ON t1.ClientId = t2.ClientId
# MAGIC LEFT JOIN party_products_and_services t4 on t1.ClientId = t4.ClientId
# MAGIC     WHERE (
# MAGIC         t1.GlobalClientOwnerLocation <> t4.BookingEntityLocation
# MAGIC         OR t1.GlobalClientOwnerLocation <> t4.ProductOfferingLocation
# MAGIC     )
# MAGIC     AND t4.ProductLifecyclestatus = 'Active'
# MAGIC     GROUP BY t2.GcobId
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Client_TradeName AS
# MAGIC SELECT DISTINCT 
# MAGIC         Id
# MAGIC         ,concat_ws(',', sort_array(collect_list(struct(ClientTradeName))).ClientTradeName) as TradeName
# MAGIC     FROM
# MAGIC         party_trade_name AS ci
# MAGIC         where ResultType = 'LegalEntity'
# MAGIC 	    group by Id

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE OR REPLACE TEMPORARY VIEW Master_CompCase AS
# MAGIC --select * from Firebird_Master

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Validate_CompCase AS
# MAGIC select distinct lc.gcobid,c.Caseid,lc.LatestCompletedCaseId,c.ValidatedRiskLevel,c.ClientType
# MAGIC from Firebird_Master lc
# MAGIC join Firebird_Master c on c.Caseid=lc.LatestCompletedCaseId
# MAGIC --where lc.gcobid = 11531
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Clients AS
# MAGIC select distinct
# MAGIC c.ClientId
# MAGIC ,c.SourceClient
# MAGIC ,case when substr(c.SourceClient,0,3) = 'LEC' and c.SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(c.SourceClient,0,2) = 'NP' and c.SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(c.SourceClient,0,6) = 'L2_LEC' and c.SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(c.SourceClient,0,5) = 'L2_NP' and c.SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC ,c.SourceSystem
# MAGIC ,c.CaseStatusName
# MAGIC ,c.GcobId
# MAGIC ,c.LatestCompletedCaseId as ApprovedCaseId
# MAGIC ,c.CaseId as CHECK
# MAGIC ,c.LatestCaseId
# MAGIC ,c.CompletedId
# MAGIC ,c.LastFullReviewId
# MAGIC ,c.ClientType
# MAGIC
# MAGIC -- ,c.GcobId as UniqueGcobId
# MAGIC , CASE 
# MAGIC     WHEN SUBSTR(c.SourceClient,0,3) = 'LEC' AND c.SourceSystem = 'GCOB' THEN c.GcobId
# MAGIC     WHEN SUBSTR(c.SourceClient,0,2) = 'NP' AND c.SourceSystem = 'GCOB' THEN CONCAT('NP_', c.GcobId)
# MAGIC     WHEN SUBSTR(c.SourceClient,0,6) = 'L2_LEC' AND c.SourceSystem = 'Legacy2' THEN c.GcobId
# MAGIC     WHEN SUBSTR(c.SourceClient,0,5) = 'L2_NP' AND c.SourceSystem = 'Legacy2' THEN CONCAT('NP_', c.GcobId)
# MAGIC   END AS UniqueGcobId
# MAGIC
# MAGIC ,c.FullLegalName
# MAGIC ,c.GlobalClientOwner
# MAGIC ,c.GlobalClientOwnerLocation
# MAGIC ,c.RiskModelName
# MAGIC ,c.BusinessLineName
# MAGIC ,c.CddType
# MAGIC ,CASE WHEN c.CddType = 'Listed Corporate' THEN 1 ELSE 0 END AS ListedCorporation
# MAGIC ,c.FIHubIndicator
# MAGIC ,case when FIHubIndicator = 1 then 'FI' when FIHubIndicator = 0 then 'Corp' else null end as FIHubIndicator_Derived
# MAGIC ,c.FatcaDateOfIssue
# MAGIC ,c.CrsFormSignedDate
# MAGIC , CASE
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya') THEN 'E&A'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') THEN 'Asia'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') THEN 'North America'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Chile', 'Rabobank Brazil', 'Rabobank ') THEN 'South America'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') THEN 'AUNZ'
# MAGIC END AS GlobalReportingRegion
# MAGIC ,c.CountryOfOperation
# MAGIC ,c.ClientLifeCycleName
# MAGIC
# MAGIC
# MAGIC -- ,c.GCDSID
# MAGIC , t1.gcid AS GCDSID 
# MAGIC
# MAGIC
# MAGIC ,c.ISB
# MAGIC ,to_date(c.NextReviewDate, "dd-MM-yyyy") as NextReviewDate
# MAGIC ,c.Signoff
# MAGIC ,case when v.ValidatedRiskLevel is null then 'Unknown' else v.ValidatedRiskLevel end as ValidatedRiskLevel
# MAGIC ,CASE 
# MAGIC   WHEN c.NextReviewDate < CAST(current_date() AS DATE) THEN 'Yes'
# MAGIC   WHEN c.NextReviewDate < add_months(current_date(), 1) THEN 'Next month'
# MAGIC   ELSE 'No'
# MAGIC END AS Overdue
# MAGIC ,CASE
# MAGIC   WHEN c.NextReviewDate < CAST(current_date() AS DATE) THEN 2
# MAGIC   WHEN c.NextReviewDate < add_months(current_date(), 1) THEN 1
# MAGIC   ELSE 0
# MAGIC END AS OverdueCategoryNr
# MAGIC ,CASE
# MAGIC   WHEN c.ValidatedRiskLevel = 'High' AND add_months(c.NextReviewDate, 2) < CAST(current_date() AS DATE) THEN 'Yes'
# MAGIC   ELSE 'No'
# MAGIC END AS Regulatoryoverdue
# MAGIC ,CASE
# MAGIC   WHEN (c.ValidatedRiskLevel = 'High') AND (add_months(c.NextReviewDate, 2) < CAST(current_date() AS DATE) ) THEN 'Yes'
# MAGIC   WHEN (c.ValidatedRiskLevel <> 'High') AND (c.NextReviewDate < CAST(current_date() AS DATE)) THEN 'Yes'
# MAGIC   ELSE 'No'
# MAGIC END AS ExternalReportingOverdue
# MAGIC ,CASE
# MAGIC   WHEN c.ReviewTypeName = 'Event Driven Review' THEN to_date(add_months(c.Prework, 3), 'yyyy-MM-dd')
# MAGIC   ELSE NULL
# MAGIC END AS EDRDuedate
# MAGIC ,c.Activecase
# MAGIC ,CASE
# MAGIC   WHEN KYC.ReviewLocation LIKE '%FI%' THEN 'FI'
# MAGIC   WHEN KYC.ReviewLocation LIKE '%Corp%' THEN 'Corp'
# MAGIC   WHEN c.BusinessLineName IN ('GWPC - Financial Institutions Group', 'GWPC - Markets', 'Capital Markets', 'MARKETS', 'Treasury Rabobank Group', 'UNKNOWN') THEN 'FI'
# MAGIC   WHEN c.CddType = 'Supervised FI' THEN 'FI'
# MAGIC   ELSE 'Corp'
# MAGIC END AS ClientFIorCorp
# MAGIC ,CASE
# MAGIC   WHEN c.CaseStatusName NOT IN ('Completed', 'Cancelled', 'Migrated') THEN 'WIP'
# MAGIC   WHEN KYC.ClientCaseInitiationStart < add_months(current_date(), -6) THEN 'Planned'
# MAGIC   WHEN COALESCE(CAST(c.NextReviewDate AS DATE), CAST(c.NextReviewDate AS DATE)) >= add_months(current_date(), 6) THEN 'Completed'
# MAGIC   ELSE 'May need planning'
# MAGIC END AS PlannedWipCompleted
# MAGIC ,CASE
# MAGIC   WHEN KYC.SectorTeam = 'EF FI' AND c.GlobalClientOwnerLocation IN ('Rabobank Netherlands') THEN 'GWPC NL'
# MAGIC   WHEN KYC.SectorTeam = 'PF FI' AND c.GlobalClientOwnerLocation IN ('Rabobank Netherlands') THEN 'GWPC NL'
# MAGIC   WHEN COALESCE(c.GCCKYCobligation, c.GCCKYCobligation) = 'Yes' THEN 'GCC NL'
# MAGIC   WHEN KYC.SectorTeam = 'EF Corp' AND c.GlobalClientOwnerLocation IN ('Rabobank Netherlands') THEN 'GCC NL'
# MAGIC   WHEN KYC.SectorTeam = 'PF Corp' AND c.GlobalClientOwnerLocation IN ('Rabobank Netherlands') THEN 'GCC NL'
# MAGIC   WHEN KYC.SectorTeam = 'Sponsor Coverage' AND c.GlobalClientOwnerLocation IN ('Rabobank Netherlands') THEN 'GCC NL'
# MAGIC   WHEN KYC.SectorTeam = 'Sponsor Coverage' AND c.GlobalClientOwnerLocation IN ('Rabobank London') THEN 'Rabobank London'
# MAGIC   WHEN KYC.SectorTeam = 'TCF Corp' AND c.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya') THEN 'GCC NL'
# MAGIC   WHEN KYC.SectorTeam = 'TCF FI' AND c.GlobalClientOwnerLocation IN ('Rabobank Netherlands') THEN 'GWPC NL'
# MAGIC   WHEN c.GlobalClientOwner IN ('Oord van, JA (Marco)', 'Erkamp, M (Maarten)') THEN 'GCC NL'
# MAGIC   -- WHEN c.BusinessLineName = 'GWPC - Trade & Commodity Finance' AND c.GlobalClientOwnerLocation IN ('Rabobank London', 'Rabobank Netherlands', 'Rabobank Kenya') THEN 'GCC NL'
# MAGIC   WHEN c.GlobalClientOwnerLocation = 'Rabobank Netherlands' AND COALESCE(c.GCCKYCobligation, c.GCCKYCobligation) = 'No' THEN 'GWPC NL'
# MAGIC   WHEN c.GlobalClientOwnerLocation = 'Rabobank Kenya' AND COALESCE(c.GCCKYCobligation, c.GCCKYCobligation) = 'No' THEN 'GWPC NL'
# MAGIC   WHEN c.GlobalClientOwnerLocation = 'Rabobank Frankfurt' AND KYC.SectorTeam = 'International Desk' THEN 'Frankfurt int. desk'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND c.BusinessLineName IN ('Capital Markets', 'GWPC - Financial Institutions Group', 'GWPC - Markets', 'MARKETS', 'Treasury Rabobank Group') THEN 'Asia FI'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND c.CddType = 'Supervised FI' THEN 'Asia FI'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') THEN 'Asia Corp'
# MAGIC   ELSE c.GlobalClientOwnerLocation
# MAGIC END AS GlobalKYCPortfolio
# MAGIC ,CASE
# MAGIC   WHEN KYC.SectorTeam IN ('Acorn') THEN 'Acorn'
# MAGIC   WHEN KYC.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Rabo Frontier Ventures', 'RCI', 'Sector Banking') THEN 'Advisory and Investments'
# MAGIC   WHEN KYC.SectorTeam IN ('AF', 'ARG', 'CNS', 'Core lending', 'EF Corp', 'EF FI', 'ETC', 'FA', 'HTD', 'REF', 'TRST') THEN 'Core lending'          
# MAGIC   WHEN KYC.SectorTeam IN ('Int. Desk') THEN 'International Services'
# MAGIC   WHEN KYC.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'Markets'       
# MAGIC   WHEN KYC.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP') THEN 'Structured Lending' 
# MAGIC   ELSE 'Unknown'
# MAGIC END AS BusinessCluster
# MAGIC ,CASE
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Netherlands') AND KYC.SectorTeam IN ('Acorn') THEN 'Acorn'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank London') AND KYC.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Sector Banking') THEN 'London Advisory & Investments'  
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank London') AND KYC.SectorTeam IN ('Core Lending') THEN 'London Core Lending' 
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank London') AND KYC.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP', 'VCF') THEN 'London Structured Lending' 
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank London') AND KYC.SectorTeam IN ('Int. Desk') THEN 'London International Services' 
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank London') AND KYC.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'London Markets' 
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND KYC.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Sector Banking') THEN 'North America Advisory & Investments'  
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND KYC.SectorTeam IN ('Core Lending') THEN 'North America Core Lending'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND KYC.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP', 'VCF') THEN 'North America Structured Lending' 
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND KYC.SectorTeam IN ('Int. Desk') THEN 'North America International Services' 
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)') AND KYC.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'North America Markets'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND KYC.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Sector Banking') THEN 'AsiaAdvisory & Investments'  
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND KYC.SectorTeam IN ('Core Lending') THEN 'Asia Core Lending'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND KYC.SectorTeam IN ('Int. Desk') THEN 'Asia International Services'         
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND KYC.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'Asia Markets'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Hong Kong', 'Rabobank China', 'Rabobank Singapore', 'Rabobank India') AND KYC.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP', 'VCF') THEN 'Asia Structured Lending'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Antwerp') AND KYC.SectorTeam IN ('Core Lending') THEN 'Antwerp Core Lending'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Dublin') AND KYC.SectorTeam IN ('Core Lending') THEN 'Dublin Core Lending'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Frankfurt') AND KYC.SectorTeam IN ('Core Lending') THEN 'Frankfurt Core Lending'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Frankfurt') AND KYC.SectorTeam IN ('Int. Desk') THEN 'Frankfurt International Services'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Madrid') AND KYC.SectorTeam IN ('Core Lending') THEN 'Madrid Core Lending'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Milan') AND KYC.SectorTeam IN ('Core Lending') THEN 'Milan Core Lending'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Paris') AND KYC.SectorTeam IN ('Core Lending') THEN 'Paris Core Lending'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya') AND KYC.SectorTeam IN ('TCF Corp', 'PF Corp', 'ABF', 'PF FI', 'TCF FI', 'SIP', 'VCF', 'PF CORP') THEN 'NL Structured Lending'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Netherlands', 'Rabobank Kenya', 'Rabobank Argentina') AND  KYC.SectorTeam IN ('FA', 'ETC', 'HTD', 'REF', 'CNS', 'AF', 'TRST', 'EF Corp', 'ARG', 'EF FI', 'TM', 'Screening')  THEN 'NL Core Lending'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Netherlands') AND KYC.SectorTeam IN ('M&A') THEN 'NL M&A + ECM'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Netherlands') AND KYC.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Rabo Frontier Ventures', 'RCI', 'Sector Banking') THEN 'NL Advisory & Investments'    
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Netherlands') AND KYC.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'NL Markets'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND KYC.SectorTeam IN ('Sponsor Coverage', 'FAS', 'M&A', 'Sector Banking') THEN 'RANZ Advisory & Investments'  
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND KYC.SectorTeam IN ('Core Lending') THEN 'RANZ Core Lending' 
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND KYC.SectorTeam IN ('ABF', 'PF Corp', 'PF FI', 'TCF Corp', 'TCF FI', 'SIP', 'VCF') THEN 'RANZ Structured Lending' 
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND KYC.SectorTeam IN ('Int. Desk') THEN 'RANZ International Services' 
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank New Zealand', 'Rabobank Australia') AND KYC.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries', 'Treasury') THEN 'RANZ Markets'
# MAGIC END AS GlobalKYCPortfolioNew
# MAGIC ,CASE
# MAGIC   WHEN KYC.SectorTeam IN ('Acorn') THEN 'Acorn'
# MAGIC   WHEN KYC.SectorTeam IN ('Sponsor Coverage', 'FAS', 'Sector Banking') THEN 'CFO'
# MAGIC   WHEN KYC.SectorTeam IN ('AF', 'ARG', 'CNS', 'Core lending', 'EF Corp', 'EF FI', 'ETC', 'FA', 'HTD', 'REF', 'TRST') THEN 'Core lending'
# MAGIC   WHEN KYC.SectorTeam IN ('Int. Desk') THEN 'International Services'
# MAGIC   WHEN KYC.SectorTeam IN ('M&A') THEN 'M&A + ECM'
# MAGIC   WHEN KYC.SectorTeam IN ('Agency', 'Correspondent Banking', 'FIG', 'FIG - Orphan Desk', 'Markets', 'Markets/FIG', 'PSP Coverage', 'Subsidiaries') THEN 'Markets/FIG'
# MAGIC   WHEN KYC.SectorTeam IN ('Treasury') THEN 'Treasury'
# MAGIC   WHEN KYC.SectorTeam IN ('Rabo Frontier Ventures', 'RCI') THEN 'Rabo Corp Investment'
# MAGIC   WHEN KYC.SectorTeam IN ('ABF', 'VCF') THEN 'VCF'
# MAGIC   WHEN KYC.SectorTeam IN ('TCF Corp', 'TCF FI') THEN 'TCF'
# MAGIC   WHEN KYC.SectorTeam IN ('PF Corp', 'PF FI') THEN 'PF'
# MAGIC END AS GlobalBusinessLine
# MAGIC /*
# MAGIC ,CASE
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Argentina', 'Rabobank Milan', 'Rabobank Paris', 'Rabobank Madrid', 'Rabobank Dublin', 'Rabobank Antwerp', 'Rabobank Turkey', 'Rabobank Frankfurt') AND (KYC.KYCGroup = '--' OR KYC.KYCGroup = '-') THEN c.FullLegalName
# MAGIC   -- WHEN KYC.KYCGroup = '--' AND ONB.ParentName <> '' THEN ONB.ParentName -- from hds.onb_pipeline
# MAGIC   ELSE KYC.KYCGroup
# MAGIC END AS KYCGroup
# MAGIC */
# MAGIC ,CASE
# MAGIC   WHEN c.GlobalClientOwnerLocation in ('Rabobank - RANZ Country Banking and ROS','Rabobank - USA Rabo AgriFinance','Rabobank Australia','Rabobank New Zealand') and c.sourceClient like 'NP%' then 'NP'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Argentina', 'Rabobank Milan', 'Rabobank Paris', 'Rabobank Madrid', 'Rabobank Dublin', 'Rabobank Antwerp', 'Rabobank Turkey', 'Rabobank Frankfurt') AND (KYC.KYCGroup = '--' OR KYC.KYCGroup = '-') THEN c.FullLegalName
# MAGIC   ELSE KYC.KYCGroup
# MAGIC END AS KYCGroup
# MAGIC ,t12.ListedCorporatesInGroup
# MAGIC ,t12.TotalEntitiesInGroup
# MAGIC ,CASE
# MAGIC   WHEN KYC.SectorTeam != '--' THEN KYC.SectorTeam
# MAGIC   WHEN c.GlobalClientOwnerLocation = 'Rabobank Argentina' THEN 'ARG'
# MAGIC   WHEN c.GlobalClientOwnerLocation IN ('Rabobank Milan', 'Rabobank Paris', 'Rabobank Madrid', 'Rabobank Dublin', 'Rabobank Antwerp', 'Rabobank Turkey') THEN 'Core Lending'
# MAGIC   WHEN c.GlobalClientOwnerLocation = 'Rabobank Frankfurt' AND c.GlobalClientOwner IN ('Meurichy de, K (Koen)') THEN 'Int. Desk'
# MAGIC   WHEN c.GlobalClientOwnerLocation = 'Rabobank Frankfurt' THEN 'Core Lending'
# MAGIC   WHEN c.GlobalClientOwner IN ('Willmott, A (Adam)', 'Oord van, JA (Marco)', 'Erkamp, M (Maarten)') THEN 'Sponsor Coverage'
# MAGIC   -- WHEN KYC.SectorTeam = '--' AND ONB.SectorTeam <> '' THEN ONB.SectorTeam -- from hds.onb_pipeline
# MAGIC END AS SectorTeam
# MAGIC ,KYC.ClientCaseInitiationStart
# MAGIC ,KYC.CDDExecution
# MAGIC ,CASE
# MAGIC   WHEN c.GlobalClientOwner IN ('Willmott, A (Adam)', 'Oord van, JA (Marco)', 'Erkamp, M (Maarten)') THEN 'Corp CDD Hub'
# MAGIC   -- WHEN KYC.ReviewLocation = '' THEN COALESCE(ONB.Location, '') -- from hds.onb_pipeline
# MAGIC   ELSE KYC.ReviewLocation
# MAGIC END AS ReviewLocation
# MAGIC ,KYC.Reason
# MAGIC ,KYC.Explanation
# MAGIC ,to_date(t13.`Sign-off date`, "dd-MM-yyyy") as OnboardingDate
# MAGIC ,COALESCE(KYC.CategoryNr, t14.`Global Files`) AS CategoryNr
# MAGIC ,COALESCE(t14.`Global Files`, 0) AS GlobalFiles
# MAGIC ,KYC.`London Sector Team` AS LondonSectorTeam
# MAGIC ,tn.TradeName
# MAGIC /*
# MAGIC ,Case when c.GeographicalRiskLevel = 'Incomplete' or c.GeographicalRiskLevel is null
# MAGIC    OR c.EntityTypeRiskLevel = 'Incomplete' or c.EntityTypeRiskLevel is null
# MAGIC    OR c.StructureRiskLevel = 'Incomplete' or c.StructureRiskLevel is null
# MAGIC    OR c.SectorRiskLevel = 'Incomplete' or c.SectorRiskLevel is null
# MAGIC    OR c.ProductAndServiceRiskLevel = 'Incomplete' or c.ProductAndServiceRiskLevel is null
# MAGIC    OR c.PEPRiskLevel = 'Incomplete' or c.PEPRiskLevel is null
# MAGIC    OR c.TransactionRiskLevel = 'Incomplete' or c.TransactionRiskLevel is null
# MAGIC    OR c.DistributionRiskLevel = 'Incomplete' or c.DistributionRiskLevel is null
# MAGIC    OR c.ThirdPartyRiskLevel = 'Incomplete' or c.ThirdPartyRiskLevel is null
# MAGIC    OR c.AdverseInfoRiskLevel = 'Incomplete' or c.AdverseInfoRiskLevel is null
# MAGIC   Then 0
# MAGIC   When c.LatestCompletedCaseId is null then 0 else 1
# MAGIC End as PartyHasAllData
# MAGIC */
# MAGIC ,CASE 
# MAGIC   WHEN substr(c.SourceClient,0,2) = 'NP' and c.SourceSystem = 'GCOB' THEN
# MAGIC   --c.SourceSystem = 'GCOB_NP-NPPC' 
# MAGIC     CASE 
# MAGIC       WHEN c.GeographicalRiskLevel = 'Incomplete' OR c.GeographicalRiskLevel IS NULL
# MAGIC         OR c.SectorRiskLevel = 'Incomplete' OR c.SectorRiskLevel IS NULL
# MAGIC         OR c.ProductAndServiceRiskLevel = 'Incomplete' OR c.ProductAndServiceRiskLevel IS NULL
# MAGIC         OR c.PEPRiskLevel = 'Incomplete' OR c.PEPRiskLevel IS NULL
# MAGIC         OR c.TransactionRiskLevel = 'Incomplete' OR c.TransactionRiskLevel IS NULL
# MAGIC         OR c.DistributionRiskLevel = 'Incomplete' OR c.DistributionRiskLevel IS NULL
# MAGIC         OR c.ThirdPartyRiskLevel = 'Incomplete' OR c.ThirdPartyRiskLevel IS NULL
# MAGIC         OR c.AdverseInfoRiskLevel = 'Incomplete' OR c.AdverseInfoRiskLevel IS NULL
# MAGIC         THEN 0 
# MAGIC       WHEN c.LatestCompletedCaseId IS NULL THEN 0 
# MAGIC       ELSE 1
# MAGIC     END
# MAGIC   ELSE 
# MAGIC     CASE 
# MAGIC       WHEN c.GeographicalRiskLevel = 'Incomplete' OR c.GeographicalRiskLevel IS NULL
# MAGIC         OR c.EntityTypeRiskLevel = 'Incomplete' OR c.EntityTypeRiskLevel IS NULL
# MAGIC         OR c.StructureRiskLevel = 'Incomplete' OR c.StructureRiskLevel IS NULL
# MAGIC         OR c.SectorRiskLevel = 'Incomplete' OR c.SectorRiskLevel IS NULL
# MAGIC         OR c.ProductAndServiceRiskLevel = 'Incomplete' OR c.ProductAndServiceRiskLevel IS NULL
# MAGIC         OR c.PEPRiskLevel = 'Incomplete' OR c.PEPRiskLevel IS NULL
# MAGIC         OR c.TransactionRiskLevel = 'Incomplete' OR c.TransactionRiskLevel IS NULL
# MAGIC         OR c.DistributionRiskLevel = 'Incomplete' OR c.DistributionRiskLevel IS NULL
# MAGIC         OR c.ThirdPartyRiskLevel = 'Incomplete' OR c.ThirdPartyRiskLevel IS NULL
# MAGIC         OR c.AdverseInfoRiskLevel = 'Incomplete' OR c.AdverseInfoRiskLevel IS NULL
# MAGIC         THEN 0 
# MAGIC       WHEN c.LatestCompletedCaseId IS NULL THEN 0 
# MAGIC       ELSE 1 
# MAGIC     END
# MAGIC END AS PartyHasAllData
# MAGIC --,LAG(c.CaseId, 1) OVER (PARTITION BY c.GcobId ORDER BY SourceSystem desc, COALESCE(c.Signoff, c.NextReviewDate, c.ClientOwnerSignOffDate) asc, (CASE WHEN Sourcesystem = 'Legacy2' THEN 0 ELSE CAST(CaseId AS INT) END) asc) AS PreviousCaseId
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
# MAGIC ,c.Previousrisklevel
# MAGIC ,c.PreviousCaseId
# MAGIC ,c.LastFullReviewCaseId
# MAGIC ,c.LatestCompletedCaseId
# MAGIC ,c.LatestId
# MAGIC ,c.PragmaticCaseId
# MAGIC ,c.PreviousAdverseInfoRiskLevel
# MAGIC ,c.PreviousGeographicalRiskLevel
# MAGIC ,c.PreviousEntityTypeRiskLevel
# MAGIC ,c.PreviousStructureRiskLevel
# MAGIC ,c.PreviousSectorRiskLevel
# MAGIC ,c.PreviousProductAndServiceRiskLevel
# MAGIC ,c.PreviousPEPRiskLevel
# MAGIC ,c.PreviousTransactionRiskLevel
# MAGIC ,c.PreviousDistributionRiskLevel
# MAGIC ,c.PreviousThirdPartyRiskLevel
# MAGIC ,CASE WHEN (
# MAGIC   ( c.GeographicalRiskLevel = c.PreviousGeographicalRiskLevel) AND
# MAGIC   ( c.EntityTypeRiskLevel  = c.PreviousEntityTypeRiskLevel) AND
# MAGIC   ( c.SectorRiskLevel  = c.PreviousSectorRiskLevel) AND
# MAGIC   ( c.ProductAndServiceRiskLevel = c.PreviousProductAndServiceRiskLevel ) AND
# MAGIC   ( c.StructureRiskLevel = c.PreviousStructureRiskLevel ) AND
# MAGIC   ( c.TransactionRiskLevel = c.PreviousTransactionRiskLevel ) AND
# MAGIC   ( c.DistributionRiskLevel = c.PreviousDistributionRiskLevel ) AND
# MAGIC   ( c.ThirdPartyRiskLevel  = c.PreviousThirdPartyRiskLevel ) AND
# MAGIC   ( c.AdverseInfoRiskLevel  = c.PreviousAdverseInfoRiskLevel ) AND
# MAGIC   ( c.PEPRiskLevel = c.PreviousPEPRiskLevel ) AND
# MAGIC   ( c.ValidatedRiskLevel  = c.Previousrisklevel )) THEN 0
# MAGIC ELSE 1 END AS CaseHasChangedRisk
# MAGIC ,c.PreviousFatcaClassification
# MAGIC ,c.PreviousCrsClassification
# MAGIC ,Case when c.GlobalClientOwnerLocation IN ('Rabobank HongKong','Rabobank Hong Kong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore') then 'Asia Lead' else 'Asia Involved' end AS AsiaLeadOrInvolved
# MAGIC ,Case when c.GlobalClientOwnerLocation IN ('Rabobank Antwerp','Rabobank Argentina','Rabobank Dublin','Rabobank Frankfurt','Rabobank Kenya','Rabobank London','Rabobank Madrid','Rabobank Milan','Rabobank Netherlands','Rabobank Paris') then 'E&A Lead' else 'E&A Involved' end AS EAndALeadOrInvolved
# MAGIC ,Case when c.GlobalClientOwnerLocation IN ('Rabobank - RANZ Country Banking and ROS','Rabobank Australia', 'Rabobank New Zealand') then 'AUNZ Lead' else 'AUNZ Involved' end AS AUNZLeadOrInvolved
# MAGIC ,Case when c.GlobalClientOwnerLocation IN ('Rabo Securities USA, Inc. (RSEC)','Rabobank - USA Rabo AgriFinance','Rabobank New York','Rabobank Canada (RCBR)','Rabobank Canada(Rural)') then 'North America Lead' else 'North America Involved' end AS NorthAmericaLeadOrInvolved
# MAGIC ,Case when c.GlobalClientOwnerLocation IN ('Rabobank Brazil','Rabobank Chile') then 'South America Lead' else 'South America Involved' end AS SouthAmericaLeadOrInvolved
# MAGIC ,c.ClientApprovalDate
# MAGIC ,c.EDL_LoadDate
# MAGIC
# MAGIC From Unique_Client_Gcob ucg
# MAGIC inner join Firebird_Master c on ucg.gcobid = c.gcobid and ucg.caseid = c.caseid and ucg.ClientType = c.ClientType
# MAGIC --LEFT JOIN KYCMasterListRegistry KYC ON c.GcobId = KYC.GcobId
# MAGIC LEFT JOIN portfolioplanning KYC ON ucg.GcobId = KYC.GcobId
# MAGIC LEFT JOIN GROUP_portfolioplanning t12 ON KYC.KYCGroup = t12.KYCGroup
# MAGIC LEFT JOIN ONBOARDING t13 ON ucg.GcobId = t13.GcobId and ucg.ClientType = t13.ClientType and ucg.SourceSystem = t13.SourceSystem --and ucg.caseid = t13.caseid 
# MAGIC LEFT JOIN GlobalFiles t14 ON c.GcobId = t14.GcobId
# MAGIC LEFT JOIN Client_TradeName tn on c.ClientId = tn.Id and c.ClientType = 'Legal Entity'
# MAGIC left join Validate_CompCase v on v.gcobid = c.gcobid and c.LatestCompletedCaseId = v.LatestCompletedCaseId and c.ClientType = v.ClientType
# MAGIC --LEFT JOIN ProdusctAndService pns on ucg.gcobid = pns.gcobid and ucg.caseid = pns.caseid and ucg.ClientType = pns.ClientType
# MAGIC --from LatestClient lc
# MAGIC --LEFT JOIN Firebird_Master c on lc.LatestClientId = c.ClientId
# MAGIC
# MAGIC
# MAGIC LEFT JOIN client_KeyStoreKey t1 ON c.GcobId = t1.keyStore_value AND SUBSTR(c.SourceClient, 0, 3) = 'LEC' AND t1.KeyStore_type = 'GCOBID'
# MAGIC LEFT JOIN client_PartyRole t3 ON t1.gcid = t3.gcid AND t3.party_role = 'Customer'

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from Firebird_Master where gcobid in (672,657,5224,5085) and ClientType <> 'Legal Entity'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from clients where FullLegalName in ('Alfred  Elzas', 'Wilhelmina Gerritdina Maria  van Dam')

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from clients where gcobid = 2948

# COMMAND ----------

# MAGIC %sql
# MAGIC --select gcobid,clienttype,count(gcobid) from radar.Clients group by gcobid,clienttype having count(gcobid) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS radar.Clients

# COMMAND ----------

spark.sql('select * from Clients').write.mode('overwrite').saveAsTable('radar.Clients')

# COMMAND ----------

# MAGIC %sql
# MAGIC --select distinct ReviewTypeName from clients;

# COMMAND ----------

# MAGIC %sql
# MAGIC --select LatestCaseId, ValidatedRiskLevel, CaseStatusName, ClientId, * from clients where gcobid = '4736' and SourceSystemReference = 'GCOB_LegalEntity'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GlobalFiles AS
# MAGIC select distinct
# MAGIC ClientId
# MAGIC ,SourceClient
# MAGIC ,case when substr(SourceClient,0,3) = 'LEC' and SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC       when substr(SourceClient,0,2) = 'NP' and SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC       when substr(SourceClient,0,6) = 'L2_LEC' and SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC       when substr(SourceClient,0,5) = 'L2_NP' and SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC ,ClientType
# MAGIC ,GcobId
# MAGIC ,FullLegalName 
# MAGIC ,ValidatedRiskLevel
# MAGIC ,NextReviewDate
# MAGIC ,Casephase
# MAGIC ,CountryOfOperation
# MAGIC ,ClientLifeCycleName
# MAGIC from Firebird_Master 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS radar.GlobalFiles

# COMMAND ----------

spark.sql('select * from GlobalFiles').write.mode('overwrite').saveAsTable('radar.GlobalFiles')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS radar.LocalClientOwners

# COMMAND ----------

spark.sql('select * from party_local_client_Owners').write.mode('overwrite').saveAsTable('radar.LocalClientOwners')

# COMMAND ----------

# %sql
# CREATE OR REPLACE TEMPORARY VIEW userlist AS
# SELECT DISTINCT
#   SUBSTRING(AssignedUserID, INSTR(AssignedUserId, '\\') + 1) AS AssignedUserAlias
#   , WorkItemAssignedUserName AS AssignedUser
#   , CAST(MIN(WorkItemCreatedDate) AS DATE) AS DateFirstSeenInGCOB
#   , CAST(MAX(WorkItemCreatedDate) AS DATE) AS DateLastSeenInGCOB
# FROM party_workitem
# WHERE ResponsibleRole <> 1
#   AND LEFT(WorkItemAssignedUserName, 3) <> 'eu.'
#   AND WorkItemAssignedUserName <> 'Unknown'
#   AND AssignedUserID <> ''
# GROUP BY WorkItemAssignedUserName, AssignedUserId


# COMMAND ----------

# %sql
# drop table IF EXISTS radar.userlist_app_migration

# COMMAND ----------

# spark.sql('select * from userlist').write.mode('overwrite').saveAsTable('radar.userlist_app_migration')

# COMMAND ----------

# MAGIC %sql
# MAGIC --Code for Process Mining Event Log
# MAGIC CREATE OR REPLACE TEMPORARY VIEW VW_ProcessMiningEventLog AS
# MAGIC WITH dml_cte AS (
# MAGIC select CaseId,DateCreated,COALESCE(DateCompleted,DateResponded) as DateCompleted from party_request_for_information
# MAGIC ),
# MAGIC
# MAGIC mycte AS
# MAGIC (
# MAGIC     SELECT CaseId, DateCreated,DateCompleted  
# MAGIC         , CASE
# MAGIC             WHEN datediff(day, LAG(DateCompleted ) OVER (PARTITION BY CaseId ORDER BY DateCreated), DateCreated) < 1 THEN 0
# MAGIC             ELSE 1
# MAGIC         END AS flag
# MAGIC     FROM dml_cte
# MAGIC     WHERE DateCompleted IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC mycte1 AS
# MAGIC (
# MAGIC     SELECT *, SUM(flag) OVER (PARTITION BY CaseId ORDER BY DateCreated) AS grp
# MAGIC     FROM mycte
# MAGIC ),
# MAGIC
# MAGIC RFI_CTE1 AS 
# MAGIC (
# MAGIC SELECT CaseId, MIN(DateCreated) AS startDate, MAX(DateCompleted) AS end_dt
# MAGIC FROM mycte1
# MAGIC GROUP BY CaseId, grp
# MAGIC ),
# MAGIC
# MAGIC -- IF Case completed, only RFI's that were completed before end of case
# MAGIC
# MAGIC RFI_CTE AS
# MAGIC (
# MAGIC SELECT
# MAGIC RFI.CaseId
# MAGIC ,CC.SourceClient
# MAGIC ,CC.GcobId
# MAGIC ,RFI.Startdate
# MAGIC ,IF(RFI.end_dt > CC.AssessmentCompletedDate,CC.AssessmentCompletedDate, RFI.end_dt) AS end_dt
# MAGIC FROM RFI_CTE1 AS RFI
# MAGIC LEFT JOIN 
# MAGIC -- this selects per Case the last time KYC assessment was completed
# MAGIC (select C.GcobId,C.SourceClient,C.caseID, max(WI.WorkItemCompletedDate) AS AssessmentCompletedDate
# MAGIC FROM party_case_client_details AS C 
# MAGIC LEFT JOIN party_workitem AS WI ON C.SourceClient = WI.SourceClient
# MAGIC LEFT JOIN gcob_static_CaseStatusType AS CS on CS.StatusId=WI.CaseStatusTypeWhenCreated 
# MAGIC WHERE C.CaseStatusName = 'Completed' 
# MAGIC AND WI.WorkItemCompletedDate IS NOT NULL 
# MAGIC AND CS.Name= 'KYC assessment in progress'
# MAGIC GROUP BY C.CaseID,C.SourceClient,C.GcobId
# MAGIC ) AS CC on CC.CaseId = RFI.CASEID
# MAGIC -- just to be sure, only where RFI time is positive.
# MAGIC WHERE IF(RFI.end_dt > CC.AssessmentCompletedDate,CC.AssessmentCompletedDate, RFI.end_dt) > RFI.Startdate 
# MAGIC ) ,
# MAGIC
# MAGIC RFI_CTE_TEST AS 
# MAGIC (
# MAGIC SELECT R1.GcobId,R1.SourceClient,R1.CaseId, R1.StartDate, R1.end_dt, r2.startDate as r2start
# MAGIC FROM RFI_CTE R1
# MAGIC LEFT JOIN RFI_CTE R2 ON (R1.CaseId = R2.CaseId) AND (R1.startDate > R2.startDate AND R1.startDate < R2.end_dt)
# MAGIC where R2.startDate is not null
# MAGIC ),
# MAGIC
# MAGIC -- ALL WI logic:
# MAGIC -- combine existing WI and add RFI WI
# MAGIC -- can be no overlap between any WI, but also no room in between.
# MAGIC -- 0. Rename WI steps to maincasephase steps
# MAGIC -- 1. Existing WI: if startdate is during a RFI - then startdate = enddate RFI
# MAGIC -- 2. Existing WI: if enddate is during a RFI - then enddate = startdate RFI
# MAGIC -- 3. remove case steps with negative timedelta between start and end. 
# MAGIC -- 4. Union RFI's to Workitems.
# MAGIC -- 5. RFI end date is max. case end date.
# MAGIC
# MAGIC
# MAGIC WI_clean_CTE AS
# MAGIC (
# MAGIC SELECT WI.CaseId
# MAGIC        ,WI.SourceClient 
# MAGIC        ,WI.GcobId
# MAGIC        , CASE 
# MAGIC               WHEN CS.Name IN ('GCOB Review in progress', 'Initiation In Progress') AND WI.WorkItemCreatedDate > T11.`Assessment in progress` THEN 'Rebound Initiation'
# MAGIC               WHEN CS.Name IN ('GCOB Review in progress', 'Initiation In Progress') THEN 'Initiation'
# MAGIC               WHEN CS.Name IN ('Ready for KYC assessment') THEN 'Ready for KYC assessment'
# MAGIC               WHEN (CS.Name IN ('KYC assessment in progress') AND T11.`4-EYE Check` < WI.WorkItemCreatedDate) THEN 'Rebound Assessment'
# MAGIC               WHEN (CS.Name IN ('KYC assessment in progress', 'Ready for Screening', 'Screening in progress', 'Ready for identification', 'Identification in progress')) AND (T10.DateCreated > WI.WorkItemCreatedDate) THEN 'Assessment 1'
# MAGIC               WHEN (CS.Name IN ('KYC assessment in progress', 'Ready for Screening', 'Screening in progress', 'Ready for identification', 'Identification in progress')) AND (T10.DateCreated < WI.WorkItemCreatedDate) THEN 'Assessment 2'
# MAGIC               WHEN (CS.Name IN ('KYC assessment in progress')) THEN 'Assessment 1'
# MAGIC               WHEN CS.Name IN ('Product Offboarding confirmation in progress', 'Ready for Product Offboarding confirmation') THEN 'Product Offboarding'
# MAGIC               WHEN CS.Name IN ('Ready for 4 eye check') THEN 'Ready for QC'
# MAGIC               WHEN CS.Name IN ('4 eye check in progress') THEN 'QC'
# MAGIC               WHEN CS.Name IN ('Client committee sign off requested' , 'Senior management sign off requested') THEN 'Client committee sign off'
# MAGIC               WHEN CS.Name IN ('Client Owner approval requested','Client owner sign off requested','Local client owner sign off requested') THEN 'Client Owner Sign-off'
# MAGIC               WHEN CS.Name IN ('Product fulfilment in progress') THEN 'Product Fulfillment'
# MAGIC               WHEN CS.Name IN ('Completed', 'Cancelled') THEN 'Completed'
# MAGIC           ELSE CS.Name
# MAGIC         END AS WorkItemType
# MAGIC         , WI.WorkItemCreatedDate
# MAGIC         , WI.WorkItemCompletedDate
# MAGIC         , RFI2.Startdate
# MAGIC         , RFI1.end_dt
# MAGIC         , COALESCE(RFI1.end_dt, WI.WorkItemCreatedDate) AS WI_Startdate
# MAGIC         , COALESCE(RFI2.Startdate, WI.WorkItemCompletedDate) AS WI_Enddate
# MAGIC         FROM party_workitem AS WI 
# MAGIC         LEFT JOIN RFI_CTE AS RFI1 ON WI.CaseId = RFI1.CaseId AND (WI.WorkItemCreatedDate> RFI1.startDate AND WI.WorkItemCreatedDate < RFI1.end_dt) 
# MAGIC         LEFT JOIN RFI_CTE AS RFI2 ON WI.CaseId = RFI2.CaseId AND (WI.WorkItemCompletedDate > RFI2.startDate AND WI.WorkItemCompletedDate < RFI2.end_dt)
# MAGIC          left JOIN gcob_rfi_status AS T10 on WI.SourceClient
# MAGIC  = T10.SourceClient
# MAGIC
# MAGIC          LEFT JOIN workitems_status AS T11 on WI.SourceClient
# MAGIC  = T11.SourceClient
# MAGIC
# MAGIC        LEFT JOIN gcob_static_CaseStatusType AS CS on CS.StatusId=WI.CaseStatusTypeWhenCreated 
# MAGIC )
# MAGIC , ALL_WI1 AS
# MAGIC (
# MAGIC SELECT WI.CaseId
# MAGIC ,GcobId
# MAGIC ,SourceClient
# MAGIC , WorkItemType
# MAGIC ,WI_Startdate
# MAGIC ,WI_Enddate
# MAGIC from WI_clean_CTE AS WI
# MAGIC WHERE WI_Startdate < WI_Enddate 
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT RFI.CaseId
# MAGIC ,RFI.GcobId
# MAGIC ,RFI.SourceClient
# MAGIC , 'Client Outreach' as WorkItemType
# MAGIC , RFI.Startdate AS WI_Startdate
# MAGIC , RFI.end_dt AS WI_Enddate
# MAGIC FROM RFI_CTE AS RFI
# MAGIC WHERE RFI.end_dt IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC --order all WI's by startdate
# MAGIC -- 1. If next WI is Client outreach, and startdate is before current enddate. then WI enddate = CO startdate
# MAGIC -- 2. If former WI is Client outreach, and WI startdate is before CO,enddate startdate = enddate.
# MAGIC
# MAGIC ,ALL_WI2 AS
# MAGIC (
# MAGIC SELECT 
# MAGIC CaseID
# MAGIC ,GcobId
# MAGIC ,SourceClient
# MAGIC ,WorkItemType
# MAGIC ,WI_Startdate
# MAGIC ,WI_Enddate
# MAGIC FROM ALL_WI1
# MAGIC ) 
# MAGIC
# MAGIC ,ALL_WI3 AS (
# MAGIC SELECT WI.CaseId
# MAGIC ,WI.SourceClient
# MAGIC ,WI.GcobId
# MAGIC ,WI.WorkItemType
# MAGIC ,WI.WI_Startdate
# MAGIC ,COALESCE(WI2.WI_Startdate,WI.WI_Enddate) AS WI_Enddate
# MAGIC FROM ALL_WI2 AS WI
# MAGIC LEFT JOIN ALL_WI2 AS WI2 ON WI.CaseId = WI2.CaseId AND WI.WI_Startdate < WI2.WI_Startdate AND WI.WI_Enddate > WI2.WI_Enddate AND WI2.WorkItemType = 'Client Outreach'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC -- This adds 2nd piece of assessment if one RFI happens between start and end of 1 assessment WI step.
# MAGIC SELECT WI1.CaseId
# MAGIC ,WI1.SourceClient
# MAGIC ,WI1.GcobId
# MAGIC ,CASE WHEN WI1.WorkItemType = 'Assessment 1' THEN 'Assessment 2' ELSE WI1.WorkItemType END AS WorkItemType
# MAGIC ,WI2.WI_Enddate AS WI_Startdate
# MAGIC ,WI1.WI_Enddate
# MAGIC FROM ALL_WI2 AS WI1
# MAGIC LEFT JOIN ALL_WI2 AS WI2 ON WI1.CaseId = WI2.CaseId AND WI1.WI_Startdate < WI2.WI_Startdate AND WI1.WI_Enddate > WI2.WI_Enddate
# MAGIC WHERE WI2.WorkItemType = 'Client Outreach'
# MAGIC )
# MAGIC
# MAGIC SELECT t1.*  
# MAGIC , t2.KYCUserTeam
# MAGIC , t2.ReviewTypeName 
# MAGIC , t3.GlobalKYCPortfolio
# MAGIC , t3.SectorTeam
# MAGIC , t3.ValidatedRiskLevel--Risklevel
# MAGIC ,t2.ModelCalculatedRiskLevel
# MAGIC ,t2.ModelRecalculatedRiskLevel
# MAGIC ,t3.AdverseInfoRiskLevel
# MAGIC ,t3.GeographicalRiskLevel
# MAGIC ,t3.EntityTypeRiskLevel
# MAGIC ,t3.StructureRiskLevel
# MAGIC ,t3.SectorRiskLevel
# MAGIC ,t3.ProductAndServiceRiskLevel
# MAGIC ,t3.PEPRiskLevel
# MAGIC ,t3.TransactionRiskLevel
# MAGIC ,t3.DistributionRiskLevel
# MAGIC ,t3.ThirdPartyRiskLevel
# MAGIC FROM ALL_WI3 as t1
# MAGIC left join Cases as t2 on t1.SourceClient  = t2.SourceClient
# MAGIC left join Clients as t3 on t2.gcobid = t3.Gcobid and t2.ClientType=t3.ClientType

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS radar.ProcessMiningEventLog

# COMMAND ----------

#Writing the view to catalog
spark.sql('select * from VW_ProcessMiningEventLog').write.mode('overwrite').saveAsTable('radar.ProcessMiningEventLog')
