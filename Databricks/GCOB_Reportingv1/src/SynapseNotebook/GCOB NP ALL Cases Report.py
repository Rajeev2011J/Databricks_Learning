# Databricks notebook source
import pandas as pd
from datetime import datetime, timedelta
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, concat, col
import os

# COMMAND ----------

currentDate = datetime.today().strftime('%Y%m%d')
Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
load_dts = 'LOADED_DTS=' + Yesterdate + 'T000000Z'
print (load_dts)

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'CaseService_NaturalPerson_NaturalPersonCase', 
'CaseService_NaturalPerson_NaturalPersonClient', 
'CaseService_NaturalPerson_CddTypeReference', 
'CaseService_NaturalPerson_CddRiskOverview', 
'CaseService_NaturalPerson_DynamicRiskModelInstanceReference', 
'RiskModel_dbo_Instance',
'RiskModel_dbo_InstanceCategory',
'RiskModel_dbo_InstanceCalculation', 
'RiskModel_dbo_InstanceCalculationCategory', 
'RiskModel_dbo_Category', 
#'RiskModel_dbo_InstanceAnswer', 
'CaseService_NaturalPerson_RabobankEntityReference',  
'CaseService_NaturalPerson_NaturalPersonBusinessActivity', 
'CaseService_NaturalPerson_Address',
'CaseService_NaturalPerson_WorkItem', 
'CaseService_NaturalPerson_InvolvedStaffMember',
'CaseService_NaturalPerson_UserReference', 
'CaseService_NaturalPerson_NaturalPersonCitizenship',
#'CaseService_NaturalPerson_ScreeningResults' ,
'CaseService_NaturalPerson_CountryReference', 
'CaseService_dbo_BusinessLine', 
'CaseService_NaturalPerson_NaturalPersonCrsAssessment',
'CaseService_NaturalPerson_NaturalPersonCrsClassificationReference',
'CaseService_NaturalPerson_NaturalPersonFatcaAssessment',
'CaseService_NaturalPerson_NaturalPersonFatcaClassificationReference',
'CaseService_NaturalPerson_BusinessLineReference',
'CaseService_dbo_TeaReason',
'CaseService_dbo_EdrReason',
'CaseService_case_OffBoardingReasonReference',
'RiskModel_dbo_Model',
'CaseService_dbo_SystemIdType',
'CaseService_NaturalPerson_NaturalPersonClientIdentifier',
'CaseService_NaturalPerson_NaturalPersonNationality'
]

# TODO make paramets for GDP load_dts

for item in load_df:
    #print('df_' + item + ' = spark.read.load(\'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/' + item + '/100/data/' + load_dts + '/*.parquet\', format=\'parquet\')') # .toPandas()
    spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/'+item+'/100/data/' + load_dts + '/*.parquet', format='parquet').createOrReplaceTempView(item)

# COMMAND ----------

# create static dfs and temp view

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
df_gcob_static_CaseStatusType = spark.createDataFrame(zip(StatusId_list, Name_list), ['StatusId', 'Name'])
df_gcob_static_CaseStatusType.createOrReplaceTempView('gcob_static_CaseStatusType')




Id_list = [i for i in range(5)]
Description_list = ['Prospect', 'Client', 'FormerProspect', 'ExitClient', 'FormerClient']
# create pyspark dataframe from lists
df_gcob_static_ClientLifeCycleStatus = spark.createDataFrame(zip(Id_list, Description_list), ['Id', 'Description'])
df_gcob_static_ClientLifeCycleStatus.createOrReplaceTempView('gcob_static_ClientLifeCycleStatus')




Id_list = [i+1 for i in range(9)]
Description_list = ['Initial On-Boarding'
, 'Amendment'
, 'Periodic Review'
, 'Event Driven Review'
, 'Client Offboarding'
, 'Change of Client Owner'
, 'Product Offboarding'
, 'Product Offboarding (Resume)'
, 'Tailored Event Assessment']
# create pyspark dataframe from lists
df_gcob_static_ReviewType = spark.createDataFrame(zip(Id_list, Description_list), ['Id', 'Description'])
df_gcob_static_ReviewType.createOrReplaceTempView('gcob_static_ReviewType')



#Static Risk Level
Risk_list = [-1,1,2,3,4,0]
Risk_Description = ['Unknown', 'Low', 'Medium', 'High', 'Unacceptable','Incomplete']
# create pyspark dataframe from lists
df_gcob_static_RiskLevel = spark.createDataFrame(zip(Risk_list, Risk_Description), ['Id', 'Description'])
df_gcob_static_RiskLevel.createOrReplaceTempView('gcob_static_RiskLevel')



#Static Risk Level Mapping
SourceId = [0,1,2,3,4,5,6,7]
DestinationId = [0,0,1,2,3,3,3,4]
# create pyspark dataframe from lists
df_gcob_static_RiskLevelMapping = spark.createDataFrame(zip(SourceId, DestinationId), ['SourceId', 'DestinationId'])
df_gcob_static_RiskLevelMapping.createOrReplaceTempView('gcob_static_RiskLevelMapping')


#Static Further Approval Type
Approval_list = [-1,0,1,2]
Approval_Description = ['Unknown', 'No', 'Yes, Client Committee Approval', 'Yes, Senior Management Approval']
# create pyspark dataframe from lists
df_gcob_static_FurtherApprovalType = spark.createDataFrame(zip(Approval_list, Approval_Description), ['Id', 'Description'])
df_gcob_static_FurtherApprovalType.createOrReplaceTempView('gcob_static_FurtherApprovalType')


# COMMAND ----------

sql_NP_cases = spark.sql("""
SELECT        
'GCOB' as SourceSystem
,cl.Id AS NaturalPersonClientId
,c.Id as CaseId
,cl.GcobId
,concat(cl.FirstName,' ',cl.MiddleName,' ',cl.LastName) as FullLegalName
,case when c.CaseReviewType=9 then c.OtherReason else null end as TEAOtherReason
,tr.Description as TEAReviewReasonDescription
,rw.Description as ReviewTypeName
,cs.Name as CaseStatusName
,bl.Name as BusinessLineName
,u1.DisplayName AS GlobalClientOwner
,e.RabobankEntityName AS GlobalClientOwnerLocation
,lcs.Description as ClientLifeCycleName
,cddt.CddTypeName AS CddType
,edr.Description as EdrReason
,cl.NextReviewDateAsString as NextReviewDate
,cl.IsRingfenced AS RingFenced
,c.ScheduledCompletionDateAsString as ScheduledCompletionDate
,fatca.IsEligible as IsEligibleForFatcaAssessment
,fatcaRef.Name as FatcaClassification
,cl.IsEligibleForCrsAssessment
,crsRef.Name AS CrsClassification
,cl.FullNameInLocalLanguage
,cl.DateOfBirth
,reg.Street as ResidentialStreet
,reg.Number as ResidentialNumber
,reg.PostalCode as RegisteredPostalCode
,reg.City as RegisteredCity
,reg.Region as RegisteredRegion
,off.Description as OffBoardingReason
,cl.CddRiskOverviewId

FROM 
CaseService_NaturalPerson_NaturalPersonCase AS c 
INNER JOIN CaseService_NaturalPerson_NaturalPersonClient AS cl ON cl.Id = c.NaturalPersonClientId 
LEFT OUTER JOIN CaseService_NaturalPerson_InvolvedStaffMember AS sm ON sm.Id = cl.GlobalClientOwnerId 
LEFT OUTER JOIN CaseService_NaturalPerson_UserReference AS us ON us.UserId = sm.UserReferenceId 
LEFT OUTER JOIN CaseService_NaturalPerson_BusinessLineReference AS bl ON bl.BusinessLineId = sm.BusinessLineReferenceId 
LEFT OUTER JOIN CaseService_NaturalPerson_RabobankEntityReference AS e ON e.RabobankEntityId = sm.RabobankEntityReferenceId
LEFT OUTER JOIN CaseService_NaturalPerson_UserReference u1 on sm.UserReferenceId = u1.UserId
LEFT OUTER JOIN CaseService_NaturalPerson_Address AS reg ON reg.AddressId = cl.ResidentialAddressId 
LEFT OUTER JOIN CaseService_NaturalPerson_NaturalPersonCrsAssessment AS crs ON crs.NaturalPersonCrsAssessmentId = cl.CrsAssessmentId 
LEFT OUTER JOIN CaseService_NaturalPerson_NaturalPersonCrsClassificationReference crsRef on crsRef.NaturalPersonCrsClassificationId = crs.NaturalPersonCrsClassificationReferenceId 
LEFT OUTER JOIN CaseService_NaturalPerson_NaturalPersonFatcaAssessment AS fatca ON fatca.NaturalPersonFatcaAssessmentId = cl.FatcaAssessmentId 
LEFT OUTER JOIN CaseService_NaturalPerson_NaturalPersonFatcaClassificationReference AS fatcaRef ON fatcaRef.NaturalPersonFatcaClassificationId = fatca.NaturalPersonFatcaClassificationReferenceId  
LEFT OUTER JOIN CaseService_dbo_TeaReason tr on c.ReasonId=tr.id and c.CaseReviewType=9 
LEFT OUTER JOIN gcob_static_ReviewType rw ON c.CaseReviewType = rw.Id
LEFT OUTER JOIN gcob_static_CaseStatusType cs ON c.CurrentStatus = cs.StatusId
LEFT OUTER JOIN gcob_static_ClientLifeCycleStatus lcs on cl.ClientLifecycleStatusTypeId = lcs.Id
LEFT OUTER JOIN CaseService_NaturalPerson_CddTypeReference cddt on cl.CddTypeId = cddt.CddTypeId
LEFT OUTER JOIN CaseService_dbo_EdrReason edr on c.ReasonId = edr.id and c.CaseReviewType = 4
LEFT OUTER JOIN CaseService_case_OffBoardingReasonReference off ON c.reasonId = off.OffBoardingReasonReferenceId and c.CaseReviewType in(6,7)

""")
sql_NP_cases.createOrReplaceTempView('NP_cases')

# COMMAND ----------

#%%sql
#select * from NP_cases limit 5

# COMMAND ----------

sql_NP_Risk = spark.sql("""
With CTE_RiskCategories AS
(
SELECT ir.NaturalPersonClientId
            , i.Id
            , c.Name as CategoryName
            , rlm.DestinationId as CalculatedRiskLevelId
        FROM 
        RiskModel_dbo_Instance i
        inner join RiskModel_dbo_InstanceCalculation ic
            on ic.InstanceId = i.Id
        inner join RiskModel_dbo_InstanceCalculationCategory icc
            on icc.InstanceCalculationId = ic.id
        inner join CaseService_NaturalPerson_DynamicRiskModelInstanceReference ir
            on ir.InstanceId = i.Id and ir.Expired is null
        inner join RiskModel_dbo_Category c
            on c.Id = icc.CategoryId
        inner join gcob_static_RiskLevelMapping rlm
            on rlm.SourceId = icc.CalculatedRiskLevelId
        left outer join RiskModel_dbo_InstanceCategory icat
            on icat.CategoryId = icc.CategoryId
            and icat.InstanceId = i.Id
        where ic.Expired is null
)
, CTE_RiskFactors AS 
   (

    select NaturalPersonClientId 
    ,Geographical AS GeoRisk
    ,`Entity Type` AS EntityTypeRisk
    ,Structure AS StructureRisk
    ,Sector AS SectorRisk
    ,`Products and Services` AS ProductRisk
    ,PEP AS PEPRisk
    ,`Transaction` AS TransactionRisk
    ,`Distribution Channel` AS DistributionRisk
    ,`Third Party` AS ThirdPartyRisk
    ,`Adverse Info` AS AdverseInfoRisk
    ,Other AS OtherRisk

    FROM (
    select NaturalPersonClientId
    , MAX(CASE WHEN CategoryName = 'Adverse Info' then CalculatedRiskLevelId else NULL END) as `Adverse Info`
    , MAX(CASE WHEN CategoryName = 'Distribution Channel' then CalculatedRiskLevelId else NULL END) as `Distribution Channel`
    , MAX(CASE WHEN CategoryName = 'Entity Type' then CalculatedRiskLevelId else NULL END) as `Entity Type`
    , MAX(CASE WHEN CategoryName = 'General' then CalculatedRiskLevelId else NULL END) as General
    , MAX(CASE WHEN CategoryName = 'Geographical' then CalculatedRiskLevelId else NULL END) as Geographical
    , MAX(CASE WHEN CategoryName = 'Other' then CalculatedRiskLevelId else NULL END) as Other
    , MAX(CASE WHEN CategoryName = 'PEP' then CalculatedRiskLevelId else NULL END) as PEP
    , MAX(CASE WHEN CategoryName = 'Products and Services' then CalculatedRiskLevelId else NULL END) as `Products and Services`
    , MAX(CASE WHEN CategoryName = 'Sector' then CalculatedRiskLevelId else NULL END) as Sector
    , MAX(CASE WHEN CategoryName = 'Structure' then CalculatedRiskLevelId else NULL END) as Structure
    , MAX(CASE WHEN CategoryName = 'Third Party' then CalculatedRiskLevelId else NULL END) as `Third Party`
    , MAX(CASE WHEN CategoryName = 'Transaction' then CalculatedRiskLevelId else NULL END) as `Transaction`

    FROM
        CTE_RiskCategories
    GROUP BY
        NaturalPersonClientId
    )
	),

    CTE_ApplicableRiskCategories as
    (
        select distinct
			ir.NaturalPersonClientId
			, c.Name as CategoryName
            ,cast(ict.IsRiskMaterial as int) as CalculatedRiskApplId
		FROM RiskModel_dbo_Instance i
		JOIN RiskModel_dbo_InstanceCalculation ic 
			on ic.InstanceId = i.Id
		JOIN CaseService_NaturalPerson_DynamicRiskModelInstanceReference ir
            on ir.InstanceId = i.Id and ir.Expired is null
		JOIN RiskModel_dbo_InstanceCategory ict  
			on ict.InstanceId = i.Id
		JOIN RiskModel_dbo_Category c 
			on c.Id=ict.CategoryId
		WHERE ic.Expired is null
    ),

    CTE_ApplicableRisk AS 
    (

    select NaturalPersonClientId 
    ,Geographical AS GeographicalApplicableRisk
    ,`Entity Type` AS EntityTypeApplicableRisk
    ,Structure AS StructureApplicableRisk
    ,Sector AS SectorApplicableRisk
    ,`Products and Services` AS ProductsApplicableRisk
    ,PEP AS PoliticallyExposedPersonsApplicableRisk
    ,`Transaction` AS TransactionApplicableRisk
    ,`Distribution Channel` AS DistributionChannelApplicableRisk
    ,`Third Party` AS ThirdPartyApplicableRisk
    ,`Adverse Info` AS AdverseInfoApplicableRisk
    ,Other AS OtherApplicableRisk
    FROM  (
    select NaturalPersonClientId
    , MAX(CASE WHEN CategoryName = 'Adverse Info' then CalculatedRiskApplId else NULL END) as `Adverse Info`
    , MAX(CASE WHEN CategoryName = 'Distribution Channel' then CalculatedRiskApplId else NULL END) as `Distribution Channel`
    , MAX(CASE WHEN CategoryName = 'Entity Type' then CalculatedRiskApplId else NULL END) as `Entity Type`
    , MAX(CASE WHEN CategoryName = 'General' then CalculatedRiskApplId else NULL END) as General
    , MAX(CASE WHEN CategoryName = 'Geographical' then CalculatedRiskApplId else NULL END) as Geographical
    , MAX(CASE WHEN CategoryName = 'Other' then CalculatedRiskApplId else NULL END) as Other
    , MAX(CASE WHEN CategoryName = 'PEP' then CalculatedRiskApplId else NULL END) as PEP
    , MAX(CASE WHEN CategoryName = 'Products and Services' then CalculatedRiskApplId else NULL END) as `Products and Services`
    , MAX(CASE WHEN CategoryName = 'Sector' then CalculatedRiskApplId else NULL END) as Sector
    , MAX(CASE WHEN CategoryName = 'Structure' then CalculatedRiskApplId else NULL END) as Structure
    , MAX(CASE WHEN CategoryName = 'Third Party' then CalculatedRiskApplId else NULL END) as `Third Party`
    , MAX(CASE WHEN CategoryName = 'Transaction' then CalculatedRiskApplId else NULL END) as `Transaction`
    FROM
        CTE_ApplicableRiskCategories
    GROUP BY
        NaturalPersonClientId
    )),

    Risk_Level_Model as
    (
    select distinct cl.Id as NaturalPersonClientId,drmi.ModelId,rlmMc.DestinationId AS ModelCalculatedRiskLevel
    ,rlmRc.DestinationId AS ModelRecalculatedRiskLevel
     FROM
        CaseService_NaturalPerson_NaturalPersonClient cl
        LEFT OUTER JOIN CaseService_NaturalPerson_DynamicRiskModelInstanceReference drmref 
            ON drmref.NaturalPersonClientId = cl.Id
            AND drmref.Expired IS NULL
        LEFT OUTER JOIN RiskModel_dbo_InstanceCalculation drmic ON drmic.InstanceId = drmref.InstanceId 
            AND drmic.Expired IS NULL
        LEFT OUTER JOIN RiskModel_dbo_Instance drmi ON drmi.Id = drmref.InstanceId
        LEFT OUTER JOIN gcob_static_RiskLevelMapping rlmMc 
            on rlmMc.SourceId = drmic.CalculatedRiskLevelId
        LEFT OUTER JOIN gcob_static_RiskLevelMapping rlmRc 
            on rlmRc.SourceId = drmic.RecalculatedRiskLevelId
    )

    select 
    cases.*
    , m.Name as RiskModelName
    , val.Description as ValidatedRiskLevel
    , cal.Description as ModelCalculatedRiskLevel
    , rcal.Description as ModelRecalculatedRiskLevel
    , geo.Description as GeographicalRiskLevel
    , Ent.Description as EntityTypeRiskLevel
    , stru.Description as StructureRiskLevel
    , sec.Description as SectorRiskLevel
    , prod.Description as ProductAndServiceRiskLevel
    , pep.Description as PEPRiskLevel
    , tran.Description as TransactionRiskLevel
    , dist.Description as DistributionRiskLevel
    , thir.Description as ThirdPartyRiskLevel
    , adv.Description as AdverseInfoRiskLevel
    , oth.Description as OtherRiskLevel
    , case when appl.GeographicalApplicableRisk = 1 then 'True' when appl.GeographicalApplicableRisk = 0 then 'False' else null end as GeographicalApplicableRisk
    , case when appl.EntityTypeApplicableRisk = 1 then 'True' when appl.EntityTypeApplicableRisk = 0 then 'False' else null end as EntityTypeApplicableRisk
    , case when appl.StructureApplicableRisk = 1 then 'True' when appl.StructureApplicableRisk = 0 then 'False' else null end as StructureApplicableRisk
    , case when appl.SectorApplicableRisk = 1 then 'True' when appl.SectorApplicableRisk = 0 then 'False' else null end as SectorApplicableRisk
    , case when appl.ProductsApplicableRisk = 1 then 'True' when appl.ProductsApplicableRisk = 0 then 'False' else null end as ProductsApplicableRisk
    , case when appl.PoliticallyExposedPersonsApplicableRisk = 1 then 'True' when appl.PoliticallyExposedPersonsApplicableRisk = 0 then 'False' else null end as PoliticallyExposedPersonsApplicableRisk
    , case when appl.TransactionApplicableRisk = 1 then 'True' when appl.TransactionApplicableRisk = 0 then 'False' else null end as TransactionApplicableRisk
    , case when appl.DistributionChannelApplicableRisk = 1 then 'True' when appl.DistributionChannelApplicableRisk = 0 then 'False' else null end as DistributionChannelApplicableRisk
    , case when appl.ThirdPartyApplicableRisk = 1 then 'True' when appl.ThirdPartyApplicableRisk = 0 then 'False' else null end as ThirdPartyApplicableRisk
    , case when appl.AdverseInfoApplicableRisk = 1 then 'True' when appl.AdverseInfoApplicableRisk = 0 then 'False' else null end as AdverseInfoApplicableRisk
    , case when appl.OtherApplicableRisk = 1 then 'True' when appl.OtherApplicableRisk = 0 then 'False' else null end as OtherApplicableRisk 
    from NP_cases as cases
    LEFT OUTER JOIN CTE_RiskFactors rf on cases.NaturalPersonClientId = rf.NaturalPersonClientId
    LEFT OUTER JOIN CTE_ApplicableRisk appl on cases.NaturalPersonClientId = appl.NaturalPersonClientId
    LEFT OUTER JOIN gcob_static_RiskLevel geo ON rf.GeoRisk = geo.Id
    LEFT OUTER JOIN gcob_static_RiskLevel Ent ON rf.EntityTypeRisk = Ent.Id
    LEFT OUTER JOIN gcob_static_RiskLevel stru ON rf.StructureRisk = stru.Id
    LEFT OUTER JOIN gcob_static_RiskLevel sec ON rf.SectorRisk = sec.Id
    LEFT OUTER JOIN gcob_static_RiskLevel prod ON rf.ProductRisk = prod.Id
    LEFT OUTER JOIN gcob_static_RiskLevel pep ON rf.PEPRisk = pep.Id
    LEFT OUTER JOIN gcob_static_RiskLevel tran ON rf.TransactionRisk = tran.Id
    LEFT OUTER JOIN gcob_static_RiskLevel dist ON rf.DistributionRisk = dist.Id
    LEFT OUTER JOIN gcob_static_RiskLevel thir ON rf.ThirdPartyRisk = thir.Id
    LEFT OUTER JOIN gcob_static_RiskLevel adv ON rf.AdverseInfoRisk = adv.Id
    LEFT OUTER JOIN gcob_static_RiskLevel oth ON rf.OtherRisk = oth.Id
    LEFT OUTER JOIN Risk_Level_Model rlm ON cases.NaturalPersonClientId = rlm.NaturalPersonClientId
    LEFT OUTER JOIN RiskModel_dbo_Model m ON rlm.ModelId = m.Id
    LEFT OUTER JOIN gcob_static_RiskLevel cal ON rlm.ModelCalculatedRiskLevel = cal.Id
    LEFT OUTER JOIN gcob_static_RiskLevel rcal ON rlm.ModelRecalculatedRiskLevel = rcal.Id
    LEFT OUTER JOIN CaseService_NaturalPerson_CddRiskOverview AS cddo ON cddo.CddRiskOverviewId = cases.CddRiskOverviewId
    LEFT OUTER JOIN gcob_static_RiskLevel val ON cddo.ValidatedCddRisk = val.Id

    """)
sql_NP_Risk.createOrReplaceTempView('NP_Cases_Risk')

# COMMAND ----------

sql_NP_Ver_Nat_Cit = spark.sql("""
with ApprovedClientVersions(GcobId, LatestApprovedVersionNumber) AS (
    SELECT        cl.GcobId, MAX(cl.Version) AS LatestApprovedVersionNumber
    FROM            CaseService_NaturalPerson_NaturalPersonClient AS cl 
    INNER JOIN CaseService_NaturalPerson_NaturalPersonCase AS c ON c.NaturalPersonClientId = cl.Id
    WHERE        (c.CurrentStatus in(8,9))
       GROUP BY cl.GcobId), ClientIdentifiers AS
    (SELECT        ci.NaturalPersonClientId, st.Name, ci.ValueOfIdentifier
      FROM            CaseService_NaturalPerson_NaturalPersonClientIdentifier AS ci INNER JOIN
                                CaseService_dbo_SystemIdType AS st ON st.Id = ci.SystemTypeReferenceId
)
,

Nationality_Citizenship as
(
    select c.NaturalPersonClientId, cr.name as ClientCitizenship ,cr1.name as ClientNationality 
    FROM 
    CaseService_NaturalPerson_NaturalPersonCase AS c 
    INNER JOIN CaseService_NaturalPerson_NaturalPersonClient AS cl ON cl.Id = c.NaturalPersonClientId 
    LEFT OUTER JOIN CaseService_NaturalPerson_NaturalPersonCitizenship cz on cl.Id = cz.NaturalPersonClientId 
    LEFT OUTER JOIN CaseService_NaturalPerson_CountryReference cr ON cr.CountryId = cz.CountryEntityReferenceId
    LEFT OUTER JOIN  CaseService_NaturalPerson_NaturalPersonNationality cn on cl.Id = cn.NaturalPersonClientId 
    LEFT OUTER JOIN CaseService_NaturalPerson_CountryReference cr1 ON cr1.CountryId = cn.CountryEntityReferenceId
),

Client_Nat_Cit as
(
    SELECT DISTINCT 
        NaturalPersonClientId
		,concat_ws(',', sort_array(collect_list(struct(ClientCitizenship))).ClientCitizenship) as ClientCitizenship
        ,concat_ws(',', sort_array(collect_list(struct(ClientNationality))).ClientNationality) as ClientNationality
    FROM
        Nationality_Citizenship AS ci
		group by NaturalPersonClientId
    
)

,ClientIdentifiersJoined AS (
        SELECT DISTINCT 
        NaturalPersonClientId
        ,Name
        ,concat_ws(',', sort_array(collect_list(struct(ValueOfIdentifier))).ValueOfIdentifier) as Identifiers
    FROM
        ClientIdentifiers AS ci
	    group by NaturalPersonClientId,Name
)

select cr.* 
, CASE 
        WHEN (v.LatestApprovedVersionNumber = cl.Version AND c.CurrentStatus = 8) 
		THEN 'True'
		WHEN (v.LatestApprovedVersionNumber = cl.Version AND c.CurrentStatus = 9)
        THEN 'True' ELSE 'False' END 
AS IsLatestApprovedVersionOfClient
,cnc.ClientCitizenship
,cnc.ClientNationality
,wwid.Identifiers AS WWID
,acbsid.Identifiers AS ACBS
,rutid.Identifiers AS RUTID
,isbid.Identifiers AS ISB
,iden.valueOfIdentifier as SalesforceClientID_nCino
,fat.Description as SubmitToClientCommittee
from NP_Cases_Risk cr
INNER JOIN CaseService_NaturalPerson_NaturalPersonCase AS c ON cr.NaturalPersonClientId = c.NaturalPersonClientId
INNER JOIN CaseService_NaturalPerson_NaturalPersonClient AS cl ON cl.Id = c.NaturalPersonClientId
LEFT OUTER JOIN ApprovedClientVersions v on cr.GcobId = v.GcobId
LEFT OUTER JOIN Client_Nat_Cit cnc on cr.NaturalPersonClientId = cnc.NaturalPersonClientId
LEFT OUTER JOIN ClientIdentifiersJoined AS wwid ON wwid.NaturalPersonClientId = cr.NaturalPersonClientId AND wwid.Name = 'WWID' 
LEFT OUTER JOIN ClientIdentifiersJoined AS acbsid ON acbsid.NaturalPersonClientId = cr.NaturalPersonClientId AND acbsid.Name = 'ACBS' 
LEFT OUTER JOIN ClientIdentifiersJoined AS rutid ON rutid.NaturalPersonClientId = cr.NaturalPersonClientId AND rutid.Name = 'RUT ID' 
LEFT OUTER JOIN ClientIdentifiersJoined AS isbid ON isbid.NaturalPersonClientId = cr.NaturalPersonClientId AND isbid.Name = 'ISB'
LEFT OUTER JOIN CaseService_NaturalPerson_NaturalPersonClientIdentifier iden ON cr.NaturalPersonClientId = iden.NaturalPersonClientId and iden.SystemTypeReferenceId = 129
LEFT OUTER JOIN gcob_static_FurtherApprovalType fat ON fat.Id = c.FurtherApprovalTypeId
""")

sql_NP_Ver_Nat_Cit.createOrReplaceTempView('NP_Ver_Nat_Cit')

# COMMAND ----------

sql_NP_WorkItem = spark.sql("""
WITH CTE_WorkItem AS
(
    select c.NaturalPersonClientId, c.CaseReviewType,c.CurrentStatus, w.*
    From CaseService_NaturalPerson_WorkItem w
    INNER JOIN CaseService_NaturalPerson_NaturalPersonCase AS c ON w.NaturalPersonCaseId = c.Id
    INNER JOIN CaseService_NaturalPerson_NaturalPersonClient AS cl ON cl.Id = c.NaturalPersonClientId
),

WorkItem_Created_Desc as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY NaturalPersonClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM CTE_WorkItem 
),

WorkItem_Completed_Desc as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY NaturalPersonClientId ORDER BY DateCompleted DESC) AS ROWNUM
    FROM CTE_WorkItem 
),

LastKYC as
(
select * from CTE_WorkItem where NaturalPersonStatusTypeWhenCreated in (3,8)
),
LastKYC_Row as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY NaturalPersonClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM LastKYC 
),

LastSentForKYC_Base as
(
select * from CTE_WorkItem where NaturalPersonStatusTypeWhenCreated = 2
),
LastSentForKYC as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY NaturalPersonClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM LastSentForKYC_Base 
),

INITInPro as
(
select * from CTE_WorkItem where NaturalPersonStatusTypeWhenCreated = 1
),
INITInPro_Row as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY NaturalPersonClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM INITInPro 
),

Reviewer as
(
    Select * from CTE_WorkItem where NaturalPersonStatusTypeWhenCreated = 5
),
Unique_Reviewer as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY NaturalPersonClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM Reviewer 
),

LastSent4Eye as
(
    Select * from CTE_WorkItem where NaturalPersonStatusTypeWhenCreated = 4
),
LastSent4Eye_Date as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY NaturalPersonClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM LastSent4Eye 
),

CompletedDateBase as
(
    select * FROM CTE_WorkItem
    where
    CurrentStatus = 9
    AND
    (
        (CaseReviewType = 1 AND NaturalPersonStatusTypeWhenCreated = 8) OR
        (CaseReviewType = 2 AND NaturalPersonStatusTypeWhenCreated = 5) OR
        (CaseReviewType = 3 AND NaturalPersonStatusTypeWhenCreated = 8) OR
        (CaseReviewType = 4 AND NaturalPersonStatusTypeWhenCreated = 8) OR
        (CaseReviewType = 6 AND NaturalPersonStatusTypeWhenCreated = 17) OR
        (CaseReviewType = 7 AND NaturalPersonStatusTypeWhenCreated = 20) OR
        (CaseReviewType = 8 AND NaturalPersonStatusTypeWhenCreated = 21) OR
        (CaseReviewType = 9 AND NaturalPersonStatusTypeWhenCreated = 8) 
    )
),
CompletedDate_derive as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY NaturalPersonClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM CompletedDateBase 
),
CompletedDate
(
    select * from CompletedDate_derive where ROWNUM = 1
),

WorkItem_Created_Asc as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY NaturalPersonClientId ORDER BY DateCreated ASC) AS ROWNUM
    FROM CTE_WorkItem 
),

ApprovalDateBase
(
    select * FROM CTE_WorkItem where NaturalPersonStatusTypeWhenCreated = 6 
        
),
ApprovalDate as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY NaturalPersonClientId ORDER BY DateCompleted DESC) AS ROWNUM
    FROM ApprovalDateBase 
),

DateSubForSignBase
(
    select * FROM CTE_WorkItem where NaturalPersonStatusTypeWhenCreated in(6,7)   
),
DateSubForSign as
(
    select * ,
    ROW_NUMBER() OVER (PARTITION BY NaturalPersonClientId ORDER BY DateCreated DESC) AS ROWNUM
    FROM DateSubForSignBase 
),

FinalDecisionDate as 
(
    select distinct cl.gcobid,max(c.FinalDecisionDateAsString) as FinalDecisionDate
    from CaseService_NaturalPerson_NaturalPersonCase c
    INNER JOIN CaseService_NaturalPerson_NaturalPersonClient cl on cl.Id = c.NaturalPersonClientId
    group by cl.gcobid
)

select 
c.* 
, u2.DisplayName AS 4EyeCheckReviewer
, u3.DisplayName AS CurrentAssignee
, u2.DisplayName AS Last4EyeCheckReviewer
, u4.DisplayName AS LastKYCAnalyst
, 4eye.DateCreated as LastSentFor4EyeCheck
, asses.DateCreated as LastSentForKYCAssesment
, u5.DisplayName AS InitiationInProgressAssignee
, comp.DateCompleted as CaseCompletedDate
, crea.DateCreated as CaseCreationDate
, 4eye.DateCreated as DateSubmittedFor4EyeCheck
, dsubs.DateCreated as DateSubmittedForSignOff
, u4.DisplayName AS KYCAssessmentInProgressAssignee
, asses.DateCreated as ReadyForKYCAssessmentDate
, ad.DateCompleted as ClientOwnerSignOffDate
, cast(fd.FinalDecisionDate as date) as FinalDecisionDate

from NP_Ver_Nat_Cit c
LEFT OUTER JOIN Unique_Reviewer ur ON c.NaturalPersonClientId = ur.NaturalPersonClientId and ur.ROWNUM = 1 
LEFT OUTER JOIN CaseService_NaturalPerson_UserReference u2 on ur.AssignedUserReferenceId = u2.UserId and ur.ROWNUM = 1
LEFT OUTER JOIN CompletedDate comp ON c.NaturalPersonClientId = comp.NaturalPersonClientId
LEFT OUTER JOIN WorkItem_Created_Asc crea ON c.NaturalPersonClientId = crea.NaturalPersonClientId and crea.ROWNUM = 1
LEFT OUTER JOIN WorkItem_Completed_Desc dcr ON c.NaturalPersonClientId = dcr.NaturalPersonClientId and dcr.ROWNUM = 1 
LEFT OUTER JOIN CaseService_NaturalPerson_UserReference u3 on dcr.AssignedUserReferenceId = u3.UserId and dcr.ROWNUM = 1
LEFT OUTER JOIN LastKYC_Row ana ON c.NaturalPersonClientId = ana.NaturalPersonClientId and ana.ROWNUM = 1 
LEFT OUTER JOIN CaseService_NaturalPerson_UserReference u4 on ana.AssignedUserReferenceId = u4.UserId and ana.ROWNUM = 1 
LEFT OUTER JOIN LastSent4Eye_Date 4eye ON c.NaturalPersonClientId = 4eye.NaturalPersonClientId and 4eye.ROWNUM = 1 
LEFT OUTER JOIN LastSentForKYC asses ON c.NaturalPersonClientId = asses.NaturalPersonClientId and asses.ROWNUM = 1 
LEFT OUTER JOIN INITInPro_Row init ON c.NaturalPersonClientId = init.NaturalPersonClientId and init.ROWNUM = 1 
LEFT OUTER JOIN CaseService_NaturalPerson_UserReference u5 on init.AssignedUserReferenceId = u5.UserId and init.ROWNUM = 1 
LEFT OUTER JOIN DateSubForSign dsubs ON c.NaturalPersonClientId = dsubs.NaturalPersonClientId and dsubs.ROWNUM = 1 
LEFT OUTER JOIN FinalDecisionDate as fd ON fd.gcobid = c.gcobid 
LEFT OUTER JOIN ApprovalDate ad ON c.NaturalPersonClientId = ad.NaturalPersonClientId and ad.ROWNUM = 1
""")
sql_NP_WorkItem.createOrReplaceTempView('NP_WorkItem')

# COMMAND ----------

df_NP_cases= sql_NP_WorkItem.withColumn("BusinessDate", F.lit(BusinessDate))

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS WR_RADAR" )
spark.sql("DROP TABLE IF EXISTS dbo.NP_All_Cases" )
df_NP_cases.write.mode("overwrite").saveAsTable("WR_RADAR.NP_All_Cases")

# COMMAND ----------


