# Databricks notebook source
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import os
import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import *

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

from datetime import datetime, timedelta

currentDate = datetime.today().strftime('%Y%m%d')
currentYear = datetime.today().strftime('%Y')
currentMonth = datetime.today().strftime('%m')
currentDay = datetime.today().strftime('%d')
yesterdayDay = (datetime.today() - timedelta(1)).strftime('%d')

print(currentDate, currentYear, currentMonth, currentDay)

# COMMAND ----------

currentDate = datetime.today().strftime('%Y%m%d')
Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
load_dts = 'LOADED_DTS=' + Yesterdate + 'T000000Z'
print (load_dts)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = ['CaseService_case_LegalEntityClient'
, 'CaseService_case_DynamicRiskModelInstanceReference'
, 'CaseService_case_Case'
, 'RiskModel_dbo_Category'
, 'RiskModel_dbo_InstanceCalculation'
, 'RiskModel_dbo_InstanceCalculationCategory'
, 'RiskModel_dbo_Model'
, 'RiskModel_dbo_Instance'
, 'RiskModel_dbo_InstanceCategory'
, 'CaseService_case_ClientProfile'
, 'RiskModel_dbo_InstanceAnswer'
, 'RiskModel_dbo_Question'
, 'RiskModel_dbo_PossibleAnswer'
, 'RiskModel_dbo_InstanceQuestionMateriality'
, 'CaseService_case_CountryReference'
, 'RiskModel_dbo_RiskLevel'
,'CaseService_case_LegalEntityClientIdentifier'
,'CaseService_dbo_SystemIdType'
,'CaseService_case_InvolvedStaffMember'
,'CaseService_dbo_RabobankEntity'
,'CaseService_user_GcobUser'
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

# MAGIC %md
# MAGIC ### Creating Data objects
# MAGIC 1. 10 Risk Categories per LegalEntityId.
# MAGIC 2. All Questions Asked and Answered per LegalEntityId
# MAGIC
# MAGIC 3. with 1 row per LegalentityId, all Questions and Risk Categories in that row
# MAGIC     - Pivot/Groupby over all the unique questionCodes
# MAGIC 4. If adding value, add for each questioncode if it was appliccable?

# COMMAND ----------

# Getting the right InstanceId and InstanceCalculationId per LEC
sdf_LE_CaseToInstance = spark.sql("""

    select t1.Id AS LegalEntityClientId
    , t1.GCOBId
    , t4.ModelId
    , t6.Name AS RiskModelName
    , t3.MaxInstanceId
    , t4.SourceSystemReferenceId
    , t4.Status
    , t5b.CalculatedRiskLevelId AS OverallCalculatedRiskLevelId
    , t5b.RecalculatedRiskLevelId AS OverallRecalculatedRiskLevelId
    , t5b.Id AS InstanceCalculationId

    from CaseService_case_LegalEntityClient as t1
    left join (select LegalEntityClientId, max(InstanceId) as MaxInstanceId from CaseService_case_DynamicRiskModelInstanceReference group by LegalEntityClientId ) as t3 on t3.LegalEntityClientId = t1.Id
    
    left join RiskModel_dbo_Instance t4 on t4.Id = t3.MaxInstanceId

    left join (select InstanceId, MAX(id) as MaxCalculationId from RiskModel_dbo_InstanceCalculation group by InstanceId) as t5 on t4.Id = t5.MaxCalculationId
    left join RiskModel_dbo_InstanceCalculation as t5b on t5.MaxCalculationId = t5b.Id

    LEFT JOIN RiskModel_dbo_Model t6 on t4.ModelId = t6.Id
    """)
    
sdf_LE_CaseToInstance.createOrReplaceTempView('CaseToInstance')

# COMMAND ----------

sdf_LE_CaseRiskCategoriesPivot = spark.sql("""
with CTE_RiskCategories AS
(
SELECT ir.LegalEntityClientId
            , i.Id
            , c.Name as CategoryName
            , rlm.DestinationId as CalculatedRiskLevelId
        FROM 
        RiskModel_dbo_Instance i
        inner join RiskModel_dbo_InstanceCalculation ic
            on ic.InstanceId = i.Id
        inner join RiskModel_dbo_InstanceCalculationCategory icc
            on icc.InstanceCalculationId = ic.id
        inner join CaseService_case_DynamicRiskModelInstanceReference ir
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

    select LegalEntityClientId 
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
    select LegalEntityClientId
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
        LegalEntityClientId
    )
	)
    select 
        LegalEntityClientId
        , geo.Description as GeographicalRetainedRiskLevel
        , Ent.Description as EntityTypeRetainedRiskLevel
        , stru.Description as StructureRetainedRiskLevel
        , sec.Description as SectorRetainedRiskLevel
        , prod.Description as ProductsRetainedRiskLevel
        , pep.Description as PoliticallyExposedPersonsRetainedRiskLevel
        , tran.Description as TransactionRetainedRiskLevel
        , dist.Description as DistributionChannelRetainedRiskLevel
        , thir.Description as ThirdPartyRetainedRiskLevel
        , adv.Description as AdverseInfoRetainedRiskLevel
        , oth.Description as OtherRetainedRiskLevel   
    from CTE_RiskFactors rf
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
    """)

sdf_LE_CaseRiskCategoriesPivot.createOrReplaceTempView('LE_CaseRiskCategoriesPivot')

# COMMAND ----------

df_LE_ApplicableRiskCategories = spark.sql("""
with CTE_ApplicableRiskCategories as
    (
        select distinct
			e.id as LegalEntityClientId
			, c.Name as CategoryName
            ,cast(ict.IsRiskMaterial as int) as CalculatedRiskApplId
		FROM RiskModel_dbo_Instance i
		JOIN RiskModel_dbo_InstanceCalculation ic 
			on ic.InstanceId = i.Id
		JOIN CaseService_case_DynamicRiskModelInstanceReference ir
            on ir.InstanceId = i.Id and ir.Expired is null
		JOIN CaseService_case_LegalEntityClient e 
			on e.Id=ir.LegalEntityClientId
		JOIN RiskModel_dbo_InstanceCategory ict  
			on ict.InstanceId = i.Id
		JOIN RiskModel_dbo_Category c 
			on c.Id=ict.CategoryId
		WHERE ic.Expired is null
    ),
    DynamicRiskApplicableData AS 
    (
    select LegalEntityClientId 
    , MAX(CASE WHEN CategoryName = 'Adverse Info' then CalculatedRiskApplId else NULL END) as AdverseInfoApplicableRisk
    , MAX(CASE WHEN CategoryName = 'Distribution Channel' then CalculatedRiskApplId else NULL END) as DistributionChannelApplicableRisk
    , MAX(CASE WHEN CategoryName = 'Entity Type' then CalculatedRiskApplId else NULL END) as EntityTypeApplicableRisk
    , MAX(CASE WHEN CategoryName = 'General' then CalculatedRiskApplId else NULL END) as GeneralApplicableRisk
    , MAX(CASE WHEN CategoryName = 'Geographical' then CalculatedRiskApplId else NULL END) as GeographicalApplicableRisk
    , MAX(CASE WHEN CategoryName = 'Other' then CalculatedRiskApplId else NULL END) as OtherApplicableRisk
    , MAX(CASE WHEN CategoryName = 'PEP' then CalculatedRiskApplId else NULL END) as PoliticallyExposedPersonsApplicableRisk
    , MAX(CASE WHEN CategoryName = 'Products and Services' then CalculatedRiskApplId else NULL END) as ProductsApplicableRisk
    , MAX(CASE WHEN CategoryName = 'Sector' then CalculatedRiskApplId else NULL END) as SectorApplicableRisk
    , MAX(CASE WHEN CategoryName = 'Structure' then CalculatedRiskApplId else NULL END) as StructureApplicableRisk
    , MAX(CASE WHEN CategoryName = 'Third Party' then CalculatedRiskApplId else NULL END) as ThirdPartyApplicableRisk
    , MAX(CASE WHEN CategoryName = 'Transaction' then CalculatedRiskApplId else NULL END) as TransactionApplicableRisk
    FROM
        CTE_ApplicableRiskCategories
    GROUP BY
        LegalEntityClientId
	)
    select distinct
    LegalEntityClientId
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
    From DynamicRiskApplicableData appl

""")

df_LE_ApplicableRiskCategories.createOrReplaceTempView('ApplicableRiskCategories')

# COMMAND ----------

# RiskmodelAnswers
sdf_LE_RiskModelAnswers = spark.sql(""" select
t1.LegalEntityClientId
, t1.GcobId
, t1.MaxInstanceId
, t4.`QuestionId`
, t5.Value AS AnswerValue
, t5.Text AS `AnswerText`
, t5.`Score` AS `AnswerRiskScore`
, t6.QuestionCode
-- ,t6.HasMaterialRiskQuestion?
from CaseToInstance t1
left join RiskModel_dbo_InstanceAnswer t4 on t1.MaxInstanceId = t4.InstanceId
left join RiskModel_dbo_PossibleAnswer t5 on t4.PossibleAnswerId = T5.Id 
left join RiskModel_dbo_Question t6 on t4.QuestionId = t6.Id
""")
sql_LE_CDDRisk = ""

# TODO: Select a specific amount of QuestionCodes might be better than only those with a dash in.
sdf_LE_RiskModelAnswers_sel = sdf_LE_RiskModelAnswers.filter(sdf_LE_RiskModelAnswers.QuestionCode.contains('-'))

hardcoded_unique_questionCodes = ['GEO-Q1', 'GEO-Q2', 'GEO-Q3', 'GEO-Q4'] # etc, not in use

# COMMAND ----------

from pyspark.sql.functions import array_join, collect_set
sdf_RiskAnswersPivot = sdf_LE_RiskModelAnswers_sel.groupby('LegalEntityClientId', 'MaxInstanceId').pivot('QuestionCode').agg(array_join(collect_set('AnswerText'), ','))

# COMMAND ----------

# Join this with sdf_LE_CaseRiskCategoriesPivot
sdf_CategoriesAndAnswersPivot = sdf_RiskAnswersPivot.join(sdf_LE_CaseRiskCategoriesPivot, on = 'LegalEntityClientId', how = 'left')

# COMMAND ----------

# Join this with sdf_LE_RiskApplicable
sdf_MainAndRiskApplicable = sdf_CategoriesAndAnswersPivot.join(df_LE_ApplicableRiskCategories, on = 'LegalEntityClientId', how = 'left')

# COMMAND ----------

# Material Risk
sdf_LE_RiskMaterial = spark.sql(""" select distinct
i.LegalEntityClientId
, iqm.InstanceId
, iqm.QuestionId
, q.QuestionCode
, iqm.IsMaterial
from CaseToInstance i
Inner join RiskModel_dbo_InstanceQuestionMateriality iqm on i.MaxInstanceId = iqm.InstanceId
left join RiskModel_dbo_Question q on iqm.QuestionId = q.Id
""")

# TODO: Select a specific amount of QuestionCodes might be better than only those with a dash in.
sdf_LE_RiskMaterial_sel = sdf_LE_RiskMaterial.filter(sdf_LE_RiskMaterial.QuestionCode.contains('-'))


# COMMAND ----------

sdf_LE_RiskMaterial_sel = sdf_LE_RiskMaterial_sel.withColumn('QuestionCodeNew',F.concat(F.col('QuestionCode'),F.lit("_MaterialRisk")))

# COMMAND ----------

sdf_RiskMaterialPivot = sdf_LE_RiskMaterial_sel.groupby('LegalEntityClientId', 'InstanceId').pivot('QuestionCodeNew').agg(F.array_join(F.collect_set('IsMaterial'), ','))

# COMMAND ----------

# Join this with sdf_RiskMaterialPivot
sdf_AllCddRisk = sdf_MainAndRiskApplicable.join(sdf_RiskMaterialPivot, on = 'LegalEntityClientId', how = 'left')

# COMMAND ----------

sdf_AllCddRisk.createOrReplaceTempView('AllCddRisk')

# COMMAND ----------

sql_LE_Ver = spark.sql("""
with ApprovedClientVersions(GcobId, LatestApprovedVersionNumber) AS (
    SELECT        cl.GcobId, MAX(cl.Version) AS LatestApprovedVersionNumber
    FROM            CaseService_case_LegalEntityClient AS cl 
    INNER JOIN CaseService_case_Case AS c ON c.LegalEntityClientId = cl.Id
    WHERE        (c.CurrentStatus in (8,9))
       GROUP BY cl.GcobId), ClientIdentifiers AS
    (SELECT        ci.LegalEntityId, st.Name, ci.ValueOfIdentifier
      FROM            CaseService_case_LegalEntityClientIdentifier AS ci INNER JOIN
                                CaseService_dbo_SystemIdType AS st ON st.Id = ci.SystemTypeReferenceId
                                
)
select * from ApprovedClientVersions
""")

sql_LE_Ver.createOrReplaceTempView('LE_Version')

# COMMAND ----------

# Risk Model Name
df_CddRislWithModel = spark.sql(""" select distinct
cc.CaseId
,c.gcobid
,c.FullLegalName
,rmRef.DefinitionName as RiskModelName
,rw.Description as ReviewTypeName
,cs.Name as CaseStatusName
,u1.DisplayName AS GlobalClientOwner
,e.name AS GlobalClientOwnerLocation
,lcs.Description as ClientLifeCycleName
, CASE 
        WHEN (v.LatestApprovedVersionNumber = c.Version AND cc.CurrentStatus = 8) 
		THEN 'True'
		WHEN (v.LatestApprovedVersionNumber = c.Version AND cc.CurrentStatus = 9)
        THEN 'True' ELSE 'False' END 
AS IsLatestApprovedVersionOfClient
,cdd.*
from AllCddRisk cdd
join CaseService_case_DynamicRiskModelInstanceReference rmRef ON rmRef.LegalEntityClientId = cdd.LegalEntityClientId and Expired IS NULL
join CaseService_case_LegalEntityClient c on cdd.LegalEntityClientId = c.id
join CaseService_case_case cc on c.Id = cc.LegalEntityClientId
LEFT OUTER JOIN gcob_static_ReviewType rw ON cc.CaseReviewType = rw.Id
LEFT OUTER JOIN gcob_static_CaseStatusType cs ON cc.CurrentStatus = cs.StatusId
LEFT OUTER JOIN gcob_static_ClientLifeCycleStatus lcs on c.ClientLifecycleStatusType = lcs.Id
LEFT OUTER JOIN CaseService_case_InvolvedStaffMember AS sm ON sm.InvolvedStaffMemberId = c.GlobalClientOwnerId 
LEFT OUTER JOIN CaseService_user_GcobUser AS us ON us.UserId = sm.UserReferenceId 
LEFT OUTER JOIN CaseService_dbo_RabobankEntity AS e ON e.Id = sm.RabobankEntityReferenceId
LEFT OUTER JOIN CaseService_user_GcobUser u1 on sm.UserReferenceId = u1.UserId
LEFT OUTER JOIN LE_Version v on c.GcobId = v.GcobId
""")

# COMMAND ----------

df_CddRislWithModel= df_CddRislWithModel.withColumn("BusinessDate", F.lit(BusinessDate))


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS WR_RADAR" )
#spark.sql("CREATE SCHEMA IF NOT EXISTS GCOB" )
#spark.sql("DROP TABLE IF EXISTS dbo.le_cddrisk" )
df_CddRislWithModel.write.mode("overwrite").saveAsTable("WR_RADAR.le_cddrisk")

# COMMAND ----------


