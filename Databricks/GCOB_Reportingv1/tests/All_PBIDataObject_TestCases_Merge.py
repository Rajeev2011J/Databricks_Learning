# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks Notebook
# MAGIC **Description**: This notebook performs data quality checks on the `hive_metastore.radar` catalog table.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **AllCases dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW micasestesting AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where ReviewTypeName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientType_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where ClientType is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt, GcobId,CaseId,FullLegalName,SourceClient,"Last4EyeCheckReviewer_Cnt" AS cnt_type  from hive_metastore.radar.mi_cases where Last4EyeCheckReviewer is NULL and CaseStatusName='Completed' 
# MAGIC and ReviewTypeName not in ('Change of Client Owner','Product Offboarding','Product Offboarding (Resume)')
# MAGIC and GcobId NOT LIKE 'RF%' and GcobId NOT LIKE 'NL%' group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"LastKYCAnalyst_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where LastKYCAnalyst is NULL and CaseStatusName='Completed'
# MAGIC and ReviewTypeName not in ('Product Offboarding','Product Offboarding (Resume)')
# MAGIC and GcobId NOT LIKE 'RF%' and GcobId NOT LIKE 'NL%' group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"NextReviewDate_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where NextReviewDate is NULL and CaseStatusName='Completed' group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ValidatedRiskLevel_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where ValidatedRiskLevel is NULL and CaseStatusName='Completed' group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientOwnerSignOffDate_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where ClientOwnerSignOffDate is NULL and CaseStatusName='Completed'
# MAGIC and GcobId NOT LIKE 'RF%' and GcobId NOT LIKE 'NL%'
# MAGIC and REVIEWTYPENAME not in ('Product Offboarding','Product Offboarding (Resume)','Amendment') 
# MAGIC group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseCompletedDate_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where CaseCompletedDate is NULL and CaseStatusName='Completed'
# MAGIC and ReviewTypeName not in ('Product Offboarding')
# MAGIC and GcobId NOT LIKE 'RF%' and GcobId NOT LIKE 'NL%'
# MAGIC group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseCreationDate_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where CaseCreationDate is NULL 
# MAGIC and GcobId NOT LIKE 'RF%' and GcobId NOT LIKE 'NL%'
# MAGIC group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName ,SourceClient,"GlobalClientOwner_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where GlobalClientOwner is NULL and CaseStatusName='Completed' group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"FIHubIndicator_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where FIHubIndicator is NULL and CaseStatusName='Completed' and ClientType='Legal Entity' and SourceSystem='GCOB'
# MAGIC group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CountryOfRegistration_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where CaseStatusName='Completed' and CountryOfRegistration is null group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"RiskModelName_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where CaseStatusName='Completed' and RiskModelName is null group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"EntityTypeRiskLevelLE_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where EntityTypeRiskLevel is NULL and CaseStatusName='Completed' and ClientType='Legal Entity'and RiskModelName not in ('Rabobank Subsidiaries Risk Model')
# MAGIC group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"EntityTypeRiskLevelNP_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where EntityTypeRiskLevel is NULL and CaseStatusName='Completed' and ClientType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')
# MAGIC and RiskModelName in ('Corporate Client Risk Model 2020 (Phase 2) (OnBoarding)',
# MAGIC 'Corporate Client Risk Model 2020 (Phase 2) (Review)',
# MAGIC 'NPPC Risk Model 2023 (Onboarding)',
# MAGIC 'NPPC Risk Model 2023 (Review)')
# MAGIC group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"StructureRiskLevelLE_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where StructureRiskLevel is NULL and CaseStatusName='Completed' and ClientType='Legal Entity' and RiskModelName not in ('Rabobank Subsidiaries Risk Model')
# MAGIC group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"StructureRiskLevelNP_Cnt" AS cnt_type from hive_metastore.radar.mi_cases where StructureRiskLevel is NULL and CaseStatusName='Completed' and ClientType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')
# MAGIC and RiskModelName in ('Corporate Client Risk Model 2020 (Phase 2) (OnBoarding)',
# MAGIC 'Corporate Client Risk Model 2020 (Phase 2) (Review)',
# MAGIC 'NPPC Risk Model 2023 (Onboarding)',
# MAGIC 'NPPC Risk Model 2023 (Review)')
# MAGIC group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW micasestesting_final AS
# MAGIC (SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS micases_Total_FullLegalName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS micases_Total_ReviewTypeName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientType_Cnt' THEN cnt ELSE 0 END) AS micases_Total_ClientType_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'Last4EyeCheckReviewer_Cnt' THEN cnt ELSE 0 END) AS micases_Total_Last4EyeCheckReviewer_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'LastKYCAnalyst_Cnt' THEN cnt ELSE 0 END) AS micases_Total_LastKYCAnalyst_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'NextReviewDate_Cnt' THEN cnt ELSE 0 END) AS micases_Total_NextReviewDate_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ValidatedRiskLevel_Cnt' THEN cnt ELSE 0 END) AS micases_Total_ValidatedRiskLevel_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientOwnerSignOffDate_Cnt' THEN cnt ELSE 0 END) AS micases_Total_ClientOwnerSignOffDate_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseCompletedDate_Cnt' THEN cnt ELSE 0 END) AS micases_Total_CaseCompletedDate_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseCreationDate_Cnt' THEN cnt ELSE 0 END) AS micases_Total_CaseCreationDate_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'GlobalClientOwner_Cnt' THEN cnt ELSE 0 END) AS micases_Total_GlobalClientOwner_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'FIHubIndicator_Cnt' THEN cnt ELSE 0 END) AS micases_Total_FIHubIndicator_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CountryOfRegistration_Cnt' THEN cnt ELSE 0 END) AS micases_Total_CountryOfRegistration_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'RiskModelName_Cnt' THEN cnt ELSE 0 END) AS micases_Total_RiskModelName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'EntityTypeRiskLevelLE_Cnt' THEN cnt ELSE 0 END) AS micases_Total_EntityTypeRiskLevelLE_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'EntityTypeRiskLevelNP_Cnt' THEN cnt ELSE 0 END) AS micases_Total_EntityTypeRiskLevelNP_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'StructureRiskLevelLE_Cnt' THEN cnt ELSE 0 END) AS micases_Total_StructureRiskLevelLE_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'StructureRiskLevelNP_Cnt' THEN cnt ELSE 0 END) AS micases_Total_StructureRiskLevelNP_Cnt,
# MAGIC     NULL AS micases_Duplicate_Gcobid_cnt
# MAGIC FROM
# MAGIC     micasestesting
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     NULL AS FullLegalName, 
# MAGIC     NULL AS SourceClient,
# MAGIC     NULL AS micases_Total_FullLegalName_Cnt, 
# MAGIC     NULL AS micases_Total_ReviewTypeName_Cnt, 
# MAGIC     NULL AS micases_Total_ClientType_Cnt, 
# MAGIC     NULL AS micases_Total_Last4EyeCheckReviewer_Cnt,
# MAGIC     NULL AS micases_Total_LastKYCAnalyst_Cnt,
# MAGIC     NULL AS micases_Total_NextReviewDate_Cnt,
# MAGIC     NULL AS micases_Total_ValidatedRiskLevel_Cnt,
# MAGIC     NULL AS micases_Total_ClientOwnerSignOffDate_Cnt,
# MAGIC     NULL AS micases_Total_CaseCompletedDate_Cnt,
# MAGIC     NULL AS micases_Total_CaseCreationDate_Cnt,
# MAGIC     NULL AS micases_Total_GlobalClientOwner_Cnt,
# MAGIC     NULL AS micases_Total_FIHubIndicator_Cnt,
# MAGIC     NULL AS micases_Total_CountryOfRegistration_Cnt,
# MAGIC     NULL AS micases_Total_RiskModelName_Cnt,
# MAGIC     NULL AS micases_Total_EntityTypeRiskLevelLE_Cnt,
# MAGIC     NULL AS micases_Total_EntityTypeRiskLevelNP_Cnt,
# MAGIC     NULL AS micases_Total_StructureRiskLevelLE_Cnt,
# MAGIC     NULL AS micases_Total_StructureRiskLevelNP_Cnt,
# MAGIC     count(GcobId) AS micases_Duplicate_Gcobid_cnt
# MAGIC FROM 
# MAGIC     hive_metastore.radar.mi_cases 
# MAGIC GROUP BY 
# MAGIC     gcobid, 
# MAGIC     CaseId, 
# MAGIC     ClientType 
# MAGIC HAVING 
# MAGIC     micases_Duplicate_Gcobid_cnt > 1
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **AllRelationships dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW allrelationships_testing AS
# MAGIC (select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where FullLegalName is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"ChildIdentity_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where ChildIdentity is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"ChildFullLegalName_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where ChildFullLegalName is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"ChildEntityType_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where ChildEntityType is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"CountryofRegistration_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where CountryofRegistration is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"RegisteredCountryofChild_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where RegisteredCountryofChild is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"ParentIdentity_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where ParentIdentity is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"ParentFullLegalName_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where ParentFullLegalName is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"ParentEntityType_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where ParentEntityType is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"DateOfBirthofParent_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where DateOfBirthofParent is NULL and ParentEntityType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person','Related Natural Person') group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"RegisteredCountryofParent_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where RegisteredCountryofParent is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"ClientLifeCycle_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where ClientLifeCycle is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient, "CaseReview_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where CaseReview is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"CaseStatusName_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where CaseStatusName is NULL group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"ClientType_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where ClientType is null group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"CitizenshipofParent_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where CitizenshipofParent is null and ParentEntityType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person','Related Natural Person') group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,ClientGcobid,CaseId,FullLegalName,SourceClient,"CitizenshipofChild_Cnt" AS cnt_type from hive_metastore.radar.allrelationships where CitizenshipofChild is null and ChildEntityType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person','Related Natural Person') group by ClientGcobid,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW allrelationships_testing_final AS
# MAGIC (SELECT 
# MAGIC     ClientGcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_FullLegalName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ChildIdentity_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_ChildIdentity_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ChildFullLegalName_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_ChildFullLegalName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ChildEntityType_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_ChildEntityType_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CountryofRegistration_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_CountryofRegistration_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'RegisteredCountryofChild_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_RegisteredCountryofChild_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ParentIdentity_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_ParentIdentity_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ParentFullLegalName_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_ParentFullLegalName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ParentEntityType_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_ParentEntityType_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'DateOfBirthofParent_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_DateOfBirthofParent_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'RegisteredCountryofParent_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_RegisteredCountryofParent_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycle_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_ClientLifeCycle_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseReview_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_CaseReview_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatusName_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_CaseStatusName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientType_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_ClientType_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CitizenshipofParent_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_CitizenshipofParent_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CitizenshipofChild_Cnt' THEN cnt ELSE 0 END) AS Allrelationships_Total_CitizenshipofChild_Cnt
# MAGIC FROM
# MAGIC     allrelationships_testing
# MAGIC GROUP BY 
# MAGIC     ClientGcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **Businessactivities dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW businessactivities_testing AS
# MAGIC (select count(*) as cnt,Gcobid,CaseId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type  from hive_metastore.radar.businessactivities where FullLegalName is NULL group by Gcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,Gcobid,CaseId,FullLegalName,SourceClient,"NAICSCode_Cnt" AS cnt_type from hive_metastore.radar.businessactivities where NAICSCode is NULL group by Gcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,Gcobid,CaseId,FullLegalName,SourceClient,"ClientLifeCycleName_Cnt" AS cnt_type from hive_metastore.radar.businessactivities where ClientLifeCycleName is NULL group by Gcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,Gcobid,CaseId,FullLegalName,SourceClient,"ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.businessactivities where ReviewTypeName is NULL group by Gcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,Gcobid,CaseId,FullLegalName,SourceClient,"CaseStatusName_Cnt" AS cnt_type from hive_metastore.radar.businessactivities where CaseStatusName is NULL group by Gcobid,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,Gcobid,CaseId,FullLegalName,SourceClient,"FIHubIndicato_Cntr" AS cnt_type from hive_metastore.radar.businessactivities where FIHubIndicator is NULL and CaseStatusName='Completed' and ClientType='Legal Entity' and SourceSystem='GCOB' group by Gcobid,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW businessactivities_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS Businessactivities_Total_FullLegalName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'NAICSCode_Cnt' THEN cnt ELSE 0 END) AS Businessactivities_Total_NAICSCode_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycleName_Cnt' THEN cnt ELSE 0 END) AS Businessactivities_Total_ClientLifeCycleName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS Businessactivities_Total_ReviewTypeName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatusName_Cnt' THEN cnt ELSE 0 END) AS Businessactivities_Total_CaseStatusName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'FIHubIndicato_Cnt' THEN cnt ELSE 0 END) AS Businessactivities_Total_FIHubIndicato_Cnt,
# MAGIC     NULL AS Businessactivities_Duplicate_Gcobid_cnt
# MAGIC FROM
# MAGIC     businessactivities_testing
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     NULL AS FullLegalName, 
# MAGIC     NULL AS SourceClient,
# MAGIC     NULL AS Businessactivities_Total_FullLegalName_Cnt, 
# MAGIC     NULL AS Businessactivities_Total_NAICSCode_Cnt,
# MAGIC     NULL AS Businessactivities_Total_ClientLifeCycleName_Cnt, 
# MAGIC     NULL AS Businessactivities_Total_ReviewTypeName_Cnt, 
# MAGIC     NULL AS Businessactivities_Total_CaseStatusName_Cnt,
# MAGIC     NULL AS Businessactivities_Total_FIHubIndicato_Cnt,
# MAGIC     count(GcobId) AS Businessactivities_Duplicate_Gcobid_cnt
# MAGIC     
# MAGIC FROM 
# MAGIC     hive_metastore.radar.businessactivities
# MAGIC GROUP BY 
# MAGIC     gcobid, 
# MAGIC     CaseId, 
# MAGIC     ClientType,
# MAGIC     NAICSCode
# MAGIC HAVING 
# MAGIC     Businessactivities_Duplicate_Gcobid_cnt > 1
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **cddriskdetails dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cddrisktesting AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.cddriskdetails where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.cddriskdetails where ReviewTypeName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientType_Cnt" AS cnt_type from hive_metastore.radar.cddriskdetails where ClientType is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"RiskModelName" AS cnt_type from hive_metastore.radar.cddriskdetails  where CaseStatusName='Completed' and RiskModelName is null group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"GlobalClientOwner_Cnt" AS cnt_type from hive_metastore.radar.cddriskdetails where GlobalClientOwner is NULL and CaseStatusName='Completed' group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,Gcobid,CaseId,FullLegalName,SourceClient,"ClientLifeCycle_Cnt" AS cnt_type from hive_metastore.radar.cddriskdetails where ClientLifeCycleName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,Gcobid,CaseId,FullLegalName,SourceClient,"EntityTypeRiskLevelLE_Cnt" AS cnt_type from hive_metastore.radar.cddriskdetails where EntityTypeRiskLevel is NULL and CaseStatusName='Completed' and ClientType='Legal Entity'and RiskModelName not in ('Rabobank Subsidiaries Risk Model') group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"EntityTypeRiskLevelNP_Cnt" AS cnt_type from hive_metastore.radar.cddriskdetails where EntityTypeRiskLevel is NULL and CaseStatusName='Completed' and ClientType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')
# MAGIC and RiskModelName in ('Corporate Client Risk Model 2020 (Phase 2) (OnBoarding)',
# MAGIC 'Corporate Client Risk Model 2020 (Phase 2) (Review)',
# MAGIC 'NPPC Risk Model 2023 (Onboarding)',
# MAGIC 'NPPC Risk Model 2023 (Review)')
# MAGIC group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"StructureRiskLevelLE_Cnt" AS cnt_type from hive_metastore.radar.cddriskdetails where StructureRiskLevel is NULL and CaseStatusName='Completed' and ClientType='Legal Entity' and RiskModelName not in ('Rabobank Subsidiaries Risk Model')
# MAGIC group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"StructureRiskLevelNP_Cnt" AS cnt_type from hive_metastore.radar.cddriskdetails where StructureRiskLevel is NULL and CaseStatusName='Completed' and ClientType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')
# MAGIC and RiskModelName in ('Corporate Client Risk Model 2020 (Phase 2) (OnBoarding)',
# MAGIC 'Corporate Client Risk Model 2020 (Phase 2) (Review)',
# MAGIC 'NPPC Risk Model 2023 (Onboarding)',
# MAGIC 'NPPC Risk Model 2023 (Review)')
# MAGIC group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cddrisktesting_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS Cddrisk_Total_FullLegalName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS Cddrisk_Total_ReviewTypeName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientType_Cnt' THEN cnt ELSE 0 END) AS Cddrisk_Total_ClientType_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'RiskModelName_Cnt' THEN cnt ELSE 0 END) AS Cddrisk_Total_RiskModelName_Cnt,   
# MAGIC     SUM(CASE WHEN cnt_type = 'GlobalClientOwner_Cnt' THEN cnt ELSE 0 END) AS Cddrisk_Total_GlobalClientOwner_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycle_Cnt' THEN cnt ELSE 0 END) AS Cddrisk_Total_ClientLifeCycle_Cnt,  
# MAGIC     SUM(CASE WHEN cnt_type = 'EntityTypeRiskLevelLE_Cnt' THEN cnt ELSE 0 END) AS Cddrisk_Total_EntityTypeRiskLevelLE_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'EntityTypeRiskLevelNP_Cnt' THEN cnt ELSE 0 END) AS Cddrisk_Total_EntityTypeRiskLevelNP_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'StructureRiskLevelLE_Cnt' THEN cnt ELSE 0 END) AS Cddrisk_Total_StructureRiskLevelLE_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'StructureRiskLevelNP_Cnt' THEN cnt ELSE 0 END) AS Cddrisk_Total_StructureRiskLevelNP_Cnt,
# MAGIC     NULL AS Cddrisk_Duplicate_Gcobid_cnt
# MAGIC FROM
# MAGIC     cddrisktesting
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     NULL AS FullLegalName, 
# MAGIC     NULL AS SourceClient,
# MAGIC     NULL AS Cddrisk_Total_FullLegalName_Cnt, 
# MAGIC     NULL AS Cddrisk_Total_ReviewTypeName_Cnt, 
# MAGIC     NULL AS Cddrisk_Total_ClientType_Cnt, 
# MAGIC     NULL AS Cddrisk_Total_RiskModelName_Cnt,
# MAGIC     NULL AS Cddrisk_Total_GlobalClientOwner_Cnt,
# MAGIC     NULL AS Cddrisk_Total_ClientLifeCycle_Cnt,
# MAGIC     NULL AS Cddrisk_Total_EntityTypeRiskLevelLE_Cnt,
# MAGIC     NULL AS Cddrisk_Total_EntityTypeRiskLevelNP_Cnt,
# MAGIC     NULL AS Cddrisk_Total_StructureRiskLevelLE_Cnt,
# MAGIC     NULL AS Cddrisk_Total_StructureRiskLevelNP_Cnt,
# MAGIC     count(GcobId) AS Cddrisk_Duplicate_Gcobid_cnt
# MAGIC FROM 
# MAGIC     hive_metastore.radar.cddriskdetails 
# MAGIC GROUP BY gcobid, CaseId, ClientType, InstanceId, MaterialInstanceId
# MAGIC HAVING 
# MAGIC     Cddrisk_Duplicate_Gcobid_cnt > 1
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **ClientOwnership dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW clientownership_testing AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.clientownership where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CountryOfRegistration_Cnt" AS cnt_type from hive_metastore.radar.clientownership where CountryOfRegistration is null and CaseStatusName='Completed' group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,Gcobid,CaseId,FullLegalName,SourceClient,"ClientLifeCycle_Cnt" AS cnt_type from hive_metastore.radar.clientownership where ClientLifeCycleName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.clientownership where ReviewTypeName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseStatusName_Cnt" AS cnt_type from hive_metastore.radar.clientownership where CaseStatusName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW clientownership_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS ClientOwnership_Total_FullLegalName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'CountryOfRegistration_Cnt' THEN cnt ELSE 0 END) AS ClientOwnership_Total_CountryOfRegistration_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycle_Cnt' THEN cnt ELSE 0 END) AS ClientOwnership_Total_ClientLifeCycle_Cnt,  
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS ClientOwnership_Total_ReviewTypeName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatusName_Cnt' THEN cnt ELSE 0 END) AS ClientOwnership_Total_CaseStatusName_Cnt,
# MAGIC     NULL AS ClientOwnership_Duplicate_Gcobid_cnt
# MAGIC FROM
# MAGIC     clientownership_testing
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     NULL AS FullLegalName, 
# MAGIC     NULL AS SourceClient,
# MAGIC     NULL AS ClientOwnership_Total_FullLegalName_Cnt, 
# MAGIC     NULL AS ClientOwnership_Total_CountryOfRegistration_Cnt,
# MAGIC     NULL AS ClientOwnership_Total_ClientLifeCycle_Cnt, 
# MAGIC     NULL AS ClientOwnership_Total_ReviewTypeName_Cnt, 
# MAGIC     NULL AS ClientOwnership_Total_CaseStatusName_Cnt,
# MAGIC     count(GcobId) AS ClientOwnership_Duplicate_Gcobid_cnt 
# MAGIC FROM 
# MAGIC     hive_metastore.radar.clientownership 
# MAGIC GROUP BY gcobid, CaseId, SourceClient, OwnerName,OwnerLocation,OwnerType
# MAGIC HAVING 
# MAGIC     ClientOwnership_Duplicate_Gcobid_cnt > 1
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **ClientTradeName dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tradeName_testing AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type  from hive_metastore.radar.mi_clienttradename where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.mi_clienttradename where ReviewTypeName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,Gcobid,CaseId,FullLegalName,SourceClient,"ClientLifeCycle_Cnt" AS cnt_type from hive_metastore.radar.mi_clienttradename where ClientLifeCycleName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseStatusName_Cnt" AS cnt_type from hive_metastore.radar.mi_clienttradename where CaseStatusName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tradeName_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS ClientTradeName_Total_FullLegalName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS ClientTradeName_Total_ReviewTypeName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycle_Cnt' THEN cnt ELSE 0 END) AS ClientTradeName_Total_ClientLifeCycle_Cnt,  
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatusName_Cnt' THEN cnt ELSE 0 END) AS ClientTradeName_Total_CaseStatusName_Cnt,
# MAGIC     NULL AS ClientTradeName_Duplicate_Gcobid_cnt
# MAGIC FROM
# MAGIC     tradeName_testing
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     NULL AS FullLegalName, 
# MAGIC     NULL AS SourceClient,
# MAGIC     NULL AS ClientTradeName_Total_FullLegalName_Cnt, 
# MAGIC     NULL AS ClientTradeName_Total_ReviewTypeName_Cnt,
# MAGIC     NULL AS ClientTradeName_Total_ClientLifeCycle_Cnt,  
# MAGIC     NULL AS ClientTradeName_Total_CaseStatusName_Cnt,
# MAGIC     count(GcobId) AS ClientTradeName_Duplicate_Gcobid_cnt 
# MAGIC FROM 
# MAGIC     hive_metastore.radar.mi_clienttradename 
# MAGIC GROUP BY gcobid, CaseId ,clienttype
# MAGIC HAVING 
# MAGIC     ClientTradeName_Duplicate_Gcobid_cnt > 1
# MAGIC ) 

# COMMAND ----------

# MAGIC %md
# MAGIC **ControlMeasure dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW controlmeasure_testing AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,ClientFullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.mi_control_measure where ClientFullLegalName is NULL group by GcobId,CaseId,ClientFullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,ClientFullLegalName,SourceClient,"FIHubIndicator_Cnt" AS cnt_type from hive_metastore.radar.mi_control_measure where FIHubIndicator is NULL and CaseStatus='Completed' and ClientType='Legal Entity' and SourceSystem='GCOB' group by GcobId,CaseId,ClientFullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,ClientFullLegalName,SourceClient,"CaseReview_Cnt" AS cnt_type from hive_metastore.radar.mi_control_measure where CaseReview is NULL group by GcobId,CaseId,ClientFullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,ClientFullLegalName,SourceClient,"CaseStatus_Cnt" AS cnt_type from hive_metastore.radar.mi_control_measure where CaseStatus is NULL group by GcobId,CaseId,ClientFullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,Gcobid,CaseId,ClientFullLegalName,SourceClient,"ClientLifeCycle_Cnt" AS cnt_type from hive_metastore.radar.mi_control_measure where ClientLifeCycle is NULL group by GcobId,CaseId,ClientFullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,Gcobid,CaseId,ClientFullLegalName,SourceClient,"ControlMeasureId_Cnt" AS cnt_type from hive_metastore.radar.mi_control_measure where ControlMeasureId is NULL group by GcobId,CaseId,ClientFullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW controlmeasure_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     ClientFullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS ControlMeasure_Total_FullLegalName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'FIHubIndicator_Cnt' THEN cnt ELSE 0 END) AS ControlMeasure_Total_FIHubIndicator_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseReview_Cnt' THEN cnt ELSE 0 END) AS ControlMeasure_Total_CaseReview_Cnt,  
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatus_Cnt' THEN cnt ELSE 0 END) AS ControlMeasure_Total_CaseStatus_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycle_Cnt' THEN cnt ELSE 0 END) AS ControlMeasure_Total_ClientLifeCycle_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ControlMeasureId_Cnt' THEN cnt ELSE 0 END) AS ControlMeasure_Total_ControlMeasureId_Cnt
# MAGIC FROM
# MAGIC     controlmeasure_testing
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     ClientFullLegalName,
# MAGIC     SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **Documents dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW documents_testing AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.documentdetails where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"GlobalClientOwner_Cnt" AS cnt_type from hive_metastore.radar.documentdetails where GlobalClientOwner is NULL and CaseStatusName='Completed' group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"NextReviewDate_Cnt" AS cnt_type from hive_metastore.radar.documentdetails where NextReviewDate is NULL and CaseStatusName='Completed' group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.documentdetails where ReviewTypeName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseStatusName_Cnt" AS cnt_type from hive_metastore.radar.documentdetails where CaseStatusName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientLifeCycleName_Cnt" AS cnt_type from hive_metastore.radar.documentdetails where ClientLifeCycleName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW documents_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS Documents_Total_FullLegalName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'GlobalClientOwner_Cnt' THEN cnt ELSE 0 END) AS Documents_Total_GlobalClientOwner_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'NextReviewDate_Cnt' THEN cnt ELSE 0 END) AS Documents_Total_NextReviewDate_Cnt,  
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS Documents_Total_ReviewTypeName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatusName_Cnt' THEN cnt ELSE 0 END) AS Documents_Total_CaseStatusName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycleName_Cnt' THEN cnt ELSE 0 END) AS Documents_Total_ClientLifeCycleName_Cnt
# MAGIC FROM
# MAGIC     documents_testing
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient)

# COMMAND ----------

# MAGIC %md
# MAGIC **fatcacrsdetails dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fatcacrsdetails_testing AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.fatcacrsdetails where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientLifeCycleName_Cnt" AS cnt_type from hive_metastore.radar.fatcacrsdetails where ClientLifeCycleName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.fatcacrsdetails where ReviewTypeName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseStatusName_Cnt" AS cnt_type from hive_metastore.radar.fatcacrsdetails where CaseStatusName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CountryOfRegistration_Cnt" AS cnt_type from hive_metastore.radar.fatcacrsdetails where CaseStatusName='Completed' and CountryOfRegistration is null group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fatcacrsdetails_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS Fatcacrsdetails_Total_FullLegalName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycleName_Cnt' THEN cnt ELSE 0 END) AS Fatcacrsdetails_Total_ClientLifeCycleName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS Fatcacrsdetails_Total_ReviewTypeName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatusName_Cnt' THEN cnt ELSE 0 END) AS Fatcacrsdetails_Total_CaseStatusName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CountryOfRegistration_Cnt' THEN cnt ELSE 0 END) AS Fatcacrsdetails_Total_CountryOfRegistration_Cnt,
# MAGIC     NULL AS Fatcacrsdetails_Duplicate_Gcobid_cnt
# MAGIC FROM
# MAGIC     fatcacrsdetails_testing
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     NULL AS FullLegalName, 
# MAGIC     NULL AS SourceClient,
# MAGIC     NULL AS Fatcacrsdetails_Total_FullLegalName_Cnt, 
# MAGIC     NULL AS Fatcacrsdetails_Total_ClientLifeCycleName_Cnt, 
# MAGIC     NULL AS Fatcacrsdetails_Total_ReviewTypeName_Cnt,
# MAGIC     NULL AS Fatcacrsdetails_Total_CaseStatusName_Cnt,  
# MAGIC     NULL AS Fatcacrsdetails_Total_CountryOfRegistration_Cnt,
# MAGIC     count(GcobId) AS Fatcacrsdetails_Duplicate_Gcobid_cnt 
# MAGIC FROM 
# MAGIC     hive_metastore.radar.fatcacrsdetails 
# MAGIC GROUP BY  gcobid, CaseId, SourceClient, SystemIdType, SystemIdValue, TinOrEquivalent, CountryOfTaxResidencies
# MAGIC HAVING 
# MAGIC     Fatcacrsdetails_Duplicate_Gcobid_cnt > 1
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **FIClients dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fiClient_testing AS
# MAGIC (select count(*) as cnt,GcobId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.mi_fi_hubindicator where FullLegalName is NULL group by GcobId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,FullLegalName,SourceClient,"ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.mi_fi_hubindicator where ReviewTypeName is NULL group by GcobId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,FullLegalName,SourceClient,"ClientLifeCycle_Cnt" AS cnt_type from hive_metastore.radar.mi_fi_hubindicator where ClientLifeCycle  is NULL group by GcobId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,FullLegalName,SourceClient,"CaseStatusType_Cnt" AS cnt_type from hive_metastore.radar.mi_fi_hubindicator where CaseStatusType  is NULL group by GcobId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fiClient_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId,        
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS FIClients_Total_FullLegalName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS FIClients_Total_ReviewTypeName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycle_Cnt' THEN cnt ELSE 0 END) AS FIClients_Total_ClientLifeCycle_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatusType_Cnt' THEN cnt ELSE 0 END) AS FIClients_Total_CaseStatusType_Cnt
# MAGIC     
# MAGIC FROM 
# MAGIC     fiClient_testing
# MAGIC GROUP BY 
# MAGIC     GcobId,
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **LocalRequirements dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW localrequirement_testing AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.mi_localrequirement where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.mi_localrequirement where ReviewTypeName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientLifeCycleName_Cnt" AS cnt_type from hive_metastore.radar.mi_localrequirement where ClientLifeCycleName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseStatusName_Cnt" AS cnt_type from hive_metastore.radar.mi_localrequirement where CaseStatusName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"LocalRequirementCountry_Cnt" AS cnt_type from hive_metastore.radar.mi_localrequirement where LocalRequirementCountry is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CountryOfRegistration_Cnt" AS cnt_type from hive_metastore.radar.mi_localrequirement where  where CaseStatusName='Completed' and CountryofRegistration is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW localrequirement_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId,   
# MAGIC     CaseId,     
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS LocalRequirements_Total_FullLegalName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS LocalRequirements_Total_ReviewTypeName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycleName_Cnt' THEN cnt ELSE 0 END) AS LocalRequirements_Total_ClientLifeCycleName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatusName_Cnt' THEN cnt ELSE 0 END) AS LocalRequirements_Total_CaseStatusName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'LocalRequirementCountry_Cnt' THEN cnt ELSE 0 END) AS LocalRequirements_Total_LocalRequirementCountry_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CountryOfRegistration_Cnt' THEN cnt ELSE 0 END) AS LocalRequirements_Total_CountryOfRegistration_Cnt
# MAGIC
# MAGIC FROM 
# MAGIC     localrequirement_testing
# MAGIC GROUP BY 
# MAGIC     GcobId,
# MAGIC     CaseId,
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **NameScreeningReport dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW namescreening_testing AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.mi_name_screening where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.mi_name_screening where ReviewTypeName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "ClientLifeCyclestatusType_Cnt" AS cnt_type from hive_metastore.radar.mi_name_screening where ClientLifeCyclestatusType is NULL and ClientType not in ('RelatedLegalEntity') group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseStatusName_Cnt" AS cnt_type from hive_metastore.radar.mi_name_screening where CaseStatusName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"RegisteredCountry_Cnt" AS cnt_type from hive_metastore.radar.mi_name_screening where RegisteredCountry is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW namescreening_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS NameScreening_Total_FullLegalName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS NameScreening_Total_ReviewTypeName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCyclestatusType_Cnt' THEN cnt ELSE 0 END) AS NameScreening_Total_ClientLifeCyclestatusType_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatusName_Cnt' THEN cnt ELSE 0 END) AS NameScreening_Total_CaseStatusName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'RegisteredCountry_Cnt' THEN cnt ELSE 0 END) AS NameScreening_Total_RegisteredCountry_Cnt,
# MAGIC     NULL AS NameScreening_Duplicate_Gcobid_cnt
# MAGIC FROM  
# MAGIC     namescreening_testing
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     NULL AS FullLegalName, 
# MAGIC     NULL AS SourceClient,
# MAGIC     NULL AS NameScreening_Total_FullLegalName_Cnt, 
# MAGIC     NULL AS NameScreening_Total_ReviewTypeName_Cnt,
# MAGIC     NULL AS NameScreening_Total_ClientLifeCyclestatusType_Cnt,
# MAGIC     NULL AS NameScreening_Total_CaseStatusName_Cnt,  
# MAGIC     NULL AS NameScreening_Total_RegisteredCountry_Cnt,
# MAGIC     count(GcobId) AS NameScreening_Duplicate_Gcobid_cnt 
# MAGIC FROM 
# MAGIC     hive_metastore.radar.mi_name_screening 
# MAGIC GROUP BY gcobid, CaseId ,ClientCitizenship ,ClientNationality 
# MAGIC HAVING 
# MAGIC     NameScreening_Duplicate_Gcobid_cnt > 1
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **Product and Service dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW productsandservice_testing AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.produsctandservice where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientLifeCycleName_Cnt" AS cnt_type from hive_metastore.radar.produsctandservice where ClientLifeCycleName is NULL  group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.produsctandservice where ReviewTypeName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientType_Cnt" AS cnt_type from hive_metastore.radar.produsctandservice where ClientType is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"FIHubIndicator_Cnt" AS cnt_type from hive_metastore.radar.produsctandservice where FIHubIndicator is NULL and CaseStatusName='Completed' and ClientType='Legal Entity' and SourceSystem='GCOB' group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW productsandservice_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS Productsandservice_Total_FullLegalName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycleName_Cnt' THEN cnt ELSE 0 END) AS Productsandservice_Total_ClientLifeCycleName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS Productsandservice_Total_ReviewTypeName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientType_Cnt' THEN cnt ELSE 0 END) AS Productsandservice_Total_ClientType_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'FIHubIndicator_Cnt' THEN cnt ELSE 0 END) AS Productsandservice_Total_FIHubIndicator_Cnt
# MAGIC FROM  
# MAGIC     productsandservice_testing
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **Sructure Questionnaire dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW structurequestionnaire_testing AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.structure_questionnaire where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseStatus_Cnt" AS cnt_type from hive_metastore.radar.structure_questionnaire where CaseStatus is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "ReviewType_Cnt" AS cnt_type from hive_metastore.radar.structure_questionnaire where ReviewType is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientLifeCycleStatus_Cnt" AS cnt_type from hive_metastore.radar.structure_questionnaire where ClientLifeCycleStatus is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientType_Cnt" AS cnt_type from hive_metastore.radar.structure_questionnaire where ClientType is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW structurequestionnaire_testing_final AS
# MAGIC (
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS structurequestionnaire_Total_FullLegalName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatus_Cnt' THEN cnt ELSE 0 END) AS structurequestionnaire_Total_CaseStatus_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewType_Cnt' THEN cnt ELSE 0 END) AS structurequestionnaire_Total_ReviewType_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycleStatus_Cnt' THEN cnt ELSE 0 END) AS structurequestionnaire_Total_ClientLifeCycleStatus_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientType_Cnt' THEN cnt ELSE 0 END) AS structurequestionnaire_Total_ClientType_Cnt,
# MAGIC     NULL AS structurequestionnaire_Duplicate_Gcobid_cnt
# MAGIC FROM  
# MAGIC     structurequestionnaire_testing
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     NULL AS FullLegalName, 
# MAGIC     NULL AS SourceClient,
# MAGIC     NULL AS structurequestionnaire_Total_FullLegalName_Cnt, 
# MAGIC     NULL AS structurequestionnaire_Total_CaseStatus_Cnt,
# MAGIC     NULL AS structurequestionnaire_Total_ReviewType_Cnt,
# MAGIC     NULL AS structurequestionnaire_Total_ClientLifeCycleStatus_Cnt,  
# MAGIC     NULL AS structurequestionnaire_Total_ClientType_Cnt,
# MAGIC     count(GcobId) AS structurequestionnaire_Duplicate_Gcobid_cnt 
# MAGIC FROM 
# MAGIC     hive_metastore.radar.structure_questionnaire 
# MAGIC GROUP BY gcobid, CaseId, ClientType
# MAGIC HAVING 
# MAGIC     structurequestionnaire_Duplicate_Gcobid_cnt > 1
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **SystemIdentifier dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW systemidentifier_testing AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.systemidentifier where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.systemidentifier where ReviewTypeName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientLifeCycleName_Cnt" AS cnt_type from hive_metastore.radar.systemidentifier where ClientLifeCycleName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseStatusName_Cnt" AS cnt_type from hive_metastore.radar.systemidentifier where CaseStatusName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW systemidentifier_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS SystemIdentifier_Total_FullLegalName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS SystemIdentifier_Total_ReviewTypeName_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycleName_Cnt' THEN cnt ELSE 0 END) AS SystemIdentifier_Total_ClientLifeCycleName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatusName_Cnt' THEN cnt ELSE 0 END) AS SystemIdentifier_Total_CaseStatusName_Cnt,
# MAGIC     NULL AS SystemIdentifier_Duplicate_Gcobid_cnt
# MAGIC FROM  
# MAGIC     systemidentifier_testing
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     NULL AS FullLegalName, 
# MAGIC     NULL AS SourceClient,
# MAGIC     NULL AS SystemIdentifier_Total_FullLegalName_Cnt, 
# MAGIC     NULL AS SystemIdentifier_Total_ReviewTypeName_Cnt,
# MAGIC     NULL AS SystemIdentifier_Total_ClientLifeCycleName_Cnt,
# MAGIC     NULL AS SystemIdentifier_Total_CaseStatusName_Cnt, 
# MAGIC     count(GcobId) AS SystemIdentifier_Duplicate_Gcobid_cnt 
# MAGIC FROM 
# MAGIC     hive_metastore.radar.systemidentifier 
# MAGIC GROUP BY gcobid,CaseId,SystemIdType,SystemIdValue,FullLegalName
# MAGIC HAVING 
# MAGIC     SystemIdentifier_Duplicate_Gcobid_cnt > 1
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **UBOPEP dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ubopep_testing AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.relatednaturalpersonparty_ubo_pep_report where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseReviewType_Cnt" AS cnt_type from hive_metastore.radar.relatednaturalpersonparty_ubo_pep_report where CaseReviewType is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientLifecycleStatusType_Cnt" AS cnt_type from hive_metastore.radar.relatednaturalpersonparty_ubo_pep_report where ClientLifecycleStatusType is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseStatusType_Cnt" AS cnt_type from hive_metastore.radar.relatednaturalpersonparty_ubo_pep_report where CaseStatusType is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CountryOfRegistration_Cnt" AS cnt_type from hive_metastore.radar.relatednaturalpersonparty_ubo_pep_report where CaseStatusType='Completed' and CountryOfRegistration is null group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"PartyGCOBID_Cnt" AS cnt_type from hive_metastore.radar.relatednaturalpersonparty_ubo_pep_report where PartyGCOBID is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ubopep_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS ubopep_Total_FullLegalName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseReviewType_Cnt' THEN cnt ELSE 0 END) AS ubopep_Total_CaseReviewType_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifecycleStatusType_Cnt' THEN cnt ELSE 0 END) AS ubopep_Total_ClientLifecycleStatusType_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatusType_Cnt' THEN cnt ELSE 0 END) AS ubopep_Total_CaseStatusType_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'CountryOfRegistration_Cnt' THEN cnt ELSE 0 END) AS ubopep_Total_CountryOfRegistration_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'PartyGCOBID_Cnt' THEN cnt ELSE 0 END) AS ubopep_Total_PartyGCOBID_Cnt
# MAGIC FROM  
# MAGIC     ubopep_testing
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **Workitem dataobject test cases**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW workitem_testing AS
# MAGIC (select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"FullLegalName_Cnt" AS cnt_type from hive_metastore.radar.workitemdetails where FullLegalName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient, "ReviewTypeName_Cnt" AS cnt_type from hive_metastore.radar.workitemdetails where ReviewTypeName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"ClientLifeCycleName_Cnt" AS cnt_type from hive_metastore.radar.workitemdetails where ClientLifeCycleName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC UNION ALL
# MAGIC select count(*) as cnt,GcobId,CaseId,FullLegalName,SourceClient,"CaseStatusName_Cnt" AS cnt_type from hive_metastore.radar.workitemdetails where CaseStatusName is NULL group by GcobId,CaseId,FullLegalName,SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW workitem_testing_final AS(
# MAGIC     SELECT 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName, 
# MAGIC     SourceClient,
# MAGIC     SUM(CASE WHEN cnt_type = 'FullLegalName_Cnt' THEN cnt ELSE 0 END) AS Workitem_Total_FullLegalName_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ReviewTypeName_Cnt' THEN cnt ELSE 0 END) AS Workitem_Total_CaseReviewType_Cnt,
# MAGIC     SUM(CASE WHEN cnt_type = 'ClientLifeCycleName_Cnt' THEN cnt ELSE 0 END) AS Workitem_Total_ClientLifecycleStatusType_Cnt, 
# MAGIC     SUM(CASE WHEN cnt_type = 'CaseStatusName_Cnt' THEN cnt ELSE 0 END) AS Workitem_Total_CountryOfRegistration_Cnt
# MAGIC FROM  
# MAGIC     workitem_testing
# MAGIC GROUP BY 
# MAGIC     GcobId, 
# MAGIC     CaseId, 
# MAGIC     FullLegalName,
# MAGIC     SourceClient
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW final_dq_check_table AS 
# MAGIC (
# MAGIC select 
# MAGIC t1.GcobId, 
# MAGIC t1.CaseId, 
# MAGIC t1.FullLegalName, 
# MAGIC t1.SourceClient,
# MAGIC t1.micases_Total_FullLegalName_Cnt, 
# MAGIC t1.micases_Total_ReviewTypeName_Cnt, 
# MAGIC t1.micases_Total_ClientType_Cnt, 
# MAGIC t1.micases_Total_Last4EyeCheckReviewer_Cnt,
# MAGIC t1.micases_Total_LastKYCAnalyst_Cnt,
# MAGIC t1.micases_Total_NextReviewDate_Cnt,
# MAGIC t1.micases_Total_ValidatedRiskLevel_Cnt,
# MAGIC t1.micases_Total_ClientOwnerSignOffDate_Cnt,
# MAGIC t1.micases_Total_CaseCompletedDate_Cnt,
# MAGIC t1.micases_Total_CaseCreationDate_Cnt,
# MAGIC t1.micases_Total_GlobalClientOwner_Cnt,
# MAGIC t1.micases_Total_FIHubIndicator_Cnt,
# MAGIC t1.micases_Total_CountryOfRegistration_Cnt,
# MAGIC t1.micases_Total_RiskModelName_Cnt,
# MAGIC t1.micases_Total_EntityTypeRiskLevelLE_Cnt,
# MAGIC t1.micases_Total_EntityTypeRiskLevelNP_Cnt,
# MAGIC t1.micases_Total_StructureRiskLevelLE_Cnt,
# MAGIC t1.micases_Total_StructureRiskLevelNP_Cnt,
# MAGIC t1.micases_Duplicate_Gcobid_cnt,
# MAGIC
# MAGIC t2.Allrelationships_Total_FullLegalName_Cnt,
# MAGIC t2.Allrelationships_Total_ChildIdentity_Cnt,
# MAGIC t2.Allrelationships_Total_ChildFullLegalName_Cnt,
# MAGIC t2.Allrelationships_Total_ChildEntityType_Cnt,
# MAGIC t2.Allrelationships_Total_CountryofRegistration_Cnt,
# MAGIC t2.Allrelationships_Total_RegisteredCountryofChild_Cnt,
# MAGIC t2.Allrelationships_Total_ParentIdentity_Cnt,
# MAGIC t2.Allrelationships_Total_ParentFullLegalName_Cnt,
# MAGIC t2.Allrelationships_Total_ParentEntityType_Cnt,
# MAGIC t2.Allrelationships_Total_DateOfBirthofParent_Cnt,
# MAGIC t2.Allrelationships_Total_RegisteredCountryofParent_Cnt,
# MAGIC t2.Allrelationships_Total_ClientLifeCycle_Cnt,
# MAGIC t2.Allrelationships_Total_CaseReview_Cnt,
# MAGIC t2.Allrelationships_Total_CaseStatusName_Cnt,
# MAGIC t2.Allrelationships_Total_ClientType_Cnt,
# MAGIC t2.Allrelationships_Total_CitizenshipofParent_Cnt,
# MAGIC t2.Allrelationships_Total_CitizenshipofChild_Cnt,
# MAGIC
# MAGIC t3.Businessactivities_Total_FullLegalName_Cnt, 
# MAGIC t3.Businessactivities_Total_NAICSCode_Cnt,
# MAGIC t3.Businessactivities_Total_ClientLifeCycleName_Cnt, 
# MAGIC t3.Businessactivities_Total_ReviewTypeName_Cnt, 
# MAGIC t3.Businessactivities_Total_CaseStatusName_Cnt,
# MAGIC t3.Businessactivities_Total_FIHubIndicato_Cnt,
# MAGIC t3.Businessactivities_Duplicate_Gcobid_cnt,
# MAGIC
# MAGIC t4.Cddrisk_Total_FullLegalName_Cnt, 
# MAGIC t4.Cddrisk_Total_ReviewTypeName_Cnt, 
# MAGIC t4.Cddrisk_Total_ClientType_Cnt, 
# MAGIC t4.Cddrisk_Total_RiskModelName_Cnt,
# MAGIC t4.Cddrisk_Total_GlobalClientOwner_Cnt,
# MAGIC t4.Cddrisk_Total_ClientLifeCycle_Cnt,
# MAGIC t4.Cddrisk_Total_EntityTypeRiskLevelLE_Cnt,
# MAGIC t4.Cddrisk_Total_EntityTypeRiskLevelNP_Cnt,
# MAGIC t4.Cddrisk_Total_StructureRiskLevelLE_Cnt,
# MAGIC t4.Cddrisk_Total_StructureRiskLevelNP_Cnt,
# MAGIC t4.Cddrisk_Duplicate_Gcobid_cnt,
# MAGIC
# MAGIC t5.ClientOwnership_Total_FullLegalName_Cnt, 
# MAGIC t5.ClientOwnership_Total_CountryOfRegistration_Cnt,
# MAGIC t5.ClientOwnership_Total_ClientLifeCycle_Cnt, 
# MAGIC t5.ClientOwnership_Total_ReviewTypeName_Cnt, 
# MAGIC t5.ClientOwnership_Total_CaseStatusName_Cnt,
# MAGIC t5.ClientOwnership_Duplicate_Gcobid_cnt,
# MAGIC
# MAGIC t6.ClientTradeName_Total_FullLegalName_Cnt, 
# MAGIC t6.ClientTradeName_Total_ReviewTypeName_Cnt,
# MAGIC t6.ClientTradeName_Total_ClientLifeCycle_Cnt,  
# MAGIC t6.ClientTradeName_Total_CaseStatusName_Cnt,
# MAGIC t6.ClientTradeName_Duplicate_Gcobid_cnt,
# MAGIC
# MAGIC t7.ControlMeasure_Total_FullLegalName_Cnt, 
# MAGIC t7.ControlMeasure_Total_FIHubIndicator_Cnt,
# MAGIC t7.ControlMeasure_Total_CaseReview_Cnt,  
# MAGIC t7.ControlMeasure_Total_CaseStatus_Cnt,
# MAGIC t7.ControlMeasure_Total_ClientLifeCycle_Cnt,
# MAGIC t7.ControlMeasure_Total_ControlMeasureId_Cnt,
# MAGIC
# MAGIC t8.Documents_Total_FullLegalName_Cnt, 
# MAGIC t8.Documents_Total_GlobalClientOwner_Cnt,
# MAGIC t8.Documents_Total_NextReviewDate_Cnt,  
# MAGIC t8.Documents_Total_ReviewTypeName_Cnt,
# MAGIC t8.Documents_Total_CaseStatusName_Cnt,
# MAGIC t8.Documents_Total_ClientLifeCycleName_Cnt,
# MAGIC
# MAGIC t9.Fatcacrsdetails_Total_FullLegalName_Cnt, 
# MAGIC t9.Fatcacrsdetails_Total_ClientLifeCycleName_Cnt, 
# MAGIC t9.Fatcacrsdetails_Total_ReviewTypeName_Cnt,
# MAGIC t9.Fatcacrsdetails_Total_CaseStatusName_Cnt,  
# MAGIC t9.Fatcacrsdetails_Total_CountryOfRegistration_Cnt,
# MAGIC t9.Fatcacrsdetails_Duplicate_Gcobid_cnt,
# MAGIC
# MAGIC t10.FIClients_Total_FullLegalName_Cnt, 
# MAGIC t10.FIClients_Total_ReviewTypeName_Cnt,
# MAGIC t10.FIClients_Total_ClientLifeCycle_Cnt,
# MAGIC t10.FIClients_Total_CaseStatusType_Cnt,
# MAGIC
# MAGIC t11.LocalRequirements_Total_FullLegalName_Cnt, 
# MAGIC t11.LocalRequirements_Total_ReviewTypeName_Cnt,
# MAGIC t11.LocalRequirements_Total_ClientLifeCycleName_Cnt,
# MAGIC t11.LocalRequirements_Total_CaseStatusName_Cnt,
# MAGIC t11.LocalRequirements_Total_LocalRequirementCountry_Cnt,
# MAGIC t11.LocalRequirements_Total_CountryOfRegistration_Cnt,
# MAGIC
# MAGIC t12.NameScreening_Total_FullLegalName_Cnt, 
# MAGIC t12.NameScreening_Total_ReviewTypeName_Cnt,
# MAGIC t12.NameScreening_Total_ClientLifeCyclestatusType_Cnt,
# MAGIC t12.NameScreening_Total_CaseStatusName_Cnt,  
# MAGIC t12.NameScreening_Total_RegisteredCountry_Cnt,
# MAGIC t12.NameScreening_Duplicate_Gcobid_cnt,
# MAGIC
# MAGIC t13.Productsandservice_Total_FullLegalName_Cnt,
# MAGIC t13.Productsandservice_Total_ClientLifeCycleName_Cnt,
# MAGIC t13.Productsandservice_Total_ReviewTypeName_Cnt, 
# MAGIC t13.Productsandservice_Total_ClientType_Cnt,
# MAGIC t13.Productsandservice_Total_FIHubIndicator_Cnt,
# MAGIC
# MAGIC t14.structurequestionnaire_Total_FullLegalName_Cnt, 
# MAGIC t14.structurequestionnaire_Total_CaseStatus_Cnt,
# MAGIC t14.structurequestionnaire_Total_ReviewType_Cnt,
# MAGIC t14.structurequestionnaire_Total_ClientLifeCycleStatus_Cnt,  
# MAGIC t14.structurequestionnaire_Total_ClientType_Cnt,
# MAGIC t14.structurequestionnaire_Duplicate_Gcobid_cnt,
# MAGIC
# MAGIC t15.SystemIdentifier_Total_FullLegalName_Cnt, 
# MAGIC t15.SystemIdentifier_Total_ReviewTypeName_Cnt,
# MAGIC t15.SystemIdentifier_Total_ClientLifeCycleName_Cnt,
# MAGIC t15.SystemIdentifier_Total_CaseStatusName_Cnt, 
# MAGIC t15.SystemIdentifier_Duplicate_Gcobid_cnt,
# MAGIC
# MAGIC t16.ubopep_Total_FullLegalName_Cnt,
# MAGIC t16.ubopep_Total_CaseReviewType_Cnt,
# MAGIC t16.ubopep_Total_ClientLifecycleStatusType_Cnt, 
# MAGIC t16.ubopep_Total_CaseStatusType_Cnt,
# MAGIC t16.ubopep_Total_CountryOfRegistration_Cnt,
# MAGIC t16.ubopep_Total_PartyGCOBID_Cnt,
# MAGIC
# MAGIC t17.Workitem_Total_FullLegalName_Cnt,
# MAGIC t17.Workitem_Total_CaseReviewType_Cnt,
# MAGIC t17.Workitem_Total_ClientLifecycleStatusType_Cnt, 
# MAGIC t17.Workitem_Total_CountryOfRegistration_Cnt
# MAGIC
# MAGIC from micasestesting_final t1
# MAGIC left join allrelationships_testing_final t2 on t1.SourceClient=t2.SourceClient
# MAGIC left join businessactivities_testing_final t3 on t1.SourceClient=t3.SourceClient
# MAGIC left join cddrisktesting_final t4 on t1.SourceClient=t4.SourceClient
# MAGIC left join clientownership_testing_final t5  on t1.SourceClient=t5.SourceClient
# MAGIC left join tradeName_testing_final t6 on t1.SourceClient=t6.SourceClient
# MAGIC left join controlmeasure_testing_final t7 on t1.SourceClient=t7.SourceClient
# MAGIC left join documents_testing_final t8 on t1.SourceClient=t8.SourceClient
# MAGIC left join fatcacrsdetails_testing_final t9 on t1.SourceClient=t9.SourceClient
# MAGIC left join fiClient_testing_final t10 on t1.SourceClient =t10.SourceClient
# MAGIC left join localrequirement_testing_final t11 on t1.SourceClient=t11.SourceClient
# MAGIC left join namescreening_testing_final t12 on t1.SourceClient=t12.SourceClient
# MAGIC left join productsandservice_testing_final t13 on t1.SourceClient=t13.SourceClient
# MAGIC left join structurequestionnaire_testing_final t14 on t1.SourceClient=t14.SourceClient
# MAGIC left join systemidentifier_testing_final t15 on t1.SourceClient=t15.SourceClient
# MAGIC left join ubopep_testing_final t16 on t1.SourceClient=t16.SourceClient
# MAGIC left join workitem_testing_final t17 on t1.SourceClient=t17.SourceClient
# MAGIC )

# COMMAND ----------

# Check if the table exists
if spark.catalog.tableExists("radar.final_dq_check_table"):
    # Drop the table if it exists
   spark.sql("DROP TABLE radar.final_dq_check_table")

# Create the table with the new data
spark.sql("SELECT * FROM final_dq_check_table").write.mode("overwrite").saveAsTable("radar.final_dq_check_table")
