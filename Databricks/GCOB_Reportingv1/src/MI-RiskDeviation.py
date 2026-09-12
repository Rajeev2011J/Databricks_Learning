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

MI_RiskDeviation_dataobject = 'MI_RiskLevelDeviation'

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
, 'Legacy2_RD_adt_Client'
, 'Legacy2_RD_MigratedClient'
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

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CaseClientDetails AS
# MAGIC     select distinct CaseId,ClientId,SourceClient,GcobId,ValidatedRiskLevel,FullLegalName,clienttype,SourceSystem,
# MAGIC     AdverseInfoRiskLevel,GeographicalRiskLevel,EntityTypeRiskLevel,StructureRiskLevel,SectorRiskLevel,ProductAndServiceRiskLevel,PEPRiskLevel,TransactionRiskLevel,DistributionRiskLevel,ThirdPartyRiskLevel
# MAGIC     ,FatcaClassification,CrsClassification
# MAGIC     From party_case_client_details
# MAGIC     where sourcesystem = 'GCOB'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CTE_PreviousRisk_Case AS
# MAGIC with PreviousRiskLevel as (
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
# MAGIC ,case when CaseId = PreviousCaseId then null else `Previous risk level` end as PreviousValidatedRiskLevel
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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_Min_Case AS
# MAGIC select distinct
# MAGIC gcobid , min(caseid) as min_caseid, FullLegalName,sourceclient
# MAGIC from party_case_client_details
# MAGIC where sourcesystem = 'GCOB'
# MAGIC group by gcobid, FullLegalName,sourceclient

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_Migrated AS
# MAGIC select distinct
# MAGIC l.ClientId
# MAGIC ,l.GcobId
# MAGIC ,l.FullLegalName
# MAGIC ,l.ClientType
# MAGIC ,l.SourceClient
# MAGIC ,l.ValidatedRiskLevel
# MAGIC ,l.GeographicalRetainedRiskLevel
# MAGIC ,l.EntityTypeRetainedRiskLevel
# MAGIC ,l.StructureRetainedRiskLevel
# MAGIC ,l.SectorRetainedRiskLevel
# MAGIC ,l.ProductsRetainedRiskLevel
# MAGIC ,l.PoliticallyExposedPersonsRetainedRiskLevel
# MAGIC ,l.ThirdPartyRetainedRiskLevel
# MAGIC ,l.TransactionRetainedRiskLevel
# MAGIC ,l.DistributionChannelRetainedRiskLevel
# MAGIC ,l.AdverseInfoRetainedRiskLevel
# MAGIC ,adt.Legacy_2_GcobId_Migrated
# MAGIC ,g.min_caseid
# MAGIC ,g.sourceclient as Gcob_sourceclient
# MAGIC From Legacy2_client_details l
# MAGIC inner join Legacy2_RD_adt_Client adt on l.clientid = adt.identity and l.gcobid=adt.gcobid
# MAGIC inner join GCOB_Min_Case g on adt.Legacy_2_GcobId_Migrated = g.min_caseid and l.FullLegalName = g.FullLegalName
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CrossSystemRisk AS
# MAGIC select distinct
# MAGIC p.GcobId,p.CaseId,p.ClientId,p.SourceClient
# MAGIC ,case when l.Legacy_2_GcobId_Migrated is not null and p.PreviousValidatedRiskLevel is null then l.ValidatedRiskLevel else p.PreviousValidatedRiskLevel end as PreviousValidatedRiskLevel
# MAGIC ,case when l.Legacy_2_GcobId_Migrated is not null and p.PreviousAdverseInfoRiskLevel is null then l.AdverseInfoRetainedRiskLevel else p.PreviousAdverseInfoRiskLevel end as PreviousAdverseInfoRiskLevel
# MAGIC ,case when l.Legacy_2_GcobId_Migrated is not null and p.PreviousGeographicalRiskLevel is null then l.GeographicalRetainedRiskLevel else p.PreviousGeographicalRiskLevel end as PreviousGeographicalRiskLevel
# MAGIC ,case when l.Legacy_2_GcobId_Migrated is not null and p.PreviousEntityTypeRiskLevel is null then l.EntityTypeRetainedRiskLevel else p.PreviousEntityTypeRiskLevel end as PreviousEntityTypeRiskLevel
# MAGIC ,case when l.Legacy_2_GcobId_Migrated is not null and p.PreviousStructureRiskLevel is null then l.StructureRetainedRiskLevel else p.PreviousStructureRiskLevel end as PreviousStructureRiskLevel
# MAGIC ,case when l.Legacy_2_GcobId_Migrated is not null and p.PreviousSectorRiskLevel is null then l.SectorRetainedRiskLevel else p.PreviousSectorRiskLevel end as PreviousSectorRiskLevel
# MAGIC ,case when l.Legacy_2_GcobId_Migrated is not null and p.PreviousProductAndServiceRiskLevel is null then l.ProductsRetainedRiskLevel else p.PreviousProductAndServiceRiskLevel end as PreviousProductAndServiceRiskLevel
# MAGIC ,case when l.Legacy_2_GcobId_Migrated is not null and p.PreviousPEPRiskLevel is null then l.PoliticallyExposedPersonsRetainedRiskLevel else p.PreviousPEPRiskLevel end as PreviousPEPRiskLevel
# MAGIC ,case when l.Legacy_2_GcobId_Migrated is not null and p.PreviousTransactionRiskLevel is null then l.TransactionRetainedRiskLevel else p.PreviousTransactionRiskLevel end as PreviousTransactionRiskLevel
# MAGIC ,case when l.Legacy_2_GcobId_Migrated is not null and p.PreviousDistributionRiskLevel is null then l.DistributionChannelRetainedRiskLevel else p.PreviousDistributionRiskLevel end as PreviousDistributionRiskLevel
# MAGIC ,case when p.PreviousThirdPartyRiskLevel is null then l.ThirdPartyRetainedRiskLevel else p.PreviousThirdPartyRiskLevel end as PreviousThirdPartyRiskLevel
# MAGIC from CTE_PreviousRisk_Case p
# MAGIC Left join Legacy2_Migrated l on p.caseid = l.min_caseid and p.sourceclient = l.Gcob_sourceclient

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOB_Risk AS
# MAGIC select distinct
# MAGIC p.SourceClient,p.ClientId,p.GcobId,p.caseid,p.FullLegalName,p.ScheduledCompletionDate,p.CaseCompletedDate,p.NextReviewDate,p.ClientOwnerSignOffDate,p.CaseStatusName,p.ClientLifeCycleName,p.ReviewTypeName,p.SourceSystem,p.IsLatestApprovedVersionOfClient,p.GlobalClientOwner,p.GlobalClientOwnerLocation,p.BusinessLineName
# MAGIC ,c.GeographicalRiskLevel
# MAGIC ,pr.PreviousGeographicalRiskLevel
# MAGIC ,case when c.GeographicalRiskLevel = pr.PreviousGeographicalRiskLevel then 'No' 
# MAGIC       when pr.PreviousGeographicalRiskLevel is null then 'No' else 'Yes' end as GeographicalRiskLevel_Change
# MAGIC ,c.EntityTypeRiskLevel
# MAGIC ,pr.PreviousEntityTypeRiskLevel
# MAGIC ,case when c.EntityTypeRiskLevel = pr.PreviousEntityTypeRiskLevel then 'No' 
# MAGIC       when pr.PreviousEntityTypeRiskLevel is null then 'No' else 'Yes' end as EntityTypeRiskLevel_Change
# MAGIC ,c.StructureRiskLevel
# MAGIC ,pr.PreviousStructureRiskLevel
# MAGIC ,case when c.StructureRiskLevel = pr.PreviousStructureRiskLevel then 'No' 
# MAGIC       when pr.PreviousStructureRiskLevel is null then 'No' else 'Yes' end as StructureRiskLevel_Change
# MAGIC ,c.SectorRiskLevel
# MAGIC ,pr.PreviousSectorRiskLevel
# MAGIC ,case when c.SectorRiskLevel = pr.PreviousSectorRiskLevel then 'No' 
# MAGIC       when pr.PreviousSectorRiskLevel is null then 'No' else 'Yes' end as SectorRiskLevel_Change
# MAGIC ,c.ProductAndServiceRiskLevel
# MAGIC ,pr.PreviousProductAndServiceRiskLevel
# MAGIC ,case when c.ProductAndServiceRiskLevel = pr.PreviousProductAndServiceRiskLevel then 'No' 
# MAGIC       when pr.PreviousProductAndServiceRiskLevel is null then 'No' else 'Yes' end as ProductAndServiceRiskLevel_Change
# MAGIC ,c.PEPRiskLevel
# MAGIC ,pr.PreviousPEPRiskLevel
# MAGIC ,case when c.PEPRiskLevel = pr.PreviousPEPRiskLevel then 'No' 
# MAGIC       when pr.PreviousPEPRiskLevel is null then 'No' else 'Yes' end as PEPRiskLevel_Change
# MAGIC ,c.TransactionRiskLevel
# MAGIC ,pr.PreviousTransactionRiskLevel
# MAGIC ,case when c.TransactionRiskLevel = pr.PreviousTransactionRiskLevel then 'No' 
# MAGIC       when pr.PreviousTransactionRiskLevel is null then 'No' else 'Yes' end as TransactionRiskLevel_Change
# MAGIC ,c.DistributionRiskLevel
# MAGIC ,pr.PreviousDistributionRiskLevel
# MAGIC ,case when c.DistributionRiskLevel = pr.PreviousDistributionRiskLevel then 'No' 
# MAGIC       when pr.PreviousDistributionRiskLevel is null then 'No' else 'Yes' end as DistributionRiskLevel_Change
# MAGIC ,c.ThirdPartyRiskLevel
# MAGIC ,pr.PreviousThirdPartyRiskLevel
# MAGIC ,case when c.ThirdPartyRiskLevel = pr.PreviousThirdPartyRiskLevel then 'No' 
# MAGIC       when pr.PreviousThirdPartyRiskLevel is null then 'No' else 'Yes' end as ThirdPartyRiskLevel_Change
# MAGIC ,c.AdverseInfoRiskLevel
# MAGIC ,pr.PreviousAdverseInfoRiskLevel
# MAGIC ,case when c.AdverseInfoRiskLevel = pr.PreviousAdverseInfoRiskLevel then 'No' 
# MAGIC       when pr.PreviousAdverseInfoRiskLevel is null then 'No' else 'Yes' end as AdverseInfoRiskLevel_Change
# MAGIC ,c.ValidatedRiskLevel
# MAGIC ,pr.PreviousValidatedRiskLevel
# MAGIC ,case when c.ValidatedRiskLevel = pr.PreviousValidatedRiskLevel then 'No' 
# MAGIC       when pr.PreviousValidatedRiskLevel is null then 'No' else 'Yes' end as ValidatedRiskLevel_Change
# MAGIC --,p.LastKYCAnalyst
# MAGIC From CaseClientDetails c
# MAGIC Inner Join CrossSystemRisk pr on c.SourceClient = pr.SourceClient
# MAGIC Left join party_case_client_details p on c.SourceClient = p.SourceClient
# MAGIC --where 
# MAGIC --c.gcobid = 16166 
# MAGIC --p.FullLegalName = '3 N & M, Inc.'
# MAGIC --order by c.CaseId desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Adt_table AS
# MAGIC select distinct Identity,GcobId,Previous_ValidatedRiskLevel,Previous_AdverseRiskLevel,Previous_DistributionChannelRiskLevel,Previous_EntityTypeRiskLevel,Previous_GeographicalRiskLevel,Previous_PepRiskLevel,Previous_ProductsAndServicesRiskLevel,Previous_SectorRiskLevel,Previous_StructureRiskLevel,Previous_ThirdPartyRiskLevel,Previous_TransactionRiskLevel from Legacy2_RD_adt_Client --where Identity = 1982 --11496

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW L2_Risk AS
# MAGIC Select DISTINCT 
# MAGIC SourceClient
# MAGIC , l.ClientId
# MAGIC , l.GcobId
# MAGIC , null as CaseId
# MAGIC , l.FullLegalName
# MAGIC , l.ScheduledCompletionDate
# MAGIC , l.CaseCompletedDate
# MAGIC , l.NextReviewDate
# MAGIC , l.ClientOwnerSignOffDate
# MAGIC , l.StatusTypeName as CaseStatusName
# MAGIC , l.ClientLifeCycleName
# MAGIC , l.ReviewTypeName
# MAGIC , l.SourceSystem
# MAGIC , l.IsLatestApprovedVersionOfClient
# MAGIC , l.GlobalClientOwner
# MAGIC , l.GlobalClientOwnerLocation
# MAGIC , l.BusinessLineName
# MAGIC , l.GeographicalRetainedRiskLevel as GeographicalRiskLevel
# MAGIC , adt.Previous_GeographicalRiskLevel as PreviousGeographicalRiskLevel
# MAGIC , case when l.GeographicalRetainedRiskLevel = adt.Previous_GeographicalRiskLevel then 'No' 
# MAGIC        when adt.Previous_GeographicalRiskLevel is null then 'No' else 'Yes' end as GeographicalRiskLevel_Change
# MAGIC ,	l.EntityTypeRetainedRiskLevel as EntityTypeRiskLevel
# MAGIC , adt.Previous_EntityTypeRiskLevel as PreviousEntityTypeRiskLevel
# MAGIC , case when l.EntityTypeRetainedRiskLevel = adt.Previous_EntityTypeRiskLevel then 'No' 
# MAGIC        when adt.Previous_EntityTypeRiskLevel is null then 'No' else 'Yes' end as EntityTypeRiskLevel_Change
# MAGIC ,l.StructureRetainedRiskLevel as StructureRiskLevel
# MAGIC , adt.Previous_StructureRiskLevel as PreviousStructureRiskLevel
# MAGIC , case when l.StructureRetainedRiskLevel = adt.Previous_StructureRiskLevel then 'No' 
# MAGIC        when adt.Previous_StructureRiskLevel is null then 'No' else 'Yes' end as StructureRiskLevel_Change
# MAGIC ,	l.SectorRetainedRiskLevel as SectorRiskLevel
# MAGIC , adt.Previous_SectorRiskLevel as PreviousSectorRiskLevel
# MAGIC , case when l.SectorRetainedRiskLevel = adt.Previous_SectorRiskLevel then 'No' 
# MAGIC        when adt.Previous_SectorRiskLevel is null then 'No' else 'Yes' end as SectorRiskLevel_Change
# MAGIC ,	l.ProductsRetainedRiskLevel as ProductAndServiceRiskLevel
# MAGIC , adt.Previous_ProductsAndServicesRiskLevel as PreviousProductAndServiceRiskLevel
# MAGIC , case when l.ProductsRetainedRiskLevel = adt.Previous_ProductsAndServicesRiskLevel then 'No' 
# MAGIC        when adt.Previous_ProductsAndServicesRiskLevel is null then 'No' else 'Yes' end as ProductAndServiceRiskLevel_Change
# MAGIC ,	l.PoliticallyExposedPersonsRetainedRiskLevel as PEPRiskLevel
# MAGIC , adt.Previous_PepRiskLevel as PreviousPEPRiskLevel
# MAGIC , case when l.PoliticallyExposedPersonsRetainedRiskLevel = adt.Previous_PepRiskLevel then 'No' 
# MAGIC        when adt.Previous_PepRiskLevel is null then 'No' else 'Yes' end as PEPRiskLevel_Change
# MAGIC ,	l.TransactionRetainedRiskLevel as TransactionRiskLevel
# MAGIC , adt.Previous_TransactionRiskLevel as PreviousTransactionRiskLevel
# MAGIC , case when l.TransactionRetainedRiskLevel = adt.Previous_TransactionRiskLevel then 'No' 
# MAGIC        when adt.Previous_TransactionRiskLevel is null then 'No' else 'Yes' end as TransactionRiskLevel_Change
# MAGIC ,	l.DistributionChannelRetainedRiskLevel as DistributionRiskLevel
# MAGIC , adt.Previous_DistributionChannelRiskLevel as PreviousDistributionRiskLevel
# MAGIC , case when l.DistributionChannelRetainedRiskLevel = adt.Previous_DistributionChannelRiskLevel then 'No' 
# MAGIC        when adt.Previous_DistributionChannelRiskLevel is null then 'No' else 'Yes' end as DistributionRiskLevel_Change
# MAGIC ,	l.ThirdPartyRetainedRiskLevel as ThirdPartyRiskLevel
# MAGIC , adt.Previous_ThirdPartyRiskLevel as PreviousThirdPartyRiskLevel
# MAGIC , case when l.ThirdPartyRetainedRiskLevel = adt.Previous_ThirdPartyRiskLevel then 'No' 
# MAGIC        when adt.Previous_ThirdPartyRiskLevel is null then 'No' else 'Yes' end as ThirdPartyRiskLevel_Change
# MAGIC ,	l.AdverseInfoRetainedRiskLevel as AdverseInfoRiskLevel
# MAGIC , adt.Previous_AdverseRiskLevel as PreviousAdverseInfoRiskLevel
# MAGIC , case when l.AdverseInfoRetainedRiskLevel = adt.Previous_AdverseRiskLevel then 'No' 
# MAGIC        when adt.Previous_AdverseRiskLevel is null then 'No' else 'Yes' end as AdverseInfoRiskLevel_Change
# MAGIC ,	l.ValidatedRiskLevel
# MAGIC , adt.Previous_ValidatedRiskLevel as PreviousValidatedRiskLevel
# MAGIC , case when l.ValidatedRiskLevel = adt.Previous_ValidatedRiskLevel then 'No' 
# MAGIC        when adt.Previous_ValidatedRiskLevel is null then 'No' else 'Yes' end as ValidatedRiskLevel_Change
# MAGIC From Legacy2_client_details l
# MAGIC left join Adt_table adt on l.ClientId = adt.Identity

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RiskDeviation AS
# MAGIC select * from GCOB_Risk
# MAGIC union
# MAGIC select * from L2_Risk

# COMMAND ----------

df_RiskDeviation=spark.table('RiskDeviation')

# COMMAND ----------

df_RiskDeviation=df_RiskDeviation.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

# df_RiskDeviation.createOrReplaceTempView("ClientRiskDeviation")

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data

# COMMAND ----------

save_to_saradar_storage_account(df_RiskDeviation, MI_RiskDeviation_dataobject)

# COMMAND ----------

# %sql
# drop table IF EXISTS radar.MI_RiskLevelDeviation

# COMMAND ----------

# spark.sql('select * from ClientRiskDeviation').write.mode('overwrite').saveAsTable('radar.MI_RiskLevelDeviation')
