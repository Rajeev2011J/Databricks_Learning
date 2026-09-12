# Databricks notebook source
import os 

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

gcob_tables = [
    'party_case_client_details'
    , 'party_workitem'
    # , 'party_local_client_Owners'
    # , 'party_request_for_information'
    # , 'party_client'
    # , 'party_products_and_sevices'
    # , 'party_trade_name'
]


for item in gcob_tables:
    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/101/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('EDL_LOAD_DTS=')[1][:8] for file in files if 'EDL_LOAD_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/101/data/EDL_LOAD_DTS={load_date}/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# load and create temp views of all gdp_tables below
gcob_tables = [
    'CaseService_dbo_Country'
]

for item in gcob_tables:
    # get the most recent file available in gdp
    path = f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/{item}/100/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOADED_DTS=')[1][:8] for file in files if 'LOADED_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/{item}/100/data/LOADED_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

id_list = [i + 1 for i in range(22)]
name_list = [
    'Initiation In Progress'
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
    , 'Senior management sign off requested'
]

spark.createDataFrame(zip(id_list, name_list), ['StatusId', 'Name']).createOrReplaceTempView('gcob_static_CaseStatusType')

# COMMAND ----------

# DBTITLE 1,CTE_vw_ClientDetail
# MAGIC %sql
# MAGIC
# MAGIC WITH CTE_vw_ClientDetail AS (
# MAGIC SELECT DISTINCT
# MAGIC   t1.EDL_LOAD_DTS                               AS RunDate
# MAGIC   , t1.SourceSystem
# MAGIC   , t1.ClientId                                 AS Identity
# MAGIC   , t1.CaseId
# MAGIC   , t1.GcobId
# MAGIC   , t1.FullLegalName
# MAGIC   , t1.GlobalClientOwner
# MAGIC   , t1.GlobalClientOwnerLocation
# MAGIC   , t1.GlobalClientOwnerOfficeLocation          AS GlobalClientOwnerOffice
# MAGIC   , t1.CaseStatusName                           AS CaseStatusType
# MAGIC   , t1.ReviewTypeName                           AS CaseReviewType
# MAGIC   , t1.RiskModelName                            AS RiskModel
# MAGIC   , t1.BusinessLineName                         AS BusinessLine
# MAGIC   , t1.ClientLifeCycleName                      AS ClientLifeCycleStatusType
# MAGIC   , t1.ClientApprovalDate
# MAGIC   , t1.NextReviewDate
# MAGIC   , t1.GeographicalRiskLevel
# MAGIC   , t1.GeographicalApplicableRisk               AS GeographicalRiskApplicable
# MAGIC   , t1.EntityTypeRiskLevel
# MAGIC   , t1.EntityTypeApplicableRisk                 AS EntityTypeRiskApplicable
# MAGIC   , t1.SectorRiskLevel
# MAGIC   , t1.SectorApplicableRisk                     AS SectorRiskApplicable
# MAGIC   , t1.ProductAndServiceRiskLevel               AS ProductsAndServicesRiskLevel
# MAGIC   , t1.ProductsApplicableRisk                   AS ProductsAndServicesRiskApplicable
# MAGIC   , t1.StructureRiskLevel
# MAGIC   , t1.StructureApplicableRisk                  AS StructureRiskApplicable
# MAGIC   , t1.TransactionRiskLevel
# MAGIC   , t1.TransactionApplicableRisk                AS TransactionRiskApplicable
# MAGIC   , t1.DistributionRiskLevel                    AS DistributionChannelRiskLevel
# MAGIC   , t1.DistributionChannelApplicableRisk        AS DistributionChannelRiskApplicable
# MAGIC   , t1.ThirdPartyRiskLevel
# MAGIC   , t1.ThirdPartyApplicableRisk                 AS ThirdPartyRiskApplicable
# MAGIC   , t1.AdverseInfoRiskLevel
# MAGIC   , t1.AdverseInfoApplicableRisk                AS AdverseInfoRiskApplicable
# MAGIC   , t1.PEPRiskLevel
# MAGIC   , t1.PoliticallyExposedPersonsApplicableRisk  AS PEPRiskApplicable
# MAGIC   , t1.ModelCalculatedRiskLevel
# MAGIC   , t1.ModelRecalculatedRiskLevel
# MAGIC   , t1.ValidatedRiskLevel
# MAGIC   , t1.ScheduledCompletionDate
# MAGIC   , t1.WWID
# MAGIC   , t1.ISB
# MAGIC   , t1.RUTID
# MAGIC   , t1.ACBS
# MAGIC   , t1.CddType
# MAGIC   , t1.EdrReason
# MAGIC   , t1.EdrOtherReason
# MAGIC   , t1.IsEligibleForFatcaAssessment
# MAGIC   , t1.FatcaClassification
# MAGIC   , t1.IsEligibleForCrsAssessment
# MAGIC   , t1.CrsClassification
# MAGIC   , t1.SubmitToClientCommittee
# MAGIC   , t1.IsLatestApprovedVersionOfClient
# MAGIC   , t1.RingFenced                               AS IsRingFenced
# MAGIC   , t1.HasTaxForm
# MAGIC   , t1.IncorporationNumber
# MAGIC   , t1.ContactPersonEmailAddress
# MAGIC   , t1.ContactPersonTelephoneNumber
# MAGIC   , t2.Name                                     AS OperatingCountry
# MAGIC   , t3.Name                                     AS RegisteredCountry
# MAGIC   , t1.FinalDecisionDate                        AS SignOffDate
# MAGIC   , t4.StatusId                                 AS CaseStatusTypeKey
# MAGIC
# MAGIC   FROM party_case_client_details t1
# MAGIC   LEFT JOIN CaseService_dbo_Country t2 ON t1.OperatingCountryIsoCode = t2.IsoCode
# MAGIC   LEFT JOIN CaseService_dbo_Country t3 ON t1.RegisteredCountryIsoCode = t3.IsoCode
# MAGIC   LEFT JOIN gcob_static_CaseStatusType t4 ON t1.CaseStatusName = t4.Name 
# MAGIC   WHERE t2.Expired IS NULL
# MAGIC     AND t3.Expired IS NULL
# MAGIC )
# MAGIC
# MAGIC SELECT TOP(1) WITH TIES * FROM CTE_vw_ClientDetail WHERE SourceSystem <> 'GCOB Legacy2'
# MAGIC ORDER BY row_number() OVER (PARTION BY GcobId, CaseId ORDER BY Rundate)

# COMMAND ----------

# DBTITLE 1,vw_WorkItems
# MAGIC %sql
# MAGIC SELECT
# MAGIC   EDL_LOAD_DTS AS BusinessDate
# MAGIC   , WorkItemId
# MAGIC   , CaseId
# MAGIC
# MAGIC
# MAGIC   
# MAGIC   , AS CreatingUser
# MAGIC   , AS CreatingUserId
# MAGIC   , AS AssignedUser
# MAGIC   , AS AssignedUserId
# MAGIC   , AS CaseStatusTypeWhenCreated
# MAGIC   , AS CaseStatusTypeWhenCreatedKey
# MAGIC   , AS ResponsibleRole
# MAGIC FROM party_workitem

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_workitem limit 100
