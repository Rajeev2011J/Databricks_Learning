# Databricks notebook source
import os
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# COMMAND ----------

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

from datetime import datetime, timedelta
import re

gcob_objects = [
    'party_products_and_services'
]

for item in gcob_objects:
    # get the most recent version available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/'
    files = dbutils.fs.ls(path)
    version = max([
        int(re.search(r'/(\d+)/$', file.path).group(1))
        for file in files
        if re.search(r'/(\d+)/$', file.path)
    ])

    version = 102 # TEMPORARY UNTIL THE NEW VERSION IS UP AND RUNNING!!!!

    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOAD_DT=')[1][:8] for file in files if 'LOAD_DT=' in file.path)

    if item == 'party_case_client_details':
        spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/LOAD_DT={load_date}*/*.parquet').createOrReplaceTempView('party_case_client_details_raw')
    else:
        spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{item}/{version}/data/LOAD_DT={load_date}*/*.parquet').createOrReplaceTempView(item)

EDL_LoadDate = datetime.strptime(load_date, '%Y%m%d').strftime('%Y-%m-%d')

# COMMAND ----------

# DBTITLE 1,gcob_tables
import re

gcob_tables = [
    'CaseService_case_LegalEntityClientStructureSnapshot'
    , 'CaseService_snapshot_ClientStructureSnapshotRelationshipDetail'
    , 'CaseService_case_UboThroughReasonShareholdingRelationshipComponent'
    , 'CaseService_case_UboThroughReasonRelationshipComponent'
    , 'CaseService_case_EndOfChainOwnershipShareholdingRelationshipComponent'
    , 'CaseService_case_UboThroughReasonReference'
    , 'CaseService_case_AuthorisedRepresentativeRelationshipComponent'
    , 'CaseService_case_BranchOfRelationshipComponent'
    , 'CaseService_case_FundManagedByRelationshipComponent'
    , 'CaseService_case_GuarantorRelationshipComponent'
    , 'CaseService_case_DirectorshipRelationshipComponent'
    , 'CaseService_case_SubAccountOfRelationshipComponent'
    , 'CaseService_case_AgentRelationshipComponent'
    , 'CaseService_case_SettlorFounderRelationshipComponent'
    , 'CaseService_case_ProtectorRelationshipComponent'
    , 'CaseService_case_TrusteeRelationshipComponent'
    , 'CaseService_case_BeneficiaryRelationshipComponent'
    , 'CaseService_case_SpvParticipantRelationshipComponent'
    , 'CaseService_case_CddRuleCertifierRelationshipComponent'
    , 'CaseService_case_GeneralPartnerRelationshipComponent'
    , 'CaseService_case_LimitedPartnerRelationshipComponent'
    , 'CaseService_case_CertificateHolderRelationshipComponent'
    , 'CaseService_case_OriginatorRelationshipComponent'
    , 'CaseService_case_OtherRelationshipComponent'
    , 'CaseService_case_IntegratorRelationshipComponent'
    , 'CaseService_case_EndOfChainOwnershipReasonReference'
    , 'CaseService_case_RelatedLegalEntityParty'
    , 'CaseService_case_RelatedNaturalPersonParty'
    , 'CaseService_case_LegalEntityClient'
    , 'CaseService_case_Case'
    , 'CaseService_case_Address'
    , 'CaseService_snapshot_ClientStructureSnapshotCalculatedUboShareholdingDetail'
    , 'CaseService_case_ProductProvidedToLegalEntity'
    , 'CaseService_dbo_Country'
    , 'CaseService_case_Relationship'
    , 'CaseService_case_RelatedNaturalPersonPartyNationality'
    , 'CaseService_case_RelatedNaturalPersonPartyCitizenship'
    , 'CaseService_case_RelatedNaturalPersonPartyAlias'
    , 'CaseService_case_RelatedLegalEntityPartyTradingName'
    , 'CaseService_case_LegalEntityTradingName'
    , 'CaseService_dbo_BusinessLine'
    , 'CaseService_NaturalPerson_InvolvedStaffMember'
    , 'CaseService_case_InvolvedStaffMember'
    , 'CaseService_user_GcobUser'
    , 'CaseService_dbo_RabobankEntity'
    , 'CaseService_NaturalPerson_NaturalPersonClient'
    , 'CaseService_NaturalPerson_NaturalPersonCase'
    , 'CaseService_NaturalPerson_ClientStructureSnapshot'
    , 'CaseService_NaturalPerson_ProductProvidedToNaturalPerson'
]

for item in gcob_tables:
    # get the most recent version available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/{item}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOADED_DTS=')[1][:8] for file in files if 'LOADED_DTS=' in file.path)

    spark.read.parquet(f'{path}LOADED_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# DBTITLE 1,rp_source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW rp_source AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t2.CaseId
# MAGIC   , t3.LegalEntityClientId
# MAGIC   , t3.ClientStructureSnapshotId
# MAGIC   , t4.ChildIdentity
# MAGIC   , t4.ChildType
# MAGIC   , t4.ParentIdentity
# MAGIC   , t4.ParentType
# MAGIC   , t4.RelationshipId
# MAGIC   , CASE
# MAGIC       WHEN t4.ParentType = 1 THEN CONCAT('RLE', t30.`Identity`)
# MAGIC       WHEN t4.ParentType = 2 THEN CONCAT('RNP', t31.`Identity`)
# MAGIC       WHEN t4.ParentType = 0 THEN CAST(t34.GcobId AS STRING)
# MAGIC       ELSE t4.ParentIdentity -- for dashboard only, for spike comment this part
# MAGIC     END AS ParentIdentityExplicit
# MAGIC   , CASE
# MAGIC       WHEN t4.ParentType = 1 THEN t30.FullLegalName
# MAGIC       WHEN t4.ParentType = 2 THEN CONCAT(t31.FirstName, ' ', t31.LastName)
# MAGIC       ELSE t34.FullLegalName -- for dashboard only, for spike comment this part
# MAGIC     END AS ParentIdentityName
# MAGIC   , CASE
# MAGIC     WHEN t4.ParentType = 2 AND t31.PoliticallyExposedPersonStatusId IN (1,2,4,5) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PEPFlag
# MAGIC   , CASE
# MAGIC       WHEN t4.ParentType = 1 THEN t41.Name
# MAGIC       WHEN t4.ParentType = 2 THEN t43.Name
# MAGIC       WHEN t4.ParentType = 0 THEN t47.Name
# MAGIC     END AS RegisteredCountry
# MAGIC   -- -- aliases
# MAGIC   -- , CASE
# MAGIC   --     WHEN t4.ParentType = 1 THEN t51.TradingName
# MAGIC   --     WHEN t4.ParentType = 2 THEN CONCAT(t50.FirstName, ' ', t50.LastName) -- t50.MiddleName
# MAGIC   --     WHEN t4.ParentType = 0 THEN t52.TradingName
# MAGIC   --   END AS ParentIdentityExplicitAliases
# MAGIC   -- , CASE
# MAGIC   --     WHEN t4.ParentType = 2 THEN t45.Name
# MAGIC   --   END AS NationalityRNP
# MAGIC   -- , CASE
# MAGIC   --     WHEN t4.ParentType = 2 THEN t49.Name
# MAGIC   --   END AS CitizenshipRNP
# MAGIC
# MAGIC     , CONCAT_WS(', ',
# MAGIC         SORT_ARRAY(
# MAGIC           COLLECT_SET(
# MAGIC             CASE
# MAGIC               WHEN t4.ParentType = 1 THEN t51.TradingName
# MAGIC               WHEN t4.ParentType = 2 THEN CONCAT(t50.FirstName, ' ', t50.LastName)
# MAGIC               WHEN t4.ParentType = 0 THEN t52.TradingName
# MAGIC             END
# MAGIC           )
# MAGIC         )
# MAGIC       ) AS ParentIdentityExplicitAliases
# MAGIC     , CONCAT_WS(', ',
# MAGIC         SORT_ARRAY(
# MAGIC           COLLECT_SET(
# MAGIC             CASE
# MAGIC               WHEN t4.ParentType = 2 THEN t45.Name
# MAGIC             END
# MAGIC           )
# MAGIC         ) 
# MAGIC       ) AS NationalityRNP
# MAGIC     , CONCAT_WS(', ',
# MAGIC         SORT_ARRAY(
# MAGIC           COLLECT_SET(
# MAGIC             CASE
# MAGIC               WHEN t4.ParentType = 2 THEN t49.Name
# MAGIC             END
# MAGIC           )
# MAGIC         )
# MAGIC       ) AS CitizenshipRNP
# MAGIC
# MAGIC   , t5.relationshipidentity AS OtherId
# MAGIC   , t5.explanation AS OtherRelationshipExplanation
# MAGIC   , t6.relationshipidentity AS IntegratorId
# MAGIC   , t7.relationshipidentity AS GuarantorId
# MAGIC   , t7.GuarantorRelationshipType AS GuarantorRelationshipType
# MAGIC   , t8.relationshipidentity AS AgentId
# MAGIC   , t13.relationshipidentity AS LimitedPartnerId
# MAGIC   , t14.relationshipidentity AS FundmanagerId
# MAGIC   , t15.relationshipidentity AS SpvId
# MAGIC   , t16.relationshipidentity AS DirectorId
# MAGIC   , t17.relationshipidentity AS SettleOrFounderId
# MAGIC   , t18.relationshipidentity AS BeneficiaryId
# MAGIC   , t19.relationshipidentity AS OriginatorId
# MAGIC   , t20.relationshipidentity AS SubaccountId
# MAGIC   , t21.relationshipidentity AS TrusteeId
# MAGIC   , t22.relationshipidentity AS ProtectorId
# MAGIC   , t23.relationshipidentity AS CDDRuleCertifierId
# MAGIC   , t24.relationshipidentity AS AuthorisedRepresentativeId
# MAGIC   , t24.AuthorisedRepresentativeAuthority AS ArAuthorisedRepresentativeAuthority
# MAGIC   , t25.relationshipidentity AS BranchOfId
# MAGIC   , t26.relationshipidentity AS CertificateHolderId
# MAGIC   , t27.relationshipidentity AS GeneralPartnerId
# MAGIC   , t28.relationshipidentity AS UboThroughReasonShareholdingId
# MAGIC   , t28.ShareholdingPercentage AS ShShareholdingPercentage
# MAGIC   , t28.VotingRightPercentage AS ShVotingRightPercentage
# MAGIC   , t28.ContextIdentity AS ShContextIdentity
# MAGIC   , t29.relationshipidentity AS EndOfChainOwnershipShareholdingId
# MAGIC   , t29.ShareholdingPercentage AS EocShareholdingPercentage
# MAGIC   , t29.VotingRightPercentage AS EocVotingRightPercentage
# MAGIC   , t29.EndOfChainOwnershipEntityReferenceId AS EocEndOfChainOwnershipEntityReferenceId
# MAGIC   , t32.relationshipidentity AS UboThroughReasonShareholdingRelationshipId
# MAGIC   , t32.ContextIdentity AS UtrContextIdentity
# MAGIC   , t32.UboThroughReasonReferenceId AS UtrUboThroughReasonReferenceId
# MAGIC   , t35.Description AS EocEndOfChainOwnershipReasonDescription
# MAGIC   , t36.Description AS UtrUboThroughReasonDescription
# MAGIC   , t31.FullNameInLocalLanguage
# MAGIC   , t31.DateOfBirthString
# MAGIC
# MAGIC FROM radar.clients t1
# MAGIC LEFT JOIN radar.cases t2 ON t1.CompletedId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC
# MAGIC LEFT JOIN CaseService_case_LegalEntityClientStructureSnapshot t3 ON t1.CompletedId = t3.LegalEntityClientId
# MAGIC LEFT JOIN CaseService_snapshot_ClientStructureSnapshotRelationshipDetail t4 ON t3.ClientStructureSnapshotId = t4.ClientStructureSnapshotId
# MAGIC INNER JOIN (
# MAGIC   SELECT
# MAGIC     MAX(ClientStructureSnapshotId) AS ClientStructureSnapshotId
# MAGIC     , LegalEntityClientId
# MAGIC   FROM CaseService_case_LegalEntityClientStructureSnapshot
# MAGIC   GROUP BY LegalEntityClientId
# MAGIC ) t9 ON t3.ClientStructureSnapshotId = t9.ClientStructureSnapshotId AND t3.LegalEntityClientId = t9.LegalEntityClientId
# MAGIC
# MAGIC LEFT JOIN CaseService_case_OtherRelationshipComponent t5 ON t4.RelationshipId = t5.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_IntegratorRelationshipComponent t6 ON t4.RelationshipId = t6.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_GuarantorRelationshipComponent t7 ON t4.RelationshipId = t7.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_AgentRelationshipComponent t8 ON t4.RelationshipId = t8.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_LimitedPartnerRelationshipComponent t13 ON t4.RelationshipId = t13.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_FundManagedByRelationshipComponent t14 ON t4.RelationshipId = t14.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_SpvParticipantRelationshipComponent t15 ON t4.RelationshipId = t15.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_DirectorshipRelationshipComponent t16 ON t4.RelationshipId = t16.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_SettlorFounderRelationshipComponent t17 ON t4.RelationshipId = t17.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_BeneficiaryRelationshipComponent t18 ON t4.RelationshipId = t18.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_OriginatorRelationshipComponent t19 ON t4.RelationshipId = t19.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_SubAccountOfRelationshipComponent t20 ON t4.RelationshipId = t20.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_TrusteeRelationshipComponent t21 ON t4.RelationshipId = t21.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_ProtectorRelationshipComponent t22 ON t4.RelationshipId = t22.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_CddRuleCertifierRelationshipComponent t23 ON t4.RelationshipId = t23.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_AuthorisedRepresentativeRelationshipComponent t24 ON t4.RelationshipId = t24.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_BranchOfRelationshipComponent t25 ON t4.RelationshipId = t25.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_CertificateHolderRelationshipComponent t26 ON t4.RelationshipId = t26.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_GeneralPartnerRelationshipComponent t27 ON t4.RelationshipId = t27.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_UboThroughReasonShareholdingRelationshipComponent t28 ON t4.RelationshipId = t28.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_EndOfChainOwnershipShareholdingRelationshipComponent t29 ON t4.RelationshipId = t29.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_UboThroughReasonRelationshipComponent t32 ON t4.RelationshipId = t32.RelationshipIdentity
# MAGIC
# MAGIC LEFT JOIN CaseService_case_RelatedLegalEntityParty t30 ON t4.ParentIdentity = t30.RelatedLegalEntityPartyId
# MAGIC LEFT JOIN CaseService_case_Address t40 ON t30.RegisteredAddressId = t40.AddressId
# MAGIC LEFT JOIN CaseService_dbo_Country as t41 ON t40.CountryReferenceId = t41.Id
# MAGIC LEFT JOIN CaseService_case_RelatedNaturalPersonParty t31 ON t4.ParentIdentity = t31.RelatedNaturalPersonPartyId
# MAGIC -- country of residence
# MAGIC LEFT JOIN CaseService_case_Address t42 ON t31.AddressId = t42.AddressId
# MAGIC LEFT JOIN CaseService_dbo_Country t43 ON t42.CountryReferenceId = t43.Id
# MAGIC -- nationality
# MAGIC LEFT JOIN CaseService_case_RelatedNaturalPersonPartyNationality t44 ON t31.RelatedNaturalPersonPartyId = t44.RelatedNaturalPersonPartyId
# MAGIC LEFT JOIN CaseService_dbo_Country t45 ON t44.CountryReferenceId = t45.Id
# MAGIC -- join parent on case_legalentityclient
# MAGIC LEFT JOIN CaseService_case_LegalEntityClient t34 ON t4.ParentIdentity = CAST(t34.Id AS STRING)
# MAGIC LEFT JOIN CaseService_case_Address t46 ON t34.RegisteredAddressId = t46.AddressId
# MAGIC LEFT JOIN CaseService_dbo_Country t47 ON t46.CountryReferenceId = t47.Id
# MAGIC LEFT JOIN CaseService_case_EndOfChainOwnershipReasonReference t35 ON t29.EndOfChainOwnershipEntityReferenceId = t35.id
# MAGIC LEFT JOIN CaseService_case_UboThroughReasonReference t36 ON t32.UboThroughReasonReferenceId = t36.Id
# MAGIC -- citizenship
# MAGIC LEFT JOIN CaseService_case_RelatedNaturalPersonPartyCitizenship t48 ON t31.RelatedNaturalPersonPartyId = t48.RelatedNaturalPersonPartyId
# MAGIC LEFT JOIN CaseService_dbo_Country t49 ON t48.CountryReferenceId = t49.Id
# MAGIC -- aliases
# MAGIC LEFT JOIN CaseService_case_RelatedNaturalPersonPartyAlias t50 ON t31.RelatedNaturalPersonPartyId = t50.RelatedNaturalPersonPartyId
# MAGIC LEFT JOIN CaseService_case_RelatedLegalEntityPartyTradingName t51 ON t30.RelatedLegalEntityPartyId = t51.RelatedLegalEntityPartyId
# MAGIC LEFT JOIN CaseService_case_LegalEntityTradingName t52 ON t1.CompletedId = t52.LegalEntityid
# MAGIC
# MAGIC WHERE t1.SourceSystemReference = 'GCOB_LegalEntity'
# MAGIC
# MAGIC GROUP BY
# MAGIC   t1.UniqueGcobId
# MAGIC   , t2.CaseId
# MAGIC   , t3.LegalEntityClientId
# MAGIC   , t3.ClientStructureSnapshotId
# MAGIC   , t4.ChildIdentity
# MAGIC   , t4.ChildType
# MAGIC   , t4.ParentIdentity
# MAGIC   , t4.ParentType
# MAGIC   , t4.RelationshipId
# MAGIC   , t5.relationshipidentity
# MAGIC   , t5.explanation
# MAGIC   , t6.relationshipidentity
# MAGIC   , t7.relationshipidentity
# MAGIC   , t7.GuarantorRelationshipType
# MAGIC   , t30.`Identity`
# MAGIC   , t31.`Identity`
# MAGIC   , t34.GcobId
# MAGIC   , t30.FullLegalName
# MAGIC   , t31.FirstName
# MAGIC   , t31.LastName
# MAGIC   , t34.FullLegalName
# MAGIC   , t31.PoliticallyExposedPersonStatusId
# MAGIC   , t41.Name
# MAGIC   , t43.Name
# MAGIC   , t47.Name
# MAGIC   , t8.relationshipidentity
# MAGIC   , t13.relationshipidentity
# MAGIC   , t14.relationshipidentity
# MAGIC   , t15.relationshipidentity
# MAGIC   , t16.relationshipidentity
# MAGIC   , t17.relationshipidentity
# MAGIC   , t18.relationshipidentity
# MAGIC   , t19.relationshipidentity
# MAGIC   , t20.relationshipidentity
# MAGIC   , t21.relationshipidentity
# MAGIC   , t22.relationshipidentity
# MAGIC   , t23.relationshipidentity
# MAGIC   , t24.relationshipidentity
# MAGIC   , t24.AuthorisedRepresentativeAuthority
# MAGIC   , t25.relationshipidentity
# MAGIC   , t26.relationshipidentity
# MAGIC   , t27.relationshipidentity
# MAGIC   , t28.relationshipidentity
# MAGIC   , t28.ShareholdingPercentage
# MAGIC   , t28.VotingRightPercentage
# MAGIC   , t28.ContextIdentity
# MAGIC   , t29.relationshipidentity
# MAGIC   , t29.ShareholdingPercentage
# MAGIC   , t29.VotingRightPercentage
# MAGIC   , t29.EndOfChainOwnershipEntityReferenceId
# MAGIC   , t32.relationshipidentity
# MAGIC   , t32.ContextIdentity
# MAGIC   , t32.UboThroughReasonReferenceId
# MAGIC   , t35.Description
# MAGIC   , t36.Description
# MAGIC   , t31.FullNameInLocalLanguage
# MAGIC   , t31.DateOfBirthString

# COMMAND ----------

# DBTITLE 1,rp_source_np
# THIS HAS TO BE RUN SEPARATELY AND THE DATASET SHOULD BE APPENDED TOGETHER AT THE END, OTHERWISE THE ALGO WILL CONFUSE CLIENTID!!!!!!!!!!!!!!!!!!!!!!!!

# %sql
# CREATE OR REPLACE TEMPORARY VIEW rp_source_np AS

# SELECT DISTINCT
#   t1.UniqueGcobId
#   , t2.CaseId
#   , t2.SourceClient
#   , t3.ClientStructureSnapshotId
#   , t4.ChildIdentity
#   , t4.ChildType
#   , t4.ParentIdentity
#   , t4.ParentType
#   , t4.RelationshipId
#   , CASE
#       WHEN t4.ParentType = 1 THEN CONCAT('RLE', t30.`Identity`)
#       WHEN t4.ParentType = 2 THEN CONCAT('RNP', t31.`Identity`)
#       WHEN t4.ParentType = 0 THEN CAST(t1.UniqueGcobId AS STRING)
#       ELSE t4.ParentIdentity
#     END AS ParentIdentityExplicit
#   , CASE
#       WHEN t4.ParentType = 1 THEN t30.FullLegalName
#       WHEN t4.ParentType = 2 THEN CONCAT(t31.FirstName, ' ', t31.LastName)
#       ELSE CONCAT(t34.FirstName, ' ', t34.MiddleName, ' ', t34.LastName)
#     END AS ParentIdentityName
#   , CONCAT_WS(', ',
#       SORT_ARRAY(
#         COLLECT_SET(
#           CASE
#             WHEN t4.ParentType = 1 THEN t51.TradingName
#             WHEN t4.ParentType = 2 THEN CONCAT(t50.FirstName, ' ', t50.LastName)
#             WHEN t4.ParentType = 0 THEN t52.TradingName
#           END
#         )
#       )
#     ) AS ParentIdentityExplicitAliases
#   , CASE
#       WHEN t4.ParentType = 2 AND t31.PoliticallyExposedPersonStatusId IN (1, 2, 4, 5) THEN 1
#     END AS PEPFlag
#   , CASE
#       WHEN t4.ParentType = 1 THEN t41.Name
#       WHEN t4.ParentType = 2 THEN t43.Name
#       WHEN t4.ParentType = 0 THEN t47.Name
#     END AS RegisteredCountry
#   , CONCAT_WS(', ',
#       SORT_ARRAY(
#         COLLECT_SET(
#           CASE
#             WHEN t4.ParentType = 2 THEN t45.Name
#           END
#         )
#       ) 
#     ) AS NationalityRNP
#   , CONCAT_WS(', ',
#       SORT_ARRAY(
#         COLLECT_SET(
#           CASE
#             WHEN t4.ParentType = 2 THEN t49.Name
#           END
#         )
#       )
#     ) AS CitizenshipRNP
#   , t5.relationshipidentity AS OtherId
#   , t5.explanation AS OtherRelationshipExplanation
#   , t6.relationshipidentity AS IntegratorId
#   , t7.relationshipidentity AS GuarantorId
#   , t7.GuarantorRelationshipType AS GuarantorRelationshipType
#   , t8.relationshipidentity AS AgentId
#   , t13.relationshipidentity AS LimitedPartnerId
#   , t14.relationshipidentity AS FundmanagerId
#   , t15.relationshipidentity AS SpvId
#   , t16.relationshipidentity AS DirectorId
#   , t17.relationshipidentity AS SettleOrFounderId
#   , t18.relationshipidentity AS BeneficiaryId
#   , t19.relationshipidentity AS OriginatorId
#   , t20.relationshipidentity AS SubaccountId
#   , t21.relationshipidentity AS TrusteeId
#   , t22.relationshipidentity AS ProtectorId
#   , t23.relationshipidentity AS CDDRuleCertifierId
#   , t24.relationshipidentity AS AuthorisedRepresentativeId
#   , t24.AuthorisedRepresentativeAuthority AS ArAuthorisedRepresentativeAuthority
#   , t25.relationshipidentity AS BranchOfId
#   , t26.relationshipidentity AS CertificateHolderId
#   , t27.relationshipidentity AS GeneralPartnerId
#   , t28.relationshipidentity AS UboThroughReasonShareholdingId
#   , t28.ShareholdingPercentage AS ShShareholdingPercentage
#   , t28.VotingRightPercentage AS ShVotingRightPercentage
#   , t28.ContextIdentity AS ShContextIdentity
#   , t29.relationshipidentity AS EndOfChainOwnershipShareholdingId
#   , t29.ShareholdingPercentage AS EocShareholdingPercentage
#   , t29.VotingRightPercentage AS EocVotingRightPercentage
#   , t29.EndOfChainOwnershipEntityReferenceId AS EocEndOfChainOwnershipEntityReferenceId
#   , t32.relationshipidentity AS UboThroughReasonShareholdingRelationshipId
#   , t32.ContextIdentity AS UtrContextIdentity
#   , t32.UboThroughReasonReferenceId AS UtrUboThroughReasonReferenceId
#   , t35.Description AS EocEndOfChainOwnershipReasonDescription
#   , t36.Description AS UtrUboThroughReasonDescription
#   , t31.FullNameInLocalLanguage
#   , t31.dateofbirthstring

# FROM radar.clients t1

# LEFT JOIN radar.cases t2 ON t1.CompletedId = t2.ClientId AND t1.UniqueGcobId = t2.UniqueGcobId

# LEFT JOIN CaseService_NaturalPerson_ClientStructureSnapshot t3 ON t1.CompletedId = t3.NaturalPersonClientId
# LEFT JOIN CaseService_snapshot_ClientStructureSnapshotRelationshipDetail t4 ON t3.ClientStructureSnapshotId = t4.ClientStructureSnapshotId
# INNER JOIN (
#   SELECT
#     MAX(ClientStructureSnapshotId) AS ClientStructureSnapshotId
#     , NaturalPersonClientId
#   FROM CaseService_NaturalPerson_ClientStructureSnapshot
#   GROUP BY NaturalPersonClientId
# ) t9 ON t3.ClientStructureSnapshotId = t9.ClientStructureSnapshotId AND t3.NaturalPersonClientId = t9.NaturalPersonClientId
# LEFT JOIN CaseService_case_OtherRelationshipComponent t5 ON t4.RelationshipId = t5.RelationshipIdentity
# LEFT JOIN CaseService_case_IntegratorRelationshipComponent t6 ON t4.RelationshipId = t6.RelationshipIdentity
# LEFT JOIN CaseService_case_GuarantorRelationshipComponent t7 ON t4.RelationshipId = t7.RelationshipIdentity
# LEFT JOIN CaseService_case_AgentRelationshipComponent t8 ON t4.RelationshipId = t8.RelationshipIdentity
# LEFT JOIN CaseService_case_LimitedPartnerRelationshipComponent t13 ON t4.RelationshipId = t13.RelationshipIdentity
# LEFT JOIN CaseService_case_FundManagedByRelationshipComponent t14 ON t4.RelationshipId = t14.RelationshipIdentity
# LEFT JOIN CaseService_case_SpvParticipantRelationshipComponent t15 ON t4.RelationshipId = t15.RelationshipIdentity
# LEFT JOIN CaseService_case_DirectorshipRelationshipComponent t16 ON t4.RelationshipId = t16.RelationshipIdentity
# LEFT JOIN CaseService_case_SettlorFounderRelationshipComponent t17 ON t4.RelationshipId = t17.RelationshipIdentity
# LEFT JOIN CaseService_case_BeneficiaryRelationshipComponent t18 ON t4.RelationshipId = t18.RelationshipIdentity
# LEFT JOIN CaseService_case_OriginatorRelationshipComponent t19 ON t4.RelationshipId = t19.RelationshipIdentity
# LEFT JOIN CaseService_case_SubAccountOfRelationshipComponent t20 ON t4.RelationshipId = t20.RelationshipIdentity
# LEFT JOIN CaseService_case_TrusteeRelationshipComponent t21 ON t4.RelationshipId = t21.RelationshipIdentity
# LEFT JOIN CaseService_case_ProtectorRelationshipComponent t22 ON t4.RelationshipId = t22.RelationshipIdentity
# LEFT JOIN CaseService_case_CddRuleCertifierRelationshipComponent t23 ON t4.RelationshipId = t23.RelationshipIdentity
# LEFT JOIN CaseService_case_AuthorisedRepresentativeRelationshipComponent t24 ON t4.RelationshipId = t24.RelationshipIdentity
# LEFT JOIN CaseService_case_BranchOfRelationshipComponent t25 ON t4.RelationshipId = t25.RelationshipIdentity
# LEFT JOIN CaseService_case_CertificateHolderRelationshipComponent t26 ON t4.RelationshipId = t26.RelationshipIdentity
# LEFT JOIN CaseService_case_GeneralPartnerRelationshipComponent t27 ON t4.RelationshipId = t27.RelationshipIdentity
# LEFT JOIN CaseService_case_UboThroughReasonShareholdingRelationshipComponent t28 ON t4.RelationshipId = t28.RelationshipIdentity
# LEFT JOIN CaseService_case_EndOfChainOwnershipShareholdingRelationshipComponent t29 ON t4.RelationshipId = t29.RelationshipIdentity
# LEFT JOIN CaseService_case_UboThroughReasonRelationshipComponent t32 ON t4.RelationshipId = t32.RelationshipIdentity
# LEFT JOIN CaseService_case_RelatedLegalEntityParty t30 ON t4.ParentIdentity = t30.RelatedLegalEntityPartyId
# LEFT JOIN CaseService_case_Address t40 ON t30.RegisteredAddressId = t40.AddressId
# LEFT JOIN CaseService_dbo_Country as t41 ON t40.CountryReferenceId = t41.Id
# LEFT JOIN CaseService_case_RelatedNaturalPersonParty t31 ON t4.ParentIdentity = t31.RelatedNaturalPersonPartyId
# -- country of residence
# LEFT JOIN CaseService_case_Address t42 ON t31.AddressId = t42.AddressId
# LEFT JOIN CaseService_dbo_Country t43 ON t42.CountryReferenceId = t43.Id
# -- nationality
# LEFT JOIN CaseService_case_RelatedNaturalPersonPartyNationality t44 ON t31.RelatedNaturalPersonPartyId = t44.RelatedNaturalPersonPartyId
# LEFT JOIN CaseService_dbo_Country t45 ON t44.CountryReferenceId = t45.Id

# LEFT JOIN CaseService_NaturalPerson_NaturalPersonClient t34 ON t4.ParentIdentity = t34.Id

# LEFT JOIN CaseService_case_Address t46 ON t34.ResidentialAddressId = t46.AddressId
# LEFT JOIN CaseService_dbo_Country t47 ON t46.CountryReferenceId = t47.Id
# LEFT JOIN CaseService_case_EndOfChainOwnershipReasonReference t35 ON t29.EndOfChainOwnershipEntityReferenceId = t35.id
# LEFT JOIN CaseService_case_UboThroughReasonReference t36 ON t32.UboThroughReasonReferenceId = t36.Id
# LEFT JOIN CaseService_case_RelatedNaturalPersonPartyCitizenship t48 ON t31.RelatedNaturalPersonPartyId = t48.RelatedNaturalPersonPartyId
# LEFT JOIN CaseService_dbo_Country t49 ON t48.CountryReferenceId = t49.Id
# LEFT JOIN CaseService_case_RelatedNaturalPersonPartyAlias t50 ON t31.RelatedNaturalPersonPartyId = t50.RelatedNaturalPersonPartyId
# LEFT JOIN CaseService_case_RelatedLegalEntityPartyTradingName t51 ON t30.RelatedLegalEntityPartyId = t51.RelatedLegalEntityPartyId
# LEFT JOIN CaseService_case_LegalEntityTradingName t52 ON CAST(t1.CompletedId AS STRING) = t52.LegalEntityid

# WHERE t1.SourceSystemReference = 'GCOB_NP-NPPC'

# GROUP BY
#   t1.UniqueGcobId
#   , t2.CaseId
#   , t2.SourceClient
#   , t3.NaturalPersonClientId
#   , t3.ClientStructureSnapshotId
#   , t4.ChildIdentity
#   , t4.ChildType
#   , t4.ParentIdentity
#   , t4.ParentType
#   , t4.RelationshipId
#   , t31.FirstName
#   , t31.LastName
#   , t30.`Identity`
#   , t31.`Identity`
#   , t30.FullLegalName
#   , t31.PoliticallyExposedPersonStatusId
#   , t41.Name
#   , t43.Name
#   , t47.Name
#   , t5.relationshipidentity
#   , t5.explanation
#   , t6.relationshipidentity
#   , t7.relationshipidentity
#   , t7.GuarantorRelationshipType
#   , t8.relationshipidentity
#   , t13.relationshipidentity
#   , t14.relationshipidentity
#   , t15.relationshipidentity
#   , t16.relationshipidentity
#   , t17.relationshipidentity
#   , t18.relationshipidentity
#   , t19.relationshipidentity
#   , t20.relationshipidentity
#   , t21.relationshipidentity
#   , t22.relationshipidentity
#   , t23.relationshipidentity
#   , t24.relationshipidentity
#   , t24.AuthorisedRepresentativeAuthority
#   , t25.relationshipidentity
#   , t26.relationshipidentity
#   , t27.relationshipidentity
#   , t28.relationshipidentity
#   , t28.ShareholdingPercentage
#   , t28.VotingRightPercentage
#   , t28.ContextIdentity
#   , t29.relationshipidentity
#   , t29.ShareholdingPercentage
#   , t29.VotingRightPercentage
#   , t29.EndOfChainOwnershipEntityReferenceId
#   , t32.relationshipidentity
#   , t32.ContextIdentity
#   , t32.UboThroughReasonReferenceId
#   , t35.Description
#   , t36.Description
#   , t31.FullNameInLocalLanguage
#   , t31.dateofbirthstring
#   , t34.FirstName
#   , t34.MiddleName
#   , t34.LastName

# COMMAND ----------

# DBTITLE 1,rp_source_mtos
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW rp_source_mtos AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   CONCAT(t4.ChildIdentity, '-', t4.ChildType) AS Child
# MAGIC   , CONCAT(t4.ParentIdentity, '-', t4.ParentType) AS Parent
# MAGIC   , t4.ChildIdentity
# MAGIC   , t4.ChildType
# MAGIC   , t4.ParentIdentity
# MAGIC   , t4.ParentType
# MAGIC   , t4.Id
# MAGIC
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t4.ParentType = 1 THEN CONCAT('RLE', t30.`Identity`)
# MAGIC       WHEN t4.ParentType = 2 THEN CONCAT('RNP', t31.`Identity`)
# MAGIC       WHEN t4.ParentType = 0 THEN CAST(t466.GcobId AS STRING)
# MAGIC       ELSE t4.ParentIdentity
# MAGIC     END AS ParentIdentityExplicit
# MAGIC   , CASE
# MAGIC       WHEN t4.ParentType = 1 THEN t30.FullLegalName
# MAGIC       WHEN t4.ParentType = 2 THEN CONCAT(t31.FirstName, ' ', t31.LastName)
# MAGIC       ELSE t466.FullLegalName
# MAGIC     END AS ParentIdentityName
# MAGIC   , CONCAT_WS(', ',
# MAGIC       SORT_ARRAY(
# MAGIC         COLLECT_SET(
# MAGIC           CASE
# MAGIC             WHEN t4.ParentType = 1 THEN t51.TradingName
# MAGIC             WHEN t4.ParentType = 2 THEN CONCAT(t50.FirstName, ' ', t50.LastName)
# MAGIC             WHEN t4.ParentType = 0 THEN t52.TradingName
# MAGIC           END
# MAGIC         )
# MAGIC       )
# MAGIC     ) AS ParentIdentityExplicitAliases
# MAGIC   , CASE
# MAGIC       WHEN t4.ParentType = 2 AND t31.PoliticallyExposedPersonStatusId IN (1,2,4,5) THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS PEPFlag
# MAGIC   , CASE
# MAGIC       WHEN t4.ParentType = 1 THEN t41.Name
# MAGIC       WHEN t4.ParentType = 2 THEN t43.Name
# MAGIC       WHEN t4.ParentType = 0 THEN t47.Name
# MAGIC     END AS RegisteredCountry
# MAGIC   , CONCAT_WS(', ',
# MAGIC       SORT_ARRAY(
# MAGIC         COLLECT_SET(
# MAGIC           CASE
# MAGIC             WHEN t4.ParentType = 2 THEN t45.Name
# MAGIC           END
# MAGIC         )
# MAGIC       ) 
# MAGIC     ) AS NationalityRNP
# MAGIC   , CONCAT_WS(', ',
# MAGIC       SORT_ARRAY(
# MAGIC         COLLECT_SET(
# MAGIC           CASE
# MAGIC             WHEN t4.ParentType = 2 THEN t49.Name
# MAGIC           END
# MAGIC         )
# MAGIC       )
# MAGIC     ) AS CitizenshipRNP
# MAGIC   , t5.relationshipidentity AS OtherId
# MAGIC   , t5.explanation AS OtherRelationshipExplanation
# MAGIC   , t6.relationshipidentity AS IntegratorId
# MAGIC   , t7.relationshipidentity AS GuarantorId
# MAGIC   , t7.GuarantorRelationshipType AS GuarantorRelationshipType
# MAGIC   , t8.relationshipidentity AS AgentId
# MAGIC   , t13.relationshipidentity AS LimitedPartnerId
# MAGIC   , t14.relationshipidentity AS FundmanagerId
# MAGIC   , t15.relationshipidentity AS SpvId
# MAGIC   , t16.relationshipidentity AS DirectorId
# MAGIC   , t17.relationshipidentity AS SettleOrFounderId
# MAGIC   , t18.relationshipidentity AS BeneficiaryId
# MAGIC   , t19.relationshipidentity AS OriginatorId
# MAGIC   , t20.relationshipidentity AS SubaccountId
# MAGIC   , t21.relationshipidentity AS TrusteeId
# MAGIC   , t22.relationshipidentity AS ProtectorId
# MAGIC   , t23.relationshipidentity AS CDDRuleCertifierId
# MAGIC   , t24.relationshipidentity AS AuthorisedRepresentativeId
# MAGIC   , t24.AuthorisedRepresentativeAuthority AS ArAuthorisedRepresentativeAuthority
# MAGIC   , t25.relationshipidentity AS BranchOfId
# MAGIC   , t26.relationshipidentity AS CertificateHolderId
# MAGIC   , t27.relationshipidentity AS GeneralPartnerId
# MAGIC   , t28.relationshipidentity AS UboThroughReasonShareholdingId
# MAGIC   , t28.ShareholdingPercentage AS ShShareholdingPercentage
# MAGIC   , t28.VotingRightPercentage AS ShVotingRightPercentage
# MAGIC   , t28.ContextIdentity AS ShContextIdentity
# MAGIC   , t29.relationshipidentity AS EndOfChainOwnershipShareholdingId
# MAGIC   , t29.ShareholdingPercentage AS EocShareholdingPercentage
# MAGIC   , t29.VotingRightPercentage AS EocVotingRightPercentage
# MAGIC   , t29.EndOfChainOwnershipEntityReferenceId AS EocEndOfChainOwnershipEntityReferenceId
# MAGIC   , t32.relationshipidentity AS UboThroughReasonShareholdingRelationshipId
# MAGIC   , t32.ContextIdentity AS UtrContextIdentity
# MAGIC   , t32.UboThroughReasonReferenceId AS UtrUboThroughReasonReferenceId
# MAGIC   , t35.Description AS EocEndOfChainOwnershipReasonDescription
# MAGIC   , t36.Description AS UtrUboThroughReasonDescription
# MAGIC   , t31.FullNameInLocalLanguage
# MAGIC   , t31.dateofbirthstring
# MAGIC
# MAGIC
# MAGIC FROM CaseService_case_Relationship t4
# MAGIC LEFT JOIN CaseService_case_OtherRelationshipComponent t5 ON t4.Id = t5.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_IntegratorRelationshipComponent t6 ON t4.Id = t6.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_GuarantorRelationshipComponent t7 ON t4.Id = t7.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_AgentRelationshipComponent t8 ON t4.Id = t8.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_LimitedPartnerRelationshipComponent t13 ON t4.Id = t13.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_FundManagedByRelationshipComponent t14 ON t4.Id = t14.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_SpvParticipantRelationshipComponent t15 ON t4.Id = t15.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_DirectorshipRelationshipComponent t16 ON t4.Id = t16.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_SettlorFounderRelationshipComponent t17 ON t4.Id = t17.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_BeneficiaryRelationshipComponent t18 ON t4.Id = t18.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_OriginatorRelationshipComponent t19 ON t4.Id = t19.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_SubAccountOfRelationshipComponent t20 ON t4.Id = t20.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_TrusteeRelationshipComponent t21 ON t4.Id = t21.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_ProtectorRelationshipComponent t22 ON t4.Id = t22.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_CddRuleCertifierRelationshipComponent t23 ON t4.Id = t23.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_AuthorisedRepresentativeRelationshipComponent t24 ON t4.Id = t24.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_BranchOfRelationshipComponent t25 ON t4.Id = t25.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_CertificateHolderRelationshipComponent t26 ON t4.Id = t26.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_GeneralPartnerRelationshipComponent t27 ON t4.Id = t27.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_UboThroughReasonShareholdingRelationshipComponent t28 ON t4.Id = t28.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_EndOfChainOwnershipShareholdingRelationshipComponent t29 ON t4.Id = t29.RelationshipIdentity
# MAGIC LEFT JOIN CaseService_case_UboThroughReasonRelationshipComponent t32 ON t4.Id = t32.RelationshipIdentity
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC   SELECT 
# MAGIC     rle.`Identity`
# MAGIC     , rle.FullLegalName 
# MAGIC     , rle.RegisteredAddressId
# MAGIC   FROM CaseService_case_RelatedLegalEntityParty rle
# MAGIC   JOIN (
# MAGIC     SELECT 
# MAGIC       `Identity`
# MAGIC       , MAX(`Version`) AS `Version`
# MAGIC     FROM CaseService_case_RelatedLegalEntityParty
# MAGIC     WHERE RegisteredAddressId IS NOT NULL
# MAGIC     GROUP BY `Identity`
# MAGIC   ) latest
# MAGIC   ON rle.Identity = latest.Identity AND rle.Version = latest.Version
# MAGIC ) t30 ON t4.ParentIdentity = t30.`Identity`
# MAGIC
# MAGIC LEFT JOIN CaseService_case_Address t40 ON t30.RegisteredAddressId = t40.AddressId
# MAGIC LEFT JOIN CaseService_dbo_Country as t41 ON t40.CountryReferenceId = t41.Id
# MAGIC LEFT JOIN CaseService_case_RelatedNaturalPersonParty t31 ON t4.ParentIdentity = t31.`Identity` AND t31.Expired IS NULL
# MAGIC -- country of residence
# MAGIC LEFT JOIN CaseService_case_Address t42 ON t31.AddressId = t42.AddressId
# MAGIC LEFT JOIN CaseService_dbo_Country t43 ON t42.CountryReferenceId = t43.Id
# MAGIC -- nationality
# MAGIC LEFT JOIN CaseService_case_RelatedNaturalPersonPartyNationality t44 ON t31.RelatedNaturalPersonPartyId = t44.RelatedNaturalPersonPartyId
# MAGIC LEFT JOIN CaseService_dbo_Country t45 ON t44.CountryReferenceId = t45.Id
# MAGIC LEFT JOIN CaseService_case_LegalEntityClient t466 on t4.ParentIdentity = t466.Id and t466.RegisteredAddressId IS NOT NULL
# MAGIC LEFT JOIN CaseService_case_Address t46 ON t466.RegisteredAddressId = t46.AddressId
# MAGIC LEFT JOIN CaseService_dbo_Country t47 ON t46.CountryReferenceId = t47.Id
# MAGIC LEFT JOIN CaseService_case_EndOfChainOwnershipReasonReference t35 ON t29.EndOfChainOwnershipEntityReferenceId = t35.id
# MAGIC LEFT JOIN CaseService_case_UboThroughReasonReference t36 ON t32.UboThroughReasonReferenceId = t36.Id
# MAGIC -- citizenship
# MAGIC LEFT JOIN CaseService_case_RelatedNaturalPersonPartyCitizenship t48 ON t31.RelatedNaturalPersonPartyId = t48.RelatedNaturalPersonPartyId
# MAGIC LEFT JOIN CaseService_dbo_Country t49 ON t48.CountryReferenceId = t49.Id
# MAGIC -- aliases
# MAGIC LEFT JOIN CaseService_case_RelatedNaturalPersonPartyAlias t50 ON t31.RelatedNaturalPersonPartyId = t50.RelatedNaturalPersonPartyId
# MAGIC LEFT JOIN CaseService_case_RelatedLegalEntityPartyTradingName t51 ON t30.`Identity` = t51.RelatedLegalEntityPartyId
# MAGIC LEFT JOIN CaseService_case_LegalEntityTradingName t52 ON t4.ParentIdentity = t52.LegalEntityid
# MAGIC
# MAGIC GROUP BY
# MAGIC   t4.ChildIdentity
# MAGIC   , t4.ChildType
# MAGIC   , t4.ParentIdentity
# MAGIC   , t4.ParentType
# MAGIC   , t4.Id
# MAGIC   , t31.FirstName
# MAGIC   , t31.LastName
# MAGIC   , t466.GcobId 
# MAGIC   , t30.`Identity`
# MAGIC   , t31.`Identity`
# MAGIC   , t30.FullLegalName
# MAGIC   , t466.FullLegalName
# MAGIC   , t31.PoliticallyExposedPersonStatusId
# MAGIC   , t41.Name
# MAGIC   , t43.Name
# MAGIC   , t47.Name
# MAGIC   , t5.relationshipidentity
# MAGIC   , t5.explanation
# MAGIC   , t6.relationshipidentity
# MAGIC   , t7.relationshipidentity
# MAGIC   , t7.GuarantorRelationshipType
# MAGIC   , t8.relationshipidentity
# MAGIC   , t13.relationshipidentity
# MAGIC   , t14.relationshipidentity
# MAGIC   , t15.relationshipidentity
# MAGIC   , t16.relationshipidentity
# MAGIC   , t17.relationshipidentity
# MAGIC   , t18.relationshipidentity
# MAGIC   , t19.relationshipidentity
# MAGIC   , t20.relationshipidentity
# MAGIC   , t21.relationshipidentity
# MAGIC   , t22.relationshipidentity
# MAGIC   , t23.relationshipidentity
# MAGIC   , t24.relationshipidentity
# MAGIC   , t24.AuthorisedRepresentativeAuthority
# MAGIC   , t25.relationshipidentity
# MAGIC   , t26.relationshipidentity
# MAGIC   , t27.relationshipidentity
# MAGIC   , t28.relationshipidentity
# MAGIC   , t28.ShareholdingPercentage
# MAGIC   , t28.VotingRightPercentage
# MAGIC   , t28.ContextIdentity
# MAGIC   , t29.relationshipidentity
# MAGIC   , t29.ShareholdingPercentage
# MAGIC   , t29.VotingRightPercentage
# MAGIC   , t29.EndOfChainOwnershipEntityReferenceId
# MAGIC   , t32.relationshipidentity
# MAGIC   , t32.ContextIdentity
# MAGIC   , t32.UboThroughReasonReferenceId
# MAGIC   , t35.Description
# MAGIC   , t36.Description
# MAGIC   , t31.FullNameInLocalLanguage
# MAGIC   , t31.dateofbirthstring

# COMMAND ----------

# DBTITLE 1,GlobalAndInvolvedFiles
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GlobalAndInvolvedFiles AS
# MAGIC
# MAGIC WITH LocationMappingRegion AS (
# MAGIC   SELECT 
# MAGIC     `Name`
# MAGIC     , CASE  
# MAGIC         WHEN `Name` IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya') THEN 'E&A'
# MAGIC         WHEN `Name` IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') THEN 'Asia'
# MAGIC         WHEN `Name` IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') THEN 'North America'
# MAGIC         WHEN `Name` IN ('Rabobank Chile', 'Rabobank Brazil') THEN 'South America'
# MAGIC         WHEN `Name` IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') THEN 'RANZ'
# MAGIC         WHEN `Name` IN ('Rabobank Foundation') THEN 'Rabobank Foundation'
# MAGIC       END AS `Region`
# MAGIC   FROM CaseService_dbo_RabobankEntity
# MAGIC GROUP BY `Name`
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SourceClient
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC         CASE
# MAGIC           WHEN t3.Region <> t4.Region
# MAGIC             OR t3.Region <> t5.Region
# MAGIC             AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress')
# MAGIC           THEN 1
# MAGIC           ELSE 0
# MAGIC         END
# MAGIC       ) > 0 THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS GlobalFiles
# MAGIC
# MAGIC   /*
# MAGIC     Location involved
# MAGIC   */
# MAGIC   , CASE 
# MAGIC       WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Netherlands' OR t2.BookingEntityLocation = 'Rabobank Netherlands') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC         THEN 1 ELSE 0 
# MAGIC       END) > 0 THEN 1 ELSE 0
# MAGIC     END AS NetherlandsInvolved
# MAGIC   , CASE 
# MAGIC       WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Paris' OR t2.BookingEntityLocation = 'Rabobank Paris') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC         THEN 1 ELSE 0 
# MAGIC       END) > 0 THEN 1 ELSE 0
# MAGIC     END AS ParisInvolved
# MAGIC   , CASE
# MAGIC       WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Antwerp' OR t2.BookingEntityLocation = 'Rabobank Antwerp') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC         THEN 1 ELSE 0 
# MAGIC       END) > 0 THEN 1 ELSE 0
# MAGIC     END AS AntwerpInvolved
# MAGIC   , CASE 
# MAGIC       WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Madrid' OR t2.BookingEntityLocation = 'Rabobank Madrid') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC         THEN 1 ELSE 0 
# MAGIC       END) > 0 THEN 1 ELSE 0
# MAGIC     END AS MadridInvolved
# MAGIC   , CASE 
# MAGIC       WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Milan' OR t2.BookingEntityLocation = 'Rabobank Milan') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC         THEN 1 ELSE 0 
# MAGIC       END) > 0 THEN 1 ELSE 0
# MAGIC     END AS MilanInvolved
# MAGIC   , CASE 
# MAGIC       WHEN SUM(CASE WHEN (t2.ProductOfferingLocation = 'Rabobank Frankfurt' OR t2.BookingEntityLocation = 'Rabobank Frankfurt') AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC         THEN 1 ELSE 0 
# MAGIC       END) > 0 THEN 1 ELSE 0
# MAGIC     END AS FrankfurtInvolved
# MAGIC
# MAGIC   /*
# MAGIC     Region involved
# MAGIC   */
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC         CASE 
# MAGIC           WHEN (t2.ProductOfferingLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya') 
# MAGIC           OR t2.BookingEntityLocation IN ('Rabobank Netherlands', 'Rabobank London', 'Rabobank Antwerp', 'Rabobank Dublin', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Turkey', 'Rabobank Argentina', 'Rabobank Kenya')) AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC           THEN 1 ELSE 0
# MAGIC         END
# MAGIC       ) > 0 
# MAGIC       THEN 1 ELSE 0
# MAGIC     END AS EAInvolved
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC         CASE 
# MAGIC           WHEN (t2.ProductOfferingLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)') 
# MAGIC           OR t2.BookingEntityLocation IN ('Rabobank New York', 'Rabobank Canada', 'Rabobank Canada (RCBR)', 'Rabo Securities USA, Inc. (RSEC)', 'Rabo Securities Canada, Inc. (RSCI)', 'Rabobank - USA Rabo AgriFinance', 'Rabobank Canada(Rural)')) AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC           THEN 1 ELSE 0 
# MAGIC         END
# MAGIC       ) > 0 
# MAGIC       THEN 1 ELSE 0
# MAGIC     END AS NAInvolved
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC           CASE 
# MAGIC             WHEN (t2.ProductOfferingLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia') 
# MAGIC             OR t2.BookingEntityLocation IN ('Rabobank Hong Kong', 'Rabobank HongKong', 'Rabobank China', 'Rabobank India', 'Rabobank Singapore', 'Rabobank Indonesia'))
# MAGIC             AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC             THEN 1 ELSE 0 
# MAGIC           END
# MAGIC       ) > 0 
# MAGIC       THEN 1 ELSE 0
# MAGIC     END AS AsiaInvolved
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC           CASE 
# MAGIC             WHEN (t2.ProductOfferingLocation IN ('Rabobank Chile', 'Rabobank Brazil') 
# MAGIC             OR t2.BookingEntityLocation IN ('Rabobank Chile', 'Rabobank Brazil')) AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC             THEN 1 ELSE 0
# MAGIC           END
# MAGIC       ) > 0 
# MAGIC       THEN 1 ELSE 0
# MAGIC     END AS SAInvolved
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN SUM(
# MAGIC         CASE 
# MAGIC           WHEN (t2.ProductOfferingLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS') 
# MAGIC           OR t2.BookingEntityLocation IN ('Rabobank New Zealand', 'Rabobank Australia', 'Rabobank - RANZ Country Banking and ROS')) AND t2.ProductLifecyclestatus IN ('Active', 'Exit In Progress') 
# MAGIC           THEN 1 ELSE 0
# MAGIC         END
# MAGIC       ) > 0 
# MAGIC       THEN 1 ELSE 0
# MAGIC     END AS RANZInvolved
# MAGIC
# MAGIC
# MAGIC FROM radar.clients t1
# MAGIC LEFT JOIN party_products_and_services t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN LocationMappingRegion t3 ON t1.GlobalClientOwnerLocation = t3.Name
# MAGIC LEFT JOIN LocationMappingRegion t4 ON t2.BookingEntityLocation = t4.Name
# MAGIC LEFT JOIN LocationMappingRegion t5 ON t2.ProductOfferingLocation = t5.Name
# MAGIC GROUP BY 1,2

# COMMAND ----------

# DBTITLE 1,rp_clients
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW rp_clients AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.FullLegalName
# MAGIC   , t1.GlobalClientOwner
# MAGIC   , t1.GlobalClientOwnerLocation
# MAGIC   , t1.ClientLifeCycleName
# MAGIC   , t2.Assessmentinprogress AS LastAssessmentStartDate -- ADD THIS WHEN AVAILABLE!!
# MAGIC   , t1.GlobalReportingRegion
# MAGIC   , t1.SectorTeam
# MAGIC   , t1.GlobalKYCPortfolioNew
# MAGIC   , t1.GCDSID
# MAGIC   , t1.FIHubIndicator_Derived AS FIHubIndicator
# MAGIC   , t1.KYCGroup
# MAGIC   , CASE WHEN t1.BusinessLineName = 'GWPC - Trade & Commodity Finance' THEN 1 ELSE 0 END AS GWPC_TCF
# MAGIC   , CASE WHEN t1.BusinessLineName = 'GWPC - Export and Project Finance' THEN 1 ELSE 0 END AS GWPC_EPF
# MAGIC   , CASE WHEN t1.BusinessLineName = 'GWPC - Acquisition Finance' THEN 1 ELSE 0 END AS GWPC_AF
# MAGIC   , t1.TradeName AS TradingName
# MAGIC
# MAGIC   , t3.EAInvolved AS EuropeAndAfricaLeadOrInvolved
# MAGIC   , CASE 
# MAGIC       WHEN t1.GlobalClientOwnerLocation IN ('Rabobank Antwerp', 'Rabobank Milan', 'Rabobank Madrid', 'Rabobank Frankfurt', 'Rabobank Paris', 'Rabobank London', 
# MAGIC                                             'Rabobank Dublin', 'Rabobank China', 'Rabobank Hong Kong', 'Rabobank Singapore', 'Rabobank India') THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS EuropeHubLink
# MAGIC   , CASE
# MAGIC       WHEN t1.GlobalFiles = 0 AND t1.GlobalClientOwnerLocation = 'Rabobank Netherlands' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS NetherlandsOnlyFile
# MAGIC   , t3.NetherlandsInvolved AS NetherlandsLeadOrInvolved
# MAGIC   , t3.MilanInvolved AS MilanLeadOrInvolved
# MAGIC   , t3.ParisInvolved AS ParisLeadOrInvolved
# MAGIC   , t3.AntwerpInvolved AS AntwerpLeadOrInvolved
# MAGIC   , t3.MadridInvolved AS MadridLeadOrInvolved
# MAGIC   , t3.FrankfurtInvolved AS FrankfurtLeadOrInvolved
# MAGIC   , t3.AsiaInvolved AS AsiaLeadOrInvolved
# MAGIC
# MAGIC FROM radar.clients t1
# MAGIC LEFT JOIN radar.cases t2 ON t1.SourceClient = t2.SourceClient
# MAGIC LEFT JOIN GlobalAndInvolvedFiles t3 ON t1.SourceClient = t3.SourceClient
# MAGIC
# MAGIC WHERE t1.SourceSystemReference = 'GCOB_LegalEntity'

# COMMAND ----------

# define pyspark df from spark sql views
df_rp_source = spark.sql('SELECT * FROM rp_source WHERE ParentIdentityName IS NOT NULL')
df_rp_source_mtos = spark.sql('SELECT * FROM rp_source_mtos WHERE ParentIdentityName IS NOT NULL')
df_rp_clients = spark.sql('SELECT * FROM rp_clients')

# COMMAND ----------

# DBTITLE 1,unique identifier for rp algo
# define uniquely valued columns for child and parent for df_rp_source
df_rp_source = df_rp_source.withColumn('Child', F.concat_ws('-', df_rp_source['ChildIdentity'], df_rp_source['ChildType'].cast('string'), df_rp_source['LegalEntityClientId'].cast('string')))
df_rp_source = df_rp_source.withColumn('Parent', F.concat_ws('-', df_rp_source['ParentIdentity'], df_rp_source['ParentType'].cast('string'), df_rp_source['LegalEntityClientId'].cast('string')))

# not needed for mtos already in sql query

# COMMAND ----------

# DBTITLE 1,RelationshipType
from pyspark.sql import functions as F

replacements = {
  'LimitedPartnerId': 'Limited Partner',
  'FundmanagerId': 'Fund Manager',
  'GuarantorId': 'Guarantor',
  'SpvId': 'SPV',
  'DirectorId': 'Director',
  'SettleOrFounderId': 'Settle or Founder',
  'BeneficiaryId': 'Beneficiary',
  'OriginatorId': 'Originator',
  'SubaccountId': 'Subaccount',
  'TrusteeId': 'Trustee',
  'ProtectorId': 'Protector',
  'CDDRuleCertifierId': 'CDD Ruler Certifier',
  'OtherId': 'Other',
  'IntegratorId': 'Integrator',
  'AgentId': 'Agent',
  'AuthorisedRepresentativeId': 'Authorised Representative',
  'BranchOfId': 'Branch Of',
  'CertificateHolderId': 'Certificate Holder',
  'GeneralPartnerId': 'General Partner',
  'UboThroughReasonShareholdingId': 'Ubo Through Reason Shareholding',
  'EndOfChainOwnershipShareholdingId': 'End of Chain Ownership Shareholding',
  'UboThroughReasonShareholdingRelationshipId': 'Ubo Through Reason Shareholding Relationship'
}

relationship_types = [
    F.when(F.col(column).isNotNull(), value) for column, value in replacements.items()
]

# concatenate non-null values with a comma separator
df_rp_source = df_rp_source.withColumn(
    'RelationshipType',
    F.concat_ws(', ', *[condition for condition in relationship_types])
)

df_rp_source_mtos = df_rp_source_mtos.withColumn(
    'RelationshipType',
    F.concat_ws(', ', *[condition for condition in relationship_types])
)

# COMMAND ----------

# DBTITLE 1,ShareholdingPercentage
from pyspark.sql import functions as F

df_rp_source = df_rp_source.withColumn(
  'ShareholdingPercentage',
  F.coalesce(
    F.col('ShShareholdingPercentage'),
    F.col('EocShareholdingPercentage')
  )
)

df_rp_source_mtos = df_rp_source_mtos.withColumn(
  'ShareholdingPercentage',
  F.coalesce(
    F.col('ShShareholdingPercentage'),
    F.col('EocShareholdingPercentage')
  )
)

# COMMAND ----------

# DBTITLE 1,spark df to pandas df
import pandas as pd

df_rp_source_pd = df_rp_source.toPandas()
df_rp_source_mtos_pd = df_rp_source_mtos.toPandas()
df_rp_clients_pd = df_rp_clients.toPandas()

# COMMAND ----------

# DBTITLE 1,related parties main part
# define list of LegalEntityClientId
list_LegalEntityClientId = [str(x) for x in df_rp_source_pd.LegalEntityClientId.unique()]

# remove LegalEntityClientId of mtos
remove_mtos_LegalEntityClientId = str(df_rp_source_pd[df_rp_source_pd.UniqueGcobId.isin([48411, 15784, 5689])].LegalEntityClientId.unique())
list_LegalEntityClientId = [s for s in list_LegalEntityClientId if s not in remove_mtos_LegalEntityClientId]

all_other_rel = df_rp_source_pd[['LimitedPartnerId', 'FundmanagerId', 'SpvId', 'DirectorId',
  'SettleOrFounderId', 'BeneficiaryId', 'OriginatorId', 'SubaccountId',
  'TrusteeId', 'ProtectorId', 'CDDRuleCertifierId',
  'AuthorisedRepresentativeId', 'ArAuthorisedRepresentativeAuthority',
  'BranchOfId', 'CertificateHolderId', 'GeneralPartnerId',
  'UboThroughReasonShareholdingId', 'ShShareholdingPercentage',
  'ShVotingRightPercentage', 'ShContextIdentity',
  'EndOfChainOwnershipShareholdingId', 'EocShareholdingPercentage',
  'EocVotingRightPercentage', 'EocEndOfChainOwnershipEntityReferenceId',
  'UboThroughReasonShareholdingRelationshipId', 'UtrContextIdentity',
  'UtrUboThroughReasonReferenceId',
  'EocEndOfChainOwnershipReasonDescription',
  'UtrUboThroughReasonDescription']].count(axis=1) > 0

client_conditions = (df_rp_source_pd['ChildIdentity'].isin(list_LegalEntityClientId)) & (df_rp_source_pd['ChildType'] == 0)

exclusive_rel = df_rp_source_pd[['OtherId', 'IntegratorId', 'GuarantorId', 'AgentId']].count(axis=1) == 0

# exclude relationships on ['OtherId', 'IntegratorId', 'GuarantorId', 'AgentId']
mask = client_conditions & (exclusive_rel | all_other_rel)

start_nodes = set(df_rp_source_pd[mask].Parent)
final_nodes = start_nodes

depth = 0
while depth < 100:
  previous_len_final = len(final_nodes)

  if depth == 0:
    temp_nodes = start_nodes
  
  mask = (df_rp_source_pd['Child'].isin(temp_nodes))
  temp_nodes = set(df_rp_source_pd[mask].Parent)

  final_nodes.update(temp_nodes - final_nodes)

  if previous_len_final == len(final_nodes):
    print(previous_len_final, depth)
    break

  depth += 1

# additional parties tab in gcob
mask_ap = (df_rp_source_pd['ChildIdentity'].isin(list_LegalEntityClientId)) & (df_rp_source_pd['ChildType'] == 0) & (df_rp_source_pd[['OtherId', 'IntegratorId', 'GuarantorId', 'AgentId']].count(axis=1) > 0)
final_nodes.update(set(df_rp_source_pd[mask_ap].Parent))

df_relatedparties = pd.DataFrame(final_nodes, columns = ['RP'])

# COMMAND ----------

# DBTITLE 1,df_relatedparties_mtos
# define list of clientid for mtos
mtos_triples = [(row.ClientId, row.UniqueGcobId, row.LatestCaseId) for row in spark.sql('SELECT ClientId, UniqueGcobId, LatestCaseId  FROM radar.clients WHERE UniqueGcobId IN (48411, 15784, 5689)').collect()]

list_of_legalentityclientid = [triple[0] for triple in mtos_triples]

df_relatedparties_mtos = pd.DataFrame()

for client in list_of_legalentityclientid:

  mask = (df_rp_source_mtos_pd['ChildIdentity'] == client) & (df_rp_source_mtos_pd['ChildType'] == 0)

  start_nodes = set(df_rp_source_mtos_pd[mask].Parent)
  final_nodes = start_nodes

  depth = 0
  while depth < 100:
    previous_len_final = len(final_nodes)

    if depth == 0:
      temp_nodes = start_nodes
    
    mask = (df_rp_source_mtos_pd['Child'].isin(temp_nodes))
    temp_nodes = set(df_rp_source_mtos_pd[mask].Parent)

    final_nodes.update(temp_nodes - final_nodes)

    if previous_len_final == len(final_nodes):
      print(previous_len_final, depth)
      break

    depth += 1


  # add UniqueGcobId and CaseId
  df_temp = pd.DataFrame(final_nodes, columns = ['RP'])

  UniqueGcobId = next(triple[1] for triple in mtos_triples if triple[0] == client)
  CaseId = next(triple[2] for triple in mtos_triples if triple[0] == client)
  
  new_columns = {
        'UniqueGcobId': UniqueGcobId,
        'CaseId': CaseId
  }

  df_temp = df_temp.assign(**new_columns)

  # append to df_relatedparties_mtos
  df_relatedparties_mtos = pd.concat([df_relatedparties_mtos, df_temp], ignore_index=True)

# COMMAND ----------

# DBTITLE 1,map to to df_relatedparties_mtos
# map missing party information to df_relatedparties_mtos

# drop duplicated parent in subgroup parentidentityname for mapping
df_rp_source_mtos_pd_no_duplicates = df_rp_source_mtos_pd.drop_duplicates(subset=['Parent', 'ParentIdentityExplicit'])
# reset the index (if needed)
df_rp_source_mtos_pd_no_duplicates = df_rp_source_mtos_pd_no_duplicates.reset_index(drop=True)

df_relatedparties_mtos['RelatedPartyExplicit'] = df_relatedparties_mtos['RP'].map(df_rp_source_mtos_pd_no_duplicates.set_index('Parent')['ParentIdentityExplicit'])
df_relatedparties_mtos['RelatedPartyName'] = df_relatedparties_mtos['RP'].map(df_rp_source_mtos_pd_no_duplicates.set_index('Parent')['ParentIdentityName'])
df_relatedparties_mtos['RelationshipType'] = df_relatedparties_mtos['RP'].map(df_rp_source_mtos_pd_no_duplicates.set_index('Parent')['RelationshipType'])    
df_relatedparties_mtos['RegisteredCountry'] = df_relatedparties_mtos['RP'].map(df_rp_source_mtos_pd_no_duplicates.set_index('Parent')['RegisteredCountry'])
df_relatedparties_mtos['NationalityRNP'] = df_relatedparties_mtos['RP'].map(df_rp_source_mtos_pd_no_duplicates.set_index('Parent')['NationalityRNP'])
df_relatedparties_mtos['CitizenshipRNP'] = df_relatedparties_mtos['RP'].map(df_rp_source_mtos_pd_no_duplicates.set_index('Parent')['CitizenshipRNP'])
df_relatedparties_mtos['ParentIdentityExplicitAliases'] = df_relatedparties_mtos['RP'].map(df_rp_source_mtos_pd_no_duplicates.set_index('Parent')['ParentIdentityExplicitAliases'])
df_relatedparties_mtos['FullNameInLocalLanguage'] = df_relatedparties_mtos['RP'].map(df_rp_source_mtos_pd_no_duplicates.set_index('Parent')['FullNameInLocalLanguage'])
df_relatedparties_mtos['DateOfBirth'] = df_relatedparties_mtos['RP'].map(df_rp_source_mtos_pd_no_duplicates.set_index('Parent')['dateofbirthstring'])
df_relatedparties_mtos['PEPFlag'] = df_relatedparties_mtos['RP'].map(df_rp_source_mtos_pd_no_duplicates.set_index('Parent')['PEPFlag'])

# COMMAND ----------

# DBTITLE 1,map missing party information to df_relatedparties
# drop duplicated parent in subgroup LegalEntityClientId for mapping
df_rp_source_pd_no_duplicates = df_rp_source_pd.drop_duplicates(subset=['Parent', 'LegalEntityClientId'])
df_rp_source_pd_no_duplicates = df_rp_source_pd_no_duplicates.reset_index(drop=True)

# map additional party information from df_rp_source_pd_no_duplicates
df_relatedparties['UniqueGcobId'] = df_relatedparties['RP'].map(df_rp_source_pd_no_duplicates.set_index('Parent')['UniqueGcobId'])
df_relatedparties['CaseId'] = df_relatedparties['RP'].map(df_rp_source_pd_no_duplicates.set_index('Parent')['CaseId'])
df_relatedparties['RelatedPartyExplicit'] = df_relatedparties['RP'].map(df_rp_source_pd_no_duplicates.set_index('Parent')['ParentIdentityExplicit'])
df_relatedparties['RelatedPartyName'] = df_relatedparties['RP'].map(df_rp_source_pd_no_duplicates.set_index('Parent')['ParentIdentityName'])
df_relatedparties['RelationshipType'] = df_relatedparties['RP'].map(df_rp_source_pd_no_duplicates.set_index('Parent')['RelationshipType'])    
df_relatedparties['RegisteredCountry'] = df_relatedparties['RP'].map(df_rp_source_pd_no_duplicates.set_index('Parent')['RegisteredCountry'])
df_relatedparties['NationalityRNP'] = df_relatedparties['RP'].map(df_rp_source_pd_no_duplicates.set_index('Parent')['NationalityRNP'])
df_relatedparties['CitizenshipRNP'] = df_relatedparties['RP'].map(df_rp_source_pd_no_duplicates.set_index('Parent')['CitizenshipRNP'])
df_relatedparties['ParentIdentityExplicitAliases'] = df_relatedparties['RP'].map(df_rp_source_pd_no_duplicates.set_index('Parent')['ParentIdentityExplicitAliases'])
df_relatedparties['FullNameInLocalLanguage'] = df_relatedparties['RP'].map(df_rp_source_pd_no_duplicates.set_index('Parent')['FullNameInLocalLanguage'])
df_relatedparties['DateOfBirth'] = df_relatedparties['RP'].map(df_rp_source_pd_no_duplicates.set_index('Parent')['DateOfBirthString'])
df_relatedparties['PEPFlag'] = df_relatedparties['RP'].map(df_rp_source_pd_no_duplicates.set_index('Parent')['PEPFlag'])

# COMMAND ----------

# DBTITLE 1,combine df_relatedparties with df_relateadparties_mtos
# append df_relatedparties_mtos to df_relatedparties

# align the two dfs and append
df_relatedparties_mtos = df_relatedparties_mtos[df_relatedparties.columns]

df_relatedparties = pd.concat([df_relatedparties, df_relatedparties_mtos], ignore_index = True, sort=False)

# COMMAND ----------

# DBTITLE 1,map client information to every row
# map client information to party
df_relatedparties['FullLegalName'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['FullLegalName'])

# df_relatedparties['LastAssessmentStartDate'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['LastAssessmentStartDate'])

df_relatedparties['FIHubIndicator'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['FIHubIndicator'])
df_relatedparties['GWPC_TCF'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['GWPC_TCF'])
df_relatedparties['GWPC_EPF'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['GWPC_EPF'])
df_relatedparties['GWPC_AF'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['GWPC_AF'])

df_relatedparties['ClientLifecycleStatus'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['ClientLifeCycleName'])

df_relatedparties['GlobalClientOwner'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['GlobalClientOwner'])
df_relatedparties['GlobalClientOwnerLocation'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['GlobalClientOwnerLocation'])

df_relatedparties['SectorTeam'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['SectorTeam'])
df_relatedparties['GlobalKYCPortfolioNew'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['GlobalKYCPortfolioNew'])
df_relatedparties['GlobalReportingRegion'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['GlobalReportingRegion'])

df_relatedparties['NetherlandsLeadOrInvolved'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['NetherlandsLeadOrInvolved'])
df_relatedparties['NetherlandsOnlyFile'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['NetherlandsOnlyFile'])
df_relatedparties['EuropeAndAfricaLeadOrInvolved'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['EuropeAndAfricaLeadOrInvolved'])
df_relatedparties['AsiaLeadOrInvolved'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['AsiaLeadOrInvolved'])

df_relatedparties['MilanLeadOrInvolved'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['MilanLeadOrInvolved'])
df_relatedparties['FrankfurtLeadOrInvolved'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['FrankfurtLeadOrInvolved'])
df_relatedparties['ParisLeadOrInvolved'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['ParisLeadOrInvolved'])
df_relatedparties['MadridLeadOrInvolved'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['MadridLeadOrInvolved'])
df_relatedparties['AntwerpLeadOrInvolved'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['AntwerpLeadOrInvolved'])

df_relatedparties['KYCGroup'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['KYCGroup'])

df_relatedparties['TradingNames'] = df_relatedparties['UniqueGcobId'].map(df_rp_clients_pd.set_index('UniqueGcobId')['TradingName'])

# drop duplicates based on the subset UniqueGcobId, RelatedPartyExplicit, because all previous ids of parties are stored in snapshot
df_relatedparties = df_relatedparties.drop_duplicates(subset=['UniqueGcobId', 'RelatedPartyExplicit'])

# COMMAND ----------

# DBTITLE 1,fix different column data type issue
import pandas as pd

def clean_column(df, column, target_type, fill_value=None):
    if target_type == 'str':
        df[column] = df[column].astype(str).replace({'None': None})
    elif target_type == 'int':
        df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
    elif target_type == 'bool':
        df[column] = df[column].astype(bool)
    elif target_type == 'datetime':
        df[column] = pd.to_datetime(df[column], errors='coerce')
    
    # fill NaN values if specified
    if fill_value is not None:
        df[column].fillna(fill_value, inplace=True)
    return df

# string columns
for col in ['GlobalClientOwner', 'GlobalClientOwnerLocation', 'SectorTeam'
            , 'GlobalKYCPortfolioNew', 'GlobalReportingRegion', 'KYCGroup', 'TradingNames'
            , 'UniqueGcobId', 'RelatedPartyExplicit', 'RelatedPartyName', 'RegisteredCountry'
            , 'FullNameInLocalLanguage', 'FullLegalName', 'FIHubIndicator']:
    df_relatedparties = clean_column(df_relatedparties, col, 'str')

# boolean columns
for col in ['GWPC_TCF', 'GWPC_EPF', 'GWPC_AF', 'PEPFlag', 'NetherlandsLeadOrInvolved'
            , 'NetherlandsOnlyFile', 'EuropeAndAfricaLeadOrInvolved', 'AsiaLeadOrInvolved'
            , 'MilanLeadOrInvolved', 'FrankfurtLeadOrInvolved', 'ParisLeadOrInvolved'
            , 'MadridLeadOrInvolved', 'AntwerpLeadOrInvolved']:
    df_relatedparties = clean_column(df_relatedparties, col, 'bool')

# datetime columns
# df_relatedparties = clean_column(df_relatedparties, 'LastAssessmentStartDate', 'datetime')
df_relatedparties = clean_column(df_relatedparties, 'DateOfBirth', 'datetime')

# others
df_relatedparties = clean_column(df_relatedparties, 'CaseId', 'int')

# COMMAND ----------

len(df_relatedparties)

# COMMAND ----------

# DBTITLE 1,raise error if df is too small
if len(df_relatedparties) < 200000:
  raise Exception('df_relatedparties has less than 200k rows. Job stopped.')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.relatedparties

# COMMAND ----------

spark.createDataFrame(df_relatedparties).write.mode('overwrite').saveAsTable('radar.relatedparties')
