# Databricks notebook source
# import pandas as pd
import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

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

Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')

load_dts = 'LOADED_DTS=' + Yesterdate + 'T000000Z'
print (load_dts)

# COMMAND ----------

# get storage accounts and file locations
source_full_storage_account_name = "edlcorestdeuprod0001.dfs.core.windows.net" 
ls_gdp_defined = "ls_GDP_defined"

spark.conf.set("spark.storage.synapse.edlcorestdeuprod0001.dfs.core.windows.net.linkedServiceName", "ls_GDP_defined") # ls_WRFEC_ADLS  ls_storage
#spark.conf.set("spark.storage.synapse.linkedServiceName", "ls_GDP_defined")
sc._jsc.hadoopConfiguration().set(f"fs.azure.account.oauth.provider.type.edlcorestdeuprod0001.dfs.core.windows.net", "com.microsoft.azure.synapse.tokenlibrary.LinkedServiceBasedTokenProvider")


# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = ['CaseService_case_LegalEntityClient'
, 'CaseService_case_Case'
, 'CaseService_dbo_Country'
, 'CaseService_case_LegalEntityClientIdentifier'
, 'CaseService_dbo_SystemIdType'
, 'CaseService_NaturalPerson_NaturalPersonClientIdentifier'
, 'CaseService_NaturalPerson_NaturalPersonAlias'
, 'CaseService_case_RelatedNaturalPersonPartyAlias'
, 'CaseService_case_LegalEntityClientStructureSnapshot'
, 'CaseService_snapshot_ClientStructureSnapshot'
, 'CaseService_snapshot_ClientStructureSnapshotRelationshipDetail'
, 'CaseService_case_DynamicRiskModelInstanceReference'
, 'RiskModel_dbo_Instance'
, 'RiskModel_dbo_InstanceAnswer'
, 'RiskModel_dbo_PossibleAnswer'
, 'CaseService_case_Address'
, 'CaseService_case_CountryReference'
, 'CaseService_case_InvolvedStaffMember'
, 'CaseService_dbo_RabobankEntity'
, 'CaseService_case_RelatedLegalEntityParty'
, 'CaseService_NaturalPerson_NaturalPersonClient'
, 'CaseService_NaturalPerson_NaturalPersonCase'
, 'CaseService_NaturalPerson_ScreeningResults'
, 'CaseService_NaturalPerson_PoliticallyExposedPersonStatusReference'
, 'CaseService_NaturalPerson_Address'
, 'CaseService_NaturalPerson_CountryReference'
, 'CaseService_NaturalPerson_InvolvedStaffMember'
, 'CaseService_case_RelatedNaturalPersonParty'
, 'CaseService_case_PoliticallyExposedPersonStatusReference'
, 'CaseService_NaturalPerson_ClientStructureSnapshot'
, 'CaseService_case_RabobankEntityReference'
, 'CaseService_NaturalPerson_RabobankEntityReference'
]

# TODO make paramets for GDP load_dts

for item in load_df:
    #print('df_' + item + ' = spark.read.load(\'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/' + item + '/100/data/' + load_dts + '/*.parquet\', format=\'parquet\')') # .toPandas()
    spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/'+item+'/100/data/' + load_dts + '/*.parquet', format='parquet').createOrReplaceTempView(item)
    

# COMMAND ----------

df_LE_ApprovedClientVersions = spark.sql("""
    SELECT DISTINCT max(cl.Id) as LegalEntityClientId
    FROM CaseService_case_LegalEntityClient AS cl 
    INNER JOIN CaseService_case_Case AS c ON c.LegalEntityClientId = cl.Id
    WHERE  c.CurrentStatus = 9
    GROUP BY cl.GcobId

""")

df_LE_ApprovedClientVersions.createOrReplaceTempView('LE_ApprovedClientVersions')
# df_LE_ApprovedClientVersions.count()

# COMMAND ----------

df_NP_ApprovedClientVersions = spark.sql("""
    SELECT DISTINCT max(np.Id) as NaturalPersonClientId
    FROM CaseService_NaturalPerson_NaturalPersonClient AS np
    INNER JOIN CaseService_NaturalPerson_NaturalPersonCase AS c ON c.NaturalPersonClientId = np.Id
    WHERE  c.CurrentStatus = 9
    GROUP BY np.GcobId

""")

df_NP_ApprovedClientVersions.createOrReplaceTempView('NP_ApprovedClientVersions')
# df_NP_ApprovedClientVersions.count()

# COMMAND ----------

df_Country = spark.sql("""
WITH CountryList AS (
SELECT MAX(Id) as Id FROM CaseService_dbo_Country GROUP BY GcobId
)
,Country AS (
SELECT  c.Id,c.GcobId, c.Name
FROM CaseService_dbo_Country c 
JOIN CountryList cl on cl.Id=c.Id
)
select * from Country

""")

df_Country.createOrReplaceTempView('Country')
# df_Country.count()

# COMMAND ----------

df_LE_ClientIdentifiers = spark.sql("""
WITH LE_ClientIdentifiers AS (
 SELECT ci.LegalEntityId, st.Name, ci.ValueOfIdentifier
 FROM CaseService_case_LegalEntityClientIdentifier AS ci 
 INNER JOIN CaseService_dbo_SystemIdType AS st ON st.Id = ci.SystemTypeReferenceId
)
,LE_ClientIdentifiersJoined AS (
    SELECT DISTINCT LegalEntityId, Name,concat_ws(',', sort_array(collect_list(struct(ValueOfIdentifier))).ValueOfIdentifier) AS Identifiers
    FROM LE_ClientIdentifiers AS ci
	GROUP BY LegalEntityId,Name
)
select * from LE_ClientIdentifiersJoined

""")

df_LE_ClientIdentifiers.createOrReplaceTempView('LE_ClientIdentifiers')
# df_LE_ClientIdentifiers.count()

# COMMAND ----------

df_NP_ClientIdentifiers = spark.sql("""
WITH NP_ClientIdentifiers AS (
 SELECT ci.NaturalPersonClientId, st.Name, ci.ValueOfIdentifier
 FROM CaseService_NaturalPerson_NaturalPersonClientIdentifier AS ci 
 INNER JOIN CaseService_dbo_SystemIdType AS st ON st.Id = ci.SystemTypeReferenceId
)
,NP_ClientIdentifiersJoined AS (
    SELECT DISTINCT NaturalPersonClientId, Name,concat_ws(',', sort_array(collect_list(struct(ValueOfIdentifier))).ValueOfIdentifier) AS Identifiers
    FROM NP_ClientIdentifiers AS ci
	GROUP BY NaturalPersonClientId,Name
)
select * from NP_ClientIdentifiersJoined

""")

df_NP_ClientIdentifiers.createOrReplaceTempView('NP_ClientIdentifiers')
# df_NP_ClientIdentifiers.count()

# COMMAND ----------

df_NP_Alias = spark.sql("""
select NaturalPersonClientId, Alias_1, Alias_2, Alias_3, Alias_4 from (
select DISTINCT
npa.NaturalPersonClientId,
concat_ws(' ',npa.FirstName,nullif(npa.MiddleName,''),npa.LastName) as Alias,
Row_Number() over (partition by npa.NaturalPersonClientId order by concat_ws(' ',npa.FirstName,nullif(npa.MiddleName,''),npa.LastName)) as RowN
from CaseService_NaturalPerson_NaturalPersonAlias npa
)
PIVOT(
    max(Alias) for RowN in (1 Alias_1,2 Alias_2,3 Alias_3,4 Alias_4)
)

""")

df_NP_Alias.createOrReplaceTempView('NP_Alias')
# df_NP_Alias.count()

# COMMAND ----------

df_RNPP_Alias = spark.sql("""
select RelatedNaturalPersonPartyId, Alias_1, Alias_2, Alias_3, Alias_4 from (
SELECT DISTINCT 
rnppa.RelatedNaturalPersonPartyId,
concat_ws(' ',rnppa.FirstName,nullif(rnppa.MiddleName,''),rnppa.LastName) as Alias,
Row_Number() over (partition by rnppa.RelatedNaturalPersonPartyId order by concat_ws(' ',rnppa.FirstName,nullif(rnppa.MiddleName,''),rnppa.LastName)) as RowN
FROM CaseService_case_RelatedNaturalPersonPartyAlias rnppa 
)
PIVOT(
    max(Alias) for RowN in (1 Alias_1,2 Alias_2,3 Alias_3,4 Alias_4)
)

""")

df_RNPP_Alias.createOrReplaceTempView('RNPP_Alias')
# df_RNPP_Alias.count()

# COMMAND ----------

df_LE_LatestClientStructSnapshot = spark.sql("""

    SELECT DISTINCT MAX(lecss.ClientStructureSnapshotId) AS ClientStructureSnapshotId, lecss.LegalEntityClientId
    FROM CaseService_case_LegalEntityClientStructureSnapshot lecss
    GROUP BY lecss.LegalEntityClientId

""")

df_LE_LatestClientStructSnapshot.createOrReplaceTempView('LE_LatestClientStructSnapshot')
# df_LE_LatestClientStructSnapshot.count()

# COMMAND ----------

df_NP_LatestClientStructSnapshot = spark.sql("""

    SELECT DISTINCT MAX(npcss.ClientStructureSnapshotId) AS ClientStructureSnapshotId, npcss.NaturalPersonClientId  AS ID
    FROM CaseService_NaturalPerson_ClientStructureSnapshot npcss
    GROUP BY npcss.NaturalPersonClientId 

""")

df_NP_LatestClientStructSnapshot.createOrReplaceTempView('NP_LatestClientStructSnapshot')
# df_NP_LatestClientStructSnapshot.count()

# COMMAND ----------

df_LE_AND_NP_ClientRelationships = spark.sql("""

SELECT DISTINCT lc.GcobId, lc.Id, ls.ClientStructureSnapshotId, cssrd.ChildIdentity, cssrd.ChildType, cssrd.ParentIdentity, cssrd.ParentType
FROM CaseService_case_LegalEntityClient lc
INNER JOIN LE_ApprovedClientVersions cc on cc.LegalEntityClientId = lc.Id  
INNER JOIN CaseService_case_LegalEntityClientStructureSnapshot lecss on lecss.LegalEntityClientId = lc.Id
INNER JOIN CaseService_snapshot_ClientStructureSnapshot css ON css.ClientStructureSnapshotId = lecss.ClientStructureSnapshotId
INNER JOIN LE_LatestClientStructSnapshot ls ON ls.ClientStructureSnapshotId = lecss.ClientStructureSnapshotId 
INNER JOIN CaseService_snapshot_ClientStructureSnapshotRelationshipDetail cssrd ON lecss.ClientStructureSnapshotId = cssrd.ClientStructureSnapshotId 
AND cssrd.Expired IS NULL 
LEFT JOIN CaseService_case_InvolvedStaffMember s ON s.InvolvedStaffMemberId = lc.GlobalClientOwnerId
LEFT JOIN CaseService_case_RabobankEntityReference e on e.RabobankEntityId= s.RabobankEntityReferenceId

WHERE lc.Expired IS NULL and lc.ClientLifecycleStatusType IN ('0','1','3') and e.GcobId NOT IN ('2','20')

UNION ALL

SELECT DISTINCT np.GcobId, np.Id, npcss.ClientStructureSnapshotId, cssrd.ChildIdentity, cssrd.ChildType, cssrd.ParentIdentity, cssrd.ParentType
FROM CaseService_NaturalPerson_NaturalPersonClient np
INNER JOIN NP_ApprovedClientVersions cc on np.Id = cc.NaturalPersonClientId
INNER JOIN CaseService_NaturalPerson_ClientStructureSnapshot npcss on npcss.NaturalPersonClientId=np.Id
INNER JOIN CaseService_snapshot_ClientStructureSnapshot css on css.ClientStructureSnapshotId=npcss.ClientStructureSnapshotId 
INNER JOIN NP_LatestClientStructSnapshot ls ON ls.ClientStructureSnapshotId = npcss.ClientStructureSnapshotId 
INNER JOIN CaseService_snapshot_ClientStructureSnapshotRelationshipDetail cssrd ON npcss.ClientStructureSnapshotId = cssrd.ClientStructureSnapshotId 
AND cssrd.Expired IS NULL
LEFT JOIN CaseService_NaturalPerson_InvolvedStaffMember ism ON ism.Id = np.GlobalClientOwnerId
LEFT JOIN CaseService_NaturalPerson_RabobankEntityReference e ON e.RabobankEntityId= ism.RabobankEntityReferenceId

WHERE np.Expired IS NULL and np.ClientLifecycleStatusTypeId IN ('0','1','3') and e.GcobId NOT IN ('2','20')


""")

df_LE_AND_NP_ClientRelationships.createOrReplaceTempView('LE_AND_NP_ClientRelationships')
# df_LE_AND_NP_ClientRelationships.count()

# --INNER JOIN CaseService_case_Case AS ca ON ca.LegalEntityClientId = lc.Id

# COMMAND ----------

df_LE_AND_RLEP_With_Duplicates = spark.sql("""

SELECT DISTINCT 
CASE WHEN gcdsid.Identifiers IS NULL THEN concat('LE_',lc.GcobId) ELSE gcdsid.Identifiers END AS GcobId, lc.FullLegalName, 'E' AS EntityType, 
lc.IncorporationNumber, lc.IncorporationDateString AS  IncorporationDate, pa.Text AS PEPStatus, ra.Number AS RegisteredNumber,
ra.Street AS RegisteredStreet, ra.city AS RegisteredCity, ra.Region AS RegisteredRegion, ra.PostalCode AS RegisteredPostalCode,
cy.Name AS RegisteredCountryName, oa.Number AS OperatingNumber,oa.Street AS OperatingStreet, oa.city AS OperatingCity, 
oa.Region AS OperatingRegion, oa.PostalCode AS OperatingPostalCode, opeCountry.Name AS OperatingCountryName, 
re.Name AS GlobalClientOwnerLocation
FROM CaseService_case_LegalEntityClient lc
INNER JOIN CaseService_case_Case AS ca ON ca.LegalEntityClientId = lc.Id
INNER JOIN LE_ApprovedClientVersions cc on ca.LegalEntityClientId = cc.LegalEntityClientId
INNER JOIN LE_AND_NP_ClientRelationships cr on cr.ChildIdentity=lc.Id and cr.ChildType=0 --and lc.Expired IS NULL
LEFT JOIN LE_ClientIdentifiers AS gcdsid ON gcdsid.LegalEntityId = lc.Id AND gcdsid.Name = 'GCDS ID'
INNER JOIN CaseService_case_DynamicRiskModelInstanceReference rmRef ON rmRef.LegalEntityClientId = lc.Id AND rmRef.Expired IS NULL
AND rmRef.DefinitionName LIKE 'Corporate Client Risk Model%'
INNER JOIN RiskModel_dbo_Instance i ON i.Id = rmRef.InstanceId
INNER JOIN RiskModel_dbo_InstanceAnswer ia on ia.InstanceId = i.Id
INNER JOIN RiskModel_dbo_PossibleAnswer pa on pa.Id = ia.PossibleAnswerId and pa.QuestionId IN ('351','37','78','472','257','79')
LEFT JOIN CaseService_case_Address AS ra ON ra.AddressId = lc.RegisteredAddressId
LEFT JOIN CaseService_case_CountryReference AS cyr ON cyr.CountryId = ra.CountryReferenceId
LEFT JOIN Country cy ON cyr.GcobId = cy.GcobId
LEFT JOIN CaseService_case_Address AS oa ON oa.AddressId = lc.OperatingAddressId 
LEFT JOIN CaseService_case_CountryReference AS cyr1 ON cyr1.CountryId = oa.CountryReferenceId
LEFT JOIN Country opeCountry ON cyr1.GcobId = opeCountry.GcobId
LEFT join CaseService_case_InvolvedStaffMember ism   on lc.GlobalClientOwnerId = ism.InvolvedStaffMemberId
LEFT JOIN CaseService_dbo_RabobankEntity re ON re.Id = ism.RabobankEntityReferenceId

WHERE lc.ClientLifecycleStatusType IN ('0','1','3') 

UNION

SELECT DISTINCT 
CASE WHEN gcdsid.Identifiers IS NULL THEN concat('LE_',lc.GcobId) ELSE gcdsid.Identifiers END AS GcobId, lc.FullLegalName, 'E' AS EntityType, 
lc.IncorporationNumber, lc.IncorporationDateString AS  IncorporationDate, pa.Text AS PEPStatus, ra.Number AS RegisteredNumber,
ra.Street AS RegisteredStreet, ra.city AS RegisteredCity, ra.Region AS RegisteredRegion, ra.PostalCode AS RegisteredPostalCode,
cy.Name AS RegisteredCountryName, oa.Number AS OperatingNumber,oa.Street AS OperatingStreet, oa.city AS OperatingCity, 
oa.Region AS OperatingRegion, oa.PostalCode AS OperatingPostalCode, opeCountry.Name AS OperatingCountryName, 
re.Name AS GlobalClientOwnerLocation
FROM CaseService_case_LegalEntityClient lc
INNER JOIN CaseService_case_Case AS ca ON ca.LegalEntityClientId = lc.Id
INNER JOIN LE_ApprovedClientVersions cc on ca.LegalEntityClientId = cc.LegalEntityClientId
INNER JOIN LE_AND_NP_ClientRelationships cr on cr.ParentIdentity=lc.Id and cr.ParentType=0 --and lc.Expired IS NULL
LEFT JOIN LE_ClientIdentifiers AS gcdsid ON gcdsid.LegalEntityId = lc.Id AND gcdsid.Name = 'GCDS ID'
INNER JOIN CaseService_case_DynamicRiskModelInstanceReference rmRef ON rmRef.LegalEntityClientId = lc.Id AND rmRef.Expired IS NULL
AND rmRef.DefinitionName LIKE 'Corporate Client Risk Model%'
INNER JOIN RiskModel_dbo_Instance i ON i.Id = rmRef.InstanceId
INNER JOIN RiskModel_dbo_InstanceAnswer ia on ia.InstanceId = i.Id
INNER JOIN RiskModel_dbo_PossibleAnswer pa on pa.Id = ia.PossibleAnswerId and pa.QuestionId IN ('351','37','78','472','257','79')
LEFT JOIN CaseService_case_Address AS ra ON ra.AddressId = lc.RegisteredAddressId
LEFT JOIN CaseService_case_CountryReference AS cyr ON cyr.CountryId = ra.CountryReferenceId
LEFT JOIN Country cy ON cyr.GcobId = cy.GcobId
LEFT JOIN CaseService_case_Address AS oa ON oa.AddressId = lc.OperatingAddressId 
LEFT JOIN CaseService_case_CountryReference AS cyr1 ON cyr1.CountryId = oa.CountryReferenceId
LEFT JOIN Country opeCountry ON cyr1.GcobId = opeCountry.GcobId
LEFT join CaseService_case_InvolvedStaffMember ism   on lc.GlobalClientOwnerId = ism.InvolvedStaffMemberId
LEFT JOIN CaseService_dbo_RabobankEntity re ON re.Id = ism.RabobankEntityReferenceId

WHERE lc.ClientLifecycleStatusType IN ('0','1','3')

UNION

SELECT DISTINCT
concat('RLEP_',r.Identity) AS GcobId, r.FullLegalName, 'E' AS EntityType, '' AS IncorporationNumber, '' AS IncorporationDate, '' AS PEPStatus
,ra.Number AS RegisteredNumber, ra.Street AS RegisteredStreet, ra.city AS RegisteredCity, ra.Region AS RegisteredRegion
, ra.PostalCode AS RegisteredPostalCode, cy.Name AS RegisteredCountryName, oa.Number AS OperatingNumber, oa.Street AS OperatingStreet
, oa.city AS OperatingCity, oa.Region AS OperatingRegion, oa.PostalCode AS OperatingPostalCode, opeCountry.Name AS OperatingCountryName
,'' AS GlobalClientOwnerLocation
FROM CaseService_case_RelatedLegalEntityParty r
INNER JOIN LE_AND_NP_ClientRelationships cr on cr.ChildIdentity=r.RelatedLegalEntityPartyId and cr.ChildType=1 and r.Expired IS NULL
LEFT JOIN CaseService_case_Address AS ra ON ra.AddressId = r.RegisteredAddressId
LEFT JOIN CaseService_case_CountryReference AS cyr ON cyr.CountryId = ra.CountryReferenceId
LEFT JOIN Country cy ON cyr.GcobId = cy.GcobId
LEFT JOIN CaseService_case_Address AS oa ON oa.AddressId = r.OperatingAddressId
LEFT JOIN CaseService_case_CountryReference AS cyr1 ON cyr1.CountryId = oa.CountryReferenceId
LEFT JOIN Country opeCountry ON cyr1.GcobId = opeCountry.GcobId

UNION

SELECT DISTINCT
concat('RLEP_',r.Identity) AS GcobId, r.FullLegalName, 'E' AS EntityType, '' AS IncorporationNumber, '' AS IncorporationDate, '' AS PEPStatus
,ra.Number AS RegisteredNumber, ra.Street AS RegisteredStreet, ra.city AS RegisteredCity, ra.Region AS RegisteredRegion
, ra.PostalCode AS RegisteredPostalCode, cy.Name AS RegisteredCountryName, oa.Number AS OperatingNumber, oa.Street AS OperatingStreet
, oa.city AS OperatingCity, oa.Region AS OperatingRegion, oa.PostalCode AS OperatingPostalCode, opeCountry.Name AS OperatingCountryName
,'' AS GlobalClientOwnerLocation
FROM CaseService_case_RelatedLegalEntityParty r
INNER JOIN LE_AND_NP_ClientRelationships cr on cr.ParentIdentity=r.RelatedLegalEntityPartyId and cr.ParentType=1 and r.Expired IS NULL
LEFT JOIN CaseService_case_Address AS ra ON ra.AddressId = r.RegisteredAddressId
LEFT JOIN CaseService_case_CountryReference AS cyr ON cyr.CountryId = ra.CountryReferenceId
LEFT JOIN Country cy ON cyr.GcobId = cy.GcobId
LEFT JOIN CaseService_case_Address AS oa ON oa.AddressId = r.OperatingAddressId
LEFT JOIN CaseService_case_CountryReference AS cyr1 ON cyr1.CountryId = oa.CountryReferenceId
LEFT JOIN Country opeCountry ON cyr1.GcobId = opeCountry.GcobId

""")

df_LE_AND_RLEP_With_Duplicates.createOrReplaceTempView('LE_AND_RLEP')

# df_LE_AND_RLEP_With_Duplicates.count()

# COMMAND ----------

df_NP_AND_RNPP = spark.sql("""

SELECT DISTINCT
CASE WHEN gcdsid.Identifiers IS NULL THEN concat('NP_',nc.GcobId) ELSE gcdsid.Identifiers END AS GcobId
, concat_ws(' ',nc.FirstName,nullif(nc.MiddleName,''),nc.LastName) AS FullLegalName
, CASE WHEN nc.NaturalPersonClientTypeId = 1 THEN 'I' WHEN nc.NaturalPersonClientTypeId = 2 THEN 'I' ELSE 'E' END AS EntityType, nc.DateOfBirth
, pep.Name AS PEPStatus, alias.Alias_1, alias.Alias_2, alias.Alias_3, alias.Alias_4, ra.Number AS ResidentialNumber,ra.Street AS ResidentialStreet
, ra.city AS ResidentialCity, ra.Region AS ResidentialRegion, ra.PostalCode AS ResidentialPostalCode, cy.Name AS RegisteredCountryName
, re.Name AS GlobalClientOwnerLocation
FROM CaseService_NaturalPerson_NaturalPersonClient nc
INNER JOIN CaseService_NaturalPerson_NaturalPersonCase  AS npc ON npc.NaturalPersonClientId = nc.Id
INNER JOIN NP_ApprovedClientVersions cc on npc.NaturalPersonClientId = cc.NaturalPersonClientId
INNER JOIN LE_AND_NP_ClientRelationships cr on cr.ChildIdentity=nc.Id and cr.ChildType=3 --and nc.Expired IS NULL
LEFT JOIN NP_ClientIdentifiers AS gcdsid ON gcdsid.NaturalPersonClientId = nc.Id AND gcdsid.Name = 'GCDS ID'
LEFT JOIN CaseService_NaturalPerson_ScreeningResults screen ON nc.ScreeningResultsId = screen.Id 
LEFT JOIN CaseService_NaturalPerson_PoliticallyExposedPersonStatusReference pep ON screen.PoliticallyExposedPersonStatusReferenceId = pep.PoliticallyExposedPersonStatusId
LEFT JOIN NP_Alias alias ON alias.NaturalPersonClientId = nc.Id
LEFT JOIN CaseService_NaturalPerson_Address AS ra ON ra.AddressId = nc.ResidentialAddressId 
LEFT JOIN CaseService_NaturalPerson_CountryReference AS cyr ON cyr.CountryId = ra.CountryReferenceId
LEFT JOIN Country cy ON cyr.GcobId = cy.GcobId
LEFT join CaseService_NaturalPerson_InvolvedStaffMember ism   on nc.GlobalClientOwnerId = ism.Id
LEFT JOIN CaseService_dbo_RabobankEntity re ON re.Id = ism.RabobankEntityReferenceId

WHERE nc.ClientLifecycleStatusTypeId IN ('0','1','3')

UNION

SELECT DISTINCT
CASE WHEN gcdsid.Identifiers IS NULL THEN concat('NP_',nc.GcobId) ELSE gcdsid.Identifiers END AS GcobId
, concat_ws(' ',nc.FirstName,nullif(nc.MiddleName,''),nc.LastName) AS FullLegalName
, CASE WHEN nc.NaturalPersonClientTypeId = 1 THEN 'I' WHEN nc.NaturalPersonClientTypeId = 2 THEN 'I' ELSE 'E' END AS EntityType, nc.DateOfBirth
, pep.Name AS PEPStatus, alias.Alias_1, alias.Alias_2, alias.Alias_3, alias.Alias_4, ra.Number AS ResidentialNumber,ra.Street AS ResidentialStreet
, ra.city AS ResidentialCity, ra.Region AS ResidentialRegion, ra.PostalCode AS ResidentialPostalCode, cy.Name AS RegisteredCountryName
, re.Name AS GlobalClientOwnerLocation
FROM CaseService_NaturalPerson_NaturalPersonClient nc
INNER JOIN CaseService_NaturalPerson_NaturalPersonCase  AS npc ON npc.NaturalPersonClientId = nc.Id
INNER JOIN NP_ApprovedClientVersions cc on npc.NaturalPersonClientId = cc.NaturalPersonClientId
INNER JOIN LE_AND_NP_ClientRelationships cr on cr.ParentIdentity=nc.Id and cr.ParentType=3 --and nc.Expired IS NULL
LEFT JOIN NP_ClientIdentifiers AS gcdsid ON gcdsid.NaturalPersonClientId = nc.Id AND gcdsid.Name = 'GCDS ID'
LEFT JOIN CaseService_NaturalPerson_ScreeningResults screen ON nc.ScreeningResultsId = screen.Id 
LEFT JOIN CaseService_NaturalPerson_PoliticallyExposedPersonStatusReference pep ON screen.PoliticallyExposedPersonStatusReferenceId = pep.PoliticallyExposedPersonStatusId
LEFT JOIN NP_Alias alias ON alias.NaturalPersonClientId = nc.Id
LEFT JOIN CaseService_NaturalPerson_Address AS ra ON ra.AddressId = nc.ResidentialAddressId 
LEFT JOIN CaseService_NaturalPerson_CountryReference AS cyr ON cyr.CountryId = ra.CountryReferenceId
LEFT JOIN Country cy ON cyr.GcobId = cy.GcobId
LEFT join CaseService_NaturalPerson_InvolvedStaffMember ism   on nc.GlobalClientOwnerId = ism.Id
LEFT JOIN CaseService_dbo_RabobankEntity re ON re.Id = ism.RabobankEntityReferenceId

WHERE nc.ClientLifecycleStatusTypeId IN ('0','1','3')

UNION

SELECT DISTINCT
concat('RNPP_',r.Identity) AS GcobId, concat_ws(' ',r.FirstName,nullif(r.MiddleName,''),r.LastName) AS FullLegalName, 'I' AS EntityType
, r.DateOfBirthString AS DateOfBirth, pepsr.Name AS PEPStatus, alias.Alias_1, alias.Alias_2, alias.Alias_3, alias.Alias_4 
, ra.Number AS ResidentialNumber, ra.Street AS ResidentialStreet, ra.city AS ResidentialCity, ra.Region AS ResidentialRegion
, ra.PostalCode AS ResidentialPostalCode, cy.Name AS RegisteredCountryName, '' AS GlobalClientOwnerLocation
FROM CaseService_case_RelatedNaturalPersonParty r
INNER JOIN LE_AND_NP_ClientRelationships cr on cr.ChildIdentity=r.RelatedNaturalPersonPartyId and cr.ChildType=2 and r.Expired IS NULL
LEFT JOIN CaseService_case_PoliticallyExposedPersonStatusReference pepsr on r.PoliticallyExposedPersonStatusId = pepsr.PoliticallyExposedPersonStatusId
LEFT JOIN RNPP_Alias alias ON r.RelatedNaturalPersonPartyId = alias.RelatedNaturalPersonPartyId
LEFT JOIN CaseService_case_Address AS ra ON ra.AddressId = r.AddressId 
LEFT JOIN CaseService_case_CountryReference AS cyr ON cyr.CountryId = ra.CountryReferenceId
LEFT JOIN Country cy ON cyr.GcobId = cy.GcobId

UNION

SELECT DISTINCT
concat('RNPP_',r.Identity) AS GcobId, concat_ws(' ',r.FirstName,nullif(r.MiddleName,''),r.LastName) AS FullLegalName, 'I' AS EntityType
, r.DateOfBirthString AS DateOfBirth, pepsr.Name AS PEPStatus, alias.Alias_1, alias.Alias_2, alias.Alias_3, alias.Alias_4 
, ra.Number AS ResidentialNumber, ra.Street AS ResidentialStreet, ra.city AS ResidentialCity, ra.Region AS ResidentialRegion
, ra.PostalCode AS ResidentialPostalCode, cy.Name AS RegisteredCountryName, '' AS GlobalClientOwnerLocation
FROM CaseService_case_RelatedNaturalPersonParty r
INNER JOIN LE_AND_NP_ClientRelationships cr on cr.ParentIdentity=cast(r.RelatedNaturalPersonPartyId as string) and cr.ParentType=2 and r.Expired IS NULL
LEFT JOIN CaseService_case_PoliticallyExposedPersonStatusReference pepsr on r.PoliticallyExposedPersonStatusId = pepsr.PoliticallyExposedPersonStatusId
LEFT JOIN RNPP_Alias alias ON r.RelatedNaturalPersonPartyId = alias.RelatedNaturalPersonPartyId
LEFT JOIN CaseService_case_Address AS ra ON ra.AddressId = r.AddressId 
LEFT JOIN CaseService_case_CountryReference AS cyr ON cyr.CountryId = ra.CountryReferenceId
LEFT JOIN Country cy ON cyr.GcobId = cy.GcobId

""")

df_NP_AND_RNPP.createOrReplaceTempView('NP_AND_RNPP')
# df_NP_AND_RNPP.count()


# COMMAND ----------

df_LE_AND_NP_Final = spark.sql("""

SELECT DISTINCT
ap2.GcobId AS ListUid,
'' AS Detail,
'' AS Gender,
ap2.EntityType AS Type,
'Wholesale' AS Category,
'' AS SubCategory,
ap2.PEPStatus AS Status,
'' AS RiskValue,
'' AS `Name.Title.1`,
ap2.FullLegalName AS `Name.Last.1`,
'' AS `Name.Title.2`,
'' AS `Name.first.2`,
'' AS `Name.Middle.2`,
'' AS `Name.Last.2`,
'' AS `Name.Suffix.2`,
'' AS `Name.Title.3`,
'' AS `Name.first.3`,
'' AS `Name.middle.3`,
'' AS `Name.Last.3`,
'' AS `Name.Title.4`,
'' AS `Name.Last.4`,
'' AS `Name.first.5`,
'' AS `Name.Last.5`,
'' AS `Name.Last.6`,
'' AS `Name.Last.7`,
'' AS `Name.Last.8`,
'Registered address' AS `Address.TypeName.1`,
regexp_replace(trim(ap2.RegisteredStreet), '[\n\r\t]', ' ') AS `Address.Line1.1`,
regexp_replace(trim(ap2.RegisteredNumber), '[\n\r\t]', ' ') AS `Address.Line2.1`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line3.1`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line4.1`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line5.1`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line6.1`,
ap2.RegisteredCity AS `Address.City.1`,
ap2.RegisteredRegion AS `Address.State.1`,
ap2.RegisteredCountryName AS `Address.Country.1`,
ap2.RegisteredPostalCode AS `Address.PostalCode.1`,
'' AS `Address.LocationString.1`,
'Correspondence address' AS `Address.TypeName.2`,
regexp_replace(trim(ap2.OperatingStreet), '[\n\r\t]', ' ') AS `Address.Line1.2`,
regexp_replace(trim(ap2.OperatingNumber), '[\n\r\t]', ' ') AS `Address.Line2.2`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line3.2`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line4.2`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line5.2`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line6.2`,
ap2.OperatingCity AS `Address.City.2`,
ap2.OperatingRegion AS `Address.State.2`,
ap2.OperatingCountryName AS `Address.Country.2`,
ap2.OperatingPostalCode AS `Address.PostalCode.2`,
'' AS `Address.LocationString.2`,
'Registered City' AS `Address.TypeName.3`,
ap2.RegisteredCity AS `Address.City.3`,
'' AS `Address.Country.3`,
'' AS `Address.LocationString.3`,
'' AS `Address.TypeName.4`,
'' AS `Address.Line1.4`,
'' AS `Address.Line2.4`,
'' AS `Address.Line3.4`,
'' AS `Address.Line4.4`,
'' AS `Address.Line5.4`,
'' AS `Address.Line6.4`,
'' AS `Address.City.4`,
'' AS `Address.State.4`,
'' AS `Address.Country.4`,
'' AS `Address.PostalCode.4`,
'' AS `Address.LocationString.4`,
'' AS `Address.TypeName.5`,
'' AS `Address.Line1.5`,
'' AS `Address.Line2.5`,
'' AS `Address.Line3.5`,
'' AS `Address.Line4.5`,
'' AS `Address.Line5.5`,
'' AS `Address.Line6.5`,
'' AS `Address.City.5`,
'' AS `Address.State.5`,
'' AS `Address.Country.5`,
'' AS `Address.PostalCode.5`,
'' AS `Address.LocationString.5`,
'' AS `Identifier.Type.1`,
'' AS `Identifier.TypeName.1`,
'' AS `Identifier.IdentifierValue.1`,
'' AS `Identifier.Type.2`,
'' AS `Identifier.TypeName.2`,
'' AS `Identifier.IdentifierValue.2`,
'' AS `Identifier.Type.3`,
'' AS `Identifier.TypeName.3`,
'' AS `Identifier.IdentifierValue.3`,
'o' AS `Identifier.Type.4`,
'Incorporation Number' AS `Identifier.TypeName.4`,
ap2.IncorporationNumber AS `Identifier.IdentifierValue.4`,
ap2.RegisteredCity AS `Identifier.IdentifierState.4`,
ap2.RegisteredCountryName AS `Identifier.IdentifierCountry.4`,
'' AS `Identifier.Type.5`,
'' AS `Identifier.TypeName.5`,
'' AS `Identifier.IdentifierValue.5`,
'' AS `Identifier.IdentifierState.5`,
'' AS `Identifier.IdentifierCountry.5`,
'' AS `Identifier.Type.6`,
'' AS `Identifier.TypeName.6`,
'' AS `Identifier.IdentifierValue.6`,
'' AS `Identifier.IdentifierState.6`,
'' AS `Identifier.IdentifierCountry.6`,
'' AS `Identifier.Type.7`,
'' AS `Identifier.TypeName.7`,
'' AS `Identifier.IdentifierValue.7`,
'' AS `Identifier.IdentifierState.7`,
'' AS `Identifier.IdentifierCountry.7`,
'' AS `Identifier.Type.8`,
'' AS `Identifier.TypeName.8`,
'' AS `Identifier.IdentifierValue.8`,
'' AS `Identifier.Type.9`,
'' AS `Identifier.TypeName.9`,
'' AS `Identifier.IdentifierValue.9`,
'' AS `Identifier.IdentifierState.9`,
'' AS `Identifier.IdentifierCountry.9`,
'' AS `Identifier.Type.10`,
'' AS `Identifier.TypeName.10`,
'' AS `Identifier.IdentifierValue.10`,
'' AS `Identifier.Type.11`,
'' AS `Identifier.TypeName.11`,
'' AS `Identifier.IdentifierValue.11`,
'o' AS `Identifier.Type.12`,
'Global Client Owner Location' AS `Identifier.TypeName.12`,
ap2.GlobalClientOwnerLocation AS `Identifier.IdentifierValue.12`, 
'' AS `Identifier.Type.13`,
'' AS `Identifier.TypeName.13`,
'' AS `Identifier.IdentifierValue.13`,
'' AS `Identifier.Type.14`,
'' AS `Identifier.TypeName.14`,
'' AS `Identifier.IdentifierValue.14`,
to_date(trim(''),'dd-MM-yyyy') AS `DOB.1`,
to_date(trim(ap2.IncorporationDate),'dd-MM-yyyy') AS `IncorporationDate.1`,
to_date(trim(''),'dd-MM-yyyy') AS `IncorporationDate.2`,
to_date(trim(''),'dd-MM-yyyy') AS `IncorporationDate.3`,
'' AS `Citizenship.1`,
'' AS `Citizenship.2`,
'' AS `DOD.1`,
'' AS `PlaceOfBirth.City.1`,
'' AS `PlaceOfBirth.Country.1`,
'' AS `PhoneNumber.type.1`,
'' AS `PhoneNumber.Number.1`,
'' AS `Keyword.1`
FROM LE_AND_RLEP ap2

UNION

SELECT  DISTINCT
ap2.GcobId AS ListUid,
'' AS Detail,
'' AS Gender,
ap2.EntityType AS Type,
'Wholesale' AS Category,
'' AS SubCategory,
ap2.PEPStatus AS Status,
'' AS RiskValue,
'' AS `Name.Title.1`,
ap2.FullLegalName AS `Name.Last.1`,
'' AS `Name.Title.2`,
'' AS `Name.first.2`,
'' AS `Name.Middle.2`,
'' AS `Name.Last.2`,
'' AS `Name.Suffix.2`,
'' AS `Name.Title.3`,
'' AS `Name.first.3`,
'' AS `Name.middle.3`,
'' AS `Name.Last.3`,
'' AS `Name.Title.4`,
'' AS `Name.Last.4`,
'' AS `Name.first.5`,
ap2.Alias_1 AS `Name.Last.5`,
ap2.Alias_2 AS `Name.Last.6`,
ap2.Alias_3 AS `Name.Last.7`,
ap2.Alias_4 AS `Name.Last.8`,
'Residential address' AS `Address.TypeName.1`,
regexp_replace(trim(ap2.ResidentialStreet), '[\n\r\t]', ' ') AS `Address.Line1.1`,
regexp_replace(trim(ap2.ResidentialNumber), '[\n\r\t]', ' ') AS `Address.Line2.1`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line3.1`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line4.1`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line5.1`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line6.1`,
ap2.ResidentialCity AS `Address.City.1`,
ap2.ResidentialRegion AS `Address.State.1`,
ap2.RegisteredCountryName AS `Address.Country.1`,
ap2.ResidentialPostalCode AS `Address.PostalCode.1`,
'' AS `Address.LocationString.1`,
'' AS `Address.TypeName.2`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line1.2`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line2.2`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line3.2`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line4.2`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line5.2`,
regexp_replace(trim(''), '[\n\r\t]', ' ') AS `Address.Line6.2`,
'' AS `Address.City.2`,
'' AS `Address.State.2`,
'' AS `Address.Country.2`,
'' AS `Address.PostalCode.2`,
'' AS `Address.LocationString.2`,
'Residential City' AS `Address.TypeName.3`,
ap2.ResidentialCity AS `Address.City.3`,
'' AS `Address.Country.3`,
'' AS `Address.LocationString.3`,
'' AS `Address.TypeName.4`,
'' AS `Address.Line1.4`,
'' AS `Address.Line2.4`,
'' AS `Address.Line3.4`,
'' AS `Address.Line4.4`,
'' AS `Address.Line5.4`,
'' AS `Address.Line6.4`,
'' AS `Address.City.4`,
'' AS `Address.State.4`,
'' AS `Address.Country.4`,
'' AS `Address.PostalCode.4`,
'' AS `Address.LocationString.4`,
'' AS `Address.TypeName.5`,
'' AS `Address.Line1.5`,
'' AS `Address.Line2.5`,
'' AS `Address.Line3.5`,
'' AS `Address.Line4.5`,
'' AS `Address.Line5.5`,
'' AS `Address.Line6.5`,
'' AS `Address.City.5`,
'' AS `Address.State.5`,
'' AS `Address.Country.5`,
'' AS `Address.PostalCode.5`,
'' AS `Address.LocationString.5`,
'' AS `Identifier.Type.1`,
'' AS `Identifier.TypeName.1`,
'' AS `Identifier.IdentifierValue.1`,
'' AS `Identifier.Type.2`,
'' AS `Identifier.TypeName.2`,
'' AS `Identifier.IdentifierValue.2`,
'' AS `Identifier.Type.3`,
'' AS `Identifier.TypeName.3`,
'' AS `Identifier.IdentifierValue.3`,
'o' AS `Identifier.Type.4`,
'' AS `Identifier.TypeName.4`,
'' AS `Identifier.IdentifierValue.4`,
'' AS `Identifier.IdentifierState.4`,
'' AS `Identifier.IdentifierCountry.4`,
'' AS `Identifier.Type.5`,
'' AS `Identifier.TypeName.5`,
'' AS `Identifier.IdentifierValue.5`,
'' AS `Identifier.IdentifierState.5`,
'' AS `Identifier.IdentifierCountry.5`,
'' AS `Identifier.Type.6`,
'' AS `Identifier.TypeName.6`,
'' AS `Identifier.IdentifierValue.6`,
'' AS `Identifier.IdentifierState.6`,
'' AS `Identifier.IdentifierCountry.6`,
'' AS `Identifier.Type.7`,
'' AS `Identifier.TypeName.7`,
'' AS `Identifier.IdentifierValue.7`,
'' AS `Identifier.IdentifierState.7`,
'' AS `Identifier.IdentifierCountry.7`,
'' AS `Identifier.Type.8`,
'' AS `Identifier.TypeName.8`,
'' AS `Identifier.IdentifierValue.8`,
'' AS `Identifier.Type.9`,
'' AS `Identifier.TypeName.9`,
'' AS `Identifier.IdentifierValue.9`,
'' AS `Identifier.IdentifierState.9`,
'' AS `Identifier.IdentifierCountry.9`,
'' AS `Identifier.Type.10`,
'' AS `Identifier.TypeName.10`,
'' AS `Identifier.IdentifierValue.10`,
'' AS `Identifier.Type.11`,
'' AS `Identifier.TypeName.11`,
'' AS `Identifier.IdentifierValue.11`,
'o' AS `Identifier.Type.12`,
'Global Client Owner Location' AS `Identifier.TypeName.12`,
ap2.GlobalClientOwnerLocation AS `Identifier.IdentifierValue.12`, 
'' AS `Identifier.Type.13`,
'' AS `Identifier.TypeName.13`,
'' AS `Identifier.IdentifierValue.13`,
'' AS `Identifier.Type.14`,
'' AS `Identifier.TypeName.14`,
'' AS `Identifier.IdentifierValue.14`,
to_date(trim(ap2.DateOfBirth),'dd-MM-yyyy') AS `DOB.1`,
to_date(trim(''),'dd-MM-yyyy') AS `IncorporationDate.1`,
to_date(trim(''),'dd-MM-yyyy') AS `IncorporationDate.2`,
to_date(trim(''),'dd-MM-yyyy') AS `IncorporationDate.3`,
'' AS `Citizenship.1`,
'' AS `Citizenship.2`,
'' AS `DOD.1`,
'' AS `PlaceOfBirth.City.1`,
'' AS `PlaceOfBirth.Country.1`,
'' AS `PhoneNumber.type.1`,
'' AS `PhoneNumber.Number.1`,
'' AS `Keyword.1`
FROM NP_AND_RNPP ap2

""")

# df_LE_AND_NP_Final.count()

# COMMAND ----------

df_Final = df_LE_AND_NP_Final.dropDuplicates(["ListUid"])

# df_Final.count()

# COMMAND ----------

#Storing as delta table
# spark.sql("CREATE DATABASE IF NOT EXISTS WR_RADAR" )
spark.sql("DROP TABLE IF EXISTS WR_RADAR.GNS_Delivery" )
df_Final.write.mode("overwrite").saveAsTable("WR_RADAR.GNS_Delivery")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Storing in Target storage name

# COMMAND ----------

# get storage accounts and file locations
target_storage_account_name = "vasaasafecreportprd" #vasaasafecreportprd.dfs.core.windows.net 
Container_Name = "gns"


# mssparkutils.fs.ls(f"abfss://{Container_Name}@{target_storage_account_name}.dfs.core.windows.net/")

# COMMAND ----------

import datetime
Current_Date = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
File_Name = 'Rabo_LOB_'+Current_Date+'.txt'
print(File_Name)

# COMMAND ----------

#TODO saving as CSV in GNS LandingZone SA

#df_Final.repartition(1) \
#.write.format("csv").options(header='true',quoteAll ='true',delimiter='|').mode("append") \
#.save(f"abfss://{Container_Name}@{target_storage_account_name}.dfs.core.windows.net/")

#filenames = mssparkutils.fs.ls(f"abfss://{Container_Name}@{target_storage_account_name}.dfs.core.windows.net/")
#name = ''

#for filename in filenames:
#    if filename.name.endswith('.csv'):
#        name = filename.name
#        print(name)
#        mssparkutils.fs.mv(f"abfss://{Container_Name}@{target_storage_account_name}.dfs.core.windows.net/"+name, f"abfss://{Container_Name}@{target_storage_account_name}.#dfs.core.windows.net/{File_Name}")
#    else:
#        print("No CSV Files present")


