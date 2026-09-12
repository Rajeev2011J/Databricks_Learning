# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To Achive Party Structure from GCOB, Legacy2 and GIC to incorporate with GCDS
# MAGIC
# MAGIC #### author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Take Client structure from GCOB
# MAGIC - Take Client structure from Legacy2 with same format of GCOB
# MAGIC - Take Client structure (Beneficiary & UBO) from GIC with same format of GCOB
# MAGIC - Incorporate GCDS to take unique identifier
# MAGIC
# MAGIC ##### Expected output
# MAGIC   - PartyIdentifier , IsUBO , UBOReason , ChildIdentity , ParentIdentity , TypesofRelation , ShareholdingPercentage , VotingPercentage 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read Files from GDP

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
import os
import pyspark.sql.functions as F
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
SARADAR = "saradar" + environment

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Defining the dataobjects' name
party_dataobject='Party_Structure'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Deriving required date parameters
# Derive the date for which data has to be processed from GDP
Today = datetime.today().strftime("%Y%m%d")
load_dts = "EDL_LOAD_DTS=" + Today + "*"

# COMMAND ----------

# DBTITLE 1,Read GCDS data from GDP defined layer
# print list of strings for loading spark dfs from GDP
gcds_dataobjects_list = [
'client_Client',
'client_ClientOwnersProduct',
# 'client_AttributeExtensionsAttribute',
'client_ClientOwnersLocal',
'client_KeyStoreKey',
'client_OnboardedLocations',
'client_PartyRole',
'client_RMA' ,
'client_PartytoPartyRelationship',
'client_Products'
]

# Create TempView for each loading table
for dataobject in gcds_dataobjects_list:
    Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=dataobject)

# COMMAND ----------

# DBTITLE 1,Read GCOB data from GDP defined layer
# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details',
'party_AllPartyDetails',
'party_client_structure_GUI'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# DBTITLE 1,Read Legacy2 data from GDP defined layer
# List of datasets from GDP
load_df =[
'Legacy2_ClientStructure',
'Legacy2_case_client_details',
'Legacy2_GCOB_ApprovedVersion',
'Legacy2_ClientIdentification'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

# COMMAND ----------

# DBTITLE 1,Read GIC data from GDP defined layer
import pandas as pd # print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
 'pessoa'
,'pessoa_beneficiario'
,'pessoa_beneficiario_intermediario'
,'pessoa_beneficiario_intermediario_pessoa_beneficiario'
,'pessoa_beneficiario_razao_de_ser'
#,'pessoa_parte_relacionada'
,'pessoa_tipo_cadastro_status'
,'pessoa_tipo_cadastro'
,'tipo_cadastro'
#,'tipo_categoria_cliente'
,'pessoa_juridica'
,'pessoa_fisica'
#,'tipo_pessoa'
#,'garantidor_cliente'
,'avalista_cliente'
,'pessoa_tipo_cadastro_cliente_detalhe'
,'vwgic_cliente_vinculados'
]})
#gic_load_dts
# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier dataobject
df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/1/data/{load_dts}/*.parquet"
)
df_Party_SystemIdentifier.createOrReplaceTempView('P_SystemIdentifier')

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - Other Sources Data Prepration

# COMMAND ----------

# DBTITLE 1,GIC Structure Details
# %sql
# Create or replace temporary view GIC_Beneficiary_Structure As
# SELECT distinct
# 'GIC' as Application,
# p.COD_INSTITUCIONAL as PartyIdentifier,
# p.COD_INSTITUCIONAL as PartyChildIdentifier,
# p1.COD_INSTITUCIONAL as PartyParentIdentifier,
# 'Beneficiary' AS TypesOfRelation,
# case when RAZAO.NOM_RAZAO_DE_SER is not null then 'True' else 'False' end as IsUbo,
# RAZAO.NOM_RAZAO_DE_SER AS UboReason,
# BENINTERMEDIARIO.VLR_PORCENTAGEM AS ShareholdingPercentage,
# null as VotingRightPercentage
# FROM PESSOA_BENEFICIARIO ben
# INNER JOIN PESSOA AS p ON p.SEQ_PESSOA = ben.SEQ_BENEFICIARIO
# INNER JOIN PESSOA AS p1 ON p1.SEQ_PESSOA = ben.SEQ_PESSOA
# INNER JOIN PESSOA_FISICA PF ON PF.SEQ_PESSOA = ben.SEQ_PESSOA
# INNER JOIN (SELECT SEQ_PESSOA, MAX(SEQ_HISTORICO) AS SEQ_HISTORICO FROM PESSOA_FISICA GROUP BY SEQ_PESSOA) AS HISTORICO
# 	ON HISTORICO.SEQ_PESSOA = PF.SEQ_PESSOA AND HISTORICO.SEQ_HISTORICO = PF.SEQ_HISTORICO
# LEFT JOIN PESSOA_BENEFICIARIO_INTERMEDIARIO_PESSOA_BENEFICIARIO AS INTERMEDIARIO
# 	ON INTERMEDIARIO.SEQ_PESSOA_BENEFICIARIO = ben.SEQ_PESSOA_BENEFICIARIO
# LEFT JOIN PESSOA_BENEFICIARIO_INTERMEDIARIO AS BENINTERMEDIARIO
# 	ON INTERMEDIARIO.SEQ_PESSOA_BENEFICIARIO_INTERMEDIARIO = BENINTERMEDIARIO.SEQ_PESSOA_BENEFICIARIO_INTERMEDIARIO
# LEFT JOIN PESSOA_BENEFICIARIO_RAZAO_DE_SER AS RAZAO
#     ON RAZAO.SEQ_PESSOA_BENEFICIARIO_RAZAO_DE_SER = ben.SEQ_PESSOA_BENEFICIARIO_RAZAO_DE_SER
# WHERE  IFNULL(ben.DTA_TERMINO_VIGENCIA, GETDATE()) >= GETDATE()
# --AND PESSOA_BENEFICIARIO.SEQ_PESSOA = 20290--2966

# UNION

# SELECT distinct
# 'GIC' as Application,
# p.COD_INSTITUCIONAL,
# p.COD_INSTITUCIONAL as PartyChildIdentifier,
# p1.COD_INSTITUCIONAL as PartyParentIdentifier,
# 'Beneficiary' AS TypesOfRelation,
# case when RAZAO.NOM_RAZAO_DE_SER is not null then 'True' else 'False' end as IsUbo,
# RAZAO.NOM_RAZAO_DE_SER AS UboReason,
# BENINTERMEDIARIO.VLR_PORCENTAGEM AS ShareholdingPercentage,
# null as VotingRightPercentage
# FROM PESSOA_BENEFICIARIO ben
# INNER JOIN PESSOA AS p ON p.SEQ_PESSOA = ben.SEQ_BENEFICIARIO
# INNER JOIN PESSOA AS p1 ON p1.SEQ_PESSOA = ben.SEQ_PESSOA
# INNER JOIN PESSOA_JURIDICA PJ ON PJ.SEQ_PESSOA = ben.SEQ_PESSOA INNER JOIN (SELECT SEQ_PESSOA, MAX(SEQ_HISTORICO) AS SEQ_HISTORICO
#     FROM PESSOA_JURIDICA GROUP BY SEQ_PESSOA) AS HISTORICO ON HISTORICO.SEQ_PESSOA = PJ.SEQ_PESSOA 
# 	AND HISTORICO.SEQ_HISTORICO = PJ.SEQ_HISTORICO
# LEFT JOIN PESSOA_BENEFICIARIO_INTERMEDIARIO_PESSOA_BENEFICIARIO AS INTERMEDIARIO
# 	ON INTERMEDIARIO.SEQ_PESSOA_BENEFICIARIO = ben.SEQ_PESSOA_BENEFICIARIO
# LEFT JOIN PESSOA_BENEFICIARIO_INTERMEDIARIO AS BENINTERMEDIARIO
# 		ON INTERMEDIARIO.SEQ_PESSOA_BENEFICIARIO_INTERMEDIARIO = BENINTERMEDIARIO.SEQ_PESSOA_BENEFICIARIO_INTERMEDIARIO
# LEFT JOIN PESSOA_BENEFICIARIO_RAZAO_DE_SER AS RAZAO
#         ON RAZAO.SEQ_PESSOA_BENEFICIARIO_RAZAO_DE_SER = ben.SEQ_PESSOA_BENEFICIARIO_RAZAO_DE_SER
# WHERE  IFNULL(ben.DTA_TERMINO_VIGENCIA, GETDATE()) >= GETDATE()
# --AND PESSOA_BENEFICIARIO.SEQ_PESSOA = 20290--2966
# ;

# Create or replace temporary view GIC_Structure_Details As
# WITH GroupMembers AS (
# SELECT distinct
# PTC.SEQ_PESSOA, PTC.SEQ_PESSOA_GRUPO
# FROM PESSOA_TIPO_CADASTRO_CLIENTE_DETALHE PTC
# WHERE PTC.FLG_PRINCIPAL_GRUPO = 1
# ),
# ActiveGuarantors AS (
# SELECT distinct 
# AC.SEQ_PESSOA_CLIENTE, AC.SEQ_PESSOA_AVALISTA, AC.SEQ_TIPO_RELACIONAMENTO_AVALISTA
# FROM AVALISTA_CLIENTE AC
# WHERE AC.DTA_DESASSOCIACAO IS NULL
# ),
# RegistrationStatus AS (
# SELECT distinct
# PTCS.SEQ_TIPO_CADASTRO, PTCS.SEQ_STATUS_TIPO_CADASTRO, PTCS.SEQ_PESSOA ,PTCS.SEQ_HISTORICO
# FROM PESSOA_TIPO_CADASTRO_STATUS PTCS
# WHERE PTCS.SEQ_HISTORICO = (
#   SELECT MAX(SEQ_HISTORICO) FROM PESSOA_TIPO_CADASTRO_STATUS
#   WHERE SEQ_TIPO_CADASTRO = PTCS.SEQ_TIPO_CADASTRO
#   ) AND PTCS.DTA_DESATIVACAO IS NULL
# )    
# SELECT DISTINCT
# 'GIC' as Application,
# concat('GIC_',P1.COD_INSTITUCIONAL) as LocalSystemIdentifier,
# concat('GIC_',P1.COD_INSTITUCIONAL) as LocalChildIdentifier,
# concat('GIC_',P3.COD_INSTITUCIONAL) AS LocalParentIdentifier,
# tc.DES_TIPO_CADASTRO AS TypesOfRelation,
# null as IsUbo,
# null as UboReason,
# null as ShareholdingPercentage,
# null as VotingRightPercentage
# FROM 
# GroupMembers GM
# INNER JOIN ActiveGuarantors AG ON GM.SEQ_PESSOA = AG.SEQ_PESSOA_CLIENTE
# LEFT JOIN RegistrationStatus RS ON GM.SEQ_PESSOA = RS.SEQ_PESSOA
# LEFT JOIN PESSOA P1 ON GM.SEQ_PESSOA = P1.SEQ_PESSOA
# LEFT JOIN PESSOA P2 ON AG.SEQ_PESSOA_CLIENTE = P2.SEQ_PESSOA
# LEFT JOIN PESSOA P3 ON AG.SEQ_PESSOA_AVALISTA = P3.SEQ_PESSOA
# INNER JOIN tipo_cadastro tc ON AG.SEQ_TIPO_RELACIONAMENTO_AVALISTA= tc.SEQ_TIPO_CADASTRO

# UNION

# select distinct
# Application,
# concat('GIC_',PartyIdentifier) as LocalSystemIdentifier,
# concat('GIC_',PartyChildIdentifier) as LocalChildIdentifier,
# concat('GIC_',PartyParentIdentifier) AS LocalParentIdentifier,
# TypesOfRelation,
# IsUbo,
# UboReason,
# ShareholdingPercentage,
# VotingRightPercentage
# from GIC_Beneficiary_Structure

# COMMAND ----------

# DBTITLE 1,GCOB Structure Details
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_Structure_Details As
# MAGIC select distinct ClientGcobId
# MAGIC ,case when ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC end as Party_type
# MAGIC ,Case when ClientType = 'Legal Entity' then concat('GCOB_LEC_',ClientGcobId)
# MAGIC       when ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then concat('GCOB_NP_NPPC_',ClientGcobId)
# MAGIC End as UniquePartyId
# MAGIC , ChildIdentity
# MAGIC ,case when ChildType = 'LegalEntityClient' then 'Legal Entity' 
# MAGIC       when ChildType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when ChildType = 'RelatedLegalEntity' then 'Related Legal Entity' 
# MAGIC       when ChildType = 'RelatedNaturalPerson' then 'Related Natural Person' 
# MAGIC end as ChildParty_type
# MAGIC ,UniqueChildPartyId
# MAGIC , ParentIdentity
# MAGIC ,case when ParentType = 'LegalEntityClient' then 'Legal Entity' 
# MAGIC       when ParentType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when ParentType = 'RelatedLegalEntity' then 'Related Legal Entity' 
# MAGIC       when ParentType = 'RelatedNaturalPerson' then 'Related Natural Person' 
# MAGIC end as ParentParty_type
# MAGIC ,UniqueParentPartyId
# MAGIC , TypesOfRelation, IsUbo, UboReason, CalculatedShareholdingPercentage as ShareholdingPercentage,CalculatedVotingRightsPercentage as VotingRightPercentage
# MAGIC from party_client_structure_GUI
# MAGIC where IsLatestApprovedVersionofclient = 'True'

# COMMAND ----------

# DBTITLE 1,Unpivot Legacy2 Structure
# Load the Legacy2_ClientStructure table into a DataFrame
df_Legacy2Structure = spark.table("Legacy2_ClientStructure")

# Define a mapping of relative party types to their corresponding labels
RelativePartyType_mapping = {
    "HasAgent": "Agent",
    "HasAuthorisedRepresentative": "Authorised Representative",
    "HasBeneficiary": "Beneficiary",
    "HasDirector": "Director",
    "HasGuarantor": "Guarantor",
    "HasSettlorFounder": "Settlor/Founder",
    "HasTrustee": "Trustee",
    "HasIntegrator": "Integrator",
    "HasOther": "Other related party",
    "HasControllingPerson": "Controlling Person",
    "HasUboShareholding": "Shareholder",
    "HasEndOfChainShareholding": "Shareholder",
}

# Create a new DataFrame with an array column containing the relative party types
df_with_party_type_array = df_Legacy2Structure.withColumn(
    "RelativePartyTypeArray",
    F.array(
        *[
            F.when(F.col(col_name).cast("boolean"), F.lit(value)).otherwise(None)
            for col_name, value in RelativePartyType_mapping.items()
        ]
    ),
)

# Explode the array column to create a row for each non-null relative party type
df_result = df_with_party_type_array.withColumn(
    "RelativePartyType",
    F.explode(F.expr("filter(RelativePartyTypeArray,x -> x IS NOT NULL)")),
)

# Create or replace a temporary view with the result DataFrame
df_result.createOrReplaceTempView("L2_Structure")

# COMMAND ----------

# DBTITLE 1,Legacy2 Structure Details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%';
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select * from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_StructureDetails AS
# MAGIC with L2 as 
# MAGIC (
# MAGIC select distinct s.ClientGcobId,c.SalesforceClientID_nCino
# MAGIC ,case when s.ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       else s.ClientType
# MAGIC end as Party_type
# MAGIC ,ChildIdentity
# MAGIC ,case when s.ChildClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       else s.ChildClientType
# MAGIC end as ChildParty_type
# MAGIC ,ParentIdentity
# MAGIC ,case when s.ParentType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       else s.ParentType
# MAGIC end as ParentParty_type
# MAGIC ,s.RelativePartyType as TypesOfRelation,s.IsUbo,s.UbothroughReason as UboReason,s.Shareholding as ShareholdingPercentage,null as VotingRightPercentage 
# MAGIC from L2_Structure s
# MAGIC inner join Legacy2_client c on s.ClientId = c.ClientId
# MAGIC where c.ClientTypeId in (1,2,3)
# MAGIC and IsLatestApprovedVersionofclient = 'True'
# MAGIC )
# MAGIC select distinct
# MAGIC * 
# MAGIC ,CASE 
# MAGIC   WHEN Party_type = 'Legal Entity' THEN concat('LEGACY2_LE_', ClientGcobId)
# MAGIC   WHEN Party_type = 'Natural Person' THEN concat('LEGACY2_NP_NPPC_', ClientGcobId)
# MAGIC   WHEN Party_type = 'Related Legal Entity' THEN concat('LEGACY2_RLE_', ClientGcobId)
# MAGIC   WHEN Party_type = 'Related Natural Person' THEN concat('LEGACY2_RNP_', ClientGcobId)
# MAGIC END as Legacy2_Identifier
# MAGIC ,CASE 
# MAGIC   WHEN ChildParty_type = 'Legal Entity' THEN concat('LEGACY2_LE_', ChildIdentity)
# MAGIC   WHEN ChildParty_type = 'Natural Person' THEN concat('LEGACY2_NP_NPPC_', ChildIdentity)
# MAGIC   WHEN ChildParty_type = 'Related Legal Entity' THEN concat('LEGACY2_RLE_', ChildIdentity)
# MAGIC   WHEN ChildParty_type = 'Related Natural Person' THEN concat('LEGACY2_RNP_', ChildIdentity)
# MAGIC END as Legacy2_ChildUniqueIdentity
# MAGIC ,CASE 
# MAGIC   WHEN ParentParty_type = 'Legal Entity' THEN concat('LEGACY2_LE_', ParentIdentity)
# MAGIC   WHEN ParentParty_type = 'Natural Person' THEN concat('LEGACY2_NP_NPPC_', ParentIdentity)
# MAGIC   WHEN ParentParty_type = 'Related Legal Entity' THEN concat('LEGACY2_RLE_', ParentIdentity)
# MAGIC   WHEN ParentParty_type = 'Related Natural Person' THEN concat('LEGACY2_RNP_', ParentIdentity)
# MAGIC END as Legacy2_ParentUniqueIdentity
# MAGIC from L2

# COMMAND ----------

# DBTITLE 1,GCOB Details for NCINO Id
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_CaseClientDetails As
# MAGIC select * 
# MAGIC ,case when ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC from party_case_client_details
# MAGIC where CaseStatusName <> 'Cancelled'
# MAGIC and IsLatestApprovedVersionofclient = 'True'

# COMMAND ----------

# DBTITLE 1,Read GCDS "GCOB"-"GCOB-NCINO"-"GIC" Client data
# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_Clients As
# MAGIC select distinct
# MAGIC k.KeyStore_value as identifier
# MAGIC , k.KeyStore_type
# MAGIC , k.status AS status
# MAGIC , c.*
# MAGIC , pr.Party_role
# MAGIC , pr.Life_cycle_status
# MAGIC , rel.`Relationship-Type` as RelationshipType
# MAGIC , rel.`Relationship-Value` as RelationshipValue_GCID
# MAGIC from client_KeyStoreKey k
# MAGIC inner join client_Client c on c.GCID = k.GCID
# MAGIC left join client_PartyRole pr on k.GCID = pr.GCID
# MAGIC left join client_PartytoPartyRelationship rel on k.GCID = rel.GCID
# MAGIC where KeyStore_type in ('GCOBID','NCINOID','GIC')
# MAGIC --and k.status = 'Active'

# COMMAND ----------

# DBTITLE 1,Data Preparation for Party_SystemIdentifier
# MAGIC %sql
# MAGIC Create or replace temporary view Party_SystemIdentifier as
# MAGIC select distinct  
# MAGIC PartyIdentifier
# MAGIC ,Application
# MAGIC ,concat(Application,'_',LocalSystemIdentifier) as LocalSystemIdentifier
# MAGIC from P_SystemIdentifier

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - Other Sources Party Data

# COMMAND ----------

# DBTITLE 1,GCOB-GCDS Structure
# MAGIC %sql
# MAGIC Create or replace temporary view GCOB_Structure As
# MAGIC select distinct
# MAGIC 'GCOB' as Application
# MAGIC ,s.UniquePartyId as LocalSystemIdentifier
# MAGIC ,s.UniquePartyId
# MAGIC ,s.Party_type
# MAGIC ,ch.LocalSystemIdentifier as ChildLocalSystemIdentifier
# MAGIC ,ch.PartyIdentifier as PartyChildIdentifier
# MAGIC ,s.UniqueChildPartyId
# MAGIC ,s.ChildParty_type
# MAGIC ,pr.LocalSystemIdentifier as ParentLocalSystemIdentifier
# MAGIC ,pr.PartyIdentifier as PartyParentIdentifier
# MAGIC ,s.UniqueParentPartyId
# MAGIC ,s.ParentParty_type
# MAGIC ,s.TypesOfRelation, s.IsUbo, s.UboReason, s.ShareholdingPercentage,s.VotingRightPercentage
# MAGIC from Gcob_Structure_Details s
# MAGIC LEFT join Party_SystemIdentifier ch on CONCAT('GCOB_',s.UniqueChildPartyId) = ch.LocalSystemIdentifier
# MAGIC LEFT join Party_SystemIdentifier pr on CONCAT('GCOB_',s.UniqueParentPartyId) = pr.LocalSystemIdentifier

# COMMAND ----------

# DBTITLE 1,Legacy2-GCDS Structure
# MAGIC %sql
# MAGIC Create or replace temporary view Legacy2_Structure As
# MAGIC select distinct
# MAGIC 'Legacy2' as Application
# MAGIC ,s.Legacy2_Identifier as LocalSystemIdentifier
# MAGIC ,s.Legacy2_Identifier
# MAGIC ,s.Party_type
# MAGIC ,ch.LocalSystemIdentifier as Child_LocalSystemIdentifier
# MAGIC ,case when ch.LocalSystemIdentifier is null then s.Legacy2_ChildUniqueIdentity else ch.PartyIdentifier end as PartyChildIdentifier
# MAGIC ,s.Legacy2_ChildUniqueIdentity
# MAGIC ,s.ChildParty_type
# MAGIC ,pr.LocalSystemIdentifier as Parent_LocalSystemIdentifier
# MAGIC ,case when pr.LocalSystemIdentifier is null then s.Legacy2_ParentUniqueIdentity else pr.PartyIdentifier end as PartyParentIdentifier
# MAGIC ,s.Legacy2_ParentUniqueIdentity
# MAGIC ,s.ParentParty_type
# MAGIC ,s.TypesOfRelation, s.IsUbo, s.UboReason, s.ShareholdingPercentage,s.VotingRightPercentage
# MAGIC from Legacy2_StructureDetails s
# MAGIC LEFT join Party_SystemIdentifier ch on s.Legacy2_ChildUniqueIdentity = ch.LocalSystemIdentifier
# MAGIC LEFT join Party_SystemIdentifier pr on s.Legacy2_ParentUniqueIdentity = pr.LocalSystemIdentifier

# COMMAND ----------

# DBTITLE 1,GIC-GCDS Structure
# %sql
# Create or replace temporary view GIC_Structure As
# select distinct
# 'GIC' as Application
# ,s.LocalSystemIdentifier
# ,ch.LocalSystemIdentifier as Child_LocalSystemIdentifier
# ,ch.PartyIdentifier as PartyChildIdentifier
# ,pr.LocalSystemIdentifier as Parent_LocalSystemIdentifier
# ,pr.PartyIdentifier as PartyParentIdentifier
# ,s.TypesOfRelation, s.IsUbo, s.UboReason, s.ShareholdingPercentage,s.VotingRightPercentage
# from GIC_Structure_Details s
# LEFT join Party_SystemIdentifier ch on s.LocalChildIdentifier = ch.LocalSystemIdentifier
# LEFT join Party_SystemIdentifier pr on s.LocalParentIdentifier = pr.LocalSystemIdentifier

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temporary view GIC_Structure As
# MAGIC select distinct
# MAGIC 'GIC' AS Application,
# MAGIC concat('GIC_',COD_GIC_CLIENTE) as LocalSystemIdentifier,
# MAGIC concat('GIC_',COD_GIC_CLIENTE) as PartyChildIdentifier,
# MAGIC concat('GIC_',COD_GIC_VINCULO) AS PartyParentIdentifier,
# MAGIC TIPO_VINCULO AS TypesOfRelation,
# MAGIC CASE WHEN TIPO_VINCULO = 'Beneficiário' and SUB_TIPO_VINCULO = 'Beneficiário Final' THEN true ELSE false END AS IsUbo,
# MAGIC NULL AS UboReason,
# MAGIC NULL AS ShareholdingPercentage,
# MAGIC NULL AS VotingRightPercentage
# MAGIC from vwgic_cliente_vinculados

# COMMAND ----------

# DBTITLE 1,Union Data
# MAGIC %sql
# MAGIC Create or replace temporary view Party_Structure As
# MAGIC select distinct Application,LocalSystemIdentifier,PartyChildIdentifier,PartyParentIdentifier,TypesOfRelation,IsUbo,UboReason,ShareholdingPercentage,VotingRightPercentage From GCOB_Structure
# MAGIC union
# MAGIC select distinct Application,LocalSystemIdentifier,PartyChildIdentifier,PartyParentIdentifier,TypesOfRelation,cast(IsUbo as BOOLEAN),UboReason,ShareholdingPercentage,VotingRightPercentage From Legacy2_Structure
# MAGIC union
# MAGIC select distinct Application,LocalSystemIdentifier,PartyChildIdentifier,PartyParentIdentifier,TypesOfRelation,cast(IsUbo as BOOLEAN),UboReason,ShareholdingPercentage,VotingRightPercentage From GIC_Structure

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_structure=spark.table('Party_Structure')

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_party_structure = add_party_identifier(df_party_structure, df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
save_to_saradar_storage_account(df_party_structure, party_dataobject, radar_datamodel_version_number, environment)
