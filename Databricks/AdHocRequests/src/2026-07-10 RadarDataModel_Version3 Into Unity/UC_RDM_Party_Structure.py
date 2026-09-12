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
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load
# MAGIC | Aayushi Jain | 11-Feb-2026 | 14165735 | adding data from MDM-RANZ GDP
# MAGIC |Rhea Gupta | 30-Mar-2026 |15580816 | Included RANZ in version 3
# MAGIC | Mahalakshmi V | 20-May-2026 | 16071492 | Included RANZ 
# MAGIC |Hari | 23-June-2026 |16314785 | RANZ Lookup Table Handling Enhancement]
# MAGIC  
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
radar_datamodel_version_number=3
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
SARADAR = "saradar" + environment

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Defining Load_Date and RunType
dbutils.widgets.text("Load_Date", "")
dbutils.widgets.text("RunType", "daily")  # default is daily

Load_Date = dbutils.widgets.get("Load_Date")
RunType = dbutils.widgets.get("RunType").lower()
if Load_Date:
    RunType="historical"
    
if not Load_Date:
    Load_Date = datetime.today().strftime('%Y%m%d')

print(f"Running for Load Date: {Load_Date}")

# COMMAND ----------

# DBTITLE 1,Defining the dataobjects' name
party_dataobject='Party_Structure'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(SARADAR)
authenticate_storage_account('edlcorestdauprod0001')

# COMMAND ----------

# DBTITLE 1,Deriving required date parameters
# Derive the date for which data has to be processed from GDP
load_dts = f"EDL_LOAD_DTS={Load_Date}*"
print (load_dts)

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
    Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=dataobject,Load_Date=Load_Date)

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
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService',Load_Date=Load_Date)

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
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read GIC data from GDP defined layer
import pandas as pd # print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'vwgic_cliente_vinculados'
]})
#gic_load_dts
# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier dataobject
df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/3/data/{load_dts}/*.parquet"
)
df_Party_SystemIdentifier.createOrReplaceTempView('P_SystemIdentifier')

# COMMAND ----------

# DBTITLE 1,Read MDM-RANZ data from GDP and extracting latest data to handle incremental load
# List of datasets from GDP
load_df =[
'c_b_party_rel_party_xref',
'c_b_party_xref'
,'c_lkp_prty_relt_rol_type_xref'
]
load_ranz_v4_rdm_tables(load_df, Load_Date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GCDS - Other Sources Data Prepration

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



# COMMAND ----------

# DBTITLE 1,Legacy2 Structure Details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = TRUE and GcobCaseId is null and Value is null and GcobId like 'RA:%';
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



# COMMAND ----------

# DBTITLE 1,Data Preparation for Party_SystemIdentifier
# MAGIC %sql
# MAGIC Create or replace temporary view Party_SystemIdentifier as
# MAGIC select distinct  
# MAGIC PartyIdentifier
# MAGIC ,concat(Application,'_',LocalSystemIdentifier) as LocalSystemIdentifier
# MAGIC from P_SystemIdentifier

# COMMAND ----------

# DBTITLE 1,alternative party_system Identifier
# MAGIC %skip
# MAGIC # this party system Identifier comes from another notebook
# MAGIC # df_Party_SystemIdentifier = spark.read.parquet(
# MAGIC     f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/3/data/{load_dts}/*.parquet"
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - Other Sources Party Data

# COMMAND ----------

# DBTITLE 1,GCOB-GCDS Structure
# MAGIC %sql
# MAGIC Create or replace temporary view GCOB_Structure As
# MAGIC select distinct
# MAGIC s.UniquePartyId as LocalSystemIdentifier
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
# MAGIC s.Legacy2_Identifier as LocalSystemIdentifier
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

# MAGIC %sql
# MAGIC Create or replace temporary view GIC_Structure As
# MAGIC select distinct
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

# DBTITLE 1,Outdated Ranz create static table for ranz party relation description
# # Define the lists
# # Read_GDP_Defined_DataObjects_RANZ('mdm-ranz','c_b_party_rel_party_xref')
# relation_dict ={ 
#                 "Code":["ACCE","ACCO","ACCOOF","ACEX","ADMI","ADMIOF","APPO","ASSO","AUREP","AUSI","AUSID","AUSIOF","AUSIOFD","AUSIOFR","AUSIR","BANK","BENE","BENEOF","BETG","BRAN","BRIL","BROK","BUSI","CEOO","CEOOOF","CFOO","CFOOOF","CHAI","CHAIOF","CHIL","CLCL","CLIE","COME","COMEOF","CONS","COOO","CORP","CORPOF","CPER","DAIL","DAIR","DECE","DEFA","DIRE","DIREOF","EMPE","EMPL","ESTA","EXEC","EXECOF","FILA","FIPL","FUND","FWOROF","GEPA","GEPAOF","GMEM","GROU","GUAR","HAAE","HEAD","HOUS","HOUSOF","INBKO","INSU","JVEN","JVENOF","LIPA","LIPAOF","LIQU","LIQUOF","MACM","MANA","MANOF","MEMB","MINR","MINROF","MOIL","NBEN","NCCS","NOMD","NOMG","OFFI","OFFIOF","OFRE","OFREOF","OTHE","PARE","PARTN","PARTOF","PASO","POAE","POAG","POAL","POAO","POAEOF","POAGOF","POALOF","POAOOF","PRIM","PRLI","PRLIOF","PROT","PROV","REBY","RECE","REFE","REMA","REMAOF","REOF","RISK","SAMA","SCRE","SECOF","SECR","SECROF","SECT","SECU","SENI","SETT","SETTOF","SHAR","SHAROF","SIBL","SIIL","SOIL","SOLI","SOWN","SOWNOF","SPOU","STAG","STKH","SUBF","SUBS","TREA","TREAOF","TRUS","TRUSOF","UBO","UBOOF","UHOL"],
#                 "Description":["Access Provider","Accountant","Accountant Of","Account Executive","Administrator","Administrator","Appointer","","Authorised Representative","","Auth Sig RaboDirect","","Auth Sig RaboDirect Of","Auth Sig RaboBank Of","Auth Sig RaboBank","Bank","Beneficiary","Beneficiary","Belongs To Group","Head Office","Brother-In-Law","Broker","Operates A Business","Chief Executive Officer","Chief Executive Officer","Chief Financial Officer","Chief Financial Officer","Chairperson","Chairperson","Parent","Client-Of-Client","Bank Client","Committee Member","Committee Member","Consultant","Chief Operating Officer","Partner","Corporate Partner","Controlling Person(FATCA/CRS) Controlling Person","Daughter-In-Law","Dairy Company","The Deceased","Defacto","Director","Director","Employee","Employer","Estate","Executor","Executor","Father-In-Law","Financial Planner","Is A Fund Of","Fellow Worker Of","General Partner","General Partner","Group Member","Group Company","Guarantor","Has As Affiliated Employer","Head Office","Household Member","Household Member Of","Internet Banking Only User","Insurance Company","Joint Venture Owner To","Joint Venture Owner Of","Limited Partner","Limited Partner","Liquidator","Liquidator","MAC Manager","Managing Director","Manager Of","Member","Minor","Minor","Mother-In-Law","Non-Beneficial/Shareholder Product Backlog Item 14369716","NCC SME","Nominee Director","Nominee General Partner","Officer","Officer","Official Receiver","Official Receiver","Other","Parent","Family Partner","Partner Of","Panel Solicitor","POA - Enduring","POA - General Supportive","POA - Limited","POA - Other","POA - Enduring","POA - General Supportive","POA - Limited","POA - Other","Primary Contact","Provisional Liquidator","Provisional Liquidator","Protector/Guardian","Provider/Contact","Referred By","Receiver","Referrer","Receiver Manager","Receiver Manager","Receiver","Belongs To Risk Group","Special Asset Management","Screening","Secretary Of","Secretary","Secretary","Security/Custodial Trustee","Security Provider","Senior Officer","Settlor","Settlor","Shareholder","Shareholder","Sibling","Sister-In-Law","Son-In-Law","Solicitor","Same Owner To","Same Owner To","Spouse","Stock Agent","Stakeholder","Is A Subfunds (Compartment) Of","Group Company","Treasurer","Treasurer","Trustee","Trustee","Ultimate Beneficial Owner","Ultimate Beneficial Owner","Unit Holder"]
# }

# # Create a DataFrame from the lists
# df_static_relationship = spark.createDataFrame(list(zip(*relation_dict.values())), list(relation_dict.keys()) )

# df_static_relationship.createOrReplaceTempView('static_relationship_desc')

# COMMAND ----------

# DBTITLE 1,CREATE RANZ STRUCTURE
# MAGIC %sql
# MAGIC Create or replace temporary view RANZ_Structure As
# MAGIC select distinct
# MAGIC concat('RANZ_',party_rel_party.SRC_PARTY_ID) as LocalSystemIdentifier,
# MAGIC concat('RANZ_',party_child.SRC_PARTY_ID) as PartyChildIdentifier,
# MAGIC concat('RANZ_',party_parent.SRC_PARTY_ID) AS PartyParentIdentifier,
# MAGIC relation_desc.PRTY_RELT_ROLE_TYPE_DESC AS TypesOfRelation,
# MAGIC CASE WHEN party_rel_party.REL_TYPE_CD = 'UBO' THEN true ELSE false END AS IsUbo,
# MAGIC NULL AS UboReason,
# MAGIC party_rel_party.OWNERSHIP_PCT AS ShareholdingPercentage,
# MAGIC NULL AS VotingRightPercentage
# MAGIC from c_b_party_rel_party party_rel_party
# MAGIC left join c_b_party party_parent 
# MAGIC on party_rel_party.FK_party_id = party_parent.ROWID_XREF
# MAGIC left join c_b_party party_child
# MAGIC on party_rel_party.FK_rel_party_id = party_child.ROWID_XREF
# MAGIC left JOIN c_lkp_prty_relt_rol_type relation_desc
# MAGIC on party_rel_party.REL_TYPE_CD = relation_desc.PRTY_RELT_ROLE_TYPE_CODE
# MAGIC

# COMMAND ----------

# DBTITLE 1,Union Data
# MAGIC %sql
# MAGIC Create or replace temporary view Party_Structure As
# MAGIC select distinct LocalSystemIdentifier,PartyChildIdentifier,PartyParentIdentifier,TypesOfRelation,IsUbo,UboReason,ShareholdingPercentage,VotingRightPercentage From GCOB_Structure
# MAGIC union
# MAGIC select distinct LocalSystemIdentifier,PartyChildIdentifier,PartyParentIdentifier,TypesOfRelation,cast(IsUbo as BOOLEAN),UboReason,ShareholdingPercentage,VotingRightPercentage From Legacy2_Structure
# MAGIC union
# MAGIC select distinct LocalSystemIdentifier,PartyChildIdentifier,PartyParentIdentifier,TypesOfRelation,cast(IsUbo as BOOLEAN),UboReason,ShareholdingPercentage,VotingRightPercentage From GIC_Structure
# MAGIC union 
# MAGIC select distinct LocalSystemIdentifier,PartyChildIdentifier,PartyParentIdentifier,TypesOfRelation,cast(IsUbo as BOOLEAN),UboReason,ShareholdingPercentage,VotingRightPercentage FROM RANZ_Structure

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_structure=spark.table('Party_Structure')

# COMMAND ----------

display(df_party_structure.limit(5))

# COMMAND ----------

# DBTITLE 1,Adding PartyIdentifier to the final dataobject
df_party_structure = add_party_identifier(df_party_structure, df_Party_SystemIdentifier)

# COMMAND ----------

# DBTITLE 0,Saving to SARADAR Storage account
if RunType == "historical":
    save_to_saradar_storage_account(df_party_structure, party_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_party_structure, party_dataobject, radar_datamodel_version_number, environment)

# COMMAND ----------


