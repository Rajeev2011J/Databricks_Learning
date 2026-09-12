# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To Achive Data Model result by considering All the source systems where W&R Clients are available and GCDS source
# MAGIC
# MAGIC #### author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Take W&R clients data from different source systems
# MAGIC - Take GCDS data
# MAGIC - Join on GCDSId to further derive required output
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |28-Aug-205 |13412714 |Moving to version 2 and removing unwanted columns
# MAGIC | Abhishek Jaiswal   |01-Sep-2025 | 13449940| Corrected LifeCycle Status	
# MAGIC | Abhishek Jaiswal   |25-Sep-2025 | 13762388| Adding new column IsRabobankEntity	
# MAGIC | Abhishek Jaiswal   |09-Oct-2025 | 13843194| Adding new columns `HO_reporting_party-FINREP_Code` & `HO_reporting_party-FINREP`
# MAGIC | Rajeev Kumar       |16-Oct-2025	| 13485489| Add KYCMasterListRegistry v2 Dataset and add two new column KYCGroup and SectorTeam
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load 
# MAGIC | Mahalakshmi V | 11-Feb-2025 |15058773 | Included MDM RANZ Source

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read Files from GDP

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
import os
from pyspark.sql.functions import regexp_extract
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=2
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
AU_GDP_Defined_Storage_Account=os.environ['AU_GDP_Defined_Storage_Account']
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
party_dataobject='Party'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(SARADAR)
authenticate_storage_account(AU_GDP_Defined_Storage_Account)

# COMMAND ----------

# DBTITLE 1,Define Date variables
#Derive the date for which data has to be processes
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
load_dts = f"EDL_LOAD_DTS={Load_Date}*"
print (load_dts)

# COMMAND ----------

# DBTITLE 1,Read GCDS data from GDP defined layer
# print list of strings for loading spark dfs from GDP
gcds_df = pd.DataFrame({'definedDatasetname':[
'client_Client',
'client_KeyStoreKey',
'client_OnboardedLocations',
'client_PartyRole',
'client_PartytoPartyRelationship',
'client_Products'
]})

# Create TempView for each loading table
for index, row in gcds_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=row.definedDatasetname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read GCOB data from GDP defined layer
# print list of strings for loading spark dfs from GDP
load_df =[
'party_case_client_details',
'party_AllPartyDetails' 
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read Legacy2 data from GDP defined layer
# List of datasets from GDP
load_df = [
'Legacy2_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2',Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read GIC data from GDP defined layer
import pandas as pd # print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'pessoa_tipo_cadastro_status'
,'pessoa_regulatorio_global_tipo_regulatorio'
,'pessoa_regulatorio_global'
,'pessoa_linha_negocio'
,'pessoa'
,'pessoa_fisica'
,'pessoa_juridica'
,'tipo_sociedade'
,'endereco'
,'cad_pais'
,'status_tipo_cadastro'
,'vwgic_rdl_pessoa_tipo_cadastro'
]})
#gic_load_dts
# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Read NLSVF data from GDP defined layer
# import pandas as pd # print list of strings for loading spark dfs from GDP
# load_df = pd.DataFrame({'GDPname':[
#  'nls_dbo_cif_detail'
# ,'nls_dbo_cif'
# ,'nls_dbo_loanacct'
# ]})
# #gic_load_dts
# # Create TempView for each loading table
# for index, row in load_df.iterrows():
#     Read_GDP_Defined_DataObjects(Source='NLSVF', Dataobject=row.GDPname)

# COMMAND ----------

# DBTITLE 1,Reading KN1 Dataobjects from GDP
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'ADKYC_CONTRAPARTES'
,'ADKYC_CONTRAPARTES_COMPL'
,'ADKYC_CONTRAPARTES_FASES'
,'ADKYC_SITUACOES'
,'ADKYC_RISCOS'
,'ADKYC_CONTRAPARTES_DESATIVADOS'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='KN1', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier dataobject
df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/1/data/{load_dts}/*.parquet"
)

# COMMAND ----------

# DBTITLE 1,Reading KN1 Cases dataobject
df_Party_KN1Cases = spark.read.parquet(f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_KN1Cases/1/data/{load_dts}/*.parquet")
df_Party_KN1Cases.createOrReplaceTempView("KN1Cases")

# COMMAND ----------

# DBTITLE 1,Reading KYCMasterListRegistry Dataobjects from GDP
# List of datasets from GDP
load_df_KYCMasterListRegistry = [
'KYCMasterListRegistry'
]

# Create TempView for each loading table
for Object in load_df_KYCMasterListRegistry:
    Read_GDP_Defined_DataObjects(Source='RadarDataModel', Dataobject=Object,path_prefix='RadarPowerApps',Load_Date=Load_Date)

# COMMAND ----------

# List of dataobjects from GDP
RANZ_Dataobjects_List = [
'c_b_party_xref',
'c_b_party_dom_cntry_xref'
,'c_b_party_pep_am_xref'
,'c_b_contract_xref'
,'c_b_contr_rol_party_xref'
]

# Create TempView for each loading table
for Object in RANZ_Dataobjects_List:
    Read_GDP_Defined_DataObjects_RANZ(Source='RANZ-MDM', Dataobject=Object,Load_Date='')

# COMMAND ----------

# DBTITLE 1,Map region to the relevant client owner locations
# Define the lists
ClientOwnerLocation_list = [
    'Rabobank New York', 'Rabobank Netherlands', 'Rabobank London', 'Rabobank India',
    'Rabobank Paris', 'Rabobank Frankfurt', 'Rabobank Australia', 'Rabobank - USA Rabo AgriFinance',
    'Rabobank New Zealand', 'Rabobank Foundation', 'Rabobank Chile', 'Rabobank Argentina',
    'Rabobank - RANZ Country Banking and ROS', 'Australia', 'Rabobank Hong Kong', 'Rabobank Singapore',
    'Rabobank China', 'Rabobank Canada(Rural)', 'Netherlands', 'Rabobank Dublin', 'New Zealand',
    'Rabobank Madrid', 'Rabobank Antwerp', 'Rabobank Canada (RCBR)', 'Rabobank Milan',
    'Rabobank Turkey', 'India', 'Kenya', 'Rabo Securities USA, Inc. (RSEC)', 'Rabobank Indonesia',
    'Singapore', 'Turkey', 'Germany', 'France', 'Argentina', 'Belgium', 'London', 'Chile',
    'Italy', 'Chicago', 'RAF', 'Spain', 'Ireland', 'Hong Kong', 'Atlanta', 'Mexico',
    'Rabobank Brazil', 'Rabobank Malaysia', 'Utrecht', 'Rabobank Kenya', 'Canada', 'Brazil',
    'New York', 'Shanghai','UTRECHT']
Region_list = [
    'NORTHAMERICA', 'EUROPEAFRICA', 'EUROPEAFRICA', 'ASIA', 'EUROPEAFRICA', 'EUROPEAFRICA',
    'RANZ', 'NORTHAMERICA', 'RANZ', 'EUROPEAFRICA', 'SOUTHAMERICA', 'SOUTHAMERICA', 'RANZ',
    'RANZ', 'ASIA', 'ASIA', 'ASIA', 'NORTHAMERICA', 'EUROPEAFRICA', 'EUROPEAFRICA', 'RANZ',
    'EUROPEAFRICA', 'EUROPEAFRICA', 'NORTHAMERICA', 'EUROPEAFRICA', 'EUROPEAFRICA', 'ASIA',
    'EUROPEAFRICA', 'NORTHAMERICA', 'ASIA', 'ASIA', 'EUROPEAFRICA', 'EUROPEAFRICA',
    'EUROPEAFRICA', 'SOUTHAMERICA', 'EUROPEAFRICA', 'EUROPEAFRICA', 'SOUTHAMERICA',
    'EUROPEAFRICA', 'NORTHAMERICA', 'NORTHAMERICA', 'EUROPEAFRICA', 'EUROPEAFRICA', 'ASIA',
    'NORTHAMERICA', 'NORTHAMERICA', 'SOUTHAMERICA', 'ASIA', 'EUROPEAFRICA', 'EUROPEAFRICA',
    'NORTHAMERICA', 'SOUTHAMERICA', 'NORTHAMERICA', 'ASIA','EUROPEAFRICA']

# Create a DataFrame from the lists
df_static_ClientOwnerRegion = spark.createDataFrame(zip(ClientOwnerLocation_list, Region_list), ['ClientOwnerLocation', 'GlobalClientRegion'])

df_static_ClientOwnerRegion.createOrReplaceTempView('static_ClientOwnerRegion')


# COMMAND ----------

# DBTITLE 1,Static Code to Description Mapping for RANZ Attributes
Ranz_Pep_Evaluation_Static_Dict ={'01'	:'PEP',
'02':	'Close associate of a PEP',
'03':	'Immediate family member of a PEP',
'04':	'Not a PEP'}

sdf_Ranz_Pep_Evaluation_Static_Mapping =spark.createDataFrame([(code, description) for code, description in Ranz_Pep_Evaluation_Static_Dict.items()],['PEP_EVAL_CD','PEP_EVAL_DESCRIPTION'])
sdf_Ranz_Pep_Evaluation_Static_Mapping.createOrReplaceTempView('Ranz_Pep_Evaluation_Static_Mapping')

Ranz_Client_Lifecycle_Status_Dict = {
    "-2": "UNKNOWN",
    "AC": "Active Client",
    "ACPR": "Active-Pending Risk Client",
    "AP": "Applicant Client",
    "BL": "Blocked Client",
    "BL01": "Blocked - Post No Debits",
    "BL02": "Blocked - Post No Credits",
    "BL03": "Blocked - Post No Entries",
    "BL04": "Blocked - Pending Documentation",
    "BL05": "Blocked - Deceased Estate",
    "BL06": "Blocked - Closure Quoted",
    "BL07": "Blocked - Account on Referral List",
    "BL08": "Blocked - Account on Referral List - CR",
    "BL09": "Blocked - Account on Referral List - DR",
    "BL10": "Blocked - Post Credits to Savings a/c",
    "BL11": "Blocked - Post Debits to Current a/c",
    "BL12": "Blocked - Customer Deceased",
    "BL20": "Blocked - Hold Debits: Funds Held as Security",
    "BL50": "Blocked - Setting up of company",
    "BL51": "Blocked - Overdraw not allowed",
    "BL52": "Blocked - Management authorisation",
    "BL53": "Blocked - General Debit Block",
    "BL54": "Blocked - General DR & CR block",
    "BL55": "Blocked - Several blocking codes",
    "BL56": "Blocked - Judicial instructions",
    "BL57": "Blocked - Missing documents",
    "BL58": "Blocked - Temporary blocking",
    "BL59": "Blocked - Multiple blocking",
    "BL60": "Blocked - ATO/IRD request",
    "BL61": "Blocked - Litigation",
    "BL62": "Blocked - Centrelink",
    "BL63": "Blocked - Pending client onboarding",
    "BL64": "Blocked - Incomplete CDD",
    "BL65": "Blocked - Client request block",
    "BL66": "Blocked - Multiple blocking",
    "BL67": "Blocked - Inactive customer",
    "BL68": "Blocked - Overdue remediation",
    "BL69": "Blocked - Bulk account closure remove",
    "BL70": "Blocked - Account being closed",
    "BL71": "Blocked - Pending Client exit",
    "BL72": "Blocked - Pending Client exit DB",
    "BL73": "Blocked - Pending Client exit CR",
    "BL74": "Blocked - Fraud",
    "BL75": "Blocked - For failed authentication",
    "BL76": "Blocked - For disabled end user",
    "BL77": "Blocked - Login Cancelled",
    "BL78": "Blocked - Login Not Used",
    "BL79": "Blocked - Login Cancelled - 1 to 7 years",
    "BL80": "Blocked - Login Cancelled - 0 to 1 years",
    "BL90": "Blocked - Automatic Closure",
    "BL91": "Blocked - Temp Auto Closing",
    "BL99": "Blocked - Account Closure",
    "CL": "Closed Client",
    "DC": "Declined Client",
    "DL": "Deleted",
    "PR": "Prospect Client",
    "WD": "Withdrawn Client",
    "WD01": "Withdrawn - Lost to competitor",
    "WD02": "Withdrawn - Structural conditions could not be met",
    "WD03": "Withdrawn - Not proceeding with funding request",
    "WD04": "Withdrawn - Time constraints",
    "WD05": "Withdrawn - To resubmit",
    "WD06": "Withdrawn - Other",
    "WD07": "Withdrawn - To apply for low cost/no fee account",
    "DC01": "Declined - Credit declined",
    "DC02": "Declined - Unacceptable AML",
    "DC03": "Declined - Other"
}

sdf_RANZ_Client_Lifecycle_Status_Mapping = spark.createDataFrame([(code, desc) for code, desc in Ranz_Client_Lifecycle_Status_Dict.items()],["LIFECYCLE_STATUS_CD", "CLIENT_LIFECYCLE_STATUS_DESCRIPTION"])

sdf_RANZ_Client_Lifecycle_Status_Mapping.createOrReplaceTempView("RANZ_Client_Lifecycle_Status_Mapping")

Ranz_Adverse_Media_Dict = {
    "01": "No Alert",
    "02": "False Positive",
    "03": "Non-relevant Adverse Information",
    "04": "Relevant Adverse Information >= 5 years",
    "05": "Relevant Adverse Information < 5 years"
}

sdf_Ranz_Adverse_Media_Mapping =spark.createDataFrame([(code, description) for code, description in Ranz_Adverse_Media_Dict.items()],['AM_EVAL_CD','AM_EVAL_DESCRIPTION'])
sdf_Ranz_Adverse_Media_Mapping.createOrReplaceTempView('Ranz_Adverse_Media_Mapping')

# COMMAND ----------

# DBTITLE 1,Creates a temp view named KYCMasterListRegistry_SQL and Adds a derived column UniqueGcobId_Numeric
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW KYCMasterListRegistry_SQL AS
# MAGIC SELECT
# MAGIC   Id,
# MAGIC   UniqueGcobId,
# MAGIC   CASE 
# MAGIC     WHEN UniqueGcobId RLIKE '^\\d+$' THEN CONCAT('LEC_', UniqueGcobId)
# MAGIC     WHEN UniqueGcobId LIKE 'NP_%' THEN CONCAT('NP_NPPC_', regexp_extract(UniqueGcobId, '\\d+', 0))    
# MAGIC     ELSE UniqueGcobId
# MAGIC   END AS KYCMasterUniqueGcobId,
# MAGIC   KYCGroup,
# MAGIC   SectorTeam,
# MAGIC   ReviewLocation,
# MAGIC   CDDExecution,
# MAGIC   ClientCaseInitiationStart,
# MAGIC   Reason,
# MAGIC   ReasonExplanation,
# MAGIC   LondonSectorTeam,
# MAGIC   RecordActiveAt,
# MAGIC   RecordExpiredAt,
# MAGIC   IsActiveRecord,
# MAGIC   Source,
# MAGIC   EDL_LOAD_DTS,
# MAGIC   EDL_ACT_DTS,
# MAGIC   EDL_ACT_DTS_UTC
# MAGIC FROM KYCMasterListRegistry

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - Other Sources Data Prepration

# COMMAND ----------

# DBTITLE 1,GIC Clients update to join
# MAGIC %sql
# MAGIC Create or replace temporary view GIC_ClientDetails As
# MAGIC with DTA_NASCIMENTO as 
# MAGIC (
# MAGIC   select max(SEQ_HISTORICO) as SEQ_HISTORICO,SEQ_PESSOA from PESSOA_FISICA where FLG_PENDENTE = 'False' group by SEQ_PESSOA
# MAGIC )
# MAGIC -- physical birth date? / incorp date?
# MAGIC ,PESSOA_FISICA_DTA_NASCIMENTO 
# MAGIC (
# MAGIC   select f.SEQ_PESSOA,CAST(f.DTA_NASCIMENTO AS DATE) from PESSOA_FISICA f -- PBI: 14525246
# MAGIC   inner join DTA_NASCIMENTO n on f.SEQ_PESSOA = n.SEQ_PESSOA and f.SEQ_HISTORICO = n.SEQ_HISTORICO
# MAGIC )
# MAGIC -- identification
# MAGIC ,JURIDICA_IDENTIFICACAO as 
# MAGIC (
# MAGIC   select max(SEQ_HISTORICO) as SEQ_HISTORICO, SEQ_PESSOA from PESSOA_JURIDICA where FLG_PENDENTE = 'False' group by SEQ_PESSOA
# MAGIC )
# MAGIC ,PESSOA_JURIDICA_IDENTIFICACAO 
# MAGIC (
# MAGIC   select f.SEQ_PESSOA,f.NUM_CNPJ as Entity_TinOrEquivalent ,f.NUM_IDENTIFICACAO_FISCAL, CAST(f.DTA_CONSTITUICAO AS DATE), -- PBI: 14525246
# MAGIC   E_Nat.NOM_PAIS as Entity_Nationality,E_Nat.NOM_PAIS as CountryOfTaxResidence,E_Nat.SGL_PAIS as NationalityIsoCode, lf.DES_TIPO_SOCIEDADE as LegalForm
# MAGIC   from PESSOA_JURIDICA f
# MAGIC   inner join JURIDICA_IDENTIFICACAO i on f.SEQ_PESSOA = i.SEQ_PESSOA and f.SEQ_HISTORICO = i.SEQ_HISTORICO
# MAGIC   left join tipo_sociedade lf on f.SEQ_TIPO_SOCIEDADE = lf.SEQ_TIPO_SOCIEDADE
# MAGIC   left join cad_pais E_Nat on f.SEQ_PAIS = E_Nat.SEQ_PAIS
# MAGIC )
# MAGIC ,fisica_IDENTIFICACAO as 
# MAGIC (
# MAGIC   select max(SEQ_HISTORICO) as SEQ_HISTORICO,SEQ_PESSOA from pessoa_fisica where FLG_PENDENTE = 'False' group by SEQ_PESSOA
# MAGIC )
# MAGIC ,PESSOA_fisica_IDENTIFICACAO 
# MAGIC (
# MAGIC   select f.SEQ_PESSOA,f.NUM_CPF as NP_TinOrEquivalent ,f.NUM_DOC_IDENTIFICACAO as IdentificationDocumentNumber,cit.NOM_PAIS as Citizenship,nat.NOM_PAIS as NP_Nationality,nat.SGL_PAIS as NationalityIsoCode from pessoa_fisica f
# MAGIC   inner join fisica_IDENTIFICACAO fi on f.SEQ_PESSOA = fi.SEQ_PESSOA and f.SEQ_HISTORICO = fi.SEQ_HISTORICO
# MAGIC   left join cad_pais nat on f.SEQ_PAIS = nat.SEQ_PAIS
# MAGIC   left join cad_pais cit on f.SEQ_PAIS = cit.SEQ_PAIS
# MAGIC )
# MAGIC ,Status as 
# MAGIC (
# MAGIC select max(SEQ_HISTORICO) as SEQ_HISTORICO,SEQ_PESSOA,SEQ_TIPO_CADASTRO from PESSOA_TIPO_CADASTRO_STATUS where FLG_PENDENTE = 'False' and SEQ_TIPO_CADASTRO in (1)--, 5, 6, 7, 24, 14, 15, 16, 30, 19)  
# MAGIC group by SEQ_PESSOA,SEQ_TIPO_CADASTRO
# MAGIC )
# MAGIC ,StatusName as
# MAGIC (
# MAGIC select distinct
# MAGIC l.SEQ_PESSOA
# MAGIC ,s.DES_STATUS_TIPO_CADASTRO as StatusName
# MAGIC from Status l
# MAGIC inner join PESSOA_TIPO_CADASTRO_STATUS p on l.SEQ_PESSOA=p.SEQ_PESSOA and l.SEQ_HISTORICO = p.SEQ_HISTORICO and l.SEQ_TIPO_CADASTRO = p.SEQ_TIPO_CADASTRO
# MAGIC inner join STATUS_TIPO_CADASTRO s on p.SEQ_STATUS_TIPO_CADASTRO = s.SEQ_STATUS_TIPO_CADASTRO
# MAGIC )
# MAGIC ,GIC_Details as
# MAGIC (
# MAGIC select distinct
# MAGIC p.SEQ_PESSOA 
# MAGIC ,p.COD_INSTITUCIONAL
# MAGIC ,p.NOM_COMPLETO
# MAGIC ,CASE WHEN p.SEQ_TIPO_PESSOA = '2' and c.SEQ_TIPO_CADASTRO = 1 THEN 'Natural Person'
# MAGIC      WHEN p.SEQ_TIPO_PESSOA = '1' and c.SEQ_TIPO_CADASTRO = 1 THEN 'Legal Entity'
# MAGIC else c.DES_TIPO_CADASTRO END AS Party_type
# MAGIC ,f.DTA_NASCIMENTO
# MAGIC ,i.NUM_IDENTIFICACAO_FISCAL as GIIN
# MAGIC ,i.DTA_CONSTITUICAO as IncorporationDate
# MAGIC ,i.LegalForm
# MAGIC ,r1.FLG_APLICABILIDADE as FATCA_FLG_APLICABILIDADE
# MAGIC ,r1.DES_CLASSIFICACAO as FATCA_DES_CLASSIFICACAO
# MAGIC ,r1.DES_MOTIVO as FatcaComments
# MAGIC ,r2.FLG_APLICABILIDADE as CRS_FLG_APLICABILIDADE
# MAGIC ,r2.DES_CLASSIFICACAO as CRS_DES_CLASSIFICACAO
# MAGIC ,r2.DES_MOTIVO as CRS_DES_MOTIVO
# MAGIC ,gco.NOM_COMPLETO as GlobalClientOwner
# MAGIC ,l.StatusName
# MAGIC ,p.FLG_ESTRANGEIRA as Tinavailable
# MAGIC --,CASE WHEN p.SEQ_TIPO_PESSOA = '2' THEN fisica.NP_Tinavailable else i.Entity_Tinavailable END AS Tinavailable
# MAGIC ,CASE WHEN p.SEQ_TIPO_PESSOA = '2' THEN fisica.NP_Nationality else i.Entity_Nationality END AS Nationality
# MAGIC ,CASE WHEN p.SEQ_TIPO_PESSOA = '2' THEN fisica.NationalityIsoCode else i.NationalityIsoCode END AS NationalityIsoCode
# MAGIC ,i.CountryOfTaxResidence
# MAGIC ,fisica.IdentificationDocumentNumber
# MAGIC ,fisica.Citizenship
# MAGIC --,p.FLG_ESTRANGEIRA as TinOrEquivalent
# MAGIC ,CASE WHEN p.SEQ_TIPO_PESSOA = '2' THEN fisica.NP_TinOrEquivalent else i.Entity_TinOrEquivalent END AS TinOrEquivalent
# MAGIC ,CONCAT('GIC_', p.COD_INSTITUCIONAL) as LocalSystemIdentifier
# MAGIC ,'Rabobank Brazil' as GlobalClientOwnerLocation
# MAGIC ,case when c.DES_STATUS_TIPO_CADASTRO = 'Pré Cadastro' then 'Prospect'
# MAGIC       when c.DES_STATUS_TIPO_CADASTRO = 'Ativo' AND c.DES_TIPO_CADASTRO ='Cliente' then 'Client'
# MAGIC       when c.DES_STATUS_TIPO_CADASTRO = 'Ativo' AND c.DES_TIPO_CADASTRO !='Cliente' then 'Active'
# MAGIC       when c.DES_STATUS_TIPO_CADASTRO = 'Inativo' then 'Former Client'
# MAGIC       else null
# MAGIC  end as ClientLifeCycleStatus
# MAGIC ,cs.IsLatestCase as IsLatestApprovedVersionOfClient
# MAGIC from pessoa p
# MAGIC LEFT JOIN PESSOA_FISICA_DTA_NASCIMENTO f on p.SEQ_PESSOA = f.SEQ_PESSOA
# MAGIC LEFT JOIN PESSOA_JURIDICA_IDENTIFICACAO i on p.SEQ_PESSOA = i.SEQ_PESSOA
# MAGIC LEFT JOIN pessoa_regulatorio_global g on p.SEQ_PESSOA = g.SEQ_PESSOA
# MAGIC LEFT JOIN PESSOA_REGULATORIO_GLOBAL_TIPO_REGULATORIO r1 on r1.SEQ_PESSOA_REGULATORIO_GLOBAL = g.SEQ_PESSOA_REGULATORIO_GLOBAL and r1.SEQ_TIPO_REGULATORIO = 1
# MAGIC LEFT JOIN PESSOA_REGULATORIO_GLOBAL_TIPO_REGULATORIO r2 on r2.SEQ_PESSOA_REGULATORIO_GLOBAL = g.SEQ_PESSOA_REGULATORIO_GLOBAL and r2.SEQ_TIPO_REGULATORIO = 2
# MAGIC LEFT JOIN PESSOA_LINHA_NEGOCIO ng on p.SEQ_PESSOA = ng.SEQ_PESSOA
# MAGIC LEFT JOIN pessoa gco on ng.SEQ_PESSOA_RM = gco.SEQ_PESSOA
# MAGIC LEFT JOIN StatusName l on p.SEQ_PESSOA = l.SEQ_PESSOA
# MAGIC LEFT JOIN PESSOA_fisica_IDENTIFICACAO fisica on p.SEQ_PESSOA = fisica.SEQ_PESSOA
# MAGIC LEFT JOIN vwgic_rdl_pessoa_tipo_cadastro c on p.COD_INSTITUCIONAL = c.COD_INSTITUCIONAL
# MAGIC Left JOIN KN1Cases cs on cs.Gicid= p.COD_INSTITUCIONAL
# MAGIC )
# MAGIC select distinct
# MAGIC *
# MAGIC ,concat(COD_INSTITUCIONAL,'-',Party_Type) as GIC_Party
# MAGIC from GIC_Details

# COMMAND ----------

# DBTITLE 1,GCOB CaseClientDetails update to join on client type
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_CaseClientDetails As
# MAGIC select * 
# MAGIC ,case when ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,Case when ClientType = 'Legal Entity' then concat('GCOB_LEC_', GcobId)
# MAGIC       when ClientType in ('Natural Person', 'Natural Person acting in a Professional Capacity (NPPC)') then concat('GCOB_NP_NPPC_', GcobId)
# MAGIC End as LocalSystemIdentifier
# MAGIC from party_case_client_details
# MAGIC where CaseStatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,GCOB All Party Details update to join on client type
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllPartyDetails As
# MAGIC select distinct 
# MAGIC p.FullLegalName,p.DateOfBirth,p.FirstName,p.MiddleName,p.LastName,p.GlobalClientOwnerName,p.GlobalClientOwnerLocation,p.IsEligibleForFatcaAssessment,p.FatcaClassification,p.GIIN,p.EIN,p.FatcaDateOfIssue,p.FatcaComments,p.IsEligibleForCrsAssessment,p.CrsClassification,p.CrsFormSignedDate,p.CrsComments,p.LegalForm,p.CaseStatusName,p.IsLatestApprovedVersionOfClient,p.FullLegalNameInLocalLanguage,p.IsIncorporated,p.IncorporationNumber,p.IncorporationDate,p.BusinessLineName,p.HasSourceOfWealth,p.SanctionsOrExternalWatchlist,p.InternalWatchlist,p.AdverseInformationOrMedia,p.StatedFindings,p.PEPStatus,p.IsTrust,p.TypeOfTrust,p.IsClientRegulated,p.RegulatorName,p.RegulatorCountry,p.HasRecognisedRegulator,p.IsClientListed,p.ExchangeName,p.ExchangeCountry,p.HasRecognisedExchange,p.CountryOfTaxResidence,p.TinAvailable,p.TinOrEquivalent,p.TinUnavailabilityReason,p.ExplanationForTinBeingUnavailable,p.SourceOfIdentificationDocument,p.IdentificationDocumentNumber,p.SourceOfVerifiedDocument,p.VerifiedDocumentNumber,p.CddType,p.ClientType,p.gcobid,p.UniquePartyId,p.gcdsid,p.PartyId,p.caseid,c.FIHubIndicator
# MAGIC ,case when p.ClientType = 'LegalEntityClient' then 'Legal Entity' 
# MAGIC       when p.ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when p.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity' 
# MAGIC       when p.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,case when p.ClientLifeCycleStatus = 'Prospect' then 'Prospect'
# MAGIC       when p.ClientLifeCycleStatus = 'FormerClient' then 'Former Client'
# MAGIC       when p.ClientLifeCycleStatus = 'FormerProspect' then 'Former Prospect'
# MAGIC       when p.ClientLifeCycleStatus = 'Client' then 'Client'
# MAGIC       when p.ClientLifeCycleStatus = 'ExitClient' then 'Exit Client'
# MAGIC  else null
# MAGIC  end as ClientLifeCycleStatus
# MAGIC ,CONCAT('GCOB_', p.UniquePartyId) AS LocalSystemIdentifier,
# MAGIC -- Add two column from KYCMasterListRegistry
# MAGIC k.KYCGroup,
# MAGIC k.SectorTeam
# MAGIC from party_AllPartyDetails p
# MAGIC Inner join Gcob_CaseClientDetails c on p.UniquePartyId = c.UniqueGcobId
# MAGIC -- Adds a LEFT JOIN with KYCMasterListRegistry_SQL inside the Gcob_AllPartyDetails view
# MAGIC Left join KYCMasterListRegistry_SQL k ON p.UniquePartyId = k.KYCMasterUniqueGcobId 
# MAGIC where Status = 'Live'

# COMMAND ----------

# DBTITLE 1,GCOB for details not in GCDS
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllParty As
# MAGIC With details as 
# MAGIC (
# MAGIC select distinct
# MAGIC p.FullLegalName,p.DateOfBirth,p.FirstName,p.MiddleName,p.LastName,p.GlobalClientOwnerName,p.GlobalClientOwnerLocation,p.IsEligibleForFatcaAssessment,p.FatcaClassification,p.GIIN,p.EIN,p.FatcaDateOfIssue,p.FatcaComments,p.IsEligibleForCrsAssessment,p.CrsClassification,p.CrsFormSignedDate,p.CrsComments,p.LegalForm,p.IsLatestApprovedVersionOfClient,p.FullLegalNameInLocalLanguage,p.IsIncorporated,p.IncorporationNumber,p.IncorporationDate,p.BusinessLineName,p.HasSourceOfWealth,p.SanctionsOrExternalWatchlist,p.InternalWatchlist,p.AdverseInformationOrMedia,p.StatedFindings,p.PEPStatus,p.IsTrust,p.TypeOfTrust,p.IsClientRegulated,p.RegulatorName,p.RegulatorCountry,p.HasRecognisedRegulator,p.IsClientListed,p.ExchangeName,p.ExchangeCountry,p.HasRecognisedExchange,p.CountryOfTaxResidence,p.TinAvailable,p.TinOrEquivalent,p.TinUnavailabilityReason,p.ExplanationForTinBeingUnavailable,p.SourceOfIdentificationDocument,p.IdentificationDocumentNumber,p.SourceOfVerifiedDocument,p.VerifiedDocumentNumber,p.CddType,p.ClientType,p.gcobid,p.UniquePartyId,p.gcdsid,p.PartyId,p.caseid,c.FIHubIndicator
# MAGIC ,case when p.ClientType = 'LegalEntityClient' then 'Legal Entity' 
# MAGIC       when p.ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when p.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity' 
# MAGIC       when p.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,case when p.ClientLifeCycleStatus = 'Prospect' then 'Prospect'
# MAGIC       when p.ClientLifeCycleStatus = 'FormerClient' then 'Former Client'
# MAGIC       when p.ClientLifeCycleStatus = 'FormerProspect' then 'Former Prospect'
# MAGIC       when p.ClientLifeCycleStatus = 'Client' then 'Client'
# MAGIC       when p.ClientLifeCycleStatus = 'ExitClient' then 'Exit Client'
# MAGIC  else null
# MAGIC  end as ClientLifeCycleStatus
# MAGIC ,case when p.CaseStatusName is null then 'RelatedParty' else c.CaseStatusName end as CaseStatusName
# MAGIC ,c.SalesforceClientID_nCino
# MAGIC ,c.gcobid as ncino_gcobid
# MAGIC ,c.CaseCompletedDate
# MAGIC ,CONCAT('GCOB_', p.UniquePartyId) AS LocalSystemIdentifier,
# MAGIC -- Add two columns from KYCMasterListRegistry
# MAGIC k.KYCGroup,
# MAGIC k.SectorTeam
# MAGIC
# MAGIC from party_AllPartyDetails p
# MAGIC LEFT JOIN Gcob_CaseClientDetails c on p.UniquePartyId = c.UniqueGcobId
# MAGIC LEFT JOIN KYCMasterListRegistry_SQL k on p.UniquePartyId = k.KYCMasterUniqueGcobId
# MAGIC where Status = 'Live'
# MAGIC )
# MAGIC select distinct
# MAGIC *
# MAGIC ,concat(GCOBId,'-',Party_Type) as GCOB_Party
# MAGIC ,concat(SalesforceClientID_nCino,'-',Party_Type) as GCOB_NCINO_Party
# MAGIC from details
# MAGIC where CaseStatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,Legacy2 All Party Details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null and GcobId like 'RA:%';
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select * from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2 AS
# MAGIC select distinct 
# MAGIC clientId,SalesforceClientID_nCino
# MAGIC ,null as gcid
# MAGIC ,CASE
# MAGIC   WHEN IsClient = 'true' and  ClientTypeId = 1 THEN concat('LE_', GcobId)
# MAGIC   WHEN IsClient = 'true' and ClientTypeId in (2,3) THEN concat('NP_NPPC_', GcobId)
# MAGIC   WHEN IsClient = 'false' and ClientTypeId = 1 THEN concat('RLE_', GcobId)
# MAGIC   WHEN IsClient = 'false' and ClientTypeId in (2,3) THEN concat('RNP_', GcobId)
# MAGIC END as Legacy2_Identifier
# MAGIC ,FullLegalName
# MAGIC ,to_date(DateOfBirth, 'dd MMM yyyy') AS DateOfBirth -- PBI 14525246
# MAGIC ,case when ClientType not in ('Related Legal Entity','Legal Entity') then ContactFirstName else null end as FirstName
# MAGIC ,case when ClientType not in ('Related Legal Entity','Legal Entity') then ContactMiddleName else null end as MiddleName
# MAGIC ,case when ClientType not in ('Related Legal Entity','Legal Entity') then ContactLastName else null end as LastName
# MAGIC ,case when ClientType = 'Legal Entity' then 'Legal Entity' 
# MAGIC       when ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when ClientType = 'Related Legal Entity' then 'Related Legal Entity' 
# MAGIC       when ClientType = 'Related Natural Person' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,concat('LEGACY2_',Legacy2_Identifier) as LocalSystemIdentifier
# MAGIC ,GlobalClientOwner as GlobalClientOwnerName,GlobalClientOwnerLocation,IsEligibleForFatcaAssessment,FatcaClassification
# MAGIC ,GIIN,EIN,FatcaDateOfIssue,null as FatcaComments,IsEligibleForCrsAssessment,CrsClassification,null as CrsFormSignedDate
# MAGIC ,null as CrsComments,null as LegalForm,StatusTypeName as CaseStatusName
# MAGIC ,IsLatestApprovedVersionOfClient,FullLegalNameLocalLanguage as FullLegalNameInLocalLanguage,IsIncorporated,IncorporationNumber,null as IncorporationDate,BusinessLineName,null as HasSourceOfWealth,SanctionsOrExternalWatchlist
# MAGIC ,null as InternalWatchlist,null as AdverseInformationOrMedia,null as StatedFindings,null as PEPStatus,null as IsTrust
# MAGIC ,null as TypeOfTrust,null as IsClientRegulated,null as RegulatorName,null as RegulatorCountry,null as HasRecognisedRegulator,null as IsClientListed,null as ExchangeName,null as ExchangeCountry,null as HasRecognisedExchange
# MAGIC ,null as CountryOfTaxResidence,null as TinAvailable,null as TinOrEquivalent,null as TinUnavailabilityReason,null as ExplanationForTinBeingUnavailable,null as SourceOfIdentificationDocument,null as IdentificationDocumentNumber,null as SourceOfVerifiedDocument,null as VerifiedDocumentNumber,CddType,Null as FIHubIndicator
# MAGIC ,case when ClientLifeCycleName = 'Prospect' then 'Prospect'
# MAGIC       when ClientLifeCycleName = 'FormerClient' then 'Former Client'
# MAGIC       when ClientLifeCycleName = 'FormerProspect' then 'Former Prospect'
# MAGIC       when ClientLifeCycleName = 'Client' then 'Client'
# MAGIC       when ClientLifeCycleName = 'ExitClient' then 'Exit Client'
# MAGIC  else null
# MAGIC  end as ClientLifeCycleStatus
# MAGIC from Legacy2_client 
# MAGIC where ClientTypeId in (1,2,3)

# COMMAND ----------

# DBTITLE 1,RANZ MDM Party Details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW MDM_RANZ AS
# MAGIC Select distinct
# MAGIC   CONCAT('RANZ_',Ranz_Party.SRC_PARTY_ID) as LocalSystemIdentifier,
# MAGIC   Ranz_Party.SRC_PARTY_ID,
# MAGIC   Ranz_Party.PARTY_NAME,
# MAGIC   Case when Ranz_Party.PARTY_TYPE_CD='I' then 'Individual'
# MAGIC   when Ranz_Party.PARTY_TYPE_CD='O' then 'Organization' end as Party_Type,
# MAGIC   Case when RANZ_Cntry.CNTRY_CD is not null then RANZ_Cntry.CNTRY_CD
# MAGIC   else Ranz_Contract.RANZG_CNTRY_CD
# MAGIC   end as CountryOfTaxResidence,
# MAGIC   pep_eval.pep_eval_description as PEPStatus,
# MAGIC   Lifecycle_Status.CLIENT_LIFECYCLE_STATUS_DESCRIPTION as CustomerLifeCycleStatus,
# MAGIC   AM.AM_EVAL_DESCRIPTION AS AdverseInformationOrMedia,
# MAGIC   'True' as IsLatestApprovedVersionOfClient
# MAGIC from
# MAGIC   c_b_party_xref Ranz_Party
# MAGIC   left join c_b_party_dom_cntry_xref RANZ_Cntry on Ranz_Party.ROWID_XREF = RANZ_Cntry.FK_PARTY_ID AND RANZ_Cntry.DOM_TYPE = 'TAX_RESD'
# MAGIC   left join c_b_party_pep_am_xref RANZ_PEP on RANZ_PEP.FK_PARTY_ID=Ranz_Party.ROWID_XREF
# MAGIC   left join c_b_contr_rol_party_xref RANZ_rol on RANZ_rol.FK_PARTY_ID=Ranz_Party.ROWID_XREF
# MAGIC   left join c_b_contract_xref Ranz_Contract on Ranz_Contract.CONTR_ID=RANZ_rol.FK_CONTR_ID
# MAGIC   lEFT JOIN RANZ_Client_Lifecycle_Status_Mapping Lifecycle_Status ON Lifecycle_Status.LIFECYCLE_STATUS_CD=Ranz_Contract.LIFECYCLE_STATUS_CD
# MAGIC   left join Ranz_Adverse_Media_Mapping AM on AM.AM_EVAL_CD=RANZ_PEP.AM_EVAL_CD
# MAGIC   left join Ranz_Pep_Evaluation_Static_Mapping pep_eval on pep_eval.PEP_EVAL_CD=RANZ_PEP.PEP_EVAL_CD where Ranz_Party.PARTY_NAME is not null

# COMMAND ----------

# DBTITLE 1,Read GCDS data
# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_Clients As
# MAGIC select distinct
# MAGIC   k.KeyStore_value as identifier
# MAGIC , k.KeyStore_type
# MAGIC , k.status AS status
# MAGIC , c.*
# MAGIC , pr.Party_role
# MAGIC , case when pr.Life_cycle_status = 'Former Prospect' then 'Former Prospect'
# MAGIC        when pr.Life_cycle_status = 'Prospect' then 'Prospect'
# MAGIC        when pr.Life_cycle_status = 'Former Client' then 'Former Client'
# MAGIC        when pr.Life_cycle_status = 'Exit Client' then 'Exit Client'
# MAGIC        when pr.Life_cycle_status = 'Client' then 'Client'
# MAGIC        else null
# MAGIC   end as Life_cycle_status
# MAGIC , rel.`Relationship-Type` as RelationshipType
# MAGIC , rel.`Relationship-Value` as RelationshipValue_GCID
# MAGIC , case when c.Party_type <> 'Natural Person' then c.Full_legal_name else c.Person_Name end Full_Name
# MAGIC from client_KeyStoreKey k
# MAGIC inner join client_Client c on c.GCID = k.GCID
# MAGIC left join client_PartyRole pr on k.GCID = pr.GCID
# MAGIC left join client_PartytoPartyRelationship rel on k.GCID = rel.GCID

# COMMAND ----------

# DBTITLE 1,Onboarded Location Status by GCID (Retail/W&R)
# MAGIC %sql
# MAGIC create or replace temporary view OnboardedLocation as
# MAGIC Select T1.GCID
# MAGIC     , t1.NrActiveOnboardedLocationsPerGCID
# MAGIC     , t1.SET_BRANCH_CODES
# MAGIC     , t2.NrActiveNLM
# MAGIC     , CASE 
# MAGIC       WHEN t1.NrActiveOnboardedLocationsPerGCID = t2.NrActiveNLM THEN 'Retail'
# MAGIC       ELSE 'W&R'
# MAGIC       END AS `WholeSaleOrRetailParty`
# MAGIC     FROM
# MAGIC     (select GCID, count(*) NrActiveOnboardedLocationsPerGCID, collect_set(branch_code) AS SET_BRANCH_CODES
# MAGIC     from client_OnboardedLocations where Status = 'Active'
# MAGIC     GROUP BY GCID) AS T1
# MAGIC   LEFT JOIN 
# MAGIC     (select GCID AS GCID2, count(*) as NrActiveNLM
# MAGIC     from client_OnboardedLocations where branch_code = 'NLM' and Status = 'Active'
# MAGIC     GROUP BY GCID2) as t2 on t1.GCID = t2.GCID2

# COMMAND ----------

# DBTITLE 1,product Location Status by GCID (Retail/W&R)
# MAGIC %sql
# MAGIC create or replace temporary view ProductsLocation as
# MAGIC  Select distinct T1.GCID
# MAGIC     , t1.NrActiveProductsPerGCID
# MAGIC     , t1.SET_BUSINESS_UNITS
# MAGIC     , t2.NrActiveNLMProducts
# MAGIC     , CASE 
# MAGIC       WHEN t1.NrActiveProductsPerGCID = t2.NrActiveNLMProducts THEN 'Retail'
# MAGIC       ELSE 'W&R'
# MAGIC       END AS `WholeSaleOrRetailParty`
# MAGIC     FROM
# MAGIC     (select GCID, count(*) NrActiveProductsPerGCID, collect_set(BusinessUnit) AS SET_BUSINESS_UNITS
# MAGIC     from client_products where Status = 'Active'
# MAGIC     GROUP BY GCID) AS T1
# MAGIC   LEFT JOIN 
# MAGIC     (select GCID AS GCID2, count(*) as NrActiveNLMProducts
# MAGIC     from client_products where BusinessUnit = 'Memberbank NL' and Status = 'Active'
# MAGIC     GROUP BY GCID2) as t2 on t1.GCID = t2.GCID2

# COMMAND ----------

# DBTITLE 1,GCDS Client Classification Based on Onboarded and Product Locations (Retail vs W&R)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW wr_or_retail AS
# MAGIC SELECT DISTINCT 
# MAGIC     c.gcid, 
# MAGIC     CASE 
# MAGIC         WHEN o.WholeSaleOrRetailParty = 'W&R' OR p.WholeSaleOrRetailParty = 'W&R' THEN 'W&R'
# MAGIC         ELSE 'Retail' 
# MAGIC     END AS WRorRetail
# MAGIC FROM GCDS_Clients c
# MAGIC LEFT JOIN OnboardedLocation o ON c.gcid = o.gcid
# MAGIC LEFT JOIN ProductsLocation p ON c.gcid = p.gcid 

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - Other Sources Party Data

# COMMAND ----------

# DBTITLE 1,GCDS-Other Sources Clients
# MAGIC %sql
# MAGIC Create or replace temporary view Party As
# MAGIC select distinct
# MAGIC gcds.gcid 
# MAGIC ,COALESCE(gcds.Full_Name, gcob.FullLegalName, ncino.FullLegalName, gic.NOM_COMPLETO,l2.FullLegalName,MDM_RANZ.Party_Name) AS FullLegalName
# MAGIC ,COALESCE(gcds.DateOfBirth, gcob.DateOfBirth, ncino.DateOfBirth, gic.DTA_NASCIMENTO,l2.DateOfBirth) AS DateOfBirth
# MAGIC ,COALESCE(gcob.FirstName, ncino.FirstName,l2.FirstName) as FirstName
# MAGIC ,COALESCE(gcob.MiddleName, ncino.MiddleName,l2.MiddleName) as MiddleName
# MAGIC ,COALESCE(gcob.LastName, ncino.LastName, l2.LastName) as LastName
# MAGIC ,COALESCE(gcds.Party_type, gcob.Party_type, ncino.Party_type, gic.Party_type, l2.Party_type,MDM_RANZ.Party_Type) AS Party_type
# MAGIC ,COALESCE(gcob.GlobalClientOwnerName, ncino.GlobalClientOwnerName, gic.GlobalClientOwner, l2.GlobalClientOwnerName,gcds.`Global_CO-name`) AS GlobalClientOwnerName
# MAGIC ,COALESCE(gcob.GlobalClientOwnerLocation, ncino.GlobalClientOwnerLocation, gic.GlobalClientOwnerLocation ,l2.GlobalClientOwnerLocation, gcds.`Global_CO-location`) AS GlobalClientOwnerLocation
# MAGIC ,COALESCE(gcob.IsEligibleForFatcaAssessment, ncino.IsEligibleForFatcaAssessment,gic.FATCA_FLG_APLICABILIDADE, l2.IsEligibleForFatcaAssessment) as IsEligibleForFatcaAssessment
# MAGIC ,COALESCE(gcds.`FATCA-Classification`, gcob.FatcaClassification, ncino.FatcaClassification, gic.FATCA_DES_CLASSIFICACAO, l2.FatcaClassification) AS FatcaClassification
# MAGIC ,COALESCE(gcds.`FATCA-GIIN`, gcob.GIIN, ncino.GIIN,gic.GIIN, l2.GIIN) AS GIIN
# MAGIC ,COALESCE(gcds.`FATCA-EIN`, gcob.EIN, ncino.EIN, l2.EIN) AS EIN
# MAGIC ,COALESCE(gcob.FatcaDateOfIssue, ncino.FatcaDateOfIssue, l2.FatcaDateOfIssue) as FatcaDateOfIssue
# MAGIC ,COALESCE(gcob.FatcaComments, ncino.FatcaComments, gic.FatcaComments, l2.FatcaComments) as FatcaComments
# MAGIC ,COALESCE(gcob.IsEligibleForCrsAssessment, ncino.IsEligibleForCrsAssessment, gic.CRS_FLG_APLICABILIDADE, l2.IsEligibleForCrsAssessment) as IsEligibleForCrsAssessment
# MAGIC ,COALESCE(gcds.`CRS-Classification`, gcob.CrsClassification, ncino.CrsClassification, gic.CRS_DES_CLASSIFICACAO, l2.CrsClassification) AS CrsClassification
# MAGIC ,COALESCE(gcob.CrsFormSignedDate, ncino.CrsFormSignedDate, l2.CrsFormSignedDate) as CrsFormSignedDate
# MAGIC ,COALESCE(gcob.CrsComments, ncino.CrsComments, gic.CRS_DES_MOTIVO, l2.CrsComments) as CrsComments
# MAGIC ,COALESCE(gcds.`Legal_form`, gcob.LegalForm, ncino.LegalForm, gic.LegalForm, l2.LegalForm) AS LegalForm
# MAGIC ,COALESCE(gcds.Life_cycle_status, gcob.ClientLifeCycleStatus, ncino.ClientLifeCycleStatus, l2.ClientLifeCycleStatus, gic.ClientLifeCycleStatus,MDM_RANZ.CustomerLifeCycleStatus) AS LifeCycleStatus
# MAGIC ,COALESCE(gcob.CaseStatusName, ncino.CaseStatusName,gic.StatusName, l2.CaseStatusName,'Undefined') AS StatusName
# MAGIC ,COALESCE(gcob.IsLatestApprovedVersionOfClient, ncino.IsLatestApprovedVersionOfClient, l2.IsLatestApprovedVersionOfClient,gic.IsLatestApprovedVersionOfClient, MDM_RANZ.IsLatestApprovedVersionOfClient) as IsLatestApprovedVersionOfClient
# MAGIC ,COALESCE(gcds.Local_name,gcob.FullLegalNameInLocalLanguage,ncino.FullLegalNameInLocalLanguage,gic.NOM_COMPLETO,l2.FullLegalNameInLocalLanguage) AS FullLegalNameInLocalLanguage
# MAGIC ,COALESCE(gcob.IsIncorporated, ncino.IsIncorporated, l2.IsIncorporated) as IsIncorporated
# MAGIC ,COALESCE(gcob.IncorporationNumber, ncino.IncorporationNumber, l2.IncorporationNumber) as IncorporationNumber
# MAGIC ,COALESCE(gcds.Registration_Incor_Date,gcob.IncorporationDate,ncino.IncorporationDate, gic.IncorporationDate,l2.IncorporationDate) AS IncorporationDate
# MAGIC ,COALESCE(gcob.HasSourceOfWealth, ncino.HasSourceOfWealth, l2.HasSourceOfWealth) as HasSourceOfWealth
# MAGIC ,COALESCE(gcob.SanctionsOrExternalWatchlist, ncino.SanctionsOrExternalWatchlist, l2.SanctionsOrExternalWatchlist) as SanctionsOrExternalWatchlist
# MAGIC ,COALESCE(gcob.InternalWatchlist, ncino.InternalWatchlist, l2.InternalWatchlist) as InternalWatchlist
# MAGIC ,COALESCE(gcob.AdverseInformationOrMedia, ncino.AdverseInformationOrMedia, l2.AdverseInformationOrMedia,MDM_RANZ.AdverseInformationOrMedia) as AdverseInformationOrMedia
# MAGIC ,COALESCE(gcob.StatedFindings, ncino.StatedFindings, l2.StatedFindings) as StatedFindings
# MAGIC ,COALESCE(gcob.PEPStatus, ncino.PEPStatus, l2.PEPStatus,MDM_RANZ.PEPStatus) as PEPStatus
# MAGIC ,COALESCE(gcob.IsTrust, ncino.IsTrust, l2.IsTrust) as IsTrust
# MAGIC ,COALESCE(gcob.TypeOfTrust, ncino.TypeOfTrust, l2.TypeOfTrust) as TypeOfTrust
# MAGIC ,COALESCE(gcob.IsClientRegulated, ncino.IsClientRegulated, l2.IsClientRegulated) as IsClientRegulated
# MAGIC ,COALESCE(gcob.RegulatorName, ncino.RegulatorName, l2.RegulatorName) as RegulatorName
# MAGIC ,COALESCE(gcob.RegulatorCountry, ncino.RegulatorCountry, l2.RegulatorCountry) as RegulatorCountry
# MAGIC ,COALESCE(gcob.HasRecognisedRegulator, ncino.HasRecognisedRegulator, l2.HasRecognisedRegulator) as HasRecognisedRegulator
# MAGIC ,COALESCE(gcob.IsClientListed, ncino.IsClientListed, l2.IsClientListed) as IsClientListed
# MAGIC ,COALESCE(gcob.ExchangeName, ncino.ExchangeName, l2.ExchangeName) as ExchangeName
# MAGIC ,COALESCE(gcob.ExchangeCountry, ncino.ExchangeCountry, l2.ExchangeCountry) as ExchangeCountry
# MAGIC ,COALESCE(gcob.HasRecognisedExchange, ncino.HasRecognisedExchange, l2.HasRecognisedExchange) as HasRecognisedExchange
# MAGIC ,COALESCE(gcob.CountryOfTaxResidence, ncino.CountryOfTaxResidence,gic.CountryOfTaxResidence, l2.CountryOfTaxResidence,MDM_RANZ.CountryOfTaxResidence) as CountryOfTaxResidence
# MAGIC ,COALESCE(gcob.TinAvailable, ncino.TinAvailable,cast(gic.TinAvailable as STRING), l2.TinAvailable) as TinAvailable
# MAGIC ,COALESCE(gcob.TinOrEquivalent, ncino.TinOrEquivalent,gic.TinOrEquivalent, l2.TinOrEquivalent) as TinOrEquivalent
# MAGIC ,COALESCE(gcob.TinUnavailabilityReason, ncino.TinUnavailabilityReason, l2.TinUnavailabilityReason) as TinUnavailabilityReason
# MAGIC ,COALESCE(gcob.ExplanationForTinBeingUnavailable, ncino.ExplanationForTinBeingUnavailable, l2.ExplanationForTinBeingUnavailable) as ExplanationForTinBeingUnavailable
# MAGIC ,COALESCE(gcob.SourceOfIdentificationDocument, ncino.SourceOfIdentificationDocument, l2.SourceOfIdentificationDocument) as SourceOfIdentificationDocument
# MAGIC ,COALESCE(gcob.IdentificationDocumentNumber, ncino.IdentificationDocumentNumber,gic.IdentificationDocumentNumber, l2.IdentificationDocumentNumber) as IdentificationDocumentNumber
# MAGIC ,COALESCE(gcob.SourceOfVerifiedDocument, ncino.SourceOfVerifiedDocument, l2.SourceOfVerifiedDocument) as SourceOfVerifiedDocument
# MAGIC ,COALESCE(gcob.VerifiedDocumentNumber, ncino.VerifiedDocumentNumber, l2.VerifiedDocumentNumber) as VerifiedDocumentNumber
# MAGIC ,COALESCE(gcds.`CDD-entitytype`,gcob.CddType,ncino.CddType,l2.CddType) AS CddType
# MAGIC ,gcob.FIHubIndicator
# MAGIC ,wr.WRorRetail as `W&RORRetail`
# MAGIC ,CASE WHEN gcds.identifier = gcob.GcobId --AND gcds.GCID = gcob.GCDSID 
# MAGIC       AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' THEN gcob.LocalSystemIdentifier
# MAGIC      WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person' THEN ncino.LocalSystemIdentifier
# MAGIC      WHEN gcds.identifier = gic.COD_INSTITUCIONAL AND gcds.KeyStore_type = 'GIC' THEN gic.LocalSystemIdentifier
# MAGIC      WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC      THEN l2.LocalSystemIdentifier
# MAGIC      WHEN gcds.identifier = MDM_RANZ.SRC_PARTY_ID and gcds.KeyStore_type = 'CB RANZ' THEN MDM_RANZ.LocalSystemIdentifier
# MAGIC      else CONCAT('GCDS_',gcds.gcid)
# MAGIC END AS LocalSystemIdentifier
# MAGIC ,CASE WHEN gcds.identifier = gcob.GcobId --AND gcds.GCID = gcob.GCDSID 
# MAGIC       AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' THEN 'GCOB'
# MAGIC      WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person' THEN 'GCOB'
# MAGIC      WHEN gcds.identifier = gic.COD_INSTITUCIONAL AND gcds.KeyStore_type = 'GIC' THEN 'GIC'
# MAGIC      WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC      THEN 'Legacy2'
# MAGIC      WHEN gcds.identifier = MDM_RANZ.SRC_PARTY_ID and gcds.KeyStore_type = 'CB RANZ' THEN 'RANZ'
# MAGIC      else 'GCDS'
# MAGIC END AS Application
# MAGIC ,gcds.Rabobank_entity as IsRabobankEntity
# MAGIC ,gcds.`HO_reporting_party-FINREP_Code`
# MAGIC ,gcds.`HO_reporting_party-FINREP`
# MAGIC -- Add KYC attributes from KYCMasterListRegistry
# MAGIC ,gcob.KYCGroup
# MAGIC ,gcob.SectorTeam
# MAGIC -- end 
# MAGIC from GCDS_Clients gcds
# MAGIC LEFT JOIN Gcob_AllPartyDetails gcob on gcds.identifier = gcob.GcobId
# MAGIC         and gcob.Party_type = 'Legal Entity'
# MAGIC         and gcds.KeyStore_type = 'GCOBID'
# MAGIC         and gcds.Party_type <> 'Natural Person'
# MAGIC LEFT JOIN Gcob_CaseClientDetails c on c.SalesforceClientID_nCino = gcds.identifier 
# MAGIC         and gcds.KeyStore_type = 'NCINOID'
# MAGIC LEFT JOIN Gcob_AllPartyDetails ncino on c.ClientId = ncino.PartyId 
# MAGIC         and ncino.Party_type = 'Natural Person' 
# MAGIC left JOIN Gcob_AllPartyDetails np on gcds.identifier = np.GcobId
# MAGIC         and np.Party_type = 'Natural Person' 
# MAGIC         and gcds.KeyStore_type = 'GCOBID'
# MAGIC         and gcds.Party_type = 'Natural Person'
# MAGIC LEFT JOIN GIC_ClientDetails gic on gcds.identifier = gic.COD_INSTITUCIONAL
# MAGIC         and gcds.KeyStore_type = 'GIC'
# MAGIC         --and gcds.party_role = 'Customer'
# MAGIC LEFT JOIN Legacy2 l2 on l2.SalesforceClientID_nCino = gcds.identifier 
# MAGIC         and l2.Party_type = gcds.Party_type
# MAGIC         and gcds.KeyStore_type = 'NCINOID'
# MAGIC         --and gcds.party_role = 'Customer'    
# MAGIC LEFT JOIN MDM_RANZ  on gcds.identifier = MDM_RANZ.SRC_PARTY_ID 
# MAGIC          and gcds.KeyStore_type = 'CB RANZ'   
# MAGIC LEFT JOIN wr_or_retail wr ON gcds.GCID = wr.gcid

# COMMAND ----------

# DBTITLE 1,GCOB data Not in GCDS
# MAGIC %sql
# MAGIC Create or replace temporary view GCOB_NonGCDS As
# MAGIC with GCOB as 
# MAGIC (
# MAGIC select distinct
# MAGIC CONCAT('GCOB_',t1.UniquePartyId) as LocalSystemIdentifier
# MAGIC ,null as gcid
# MAGIC ,t1.FullLegalName
# MAGIC ,t1.DateOfBirth
# MAGIC ,t1.FirstName
# MAGIC ,t1.MiddleName
# MAGIC ,t1.LastName
# MAGIC ,t1.Party_type
# MAGIC ,t1.GlobalClientOwnerName
# MAGIC ,t1.GlobalClientOwnerLocation
# MAGIC ,t1.IsEligibleForFatcaAssessment
# MAGIC ,t1.FatcaClassification
# MAGIC ,t1.GIIN
# MAGIC ,t1.EIN
# MAGIC ,t1.FatcaDateOfIssue
# MAGIC ,t1.FatcaComments
# MAGIC ,t1.IsEligibleForCrsAssessment
# MAGIC ,t1.CrsClassification
# MAGIC ,t1.CrsFormSignedDate
# MAGIC ,t1.CrsComments
# MAGIC ,t1.LegalForm
# MAGIC ,t1.ClientLifeCycleStatus
# MAGIC ,t1.CaseStatusName
# MAGIC ,t1.IsLatestApprovedVersionOfClient
# MAGIC ,t1.FullLegalNameInLocalLanguage
# MAGIC ,t1.IsIncorporated
# MAGIC ,t1.IncorporationNumber
# MAGIC ,t1.IncorporationDate
# MAGIC ,t1.HasSourceOfWealth
# MAGIC ,t1.SanctionsOrExternalWatchlist
# MAGIC ,t1.InternalWatchlist
# MAGIC ,t1.AdverseInformationOrMedia
# MAGIC ,t1.StatedFindings
# MAGIC ,t1.PEPStatus
# MAGIC ,t1.IsTrust
# MAGIC ,t1.TypeOfTrust
# MAGIC ,t1.IsClientRegulated
# MAGIC ,t1.RegulatorName
# MAGIC ,t1.RegulatorCountry
# MAGIC ,t1.HasRecognisedRegulator
# MAGIC ,t1.IsClientListed
# MAGIC ,t1.ExchangeName
# MAGIC ,t1.ExchangeCountry
# MAGIC ,t1.HasRecognisedExchange
# MAGIC ,t1.CountryOfTaxResidence
# MAGIC ,t1.TinAvailable
# MAGIC ,t1.TinOrEquivalent
# MAGIC ,t1.TinUnavailabilityReason
# MAGIC ,t1.ExplanationForTinBeingUnavailable
# MAGIC ,t1.SourceOfIdentificationDocument
# MAGIC ,t1.IdentificationDocumentNumber
# MAGIC ,t1.SourceOfVerifiedDocument
# MAGIC ,t1.VerifiedDocumentNumber
# MAGIC ,t1.CddType
# MAGIC ,t1.caseid
# MAGIC ,t1.FIHubIndicator
# MAGIC -- Add KYC attributes from KYCMasterListRegistry
# MAGIC ,t1.KYCGroup
# MAGIC ,t1.SectorTeam
# MAGIC
# MAGIC from 
# MAGIC Gcob_AllParty t1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC CONCAT('GCOB_',t1.UniquePartyId) as LocalSystemIdentifier
# MAGIC ,null as gcid
# MAGIC ,t1.FullLegalName
# MAGIC ,t1.DateOfBirth
# MAGIC ,t1.FirstName
# MAGIC ,t1.MiddleName
# MAGIC ,t1.LastName
# MAGIC ,t1.Party_type
# MAGIC ,t1.GlobalClientOwnerName
# MAGIC ,t1.GlobalClientOwnerLocation
# MAGIC ,t1.IsEligibleForFatcaAssessment
# MAGIC ,t1.FatcaClassification
# MAGIC ,t1.GIIN
# MAGIC ,t1.EIN
# MAGIC ,t1.FatcaDateOfIssue
# MAGIC ,t1.FatcaComments
# MAGIC ,t1.IsEligibleForCrsAssessment
# MAGIC ,t1.CrsClassification
# MAGIC ,t1.CrsFormSignedDate
# MAGIC ,t1.CrsComments
# MAGIC ,t1.LegalForm
# MAGIC ,t1.ClientLifeCycleStatus
# MAGIC ,t1.CaseStatusName
# MAGIC ,t1.IsLatestApprovedVersionOfClient
# MAGIC ,t1.FullLegalNameInLocalLanguage
# MAGIC ,t1.IsIncorporated
# MAGIC ,t1.IncorporationNumber
# MAGIC ,t1.IncorporationDate
# MAGIC ,t1.HasSourceOfWealth
# MAGIC ,t1.SanctionsOrExternalWatchlist
# MAGIC ,t1.InternalWatchlist
# MAGIC ,t1.AdverseInformationOrMedia
# MAGIC ,t1.StatedFindings
# MAGIC ,t1.PEPStatus
# MAGIC ,t1.IsTrust
# MAGIC ,t1.TypeOfTrust
# MAGIC ,t1.IsClientRegulated
# MAGIC ,t1.RegulatorName
# MAGIC ,t1.RegulatorCountry
# MAGIC ,t1.HasRecognisedRegulator
# MAGIC ,t1.IsClientListed
# MAGIC ,t1.ExchangeName
# MAGIC ,t1.ExchangeCountry
# MAGIC ,t1.HasRecognisedExchange
# MAGIC ,t1.CountryOfTaxResidence
# MAGIC ,t1.TinAvailable
# MAGIC ,t1.TinOrEquivalent
# MAGIC ,t1.TinUnavailabilityReason
# MAGIC ,t1.ExplanationForTinBeingUnavailable
# MAGIC ,t1.SourceOfIdentificationDocument
# MAGIC ,t1.IdentificationDocumentNumber
# MAGIC ,t1.SourceOfVerifiedDocument
# MAGIC ,t1.VerifiedDocumentNumber
# MAGIC ,t1.CddType
# MAGIC ,t1.caseid
# MAGIC ,t1.FIHubIndicator
# MAGIC -- Add KYC attributes from KYCMasterListRegistry
# MAGIC ,t1.KYCGroup
# MAGIC ,t1.SectorTeam
# MAGIC from 
# MAGIC Gcob_AllParty t1
# MAGIC )
# MAGIC select distinct
# MAGIC gcid
# MAGIC ,FullLegalName
# MAGIC ,DateOfBirth
# MAGIC ,FirstName
# MAGIC ,MiddleName
# MAGIC ,LastName
# MAGIC ,Party_type
# MAGIC ,GlobalClientOwnerName
# MAGIC ,GlobalClientOwnerLocation
# MAGIC ,IsEligibleForFatcaAssessment
# MAGIC ,FatcaClassification
# MAGIC ,GIIN
# MAGIC ,EIN
# MAGIC ,FatcaDateOfIssue
# MAGIC ,FatcaComments
# MAGIC ,IsEligibleForCrsAssessment
# MAGIC ,CrsClassification
# MAGIC ,CrsFormSignedDate
# MAGIC ,CrsComments
# MAGIC ,LegalForm
# MAGIC ,ClientLifeCycleStatus
# MAGIC ,CaseStatusName
# MAGIC ,IsLatestApprovedVersionOfClient
# MAGIC ,FullLegalNameInLocalLanguage
# MAGIC ,IsIncorporated
# MAGIC ,IncorporationNumber
# MAGIC ,IncorporationDate
# MAGIC ,HasSourceOfWealth
# MAGIC ,SanctionsOrExternalWatchlist
# MAGIC ,InternalWatchlist
# MAGIC ,AdverseInformationOrMedia
# MAGIC ,StatedFindings
# MAGIC ,PEPStatus
# MAGIC ,IsTrust
# MAGIC ,TypeOfTrust
# MAGIC ,IsClientRegulated
# MAGIC ,RegulatorName
# MAGIC ,RegulatorCountry
# MAGIC ,HasRecognisedRegulator
# MAGIC ,IsClientListed
# MAGIC ,ExchangeName
# MAGIC ,ExchangeCountry
# MAGIC ,HasRecognisedExchange
# MAGIC ,CountryOfTaxResidence
# MAGIC ,TinAvailable
# MAGIC ,TinOrEquivalent
# MAGIC ,TinUnavailabilityReason
# MAGIC ,ExplanationForTinBeingUnavailable
# MAGIC ,SourceOfIdentificationDocument
# MAGIC ,IdentificationDocumentNumber
# MAGIC ,SourceOfVerifiedDocument
# MAGIC ,VerifiedDocumentNumber
# MAGIC ,CddType 
# MAGIC ,FIHubIndicator
# MAGIC ,null as `W&RORRetail`
# MAGIC ,LocalSystemIdentifier
# MAGIC ,'GCOB' as Application
# MAGIC -- Add KYC attributes from KYCMasterListRegistry
# MAGIC ,KYCGroup
# MAGIC ,SectorTeam
# MAGIC from GCOB 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Sources data Not in GCDS
# MAGIC %sql
# MAGIC Create or replace temporary view Party_NonGCDS As
# MAGIC with NonGCDS as 
# MAGIC (
# MAGIC select * from GCOB_NonGCDS
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC null as gcid
# MAGIC ,t1.NOM_COMPLETO as FullLegalName
# MAGIC ,t1.DTA_NASCIMENTO as DateOfBirth
# MAGIC ,null as FirstName
# MAGIC ,null as MiddleName
# MAGIC ,null as LastName
# MAGIC ,t1.Party_type
# MAGIC ,t1.GlobalClientOwner as GlobalClientOwnerName
# MAGIC ,t1.GlobalClientOwnerLocation as GlobalClientOwnerLocation
# MAGIC ,t1.FATCA_FLG_APLICABILIDADE as IsEligibleForFatcaAssessment
# MAGIC ,t1.FATCA_DES_CLASSIFICACAO as FatcaClassification
# MAGIC ,t1.GIIN
# MAGIC ,null as EIN
# MAGIC ,null as FatcaDateOfIssue
# MAGIC ,t1.FatcaComments as FatcaComments
# MAGIC ,t1.CRS_FLG_APLICABILIDADE as IsEligibleForCrsAssessment
# MAGIC ,t1.CRS_DES_CLASSIFICACAO as CrsClassification
# MAGIC ,null as CrsFormSignedDate
# MAGIC ,t1.CRS_DES_MOTIVO as CrsComments
# MAGIC ,t1.LegalForm
# MAGIC ,t1.ClientLifeCycleStatus as ClientLifeCycleStatus
# MAGIC ,t1.StatusName as CaseStatusName
# MAGIC ,t1.IsLatestApprovedVersionOfClient
# MAGIC ,t1.NOM_COMPLETO as FullLegalNameInLocalLanguage
# MAGIC ,null as IsIncorporated
# MAGIC ,null as IncorporationNumber
# MAGIC ,t1.IncorporationDate
# MAGIC ,null as HasSourceOfWealth
# MAGIC ,null as SanctionsOrExternalWatchlist
# MAGIC ,null as InternalWatchlist
# MAGIC ,null as AdverseInformationOrMedia
# MAGIC ,null as StatedFindings
# MAGIC ,null as PEPStatus
# MAGIC ,null as IsTrust
# MAGIC ,null as TypeOfTrust
# MAGIC ,null as IsClientRegulated
# MAGIC ,null as RegulatorName
# MAGIC ,null as RegulatorCountry
# MAGIC ,null as HasRecognisedRegulator
# MAGIC ,null as IsClientListed
# MAGIC ,null as ExchangeName
# MAGIC ,null as ExchangeCountry
# MAGIC ,null as HasRecognisedExchange
# MAGIC ,t1.CountryOfTaxResidence
# MAGIC ,cast(t1.TinAvailable as STRING)as TinAvailable
# MAGIC ,t1.TinOrEquivalent as TinOrEquivalent
# MAGIC ,null as TinUnavailabilityReason
# MAGIC ,null as ExplanationForTinBeingUnavailable
# MAGIC ,null as SourceOfIdentificationDocument
# MAGIC ,t1.IdentificationDocumentNumber
# MAGIC ,null as SourceOfVerifiedDocument
# MAGIC ,null as VerifiedDocumentNumber
# MAGIC ,null as CddType
# MAGIC ,null as FIHubIndicator
# MAGIC ,null as `W&RORRetail`
# MAGIC ,CONCAT('GIC_',t1.COD_INSTITUCIONAL) as LocalSystemIdentifier
# MAGIC ,'GIC' as Application
# MAGIC -- Add KYC columns as NULL
# MAGIC ,null as KYCGroup
# MAGIC ,null as SectorTeam
# MAGIC
# MAGIC From GIC_ClientDetails t1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC t1.gcid
# MAGIC ,t1.FullLegalName
# MAGIC ,t1.DateOfBirth
# MAGIC ,t1.FirstName
# MAGIC ,t1.MiddleName
# MAGIC ,t1.LastName
# MAGIC ,t1.Party_type
# MAGIC ,t1.GlobalClientOwnerName
# MAGIC ,t1.GlobalClientOwnerLocation
# MAGIC ,t1.IsEligibleForFatcaAssessment
# MAGIC ,t1.FatcaClassification
# MAGIC ,t1.GIIN
# MAGIC ,t1.EIN
# MAGIC ,t1.FatcaDateOfIssue
# MAGIC ,t1.FatcaComments
# MAGIC ,t1.IsEligibleForCrsAssessment
# MAGIC ,t1.CrsClassification
# MAGIC ,t1.CrsFormSignedDate
# MAGIC ,t1.CrsComments
# MAGIC ,t1.LegalForm
# MAGIC ,t1.ClientLifeCycleStatus
# MAGIC ,t1.CaseStatusName
# MAGIC ,t1.IsLatestApprovedVersionOfClient
# MAGIC ,t1.FullLegalNameInLocalLanguage
# MAGIC ,t1.IsIncorporated
# MAGIC ,t1.IncorporationNumber
# MAGIC ,t1.IncorporationDate
# MAGIC ,t1.HasSourceOfWealth
# MAGIC ,t1.SanctionsOrExternalWatchlist
# MAGIC ,t1.InternalWatchlist
# MAGIC ,t1.AdverseInformationOrMedia
# MAGIC ,t1.StatedFindings
# MAGIC ,t1.PEPStatus
# MAGIC ,t1.IsTrust
# MAGIC ,t1.TypeOfTrust
# MAGIC ,t1.IsClientRegulated
# MAGIC ,t1.RegulatorName
# MAGIC ,t1.RegulatorCountry
# MAGIC ,t1.HasRecognisedRegulator
# MAGIC ,t1.IsClientListed
# MAGIC ,t1.ExchangeName
# MAGIC ,t1.ExchangeCountry
# MAGIC ,t1.HasRecognisedExchange
# MAGIC ,t1.CountryOfTaxResidence
# MAGIC ,t1.TinAvailable
# MAGIC ,t1.TinOrEquivalent
# MAGIC ,t1.TinUnavailabilityReason
# MAGIC ,t1.ExplanationForTinBeingUnavailable
# MAGIC ,t1.SourceOfIdentificationDocument
# MAGIC ,t1.IdentificationDocumentNumber
# MAGIC ,t1.SourceOfVerifiedDocument
# MAGIC ,t1.VerifiedDocumentNumber
# MAGIC ,t1.CddType
# MAGIC ,t1.FIHubIndicator
# MAGIC ,null as `W&RORRetail`
# MAGIC ,CONCAT('LEGACY2_',t1.Legacy2_Identifier) as LocalSystemIdentifier
# MAGIC ,'Legacy2' as Application
# MAGIC -- Add KYC columns as NULL
# MAGIC ,null as KYCGroup
# MAGIC ,null as SectorTeam
# MAGIC from Legacy2 as t1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC null as gcid
# MAGIC ,t1.PARTY_NAME as FullLegalName
# MAGIC ,null as DateOfBirth
# MAGIC ,null as FirstName
# MAGIC ,null as MiddleName
# MAGIC ,null as LastName
# MAGIC ,t1.Party_Type
# MAGIC ,null as GlobalClientOwnerName
# MAGIC ,null as GlobalClientOwnerLocation
# MAGIC ,null as IsEligibleForFatcaAssessment
# MAGIC ,null as FatcaClassification
# MAGIC ,null as GIIN
# MAGIC ,null as EIN
# MAGIC ,null as FatcaDateOfIssue
# MAGIC ,null as FatcaComments
# MAGIC ,null as IsEligibleForCrsAssessment
# MAGIC ,null as CrsClassification
# MAGIC ,null as CrsFormSignedDate
# MAGIC ,null as CrsComments
# MAGIC ,null as LegalForm
# MAGIC ,t1.CustomerLifeCycleStatus as ClientLifeCycleStatus
# MAGIC ,null as CaseStatusName
# MAGIC ,t1.IsLatestApprovedVersionOfClient
# MAGIC ,null as FullLegalNameInLocalLanguage
# MAGIC ,null as IsIncorporated
# MAGIC ,null as IncorporationNumber
# MAGIC ,null as IncorporationDate
# MAGIC ,null as HasSourceOfWealth
# MAGIC ,null as SanctionsOrExternalWatchlist
# MAGIC ,null as InternalWatchlist
# MAGIC ,t1.AdverseInformationOrMedia as AdverseInformationOrMedia
# MAGIC ,null as StatedFindings
# MAGIC ,t1.PEPStatus
# MAGIC ,null as IsTrust
# MAGIC ,null as TypeOfTrust
# MAGIC ,null as IsClientRegulated
# MAGIC ,null as RegulatorName
# MAGIC ,null as RegulatorCountry
# MAGIC ,null as HasRecognisedRegulator
# MAGIC ,null as IsClientListed
# MAGIC ,null as ExchangeName
# MAGIC ,null as ExchangeCountry
# MAGIC ,null as HasRecognisedExchange
# MAGIC ,t1.CountryOfTaxResidence
# MAGIC ,null as TinAvailable
# MAGIC ,null as TinOrEquivalent
# MAGIC ,null as TinUnavailabilityReason
# MAGIC ,null as ExplanationForTinBeingUnavailable
# MAGIC ,null as SourceOfIdentificationDocument
# MAGIC ,null as IdentificationDocumentNumber
# MAGIC ,null as SourceOfVerifiedDocument
# MAGIC ,null as VerifiedDocumentNumber
# MAGIC ,NULL AS CddType
# MAGIC ,null as FIHubIndicator
# MAGIC ,null as `W&RORRetail`
# MAGIC ,CONCAT('RANZ_',t1.SRC_PARTY_ID) as LocalSystemIdentifier
# MAGIC ,'RANZ' as Application
# MAGIC -- Add KYC columns as NULL
# MAGIC ,null as KYCGroup
# MAGIC ,null as SectorTeam
# MAGIC From MDM_RANZ t1
# MAGIC )
# MAGIC select  distinct
# MAGIC t1.gcid
# MAGIC ,t1.FullLegalName
# MAGIC ,t1.DateOfBirth
# MAGIC ,t1.FirstName
# MAGIC ,t1.MiddleName
# MAGIC ,t1.LastName
# MAGIC ,t1.Party_type
# MAGIC ,t1.GlobalClientOwnerName
# MAGIC ,t1.GlobalClientOwnerLocation
# MAGIC ,t1.IsEligibleForFatcaAssessment
# MAGIC ,t1.FatcaClassification
# MAGIC ,t1.GIIN
# MAGIC ,t1.EIN
# MAGIC ,t1.FatcaDateOfIssue
# MAGIC ,t1.FatcaComments
# MAGIC ,t1.IsEligibleForCrsAssessment
# MAGIC ,t1.CrsClassification
# MAGIC ,t1.CrsFormSignedDate
# MAGIC ,t1.CrsComments
# MAGIC ,t1.LegalForm
# MAGIC ,t1.ClientLifeCycleStatus
# MAGIC ,t1.CaseStatusName
# MAGIC ,t1.IsLatestApprovedVersionOfClient
# MAGIC ,t1.FullLegalNameInLocalLanguage
# MAGIC ,t1.IsIncorporated
# MAGIC ,t1.IncorporationNumber
# MAGIC ,t1.IncorporationDate
# MAGIC ,t1.HasSourceOfWealth
# MAGIC ,t1.SanctionsOrExternalWatchlist
# MAGIC ,t1.InternalWatchlist
# MAGIC ,t1.AdverseInformationOrMedia
# MAGIC ,t1.StatedFindings
# MAGIC ,t1.PEPStatus
# MAGIC ,t1.IsTrust
# MAGIC ,t1.TypeOfTrust
# MAGIC ,t1.IsClientRegulated
# MAGIC ,t1.RegulatorName
# MAGIC ,t1.RegulatorCountry
# MAGIC ,t1.HasRecognisedRegulator
# MAGIC ,t1.IsClientListed
# MAGIC ,t1.ExchangeName
# MAGIC ,t1.ExchangeCountry
# MAGIC ,t1.HasRecognisedExchange
# MAGIC ,t1.CountryOfTaxResidence
# MAGIC ,t1.TinAvailable
# MAGIC ,t1.TinOrEquivalent
# MAGIC ,t1.TinUnavailabilityReason
# MAGIC ,t1.ExplanationForTinBeingUnavailable
# MAGIC ,t1.SourceOfIdentificationDocument
# MAGIC ,t1.IdentificationDocumentNumber
# MAGIC ,t1.SourceOfVerifiedDocument
# MAGIC ,t1.VerifiedDocumentNumber
# MAGIC ,t1.CddType
# MAGIC ,Null as FIHubIndicator
# MAGIC ,t1.`W&RORRetail`
# MAGIC ,t1.LocalSystemIdentifier
# MAGIC ,t1.Application
# MAGIC ,null as IsRabobankEntity
# MAGIC ,null as `HO_reporting_party-FINREP_Code`
# MAGIC ,null as `HO_reporting_party-FINREP`
# MAGIC -- Add KYC columns as NULL
# MAGIC ,null as KYCGroup
# MAGIC ,null as SectorTeam
# MAGIC from NonGCDS as t1

# COMMAND ----------

# DBTITLE 1,Unique Local System Identifier
# MAGIC %sql
# MAGIC Create or replace temporary view NonGCDS_UniqueParty As
# MAGIC select * 
# MAGIC from Party_NonGCDS a
# MAGIC left anti join Party b
# MAGIC on a.LocalSystemIdentifier = b.LocalSystemIdentifier
# MAGIC

# COMMAND ----------

# DBTITLE 1,Union GCDS and NonGCDS
# MAGIC %sql
# MAGIC Create or replace temporary view All_Party As
# MAGIC select p.*, s.GlobalClientRegion as GlobalClientOwnerRegion
# MAGIC from Party p
# MAGIC left join static_ClientOwnerRegion s on p.GlobalClientOwnerLocation= s.ClientOwnerLocation
# MAGIC union
# MAGIC select pn.*, s.GlobalClientRegion as GlobalClientOwnerRegion
# MAGIC from NonGCDS_UniqueParty pn
# MAGIC left join static_ClientOwnerRegion s on pn.GlobalClientOwnerLocation= s.ClientOwnerLocation

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party=spark.table('All_Party')

# COMMAND ----------

# DBTITLE 1,Dataframe to create temp view
df_party = add_party_identifier(df_party, df_Party_SystemIdentifier)


# COMMAND ----------

# DBTITLE 1,Create Temp View
df_party.createOrReplaceTempView('Derived_Party_Identifier')

# COMMAND ----------

# DBTITLE 1,Attributes values based on Application
# MAGIC %sql
# MAGIC Create or replace temporary view SystemPrefAttribute As
# MAGIC WITH ranked_data AS (
# MAGIC SELECT distinct
# MAGIC PartyIdentifier,
# MAGIC GlobalClientOwnerName,
# MAGIC ROW_NUMBER() OVER (PARTITION BY PartyIdentifier ORDER BY
# MAGIC   CASE
# MAGIC   WHEN GlobalClientOwnerName IS NOT NULL AND Application = 'GCOB' THEN 1
# MAGIC   WHEN GlobalClientOwnerName IS NOT NULL AND Application = 'Legacy2' THEN 2
# MAGIC   WHEN GlobalClientOwnerName IS NOT NULL AND Application = 'GIC' THEN 3
# MAGIC   WHEN GlobalClientOwnerName IS NOT NULL AND Application = 'GCDS' THEN 4
# MAGIC   ELSE 10 END 
# MAGIC ) AS rn_GlobalClientOwnerName,
# MAGIC LifeCycleStatus,
# MAGIC ROW_NUMBER() OVER (PARTITION BY PartyIdentifier ORDER BY
# MAGIC   CASE
# MAGIC   WHEN LifeCycleStatus IS NOT NULL AND LifeCycleStatus= 'Client' AND Application = 'GCDS' THEN 1
# MAGIC   WHEN LifeCycleStatus IS NOT NULL AND Application = 'GCOB' THEN 2
# MAGIC   WHEN LifeCycleStatus IS NOT NULL AND Application = 'Legacy2' THEN 3
# MAGIC   WHEN LifeCycleStatus IS NOT NULL AND Application = 'GIC' THEN 4
# MAGIC   ELSE 10 END
# MAGIC ) AS rn_LifeCycleStatus,
# MAGIC Application
# MAGIC FROM Derived_Party_Identifier
# MAGIC )
# MAGIC select * from ranked_data

# COMMAND ----------

# DBTITLE 1,Unique Party Per PartyIdentifier
# MAGIC %sql
# MAGIC Create or replace temporary view Party_Final As
# MAGIC select distinct
# MAGIC p.PartyIdentifier
# MAGIC ,max(gcid) as gcid 
# MAGIC ,MAX(CASE WHEN FullLegalName IS NOT NULL THEN FullLegalName END) AS FullLegalName
# MAGIC ,MAX(CASE WHEN DateOfBirth IS NOT NULL THEN DateOfBirth END) AS DateOfBirth
# MAGIC ,MAX(CASE WHEN FirstName IS NOT NULL THEN FirstName END) AS FirstName
# MAGIC ,MAX(CASE WHEN MiddleName IS NOT NULL THEN MiddleName END) AS MiddleName
# MAGIC ,MAX(CASE WHEN LastName IS NOT NULL THEN LastName END) AS LastName
# MAGIC ,MAX(CASE WHEN Party_type IS NOT NULL THEN Party_type END) AS Party_type
# MAGIC ,MAX(CASE WHEN a.rn_GlobalClientOwnerName = 1 THEN a.GlobalClientOwnerName END) AS GlobalClientOwnerName
# MAGIC ,MAX(CASE WHEN IsEligibleForFatcaAssessment IS NOT NULL THEN IsEligibleForFatcaAssessment END) AS IsEligibleForFatcaAssessment
# MAGIC ,MAX(CASE WHEN FatcaClassification IS NOT NULL THEN FatcaClassification END) AS FatcaClassification
# MAGIC ,MAX(CASE WHEN GIIN IS NOT NULL THEN GIIN END) AS GIIN
# MAGIC ,MAX(CASE WHEN EIN IS NOT NULL THEN EIN END) AS EIN
# MAGIC ,MAX(CASE WHEN FatcaDateOfIssue IS NOT NULL THEN FatcaDateOfIssue END) AS FatcaDateOfIssue
# MAGIC ,MAX(CASE WHEN FatcaComments IS NOT NULL THEN FatcaComments END) AS FatcaComments
# MAGIC ,MAX(CASE WHEN IsEligibleForCrsAssessment IS NOT NULL THEN IsEligibleForCrsAssessment END) AS IsEligibleForCrsAssessment
# MAGIC ,MAX(CASE WHEN CrsClassification IS NOT NULL THEN CrsClassification END) AS CrsClassification
# MAGIC ,MAX(CASE WHEN CrsFormSignedDate IS NOT NULL THEN CrsFormSignedDate END) AS CrsFormSignedDate
# MAGIC ,MAX(CASE WHEN CrsComments IS NOT NULL THEN CrsComments END) AS CrsComments
# MAGIC ,MAX(CASE WHEN LegalForm IS NOT NULL THEN LegalForm END) AS LegalForm
# MAGIC ,MAX(CASE WHEN a.rn_LifeCycleStatus =1 THEN a.LifeCycleStatus END) AS LifeCycleStatus
# MAGIC ,MAX(CASE WHEN IsLatestApprovedVersionOfClient IS NOT NULL THEN IsLatestApprovedVersionOfClient END) AS IsLatestApprovedVersionOfClient
# MAGIC ,MAX(CASE WHEN FullLegalNameInLocalLanguage IS NOT NULL THEN FullLegalNameInLocalLanguage END) AS FullLegalNameInLocalLanguage
# MAGIC ,MAX(CASE WHEN IsIncorporated IS NOT NULL THEN IsIncorporated END) AS IsIncorporated
# MAGIC ,MAX(CASE WHEN IncorporationNumber IS NOT NULL THEN IncorporationNumber END) AS IncorporationNumber
# MAGIC ,MAX(CASE WHEN IncorporationDate IS NOT NULL THEN IncorporationDate END) AS IncorporationDate
# MAGIC ,MAX(CASE WHEN HasSourceOfWealth IS NOT NULL THEN HasSourceOfWealth END) AS HasSourceOfWealth
# MAGIC ,MAX(CASE WHEN SanctionsOrExternalWatchlist IS NOT NULL THEN SanctionsOrExternalWatchlist END) AS SanctionsOrExternalWatchlist
# MAGIC ,MAX(CASE WHEN InternalWatchlist IS NOT NULL THEN InternalWatchlist END) AS InternalWatchlist
# MAGIC ,MAX(CASE WHEN AdverseInformationOrMedia IS NOT NULL THEN AdverseInformationOrMedia END) AS AdverseInformationOrMedia
# MAGIC ,MAX(CASE WHEN StatedFindings IS NOT NULL THEN StatedFindings END) AS StatedFindings
# MAGIC ,MAX(CASE WHEN PEPStatus IS NOT NULL THEN PEPStatus END) AS PEPStatus
# MAGIC ,MAX(CASE WHEN IsTrust IS NOT NULL THEN IsTrust END) AS IsTrust
# MAGIC ,MAX(CASE WHEN TypeOfTrust IS NOT NULL THEN TypeOfTrust END) AS TypeOfTrust
# MAGIC ,MAX(CASE WHEN IsClientRegulated IS NOT NULL THEN IsClientRegulated END) AS IsClientRegulated
# MAGIC ,MAX(CASE WHEN RegulatorName IS NOT NULL THEN RegulatorName END) AS RegulatorName
# MAGIC ,MAX(CASE WHEN RegulatorCountry IS NOT NULL THEN RegulatorCountry END) AS RegulatorCountry
# MAGIC ,MAX(CASE WHEN HasRecognisedRegulator IS NOT NULL THEN HasRecognisedRegulator END) AS HasRecognisedRegulator
# MAGIC ,MAX(CASE WHEN IsClientListed IS NOT NULL THEN IsClientListed END) AS IsClientListed
# MAGIC ,MAX(CASE WHEN ExchangeName IS NOT NULL THEN ExchangeName END) AS ExchangeName
# MAGIC ,MAX(CASE WHEN ExchangeCountry IS NOT NULL THEN ExchangeCountry END) AS ExchangeCountry
# MAGIC ,MAX(CASE WHEN HasRecognisedExchange IS NOT NULL THEN HasRecognisedExchange END) AS HasRecognisedExchange
# MAGIC ,MAX(CASE WHEN CountryOfTaxResidence IS NOT NULL THEN CountryOfTaxResidence END) AS CountryOfTaxResidence
# MAGIC ,MAX(CASE WHEN TinAvailable IS NOT NULL THEN TinAvailable END) AS TinAvailable
# MAGIC ,MAX(CASE WHEN TinOrEquivalent IS NOT NULL THEN TinOrEquivalent END) AS TinOrEquivalent
# MAGIC ,MAX(CASE WHEN TinUnavailabilityReason IS NOT NULL THEN TinUnavailabilityReason END) AS TinUnavailabilityReason
# MAGIC ,MAX(CASE WHEN ExplanationForTinBeingUnavailable IS NOT NULL THEN ExplanationForTinBeingUnavailable END) AS ExplanationForTinBeingUnavailable
# MAGIC ,MAX(CASE WHEN SourceOfIdentificationDocument IS NOT NULL THEN SourceOfIdentificationDocument END) AS SourceOfIdentificationDocument
# MAGIC ,MAX(CASE WHEN IdentificationDocumentNumber IS NOT NULL THEN IdentificationDocumentNumber END) AS IdentificationDocumentNumber
# MAGIC ,MAX(CASE WHEN SourceOfVerifiedDocument IS NOT NULL THEN SourceOfVerifiedDocument END) AS SourceOfVerifiedDocument
# MAGIC ,MAX(CASE WHEN VerifiedDocumentNumber IS NOT NULL THEN VerifiedDocumentNumber END) AS VerifiedDocumentNumber
# MAGIC ,MAX(CASE WHEN CddType IS NOT NULL THEN CddType END) AS CddType
# MAGIC ,MAX(CASE WHEN FIHubIndicator IS NOT NULL THEN FIHubIndicator END) AS FIHubIndicator
# MAGIC ,MAX(CASE WHEN `W&RORRetail` IS NOT NULL THEN `W&RORRetail` END) AS `W&RORRetail`
# MAGIC ,MAX(CASE WHEN IsRabobankEntity IS NOT NULL THEN IsRabobankEntity END) AS IsRabobankEntity
# MAGIC ,MAX(CASE WHEN `HO_reporting_party-FINREP_Code` IS NOT NULL THEN `HO_reporting_party-FINREP_Code` END) AS `HO_reporting_party-FINREP_Code`
# MAGIC ,MAX(CASE WHEN `HO_reporting_party-FINREP` IS NOT NULL THEN `HO_reporting_party-FINREP` END) AS `HO_reporting_party-FINREP`
# MAGIC -- Add KYCGroup and SectorTeam are present in the Derived_Party_Identifier view
# MAGIC ,MAX(CASE WHEN KYCGroup IS NOT NULL THEN KYCGroup END) AS KYCGroup
# MAGIC ,MAX(CASE WHEN SectorTeam IS NOT NULL THEN SectorTeam END) AS SectorTeam
# MAGIC
# MAGIC from Derived_Party_Identifier p
# MAGIC left join SystemPrefAttribute a on p.PartyIdentifier = a.PartyIdentifier
# MAGIC where p.PartyIdentifier is not null
# MAGIC group by p.PartyIdentifier

# COMMAND ----------

# DBTITLE 1,Logic for Leading Identifier
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCDS_LeadGCID2 AS
# MAGIC SELECT distinct
# MAGIC       cast(
# MAGIC         CASE
# MAGIC           WHEN RC3.`Relationship-Value` IS NOT NULL AND C3.Party_type <> 'Legal Entity' THEN RC3.`Relationship-Value`
# MAGIC           WHEN C4.GCID IS NOT NULL THEN C4.GCID
# MAGIC           WHEN RC2.`Relationship-Value` IS NOT NULL AND C2.Party_type <> 'Legal Entity' THEN RC2.`Relationship-Value`
# MAGIC           WHEN C3.GCID IS NOT NULL THEN C3.GCID
# MAGIC           WHEN RC1.`Relationship-Value` IS NOT NULL AND C1.Party_type <> 'Legal Entity' THEN RC1.`Relationship-Value`
# MAGIC           WHEN C2.GCID IS NOT NULL THEN C2.GCID
# MAGIC           WHEN RC.`Relationship-Value` IS NOT NULL AND C.Party_type <> 'Legal Entity' THEN RC.`Relationship-Value`
# MAGIC           WHEN C1.GCID IS NULL THEN C.GCID
# MAGIC           ELSE R.`Relationship-Value`
# MAGIC         END AS INT
# MAGIC       ) LegalEntity
# MAGIC       , R.`Relationship-Value`
# MAGIC       , R.`Relationship-Type`
# MAGIC       , C.GCID
# MAGIC    
# MAGIC     FROM client_Client C                                                      
# MAGIC     LEFT JOIN client_PartytoPartyRelationship RC ON RC.GCID = C.GCID AND RC.`Relationship-Type` = 'Credit'
# MAGIC     LEFT JOIN client_PartytoPartyRelationship R ON C.GCID = R.GCID 
# MAGIC       AND (
# MAGIC         (C.Party_type IN ('Branch','Foreign Branch') AND R.`Relationship-Type` = 'Branch of')
# MAGIC         OR (C.Party_type = 'Sub Account' AND R.`Relationship-Type` = 'Main Account')
# MAGIC         OR (C.Party_type = 'Organisation Unit' AND R.`Relationship-Type` = 'Organisation Unit of')
# MAGIC         OR (C.Party_type = 'Managed Fund' AND R.`Relationship-Type` = 'Managed by')
# MAGIC         OR (C.Party_type = 'Sub-fund' AND R.`Relationship-Type` = 'Sub-fund of')
# MAGIC       )
# MAGIC     LEFT JOIN client_Client C1 ON C1.GCID = R.`Relationship-Value`
# MAGIC       AND ((C.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C1.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
# MAGIC       OR (C.Party_type IN ('Branch','Foreign Branch') AND C1.Party_type = 'Legal Entity'))
# MAGIC     LEFT JOIN client_PartytoPartyRelationship RC1 ON RC1.GCID = C1.GCID AND RC1.`Relationship-Type` = 'Credit'
# MAGIC     LEFT JOIN client_PartytoPartyRelationship R1 ON R1.GCID = C1.GCID -- AND RC1.`Relationship-Value` IS NULL
# MAGIC       AND R1.`Relationship-Value` <> R1.GCID
# MAGIC       AND (
# MAGIC         (C1.Party_type IN ('Branch','Foreign Branch') AND R1.`Relationship-Type` = 'Branch of')
# MAGIC         OR (C1.Party_type = 'Sub Account' AND R1.`Relationship-Type` = 'Main Account')
# MAGIC         OR (C1.Party_type = 'Organisation Unit' AND R1.`Relationship-Type` = 'Organisation Unit of')
# MAGIC         OR (C1.Party_type = 'Managed Fund' AND R1.`Relationship-Type` = 'Managed by')
# MAGIC         OR (C1.Party_type = 'Sub-fund' AND R1.`Relationship-Type` = 'Sub-fund of')
# MAGIC       )
# MAGIC     LEFT JOIN client_Client C2 ON C2.GCID = R1.`Relationship-Value`
# MAGIC       AND ((C1.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C2.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
# MAGIC       OR (C1.Party_type IN ('Branch','Foreign Branch') AND C2.Party_type = 'Legal Entity'))
# MAGIC     LEFT JOIN client_PartytoPartyRelationship RC2 ON RC2.GCID = C2.GCID AND RC2.`Relationship-Type` = 'Credit'
# MAGIC     LEFT JOIN client_PartytoPartyRelationship R2 ON R2.GCID = C2.GCID 
# MAGIC       AND R2.`Relationship-Value` <> R2.GCID
# MAGIC       AND (
# MAGIC         (C2.Party_type IN ('Branch','Foreign Branch') AND R2.`Relationship-Type` = 'Branch of')
# MAGIC         OR (C2.Party_type = 'Sub Account' AND R2.`Relationship-Type` = 'Main Account')
# MAGIC         OR (C2.Party_type = 'Organisation Unit' AND R2.`Relationship-Type` = 'Organisation Unit of')
# MAGIC         OR (C2.Party_type = 'Managed Fund' AND R2.`Relationship-Type` = 'Managed by')
# MAGIC         OR (C2.Party_type = 'Sub-fund' AND R2.`Relationship-Type` = 'Sub-fund of')
# MAGIC       )
# MAGIC     LEFT JOIN client_Client C3 ON C3.GCID = R2.`Relationship-Value`
# MAGIC       AND ((C2.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C3.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
# MAGIC       OR ( C2.Party_type IN ('Branch','Foreign Branch') AND C3.Party_type = 'Legal Entity'))
# MAGIC     LEFT JOIN client_PartytoPartyRelationship RC3 ON RC3.GCID = C3.GCID 
# MAGIC       AND RC3.`Relationship-Type` = 'Credit'
# MAGIC     LEFT JOIN client_PartytoPartyRelationship R3 ON R3.GCID = R2.`Relationship-Value` 
# MAGIC       AND R3.`Relationship-Value` <> R3.GCID
# MAGIC       AND (
# MAGIC         (C3.Party_type IN ('Branch','Foreign Branch') AND R3.`Relationship-Type` = 'Branch of')
# MAGIC         OR (C3.Party_type = 'Sub Account' AND R3.`Relationship-Type` = 'Main Account')
# MAGIC         OR (C3.Party_type = 'Organisation Unit' AND R3.`Relationship-Type` = 'Organisation Unit of')
# MAGIC         OR (C3.Party_type = 'Managed Fund' AND R3.`Relationship-Type` = 'Managed by')
# MAGIC         OR (C3.Party_type = 'Sub-fund' AND R3.`Relationship-Type` = 'Sub-fund of')
# MAGIC       )
# MAGIC     LEFT JOIN client_Client C4 ON C4.GCID = R2.`Relationship-Value`
# MAGIC       AND ((C3.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C4.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
# MAGIC       OR (C3.Party_type IN ('Branch','Foreign Branch') AND C4.Party_type = 'Legal Entity'))

# COMMAND ----------

# DBTITLE 1,Join to get LeadingPartyIdentifier
# MAGIC %sql
# MAGIC create or replace temporary view AllParty_Final as
# MAGIC select distinct
# MAGIC pf.PartyIdentifier,
# MAGIC pf.FullLegalName,
# MAGIC pf.DateOfBirth,
# MAGIC pf.FirstName,
# MAGIC pf.MiddleName,
# MAGIC pf.LastName,
# MAGIC pf.Party_type,
# MAGIC pf.GlobalClientOwnerName,
# MAGIC pf.IsEligibleForFatcaAssessment,
# MAGIC pf.FatcaClassification,
# MAGIC pf.GIIN,
# MAGIC pf.EIN,
# MAGIC pf.FatcaDateOfIssue,
# MAGIC pf.FatcaComments,
# MAGIC pf.IsEligibleForCrsAssessment,
# MAGIC pf.CrsClassification,
# MAGIC pf.CrsFormSignedDate,
# MAGIC pf.CrsComments,
# MAGIC pf.LegalForm,
# MAGIC pf.LifeCycleStatus as CustomerLifeCycleStatus ,
# MAGIC pf.IsLatestApprovedVersionOfClient,
# MAGIC pf.FullLegalNameInLocalLanguage,
# MAGIC pf.IsIncorporated,
# MAGIC pf.IncorporationNumber,
# MAGIC pf.IncorporationDate,
# MAGIC pf.HasSourceOfWealth,
# MAGIC pf.SanctionsOrExternalWatchlist,
# MAGIC pf.InternalWatchlist,
# MAGIC pf.AdverseInformationOrMedia,
# MAGIC pf.StatedFindings,
# MAGIC pf.PEPStatus,
# MAGIC pf.IsTrust,
# MAGIC pf.TypeOfTrust,
# MAGIC pf.IsClientRegulated,
# MAGIC pf.RegulatorName,
# MAGIC pf.RegulatorCountry,
# MAGIC pf.HasRecognisedRegulator,
# MAGIC pf.IsClientListed,
# MAGIC pf.ExchangeName,
# MAGIC pf.ExchangeCountry,
# MAGIC pf.HasRecognisedExchange,
# MAGIC pf.CountryOfTaxResidence,
# MAGIC pf.TinAvailable,
# MAGIC pf.TinOrEquivalent,
# MAGIC pf.TinUnavailabilityReason,
# MAGIC pf.ExplanationForTinBeingUnavailable,
# MAGIC pf.SourceOfIdentificationDocument,
# MAGIC pf.IdentificationDocumentNumber,
# MAGIC pf.SourceOfVerifiedDocument,
# MAGIC pf.VerifiedDocumentNumber,
# MAGIC pf.CddType,
# MAGIC pf.FIHubIndicator,
# MAGIC pf.`W&RORRetail`,
# MAGIC concat("GCDS_",gl.LegalEntity) as Party_LeadingPartyIdentifier  
# MAGIC ,pf.IsRabobankEntity
# MAGIC ,pf.`HO_reporting_party-FINREP_Code`
# MAGIC ,pf.`HO_reporting_party-FINREP`
# MAGIC -- Add KYCGroup and SectorTeam in this view
# MAGIC ,pf.KYCGroup
# MAGIC ,pf.SectorTeam
# MAGIC from Party_Final pf
# MAGIC left outer join GCDS_LeadGCID2 gl on gl.GCID=pf.GCID

# COMMAND ----------

df_Party_Final=spark.table('AllParty_Final')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
if RunType == "historical":
    save_to_saradar_storage_account(df_Party_Final, party_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_Party_Final, party_dataobject, radar_datamodel_version_number, environment)
