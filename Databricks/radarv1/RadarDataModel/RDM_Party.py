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
# MAGIC #### Expected output
# MAGIC   - Column List is added at the end of this notebook 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read Files from GDP

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
import os
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
SARADAR = "saradar" + environment

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Defining the dataobjects' name
party_dataobject='Party'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Define Date variables
#Derive the date for which data has to be processes
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
ThisDay = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' +ThisDay+ '*'
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
    Read_GDP_Defined_DataObjects(Source='GCDS', Dataobject=row.definedDatasetname)

# COMMAND ----------

# DBTITLE 1,Read GCOB data from GDP defined layer
# print list of strings for loading spark dfs from GDP
load_df =[
'party_case_client_details',
'party_AllPartyDetails' 
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# DBTITLE 1,Read Legacy2 data from GDP defined layer
# List of datasets from GDP
load_df = [
'Legacy2_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

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
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname)

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
    Read_GDP_Defined_DataObjects(Source='KN1', Dataobject=row.GDPname)

# COMMAND ----------

# DBTITLE 1,Reading Party_SystemIdentifier dataobject
df_Party_SystemIdentifier = spark.read.parquet(
    f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/1/data/{load_dts}/*.parquet"
)

# COMMAND ----------

df_Party_KN1Cases = spark.read.parquet(f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_KN1Cases/1/data/{load_dts}/*.parquet")
df_Party_KN1Cases.createOrReplaceTempView("KN1Cases")

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
# MAGIC   select f.SEQ_PESSOA,f.DTA_NASCIMENTO from PESSOA_FISICA f
# MAGIC   inner join DTA_NASCIMENTO n on f.SEQ_PESSOA = n.SEQ_PESSOA and f.SEQ_HISTORICO = n.SEQ_HISTORICO
# MAGIC )
# MAGIC -- identification
# MAGIC ,JURIDICA_IDENTIFICACAO as 
# MAGIC (
# MAGIC   select max(SEQ_HISTORICO) as SEQ_HISTORICO, SEQ_PESSOA from PESSOA_JURIDICA where FLG_PENDENTE = 'False' group by SEQ_PESSOA
# MAGIC )
# MAGIC ,PESSOA_JURIDICA_IDENTIFICACAO 
# MAGIC (
# MAGIC   select f.SEQ_PESSOA,f.NUM_CNPJ as Entity_TinAvailable ,f.NUM_IDENTIFICACAO_FISCAL,f.DTA_CONSTITUICAO ,E_Nat.NOM_PAIS as Entity_Nationality,E_Nat.NOM_PAIS as CountryOfTaxResidence,E_Nat.SGL_PAIS as NationalityIsoCode, lf.DES_TIPO_SOCIEDADE as LegalForm
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
# MAGIC   select f.SEQ_PESSOA,f.NUM_CPF as NP_TinAvailable ,f.NUM_DOC_IDENTIFICACAO as IdentificationDocumentNumber,cit.NOM_PAIS as Citizenship,nat.NOM_PAIS as NP_Nationality,nat.SGL_PAIS as NationalityIsoCode from pessoa_fisica f
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
# MAGIC ,Min_COD_INSTITUCIONAL as
# MAGIC (
# MAGIC   select min(COD_INSTITUCIONAL) as COD_INSTITUCIONAL,trim(upper(NOM_COMPLETO)) as NOM_COMPLETO from pessoa group by trim(upper(NOM_COMPLETO))
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
# MAGIC ,CASE WHEN p.SEQ_TIPO_PESSOA = '2' THEN fisica.NP_TinAvailable else i.Entity_TinAvailable END AS TinAvailable
# MAGIC ,CASE WHEN p.SEQ_TIPO_PESSOA = '2' THEN fisica.NP_Nationality else i.Entity_Nationality END AS Nationality
# MAGIC ,CASE WHEN p.SEQ_TIPO_PESSOA = '2' THEN fisica.NationalityIsoCode else i.NationalityIsoCode END AS NationalityIsoCode
# MAGIC ,i.CountryOfTaxResidence
# MAGIC ,fisica.IdentificationDocumentNumber
# MAGIC ,fisica.Citizenship
# MAGIC ,p.FLG_ESTRANGEIRA as TinOrEquivalent
# MAGIC ,CONCAT('GIC_', p.COD_INSTITUCIONAL) as LocalSystemIdentifier
# MAGIC ,'Rabobank Brazil' as GlobalClientOwnerLocation
# MAGIC ,c.DES_STATUS_TIPO_CADASTRO as ClientLifeCycleStatus
# MAGIC ,cs.IsLatestCase as IsLatestApprovedVersionOfClient
# MAGIC from pessoa p
# MAGIC --INNER JOIN Min_COD_INSTITUCIONAL max on p.COD_INSTITUCIONAL = max.COD_INSTITUCIONAL
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
# MAGIC --and IsLatestApprovedVersionofclient = 'True'

# COMMAND ----------

# DBTITLE 1,GCOB All Party Details update to join on client type
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllPartyDetails_Row As
# MAGIC with Nationality_Citizenship as 
# MAGIC (
# MAGIC SELECT distinct
# MAGIC UniquePartyId,
# MAGIC array_join(Collect_Set(Nationality), ', ') as Nationality,
# MAGIC array_join(Collect_Set(NationalityIsoCode), ', ') as NationalityIsoCode,
# MAGIC array_join(Collect_Set(Citizenship), ', ') AS Citizenship,
# MAGIC array_join(Collect_Set(CitizenshipIsoCode), ', ') AS CitizenshipIsoCode
# MAGIC FROM party_AllPartyDetails
# MAGIC where Status = 'Live'
# MAGIC GROUP BY UniquePartyId
# MAGIC )
# MAGIC select distinct 
# MAGIC p.FullLegalName,p.DateOfBirth,p.FirstName,p.MiddleName,p.LastName,p.GlobalClientOwnerName,p.GlobalClientOwnerLocation,p.IsEligibleForFatcaAssessment,p.FatcaClassification,p.GIIN,p.EIN,p.FatcaDateOfIssue,p.FatcaComments,p.IsEligibleForCrsAssessment,p.CrsClassification,p.CrsFormSignedDate,p.CrsComments,p.LegalForm,p.ClientLifeCycleStatus,p.CaseStatusName,p.IsLatestApprovedVersionOfClient,p.FullLegalNameInLocalLanguage,p.IsIncorporated,p.IncorporationNumber,p.IncorporationDate,p.BusinessLineName,p.HasSourceOfWealth,p.SanctionsOrExternalWatchlist,p.InternalWatchlist,p.AdverseInformationOrMedia,p.StatedFindings,p.PEPStatus,p.IsTrust,p.TypeOfTrust,p.IsClientRegulated,p.RegulatorName,p.RegulatorCountry,p.HasRecognisedRegulator,p.IsClientListed,p.ExchangeName,p.ExchangeCountry,p.HasRecognisedExchange,p.CountryOfTaxResidence,p.TinAvailable,p.TinOrEquivalent,p.TinUnavailabilityReason,p.ExplanationForTinBeingUnavailable,p.SourceOfIdentificationDocument,p.IdentificationDocumentNumber,p.SourceOfVerifiedDocument,p.VerifiedDocumentNumber,p.CddType,p.ClientType,p.gcobid,p.UniquePartyId,p.gcdsid,p.PartyId,p.caseid
# MAGIC ,nc.Nationality,nc.NationalityIsoCode,nc.Citizenship,nc.CitizenshipIsoCode,c.FIHubIndicator
# MAGIC ,case when p.ClientType = 'LegalEntityClient' then 'Legal Entity' 
# MAGIC       when p.ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when p.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity' 
# MAGIC       when p.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,CONCAT('GCOB_', p.UniquePartyId) AS LocalSystemIdentifier
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY p.gcobid,p.ClientType ORDER BY CASE WHEN p.ClientLifeCycleStatus='Client' THEN 1 ELSE 2 END ASC, c.CaseCompletedDate DESC) AS ROWNUM
# MAGIC from party_AllPartyDetails p
# MAGIC Inner join Gcob_CaseClientDetails c on p.UniquePartyId = c.UniqueGcobId
# MAGIC --CASE WHEN p.ClientType='LegalEntityClient' THEN concat('LEC_',p.Id) WHEN p.ClientType IN ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_NPPC_',p.Id) end = c.SourceClient
# MAGIC left join Nationality_Citizenship nc on p.UniquePartyId = nc.UniquePartyId
# MAGIC where Status = 'Live'
# MAGIC --and p.CaseStatusName <> 'Cancelled'
# MAGIC --and p.IsLatestApprovedVersionofclient = 'True'

# COMMAND ----------

# DBTITLE 1,GCOB for details not in GCDS
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllParty As
# MAGIC With Nationality_Citizenship as 
# MAGIC (
# MAGIC SELECT distinct
# MAGIC UniquePartyId,
# MAGIC array_join(Collect_Set(Nationality), ', ') as Nationality,
# MAGIC array_join(Collect_Set(NationalityIsoCode), ', ') as NationalityIsoCode,
# MAGIC array_join(Collect_Set(Citizenship), ', ') AS Citizenship,
# MAGIC array_join(Collect_Set(CitizenshipIsoCode), ', ') AS CitizenshipIsoCode
# MAGIC FROM party_AllPartyDetails
# MAGIC where Status = 'Live'
# MAGIC GROUP BY UniquePartyId
# MAGIC )
# MAGIC ,details as 
# MAGIC (
# MAGIC select distinct
# MAGIC p.FullLegalName,p.DateOfBirth,p.FirstName,p.MiddleName,p.LastName,p.GlobalClientOwnerName,p.GlobalClientOwnerLocation,p.IsEligibleForFatcaAssessment,p.FatcaClassification,p.GIIN,p.EIN,p.FatcaDateOfIssue,p.FatcaComments,p.IsEligibleForCrsAssessment,p.CrsClassification,p.CrsFormSignedDate,p.CrsComments,p.LegalForm,p.ClientLifeCycleStatus,p.IsLatestApprovedVersionOfClient,p.FullLegalNameInLocalLanguage,p.IsIncorporated,p.IncorporationNumber,p.IncorporationDate,p.BusinessLineName,p.HasSourceOfWealth,p.SanctionsOrExternalWatchlist,p.InternalWatchlist,p.AdverseInformationOrMedia,p.StatedFindings,p.PEPStatus,p.IsTrust,p.TypeOfTrust,p.IsClientRegulated,p.RegulatorName,p.RegulatorCountry,p.HasRecognisedRegulator,p.IsClientListed,p.ExchangeName,p.ExchangeCountry,p.HasRecognisedExchange,p.CountryOfTaxResidence,p.TinAvailable,p.TinOrEquivalent,p.TinUnavailabilityReason,p.ExplanationForTinBeingUnavailable,p.SourceOfIdentificationDocument,p.IdentificationDocumentNumber,p.SourceOfVerifiedDocument,p.VerifiedDocumentNumber,p.CddType,p.ClientType,p.gcobid,p.UniquePartyId,p.gcdsid,p.PartyId,p.caseid
# MAGIC ,nc.Nationality,nc.NationalityIsoCode,nc.Citizenship,nc.CitizenshipIsoCode,c.FIHubIndicator
# MAGIC ,case when p.ClientType = 'LegalEntityClient' then 'Legal Entity' 
# MAGIC       when p.ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') then 'Natural Person' 
# MAGIC       when p.ClientType = 'RelatedLegalEntity' then 'Related Legal Entity' 
# MAGIC       when p.ClientType = 'RelatedNaturalPerson' then 'Related Natural Person' 
# MAGIC       else null
# MAGIC end as Party_type
# MAGIC ,case when p.CaseStatusName is null then 'RelatedParty' else c.CaseStatusName end as CaseStatusName
# MAGIC ,c.SalesforceClientID_nCino
# MAGIC ,c.gcobid as ncino_gcobid
# MAGIC ,c.CaseCompletedDate
# MAGIC ,CONCAT('GCOB_', p.UniquePartyId) AS LocalSystemIdentifier
# MAGIC from party_AllPartyDetails p
# MAGIC LEFT JOIN Gcob_CaseClientDetails c on p.UniquePartyId = c.UniqueGcobId
# MAGIC --on CASE WHEN p.ClientType='LegalEntityClient' THEN concat('LEC_',p.Id) WHEN p.ClientType IN ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_NPPC_',p.Id) end = c.SourceClient and p.ClientType in ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)')
# MAGIC left join Nationality_Citizenship nc on p.UniquePartyId = nc.UniquePartyId
# MAGIC where Status = 'Live'
# MAGIC --and (p.IsLatestApprovedVersionofclient = 'True' or p.IsLatestApprovedVersionofclient is null)
# MAGIC )
# MAGIC select distinct
# MAGIC *
# MAGIC ,concat(GCOBId,'-',Party_Type) as GCOB_Party
# MAGIC ,concat(SalesforceClientID_nCino,'-',Party_Type) as GCOB_NCINO_Party
# MAGIC from details
# MAGIC where CaseStatusName <> 'Cancelled'

# COMMAND ----------

# DBTITLE 1,GCOB All Party Details Unique GCOBId
# MAGIC %sql
# MAGIC Create or replace temporary view Gcob_AllPartyDetails As
# MAGIC select * from Gcob_AllPartyDetails_Row --where ROWNUM <> 2 

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
# MAGIC ,null as gcid,null as GCOB_Identifier,null as GCOB_NCINO_Identifier,null as GIC_Identifier
# MAGIC ,CASE
# MAGIC   WHEN IsClient = 'true' and  ClientTypeId = 1 THEN concat('LE_', GcobId)
# MAGIC   WHEN IsClient = 'true' and ClientTypeId in (2,3) THEN concat('NP_NPPC_', GcobId)
# MAGIC   WHEN IsClient = 'false' and ClientTypeId = 1 THEN concat('RLE_', GcobId)
# MAGIC   WHEN IsClient = 'false' and ClientTypeId in (2,3) THEN concat('RNP_', GcobId)
# MAGIC END as Legacy2_Identifier
# MAGIC ,FullLegalName,DateOfBirth
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
# MAGIC ,null as CrsComments,null as LegalForm,ClientLifeCycleName as ClientLifeCycleStatus,StatusTypeName as CaseStatusName
# MAGIC ,IsLatestApprovedVersionOfClient,FullLegalNameLocalLanguage as FullLegalNameInLocalLanguage,IsIncorporated,IncorporationNumber,null as IncorporationDate,BusinessLineName,null as HasSourceOfWealth,SanctionsOrExternalWatchlist
# MAGIC ,null as InternalWatchlist,null as AdverseInformationOrMedia,null as StatedFindings,null as PEPStatus,null as IsTrust
# MAGIC ,null as TypeOfTrust,null as IsClientRegulated,null as RegulatorName,null as RegulatorCountry,null as HasRecognisedRegulator,null as IsClientListed,null as ExchangeName,null as ExchangeCountry,null as HasRecognisedExchange
# MAGIC ,null as CountryOfTaxResidence,null as TinAvailable,null as TinOrEquivalent,null as TinUnavailabilityReason,null as ExplanationForTinBeingUnavailable,null as SourceOfIdentificationDocument,null as IdentificationDocumentNumber,null as SourceOfVerifiedDocument,null as VerifiedDocumentNumber,CddType,null as Nationality,null as NationalityIsoCode,null as Citizenship,null as CitizenshipIsoCode,Null as FIHubIndicator
# MAGIC from Legacy2_client 
# MAGIC where ClientTypeId in (1,2,3)
# MAGIC --and IsLatestApprovedVersionofclient = 'True'

# COMMAND ----------

# DBTITLE 1,Read GCDS "GCOB"-"GCOB-NCINO"-"GIC" Client data
# MAGIC %sql
# MAGIC Create or replace temporary view GCDS_Clients As
# MAGIC select distinct
# MAGIC   k.KeyStore_value as identifier
# MAGIC , k.KeyStore_type
# MAGIC , k.status AS status
# MAGIC , k.bank_code
# MAGIC , c.*
# MAGIC , pr.Party_role
# MAGIC , pr.Life_cycle_status
# MAGIC , rel.`Relationship-Type` as RelationshipType
# MAGIC , rel.`Relationship-Value` as RelationshipValue_GCID
# MAGIC , case when c.Party_type <> 'Natural Person' then c.Full_legal_name else c.Person_Name end Full_Name
# MAGIC from client_KeyStoreKey k
# MAGIC inner join client_Client c on c.GCID = k.GCID
# MAGIC left join client_PartyRole pr on k.GCID = pr.GCID
# MAGIC left join client_PartytoPartyRelationship rel on k.GCID = rel.GCID
# MAGIC -- where KeyStore_type in ('GCOBID','NCINOID','GIC')
# MAGIC --and k.status = 'Active'
# MAGIC --and k.KeyStore_value not in (select distinct gcobid from Gcob_AllPartyDetails_Row where ROWNUM = 2 and gcdsid is not null)

# COMMAND ----------

# DBTITLE 1,select gcds customers
# MAGIC %sql
# MAGIC Create or replace temporary view gcds_col AS
# MAGIC select t2.life_cycle_status
# MAGIC , t2.party_role
# MAGIC ,t1.*
# MAGIC from Client_Client t1 
# MAGIC left join client_PartyRole t2 on t1.gcid = t2.gcid
# MAGIC

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
# MAGIC FROM gcds_col c
# MAGIC LEFT JOIN OnboardedLocation o ON c.gcid = o.gcid
# MAGIC LEFT JOIN ProductsLocation p ON c.gcid = p.gcid 
# MAGIC -- WHERE o.WholeSaleOrRetailParty='Retail' AND p.WholeSaleOrRetailParty='W&R'  --c.gcid = 1142747;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####GCDS - Other Sources Party Data

# COMMAND ----------

# DBTITLE 1,GCDS-Other Sources Clients
# MAGIC %sql
# MAGIC --Create or replace temporary view GCDS_OtherSources_Party As
# MAGIC Create or replace temporary view Party As
# MAGIC select distinct
# MAGIC gcds.gcid 
# MAGIC --,gcds.identifier as GCDS_KeyStore_value
# MAGIC --,case when c.SalesforceClientID_nCino is not null then ncino.UniquePartyId else gcob.UniquePartyId end as GCOB_Identifier
# MAGIC ,case when c.SalesforceClientID_nCino is not null and ncino.Party_type = 'Natural Person' then ncino.UniquePartyId
# MAGIC       when np.GcobId is not null and np.Party_type = 'Natural Person' then np.UniquePartyId
# MAGIC       else gcob.UniquePartyId
# MAGIC end as GCOB_Identifier
# MAGIC --,gcob.UniquePartyId as GCOB_Identifier 
# MAGIC --,c.SalesforceClientID_nCino as GCOB_NCINO_Identifier
# MAGIC ,gic.COD_INSTITUCIONAL as GIC_Identifier
# MAGIC ,l2.Legacy2_Identifier as Legacy2_Identifier
# MAGIC ,null as NLSVF_Identifier
# MAGIC ,COALESCE(gcds.Full_Name, gcob.FullLegalName, ncino.FullLegalName, gic.NOM_COMPLETO,l2.FullLegalName) AS FullLegalName
# MAGIC ,COALESCE(gcds.DateOfBirth, gcob.DateOfBirth, ncino.DateOfBirth, gic.DTA_NASCIMENTO,l2.DateOfBirth) AS DateOfBirth
# MAGIC ,COALESCE(gcob.FirstName, ncino.FirstName,l2.FirstName) as FirstName
# MAGIC ,COALESCE(gcob.MiddleName, ncino.MiddleName,l2.MiddleName) as MiddleName
# MAGIC ,COALESCE(gcob.LastName, ncino.LastName, l2.LastName) as LastName
# MAGIC ,COALESCE(gcds.Party_type, gcob.Party_type, ncino.Party_type, gic.Party_type, l2.Party_type) AS Party_type
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
# MAGIC ,COALESCE(gcds.Life_cycle_status, gcob.ClientLifeCycleStatus, ncino.ClientLifeCycleStatus, l2.ClientLifeCycleStatus, gic.ClientLifeCycleStatus) AS LifeCycleStatus
# MAGIC ,COALESCE(gcob.CaseStatusName, ncino.CaseStatusName,gic.StatusName, l2.CaseStatusName,'Undefined') AS StatusName
# MAGIC ,COALESCE(gcob.IsLatestApprovedVersionOfClient, ncino.IsLatestApprovedVersionOfClient, l2.IsLatestApprovedVersionOfClient,gic.IsLatestApprovedVersionOfClient) as IsLatestApprovedVersionOfClient
# MAGIC ,COALESCE(gcds.Local_name,gcob.FullLegalNameInLocalLanguage,ncino.FullLegalNameInLocalLanguage,gic.NOM_COMPLETO,l2.FullLegalNameInLocalLanguage) AS FullLegalNameInLocalLanguage
# MAGIC ,COALESCE(gcob.IsIncorporated, ncino.IsIncorporated, l2.IsIncorporated) as IsIncorporated
# MAGIC ,COALESCE(gcob.IncorporationNumber, ncino.IncorporationNumber, l2.IncorporationNumber) as IncorporationNumber
# MAGIC ,COALESCE(gcds.Registration_Incor_Date,gcob.IncorporationDate,ncino.IncorporationDate, gic.IncorporationDate,l2.IncorporationDate) AS IncorporationDate
# MAGIC ,COALESCE(gcds.`CO-businessline_description`,gcob.BusinessLineName,ncino.BusinessLineName,l2.BusinessLineName) AS BusinessLineName
# MAGIC ,COALESCE(gcob.HasSourceOfWealth, ncino.HasSourceOfWealth, l2.HasSourceOfWealth) as HasSourceOfWealth
# MAGIC ,COALESCE(gcob.SanctionsOrExternalWatchlist, ncino.SanctionsOrExternalWatchlist, l2.SanctionsOrExternalWatchlist) as SanctionsOrExternalWatchlist
# MAGIC ,COALESCE(gcob.InternalWatchlist, ncino.InternalWatchlist, l2.InternalWatchlist) as InternalWatchlist
# MAGIC ,COALESCE(gcob.AdverseInformationOrMedia, ncino.AdverseInformationOrMedia, l2.AdverseInformationOrMedia) as AdverseInformationOrMedia
# MAGIC ,COALESCE(gcob.StatedFindings, ncino.StatedFindings, l2.StatedFindings) as StatedFindings
# MAGIC ,COALESCE(gcob.PEPStatus, ncino.PEPStatus, l2.PEPStatus) as PEPStatus
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
# MAGIC ,COALESCE(gcob.CountryOfTaxResidence, ncino.CountryOfTaxResidence,gic.CountryOfTaxResidence, l2.CountryOfTaxResidence) as CountryOfTaxResidence
# MAGIC ,COALESCE(gcob.TinAvailable, ncino.TinAvailable,gic.TinAvailable, l2.TinAvailable) as TinAvailable
# MAGIC ,COALESCE(gcob.TinOrEquivalent, ncino.TinOrEquivalent,cast(gic.TinOrEquivalent as STRING), l2.TinOrEquivalent) as TinOrEquivalent
# MAGIC ,COALESCE(gcob.TinUnavailabilityReason, ncino.TinUnavailabilityReason, l2.TinUnavailabilityReason) as TinUnavailabilityReason
# MAGIC ,COALESCE(gcob.ExplanationForTinBeingUnavailable, ncino.ExplanationForTinBeingUnavailable, l2.ExplanationForTinBeingUnavailable) as ExplanationForTinBeingUnavailable
# MAGIC ,COALESCE(gcob.SourceOfIdentificationDocument, ncino.SourceOfIdentificationDocument, l2.SourceOfIdentificationDocument) as SourceOfIdentificationDocument
# MAGIC ,COALESCE(gcob.IdentificationDocumentNumber, ncino.IdentificationDocumentNumber,gic.IdentificationDocumentNumber, l2.IdentificationDocumentNumber) as IdentificationDocumentNumber
# MAGIC ,COALESCE(gcob.SourceOfVerifiedDocument, ncino.SourceOfVerifiedDocument, l2.SourceOfVerifiedDocument) as SourceOfVerifiedDocument
# MAGIC ,COALESCE(gcob.VerifiedDocumentNumber, ncino.VerifiedDocumentNumber, l2.VerifiedDocumentNumber) as VerifiedDocumentNumber
# MAGIC ,COALESCE(gcds.`CDD-entitytype`,gcob.CddType,ncino.CddType,l2.CddType) AS CddType
# MAGIC ,COALESCE(gcds.Nationality,gcob.Nationality,ncino.Nationality,gic.Nationality,l2.Nationality) AS Nationality
# MAGIC ,COALESCE(gcob.NationalityIsoCode, ncino.NationalityIsoCode,gic.NationalityIsoCode, l2.NationalityIsoCode) as NationalityIsoCode
# MAGIC ,COALESCE(gcds.Citizenship,gcob.Citizenship,ncino.Citizenship,gic.Citizenship,l2.Citizenship) AS Citizenship
# MAGIC ,COALESCE(gcob.CitizenshipIsoCode, ncino.CitizenshipIsoCode, l2.CitizenshipIsoCode) as CitizenshipIsoCode
# MAGIC ,gcob.FIHubIndicator
# MAGIC , gcds.Bank_code
# MAGIC ,wr.WRorRetail as `W&RORRetail`
# MAGIC ,CASE WHEN gcds.identifier = gcob.GcobId --AND gcds.GCID = gcob.GCDSID 
# MAGIC       AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' THEN gcob.LocalSystemIdentifier
# MAGIC      WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person' THEN ncino.LocalSystemIdentifier
# MAGIC      WHEN gcds.identifier = gic.COD_INSTITUCIONAL AND gcds.KeyStore_type = 'GIC' THEN gic.LocalSystemIdentifier
# MAGIC      WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC      THEN l2.LocalSystemIdentifier
# MAGIC      else CONCAT('GCDS_',gcds.gcid)
# MAGIC END AS LocalSystemIdentifier
# MAGIC ,CASE WHEN gcds.identifier = gcob.GcobId --AND gcds.GCID = gcob.GCDSID 
# MAGIC       AND gcob.Party_type = 'Legal Entity' AND gcds.KeyStore_type = 'GCOBID' THEN 'GCOB'
# MAGIC      WHEN c.ClientId = ncino.PartyId AND ncino.Party_type = 'Natural Person' THEN 'GCOB'
# MAGIC      WHEN gcds.identifier = gic.COD_INSTITUCIONAL AND gcds.KeyStore_type = 'GIC' THEN 'GIC'
# MAGIC      WHEN l2.SalesforceClientID_nCino = gcds.identifier AND l2.Party_type = gcds.Party_type AND gcds.KeyStore_type = 'NCINOID'
# MAGIC      THEN 'Legacy2'
# MAGIC      else 'GCDS'
# MAGIC END AS Application
# MAGIC from GCDS_Clients gcds
# MAGIC LEFT JOIN Gcob_AllPartyDetails gcob on gcds.identifier = gcob.GcobId
# MAGIC         --and gcds.GCID = gcob.GCDSID
# MAGIC         and gcob.Party_type = 'Legal Entity'
# MAGIC         --and gcds.party_role = 'Customer'
# MAGIC         and gcds.KeyStore_type = 'GCOBID'
# MAGIC         and gcds.Party_type <> 'Natural Person'
# MAGIC LEFT JOIN Gcob_CaseClientDetails c on c.SalesforceClientID_nCino = gcds.identifier 
# MAGIC         and gcds.KeyStore_type = 'NCINOID'
# MAGIC         --and gcds.party_role = 'Customer'
# MAGIC LEFT JOIN Gcob_AllPartyDetails ncino on c.ClientId = ncino.PartyId 
# MAGIC         and ncino.Party_type = 'Natural Person' 
# MAGIC         --and gcds.party_role = 'Customer'
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
# MAGIC LEFT JOIN wr_or_retail wr ON gcds.GCID = wr.gcid
# MAGIC
# MAGIC --where
# MAGIC --gcds.party_role = 'Customer'
# MAGIC /*
# MAGIC AND
# MAGIC (
# MAGIC gcob.gcobid is not null
# MAGIC OR c.SalesforceClientID_nCino is not null
# MAGIC OR gic.COD_INSTITUCIONAL is not null
# MAGIC --OR s.SiebelId is not null
# MAGIC )
# MAGIC */

# COMMAND ----------

# DBTITLE 1,GCOB data Not in GCDS
# MAGIC %sql
# MAGIC Create or replace temporary view GCOB_NonGCDS As
# MAGIC with GCOB as 
# MAGIC (
# MAGIC select distinct
# MAGIC CONCAT('GCOB_',t1.UniquePartyId) as LocalSystemIdentifier
# MAGIC ,null as gcid
# MAGIC ,t1.UniquePartyId as GCOB_Identifier
# MAGIC ,null as GCOB_NCINO_Identifier
# MAGIC ,null as GIC_Identifier
# MAGIC ,null as Legacy2_Identifier 
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
# MAGIC ,t1.BusinessLineName
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
# MAGIC --,t1.Nationality
# MAGIC --,t1.NationalityIsoCode
# MAGIC --,t1.Citizenship
# MAGIC --,t1.CitizenshipIsoCode
# MAGIC --,ROW_NUMBER() OVER (PARTITION BY t1.gcobid,t1.ClientType ORDER BY t1.caseid desc) AS ROWNUM
# MAGIC ,t1.caseid
# MAGIC ,t1.FIHubIndicator
# MAGIC from 
# MAGIC Gcob_AllParty t1
# MAGIC /*
# MAGIC left join Party_Gcob_Check t2 on t1.UniquePartyId = t2.GCOB_Identifier
# MAGIC where t2.GCOB_Party is null
# MAGIC and t1.Party_Type <> 'Natural Person'
# MAGIC */
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC CONCAT('GCOB_',t1.UniquePartyId) as LocalSystemIdentifier
# MAGIC ,null as gcid
# MAGIC ,t1.UniquePartyId as GCOB_Identifier
# MAGIC ,t1.UniquePartyId as GCOB_NCINO_Identifier
# MAGIC ,null as GIC_Identifier
# MAGIC ,null as Legacy2_Identifier
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
# MAGIC ,t1.BusinessLineName
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
# MAGIC --,t1.Nationality
# MAGIC --,t1.NationalityIsoCode
# MAGIC --,t1.Citizenship
# MAGIC --,t1.CitizenshipIsoCode
# MAGIC --,ROW_NUMBER() OVER (PARTITION BY t1.GcobId,t1.ClientType ORDER BY caseid desc) AS ROWNUM
# MAGIC ,t1.caseid
# MAGIC ,t1.FIHubIndicator
# MAGIC from 
# MAGIC Gcob_AllParty t1
# MAGIC /*
# MAGIC left join Party_ncino_Check t2 on t1.GCOB_NCINO_Party = t2.GCOB_NCINO_Party
# MAGIC where t2.GCOB_NCINO_Party is null
# MAGIC and t1.Party_Type = 'Natural Person'
# MAGIC */
# MAGIC )
# MAGIC select distinct
# MAGIC gcid
# MAGIC ,GCOB_Identifier
# MAGIC ,GIC_Identifier
# MAGIC ,Legacy2_Identifier
# MAGIC ,null as NLSVF_Identifier
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
# MAGIC ,BusinessLineName
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
# MAGIC ,null as Nationality
# MAGIC ,null as NationalityIsoCode
# MAGIC ,null as Citizenship
# MAGIC ,null as CitizenshipIsoCode
# MAGIC ,FIHubIndicator
# MAGIC ,null as Bank_code
# MAGIC ,null as `W&RORRetail`
# MAGIC ,LocalSystemIdentifier
# MAGIC ,'GCOB' as Application
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY LocalSystemIdentifier,Party_Type ORDER BY caseid desc,InternalWatchlist desc) AS ROWNUM
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
# MAGIC ,null as GCOB_Identifier
# MAGIC --,null as GCOB_NCINO_Identifier
# MAGIC ,t1.COD_INSTITUCIONAL as GIC_Identifier
# MAGIC ,null as Legacy2_Identifier
# MAGIC ,null as NLSVF_Identifier
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
# MAGIC ,null as BusinessLineName
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
# MAGIC ,t1.TinAvailable
# MAGIC ,cast(t1.TinOrEquivalent as STRING) as TinOrEquivalent
# MAGIC ,null as TinUnavailabilityReason
# MAGIC ,null as ExplanationForTinBeingUnavailable
# MAGIC ,null as SourceOfIdentificationDocument
# MAGIC ,t1.IdentificationDocumentNumber
# MAGIC ,null as SourceOfVerifiedDocument
# MAGIC ,null as VerifiedDocumentNumber
# MAGIC ,null as CddType
# MAGIC ,t1.Nationality
# MAGIC ,t1.NationalityIsoCode
# MAGIC ,t1.Citizenship
# MAGIC ,null as CitizenshipIsoCode
# MAGIC ,null as FIHubIndicator
# MAGIC ,null as Bank_code
# MAGIC ,null as `W&RORRetail`
# MAGIC ,CONCAT('GIC_',t1.COD_INSTITUCIONAL) as LocalSystemIdentifier
# MAGIC ,'GIC' as Application
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY t1.COD_INSTITUCIONAL,t1.Party_Type ORDER BY t1.COD_INSTITUCIONAL DESC) AS ROWNUM
# MAGIC From GIC_ClientDetails t1
# MAGIC /*
# MAGIC left join Party_GIC_Check t2 on t1.GIC_Party = t2.GIC_Party
# MAGIC where t2.GIC_Party is null
# MAGIC */
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC t1.gcid
# MAGIC ,t1.GCOB_Identifier
# MAGIC --,t1.GCOB_NCINO_Identifier
# MAGIC ,t1.GIC_Identifier
# MAGIC ,t1.Legacy2_Identifier 
# MAGIC ,null as NLSVF_Identifier
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
# MAGIC ,t1.BusinessLineName
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
# MAGIC ,t1.Nationality
# MAGIC ,t1.NationalityIsoCode
# MAGIC ,t1.Citizenship
# MAGIC ,t1.CitizenshipIsoCode
# MAGIC ,t1.FIHubIndicator
# MAGIC ,null as Bank_code
# MAGIC ,null as `W&RORRetail`
# MAGIC ,CONCAT('LEGACY2_',t1.Legacy2_Identifier) as LocalSystemIdentifier
# MAGIC ,'Legacy2' as Application
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY t1.Legacy2_Identifier ORDER BY t1.clientid DESC) AS ROWNUM
# MAGIC from Legacy2 as t1
# MAGIC /*
# MAGIC left join Party_Legacy2_Check t2 on t1.Legacy2_Identifier = t2.Legacy2_Identifier
# MAGIC where t2.Legacy2_Identifier is null
# MAGIC
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct
# MAGIC null as gcid
# MAGIC ,null as GCOB_Identifier
# MAGIC ,null as GIC_Identifier
# MAGIC ,null as Legacy2_Identifier
# MAGIC ,c.CIFNUMBER as NLSVF_Identifier
# MAGIC ,c.MAIL_NAME1 as FullLegalName
# MAGIC ,c.DOB as DateOfBirth
# MAGIC ,c.FIRSTNAME1 as FirstName
# MAGIC ,c.MIDDLENAME1 as MiddleName
# MAGIC ,c.LASTNAME1 as LastName
# MAGIC ,case when c.ENTITY = 'Individual' then 'Natural Person' else 'Legal Entity' end as Party_type
# MAGIC ,Null as GlobalClientOwnerName
# MAGIC ,Null as GlobalClientOwnerLocation
# MAGIC ,Null as IsEligibleForFatcaAssessment
# MAGIC ,Null as FatcaClassification
# MAGIC ,Null as GIIN
# MAGIC ,Null as EIN
# MAGIC ,Null as FatcaDateOfIssue
# MAGIC ,Null as FatcaComments
# MAGIC ,Null as IsEligibleForCrsAssessment
# MAGIC ,Null as CrsClassification
# MAGIC ,Null as CrsFormSignedDate
# MAGIC ,Null as CrsComments
# MAGIC ,Null as LegalForm
# MAGIC ,case when acc.status_code_no = 0 then 'Formar' when acc.status_code_no = 1 then 'Active' end as ClientLifeCycleStatus
# MAGIC ,Null as CaseStatusName
# MAGIC ,Null as IsLatestApprovedVersionOfClient
# MAGIC ,Null as FullLegalNameInLocalLanguage
# MAGIC ,Null as IsIncorporated
# MAGIC ,Null as IncorporationNumber
# MAGIC ,Null as IncorporationDate
# MAGIC ,Null as BusinessLineName
# MAGIC ,Null as HasSourceOfWealth
# MAGIC ,Null as SanctionsOrExternalWatchlist
# MAGIC ,Null as InternalWatchlist
# MAGIC ,Null as AdverseInformationOrMedia
# MAGIC ,Null as StatedFindings
# MAGIC ,Null as PEPStatus
# MAGIC ,Null as IsTrust
# MAGIC ,Null as TypeOfTrust
# MAGIC ,Null as IsClientRegulated
# MAGIC ,Null as RegulatorName
# MAGIC ,Null as RegulatorCountry
# MAGIC ,Null as HasRecognisedRegulator
# MAGIC ,Null as IsClientListed
# MAGIC ,Null as ExchangeName
# MAGIC ,Null as ExchangeCountry
# MAGIC ,Null as HasRecognisedExchange
# MAGIC ,Null as CountryOfTaxResidence
# MAGIC ,Null as TinAvailable
# MAGIC ,c.TIN as TinOrEquivalent
# MAGIC ,Null as TinUnavailabilityReason
# MAGIC ,Null as ExplanationForTinBeingUnavailable
# MAGIC ,Null as SourceOfIdentificationDocument
# MAGIC ,Null as IdentificationDocumentNumber
# MAGIC ,Null as SourceOfVerifiedDocument
# MAGIC ,Null as VerifiedDocumentNumber
# MAGIC ,Null as CddType
# MAGIC ,Null as Nationality
# MAGIC ,Null as NationalityIsoCode
# MAGIC ,Null as Citizenship
# MAGIC ,Null as CitizenshipIsoCode
# MAGIC ,Null as FIHubIndicator
# MAGIC ,Null as Bank_code
# MAGIC ,null as `W&RORRetail`
# MAGIC ,concat('NLSVF_',c.CIFNUMBER) as LocalSystemIdentifier
# MAGIC ,'NLSVF' as Application
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY c.CIFNUMBER ORDER BY acc.status_code_no DESC) AS ROWNUM
# MAGIC from nls_dbo_cif_detail d
# MAGIC inner join nls_dbo_cif c on d.CIFNO = c.CIFNO
# MAGIC left join nls_dbo_loanacct acc on c.CIFNO = acc.CIFNO
# MAGIC where c.MAIL_NAME1 is not null
# MAGIC */
# MAGIC )
# MAGIC select  distinct
# MAGIC t1.gcid
# MAGIC ,t1.GCOB_Identifier
# MAGIC --,t1.GCOB_NCINO_Identifier
# MAGIC ,t1.GIC_Identifier
# MAGIC ,t1.Legacy2_Identifier
# MAGIC ,t1.NLSVF_Identifier
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
# MAGIC ,t1.BusinessLineName
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
# MAGIC ,t1.Nationality
# MAGIC ,t1.NationalityIsoCode
# MAGIC ,t1.Citizenship
# MAGIC ,t1.CitizenshipIsoCode
# MAGIC ,Null as FIHubIndicator
# MAGIC ,t1.Bank_code
# MAGIC ,t1.`W&RORRetail`
# MAGIC ,t1.LocalSystemIdentifier
# MAGIC ,t1.Application
# MAGIC
# MAGIC from NonGCDS as t1 where t1.ROWNUM = 1

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

df_party = add_party_identifier(df_party, df_Party_SystemIdentifier)

# COMMAND ----------

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
# MAGIC GlobalClientOwnerLocation,
# MAGIC ROW_NUMBER() OVER (PARTITION BY PartyIdentifier ORDER BY
# MAGIC   CASE
# MAGIC   WHEN GlobalClientOwnerLocation IS NOT NULL AND Application = 'GCOB' THEN 1
# MAGIC   WHEN GlobalClientOwnerLocation IS NOT NULL AND Application = 'Legacy2' THEN 2
# MAGIC   WHEN GlobalClientOwnerLocation IS NOT NULL AND Application = 'GIC' THEN 3
# MAGIC   WHEN GlobalClientOwnerLocation IS NOT NULL AND Application = 'GCDS' THEN 4
# MAGIC   ELSE 10 END
# MAGIC ) AS rn_GlobalClientOwnerLocation,
# MAGIC Application
# MAGIC FROM Derived_Party_Identifier
# MAGIC )
# MAGIC
# MAGIC select * from ranked_data
# MAGIC --order by PartyIdentifier

# COMMAND ----------

# DBTITLE 1,Unique Party Per PartyIdentifier
# MAGIC %sql
# MAGIC Create or replace temporary view Party_Final As
# MAGIC select distinct
# MAGIC p.PartyIdentifier
# MAGIC ,max(gcid) as gcid 
# MAGIC ,max(GCOB_Identifier) as GCOB_Identifier
# MAGIC ,max(GIC_Identifier) as GIC_Identifier
# MAGIC ,max(Legacy2_Identifier) as Legacy2_Identifier
# MAGIC ,cast(max(NLSVF_Identifier) as string) as NLSVF_Identifier
# MAGIC ,MAX(CASE WHEN FullLegalName IS NOT NULL THEN FullLegalName END) AS FullLegalName
# MAGIC ,MAX(CASE WHEN DateOfBirth IS NOT NULL THEN DateOfBirth END) AS DateOfBirth
# MAGIC ,MAX(CASE WHEN FirstName IS NOT NULL THEN FirstName END) AS FirstName
# MAGIC ,MAX(CASE WHEN MiddleName IS NOT NULL THEN MiddleName END) AS MiddleName
# MAGIC ,MAX(CASE WHEN LastName IS NOT NULL THEN LastName END) AS LastName
# MAGIC ,MAX(CASE WHEN Party_type IS NOT NULL THEN Party_type END) AS Party_type
# MAGIC --,MAX(CASE WHEN p.GlobalClientOwnerName IS NOT NULL THEN p.GlobalClientOwnerName END) AS GlobalClientOwnerName
# MAGIC ,MAX(CASE WHEN a.rn_GlobalClientOwnerName = 1 THEN a.GlobalClientOwnerName END) AS GlobalClientOwnerName
# MAGIC --,MAX(CASE WHEN p.GlobalClientOwnerLocation IS NOT NULL THEN p.GlobalClientOwnerLocation END) AS GlobalClientOwnerLocation
# MAGIC ,MAX(CASE WHEN a.rn_GlobalClientOwnerLocation = 1 THEN a.GlobalClientOwnerLocation END) AS GlobalClientOwnerLocation
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
# MAGIC ,MAX(CASE WHEN LifeCycleStatus IS NOT NULL THEN LifeCycleStatus END) AS LifeCycleStatus
# MAGIC ,MAX(CASE WHEN StatusName IS NOT NULL THEN StatusName END) AS StatusName
# MAGIC ,MAX(CASE WHEN IsLatestApprovedVersionOfClient IS NOT NULL THEN IsLatestApprovedVersionOfClient END) AS IsLatestApprovedVersionOfClient
# MAGIC ,MAX(CASE WHEN FullLegalNameInLocalLanguage IS NOT NULL THEN FullLegalNameInLocalLanguage END) AS FullLegalNameInLocalLanguage
# MAGIC ,MAX(CASE WHEN IsIncorporated IS NOT NULL THEN IsIncorporated END) AS IsIncorporated
# MAGIC ,MAX(CASE WHEN IncorporationNumber IS NOT NULL THEN IncorporationNumber END) AS IncorporationNumber
# MAGIC ,MAX(CASE WHEN IncorporationDate IS NOT NULL THEN IncorporationDate END) AS IncorporationDate
# MAGIC ,MAX(CASE WHEN BusinessLineName IS NOT NULL THEN BusinessLineName END) AS BusinessLineName
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
# MAGIC ,MAX(CASE WHEN Nationality IS NOT NULL THEN Nationality END) AS Nationality
# MAGIC ,MAX(CASE WHEN NationalityIsoCode IS NOT NULL THEN NationalityIsoCode END) AS NationalityIsoCode
# MAGIC ,MAX(CASE WHEN Citizenship IS NOT NULL THEN Citizenship END) AS Citizenship
# MAGIC ,MAX(CASE WHEN CitizenshipIsoCode IS NOT NULL THEN CitizenshipIsoCode END) AS CitizenshipIsoCode
# MAGIC ,MAX(CASE WHEN FIHubIndicator IS NOT NULL THEN FIHubIndicator END) AS FIHubIndicator
# MAGIC ,MAX(CASE WHEN Bank_code IS NOT NULL THEN Bank_code END) AS Bank_code
# MAGIC ,MAX(CASE WHEN `W&RORRetail` IS NOT NULL THEN `W&RORRetail` END) AS `W&RORRetail`
# MAGIC ,MAX(p.Application) as Application
# MAGIC from Derived_Party_Identifier p
# MAGIC left join SystemPrefAttribute a on p.PartyIdentifier = a.PartyIdentifier
# MAGIC where p.PartyIdentifier is not null
# MAGIC group by p.PartyIdentifier

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW GCDS_LeadGCID2 AS
# MAGIC
# MAGIC SELECT
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
# MAGIC     LEFT JOIN client_PartytoPartyRelationship R ON C.GCID = R.GCID -- AND RC.`Relationship-Value` IS NULL
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
# MAGIC     LEFT JOIN client_PartytoPartyRelationship R2 ON R2.GCID = C2.GCID -- AND RC2.`Relationship-Value` IS NULL
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
# MAGIC     LEFT JOIN client_PartytoPartyRelationship R3 ON R3.GCID = R2.`Relationship-Value` -- AND RC3.`Relationship-Value` IS NULL
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

# MAGIC %sql
# MAGIC create or replace temporary view AllParty_Final as
# MAGIC select pf.*, concat("GCDS_",gl.LegalEntity) as Party_LeadingPartyIdentifier  from Party_Final pf
# MAGIC left outer join GCDS_LeadGCID2 gl on gl.GCID=pf.GCID

# COMMAND ----------

# DBTITLE 1,Fianl DataFrame to write in storage
df_Party_Final=spark.table('AllParty_Final')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
save_to_saradar_storage_account(df_Party_Final, party_dataobject, radar_datamodel_version_number, environment)
