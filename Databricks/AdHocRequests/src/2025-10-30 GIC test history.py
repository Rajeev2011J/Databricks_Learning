# Databricks notebook source
# MAGIC %md 
# MAGIC ### GIC test connect. 
# MAGIC - GICto see if one day has good history of files.

# COMMAND ----------

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

from RadarUtils import *

# COMMAND ----------

authenticate_storage_account(GIC_ReadStorage)

# COMMAND ----------

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



# COMMAND ----------

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

with DTA_NASCIMENTO as 
(
  select max(SEQ_HISTORICO) as SEQ_HISTORICO,SEQ_PESSOA from PESSOA_FISICA where FLG_PENDENTE = 'False' group by SEQ_PESSOA
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pessoa 
# MAGIC where COD_Institucional = '205'
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(SEQ_PESSOA), COD_Institucional
# MAGIC  from pessoa 
# MAGIC  GROUP by COD_Institucional

# COMMAND ----------

Each PessOA Fisica has: multiple snapshots per person. 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from PESSOA_FISICA 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pessoa 
# MAGIC where SEQ_PESSOA = '3270'
# MAGIC limit 10

# COMMAND ----------


