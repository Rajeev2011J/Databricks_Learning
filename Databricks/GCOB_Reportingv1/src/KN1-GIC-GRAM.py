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

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

# COMMAND ----------

SALZReadStorage = 'salandingzonefecradarprd'

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

jdbcHostname = f'asafecreportprd.sql.azuresynapse.net'
jdbcPort = 1433
jdbcDatabase = "reportingdwhprd"

jdbcTestTables =["contraparts_sheet1" , "contraparts_Version2"]
Service_Principal_Id = f'{app_reg_app_id}'
Service_Principal_Secret = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
connectionProperties = {
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication" : "ActiveDirectoryServicePrincipal",
    "UserName" : Service_Principal_Id ,
    "Password" : Service_Principal_Secret
 }

for temptableName in jdbcTestTables:
    spark.read.jdbc(url=jdbcUrl,table="test."+temptableName,properties = connectionProperties).createOrReplaceTempView(temptableName)


# COMMAND ----------

temp='20241129'

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Party_RiskModelInstanceQuestionAnswers'
, 'Party_RiskModelCategories'
, 'Party_RiskModelInstance'
, 'party_WRCDDModel'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'adkyc_contrapartes'
,'adkyc_contrapartes_compl'
,'adrbb_contrapartes_questoes_respostas'
,'adkyc_situacoes'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://kn1-gic@{SALZReadStorage}.dfs.core.windows.net/brazil/kn1/{row.GDPname}/*{temp}.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

load_df =pd.DataFrame({'GDPname':['contraparts_sheet1'
                                  ,'contraparts_Version2']})
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://kn1-gic@{SALZReadStorage}.dfs.core.windows.net/brazil/*{row.GDPname}.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'vwgic_rdl_pessoa_tipo_cadastro'
,'vwgic_rdl_pessoa'
,'vwgic_rdl_pessoa_enderecos'
,'cad_municipio'
,'cad_pais'
,'cad_uf'
,'linha_negocio'
,'naics'
,'status_tipo_cadastro'
,'tipo_categoria_cliente'
,'tipo_sociedade'
,'vwgic_rdl_cliente_documentacao_requerida'
,'vwgic_rdl_pessoa_beneficiarios'
,'vwgic_rdl_pessoa_cidadania'
,'vwgic_rdl_pessoa_fisica_identificacao'
,'vwgic_rdl_pessoa_juridica_identificacao'
,'vwgic_rdl_pessoa_linha_negocio'
,'vwgic_rdl_pessoa_tipo_cadastro_dd'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://kn1-gic@{SALZReadStorage}.dfs.core.windows.net/brazil/gic/{row.GDPname}/*{temp}*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

#Static Risk Level
Risk_list = [0,1,2,3,4,5,6,7]
Risk_Description = ['Undefined', 'No Risk', 'Low Risk', 'Medium Risk', 'High Risk','Super High Risk','Extra High Risk','Unacceptable Risk']
# create pyspark dataframe from lists
df_gcob_static_RiskLevel = spark.createDataFrame(zip(Risk_list, Risk_Description), ['Id', 'Description'])
df_gcob_static_RiskLevel.createOrReplaceTempView('static_RiskLevel')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW KN1_Cases AS
# MAGIC select distinct
# MAGIC kn_cont.ID_CONTRAPARTE as CaseId_KN1
# MAGIC ,gic.SEQ_PESSOA as  GIC_Id
# MAGIC ,gic.COD_INSTITUCIONAL as LocalSystemId
# MAGIC ,gic.NOM_COMPLETO as GIC_Clientname
# MAGIC ,gram.QuestionId
# MAGIC ,gram.QuestionText
# MAGIC ,gram.AnswerValue
# MAGIC ,gram.AnswerText
# MAGIC ,gram.QuestionCode
# MAGIC ,kn_cont.DTHR_FIM
# MAGIC ,gram.InstanceId
# MAGIC --,gram.SourceSystemName
# MAGIC from adkyc_contrapartes kn_cont
# MAGIC left join contraparts_Version2 stat on kn_cont.ID_CONTRAPARTE = stat.ID_CONTRAPARTE
# MAGIC LEFT join adkyc_contrapartes_compl kncompl on kn_cont.ID_CONTRAPARTE = kncompl.ID_CONTRAPARTE
# MAGIC left join vwgic_rdl_pessoa gic on kncompl.CD_CONTRAPARTE = gic.COD_INSTITUCIONAL
# MAGIC left join Party_RiskModelInstanceQuestionAnswers gram on stat.ID_GRAM_IDENTITY = gram.sourcesystemreferenceid 
# MAGIC and SourceSystemName = 'Brazil - KN1'
# MAGIC where YEAR(kn_cont.DTHR_FIM) = 2024

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GIC_Cases AS
# MAGIC select distinct 
# MAGIC gic.SEQ_PESSOA as  GIC_Id
# MAGIC ,gic.COD_INSTITUCIONAL as LocalSystemId
# MAGIC ,gic.NOM_COMPLETO as GIC_Clientname
# MAGIC ,gicrt.DES_TIPO_CADASTRO as RegisteredType
# MAGIC ,gicrt.DES_STATUS_TIPO_CADASTRO as StatusType
# MAGIC ,gicrt.DTA_CADASTRO_PESSOA as RegisteredDate
# MAGIC ,gicrt.DTA_CADASTRO as RecordedDate
# MAGIC ,BRCountryList.NOM_PAIS as CountryOfRegistration
# MAGIC ,BRCountryList.COD_COUNTRY_RISK_MTGT as CountryCodeOfRegistration
# MAGIC ,gic.DES_TIPO_RISCO_DD as RiskType
# MAGIC ,gicc.SEQ_ENDERECO as AddressId
# MAGIC ,gicc.DES_TIPO_ENDERECO as AddressType
# MAGIC ,gicc.DES_MUNICIPIO as Municipality
# MAGIC ,gicc.UF as State
# MAGIC ,gicc.NOM_PAIS as Country
# MAGIC ,gicc.NUM_CEP as PostalCode
# MAGIC
# MAGIC from vwgic_rdl_pessoa gic
# MAGIC inner join vwgic_rdl_pessoa_tipo_cadastro gicrt on gic.SEQ_PESSOA = gicrt.SEQ_PESSOA
# MAGIC left join vwgic_rdl_pessoa_enderecos gicc on gic.SEQ_PESSOA = gicc.SEQ_PESSOA
# MAGIC --LEFT JOIN (SELECT * FROM vwgic_rdl_pessoa_enderecos
# MAGIC --WHERE FLG_CORRESPONDENCIA = 'Sim') gicc on gic.SEQ_PESSOA = gicc.SEQ_PESSOA
# MAGIC LEFT JOIN cad_pais BRCountryList ON gicc.SEQ_PAIS = BRCountryList.SEQ_PAIS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW KN1_GIC_Cases AS
# MAGIC SELECT distinct 
# MAGIC kn1.GIC_Id
# MAGIC ,kn1.CaseId_KN1
# MAGIC ,kn1.LocalSystemId
# MAGIC ,kn1.GIC_Clientname
# MAGIC ,kn1.QuestionId
# MAGIC ,kn1.QuestionText
# MAGIC ,kn1.AnswerValue
# MAGIC ,kn1.AnswerText
# MAGIC ,kn1.QuestionCode
# MAGIC ,kn1.DTHR_FIM
# MAGIC ,kn1.InstanceId
# MAGIC ,gic.RegisteredType
# MAGIC ,gic.StatusType
# MAGIC ,gic.RegisteredDate
# MAGIC ,gic.RecordedDate
# MAGIC ,gic.CountryOfRegistration
# MAGIC ,gic.CountryCodeOfRegistration
# MAGIC ,gic.RiskType
# MAGIC ,gic.AddressId
# MAGIC ,gic.AddressType
# MAGIC ,gic.Municipality
# MAGIC ,gic.State
# MAGIC ,gic.Country
# MAGIC ,gic.PostalCode
# MAGIC
# MAGIC  from GIC_Cases gic
# MAGIC  join KN1_Cases kn1 on gic.GIC_Id= kn1.GIC_Id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RiskModelCategories AS
# MAGIC with InstanceCategory as (
# MAGIC      select InstanceId 
# MAGIC     ,Geographical AS GeoRisk
# MAGIC     ,`Entity Type` AS EntityTypeRisk
# MAGIC     ,Structure AS StructureRisk
# MAGIC     ,Sector AS SectorRisk
# MAGIC     ,`Products and Services` AS ProductRisk
# MAGIC     ,PEP AS PEPRisk
# MAGIC     ,`Transaction` AS TransactionRisk
# MAGIC     ,`Distribution Channel` AS DistributionRisk
# MAGIC     ,`Third Party` AS ThirdPartyRisk
# MAGIC     ,`Adverse Info` AS AdverseInfoRisk
# MAGIC     ,Other AS OtherRisk
# MAGIC
# MAGIC     FROM (
# MAGIC     select InstanceId
# MAGIC     , MAX(CASE WHEN CategoryName = 'Adverse Info' then CalculatedRiskLevelId else NULL END) as `Adverse Info`
# MAGIC     , MAX(CASE WHEN CategoryName = 'Distribution Channel' then CalculatedRiskLevelId else NULL END) as `Distribution Channel`
# MAGIC     , MAX(CASE WHEN CategoryName = 'Entity Type' then CalculatedRiskLevelId else NULL END) as `Entity Type`
# MAGIC     , MAX(CASE WHEN CategoryName = 'General' then CalculatedRiskLevelId else NULL END) as General
# MAGIC     , MAX(CASE WHEN CategoryName = 'Geographical' then CalculatedRiskLevelId else NULL END) as Geographical
# MAGIC     , MAX(CASE WHEN CategoryName = 'Other' then CalculatedRiskLevelId else NULL END) as Other
# MAGIC     , MAX(CASE WHEN CategoryName = 'PEP' then CalculatedRiskLevelId else NULL END) as PEP
# MAGIC     , MAX(CASE WHEN CategoryName = 'Products and Services' then CalculatedRiskLevelId else NULL END) as `Products and Services`
# MAGIC     , MAX(CASE WHEN CategoryName = 'Sector' then CalculatedRiskLevelId else NULL END) as Sector
# MAGIC     , MAX(CASE WHEN CategoryName = 'Structure' then CalculatedRiskLevelId else NULL END) as Structure
# MAGIC     , MAX(CASE WHEN CategoryName = 'Third Party' then CalculatedRiskLevelId else NULL END) as `Third Party`
# MAGIC     , MAX(CASE WHEN CategoryName = 'Transaction' then CalculatedRiskLevelId else NULL END) as `Transaction`
# MAGIC
# MAGIC     FROM
# MAGIC         Party_RiskModelCategories
# MAGIC     GROUP BY
# MAGIC         InstanceId
# MAGIC     )
# MAGIC     )
# MAGIC select distinct
# MAGIC geo.Description as GeographicalRiskLevel
# MAGIC , Ent.Description as EntityTypeRiskLevel
# MAGIC , stru.Description as StructureRiskLevel
# MAGIC , sec.Description as SectorRiskLevel
# MAGIC , prod.Description as ProductAndServiceRiskLevel
# MAGIC , pep.Description as PEPRiskLevel
# MAGIC , tran.Description as TransactionRiskLevel
# MAGIC , dist.Description as DistributionRiskLevel
# MAGIC , thir.Description as ThirdPartyRiskLevel
# MAGIC , adv.Description as AdverseInfoRiskLevel
# MAGIC ,InstanceId
# MAGIC FROM 
# MAGIC InstanceCategory AS rf
# MAGIC LEFT OUTER JOIN static_RiskLevel geo ON rf.GeoRisk = geo.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel Ent ON rf.EntityTypeRisk = Ent.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel stru ON rf.StructureRisk = stru.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel sec ON rf.SectorRisk = sec.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel prod ON rf.ProductRisk = prod.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel pep ON rf.PEPRisk = pep.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel tran ON rf.TransactionRisk = tran.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel dist ON rf.DistributionRisk = dist.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel thir ON rf.ThirdPartyRisk = thir.Id
# MAGIC LEFT OUTER JOIN static_RiskLevel adv ON rf.AdverseInfoRisk = adv.Id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GIC_RiskCategories AS
# MAGIC select distinct
# MAGIC kg.GIC_Id
# MAGIC ,kg.CaseId_KN1
# MAGIC ,kg.LocalSystemId
# MAGIC ,kg.GIC_Clientname
# MAGIC ,kg.QuestionId
# MAGIC ,kg.QuestionText
# MAGIC ,kg.AnswerValue
# MAGIC ,kg.AnswerText
# MAGIC ,kg.QuestionCode
# MAGIC ,kg.DTHR_FIM
# MAGIC --,kn1.InstanceId
# MAGIC ,kg.RegisteredType
# MAGIC ,kg.StatusType
# MAGIC ,kg.RegisteredDate
# MAGIC ,kg.RecordedDate
# MAGIC ,kg.CountryOfRegistration
# MAGIC ,kg.CountryCodeOfRegistration
# MAGIC ,kg.RiskType
# MAGIC ,kg.AddressId
# MAGIC ,kg.AddressType
# MAGIC ,kg.Municipality
# MAGIC ,kg.State
# MAGIC ,kg.Country
# MAGIC ,kg.PostalCode
# MAGIC
# MAGIC ,risk.*
# MAGIC
# MAGIC From KN1_GIC_Cases Kg
# MAGIC left join RiskModelCategories risk on Kg.InstanceId = risk.InstanceId

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GIC_RiskCategories 

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from adkyc_contrapartes where ID_CONTRAPARTE = 35905 limit 2
# MAGIC --select * from contraparts_Version2 limit 2
# MAGIC --select * from Party_RiskModelInstanceQuestionAnswers where SourceSystemName = 'Brazil - KN1' and sourcesystemreferenceid = 35905 limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from GIC_RDL_PESSOA_FISICA_IDENTIFICACAO limit 2

# COMMAND ----------

'''
865 : brazil/kn1/adrbb_contrapartes_questoes_respostas
866 : brazil/kn1/adrbb_questoes
863 : brazil/kn1/adrbb_cob_naics
864	: brazil/kn1/adrbb_contrapartes_pontuacoes_abas
'''

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE SCHEMA IF NOT EXISTS radar

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table IF EXISTS radar.ProdusctAndService

# COMMAND ----------

#spark.sql('select * from ProdusctAndService').write.mode('overwrite').saveAsTable('radar.ProdusctAndService')
