# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal
# MAGIC To answer RAF related questions on customer types
# MAGIC
# MAGIC - GR.17.01; [customer company legal form is:] Trust (anglo-saxon)
# MAGIC - GR.17.02;[customer company legal form is:] Foundation or other similar foreign legal form
# MAGIC - GR.17.05; [customer company legal form is:] Limited liability partnership (LLP) and/or Limited Partnership (LP)
# MAGIC - GR.17.06: [the customer structure has a] Nominee shareholder
# MAGIC - GR.17.07: [the company gives out] Bearer shares
# MAGIC
# MAGIC
# MAGIC ### Flow of logic
# MAGIC - Legacy 2
# MAGIC - GCOB

# COMMAND ----------

import os

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the files

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

#Derive the date for which data has to be processes
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
Today = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'
print (load_dts)
Load = 'EDL_LOAD_DTS='+ datetime.today().strftime('%Y%m%d')
print(Load)
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
EDL_LoadDate = (datetime.today()).strftime("%Y-%m-%dT%H:%m:%mZ")

load_dts = 'EDL_LOAD_DTS=20250101*'

# COMMAND ----------



# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
#'party_AllCasesReport'
    'party_case_client_details'
    , 'party_client'
   # , 'Party_AllParty_LocationCoverage'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{row.GDPname}/101/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
#'party_AllCasesReport'
    'Party_RiskModelInstanceQuestionAnswers'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/RISKMODEL/{row.GDPname}/101/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------



# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
    'Legacy2_GCOB_ApprovedVersion'
    , 'Legacy2_case_client_details'
    , 'Legacy2_risk'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/Legacy2/{row.GDPname}/101/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Brazil loads
# MAGIC
# MAGIC Input from Bruno": In Brazil, the Limited Liability Partnership (LLP) structure is not common. Instead, we have the Sociedade Limitada (LTDA), which is the closest form of an LLP. The LTDA offers limited liability protection to partners, like what an LLP offers in other countries.

# COMMAND ----------

import os
import pandas as pd
import datetime

# COMMAND ----------

# DBTITLE 1,Brazil
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

SALZReadStorage = 'salandingzonefecradarprd'


spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Connect to SA GDP
SAGDPStorage = 'edlcorestdbrprod0001'

spark.conf.set("fs.azure.account.auth.type."+SAGDPStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SAGDPStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SAGDPStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SAGDPStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SAGDPStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

dbutils.fs.ls(f'abfss://kn1@{SAGDPStorage}.dfs.core.windows.net/ADRBB_RESPOSTAS_ESCOLHIDAS/1/data/LOADED_DTS=20250407T000825Z')

# COMMAND ----------

ThisDay = (datetime.datetime.today() - datetime.timedelta(0)).strftime('%Y%m%d')
gic_load_dts = 'LOADED_DTS=' +ThisDay+ '*'

# COMMAND ----------

# DBTITLE 1,Read GIC files
load_df = pd.DataFrame({'GDPname':[
'workflow_cadastral_detalhe'
,'pessoa_tipo_cadastro_status'
,'pessoa_tipo_cadastro_risco_dd'
,'pessoa_tipo_cadastro_cliente_detalhe'
,'pessoa_regulatorio_global_tipo_regulatorio'
,'pessoa_regulatorio_global'
,'pessoa_perfil_investidor'
,'pessoa_linha_negocio'
,'pessoa_levantamento_patrimonial'
,'pessoa_email'
,'pessoa_cliente_lembrete_atualizacao' 
,'cliente_tipo_segmento_cliente'
,'cliente_tipo_categoria_cliente'
,'cliente_classificacao_risco'
,'cad_tipo_regulatorio'
,'avalista_cliente'
,'pessoa_complemento'
,'pessoa_agencia_rbb'
,'pessoa'
,'pessoa_duplo_check'
,'pessoa_fisica'
,'pessoa_juridica'
,'endereco'
,'pessoa_endereco'
,'tipo_endereco'
,'tipo_sociedade'
,'cad_municipio'
,'cad_pais'
,'status_tipo_cadastro'
]})


#gic_load_dts
# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gic@{SAGDPStorage}.dfs.core.windows.net/{row.GDPname}/1/data/{gic_load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

#gic_load_dts
# Create TempView for each loading table

spark.read.parquet(f'abfss://gic@{SAGDPStorage}.dfs.core.windows.net/tipo_sociedade/1/data/{gic_load_dts}/*.parquet').createOrReplaceTempView('tipo_sociedade')

# COMMAND ----------

# DBTITLE 1,sample pesso
# MAGIC %sql
# MAGIC select t1.*
# MAGIC  from pessoa t1
# MAGIC  
# MAGIC where nom_completo like '%Limitada%' or nom_completo like '%LTDA%'
# MAGIC -- limit 10
# MAGIC order by COD_INSTITUCIONAL

# COMMAND ----------

# DBTITLE 1,Juridisch type
# MAGIC %sql
# MAGIC select * from PESSOA_JURIDICA limit 10 null 
# MAGIC 191
# MAGIC 248

# COMMAND ----------

# DBTITLE 1,Company type
# MAGIC %sql
# MAGIC select * from tipo_sociedade

# COMMAND ----------

# DBTITLE 1,view for PESSOA unique
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW LATEST_PESSOA_JURIDICA AS
# MAGIC
# MAGIC SELECT t1.*
# MAGIC from PESSOA_JURIDICA t1
# MAGIC INNER JOIN (Select SEQ_PESSOA, MAX(SEQ_HISTORICO) AS SEQ_HISTORICO_MAX FROM PESSOA_JURIDICA GROUP BY SEQ_PESSOA ) as t3 on t1.SEQ_HISTORICO = t3.SEQ_HISTORICO_MAX AND t1.SEQ_PESSOA = t3.SEQ_PESSOA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW LATEST_pessoa_tipo_cadastro_status AS
# MAGIC
# MAGIC SELECT t1.*
# MAGIC from pessoa_tipo_cadastro_status t1
# MAGIC INNER JOIN (Select SEQ_PESSOA, MAX(SEQ_HISTORICO) AS SEQ_HISTORICO_MAX FROM pessoa_tipo_cadastro_status GROUP BY SEQ_PESSOA ) as t3 on t1.SEQ_HISTORICO = t3.SEQ_HISTORICO_MAX AND t1.SEQ_PESSOA = t3.SEQ_PESSOA

# COMMAND ----------

# DBTITLE 1,Select all LTDA clients
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW PESSOA_LP AS
# MAGIC
# MAGIC select 
# MAGIC
# MAGIC t1.COD_INSTITUCIONAL
# MAGIC , t1.NOM_COMPLETO
# MAGIC , t2.SEQ_PESSOA
# MAGIC , t2.SEQ_HISTORICO
# MAGIC , t2.SEQ_TIPO_SOCIEDADE 
# MAGIC , t3.DES_TIPO_SOCIEDADE
# MAGIC , t4.SEQ_STATUS_TIPO_CADASTRO
# MAGIC , t5.DES_STATUS_TIPO_CADASTRO
# MAGIC
# MAGIC from PESSOA t1
# MAGIC LEFT JOIN LATEST_PESSOA_JURIDICA t2 on t1.SEQ_PESSOA = t2.SEQ_PESSOA
# MAGIC LEFT JOIN tipo_sociedade t3 on t2.SEQ_TIPO_SOCIEDADE  = t3.SEQ_TIPO_SOCIEDADE 
# MAGIC LEFT JOIN LATEST_pessoa_tipo_cadastro_status t4 on t1.SEQ_PESSOA = t4.SEQ_PESSOA
# MAGIC LEFT JOIN status_tipo_cadastro t5 on t4.SEQ_STATUS_TIPO_CADASTRO = t5.SEQ_STATUS_TIPO_CADASTRO
# MAGIC
# MAGIC where  t2.SEQ_TIPO_SOCIEDADE IN (1, 27)
# MAGIC AND t4.SEQ_TIPO_CADASTRO = 1
# MAGIC and t4.SEQ_STATUS_TIPO_CADASTRO in (1,3) -- Active or blocked

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tipo_sociedade

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from PESSOA_LP

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(COD_INSTITUCIONAL)) from PESSOA_LP 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from PESSOA_LP order by COD_INSTITUCIONAL

# COMMAND ----------

# DBTITLE 1,pesso type?
# MAGIC %sql
# MAGIC select * from pessoa_complemento limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pessoa_tipo_cadastro_status limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pessoa_tipo_cadastro_cliente_detalhe limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(SEQ_TIPO_CADASTRO) from pessoa_tipo_cadastro_cliente_detalhe

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Client status type
# MAGIC %sql
# MAGIC select * from status_tipo_cadastro

# COMMAND ----------

Customer_status_type ={'Ativo': 'Client',
          'Inativo': 'Former Client',
          'Bloqueado' : 'Blocked Client (Ring-fenced?)',
          'Pré Cadastro' : 'Prospect',
          'Reativação' : 'Re-Onboarding'}


# COMMAND ----------

# DBTITLE 1,Read KN1 files
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'ADRBB_RESPOSTAS_ESCOLHIDAS'
, 'ADRBB_QUESTOES'
, 'ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS'
, 'ADKYC_QUESTOES_COMPORT_CLIENTES'
, 'ADKYC_TIPOS_CONTRAPARTES'
, 'ADKYC_TP_CADASTRAIS'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://kn1@{SAGDPStorage}.dfs.core.windows.net/{row.GDPname}/1/data/LOADED_DTS=20250407*/*').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ADKYC_TIPOS_CONTRAPARTES

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from ADRBB_QUESTOES

# COMMAND ----------

# QuestionId 141 and QuestionId 294 seem to relate to trust question in the structure.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from ADRBB_QUESTOES
# MAGIC where DE_QUESTAO like '%Empresa%'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS limit 3

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ADRBB_RESPOSTAS_ESCOLHIDAS
# MAGIC where texto like '%caract%'
# MAGIC --WHERE ID_Questao = 2
# MAGIC --AND VALOR <> 33

# COMMAND ----------

df_ADRBB_RESPOSTAS_ESCOLHIDAS = spark.sql('select * from ADRBB_RESPOSTAS_ESCOLHIDAS')

# COMMAND ----------

# DBTITLE 1,Connect to DWH
jdbcHostname = f'asafecreportprd.sql.azuresynapse.net'
jdbcPort = 1433
jdbcDatabase = "reportingdwhprd"

jdbcTestTables =["ADRBB_RESPOSTAS_ESCOLHIDAS"]
Service_Principal_Id = f'{app_reg_app_id}'
Service_Principal_Secret = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
connectionProperties = {
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication" : "ActiveDirectoryServicePrincipal",
    "UserName" : Service_Principal_Id ,
    "Password" : Service_Principal_Secret
 }



# COMMAND ----------

# DBTITLE 1,attempt to read table first
spark.read.jdbc(url=jdbcUrl,table="KN1.ADKYC_TIPOS_CONTRAPARTES",properties = connectionProperties)

# COMMAND ----------

df_party_types = spark.sql('select * from ADKYC_TIPOS_CONTRAPARTES')

# COMMAND ----------

# DBTITLE 1,write kn1 party types
create_table = """
CREATE TABLE [KN1].[ADKYC_TIPOS_CONTRAPARTES]
(
	[ID_TIPO_CONTRAPARTE] [int] NULL,
	[DS_TIPO_CONTRAPARTE] [varchar](200) NULL,
	[FL_DESATIVADO] [int]  NULL,
	[EDL_LOAD_DTS] [datetime] NULL,
	[EDL_ACT_DTS] [datetime] NULL

)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
""" 

df_party_types.write.option("truncate",True).mode('append').jdbc(url=jdbcUrl,table="KN1.ADKYC_TIPOS_CONTRAPARTES",properties = connectionProperties)

# COMMAND ----------

df_party_types.schema

# COMMAND ----------

Service_Principal_Id

# COMMAND ----------

from pyspark.sql.functions import substring, length

# COMMAND ----------

# DBTITLE 1,Reshaping texto column to have save-able lenght for synapse
#limit lenght
df_ADRBB_RESPOSTAS_ESCOLHIDAS = df_ADRBB_RESPOSTAS_ESCOLHIDAS.withColumn("TEXTO", substring("TEXTO", 0,1900))

# also filter longer ones
df_ADRBB_RESPOSTAS_ESCOLHIDAS_filtered = df_ADRBB_RESPOSTAS_ESCOLHIDAS.filter((length(df_ADRBB_RESPOSTAS_ESCOLHIDAS["TEXTO"]) <= 255))

# COMMAND ----------

spark.read.jdbc(url=jdbcUrl,table="KN1.ADRBB_RESPOSTAS_ESCOLHIDAS",properties = connectionProperties)

# COMMAND ----------

# DBTITLE 1,Write KN1 into DWH

df_ADRBB_RESPOSTAS_ESCOLHIDAS_filtered.write.mode('append').jdbc(url=jdbcUrl,table="KN1.ADRBB_RESPOSTAS_ESCOLHIDAS",properties = connectionProperties)



# COMMAND ----------

display(df_ADRBB_RESPOSTAS_ESCOLHIDAS.limit(10))

# COMMAND ----------

df_ADRBB_RESPOSTAS_ESCOLHIDAS.schema

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,KN!-GRAM export for Bruno
# MAGIC %sql
# MAGIC -- Brazil export for Bruno.
# MAGIC -- To get all elements from KN1 calling GRAM where questionId in 540, 
# MAGIC --(QuestionId in (71,551,17,363,341) --bearer
# MAGIC -- OR QuestionId in (14, 361, 549)) --nominee shareholders
# MAGIC
# MAGIC Select * 
# MAGIC from Party_RiskModelInstanceQuestionAnswers
# MAGIC WHERE QuestionId in (71,551,17,363,341 ,14, 361, 549, 540)
# MAGIC AND SourceSystemName like '%KN1%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### GIC data from Synapse

# COMMAND ----------

jdbcHostname = f'asafecreportprd.sql.azuresynapse.net'
jdbcPort = 1433
jdbcDatabase = "reportingdwhprd"

jdbcTestTables =["ADRBB_RESPOSTAS_ESCOLHIDAS"]
Service_Principal_Id = f'{app_reg_app_id}'
Service_Principal_Secret = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
connectionProperties = {
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication" : "ActiveDirectoryServicePrincipal",
    "UserName" : Service_Principal_Id ,
    "Password" : Service_Principal_Secret
 }



# COMMAND ----------

# DBTITLE 1,Load from Synapse: GIC / KN1
## Loading from Barends view
## [GEN].[vw_DNB_GR17_Brazil_20241231]

df_DNB_GR17_Brazil_20241231 = spark.read.jdbc(url=jdbcUrl,table="GEN.vw_DNB_GR17_Brazil_20241231",properties = connectionProperties)

# COMMAND ----------

df_DNB_GR17_Brazil_20241231 = df_DNB_GR17_Brazil_20241231.toDF(*[c.replace(' ', '_').replace(',', '_').replace(';', '_').replace('{', '_').replace('}', '_').replace('(', '_').replace(')', '_').replace('\n', '_').replace('\t', '_').replace('=', '_') for c in df_DNB_GR17_Brazil_20241231.columns]); df_DNB_GR17_Brazil_20241231.write.saveAsTable("radar.DNB_GR17_Brazil_20241231")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.DNB_GR17_Brazil_20241231 limit 3

# COMMAND ----------

GR.17.02_IsStichting
0
0
0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 'GR.17.01_IsTrust' AS QuestionName, count(*) from  radar.DNB_GR17_Brazil_20241231 where `GR.17.01_IsTrust` = 1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select 'GR.17.02_IsStichting' AS QuestionName, count(*) from  radar.DNB_GR17_Brazil_20241231 where `GR.17.02_IsStichting` =1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select 'GR.17.05_LP_LLP' AS QuestionName, count(*) from  radar.DNB_GR17_Brazil_20241231 where `GR.17.05_isLLPandorLP`  =1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select 'GR.17.06_NomineeShareholders' AS QuestionName, count(*) from  radar.DNB_GR17_Brazil_20241231 where `GR.17.06_NomineeShareholders` = 1
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select 'GR.17.07_IsBearerShares' AS QuestionName, count(*) from  radar.DNB_GR17_Brazil_20241231 where `GR.17.07_IsBearerShares` =1
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


