# Databricks notebook source
# MAGIC %md
# MAGIC ### Context
# MAGIC This notebook is to get relevant GIC/KN1 data from GDP SA and upload in onelab so that the right development can happen in there

# COMMAND ----------

# simple command to start as test
import pandas as pd
import os
from datetime import datetime, timedelta

# COMMAND ----------

SaveDate = (datetime.today()).strftime("%Y-%m-%dT%H:%m:%mZ")

# COMMAND ----------

SaveDate

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,Define write storage
#
WriteStorage = 'vasaasawrfecreportdev'
WriteContainer = 'databricks'
WritePath = 'BrazilOneLab'

# COMMAND ----------

# DBTITLE 1,Authenticating WriteStorage
spark.conf.set("fs.azure.account.auth.type."+WriteStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+WriteStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+WriteStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+WriteStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+WriteStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# DBTITLE 1,Authenticating ReadStorage
SAGDPStorage = 'edlcorestdbrprod0001'

spark.conf.set("fs.azure.account.auth.type."+SAGDPStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SAGDPStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SAGDPStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SAGDPStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SAGDPStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 


# COMMAND ----------

dbutils.fs.ls(f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### retreiving data

# COMMAND ----------

DateToday = datetime.now().strftime('%Y%m%d')

# COMMAND ----------

# DBTITLE 1,Function for renaming the file right
def rename_single_parquet(filepath, target_file_name):
    #get all files from filepath

    target_folder_files = dbutils.fs.ls(filepath)

    # get the parquet file (expecting only 1! make sure to coalesce or repartition 1 the dataframe before saving)
    current_file_name = [file.path for file in target_folder_files if file.path.endswith('.snappy.parquet')][0]

    # move the file towards the new folder with the new name
    dbutils.fs.mv(current_file_name,
               target_file_name)
    
    return 1
    


# COMMAND ----------

# DBTITLE 1,Load  KN1
# print list of strings for loading spark dfs from GDP
kn1_datepart = f'LOADED_DTS={DateToday}*'

load_df = pd.DataFrame({'GDPname':[
'ADRBB_RESPOSTAS_ESCOLHIDAS'
, 'ADRBB_QUESTOES'
, 'ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS'
, 'ADKYC_QUESTOES_COMPORT_CLIENTES'
, 'ADKYC_TIPOS_CONTRAPARTES'
, 'ADKYC_TP_CADASTRAIS'
, 'ADKYC_CONTRAPARTES_COMPL'
, 'ADKYC_CONTRAPARTES'
, 'ADRBB_COB_NAICS'
, 'ADRBB_CONTRAPARTES_GRAM'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    #spark.read.parquet(f'abfss://kn1@{SAGDPStorage}.dfs.core.windows.net/{row.GDPname}/1/data/{kn1_datepart}/*').createOrReplaceTempView(row.GDPname)

    # read from 1 storage account and store in another
    df = spark.read.parquet(f'abfss://kn1@{SAGDPStorage}.dfs.core.windows.net/{row.GDPname}/1/data/{kn1_datepart}/*')
    df.repartition(1).write.format('parquet').mode('overwrite').save(f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/SA/KN1/{row.GDPname}' )

    #now move the file so it has the filename
    rename_single_parquet(f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/SA/KN1/{row.GDPname}',
                          f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/SA/KN1/SingleFiles/{row.GDPname}.snappy.parquet')



# COMMAND ----------

dbutils.fs.ls(f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/SA/KN1/SingleFiles/')

# COMMAND ----------

YesterDate = (datetime.now()-timedelta(1)).strftime('%Y%m%d')

# COMMAND ----------

def rename_single_parquet(filepath, target_file_name):
    #get all files from filepath

    target_folder_files = dbutils.fs.ls(filepath)

    # get the parquet file (expecting only 1! make sure to coalesce or repartition 1 the dataframe before saving)
    current_file_name = [file.path for file in target_folder_files if file.path.endswith('.snappy.parquet')][0]

    # move the file towards the new folder with the new name
    dbutils.fs.mv(current_file_name,
               target_file_name)
    
    return 1

# COMMAND ----------

# DBTITLE 1,Load GIC
gic_load_dts = f'LOADED_DTS={YesterDate}*'

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'naics'
,'workflow_cadastral_detalhe'
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
,'cad_municipio'
,'cad_pais'
,'status_tipo_cadastro'
]})


# Create TempView for each loading table
for index, row in load_df.iterrows():
    #spark.read.parquet(f'abfss://gic@{SAGDPStorage}.dfs.core.windows.net/{row.GDPname}/1/data/{gic_load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

    # read from 1 storage account and store in another
    df = spark.read.parquet(f'abfss://gic@{SAGDPStorage}.dfs.core.windows.net/{row.GDPname}/1/data/{gic_load_dts}/*')
    df.repartition(1).write.format('parquet').mode('overwrite').save(f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/SA/GIC/{row.GDPname}' )

    #now move the file so it has the filename
    rename_single_parquet(f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/SA/GIC/{row.GDPname}',
                          f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/SA/GIC/SingleFiles/{row.GDPname}.snappy.parquet')

# COMMAND ----------

# DBTITLE 1,Check GIC singleFiles folder
dbutils.fs.ls(f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/SA/GIC/SingleFiles/')

# COMMAND ----------

#df.write.format('delta').save(f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/testsample/load_dts={SaveDate}/')

# COMMAND ----------



# COMMAND ----------

# GAPS on 05-16
gic_load_dts = f'LOADED_DTS={YesterDate}*'

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'tipo_sociedade'
]})


# Create TempView for each loading table
for index, row in load_df.iterrows():
    #spark.read.parquet(f'abfss://gic@{SAGDPStorage}.dfs.core.windows.net/{row.GDPname}/1/data/{gic_load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

    # read from 1 storage account and store in another
    df = spark.read.parquet(f'abfss://gic@{SAGDPStorage}.dfs.core.windows.net/{row.GDPname}/1/data/{gic_load_dts}/*')
    df.repartition(1).write.format('parquet').mode('overwrite').save(f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/SA/GIC/{row.GDPname}' )

    #now move the file so it has the filename
    rename_single_parquet(f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/SA/GIC/{row.GDPname}',
                          f'abfss://{WriteContainer}@{WriteStorage}.dfs.core.windows.net/SA/GIC/SingleFiles/{row.GDPname}.snappy.parquet')


# COMMAND ----------

display(df)

# COMMAND ----------


