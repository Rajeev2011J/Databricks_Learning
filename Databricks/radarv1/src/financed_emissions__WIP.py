# Databricks notebook source
# DBTITLE 1,set up connections to gdp
import os

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

# DBTITLE 1,load gcds
import re

from pyspark.sql import SparkSession
# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
    , 'client_Client'
    , 'client_PartyRole'
    # , 'client_Products'
    # , 'client_OnboardedLocations'
    , 'client_PartytoPartyRelationship'
]

for item in gcds_tables:
  # get the most recent version available in gdp
  path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/'
  files = dbutils.fs.ls(path)
  version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
  
  # get the most recent file available in gdp
  path_file = f'{path}{version}/data/'
  files = dbutils.fs.ls(path_file)
  load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

  spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data/LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# DBTITLE 1,load financed_emissions
spark.read.parquet(f'abfss://calculusgdp@{ReadStorage}.dfs.core.windows.net/FEWholesale/1/data/FiscalYear=*/*.parquet').createOrReplaceTempView('financed_emissions_raw')

# COMMAND ----------

# MAGIC %md
# MAGIC # Sub-SuperParent hierarchy

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC   2025-05-21 code by Trouw, G (Gerben) 
# MAGIC */
# MAGIC CREATE OR REPLACE TEMP VIEW hierarchy AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   CASE
# MAGIC     WHEN Com_child.GCID IS NULL OR Com_parent.GCID IS NULL THEN UltAndDown.UltimateParent
# MAGIC     ELSE Com_parent.GCID
# MAGIC   END SuperParent
# MAGIC   , UltAndDown.* 
# MAGIC
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     cast(
# MAGIC       CASE
# MAGIC         WHEN L8P.`Relationship-Value` IS NOT NULL AND L8P.`Relationship-Type` = 'Credit' THEN L8P.`Relationship-Value`
# MAGIC         WHEN L8O.`Relationship-Value` IS NOT NULL AND L8O.`Relationship-Type` = 'Operational Hierarchy' THEN L8O.`Relationship-Value`
# MAGIC         WHEN L7P.`Relationship-Value` IS NOT NULL AND L7P.`Relationship-Type` = 'Credit' THEN L7P.`Relationship-Value`
# MAGIC         WHEN L7O.`Relationship-Value` IS NOT NULL AND L7O.`Relationship-Type` = 'Operational Hierarchy' THEN L7O.`Relationship-Value`
# MAGIC         WHEN L6P.`Relationship-Value` IS NOT NULL AND L6P.`Relationship-Type` = 'Credit' THEN L6P.`Relationship-Value`
# MAGIC         WHEN L6O.`Relationship-Value` IS NOT NULL AND L6O.`Relationship-Type` = 'Operational Hierarchy' THEN L6O.`Relationship-Value`
# MAGIC         WHEN L5P.`Relationship-Value` IS NOT NULL AND L5P.`Relationship-Type` = 'Credit' THEN L5P.`Relationship-Value`
# MAGIC         WHEN L5O.`Relationship-Value` IS NOT NULL AND L5O.`Relationship-Type` = 'Operational Hierarchy' THEN L5O.`Relationship-Value`
# MAGIC         WHEN L4P.`Relationship-Value` IS NOT NULL AND L4P.`Relationship-Type` = 'Credit' THEN L4P.`Relationship-Value`
# MAGIC         WHEN L4O.`Relationship-Value` IS NOT NULL AND L4O.`Relationship-Type` = 'Operational Hierarchy' THEN L4O.`Relationship-Value`
# MAGIC         WHEN L3P.`Relationship-Value` IS NOT NULL AND L3P.`Relationship-Type` = 'Credit' THEN L3P.`Relationship-Value`
# MAGIC         WHEN L3O.`Relationship-Value` IS NOT NULL AND L3O.`Relationship-Type` = 'Operational Hierarchy' THEN L3O.`Relationship-Value`
# MAGIC         WHEN L2P.`Relationship-Value` IS NOT NULL AND L2P.`Relationship-Type` = 'Credit' THEN L2P.`Relationship-Value`
# MAGIC         WHEN L2O.`Relationship-Value` IS NOT NULL AND L2O.`Relationship-Type` = 'Operational Hierarchy' THEN L2O.`Relationship-Value`
# MAGIC         WHEN L1P.`Relationship-Value` IS NOT NULL AND L1P.`Relationship-Type` = 'Credit' THEN L1P.`Relationship-Value`
# MAGIC         WHEN L1O.`Relationship-Value` IS NOT NULL AND L1O.`Relationship-Type` = 'Operational Hierarchy' THEN L1O.`Relationship-Value`
# MAGIC         ELSE Legal1.LegalEntity
# MAGIC       END AS INT
# MAGIC     ) AS UltimateParent
# MAGIC
# MAGIC     , Legal1.LegalEntity
# MAGIC     , Legal1.`Relationship-Value`	AS ParentGlobalClientID
# MAGIC     , Legal1.`Relationship-Type`	AS RelationshipType
# MAGIC     , Legal1.GCID AS GCID
# MAGIC     , C.Full_legal_name
# MAGIC     , coalesce(L8P.`Relationship-Value`, L8O.`Relationship-Value`) AS L8P
# MAGIC     , coalesce(L8P.`Relationship-Type`, L8O.`Relationship-Type`  ) AS L8PType
# MAGIC     , coalesce(L7P.`Relationship-Value`, L7O.`Relationship-Value`) AS L7P
# MAGIC     , coalesce(L7P.`Relationship-Type`, L7O.`Relationship-Type`  ) AS L7PType
# MAGIC     , coalesce(L6P.`Relationship-Value`, L6O.`Relationship-Value`) AS L6P
# MAGIC     , coalesce(L6P.`Relationship-Type`, L6O.`Relationship-Type`  ) AS L6PType
# MAGIC     , coalesce(L5P.`Relationship-Value`, L5O.`Relationship-Value`) AS L5P
# MAGIC     , coalesce(L5P.`Relationship-Type`, L5O.`Relationship-Type`  ) AS L5PType
# MAGIC     , coalesce(L4P.`Relationship-Value`, L4O.`Relationship-Value`) AS L4P
# MAGIC     , coalesce(L4P.`Relationship-Type`, L4O.`Relationship-Type`  ) AS L4PType
# MAGIC     , coalesce(L3P.`Relationship-Value`, L3O.`Relationship-Value`) AS L3P
# MAGIC     , coalesce(L3P.`Relationship-Type`, L3O.`Relationship-Type`  ) AS L3PType
# MAGIC     , coalesce(L2P.`Relationship-Value`, L2O.`Relationship-Value`) AS L2P
# MAGIC     , coalesce(L2P.`Relationship-Type`, L2O.`Relationship-Type`  ) AS L2PType
# MAGIC     , coalesce(L1P.`Relationship-Value`, L1O.`Relationship-Value`) AS L1P
# MAGIC     , coalesce(L1P.`Relationship-Type`, L1O.`Relationship-Type`  ) AS L1PTyp
# MAGIC
# MAGIC   
# MAGIC   FROM client_Client C
# MAGIC   LEFT JOIN ( 
# MAGIC     SELECT
# MAGIC       cast(
# MAGIC         CASE
# MAGIC           WHEN RC3.`Relationship-Value` IS NOT NULL AND C3.Party_type <> 'Legal Entity' THEN RC3.`Relationship-Value`
# MAGIC           WHEN C4.GCID IS NOT NULL THEN C4.GCID
# MAGIC           WHEN RC2.`Relationship-Value` IS NOT NULL AND C2.Party_type <> 'Legal Entity' THEN RC2.`Relationship-Value`
# MAGIC           WHEN C3.GCID IS NOT NULL THEN C3.GCID
# MAGIC           WHEN RC1.`Relationship-Value` IS NOT NULL AND C1.Party_type <> 'Legal Entity' THEN RC1.`Relationship-Value`
# MAGIC           WHEN C2.GCID IS NOT NULL THEN C2.GCID
# MAGIC 				  WHEN RC.`Relationship-Value` IS NOT NULL AND C.Party_type <> 'Legal Entity' THEN RC.`Relationship-Value`
# MAGIC   				WHEN C1.GCID IS NULL THEN C.GCID
# MAGIC   				ELSE R.`Relationship-Value`
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
# MAGIC   		)
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
# MAGIC   		)
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
# MAGIC                                                                        
# MAGIC   ) Legal1 ON Legal1.GCID = C.GCID
# MAGIC                                                 
# MAGIC   LEFT JOIN client_Client LegalC1 ON LegalC1.GCID = Legal1.LegalEntity
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L1P ON  L1P.GCID = Legal1.LegalEntity
# MAGIC     AND LegalC1.Party_type in ('Legal Entity','Natural Person')
# MAGIC     AND L1P.`Relationship-Type` = 'Credit'
# MAGIC     AND L1P.`Relationship-Value` <> L1P.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L1O ON  L1O.GCID = Legal1.LegalEntity
# MAGIC     AND LegalC1.Party_type in ('Legal Entity','Natural Person')
# MAGIC     AND L1O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L1O.`Relationship-Value` <> L1O.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L2P ON L2P.GCID = L1P.`Relationship-Value` 
# MAGIC     AND L2P.`Relationship-Type` = 'Credit'
# MAGIC     AND L2P.`Relationship-Value` <> L2P.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L2O ON L2O.GCID = L1O.`Relationship-Value`
# MAGIC     AND L2O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L2O.`Relationship-Value` <> L2O.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L3P ON L3P.GCID = L2P.`Relationship-Value`
# MAGIC     AND L3P.`Relationship-Type` = 'Credit'
# MAGIC     AND L3P.`Relationship-Value` <> L3P.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L3O ON L3O.GCID = L2O.`Relationship-Value` 
# MAGIC     AND L3O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L3O.`Relationship-Value` <> L3O.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L4P ON L4P.GCID = L3P.`Relationship-Value`
# MAGIC     AND L4P.`Relationship-Type` = 'Credit'
# MAGIC     AND L4P.`Relationship-Value` <> L4P.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L4O ON L4O.GCID = L3O.`Relationship-Value`
# MAGIC     AND L4O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L4O.`Relationship-Value` <> L4O.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L5P ON L5P.GCID = L4P.`Relationship-Value`
# MAGIC     AND L5P.`Relationship-Type` = 'Credit'
# MAGIC     AND L5P.`Relationship-Value` <> L5P.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L5O ON L5O.GCID = L4O.`Relationship-Value`
# MAGIC     AND L5O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L5O.`Relationship-Value` <> L5O.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L6P ON L6P.GCID = L5P.`Relationship-Value`
# MAGIC     AND L6P.`Relationship-Type` = 'Credit'
# MAGIC     AND L6P.`Relationship-Value` <> L6P.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L6O ON L6O.GCID = L5O.`Relationship-Value`
# MAGIC     AND L6O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L6O.`Relationship-Value` <> L6O.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L7P ON L7P.GCID = L6P.`Relationship-Value`
# MAGIC     AND L7P.`Relationship-Type` = 'Credit'
# MAGIC     AND L7P.`Relationship-Value` <> L7P.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L7O ON L7O.GCID = L6O.`Relationship-Value`
# MAGIC     AND L7O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L7O.`Relationship-Value` <> L7O.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L8P ON L8P.GCID = L7P.`Relationship-Value`
# MAGIC     AND L8P.`Relationship-Type` = 'Credit'
# MAGIC     AND L8P.`Relationship-Value` <> L8P.GCID
# MAGIC
# MAGIC   LEFT JOIN client_PartytoPartyRelationship L8O ON L8O.GCID = L7O.`Relationship-Value`
# MAGIC     AND L8O.`Relationship-Type` = 'Operational Hierarchy'
# MAGIC     AND L8O.`Relationship-Value` <> L8O.GCID
# MAGIC
# MAGIC ) UltAndDown
# MAGIC
# MAGIC LEFT JOIN client_PartytoPartyRelationship Commercial ON Commercial.GCID = UltAndDown.UltimateParent AND Commercial.`Relationship-Type` = 'Commercial'
# MAGIC LEFT JOIN client_Client Com_Parent ON Com_Parent.GCID = Commercial.`Relationship-Value` AND Com_Parent.Party_type in ('Legal Entity','Natural Person')
# MAGIC LEFT JOIN client_Client Com_child ON Com_child.GCID = Commercial.GCID AND Com_child.Party_type in ('Legal Entity','Natural Person')

# COMMAND ----------

# MAGIC %md
# MAGIC # Financed emissions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW financed_emissions AS
# MAGIC
# MAGIC WITH fe_cte AS (
# MAGIC
# MAGIC   SELECT
# MAGIC     t1.BusinessEffective_Datetimestamp
# MAGIC     , t1.Load_Datetimestamp
# MAGIC     , t1.FiscalYear
# MAGIC     , t1.Portfolio
# MAGIC     , t1.ClientId
# MAGIC
# MAGIC     , t3.SuperParent
# MAGIC     , t3.UltimateParent
# MAGIC
# MAGIC     -- convert numeric columns to STRING with comma AS decimal separato
# MAGIC     , regexp_replace(cast(t1.Scope1Emissions AS STRING), '\\.', ',') AS Scope1Emissions
# MAGIC     , regexp_replace(cast(t1.Scope2Emissions AS STRING), '\\.', ',') AS Scope2Emissions
# MAGIC     , regexp_replace(cast(t1.TotalEmissionsScope1_and_2 AS STRING), '\\.', ',') AS TotalEmissionsScope1_and_2
# MAGIC     , SourceScope1_and_2Emissions
# MAGIC     , regexp_replace(cast(t1.Scope3Emissions AS STRING), '\\.', ',') AS Scope3Emissions
# MAGIC     , SourceScope3Emissions
# MAGIC     , regexp_replace(cast(t1.RabobankFeScope1 AS STRING), '\\.', ',') AS RabobankFeScope1
# MAGIC     , regexp_replace(cast(t1.RabobankFeScope2 AS STRING), '\\.', ',') AS RabobankFeScope2
# MAGIC     , regexp_replace(cast(t1.RabobankFeScope1_and_2 AS STRING), '\\.', ',') AS RabobankFeScope1_and_2
# MAGIC     , regexp_replace(cast(t1.RabobankFeScope3 AS STRING), '\\.', ',') AS RabobankFeScope3
# MAGIC     , regexp_replace(cast(t1.IntensityScope1_and_2 AS STRING), '\\.', ',') AS IntensityScope1_and_2
# MAGIC     , regexp_replace(cast(t1.IntensityScope3 AS STRING), '\\.', ',') AS IntensityScope3
# MAGIC     , regexp_replace(cast(t1.DataQualityScoreScope1_and_2 AS STRING), '\\.', ',') AS DataQualityScoreScope1_and_2
# MAGIC     , regexp_replace(cast(t1.DataQualityScoreScope3 AS STRING), '\\.', ',') AS DataQualityScoreScope3
# MAGIC     , t1.RunVersion
# MAGIC     , t1.EDL_LOAD_DTS
# MAGIC     , t1.EDL_ACT_DTS
# MAGIC     -- , t4.Life_cycle_status
# MAGIC
# MAGIC   FROM financed_emissions_raw t1
# MAGIC   LEFT JOIN client_KeyStoreKey t2 ON t1.ClientId = t2.KeyStore_value AND t2.Status = 'Active' AND t2.keyStore_type = 'WWID' 
# MAGIC   LEFT JOIN hierarchy t3 ON t2.GCID = t3.GCID
# MAGIC   -- LEFT JOIN client_PartyRole t4 ON t2.GCID = t4.GCID
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   BusinessEffective_Datetimestamp
# MAGIC   , t1.Load_Datetimestamp
# MAGIC   , t1.FiscalYear
# MAGIC   , t1.Portfolio
# MAGIC   , t1.ClientId
# MAGIC
# MAGIC   , t2.KeyStore_value AS SuperParent
# MAGIC   -- , t3.KeyStore_value AS UltimateParent
# MAGIC
# MAGIC   , t1.Scope1Emissions
# MAGIC   , t1.Scope2Emissions
# MAGIC   , t1.TotalEmissionsScope1_and_2
# MAGIC   , t1.SourceScope1_and_2Emissions
# MAGIC   , t1.Scope3Emissions
# MAGIC   , t1.SourceScope3Emissions
# MAGIC   , t1.RabobankFeScope1
# MAGIC   , t1.RabobankFeScope2
# MAGIC   , t1.RabobankFeScope1_and_2
# MAGIC   , t1.RabobankFeScope3
# MAGIC   , t1.IntensityScope1_and_2
# MAGIC   , t1.IntensityScope3
# MAGIC   , t1.DataQualityScoreScope1_and_2
# MAGIC   , t1.DataQualityScoreScope3
# MAGIC   , t1.RunVersion
# MAGIC   , t1.EDL_LOAD_DTS
# MAGIC   , t1.EDL_ACT_DTS
# MAGIC
# MAGIC FROM fe_cte t1
# MAGIC LEFT JOIN client_KeyStoreKey t2 ON t1.SuperParent = t2.GCID AND t2.Status = 'Active' AND t2.keyStore_type = 'WWID'
# MAGIC -- LEFT JOIN client_KeyStoreKey t3 ON t1.UltimateParent = t3.GCID AND t3.Status = 'Active' AND t3.keyStore_type = 'WWID'

# COMMAND ----------

# DBTITLE 1,transforming to WWID
# MAGIC %sql
# MAGIC SELECT
# MAGIC   t2.KeyStore_value AS SuperParent
# MAGIC   , t3.KeyStore_value AS UltimateParent
# MAGIC   , t4.KeyStore_value AS LegalEntity
# MAGIC   , t5.KeyStore_value AS WWID
# MAGIC   , t1.GCID
# MAGIC   , t1.Full_legal_name
# MAGIC
# MAGIC   , t6.Full_legal_name AS SuperParentFullLegalName
# MAGIC   
# MAGIC FROM hierarchy t1
# MAGIC
# MAGIC LEFT JOIN client_KeyStoreKey t2 ON t1.SuperParent = t2.GCID AND t2.Status = 'Active' AND t2.keyStore_type = 'WWID'
# MAGIC LEFT JOIN client_KeyStoreKey t3 ON t1.UltimateParent = t3.GCID AND t3.Status = 'Active' AND t3.keyStore_type = 'WWID'
# MAGIC LEFT JOIN client_KeyStoreKey t4 ON t1.LegalEntity = t4.GCID AND t4.Status = 'Active' AND t4.keyStore_type = 'WWID'
# MAGIC LEFT JOIN client_KeyStoreKey t5 ON t1.GCID = t5.GCID AND t5.Status = 'Active' AND t5.keyStore_type = 'WWID'
# MAGIC
# MAGIC LEFT JOIN client_Client t6 ON t1.SuperParent = t6.GCID
