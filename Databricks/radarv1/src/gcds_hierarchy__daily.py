# Databricks notebook source
# MAGIC %md
# MAGIC # GCDS hierarchy as per Gerben definition

# COMMAND ----------

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

import re

# load and create temp views of all gdp_tables below
gcds_tables = [
    'client_Client'
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

    spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data/LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcds_'+ item)

# COMMAND ----------

# DBTITLE 1,drop and create radar.gcds_hierarchy
spark.sql('DROP TABLE IF EXISTS radar.gcds_hierarchy')


spark.sql('''
    /*
    2025-05-21 code by Trouw, G (Gerben) 
    */

    SELECT DISTINCT
    CASE
        WHEN Com_child.GCID IS NULL OR Com_parent.GCID IS NULL THEN UltAndDown.UltimateParent
        ELSE Com_parent.GCID
    END SuperParent
    , UltAndDown.* 

    FROM (
    SELECT 
        cast(
        CASE
            WHEN L8P.`Relationship-Value` IS NOT NULL AND L8P.`Relationship-Type` = 'Credit' THEN L8P.`Relationship-Value`
            WHEN L8O.`Relationship-Value` IS NOT NULL AND L8O.`Relationship-Type` = 'Operational Hierarchy' THEN L8O.`Relationship-Value`
            WHEN L7P.`Relationship-Value` IS NOT NULL AND L7P.`Relationship-Type` = 'Credit' THEN L7P.`Relationship-Value`
            WHEN L7O.`Relationship-Value` IS NOT NULL AND L7O.`Relationship-Type` = 'Operational Hierarchy' THEN L7O.`Relationship-Value`
            WHEN L6P.`Relationship-Value` IS NOT NULL AND L6P.`Relationship-Type` = 'Credit' THEN L6P.`Relationship-Value`
            WHEN L6O.`Relationship-Value` IS NOT NULL AND L6O.`Relationship-Type` = 'Operational Hierarchy' THEN L6O.`Relationship-Value`
            WHEN L5P.`Relationship-Value` IS NOT NULL AND L5P.`Relationship-Type` = 'Credit' THEN L5P.`Relationship-Value`
            WHEN L5O.`Relationship-Value` IS NOT NULL AND L5O.`Relationship-Type` = 'Operational Hierarchy' THEN L5O.`Relationship-Value`
            WHEN L4P.`Relationship-Value` IS NOT NULL AND L4P.`Relationship-Type` = 'Credit' THEN L4P.`Relationship-Value`
            WHEN L4O.`Relationship-Value` IS NOT NULL AND L4O.`Relationship-Type` = 'Operational Hierarchy' THEN L4O.`Relationship-Value`
            WHEN L3P.`Relationship-Value` IS NOT NULL AND L3P.`Relationship-Type` = 'Credit' THEN L3P.`Relationship-Value`
            WHEN L3O.`Relationship-Value` IS NOT NULL AND L3O.`Relationship-Type` = 'Operational Hierarchy' THEN L3O.`Relationship-Value`
            WHEN L2P.`Relationship-Value` IS NOT NULL AND L2P.`Relationship-Type` = 'Credit' THEN L2P.`Relationship-Value`
            WHEN L2O.`Relationship-Value` IS NOT NULL AND L2O.`Relationship-Type` = 'Operational Hierarchy' THEN L2O.`Relationship-Value`
            WHEN L1P.`Relationship-Value` IS NOT NULL AND L1P.`Relationship-Type` = 'Credit' THEN L1P.`Relationship-Value`
            WHEN L1O.`Relationship-Value` IS NOT NULL AND L1O.`Relationship-Type` = 'Operational Hierarchy' THEN L1O.`Relationship-Value`
            ELSE Legal1.LegalEntity
        END AS INT
        ) AS UltimateParent

        , Legal1.LegalEntity
        , Legal1.`Relationship-Value` AS ParentGlobalClientID
        , Legal1.`Relationship-Type` AS RelationshipType
        , Legal1.GCID AS GCID
        , C.Full_legal_name
        , coalesce(L8P.`Relationship-Value`, L8O.`Relationship-Value`) AS L8P
        , coalesce(L8P.`Relationship-Type`, L8O.`Relationship-Type`  ) AS L8PType
        , coalesce(L7P.`Relationship-Value`, L7O.`Relationship-Value`) AS L7P
        , coalesce(L7P.`Relationship-Type`, L7O.`Relationship-Type`  ) AS L7PType
        , coalesce(L6P.`Relationship-Value`, L6O.`Relationship-Value`) AS L6P
        , coalesce(L6P.`Relationship-Type`, L6O.`Relationship-Type`  ) AS L6PType
        , coalesce(L5P.`Relationship-Value`, L5O.`Relationship-Value`) AS L5P
        , coalesce(L5P.`Relationship-Type`, L5O.`Relationship-Type`  ) AS L5PType
        , coalesce(L4P.`Relationship-Value`, L4O.`Relationship-Value`) AS L4P
        , coalesce(L4P.`Relationship-Type`, L4O.`Relationship-Type`  ) AS L4PType
        , coalesce(L3P.`Relationship-Value`, L3O.`Relationship-Value`) AS L3P
        , coalesce(L3P.`Relationship-Type`, L3O.`Relationship-Type`  ) AS L3PType
        , coalesce(L2P.`Relationship-Value`, L2O.`Relationship-Value`) AS L2P
        , coalesce(L2P.`Relationship-Type`, L2O.`Relationship-Type`  ) AS L2PType
        , coalesce(L1P.`Relationship-Value`, L1O.`Relationship-Value`) AS L1P
        , coalesce(L1P.`Relationship-Type`, L1O.`Relationship-Type`  ) AS L1PTyp


    FROM gcds_client_Client C
    LEFT JOIN ( 
        SELECT
        cast(
            CASE
            WHEN RC3.`Relationship-Value` IS NOT NULL AND C3.Party_type <> 'Legal Entity' THEN RC3.`Relationship-Value`
            WHEN C4.GCID IS NOT NULL THEN C4.GCID
            WHEN RC2.`Relationship-Value` IS NOT NULL AND C2.Party_type <> 'Legal Entity' THEN RC2.`Relationship-Value`
            WHEN C3.GCID IS NOT NULL THEN C3.GCID
            WHEN RC1.`Relationship-Value` IS NOT NULL AND C1.Party_type <> 'Legal Entity' THEN RC1.`Relationship-Value`
            WHEN C2.GCID IS NOT NULL THEN C2.GCID
            WHEN RC.`Relationship-Value` IS NOT NULL AND C.Party_type <> 'Legal Entity' THEN RC.`Relationship-Value`
            WHEN C1.GCID IS NULL THEN C.GCID
            ELSE R.`Relationship-Value`
            END AS INT
        ) LegalEntity
        , R.`Relationship-Value`
        , R.`Relationship-Type`
        , C.GCID

        FROM gcds_client_Client C                                                       
        LEFT JOIN gcds_client_PartytoPartyRelationship RC ON RC.GCID = C.GCID AND RC.`Relationship-Type` = 'Credit'
        LEFT JOIN gcds_client_PartytoPartyRelationship R ON C.GCID = R.GCID -- AND RC.`Relationship-Value` IS NULL
        AND (
            (C.Party_type IN ('Branch','Foreign Branch') AND R.`Relationship-Type` = 'Branch of')
            OR (C.Party_type = 'Sub Account' AND R.`Relationship-Type` = 'Main Account')
            OR (C.Party_type = 'Organisation Unit' AND R.`Relationship-Type` = 'Organisation Unit of')
            OR (C.Party_type = 'Managed Fund' AND R.`Relationship-Type` = 'Managed by')
            OR (C.Party_type = 'Sub-fund' AND R.`Relationship-Type` = 'Sub-fund of')
        )
        LEFT JOIN gcds_client_Client C1 ON C1.GCID = R.`Relationship-Value`
        AND ((C.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C1.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
        OR (C.Party_type IN ('Branch','Foreign Branch') AND C1.Party_type = 'Legal Entity'))
        LEFT JOIN gcds_client_PartytoPartyRelationship RC1 ON RC1.GCID = C1.GCID AND RC1.`Relationship-Type` = 'Credit'
        LEFT JOIN gcds_client_PartytoPartyRelationship R1 ON R1.GCID = C1.GCID -- AND RC1.`Relationship-Value` IS NULL
        AND R1.`Relationship-Value` <> R1.GCID
        AND (
            (C1.Party_type IN ('Branch','Foreign Branch') AND R1.`Relationship-Type` = 'Branch of')
            OR (C1.Party_type = 'Sub Account' AND R1.`Relationship-Type` = 'Main Account')
            OR (C1.Party_type = 'Organisation Unit' AND R1.`Relationship-Type` = 'Organisation Unit of')
            OR (C1.Party_type = 'Managed Fund' AND R1.`Relationship-Type` = 'Managed by')
            OR (C1.Party_type = 'Sub-fund' AND R1.`Relationship-Type` = 'Sub-fund of')
        )
        LEFT JOIN gcds_client_Client C2 ON C2.GCID = R1.`Relationship-Value`
        AND ((C1.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C2.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
        OR (C1.Party_type IN ('Branch','Foreign Branch') AND C2.Party_type = 'Legal Entity')) 
        LEFT JOIN gcds_client_PartytoPartyRelationship RC2 ON RC2.GCID = C2.GCID AND RC2.`Relationship-Type` = 'Credit'
        LEFT JOIN gcds_client_PartytoPartyRelationship R2 ON R2.GCID = C2.GCID -- AND RC2.`Relationship-Value` IS NULL
        AND R2.`Relationship-Value` <> R2.GCID
        AND (
            (C2.Party_type IN ('Branch','Foreign Branch') AND R2.`Relationship-Type` = 'Branch of')
            OR (C2.Party_type = 'Sub Account' AND R2.`Relationship-Type` = 'Main Account')
            OR (C2.Party_type = 'Organisation Unit' AND R2.`Relationship-Type` = 'Organisation Unit of')
            OR (C2.Party_type = 'Managed Fund' AND R2.`Relationship-Type` = 'Managed by')
            OR (C2.Party_type = 'Sub-fund' AND R2.`Relationship-Type` = 'Sub-fund of')
        )
        LEFT JOIN gcds_client_Client C3 ON C3.GCID = R2.`Relationship-Value`
        AND ((C2.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C3.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
        OR ( C2.Party_type IN ('Branch','Foreign Branch') AND C3.Party_type = 'Legal Entity')) 
        LEFT JOIN gcds_client_PartytoPartyRelationship RC3 ON RC3.GCID = C3.GCID 
        AND RC3.`Relationship-Type` = 'Credit'
        LEFT JOIN gcds_client_PartytoPartyRelationship R3 ON R3.GCID = R2.`Relationship-Value` -- AND RC3.`Relationship-Value` IS NULL
        AND R3.`Relationship-Value` <> R3.GCID
        AND (
            (C3.Party_type IN ('Branch','Foreign Branch') AND R3.`Relationship-Type` = 'Branch of')
            OR (C3.Party_type = 'Sub Account' AND R3.`Relationship-Type` = 'Main Account')
            OR (C3.Party_type = 'Organisation Unit' AND R3.`Relationship-Type` = 'Organisation Unit of')
            OR (C3.Party_type = 'Managed Fund' AND R3.`Relationship-Type` = 'Managed by')
            OR (C3.Party_type = 'Sub-fund' AND R3.`Relationship-Type` = 'Sub-fund of')
        )
        LEFT JOIN gcds_client_Client C4 ON C4.GCID = R2.`Relationship-Value`
        AND ((C3.Party_type IN ('Sub Account','Organisation Unit','Managed Fund','Sub-fund') AND C4.Party_type IN ('Legal Entity','Branch','Foreign Branch'))
        OR (C3.Party_type IN ('Branch','Foreign Branch') AND C4.Party_type = 'Legal Entity')) 

    ) Legal1 ON Legal1.GCID = C.GCID

    LEFT JOIN gcds_client_Client LegalC1 ON LegalC1.GCID = Legal1.LegalEntity

    LEFT JOIN gcds_client_PartytoPartyRelationship L1P ON  L1P.GCID = Legal1.LegalEntity
        AND LegalC1.Party_type in ('Legal Entity','Natural Person')
        AND L1P.`Relationship-Type` = 'Credit'
        AND L1P.`Relationship-Value` <> L1P.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L1O ON  L1O.GCID = Legal1.LegalEntity
        AND LegalC1.Party_type in ('Legal Entity','Natural Person')
        AND L1O.`Relationship-Type` = 'Operational Hierarchy'
        AND L1O.`Relationship-Value` <> L1O.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L2P ON L2P.GCID = L1P.`Relationship-Value` 
        AND L2P.`Relationship-Type` = 'Credit'
        AND L2P.`Relationship-Value` <> L2P.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L2O ON L2O.GCID = L1O.`Relationship-Value`
        AND L2O.`Relationship-Type` = 'Operational Hierarchy'
        AND L2O.`Relationship-Value` <> L2O.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L3P ON L3P.GCID = L2P.`Relationship-Value`
        AND L3P.`Relationship-Type` = 'Credit'
        AND L3P.`Relationship-Value` <> L3P.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L3O ON L3O.GCID = L2O.`Relationship-Value` 
        AND L3O.`Relationship-Type` = 'Operational Hierarchy'
        AND L3O.`Relationship-Value` <> L3O.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L4P ON L4P.GCID = L3P.`Relationship-Value`
        AND L4P.`Relationship-Type` = 'Credit'
        AND L4P.`Relationship-Value` <> L4P.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L4O ON L4O.GCID = L3O.`Relationship-Value`
        AND L4O.`Relationship-Type` = 'Operational Hierarchy'
        AND L4O.`Relationship-Value` <> L4O.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L5P ON L5P.GCID = L4P.`Relationship-Value`
        AND L5P.`Relationship-Type` = 'Credit'
        AND L5P.`Relationship-Value` <> L5P.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L5O ON L5O.GCID = L4O.`Relationship-Value`
        AND L5O.`Relationship-Type` = 'Operational Hierarchy'
        AND L5O.`Relationship-Value` <> L5O.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L6P ON L6P.GCID = L5P.`Relationship-Value`
        AND L6P.`Relationship-Type` = 'Credit'
        AND L6P.`Relationship-Value` <> L6P.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L6O ON L6O.GCID = L5O.`Relationship-Value`
        AND L6O.`Relationship-Type` = 'Operational Hierarchy'
        AND L6O.`Relationship-Value` <> L6O.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L7P ON L7P.GCID = L6P.`Relationship-Value`
        AND L7P.`Relationship-Type` = 'Credit'
        AND L7P.`Relationship-Value` <> L7P.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L7O ON L7O.GCID = L6O.`Relationship-Value`
        AND L7O.`Relationship-Type` = 'Operational Hierarchy'
        AND L7O.`Relationship-Value` <> L7O.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L8P ON L8P.GCID = L7P.`Relationship-Value`
        AND L8P.`Relationship-Type` = 'Credit'
        AND L8P.`Relationship-Value` <> L8P.GCID

    LEFT JOIN gcds_client_PartytoPartyRelationship L8O ON L8O.GCID = L7O.`Relationship-Value`
        AND L8O.`Relationship-Type` = 'Operational Hierarchy'
        AND L8O.`Relationship-Value` <> L8O.GCID

    ) UltAndDown

    LEFT JOIN gcds_client_PartytoPartyRelationship Commercial ON Commercial.GCID = UltAndDown.UltimateParent AND Commercial.`Relationship-Type` = 'Commercial'
    LEFT JOIN gcds_client_Client Com_Parent ON Com_Parent.GCID = Commercial.`Relationship-Value` AND Com_Parent.Party_type in ('Legal Entity','Natural Person')
    LEFT JOIN gcds_client_Client Com_child ON Com_child.GCID = Commercial.GCID AND Com_child.Party_type in ('Legal Entity','Natural Person')

''').write.mode('overwrite').saveAsTable('radar.gcds_hierarchy')
