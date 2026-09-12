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

LEC_RANZ_dataobject = 'LEC_RANZ'
LEC_RLERANZ_dataobject = 'LEC_RLERANZ'
LEC_RNPRANZ_dataobject = 'LEC_RNPRANZ'

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details',
'party_client_structure_GUI',
'party_products_and_services',
'party_AllPartyDetails',
'party_trade_name',
'party_Alias'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# MAGIC %md
# MAGIC Required Dataframes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fetch the legal entity RANZ clients
# MAGIC CREATE OR REPLACE TEMPORARY VIEW LEC_Clients AS
# MAGIC SELECT DISTINCT pc.SourceClient, pc.Gcobid, pc.FullLegalName, pc.ClientLifeCycleName, pc.CountryofRegistration, pc.ClientType
# MAGIC FROM party_case_client_details AS pc
# MAGIC INNER JOIN party_products_and_services AS ps 
# MAGIC   ON pc.SourceClient = ps.SourceClient
# MAGIC     AND pc.ClientType = 'Legal Entity'
# MAGIC     AND pc.IsLatestApprovedVersionOfClient = 'True'
# MAGIC     AND pc.ClientId = ps.ClientId
# MAGIC WHERE pc.globalclientownerlocation IN ('Rabobank Australia','Rabobank New Zealand') 
# MAGIC   OR ps.ProductOfferingLocation IN ('Rabobank Australia','Rabobank New Zealand') 
# MAGIC   OR ps.BookingEntityLocation IN ('Rabobank Australia','Rabobank New Zealand')

# COMMAND ----------

# MAGIC %md
# MAGIC ### LEC_RANZ

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW LEC_TradeName AS
# MAGIC SELECT PartyId
# MAGIC     , Id
# MAGIC     , ResultType
# MAGIC     , TradeName_1
# MAGIC     , TradeName_2
# MAGIC     , TradeName_3
# MAGIC     , TradeName_4
# MAGIC     , TradeName_5 
# MAGIC FROM (
# MAGIC         SELECT DISTINCT
# MAGIC             TradeName.PartyId
# MAGIC             ,TradeName.Id
# MAGIC             ,TradeName.ResultType
# MAGIC             ,TradeName.ClientTradeName
# MAGIC             ,Row_Number() over (PARTITION BY TradeName.PartyId,TradeName.Id,TradeName.ResultType
# MAGIC                                 ORDER BY TradeName.ClientTradeName
# MAGIC                                 ) as RowN
# MAGIC         FROM party_trade_name AS TradeName
# MAGIC         WHERE TradeName.ResultType = 'LegalEntityClient'
# MAGIC )
# MAGIC PIVOT(
# MAGIC     max(ClientTradeName) for RowN in (1 TradeName_1,2 TradeName_2,3 TradeName_3,4 TradeName_4, 5 TradeName_5)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW LEC_RANZ AS
# MAGIC Select distinct  
# MAGIC   gui.SourceClient
# MAGIC   , gui.ClientGcobid AS GCOBID
# MAGIC   , gui.ClientFullLegalName as FullLegalName
# MAGIC   , gui.ClientCountryofRegistration AS CountryOfRegistrationofLEC
# MAGIC   , gui.ClientLifeCycleName AS ClientLifeCycleStatus
# MAGIC   , apd.FullLegalNameInLocalLanguage AS FullLegalNameinclientlocallanguageofLEC
# MAGIC   , trade.Tradename_1 AS Tradename1ofLEC
# MAGIC   , trade.Tradename_2 AS Tradename2ofLEC
# MAGIC   , trade.Tradename_3 AS Tradename3ofLEC
# MAGIC   , trade.Tradename_4 AS Tradename4ofLEC
# MAGIC   , trade.Tradename_5 AS Tradename5ofLEC 
# MAGIC from LEC_Clients lec
# MAGIC left outer join party_client_structure_GUI gui on gui.SourceClient=lec.SourceClient
# MAGIC left  join party_AllPartyDetails apd on gui.ClientId=apd.PartyId and apd.Status='Snapshot' and apd.UniquePartyId=gui.SourceClient
# MAGIC LEFT OUTER JOIN (
# MAGIC         SELECT PartyId, Id, ResultType, Tradename_1, Tradename_2, Tradename_3, Tradename_4, Tradename_5
# MAGIC         FROM LEC_TradeName
# MAGIC       ) trade
# MAGIC ON apd.PartyId = trade.PartyId
# MAGIC
# MAGIC where gui.SourceClient like 'LEC_%'
# MAGIC   -- AND gui.CaseStatusName = 'Completed'
# MAGIC   -- AND gui.IsLatestApprovedVersionOfClient = true

# COMMAND ----------

df_LEC_RANZ = spark.table('LEC_RANZ')
save_to_saradar_storage_account(df_LEC_RANZ, LEC_RANZ_dataobject)

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS radar.LEC_RANZ;

# COMMAND ----------

# spark.sql('select distinct * from LEC_RANZ').write.mode('overwrite').saveAsTable('radar.LEC_RANZ')

# COMMAND ----------

# MAGIC %md
# MAGIC ###RLE_RANZ

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RLE_TradeName AS
# MAGIC SELECT PartyId
# MAGIC     , Id
# MAGIC     , ResultType
# MAGIC     , TradeName_1
# MAGIC     , TradeName_2
# MAGIC     , TradeName_3
# MAGIC     , TradeName_4
# MAGIC     , TradeName_5 
# MAGIC FROM (
# MAGIC         SELECT DISTINCT
# MAGIC             TradeName.PartyId
# MAGIC             ,TradeName.Id
# MAGIC             ,TradeName.ResultType
# MAGIC             ,TradeName.ClientTradeName
# MAGIC             ,Row_Number() over (PARTITION BY TradeName.PartyId,TradeName.Id,TradeName.ResultType
# MAGIC                                 ORDER BY TradeName.ClientTradeName
# MAGIC                                 ) as RowN
# MAGIC         FROM party_trade_name AS TradeName
# MAGIC         WHERE TradeName.ResultType = 'RelatedLegalEntity'
# MAGIC )
# MAGIC PIVOT(
# MAGIC     max(ClientTradeName) for RowN in (1 TradeName_1,2 TradeName_2,3 TradeName_3,4 TradeName_4, 5 TradeName_5)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or Replace temporary view RLE_RANZ as
# MAGIC SELECT distinct
# MAGIC   rel.SourceClient
# MAGIC   , rel.ClientGcobid AS GCOBID
# MAGIC   , rel.ClientFullLegalName AS FullLegalName
# MAGIC   , rel.ClientLifeCycleName AS ClientLifeCycleStatus
# MAGIC   , rel.ParentIdentity AS GCOBidRLE
# MAGIC   , allparty.RegisteredCountryName AS CountryOfRegistrationofRLE
# MAGIC   , rel.ParentIdentityName AS FullLegalNameOfRLE
# MAGIC   , allparty.FullLegalNameInLocalLanguage AS FullLegalNameinclientlocallanguageforRLE
# MAGIC   , allparty.LegalForm AS LegalFormofRLE
# MAGIC   , trade.TradeName_1 as Tradename1ofRLE
# MAGIC   , trade.TradeName_2 as Tradename2ofRLE
# MAGIC   , trade.TradeName_3 as Tradename3ofRLE
# MAGIC   , trade.TradeName_4 as Tradename4ofRLE
# MAGIC   , trade.TradeName_5 as Tradename5ofRLE
# MAGIC FROM (
# MAGIC         SELECT DISTINCT pcs.SourceClient ,pcs.ClientGcobid, pcs.ClientFullLegalName, pcs.ClientLifeCycleName, pcs.ParentIdentity, pcs.ParentIdentityName, pcs.Parententityid, pcs.UniqueParentPartyId
# MAGIC         FROM LEC_Clients as lec
# MAGIC         INNER JOIN party_client_structure_GUI AS pcs
# MAGIC           ON pcs.SourceClient = lec.SourceClient 
# MAGIC             AND pcs.ClientType = lec.ClientType
# MAGIC             AND pcs.ParentType = 'RelatedLegalEntity'
# MAGIC             AND pcs.IsLatestApprovedVersionOfClient = 'True'
# MAGIC       ) rel
# MAGIC left join party_AllPartyDetails allparty on rel.Parententityid=allparty.PartyId and allparty.Status='Snapshot' and 
# MAGIC allparty.UniquePartyId=rel.UniqueParentPartyId and allparty.clienttype = 'RelatedLegalEntity'
# MAGIC
# MAGIC LEFT OUTER JOIN (
# MAGIC         SELECT PartyId, Id, ResultType, Tradename_1, Tradename_2, Tradename_3, Tradename_4, Tradename_5
# MAGIC         FROM RLE_TradeName
# MAGIC       ) trade 
# MAGIC ON allparty.PartyId = trade.PartyId 
# MAGIC   AND allparty.Id = trade.Id 
# MAGIC   AND allparty.ClientType = trade.ResultType

# COMMAND ----------

df_RLE_RANZ = spark.table('RLE_RANZ')
save_to_saradar_storage_account(df_RLE_RANZ, LEC_RLERANZ_dataobject)

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS radar.LEC_RLERANZ;

# COMMAND ----------

# spark.sql('select distinct * from RLE_RANZ ').write.mode('overwrite').saveAsTable('radar.LEC_RLERANZ')

# COMMAND ----------

# MAGIC %md
# MAGIC ###RNP_RANZ

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW RNP_Alias AS
# MAGIC SELECT PartyId
# MAGIC     , Id
# MAGIC     , ResultType
# MAGIC     , Alias_1
# MAGIC     , Alias_2
# MAGIC     , Alias_3
# MAGIC     , Alias_4
# MAGIC     , Alias_5 
# MAGIC FROM (
# MAGIC         SELECT DISTINCT
# MAGIC             alias.PartyId
# MAGIC             , alias.Id
# MAGIC             , alias.ResultType
# MAGIC             , alias.Alias
# MAGIC             , ROW_NUMBER() OVER (PARTITION BY alias.PartyId,alias.Id,alias.ResultType 
# MAGIC                                 ORDER BY alias.Alias
# MAGIC                                 ) as RowN
# MAGIC         FROM party_Alias AS alias
# MAGIC         WHERE alias.ResultType = 'RelatedNaturalPerson'
# MAGIC )
# MAGIC PIVOT(
# MAGIC     MAX(Alias) FOR RowN IN (1 Alias_1,2 Alias_2,3 Alias_3,4 Alias_4, 5 Alias_5)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or Replace temporary view RNP_RANZ as
# MAGIC SELECT distinct
# MAGIC   rel.SourceClient
# MAGIC   , rel.ClientGcobid AS GCOBID
# MAGIC   , rel.ClientFullLegalName AS FullLegalName
# MAGIC   , rel.ClientLifeCycleName AS ClientLifeCycleStatus
# MAGIC   , rel.ParentIdentity AS GCOBidRNP
# MAGIC   , rel.ParentIdentityName AS FullLegalNameOfRNP
# MAGIC   , allparty.FullLegalNameInLocalLanguage AS FullLegalNameinclientlocallanguageforRNP
# MAGIC   , allparty.DateOfbirth as Dateofbirth
# MAGIC   , allparty.RegisteredCountryName AS Countryofresidence
# MAGIC   , alias.Alias_1 as Alias1
# MAGIC   , alias.Alias_2 as Alias2
# MAGIC   , alias.Alias_3 as Alias3
# MAGIC   , alias.Alias_4 as Alias4
# MAGIC   , alias.Alias_5 as Alias5
# MAGIC FROM (
# MAGIC         SELECT DISTINCT pcs.SourceClient, pcs.ClientGcobid, pcs.ClientFullLegalName, pcs.ClientLifeCycleName, pcs.ParentIdentity, pcs.ParentIdentityName, pcs.Parententityid,pcs.UniqueParentPartyId
# MAGIC         FROM LEC_Clients as lec
# MAGIC         INNER JOIN party_client_structure_GUI AS pcs
# MAGIC           ON pcs.SourceClient = lec.SourceClient 
# MAGIC             AND pcs.ClientType = lec.ClientType
# MAGIC             AND pcs.ParentType = 'RelatedNaturalPerson'
# MAGIC             AND pcs.IsLatestApprovedVersionOfClient = 'True'
# MAGIC       ) rel
# MAGIC left join party_AllPartyDetails allparty on rel.Parententityid=allparty.PartyId and allparty.Status='Snapshot' and 
# MAGIC allparty.UniquePartyId=rel.UniqueParentPartyId and allparty.clienttype = 'RelatedNaturalPerson'
# MAGIC LEFT OUTER JOIN (
# MAGIC         SELECT PartyId, Id, ResultType, Alias_1, Alias_2, Alias_3, Alias_4, Alias_5
# MAGIC         FROM RNP_Alias
# MAGIC       ) alias 
# MAGIC ON allparty.PartyId = alias.PartyId 
# MAGIC   AND allparty.Id = alias.Id 
# MAGIC   AND allparty.ClientType = alias.ResultType

# COMMAND ----------

df_RNP_RANZ = spark.table('RNP_RANZ')
save_to_saradar_storage_account(df_RNP_RANZ, LEC_RNPRANZ_dataobject)

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS radar.LEC_RNPRANZ;

# COMMAND ----------

# spark.sql('select distinct * from RNP_RANZ ').write.mode('overwrite').saveAsTable('radar.LEC_RNPRANZ')
