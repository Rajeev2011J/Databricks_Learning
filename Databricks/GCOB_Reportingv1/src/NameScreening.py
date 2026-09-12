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

Name_Screening_dataobject = 'mi_name_screening'

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_AllPartyDetails',
'party_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Legacy2_Nationality_Citizenship',
'Legacy2_case_client_details',
'Legacy2_risk',
]


# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='Legacy2', Dataobject=Object,path_prefix='Legacy2')

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NameScreening_GCOB AS
# MAGIC select DISTINCT 
# MAGIC to_date(dateadd(day,-1,getdate()),'yyyy-MM-dd') as BusinessDate,
# MAGIC b.CaseId,
# MAGIC a.GcobId,
# MAGIC a.FullLegalName,
# MAGIC a.ClientType,
# MAGIC a.FirstName,
# MAGIC a.MiddleName,
# MAGIC a.LastName,
# MAGIC a.FullLegalNameInLocalLanguage as FullNameInLocalLanguage,
# MAGIC b.RiskModelName,
# MAGIC a.DateOfBirth,
# MAGIC a.Nationality as ClientNationality,
# MAGIC a.Citizenship as ClientCitizenship,
# MAGIC a.RegisteredStreet as ResidencialStreet,
# MAGIC a.RegisteredNumber as ResidencialNumber,
# MAGIC a.RegisteredPostalCode,
# MAGIC a.RegisteredCity,
# MAGIC a.RegisteredRegion,
# MAGIC a.RegisteredCountryName as RegisteredCountry,
# MAGIC b.ReviewTypeName,
# MAGIC b.CaseStatusName,
# MAGIC a.ClientLifeCyclestatus as ClientLifeCyclestatusType,
# MAGIC a.SourceSystem,
# MAGIC a.IsLatestApprovedVersionOfClient,
# MAGIC a.SanctionsOrExternalWatchList,
# MAGIC a.InternalWatchList,
# MAGIC a.AdverseInformationOrMedia,
# MAGIC a.StatedFindings as StateFindingsInCaseOfAnyHits,
# MAGIC b.SourceClient,
# MAGIC case when substr(b.SourceClient,0,3) = 'LEC' and b.SourceSystem = 'GCOB' then 'GCOB_LegalEntity'
# MAGIC      when substr(b.SourceClient,0,2) = 'NP' and b.SourceSystem = 'GCOB' then 'GCOB_NP-NPPC'
# MAGIC      when substr(b.SourceClient,0,6) = 'L2_LEC' and b.SourceSystem = 'Legacy2' then 'Legacy2_LegalEntity'
# MAGIC      when substr(b.SourceClient,0,5) = 'L2_NP' and b.SourceSystem = 'Legacy2' then 'Legacy2_NP-NPPC'
# MAGIC      end as SourceSystemReference
# MAGIC from party_AllPartyDetails a
# MAGIC inner join party_case_client_details b on  b.ClientID = a.Id  AND b.FullLegalName = a.FullLegalName and a.status='Live'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NonSFDCID_Legacy2_Client AS
# MAGIC select * from Legacy2_case_client_details where isclient = 1 and GcobCaseId is null and Value is null;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Legacy2_client AS
# MAGIC select * from Legacy2_case_client_details
# MAGIC where ClientId not in (select ClientId from NonSFDCID_Legacy2_Client)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NameScreening_Legacy2 AS
# MAGIC select DISTINCT 
# MAGIC to_date(dateadd(day,-1,getdate()),'yyyy-MM-dd') as BusinessDate,
# MAGIC null as CaseId,
# MAGIC a.GcobId,
# MAGIC a.FullLegalName,
# MAGIC a.ClientType,
# MAGIC a.ContactFirstName as FirstName,
# MAGIC a.ContactMiddleName as MiddleName,
# MAGIC a.ContactLastName as LastName,
# MAGIC a.FullLegalNameLocalLanguage as FullNameInLocalLanguage,
# MAGIC c.RiskModelName,
# MAGIC a.DateOfBirth,
# MAGIC b.ClientNationality,
# MAGIC b.ClientCitizenship,
# MAGIC a.RegisteredStreet as ResidencialStreet,
# MAGIC a.RegisteredAddressNumber as ResidencialNumber,
# MAGIC a.RegisteredPostcode as RegisteredPostalCode,
# MAGIC a.RegisteredCity,
# MAGIC a.RegisteredRegion,
# MAGIC a.RegisteredCountry as RegisteredCountry,
# MAGIC a.ReviewTypeName,
# MAGIC a.StatusTypeName as CaseStatusName,
# MAGIC a.ClientLifeCycleName as ClientLifeCycleName,
# MAGIC a.SourceSystem,
# MAGIC a.IsLatestApprovedVersionOfClient,
# MAGIC a.SanctionsOrExternalWatchList,
# MAGIC null as InternalWatchList,--LE
# MAGIC null as AdverseInformationOrMedia,--LE
# MAGIC null as StateFindingsInCaseOfAnyHits,
# MAGIC case when IsClient = 'true' and ClientTypeId = 1 then concat('L2_LEC_',c.ClientId)
# MAGIC      when IsClient = 'true' and ClientTypeId in (2,3) then concat('L2_NP_NPPC_',c.ClientId)
# MAGIC      when IsClient = 'false' and ClientTypeId = 1 then concat('L2_RLEP_',c.ClientId)
# MAGIC      when IsClient = 'false' and ClientTypeId in (2,3) then concat('L2_RNPP_',c.ClientId)
# MAGIC end as SourceClient,
# MAGIC case when IsClient = 'true' and ClientTypeId = 1 then 'Legacy2_LegalEntity'
# MAGIC      when IsClient = 'true' and ClientTypeId in (2,3) then'Legacy2_NP-NPPC'
# MAGIC end as SourceSystemReference
# MAGIC
# MAGIC from  Legacy2_client a   
# MAGIC left outer join Legacy2_Nationality_Citizenship b on b.ClientID = a.ClientID 
# MAGIC left outer join Legacy2_risk c on c.ClientID = a.ClientID 
# MAGIC where IsClient='true' and  a.ClientTypeId in (1,2,3)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Name_Screening as
# MAGIC select * from NameScreening_GCOB
# MAGIC union
# MAGIC select * from NameScreening_Legacy2

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data

# COMMAND ----------

df_Name_Screening = spark.table('Name_Screening')
save_to_saradar_storage_account(df_Name_Screening, Name_Screening_dataobject)

# COMMAND ----------

# %sql
#  DROP TABLE IF EXISTS radar.MI_Name_Screening;

# COMMAND ----------

# spark.sql('select * from Name_Screening').write.mode('overwrite').saveAsTable('radar.mi_name_screening')
