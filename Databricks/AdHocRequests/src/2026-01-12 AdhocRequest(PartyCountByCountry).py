# Databricks notebook source
import os
from datetime import datetime, timedelta, timezone
from pyspark.sql.functions import *

#Local File Import
from RadarUtils import *

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
print(date_parameter)
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'

# COMMAND ----------

GDP_EU_Storage_Account = os.environ['GDP_STORAGE_NAME']
GNS_STORAGE_ACCOUNT= os.environ['GNS_STORAGE_ACCOUNT']
GNS_CONTAINER= os.environ['GNS_CONTAINER']
environment=os.environ['ENV']
SARadarStorage = f'saradar{environment}'

# COMMAND ----------

authenticate_storage_account(GDP_EU_Storage_Account)

# COMMAND ----------

# List of dataobjects to load from from GDP


load_df = [
'party_client_structure_GUI',
'party_case_client_details',
'party_AllPartyDetails'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')
    #final_path = f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/{base_path}/{path_suffix}/{Object}/102/data/{load_dts}/*.parquet'
    

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view DistinctCountryName as
# MAGIC select distinct coalesce(RegisteredCountryName, 'NA') as CountryName from party_AllPartyDetails
# MAGIC union
# MAGIC select distinct coalesce(Nationality,'NA') as CountryName from party_AllPartyDetails

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view RegulatorParty as
# MAGIC SELECT
# MAGIC case when TRIM(country) = '' then 'NA' else TRIM(country) end as CountryGroup,
# MAGIC     COUNT(DISTINCT sourceclient) AS RegulatorPartyCount
# MAGIC FROM (
# MAGIC     SELECT DISTINCT
# MAGIC         sourceclient,      
# MAGIC         explode(
# MAGIC                 CASE 
# MAGIC                     WHEN CountryOfRegulator = 'Tanzania, the United Republic of'
# MAGIC                         THEN array(CountryOfRegulator)
# MAGIC                     ELSE split(CountryOfRegulator, ',')
# MAGIC                 END
# MAGIC             ) AS country
# MAGIC
# MAGIC     FROM party_case_client_details
# MAGIC     WHERE IsLatestApprovedVersionOfClient = 'True' and IsClientRegulated = 'True'
# MAGIC ) sub
# MAGIC GROUP BY
# MAGIC case when TRIM(country) = '' then 'NA' else TRIM(country) end;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view StockExchangeListedParty as
# MAGIC SELECT
# MAGIC case when TRIM(country) = '' then 'NA' else TRIM(country) end as CountryGroup,
# MAGIC     COUNT(DISTINCT sourceclient) AS StockExchangeListedPartyCount
# MAGIC FROM (
# MAGIC     SELECT DISTINCT
# MAGIC         sourceclient,      
# MAGIC         explode(
# MAGIC                 CASE 
# MAGIC                     WHEN CountryOfExchange = 'Tanzania, the United Republic of'
# MAGIC                         THEN array(CountryOfExchange)
# MAGIC                     ELSE split(CountryOfExchange, ',')
# MAGIC                 END
# MAGIC             ) AS country
# MAGIC
# MAGIC     FROM party_case_client_details
# MAGIC     WHERE IsLatestApprovedVersionOfClient = 'True'
# MAGIC ) sub
# MAGIC GROUP BY
# MAGIC case when TRIM(country) = '' then 'NA' else TRIM(country) end;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view UboParty as
# MAGIC SELECT
# MAGIC     CountryGroup,
# MAGIC     COUNT(CASE WHEN GroupType = 'RegisteredCountryName' THEN UniqueParentPartyId END) AS UBO_RegisteredCountryName_Count,
# MAGIC     COUNT(DISTINCT CASE WHEN GroupType = 'Nationality' THEN UniqueParentPartyId END) AS UBO_Nationality_Count
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         a.UniqueParentPartyId,
# MAGIC         coalesce(b.RegisteredCountryName, 'NA') as CountryGroup,
# MAGIC         'RegisteredCountryName' AS GroupType
# MAGIC     FROM (
# MAGIC         SELECT DISTINCT UniqueParentPartyId
# MAGIC         FROM party_client_structure_GUI
# MAGIC         WHERE IsLatestApprovedVersionOfClient = 'True' AND IsUbo = 'True'
# MAGIC     ) a
# MAGIC     JOIN party_AllPartyDetails b
# MAGIC         ON a.UniqueParentPartyId = b.UniquePartyId
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         a.UniqueParentPartyId,
# MAGIC         coalesce(b.Nationality,'NA') as CountryGroup,
# MAGIC         'Nationality' AS GroupType
# MAGIC     FROM (
# MAGIC         SELECT DISTINCT UniqueParentPartyId
# MAGIC         FROM party_client_structure_GUI
# MAGIC         WHERE IsLatestApprovedVersionOfClient = 'True' AND IsUbo = 'True'
# MAGIC     ) a
# MAGIC     JOIN party_AllPartyDetails b
# MAGIC         ON a.UniqueParentPartyId = b.UniquePartyId
# MAGIC ) combined
# MAGIC GROUP BY CountryGroup;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view AuthorisedRepresentativeParty as
# MAGIC SELECT
# MAGIC     CountryGroup,
# MAGIC     COUNT(CASE WHEN GroupType = 'RegisteredCountryName' THEN UniqueParentPartyId END) AS Rep_RegisteredCountryName_Count,
# MAGIC     COUNT(DISTINCT CASE WHEN GroupType = 'Nationality' THEN UniqueParentPartyId END) AS Rep_Nationality_Count
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         a.UniqueParentPartyId,
# MAGIC         coalesce(b.RegisteredCountryName, 'NA') as CountryGroup,
# MAGIC         'RegisteredCountryName' AS GroupType
# MAGIC     FROM (
# MAGIC         SELECT DISTINCT UniqueParentPartyId
# MAGIC         FROM party_client_structure_GUI
# MAGIC         WHERE IsLatestApprovedVersionOfClient = 'True' AND TypesOfRelation = 'Authorised Representative'
# MAGIC     ) a
# MAGIC     JOIN party_AllPartyDetails b
# MAGIC         ON a.UniqueParentPartyId = b.UniquePartyId
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         a.UniqueParentPartyId,
# MAGIC         coalesce(b.Nationality, 'NA') as CountryGroup,
# MAGIC         'Nationality' AS GroupType
# MAGIC     FROM (
# MAGIC         SELECT DISTINCT UniqueParentPartyId
# MAGIC         FROM party_client_structure_GUI
# MAGIC         WHERE IsLatestApprovedVersionOfClient = 'True' AND TypesOfRelation = 'Authorised Representative'
# MAGIC     ) a
# MAGIC     JOIN party_AllPartyDetails b
# MAGIC         ON a.UniqueParentPartyId = b.UniquePartyId
# MAGIC ) combined
# MAGIC GROUP BY CountryGroup;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view DistinctParty as
# MAGIC select distinct UniqueChildPartyId as UniquePartyId, ChildType as PartyType, ChildCountryOfRegistration as CountryOfRegistration
# MAGIC from party_client_structure_GUI where IsLatestApprovedVersionOfClient = 'True'
# MAGIC union
# MAGIC select distinct UniqueParentPartyId as UniquePartyId, ParentType as PartyType, ParentCountryOfRegistration as CountryOfRegistration
# MAGIC from party_client_structure_GUI where IsLatestApprovedVersionOfClient = 'True'

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view Party as
# MAGIC SELECT
# MAGIC     coalesce(CountryOfRegistration, 'NA') as CountryGroup,
# MAGIC     SUM(CASE WHEN partytype IN ('NaturalPersonClient','LegalEntityClient') THEN 1 ELSE 0 END) AS ClientCount_Regestered_addr,
# MAGIC     SUM(CASE WHEN partytype IN ('RelatedLegalEntity','RelatedNaturalPerson') THEN 1 ELSE 0 END) AS RelatedPartyCount_Regestered_addr
# MAGIC FROM DistinctParty
# MAGIC GROUP BY coalesce(CountryOfRegistration, 'NA')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.CountryName as COUNTRY,
# MAGIC coalesce(b.ClientCount_Regestered_addr,0) as `Number of CLIENTS with REGISTERED ADDRESS in the country`,
# MAGIC coalesce(b.RelatedPartyCount_Regestered_addr,0) as `Number of RELATED PARTIES with REGISTERED ADDRESS in the country`,
# MAGIC coalesce(c.RegulatorPartyCount,0) as `Number of CLIENTS and RELATED PARTIEs with REGULATOR in the country`,
# MAGIC coalesce(d.StockExchangeListedPartyCount,0) as `Number of CLIENTS and RELATED PARTIES LISTED ON A STOCK EXCHANGE in the country`,
# MAGIC coalesce(e.UBO_RegisteredCountryName_Count,0) as `Number of UBOs with COUNTRY OF RESIDENCE in the country`,
# MAGIC coalesce(e.UBO_Nationality_Count,0) as `Number of UBOs with NATIONALITY of the country`,
# MAGIC coalesce(f.Rep_RegisteredCountryName_Count,0) as `Number of AUTORIZED REPs with COUNTRY OF RESIDENCE In the country`,
# MAGIC coalesce(f.Rep_Nationality_Count,0) as `Number of AUTORIZED REPs with NATIONALITY of the country`
# MAGIC FROM DistinctCountryName as a
# MAGIC LEFT JOIN Party as b
# MAGIC on a.CountryName = b.CountryGroup
# MAGIC LEFT JOIN RegulatorParty as c
# MAGIC on a.CountryName = c.CountryGroup
# MAGIC LEFT JOIN StockExchangeListedParty as d
# MAGIC on a.CountryName = d.CountryGroup
# MAGIC LEFT JOIN UboParty as e
# MAGIC on a.CountryName = e.CountryGroup
# MAGIC LEFT JOIN AuthorisedRepresentativeParty as f
# MAGIC on a.CountryName = f.CountryGroup
# MAGIC order by a.CountryName
