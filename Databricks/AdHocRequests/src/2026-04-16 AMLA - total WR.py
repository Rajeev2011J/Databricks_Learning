# Databricks notebook source
# MAGIC %md
# MAGIC ## Request AMLA
# MAGIC We zijn op zoek naar de W&R brede info voor:
# MAGIC - Number of new customers during 2025
# MAGIC - Number of customers that enter into a business relationship with the firm in a non-face-to-face manner
# MAGIC - Number of clients with HR activities + CDD rating
# MAGIC
# MAGIC Indien mogelijk met split naar regio en bsl.
# MAGIC  
# MAGIC
# MAGIC ## GCDS
# MAGIC
# MAGIC ## MDM
# MAGIC - contract start dt
# MAGIC - customers from ROS
# MAGIC - RDM -> check customers with latest case with RiskCategories on Sector = High. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Connect
import pandas as pd
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Connect
import os
import re
from datetime import datetime,timedelta
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat_ws, regexp_extract, to_date, lit, date_format, row_number

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

radar_datamodel_version_number=2
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
AU_GDP_Defined_Storage_Account=os.environ['AU_GDP_Defined_Storage_Account']

# COMMAND ----------



# COMMAND ----------

SALZReadStorage = 'salandingzonefecradarprd'

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

SALZReadStorage = ReadStorage

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

SALZReadStorage = GIC_ReadStorage

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 


SALZReadStorage = AU_GDP_Defined_Storage_Account

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 


# COMMAND ----------

#Function from 'RadarUtils.py from 2026-02-23 dev branch
# changed to only read data as of latest '1-1-2026'
def Read_GDP_Defined_DataObjects_RANZ(Source , Dataobject,Load_Date=''):
    print(f"{Source}: Processing \033[1m{Dataobject}\033[0m")
 
    AU_GDP_Defined_Storage_Account=os.environ['AU_GDP_Defined_Storage_Account']
 
    # Build the base path
    base_path = f"abfss://mdm-ranz@{AU_GDP_Defined_Storage_Account}.dfs.core.windows.net/{Dataobject}"
    # Build version mapping
    version_mapping_by_source = {
        'RANZ-MDM': {
            2: ['c_lkp_naics_xref','c_lkp_naics_sector_xref','c_lkp_cdd_risk_rating_xref','c_b_party_xref','c_b_party_rel_party_xref','c_b_party_rel_addr_xref','c_b_party_pep_am_xref','c_b_party_naics_xref','c_b_party_dom_cntry_xref','c_b_due_diligence_xref','c_b_contr_rol_party_xref','c_b_contract_xref']
        } }
    Primary_Key_dict = {
    "ROWID_OBJECT": [
        "c_b_party_rel_addr_xref",
        "c_b_contract_xref",
        "c_b_due_diligence_xref",
        "c_b_party_dom_cntry_xref",
        "c_b_party_naics_xref",
        "c_b_party_pep_am_xref",
        "c_b_party_rel_party_xref",
        "c_lkp_cdd_risk_rating_xref",
        "c_lkp_naics_sector_xref",
        "c_lkp_naics_xref",
        "c_b_contr_rol_party_xref"
    ],
    "SRC_PARTY_ID": [
        "c_b_party_xref"
    ]
    }
    try:
        # List files in the base path
        files = dbutils.fs.ls(base_path)
 
        # Determine version
        if Source in version_mapping_by_source:
            source_versions = version_mapping_by_source[Source]
            version = next((v for v, objs in source_versions.items() if Dataobject in objs),None)
            version = '1'
            if version is None:
                raise ValueError(f"{Source} Dataobject '{Dataobject}' not found in version mapping.")
 
        print(f"Selected version : {version}")
 
        # Detect partition folder containing Load_Date
        partition_base_path = f"{base_path}/{version}/data/"
 
        print(f"Reading from path: {partition_base_path}")
 
        df_ranz=spark.read.format('parquet').load(partition_base_path)
       
        # Detect primary key
        pk_column = None
        for pk, table_list in Primary_Key_dict.items():
            if Dataobject in table_list:
                pk_column = pk
                break
 
        if pk_column is None:
            raise ValueError(f"No primary key found for Dataobject {Dataobject} in Primary_Key_dict")
 
        print(f"Using Primary Key for window: {pk_column}")

        # Old logic  
        #df_ranz.withColumn("rn", row_number().over(Window.partitionBy(pk_column).orderBy(col("EDL_ACT_DTS").desc()))).filter(col("rn") == 1).drop("rn").createOrReplaceTempView(Dataobject)

        #First filter with only records before EDL_ACT_DTS = 1-1-2026, then order per PK column.
        df_ranz.filter(col("EDL_ACT_DTS") <= '2026-01-30').withColumn("rn", row_number().over(Window.partitionBy(pk_column).orderBy(col("EDL_ACT_DTS").desc()))).filter(col("rn") == 1).drop("rn").createOrReplaceTempView(Dataobject)
 
        # Print row count
        row_count = spark.sql(f"SELECT COUNT(*) FROM {Dataobject}").collect()[0][0]
        print(f"No. of rows read : {row_count}")
        print(f"{Dataobject} read successfully\n")
 
    except Exception as e:
        raise RuntimeError(f"Error reading {Dataobject} from {Source}: {e}")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### loading the sources

# COMMAND ----------

# DBTITLE 1,connect GCDS
load_dts = 'LOAD_DTS=' + '20260101' + '*'

GCDS_LIST = ['client_Client',
'client_KeyStoreKey',
'client_PartyRole',
'client_PartytoPartyRelationship',
'client_OnboardedLocations']

for dataobject in GCDS_LIST:
    spark.read.format('parquet').load(f"abfss://gcds@{ReadStorage}.dfs.core.windows.net/{dataobject}/4602/data/{load_dts}/*.parquet").createOrReplaceTempView(dataobject)


# COMMAND ----------

# DBTITLE 1,connect MDM
# List of dataobjects from GDP
RANZ_Dataobjects_List = [
'c_b_party_xref'
,'c_b_party_dom_cntry_xref'
,'c_b_party_pep_am_xref'
,'c_b_contract_xref'
,'c_b_party_naics_xref'
,'c_b_party_rel_party_xref'
, 'c_lkp_naics_xref'
, 'c_lkp_naics_sector_xref'
, 'c_b_party_rel_addr_xref'
]

# just a shorter list for this
RANZ_Dataobjects_List = [
'c_b_party_xref'
,'c_b_contract_xref'
]


# Create TempView for each loading table
for Object in RANZ_Dataobjects_List:
    Read_GDP_Defined_DataObjects_RANZ(Source='RANZ-MDM', Dataobject=Object,Load_Date='')

# COMMAND ----------

# DBTITLE 1,connect RDMv3
df_Party = spark.read.format('delta').load('abfss://fec-radar@edlcorestdeuprod0001.dfs.core.windows.net/RadarDataModel/Party/3/data/')
df_Party_case = spark.read.format('delta').load('abfss://fec-radar@edlcorestdeuprod0001.dfs.core.windows.net/RadarDataModel/Party_CDDCase/3/data/')
df_Party_Case_RiskCat = spark.read.format('delta').load('abfss://fec-radar@edlcorestdeuprod0001.dfs.core.windows.net/RadarDataModel/Party_CDDCase_RiskCategories/3/data/')
df_Party_Case_QA = spark.read.format('delta').load('abfss://fec-radar@edlcorestdeuprod0001.dfs.core.windows.net/RadarDataModel/Party_CDDCase_QuestionAnswer/3/data/')

df_Party.filter('LOAD_DT == 20260416').createOrReplaceTempView('Party')
df_Party_case.filter('LOAD_DT== 20260416').createOrReplaceTempView('Party_case')
df_Party_Case_RiskCat.filter('LOAD_DT== 20260416').createOrReplaceTempView('Party_Case_RiskCat') 
df_Party_Case_QA.filter('LOAD_DT== 20260416').createOrReplaceTempView('Party_Case_QA')

# COMMAND ----------

# DBTITLE 1,LatestCompletedUniqueCaseId
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW LatestCompletedUniqueCaseId AS
# MAGIC  
# MAGIC  select PartyIdentifier
# MAGIC  , max_by(UniqueCaseId, CaseCompletedDate ) AS LatestCompletedUniqueCaseId
# MAGIC  , max_by(CaseId, CaseCompletedDate ) AS LatestCompletedCaseId
# MAGIC  , max(CaseCompletedDate) AS MaxCaseCompletedDate
# MAGIC from Party_case 
# MAGIC WHERE StandardCaseStatus IN ('Completed', 'Approved')
# MAGIC GROUP BY PartyIdentifier
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1: Onboarded parties per WR

# COMMAND ----------

# DBTITLE 1,parties RANZ
# MAGIC %sql
# MAGIC select count(*) 
# MAGIC from c_b_contract_xref
# MAGIC where START_DT BETWEEN '2025-01-01' AND '2026-01-01'
# MAGIC -- AND ROWID_SYSTEM = 'RABODIRECT'

# COMMAND ----------

# DBTITLE 1,parties GCDS Except RANZ
# MAGIC %sql
# MAGIC select
# MAGIC  count(*)
# MAGIC from client_client t1
# MAGIC left join client_OnboardedLocations t2 on t1.GCID = t2.GCID
# MAGIC
# MAGIC where t2.Branche_name not in ('Rabo New Zealand', 'Rabobank New Zealand' , 'New Zealand', 'Australia' , 'Rabobank Australia')
# MAGIC AND t2.Onboarding_date > '2025'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2: non-face-to-face onboarded in 2025
# MAGIC
# MAGIC Approach:
# MAGIC - get all from GCOB with that specific risk question
# MAGIC ### - get all from MDM contracts on IDB.

# COMMAND ----------

# DBTITLE 1,total from MDM ranz
# MAGIC %sql
# MAGIC select count(*) AS `Non-F2F`
# MAGIC from c_b_contract_xref 
# MAGIC where LOB_CD= 'RD'
# MAGIC AND LEFT(LIFECYCLE_STATUS_CD, 2) IN ( 'AC', 'BL')
# MAGIC AND START_DT BETWEEN '2025-01-01' AND '2026-01-01'

# COMMAND ----------

# DBTITLE 1,RDM non-ranz for F2F
# MAGIC %sql
# MAGIC select 
# MAGIC  count(distinct(t1.PartyIdentifier)) AS `CUST-WITHOUT-F2F`
# MAGIC  --t1.PartyIdentifier, t2.LatestCompletedCaseId, t4.questionId,  t4.AnswerValue
# MAGIC from Party as t1
# MAGIC left join LatestCompletedUniqueCaseId t2 on t1.PartyIdentifier = t2.PartyIdentifier
# MAGIC --left join party_case t3 on t2.LatestCompletedUniqueCaseId = t3.UniqueCaseId
# MAGIC left join party_case_QA t4 on t2.LatestCompletedCaseId = t4.CaseId AND t2.PartyIdentifier = t4.PartyIdentifier
# MAGIC left join client_OnboardedLocations t5 on REPLACE(t1.PartyIdentifier, 'GCDS_', '') = t5.GCID
# MAGIC
# MAGIC where (t1.CustomerLifeCycleStatus = 'Client'
# MAGIC OR LEFT(t1.CustomerLifeCycleStatus,2) IN ('Bl', 'Ac') )
# MAGIC AND t4.questionId IN (61, 261, 728, 674, 470, 383, 261)  
# MAGIC AND T4.AnswerValue = 'FALSE'
# MAGIC
# MAGIC and t5.Branche_name not in ('Rabo New Zealand', 'Rabobank New Zealand' , 'New Zealand', 'Australia' , 'Rabobank Australia')
# MAGIC AND t5.Onboarding_date > '2025'

# COMMAND ----------

# DBTITLE 1,RDM non-ranz for F2F check party_Details
# MAGIC %sql
# MAGIC select 
# MAGIC  t1.*
# MAGIC from Party as t1
# MAGIC left join LatestCompletedUniqueCaseId t2 on t1.PartyIdentifier = t2.PartyIdentifier
# MAGIC --left join party_case t3 on t2.LatestCompletedUniqueCaseId = t3.UniqueCaseId
# MAGIC left join party_case_QA t4 on t2.LatestCompletedCaseId = t4.CaseId AND t2.PartyIdentifier = t4.PartyIdentifier
# MAGIC left join client_OnboardedLocations t5 on REPLACE(t1.PartyIdentifier, 'GCDS_', '') = t5.GCID
# MAGIC
# MAGIC where (t1.CustomerLifeCycleStatus = 'Client'
# MAGIC OR LEFT(t1.CustomerLifeCycleStatus,2) IN ('Bl', 'Ac') )
# MAGIC AND t4.questionId IN (61, 261, 728, 674, 470, 383, 261)  
# MAGIC AND T4.AnswerValue = 'FALSE'
# MAGIC
# MAGIC and t5.Branche_name not in ('Rabo New Zealand', 'Rabobank New Zealand' , 'New Zealand', 'Australia' , 'Rabobank Australia')
# MAGIC AND t5.Onboarding_date > '2025'

# COMMAND ----------

# DBTITLE 1,RDM Ranz F2F
# MAGIC %sql
# MAGIC select distinct
# MAGIC  count(distinct(t1.PartyIdentifier))
# MAGIC  --t1.PartyIdentifier, 'Ranz_NoCaseId', t4.questionId, t4.AnswerValue
# MAGIC from Party as t1
# MAGIC left join party_case_QA t4 on t1.PartyIdentifier = t4.PartyIdentifier
# MAGIC where LEFT(t1.PartyIdentifier, 4) = 'RANZ'
# MAGIC AND (t1.CustomerLifeCycleStatus = 'Client'
# MAGIC OR LEFT(t1.CustomerLifeCycleStatus,2) IN ('Bl', 'Ac') )
# MAGIC AND t4.questionId IN (61, 261, 728, 674, 470, 383, 261) 
# MAGIC AND T4.AnswerValue = 'FALSE'

# COMMAND ----------

# DBTITLE 1,checking answervalue per questionID
# MAGIC %sql
# MAGIC select distinct questionID, questiontext, answerValue from party_case_QA 
# MAGIC WHERE questionId IN (61, 261, 728, 674, 470, 383, 261) 

# COMMAND ----------

questioncode IN (61, 261, 728, 674, 470, 383, 261)

 for f2f.  where answer = no.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Party_case
# MAGIC WHERE PartyIdentifier IN (
# MAGIC
# MAGIC   SELECT DISTINCT PARTYIDENTIFIER
# MAGIC from Party_case_RiskCat as t1
# MAGIC Where t1.UniqueCaseId like '%KN1%'
# MAGIC AND t1.SectorRiskLevel = 'High'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4: HR Sector involvement per client
# MAGIC
# MAGIC - RANZ, GCOB, KN1 clients with latest case that has High risk Sector

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE_1 AS (
# MAGIC select 
# MAGIC  t1.PartyIdentifier, t2.LatestCompletedCaseId, t4.SectorRiskLevel, t3.ValidatedRiskLevel
# MAGIC from Party as t1
# MAGIC left join LatestCompletedUniqueCaseId t2 on t1.PartyIdentifier = t2.PartyIdentifier
# MAGIC left join party_case t3 on t2.LatestCompletedCaseId = t3.CaseId AND t2.PartyIdentifier = t3.PartyIdentifier
# MAGIC left join party_case_RiskCat t4 on t2.LatestCompletedCaseId = t4.CaseId AND t2.PartyIdentifier = t4.PartyIdentifier
# MAGIC
# MAGIC where (t1.CustomerLifeCycleStatus = 'Client'
# MAGIC OR LEFT(t1.CustomerLifeCycleStatus,2) IN ('Bl', 'Ac') )
# MAGIC AND t4.SectorRiskLevel = 'High'
# MAGIC )
# MAGIC select count(distinct(partyIdentifier)) as COUNT
# MAGIC , ValidatedRiskLevel
# MAGIC FROM CTE_1
# MAGIC GROUP BY ValidatedRiskLevel

# COMMAND ----------

# DBTITLE 1,Base query HR Sectors
# MAGIC %sql
# MAGIC WITH CTE_1 AS (
# MAGIC select 
# MAGIC  t1.PartyIdentifier, t2.LatestCompletedCaseId, t4.SectorRiskLevel, t3.ValidatedRiskLevel
# MAGIC from Party as t1
# MAGIC left join LatestCompletedUniqueCaseId t2 on t1.PartyIdentifier = t2.PartyIdentifier
# MAGIC left join party_case t3 on t2.LatestCompletedCaseId = t3.CaseId AND t2.PartyIdentifier = t3.PartyIdentifier
# MAGIC left join party_case_RiskCat t4 on t2.LatestCompletedCaseId = t4.CaseId AND t2.PartyIdentifier = t4.PartyIdentifier
# MAGIC
# MAGIC where (t1.CustomerLifeCycleStatus = 'Client'
# MAGIC OR LEFT(t1.CustomerLifeCycleStatus,2) IN ('Bl', 'Ac') )
# MAGIC AND t4.SectorRiskLevel = 'High'
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC select distinct
# MAGIC  t1.PartyIdentifier, 'Ranz_NoCaseId', t4.SectorRiskLevel, t2.ValidatedRiskLevel
# MAGIC from Party as t1
# MAGIC  left join party_case t2 on t1.PArtyIDentifier = t2.PartyIDentifier
# MAGIC left join party_case_RiskCat t4 on t1.PartyIdentifier = t4.PartyIdentifier
# MAGIC where LEFT(t1.PartyIdentifier, 4) = 'RANZ'
# MAGIC AND (t1.CustomerLifeCycleStatus = 'Client'
# MAGIC OR LEFT(t1.CustomerLifeCycleStatus,2) IN ('Bl', 'Ac') )
# MAGIC AND t4.SectorRiskLevel = 'High'
# MAGIC --2195 GCOB
# MAGIC
# MAGIC )
# MAGIC select count(distinct(partyIdentifier)) as COUNT
# MAGIC , ValidatedRiskLevel
# MAGIC FROM CTE_1
# MAGIC GROUP BY ValidatedRiskLevel

# COMMAND ----------

# DBTITLE 1,potentially based on questionid risky sectors
questionid 23, 24, 
774, 559, 647, 
418 , 475 risky sectors?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix: Reference Queries

# COMMAND ----------

# DBTITLE 1,RDM party
# MAGIC %sql
# MAGIC select * from party
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,RDM Case
# MAGIC %sql
# MAGIC select * from party_case
# MAGIC WHERE LEFT(PartyIDentifier, 4) = 'RANZ'
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,RDM QA
# MAGIC %sql
# MAGIC select * from party_case_QA
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_case_RiskCat
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC
