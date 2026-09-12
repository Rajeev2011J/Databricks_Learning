# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal:
# MAGIC
# MAGIC To extract all RANZ IRAP answers from MDM directly. 
# MAGIC - https://raboweb.sharepoint.com/:x:/r/teams/DNBIntegrityQuestionnaire2022-DNBQ.2026report2025/_layouts/15/Doc.aspx?sourcedoc=%7B9F30FC93-80AE-41E4-88D3-0EFD49E4B5F9%7D&file=ENG%20-%20DNBQ%20IRAP%202026%20-%20team%2039.xlsx&action=default&mobileredirect=true 
# MAGIC - MDM on GDP: https://rabobank.collibra.com/asset/019affe7-308e-70d9-a649-7209cb9999f2
# MAGIC
# MAGIC
# MAGIC ##### Themes to uncover
# MAGIC 1. Client numbers
# MAGIC 2. NAICS related to risks
# MAGIC 3. TRUST related
# MAGIC 4. PEPS

# COMMAND ----------

import os
import re
from datetime import datetime,timedelta
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat_ws, regexp_extract, to_date, lit, date_format, row_number

# COMMAND ----------


app_reg_app_id = os.environ['APP_REG_APP_ID']
application_id = app_reg_app_id
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
tenant_id = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=2
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
AU_GDP_Defined_Storage_Account=os.environ['AU_GDP_Defined_Storage_Account']
SARADAR = "saradar" + environment

# COMMAND ----------

def authenticate_storage_account(write_storage):
    # This method is for Configuring Spark to access SARADAR Storage account using OAuth authentication
    spark.conf.set("fs.azure.account.auth.type."+write_storage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+write_storage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+write_storage+".dfs.core.windows.net", ""+application_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+write_storage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+write_storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")

    print('Connection to ' + write_storage +' storage account is successful.')

    return 

# COMMAND ----------

#authenticate RANZ SA:
authenticate_storage_account(AU_GDP_Defined_Storage_Account)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preparation and Exploration
# MAGIC - mechanisms to load the data from GDP in-memory
# MAGIC - mechanisms to explore the data to see what we're dealing with

# COMMAND ----------

# DBTITLE 1,reading mdm function
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

# DBTITLE 1,read later version of contr_rol_party

df_ranz = df_ranz=spark.read.format('delta').load(f"abfss://mdm-ranz@{AU_GDP_Defined_Storage_Account}.dfs.core.windows.net/c_b_contr_rol_party_xref/2/data")
# Old logic  
df_ranz.withColumn("rn", row_number().over(Window.partitionBy('ROWID_OBJECT').orderBy(col("EDL_ACT_DTS").desc()))).filter(col("rn") == 1).drop("rn").createOrReplaceTempView('c_b_contr_rol_party_xref')



# COMMAND ----------

# DBTITLE 1,read all objects
# List of dataobjects from GDP
RANZ_Dataobjects_List = [
'c_b_party_xref',
'c_b_party_dom_cntry_xref'
,'c_b_party_pep_am_xref'
,'c_b_contract_xref'
,'c_b_party_naics_xref'
,'c_b_party_rel_party_xref'
, 'c_lkp_naics_xref'
, 'c_lkp_naics_sector_xref'
, 'c_b_party_rel_addr_xref'

]

# Create TempView for each loading table
for Object in RANZ_Dataobjects_List:
    Read_GDP_Defined_DataObjects_RANZ(Source='RANZ-MDM', Dataobject=Object,Load_Date='')

# COMMAND ----------

# DBTITLE 1,top 10 Address
# MAGIC %sql
# MAGIC select * from c_b_party_rel_addr_xref limit 4

# COMMAND ----------

# DBTITLE 1,top 10 - party_dom_cntry
# MAGIC %sql
# MAGIC select * from c_b_party_dom_cntry_xref
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,top 10 - contract
# MAGIC %sql
# MAGIC select * 
# MAGIC from c_b_contract_xref
# MAGIC limit 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct LOB_CD
# MAGIC FROM c_b_contract_xref

# COMMAND ----------

# DBTITLE 1,top 10 - contract rel party
# MAGIC %sql
# MAGIC select * 
# MAGIC from c_b_contr_rol_party_xref
# MAGIC limit 10
# MAGIC
# MAGIC -- HUB_STATE_IND should be 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(ROLE_TYPE_CD)
# MAGIC from c_b_contr_rol_party_xref
# MAGIC  -- HUB_STATE_IND should be 1
# MAGIC -- who has power of atterney?

# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT *
# MAGIC from c_b_party_rel_party_xref
# MAGIC WHERE REL_TYPE_CD = 'UBO'
# MAGIC limit 10
# MAGIC  -- HUB_STATE_IND should be 1
# MAGIC -- who has power of atterney?

# COMMAND ----------

# DBTITLE 1,top 10 - Party_rel_Party
# MAGIC %sql
# MAGIC select DISTINCT REL_TYPE_CD, CNTRL_PERSON_TYPE_CD, REL_SUB_TYPE
# MAGIC from c_b_party_rel_party_xref
# MAGIC WHERE REL_TYPE_CD = 'UBO'
# MAGIC limit 10
# MAGIC  -- HUB_STATE_IND should be 1
# MAGIC -- who has power of atterney?

# COMMAND ----------

# DBTITLE 1,top 10 - party
# MAGIC %sql
# MAGIC select * 
# MAGIC from c_b_party_xref
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SRC_SYS_CD, count(*)
# MAGIC from c_b_party_xref
# MAGIC  group by SRC_SYS_CD

# COMMAND ----------

# DBTITLE 1,top 10 - naics
# MAGIC %sql
# MAGIC select * 
# MAGIC from c_b_party_naics_xref
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,NAICS_DNB_MAPPING_V1
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW DNB_NAICS_MAPPING AS 
# MAGIC
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     read_files(
# MAGIC       'file:/Workspace/data/DNB-SECTOR-MAPPING.csv',
# MAGIC       header => "True",
# MAGIC       sep => ";"
# MAGIC     )
# MAGIC     WHERE `Include` = 'Yes'

# COMMAND ----------

# DBTITLE 1,NAICS_DNB_MAPPING_V2
# MAGIC %sql
# MAGIC -- TODO NAICS; How many Naics codes are there per customer. For how many are there a 
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW DNB_NAICS_MAPPING_V2 AS 
# MAGIC
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     read_files(
# MAGIC       'file:/Workspace/data/GR10 Mapping for GR10 Sectors & SBI Codes 2026 - version 4.csv',
# MAGIC       header => "True",
# MAGIC       sep => ";"
# MAGIC     )
# MAGIC     WHERE `Include` = 'Yes'
# MAGIC     --?AND HRSector IS NULL
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,DNB Sector Mapping V3
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW DNB_NAICS_MAPPING_V3 AS 
# MAGIC
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     read_files(
# MAGIC       'file:/Workspace/data/SIRA 2024 DNB-Q GR10 Sector Reference Table - for RANZ mapping.csv',
# MAGIC       header => "True",
# MAGIC       sep => ";"
# MAGIC     )
# MAGIC     WHERE `Include` = 'Yes'
# MAGIC     --?AND HRSector IS NULL
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,NAICS_DNB_MAPPING_v4
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW DNB_NAICS_MAPPING_V4 AS 
# MAGIC
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     read_files(
# MAGIC       'file:/Workspace/data/Applied Mapping for GR10 Sectors & SBI Codes 17 MAR 2026.csv',
# MAGIC       header => "True",
# MAGIC       sep => ";"
# MAGIC     )
# MAGIC     WHERE `Include` = 'Yes'
# MAGIC     --?AND HRSector IS NULL
# MAGIC
# MAGIC

# COMMAND ----------

Ranz_Client_Lifecycle_Status_Dict = {
    "-2": "UNKNOWN",
    "AC": "Active Client",
    "ACPR": "Active-Pending Risk Client",
    "AP": "Applicant Client",
    "BL": "Blocked Client",
    "BL01": "Blocked - Post No Debits",
    "BL02": "Blocked - Post No Credits",
    "BL03": "Blocked - Post No Entries",
    "BL04": "Blocked - Pending Documentation",
    "BL05": "Blocked - Deceased Estate",
    "BL06": "Blocked - Closure Quoted",
    "BL07": "Blocked - Account on Referral List",
    "BL08": "Blocked - Account on Referral List - CR",
    "BL09": "Blocked - Account on Referral List - DR",
    "BL10": "Blocked - Post Credits to Savings a/c",
    "BL11": "Blocked - Post Debits to Current a/c",
    "BL12": "Blocked - Customer Deceased",
    "BL20": "Blocked - Hold Debits: Funds Held as Security",
    "BL50": "Blocked - Setting up of company",
    "BL51": "Blocked - Overdraw not allowed",
    "BL52": "Blocked - Management authorisation",
    "BL53": "Blocked - General Debit Block",
    "BL54": "Blocked - General DR & CR block",
    "BL55": "Blocked - Several blocking codes",
    "BL56": "Blocked - Judicial instructions",
    "BL57": "Blocked - Missing documents",
    "BL58": "Blocked - Temporary blocking",
    "BL59": "Blocked - Multiple blocking",
    "BL60": "Blocked - ATO/IRD request",
    "BL61": "Blocked - Litigation",
    "BL62": "Blocked - Centrelink",
    "BL63": "Blocked - Pending client onboarding",
    "BL64": "Blocked - Incomplete CDD",
    "BL65": "Blocked - Client request block",
    "BL66": "Blocked - Multiple blocking",
    "BL67": "Blocked - Inactive customer",
    "BL68": "Blocked - Overdue remediation",
    "BL69": "Blocked - Bulk account closure remove",
    "BL70": "Blocked - Account being closed",
    "BL71": "Blocked - Pending Client exit",
    "BL72": "Blocked - Pending Client exit DB",
    "BL73": "Blocked - Pending Client exit CR",
    "BL74": "Blocked - Fraud",
    "BL75": "Blocked - For failed authentication",
    "BL76": "Blocked - For disabled end user",
    "BL77": "Blocked - Login Cancelled",
    "BL78": "Blocked - Login Not Used",
    "BL79": "Blocked - Login Cancelled - 1 to 7 years",
    "BL80": "Blocked - Login Cancelled - 0 to 1 years",
    "BL90": "Blocked - Automatic Closure",
    "BL91": "Blocked - Temp Auto Closing",
    "BL99": "Blocked - Account Closure",
    "CL": "Closed Client",
    "DC": "Declined Client",
    "DL": "Deleted",
    "PR": "Prospect Client",
    "WD": "Withdrawn Client",
    "WD01": "Withdrawn - Lost to competitor",
    "WD02": "Withdrawn - Structural conditions could not be met",
    "WD03": "Withdrawn - Not proceeding with funding request",
    "WD04": "Withdrawn - Time constraints",
    "WD05": "Withdrawn - To resubmit",
    "WD06": "Withdrawn - Other",
    "WD07": "Withdrawn - To apply for low cost/no fee account",
    "DC01": "Declined - Credit declined",
    "DC02": "Declined - Unacceptable AML",
    "DC03": "Declined - Other"
}

sdf_RANZ_Client_Lifecycle_Status_Mapping = spark.createDataFrame([(code, desc) for code, desc in Ranz_Client_Lifecycle_Status_Dict.items()],["LIFECYCLE_STATUS_CD", "CLIENT_LIFECYCLE_STATUS_DESCRIPTION"])
sdf_RANZ_Client_Lifecycle_Status_Mapping.createOrReplaceTempView("RANZ_Client_Lifecycle_Status_Mapping")


Ranz_Pep_Evaluation_Static_Dict ={'01'	:'PEP',
'02':	'Close associate of a PEP',
'03':	'Immediate family member of a PEP',
'04':	'Not a PEP'}

sdf_Ranz_Pep_Evaluation_Static_Mapping =spark.createDataFrame([(code, description) for code, description in Ranz_Pep_Evaluation_Static_Dict.items()],['PEP_EVAL_CD','PEP_EVAL_DESCRIPTION'])
sdf_Ranz_Pep_Evaluation_Static_Mapping.createOrReplaceTempView('Ranz_Pep_Evaluation_Static_Mapping')

# COMMAND ----------

# DBTITLE 1,unique domicile country types
# MAGIC %sql
# MAGIC select distinct DOM_TYPE
# MAGIC from c_b_party_dom_cntry_xref
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct ROWID_SYSTEM 
# MAGIC from c_b_party_xref

# COMMAND ----------

# DBTITLE 1,CENTRAL MDM table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW MDM_RANZ AS
# MAGIC Select distinct
# MAGIC t1.CONTR_ID,
# MAGIC t1.CONTR_TYPE_CD,
# MAGIC t2.ROLE_TYPE_CD,
# MAGIC t6.CLIENT_LIFECYCLE_STATUS_DESCRIPTION as CustomerLifeCycleStatus,
# MAGIC
# MAGIC   CONCAT('RANZ_',t3.SRC_PARTY_ID) as LocalSystemIdentifier,
# MAGIC   t3.SRC_PARTY_ID,
# MAGIC   t3.ROWID_XREF,
# MAGIC   t3.PARTY_NAME,
# MAGIC   Case 
# MAGIC     when t3.PARTY_TYPE_CD='I' then 'Individual'
# MAGIC     when t3.PARTY_TYPE_CD='O' then 'Organization' 
# MAGIC     end as Party_Type,
# MAGIC
# MAGIC   t1.RANZG_CNTRY_CD AS Branch_CNTRY_CD,
# MAGIC   t1.LOB_CD AS LineOfBusiness,
# MAGIC   Case 
# MAGIC     when t5.CNTRY_CD is not null then t5.CNTRY_CD
# MAGIC     else t1.RANZG_CNTRY_CD
# MAGIC     end as CountryOfTaxResidence,
# MAGIC   
# MAGIC   t7.pep_eval_description as PEPStatus
# MAGIC   
# MAGIC
# MAGIC FROM c_b_contract_xref t1
# MAGIC LEFT JOIN c_b_contr_rol_party_xref t2 on t1.CONTR_ID=t2.FK_CONTR_ID
# MAGIC LEFT JOIN c_b_party_xref t3 on t2.FK_PARTY_ID = t3.ROWID_XREF
# MAGIC LEFT JOIN c_b_party_pep_am_xref t4 on t4.FK_PARTY_ID = t3.ROWID_XREF
# MAGIC LEFT JOIN c_b_party_dom_cntry_xref t5 on t3.ROWID_XREF = t5.FK_PARTY_ID AND t5.DOM_TYPE = 'TAX_RESD'
# MAGIC lEFT JOIN RANZ_Client_Lifecycle_Status_Mapping t6 ON t6.LIFECYCLE_STATUS_CD = t1.LIFECYCLE_STATUS_CD
# MAGIC LEFT JOIN Ranz_Pep_Evaluation_Static_Mapping t7 on t7.PEP_EVAL_CD  = t4.PEP_EVAL_CD
# MAGIC
# MAGIC WHERE LEFT(t1.LIFECYCLE_STATUS_CD, 2) IN ( 'AC', 'BL') -- only Active and Blocked clients
# MAGIC
# MAGIC   
# MAGIC  -- left join Ranz_Adverse_Media_Mapping AM on AM.AM_EVAL_CD=RANZ_PEP.AM_EVAL_CD
# MAGIC  -- Customer is typically PHOLD or SHOLD or JMEM. (anything other is more like a role on a contract)
# MAGIC  -- Advise from Anil; If it's related to Address, then go to address table and look for country code. There could be a mismatch between
# MAGIC  -- Get Registered or Residence, or Principal Place of Business. 
# MAGIC   

# COMMAND ----------

# DBTITLE 1,top 10 - combined 'MDM_RANZ' query
# MAGIC %sql
# MAGIC select * 
# MAGIC from MDM_RANZ
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,Check how many roles there are
# MAGIC %sql
# MAGIC select count(*) , ROLE_TYPE_CD
# MAGIC from MDM_RANZ
# MAGIC GROUP BY ROLE_TYPE_CD

# COMMAND ----------

# MAGIC %md
# MAGIC ### IRAP questions

# COMMAND ----------

# MAGIC %md
# MAGIC #### 08 - Customer countries relative to Rabo Branch/Subsidiary - Business Definition alligned to MDM dataset
# MAGIC For each Contract, the count of the instances where the Country of Tax residence (of the Primary holder) is equal to the Country of the branch where the Primary holder is a customer, split between Individuals and Organization customers
# MAGIC

# COMMAND ----------

# DBTITLE 1,08. Party per Sub / branch per residence country per party_type
# MAGIC %sql
# MAGIC WITH CTE_1 AS (
# MAGIC select Branch_CNTRY_CD 
# MAGIC , count(distinct(SRC_PARTY_ID)) AS CountOfCustomersPerBranchPerResidenceCountry
# MAGIC , CASE 
# MAGIC     WHEN Branch_CNTRY_CD  = CountryOfTaxResidence THEN 'SameCountry'
# MAGIC     ELSE 'NotSameCountry'
# MAGIC     END AS CustomerSameCountryAsRabo
# MAGIC , CountryOfTaxResidence
# MAGIC , Party_Type
# MAGIC
# MAGIC from MDM_RANZ
# MAGIC
# MAGIC WHERE ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC group by Branch_CNTRY_CD, CountryOfTaxResidence, Party_Type
# MAGIC ORDER BY Branch_CNTRY_CD, Party_Type, count(distinct(SRC_PARTY_ID)) desc
# MAGIC )
# MAGIC SELECT 
# MAGIC CustomerSameCountryAsRabo, Party_Type, Branch_CNTRY_CD , sum(CountOfCustomersPerBranchPerResidenceCountry) AS NR_OF_CUST
# MAGIC FROM CTE_1
# MAGIC GROUP BY CustomerSameCountryAsRabo, Party_Type, Branch_CNTRY_CD 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select ADDR_TYPE_CD , count(*) AS countOfAdrressType
# MAGIC  from c_b_party_rel_addr_xref 
# MAGIC  GROUP BY ADDR_TYPE_CD

# COMMAND ----------

# DBTITLE 1,8 - Customers Relative to Branch: By RESD and REGD Address
# MAGIC %sql
# MAGIC WITH CTE_1 AS (
# MAGIC select 
# MAGIC t1.LineOfBusiness
# MAGIC , t1.Branch_CNTRY_CD 
# MAGIC , count(distinct(t1.SRC_PARTY_ID)) AS CountOfCustomersPerBranchPerResidenceCountry
# MAGIC , CASE 
# MAGIC     WHEN t1.Branch_CNTRY_CD  = t2.CNTRY_CD THEN 'SameCountry'
# MAGIC     WHEN t2.CNTRY_CD IS NULL THEN 'SameCountry' -- business logic explained by RANZ SA
# MAGIC     WHEN t1.Branch_CNTRY_CD  != t2.CNTRY_CD THEN 'NotSameCountry'
# MAGIC     ELSE 'SameCountry'
# MAGIC     END AS CustomerSameCountryAsRabo
# MAGIC --CountryOfTaxResidence
# MAGIC , t2.CNTRY_CD AS RESIDENTIAL_OR_REGISTERED_ADDRESS_COUNTRY_CD
# MAGIC , t1.Party_Type
# MAGIC
# MAGIC from MDM_RANZ AS T1
# MAGIC LEFT OUTER JOIN (SELECT * FROM c_b_party_rel_addr_xref WHERE ADDR_TYPE_CD IN ('RESD', 'REGD', 'PPOB')) AS t2 on t1.ROWID_XREF = t2.FK_PARTY_ID
# MAGIC
# MAGIC WHERE t1.ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC group by t1.Branch_CNTRY_CD, t2.CNTRY_CD, t1.Party_Type, t1.LineOfBusiness
# MAGIC --ORDER BY t1.Branch_CNTRY_CD, t1.Party_Type, count(distinct(t1.SRC_PARTY_ID)) desc
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     LineOfBusiness, 
# MAGIC     CustomerSameCountryAsRabo, 
# MAGIC     Party_Type, 
# MAGIC     Branch_CNTRY_CD , 
# MAGIC     sum(CountOfCustomersPerBranchPerResidenceCountry) AS NR_OF_CUST
# MAGIC FROM CTE_1
# MAGIC GROUP BY CustomerSameCountryAsRabo, Party_Type, Branch_CNTRY_CD , LineOfBusiness
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(t1.ROWID_XREF) )
# MAGIC from MDM_RANZ as t1
# MAGIC left join c_b_party_rel_addr_xref as t2 on t1.ROWID_XREF = t2.FK_PARTY_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM MDM_RANZ WHERE ROLE_TYPE_CD IN ('PHOLD' 'SHOLD', 'JMEM')

# COMMAND ----------

# DBTITLE 1,CUST_ADDRESS definition
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CUST_ADDRESS AS
# MAGIC
# MAGIC SELECT t1.Branch_CNTRY_CD 
# MAGIC , t1.SRC_PARTY_ID
# MAGIC , t1.ROWID_XREF
# MAGIC , t1.Party_Type
# MAGIC , t1.ROLE_TYPE_CD
# MAGIC
# MAGIC , t2.ADDR_TYPE_CD
# MAGIC , t2.CNTRY_CD AS ADDRESS_COUNTRY_CD
# MAGIC
# MAGIC From MDM_RANZ AS T1
# MAGIC LEFT OUTER JOIN c_b_party_rel_addr_xref  t2 on t1.ROWID_XREF = t2.FK_PARTY_ID
# MAGIC
# MAGIC --WHERE t1.ROLE_TYPE_CD IN ('PHOLD' 'SHOLD', 'JMEM')

# COMMAND ----------

# DBTITLE 1,CUST_ADDRESS_GR8 Definition
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CUST_ADDRESS_GR8 AS
# MAGIC
# MAGIC SELECT 
# MAGIC   ROWID_XREF
# MAGIC   ,Party_Type
# MAGIC  ,Branch_CNTRY_CD
# MAGIC  , ROLE_TYPE_CD
# MAGIC   ,  CASE 
# MAGIC     WHEN ADDR_TYPE_CD IS NULL THEN 'SameCountry'
# MAGIC     WHEN ADDR_TYPE_CD IN ('RESD', 'REGD', 'PPOB') AND ADDRESS_COUNTRY_CD != Branch_CNTRY_CD THEN 'NotSameCountry'
# MAGIC     ELSE 'SameCountry'
# MAGIC     END AS CustomerSameCountryAsRabo
# MAGIC  
# MAGIC   FROM CUST_ADDRESS
# MAGIC   WHERE ADDR_TYPE_CD IN ('RESD', 'REGD', 'PPOB')
# MAGIC

# COMMAND ----------

# DBTITLE 1,FINAL count of parties per location
# MAGIC %sql
# MAGIC SELECT 
# MAGIC CustomerSameCountryAsRabo
# MAGIC ,Party_Type
# MAGIC  ,Branch_CNTRY_CD
# MAGIC ,COUNT(DISTINCT(ROWID_XREF)) AS NR_OF_CUST
# MAGIC  FROM CUST_ADDRESS_GR8
# MAGIC WHERE ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC GROUP BY 
# MAGIC   Party_Type
# MAGIC  ,Branch_CNTRY_CD
# MAGIC  ,CustomerSameCountryAsRabo
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 10 - Customers in Risky sectors - Business Definition alligned to MDM dataset
# MAGIC For each Contract, the count of the instances where the first NAICS* (of the Primary holder) is risky according to the sector mapping. 
# MAGIC
# MAGIC *first NAICS = the Naics with the highest percentage if available

# COMMAND ----------

# DBTITLE 1,10 - define unique naics per cust first
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW UniqueNaics AS 
# MAGIC
# MAGIC WITH CTE_1 AS (
# MAGIC   SELECT row_number() OVER (Partition by FK_PARTY_ID ORDER BY NAICS_EXP_PCT DESC, NAICS_CD ASC) AS RN
# MAGIC   ,NAICS_CD
# MAGIC   ,NAICS_EXP_PCT
# MAGIC   ,FK_PARTY_ID
# MAGIC   FROM c_b_party_naics_xref
# MAGIC )
# MAGIC
# MAGIC Select RN
# MAGIC   ,NAICS_CD
# MAGIC   ,naics_exp_pct
# MAGIC   ,FK_PARTY_ID
# MAGIC
# MAGIC FROM CTE_1 WHERE RN=1
# MAGIC
# MAGIC --NAICS_EXP_PCT only for CB / Lending.
# MAGIC -- If we want to Identify Primary NAICS, what they do is take the list_value as the item to order over. so naics 111110 will precede over 121110 (Decision by IT.)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DNB_NAICS_MAPPING_v4

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS adhocrequests.IRAP_RANZ_2026_CUST_NAICS

# COMMAND ----------

# DBTITLE 1,10 export of client -> NAICS data
df1 = spark.sql("""
                
  select DISTINCT
  t1.CONTR_ID,
  t1.SRC_PARTY_ID,
  t1.ROWID_XREF,
  t1.Branch_CNTRY_CD,
 t1.PARTY_NAME
 , t1.LineOfBusiness
 , t2.NAICS_CD AS PrimaryNAICS
, t3.NAICS AS `NAICS_IN_DNB_MAPPING`
, t3.`Sector DNB Vragenlijst` AS `Sector_DNB_Vragenlijst`
, t3.`Omschrijving 2020`AS `Description_Mapped_Risk_Sector`
--, t3.HRSector

FROM MDM_RANZ t1
LEFT JOIN UniqueNaics t2 on t1.ROWID_XREF = t2.FK_PARTY_ID
LEFT JOIN DNB_NAICS_MAPPING_V4 t3 on t2.NAICS_CD = t3.NAICS
WHERE t1.ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
AND t3.HRSector IS NULL

 """)


df1.write.mode("overwrite").saveAsTable('adhocrequests.IRAP_RANZ_2026_CUST_NAICS')

# COMMAND ----------

# DBTITLE 1,10 - Activity in DNB NAICS - version 4 of list
# MAGIC %sql
# MAGIC -- PREP unique Naics per FK_PARTY_ID?
# MAGIC select 
# MAGIC
# MAGIC t1.LineOfBusiness
# MAGIC --, t3.`Sector DNB Vragenlijst`
# MAGIC , REPLACE(t3.`Sector DNB Vragenlijst`, 'NL.39', 'GR.10') AS IRAP_SECTOR
# MAGIC , t3.`Omschrijving 2020`
# MAGIC , t1.Party_type
# MAGIC , COUNT(DISTINCT(t1.ROWID_XREF)) AS CountOfCust
# MAGIC
# MAGIC --, t3.HRSectorCode
# MAGIC --, t3.HRSector
# MAGIC
# MAGIC FROM MDM_RANZ t1
# MAGIC LEFT JOIN UniqueNaics t2 on t1.ROWID_XREF = t2.FK_PARTY_ID
# MAGIC LEFT JOIN DNB_NAICS_MAPPING_V4 t3 on t2.NAICS_CD = t3.NAICS
# MAGIC
# MAGIC WHERE t1.ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM') 
# MAGIC AND t3.HRSector IS NULL
# MAGIC
# MAGIC GROUP BY t3.`Sector DNB Vragenlijst`, t1.Party_type , t1.LineOfBusiness, t3.`Omschrijving 2020`
# MAGIC ORDER BY t3.`Sector DNB Vragenlijst`

# COMMAND ----------

# DBTITLE 1,10 - Activity in DNB NAICS - version 3 of list
# MAGIC %sql
# MAGIC -- PREP unique Naics per FK_PARTY_ID?
# MAGIC select 
# MAGIC COUNT(DISTINCT(t1.ROWID_XREF)) AS CountOfCust
# MAGIC , t3.`Sector DNB Vragenlijst`
# MAGIC , REPLACE(t3.`Sector DNB Vragenlijst`, 'NL.39', 'GR.10') AS IRAP_SECTOR
# MAGIC , t1.Party_type
# MAGIC --, t3.HRSectorCode
# MAGIC --, t3.HRSector
# MAGIC
# MAGIC
# MAGIC FROM MDM_RANZ t1
# MAGIC LEFT JOIN UniqueNaics t2 on t1.ROWID_XREF = t2.FK_PARTY_ID
# MAGIC LEFT JOIN DNB_NAICS_MAPPING_V3 t3 on t2.NAICS_CD = t3.NAICS
# MAGIC
# MAGIC WHERE t1.ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM') 
# MAGIC
# MAGIC GROUP BY t3.`Sector DNB Vragenlijst`, t1.Party_type
# MAGIC ORDER BY t3.`Sector DNB Vragenlijst`

# COMMAND ----------

# DBTITLE 1,10 - Activity in risky sector - version 3 of list
# MAGIC %sql
# MAGIC -- PREP unique Naics per FK_PARTY_ID?
# MAGIC select 
# MAGIC COUNT(DISTINCT(t1.ROWID_XREF)) AS CountOfCust
# MAGIC , t3.`Sector DNB Vragenlijst`
# MAGIC , REPLACE(t3.`Sector DNB Vragenlijst`, 'NL.39', 'GR.10') AS IRAP_SECTOR
# MAGIC , t1.Party_type
# MAGIC --, t3.HRSectorCode
# MAGIC --, t3.HRSector
# MAGIC
# MAGIC
# MAGIC FROM MDM_RANZ t1
# MAGIC LEFT JOIN UniqueNaics t2 on t1.ROWID_XREF = t2.FK_PARTY_ID
# MAGIC LEFT JOIN DNB_NAICS_MAPPING_V3 t3 on t2.NAICS_CD = t3.NAICS
# MAGIC
# MAGIC WHERE t1.ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM') 
# MAGIC AND t3.HRSector IS NOT NULL
# MAGIC
# MAGIC GROUP BY t3.`Sector DNB Vragenlijst`, t1.Party_type
# MAGIC ORDER BY t3.`Sector DNB Vragenlijst`
# MAGIC

# COMMAND ----------

# DBTITLE 1,10 - Activity in risky sector - version 2 of list
# MAGIC %sql
# MAGIC -- PREP unique Naics per FK_PARTY_ID?
# MAGIC select 
# MAGIC COUNT(t1.ROWID_XREF) AS CountOfCust
# MAGIC , t3.`Sector DNB Vragenlijst`
# MAGIC , REPLACE(t3.`Sector DNB Vragenlijst`, 'NL.39', 'GR.10') AS IRAP_SECTOR
# MAGIC , t1.Party_type
# MAGIC --, t3.HRSectorCode
# MAGIC --, t3.HRSector
# MAGIC
# MAGIC
# MAGIC FROM MDM_RANZ t1
# MAGIC LEFT JOIN UniqueNaics t2 on t1.ROWID_XREF = t2.FK_PARTY_ID
# MAGIC LEFT JOIN DNB_NAICS_MAPPING_V2 t3 on t2.NAICS_CD = t3.NAICS
# MAGIC
# MAGIC WHERE ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM') 
# MAGIC AND t3.HRSector IS NULL
# MAGIC
# MAGIC GROUP BY t3.`Sector DNB Vragenlijst`, t1.Party_type
# MAGIC ORDER BY t3.`Sector DNB Vragenlijst`
# MAGIC

# COMMAND ----------

# DBTITLE 1,10 - Activity in risky sector
# MAGIC %sql
# MAGIC -- PREP unique Naics per FK_PARTY_ID?
# MAGIC select 
# MAGIC COUNT(t1.ROWID_XREF) AS CountOfCust
# MAGIC , t3.`Sector DNB Vragenlijst`
# MAGIC , REPLACE(t3.`Sector DNB Vragenlijst`, 'NL.39', 'GR.10') AS IRAP_SECTOR
# MAGIC --, t3.HRSectorCode
# MAGIC --, t3.HRSector
# MAGIC
# MAGIC
# MAGIC FROM MDM_RANZ t1
# MAGIC LEFT JOIN UniqueNaics t2 on t1.ROWID_XREF = t2.FK_PARTY_ID
# MAGIC LEFT JOIN DNB_NAICS_MAPPING t3 on t2.NAICS_CD = t3.NAICS
# MAGIC
# MAGIC WHERE ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC
# MAGIC GROUP BY t3.`Sector DNB Vragenlijst`
# MAGIC ORDER BY t3.`Sector DNB Vragenlijst`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 13 - PEP

# COMMAND ----------

# MAGIC %md
# MAGIC #### 13.00 - PEPs - Business Definition alligned to MDM dataset
# MAGIC The total amount of times in which any party related to the contract, contains a PEP status within 'Immediate family member, Close associate, or full PEP'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2026-03-30 Question From Murray;
# MAGIC -- split by BusinessLine and Country

# COMMAND ----------

# DBTITLE 1,PEP VIA Direct relation on Customer
# MAGIC %sql
# MAGIC select 
# MAGIC 'GR.13.00' AS QuestionName
# MAGIC , count(CONTR_ID)
# MAGIC , PEPSTATUS
# MAGIC from MDM_RANZ
# MAGIC GROUP BY  PEPSTATUS
# MAGIC
# MAGIC -- might want to join Party_rel_party on top of all parties. 
# MAGIC -- from Primary holder, we can travel up to 2 levels within . The depth can be high. We'd have to iterate over Party_rel_party to find all levels of the hierarchy.
# MAGIC -- Any type of relationships to take into account? --> Kiran may be better in answreing. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Additional PEP question
# MAGIC - FROM Ed van der Jagt (2025-04-16)
# MAGIC : AS per AMLA test questionnaire
# MAGIC
# MAGIC 1. How many customers per country are pep
# MAGIC 2. How many LE (per PEP country) have at least 1 PEP UBO .

# COMMAND ----------

# DBTITLE 1,addition 1. Customers themselves PEP
# MAGIC %sql
# MAGIC select 
# MAGIC 'C0040' AS QuestionName
# MAGIC , count(t1.CONTR_ID)
# MAGIC --, t1.PEPSTATUS
# MAGIC , COALESCE(t5.CNTRY_CD , t8.CNTRY_CD ) AS CNTRY_CD
# MAGIC
# MAGIC from MDM_RANZ t1
# MAGIC LEFT JOIN c_b_party_dom_cntry_xref t5 on t1.ROWID_XREF = t5.FK_PARTY_ID AND t5.DOM_TYPE = 'CITZ' --citizenship
# MAGIC LEFT JOIN (SELECT * FROM c_b_party_rel_addr_xref WHERE ADDR_TYPE_CD = 'RESD') AS t8 on t1.ROWID_XREF = t8.FK_PARTY_ID
# MAGIC
# MAGIC WHERE Party_Type = 'Individual'
# MAGIC AND ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC AND PEPSTATUS IS NOT NULL
# MAGIC AND PEPSTATUS <> 'Not a PEP'
# MAGIC
# MAGIC GROUP BY  COALESCE(t5.CNTRY_CD , t8.CNTRY_CD )

# COMMAND ----------

# DBTITLE 1,addition 2. UBO of customer is PEP
# MAGIC %sql
# MAGIC WITH CTE_1 AS (
# MAGIC   select 
# MAGIC   t1.ROWID_XREF
# MAGIC   , t1.CountryOfTaxResidence AS ORGCountry
# MAGIC   , t2.REL_TYPE_CD
# MAGIC   , t5.CNTRY_CD AS PEP_CNTRY_CD
# MAGIC   , t2.FK_REL_PARTY_ID AS UBO_REL_ID
# MAGIC   , t7.pep_eval_description
# MAGIC   , t8.CNTRY_CD AS RESD_CTRYCODE
# MAGIC   , COALESCE(t5.CNTRY_CD , t8.CNTRY_CD ) AS CNTRY_CD
# MAGIC from MDM_RANZ t1
# MAGIC LEFT JOIN c_b_party_rel_party_xref AS t2 ON t1.ROWID_XREF = t2.FK_PARTY_ID
# MAGIC LEFT JOIN c_b_party_xref t3 on t2.FK_REL_PARTY_ID = t3.ROWID_XREF
# MAGIC
# MAGIC LEFT JOIN c_b_party_pep_am_xref t4 on t4.FK_PARTY_ID = t3.ROWID_XREF
# MAGIC LEFT JOIN c_b_party_dom_cntry_xref t5 on t3.ROWID_XREF = t5.FK_PARTY_ID AND t5.DOM_TYPE = 'CITZ' --citizenship
# MAGIC LEFT JOIN Ranz_Pep_Evaluation_Static_Mapping t7 on t7.PEP_EVAL_CD  = t4.PEP_EVAL_CD
# MAGIC LEFT JOIN (SELECT * FROM c_b_party_rel_addr_xref WHERE ADDR_TYPE_CD = 'RESD') AS t8 on t3.ROWID_XREF = t8.FK_PARTY_ID
# MAGIC
# MAGIC WHERE t1.Party_Type = 'Organization'
# MAGIC AND t2.REL_TYPE_CD = 'UBO'  
# MAGIC AND t1.ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC AND t7.pep_eval_description IS NOT NULL
# MAGIC AND t7.pep_eval_description <> 'Not a PEP'
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC 'C0050' AS QuestionName
# MAGIC   , CNTRY_CD AS COUNTRY
# MAGIC   ,count(distinct(ROWID_XREF)) as countOrg 
# MAGIC FROM CTE_1
# MAGIC GROUP BY CNTRY_CD

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE_1 AS (
# MAGIC   select 
# MAGIC   t1.ROWID_XREF
# MAGIC   , t1.CountryOfTaxResidence AS ORGCountry
# MAGIC   , t2.REL_TYPE_CD
# MAGIC   , t5.CNTRY_CD AS PEP_CNTRY_CD
# MAGIC   , t2.FK_REL_PARTY_ID AS UBO_REL_ID
# MAGIC   , t7.pep_eval_description
# MAGIC   , t8.CNTRY_CD AS RESD_CTRYCODE
# MAGIC   , COALESCE(t5.CNTRY_CD , t8.CNTRY_CD ) AS CNTRY_CD
# MAGIC from MDM_RANZ t1
# MAGIC LEFT JOIN c_b_party_rel_party_xref AS t2 ON t1.ROWID_XREF = t2.FK_PARTY_ID
# MAGIC LEFT JOIN c_b_party_xref t3 on t2.FK_REL_PARTY_ID = t3.ROWID_XREF
# MAGIC
# MAGIC LEFT JOIN c_b_party_pep_am_xref t4 on t4.FK_PARTY_ID = t3.ROWID_XREF
# MAGIC LEFT JOIN c_b_party_dom_cntry_xref t5 on t3.ROWID_XREF = t5.FK_PARTY_ID AND t5.DOM_TYPE = 'CITZ' --citizenship
# MAGIC LEFT JOIN Ranz_Pep_Evaluation_Static_Mapping t7 on t7.PEP_EVAL_CD  = t4.PEP_EVAL_CD
# MAGIC LEFT JOIN (SELECT * FROM c_b_party_rel_addr_xref WHERE ADDR_TYPE_CD = 'RESD') AS t8 on t3.ROWID_XREF = t8.FK_PARTY_ID
# MAGIC
# MAGIC WHERE t1.Party_Type = 'Organization'
# MAGIC AND t2.REL_TYPE_CD = 'UBO'  
# MAGIC AND t1.ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC AND t7.pep_eval_description IS NOT NULL
# MAGIC AND t7.pep_eval_description <> 'Not a PEP'
# MAGIC )
# MAGIC
# MAGIC select count(distinct ROWID_XREF) from CTE_1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) 
# MAGIC
# MAGIC from 
# MAGIC
# MAGIC MDM_RANZ as T1
# MAGIC WHERE t1.Party_Type = 'Organization'
# MAGIC --AND t2.REL_TYPE_CD = 'UBO'  
# MAGIC AND t1.ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CUST_ADDRESS AS
# MAGIC
# MAGIC SELECT t1.Branch_CNTRY_CD 
# MAGIC , t1.SRC_PARTY_ID
# MAGIC , t1.ROWID_XREF
# MAGIC , t1.Party_Type
# MAGIC , t1.ROLE_TYPE_CD
# MAGIC
# MAGIC , t2.ADDR_TYPE_CD
# MAGIC , t2.CNTRY_CD AS ADDRESS_COUNTRY_CD
# MAGIC
# MAGIC From MDM_RANZ AS T1
# MAGIC LEFT OUTER JOIN c_b_party_rel_addr_xref  t2 on t1.ROWID_XREF = t2.FK_PARTY_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct  ADDR_TYPE_CD FROM c_b_party_rel_addr_xref WHERE ADDR_TYPE_CD = 'RESD'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct DOM_TYPE
# MAGIC from c_b_party_dom_cntry_xref

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from c_b_party_rel_party_xref limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(REL_TYPE_CD) 
# MAGIC FROM c_b_party_rel_party_xref
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 17
# MAGIC
# MAGIC 0. total count
# MAGIC 1. trusts count
# MAGIC 2. count of foundations
# MAGIC 3. STAK's (typical dutch form)
# MAGIC 4. CV's (typical dutch form)
# MAGIC 5. LP / LLP legal forms
# MAGIC 6. Nominee shareholders

# COMMAND ----------

# MAGIC %md
# MAGIC #### 17.00 - Total Customers - Business Definition alligned to MDM dataset
# MAGIC The total amount of contracts, counted by the amount of Primary holders in total, for all the active contracts. 
# MAGIC - ALTERNATIVE 1, could be counted for ALL parties per contract
# MAGIC - ALTERNATIVE 2, could be counted for specific sub-selection of roles within a contract

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,17.00 Total Contracts
# MAGIC %sql
# MAGIC -- PREP unique Naics per FK_PARTY_ID?
# MAGIC select 
# MAGIC 'GR.17.00' AS QuestionName
# MAGIC , COUNT(t1.ROWID_XREF) AS CountOfCust
# MAGIC
# MAGIC FROM MDM_RANZ t1
# MAGIC WHERE ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 17.01 - Trust - Business Definition alligned to MDM dataset
# MAGIC The total amount of contracts in which Contract Type = TRST (Trust) or SMSF (SelfManaged Superfund)
# MAGIC - https://confluence.dev.rabobank.nl/spaces/CMDM/pages/1053725470/CONTRACT_TYPE+Lookup
# MAGIC - https://confluence.dev.rabobank.nl/spaces/CMDM/pages/1053725632/LEGAL_ENTITY_TYPE+Lookup

# COMMAND ----------

# DBTITLE 1,17.01 Total Trust
# MAGIC %sql
# MAGIC select 
# MAGIC 'GR.17.01_IsTrust' AS QuestionName
# MAGIC -- , Client_BUSINESS_LINE
# MAGIC   ,count(distinct(CONTR_ID)) as Count_Contracts
# MAGIC  , CONTR_TYPE_CD
# MAGIC FROM MDM_RANZ t1
# MAGIC WHERE ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC AND CONTR_TYPE_CD IN ('TRUS', 'SMSF')
# MAGIC GROUP BY CONTR_TYPE_CD
# MAGIC
# MAGIC --Party TYPE should be 'organization' - This will only be available in the future in GDP. As it'll come from the 'Organization' Object.
# MAGIC -- LE type code, group code, subtype code. (part of Primary holder / party information) 
# MAGIC -- https://confluence.dev.rabobank.nl/spaces/CMDM/pages/1053725632/LEGAL_ENTITY_TYPE+Lookup
# MAGIC -- https://confluence.dev.rabobank.nl/spaces/CMDM/pages/1053725407/LEGAL_ENTITY_SUB_TYPE+Lookup 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 17.02 Foundation - Business Definition alligned to MDM dataset
# MAGIC The total amount of contracts in which the Primary holder name contains 'Foundation'.
# MAGIC There is no Contract Type reference that specifies a Foundation as a separate type
# MAGIC https://confluence.dev.rabobank.nl/spaces/CMDM/pages/1053725470/CONTRACT_TYPE+Lookup

# COMMAND ----------

# DBTITLE 1,17.02 count of Foundation
# MAGIC %sql 
# MAGIC -- Global_Client_ID IN (2608127 ,2591655,2612665,2615110,2912234,2931150) 
# MAGIC select  'GR.17.02_IsStichting' AS QuestionName
# MAGIC
# MAGIC , COUNT(ROWID_XREF) AS CountOfCust
# MAGIC  from MDM_RANZ
# MAGIC WHERE ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC AND Party_Name like '%Foundation%'
# MAGIC -- and... some stichting identifiying metric? assuming 0.
# MAGIC -- same as above. 
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,17.02 - actual names
# MAGIC %sql 
# MAGIC -- Global_Client_ID IN (2608127 ,2591655,2612665,2615110,2912234,2931150) 
# MAGIC select  'GR.17.02_IsStichting' AS QuestionName
# MAGIC
# MAGIC , Party_Name
# MAGIC  from MDM_RANZ
# MAGIC WHERE ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC AND Party_Name like '%Stichting%'
# MAGIC -- and... some stichting identifiying metric? assuming 0.
# MAGIC -- same as above. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 17.03 STAK - Business Definition alligned to MDM dataset
# MAGIC The total amount of STAK is always zero because it's a NL-specific legal form

# COMMAND ----------

# MAGIC %md
# MAGIC #### 17.04 - CV - Business Definition alligned to MDM dataset
# MAGIC The total amount of CV is always zero because it's not a legal form used there.  
# MAGIC Optionally, we can use Contract Type Code 'PART' for Partnerships if we find that is equal

# COMMAND ----------

# MAGIC %md
# MAGIC #### 17.05 LP/LLP - Business Definition alligned to MDM dataset
# MAGIC The total amount of contracts in which the Primary holder has the Contract Type Code 'LPAR' (Limited Partnership)
# MAGIC https://confluence.dev.rabobank.nl/spaces/CMDM/pages/1053725470/CONTRACT_TYPE+Lookup

# COMMAND ----------

# DBTITLE 1,17.05 LP / LLP
# MAGIC %sql
# MAGIC --CUSTOMER_TYPE_CD = 'LPAR' 
# MAGIC select 
# MAGIC 'GR.17.05_LP_LLP' AS QuestionName
# MAGIC   ,count(distinct(CONTR_ID)) as Count_Contracts
# MAGIC  , CONTR_TYPE_CD
# MAGIC FROM MDM_RANZ t1
# MAGIC WHERE ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC AND CONTR_TYPE_CD IN ('LPAR')
# MAGIC GROUP BY CONTR_TYPE_CD

# COMMAND ----------

# MAGIC %md
# MAGIC #### 17.06 Nominee Shareholders - Business Definition alligned to MDM dataset
# MAGIC The total amount of parties in which the Role between the party that holds the contract, and one of it's parents (through party_rel_party) is of the type 'NBEN'.  
# MAGIC  As per latest documentation, <span style="color:red">It looks like the 'Nominee' part of NBEN has dropped. Re-alligning with RANZ what this definition should be.</span> Reached out to Gayathri.Sivaji@rabobank.com in absence of Kiran
# MAGIC
# MAGIC - https://confluence.dev.rabobank.nl/spaces/CMDM/pages/1053725403/PARTY_RELATIONSHIP_ROLE_TYPE+Lookup
# MAGIC - https://confluence.dev.rabobank.nl/spaces/CMDM/pages/1053725447/Contract+Role+Party+Type+Code

# COMMAND ----------

# DBTITLE 1,17.06 Nominee shareholder
# MAGIC %sql
# MAGIC select 
# MAGIC 'GR.17.06_NomineeShareholders' AS QuestionName
# MAGIC   ,count(distinct(t1.CONTR_ID)) as Count_Contracts
# MAGIC  , t2.REL_TYPE_CD
# MAGIC FROM MDM_RANZ t1
# MAGIC LEFT JOIN c_b_party_rel_party_xref t2 ON t1.ROWID_XREF = t2.FK_PARTY_ID
# MAGIC WHERE t2.REL_TYPE_CD = 'NBEN'
# MAGIC AND t1.ROLE_TYPE_CD IN ('PHOLD', 'SHOLD', 'JMEM')
# MAGIC GROUP BY t2.REL_TYPE_CD
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- skipping bearer because 0 anyway.
# MAGIC
# MAGIC
# MAGIC select 'GR.17.07_IsBearerShares' AS QuestionName
# MAGIC , 0 AS countnr
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from c_b_party_rel_party_xref limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from MDM_RANZ limit 10

# COMMAND ----------


