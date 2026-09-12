# Databricks notebook source
# MAGIC %md
# MAGIC ##Radar Data model Data Quality Check

# COMMAND ----------

# Fetching environment variables
import os
app_reg_app_id = os.environ['APP_REG_APP_ID']
TenantId = os.environ['TENANT_ID']
environment=os.environ['ENV']
ReadStorage = f'saradar{environment}'
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

#Connection to SARADAR Storage account
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
Today = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'
print (load_dts)
Load = 'EDL_LOAD_DTS='+Today+''
print(Load)
EDL_LoadDate = (datetime.today()).strftime("%Y-%m-%dT%H:%m:%mZ")

# COMMAND ----------

# print list of strings for loading spark dfs from SARADAR Storage account
load_df = pd.DataFrame({'RadarDataobject':[
'Party',
'Party_Address',
'Party_CDDCase',
'Party_CDDCase_QuestionAnswer',
'Party_Naics',
'Party_SystemIdentifier',
'Party_CDDCase_RiskCategories',
'Party_Structure',
'Party_Coverage',
'Party_Selection',
'Party_CountryAffiliation',
'Party_Documents',
'Party_ClientOwnership'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://radardatamodel@{ReadStorage}.dfs.core.windows.net/{row.RadarDataobject}/1/data/{load_dts}/*.parquet').createOrReplaceTempView(row.RadarDataobject)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC CREATE TABLE IF NOT EXISTS testing.test_log_catalog (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   timestamp TIMESTAMP,
# MAGIC   dataframe_name STRING,
# MAGIC   validation_rule STRING, ---check
# MAGIC   columns_checked STRING,
# MAGIC   status STRING,
# MAGIC   message STRING   
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE testing.test_log_catalog SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE testing.test_log_catalog ALTER COLUMN timestamp SET DEFAULT CURRENT_TIMESTAMP();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uniqueness check

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime

# Initialize Spark session with schema auto-merge enabled
spark = SparkSession.builder.appName("DataFrameUniquenessTest").getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

def log_to_catalog(dataframe_name, rule, columns, status, message):
    # Create a DataFrame for the log entry
    log_df = spark.createDataFrame(
        [(datetime.now(), dataframe_name, rule, columns, status, message)],
        ["timestamp", "dataframe_name", "validation_rule", "columns_checked", "status", "message"]       
    ) 
    # Append the log entry to the log_catalog table
    log_df.write.mode("append").saveAsTable("testing.test_log_catalog")

def test_dataframe_uniqueness(df, dataframe_name, columns, rule_name, where_condition=None):
    try:
        # Apply where condition if provided
        if where_condition:
            df = df.filter(where_condition)

        # Check for uniqueness in the specified columns
        unique_count = df.select(columns).distinct().count()
        total_count = df.count()

        if unique_count == total_count:
            status = "Pass"
            message = f"All values in column(s) {columns} are unique"
        else:
            status = "Fail"
            duplicates_df = df.groupBy(columns).count().filter("count > 1")
            duplicates_count = duplicates_df.count()
            duplicate_partyidentifiers = duplicates_df.select("partyidentifier").limit(10).collect()
            duplicate_partyidentifiers_str = "\n".join([str(row["partyidentifier"]) for row in duplicate_partyidentifiers])
            message      = f"Column(s) {columns} contains {duplicates_count} duplicate values. Few Problematic partyidentifiers: {duplicate_partyidentifiers_str}"

        print(message)
        print(f"Status: {status}")
        # Log the result to the catalog table
        log_to_catalog(dataframe_name, rule_name, ','.join(columns), status, message)
    except Exception as e:
        error_message = f"An error occurred: {e}"
        print(error_message)
        log_to_catalog(dataframe_name, rule_name, ','.join(columns), "Error", error_message)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null value check

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, trim

# Initialize Spark session with schema auto-merge enabled
spark = SparkSession.builder.appName("DataFrameNullTest").getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

def log_to_catalog(dataframe_name, rule, columns, status, message):
    # Create a DataFrame for the log entry
    log_df = spark.createDataFrame(
        [(datetime.now(), dataframe_name, rule, columns, status, message)],
        ["timestamp", "dataframe_name", "validation_rule", "columns_checked", "status", "message"]       
    ) 
    # Append the log entry to the log_catalog table
    log_df.write.mode("append").saveAsTable("testing.test_log_catalog")

def test_dataframe_null_fields(df, dataframe_name, columns, rule_name, where_condition=None):
    try:
        # Apply where condition if provided
        if where_condition:
            df = df.filter(where_condition)

        # Check for null or Blank fields in the specified columns
        for column in columns:
            null_count = df.filter(df[column].isNull()).count()
            
            if null_count==0:
                status = "Pass"                
                message = f"Column(s) {columns} does not contain any null values"
            else:
                status = "Fail"
                #Collect problematic partyidentifier values
                problematic_partyidentifiers = df.filter(df[column].isNull()).select("partyidentifier").limit(10).collect()
                problematic_partyidentifiers_str = "\n".join([str(row["partyidentifier"]) for row in problematic_partyidentifiers])
                message = f"Column(s) {columns} contains {null_count} null values. Few Problematic partyidentifiers: {problematic_partyidentifiers_str}"

            print(message)
            print(f"Status: {status}")
        # Log the result to the catalog table
        log_to_catalog(dataframe_name, rule_name, ','.join(columns), status, message)
    except Exception as e:
        error_message = f"An error occurred: {e}"
        print(error_message)
        log_to_catalog(dataframe_name, rule_name, ','.join(columns), "Error", error_message)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finding missing clients

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime

# Initialize Spark session with schema auto-merge enabled
spark = SparkSession.builder.appName("NLATransactiontoClientTest").getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

def log_to_catalog(dataframe_name, rule, columns, status, message):
    # Create a DataFrame for the log entry
    log_df = spark.createDataFrame(
        [(datetime.now(), dataframe_name, rule, columns, status, message)],
        ["timestamp", "dataframe_name", "validation_rule", "columns_checked", "status", "message"]       
    ) 
    # Append the log entry to the log_catalog table
    log_df.write.mode("append").saveAsTable("testing.test_log_catalog")

def find_missing_parties(df1, df2, dataframe_name1, dataframe_name2, columns, rule_name, where_condition=None):
    try:
        # Apply where condition if provided
        if where_condition:
            df1 = df1.filter(where_condition)
        
        # Perform a left join to find values in df1 not present in df2
        joined_df = df1.join(df2, df1[columns] == df2[columns], 'left')
         
        # Find rows where parties in df2 is null
        missing_parties_df = joined_df.filter(df2[columns].isNull())
        
        # Check if there are any missing values
        if missing_parties_df.count() == 0:
            status = "Pass"
            message = f"All {columns} values in {dataframe_name1} are present in {dataframe_name2} dataobject."        
        else:
            status = "Fail"
            missing_count = missing_parties_df.count()
            # Collect all the missing identifiers
            missing_details = missing_parties_df.select(df1[columns]).distinct().limit(10).collect()
            missing_info = "\n".join([str(row[columns]) for row in missing_details])
            message = f"There are {missing_count} {columns} values in {dataframe_name1} that are not present in {dataframe_name2} dataobject. The details are:\n{missing_info}"

        print(message)
        print(f"Status: {status}")
        # Log the result to the catalog table
        log_to_catalog(dataframe_name2, rule_name, columns, status, message)
    except Exception as e:
        error_message = f"An error occurred: {e}"
        print(error_message)
        log_to_catalog(dataframe_name2, rule_name, columns, "Error", error_message)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule_based_data_check

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime

# Initialize Spark session with schema auto-merge enabled
spark = SparkSession.builder.appName("Rulebaseddatacheck").getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

def log_to_catalog(dataframe_name, rule, columns, status, message):
    # Create a DataFrame for the log entry
    log_df = spark.createDataFrame(
        [(datetime.now(), dataframe_name, rule, columns, status, message)],
        ["timestamp", "dataframe_name", "validation_rule", "columns_checked", "status", "message"]       
    ) 
    # Append the log entry to the log_catalog table
    log_df.write.mode("append").saveAsTable("testing.test_log_catalog")

def test_dataframe_condition(df, dataframe_name, rule_name, condition_description, condition):
    try:
        # Filter rows that violate the condition
        violating_rows = df.filter(condition)
        violating_count = violating_rows.count()

        if violating_count == 0:
            status = "Pass"
            message = f"No rows violate the condition: {condition_description}"
        else:
            status = "Fail"
            # Dynamically select columns that exist
            available_columns = df.columns
            selected_columns = [col for col in ["PartyIdentifier", "CaseId"] if col in available_columns]
            sample_rows = violating_rows.select(*selected_columns).limit(10).toPandas().to_string(index=False)
            message = (
                f"{violating_count} rows violate the condition: {condition_description}\n"
                f"Sample rows:\n{sample_rows}"
            )

        print(message)
        print(f"Status: {status}")
        log_to_catalog(dataframe_name, rule_name, ", ".join(selected_columns), status, message)

    except Exception as e:
        error_message = f"An error occurred: {e}"
        print(error_message)
        log_to_catalog(dataframe_name, rule_name, "N/A", "Error", error_message)



# COMMAND ----------

# MAGIC %md
# MAGIC ##GIC Test Scripts

# COMMAND ----------

# DBTITLE 1,IsLatestApprovedVersionOfClient_null_check
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, trim

# Initialize Spark session with schema auto-merge enabled
spark = SparkSession.builder.appName("DataFrameNullTest").getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

def log_to_catalog(dataframe_name, rule, columns, status, message):
    # Create a DataFrame for the log entry
    log_df = spark.createDataFrame(
        [(datetime.now(), dataframe_name, rule, columns, status, message)],
        ["timestamp", "dataframe_name", "validation_rule", "columns_checked", "status", "message"]
    )
    # Append the log entry to the log_catalog table
    log_df.write.mode("append").saveAsTable("testing.test_log_catalog")

def test_gic_IsLatestApprovedVersionOfClient_null_check(dataframe_name, columns, rule_name):
    try:
        # Load tables
        df_party = spark.table("party")
        df_party_cddcase = spark.table("party_cddcase")

        # Apply filter logic as per SQL
        filtered_party = df_party.filter(
            (col("application") == "GIC") &
            (col("IsLatestApprovedVersionOfClient").isNull())
        )

        # Join with party_cddcase
        joined_df = df_party_cddcase.join(
                    filtered_party.select("partyidentifier").distinct(),
                    on="partyidentifier",
                    how="inner")

        # Count matching records
        record_count = joined_df.count()

        # Prepare message and status
        if record_count == 0:
            status = "Pass"
            message = "All GIC parties that have caseids have non-null IsLatestApprovedVersionOfClient."
        else:
            status = "Fail"
            # Collect mismatched partyidentifiers
            mismatched_ids = joined_df.select("partyidentifier").distinct().collect()
            mismatched_ids_str = "\n".join([str(row["partyidentifier"]) for row in mismatched_ids])
            message = (
                f"Found {record_count} GIC records that have caseids but IsLatestApprovedVersionOfClient is null.\n"
                f"Mismatched partyidentifiers:\n{mismatched_ids_str}"
            )

        print(message)
        print(f"Status: {status}")

        # Log result
        log_to_catalog(dataframe_name, rule_name, ','.join(columns), status, message)

    except Exception as e:
        error_message = f"An error occurred: {e}"
        print(error_message)
        log_to_catalog(dataframe_name, rule_name, ','.join(columns), "Error", error_message)

# Calling the function
test_gic_IsLatestApprovedVersionOfClient_null_check(
    dataframe_name="party",
    columns=["IsLatestApprovedVersionOfClient"],
    rule_name="GIC_IsLatestApprovedVersionOfClient_Validation"
)

# COMMAND ----------

# DBTITLE 1,GIC Clients that dont have CDD details
df_Party=spark.table('Party')
df_Party_CDDCase_QuestionAnswer=spark.table('Party_CDDCase_QuestionAnswer')
find_missing_parties(df_Party, df_Party_CDDCase_QuestionAnswer,'Party','Party_CDDCase_QuestionAnswer', 'PartyIdentifier', 'GIC Clients that dont have CDD details',"IsLatestApprovedVersionOfClient is not null and application='GIC'")

# COMMAND ----------

# DBTITLE 1,GIC clients missing in Party_Structure but present in Party_CDDCase
df_Party_Structure=spark.table('Party_Structure')
df_Party_CDDCase = spark.table('Party_CDDCase')
find_missing_parties(df_Party_CDDCase, df_Party_Structure,'Party_CDDCase', 'Party_Structure', 'PartyIdentifier', 'GIC Clients that are present in Party_CDDCase but not in Party_Structure',"application='GIC'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executing Data Quality Checks

# COMMAND ----------

df_Party=spark.table('Party')
df_Party_CDDCase=spark.table('Party_CDDCase')
df_Party_SystemIdentifier=spark.table('Party_SystemIdentifier')
df_Party_CDDCase_QuestionAnswer=spark.table('Party_CDDCase_QuestionAnswer')
df_Party_Address=spark.table('Party_Address')
df_Party_Structure=spark.table('Party_Structure')
df_Party_Coverage=spark.table('Party_Coverage')
df_Party_CDDCase_RiskCategories=spark.table('Party_CDDCase_RiskCategories')
df_Party_Naics=spark.table('Party_Naics')
df_Party_CountryAffiliation=spark.table('Party_CountryAffiliation')
df_Party_Selection=spark.table('Party_Selection')
df_Party_Documents=spark.table('Party_Documents')
df_Party_ClientOwnership=spark.table('Party_ClientOwnership')

# COMMAND ----------

# DBTITLE 1,Clients that are present in Party_CDDCase_RiskCategories but not in Party_CDDCase_QuestionAnswer

# Step 1: Join df_Party_CDDCase_RiskCategories with df_Party to get lifecyclestatus
df_filtered = df_Party_CDDCase_RiskCategories.alias("risk") \
    .join(df_Party.select("PartyIdentifier", "lifecyclestatus").alias("party"),
          col("risk.PartyIdentifier") == col("party.PartyIdentifier"),
          "inner") \
    .filter(col("lifecyclestatus") != "Prospect") \
    .select("risk.*")  # Keep only original columns from df_Party_CDDCase_RiskCategories
# Step 2: Call your generic function with the filtered df
find_missing_parties(
    df_filtered,
    df_Party_CDDCase_QuestionAnswer,
    'Party_CDDCase_RiskCategories',
    'Party_CDDCase_QuestionAnswer',
    'PartyIdentifier',
    'Clients that are present in Party_CDDCase_RiskCategories but not in Party_CDDCase_QuestionAnswer'
)

# COMMAND ----------

#calling these functions
test_dataframe_uniqueness(df_Party, 'Party', ['PartyIdentifier','gcid'], 'GCID_uniqueness_check', "gcid is not null")
test_dataframe_uniqueness(df_Party, 'Party', ['PartyIdentifier'], 'PartyIdentifier_uniqueness_check_in_Party')
test_dataframe_uniqueness(df_Party_Address, 'Party_Address', ['PartyIdentifier','AddressType'], 'PartyIdentifier_uniqueness_check_in_Party_Address')
test_dataframe_uniqueness(df_Party_CDDCase, 'Party_CDDCase', ['PartyIdentifier','UniqueCaseId'], 'PartyIdentifier_UniqueCaseId_uniqueness_check_in_PartyCDDCase',"application<>'KN1'")
test_dataframe_uniqueness(df_Party_CountryAffiliation, 'Party_CountryAffiliation', ['NationalAffiliationType', 'CountryISOcode','PartyIdentifier'], 'unique_record_check_in_Party_CountryAffiliation')

test_dataframe_null_fields(df_Party, 'Party', ['FullLegalName'], 'FullLegalName_Null_check')
test_dataframe_null_fields(df_Party, 'Party', ['LifeCycleStatus'], 'LifeCycleStatus_Null_check', "party_type not in('Related Legal Entity','Related Natural Person')")
test_dataframe_null_fields(df_Party_CDDCase, 'Party_CDDCase', ['NextReviewDate'], 'NextReviewDate_Null_check', "CaseStatusName='Completed'")
test_dataframe_null_fields(df_Party, 'Party', ['gcid'], 'GCID_GCOB_Null_check', "party_type not in('Related Legal Entity','Related Natural Person') and GCOB_identifier is not null")

test_dataframe_null_fields(df_Party, 'Party', ['GlobalClientOwnerLocation'], 'GlobalClientOwnerLocation_Null_Check', "((GlobalClientOwnerLocation IS NULL) AND Party_type NOT IN ('Related Legal Entity','Related Natural Person'))")

test_dataframe_null_fields(df_Party_CDDCase, 'Party_CDDCase', ['CaseCompletedDate'], 'CaseCompletedDate_Null_check_CompletedCase', "CaseStatusName='Completed'")

test_dataframe_null_fields(df_Party_Coverage, 'Party_Coverage', ['CoverageValue'], 'GlobalClientownership_Null_Check', "CoverageType in ('Business Line','Location','GCO') and CoverageTypeDescription in ('GCO Location','Global Client Owner','') and CoverageValue is null and partyidentifier not like 'GCOB_R%'")

test_dataframe_null_fields(df_Party, 'Party', ['IsLatestApprovedVersionOfClient'], 'IsLatestApprovedVersionOfClient_Null_check', "party_type not in ('Related Legal Entity', 'Related Natural Person') and application not in ('GIC', 'GCDS')")

test_dataframe_null_fields(df_Party_CDDCase, 'Party_CDDCase', ['CaseStatusName'], 'CaseStatusName_Null_check')

test_dataframe_null_fields(df_Party, 'Party', ['FirstName','MiddleName','LastName'], 'FirstName_MiddleName_LastName_Null_Check', "PARTY_TYPE IN ('Legal Entity', 'Related Legal Entity') AND (FirstName IS NOT NULL OR MiddleName IS NOT NULL OR LastName IS NOT NULL)")

test_dataframe_null_fields(df_Party_Structure, 'Party_Structure', ['IsUbo'], 'IsUbo_Null_Check', "TypesOfRelation='UBO'")

test_dataframe_null_fields(df_Party, 'Party', ['Party_type'], 'PartyType_Null_check')
test_dataframe_null_fields(df_Party_SystemIdentifier, 'Party_SystemIdentifier', ['LocalSystemIdentifier'], 'LocalSystemIdentifier_Null_check')

test_dataframe_null_fields(df_Party_CountryAffiliation, 'Party_CountryAffiliation', ['PartyIdentifier'], 'PartyIdentifier_Null_check_in_Party_CountryAffiliation')
test_dataframe_null_fields(df_Party_CountryAffiliation, 'Party_CountryAffiliation', ['CountryISOcode'], 'CountryISOcode_Null_check')

test_dataframe_null_fields(df_Party_Selection, 'Party_Selection', ['PartyIdentifier'], 'PartyIdentifier_Null_check_in_Party_Selection')
test_dataframe_null_fields(df_Party_Selection, 'Party_Selection', ['SelectionValue'], 'BankCode_Null_check',"SelectionType='bank_code'")

test_dataframe_null_fields(df_Party_Documents, 'Party_Documents', ['PartyIdentifier'], 'PartyIdentifier_Null_check_in_Party_Documents')
test_dataframe_null_fields(df_Party_Documents, 'Party_Documents', ['DocumentId'], 'DocumentId_Null_check')

test_dataframe_uniqueness(df_Party_ClientOwnership, 'Party_ClientOwnership', ['PartyIdentifier','ClientOwnerType'], 'PartyIdentifier_uniqueness_check_in_Party_ClientOwnership',"ClientOwnerType='GlobalClientOwner'")
test_dataframe_null_fields(df_Party_ClientOwnership, 'Party_ClientOwnership', ['PartyIdentifier'], 'PartyIdentifier_Null_check_in_Party_ClientOwnership')
test_dataframe_null_fields(df_Party_ClientOwnership, 'Party_ClientOwnership', ['ClientOwnerUPN'], 'ClientOwnerUPN_Null_check')
test_dataframe_null_fields(df_Party_ClientOwnership, 'Party_ClientOwnership', ['ClientOwnerLocationCountryISOCode'], 'ClientOwnerLocationCountryISOCode_Null_check')

find_missing_parties(df_Party_SystemIdentifier, df_Party,'Party_SystemIdentifier','Party', 'PartyIdentifier', 'SIEBEL customers without a active record in GCDS', "application='SIEBEL' and partyidentifier like 'SIEBEL%'")

find_missing_parties(df_Party_SystemIdentifier, df_Party,'Party_SystemIdentifier','Party', 'PartyIdentifier', 'SIEBEL customers without a GCOB LocalSystemIdentifier', "application='SIEBEL' and partyidentifier like 'GCDS%'")

find_missing_parties(df_Party, df_Party_CDDCase_QuestionAnswer,'Party','Party_CDDCase_QuestionAnswer', 'PartyIdentifier', 'Clients that dont have CDD details',"party_type not in('Related Legal Entity','Related Natural Person') and IsLatestApprovedVersionOfClient is not null and application<>'GIC' and lifecyclestatus<>'Prospect'")

find_missing_parties(df_Party_CDDCase, df_Party,'Party_CDDCase','Party', 'PartyIdentifier', 'Clients that are present in Party_CDDCase but not in Party')
find_missing_parties(df_Party_CDDCase_QuestionAnswer, df_Party,'Party_CDDCase_QuestionAnswer','Party', 'PartyIdentifier', 'Clients that are present in Party_CDDCase_QuestionAnswer but not in Party')
find_missing_parties(df_Party_Address, df_Party,'Party_Address','Party', 'PartyIdentifier', 'Clients that are present in Party_Address but not in Party')
find_missing_parties(df_Party_Structure, df_Party,'Party_Structure','Party', 'PartyIdentifier', 'Clients that are present in Party_Structure but not in Party')
find_missing_parties(df_Party_CDDCase_RiskCategories, df_Party,'Party_CDDCase_RiskCategories','Party', 'PartyIdentifier', 'Clients that are present in Party_CDDCase_RiskCategories but not in Party')
find_missing_parties(df_Party_Naics, df_Party,'Party_Naics','Party', 'PartyIdentifier', 'Clients that are present in Party_Naics but not in Party')

find_missing_parties(df_Party_CDDCase_QuestionAnswer, df_Party_CDDCase,'Party_CDDCase_QuestionAnswer','Party_CDDCase', 'PartyIdentifier', 'Clients that are present in Party_CDDCase_QuestionAnswer but not in Party_cddcase')

find_missing_parties(df_Party_CDDCase,df_Party_Address,'Party_CDDCase','Party_Address', 'PartyIdentifier', 'Clients that are present in Party_CDDCase but not in Party_Address')

find_missing_parties(df_Party_Structure, df_Party_CDDCase,'Party_Structure','Party_CDDCase', 'PartyIdentifier', 'Clients that are present in Party_Structure but not in Party_CDDCase',"application<>'GIC'")

find_missing_parties(df_Party_CDDCase_RiskCategories, df_Party_CDDCase,'Party_CDDCase_RiskCategories','Party_CDDCase', 'PartyIdentifier', 'Clients that are present in Party_CDDCase_RiskCategories but not in Party_CDDCase')

find_missing_parties(df_Party, df_Party_CDDCase,'Party','Party_CDDCase', 'PartyIdentifier', 'Clients that are present in Party but not in Party_CDDCase', "party_type not in('Related Legal Entity','Related Natural Person','Related Party') and StatusName<>'Cancelled' and IsLatestApprovedVersionOfClient IS NOT NULL")

test_dataframe_condition(df_Party_CDDCase, 'Party_CDDCase', "FinalDecisionDate_Validation", "FinalDecisionDate for completed cases can't be greater than CaseCompletedDate (except amendment & change of client owner review type)", "casestatusname = 'Completed' AND ReviewTypeName NOT IN ('Amendment','Change of Client Owner') AND FinalDecisionDate > CaseCompletedDate")

#test_dataframe_condition(df_Party_SystemIdentifier, 'Party_SystemIdentifier', "GCOB Clients which dont have GCDS ID","All GCOB clients should have a connection from GCOB to GCDS","application='GCOB' and PartyIdentifier not like 'GCDS%' and PartyIdentifier not like 'GCOB_R%'") #modified script for GCID_GCOB_Null_check
