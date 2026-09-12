# Databricks notebook source
import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

storage_accnt = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(storage_accnt)

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'Party_RiskModelInstanceQuestionAnswers'
, 'Party_RiskModelCategories'
, 'Party_RiskModelInstance'
, 'party_WRCDDModel'
]

# Create TempView for each loading table
for Object in load_df:
    fetch_latest_file(Object, date_parameter, storage_accnt, RISKMODEL=True)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details',
'party_AllPartyDetails',
'party_client_structure_GUI',
'party_business_activities',
'party_workitem',
'party_products_and_services'
#CaseService/party_local_client_Owners,
# CaseService/party_trade_name
# CaseService/party_control_measures,
# CaseService/party_documents,
# CaseService/party_tax_info_fatca_crs,
# CaseService/party_LocalRequirement,
# CaseService/party_structure_Questionnaire,
# CaseService/party_client_identifiers,
]

# Create TempView for each loading table
for Object in load_df:
    fetch_latest_file(Object, date_parameter, storage_accnt, CaseService=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC CREATE TABLE IF NOT EXISTS testing.GCOB_Producer_test_Log (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   timestamp TIMESTAMP,
# MAGIC   dataobject_name STRING,
# MAGIC   test_scenario STRING, ---check
# MAGIC   columns_checked STRING,
# MAGIC   status STRING,
# MAGIC   message STRING   
# MAGIC );

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime

# Initialize Spark session with schema auto-merge enabled
spark = SparkSession.builder.appName("GCOBProducerValidationLogger").getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

def log_to_catalog(dataframe_name, rule, columns, status, message):
    # Create a DataFrame for the log entry
    log_df = spark.createDataFrame(
        [(datetime.now(), dataframe_name, rule, columns, status, message)],
        ["timestamp", "dataobject_name", "test_scenario", "columns_checked", "status", "message"]       
    ) 
    # Append the log entry to the log_catalog table
    log_df.write.mode("append").saveAsTable("testing.GCOB_Producer_test_Log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uniqueness check

# COMMAND ----------

def test_dataobject_uniqueness(df, dataframe_name, columns, rule_name, id_column=None, where_condition=None):
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

            # Dynamically handle the ID column if provided
            if id_column and id_column in df.columns:
                duplicate_ids = duplicates_df.select(id_column).limit(10).collect()
                duplicate_ids_str = "\n".join([str(row[id_column]) for row in duplicate_ids])
                message = (
                    f"Column(s) {columns} contains {duplicates_count} duplicate values. "
                    f"Few problematic {id_column}: {duplicate_ids_str}"
                )
            else:
                message = f"Column(s) {columns} contains {duplicates_count} duplicate values."

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
# MAGIC ## Null Value check

# COMMAND ----------

def test_dataobject_null_fields(df, dataframe_name, columns, rule_name, id_column=None, where_condition=None):
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
                #Collect problematic Gcobid values
                problematic_id = df.filter(df[column].isNull()).select(id_column).limit(10).collect()
                problematic_id_str = "\n".join([str(row[id_column]) for row in problematic_id])
                message = f"Column(s) {columns} contains {null_count} null values. Few Problematic UniqueGcobIds: {problematic_id_str}"

            print(message)
            print(f"Status: {status}")
        # Log the result to the catalog table
        log_to_catalog(dataframe_name, rule_name, ','.join(columns), status, message)
    except Exception as e:
        error_message = f"An error occurred: {e}"
        print(error_message)
        log_to_catalog(dataframe_name, rule_name, ','.join(columns), "Error", error_message)

# COMMAND ----------

df_party_case_client_details=spark.table('party_case_client_details')
df_party_client_structure_GUI=spark.table('party_client_structure_GUI')
df_party_AllPartyDetails=spark.table('party_AllPartyDetails')
df_party_business_activities=spark.table('party_business_activities')
df_party_workitem=spark.table('party_workitem')
df_Party_RiskModelInstanceQuestionAnswers=spark.table('Party_RiskModelInstanceQuestionAnswers')
df_Party_RiskModelCategories=spark.table('Party_RiskModelCategories')
df_Party_RiskModelInstance=spark.table('Party_RiskModelInstance')
df_party_WRCDDModel=spark.table('party_WRCDDModel')

# COMMAND ----------

# DBTITLE 1,party_case_client_details
test_dataobject_uniqueness(df_party_case_client_details, 'party_case_client_details', ['UniqueGcobId','CaseId'], 'GcobId_uniqueness_check_in_Party_case_client_details','UniqueGcobId')
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['FullLegalName'], 'FullLegalName_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['Gcobid'], 'GcobId_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['ClientId'], 'ClientId_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['UniqueGcobId'], 'UniqueGcobId_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['CaseId'], 'CaseId_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['ReviewTypeName'], 'ReviewTypeName_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['CaseStatusName'], 'CaseStatusName_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['ClientLifeCycleName'], 'ClientLifeCycleName_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['FIHubIndicator'], 'FIHubIndicator_Null_check_GCOB','UniqueGcobId',"ClientType='Legal Entity'")
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['CaseCompletedDate'], 'CaseCompletedDate_Null_check_GCOB','UniqueGcobId',"CaseStatusName='Completed'")
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['ClientOwnerSignOffDate'], 'ClientOwnerSignOffDate_Null_check_GCOB','UniqueGcobId',"CaseStatusName='Completed' and ReviewTypeName NOT IN ('Product Offboarding','Product Offboarding (Resume)','Amendment','Change of Client Owner','Event Assessment')")
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['ValidatedRiskLevel'], 'ValidatedRiskLevel_Null_check_GCOB','UniqueGcobId',"CaseStatusName='Completed'") #sourceside####
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['CountryOfRegistration'], 'CountryOfRegistration_Null_check_GCOB','UniqueGcobId',"CaseStatusName='Completed'")
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['RiskModelName'], 'RiskModelName_Null_check_GCOB','UniqueGcobId',"CaseStatusName='Completed'") #Sourceside only blank
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['EntityTypeRiskLevel'], 'EntityTypeRiskLevel_Null_check_GCOB_LE','UniqueGcobId',"CaseStatusName='Completed' and ClientType='Legal Entity'and RiskModelName not in ('Rabobank Subsidiaries Risk Model')")  #Sourceside only blank/undefined
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['EntityTypeRiskLevel'], 'EntityTypeRiskLevel_Null_check_GCOB_NP','UniqueGcobId',"CaseStatusName='Completed' and ClientType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person') and RiskModelName in ('Corporate Client Risk Model 2020 (Phase 2) (OnBoarding)', 'Corporate Client Risk Model 2020 (Phase 2) (Review)', 'NPPC Risk Model 2023 (Onboarding)', 'NPPC Risk Model 2023 (Review)')") 
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['StructureRiskLevel'], 'StructureRiskLevel_Null_check_GCOB_LE','UniqueGcobId',"CaseStatusName='Completed' and ClientType='Legal Entity'and RiskModelName not in ('Rabobank Subsidiaries Risk Model')")  #Sourceside only blank/undefined
test_dataobject_null_fields(df_party_case_client_details, 'party_case_client_details', ['StructureRiskLevel'], 'StructureRiskLevel_Null_check_GCOB_NP','UniqueGcobId',"CaseStatusName='Completed' and ClientType in ('Natural Person acting in a Professional Capacity (NPPC)','Natural Person') and RiskModelName in ('Corporate Client Risk Model 2020 (Phase 2) (OnBoarding)', 'Corporate Client Risk Model 2020 (Phase 2) (Review)', 'NPPC Risk Model 2023 (Onboarding)', 'NPPC Risk Model 2023 (Review)')") 

# COMMAND ----------

# DBTITLE 1,party_client_structure_GUI
test_dataobject_null_fields(df_party_client_structure_GUI, 'party_client_structure_GUI', ['ClientFullLegalName'], 'ClientFullLegalName_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_client_structure_GUI, 'party_client_structure_GUI', ['ClientLifeCycleName'], 'ClientLifeCycleName_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_client_structure_GUI, 'party_client_structure_GUI', ['ChildIdentity'], 'ChildIdentity_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_client_structure_GUI, 'party_client_structure_GUI', ['ChildIdentityName'], 'ChildIdentityName_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_client_structure_GUI, 'party_client_structure_GUI', ['ChildType'], 'ChildType_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_client_structure_GUI, 'party_client_structure_GUI', ['ClientCountryOfRegistration'], 'ClientCountryOfRegistration_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_client_structure_GUI, 'party_client_structure_GUI', ['ClientCountryOfRegistration'], 'ClientCountryOfRegistration_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_client_structure_GUI, 'party_client_structure_GUI', ['ReviewTypeName'], 'ReviewTypeName_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_client_structure_GUI, 'party_client_structure_GUI', ['CaseStatusName'], 'CaseStatusName_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_client_structure_GUI, 'party_client_structure_GUI', ['ParentIdentity'], 'ParentIdentity_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_client_structure_GUI, 'party_client_structure_GUI', ['ParentIdentityName'], 'ParentIdentityName_Null_check_GCOB','UniqueGcobId')
test_dataobject_null_fields(df_party_client_structure_GUI, 'party_client_structure_GUI', ['ParentType'], 'ParentType_Null_check_GCOB','UniqueGcobId')

# COMMAND ----------

# DBTITLE 1,Do not display anything above Directorship/Authorised Representative except other Directorship/Authorised/Other

# ---------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------
SOURCE_TABLE = "party_client_structure_GUI"
DATAFRAME_NAME = "party_client_structure_GUI"
RULE_NAME = "Do not display anything above Directorship/Authorised Representative except other Directorship/Authorised/Other"
COLUMNS_CHECKED = "ClientGcobId, ClientCaseId, ChildIdentity, ParentIdentity, TypesOfRelation"

# ---------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------
df = spark.table(SOURCE_TABLE)

# ---------------------------------------------------------------------
# Step 1: Exclude parents that have ANY relation other than Directorship or Authorised Representative
# ---------------------------------------------------------------------
allowed_relations = ["Directorship", "Authorised Representative"]

# Find parents that have disallowed relations
parents_with_disallowed = (
    df.filter(~col("TypesOfRelation").isin(*allowed_relations))
      .select("ParentIdentity")
      .distinct()
)

# Keep only parents that have ONLY allowed relations
dir_auth_parents = (
    df.filter(col("TypesOfRelation").isin(*allowed_relations))
      .join(parents_with_disallowed, "ParentIdentity", "left_anti")  # exclude disallowed parents
      .select(
          col("ParentIdentity").alias("DirectAuthParentIdentity"),
          col("ClientStructureSnapshotId").alias("DirectAuthClientStructureSnapshotId"),
          col("ClientGcobId").alias("DirectAuthGcobId"),
          col("ClientCaseId").alias("DirectAuthClientCaseId")
      )
      .distinct()
)

# ---------------------------------------------------------------------
# Step 2: Find all relations where ChildIdentity == ParentIdentity from step 1
# ---------------------------------------------------------------------
upstream_relations = df.join(
    dir_auth_parents,
    (df.ChildIdentity == dir_auth_parents.DirectAuthParentIdentity) &
    (df.ClientStructureSnapshotId == dir_auth_parents.DirectAuthClientStructureSnapshotId) &
    (df.ClientGcobId == dir_auth_parents.DirectAuthGcobId) &
    (df.ClientCaseId == dir_auth_parents.DirectAuthClientCaseId),
    "inner"
)

# ---------------------------------------------------------------------
# Step 3: Violations where relation is NOT in allowed set
# ---------------------------------------------------------------------
allowed_relations_extended = ["Directorship", "Authorised Representative", "Other"]
violations_df = upstream_relations.filter(~col("TypesOfRelation").isin(*allowed_relations_extended))

violation_count = violations_df.count()

# ---------------------------------------------------------------------
# Build log message
# ---------------------------------------------------------------------
if violation_count == 0:
    status = "Pass"
    message = f"Validation passed. No violations found."
else:
    status = "Fail"
    sample_rows = violations_df.select(
        "ClientGcobId",
        "ClientCaseId",
        "ClientStructureSnapshotId",
        "ChildIdentity",
        "ParentIdentity",
        "TypesOfRelation"
    ).limit(5).collect()

    sample_text = "; ".join([
        f"ClientGcobId={r['ClientGcobId']}, ClientCaseId={r['ClientCaseId']}, "
        f"ClientStructureSnapshotId={r['ClientStructureSnapshotId']}, ChildIdentity={r['ChildIdentity']}, "
        f"ParentIdentity={r['ParentIdentity']}, TypesOfRelation={r['TypesOfRelation']}"
        for r in sample_rows
    ])

    message = (
        f"Validation failed. Violations found: {violation_count}. "
        f"Sample Violations: {sample_text}"
    )

# ---------------------------------------------------------------------
# Log outcome
# ---------------------------------------------------------------------
log_to_catalog(DATAFRAME_NAME, RULE_NAME, COLUMNS_CHECKED, status, message)


# COMMAND ----------

# DBTITLE 1,party_AllPartyDetails
test_dataobject_uniqueness(df_party_AllPartyDetails, 'party_AllPartyDetails', ['UniquePartyId','CaseId','Status','Citizenship','Nationality','PartyId'], 'GcobId_uniqueness_check_in_party_AllPartyDetails','UniquePartyId')
test_dataobject_null_fields(df_party_AllPartyDetails, 'party_AllPartyDetails', ['GcobId'], 'GcobId_Null_check_GCOB','UniquePartyId')
test_dataobject_null_fields(df_party_AllPartyDetails, 'party_AllPartyDetails', ['FullLegalName'], 'FullLegalName_Null_check_GCOB','UniquePartyId')
test_dataobject_null_fields(df_party_AllPartyDetails, 'party_AllPartyDetails', ['ClientLifeCycleStatus'], 'ClientLifeCycleStatus_Null_check_GCOB','UniquePartyId', "ClientType not in('RelatedNaturalPerson','RelatedLegalEntity')")
test_dataobject_null_fields(df_party_AllPartyDetails, 'party_AllPartyDetails', ['UniquePartyId'], 'UniquePartyId_Null_check_GCOB','UniquePartyId')
test_dataobject_null_fields(df_party_AllPartyDetails, 'party_AllPartyDetails', ['CaseStatusName'], 'CaseStatusName_Null_check_GCOB','UniquePartyId',"ClientType not in('RelatedNaturalPerson','RelatedLegalEntity')")

# COMMAND ----------

# DBTITLE 1,party_business_activities
test_dataobject_uniqueness(df_party_business_activities, 'party_business_activities', ['SourceClient','NaicsCode'], 'SourceClient_uniqueness_check_in_party_business_activities','SourceClient')
test_dataobject_null_fields(df_party_business_activities, 'party_business_activities', ['ClientId'], 'ClientId_Null_check_GCOB','SourceClient')
test_dataobject_null_fields(df_party_business_activities, 'party_business_activities', ['NaicsCode'], 'NaicsCode_Null_check_GCOB','SourceClient')
test_dataobject_null_fields(df_party_business_activities, 'party_business_activities', ['NaicsName'], 'NaicsName_Null_check_GCOB','SourceClient')
test_dataobject_null_fields(df_party_business_activities, 'party_business_activities', ['SectorGroup'], 'SectorGroup_Null_check_GCOB','SourceClient')
test_dataobject_null_fields(df_party_business_activities, 'party_business_activities', ['CaseStatusName'], 'SectorGroup_Null_check_GCOB','SourceClient')
test_dataobject_null_fields(df_party_business_activities, 'party_business_activities', ['SourceClient'], 'SourceClient_Null_check_GCOB','SourceClient')


# COMMAND ----------

# DBTITLE 1,party_workitem
test_dataobject_uniqueness(df_party_workitem, 'party_workitem', ['SourceClient','WorkItemId'], 'SourceClient_uniqueness_check_in_party_workitem','SourceClient')
test_dataobject_null_fields(df_party_workitem, 'party_workitem', ['GcobId'], 'GcobId_Null_check_GCOB','SourceClient')
test_dataobject_null_fields(df_party_workitem, 'party_workitem', ['WorkItemId'], 'WorkItemId_Null_check_GCOB','SourceClient')
test_dataobject_null_fields(df_party_workitem, 'party_workitem', ['SourceClient'], 'SourceClient_Null_check_GCOB','SourceClient')
test_dataobject_null_fields(df_party_workitem, 'party_workitem', ['ClientId'], 'ClientId_Null_check_GCOB','SourceClient')
test_dataobject_null_fields(df_party_workitem, 'party_workitem', ['CaseStatusName'], 'CaseStatusName_Null_check_GCOB','SourceClient')
test_dataobject_null_fields(df_party_workitem, 'party_workitem', ['ReviewTypeName'], 'ReviewTypeName_Null_check_GCOB','SourceClient')
test_dataobject_null_fields(df_party_workitem, 'party_workitem', ['ClientLifeCycleName'], 'ClientLifeCycleName_Null_check_GCOB','SourceClient')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Finding missing clients in riskmodel data objects

# COMMAND ----------


def find_missing_clients(df1, df2, dataframe_name1, dataframe_name2, columns, rule_name, where_condition=None):
    try:
        # Apply where condition if provided
        if where_condition:
            df1 = df1.filter(where_condition)
        
        # Perform a left join to find values in df1 not present in df2
        joined_df = df1.join(df2, df1[columns] == df2[columns], 'left')
         
        # Find rows where clients in df2 is null
        missing_clients_df = joined_df.filter(df2[columns].isNull())
        
        # Check if there are any missing values
        if missing_clients_df.count() == 0:
            status = "Pass"
            message = f"All {columns} values in {dataframe_name1} are present in {dataframe_name2} dataobject."
        else:
            status = "Fail"
            missing_count = missing_clients_df.count()
            # Collect all the missing clients
            missing_details = missing_clients_df.select(df1[columns]).distinct().limit(10).collect()
            missing_info = "\n".join([str(row[columns]) for row in missing_details])
            message = (
                f"There are {missing_count} {columns} values in {dataframe_name1} "
                f"that are not present in {dataframe_name2} dataobject. The details are:\n{missing_info}"
            )

        print(message)
        print(f"Status: {status}")
        # Log the result to the catalog table
        log_to_catalog(dataframe_name2, rule_name, columns, status, message)
    except Exception as e:
        error_message = f"An error occurred: {e}"
        print(error_message)
        log_to_catalog(dataframe_name2, rule_name, columns, "Error", error_message)


# COMMAND ----------

# DBTITLE 1,RiskModel data Objects
test_dataobject_null_fields(df_Party_RiskModelInstanceQuestionAnswers, 'Party_RiskModelInstanceQuestionAnswers', ['InstanceId'], 'InstanceId_Null_check_Party_RiskModelInstanceQuestionAnswers','SourceClient')
test_dataobject_null_fields(df_Party_RiskModelCategories, 'Party_RiskModelCategories', ['InstanceId'], 'InstanceId_Null_check_Party_RiskModelCategories','SourceClient')

find_missing_clients(df_Party_RiskModelInstanceQuestionAnswers, df_party_case_client_details,'Party_RiskModelInstanceQuestionAnswers','party_case_client_details', 'sourceclient', 'Clients that are present in Party_RiskModelInstanceQuestionAnswers but not in party_case_client_details',"sourceclient is not null")

find_missing_clients(df_Party_RiskModelCategories, df_party_case_client_details,'Party_RiskModelCategories','party_case_client_details', 'sourceclient', 'Clients that are present in Party_RiskModelCategories but not in party_case_client_details',"sourceclient is not null")

find_missing_clients(df_Party_RiskModelInstance, df_party_case_client_details,'Party_RiskModelInstance','party_case_client_details', 'sourceclient', 'Clients that are present in Party_RiskModelInstance but not in party_case_client_details',"sourceclient is not null")

test_dataobject_null_fields(df_Party_RiskModelInstance, 'Party_RiskModelInstance', ['RiskModelName'], 'RiskModelName_Null_check_Party_RiskModelInstance','SourceClient')
test_dataobject_null_fields(df_Party_RiskModelInstance, 'Party_RiskModelInstance', ['InstanceId'], 'InstanceId_Null_check_Party_RiskModelInstance','SourceClient')
test_dataobject_null_fields(df_party_WRCDDModel, 'party_WRCDDModel', ['InstanceId'], 'InstanceId_Null_check_party_WRCDDModel','SourceClient')
test_dataobject_null_fields(df_party_WRCDDModel, 'party_WRCDDModel', ['ModelName'], 'ModelName_Null_check_party_WRCDDModel','SourceClient')
find_missing_clients(df_party_WRCDDModel, df_party_case_client_details,'party_WRCDDModel','party_case_client_details', 'sourceclient', 'Clients that are present in party_WRCDDModel but not in party_case_client_details',"sourceclient is not null")
