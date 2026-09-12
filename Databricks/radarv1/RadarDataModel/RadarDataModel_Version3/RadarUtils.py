## This file will contain generic functions that cover functionality for multiple notebooks / activities.

# Importing the required packages
import os
import re
from datetime import datetime,timedelta
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat_ws, regexp_extract, to_date, lit, date_format, row_number, input_file_name
from typing import Optional, Tuple, Dict, List
# PARALLEL LOADING with ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed


# Fetching environment variables
application_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']
environment=os.environ['ENV']

# Retrieving Client Secret from connected Secrets
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{application_id}")

def authenticate_storage_account(write_storage):
    # This method is for Configuring Spark to access SARADAR Storage account using OAuth authentication
    spark.conf.set("fs.azure.account.auth.type."+write_storage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+write_storage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+write_storage+".dfs.core.windows.net", ""+application_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+write_storage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+write_storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")

    print('Connection to ' + write_storage +' storage account is successful.')

    return 

def save_to_saradar_storage_account(df, object_name , version_number, environment, Load_Date=None):
    #This method will authenticate and save the dataframe to SARADAR Storage account in the right environment 
    saradar_write_storage = f'saradar{environment}'
    authenticate_storage_account(saradar_write_storage) 
    if Load_Date:
        EDL_LOAD_DTS = f'EDL_LOAD_DTS={datetime.strptime(Load_Date, "%Y%m%d").strftime("%Y%m%d")}'
    else:
        EDL_LOAD_DTS = f'EDL_LOAD_DTS={str(datetime.now().strftime("%Y%m%d"))}'
    saradar_container = 'radardatamodel'

    from pyspark.sql import functions as F
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(
                col_name,
                F.when(
                    (F.trim(F.col(col_name)) == "") |
                    (F.lower(F.trim(F.col(col_name))) == "null"),
                    F.lit(None)
                    ).otherwise(F.col(col_name))
        )
    df_count = df.count()

    saradar_path = 'abfss://'+saradar_container+'@'+saradar_write_storage+'.dfs.core.windows.net/'
    target_folder_path = object_name+'/'+str(version_number)+'/data/'+EDL_LOAD_DTS+'/'
    
    print('The destination path is: ', saradar_path + target_folder_path)
    
    df.repartition(1).write.format("parquet").mode("overwrite").option("compression", "snappy").save(saradar_path + target_folder_path)
    
    print('\033[1m' + object_name + '\033[0m' +  ' data object stored successfully. \nNumber of rows written:', df_count)  

    return 


def fetch_latest_file(Object, Date, Storage, Container='gcob', Dataversion=None, Legacy2=False, CaseService=False, RISKMODEL=False):
    # Function that returns latest version dynamically for Defined Data Objects

    base_path = f'abfss://{Container}@{Storage}.dfs.core.windows.net/'
    path_suffix = 'Legacy2/' if Legacy2 else 'CaseService/' if CaseService else 'RISKMODEL/' if RISKMODEL else ''

    # Fetch the latest DataVersion
    if Dataversion is None:
        all_versions = []
        versionFiles = dbutils.fs.ls(f'{base_path}/{path_suffix}/{Object}/')

        for file in versionFiles:
            all_versions.append(re.split("/", file.name)[0])
        # Condition check to bring only numbers in list
        all_versions = [int(item) for item in all_versions if item.isdigit()]
        current_version = str(max(all_versions))
    else:
        current_version = Dataversion

    #To find if the file exists for today and extract load_dts
    currentVersionPath = dbutils.fs.ls(f'{base_path}/{path_suffix}/{Object}/{current_version}/data/')

    getLoad_dts = [file for file in currentVersionPath if Date in file.name][0]
    load_dts = re.split("/", getLoad_dts.name)[0] + '*'

    final_path = f'{base_path}/{path_suffix}/{Object}/{current_version}/data/{load_dts}/*.parquet'


    return spark.read.parquet(final_path).createOrReplaceTempView(Object)

def add_party_identifier(Actual_Dataobject_df, Party_SystemIdentifier_df):
    from pyspark.sql.functions import concat_ws
    #Derive LocalSystemIdentifier to join with other object
    Party_SystemIdentifier_df = Party_SystemIdentifier_df.withColumn("DerivedLocalSystemIdentifier",concat_ws("_", Party_SystemIdentifier_df["Application"], Party_SystemIdentifier_df["LocalSystemIdentifier"])
    )

    # Perform left join
    joined_df = Actual_Dataobject_df.join(
        Party_SystemIdentifier_df.select("DerivedLocalSystemIdentifier", "PartyIdentifier"),
        on=[Actual_Dataobject_df.LocalSystemIdentifier == Party_SystemIdentifier_df.DerivedLocalSystemIdentifier],
        how="left"
    )

    # Drop the column from Actual_Dataobject_df
    Actual_Dataobject_df_cleaned = Actual_Dataobject_df.drop("LocalSystemIdentifier")

    
    # Add only PartyIdentifier column to original CDD_Cases DataFrame
    result_df = joined_df.select(col("PartyIdentifier"), *Actual_Dataobject_df_cleaned.columns)

    return result_df

def Read_GDP_Defined_DataObjects(Source , Dataobject, path_prefix='',Load_Date='',Dataversion=None):
    
    if Load_Date=='':
        historical_load='No'
    else:
        historical_load='Yes'

    from pyspark.sql.functions import col, regexp_extract, to_date, desc

    print(f"{Source}: Processing \033[1m{Dataobject}\033[0m")
    # Determine Load_Date based on Source
    today = datetime.today()
    if Source in ['GCOB', 'Legacy2', 'GIC', 'KN1', 'Siebel', 'RadarDataModel']:
        if Load_Date=='':
            Load_Date = today.strftime('%Y%m%d')
        else:
            Load_Date = datetime.strptime(Load_Date, '%Y%m%d').strftime('%Y%m%d')
    elif Source == 'GCDS':
        if Load_Date=='':
            # load_date = today - timedelta(days=1) if today.weekday() == 6 else today
            Load_Date = today.strftime('%Y%m%d')
        else:
            Load_Date = datetime.strptime(Load_Date, '%Y%m%d').strftime('%Y%m%d')
            # Load_Date = Load_Date - timedelta(days=1) if today.weekday() == 6 else Load_Date
    elif Source == 'NLSVF':
        if Load_Date=='':
            Load_Date = (today - timedelta(days=2)).strftime('%Y-%m-%d')
            Load_Date = (today - timedelta(days=2)).strftime('%Y-%m-%d')
        else:
            Load_Date = datetime.strptime(Load_Date, '%Y%m%d').strftime('%Y-%m-%d')
            Load_Date = (Load_Date - timedelta(days=2)).strftime('%Y-%m-%d')
    elif Source == 'EBX':
        if Load_Date=='':
            Load_Date = (today - timedelta(days=1)).strftime('%Y%m%d')
        else:
            Load_Date = datetime.strptime(Load_Date, '%Y%m%d')
            Load_Date = (Load_Date - timedelta(days=1)).strftime('%Y%m%d')        
    else:
        raise ValueError(f"Unknown source '{Source}' for Load_Date logic.")

    # Read storage account names from environment
    EU_GDP_Defined_Storage_Account = os.environ['GDP_STORAGE_NAME']
    SA_GDP_Defined_Storage_Account = os.environ['GDP_SA_STORAGE_NAME']
    NA_GDP_Defined_Storage_Account = os.environ['GDP_NA_STORAGE_NAME']


    # Map sources to containers
    container_map = {
        'GCDS': 'gcds',
        'GCOB': 'gcob',
        'Legacy2': 'gcob',
        'GIC': 'gic',
        'KN1': 'kn1',
        'NLSVF': 'nlsvf',
        'Siebel': 'siebel-idaa-cdf',
        'RadarDataModel' : 'fec-radar',
        'EBX' :'ebx'
    }

    # Map storage accounts to sources
    storage_sources_map = {
        EU_GDP_Defined_Storage_Account: ['GCDS', 'GCOB', 'Legacy2', 'Siebel','RadarDataModel','EBX'] , 
        SA_GDP_Defined_Storage_Account: ['GIC', 'KN1'],
        NA_GDP_Defined_Storage_Account: ['NLSVF']
    }

    # Determine the correct storage account for the source
    Storage_Account = next((account for account, sources in storage_sources_map.items() if Source in sources), None)
    if not Storage_Account:
        raise ValueError(f"Source '{Source}' not mapped to any known storage account.")

    # Determine the correct container
    Container = container_map.get(Source)
    if not Container:
        raise ValueError(f"Source '{Source}' not mapped to any known container.")

    # Build the base path
    base_path = f"abfss://{Container}@{Storage_Account}.dfs.core.windows.net/{path_prefix}/{Dataobject}"
    # Define cutoff date
    cutoff_date = datetime(2025, 7, 15)

    # Current date (or pass your load date dynamically)
    current_date = datetime.strptime(Load_Date, '%Y%m%d')

    # Determine GCDS version dynamically
    gcds_version = 4601 if current_date < cutoff_date else 4602
    
    if Dataversion is not None:
    #New Logic for versioning extends on previous logic to include v4801
        gcds_version = (4801 if current_date >= datetime(2026, 6, 23) 
                        else 4602 if current_date >= datetime(2025, 7, 15)
                        else 4601)

    # Build version mapping
    version_mapping_by_source = {
        'Siebel': {
            3: ['cdf_ggm_rel_x_ar_hist'],
            2: ['cdf_ggm_org_hist', 'cdf_ggm_ar_hist', 'cdf_ggm_rel_x_rel_hist', 'cdf_ggm_np_hist']
        },

        'EBX': {
            1: ['EBX_ISO3166COUNTRYCODES']
        },
        'GCDS': {
            gcds_version: [
                'client_Client', 'client_KeyStoreKey', 'client_PartyRole', 'client_PartytoPartyRelationship',
                'client_ClientOwnersProduct', 'client_ClientOwnersLocal', 'client_OnboardedLocations',
                'client_RMA', 'client_Products'
            ]
        }
    }    
    try:
        # List files in the base path
        files = dbutils.fs.ls(base_path)

        # Determine version
        if Source in version_mapping_by_source:
            source_versions = version_mapping_by_source[Source]
            version = next((v for v, objs in source_versions.items() if Dataobject in objs),None)
            if version is None:
                raise ValueError(f"{Source} Dataobject '{Dataobject}' not found in version mapping.")
        else:
            version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
        print(f"Selected version : {version}")

        # Detect partition folder containing Load_Date
        partition_base_path = f"{base_path}/{version}/data/"
        partition_folders = dbutils.fs.ls(partition_base_path)
        # matching_partition = next((file.name for file in partition_folders if Load_Date in file.name),None)

        partition_folders_df = spark.createDataFrame(partition_folders)

        row = partition_folders_df.filter(col("name") != "_delta_log/") \
            .withColumn("ymd", regexp_extract(col("name"), r"=(\d{8})", 1)) \
            .filter(col("ymd") != "") \
            .withColumn("folder_date", to_date(col("ymd"), "yyyyMMdd")) \
            .filter(col("folder_date").isNotNull()) \
            .filter(col("folder_date") == to_date(lit(Load_Date), "yyyyMMdd")) \
            .head(1)

        if row:
            matching_partition = row[0]["name"]
            print("matching_partition=",matching_partition)
            partition_available="Yes"
        else:
            matching_partition = None
            partition_available="No"
            print("No valid matching_partition found.")

            if Source in ('GIC','KN1'):
                latest_partition = (partition_folders_df
                                    .filter(col("name") != "_delta_log/")
                                    .withColumn("ymd", regexp_extract(col("name"), r"=(\d{8})", 1))
                                    .filter(col("ymd") != "")
                                    .withColumn("folder_date", to_date(col("ymd"), "yyyyMMdd"))
                                    .filter(col("folder_date").isNotNull())
                                    .orderBy(desc("folder_date"))
                                    .head(1))
                matching_partition = latest_partition[0]["name"]
                print("matching_partition=",matching_partition)
                partition_available ="No"

        if Source == 'GCDS' and matching_partition is None:
            previous_date = (datetime.strptime(Load_Date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')
            matching_partition = next((file.name for file in partition_folders if previous_date in file.name),None)
            latest_dt = None

        #     for file in partition_folders:
        #         folder_date_str = file.name[9:17]
        #         folder_dt = datetime.strptime(folder_date_str, '%Y%m%d')
        #         current_dt = datetime.strptime(Load_Date, '%Y%m%d')
        #         if folder_dt <= current_dt:
        #             if latest_dt is None or folder_dt > latest_dt:
        #                 latest_dt = folder_dt
        #                 matching_partition = file.name

        # # The below lines will try to read t-1/t-2 data when a particular day's data is not available from Source for History Load
        # if Load_Date!='':
        #     if matching_partition is None:
        #         previous_date = (datetime.strptime(Load_Date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')
        #         matching_partition = next((file.name for file in partition_folders if previous_date in file.name),None)

        #     if matching_partition is None:
        #         previous_date = (datetime.strptime(Load_Date, '%Y%m%d') - timedelta(days=2)).strftime('%Y%m%d')
        #         matching_partition = next((file.name for file in partition_folders if previous_date in file.name),None)
        
        if not matching_partition:
            raise RuntimeError(
                f"\nPartition file not found for,\n"
                f"Source: {Source}\n"
                f"Load_Date: {Load_Date}\n"
                f"DataObjectName: {Dataobject}\n"
                f"Path: {base_path}\n"
        )
        full_path = f"{partition_base_path}{matching_partition}"
        print(f"Reading from path: {full_path}")


        # Read data using appropriate format
        if Source == 'Siebel' or (Source=='GCOB' and version==103):
            spark.read.format('delta').load(full_path).createOrReplaceTempView(Dataobject)
        elif  (historical_load=='Yes' and partition_available == "No") and ((Source in ('GIC','KN1') ) or (Source=='RadarDataModel' and path_prefix=='RadarPowerApps')):
            print("GIC/KN1/RadarPowerApps not available")
            spark.read.parquet(f"{full_path}/*.parquet").filter("1 = 0").createOrReplaceTempView(Dataobject)
        else:
            spark.read.parquet(f"{full_path}/*.parquet").createOrReplaceTempView(Dataobject)

        # Print row count
        row_count = spark.sql(f"SELECT COUNT(*) FROM {Dataobject}").collect()[0][0]
        print(f"No. of rows read : {row_count}")
        print(f"{Dataobject} read successfully\n")

    except Exception as e:
        raise RuntimeError(f"Error reading {Dataobject} from {Source}: {e}")
    

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
            if version is None:
                raise ValueError(f"{Source} Dataobject '{Dataobject}' not found in version mapping.")
 
        print(f"Selected version : {version}")
 
        # Detect partition folder containing Load_Date
        partition_base_path = f"{base_path}/{version}/data/"
 
        print(f"Reading from path: {partition_base_path}")
 
        df_ranz=spark.read.format('delta').load(partition_base_path)
       
        # Detect primary key
        pk_column = None
        for pk, table_list in Primary_Key_dict.items():
            if Dataobject in table_list:
                pk_column = pk
                break
 
        if pk_column is None:
            raise ValueError(f"No primary key found for Dataobject {Dataobject} in Primary_Key_dict")
 
        print(f"Using Primary Key for window: {pk_column}")
 
        df_ranz.withColumn("rn", row_number().over(Window.partitionBy(pk_column).orderBy(col("EDL_ACT_DTS").desc()))).filter(col("rn") == 1).drop("rn").createOrReplaceTempView(Dataobject)
 
        # Print row count
        row_count = spark.sql(f"SELECT COUNT(*) FROM {Dataobject}").collect()[0][0]
        print(f"No. of rows read : {row_count}")
        print(f"{Dataobject} read successfully\n")
 
    except Exception as e:
        raise RuntimeError(f"Error reading {Dataobject} from {Source}: {e}")

# ================================================================================
# RANZ VERSION 4 RDM Data Loader with Auto-Merge - OPTIMIZED
# ================================================================================
# ========== CONSTANTS ==========
CUTOFF_DATE = datetime.strptime('20260412', '%Y%m%d')
ONGOING_START = datetime.strptime('20260413', '%Y%m%d')
HISTORICAL_START = '2025-01-01'

# Data freshness validation (LIVE mode only, bypassed for lookup tables)
MAX_DAYS_OLD = 1

# Primary key mapping - TABLE NAME WITH SUFFIX -> LIST OF COLUMNS
# Supports both single and composite primary keys
PRIMARY_KEY_MAP = {
    # Regular tables - LIVE (_xref) variants use single key
    "c_b_business_line_xref": ["ROWID_XREF"],
    "c_b_contract_xref": ["ROWID_XREF"],
    "c_b_contr_party_dd_xref": ["ROWID_XREF"],
    "c_b_contr_rol_party_xref": ["ROWID_XREF"],
    "c_b_due_diligence_xref": ["ROWID_XREF"],
    "c_b_individual_xref": ["ROWID_OBJECT"],  # Special case
    "c_b_omb_login_xref": ["ROWID_XREF"],
    "c_b_organisation_xref": ["ROWID_XREF"],
    "c_b_party_dom_cntry_xref": ["ROWID_XREF"],
    "c_b_party_identifier_xref": ["ROWID_XREF"],
    "c_b_party_naics_xref": ["ROWID_XREF"],
    "c_b_party_occupation_xref": ["ROWID_XREF"],
    "c_b_party_pep_am_xref": ["ROWID_XREF"],
    "c_b_party_rel_addr_xref": ["ROWID_XREF"],
    "c_b_party_rel_party_xref": ["ROWID_XREF"],
    "c_b_party_xref": ["SRC_PARTY_ID"],
    
    # Regular tables - HISTORY (_hxrf) variants use composite key
    "c_b_contract_hxrf": ["ROWID_XREF"],
    "c_b_contr_party_dd_hxrf": ["ROWID_XREF"],
    "c_b_contr_rol_party_hxrf": ["ROWID_XREF"],
    "c_b_due_diligence_hxrf": ["ROWID_XREF"],
    "c_b_individual_hxrf": ["ROWID_XREF"],
    "c_b_omb_login_hxrf": ["ROWID_XREF"],
    "c_b_organisation_hxrf": ["ROWID_XREF"],
    "c_b_party_dom_cntry_hxrf": ["ROWID_XREF"],
    "c_b_party_hxrf": ["SRC_PARTY_ID"],
    "c_b_party_identifier_hxrf": ["ROWID_XREF"],
    "c_b_party_naics_hxrf": ["ROWID_XREF"],
    "c_b_party_occupation_hxrf": ["ROWID_XREF"],
    "c_b_party_pep_am_hxrf": ["ROWID_XREF"],
    "c_b_party_rel_addr_hxrf": ["ROWID_XREF"],
    "c_b_party_rel_party_hxrf": ["ROWID_XREF"],
    
    # Lookup tables - all use single ROWID_XREF
    "c_lkp_am_eval_xref": ["ROWID_XREF"],
    "c_lkp_cdd_risk_rating_xref": ["ROWID_XREF"],
    "c_lkp_client_life_cycle_xref": ["ROWID_XREF"],
    "c_lkp_contract_type_xref": ["ROWID_XREF"],
    "c_lkp_country_xref": ["ROWID_XREF"],
    "c_lkp_group_branch_xref": ["ROWID_XREF"],
    "c_lkp_legal_entity_type_xref": ["ROWID_XREF"],
    "c_lkp_naics_sector_xref": ["ROWID_XREF"],
    "c_lkp_naics_xref": ["ROWID_XREF"],
    "c_lkp_occupation_xref": ["ROWID_XREF"],
    "c_lkp_party_type_code_xref": ["ROWID_XREF"],
    "c_lkp_pep_eval_xref": ["ROWID_XREF"],
    "c_lkp_pep_type_xref": ["ROWID_XREF"],
    "c_lkp_prty_relt_rol_type_xref": ["ROWID_XREF"],
    "c_rbo_rel_type_xref": ["ROWID_XREF"],
}

FORCE_LIVE_TABLES = ["c_b_business_line_xref", "c_rbo_rel_type_xref"]

#print(f"[OK] PRIMARY_KEY_MAP loaded with {len(PRIMARY_KEY_MAP)} table mappings")
#print(f"   - {sum(1 for v in PRIMARY_KEY_MAP.values() if len(v) == 1)} tables with single primary key")
#print(f"   - {sum(1 for v in PRIMARY_KEY_MAP.values() if len(v) > 1)} tables with composite primary key")

# ========== HELPER FUNCTIONS ==========
def get_base_table_name(table_name: str) -> str:
    """Extract base table name by removing _xref or _hxrf suffix"""
    return table_name.lower().replace('_xref', '').replace('_hxrf', '')

def get_primary_key(table_name: str) -> Optional[List[str]]:
    """Get primary key column(s) for a table (supports composite keys)
    
    Args:
        table_name: Full table name WITH suffix (_xref or _hxrf)
    
    Returns:
        List of primary key column names, or None if not found
    """
    table_lower = table_name.lower()
    return PRIMARY_KEY_MAP.get(table_lower, None)

def deduplicate_dataframe(df: DataFrame, table_name: str, verbose: bool = False) -> DataFrame:
    """Deduplicate DataFrame by primary key(s) + EDL_ACT_DTS
    
    Supports both single and composite primary keys.
    For composite keys, deduplication uses all key columns together.
    
    Args:
        verbose: If False, skips count operations for better performance (default: True)
    """
    pk_columns = get_primary_key(table_name)
    
    # Skip deduplication if no primary key defined or EDL_ACT_DTS missing
    if not pk_columns or 'EDL_ACT_DTS' not in df.columns:
        return df
    
    # Verify all primary key columns exist in DataFrame
    missing_cols = [col_name for col_name in pk_columns if col_name not in df.columns]
    if missing_cols:
        if verbose:
            print(f"[WARNING] Primary key columns missing: {missing_cols}. Skipping deduplication.")
        return df
    
    # Create window spec with all primary key columns (handles both single and composite)
    window_spec = Window.partitionBy(*pk_columns).orderBy(col("EDL_ACT_DTS").desc())
    df_deduped = df.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")
    
    # Only count if verbose mode enabled (performance optimization)
    if verbose:
        before_count = df.count()
        after_count = df_deduped.count()
        if before_count != after_count:
            pk_display = ", ".join(pk_columns)
            print(f"Deduplication by [{pk_display}]: {before_count:,} -> {after_count:,} rows (removed {before_count - after_count:,} duplicates)")
    
    return df_deduped

def align_schemas(df1: DataFrame, df2: DataFrame) -> tuple:
    """Align schemas of two DataFrames by adding missing columns"""
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    
    if cols1 == cols2:
        return df1, df2
    
    all_cols = sorted(cols1.union(cols2))
    
    # Build column maps once
    cols_to_add_df1 = {c: lit(None) for c in all_cols if c not in cols1}
    cols_to_add_df2 = {c: lit(None) for c in all_cols if c not in cols2}
    
    # Add missing columns using withColumns
    if cols_to_add_df1:
        df1 = df1.withColumns(cols_to_add_df1)
    if cols_to_add_df2:
        df2 = df2.withColumns(cols_to_add_df2)
    
    return df1.select(all_cols), df2.select(all_cols)

def build_base_path(table_name: str) -> str:
    """Build base path for table storage"""
    storage_account = os.environ.get('AU_GDP_Defined_Storage_Account')
    if not storage_account:
        raise ValueError("Environment variable 'AU_GDP_Defined_Storage_Account' not set")
    return f"abfss://mdm-ranz@{storage_account}.dfs.core.windows.net/{table_name}"

def validate_table_exists(table_name: str) -> tuple:
    """Validate if a table exists by checking its base path"""
    try:
        base_path = build_base_path(table_name)
        data_path = f"{base_path}/4/data/"
        dbutils.fs.ls(data_path)
        return (True, None)
    except Exception as e:
        error_msg = str(e)
        if "FileNotFoundException" in error_msg or "does not exist" in error_msg.lower():
            return (False, f"Table path does not exist: {table_name}")
        else:
            return (False, f"Error accessing table: {error_msg}")

def find_available_partitions(base_path: str) -> List[Dict]:
    """Find all available partitions for a table"""
    partitions = []
    data_path = f"{base_path}/4/data/"
    
    try:
        items = dbutils.fs.ls(data_path)
        for item in items:
            if item.isDir():
                partition_name = item.name.rstrip('/')
                match = re.search(r'LOADED_DTS=(\d{8})T\d{6}Z', partition_name)
                if match:
                    date_str = match.group(1)
                    date_obj = datetime.strptime(date_str, '%Y%m%d')
                    partitions.append({
                        'path': f"{data_path}{partition_name}/",
                        'date': date_obj,
                        'date_str': date_str
                    })
        partitions.sort(key=lambda x: x['date'])
    except Exception as e:
        print(f"Error finding partitions: {e}")
    
    return partitions

def is_lookup_table(table_name: str) -> bool:
    """Check if table is a lookup table by prefix (works with or without suffix)"""
    return table_name.lower().startswith("c_lkp_")

# ========== PARTITION SELECTION ==========
def select_live_partitions(partitions: List[Dict], requested_date: datetime, table_name: str) -> List[Dict]:
    """Select all partitions from ONGOING_START to T-1 for multi-partition reads
    
    For LOOKUP tables: Selects ALL partitions regardless of requested_date (ignores date filtering)
    For LIVE tables: Selects partitions from ONGOING_START to requested_date - 1
    """
    # For LOOKUP tables: return ALL partitions (no date filtering)
    if is_lookup_table(table_name):
        if not partitions:
            raise ValueError(f"No partitions found for lookup table {table_name}")
        
        print(f"LOOKUP mode: Selected {len(partitions)} partitions (ALL available) from {partitions[0]['date_str']} to {partitions[-1]['date_str']}")
        return partitions
    
    # For LIVE tables: filter by date range
    t_minus_1 = requested_date - timedelta(days=1)
    selected = [p for p in partitions if ONGOING_START <= p['date'] <= t_minus_1]
    
    if not selected:
        raise ValueError(f"No partitions found between {ONGOING_START.strftime('%Y%m%d')} and {t_minus_1.strftime('%Y%m%d')}")
    
    # FRESHNESS CHECK (only for LIVE tables)
    latest_partition = selected[-1]
    days_diff = (t_minus_1 - latest_partition['date']).days
    
    if days_diff > MAX_DAYS_OLD:
        raise ValueError(
            f"Data freshness check FAILED: Latest partition is {days_diff} days old "
            f"(max allowed: {MAX_DAYS_OLD}). Expected partition up to "
            f"{t_minus_1.strftime('%Y%m%d')}, but latest is {latest_partition['date_str']}"
        )
    
    print(f"LIVE mode: Selected {len(selected)} partitions from {selected[0]['date_str']} to {selected[-1]['date_str']}")
    return selected

def find_closest_partition(partitions: List[Dict], requested_date: datetime, read_mode: str, table_name: str) -> Optional[Dict]:
    """Find closest partition on or before requested date (freshness check only for LIVE mode)
    
    Note: This function should NOT be called for lookup tables in normal operation.
    Lookup tables always use multi-partition reads (ALL partitions) via select_live_partitions().
    
    For regular tables: Returns partition on or before requested date with freshness checks
    """
    # DEFENSIVE: Lookup tables should never reach this function (they use multi-partition reads)
    if is_lookup_table(table_name):
        if not partitions:
            raise ValueError(f"No partitions found for lookup table {table_name}")
        
        latest_partition = sorted(partitions, key=lambda x: x['date'])[-1]
        print(f"[WARNING] Lookup table reached find_closest_partition() - this should not happen!")
        print(f"[WARNING] Lookup tables should use multi-partition reads (ALL partitions)")
        print(f"[FALLBACK] Using latest partition as safety fallback: {latest_partition['date_str']}")
        return latest_partition
    
    # Regular tables: find partition on or before requested date
    valid_partitions = [p for p in partitions if p['date'] <= requested_date]
    if not valid_partitions:
        raise ValueError(f"No partition found on or before {requested_date.strftime('%Y%m%d')}")
    
    selected_partition = sorted(valid_partitions, key=lambda x: x['date'])[-1]
    days_diff = (requested_date - selected_partition['date']).days
    
    # FRESHNESS CHECK - Only for LIVE mode and non-lookup tables
    if read_mode == 'LIVE' and days_diff > MAX_DAYS_OLD:
        raise ValueError(
            f"Data freshness check FAILED: Latest partition is {days_diff} days old (max allowed: {MAX_DAYS_OLD}). "
            f"Requested date: {requested_date.strftime('%Y%m%d')}, Latest partition: {selected_partition['date_str']}"
        )
    
    if days_diff > 0:
        if read_mode == 'LIVE':
            print(f"Using partition from {days_diff} day(s) before requested date (within tolerance)")
        else:
            print(f"Using partition from {days_diff} day(s) before requested date")
    
    return selected_partition

# ========== DATA READING (OPTIMIZED) ==========
def read_multiple_partitions_optimized(base_path: str, table_name: str, start_date: datetime, end_date: datetime, verbose: bool = False) -> DataFrame:
    """Read multiple partitions efficiently using single directory read
 
   
    Args:
        base_path: Base path to table storage
        table_name: Table name for deduplication
        start_date: Start date for filtering (inclusive)
        end_date: End date for filtering (inclusive)
        verbose: If False, skips count operations for better performance
    """
    if verbose:
        print(f"Reading data from {start_date.strftime('%Y%m%d')} to {end_date.strftime('%Y%m%d')}...")
    
    # Read entire /data directory at once
    # Delta Lake automatically handles all partitions
    data_path = f"{base_path}/4/data/"
    df = spark.read.format('delta').load(data_path)
    
    # Extract partition date from file path for filtering    
    df = df.withColumn(
        "_partition_date",
        regexp_extract(input_file_name(), r"LOADED_DTS=(\d{8})", 1)
    )
    
    # Filter to date range (converts string to date for comparison)
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    
    df_filtered = df.filter(
        (col("_partition_date") >= start_str) & 
        (col("_partition_date") <= end_str)
    ).drop("_partition_date")
    
    # Only count if verbose mode enabled
    if verbose:
        total_rows = df_filtered.count()
        print(f"Loaded {total_rows:,} rows")
    
    # Deduplicate
    df_deduped = deduplicate_dataframe(df_filtered, table_name, verbose)
    
    if verbose:
        final_count = df_deduped.count()
        print(f"Final row count after deduplication: {final_count:,}")
    
    return df_deduped

def read_all_partitions_optimized(base_path: str, table_name: str, verbose: bool = False) -> DataFrame:
    """Read ALL partitions efficiently for lookup tables
    
    MAJOR OPTIMIZATION: Reads entire /data directory at once.
    
    Args:
        base_path: Base path to table storage
        table_name: Table name for deduplication
        verbose: If False, skips count operations for better performance
    """
    if verbose:
        print(f"Reading all partitions (lookup table)...")
    
    # Read entire /data directory at once
    data_path = f"{base_path}/4/data/"
    df = spark.read.format('delta').load(data_path)
    
    # Only count if verbose mode enabled
    if verbose:
        total_rows = df.count()
        print(f"Loaded {total_rows:,} rows from all partitions")
    
    # Deduplicate
    df_deduped = deduplicate_dataframe(df, table_name, verbose)
    
    if verbose:
        final_count = df_deduped.count()
        print(f"Final row count after deduplication: {final_count:,}")
    
    return df_deduped

def apply_history_date_filter(df: DataFrame, requested_datetime: datetime) -> DataFrame:
    """Apply HIST_CREATE_DATE filtering for HISTORY tables"""
    if 'HIST_CREATE_DATE' not in df.columns:
        return df
    
    if requested_datetime <= CUTOFF_DATE:
        end_date = requested_datetime.strftime('%Y-%m-%d')
        print(f"HISTORY mode (one-time): Filtering HIST_CREATE_DATE from {HISTORICAL_START} to {end_date}")
        return df.filter((col('HIST_CREATE_DATE') >= HISTORICAL_START) & (col('HIST_CREATE_DATE') <= end_date))
    else:
        hist_end = CUTOFF_DATE.strftime('%Y-%m-%d')
        df_historical = df.filter((col('HIST_CREATE_DATE') >= HISTORICAL_START) & (col('HIST_CREATE_DATE') <= hist_end))
        
        t_minus_1 = requested_datetime - timedelta(days=1)
        live_start = ONGOING_START.strftime('%Y-%m-%d')
        live_end = t_minus_1.strftime('%Y-%m-%d') if t_minus_1 >= ONGOING_START else live_start
        
        df_live = df.filter((col('HIST_CREATE_DATE') >= live_start) & (col('HIST_CREATE_DATE') <= live_end))
        print(f"HISTORY mode (ongoing): Historical + Live segments merged")
        return df_historical.union(df_live)

def read_single_partition(partition: Dict, table_name: str, read_mode: str, requested_date: str, verbose: bool = False) -> DataFrame:
    """Read single partition with optional filtering
    
    Note: Lookup tables should NEVER reach this function - they use multi-partition reads.
    
    Args:
        verbose: If False, skips count operations for better performance (default: True)
    """
    df = spark.read.format('delta').load(partition['path'])
    
    if verbose:
        initial_count = df.count()
        print(f"Initial row count: {initial_count:,}")
    
    # DEFENSIVE: Lookup tables should use multi-partition reads, not single partition
    if read_mode == 'LOOKUP':
        if verbose:
            print(f"[WARNING] LOOKUP table in single-partition read - this should not happen!")
            print(f"[FALLBACK] Reading complete snapshot from partition (no filtering)")
    elif read_mode == 'HISTORY':
        requested_datetime = datetime.strptime(requested_date, '%Y%m%d')
        df = apply_history_date_filter(df, requested_datetime)
    else:
        if verbose:
            print(f"LIVE mode: Reading complete snapshot from partition")
    
    df_deduped = deduplicate_dataframe(df, table_name, verbose)
    
    if verbose:
        final_count = df_deduped.count()
        print(f"Final row count: {final_count:,}")
    
    return df_deduped

# ========== MAIN READ FUNCTION ==========
def read_ranz_v4_data(table_name: str, requested_date: str, verbose: bool = False) -> DataFrame:
    """Main entry point to read RANZ V4 data
    
    Read strategies by table type:
    - LOOKUP tables (c_lkp_*): ALWAYS read ALL partitions (multi-partition union)
    - LIVE tables (_xref): Multi-partition read from ONGOING_START to T-1
    - HISTORY tables (_hxrf): Single partition with date filtering
    
    Args:
        verbose: If False, minimal logging and skips count operations for better performance (default: True)
    """
    if verbose:
        print(f"\n{'='*80}")
        print(f"RANZ-MDM: Processing {table_name} for date {requested_date}")
        print(f"{'='*80}")
    
    if not table_name or not requested_date:
        raise ValueError("Table name and date are mandatory")
    if not re.match(r'^\d{8}$', requested_date):
        raise ValueError(f"Invalid date format: {requested_date}. Expected YYYYMMDD")
    
    requested_datetime = datetime.strptime(requested_date, '%Y%m%d')
    
    # Determine read mode (check for lookup tables first)
    if is_lookup_table(table_name):
        read_mode = 'LOOKUP'
    else:
        table_lower = table_name.lower()
        is_live = table_lower.endswith('_xref') or table_lower in FORCE_LIVE_TABLES
        read_mode = 'LIVE' if is_live else 'HISTORY'
    
    if verbose:
        print(f"Read mode: {read_mode}")
    
    base_path = build_base_path(table_name)
    available_partitions = find_available_partitions(base_path)
    
    if not available_partitions:
        raise ValueError(f"No data found for table {table_name}")
    if verbose:
        print(f"Available partitions: {len(available_partitions)} found")
    
    # Read data based on mode and date
    if read_mode == 'LOOKUP':
        # Read all partitions at once for lookup tables
        if verbose:
            print(f"LOOKUP mode: Reading ALL partitions")
        return read_all_partitions_optimized(base_path, table_name, verbose)
    
    elif read_mode == 'LIVE' and requested_datetime >= ONGOING_START:
        # Read date range at once instead of individual partitions
        t_minus_1 = requested_datetime - timedelta(days=1)
        
        # Validate partitions exist in range
        selected = [p for p in available_partitions if ONGOING_START <= p['date'] <= t_minus_1]
        if not selected:
            raise ValueError(f"No partitions found between {ONGOING_START.strftime('%Y%m%d')} and {t_minus_1.strftime('%Y%m%d')}")
        
        # FRESHNESS CHECK
        latest_partition = selected[-1]
        days_diff = (t_minus_1 - latest_partition['date']).days
        if days_diff > MAX_DAYS_OLD:
            raise ValueError(
                f"Data freshness check FAILED: Latest partition is {days_diff} days old "
                f"(max allowed: {MAX_DAYS_OLD}). Expected partition up to "
                f"{t_minus_1.strftime('%Y%m%d')}, but latest is {latest_partition['date_str']}"
            )
        
        if verbose:
            print(f"LIVE mode: Reading {len(selected)} partitions from {selected[0]['date_str']} to {selected[-1]['date_str']}")
        
        # Read entire date range at once (much faster than individual reads)
        return read_multiple_partitions_optimized(base_path, table_name, ONGOING_START, t_minus_1, verbose)
    else:
        selected_partition = find_closest_partition(available_partitions, requested_datetime, read_mode, table_name)
        if verbose:
            print(f"Selected partition date: {selected_partition['date_str']}")
        return read_single_partition(selected_partition, table_name, read_mode, requested_date, verbose)

# ========== MERGE FUNCTION ==========
def merge_live_and_history(base_table_name: str, requested_date: str, verbose: bool = False) -> DataFrame:
    """Merge LIVE and HISTORY tables, or read lookup table
    
    For LOOKUP tables (c_lkp_*): Reads only _xref variant (ALL partitions)
    For regular tables (c_b_*): Merges _xref (LIVE) and _hxrf (HISTORY) variants
    
    Args:
        verbose: If False, minimal logging and skips count operations for better performance (default: True)
    """
    if verbose:
        print(f"\n{'='*80}")
        print(f"PROCESSING: {base_table_name}")
        print(f"{'='*80}")
    
    if is_lookup_table(base_table_name):
        live_table = f"{base_table_name}_xref"
        if verbose:
            print(f"Lookup table detected - reading {live_table}")
        df = read_ranz_v4_data(live_table, requested_date, verbose)
        return df
    
    if verbose:
        print(f"Regular table detected - merging LIVE and HISTORY variants")
    live_table = f"{base_table_name}_xref"
    history_table = f"{base_table_name}_hxrf"
    
    df_live, df_history = None, None
    
    try:
        df_live = read_ranz_v4_data(live_table, requested_date, verbose)
    except Exception as e:
        if verbose:
            print(f"LIVE table failed: {e}")
    
    try:
        df_history = read_ranz_v4_data(history_table, requested_date, verbose)
    except Exception as e:
        if verbose:
            print(f"HISTORY table failed: {e}")
    
    if df_live is None and df_history is None:
        raise ValueError("Both tables failed")
    if df_live is None:
        return df_history
    if df_history is None:
        return df_live
    
    df_live, df_history = align_schemas(df_live, df_history)
    df_merged = df_live.unionByName(df_history, allowMissingColumns=True)
    
    # Use LIVE table name (_xref) for primary key lookup
    pk_columns = get_primary_key(live_table)
    
    if pk_columns:
        if verbose:
            pk_display = ", ".join(pk_columns)
            print(f"Deduplicating merged data using primary key(s): {pk_display}")
        df_merged = deduplicate_dataframe(df_merged, live_table, verbose)
    else:
        if verbose:
            print(f"No deduplication applied - primary key not found for {live_table}")
    
    return df_merged

# ========== HELPER FUNCTION FOR PARALLEL LOADING ==========
def load_single_table_parallel(base_name: str, load_date: str, verbose: bool = False) -> dict:
    """Load a single table (thread-safe for parallel execution)
    
    Args:
        base_name: Base table name (without _xref or _hxrf suffix)
        load_date: Date string in 'YYYYMMDD' format
        verbose: If False, minimal logging for better performance
    
    Returns:
        Dictionary with table name, status, rows, and optional error
    """
    try:
        # Load and merge LIVE + HISTORY
        df_merged = merge_live_and_history(base_name, load_date, verbose)

        
        
        #Applied to Fetch Only Active Records from Ranz object based on the recommendations which was provided by RANZ team. 
        from pyspark.sql.functions import col, coalesce, lit

        def apply_ranz_filters(df, table_name):

            name = table_name.lower()
            # Apply HUB_STATE_IND filter only for tables that contain the column
            if name!="c_b_individual_birth_dt":
                df = df.filter(col("HUB_STATE_IND") == 1)

            if name == "c_b_party":

                # Replace NULL with AC in the actual column
                df = df.withColumn("PARTY_STATUS_CD",coalesce(col("PARTY_STATUS_CD"), lit("AC")))

                return df.filter(
                    col("SRC_SYS_CD").isin("T24RURAL", "OMB", "CMS", "RABODIRECT")
                    &
                    ~col("PARTY_STATUS_CD").isin("DL", "RD", "WD")
                )

            elif name == "c_b_contract":

                # Replace NULL with AC in the actual column
                df = df.withColumn("LIFECYCLE_STATUS_CD",coalesce(col("LIFECYCLE_STATUS_CD"), lit("AC")))

                return df.filter(
                    ((col("SRC_SYS_CD").isin("T24RURAL"))|(col("SRC_SYS_CD").isin("RABODIRECT")|(col("LOB_CD") == "RD")))
                    &
                    (~col("LIFECYCLE_STATUS_CD").isin("DL", "RD", "WD"))
                )

            return df
        
        df_merged = apply_ranz_filters(df_merged, base_name)
        
        # Create temp view
        df_merged.createOrReplaceTempView(base_name)
        
        # Count rows (only once, at the end)
        row_count = df_merged.count()
        
        # Success
        print(f"[SUCCESS] {base_name}: {row_count:,} rows")
        return {'table': base_name, 'status': 'SUCCESS', 'rows': row_count}
        
    except Exception as e:
        error_msg = str(e)
        print(f"[FAILED] {base_name}: {error_msg}")
        if verbose:
            import traceback
            traceback.print_exc()
        return {'table': base_name, 'status': 'FAILED', 'error': error_msg}

# ========== MAIN LOAD FUNCTION (PARALLEL OPTIMIZED) ==========
def load_ranz_v4_rdm_tables(load_df: list, load_date: str, skip_invalid: bool = False, verbose: bool = False):
    """
    Load RANZ V4 RDM tables with automatic LIVE and HISTORY merging.  
    
    Args:
        load_df: List of table names (can include _xref or _hxrf suffixes)
        load_date: Date string in 'YYYYMMDD' format (e.g., '20260520')
        skip_invalid: If True, skip invalid tables and continue; if False, fail fast (default: False)
        verbose: If False, minimal logging and skips count operations for better performance (default: True)
    
    Returns:
        List of result dictionaries with status and row counts    

    """
    # max_workers to 3 for optimal performance
    max_workers = 3
    
    print(f"\n{'='*80}")
    mode = "PARALLEL" if not verbose else "PARALLEL + DEBUG"
    print(f"LOADING RDM DATA OBJECTS - Load Date: {load_date} ({mode} MODE)")
    print(f"Workers: {max_workers} parallel threads")
    print(f"{'='*80}")
    
    # ========== VALIDATION PHASE ==========
    if verbose:
        print(f"\n[VALIDATION] Checking {len(load_df)} table(s)...")
    
    validation_results = {}
    invalid_tables = []
    
    for dataobject in load_df:
        base_name = get_base_table_name(dataobject)
        is_lookup = is_lookup_table(base_name)
        
        if is_lookup:
            live_table = f"{base_name}_xref"
            live_exists, live_error = validate_table_exists(live_table)
            
            validation_results[base_name] = {
                'live_exists': live_exists,
                'live_error': live_error,
                'is_lookup': True
            }
            
            if not live_exists:
                invalid_tables.append({
                    'table': base_name,
                    'live_table': live_table,
                    'live_error': live_error,
                    'is_lookup': True
                })
                if verbose:
                    print(f"  [X] {base_name}: INVALID ({live_table} does not exist)")
            else:
                if verbose:
                    print(f"  [OK] {base_name}: Valid (lookup table: {live_table} [OK])")
        else:
            live_table = f"{base_name}_xref"
            history_table = f"{base_name}_hxrf"
            
            live_exists, live_error = validate_table_exists(live_table)
            history_exists, history_error = validate_table_exists(history_table)
            
            validation_results[base_name] = {
                'live_exists': live_exists,
                'history_exists': history_exists,
                'live_error': live_error,
                'history_error': history_error,
                'is_lookup': False
            }
            
            if not live_exists and not history_exists:
                invalid_tables.append({
                    'table': base_name,
                    'live_table': live_table,
                    'history_table': history_table,
                    'live_error': live_error,
                    'history_error': history_error,
                    'is_lookup': False
                })
                if verbose:
                    print(f"  [X] {base_name}: INVALID (neither {live_table} nor {history_table} exist)")
            else:
                if verbose:
                    status_parts = []
                    if live_exists:
                        status_parts.append(f"{live_table} [OK]")
                    if history_exists:
                        status_parts.append(f"{history_table} [OK]")
                    print(f"  [OK] {base_name}: Valid ({', '.join(status_parts)})")
    
    if invalid_tables:
        print(f"\n[ERROR] Found {len(invalid_tables)} invalid table(s). Set skip_invalid=True to continue.")
        
        if not skip_invalid:
            raise ValueError(f"Invalid tables found: {[t['table'] for t in invalid_tables]}")
        else:
            print(f"[WARNING] Skipping {len(invalid_tables)} invalid table(s)...")
    else:
        print(f"[VALIDATION] All {len(load_df)} table(s) are valid!")
    
    print(f"\n[PROCESSING] Starting data load...")
    print()
    
    # Get unique base names (avoid duplicates from _xref/_hxrf variants)
    processed = set()
    tables_to_load = []
    for dataobject in load_df:
        base_name = get_base_table_name(dataobject)
        if base_name not in processed:
            # Skip invalid tables if requested
            if skip_invalid and base_name in [t['table'] for t in invalid_tables]:
                print(f"[SKIPPED] {base_name}")
                continue
            tables_to_load.append(base_name)
            processed.add(base_name)  

    
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_table = {
            executor.submit(load_single_table_parallel, base_name, load_date, verbose): base_name
            for base_name in tables_to_load
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"[FAILED] {table_name}: {str(e)}")
                results.append({'table': table_name, 'status': 'FAILED', 'error': str(e)})
    
    return results
