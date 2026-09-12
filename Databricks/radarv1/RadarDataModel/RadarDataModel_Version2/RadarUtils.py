## This file will contain generic functions that cover functionality for multiple notebooks / activities.

# Importing the required packages
import os
import re
from datetime import datetime,timedelta
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat_ws, regexp_extract, to_date, lit, date_format, row_number



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

def Read_GDP_Defined_DataObjects(Source , Dataobject, path_prefix='',Load_Date=''):

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
        else:
            Load_Date = datetime.strptime(Load_Date, '%Y%m%d').strftime('%Y-%m-%d')
            Load_Date = (Load_Date - timedelta(days=2)).strftime('%Y-%m-%d')
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
        'RadarDataModel' : 'fec-radar'
    }

    # Map storage accounts to sources
    storage_sources_map = {
        EU_GDP_Defined_Storage_Account: ['GCDS', 'GCOB', 'Legacy2', 'Siebel','RadarDataModel'] , 
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

    # Build version mapping
    version_mapping_by_source = {
        'Siebel': {
            3: ['cdf_ggm_rel_x_ar_hist'],
            2: ['cdf_ggm_org_hist', 'cdf_ggm_ar_hist', 'cdf_ggm_rel_x_rel_hist', 'cdf_ggm_np_hist']
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
        else:
            matching_partition = None
            print("No valid matching_partition found.")

        # if Source == 'GCDS' and matching_partition is None:
        #     # previous_date = (datetime.strptime(Load_Date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')
        #     # matching_partition = next((file.name for file in partition_folders if previous_date in file.name),None)
        #     latest_dt = None

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
