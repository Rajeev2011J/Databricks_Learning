## This file will contain generic functions that cover functionality for multiple notebooks / activities.

# Import Required Functions
import os
import re
from datetime import datetime,timedelta
from pyspark.sql import SparkSession


# required for authenticate part
from databricks.sdk.runtime import dbutils, spark

# Fetching environment variables
application_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']
environment=os.environ['ENV']

# Retrieving Client Secret from connected Secrets
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{application_id}")

# def CallAMSAPI():
    # pass

def authenticate_storage_account(write_storage):
    service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{application_id}")
    # This method is for Configuring Spark to access SARADAR Storage account using OAuth authentication
    spark.conf.set("fs.azure.account.auth.type."+write_storage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+write_storage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+write_storage+".dfs.core.windows.net", ""+application_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+write_storage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+write_storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")

    print('Connection to ' + write_storage +' storage account is successful.')

    return

def save_to_saradar_storage_account(df, object_name):
    # This method will authenticate and save the dataframe to SARADAR Storage account in the right environment 
    saradar_write_storage = f'saradar{environment}'
    authenticate_storage_account(saradar_write_storage)

    saradar_container = 'radarconsumerdata'
    saradar_path = f'abfss://{saradar_container}@{saradar_write_storage}.dfs.core.windows.net/EXTERNAL_TABLE/{object_name}'

    print('The destination path is: ', saradar_path)

    try:
        df.write.mode("overwrite") \
            .format('parquet') \
            .option('path', saradar_path) \
            .save()
        print('\033[1m' + object_name + '\033[0m' + ' data object stored successfully.')
    except Exception as e:
        print(f"Failed to write data to storage: {e}")
        return

    spark.sql("CREATE SCHEMA IF NOT EXISTS `hive_metastore`.`radar`")

    if spark.catalog.tableExists(f"radar.{object_name}"):
        spark.sql(f"DROP TABLE IF EXISTS `hive_metastore`.`radar`.{object_name}")
        print(f"Dropped table: {object_name} Successfully")

    df = spark.read.format('parquet').load(saradar_path)

    df.write \
        .mode("overwrite") \
        .format('parquet') \
        .option('path', f'{saradar_container}/EXTERNAL_TABLE/{object_name}') \
        .saveAsTable(f"hive_metastore.radar.{object_name}")

    print('\033[1m' + object_name + '\033[0m' + ' table written successfully.')


def fetch_latest_file(Object, Date, Storage, Container='gcob', Dataversion=None, Legacy2=False, CaseService=False, RISKMODEL=False, RadarDataModel=False):
    # Function that returns latest version dynamically for Defined Data Objects

    base_path = f'abfss://{Container}@{Storage}.dfs.core.windows.net/'
    path_suffix = 'Legacy2/' if Legacy2 else 'CaseService/' if CaseService else 'RISKMODEL/' if RISKMODEL else 'RadarDataModel/' if RadarDataModel else ''

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

def Load_from_Databricks_to_Synapse_Dedicated_SQL_Pool(df, TableName , tempDirectory, Mode_of_writing):
        # Authenticate to Storage account
        saradar_write_storage = f'saradar{environment}'
        authenticate_storage_account(saradar_write_storage)
        # Set the Azure Data Lake Storage Gen2 account key for authentication
        spark.conf.set("fs.azure.account.key."+saradar_write_storage+".dfs.core.windows.net","")
        # Write the DataFrame to Azure Synapse Dedicated SQL Pool using the Databricks connector
        df.write \
        .format("com.databricks.spark.sqldw") \
        .option("url", "jdbc:sqlserver://asafecreportprd.sql.azuresynapse.net:1433;database=reportingdwhprd;") \
        .option("enableServicePrincipalAuth","true") \
        .option("forwardSparkAzureStorageCredentials", "true") \
        .option("tempDir", "abfss://synapse@"+saradar_write_storage+".dfs.core.windows.net/"+tempDirectory+"/") \
        .option("dbtable", TableName) \
        .mode(Mode_of_writing) \
        .save()
        print("Data insertion into Synapse Dedicated SQL Pool completed for table: " + TableName +"\nNo. of Rows Written: "+str(df.count()))

def Read_GDP_Defined_DataObjects(Source , Dataobject, path_prefix=''):

    print(f"{Source}: Processing \033[1m{Dataobject}\033[0m")
    # Determine Load_Date based on Source
    today = datetime.today()
    if Source in ['GCOB', 'Legacy2', 'GIC', 'KN1', 'Siebel', 'RadarDataModel']:
        Load_Date = today.strftime('%Y%m%d')
    elif Source == 'GCDS':
        load_date = today - timedelta(days=1) if today.weekday() == 6 else today
        Load_Date = load_date.strftime('%Y%m%d')
    elif Source == 'NLSVF':
        Load_Date = (today - timedelta(days=2)).strftime('%Y-%m-%d')
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
    
    # Mapping for Siebel data objects to their specific versions
    version_mapping_by_source = {
    'Siebel': {
        4: ['cdf_ggm_rel_x_ar_hist'],
        2: ['cdf_ggm_org_hist', 'cdf_ggm_ar_hist', 'cdf_ggm_rel_x_rel_hist', 'cdf_ggm_np_hist']
    },
    'GCDS': {
        4602: ['client_Client', 'client_KeyStoreKey', 'client_PartyRole', 'client_PartytoPartyRelationship','client_ClientOwnersProduct','client_ClientOwnersLocal','client_OnboardedLocations','client_RMA' ,'client_Products']
    },
    'RadarDataModel': {
        1: ['Party', 
           'Party_Address', 
           'Party_CDDCase', 
           'Party_CDDCase_QuestionAnswer', 
           'Party_Naics', 
           'Party_CDDCase_RiskCategories',
           'Party_Structure',
           'Party_SystemIdentifier',
           'Party_CountryAffiliation',
           'Party_Coverage',
           'Party_Identification',
           'Party_Documents',
           'Party_Selection',
           'Party_AlternativeNames']}
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
        matching_partition = next((file.name for file in partition_folders if Load_Date in file.name),None)

        if Source == 'GCDS' and matching_partition is None:
            previous_date = (datetime.strptime(Load_Date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')
            matching_partition = next((file.name for file in partition_folders if previous_date in file.name),None)

        if not matching_partition:
            raise RuntimeError(f"No partition file found for Load_Date={Load_Date} under {partition_base_path}")

        full_path = f"{partition_base_path}{matching_partition}"
        print(f"Reading from path: {full_path}")


        # Read data using appropriate format
        if Source == 'Siebel':
            spark.read.format('delta').load(full_path).createOrReplaceTempView(Dataobject)
        else:
            spark.read.parquet(f"{full_path}/*.parquet").createOrReplaceTempView(Dataobject)

        # Print row count
        row_count = spark.sql(f"SELECT COUNT(*) FROM {Dataobject}").collect()[0][0]
        print(f"No. of rows read : {row_count}")
        print(f"{Dataobject} read successfully\n")

    except Exception as e:
        raise RuntimeError(f"Error reading {Dataobject} from {Source}: {e}")

