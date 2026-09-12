## This file will contain generic functions that cover functionality for multiple notebooks / activities.

# Importing the required packages
import os
from datetime import datetime
from databricks.sdk.runtime import dbutils, spark

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

def save_to_saradar_storage_account(df, object_name , version_number, environment,IF_LoadDate):
    #This method will authenticate and save the dataframe to SARADAR Storage account in the right environment 
    saradar_write_storage = f'saradar{environment}'
    authenticate_storage_account(saradar_write_storage) 
    
    EDL_LOAD_DTS=f'EDL_LOAD_DTS={IF_LoadDate}'
    # EDL_LOAD_DTS = f'EDL_LOAD_DTS={str(datetime.now().strftime("%Y%m%d"))}'
    saradar_container = 'riskshield'
    df_count = df.count()

    saradar_path = 'abfss://'+saradar_container+'@'+saradar_write_storage+'.dfs.core.windows.net/'
    target_folder_path = object_name+'/'+str(version_number)+'/data/'+EDL_LOAD_DTS+'/'
    
    print('The destination path is: ', saradar_path + target_folder_path)
    
    df.repartition(1).write.format("parquet").mode("overwrite").option("compression", "snappy").save(saradar_path + target_folder_path)
    
    print('\033[1m' + object_name + '\033[0m' +  ' data object stored successfully. \nNumber of rows written:', df_count)  
    
    return 

def Load_from_Databricks_to_Synapse_Dedicated_SQL_Pool(df, TableName , tempDirectory, Mode_of_writing):
        # Authenticate to Storage account
        saradar_write_storage = f'saradar{environment}'
        authenticate_storage_account(saradar_write_storage)
        # Set the Azure Data Lake Storage Gen2 account key for authentication
        spark.conf.set("fs.azure.account.key."+saradar_write_storage+".dfs.core.windows.net","")
        # Write the DataFrame to Azure Synapse Dedicated SQL Pool using the Databricks connector
        writer = df.write \
            .format("com.databricks.spark.sqldw") \
            .option("url", "jdbc:sqlserver://asafecreportprd.sql.azuresynapse.net:1433;database=reportingdwhprd;") \
            .option("enableServicePrincipalAuth","true") \
            .option("forwardSparkAzureStorageCredentials", "true") \
            .option("tempDir", "abfss://synapse@"+saradar_write_storage+".dfs.core.windows.net/"+tempDirectory+"/") \
            .option("dbtable", TableName) 
        
         #Add extra options for overwrite mode
        if Mode_of_writing.lower() == 'overwrite':
            writer = writer.option("truncate", "true") \
                .option("maxStrLength", "4000")
        # Write the data
        writer.mode(Mode_of_writing).save()

        print("Data insertion into Synapse Dedicated SQL Pool completed for table: " + TableName +"\nNo. of Rows Written: "+str(df.count()))
