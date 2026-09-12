import os
def ConnectGDP(env):
    app_reg_app_id = os.environ['APP_REG_APP_ID']
    ReadStorage = os.environ['GDP_STORAGE_NAME']
    TenantId = os.environ['TENANT_ID']

    service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

    spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

    print('connected')

    return None


def StoreToGDP(archive, producer):
    print('currently mock for storing')


def ReadFromGDP(archive, producer, version, objectname, datedetails, tableformat = 'parquet', createTempView = 'yes'):
    print("mock for reading simply from GDP")

    return None