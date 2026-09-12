# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal:
# MAGIC To deliver data in document (JSON) format to the document db.
# MAGIC Should have many Client documents with
# MAGIC - Client/{gcid}/
# MAGIC   - GCID
# MAGIC   - GCOBid
# MAGIC   - client owner
# MAGIC   - FullLegalName
# MAGIC   - Party type
# MAGIC   - Addresses [list]
# MAGIC     - adress type
# MAGIC     - street name
# MAGIC     - street number
# MAGIC     - postal code
# MAGIC     - city
# MAGIC     - countryname
# MAGIC     - countryIso
# MAGIC   - KYC Naics [list]
# MAGIC     - Naics code
# MAGIC     - Naics description
# MAGIC   - DocumentIdentifiers [list]
# MAGIC     - DocumentIdType
# MAGIC     - DocumentIdValue
# MAGIC   - CDDHierarchy [list?/ nested structure]
# MAGIC     - StructureTabComment
# MAGIC     - Relationships [list]
# MAGIC       - child
# MAGIC       - parent
# MAGIC       - relationtype
# MAGIC     - Parties [list]
# MAGIC       - Party Type
# MAGIC       - unique identifier
# MAGIC       - DOB
# MAGIC       - Date of Incorp
# MAGIC       - Name
# MAGIC       - Address
# MAGIC       - GCID if possible
# MAGIC       - IdentificationDocuments [list]
# MAGIC         - Filenet documentID
# MAGIC         - Document Name
# MAGIC         - FilenetUniqueIdentifingLocation

# COMMAND ----------

import pandas as pd
import os

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

# https://learn.microsoft.com/en-us/azure/databricks/scenarios/service-endpoint-cosmosdb
# https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/tutorial-spark-connector?pivots=programming-language-python
# https://community.databricks.com/t5/data-engineering/need-help-to-insert-huge-data-into-cosmos-db-from-azure-data/td-p/13357



# COMMAND ----------

# AAD Auth Config
# https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/cosmos/azure-cosmos-spark_3_2-12/docs/configuration-reference.md
# Config Property Name	Default	Description
# spark.cosmos.auth.type	= ServicePrincipal
# spark.cosmos.auth.aad.clientId		# The clientId/ApplicationId of the service principal 
# spark.cosmos.auth.aad.resourceId	# The resourceId of the service principal. Optional for ManagedIdentity authentication.
# spark.cosmos.auth.aad.clientSecret	# The client secret/password of the service principal. Required for ServicePrincipal authentication.

# COMMAND ----------

# access: 
CosmosAccount = 'radarapicosmossqlapidev'
cosmosDatabaseName = 'radarapisqldb'
cosmosContainerName = 'Client'
cosmosMasterKey = 'donthardcode'
cosmosConnectionString = 'donthardcode'

# COMMAND ----------

readCfg = {

  "spark.cosmos.accountEndpoint": f'https://{CosmosAccount}.documents.azure.com:443/',

  "spark.cosmos.accountKey": cosmosMasterKey,

  "spark.cosmos.database": cosmosDatabaseName,

  "spark.cosmos.container": cosmosContainerName,

  "spark.cosmos.read.inferSchema.enabled": "true",

  "spark.cosmos.write.strategy": "ItemOverwrite"

}

# COMMAND ----------

columns = ["id", "Gcobid", "value"]
data = [("1", "20000", "a"), ("2", "100000", "b"), ("3", "3000", "c")]

# COMMAND ----------

sdf = spark.createDataFrame(data, columns)

# COMMAND ----------

json_data = {
    "id": "1",
    "gcobid": "1",
    "FullLegalName": "ABCV",
    "naics": [123456, 234567, 345678],
    "hierarchy":
        {
           "commenttext" : "abcdefg",
           "relations" : [
             {
                "child": "1234",
                "parent": "5678",
                "type": "UBO"
             },
             {
                "child": "1",
                "parent": "1234",
                "type": "test123"
             }
           ],
           "parties": [

            {
                "partyid": "1234",
                "partytype" : "LE"
            },
            {
                "partyid": "1",
                "partytype" : "LE"
            },
            {
                "partyid": "5678",
                "partytype" : "NP"
            }
           ]
        }
}

# COMMAND ----------

sdf.write.format("cosmos.oltp").options(**readCfg).mode("append").save()

# COMMAND ----------

# pip install azure-cosmos

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
from azure.cosmos import CosmosClient, exceptions, PartitionKey
from azure.core.exceptions import AzureError

# COMMAND ----------

# Initialize the Cosmos client
endpoint = f'https://{CosmosAccount}.documents.azure.com:443'
key = cosmosMasterKey
client = CosmosClient(endpoint, key)

# COMMAND ----------

# Create a database
database_name = cosmosDatabaseName
database = client.create_database_if_not_exists(id=database_name)

# COMMAND ----------

# Create a container
# https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-python-create-container

container_name = cosmosContainerName
container = database.create_container_if_not_exists(
     id=container_name,
     partition_key=  PartitionKey(path="/gcobid"),
     offer_throughput=400
 )

# COMMAND ----------

# Insert JSON data into the container
try:
    container.create_item(body=json_data)
    print("Data inserted successfully")
except exceptions.CosmosHttpResponseError as e:
    print(f"An error occurred: {e.message}")
