# Databricks notebook source
# DBTITLE 1,Cluster Creation
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()



# COMMAND ----------

print("Attempting to create cluster. Please wait...")
#create/update small

c = w.clusters.create_and_wait(
  cluster_name             = 'python-based-cluster',
  spark_version            = '15.3.x-cpu-ml-scala2.12',
  node_type_id             = 'Standard_DS3_v2',
  autotermination_minutes  = 15,
  num_workers              = 1,
  spark_env_vars           = {'APP_REG_APP_ID':'5864572e-dc77-4105-b900-4f72ab4b0cd8', 
                              'GDP_STORAGE_NAME':'edlcorestdeuprod0001',
                              'TENANT_ID':'6e93a626-8aca-4dc1-9191-ce291b4b75a1',
                              'ENVIRONMENT': 'preprd'}
)

print(f"The cluster is now ready at " \
      f"{w.config.host}#setting/clusters/{c.cluster_id}/configuration\n")

# COMMAND ----------

#create/update medium

