# Databricks notebook source
import os
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
SARADAR = "saradar" + environment

# COMMAND ----------

from RadarUtils import *

# COMMAND ----------

authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

dbutils.fs.ls("abfss://gcob@saradardev.dfs.core.windows.net/")

# COMMAND ----------


spark.conf.set(
    "fs.azure.account.auth.type.mystorage.dfs.core.windows.net",
    "ManagedIdentity"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.mystorage.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ManagedIdentityTokenProvider"
)


# COMMAND ----------

gcob_tables = [
      'CaseService_case_LegalEntityClient'
    , 'CaseService_case_Case'
    , 'CaseService_case_LegalEntityClientIdentifier'
    , 'CaseService_NaturalPerson_NaturalPersonClientIdentifier'
    , 'CaseService_case_SystemTypeReference'
    , 'CaseService_NaturalPerson_SystemTypeReference'
]

for item in gcob_tables:
    # get the most recent file available in gdp
    path = f'abfss://gcob@dlcorestdeudev0001.dfs.core.windows.net/{item}/100/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOADED_DTS=')[1][:8] for file in files if 'LOADED_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcob@edlcdlcorestdeudev0001orestdamprod0001.dfs.core.windows.net/{item}/100/data/LOADED_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcob_'+item)

# COMMAND ----------

# MAGIC %pip install databricks-sdk
# MAGIC

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()  # Auth via your configured profile/env
me = w.current_user.me()
print(me.user_name)     # typically the username/email
print(me.emails)        # list of email objects (value/type/primary)
print(me.id)            # numeric workspace user ID


# COMMAND ----------

# Works in single-user clusters & SQL warehouses
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
print(user)  # Often the user's email (e.g., john.doe@contoso.com)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Databricks SQL or spark.sql
# MAGIC SELECT current_user()          AS principal;   -- alias of USER
# MAGIC -- On DBR 14.1+ you can prefer:
# MAGIC SELECT session_user()          AS principal;
