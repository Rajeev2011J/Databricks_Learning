# Databricks notebook source
import os
 
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
 
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
 

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Load Arrow Data
import re
 
from pyspark.sql import SparkSession
# load and create temp views of all gdp_tables below
arrow_tables = [
      'caa_scenarios',
      'caa_versionedclients'
]
for item in arrow_tables:
  # get the most recent version available in gdp
  path = f'abfss://arrow@edlcorestdeuprod0001.dfs.core.windows.net/{item}/'
  files = dbutils.fs.ls(path)
  version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])
  
  # get the most recent file available in gdp
  path_file = f'{path}{version}/data/'
  files = dbutils.fs.ls(path_file)
  load_date = max(file.path.split('LOADED_DTS=')[1][:8] for file in files if 'LOADED_DTS=' in file.path)
 
  spark.read.parquet(f'abfss://arrow@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data/LOADED_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)
 

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col
from pyspark.sql import functions as F


dfv = spark.sql("""select FullLegalName,
               NAICSCode,
               NAICSDescription,
               WWID,
               Client_GCID,
               Requests_UUID from caa_versionedclients""")

dfv = dfv.withColumn("Requests_UUID", F.explode(F.split(F.col("Requests_UUID"), ",")))


# Replace newlines (\n and \r) with a space or nothing
clean_versioned = dfv.select([
    regexp_replace(col(c).cast("string"), "[\\r\\n]+", " ").alias(c)
    for c in dfv.columns
])

##display(clean_versioned)

# COMMAND ----------

dfs = spark.sql("""select 
                CreditRiskProfile_CovenantLite,
                CreditRiskProfile_LeveragedRabobankDefinition,
                CreditRiskProfile_LeverageMultipleRabo,
                CSRData_SustainableLoanType,
                ComplianceData_ComplianceWithUnderwritingCriteriaOfBusinessLine,
                ComplianceData_ComplianceWithUnderwritingCriteriaOfBusinessLinePast,
                ComplianceData_CreditPolicyDeviation,
                ComplianceData_ReasoningOutsideUnderwritingCriteria,
                ComplianceData_WithinCreditPolicy,
                ComplianceData_WithinRasPolicy,
                Request_LaneOutcome,
                Request_Approvers,
                Request_CreatedBy,
                Request_CreditSubmissionDate,
                Request_DateCreated,
                Request_DecisionDate,
                Request_Description,
                Request_RequestDecision,
                Request_RequestStatus,
                Request_RequestSubject,
                CreditRiskProfile_ExistingClientCRC,
                CreditRiskProfile_ExistingClientLGD,
                CreditRiskProfile_ExistingClientRRR,
                CreditRiskProfile_NewClientCRC,
                CreditRiskProfile_NewClientLGD,
                CreditRiskProfile_NewClientRRR,
                ClientProfitability_CandLRAROCProjected,
                ClientProfitability_CandLRAROCProjectedPast,
                ClientProfitability_CandLRAROCRealized,
                ClientProfitability_ClientRAROCProjected,
                ClientProfitability_ClientRAROCProjectedPast,
                ClientProfitability_ClientRAROCRealised,
                ExposureLimits_ClientExposureDerivativeCurrency,
                ExposureLimits_ClientExposurePrincipalCurrency,
                ExposureLimits_ClientExposureSettlementCurrency,
                ExposureLimits_Decimal_ClientExposureLimitDerivativesExisting,
                ExposureLimits_Decimal_ClientExposureLimitDerivativesNew,
                ExposureLimits_Decimal_ClientExposureLimitPrincipalExisting,
                ExposureLimits_Decimal_ClientExposureLimitPrincipalNew,
                ExposureLimits_Decimal_ClientExposureLimitSettlementExisting,
                ExposureLimits_Decimal_ClientExposureLimitSettlementNew,
                Request_UUID
                from caa_scenarios""")

# Replace newlines (\n and \r) with a space or nothing
clean_scenario = dfs.select([
    regexp_replace(col(c).cast("string"), "[\\r\\n]+", " ").alias(c)
    for c in dfs.columns
])

##display(clean_scenario)

# COMMAND ----------


# Register inputs as temp views
clean_versioned.createOrReplaceTempView("versioned")
clean_scenario.createOrReplaceTempView("scenario")

# Create a new temp view using SQL
spark.sql("""
    CREATE OR REPLACE TEMP VIEW arrow AS
    SELECT
        s.*,
        v.*
    FROM scenario s
    LEFT JOIN versioned v
      ON v.Requests_UUID = s.Request_UUID
""")




# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.arrowvs

# COMMAND ----------

spark.sql('SELECT * from arrow').write.mode('overwrite').saveAsTable('radar.arrowvs')
