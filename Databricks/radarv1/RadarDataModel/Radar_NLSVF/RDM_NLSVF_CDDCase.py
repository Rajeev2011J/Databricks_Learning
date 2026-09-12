# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal
# MAGIC To achieve Party_CDDCase Object result by considering NLSVF source systems
# MAGIC  
# MAGIC ### Author
# MAGIC sowmyashree.parashivamurthy.sudha@rabobank.com
# MAGIC  
# MAGIC ### Flow_of_logic
# MAGIC - Use AMP extract from August month as the primary source.  
# MAGIC - If columns are not found in AMP extract, fetch them from NLSVF objects.  
# MAGIC - Finalize the column list based on available data.
# MAGIC  
# MAGIC ### Expected_output
# MAGIC - Column list is added at the end of this notebook

# COMMAND ----------

# MAGIC %md
# MAGIC #####READING FILES FROM GDP

# COMMAND ----------

# DBTITLE 1,Importing the required packages
import os
import pandas as pd
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Importing modules from RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
environment=os.environ['ENV']
radar_datamodel_version_number=1
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']

# COMMAND ----------

# DBTITLE 1,Defining the SARADAR Storage account
SARADAR='saradar'+environment

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(SARADAR)
authenticate_storage_account(NLS_ReadStorage)

# COMMAND ----------

# List of dataobjects from GDP
load_df =[
'Party_RiskModelInstanceQuestionAnswers'
, 'Party_RiskModelCategories'
, 'Party_RiskModelInstance'
, 'party_WRCDDModel'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='RISKMODEL')

# COMMAND ----------

import pandas as pd # print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
 'nls_dbo_cif_detail'
,'nls_dbo_cif'
,'nls_dbo_loanacct'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='NLSVF', Dataobject=row.GDPname)

# COMMAND ----------

# DBTITLE 1,Defining the dataobject name
party_CDDcase_dataobject='Nlsvf_Party_CDDCase'

# COMMAND ----------

# MAGIC %md
# MAGIC #### TRANSFORMATION TO GET ![Party_CDDCase](path) OBJECT 

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view Nlsvf_CDD_Cases as
# MAGIC select amp.cifno as NLSVF_Identifier
# MAGIC ,CONCAT('NLSVF_',amp.cifno) as PartyIdentifier
# MAGIC ,'NLSVF' as Application
# MAGIC ,amp.cifno AS LocalSystemIdentifier
# MAGIC ,amp.CustomerName
# MAGIC ,amp.CIFNUMBER as ClientId
# MAGIC ,amp.CIFNUMBER as CaseId
# MAGIC ,case when c.ENTITY = 'Individual' then concat('NP_NPPC_', c.CIFNUMBER) else concat('LEC_', c.CIFNUMBER) end as UniqueCaseId
# MAGIC ,amp.CDDType 
# MAGIC ,amp.CompletionDate
# MAGIC from radar.amp_extract amp
# MAGIC left outer join nls_dbo_cif c on c.cifno=amp.cifno 

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_CDDcase=spark.table('Nlsvf_CDD_Cases')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
save_to_saradar_storage_account(df_party_CDDcase, party_CDDcase_dataobject, radar_datamodel_version_number, environment)
