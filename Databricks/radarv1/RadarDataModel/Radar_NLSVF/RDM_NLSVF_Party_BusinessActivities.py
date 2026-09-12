# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal
# MAGIC To achieve Party Object result by considering NLSVF source systems
# MAGIC  
# MAGIC ### Author
# MAGIC sowmyashree.parashivamurthy.sudha@rabobank.com
# MAGIC  
# MAGIC ### Flow_of_logic
# MAGIC - Use AMP extract from August month as the primary source.  
# MAGIC - If columns are not found, fetch them from NLSVF objects.  
# MAGIC - Finalize the column list based on available data.
# MAGIC  
# MAGIC ### Expected_output
# MAGIC - Column list is added at the end of this notebook

# COMMAND ----------

# DBTITLE 1,import libraries
import os
import pandas as pd
from datetime import datetime, timedelta
import re

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
SARADAR = "saradar" + environment
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Defining the dataobjects' name
party_Naics_dataobject='Nlsvf_Party_Naics'

# COMMAND ----------

# DBTITLE 1,Storage Account Authentication
authenticate_storage_account(ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2f. North America - (NLSvf data for Input Finance Scope)

# COMMAND ----------

# DBTITLE 1,NA - NLSvf
load_df = pd.DataFrame({'GDPname':[
   'nls_dbo_cif_detail'
   , 'nls_dbo_loanacct'
   , 'nls_dbo_loan_status_codes'
]})

#Create TempView for each loading table
for index, row in load_df.iterrows():
  Read_GDP_Defined_DataObjects(Source='NLSVF', Dataobject=row.GDPname)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW NLS_Client_NAICS AS
# MAGIC select 
# MAGIC t1.cifno
# MAGIC , CASE 
# MAGIC     WHEN substring(t2.userdef01, 5, 1) = '/' THEN LEFT(t2.userdef16, 6) 
# MAGIC     ELSE LEFT(t2.userdef01, 6)
# MAGIC     END AS NaicsCode 
# MAGIC , 'InputFinance' as BusinessLine
# MAGIC , 'Input Finance - North America' AS `Location`
# MAGIC , 'NA' AS `Region`
# MAGIC from nls_dbo_loanacct  as t1
# MAGIC left join nls_dbo_cif_detail t2 on t1.cifno = t2.cifno
# MAGIC left join nls_dbo_loan_status_codes t3 on t1.STATUS_CODE_NO = t3.STATUS_CODE_NO
# MAGIC where t1.closed_date is null
# MAGIC and t1.status_code_no  = 0
# MAGIC

# COMMAND ----------

# DBTITLE 1,Considering AMP extract as the base
# MAGIC %sql
# MAGIC Create or replace temporary view Final_Business_Activities As
# MAGIC select distinct
# MAGIC a.CIFNO as NLSVF_Identifier
# MAGIC , concat('NLSVF_',a.CIFNO) as PartyIdentifier
# MAGIC , a.CIFNO AS LocalSystemIdentifier
# MAGIC , 'NLSVF' as Application
# MAGIC , c.NAICScode
# MAGIC , c.BusinessLine
# MAGIC , c.Location
# MAGIC , c.Region
# MAGIC from radar.amp_extract a
# MAGIC left outer join NLS_Client_NAICS c on c.cifno=a.cifno

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party_Naics=spark.table('Final_Business_Activities')

# COMMAND ----------

# DBTITLE 1,Read DF to table
df_party_Naics.createOrReplaceTempView('Final_Business_Activities')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
save_to_saradar_storage_account(df_party_Naics, party_Naics_dataobject, radar_datamodel_version_number, environment)
