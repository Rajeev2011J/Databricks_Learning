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

# MAGIC %md
# MAGIC #####Read Files from GDP

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
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

# DBTITLE 1,Importing RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Defining the dataobjects' name
party_dataobject='Nlsvf_Party'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Define Date variables
#Derive the date for which data has to be processes
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils

# COMMAND ----------

# DBTITLE 1,Read NLSVF data from GDP defined layer
import pandas as pd # print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
 'nls_dbo_cif_detail'
,'nls_dbo_cif'
,'nls_dbo_loanacct'
, 'nls_dbo_loan_port_codes'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='NLSVF', Dataobject=row.GDPname)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nls_dbo_loan_port_codes

# COMMAND ----------

# DBTITLE 1,Extracting data from NLSVF
# MAGIC %sql
# MAGIC create or replace temporary view nlsvf_party as
# MAGIC select distinct
# MAGIC c.cifno
# MAGIC ,c.cifno as NLSVF_Identifier
# MAGIC ,c.MAIL_NAME1 as FullLegalName
# MAGIC ,c.DOB as DateOfBirth
# MAGIC ,c.FIRSTNAME1 as FirstName
# MAGIC ,c.MIDDLENAME1 as MiddleName
# MAGIC ,c.LASTNAME1 as LastName
# MAGIC ,Null as GlobalClientOwnerLocation
# MAGIC ,case when c.ENTITY = 'Individual' then 'Natural Person' else 'Legal Entity' end as Party_type
# MAGIC ,case when acc.status_code_no = 0 then 'Former' when acc.status_code_no = 1 then 'Active' end as ClientLifeCycleStatus
# MAGIC ,c.TIN as TinOrEquivalent
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY c.CIFNUMBER ORDER BY acc.status_code_no DESC) AS ROWNUM
# MAGIC from nls_dbo_cif_detail d
# MAGIC inner join nls_dbo_cif c on d.CIFNO = c.CIFNO
# MAGIC left join nls_dbo_loanacct acc on c.CIFNO = acc.CIFNO
# MAGIC where c.MAIL_NAME1 is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nls_dbo_cif
# MAGIC order by CIFNUMBER
# MAGIC  limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(Portfolio_Code_ID) from nls_dbo_cif

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(CIFno)) from nlsvf_party where ClientLifeCycleStatus = 'Active'

# COMMAND ----------

# DBTITLE 1,Considering AMP extract as the base
# MAGIC %sql
# MAGIC Create or replace temporary view Final_Nlsvf_Party As
# MAGIC select distinct
# MAGIC a.CIFNO as NLSVF_Identifier
# MAGIC ,concat('NLSVF_',a.CIFNO) as PartyIdentifier
# MAGIC ,a.CIFNO AS LocalSystemIdentifier
# MAGIC ,'NLSVF' as Application
# MAGIC ,c.FullLegalName
# MAGIC ,c.FirstName
# MAGIC ,c.MiddleName
# MAGIC ,c.LastName
# MAGIC ,c.DateOfBirth
# MAGIC ,c.Party_type
# MAGIC ,c.ClientLifeCycleStatus
# MAGIC ,c.TinOrEquivalent
# MAGIC ,a.CDDType
# MAGIC from radar.amp_extract a
# MAGIC left outer join Nlsvf_Party c on c.cifno=a.cifno where ROWNUM=1

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_party=spark.table('Final_Nlsvf_Party')

# COMMAND ----------

# DBTITLE 1,Saving to SARADAR Storage account
save_to_saradar_storage_account(df_party, party_dataobject, radar_datamodel_version_number, environment)
