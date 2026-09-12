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
# MAGIC - If columns are not found in AMP extract, fetch them from NLSVF objects.  
# MAGIC - Finalize the column list based on available data.
# MAGIC  
# MAGIC ### Expected_output
# MAGIC - Column list is added at the end of this notebook

# COMMAND ----------

# DBTITLE 1,Importing the required packages
import os
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
import os
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Defining SA RADAR Storage account
SARADAR='saradar'+environment

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Defining the dataobjects' name
party_CDDCases_RiskCategory_dataobject = 'Nlsvf_Party_CDDCase_RiskCategories'

# COMMAND ----------

# DBTITLE 1,Read GCOB Risk data from GDP defined layer
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

# DBTITLE 1,Read NLSVF data from GDP defined layer
import pandas as pd # print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'nls_dbo_cif'
,'nls_dbo_loanacct'
,'nls_dbo_loanacct_detail'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='NLSVF', Dataobject=row.GDPname)

# COMMAND ----------

# DBTITLE 1,AMP risk category details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW AMP_Client_Risk_categories AS
# MAGIC select distinct 
# MAGIC c.cifno
# MAGIC ,c.CIFNUMBER
# MAGIC ,c.GeographicalRiskLevel
# MAGIC ,c.EntityTypeRiskLevel
# MAGIC ,c.StructureRiskLevel
# MAGIC ,c.SectorRiskLevel
# MAGIC ,c.ProductServiceRiskLevel as ProductAndServiceRiskLevel
# MAGIC ,c.PEPRiskLevel
# MAGIC ,c.TransactionRiskLevel
# MAGIC ,c.DistributionChannelRiskLevel as DistributionRiskLevel
# MAGIC ,c.ThirdPartyRiskLevel
# MAGIC ,c.AdverseInformationRiskLevel as AdverseInfoRiskLevel
# MAGIC ,c.AMLOverallRiskLevel as CalculatedRiskLevel
# MAGIC from radar.amp_extract c

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW NLSVF_Client_Risk_categories AS
# MAGIC select distinct
# MAGIC c.cifno
# MAGIC ,case when c.ENTITY = 'Individual' then concat('NP_NPPC_', c.CIFNUMBER) else concat('LEC_', c.CIFNUMBER) end as UniqueCaseId
# MAGIC ,acc.userdef09 as GeographicalRiskLevel
# MAGIC ,acc.userdef10 as EntityTypeRiskLevel
# MAGIC ,acc.userdef11 as StructureRiskLevel
# MAGIC ,acc.userdef12 as SectorRiskLevel
# MAGIC ,acc.userdef13 as ProductAndServiceRiskLevel
# MAGIC ,acc.userdef14 as PEPRiskLevel
# MAGIC ,acc.userdef15 as TransactionRiskLevel
# MAGIC ,acc.userdef16 as DistributionRiskLevel
# MAGIC ,acc.userdef16 as ThirdPartyRiskLevel
# MAGIC ,acc.userdef18 as AdverseInfoRiskLevel
# MAGIC ,acc.userdef02 as CalculatedRiskLevel
# MAGIC -- ,acc.userdef03 as RecalculatedRiskLevel
# MAGIC from nls_dbo_cif c
# MAGIC inner join nls_dbo_loanacct ac on c.CIFNO = ac.CIFNO
# MAGIC inner join nls_dbo_loanacct_detail acc on ac.acctrefno = acc.acctrefno

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view  Final_Case_RiskCategory as
# MAGIC select distinct
# MAGIC a.cifno as NLSVF_Identifier
# MAGIC , CONCAT('NLSVF_',a.cifno) as PartyIdentifier
# MAGIC ,a.CIFNO AS LocalSystemIdentifier
# MAGIC ,'NLSVF' as Application
# MAGIC ,a.CIFNUMBER as ClientId
# MAGIC ,a.CIFNUMBER as CaseId
# MAGIC ,c.UniqueCaseId
# MAGIC ,a.GeographicalRiskLevel
# MAGIC ,a.EntityTypeRiskLevel
# MAGIC ,a.StructureRiskLevel
# MAGIC ,a.SectorRiskLevel
# MAGIC ,a.ProductAndServiceRiskLevel
# MAGIC ,a.PEPRiskLevel
# MAGIC ,a.TransactionRiskLevel
# MAGIC ,a.DistributionRiskLevel
# MAGIC ,a.ThirdPartyRiskLevel
# MAGIC ,a.AdverseInfoRiskLevel
# MAGIC ,a.CalculatedRiskLevel
# MAGIC -- ,c.RecalculatedRiskLevel
# MAGIC from AMP_Client_Risk_categories a
# MAGIC left outer join NLSVF_Client_Risk_categories c on a.cifno=c.cifno

# COMMAND ----------

display(spark.table('Final_Case_RiskCategory').count())

# COMMAND ----------

df_party_CDDCases_RiskCategory=spark.table('Final_Case_RiskCategory')

# COMMAND ----------

save_to_saradar_storage_account(df_party_CDDCases_RiskCategory, party_CDDCases_RiskCategory_dataobject, radar_datamodel_version_number, environment)
