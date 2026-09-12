# Databricks notebook source
import os

import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta

#Local File Import
from RadarUtils import *

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

CoCos_dataobject = 'Co_Cos'

# COMMAND ----------

ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = [
'party_GcobUsers'
,'party_client'
,'party_case_client_details'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view CoCos AS
# MAGIC with max_case as 
# MAGIC (
# MAGIC select gcobid,max(Caseid) as caseid
# MAGIC From party_case_client_details 
# MAGIC where casestatusname!='Cancelled'
# MAGIC group by gcobid
# MAGIC )
# MAGIC select distinct 
# MAGIC pcd.SourceClient
# MAGIC ,pcd.GcobId
# MAGIC ,pcd.CaseId
# MAGIC ,pcd.FullLegalName
# MAGIC ,pcd.GlobalClientOwnerLocation 
# MAGIC ,pcd.GlobalClientOwner as GlobalClientOwnerName
# MAGIC --,u.ClientOwnerName as GlobalClientOwnerName
# MAGIC ,u.SupportedClientOwnerMailAdress as GlobalClientOwnerEmail
# MAGIC ,`Role Type Name` as RoleTypeName
# MAGIC ,case when `Role Type Name` = 'GCOB Client Owner Support' then ClientOwnerSupport else null end as ClientOwnerSupport
# MAGIC ,case when `Role Type Name` = 'GCOB Client Owner Support' then UserMailAdress else null end as ClientOwnerSupportEmail
# MAGIC ,case when `Role Type Name` = 'GCOB Client Owner Delegates' then ClientOwnerSupport else null end as ClientOwnerDelegate 
# MAGIC ,case when `Role Type Name` = 'GCOB Client Owner Delegates' then UserMailAdress else null end as ClientOwnerDelegateEmail 
# MAGIC ,case when mc.caseid is not null then 'Yes' else 'No' end as IsLatestReviewOfTheClient
# MAGIC ,pcd.islatestapprovedversionofclient
# MAGIC from party_case_client_details pcd
# MAGIC left join max_case mc on pcd.gcobid = mc.gcobid and pcd.caseid = mc.caseid
# MAGIC left join party_GcobUsers u on lower(pcd.globalclientowner) = lower(u.ClientOwnerName) and u.supportusertypeid in(1,2)
# MAGIC where pcd.casestatusname!='Cancelled'

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC create or replace temporary view CoCos AS
# MAGIC select distinct pcd.SourceClient,cos.GcobId,cos.FullLegalName,ClientOwnerID,cos.OwnerLocation ,ClientOwnerName,UserMailAdress,SupportedClientOwnerMailAdress,`Role Type Code` as RoleTypeCode
# MAGIC 	, `Role Type Name` as RoleTypeName,
# MAGIC case when supportusertypeid=1 then ClientOwnerSupport else '' end as ClientOwnerSupport,
# MAGIC case when supportusertypeid=2 then ClientOwnerSupport else '' end as ClientOwnerdelegate from party_GcobUsers  pgu join radar.clientownership  cos 
# MAGIC on  cos.Ownername=pgu.clientownername and cos.OwnerType='Global'
# MAGIC and cos.CaseStatusName!='Cancelled' and cos.OwnerLocation not in ('India','Netherlands') join party_client pc
# MAGIC on cos.GcobId=pc.GcobId join party_case_client_details pcd 
# MAGIC on pc.GcobId=pcd.GcobId and pcd.FullLegalName=cos.FullLegalName and pcd.OwnerType=cos.OwnerType and pcd.GlobalClientOwnerLocation=cos.OwnerLocation
# MAGIC and pcd.GlobalClientOwner=pgu.ClientOwnerName
# MAGIC where supportusertypeid in(1,2) and pcd.IsLatestApprovedVersionOfClient='True'

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data

# COMMAND ----------

df_CoCos = spark.table('CoCos')
save_to_saradar_storage_account(df_CoCos, CoCos_dataobject)

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS radar.Co_Cos;


# COMMAND ----------


# #writting to Delta table
# spark.sql('select * from  CoCos').write.mode('overwrite').saveAsTable('radar.Co_Cos')
