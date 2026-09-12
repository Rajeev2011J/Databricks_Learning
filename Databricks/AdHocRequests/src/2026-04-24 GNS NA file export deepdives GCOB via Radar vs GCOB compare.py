# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal: to deep-dive into reported issue 
# MAGIC
# MAGIC ##### Context
# MAGIC 1. Akhilesh has shared files of deepdives with Ezgi, Ruud. 
# MAGIC 2. The files have some manual download out of GCOB, out of which Akhilesh has applied logic to come up with all expected data contents
# MAGIC 3. The files are compared against data that GNS team has forwarded from what they received from Radar towards them
# MAGIC
# MAGIC
# MAGIC ### Stakeholder:
# MAGIC Ezgi
# MAGIC Akhilesh
# MAGIC
# MAGIC #### Lineage:
# MAGIC 1. Only deep-dive via Databricks
# MAGIC 2. Look into Sample cases; mostly: Should something be in GNS scope yes or no
# MAGIC
# MAGIC
# MAGIC ##### Delivery agreement
# MAGIC

# COMMAND ----------

import os
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta, timezone
from pyspark.sql.functions import col
import requests
import json

from RadarUtils import *


# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
print(date_parameter)
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'

# COMMAND ----------

GNS_STORAGE_ACCOUNT= os.environ['GNS_STORAGE_ACCOUNT']
GNS_CONTAINER= os.environ['GNS_CONTAINER']
environment=os.environ['ENV']
SARadarStorage = f'saradar{environment}'

# COMMAND ----------

authenticate_storage_account(SARadarStorage)

# COMMAND ----------

list_dates = ['20260318']

# COMMAND ----------

for date_item in list_dates:
    print(dbutils.fs.ls(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/WR001_v2/Load_date{date_item}.parquet"))
    df = spark.read.parquet(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/WR001_v2/Load_date{date_item}.parquet")

    #df.write.mode('overwrite').saveAsTable(f'adhocrequests.WR001_{date_item}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load GCOB GUI files

# COMMAND ----------

load_dts = 'LOAD_DT=20260330'

# COMMAND ----------

GDP_EU_Storage_Account = os.environ['GDP_STORAGE_NAME']
GNS_STORAGE_ACCOUNT= os.environ['GNS_STORAGE_ACCOUNT']
GNS_CONTAINER= os.environ['GNS_CONTAINER']
environment=os.environ['ENV']
SARadarStorage = f'saradar{environment}'
application_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']

# COMMAND ----------

def authenticate_storage_account(write_storage):
    service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{application_id}")
    # This method is for Configuring Spark to access SARADAR Storage account using OAuth authentication
    spark.conf.set("fs.azure.account.auth.type."+write_storage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+write_storage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+write_storage+".dfs.core.windows.net", ""+application_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+write_storage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+write_storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")

    print('Connection to ' + write_storage +' storage account is successful.')

    return
authenticate_storage_account(GDP_EU_Storage_Account)

# COMMAND ----------

# List of dataobjects to load from from GDP
from pyspark.sql.functions import * 

load_df = [
'party_client_structure_GUI',
'party_client_structure', 
'party_case_client_details',
'party_AllPartyDetails',
'party_products_and_sevices',
'party_Alias',

'Party_AllParty_LocationCoverage',
'party_trade_name'
]

# Create TempView for each loading table
for Object in load_df:
    #Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')
    final_path = f'abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService/{Object}/102/data/{load_dts}/*.parquet'
    spark.read.parquet(final_path).createOrReplaceTempView(Object)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deepdiving issues

# COMMAND ----------

# DBTITLE 1,scenario 1. Check in original CS for party
# MAGIC %sql
# MAGIC select * 
# MAGIC from party_client_structure
# MAGIC
# MAGIC where 1=1
# MAGIC --ParentIdentity = 23547
# MAGIC and ClientGcobId = 43209
# MAGIC and clientid = 88617
# MAGIC --limit 10

# COMMAND ----------

# DBTITLE 1,sc 1 - Check in CS GUI for party
# MAGIC %sql
# MAGIC select * 
# MAGIC from party_client_structure_GUI
# MAGIC
# MAGIC where 1=1
# MAGIC --ParentIdentity = 23547
# MAGIC and ClientGcobId = 43209
# MAGIC and clientid = 88617
# MAGIC --limit 10

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Check GCOB Client Structure for availability of some Related PartyID
# MAGIC

# COMMAND ----------

# DBTITLE 1,sc 2
# MAGIC %sql
# MAGIC select * 
# MAGIC from party_client_structure_GUI
# MAGIC
# MAGIC where 1=1
# MAGIC and ParentIdentity = 9029
# MAGIC --and ClientGcobId = 43209
# MAGIC --and clientid = 88617

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Check: Items with comma in list UID.

# COMMAND ----------

display(df.limit(3))

# COMMAND ----------

df.createOrReplaceTempView('gns')

# COMMAND ----------

# DBTITLE 1,Review 1; comma in filename
# MAGIC %sql
# MAGIC select * from gns where ListUid like '%,%'

# COMMAND ----------

# DBTITLE 1,Observation 2; duplicate through multiple GCIDS
# MAGIC %sql 
# MAGIC select * 
# MAGIC from gns 
# MAGIC where ListUid in ('2434777' , '1311357')
# MAGIC

# COMMAND ----------

# DBTITLE 1,Observation 3; Relation that we wouldn't expect
# MAGIC %sql
# MAGIC -- Observation 3: GCID 2438449 (AMERICOLD REALTY TRUST) exists in Radar extract to GNS. The Id is a duplicate of AMERICOLD REALTY TRUST, INC. (question is why it still exists) 125857
# MAGIC select * 
# MAGIC from gns 
# MAGIC where listuid = '2438449'

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

datetime.fromtimestamp(1765160698000/1000)

# COMMAND ----------

display(df.filter(col('UniquePartyId') == 'LEC_2489')) #MUFG Bank, Ltd

# COMMAND ----------

display(df.filter(col('UniquePartyId') == 'LEC_2123')) #Deutsche Bank AG.

# COMMAND ----------

display(df.filter((col('UniquePartyId') == 'LEC_14030') | (col('`Name.Last.1`') == 'Vitens N.V.' ))) #Vitens

# COMMAND ----------

display(df.limit(2))

# COMMAND ----------


