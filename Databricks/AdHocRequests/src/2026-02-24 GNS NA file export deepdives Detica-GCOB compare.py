# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal: to deep-dive into reported issue 
# MAGIC - Observation 1: Some records that are sent to GNS, it appears to make a listUID like '1234,1234' (i.e. its the right ID, but exists twice and comma-separated )
# MAGIC     - Still seems like it exist. Data is Sourced from GCOB Producers object 'Party AllParty Details', and is concatenated in that scope. It's a value that exists twice in the database of the PartyIdentifiers, which is causing it to come up as comma-separated
# MAGIC     --> suggesting fix/ bug: Fix by only getting a unique list of the GCID Values, and comma-separate if still necessary then.
# MAGIC  
# MAGIC - Observation 2: 2434777 / 1311357 (Sealed air limited) is in Radar -> Twice, but only once in GCOB extract (only as 1311357 ).
# MAGIC Today (2026-04-02 ) the GCID 2434777 is an old duplicate of 1311357.
# MAGIC     - It exists as RLE of GcobId 29144 (Sealed Air de Mexico Operations, S. de R.L. de C.V.) - in the snapshot of that latest structure, it has GCID 2434777
# MAGIC  
# MAGIC - Observation 3: GCID 2438449 (AMERICOLD REALTY TRUST) exists in Radar extract to GNS. The Id is a duplicate of AMERICOLD REALTY TRUST, INC. (question is why it still exists) 125857
# MAGIC     - Answer: GCOBID 66099 (Americold Realty Operating Partnership, L.P.) Has this party as Related Client. In that context, this old GCID is still connected as part of the old snapshot of this LE. ( https://gcob.rabonet.com/legalEntityClientDetails/view/11413/client-identifiers )
# MAGIC
# MAGIC ### Stakeholder:
# MAGIC Ezgi
# MAGIC
# MAGIC #### Lineage:
# MAGIC 1. Only deep-dive via Databricks
# MAGIC 2. Look into the 3 issues, create code
# MAGIC 3. export results
# MAGIC
# MAGIC ##### Delivery agreement
# MAGIC ...

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


