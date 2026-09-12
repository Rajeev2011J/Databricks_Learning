# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal: to export files for GNS delivery on 13-10 till 17-10
# MAGIC
# MAGIC
# MAGIC ### Stakeholder:
# MAGIC Archana Anand
# MAGIC
# MAGIC
# MAGIC #### Lineage:
# MAGIC Export from databricks and send via Kiteworks
# MAGIC
# MAGIC
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

list_dates = ['20251118']

# COMMAND ----------

for date_item in list_dates:
    print(dbutils.fs.ls(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/WR001/Load_date{date_item}.parquet"))
    df = spark.read.parquet(f"abfss://ad-hoc-exports@{SARadarStorage}.dfs.core.windows.net/gns/WR001/Load_date{date_item}.parquet")

    df.write.mode('overwrite').saveAsTable(f'adhocrequests.WR001_{date_item}')

# COMMAND ----------



# COMMAND ----------


