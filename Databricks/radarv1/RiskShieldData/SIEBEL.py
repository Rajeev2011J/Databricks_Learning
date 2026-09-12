# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal :
# MAGIC Take the latest file in GDP and load into Synapse DWH
# MAGIC #### author :
# MAGIC Devi.Chennareddy@rabobank.com
# MAGIC #### Flow of logic
# MAGIC Load Siebel tables from GDP, determine its target Synapse table name, and write it to Synapse.

# COMMAND ----------

import os
import re
from pyspark.sql.functions import lit
from pyspark.sql.types import *
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
environment=os.environ['ENV']

# COMMAND ----------

from RiskShieldUtils import *

# COMMAND ----------

ENV=os.getenv("ENV")

# COMMAND ----------

#Derive the date for which data has to be processed from GDP
from datetime import datetime, timedelta
ThisDay = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
Today = datetime.today().strftime('%Y%m%d')
load_dts = 'edl_partition_date=' + ThisDay+'*'

print (load_dts)

# COMMAND ----------


GDPStorage ='edlcorestdeuprod0001'
authenticate_storage_account(GDPStorage)

# COMMAND ----------

# DBTITLE 1,Load siebel data to synapse DWH
if ENV =='prod':

    siebel_tables = {
        'cdf_ggm_rel_x_ar_hist': '3',
        'cdf_ggm_np_hist': '2',
        'cdf_ggm_ar_hist':'2',
        'cdf_ggm_cmrcl_pd_hist': '2',
        'cdf_ggm_org_hist':'2',
        'cdf_ggm_org_idy_cl_hist':'2'
    }

    for item, version in siebel_tables.items():
        # Get the most recent file available in gdp
        path = f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data/'
        files = dbutils.fs.ls(path)
        load_date = max(file.path.split('edl_partition_date=')[1][:8] for file in files if 'edl_partition_date=' in file.path)

        df_siebel=spark.read.format('delta').load(f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data/')
        display(item)

        if item =='cdf_ggm_rel_x_ar_hist':
            df_siebel = df_siebel.drop('edl_partition_date')
            file_name = 'siebel.cdf_ggm_rel_x_ar_hist_2025'

        elif item =='cdf_ggm_np_hist':
            # Add new columns with null values
            df_siebel = df_siebel.drop('edl_partition_date')
            timestamp_columns = ['edl_pcs_dts', 'tech_from_dts', 'tech_to_dts','mbr_end_dts', 'mbr_strt_dts']
            for col_name in timestamp_columns:
                df_siebel = df_siebel.withColumn(col_name, lit(None).cast(TimestampType()))

            string_columns = ['soc_st_ggm_code', 'soc_st_ggm_dsc','mbr_st_tp_ggm_code','mbr_st_tp_ggm_dsc','hh_sbl_id','ip_ggm_dsc']
            for col_name in string_columns:
                df_siebel = df_siebel.withColumn(col_name, lit(None).cast(StringType()))

            file_name = 'siebel.cdf_ggm_np_hist_2025'

        elif item =='cdf_ggm_ar_hist':
            df_siebel = df_siebel.drop('ar_int_id','edl_partition_date')
            file_name = 'siebel.cdf_ggm_ar_hist_2025'

        elif item =='cdf_ggm_cmrcl_pd_hist':
            df_siebel = df_siebel.drop('edl_partition_date')
            file_name= 'siebel.cdf_ggm_cmrcl_pd_hist_2025'
        
        elif item =='cdf_ggm_org_hist':
            # Add new columns with null values
            df_siebel = df_siebel.drop('edl_partition_date')
            timestamp_columns = ['edl_pcs_dts', 'tech_from_dts', 'tech_to_dts','mbr_end_dts', 'mbr_strt_dts']
            for col_name in timestamp_columns:
                df_siebel = df_siebel.withColumn(col_name, lit(None).cast(TimestampType()))

            string_columns = ['ip_ggm_dsc','mbr_st_tp_ggm_code','mbr_st_tp_ggm_dsc']
            for col_name in string_columns:
                df_siebel = df_siebel.withColumn(col_name, lit(None).cast(StringType()))
            file_name = 'siebel.cdf_ggm_org_hist_2025'

        else:
            df_siebel = df_siebel.drop('nace_cl_code','naics_2012_cl_code','naics_2022_cl_code','org_idy_cl_crt_dts','org_idy_cl_last_rvw_by_empe_dts','org_idy_cl_last_rvw_by_empe_eml_addr','org_idy_cl_st_code','org_idy_cl_orig_code','edl_partition_date')
            file_name = 'siebel.cdf_ggm_org_idy_cl_hist_2025'

        Load_from_Databricks_to_Synapse_Dedicated_SQL_Pool(df_siebel, file_name ,"siebel","overwrite")
    

    
    
