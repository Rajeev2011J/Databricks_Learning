# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal
# MAGIC
# MAGIC - To export the metadata (column names) 
# MAGIC - 
# MAGIC
# MAGIC ### 

# COMMAND ----------

# importing

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loop over all for radar

# COMMAND ----------

# get all tablenames
p_df_tables = spark.sql("show table extended in radar like '*'").toPandas()

# COMMAND ----------

p_df_tables['tableName']

# COMMAND ----------

df_tot = pd.DataFrame({'col_name':[], 'data_type':[], 'comment':[], 'tablename': []})
df_tot


# COMMAND ----------

for tablename in p_df_tables['tableName']:

    df_temp = spark.sql(f'Describe radar.{tablename}').toPandas()
    df_temp['tablename'] = tablename
    df_temp['comment'] = 'na'
    print(tablename)

    df_tot = pd.concat([df_tot, df_temp], axis = 0)

# COMMAND ----------

s_df_metadata = spark.createDataFrame(df_tot)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.allTableMetadata

# COMMAND ----------

s_df_metadata.write.mode('overwrite').saveAsTable('radar.allTableMetadata')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.allTableMetadata where tablename like 'sira%'
# MAGIC or tablename like 'tx'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loop over all for WR_Radar

# COMMAND ----------

# get all tablenames
database = 'wr_radar'

def updateMetadataForDatabase(databasename):
    # TODO: check if metastore database exists? otherwise give error

    # get all content from metadata
    p_df_tables = spark.sql(f"show table extended in {databasename} like '*'").toPandas()

    # get empty file
    df_tot = pd.DataFrame({'col_name':[], 'data_type':[], 'comment':[], 'tablename': []})
    #df_tot
    

    for tablename in p_df_tables['tableName']:
        df_temp = spark.sql(f'Describe {databasename}.{tablename}').toPandas()
        df_temp['tablename'] = tablename
        df_temp['comment'] = 'na'
        print(tablename)
        df_tot = pd.concat([df_tot, df_temp], axis = 0)

    # add column for databaseName
    df_tot['SparkDataBaseName'] = databasename

    # make the pandas dataframe into a spark dataframe
    s_df_metadata = spark.createDataFrame(df_tot)
    
    # drop metadatatable for existing schema
    spark.sql(f'drop table if exists {databasename}.allTableMetadata')

    # save table for schema
    s_df_metadata.write.mode('overwrite').saveAsTable(f'{databasename}.allTableMetadata')

    return print(f'MetadataTable updated: {databasename}.allTableMetadata')

# COMMAND ----------

updateMetadataForDatabase('wr_radar')

# COMMAND ----------

p_df_tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wr_radar.alltablemetadata limit 100

# COMMAND ----------


