# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To redesign GCOB consumer Party tasks in alignment with new data model to load in unity catalog
# MAGIC
# MAGIC #### author
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Read Data from GCOB and Legacy2 to further transform if needed
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal | 26-Aug-2026 |17043381 |First release 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os

import pandas as pd
from pyspark.sql.functions import lit
from datetime import datetime, timedelta

#Local File Import
from GcobUtils import write_to_unity_catalog, authenticate_storage_account, read_gdp_defined_dataobjects
catalog= os.environ['CATALOG']
schema = os.environ['GCOB_UC_SCHEMA']

# COMMAND ----------

# DBTITLE 1,Testable Functions (for pytest coverage)
# =============================================================================
# Testable Functions — these are imported by test_gcob_party.py
# =============================================================================

def calculate_business_date():
    """Calculate BusinessDate as yesterday in MM/DD/YYYY format.
    
    Returns:
        str: Yesterday's date in MM/DD/YYYY format
    
    Example:
        >>> bd = calculate_business_date()
        >>> import re
        >>> bool(re.match(r'^\\d{2}/\\d{2}/\\d{4}$', bd))
        True
    """
    return (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')


def create_gcob_legacy2_view(spark):
    """Create the Gcob_Legacy2_Clients temp view.
    
    Combines GCOB and Legacy2 client sources with UniqueGcobId logic:
    - GCOB Legal Entity -> 'LE_<GcobId>'
    - GCOB non-Legal Entity -> 'NP_NPPC_<GcobId>'
    - Legacy2 -> uses existing UniqueGcobId as-is
    
    Args:
        spark: SparkSession
    
    Requires:
        - global_temp.party_case_client_details (GCOB source)
        - global_temp.Legacy2_client (Legacy2 source)
    
    Creates:
        - Gcob_Legacy2_Clients temp view
    """
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW Gcob_Legacy2_Clients AS
        select distinct
        cast(GcobId as string) as GcobId
        ,case when ClientType = 'Legal Entity' then concat('LE_',GcobId) else concat('NP_NPPC_',GcobId) end as UniqueGcobId
        ,'GCOB' as SourceSystem
        ,FullLegalName 
        ,ClientType as PartyType
        from global_temp.party_case_client_details 
        
        union 
        
        select distinct
        cast(GcobId as string) as GcobId
        ,UniqueGcobId
        ,'Legacy2' as SourceSystem
        ,FullLegalName 
        ,ClientType as PartyType
        from global_temp.Legacy2_client
    """)


def create_party_view(spark):
    """Create the Party temp view with protected client masking.
    
    Applies protected client logic:
    - If UniqueGcobId exists in Gcob_protectedClients -> mask as 'protected account <gcobid>'
    - Otherwise -> use FullLegalName as-is
    
    Args:
        spark: SparkSession
    
    Requires:
        - Gcob_Legacy2_Clients temp view (created by create_gcob_legacy2_view)
        - global_temp.Gcob_protectedClients
    
    Creates:
        - Party temp view
    """
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW Party AS
        select distinct
        c.GcobId
        ,c.UniqueGcobId
        ,c.SourceSystem
        ,case when p.UniqueGcobId is not null then CONCAT('protected account ', p.gcobid) else c.FullLegalName end as FullLegalName
        ,c.PartyType
        from Gcob_Legacy2_Clients c
        left join global_temp.Gcob_protectedClients p on c.UniqueGcobId = p.UniqueGcobId
    """)


def add_business_date_column(df, business_date):
    """Add BusinessDate column to DataFrame.
    
    Args:
        df: PySpark DataFrame
        business_date: str in MM/DD/YYYY format
    
    Returns:
        DataFrame with BusinessDate column added
    """
    return df.withColumn("BusinessDate", lit(business_date))

# COMMAND ----------

# DBTITLE 1,Date parameters
BusinessDate = calculate_business_date()

# COMMAND ----------

# DBTITLE 1,Final Object Name
Party_dataobject = 'Party'

# COMMAND ----------

# DBTITLE 1,Combine Gcob and Legacy2
create_gcob_legacy2_view(spark)

# COMMAND ----------

# DBTITLE 1,Apply IsProtected Logic
create_party_view(spark)

# COMMAND ----------

# DBTITLE 1,Create DataFrame to derive Business Date
df_Party=spark.table('Party')

# COMMAND ----------

# DBTITLE 1,Add BusinessDate column
df_Party = add_business_date_column(df_Party, BusinessDate)

# COMMAND ----------

# DBTITLE 1,Write to Unity Catalog
write_to_unity_catalog(df_Party,catalog,schema, Party_dataobject)
