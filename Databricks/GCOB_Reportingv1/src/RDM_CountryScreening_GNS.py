# Databricks notebook source
# DBTITLE 1,Importing Libraries
import os
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta
# Local File Import - RadarUtils.py is in the same GCOB_Reportingv1 directory
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Date Variables
date_parameter = datetime.today().strftime('%Y%m%d')
BusinessDate = (datetime.today() - timedelta(1)).strftime('%d-%m-%Y')
print(f"Business Date: {BusinessDate}")

# Function to get latest LOAD_DTS folder
def get_latest_load_dts(storage_account, data_object_name, version_number):
    """
    Automatically finds the latest LOAD_DTS folder from the data directory.
    
    Parameters:
    -----------
    storage_account : str
        Storage account name (e.g., 'saradardev')
    data_object_name : str
        Name of the data object
    version_number : int or str
        Version number
    
    Returns:
    --------
    str
        Latest LOAD_DTS folder name (e.g., 'LOAD_DTS=20260812T063938Z')
    """
    base_path = f"abfss://radardatamodel@{storage_account}.dfs.core.windows.net/{data_object_name}/{version_number}/data/"
    
    try:
        # List all folders in the data directory
        folders = dbutils.fs.ls(base_path)
        
        # Filter for LOAD_DTS folders and extract folder names
        load_dts_folders = [f.name.rstrip('/') for f in folders if f.name.startswith('LOAD_DTS=')]
        
        if not load_dts_folders:
            raise ValueError(f"No LOAD_DTS folders found in {base_path}")
        
        # Sort to get the latest (assumes timestamp format allows lexicographic sorting)
        latest_folder = sorted(load_dts_folders)[-1]
        
        print(f"Found {len(load_dts_folders)} LOAD_DTS folder(s)")
        print(f"Latest: {latest_folder}")
        
        return latest_folder
        
    except Exception as e:
        print(f"Error finding latest LOAD_DTS: {e}")
        print(f"Falling back to manual date parameter")
        return f"LOAD_DTS={date_parameter}*"


# COMMAND ----------

# DBTITLE 1,Reading environment variables
GDP_EU_Storage_Account = os.environ['GDP_STORAGE_NAME']
GNS_STORAGE_ACCOUNT= os.environ['GNS_STORAGE_ACCOUNT']
GNS_CONTAINER= os.environ['GNS_CONTAINER']
environment=os.environ['ENV']
SARadarStorage = f'saradar{environment}'

# COMMAND ----------

# DBTITLE 1,Authenticating Storage accounts
authenticate_storage_account(SARadarStorage)
authenticate_storage_account(GDP_EU_Storage_Account)

# COMMAND ----------

GNS_Hit_dataobject = 'Gnshitinsights_CountryScreening'

# COMMAND ----------

# DBTITLE 1,Automatically get the latest LOAD_DTS folder
print("\n" + "="*60)
print("LOADING LATEST DATA")
print("="*60)
load_dts = get_latest_load_dts(
    storage_account=SARadarStorage,
    data_object_name='gnshitinsights_countryscreening',
    version_number=3
)
print(f"\nUsing load_dts: {load_dts}")

# COMMAND ----------

# DBTITLE 1,Functions for Data Loading
def read_data_object(storage_account, data_object_name, version_number, load_dts, temp_view_name=None):
    """
    function to read data object.
    """
    path = f"abfss://radardatamodel@{storage_account}.dfs.core.windows.net/{data_object_name}/{version_number}/data/{load_dts}/*.parquet"
    
    print(f"Reading: {data_object_name}")
    print(f"Path: {path}")
    
    df = spark.read.parquet(path)
    
    view_name = temp_view_name if temp_view_name else data_object_name
    df.createOrReplaceTempView(view_name)
    
    record_count = df.count()
    print(f"View: {view_name}")
    print(f"Records: {record_count:,}")
    
    return df

    # Get the latest HitGeneratedTime date for filtering
    latest_date_df = spark.sql(f"""
        SELECT MAX(CAST(HitGeneratedTime AS DATE)) AS latest_date
        FROM temp_{data_object_name}
    """)
    latest_hit_date = latest_date_df.collect()[0]['latest_date']
    
    print(f"  Filtering to latest Hit Date: {latest_hit_date}")

# COMMAND ----------

# DBTITLE 1,Load GNS Country Screening Data
# This function handles the path construction and temp view creation automatically
df_CountryScreening_GNS = read_data_object(
    storage_account=SARadarStorage,
    data_object_name='gnshitinsights_countryscreening',
    version_number=3,
    load_dts=load_dts,  
    temp_view_name='gnshitinsights_countryscreening' 
)
display(df_CountryScreening_GNS)

# COMMAND ----------

# DBTITLE 1,Final Dataset - Parties Hit Count
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW Final_Parties_Hit_Dataset AS
    SELECT 
        '{BusinessDate}' AS Business_Date,
        LOB AS Business_Line,
        ListUID AS Party_Id,
        '' AS Party_Full_Legal_Name,
        CONCAT_WS(', ', PRIMARY_IDENTIFIER_1, PRIMARY_IDENTIFIER_2) AS Party_Country_Name,
        MAX(DATE(HitGeneratedTime)) OVER () AS HitGeneratedDate,
        address_country_1_iso2 AS CountryISOCode,
        ALIAS_1 AS CountryName,
        date_format(current_date(), 'dd-MM-yyyy') AS RefreshDate
    FROM gnshitinsights_countryscreening
    WHERE ListUID IS NOT NULL
""")

# Display the final dataset
display(spark.sql("SELECT * FROM Final_Parties_Hit_Dataset ORDER BY Business_Line, Party_Id"))

# COMMAND ----------

# DBTITLE 1,Related Client - Temporary View
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW Related_Client_Dataset AS
    SELECT 
        '' AS ClientGcobId,
        '' AS ClientFullLegalName,
        ListUID AS PartyId,
        '' AS PartyFullLegalName,
        CONCAT_WS(', ', PRIMARY_IDENTIFIER_1, PRIMARY_IDENTIFIER_2) AS PartyCountryName
       
    FROM gnshitinsights_countryscreening    
""")

# Display the final dataset
display(spark.sql("SELECT * FROM Related_Client_Dataset ORDER BY ClientGcobId"))

# COMMAND ----------

# Combined view with all columns from both datasets
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW Combined_Dashboard_Dataset AS
    SELECT 
        p.Business_Date,
        p.Business_Line,
        p.Party_Id,
        p.Party_Full_Legal_Name,
        p.Party_Country_Name,
        p.HitGeneratedDate,
        p.CountryISOCode,
        p.CountryName,
        p.RefreshDate,
        c.ClientGcobId,
        c.ClientFullLegalName,
        c.PartyId,
        c.PartyFullLegalName,
        c.PartyCountryName
    FROM Final_Parties_Hit_Dataset p
    LEFT JOIN Related_Client_Dataset c ON p.Party_Id = c.PartyId
""")

df_combined = spark.sql("SELECT * FROM Combined_Dashboard_Dataset ORDER BY Business_Line, Party_Id")
print(f"\nTotal Records: {df_combined.count():,}")
print("="*70)
display(df_combined)

# COMMAND ----------

# Save combined dataset to storage
df_combined_dashboard = spark.table('Combined_Dashboard_Dataset')
save_to_saradar_storage_account(df_combined_dashboard, GNS_Hit_dataobject)