from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col
from databricks.sdk.runtime import spark
from pyspark.sql.utils import AnalysisException
from datetime import datetime, timedelta
import re
import os

# Lazy dbutils — initialized on first use, tests can monkeypatch this
dbutils = None


def append_to_databricks_table(df: DataFrame, schema_name: str, table_name: str):
    """
    Append to existing Databricks table
    """

    # if the table already exists and you want to append data
    df.write.mode('append').saveAsTable(f'{schema_name}.{table_name}')


def full_load_to_databricks_table(df: DataFrame, schema_name: str, table_name: str):
    """
    Overwrite existing table with a full load. The table is deleted firs, if it exists
    """

    spark.sql(f'DROP TABLE IF EXISTS {schema_name}.{table_name}')

    # save the data to a new Databricks table
    df.write.format('delta').mode('overwrite').saveAsTable(f'{schema_name}.{table_name}')


def upsert_to_databricks_table(df: DataFrame, schema_name: str, table_name: str, primary_key: list):
    """
    Upsert in Databricks table, checking if records already exist based on the Primary Key.
    If not, records are inserted; otherwise, they are updated.
    The function will raise an error if duplicate primary keys are detected in the source data.
    """  

    # check for duplicate primary keys in the input DataFrame
    duplicates = df.groupBy(primary_key).count().filter("count > 1")
    if duplicates.count() > 0:
        raise ValueError("Duplicate primary keys detected. Upsert aborted. Change the primary key and try again.")

    # read the existing table
    existing_table = f"{schema_name}.{table_name}"

    # convert the input dataframe to a temporary view
    df.createOrReplaceTempView("updates")

    # build the merge condition
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_key])

    # perform the merge operation
    spark.sql(f"""
              MERGE INTO {existing_table} AS target
              USING updates AS source
              ON {merge_condition}
              WHEN MATCHED THEN UPDATE SET *
              WHEN NOT MATCHED THEN INSERT *
              """)


def check_table_not_exists(table_name: str) -> bool:
    """
    Check if table exists in Databricks
    """
    try:
        spark.table(table_name)
        return False
    except AnalysisException:
        return True




def load_from_gdp_parquet(producer: str, table: str, given_version: int = None):
    """
    Load a table from GDP: given the producer name, table name and version if necessary
    (otherwise takes the latest one) --> connects to GDP, load the lastest version and 
    latest file available and return the EDL_LoadDate and a sql temp view of the table itself.

    Also checks if the lastest file and todays date are not more thant 1 day apart,
    otherwise stops and throw an error.

    Only these producers are allowed: gcob, core-cbt
    """
    global dbutils
    if dbutils is None:
        from databricks.sdk.runtime import dbutils as _real_dbutils
        dbutils = _real_dbutils

    # validate producer (runtime check)
    allowed_producers = ['gcob', 'core-cbt', 'coj-card-mgmt-data', 'gcds', 'planet']
    if producer not in allowed_producers:
        raise ValueError(f"Producer '{producer}' is not allowed. Must be one of: {allowed_producers}")
    
    # based on producerd name, define folder, date_format and version
    folder = {'gcob': '/CaseService/', 'core-cbt': '', 'coj-card-mgmt-data': '', 'gcds': '', 'planet': ''}[producer]
    date_format = {'gcob': 'EDL_LOAD_DTS=', 'core-cbt': 'BUSINESS_DTS=', 'coj-card-mgmt-data': 'ACT_DT=', 'gcds': 'LOAD_DTS=', 'planet': 'LOADED_DTS='}[producer]
    producer_version = {'gcob': 101, 'gcds': 4602}[producer]


    # establish connection to gdp
    app_reg_app_id = os.environ['APP_REG_APP_ID']
    ReadStorage = os.environ['GDP_STORAGE_NAME']
    TenantId = os.environ['TENANT_ID']
    service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
    jdbcHostname='firebirdsqlserverprodnla.database.windows.net'
    ServiceKey='app-reg-databricks-wr-radar-preprd'
    spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")


    # get the most recent version available in gdp
    path = f'abfss://{producer}@{ReadStorage}.dfs.core.windows.net{folder}{table}/'
    files = dbutils.fs.ls(path)
    version = max([
        int(re.search(r'/(\d+)/$', file.path).group(1))
        for file in files
        if re.search(r'/(\d+)/$', file.path)
    ])
    

    # if not null, prioritize given_version, then producer_version else version
    version = given_version if given_version is not None else (producer_version if producer_version is not None else version)


    # get the most recent file available in gdp
    path = f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{table}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split(f'{date_format}')[1][:8] for file in files if f'{date_format}' in file.path)

    # load from gdp and create temp view
    spark.read.parquet(f'{path}{date_format}{load_date}*/*.parquet').createOrReplaceTempView(table)

    # create edl_load_date with proper format
    EDL_LoadDate = datetime.strptime(load_date, '%Y%m%d').strftime('%Y-%m-%d')


    # throw error if gdp file is older than 1 day!
    today = datetime.now().date()
    load_date_obj = datetime.strptime(load_date, '%Y%m%d').date()
    days_diff = (today - load_date_obj).days
    
    if days_diff > 1:
        raise ValueError(f"Latest file date ({EDL_LoadDate}) is more than 1 day old (difference: {days_diff} days)")


    return EDL_LoadDate
