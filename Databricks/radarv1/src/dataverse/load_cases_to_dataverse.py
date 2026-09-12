# Databricks notebook source
import os
import requests
from functions_dataverse import get_access_token, upsert_to_dataverse_table, get_dataverse_data

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

dataverse_url = os.environ['DATAVERSE_URL']
dataverse_api_url = f"{dataverse_url}/api/data/v9.1/"

# COMMAND ----------

access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)

# COMMAND ----------

dataverse_table_name = 'rdr_caseses'

headers = {
     "Authorization": f"Bearer {access_token}",
     "Accept": "application/json",
     "Content-Type": "application/json; charset=utf-8",
}

# COMMAND ----------

# fetch data from Dataverse
df = get_dataverse_data(access_token, dataverse_api_url, dataverse_table_name)

# check if the Dataverse table is empty and handle accordingly
if df is None: # upsert from full Databricks table
    query_cases = f'''
            SELECT DISTINCT
                CASE 
                    WHEN CaseId IS NULL THEN ca.SourceClient 
                    ELSE CONCAT(ca.SourceClient, '-', CASE
                                                        WHEN ca.SourceSystemReference LIKE '%NP%' THEN CONCAT('NP_', caseid)
                                                        ELSE caseid
                                                       END) 
                 END AS rdr_id
                ,ca.SourceClient AS rdr_sourceclient
                ,CASE
                    WHEN ca.SourceSystemReference LIKE '%NP%' THEN CONCAT('NP_', caseid)
                    ELSE caseid
                 END AS rdr_caseid
                ,ca.UniqueGcobId AS rdr_uniquegcobid
                ,ca.FullLegalName AS rdr_clientname
                ,CaseReviewType AS rdr_casereviewtype
                ,Casephase AS rdr_casephase
                ,CasePhase_yst AS rdr_casephaseyst     
                ,FOSTeamScope AS rdr_fosteam
                ,Daysincurrentcasephase AS rdr_daysincurrentphase 
                ,Preworkanalyst AS rdr_preworkanalyst
                ,ca.CaseStatusName AS rdr_casestatus                
                ,CASE
                    WHEN cl.SourceClient IS NOT NULL THEN true
                    ELSE false
                 END AS rdr_islatestcase         
            FROM hive_metastore.radar.cases ca
                LEFT JOIN hive_metastore.radar.clients cl
                    ON ca.SourceClient = cl.SourceClient
            WHERE ca.SourceSystem <> 'Legacy2'                        
        '''
    
    # Refresh token immediately before upsert
    print("Start upsert...")
    access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)
    upsert_to_dataverse_table(dataverse_api_url, dataverse_table_name, query_cases, access_token, 'rdr_id', 'rdr_casesid', 1000)
else: # only upsert missing Databricks rows in Dataverse
    df.createOrReplaceTempView("df_view")

    df_cases_dataverse = spark.sql(f'''
            SELECT
                  rdr_id
                , rdr_sourceclient
                , rdr_caseid
                , rdr_uniquegcobid
                , rdr_clientname
                , rdr_casereviewtype
                , rdr_casephase
                , rdr_casephaseyst
                , rdr_fosteam
                , CAST(rdr_daysincurrentphase AS INT) AS rdr_daysincurrentphase
                , rdr_preworkanalyst
                , rdr_casestatus 
                , rdr_islatestcase
            FROM df_view    
        ''')

    df_cases_databricks = spark.sql(f'''
            SELECT DISTINCT
                 CASE 
                    WHEN CaseId IS NULL THEN ca.SourceClient 
                    ELSE CONCAT(ca.SourceClient, '-', CASE
                                                        WHEN ca.SourceSystemReference LIKE '%NP%' THEN CONCAT('NP_', caseid)
                                                        ELSE caseid
                                                       END) 
                 END AS rdr_id 
                ,ca.SourceClient AS rdr_sourceclient
                ,CASE
                    WHEN ca.SourceSystemReference LIKE '%NP%' THEN CONCAT('NP_', caseid)
                    ELSE caseid
                 END AS rdr_caseid
                ,ca.UniqueGcobId AS rdr_uniquegcobid 
                ,ca.FullLegalName AS rdr_clientname   
                ,ca.CaseReviewType AS rdr_casereviewtype      
                ,ca.Casephase AS rdr_casephase 
                ,ca.CasePhase_yst AS rdr_casephaseyst         
                ,cl.FOSTeamScope AS rdr_fosteam
                ,ca.Daysincurrentcasephase AS rdr_daysincurrentphase 
                ,ca.Preworkanalyst AS rdr_preworkanalyst
                ,ca.CaseStatusName AS rdr_casestatus               
                ,CASE
                    WHEN cl.SourceClient IS NOT NULL THEN true
                    ELSE false
                 END AS rdr_islatestcase
            FROM hive_metastore.radar.cases ca
                LEFT JOIN hive_metastore.radar.clients cl
                    ON ca.SourceClient = cl.SourceClient
            WHERE ca.SourceSystem <> 'Legacy2'       
         ''')    

    # find rows that are in df_cases_databricks but not in df_cases_dataverse
    diff = df_cases_databricks.fillna(value="").exceptAll(df_cases_dataverse.fillna(value=""))    

    diff.createOrReplaceTempView("diff_view")
    
    query_cases = f'''
            SELECT
                  rdr_id     
                , rdr_sourceclient
                , rdr_caseid
                , rdr_uniquegcobid  
                , rdr_clientname   
                , rdr_casereviewtype
                , rdr_casephase
                , rdr_casephaseyst     
                , rdr_fosteam  
                , rdr_daysincurrentphase
                , rdr_preworkanalyst   
                , rdr_casestatus 
                , rdr_islatestcase 
            FROM diff_view             
            '''   
    print("Starting upsert")
    # Refresh token immediately before upsert
    access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)
    upsert_to_dataverse_table(dataverse_api_url, dataverse_table_name, query_cases, access_token, 'rdr_id', 'rdr_casesid', 1000)

