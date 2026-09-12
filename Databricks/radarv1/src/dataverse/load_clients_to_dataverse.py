# Databricks notebook source
import os
import requests
from functions_dataverse import get_access_token, upsert_to_dataverse_table, get_dataverse_data, _delete_record

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

dataverse_table_name = 'rdr_clientses'

headers = {
     "Authorization": f"Bearer {access_token}",
     "Accept": "application/json",
     "Content-Type": "application/json; charset=utf-8",
}

# COMMAND ----------

# MAGIC %md
# MAGIC Update delta in Dataverse.clients

# COMMAND ----------

from pyspark.sql.functions import date_add, date_format

# fetch data from Dataverse
df = get_dataverse_data(access_token, dataverse_api_url, dataverse_table_name)

# check if the Dataverse table is empty and handle accordingly
if df is None: # upsert from full Databricks table
    query_clients = f'''
            SELECT
                  cl.SourceClient AS rdr_sourceclient               
                , cl.UniqueGcobId AS rdr_uniquegcobid
                , cl.KYCGroup AS rdr_kycgroup
                , cl.FullLegalName AS rdr_clientname
                , cl.GlobalClientOwner AS rdr_globalclientowner
                , cl.GlobalClientOwnerLocation AS rdr_globalclientownerlocation  
                , cl.GlobalKYCPortfolioNew AS rdr_globalkycportfolio
                , cl.ClientLifeCycleName AS rdr_clientlifecyclestatus
                , cl.BusinessLineName AS rdr_businessline
                , cl.Reason AS rdr_reason
                , cl.ValidatedRiskLevel AS rdr_risklevel
                , cl.SectorTeam AS rdr_sectorteam
                , cl.ReviewLocation AS rdr_reviewlocation
                , CASE 
                    WHEN cl.NextReviewDate IS NOT NULL THEN CONCAT(cl.NextReviewDate, 'T00:00:00Z')
                  END AS rdr_nextreviewdate
                , CASE 
                    WHEN cl.ClientCaseInitiationStart IS NOT NULL THEN CONCAT(cl.ClientCaseInitiationStart, 'T00:00:00Z')
                  END AS rdr_clientcaseinitiationstart
                , CASE 
                    WHEN cl.CDDExecution IS NOT NULL THEN CONCAT(cl.CDDExecution, 'T00:00:00Z')
                  END AS rdr_cddexecution
                , cl.GlobalReportingRegion AS rdr_globalreportingregion
                , cl.FiHubIndicator_Derived AS rdr_fihubindicator               
                , CASE 
                    WHEN cl.ClientCaseInitiationStart < 
                        CASE  
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Monday' THEN date_add(cl.NextReviewDate, -90)
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Tuesday' THEN date_add(cl.NextReviewDate, -91)
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Wednesday' THEN date_add(cl.NextReviewDate, -92)
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Thursday' THEN date_add(cl.NextReviewDate, -93)
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Friday' THEN date_add(cl.NextReviewDate, -94)
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Saturday' THEN date_add(cl.NextReviewDate, -95)
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Sunday' THEN date_add(cl.NextReviewDate, -96)
                        END
                    THEN true
                    ELSE false
                  END AS rdr_initiationstartearlierthenrecommendation
                , CASE
                    WHEN cl.ClientCaseInitiationStart > cl.NextReviewDate THEN true
                    ELSE false
                  END AS rdr_pidlaterthennrd
                , CASE
                    WHEN cl.GlobalKYCPortfolioNew IN (
                        'NL Core Lending', 'Dublin Core Lending', 'Paris Core Lending', 'Madrid Core Lending',
                        'Frankfurt Core Lending', 'Milan Core Lending', 'Antwerp Core Lending',
                        'Frankfurt International Services', 'Antwerp Advisory & Investments', 'Acorn', 'Foundation'
                    )
                    OR cl.GlobalKYCPortfolioNew = 'NL Structured Lending' AND cl.SectorTeam IN ('TCF Corp', 'PF Corp', 'VCF')
                    OR cl.GlobalKYCPortfolioNew IN ('NL Advisory & Investments', 'London Advisory & Investments') AND cl.SectorTeam IN ('Sponsor Coverage', 'RCI', 'Rabo Frontier Ventures')
                    OR cl.GlobalKYCPortfolioNew = 'NL Markets' AND cl.SectorTeam IN ('PSP Coverage')
                    OR cl.GlobalKYCPortfolioNew = 'London Core Lending' AND cl.SectorTeam IN ('Core Lending')
                    OR cl.GlobalKYCPortfolioNew = 'London International Services' AND cl.SectorTeam IN ('Int. Desk')
                    OR cl.GlobalKYCPortfolioNew = 'London Structured Lending' AND cl.SectorTeam IN ('VCF')
                    THEN true
                    ELSE false
                END AS rdr_incddportfolio
                , CASE 
                    WHEN cl.GlobalFiles = 1 THEN true
                    WHEN cl.GlobalFiles = 0 THEN false
                    ELSE NULL
                  END AS rdr_global
                , CategoryNr AS rdr_categorynr
                , LondonSectorTeam AS rdr_londonsectorteam
                , Explanation AS rdr_reasonexplanation
                , CASE 
                    WHEN cl.SignOffDate IS NOT NULL THEN CONCAT(cl.SignOffDate, 'T00:00:00Z')
                  END AS rdr_signoffdate
                , filecomplexity AS rdr_filecomplexity
                , complexfiletypes AS rdr_complexfiletypes
            FROM hive_metastore.radar.clients cl                             
            '''   

    upsert_to_dataverse_table(dataverse_api_url, dataverse_table_name, query_clients, access_token, 'rdr_uniquegcobid', 'rdr_clientsid', 1000)
else: # only upsert missing Databricks rows in Dataverse
    df.createOrReplaceTempView("df_view")

    df_clients_dataverse = spark.sql(f'''
            SELECT
                  rdr_sourceclient
                , rdr_uniquegcobid
                , rdr_kycgroup
                , rdr_clientname
                , rdr_globalclientowner
                , rdr_globalclientownerlocation
                , rdr_globalkycportfolio
                , rdr_clientlifecyclestatus
                , rdr_businessline
                , rdr_reason
                , rdr_risklevel
                , rdr_sectorteam
                , rdr_reviewlocation
                , rdr_nextreviewdate
                , rdr_clientcaseinitiationstart
                , rdr_cddexecution               
                , rdr_globalreportingregion
                , rdr_fihubindicator
                , rdr_initiationstartearlierthenrecommendation
                , rdr_pidlaterthennrd
                , rdr_incddportfolio
                , rdr_global   
                , CAST(rdr_categorynr AS LONG) AS rdr_categorynr
                , rdr_londonsectorteam       
                , rdr_reasonexplanation
                , rdr_signoffdate     
                , rdr_filecomplexity
                , rdr_complexfiletypes
            FROM df_view            
        ''')

    df_clients_databricks = spark.sql(f'''
            SELECT
                  cl.SourceClient AS rdr_sourceclient               
                , cl.UniqueGcobId AS rdr_uniquegcobid
                , cl.KYCGroup AS rdr_kycgroup
                , cl.FullLegalName AS rdr_clientname
                , cl.GlobalClientOwner AS rdr_globalclientowner
                , cl.GlobalClientOwnerLocation AS rdr_globalclientownerlocation  
                , cl.GlobalKYCPortfolioNew AS rdr_globalkycportfolio
                , cl.ClientLifeCycleName AS rdr_clientlifecyclestatus
                , cl.BusinessLineName AS rdr_businessline
                , cl.Reason AS rdr_reason
                , cl.ValidatedRiskLevel AS rdr_risklevel
                , cl.SectorTeam AS rdr_sectorteam
                , cl.ReviewLocation AS rdr_reviewlocation
                , CASE 
                    WHEN cl.NextReviewDate IS NOT NULL THEN CONCAT(cl.NextReviewDate, 'T00:00:00Z')
                  END AS rdr_nextreviewdate
                , CASE 
                    WHEN cl.ClientCaseInitiationStart IS NOT NULL THEN CONCAT(cl.ClientCaseInitiationStart, 'T00:00:00Z')
                  END AS rdr_clientcaseinitiationstart
                , CASE 
                    WHEN cl.CDDExecution IS NOT NULL THEN CONCAT(cl.CDDExecution, 'T00:00:00Z')
                  END AS rdr_cddexecution
                , cl.GlobalReportingRegion AS rdr_globalreportingregion
                , cl.FiHubIndicator_Derived AS rdr_fihubindicator               
                , CASE 
                    WHEN cl.ClientCaseInitiationStart < 
                        CASE  
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Monday' THEN date_add(cl.NextReviewDate, -90)
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Tuesday' THEN date_add(cl.NextReviewDate, -91)
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Wednesday' THEN date_add(cl.NextReviewDate, -92)
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Thursday' THEN date_add(cl.NextReviewDate, -93)
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Friday' THEN date_add(cl.NextReviewDate, -94)
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Saturday' THEN date_add(cl.NextReviewDate, -95)
                            WHEN date_format(date_add(cl.NextReviewDate, -90), 'EEEE') = 'Sunday' THEN date_add(cl.NextReviewDate, -96)
                        END
                    THEN true
                    ELSE false
                  END AS rdr_initiationstartearlierthenrecommendation
                , CASE
                    WHEN cl.ClientCaseInitiationStart > cl.NextReviewDate THEN true
                    ELSE false
                  END AS rdr_pidlaterthennrd
                , CASE
                    WHEN cl.GlobalKYCPortfolioNew IN (
                        'NL Core Lending', 'Dublin Core Lending', 'Paris Core Lending', 'Madrid Core Lending',
                        'Frankfurt Core Lending', 'Milan Core Lending', 'Antwerp Core Lending',
                        'Frankfurt International Services', 'Antwerp Advisory & Investments', 'Acorn', 'Foundation'
                    )
                    OR cl.GlobalKYCPortfolioNew = 'NL Structured Lending' AND cl.SectorTeam IN ('TCF Corp', 'PF Corp', 'VCF')
                    OR cl.GlobalKYCPortfolioNew IN ('NL Advisory & Investments', 'London Advisory & Investments') AND cl.SectorTeam IN ('Sponsor Coverage', 'RCI', 'Rabo Frontier Ventures')
                    OR cl.GlobalKYCPortfolioNew = 'NL Markets' AND cl.SectorTeam IN ('PSP Coverage')
                    OR cl.GlobalKYCPortfolioNew = 'London Core Lending' AND cl.SectorTeam IN ('Core Lending')
                    OR cl.GlobalKYCPortfolioNew = 'London International Services' AND cl.SectorTeam IN ('Int. Desk')
                    OR cl.GlobalKYCPortfolioNew = 'London Structured Lending' AND cl.SectorTeam IN ('VCF')
                    THEN true
                    ELSE false
                END AS rdr_incddportfolio
                , CASE
                    WHEN cl.ApprovedGlobalFiles = 1 THEN true
                    ELSE false
                  END AS rdr_global    
                , CategoryNr AS rdr_categorynr
                , LondonSectorTeam AS rdr_londonsectorteam   
                , Explanation AS rdr_reasonexplanation
                , CASE 
                    WHEN cl.SignOffDate IS NOT NULL THEN CONCAT(cl.SignOffDate, 'T00:00:00Z')
                  END AS rdr_signoffdate
                , filecomplexity AS rdr_filecomplexity
                , complexfiletypes AS rdr_complexfiletypes
                
            FROM hive_metastore.radar.clients cl                     
            ''')    

    # find rows that are in df_clients_databricks but not in df_clients_dataverse
    diff = df_clients_databricks.fillna(value="").exceptAll(df_clients_dataverse.fillna(value=""))    

    diff.createOrReplaceTempView("diff_view")

    query_clients = f'''
            SELECT
                  rdr_sourceclient
                , rdr_uniquegcobid
                , rdr_kycgroup
                , rdr_clientname
                , rdr_globalclientowner
                , rdr_globalclientownerlocation
                , rdr_globalkycportfolio
                , rdr_clientlifecyclestatus
                , rdr_businessline
                , rdr_reason
                , rdr_risklevel
                , rdr_sectorteam
                , rdr_reviewlocation
                , rdr_reviewlocation
                , rdr_nextreviewdate
                , rdr_clientcaseinitiationstart
                , rdr_cddexecution
                , rdr_globalreportingregion
                , rdr_fihubindicator
                , rdr_initiationstartearlierthenrecommendation
                , rdr_pidlaterthennrd
                , rdr_incddportfolio
                , rdr_global    
                , rdr_categorynr
                , rdr_londonsectorteam   
                , rdr_reasonexplanation
                , rdr_signoffdate   
                , rdr_filecomplexity
                , rdr_complexfiletypes              
            FROM diff_view             
            '''   
    
    upsert_to_dataverse_table(dataverse_api_url, dataverse_table_name, query_clients, access_token, 'rdr_uniquegcobid', 'rdr_clientsid', 1000)


# COMMAND ----------

# MAGIC %md
# MAGIC Delete records in Dataverse.clients which are not in Databricks.clients anymore

# COMMAND ----------

query_url = f"{dataverse_api_url}/{dataverse_table_name}"

# fetch data from Dataverse
df = get_dataverse_data(access_token, dataverse_api_url, dataverse_table_name)

# check if the Dataverse table is empty and handle accordingly
if df is not None: # delete records
    df.createOrReplaceTempView("df_view")

    df_clients_dataverse = spark.sql(f'''
            SELECT
                  rdr_clientsid
                , rdr_uniquegcobid                
            FROM df_view   
        ''')

    df_clients_databricks = spark.sql(f'''
            SELECT
                 UniqueGcobId                
            FROM hive_metastore.radar.clients                                   
            ''') 

    # find rows that are in df_clients_dataverse but not in df_clients_databricks
    diff = df_clients_dataverse.join(
        df_clients_databricks,
        df_clients_dataverse.rdr_uniquegcobid == df_clients_databricks.UniqueGcobId,
        how='left_anti'
    )

    # select the rdr_clientsid from the resulting DataFrame
    result = diff.select("rdr_clientsid")   
    
    # check if there are records to delete
    if result.count() == 0:
        print("No records to delete.")
    else:
        # iterate through each `rdr_clientsid` in the result DataFrame and delete records
        for row in result.select("rdr_clientsid").distinct().collect():
            record_id = row["rdr_clientsid"]
            try:
                success = _delete_record(query_url, record_id, headers)
                if success:
                    print(f"Successfully deleted record: {record_id}")
                else:
                    print(f"Failed to delete record: {record_id}")
            except Exception as e:
                print(f"Error while deleting record {record_id}: {e}")
else: 
     print("rdr_clients in Dataverse is empty.")
    

