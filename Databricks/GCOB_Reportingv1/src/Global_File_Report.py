# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To create a dataobject to have all information about a CDD Case  for all W&R Sourcesystems
# MAGIC
# MAGIC #### Authors
# MAGIC - Abhishek.R.Jaiswal@rabobank.com
# MAGIC
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done
# MAGIC |----------|----------|----------|----------|
# MAGIC | Abhishek Jaiswal	 |13-Feb-2026 |14814367 |MI report for new Global File process
# MAGIC | Abhishek Jaiswal	 |11-Mar-2026 |14814367 |Adjust report similar to E&A team
# MAGIC | Abhishek Jaiswal	 |21-Mar-2026 |15939181 |Enhancement 
# MAGIC | Abhishek Jaiswal	 |04-May-2026 |15227602 |Integrate Global files' sharepoint's excel file through databricks 
# MAGIC | Abhishek Jaiswal	 |28-May-2026 |16254749 |Make the changes suggested in tracker 
# MAGIC | Abhishek Jaiswal	 |25-June-2026 |16414789 |Add "CDD Responsible Location" in the dashboard
# MAGIC | Abhishek Jaiswal	 |08-July-2026 |16630147 |Incorrect Lead_Involved location
# MAGIC | Abhishek Jaiswal	 |01-Sept-2026 |16885585 |Add Latest Case Flag

# COMMAND ----------

# MAGIC %pip install msal --index-url 'https://user:pw@repo.nexuscloud.aws.rabo.cloud/repository/pypi-org/simple'

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Importing the required packages
import os
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime, timedelta
#Local File Import
from RadarUtils import *

import openpyxl
import io
import msal
import requests
from urllib.parse import quote

# COMMAND ----------

# DBTITLE 1,Defining Load_Date and RunType
date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

# DBTITLE 1,Defining the dataobject name
GlobalFile_dataobject = 'Global_File_Report'
GF_dataobject = 'Global_Files_Object'
GF_Excel = 'Global_Files_ExcelData'

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
ReadStorage = os.environ['GDP_STORAGE_NAME']
authenticate_storage_account(ReadStorage)

# COMMAND ----------

# DBTITLE 1,Reading GCOB CaseService Dataobjects from GDP
# print list of strings for loading spark dfs from GDP
load_df = [
'party_case_client_details'
#,'Party_AllParty_LocationCoverage'
,'party_local_client_Owners'
,'party_workitem'
,'party_products_and_services'
]

# Create TempView for each loading table
for Object in load_df:
    Read_GDP_Defined_DataObjects(Source='GCOB', Dataobject=Object, path_prefix='CaseService')

# COMMAND ----------

# DBTITLE 1,Identify Global File with Location
# MAGIC %skip
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Global_Files AS
# MAGIC with gf AS
# MAGIC (
# MAGIC select UniquePartyId,count(*) from Party_AllParty_LocationCoverage group by UniquePartyId having count(*) > 1 
# MAGIC )
# MAGIC ,lead_involved as 
# MAGIC (
# MAGIC select distinct 
# MAGIC lc.*
# MAGIC from Party_AllParty_LocationCoverage lc
# MAGIC inner join gf on lc.UniquePartyId = gf.UniquePartyId
# MAGIC )
# MAGIC select DISTINCT
# MAGIC UniquePartyId
# MAGIC ,array_sort(collect_set(nullif(trim(Location), ''))) AS LeadInvolved_Locations
# MAGIC from lead_involved
# MAGIC group by UniquePartyId

# COMMAND ----------

# DBTITLE 1,Base view to find Global Clients
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ProdusctAndService AS
# MAGIC select distinct
# MAGIC acr.SourceClient
# MAGIC ,acr.GcobId
# MAGIC ,acr.CaseId 
# MAGIC ,Case when acr.ClientType = 'Legal Entity' then concat('LEC_',acr.GcobId)
# MAGIC       when acr.ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then concat('NP_NPPC_',acr.GcobId)
# MAGIC End as UniqueClientId
# MAGIC ,pns.BookingEntityLocation
# MAGIC ,pns.ProductOfferingLocation
# MAGIC ,pns.ProductLifecyclestatus
# MAGIC From party_case_client_details acr
# MAGIC INNER JOIN party_products_and_services pns on acr.ClientId = pns.ClientId and acr.SourceClient = pns.SourceClient 
# MAGIC where acr.CaseStatusName <> 'Cancelled' and acr.IslatestApprovedVersionOfClient='True'
# MAGIC and acr.ClientType = 'Legal Entity'
# MAGIC UNION
# MAGIC select distinct
# MAGIC acr.SourceClient
# MAGIC ,acr.GcobId
# MAGIC ,acr.CaseId 
# MAGIC ,Case when acr.ClientType = 'Legal Entity' then concat('LEC_',acr.GcobId)
# MAGIC       when acr.ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then concat('NP_NPPC_',acr.GcobId)
# MAGIC End as UniqueClientId
# MAGIC ,pns.BookingEntityLocation
# MAGIC ,pns.ProductOfferingLocation
# MAGIC ,pns.ProductLifecyclestatus
# MAGIC From party_case_client_details acr
# MAGIC INNER JOIN party_products_and_services pns on acr.ClientId = pns.ClientId and acr.SourceClient = pns.SourceClient 
# MAGIC where acr.CaseStatusName not in ('Cancelled','Completed')
# MAGIC and acr.ClientType = 'Legal Entity'
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW UnionProductAndBookingLocation AS
# MAGIC select distinct SourceClient,GcobId,CaseId,UniqueClientId,ProductOfferingLocation as P_B_location from ProdusctAndService
# MAGIC where ProductLifecyclestatus = 'Active'
# MAGIC Union 
# MAGIC select distinct SourceClient,GcobId,CaseId,UniqueClientId,BookingEntityLocation as P_B_location from ProdusctAndService
# MAGIC where ProductLifecyclestatus = 'Active'
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW DistinctProductAndBookingLocation AS
# MAGIC select distinct SourceClient,GcobId,CaseId,UniqueClientId,P_B_location as location from UnionProductAndBookingLocation
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GCOLocation AS
# MAGIC select distinct SourceClient,GcobId,CaseId,UniqueGcobId,GlobalClientOwnerLocation as location from party_case_client_details
# MAGIC where IslatestApprovedVersionOfClient='True'
# MAGIC and ClientType = 'Legal Entity'
# MAGIC and GlobalClientOwnerLocation is not null
# MAGIC union 
# MAGIC select distinct SourceClient,GcobId,CaseId,UniqueGcobId,GlobalClientOwnerLocation as location from party_case_client_details
# MAGIC where CaseStatusName not in ('Cancelled','Completed')
# MAGIC and ClientType = 'Legal Entity'
# MAGIC and GlobalClientOwnerLocation is not null
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GlobalFileBase AS
# MAGIC select *,'Involved' as LeadOrInvolved from DistinctProductAndBookingLocation
# MAGIC union 
# MAGIC select *,'Lead' as LeadOrInvolved from GCOLocation
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Identify Global File with Location
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Global_Files AS
# MAGIC with gf AS
# MAGIC (
# MAGIC select SourceClient,COUNT(DISTINCT location) from GlobalFileBase group by SourceClient having COUNT(DISTINCT location) > 1 
# MAGIC )
# MAGIC ,lead_involved as 
# MAGIC (
# MAGIC select distinct 
# MAGIC lc.*
# MAGIC from GlobalFileBase lc
# MAGIC inner join gf on lc.SourceClient = gf.SourceClient
# MAGIC )
# MAGIC select DISTINCT
# MAGIC UniqueClientId as UniquePartyId
# MAGIC ,GcobId
# MAGIC ,CaseId
# MAGIC ,SourceClient
# MAGIC ,array_sort(collect_set(nullif(trim(Location), ''))) AS LeadInvolved_Locations
# MAGIC from lead_involved
# MAGIC group by UniqueClientId,GcobId,CaseId,SourceClient

# COMMAND ----------

# DBTITLE 1,Global File Object for PowerBI
# MAGIC %skip
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Global_Files_Object AS
# MAGIC with gf AS
# MAGIC (
# MAGIC select UniquePartyId,count(*) from Party_AllParty_LocationCoverage group by UniquePartyId having count(*) > 1 
# MAGIC )
# MAGIC ,client as 
# MAGIC (
# MAGIC select distinct
# MAGIC c.*
# MAGIC ,Case when c.ClientType = 'Legal Entity' then concat('LEC_',c.GcobId)
# MAGIC       when c.ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then concat('NP_NPPC_',c.GcobId)
# MAGIC End as UniqueClientId
# MAGIC From party_case_client_details c
# MAGIC where c.CaseStatusName not in ('Cancelled')
# MAGIC )
# MAGIC select distinct 
# MAGIC lc.UniquePartyId
# MAGIC ,lc.Location
# MAGIC ,lc.LeadOrInvolved
# MAGIC from Party_AllParty_LocationCoverage lc
# MAGIC inner join gf on lc.UniquePartyId = gf.UniquePartyId
# MAGIC inner join client c on c.UniqueClientId = lc.UniquePartyId
# MAGIC

# COMMAND ----------

# DBTITLE 1,Global File Object for PowerBI
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Global_Files_Object AS
# MAGIC with gf AS
# MAGIC (
# MAGIC select SourceClient,COUNT(DISTINCT location) from GlobalFileBase group by SourceClient having COUNT(DISTINCT location) > 1 
# MAGIC )
# MAGIC ,client as 
# MAGIC (
# MAGIC select distinct
# MAGIC c.*
# MAGIC ,Case when c.ClientType = 'Legal Entity' then concat('LEC_',c.GcobId)
# MAGIC       when c.ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then concat('NP_NPPC_',c.GcobId)
# MAGIC End as UniqueClientId
# MAGIC From party_case_client_details c
# MAGIC where c.CaseStatusName not in ('Cancelled')
# MAGIC )
# MAGIC select distinct 
# MAGIC lc.UniqueClientId as UniquePartyId
# MAGIC ,lc.SourceClient
# MAGIC ,lc.Location
# MAGIC ,lc.LeadOrInvolved
# MAGIC from GlobalFileBase lc
# MAGIC inner join gf on lc.SourceClient = gf.SourceClient
# MAGIC inner join client c on c.SourceClient = lc.SourceClient

# COMMAND ----------

# DBTITLE 1,Identify LocalClientOwners
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW LocalClientOwner AS
# MAGIC select distinct 
# MAGIC SourceClient
# MAGIC ,array_sort(collect_set(nullif(trim(LocalClientOwnerName), ''))) AS LocalClientOwnerName
# MAGIC from party_local_client_Owners
# MAGIC Group by
# MAGIC SourceClient

# COMMAND ----------

# DBTITLE 1,Party Case Client Details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Client_Details AS
# MAGIC select distinct
# MAGIC c.*
# MAGIC ,Case when c.ClientType = 'Legal Entity' then concat('LEC_',c.GcobId)
# MAGIC       when c.ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then concat('NP_NPPC_',c.GcobId)
# MAGIC End as UniqueClientId
# MAGIC ,case when c.ClientType='Legal Entity' then concat('LE_', Gcobid)
# MAGIC     when c.ClientType in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',Gcobid) else 'NA' end as UniqueGcobId_1
# MAGIC ,case when c.ClientType='Legal Entity' then Gcobid
# MAGIC     when c.ClientType in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_',Gcobid) else 'NA' end as EnA_UniqueGcobID
# MAGIC ,lc.LocalClientOwnerName
# MAGIC From party_case_client_details c
# MAGIC left join LocalClientOwner lc on c.SourceClient = lc.SourceClient
# MAGIC where c.CaseStatusName <> 'Cancelled' and c.IslatestApprovedVersionOfClient='True'
# MAGIC and c.ClientType = 'Legal Entity'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select distinct
# MAGIC c.*
# MAGIC ,Case when c.ClientType = 'Legal Entity' then concat('LEC_',c.GcobId)
# MAGIC       when c.ClientType in ('Natural Person','Natural Person acting in a Professional Capacity (NPPC)') then concat('NP_NPPC_',c.GcobId)
# MAGIC End as UniqueClientId
# MAGIC ,case when c.ClientType='Legal Entity' then concat('LE_', Gcobid)
# MAGIC     when c.ClientType in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_NPPC_',Gcobid) else 'NA' end as UniqueGcobId_1
# MAGIC ,case when c.ClientType='Legal Entity' then Gcobid
# MAGIC     when c.ClientType in('Natural Person acting in a Professional Capacity (NPPC)','Natural Person')then concat('NP_',Gcobid) else 'NA' end as EnA_UniqueGcobID
# MAGIC ,lc.LocalClientOwnerName
# MAGIC From party_case_client_details c
# MAGIC left join LocalClientOwner lc on c.SourceClient = lc.SourceClient
# MAGIC where c.CaseStatusName not in ('Cancelled','Completed')
# MAGIC and c.ClientType = 'Legal Entity'

# COMMAND ----------

# DBTITLE 1,Parameters for Excel Read
SITE_PATH = 'teams/GlobalFileTopics'
DRIVE_NAME = 'Documents'
FOLDER_NAME = '1. Excel Tracker'
FILE_NAME = 'FEC_CDD_GF_Tracker_v8.xlsx'
FILE_REL_PATH = f'{FOLDER_NAME}/{FILE_NAME}'

# COMMAND ----------

# DBTITLE 1,SharePoint connection to read Excel
def Read_file_from_Sharepoint(FILE_REL_PATH, SITE_PATH, DRIVE_NAME):

    TENANT_ID = os.environ["TENANT_ID"]
    CLIENT_ID = os.environ["APP_REG_APP_ID"]

    CLIENT_SECRET = dbutils.secrets.get(
        scope="connectedsecrets",
        key=f"appreg-{CLIENT_ID}"
    )

    HOSTNAME = "raboweb.sharepoint.com"

    AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
    SCOPE = ["https://graph.microsoft.com/.default"]
    GRAPH_API = "https://graph.microsoft.com/v1.0"

    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )

    token = app.acquire_token_for_client(scopes=SCOPE)

    if "access_token" not in token:
        raise RuntimeError(f"Token failure: {token}")

    headers = {"Authorization": f"Bearer {token['access_token']}"}

    # ---- Resolve Site ----
    site_resp = requests.get(
        f"{GRAPH_API}/sites/{HOSTNAME}:/{quote(SITE_PATH)}:",
        headers=headers
    )
    site_resp.raise_for_status()
    site_id = site_resp.json()["id"]

    # ---- Resolve Drive ----
    drives_resp = requests.get(
        f"{GRAPH_API}/sites/{site_id}/drives",
        headers=headers
    )
    drives_resp.raise_for_status()

    drives = drives_resp.json()["value"]
    drive_id = next(
        d["id"] for d in drives if d["name"] == DRIVE_NAME
    )

    # ---- Download Excel (IN MEMORY) ----
    download_url = (
        f"{GRAPH_API}/drives/{drive_id}/root:/{quote(FILE_REL_PATH)}:/content"
    )

    resp = requests.get(download_url, headers=headers)
    resp.raise_for_status()

    print("Excel file downloaded from SharePoint")

    #THIS IS WHAT WE RETURN
    return resp.content

# COMMAND ----------

# DBTITLE 1,Excel sheets to read
SHEETS_TO_READ = [ 
               '1. MAIN SCREEN'
               ,'CASES_DATA'
               ,'LL_DATA'
               ,'IL_DATA'
]

# COMMAND ----------

# DBTITLE 1,Function to read excel tab data
import datetime
import pandas as pd
from pyspark.sql.functions import col, to_timestamp, date_add, lit, when, to_date, regexp_replace

def create_temp_views_from_excel_bytes(
    excel_bytes,
    sheet_names
):
    excel_stream = io.BytesIO(excel_bytes)

    #Protected columns (never modify)
    PROTECTED_COLUMNS = ["gcob_case_id", "gcob_id"]

    for sheet in sheet_names:
        print(f"Reading sheet: {sheet}")

        # ---- Header handling ----
        if sheet == "1. MAIN SCREEN":
            header_row = 9
        else:
            header_row = 0

        pdf = pd.read_excel(
            excel_stream,
            engine="openpyxl",
            sheet_name=sheet,
            header=header_row
        )

        excel_stream.seek(0)

        # ---- Remove empty rows/columns ----
        pdf = pdf.dropna(how="all")
        #pdf = pdf.dropna(axis=1, how="all")

        #FIX 1: CLEAN COLUMN NAMES + HANDLE DUPLICATES
        cleaned_cols = [
            c.strip()
             .lower()
             .replace(" ", "_")
             .replace(".", "")
             .replace("(", "")
             .replace(")", "")
             .replace("-", "_")
             .replace("—", "_")   # IMPORTANT for Excel dash
            for c in pdf.columns
        ]

        #Ensure unique column names
        seen = {}
        final_cols = []

        for c in cleaned_cols:
            if c in seen:
                seen[c] += 1
                new_col = f"{c}_{seen[c]}"
            else:
                seen[c] = 0
                new_col = c
            final_cols.append(new_col)

        pdf.columns = final_cols

        #HANDLE EMPTY DATAFRAME
        if pdf.empty:
            print(f"Sheet '{sheet}' has no data. Creating empty temp view with schema.")

            schema = StructType([
                StructField(col_name, StringType(), True)
                for col_name in pdf.columns
            ])

            sdf = spark.createDataFrame([], schema)

            view_name = sheet.lower().replace(" ", "_").replace(".", "")
            sdf.createOrReplaceTempView(view_name)

            print(f"Empty temp view created with schema: {view_name}")

            continue  #IMPORTANT → skip rest of logic ONLY for empty sheets

        # ----NORMAL FLOW (ONLY IF DATA EXISTS) ----

        # ---- Normalize datetime & time ----
        for col_name in pdf.columns:
            if pd.api.types.is_datetime64_any_dtype(pdf[col_name]):
                pdf[col_name] = pdf[col_name].dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                pdf[col_name] = pdf[col_name].apply(
                    lambda x: (
                        x.strftime("%H:%M:%S")
                        if isinstance(x, datetime.time)
                        else x
                    )
                )

        # ---- Convert to string ----
        pdf = pdf.astype(str)

        # ---- Restore NULLs ----
        pdf = pdf.replace(["nan", "nat", "none"], None)

        # ---- Create Spark DataFrame ----
        sdf = spark.createDataFrame(pdf)

        #FIX 2: ROBUST DATE HANDLING (ROW-LEVEL)
        for c in sdf.columns:

            #Skip protected columns
            if c.lower() in PROTECTED_COLUMNS:
                continue

            col_lower = c.lower()

            #Only process date-like columns
            if any(k in col_lower for k in ["date", "dt", "review", "created", "completed", "modified"]):

                sdf = sdf.withColumn(
                    c,
                    when(
                        col(c).rlike("^[0-9]+(\\.0)?$"),  # Excel serial (int + float)
                        date_add(
                            lit("1899-12-30"),
                            regexp_replace(col(c), "\\.0$", "").cast("int")
                        )
                    )
                    .when(
                        col(c).rlike("^[0-9]{2}/[0-9]{2}/[0-9]{4}$"),  # dd/MM/yyyy
                        to_date(col(c), "dd/MM/yyyy")
                    )
                    .when(
                        col(c).rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}"),  # yyyy-MM-dd or datetime
                        to_timestamp(col(c)).cast("date")
                    )
                    .otherwise(col(c))  #Preserve text like "Not Required"
                )

        # ---- Create temp view ----
        view_name = (
            sheet.lower()
                 .replace(" ", "_")
                 .replace(".", "")
        )

        sdf.createOrReplaceTempView(view_name)

        print(f"Temp view created: {view_name}")

# COMMAND ----------

# DBTITLE 1,Create temp view from excel tab
excel_bytes = Read_file_from_Sharepoint(
    FILE_REL_PATH=FILE_REL_PATH,
    SITE_PATH=SITE_PATH,
    DRIVE_NAME=DRIVE_NAME
)

create_temp_views_from_excel_bytes(
    excel_bytes=excel_bytes,
    sheet_names=SHEETS_TO_READ
)


# COMMAND ----------

# DBTITLE 1,workitem to define business logic
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW WorkItem_CreatedDate AS
# MAGIC with Inprogress_workitem as
# MAGIC (
# MAGIC select distinct w.*  
# MAGIC from Client_Details c
# MAGIC inner join Global_Files gc on c.UniqueClientId = gc.UniquePartyId
# MAGIC inner join party_workitem w on c.SourceClient = w.SourceClient
# MAGIC where c.CaseStatusName not in ('Completed','Product fulfilment in progress')
# MAGIC and c.ReviewTypeName in ('Periodic Review')
# MAGIC )
# MAGIC ,workitem_status as
# MAGIC (
# MAGIC select * from Inprogress_workitem where CaseStatusTypeWhenCreated in (1,16)
# MAGIC -- 1 = 'Initiation In Progress' , 16 = 'GCOB Review in progress'
# MAGIC )
# MAGIC ,workitem_status_Row as
# MAGIC (
# MAGIC     select * ,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY ClientId ORDER BY WorkItemCreatedDate asc) AS ROWNUM
# MAGIC     FROM workitem_status 
# MAGIC )
# MAGIC ,WorkItem_CreatedDate as 
# MAGIC (
# MAGIC select * from workitem_status_Row where ROWNUM = 1
# MAGIC )
# MAGIC ,workitem_RF4Eye as
# MAGIC (
# MAGIC select * from Inprogress_workitem where CaseStatusTypeWhenCreated in (4)
# MAGIC -- 4 = 'Ready for 4 eye check' 
# MAGIC )
# MAGIC ,workitem_RF4Eye_Row as
# MAGIC (
# MAGIC     select * ,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY ClientId ORDER BY WorkItemCreatedDate asc) AS ROWNUM
# MAGIC     FROM workitem_RF4Eye 
# MAGIC )
# MAGIC ,WorkItem_RF4Eye_Final as 
# MAGIC (
# MAGIC select * from workitem_RF4Eye_Row where ROWNUM = 1
# MAGIC )
# MAGIC ,workitem_CO_signoff as
# MAGIC (
# MAGIC select * from Inprogress_workitem where CaseStatusTypeWhenCreated in (6)
# MAGIC -- 6 = 'Client owner sign off requested' 
# MAGIC )
# MAGIC ,workitem_CO_signoff_Row as
# MAGIC (
# MAGIC     select * ,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY ClientId ORDER BY WorkItemCreatedDate asc) AS ROWNUM
# MAGIC     FROM workitem_CO_signoff 
# MAGIC )
# MAGIC ,workitem_CO_signoff_Final as 
# MAGIC (
# MAGIC select * from workitem_CO_signoff_Row where ROWNUM = 1
# MAGIC )
# MAGIC ,completed_cases AS (
# MAGIC SELECT distinct
# MAGIC GcobId,
# MAGIC NextReviewDate
# MAGIC FROM Client_Details
# MAGIC WHERE CaseStatusName in ('Completed','Product fulfilment in progress')
# MAGIC )
# MAGIC ,combined_data AS (
# MAGIC SELECT distinct
# MAGIC w.GcobId,
# MAGIC w.CaseId,
# MAGIC w.SourceClient,
# MAGIC CAST(i.WorkItemCreatedDate AS DATE) AS WorkItemCreatedDate,
# MAGIC CAST(RF4Eye.WorkItemCreatedDate AS DATE) AS RF4Eye_CreatedDate,
# MAGIC CAST(so.WorkItemCreatedDate AS DATE) as CO_signoff_CreatedDate,
# MAGIC c.NextReviewDate
# MAGIC FROM Inprogress_workitem w
# MAGIC LEFT JOIN WorkItem_CreatedDate i ON w.GcobId = i.GcobId
# MAGIC LEFT JOIN completed_cases c ON w.GcobId = c.GcobId
# MAGIC left join WorkItem_RF4Eye_Final RF4Eye on w.GcobId = RF4Eye.GcobId
# MAGIC left join workitem_CO_signoff_Final so on w.GcobId = so.GcobId
# MAGIC )
# MAGIC SELECT distinct
# MAGIC GcobId,
# MAGIC CaseId,
# MAGIC SourceClient,
# MAGIC RF4Eye_CreatedDate,
# MAGIC CO_signoff_CreatedDate,
# MAGIC WorkItemCreatedDate,
# MAGIC NextReviewDate,
# MAGIC date_sub(NextReviewDate, 150) as NextReviewDate_150,
# MAGIC DATEDIFF(date_sub(NextReviewDate, 150),WorkItemCreatedDate) AS Days_Diff,
# MAGIC /*
# MAGIC CASE
# MAGIC     --GREEN (150 days or more before)
# MAGIC     WHEN datediff(NextReviewDate, WorkItemCreatedDate) >= 150
# MAGIC         THEN 'On Track'
# MAGIC     --ORANGE (147 to 149 days before)
# MAGIC     WHEN datediff(NextReviewDate, WorkItemCreatedDate) BETWEEN 147 AND 149
# MAGIC         THEN 'At Risk'
# MAGIC     --RED (less than 147 days)
# MAGIC     WHEN datediff(NextReviewDate, WorkItemCreatedDate) < 147
# MAGIC         THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS Cdd_Initiation
# MAGIC */
# MAGIC CASE
# MAGIC         --GREEN (on or before threshold)
# MAGIC     WHEN WorkItemCreatedDate <= date_sub(NextReviewDate, 150)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (1st to 3rd day after threshold)
# MAGIC     WHEN WorkItemCreatedDate BETWEEN date_add(date_sub(NextReviewDate, 150), 1)
# MAGIC         AND date_add(date_sub(NextReviewDate, 150), 3)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (4th day onwards after threshold)
# MAGIC     WHEN WorkItemCreatedDate >= date_add(date_sub(NextReviewDate, 150), 4)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS Cdd_Initiation
# MAGIC
# MAGIC --,IL.`IL CDD Analyst Name Provided — Date Completed`
# MAGIC FROM combined_data
# MAGIC --left join IL_DATA IL on combined_data.GcobId = IL.`GCOB ID`

# COMMAND ----------

# DBTITLE 1,Business Logic to derive columns for excel
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW GlobalFile_Excel AS
# MAGIC with IL_DATA_Date as
# MAGIC (
# MAGIC select distinct
# MAGIC gcob_case_id as CaseId
# MAGIC ,gcob_id as GcobId
# MAGIC ,CAST(`il_cdd_analyst_name_provided___date_completed` as DATE) as IL_CDD_Analyst_Name_Provided_Date_Completed
# MAGIC ,il_cdd_analyst_name_provided
# MAGIC ,local_documents_delivered
# MAGIC ,CAST(`local_documents_delivered___date_completed` as DATE) as local_documents_delivered_date_completed
# MAGIC ,rfi_feedback_provided
# MAGIC ,CAST(`rfi_feedback_provided___date_completed` as DATE) as rfi_feedback_provided_date_completed
# MAGIC ,il_local_add_ons_completed
# MAGIC ,CAST(`il_local_add_ons_completed___date_completed` as DATE) as il_local_add_ons_completed_date_completed
# MAGIC ,il_qc_completed
# MAGIC ,CAST(`il_qc_completed___date_completed` as DATE) as il_qc_completed_date_completed
# MAGIC ,il_cc_process_initiated
# MAGIC ,CAST(il_cc_process_initiated___date_completed as DATE) as il_cc_process_initiated_date_completed
# MAGIC ,il_approval_uploaded_in_gcob
# MAGIC ,CAST(il_approval_uploaded_in_gcob___date_completed as DATE) as il_approval_uploaded_in_gcob_date_completed
# MAGIC ,smso_sign_off_initiated
# MAGIC ,CAST(smso_sign_off_initiated___date_completed as DATE) as smso_sign_off_initiated_date_completed
# MAGIC from IL_DATA
# MAGIC )
# MAGIC ,LL_DATA_Date as
# MAGIC (
# MAGIC select distinct
# MAGIC gcob_id as GcobId
# MAGIC ,gcob_case_id as CaseId
# MAGIC ,draft_rfi_ready
# MAGIC ,CAST(`draft_rfi_ready___date_completed` as DATE) as draft_rfi_ready_date_completed
# MAGIC ,client_outreach_completed
# MAGIC ,CAST(`client_outreach_completed___date_completed` as DATE) as client_outreach_completed_date_completed
# MAGIC ,ll_local_add_ons_completion_request
# MAGIC ,CAST(`ll_local_add_ons_completion_request___date_completed` as DATE) as ll_local_add_ons_completion_request_date_completed
# MAGIC ,client_outreach_initiated
# MAGIC ,cast(client_outreach_initiated___date_completed as date) as client_outreach_initiated_date_completed
# MAGIC ,ra_qc_completed
# MAGIC ,cast(ra_qc_completed____date_completed as date) as ra_qc_completed_date_completed
# MAGIC ,mlro_advice_required
# MAGIC ,cast(mlro_advice_required___date_completed as date) as mlro_advice_required_date_completed
# MAGIC ,mlro_advice_initiated
# MAGIC ,cast(mlro_advice_initiated___date_completed as date) as mlro_advice_initiated_date_completed
# MAGIC ,mlro_advice_completed
# MAGIC ,cast(mlro_advice_completed___date_completed as date) as mlro_advice_completed_date_completed
# MAGIC ,ll_cc_process_initiated
# MAGIC ,cast(ll_cc_process_initiated___date_completed as date) as ll_cc_process_initiated_date_completed
# MAGIC ,ll_approval_uploaded_in_gcob
# MAGIC ,cast(ll_approval_uploaded_in_gcob___date_completed as date) as ll_approval_uploaded_in_gcob_date_completed
# MAGIC ,ll_smso_sign_off_initiated
# MAGIC ,cast(ll_smso_sign_off_initiated___date_completed as date) as ll_smso_sign_off_initiated_date_completed
# MAGIC from LL_DATA
# MAGIC )
# MAGIC ,IL_LL_Combined_DATA_Date as
# MAGIC (
# MAGIC select distinct
# MAGIC gcob_case_id as CaseId
# MAGIC ,gcob_id as GcobId 
# MAGIC ,il_compliance_advice_requested as compliance_advice_requested
# MAGIC ,cast(il_compliance_advice_requested___date_completed as date) as compliance_advice_requested_date_completed
# MAGIC ,il_advice_received as advice_received
# MAGIC ,cast(il_advice_received___date_completed as date) as advice_received_date_completed
# MAGIC ,il_approval_uploaded_in_gcob as approval_uploaded_in_gcob
# MAGIC ,CAST(il_approval_uploaded_in_gcob___date_completed as DATE) as approval_uploaded_in_gcob_date_completed
# MAGIC from IL_DATA
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select distinct 
# MAGIC gcob_case_id as CaseId
# MAGIC ,gcob_id as GcobId
# MAGIC ,ll_compliance_advice_requested as compliance_advice_requested
# MAGIC ,cast(ll_compliance_advice_requested___date_completed as date) as compliance_advice_requested_date_completed
# MAGIC ,ll_advice_received as advice_received
# MAGIC ,cast(ll_advice_received___date_completed as date) as advice_received_date_completed
# MAGIC ,ll_approval_uploaded_in_gcob as approval_uploaded_in_gcob
# MAGIC ,cast(ll_approval_uploaded_in_gcob___date_completed as date) as approval_uploaded_in_gcob_date_completed
# MAGIC from LL_DATA
# MAGIC )
# MAGIC ,IL_CDD_Analyst_Date_selection as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(IL_CDD_Analyst_Name_Provided_Date_Completed) as IL_CDD_Analyst_Name_Provided_Date_Completed
# MAGIC ,il_cdd_analyst_name_provided
# MAGIC from IL_DATA_Date
# MAGIC where il_cdd_analyst_name_provided = 'Yes' 
# MAGIC group by CaseId,GcobId,il_cdd_analyst_name_provided
# MAGIC )
# MAGIC ,local_documents_selection as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(local_documents_delivered_date_completed) as local_documents_delivered_date_completed
# MAGIC ,local_documents_delivered
# MAGIC from IL_DATA_Date
# MAGIC where local_documents_delivered = 'Yes' 
# MAGIC group by CaseId,GcobId,local_documents_delivered
# MAGIC )
# MAGIC ,LL_draft_rfi_selection as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(draft_rfi_ready_date_completed) as draft_rfi_ready_date_completed
# MAGIC ,draft_rfi_ready
# MAGIC from LL_DATA_Date
# MAGIC where draft_rfi_ready = 'Yes' 
# MAGIC group by CaseId,GcobId,draft_rfi_ready
# MAGIC )
# MAGIC ,IL_rfi_feedback_selection as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(rfi_feedback_provided_date_completed) as rfi_feedback_provided_date_completed
# MAGIC ,rfi_feedback_provided
# MAGIC from IL_DATA_Date
# MAGIC where rfi_feedback_provided = 'Yes' 
# MAGIC group by CaseId,GcobId,rfi_feedback_provided
# MAGIC )
# MAGIC ,LL_Outreach_Completed_selection as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(client_outreach_completed_date_completed) as client_outreach_completed_date_completed
# MAGIC ,client_outreach_completed
# MAGIC from LL_DATA_Date
# MAGIC where client_outreach_completed = 'Yes' 
# MAGIC group by CaseId,GcobId,client_outreach_completed
# MAGIC )
# MAGIC ,LL_Outreach_Initiated_selection as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(client_outreach_initiated_date_completed) as client_outreach_initiated_date_completed
# MAGIC ,client_outreach_initiated
# MAGIC from LL_DATA_Date
# MAGIC where client_outreach_initiated = 'Yes' 
# MAGIC group by CaseId,GcobId,client_outreach_initiated
# MAGIC )
# MAGIC ,IL_Local_add_ons_selection as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(il_local_add_ons_completed_date_completed) as il_local_add_ons_completed_date_completed
# MAGIC ,il_local_add_ons_completed
# MAGIC from IL_DATA_Date
# MAGIC where il_local_add_ons_completed = 'Yes' 
# MAGIC group by CaseId,GcobId,il_local_add_ons_completed
# MAGIC )
# MAGIC ,LL_Local_add_ons_selection as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(ll_local_add_ons_completion_request_date_completed) as ll_local_add_ons_completion_request_date_completed
# MAGIC ,ll_local_add_ons_completion_request
# MAGIC from LL_DATA_Date
# MAGIC where ll_local_add_ons_completion_request = 'Yes' 
# MAGIC group by CaseId,GcobId,ll_local_add_ons_completion_request
# MAGIC )
# MAGIC ,IL_LL_combined_advice as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,date_add(max(compliance_advice_requested_date_completed), 7) as compliance_advice_requested_date_completed_7
# MAGIC ,max(advice_received_date_completed) as advice_received_date_completed
# MAGIC ,max(approval_uploaded_in_gcob_date_completed) as approval_uploaded_in_gcob_date_completed
# MAGIC from IL_LL_Combined_DATA_Date
# MAGIC group by CaseId,GcobId
# MAGIC )
# MAGIC ,IL_QC_Completed as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(il_qc_completed_date_completed) as il_qc_completed_date_completed
# MAGIC ,il_qc_completed
# MAGIC from IL_DATA_Date
# MAGIC where il_qc_completed = 'Yes' 
# MAGIC group by CaseId,GcobId,il_qc_completed
# MAGIC )
# MAGIC ,LL_RA_QC_Completed as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(ra_qc_completed_date_completed) as ra_qc_completed_date_completed
# MAGIC ,ra_qc_completed
# MAGIC from LL_DATA_Date
# MAGIC where ra_qc_completed = 'Yes' 
# MAGIC group by CaseId,GcobId,ra_qc_completed
# MAGIC )
# MAGIC ,LL_mlro_advice_initiated as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(mlro_advice_initiated_date_completed) as mlro_advice_initiated_date_completed
# MAGIC ,mlro_advice_initiated
# MAGIC from LL_DATA_Date
# MAGIC where mlro_advice_initiated = 'Yes' 
# MAGIC group by CaseId,GcobId,mlro_advice_initiated
# MAGIC )
# MAGIC ,LL_mlro_advice_completed as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(mlro_advice_completed_date_completed) as mlro_advice_completed_date_completed
# MAGIC ,mlro_advice_completed
# MAGIC from LL_DATA_Date
# MAGIC where mlro_advice_completed = 'Yes' 
# MAGIC group by CaseId,GcobId,mlro_advice_completed
# MAGIC )
# MAGIC ,LL_cc_process_initiated as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(ll_cc_process_initiated_date_completed) as ll_cc_process_initiated_date_completed
# MAGIC ,ll_cc_process_initiated
# MAGIC from LL_DATA_Date
# MAGIC where ll_cc_process_initiated = 'Yes' 
# MAGIC group by CaseId,GcobId,ll_cc_process_initiated
# MAGIC )
# MAGIC ,LL_approval_uploaded_in_gcob as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(ll_approval_uploaded_in_gcob_date_completed) as ll_approval_uploaded_in_gcob_date_completed
# MAGIC ,ll_approval_uploaded_in_gcob
# MAGIC from LL_DATA_Date
# MAGIC where ll_approval_uploaded_in_gcob = 'Yes' 
# MAGIC group by CaseId,GcobId,ll_approval_uploaded_in_gcob
# MAGIC )
# MAGIC ,IL_cc_process_initiated as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(il_cc_process_initiated_date_completed) as il_cc_process_initiated_date_completed
# MAGIC ,il_cc_process_initiated
# MAGIC from IL_DATA_Date
# MAGIC where il_cc_process_initiated = 'Yes' 
# MAGIC group by CaseId,GcobId,il_cc_process_initiated
# MAGIC )
# MAGIC ,IL_approval_uploaded_in_gcob as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(il_approval_uploaded_in_gcob_date_completed) as il_approval_uploaded_in_gcob_date_completed
# MAGIC ,il_approval_uploaded_in_gcob
# MAGIC from IL_DATA_Date
# MAGIC where il_approval_uploaded_in_gcob = 'Yes' 
# MAGIC group by CaseId,GcobId,il_approval_uploaded_in_gcob
# MAGIC )
# MAGIC ,LL_smso_sign_off_initiated as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(ll_smso_sign_off_initiated_date_completed) as ll_smso_sign_off_initiated_date_completed
# MAGIC ,ll_smso_sign_off_initiated
# MAGIC from LL_DATA_Date
# MAGIC where ll_smso_sign_off_initiated = 'Yes' 
# MAGIC group by CaseId,GcobId,ll_smso_sign_off_initiated
# MAGIC )
# MAGIC ,IL_smso_sign_off_initiated as 
# MAGIC (
# MAGIC select distinct
# MAGIC CaseId
# MAGIC ,GcobId
# MAGIC ,max(smso_sign_off_initiated_date_completed) as smso_sign_off_initiated_date_completed
# MAGIC ,smso_sign_off_initiated
# MAGIC from IL_DATA_Date
# MAGIC where smso_sign_off_initiated = 'Yes' 
# MAGIC group by CaseId,GcobId,smso_sign_off_initiated
# MAGIC )
# MAGIC select distinct
# MAGIC w.gcobid
# MAGIC ,w.caseid
# MAGIC ,w.SourceClient
# MAGIC ,w.WorkItemCreatedDate
# MAGIC ,i.IL_CDD_Analyst_Name_Provided_Date_Completed
# MAGIC ,i.il_cdd_analyst_name_provided
# MAGIC ,CASE
# MAGIC       --GREEN (within 4 days)
# MAGIC   WHEN i.IL_CDD_Analyst_Name_Provided_Date_Completed <= date_add(w.WorkItemCreatedDate, 4)
# MAGIC   THEN 'On Track'
# MAGIC       --ORANGE (5th till 7th day) (fix this)
# MAGIC   WHEN i.IL_CDD_Analyst_Name_Provided_Date_Completed > date_add(w.WorkItemCreatedDate, 4)
# MAGIC          AND i.IL_CDD_Analyst_Name_Provided_Date_Completed <= date_add(w.WorkItemCreatedDate, 7)
# MAGIC   THEN 'At Risk'
# MAGIC         --RED (8th day or later)
# MAGIC   WHEN i.IL_CDD_Analyst_Name_Provided_Date_Completed >= date_add(w.WorkItemCreatedDate, 8)
# MAGIC   THEN 'Late'
# MAGIC   ELSE NULL
# MAGIC END AS Provide_IL_Analyst
# MAGIC ,l.local_documents_delivered
# MAGIC ,l.local_documents_delivered_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 8 days)
# MAGIC     WHEN l.local_documents_delivered_date_completed <= date_add(w.WorkItemCreatedDate, 8)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (9th to 11th day)
# MAGIC     WHEN l.local_documents_delivered_date_completed > date_add(w.WorkItemCreatedDate, 8)
# MAGIC          AND l.local_documents_delivered_date_completed <= date_add(w.WorkItemCreatedDate, 11)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (12th day or later)
# MAGIC     WHEN l.local_documents_delivered_date_completed >= date_add(w.WorkItemCreatedDate, 12)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS Provide_IL_Documents
# MAGIC ,r.draft_rfi_ready
# MAGIC ,r.draft_rfi_ready_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 13 days)
# MAGIC     WHEN r.draft_rfi_ready_date_completed <= date_add(w.WorkItemCreatedDate, 13)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (14th to 16th day)
# MAGIC     WHEN r.draft_rfi_ready_date_completed > date_add(w.WorkItemCreatedDate, 13)
# MAGIC          AND r.draft_rfi_ready_date_completed <= date_add(w.WorkItemCreatedDate, 16)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (17th day or later)
# MAGIC     WHEN r.draft_rfi_ready_date_completed >= date_add(w.WorkItemCreatedDate, 17)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS Initial_Assessment
# MAGIC ,f.rfi_feedback_provided
# MAGIC ,f.rfi_feedback_provided_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 4 days)
# MAGIC     WHEN f.rfi_feedback_provided_date_completed <= date_add(r.draft_rfi_ready_date_completed, 4)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (5th to 7th day)
# MAGIC     WHEN f.rfi_feedback_provided_date_completed > date_add(r.draft_rfi_ready_date_completed, 4)
# MAGIC          AND f.rfi_feedback_provided_date_completed <= date_add(r.draft_rfi_ready_date_completed, 7)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (8th day or later)
# MAGIC     WHEN f.rfi_feedback_provided_date_completed >= date_add(r.draft_rfi_ready_date_completed, 8)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS RFI_Review 
# MAGIC ,o.client_outreach_completed
# MAGIC ,o.client_outreach_completed_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 4 days)
# MAGIC     WHEN o.client_outreach_completed_date_completed <= date_add(f.rfi_feedback_provided_date_completed, 4)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (5th to 7th day)
# MAGIC     WHEN o.client_outreach_completed_date_completed > date_add(f.rfi_feedback_provided_date_completed, 4)
# MAGIC          AND o.client_outreach_completed_date_completed <= date_add(f.rfi_feedback_provided_date_completed, 7)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (8th day or later)
# MAGIC     WHEN o.client_outreach_completed_date_completed >= date_add(f.rfi_feedback_provided_date_completed, 8)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS Final_RFI 
# MAGIC ,oi.client_outreach_initiated_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 44 days)
# MAGIC     WHEN o.client_outreach_completed_date_completed <= date_add(oi.client_outreach_initiated_date_completed, 44)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (45th to 47th day)
# MAGIC     WHEN o.client_outreach_completed_date_completed > date_add(oi.client_outreach_initiated_date_completed, 44)
# MAGIC          AND o.client_outreach_completed_date_completed <= date_add(oi.client_outreach_initiated_date_completed, 47)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (48th day or later)
# MAGIC     WHEN o.client_outreach_completed_date_completed >= date_add(oi.client_outreach_initiated_date_completed, 48)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS Client_Outreach 
# MAGIC ,Il_add.il_local_add_ons_completed
# MAGIC ,ll_add.ll_local_add_ons_completion_request
# MAGIC ,ll_add.ll_local_add_ons_completion_request_date_completed
# MAGIC ,Il_add.il_local_add_ons_completed_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 7 days)
# MAGIC     WHEN Il_add.il_local_add_ons_completed_date_completed <= date_add(ll_add.ll_local_add_ons_completion_request_date_completed, 7)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (8th to 10th day)
# MAGIC     WHEN Il_add.il_local_add_ons_completed_date_completed > date_add(ll_add.ll_local_add_ons_completion_request_date_completed, 7)
# MAGIC          AND Il_add.il_local_add_ons_completed_date_completed <= date_add(ll_add.ll_local_add_ons_completion_request_date_completed, 10)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (11th day or later)
# MAGIC     WHEN Il_add.il_local_add_ons_completed_date_completed >= date_add(ll_add.ll_local_add_ons_completion_request_date_completed, 11)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS IL_Local_Requirements_Feedback 
# MAGIC ,w.RF4Eye_CreatedDate
# MAGIC ,CASE
# MAGIC         --GREEN (within 14 days)
# MAGIC     WHEN w.RF4Eye_CreatedDate <= date_add(o.client_outreach_completed_date_completed, 14)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (15th to 17th day)
# MAGIC     WHEN w.RF4Eye_CreatedDate > date_add(o.client_outreach_completed_date_completed, 14)
# MAGIC          AND w.RF4Eye_CreatedDate <= date_add(o.client_outreach_completed_date_completed, 17)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (18th day or later)
# MAGIC     WHEN w.RF4Eye_CreatedDate >= date_add(o.client_outreach_completed_date_completed, 18)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS Final_Assessment 
# MAGIC ,comp.compliance_advice_requested_date_completed_7
# MAGIC ,rec.advice_received_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 7 days)
# MAGIC     WHEN rec.advice_received_date_completed <= date_add(comp.compliance_advice_requested_date_completed_7, 7)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (8th to 10th day)
# MAGIC     WHEN rec.advice_received_date_completed > date_add(comp.compliance_advice_requested_date_completed_7, 7)
# MAGIC          AND rec.advice_received_date_completed <= date_add(comp.compliance_advice_requested_date_completed_7, 10)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (11th day or later)
# MAGIC     WHEN rec.advice_received_date_completed >= date_add(comp.compliance_advice_requested_date_completed_7, 11)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS Second_Line_Advice 
# MAGIC ,iq.il_qc_completed_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 3 days)
# MAGIC     WHEN iq.il_qc_completed_date_completed <= date_add(w.RF4Eye_CreatedDate, 3)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (4th to 6th day)
# MAGIC     WHEN iq.il_qc_completed_date_completed > date_add(w.RF4Eye_CreatedDate, 3)
# MAGIC          AND iq.il_qc_completed_date_completed <= date_add(w.RF4Eye_CreatedDate, 6)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (7th day or later)
# MAGIC     WHEN iq.il_qc_completed_date_completed >= date_add(w.RF4Eye_CreatedDate, 7)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS IL_QC 
# MAGIC ,raqc.ra_qc_completed_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 5 days)
# MAGIC     WHEN raqc.ra_qc_completed_date_completed <= date_add(w.RF4Eye_CreatedDate, 5)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (6th to 8th day)
# MAGIC     WHEN raqc.ra_qc_completed_date_completed > date_add(w.RF4Eye_CreatedDate, 5)
# MAGIC          AND raqc.ra_qc_completed_date_completed <= date_add(w.RF4Eye_CreatedDate, 8)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (9th day or later)
# MAGIC     WHEN raqc.ra_qc_completed_date_completed >= date_add(w.RF4Eye_CreatedDate, 9)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS RA_QC 
# MAGIC ,ai.mlro_advice_initiated_date_completed
# MAGIC ,ac.mlro_advice_completed_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 14 days)
# MAGIC     WHEN ac.mlro_advice_completed_date_completed <= date_add(ai.mlro_advice_initiated_date_completed, 14)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (15th to 17th day)
# MAGIC     WHEN ac.mlro_advice_completed_date_completed > date_add(ai.mlro_advice_initiated_date_completed, 14)
# MAGIC          AND ac.mlro_advice_completed_date_completed <= date_add(ai.mlro_advice_initiated_date_completed, 17)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (18th day or later)
# MAGIC     WHEN ac.mlro_advice_completed_date_completed >= date_add(ai.mlro_advice_initiated_date_completed, 18)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS MLRO_Advice 
# MAGIC ,cc.ll_cc_process_initiated_date_completed
# MAGIC ,appr.ll_approval_uploaded_in_gcob_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 25 days)
# MAGIC     WHEN appr.ll_approval_uploaded_in_gcob_date_completed <= date_add(cc.ll_cc_process_initiated_date_completed, 25)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (26th to 28th day)
# MAGIC     WHEN appr.ll_approval_uploaded_in_gcob_date_completed > date_add(cc.ll_cc_process_initiated_date_completed, 25)
# MAGIC          AND appr.ll_approval_uploaded_in_gcob_date_completed <= date_add(cc.ll_cc_process_initiated_date_completed, 28)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (29th day or later)
# MAGIC     WHEN appr.ll_approval_uploaded_in_gcob_date_completed >= date_add(cc.ll_cc_process_initiated_date_completed, 28)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS Lead_Location_Client_Committee 
# MAGIC ,ilcc.il_cc_process_initiated_date_completed
# MAGIC ,ilappr.il_approval_uploaded_in_gcob_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 7 days)
# MAGIC     WHEN ilappr.il_approval_uploaded_in_gcob_date_completed <= date_add(ilcc.il_cc_process_initiated_date_completed, 7)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (8th to 10th day)
# MAGIC     WHEN ilappr.il_approval_uploaded_in_gcob_date_completed > date_add(ilcc.il_cc_process_initiated_date_completed, 7)
# MAGIC          AND ilappr.il_approval_uploaded_in_gcob_date_completed <= date_add(ilcc.il_cc_process_initiated_date_completed, 10)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (11th day or later)
# MAGIC     WHEN ilappr.il_approval_uploaded_in_gcob_date_completed >= date_add(ilcc.il_cc_process_initiated_date_completed, 11)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS  Involved_Location_Client_Committee 
# MAGIC ,llsmso.ll_smso_sign_off_initiated_date_completed
# MAGIC ,CASE
# MAGIC         --GREEN (within 25 days)
# MAGIC     WHEN appr.ll_approval_uploaded_in_gcob_date_completed <= date_add(llsmso.ll_smso_sign_off_initiated_date_completed, 25)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (26th to 28th day)
# MAGIC     WHEN appr.ll_approval_uploaded_in_gcob_date_completed > date_add(llsmso.ll_smso_sign_off_initiated_date_completed, 25)
# MAGIC          AND appr.ll_approval_uploaded_in_gcob_date_completed <= date_add(llsmso.ll_smso_sign_off_initiated_date_completed, 28)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (29th day or later)
# MAGIC     WHEN appr.ll_approval_uploaded_in_gcob_date_completed >= date_add(llsmso.ll_smso_sign_off_initiated_date_completed, 29)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS Lead_Location_SMSO_Sign_Off
# MAGIC ,ilsmso.smso_sign_off_initiated_date_completed 
# MAGIC ,CASE
# MAGIC         --GREEN (within 7 days)
# MAGIC     WHEN ilappr.il_approval_uploaded_in_gcob_date_completed <= date_add(ilsmso.smso_sign_off_initiated_date_completed, 7)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (8th to 10th day)
# MAGIC     WHEN ilappr.il_approval_uploaded_in_gcob_date_completed > date_add(ilsmso.smso_sign_off_initiated_date_completed, 7)
# MAGIC          AND ilappr.il_approval_uploaded_in_gcob_date_completed <= date_add(ilsmso.smso_sign_off_initiated_date_completed, 10)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (11th day or later)
# MAGIC     WHEN ilappr.il_approval_uploaded_in_gcob_date_completed >= date_add(ilsmso.smso_sign_off_initiated_date_completed, 11)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS  Involved_Location_SMSO_Sign_Off
# MAGIC ,w.CO_signoff_CreatedDate
# MAGIC ,comp.approval_uploaded_in_gcob_date_completed 
# MAGIC ,CASE
# MAGIC         --GREEN (within 2 days)
# MAGIC     WHEN w.CO_signoff_CreatedDate <= date_add(comp.approval_uploaded_in_gcob_date_completed , 2)
# MAGIC     THEN 'On Track'
# MAGIC         --ORANGE (3rd to 5th day)
# MAGIC     WHEN w.CO_signoff_CreatedDate > date_add(comp.approval_uploaded_in_gcob_date_completed , 2)
# MAGIC          AND w.CO_signoff_CreatedDate <= date_add(comp.approval_uploaded_in_gcob_date_completed , 5)
# MAGIC     THEN 'At Risk'
# MAGIC         --RED (6th day or later)
# MAGIC     WHEN w.CO_signoff_CreatedDate >= date_add(comp.approval_uploaded_in_gcob_date_completed, 6)
# MAGIC     THEN 'Late'
# MAGIC     ELSE NULL
# MAGIC END AS  Final_QC
# MAGIC
# MAGIC from WorkItem_CreatedDate w
# MAGIC left join IL_CDD_Analyst_Date_selection i on w.Gcobid = i.gcobid and w.caseid = i.caseid
# MAGIC left join local_documents_selection l on w.Gcobid = l.gcobid and w.caseid = l.caseid
# MAGIC left join LL_draft_rfi_selection r on w.Gcobid = r.gcobid and w.caseid = r.caseid
# MAGIC left join IL_rfi_feedback_selection f on r.Gcobid = f.gcobid and r.caseid = f.caseid
# MAGIC left join LL_Outreach_Completed_selection o on w.Gcobid = o.gcobid and w.caseid = o.caseid
# MAGIC left join LL_Local_add_ons_selection ll_add on w.Gcobid = ll_add.gcobid and w.caseid = ll_add.caseid
# MAGIC left join IL_Local_add_ons_selection Il_add on ll_add.Gcobid = Il_add.gcobid and ll_add.caseid = Il_add.caseid
# MAGIC left join IL_LL_combined_advice comp on w.Gcobid = comp.gcobid and w.caseid = comp.caseid
# MAGIC left join IL_LL_combined_advice rec on w.Gcobid = rec.gcobid and w.caseid = rec.caseid
# MAGIC left join LL_Outreach_Initiated_selection oi on w.Gcobid = oi.gcobid and w.caseid = oi.caseid
# MAGIC left join IL_QC_Completed iq on w.Gcobid = iq.gcobid and w.caseid = iq.caseid
# MAGIC left join LL_RA_QC_Completed raqc on w.Gcobid = raqc.gcobid and w.caseid = raqc.caseid
# MAGIC left join LL_mlro_advice_initiated ai on w.Gcobid = ai.gcobid and w.caseid = ai.caseid
# MAGIC left join LL_mlro_advice_completed ac on w.Gcobid = ac.gcobid and w.caseid = ac.caseid
# MAGIC left join LL_cc_process_initiated cc on w.Gcobid = cc.gcobid and w.caseid = cc.caseid
# MAGIC left join LL_approval_uploaded_in_gcob appr on w.Gcobid = appr.gcobid and w.caseid = appr.caseid
# MAGIC left join IL_cc_process_initiated ilcc on w.Gcobid = ilcc.gcobid and w.caseid = ilcc.caseid
# MAGIC left join IL_approval_uploaded_in_gcob ilappr on w.Gcobid = ilappr.gcobid and w.caseid = ilappr.caseid
# MAGIC left join LL_smso_sign_off_initiated llsmso on w.Gcobid = llsmso.gcobid and w.caseid = llsmso.caseid
# MAGIC left join IL_smso_sign_off_initiated ilsmso on w.Gcobid = ilsmso.gcobid and w.caseid = ilsmso.caseid
# MAGIC

# COMMAND ----------

# DBTITLE 1,Combine WorkItem and Excel business logic columns
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW WorkItem_GlobalFile_Excel AS
# MAGIC select distinct 
# MAGIC w.gcobid
# MAGIC ,w.caseid
# MAGIC ,w.Cdd_Initiation
# MAGIC ,e.Provide_IL_Analyst
# MAGIC ,e.Provide_IL_Documents
# MAGIC ,e.Initial_Assessment
# MAGIC ,e.RFI_Review
# MAGIC ,e.Final_RFI
# MAGIC ,e.Client_Outreach
# MAGIC ,e.IL_Local_Requirements_Feedback
# MAGIC ,e.Final_Assessment
# MAGIC ,e.Second_Line_Advice
# MAGIC ,e.IL_QC
# MAGIC ,e.RA_QC
# MAGIC ,e.MLRO_Advice
# MAGIC ,e.Lead_Location_Client_Committee
# MAGIC ,e.Involved_Location_Client_Committee
# MAGIC ,e.Lead_Location_SMSO_Sign_Off
# MAGIC ,e.Involved_Location_SMSO_Sign_Off
# MAGIC ,e.Final_QC
# MAGIC From WorkItem_CreatedDate w
# MAGIC Inner join GlobalFile_Excel e on w.Gcobid = e.gcobid and w.caseid = e.caseid

# COMMAND ----------

# DBTITLE 1,Derive Global File Report
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Global_File_Report AS
# MAGIC WITH Prework AS (
# MAGIC SELECT distinct
# MAGIC SourceClient
# MAGIC ,MIN(WorkItemCreatedDate) AS Prework
# MAGIC FROM party_workitem
# MAGIC WHERE CaseStatusTypeWhenCreated IN (1, 16)
# MAGIC GROUP BY SourceClient
# MAGIC )
# MAGIC ,LatestCase AS
# MAGIC (
# MAGIC select distinct 
# MAGIC UniqueGcobId,max(CaseId) as Max_Case_Id
# MAGIC from Client_Details
# MAGIC group by UniqueGcobId 
# MAGIC )
# MAGIC
# MAGIC ,main as 
# MAGIC (
# MAGIC select distinct
# MAGIC c.SourceClient
# MAGIC ,c.GcobId
# MAGIC ,c.CaseId
# MAGIC ,c.FullLegalName
# MAGIC ,c.ReviewTypeName
# MAGIC ,c.CaseCompletedDate
# MAGIC ,c.NextReviewDate
# MAGIC ,c.ValidatedRiskLevel
# MAGIC ,c.GlobalClientOwner
# MAGIC ,c.LocalClientOwnerName
# MAGIC ,gc.LeadInvolved_Locations
# MAGIC ,c.UniqueGcobId_1 as UniqueGcobId
# MAGIC ,c.casestatusname
# MAGIC ,c.IsLatestApprovedVersionOfClient
# MAGIC ,c.CaseCreationDate
# MAGIC ,c.KYCAssessmentInProgressAssignee
# MAGIC ,c.LastKYCAnalyst
# MAGIC ,gc.UniquePartyId
# MAGIC ,ena.Overdue
# MAGIC ,ena.EDROverdue
# MAGIC ,ena.NewPROverdueLogic
# MAGIC ,ena.EDRDuedate
# MAGIC ,ena.Prework
# MAGIC ,date_sub(c.NextReviewDate, 120) AS TargetInitiationDay
# MAGIC ,case when c.casestatusname = 'Completed' then null else datediff(c.NextReviewDate, current_date()) end AS DaysToCompletionDuedate
# MAGIC ,c.EDL_LOAD_DTS as EDL_LOAD_DATE
# MAGIC ,ena.GlobalKYCPortfolioNew
# MAGIC ,ena.SectorTeam
# MAGIC ,ena.GlobalReportingRegion
# MAGIC ,ena.BusinessLineName
# MAGIC ,ena.FIHubIndicator_Derived
# MAGIC ,ena.ClientLifeCycleName
# MAGIC ,CASE
# MAGIC     --WHEN lower(c.CaseStatusName) = 'completed' AND date(c.CaseCompletedDate) <= date(c.NextReviewDate) THEN false
# MAGIC     --WHEN lower(c.CaseStatusName) = 'completed' AND date(c.CaseCompletedDate) > date(c.NextReviewDate) THEN true
# MAGIC     WHEN lower(c.CaseStatusName) = 'completed' then null
# MAGIC     WHEN lower(c.CaseStatusName) <> 'completed' AND current_date() > date(c.NextReviewDate) then 'true'
# MAGIC     ELSE 'false'
# MAGIC END AS radar_overdue
# MAGIC ,c.ClientType
# MAGIC --,CASE WHEN lower(c.CaseStatusName) = 'completed' then null else c.NextReviewDate end as CompletionDueDate
# MAGIC ,c.NextReviewDate as CompletionDueDate
# MAGIC ,c.FIHubIndicator
# MAGIC ,c.ClientCDDResponsibleLocation
# MAGIC ,case when c.CaseId = lc.Max_Case_Id then 'Yes' else 'No' end as Latest_Case_Flag
# MAGIC from Client_Details c
# MAGIC inner join Global_Files gc on c.SourceClient = gc.SourceClient
# MAGIC left join Prework pw on c.SourceClient = pw.SourceClient
# MAGIC left join radar.clients ena on c.EnA_UniqueGcobID = ena.UniqueGcobId
# MAGIC left join LatestCase lc on c.UniqueGcobId = lc.UniqueGcobId
# MAGIC )
# MAGIC select distinct * from main
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create DataFrame
df_global_file_report=spark.table('Global_File_Report')
df_global_file_object=spark.table('Global_Files_Object')
df_global_file_excel=spark.table('WorkItem_GlobalFile_Excel')

# COMMAND ----------

# DBTITLE 1,Add Business Date
df_global_file_report=df_global_file_report.withColumn("BusinessDate",lit(BusinessDate))

# COMMAND ----------

# DBTITLE 1,Write to Storage Account
save_to_saradar_storage_account(df_global_file_report, GlobalFile_dataobject)

# COMMAND ----------

# DBTITLE 1,Global File Object for PowerBI
save_to_saradar_storage_account(df_global_file_object, GF_dataobject)

# COMMAND ----------

# DBTITLE 1,Global File excel for PowerBI
save_to_saradar_storage_account(df_global_file_excel, GF_Excel)
