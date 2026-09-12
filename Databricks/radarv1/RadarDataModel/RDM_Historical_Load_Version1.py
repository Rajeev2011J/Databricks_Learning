# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To derive a notebook to load historical data for RadarDataModel Version 1 dataobjects
# MAGIC
# MAGIC #### author
# MAGIC - Mahalakshmi.Vengateswaran@rabobank.com
# MAGIC
# MAGIC ##### flow of logic
# MAGIC - Set Date Range and Convert to Datetime
# MAGIC   - Define the start and end dates for historical runs, then convert them into datetime objects for easy iteration.
# MAGIC
# MAGIC - Select Notebooks to Run
# MAGIC   - Prepare a list of notebooks to execute (Selected_List_of_Notebooks) from the full available list, ensuring only relevant notebooks are processed.
# MAGIC
# MAGIC - Loop Through Dates and Run Notebooks
# MAGIC   - For each notebook in the selected list, iterate through each date in the range, format the date as YYYYMMDD, and trigger the Databricks notebook using dbutils.notebook.run() with parameters (Load_Date and RunType).
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | | | | 

# COMMAND ----------

from datetime import datetime, timedelta

# Define start and end dates directly
start_date = "20250714"  # YYYYMMDD
end_date = "20250715"    # YYYYMMDD

# Convert to datetime objects
start_dt = datetime.strptime(start_date, "%Y%m%d")
end_dt = datetime.strptime(end_date, "%Y%m%d")

# Define the list of Notebooks
Selected_List_of_Notebooks=["RDM_Party_RadarKeyStore"]
List_of_Notebooks=["RDM_Party_SystemIdentifier","RDM_Party_KN1Cases","RDM_Party_AlternativeNames","RDM_Party_ClientOwnership","RDM_Party_CountryAffiliation","RDM_Party_Coverage","RDM_Party_Documents","RDM_Party_Identification","RDM_Party_RadarKeyStore","RDM_Party_Selection"]

# Loop through each date and call each notebook
while start_dt <= end_dt:
    for Notebook in Selected_List_of_Notebooks:
        load_date = start_dt.strftime("%Y%m%d")
        print(f"Running {Notebook} for historical date: {load_date}")
        
        dbutils.notebook.run(f"/Workspace/live/radarv1/files/RadarDataModel/{Notebook}", 3600, {
            "Load_Date": load_date,
            "RunType": "historical"
        })

            
    start_dt += timedelta(days=1)

