# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To derive a notebook to load historical data for RadarDataModel Version 2 dataobjects
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
import os

# Define start and end dates directly

start_date = os.getenv("START_DATE5")
end_date = os.getenv("END_DATE5")

Selected_List_of_Notebooks = [
    notebook.strip()
    for notebook in os.getenv("SELECTED_NOTEBOOKS5", "").split(",")
    if notebook.strip()
]


# Convert to datetime objects
start_dt = datetime.strptime(start_date, "%Y%m%d")
end_dt = datetime.strptime(end_date, "%Y%m%d")


List_of_Notebooks = [
    "RDM_Party_SystemIdentifier",
    "RDM_Party_KN1Cases",
    "RDM_Party_RadarKeyStore",
    "RDM_CDDCase_GRAM",
    "RDM_CDDCase_QuestionAnswer_GRAM",
    "RDM_Party_CDDCase_RiskCategories",
    "RDM_Party_AlternativeNames",
    "RDM_Party_ClientOwnership",
    "RDM_Party_Coverage",
    "RDM_Party_Documents",
    "RDM_Party_Identification",
    "RDM_Party",
    "RDM_Party_role",
    "RDM_Party_Selection",
    "RDM_Party_Structure",
    "RDM_Internal_SystemComparison",
    "RDM_Party_Address",
    "RDM_Party_BusinessActivities",
    "RDM_Party_CountryAffiliation"
]

# Track failures
failed_runs = []

# Loop through each date and call each notebook
while start_dt <= end_dt:
    for Notebook in Selected_List_of_Notebooks:
        load_date = start_dt.strftime("%Y%m%d")
        print(f"Running {Notebook} for historical date: {load_date}")
        
        try:
            dbutils.notebook.run(
              f"/Workspace/Users/sowmyashree.parashivamurthy.sudha@rabobank.com/R-FEC-RADAR/Databricks/radarv1/RadarDataModel/RadarDataModel_Version3/{Notebook}",
                3600,
                {
                    "Load_Date": load_date,
                    "RunType": "historical"
                }
            )
        except Exception as e:
            error_msg = f"FAILED: {Notebook} for date {load_date} | Error: {str(e)}"
            # print(error_msg)
            
            # Store failure for later analysis
            failed_runs.append({
                "Notebook": Notebook,
                "Date": load_date
                # ,"Error": str(e)
            })
    
    start_dt += timedelta(days=1)


# COMMAND ----------

# Optional: Print summary of failures at the end
print("\n==== FAILED RUN SUMMARY ====")
for fail in failed_runs:
    print(fail)

# COMMAND ----------

from pyspark.sql import SparkSession

# Convert list to DataFrame
failed_df = spark.createDataFrame(failed_runs)

display(failed_df)

# Save as a table (overwrite or append as needed)
failed_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("radar.failed_notebook_runs")
