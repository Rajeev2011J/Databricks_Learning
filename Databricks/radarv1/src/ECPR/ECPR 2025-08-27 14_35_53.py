# Databricks notebook source
#import openpyxl

# COMMAND ----------

#%pip install openpyxl

# COMMAND ----------

from pyspark.sql.functions import col, expr, lit, length, size, split, regexp_replace, current_date, add_months
from functools import reduce
import pandas as pd


# COMMAND ----------

# Set the file paths
confirmed_path = "/FileStore/tables/Utrecht-4.csv"
empty_path = "/FileStore/tables/Collaterals_search_empty_fields_export-4.csv" #convert to csv
#ecpr_path = "/FileStore/ECPR/ECPRS_Report.csv"
##linking_path = "/dbfs/FileStore/ECPR/Total_Non_Linking_Analysis.xlsx"

# COMMAND ----------

# Load confirmed file CSV into a Spark DataFrame
confirmed = spark.read.option("header", "true").option("encoding", "UTF-16").option("inferSchema", "true").option("delimiter",",").csv(confirmed_path)
# Load empty file CSV into a Spark DataFrame
empty = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",";").csv(empty_path)
# Load ecpr file CSV into a Spark DataFrame
#ecpr = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",";").csv(ecpr_path)
# Load linking file CSV into a Spark DataFrame
##lindf = pd.read_excel(linking_path, sheet_name="Facility")
##linking = spark.createDataFrame(lindf)


# COMMAND ----------

empty_fields = empty.select("Meta_Source_System_Code","Meta_Booking_Entity_Code","Key_Business_line","Key_Business_line_description","COL_Type","COL_ID","COL_Documentation_ID_available","COL_Documentation_ID","COL_Legal_Opinion_date","COL_Policy_Advance_rate_Indicator","RE_Property_National_registrationnr","RE_Renovation","RE_Year_Of_Renovation","RE_State_Of_Completion","RE_Original_Collateral_valuation_method","RE_Size_Units","RE_Size","RE_NLCRE_Detailed_Rank_Of_Security","RE_NLCRE_Type_Of_Mortgage","RE_NLCRE_Total_Mortage_Amount","INV_Current_Quality","REC_Current_Quality","EQ_Type_Description","EQ_Current_Quality","Project_State_Of_Completion","FA_Equity_nr","UFC_Guarantor_Local_ID","UFC_Guarantor_Global_ID","Closed")
empty_fields = empty_fields.filter(
    (col("Meta_Source_System_Code") == "ECPR") & 
    (col("Meta_Booking_Entity_Code") == 5201) & 
    (col("Key_Business_line").isin("COLEN", "PJFIN", "REFWR", "TCFEF", "ACFIN"))
)
empty_fields = empty_fields.filter(reduce(lambda a, b: a | b, [col(c) == "EMPTY_FIELD" for c in empty_fields.columns]))
empty_fields = empty_fields.withColumn("Hardcoded", expr("substring(COL_ID, 9)"))

# COMMAND ----------

not_closed = confirmed.select("CPID","Status","Last_actioned_by","Last_actioned_date","CD_Customer_WWID","CD_Legal_Name","CPLSD_Transaction_booking_Entity","GI_CP_Type","RCI_Risk_entities_business_lines_covered_details_Business_Line")
not_closed = not_closed.filter(
    (col("Status") != "Confirmed") & 
    (col("Status") != "Closed") &
    (col("CPLSD_Transaction_booking_Entity").startswith("5201")) & 
    (col("Last_actioned_date") > add_months(current_date(), -2)) & 
    ~(col("RCI_Risk_entities_business_lines_covered_details_Business_Line").startswith("ASBAF"))
)

# COMMAND ----------

#ecpr = ecpr.withColumn("count", 1+ length(col("Key_Local_facility_ID")) - length(regexp_replace(col("Key_Local_facility_ID"), r"\|", "")))

# COMMAND ----------

##non_linking = linking.filter(
##    (col("Business line") != "TCFAG") &
##    (col("Upload Source System CPR") == "ECPR") &
##    (col("Col_ID CPR").startswith("5201")) &
##    (col("Originating Entity CRM") == 5201)
##)
#non_linking = non_linking.withColumn("Hardcoded", expr("substring(`Col_ID CPR`, 9)"))
##v = non_linking.groupBy("Col_ID CPR").count()
##non_linking = non_linking.join(v, on="Col_ID CPR", how="left")

# COMMAND ----------

empty_fields.createOrReplaceTempView("empty_fields")
##non_linking.createOrReplaceTempView("non_linking")
confirmed.createOrReplaceTempView("confirmed")
#ecpr.createOrReplaceTempView("ecpr")

empty_fields = spark.sql("""
SELECT
  c.Status AS Confirmed,
  c.Last_actioned_by AS Last_actioned_by,
  c.Last_actioned_date AS Last_actioned_date,
  c.CD_Legal_Name AS CD_Legal_Name,
  e.*
FROM empty_fields e
LEFT JOIN confirmed c ON e.Hardcoded = c.CPID
  """)
empty_fields = empty_fields.drop('Hardcoded')

# COMMAND ----------

# non_linking = spark.sql("""
# SELECT
#   c.Status AS `Status of facilities`,
#   c.Last_actioned_date AS `Datum verlopen`,
#   c.CD_Legal_Name AS `CD Legal Name`,
#   c.Last_actioned_by AS `Last actioned by`,
#   pr.count AS count_ecpr,
#   l.*
# FROM non_linking l
# LEFT JOIN confirmed c ON l.Hardcoded = c.CPID
# LEFT JOIN ecpr pr ON l.`Col_ID CPR` = pr.COL_ID
#   """)

# non_linking = non_linking.filter(
#     (col("count_ecpr") == col("count")) &
#     (col("Status of facilities") == "Confirmed") |
#     (col("Status of facilities").isNull()) 
#     )
# non_linking = non_linking.drop('count_ecpr').drop("count").drop('Hardcoded')


# COMMAND ----------

display(empty_fields)
##display(non_linking)
display(not_closed)

# COMMAND ----------

##empty_fields.write.csv("/FileStore/ECPR/empty_fields.csv", header=True, mode="overwrite")
##https://adb-2475554465308574.14.azuredatabricks.net/files/ECPR/empty_fields.csv
##non_linking.write.csv("/FileStore/ECPR/non_linking.csv", header=True, mode="overwrite")
##https://adb-2475554465308574.14.azuredatabricks.net/files/ECPR/non_linking.csv
##not_closed.write.csv("/FileStore/ECPR/not_closed.csv", header=True, mode="overwrite")
##https://adb-2475554465308574.14.azuredatabricks.net/files/ECPR/not_closed.csv
