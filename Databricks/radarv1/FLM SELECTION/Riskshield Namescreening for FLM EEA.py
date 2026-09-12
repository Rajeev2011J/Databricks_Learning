# Databricks notebook source
# MAGIC %md
# MAGIC ## Goal:
# MAGIC To do data exploration for generating automated selections of Namescreening alerts
# MAGIC
# MAGIC #### Flow of logic:
# MAGIC - connect to tables
# MAGIC - read all of them
# MAGIC - get unique values for columns per domain value
# MAGIC - allign on columns that should match the selection criteria for FLM
# MAGIC
# MAGIC ##### PBI related
# MAGIC PBI: 13917138: [FLM automated selection] Select the scope of Namescreening alerts from the Riskshield GDP data and apply selection logic
# MAGIC - All selection is First-of-the-month. Looking back at all the selections of last month. 
# MAGIC
# MAGIC
# MAGIC #### Selection logic requirements
# MAGIC Percentage of alerts per portfolio
# MAGIC 0. UK: 7%
# MAGIC NL 3.5%
# MAGIC FI 3,5%
# MAGIC 1. Client Groups: Minimize overlaps
# MAGIC 2. NS Close codes classifications: include all different classifications that can be identified
# MAGIC 3. Inclusion of different Match Score percentages
# MAGIC  --> Go for an even spread at first, but prepare ourselves for a situation where we want more risk-based on percentage. 
# MAGIC 4. Inclusion of Legal entities and natural persons
# MAGIC 5. Over Night Screening team: An even spread of Analysts in the sample
# MAGIC 6. Alerts which are out of scope of screening teams/signals team excluded from the data received.
# MAGIC 7. Percentage per portfolio
# MAGIC a) 7% of population of each cathegory for UK and BP : Sanction/AM/PEP
# MAGIC b) 3.5% of population of each cathergory for NL and FI: Sanction/AM/PEP
# MAGIC 8. Exclude NSAAS alerts. (column 'Filename' = NSAAS )
# MAGIC 9. Exclude "no match/not relevant"* (column = .. )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - UK hub: Clients and RP related to where assigned ReviewLocation (via E&A portfolio) is: Corp Hub UK
# MAGIC   - CH London
# MAGIC   - Corporate Hub (not in current list - to ask Data Steward of system)
# MAGIC   - FOR UK hub, do not select CUSTOMER ID with L2 or CCDB
# MAGIC - Business Partners
# MAGIC   - Could come from 'CUSTOMER_ID' starting with L2 or CCDB
# MAGIC   - And then apply percentage.
# MAGIC - NL hub: Clients and RP related to where assigned ReviewLocation (via E&A portfolio) is: Corp Hub NL
# MAGIC   - CH Dublin
# MAGIC   - NL Wholesale (comes with Dublin included)
# MAGIC - FI hub: Clients and RP related to where assigned ReviewLocation (via E&A portfolio) is: FI Hub
# MAGIC   - Financial Institutions
# MAGIC - Asia:
# MAGIC   - China
# MAGIC     - SAN: 10% or 10 (highest)
# MAGIC     - PEP/AM: 0.75% or 20 (highest)
# MAGIC   - Hong kong 1% (might change)
# MAGIC     - SAN: 10% or 10 (highest)
# MAGIC     - PEP/AM: 0.75% or 20 (highest)
# MAGIC   - Singapore. 5%
# MAGIC     - 5% SAN
# MAGIC     - 5% PEP/AM
# MAGIC - Other
# MAGIC   - Rabo Foundation
# MAGIC   - Rabo Impact Foundation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Addition 31-03
# MAGIC - UK ; sample is too little. 7%, or at least 7 alerts.
# MAGIC - expecting extra logic for singapore
# MAGIC - include 'at-least-minimum' of logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Addition 08-06-2026
# MAGIC Selection:
# MAGIC 	- Try to select MORE of HIT: Match/Relevant (not per se in relation to 
# MAGIC 1. Match / relevenat
# MAGIC 2. Match not relevant
# MAGIC 3. NO hit: no match / relevance not investigated.
# MAGIC
# MAGIC
# MAGIC De-depulicate on CUSTOMER_NAME (even if 1 sanction, and 1 CDD, still select 1 ideally). 
# MAGIC
# MAGIC Can we also get the LISTCODE in the selection file. 
# MAGIC
# MAGIC Try to get balance between Adverse Media and PEP if possible. 
# MAGIC
# MAGIC
# MAGIC ==> ..
# MAGIC 	- Question is this use-able for month of May?
# MAGIC 		○ After the other codes are there
# MAGIC For population of PEP a percentage, and then population of AM another percentage. 

# COMMAND ----------

# DBTITLE 1,0. set up key variables
VARIABLE_DICT = {'Singapore': {
     'Sanctions': 5,
     'Sanctions_min': 0,
     'CDD': 5,
     'CDD_min': 0
},
                 'China': {
     'Sanctions': 10,
     'CDD': 0.75,
     'Sanctions_min': 10,
     'CDD_min': 20
},
                  'Hong Kong': {
     'Sanctions': 10,
     'CDD': 0.75,
     'Sanctions_min': 10,
     'CDD_min': 20
},  
                  'CH London': {
     'Sanctions': 7,
     'CDD': 7,
     'Sanctions_min': 7,
     'CDD_min': 7
}, 
                 'CH Dublin': {
     'Sanctions': 7,
     'CDD': 7,
     'Sanctions_min':  7,
     'CDD_min': 7
},
                  'NL Wholesale':{
     'Sanctions': 3.5,
     'CDD': 3.5,
     'Sanctions_min': 10,
     'CDD_min': 10
},
                   'Financial Institutions':{
     'Sanctions': 3.5,
     'CDD': 3.5,
     'Sanctions_min': 10,
     'CDD_min': 10
},
                   'Business Partners':{
     'Sanctions': 7,
     'CDD': 7,
     'Sanctions_min': 10,
     'CDD_min': 10
 },
                    'Rabobank Foundation':{
     'Sanctions': 100,
     'CDD': 15,
     'Sanctions_min': 0,
     'CDD_min': 20
 }                 
}


# COMMAND ----------

import os
import re
from datetime import datetime, timedelta

#Importing pyspark libraries and modules
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark import StorageLevel
import pandas as pd
import math
import numpy as np

# COMMAND ----------

application_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
tenant_id = os.environ['TENANT_ID']
environment=os.environ['ENV']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{application_id}")

# COMMAND ----------

def authenticate_storage_account(write_storage):
    # This method is for Configuring Spark to access SARADAR Storage account using OAuth authentication
    spark.conf.set("fs.azure.account.auth.type."+write_storage+".dfs.core.windows.net", "OAuth") 
    spark.conf.set("fs.azure.account.oauth.provider.type."+write_storage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
    spark.conf.set("fs.azure.account.oauth2.client.id."+write_storage+".dfs.core.windows.net", ""+application_id+"") 
    spark.conf.set("fs.azure.account.oauth2.client.secret."+write_storage+".dfs.core.windows.net", service_credential) 
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+write_storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")

    print('Connection to ' + write_storage +' storage account is successful.')

    return 

# COMMAND ----------

authenticate_storage_account(ReadStorage)

# COMMAND ----------

date_parameter = datetime.today().strftime('%Y%m%d')
load_dts = 'LOAD_DTS=' + date_parameter + '*'
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

# COMMAND ----------

load_df = pd.DataFrame({'GDPname':[
'rcmevent_v_gdp'
,'trx_v_alert_gdp'
,'trx_v_report_gdp'
]})
 
# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://rs-kjr@{ReadStorage}.dfs.core.windows.net/{row.GDPname}/1/data/').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# DBTITLE 1,0. Set daterange for selection scope
currentdate = datetime.now()
EndOfDateRange = (currentdate.replace(day=1)).date()
EndOfDateRange_str = EndOfDateRange.strftime('%Y-%m-%d')

print('EndOfDateRange :', EndOfDateRange)
StartOfDateRange = (EndOfDateRange -timedelta(1)).replace(day=1)
StartOfDateRange_str = StartOfDateRange.strftime('%Y-%m-%d')
print('StartOfDateRange :', StartOfDateRange)

# COMMAND ----------

# DBTITLE 1,1. Define SELECTION SCOPE
sdf_Selection_Scope = spark.sql(f""" 
                                WITH CTE_1 AS (
                                    SELECT * 
                                        , CASE 
                                            WHEN LEFT(CUSTOMER_ID,2) = 'L2' THEN 'Business Partners'
                                            WHEN LEFT(CUSTOMER_ID,4) = 'CCDB' THEN 'Business Partners'
                                            ELSE TENANT_NAME
                                            END AS SELECTION_PORTFOLIO

                                        , CASE
                                            WHEN FILENAME LIKE '%NSAAS%' THEN 'NSAAS-file'
                                            WHEN ( TENANT_NAME IN ( 'CH London', 'CH Dublin', 'NL Wholesale', 'Financial Institutions') AND CLOSECODESHORTNAME IN ("NO HIT: No match/Not relevant", "NO HIT: Match not investigated/Not relevant"  )) THEN 'Closecode in this Location is exlcuded'
                                            WHEN EVENT_IDENTIFICATION = 'Report' THEN 'EVENT Identification not relevant'
                                            ELSE 'In scope for sampling'
                                            END AS SELECTION_SCOPE_REASON 
                                        , CASE
                                            WHEN CLOSECODESHORTNAME = 'HIT: Match/Relevant' THEN 1
                                            WHEN CLOSECODESHORTNAME = 'NO HIT: Match/Not relevant' THEN 2
                                            WHEN CLOSECODESHORTNAME = 'NO HIT: No match/Relevance not investigated' THEN 3
                                            WHEN CLOSECODESHORTNAME = 'NO HIT: No match/Relevant' THEN 4
                                            WHEN CLOSECODESHORTNAME = 'NO HIT: No match/Not relevant' THEN 5
                                            WHEN CLOSECODESHORTNAME = 'NO HIT: Match not investigated/Not relevant' THEN 6
                                            ELSE 7
                                            END AS CLOSECODEPRIO
                                        , CASE  
                                            WHEN LISTCODE LIKE '%PEP%' THEN 'PEP'
                                            ELSE 'AM'
                                            END AS AM_OR_PEP

                                    FROM rcmevent_v_gdp
                                    WHERE 1=1 --CLOSECODESHORTNAME <> "NO HIT: No match/Not relevant" --as per business requirement
                                    AND TENANT_NAME IN ( 'Singapore', 'China',  'CH London', 'CH Dublin', 'Hong Kong', 'NL Wholesale', 'Financial Institutions', 'Rabobank Foundation')
                                    AND EVENT_CLOSUREDATE BETWEEN date('{StartOfDateRange_str}') AND date('{EndOfDateRange_str}')
                                    
                                    --AND FILENAME NOT LIKE '%NSAAS%'
                                    --AND NOT ( TENANT_NAME IN ( 'CH London', 'CH Dublin', 'NL Wholesale', 'Financial Institutions') AND CLOSECODESHORTNAME IN ("NO HIT: No match/Not relevant", "NO HIT: Match not investigated/Not relevant"  )
                                    --)
                                    -- AND MATCH_REASON or RELEVANCE REASON some specific mass-closure term.
                                )
                                    
                                    SELECT * FROM CTE_1


""")
sdf_Selection_Scope.createOrReplaceTempView('Selection_Scope')
# Filename from trx_v_alert_gdp Where Filename like '%NSAAS%'

df_Selection_Scope = spark.sql('SELECT * FROM selection_Scope where SELECTION_SCOPE_REASON = "In scope for sampling"').toPandas()


# COMMAND ----------

# DBTITLE 1,use this to delete current month selection
# MAGIC %skip
# MAGIC spark.sql(f"""delete from FLM.RiskShield_NameScreening_Scope where EVENT_CLOSUREDATE BETWEEN date('{StartOfDateRange_str}') AND date('{EndOfDateRange_str}')""")

# COMMAND ----------

# DBTITLE 1,1. Get how many there are in current selection scope
#Check if selectionscope was already stored.
FILES_IN_SEL_SCOPE = spark.sql(f"""select count(*) from FLM.RiskShield_NameScreening_Scope where EVENT_CLOSUREDATE BETWEEN date('{StartOfDateRange_str}') AND date('{EndOfDateRange_str}')""").toPandas().values[0][0]


# COMMAND ----------

# DBTITLE 1,1. Store total Scope as part of selectionScope if not done before this month
if FILES_IN_SEL_SCOPE > 0:
    # there's already a scope created before. No need to store again.
    print('files already in scope')
else:
    # adding selection scope to current selection
    sdf_Selection_Scope.drop('OTHERINFORMATION2').select("MATCHING_PERCENTAGE", "ASSIGNEDUSER", "CLOSECODESHORTNAME","CUSTOMER_TYPE", "EVENTID", "SELECTION_PORTFOLIO", 'EVENT_IDENTIFICATION', 'EVENT_CLOSUREDATE', 'MATCH_REASON','SELECTION_SCOPE_REASON', 'LISTCODE', 'AM_OR_PEP', "CUSTOMER_NAME").write.mode('append').option("mergeSchema", "true").saveAsTable('FLM.RiskShield_NameScreening_Scope')

# COMMAND ----------

# DBTITLE 1,2. TODO: Export numbers of summarized data
display(df_Selection_Scope[['CLIENT_ID','SELECTION_PORTFOLIO', 'EVENT_IDENTIFICATION']].groupby(['SELECTION_PORTFOLIO', 'EVENT_IDENTIFICATION']).count().reset_index())

# COMMAND ----------

# DBTITLE 1,3. To select these columns from final file
SELECT_COLUMNS = ['Assignee',	
                  'Case Class ID',	
                  'CloseCodeShortName',	
                  'Close Date',	
                  'Create Date',
                    'Event Class',	
                    'Event Id',	
                    'Event Key',	
                    'File ID',	
                    'File Name',	
                    'FIM Alert Key',	
                    'Date of Birth',	
                    'List Name'	,
                    'Importance',	
                    'Branch',	
                    'Rabo Name',	
                    'Client number',	
                    'Modified By',	
                    'Accuity OLC Dossier',	
                    'List Gender',	
                    'Original ID Accuity', 
                    'OLC'	,
                    'Original Source',	
                    'List',	
                    'Information 1',	
                    'Information 2', 
                    'List Date of Birth',	
                    'Match Reason',	
                    'ScoreName',	
                    'Matched Date of Birth',	
                    'Matched client Name',	
                    'Modified Date',	
                    'Native Name',	
                    'Client Type',	
                    'Information 3',	
                    'Information 4',	
                    'Relevance Reason',	
                    'Row Version',	
                    'Status',
                    'Tenant',	
                    'Trigger Count']
 
 

# COMMAND ----------

# DBTITLE 1,4. Define Sampling functions
def proportionate_stratified_sample(
    df, #expects pandas dataframe
    strata_cols,
    fraction_map,
    seed=42,
    stratum_col_name="stratum",
):
    #create one stratified column
    df["stratum"] = df[strata_cols].agg('_'.join, axis=1)

    sampled_df = df.sampleBy(col=stratum_col_name, fractions=fraction_map, seed=seed)

    return sampled_df




# COMMAND ----------

display(df_Selection_Scope.head(2))

# COMMAND ----------

# DBTITLE 1,4. De-duplicate selection scope and Prioritize higher-risk alerts
# assign the right order in table
df_Selection_Scope = df_Selection_Scope.sort_values(by = ['CLOSECODEPRIO' , 'CUSTOMER_NAME', 'MATCHING_PERCENTAGE'])


# de-duplicate table.
df_Selection_Scope_dedup = df_Selection_Scope.drop_duplicates(subset = ['CUSTOMER_NAME'], keep = 'first')



# COMMAND ----------

# DBTITLE 1,5. Making subselections - 1
#This will be a dict to loop over different dataframes
# TODO: within CDD, a  more defined split between PEP and AM via LISTCODE alerts? within alert_type
strata_cols = ["MATCHING_PERCENTAGE", "ASSIGNEDUSER", "CLOSECODESHORTNAME","CUSTOMER_TYPE", "AM_OR_PEP"]

df_dict = {}

for location in VARIABLE_DICT:
    #print(location)
    for alert_type in VARIABLE_DICT[location]:
        if 'min' not in alert_type:
            #print(alert_type)
            #print(VARIABLE_DICT[location][alert_type])

            sub_df_dedup = df_Selection_Scope_dedup[(df_Selection_Scope_dedup.EVENT_IDENTIFICATION == alert_type) &
                                        (df_Selection_Scope_dedup.SELECTION_PORTFOLIO == location) ]
            
            sub_df = df_Selection_Scope[(df_Selection_Scope.EVENT_IDENTIFICATION == alert_type) &
                            (df_Selection_Scope.SELECTION_PORTFOLIO == location) ]
            
            # The minimal sample size for this selection
            alert_type_min = VARIABLE_DICT[location][alert_type + '_min']
            
            # calculate the minimal lenght of sample: it can't be higher than len(sub_df), but should at least be max of len(sub_df_sample),  and Sanctions_min or CDD_min
            sub_df_sample = sub_df.sample(frac = VARIABLE_DICT[location][alert_type]/100)

            # is len(sub_df_dedup) not smaller than the aimed sample?

            #calculate actual number of sample
            print([len(sub_df_sample), alert_type_min])
            print(len(sub_df))
            print(max([len(sub_df_sample), alert_type_min]))

            final_selection_size = min([len(sub_df_dedup), max([len(sub_df_sample), alert_type_min])])

            # execute sample #TODO: Apply better logic for spread of group names.
            final_sample = sub_df_dedup.sample(n = final_selection_size  )

            final_sample['SelectionPortfolio'] = f'{location}_{alert_type}'

            #TODO: apply better sampling logic here! ? Use mahalakshmi functions below??
            #sub_df["stratum"] = sub_df[strata_cols].agg('_'.join, axis=1)


            df_dict[f'SAMPLE_{location}_{alert_type}'] = final_sample



# COMMAND ----------

# DBTITLE 1,6. Past all sample DF's together and show in 1 big df. transform to spark.
df_tot = pd.concat(df_dict.values())
final_sample = spark.createDataFrame(df_tot).drop('OTHERINFORMATION2')

# COMMAND ----------

display(final_sample.limit(5))

# COMMAND ----------

# DBTITLE 1,7. Set the MONTH_RUN_NR for that month
# MONTH_RUN_NUMBER:
# Check if a selection was already produced for that month
# IF NOT, then run number = 1. IF SO, then run number is max run number of that month + 1
# What to check/ 1. What month Is currently being referenced? 2. Of all selected alerts, how many are in that period. And what is their max run number?

MONTH_NR = spark.sql(f"""select max(MONTH_RUN_NUMBER) FROM FLM.RiskShield_NameScreening_Sampling WHERE EVENT_CLOSUREDATE BETWEEN date('{StartOfDateRange_str}') AND date('{EndOfDateRange_str}')""").toPandas().values[0][0]

if MONTH_NR is None or np.isnan(MONTH_NR):
    print('no selection yet for this month')
    MONTH_NR = 1
else:
    MONTH_NR += 1

print(MONTH_NR)
print(type(MONTH_NR))

# COMMAND ----------

final_sample = final_sample.withColumn("MONTH_RUN_NUMBER", F.lit(MONTH_NR).cast("bigint"))

# COMMAND ----------

display(final_sample.limit(3))

# COMMAND ----------

# DBTITLE 1,7. write into table.
final_sample.write.mode('append').option("mergeSchema", "true").saveAsTable('FLM.RiskShield_NameScreening_Sampling')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.FLM.RiskShield_NameScreening_Sampling limit 10

# COMMAND ----------

# DBTITLE 1,ONE-Off: Update table to have MONTH_RUN_NUMBER column
# MAGIC %skip
# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE hive_metastore.FLM.RiskShield_NameScreening_Sampling
# MAGIC ADD COLUMN (MONTH_RUN_NUMBER BIGINT );
# MAGIC
# MAGIC UPDATE hive_metastore.FLM.RiskShield_NameScreening_Sampling
# MAGIC SET MONTH_RUN_NUMBER = 1;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Store all data finally
# MAGIC %skip
# MAGIC final_sample.write.mode('overwrite').option("mergeSchema", "true").saveAsTable('wr_fj_parties_and_risk_assessment_preprd.radar.RS_NameScreening_Sampling')

# COMMAND ----------

display(df_tot)

# COMMAND ----------

# MAGIC %skip 
# MAGIC # UNIVERSAL EQUALâ€‘SPLIT BALANCING FUNCTION
# MAGIC ##?
# MAGIC def equal_split_balance(df, col_name, target, seed=42):
# MAGIC     groups = df.groupBy(col_name).agg(F.count("*").alias("cnt"))
# MAGIC     num_groups = groups.count()
# MAGIC     per_group = math.ceil(target / num_groups)
# MAGIC     w = Window.partitionBy(col_name).orderBy(F.rand(seed))
# MAGIC     df2 = df.withColumn("rn_grp", F.row_number().over(w))
# MAGIC
# MAGIC     balanced = df2.filter(F.col("rn_grp") <= per_group).drop("rn_grp")
# MAGIC     w2 = Window.orderBy(F.rand(seed))
# MAGIC     final = (
# MAGIC         balanced.withColumn("rn_final", F.row_number().over(w2))
# MAGIC         .filter(F.col("rn_final") <= target)
# MAGIC         .drop("rn_final")
# MAGIC     )
# MAGIC
# MAGIC     return final
# MAGIC
# MAGIC
# MAGIC # PRIORITY WEIGHTING (Matching % logic)
# MAGIC ##?
# MAGIC def apply_priority_weight(df, column, conditions, target):
# MAGIC     expr = None
# MAGIC
# MAGIC     for thresh, weight in conditions:
# MAGIC         if thresh != "else":
# MAGIC             cond = F.col(column) >= thresh
# MAGIC             if expr is None:
# MAGIC                 expr = F.when(cond, F.lit(weight))
# MAGIC             else:
# MAGIC                 expr = expr.when(cond, F.lit(weight))
# MAGIC         else:
# MAGIC             expr = expr.otherwise(F.lit(weight))
# MAGIC
# MAGIC     df = df.withColumn("priority_weight", expr)
# MAGIC     df = df.withColumn("priority_rand", F.rand() * F.col("priority_weight"))
# MAGIC
# MAGIC     w = Window.orderBy(F.col("priority_rand").desc())
# MAGIC
# MAGIC     df = df.withColumn("rn_p", F.row_number().over(w))
# MAGIC     df = df.filter(F.col("rn_p") <= target)
# MAGIC
# MAGIC     return df.drop("rn_p", "priority_weight", "priority_rand")

# COMMAND ----------

# MAGIC  %skip
# MAGIC  %sql
# MAGIC  select distinct CLOSECODESHORTNAME
# MAGIC  from rcmevent_v_gdp 
# MAGIC  --WHERE TENANT_NAME LIKE '%Wholesale%'

# COMMAND ----------

# MAGIC  %skip
# MAGIC  %sql 
# MAGIC select * 
# MAGIC from rcmevent_v_gdp 
# MAGIC WHERE TENANT_NAME LIKE '%Wholesale%'
# MAGIC AND CUSTOMER_ID LIKE '%RNP%'
# MAGIC limit 10
# MAGIC -- CUSTOMER_ID,? 
# MAGIC -- type would be RNP, RLE or number (but CUSTOMER TYPE will distinguish between Customer NP and Customer LE)

# COMMAND ----------

# DBTITLE 1,check for how to split sanctions vs other
# MAGIC  %skip
# MAGIC  %sql
# MAGIC SELECT DISTINCT EVENT_IDENTIFICATION FROM rcmevent_v_gdp 

# COMMAND ----------

# MAGIC  %skip
# MAGIC  %sql
# MAGIC SELECT DISTINCT LIST_ORIGINALSOURCE FROM rcmevent_v_gdp 

# COMMAND ----------

# MAGIC  %skip
# MAGIC  %sql
# MAGIC SELECT DISTINCT LISTCODE FROM rcmevent_v_gdp 

# COMMAND ----------




