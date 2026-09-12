# Databricks notebook source
# MAGIC %md
# MAGIC ## TF alert filtering towards FLM W&R for Transactions.
# MAGIC Goal: to export for each month (dec at start) the alerts that are generated on W&R clients. 
# MAGIC To be selected according to ACAMS selection methodology.
# MAGIC
# MAGIC - For future: combi for 70% business-rule risk-based & 30% random
# MAGIC
# MAGIC ###### <span style="color:orange">Request for Delivery:</span>
# MAGIC FEC OPS Wholesale Europe. 
# MAGIC to deliver quarterly and monthly overview of 
# MAGIC 1) alerts numbers
# MAGIC 2) alert selection and relevant data attributes
# MAGIC  - tbd in agile fashion.
# MAGIC
# MAGIC ##### Data elements
# MAGIC - split in Incoming / Outgoing
# MAGIC
# MAGIC
# MAGIC ###### <span style="color:blue">Flow of the logic:</span>
# MAGIC 1.  Connect to GDP sources
# MAGIC     - Siebel Arrangements (delta table)
# MAGIC     - wnh - alerts
# MAGIC     - wnh - transactions
# MAGIC     - wcm - alerts
# MAGIC     - wcm - transactions.
# MAGIC     - ? wcm alert-match
# MAGIC     - ? wlm alert-match
# MAGIC
# MAGIC
# MAGIC ### Business logic
# MAGIC 1 transactie kan meerdere alerts hebben
# MAGIC 1 alerts kan meerdere matches hebben. 
# MAGIC
# MAGIC stel: swift transactie, met link naar rusland. 
# MAGIC misschien 1 alert op Rusland, en ook 1 alert op naam acmed ali. En binnen de achmed alert, kunnen 2 matches zitten met 2 verschillende sanctie lijsten.
# MAGIC
# MAGIC Meestal 1 of 2 alerts per transactie. 
# MAGIC Maar kan ook t/m 20 zijn.
# MAGIC
# MAGIC ### Output;
# MAGIC data from transaction
# MAGIC data from alerts
# MAGIC
# MAGIC - Matched input/ input
# MAGIC - Comment_Text
# MAGIC - Status (hit, no hit etc.)
# MAGIC - Reason
# MAGIC - Listname/List_individual
# MAGIC - Score
# MAGIC
# MAGIC ## Context
# MAGIC Bij Swift zouden beide inkomende en uitgaande berichten gescreend worden.
# MAGIC Bij SEPA zou er maar 1 richting zijn, dus beter om in alle gevallen zowel naar alle Creditor én alle debtor rekeningen te kijken.
# MAGIC we doen geen filtering op binnenlandse boekingen
# MAGIC vooral internationaal betalingsverkeer. Kans dat we een internationale betalingen
# MAGIC
# MAGIC

# COMMAND ----------

# simple command to start as test
import pandas as pd
import os

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

ReadStorage

# COMMAND ----------

from datetime import datetime
currentDate = datetime.today().strftime('%Y%m%d')
currentYear = datetime.today().strftime('%Y')
currentMonth = datetime.today().strftime('%m')
currentDay = datetime.today().strftime('%d')

print(currentDate, currentYear, currentMonth, currentDay)

# COMMAND ----------

# MAGIC %md
# MAGIC <mark color = red>**Alerts**</mark>

# COMMAND ----------

# creating sql views
source_list = [
    'wnh_alert'
    ,'wnh_transaction'
    ,'wnh_match'
    ,'wcm_alert'
    ,'wcm_transaction'
    , 'wcm_match'
]

for data_object in source_list:
    source = data_object[:3]

    # selecting sources
    if source == 'wnh':
        version = 1
    if source == 'wcm':
        version = 0
    
    #print(source)
    #print(version)
    #create sql view
    spark.read.load(f'abfss://{source}@edlcorestdeuprod0001.dfs.core.windows.net/{data_object}/{version}/data/event_dt=202[5]*/', format='parquet').createOrReplaceTempView(data_object)


# COMMAND ----------

dbutils.fs.ls(f'abfss://{source}@edlcorestdeuprod0001.dfs.core.windows.net/')

# COMMAND ----------

# siebel arrangements for bank code
# 
df_AR = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_ar_hist/2/data/', format='delta').select(
    "ar_ac_iban", "cmrcl_pd_tp_code" ,"admn_ggm_code", "bnk_code", "ar_prim_org_rel_id", "edl_valid_to_dts"
    )
df_AR = df_AR.dropDuplicates(subset = ['ar_ac_iban'])
df_AR.createOrReplaceTempView('siebel_acct')

# COMMAND ----------

df_AR2 = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_ar_hist/2/data/', format='delta')
df_AR2 = df_AR2.dropDuplicates(subset = ['ar_ac_iban'])
df_AR2.createOrReplaceTempView('ar2')

# COMMAND ----------

# df REL_X_AR
df_REL_X_AR = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_ar_hist/3/data/', format='delta')
df_REL_X_AR = df_REL_X_AR.filter(df_REL_X_AR.edl_valid_to_dts > datetime.today() )
df_REL_X_AR = df_REL_X_AR.dropDuplicates(subset = ['ar_ac_iban'])
df_REL_X_AR.createOrReplaceTempView('rel_x_ar')

# ar_ac_iban
# dbutils.fs.ls('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_ar_hist/1/data/')

# COMMAND ----------

# df ORG
df_ORG = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_org_hist/2/data/', format='delta')
df_ORG = df_ORG.filter(df_ORG.edl_valid_to_dts > datetime.today() )
df_ORG.createOrReplaceTempView('org')

# COMMAND ----------

# df NP
df_NP = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_np_hist/2/data/', format='delta')
df_NP = df_NP.filter(df_NP.edl_valid_to_dts > datetime.today() )
df_NP.createOrReplaceTempView('NP')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Siebel_SEL AS
# MAGIC (
# MAGIC   Select t1.REL_ID
# MAGIC   , t1.BNK_CODE 
# MAGIC   , t2.ar_ac_iban
# MAGIC
# MAGIC   FROM NP t1
# MAGIC   LEFT JOIN rel_x_ar t2 on t1.rel_id = t2.rel_id
# MAGIC
# MAGIC   UNION
# MAGIC
# MAGIC   Select t1.REL_ID
# MAGIC   , t1.BNK_CODE 
# MAGIC   , t2.ar_ac_iban
# MAGIC
# MAGIC   FROM ORG t1
# MAGIC   LEFT JOIN rel_x_ar t2 on t1.rel_id = t2.rel_id
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wnh_alert limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wnh_match limit 10

# COMMAND ----------

# DBTITLE 1,select WR alerts for Bank code
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW WNH_SEL AS
# MAGIC SELECT 
# MAGIC 'WNH' AS ALERT_SYSTEM
# MAGIC , t1.RULE AS ALERT_RULE
# MAGIC , t1.ALERT_ID
# MAGIC , t1.ORG_UNIT
# MAGIC , t2.EVENT_DATE
# MAGIC , t2.TRANSACTION_KEY
# MAGIC , DATASOURCE 
# MAGIC , DIRECTION
# MAGIC , CREDITOR_ACCOUNT
# MAGIC , DEBTOR_ACCOUNT
# MAGIC , CREDITOR_NAME
# MAGIC , DEBTOR_NAME
# MAGIC   , t3.LISTNAME
# MAGIC   , t3.LIST_INDIVIDUAL
# MAGIC   , t3.ENTITY_NAME
# MAGIC   , t3.MATCHED_LIST
# MAGIC   , t3.INPUT
# MAGIC   , t3.SCORE
# MAGIC   , t3.TRIAGE
# MAGIC , CASE
# MAGIC      WHEN DIRECTION = 'I' THEN CREDITOR_ACCOUNT
# MAGIC      WHEN DIRECTION = 'O' THEN DEBTOR_ACCOUNT
# MAGIC    END AS RABO_PARTY_ACCOUNT
# MAGIC
# MAGIC    FROM wnh_alert t1
# MAGIC    LEFT JOIN wnh_transaction t2 ON t1.transaction_key = t2.transaction_key
# MAGIC LEFT JOIN wnh_match t3 ON t1.ALERT_ID = t3.ALERT_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW WCM_SEL AS
# MAGIC SELECT 
# MAGIC   'WCM' AS ALERT_SYSTEM
# MAGIC   , t1.RULE AS ALERT_RULE
# MAGIC   , t1.ALERT_ID 
# MAGIC   , t1.ORG_UNIT
# MAGIC   , t2.EVENT_DATE
# MAGIC   , t2.TRANSACTION_KEY
# MAGIC   , t2.DATASOURCE 
# MAGIC   , t2.DIRECTION
# MAGIC   , t2.CREDITOR_ACCOUNT
# MAGIC   , t2.DEBTOR_ACCOUNT
# MAGIC   , t2.CREDITOR_NAME
# MAGIC   , t2.DEBTOR_NAME
# MAGIC   , t3.LISTNAME
# MAGIC   , t3.LIST_INDIVIDUAL
# MAGIC   , t3.ENTITY_NAME
# MAGIC   , t3.MATCHED_LIST
# MAGIC   , t3.INPUT
# MAGIC   , t3.SCORE
# MAGIC   , t3.TRIAGE
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t2.DIRECTION = 'I' THEN t2.CREDITOR_ACCOUNT
# MAGIC       WHEN t2.DIRECTION = 'O' THEN t2.DEBTOR_ACCOUNT
# MAGIC     END AS RABO_PARTY_ACCOUNT
# MAGIC
# MAGIC    FROM wcm_alert t1
# MAGIC    LEFT JOIN wcm_transaction t2 ON t1.transaction_key = t2.transaction_key
# MAGIC    left join wcm_match t3 on t1.ALERT_ID = t3.ALERT_ID

# COMMAND ----------

# DBTITLE 1,select all together
# MAGIC %sql
# MAGIC --result selection
# MAGIC CREATE OR REPLACE TEMP VIEW RESULT_SEL AS
# MAGIC
# MAGIC   WITH CTE_TF_SEL AS 
# MAGIC     (
# MAGIC       SELECT * FROM WCM_SEL
# MAGIC       UNION
# MAGIC       SELECT * FROM WNH_SEL
# MAGIC     )
# MAGIC   
# MAGIC   SELECT
# MAGIC     t1.*
# MAGIC     ,t2.*
# MAGIC   FROM CTE_TF_SEL t1
# MAGIC   LEFT JOIN Siebel_SEL t2 on t1.RABO_PARTY_ACCOUNT = t2.ar_ac_iban
# MAGIC   WHERE t2.bnk_code in (3000,3400)
# MAGIC   -- TODO: OR ORG_UNIT IN ('..list of all WR org units.')
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct ALERT_RULE from RESULT_SEL

# COMMAND ----------

# DBTITLE 1,checking out result
# MAGIC %sql
# MAGIC select * from RESULT_SEL
# MAGIC order by TRANSACTION_KEY limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from RESULT_SEL

# COMMAND ----------

# DBTITLE 1,Summarize Result per month
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW RESULT_SUMMARY AS
# MAGIC
# MAGIC   SELECT 
# MAGIC     ALERT_SYSTEM
# MAGIC     , ORG_UNIT
# MAGIC     , DIRECTION 
# MAGIC     , CONCAT(CAST(YEAR(EVENT_DATE) AS STRING),'_', RIGHT(CONCAT('0',CAST(MONTH(EVENT_DATE) AS STRING)),2)) AS YEAR_MONTH
# MAGIC     , count(distinct(TRANSACTION_KEY)) AS NUMBER_OF_TRANSACTIONS
# MAGIC     , count(*) AS NUMBER_OF_ALERTS
# MAGIC   FROM RESULT_SEL 
# MAGIC   GROUP BY ALERT_SYSTEM
# MAGIC     , ORG_UNIT
# MAGIC     , DIRECTION 
# MAGIC     , CONCAT(CAST(YEAR(EVENT_DATE) AS STRING),'_', RIGHT(CONCAT('0',CAST(MONTH(EVENT_DATE) AS STRING)),2))

# COMMAND ----------

# DBTITLE 1,showing results
# MAGIC %sql
# MAGIC select * from RESULT_SUMMARY ORDER BY ALERT_SYSTEM, ORG_UNIT, DIRECTION, YEAR_MONTH

# COMMAND ----------

# MAGIC %md
# MAGIC ## The end
# MAGIC possibly store/create DB if not exists
# MAGIC
# MAGIC **RABO_TA**
# MAGIC
# MAGIC **RABO_NL**

# COMMAND ----------


