# Databricks notebook source
# MAGIC %md
# MAGIC ## W&R Namescreening Alerts from Retail NL Screening
# MAGIC Goal: This notebook analyses the alerts generated through Retail NL daily screening on parties linked to W&R in Siebel.
# MAGIC
# MAGIC ##### Introduction
# MAGIC Hello! Welcome to this analysis of Namescreening alerts. You may wonder: 'why this strange format and not just a normal powerpoint?'. <br><br> What you see in front of you is a 'jupyter notebook'. When you upload this into an 'information factory' that has the right access to the GDP, you can completely re-run this analysis. That makes the analysis completely reproducable, and even right on top of our strategic source of truth for data analysis. The code cells are the only code between the GDP and the output, so we shouldn't need anything else than this notebook for documentation. Nor do I need to download or store datasets elsewhere, which is greatly reducing risk of data leakage. 
# MAGIC <br>
# MAGIC <br>
# MAGIC Next to blocks of code, I can also write 'markdown' descriptions like this piece of text. This is really handy to allow my readers to follow the abracadabra a bit :)
# MAGIC
# MAGIC
# MAGIC ##### <span style="color:blue">Contents of this notebook:</span>
# MAGIC 1.  Load and connect to data (create a more specific md cell at the start of that step)
# MAGIC     - Siebel files
# MAGIC     - W&R (GCOB/GCDS)
# MAGIC 2.  Transform data (create a more specific md cell at that stage)
# MAGIC     - Connect WR Siebel parties to WR siebel clients. 
# MAGIC     - See which clients are the ones with Parties with Alerts in their relations. 
# MAGIC     - See if the client is still relevant in WR scope.
# MAGIC     - Match Siebel Client with WR client to see which have been already processed in that screening.
# MAGIC 3. Show final buckets with reasons for mass closure or further investigation
# MAGIC
# MAGIC Kind regards,
# MAGIC Ruud
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### <font color = Green>Conclusion</font>
# MAGIC 22600 Alerts are still open at the time of writing, after several earlier batch closures. The aim of the analysis was to map remaining parties to W&R, and show which bucket for mass closing can be applied to the alerts and parties that generate the alert.
# MAGIC - 22600 alerts were generated for 845 unique parties. Mostly Natural person Parties.
# MAGIC
# MAGIC I have managed to find a mapping to existing W&R clients, or show that the party that triggered the alert is not related to a W&R client, for all but 20 entities:
# MAGIC - 11 had a previous connection to a W&R client, that does not currently map to a client in GCOB
# MAGIC - 9 needed further investigation as the link between system administrations of Retail and W&R did not result in a succesful mapping
# MAGIC
# MAGIC <font color = red>**Potential Gap**</font> <br>
# MAGIC For most of the alerts, the argument is that they are already in scope of Screening for W&R because W&R has their own daily screening set up. Upon consultation with the Overnight Riskshield Screening for W&R, that project is fully running in product on the entire client scope that matches siebel - but formally is in a 'Pilot' status. I am not a 100% sure if we can use that argument until that overnight screening is formally in production.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Generic imports
# MAGIC Import Pandas, a Python library for data manipulation and analysis.
# MAGIC
# MAGIC Define today's date (using the library datetime), used in the followings the up to date data from GDP.

# COMMAND ----------

# generic imports
import pandas as pd
from notebookutils import mssparkutils
from datetime import datetime
from datetime import timedelta

currentDate = datetime.today().strftime('%Y%m%d')
Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')

currentYear = datetime.today().strftime('%Y')
currentMonth = datetime.today().strftime('%m')
currentDay = datetime.today().strftime('%d')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Connecting to the GDP
# MAGIC The following lines set the spark configuration to match the linked service 'ls_GDP_defined'. 
# MAGIC This linked service connects to the GDP via managed identity, so it's important to run this notebook as 'managed identity' of synapse. 
# MAGIC Otherwise it'll try to use your credentials as pass-through and you as an individual are not allowed to read on the GDP. 
# MAGIC ** note that that's different in OneLab, where your user-account will get direct read-rights on GDP data, but limited from the OneLab network.

# COMMAND ----------

source_full_storage_account_name = "edlcorestdeuprod0001.dfs.core.windows.net" 
ls_gdp_defined = "ls_GDP_defined"

spark.conf.set("spark.storage.synapse.edlcorestdeuprod0001.dfs.core.windows.net.linkedServiceName", "ls_GDP_defined") # ls_WRFEC_ADLS  ls_storage
#spark.conf.set("spark.storage.synapse.linkedServiceName", "ls_GDP_defined")
sc._jsc.hadoopConfiguration().set(f"fs.azure.account.oauth.provider.type.edlcorestdeuprod0001.dfs.core.windows.net", "com.microsoft.azure.synapse.tokenlibrary.LinkedServiceBasedTokenProvider")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Loading from the GDP
# MAGIC The following specifices a spark.read.load() from a specific file location. Note that wildcards may be used in multiple places to specify patterns of filenames. The returned object is called a 'spark dataframe'.
# MAGIC The line below also calls 'createOrReplaceTempView'. This creates a catalog table that enables you to reference it in sql. Using: sdf = spark.sql(""" select.. """) you can then reference the results back as a dataframe. And make that dataframe into a view again.
# MAGIC
# MAGIC **lazy execution**
# MAGIC Everything in spark is lazily executed. This means that nothing is actually loading, or querying the data yet. Spark just creates an execution plan ('DAG', i.e., Directed acyclic graph) for all the transformations you call upon the data. Only when you do 'display' or 'show' or want to store the data, it'll start executing anything, starting with loading the data.

# COMMAND ----------

df_AR = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_ar_hist/2/data/', format='delta') #.select(
 #   "ar_ac_iban", "cmrcl_pd_tp_code" ,"admn_ggm_code", "bnk_code", "ar_prim_org_rel_id", "edl_valid_to_dts")
#df_AR.columns

df_REL_X_AR = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_ar_hist/3/data/', format='delta')
df_REL_X_REL = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_rel_hist/2/data/', format='delta')
#
df_NP = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_np_hist/2/data/', format='delta')
df_ORG = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_org_hist/2/data/', format='delta')

#no access? It may be required for Cash/MTO agents?
#df_AR_ATTR = spark.read.load('abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_ar_attr_hist/1/data/', format='delta')


# COMMAND ----------

df_REL_X_AR.createOrReplaceTempView('rel_x_ar')
df_REL_X_AR.createOrReplaceTempView('relxar')

df_REL_X_REL.createOrReplaceTempView('rel_x_rel')
df_AR.createOrReplaceTempView('ar')
df_ORG.createOrReplaceTempView('org')
df_NP.createOrReplaceTempView('np')

#df_AR_ATTR.createOrReplaceTempView('AR_ATTR')

# COMMAND ----------

#gcds keystore to match against GCOB-id's - to see if already in scope of review.
gcds_keystorekey = spark.read.parquet('abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/client_KeyStoreKey/4601/data/EDL_LOAD_DTS=20240318*/*.parquet')

df_gcds_siebel = gcds_keystorekey.filter('KeyStore_type == "SBWRR"')
df_gcds_gcob = gcds_keystorekey.filter('KeyStore_type == "GCOBID"')

# to temp views
df_gcds_gcob.createOrReplaceTempView('gcds_gcob')
df_gcds_siebel.createOrReplaceTempView('gcds_siebel')

# To match siebelid with GcobID
df_siebel_gcob = spark.sql(""" select t1.KeyStore_value AS REL_ID, 
    t2.KeyStore_Value AS GcobId
    from gcds_siebel AS t1
    LEFT JOIN gcds_gcob AS t2 on t1.GCID = t2.GCID
    """)

df_siebel_gcob.createOrReplaceTempView('siebel_gcob')

# COMMAND ----------

display(df_gcds_siebel.limit(4))

# COMMAND ----------

display(df_gcds_gcob.limit(4))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Connecting to our landing zone
# MAGIC The file with alerts received from the Sanctions departments has been upoaded here. 
# MAGIC If you are trying to replicate this analysis, you'll have to do the same.

# COMMAND ----------

# historic file with namescreening provided by the riskshield team

# get file from our landing zone https://salandingzonefecradarprd.dfs.core.windows.net/
source_full_storage_account_name = "salandingzonefecradarprd.dfs.core.windows.net" 

spark.conf.set("spark.storage.synapse.salandingzonefecradarprd.dfs.core.windows.net.linkedServiceName", "ls_salandingzonefecradarprd") # 
#spark.conf.set("spark.storage.synapse.linkedServiceName", "ls_GDP_defined")
sc._jsc.hadoopConfiguration().set(f"fs.azure.account.oauth.provider.type.salandingzonefecradarprd.dfs.core.windows.net", "com.microsoft.azure.synapse.tokenlibrary.LinkedServiceBasedTokenProvider")

# COMMAND ----------

from notebookutils import mssparkutils

# COMMAND ----------

#check file path and content
mssparkutils.fs.ls("abfss://adhoc-uploads@salandingzonefecradarprd.dfs.core.windows.net/namescreening/Upload_RiskshieldSiebelAlerts.csv")

#expecting Upload_RiskshieldSiebelAlerts.csv 

# COMMAND ----------

sdf_alerts = spark.read.csv("abfss://adhoc-uploads@salandingzonefecradarprd.dfs.core.windows.net/namescreening/Upload_RiskshieldSiebelAlerts.csv", header= True, sep= ";")

# COMMAND ----------

# It should be sufficient to have all alerts with only WR listed. the rest have been deleted/removed. Should be a scope of 22690
display(sdf_alerts.limit(3))

# COMMAND ----------

sdf_alerts.createOrReplaceTempView('alerts')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Matching alerts to Siebel NP or ORG tables

# COMMAND ----------

# MAGIC %%sql
# MAGIC -- select items that currently do not exist
# MAGIC CREATE OR REPLACE TEMPORARY VIEW AlertsToCurrentSiebelSelection AS
# MAGIC select t1.`REL_ID with zeros` 
# MAGIC     , t2.REL_ID AS NPRELID
# MAGIC     , t3.REL_ID AS ORGRELID
# MAGIC     , t1.InStart2022file
# MAGIC     , COALESCE(t2.BNK_CODE, t3.BNK_CODE) AS PARTY_BANK_CODE
# MAGIC     from alerts t1 
# MAGIC     left join (select * from np where edl_valid_to_dts = date('9999-12-31')) AS T2 on t2.REL_ID = t1.`REL_ID with zeros` 
# MAGIC     left join (select * from org where edl_valid_to_dts = date('9999-12-31')) AS t3 on t3.REL_ID = t1.`REL_ID with zeros` 
# MAGIC     WHERE t1.IsWR = 'WR'

# COMMAND ----------

# MAGIC %%sql 
# MAGIC select * from AlertsToCurrentSiebelSelection limit 100

# COMMAND ----------

# MAGIC %%sql
# MAGIC select * from AlertsToCurrentSiebelSelection where NPRELID IS NULL AND ORGRELID IS NULL

# COMMAND ----------

# showing if there is any 'REL_ID' from the alert list that has no match to anything currently in the Active Siebel scope.
#display(spark.sql("""select * from gap where NPRELID IS NULL OR ORGRELID IS NULL"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Storing the data
# MAGIC Just like loading or reading, we can also use spark to store the data as a table. All data stored should be in .parquet in object storage. This allows us to interact with the data as if it's a table in database - without having to pay for a database, or transform data-types into a format a database will understand.

# COMMAND ----------

#Create a spark database to store the data in
#3 https://spark.apache.org/docs/3.0.0/sql-ref-syntax-ddl-create-database.html

spark.sql("""CREATE DATABASE IF NOT EXISTS NSalerts""")
spark.conf.get("spark.sql.warehouse.dir")

# COMMAND ----------

df_gap = spark.sql('select * from AlertsToCurrentSiebelSelection')
df_gap.write.mode('overwrite').saveAsTable('NSalerts.gap')

# COMMAND ----------

# MAGIC %%sql
# MAGIC select * from NSalerts.gap limit 4

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checking if the identified scope (845 parties) is associated with "Moneycard"
# MAGIC By utilizing Spark SQL, which provides a SQL-like syntax while leveraging the computational power of a Spark cluster, we create a new table named “gapwithproducts”. This table is designed to find any eventual link between the 845 parties and the "Moneycard" product and its status.

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gapwithproducts AS
# MAGIC
# MAGIC select t1.`REL_ID with zeros` AS `REL_ID`
# MAGIC , t1.InStart2022file
# MAGIC , t1.PARTY_BANK_CODE
# MAGIC , T2.ADMN_GGM_CODE AS ADMN_GGM_CODE_MONEYCARD
# MAGIC , t3.AR_ST_GGM_CODE_VNUMMER
# MAGIC , t4.AR_ST_GGM_CODE_WERELDPAS
# MAGIC
# MAGIC from NSalerts.gap AS t1
# MAGIC     
# MAGIC LEFT JOIN (select
# MAGIC     t3.REL_ID,
# MAGIC     T4.ADMN_GGM_CODE , -- MoneyCard
# MAGIC     MIN(T4.AR_LCS_TP_GGM_CODE) AS AR_LCS_TP_GGM_CODE, -- lifecycle status
# MAGIC     MIN(T4.AR_ST_GGM_CODE) AS AR_ST_GGM_CODE -- Status of product
# MAGIC     from REL_X_AR as t3
# MAGIC     inner join AR as t4 on t3.AR_NO = t4.AR_NO AND t3.ADMN_GGM_CODE = t4.ADMN_GGM_CODE
# MAGIC     where t4.ADMN_GGM_CODE = 1301
# MAGIC     and t3.edl_valid_to_dts = date('9999-12-31')
# MAGIC     and t4.edl_valid_to_dts = date('9999-12-31')
# MAGIC     GROUP BY t3.REL_ID, T4.ADMN_GGM_CODE
# MAGIC     ) as t2 
# MAGIC on t1.`REL_ID with zeros` = t2.REL_ID
# MAGIC
# MAGIC LEFT JOIN (select 
# MAGIC     t3.REL_ID,
# MAGIC     T4.ADMN_GGM_CODE ,
# MAGIC     MIN(T4.AR_ST_GGM_CODE) AS AR_ST_GGM_CODE_VNUMMER
# MAGIC     from REL_X_AR as t3
# MAGIC     inner join AR as t4 on t3.AR_NO = t4.AR_NO AND t3.ADMN_GGM_CODE = t4.ADMN_GGM_CODE
# MAGIC     where t4.ADMN_GGM_CODE = 1330
# MAGIC     and t3.edl_valid_to_dts = date('9999-12-31')
# MAGIC     and t4.edl_valid_to_dts = date('9999-12-31')
# MAGIC     GROUP BY t3.REL_ID, T4.`ADMN_GGM_CODE`
# MAGIC     ) AS t3 
# MAGIC on t1.`REL_ID with zeros` = t3.REL_ID
# MAGIC
# MAGIC LEFT JOIN (select 
# MAGIC     t3.REL_ID,
# MAGIC     T4.`ADMN_GGM_CODE` ,
# MAGIC     MIN(T4.`AR_ST_GGM_CODE`) AS `AR_ST_GGM_CODE_WERELDPAS` -- Status of product
# MAGIC     from REL_X_AR as t3
# MAGIC     inner join AR as t4 on t3.AR_NO = t4.AR_NO AND t3.ADMN_GGM_CODE = t4.ADMN_GGM_CODE
# MAGIC     where t4.`ADMN_GGM_CODE` = '01'
# MAGIC     and t3.edl_valid_to_dts = date('9999-12-31')
# MAGIC     and t4.edl_valid_to_dts = date('9999-12-31')
# MAGIC     GROUP BY t3.REL_ID, T4.`ADMN_GGM_CODE`
# MAGIC     ) AS t4 
# MAGIC on t1.`REL_ID with zeros` = t4.REL_ID
# MAGIC

# COMMAND ----------

df_gapwithproducts = spark.sql("SELECT * FROM gapwithproducts")
df_gapwithproducts.write.mode('overwrite').saveAsTable('NSalerts.gapwithproducts')

# COMMAND ----------

# MAGIC %%sql
# MAGIC select count(distinct REL_ID) AS `UniquePartiesToResearch` from gapwithproducts

# COMMAND ----------

# MAGIC %md
# MAGIC No party is associated with Moneycard.

# COMMAND ----------

# MAGIC %%sql
# MAGIC select ADMN_GGM_CODE_MONEYCARD AS `W&R MONEYCARD`
# MAGIC , count(distinct(REL_ID)) AS `COUNT OF PERSONS` 
# MAGIC from NSalerts.gapwithproducts 
# MAGIC GROUP BY ADMN_GGM_CODE_MONEYCARD

# COMMAND ----------

# MAGIC %%sql
# MAGIC select count(*) from NSalerts.gapwithproducts 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking for NP connection to products as Volmacht; through main product owner is WR Client
# MAGIC The product code 1006 is volmacht. We will see the primairy organisation linked to the 'volmacht' product
# MAGIC

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW np_to_main_client AS 
# MAGIC select distinct 
# MAGIC t1.REL_ID
# MAGIC ,   t1.InStart2022file
# MAGIC , t1.PARTY_BANK_CODE
# MAGIC ,    t1.ADMN_GGM_CODE_MONEYCARD
# MAGIC ,    t1.AR_ST_GGM_CODE_VNUMMER
# MAGIC ,    t1.AR_ST_GGM_CODE_WERELDPAS
# MAGIC , t2.AR_NO
# MAGIC
# MAGIC , t3.ar_prim_org_rel_id
# MAGIC , CASE WHEN t3.ar_prim_org_rel_id IN ( '000000116434369',  '000000117165225' , '000000115897236' ,'116434369',  '117165225' , '115897236') THEN 'MTO' 
# MAGIC     ELSE ''
# MAGIC     END AS `Prim_ORG_Special_Client`
# MAGIC , t3.root_ar_no
# MAGIC , t3.admn_ggm_code
# MAGIC
# MAGIC , t4.org_cmrcl_nm
# MAGIC , t4.rel_st_tp_ggm_dsc
# MAGIC , t4.bnk_code
# MAGIC
# MAGIC , t5.GcobID
# MAGIC
# MAGIC from NSalerts.gapwithproducts AS t1
# MAGIC LEFT JOIN (select * from RELXAR where edl_valid_to_dts = date('9999-12-31') )AS t2 on t1.REL_ID = t2.REL_ID 
# MAGIC LEFT JOIN (select * from AR where ADMN_GGM_CODE = 1006 and tech_to_dts = date('9999-12-31')) AS T3 on t2.AR_NO = T3.AR_NO AND t2.ADMN_GGM_CODE = t3.ADMN_GGM_CODE
# MAGIC LEFT JOIN (select * from org where edl_valid_to_dts = date('9999-12-31')) AS t4 on t3.ar_prim_org_rel_id = t4.REL_ID
# MAGIC LEFT JOIN siebel_gcob t5 on t4.REL_ID = t5.REL_ID
# MAGIC
# MAGIC
# MAGIC -- R
# MAGIC --select * from np_to_main_client limit 10

# COMMAND ----------

spark.sql('select * from np_to_main_client').write.mode('overwrite').saveAsTable('NSalerts.np_to_main_client')

# COMMAND ----------

# MAGIC %%sql
# MAGIC select count(distinct(REL_ID)) AS `DistinctNP` from NSalerts.np_to_main_client

# COMMAND ----------

# MAGIC %%sql
# MAGIC select * from NSalerts.np_to_main_client limit 3

# COMMAND ----------

# MAGIC %%sql
# MAGIC /*
# MAGIC select bnk_code
# MAGIC     ,count(distinct(REL_ID)) AS `distinctPersons` 
# MAGIC     from NSalerts.np_to_main_client 
# MAGIC     where GCOBId is null
# MAGIC     group by bnk_code
# MAGIC     order by count(distinct(REL_ID)) desc
# MAGIC */
# MAGIC select 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### <font color = Orange>Checking for Rel_x_Rel relations</font>

# COMMAND ----------

# MAGIC %%sql
# MAGIC select * from rel_x_rel limit 4

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW np_as_parent_of AS 
# MAGIC
# MAGIC select t1.*
# MAGIC ,t2.rel_x_rel_rlshp_tp_ggm_code
# MAGIC ,t3.org_cmrcl_nm AS `Structure_child_Name`
# MAGIC ,t3.rel_st_tp_ggm_dsc AS `Structure_child_lcStatus` 
# MAGIC , t3.bnk_code AS `Structure_child_bnk_code` 
# MAGIC , t5.REL_ID AS `Structure_child_RELID`
# MAGIC , t5.GcobId AS `Structure_child_GCOBID`
# MAGIC from NSalerts.np_to_main_client t1
# MAGIC left join (select * from REL_X_REL where edl_valid_to_dts = date('9999-12-31')) AS t2 on t1.REL_ID = t2.rlshp_to_rel_id
# MAGIC left join (select * from ORG where edl_valid_to_dts = date('9999-12-31')) AS t3 on t3.REL_ID = t2.rlshp_fm_rel_id
# MAGIC LEFT JOIN siebel_gcob t5 on t3.REL_ID = t5.REL_ID

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL for Save As Table
# MAGIC Could the below aslo be:
# MAGIC - REPLACE TABLE <db>.<your-table> USING PARQUET AS SELECT ... -- Managed table
# MAGIC - REPLACE TABLE <your-table> USING DELTA PARTITIONED BY (<your-partition-columns>) LOCATION "<your-table-path>" AS SELECT ... -- External table
# MAGIC
# MAGIC [https://docs.delta.io/latest/best-practices.html#language-sql]

# COMMAND ----------

spark.sql('select * from np_as_parent_of').write.mode('overwrite').saveAsTable('NSalerts.np_as_parent_of')

# COMMAND ----------

# MAGIC %%sql
# MAGIC select count(distinct(REL_ID)) from NSalerts.np_as_parent_of

# COMMAND ----------

# MAGIC %%sql
# MAGIC select * from NSalerts.np_as_parent_of limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC #### <font color = orange> Applying mass-closing flags for alerts </font>
# MAGIC In the code above, we have connected the parties with alerts to:
# MAGIC - W&R clients because the party may have a volmacht/Power of attorney on behalf of W&R
# MAGIC - The bank code of the party/client that our alert-generating party is related to
# MAGIC - W&R clients through a relation of ownership/influence
# MAGIC - Matched against a previous file where we concluded we could do a mass closure early 2022
# MAGIC - Matched if the alert is generated on a non-client
# MAGIC - Matched if the alert-generating party is even connected to any party of W&R in Siebel
# MAGIC
# MAGIC Then we Summarize all potential flags to create a unique list per entity, and we can conclude which 'flag' applies to who. <br>
# MAGIC Finally, we check in order of a number of buckets, in which of these buckets the entity should be assigned through the mass-closing flags that may appear or not on the entity.

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW total_view_flags AS 
# MAGIC
# MAGIC SELECT REL_ID, PARTY_BANK_CODE, GcobId
# MAGIC , IF(GcobID IS NOT NULL, 1,0) AS VolmachtOnGcob_Flag
# MAGIC , IF(((ADMN_GGM_CODE_MONEYCARD is not null ) and (AR_ST_GGM_CODE_VNUMMER is not null)),1,0) AS COA_Flag
# MAGIC , IF(Structure_child_GCOBID is not null,1,0) StructureToGcob_Flag
# MAGIC , IF(AND(bnk_code is not null, AND(bnk_code <> 3000, bnk_code <> 3400)),1,0) AS ParentOfNonWR_Flag
# MAGIC , IF(OR(bnk_code == 3000,bnk_code == 3400),1,0) AS ParentOfWR_Flag
# MAGIC , IF(LEFT(InStart2022file,3) == '000' ,1,0) AS InClosurefile2022_Flag
# MAGIC , IF(OR(PARTY_BANK_CODE == 3000,PARTY_BANK_CODE == 3400),1,0) AS Party30003400_Flag
# MAGIC , IF(Structure_child_bnk_code in (3000,3400) and Structure_child_lcStatus = 'Ex-klant', 1,0) AS ParentOfWRExclient_Flag
# MAGIC , IF(Structure_child_RELID IS NOT NULL and Structure_child_GCOBID IS NULL and Structure_child_lcStatus = 'Klant',1,0) AS ParentOfSiebelClientNotInGcob_Flag
# MAGIC , IF(Structure_child_RELID IS NOT NULL OR ar_prim_org_rel_id IS NOT NULL,1,0) AS AnyRelationToAnything_Flag
# MAGIC
# MAGIC from Nsalerts.np_as_parent_of
# MAGIC             -- has products for coa then coa
# MAGIC             -- has relation to WR Then check already match to GCOB
# MAGIC             -- has relaton to WR to product then WR
# MAGIC             -- 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %%sql
# MAGIC select * from total_view_flags limit 3

# COMMAND ----------

# MAGIC %%sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW total_view_flags_agg AS 
# MAGIC select REL_ID, PARTY_BANK_CODE
# MAGIC     , SUM(InClosurefile2022_Flag) AS InClosurefile2022
# MAGIC     , SUM(VolmachtOnGcob_Flag) AS VolmachtOnGcob
# MAGIC     , SUM(COA_Flag) AS COA
# MAGIC     , SUM(StructureToGcob_Flag) AS StructureToGcob
# MAGIC     , SUM(ParentOfNonWR_Flag) AS ParentOfNonWR
# MAGIC     , SUM(Party30003400_Flag) AS Party30003400
# MAGIC     , SUM(ParentOfWRExclient_Flag) AS ParentOfWRExclient
# MAGIC     , SUM(ParentOfSiebelClientNotInGcob_Flag) AS ParentOfSiebelClientNotInGcob
# MAGIC     , SUM(AnyRelationToAnything_Flag) AS AnyRelationToAnything
# MAGIC from total_view_flags
# MAGIC GROUP BY REL_ID, PARTY_BANK_CODE

# COMMAND ----------

# MAGIC %%sql
# MAGIC select * from total_view_flags_agg

# COMMAND ----------

# MAGIC %%sql
# MAGIC select * from total_view_flags_agg where Party_bank_code not in (3000,3400)

# COMMAND ----------

#%%sql
df_sum = spark.sql("""select REL_ID
, CASE WHEN VolmachtOnGcob > 0 THEN 'Link to GCOB through Product' 
    WHEN StructureToGcob > 0 THEN 'Link to GCOB through structure'
    WHEN ParentOfNonWR > 0  THEN 'parent of non-WR party'
    WHEN Party30003400 = 0 THEN 'Not W&R party'
    WHEN ParentOfWRExclient > 0 THEN 'Parent of WR Ex-client'
    WHEN ParentOfNonWR > 0 THEN 'Parent of Retail party'
    WHEN ParentOfSiebelClientNotInGcob > 0 THEN 'Parent of Siebel Client not matching GCOB'
    WHEN AnyRelationToAnything = 0 THEN 'no relation to WR'
    WHEN InClosurefile2022 > 0 THEN 'InClose2022'
    ELSE 'FurtherInvestigation' 
END AS `Bucket`

from total_view_flags_agg""")

# COMMAND ----------

df_sum.createOrReplaceTempView('summary')
df = spark.sql(""" select t1.`Bucket`
, count(distinct(t1.REL_ID)) AS Parties
, count(distinct(t2.ID_KEY)) AS AlertsOnParties
from summary t1
left join alerts t2 on t1.REL_ID = t2.`REL_ID with zeros`
GROUP BY t1.`Bucket`
order by count(distinct(t1.REL_ID)) DESC""").toPandas()

# COMMAND ----------

# MAGIC %%sql
# MAGIC select t1.`Bucket`
# MAGIC , count(distinct(t1.REL_ID)) AS Parties
# MAGIC , count(distinct(t2.ID_KEY)) AS AlertsOnParties
# MAGIC from summary t1
# MAGIC left join alerts t2 on t1.REL_ID = t2.`REL_ID with zeros`
# MAGIC GROUP BY t1.`Bucket`
# MAGIC order by count(distinct(t1.REL_ID)) DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### <font color = orange>  Showing conclusions  </font>
# MAGIC Here, all alerts have been assigned to a bucket and we are showing with a simple graph how may fit into which bucket. The numbers are in the table above.
# MAGIC Only 20 parties remain to be investigated manually before all alerts can be closed.

# COMMAND ----------

# Plotting buckets of clients
df[['Bucket', 'Parties']].plot(kind = 'barh', x = 'Bucket', y = 'Parties')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exporting for mass closure

# COMMAND ----------

spark.sql("""select t1.`Bucket`
, t1.REL_ID
, t2.ID_KEY
, t2.ID_DATE
from summary t1
left join alerts t2 on t1.REL_ID = t2.`REL_ID with zeros`""").write.mode('overwrite').saveAsTable('NSalerts.TotalsForMassClosure')

# COMMAND ----------

print(df[df.Bucket == 'FurtherInvestigation'].shape)
display(df[df.Bucket == 'FurtherInvestigation'])

# 000000115310477 has only link with ex-volmacht on ex-client
# 000000115307412 Has only link with de-activated volmacht
# 000000115309618 has link with volmacht (inactive?) on Party - not Klant
# 000000118730809 has only link with ex-volmacht on ex-client
# 000000116722994 has only link with ex-volmacht on ex-client

# COMMAND ----------

# MAGIC %%sql
# MAGIC select * from NSalerts.np_as_parent_of where REL_ID = 000000116722994

# COMMAND ----------

# MAGIC %%sql 
# MAGIC select * from total_view_flags_agg where REL_ID = 000000115030933

# COMMAND ----------

# MAGIC %%sql
# MAGIC select * from alerts  limit 3

# COMMAND ----------

# MAGIC %%sql
# MAGIC select count(distinct (ID_KEY)) from alerts  where IsWR = 'WR'
