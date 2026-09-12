# Databricks notebook source
# MAGIC %md
# MAGIC ## AML WLM Monitoring
# MAGIC Goal: To find the count of incoming and outgoing transactions per month for non-NL Banks
# MAGIC  
# MAGIC ###### <span style="color:orange">Authors:</span>
# MAGIC 1.  Mahalakshmi.Vengateswaran@rabobank.com
# MAGIC  
# MAGIC ###### <span style="color:orange">Business value:</span>
# MAGIC  
# MAGIC      Track number of incoming and outgoing transactions 
# MAGIC  
# MAGIC  
# MAGIC ###### <span style="color:blue">Flow of the logic:</span>
# MAGIC 1. Read data from GDP Defined layer
# MAGIC 2. Apply filters which are applicable and find the count of incoming and outgoing transactions
# MAGIC 4. Load the data to catalog
# MAGIC

# COMMAND ----------

#Importing required packages
import os
from pyspark.sql.functions import when,col,count,last_day,expr,date_format,add_months,current_date,lit
from pyspark.sql import Row
from datetime import datetime
from dateutil.relativedelta import relativedelta

# COMMAND ----------

#Fetching environment variables
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

# COMMAND ----------

#Retrieving Client Secret from connected Secrets
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
# Configuring Spark to access GDP Defined Storage account using OAuth authentication
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

#Taking current date and creating a dataframe
df_date = spark.range(1).select(current_date().alias("current_date"))
# Calculating the last day of the previous month
df_date = df_date.select(last_day(add_months(df_date["current_date"], -1)).alias("last_day_previous_month"))
# Extracting the day and year parts
df_date = df_date.withColumn("month", date_format(df_date["last_day_previous_month"], "MMMM")).withColumn("month_num", date_format(df_date["last_day_previous_month"], "MM")).withColumn("year", date_format(df_date["last_day_previous_month"], "yyyy"))
#Fetching previous month and year
Last_Month=df_date.collect()[0].__getitem__('month')
Last_Year=df_date.collect()[0].__getitem__('year')
current_month=datetime.now()
two_months_ago_str=(current_month-relativedelta(months=2)).strftime("%Y-%m")
previous_month_str=(current_month-relativedelta(months=1)).strftime("%Y-%m")
current_month_str=datetime.now().strftime("%Y-%m")
months=[previous_month_str,two_months_ago_str,current_month_str]


# COMMAND ----------

#Reading data from GDP Defined Layer
df_Sepa_Transactions = None
for month in months:
    Load_Date=month+"-"
    spark.read.parquet('abfss://opf-pex@edlcorestdeuprod0001.dfs.core.windows.net/PAYMENTTRANSACTION/0/data/loaddate='+Load_Date+'*/*.parquet').createOrReplaceTempView("SEPA_Transactions")
    if df_Sepa_Transactions is None:
        df_Sepa_Transactions=spark.sql("select PAYMENTTRANSACTIONKEY,CREDITPARTYAGENTIDIDX,DEBITPARTYAGENTIDIDX,Bankkey,OUTBOUNDIPX,TRN_RTRN_TRNSTS,RTRANSACTIONFLAGIDX,RTRANSACTIONLEVELIDX,RTRANSACTIONPOSTSETLMNTFLAGIDX,RTRANSACTIONREFUNDFLAGIDX,RTRANSACTIONQUALIFICATIONIDX,PROCESSINGSCHEMEIDX,CLASSIFICATIONIDX,CHANNELSTATUSIDX,SETTLEMENTDAYIDX,Status from SEPA_Transactions  ")
    else:
        df_Sepa_Transactions=df_Sepa_Transactions.union(spark.sql("select PAYMENTTRANSACTIONKEY,CREDITPARTYAGENTIDIDX,DEBITPARTYAGENTIDIDX,Bankkey,OUTBOUNDIPX,TRN_RTRN_TRNSTS,RTRANSACTIONFLAGIDX,RTRANSACTIONLEVELIDX,RTRANSACTIONPOSTSETLMNTFLAGIDX,RTRANSACTIONREFUNDFLAGIDX,RTRANSACTIONQUALIFICATIONIDX,PROCESSINGSCHEMEIDX,CLASSIFICATIONIDX,CHANNELSTATUSIDX,SETTLEMENTDAYIDX,status from SEPA_Transactions "))
    df_Sepa_Transactions=df_Sepa_Transactions.filter(df_Sepa_Transactions.SETTLEMENTDAYIDX.like(f"{previous_month_str}%")).distinct()


# COMMAND ----------

df_Sepa_Transactions=df_Sepa_Transactions.fillna({'Status': 'Null'})
df_Sepa_Transactions=df_Sepa_Transactions.fillna({'CHANNELSTATUSIDX': 'Null'})

# COMMAND ----------


#Applying filter conditions for deriving Incoming transactions
df_Sepa_Transactions1=df_Sepa_Transactions.filter("CREDITPARTYAGENTIDIDX in ('RABOBE23','RABOGB2L','RABODEFF')").filter(col('Bankkey') != 1600).filter("DEBITPARTYAGENTIDIDX not in ('RABOBE23','RABOGB2L','RABODEFF') ").filter("PROCESSINGSCHEMEIDX == 'SCT'").filter(col('CLASSIFICATIONIDX')=='Reject').filter(col('Status')!='Cancelled')

# Calculating total incoming transactions
df_Sepa_Transactions1=df_Sepa_Transactions1.groupby("CREDITPARTYAGENTIDIDX").agg(count("*").alias("Incoming_Transactions")).withColumn('Branch',  when(col('CREDITPARTYAGENTIDIDX')=='RABOBE23','Antwerp').when(col('CREDITPARTYAGENTIDIDX')=='RABODEFF','Frankfurt').when(col('CREDITPARTYAGENTIDIDX')=='RABOGB2L','London'))

#Handling output when count is 0
bic={'RABOBE23':'Antwerp','RABODEFF':'Frankfurt','RABOGB2L':'London'}
Existing_bic=set(df_Sepa_Transactions1.select('CREDITPARTYAGENTIDIDX').distinct().rdd.flatMap(lambda x: x).collect())
Missing_bic={bic:branch for bic,branch in bic.items() if bic not in Existing_bic}
add_bic=[Row(**{'CREDITPARTYAGENTIDIDX':bic,'Incoming_Transactions':0,'Branch':branch})for bic,branch in Missing_bic.items()]
if len(add_bic)>0:
    df_Sepa_Transactions1=df_Sepa_Transactions1.union(spark.createDataFrame(add_bic))


# COMMAND ----------

#Applying filter conditions for deriving Outgoing transactions
df_Sepa_Transactions2=df_Sepa_Transactions.filter("DEBITPARTYAGENTIDIDX in ('RABOBE23','RABOGB2L','RABODEFF')").filter(col('Bankkey') != 1600).filter("CREDITPARTYAGENTIDIDX not in ('RABOBE23','RABOGB2L','RABODEFF') ").filter("PROCESSINGSCHEMEIDX == 'SCT'").filter(col('Status')!='Cancelled').filter(col('Status')!='Rejected')

# Calculating total outgoing transactions
df_Sepa_Transactions2=df_Sepa_Transactions2.groupby("DEBITPARTYAGENTIDIDX").agg(count("*").alias("Outgoing_Transactions")).withColumn('Branch',  when(col('DEBITPARTYAGENTIDIDX')=='RABOBE23','Antwerp').when(col('DEBITPARTYAGENTIDIDX')=='RABODEFF','Frankfurt').when(col('DEBITPARTYAGENTIDIDX')=='RABOGB2L','London'))

#Handling output when count is 0
bic={'RABOBE23':'Antwerp','RABODEFF':'Frankfurt','RABOGB2L':'London'}
Existing_bic=set(df_Sepa_Transactions2.select('DEBITPARTYAGENTIDIDX').distinct().rdd.flatMap(lambda x: x).collect())
Missing_bic={bic:branch for bic,branch in bic.items() if bic not in Existing_bic}
if len(Missing_bic)>0:
    add_bic=[Row(**{'DEBITPARTYAGENTIDIDX':bic,'Outgoing_Transactions':0,'Branch':branch})for bic,branch in Missing_bic.items()]
    df_Sepa_Transactions2=df_Sepa_Transactions2.union(spark.createDataFrame(add_bic))



# COMMAND ----------

#Joining incoming and outgoing transactions
Result_df=df_Sepa_Transactions1.join(df_Sepa_Transactions2,"Branch","inner").select("Branch",col("CREDITPARTYAGENTIDIDX").alias("BIC"),"Incoming_Transactions","Outgoing_Transactions")

#Adding necessary columns and calculating total transactions
Result_df=Result_df.withColumn("Month",lit(Last_Month)).withColumn("Year",lit(Last_Year)).withColumn("Total_Number_of_Transactions", col("Incoming_Transactions") + col("Outgoing_Transactions"))



# COMMAND ----------

#Selecting necessary columns
Result_df = Result_df.select("Branch","BIC","Year","Month","Incoming_Transactions","Outgoing_Transactions","Total_Number_of_Transactions")

# COMMAND ----------

Result_df.createOrReplaceTempView('AML_WLM_Monitoring_SEPA_Transaction_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS radar.AML_WLM_Monitoring_SEPA_Transaction

# COMMAND ----------

spark.sql('select * from AML_WLM_Monitoring_SEPA_Transaction_view').write.mode('overwrite').saveAsTable('radar.AML_WLM_Monitoring_SEPA_Transaction')
