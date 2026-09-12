# Databricks notebook source
import os
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=1
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
NLS_ReadStorage = os.environ['GDP_NA_STORAGE_NAME']
SARADAR = "saradar" + environment

# COMMAND ----------

from RadarUtils import *

# COMMAND ----------

#Derive the date for which data has to be processes
import pandas as pd
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
ThisDay = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' +ThisDay+ '*'
print (load_dts)

# COMMAND ----------

authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(NLS_ReadStorage)
authenticate_storage_account(SARADAR)

# COMMAND ----------

df_Party = spark.read.parquet(f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party/2/data/{load_dts}/*.parquet")
df_Party.createOrReplaceTempView("Party")

df_Party_CDD_CaseQuestionAnswers = spark.read.parquet(f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_CDDCase_QuestionAnswer/2/data/{load_dts}/*.parquet")
df_Party_CDD_CaseQuestionAnswers.createOrReplaceTempView("Party_CDD_CaseQuestionAnswers")

df_Party_coverage = spark.read.parquet(f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_Coverage/1/data/{load_dts}/*.parquet")
df_Party_coverage.createOrReplaceTempView("Party_Coverage")

df_Party_CDDCase = spark.read.parquet(f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_CDDCase/2/data/{load_dts}/*.parquet")
df_Party_CDDCase.createOrReplaceTempView("Party_CDDCase")

df_Party_Systemidentifier = spark.read.parquet(f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_SystemIdentifier/1/data/{load_dts}/*.parquet")
df_Party_Systemidentifier.createOrReplaceTempView("Party_SystemIdentifier")

df_Party_CountryAffiliation = spark.read.parquet(f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/Party_CountryAffiliation/1/data/{load_dts}/*.parquet")
df_Party_CountryAffiliation.createOrReplaceTempView("Party_CountryAffiliation")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Party_CDD_CaseQuestionAnswers where 
# MAGIC questiontext="Is the value of the client assets more than 1 million Euro?" or questiontext="Is the value of the clients assets more than 1.5 million Euro (ROS) or 3 million Euro (Country Banking)? "
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view locations 
# MAGIC as
# MAGIC (select distinct PartyIdentifier, CoverageValue as location
# MAGIC from party_coverage 
# MAGIC where  CoverageTypeDescription in ("Involved Location", "GCO Location")
# MAGIC and  CoverageType='Location')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Party_CountryAffiliation

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view final as
# MAGIC select distinct p.PartyIdentifier,pc.UniquecaseId, ps.application, p.Party_type, location, questiontext, AnswerText, ValidatedRiskLevel, pn.CountryISOcode as nationality
# MAGIC from party p
# MAGIC left outer join locations l on l.PartyIdentifier=p.PartyIdentifier
# MAGIC left outer join Party_CDDCase pc on pc.PartyIdentifier=p.PartyIdentifier 
# MAGIC left outer join Party_CDD_CaseQuestionAnswers qa on  pc.UniquecaseId=qa.UniquecaseId
# MAGIC left outer join Party_SystemIdentifier ps on ps.PartyIdentifier=p.PartyIdentifier
# MAGIC left outer join Party_CountryAffiliation pn on pn.PartyIdentifier=p.PartyIdentifier and NationalAffiliationType='Nationality'
# MAGIC where Party_type like "%Natural%" 
# MAGIC and ps.Application='GIC' 
# MAGIC and Islatestapprovedversionofclient="True"
# MAGIC and pc.caseid=pc.LatestApprovedCaseId
# MAGIC and p.CustomerLifeCycleStatus not in ('Former Client')
# MAGIC and pc.casestatusname<>'Cancelled' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select nationality, count(PartyIdentifier) from final group by nationality

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from final where questiontext='Is the value of the client assets more than 1 million Euro?' or questiontext="Is the value of the clients assets more than 1.5 million Euro (ROS) or 3 million Euro (Country Banking)? "

# COMMAND ----------

df=spark.table('final')

# COMMAND ----------

#NORTHAMERICA
WR001 = ['Rabo Securities Canada, Inc. (RSCI)','Rabo Securities USA, Inc. (RSEC)','Rabobank - USA Rabo AgriFinance','Rabobank Canada (RCBR)','Rabobank Canada(Rural)','Rabobank New York']

# CHILE
WR002 = ['Rabobank Chile']

# BRAZIL
WR003 = ['Rabobank Brazil']

# HONGKONG
WR004 = ['Rabobank Hong Kong']

# SINGAPORE
WR005 = ['Rabobank Singapore']

# CHINA
WR006 = ['Rabobank China']

# INDIA
INDIA = ['Rabobank India']

# EUROPEAFRICA
EUROPEAFRICA = ['Rabobank Antwerp','Rabobank Dublin','Rabobank Frankfurt','Rabobank London','Rabobank Argentina','Rabobank Madrid','Rabobank Milan','Rabobank Netherlands','Rabobank Kenya','Rabobank Paris','Rabobank Foundation','Rabobank - Smallholder Agroforestry Finance (SAF)','Rabobank Turkey']

# AUSTRALIANEWZEALAND
WR009 = ['Rabobank Australia','Rabobank New Zealand','Rabobank - RANZ Country Banking and ROS']

# COMMAND ----------


from functools import reduce
from pyspark.sql import functions as F

cols=['location']

groups = {
    "WR001": list(WR001),
    "WR002": list(WR002),
    "WR003": list(WR003),
    "WR004": list(WR004),
    "WR005": list(WR005),
    "WR006": list(WR006),
    "WR009": list(WR009),
    "INDIA": list(INDIA),
    "EUROPEAFRICA": list(EUROPEAFRICA),
}

def build_condition(cols, values):
    # OR across columns: col1 in values OR col2 in values OR ...
    return reduce(lambda a, b: a | b, (F.col(c).isin(values) for c in cols))

results = {}

for name, values in groups.items():
    condition = build_condition(cols, values)
    count_val = df.filter(condition).count()
    results[name] = count_val
    print(f"count_{name} = {count_val}")

for name, values in groups.items():
    condition = build_condition(cols, values)
    count_val = df.filter(col('ValidatedRiskLevel')=="High").filter(condition).count()
    results[name] = count_val
    print(f"highRisk_count_{name} = {count_val}")

for name, values in groups.items():
    condition = build_condition(cols, values)
    count_val = df.filter(col('questiontext')=="Is the value of the client assets more than 1 million Euro?").filter(col('answertext')=='Yes').filter(condition).count()
    results[name] = count_val
    print(f"count_>1million_{name} = {count_val}")

for name, values in groups.items():
    condition = build_condition(cols, values)
    count_val = df.filter(col('questiontext')=="Is the value of the clients assets more than 1.5 million Euro (ROS) or 3 million Euro (Country Banking)? ").filter(col('answertext')=='Yes').filter(condition).count()
    results[name] = count_val
    print(f"count_>1.5m_to_3m_{name} = {count_val}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from final where partyidentifier='GCDS_800610'
# MAGIC -- where questiontext='Is the value of the client assets more than 1 million Euro?' or questiontext="Is the value of the clients assets more than 1.5 million Euro (ROS) or 3 million Euro (Country Banking)? "
# MAGIC -- and AnswerText='Yes'
