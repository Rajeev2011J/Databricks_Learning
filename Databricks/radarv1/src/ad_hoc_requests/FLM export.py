# Databricks notebook source
# MAGIC %md
# MAGIC ### Goal
# MAGIC
# MAGIC Export random files for flm selection per part of scope
# MAGIC should come from cases table in last sprint
# MAGIC
# MAGIC
# MAGIC ##### Selections
# MAGIC Sample size: 51 EDR/PR/ONB + 10 TEAs per maand
# MAGIC  
# MAGIC - Corporate: 34 EDR/PR/ONB files en 7 TEAs 
# MAGIC   -  Corp CDD Hub NL (12+3 TEA)
# MAGIC   - KYSC SC - UK (11+ 2 TEA), Corp CDD Hub London
# MAGIC   - Corp Hub UK (11+2 TEA)
# MAGIC - Global FIs: 
# MAGIC   - Global FI CDD Hub: 17 EDR/PR/ONB en 3 TEAs (PSPs EDR/PR/ONB/TEA zijn inbegrepen)
# MAGIC
# MAGIC ##### 
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

## imports
import pandas as pd



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW FLM_LAST_MONTH AS 
# MAGIC
# MAGIC   select FIHubIndicator_Derived
# MAGIC   , GcobId
# MAGIC   , CaseId
# MAGIC   , Assessmentanalyst
# MAGIC   , `4EYEanalyst`
# MAGIC   , CaseReviewType
# MAGIC   , TotalCaseDuration
# MAGIC   , CaseCompletedDate
# MAGIC   , KYCDepartment
# MAGIC   , KYCUserTeam 
# MAGIC   , GlobalClientOwnerLocation
# MAGIC   , GlobalClientOwner
# MAGIC   , CASE
# MAGIC     WHEN KYCDepartment in ('Corp CDD Hub NL') THEN 'NL'
# MAGIC     WHEN KYCDepartment in ('Corp CDD Hub UK', 'Corp CDD Hub London') AND KycUserTeam in ('Corp UK - Team 1' , 'Corp UK - Team 2', 'Corp UK - Team 3') THEN 'Corp hub UK'
# MAGIC     WHEN KYCDepartment in ('Corp CDD Hub UK', 'Corp CDD Hub London') AND KycUserTeam not in ('Corp UK - Team 1' , 'Corp UK - Team 2', 'Corp UK - Team 3') THEN 'UK'
# MAGIC     WHEN (KYCDepartment in ('Global FI CDD Hub') OR KycUserteam = 'Global FI Hub') THEN 'FI'
# MAGIC     END AS FLMSelectionBucket
# MAGIC   from radar.cases 
# MAGIC   where CaseCompletedDate between date('2024-10-31') and date('2024-12-01')
# MAGIC   and KYCDepartment in ('KYSC SC - UK', 'Corp CDD Hub NL', 'Corp CDD Hub London', 'Global FI CDD Hub', 'Corp CDD Hub UK')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct CaseReviewType from FLM_LAST_MONTH

# COMMAND ----------

df_flm = spark.table('FLM_LAST_MONTH')
p_df_flm = df_flm.toPandas()

# COMMAND ----------

def Select_sample_flm(df, flmbucket = 'NL', fullreviewnumber = 10, teanumber = 2):
    """
    This functions expects a dataframe of potential files in the full period, already selected for a specific scope
    It returns for a certain sub-portfolio, a number of randomly selected within the scopes of full review files and tea files
    """


    # potential files in full review selection for scope
    FullReviewSelection = df[(df['FLMSelectionBucket'] == flmbucket) & (df['ReviewTypeName'].apply(lambda x: x in ('Periodic Review', 'Event Driven Review', 'Initial On-Boarding')))]
    FullReviewSelection['FinalScope'] = flmbucket +'_FullReview'

    # random fullreviewnumber from the scope
    # TODO: what if there are less than nr available?
    FullReviewSample = FullReviewSelection.sample(n=fullreviewnumber)

    # potential files in TEA for scope
    TeaSelection = df[(df['FLMSelectionBucket'] == flmbucket) & (df.ReviewTypeName == 'Tailored Event Assessment')]
    TeaSelection['FinalScope'] = flmbucket +'_TEA'

    # random teanumber
    # TODO what if there are less than 'teanumber' available?
    TeaSample = TeaSelection.sample(n = teanumber)

    return pd.concat([FullReviewSample, TeaSample])


# COMMAND ----------

# test 
df = p_df_flm
flmbucket = 'NL'
fullreviewnumber = 10
teanumber = 2

FullReviewSelection = df[(df['FLMSelectionBucket'] == flmbucket) & (df['ReviewTypeName'].apply(lambda x: x in ('Periodic Review'
                                                                                                               , 'Event Driven Review'
                                                                                                               , 'Initial On-Boarding')))]

# COMMAND ----------

# DBTITLE 1,Selections
# NL
df_nl = Select_sample_flm(p_df_flm, flmbucket = 'NL', fullreviewnumber=12, teanumber=3)

# UK
df_uk = Select_sample_flm(p_df_flm, flmbucket = 'UK', fullreviewnumber=11, teanumber=2)

# UK Corp hub
df_uk_corp = Select_sample_flm(p_df_flm, flmbucket = 'Corp hub UK', fullreviewnumber=11, teanumber=2)

# FI hub
df_fi = Select_sample_flm(p_df_flm, flmbucket = 'FI', fullreviewnumber=17, teanumber=3)

# COMMAND ----------

df_tot = pd.concat([df_nl, df_uk, df_uk_corp, df_fi])
# display(df_tot)

# COMMAND ----------

s_df_tot = spark.createDataFrame(df_tot)
s_df_tot.write.mode('overwrite').saveAsTable('radar.FLM_Random_Selection')

# COMMAND ----------

# %sql
# select * from radar.FLM_Random_Selection limit 10

# COMMAND ----------

# %sql
# select distinct 
# KYCDepartment, KYCUserTeam
# from radar.cases 
# where CaseCompletedDate between date('2024-10-31') and date('2024-12-01') order by KYCDepartment

# COMMAND ----------

# %sql
# select * 
# from FLM_LAST_MONTH 
# where KYCDepartment = 'Corp CDD Hub UK'

# COMMAND ----------


