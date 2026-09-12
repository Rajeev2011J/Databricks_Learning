# Databricks notebook source
from pandas.tseries.offsets import MonthEnd
import os
from datetime import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pandas as pd

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

currentDate = datetime.today().strftime('%Y%m%d')
Yesterdate = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
BusinessDate = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
load_dts = 'LOADED_DTS=' + Yesterdate + 'T000000Z'
print (load_dts)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = ['CaseService_case_LegalEntityClient',
'CaseService_case_Case',
'CaseService_case_ProductProvidedToLegalEntity',
'CaseService_case_ProductReference',
'CaseService_case_LegalEntityClientExpectedTransactionProfileInstance',
'CaseService_dbo_RabobankEntity',
'CaseService_ExpectedTransactionProfile_Answer',
'CaseService_ExpectedTransactionProfile_Instance',
'CaseService_ExpectedTransactionProfile_Model',
'CaseService_ExpectedTransactionProfile_ModelQuestionGroup',
'CaseService_ExpectedTransactionProfile_PossibleAnswer',
'CaseService_ExpectedTransactionProfile_ProductGroupModel',
'CaseService_ExpectedTransactionProfile_Question',
'CaseService_ExpectedTransactionProfile_QuestionGroup',
'CaseService_ExpectedTransactionProfile_QuestionPossibleAnswer',
'CaseService_ExpectedTransactionProfile_QuestionQuestionGroup',
'CaseService_ExpectedTransactionProfile_QuestionTrigger',
'CaseService_ExpectedTransactionProfile_QuestionType']


for df in load_df:
    spark.read.load('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/'+df+'/100/data/' + load_dts + '/*.parquet', format='parquet').createOrReplaceTempView(df)

# COMMAND ----------

#Client LifeCycle Mapping with Id

ClientLifecycleStatus_dict={'ClientLifecycleStatusTypeId':[i for i in range(5)],
'ClientLifecycleStatus':['Prospect', 'Client', 'FormerProspect', 'ExitClient', 'FormerClient']}
# create pyspark dataframe from dictionary
sdf_gcob_static_ClientLifecycleStatus=spark.createDataFrame(pd.DataFrame(ClientLifecycleStatus_dict))
#display(sdf_gcob_static_ClientLifecycleStatus)
sdf_gcob_static_ClientLifecycleStatus.createOrReplaceTempView('ClientLifecycleStatus')

#Case review type Mapping with Id


CaseReviewType_dict={'CaseReviewTypeId':[i for i in range(10)],
'CaseReviewType':['Unknown', 'Initial On-Boarding', 'Amendment', 'Periodic Review', 'Event Driven Review', 
'Client Offboarding', 'Change of Client Owner', 'Product Offboarding', 
'Product Offboarding (Resume)', 'Tailored Event Assessment']}
sdf_gcob_static_CaseReviewType=spark.createDataFrame(pd.DataFrame(CaseReviewType_dict))
#display(sdf_gcob_static_CaseReviewType)
sdf_gcob_static_CaseReviewType.createOrReplaceTempView('CaseReviewType')

#Case status type Mapping with Id

CaseStatusType_dict={'CaseStatusTypeId':[i for i in range(24)],'CaseStatusType':['Unknown'
,'Initiation In Progress'
, 'Ready for KYC assessment'
, 'KYC assessment in progress'
, 'Ready for 4 eye check'
, '4 eye check in progress'
, 'Client owner sign off requested'
, 'Client committee sign off requested'
, 'Product fulfilment in progress'
, 'Completed'
, 'Cancelled'
, 'Migrated'
, 'Ready for identification'
, 'Identification in progress'
, 'Ready for screening'
, 'Screening in progress'
, 'GCOB Review in progress'
, 'Client Owner approval requested'
, 'Local client owner sign off requested'
, 'Ready for Product Offboarding confirmation'
, 'Product Offboarding confirmation in progress'
, 'Product Offboarding in progress'
, 'Senior management sign off requested','External update in progress']}

sdf_gcob_static_CaseStatusType=spark.createDataFrame(pd.DataFrame(CaseStatusType_dict))
#display(sdf_gcob_static_CaseStatusType)
sdf_gcob_static_CaseStatusType.createOrReplaceTempView('CaseStatusType')



# COMMAND ----------

sdf_LE_ETPInstanceAnswers=spark.sql("""select distinct
i.id as InstanceId,
i.ModelId,
q.id as QuestionId,
q.Text as QuestionName,
pa.id as PossibleAnswerId,
pa.Value, 
a.FreeFormAnswerValue
from CaseService_ExpectedTransactionProfile_Instance i 
inner join CaseService_ExpectedTransactionProfile_Answer a on i.Id = a.InstanceId 
left join CaseService_ExpectedTransactionProfile_PossibleAnswer pa on a.PossibleAnswerId = pa.Id
		and pa.ExpiredDate is null 
inner join CaseService_ExpectedTransactionProfile_Question q on a.QuestionId = q.Id  
left join CaseService_ExpectedTransactionProfile_Model model on model.Id=i.ModelId""")

sdf_LE_ETPInstanceAnswers.createOrReplaceTempView('LE_ETPInstanceAnswers')
display(sdf_LE_ETPInstanceAnswers)

# COMMAND ----------

sdf_LE_ETP=spark.sql("""select lec.Id as LegalEntityClientId,
lec.GcobId,
ca.CaseId,
lec.FullLegalName ,
cls.ClientLifecycleStatus as ClientLifecycle,
cst.CaseStatusType as CaseStatusName,
crt.CaseReviewType,
lec.NextReviewDateString as NextReviewDate,
pr.ProductName,
re.Name as ProductLocation,
lecetp.ExpectedTransactionProfileInstanceId
from CaseService_case_case ca  
inner join CaseService_case_LegalEntityClient lec on ca.LegalEntityClientId=lec.Id
left join CaseService_case_ProductProvidedToLegalEntity pp ON pp.LegalEntityId = lec.Id
left join CaseService_case_ProductReference pr on pr.ProductId = pp.ProductReferenceId
left join CaseService_case_LegalEntityClientExpectedTransactionProfileInstance lecetp on lec.Id=lecetp.LegalEntityClientId
left join CaseService_ExpectedTransactionProfile_Instance i on i.Id=lecetp.ExpectedTransactionProfileInstanceId and 
(
    (i.IsBusinessUnitBased = 1 and pr.BusinessUnit = i.ProductGroupName) 
    or (i.IsBusinessUnitBased = 0 and i.ProductLocationId is null and i.ProductGroupName=pr.EtpProductGroup)
    or (i.IsBusinessUnitBased = 0 and i.ProductLocationId is not null and i.ProductGroupName = pr.EtpProductGroup 
    and i.ProductLocationId = pp.RabobankEntityReferenceIdOfProductLocation)
)
left join CaseService_dbo_RabobankEntity re on i.ProductLocationId = re.Id
left join ClientLifecycleStatus cls on cls.ClientLifecycleStatusTypeId=lec.ClientLifecycleStatusType
left join CaseStatusType cst on cst.CaseStatusTypeId= ca.CurrentStatus
left join CaseReviewType crt on crt.CaseReviewTypeId= ca.CaseReviewType
""")
sdf_LE_ETP.createOrReplaceTempView('LE_ETP')
 
display(sdf_LE_ETP)
 

# COMMAND ----------

sdf_LE_ETPQG=spark.sql("""select leetp.*,
etpans.Questionid,
etpans.QuestionName,
etpans.Value
from LE_ETP leetp
inner join LE_ETPInstanceAnswers etpans on etpans.InstanceId=leetp.ExpectedTransactionProfileInstanceId""")
display(sdf_LE_ETPQG)



# COMMAND ----------

sdf_LE_ETPQG=sdf_LE_ETPQG.select(F.lit(BusinessDate).alias("BusinessDate"), "*")
# Rearrange columns to insert the new column at the desired position
position=8
new_column = sdf_LE_ETPQG.columns[:position] + ["SourceSystem"] + sdf_LE_ETPQG.columns[position:]

# Create a new DataFrame with the rearranged columns and add the new column
sdf_LE_ETPQG= sdf_LE_ETPQG.select(*sdf_LE_ETPQG.columns).withColumn("SourceSystem", F.lit("GCOB")).select(*new_column)
#display(sdf_LE_ETPQG)
sdf_LE_ETPQG.createOrReplaceTempView('LE_ETPQG')


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS WR_RADAR" )
spark.sql("DROP TABLE IF EXISTS dbo.le_etp" )
sdf_LE_ETPQG.write.mode("overwrite").saveAsTable("WR_RADAR.le_etp")

# COMMAND ----------


