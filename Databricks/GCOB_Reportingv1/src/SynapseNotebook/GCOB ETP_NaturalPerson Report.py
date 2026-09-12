# Databricks notebook source
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import os
from datetime import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

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
load_df = ['CaseService_NaturalPerson_NaturalPersonClient',
'CaseService_NaturalPerson_NaturalPersonCase',
'CaseService_NaturalPerson_ProductProvidedToNaturalPerson',
'CaseService_NaturalPerson_ProductReference',
'CaseService_NaturalPerson_NaturalPersonClientExpectedTransactionProfileInstance',
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
'CaseService_ExpectedTransactionProfile_QuestionType',
'CaseService_NaturalPerson_InvolvedStaffMember', 
'CaseService_NaturalPerson_UserReference',
'CaseService_NaturalPerson_BusinessLineReference',
'CaseService_NaturalPerson_NaturalPersonClientIdentifier',
'CaseService_dbo_SystemIdType']


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

sdf_NP_ETP=spark.sql("""with ApprovedClientVersions(GcobId, LatestApprovedVersionNumber) AS (
    SELECT        cl.GcobId, MAX(cl.Version) AS LatestApprovedVersionNumber
    FROM            CaseService_NaturalPerson_NaturalPersonClient AS cl 
    INNER JOIN CaseService_NaturalPerson_NaturalPersonCase AS c ON c.NaturalPersonClientId = cl.Id
    WHERE        (c.CurrentStatus in(8,9))
       GROUP BY cl.GcobId), ClientIdentifiers AS
    (SELECT        ci.NaturalPersonClientId, st.Name, ci.ValueOfIdentifier
      FROM            CaseService_NaturalPerson_NaturalPersonClientIdentifier AS ci INNER JOIN
                                CaseService_dbo_SystemIdType AS st ON st.Id = ci.SystemTypeReferenceId
)select npc.Id as NaturalPersonClientId,
npc.GcobId,
ca.Id as CaseId,
npc.FullNameInLocalLanguage as FullLegalName ,
cls.ClientLifecycleStatus as ClientLifecycle,
cst.CaseStatusType as CaseStatusName,
crt.CaseReviewType,
bl.Name as BusinessLineName,
npc.NextReviewDateAsString as NextReviewDate,
CASE 
        WHEN (v.LatestApprovedVersionNumber = npc.Version AND ca.CurrentStatus = 8) 
		THEN 'True'
		WHEN (v.LatestApprovedVersionNumber = npc.Version AND ca.CurrentStatus = 9)
        THEN 'True' ELSE 'False' END 
AS IsLatestApprovedVersionOfClient,
pr.ProductName,
re.Name as ProductLocation,
npcetp.ExpectedTransactionProfileInstanceId,
q.id as QuestionId,
q.Text as QuestionName,
pa.Value
from CaseService_NaturalPerson_NaturalPersonCase ca  
inner join CaseService_NaturalPerson_NaturalPersonClient npc on ca.NaturalPersonClientId=npc.Id
LEFT JOIN CaseService_NaturalPerson_InvolvedStaffMember AS sm ON sm.Id = npc.GlobalClientOwnerId 
LEFT JOIN CaseService_NaturalPerson_UserReference AS us ON us.UserId = sm.UserReferenceId 
LEFT JOIN CaseService_NaturalPerson_BusinessLineReference AS bl ON bl.BusinessLineId = sm.BusinessLineReferenceId 
LEFT JOIN ApprovedClientVersions v on ca.Id = v.GcobId
left join CaseService_NaturalPerson_ProductProvidedToNaturalPerson pp ON pp.NaturalPersonClientId  = npc.Id
left join CaseService_NaturalPerson_ProductReference pr on pr.ProductId = pp.ProductReferenceId
left join CaseService_NaturalPerson_NaturalPersonClientExpectedTransactionProfileInstance npcetp on npc.Id=npcetp.NaturalPersonClientId 
left join CaseService_ExpectedTransactionProfile_Instance i on i.Id=npcetp.ExpectedTransactionProfileInstanceId and 
(
    (i.IsBusinessUnitBased = 1 and pr.BusinessUnit = i.ProductGroupName) 
    or (i.IsBusinessUnitBased = 0 and i.ProductLocationId is null and i.ProductGroupName=pr.EtpProductGroup)
    or (i.IsBusinessUnitBased = 0 and i.ProductLocationId is not null and i.ProductGroupName = pr.EtpProductGroup 
    and i.ProductLocationId = pp.RabobankEntityReferenceIdOfProductLocation)
)
inner join CaseService_ExpectedTransactionProfile_Answer a on i.Id = a.InstanceId 
inner join CaseService_ExpectedTransactionProfile_PossibleAnswer pa on a.PossibleAnswerId = pa.Id
		and pa.ExpiredDate is null 
inner  join CaseService_ExpectedTransactionProfile_Question q on a.QuestionId = q.Id  
left join CaseService_ExpectedTransactionProfile_Model model on model.Id=i.ModelId
left join CaseService_dbo_RabobankEntity re on i.ProductLocationId = re.Id
left join ClientLifecycleStatus cls on cls.ClientLifecycleStatusTypeId=npc.ClientLifecycleStatusTypeId
left join CaseStatusType cst on cst.CaseStatusTypeId= ca.CurrentStatus
left join CaseReviewType crt on crt.CaseReviewTypeId= ca.CaseReviewType
""")

 
display(sdf_NP_ETP)
 

# COMMAND ----------

sdf_NP_ETP=sdf_NP_ETP.select(F.lit(BusinessDate).alias("BusinessDate"), "*")
# Rearrange columns to insert the new column at the desired position
position=8
new_column = sdf_NP_ETP.columns[:position] + ["SourceSystem"] + sdf_NP_ETP.columns[position:]

# Create a new DataFrame with the rearranged columns and add the new column
sdf_NP_ETP= sdf_NP_ETP.select(*sdf_NP_ETP.columns).withColumn("SourceSystem", F.lit("GCOB")).select(*new_column)
display(sdf_NP_ETP)
sdf_NP_ETP.createOrReplaceTempView('NP_ETP')

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS WR_RADAR" )
spark.sql("DROP TABLE IF EXISTS dbo.npc_etp" )
sdf_NP_ETP.write.mode("overwrite").saveAsTable("WR_RADAR.npc_etp")

# COMMAND ----------


