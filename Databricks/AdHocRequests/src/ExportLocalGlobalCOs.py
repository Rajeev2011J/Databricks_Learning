# Databricks notebook source
# MAGIC %md
# MAGIC ## goal
# MAGIC Export Local and Global CO
# MAGIC
# MAGIC Hi Andrew,
# MAGIC
# MAGIC  
# MAGIC
# MAGIC We just had a call about the data example we are looking for. As we discussed we are looking for data example in which we can see how the Client Ownership Framework (COF) in implemented in GCOB.
# MAGIC
# MAGIC We are aware that the COF is not fully reflected in the systems however, we would like to see the current state of these roles, people who fulfil the role and their departments and region in the GCOB for three client groups Mars, Darling and AGCO.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Definition of the roles based on COF for your reference:
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Global Client Owner(Global CO): A natural person who is ultimately accountable (& responsible if no Local CO in place)for a Client Group. All Client Groups must have a Global Client Owner.
# MAGIC  
# MAGIC
# MAGIC Local Client Owner (Local CO): Responsible for a Client Entity within a Client Group. If no Local Client Owner is in place, the Global CO is responsible.
# MAGIC  
# MAGIC
# MAGIC Product Specialist: A Rabobank product specialist who is allowed to offer the relevant product offering to client entities, providing the Client Owner is aware of the product offerings,  with due observance of article 2.3.1.
# MAGIC  
# MAGIC
# MAGIC  
# MAGIC
# MAGIC I understood that these roles might have a different name in the systems compared to the business terms we use in COF. For example, product specialist is not implemented in systems however, Relation Manager are acting as product specialist. Would appreciate if you could also briefly pinpoint the responsibility of those roles as they are in GCOB. Many thanks!
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Kind regards,
# MAGIC
# MAGIC Baharak Bakhtiari
# MAGIC
# MAGIC Business Manager
# MAGIC
# MAGIC W&R Products

# COMMAND ----------

# get GCOBids for Mars, Darling, AGCO. 

# get their GCO

# get LCO's


# COMMAND ----------

import os

# COMMAND ----------

#Fetching environment variables
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

#Retrieving Client Secret from connected Secrets
service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")
# Configuring Spark to access GDP Defined Storage account using OAuth authentication
spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------



# COMMAND ----------

portfolio = spark.table('radar.clients')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.clients where KYCGroup in ('Mars Incorporated', 'Darling Group', 'Darling International Netherlands B.V.', 'AGCO Group', 'AGCO International GmbH')

# COMMAND ----------

party_lco = spark.read.parquet('abfss://gcob@edlcorestdeuprod0001.dfs.core.windows.net/CaseService/party_local_client_Owners/101/data/EDL_LOAD_DTS=20241121/*.parquet')

# COMMAND ----------

party_lco.createOrReplaceTempView('party_lco')

# COMMAND ----------

display(party_lco.limit(3))

# COMMAND ----------



# COMMAND ----------

party_gco = spark.sql('select GcobId, from wr_radar.portfolio')

# COMMAND ----------

party_gco = spark.sql('select .. from wr_radar.portfolio')

# COMMAND ----------

# MAGIC %sql
# MAGIC select t1.Gcobid
# MAGIC , t1.FullLegalName
# MAGIC ,t1.KYCGroup
# MAGIC , 'LCO' AS Clientownertype
# MAGIC , t2.LocalClientOwnerName AS Clientownername 
# MAGIC
# MAGIC from radar.clients  t1
# MAGIC left join party_lco t2 on t1.latestCaseId = t2.CaseId
# MAGIC
# MAGIC
# MAGIC where t1.KYCGroup in ('Mars Incorporated', 'Darling Group', 'Darling International Netherlands B.V.', 'AGCO Group', 'AGCO International GmbH')

# COMMAND ----------

# MAGIC %sql
# MAGIC select t1.Gcobid
# MAGIC , t1.FullLegalName
# MAGIC ,t1.KYCGroup
# MAGIC , 'LCO' AS Clientownertype
# MAGIC , t2.LocalClientOwnerName AS Clientownername 
# MAGIC
# MAGIC from radar.clients  t1
# MAGIC left join party_lco t2 on t1.latestCaseId = t2.CaseId
# MAGIC
# MAGIC
# MAGIC where t1.KYCGroup in ('Mars Incorporated', 'Darling Group', 'Darling International Netherlands B.V.', 'AGCO Group', 'AGCO International GmbH')
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select Gcobid
# MAGIC ,FullLegalName
# MAGIC , KYCGroup
# MAGIC , 'GCO' AS Clientownertype
# MAGIC , GlobalClientOwner AS Clientownername 
# MAGIC from radar.clients where KYCGroup in ('Mars Incorporated', 'Darling Group', 'Darling International Netherlands B.V.', 'AGCO Group', 'AGCO International GmbH')

# COMMAND ----------

# MAGIC %sql
# MAGIC select Gcobid
# MAGIC , KYCGroup
# MAGIC , 'GCO' AS Clientownertype
# MAGIC , GlobalClientOwner AS Clientownername 
# MAGIC from radar.clients where KYCGroup in ('Mars Incorporated', 'Darling Group', 'Darling International Netherlands B.V.', 'AGCO Group', 'AGCO International GmbH')

# COMMAND ----------


