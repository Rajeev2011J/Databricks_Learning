# Databricks notebook source
# MAGIC %md
# MAGIC David rangkouw vraagt:
# MAGIC
# MAGIC
# MAGIC Data GCOB en GCDS
# MAGIC
# MAGIC - 1: Geen unieke identifiers tussen een Natural Person (NP) en een Legal Entity (LE)
# MAGIC   - Ik laat een kolom UniqueGcobID toevoegen aan case_party_client_details
# MAGIC
# MAGIC - 2: Dubbele records op CaseId-niveau. Alleen de waarde in kolom ‘ConsultationType’ is anders, waardoor het lijkt dat er op Case-niveau ook sprak is van one-to-many
# MAGIC   - plan ik in voor volgende sprint om te repareren.
# MAGIC
# MAGIC - 3: Een afgeronde case zonder een datum in CaseCompletedDate
# MAGIC   - gek. ik zal een bug registreren of het een fout in source data is, of in logica.
# MAGIC
# MAGIC - 4: Verschil in de FullLegalName tussen GCDS en GCOB (geen directe issue als we bijvoorbeeld bepalen om de naam in GCOB aan te houden, maar wel goed om hier te benoemen)
# MAGIC   - GRPA (Party module) wordt geimplementeerd om een enkele input voor party opvoer te hebben, maar er zullen nog wel een tijd verschillen zijn
# MAGIC
# MAGIC - 5: Inconsistentie tussen de waarden in kolom SourceClient (start met NP_NPPC_...) en ClientType (Natural Person)
# MAGIC   - Wat was de verwachting? De betekenis van NP_NPPC is: 'scope met NP OF NPPC'. Alle NP's in de scope van Wholesale NL zijn normale NP's - geen NPPC's. NPPC's zijn een term voor Rural in North America (RAF).
# MAGIC
# MAGIC - 6: Klanten met één GCID in GCDS, maar met een verwijzing naar meer dan 1 actieve GCOBID’s
# MAGIC   - They seem like duplicates in GCOB to me. CDD files would need to be merged to clean up GCOB (e.g. offboard one and keep the latest with all risks intact). 
# MAGIC
# MAGIC - 7: Klanten met dubbele records in tabel party_client_structure op basis van de ClientId, ChildId, ParentId en TypeOfRelation
# MAGIC   - Ik heb ook een bug aangemaakt voor de volgende sprint. Mijn vermoeden voor de oorzaak is alsvolgt: In de basis tabel van de GCOB clientStructureSnapshot, ligt niet een relatie naar een GCOBid, maar een relatie naar alle cases van dat GCOBid. Er is een kans dat die momenteel niet goed ontdubbeld worden.
# MAGIC  
# MAGIC
# MAGIC - Daarnaast zie ik ook hiaten in de data voor een check richting Siebel. Als ik in onze databases kijk welke klanten er met de juist bankcode en klantindeling in Siebel staan, dan vind ik veel klanten terug in GCOB, maar zeker niet allemaal. Dit baart mij grote zorgen, omdat deze check voor deze regio noodzakelijk is in verband met de controle voor de toegang van de data. Deze analyse moet ik nog verder afronden.
# MAGIC   - als ik in de sources van GCDS kijk, staat daar Siebel met bankcode 3000/3400/3508. Je zou verwachten dat dat een superset is van de selectie die je maakt. Zie hier de link; https://confluence.dev.rabobank.nl/display/gcds/Producers . Dat lijkt me een goed aanknopingspunt als je wilt zoeken hoe de siebel-data in GCDS terecht komt
# MAGIC
# MAGIC - Daarnaast heb ik gisteren nog een overleg gehad met Peter van de Schepop van W&R MI en hij geeft aan alle bovenstaande punten ook te herkennen. Volgens Peter is het zelfs zo dat het aanpassen van de bankcode en klantindeling in Siebel handmatig gebeurt. Om hiervoor een beter beeld te krijgen heb ik voor volgende week een call met Barend van der Meulen van team W&R MI. Hij heeft veel ervaring met de GCDS en GCOB-data.
# MAGIC   - Barend heeft zeker veel ervaring met de data. Ik kan me voorstellen dat je ook meer van de processen wilt begrijpen die voor de input van de data zorgen. Zo uit mijn hoofd - maar is een tijd geleden dat ik bij het Siebel-GCDS-GCOB (SGG) project betrokken was - is het current account team in Retail NL verantwoordelijk voor data opvoer - maar had niet het budget of prioriteit voor continue data management. Een type proces waar je in zou kunnen duiken is de 'fixatie' - waarin bijv. de bankcode van een siebel - party wordt gewijzigd (naar 3000/3400 toe). 
# MAGIC
# MAGIC We hebben afgesproken dat je mij uiterlijk vrijdag een reactie op bovenstaande punten geeft. Zou je dit voor 10:00 uur willen doen? Dan ik heb ik nog de tijd om dit intern ook met mijn PM te bespreken.
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import pandas as pd
import os
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
import pyspark.sql.functions as F

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

Today = datetime.today().strftime('%Y%m%d')
load_dts = 'EDL_LOAD_DTS=' + Today + '*'
print (load_dts)

# COMMAND ----------

# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
#'party_AllCasesReport'
'party_case_client_details'
, 'party_LocalRequirement'
, 'party_local_client_Owners'
, 'party_client'
, 'party_client_structure'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{row.GDPname}/101/data/{load_dts}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,dubbele records en geen uniek id
# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     SourceClient, GcobId, ClientId, CaseId, CaseCompletedDate, FullLegalName, ClientType, GlobalClientOwnerLocation, RegisteredCity, IsLatestApprovedVersionOfClient, *
# MAGIC   
# MAGIC FROM
# MAGIC
# MAGIC     party_case_client_details
# MAGIC
# MAGIC WHERE
# MAGIC
# MAGIC     IsLatestApprovedVersionOfClient = true
# MAGIC
# MAGIC     and GcobId = 16
# MAGIC
# MAGIC ORDER BY
# MAGIC
# MAGIC     CaseCompletedDate DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_client limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## item 2

# COMMAND ----------

# DBTITLE 1,Dubbele records op CaseId-niveau. Alleen de waarde in kolom ‘ConsultationType’ is anders
# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     SourceClient,
# MAGIC     ConsultationType,
# MAGIC
# MAGIC     ClientId,
# MAGIC
# MAGIC     CaseId,
# MAGIC
# MAGIC     GcobId,
# MAGIC
# MAGIC     FullLegalName,
# MAGIC
# MAGIC     ClientType,
# MAGIC
# MAGIC     GlobalClientOwnerLocation,
# MAGIC
# MAGIC     RegisteredCity,
# MAGIC
# MAGIC     IsLatestApprovedVersionOfClient,
# MAGIC
# MAGIC     *
# MAGIC
# MAGIC FROM
# MAGIC
# MAGIC     party_case_client_details
# MAGIC
# MAGIC WHERE
# MAGIC
# MAGIC     IsLatestApprovedVersionOfClient = true
# MAGIC
# MAGIC     AND ClientType = 'Legal Entity'
# MAGIC
# MAGIC     AND ClientId IN (
# MAGIC
# MAGIC         SELECT
# MAGIC
# MAGIC             ClientId
# MAGIC
# MAGIC         FROM
# MAGIC
# MAGIC             party_case_client_details
# MAGIC
# MAGIC         WHERE
# MAGIC
# MAGIC             IsLatestApprovedVersionOfClient = true
# MAGIC
# MAGIC             AND ClientType = 'Legal Entity'
# MAGIC
# MAGIC         GROUP BY
# MAGIC
# MAGIC             ClientId
# MAGIC
# MAGIC         HAVING
# MAGIC
# MAGIC             COUNT(*) > 1
# MAGIC
# MAGIC     )
# MAGIC
# MAGIC     ORDER BY
# MAGIC
# MAGIC     ClientId ASC;

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,3: Een afgeronde case zonder een datum in CaseCompletedDate
# MAGIC %sql
# MAGIC --3 
# MAGIC SELECT
# MAGIC
# MAGIC     SourceClient, GcobId, ClientId, CaseId, CaseStatusName, CaseCompletedDate, FullLegalName, ClientType, GlobalClientOwnerLocation, RegisteredCity, IsLatestApprovedVersionOfClient, *
# MAGIC
# MAGIC FROM
# MAGIC
# MAGIC     party_case_client_details
# MAGIC
# MAGIC WHERE
# MAGIC
# MAGIC     IsLatestApprovedVersionOfClient = true
# MAGIC
# MAGIC     AND ClientType = 'Legal Entity'
# MAGIC
# MAGIC     AND CaseStatusName = 'Completed'
# MAGIC
# MAGIC     AND CaseCompletedDate IS NULL
# MAGIC
# MAGIC ORDER BY
# MAGIC
# MAGIC     GcobId ASC

# COMMAND ----------

# DBTITLE 1,connecting gcds parquet files
## Loading GCDS
# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
    , 'client_Client'
    , 'client_PartyRole'
    , 'client_Products'
    , 'client_OnboardedLocations'
]

for item in gcds_tables:
    # get the most recent file available in gdp
    path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4602/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4602/data/LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcds_'+ item)


# COMMAND ----------

# DBTITLE 1,5; GCOB/GCDS namen niet hetzelfde
# MAGIC %sql
# MAGIC -- vb4
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     gcob_case_client.GcobId GCOB_Case_Client_GcobId,
# MAGIC
# MAGIC     gcob_case_client.IsLatestApprovedVersionOfClient GCOB_Case_Client_Latest_Approval,
# MAGIC
# MAGIC     gcob_case_client.SourceClient GCOB_Case_Client_Source_Client,
# MAGIC
# MAGIC     gcob_case_client.ClientType GCOB_Case_Client_Type,
# MAGIC
# MAGIC     gcob_case_client.FullLegalName GCOB_Case_Client_Full_Legal_Name,
# MAGIC
# MAGIC     client.Full_legal_name GCDS_Client_Full_Legal_Name,
# MAGIC
# MAGIC     key_store.KeyStore_value GCDS_Key_Store_Value,
# MAGIC
# MAGIC     key_store.KeyStore_Type GCDS_Key_Store_Type,
# MAGIC
# MAGIC     key_store.Status GCDS_Key_Store_Status,
# MAGIC
# MAGIC     client.GCID GCDS_Client_GCID
# MAGIC
# MAGIC FROM
# MAGIC
# MAGIC     party_case_client_details as gcob_case_client
# MAGIC
# MAGIC JOIN
# MAGIC
# MAGIC     gcds_client_KeyStoreKey as key_store
# MAGIC
# MAGIC     ON gcob_case_client.GcobId = key_store.KeyStore_value
# MAGIC
# MAGIC JOIN
# MAGIC
# MAGIC     gcds_client_client as client
# MAGIC
# MAGIC     ON key_store.GCID = client.GCID
# MAGIC
# MAGIC WHERE
# MAGIC
# MAGIC     gcob_case_client.IsLatestApprovedVersionOfClient = true AND
# MAGIC
# MAGIC     key_store.Status = 'Active' AND
# MAGIC
# MAGIC     KeyStore_type = 'GCOBID' AND
# MAGIC
# MAGIC     gcob_case_client.FullLegalName <> client.Full_legal_name AND
# MAGIC
# MAGIC     gcob_case_client.ClientType = 'Legal Entity'
# MAGIC
# MAGIC ORDER BY
# MAGIC
# MAGIC     GCOB_Case_Client_GcobId ASC
# MAGIC
# MAGIC LIMIT 1000

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,NP as NP/NPPC
# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     CaseStatusName, SourceClient, GcobId, ClientId, CaseId, CaseCompletedDate, FullLegalName, ClientType, GlobalClientOwnerLocation, RegisteredCity, IsLatestApprovedVersionOfClient, *
# MAGIC
# MAGIC FROM
# MAGIC
# MAGIC     party_case_client_details
# MAGIC
# MAGIC WHERE
# MAGIC
# MAGIC     ClientType <> 'Natural Person'
# MAGIC
# MAGIC     AND SourceClient like 'NP_NPPC_%'
# MAGIC
# MAGIC     AND GlobalClientOwnerLocation = 'Rabobank Netherlands'
# MAGIC
# MAGIC ORDER BY
# MAGIC
# MAGIC     CaseStatusName ASC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gcds_client_client limit 4

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gcds_client_PartyRole limit 10

# COMMAND ----------

# DBTITLE 1,6; keys niet 1:1 match
# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     gcds_cksk.GCID,
# MAGIC
# MAGIC     gcds_cksk.KeyStore_type,
# MAGIC
# MAGIC     gcds_cksk.KeyStore_value,
# MAGIC
# MAGIC     gcds_recordcount.record_count,
# MAGIC
# MAGIC     gcob_pccd.FullLegalName,
# MAGIC
# MAGIC     gcob_pccd.clientLifecycleName AS GCOB_lifecycleName,
# MAGIC
# MAGIC     t4.life_cycle_status AS GCDS_LifecycleName
# MAGIC
# MAGIC
# MAGIC
# MAGIC FROM
# MAGIC
# MAGIC     gcds_client_keystorekey as  gcds_cksk
# MAGIC
# MAGIC INNER JOIN
# MAGIC
# MAGIC     (SELECT
# MAGIC
# MAGIC         GCID,
# MAGIC
# MAGIC         KeyStore_type,
# MAGIC
# MAGIC         COUNT(*) as record_count
# MAGIC
# MAGIC      FROM
# MAGIC
# MAGIC         gcds_client_keystorekey gcds_cksk
# MAGIC
# MAGIC      WHERE
# MAGIC
# MAGIC         KeyStore_type = 'GCOBID'
# MAGIC
# MAGIC      GROUP BY
# MAGIC
# MAGIC         GCID,
# MAGIC
# MAGIC         KeyStore_type
# MAGIC
# MAGIC      HAVING
# MAGIC
# MAGIC         COUNT(*) > 1) gcds_recordcount
# MAGIC
# MAGIC ON
# MAGIC
# MAGIC     gcds_cksk.GCID = gcds_recordcount.GCID
# MAGIC
# MAGIC     AND gcds_cksk.KeyStore_type = gcds_recordcount.KeyStore_type
# MAGIC
# MAGIC LEFT JOIN
# MAGIC
# MAGIC     party_case_client_details gcob_pccd
# MAGIC
# MAGIC ON
# MAGIC
# MAGIC     CAST(gcds_cksk.KeyStore_value AS STRING) = CAST(gcob_pccd.GCOBID AS STRING)
# MAGIC
# MAGIC     AND gcob_pccd.IsLatestApprovedVersionOfClient = True
# MAGIC
# MAGIC LEFT JOIN
# MAGIC     (select * from gcds_client_PartyRole where party_role = 'customer') as t4 on gcds_cksk.GCID = t4.GCID
# MAGIC
# MAGIC WHERE
# MAGIC
# MAGIC     gcds_cksk.KeyStore_type = 'GCOBID'
# MAGIC
# MAGIC     AND gcds_cksk.Status = 'Active'
# MAGIC
# MAGIC ORDER BY
# MAGIC
# MAGIC     gcds_cksk.GCID ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from party_case_client_details where gcobId in (31275, 18023) and IsLatestApprovedVersionOfClient = True

# COMMAND ----------

# MAGIC %sql
# MAGIC select t2.* , t1.* 
# MAGIC from gcds_client_client t1
# MAGIC left join gcds_client_PartyRole as t2 on t1.GCID = t2.GCID
# MAGIC  where t1.GCID in (10309, 10458, 10458, 128718, 128718, 15664)
# MAGIC  order by t1.GCID
# MAGIC

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,item 7; client structure - multiple relations
# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     clientid, childidentity, parentidentity, ClientFullLegalName, TypesOfRelation, count(*)  
# MAGIC
# MAGIC FROM
# MAGIC
# MAGIC     party_client_structure
# MAGIC
# MAGIC GROUP BY
# MAGIC
# MAGIC     clientid, childidentity, parentidentity, ClientFullLegalName, TypesOfRelation
# MAGIC
# MAGIC HAVING
# MAGIC
# MAGIC     count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     sourceclient, clientid, childidentity, parentidentity, ClientFullLegalName, TypesOfRelation, CalculatedShareholdingPercentage, *
# MAGIC FROM
# MAGIC
# MAGIC     party_client_structure
# MAGIC
# MAGIC where clientid = 174 and childidentity = 174 and parentidentity = 172

# COMMAND ----------


