# Databricks notebook source
# MAGIC %md
# MAGIC ## FATCA, CRS And UBO Reports for  CFO Reporting Hub
# MAGIC Goal: To Get Fatca, CRSTaxResidencies and UBO details for NL Clients
# MAGIC  
# MAGIC ###### <span style="color:orange">Authors:</span>
# MAGIC 1.  Mahalakshmi.Vengateswaran@rabobank.com
# MAGIC 2.  Devi.Chennareddy@rabobank.com
# MAGIC  
# MAGIC ###### <span style="color:orange">Business value:</span>
# MAGIC  
# MAGIC      List of NL,Milan,Madrid,Paris,Frankfurt clients and their Fatca, CRSTaxResidencies and UBO details
# MAGIC  
# MAGIC  
# MAGIC ###### <span style="color:blue">Flow of the logic:</span>
# MAGIC 1. Define the business date manually for which the data is being processed
# MAGIC 2. Read data from GDP Defined layer
# MAGIC 3. Fetch the required columns for each report and write appropriate join conditions
# MAGIC 4. Load the data as separate tables per location into the catalog
# MAGIC

# COMMAND ----------

#Importing required packages
import os
import pyspark.sql.functions as F
import pandas as pd
from datetime import datetime

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

#Change the File date  for which the data needs to be loaded
ActDts=datetime.strptime('20251231',"%Y%m%d").date()
ActDtsLoad = 'LOAD_DT=' + str(ActDts).replace("-","") + '*'
BusinessDate = ActDts

# COMMAND ----------

#Fetching Last loaded Date and DeliverySequence from the catalog table
Last_Load_ActDts=spark.table('radar.FatcaCRSAdhocReportNetherlands').select("ActDts").head()[0]
DeliverySqn=spark.table('radar.FatcaCRSAdhocReportNetherlands').select("DeliverySqn").head()[0]

# COMMAND ----------

# Deriving dates which are to be added to the results
Last_Load_ActDts_str = Last_Load_ActDts.strftime("%Y-%m-%d")
prevActDts = datetime.strptime(Last_Load_ActDts_str, "%Y-%m-%d").date()
LoadDts=(datetime.today()).strftime("%Y-%m-%d") 
#Calculating the Delivery Sequence for particular year
if ActDts.year == datetime.strptime(Last_Load_ActDts_str, "%Y-%m-%d").year:
    DeliverySqn+=1
else:
    DeliverySqn=1 

# COMMAND ----------

# Method to add all the dates and delivery sequence Attributes to the dataframe
def add_dates(sdf):
    sdf = sdf.withColumn("DeliverySqn", F.lit(DeliverySqn))
    sdf = sdf.withColumn("ActDts", F.lit(ActDts))
    sdf = sdf.withColumn("prevActDts", F.lit(prevActDts))
    sdf = sdf.withColumn("LoadDts", F.lit(LoadDts))
    #Adding Business Date column
    new_column = sdf.columns[:0] + ["BusinessDate"] + sdf.columns[0:]
    sdf= sdf.select(*sdf.columns).withColumn("BusinessDate", F.lit(BusinessDate)).select(*new_column)
    return sdf

# COMMAND ----------

# List of datasets from GDP
load_df = pd.DataFrame({'GDPname':[
'party_case_client_details'
,'party_AllPartyDetails'
,'party_tax_info_fatca_crs'
,'party_client_structure'
,'party_client_structure_GUI'
,'party_products_and_sevices'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    spark.read.parquet(f'abfss://gcob@{ReadStorage}.dfs.core.windows.net/CaseService/{row.GDPname}/102/data/{ActDtsLoad}/*.parquet').createOrReplaceTempView(row.GDPname)

# COMMAND ----------

# MAGIC %md
# MAGIC **FATCACRS REPORT**

# COMMAND ----------

# MAGIC %sql
# MAGIC --Generating FATCACRS Report
# MAGIC CREATE OR REPLACE TEMPORARY VIEW FatcaCrs AS
# MAGIC select distinct 
# MAGIC c.GcobId
# MAGIC ,crs.GCID
# MAGIC ,c.CaseId
# MAGIC ,c.FullLegalName
# MAGIC ,concat(COALESCE(c.RegisteredStreet,''), ' ', COALESCE(c.RegisteredNumber,''), ' ',COALESCE(c.RegisteredCity,''), ' ',COALESCE(c.RegisteredRegion,''), ' ',COALESCE(c.RegisteredPostalCode,''), ' ',COALESCE(c.CountryOfRegistration,'')) as RegisteredAddress
# MAGIC ,case when c.IsOperatingAddressDifferentToRegisteredAddress = 1 then concat(COALESCE(c.OperatingStreet,''), ' ', COALESCE(c.OperatingNumber,''), ' ',COALESCE(c.OperatingCity,''), ' ',COALESCE(c.OperatingRegion,''), ' ',COALESCE(c.OperatingPostalCode,''), ' ',COALESCE(c.CountryOfOperation,'')) else concat(COALESCE(c.RegisteredStreet,''), ' ', COALESCE(c.RegisteredNumber,''), ' ',COALESCE(c.RegisteredCity,''), ' ',COALESCE(c.RegisteredRegion,''), ' ',COALESCE(c.RegisteredPostalCode,''), ' ',COALESCE(c.CountryOfRegistration,''))  end as OperatingAddress
# MAGIC ,c.IsEligibleForFatcaAssessment
# MAGIC ,c.FatcaClassification
# MAGIC ,crs.Giin
# MAGIC ,crs.Ein
# MAGIC ,crs.DateOfIssue as FatcaDateOfIssue
# MAGIC ,c.IsEligibleForCrsAssessment
# MAGIC ,c.CrsClassification
# MAGIC ,crs.FormSignedDate as CrsFormSignedDate
# MAGIC ,c.ClientLifeCycleName
# MAGIC ,c.IsClientListed
# MAGIC ,c.SourceSystem
# MAGIC ,c.GlobalClientOwnerLocation
# MAGIC ,pns.BookingEntityLocation
# MAGIC ,pns.ProductOfferingLocation
# MAGIC from party_case_client_details c
# MAGIC Left join party_tax_info_fatca_crs crs on c.SourceClient = crs.SourceClient
# MAGIC Left join party_products_and_sevices pns on c.SourceClient = pns.SourceClient
# MAGIC where  c.IsLatestApprovedVersionOfClient=true and c.ClientLifeCycleName in ('FormerClient','Client','ExitClient') and (c.GlobalClientOwnerLocation in ('Rabobank Netherlands','Rabobank Milan','Rabobank Madrid','Rabobank Paris','Rabobank Frankfurt') or pns.BookingEntityLocation in ('Rabobank Netherlands','Rabobank Milan','Rabobank Madrid','Rabobank Paris','Rabobank Frankfurt') or pns.ProductOfferingLocation in ('Rabobank Netherlands','Rabobank Milan','Rabobank Madrid','Rabobank Paris','Rabobank Frankfurt'))

# COMMAND ----------

#Creating dataframe from Tempview
df_fatcacrs=spark.table('FatcaCrs')
#Adding extra attributes to the dataframe
df_fatcacrs=add_dates(df_fatcacrs)


# COMMAND ----------

from pyspark.sql import DataFrame

def fatca_crs_by_location(df_fatcacrs: DataFrame, table_prefix: str ):
    columns_to_drop = ['GlobalClientOwnerLocation', 'ProductOfferingLocation', 'BookingEntityLocation']
    locations = ['Rabobank Netherlands', 'Rabobank Milan', 'Rabobank Madrid','Rabobank Paris','Rabobank Frankfurt']
    
    for location in locations:
        # Filter rows where any of the three location columns match the current location
        df_filtered = df_fatcacrs.filter(
            (df_fatcacrs.GlobalClientOwnerLocation == location) |
            (df_fatcacrs.ProductOfferingLocation == location) |
            (df_fatcacrs.BookingEntityLocation == location)
        )
        
        # Drop specified columns and remove duplicates
        df_final = df_filtered.drop(*columns_to_drop).distinct()
        
        print(f"Count of {location}: {df_final.count()}")
        # Write the DataFrame directly to a table
        table_name = f"{table_prefix}{location.replace('Rabobank ', '')}"
        df_final.write.mode('overwrite').saveAsTable(table_name)
        print(f"Saved table: {table_name}")

# COMMAND ----------

fatca_crs_by_location(df_fatcacrs,table_prefix='radar.FatcaCRSAdhocReport')

# COMMAND ----------

# MAGIC %md
# MAGIC **CRSTAXRESIDENCIES REPORT**

# COMMAND ----------

# MAGIC %sql
# MAGIC --Generating CrsTaxResidencies Report 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CrsTaxResidencies AS
# MAGIC select distinct 
# MAGIC c.GcobId
# MAGIC ,apd.GCDSID as GCID
# MAGIC ,c.CaseId
# MAGIC ,c.FullLegalName
# MAGIC ,apd.CountryOfTaxResidenceIsoCode as CountryOfTaxResidence
# MAGIC ,c.IsClientListed
# MAGIC ,apd.TinAvailable
# MAGIC ,apd.TinOrEquivalent
# MAGIC ,apd.TinUnavailabilityReason
# MAGIC ,apd.ExplanationForTinBeingUnavailable AS TinUnavailableExplanation
# MAGIC ,c.ClientLifeCycleName
# MAGIC ,c.IsLatestApprovedVersionOfClient
# MAGIC ,c.SourceSystem
# MAGIC ,c.GlobalClientOwnerLocation
# MAGIC ,pns.BookingEntityLocation
# MAGIC ,pns.ProductOfferingLocation
# MAGIC from party_case_client_details c
# MAGIC Left Join party_AllPartyDetails apd on c.sourceclient = CASE WHEN apd.ClientType='LegalEntityClient' THEN concat('LEC_',apd.Id)
# MAGIC WHEN apd.ClientType IN ('NaturalPersonClient','Natural Person acting in a Professional Capacity (NPPC)') THEN concat('NP_NPPC_',apd.Id) end and apd.status="Snapshot" and apd.IsLatestApprovedVersionOfClient = True
# MAGIC  Left join party_products_and_sevices pns on c.SourceClient = pns.SourceClient
# MAGIC where  c.IsLatestApprovedVersionOfClient=true and c.ClientLifeCycleName in ('FormerClient','Client','ExitClient')
# MAGIC  and (c.GlobalClientOwnerLocation in ('Rabobank Netherlands','Rabobank Milan','Rabobank Madrid','Rabobank Paris','Rabobank Frankfurt') or pns.BookingEntityLocation in ('Rabobank Netherlands','Rabobank Milan','Rabobank Madrid','Rabobank Paris','Rabobank Frankfurt') or pns.ProductOfferingLocation in ('Rabobank Netherlands','Rabobank Milan','Rabobank Madrid','Rabobank Paris','Rabobank Frankfurt'))

# COMMAND ----------

#Creating dataframe from Tempview
df_CrsTaxResidencies=spark.table('CrsTaxResidencies')
#Adding extra attributes to the dataframe
df_CrsTaxResidencies=add_dates(df_CrsTaxResidencies)


# COMMAND ----------

fatca_crs_by_location(df_CrsTaxResidencies,table_prefix='radar.CRSTaxResidenciesAdhocReport')

# COMMAND ----------

# MAGIC %md
# MAGIC **UBO REPORT**

# COMMAND ----------

# MAGIC %sql
# MAGIC --Generating UBO Report
# MAGIC CREATE OR REPLACE TEMPORARY VIEW UBO AS
# MAGIC select distinct 
# MAGIC c.GcobId as ClientGcobId
# MAGIC ,crs.GCID
# MAGIC ,c.FullLegalName
# MAGIC ,c.RegisteredCountryIsoCode as ClientCountryOfRegistration
# MAGIC ,cs.ParentIdentity as ShareholderGcobId
# MAGIC ,apd.FirstName
# MAGIC ,apd.MiddleName
# MAGIC ,apd.LastName
# MAGIC ,apd.DateOfBirth
# MAGIC ,apd.NationalityIsoCode as Nationality
# MAGIC ,apd.CitizenshipIsocode as Citizenship
# MAGIC ,apd.CountryOfTaxResidenceIsoCode as CountryOfTaxResidence
# MAGIC ,apd.RegisteredStreet as ResidentialStreet
# MAGIC ,apd.RegisteredNumber as ResidentialNumber
# MAGIC ,apd.RegisteredPostalCode as ResidentialPostalCode
# MAGIC ,apd.RegisteredCity as ResidentialCity
# MAGIC ,apd.RegisteredRegion as ResidentialRegion
# MAGIC ,apd.RegisteredCountryIsoCode as ResidentialCountry
# MAGIC ,apd.TinAvailable
# MAGIC ,apd.TinOrEquivalent
# MAGIC ,apd.TinUnavailabilityReason as MissingTINReason
# MAGIC ,cs.CalculatedShareholdingPercentage as ShareholdingPercentage
# MAGIC ,cs.UBOReason as UBOThroughReason
# MAGIC ,c.GlobalClientOwnerLocation
# MAGIC ,pns.BookingEntityLocation
# MAGIC ,pns.ProductOfferingLocation
# MAGIC from party_case_client_details c
# MAGIC Left join party_tax_info_fatca_crs crs on c.SourceClient = crs.SourceClient  
# MAGIC inner Join party_client_structure_gui cs on c.SourceClient = cs.SourceClient and cs.ISUBO=true   and cs.TypesOfRelation in('UboShareholding','UBO') and cs.IsLatestApprovedVersionOfClient=true
# MAGIC Left outer Join party_AllPartyDetails apd 
# MAGIC on cs.UniqueParentPartyId = apd.UniquePartyId
# MAGIC and apd.PartyId = cs.ParentEntityId
# MAGIC and apd.Status = 'Snapshot'
# MAGIC Left join party_products_and_sevices pns on c.SourceClient = pns.SourceClient
# MAGIC where  C.ClientType='Legal Entity' and c.ClientLifeCycleName in ('FormerClient','Client','ExitClient') and (c.GlobalClientOwnerLocation in ('Rabobank Netherlands','Rabobank Milan','Rabobank Madrid','Rabobank Paris','Rabobank Frankfurt') or pns.BookingEntityLocation in ('Rabobank Netherlands','Rabobank Milan','Rabobank Madrid','Rabobank Paris','Rabobank Frankfurt') or pns.ProductOfferingLocation in ('Rabobank Netherlands','Rabobank Milan','Rabobank Madrid','Rabobank Paris','Rabobank Frankfurt')) 

# COMMAND ----------

#Creating dataframe from Tempview
df_UBO=spark.table('UBO')
#Adding extra attributes to the dataframe
df_UBO=add_dates(df_UBO)


# COMMAND ----------

fatca_crs_by_location(df_UBO,table_prefix='radar.UBOAdhocReport')
