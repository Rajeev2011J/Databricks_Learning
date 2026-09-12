# Databricks notebook source
import os
import requests
from functions_dataverse import get_access_token, upsert_to_dataverse_table

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
tenant_id = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

dataverse_url = os.environ['DATAVERSE_URL']
dataverse_api_url = f"{dataverse_url}/api/data/v9.1/"

# COMMAND ----------

access_token = get_access_token(tenant_id, app_reg_app_id, service_credential, dataverse_url)

# COMMAND ----------

query = f'''
  SELECT
      facilityCode AS rdr_facilitycode
    , dateOfBreach AS rdr_dateofbreach
    , Gcid AS rdr_gcid
    , gcidClientName AS rdr_gcidclientname
    , RelationshipManagerName AS rdr_relationshipmanagername
    , Description AS rdr_description
    , Category AS rdr_category
    , FacilityStatus AS rdr_facilitystatus
    , Revolving AS rdr_revolving
    , FacilityCurrency AS rdr_facilitycurrency
    , FacilityStartDate AS rdr_facilitystartdate
    , FacilityExpiryDate AS rdr_facilityexpirydate
    , Availability AS rdr_availability
    , LimitAmount AS rdr_limitamount
    , CollateralAmount AS rdr_collateralamount
    , EffectiveLimitAmount AS rdr_effectivelimitamount
    , Utilisation AS rdr_utilisation
    , UnavailableAmount AS rdr_unavailableamount
    , WithheldAmount AS rdr_withheldamount
    , AvailableAmount AS rdr_availableamount
    , LiabilityCode AS rdr_liabilitycode
    , LiabilityName AS rdr_liabilityname
    , CreditApprovalDate AS rdr_creditapprovaldate
    , AvailabilityEndDate AS rdr_availabilityenddate
    , PortfolioCode AS rdr_portfoliocode
    , PortfolioName AS rdr_portfolioname
    , CountryOfRisk AS rdr_countryofrisk
    , Margin AS rdr_margin
    , mainFacilityCode AS rdr_mainfacilitycode
    , intraDayLimitAmount AS rdr_intradaylimitamount
    , intraDayLimitAmountStartdate AS rdr_intradaylimitamountstartdate
    , newAvailableAmount AS rdr_newavailableamount
    , newAvailableAmountEuro AS rdr_newavailableamounteuro
    , conversionRate AS rdr_conversionrate
    , facilityStatusRecordDescription AS rdr_facilitystatusrecorddescription
    , dateOfFirstBreach AS rdr_dateoffirstbreach
  FROM radar.facility_limit
'''

# COMMAND ----------

upsert_to_dataverse_table(dataverse_api_url, 'rdr_flexcubedatas', query, access_token, 'rdr_facilitycode', 'rdr_flexcubedataid', 1000)
