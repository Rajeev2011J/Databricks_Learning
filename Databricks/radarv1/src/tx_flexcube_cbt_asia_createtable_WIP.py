# Databricks notebook source
# DBTITLE 1,connections
import os

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

# DBTITLE 1,check starting year month
from datetime import date
import sys

# get the latest TransactionMonthYear
result = spark.sql("""
    SELECT MAX(TransactionMonthYear) 
    FROM radar.tx
    WHERE SourceSystem LIKE '%CBT%'
""").collect()[0][0]

# fallback to default start date if no data available
if result:
    start_year  = result.year
    start_month = result.month
else:
    start_year  = 2026
    start_month = 1

start_date = f"{start_year - 1}-12-31"

# determine target year and month (today)
today = date.today()
target_year = today.year
target_month = today.month

# stop notebook if target and start dates match
if target_year == start_year and target_month == start_month:
    print(f"Target date {target_year}-{target_month:02d} "
          f"is the same as latest TransactionMonthYear. Stopping execution.")
    sys.exit()

# COMMAND ----------

# DBTITLE 1,year_month_combinations
# create year month combination starting from start year/month to target

def create_year_month_combinations(start_year, start_month, target_year, target_month):
    year_month_combinations = []
    for year in range(start_year, target_year + 1):
        start_m = start_month if year == start_year else 1
        end_m = target_month - 1 if year == target_year else 12 # remove -1 to include current month
        for month in range(start_m, end_m + 1):
            year_month_combinations.append((str(year), f'{month:02d}'))
    return year_month_combinations

year_month_combinations = create_year_month_combinations(start_year, start_month, target_year, target_month)

# COMMAND ----------

# DBTITLE 1,monthly_avg_exchange_rates
from pyspark.sql import functions as F
import re

columns_to_load = [
    'PriceCurrency', 
    'Close_2100CET', 
    'BaseCurrency', 
    'RateDate'
]

df_merged = None

for year, month in year_month_combinations:
    # get the most recent version available in gdp
    path = f'abfss://timescape@edlcorestdeuprod0001.dfs.core.windows.net/fx_rates_2100cet/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

    version = 1 # forcing version = 1 until the new version works

    df = spark.read.parquet(f'{path}{version}/data/RATES_DT={year}{month}*/*.parquet') \
        .select(*columns_to_load) \
        .filter(F.col('BaseCurrency') == 'EUR')

    # group by pricecurrency and calculate avg
    df = df.groupBy('PriceCurrency').agg(F.avg('Close_2100CET').alias('avg_exchange_rate'))

    # rename pricecurrency to currency_codes
    df = df.withColumnRenamed('PriceCurrency', 'currency_codes')

    # add column yearmonth
    df = df.withColumn('YearMonth', F.lit(f'{year}-{month}'))

    if df_merged is not None:
        df_merged = df_merged.union(df)
    else:
        df_merged = df

df_merged.createOrReplaceTempView('monthly_avg_exchange_rates')

# COMMAND ----------

# DBTITLE 1,ebx_static_country_list
# create country list risk per each year month

from pyspark.sql.functions import lit, concat, col, row_number
from pyspark.sql import Window

df_merged = None

for year, month in year_month_combinations:
    
    month_adjusted = month if not (year == '2023' and int(month) < 10) else '10'
    df = spark.read.parquet(f'abfss://ebx@edlcorestdeuprod0001.dfs.core.windows.net/EBX_FECCountryRiskList/1/data/EDL_LOAD_DT={year}{month_adjusted}*/*.parquet')

    df = df.withColumn('YearMonth', concat(lit(year), lit('-'), lit(month)))

    df = df.select(
        col('COUNTRYNAMEENGLISH').alias('country_name'),
        col('COUNTRYCODE').alias('ISO_code'),
        col('MONEYLAUNDERINGRISK').alias('Money_Laundering_Risk'),
        col('TERRORISMFINANCINGRISK').alias('Terrorism_Financing_Risk'),
        col('SANCTIONSRISK').alias('Sanctions_Risk'),
        col('CORRUPTIONRISK').alias('Corruption_Risk'),
        col('TAXINTEGRITYRISK').alias('Tax_Integrity_Risk'),
        # col('ECNONCOOPERATIVEJURISDICTIONS').alias('EC_Non_Cooperative_Jurisdictions'), # not present in past files, not needed anyway
        col('HIGHFATFRISK').alias('High_FATF_Risk'),
        col('ECHIGHRISK').alias('EC_High_Risk'),
        col('TOTALITARIANREGIMESRISK').alias('Totalitarian_Regimes'),
        col('VALIDTILL'),
        col('YearMonth'),
        col('EDL_LOAD_DTS')
    ).where(col('VALIDTILL').isNull()).distinct()
    
    if df_merged is None:
        df_merged = df
    else:
        df_merged = df_merged.union(df)

window_spec = Window.partitionBy('ISO_code', 'YearMonth').orderBy('EDL_LOAD_DTS')
df_merged = df_merged.withColumn('row_number', row_number().over(window_spec)).where(col('row_number') == 1).drop('row_number', 'EDL_LOAD_DTS').distinct()

df_merged.createOrReplaceTempView('ebx_static_country_list')

# COMMAND ----------

# DBTITLE 1,cbt_asia_tx
from pyspark.sql.functions import col, lit, date_format
import re

citylist = {
    'HKG': 'Hong Kong'
    , 'SGP': 'Singapore' 
    , 'NLU': 'Utrecht'
    , 'CNS': 'Shanghai'
    , 'INM': 'Mumbai'
}

columns_to_load = [
    'debitTransaction_valueDate'
    # , 'debitTransaction_debitValueDate'
    , 'creditTransaction_valueDate'
    # , 'creditTransaction_creditValueDate'

    , 'debitTransaction_amount'
    # , 'debitTransaction_debitAmount'
    , 'creditTransaction_amount'
    # , 'creditTransaction_creditAmount'

    , 'debitTransaction_currency'
    # , 'debitTransaction_debitAccountCurrency'
    , 'creditTransaction_currency'
    # , 'creditTransaction_creditAccountCurrency'

    , 'primaryFeatures_typeName'
    # , 'contractProductCodeDescription'
    , 'primaryFeatures_typeCode'
    # , 'contractProductCode'
    # , 'BUSINESS_DTS'
    , 'EDL_LOAD_DTS'

    # , 'additionalSettlement_debitOrCreditIndicator'

    , 'settlement_ultimateBeneficiaryOneDescription'
    , 'settlement_ultimateBeneficiaryTwoDescription'
    , 'settlement_ultimateBeneficiaryThreeDescription'
    , 'settlement_ultimateBeneficiaryFourDescription'
    , 'settlement_ultimateBeneficiaryFiveDescription'
    , 'settlement_ultimateBeneficiaryCountryDescription'

    , 'settlement_accountWithInstitutionOneDescription'
    , 'settlement_accountWithInstitutionTwoDescription'
    , 'settlement_accountWithInstitutionThreeDescription'
    , 'settlement_accountWithInstitutionFourDescription'
    , 'settlement_accountWithInstitutionFiveDescription'
    , 'settlement_accountWithInstitutionCountryDescription'
    , 'settlement_byOrderOfOneDescription'
    , 'settlement_byOrderOfTwoDescription'
    , 'settlement_byOrderOfThreeDescription'
    , 'settlement_byOrderOfFourDescription'
    , 'settlement_byOrderOfFiveDescription'
    , 'settlement_byOrderOfCountryDescription'

    , 'transactionReference'

    , 'europeSpecific_creditIBAN'
    , 'europeSpecific_debitIBAN'

    , 'creditTransaction_accountNumber'
    , 'debitTransaction_accountNumber'

    , 'chinaSpecific_relatedAccountClass'
    , 'chinaSpecific_relatedAccountReference'

    , 'creditTransaction_accountDescription'
    , 'debitTransaction_accountDescription'

    , 'events_settlements_accountNumber'
    , 'events_settlements_accountCurrency'
]

df_cbt_asia_tx = None
for city_code, city_name in citylist.items():

    # get the most recent version available in gdp
    path = f'abfss://core-cbt@edlcorestdeuprod0001.dfs.core.windows.net/transaction_{city_code}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

    df_union = None

    # find all the files for a specific month using dbutils.fs.ls and run through each day, and check if there are empty day
    for year, month in year_month_combinations:
        df_month = (
            spark.read.parquet(f'{path}{version}/data/BUSINESS_DTS={year}{month}*') #.filter(col('EDL_LOAD_DTS').like(f'{year}{month}%'))
            .select(*columns_to_load)
            # select only below products
            .filter(col('primaryFeatures_typeCode').isin('CBTO', 'DCCO', 'XCCO', 'DCCI', 'CBTI'))
            .withColumn('SourceSystem', lit(f'CBT_{city_code}'))
            .withColumn('YearMonth', lit(f'{year}-{month}'))
        )

        # filter the data based on YEAR_MONTH to be after or equal to 202601
        df_month = df_month.filter(
            date_format(col("debitTransaction_ValueDate"), "yyyyMM") >= f"{year_month_combinations[0][0]}{year_month_combinations[0][1]}"
        )

        df_union = (df_month if df_union is None else df_union.unionByName(df_month))

    # union and create view
    df_cbt_asia_tx = (df_union if df_cbt_asia_tx is None else df_cbt_asia_tx.unionByName(df_union))


df_cbt_asia_tx.createOrReplaceTempView('cbt_asia_tx')

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct primaryFeatures_typeName from cbt_asia_tx 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cbt_asia_tx

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from radar.tx_wr_iban 
# MAGIC
# MAGIC where iban in (select distinct europeSpecific_debitIBAN from cbt_asia_tx where europeSpecific_debitIBAN is not null)
# MAGIC
# MAGIC  -- where iban like '%801110000756'

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cbt_asia_tx where europeSpecific_debitIBAN is not null
# MAGIC -- select distinct debitTransaction_accountNumber from cbt_asia_tx 
# MAGIC -- where (creditTransaction_accountNumber is not null or debitTransaction_accountNumber is not null)
# MAGIC
# MAGIC -- 0300056036
# MAGIC -- 344240002162

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct primaryFeatures_typeName
# MAGIC  from cbt_asia_tx

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct *
# MAGIC  from cbt_asia_tx LIMIT 2

# COMMAND ----------

# DBTITLE 1,cbt_asia_tx_final
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cbt_asia_tx_final AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   SourceSystem
# MAGIC   , AccountNumber
# MAGIC   , date_format(BOOKG_DT_TM_GMT, 'yyyy-MM') AS TransactionMonthYear
# MAGIC   
# MAGIC
# MAGIC
# MAGIC   
# MAGIC   , AmountOriginalTransaction
# MAGIC   , TransactionCurrency
# MAGIC   , CreditDebitIndicator
# MAGIC   , AccountHolderCountryCode
# MAGIC   , CounterpartyCountryCode
# MAGIC   , DetailledTrxTypeCode
# MAGIC   , TransactionsCount
# MAGIC
# MAGIC FROM cbt_asia_tx

# COMMAND ----------

# DBTITLE 1,cbt_asia_tx_final
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cbt_asia_tx_final AS
# MAGIC
# MAGIC WITH CTE AS (
# MAGIC     SELECT DISTINCT
# MAGIC         CONCAT('cbt_', t1.city) AS SourceSystem
# MAGIC         , YearMonth AS TransactionMonthYear
# MAGIC         , SUM(t1.debitTransaction_debitAmount) AS AmountOriginalTransaction
# MAGIC         -- , t2.accountCurrency AS TransactionCurrency_TEST
# MAGIC
# MAGIC         , t1.debitTransaction_debitAccountCurrency AS TransactionCurrency
# MAGIC         , t1.additionalSettlement_debitOrCreditIndicator[0] AS CreditDebitIndicator
# MAGIC         
# MAGIC         , CASE
# MAGIC             WHEN t1.additionalSettlement_debitOrCreditIndicator[0] = 'Debit' THEN
# MAGIC                 CASE 
# MAGIC                     WHEN SUBSTRING(t1.settlement_accountWithInstitutionOneDescription, 5, 2) RLIKE '^[A-Za-z]{2}$' THEN 
# MAGIC                         SUBSTRING(t1.settlement_accountWithInstitutionOneDescription, 5, 2)
# MAGIC                     ELSE 
# MAGIC                         COALESCE(SUBSTRING(t1.settlement_byOrderOfFiveDescription, 3, 2), 
# MAGIC                                  SUBSTRING(t1.settlement_byOrderOfFourDescription, 3, 2))
# MAGIC                 END
# MAGIC             ELSE ''
# MAGIC         END AS CounterpartyCountryCode
# MAGIC         , t1.settlement_ultimateBeneficiaryOneDescription
# MAGIC         , t1.settlement_ultimateBeneficiaryTwoDescription
# MAGIC         , t1.settlement_ultimateBeneficiaryThreeDescription
# MAGIC         , t1.settlement_ultimateBeneficiaryFourDescription
# MAGIC         , t1.settlement_ultimateBeneficiaryFiveDescription
# MAGIC         , t1.settlement_ultimateBeneficiaryCountryDescription
# MAGIC
# MAGIC         , t1.additionalSettlement_beneficiaryInstitutionOneDescription
# MAGIC         , t1.additionalSettlement_beneficiaryInstitutionTwoDescription
# MAGIC         , t1.additionalSettlement_beneficiaryInstitutionThreeDescription
# MAGIC         , t1.additionalSettlement_beneficiaryInstitutionFourDescription
# MAGIC         , t1.additionalSettlement_beneficiaryInstitutionFiveDescription
# MAGIC
# MAGIC         , t1.settlement_accountWithInstitutionOneDescription
# MAGIC         , t1.settlement_accountWithInstitutionTwoDescription
# MAGIC         , t1.settlement_accountWithInstitutionThreeDescription
# MAGIC         , t1.settlement_accountWithInstitutionFourDescription
# MAGIC         , t1.settlement_accountWithInstitutionFiveDescription
# MAGIC         , t1.settlement_accountWithInstitutionCountryDescription
# MAGIC
# MAGIC         , t1.settlement_byOrderOfOneDescription
# MAGIC         , t1.settlement_byOrderOfTwoDescription
# MAGIC         , t1.settlement_byOrderOfThreeDescription
# MAGIC         , t1.settlement_byOrderOfFourDescription
# MAGIC         , t1.settlement_byOrderOfFiveDescription
# MAGIC         , t1.settlement_byOrderOfCountryDescription
# MAGIC
# MAGIC         -- DO CREDIT PART!!
# MAGIC
# MAGIC
# MAGIC
# MAGIC         , '' AS CounterPartyBankBIC
# MAGIC         , t2.address_countryCode AS AccountHolderCountryCode
# MAGIC         , t3.GcobId
# MAGIC         , t1.contractProductCodeDescription AS TransactionType
# MAGIC         , COUNT(*) AS TransactionsCount
# MAGIC
# MAGIC     FROM flexcube_cbt_asia_tx t1
# MAGIC     LEFT JOIN flexcube_cbt_asia_account t2 ON t1.creditTransaction_creditAccountNumber = t2.additionalFeatures_clearingAccountNumber AND t1.city = t2.city
# MAGIC     INNER JOIN gcid_to_gcobid t3 ON t2.customerNumber = t3.gcid
# MAGIC     GROUP BY
# MAGIC         t1.city
# MAGIC         , t1.debitTransaction_debitValueDate
# MAGIC         , t1.debitTransaction_debitAmount
# MAGIC         , t1.debitTransaction_debitAccountCurrency
# MAGIC         , t1.additionalSettlement_debitOrCreditIndicator
# MAGIC         , t1.settlement_accountWithInstitutionOneDescription
# MAGIC         , t1.settlement_byOrderOfFiveDescription
# MAGIC         , t1.settlement_byOrderOfFourDescription
# MAGIC         , t2.address_countryCode
# MAGIC         , t3.GcobId
# MAGIC         , t1.contractProductCodeDescription
# MAGIC
# MAGIC
# MAGIC         , t1.settlement_ultimateBeneficiaryOneDescription
# MAGIC         , t1.settlement_ultimateBeneficiaryTwoDescription
# MAGIC         , t1.settlement_ultimateBeneficiaryThreeDescription
# MAGIC         , t1.settlement_ultimateBeneficiaryFourDescription
# MAGIC         , t1.settlement_ultimateBeneficiaryFiveDescription
# MAGIC         , t1.settlement_ultimateBeneficiaryCountryDescription
# MAGIC
# MAGIC         , t1.additionalSettlement_beneficiaryInstitutionOneDescription
# MAGIC         , t1.additionalSettlement_beneficiaryInstitutionTwoDescription
# MAGIC         , t1.additionalSettlement_beneficiaryInstitutionThreeDescription
# MAGIC         , t1.additionalSettlement_beneficiaryInstitutionFourDescription
# MAGIC         , t1.additionalSettlement_beneficiaryInstitutionFiveDescription
# MAGIC
# MAGIC         , t1.settlement_accountWithInstitutionOneDescription
# MAGIC         , t1.settlement_accountWithInstitutionTwoDescription
# MAGIC         , t1.settlement_accountWithInstitutionThreeDescription
# MAGIC         , t1.settlement_accountWithInstitutionFourDescription
# MAGIC         , t1.settlement_accountWithInstitutionFiveDescription
# MAGIC         , t1.settlement_accountWithInstitutionCountryDescription
# MAGIC
# MAGIC         , t1.settlement_byOrderOfOneDescription
# MAGIC         , t1.settlement_byOrderOfTwoDescription
# MAGIC         , t1.settlement_byOrderOfThreeDescription
# MAGIC         , t1.settlement_byOrderOfFourDescription
# MAGIC         , t1.settlement_byOrderOfFiveDescription
# MAGIC         , t1.settlement_byOrderOfCountryDescription
# MAGIC
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     t1.SourceSystem
# MAGIC     , t1.TransactionMonthYear
# MAGIC     , t1.AmountOriginalTransaction
# MAGIC     , t1.TransactionCurrency
# MAGIC     , t1.CreditDebitIndicator
# MAGIC     , t1.CounterpartyCountryCode
# MAGIC     , t1.CounterPartyBankBIC
# MAGIC     , t1.AccountHolderCountryCode
# MAGIC     , t1.GcobId
# MAGIC     , t2.country_name AS CounterpartyCountry
# MAGIC     , t1.TransactionType
# MAGIC     , t1.TransactionsCount
# MAGIC     , CASE
# MAGIC         WHEN t1.TransactionCurrency = 'EUR' THEN t1.AmountOriginalTransaction
# MAGIC         ELSE ROUND(t1.AmountOriginalTransaction / NULLIF(CAST(t3.avg_exchange_rates AS FLOAT), 0), 2)
# MAGIC     END AS AmountEuro
# MAGIC     , t4.Sanctions_Risk AS `SanctionRisk`
# MAGIC     , t4.Money_Laundering_Risk AS `MLRisk`
# MAGIC     , t4.Terrorism_Financing_Risk AS `TFRisk`
# MAGIC     , t4.Tax_Integrity_Risk AS `TaxIntegrityRiskJurisdictions`
# MAGIC     , t4.Corruption_Risk AS `CorruptionRisk`
# MAGIC
# MAGIC FROM CTE t1
# MAGIC LEFT JOIN ebx_static_country_list t2 ON TRIM(t1.CounterpartyCountryCode) = TRIM(t2.ISO_code)
# MAGIC LEFT JOIN monthly_avg_exchange_rates t3 ON t1.TransactionCurrency = TRIM(t3.currency_codes)
# MAGIC LEFT JOIN ebx_static_country_list t4 ON t1.CounterpartyCountryCode = t4.ISO_code

# COMMAND ----------

# DBTITLE 1,append to tx
# check if both the month and the system alreayd exists in tx!!




# # define the month you want to check
# target_month = spark.sql("SELECT DISTINCT TransactionMonthYear FROM tx").collect()[0][0]

# # check if it already exists in radar.tx
# existing_months = spark.sql("SELECT DISTINCT TransactionMonthYear FROM radar.tx") \
#     .rdd.flatMap(lambda x: x).collect()

# # if not present, append the data
# if target_month not in existing_months:
#     spark.sql("SELECT * FROM tx").write.mode('append').saveAsTable('radar.tx')
# else:
#     print(f"TransactionMonthYear {target_month} already exists in radar.tx. Skipping write.")
