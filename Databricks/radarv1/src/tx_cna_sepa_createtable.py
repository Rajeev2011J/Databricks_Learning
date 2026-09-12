# Databricks notebook source
# DBTITLE 1,year_month_combinations
from datetime import datetime, timedelta
# create year month combination starting from start year/month to target
start_year = 2024
start_month = 1

today = datetime.today()

# calculate previous month
if today.month == 1:
    target_month = 12
    target_year = today.year - 1
else:
    target_month = today.month - 1
    target_year = today.year

def create_year_month_combinations(start_year, start_month, target_year, target_month):
    year_month_combinations = []
    for year in range(start_year, target_year + 1):
        start_m = start_month if year == start_year else 1
        end_m = target_month if year == target_year else 12
        for month in range(start_m, end_m + 1):
            year_month_combinations.append((str(year), f'{month:02d}'))
    return year_month_combinations

year_month_combinations = create_year_month_combinations(start_year, start_month, target_year, target_month)

# COMMAND ----------

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

# DBTITLE 1,df_wr_iban
# create df_wr_iban used for joining with cna and sepa to keep only records for which account belongs to wr
df_wr_iban = spark.sql('SELECT * FROM radar.tx_wr_iban')

# COMMAND ----------

# DBTITLE 1,EBX_OKTB250
# https://edlcorestdeuprod0001.dfs.core.windows.net/ebx/EBX_OKTB250/1/data/EDL_LOAD_DT=20240808
# this is the legend of payment types

from datetime import datetime, timedelta

# get the most recent file available in gdp
path = f'abfss://ebx@edlcorestdeuprod0001.dfs.core.windows.net/EBX_OKTB250/1/data/'
files = dbutils.fs.ls(path)
load_date = max(file.path.split('EDL_LOAD_DT=')[1][:8] for file in files if 'EDL_LOAD_DT=' in file.path)

spark.read.parquet(f'{path}EDL_LOAD_DT={load_date}*/*.parquet').createOrReplaceTempView('EBX_OKTB250')

# COMMAND ----------

# DBTITLE 1,ebx_oktb250_filtered
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ebx_oktb250_filtered AS
# MAGIC
# MAGIC SELECT 
# MAGIC   CAST(ENGLS_OMS AS STRING) AS TransactionType
# MAGIC   , CAST(TAR_GRP AS DECIMAL(4,0)) AS TAR_GRP
# MAGIC   , BETALING_IND
# MAGIC   , CONTANT_IND
# MAGIC FROM EBX_OKTB250
# MAGIC WHERE 
# MAGIC   (BETALING_IND = 'J' OR CONTANT_IND = 'J')
# MAGIC   AND (END_DAT > '{start_date}' OR END_DAT IS NULL)

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

# DBTITLE 1,cna
from pyspark.sql import functions as F
from pyspark.sql import Window
import re

# these columns can be reduced
columns_to_load = [
    'ACCT_ID', 'ACCT_CCY', 'PRTRY_AMT_CCY', 'TX_ID', 'TX_ACCT_SVCR_REF', 
    'BOOKG_CDT_DBT_IND', 'BOOKG_AMT', 'BOOKG_DT_TM_GMT', 'INPTY_CTRY', 
    'CTPTY_CTRY', 'CTPTY_AGT_BIC', 'CTPTY_ACCT_ID_IBAN', 'CTPTY_ACCT_ID_BBAN',
    'YEAR_MONTH', 'BTCH_BOOKG', 'DTLD_TX_TP'
]

df_cna = None
for year, month in year_month_combinations:
    # get the most recent version available in gdp
    path = f'abfss://cna@edlcorestdeuprod0001.dfs.core.windows.net/CAC_ACG_ENTR/'
    files = dbutils.fs.ls(path)
    version_new = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])


    if (year == '2024') or (year == '2025' and int(month) <= 6):
        version = 0
        path = f'abfss://cna@edlcorestdeuprod0001.dfs.core.windows.net/CAC_ACG_ENTR/{version}/data/loaddate={year}-{month}-*/*.parquet'
        df_month = spark.read.parquet(path).select(*columns_to_load)
    else:
        version = version_new
        path = f'abfss://cna@edlcorestdeuprod0001.dfs.core.windows.net/CAC_ACG_ENTR/{version}/data/ACT_DT={year}{month}*/*.parquet'
        df_month = spark.read.parquet(path).select(*columns_to_load)
    

    # filter the data based on YEAR_MONTH to be after or equal to 202401
    df_month = df_month.filter(F.col('YEAR_MONTH') >= f'{year_month_combinations[0][0]}{year_month_combinations[0][1]}')
    
    if df_cna is None:
        df_cna = df_month
    else:
        df_cna = df_cna.unionByName(df_month)

# join df_cna with df_wr_iban on ACCT_ID
df_joined = df_cna.join(
    F.broadcast(df_wr_iban),
    df_cna['ACCT_ID'] == df_wr_iban['IBAN'], 
    'inner'
)

# deduplicate df_joined
window_spec = Window.partitionBy(
    'ACCT_ID',
    F.coalesce(F.col('TX_ID'), F.col('TX_ACCT_SVCR_REF')),
    'BOOKG_CDT_DBT_IND',
    F.coalesce(F.col('CTPTY_ACCT_ID_IBAN'), F.col('CTPTY_ACCT_ID_BBAN')),
    'CTPTY_CTRY',
    'BOOKG_DT_TM_GMT'
).orderBy(F.lit(None))

df_deduplicated = df_joined.withColumn('row_num', F.row_number().over(window_spec)).filter(F.col('row_num') == 1).drop('row_num')

df_deduplicated.createOrReplaceTempView('cna')

# COMMAND ----------

# DBTITLE 1,sepa
from pyspark.sql import functions as F
from pyspark.sql import Window
from functools import reduce
import re

columns_to_load = [
    'PAYMENTTRANSACTIONKEY', 'WHENMODIFIED', 'INCOMINGINSTRUCTIONKEY',
    'SETTLEMENTAMOUNTIDX', 'SETTLEMENTCURRENCYIDX', 'INCOMINGSETTLEMENTCURRENCYIDX',
    'DEBITCREDITINDICATORIDX', 'DEBITPARTYACCOUNTIDX', 'DEBITPARTYAGENTIDIDX',
    'CREDITPARTYACCOUNTIDX', 'CREDITPARTYAGENTIDIDX', 'INSTRUCTEDAMOUNTIDX',
    'TRN_CDTR_PSTLADR_CTRY', 'TRN_DBTR_PSTLADR_CTRY'
]

# get the most recent version available in gdp
path = f'abfss://opf-pex@edlcorestdeuprod0001.dfs.core.windows.net/PAYMENTTRANSACTION/'
files = dbutils.fs.ls(path)
version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

df = reduce(lambda df1, df2: df1.union(df2), [
    spark.read.parquet(
        f'{path}{version}/data/loaddate={year}-{month}-*/*.parquet'
    ).select(*columns_to_load) for year, month in year_month_combinations
])

# join df with df_wr_iban on CREDITPARTYACCOUNTIDX
df_joined = df.join(df_wr_iban, df['CREDITPARTYACCOUNTIDX'] == df_wr_iban['IBAN'], 'inner')

# filter the data based on WHENMODIFIED to be after or equal to 202401
df_filtered_sepa = df_joined.filter(F.col('WHENMODIFIED') >= f'{year_month_combinations[0][0]}-{year_month_combinations[0][1]}-01')

# window specification for ranking
window_spec = Window.partitionBy('PAYMENTTRANSACTIONKEY').orderBy(F.col('WHENMODIFIED').desc())

# filter rows with non-null 'CREDITPARTYACCOUNTIDX' and rank them, selecting the top-ranked row
df_filtered_sepa = df_filtered_sepa \
    .withColumn('ranking', F.row_number().over(window_spec)) \
    .filter((F.col('CREDITPARTYACCOUNTIDX').isNotNull()) & (F.col('ranking') == 1)) \
    .drop('ranking')

df_filtered_sepa.createOrReplaceTempView('sepa')

# COMMAND ----------

# DBTITLE 1,monthly_avg_exchange_rates
from pyspark.sql import functions as F

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

# DBTITLE 1,sepa_cna_transactions_batch
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sepa_cna_transactions_batch AS 
# MAGIC
# MAGIC WITH cna_batch AS (
# MAGIC   SELECT DISTINCT
# MAGIC     COALESCE(TX_ID, TX_ACCT_SVCR_REF) AS TransactionId
# MAGIC     , DTLD_TX_TP AS DetailledTrxTypeCode
# MAGIC   FROM cna
# MAGIC   WHERE BTCH_BOOKG = TRUE
# MAGIC   AND COALESCE(TX_ID, TX_ACCT_SVCR_REF) IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   -- 'SEPA' AS SourceSystem
# MAGIC   t1.CREDITPARTYACCOUNTIDX AS AccountNumber
# MAGIC   , date_format(t1.WHENMODIFIED, 'yyyy-MM') AS TransactionMonthYear
# MAGIC   , ROUND(SUM(COALESCE(CAST(REPLACE(SUBSTRING(t1.SETTLEMENTAMOUNTIDX, 1, 15), ',', '.') AS FLOAT), 0)), 2) AS AmountOriginalTransaction
# MAGIC   , COALESCE(TRIM(t1.SETTLEMENTCURRENCYIDX), TRIM(t1.INCOMINGSETTLEMENTCURRENCYIDX)) AS TransactionCurrency
# MAGIC   , CASE
# MAGIC       WHEN t1.DEBITCREDITINDICATORIDX = 'D' THEN 'Debit'
# MAGIC       WHEN t1.DEBITCREDITINDICATORIDX = 'C' THEN 'Credit'
# MAGIC       ELSE t1.DEBITCREDITINDICATORIDX
# MAGIC     END AS CreditDebitIndicator
# MAGIC   , COALESCE(t1.TRN_CDTR_PSTLADR_CTRY, SUBSTRING(t1.CREDITPARTYACCOUNTIDX, 1, 2)) AS AccountHolderCountryCode
# MAGIC   , TRIM(COALESCE(SUBSTRING(t1.DEBITPARTYACCOUNTIDX, 1, 2), SUBSTRING(t1.DEBITPARTYAGENTIDIDX, 5, 2), t1.TRN_DBTR_PSTLADR_CTRY)) AS CounterpartyCountryCode
# MAGIC   , t2.DetailledTrxTypeCode
# MAGIC   , COUNT(*) AS TransactionsCount
# MAGIC
# MAGIC   -- , t1.DEBITPARTYAGENTIDIDX AS CounterPartyBankBIC 
# MAGIC   
# MAGIC   -- , t1.DEBITPARTYACCOUNTIDX AS CounterpartyAccountnumber
# MAGIC   -- t1.PAYMENTTRANSACTIONKEY AS TransactionID
# MAGIC   -- , t1.INCOMINGINSTRUCTIONKEY
# MAGIC   -- , t3.ar_ac_ccy_code -- CURRENCY CODE OF ACCOUNT HOLDER
# MAGIC
# MAGIC FROM sepa t1
# MAGIC INNER JOIN cna_batch t2 ON SUBSTRING(t2.TransactionId, 1, INSTR(t2.TransactionId, ':') - 1) = CAST(t1.INCOMINGINSTRUCTIONKEY AS STRING)
# MAGIC
# MAGIC GROUP BY
# MAGIC   t1.CREDITPARTYACCOUNTIDX
# MAGIC   , t1.WHENMODIFIED
# MAGIC   , t1.SETTLEMENTAMOUNTIDX
# MAGIC   , COALESCE(TRIM(t1.SETTLEMENTCURRENCYIDX), TRIM(t1.INCOMINGSETTLEMENTCURRENCYIDX))
# MAGIC   , t1.DEBITCREDITINDICATORIDX
# MAGIC   , COALESCE(t1.TRN_CDTR_PSTLADR_CTRY, SUBSTRING(t1.CREDITPARTYACCOUNTIDX, 1, 2))
# MAGIC   , TRIM(COALESCE(SUBSTRING(t1.DEBITPARTYACCOUNTIDX, 1, 2), SUBSTRING(t1.DEBITPARTYAGENTIDIDX, 5, 2), t1.TRN_DBTR_PSTLADR_CTRY))
# MAGIC   , t2.DetailledTrxTypeCode

# COMMAND ----------

# DBTITLE 1,cna_transactions_non_batch
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cna_transactions_non_batch AS
# MAGIC
# MAGIC WITH cna_non_batch AS (
# MAGIC   SELECT DISTINCT
# MAGIC     COALESCE(TX_ID, TX_ACCT_SVCR_REF) AS TransactionId
# MAGIC     , date_format(BOOKG_DT_TM_GMT, 'yyyy-MM') AS TransactionMonthYear
# MAGIC
# MAGIC     , ROUND(COALESCE(CAST(REPLACE(SUBSTRING(BOOKG_AMT, 1, 15), ',', '.') AS FLOAT), 0), 2) AS AmountOriginalTransaction
# MAGIC     , COALESCE(TRIM(PRTRY_AMT_CCY), TRIM(ACCT_CCY)) AS TransactionCurrency
# MAGIC
# MAGIC     , CASE
# MAGIC         WHEN BOOKG_CDT_DBT_IND = 'DBIT' THEN 'Debit'
# MAGIC         WHEN BOOKG_CDT_DBT_IND = 'CRDT' THEN 'Credit'
# MAGIC         ELSE BOOKG_CDT_DBT_IND
# MAGIC       END AS CreditDebitIndicator
# MAGIC
# MAGIC     -- , TRIM(COALESCE(CTPTY_CTRY, SUBSTRING(CTPTY_ACCT_ID_IBAN, 1, 2), SUBSTRING(CTPTY_AGT_BIC, 5, 2))) AS CounterpartyCountryCode -- MI team way
# MAGIC     , TRIM(COALESCE(SUBSTRING(CTPTY_ACCT_ID_IBAN, 1, 2), SUBSTRING(CTPTY_AGT_BIC, 5, 2), CTPTY_CTRY)) AS CounterpartyCountryCode
# MAGIC     -- , CTPTY_AGT_BIC AS CounterPartyBankBIC
# MAGIC
# MAGIC     , ACCT_ID
# MAGIC     , DTLD_TX_TP AS DetailledTrxTypeCode
# MAGIC   FROM cna
# MAGIC   WHERE BTCH_BOOKG = FALSE
# MAGIC   AND COALESCE(TX_ID, TX_ACCT_SVCR_REF) IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   t1.ACCT_ID AS AccountNumber
# MAGIC   , t1.TransactionMonthYear
# MAGIC   , SUM(t1.AmountOriginalTransaction) AS AmountOriginalTransaction
# MAGIC   , t1.TransactionCurrency
# MAGIC   , t1.CreditDebitIndicator
# MAGIC   , t1.CounterpartyCountryCode
# MAGIC   , t1.DetailledTrxTypeCode
# MAGIC   , COUNT(*) AS TransactionsCount
# MAGIC
# MAGIC   -- , t1.CounterPartyBankBIC
# MAGIC   -- , COALESCE(t4.TRN_CDTR_PSTLADR_CTRY, SUBSTRING(t4.CREDITPARTYACCOUNTIDX, 1, 2)) AS AccountHolderCountryCode
# MAGIC
# MAGIC FROM cna_non_batch t1
# MAGIC
# MAGIC GROUP BY
# MAGIC   t1.ACCT_ID
# MAGIC   , t1.TransactionMonthYear
# MAGIC   , t1.AmountOriginalTransaction
# MAGIC   , t1.TransactionCurrency
# MAGIC   , t1.CreditDebitIndicator
# MAGIC   , t1.CounterpartyCountryCode
# MAGIC   , t1.DetailledTrxTypeCode

# COMMAND ----------

# DBTITLE 1,tx
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tx AS
# MAGIC
# MAGIC WITH tx_union AS (
# MAGIC   SELECT
# MAGIC     'SEPA' AS SourceSystem
# MAGIC     , AccountNumber
# MAGIC     , TransactionMonthYear
# MAGIC     , AmountOriginalTransaction
# MAGIC     , TransactionCurrency
# MAGIC     , CreditDebitIndicator
# MAGIC     , AccountHolderCountryCode
# MAGIC     , CounterpartyCountryCode
# MAGIC     , DetailledTrxTypeCode
# MAGIC     , TransactionsCount
# MAGIC   
# MAGIC   FROM sepa_cna_transactions_batch
# MAGIC
# MAGIC   UNION
# MAGIC   
# MAGIC   SELECT
# MAGIC     'CNA' AS SourceSystem
# MAGIC     , AccountNumber
# MAGIC     , TransactionMonthYear
# MAGIC     , AmountOriginalTransaction
# MAGIC     , TransactionCurrency
# MAGIC     , CreditDebitIndicator
# MAGIC     , 'NL' AS AccountHolderCountryCode
# MAGIC     , CounterpartyCountryCode
# MAGIC     , DetailledTrxTypeCode
# MAGIC     , TransactionsCount
# MAGIC   FROM cna_transactions_non_batch
# MAGIC )
# MAGIC
# MAGIC , tx_CTE AS (
# MAGIC   SELECT
# MAGIC     SourceSystem
# MAGIC     , t3.UniqueGcobid
# MAGIC     , CAST(TransactionMonthYear AS DATE) AS TransactionMonthYear
# MAGIC     , TransactionCurrency AS OriginalTransactionCurrency
# MAGIC     -- , AmountOriginalTransaction
# MAGIC     , CreditDebitIndicator
# MAGIC     
# MAGIC     , AccountHolderCountryCode
# MAGIC
# MAGIC     , t2.country_name AS CounterpartyCountry
# MAGIC     , t2.Sanctions_Risk AS SanctionRisk
# MAGIC     , t2.Money_Laundering_Risk AS MLRisk
# MAGIC     , t2.Terrorism_Financing_Risk AS TFRisk
# MAGIC     , t2.Tax_Integrity_Risk AS TaxIntegrityRiskJurisdictions
# MAGIC     , t2.Corruption_Risk AS CorruptionRisk
# MAGIC     , TransactionsCount
# MAGIC     -- , t4.TransactionType
# MAGIC     , CASE
# MAGIC         WHEN TransactionCurrency = 'EUR' THEN t1.AmountOriginalTransaction
# MAGIC         ELSE ROUND(t1.AmountOriginalTransaction / NULLIF(CAST(t5.avg_exchange_rate AS FLOAT), 0), 2)
# MAGIC       END AS AmountEuro
# MAGIC
# MAGIC
# MAGIC   FROM tx_union t1
# MAGIC   LEFT JOIN ebx_static_country_list t2 ON t1.CounterpartyCountryCode = t2.ISO_code
# MAGIC     AND t1.TransactionMonthYear = t2.YearMonth
# MAGIC   LEFT JOIN radar.tx_wr_iban t3 ON t1.AccountNumber = t3.IBAN
# MAGIC   LEFT JOIN ebx_oktb250_filtered t4 ON t1.DetailledTrxTypeCode = t4.TAR_GRP
# MAGIC
# MAGIC   LEFT JOIN monthly_avg_exchange_rates t5 ON t1.TransactionCurrency = TRIM(t5.currency_codes)
# MAGIC     AND t1.TransactionMonthYear = t5.YearMonth
# MAGIC     
# MAGIC   WHERE t4.BETALING_IND = 'J' -- CONTANT_IND = 'J' for cash tx
# MAGIC   
# MAGIC   -- WHERE CounterpartyCountry IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   SourceSystem
# MAGIC   , UniqueGcobid
# MAGIC   , TransactionMonthYear
# MAGIC   , CreditDebitIndicator
# MAGIC   , AccountHolderCountryCode
# MAGIC   , CounterpartyCountry
# MAGIC   , SanctionRisk
# MAGIC   , MLRisk
# MAGIC   , TFRisk
# MAGIC   , TaxIntegrityRiskJurisdictions
# MAGIC   , CorruptionRisk
# MAGIC   , OriginalTransactionCurrency
# MAGIC   
# MAGIC   , SUM(TransactionsCount) AS TransactionsCount
# MAGIC   , SUM(AmountEuro) AS AmountEuro
# MAGIC
# MAGIC FROM tx_CTE
# MAGIC GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.tx

# COMMAND ----------

from pyspark.sql.functions import current_date

spark.sql('SELECT * FROM tx').write.mode('overwrite').saveAsTable('radar.tx')
