# Databricks notebook source
# DBTITLE 1,year_month_combinations
# create year month combination starting from start year/month to target

start_year = 2023
start_month = 1

target_year = 2025
target_month = 3

start_date = (datetime(start_year, start_month, 1) - timedelta(days=1)).strftime('%Y-%m-%d')
target_date = (datetime(target_year, target_month, 31) + timedelta(days=1)).strftime('%Y-%m-%d')

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

# ONLY SELECT KYCGROUP 'ACHMEA'

# ONLY NOW FOR Gemeente Amsterdam

# df_wr_iban = spark.sql("""
#     SELECT * 
#     FROM radar.tx_wr_iban 
#     WHERE UniqueGcobId IN (
#         SELECT UniqueGcobId 
#         FROM radar.clients 
#         WHERE KYCGroup = 'ACHMEA'
#     )
# """)

df_wr_iban = spark.sql("""
    SELECT * 
    FROM radar.tx_wr_iban 
    WHERE UniqueGcobId IN (
        SELECT UniqueGcobId 
        FROM radar.clients 
        WHERE UniqueGcobId = '8475' -- Gemeente Amsterdam
    )
""")

# df_wr_iban = spark.sql("""
#     SELECT * 
#     FROM radar.tx_wr_iban 
#     WHERE UniqueGcobId IN (
#         SELECT UniqueGcobId 
#         FROM radar.clients 
#         WHERE UniqueGcobId = '26683' -- Rabo Factoring B.V.
#     )
# """)

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
# MAGIC   SELECT 
# MAGIC     CAST(ENGLS_OMS AS STRING) AS TransactionType
# MAGIC     , CAST(TAR_GRP AS DECIMAL(4,0)) AS TAR_GRP
# MAGIC     , BETALING_IND
# MAGIC     , CONTANT_IND
# MAGIC   FROM EBX_OKTB250
# MAGIC   WHERE 
# MAGIC     (BETALING_IND = 'J' OR CONTANT_IND = 'J')
# MAGIC     AND (END_DAT > '{start_date}' OR END_DAT IS NULL)

# COMMAND ----------

# DBTITLE 1,cna new
from pyspark.sql import functions as F
from pyspark.sql import Window

# these columns can be reduced
columns_to_load = [
    'ACCT_ID', 'ACCT_CCY', 'PRTRY_AMT_CCY', 'TX_ID', 'TX_ACCT_SVCR_REF', 
    'BOOKG_CDT_DBT_IND', 'BOOKG_AMT', 'BOOKG_DT_TM_GMT', 'INPTY_CTRY', 
    'CTPTY_CTRY', 'CTPTY_AGT_BIC', 'CTPTY_ACCT_ID_IBAN', 'CTPTY_ACCT_ID_BBAN',
    'YEAR_MONTH', 'EDL_LOAD_DTS_UTC', 'BTCH_BOOKG', 'DTLD_TX_TP'
]

path_2023 = f'abfss://cna@edlcorestdeuprod0001.dfs.core.windows.net/CAC_ACG_ENTR/0/data/loaddate=2023-*/*.parquet'
path_2024 = f'abfss://cna@edlcorestdeuprod0001.dfs.core.windows.net/CAC_ACG_ENTR/0/data/loaddate=2024-*/*.parquet'
path_2025 = f'abfss://cna@edlcorestdeuprod0001.dfs.core.windows.net/CAC_ACG_ENTR/0/data/loaddate=2025-*/*.parquet'

df_cna_2023 = spark.read.parquet(path_2023).select(*columns_to_load)
df_cna_2024 = spark.read.parquet(path_2024).select(*columns_to_load)
df_cna_2025 = spark.read.parquet(path_2025).select(*columns_to_load)

df_cna = df_cna_2023.unionByName(df_cna_2024).unionByName(df_cna_2025)

# join df_cna with df_wr_iban on ACCT_ID
df_joined = df_cna.join(
    F.broadcast(df_wr_iban),
    df_cna['ACCT_ID'] == df_wr_iban['ar_ac_iban'], 
    'inner'
)

df_joined.createOrReplaceTempView('cna')

# COMMAND ----------

# DBTITLE 1,sepa
from pyspark.sql import functions as F
from pyspark.sql import Window
from functools import reduce

columns_to_load = [
    'PAYMENTTRANSACTIONKEY', 'WHENMODIFIED', 'INCOMINGINSTRUCTIONKEY',
    'SETTLEMENTAMOUNTIDX', 'SETTLEMENTCURRENCYIDX', 'INCOMINGSETTLEMENTCURRENCYIDX',
    'DEBITCREDITINDICATORIDX', 'DEBITPARTYACCOUNTIDX', 'DEBITPARTYAGENTIDIDX',
    'CREDITPARTYACCOUNTIDX', 'CREDITPARTYAGENTIDIDX', 'INSTRUCTEDAMOUNTIDX',
    'TRN_CDTR_PSTLADR_CTRY', 'TRN_DBTR_PSTLADR_CTRY'
]

path_2023 = f'abfss://opf-pex@edlcorestdeuprod0001.dfs.core.windows.net/PAYMENTTRANSACTION/0/data/loaddate=2023-*/*.parquet'
path_2024 = f'abfss://opf-pex@edlcorestdeuprod0001.dfs.core.windows.net/PAYMENTTRANSACTION/0/data/loaddate=2024-*/*.parquet'
path_2025 = f'abfss://opf-pex@edlcorestdeuprod0001.dfs.core.windows.net/PAYMENTTRANSACTION/0/data/loaddate=2025-*/*.parquet'

df_sepa_2023 = spark.read.parquet(path_2023).select(*columns_to_load)
df_sepa_2024 = spark.read.parquet(path_2024).select(*columns_to_load)
df_sepa_2025 = spark.read.parquet(path_2025).select(*columns_to_load)

df_sepa = df_sepa_2023.unionByName(df_sepa_2024).unionByName(df_sepa_2025)

# join df with df_wr_iban on CREDITPARTYACCOUNTIDX
df_joined = df_sepa.join(df_wr_iban, df_sepa['CREDITPARTYACCOUNTIDX'] == df_wr_iban['ar_ac_iban'], 'inner')

# window specification for ranking
window_spec = Window.partitionBy('PAYMENTTRANSACTIONKEY').orderBy(F.col('WHENMODIFIED').desc())

# filter rows with non-null 'CREDITPARTYACCOUNTIDX' and rank them, selecting the top-ranked row
df_joined = df_joined \
    .withColumn('ranking', F.row_number().over(window_spec)) \
    .filter((F.col('CREDITPARTYACCOUNTIDX').isNotNull()) & (F.col('ranking') == 1)) \
    .drop('ranking')

df_joined.createOrReplaceTempView('sepa')

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
# MAGIC   'SEPA' AS SourceSystem
# MAGIC
# MAGIC   , t1.WHENMODIFIED AS TransactionDate
# MAGIC
# MAGIC   , ROUND(COALESCE(CAST(REPLACE(SUBSTRING(t1.SETTLEMENTAMOUNTIDX, 1, 15), ',', '.') AS FLOAT), 0), 2) AS AmountOriginalTransaction
# MAGIC   , COALESCE(TRIM(t1.SETTLEMENTCURRENCYIDX), TRIM(t1.INCOMINGSETTLEMENTCURRENCYIDX)) AS TransactionCurrency
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t1.DEBITCREDITINDICATORIDX = 'D' THEN 'Debit'
# MAGIC       WHEN t1.DEBITCREDITINDICATORIDX = 'C' THEN 'Credit'
# MAGIC       ELSE t1.DEBITCREDITINDICATORIDX
# MAGIC     END AS CreditDebitIndicator
# MAGIC
# MAGIC   , t1.DEBITPARTYACCOUNTIDX AS CounterPartyIBAN
# MAGIC   , t1.TRN_DBTR_PSTLADR_CTRY AS CounterParty
# MAGIC   , t1.DEBITPARTYAGENTIDIDX AS CounterPartyBankBIC
# MAGIC   -- , COALESCE(t1.TRN_CDTR_PSTLADR_CTRY, SUBSTRING(t1.CREDITPARTYACCOUNTIDX, 1, 2)) AS AccountHolderCountryCode
# MAGIC   , t3.UniqueGcobId
# MAGIC   -- , t4.country_name AS CounterpartyCountry
# MAGIC   
# MAGIC   , t1.CREDITPARTYACCOUNTIDX AS AccountNumber
# MAGIC   -- t1.PAYMENTTRANSACTIONKEY AS TransactionID
# MAGIC   -- , t1.INCOMINGINSTRUCTIONKEY
# MAGIC   -- , t1.WHENMODIFIED AS TransactionDate
# MAGIC
# MAGIC   -- , t3.ar_ac_ccy_code -- CURRENCY CODE OF ACCOUNT HOLDER
# MAGIC
# MAGIC FROM sepa t1
# MAGIC INNER JOIN cna_batch t2 ON SUBSTRING(t2.TransactionId, 1, INSTR(t2.TransactionId, ':') - 1) = CAST(t1.INCOMINGINSTRUCTIONKEY AS STRING)
# MAGIC LEFT JOIN radar.tx_wr_iban t3 ON t3.ar_ac_iban = t1.CREDITPARTYACCOUNTIDX
# MAGIC
# MAGIC WHERE SUBSTRING(t1.DEBITPARTYACCOUNTIDX, 1, 2) <> 'NL'
# MAGIC   AND SUBSTRING(t1.DEBITPARTYAGENTIDIDX, 5, 2) <> 'NL'
# MAGIC   AND t1.TRN_DBTR_PSTLADR_CTRY <> 'NL'

# COMMAND ----------

# DBTITLE 1,cna_transactions_non_batch
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cna_transactions_non_batch AS
# MAGIC
# MAGIC WITH cna_non_batch AS (
# MAGIC   SELECT DISTINCT
# MAGIC     COALESCE(TX_ID, TX_ACCT_SVCR_REF) AS TransactionId
# MAGIC     -- , CONCAT(CAST(MONTH(BOOKG_DT_TM_GMT) AS STRING), '/', CAST(YEAR(BOOKG_DT_TM_GMT) AS STRING)) AS TransactionMonthYear
# MAGIC
# MAGIC     -- , date_format(BOOKG_DT_TM_GMT, 'yyyy-MM') AS TransactionMonthYear
# MAGIC     , BOOKG_DT_TM_GMT AS TransactionDate
# MAGIC
# MAGIC     , ROUND(COALESCE(CAST(REPLACE(SUBSTRING(BOOKG_AMT, 1, 15), ',', '.') AS FLOAT), 0), 2) AS AmountOriginalTransaction
# MAGIC     , COALESCE(TRIM(PRTRY_AMT_CCY), TRIM(ACCT_CCY)) AS TransactionCurrency
# MAGIC     , CASE
# MAGIC         WHEN BOOKG_CDT_DBT_IND = 'DBIT' THEN 'Debit'
# MAGIC         WHEN BOOKG_CDT_DBT_IND = 'CRDT' THEN 'Credit'
# MAGIC         ELSE BOOKG_CDT_DBT_IND
# MAGIC       END AS CreditDebitIndicator
# MAGIC     -- , TRIM(COALESCE(SUBSTRING(CTPTY_ACCT_ID_IBAN, 1, 2), SUBSTRING(CTPTY_AGT_BIC, 5, 2), CTPTY_CTRY)) AS CounterpartyCountryCode
# MAGIC
# MAGIC     , CTPTY_CTRY AS CounterParty
# MAGIC     , CTPTY_ACCT_ID_IBAN AS CounterPartyIBAN
# MAGIC
# MAGIC     , CTPTY_AGT_BIC AS CounterPartyBankBIC
# MAGIC     , ACCT_ID
# MAGIC     , DTLD_TX_TP AS DetailledTrxTypeCode
# MAGIC   FROM cna
# MAGIC   WHERE BTCH_BOOKG = FALSE
# MAGIC   AND COALESCE(TX_ID, TX_ACCT_SVCR_REF) IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   'CNA' AS SourceSystem
# MAGIC   , t1.TransactionDate
# MAGIC   -- , SUM(t1.AmountOriginalTransaction) AS AmountOriginalTransaction
# MAGIC   , t1.AmountOriginalTransaction AS AmountOriginalTransaction
# MAGIC   , t1.TransactionCurrency
# MAGIC   , t1.CreditDebitIndicator
# MAGIC   , t1.CounterPartyIBAN
# MAGIC   , t1.CounterParty
# MAGIC   , t1.CounterPartyBankBIC
# MAGIC   , t3.UniqueGcobId
# MAGIC   -- , t2.country_name AS CounterpartyCountry
# MAGIC
# MAGIC   , t1.ACCT_ID AS AccountNumber
# MAGIC
# MAGIC FROM cna_non_batch t1
# MAGIC LEFT JOIN radar.tx_wr_iban t3 ON t1.ACCT_ID = t3.ar_ac_iban
# MAGIC WHERE t1.CounterParty <> 'NL'
# MAGIC   AND SUBSTRING(t1.CounterPartyIBAN, 1, 2) <> 'NL'
# MAGIC   AND SUBSTRING(t1.CounterPartyBankBIC, 5, 2) <> 'NL'
# MAGIC

# COMMAND ----------

# DBTITLE 1,tx
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tx_by_ru AS
# MAGIC
# MAGIC SELECT * FROM sepa_cna_transactions_batch
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT * FROM cna_transactions_non_batch

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.tx_by_ru

# COMMAND ----------

from pyspark.sql.functions import current_date

spark.sql('SELECT * FROM tx_by_ru').write.mode('overwrite').saveAsTable('radar.tx_by_ru')

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   *
# MAGIC from radar.tx_by_ru
# MAGIC where counterparty in ('BY', 'RU')
# MAGIC   or substring(counterpartybankbic, 5, 2) in ('BY', 'RU')
# MAGIC   or substring(counterpartyiban, 1, 2) in ('BY', 'RU')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from radar.tx_by_ru
