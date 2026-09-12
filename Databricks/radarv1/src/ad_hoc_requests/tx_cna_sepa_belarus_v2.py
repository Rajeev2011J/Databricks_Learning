# Databricks notebook source
# DBTITLE 1,year_month_combinations
# create year month combination starting from start year/month to target

start_year = 2024
start_month = 1

start_date = f'{start_year-1}-12-31'

target_year = 2024
target_month = 12

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

# DBTITLE 1,gcds_keystore
from datetime import datetime, timedelta
import re

gcds_tables = ['client_KeyStoreKey', 'client_Client'] # client_PartyRole

for item in gcds_tables:
    # get the most recent version available in gdp
    path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])

    # get the most recent file available in gdp
    path_file = f'{path}{version}/data/'
    files = dbutils.fs.ls(path_file)
    load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

    spark.read.parquet(f'{path_file}LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView(item)

# COMMAND ----------

# DBTITLE 1,siebel_rel_x_ar to get rel_id and IBAN
# siebel is a delta table, get all valid records
columns_to_load = [
    'ar_ac_iban', 'ar_ac_ccy_code', 'edl_valid_to_dts',
    'ar_del_f', 'rel_id'
]

# manually adding version for siebel, otherwise referencing higher numbers (eg 311224)
version = '3'

df_siebel_rel_x_ar = spark.read.format('delta').load(
    f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_rel_x_ar_hist/{version}/data/'
).select(*columns_to_load).filter("ar_ac_iban IS NOT NULL") # edl_valid_to_dts = '9999-12-31 00:00:00.0000000' AND ar_del_f = 'N' AND --> TAKE ALL THE IBAN, EVEN IF THEY ARE EXPIRED, THEY MIGHT MATCH OLDER TRANSACTIONS

df_siebel_rel_x_ar.createOrReplaceTempView('siebel_rel_x_ar')

# COMMAND ----------

# DBTITLE 1,siebel_org_hist to get bnk_code
# siebel table to get the bnk_code
columns_to_load = [
    'rel_id', 'bnk_code', 'edl_valid_to_dts', 'del_f'
]

# manually adding version for siebel, otherwise referencing higher numbers (eg 241224)
version = '2'

df_siebel_cdf_ggm_org_hist = spark.read.format('delta').load(
    f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/cdf_ggm_org_hist/{version}/data/'
).select(*columns_to_load) # .filter("edl_valid_to_dts = '9999-12-31 00:00:00.0000000' AND ar_del_f = 'N'")

df_siebel_cdf_ggm_org_hist.createOrReplaceTempView('siebel_org_hist')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW wr_iban AS
# MAGIC
# MAGIC -- select all iban and rel_id for bnk_code WR + selecting primary rel_id to an arragement
# MAGIC WITH siebel_scope_cte AS (
# MAGIC   SELECT DISTINCT
# MAGIC     t1.rel_id
# MAGIC     , t1.ar_ac_iban
# MAGIC     -- , t3.ar_prim_org_rel_id
# MAGIC     , t2.bnk_code
# MAGIC
# MAGIC   FROM siebel_rel_x_ar t1
# MAGIC   LEFT JOIN siebel_org_hist t2 ON t1.rel_id = t2.rel_id
# MAGIC   -- LEFT JOIN siebel_ar_hist t3 ON t1.ar_ac_iban = t3.ar_ac_iban
# MAGIC   WHERE 1=1
# MAGIC     -- AND t2.bnk_code IN (3000, 3400, 3508) -- only selecting these bnk_code -- WAIT FOR ROB!!!!!!!!
# MAGIC     AND t1.ar_ac_iban IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC , gcds_keystore_cte AS (
# MAGIC     SELECT DISTINCT
# MAGIC       CAST(keystore_value AS STRING) AS rel_id
# MAGIC       , gcid
# MAGIC     FROM client_keystorekey
# MAGIC     WHERE 1=1
# MAGIC       -- AND (keystore_type = 'SBWRR') -- keystore_type = 'SIEBEL'
# MAGIC       -- AND
# MAGIC       -- (status = 'Active')
# MAGIC       --  OR status IS NULL) -- THEN JOIN ON RADAR.CLIENTS SO DOES NOT MATTER HERE + THERE IS NO OTHER IDENTIFIER SIMILAR TO REL_ID, SO NO NEED TO SPECIFY SIEBEL AS keystore_type
# MAGIC   )
# MAGIC
# MAGIC , siebel_gcob_scope_cte AS (
# MAGIC     SELECT DISTINCT
# MAGIC       t1.ar_ac_iban
# MAGIC       , t2.gcid
# MAGIC       , t3.GcobId AS UniqueGcobId
# MAGIC       , t1.bnk_code
# MAGIC     FROM siebel_scope_cte t1
# MAGIC     INNER JOIN gcds_keystore_cte t2 ON t1.rel_id = t2.rel_id -- GET GCID FROM RELID
# MAGIC     LEFT JOIN (
# MAGIC       SELECT
# MAGIC         gcid
# MAGIC         , keystore_value AS GcobId -- only gcobid for LE on GCDS
# MAGIC       FROM client_keystorekey
# MAGIC       WHERE keystore_type = 'GCOBID'
# MAGIC     ) t3 ON t2.gcid = t3.gcid
# MAGIC     WHERE t3.GcobId IS NOT NULL
# MAGIC   )
# MAGIC
# MAGIC   , iban_deduplication_cte AS (
# MAGIC       SELECT
# MAGIC         ar_ac_iban
# MAGIC         , gcid
# MAGIC         , UniqueGcobId
# MAGIC         , bnk_code
# MAGIC         , RANK() OVER (PARTITION BY ar_ac_iban ORDER BY UniqueGcobId DESC) AS rn --> avoiding duplication of IBAN/GcobId
# MAGIC       FROM siebel_gcob_scope_cte
# MAGIC     )
# MAGIC
# MAGIC SELECT
# MAGIC   t1.ar_ac_iban AS IBAN
# MAGIC   , t1.gcid
# MAGIC   , t1.UniqueGcobId
# MAGIC   , t1.bnk_code
# MAGIC FROM iban_deduplication_cte t1
# MAGIC INNER JOIN radar.clients t2 ON t1.UniqueGcobId = t2.UniqueGcobId
# MAGIC WHERE t1.rn = 1
# MAGIC   AND t2.UniqueGcobId IN ('26683') -- rabo factoring!!!!

# COMMAND ----------

# create df_wr_iban used for joining with cna and sepa to keep only records for which account belongs to wr
df_wr_iban = spark.sql("SELECT * FROM wr_iban")

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
    'YEAR_MONTH', 'EDL_LOAD_DTS_UTC', 'BTCH_BOOKG', 'DTLD_TX_TP'
]

df_cna = None
for year, month in year_month_combinations:
    # get the most recent version available in gdp
    path = f'abfss://cna@edlcorestdeuprod0001.dfs.core.windows.net/CAC_ACG_ENTR/'
    files = dbutils.fs.ls(path)
    version = max([int(re.search(r'/(\d+)/$', file.path).group(1)) for file in files if re.search(r'/(\d+)/$', file.path)])


    path = f'abfss://cna@edlcorestdeuprod0001.dfs.core.windows.net/CAC_ACG_ENTR/{version}/data/loaddate={year}-{month}-*/*.parquet'
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
# MAGIC   -- , date_format(t1.WHENMODIFIED, 'yyyy-MM') AS TransactionMonthYear
# MAGIC   , t1.WHENMODIFIED AS BookingDateTime
# MAGIC   , ROUND(COALESCE(CAST(REPLACE(SUBSTRING(t1.SETTLEMENTAMOUNTIDX, 1, 15), ',', '.') AS FLOAT), 0), 2) AS AmountOriginalTransaction
# MAGIC   , COALESCE(TRIM(t1.SETTLEMENTCURRENCYIDX), TRIM(t1.INCOMINGSETTLEMENTCURRENCYIDX)) AS TransactionCurrency
# MAGIC   , CASE
# MAGIC       WHEN t1.DEBITCREDITINDICATORIDX = 'D' THEN 'Debit'
# MAGIC       WHEN t1.DEBITCREDITINDICATORIDX = 'C' THEN 'Credit'
# MAGIC       ELSE t1.DEBITCREDITINDICATORIDX
# MAGIC     END AS CreditDebitIndicator
# MAGIC   -- , COALESCE(t1.TRN_CDTR_PSTLADR_CTRY, SUBSTRING(t1.CREDITPARTYACCOUNTIDX, 1, 2)) AS AccountHolderCountryCode
# MAGIC   , SUBSTRING(t1.DEBITPARTYACCOUNTIDX, 1, 2) AS CounterpartyCountryIBAN
# MAGIC   , SUBSTRING(t1.DEBITPARTYAGENTIDIDX, 5, 2) AS CounterpartyCountryBIC
# MAGIC   , t1.TRN_DBTR_PSTLADR_CTRY AS CounterpartyCountry
# MAGIC
# MAGIC   -- t1.PAYMENTTRANSACTIONKEY AS TransactionID
# MAGIC   -- , t1.INCOMINGINSTRUCTIONKEY
# MAGIC   -- , t3.ar_ac_ccy_code AS  TransactionCurrency -- CURRENCY CODE OF ACCOUNT HOLDER
# MAGIC   , t2.DetailledTrxTypeCode 
# MAGIC
# MAGIC FROM sepa t1
# MAGIC INNER JOIN cna_batch t2 ON SUBSTRING(t2.TransactionId, 1, INSTR(t2.TransactionId, ':') - 1) = CAST(t1.INCOMINGINSTRUCTIONKEY AS STRING)

# COMMAND ----------

# DBTITLE 1,cna_transactions_non_batch
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cna_transactions_non_batch AS
# MAGIC
# MAGIC WITH cna_non_batch AS (
# MAGIC   SELECT DISTINCT
# MAGIC     COALESCE(TX_ID, TX_ACCT_SVCR_REF) AS TransactionId
# MAGIC     -- , date_format(BOOKG_DT_TM_GMT, 'yyyy-MM') AS TransactionMonthYear
# MAGIC     , BOOKG_DT_TM_GMT AS BookingDateTime
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
# MAGIC     -- , TRIM(COALESCE(SUBSTRING(CTPTY_ACCT_ID_IBAN, 1, 2), SUBSTRING(CTPTY_AGT_BIC, 5, 2), CTPTY_CTRY)) AS CounterpartyCountryCode
# MAGIC     -- , CTPTY_AGT_BIC AS CounterPartyBankBIC
# MAGIC
# MAGIC     , SUBSTRING(CTPTY_ACCT_ID_IBAN, 1, 2) AS CounterpartyCountryIBAN
# MAGIC     , SUBSTRING(CTPTY_AGT_BIC, 5, 2) AS CounterpartyCountryBIC
# MAGIC     , CTPTY_CTRY AS CounterpartyCountry
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
# MAGIC   -- , t1.TransactionMonthYear
# MAGIC   , t1.AmountOriginalTransaction
# MAGIC   , t1.TransactionCurrency
# MAGIC   , t1.CreditDebitIndicator
# MAGIC   , t1.CounterpartyCountryIBAN
# MAGIC   , t1.CounterpartyCountryBIC
# MAGIC   , t1.CounterpartyCountry
# MAGIC   , t1.DetailledTrxTypeCode
# MAGIC   , t1.BookingDateTime
# MAGIC
# MAGIC FROM cna_non_batch t1

# COMMAND ----------

# DBTITLE 1,tx
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tx AS
# MAGIC
# MAGIC SELECT
# MAGIC   'SEPA' AS SourceSystem
# MAGIC   , AccountNumber
# MAGIC   -- , TransactionMonthYear
# MAGIC   , BookingDateTime
# MAGIC   , AmountOriginalTransaction
# MAGIC   , TransactionCurrency
# MAGIC   , CreditDebitIndicator
# MAGIC   , CounterpartyCountryIBAN
# MAGIC   , CounterpartyCountryBIC
# MAGIC   , CounterpartyCountry
# MAGIC   , DetailledTrxTypeCode
# MAGIC FROM sepa_cna_transactions_batch
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT
# MAGIC   'CNA' AS SourceSystem
# MAGIC   , AccountNumber
# MAGIC   -- , TransactionMonthYear
# MAGIC   , BookingDateTime
# MAGIC   , AmountOriginalTransaction
# MAGIC   , TransactionCurrency
# MAGIC   , CreditDebitIndicator
# MAGIC   , CounterpartyCountryIBAN
# MAGIC   , CounterpartyCountryBIC
# MAGIC   , CounterpartyCountry
# MAGIC   , DetailledTrxTypeCode
# MAGIC FROM cna_transactions_non_batch

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists radar.tx_test_factoring

# COMMAND ----------

from pyspark.sql.functions import current_date

spark.sql('select * from tx').write.mode('overwrite').saveAsTable('radar.tx_test_factoring')

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   t1.*
# MAGIC   , t2.TransactionType
# MAGIC   , t3.GCID
# MAGIC   , t3.bnk_code as BankCode
# MAGIC from radar.tx_test_factoring t1
# MAGIC left join ebx_oktb250_filtered t2 on t1.DetailledTrxTypeCode = t2.TAR_GRP
# MAGIC left join wr_iban t3 on t1.AccountNumber = t3.iban
# MAGIC
# MAGIC where CounterpartyCountryBIC in ('RU', 'BY') or CounterpartyCountryIBAN in ('RU', 'BY') or CounterpartyCountry in ('RU', 'BY')
