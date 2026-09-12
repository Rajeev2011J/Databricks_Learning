# Databricks notebook source
# generic imports
import os
import re
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from RiskShieldUtils import *

# COMMAND ----------

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

# DBTITLE 1,Read stoarge
# SALZReadStorage Configuartions
SALZReadStorage = 'salandingzonefecradarprd'

spark.conf.set("fs.azure.account.auth.type."+SALZReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+SALZReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+SALZReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+SALZReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SALZReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token") 

# COMMAND ----------

from pyspark.sql.functions import *

def rename_columns(df):
    newdf=df.select(
        when(col('_c0').contains("0"), 0)
        .otherwise(
            when(col('_c0').contains("1"), 1)
            .otherwise(
                when(col('_c0').contains("9"), 9)
                .otherwise(col('_c0'))
            )).alias('Record_code'),
    col('_c1').alias('Branch_no'),
    col('_c2').alias('Customer_ID'),
    col('_c3').alias('Account_number'),
    col('_c4').alias('Timestamp'),
    col('_c5').alias('Source_code'),
    col('_c6').alias('Transaction_type'),
    col('_c7').alias('Transaction_ID'),
    col('_c8').alias('Original_currency_code'),
    col('_c9').alias('Original_transaction_amount'),
    col('_c10').alias('Credit_debit'),
    col('_c11').alias('Country_currency_code'),
    col('_c12').alias('Country_transaction_amount'),
    col('_c13').alias('Base_currency_code'),
    col('_c14').alias('Base_transaction_amount'),
    col('_c15').alias('Department'),
    col('_c16').alias('Counterparty_bank_name'),
    col('_c17').alias('Counterparty_bank_country_code'),
    col('_c18').alias('Counterparty_account_number'),
    col('_c19').alias('Counterparty_name_For_swift_messages_tag_50A_or_59A'),
    col('_c20').alias('Country_of_ordering_party'),
    col('_c21').alias('Country_of_beneficiary'),
    col('_c22').alias('Item_type'),
    col('_c23').alias('Description_part_1_For_swift_messages_tag_72'),
    col('_c24').alias('Description_part_2'),
    col('_c25').alias('Intermediary_bank_name_1'),
    col('_c26').alias('Intermediary_bank_BIC_1'),
    col('_c27').alias('Intermediary_bank_Address_1'),
    col('_c28').alias('Intermediary_bank_country_code_1'),
    col('_c29').alias('Intermediary_bank_name_2'),
    col('_c30').alias('Intermediary_bank_BIC_2'),
    col('_c31').alias('Intermediary_bank_Address_2'),
    col('_c32').alias('Intermediary_bank_country_code_2'),
    col('_c33').alias('Intermediary_bank_name_3'),
    col('_c34').alias('Intermediary_bank_BIC_3'),
    col('_c35').alias('Intermediary_bank_Address_3'),
    col('_c36').alias('Intermediary_bank_country_code_3'),
    col('_c37').alias('Intermediary_bank_name_4'),
    col('_c38').alias('Intermediary_bank_BIC_4'),
    col('_c39').alias('Intermediary_bank_Address_4'),
    col('_c40').alias('Intermediary_bank_country_code_4'),
    col('_c41').alias('Originator_bank_name'),
    col('_c42').alias('Originator_bank_BIC'),
    col('_c43').alias('Originator_bank_Address'),
    col('_c44').alias('Originator_bank_country_code'),
    col('_c45').alias('Beneficiary_bank_name'),
    col('_c46').alias('Beneficiary_bank_BIC'),
    col('_c47').alias('Beneficiary_bank_Address'),
    col('_c48').alias('Beneficiary_bank_country_code'),
    col('_c49').alias('Whole_Swift_message'),
    col('_c50').alias('Ordering_Bank_Routing_Number'),
    col('_c51').alias('Txn_channel_code'),
    col('_c52').alias('Counterparty_address'),
    col('_c53').alias('Counterparty_country'),
    col('_c54').alias('Our_Customer'),
    col('_c55').alias('Our_Customer_Address'),
    col('_c56').alias('Our_Customer_Country'),
    col('_c57').alias('Issue_Date'),
    col('_c58').alias('Expiry_Date'),
    col('_c59').alias('Description_of_Goods'),
    col('_c60').alias('Goods_Code'),
    col('_c61').alias('Port_of_origin_name'),
    col('_c62').alias('Port_of_origin_country'),
    col('_c63').alias('Port_of_call_name'),
    col('_c64').alias('Port_of_call_country'),
    col('_c65').alias('Port_of_destination_name'),
    col('_c66').alias('Port_of_destination_country'),
    col('_c67').alias('Shipper_Name'),
    col('_c68').alias('Shipper_Country'),
    col('_c69').alias('Insurer_Name'),
    col('_c70').alias('Insurer_Country'),
    col('_c71').alias('Non_Performance_flag'),
    col('_c72').alias('Vessel_Flag_Registration'),
    col('_c73').alias('Presentation_Date'),
    col('_c74').alias('Presentation_Amount'),
    col('_c75').alias('Drawee_Name'),
    col('_c76').alias('Drawee_Address'),
    col('_c77').alias('Drawee_Country'),
    col('_c78').alias('Narrative'),
    col('_c79').alias('Our_Reference'),
    col('_c80').alias('Vessel_Airline'),
    col('_c81').alias('Type'),
    col('_c82').alias('Drawdown_Date'),
    col('_c83').alias('Reimbursing_Bank_Name'),
    col('_c84').alias('Reimbursing_Bank_ID'),
    col('_c85').alias('Reimbursing_Bank_Address'),
    col('_c86').alias('Reimbursing_Bank_country_code'),
    col('_c87').alias('Advise_through_Bank_Name'),
    col('_c88').alias('Advise_through_Bank_ID'),
    col('_c89').alias('Advise_through_Bank_Address'),
    col('_c90').alias('Advise_through_Bank_country_code'),
    col('_c91').alias('Advising_Bank_Name'),
    col('_c92').alias('Advising_Bank_ID'),
    col('_c93').alias('Advising_Bank_Address'),
    col('_c94').alias('Advising_Bank_country_code'),
    col('_c95').alias('Available_with_Bank_Name'),
    col('_c96').alias('Available_with_Bank_ID'),
    col('_c97').alias('Available_with_Bank_Address'),
    col('_c98').alias('Available_with_Bank_country_code'),
    col('_c99').alias('Presenting_Bank_Name'),
    col('_c100').alias('Presenting_Bank_ID'),
    col('_c101').alias('Presenting_Bank_Address'),
    col('_c102').alias('Presenting_Bank_country_code'),
    col('_c103').alias('Intermediary_Bank_Name'),
    col('_c104').alias('Intermediary_Bank_ID'),
    col('_c105').alias('Intermediary_Bank_Address'),
    col('_c106').alias('Intermediary_Bank_country_code'),
    col('_c107').alias('Issuing_Bank_Name'),
    col('_c108').alias('Issuing_Bank_ID'),
    col('_c109').alias('Issuing_Bank_Address'),
    col('_c110').alias('Issuing_Bank_country_code'),
    col('_c111').alias('Collecting_Bank_Name'),
    col('_c112').alias('Collecting_Bank_ID'),
    col('_c113').alias('Collecting_Bank_Address'),
    col('_c114').alias('Collecting_Bank_country_code'),
    col('_c115').alias('Remitting_Bank_Name'),
    col('_c116').alias('Remitting_Bank_ID'),
    col('_c117').alias('Remitting_bank_Address'),
    col('_c118').alias('Remitting_bank_country_code'),
    col('_c119').alias('Trade_Date'),
    col('_c120').alias('Security_ID'),
    col('_c121').alias('Notional'),
    col('_c122').alias('Principal'),
    col('_c123').alias('Interest'),
    col('_c124').alias('Lender_Type'),
    col('_c125').alias('Principal_Balance'),
    col('_c126').alias('Current_Status'),
    col('_c127').alias('Number_of_amendments'),
    col('_c128').alias('TCF_transaction_ID'),
    col('_c129').alias('Principal_write_down'),
    col('_c130').alias('Revolving_limit'),
    col('_c131').alias('Available_amount'),
    col('_c132').alias('Product_group'),
    col('_c133').alias('Booking_type'),
    col('_c134').alias('Advance_type'),
    col('_c135').alias('Structure_code'),
    col('_c136').alias('Closure_type'),
    col('_c137').alias('Early_repayment'),
    col('_c138').alias('Loan_closure_date'),
    col('_c139').alias('Business_date'),
    col('_c140').alias('Value_date_settlement_date'),
    col('_c141').alias('Product_type'),
    col('_c142').alias('Product_name'),
    col('_c143').alias('Security_description'),
    col('_c144').alias('Isin_Cusip'),
    col('_c145').alias('Buy_sell'),
    col('_c146').alias('Net_considerations'),
    col('_c147').alias('Currency'),
    col('_c148').alias('Counterparty_delivery_instructions'),
    col('_c149').alias('Counterparty_bank_instructions'),
    col('_c150').alias('Originating_customer_name'),
    col('_c151').alias('Originating_customer_address'),
    col('_c152').alias('Originating_customer_postal_code'),
    col('_c153').alias('Originating_customer_state'),
    col('_c154').alias('Originating_customer_city'),
    col('_c155').alias('Originating_customer_country'),
    col('_c156').alias('Counterparty_state'),
    col('_c157').alias('Counterparty_postal_code'),
    col('_c158').alias('Counterparty_city'),
    col('_c159').alias('Beneficiary_name'),
    col('_c160').alias('Beneficiary_address'),
    col('_c161').alias('Beneficiary_postal_code'),
    col('_c162').alias('Beneficiary_state'),
    col('_c163').alias('Beneficiary_city'),
    regexp_replace(col("_c164"), "[')]", "").alias('Beneficiary_country'))
    return newdf.select(
    col('Branch_no'),
    col('Customer_ID'),
    col('Account_number'),
    col('Timestamp'),
    col('Source_code'),
    col('Transaction_type'),
    col('Transaction_ID'),
    col('Original_currency_code'),
    col('Original_transaction_amount'),
    col('Credit_debit'),
    col('Country_currency_code'),
    col('Country_transaction_amount'),
    col('Base_currency_code'),
    col('Base_transaction_amount'),
    col('Department'),
    col('Counterparty_bank_name'),
    col('Counterparty_bank_country_code'),
    col('Counterparty_account_number'),
    col('Counterparty_name_For_swift_messages_tag_50A_or_59A'),
    col('Country_of_ordering_party'),
    col('Country_of_beneficiary'),
    col('Item_type'),
    col('Description_part_1_For_swift_messages_tag_72'),
    col('Description_part_2'),
    col('Whole_Swift_message'),
    col('Counterparty_country'),
    col('Our_Customer_Country'),
    col('Reimbursing_Bank_Name'),
    col('Reimbursing_Bank_country_code'),
    col('Advising_Bank_Name'),
    col('Advising_Bank_country_code'),
    col('Issuing_Bank_Name'),
    col('Issuing_Bank_country_code'),
    col('Remitting_Bank_Name'),
    col('Remitting_bank_country_code'),
    col('Trade_Date'),
    col('Current_Status'),
    col('Business_date'),
    col('Currency'),
    col('Originating_customer_name'),
    col('Originating_customer_country'),
    col('Beneficiary_country')
)

# COMMAND ----------

# function that gets schema from deleting x amount of first columns

def ReadRiskshieldCSV(filepaths_palermo_nyw_account_traffic):

 n_skip_rows = 1
 n_skip_rows_from_end = 1

 row_rdd = spark.read.text(filepaths_palermo_nyw_account_traffic).rdd.zipWithIndex()

 count = row_rdd.count()
 print(filepaths_palermo_nyw_account_traffic)
 print(count)
 if count>4:
    row_rdd = row_rdd.filter(lambda row: row[1] >= n_skip_rows) \
       .filter(lambda row: row[1] <= (count - n_skip_rows_from_end - n_skip_rows)) \
          .map(lambda row: row[0])
    palermo_nyw_account_traffic = spark.read.csv(row_rdd)
    df_renamed_palermo_nyw_account_traffic = rename_columns(palermo_nyw_account_traffic)
    return df_renamed_palermo_nyw_account_traffic

# COMMAND ----------

tab = 'PALERMO.NYW.ACCOUNT.TRAFFIC'
tab_name = tab.lower().replace('.', '_')

IF_LoadDate=datetime.today() - timedelta(days=1)
IF_LoadDate=IF_LoadDate.strftime('%Y%m%d')
load_current_dt=datetime.today().strftime('%Y%m%d')
table = tab + '_' + IF_LoadDate

filepaths_nyw_account_traffic = 'abfss://riskshield-nyw@salandingzonefecradarprd.dfs.core.windows.net/'+ table + '*.csv'

try:
    final_df = ReadRiskshieldCSV(filepaths_nyw_account_traffic)
        
except Exception as e:
    if "PATH_NOT_FOUND" in str(e) or "PathNotFound" in str(e):
        final_df=None
        file_exists='No'
        final_df_count=0
    else:
        raise e
            
if final_df is not None :
    final_df = final_df.withColumn("IF_LoadDate", lit(IF_LoadDate))
    final_df_count=final_df.count()
    file_exists='Yes'

    final_df.coalesce(1).write.parquet("abfss://riskshield-nyw@salandingzonefecradarprd.dfs.core.windows.net/" +load_current_dt + '/' + tab + '_' + load_current_dt + '.parquet', mode='overwrite')
        
    save_to_saradar_storage_account(final_df, tab_name, 101, environment , IF_LoadDate)  

#Update logs
log_df=spark.createDataFrame([(tab_name,IF_LoadDate, file_exists, final_df_count)],["File_Name","IF_LoadDate","File_Exist","Record_Count"])   
log_df.write.mode("append").saveAsTable('radar.riskshield_logs')
