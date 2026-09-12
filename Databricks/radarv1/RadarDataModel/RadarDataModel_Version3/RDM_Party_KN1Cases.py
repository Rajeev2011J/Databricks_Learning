# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal
# MAGIC - To create a dataobject to have all information about a CDD Case Dates and status for GIC-KN1
# MAGIC
# MAGIC #### Authors
# MAGIC - Prasad.gadidala@rabobank.com
# MAGIC
# MAGIC ##### Flow of Logic
# MAGIC - Reading KN1 dataobjects from GDP
# MAGIC - Fetching the Cases details of GIC Clients from KN1 Dataobjects
# MAGIC - Combining all the case related information CaseId, Case CreationDate etc.from KN1 sourcesystem
# MAGIC - Loading the final dataobject to SA RADAR Storage account
# MAGIC
# MAGIC #### Note: 
# MAGIC - This notebook has scope of KN1 SourceSystem
# MAGIC
# MAGIC ##### Version & Changes
# MAGIC
# MAGIC |     Developer |Date	   | PBI/Bug No |	Changes done |
# MAGIC |----------|----------|----------|----------|
# MAGIC | Mahalakshmi V | 20-Nov-2025 | 14165735 | Made notebook compatible for Historical Load 
# MAGIC | Gopi Prasad K | 11-Aug-2026 | 16107896 | Party type changes for Legacy2 Source system | 3
# MAGIC | Mahalakshmi V| 21-Aug-2026 |16934371 | Notebook Optimization and Performance Enhancements
# MAGIC    
# MAGIC

# COMMAND ----------

# DBTITLE 1,Importing the required packages
import os
import pandas as pd
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Optimization: Enable Adaptive Query Execution (AQE) and Join Performance Tuning
# Enable Adaptive Query Execution to optimize query plans at runtime
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Dynamically coalesce shuffle partitions to reduce small tasks and improve performance
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Detect and handle data skew automatically to improve join performance
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Increase broadcast join threshold to 100 MB to allow more small tables to be broadcasted instead of shuffled across the cluster
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")

# Configure the default number of shuffle partitions for large transformations
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Enable Dynamic Partition Pruning to reduce data scanned during joins
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

# COMMAND ----------

# DBTITLE 1,Importing modules from RadarUtils file
from RadarUtils import *

# COMMAND ----------

# DBTITLE 1,Read environment variables from cluster
app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']
GIC_ReadStorage = os.environ['GDP_SA_STORAGE_NAME']
environment=os.environ['ENV']
radar_datamodel_version_number=3

# COMMAND ----------

# DBTITLE 1,Defining Load_Date and RunType
dbutils.widgets.text("Load_Date", "")
dbutils.widgets.text("RunType", "daily")  # default is daily

Load_Date = dbutils.widgets.get("Load_Date")
RunType = dbutils.widgets.get("RunType").lower()
if Load_Date:
    RunType="historical"
    
if not Load_Date:
    Load_Date = datetime.today().strftime('%Y%m%d')

print(f"Running for Load Date: {Load_Date}")

# COMMAND ----------

# DBTITLE 1,Defining the dataobject name
party_KN1Cases_dataobject='Party_KN1Cases'

# COMMAND ----------

# DBTITLE 1,Defining the SARADAR Storage account
SARADAR='saradar'+environment

# COMMAND ----------

# DBTITLE 1,Authenticating the Storage Accounts
authenticate_storage_account(ReadStorage)
authenticate_storage_account(GIC_ReadStorage)
authenticate_storage_account(SARADAR)


# COMMAND ----------

# DBTITLE 1,Reading KN1 CaseService Dataobjects from GDP
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
'ADKYC_CONTRAPARTES'
,'ADKYC_CONTRAPARTES_COMPL'
,'ADKYC_CONTRAPARTES_FASES'
,'ADKYC_SITUACOES'
,'ADKYC_RISCOS'
,'ADRBB_CONTRAPARTES_QUESTOES_RESPOSTAS'
,'ADRBB_CONTRAPARTES_PONTUACOES_ABAS'
,'ADKYC_CONTRAPARTES_KYC'
,'ADKYC_CONTRAPARTES_DESATIVADOS'
,'ADRBB_QUESTOES'
,'ADKYC_PROCESSOS'
,'ADRBB_CONTRAPARTES_GRAM'
#,'ADKYC_EVENTOS'
]})

# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='KN1', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Reading GIC Dataobjects from GDP
# print list of strings for loading spark dfs from GDP
load_df = pd.DataFrame({'GDPname':[
# 'pessoa',
'vwgic_rdl_pessoa_tipo_cadastro'
]})
#gic_load_dts
# Create TempView for each loading table
for index, row in load_df.iterrows():
    Read_GDP_Defined_DataObjects(Source='GIC', Dataobject=row.GDPname,Load_Date=Load_Date)

# COMMAND ----------

# DBTITLE 1,Handling Null values from source ADKYC_SITUACAO
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW ADKYC_SITUACOES_UPD AS
# MAGIC     SELECT CD_SITUACAO,
# MAGIC     CASE 
# MAGIC       WHEN CD_SITUACAO=0 THEN "InProgress"
# MAGIC       ELSE NM_SITUACAO
# MAGIC     END AS NM_SITUACAO
# MAGIC     from ADKYC_SITUACOES

# COMMAND ----------

# DBTITLE 1,Create cte language conversion
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW cte AS(
# MAGIC   SELECT 'Novo' AS Portugese, 'Onboarding' AS English UNION ALL
# MAGIC   SELECT 'Renovação', 'Renewal' UNION ALL
# MAGIC   SELECT 'Aprovado', 'Approved' UNION ALL
# MAGIC   SELECT 'Cancelado', 'Cancelled' UNION ALL
# MAGIC   SELECT 'Reprovado', 'Reapproved' UNION ALL
# MAGIC   SELECT 'Reativado', 'Reactivated' UNION ALL
# MAGIC   SELECT 'Ativo', 'Client' UNION ALL
# MAGIC   SELECT 'Bloqueado', 'Blocked' UNION ALL
# MAGIC   SELECT 'Reativação', 'Client Reactivated' UNION ALL
# MAGIC   SELECT 'Inativo', 'Former Client' UNION ALL
# MAGIC   SELECT 'Pré Cadastro', 'Prospect'
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,CasesClientStatus
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW cte_CasesClientStatus AS
# MAGIC SELECT
# MAGIC     COPA.ID_CONTRAPARTE,
# MAGIC     COPA.ID_PROCESSO,
# MAGIC     COPA.CD_SITUACAO,
# MAGIC     COALESCE(cte.English, CPST.NM_SITUACAO) AS NM_SITUACAO_EN,
# MAGIC     CASE WHEN COPA.DTHR_FIM = '1900-01-01 00:00:00.0000000' THEN NULL ELSE COPA.DTHR_FIM END AS DTHR_FIM,
# MAGIC     COPA.DTHR_INICIO,
# MAGIC     CPCO.DE_SIT_CADASTRO,
# MAGIC     COPA.DT_RENOVACAO,
# MAGIC     TRIM(REPLACE(CPCO.CD_CONTRAPARTE, CHAR(160), '')) AS CD_CONTRAPARTE,
# MAGIC     CASE WHEN CPST.NM_SITUACAO = 'Aprovado' THEN 'Y' ELSE 'N' END AS IsLatestApproved
# MAGIC FROM ADKYC_CONTRAPARTES COPA
# MAGIC JOIN ADKYC_CONTRAPARTES_COMPL CPCO
# MAGIC   ON COPA.ID_CONTRAPARTE = CPCO.ID_CONTRAPARTE
# MAGIC LEFT JOIN ADKYC_SITUACOES_UPD CPST
# MAGIC   ON CPST.CD_SITUACAO = COPA.CD_SITUACAO
# MAGIC LEFT JOIN cte cte
# MAGIC   ON CPST.NM_SITUACAO = cte.Portugese
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW cte_PECA AS (
# MAGIC   SELECT
# MAGIC     CAST(COD_INSTITUCIONAL AS STRING) AS COD_INSTITUCIONAL,
# MAGIC     DES_TIPO_CADASTRO,
# MAGIC     DES_STATUS_TIPO_CADASTRO,
# MAGIC     cte.English AS DES_STATUS_TIPO_CADASTRO_EN,
# MAGIC     CASE 
# MAGIC       WHEN DES_STATUS_TIPO_CADASTRO = 'Inativo' THEN DTA_EXCLUSAO 
# MAGIC       ELSE NULL 
# MAGIC     END AS OffboardingDate_PartyRole
# MAGIC
# MAGIC   FROM vwgic_rdl_pessoa_tipo_cadastro AS CADA
# MAGIC   LEFT JOIN cte AS cte
# MAGIC     ON cte.Portugese = CADA.DES_STATUS_TIPO_CADASTRO
# MAGIC   --WHERE DES_TIPO_CADASTRO = 'Cliente'
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW CTE_offboardingdates AS
# MAGIC SELECT 
# MAGIC     CAST(COD_INSTITUCIONAL AS INT) AS CD_contraparte,
# MAGIC     CAST(OffboardingDate_PartyRole AS DATE) AS ofbd
# MAGIC FROM cte_PECA
# MAGIC WHERE OffboardingDate_PartyRole IS NOT NULL
# MAGIC
# MAGIC EXCEPT
# MAGIC
# MAGIC SELECT 
# MAGIC     CD_contraparte,
# MAGIC     CAST(dthr_fim AS DATE) AS ofbd
# MAGIC FROM cte_CasesClientStatus
# MAGIC WHERE CD_contraparte != ''
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define schema
schema = StructType([
    StructField("CD_EVENTO", IntegerType(), True),
    StructField("NM_EVENTO", StringType(), True)
])

# Define data
data = [
    (1, "Renovação"),
    (2, "Edição"),
    (0, "Novo"),
    (3, "Clonagem")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.createOrReplaceTempView("ADKYC_EVENTOS")

# COMMAND ----------

# MAGIC %md
# MAGIC Python code for Cases Logic 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Translation mapping
translation_data = [
    ("Novo", "Onboarding"),
    ("Renovação", "Renewal"),
    ("Aprovado", "Approved"),
    ("Cancelado", "Cancelled"),
    ("Reprovado", "Reapproved"),
    ("Reativado", "Reactivated"),
    ("Ativo", "Client"),
    ("Bloqueado", "Blocked"),
    ("Reativação", "Client Reactivated"),
    ("Inativo", "Former Client"),
    ("Pré Cadastro", "Prospect")
]
cte = spark.createDataFrame(translation_data, ["Portugese", "English"])

# Load and clean base tables
COPA = spark.table("ADKYC_CONTRAPARTES").withColumn(
    "DTHR_FIM",
    F.when(F.col("DTHR_FIM") == "1900-01-01 00:00:00.0000000", None).otherwise(F.col("DTHR_FIM"))
)

CPCO = spark.table("ADKYC_CONTRAPARTES_COMPL") \
    .withColumn("CD_CONTRAPARTE", F.trim(F.translate("CD_CONTRAPARTE", '\u00A0', ''))) \
    .withColumn("CD_CONTRAPARTE", F.col("CD_CONTRAPARTE").cast("int"))

CPST = spark.table("ADKYC_SITUACOES_UPD") \
    .join(cte, F.col("NM_SITUACAO") == F.col("Portugese"), "left") \
    .withColumn("NM_SITUACAO_EN", F.coalesce(F.col("English"), F.col("NM_SITUACAO"))) \
    .select("CD_SITUACAO", "NM_SITUACAO_EN")

# Build cte_CasesClientStatus
cte_CasesClientStatus = COPA.alias("COPA") \
    .join(CPCO.alias("CPCO"), "ID_CONTRAPARTE") \
    .join(CPST.alias("CPST"), "CD_SITUACAO", "left") \
    .select(
        "COPA.ID_CONTRAPARTE", "COPA.ID_PROCESSO", "COPA.CD_SITUACAO",
        "CPST.NM_SITUACAO_EN", "COPA.DTHR_FIM", "COPA.DTHR_INICIO",
        "CPCO.DE_SIT_CADASTRO", "COPA.DT_RENOVACAO", "CPCO.CD_CONTRAPARTE"
    ).filter(
        F.col("CD_CONTRAPARTE").isNotNull()
    )

# Build cte_PECA with valid lifecycle statuses only
PECA = spark.table("VWGIC_RDL_PESSOA_TIPO_CADASTRO")
    # .filter(
    # F.col("des_tipo_cadastro") == "Cliente"
#)

cte_PECA = PECA.join(
    cte, PECA["DES_STATUS_TIPO_CADASTRO"] == cte["Portugese"], "left"
).withColumn(
    "OffboardingDate_PartyRole",
    F.when(F.col("DES_STATUS_TIPO_CADASTRO") == "Inativo", F.col("dta_exclusao"))
).withColumn(
    "DES_STATUS_TIPO_CADASTRO_EN", F.col("English")
).filter(
    F.col("English").isNotNull() & (F.col("English") != "")
).select(
    F.col("COD_INSTITUCIONAL").cast("string").alias("COD_INSTITUCIONAL"),
    "DES_TIPO_CADASTRO", "DES_STATUS_TIPO_CADASTRO",
    "DES_STATUS_TIPO_CADASTRO_EN", "OffboardingDate_PartyRole"
)


# Build CTE_offboardingdates
offboarding = cte_PECA.filter("OffboardingDate_PartyRole IS NOT NULL") \
    .select(
        F.col("COD_INSTITUCIONAL").cast("int").alias("CD_contraparte"),
        F.col("OffboardingDate_PartyRole").cast("date").alias("ofbd")
    )

case_end_dates = cte_CasesClientStatus.select(
    "CD_contraparte", F.col("DTHR_FIM").cast("date").alias("ofbd")
)

CTE_offboardingdates = offboarding.subtract(case_end_dates)


# COMMAND ----------

# DBTITLE 1,Final selection of columns
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

# =====================================================================================
# LOAD TABLES
# =====================================================================================
cte_CasesClientStatus = spark.table("cte_CasesClientStatus")
cte_PECA = spark.table("cte_PECA")
CTE_offboardingdates = spark.table("CTE_offboardingdates")
ADKYC_PROCESSOS = spark.table("ADKYC_PROCESSOS")
ADKYC_EVENTOS = spark.table("ADKYC_EVENTOS")
cte = spark.table("cte")

# =====================================================================================
# PREPARE PECA
# =====================================================================================
peca = (
    cte_PECA.select(
        "COD_INSTITUCIONAL",
        "OffboardingDate_PartyRole",
        "DES_STATUS_TIPO_CADASTRO_EN"
    )
    .cache()
)

# =====================================================================================
# FILTER CASES
# =====================================================================================
filtered_cases = (
    cte_CasesClientStatus.alias("CCS")
    .join(
        peca.alias("OFBD"),
        F.col("CCS.CD_CONTRAPARTE") == F.col("OFBD.COD_INSTITUCIONAL"),
        "left"
    )
    .filter(
        (
            F.to_date("CCS.DTHR_FIM")
            <= F.coalesce(
                F.to_date("OFBD.OffboardingDate_PartyRole"),
                F.lit("2199-12-31").cast("date")
            )
        )
        |
        (
            F.to_date("CCS.DTHR_INICIO")
            <= F.coalesce(
                F.to_date("OFBD.OffboardingDate_PartyRole"),
                F.lit("2199-12-31").cast("date")
            )
        )
    )
    .select(
        "ID_CONTRAPARTE",
        "ID_PROCESSO",
        "CD_SITUACAO",
        "NM_SITUACAO_EN",
        "DTHR_FIM",
        "DTHR_INICIO",
        "DE_SIT_CADASTRO",
        "CD_CONTRAPARTE",
        "DT_RENOVACAO"
    )
)

# =====================================================================================
# OFFBOARDING RECORDS
# =====================================================================================
offboarding_union = (
    CTE_offboardingdates.selectExpr(
        "NULL as ID_CONTRAPARTE",
        "NULL as ID_PROCESSO",
        "NULL as CD_SITUACAO",
        "'Offboarding' as NM_SITUACAO_EN",
        "ofbd as DTHR_FIM",
        "NULL as DTHR_INICIO",
        "NULL as DE_SIT_CADASTRO",
        "CD_contraparte as CD_CONTRAPARTE",
        "NULL as DT_RENOVACAO"
    )
)

# =====================================================================================
# ALL RECORDS
# =====================================================================================
allRecords = (
    filtered_cases
    .unionByName(offboarding_union)
    .persist(StorageLevel.MEMORY_AND_DISK)
)

# =====================================================================================
# RECURSIVE LOGIC (UNCHANGED)
# =====================================================================================
case_end = (
    allRecords
    .select("CD_CONTRAPARTE", "DTHR_FIM")
    .distinct()
    .withColumn("CaseEndDate", F.lit(None).cast("timestamp"))
    .persist(StorageLevel.MEMORY_AND_DISK)
)

max_iterations = 10

for _ in range(max_iterations):

    next_case = (
        allRecords.alias("future")
        .join(
            case_end.alias("current"),
            (
                F.col("future.CD_CONTRAPARTE")
                == F.col("current.CD_CONTRAPARTE")
            )
            &
            (
                F.col("future.DTHR_FIM").cast("timestamp")
                > F.col("current.DTHR_FIM").cast("timestamp")
            ),
            "inner"
        )
        .groupBy(
            F.col("current.CD_CONTRAPARTE").alias("CD_CONTRAPARTE"),
            F.col("current.DTHR_FIM").alias("DTHR_FIM")
        )
        .agg(
            F.min("future.DTHR_FIM").alias("NextEndDate")
        )
    )

    updated = (
        case_end.alias("base")
        .join(
            next_case.alias("n"),
            ["CD_CONTRAPARTE", "DTHR_FIM"],
            "left"
        )
        .select(
            F.col("base.CD_CONTRAPARTE"),
            F.col("base.DTHR_FIM"),
            F.coalesce(
                F.col("base.CaseEndDate"),
                F.col("n.NextEndDate")
            ).alias("CaseEndDate")
        )
    )

    change_count = (
        updated
        .select("CD_CONTRAPARTE", "DTHR_FIM", "CaseEndDate")
        .subtract(
            case_end.select(
                "CD_CONTRAPARTE",
                "DTHR_FIM",
                "CaseEndDate"
            )
        )
        .limit(1)
        .count()
    )

    case_end.unpersist(blocking=False)
    case_end = updated.persist(StorageLevel.MEMORY_AND_DISK)

    if change_count == 0:
        break

# =====================================================================================
# TRANSLATE EVENTS
# =====================================================================================
translated_eventos = (
    ADKYC_EVENTOS.alias("E")
    .join(
        cte.alias("T"),
        F.col("E.NM_EVENTO") == F.col("T.Portugese"),
        "left"
    )
    .select(
        "CD_EVENTO",
        F.coalesce(
            F.col("T.English"),
            F.col("E.NM_EVENTO")
        ).alias("NM_EVENTO_EN")
    )
)

# =====================================================================================
# MAIN JOIN
# =====================================================================================
KN1Cases = (
    allRecords.alias("a")
    .join(
        case_end.alias("b"),
        ["CD_CONTRAPARTE", "DTHR_FIM"],
        "left"
    )
    .join(
        ADKYC_PROCESSOS.alias("PROX"),
        F.col("a.ID_PROCESSO") == F.col("PROX.ID_PROCESSO"),
        "left"
    )
    .join(
        translated_eventos.alias("EVTO"),
        F.col("PROX.CD_EVENTO") == F.col("EVTO.CD_EVENTO"),
        "left"
    )
    .join(
        peca.alias("PECA"),
        F.col("a.CD_CONTRAPARTE") == F.col("PECA.COD_INSTITUCIONAL"),
        "left"
    )
    .withColumn(
        "PartyLifeCycleStatus",
        F.coalesce(
            F.col("PECA.DES_STATUS_TIPO_CADASTRO_EN"),
            F.lit("Unknown")
        )
    )
    .withColumn(
        "PartyLifeCycleStatus2",
        F.when(
            (
                F.col("PECA.DES_STATUS_TIPO_CADASTRO_EN")
                == "Former Client"
            )
            &
            (
                F.to_date("PECA.OffboardingDate_PartyRole")
                > F.to_date("a.DTHR_FIM")
            ),
            "Client"
        ).otherwise(
            F.coalesce(
                F.col("PECA.DES_STATUS_TIPO_CADASTRO_EN"),
                F.lit("Unknown")
            )
        )
    )
    .withColumn(
        "datecomp",
        F.when(
            F.to_date("PECA.OffboardingDate_PartyRole")
            == F.to_date("a.DTHR_FIM"),
            1
        ).otherwise(0)
    )
    .withColumn(
        "daysAfterOff",
        F.when(
            F.to_date("PECA.OffboardingDate_PartyRole")
            < F.to_date("a.DTHR_FIM"),
            F.datediff(
                F.to_date("a.DTHR_FIM"),
                F.to_date("PECA.OffboardingDate_PartyRole")
            )
        )
    )
    .select(
        F.col("a.ID_CONTRAPARTE").alias("CaseID"),
        F.col("a.DE_SIT_CADASTRO"),
        F.col("a.CD_CONTRAPARTE").alias("GicID"),
        F.col("PartyLifeCycleStatus"),
        F.col("PartyLifeCycleStatus2"),
        F.col("a.DTHR_FIM").alias("CaseCompletedDate"),
        F.col("b.CaseEndDate"),
        F.lit(None).cast("timestamp").alias("FinalDecisionDate"),
        F.col("a.DTHR_INICIO").alias("CaseCreationDate"),
        F.col("a.NM_SITUACAO_EN").alias("CaseStatus"),
        F.col("EVTO.NM_EVENTO_EN").alias("ReviewType"),
        F.col("PECA.OffboardingDate_PartyRole"),
        F.col("a.DT_RENOVACAO").alias("NextReviewDate")
    )
    .distinct()
)

# =====================================================================================
# OFFBOARDING FIX
# =====================================================================================
offboarding_cases = (
    KN1Cases
    .filter(F.col("ReviewType") == "Offboarding")
    .withColumn("CaseEndDate", F.col("CaseCompletedDate"))
    .withColumn("PartyLifeCycleStatus", F.lit("Former Client"))
    .withColumn("PartyLifeCycleStatus2", F.lit("Former Client"))
)

non_offboarding_cases = (
    KN1Cases
    .filter(F.col("ReviewType") != "Offboarding")
)

KN1casesfinal_df = (
    non_offboarding_cases
    .unionByName(offboarding_cases)
    .persist(StorageLevel.MEMORY_AND_DISK)
)

# COMMAND ----------

# DBTITLE 1,Get the latest Case Status

# Keep only unique combinations of GICID, CaseCompletedDate, and CaseID
KN1casesfinal_df = KN1casesfinal_df.dropDuplicates(["GICID", "CaseCompletedDate", "CaseID"])

#Define window spec to rank cases by CaseCompletedDate per GICID
window_spec = Window.partitionBy("GICID").orderBy(F.col("CaseCompletedDate").desc())

# Add row number to identify the latest case per GICID
df_with_rn = KN1casesfinal_df.withColumn("rn", F.row_number().over(window_spec))

# Mark IsLatestCase = TRUE for the top-ranked row per GICID
KN1casesfinal_df = df_with_rn.withColumn(
    "IsLatestCase",
    F.when((F.col("rn") == 1) & (F.col("CaseCompletedDate").isNotNull()), F.lit("True")).otherwise(F.lit("False"))
).drop("rn")

KN1casesfinal_df = KN1casesfinal_df.filter(F.col("GicID") != "0")


# COMMAND ----------

# DBTITLE 1,Create Table  for dataframe
KN1casesfinal_df.createOrReplaceTempView("KN1Cases")

# COMMAND ----------

# DBTITLE 1,Creating a dataframe from Temporary view
df_Party_KN1Cases=spark.table('KN1Cases')

# COMMAND ----------

# DBTITLE 1,Optimization: Persist Dataset in Memory and Disk
from pyspark.storagelevel import StorageLevel
df_Party_KN1Cases.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------

# DBTITLE 1,Write Data to SA storage
if RunType == "historical":
    save_to_saradar_storage_account(df_Party_KN1Cases, party_KN1Cases_dataobject, radar_datamodel_version_number, environment,Load_Date)
else:
    save_to_saradar_storage_account(df_Party_KN1Cases, party_KN1Cases_dataobject, radar_datamodel_version_number, environment)

# COMMAND ----------

# DBTITLE 1,Optimization: Release Cached Dataset from Memory and Disk
df_Party_KN1Cases.unpersist()
