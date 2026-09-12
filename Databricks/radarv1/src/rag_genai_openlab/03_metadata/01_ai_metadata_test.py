# Databricks notebook source
# MAGIC %md
# MAGIC # Metadata driven AI solution for answering business ad-hoc requests

# COMMAND ----------

# MAGIC %md
# MAGIC - This PoC will answer ad-hoc request like: "How many onboardings initiated by FOS have been cancelled since 2026?"
# MAGIC - Internal data will not be exposed to the LLM because LLM will generate a SQL query based on metadata information. The SQL retrieval will be executed by a separate internal compute at Rabobank to retrieve actual data.

# COMMAND ----------

# DBTITLE 1,configure pip
# MAGIC %pip config --user set global.index-url https://ZBLD2FIb:o6Ob6S11rfMTmUtoRnM_1FvwAbXat81Kyi53v_B5ooVc@repo.nexuscloud.aws.rabo.cloud/repository/gr-pypi-174/simple

# COMMAND ----------

# DBTITLE 1,import packages
# If running in dev/UAT as a platform engineer, use your personal Nexus credentials
#%pip config --user set global.index-url https://NEXUS_USERNAME:NEXUS_PASSWORD@repo.nexuscloud.aws.rabo.cloud/repository/gr-pypi-13/simple

%pip install openai==1.56.2
%pip install azure-identity==1.19.0

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,define lab environment
LAB_VARIANT = "OpenLab"  # USE "OneLab" IF YOU'RE USING ONELAB

# COMMAND ----------

# DBTITLE 1,define function for AzureOpenAI connect
import os
from openai import AzureOpenAI
from azure.identity import ClientSecretCredential

ENVIRONMENT = "prd"  # choose dev, uat or prd based on environment

def get_openai_urls(lab_variant: str):
    """Returns OpenAI URLs for OpenLab/OneLab. Uses lazy evaluation so OneLab does not affect OpenLab and vice-versa."""
    if lab_variant == "OpenLab":
        secret_scope = f"{lab_variant}-SecretScope"
        return f"https://{dbutils.secrets.get(scope=secret_scope, key='OpenAiHostname')}openoaisdc-completions-apis/"
    elif lab_variant == "OneLab":
        return f"https://apim-1labgen-ap-apizone-{ENVIRONMENT}01.azure-api.net/openaisdc-completions-apis/"
    else:
        raise Exception("Invalid lab_variant")

client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientSecret")
credential = ClientSecretCredential(tenant_id="6e93a626-8aca-4dc1-9191-ce291b4b75a1", client_id=client_id, client_secret=client_secret)
access_token = credential.get_token("https://cognitiveservices.azure.com/.default")

os.environ["AZURE_OPENAI_TOKEN"] = access_token.token
os.environ["AZURE_OPENAI_VERSION"] = "2024-10-21"
os.environ["AZURE_OPENAI_BASE_URL"] = get_openai_urls(LAB_VARIANT)
os.environ["USER_AGENT"] = "myagent"

client = AzureOpenAI(
    api_key=os.environ["AZURE_OPENAI_TOKEN"],
    api_version=os.environ["AZURE_OPENAI_VERSION"],
    azure_endpoint=os.environ["AZURE_OPENAI_BASE_URL"]
)

# COMMAND ----------

# DBTITLE 1,Create cases_dummy
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cases_dummy;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS cases_dummy (
# MAGIC     UniqueGcobId              STRING NOT NULL,
# MAGIC     FullLegalName             STRING NOT NULL,
# MAGIC     CaseReviewType            STRING NOT NULL,
# MAGIC     CaseStatusName            STRING NOT NULL,
# MAGIC     Prework                   DATE,
# MAGIC     GlobalClientOwnerLocation STRING NOT NULL,
# MAGIC     GCOBCaseStatus            STRING NOT NULL,
# MAGIC     PreworkDepartment         STRING NOT NULL
# MAGIC );
# MAGIC
# MAGIC INSERT INTO cases_dummy VALUES
# MAGIC   ('141952', 'Combi Metal Industries Inc.',       'On-Boarding',    'Cancelled',                 '2026-01-15', 'Netherlands', 'Not Completed', 'FOS'),
# MAGIC   ('143321', 'Grundel Granite S.l.',              'On-Boarding',    'Cancelled',                 '2026-02-03', 'Barcelona',   'Not Completed', 'FOS'),
# MAGIC   ('NP_665', 'Thalassa Shipping Ltd.',            'On-Boarding',    'Cancelled',                 '2026-03-10', 'Monaco',      'Not Completed', 'FOS'),
# MAGIC   ('NP_78',  'Orion Capital Partners GmbH',       'On-Boarding',    'Cancelled',                 '2026-04-22', 'Berlin',      'Not Completed', 'FOS'),
# MAGIC   ('150011', 'Lakeview Asset Management AG',      'On-Boarding',    'Cancelled',                 '2026-05-07', 'Zurich',      'Not Completed', 'FOS'),
# MAGIC   ('150022', 'Meridian Trade Solutions B.V.',     'On-Boarding',    'Initiation In Progress',    '2026-01-20', 'Netherlands', 'Not Completed', 'FOS'),
# MAGIC   ('150033', 'Alpine Holdings S.A.',              'On-Boarding',    'KYC assessment in progress','2026-02-14', 'Zurich',      'Not Completed', 'FOS'),
# MAGIC   ('150044', 'Iberian Logistics Corp.',           'On-Boarding',    'Completed',                 '2025-11-05', 'Barcelona',   'Completed',     'FOS'),
# MAGIC   ('150055', 'Nordsee Handel GmbH',               'On-Boarding',    'Cancelled',                 '2025-08-19', 'Berlin',      'Not Completed', 'FOS'),
# MAGIC   ('150066', 'Roma Ventures S.p.A.',              'On-Boarding',    'Sign off requested',        '2026-03-01', 'Rome',        'Not Completed', 'COB NA'),
# MAGIC   ('155001', 'Delta Pharma Holding B.V.',         'Periodic Review','Completed',                 '2025-03-10', 'Netherlands', 'Completed',     'COB NA'),
# MAGIC   ('155002', 'Fortis Real Estate AG',             'Periodic Review','Completed',                 '2024-07-22', 'Zurich',      'Completed',     'KYC SC'),
# MAGIC   ('155003', 'Solaris Energy S.l.',               'Periodic Review','4 eye check in progress',  '2026-01-08', 'Barcelona',   'Not Completed', 'KYC SC'),
# MAGIC   ('155004', 'Maximus Retail Group Ltd.',         'Periodic Review','Approval requested',        '2026-02-28', 'Manchester',  'Not Completed', 'COB NA'),
# MAGIC   ('155005', 'Brenner Automotive GmbH',           'Periodic Review','Cancelled',                 '2025-05-15', 'Berlin',      'Not Completed', 'FOS'),
# MAGIC   ('155006', 'Adriatica Marine S.r.l.',           'Periodic Review','Completed',                 '2023-11-30', 'Rome',        'Completed',     'Rome'),
# MAGIC   ('155007', 'Crown Asset Finance Ltd.',          'Periodic Review','KYC assessment in progress','2026-04-10', 'Manchester',  'Not Completed', 'RANZ'),
# MAGIC   ('155008', 'Helvetia Food Group AG',            'Periodic Review','Completed',                 '2024-09-01', 'Zurich',      'Completed',     'KYC SC'),
# MAGIC   ('NP_201', 'Mistral Wind Energy S.a.s.',        'Amendment',      'Completed',                 '2025-06-18', 'Monaco',      'Completed',     'COB NA'),
# MAGIC   ('NP_202', 'Titan Minerals Corp.',              'Amendment',      'Sign off requested',        '2026-03-25', 'Netherlands', 'Not Completed', 'FOS'),
# MAGIC   ('NP_203', 'Elbe Textiles GmbH',               'Amendment',      'Cancelled',                 '2024-12-05', 'Berlin',      'Not Completed', 'KYC SC'),
# MAGIC   ('NP_204', 'Vivace Music Publishing S.l.',     'Amendment',      'Completed',                 '2023-08-14', 'Barcelona',   'Completed',     'Rome'),
# MAGIC   ('NP_205', 'Albion Tech Ventures Ltd.',         'Amendment',      '4 eye check in progress',  '2026-05-02', 'Manchester',  'Not Completed', 'RANZ'),
# MAGIC   ('160001', 'Riviera Hospitality Group S.A.',   'Event Assessment','Completed',                 '2024-02-20', 'Monaco',      'Completed',     'COB NA'),
# MAGIC   ('160002', 'Piedmont Steel S.p.A.',             'Event Assessment','Approval requested',       '2026-01-30', 'Rome',        'Not Completed', 'Rome'),
# MAGIC   ('160003', 'Clover Dairy Cooperative B.V.',    'Event Assessment','Cancelled',                 '2025-10-11', 'Netherlands', 'Not Completed', 'COB NA'),
# MAGIC   ('160004', 'Zephyr Aviation GmbH',             'Event Assessment','Completed',                 '2023-05-25', 'Berlin',      'Completed',     'KYC SC'),
# MAGIC   ('160005', 'Tyne Bridge Capital Ltd.',          'Event Assessment','KYC assessment in progress','2026-04-15', 'Manchester',  'Not Completed', 'RANZ'),
# MAGIC   ('165001', 'Catalonia Exports S.l.',            'Offboarding',    'Completed',                 '2024-04-03', 'Barcelona',   'Completed',     'KYC SC'),
# MAGIC   ('165002', 'Rhine Valley Logistics GmbH',       'Offboarding',    'Initiation In Progress',    '2026-02-17', 'Berlin',      'Not Completed', 'FOS'),
# MAGIC   ('165003', 'Lido Investments S.r.l.',           'Offboarding',    'Completed',                 '2025-01-09', 'Rome',        'Completed',     'Rome'),
# MAGIC   ('165004', 'Bern Precision Engineering AG',     'Offboarding',    'Sign off requested',        '2026-05-20', 'Zurich',      'Not Completed', 'COB NA'),
# MAGIC   ('165005', 'Northern Ports Authority Ltd.',     'Offboarding',    'Completed',                 '2023-12-01', 'Manchester',  'Completed',     'RANZ'),
# MAGIC   ('170001', 'Pegasus Commodity Trading B.V.',   'On-Boarding',    'Cancelled',                 '2026-01-11', 'Netherlands', 'Not Completed', 'FOS'),
# MAGIC   ('170002', 'Stratos Digital S.l.',             'On-Boarding',    'Completed',                 '2024-06-30', 'Barcelona',   'Completed',     'RANZ'),
# MAGIC   ('170003', 'Veronese Textiles S.p.A.',         'Periodic Review','Cancelled',                 '2026-03-14', 'Rome',        'Not Completed', 'Rome'),
# MAGIC   ('170004', 'Fjord Seafood Exports AS',          'Amendment',      'Completed',                 '2025-09-22', 'Netherlands', 'Completed',     'KYC SC'),
# MAGIC   ('170005', 'Geneva Wealth Partners AG',         'Event Assessment','Approval requested',       '2026-06-01', 'Zurich',      'Not Completed', 'COB NA'),
# MAGIC   ('170006', 'Midlands Manufacturing Ltd.',       'Periodic Review','Completed',                 '2024-11-18', 'Manchester',  'Completed',     'RANZ'),
# MAGIC   ('170007', 'Sirocco Energy S.A.',               'On-Boarding',    'Cancelled',                 '2026-02-25', 'Monaco',      'Not Completed', 'FOS');

# COMMAND ----------

# DBTITLE 1,Creating Metadata for Sample Data
metadata = {
  "tables": [
    {
      "name": "cases_dummy",
      "description": "Contains KYC case review records for clients managed by Rabobank",
      "columns": [
        {
          "name": "UniqueGcobId",
          "type": "STRING",
          "constraints": "NOT NULL",
          "description": "Unique client identifier"
        },
        {
          "name": "FullLegalName",
          "type": "STRING",
          "constraints": "NOT NULL",
          "description": "Full legal name of the client"
        },
        {
          "name": "CaseReviewType",
          "type": "STRING",
          "constraints": "NOT NULL",
          "description": "Type of review the case refers to. Clients can have multiple different reviews in the same year. Possible values: 'Periodic Review', 'Amendment', 'Event Assessment', 'Offboarding', 'On-Boarding'"
        },
        {
          "name": "CaseStatusName",
          "type": "STRING",
          "constraints": "NOT NULL",
          "description": "Current status or step of the case review. Possible values: 'Initiation In Progress', 'Cancelled', 'Sign off requested', '4 eye check in progress', 'Approval requested', 'KYC assessment in progress', 'Completed'"
        },
        {
          "name": "Prework",
          "type": "DATE",
          "constraints": "NULL",
          "description": "Date on which the case review was initiated, ranging from 2023 to present"
        },
        {
          "name": "GlobalClientOwnerLocation",
          "type": "STRING",
          "constraints": "NOT NULL",
          "description": "Location of the client owner. Possible values: 'Netherlands', 'Manchester', 'Barcelona', 'Berlin', 'Monaco', 'Rome', 'Zurich'"
        },
        {
          "name": "GCOBCaseStatus",
          "type": "STRING",
          "constraints": "NOT NULL",
          "description": "Indicates whether the case is ongoing or completed. Possible values: 'Not Completed', 'Completed'"
        },
        {
          "name": "PreworkDepartment",
          "type": "STRING",
          "constraints": "NOT NULL",
          "description": "Department responsible for handling the prework of a case review. Possible values: 'FOS', 'COB NA', 'KYC SC', 'Rome', 'RANZ'"
        }
      ]
    }
  ],
  "relationships": []
}

# COMMAND ----------

import json

def get_sql_from_question(query: str, metadata: dict) -> str:
    """Converts a natural language question into a SQL query using the provided metadata."""
    system_prompt = f"""
You are a data assistant that generates SQL queries.

Rules:
- Only use the tables and columns provided in the metadata
- Do not invent columns or tables
- Always generate valid SQL
- Output only SQL (no explanation)

Metadata: {metadata}
    """
    completion = client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query}
        ],
        seed=1
    )
    return completion.choices[0].message.content


def execute_sql_query(sql_query: str) -> str:
    """Executes a SQL query on Spark and returns results as a JSON string."""
    try:
        df = spark.sql(sql_query)
        display(df)
        return json.dumps([row.asDict() for row in df.collect()])
    except Exception as e:
        print("Error executing SQL query:")
        print(e)
        return None


def get_nl_answer(user_query: str, sql_query: str, query_result: str) -> str:
    """Generates a natural language explanation of the SQL results for the user."""
    instructions = """
      Role: You are a data assistant for Rabobank.

      Your task: Explain database query results clearly and accurately to the end user.

      You will receive:
      1. The original user question
      2. The SQL query that was generated to answer it
      3. The result of executing that SQL query (in JSON format)

      Instructions:
      - Use the SQL result as the SINGLE source of truth
      - Do NOT re-run or reinterpret the SQL query
      - Do NOT invent or assume missing data
      - Explain the results in a clear, simple, and user-friendly way
      - Relate the answer back to the original user question
      - If multiple records exist, summarize them naturally
      - If the result is empty, respond with: "No results found for your request."

      Tone: Professional, helpful, and friendly.
      Output: A short, clear explanation with no technical terms like SQL, JSON, or tables.
    """
    user_input = f"""
User question:
{user_query}

SQL query used:
{sql_query}

Query results:
{query_result}
    """
    completion = client.chat.completions.create(
        model="gpt-5.4",
        messages=[
            {"role": "system", "content": instructions},
            {"role": "user", "content": user_input}
        ],
        seed=1
    )
    return completion.choices[0].message.content


# --- Main flow ---
user_query = input("Please enter your question: ")

print("\nGenerating SQL query...")
sql_query = get_sql_from_question(user_query, metadata)
print(f"\nGenerated SQL:\n{sql_query}")

print("\nExecuting query...")
query_result = execute_sql_query(sql_query)

if query_result is not None:
    print("\nAnswer:")
    answer = get_nl_answer(user_query, sql_query, query_result)
    print(answer)
