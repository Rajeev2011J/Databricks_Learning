"""
Streamlit app for the metadata-driven ad-hoc request PoC.

Converts a natural language business question into a SQL query via Azure OpenAI,
executes it against a Databricks SQL Warehouse, and returns a plain-language answer.

Deployed as a Databricks App.

This version uses:
- DefaultAzureCredential / Managed Identity for Azure OpenAI
- Databricks SQL Connector for SQL Warehouse execution
- Environment variables for Databricks SQL Warehouse connection details
"""

import json
import os

import pandas as pd
import streamlit as st
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from databricks import sql as databricks_sql
from databricks.sdk import WorkspaceClient
from openai import AzureOpenAI


# -------------------------
# STREAMLIT CONFIG
# -------------------------
st.set_page_config(
    page_title="Ad-hoc Request Assistant",
    page_icon="📊",
    layout="wide",
)


# -------------------------
# GENERAL CONFIG
# -------------------------
LAB_VARIANT = os.getenv("LAB_VARIANT", "OpenLab")
ENVIRONMENT = os.getenv("ENVIRONMENT", "prd")

AZURE_OPENAI_ENDPOINT = os.getenv(
    "AZURE_OPENAI_ENDPOINT",
    "https://apim-plabgen-pz-apizone-prd01.azure-api.net/openoaisdc-completions-apis"
)

AZURE_OPENAI_API_VERSION = os.getenv(
    "AZURE_OPENAI_API_VERSION",
    "2024-12-01-preview"
)

AZURE_OPENAI_DEPLOYMENT = os.getenv(
    "AZURE_OPENAI_DEPLOYMENT",
    "gpt-5.4"
)


# -------------------------
# DATABRICKS SQL CONFIG
# -------------------------
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

DATABRICKS_HOST = (
    os.getenv("DATABRICKS_SERVER_HOSTNAME")
    or os.getenv("DATABRICKS_HOST")
)

if DATABRICKS_HOST:
    DATABRICKS_HOST = (
        DATABRICKS_HOST
        .replace("https://", "")
        .replace("http://", "")
        .rstrip("/")
    )


# -------------------------
# METADATA
# -------------------------
METADATA = {
    "tables": [
        {
            "name": "cases_dummy",
            "description": "Contains KYC case review records for clients managed by Rabobank",
            "columns": [
                {
                    "name": "UniqueGcobId",
                    "type": "STRING",
                    "constraints": "NOT NULL",
                    "description": "Unique client identifier",
                },
                {
                    "name": "FullLegalName",
                    "type": "STRING",
                    "constraints": "NOT NULL",
                    "description": "Full legal name of the client",
                },
                {
                    "name": "CaseReviewType",
                    "type": "STRING",
                    "constraints": "NOT NULL",
                    "description": (
                        "Type of review the case refers to. Clients can have multiple different "
                        "reviews in the same year. Possible values: 'Periodic Review', 'Amendment', "
                        "'Event Assessment', 'Offboarding', 'On-Boarding'"
                    ),
                },
                {
                    "name": "CaseStatusName",
                    "type": "STRING",
                    "constraints": "NOT NULL",
                    "description": (
                        "Current status or step of the case review. Possible values: "
                        "'Initiation In Progress', 'Cancelled', 'Sign off requested', "
                        "'4 eye check in progress', 'Approval requested', "
                        "'KYC assessment in progress', 'Completed'"
                    ),
                },
                {
                    "name": "Prework",
                    "type": "DATE",
                    "constraints": "NULL",
                    "description": "Date on which the case review was initiated, ranging from 2023 to present",
                },
                {
                    "name": "GlobalClientOwnerLocation",
                    "type": "STRING",
                    "constraints": "NOT NULL",
                    "description": (
                        "Location of the client owner. Possible values: 'Netherlands', 'Manchester', "
                        "'Barcelona', 'Berlin', 'Monaco', 'Rome', 'Zurich'"
                    ),
                },
                {
                    "name": "GCOBCaseStatus",
                    "type": "STRING",
                    "constraints": "NOT NULL",
                    "description": (
                        "Indicates whether the case is ongoing or completed. "
                        "Possible values: 'Not Completed', 'Completed'"
                    ),
                },
                {
                    "name": "PreworkDepartment",
                    "type": "STRING",
                    "constraints": "NOT NULL",
                    "description": (
                        "Department responsible for handling the prework of a case review. "
                        "Possible values: 'FOS', 'COB NA', 'KYC SC', 'Rome', 'RANZ'"
                    ),
                },
            ],
        }
    ],
    "relationships": [],
}


# -------------------------
# CLIENTS
# -------------------------
@st.cache_resource
def get_azure_credential():
    return DefaultAzureCredential()


@st.cache_resource
def get_openai_client() -> AzureOpenAI:
    credential = get_azure_credential()

    token_provider = get_bearer_token_provider(
        credential,
        "https://cognitiveservices.azure.com/.default"
    )

    return AzureOpenAI(
        api_version=AZURE_OPENAI_API_VERSION,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        azure_ad_token_provider=token_provider,
    )


@st.cache_resource
def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


def validate_databricks_sql_config():
    if not DATABRICKS_HOST:
        st.error(
            "Missing Databricks host. Configure either "
            "`DATABRICKS_SERVER_HOSTNAME` or `DATABRICKS_HOST` in the Databricks App environment variables."
        )
        st.stop()

    if not DATABRICKS_HTTP_PATH:
        st.error(
            "Missing `DATABRICKS_HTTP_PATH`. Configure it in the Databricks App environment variables. "
            "You can find it under SQL Warehouse → Connection details → HTTP path."
        )
        st.stop()


def get_sql_connection():
    validate_databricks_sql_config()

    workspace_client = get_workspace_client()

    return databricks_sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        credentials_provider=workspace_client.config.authenticate,
    )


# -------------------------
# LLM FUNCTIONS
# -------------------------
def get_sql_from_question(client: AzureOpenAI, query: str, metadata: dict) -> str:
    """Converts a natural language question into a SQL query using the provided metadata."""

    system_prompt = f"""
You are a data assistant that generates Databricks SQL queries.

Rules:
- Only use the tables and columns provided in the metadata
- Do not invent columns or tables
- Generate only SELECT queries
- Do not generate INSERT, UPDATE, DELETE, MERGE, DROP, ALTER or CREATE statements
- Always generate valid Databricks SQL
- Output only SQL, with no markdown and no explanation

Metadata:
{json.dumps(metadata, indent=2)}
    """

    completion = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query},
        ],
        seed=1,
    )

    return completion.choices[0].message.content.strip()


def is_safe_select_query(sql_query: str) -> bool:
    """Basic safety check to avoid non-read queries."""

    blocked_keywords = [
        "insert",
        "update",
        "delete",
        "merge",
        "drop",
        "alter",
        "create",
        "truncate",
        "grant",
        "revoke",
    ]

    cleaned = sql_query.strip().lower()

    if not cleaned.startswith("select"):
        return False

    return not any(keyword in cleaned for keyword in blocked_keywords)


def execute_sql_query(sql_query: str) -> tuple[pd.DataFrame | None, str | None]:
    """Executes a SQL query against the Databricks SQL Warehouse."""

    if not is_safe_select_query(sql_query):
        st.error("The generated query was blocked because it is not a safe SELECT query.")
        return None, None

    try:
        with get_sql_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                df = cursor.fetchall_arrow().to_pandas()

        result_json = json.dumps(df.to_dict(orient="records"), default=str)
        return df, result_json

    except Exception as exc:
        st.error(f"Error executing SQL query: {exc}")
        return None, None


def get_nl_answer(
    client: AzureOpenAI,
    user_query: str,
    sql_query: str,
    query_result: str,
) -> str:
    """Generates a natural language explanation of the SQL results for the user."""

    instructions = """
Role: You are a data assistant for Rabobank.

Your task:
Explain database query results clearly and accurately to the end user.

You will receive:
1. The original user question
2. The SQL query that was generated to answer it
3. The result of executing that SQL query in JSON format

Instructions:
- Use the SQL result as the single source of truth
- Do not re-run or reinterpret the SQL query
- Do not invent or assume missing data
- Explain the results in a clear, simple, and user-friendly way
- Relate the answer back to the original user question
- If multiple records exist, summarize them naturally
- If the result is empty, respond with: "No results found for your request."
- Avoid technical terms like SQL, JSON, or tables in the final answer

Tone:
Professional, helpful, and friendly.
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
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": instructions},
            {"role": "user", "content": user_input},
        ],
        seed=1,
    )

    return completion.choices[0].message.content.strip()


# -------------------------
# STREAMLIT UI
# -------------------------
st.title("Metadata Driven AI Ad-hoc Request Assistant")

st.caption(
    "Ask a business question in plain English. "
    "The answer is generated from your data, not exposed to the LLM."
)

with st.expander("Available data"):
    st.json(METADATA)

with st.expander("Runtime configuration"):
    st.write("Azure OpenAI endpoint:", AZURE_OPENAI_ENDPOINT)
    st.write("Azure OpenAI API version:", AZURE_OPENAI_API_VERSION)
    st.write("Azure OpenAI deployment:", AZURE_OPENAI_DEPLOYMENT)
    st.write("Databricks host configured:", bool(DATABRICKS_HOST))
    st.write("Databricks HTTP path configured:", bool(DATABRICKS_HTTP_PATH))

user_query = st.text_input(
    "Your question",
    placeholder="e.g. How many onboardings initiated by FOS have been cancelled since 2026?",
)

if st.button("Ask", type="primary") and user_query:
    openai_client = get_openai_client()

    with st.spinner("Generating SQL query..."):
        sql_query = get_sql_from_question(
            openai_client,
            user_query,
            METADATA,
        )

    st.subheader("Generated query")
    st.code(sql_query, language="sql")

    with st.spinner("Executing query..."):
        df, query_result = execute_sql_query(sql_query)

    if df is not None:
        st.subheader("Query results")
        st.dataframe(df, use_container_width=True)

        with st.spinner("Generating answer..."):
            answer = get_nl_answer(
                openai_client,
                user_query,
                sql_query,
                query_result,
            )

        st.subheader("Answer")
        st.write(answer)