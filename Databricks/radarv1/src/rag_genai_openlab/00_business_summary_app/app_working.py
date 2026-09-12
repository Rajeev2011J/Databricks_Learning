import os
import streamlit as st
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.identity import ClientSecretCredential
from openai import AzureOpenAI
from io import BytesIO

# -------------------------
# CONFIGURATION
# -------------------------
LAB_VARIANT = "OpenLab"
PROJECT_NAME = "wrrdr"
ENVIRONMENT = "prd"
DEPLOYMENT_NAME = "gpt-4o"
EXAMPLE_FILE = "example.txt"  # Must be in the same folder as this script

# -------------------------
# AUTHENTICATION
# -------------------------
from databricks.sdk.runtime import dbutils

client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientSecret")
tenant_id = "6e93a626-8aca-4dc1-9191-ce291b4b75a1"

credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

# -------------------------
# FUNCTIONS
# -------------------------
def get_docintelligence_endpoint(lab_variant: str):
    if lab_variant == "OpenLab":
        return f"https://di-plab-md-{PROJECT_NAME}-{ENVIRONMENT}01.cognitiveservices.azure.com/"
    elif lab_variant == "OneLab":
        return f"https://di-1lab-md-{PROJECT_NAME}-{ENVIRONMENT}01.cognitiveservices.azure.com/"
    else:
        raise Exception("Invalid lab_variant")

def get_openai_urls(lab_variant: str):
    if lab_variant == "OpenLab":
        secret_scope = f"{lab_variant}-SecretScope"
        return f"https://{dbutils.secrets.get(scope=secret_scope, key='OpenAiHostname')}openoaisdc-completions-apis/"
    elif lab_variant == "OneLab":
        return f"https://apim-1labgen-ap-apizone-{ENVIRONMENT}01.azure-api.net/openaisdc-completions-apis/"
    else:
        raise Exception("Invalid lab_variant")

def load_example():
    if os.path.exists(EXAMPLE_FILE):
        with open(EXAMPLE_FILE, "r", encoding="utf-8") as f:
            return f.read()
    return "No example format provided."

example_text = load_example()

# -------------------------
# Streamlit UI
# -------------------------
st.title("📄 Business Summary Generator")
client_name = st.text_input("Client Name", placeholder="Enter client name")
uploaded_files = st.file_uploader("Upload Documents (PDF, DOCX, TXT)", type=["pdf", "docx", "txt"], accept_multiple_files=True)

if st.button("Generate Summary"):
    if uploaded_files and client_name.strip():
        # Extract text using Azure Document Intelligence
        endpoint = get_docintelligence_endpoint(LAB_VARIANT)
        doc_client = DocumentIntelligenceClient(endpoint=endpoint, credential=credential)

        all_text = ""
        for file in uploaded_files:
            try:
                content = file.read()
                poller = doc_client.begin_analyze_document("prebuilt-read", BytesIO(content))
                result = poller.result()

                all_text += f"\n\n--- Extracted from: {file.name} ---\n"
                for page in result.pages:
                    for line in page.lines:
                        all_text += line.content + "\n"
            except Exception as e:
                all_text += f"\nError processing {file.name}: {e}\n"

        # Prepare OpenAI client
        os.environ["AZURE_OPENAI_BASE_URL"] = get_openai_urls(LAB_VARIANT)

        access_token = credential.get_token("https://cognitiveservices.azure.com/.default")
        os.environ["AZURE_OPENAI_TOKEN"] = access_token.token
        os.environ["AZURE_OPENAI_VERSION"] = "2024-02-01"

        ai_client = AzureOpenAI(
            api_key=os.environ["AZURE_OPENAI_TOKEN"],
            api_version=os.environ["AZURE_OPENAI_VERSION"],
            azure_endpoint=os.environ["AZURE_OPENAI_BASE_URL"]
        )


        # Generate summary
        response = ai_client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "assistant", "content": f"Here is an example of the desired format:\n\n{example_text}"},
                {"role": "user", "content": f"""
                Based ONLY on the provided text, create a comprehensive business summary.

                Requirements:
                - Make the summary detailed and at least 1000 words.
                - Our client is {client_name}, identify the corresponding main shareholder (the group).
                - Organize the output into clear sections:
                    1. Client Level Business Activities
                    2. Group Level Business Activities
                    3. Geographical Footprint
                - Do NOT add information that is not in the provided text.
                - At the end, include a section called "Sources" listing all and only the names of the files used.

                Text:
                {all_text}
                """}
            ],
            temperature=0.2, 
            max_tokens=3000
        )

        st.subheader(f"Business Summary for {client_name}")
        st.text_area("Summary", response.choices[0].message.content, height=600)
    else:
        st.warning("Please enter a client name and upload at least one document.")
