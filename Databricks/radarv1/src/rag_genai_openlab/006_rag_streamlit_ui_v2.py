# # install packages
# %pip install azure-identity
# %pip install azure-ai-documentintelligence
# %pip install openai
# %pip install gradio==4.43.
# %pip install python-dotenv
# %pip install azure-core
# %pip install azure-search-documents
# %pip install azure-storage-blob
# %pip install aiohttp
# %pip install ipywidgets
# %pip install ipykernel
# %pip install mlflow>=3.0 --upgrade
# %pip install streamlit
# # restart the kernel
# dbutils.library.restartPython()


# --- imports ---
import streamlit as st
import os
from azure.identity import ClientSecretCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient

# --- UI ---
st.title("Business Summary Generator")
client_name = st.text_input("Enter Client Name")
uploaded_files = st.file_uploader("Upload Documents", accept_multiple_files=True, type=["pdf", "docx", "txt"])
generate_button = st.button("Generate Summary")

# --- Config ---
LAB_VARIANT = "OpenLab"
ENVIRONMENT = "prd"
client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientSecret")
tenant_id = "6e93a626-8aca-4dc1-9191-ce291b4b75a1"  

# --- Auth ---
credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

def get_docintelligence_endpoint(lab_variant: str):
    if lab_variant == "OpenLab":
        return f"https://di-plab-md-yourproject-{ENVIRONMENT}01.cognitiveservices.azure.com/"
    elif lab_variant == "OneLab":
        return f"https://di-1lab-md-yourproject-{ENVIRONMENT}01.cognitiveservices.azure.com/"
    else:
        raise Exception("Invalid lab_variant")

if generate_button and client_name and uploaded_files:
    st.info("Processing files and generating summary...")
    endpoint = get_docintelligence_endpoint(LAB_VARIANT)
    client = DocumentIntelligenceClient(endpoint=endpoint, credential=credential)

    all_text = ""

    for file in uploaded_files:
        try:
            # Read file content
            with file as document:
                poller = client.begin_analyze_document("prebuilt-read", document)
                result = poller.result()

                all_text += f"\n\n--- Extracted from: {file.name} ---\n"
                for page in result.pages:
                    for line in page.lines:
                        all_text += line.content + "\n"

        except Exception as e:
            st.error(f"Error processing {file.name}: {e}")

    st.success("Extraction complete!")
    st.text_area("Extracted Text", all_text, height=400)