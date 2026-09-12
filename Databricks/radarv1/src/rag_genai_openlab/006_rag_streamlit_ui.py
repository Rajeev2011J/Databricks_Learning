# install packages
%pip install azure-identity
%pip install azure-ai-documentintelligence
%pip install openai
%pip install gradio==4.43.
%pip install python-dotenv
%pip install azure-core
%pip install azure-search-documents
%pip install azure-storage-blob
%pip install aiohttp
%pip install ipywidgets
%pip install ipykernel
%pip install mlflow>=3.0 --upgrade

# restart the kernel
dbutils.library.restartPython()

# imports
import streamlit as st
import os
import shutil
from openai import AzureOpenAI
from azure.identity import ClientSecretCredential
import requests
from azure.ai.documentintelligence import DocumentIntelligenceClient

# --- create UI ---
st.title("Business Summary Generator")

client_name = st.text_input("Enter Client Name")
uploaded_files = st.file_uploader("Upload Documents", accept_multiple_files=True, type=["pdf", "docx", "txt"])
generate_button = st.button("Generate Summary")

# --- logic ---
LAB_VARIANT = "OpenLab"
ENVIRONMENT = "prd"

# --- authentication ---
ENVIRONMENT = "prd"
client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientSecret")
tenant_id = "6e93a626-8aca-4dc1-9191-ce291b4b75a1"  
 
credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
 
# --- get_docintelligence_endpoint ---
def get_docintelligence_endpoint(lab_variant: str):
  """This function is created to return Docuement Intelligence End Point so that it can be distinguished for OpenLab and OneLab users."""
  if lab_variant == "OpenLab":
    return f"https://di-plab-md-{project_name}-{ENVIRONMENT}01.cognitiveservices.azure.com/"
  elif lab_variant == "OneLab":
    return f"https://di-1lab-md-{project_name}-{ENVIRONMENT}01.cognitiveservices.azure.com/"
  else: 
    raise Exception("Invalid lab_variant")

# --- get_openai_urls ---
def get_openai_urls(lab_variant: str):
    if lab_variant == "OpenLab":
        secret_scope = f"{lab_variant}-SecretScope"
        return f"https://{dbutils.secrets.get(scope=secret_scope, key='OpenAiHostname')}openoaisdc-completions-apis/"
    elif lab_variant == "OneLab":
        return f"https://apim-1labgen-ap-apizone-{ENVIRONMENT}01.azure-api.net/openaisdc-completions-apis/"
    else:
        raise Exception("Invalid lab_variant")





if generate_button and client_name and uploaded_files:
    st.info("Processing files and generating summary...")

    # --- Initialize Document Intelligence Client ---
    endpoint = get_docintelligence_endpoint(LAB_VARIANT)
    client = DocumentIntelligenceClient(endpoint=endpoint, credential=credential)

    temp_dir = "dbfs:/tmp/temp_files/"

    # --- Ensure temp directory exists ---
    dbutils.fs.mkdirs(temp_dir)


    for file in uploaded_files:
        try:
        # Extract just the filename
        filename = pdf_file.split("/")[-1]

        # Open and analyze
        with open(local_path, "rb") as document:
            poller = client.begin_analyze_document("prebuilt-read", document)
            result = poller.result()

            # Add filename as a header
            all_text += f"\n\n--- Extracted from: {filename} ---\n"

            for page in result.pages:
                for line in page.lines:
                    all_text += line.content + "\n"

        except Exception as e:
            print(f"Error processing {pdf_file}: {e}")



    # Save files to mounted container
    input_folder = "/dbfs/mnt/teamdata/input"
    temp_dir = "dbfs:/tmp/temp_files/"
    os.makedirs(input_folder, exist_ok=True)
    file_names = []
    for file in uploaded_files:
        file_path = os.path.join(input_folder, file.name)
        with open(file_path, "wb") as f:
            f.write(file.read())
        file_names.append(file.name)

    # Extract text using Azure Document Intelligence (placeholder)
    all_text = ""
    for file_name in file_names:
        # Replace with actual extraction logic
        all_text += f"\nExtracted text from {file_name}"













    # Azure OpenAI setup
    client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientId")
    client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientSecret")
    credential = ClientSecretCredential(tenant_id="6e93a626-8aca-4dc1-9191-ce291b4b75a1", client_id=client_id, client_secret=client_secret)
    access_token = credential.get_token("https://cognitiveservices.azure.com/.default")

    client = AzureOpenAI(
        api_key=access_token.token,
        api_version="2024-02-01",
        azure_endpoint=get_openai_urls(LAB_VARIANT)
    )

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": f"""
            Based ONLY on the provided text, create a comprehensive business summary.

            Requirements:
            - At least 1000 words.
            - Client: {client_name}.
            - Sections: Client Level, Group Level, Geographical Footprint.
            - Include Sources: {', '.join(file_names)}.

            Text:
            {all_text}
            """}
        ],
        temperature=0.2,
        max_tokens=3000
    )

    business_summary = response.choices[0].message.content
    st.subheader(f"Business Summary for {client_name}")
    st.write(business_summary)