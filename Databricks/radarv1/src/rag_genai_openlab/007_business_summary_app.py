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

# define variables
LAB_VARIANT = "OpenLab"  # Use "OneLab" if applicable
project_name = "wrrdr" # if your databricks workspace is named, dbw-plab-md-myproject-prd01, myproject is your project name

# define get_docintelligence_endpoint

import os
import requests
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.identity import ClientSecretCredential
 
# --- Authentication ---
ENVIRONMENT = "prd"
client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientSecret")
tenant_id = "6e93a626-8aca-4dc1-9191-ce291b4b75a1"  
 
credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
 
# --- Endpoint ---
def get_docintelligence_endpoint(lab_variant: str):
  """This function is created to return Docuement Intelligence End Point so that it can be distinguished for OpenLab and OneLab users."""
  if lab_variant == "OpenLab":
    return f"https://di-plab-md-{project_name}-{ENVIRONMENT}01.cognitiveservices.azure.com/"
  elif lab_variant == "OneLab":
    return f"https://di-1lab-md-{project_name}-{ENVIRONMENT}01.cognitiveservices.azure.com/"
  else: 
    raise Exception("Invalid lab_variant")

# --- Initialize Document Intelligence Client ---
endpoint = get_docintelligence_endpoint(LAB_VARIANT)
client = DocumentIntelligenceClient(endpoint=endpoint, credential=credential)

# --- Define PDF folder path ---
pdf_dir = "dbfs:/mnt/teamdata/source_documents/SigmaInvestco/"
temp_dir = "dbfs:/tmp/temp_files/"

# --- Ensure temp directory exists ---
dbutils.fs.mkdirs(temp_dir)

# --- Collect all PDF files ---
pdf_files = [f.path for f in dbutils.fs.ls(pdf_dir) if f.name.endswith(".pdf")]

all_text = ""

for pdf_file in pdf_files:
    try:
        # Extract just the filename
        filename = pdf_file.split("/")[-1]

        # Define a flat temp path
        temp_path = temp_dir + filename

        # Copy file from mount to temp folder
        dbutils.fs.cp(pdf_file, temp_path)

        # Convert to local file system path
        local_path = temp_path.replace("dbfs:", "/dbfs")
        print(f"Processing: {filename}")

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

# remove the temporary files afterward
dbutils.fs.rm("dbfs:/tmp/temp_files/", recurse=True)


import os
import base64
import openai
from openai import AzureOpenAI
import requests
from azure.identity import ClientSecretCredential
from io import BytesIO
from PIL import Image


# define variables
LAB_VARIANT = "OpenLab"  # Use "OneLab" if applicable
project_name = "wrrdr" # if your databricks workspace is named, dbw-plab-md-myproject-prd01, myproject is your project name
 
ENVIRONMENT = "prd" # choose ENVIRONMENT as dev, uat or prd based on environment
def get_openai_urls(lab_variant: str):
  """This function is created to return OpenAI URLs for OpenLab/OneLab. We want to use the function with lazy evaluation so that OneLab doesn't affect OpenLab and vice-versa."""
  if lab_variant == "OpenLab":
    secret_scope = f"{lab_variant}-SecretScope"
    return f"https://{dbutils.secrets.get(scope=secret_scope, key='OpenAiHostname')}openoaisdc-completions-apis/"
  elif lab_variant == "OneLab":
    return f"https://apim-1labgen-ap-apizone-{ENVIRONMENT}01.azure-api.net/openaisdc-completions-apis/"
  else: 
    raise Exception("Invalid lab_variant")

# Client variables
client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope",key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope",key="DataServicePrincipalClientSecret")
credential = ClientSecretCredential(tenant_id="6e93a626-8aca-4dc1-9191-ce291b4b75a1", client_id=client_id, client_secret=client_secret)
access_token = credential.get_token("https://cognitiveservices.azure.com/.default")

os.environ["AZURE_OPENAI_TOKEN"] = access_token.token
os.environ["AZURE_OPENAI_VERSION"] = "2024-02-01"  # https://learn.microsoft.com/en-us/azure/ai-services/openai/reference#api-specs
os.environ["AZURE_OPENAI_BASE_URL"] = get_openai_urls(LAB_VARIANT)

# Client
client = AzureOpenAI(
  api_key=os.environ["AZURE_OPENAI_TOKEN"],  
  api_version=os.environ["AZURE_OPENAI_VERSION"],
  azure_endpoint=os.environ["AZURE_OPENAI_BASE_URL"]
)


deployment_name = "gpt-4o"

client_name = 'Sigma Investco'

response = client.chat.completions.create(
    model=deployment_name,
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        # Provide the example as a reference
        {"role": "assistant", "content": f"Here is an example of the desired format:\n\n{example}"},
        # Main user instruction with explicit length and structure
        {"role": "user", "content": f"""
        Based ONLY on the provided text, create a comprehensive business summary.
        
        Requirements:
        - Follow the style and tone of the example provided above.
        - Make the summary detailed and at least 1000 words.
        - Our client is {client_name}, identify the corresponding main shareholder, which is the group.
        - Organize the output into clear sections:
            1. Client Level Business Activities
            2. Group Level Business Activities
            3. Geographical Footprint (if the information is available, list the countries the Client and group are active in)
        - Do NOT add information that is not in the provided text.
        - At the end, include a section called "Sources" listing the name of the files from which the text was extracted from, only if you use that text to support your summary.

        Text:
        {all_text}
        """}
    ],
    temperature=0.2, # reduce it for more focused
    max_tokens=3000  # Increase this for longer output
)

business_summary = response.choices[0].message.content
displayHTML(f"<h2>Business Summary for {client_name}</h2><p>{business_summary}</p>")