# Databricks notebook source
# MAGIC %md
# MAGIC # RAG for clients business activities

# COMMAND ----------

dbutils.secrets.list("OpenLab-SecretScope")

# COMMAND ----------

# MAGIC %pip install azure-identity
# MAGIC %pip install azure-ai-documentintelligence
# MAGIC %pip install openai
# MAGIC %pip install gradio==4.43.
# MAGIC %pip install python-dotenv
# MAGIC %pip install azure-core
# MAGIC %pip install azure-search-documents
# MAGIC %pip install azure-storage-blob
# MAGIC %pip install aiohttp
# MAGIC %pip install ipywidgets
# MAGIC %pip install ipykernel
# MAGIC %pip install mlflow>=3.0 --upgrade
# MAGIC
# MAGIC # restart the kernel
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# # Create mounted container to access ADLS (Azure Data Lake Storage)

# # for lab environment teamdata:
# container = 'teamdata'    # lab environment container name
# storage_account = 'dlsplabmdwrrdrprd01' #+ project + env + '01'   #  project storage account

# mount_point = '/mnt/' + container 
# source = 'abfss://' + container + '@' + storage_account + '.dfs.core.windows.net/'

# if mount_point in [mi.mountPoint for mi in dbutils.fs.mounts()]: 
#     dbutils.notebook.exit(f"Skipped, already mounted: {mount_point}")

# dbutils.fs.mount(
#     source = source,
#     mount_point = mount_point,
#     extra_configs = {
#         "fs.azure.account.auth.type": "CustomAccessToken",
#         "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
#     }
# )

# COMMAND ----------

# find folder path
display(dbutils.fs.ls("file:/Workspace/Users/mattia.didone@rabobank.com/R-FEC-RADAR/Databricks/radarv1/src/rag_genai_openlab/source_documents/"))

# COMMAND ----------

# copy from folder to mounted container mnt/teamdata
dbutils.fs.cp("file:/Workspace/Users/mattia.didone@rabobank.com/R-FEC-RADAR/Databricks/radarv1/src/rag_genai_openlab/source_documents/", "dbfs:/mnt/teamdata/source_documents/", recurse=True)

# COMMAND ----------

# see content of mounted container (ADLS Gen2 in Databricks workspace)
dbutils.fs.ls("/mnt/teamdata/source_documents/demo/")

# COMMAND ----------

client_name = 'Renault Finance SA'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Azure Document Intelligence

# COMMAND ----------

# define variables
LAB_VARIANT = "OpenLab"  # Use "OneLab" if applicable
project_name = "wrrdr" # if your databricks workspace is named, dbw-plab-md-myproject-prd01, myproject is your project name

# COMMAND ----------

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

# COMMAND ----------

# --- Initialize Document Intelligence Client ---
endpoint = get_docintelligence_endpoint(LAB_VARIANT)
client = DocumentIntelligenceClient(endpoint=endpoint, credential=credential)

# --- Define PDF folder path ---
pdf_dir = "dbfs:/mnt/teamdata/source_documents/demo/"
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

# --- Output or use the concatenated text ---
print(all_text)

# COMMAND ----------

# remove the temporary files afterward
dbutils.fs.rm("dbfs:/tmp/temp_files/", recurse=True)

# COMMAND ----------

# # remove all from the mounted container!!
# dbutils.fs.rm("/mnt/teamdata/", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define prompt and example

# COMMAND ----------

example = f"""
CLIENT BUSINESS ACTIVITIES

GreenImpact Capital B.V. is a limited liability company domiciled in Rotterdam with the registered office located at Harborweg 25, 3011 AB Rotterdam, Netherlands. The Company was incorporated on 26 January 2024 and filed with the Trade Register at the Chamber of Commerce under number 92790000. According to the CoC, the Client is a financial holding company. Please note that the client does not hold any subsidiaries.
The Client is part of GreenImpact Foundation, incorporated on 11 July 2017 as an impact development fund. Its objective is to prove that financing inclusive, sustainable and deforestation-free commodity production can be commercially viable and replicable, thus strengthening the case for a new rural development paradigm that protects valuable forests and peat lands and promotes high-productivity agriculture.
Source:

GreenImpact Capital B.V. - CoC extract - 20 February 2025
GreenImpact Capital B.V. - Articles of Incorporation - 26 January 2024
GreenImpact Capital B.V. - Statutes Amendment - 31 October 2024
GreenImpact Foundation - Website about us - 19 February 2025
GreenImpact Foundation - Website invest - 19 February 2025
GreenImpact Foundation - Audited Annual Report 2023 - 09 January 2025 P.3 & P.15


PURPOSE OF THE BV:
According to the tax memo, the purpose of the BVs is to raise finance from the private sector through the issue of debt instruments, while the GreenImpact Foundation will continue to raise finance through concessional loans, grants and contributions, primarily from public sector contributions.
The GreenImpact Foundation and Sail Advisors have agreed to establish the Fund in order to catalyse further finance from the private and public sector to scale up the impact objectives of the GreenImpact Foundation. The Foundation shall sell and transfer the part of the portfolio located in Green Climate Fund (GCF) Mandate Countries to the client. As a result, all rights and obligations from the Foundation vis-à-vis its current borrowers shall transfer to the relevant BV and will no longer be a party to the arrangements with the borrowers. To ensure compliance with GCF's requirement, the investment objective of the fund will be restricted to making investments in GCF Mandate Countries only. GCF Mandate Countries means any of Brazil, Cameroon, Colombia, Côte d’Ivoire, Democratic Republic of the Congo, Ecuador, Gabon, Indonesia, Laos, Liberia and Zambia, as well as any other country added hereto in accordance with the policies and procedures of the Green Climate Fund. (Pg 8, 27)
The Foundation will enter into a Subscription Facility Agreement (SFA) with the BV, whereby it agrees to make funds available as capital to provide loans. USD 180 million has been committed of which USD 147 million has been provided upfront.
During the current outreach, the client has confirmed that there are no changes in the activities of the client since the last review and there are no private investors involved yet. GreenImpact currently has USD 400 million in capital committed through grants, redeemable grants and loans from its contributors. GreenImpact is actively raising capital from different investors such as Nordic Climate Fund Initiative (NICFI), FutureGrowth Bank (FGB), Unilever, UN Environment Programme, Green Climate Fund, etc., including USD 51 million Financing Agreement from Central African Forest Initiative (CAFI) in 2024.
Source:

GreenImpact Foundation - Tax and legal memo - 20 February 2024 (Page 14)
GreenImpact Capital B.V. - Shareholders agreement - 21 February 2024 (Pdf Pg 4, 8, 27, 6, 37)
GreenImpact Capital B.V. - Articles of Incorporation - 26 January 2024 (Pdf Pg:26)
GreenImpact Capital B.V. - Statutes Amendment - 31 October 2024
GreenImpact Capital B.V. - CoC extract - 20 February 2025
GreenImpact Foundation - Audited Annual Report 2023 - 09 January 2025 P.3 & P.15
GreenImpact Foundation - Document request completed - 05 February 2025
GreenImpact Foundation - Document request & client answers email trail - 05 February 2025
GreenImpact Foundation - Company Website activity - 17 February 2025
GreenImpact Foundation - Company Website Investors - 17 February 2025


-----------------------------------------------
Geographical Exposure:
-------------------------------------------------
Please note that the client or the group itself has no activities in any high-risk sanctioned countries, which is also confirmed by the client via the sanctions questionnaire.
The client has incoming/outgoing payments with EU High-Risk Third Countries. The incoming/outgoing payments are with the following countries: Netherlands, Emerald Emirates (EU High-Risk Third Country), Luxembourg, Indoria, and Pacora (EU High-Risk Third Country).
(Reference: GreenImpact Capital B.V. - Transaction History - 08 January 2025.pdf)
The client has a current account with RiverBank for providing facilities/loans with the end-borrower, which is incorporated in Indoria, Bravaria, and Ivory Shores.
According to the Shareholders Agreement, GreenImpact Capital B.V. will acquire loan facility and guarantee agreements with the following companies and jurisdictions where GreenImpact Foundation was the original lender:

PT Dharma Sustainable Agro Ltd. (DSA) with total GreenImpact investment of USD 30 million for a 10-year loan term from April 2020 in Indoria.
PT Hilton Agro Lestari (HAL) with total GreenImpact investment of USD 12 million for an 8-year loan term from March 2022 in Indoria.
Agropecuaria Bambusa S.A.S. (ABS) with total GreenImpact investment of COP 300 million for a 12-year loan term from December 2021 in Bravaria.
Marfrig Global Foods S.A. (Marfrig) with total GreenImpact investment of USD 30 million for a 10-year loan term from January 2021 in Bravaria.
FS Lux Holdings S.A.R.L. with total GreenImpact investment of USD 30 million for an 8-year tenor loan term from May 2022 in Bravaria.
ETC Group (ETG) with total GreenImpact investment of USD 30 million for an 8-year tenor from December 2023 in Ivory Shores.
Agropecuaria Roncador Ltda. (Roncador) with GreenImpact investment of USD 10 million for an 8-year tenor from April 2020 in Bravaria.


In addition, please note that the client is part of GreenImpact Foundation, which acts as an impact development fund. The client has also received funds from GreenImpact Foundation, which has transactions with the following high-risk jurisdictions: Netherlands, United States, Bravaria, Indoria, Germania, Luxora, Columbara, Emerald Emirates (HIGH FATF), Francoria, Bahamora, Albion, Helvetia, Southland (HIGH FATF and/or EU High-Risk Third Countries), Pacora (HIGH FATF and/or EU High-Risk Third Countries), Hungaria, Vietoria (HIGH FATF), Cayman Isles, Daneburg, Mauritia, New Albion, Francoria, Lithoria, Australica, and Singapura.
(Reference: GreenImpact Foundation - Transaction History - 08 January 2025.pdf)
As per the client’s website, GreenImpact Advisory Board has approved the following jurisdictions for investment by GreenImpact Foundation, which include Bravaria, Equatoria, Ivory Shores, Gabora, Congo Republic, Columbara, Indoria, Laosia, Vietoria, and Zambria.

Therefore, based on the above information, GreenImpact Foundation has transactions/activities/investments in the following high-risk jurisdictions:

High TF Risk: Columbara, Vietoria, Southland, Pacora
EU Non-Cooperative Jurisdictions: Bahamora, Pacora
Corruption Risk: Equatoria, Gabora, Laosia, Zambria
EU High-Risk Third Country: Vietoria, Southland, Pacora, Emerald Emirates
High FATF Risk: Vietoria, Southland
Tax Integrity Jurisdictions: Bahamora, Pacora
Totalitarian Regimes: Laosia
High ML Risk: Columbara, Gabora, Laosia, Liberia, Vietoria, Southland, Pacora
Medium Sanctioned: Congo Republic


CONCLUSION: As there is fund transfer from GreenImpact Foundation to our client, there is commingling of funds. Therefore, the above geo-risk is also applicable to the client and accordingly geo-risks have been triggered in the file and assessed under EDD follow-up tabs. Please check follow-up tabs for detailed mitigating factors.
Source:

GreenImpact Capital B.V. - Shareholders Agreement - 21 February 2024 (Pg 98)
GreenImpact Fund - Portfolio - 20 February 2025
GreenImpact Foundation - Signed Organisation Chart - Nov 2024
GreenImpact Fund - Client Answers - 07 March 2024
GreenImpact Foundation - CoC Extract - 12 February 2025
GreenImpact Fund - Website About Us - 19 February 2025
GreenImpact Fund - Approved Jurisdiction - 09 January 2025
GreenImpact Foundation - Transaction History - 08 January 2025
GreenImpact Foundation - Completed Sanctions Questionnaire - 05 February 2025
GreenImpact Fund - Document Request & Client Answers Email Trail - 05 February 2025
GreenImpact Foundation - Audited Annual Report 2023 - 09 January 2025 P.3 & P.15            
 
MLRO Consultation/  Client Committee(CC) approval ; 
------------------------------------------------------------------------------------------
During the onboarding of GreenImpact Fund Vehicle B.V. and GreenImpact Capital B.V., the file was submitted to the client committee due to ECHR3C nexus and was approved. Furthermore, the MLRO was consulted due to nexus with ECHR3C, and they had no objections as no unacceptable risks were identified.
However, GreenImpact Foundation was transferred from FI’s to W&R in 2025, and this risk was not covered earlier at the Foundation level. In addition, as the other two BVs have also received funds from the Foundation, and this Foundation has more ECHR3Cs, these risks are also applicable to the other BVs due to fund transfers. All files have an overall high-risk rating; hence, considering recent changes to the ECHR3C risk framework, CC approval was required.
During the current review (2025), the file was submitted to the client committee for further approval and was approved as is. The MLRO was also consulted again and had no objections.

Sources:

GreenImpact Funds - Client Committee Minutes - 28 June 2024
CC Memo - GreenImpact Fund Vehicle and GreenImpact Fund V.1
GreenImpact Foundation - Client Committee Memo - 10 March 2025
GreenImpact Foundation Group - Client Committee Decision Email Trail - 22 May 2025
GreenImpact Foundation Group - Client Committee Minutes - 22 May 2025
GreenImpact Fund - MLRO Consultation SMSO ECHRTC - 16 May 2025
GreenImpact Fund - MLRO Consultation Email Trail - 16 May 2025
 

Financial connections;
---------------------------------------------
We see credits only from the group company (GreenImpact Foundation), and from business partners/investors which are also active in the same industry as the client. Therefore, accordingly the sector risk has been triggered. The NAICS codes as well as sector risks were triggered based on the actual business activities of the client, Chamber of Commerce extract and Articles of Association.

GROUP ACTIVITIES AND GEOGRAPHICAL FOOTPRINTS: (FOUNDATION)
GreenImpact is a Foundation (“Stichting”), established in Netherlands in 2017. Its governance structure is designed to safeguard the environmental and social return as well as the financial and commercial sustainability of the Fund and its investments.
The forests and peatlands under the greatest threat from current agricultural expansion and unsustainable practices are located in the tropical regions of Latin America, Africa, and South East Asia. GreenImpact concentrates its efforts on exactly these geographic locations. In these regions, GreenImpact only works in countries or states where the local authorities are committed to tackling land conversion as well.
The target sectors of GreenImpact are the commodities that are driving deforestation – such as palm oil, soy, beef, forestry, and others – in tropical forest jurisdictions with progressive forest protection strategies and targets in place. In this way, GreenImpact can demonstrate the decoupling of commodity production from deforestation while being socially inclusive.
According to the Annual Report, GreenImpact Fund selects jurisdictions with a regulatory framework to safeguard forest protection achievements associated with GreenImpact’s investments.
Group Geography: Please refer above under Geographical Exposure section.
Source:

GreenImpact Foundation - Audited Annual Report 2023 - 09 January 2025
GreenImpact Fund - Website About Us - 19 February 2025
GreenImpact Fund - Approved Jurisdiction - 09 January 2025


GROUP ACTIVITIES AND GEOGRAPHICAL FOOTPRINTS: (SAIL)
Sail Capital is a private markets investment firm that deploys capital to generate measurable long-term impact and consistent returns from direct exposure to sustainable growth businesses around the world. It is focused on scaling up a private credit approach, and their first fund is the GreenImpact Fund.
Currently, Sail Capital is not managing other mandates or strategies. However, through deployment of capital globally, they see a broad range of adjacent possibilities, which may lead them to pursue other opportunities in the future. Geographies involved are tropical forest zones globally, with a current portfolio delivering impact in Bravaria, Columbara, Indoria, Vietoria, and Ivory Shores.
Source:

Sail Capital B.V. - CoC Extract - 13 January 2025
Sail Capital - What We Do - 19 February 2025
Sail Capital - GEO Overview - 19 February 2025.pdf
Sail Capital - Annual Letter 2024


All documents related to GreenImpact Foundation are attached under GreenImpact Foundation (GCOB ID 31278), which is also part of the client’s structure.
Based on the aforementioned, the purpose and position of GreenImpact Capital B.V. within the larger group is clear and logical.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### OpenAI

# COMMAND ----------

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

# client_name = 'Sigma Investco'

# OLD
response = client.chat.completions.create(
    model=deployment_name,
    messages=[
        {"role": "system", "content": "Act as a senior KYC bank associate."}, # You are a helpful assistant.
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
        - Do **not** include any concluding or meta sentences (e.g., “This business summary provides…”, “Based on the provided text…”, “ensuring accuracy…”, etc.).
        - Do **not** include apologies, disclaimers, self-references, or process descriptions.
        - Do **not** repeat the instructions.
        - Do **not** add content not present in the provided text.

        Text:
        {all_text}
        """}
    ],
    temperature=0.2, # reduce it for more focused
    max_tokens=3000  # Increase this for longer output
)


# # NEW
# response = client.chat.completions.create(
#   model=deployment_name,
#   messages=[
#     {"role": "system", "content": "Act as a senior KYC bank associate. You must produce only the requested sections, with no extra commentary."},
#     {"role": "assistant", "content": f"Here is an example of the desired format:\n\n{example}"},
#     {"role": "user", "content": f"""
#     Based ONLY on the provided text, create a comprehensive business summary.

#     **Output Rules (very important):**
#     - Output **only** the following sections, in this exact order and with these exact headings:
#       1. Client Level Business Activities
#       2. Group Level Business Activities
#       3. Geographical Footprint
#       4. Sources
#     - Do **not** include any concluding or meta sentences (e.g., “This business summary provides…”, “Based on the provided text…”, “ensuring accuracy…”, etc.).
#     - Do **not** include apologies, disclaimers, self-references, or process descriptions.
#     - Do **not** repeat the instructions.
#     - Do **not** add content not present in the provided text.

#     **Content Requirements:**
#     - Follow the style and tone of the example provided above.
#     - Make the summary detailed and at least 1000 words.
#     - Our client is {client_name}. Identify the corresponding main shareholder (the group) **only if clearly stated in the text**.
#     - For **Geographical Footprint**, list the countries the client and group are active in **only if present** in the text. If not available, write “Not specified in the provided text.”
#     - In **Sources**, list only the filenames actually used to support the summary. If no file was used, write “None”.

#     **Text to analyze:**
#     {all_text}
#     """}
#   ],
#   temperature=0.2,
#   max_tokens=3000,
# )


business_summary = response.choices[0].message.content
displayHTML(f"<h2>Business Summary for {client_name}</h2><p>{business_summary}</p>")
