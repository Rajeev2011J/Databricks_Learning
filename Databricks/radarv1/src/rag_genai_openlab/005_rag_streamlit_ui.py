# Databricks notebook source
# MAGIC %md
# MAGIC # RAG for clients business activities

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
dbutils.fs.ls("/mnt/teamdata/source_documents/SigmaInvestco/")

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

prompt = f"Describe the client's business activities and geographies. If the client is part of a group, also describe the activities of the group entities and their geographical footprint."

example = f"""
CLIENT BUSINESS ACTIVITIES

&Green Fund B.V. is a limited liability company domiciled in Amsterdam with the registered office located at Basisweg 10, 1043 AP Amsterdam, the Netherlands. The Company was incorporated on 26-01-2024 and filed with the Trade Register at the Chamber of Commerce under number 92797814. According to the CoC, the Client is a financial holding company. Please note that client does not hold any subsidiaries.

The Client is part of &Green Fund (Stichting) incorporated on 11 July 2017 as an impact development fund. Its objective is to prove that financing inclusive, sustainable and deforestation-free commodity production can be commercially viable and replicable, thus strengthening the case for a new rural development paradigm that protects valuable forests and peat lands and promotes high-productivity agriculture.

Source: 
&Green Fund B.V.- KVK CoC extract- 20022025
&Green Fund B.V. - AoI - 26012024
&Green Fund B.V. - Statuten_Statutenwijziging- 31102024
&Green Fund - Website about us - 19022025
&Green Fund - Website invest - 19022025
Stichting andgreen.fund - KPMG Audited Annual Report 2023- 09012025 P.3 & P.15

PURPOSE OF THE BV:

According to the tax memo, the purpose of the BVs is to raise finance from the private sector through the issue of debt instruments, while the Stichting will continue to raise finance through concessional loans, grants and contributions, primarily from public sector contributions. 

The Stichting and Sail (investment advisor of the Stichting) have agreed to establish the Fund in order to catalyse further finance from the private and public sector to scale up the impact objectives of the &Green Fund.
Stichting shall sell and transfer the part of the &Gren Portfolio located in Green Climate Fund(GCF) Mandate Countries to the client. As a result all rights and obligations from Stichting vis a vis its current borrowers shall transfer to the relevant BV and will no longer be a party to the arrangements with the borrowers. To ensure compliance with GCF's requirement, the investment objective of the fund will be restricted to making investments in GCF Mandate Countries only. GCF Mandate
Countries means any of Brazil, Cameroon, Colombia, Cote d’Ivoire, Democratic Republic of the Congo, Ecuador, Gabon,Indonesia, Loa PDR, Liberia and Zambia, as well as any other country added hereto in accordance with the policies and procedures of the Green Climate Fund;(Pg 8, 27)

Stichting will enter into a Subsription Facility Agreement (SFA) with the BV, whereby it agrees to make funds available as a capital to provide loans. USD 180 million has been committed of which USD 147 million has been provided upfront. 

During the current outreach client has confirmed that, there are no changes in the activities of the client since last review and there are no private investors involved yet. &Green currently has USD 400* million in capital committed through grants, redeemable grants and loans from its contributors. &Green is actively raising capital from different investors such as NICFI, FMO/MFF, UNILIVER, UN environment programme, Green Climate Fund etc. Including USD 51 million Financing Agreement from Central African Forest Initiative (CAFI), in 2024.


Source: &Green - Tax and legal memo - 20022024(Page 14)
             &Green Fund B.V. - Shareholders agreement - 21022024(Pdf Pg 4, 8, 27, 6,37)
             &Green Fund B.V. - AoI - 26012024(Pdf Pg:26)
             &Green Fund B.V. - Statuten_Statutenwijziging- 31102024
             &Green Fund B.V.- KVK CoC extract- 20022025
             Stichting andgreen.fund - KPMG Audited Annual Report 2023- 09012025 P.3 & P.15
            Green Fund - Document request_Completed-05022025
           Green Fund -  Document request & client answers email trail-05022025
           Stichting andgreen.fund- Company Website activity- 17022025
            Stichting andgreen.fund- Company Website Investors- 17022025


-----------------------------------------------
Geographical Exposure:
-------------------------------------------------
Please note that client or the group itself has no activities in any high risk sanctioned countries, which is also confirmed by the client via sanction questionnaire. 

The client has incoming/outgoing payments with the  EC High Risk Third countries.  The incoming/outgoing payments are with the following countries; The Netherlands , United Arab Emirates (EC High Risk Third country) , Luxembourg, Indonesia and  Panama  (EC High Risk Third country ) (Reference; &Green Fund B.V. - Transaction History - 08012025.pdf)

The client has a  current account with Rabobank for providing facility/loans with the end-borrower which is incorporated in Indonesia, Brazil and Ivory Coast.

According to the Shareholders Agreement, &Green Fund B.V. will acquire Loan facility and Guarantee agreements with the following companies and jurisdictions where Stichting AndGreen.Fund was the original lender.

The account will receive capital from the Stichting andgreen.fund and will use that capital to provide loans that meet the investment principals of the fund. 

- PT Dharma Satya Nusantara TBK (DSNG) with total &Green investment of USD 30 million for a 10 year loan term from April 2020 in Indonesia.
- PT Hilton Duta Lestari (HDL) with total &Green investment of USD 12 million for a 8 year loan term from March 2022 in Indonesia.
- Agropecuaria Bambusa S.A.S. (HSJ) with total &Green investment of COP 300 million for 12 year loan term from December 2021 in Brazil.
- Marfrig Global Foods S.A. (Marfrig) with total &Green investment of USD 30 million for a 10 year loan term from January 2021 in Brazil.
-FS Luxembourg S.A.R.L. with total &Green investment of USD 30 million for 8-year tenor loan term from May 2022 in Brazil.
-ETC Group (ETG) with total &Green investment of USD 30 million for 8-year tenor from December 2023 in Ivory Coast.
- Agropecuaria Roncador LTDA. (Roncador) with &Green investment of USD 10 million for 8-year tenor from April 2020 in Brazil.

In addition, 
Please note that, Client is part of &Green Fund (Stichting) which act as an impact development fund. Client has also received  funds from Stichting andgreen.fund and  Stichting andgreen.fund has transaction with following high risk jurisdictions including, The Netherlands, US, Brazil, Indonesia, Germany, Luxembourg Colombia, UAE (HIGH FATF), France, Bahamas, UK, Switzerland, South Africa (HIGH FATF, and/or EC High Risk Third countries), Panama (HIGH FATF, and/or EC High Risk Third countries), Hungary, Vietnam (HIGH FATF ), Cayman Islands, Denmark, Mauritius, New Zealand, France, Lithuania, Australia and Singapore  (Reference; Stichting andgreen.fund - Transaction History - 08012025.pdf)

 As per clients website, & Green advisory board has approved following jurisdiction for the investment by Stichting andgreen.fund which includes Brazil, Ecuador, Ivory Coast, Gabon, Dr of the Congo, Colombia, Indonesia, Laos, Vietnam, Zambia. 

Therefore, based on above information Stichting andgreen.fund has transactions/ activities/Investments in following high risk jurisdictions;
High TF Risk : Colombia, Vietnam, South Africa, Panama
EC Non-Cooperative Jurisdictions: Bahamas, Panama
Corruption Risk: Ecuador, Gabon, Laos, Zambia	
EC High Risk Third Country: Vietnam, South Africa, Panama, UAE
High FATF Risk: Vietnam, South Africa
Tax Integrity Jurisdictions: Bahamas, Panama
Totalitarian Regimes: Laos			
High ML Risk: Colombia, Gabon, Laos, Liberia, Vietnam, South Africa, Panama
Medium sanctioned;  Dr of the Congo

CONCLUSION; As there is fund transfer from Stichting andgreen.fund to our client hence, there is comingling of funds. Therefore,   the above geo risk is also applicable to client and accordingly geo risks have been triggered in the file and assessed under EDD- follow up tabs. Please check follow up tabs for detailed mitigating factors.  

Source: &Green Fund B.V. - Shareholders agreement - 21022024(Pdf Pg 98)
             &Green Fund-Portfolio- 20022025
             &Green Stichting - Signed Organisation Chart_Nov 2024-05022025
             &Green Fund - Client answers - 07032024
            Stichting andgreen.fund - KVK CoC extract- 12022025
            &Green Fund - Website about us - 19022025
           &Green Fund  - Approved jurisdiction - 09012025
           Stichting andgreen.fund - Transaction History - 08012025.pdf
          Stichting andgreen.fund- Completed Sanctions Questionnaire - 05022025
           Green Fund -  Document request & client answers email trail-05022025
        &Green Fund B.V.- KVK CoC extract- 20022025 ; 
      Stichting andgreen.fund - KPMG Audited Annual Report 2023- 09012025 P.3 & P.15
       Green Fund - Document request_Completed-05022025
        Green Fund -  Document request & client answers email trail-05022025
       Stichting andgreen.fund- Company Website activity- 17022025
       Stichting andgreen.fund- Company Website Investors- 17022025
         &Green Fund B.V. - Transaction History - 08012025.pdf
          &Green Fund B.V. - Shareholders agreement - 21022024(P. 95)
            
 
MLRO Consultation/  Client Committee(CC) approval ; 
------------------------------------------------------------------------------------------
Please note that, during the onboarding of &Green Fund Vehicle B.V & Green Fund B.V. the file was already submitted to client committee due to ECHR3C nexus and it was discussed and approved by client committee. Furthermore, the MLRO was also consulted due to nexus with ECHR3C, and they have no objections as there are no unacceptable risks that have been identified in the file.  (Also, refer Executive summary)

However, Stichting andgreen.fund is transferred from FI’s to W&R this year in 2025 and this risk was not covered earlier on Stichting level. In addition, as the other two BV’s have also received funds from Stichting and this Stichting has more ECHR3Cs which are also applicable to other  BVs  due to transfer of funds from Stichting. All files have overall high risk rating hence, considering the recent changes to the ECHR3C risk framework, CC approval is required.

Therefore,  during the current review  (2025) the file was submitted to client committee(CC) for further  approval and the file is approved as is by client committee. Furthermore, the MLRO was also consulted due to nexus with ECHR3C, and they have no objections as there are no unacceptable risks that have been identified in the file.

Sources; &Green Funds - Client Committee Minutes - 28062024
CC Memo- &Green Fund Vehicle and &Green Fund V.1
&green.fund- Client Committee Memo - 10032025
Stichting andgreen_fund Group - Client Committee Decision email trail - 22052025
Stichting andgreen.fund Group  - Client Committee Minutes - 22052025
&green fund - MLRO Consultation SMSO ECHRTC-16052025
& Green fund- MRLO Consultation &Green Fund email trail-16052025
 

Financial connections;
---------------------------------------------
We see credits only from the group company (Stichting andgreen.fund), and from busines partners/investers which are also active in the same industry as client. Therefore, accordingly the sector risk has been triggered. The NAICS codes as well as sector risks  were triggered based on the actual business activities of the client, Chamber of Commerce extract and  Articles of Association.

GROUP ACTIVITIES AND GEOGRAPHICAL FOOTPRINTS: (STICHTING)

&Green is a Foundation (“Stichting”), established in the Netherlands in 2017. Its governance structure is designed to safeguard the environmental and social return as well as the financial and commercial sustainability of the Fund and its investments.

The forests and peatlands under the greatest threat from current agricultural expansion and unsustainable practices are located in the tropical regions of Latin America, Africa and South East Asia. &Green concentrates its efforts on exactly these geographic locations. In these regions, &Green only work in countries or states where the (local) authorities are committed to tackling land conversion as well.

The target sectors of &Green are the commodities that are driving deforestation – such as palm oil, soy, beef, forestry, and others, – in tropical forest jurisdictions with progressive forest protection strategies and targets in place. In this way, &Green can demonstrate the decoupling of commodity production from deforestation while being socially inclusive.

According to the Annual report, &Green Fund select jurisdictions with a regulatory framework protection to safeguard forest protection achievements associated with &Green’s investments.

Group Geography: Please refer above under Geographical Exposure section.

Source: 
Stichting andgreen.fund - KPMG Audited Annual Report 2023- 09012025
&Green Fund - Website about us - 19022025
&Green Fund  - Approved jurisdiction - 09012025

GROUP ACTIVITIES AND GEOGRAPHICAL FOOTPRINTS: (SAIL)

SAIL Investments is a private markets investment firm who deploy capital to generate measurable long-term impact and consistent returns from a direct exposure to sustainable growth businesses around the world.It is focused on scaling up private credit approach. And their first Fund is the &Green Fund.

Currently SAIL is not managing other mandates or strategies. However, through deployment of capital globally they see a broad range of adjacent possibilities, which may lead them to pursue other opportunities in the future. Geographies involved are; Tropical forest zones globally, with a current portfolio delivering impact in Brazil, Colombia, Indonesia, Vietnam and Côte d’Ivoire.

Source:Sail Investments B.V. - KVK CoC extract- 13012025
SAIL Investments - What we do - 19022025
SAIL Investments - What we do- GEO - 19022025.pdf
SAIL Investments - Annual letter 2024

All documents related to Stichting andgreen.fund are attached under Stichting andgreen.fund (GCOB ID 31278) which is also part of client's structure.

Based on the aforementioned the purpose and position of &Green Fund B.V.  within the larger group is clear and logical."""

# COMMAND ----------

# MAGIC %md
# MAGIC ### OpenAI

# COMMAND ----------

prompt = f"""
You are an assistant that creates business summaries based ONLY on the provided text.
Text:
{all_text}

Task:
Describe the client's business activities and geographies.
If the client is part of a group, also describe the activities of the group entities and their geographical footprint.
"""

# COMMAND ----------

prompt = f"""
You are an assistant that creates business summaries based ONLY on the provided text.

Example of desired output:
{example}

Text:
{all_text}

Task:
Describe the client's business activities and geographies.
If the client is part of a group, also describe the activities of the group entities and their geographical footprint.
"""

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

# COMMAND ----------



# COMMAND ----------

# streamlit_app.py
import streamlit as st
import os
import shutil
from openai import AzureOpenAI
from azure.identity import ClientSecretCredential

# --- UI ---
st.title("Business Summary Generator")

client_name = st.text_input("Enter Client Name")
uploaded_files = st.file_uploader("Upload Documents", accept_multiple_files=True, type=["pdf", "docx", "txt"])
generate_button = st.button("Generate Summary")

# --- Logic ---
LAB_VARIANT = "OpenLab"
ENVIRONMENT = "prd"

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

    # Save files to mounted container
    input_folder = "/dbfs/mnt/teamdata/input"
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

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install streamlit
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC streamlit run 006_rag_streamlit_ui_v2.py
