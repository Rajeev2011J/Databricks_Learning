# Databricks notebook source
# MAGIC %md
# MAGIC # OpenAI: getting started
# MAGIC This notebook is meant for getting started with OpenAI service. It can be used in **One!Lab** and **Open!Lab**. 
# MAGIC
# MAGIC ## Learning resources
# MAGIC
# MAGIC This repository only contains code snippets that help you get started in our platform. Check the next notebook for more resources.
# MAGIC
# MAGIC If you want to learn more or have questions, there are two main resources:
# MAGIC 1. [Azure OpenAI Service documentation](https://learn.microsoft.com/en-us/azure/ai-services/openai/). Not all code that they provide works from the get-go in our platform. You often need to change how you connect to the Azure AI Service, as we show in some of our examples.
# MAGIC 2. [Microsoft Learn - Develop Generative AI solutions with Azure OpenAI Service](https://learn.microsoft.com/en-us/training/paths/develop-ai-solutions-azure-openai/). Note that this course often **uses Azure AI Studio, which we don't support** due to technical and compliance reasons.
# MAGIC
# MAGIC

# COMMAND ----------

# If you are a platform engineer running this in dev, please use the following:
#%pip config --user set global.index-url https://NEXUS_USERNAME:NEXUS_PASSWORD@repo.nexuscloud.aws.rabo.cloud/repository/gr-pypi-13/simple
%pip install openai==1.56.2
%pip install azure-identity==1.19.0
%pip install gradio==4.43.0

# restart the kernel
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choose the OneLab variant you're using
# MAGIC
# MAGIC This example is using the **OpenLab/OneLab production** environment (if you are a user of the platform, you'll only have access to production resources, "dev" and "uat" are meant for platform engineers).
# MAGIC
# MAGIC Change the statement containing `lab` to either OpenLab or OneLab.

# COMMAND ----------

# Choose your Lab Environment
# USE "OneLab" IF YOU'RE USING ONELAB
LAB_VARIANT = "OpenLab" 

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC If everything is working, you should be able to run the code after this point. 
# MAGIC
# MAGIC
# MAGIC ## Create an Azure Open AI client instance

# COMMAND ----------

import os
import openai
from openai import AzureOpenAI
from azure.identity import ClientSecretCredential
 
client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope",key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope",key="DataServicePrincipalClientSecret")
credential = ClientSecretCredential(tenant_id="6e93a626-8aca-4dc1-9191-ce291b4b75a1", client_id=client_id, client_secret=client_secret)
access_token = credential.get_token("https://cognitiveservices.azure.com/.default")

os.environ["AZURE_OPENAI_TOKEN"] = access_token.token
os.environ["AZURE_OPENAI_VERSION"] = "2024-10-21"  # https://learn.microsoft.com/en-us/azure/ai-services/openai/reference#api-specs
os.environ["AZURE_OPENAI_BASE_URL"] = get_openai_urls(LAB_VARIANT)

client = AzureOpenAI(
  api_key=os.environ["AZURE_OPENAI_TOKEN"],  
  api_version=os.environ["AZURE_OPENAI_VERSION"],
  azure_endpoint=os.environ["AZURE_OPENAI_BASE_URL"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: chat completion with GPT 4
# MAGIC
# MAGIC You can find other models at [Microsoft Learn](https://learn.microsoft.com/en-us/azure/ai-services/openai/concepts/models?tabs=python-secure%2Cglobal-standard%2Cstandard-chat-completions) to use something other than a `gpt-4o` deployment.

# COMMAND ----------

deployment_name = "gpt-4-turbo"
    
# Send a completion call to generate an answer
completion = client.chat.completions.create(
  model=deployment_name,
  messages=[
    {"role": "system", "content": "You are a helpful assistant. Help me with my math homework and explain the problem-solving approach you used!"}, # <-- This is the system message that provides context to the model
    {"role": "user", "content": "Hello! Could you solve 2156x1398?"}                                                                                # <-- This is the user message for which the model will generate a response
  ]
)

print("Assistant: " + completion.choices[0].message.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: using images in GPT-4o
# MAGIC
# MAGIC This is a model that allows us to input images, which allows you to build an application with some additional interactivity.
# MAGIC
# MAGIC Example adapted from: https://github.com/retkowsky/Azure-OpenAI-demos/blob/main/GPT-4o/GPT-4o%20model%20with%20Azure%20OpenAI.ipynb

# COMMAND ----------

import base64
import os
import requests

from io import BytesIO
from openai import AzureOpenAI
from PIL import Image


def file_exists_in_workspace(path: str) -> bool:
    path = "file:" + path
    try:
      dbutils.fs.ls(path)
      return True
    except Exception as e:
      if 'java.io.FileNotFoundException' in str(e):
        return False
      else:
        raise

path_jpg = "/Workspace/Repos/Starter_Kit/Databricks_Library/sample-data/paris2024.jpg"
if not file_exists_in_workspace(path_jpg):
    image_url = "https://github.com/retkowsky/images/blob/master/jo.png?raw=true"
    response = requests.get(image_url)
    img = Image.open(BytesIO(response.content))
    img.save("sample-data/paris2024.jpg", optimize=True, quality=10)  # reduce quality due to rate limits

new_img = Image.open(path_jpg)
new_img

# COMMAND ----------

with open(path_jpg, "rb") as image_file:
    base64_encoded_data = base64.b64encode(image_file.read()).decode('utf-8')
deployment_name = "gpt-4o"
response = client.chat.completions.create(
    model=deployment_name,
    messages=[
        {
            "role": "system",
            "content": "You are a helpful assistant to analyse images.",
        },
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Where is this picture taken from? I don't know, help me out! D: "},
                {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + base64_encoded_data}},
            ],
        },
    ]
)
response.choices[0].message.content

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating images using DALL-E-3

# COMMAND ----------

import requests

from io import BytesIO
from openai import AzureOpenAI
from PIL import Image

if LAB_VARIANT == "OpenLab": 
  prompt_response = client.images.generate(model="dall-e-3", prompt="Generate a logo with beers in it for a cycling event in Nijmegen Weurtse Dijk", size="1024x1024", n=1)
  image_response = requests.get(prompt_response.data[0].url)
  img = Image.open(BytesIO(image_response.content))
  display(img)
else:
  print("This is only supported for OpenLab")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full example
# MAGIC
# MAGIC Easier to copy-paste if you want to reuse it.

# COMMAND ----------

# If you are a platform engineer running this in dev, please use the following:
#%pip config --user set global.index-url https://NEXUS_USERNAME:NEXUS_PASSWORD@repo.nexuscloud.aws.rabo.cloud/repository/gr-pypi-13/simple
%pip install openai==1.56.2
%pip install azure-identity==1.19.0
%pip install gradio==4.43.0

# restart the kernel
dbutils.library.restartPython()

# COMMAND ----------

# Choose your Lab Environment 
# USE "OneLab" IF YOU'RE USING ONELAB
LAB_VARIANT = "OpenLab" 

# COMMAND ----------

import os
import base64
import openai
from openai import AzureOpenAI
import requests
from azure.identity import ClientSecretCredential
from io import BytesIO
from PIL import Image
 
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

# Example
deployment_name = "gpt-4-turbo"   
completion = client.chat.completions.create(
  model=deployment_name,
  messages=[
    {"role": "system", "content": "You are a helpful assistant. Help me with my math homework and explain the problem-solving approach you used!"}, # <-- This is the system message that provides context to the model
    {"role": "user", "content": "Hello! Could you solve 2156x1398?"}  # <-- This is the user message for which the model will generate a response
  ]
)
print("Assistant: " + completion.choices[0].message.content)


# Example for inputting images
def file_exists_in_workspace(path: str) -> bool:
    path = "file:" + path
    try:
      dbutils.fs.ls(path)
      return True
    except Exception as e:
      if 'java.io.FileNotFoundException' in str(e):
        return False
      else:
        raise

path_jpg = "/Workspace/Repos/Starter_Kit/Databricks_Library/sample-data/paris2024.jpg"
if not file_exists_in_workspace(path_jpg):
    image_url = "https://github.com/retkowsky/images/blob/master/jo.png?raw=true"
    response = requests.get(image_url)
    img = Image.open(BytesIO(response.content))
    img.save("sample-data/paris2024.jpg", optimize=True, quality=10)  # reduce quality due to rate limits
new_img = Image.open(path_jpg)
with open(path_jpg, "rb") as image_file:
    base64_encoded_data = base64.b64encode(image_file.read()).decode('utf-8')

deployment_name = "gpt-4o"
response = client.chat.completions.create(
    model=deployment_name,
    messages=[
        {
            "role": "system",
            "content": "You are a helpful assistant to analyse images.",
        },
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Where is this picture taken from? I don't know, help me out! D: "},
                {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + base64_encoded_data}},
            ],
        },
    ]
)
response.choices[0].message.content


# COMMAND ----------

# MAGIC %md
# MAGIC The following examples are using the Azure OpenAI **gpt-4o** and **gpt-35-turbo-instruct** models.
# MAGIC
# MAGIC Make sure to update the endpoints. You can find them on [Azure OpenAI](https://confluence.dev.rabobank.nl/display/OneLab/Azure+OpenAI) documentation.

# COMMAND ----------

# Using gpt-4o model

import os
import openai
from openai import AzureOpenAI
from azure.identity import ClientSecretCredential

completion = client.chat.completions.create(
  model="gpt-4o",
  messages=[
    {"role": "system", "content": "You are a helpful assistant. Help me with my math homework!"}, # <-- This is the system message that provides context to the model
    {"role": "user", "content": "Hello! Could you solve 2156x1398?"}  # <-- This is the user message for which the model will generate a response
  ]
)

print("Assistant: " + completion.choices[0].message.content)

# COMMAND ----------

# Using gpt-35-turbo-instruct model

import os
import openai
from openai import AzureOpenAI
from azure.identity import ClientSecretCredential

deployment_name='gpt-35-turbo-instruct' #This will correspond to the custom name you chose for your deployment when you deployed a model. Use a gpt-35-turbo-instruct deployment. 

# Send a completion call to generate an answer
print('Sending a test completion job')
start_phrase = 'Write a tagline for an ice cream shop. '
response = client.completions.create(
    model=deployment_name, 
    prompt=start_phrase, 
    max_tokens=10
)

# print(response)

print(start_phrase+response.choices[0].text)
