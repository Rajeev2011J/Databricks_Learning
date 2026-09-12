# Databricks notebook source
# MAGIC %md
# MAGIC # OpenAI - basic RAG examples
# MAGIC
# MAGIC This example is partially adapted from [LangChain's RAG tutorial](https://python.langchain.com/docs/tutorials/rag/). We are keeping our example a bit smaller and more focused so that you can get started quickly, even if you're not very familiar with programming.
# MAGIC
# MAGIC If you want to learn more or develop something more advanced, check the following resources:
# MAGIC
# MAGIC * [OpenAI cookbook](https://github.com/openai/openai-cookbook). It has many examples like ["How to handle rate limits"](https://github.com/openai/openai-cookbook/blob/main/examples/How_to_handle_rate_limits.ipynb).
# MAGIC * [LangChain documentation](https://python.langchain.com/docs/introduction/). This is the most commonly used Python framework/package for creating apps using large language models (LLMs). It goes over many use cases for large language models.
# MAGIC * [Azure OpenAI Service documentation](https://learn.microsoft.com/en-us/azure/ai-services/openai/). 
# MAGIC * [GDP's dummy data generator API](https://dev.azure.com/raboweb/Tribe%20Data%20and%20Analytics/_wiki/wikis/The%20Global%20Data%20Platform%20%28GDP%29/130727/Dummy-Data-Generator/). You can generate synthetic data based on Data Objects from the schema in the Collibra Data Catalog or a user defined schema. 
# MAGIC * [Rabobank's Oasis repository](https://dev.azure.com/raboweb/Tribe%20Data%20and%20Analytics/_git/oasis?version=GBmain) and [Rabobank's data-science-area-interview-case](https://dev.azure.com/raboweb/Tribe%20Data%20and%20Analytics/_git/data-science-area-interview-case?path=%2Fdata) (this is where the synthetic datasets in /Workspace/Repos/Starter_Kit/Databricks_Library/sample-data were taken from). This is something developed within the Rabobank. Check some of their notebooks (e.g., [summary comparisons](https://dev.azure.com/raboweb/Tribe%20Data%20and%20Analytics/_git/oasis?version=GBmain&path=/notebooks/archive/archive_cdd_summarization/run_summary_comparisons.py)).

# COMMAND ----------

# If you're running this in dev/UAT (as a platform engineer), use your *personal* Nexus credentials so that it uses nexuscloud prd
#%pip config --user set global.index-url https://NEXUS_USERNAME:NEXUS_PASSWORD@repo.nexuscloud.aws.rabo.cloud/repository/gr-pypi-13/simple

# Fixing the versions so that the tutorial doesn't break, DISREGARD THE ERRORS
%pip install openai==1.56.2
%pip install azure-identity==1.19.0
%pip install langchain
%pip install langchain-community
%pip install langchain-chroma
%pip install langchain-openai
%pip install beautifulsoup4==4.12.3
%pip install pydantic==2.9.2
%pip install bs4
%pip install -U mlflow

# it's necessary to restart the kernel
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding the fundamentals: a simple RAG pipeline
# MAGIC
# MAGIC The first chunk contains all the RAG code, the second one explains the results.
# MAGIC
# MAGIC

# COMMAND ----------

######## Boilerplate necessary to make things work in OneLab/OpenLab ########
LAB_VARIANT = "OpenLab" # USE "OneLab" IF YOU'RE USING ONELAB

# COMMAND ----------

import os
import bs4
from openai import AzureOpenAI
from azure.identity import ClientSecretCredential
from langchain_community.document_loaders import WebBaseLoader

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

client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientSecret")
credential = ClientSecretCredential(tenant_id="6e93a626-8aca-4dc1-9191-ce291b4b75a1", client_id=client_id, client_secret=client_secret)
access_token = credential.get_token("https://cognitiveservices.azure.com/.default")

os.environ["AZURE_OPENAI_TOKEN"] = access_token.token
os.environ["AZURE_OPENAI_VERSION"] = "2024-10-21"  # https://learn.microsoft.com/en-us/azure/ai-services/openai/reference#api-specs
os.environ["AZURE_OPENAI_BASE_URL"] = get_openai_urls(LAB_VARIANT)
os.environ["USER_AGENT"] = "myagent"

client = AzureOpenAI(
  api_key=os.environ["AZURE_OPENAI_TOKEN"],  
  api_version=os.environ["AZURE_OPENAI_VERSION"],
  azure_endpoint=os.environ["AZURE_OPENAI_BASE_URL"]
)

# COMMAND ----------

######## Utility functions ########
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
  

######## Load the data (scraped Rabobank Wikipedia page) ########
path_rabo_wiki = "/Workspace/Repos/Starter_Kit/Databricks_Library/sample-data/rabo_wiki_page.txt"
if not file_exists_in_workspace(path_rabo_wiki):
    # scrape the page and save it 
    bs4_strainer = bs4.SoupStrainer()
    loader = WebBaseLoader(
        web_paths=("https://en.wikipedia.org/wiki/Rabobank",),
        bs_kwargs={"parse_only": bs4_strainer},
    )
    docs = loader.load()
    rabo_wiki_content = docs[0].page_content.replace("\n", " ")
    with open(path_rabo_wiki, mode="w+") as file:
      file.write(rabo_wiki_content)
else: 
    with open(path_rabo_wiki, mode="r") as file:
      rabo_wiki_content = file.read()


######## Functions for chat completion ########
def get_completion(query: str, context: str) -> str:
    """Returns the completion from GPT4 without any augmentation."""
    completion = client.chat.completions.create(
      model="gpt-4o",
      messages=[
        {"role": "system", "content": context},  # <-- This is the system message that provides context to the model
        {"role": "user", "content": query}  # <-- This is the user message for which the model will generate a response
      ],
      seed=1
    )
    return completion.choices[0].message.content


def get_completion_with_rag(query: str, context: str, extra_content: str) -> str:
    """Runs a RAG pipeline that will augment the context with extra_content if 'rabo' is in the query. Returns the chat completion."""  
    if "rabo" in query.lower():
      context += f"Wikipedia page about Rabobank that you can reference: {extra_content}"
    completion = client.chat.completions.create(
      model="gpt-4o",
      messages=[
        {"role": "system", "content": context},  # <-- This is the system message that provides context to the model
        {"role": "user", "content": query}  # <-- This is the user message for which the model will generate a response
      ],
      seed=1
    )
    return completion.choices[0].message.content


######## Main part of the code ########
user_query = "In which countries does RaboDirect operate in?"
instructions = "The user who is asking you a question is a potential Rabobank customer. Treat them well and sell them on our services whenever possible! Give a one-line answer, but still try to sell."
result_without_rag = get_completion(query=user_query, context=instructions)
result_with_rag = get_completion_with_rag(query=user_query, context=instructions, extra_content=rabo_wiki_content[:-10_000])  # exclude the last 10.000 characters due to model limits


# COMMAND ----------

import mlflow
mlflow.openai.autolog()
######## Explanation (run this chunk for it to render) ########
from IPython.display import display, Markdown

print_markdown = lambda s: display(Markdown(s))  # small helper function for displaying markdown next to code output since I'm allergic to too many jupiter notebook chunks :(
new_line_char = "\n"  # workaround for python <3.12
print_markdown(f"""
### Explanation

Imagine that you have a generative AI application that allows potential Rabobank customers to ask any question. We have been tasked with improving the reliability of the answers.

The first thing we could do is to feed the user's question to a generative model and give back its answer to the user. That's what we are doing with the `get_completion` function,
where we pass the user's query:

> USER QUERY: {user_query}

and we also add pass some context to instruct the model to answer the question in the way that we want to:

> INSTRUCTIONS: {instructions}

This is how GPT-4 responded:

> RESULT WITHOUT RAG: {result_without_rag.replace(new_line_char, ' ')}

The answer is wrong ("**Ireland**, **New Zealand** and **Australia**", failed to mention Belgium and Germany) since GPT-4 doesn't have any direct access to
Rabobank's wiki page. It's true that the wiki page is in the data that the model was trained on, but that model only contains an approximate representation of that information, it doesn't have the 
the information itself (analogously, a map contains an approximate representation of a country, but a lot of information is lost). It may also have been trained on an older version of the wiki page.

We can do better by using retrieval augmented generation. In our simple example, it will "retrieve" the Rabobank wiki page and augment the context that's fed to the model if "rabo" is in the user's query.
When we do that, we get this result:

> RESULT WITH RAG: {result_with_rag.replace(new_line_char, ' ')}

Here, it's giving us all the countries that are in the wiki page ("RaboDirect operates in **Belgium**, the **Republic of Ireland**, **Australia**, **New Zealand**, and **Germany**"). 

Be mindful that the model is still probabilistic, so there's no guarantee that it will use the information you provide it, or that it will even reference it correctly (search for "LLM hallucinations" to know more).
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A more realistic example: RAG with semantic search using LangChain
# MAGIC
# MAGIC The first chunk contains all the RAG code, the second one explains the results.

# COMMAND ----------

######## Boilerplate necessary to make things work in OneLab/OpenLab ########
LAB_VARIANT = "OpenLab" # USE "OneLab" IF YOU'RE USING ONELAB

# COMMAND ----------

# WARNING: takes ~14 minutes to run on a Standard_DS3_V2 cluster (mostly due to the calls to the OpenAI embeddings API), speed it up by making it async

import os
import bs4
from openai import AzureOpenAI, RateLimitError
from azure.identity import ClientSecretCredential
from langchain_community.document_loaders import TextLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_chroma import Chroma
from langchain_openai import AzureOpenAIEmbeddings
import re
import time
from uuid import uuid4
from langchain_core.documents import Document

ENVIRONMENT = "prd"
def get_openai_urls(lab_variant: str):
  """This function is created to return OpenAI URLs for OpenLab/OneLab. We want to use the function with lazy evaluation so that OneLab doesn't affect OpenLab and vice-versa."""
  if lab_variant == "OpenLab":
    secret_scope = f"{lab_variant}-SecretScope"
    return f"https://{dbutils.secrets.get(scope=secret_scope, key='OpenAiHostname')}openoaisdc-completions-apis/"
  elif lab_variant == "OneLab":
    return f"https://apim-1labgen-ap-apizone-{ENVIRONMENT}01.azure-api.net/openaisdc-completions-apis/"
  else: 
    raise Exception("Invalid lab_variant")

client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope",key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope",key="DataServicePrincipalClientSecret")
credential = ClientSecretCredential(tenant_id="6e93a626-8aca-4dc1-9191-ce291b4b75a1", client_id=client_id, client_secret=client_secret)
access_token = credential.get_token("https://cognitiveservices.azure.com/.default")

os.environ["AZURE_OPENAI_TOKEN"] = access_token.token
os.environ["AZURE_OPENAI_AD_TOKEN"] = access_token.token
os.environ["AZURE_OPENAI_VERSION"] = "2024-10-21"  # https://learn.microsoft.com/en-us/azure/ai-services/openai/reference#api-specs
os.environ["AZURE_OPENAI_BASE_URL"] = get_openai_urls(LAB_VARIANT)
os.environ["AZURE_OPENAI_ENDPOINT"] = get_openai_urls(LAB_VARIANT)

client = AzureOpenAI(
  api_key=os.environ["AZURE_OPENAI_TOKEN"],  
  api_version=os.environ["AZURE_OPENAI_VERSION"],
  azure_endpoint=os.environ["AZURE_OPENAI_BASE_URL"]
)


######## Utility functions ########
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


######## Load the data (scraped Rabobank Wikipedia page) ########
path_rabo_wiki = "/Workspace/Repos/Starter_Kit/Databricks_Library/sample-data/rabo_wiki_page.txt"
loader = TextLoader(path_rabo_wiki)
docs = loader.load()
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000, chunk_overlap=200, add_start_index=True
)
all_splits = text_splitter.split_documents(docs)

if LAB_VARIANT == "OpenLab": 
  ######## Create the vector store for embeddings and store the split data ########
  def add_doc_with_backoff(vector_store: Chroma, doc: Document, sleep_seconds: str=5) -> None:
      """Tries to add a document vector store but it will wait and retry if we get a warning about it.
        This is a simple function for demonstration purposes. 
        Check https://github.com/openai/openai-cookbook/blob/main/examples/How_to_handle_rate_limits.ipynb to see better ways of doing it."""
      try: 
          time.sleep(sleep_seconds)
          doc_id = str(uuid4())
          print(f"Adding {doc_id} to the vector store.")
          vector_store.add_documents(documents=[doc], id=[doc_id])
          print(f"Successfully added {doc_id} to vector vector store.")
      except RateLimitError as error: 
          error_message = str(error)
          seconds_to_wait = int(re.search(r"(\d+) seconds", error_message).group(1)) + 1
          if seconds_to_wait < 600:
              print(f"Waiting {seconds_to_wait} before retrying to add document to the vector store.")
              add_doc_with_backoff(vector_store=vector_store, doc=doc, sleep_seconds=seconds_to_wait)
          else: 
              # If you have to wait too long, there's likely an issue (the document is probably way too big or your rate limits are too small)
              raise error

  vector_store = Chroma(
      collection_name="rabo_wiki_page",
      embedding_function=AzureOpenAIEmbeddings(model="text-embedding-ada-002"),  # uses AZURE_OPENAI_ENDPOINT
      persist_directory="./chroma_db_rag_example",  # Where to save data locally, remove if not necessary
  )

  for d in all_splits:
      add_doc_with_backoff(vector_store, doc=d, sleep_seconds=5)


  ######## Functions for chat completion ########
  def get_completion(query: str, context: str) -> str:
      """Returns the completion from GPT4 without any augmentation."""
      completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
          {"role": "system", "content": context}, # <-- This is the system message that provides context to the model
          {"role": "user", "content": query}  # <-- This is the user message for which the model will generate a response
        ],
        seed=1
      )
      return completion.choices[0].message.content
    

  def get_completion_with_rag(query: str, context: str, extra_content: str) -> str:
      """Runs a RAG pipeline that will augment the context with extra_content if 'rabo' is in the query. Returns the chat completion."""  
      
      completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
          {"role": "system", "content": context}, # <-- This is the system message that provides context to the model
          {"role": "user", "content": query}  # <-- This is the user message for which the model will generate a response
        ],
        seed=1
      )
      return completion.choices[0].message.content

  def get_completion_with_rag(query: str, context: str, extra_content: str) -> str:
      """Runs a RAG pipeline that will augment the context with extra_content if 'rabo' is in the query. Returns the chat completion."""  
      context += f"Wikipedia page about Rabobank that you can reference: {extra_content}"
      completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
          {"role": "system", "content": context},  # <-- This is the system message that provides context to the model
          {"role": "user", "content": query}  # <-- This is the user message for which the model will generate a response
        ],
        seed=1
      )
      return completion.choices[0].message.content


  ######## Main part of the code ########
  user_query = "I'm buying a home and I want to borrow from an institution with low CO2 emissions. Are you the right choice?"
  instructions = "The user who is asking you a question is a potential Rabobank customer. Treat them well and sell them on our services whenever possible! Give a one-line answer, but still try to sell."
  result_without_rag = get_completion(query=user_query, context=instructions)

  # This is a simple example, so it's not using all of LangChain's abstractions
  retriever = vector_store.as_retriever(
      search_type="mmr",      # https://python.langchain.com/v0.1/docs/modules/model_io/prompts/example_selectors/mmr/
      search_kwargs={"k": 5, "fetch_k": 100}  # top 5 results out of 100 fetched at random
  )
  retrieved_rabo_wiki_chunks = "; ".join([doc.page_content for doc in retriever.invoke(instructions + user_query)])  # will look like "{1st retrieved doc} ; {2nd retrieved doc}; (...)"
  result_with_rag = get_completion_with_rag(query=user_query, context=instructions, extra_content=retrieved_rabo_wiki_chunks)

else:
  print("This is only supported for OpenLab")
  

# COMMAND ----------

if LAB_VARIANT == "OpenLab": 
  ######## Explanation (run this chunk for it to render) ########
  from IPython.display import display, Markdown

  print_markdown = lambda s: display(Markdown(s))  # small helper function for displaying markdown next to code output since I'm allergic to too many jupiter notebook chunks :(
  new_line_char = "\n"  # workaround for python <3.12
  print_markdown(f"""
  ### Explanation

  There are two main issues with the previous approach:

  * **You can't feed all information to the model:** you might have millions of pages of content that you want the model to potentially reference, but even the best models only allow you to put up to a few thousand pages at most in the prompt.
  * **It's hard to search and filter information** if you only want to feed information that relates to the user's question: a lot of content is not relevant, and users may ask questions in an indirect way — "I want
  to buy a house" implies that someone may want a mortgage, but finding all pages with "buy a house" in it might not bring up all the documents that contain "mortgage". 

  Even in our simple RAG example, passing the whole wikipedia page gives us issues since it contains 8500+ tokens and GPT-4 doesn't allow for that many. That's why we exclude the last 10.000 characters (`extra_content=rabo_wiki_content[:-10_000]`).

  In the next example will do three new things:
  1. We will use use an OpenAI embeddings to then perform semantic search (i.e., match content based on the meaning of the text rather than just matching keywords).
  2. We use the LangChain framework more. It allows you to potentially organize your code in a better way and simplifies some tasks, although it does 

  > USER QUERY: {user_query}

  It's clear from the meaning that the user wants a mortgage with a bank that prioritizes climate goals but, at the time of this writing, the only keyword that's simultaneously in the user query and in the Rabo wiki is "institution(s)". 
  The parts of the wiki with those keywords are also not relevant for the query in question.

  Similarly to the [LangChain tutorial](https://python.langchain.com/docs/tutorials/rag/#setup), we do the following...

  ### Load the data

  We use a [document loader](https://python.langchain.com/v0.1/docs/modules/data_connection/document_loaders/). 

  ### Split the text into chunks 

  We could've made chunks based on sentences, paragraphs, or anything else. Here's an example of one of those splits:

  > CHUNK OF THE WIKIPEDIA PAGE: {all_splits[29].page_content.replace(new_line_char, ' ')}

  This split contains the part that we're interested in passing to the model, since it talks about climate goals and everything else. 

  ### Index the chunks and store them

  Then we store the embeddings in a vector store/embedding database called Chroma. These embeddings are just vectors (arrays of numbers) that model how closely related two tokens or groups of tokens are — the more they are related, the smaller the distance between those vectors. That relatedness is inferred by how often the tokens co-occur. 

  Make sure that your splits of your documents are not too big, otherwise you'll get rate-limited.

  ### Retrieval 

  When we ask for the top 5 results that are most closely related to our prompt, we get the following:

  > RETRIEVED CHUNKS (truncated): {retrieved_rabo_wiki_chunks[:1000].replace(new_line_char, ' ')}

  This is what we wanted, the part of the article that talks about sustainability at Rabobank! We didn't have to put the whole page in there to get this.

  ### Generation

  In the result without RAG, we get something that's not very specific.
  > RESULT WITHOUT RAG: {result_without_rag.replace(new_line_char, ' ')}

  In the result with RAG, the generated response that slightly references things said in the article (the model says "integrating practices of corporate social responsibility in our core activities", which is said in the article in "Corporate social responsibility is a basic principle in Rabobank's core activities").

  > RESULT WITH RAG: {result_with_rag.replace(new_line_char, ' ')}

  Expect to spend most of your time getting the retrieval part to work well. If that's done correctly, generating good and accurate output is much easier. 
  """)
else:
  print("This is only supported for OpenLab")

# COMMAND ----------

# MAGIC %md
# MAGIC # Assitants APIs
# MAGIC The OpenAI Assistants API enables to build intelligent, multi-turn agents that can maintain context, use tools, and interact with files. It works by creating an Assistant with predefined instructions and capabilities, then managing conversations through a thread, where user messages are added and processed via "runs" that trigger the assistant's response.
# MAGIC
# MAGIC Example below: 
# MAGIC It sets up an assistant using the GPT-4 Turbo model with code interpreter capabilities, creates a thread, sends a user query, and initiates a run to generate the assistant's response. The thread is polled until completion, after which the assistant's message is retrieved and printed. The setup is tailored for secure, enterprise-grade usage behind APIM, enabling multi-turn, context-aware interactions with Azure OpenAI services.

# COMMAND ----------

# MAGIC %pip install openai==1.56.2
# MAGIC %pip install azure-identity==1.19.0
# MAGIC %pip install gradio==4.43.0

# COMMAND ----------

######## Boilerplate necessary to make things work in OneLab/OpenLab ########
LAB_VARIANT = "OpenLab" # USE "OneLab" IF YOU'RE USING ONELAB

# COMMAND ----------

import os
import time
from openai import AzureOpenAI
from azure.identity import ClientSecretCredential

ENVIRONMENT = "prd"
def get_openai_urls(lab_variant: str):
  """This function is created to return OpenAI URLs for OpenLab/OneLab. We want to use the function with lazy evaluation so that OneLab doesn't affect OpenLab and vice-versa."""
  if lab_variant == "OpenLab":
    secret_scope = f"{lab_variant}-SecretScope"
    return f"https://{dbutils.secrets.get(scope=secret_scope, key='OpenAiHostname')}openoaisdc-assistants-apis/"
  elif lab_variant == "OneLab":
    return f"https://apim-1labgen-ap-apizone-{ENVIRONMENT}01.azure-api.net/openaisdc-assistants-apis/"
  else: 
    raise Exception("Invalid lab_variant")

client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientSecret")
credential = ClientSecretCredential(tenant_id="6e93a626-8aca-4dc1-9191-ce291b4b75a1", client_id=client_id, client_secret=client_secret)
access_token = credential.get_token("https://cognitiveservices.azure.com/.default")

os.environ['USER_AGENT'] = 'myagent'
os.environ["AZURE_OPENAI_TOKEN"] = access_token.token
os.environ["AZURE_OPENAI_VERSION"] = "2024-05-01-preview"  # https://learn.microsoft.com/en-us/azure/ai-services/openai/reference#api-specs
os.environ["AZURE_OPENAI_BASE_URL"] = get_openai_urls(LAB_VARIANT)

client = AzureOpenAI(
  api_key=os.environ["AZURE_OPENAI_TOKEN"],  
  api_version=os.environ["AZURE_OPENAI_VERSION"],
  azure_endpoint=os.environ["AZURE_OPENAI_BASE_URL"]
)
# Create assistant
assistant = client.beta.assistants.create(
    name="Macro Analyser",
    instructions="Give average & median salaries in JSON format.",
    model="gpt-4-turbo",
    tools=[{"type": "code_interpreter"}],
)
 
# Run a simple thread
thread = client.beta.threads.create()
message = client.beta.threads.messages.create(thread_id=thread.id, role="user", content="Average salary in US")
run = client.beta.threads.runs.create(thread_id=thread.id, assistant_id=assistant.id)
 
# Wait until it's done
import time
while True:
    status = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
    if status.status in ["completed", "failed", "cancelled"]:
        break
    time.sleep(1)
 
# Print response
messages = client.beta.threads.messages.list(thread_id=thread.id)
for msg in messages.data:
    if msg.role == "assistant":
        print(msg.content[0].text.value)

# COMMAND ----------

# MAGIC %md
# MAGIC # Embeddings APIs
# MAGIC The OpenAI Embeddings API generates high-dimensional vector representations of text, capturing semantic meaning in a form that's suitable for tasks like semantic search, similarity comparison, clustering, and recommendation. Each input text is mapped to a numerical vector such that semantically similar texts have embeddings that are close in the vector space. These embeddings can then be stored and indexed in a vector database to enable efficient retrieval of contextually relevant information.
# MAGIC
# MAGIC Example below: 
# MAGIC The script sends two input sentences to the text-embedding-ada-002 model to obtain their vector representations, and uses cosine similarity from scikit-learn to measure how semantically close they are. This approach is commonly used in applications like semantic search, document clustering, and recommendation systems.
# MAGIC
# MAGIC The cosine similarity score ranges from -1 to 1:
# MAGIC - Score 1 means the vectors are identical in direction → very similar meanings.
# MAGIC - Score 0 means the vectors are orthogonal → no semantic similarity.
# MAGIC - Score -1 means the vectors are in opposite directions → completely dissimilar, but in practice, OpenAI embeddings rarely produce negative values.

# COMMAND ----------

# MAGIC %pip install openai==1.56.2
# MAGIC %pip install azure-identity==1.19.0
# MAGIC %pip install gradio==4.43.0

# COMMAND ----------

# Auth setup
######## Boilerplate necessary to make things work in OneLab/OpenLab ########
LAB_VARIANT = "OpenLab" # USE "OneLab" IF YOU'RE USING ONELAB

# COMMAND ----------

import os
from openai import AzureOpenAI
from azure.identity import ClientSecretCredential
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
 
ENVIRONMENT = "prd" 

def get_openai_urls(lab_variant: str):
  """This function is created to return OpenAI URLs for OpenLab/OneLab. We want to use the function with lazy evaluation so that OneLab doesn't affect OpenLab and vice-versa."""
  if lab_variant == "OpenLab":
    secret_scope = f"{lab_variant}-SecretScope"
    return f"https://{dbutils.secrets.get(scope=secret_scope, key='OpenAiHostname')}openoaisdc-embeddings-apis/"
  elif lab_variant == "OneLab":
    return f"https://apim-1labgen-ap-apizone-{ENVIRONMENT}01.azure-api.net/openaisdc-embeddings-apis/"
  else: 
    raise Exception("Invalid lab_variant")

os.environ['USER_AGENT'] = 'myagent'
client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientSecret")
cred = ClientSecretCredential(tenant_id="6e93a626-8aca-4dc1-9191-ce291b4b75a1", client_id=client_id, client_secret=client_secret)
token = cred.get_token("https://cognitiveservices.azure.com/.default").token
 
# Client init
client = AzureOpenAI(
    api_key=token,
    api_version="2023-05-15",
    azure_endpoint=get_openai_urls(LAB_VARIANT)
)
 
# Sentences to compare
sentence_1 = "I love machine learning."
sentence_2 = "Artificial intelligence is fascinating."
 
# Get embeddings
embedding_1 = client.embeddings.create(input=sentence_1, model="text-embedding-ada-002").data[0].embedding
embedding_2 = client.embeddings.create(input=sentence_2, model="text-embedding-ada-002").data[0].embedding
 
# Compute cosine similarity
similarity = cosine_similarity([embedding_1], [embedding_2])[0][0]
 
# Show human-friendly output
print(f"Similarity between:\n- \"{sentence_1}\"\n- \"{sentence_2}\"\n=> Score: {similarity:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Azure Document Intelligence
# MAGIC [Confluence page](https://confluence.dev.rabobank.nl/display/OneLab/Azure+AI+Document+Intelligence)
# MAGIC
# MAGIC Make sure to use the correct endpoints. Refer to [Azure OpenAI](https://confluence.dev.rabobank.nl/display/OneLab/Azure+OpenAI) documentation in One!Lab. 
# MAGIC
# MAGIC Pre-requisite: 
# MAGIC - Upload the files you want to use in the dedicated containers or upload it directly to databricks workspace to reference the file
# MAGIC In this example, we are referencing a pdf file directly from a One!Lab allowed website

# COMMAND ----------

# MAGIC %pip install azure-identity
# MAGIC %pip install azure-ai-documentintelligence
# MAGIC %pip install openai

# COMMAND ----------

# it's necessary to restart the kernel
dbutils.library.restartPython()

# COMMAND ----------

LAB_VARIANT = "OpenLab"  # Use "OneLab" if applicable
project_name = "wrrdr" # if your databricks workspace is named, dbw-plab-md-myproject-prd01, myproject is your project name

# COMMAND ----------


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
  """This function is created to return Docuement Intelligence End Point so that it can be disntiguished for OpenLab and OneLab users."""
  if lab_variant == "OpenLab":
    return f"https://di-plab-md-{project_name}-{ENVIRONMENT}01.cognitiveservices.azure.com/"
  elif lab_variant == "OneLab":
    return f"https://di-1lab-md-{project_name}-{ENVIRONMENT}01.cognitiveservices.azure.com/"
  else: 
    raise Exception("Invalid lab_variant")
  
endpoint = get_docintelligence_endpoint(LAB_VARIANT)

 
# --- Download Sample PDF ---
pdf_path = "/Workspace/Repos/Starter_Kit/Databricks_Library/sample-data/layout-pageobject.pdf" # Update the document URL with an allowed whitelisted URL
 
# --- Initialize Document Intelligence Client ---
client = DocumentIntelligenceClient(endpoint=endpoint, credential=credential)
 
# --- Analyze Document ---
model_id = "prebuilt-read"
with open(pdf_path, "rb") as document:
    poller = client.begin_analyze_document(model_id, document)
    result = poller.result()
 
# --- Output Extracted Content ---
for page in result.pages:
    for line in page.lines:
        print(line.content)

# COMMAND ----------

# MAGIC %md
# MAGIC # Azure AI Search
# MAGIC Refer the confluence page for more details regarding AI search[Confluence page](https://confluence.dev.rabobank.nl/display/OneLab/Azure+AI+Search)
# MAGIC
# MAGIC Make sure to use the correct endpoints. Refer to [Azure OpenAI](https://confluence.dev.rabobank.nl/display/OneLab/Azure+OpenAI) documentation in One!Lab. 

# COMMAND ----------

# you can also save this in requirements.txt and install it in your cluster's library
# add or remove the packages that you may or may not need

%pip install python-dotenv
%pip install azure-core
%pip install azure-search-documents
%pip install azure-storage-blob
%pip install azure-identity
%pip install openai
%pip install aiohttp
%pip install ipywidgets
%pip install ipykernel

# COMMAND ----------

# MAGIC %md
# MAGIC Classic/full-text search
# MAGIC
# MAGIC This example demonstrates how to use Azure Cognitive Search with Python to upload simple text documents and perform basic keyword searches. It initializes a SearchClient, uploads a few sample documents with fields like id, title, and content, and then performs a text-based search for the phrase "AI models." The matched documents are printed to show the search results, helping validate that the index and search functionality are working as expected.

# COMMAND ----------

LAB_VARIANT = "OpenLab"  # Use "OneLab" if applicable
project_name = "<enter your project name>" # if your databricks workspace is named, dbw-plab-md-myproject-prd01, myproject is your project name

# COMMAND ----------

# Creates Search Index
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SimpleField,
    SearchableField,
    SearchFieldDataType
)
from azure.identity import ClientSecretCredential 

index_name = "sample-index"  # Update the name of the serach index you want to create

# --- Authentication ---
ENVIRONMENT = "prd"
client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientSecret")
tenant_id = "6e93a626-8aca-4dc1-9191-ce291b4b75a1"  # Replace with your tenant ID 
credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret) 

# --- Search Index Client ---

def get_search_endpoint(lab_variant: str):
  """This function is created to return Docuement Intelligence End Point so that it can be disntiguished for OpenLab and OneLab users."""
  if lab_variant == "OpenLab":
    return f"https://srch-plab-md-{project_name}-{ENVIRONMENT}01.search.windows.net"
  elif lab_variant == "OneLab":
    return f"https://srch-1lab-md-{project_name}-{ENVIRONMENT}01.search.windows.net"
  else: 
    raise Exception("Invalid lab_variant")

search_endpoint = get_search_endpoint(LAB_VARIANT)
index_client = SearchIndexClient(endpoint=search_endpoint, credential=credential) 
# --- Define Index Fields ---
fields = [
    SimpleField(name="id", type=SearchFieldDataType.String, key=True),
    SearchableField(name="title", type=SearchFieldDataType.String, sortable=True),
    SearchableField(name="content", type=SearchFieldDataType.String, analyzer_name="en.lucene"),
] 
# --- Create Index ---
index = SearchIndex(name=index_name, fields=fields)
result = index_client.create_or_update_index(index)
print(f"Index '{result.name}' created successfully ")
 

# COMMAND ----------

# Document Upload

from azure.search.documents import SearchClient 

# --- Use same endpoint and credentials from earlier ---
search_client = SearchClient(endpoint=search_endpoint, index_name=index_name, credential=credential)

documents = [    {
        "id": "1",        "title": "Azure Cognitive Search",
        "content": "Azure Cognitive Search provides indexing and querying capabilities for your data."    },
    {
        "id": "2",        "title": "Document Intelligence",
        "content": "Azure Document Intelligence extracts information from documents using AI models."    },
    {
        "id": "3",        "title": "Vector Search",
        "content": "Vector search allows similarity search over embedded content like text or images."
    }
] 
# --- Upload documents ---
result = search_client.upload_documents(documents=documents)
print("Document upload status:", result[0].status_code)

# COMMAND ----------

# AI Search
# --- Simple text search ---
results = search_client.search(search_text="AI models")
 
# --- Print results ---
for result in results:
    print(f" Title: {result['title']}")
    print(f"   Content: {result['content']}\n")


# COMMAND ----------

# MAGIC %md
# MAGIC Vector (semantic) search
# MAGIC
# MAGIC This code sets up a vector-enabled search index in Azure Cognitive Search, designed specifically for semantic search or retrieval-augmented generation (RAG) use cases. It authenticates using a service principal and connects to the search service using a SearchIndexClient. The index includes both traditional fields for storing metadata and document chunks, as well as a special text_vector field that stores 1536-dimensional embeddings generated by models like OpenAI. It configures vector search using the HNSW algorithm and links it to the vector field through a named profile, enabling similarity-based queries. This setup allows applications to perform intelligent search over embedded content, going beyond keyword matching to understand the semantic relevance of queries and results.
# MAGIC
# MAGIC This creates the index py-rag-tutorial-idx which can be used for vector based search

# COMMAND ----------

# MAGIC %pip install python-dotenv
# MAGIC %pip install azure-core
# MAGIC %pip install azure-search-documents
# MAGIC %pip install azure-storage-blob
# MAGIC %pip install azure-identity
# MAGIC %pip install openai
# MAGIC %pip install aiohttp
# MAGIC %pip install ipywidgets
# MAGIC %pip install ipykernel

# COMMAND ----------

LAB_VARIANT = "OpenLab"  # Use "OneLab" if applicable
project_name = "<enter your project name>" # if your databricks workspace is named, dbw-plab-md-myproject-prd01, myproject is your project name

# COMMAND ----------

from azure.identity import DefaultAzureCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.identity import ClientSecretCredential
from azure.search.documents.indexes.models import (
    SearchField,
    SearchFieldDataType,
    VectorSearch,
    HnswAlgorithmConfiguration,
    VectorSearchProfile,
    SearchIndex
)
# Replace with your values
ENVIRONMENT = "prd"
def get_search_endpoint(lab_variant: str):
  """This function is created to return Docuement Intelligence End Point so that it can be disntiguished for OpenLab and OneLab users."""
  if lab_variant == "OpenLab":
    return f"https://srch-plab-md-{project_name}-{ENVIRONMENT}01.search.windows.net"
  elif lab_variant == "OneLab":
    return f"https://srch-1lab-md-{project_name}-{ENVIRONMENT}01.search.windows.net"
  else: 
    raise Exception("Invalid lab_variant")
AZURE_SEARCH_SERVICE = get_search_endpoint(LAB_VARIANT)

client_id = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientId")
client_secret = dbutils.secrets.get(scope=f"{LAB_VARIANT}-SecretScope", key="DataServicePrincipalClientSecret")
credential = ClientSecretCredential(tenant_id="6e93a626-8aca-4dc1-9191-ce291b4b75a1", client_id=client_id, client_secret=client_secret)
 
# Create index client
index_name = "py-rag-tutorial-idx"
index_client = SearchIndexClient(endpoint=AZURE_SEARCH_SERVICE, credential=credential)
# Define index fields
fields = [
    SearchField(name="parent_id", type=SearchFieldDataType.String),
    SearchField(name="title", type=SearchFieldDataType.String),
    SearchField(name="locations", type=SearchFieldDataType.Collection(SearchFieldDataType.String), filterable=True),
    SearchField(name="chunk_id", type=SearchFieldDataType.String, key=True, sortable=True, filterable=True, facetable=True, analyzer_name="keyword"),
    SearchField(name="chunk", type=SearchFieldDataType.String, sortable=False, filterable=False, facetable=False),
    SearchField(name="text_vector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single), vector_search_dimensions=1536, vector_search_profile_name="myHnswProfile")
]
# Define vector search config
vector_search = VectorSearch(
    algorithms=[
        HnswAlgorithmConfiguration(name="myHnsw"),
    ],
    profiles=[
        VectorSearchProfile(
            name="myHnswProfile",
            algorithm_configuration_name="myHnsw"
        )
    ]
)
# Create the index
index = SearchIndex(name=index_name, fields=fields, vector_search=vector_search)
result = index_client.create_or_update_index(index)
print(f"{result.name} created")
