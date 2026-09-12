# Databricks notebook source
# DBTITLE 1,create requirement.txt in tmp
requirements = """\
streamlit
openai
azure-identity
azure-ai-documentintelligence
"""

dbutils.fs.put(
    "dbfs:/tmp/requirements.txt",
    requirements,
    overwrite=True
)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/"))

# COMMAND ----------

# DBTITLE 1,create folder wheels in tmp
dbutils.fs.mkdirs("dbfs:/tmp/wheels")

# COMMAND ----------

# DBTITLE 1,download and install linux compatible wheels
# MAGIC %pip download \
# MAGIC   --only-binary=:all: \
# MAGIC   --platform manylinux2014_x86_64 \
# MAGIC   --python-version 310 \
# MAGIC   --implementation cp \
# MAGIC   --abi cp310 \
# MAGIC   -r /dbfs/tmp/requirements.txt \
# MAGIC   -d /dbfs/tmp/wheels

# COMMAND ----------

dbutils.fs.ls("dbfs:/tmp/wheels")

# COMMAND ----------

dbutils.fs.cp(
    "dbfs:/tmp/wheels",
    "file:/tmp/wheels",
    recurse=True
)

# COMMAND ----------

# DBTITLE 1,see wheels
# MAGIC %sh
# MAGIC ls -lh /tmp/wheels

# COMMAND ----------

# DBTITLE 1,zip all wheels
# MAGIC %sh
# MAGIC cd /tmp
# MAGIC zip -r wheels.zip wheels

# COMMAND ----------

dbutils.fs.cp(
    "file:/tmp/wheels.zip",
    "dbfs:/tmp/wheels.zip"
)
