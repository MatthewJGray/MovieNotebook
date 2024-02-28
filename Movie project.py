# Databricks notebook source
dbutils.fs.mount(
    source='wasbs://project-movies-data@moviesdatatutorial.blob.core.windows.net',
    mount_point='/mnt/project-movies-data',
    extra_configs = {'fs.azure.account.key.moviesdatatutorial.blob.core.windows.net': dbutils.secrets.get('projectmoviesscope', 'storageAccountKey')}
)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/project-movies-data"
# MAGIC

# COMMAND ----------

action = spark.read.format("csv").load("/mnt/project-movies-data/raw-data/action.csv")

# COMMAND ----------

action.show()

# COMMAND ----------

action = spark.read.format("csv").option("header","true").load("/mnt/project-movies-data/raw-data/action.csv")

# COMMAND ----------

action.show()

# COMMAND ----------

jjj
