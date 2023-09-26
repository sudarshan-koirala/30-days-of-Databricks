# Databricks notebook source
# MAGIC %md 
# MAGIC - https://bamboolib.8080labs.com/
# MAGIC - https://docs.bamboolib.8080labs.com/databricks
# MAGIC - https://docs.bamboolib.8080labs.com/documentation/getting-started
# MAGIC - https://www.databricks.com/blog/2022/08/14/low-code-exploratory-data-analysis-bamboolib-databricks.html
# MAGIC - https://docs.databricks.com/en/notebooks/bamboolib.html#limitations

# COMMAND ----------

# MAGIC %md ## Notebook-scoped packages vs Cluster-scoped

# COMMAND ----------

# MAGIC %pip install bamboolib

# COMMAND ----------

# MAGIC %md ## Loading the UI and exploring

# COMMAND ----------

import bamboolib as bam

# COMMAND ----------

bam

# COMMAND ----------

# MAGIC %md ## Loading data and then bamboolib UI

# COMMAND ----------

import bamboolib as bam
import pandas as pd
df = pd.read_csv(bam.titanic_csv)
df

# COMMAND ----------

import bamboolib as bam

# COMMAND ----------

bam

# COMMAND ----------

df = spark.table("main.default.movie_statistic_dataset").limit(100).toPandas()
df

# COMMAND ----------


