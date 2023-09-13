# Databricks notebook source
# MAGIC %md # Exploring DBFS
# MAGIC - [Databricks Glossary](https://www.databricks.com/glossary)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.ls()

# COMMAND ----------

dbutils.fs.ls(".")

# COMMAND ----------

display(dbutils.fs.ls("."))

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# File location and type
file_location = "/dbfs/FileStore/tables/movie_statistic_dataset.csv"
df = pd.read_csv(filepath_or_buffer=file_location, sep=",")
display(df)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/movie_statistic_dataset.csv"
df = pd.read_csv(filepath_or_buffer=file_location, sep=",")
display(df)

# COMMAND ----------

# File location and type
file_location = "dbfs:/FileStore/tables/movie_statistic_dataset.csv"
df = pd.read_csv(filepath_or_buffer=file_location, sep=",")
display(df)

# COMMAND ----------

# MAGIC %md ### Try spark instead

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/movie_statistic_dataset.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# MAGIC %md #### Spark df to pandas df

# COMMAND ----------

pandasDF = df.toPandas()
print(type(pandasDF))
display(pandasDF)

# COMMAND ----------

# MAGIC %md ## Read Excel file in Community Edition of Databricks
# MAGIC - https://github.com/crealytics/spark-excel

# COMMAND ----------

df = spark.read.format("com.crealytics.spark.excel") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load("dbfs:/FileStore/tables/practice.xlsx")
display(df)

# COMMAND ----------

# DBTITLE 1,Creating temp view
# Create a view or table
temp_table_name = "practice_excel"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# DBTITLE 1,Read it in sql cell
# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `practice_excel`

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

# convert to pandas df
pandasDF = df.toPandas()
print(type(pandasDF))
display(pandasDF)

# COMMAND ----------


