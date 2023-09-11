-- Databricks notebook source
-- MAGIC %md # Dashboard from notebook + Dashboard with Widget enabled
-- MAGIC
-- MAGIC In this tutorial module, you will learn how to:
-- MAGIC
-- MAGIC - Load sample data
-- MAGIC - View a DataFrame
-- MAGIC - Run SQL queries
-- MAGIC - Visualize the DataFrame
-- MAGIC - Create dashboard
-- MAGIC - Dashboard with Widget enabled

-- COMMAND ----------

-- MAGIC %md ## Load the data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Use the Spark CSV datasource with options specifying:
-- MAGIC # - First line of file is a header
-- MAGIC # - Automatically infer the schema of the data
-- MAGIC data = spark.read.format("csv") \
-- MAGIC     .option("header", "true") \
-- MAGIC     .option("inferSchema", "true") \
-- MAGIC     .load("/databricks-datasets/samples/population-vs-price/data_geo.csv")
-- MAGIC
-- MAGIC data.cache()  # Cache data for faster reuse
-- MAGIC data = data.dropna()  # Drop rows with missing values
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC 1. `spark.read`: In PySpark, `spark.read` is used to create a DataFrameReader, which is responsible for reading data from various sources, including CSV files.
-- MAGIC
-- MAGIC 2. `.format("csv")`: Here, you specify the format of the data source you're reading from. In this case, it's set to "csv," indicating that you are reading data from a CSV file.
-- MAGIC
-- MAGIC 3. `.option("header", "true")`: This line sets an option for the CSV reader. It specifies that the first row of the CSV file contains column names (headers). The "header" option is set to "true" to indicate that the first row should be treated as headers.
-- MAGIC
-- MAGIC 4. `.option("inferSchema", "true")`: This line sets another option for the CSV reader. It specifies that the reader should automatically infer the data types of the columns in the CSV file. When "inferSchema" is set to "true," PySpark will make an attempt to automatically detect the data types (e.g., integer, string) of each column based on the data present in the file.
-- MAGIC
-- MAGIC 5. `.load("/databricks-datasets/samples/population-vs-price/data_geo.csv")`: this line specifies the location of the CSV file to load. In this case, the file is located at "/databricks-datasets/samples/population-vs-price/data_geo.csv." The `load` method reads the data from this file and returns it as a DataFrame.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md ## View the DataFrame

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data.take(10)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(data) #view data in tabular format

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data.printSchema()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC data.take??

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display??

-- COMMAND ----------

-- MAGIC %md ## Run SQL queries

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Register table(view) so it is accessible via SQL Context
-- MAGIC data.createOrReplaceTempView("data_geo")

-- COMMAND ----------

select `State Code`, `2015 median sales price` from data_geo

-- COMMAND ----------

-- MAGIC %md ## Visualize the data & Create Dashboard

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(data)

-- COMMAND ----------

-- MAGIC %md ### Widgets in Dashboard

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(data)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Get distinct values from the "State" column as a list
-- MAGIC # In this code, we directly use a list comprehension to extract the "State" values from the DataFrame and collect them as a list.
-- MAGIC distinct_values_list = [row.State for row in data.select("State").distinct().collect()]
-- MAGIC
-- MAGIC # Show the list of distinct values
-- MAGIC print(distinct_values_list)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create a dropdown widget
-- MAGIC dbutils.widgets.dropdown(name='dropdown_filter', defaultValue='Alabama', choices=distinct_values_list, label='Dropdown')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Functions for data processing
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC # Pass widget values using get()
-- MAGIC display(data.filter(col('State')==dbutils.widgets.get('dropdown_filter')))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit('Success')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------


