# Databricks notebook source
# MAGIC %md ## Data transformation in Databricks
# MAGIC - PySpark to perform the same operations on DataFrames in a distributed fashion, which is more suitable for larger datasets
# MAGIC - PySpark operations are similar to the Pandas operations but are designed to work with distributed data in a Spark cluster, making them suitable for handling large datasets.
# MAGIC - But depends upon what kind of transformation you are doing (you don't always need pyspark)
# MAGIC - First pyspark example
# MAGIC - Do the same with SQL
# MAGIC - Finally with pandas

# COMMAND ----------

import random

# Create a sample DataFrame 'df1' with 100,000 rows
data1 = [(i, f'Name_{i}', random.randint(60, 100)) for i in range(1, 100001)]
df1 = spark.createDataFrame(data1, ["ID", "Name", "Score1"])

# Create another sample DataFrame 'df2' with 100,000 rows
data2 = [(random.randint(1, 100000), random.randint(60, 100), f'City_{i}') for i in range(1, 100001)]
df2 = spark.createDataFrame(data2, ["ID", "Score2", "City"])

# COMMAND ----------

display(df1)

# COMMAND ----------

from pyspark.sql.functions import min, max

df1.agg(min("ID")).collect()[0][0], df1.agg(max("ID")).collect()[0][0]

# COMMAND ----------

display(df2)

# COMMAND ----------

from pyspark.sql.functions import min, max

df2.agg(min("ID")).collect()[0][0], df2.agg(max("ID")).collect()[0][0]

# COMMAND ----------

# MAGIC %md ## Create a calculated field
# MAGIC Data transformation can involve various operations like filtering, sorting, aggregating, or modifying columns. 
# MAGIC
# MAGIC Here's an example of adding a new column to df1 by multiplying Score1 by 2

# COMMAND ----------

from pyspark.sql.functions import col

df1 = df1.withColumn("Score1_double", col("Score1") * 2)

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md ## Inner Join
# MAGIC - To perform an inner join using PySpark, you can use the join method. Here's how to do an inner join between `df1` and `df2` on the `ID` column

# COMMAND ----------

inner_join_df = df1.join(df2, on="ID", how="inner")

# COMMAND ----------

display(inner_join_df)

# COMMAND ----------

# MAGIC %md ## Left Join 
# MAGIC To perform a left join, you can use the join method with the how parameter set to "left"

# COMMAND ----------

left_join_df = df1.join(df2, on="ID", how="left")
display(left_join_df)

# COMMAND ----------

# MAGIC %md ## Right Join
# MAGIC To perform a right join, you can use the join method with the how parameter set to "right"

# COMMAND ----------

right_join_df = df1.join(df2, on="ID", how="right")
display(right_join_df)

# COMMAND ----------

# MAGIC %md # Now what if we want to use SQL

# COMMAND ----------

import random

# Create a sample DataFrame 'df1' with 100,000 rows
data1 = [(i, f'Name_{i}', random.randint(60, 100)) for i in range(1, 100001)]
df1 = spark.createDataFrame(data1, ["ID", "Name", "Score1"])

# Create another sample DataFrame 'df2' with 100,000 rows
data2 = [(random.randint(1, 100000), random.randint(60, 100), f'City_{i}') for i in range(1, 100001)]
df2 = spark.createDataFrame(data2, ["ID", "Score2", "City"])

# COMMAND ----------

# Register 'df1' as a SQL table
df1.createOrReplaceTempView("table1")

# Register 'df2' as a SQL table
df2.createOrReplaceTempView("table2")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a new calculated field 'Score1_double' by multiplying 'Score1' by 2
# MAGIC SELECT *, Score1 * 2 AS Score1_double FROM table1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inner join 'table1' and 'table2' on 'ID' column
# MAGIC SELECT * FROM table1
# MAGIC INNER JOIN table2 ON table1.ID = table2.ID

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Left join 'table1' and 'table2' on 'ID' column
# MAGIC SELECT * FROM table1
# MAGIC LEFT JOIN table2 ON table1.ID = table2.ID

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Right join 'table1' and 'table2' on 'ID' column
# MAGIC SELECT * FROM table1
# MAGIC RIGHT JOIN table2 ON table1.ID = table2.ID

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Group by the "City" column and calculate the average "Score2" for each city
# MAGIC SELECT City, AVG(Score2) AS Average_Score2
# MAGIC FROM table2
# MAGIC GROUP BY City;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Create a new column 'Total_Score' by adding 'Score1' and 'Score2'
# MAGIC SELECT *, Score1 + Score2 AS Total_Score FROM (
# MAGIC     SELECT * FROM table1
# MAGIC     INNER JOIN table2 ON table1.ID = table2.ID
# MAGIC )

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

pddf = _sqldf.toPandas()
display(pddf)

# COMMAND ----------

pddf.describe()

# COMMAND ----------

# MAGIC %md # Using pandas

# COMMAND ----------

import pandas as pd
import random

# Create a sample DataFrame 'df1' with 100,000 rows
data1 = {
    'ID': range(1, 100001),
    'Name': [f'Name_{i}' for i in range(1, 100001)],
    'Score1': [random.randint(60, 100) for _ in range(100000)],
}

df1 = pd.DataFrame(data1)

# Create another sample DataFrame 'df2' with 100,000 rows
data2 = {
    'ID': [random.randint(1, 100000) for _ in range(100000)],
    'Score2': [random.randint(60, 100) for _ in range(100000)],
    'City': [f'City_{i}' for i in range(1, 100001)],
}

df2 = pd.DataFrame(data2)

# COMMAND ----------

display(df1)

# COMMAND ----------

display(df2)

# COMMAND ----------

inner_join_df = pd.merge(df1, df2, on='ID', how='inner')

# COMMAND ----------

display(inner_join_df)

# COMMAND ----------

df1['Score1_double'] = df1['Score1'] * 2

# COMMAND ----------

inner_join_df = pd.merge(df1, df2, on='ID', how='inner')
display(inner_join_df)

# COMMAND ----------

left_join_df = pd.merge(df1, df2, on='ID', how='left')
display(left_join_df)

# COMMAND ----------

right_join_df = pd.merge(df1, df2, on='ID', how='right')
display(right_join_df)

# COMMAND ----------

inner_join_df['Total_Score'] = inner_join_df['Score1'] + inner_join_df['Score2']
display(inner_join_df)

# COMMAND ----------


