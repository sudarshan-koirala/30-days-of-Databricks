# Databricks notebook source
# MAGIC %md ## Access the S3 data directly without mounting the S3 bucket

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "aws", key = "access-key")
secret_key = dbutils.secrets.get(scope = "aws", key = "secret-key")

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)


df = spark.read.csv('s3://movie-dataset-youtube/movie_statistic_dataset.csv',inferSchema=True,header=True)
display(df)

# COMMAND ----------

# MAGIC %md ## Access the S3 data by mounting the S3 bucket

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "aws", key = "access-key")
secret_key = dbutils.secrets.get(scope = "aws", key = "secret-key")
encoded_secret_key = secret_key.replace("/", "%2F")

aws_bucket_name = "movie-dataset-youtube"
mount_name = "s3data"

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# COMMAND ----------

file_location = "dbfs:/mnt/s3data/movie_statistic_dataset.csv"

df = spark.read.csv(file_location, inferSchema = True, header= True)
display(df)

# COMMAND ----------

# MAGIC %md ## Unmount

# COMMAND ----------

dbutils.fs.unmount(f"/mnt/{mount_name}")

# COMMAND ----------

df = spark.read.csv(file_location, inferSchema = True, header= True)
display(df)

# COMMAND ----------


