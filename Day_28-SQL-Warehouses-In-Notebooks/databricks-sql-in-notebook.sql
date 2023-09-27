-- Databricks notebook source
-- MAGIC %md 
-- MAGIC Each [Unity Catalog](https://www.databricks.com/product/unity-catalog) metastore contains a default catalog named `main` with an empty schema (also called a database) named `default`.
-- MAGIC
-- MAGIC - To create a catalog, use the CREATE CATALOG command with `spark.sql`. **You must be a metastore admin to create a catalog**.
-- MAGIC
-- MAGIC - `catalog.schema.table` (for example) (3 level)
-- MAGIC
-- MAGIC - https://docs.databricks.com/en/data-governance/unity-catalog/best-practices.html

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS quickstart_catalog

-- COMMAND ----------

USE CATALOG main

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

show schemas

-- COMMAND ----------

select * from main.default.movie_statistic_dataset limit 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print('hi')

-- COMMAND ----------


