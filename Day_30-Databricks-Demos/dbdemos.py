# Databricks notebook source
# MAGIC %md ## Link to dbdemos [GitHub](https://github.com/databricks-demos/dbdemos)

# COMMAND ----------

# MAGIC %md Please visit [dbdemos.ai](https://www.databricks.com/resources/demos/tutorials?itm_data=demo_center) to explore all demos.

# COMMAND ----------

# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos
dbdemos.help()

# COMMAND ----------

dbdemos.list_demos()

# COMMAND ----------

dbdemos.install('mlops-end2end')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Requirements
# MAGIC dbdemos requires the current user to have:
# MAGIC
# MAGIC - Cluster creation permission
# MAGIC - DLT Pipeline creation permission
# MAGIC - DBSQL dashboard & query creation permission
# MAGIC - For UC demos: Unity Catalog metastore must be available (demo will be installed but won't work)
# MAGIC New with 0.2: dbdemos can import/export dahsboard without the import/export preview (using the dbsqlclone toolkit)
# MAGIC
# MAGIC ### Features
# MAGIC - Load demo notebooks (pre-run) to the given path
# MAGIC - Start job to load dataset based on demo requirement
# MAGIC - Start demo cluster customized for the demo & the current user
# MAGIC - Setup DLT pipelines
# MAGIC - Setup DBSQL dashboard
# MAGIC - Create ML Model
# MAGIC - Demo links are updated with resources created for an easy navigation
# MAGIC

# COMMAND ----------

# MAGIC %md ### [Link to Demo Center](https://www.databricks.com/resources/demos?itm_data=demo_center) & Enjoy 😎

# COMMAND ----------


