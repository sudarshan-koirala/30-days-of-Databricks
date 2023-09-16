# Databricks notebook source
# MAGIC %md ## MLFlow
# MAGIC - [MLflow](https://www.mlflow.org/) is an open source platform to manage the ML lifecycle, including experimentation, reproducibility, deployment, and a central model registry.
# MAGIC - MLflow was created by Databricks, a company founded by the creators of Apache Spark. [Blog post](https://www.databricks.com/blog/2018/06/05/introducing-mlflow-an-open-source-machine-learning-platform.html)

# COMMAND ----------

# If you are running Databricks Runtime version 7.1 or above, uncomment this line and run this cell:
#%pip install mlflow
 
# If you are running Databricks Runtime version 6.4 to 7.0, uncomment this line and run this cell:
#dbutils.library.installPyPI("mlflow")

# COMMAND ----------

# MAGIC %md #### As we are dealing with ML stuffs, used the ML Databricks Runtime
# MAGIC - 13.3 LTS ML (includes Apache Spark 3.4.0, Scala 2.12)

# COMMAND ----------

# DBTITLE 1,Import required libraries
import mlflow #installed already
import mlflow.sklearn
import pandas as pd
import matplotlib.pyplot as plt
 
from numpy import savetxt
 
from sklearn.model_selection import train_test_split
from sklearn.datasets import load_diabetes
 
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

# COMMAND ----------

# MAGIC %pip list

# COMMAND ----------

# MAGIC %pip show mlflow-skinny

# COMMAND ----------

# MAGIC %md #### MLflow Skinny: A Lightweight Machine Learning Lifecycle Platform Client
# MAGIC More info in this [link](https://pypi.org/project/mlflow-skinny/)

# COMMAND ----------

# DBTITLE 1,Import the dataset from scikit-learn and create the training and test datasets.
db = load_diabetes()
X = db.data
y = db.target
X_train, X_test, y_train, y_test = train_test_split(X, y)

# COMMAND ----------

train_test_split??

# COMMAND ----------

X.shape, y.shape

# COMMAND ----------

X

# COMMAND ----------

y

# COMMAND ----------

# MAGIC %md ### Create a random forest model and log parameters
# MAGIC - [MLFlow Python API](https://mlflow.org/docs/latest/python_api/index.html)
# MAGIC - [Quickstart Guide Outside Databricks](https://mlflow.org/docs/latest/quickstart.html)

# COMMAND ----------

# Enable autolog()
# mlflow.sklearn.autolog() requires mlflow 1.11.0 or above.
mlflow.sklearn.autolog()

# COMMAND ----------

# With autolog() enabled, all model parameters, a model score, and the fitted model are automatically logged.  
with mlflow.start_run():
  
  # Set the model parameters. 
  n_estimators = 100
  max_depth = 6
  max_features = 3
  
  # Create and train model.
  rf = RandomForestRegressor(n_estimators = n_estimators, max_depth = max_depth, max_features = max_features)
  rf.fit(X_train, y_train)
  
  # Use the model to make predictions on the test dataset.
  predictions = rf.predict(X_test)

# COMMAND ----------

# MAGIC %md ### Classification problem

# COMMAND ----------

import warnings

# To ignore all warnings, you can use the following line:
warnings.filterwarnings('ignore')

# COMMAND ----------

import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.datasets import load_iris
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score

# COMMAND ----------

# Load a dataset (in this case, the Iris dataset for simplicity)
data = load_iris()
X = data.data
y = data.target

# COMMAND ----------

# Split the dataset into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# COMMAND ----------

with mlflow.start_run(run_name="classifier"):
    # Create a classifier (in this case, K-Nearest Neighbors)
    clf = KNeighborsClassifier(n_neighbors=3)

    # Train the classifier on the training data
    clf.fit(X_train, y_train)

    # Make predictions on the test data
    y_pred = clf.predict(X_test)


# COMMAND ----------

# Calculate accuracy
accuracy = accuracy_score(y_test, y_pred)

# Print the accuracy
print(f'Accuracy: {accuracy:.2f}')

# COMMAND ----------

# MAGIC %md ## Register models to [model registry](https://docs.databricks.com/en/mlflow/model-registry.html)

# COMMAND ----------

logged_model = 'runs:/672634f12f0c45179ebfc7945dd9081e/model'
register_model_name = "classifier-model"

# COMMAND ----------

mlflow.register_model(logged_model, register_model_name)

# COMMAND ----------


