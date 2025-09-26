# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables (Declarative Lakeflow Pipelines) example
# MAGIC
# MAGIC Delta Live Tables is managed compute DBT-like ETL framework.
# MAGIC
# MAGIC What you will learn:
# MAGIC
# MAGIC 1. How to deploy a DLT job
# MAGIC 2. How to fix data to avoid expectations from failing
# MAGIC 3. Observe how DLT shows lineage
# MAGIC
# MAGIC **DO NOT RUN HERE!**
# MAGIC
# MAGIC **Very important!** This notebook cannot be run manually. It can only run from Jobs.
# MAGIC If you try to run here, you will get an error when trying to import dlt module.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy this notebook as a DLT pipeline
# MAGIC
# MAGIC How to deploy:
# MAGIC
# MAGIC 1. Go to `Job and Pipelines` Menu in the left bar.
# MAGIC 2. Press `Create -> ETL Pipeline`
# MAGIC
# MAGIC * Name: `donjohnson_trips_dq_dlt`, replace `donjohnson` with your name.
# MAGIC * In the top left select the catalog `training`, and the schema `dev_[your username]_taxi_db`
# MAGIC * Press Add existing assets
# MAGIC * Pipeline root folder: select this folder
# MAGIC * Notebook source: Find the trips_dlt.py file in this folder
# MAGIC
# MAGIC Press `Create`
# MAGIC
# MAGIC 3. Run the pipeline by pressing `Run pipelines` button
# MAGIC
# MAGIC It can take a couple of minutes to run the pipeline
# MAGIC
# MAGIC 4. Observe the output to see the lineage between tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Observe failed records
# MAGIC
# MAGIC In the run view, press the Curated data set and observe the expectations fail rate, 
# MAGIC which should be about 36%.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise Convert pickup_borough Null values to `Unknown`
# MAGIC Make sure expectation is not failing.
# MAGIC Run job again to ensure there are no failing expectations.
# MAGIC
# MAGIC Tip: use `fillna()` function with pickup_borough as subset arg.
