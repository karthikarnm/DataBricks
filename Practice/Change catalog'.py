# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("change_catalog").getOrCreate()

available_databases = spark.catalog.listDatabases()
for i in available_databases:
    print(i.name)

# COMMAND ----------

spark.catalog.setCurrentDatabase("new_one_schema")

# COMMAND ----------


available_tables = spark.catalog.listTables()

# Print available tables in the new catalog
print("Available tables in the new catalog:")
for table in available_tables:
    print(table.name)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS catalog_creat MANAGED LOCATION

# COMMAND ----------

