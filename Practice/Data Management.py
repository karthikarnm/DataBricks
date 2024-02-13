# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS new_one_schema2 ;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA new_one_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM testing.test_sch.names

# COMMAND ----------

# MAGIC %sql
# MAGIC USE testing.test_sch

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  names

# COMMAND ----------

# CREATE TABLE IF NOT EXISTS copied_data2;
# COPY INTO copied_data2
#   FROM testing.test_sch.names
#   FILEFORMAT = 'CSV'
#   FORMAT_OPTION("inferSchema" = "true", 'header' = "true" )
#   COPY_OPTION("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC use testing.new_one_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS copied_data2