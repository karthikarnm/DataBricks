# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS schema_01

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tabular_01(tab_id  INT, tab_name STRING )
# MAGIC USING DELTA

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark)\
  .tableName("deltatable")\
    .addColumn()

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore"))

# COMMAND ----------

# MAGIC %sql
# MAGIC use test_sch

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view na as select * from parquet names

# COMMAND ----------

# MAGIC %sql
# MAGIC insert  overwrite names
# MAGIC select * from na;
# MAGIC
# MAGIC select * from na

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY names

# COMMAND ----------

