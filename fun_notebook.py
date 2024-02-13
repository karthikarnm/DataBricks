# Databricks notebook source
def create_table(tname):
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {tname}
              (t_id INTEGER, t_name STRING, t_des STRING, time_stamp TIMESTAMP)
              USING DELTA
              """
              )

# COMMAND ----------

# MAGIC %sql
# MAGIC  USE SCHEMA test_sch
# MAGIC
# MAGIC               

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS t2
# MAGIC               (t_id INTEGER, t_name STRING, t_des STRING, time_stamp TIMESTAMP)

# COMMAND ----------

