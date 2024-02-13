# Databricks notebook source
FILL_IN = 'true'
print(f'actual')

# COMMAND ----------


def fn():
    if FILL_IN:
        assert "SEE THE CODE"
        print("code")
fn()


# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM testing.test_sch.names

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name FROM testing.test_sch.names

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE emp_test(
# MAGIC   emp_id INT,
# MAGIC   emp_name STRING,
# MAGIC   emp_age INT
# MAGIC );
# MAGIC USE LOCATION()
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE emp_test

# COMMAND ----------

