-- Databricks notebook source
CREATE OR REPLACE FUNCTION tab_data(first_name STRING, last_name STRING)
-- RETURN STRING
RETURN concat('the', ' ', first_name, last_name);

SELECT *, tab_data(first_name, last_name) AS fullname FROM names

-- COMMAND ----------

describe function extended tab_data

-- COMMAND ----------

