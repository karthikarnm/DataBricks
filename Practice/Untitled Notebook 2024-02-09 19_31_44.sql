-- Databricks notebook source
from pyspark.sql.types import *


 schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

data = [
    (1, "John", "Doe", "Engineering", 80000),
    (2, "Jane", "Smith", "Sales", 75000),
    (3, "Alice", "Johnson", "Marketing", 70000),
    (4, "Bob", "Brown", "Engineering", 85000),
    (5, "Emily", "Davis", "Sales", 72000)
]


df = spark.createDataFrame(data, schema)

display(df)