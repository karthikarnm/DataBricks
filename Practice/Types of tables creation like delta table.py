# Databricks notebook source
# MAGIC %fs
# MAGIC ls "dbfs:/FileStore/tables"

# COMMAND ----------

# MAGIC %md
# MAGIC creating a schema , table and inserting data in a regular format using sql

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS  tab_data ;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA tab_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_test(
# MAGIC     data_id INT,
# MAGIC     data_name STRING
# MAGIC
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO data_test(data_id, data_name)
# MAGIC VALUES(22,"KML")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM data_test

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a delta table

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1 Pyspark

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark) \
  .tableName("Delta_table") \
    .addColumn("tab_id", "int")\
    .addColumn("tab_name", "string")\
    .property("description", "table contain data")\
       .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from Delta_table

# COMMAND ----------

from delta.tables import *

DeltaTable.createOrReplace(spark) \
  .tableName("Delta_table1") \
    .addColumn("tab_id", "int")\
    .addColumn("tab_name", "string")\
    .property("description", "table contain data")\
       .execute()

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
  .tableName("Delta_table2") \
    .addColumn("tab_id", "int")\
    .addColumn("tab_name", "string")\
    .property("description", "table contain data")\
      .save("dbfs:/FileStore/tables")\
       .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into Delta_table2 
# MAGIC values(21, "tb1");
# MAGIC SELECT  * from Delta_table2

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema test_sch
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from names

# COMMAND ----------

# MAGIC %md
# MAGIC USING DATAFRAME 
# MAGIC

# COMMAND ----------

# MAGIC %pip install delta

# COMMAND ----------

from delta import *

emp_schema = ['emp_id', 'emp_name', "age"]
emp_data = [[21, "karthik", 22], [22, "santhosh", 24]]

df= spark.createDataFrame(emp_data, schema = emp_schema)

print(df)

# COMMAND ----------

df.write.format('delta').saveAsTable("datalake_table_using_dataframe")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from data_sample

# COMMAND ----------

# MAGIC %md
# MAGIC Spark reading
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Sprak_file").getOrCreate()

file_path = spark.sql("SELECT * FROM names")

file_path.display()
# df = spark.read.csv(file_path, header = True, inferSchema = True )

# df.show()
# df.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC use schema test_sch

# COMMAND ----------

from pyspark.sql.functions import col

data_table = spark.read.table('names')
data_table.distinct().count()

# COMMAND ----------

tab = data_table.where(col('first_name') == "Dave")

# COMMAND ----------

similar_names_df = data_table.groupBy("first_name", "last_name").count().filter(col("count") > 1)

# COMMAND ----------

similar_names_df.display()

# COMMAND ----------

# Find unique first names
unique_names_df = data_table.join(similar_names_df, "first_name", "left_anti")
uni_data = []
unique_names_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC TRANSFORM UNIQUE DATA INTO ANOTHER FILE

# COMMAND ----------

unique_names_df.write.csv("dbfs:/FileStore/tables/new_name", header=True, mode='overwrite')

# COMMAND ----------

similar_names_df.write.csv("dbfs:/FileStore/tables/similar_names_name", header=True, mode='overwrite')

# COMMAND ----------

uni = spark.read.csv("dbfs:/FileStore/tables/new_name", header=True, inferSchema=True)
uni.display()

# COMMAND ----------

uni = spark.read.csv("dbfs:/FileStore/tables/similar_names_name", header=True, inferSchema=True)
uni.display()

# COMMAND ----------

uni.distinct().count()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*), count(first_name), count(last_name), count(email) FROM names

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count_if(email IS NULL) FROM names;
# MAGIC SELECT count(*) FROM names WHERE email IS NULL

# COMMAND ----------

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("data").getOrCreate()

data_read = spark.read.table("names")

data_read.selectExpr("count_if(email is null)").display()

# COMMAND ----------

