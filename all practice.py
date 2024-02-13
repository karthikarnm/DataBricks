# Databricks notebook source

from pyspark.sql.functions import current_timestamp

spark.sql("""
          CREATE TABLE IF NOT EXISTS prac_quries
          (Q_id INTEGER, Q_name STRING, time_stamp TIMESTAMP)
          USING DELTA
   
          """)

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into prac_quries
# MAGIC values(1,"create", current_timestamp())

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into prac_quries
# MAGIC values(2,"update", current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prac_quries

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, avg, sum,col, min, max


spark = SparkSession.builder.appName("prac-queries").getOrCreate()

data = [("John", 25),
        ("Alice", 30),
        ("Bob", 35)]

columns = ["Name", "Age"]

df = spark.createDataFrame(data, schema= columns)
df= df.withColumn("timeStamp", current_timestamp())

df = df.withColumn("indian_time", from_utc_timestamp(df['timeStamp'], "IST"))

# COMMAND ----------

df.agg(sum("Age"), avg("Age"), min("Age"), max("Age")).show()

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('cleaning').getOrCreate()

file_path = "dbfs:/FileStore/survey_results_public.csv"
csv_data = spark.read.csv(file_path, header=True, inferSchema=True)

# display(csv_data.where(col("employment").isNull()).limit(10))
display(csv_data.distinct().count())




# COMMAND ----------

# MAGIC %run "/Users/karanamkarthi5@gmail.com/fun_notebook"

# COMMAND ----------

create_table("call_from_another")

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from  testing.test_sch.names

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC  use  data_catalog.`data-schema`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table new1
# MAGIC (id_1 int, name_1 string)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view new1 as 
# MAGIC select * from new1
# MAGIC
# MAGIC

# COMMAND ----------

