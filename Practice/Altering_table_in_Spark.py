# Databricks notebook source
# MAGIC %fs mkdirs dbfs:/testing.test_sch.names
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

dbfs_path = "dbfs:/FileStore/tables/names.csv"
copied_file = "dbfs:/FileStore/tables/copy_names.csv"

df = spark.read.csv(dbfs_path, header = True, inferSchema = True)


# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()


# COMMAND ----------

# df.select('first_name').show()

# COMMAND ----------

filterd_data = df.filter(df['first_name'] == "Jane").display()
# show_data = df.select('first_name').show()

# COMMAND ----------

filterd_data.show()

# COMMAND ----------

copy_location = "dbfs:/FileStore/tables/copy_names.csv"
df.write.csv(copy_location, mode = "overwrite", header = True)
print("copied")


# COMMAND ----------

# spark.stop()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

spark = SparkSession.builder.appName('application').getOrCreate()

file_data = "dbfs:/FileStore/tables/names.csv"
data = spark.read.csv(file_data, header = True, inferSchema=True )


data.show()


# COMMAND ----------

similar =data.groupBy("first_name").count().filter(col("count")>1)

similar.display()

# COMMAND ----------

data.join(similar, "first_name",'leftanti').display()

# COMMAND ---------
drop_dup = data.dropDuplicates(["first_name"]).filter(data['email'].isNotNull())


drop_dup.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from names

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(first_name) from names

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW duped_names AS 
# MAGIC SELECT first_name,last_name, max(email) AS email from names
# MAGIC WHERE first_name IS NOT NULL
# MAGIC GROUP BY first_name,last_name, email;
# MAGIC
# MAGIC SELECT * FROM duped_names

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended names

# COMMAND ----------

