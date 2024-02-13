# Databricks notebook source
#Insatalling Dependencies
from pyspark.sql import SparkSession

#initialize the SparkSession
spark = SparkSession.builder.appName("Date_cleaning_app").getOrCreate()

# Specify the file which you want to cleaning
file_path = "dbfs:/FileStore/tables/Data_cleaning/data_clean.csv"

# Reading the file
cleaning_file = spark.read.csv(file_path, header = True, inferSchema = True)

print("---------Before cleaning---------")
# cleaning_file.printSchema()
cleaning_file.display()


# Dropping Rows containing Null values: this gives the data with out null values 
null_dropper = cleaning_file.na.drop()
# null_dropper.display()

# Subset in dataframe.na.drop() : this will chech the perticular row with null
subset_null = cleaning_file.na.drop(subset=['Joining Year'])
# subset_null.display()

# Thresh in dataframe.na.drop(): Thresh parameter inside drop method takes an integer which acts as a threshold such that all rows containing Non-Null values lesser than the threshold are dropped. 
thresh_null = subset_null.na.drop(thresh=4)
# thresh_null.display()|

# df_pyspark.na.fill() Fill Null Values
null_filling = thresh_null.na.fill("Bio-medical", subset=['Department'])
# null_filling.display()

# Replace Values :
Replacing_val = null_filling.replace({"Information Tech" : "IT"}, subset=['Department'])
# Replacing_val.display()

# Outlier Removal: filter
filter_data = Replacing_val.filter('Age<60')
# filter_data.display()

#Remove null in first and lastname
filter_names = filter_data.filter(filter_data["First Name"].isNotNull() | filter_data["Last Name"].isNotNull())

print("--------After data cleaning---------=")
filter_names.display()











# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, to_date

spark = SparkSession.builder.appName("cleaning").getOrCreate()


data = [
    (1, "John", "Doe", "1980-01-01", "New York", "john.doe@example.com"),
    (2, "Jane", "Smith", None, "Chicago", "jane.smith@example.com"),
    (3, "Mike", "Johnson", "1975/05/10", "Los Angeles", "mike.johnson@example.com"),
    (4, None, "Brown", "1990-12-15", "New York", None),
    (5, "Emily", "Davis", "1988-07-20", "Boston", "emily.davis@example.com"),
    (6, "John", "Doe", "1980-01-01", "New York", "john.doe@example.com")  # Duplicate row
]

schema = ["id", "first_name", "last_name", "dob", "city", "email"]

df = spark.createDataFrame(data, schema= schema)

print("------Before cleaning-----")
display(df)

extract_null = df.na.drop()
# extract_null.display()

remove_dup = extract_null.select("id", "first_name", "last_name", "dob", "city", "email").distinct()
# remove_dup.display()

re = remove_dup.withColumn("dob",
              when(remove_dup['dob'].contains('-'), remove_dup['dob'])
              .when(remove_dup['dob'].contains('/'), to_date(remove_dup['dob'], "yyyy/mm/dd" ) )
              )
final =  re.dropDuplicates(["email"])


final.orderBy('id').display()


# COMMAND ----------

from pyspark.sql.functions import schema_of_json, from_json, collect_set, array_distinct, col, flatten, explode, size
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType


jdata = [
    (1, "John", '{"age": 35, "department": "Engineering"}', ['Python', 'Java']),
    (2, "Jane", '{"age": 28, "department": "Marketing"}', ['JavaScript', 'SQL']),
    (3, "Michael", '{"age": 42, "department": "Finance"}', ['R', 'Scala']),
    (4, "Emily", '{"age": 31, "department": "Human Resources"}', ['C#', 'Ruby'])
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("details", StringType(), True),
    StructField("p_lang", ArrayType(StringType()), True)
])

det = spark.createDataFrame(jdata, schema=schema)
det.display()
# det.withColumnRenamed("values", "age" ).display()
     

# COMMAND ----------

ddf = det.where('details: department = "Engineering"').orderBy("id").limit(1)

ddf.display()
       

# COMMAND ----------

arr = (det.withColumn('p_lang',explode("p_lang")))
display(arr)

# COMMAND ----------

spark.sql("""
create table if not exists sql_1
(s1 int, sname string)
using Delta
options(header = True, inferSchema = True)
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into sql_1
# MAGIC values(1,"karthik")

# COMMAND ----------

print({DA.username})

# COMMAND ----------

