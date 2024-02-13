# Databricks notebook source
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, explode
from requests.auth import HTTPBasicAuth



def Rest_api(Access_mode, api):
    res = None
    try:
        if Access_mode == "get":

            res = requests.get(api )
        else:
            print("invalid req")
    except Exception as e:
        return e

    if res != None:
        return json.loads(res.text)
    return None
        
  

   


# COMMAND ----------

Rest_api("get","https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json" )

# COMMAND ----------

spark = SparkSession.builder.appName("rest_api").getOrCreate()

schema = StructType([
  StructField("Count", IntegerType(), True),
  StructField("Message", StringType(), True),
  StructField("SearchCriteria", StringType(), True),
  StructField("Results", ArrayType(
    StructType([
      StructField("Make_ID", IntegerType()),
      StructField("Make_Name", StringType())
    ])
  ))
])


api_df = spark.createDataFrame([
    Rest_api("get","https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json")
], schema= schema)

api_df.display()

# COMMAND ----------

from pyspark.sql.functions import explode, col

api_to_table = api_df.select(explode(col("Results")).alias("results")) \
    .select(col("results").getItem("Make_ID").alias("Make_ID"),
            col("results").getItem("Make_Name").alias("Make_Name"))\
    

api_to_table.write.format("delta").saveAsTable("apiData_to_table1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from apiData_to_table

# COMMAND ----------

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, explode
from requests.auth import HTTPBasicAuth
from pyspark.sql.functions import explode, col


headers = {
    "content-type" : "application/json"
}
body = json.dumps({})

def Rest_api(Access_mode, api):

    valid_users = {
    "user1": "password1",
    "user2": "password2"
}

    username = input("Enter your username: ")
    password = input("Enter your password: ")
    res = None
    if username in valid_users and password == valid_users[username]:
        try:
            if Access_mode == "get":

                res = requests.get(api, auth=HTTPBasicAuth(username, password) )
            else:
                print("invalid req")
        except Exception as e:
            return e
    else:
        return "please check the credentials "
    if res != None:
        return json.loads(res.text)
    return None
        
  


# COMMAND ----------

spark = SparkSession.builder.appName("rest_api").getOrCreate()

schema = StructType([
  StructField("Count", IntegerType(), True),
  StructField("Message", StringType(), True),
  StructField("SearchCriteria", StringType(), True),
  StructField("Results", ArrayType(
    StructType([
      StructField("Make_ID", IntegerType()),
      StructField("Make_Name", StringType())
    ])
  ))
])


api_df = spark.createDataFrame([
    Rest_api("get","https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json", )
], schema= schema)



api_to_table = api_df.select(explode(col("Results")).alias("results")) \
    .select(col("results").getItem("Make_ID").alias("Make_ID"),
            col("results").getItem("Make_Name").alias("Make_Name"))\
    
# api_to_table.display()
api_to_table.write.format("delta").saveAsTable("apiData_to_table3")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from apiData_to_table3

# COMMAND ----------

