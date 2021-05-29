# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema='True', header='True', delimiter=',').csv('/FileStore/tables/StudentData.csv')
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
                    StructField("age", IntegerType(), True),
                    StructField("gender", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("course", StringType(), True),
                    StructField("roll", StringType(), True),
                    StructField("marks", IntegerType(), True),
                    StructField("email", StringType(), True)
])



# COMMAND ----------

df = spark.read.options(header='True').schema(schema).csv('/FileStore/tables/StudentData.csv')
df.show()
df.printSchema()

# COMMAND ----------


