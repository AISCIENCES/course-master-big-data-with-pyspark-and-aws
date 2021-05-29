# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.groupBy("course").count().show()
df.groupBy("course","gender").count().show()

# COMMAND ----------

from pyspark.sql.functions import sum,avg,max,min,mean,count

# COMMAND ----------

df.groupBy("course","gender").agg(count("*").alias("total_enrollments"), sum("marks").alias("total_marks"), min("marks").alias("min_makrs"), max("marks"), avg("marks")).show()
