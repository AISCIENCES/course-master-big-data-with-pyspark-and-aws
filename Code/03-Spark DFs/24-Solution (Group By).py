# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import sum,avg,max,min,mean,count
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

# 1
df.groupBy("course").count().show()
df.groupBy("course").agg(count("*").alias("total_enrollment")).show()

# COMMAND ----------

# 2
df.groupBy("course", "gender").agg(count("*").alias("total_enrollment")).show()

# COMMAND ----------

# 3
df.groupBy("course", "gender").agg(sum("marks").alias("total_marks")).show()

# COMMAND ----------

# 4
df.groupBy("course", "age").agg(min("marks"), max("marks"), avg("marks")).show()
