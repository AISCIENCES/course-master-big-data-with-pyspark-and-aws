# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.groupBy("gender").sum("marks").show()

# COMMAND ----------

df.groupBy("gender").count().show()
df.groupBy("course").count().show()
df.groupBy("course").sum("marks").show()

# COMMAND ----------

df.groupBy("gender").max("marks").show()
df.groupBy("gender").min("marks").show()

# COMMAND ----------

df.groupBy("age").avg("marks").show()

# COMMAND ----------

df.groupBy("gender").mean("marks").show()
