# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.filter(df.course == "DB").count()

# COMMAND ----------

df.select("gender").distinct().show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.dropDuplicates(["gender","course"]).show()
