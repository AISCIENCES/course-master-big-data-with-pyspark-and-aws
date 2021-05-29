# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.sort(df.marks, df.age).show()

# COMMAND ----------

df.orderBy(df.marks, df.age).show()

# COMMAND ----------

df.sort(df.marks.asc(), df.age.desc()).show()
