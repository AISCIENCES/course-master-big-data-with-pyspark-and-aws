# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df = df.withColumnRenamed("gender","sex").withColumnRenamed("roll", "roll number")
df.show()

# COMMAND ----------

df.select(col("name").alias("Full Name")).show()

# COMMAND ----------

df.show()
