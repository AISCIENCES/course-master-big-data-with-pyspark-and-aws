# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options().text('/FileStore/tables/WordData.txt')
df.show()

# COMMAND ----------

df.groupBy('value').count().show()
