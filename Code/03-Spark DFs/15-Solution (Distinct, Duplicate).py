# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df2 = df.select("age","gender","course")
df2.distinct().show()
df2.distinct().count()

# COMMAND ----------

df3 = df.dropDuplicates(["age", "gender", "course"])
df3.show()
df3.count()

# COMMAND ----------


