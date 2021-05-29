# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.filter(df.course == "DB").show()

# COMMAND ----------

df.filter(col("course") == "DB").show()

# COMMAND ----------

df.filter( (df.course == "DB") & (df.marks > 50) ).show()

# COMMAND ----------

courses = ["DB", "Cloud", "OOP", "DSA"]
df.filter(df.course.isin(courses)).show()

# COMMAND ----------

df.filter(df.course.startswith("DS")).show()

# COMMAND ----------

df.filter(df.name.endswith("se")).show()

# COMMAND ----------

df.filter(df.name.contains("se")).show()

# COMMAND ----------

df.filter(df.name.like('%s%e%')).show()
